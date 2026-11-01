package gen

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/gen/value"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

// StreamMain is the only stream of random and hybrid schemas.
const StreamMain = "main"

// StreamID names an independent index space in a session.
type StreamID string

// TableStream returns a relational table's stream.
func TableStream(table string) StreamID { return StreamID("table:" + table) }

// StreamSpec describes a stream to the planner and serializers.
type StreamSpec struct {
	ID StreamID
	// Space is the size of the index space, or 0 when only the session's limits bound it.
	Space uint64
	// PerParent is the index space's block size for an owned table: indices come in runs
	// of this many, one run per parent. It is 1 for dense streams.
	PerParent uint64
	// ExpectedRows estimates how many indices hold a row. It equals Space when dense.
	ExpectedRows float64
	// MaxRecordBytes bounds one record's serialized size.
	MaxRecordBytes int64
	// Fields are the columns, in order, for tabular output. Nil for schemaless streams.
	Fields []schema.FieldDef
}

// Input is everything generation depends on. Equal inputs, with the same engine
// version, produce byte-identical records.
type Input struct {
	Schema        *schema.Schema // must have passed schema.Validate
	Seed          uint64
	ReferenceTime time.Time
	Vocabulary    value.Vocabulary // optional
	Registry      *value.Registry  // nil means the built-in generators
	// MaxRows is how many records a random or hybrid stream may produce, 0 if unlimited.
	// Unique fields must have enough values for it.
	MaxRows uint64
	// NestedRoot selects the nested layout: this table's rows embed what they own.
	NestedRoot string
}

// Engine generates one schema. It is immutable and safe to share; each goroutine takes
// its own StreamGenerator.
type Engine struct {
	in      Input
	reg     *value.Registry
	root    unique.Key
	defs    map[string]*schema.Definition
	streams []StreamSpec
	tables  map[string]*tablePlan
}

// StreamGenerator produces the records of one stream. It belongs to one goroutine.
type StreamGenerator interface {
	// Fill appends records with indices in [from, to) until b holds limit records or the
	// range is finished, and returns where to continue. It may stop early, even with b
	// empty, so that a long run of empty indices cannot delay cancellation; next is
	// always greater than from unless an error is returned.
	Fill(b *record.Batch, from, to uint64, limit int) (next uint64, err error)
}

// Build prepares an engine. It binds every generator once, so problems surface now
// rather than deep in a run.
func Build(in Input) (*Engine, error) {
	if in.Schema == nil {
		return nil, errors.New("no schema")
	}
	e := &Engine{
		in:   in,
		reg:  in.Registry,
		root: unique.SeedKey(in.Seed).Derive("engine\x00" + strconv.Itoa(Version)),
		defs: make(map[string]*schema.Definition, len(in.Schema.Definitions)),
	}
	if e.reg == nil {
		e.reg = value.NewRegistry()
	}
	for i := range in.Schema.Definitions {
		d := &in.Schema.Definitions[i]
		e.defs[d.Name] = d
	}

	switch {
	case in.Schema.Random != nil:
		e.streams = []StreamSpec{{
			ID: StreamMain, Space: in.MaxRows, PerParent: 1, ExpectedRows: float64(in.MaxRows),
			MaxRecordBytes: in.Schema.Random.MaxRecordBytes,
		}}
	case in.Schema.Hybrid != nil:
		e.streams = []StreamSpec{{
			ID: StreamMain, Space: in.MaxRows, PerParent: 1, ExpectedRows: float64(in.MaxRows),
			MaxRecordBytes: schema.MaxRecordBytes(in.Schema.Hybrid.Fields, in.Schema.Definitions),
			Fields:         in.Schema.Hybrid.Fields,
		}}
	case in.Schema.Relational != nil:
		if err := e.planTables(); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("schema has no content")
	}

	for _, s := range e.streams {
		if _, err := e.NewStreamGenerator(s.ID); err != nil {
			return nil, fmt.Errorf("stream %s: %w", s.ID, err)
		}
	}
	return e, nil
}

// Streams lists the streams, parents before the tables they own.
func (e *Engine) Streams() []StreamSpec { return e.streams }

// NewStreamGenerator returns a generator for one goroutine.
func (e *Engine) NewStreamGenerator(id StreamID) (StreamGenerator, error) {
	switch {
	case e.in.Schema.Random != nil:
		return e.newFlat(id, func(env *value.Env) (rowFunc, error) {
			g, err := newRandomGen(e.in.Schema.Random, e.reg, env)
			if err != nil {
				return nil, err
			}
			return func(c *value.Ctx, o *record.Object) error {
				return g.fill(c, o, e.in.Schema.Random.MaxRecordBytes, nil)
			}, nil
		})
	case e.in.Schema.Hybrid != nil:
		return e.newFlat(id, func(env *value.Env) (rowFunc, error) { return e.hybridRow(env) })
	}
	return e.newTableStream(id)
}

func (e *Engine) env(stream StreamID, r *rand.Rand, rows uint64) *value.Env {
	return &value.Env{
		Rand:        r,
		Key:         e.root.Derive("stream\x00" + string(stream)),
		RefTime:     e.in.ReferenceTime,
		Definitions: e.defs,
		Vocabulary:  e.in.Vocabulary,
		Rows:        rows,
	}
}

type rowFunc func(c *value.Ctx, o *record.Object) error

// flatStream is a dense stream: every index holds a record.
type flatStream struct {
	id  StreamID
	key uint64
	pcg *rand.PCG
	ctx value.Ctx
	row rowFunc
}

func (e *Engine) newFlat(id StreamID, build func(*value.Env) (rowFunc, error)) (StreamGenerator, error) {
	if id != StreamMain {
		return nil, fmt.Errorf("unknown stream %q", id)
	}
	pcg := rand.NewPCG(0, 0)
	r := rand.New(pcg)
	env := e.env(id, r, e.in.MaxRows)
	row, err := build(env)
	if err != nil {
		return nil, err
	}
	return &flatStream{
		id:  id,
		key: env.Key.K0,
		pcg: pcg,
		row: row,
		ctx: value.Ctx{Rand: r, RefTime: e.in.ReferenceTime, Scratch: make([]byte, 0, 256)},
	}, nil
}

func (s *flatStream) Fill(b *record.Batch, from, to uint64, limit int) (uint64, error) {
	b.Stream = string(s.id)
	s.ctx.Arena = &b.Arena
	i := from
	for i < to && b.Len() < limit {
		s.pcg.Seed(s.key, unique.Mix(i))
		s.ctx.Index = i
		if err := s.row(&s.ctx, b.Next(i)); err != nil {
			b.DropLast()
			return i, fmt.Errorf("record %d: %w", i, err)
		}
		i++
	}
	return i, nil
}

// hybridRow builds the generator of one hybrid record.
func (e *Engine) hybridRow(env *value.Env) (rowFunc, error) {
	h := e.in.Schema.Hybrid
	fields, err := e.reg.BindFields(h.Fields, env)
	if err != nil {
		return nil, err
	}
	declared := &value.ObjectGen{Fields: fields}
	if h.ExtraRandom == nil {
		return declared.Fill, nil
	}

	extra, err := newRandomGen(&h.ExtraRandom.Spec, e.reg, env)
	if err != nil {
		return nil, err
	}
	reserved := make(map[string]bool, len(h.Fields))
	for i := range h.Fields {
		reserved[h.Fields[i].Name] = true
	}
	p := h.ExtraRandom.Probability
	limit := h.ExtraRandom.Spec.MaxRecordBytes
	return func(c *value.Ctx, o *record.Object) error {
		if err := declared.Fill(c, o); err != nil {
			return err
		}
		if c.Rand.Float64() >= p {
			return nil
		}
		// Extras only fill what the declared fields leave.
		return extra.fill(c, o, limit-o.ApproxSize(), reserved)
	}, nil
}
