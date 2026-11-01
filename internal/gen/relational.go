package gen

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/gen/value"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

const (
	// parentSearchAttempts bounds how long a selecting reference looks for a parent row
	// that has children. See docs/02-domain/relational-generation.md.
	parentSearchAttempts = 32
	// scanLimit is how many indices one Fill call looks at, so empty stretches of a sparse
	// stream cannot hold up cancellation.
	scanLimit = 1 << 16
)

// tablePlan is what is known about a table before any generation.
type tablePlan struct {
	table    *schema.Table
	stream   StreamID
	parent   *tablePlan
	children []*tablePlan
	space    uint64 // size of the index space
	perMax   uint64 // per_parent.max, or 1 for a root
	expected float64
	countKey uint64
}

func (e *Engine) planTables() error {
	r := e.in.Schema.Relational
	e.tables = make(map[string]*tablePlan, len(r.Tables))
	for i := range r.Tables {
		t := &r.Tables[i]
		e.tables[t.Name] = &tablePlan{table: t, stream: TableStream(t.Name), perMax: 1}
	}
	for _, p := range e.tables {
		if p.table.Owner != nil {
			parent := e.tables[p.table.Owner.Table]
			if parent == nil {
				return fmt.Errorf("table %s: unknown owner %s", p.table.Name, p.table.Owner.Table)
			}
			p.parent = parent
			p.perMax = uint64(p.table.Owner.PerParent.Max)
			p.countKey = e.root.Derive("count\x00" + p.table.Name).K0
		}
	}
	// Document order keeps children and streams deterministic.
	for i := range r.Tables {
		p := e.tables[r.Tables[i].Name]
		if p.parent != nil {
			p.parent.children = append(p.parent.children, p)
		}
	}

	seen := map[*tablePlan]bool{}
	var visit func(p *tablePlan) error
	visit = func(p *tablePlan) error {
		if seen[p] {
			return nil
		}
		seen[p] = true
		switch {
		case p.parent == nil:
			if p.table.Count == nil {
				return fmt.Errorf("table %s has no count", p.table.Name)
			}
			p.space = uint64(*p.table.Count)
			p.expected = float64(p.space)
		default:
			if err := visit(p.parent); err != nil {
				return err
			}
			p.space = p.parent.space * p.perMax
			pp := p.table.Owner.PerParent
			p.expected = p.parent.expected * float64(pp.Min+pp.Max) / 2
		}
		if e.in.NestedRoot == "" {
			e.streams = append(e.streams, StreamSpec{
				ID: p.stream, Space: p.space, PerParent: p.perMax, ExpectedRows: p.expected,
				MaxRecordBytes: schema.MaxRecordBytes(p.table.Fields, e.in.Schema.Definitions),
				Fields:         p.table.Fields,
			})
		}
		return nil
	}
	for i := range r.Tables {
		if err := visit(e.tables[r.Tables[i].Name]); err != nil {
			return err
		}
	}

	if e.in.NestedRoot != "" {
		root := e.tables[e.in.NestedRoot]
		if root == nil || root.parent != nil {
			return fmt.Errorf("nested layout: %q is not a root table", e.in.NestedRoot)
		}
		e.streams = []StreamSpec{{
			ID: root.stream, Space: root.space, PerParent: 1, ExpectedRows: root.expected,
			MaxRecordBytes: nestedBytes(root, e.in.Schema.Definitions),
		}}
	}
	return nil
}

func nestedBytes(p *tablePlan, defs []schema.Definition) int64 {
	total := schema.MaxRecordBytes(p.table.Fields, defs)
	for _, c := range p.children {
		n := nestedBytes(c, defs)
		if n > math.MaxInt64/int64(c.perMax) {
			return math.MaxInt64
		}
		total += n * int64(c.perMax)
	}
	return total
}

// slot holds one goroutine's state for every table, as generating a row may need keys,
// counts or fields of others.
type slot struct {
	e      *Engine
	tables map[*tablePlan]*tableState
}

// tableState generates rows of one table.
type tableState struct {
	s    *slot
	plan *tablePlan
	key  unique.Key

	pcg *rand.PCG
	rnd *rand.Rand
	ctx value.Ctx

	countPCG *rand.PCG
	countRnd *rand.Rand
	zipf     *rand.Zipf
	lastP    uint64
	lastK    uint64
	hasLast  bool

	cols  []column
	keys  map[string]value.KeyGenerator
	bound bool

	replayIdx   uint64
	hasReplay   bool
	replayRow   record.Object
	replayArena record.Arena
}

type column struct {
	name string
	gen  value.Generator
}

func (e *Engine) newTableStream(id StreamID) (StreamGenerator, error) {
	s := &slot{e: e, tables: map[*tablePlan]*tableState{}}
	var target *tablePlan
	for _, p := range e.tables {
		if p.stream == id {
			target = p
		}
	}
	if target == nil {
		return nil, fmt.Errorf("unknown stream %q", id)
	}
	if e.in.NestedRoot != "" {
		st, err := s.state(target)
		if err != nil {
			return nil, err
		}
		if err := st.bindNested(); err != nil {
			return nil, err
		}
		return &nestedStream{st: st}, nil
	}
	st, err := s.state(target)
	if err != nil {
		return nil, err
	}
	return &tableStream{st: st}, nil
}

// state returns a table's state, binding its columns on first use.
func (s *slot) state(p *tablePlan) (*tableState, error) {
	if st, ok := s.tables[p]; ok {
		return st, nil
	}
	pcg := rand.NewPCG(0, 0)
	r := rand.New(pcg)
	countPCG := rand.NewPCG(0, 0)
	st := &tableState{
		s: s, plan: p, pcg: pcg, rnd: r, countPCG: countPCG, countRnd: rand.New(countPCG),
		key:  s.e.root.Derive("stream\x00" + string(p.stream)),
		keys: map[string]value.KeyGenerator{},
		ctx:  value.Ctx{Rand: r, RefTime: s.e.in.ReferenceTime, Scratch: make([]byte, 0, 256)},
	}
	s.tables[p] = st
	if p.parent != nil {
		if d := p.table.Owner.PerParent.Distribution; d != nil && d.Type == schema.DistZipf {
			pp := p.table.Owner.PerParent
			st.zipf = value.NewZipf(st.countRnd, d, uint64(pp.Max-pp.Min))
		}
	}
	return st, st.bind()
}

func (st *tableState) env() *value.Env { return st.s.e.env(st.plan.stream, st.rnd, st.plan.space) }

// bind builds the columns. Cycles between tables are fine: selecting references only
// need a key generator, which involves no other table.
func (st *tableState) bind() error {
	if st.bound {
		return nil
	}
	st.bound = true
	t := st.plan.table
	st.cols = make([]column, 0, len(t.Fields))
	for i := range t.Fields {
		f := &t.Fields[i]
		g, err := st.column(f)
		if err != nil {
			return fmt.Errorf("table %s, field %s: %w", t.Name, f.Name, err)
		}
		st.cols = append(st.cols, column{f.Name, g})
	}
	return nil
}

func (st *tableState) column(f *schema.FieldDef) (value.Generator, error) {
	t := st.plan.table
	if f.Generator != nil && f.Generator.Name == schema.GeneratorOrdinal && t.Owner == nil {
		return nil, fmt.Errorf("the ordinal generator needs a table with an owner")
	}
	switch {
	case f.Ref != nil:
		return st.newRefGen(f)
	case f.Derive != nil:
		return st.newDeriveGen(f)
	case f.Name == t.PrimaryKey || f.Unique:
		copied := *f
		copied.Unique = true
		return st.s.e.reg.Bind(&copied, st.env())
	}
	return st.s.e.reg.Bind(f, st.env())
}

// keyGen returns the generator that computes a key field for any row of this table.
func (st *tableState) keyGen(field string) (value.KeyGenerator, error) {
	if g, ok := st.keys[field]; ok {
		return g, nil
	}
	f := st.plan.table.Field(field)
	if f == nil {
		return nil, fmt.Errorf("table %s has no field %s", st.plan.table.Name, field)
	}
	copied := *f
	copied.Unique = true
	g, err := st.s.e.reg.BindKey(&copied, st.env())
	if err != nil {
		return nil, fmt.Errorf("key %s.%s: %w", st.plan.table.Name, field, err)
	}
	st.keys[field] = g
	return g, nil
}

// childCount returns how many rows of this table a parent row owns. It is a pure
// function of the parent's index.
func (st *tableState) childCount(parent uint64) uint64 {
	if st.hasLast && st.lastP == parent {
		return st.lastK
	}
	pp := st.plan.table.Owner.PerParent
	st.countPCG.Seed(st.plan.countKey, unique.Mix(parent))
	span := uint64(pp.Max - pp.Min)
	var k uint64
	switch {
	case span == 0:
		k = uint64(pp.Min)
	case st.zipf != nil:
		k = uint64(pp.Min) + st.zipf.Uint64()
	case pp.Distribution != nil && pp.Distribution.Type == schema.DistNormal:
		d := pp.Distribution
		x := math.Round(st.countRnd.NormFloat64()*(*d.StdDev) + *d.Mean)
		k = uint64(min(max(x, float64(pp.Min)), float64(pp.Max)))
	default:
		k = uint64(pp.Min) + st.countRnd.Uint64N(span+1)
	}
	st.lastP, st.lastK, st.hasLast = parent, k, true
	return k
}

// exists reports whether an index holds a row.
func (st *tableState) exists(idx uint64) (bool, error) {
	p := st.plan
	if p.parent == nil {
		return idx < p.space, nil
	}
	parentIdx, ordinal := idx/p.perMax, idx%p.perMax
	parent, err := st.s.state(p.parent)
	if err != nil {
		return false, err
	}
	ok, err := parent.exists(parentIdx)
	if err != nil || !ok {
		return false, err
	}
	return ordinal < st.childCount(parentIdx), nil
}

// row generates the row at idx. Fields are drawn in schema order from a generator
// seeded by the row's index, so re-running it reproduces the row exactly.
func (st *tableState) row(idx uint64, arena *record.Arena, o *record.Object) error {
	st.pcg.Seed(st.key.K0, unique.Mix(idx))
	st.ctx.Index = idx
	st.ctx.Arena = arena
	st.ctx.Ordinal, st.ctx.Parent = 0, 0
	if st.plan.parent != nil {
		st.ctx.Parent = idx / st.plan.perMax
		st.ctx.Ordinal = idx%st.plan.perMax + 1
	}
	for i := range st.cols {
		if err := st.cols[i].gen.Generate(&st.ctx, o.Add(st.cols[i].name)); err != nil {
			return &value.FieldError{Name: st.cols[i].name, Err: err}
		}
	}
	return nil
}

// replay reproduces a row without storing anything, for fields derived from it.
func (st *tableState) replay(idx uint64) (*record.Object, error) {
	if st.hasReplay && st.replayIdx == idx {
		return &st.replayRow, nil
	}
	st.replayArena.Reset()
	clear(st.replayRow.Fields)
	st.replayRow.Fields = st.replayRow.Fields[:0]
	if err := st.row(idx, &st.replayArena, &st.replayRow); err != nil {
		st.hasReplay = false
		return nil, err
	}
	st.replayIdx, st.hasReplay = idx, true
	return &st.replayRow, nil
}

// tableStream is one table in the normalized layout. Its indices may be sparse.
type tableStream struct{ st *tableState }

func (s *tableStream) Fill(b *record.Batch, from, to uint64, limit int) (uint64, error) {
	st := s.st
	b.Stream = string(st.plan.stream)
	perMax := st.plan.perMax
	idx := from
	for scanned := 0; idx < to && b.Len() < limit && scanned < scanLimit; scanned++ {
		ok, err := st.exists(idx)
		if err != nil {
			return idx, err
		}
		if !ok {
			// Nothing else in this parent's run can exist either.
			idx = (idx/perMax + 1) * perMax
			continue
		}
		if err := st.row(idx, &b.Arena, b.Next(idx)); err != nil {
			b.DropLast()
			return idx, fmt.Errorf("table %s, row %d: %w", st.plan.table.Name, idx, err)
		}
		idx++
	}
	return min(idx, to), nil
}

// nestedStream embeds owned rows inside their root row.
type nestedStream struct{ st *tableState }

func (st *tableState) bindNested() error {
	for _, c := range st.plan.children {
		child, err := st.s.state(c)
		if err != nil {
			return err
		}
		if err := child.bindNested(); err != nil {
			return err
		}
	}
	return nil
}

func (s *nestedStream) Fill(b *record.Batch, from, to uint64, limit int) (uint64, error) {
	b.Stream = string(s.st.plan.stream)
	i := from
	for i < to && b.Len() < limit {
		if err := s.st.nestedRow(i, &b.Arena, b.Next(i)); err != nil {
			b.DropLast()
			return i, fmt.Errorf("table %s, row %d: %w", s.st.plan.table.Name, i, err)
		}
		i++
	}
	return i, nil
}

func (st *tableState) nestedRow(idx uint64, arena *record.Arena, o *record.Object) error {
	if err := st.row(idx, arena, o); err != nil {
		return err
	}
	for _, c := range st.plan.children {
		child := st.s.tables[c]
		k := child.childCount(idx)
		list := arena.Values(int(k))
		for ord := range k {
			row := arena.Object(len(child.cols))
			if err := child.nestedRow(idx*c.perMax+ord, arena, row); err != nil {
				return err
			}
			list[ord].SetObject(row)
		}
		o.Add(c.table.Name).SetArray(list)
	}
	return nil
}

// refGen fills a foreign key. Parents are never looked up: a key is computed from the
// parent's index, and the index is chosen so that the row is known to exist.
type refGen struct {
	st       *tableState // the table holding this column
	target   *tableState
	key      value.KeyGenerator
	owner    bool
	nullP    float64
	dist     *schema.Distribution
	zipf     *rand.Zipf
	earlier  bool
	distinct bool

	distinctKey    uint64
	distinctPCG    *rand.PCG
	distinctRnd    *rand.Rand
	distinctParent uint64
	distinctSet    []uint64
	distinctHas    bool
}

func (st *tableState) newRefGen(f *schema.FieldDef) (value.Generator, error) {
	targetPlan := st.s.e.tables[f.Ref.Table]
	if targetPlan == nil {
		return nil, fmt.Errorf("unknown table %s", f.Ref.Table)
	}
	target, err := st.s.state(targetPlan)
	if err != nil {
		return nil, err
	}
	key, err := target.keyGen(f.Ref.Field)
	if err != nil {
		return nil, err
	}
	g := &refGen{st: st, target: target, key: key, owner: f.Ref.Owner}
	if f.Nullable {
		g.nullP = value.DefaultNullProbability
		if f.NullProbability != nil {
			g.nullP = *f.NullProbability
		}
	}
	if s := f.Ref.Select; s != nil {
		g.dist = s.Distribution
		g.earlier = s.Mode == schema.SelectEarlier
		g.distinct = s.Distinct
		if g.dist != nil && g.dist.Type == schema.DistZipf {
			// The rows of the root of the target's ownership chain are what is selected.
			g.zipf = value.NewZipf(st.rnd, g.dist, rootOf(targetPlan).space-1)
		}
		if g.distinct {
			g.distinctKey = st.key.Derive("distinct\x00" + f.Name).K0
			g.distinctPCG = rand.NewPCG(0, 0)
			g.distinctRnd = rand.New(g.distinctPCG)
		}
	}
	return g, nil
}

func rootOf(p *tablePlan) *tablePlan {
	for p.parent != nil {
		p = p.parent
	}
	return p
}

func (g *refGen) Generate(c *value.Ctx, out *record.Value) error {
	if g.owner {
		return g.key.KeyAt(c, c.Parent, out)
	}
	if g.nullP > 0 && c.Rand.Float64() < g.nullP {
		out.SetNull()
		return nil
	}
	idx, ok, err := g.chooseIndex(c)
	if err != nil {
		return err
	}
	if !ok {
		if g.nullP > 0 || g.earlier {
			out.SetNull()
			return nil
		}
		return fmt.Errorf("found no row of %s to refer to", g.target.plan.table.Name)
	}
	return g.key.KeyAt(c, idx, out)
}

func (g *refGen) chooseIndex(c *value.Ctx) (uint64, bool, error) {
	if g.earlier {
		if c.Index == 0 {
			return 0, false, nil
		}
		return c.Rand.Uint64N(c.Index), true, nil
	}
	if g.distinct {
		return g.distinctIndex(c)
	}
	return g.pick(c, g.target.plan)
}

// pick draws a row of p that certainly exists, working down from the root of its
// ownership chain.
func (g *refGen) pick(c *value.Ctx, p *tablePlan) (uint64, bool, error) {
	if p.parent == nil {
		return g.pickRoot(c, p), true, nil
	}
	st := g.st.s.tables[p]
	for range parentSearchAttempts {
		parent, ok, err := g.pick(c, p.parent)
		if err != nil || !ok {
			return 0, false, err
		}
		if k := st.childCount(parent); k > 0 {
			return parent*p.perMax + c.Rand.Uint64N(k), true, nil
		}
	}
	return 0, false, nil
}

func (g *refGen) pickRoot(c *value.Ctx, p *tablePlan) uint64 {
	switch {
	case g.zipf != nil:
		return g.zipf.Uint64()
	case g.dist != nil && g.dist.Type == schema.DistNormal:
		x := math.Round(c.Rand.NormFloat64()*(*g.dist.StdDev) + *g.dist.Mean)
		return uint64(min(max(x, 0), float64(p.space-1)))
	}
	return c.Rand.Uint64N(p.space)
}

// distinctIndex gives each child of one owner a different parent, using Floyd's sampling
// without replacement over the target's rows. The set is a pure function of the owner.
func (g *refGen) distinctIndex(c *value.Ctx) (uint64, bool, error) {
	if !g.distinctHas || g.distinctParent != c.Parent {
		k := g.st.childCount(c.Parent)
		n := g.target.plan.space
		g.distinctPCG.Seed(g.distinctKey, unique.Mix(c.Parent))
		g.distinctSet = g.distinctSet[:0]
		seen := make(map[uint64]bool, k)
		for j := n - k; j < n; j++ {
			t := g.distinctRnd.Uint64N(j + 1)
			if seen[t] {
				t = j
			}
			seen[t] = true
			g.distinctSet = append(g.distinctSet, t)
		}
		g.distinctParent, g.distinctHas = c.Parent, true
	}
	return g.distinctSet[c.Ordinal-1], true, nil
}

// deriveGen copies a field from the owning row, regenerating that row as needed.
type deriveGen struct {
	parent   *tableState
	field    string
	typ      schema.TypeName
	format   string
	min, max time.Duration
	offset   bool
}

func (st *tableState) newDeriveGen(f *schema.FieldDef) (value.Generator, error) {
	if st.plan.parent == nil {
		return nil, fmt.Errorf("the table has no owner to derive from")
	}
	parent, err := st.s.state(st.plan.parent)
	if err != nil {
		return nil, err
	}
	g := &deriveGen{parent: parent, field: f.Derive.Field, typ: f.Type, format: f.Format}
	if d := f.Derive; d.OffsetMin != nil || d.OffsetMax != nil {
		g.offset = true
		if d.OffsetMin != nil {
			g.min = *d.OffsetMin
		}
		if d.OffsetMax != nil {
			g.max = *d.OffsetMax
		}
		g.max = max(g.min, g.max)
	}
	return g, nil
}

func (g *deriveGen) Generate(c *value.Ctx, out *record.Value) error {
	row, err := g.parent.replay(c.Parent)
	if err != nil {
		return err
	}
	src := row.Get(g.field)
	if src == nil {
		return fmt.Errorf("the owning row has no field %s", g.field)
	}
	*out = *src
	if !g.offset || out.Kind == record.KindNull {
		return nil
	}
	d := g.min + time.Duration(c.Rand.Int64N(int64(g.max-g.min)+1))
	return addOffset(out, g.typ, d)
}

// addOffset moves a rendered temporal value by d.
func addOffset(v *record.Value, t schema.TypeName, d time.Duration) error {
	if v.Kind == record.KindInt { // unix or unix_ms
		// Which one is told apart by magnitude: seconds will not reach 10^11 until year 5138.
		if v.Int() > 100_000_000_000 {
			v.SetInt(v.Int() + d.Milliseconds())
		} else {
			v.SetInt(v.Int() + int64(d/time.Second))
		}
		return nil
	}
	layout := time.RFC3339
	switch t {
	case schema.TypeDate:
		layout = "2006-01-02"
	case schema.TypeTime:
		layout = "15:04:05"
	}
	parsed, err := time.Parse(layout, v.Str)
	if err != nil {
		return err
	}
	v.SetString(parsed.Add(d).Format(layout))
	return nil
}
