// Package value turns field definitions into generators. Every top-level concern is a
// small wrapper: nullability, defaults and uniqueness compose around a base generator
// chosen by type. See docs/02-domain/schema-model.md.
package value

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"time"

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

// DefaultNullProbability applies to nullable fields that do not set one.
const DefaultNullProbability = 0.1

// Ctx is what a generator sees for one record. It belongs to one goroutine.
type Ctx struct {
	Rand    *rand.Rand
	Index   uint64 // the record's index in its stream
	Ordinal uint64 // 1-based position among its siblings, for owned rows
	Parent  uint64 // the owning row's index, for owned rows
	RefTime time.Time
	Arena   *record.Arena
	Scratch []byte // reusable buffer for building strings
}

// Generator produces one value.
type Generator interface {
	Generate(c *Ctx, out *record.Value) error
}

// UniqueGenerator can produce a value that is an injective function of a position.
type UniqueGenerator interface {
	Generator
	// UniqueSpace returns how many positions exist, or 0 for the full 64-bit space.
	UniqueSpace() uint64
	// At produces the value at position p, which is below UniqueSpace.
	At(c *Ctx, p uint64, out *record.Value) error
}

// sequential marks a UniqueGenerator whose positions should be used in order rather
// than scrambled, because ascending output is its point.
type sequential interface{ sequential() }

// Vocabulary supplies catalog values. Generation works without one.
type Vocabulary interface {
	Group(name string) (Sampler, bool)
}

// Sampler draws from one vocabulary group.
type Sampler interface {
	Sample(r *rand.Rand) string
	Len() int
	At(i int) string
}

// Env holds what binding a field needs. Each slot binds its own generators, so they
// may keep state and hold Rand.
type Env struct {
	Rand        *rand.Rand
	Key         unique.Key // the stream's key; fields derive theirs from it
	RefTime     time.Time
	Definitions map[string]*schema.Definition
	Vocabulary  Vocabulary
	// Rows is how many positions a unique field must cover, or 0 if not known yet.
	Rows uint64
}

// Factory builds a named generator, for use through `generator:` on a field.
type Factory func(f *schema.FieldDef, env *Env) (Generator, error)

// Registry maps generator names to factories.
type Registry struct{ factories map[string]Factory }

// NewRegistry returns the built-in generators.
func NewRegistry() *Registry {
	r := &Registry{factories: map[string]Factory{}}
	r.mustRegister("sequence", newSequenceFactory)
	r.mustRegister("vocab", newVocabFactory)
	r.mustRegister(schema.GeneratorOrdinal, func(*schema.FieldDef, *Env) (Generator, error) { return ordinalGen{}, nil })
	return r
}

func (r *Registry) mustRegister(name string, f Factory) {
	if err := r.Register(name, f); err != nil {
		panic(err)
	}
}

// Register adds a generator; reusing a name is an error.
func (r *Registry) Register(name string, f Factory) error {
	if _, ok := r.factories[name]; ok {
		return fmt.Errorf("generator %q is already registered", name)
	}
	r.factories[name] = f
	return nil
}

// Bind builds the full generator for a field, including its wrappers. Only top-level
// fields reach here with Unique set: schema validation rejects it elsewhere.
func (r *Registry) Bind(f *schema.FieldDef, env *Env) (Generator, error) {
	if f.Unique {
		g, err := r.BindKey(f, env)
		if err != nil {
			return nil, err
		}
		return wrapNullable(f, g), nil
	}
	base, err := r.base(f, env)
	if err != nil {
		return nil, err
	}
	g := base
	if f.Default != nil {
		lit, err := literalValue(f, *f.Default)
		if err != nil {
			return nil, err
		}
		p := 1.0
		if f.DefaultProbability != nil {
			p = *f.DefaultProbability
		}
		g = &defaultGen{inner: g, value: lit, p: p}
	}
	return wrapNullable(f, g), nil
}

func wrapNullable(f *schema.FieldDef, g Generator) Generator {
	if !f.Nullable {
		return g
	}
	p := DefaultNullProbability
	if f.NullProbability != nil {
		p = *f.NullProbability
	}
	return &nullableGen{inner: g, p: p}
}

// KeyGenerator computes a unique field's value for any row, without generating that
// row. References use it, so a child can name a parent that is not stored anywhere.
type KeyGenerator interface {
	Generator
	KeyAt(c *Ctx, index uint64, out *record.Value) error
}

// BindKey builds the unique generator for a key field, with no nullable wrapper. Two
// binds of one field in one stream give identical values, because the field's key, not
// the row's random sequence, drives them.
func (r *Registry) BindKey(f *schema.FieldDef, env *Env) (KeyGenerator, error) {
	base, err := r.base(f, env)
	if err != nil {
		return nil, err
	}
	u, ok := base.(UniqueGenerator)
	if !ok {
		return nil, fmt.Errorf("type %s cannot produce guaranteed unique values", f.Type)
	}
	space := u.UniqueSpace()
	if space != 0 && env.Rows > space {
		return nil, fmt.Errorf("only %d distinct values are possible, but up to %d rows need one each", space, env.Rows)
	}

	key := env.Key.Derive("field\x00" + f.Name)
	pcg := rand.NewPCG(0, 0)
	g := &uniqueGen{inner: u, space: space, key: key.K0, pcg: pcg, rnd: rand.New(pcg)}
	if _, ordered := base.(sequential); !ordered {
		g.perm = unique.NewPermutation(key, space)
	}
	return g, nil
}

// base picks the generator that produces plain values.
func (r *Registry) base(f *schema.FieldDef, env *Env) (Generator, error) {
	if f.Generator != nil {
		factory, ok := r.factories[f.Generator.Name]
		if !ok {
			return nil, fmt.Errorf("unknown generator %q", f.Generator.Name)
		}
		return factory(f, env)
	}
	if len(f.Values) > 0 {
		return newValuesGen(f)
	}
	if env.Vocabulary != nil {
		// A catalog group named after a semantic type replaces its built-in list.
		if s, ok := env.Vocabulary.Group(string(f.Type)); ok && isTextType(f.Type) {
			return &vocabGen{sampler: s}, nil
		}
	}
	return r.byType(f, env)
}

func isTextType(t schema.TypeName) bool {
	switch t {
	case schema.TypeCountry, schema.TypeCity, schema.TypeFirstName, schema.TypeLastName, schema.TypeFullName,
		schema.TypeCompanyName, schema.TypeAddress, schema.TypeCurrency, schema.TypePaymentStatus, schema.TypeOrderStatus:
		return true
	}
	return false
}

func (r *Registry) byType(f *schema.FieldDef, env *Env) (Generator, error) {
	switch f.Type {
	case schema.TypeString:
		return newStringGen(f)
	case schema.TypeInteger, schema.TypeLong:
		return newIntGen(f, env)
	case schema.TypeFloat:
		return newFloatGen(f)
	case schema.TypeDecimal, schema.TypeMoney:
		return newDecimalGen(f)
	case schema.TypeBoolean:
		return boolGen{}, nil
	case schema.TypeNull:
		return nullGen{}, nil
	case schema.TypeUUID:
		return uuidGen{}, nil
	case schema.TypeDate, schema.TypeTime, schema.TypeDateTime, schema.TypeTimestamp:
		return newTemporalGen(f, env)
	case schema.TypeBinary:
		return newBinaryGen(f)
	case schema.TypeEmail:
		return emailGen{}, nil
	case schema.TypeURL:
		return urlGen{}, nil
	case schema.TypePhone:
		return phoneGen{}, nil
	case schema.TypeIPv4:
		return ipv4Gen{any: f.Ranges == "any"}, nil
	case schema.TypeIPv6, schema.TypeIPAddress:
		return ipv6Gen{any: f.Ranges == "any", mixed: f.Type == schema.TypeIPAddress}, nil
	case schema.TypeCountry:
		return listGen(countries), nil
	case schema.TypeCity:
		return listGen(cities), nil
	case schema.TypePostalCode:
		return postalGen{}, nil
	case schema.TypeFirstName:
		return listGen(firstNames), nil
	case schema.TypeLastName:
		return listGen(lastNames), nil
	case schema.TypeFullName:
		return fullNameGen{}, nil
	case schema.TypeCompanyName:
		return companyGen{}, nil
	case schema.TypeAddress:
		return addressGen{}, nil
	case schema.TypeCurrency:
		return listGen(currencies), nil
	case schema.TypeCreditCard:
		return listGen(testCardNumbers), nil
	case schema.TypePaymentStatus:
		return listGen(paymentStatuses), nil
	case schema.TypeOrderStatus:
		return listGen(orderStatuses), nil
	case schema.TypeObject:
		return r.newObjectGen(f.Fields, env)
	case schema.TypeNestedSchema:
		d := env.Definitions[f.Schema]
		if d == nil {
			return nil, fmt.Errorf("no definition named %q", f.Schema)
		}
		return r.newObjectGen(d.Fields, env)
	case schema.TypeArray:
		return r.newArrayGen(f, env)
	case schema.TypeMap:
		return r.newMapGen(f, env)
	}
	return nil, fmt.Errorf("type %s has no generator here", f.Type)
}

// uniqueGen draws from its own random sequence, keyed by field and row, so a key field
// can be recomputed for any row without replaying the rest of that row.
type uniqueGen struct {
	inner UniqueGenerator
	perm  *unique.Permutation // nil when positions are used in order
	space uint64
	key   uint64
	pcg   *rand.PCG
	rnd   *rand.Rand
	sub   Ctx
}

func (g *uniqueGen) Generate(c *Ctx, out *record.Value) error { return g.KeyAt(c, c.Index, out) }

func (g *uniqueGen) KeyAt(c *Ctx, index uint64, out *record.Value) error {
	if g.space != 0 && index >= g.space {
		return fmt.Errorf("unique values exhausted: row %d is beyond the %d distinct values possible", index, g.space)
	}
	p := index
	if g.perm != nil {
		p = g.perm.Apply(index)
	}
	g.pcg.Seed(g.key, unique.Mix(index))
	g.sub = *c
	g.sub.Rand = g.rnd
	g.sub.Index = index
	return g.inner.At(&g.sub, p, out)
}

type nullableGen struct {
	inner Generator
	p     float64
}

func (g *nullableGen) Generate(c *Ctx, out *record.Value) error {
	if c.Rand.Float64() < g.p {
		out.SetNull()
		return nil
	}
	return g.inner.Generate(c, out)
}

type defaultGen struct {
	inner Generator
	value record.Value
	p     float64
}

func (g *defaultGen) Generate(c *Ctx, out *record.Value) error {
	if c.Rand.Float64() < g.p {
		*out = g.value
		return nil
	}
	return g.inner.Generate(c, out)
}

// literalValue converts a validated literal to the value a generator of that type
// would produce.
func literalValue(f *schema.FieldDef, l schema.Literal) (record.Value, error) {
	var v record.Value
	switch l.Kind {
	case schema.LiteralNull:
		return v, nil
	case schema.LiteralBool:
		v.SetBool(l.Text == "true")
		return v, nil
	}
	switch f.Type {
	case schema.TypeInteger, schema.TypeLong:
		r, ok := new(big.Rat).SetString(l.Text)
		if !ok || !r.IsInt() || !r.Num().IsInt64() {
			return v, fmt.Errorf("%q is not an integer", l.Text)
		}
		v.SetInt(r.Num().Int64())
	case schema.TypeFloat:
		r, ok := new(big.Rat).SetString(l.Text)
		if !ok {
			return v, fmt.Errorf("%q is not a number", l.Text)
		}
		x, _ := r.Float64()
		v.SetFloat(x)
	case schema.TypeDecimal, schema.TypeMoney:
		r, ok := new(big.Rat).SetString(l.Text)
		if !ok {
			return v, fmt.Errorf("%q is not a number", l.Text)
		}
		v.SetDecimal(r.FloatString(decimalScale(f)))
	case schema.TypeTimestamp:
		if f.Format == "unix" || f.Format == "unix_ms" {
			t, err := time.Parse(time.RFC3339Nano, l.Text)
			if err != nil {
				return v, err
			}
			if f.Format == "unix" {
				v.SetInt(t.Unix())
			} else {
				v.SetInt(t.UnixMilli())
			}
			return v, nil
		}
		v.SetString(l.Text)
	default:
		if l.Kind == schema.LiteralNumber && f.Type == schema.TypeEnum {
			r, ok := new(big.Rat).SetString(l.Text)
			if ok && r.IsInt() && r.Num().IsInt64() {
				v.SetInt(r.Num().Int64())
				return v, nil
			}
			v.SetDecimal(l.Text)
			return v, nil
		}
		v.SetString(l.Text)
	}
	return v, nil
}

// Alias draws an index with probability proportional to its weight, in constant time
// (Vose's alias method).
type Alias struct {
	prob  []float64
	alias []int
}

// NewAlias builds the table. Weights must not be negative, and one must be positive.
func NewAlias(weights []float64) *Alias {
	n := len(weights)
	a := &Alias{prob: make([]float64, n), alias: make([]int, n)}
	var total float64
	for _, w := range weights {
		total += w
	}
	scaled := make([]float64, n)
	var small, large []int
	for i, w := range weights {
		scaled[i] = w / total * float64(n)
		if scaled[i] < 1 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}
	for len(small) > 0 && len(large) > 0 {
		s, l := small[len(small)-1], large[len(large)-1]
		small = small[:len(small)-1]
		a.prob[s], a.alias[s] = scaled[s], l
		scaled[l] += scaled[s] - 1
		if scaled[l] < 1 {
			large = large[:len(large)-1]
			small = append(small, l)
		}
	}
	// Whatever is left is 1 up to rounding error.
	for _, i := range large {
		a.prob[i] = 1
	}
	for _, i := range small {
		a.prob[i] = 1
	}
	return a
}

// Sample draws an index.
func (a *Alias) Sample(r *rand.Rand) int {
	i := r.IntN(len(a.prob))
	if r.Float64() < a.prob[i] {
		return i
	}
	return a.alias[i]
}

// valuesGen draws from a declared list.
type valuesGen struct {
	values []record.Value
	alias  *Alias // nil when all are equally likely
}

func newValuesGen(f *schema.FieldDef) (Generator, error) {
	g := &valuesGen{values: make([]record.Value, len(f.Values))}
	for i, l := range f.Values {
		v, err := literalValue(f, l)
		if err != nil {
			return nil, err
		}
		g.values[i] = v
	}
	if f.Weights != nil {
		g.alias = NewAlias(f.Weights)
	}
	return g, nil
}

func (g *valuesGen) Generate(c *Ctx, out *record.Value) error {
	if g.alias != nil {
		*out = g.values[g.alias.Sample(c.Rand)]
		return nil
	}
	*out = g.values[c.Rand.IntN(len(g.values))]
	return nil
}

func (g *valuesGen) UniqueSpace() uint64 { return uint64(len(g.values)) }

func (g *valuesGen) At(_ *Ctx, p uint64, out *record.Value) error {
	*out = g.values[p]
	return nil
}

// vocabGen draws from a catalog group.
type vocabGen struct{ sampler Sampler }

func newVocabFactory(f *schema.FieldDef, env *Env) (Generator, error) {
	var group string
	for _, p := range f.Generator.Params {
		l, ok := p.Value.(schema.Literal)
		if p.Name != "group" || !ok {
			return nil, fmt.Errorf("vocab generator: unknown parameter %q", p.Name)
		}
		group = l.Text
	}
	if group == "" {
		return nil, fmt.Errorf("vocab generator needs a group parameter")
	}
	if env.Vocabulary == nil {
		// Without a catalog, plain text keeps schemas usable, as generation must work
		// with no vocabulary at all.
		return newStringGen(&schema.FieldDef{Type: schema.TypeString})
	}
	s, ok := env.Vocabulary.Group(group)
	if !ok {
		return nil, fmt.Errorf("the selected catalog has no group %q", group)
	}
	return &vocabGen{sampler: s}, nil
}

func (g *vocabGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(g.sampler.Sample(c.Rand))
	return nil
}

func (g *vocabGen) UniqueSpace() uint64 { return uint64(g.sampler.Len()) }

func (g *vocabGen) At(_ *Ctx, p uint64, out *record.Value) error {
	out.SetString(g.sampler.At(int(p)))
	return nil
}

// ordinalGen emits a child row's 1-based position among its siblings.
type ordinalGen struct{}

func (ordinalGen) Generate(c *Ctx, out *record.Value) error {
	out.SetInt(int64(c.Ordinal))
	return nil
}

// sequenceGen emits start, start+step, … from the record index.
type sequenceGen struct{ start, step int64 }

func newSequenceFactory(f *schema.FieldDef, _ *Env) (Generator, error) {
	g := &sequenceGen{start: 0, step: 1}
	for _, p := range f.Generator.Params {
		l, ok := p.Value.(schema.Literal)
		if !ok {
			return nil, fmt.Errorf("sequence generator: %q must be a number", p.Name)
		}
		r, ok := new(big.Rat).SetString(l.Text)
		if l.Kind != schema.LiteralNumber || !ok || !r.IsInt() || !r.Num().IsInt64() {
			return nil, fmt.Errorf("sequence generator: %q must be an integer", p.Name)
		}
		switch p.Name {
		case "start":
			g.start = r.Num().Int64()
		case "step":
			g.step = r.Num().Int64()
			if g.step <= 0 {
				return nil, fmt.Errorf("sequence generator: step must be greater than zero")
			}
		default:
			return nil, fmt.Errorf("sequence generator: unknown parameter %q", p.Name)
		}
	}
	return g, nil
}

func (*sequenceGen) sequential() {}

func (g *sequenceGen) Generate(c *Ctx, out *record.Value) error { return g.At(c, c.Index, out) }

func (g *sequenceGen) UniqueSpace() uint64 { return 0 }

func (g *sequenceGen) At(_ *Ctx, p uint64, out *record.Value) error {
	out.SetInt(g.start + int64(p)*g.step)
	return nil
}
