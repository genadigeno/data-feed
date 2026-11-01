package gen

import (
	"math"
	"strconv"
	"strings"

	"github.com/genadigeno/data-feed/internal/gen/value"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

const (
	nameAttempts = 8
	// perFieldOverhead counts the quotes, colon and comma around a field.
	perFieldOverhead = 6
	// minValueBytes is the room a new value needs: below this, stop adding values. It
	// covers the longest scalar, a decimal of full precision, so a record ends close to
	// its budget rather than far past it.
	minValueBytes = 48
	nameLetters   = "abcdefghijklmnopqrstuvwxyz"
)

var nameWords = []string{
	"value", "item", "count", "status", "name", "total", "code", "label", "kind", "group", "owner", "price", "weight",
	"score", "level", "rank", "state", "region", "source", "target", "note", "flag", "index", "amount", "title",
	"color", "size", "stage", "phase", "batch",
}

// randomGen produces schemaless objects. All drawing goes through the row's RNG, so
// a record depends only on the seed and its index.
type randomGen struct {
	spec    *schema.RandomSpec
	kinds   []schema.TypeName
	alias   *value.Alias
	scalars []schema.TypeName // kinds without object and array
	sAlias  *value.Alias      // nil if there are none
	bound   map[schema.TypeName]value.Generator
	vocab   value.Sampler
}

func newRandomGen(spec *schema.RandomSpec, reg *value.Registry, env *value.Env) (*randomGen, error) {
	g := &randomGen{spec: spec, bound: map[schema.TypeName]value.Generator{}}

	if spec.Types != nil {
		weights := make([]float64, 0, len(spec.Types))
		var scalarWeights []float64
		for _, w := range spec.Types {
			g.kinds = append(g.kinds, w.Type)
			weights = append(weights, w.Weight)
			if w.Type != schema.TypeObject && w.Type != schema.TypeArray && w.Weight > 0 {
				g.scalars = append(g.scalars, w.Type)
				scalarWeights = append(scalarWeights, w.Weight)
			}
		}
		g.alias = value.NewAlias(weights)
		if len(g.scalars) > 0 {
			g.sAlias = value.NewAlias(scalarWeights)
		}
	} else {
		p := spec.Probabilities
		rest := (1 - p.Null - p.Object - p.Array) / 4
		g.kinds = []schema.TypeName{
			schema.TypeNull, schema.TypeObject, schema.TypeArray,
			schema.TypeString, schema.TypeInteger, schema.TypeFloat, schema.TypeBoolean,
		}
		g.alias = value.NewAlias([]float64{p.Null, p.Object, p.Array, rest, rest, rest, rest})
		g.scalars = []schema.TypeName{schema.TypeString, schema.TypeInteger, schema.TypeFloat, schema.TypeBoolean}
		g.sAlias = value.NewAlias([]float64{1, 1, 1, 1})
	}

	// String, integer and float follow the spec's own ranges; the other scalars are
	// ordinary typed fields.
	for _, t := range g.kinds {
		switch t {
		case schema.TypeString, schema.TypeInteger, schema.TypeFloat, schema.TypeObject, schema.TypeArray:
			continue
		}
		bound, err := reg.Bind(&schema.FieldDef{Type: t}, env)
		if err != nil {
			return nil, err
		}
		g.bound[t] = bound
	}

	if group := spec.FieldNames.VocabularyGroup; group != "" && env.Vocabulary != nil {
		if s, ok := env.Vocabulary.Group(group); ok {
			g.vocab = s
		}
	}
	return g, nil
}

// budget tracks the bytes a record may still use.
type budget struct{ left int64 }

// fill adds random fields to o. Fields whose names are in reserved are avoided.
func (g *randomGen) fill(c *value.Ctx, o *record.Object, limit int64, reserved map[string]bool) error {
	b := &budget{left: limit - o.ApproxSize()}
	return g.fillObject(c, o, 0, b, reserved)
}

func (g *randomGen) fillObject(c *value.Ctx, o *record.Object, level int64, b *budget, reserved map[string]bool) error {
	r := g.spec.Fields
	n := r.Min + c.Rand.Int64N(r.Max-r.Min+1)
	mustNest := level < g.spec.Depth.Min
	if mustNest {
		n = max(n, 1)
	}
	for i := range n {
		if b.left < minValueBytes {
			return nil
		}
		name := g.fieldName(c, o, reserved)
		b.left -= int64(len(name)) + perFieldOverhead

		kind := g.draw(c, level)
		if mustNest && i == 0 {
			kind = schema.TypeObject
		}
		if err := g.value(c, kind, level, b, o.Add(name)); err != nil {
			return err
		}
	}
	return nil
}

// draw picks a type; at the depth limit only scalars can be drawn.
func (g *randomGen) draw(c *value.Ctx, level int64) schema.TypeName {
	kind := g.kinds[g.alias.Sample(c.Rand)]
	if (kind == schema.TypeObject || kind == schema.TypeArray) && level >= g.spec.Depth.Max {
		if g.sAlias == nil {
			return schema.TypeString
		}
		return g.scalars[g.sAlias.Sample(c.Rand)]
	}
	return kind
}

func (g *randomGen) value(c *value.Ctx, kind schema.TypeName, level int64, b *budget, out *record.Value) error {
	switch kind {
	case schema.TypeObject:
		o := c.Arena.Object(int(g.spec.Fields.Max))
		out.SetObject(o)
		b.left -= 2 // the braces
		return g.fillObject(c, o, level+1, b, nil)
	case schema.TypeArray:
		r := g.spec.ArrayLength
		n := r.Min + c.Rand.Int64N(r.Max-r.Min+1)
		// Every element needs at least its separator and some room for a value.
		n = min(n, max(b.left/minValueBytes, 0))
		list := c.Arena.Values(int(n))
		b.left -= 2 // the brackets
		for i := range list {
			if b.left < minValueBytes {
				list = list[:i]
				break
			}
			b.left-- // the comma
			if err := g.value(c, g.draw(c, level+1), level+1, b, &list[i]); err != nil {
				return err
			}
		}
		out.SetArray(list)
		return nil
	case schema.TypeString:
		g.randomString(c, b, out)
		return nil
	case schema.TypeInteger:
		r := g.spec.Integer
		out.SetInt(int64(uint64(r.Min) + c.Rand.Uint64N(uint64(r.Max)-uint64(r.Min)+1)))
	case schema.TypeFloat:
		out.SetFloat(g.randomFloat(c))
	default:
		if err := g.bound[kind].Generate(c, out); err != nil {
			return err
		}
	}
	b.left -= out.ApproxSize()
	return nil
}

func (g *randomGen) randomString(c *value.Ctx, b *budget, out *record.Value) {
	r := g.spec.StringLength
	n := r.Min + c.Rand.Int64N(r.Max-r.Min+1)
	n = min(n, max(b.left-2, 0)) // the budget outranks the requested length
	buf := c.Scratch[:0]
	for range n {
		buf = append(buf, nameLetters[c.Rand.IntN(len(nameLetters))])
	}
	out.SetString(string(buf))
	c.Scratch = buf[:0]
	b.left -= n + 2
}

func (g *randomGen) randomFloat(c *value.Ctx) float64 {
	r := g.spec.Float
	x := r.Min + c.Rand.Float64()*(r.Max-r.Min)
	if r.Precision > 0 {
		scale := math.Pow10(r.Precision)
		x = min(max(math.Round(x*scale)/scale, r.Min), r.Max)
	}
	return x
}

func (g *randomGen) fieldName(c *value.Ctx, o *record.Object, reserved map[string]bool) string {
	taken := func(name string) bool { return reserved[name] || o.Get(name) != nil }
	if !g.spec.FieldNames.UniquePerObject && reserved == nil {
		return g.drawName(c)
	}
	var name string
	for range nameAttempts {
		name = g.drawName(c)
		if !taken(name) {
			return name
		}
	}
	// A narrow name space: a suffix makes it distinct.
	base := name
	for i := 2; ; i++ {
		name = base + "_" + strconv.Itoa(i)
		if !taken(name) {
			return name
		}
	}
}

func (g *randomGen) drawName(c *value.Ctx) string {
	fn := g.spec.FieldNames
	var name string
	switch {
	case g.vocab != nil:
		name = g.vocab.Sample(c.Rand)
	case fn.Style == "random":
		n := fn.MinLen + c.Rand.IntN(fn.MaxLen-fn.MinLen+1)
		var b strings.Builder
		for range n {
			b.WriteByte(nameLetters[c.Rand.IntN(len(nameLetters))])
		}
		return b.String()
	case fn.Style == "camel":
		second := nameWords[c.Rand.IntN(len(nameWords))]
		name = nameWords[c.Rand.IntN(len(nameWords))] + strings.ToUpper(second[:1]) + second[1:]
	case fn.Style == "snake":
		name = nameWords[c.Rand.IntN(len(nameWords))] + "_" + nameWords[c.Rand.IntN(len(nameWords))]
	default:
		name = nameWords[c.Rand.IntN(len(nameWords))]
	}
	if len(name) > fn.MaxLen {
		name = name[:fn.MaxLen]
	}
	for len(name) < fn.MinLen {
		name += string(nameLetters[c.Rand.IntN(len(nameLetters))])
	}
	return name
}
