package value

import (
	"strconv"

	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
)

const (
	defaultCollectionLow = 0
	// mapKeyAttempts bounds redraws of a repeated map key before a suffix makes it distinct.
	mapKeyAttempts = 4
)

// ObjectField is one member of an object generator.
type ObjectField struct {
	Name     string
	Optional bool
	P        float64 // presence probability when Optional
	Gen      Generator
}

// ObjectGen produces an object field by field, in order. It is the shared core of nested
// objects and hybrid records.
type ObjectGen struct{ Fields []ObjectField }

// BindFields builds generators for fields, in order.
func (r *Registry) BindFields(fields []schema.FieldDef, env *Env) ([]ObjectField, error) {
	out := make([]ObjectField, len(fields))
	for i := range fields {
		f := &fields[i]
		g, err := r.Bind(f, env)
		if err != nil {
			return nil, &FieldError{Name: f.Name, Err: err}
		}
		out[i] = ObjectField{Name: f.Name, Optional: f.Presence.Optional, P: f.Presence.Probability, Gen: g}
	}
	return out, nil
}

// FieldError says which field a bind failed on; nesting builds up the path.
type FieldError struct {
	Name string
	Err  error
}

func (e *FieldError) Error() string {
	if inner, ok := e.Err.(*FieldError); ok {
		return e.Name + "." + inner.Error()
	}
	return e.Name + ": " + e.Err.Error()
}

func (e *FieldError) Unwrap() error { return e.Err }

func (r *Registry) newObjectGen(fields []schema.FieldDef, env *Env) (Generator, error) {
	bound, err := r.BindFields(fields, env)
	if err != nil {
		return nil, err
	}
	return &ObjectGen{Fields: bound}, nil
}

// Fill generates the fields into o.
func (g *ObjectGen) Fill(c *Ctx, o *record.Object) error {
	for i := range g.Fields {
		f := &g.Fields[i]
		if f.Optional && c.Rand.Float64() >= f.P {
			continue
		}
		if err := f.Gen.Generate(c, o.Add(f.Name)); err != nil {
			return &FieldError{Name: f.Name, Err: err}
		}
	}
	return nil
}

// Generate implements Generator for nested objects.
func (g *ObjectGen) Generate(c *Ctx, out *record.Value) error {
	o := c.Arena.Object(len(g.Fields))
	if err := g.Fill(c, o); err != nil {
		return err
	}
	out.SetObject(o)
	return nil
}

// collectionLength draws within the declared bounds; the upper default matches the
// schema's sizing assumption.
func collectionBounds(f *schema.FieldDef) (lo, hi int64) {
	lo, hi = defaultCollectionLow, schema.DefaultCollectionMax
	if f.MinLength != nil {
		lo = *f.MinLength
		hi = max(hi, lo)
	}
	if f.MaxLength != nil {
		hi = *f.MaxLength
	}
	return lo, hi
}

type arrayGen struct {
	lo, hi int64
	item   Generator
}

func (r *Registry) newArrayGen(f *schema.FieldDef, env *Env) (Generator, error) {
	item, err := r.Bind(f.Items, env)
	if err != nil {
		return nil, &FieldError{Name: "items", Err: err}
	}
	lo, hi := collectionBounds(f)
	return &arrayGen{lo: lo, hi: hi, item: item}, nil
}

func (g *arrayGen) Generate(c *Ctx, out *record.Value) error {
	n := g.lo + c.Rand.Int64N(g.hi-g.lo+1)
	list := c.Arena.Values(int(n))
	for i := range list {
		if err := g.item.Generate(c, &list[i]); err != nil {
			return err
		}
	}
	out.SetArray(list)
	return nil
}

type mapGen struct {
	lo, hi int64
	key    Generator
	value  Generator
}

func (r *Registry) newMapGen(f *schema.FieldDef, env *Env) (Generator, error) {
	value, err := r.Bind(f.MapValue, env)
	if err != nil {
		return nil, &FieldError{Name: "value", Err: err}
	}
	keyDef := f.MapKey
	if keyDef == nil {
		low, high := int64(4), int64(12)
		keyDef = &schema.FieldDef{Type: schema.TypeString, MinLength: &low, MaxLength: &high}
	}
	key, err := r.Bind(keyDef, env)
	if err != nil {
		return nil, &FieldError{Name: "key", Err: err}
	}
	lo, hi := collectionBounds(f)
	return &mapGen{lo: lo, hi: hi, key: key, value: value}, nil
}

func (g *mapGen) Generate(c *Ctx, out *record.Value) error {
	n := g.lo + c.Rand.Int64N(g.hi-g.lo+1)
	o := c.Arena.Object(int(n))
	var v record.Value
	for i := range n {
		name, err := g.distinctKey(c, o, int(i), &v)
		if err != nil {
			return err
		}
		if err := g.value.Generate(c, o.Add(name)); err != nil {
			return err
		}
	}
	out.SetObject(o)
	return nil
}

// distinctKey draws a key not yet used in this map, because duplicate keys would make
// invalid or lossy JSON.
func (g *mapGen) distinctKey(c *Ctx, o *record.Object, i int, v *record.Value) (string, error) {
	var name string
	for range mapKeyAttempts {
		if err := g.key.Generate(c, v); err != nil {
			return "", err
		}
		name = keyText(v)
		if o.Get(name) == nil {
			return name, nil
		}
	}
	// A small key space, such as a short enum: the index makes it distinct.
	return name + "_" + strconv.Itoa(i), nil
}

func keyText(v *record.Value) string {
	switch v.Kind {
	case record.KindString, record.KindDecimal:
		return v.Str
	case record.KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case record.KindBool:
		return strconv.FormatBool(v.Bool())
	}
	return "null"
}
