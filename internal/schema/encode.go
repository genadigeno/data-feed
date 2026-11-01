package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// EncodeJSON renders the schema as canonical JSON: equal schemas give equal bytes,
// and field order is preserved.
func EncodeJSON(s *Schema) []byte {
	var b strings.Builder
	writeJSON(&b, s.tree())
	return []byte(b.String())
}

// EncodeYAML renders the schema as YAML for people to read and edit.
func EncodeYAML(s *Schema) ([]byte, error) {
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(toYAML(s.tree())); err != nil {
		return nil, fmt.Errorf("encode schema: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("encode schema: %w", err)
	}
	return b.Bytes(), nil
}

// Hash returns the SHA-256 of the canonical encoding.
func Hash(s *Schema) string {
	sum := sha256.Sum256(EncodeJSON(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// mapBuilder assembles a mapping in a fixed key order.
type mapBuilder struct{ n *node }

func newMap() *mapBuilder { return &mapBuilder{&node{kind: nodeMap}} }

func (m *mapBuilder) set(key string, value *node) {
	m.n.keys = append(m.n.keys, key)
	m.n.items = append(m.n.items, value)
}

func (m *mapBuilder) str(key, v string) {
	if v != "" {
		m.set(key, strNode(v))
	}
}

func (m *mapBuilder) flag(key string, v bool) {
	if v {
		m.set(key, boolNode(true))
	}
}

func (m *mapBuilder) optInt(key string, v *int) {
	if v != nil {
		m.set(key, intNode(int64(*v)))
	}
}

func (m *mapBuilder) optInt64(key string, v *int64) {
	if v != nil {
		m.set(key, intNode(*v))
	}
}

func (m *mapBuilder) optFloat(key string, v *float64) {
	if v != nil {
		m.set(key, floatNode(*v))
	}
}

func (m *mapBuilder) literal(key string, v *Literal) {
	if v != nil {
		m.set(key, literalNode(*v))
	}
}

func strNode(v string) *node { return &node{kind: nodeString, text: v} }
func boolNode(v bool) *node  { return &node{kind: nodeBool, text: strconv.FormatBool(v)} }
func intNode(v int64) *node  { return &node{kind: nodeNumber, text: strconv.FormatInt(v, 10)} }
func floatNode(v float64) *node {
	return &node{kind: nodeNumber, text: strconv.FormatFloat(v, 'g', -1, 64)}
}

func literalNode(l Literal) *node {
	switch l.Kind {
	case LiteralBool:
		return &node{kind: nodeBool, text: l.Text}
	case LiteralNumber:
		return &node{kind: nodeNumber, text: l.Text}
	case LiteralString:
		return strNode(l.Text)
	}
	return &node{kind: nodeNull}
}

func (s *Schema) tree() *node {
	m := newMap()
	m.set("schema_version", intNode(int64(s.Version)))
	m.str("kind", string(s.Kind))
	if len(s.Definitions) > 0 {
		defs := newMap()
		for _, d := range s.Definitions {
			one := newMap()
			one.set("fields", fieldsTree(d.Fields))
			defs.set(d.Name, one.n)
		}
		m.set("definitions", defs.n)
	}
	switch {
	case s.Random != nil:
		randomInto(m, s.Random)
	case s.Hybrid != nil:
		m.set("fields", fieldsTree(s.Hybrid.Fields))
		if e := s.Hybrid.ExtraRandom; e != nil {
			extra := newMap()
			extra.set("probability", floatNode(e.Probability))
			spec := newMap()
			randomInto(spec, &e.Spec)
			extra.set("spec", spec.n)
			m.set("extra_random", extra.n)
		}
	case s.Relational != nil:
		tables := newMap()
		for i := range s.Relational.Tables {
			tables.set(s.Relational.Tables[i].Name, tableTree(&s.Relational.Tables[i]))
		}
		m.set("tables", tables.n)
	}
	return m.n
}

func rangeNode(r IntRange) *node {
	m := newMap()
	m.set("min", intNode(r.Min))
	m.set("max", intNode(r.Max))
	return m.n
}

func randomInto(m *mapBuilder, r *RandomSpec) {
	m.set("fields", rangeNode(r.Fields))
	m.set("depth", rangeNode(r.Depth))
	m.set("array_length", rangeNode(r.ArrayLength))
	m.set("string_length", rangeNode(r.StringLength))
	m.set("integer", rangeNode(r.Integer))

	f := newMap()
	f.set("min", floatNode(r.Float.Min))
	f.set("max", floatNode(r.Float.Max))
	f.set("precision", intNode(int64(r.Float.Precision)))
	m.set("float", f.n)

	if r.Types != nil {
		t := newMap()
		for _, w := range r.Types {
			t.set(string(w.Type), floatNode(w.Weight))
		}
		m.set("types", t.n)
	}
	if p := r.Probabilities; p != nil {
		pm := newMap()
		pm.set("null", floatNode(p.Null))
		pm.set("object", floatNode(p.Object))
		pm.set("array", floatNode(p.Array))
		m.set("probabilities", pm.n)
	}

	names := newMap()
	names.str("style", r.FieldNames.Style)
	names.str("vocabulary_group", r.FieldNames.VocabularyGroup)
	names.set("min_len", intNode(int64(r.FieldNames.MinLen)))
	names.set("max_len", intNode(int64(r.FieldNames.MaxLen)))
	names.set("unique_per_object", boolNode(r.FieldNames.UniquePerObject))
	m.set("field_names", names.n)

	m.set("max_record_bytes", intNode(r.MaxRecordBytes))
}

func tableTree(t *Table) *node {
	m := newMap()
	m.optInt64("count", t.Count)
	m.str("primary_key", t.PrimaryKey)
	if o := t.Owner; o != nil {
		owner := newMap()
		owner.str("ref", o.Table)
		pp := newMap()
		pp.set("min", intNode(o.PerParent.Min))
		pp.set("max", intNode(o.PerParent.Max))
		flatDistributionInto(pp, o.PerParent.Distribution)
		owner.set("per_parent", pp.n)
		m.set("owner", owner.n)
	}
	m.set("fields", fieldsTree(t.Fields))
	if len(t.UniqueKeys) > 0 {
		list := &node{kind: nodeList}
		for _, k := range t.UniqueKeys {
			key := newMap()
			names := &node{kind: nodeList}
			for _, f := range k.Fields {
				names.items = append(names.items, strNode(f))
			}
			key.set("fields", names)
			key.str("driver", k.Driver)
			list.items = append(list.items, key.n)
		}
		m.set("unique_keys", list)
	}
	return m.n
}

func fieldsTree(fields []FieldDef) *node {
	m := newMap()
	for i := range fields {
		m.set(fields[i].Name, fieldTree(&fields[i]))
	}
	return m.n
}

func fieldTree(f *FieldDef) *node {
	m := newMap()
	m.str("type", string(f.Type))
	if f.Presence.Optional {
		p := newMap()
		p.set("optional", floatNode(f.Presence.Probability))
		m.set("presence", p.n)
	}
	m.flag("nullable", f.Nullable)
	m.optFloat("null_probability", f.NullProbability)
	m.flag("unique", f.Unique)
	m.literal("default", f.Default)
	m.optFloat("default_probability", f.DefaultProbability)
	m.literal("min", f.Min)
	m.literal("max", f.Max)
	m.optInt64("min_length", f.MinLength)
	m.optInt64("max_length", f.MaxLength)
	m.str("pattern", f.Pattern)
	if f.Values != nil {
		values := &node{kind: nodeList}
		for _, v := range f.Values {
			values.items = append(values.items, literalNode(v))
		}
		m.set("values", values)
	}
	if f.Weights != nil {
		weights := &node{kind: nodeList}
		for _, w := range f.Weights {
			weights.items = append(weights.items, floatNode(w))
		}
		m.set("weights", weights)
	}
	if f.Distribution != nil {
		d := newMap()
		d.str("type", f.Distribution.Type)
		distributionParamsInto(d, f.Distribution)
		m.set("distribution", d.n)
	}
	m.optInt("precision", f.Precision)
	m.optInt("scale", f.Scale)
	m.str("format", f.Format)
	m.str("zone", f.Zone)
	m.str("ranges", f.Ranges)
	if g := f.Generator; g != nil {
		gen := newMap()
		gen.str("name", g.Name)
		if len(g.Params) > 0 {
			params := newMap()
			for _, p := range g.Params {
				switch v := p.Value.(type) {
				case Literal:
					params.set(p.Name, literalNode(v))
				case []Literal:
					list := &node{kind: nodeList}
					for _, l := range v {
						list.items = append(list.items, literalNode(l))
					}
					params.set(p.Name, list)
				}
			}
			gen.set("params", params.n)
		}
		m.set("generator", gen.n)
	}
	if f.Fields != nil {
		m.set("fields", fieldsTree(f.Fields))
	}
	if f.Items != nil {
		m.set("items", fieldTree(f.Items))
	}
	if f.MapKey != nil {
		m.set("key", fieldTree(f.MapKey))
	}
	if f.MapValue != nil {
		m.set("value", fieldTree(f.MapValue))
	}
	m.str("schema", f.Schema)
	if r := f.Ref; r != nil {
		m.set("ref", strNode(r.Table+"."+r.Field))
		m.flag("owner", r.Owner)
		if s := r.Select; s != nil {
			sel := newMap()
			flatDistributionInto(sel, s.Distribution)
			sel.str("mode", s.Mode)
			sel.flag("distinct", s.Distinct)
			m.set("select", sel.n)
		}
	}
	if d := f.Derive; d != nil {
		dm := newMap()
		dm.str("from", d.From)
		dm.str("field", d.Field)
		if d.OffsetMin != nil || d.OffsetMax != nil {
			off := newMap()
			if d.OffsetMin != nil {
				off.str("min", d.OffsetMin.String())
			}
			if d.OffsetMax != nil {
				off.str("max", d.OffsetMax.String())
			}
			dm.set("offset", off.n)
		}
		m.set("derive", dm.n)
	}
	return m.n
}

func flatDistributionInto(m *mapBuilder, d *Distribution) {
	if d == nil {
		return
	}
	m.str("distribution", d.Type)
	distributionParamsInto(m, d)
}

func distributionParamsInto(m *mapBuilder, d *Distribution) {
	m.optFloat("mean", d.Mean)
	m.optFloat("stddev", d.StdDev)
	m.optFloat("rate", d.Rate)
	m.optFloat("s", d.S)
	m.optFloat("v", d.V)
	m.optInt64("start", d.Start)
	m.optInt64("step", d.Step)
}
