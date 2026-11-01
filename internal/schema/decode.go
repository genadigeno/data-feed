package schema

import (
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Decoding problem codes.
const (
	CodeSyntax        = "ERR_SCHEMA_SYNTAX"
	CodeUnknownKey    = "ERR_UNKNOWN_KEY"
	CodeWrongType     = "ERR_WRONG_TYPE"
	CodeMissingKey    = "ERR_MISSING_KEY"
	CodeKeyNotAllowed = "ERR_KEY_NOT_ALLOWED"
	CodeBadValue      = "ERR_BAD_VALUE"
)

// Decode parses a schema document. It checks the document's shape; Validate checks
// its meaning. All problems found are returned together.
func Decode(data []byte, format Format) (*Schema, Problems) {
	root, err := parseTree(data, format)
	if err != nil {
		return nil, Problems{{Code: CodeSyntax, Message: err.Error()}}
	}
	d := &decoder{}
	s := d.schema(root)
	if len(d.problems) > 0 {
		return nil, d.problems
	}
	return s, nil
}

type decoder struct{ problems Problems }

// fieldContext says which keys a field may use where it appears.
type fieldContext struct {
	presence bool // hybrid fields
	column   bool // relational columns: ref, owner, select, derive, unique
}

func join(path, key string) string {
	if key == "" {
		return path
	}
	if path == "" {
		return key
	}
	return path + "." + key
}

// object reads a mapping and remembers which keys were used, so what is left over
// can be reported: a typo must not silently fall back to a default.
type object struct {
	d    *decoder
	n    *node
	path string
	used map[string]bool
}

func (d *decoder) object(n *node, path string) *object {
	if n.kind != nodeMap {
		d.problems.add(path, CodeWrongType, "want a mapping, found a %s", n.kind)
		return &object{d: d, n: &node{kind: nodeMap}, path: path, used: map[string]bool{}}
	}
	return &object{d: d, n: n, path: path, used: map[string]bool{}}
}

func (o *object) take(key string) *node {
	o.used[key] = true
	return o.n.get(key)
}

func (o *object) done() {
	for _, k := range o.n.keys {
		if !o.used[k] {
			o.d.problems.add(join(o.path, k), CodeUnknownKey, "unknown key")
		}
	}
}

func (o *object) wrongType(key, want string, n *node) {
	o.d.problems.add(join(o.path, key), CodeWrongType, "want %s, found a %s", want, n.kind)
}

func (o *object) str(key string) (string, bool) {
	n := o.take(key)
	if n == nil {
		return "", false
	}
	if n.kind != nodeString {
		o.wrongType(key, "a string", n)
		return "", false
	}
	if n.text == "" {
		o.d.problems.add(join(o.path, key), CodeBadValue, "must not be empty")
		return "", false
	}
	return n.text, true
}

func (o *object) boolean(key string) (bool, bool) {
	n := o.take(key)
	if n == nil {
		return false, false
	}
	if n.kind != nodeBool {
		o.wrongType(key, "true or false", n)
		return false, false
	}
	return n.text == "true", true
}

func (o *object) int64(key string) (int64, bool) {
	n := o.take(key)
	if n == nil {
		return 0, false
	}
	if n.kind != nodeNumber {
		o.wrongType(key, "an integer", n)
		return 0, false
	}
	v, ok := parseInt64(n.text)
	if !ok {
		o.d.problems.add(join(o.path, key), CodeBadValue, "%s is not a 64-bit integer", n.text)
		return 0, false
	}
	return v, true
}

// parseInt64 accepts exact integers in any spelling, including 1e3 and 2.0.
func parseInt64(text string) (int64, bool) {
	if v, err := strconv.ParseInt(text, 10, 64); err == nil {
		return v, true
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok || !r.IsInt() || !r.Num().IsInt64() {
		return 0, false
	}
	return r.Num().Int64(), true
}

func (o *object) float(key string) (float64, bool) {
	n := o.take(key)
	if n == nil {
		return 0, false
	}
	if n.kind != nodeNumber {
		o.wrongType(key, "a number", n)
		return 0, false
	}
	v, err := strconv.ParseFloat(n.text, 64)
	if err != nil {
		o.d.problems.add(join(o.path, key), CodeBadValue, "%s is out of range", n.text)
		return 0, false
	}
	return v, true
}

func (o *object) optFloat(key string) *float64 {
	if v, ok := o.float(key); ok {
		return &v
	}
	return nil
}

func (o *object) optInt64(key string) *int64 {
	if v, ok := o.int64(key); ok {
		return &v
	}
	return nil
}

func (o *object) optInt(key string) *int {
	if v, ok := o.int64(key); ok {
		i := int(v)
		return &i
	}
	return nil
}

func (o *object) literal(key string) *Literal {
	n := o.take(key)
	if n == nil {
		return nil
	}
	l, ok := nodeLiteral(n)
	if !ok {
		o.wrongType(key, "a scalar", n)
		return nil
	}
	return &l
}

func nodeLiteral(n *node) (Literal, bool) {
	switch n.kind {
	case nodeNull:
		return Literal{Kind: LiteralNull}, true
	case nodeBool:
		return Literal{Kind: LiteralBool, Text: n.text}, true
	case nodeNumber:
		return Literal{Kind: LiteralNumber, Text: n.text}, true
	case nodeString:
		return Literal{Kind: LiteralString, Text: n.text}, true
	}
	return Literal{}, false
}

func (o *object) notAllowed(key, where string) {
	if o.n.get(key) != nil {
		o.used[key] = true
		o.d.problems.add(join(o.path, key), CodeKeyNotAllowed, "not allowed %s", where)
	}
}

func (d *decoder) schema(root *node) *Schema {
	o := d.object(root, "")
	s := &Schema{Version: CurrentVersion}
	if v, ok := o.int64("schema_version"); ok {
		if v != CurrentVersion {
			d.problems.add("schema_version", CodeBadValue, "unsupported version %d (this binary reads %d)", v, CurrentVersion)
		}
		s.Version = int(v)
	}

	kind, ok := o.str("kind")
	if !ok {
		if root.kind == nodeMap && root.get("kind") == nil {
			d.problems.add("kind", CodeMissingKey, "required: random, hybrid or relational")
		}
		return s
	}
	s.Kind = Kind(kind)

	ctx := fieldContext{presence: s.Kind == KindHybrid}
	if n := o.take("definitions"); n != nil {
		s.Definitions = d.definitions(n, "definitions", ctx)
	}

	switch s.Kind {
	case KindRandom:
		s.Random = d.randomSpec(o)
	case KindHybrid:
		s.Hybrid = d.hybridSpec(o)
	case KindRelational:
		s.Relational = d.relationalSpec(o)
	default:
		d.problems.add("kind", CodeBadValue, "%q is not one of random, hybrid, relational", kind)
		return s
	}
	o.done()
	return s
}

func (d *decoder) definitions(n *node, path string, ctx fieldContext) []Definition {
	o := d.object(n, path)
	defs := make([]Definition, 0, len(o.n.keys))
	for _, name := range o.n.keys {
		p := join(path, name)
		def := d.object(o.take(name), p)
		fields := def.take("fields")
		if fields == nil {
			d.problems.add(join(p, "fields"), CodeMissingKey, "required")
		}
		item := Definition{Name: name}
		if fields != nil {
			item.Fields = d.fields(fields, join(p, "fields"), ctx)
		}
		def.done()
		defs = append(defs, item)
	}
	return defs
}

func (d *decoder) intRange(o *object, key string, into *IntRange) {
	n := o.take(key)
	if n == nil {
		return
	}
	r := d.object(n, join(o.path, key))
	if v, ok := r.int64("min"); ok {
		into.Min = v
	}
	if v, ok := r.int64("max"); ok {
		into.Max = v
	}
	r.done()
}

// randomSpec reads random settings from o, on top of the defaults. o may hold other
// keys, which the caller reports.
func (d *decoder) randomSpec(o *object) *RandomSpec {
	spec := DefaultRandomSpec()
	d.intRange(o, "fields", &spec.Fields)
	d.intRange(o, "depth", &spec.Depth)
	d.intRange(o, "array_length", &spec.ArrayLength)
	d.intRange(o, "string_length", &spec.StringLength)
	d.intRange(o, "integer", &spec.Integer)

	if n := o.take("float"); n != nil {
		f := d.object(n, join(o.path, "float"))
		if v, ok := f.float("min"); ok {
			spec.Float.Min = v
		}
		if v, ok := f.float("max"); ok {
			spec.Float.Max = v
		}
		if v, ok := f.int64("precision"); ok {
			spec.Float.Precision = int(v)
		}
		f.done()
	}

	if n := o.take("types"); n != nil {
		t := d.object(n, join(o.path, "types"))
		spec.Types = nil
		for _, name := range t.n.keys {
			if w, ok := t.float(name); ok {
				spec.Types = append(spec.Types, TypeWeight{TypeName(name), w})
			}
		}
		t.done()
	}
	if n := o.take("probabilities"); n != nil {
		if o.n.get("types") != nil {
			d.problems.add(join(o.path, "probabilities"), CodeKeyNotAllowed, "use either types or probabilities, not both")
		}
		p := d.object(n, join(o.path, "probabilities"))
		probs := &RandomProbabilities{}
		if v, ok := p.float("null"); ok {
			probs.Null = v
		}
		if v, ok := p.float("object"); ok {
			probs.Object = v
		}
		if v, ok := p.float("array"); ok {
			probs.Array = v
		}
		p.done()
		spec.Probabilities = probs
		spec.Types = nil
	}

	if n := o.take("field_names"); n != nil {
		f := d.object(n, join(o.path, "field_names"))
		if v, ok := f.str("style"); ok {
			spec.FieldNames.Style = v
		}
		if v, ok := f.str("vocabulary_group"); ok {
			spec.FieldNames.VocabularyGroup = v
		}
		if v, ok := f.int64("min_len"); ok {
			spec.FieldNames.MinLen = int(v)
		}
		if v, ok := f.int64("max_len"); ok {
			spec.FieldNames.MaxLen = int(v)
		}
		if v, ok := f.boolean("unique_per_object"); ok {
			spec.FieldNames.UniquePerObject = v
		}
		f.done()
	}

	if v, ok := o.int64("max_record_bytes"); ok {
		spec.MaxRecordBytes = v
	}
	return &spec
}

func (d *decoder) hybridSpec(o *object) *HybridSpec {
	spec := &HybridSpec{}
	n := o.take("fields")
	if n == nil {
		d.problems.add("fields", CodeMissingKey, "required")
	} else {
		spec.Fields = d.fields(n, "fields", fieldContext{presence: true})
	}
	if n := o.take("extra_random"); n != nil {
		e := d.object(n, "extra_random")
		extra := &ExtraRandom{Spec: DefaultRandomSpec()}
		if v, ok := e.float("probability"); ok {
			extra.Probability = v
		} else if e.n.get("probability") == nil {
			d.problems.add("extra_random.probability", CodeMissingKey, "required")
		}
		if sn := e.take("spec"); sn != nil {
			so := d.object(sn, "extra_random.spec")
			extra.Spec = *d.randomSpec(so)
			so.done()
		}
		e.done()
		spec.ExtraRandom = extra
	}
	return spec
}

func (d *decoder) relationalSpec(o *object) *RelationalSpec {
	spec := &RelationalSpec{}
	n := o.take("tables")
	if n == nil {
		d.problems.add("tables", CodeMissingKey, "required")
		return spec
	}
	tables := d.object(n, "tables")
	for _, name := range tables.n.keys {
		spec.Tables = append(spec.Tables, d.table(name, tables.take(name), join("tables", name)))
	}
	return spec
}

func (d *decoder) table(name string, n *node, path string) Table {
	o := d.object(n, path)
	t := Table{Name: name, Count: o.optInt64("count")}
	if v, ok := o.str("primary_key"); ok {
		t.PrimaryKey = v
	}

	if on := o.take("owner"); on != nil {
		t.Owner = d.owner(on, join(path, "owner"))
	}

	if fields := o.take("fields"); fields == nil {
		d.problems.add(join(path, "fields"), CodeMissingKey, "required")
	} else {
		t.Fields = d.fields(fields, join(path, "fields"), fieldContext{column: true})
	}

	if uk := o.take("unique_keys"); uk != nil {
		p := join(path, "unique_keys")
		if uk.kind != nodeList {
			d.problems.add(p, CodeWrongType, "want a list, found a %s", uk.kind)
		} else {
			for i, item := range uk.items {
				t.UniqueKeys = append(t.UniqueKeys, d.uniqueKey(item, p+"["+strconv.Itoa(i)+"]"))
			}
		}
	}
	o.done()
	return t
}

func (d *decoder) owner(n *node, path string) *Owner {
	o := d.object(n, path)
	owner := &Owner{PerParent: PerParent{Min: 1, Max: 1}}
	if v, ok := o.str("ref"); ok {
		owner.Table = v
	} else if o.n.get("ref") == nil {
		d.problems.add(join(path, "ref"), CodeMissingKey, "required: the owning table")
	}
	pp := o.take("per_parent")
	if pp == nil {
		d.problems.add(join(path, "per_parent"), CodeMissingKey, "required: rows per owning row")
	} else {
		p := d.object(pp, join(path, "per_parent"))
		if v, ok := p.int64("min"); ok {
			owner.PerParent.Min = v
		}
		if v, ok := p.int64("max"); ok {
			owner.PerParent.Max = v
		}
		owner.PerParent.Distribution = d.flatDistribution(p)
		p.done()
	}
	o.done()
	return owner
}

func (d *decoder) uniqueKey(n *node, path string) UniqueKey {
	o := d.object(n, path)
	key := UniqueKey{}
	if fn := o.take("fields"); fn == nil {
		d.problems.add(join(path, "fields"), CodeMissingKey, "required")
	} else if fn.kind != nodeList {
		d.problems.add(join(path, "fields"), CodeWrongType, "want a list, found a %s", fn.kind)
	} else {
		for _, item := range fn.items {
			if item.kind != nodeString {
				d.problems.add(join(path, "fields"), CodeWrongType, "want field names, found a %s", item.kind)
				continue
			}
			key.Fields = append(key.Fields, item.text)
		}
	}
	if v, ok := o.str("driver"); ok {
		key.Driver = v
	}
	o.done()
	return key
}

func (d *decoder) fields(n *node, path string, ctx fieldContext) []FieldDef {
	o := d.object(n, path)
	out := make([]FieldDef, 0, len(o.n.keys))
	for _, name := range o.n.keys {
		f := d.field(o.take(name), join(path, name), ctx)
		f.Name = name
		out = append(out, f)
	}
	return out
}

func (d *decoder) field(n *node, path string, ctx fieldContext) FieldDef {
	o := d.object(n, path)
	f := FieldDef{}

	if tn := n.get("type"); tn != nil && tn.kind == nodeNull {
		// A bare `type: null` reads as a null value in YAML, but plainly means the type.
		o.used["type"] = true
		f.Type = TypeNull
	} else if v, ok := o.str("type"); ok {
		f.Type = TypeName(v)
	} else if n.kind == nodeMap && tn == nil {
		d.problems.add(join(path, "type"), CodeMissingKey, "required")
	}

	if ctx.presence {
		if pn := o.take("presence"); pn != nil {
			f.Presence = d.presence(pn, join(path, "presence"))
		}
	} else {
		o.notAllowed("presence", "outside hybrid schemas")
	}

	if v, ok := o.boolean("nullable"); ok {
		f.Nullable = v
	}
	f.NullProbability = o.optFloat("null_probability")
	if v, ok := o.boolean("unique"); ok {
		f.Unique = v
	}
	f.Default = o.literal("default")
	f.DefaultProbability = o.optFloat("default_probability")

	f.Min = o.literal("min")
	f.Max = o.literal("max")
	f.MinLength = o.optInt64("min_length")
	f.MaxLength = o.optInt64("max_length")
	if v, ok := o.str("pattern"); ok {
		f.Pattern = v
	}
	f.Values = d.literalList(o, "values")
	f.Weights = d.floatList(o, "weights")
	if dn := o.take("distribution"); dn != nil {
		f.Distribution = d.distribution(dn, join(path, "distribution"))
	}

	f.Precision = o.optInt("precision")
	f.Scale = o.optInt("scale")
	if v, ok := o.str("format"); ok {
		f.Format = v
	}
	if v, ok := o.str("zone"); ok {
		f.Zone = v
	}
	if v, ok := o.str("ranges"); ok {
		f.Ranges = v
	}

	if gn := o.take("generator"); gn != nil {
		f.Generator = d.generator(gn, join(path, "generator"))
	}

	// Elements and nested fields are never columns of a table themselves.
	inner := fieldContext{presence: ctx.presence}
	if fn := o.take("fields"); fn != nil {
		f.Fields = d.fields(fn, join(path, "fields"), inner)
	}
	elem := fieldContext{}
	if in := o.take("items"); in != nil {
		item := d.field(in, join(path, "items"), elem)
		f.Items = &item
	}
	if kn := o.take("key"); kn != nil {
		key := d.field(kn, join(path, "key"), elem)
		f.MapKey = &key
	}
	if vn := o.take("value"); vn != nil {
		value := d.field(vn, join(path, "value"), elem)
		f.MapValue = &value
	}
	if v, ok := o.str("schema"); ok {
		f.Schema = v
	}

	if ctx.column {
		d.columnKeys(o, &f, path)
	} else {
		for _, key := range []string{"ref", "owner", "select", "derive"} {
			o.notAllowed(key, "outside the columns of a relational table")
		}
	}
	o.done()
	return f
}

func (d *decoder) columnKeys(o *object, f *FieldDef, path string) {
	if v, ok := o.str("ref"); ok {
		table, field, found := strings.Cut(v, ".")
		if !found || table == "" || field == "" {
			d.problems.add(join(path, "ref"), CodeBadValue, "%q: want table.field", v)
		}
		f.Ref = &Reference{Table: table, Field: field}
	}
	owner, hasOwner := o.boolean("owner")
	sel := o.take("select")
	if f.Ref == nil && (hasOwner || sel != nil) {
		d.problems.add(path, CodeKeyNotAllowed, "owner and select need a ref")
	}
	if f.Ref != nil {
		f.Ref.Owner = hasOwner && owner
		if sel != nil {
			f.Ref.Select = d.selectSpec(sel, join(path, "select"))
		}
	}
	if dn := o.take("derive"); dn != nil {
		f.Derive = d.derive(dn, join(path, "derive"))
	}
}

func (d *decoder) presence(n *node, path string) Presence {
	if n.kind == nodeString {
		if n.text != "required" {
			d.problems.add(path, CodeBadValue, "%q: want required, or {optional: probability}", n.text)
		}
		return Presence{}
	}
	o := d.object(n, path)
	p, ok := o.float("optional")
	if !ok && n.kind == nodeMap && n.get("optional") == nil {
		d.problems.add(join(path, "optional"), CodeMissingKey, "required")
	}
	o.done()
	return Presence{Optional: true, Probability: p}
}

func (d *decoder) literalList(o *object, key string) []Literal {
	n := o.take(key)
	if n == nil {
		return nil
	}
	if n.kind != nodeList {
		o.wrongType(key, "a list", n)
		return nil
	}
	out := make([]Literal, 0, len(n.items))
	for i, item := range n.items {
		l, ok := nodeLiteral(item)
		if !ok {
			d.problems.add(join(o.path, key)+"["+strconv.Itoa(i)+"]", CodeWrongType, "want a scalar, found a %s", item.kind)
			continue
		}
		out = append(out, l)
	}
	return out
}

func (d *decoder) floatList(o *object, key string) []float64 {
	n := o.take(key)
	if n == nil {
		return nil
	}
	if n.kind != nodeList {
		o.wrongType(key, "a list", n)
		return nil
	}
	out := make([]float64, 0, len(n.items))
	for i, item := range n.items {
		v, err := strconv.ParseFloat(item.text, 64)
		if item.kind != nodeNumber || err != nil {
			d.problems.add(join(o.path, key)+"["+strconv.Itoa(i)+"]", CodeWrongType, "want a number, found a %s", item.kind)
			continue
		}
		out = append(out, v)
	}
	return out
}

// distribution reads "zipf" or {type: zipf, s: 1.2}.
func (d *decoder) distribution(n *node, path string) *Distribution {
	if n.kind == nodeString {
		if n.text == "" {
			d.problems.add(path, CodeBadValue, "must not be empty")
		}
		return &Distribution{Type: n.text}
	}
	o := d.object(n, path)
	dist := &Distribution{}
	if v, ok := o.str("type"); ok {
		dist.Type = v
	} else if n.kind == nodeMap && n.get("type") == nil {
		d.problems.add(join(path, "type"), CodeMissingKey, "required")
	}
	d.distributionParams(o, dist)
	o.done()
	return dist
}

// flatDistribution reads {distribution: zipf, s: 1.2} from a mapping that has other
// keys too, as per_parent and select do.
func (d *decoder) flatDistribution(o *object) *Distribution {
	v, ok := o.str("distribution")
	if !ok {
		return nil
	}
	dist := &Distribution{Type: v}
	d.distributionParams(o, dist)
	return dist
}

func (d *decoder) distributionParams(o *object, dist *Distribution) {
	dist.Mean = o.optFloat("mean")
	dist.StdDev = o.optFloat("stddev")
	dist.Rate = o.optFloat("rate")
	dist.S = o.optFloat("s")
	dist.V = o.optFloat("v")
	dist.Start = o.optInt64("start")
	dist.Step = o.optInt64("step")
}

func (d *decoder) selectSpec(n *node, path string) *Select {
	o := d.object(n, path)
	s := &Select{Distribution: d.flatDistribution(o)}
	if v, ok := o.str("mode"); ok {
		s.Mode = v
	}
	if v, ok := o.boolean("distinct"); ok {
		s.Distinct = v
	}
	o.done()
	return s
}

func (d *decoder) derive(n *node, path string) *Derive {
	o := d.object(n, path)
	out := &Derive{}
	if v, ok := o.str("from"); ok {
		out.From = v
	} else if n.kind == nodeMap && n.get("from") == nil {
		d.problems.add(join(path, "from"), CodeMissingKey, "required")
	}
	if v, ok := o.str("field"); ok {
		out.Field = v
	} else if n.kind == nodeMap && n.get("field") == nil {
		d.problems.add(join(path, "field"), CodeMissingKey, "required")
	}
	if on := o.take("offset"); on != nil {
		off := d.object(on, join(path, "offset"))
		out.OffsetMin = d.duration(off, "min")
		out.OffsetMax = d.duration(off, "max")
		off.done()
	}
	o.done()
	return out
}

func (d *decoder) duration(o *object, key string) *time.Duration {
	s, ok := o.str(key)
	if !ok {
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		d.problems.add(join(o.path, key), CodeBadValue, "%q is not a duration such as 72h", s)
		return nil
	}
	return &v
}

func (d *decoder) generator(n *node, path string) *GeneratorRef {
	if n.kind == nodeString {
		if n.text == "" {
			d.problems.add(path, CodeBadValue, "must not be empty")
		}
		return &GeneratorRef{Name: n.text}
	}
	o := d.object(n, path)
	g := &GeneratorRef{}
	if v, ok := o.str("name"); ok {
		g.Name = v
	} else if n.kind == nodeMap && n.get("name") == nil {
		d.problems.add(join(path, "name"), CodeMissingKey, "required")
	}
	if pn := o.take("params"); pn != nil {
		params := d.object(pn, join(path, "params"))
		for _, name := range params.n.keys {
			value := params.take(name)
			p := join(params.path, name)
			if l, ok := nodeLiteral(value); ok {
				g.Params = append(g.Params, Param{name, l})
				continue
			}
			if value.kind != nodeList {
				d.problems.add(p, CodeWrongType, "want a scalar or list, found a %s", value.kind)
				continue
			}
			list := make([]Literal, 0, len(value.items))
			for _, item := range value.items {
				l, ok := nodeLiteral(item)
				if !ok {
					d.problems.add(p, CodeWrongType, "list items must be scalars")
					continue
				}
				list = append(list, l)
			}
			g.Params = append(g.Params, Param{name, list})
		}
	}
	o.done()
	return g
}
