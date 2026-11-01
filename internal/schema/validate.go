package schema

import (
	"regexp"
	"strings"
	"unicode"
)

// Validation problem codes.
const (
	CodeUnknownType       = "ERR_UNKNOWN_TYPE"
	CodeConstraint        = "ERR_CONSTRAINT_NOT_APPLICABLE"
	CodeRange             = "ERR_INVALID_RANGE"
	CodeProbability       = "ERR_INVALID_PROBABILITY"
	CodeLiteral           = "ERR_INVALID_LITERAL"
	CodeEmptyEnum         = "ERR_EMPTY_ENUM"
	CodeWeights           = "ERR_INVALID_WEIGHTS"
	CodePattern           = "ERR_INVALID_PATTERN"
	CodeDistribution      = "ERR_INVALID_DISTRIBUTION"
	CodeName              = "ERR_INVALID_NAME"
	CodeRequiredFields    = "ERR_FIELDS_REQUIRED"
	CodeLimit             = "ERR_LIMIT_EXCEEDED"
	CodeSchemaCycle       = "ERR_SCHEMA_CYCLE"
	CodeUnknownSchema     = "ERR_UNKNOWN_SCHEMA"
	CodeDepth             = "ERR_TOO_DEEP"
	CodeRecordTooLarge    = "ERR_RECORD_TOO_LARGE"
	CodeColumnCollision   = "ERR_COLUMN_COLLISION"
	CodeNullable          = "ERR_NOT_NULLABLE"
	CodeFormatIncompat    = "ERR_FORMAT_INCOMPATIBLE"
	CodeUniqueUnsupported = "ERR_UNIQUE_UNSUPPORTED"
	CodeUniqueInfeasible  = "ERR_UNIQUE_INFEASIBLE"
)

// MaxTables caps tables and, separately, definitions. Real schemas are far below it;
// the cap keeps validation of hostile documents fast, as both are searched by name.
const MaxTables = 1024

// Limits are the hard caps a schema must stay within, from generation settings.
type Limits struct {
	MaxDepth        int64
	MaxFields       int64
	MaxArrayLength  int64
	MaxStringLength int64
	MaxRecordBytes  int64
}

// DefaultLimits returns the documented default caps.
func DefaultLimits() Limits {
	return Limits{MaxDepth: 16, MaxFields: 256, MaxArrayLength: 1024, MaxStringLength: 65536, MaxRecordBytes: 64 << 10}
}

// Output says how the schema will be serialized, for the checks that depend on it.
type Output struct {
	Format       string // json | ndjson | xml | csv; empty skips format checks
	Separator    string // CSV flattening separator; empty means "."
	ArrayColumns int    // CSV: expand arrays to this many columns; 0 means one JSON column
	Nested       bool   // relational nested layout
	Root         string // the root table of a nested layout
}

// Options configures Validate.
type Options struct {
	Limits Limits
	Output Output
	// MaxRecords is how many records a random or hybrid stream may produce, for unique
	// feasibility. 0 skips that check, because the count belongs to the session.
	MaxRecords int64
}

// Validate checks a decoded schema's meaning and returns every problem found.
func Validate(s *Schema, o Options) Problems {
	v := &validator{opts: o, schema: s, defs: map[string]*Definition{}, depths: map[string]int64{}}
	v.definitions()
	switch {
	case s.Random != nil:
		v.random("", s.Random)
		if o.Output.Nested {
			v.problems.add("", CodeFormatIncompat, "the nested layout needs a relational schema")
		}
	case s.Hybrid != nil:
		v.hybrid(s.Hybrid)
		if o.Output.Nested {
			v.problems.add("", CodeFormatIncompat, "the nested layout needs a relational schema")
		}
	case s.Relational != nil:
		v.relational(s.Relational)
	}
	return v.problems
}

type validator struct {
	problems Problems
	opts     Options
	schema   *Schema
	defs     map[string]*Definition
	// defCycle means sizes and columns cannot be computed: expanding would never end.
	defCycle bool
	depths   map[string]int64
}

func (v *validator) add(path, code, format string, args ...any) {
	v.problems.add(path, code, format, args...)
}

var tableNamePattern = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// validFieldName keeps names printable. Serializers sanitize further for XML and CSV.
func validFieldName(name string) bool {
	if name == "" || len(name) > 256 {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (v *validator) definitions() {
	if len(v.schema.Definitions) > MaxTables {
		v.add("definitions", CodeLimit, "%d definitions exceed the limit of %d", len(v.schema.Definitions), MaxTables)
		v.defCycle = true // too many to expand: skip what depends on expansion
		return
	}
	for i := range v.schema.Definitions {
		d := &v.schema.Definitions[i]
		v.defs[d.Name] = d
	}
	for i := range v.schema.Definitions {
		d := &v.schema.Definitions[i]
		path := join("definitions", d.Name)
		if !validFieldName(d.Name) {
			v.add(path, CodeName, "invalid definition name")
		}
		if len(d.Fields) == 0 {
			v.add(join(path, "fields"), CodeRequiredFields, "a definition needs at least one field")
		}
		v.checkDefinitionCycle(d, path)
		v.fields(join(path, "fields"), d.Fields, 2)
	}
}

// definitionDepth is how many object levels a definition expands to. It must only be
// called once definition cycles are ruled out; memoizing keeps wide chains linear.
func (v *validator) definitionDepth(name string) int64 {
	if d, ok := v.depths[name]; ok {
		return d
	}
	def, ok := v.defs[name]
	if !ok {
		return 0
	}
	v.depths[name] = 0
	var deepest int64
	var walk func(f *FieldDef, level int64)
	walk = func(f *FieldDef, level int64) {
		deepest = max(deepest, level)
		if f.Schema != "" {
			deepest = max(deepest, level+v.definitionDepth(f.Schema))
		}
		for i := range f.Fields {
			walk(&f.Fields[i], level+1)
		}
		for _, sub := range []*FieldDef{f.Items, f.MapKey, f.MapValue} {
			if sub != nil {
				walk(sub, level+1)
			}
		}
	}
	for i := range def.Fields {
		walk(&def.Fields[i], 1)
	}
	v.depths[name] = deepest
	return deepest
}

// checkDefinitionCycle follows nested_schema references from d looking for d again.
func (v *validator) checkDefinitionCycle(d *Definition, path string) {
	seen := map[string]bool{}
	var visit func(fields []FieldDef, trail []string) bool
	visit = func(fields []FieldDef, trail []string) bool {
		for i := range fields {
			for _, name := range nestedNames(&fields[i]) {
				if name == d.Name {
					v.add(path, CodeSchemaCycle, "definition refers to itself: %s", strings.Join(append(trail, name), " -> "))
					v.defCycle = true
					return true
				}
				next, ok := v.defs[name]
				if !ok || seen[name] {
					continue
				}
				seen[name] = true
				if visit(next.Fields, append(trail, name)) {
					return true
				}
			}
		}
		return false
	}
	visit(d.Fields, []string{d.Name})
}

// nestedNames lists the definitions a field refers to, at any depth inside it.
func nestedNames(f *FieldDef) []string {
	var out []string
	if f.Schema != "" {
		out = append(out, f.Schema)
	}
	for i := range f.Fields {
		out = append(out, nestedNames(&f.Fields[i])...)
	}
	for _, sub := range []*FieldDef{f.Items, f.MapKey, f.MapValue} {
		if sub != nil {
			out = append(out, nestedNames(sub)...)
		}
	}
	return out
}

func (v *validator) rangeCheck(path string, r IntRange, floor, ceiling int64) {
	if r.Min > r.Max {
		v.add(path, CodeRange, "min %d is above max %d", r.Min, r.Max)
	}
	if r.Min < floor {
		v.add(join(path, "min"), CodeRange, "must be at least %d", floor)
	}
	if ceiling > 0 && r.Max > ceiling {
		v.add(join(path, "max"), CodeLimit, "%d exceeds the limit of %d", r.Max, ceiling)
	}
}

func (v *validator) probability(path string, p float64) {
	// The negated form also catches NaN.
	if !(p >= 0 && p <= 1) {
		v.add(path, CodeProbability, "%v is not between 0 and 1", p)
	}
}

func (v *validator) random(path string, r *RandomSpec) {
	l := v.opts.Limits
	v.rangeCheck(join(path, "fields"), r.Fields, 0, l.MaxFields)
	v.rangeCheck(join(path, "depth"), r.Depth, 0, l.MaxDepth)
	v.rangeCheck(join(path, "array_length"), r.ArrayLength, 0, l.MaxArrayLength)
	v.rangeCheck(join(path, "string_length"), r.StringLength, 0, l.MaxStringLength)
	if r.Integer.Min > r.Integer.Max {
		v.add(join(path, "integer"), CodeRange, "min %d is above max %d", r.Integer.Min, r.Integer.Max)
	}
	if !(r.Float.Min <= r.Float.Max) {
		v.add(join(path, "float"), CodeRange, "min %v is above max %v", r.Float.Min, r.Float.Max)
	}
	if r.Float.Precision < 0 || r.Float.Precision > 15 {
		v.add(join(path, "float.precision"), CodeRange, "must be between 0 and 15")
	}

	if r.Types != nil {
		v.randomTypes(join(path, "types"), r)
	}
	if p := r.Probabilities; p != nil {
		pp := join(path, "probabilities")
		v.probability(join(pp, "null"), p.Null)
		v.probability(join(pp, "object"), p.Object)
		v.probability(join(pp, "array"), p.Array)
		if p.Null+p.Object+p.Array > 1 {
			v.add(pp, CodeProbability, "probabilities add up to more than 1")
		}
	}
	if r.Types == nil && r.Probabilities == nil {
		v.add(path, CodeWeights, "needs types or probabilities")
	}

	fn := r.FieldNames
	np := join(path, "field_names")
	switch fn.Style {
	case "words", "camel", "snake", "random":
	default:
		v.add(join(np, "style"), CodeBadValue, "%q is not one of words, camel, snake, random", fn.Style)
	}
	if fn.MinLen < 1 || fn.MinLen > fn.MaxLen || fn.MaxLen > 256 {
		v.add(np, CodeRange, "need 1 ≤ min_len ≤ max_len ≤ 256")
	}

	if r.MaxRecordBytes <= 0 {
		v.add(join(path, "max_record_bytes"), CodeRange, "must be greater than zero")
	}
	if l.MaxRecordBytes > 0 && r.MaxRecordBytes > l.MaxRecordBytes {
		v.add(join(path, "max_record_bytes"), CodeLimit, "%d exceeds the limit of %d", r.MaxRecordBytes, l.MaxRecordBytes)
	}
}

func (v *validator) randomTypes(path string, r *RandomSpec) {
	var total float64
	for _, w := range r.Types {
		p := join(path, string(w.Type))
		if !randomTypes[w.Type] {
			v.add(p, CodeUnknownType, "random generation cannot produce type %q", w.Type)
		}
		if !(w.Weight >= 0) {
			v.add(p, CodeWeights, "weight must not be negative")
			continue
		}
		total += w.Weight
	}
	if total <= 0 {
		v.add(path, CodeWeights, "at least one type needs a positive weight")
	}
}

func (v *validator) hybrid(h *HybridSpec) {
	if len(h.Fields) == 0 {
		v.add("fields", CodeRequiredFields, "a hybrid schema needs at least one field")
	}
	v.fields("fields", h.Fields, 1)
	v.uniqueFlatFields("fields", h.Fields, v.opts.MaxRecords)

	if e := h.ExtraRandom; e != nil {
		v.probability("extra_random.probability", e.Probability)
		v.random("extra_random.spec", &e.Spec)
	}
	if v.defCycle {
		return
	}
	// Extra random fields do not count: the generator stops adding them when the record
	// reaches its limit, so they fill what the declared fields leave.
	v.recordSize("fields", fieldsMaxBytes(h.Fields, v.defs))
	v.csvColumns("fields", h.Fields)
}

func (v *validator) csvColumns(path string, fields []FieldDef) {
	if v.opts.Output.Format != "csv" {
		return
	}
	columns := Columns(fields, v.defs, v.opts.Output.Separator, v.opts.Output.ArrayColumns)
	if len(columns) >= MaxColumns {
		v.add(path, CodeLimit, "flattening produces more than %d CSV columns", MaxColumns)
		return
	}
	seen := map[string]bool{}
	for _, c := range columns {
		if seen[c] {
			v.add(path, CodeColumnCollision, "two fields flatten to the CSV column %q: rename one or change the separator", c)
		}
		seen[c] = true
	}
}

// fields validates a list of fields; depth is the object nesting level.
func (v *validator) fields(path string, fields []FieldDef, depth int64) {
	if max := v.opts.Limits.MaxFields; max > 0 && int64(len(fields)) > max {
		v.add(path, CodeLimit, "%d fields exceed the limit of %d", len(fields), max)
	}
	if max := v.opts.Limits.MaxDepth; max > 0 && depth > max {
		v.add(path, CodeDepth, "nesting exceeds the limit of %d", max)
		return
	}
	for i := range fields {
		f := &fields[i]
		p := join(path, f.Name)
		if !validFieldName(f.Name) {
			v.add(p, CodeName, "invalid field name")
		}
		v.field(p, f, depth)
	}
}

func (v *validator) field(path string, f *FieldDef, depth int64) {
	if !KnownType(f.Type) {
		v.add(join(path, "type"), CodeUnknownType, "unknown type %q", f.Type)
		return
	}

	if f.Presence.Optional {
		v.probability(join(path, "presence.optional"), f.Presence.Probability)
	}
	if f.NullProbability != nil {
		if !f.Nullable {
			v.add(join(path, "null_probability"), CodeConstraint, "needs nullable: true")
		}
		v.probability(join(path, "null_probability"), *f.NullProbability)
	}
	if f.DefaultProbability != nil {
		if f.Default == nil {
			v.add(join(path, "default_probability"), CodeConstraint, "needs a default")
		}
		v.probability(join(path, "default_probability"), *f.DefaultProbability)
	}

	v.literalsAndRange(path, f)
	v.lengths(path, f)
	v.patternConstraint(path, f)
	v.valuesConstraint(path, f)
	v.distributionConstraint(path, f)
	v.typeOptions(path, f)
	v.composite(path, f, depth)

	if f.Type == TypeReference && f.Ref == nil {
		v.add(join(path, "ref"), CodeConstraint, "type reference needs a ref, and exists only on columns of a relational table")
	}
	if f.Ref != nil && f.Type != TypeReference {
		v.add(join(path, "ref"), CodeConstraint, "ref needs type reference, not %s", f.Type)
	}
	if f.Unique && !f.Type.isScalar() {
		v.add(join(path, "unique"), CodeUniqueUnsupported, "type %s cannot be unique", f.Type)
	}
	if f.Unique && depth > 1 {
		v.add(join(path, "unique"), CodeUniqueUnsupported, "only top-level fields can be unique, because uniqueness comes from the record index")
	}
}

func (v *validator) literalsAndRange(path string, f *FieldDef) {
	bound := func(key string, l *Literal) bool {
		if l == nil {
			return false
		}
		if !f.Type.isOrdered() {
			v.add(join(path, key), CodeConstraint, "not applicable to type %s", f.Type)
			return false
		}
		if err := checkLiteral(f.Type, *l); err != nil {
			v.add(join(path, key), CodeLiteral, "%v", err)
			return false
		}
		return true
	}
	hasMin := bound("min", f.Min)
	hasMax := bound("max", f.Max)
	if hasMin && hasMax && compareLiterals(f.Type, *f.Min, *f.Max) > 0 {
		v.add(path, CodeRange, "min %s is above max %s", f.Min.Text, f.Max.Text)
	}

	if f.Default != nil {
		switch {
		case f.Default.Kind == LiteralNull:
			if !f.Nullable && f.Type != TypeNull {
				v.add(join(path, "default"), CodeNullable, "default null needs nullable: true")
			}
		case !f.Type.isScalar() || f.Type == TypeReference:
			v.add(join(path, "default"), CodeConstraint, "not applicable to type %s", f.Type)
		default:
			if err := checkLiteral(f.Type, *f.Default); err != nil {
				v.add(join(path, "default"), CodeLiteral, "%v", err)
			}
		}
	}
}

func (v *validator) lengths(path string, f *FieldDef) {
	if f.MinLength == nil && f.MaxLength == nil {
		return
	}
	switch f.Type {
	case TypeString, TypeBinary, TypeArray, TypeMap:
	default:
		v.add(path, CodeConstraint, "min_length and max_length do not apply to type %s", f.Type)
		return
	}
	limit := v.opts.Limits.MaxStringLength
	if f.Type == TypeArray || f.Type == TypeMap {
		limit = v.opts.Limits.MaxArrayLength
	}
	if f.MinLength != nil && *f.MinLength < 0 {
		v.add(join(path, "min_length"), CodeRange, "must not be negative")
	}
	if f.MaxLength != nil {
		if *f.MaxLength < 0 {
			v.add(join(path, "max_length"), CodeRange, "must not be negative")
		}
		if limit > 0 && *f.MaxLength > limit {
			v.add(join(path, "max_length"), CodeLimit, "%d exceeds the limit of %d", *f.MaxLength, limit)
		}
	}
	if f.MinLength != nil && f.MaxLength != nil && *f.MinLength > *f.MaxLength {
		v.add(path, CodeRange, "min_length %d is above max_length %d", *f.MinLength, *f.MaxLength)
	}
	if f.MinLength != nil && limit > 0 && *f.MinLength > limit {
		v.add(join(path, "min_length"), CodeLimit, "%d exceeds the limit of %d", *f.MinLength, limit)
	}
}

func (v *validator) patternConstraint(path string, f *FieldDef) {
	if f.Pattern == "" {
		return
	}
	p := join(path, "pattern")
	if f.Type != TypeString {
		v.add(p, CodeConstraint, "not applicable to type %s", f.Type)
		return
	}
	info, err := AnalyzePattern(f.Pattern)
	if err != nil {
		v.add(p, CodePattern, "%v", err)
		return
	}
	if f.MaxLength != nil && info.MinLen > *f.MaxLength {
		v.add(p, CodePattern, "matches are at least %d long, above max_length %d", info.MinLen, *f.MaxLength)
	}
	if f.MinLength != nil && info.MaxLen < *f.MinLength {
		v.add(p, CodePattern, "matches are at most %d long, below min_length %d", info.MaxLen, *f.MinLength)
	}
}

func (v *validator) valuesConstraint(path string, f *FieldDef) {
	p := join(path, "values")
	if f.Values == nil {
		if f.Type == TypeEnum {
			v.add(p, CodeEmptyEnum, "an enum needs values")
		}
		if f.Weights != nil {
			v.add(join(path, "weights"), CodeWeights, "weights need values")
		}
		return
	}
	if !f.Type.isScalar() || f.Type == TypeReference || f.Type == TypeNull {
		v.add(p, CodeConstraint, "not applicable to type %s", f.Type)
		return
	}
	if len(f.Values) == 0 {
		v.add(p, CodeEmptyEnum, "must not be empty")
		return
	}
	seen := make(map[Literal]bool, len(f.Values))
	for _, l := range f.Values {
		if err := checkLiteral(f.Type, l); err != nil {
			v.add(p, CodeLiteral, "%s: %v", literalString(l), err)
			continue
		}
		if seen[l] {
			v.add(p, CodeLiteral, "%s is listed twice", literalString(l))
		}
		seen[l] = true
	}
	if f.Weights != nil {
		w := join(path, "weights")
		if len(f.Weights) != len(f.Values) {
			v.add(w, CodeWeights, "%d weights for %d values", len(f.Weights), len(f.Values))
			return
		}
		var total float64
		for _, x := range f.Weights {
			if !(x >= 0) {
				v.add(w, CodeWeights, "weights must not be negative")
				return
			}
			total += x
		}
		if total <= 0 {
			v.add(w, CodeWeights, "at least one weight must be positive")
		}
	}
}

func (v *validator) distributionConstraint(path string, f *FieldDef) {
	if f.Distribution == nil {
		return
	}
	p := join(path, "distribution")
	if !f.Type.isOrdered() {
		v.add(p, CodeConstraint, "not applicable to type %s", f.Type)
		return
	}
	if f.Distribution.Type == DistSequence && f.Type != TypeInteger && f.Type != TypeLong {
		v.add(p, CodeDistribution, "sequence needs an integer or long")
		return
	}
	if f.Distribution.Type == DistZipf && f.Type != TypeInteger && f.Type != TypeLong {
		v.add(p, CodeDistribution, "zipf needs an integer or long")
		return
	}
	v.distribution(p, f.Distribution, DistUniform, DistNormal, DistExponential, DistZipf, DistSequence)
}

// distribution checks parameters, allowing only the listed types.
func (v *validator) distribution(path string, d *Distribution, allowed ...string) {
	ok := false
	for _, a := range allowed {
		ok = ok || d.Type == a
	}
	if !ok {
		v.add(path, CodeDistribution, "%q is not one of %s", d.Type, strings.Join(allowed, ", "))
		return
	}

	stray := func(present bool, name string) {
		if present {
			v.add(join(path, name), CodeDistribution, "does not apply to %s", d.Type)
		}
	}
	if d.Type != DistNormal {
		stray(d.Mean != nil, "mean")
		stray(d.StdDev != nil, "stddev")
	}
	if d.Type != DistExponential {
		stray(d.Rate != nil, "rate")
	}
	if d.Type != DistZipf {
		stray(d.S != nil, "s")
		stray(d.V != nil, "v")
	}
	if d.Type != DistSequence {
		stray(d.Start != nil, "start")
		stray(d.Step != nil, "step")
	}

	switch d.Type {
	case DistNormal:
		if d.Mean == nil || d.StdDev == nil {
			v.add(path, CodeDistribution, "normal needs mean and stddev")
		} else if !(*d.StdDev > 0) {
			v.add(join(path, "stddev"), CodeDistribution, "must be greater than zero")
		}
	case DistExponential:
		if d.Rate == nil {
			v.add(path, CodeDistribution, "exponential needs rate")
		} else if !(*d.Rate > 0) {
			v.add(join(path, "rate"), CodeDistribution, "must be greater than zero")
		}
	case DistZipf:
		if d.S != nil && !(*d.S > 1) {
			v.add(join(path, "s"), CodeDistribution, "must be greater than 1")
		}
		if d.V != nil && !(*d.V >= 1) {
			v.add(join(path, "v"), CodeDistribution, "must be at least 1")
		}
	case DistSequence:
		if d.Step != nil && *d.Step <= 0 {
			v.add(join(path, "step"), CodeDistribution, "must be greater than zero")
		}
	}
}

func (v *validator) typeOptions(path string, f *FieldDef) {
	if f.Precision != nil && f.Type != TypeDecimal {
		v.add(join(path, "precision"), CodeConstraint, "applies to decimal only")
	}
	if f.Scale != nil && f.Type != TypeDecimal && f.Type != TypeMoney {
		v.add(join(path, "scale"), CodeConstraint, "applies to decimal and money only")
	}
	if f.Type == TypeDecimal || f.Type == TypeMoney {
		v.decimalShape(path, f)
	}

	if f.Format != "" {
		if f.Type != TypeTimestamp {
			v.add(join(path, "format"), CodeConstraint, "applies to timestamp only")
		} else if f.Format != "rfc3339" && f.Format != "unix" && f.Format != "unix_ms" {
			v.add(join(path, "format"), CodeBadValue, "%q is not one of rfc3339, unix, unix_ms", f.Format)
		}
	}
	if f.Zone != "" && f.Type != TypeDateTime {
		v.add(join(path, "zone"), CodeConstraint, "applies to datetime only")
	}
	if f.Ranges != "" {
		if f.Type.class() != classIP {
			v.add(join(path, "ranges"), CodeConstraint, "applies to IP types only")
		} else if f.Ranges != "documentation" && f.Ranges != "any" {
			v.add(join(path, "ranges"), CodeBadValue, "%q is not one of documentation, any", f.Ranges)
		}
	}
}

func (v *validator) decimalShape(path string, f *FieldDef) {
	scale := DefaultDecimalScale
	if f.Scale != nil {
		scale = *f.Scale
	}
	precision := DefaultDecimalPrecision
	if f.Precision != nil {
		precision = *f.Precision
	}
	if precision < 1 || precision > MaxDecimalPrecision {
		v.add(join(path, "precision"), CodeRange, "must be between 1 and %d", MaxDecimalPrecision)
		return
	}
	if scale < 0 || scale > precision {
		v.add(join(path, "scale"), CodeRange, "must be between 0 and precision (%d)", precision)
		return
	}
	for key, l := range map[string]*Literal{"min": f.Min, "max": f.Max} {
		if l != nil && !literalFitsDecimal(*l, precision, scale) {
			v.add(join(path, key), CodeRange, "%s does not fit precision %d, scale %d", l.Text, precision, scale)
		}
	}
}

func (v *validator) composite(path string, f *FieldDef, depth int64) {
	notFor := func(present bool, key string) {
		if present {
			v.add(join(path, key), CodeConstraint, "not applicable to type %s", f.Type)
		}
	}
	notFor(f.Fields != nil && f.Type != TypeObject, "fields")
	notFor(f.Items != nil && f.Type != TypeArray, "items")
	notFor(f.MapKey != nil && f.Type != TypeMap, "key")
	notFor(f.MapValue != nil && f.Type != TypeMap, "value")
	notFor(f.Schema != "" && f.Type != TypeNestedSchema, "schema")

	switch f.Type {
	case TypeObject:
		if len(f.Fields) == 0 {
			v.add(join(path, "fields"), CodeRequiredFields, "an object needs at least one field")
		}
		v.fields(join(path, "fields"), f.Fields, depth+1)
	case TypeArray:
		if f.Items == nil {
			v.add(join(path, "items"), CodeMissingKey, "an array needs items")
			return
		}
		v.element(join(path, "items"), f.Items, depth+1)
	case TypeMap:
		if f.MapValue == nil {
			v.add(join(path, "value"), CodeMissingKey, "a map needs value")
			return
		}
		v.element(join(path, "value"), f.MapValue, depth+1)
		if f.MapKey != nil {
			if f.MapKey.Type != TypeString && f.MapKey.Type.class() != classSemantic && f.MapKey.Type != TypeEnum && f.MapKey.Type != TypeUUID {
				v.add(join(path, "key"), CodeConstraint, "map keys must be text, not %s", f.MapKey.Type)
			}
			v.element(join(path, "key"), f.MapKey, depth+1)
		}
	case TypeNestedSchema:
		if f.Schema == "" {
			v.add(join(path, "schema"), CodeMissingKey, "a nested_schema needs a definition name")
		} else if _, ok := v.defs[f.Schema]; !ok {
			v.add(join(path, "schema"), CodeUnknownSchema, "no definition named %q", f.Schema)
		} else if limit := v.opts.Limits.MaxDepth; limit > 0 && !v.defCycle && depth+v.definitionDepth(f.Schema) > limit {
			v.add(join(path, "schema"), CodeDepth, "expanding %s exceeds the nesting limit of %d", f.Schema, limit)
		}
	}
}

// element validates an array item or map key or value, which is unnamed.
func (v *validator) element(path string, f *FieldDef, depth int64) {
	if max := v.opts.Limits.MaxDepth; max > 0 && depth > max {
		v.add(path, CodeDepth, "nesting exceeds the limit of %d", max)
		return
	}
	v.field(path, f, depth)
}
