package schema

import (
	"errors"
	"math"
	"math/big"
	"strconv"
	"time"
)

// Decimal defaults.
const (
	DefaultDecimalPrecision = 18
	DefaultDecimalScale     = 2
	MaxDecimalPrecision     = 38
)

// Sizing defaults for values with no declared maximum. The engine must not exceed
// these, because memory bounds are computed from them.
const (
	DefaultStringMaxLength = 64
	DefaultCollectionMax   = 8
	// MaxVocabularyValueBytes bounds a catalog value; 4 bytes per character at the
	// vocabulary value limit of 200 characters.
	MaxVocabularyValueBytes = 800
	bytesPerCharacter       = 4 // UTF-8 worst case
	jsonOverhead            = 8 // quotes, punctuation and escapes per value, on average worst case
)

// semanticMaxLength bounds generated semantic text, in bytes.
var semanticMaxLength = map[TypeName]int64{
	TypeEmail: 254, TypePhone: 32, TypeURL: 2048, TypeCountry: 64, TypeCity: 128,
	TypePostalCode: 16, TypeFirstName: 64, TypeLastName: 64, TypeFullName: 128,
	TypeCompanyName: 128, TypeAddress: 256, TypeCurrency: 3, TypeCreditCard: 19,
	TypePaymentStatus: 32, TypeOrderStatus: 32,
}

// Capacity is how many distinct values a unique field can take.
type Capacity struct {
	// Supported is false when the type cannot be encoded injectively.
	Supported bool
	// Known is false when the answer depends on a generator or catalog, which check it
	// themselves once bound.
	Known bool
	N     *big.Int // nil means effectively unbounded (2^63 or more)
}

// UniqueCapacity computes the size of a field's injective value space. The encoders
// in gen/unique must agree with this; see docs/02-domain/unique-encoders.md.
func UniqueCapacity(f *FieldDef) Capacity {
	unbounded := Capacity{Supported: true, Known: true}
	exact := func(n *big.Int) Capacity { return Capacity{Supported: true, Known: true, N: n} }

	if f.Generator != nil {
		return Capacity{Supported: true}
	}
	if len(f.Values) > 0 {
		return exact(big.NewInt(int64(len(f.Values))))
	}
	if f.Distribution != nil && f.Distribution.Type == DistSequence {
		return unbounded
	}

	switch f.Type {
	case TypeInteger, TypeLong:
		return exact(integerCapacity(f))
	case TypeDecimal, TypeMoney:
		return decimalCapacity(f)
	case TypeDate, TypeTime, TypeDateTime, TypeTimestamp:
		return temporalCapacity(f)
	case TypeUUID, TypeEmail, TypeURL:
		return unbounded
	case TypeString:
		return stringCapacity(f)
	case TypeBinary:
		return binaryCapacity(f)
	case TypeIPv4:
		if f.Ranges == "any" {
			return exact(big.NewInt(1 << 32))
		}
		return exact(big.NewInt(3 * 256)) // the three IPv4 documentation /24 networks
	case TypeIPv6, TypeIPAddress:
		return unbounded // 2001:db8::/32 alone holds 2^96
	case TypePhone:
		return Capacity{Supported: true} // the fictional range depends on the locale's generator
	case TypeEnum:
		return exact(big.NewInt(0))
	}
	return Capacity{}
}

func integerCapacity(f *FieldDef) *big.Int {
	lo, hi := big.NewInt(math.MinInt32), big.NewInt(math.MaxInt32)
	if f.Type == TypeLong {
		lo, hi = big.NewInt(math.MinInt64), big.NewInt(math.MaxInt64)
	}
	if f.Min != nil {
		if r, ok := literalRat(*f.Min); ok && r.IsInt() {
			lo = r.Num()
		}
	}
	if f.Max != nil {
		if r, ok := literalRat(*f.Max); ok && r.IsInt() {
			hi = r.Num()
		}
	}
	n := new(big.Int).Sub(hi, lo)
	return n.Add(n, big.NewInt(1))
}

func decimalCapacity(f *FieldDef) Capacity {
	if f.Min == nil || f.Max == nil {
		return Capacity{Supported: true, Known: true}
	}
	lo, okLo := literalRat(*f.Min)
	hi, okHi := literalRat(*f.Max)
	if !okLo || !okHi {
		return Capacity{Supported: true, Known: true}
	}
	scale := DefaultDecimalScale
	if f.Scale != nil {
		scale = *f.Scale
	}
	first, last := DecimalGrid(lo, hi, scale)
	n := new(big.Int).Sub(last, first)
	n.Add(n, big.NewInt(1))
	if n.Sign() < 0 {
		n.SetInt64(0)
	}
	return Capacity{Supported: true, Known: true, N: n}
}

// DecimalGrid returns the first and last representable values in [lo, hi] at a scale,
// as integers in units of 10^-scale. Bounds finer than the scale round inward.
func DecimalGrid(lo, hi *big.Rat, scale int) (first, last *big.Int) {
	unit := new(big.Rat).SetInt(pow10(scale))
	return ceilRat(new(big.Rat).Mul(lo, unit)), floorRat(new(big.Rat).Mul(hi, unit))
}

func floorRat(r *big.Rat) *big.Int {
	// Int.Div is Euclidean, so with a positive denominator this rounds toward -inf.
	return new(big.Int).Div(r.Num(), r.Denom())
}

func ceilRat(r *big.Rat) *big.Int {
	return new(big.Int).Neg(floorRat(new(big.Rat).Neg(r)))
}

// DefaultTemporalDays is the width of the range of a date or time field with at most
// one bound: it ends at the session's reference time, or extends from the given bound.
const DefaultTemporalDays = 3652

func temporalCapacity(f *FieldDef) Capacity {
	if f.Type == TypeTime && (f.Min == nil || f.Max == nil) {
		return Capacity{Supported: true, Known: true, N: big.NewInt(86400)}
	}
	if f.Min == nil || f.Max == nil {
		steps := int64(DefaultTemporalDays)
		if f.Type != TypeDate {
			steps *= 86400
		}
		return Capacity{Supported: true, Known: true, N: big.NewInt(steps + 1)}
	}
	lo, errLo := parseInstant(f.Type, f.Min.Text)
	hi, errHi := parseInstant(f.Type, f.Max.Text)
	if errLo != nil || errHi != nil {
		return Capacity{Supported: true, Known: true}
	}
	unit := int64(time.Second)
	if f.Type == TypeDate {
		unit = int64(24 * time.Hour)
	}
	seconds := new(big.Int).Sub(big.NewInt(hi.Unix()), big.NewInt(lo.Unix()))
	steps := seconds.Mul(seconds, big.NewInt(int64(time.Second)))
	steps.Quo(steps, big.NewInt(unit))
	return Capacity{Supported: true, Known: true, N: steps.Add(steps, big.NewInt(1))}
}

const base36 = 36

// stringCapacity allows free strings a base-36 unique suffix of up to max_length.
func stringCapacity(f *FieldDef) Capacity {
	if f.Pattern != "" {
		info, err := AnalyzePattern(f.Pattern)
		if err != nil || info.Capacity == nil {
			return Capacity{}
		}
		return Capacity{Supported: true, Known: true, N: info.Capacity}
	}
	maxLen := int64(DefaultStringMaxLength)
	if f.MaxLength != nil {
		maxLen = *f.MaxLength
	}
	if maxLen >= 13 { // 36^13 > 2^63
		return Capacity{Supported: true, Known: true}
	}
	n := new(big.Int).Exp(big.NewInt(base36), big.NewInt(maxLen), nil)
	return Capacity{Supported: true, Known: true, N: n}
}

func binaryCapacity(f *FieldDef) Capacity {
	maxLen := int64(DefaultStringMaxLength)
	if f.MaxLength != nil {
		maxLen = *f.MaxLength
	}
	if maxLen >= 8 {
		return Capacity{Supported: true, Known: true}
	}
	return Capacity{Supported: true, Known: true, N: new(big.Int).Lsh(big.NewInt(1), uint(8*maxLen))}
}

func pow10(n int) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }

// literalFitsDecimal reports whether a number has at most precision-scale integer digits.
func literalFitsDecimal(l Literal, precision, scale int) bool {
	r, ok := literalRat(l)
	if !ok {
		return true // reported separately
	}
	limit := new(big.Rat).SetInt(pow10(precision - scale))
	return new(big.Rat).Abs(r).Cmp(limit) < 0
}

// uniqueField checks that a field can take required distinct values.
func (v *validator) uniqueField(path string, f *FieldDef, required *big.Int) {
	c := UniqueCapacity(f)
	if !c.Supported {
		v.add(join(path, "unique"), CodeUniqueUnsupported,
			"type %s cannot produce guaranteed unique values%s", f.Type, patternHint(f))
		return
	}
	if !c.Known || c.N == nil || required == nil {
		return
	}
	if c.N.Cmp(required) < 0 {
		v.add(join(path, "unique"), CodeUniqueInfeasible,
			"only %s distinct values are possible, but up to %s rows need one each", c.N, required)
	}
}

func patternHint(f *FieldDef) string {
	if f.Pattern != "" {
		return ": a unique pattern must be a fixed-length sequence of literals and character classes"
	}
	return ""
}

// uniqueFlatFields checks a stream's top-level unique fields against its record count.
func (v *validator) uniqueFlatFields(path string, fields []FieldDef, maxRecords int64) {
	var required *big.Int
	if maxRecords > 0 {
		required = big.NewInt(maxRecords)
	}
	for i := range fields {
		f := &fields[i]
		if f.Unique && f.Type.isScalar() {
			v.uniqueField(join(path, f.Name), f, required)
		}
	}
}

// Bounds on flattening, which shared definitions could otherwise multiply.
const (
	MaxColumns     = 1 << 16
	MaxColumnDepth = maxTreeDepth
)

// Columns lists the flat column names of fields for tabular output, in order. Objects
// and nested schemas expand into dotted paths. Maps stay one column, and so do arrays
// unless arrayColumns expands them. Output stops at MaxColumns.
func Columns(fields []FieldDef, defs map[string]*Definition, separator string, arrayColumns int) []string {
	if separator == "" {
		separator = "."
	}
	var out []string
	var walk func(prefix string, fields []FieldDef, depth int)
	walk = func(prefix string, fields []FieldDef, depth int) {
		for i := range fields {
			if len(out) >= MaxColumns {
				return
			}
			f := &fields[i]
			name := prefix + f.Name
			switch {
			case depth >= maxTreeDepth:
				out = append(out, name)
			case f.Type == TypeObject && len(f.Fields) > 0:
				walk(name+separator, f.Fields, depth+1)
			case f.Type == TypeNestedSchema && defs[f.Schema] != nil:
				walk(name+separator, defs[f.Schema].Fields, depth+1)
			case f.Type == TypeArray && arrayColumns > 0:
				for j := range arrayColumns {
					out = append(out, name+separator+strconv.Itoa(j))
				}
			default:
				out = append(out, name)
			}
		}
	}
	walk("", fields, 0)
	return out
}

func saturatingAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func saturatingMul(a, b int64) int64 {
	if a != 0 && b > math.MaxInt64/a {
		return math.MaxInt64
	}
	return a * b
}

// lengthBound is the longest a string or binary value can be.
func lengthBound(f *FieldDef) int64 {
	if f.MaxLength != nil {
		return *f.MaxLength
	}
	if f.MinLength != nil && *f.MinLength > DefaultStringMaxLength {
		return *f.MinLength
	}
	return DefaultStringMaxLength
}

// MaxRecordBytes bounds the serialized size of records with these fields. Definition
// cycles must be ruled out first, which Validate does.
func MaxRecordBytes(fields []FieldDef, defs []Definition) int64 {
	m := make(map[string]*Definition, len(defs))
	for i := range defs {
		m[defs[i].Name] = &defs[i]
	}
	return fieldsMaxBytes(fields, m)
}

// sizer computes worst-case serialized sizes. Definitions are memoized, so definitions
// that reference each other many times stay linear to evaluate.
type sizer struct {
	defs map[string]*Definition
	memo map[string]int64
}

// fieldsMaxBytes bounds the serialized size of a record with these fields. The limit
// is what keeps the worst-case memory calculation honest. Definition cycles must be
// ruled out first.
func fieldsMaxBytes(fields []FieldDef, defs map[string]*Definition) int64 {
	z := &sizer{defs: defs, memo: map[string]int64{}}
	return z.fields(fields)
}

func (z *sizer) fields(fields []FieldDef) int64 {
	var total int64 = 2
	for i := range fields {
		f := &fields[i]
		total = saturatingAdd(total, int64(len(f.Name))+jsonOverhead)
		total = saturatingAdd(total, z.field(f))
	}
	return total
}

func (z *sizer) field(f *FieldDef) int64 {
	if f.Default != nil && f.Default.Kind != LiteralNull {
		// The default competes with generated values, so it only ever raises the bound.
		return max(int64(len(f.Default.Text))*2+jsonOverhead, z.value(f))
	}
	return z.value(f)
}

func (z *sizer) definition(name string) int64 {
	if n, ok := z.memo[name]; ok {
		return n
	}
	d := z.defs[name]
	if d == nil {
		return 0
	}
	z.memo[name] = 0
	n := z.fields(d.Fields)
	z.memo[name] = n
	return n
}

func (z *sizer) value(f *FieldDef) int64 {
	if len(f.Values) > 0 {
		var longest int64
		for _, l := range f.Values {
			longest = max(longest, int64(len(l.Text)))
		}
		return longest*2 + jsonOverhead
	}
	if f.Generator != nil && f.Generator.Name == "vocab" {
		return MaxVocabularyValueBytes
	}

	collectionLen := int64(DefaultCollectionMax)
	if f.MaxLength != nil {
		collectionLen = *f.MaxLength
	}

	switch f.Type {
	case TypeNull, TypeBoolean:
		return 5
	case TypeInteger:
		return 11
	case TypeLong:
		return 20
	case TypeFloat:
		return 24
	case TypeDecimal, TypeMoney:
		return MaxDecimalPrecision + 2
	case TypeUUID:
		return 36
	case TypeDate, TypeTime:
		return 12
	case TypeDateTime, TypeTimestamp:
		return 35
	case TypeIPv4:
		return 15
	case TypeIPv6, TypeIPAddress:
		return 45
	case TypeReference:
		return 128
	case TypeString:
		if f.Pattern != "" {
			if info, err := AnalyzePattern(f.Pattern); err == nil {
				return saturatingMul(info.MaxLen, bytesPerCharacter)
			}
		}
		return saturatingMul(lengthBound(f), bytesPerCharacter)
	case TypeBinary:
		return saturatingAdd(lengthBound(f)/3*4, 4) // base64
	case TypeObject:
		return z.fields(f.Fields)
	case TypeNestedSchema:
		return z.definition(f.Schema)
	case TypeArray:
		if f.Items == nil {
			return 2
		}
		return saturatingAdd(2, saturatingMul(collectionLen, saturatingAdd(z.field(f.Items), 1)))
	case TypeMap:
		if f.MapValue == nil {
			return 2
		}
		keySize := int64(DefaultStringMaxLength * bytesPerCharacter)
		if f.MapKey != nil {
			keySize = z.field(f.MapKey)
		}
		one := saturatingAdd(saturatingAdd(keySize, z.field(f.MapValue)), 2)
		return saturatingAdd(2, saturatingMul(collectionLen, one))
	}
	if n, ok := semanticMaxLength[f.Type]; ok {
		return n
	}
	return 0
}

// nestedMaxBytes bounds a nested document: a root row with its owned rows embedded,
// multiplying by per_parent.max at each level.
func nestedMaxBytes(root *Table, r *RelationalSpec, defs map[string]*Definition) int64 {
	var size func(t *Table, depth int) int64
	size = func(t *Table, depth int) int64 {
		total := fieldsMaxBytes(t.Fields, defs)
		if depth >= len(r.Tables) {
			return total
		}
		for i := range r.Tables {
			child := &r.Tables[i]
			if child.Owner != nil && child.Owner.Table == t.Name {
				total = saturatingAdd(total, saturatingMul(child.Owner.PerParent.Max, size(child, depth+1)))
			}
		}
		return total
	}
	return size(root, 0)
}

// SetRootCount sets the rows of a relational schema's only root table. With several roots
// a single count would be ambiguous, so each must set its own.
func SetRootCount(s *Schema, count uint64) error {
	if s.Relational == nil {
		return nil
	}
	var root *Table
	for i := range s.Relational.Tables {
		t := &s.Relational.Tables[i]
		if t.Owner != nil {
			continue
		}
		if root != nil {
			return errors.New("ambiguous: this schema has several tables without an owner, so set each one's count in the schema")
		}
		root = t
	}
	if root == nil {
		return nil // validation reports the ownership problem
	}
	if count > math.MaxInt64 {
		return errors.New("too large")
	}
	n := int64(count)
	root.Count = &n
	return nil
}
