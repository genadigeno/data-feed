// Package schema defines the data shapes the engine generates: the type system,
// constraints, the three schema kinds, their document format and their validation.
// It knows what a value is, never how one is produced. See docs/02-domain/schema-model.md.
package schema

import (
	"fmt"
	"time"
)

// CurrentVersion is the schema document format version.
const CurrentVersion = 1

// Kind selects the generation strategy a schema describes.
type Kind string

const (
	KindRandom     Kind = "random"
	KindHybrid     Kind = "hybrid"
	KindRelational Kind = "relational"
)

// Schema is a parsed schema document. Exactly one of Random, Hybrid and Relational
// is set, matching Kind.
type Schema struct {
	Version     int
	Kind        Kind
	Definitions []Definition
	Random      *RandomSpec
	Hybrid      *HybridSpec
	Relational  *RelationalSpec
}

// Definition is a named object shape used by nested_schema fields.
type Definition struct {
	Name   string
	Fields []FieldDef
}

// IntRange is an inclusive range.
type IntRange struct {
	Min, Max int64
}

// TypeWeight gives a type's relative likelihood in random generation.
type TypeWeight struct {
	Type   TypeName
	Weight float64
}

// RandomProbabilities is the alternative to explicit type weights: these three are
// fixed and the remainder is shared evenly by the scalar types.
type RandomProbabilities struct {
	Null, Object, Array float64
}

// FloatRange bounds random floats; Precision is decimal places, 0 meaning unrounded.
type FloatRange struct {
	Min, Max  float64
	Precision int
}

// FieldNames configures generated field names.
type FieldNames struct {
	Style           string // words | camel | snake | random
	VocabularyGroup string
	MinLen, MaxLen  int
	UniquePerObject bool
}

// RandomSpec configures schemaless generation.
type RandomSpec struct {
	Fields         IntRange
	Depth          IntRange
	ArrayLength    IntRange
	StringLength   IntRange
	Integer        IntRange
	Float          FloatRange
	Types          []TypeWeight         // nil when Probabilities is used
	Probabilities  *RandomProbabilities // nil when Types is used
	FieldNames     FieldNames
	MaxRecordBytes int64
}

// DefaultRandomSpec returns the documented defaults.
func DefaultRandomSpec() RandomSpec {
	return RandomSpec{
		Fields:       IntRange{3, 12},
		Depth:        IntRange{0, 3},
		ArrayLength:  IntRange{0, 8},
		StringLength: IntRange{3, 24},
		Integer:      IntRange{-1 << 31, 1<<31 - 1},
		Float:        FloatRange{Min: 0, Max: 1e6},
		Types: []TypeWeight{
			{TypeString, 30}, {TypeInteger, 20}, {TypeFloat, 10}, {TypeBoolean, 10},
			{TypeNull, 5}, {TypeObject, 15}, {TypeArray, 10},
		},
		FieldNames:     FieldNames{Style: "words", MinLen: 3, MaxLen: 16, UniquePerObject: true},
		MaxRecordBytes: 64 << 10,
	}
}

// HybridSpec is a contract of required and optional fields plus optional randomness.
type HybridSpec struct {
	Fields      []FieldDef
	ExtraRandom *ExtraRandom
}

// ExtraRandom adds random fields to a share of hybrid records.
type ExtraRandom struct {
	Probability float64
	Spec        RandomSpec
}

// RelationalSpec is a set of related tables, in document order.
type RelationalSpec struct {
	Tables []Table
}

// Table returns the named table, or nil.
func (r *RelationalSpec) Table(name string) *Table {
	for i := range r.Tables {
		if r.Tables[i].Name == name {
			return &r.Tables[i]
		}
	}
	return nil
}

// Table is one relational table. A root table has Count; an owned table has Owner,
// and its rows are produced per parent row.
type Table struct {
	Name       string
	Count      *int64
	PrimaryKey string
	Owner      *Owner
	Fields     []FieldDef
	UniqueKeys []UniqueKey
}

// Field returns the named field, or nil.
func (t *Table) Field(name string) *FieldDef {
	return findField(t.Fields, name)
}

// OwnerField returns the foreign key marked as the owning one, or nil.
func (t *Table) OwnerField() *FieldDef {
	for i := range t.Fields {
		if t.Fields[i].Ref != nil && t.Fields[i].Ref.Owner {
			return &t.Fields[i]
		}
	}
	return nil
}

func findField(fields []FieldDef, name string) *FieldDef {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}

// Owner declares that each row of the owning table has PerParent rows of this one.
type Owner struct {
	Table     string
	PerParent PerParent
}

// PerParent is the cardinality of an owning relationship.
type PerParent struct {
	Min, Max     int64
	Distribution *Distribution
}

// UniqueKey is a composite uniqueness constraint. Driver names the column whose own
// uniqueness guarantees the composite's; it may be empty for (owner key, ordinal).
type UniqueKey struct {
	Fields []string
	Driver string
}

// Presence says whether a hybrid field appears in a record.
type Presence struct {
	Optional    bool
	Probability float64 // used when Optional
}

// FieldDef defines one field, or the element of a collection (with an empty Name).
type FieldDef struct {
	Name     string
	Type     TypeName
	Presence Presence

	Nullable           bool
	NullProbability    *float64
	Unique             bool
	Default            *Literal
	DefaultProbability *float64

	Min, Max             *Literal
	MinLength, MaxLength *int64
	Pattern              string
	Values               []Literal
	Weights              []float64
	Distribution         *Distribution

	Precision, Scale *int   // decimal, money
	Format           string // timestamp: rfc3339 | unix | unix_ms
	Zone             string // datetime
	Ranges           string // IP types: documentation | any

	Generator *GeneratorRef

	Fields   []FieldDef // object
	Items    *FieldDef  // array
	MapKey   *FieldDef  // map, optional
	MapValue *FieldDef  // map
	Schema   string     // nested_schema: a definition's name

	Ref    *Reference // reference
	Derive *Derive
}

// GeneratorRef overrides how a value is produced. Factories validate Params.
type GeneratorRef struct {
	Name   string
	Params []Param
}

// Param is one generator parameter, in document order. Value is a Literal, or a
// []Literal for a list.
type Param struct {
	Name  string
	Value any
}

// Reference is a foreign key.
type Reference struct {
	Table, Field string
	Owner        bool
	Select       *Select
}

// Reference selection modes.
const (
	SelectAny     = ""
	SelectEarlier = "earlier" // self references: a row before this one, roots are null
)

// Select configures how a selecting foreign key chooses its parent row.
type Select struct {
	Distribution *Distribution
	Mode         string
	Distinct     bool // no repeated parent among the children of one owner
}

// Derive copies a field from the owning row, optionally offset in time.
type Derive struct {
	From                 string // only "owner" is defined
	Field                string
	OffsetMin, OffsetMax *time.Duration
}

// Distribution types.
const (
	DistUniform     = "uniform"
	DistNormal      = "normal"
	DistExponential = "exponential"
	DistZipf        = "zipf"
	DistSequence    = "sequence"
)

// Distribution shapes how values spread within their range. Unset parameters take
// the generator's defaults, where a distribution has any.
type Distribution struct {
	Type         string
	Mean, StdDev *float64 // normal
	Rate         *float64 // exponential
	S, V         *float64 // zipf
	Start, Step  *int64   // sequence
}

// LiteralKind is the document-level type of a Literal.
type LiteralKind uint8

const (
	LiteralNull LiteralKind = iota
	LiteralBool
	LiteralNumber
	LiteralString
)

// Literal is a scalar from the document. Numbers keep their source text so decimals
// and 64-bit integers stay exact.
type Literal struct {
	Kind LiteralKind
	Text string
}

// Problem is one issue found in a schema document.
type Problem struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p Problem) String() string {
	if p.Path == "" {
		return p.Code + ": " + p.Message
	}
	return p.Path + ": " + p.Code + ": " + p.Message
}

// Problems is a list of issues; all are reported together.
type Problems []Problem

func (ps Problems) Error() string {
	switch len(ps) {
	case 0:
		return "no problems"
	case 1:
		return ps[0].String()
	}
	return fmt.Sprintf("%s (and %d more)", ps[0], len(ps)-1)
}

// maxProblems is plenty to fix a document by, and keeps a hostile one from filling memory.
const maxProblems = 200

func (ps *Problems) add(path, code, format string, args ...any) {
	switch {
	case len(*ps) > maxProblems:
	case len(*ps) == maxProblems:
		*ps = append(*ps, Problem{Code: CodeLimit, Message: "too many problems: fix these and check again"})
	default:
		*ps = append(*ps, Problem{Path: path, Code: code, Message: fmt.Sprintf(format, args...)})
	}
}
