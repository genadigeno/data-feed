package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/snapshot"
)

// Modes.
const (
	ModeBatch  = "batch"
	ModeStream = "stream"
)

// SpecAPIVersion is the specification document's version.
const SpecAPIVersion = "datafeed/v1"

// unboundedRows stands for "no limit" where a row count is needed.
const unboundedRows = uint64(1) << 63

// Duration reads "30s" in YAML and JSON.
type Duration time.Duration

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("invalid duration %q", b)
	}
	*d = Duration(v)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) { return []byte(time.Duration(d).String()), nil }

// InlineSchema captures an embedded schema document as it was written, because field
// order matters and generic decoding would lose it. It may also be a string holding a
// document, which is how a tool carries a YAML file inside a JSON request.
type InlineSchema struct{ Doc []byte }

// UnmarshalJSON implements json.Unmarshaler.
func (s *InlineSchema) UnmarshalJSON(b []byte) error {
	var text string
	if json.Unmarshal(b, &text) == nil {
		s.Doc = []byte(text)
		return nil
	}
	s.Doc = bytes.Clone(b)
	return nil
}

// MarshalJSON implements json.Marshaler. A schema written in YAML becomes canonical JSON,
// which keeps its field order.
func (s InlineSchema) MarshalJSON() ([]byte, error) {
	if json.Valid(s.Doc) {
		return s.Doc, nil
	}
	parsed, problems := schema.Decode(s.Doc, schema.FormatAuto)
	if len(problems) > 0 {
		return nil, problems
	}
	return schema.EncodeJSON(parsed), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (s *InlineSchema) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		s.Doc = []byte(n.Value)
		return nil
	}
	b, err := yaml.Marshal(n)
	if err != nil {
		return err
	}
	s.Doc = b
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (s InlineSchema) MarshalYAML() (any, error) {
	var n yaml.Node
	if err := yaml.Unmarshal(s.Doc, &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// SchemaRef names a saved schema, with version 0 meaning the latest.
type SchemaRef struct {
	ID      string `json:"id" yaml:"id"`
	Version int    `json:"version,omitempty" yaml:"version,omitempty"`
}

// CatalogRef names a catalog: a saved one, or builtin:<domain>.
type CatalogRef struct {
	ID      string `json:"id" yaml:"id"`
	Version int    `json:"version,omitempty" yaml:"version,omitempty"`
}

// Spec is the session specification document, accepted by every interface.
type Spec struct {
	APIVersion  string            `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	Kind        string            `json:"kind,omitempty" yaml:"kind,omitempty"`
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`

	Strategy string `json:"strategy,omitempty" yaml:"strategy,omitempty"`
	Schema   struct {
		Ref    *SchemaRef    `json:"ref,omitempty" yaml:"ref,omitempty"`
		Inline *InlineSchema `json:"inline,omitempty" yaml:"inline,omitempty"`
	} `json:"schema" yaml:"schema"`
	Catalog *struct {
		Ref CatalogRef `json:"ref" yaml:"ref"`
	} `json:"catalog,omitempty" yaml:"catalog,omitempty"`

	Mode   string `json:"mode" yaml:"mode"`
	Limits Limits `json:"limits,omitempty" yaml:"limits,omitempty"`

	Seed          *uint64    `json:"seed,omitempty" yaml:"seed,omitempty"`
	ReferenceTime *time.Time `json:"reference_time,omitempty" yaml:"reference_time,omitempty"`

	Execution struct {
		BatchSize      int               `json:"batch_size,omitempty" yaml:"batch_size,omitempty"`
		UnitRecords    int64             `json:"unit_records,omitempty" yaml:"unit_records,omitempty"`
		WorkerSelector map[string]string `json:"worker_selector,omitempty" yaml:"worker_selector,omitempty"`
		Debug          bool              `json:"debug,omitempty" yaml:"debug,omitempty"`
	} `json:"execution,omitempty" yaml:"execution,omitempty"`

	Output OutputSpec `json:"output" yaml:"output"`

	Retention struct {
		Pin bool `json:"pin,omitempty" yaml:"pin,omitempty"`
	} `json:"retention,omitempty" yaml:"retention,omitempty"`
}

// Limits bound a session.
type Limits struct {
	Count      uint64   `json:"count,omitempty" yaml:"count,omitempty"`
	Duration   Duration `json:"duration,omitempty" yaml:"duration,omitempty"`
	Rate       float64  `json:"rate,omitempty" yaml:"rate,omitempty"`
	MaxWorkers int      `json:"max_workers,omitempty" yaml:"max_workers,omitempty"`
	MaxBytes   int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
}

// OutputSpec says what the output looks like.
type OutputSpec struct {
	Format         string   `json:"format" yaml:"format"`
	Compression    string   `json:"compression,omitempty" yaml:"compression,omitempty"`
	PartMaxBytes   string   `json:"part_max_bytes,omitempty" yaml:"part_max_bytes,omitempty"`
	PartMaxRecords int64    `json:"part_max_records,omitempty" yaml:"part_max_records,omitempty"`
	PartMaxAge     Duration `json:"part_max_age,omitempty" yaml:"part_max_age,omitempty"`
	JSON           struct {
		Pretty bool `json:"pretty,omitempty" yaml:"pretty,omitempty"`
	} `json:"json,omitempty" yaml:"json,omitempty"`
	XML struct {
		Root   string `json:"root,omitempty" yaml:"root,omitempty"`
		Record string `json:"record,omitempty" yaml:"record,omitempty"`
		Null   string `json:"null,omitempty" yaml:"null,omitempty"`
	} `json:"xml,omitempty" yaml:"xml,omitempty"`
	CSV struct {
		Delimiter        string `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
		NoHeader         bool   `json:"no_header,omitempty" yaml:"no_header,omitempty"`
		Null             string `json:"null,omitempty" yaml:"null,omitempty"`
		Separator        string `json:"separator,omitempty" yaml:"separator,omitempty"`
		ArrayColumns     int    `json:"array_columns,omitempty" yaml:"array_columns,omitempty"`
		SchemalessLayout string `json:"schemaless_layout,omitempty" yaml:"schemaless_layout,omitempty"`
	} `json:"csv,omitempty" yaml:"csv,omitempty"`
	Relational struct {
		Layout string `json:"layout,omitempty" yaml:"layout,omitempty"`
		Root   string `json:"root,omitempty" yaml:"root,omitempty"`
	} `json:"relational,omitempty" yaml:"relational,omitempty"`
}

// Floors on how finely output is cut: below these, one record could cost a file, a database
// row and a sync to disk.
const (
	minPartBytes   = 64 << 10
	minPartRecords = 100
	minUnitRecords = 100
	minPartAge     = time.Second
)

// StoredLimits is what the Coordinator needs at run time, kept with the session.
type StoredLimits struct {
	Count           uint64            `json:"count,omitempty"`
	DurationMS      int64             `json:"duration_ms,omitempty"`
	MaxBytes        int64             `json:"max_bytes,omitempty"`
	Selector        map[string]string `json:"selector,omitempty"`
	UnitMemoryBytes int64             `json:"unit_memory_bytes,omitempty"`
	Debug           bool              `json:"debug,omitempty"`
}

// ParseSpec reads a specification from YAML or JSON; unknown keys are errors, so a typo
// cannot silently fall back to a default.
func ParseSpec(data []byte) (*Spec, error) {
	var s Spec
	trimmed := bytes.TrimSpace(data)
	var err error
	if len(trimmed) > 0 && trimmed[0] == '{' {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		err = dec.Decode(&s)
	} else {
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		err = dec.Decode(&s)
		if errors.Is(err, io.EOF) {
			err = errors.New("document is empty")
		}
	}
	if err != nil {
		return nil, apperr.Validation(CodeValidation, "invalid session specification", []apperr.Problem{
			{Code: "ERR_SPEC_SYNTAX", Message: err.Error()},
		})
	}
	return &s, nil
}

// CodeValidation marks a specification with problems.
const CodeValidation = "ERR_VALIDATION"

// Resolver finds what a specification refers to.
type Resolver interface {
	// SchemaDocument returns a saved schema's canonical JSON.
	SchemaDocument(ref SchemaRef) ([]byte, error)
	// CatalogContent returns a pinned catalog's content and the version it resolved to.
	CatalogContent(ref CatalogRef) (content json.RawMessage, version int, err error)
}

// Compiled is a validated specification ready to store.
type Compiled struct {
	Spec     *Spec
	Snapshot *snapshot.Snapshot
	Limits   StoredLimits
	Strategy string
	Catalog  CatalogRef
}

// Compile validates a specification against limits and resolves it into a snapshot.
func Compile(spec *Spec, cfg *config.Config, r Resolver, vocab snapshot.VocabularyDecoder, seed uint64, now time.Time) (*Compiled, error) {
	var problems []apperr.Problem
	add := func(path, code, msg string) {
		problems = append(problems, apperr.Problem{Path: path, Code: code, Message: msg})
	}

	if spec.APIVersion != "" && spec.APIVersion != SpecAPIVersion {
		add("apiVersion", "ERR_SPEC_VERSION", fmt.Sprintf("want %s", SpecAPIVersion))
	}
	if spec.Kind != "" && spec.Kind != "Session" {
		add("kind", "ERR_BAD_VALUE", "want Session")
	}
	if strings.TrimSpace(spec.Name) == "" {
		add("name", "ERR_MISSING_KEY", "required")
	}
	if spec.Mode != ModeBatch && spec.Mode != ModeStream {
		add("mode", "ERR_BAD_VALUE", "want batch or stream")
	}

	g := cfg.Generation
	if spec.Limits.MaxWorkers < 0 || spec.Limits.MaxWorkers > g.MaxWorkersPerSession {
		add("limits.max_workers", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be between 0 and %d", g.MaxWorkersPerSession))
	}
	if spec.Limits.Rate < 0 {
		add("limits.rate", "ERR_BAD_VALUE", "must not be negative")
	}
	if spec.Limits.Duration < 0 {
		add("limits.duration", "ERR_BAD_VALUE", "must not be negative")
	}
	if spec.Limits.Duration > 0 && spec.Mode == ModeBatch {
		add("limits.duration", "ERR_BAD_VALUE", "applies to stream mode")
	}
	if spec.Limits.MaxBytes < 0 {
		add("limits.max_bytes", "ERR_BAD_VALUE", "must not be negative")
	}
	if g.MaxTotalRecords > 0 && spec.Limits.Count > uint64(g.MaxTotalRecords) {
		add("limits.count", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must not exceed %d", g.MaxTotalRecords))
	}
	batch := spec.Execution.BatchSize
	if batch == 0 {
		batch = g.DefaultBatchSize
	}
	if batch < 1 || batch > g.MaxBatchSize {
		add("execution.batch_size", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be between 1 and %d", g.MaxBatchSize))
	}
	unitRecords := spec.Execution.UnitRecords
	if unitRecords == 0 {
		unitRecords = g.UnitRecords
	}
	if unitRecords < 1 {
		add("execution.unit_records", "ERR_BAD_VALUE", "must be greater than zero")
	}

	out := spec.Output
	switch out.Format {
	case serial.FormatJSON, serial.FormatNDJSON, serial.FormatXML, serial.FormatCSV:
	default:
		add("output.format", "ERR_BAD_VALUE", "want json, ndjson, xml or csv")
	}
	switch out.Compression {
	case "", "none", "gzip":
	default:
		add("output.compression", "ERR_BAD_VALUE", "want none or gzip")
	}
	compression := out.Compression
	if compression == "" {
		compression = cfg.Storage.Compression
	}
	partBytes := g.PartMaxBytes
	if out.PartMaxBytes != "" {
		size, err := config.ParseSize(out.PartMaxBytes)
		switch {
		case err != nil:
			add("output.part_max_bytes", "ERR_BAD_VALUE", err.Error())
		case size > g.PartMaxBytes || size < minPartBytes:
			add("output.part_max_bytes", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be between %s and %s", config.Size(minPartBytes), g.PartMaxBytes))
		default:
			partBytes = size
		}
	}
	if out.PartMaxRecords != 0 && out.PartMaxRecords < minPartRecords {
		add("output.part_max_records", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be 0 for no limit, or at least %d", minPartRecords))
	}
	if out.CSV.ArrayColumns < 0 || out.CSV.ArrayColumns > schema.MaxColumns {
		add("output.csv.array_columns", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be between 0 and %d", schema.MaxColumns))
	}
	if spec.Execution.UnitRecords != 0 && spec.Execution.UnitRecords < minUnitRecords {
		add("execution.unit_records", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be 0 for the default, or at least %d", minUnitRecords))
	}
	partAge := time.Duration(out.PartMaxAge)
	if out.PartMaxAge != 0 && partAge < minPartAge {
		add("output.part_max_age", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("must be 0 for the default, or at least %s", minPartAge))
	}
	if partAge == 0 && spec.Mode == ModeStream {
		partAge = g.PartMaxAge
	}
	encoder := serial.Options{
		Pretty: out.JSON.Pretty, XMLRoot: out.XML.Root, XMLRecord: out.XML.Record, XMLNull: out.XML.Null,
		CSVNoHeader: out.CSV.NoHeader, CSVNull: out.CSV.Null, CSVSeparator: out.CSV.Separator,
		CSVArrayColumns: out.CSV.ArrayColumns, CSVLayout: out.CSV.SchemalessLayout,
	}
	if out.CSV.Delimiter != "" {
		if len(out.CSV.Delimiter) != 1 {
			add("output.csv.delimiter", "ERR_BAD_VALUE", "must be one character")
		} else {
			encoder.CSVDelimiter = out.CSV.Delimiter[0]
		}
	}
	nestedRoot := ""
	switch out.Relational.Layout {
	case "", "normalized":
		if out.Relational.Root != "" {
			add("output.relational.root", "ERR_BAD_VALUE", "applies to the nested layout")
		}
	case "nested":
		nestedRoot = out.Relational.Root
		if nestedRoot == "" {
			add("output.relational.root", "ERR_MISSING_KEY", "the nested layout needs a root table")
		}
	default:
		add("output.relational.layout", "ERR_BAD_VALUE", "want normalized or nested")
	}

	doc, err := schemaDocument(spec, r)
	if err != nil {
		if e, ok := apperr.As(err); ok && e.Kind == apperr.Internal {
			return nil, err
		}
		add("schema", "ERR_SCHEMA", err.Error())
	}

	if len(problems) > 0 && doc == nil {
		return nil, apperr.Validation(CodeValidation, "invalid session specification", problems)
	}

	parsed, schemaProblems := schema.Decode(doc, schema.FormatAuto)
	for _, p := range schemaProblems {
		problems = append(problems, apperr.Problem{Path: prefixed("schema", p.Path), Code: p.Code, Message: p.Message})
	}
	if len(schemaProblems) > 0 {
		return nil, apperr.Validation(CodeValidation, "invalid session specification", problems)
	}

	strategy := string(parsed.Kind)
	if spec.Strategy != "" && spec.Strategy != strategy {
		add("strategy", "ERR_BAD_VALUE", fmt.Sprintf("the schema is %s, not %s", strategy, spec.Strategy))
	}
	if spec.Limits.Count > 0 && parsed.Relational != nil {
		if err := schema.SetRootCount(parsed, spec.Limits.Count); err != nil {
			add("limits.count", "ERR_BAD_VALUE", err.Error())
		}
	}
	if spec.Mode == ModeBatch && spec.Limits.Count == 0 && parsed.Relational == nil {
		add("limits.count", "ERR_MISSING_KEY", "batch mode needs a record count")
	}

	maxRows := spec.Limits.Count
	opts := schema.Options{
		Limits: schema.Limits{
			MaxDepth: int64(g.RandomCaps.MaxDepth), MaxFields: int64(g.RandomCaps.MaxFields),
			MaxArrayLength: int64(g.RandomCaps.MaxArrayLength), MaxStringLength: int64(g.RandomCaps.MaxStringLength),
			MaxRecordBytes: int64(g.MaxRecordSizeBytes),
		},
		Output: schema.Output{
			Format: out.Format, Separator: out.CSV.Separator, ArrayColumns: out.CSV.ArrayColumns,
			Nested: nestedRoot != "", Root: nestedRoot,
		},
	}
	switch {
	case maxRows > 0 && maxRows < unboundedRows:
		opts.MaxRecords = int64(maxRows)
	case maxRows == 0 && parsed.Relational == nil:
		opts.MaxRecords = 1<<63 - 1 // an unlimited stream needs unique values for any index
	}
	for _, p := range schema.Validate(parsed, opts) {
		problems = append(problems, apperr.Problem{Path: prefixed("schema", p.Path), Code: p.Code, Message: p.Message})
	}

	refTime := now.UTC().Truncate(time.Second)
	if spec.ReferenceTime != nil {
		refTime = spec.ReferenceTime.UTC()
	}
	if spec.Seed != nil {
		seed = *spec.Seed
	}

	var catalogRef CatalogRef
	var catalog json.RawMessage
	if spec.Catalog != nil {
		content, version, err := r.CatalogContent(spec.Catalog.Ref)
		if err != nil {
			if e, ok := apperr.As(err); ok && e.Kind == apperr.Internal {
				return nil, err
			}
			add("catalog", "ERR_CATALOG", err.Error())
		} else {
			catalog = content
			catalogRef = CatalogRef{ID: spec.Catalog.Ref.ID, Version: version}
			if int64(len(content)) > int64(g.MaxSnapshotBytes) {
				add("catalog", "ERR_LIMIT_EXCEEDED", fmt.Sprintf("the catalog exceeds generation.max_snapshot_bytes (%s)", g.MaxSnapshotBytes))
			}
		}
	}

	if len(problems) > 0 {
		return nil, apperr.Validation(CodeValidation, "invalid session specification", problems)
	}

	snap := &snapshot.Snapshot{
		EngineVersion:  gen.Version,
		Schema:         schema.EncodeJSON(parsed),
		Seed:           seed,
		ReferenceTime:  refTime,
		Mode:           spec.Mode,
		Format:         out.Format,
		NestedRoot:     nestedRoot,
		MaxRows:        maxRows,
		Compression:    compression,
		Encoder:        encoder,
		BatchSize:      batch,
		QueueCapacity:  g.QueueCapacity,
		UnitRecords:    unitRecords,
		UnitMaxBytes:   int64(g.UnitMaxBytes),
		PartMaxBytes:   int64(partBytes),
		PartMaxRecords: out.PartMaxRecords,
		PartMaxAgeMS:   partAge.Milliseconds(),
		Catalog:        catalog,
	}
	compiled, err := snap.Compile(vocab)
	if err != nil {
		return nil, apperr.Validation(CodeValidation, "invalid session specification", []apperr.Problem{
			{Path: "schema", Code: "ERR_SCHEMA", Message: err.Error()},
		})
	}

	// The largest record decides how much memory one running unit can need.
	var maxRecord int64
	for _, s := range compiled.Engine.Streams() {
		maxRecord = max(maxRecord, s.MaxRecordBytes)
	}
	unitMemory := int64(g.QueueCapacity+2) * int64(batch) * min(maxRecord, int64(g.MaxRecordSizeBytes))

	return &Compiled{
		Spec: spec, Snapshot: snap, Strategy: strategy, Catalog: catalogRef,
		Limits: StoredLimits{
			Count: spec.Limits.Count, DurationMS: time.Duration(spec.Limits.Duration).Milliseconds(), MaxBytes: spec.Limits.MaxBytes,
			Selector: spec.Execution.WorkerSelector, UnitMemoryBytes: unitMemory, Debug: spec.Execution.Debug,
		},
	}, nil
}

func prefixed(prefix, path string) string {
	if path == "" {
		return prefix
	}
	return prefix + "." + path
}

func schemaDocument(spec *Spec, r Resolver) ([]byte, error) {
	switch {
	case spec.Schema.Inline != nil && spec.Schema.Ref != nil:
		return nil, errors.New("set either ref or inline, not both")
	case spec.Schema.Inline != nil:
		return spec.Schema.Inline.Doc, nil
	case spec.Schema.Ref != nil:
		return r.SchemaDocument(*spec.Schema.Ref)
	}
	// With no schema at all, a session generates schemaless random data.
	if spec.Strategy == "" || spec.Strategy == string(schema.KindRandom) {
		return []byte("kind: random\n"), nil
	}
	return nil, errors.New("a schema is required for this strategy")
}

// Rate returns the session's target in records per second, 0 for none.
func (s *Spec) Rate() float64 { return s.Limits.Rate }
