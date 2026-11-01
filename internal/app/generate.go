package app

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/gen/value"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/vocab"
)

// Generation modes.
const (
	ModeBatch  = "batch"
	ModeStream = "stream"

	// StdoutPath selects standard output.
	StdoutPath = "-"

	// unboundedEnd ends the index space of an unlimited stream.
	unboundedEnd = uint64(1) << 63

	defaultRandomSchema = "kind: random\n"
	codeGenerateUsage   = "ERR_USAGE"
)

// GenerateOptions configure session-less generation.
type GenerateOptions struct {
	SchemaPath string // empty means schemaless random data
	Mode       string // batch | stream
	Count      uint64 // records; 0 means not set
	Rate       float64
	Duration   time.Duration
	Format     string
	Output     string // "-" for standard output, a file, or a directory
	Seed       *uint64
	// ReferenceTime fixes "now" for generated dates. Reproducing a dataset needs the seed
	// and this.
	ReferenceTime *time.Time
	Workers       int
	BatchSize     int
	Table         string // restrict to one table
	NestedRoot    string
	PartMaxBytes  config.Size
	Compression   string
	Encoder       serial.Options
	Stats         *pipeline.Stats
	// Catalog is a built-in catalog's domain, as direct generation has no database.
	Catalog string
}

// builtinVocabulary loads a built-in catalog. Saved catalogs live in a Coordinator's
// database, which direct generation doesn't touch.
func builtinVocabulary(ref string) (value.Vocabulary, error) {
	usage := func(msg string) error { return apperr.New(apperr.InvalidArgument, codeGenerateUsage, msg) }
	domain, ok := strings.CutPrefix(ref, vocab.BuiltinPrefix)
	if !ok {
		return nil, usage("direct generation can only use built-in catalogs, as builtin:<domain>: for a saved one, run a session")
	}
	doc, ok := vocab.Builtin(domain)
	if !ok {
		return nil, usage("no built-in catalog " + domain + ": there are " + strings.Join(vocab.Builtins(), ", "))
	}
	return vocab.Compile(&vocab.Content{Groups: doc.Groups})
}

// GenerateResult reports what was produced.
type GenerateResult struct {
	Seed          uint64    `json:"seed"`
	ReferenceTime time.Time `json:"reference_time"`
	Records       uint64    `json:"records"`
	Bytes         int64     `json:"bytes"`
	Parts         int       `json:"parts"`
	Duration      string    `json:"duration"`
	Output        string    `json:"output"`
	Interrupted   bool      `json:"interrupted"`
}

// Generate runs the engine and writes the output directly, with no Coordinator, session
// or database. Cancelling ctx finishes cleanly: what was written so far stays valid.
func Generate(ctx context.Context, cfg *config.Config, o GenerateOptions, stdout io.Writer, clk clock.Clock) (result GenerateResult, err error) {
	start := clk.Now()

	s, err := loadGenerateSchema(cfg, &o)
	if err != nil {
		return result, err
	}

	seed, err := chooseSeed(o.Seed)
	if err != nil {
		return result, err
	}
	refTime := clk.Now().UTC().Truncate(time.Second)
	if o.ReferenceTime != nil {
		refTime = o.ReferenceTime.UTC()
	}
	result.Seed, result.ReferenceTime = seed, refTime

	input := gen.Input{Schema: s, Seed: seed, ReferenceTime: refTime, MaxRows: o.Count, NestedRoot: o.NestedRoot}
	if o.Catalog != "" {
		if input.Vocabulary, err = builtinVocabulary(o.Catalog); err != nil {
			return result, err
		}
	}
	engine, err := gen.Build(input)
	if err != nil {
		return result, apperr.Wrap(err, apperr.InvalidArgument, "ERR_VALIDATION", "schema cannot be generated")
	}

	streams, err := selectStreams(engine.Streams(), o.Table)
	if err != nil {
		return result, err
	}

	dest, err := planDestination(&o, streams)
	if err != nil {
		return result, err
	}
	result.Output = dest.describe(o.Output)

	stats := o.Stats
	if stats == nil {
		stats = &pipeline.Stats{}
	}

	// Signals and deadlines end the run through Finish rather than cancellation, so the
	// open part closes as a complete document.
	finish := make(chan struct{})
	stop := make(chan struct{})
	watcherDone := make(chan bool, 1)
	go func() {
		var deadline <-chan time.Time
		if o.Duration > 0 {
			deadline = clk.After(o.Duration)
		}
		select {
		case <-ctx.Done():
			close(finish)
			watcherDone <- true
		case <-deadline:
			close(finish)
			watcherDone <- false
		case <-stop:
			// A run can finish just as it is cancelled, and then both of these are ready.
			watcherDone <- ctx.Err() != nil
		}
	}()
	defer func() {
		close(stop)
		result.Interrupted = <-watcherDone
	}()

	parts := map[gen.StreamID][]output.FinishedPart{}
	for _, spec := range streams {
		run, err := generateStream(engine, cfg, &o, spec, s, dest, stdout, stats, finish, clk, parts)
		result.Records += run.Records
		result.Parts += run.Parts
		if err != nil {
			return result, err
		}
		if run.Truncated {
			break
		}
	}
	result.Bytes = stats.Bytes.Load()

	if dest.kind == destDirectory {
		if err := writeManifests(o.Output, o.Format, o.Compression, parts); err != nil {
			return result, fmt.Errorf("write manifest: %w", err)
		}
	}
	result.Duration = clk.Now().Sub(start).Round(time.Millisecond).String()
	return result, nil
}

func loadGenerateSchema(cfg *config.Config, o *GenerateOptions) (*schema.Schema, error) {
	data := []byte(defaultRandomSchema)
	if o.SchemaPath != "" {
		read, err := os.ReadFile(o.SchemaPath)
		if err != nil {
			return nil, apperr.Wrap(err, apperr.InvalidArgument, "ERR_SCHEMA_FILE", "cannot read the schema file")
		}
		data = read
	}
	s, problems := schema.Decode(data, schema.FormatAuto)
	if len(problems) > 0 {
		return nil, schemaProblems(problems)
	}

	if o.Count > 0 {
		if err := schema.SetRootCount(s, o.Count); err != nil {
			return nil, usage("--count is " + err.Error())
		}
	}
	if err := checkModeOptions(s, o); err != nil {
		return nil, err
	}

	g := cfg.Generation
	opts := schema.Options{
		Limits: schema.Limits{
			MaxDepth: int64(g.RandomCaps.MaxDepth), MaxFields: int64(g.RandomCaps.MaxFields),
			MaxArrayLength: int64(g.RandomCaps.MaxArrayLength), MaxStringLength: int64(g.RandomCaps.MaxStringLength),
			MaxRecordBytes: int64(g.MaxRecordSizeBytes),
		},
		Output: schema.Output{
			Format: o.Format, Separator: o.Encoder.CSVSeparator, ArrayColumns: o.Encoder.CSVArrayColumns,
			Nested: o.NestedRoot != "", Root: o.NestedRoot,
		},
	}
	if o.Count <= 1<<63-1 {
		opts.MaxRecords = int64(o.Count)
	}
	if problems := schema.Validate(s, opts); len(problems) > 0 {
		return nil, schemaProblems(problems)
	}
	return s, nil
}

func checkModeOptions(s *schema.Schema, o *GenerateOptions) error {
	switch o.Mode {
	case ModeBatch:
		if o.Count == 0 && s.Relational == nil {
			return usage("batch mode needs --count")
		}
	case ModeStream:
	default:
		return usage(fmt.Sprintf("--mode must be batch or stream, not %q", o.Mode))
	}
	if o.Mode == ModeBatch && o.Duration > 0 {
		return usage("--duration applies to stream mode")
	}
	if o.Rate < 0 {
		return usage("--rate must not be negative")
	}
	if o.Table != "" && s.Relational == nil {
		return usage("--table needs a relational schema")
	}
	if o.Table != "" && o.NestedRoot != "" {
		return usage("--table and --nested cannot be combined: the nested layout is one document stream")
	}
	if o.Compression != "" && o.Compression != output.CompressionNone && o.Compression != output.CompressionGzip {
		return usage(fmt.Sprintf("--compression must be none or gzip, not %q", o.Compression))
	}
	return nil
}

func usage(msg string) error { return apperr.New(apperr.InvalidArgument, codeGenerateUsage, msg) }

func schemaProblems(problems schema.Problems) error {
	out := make([]apperr.Problem, len(problems))
	for i, p := range problems {
		out[i] = apperr.Problem{Path: p.Path, Code: p.Code, Message: p.Message}
	}
	return apperr.Validation("ERR_VALIDATION", "the schema has problems", out)
}

func chooseSeed(seed *uint64) (uint64, error) {
	if seed != nil {
		return *seed, nil
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("choose seed: %w", err)
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func selectStreams(streams []gen.StreamSpec, table string) ([]gen.StreamSpec, error) {
	if table == "" {
		return streams, nil
	}
	for _, s := range streams {
		if s.ID == gen.TableStream(table) {
			return []gen.StreamSpec{s}, nil
		}
	}
	return nil, usage(fmt.Sprintf("no table named %q", table))
}

type destKind uint8

const (
	destStdout destKind = iota
	destFile
	destDirectory
)

type destination struct{ kind destKind }

func (d destination) describe(path string) string {
	if d.kind == destStdout {
		return "stdout"
	}
	return path
}

// planDestination decides between one document and a directory of parts. Several streams
// or a part size need a directory; a trailing separator or an existing directory ask for
// one. Standard output and a single file hold exactly one complete document.
func planDestination(o *GenerateOptions, streams []gen.StreamSpec) (destination, error) {
	if o.Output == "" || o.Output == StdoutPath {
		if len(streams) > 1 {
			return destination{}, usage("this schema has several tables: set an output directory with -O, or choose one with --table")
		}
		if o.PartMaxBytes > 0 {
			return destination{}, usage("--part-max-bytes needs an output directory")
		}
		if o.Compression == output.CompressionGzip {
			return destination{}, usage("--compression needs an output file or directory")
		}
		return destination{destStdout}, nil
	}
	isDir := strings.HasSuffix(o.Output, "/") || strings.HasSuffix(o.Output, string(filepath.Separator))
	if st, err := os.Stat(o.Output); err == nil && st.IsDir() {
		isDir = true
	}
	if isDir || len(streams) > 1 || o.PartMaxBytes > 0 {
		return destination{destDirectory}, nil
	}
	return destination{destFile}, nil
}

func generateStream(engine *gen.Engine, cfg *config.Config, o *GenerateOptions, spec gen.StreamSpec, s *schema.Schema,
	dest destination, stdout io.Writer, stats *pipeline.Stats, finish <-chan struct{}, clk clock.Clock, parts map[gen.StreamID][]output.FinishedPart,
) (pipeline.Result, error) {
	enc, err := serial.New(o.Format, o.Encoder, serial.Stream{Fields: spec.Fields, Definitions: s.Definitions})
	if err != nil {
		return pipeline.Result{}, apperr.Wrap(err, apperr.InvalidArgument, codeGenerateUsage, err.Error())
	}

	generators := make([]gen.StreamGenerator, max(o.Workers, 1))
	for i := range generators {
		if generators[i], err = engine.NewStreamGenerator(spec.ID); err != nil {
			return pipeline.Result{}, err
		}
	}

	end := spec.Space
	if end == 0 {
		end = o.Count
	}
	if end == 0 {
		end = unboundedEnd
	}

	onPart := func(p output.FinishedPart) { parts[spec.ID] = append(parts[spec.ID], p) }
	var sink output.Sink
	limits := pipeline.PartLimits{}
	switch dest.kind {
	case destStdout:
		sink = &output.WriterSink{W: stdout, OnPart: onPart}
	case destFile:
		sink = &output.FileSink{Path: o.Output, Compression: o.Compression, OnPart: onPart}
	case destDirectory:
		sink = &output.DirSink{Dir: o.Output, Compression: o.Compression, OnPart: onPart}
		limits.MaxBytes = int64(cfg.Generation.PartMaxBytes)
		if o.PartMaxBytes > 0 {
			limits.MaxBytes = int64(o.PartMaxBytes)
		}
		limits.MaxRecords = uint64(cfg.Generation.PartMaxRecords)
		if o.Mode == ModeStream {
			limits.MaxAge = cfg.Generation.PartMaxAge
		}
	}

	batchSize := o.BatchSize
	if batchSize <= 0 {
		batchSize = cfg.Generation.DefaultBatchSize
	}
	result, err := pipeline.Run(context.Background(), pipeline.Unit{
		Generators: generators, Stream: string(spec.ID), From: 0, End: end, BatchSize: batchSize,
		QueueCap: cfg.Generation.QueueCapacity, Encoder: enc, Sink: sink, Limits: limits,
		RecordsPerSecond: o.Rate, MaxRecordBytes: spec.MaxRecordBytes, Clock: clk, Stats: stats, Finish: finish,
	})
	if err != nil {
		return result, fmt.Errorf("stream %s: %w", spec.ID, err)
	}

	// A stream with no rows still gets a valid document when one was asked for.
	if result.Parts == 0 && dest.kind != destDirectory {
		if err := writeEmpty(o, dest, enc, stdout); err != nil {
			return result, err
		}
	}
	return result, nil
}

func writeEmpty(o *GenerateOptions, dest destination, enc serial.Encoder, stdout io.Writer) error {
	doc := serial.EmptyDocument(enc)
	if dest.kind == destStdout {
		_, err := stdout.Write(doc)
		return err
	}
	part, err := output.NewFilePart(o.Output, o.Compression)
	if err != nil {
		return err
	}
	if _, err := part.Write(doc); err != nil {
		return errors.Join(err, part.Abort())
	}
	_, err = part.Commit()
	return err
}

func writeManifests(dir, format, compression string, parts map[gen.StreamID][]output.FinishedPart) error {
	if compression == "" {
		compression = output.CompressionNone
	}
	m := artifact.Manifest{Format: format, Compression: compression}
	for _, list := range parts {
		for _, p := range list {
			rel, err := filepath.Rel(dir, p.Path)
			if err != nil {
				return err
			}
			m.Records += p.Records
			m.Bytes += p.Bytes
			m.Parts = append(m.Parts, artifact.Part{
				Stream: p.Stream, Seq: p.Seq, RelPath: filepath.ToSlash(rel),
				FirstIndex: p.FirstIndex, EndIndex: p.EndIndex, Records: p.Records, Bytes: p.Bytes,
				SHA256: artifact.EncodeSHA(p.SHA256), HeaderLen: p.HeaderLen, FooterLen: p.FooterLen,
			})
		}
	}
	artifact.SortManifest(&m)
	return artifact.WriteManifestFile(filepath.Join(dir, "manifest.json"), m)
}
