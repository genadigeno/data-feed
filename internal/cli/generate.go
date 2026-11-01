package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/genadigeno/data-feed/internal/app"
	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/serial"
)

func newGenerateCommand(g *globals) *cobra.Command {
	o := app.GenerateOptions{Stats: &pipeline.Stats{}}
	var seed uint64
	var referenceTime, partMaxBytes, delimiter string

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate data directly, with no Coordinator, session or database",
		Long: `Generate data directly to standard output, a file or a directory.

Without --schema, generates schemaless random data. Several tables, or a part size, need
an output directory, which also gets a manifest.

The same --schema, --seed and --reference-time reproduce a dataset byte for byte.

Ctrl+C stops cleanly: what was written so far is a complete, valid document. A second
Ctrl+C stops immediately.`,
		Example: `  datafeed generate --count 1000 --format ndjson
  datafeed generate --schema shop.yaml --format csv -O out/
  datafeed generate --schema customer.yaml --count 100000 --format json -O customers.json --seed 42
  datafeed generate --mode stream --rate 1000 --duration 30s --format ndjson -O events/`,
		Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("seed") {
				o.Seed = &seed
			}
			if referenceTime != "" {
				t, err := time.Parse(time.RFC3339, referenceTime)
				if err != nil {
					return usageError(fmt.Errorf("--reference-time: want a time like 2026-01-31T00:00:00Z"))
				}
				o.ReferenceTime = &t
			}
			if partMaxBytes != "" {
				size, err := config.ParseSize(partMaxBytes)
				if err != nil {
					return usageError(fmt.Errorf("--part-max-bytes: %w", err))
				}
				o.PartMaxBytes = size
			}
			if delimiter != "" {
				if len(delimiter) != 1 {
					return usageError(fmt.Errorf("--csv-delimiter must be one character"))
				}
				o.Encoder.CSVDelimiter = delimiter[0]
			}

			loaded, err := g.loadConfig(config.RoleDirect, nil)
			if err != nil {
				return err
			}
			return g.runGenerate(cmd.Context(), &loaded.Config, o)
		},
	}

	f := cmd.Flags()
	f.StringVar(&o.SchemaPath, "schema", "", "schema file (YAML or JSON); default: schemaless random data")
	f.StringVar(&o.Mode, "mode", app.ModeBatch, "batch (a set quantity) or stream (until a stop condition)")
	f.StringVar(&o.Catalog, "catalog", "", "built-in catalog to draw values from: builtin:<domain>")
	f.Uint64Var(&o.Count, "count", 0, "records to generate; for a relational schema, its only root table's rows")
	f.Float64Var(&o.Rate, "rate", 0, "records per second; 0 means as fast as possible")
	f.DurationVar(&o.Duration, "duration", 0, "stream mode: stop after this long")
	f.StringVar(&o.Format, "format", serial.FormatNDJSON, "json, ndjson, xml or csv")
	f.StringVarP(&o.Output, "out", "O", app.StdoutPath, "output: - for standard output, a file, or a directory")
	f.Uint64Var(&seed, "seed", 0, "random seed; default: chosen and reported")
	f.StringVar(&referenceTime, "reference-time", "", `"now" for generated dates, RFC 3339; default: current time, reported`)
	f.IntVar(&o.Workers, "workers", 1, "generator goroutines")
	f.IntVar(&o.BatchSize, "batch-size", 0, "records per batch; default: generation.default_batch_size")
	f.StringVar(&o.Table, "table", "", "generate only this table")
	f.StringVar(&o.NestedRoot, "nested", "", "embed owned rows inside this root table's rows")
	f.StringVar(&partMaxBytes, "part-max-bytes", "", "rotate parts at this size, such as 64MiB; needs a directory")
	f.StringVar(&o.Compression, "compression", "", "none or gzip; needs a file or directory")
	f.BoolVar(&o.Encoder.Pretty, "pretty", false, "JSON: indent nested values")
	f.StringVar(&o.Encoder.XMLRoot, "xml-root", "", `XML root element (default "records")`)
	f.StringVar(&o.Encoder.XMLRecord, "xml-record", "", `XML record element (default "record")`)
	f.StringVar(&o.Encoder.XMLNull, "xml-null", "", "XML nulls: omit (default), empty or xsi_nil")
	f.StringVar(&delimiter, "csv-delimiter", "", `CSV delimiter (default ",")`)
	f.BoolVar(&o.Encoder.CSVNoHeader, "csv-no-header", false, "CSV: leave out the header row")
	f.StringVar(&o.Encoder.CSVNull, "csv-null", "", "CSV: how a null prints (default empty)")
	f.StringVar(&o.Encoder.CSVSeparator, "csv-separator", "", `CSV: joins flattened names (default ".")`)
	f.IntVar(&o.Encoder.CSVArrayColumns, "csv-array-columns", 0, "CSV: expand arrays to this many columns; default one JSON column")
	f.StringVar(&o.Encoder.CSVLayout, "csv-layout", "", "CSV without a schema: long (default) or json_column")
	return cmd
}

func (g *globals) runGenerate(ctx context.Context, cfg *config.Config, o app.GenerateOptions) error {
	stopProgress := g.startProgress(o.Stats)
	result, err := app.Generate(ctx, cfg, o, g.stdout, clock.Real())
	stopProgress()
	if err != nil {
		return err
	}

	// Data may be on standard output, so a summary goes to standard error unless the data
	// went elsewhere.
	summary := g.stdout
	if result.Output == "stdout" {
		summary = g.stderr
	}
	if g.output == outputJSON {
		if err := writeJSON(summary, result); err != nil {
			return err
		}
	} else if !g.quiet {
		fmt.Fprintf(summary, "%d records, %d bytes, %d parts in %s\nseed %d, reference time %s\noutput: %s\n",
			result.Records, result.Bytes, result.Parts, result.Duration,
			result.Seed, result.ReferenceTime.Format(time.RFC3339), result.Output)
	}
	if result.Interrupted {
		return apperr.New(apperr.Canceled, "ERR_INTERRUPTED", "stopped early: the output written so far is complete")
	}
	return nil
}

// startProgress prints progress on an interactive terminal. It never goes into data.
func (g *globals) startProgress(stats *pipeline.Stats) (stop func()) {
	if g.quiet || g.stderr != os.Stderr || !isTerminal(os.Stderr) {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last uint64
		for {
			select {
			case <-done:
				fmt.Fprint(g.stderr, "\r\033[K")
				return
			case <-ticker.C:
				now := stats.Records.Load()
				fmt.Fprintf(g.stderr, "\r\033[K%d records, %d/s, %d bytes", now, now-last, stats.Bytes.Load())
				last = now
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
