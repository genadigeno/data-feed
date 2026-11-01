package app

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/clock/clocktest"
	"github.com/genadigeno/data-feed/internal/config"
)

var fixedTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testConfig() *config.Config {
	c := config.Defaults(config.EnvTest)
	c.Normalize(config.Facts{NumCPU: 2, MemoryBytes: 8 << 30})
	return &c
}

func writeSchema(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func options(format string, count uint64) GenerateOptions {
	seed := uint64(11)
	refTime := fixedTime
	return GenerateOptions{Mode: ModeBatch, Count: count, Format: format, Output: StdoutPath, Seed: &seed, ReferenceTime: &refTime, Workers: 1}
}

func run(t *testing.T, o GenerateOptions) (GenerateResult, []byte) {
	t.Helper()
	var buf bytes.Buffer
	r, err := Generate(context.Background(), testConfig(), o, &buf, clock.Real())
	if err != nil {
		t.Fatal(err)
	}
	return r, buf.Bytes()
}

const customers = "kind: hybrid\nfields: {id: {type: uuid, unique: true}, name: {type: full_name}, age: {type: integer, min: 18, max: 90}}\n"

const shop = `kind: relational
tables:
  customers: {count: 50, primary_key: id, fields: {id: {type: uuid}}}
  orders:
    primary_key: id
    owner: {ref: customers, per_parent: {min: 1, max: 3}}
    fields: {id: {type: uuid}, c: {type: reference, ref: customers.id, owner: true}}
`

func TestAllFormatsToStdoutParse(t *testing.T) {
	path := writeSchema(t, customers)
	for _, format := range []string{"json", "ndjson", "xml", "csv"} {
		o := options(format, 100)
		o.SchemaPath = path
		r, data := run(t, o)
		if r.Records != 100 || r.Output != "stdout" || r.Seed != 11 {
			t.Fatalf("%s: %+v", format, r)
		}
		switch format {
		case "json":
			var arr []map[string]any
			if err := json.Unmarshal(data, &arr); err != nil || len(arr) != 100 {
				t.Fatalf("JSON: %d %v", len(arr), err)
			}
		case "ndjson":
			for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
				if !json.Valid([]byte(line)) {
					t.Fatalf("NDJSON line %q", line)
				}
			}
		case "xml":
			var root struct {
				Records []struct {
					Age int `xml:"age"`
				} `xml:"record"`
			}
			if err := xml.Unmarshal(data, &root); err != nil || len(root.Records) != 100 {
				t.Fatalf("XML: %v", err)
			}
		case "csv":
			rows, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
			if err != nil || len(rows) != 101 || strings.Join(rows[0], ",") != "id,name,age" {
				t.Fatalf("CSV: %d %v", len(rows), err)
			}
		}
	}
}

func TestSameInputsReproduceAcrossWorkersAndOutputs(t *testing.T) {
	path := writeSchema(t, customers)
	o := options("ndjson", 3000)
	o.SchemaPath = path
	_, one := run(t, o)

	for _, workers := range []int{2, 5} {
		o.Workers = workers
		if _, data := run(t, o); !bytes.Equal(data, one) {
			t.Fatalf("%d workers changed the data", workers)
		}
	}

	o.Workers = 1
	o.Output = filepath.Join(t.TempDir(), "x.ndjson")
	run(t, o)
	onDisk, err := os.ReadFile(o.Output)
	if err != nil || !bytes.Equal(onDisk, one) {
		t.Fatalf("file differs from stdout: %v", err)
	}

	seed := uint64(12)
	o.Seed = &seed
	o.Output = StdoutPath
	if _, data := run(t, o); bytes.Equal(data, one) {
		t.Fatal("a different seed gave the same data")
	}
}

func TestDirectoryOutputHasPartsAndManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	o := options("csv", 0)
	o.SchemaPath = writeSchema(t, shop)
	o.Output = dir
	r, _ := run(t, o)

	if r.Records == 0 || r.Output != dir {
		t.Fatalf("%+v", r)
	}
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m artifact.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m.Format != "csv" || m.Compression != "none" || m.Records != r.Records || len(m.Parts) != 2 {
		t.Fatalf("manifest: %+v", m)
	}
	for _, p := range m.Parts {
		info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(p.RelPath)))
		if err != nil || info.Size() != p.Bytes {
			t.Fatalf("part %+v: %v", p, err)
		}
		if len(p.SHA256) != 64 {
			t.Fatalf("checksum %q", p.SHA256)
		}
	}
	rows := func(table string) int {
		f, err := os.Open(filepath.Join(dir, "table-"+table, "part-000000.csv"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		all, err := csv.NewReader(f).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		return len(all) - 1
	}
	if rows("customers") != 50 || rows("orders") < 50 {
		t.Fatalf("%d customers, %d orders", rows("customers"), rows("orders"))
	}
}

func TestPartRotationAndGzip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	o := options("ndjson", 20000)
	o.SchemaPath = writeSchema(t, customers)
	o.Output = dir
	o.PartMaxBytes = 128 * config.KiB
	o.Compression = "gzip"
	r, _ := run(t, o)
	if r.Parts < 2 {
		t.Fatalf("%+v", r)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "main"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".ndjson.gz") {
			t.Fatalf("file %q", e.Name())
		}
	}
}

func TestNestedAndTableSelection(t *testing.T) {
	path := writeSchema(t, shop)

	o := options("ndjson", 0)
	o.SchemaPath = path
	o.NestedRoot = "customers"
	r, data := run(t, o)
	if r.Records != 50 || !strings.Contains(string(data), `"orders":[`) {
		t.Fatalf("%+v %.100s", r, data)
	}

	o = options("ndjson", 0)
	o.SchemaPath = path
	o.Table = "customers"
	if r, _ := run(t, o); r.Records != 50 {
		t.Fatalf("%+v", r)
	}

	// --count sets the only root's rows.
	o = options("ndjson", 7)
	o.SchemaPath = path
	o.Table = "customers"
	if r, _ := run(t, o); r.Records != 7 {
		t.Fatalf("%+v", r)
	}
}

func TestEmptyStreamStillMakesAValidDocument(t *testing.T) {
	o := options("json", 1)
	o.SchemaPath = writeSchema(t, customers)
	o.Mode = ModeStream
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // finished before anything was asked for
	var buf bytes.Buffer
	r, err := Generate(ctx, testConfig(), o, &buf, clock.Real())
	if err != nil {
		t.Fatal(err)
	}
	if !r.Interrupted {
		t.Fatalf("%+v", r)
	}
	var arr []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &arr); err != nil {
		t.Fatalf("%q: %v", buf.String(), err)
	}

	o.Output = filepath.Join(t.TempDir(), "empty.json")
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, err := Generate(ctx2, testConfig(), o, &buf, clock.Real()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(o.Output)
	if err != nil || json.Unmarshal(data, &arr) != nil {
		t.Fatalf("%q %v", data, err)
	}
}

func TestStreamEndsAtDeadlineWithValidOutput(t *testing.T) {
	clk := clocktest.NewFake(fixedTime)
	o := options("json", 0)
	o.Mode = ModeStream
	o.Duration = time.Minute

	var buf bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := Generate(context.Background(), testConfig(), o, &buf, clk)
		done <- err
	}()
	clk.BlockUntil(1)
	clk.Advance(time.Minute)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("did not stop at the deadline")
	}
	var arr []any
	if err := json.Unmarshal(buf.Bytes(), &arr); err != nil {
		t.Fatalf("%.100q: %v", buf.String(), err)
	}
}

func TestInterruptKeepsWhatWasWritten(t *testing.T) {
	o := options("xml", 0)
	o.Mode = ModeStream
	o.Output = filepath.Join(t.TempDir(), "x.xml")
	o.Rate = 5000

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan GenerateResult, 1)
	go func() {
		r, err := Generate(ctx, testConfig(), o, &bytes.Buffer{}, clock.Real())
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	r := <-done
	if !r.Interrupted {
		t.Fatalf("%+v", r)
	}
	data, err := os.ReadFile(o.Output)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Records []struct{} `xml:"record"`
	}
	if err := xml.Unmarshal(data, &root); err != nil {
		t.Fatalf("not well-formed after interrupt: %v", err)
	}
	if uint64(len(root.Records)) != r.Records {
		t.Fatalf("%d records in the file, result says %d", len(root.Records), r.Records)
	}
}

func TestSeedAndTimeAreReportedWhenChosen(t *testing.T) {
	o := options("ndjson", 5)
	o.Seed, o.ReferenceTime = nil, nil
	clk := clocktest.NewFake(fixedTime.Add(90 * time.Second))
	var buf bytes.Buffer
	r, err := Generate(context.Background(), testConfig(), o, &buf, clk)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ReferenceTime.Equal(fixedTime.Add(90 * time.Second)) {
		t.Fatalf("%+v", r)
	}

	// The reported values reproduce the output.
	first := buf.String()
	o.Seed, o.ReferenceTime = &r.Seed, &r.ReferenceTime
	if _, again := run(t, o); string(again) != first {
		t.Fatal("reported seed and time did not reproduce the data")
	}
}

func TestUsageErrors(t *testing.T) {
	relational := writeSchema(t, shop)
	hybrid := writeSchema(t, customers)
	twoRoots := writeSchema(t, "kind: relational\ntables: {a: {count: 1, fields: {x: {type: uuid}}}, b: {count: 1, fields: {x: {type: uuid}}}}\n")
	extras := writeSchema(t, "kind: hybrid\nfields: {a: {type: uuid}}\nextra_random: {probability: 1}\n")

	cases := map[string]func(*GenerateOptions){
		"batch without count":       func(o *GenerateOptions) { o.Count = 0 },
		"bad mode":                  func(o *GenerateOptions) { o.Mode = "sometimes" },
		"duration in batch":         func(o *GenerateOptions) { o.Duration = time.Second },
		"negative rate":             func(o *GenerateOptions) { o.Rate = -1 },
		"unknown format":            func(o *GenerateOptions) { o.Format = "yaml" },
		"missing schema":            func(o *GenerateOptions) { o.SchemaPath = "no/such/file" },
		"bad compression":           func(o *GenerateOptions) { o.Compression = "zip" },
		"table without tables":      func(o *GenerateOptions) { o.Table = "x"; o.SchemaPath = hybrid },
		"unknown table":             func(o *GenerateOptions) { o.Table = "x"; o.SchemaPath = relational },
		"table with nested":         func(o *GenerateOptions) { o.Table = "customers"; o.NestedRoot = "customers"; o.SchemaPath = relational },
		"several streams to stdout": func(o *GenerateOptions) { o.SchemaPath = relational; o.Count = 0 },
		"parts to stdout":           func(o *GenerateOptions) { o.PartMaxBytes = config.MiB },
		"gzip to stdout":            func(o *GenerateOptions) { o.Compression = "gzip" },
		"ambiguous count":           func(o *GenerateOptions) { o.SchemaPath = twoRoots },
		"nested csv":                func(o *GenerateOptions) { o.SchemaPath = relational; o.NestedRoot = "customers"; o.Format = "csv" },
		"extras in csv":             func(o *GenerateOptions) { o.SchemaPath = extras; o.Format = "csv" },
		"bad xml root":              func(o *GenerateOptions) { o.Format = "xml"; o.Encoder.XMLRoot = "1 bad" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := options("ndjson", 5)
			mutate(&o)
			_, err := Generate(context.Background(), testConfig(), o, &bytes.Buffer{}, clock.Real())
			e, ok := apperr.As(err)
			if !ok || e.Kind != apperr.InvalidArgument {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestSchemaProblemsArePassedOn(t *testing.T) {
	o := options("ndjson", 5)
	o.SchemaPath = writeSchema(t, "kind: hybrid\nfields: {a: {type: integer, min: 5, max: 1}}\n")
	_, err := Generate(context.Background(), testConfig(), o, &bytes.Buffer{}, clock.Real())
	e, ok := apperr.As(err)
	if !ok || len(e.Problems) != 1 || e.Problems[0].Path != "fields.a" {
		t.Fatalf("err = %v", err)
	}
}

func TestPlanDestination(t *testing.T) {
	existing := t.TempDir()
	cases := []struct {
		output string
		parts  config.Size
		want   destKind
	}{
		{"-", 0, destStdout},
		{"", 0, destStdout},
		{"plain.json", 0, destFile},
		{"dir/", 0, destDirectory},
		{existing, 0, destDirectory},
		{"plain.json", config.MiB, destDirectory},
	}
	for i, c := range cases {
		o := GenerateOptions{Output: c.output, PartMaxBytes: c.parts}
		d, err := planDestination(&o, nil)
		if err != nil {
			t.Fatalf("case %s: %v", strconv.Itoa(i), err)
		}
		if d.kind != c.want {
			t.Errorf("case %d: %+v gave %d", i, c, d.kind)
		}
	}
}
