package session

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/snapshot"
)

var specTime = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

const specYAML = `
name: orders
mode: batch
schema:
  inline:
    kind: hybrid
    fields:
      zeta: {type: integer, min: 1, max: 9}
      alpha: {type: uuid}
limits: {count: 1000}
output: {format: ndjson}
`

func testConfig() *config.Config {
	c := config.Defaults(config.EnvDevelopment)
	c.Normalize(config.Facts{NumCPU: 4, MemoryBytes: 8 << 30})
	return &c
}

type noResolver struct{}

func (noResolver) SchemaDocument(SchemaRef) ([]byte, error) {
	return nil, apperr.New(apperr.NotFound, CodeNotFound, "no schemas here")
}

func (noResolver) CatalogContent(CatalogRef) (json.RawMessage, int, error) {
	return nil, 0, apperr.New(apperr.NotFound, CodeNotFound, "no catalogs here")
}

func compile(t *testing.T, doc string) (*Compiled, error) {
	t.Helper()
	spec, err := ParseSpec([]byte(doc))
	if err != nil {
		return nil, err
	}
	return Compile(spec, testConfig(), noResolver{}, nil, 42, specTime)
}

func wantProblem(t *testing.T, err error, path string) {
	t.Helper()
	e, ok := apperr.As(err)
	if !ok {
		t.Fatalf("got %v", err)
	}
	for _, p := range e.Problems {
		if strings.HasPrefix(p.Path, path) {
			return
		}
	}
	t.Fatalf("no problem at %s in %+v", path, e.Problems)
}

func TestCompileFillsAndKeepsFieldOrder(t *testing.T) {
	c, err := compile(t, specYAML)
	if err != nil {
		t.Fatal(err)
	}
	s := c.Snapshot
	if s.Seed != 42 || !s.ReferenceTime.Equal(specTime) || s.MaxRows != 1000 || s.Format != "ndjson" {
		t.Fatalf("%+v", s)
	}
	if s.BatchSize <= 0 || s.UnitRecords <= 0 || s.PartMaxBytes <= 0 || s.QueueCapacity <= 0 {
		t.Fatalf("defaults missing: %+v", s)
	}
	// Field order decides the output, so YAML must not lose it on the way to JSON.
	doc := string(s.Schema)
	if strings.Index(doc, "zeta") > strings.Index(doc, "alpha") {
		t.Fatalf("field order lost: %s", doc)
	}
	if c.Strategy != "hybrid" || c.Limits.Count != 1000 || c.Limits.UnitMemoryBytes <= 0 {
		t.Fatalf("%+v", c)
	}
}

func TestSpecSeedAndTimeOverrideChoices(t *testing.T) {
	c, err := compile(t, specYAML+"seed: 7\nreference_time: 2020-01-02T03:04:05Z\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Snapshot.Seed != 7 || c.Snapshot.ReferenceTime.Year() != 2020 {
		t.Fatalf("%+v", c.Snapshot)
	}
}

func TestParseSpecRejectsUnknownKeys(t *testing.T) {
	if _, err := ParseSpec([]byte(specYAML + "limts: {count: 1}\n")); err == nil {
		t.Fatal("a misspelled key was accepted")
	}
}

func TestParseSpecAcceptsJSON(t *testing.T) {
	doc := `{"name":"a","mode":"batch","schema":{"inline":{"kind":"hybrid","fields":{"a":{"type":"uuid"}}}},"limits":{"count":1},"output":{"format":"csv"}}`
	if _, err := compile(t, doc); err != nil {
		t.Fatal(err)
	}
}

func TestCompileReportsProblems(t *testing.T) {
	cases := []struct {
		name, doc, path string
	}{
		{"batch without a count", strings.Replace(specYAML, "limits: {count: 1000}\n", "", 1), "limits.count"},
		{"strategy against schema", specYAML + "strategy: relational\n", "strategy"},
		{"bad schema", strings.Replace(specYAML, "type: uuid", "type: nonsense", 1), "schema"},
	}
	for _, tc := range cases {
		_, err := compile(t, tc.doc)
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		wantProblem(t, err, tc.path)
	}
}

func TestDurationParsesInSpecs(t *testing.T) {
	doc := strings.Replace(strings.Replace(specYAML, "mode: batch", "mode: stream", 1), "limits: {count: 1000}", "limits: {duration: 90s, rate: 50}", 1)
	c, err := compile(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	if c.Limits.DurationMS != 90000 {
		t.Fatalf("%+v", c.Limits)
	}
}

func streamsOf(t *testing.T, c *Compiled) []gen.StreamSpec {
	t.Helper()
	compiled, err := c.Snapshot.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	return compiled.Engine.Streams()
}

func TestStreamStatesUseCountOrUnbounded(t *testing.T) {
	c, err := compile(t, specYAML)
	if err != nil {
		t.Fatal(err)
	}
	states := StreamStatesFor("ses", streamsOf(t, c))
	if len(states) != 1 || states[0].End != 1000 || states[0].Stream != "main" {
		t.Fatalf("%+v", states)
	}

	doc := strings.Replace(strings.Replace(specYAML, "mode: batch", "mode: stream", 1), "limits: {count: 1000}\n", "", 1)
	c, err = compile(t, doc)
	if err != nil {
		t.Fatal(err)
	}
	states = StreamStatesFor("ses", streamsOf(t, c))
	if states[0].End != unboundedRows {
		t.Fatalf("%+v", states)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	c, err := compile(t, specYAML)
	if err != nil {
		t.Fatal(err)
	}
	hash, body, err := c.Snapshot.Encode()
	if err != nil {
		t.Fatal(err)
	}
	again, body2, err := c.Snapshot.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if hash != again || string(body) != string(body2) {
		t.Fatal("encoding is not stable, which would break content addressing")
	}
	got, err := snapshot.Decode(body, hash)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seed != 42 || string(got.Schema) != string(c.Snapshot.Schema) {
		t.Fatalf("%+v", got)
	}
	if _, err := snapshot.Decode(body, strings.Repeat("0", len(hash))); err == nil {
		t.Fatal("a wrong hash was accepted")
	}
	if _, err := snapshot.Decode([]byte("not gzip"), hash); err == nil {
		t.Fatal("garbage was accepted")
	}
	compiled, err := got.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := compiled.Stream("main"); !ok {
		t.Fatal("no main stream")
	}
}
