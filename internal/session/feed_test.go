package session

import (
	"strings"
	"testing"

	"github.com/genadigeno/data-feed/internal/gen"
)

func TestFeedProblemsAllowAPlainSpecification(t *testing.T) {
	spec, err := ParseSpec([]byte("name: f\nkind: Feed\nseed: 1\nlimits: {count: 10, rate: 5}\noutput: {format: ndjson}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if problems := feedProblems(spec); len(problems) != 0 {
		t.Fatal(problems)
	}
}

func TestFeedProblemsRefuseWhatOnlyARunMeans(t *testing.T) {
	for path, doc := range map[string]string{
		"kind":                      "kind: Session",
		"mode":                      "mode: batch",
		"limits.duration":           "limits: {duration: 10s}",
		"limits.max_bytes":          "limits: {max_bytes: 5}",
		"limits.max_workers":        "limits: {max_workers: 2}",
		"execution.unit_records":    "execution: {unit_records: 500}",
		"execution.worker_selector": "execution: {worker_selector: {a: b}}",
		"output.compression":        "output: {format: csv, compression: gzip}",
		"output.part_max_bytes":     "output: {format: csv, part_max_bytes: 1MiB}",
		"output.part_max_records":   "output: {format: csv, part_max_records: 500}",
		"output.part_max_age":       "output: {format: csv, part_max_age: 5s}",
		"retention.pin":             "retention: {pin: true}",
	} {
		spec, err := ParseSpec([]byte("name: f\n" + doc + "\n"))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		found := false
		for _, p := range feedProblems(spec) {
			found = found || p.Path == path
		}
		if !found {
			t.Errorf("%s was not refused", path)
		}
	}
}

func TestStreamEnd(t *testing.T) {
	if StreamEnd(gen.StreamSpec{Space: 7}) != 7 {
		t.Fatal("a bounded stream ends at its space")
	}
	if StreamEnd(gen.StreamSpec{}) != unboundedRows {
		t.Fatal("an unbounded stream ends at the top of the index space")
	}
}

func TestFeedStreamsMarkSparseTables(t *testing.T) {
	spec, err := ParseSpec([]byte(strings.Join([]string{
		"name: shop", "mode: stream", "output: {format: csv}", "schema:", "  inline:", "    kind: relational", "    tables:",
		"      customers: {count: 10, primary_key: id, fields: {id: {type: uuid}}}",
		"      orders:",
		"        primary_key: id",
		"        owner: {ref: customers, per_parent: {min: 1, max: 3}}",
		"        fields: {id: {type: uuid}, customer: {type: reference, ref: customers.id, owner: true}}",
	}, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	c, err := Compile(spec, testConfig(), noResolver{}, nil, 1, specTime)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := c.Snapshot.Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	dense := map[string]bool{}
	for _, s := range FeedStreams(compiled.Engine.Streams()) {
		if s.End == 0 {
			t.Errorf("%s: a relational table has an end", s.ID)
		}
		dense[s.ID] = s.Dense
	}
	if !dense[string(gen.TableStream("customers"))] || dense[string(gen.TableStream("orders"))] {
		t.Fatalf("%v", dense)
	}
}
