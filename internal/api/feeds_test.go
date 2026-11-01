package api

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/store"
)

func TestParseFeedWindow(t *testing.T) {
	const end = 2500
	cases := []struct {
		query    string
		from, to uint64
		next     uint64
		hasNext  bool
	}{
		{"", 0, 1000, 1000, true},
		{"offset=1000", 1000, 2000, 2000, true},
		{"offset=2000", 2000, 2500, 0, false},
		{"offset=2400&limit=100", 2400, 2500, 0, false},
		{"offset=2400&limit=50", 2400, 2450, 2450, true},
		{"offset=2500", 2500, 2500, 0, false},
		{"offset=9000", 9000, 9000, 0, false},
	}
	for _, c := range cases {
		q, _ := url.ParseQuery(c.query)
		w, err := parseFeedWindow(q, "offset", 1000, 10000, end)
		if err != nil {
			t.Fatalf("%q: %v", c.query, err)
		}
		if w.From != c.from || w.To != c.to || w.Next != c.next || w.HasNext != c.hasNext {
			t.Errorf("%q: got %+v", c.query, w)
		}
	}
}

func TestParseFeedWindowEndsAtTheStreamWithNoLimit(t *testing.T) {
	q, _ := url.ParseQuery("from=5")
	w, err := parseFeedWindow(q, "from", 0, 0, 100)
	if err != nil || w.From != 5 || w.To != 100 || w.HasNext {
		t.Fatalf("got %+v, %v", w, err)
	}
}

func TestParseFeedWindowSurvivesHugeNumbers(t *testing.T) {
	const end = uint64(1) << 63
	q, _ := url.ParseQuery("from=9223372036854775000&limit=18446744073709551615")
	w, err := parseFeedWindow(q, "from", 0, 0, end)
	if err != nil || w.To != end || w.HasNext {
		t.Fatalf("got %+v, %v", w, err)
	}
}

func TestParseFeedWindowRefusesBadValues(t *testing.T) {
	for _, query := range []string{"offset=-1", "offset=x", "limit=0", "limit=-5", "limit=x", "limit=10001"} {
		q, _ := url.ParseQuery(query)
		if _, err := parseFeedWindow(q, "offset", 1000, 10000, 1<<40); err == nil {
			t.Errorf("%q accepted", query)
		}
	}
}

func TestParseFeedRate(t *testing.T) {
	for query, want := range map[string]float64{"": 250, "rate=0": 0, "rate=1000": 1000, "rate=0.5": 0.5} {
		q, _ := url.ParseQuery(query)
		if got, err := parseFeedRate(q, 250); err != nil || got != want {
			t.Errorf("%q: got %v, %v", query, got, err)
		}
	}
	for _, query := range []string{"rate=-1", "rate=NaN", "rate=Inf", "rate=fast"} {
		q, _ := url.ParseQuery(query)
		if _, err := parseFeedRate(q, 250); err == nil {
			t.Errorf("%q accepted", query)
		}
	}
}

func TestChooseFeedStream(t *testing.T) {
	one := []gen.StreamSpec{{ID: gen.StreamMain}}
	if s, err := chooseFeedStream(one, ""); err != nil || s.ID != gen.StreamMain {
		t.Fatal(s, err)
	}
	several := []gen.StreamSpec{{ID: gen.TableStream("a")}, {ID: gen.TableStream("b")}}
	if _, err := chooseFeedStream(several, ""); err == nil {
		t.Fatal("a table must be chosen")
	}
	if s, err := chooseFeedStream(several, "b"); err != nil || s.ID != gen.TableStream("b") {
		t.Fatal(s, err)
	}
	if _, err := chooseFeedStream(several, "c"); err == nil {
		t.Fatal("no such table")
	}
}

func TestCanStreamOnlyFormatsThatSurviveBeingCut(t *testing.T) {
	for format, want := range map[string]bool{serial.FormatNDJSON: true, serial.FormatCSV: true, serial.FormatJSON: false, serial.FormatXML: false} {
		if canStream(format) != want {
			t.Errorf("%s: want %v", format, want)
		}
	}
}

func TestFeedMemoryUsesTheSmallerRecordBound(t *testing.T) {
	if got := feedMemory(4, 1000, 500, 65536); got != 6*1000*500 {
		t.Fatal(got)
	}
	if got := feedMemory(4, 1000, 1<<30, 65536); got != 6*1000*65536 {
		t.Fatal(got)
	}
	if got := feedMemory(4, 1000, 0, 65536); got != 6*1000*65536 {
		t.Fatal("an unknown size must count as the largest", got)
	}
}

func TestStreamRoutesDoNotHoldRequestSlots(t *testing.T) {
	if !isLongLived(prefix + "/feeds/fed_1/stream") {
		t.Error("a stream would use up a request slot")
	}
	if isLongLived(prefix+"/feeds/fed_1/records") || isLongLived(prefix+"/feeds/fed_1") {
		t.Error("pages are ordinary requests")
	}
}

func TestToFeedDTOShowsAnEndOnlyWhenThereIsOne(t *testing.T) {
	d := ToFeedDTO(&store.Feed{ID: "fed_1", Seed: 7, StreamsJSON: `[{"id":"main","dense":true},{"id":"t","end":9,"dense":false}]`})
	if len(d.Streams) != 2 || d.Streams[0].End != "" || d.Streams[1].End != "9" || d.Seed != "7" {
		t.Fatalf("%+v", d)
	}
}

// feedEngine builds a generator source for the page tests.
func feedEngine(t *testing.T) *gen.Engine {
	t.Helper()
	parsed, problems := schema.Decode([]byte("kind: hybrid\nfields:\n  id: {type: uuid, unique: true}\n  n: {type: integer, min: 1, max: 1000}\n"), schema.FormatAuto)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	e, err := gen.Build(gen.Input{Schema: parsed, Seed: 42, ReferenceTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func runWindow(t *testing.T, e *gen.Engine, from, to uint64, flush bool) string {
	t.Helper()
	g, err := e.NewStreamGenerator(gen.StreamMain)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := serial.New(serial.FormatNDJSON, serial.Options{}, serial.Stream{Fields: e.Streams()[0].Fields})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, err = pipeline.Run(context.Background(), pipeline.Unit{
		Generators: []gen.StreamGenerator{g}, Stream: "main", From: from, End: to, BatchSize: 64, QueueCap: 2,
		Encoder: enc, Sink: &output.WriterSink{W: &out}, FlushBatches: flush,
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// The promise a client relies on: however it cuts up the positions, the bytes are the same.
func TestPagesJoinUpToTheWholeStream(t *testing.T) {
	e := feedEngine(t)
	whole := runWindow(t, e, 0, 5000, false)
	if strings.Count(whole, "\n") != 5000 {
		t.Fatalf("%d lines", strings.Count(whole, "\n"))
	}
	for _, size := range []uint64{1, 7, 64, 1000, 4999} {
		var joined strings.Builder
		for from := uint64(0); from < 5000; from += size {
			joined.WriteString(runWindow(t, e, from, min(from+size, 5000), false))
		}
		if joined.String() != whole {
			t.Errorf("pages of %d differ from the whole", size)
		}
	}
}

func TestFlushingBatchesKeepsTheBytes(t *testing.T) {
	e := feedEngine(t)
	if runWindow(t, e, 100, 900, true) != runWindow(t, e, 100, 900, false) {
		t.Fatal("flushing changed the output")
	}
}

// A window over no positions must not fail: a client can ask past the end.
func TestEmptyWindowWritesNothing(t *testing.T) {
	if got := runWindow(t, feedEngine(t), 500, 500, true); got != "" {
		t.Fatalf("got %q", got)
	}
}

// With a part open for a slow reader, records must leave at every batch, not when 1 MiB fills.
func TestFlushBatchesDeliversEachBatchAtOnce(t *testing.T) {
	e := feedEngine(t)
	g, err := e.NewStreamGenerator(gen.StreamMain)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := serial.New(serial.FormatNDJSON, serial.Options{}, serial.Stream{Fields: e.Streams()[0].Fields})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan int, 16)
	w := writerFunc(func(b []byte) (int, error) {
		seen <- bytes.Count(b, []byte("\n"))
		return len(b), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := pipeline.Run(ctx, pipeline.Unit{
			Generators: []gen.StreamGenerator{g}, Stream: "main", End: 1 << 40, BatchSize: 8, QueueCap: 1,
			Encoder: enc, Sink: &output.WriterSink{W: w}, RecordsPerSecond: 10, FlushBatches: true,
		})
		done <- err
	}()
	select {
	case n := <-seen:
		if n == 0 {
			t.Fatal("nothing in the first write")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a slow stream's first record was held back")
	}
	cancel()
	go func() {
		for range seen { // whatever is on its way must not block the run from ending
		}
	}()
	<-done
	close(seen)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }
