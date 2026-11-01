package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/clock/clocktest"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
)

var refTime = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// memorySink keeps parts in memory, and can block or fail on demand.
type memorySink struct {
	mu       sync.Mutex
	parts    []output.FinishedPart
	data     map[int][]byte
	aborted  int
	openGate chan struct{}
	failOpen error
	failAt   int // part sequence whose Accept fails
	failErr  error
}

func newMemorySink() *memorySink { return &memorySink{data: map[int][]byte{}, failAt: -1} }

type memoryPart struct {
	s   *memorySink
	seq int
	buf bytes.Buffer
}

func (s *memorySink) Open(ctx context.Context, r output.PartRequest) (output.PartFile, error) {
	if s.openGate != nil {
		select {
		case <-s.openGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.failOpen != nil {
		return nil, s.failOpen
	}
	return &memoryPart{s: s, seq: r.Seq}, nil
}

func (s *memorySink) Accept(_ context.Context, p output.FinishedPart) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.Seq == s.failAt {
		return s.failErr
	}
	s.parts = append(s.parts, p)
	return nil
}

func (p *memoryPart) Write(b []byte) (int, error) { return p.buf.Write(b) }

func (p *memoryPart) Commit() (output.FinishedPart, error) {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	p.s.data[p.seq] = bytes.Clone(p.buf.Bytes())
	return output.FinishedPart{Bytes: int64(p.buf.Len()), SHA256: sha256.Sum256(p.buf.Bytes())}, nil
}

func (p *memoryPart) Abort() error {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	p.s.aborted++
	return nil
}

func (s *memorySink) joined() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for i := range len(s.data) + 1 {
		b.Write(s.data[i])
	}
	return b.String()
}

func engineFor(t testing.TB, doc string) *gen.Engine {
	t.Helper()
	s, problems := schema.Decode([]byte(doc), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if problems := schema.Validate(s, schema.Options{Limits: schema.DefaultLimits()}); len(problems) > 0 {
		t.Fatal(problems)
	}
	e, err := gen.Build(gen.Input{Schema: s, Seed: 1, ReferenceTime: refTime})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

const hybridDoc = "kind: hybrid\nfields: {id: {type: long, generator: sequence}, name: {type: string}}\n"

const sparseDoc = `kind: relational
tables:
  p: {count: 1000, primary_key: id, fields: {id: {type: long, generator: sequence}}}
  c:
    primary_key: id
    owner: {ref: p, per_parent: {min: 0, max: 3}}
    fields:
      id: {type: uuid}
      f: {type: reference, ref: p.id, owner: true}
`

func unit(t testing.TB, e *gen.Engine, stream gen.StreamID, gens int, sink output.Sink, end uint64) Unit {
	t.Helper()
	generators := make([]gen.StreamGenerator, gens)
	for i := range generators {
		g, err := e.NewStreamGenerator(stream)
		if err != nil {
			t.Fatal(err)
		}
		generators[i] = g
	}
	enc, err := serial.New(serial.FormatNDJSON, serial.Options{}, serial.Stream{})
	if err != nil {
		t.Fatal(err)
	}
	return Unit{
		Generators: generators, Stream: string(stream), End: end, BatchSize: 64, QueueCap: 2,
		Encoder: enc, Sink: sink, Stats: &Stats{},
	}
}

// checkNoLeak fails if goroutines started during the test are still around.
func checkNoLeak(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			now := runtime.NumGoroutine()
			if now <= before {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<16)
				n := runtime.Stack(buf, true)
				t.Fatalf("goroutines leaked: %d before, %d after\n%s", before, now, buf[:n])
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func lines(s string) []string {
	return strings.Split(strings.TrimRight(s, "\n"), "\n")
}

func TestRunWritesEveryRecordInOrder(t *testing.T) {
	defer checkNoLeak(t)()
	sink := newMemorySink()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 1000)
	r, err := Run(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if r.EndIndex != 1000 || r.Records != 1000 || r.Parts != 1 || r.Truncated {
		t.Fatalf("result: %+v", r)
	}
	got := lines(sink.joined())
	if len(got) != 1000 {
		t.Fatalf("%d lines", len(got))
	}
	for i, line := range got {
		var rec struct{ ID int }
		if err := json.Unmarshal([]byte(line), &rec); err != nil || rec.ID != i {
			t.Fatalf("line %d: %s", i, line)
		}
	}
	if u.Stats.Records.Load() != 1000 || u.Stats.Bytes.Load() == 0 || u.Stats.Parts.Load() != 1 {
		t.Fatalf("stats: %d records, %d bytes", u.Stats.Records.Load(), u.Stats.Bytes.Load())
	}
}

func TestSeveralGeneratorsKeepOrderAndBytes(t *testing.T) {
	defer checkNoLeak(t)()
	for name, doc := range map[string]string{"dense": hybridDoc, "sparse": sparseDoc} {
		t.Run(name, func(t *testing.T) {
			e := engineFor(t, doc)
			stream := e.Streams()[len(e.Streams())-1]
			total := max(stream.Space, 2000)

			one := newMemorySink()
			if _, err := Run(context.Background(), unit(t, e, stream.ID, 1, one, total)); err != nil {
				t.Fatal(err)
			}
			for _, gens := range []int{2, 4, 8} {
				many := newMemorySink()
				r, err := Run(context.Background(), unit(t, e, stream.ID, gens, many, total))
				if err != nil {
					t.Fatal(err)
				}
				if r.EndIndex != total {
					t.Fatalf("%d generators ended at %d", gens, r.EndIndex)
				}
				if many.joined() != one.joined() {
					t.Fatalf("%d generators changed the bytes", gens)
				}
			}
		})
	}
}

func TestPartRangesJoinUpExactly(t *testing.T) {
	defer checkNoLeak(t)()
	e := engineFor(t, sparseDoc)
	stream := e.Streams()[1]
	sink := newMemorySink()
	u := unit(t, e, stream.ID, 1, sink, stream.Space)
	u.Limits.MaxRecords = 50

	r, err := Run(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if r.Parts < 5 {
		t.Fatalf("%d parts", r.Parts)
	}
	var pos, records uint64
	for i, p := range sink.parts {
		if p.Seq != i || p.FirstIndex != pos || p.EndIndex <= p.FirstIndex {
			t.Fatalf("part %d: %+v at position %d", i, p, pos)
		}
		if i < len(sink.parts)-1 && p.Records != 50 {
			t.Fatalf("part %d holds %d records", i, p.Records)
		}
		pos = p.EndIndex
		records += p.Records
	}
	if pos != stream.Space {
		t.Fatalf("parts end at %d of %d", pos, stream.Space)
	}
	if records != r.Records {
		t.Fatalf("parts hold %d records, result says %d", records, r.Records)
	}
}

func TestRotationBySize(t *testing.T) {
	sink := newMemorySink()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 5000)
	u.Limits.MaxBytes = 4096
	u.MaxRecordBytes = 200
	if _, err := Run(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if len(sink.parts) < 3 {
		t.Fatalf("%d parts", len(sink.parts))
	}
	for _, p := range sink.parts {
		if p.Bytes > 4096+200 {
			t.Fatalf("part %d has %d bytes, limit 4096 plus one record", p.Seq, p.Bytes)
		}
	}
}

func TestRotationByAge(t *testing.T) {
	clk := clocktest.NewFake(refTime)
	sink := newMemorySink()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 100)
	u.Limits.MaxAge = time.Minute
	u.Clock = clk
	u.BatchSize = 10

	// A sink that moves time forward at each part, so each batch finds its part old.
	slow := &advancingSink{Sink: sink, clk: clk, by: 0}
	u.Sink = slow
	gate := make(chan struct{})
	u.Generators[0] = &tickingGenerator{inner: u.Generators[0], clk: clk, by: 30 * time.Second, gate: gate}
	close(gate)

	if _, err := Run(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	if len(sink.parts) < 2 {
		t.Fatalf("%d parts", len(sink.parts))
	}
}

type advancingSink struct {
	output.Sink
	clk *clocktest.Fake
	by  time.Duration
}

type tickingGenerator struct {
	inner gen.StreamGenerator
	clk   *clocktest.Fake
	by    time.Duration
	gate  chan struct{}
}

func (g *tickingGenerator) Fill(b *record.Batch, from, to uint64, limit int) (uint64, error) {
	g.clk.Advance(g.by)
	return g.inner.Fill(b, from, to, limit)
}

func TestResumeProducesIdenticalBytes(t *testing.T) {
	e := engineFor(t, sparseDoc)
	stream := e.Streams()[1]

	whole := newMemorySink()
	full := unit(t, e, stream.ID, 1, whole, stream.Space)
	full.Limits.MaxRecords = 40
	if _, err := Run(context.Background(), full); err != nil {
		t.Fatal(err)
	}

	// Resume from each part boundary: a later attempt must produce the same later parts.
	for _, boundary := range whole.parts[:len(whole.parts)-1] {
		resumed := newMemorySink()
		u := unit(t, e, stream.ID, 1, resumed, stream.Space)
		u.Limits.MaxRecords = 40
		u.From, u.FirstSeq = boundary.EndIndex, boundary.Seq+1
		if _, err := Run(context.Background(), u); err != nil {
			t.Fatal(err)
		}
		if resumed.parts[0].FirstIndex != boundary.EndIndex {
			t.Fatalf("resumed part starts at %d, want %d", resumed.parts[0].FirstIndex, boundary.EndIndex)
		}
		for _, p := range resumed.parts {
			if want := whole.parts[p.Seq]; p.SHA256 != want.SHA256 || p.EndIndex != want.EndIndex {
				t.Fatalf("part %d differs after resuming at part %d", p.Seq, boundary.Seq+1)
			}
		}
	}
}

// fillCounter counts generated records, to observe backpressure.
type fillCounter struct {
	inner gen.StreamGenerator
	n     *atomic.Int64
}

func (g *fillCounter) Fill(b *record.Batch, from, to uint64, limit int) (uint64, error) {
	before := b.Len()
	next, err := g.inner.Fill(b, from, to, limit)
	g.n.Add(int64(b.Len() - before))
	return next, err
}

func TestSlowSinkHoldsBackGeneration(t *testing.T) {
	defer checkNoLeak(t)()
	var generated atomic.Int64
	sink := newMemorySink()
	sink.openGate = make(chan struct{})

	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 1_000_000)
	u.BatchSize, u.QueueCap = 100, 3
	u.Generators[0] = &fillCounter{u.Generators[0], &generated}

	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, err := Run(ctx, u)
		done <- err
	}()

	// The sink is stuck on its first part, so generation must stop on its own at: the
	// queue, plus one batch being sent, plus one being encoded.
	limit := int64((u.QueueCap + 2) * u.BatchSize)
	var settled int64
	for range 200 {
		time.Sleep(5 * time.Millisecond)
		now := generated.Load()
		if now == settled && now > 0 {
			break
		}
		settled = now
	}
	if settled == 0 || settled > limit {
		t.Fatalf("generated %d records while the sink was blocked; bound is %d", settled, limit)
	}
	time.Sleep(50 * time.Millisecond)
	if now := generated.Load(); now != settled {
		t.Fatalf("generation kept going: %d then %d", settled, now)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestMemoryStaysBoundedOverALongRun(t *testing.T) {
	sink := &discardSink{}
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 300_000)
	u.BatchSize = 100

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak.Load() {
					peak.Store(m.HeapAlloc)
				}
			}
		}
	})
	if _, err := Run(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	// 300k records of about 60 bytes would be 18 MiB held in memory; the bound is far less,
	// with slack for garbage not yet collected.
	const slack = 64 << 20
	if peak.Load() > before.HeapAlloc+slack {
		t.Fatalf("heap grew from %d to %d", before.HeapAlloc, peak.Load())
	}
	if sink.bytes.Load() < 1<<20 {
		t.Fatalf("only %d bytes written", sink.bytes.Load())
	}
}

type discardSink struct{ bytes atomic.Int64 }

type discardPart struct{ s *discardSink }

func (s *discardSink) Open(context.Context, output.PartRequest) (output.PartFile, error) {
	return discardPart{s}, nil
}
func (s *discardSink) Accept(context.Context, output.FinishedPart) error { return nil }
func (p discardPart) Write(b []byte) (int, error) {
	p.s.bytes.Add(int64(len(b)))
	return len(b), nil
}
func (p discardPart) Commit() (output.FinishedPart, error) { return output.FinishedPart{}, nil }
func (p discardPart) Abort() error                         { return nil }

func TestCancellationAtAnyPointLeavesNothingBehind(t *testing.T) {
	defer checkNoLeak(t)()
	e := engineFor(t, hybridDoc)
	for _, gens := range []int{1, 4} {
		for _, wait := range []time.Duration{0, time.Microsecond, 100 * time.Microsecond, time.Millisecond, 5 * time.Millisecond} {
			sink := newMemorySink()
			u := unit(t, e, gen.StreamMain, gens, sink, 1<<40)
			u.Limits.MaxRecords = 1000

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			var result Result
			go func() {
				r, err := Run(ctx, u)
				result = r
				done <- err
			}()
			time.Sleep(wait)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("gens %d, wait %v: err = %v", gens, wait, err)
			}

			// Whatever was accepted is intact, and a later run can continue from EndIndex.
			var end uint64
			for _, p := range sink.parts {
				if p.FirstIndex != end {
					t.Fatalf("gap at part %d", p.Seq)
				}
				end = p.EndIndex
			}
			if result.EndIndex != end {
				t.Fatalf("result says %d, parts reach %d", result.EndIndex, end)
			}
		}
	}
}

func TestFinishClosesTheOpenPartCleanly(t *testing.T) {
	defer checkNoLeak(t)()
	e := engineFor(t, hybridDoc)
	for _, gens := range []int{1, 3} {
		sink := newMemorySink()
		finish := make(chan struct{})
		u := unit(t, e, gen.StreamMain, gens, sink, 1<<40)
		u.Finish = finish

		done := make(chan Result, 1)
		go func() {
			r, err := Run(context.Background(), u)
			if err != nil {
				t.Error(err)
			}
			done <- r
		}()
		for u.Stats.Records.Load() < 500 {
			time.Sleep(time.Millisecond)
		}
		close(finish)
		r := <-done

		if !r.Truncated || r.Parts != 1 || r.EndIndex == 0 || r.Records == 0 {
			t.Fatalf("result: %+v", r)
		}
		got := lines(sink.joined())
		if uint64(len(got)) != r.Records || r.EndIndex != r.Records {
			t.Fatalf("%d lines, result %+v", len(got), r)
		}
		if sink.parts[0].EndIndex != r.EndIndex {
			t.Fatalf("part ends at %d, result at %d", sink.parts[0].EndIndex, r.EndIndex)
		}
	}
}

func TestGeneratorErrorFailsTheRun(t *testing.T) {
	defer checkNoLeak(t)()
	// Three values, more rows: uniqueness runs out at row 3.
	e := engineFor(t, "kind: hybrid\nfields: {a: {type: integer, min: 1, max: 3, unique: true}}\n")
	for _, gens := range []int{1, 2} {
		sink := newMemorySink()
		u := unit(t, e, gen.StreamMain, gens, sink, 1000)
		if _, err := Run(context.Background(), u); err == nil || !strings.Contains(err.Error(), "exhausted") {
			t.Fatalf("err = %v", err)
		}
		if sink.aborted > 1 {
			t.Fatalf("%d aborts", sink.aborted)
		}
	}
}

type panicGenerator struct{}

func (panicGenerator) Fill(*record.Batch, uint64, uint64, int) (uint64, error) {
	panic("generator bug")
}

func TestGeneratorPanicBecomesAnError(t *testing.T) {
	defer checkNoLeak(t)()
	for _, gens := range []int{1, 3} {
		u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, gens, newMemorySink(), 1000)
		for i := range u.Generators {
			u.Generators[i] = panicGenerator{}
		}
		if _, err := Run(context.Background(), u); err == nil || !strings.Contains(err.Error(), "generator bug") {
			t.Fatalf("err = %v", err)
		}
	}
}

type panicEncoder struct{ serial.Encoder }

func (panicEncoder) Header() []byte { panic("encoder bug") }

func TestEncoderPanicBecomesAnError(t *testing.T) {
	defer checkNoLeak(t)()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, newMemorySink(), 100000)
	u.Encoder = panicEncoder{u.Encoder}
	if _, err := Run(context.Background(), u); err == nil || !strings.Contains(err.Error(), "encoder bug") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkFailuresFailTheRun(t *testing.T) {
	defer checkNoLeak(t)()
	boom := errors.New("disk on fire")

	sink := newMemorySink()
	sink.failOpen = boom
	if _, err := Run(context.Background(), unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 100)); !errors.Is(err, boom) {
		t.Fatalf("open: %v", err)
	}

	sink = newMemorySink()
	sink.failAt, sink.failErr = 1, boom
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 1000)
	u.Limits.MaxRecords = 100
	r, err := Run(context.Background(), u)
	if !errors.Is(err, boom) {
		t.Fatalf("accept: %v", err)
	}
	if r.Parts != 1 || r.EndIndex != 100 {
		t.Fatalf("result: %+v", r)
	}
}

func TestEmptyRunMakesNoParts(t *testing.T) {
	defer checkNoLeak(t)()
	sink := newMemorySink()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 0)
	r, err := Run(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if r.Parts != 0 || len(sink.parts) != 0 {
		t.Fatalf("result: %+v", r)
	}
}

func TestEverythingSkippedStillReachesTheEnd(t *testing.T) {
	defer checkNoLeak(t)()
	e := engineFor(t, sparseDoc)
	stream := e.Streams()[1]
	// Find a run of indices holding no row: its parent has fewer than three children.
	all := newMemorySink()
	if _, err := Run(context.Background(), unit(t, e, stream.ID, 1, all, stream.Space)); err != nil {
		t.Fatal(err)
	}
	present := map[uint64]bool{}
	for _, line := range lines(all.joined()) {
		var rec struct{ F int }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		present[uint64(rec.F)] = true
	}
	var empty uint64
	found := false
	for p := range uint64(1000) {
		if !present[p] {
			empty, found = p, true
			break
		}
	}
	if !found {
		t.Skip("every parent has children under this seed")
	}

	sink := newMemorySink()
	u := unit(t, e, stream.ID, 1, sink, (empty+1)*3)
	u.From = empty * 3
	r, err := Run(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if r.Parts != 0 || r.EndIndex != (empty+1)*3 {
		t.Fatalf("result: %+v", r)
	}
}

func TestRateLimitPacesRecords(t *testing.T) {
	defer checkNoLeak(t)()
	clk := clocktest.NewFake(refTime)
	sink := newMemorySink()
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 500)
	u.Clock = clk
	u.RecordsPerSecond = 100 // batches of 10, every 100 ms

	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), u)
		done <- err
	}()

	var advanced time.Duration
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if advanced < 4*time.Second || advanced > 6*time.Second {
				t.Fatalf("500 records at 100/s took %v of clock time", advanced)
			}
			if u.Stats.RateWaitNanos.Load() == 0 {
				t.Fatal("rate wait not recorded")
			}
			return
		default:
		}
		clk.Advance(10 * time.Millisecond)
		advanced += 10 * time.Millisecond
		time.Sleep(50 * time.Microsecond)
	}
}

func TestLowRateKeepsOutputFlowing(t *testing.T) {
	defer checkNoLeak(t)()
	clk := clocktest.NewFake(refTime)
	sink := newMemorySink()
	finish := make(chan struct{})
	u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, 1, sink, 1<<40)
	u.Clock = clk
	u.RecordsPerSecond = 0.001 // one record every 1000 s
	u.Finish = finish

	done := make(chan Result, 1)
	go func() {
		r, err := Run(context.Background(), u)
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	clk.BlockUntil(1) // waiting for the rate limit

	// Finish must not sit out the wait, and what has not been paced yet is not output.
	close(finish)
	select {
	case r := <-done:
		if !r.Truncated || r.Records != 0 || r.EndIndex != 0 || r.Parts != 0 {
			t.Fatalf("result: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finish was stuck behind the rate limit")
	}
}

// Batches generated ahead of the rate must not all pass at once when a run finishes.
func TestFinishDoesNotOvershootTheRate(t *testing.T) {
	defer checkNoLeak(t)()
	for _, gens := range []int{1, 4} {
		clk := clocktest.NewFake(refTime)
		sink := newMemorySink()
		finish := make(chan struct{})
		u := unit(t, engineFor(t, hybridDoc), gen.StreamMain, gens, sink, 1<<40)
		u.Clock = clk
		u.RecordsPerSecond = 100
		u.Finish = finish

		done := make(chan Result, 1)
		go func() {
			r, err := Run(context.Background(), u)
			if err != nil {
				t.Error(err)
			}
			done <- r
		}()
		clk.BlockUntil(1)
		close(finish)
		select {
		case r := <-done:
			if !r.Truncated || r.Records != 0 {
				t.Fatalf("%d generators: %+v", gens, r)
			}
			if got := sink.joined(); len(got) != 0 {
				t.Fatalf("%d generators: wrote %d bytes, result %+v", gens, len(got), r)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%d generators: stuck", gens)
		}
	}
}

func TestControlChangesTheRate(t *testing.T) {
	var c Control
	c.SetRate(5) // nothing is running yet: no effect, and no failure
	clk := clocktest.NewFake(refTime)
	p := newPacer(100, 10, clk, &Stats{})
	c.pacer.Store(p)
	c.SetRate(40)
	if got := float64(p.lim.Limit()); got != 40 {
		t.Fatalf("rate %v", got)
	}
	c.SetRate(0) // not a rate: a run is never silently unlimited or stopped
	if got := float64(p.lim.Limit()); got != 40 {
		t.Fatalf("rate %v", got)
	}
}

func TestNoGeneratorsIsAnError(t *testing.T) {
	if _, err := Run(context.Background(), Unit{}); err == nil {
		t.Fatal("accepted")
	}
}

func BenchmarkPipeline(b *testing.B) {
	for _, format := range []string{serial.FormatNDJSON, serial.FormatCSV, serial.FormatXML} {
		b.Run(format, func(b *testing.B) {
			e := engineFor(b, hybridDoc)
			spec := e.Streams()[0]
			enc, err := serial.New(format, serial.Options{}, serial.Stream{Fields: spec.Fields})
			if err != nil {
				b.Fatal(err)
			}
			g, err := e.NewStreamGenerator(gen.StreamMain)
			if err != nil {
				b.Fatal(err)
			}
			sink := &discardSink{}
			const records = 100000
			b.ReportAllocs()
			for b.Loop() {
				_, err := Run(context.Background(), Unit{
					Generators: []gen.StreamGenerator{g}, Stream: gen.StreamMain, End: records,
					BatchSize: 1000, QueueCap: 4, Encoder: enc, Sink: sink,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(records), "records/op")
		})
	}
}
