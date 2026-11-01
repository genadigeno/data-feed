package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/wire"
)

const testSchema = `{"kind":"hybrid","fields":{"id":{"type":"integer","min":0,"max":1000000}}}`

func testSnapshot(t *testing.T) (string, []byte) {
	t.Helper()
	s := &snapshot.Snapshot{
		EngineVersion: gen.Version, Schema: json.RawMessage(testSchema), Seed: 7, Mode: "batch", Format: "ndjson", MaxRows: 1000,
		Compression: "none", BatchSize: 50, QueueCapacity: 2, UnitRecords: 1000, UnitMaxBytes: 1 << 30, PartMaxBytes: 1 << 20, PartMaxRecords: 300,
	}
	hash, body, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return hash, body
}

// fakeCoordinator stands in for the Coordinator, and keeps what a real one would check.
type fakeCoordinator struct {
	mu          sync.Mutex
	hash        string
	snap        []byte
	assignments []wire.Assignment
	parts       map[int]wire.PartMeta
	bodies      map[int][]byte
	failNext    int // upload attempts to fail after storing, as a lost response does
	fencePart   int // part number to refuse as fenced, or -1
	completed   chan completion
	released    chan wire.LeaseRef
	failed      chan string
	beats       chan wire.Heartbeat
	directives  []wire.Directive
	lost        []string
	refuseBeat  error
	deregister  chan struct{}
}

type completion struct {
	end   uint64
	parts int
}

func newFake(t *testing.T) *fakeCoordinator {
	hash, body := testSnapshot(t)
	return &fakeCoordinator{
		hash: hash, snap: body, parts: map[int]wire.PartMeta{}, bodies: map[int][]byte{}, fencePart: -1,
		completed: make(chan completion, 4), released: make(chan wire.LeaseRef, 4), failed: make(chan string, 4),
		beats: make(chan wire.Heartbeat, 1024), deregister: make(chan struct{}, 1),
	}
}

func (f *fakeCoordinator) assign(end uint64) wire.Assignment {
	return wire.Assignment{
		LeaseRef: wire.LeaseRef{UnitID: "unt1", LeaseID: "lse1", FencingToken: 1}, SessionID: "ses1", LeaseTTLMillis: 60000,
		SnapshotHash: f.hash, Stream: string(gen.StreamMain), RangeEnd: end,
		PartLimits: wire.PartLimits{MaxBytes: 1 << 20, MaxRecords: 300},
	}
}

func (f *fakeCoordinator) Register(context.Context, wire.RegisterRequest) (wire.RegisterResponse, error) {
	return wire.RegisterResponse{WorkerID: "wrk1", Credential: "wrk1.secret", HeartbeatInterval: 1000, LeaseDuration: 60000}, nil
}

func (f *fakeCoordinator) Heartbeat(_ context.Context, _ string, h wire.Heartbeat) (wire.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case f.beats <- h:
	default:
	}
	if f.refuseBeat != nil {
		return wire.HeartbeatResponse{}, f.refuseBeat
	}
	resp := wire.HeartbeatResponse{Lost: f.lost, Directives: f.directives, LeaseTTLMillis: 60000}
	f.directives, f.lost = nil, nil
	return resp, nil
}

func (f *fakeCoordinator) Lease(ctx context.Context, _ string, _ int, wait time.Duration) ([]wire.Assignment, error) {
	f.mu.Lock()
	out := f.assignments
	f.assignments = nil
	f.mu.Unlock()
	if len(out) > 0 {
		return out, nil
	}
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func (f *fakeCoordinator) Snapshot(context.Context, string, string) ([]byte, error) {
	return f.snap, nil
}

func (f *fakeCoordinator) PartCommitted(_ context.Context, _, _ string, seq int) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.parts[seq]
	return ok, p.SHA256, nil
}

func (f *fakeCoordinator) UploadPart(_ context.Context, _ string, m wire.PartMeta, body wire.PartBody) (wire.UploadResult, error) {
	data, err := io.ReadAll(body.Reader)
	if err != nil {
		return wire.UploadResult{}, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != m.SHA256 || int64(len(data)) != m.Bytes {
		return wire.UploadResult{}, apperr.New(apperr.InvalidArgument, wire.CodeChecksumMismatch, "bad body")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if m.Seq == f.fencePart {
		return wire.UploadResult{}, apperr.New(apperr.Conflict, wire.CodeLeaseFenced, "fenced")
	}
	f.parts[m.Seq] = m
	f.bodies[m.Seq] = data
	if f.failNext > 0 {
		f.failNext--
		return wire.UploadResult{}, errors.New("connection reset")
	}
	return wire.UploadResult{CommittedThrough: m.EndIndex}, nil
}

func (f *fakeCoordinator) Complete(_ context.Context, _ string, _ wire.LeaseRef, end uint64, parts int) error {
	f.completed <- completion{end, parts}
	return nil
}

func (f *fakeCoordinator) Release(_ context.Context, _ string, ref wire.LeaseRef) error {
	f.released <- ref
	return nil
}

func (f *fakeCoordinator) Fail(_ context.Context, _ string, _ wire.LeaseRef, message string, _ bool) error {
	f.failed <- message
	return nil
}

func (f *fakeCoordinator) Deregister(context.Context, string) error {
	select {
	case f.deregister <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeCoordinator) partCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.parts)
}

// waitCompleted returns the unit's completion, and fails at once if it reported a failure.
func waitCompleted(t *testing.T, f *fakeCoordinator) completion {
	t.Helper()
	select {
	case c := <-f.completed:
		return c
	case m := <-f.failed:
		t.Fatalf("unit failed: %s", m)
	case <-time.After(30 * time.Second):
		t.Fatal("unit did not complete")
	}
	return completion{}
}

func startRuntime(t *testing.T, ctx context.Context, f *fakeCoordinator, spool bool) (*Runtime, chan error) {
	t.Helper()
	dir := t.TempDir()
	r := New(Options{
		Name: "test", Transport: f, Capabilities: wire.Capabilities{Slots: 1}, SpoolDir: dir, SpoolEnabled: spool, SpoolMaxBytes: 1 << 24,
		UploadConcurrency: 1, DrainTimeout: 30 * time.Second, LongPoll: time.Second, Clock: clock.Real(), Logger: slog.New(slog.DiscardHandler),
		Rand: func() float64 { return 0.01 },
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return r, done
}

// inBubble runs on a fake clock, so timers and backoff take no real time. Filesystem work
// does not count as idle; these tests wait on channels, not on durations.
func inBubble(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	synctest.Test(t, f)
}

func wantClean(t *testing.T, r *Runtime, dir string) {
	t.Helper()
	if used := r.spool.Used(); used != 0 {
		t.Errorf("spool still reserves %d bytes", used)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("spool holds %d files", len(entries))
	}
}

func TestUnitIsGeneratedDeliveredAndCompleted(t *testing.T) {
	for _, spool := range []bool{true, false} {
		f := newFake(t)
		f.assignments = []wire.Assignment{f.assign(1000)}
		ctx, cancel := context.WithCancel(context.Background())
		r, done := startRuntime(t, ctx, f, spool)
		c := waitCompleted(t, f)
		if c.end != 1000 || c.parts != f.partCount() {
			t.Fatalf("spool=%v: completed %+v with %d parts stored", spool, c, f.partCount())
		}
		// 1000 records at 300 per part.
		if c.parts != 4 {
			t.Fatalf("spool=%v: %d parts", spool, c.parts)
		}
		var next uint64
		for seq := range c.parts {
			p := f.parts[seq]
			if p.FirstIndex != next {
				t.Fatalf("part %d starts at %d, want %d", seq, p.FirstIndex, next)
			}
			next = p.EndIndex
		}
		if next != 1000 {
			t.Fatalf("parts end at %d", next)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		select {
		case <-f.deregister:
		default:
			t.Fatal("did not deregister")
		}
		wantClean(t, r, r.o.SpoolDir)
	}
}

func TestLostResponseDoesNotDuplicateAPart(t *testing.T) {
	f := newFake(t)
	f.failNext = 2
	f.assignments = []wire.Assignment{f.assign(1000)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, done := startRuntime(t, ctx, f, true)
	c := waitCompleted(t, f)
	if c.parts != 4 || c.end != 1000 {
		t.Fatalf("completed %+v", c)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResumeContinuesFromCommitted(t *testing.T) {
	f := newFake(t)
	a := f.assign(1000)
	a.ResumeFrom, a.NextPartSeq = 600, 2
	f.assignments = []wire.Assignment{a}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, done := startRuntime(t, ctx, f, true)
	c := waitCompleted(t, f)
	if c.end != 1000 || c.parts != 4 {
		t.Fatalf("completed %+v", c)
	}
	if p, ok := f.parts[2]; !ok || p.FirstIndex != 600 {
		t.Fatalf("first resumed part: %+v", p)
	}
	if _, ok := f.parts[0]; ok {
		t.Fatal("generated a committed part again")
	}
	cancel()
	<-done
}

func TestFencedUploadDiscardsTheUnit(t *testing.T) {
	f := newFake(t)
	f.fencePart = 1
	f.assignments = []wire.Assignment{f.assign(1000)}
	ctx, cancel := context.WithCancel(context.Background())
	r, done := startRuntime(t, ctx, f, true)
	deadline := time.After(30 * time.Second)
	for f.partCount() < 1 || r.freeSlots() != 1 {
		select {
		case <-deadline:
			t.Fatal("unit did not stop")
		case <-time.After(5 * time.Millisecond):
		}
	}
	select {
	case c := <-f.completed:
		t.Fatalf("completed despite fencing: %+v", c)
	case m := <-f.failed:
		t.Fatalf("reported failure despite fencing: %s", m)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wantClean(t, r, r.o.SpoolDir)
}

func TestRevokedWorkerStops(t *testing.T) {
	f := newFake(t)
	f.refuseBeat = apperr.New(apperr.PermissionDenied, wire.CodeWorkerRevoked, "revoked")
	r := New(Options{
		Name: "test", Transport: f, Capabilities: wire.Capabilities{Slots: 1}, SpoolDir: t.TempDir(), SpoolEnabled: true, SpoolMaxBytes: 1 << 24,
		UploadConcurrency: 1, DrainTimeout: time.Second, LongPoll: 10 * time.Millisecond, Clock: clock.Real(), Logger: slog.New(slog.DiscardHandler),
	})
	err := r.Run(context.Background())
	if e, ok := apperr.As(err); !ok || e.Code != wire.CodeWorkerRevoked {
		t.Fatalf("got %v", err)
	}
}

func TestDrainReleasesRunningUnits(t *testing.T) {
	f := newFake(t)
	a := f.assign(1 << 40) // far more than can finish
	a.Rate = 1000
	f.assignments = []wire.Assignment{a}
	ctx, cancel := context.WithCancel(context.Background())
	r, done := startRuntime(t, ctx, f, true)
	deadline := time.After(30 * time.Second)
	for {
		h := r.freeSlots()
		if h == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("unit did not start")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case ref := <-f.released:
		if ref.UnitID != "unt1" {
			t.Fatalf("released %+v", ref)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no release")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wantClean(t, r, r.o.SpoolDir)
}

func TestLostLeaseFromHeartbeatStopsTheUnit(t *testing.T) {
	f := newFake(t)
	a := f.assign(1 << 40)
	a.Rate = 1000
	f.assignments = []wire.Assignment{a}
	f.lost = []string{"lse1"}
	ctx, cancel := context.WithCancel(context.Background())
	r, done := startRuntime(t, ctx, f, true)
	deadline := time.After(30 * time.Second)
	started := false
	for !started || r.freeSlots() != 1 {
		started = started || r.freeSlots() == 0
		select {
		case <-deadline:
			t.Fatal("unit did not stop")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case ref := <-f.released:
		t.Fatalf("released a lost lease: %+v", ref)
	default:
	}
	cancel()
	<-done
}

func TestSpoolReserveBlocksUntilRoom(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		s := NewSpool(100)
		if err := s.Reserve(context.Background(), 80); err != nil {
			t.Fatal(err)
		}
		acquired := make(chan error, 1)
		go func() { acquired <- s.Reserve(context.Background(), 50) }()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("reserved beyond the limit")
		default:
		}
		s.Release(80)
		if err := <-acquired; err != nil {
			t.Fatal(err)
		}
	})
}

func TestSpoolAdmitsOversizedPartWhenEmpty(t *testing.T) {
	s := NewSpool(100)
	if err := s.Reserve(context.Background(), 500); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Reserve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	s.Release(500)
	if s.Used() != 0 {
		t.Fatal(s.Used())
	}
}

func TestBackoffIsCappedAndJittered(t *testing.T) {
	if got := Backoff(0, 1); got != backoffBase {
		t.Fatalf("first: %v", got)
	}
	if got := Backoff(100, 1); got != backoffCap {
		t.Fatalf("late: %v", got)
	}
	if got := Backoff(3, 0); got != 0 {
		t.Fatalf("no jitter: %v", got)
	}
}

func TestLeaseWindowStaysPositive(t *testing.T) {
	if got := leaseWindow(60000); got != 58*time.Second {
		t.Fatal(got)
	}
	if got := leaseWindow(1000); got != 750*time.Millisecond {
		t.Fatal(got)
	}
}
