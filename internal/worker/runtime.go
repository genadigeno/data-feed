// Package worker is the Worker runtime: it registers, takes leases, generates units and
// delivers their parts. It never touches SQLite or the Coordinator's filesystem: everything
// goes through wire.Coordinator, which is the network for a remote Worker and a direct call
// for the embedded one, so both behave the same.
// See docs/01-architecture/distributed-execution.md.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/wire"
)

const (
	backoffBase       = 250 * time.Millisecond
	backoffCap        = 30 * time.Second
	defaultLongPoll   = 25 * time.Second
	idlePollInterval  = time.Second
	deadlineMargin    = 2 * time.Second
	deregisterTimeout = 5 * time.Second
	maxCachedEngines  = 8
)

// Options configures a Runtime.
type Options struct {
	Name, Host   string
	Transport    wire.Coordinator
	Capabilities wire.Capabilities
	JoinToken    string
	// Credential is what an earlier registration issued, if any.
	Credential string
	// SaveCredential stores a newly issued credential.
	SaveCredential func(string) error

	SpoolDir          string
	SpoolEnabled      bool
	SpoolMaxBytes     int64
	UploadConcurrency int
	DrainTimeout      time.Duration
	// LocalFiles hands parts over by path. Only for the embedded worker, whose spool is on
	// the Coordinator's filesystem.
	LocalFiles bool
	LongPoll   time.Duration
	Vocab      snapshot.VocabularyDecoder

	Clock  clock.Clock
	Logger *slog.Logger
	// Rand returns a value in [0, 1) for retry jitter. Nil means math/rand/v2.
	Rand func() float64
}

// Capabilities describes what this binary can run.
func Capabilities(slots int, maxMemory, spool int64, version string, labels map[string]string) wire.Capabilities {
	return wire.Capabilities{
		EngineVersions: []int{gen.Version},
		Formats:        []string{serial.FormatJSON, serial.FormatNDJSON, serial.FormatXML, serial.FormatCSV},
		Strategies:     []string{string(schema.KindRandom), string(schema.KindHybrid), string(schema.KindRelational)},
		OS:             runtime.GOOS, Arch: runtime.GOARCH,
		Slots: slots, MaxMemoryBytes: maxMemory, SpoolMaxBytes: spool, BinaryVersion: version, Labels: labels,
	}
}

// Backoff returns the wait before retry number attempt (0-based): exponential with
// full jitter, capped.
func Backoff(attempt int, random float64) time.Duration {
	ceiling := backoffCap
	if attempt < 16 { // beyond this the shift exceeds the cap anyway
		if d := backoffBase << attempt; d < ceiling {
			ceiling = d
		}
	}
	return time.Duration(random * float64(ceiling))
}

// Runtime is a running Worker.
type Runtime struct {
	o     Options
	log   *slog.Logger
	spool *Spool

	id                string
	heartbeatInterval time.Duration

	mu        sync.Mutex
	units     map[string]*unitRun
	wg        sync.WaitGroup
	draining  atomic.Bool
	noNewWork atomic.Bool
	slotFree  chan struct{}
	uploads   chan struct{}
	engines   *engineCache
}

// New returns a Runtime.
func New(o Options) *Runtime {
	if o.Rand == nil {
		o.Rand = rand.Float64
	}
	if o.LongPoll == 0 {
		o.LongPoll = defaultLongPoll
	}
	limit := o.SpoolMaxBytes
	if !o.SpoolEnabled {
		limit = 0 // parts wait for their acknowledgement instead, which bounds them
	}
	r := &Runtime{
		o: o, log: o.Logger.With("component", "worker", "worker_name", o.Name),
		spool:    NewSpool(limit),
		units:    map[string]*unitRun{},
		slotFree: make(chan struct{}, 1),
		uploads:  make(chan struct{}, max(o.UploadConcurrency, 1)),
	}
	r.engines = newEngineCache(maxCachedEngines, r.compile)
	return r
}

// Run blocks until ctx is cancelled, which drains and returns nil, or until the Coordinator
// refuses this Worker, which retrying cannot fix.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.register(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if err := resetDir(r.o.SpoolDir); err != nil {
		return fmt.Errorf("prepare spool: %w", err)
	}

	// Units outlive ctx: a drain finishes them against a working connection.
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	pollCtx, stopPolling := context.WithCancelCause(ctx)
	defer stopPolling(nil)

	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		r.heartbeatLoop(workCtx, stopPolling)
	}()

	r.log.Info("worker started", "worker_id", r.id, "slots", r.o.Capabilities.Slots)
	fatal := r.poll(pollCtx, workCtx)
	if fatal == nil {
		fatal = context.Cause(pollCtx)
		if errors.Is(fatal, context.Canceled) {
			fatal = nil
		}
	}

	if fatal == nil {
		r.drain()
	}
	cancelWork()
	r.wg.Wait()
	<-beatDone

	if fatal != nil {
		return fatal
	}
	deregCtx, cancel := context.WithTimeout(context.Background(), deregisterTimeout)
	defer cancel()
	if err := r.o.Transport.Deregister(deregCtx, r.id); err != nil {
		r.log.Warn("deregister", "err", err)
	}
	r.log.Info("worker stopped")
	return nil
}

// refused reports a registration or call failure that retrying cannot fix.
func refused(err error) bool {
	e, ok := apperr.As(err)
	if !ok {
		return false
	}
	switch e.Kind {
	case apperr.Unauthenticated, apperr.PermissionDenied, apperr.Conflict, apperr.InvalidArgument:
		return true
	}
	return false
}

func (r *Runtime) register(ctx context.Context) error {
	req := wire.RegisterRequest{
		ProtocolVersion: wire.ProtocolVersion, Name: r.o.Name, Host: r.o.Host,
		JoinToken: r.o.JoinToken, Credential: r.o.Credential, Capabilities: r.o.Capabilities,
	}
	for attempt := 0; ; attempt++ {
		resp, err := r.o.Transport.Register(ctx, req)
		if err == nil {
			if resp.Credential != "" && r.o.SaveCredential != nil {
				if err := r.o.SaveCredential(resp.Credential); err != nil {
					return fmt.Errorf("save worker credential: %w", err)
				}
			}
			r.id = resp.WorkerID
			r.heartbeatInterval = time.Duration(resp.HeartbeatInterval) * time.Millisecond
			return nil
		}
		if refused(err) {
			return err
		}
		if attempt == 0 {
			r.log.Warn("coordinator unreachable, retrying", "err", err)
		}
		if !r.sleep(ctx, Backoff(attempt, r.o.Rand())) {
			return ctx.Err()
		}
	}
}

func (r *Runtime) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-r.o.Clock.After(d):
		return true
	}
}

func (r *Runtime) freeSlots() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.o.Capabilities.Slots - len(r.units)
}

// poll takes work whenever a slot is free, until ctx ends or the Coordinator refuses.
func (r *Runtime) poll(ctx, workCtx context.Context) error {
	failures := 0
	for ctx.Err() == nil {
		free := r.freeSlots()
		if free <= 0 || r.noNewWork.Load() {
			select {
			case <-ctx.Done():
			case <-r.slotFree:
			case <-r.o.Clock.After(idlePollInterval):
			}
			continue
		}
		asked := r.o.Clock.Now()
		assignments, err := r.o.Transport.Lease(ctx, r.id, free, r.o.LongPoll)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if refused(err) {
				return err
			}
			if failures == 0 {
				r.log.Warn("cannot reach coordinator for work, retrying", "err", err)
			}
			r.sleep(ctx, Backoff(failures, r.o.Rand()))
			failures++
			continue
		}
		failures = 0
		for i := range assignments {
			r.start(workCtx, assignments[i])
		}
		// An instant empty answer means the Coordinator is not holding polls; do not spin.
		if len(assignments) == 0 && r.o.Clock.Now().Sub(asked) < idlePollInterval/10 {
			r.sleep(ctx, idlePollInterval)
		}
	}
	return nil
}

func (r *Runtime) start(workCtx context.Context, a wire.Assignment) {
	u := newUnitRun(workCtx, a, r.o.Clock.Now(), leaseWindow(a.LeaseTTLMillis))
	r.mu.Lock()
	r.units[a.UnitID] = u
	r.mu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() {
			u.cancel()
			r.mu.Lock()
			delete(r.units, a.UnitID)
			r.mu.Unlock()
			select {
			case r.slotFree <- struct{}{}:
			default:
			}
		}()
		r.runUnit(u)
	}()
}

// leaseWindow is how long a lease may be trusted after being renewed. The margin stops
// work on a lease the Coordinator may be about to expire.
func leaseWindow(ttlMillis int64) time.Duration {
	ttl := time.Duration(ttlMillis) * time.Millisecond
	return ttl - min(deadlineMargin, ttl/4)
}

// drain gives back running units and waits for them, up to the drain timeout.
func (r *Runtime) drain() {
	r.draining.Store(true)
	r.mu.Lock()
	for _, u := range r.units {
		u.ask(modeRelease)
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-r.o.Clock.After(r.o.DrainTimeout):
		r.log.Warn("drain timeout expired, abandoning running units")
	}
}

func (r *Runtime) heartbeatLoop(ctx context.Context, stop context.CancelCauseFunc) {
	ticker := r.o.Clock.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			if err := r.beat(ctx); err != nil {
				if refused(err) && ctx.Err() == nil {
					r.mu.Lock()
					for _, u := range r.units {
						u.markLost()
					}
					r.mu.Unlock()
					stop(err)
					return
				}
				if ctx.Err() == nil {
					r.log.Warn("heartbeat failed", "err", err)
				}
			}
		}
	}
}

// beat reports progress and applies the answer. Whatever the outcome, a unit whose lease
// can no longer be trusted stops, because another Worker may have it by now.
func (r *Runtime) beat(ctx context.Context) error {
	r.mu.Lock()
	running := make([]*unitRun, 0, len(r.units))
	for _, u := range r.units {
		running = append(running, u)
	}
	r.mu.Unlock()

	h := wire.Heartbeat{
		FreeSlots: r.o.Capabilities.Slots - len(running), SpoolBytes: r.spool.Used(), Draining: r.draining.Load(),
		Units: make([]wire.UnitProgress, len(running)),
	}
	for i, u := range running {
		h.Units[i] = u.progress()
	}
	sent := r.o.Clock.Now()
	resp, err := r.o.Transport.Heartbeat(ctx, r.id, h)
	if err != nil {
		now := r.o.Clock.Now()
		for _, u := range running {
			if !u.leaseValid(now) {
				r.log.Warn("lease timed out without contact", "unit_id", u.a.UnitID)
				u.markLost()
			}
		}
		return err
	}

	lost := make(map[string]bool, len(resp.Lost))
	for _, id := range resp.Lost {
		lost[id] = true
	}
	byUnit := make(map[string]*unitRun, len(running))
	window := leaseWindow(resp.LeaseTTLMillis)
	for _, u := range running {
		byUnit[u.a.UnitID] = u
		if lost[u.a.LeaseID] {
			r.log.Info("lease lost", "unit_id", u.a.UnitID)
			u.markLost()
			continue
		}
		u.renew(sent.Add(window)) // from before the request, which is the safe side
	}

	noNewWork := false
	for _, d := range resp.Directives {
		if d.Action == wire.DirectiveDrain {
			noNewWork = true
			continue
		}
		u := byUnit[d.UnitID]
		if u == nil {
			continue
		}
		switch d.Action {
		case wire.DirectiveRelease:
			u.ask(modeRelease)
		case wire.DirectiveFinish:
			u.ask(modeFinish)
		case wire.DirectiveCancel:
			u.markLost()
		case wire.DirectiveSetRate:
			u.control.SetRate(d.Rate)
		}
	}
	r.noNewWork.Store(noNewWork)
	return nil
}

// Stop modes: what a unit does when asked to end early.
const (
	modeNone int32 = iota
	modeFinish
	modeRelease
)

// unitRun is one leased unit in progress.
type unitRun struct {
	a       wire.Assignment
	ctx     context.Context
	cancel  context.CancelFunc
	finish  chan struct{}
	once    sync.Once
	mode    atomic.Int32
	lost    atomic.Bool
	stats   pipeline.Stats
	control pipeline.Control
	acked   atomic.Uint64

	mu       sync.Mutex
	deadline time.Time
}

func newUnitRun(parent context.Context, a wire.Assignment, now time.Time, window time.Duration) *unitRun {
	ctx, cancel := context.WithCancel(parent)
	return &unitRun{a: a, ctx: ctx, cancel: cancel, finish: make(chan struct{}), deadline: now.Add(window)}
}

// ask ends the run at the next batch. The first request decides how.
func (u *unitRun) ask(mode int32) {
	u.once.Do(func() {
		u.mode.Store(mode)
		close(u.finish)
	})
}

func (u *unitRun) markLost() {
	u.lost.Store(true)
	u.cancel()
}

func (u *unitRun) renew(deadline time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if deadline.After(u.deadline) {
		u.deadline = deadline
	}
}

func (u *unitRun) leaseValid(now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return now.Before(u.deadline)
}

func (u *unitRun) progress() wire.UnitProgress {
	generated, acked := u.stats.Records.Load(), u.acked.Load()
	return wire.UnitProgress{
		LeaseRef: u.a.LeaseRef, Records: generated - min(acked, generated), Bytes: u.stats.Bytes.Load(),
		GenBlockedNanos: u.stats.GenBlockedNanos.Load(), EncWaitNanos: u.stats.EncWaitNanos.Load(),
		SinkBlockedNanos: u.stats.SinkBlockedNanos.Load(), RateWaitNanos: u.stats.RateWaitNanos.Load(),
	}
}

func (r *Runtime) runUnit(u *unitRun) {
	log := r.log.With("session_id", u.a.SessionID, "unit_id", u.a.UnitID)
	defer func() {
		if p := recover(); p != nil {
			log.Error("unit panicked", "panic", p, "stack", string(debug.Stack()))
			r.report(u, log, "fail", func(ctx context.Context) error {
				return r.o.Transport.Fail(ctx, r.id, u.a.LeaseRef, fmt.Sprintf("worker panicked: %v", p), true)
			})
		}
	}()

	compiled, err := r.engines.get(u.ctx, u.a.SnapshotHash)
	if err != nil {
		if u.ctx.Err() != nil {
			return
		}
		log.Error("cannot prepare unit", "err", err)
		r.report(u, log, "fail", func(ctx context.Context) error {
			return r.o.Transport.Fail(ctx, r.id, u.a.LeaseRef, err.Error(), false)
		})
		return
	}

	sink := newUploadSink(r, u, log)
	go sink.loop()
	result, runErr := r.generate(u, compiled, sink)
	uploadErr := sink.close()

	switch {
	case u.lost.Load():
		log.Info("unit abandoned: lease lost")
	case u.ctx.Err() != nil:
		log.Warn("unit abandoned: worker stopping")
	case uploadErr != nil || runErr != nil:
		cause := uploadErr
		if cause == nil {
			cause = runErr
		}
		log.Error("unit failed", "err", cause)
		r.report(u, log, "fail", func(ctx context.Context) error {
			return r.o.Transport.Fail(ctx, r.id, u.a.LeaseRef, cause.Error(), true)
		})
	case result.Truncated && u.mode.Load() == modeRelease:
		r.report(u, log, "release", func(ctx context.Context) error {
			return r.o.Transport.Release(ctx, r.id, u.a.LeaseRef)
		})
	default:
		r.report(u, log, "complete", func(ctx context.Context) error {
			return r.o.Transport.Complete(ctx, r.id, u.a.LeaseRef, result.EndIndex, u.a.NextPartSeq+result.Parts)
		})
	}
}

func (r *Runtime) generate(u *unitRun, compiled *snapshot.Compiled, sink *uploadSink) (pipeline.Result, error) {
	spec, ok := compiled.Stream(u.a.Stream)
	if !ok {
		return pipeline.Result{}, fmt.Errorf("the snapshot has no stream %q", u.a.Stream)
	}
	enc, err := compiled.NewEncoder(spec)
	if err != nil {
		return pipeline.Result{}, err
	}
	generators := make([]gen.StreamGenerator, max(runtime.GOMAXPROCS(0)/max(r.o.Capabilities.Slots, 1), 1))
	for i := range generators {
		if generators[i], err = compiled.Engine.NewStreamGenerator(spec.ID); err != nil {
			return pipeline.Result{}, err
		}
	}
	sink.compression = compiled.Snapshot.Compression
	return pipeline.Run(u.ctx, pipeline.Unit{
		Generators: generators, Stream: u.a.Stream, From: u.a.ResumeFrom, End: u.a.RangeEnd, FirstSeq: u.a.NextPartSeq,
		BatchSize: compiled.Snapshot.BatchSize, QueueCap: compiled.Snapshot.QueueCapacity, Encoder: enc, Sink: sink,
		Limits: pipeline.PartLimits{
			MaxBytes: u.a.PartLimits.MaxBytes, MaxRecords: uint64(u.a.PartLimits.MaxRecords),
			MaxAge: time.Duration(u.a.PartLimits.MaxAgeMS) * time.Millisecond,
		},
		RecordsPerSecond: u.a.Rate, MaxRecordBytes: spec.MaxRecordBytes, Clock: r.o.Clock, Stats: &u.stats, Finish: u.finish, Control: &u.control,
	})
}

// report delivers a unit's outcome, retrying while the lease can still be trusted. If it
// never arrives the lease expires, and the unit is simply run again.
func (r *Runtime) report(u *unitRun, log *slog.Logger, what string, send func(context.Context) error) {
	for attempt := 0; ; attempt++ {
		err := send(u.ctx)
		if err == nil {
			return
		}
		if wire.IsFenced(err) || !wire.IsRetryable(err) {
			log.Warn("coordinator refused report", "report", what, "err", err)
			return
		}
		if u.ctx.Err() != nil || !u.leaseValid(r.o.Clock.Now()) {
			log.Warn("giving up on report", "report", what, "err", err)
			return
		}
		if !r.sleep(u.ctx, Backoff(attempt, r.o.Rand())) {
			return
		}
	}
}

// compile fetches and prepares a snapshot, retrying network failures while ctx lasts.
func (r *Runtime) compile(ctx context.Context, hash string) (*snapshot.Compiled, error) {
	for attempt := 0; ; attempt++ {
		body, err := r.o.Transport.Snapshot(ctx, r.id, hash)
		if err == nil {
			snap, err := snapshot.Decode(body, hash)
			if err != nil {
				return nil, err
			}
			return snap.Compile(r.o.Vocab)
		}
		if !wire.IsRetryable(err) {
			return nil, err
		}
		if !r.sleep(ctx, Backoff(attempt, r.o.Rand())) {
			return nil, ctx.Err()
		}
	}
}

func resetDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o750)
}

// engineCache keeps compiled snapshots, because building an engine can be expensive and
// units of one session arrive one after another.
type engineCache struct {
	mu      sync.Mutex
	limit   int
	entries map[string]*engineEntry
	order   []string
	build   func(context.Context, string) (*snapshot.Compiled, error)
}

type engineEntry struct {
	mu       sync.Mutex
	compiled *snapshot.Compiled
}

func newEngineCache(limit int, build func(context.Context, string) (*snapshot.Compiled, error)) *engineCache {
	return &engineCache{limit: limit, entries: map[string]*engineEntry{}, build: build}
}

func (c *engineCache) get(ctx context.Context, hash string) (*snapshot.Compiled, error) {
	c.mu.Lock()
	e := c.entries[hash]
	if e == nil {
		e = &engineEntry{}
		c.entries[hash] = e
		c.order = append(c.order, hash)
		if len(c.order) > c.limit {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
	}
	c.mu.Unlock()

	// One build per snapshot; concurrent units wait for it. A failure is not remembered.
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.compiled != nil {
		return e.compiled, nil
	}
	compiled, err := c.build(ctx, hash)
	if err != nil {
		return nil, err
	}
	e.compiled = compiled
	return compiled, nil
}

var _ output.Sink = (*uploadSink)(nil)
