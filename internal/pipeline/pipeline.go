// Package pipeline executes one range of a stream: generators fill batches, a bounded
// channel carries them to an encoder, and the encoder writes parts to a sink. A slow
// sink blocks the encoder, which fills the channel, which blocks the generators, so
// memory stays bounded however slow the destination is.
// See docs/01-architecture/generation-pipeline.md.
package pipeline

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/record"
	"github.com/genadigeno/data-feed/internal/serial"
)

const (
	writeBufferBytes = 1 << 20
	// rateWindowsPerSecond makes batches small enough at low rates that output flows
	// steadily rather than in long silent gaps.
	rateWindowsPerSecond = 10
)

// PartLimits rotate parts; 0 means no limit of that kind. Bytes count before compression.
type PartLimits struct {
	MaxBytes   int64
	MaxRecords uint64
	MaxAge     time.Duration
}

// Stats are counters a run updates as it goes, safe to read at any time.
type Stats struct {
	Records atomic.Uint64
	Bytes   atomic.Int64 // serialized, before compression
	Parts   atomic.Int64
	// Time spent blocked, which says where a slow run's bottleneck is.
	GenBlockedNanos  atomic.Int64 // generators waiting for room: output is the bottleneck
	EncWaitNanos     atomic.Int64 // encoder waiting for records: generation is the bottleneck
	SinkBlockedNanos atomic.Int64 // waiting for the sink: the destination is the bottleneck
	RateWaitNanos    atomic.Int64 // waiting for the rate limit: as configured
}

// Unit is one run.
type Unit struct {
	Generators []gen.StreamGenerator // one per goroutine, at least one
	Stream     string
	From, End  uint64 // exclusive End
	FirstSeq   int    // sequence number of the first part
	BatchSize  int
	QueueCap   int // batches between generators and encoder
	Encoder    serial.Encoder
	Sink       output.Sink
	Limits     PartLimits
	// RecordsPerSecond paces the run; 0 means unlimited.
	RecordsPerSecond float64
	// MaxRecordBytes tells the sink how far past Limits.MaxBytes a part can go.
	MaxRecordBytes int64
	Pool           *record.Pool
	Clock          clock.Clock
	Stats          *Stats
	// Finish, when closed, ends the run cleanly at the next batch: the open part is closed
	// and handed over. Used for graceful stops, pauses and deadlines.
	Finish <-chan struct{}
	// Control, if set, adjusts the run while it goes.
	Control *Control
	// FlushBatches passes each batch to the part as soon as it is encoded, for a reader
	// waiting at the other end, rather than when the write buffer fills.
	FlushBatches bool
}

// Control adjusts a running unit. A run that started without a rate has nothing to adjust.
type Control struct{ pacer atomic.Pointer[pacer] }

// SetRate changes the record rate of the run. It takes effect from the next batch.
func (c *Control) SetRate(perSecond float64) {
	p := c.pacer.Load()
	if p == nil || perSecond <= 0 {
		return
	}
	p.lim.SetLimitAt(p.clk.Now(), rate.Limit(perSecond))
}

// Result says how a run ended.
type Result struct {
	// EndIndex is where a later run must continue: everything below it is in parts that
	// were accepted, and nothing at or above it is.
	EndIndex  uint64
	Parts     int
	Records   uint64
	Truncated bool // finished early through Finish
}

// Run blocks until the unit is done, Finish is closed, ctx is cancelled, or something
// fails. All its goroutines have exited by the time it returns. A cancelled or failed
// run discards its open part; parts already accepted stay valid.
func Run(ctx context.Context, u Unit) (result Result, err error) {
	if len(u.Generators) == 0 {
		return Result{}, errors.New("pipeline: no generators")
	}
	if u.Pool == nil {
		u.Pool = record.NewPool(u.BatchSize)
	}
	if u.Stats == nil {
		u.Stats = &Stats{}
	}
	if u.Clock == nil {
		u.Clock = clock.Real()
	}
	limit := u.BatchSize
	var limiter *pacer
	if u.RecordsPerSecond > 0 {
		limit = max(1, min(limit, int(u.RecordsPerSecond/rateWindowsPerSecond)))
		limiter = newPacer(u.RecordsPerSecond, limit, u.Clock, u.Stats)
		if u.Control != nil {
			u.Control.pacer.Store(limiter)
		}
	}

	genCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var genErr error
	var errOnce sync.Once
	fail := func(e error) {
		errOnce.Do(func() {
			genErr = e
			cancel()
		})
	}

	// The batches channel is closed when all generation has stopped.
	batches, truncated, nextIndex, wait := u.startGenerators(genCtx, limit, limiter, fail)

	enc := &partWriter{u: &u, ctx: ctx}
	defer func() {
		p := recover()
		cancel()
		drain(batches, u.Pool)
		wait()
		enc.abort()
		if p != nil {
			result = enc.result(u.From)
			err = fmt.Errorf("encoder panicked: %v\n%s", p, debug.Stack())
		}
	}()

	result.EndIndex = u.From
	for {
		start := u.Clock.Now()
		b, ok := <-batches
		u.Stats.EncWaitNanos.Add(int64(u.Clock.Now().Sub(start)))
		if !ok {
			break
		}
		if err := enc.writeBatch(b); err != nil {
			u.Pool.Put(b)
			return enc.result(u.From), err
		}
		u.Pool.Put(b)
	}
	wait()

	switch {
	case genErr != nil:
		return enc.result(u.From), genErr
	case ctx.Err() != nil:
		return enc.result(u.From), ctx.Err()
	}

	end := nextIndex()
	if err := enc.closeLast(end); err != nil {
		return enc.result(u.From), err
	}
	r := enc.result(u.From)
	r.EndIndex = end
	r.Truncated = truncated.Load()
	return r, nil
}

func drain(ch <-chan *record.Batch, pool *record.Pool) {
	for b := range ch {
		pool.Put(b)
	}
}

// startGenerators runs generation. With several generators, ranges are handed out in
// order and each result has its own slot in an ordered queue, so the encoder sees
// batches in index order however the generators race.
func (u *Unit) startGenerators(ctx context.Context, limit int, p *pacer, fail func(error)) (<-chan *record.Batch, *atomic.Bool, func() uint64, func()) {
	out := make(chan *record.Batch, u.QueueCap)
	truncated := &atomic.Bool{}
	var end atomic.Uint64
	end.Store(u.From)
	var wg sync.WaitGroup

	finished := func() bool {
		select {
		case <-u.Finish:
			truncated.Store(true)
			return true
		default:
			return false
		}
	}

	send := func(b *record.Batch) bool {
		start := u.Clock.Now()
		defer func() { u.Stats.GenBlockedNanos.Add(int64(u.Clock.Now().Sub(start))) }()
		select {
		case out <- b:
			return true
		case <-ctx.Done():
			u.Pool.Put(b)
			return false
		}
	}

	if len(u.Generators) == 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(out)
			defer recoverPanic(fail)
			next := u.From
			for next < u.End && ctx.Err() == nil && !finished() {
				b := u.Pool.Get()
				n, err := u.Generators[0].Fill(b, next, u.End, limit)
				if err != nil {
					u.Pool.Put(b)
					fail(err)
					return
				}
				next = n
				if b.Len() == 0 {
					u.Pool.Put(b)
					end.Store(next)
					continue
				}
				if !p.wait(ctx, u.Finish, b.Len()) {
					u.Pool.Put(b)
					finished()
					return
				}
				if !send(b) {
					return
				}
				end.Store(next)
			}
		}()
		return out, truncated, end.Load, wg.Wait
	}

	// Several generators: an ordered queue of single-use result channels.
	type job struct {
		from, to uint64
		result   chan jobResult
	}
	jobs := make(chan job)
	ordered := make(chan chan jobResult, u.QueueCap+len(u.Generators))
	width := uint64(max(limit, 1))

	wg.Add(1)
	go func() { // dispatcher
		defer wg.Done()
		defer close(jobs)
		defer close(ordered)
		for next := u.From; next < u.End && ctx.Err() == nil && !finished(); {
			to := min(next+width, u.End)
			if to <= next {
				to = u.End
			}
			result := make(chan jobResult, 1)
			select {
			case ordered <- result:
			case <-u.Finish:
				return
			case <-ctx.Done():
				return
			}
			select {
			case jobs <- job{next, to, result}:
			case <-u.Finish:
				return
			case <-ctx.Done():
				return
			}
			next = to
		}
	}()

	for _, g := range u.Generators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer recoverPanic(fail)
			for j := range jobs {
				b := u.Pool.Get()
				next := j.from
				for next < j.to {
					n, err := g.Fill(b, next, j.to, int(j.to-j.from))
					if err != nil {
						u.Pool.Put(b)
						fail(err)
						j.result <- jobResult{}
						return
					}
					next = n
				}
				j.result <- jobResult{batch: b, end: j.to}
			}
		}()
	}

	wg.Add(1)
	go func() { // collector: restores index order
		defer wg.Done()
		defer close(out)
		defer recoverPanic(fail)
		for result := range ordered {
			select {
			case r := <-result:
				if r.batch == nil {
					return
				}
				// Batches generated ahead are not part of the output until they are sent, so a
				// finished run drops them: end has not moved past them, and they would otherwise
				// all pass at once, whatever the rate.
				if finished() {
					u.Pool.Put(r.batch)
					return
				}
				if r.batch.Len() == 0 {
					u.Pool.Put(r.batch)
					end.Store(r.end)
					continue
				}
				if !p.wait(ctx, u.Finish, r.batch.Len()) {
					u.Pool.Put(r.batch)
					finished()
					return
				}
				if !send(r.batch) {
					return
				}
				end.Store(r.end)
			case <-u.Finish:
				finished()
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, truncated, end.Load, wg.Wait
}

type jobResult struct {
	batch *record.Batch
	end   uint64
}

func recoverPanic(fail func(error)) {
	if p := recover(); p != nil {
		fail(fmt.Errorf("generator panicked: %v\n%s", p, debug.Stack()))
	}
}

// pacer enforces a record rate, reading time through the clock so tests stay fast.
type pacer struct {
	lim   *rate.Limiter
	clk   clock.Clock
	stats *Stats
}

func newPacer(perSecond float64, burst int, clk clock.Clock, stats *Stats) *pacer {
	lim := rate.NewLimiter(rate.Limit(perSecond), burst)
	lim.AllowN(clk.Now(), burst) // start empty, so the rate holds from the first record
	return &pacer{lim: lim, clk: clk, stats: stats}
}

// wait returns false if ctx ended or Finish closed: the batch has not earned its place in
// the output yet, and nothing has been promised about it, so it is simply not sent.
func (p *pacer) wait(ctx context.Context, finish <-chan struct{}, n int) bool {
	if p == nil {
		return true
	}
	now := p.clk.Now()
	delay := p.lim.ReserveN(now, n).DelayFrom(now)
	if delay <= 0 {
		return true
	}
	defer p.stats.RateWaitNanos.Add(int64(delay))
	select {
	case <-p.clk.After(delay):
		return true
	case <-finish:
		return false
	case <-ctx.Done():
		return false
	}
}

// partWriter encodes records into rotating parts. Rotation happens when the next record
// arrives, so a part's range runs up to the record that starts the next part: ranges
// join up exactly, and no empty part is ever created.
type partWriter struct {
	u   *Unit
	ctx context.Context

	file    output.PartFile
	w       *bufio.Writer
	count   countingWriter
	opened  time.Time
	seq     int
	first   uint64
	records uint64
	parts   int
	total   uint64
	header  int
	lastEnd uint64
	hasEnd  bool
}

// countingWriter counts bytes reaching the part, before any compression.
type countingWriter struct {
	file output.PartFile
	n    int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.file.Write(b)
	c.n += int64(n)
	return n, err
}

func (p *partWriter) writeBatch(b *record.Batch) error {
	for i := range b.Records {
		index := b.Indices[i]
		if p.file != nil && p.rotationDue() {
			if err := p.closePart(index); err != nil {
				return err
			}
		}
		if p.file == nil {
			if err := p.openPart(); err != nil {
				return err
			}
		}
		before := p.count.n + int64(p.w.Buffered())
		if err := p.u.Encoder.Encode(p.w, &b.Records[i], index); err != nil {
			return fmt.Errorf("write record %d: %w", index, err)
		}
		p.records++
		p.total++
		p.u.Stats.Records.Add(1)
		p.u.Stats.Bytes.Add(p.count.n + int64(p.w.Buffered()) - before)
	}
	if p.u.FlushBatches && p.w != nil {
		return p.w.Flush()
	}
	return nil
}

func (p *partWriter) rotationDue() bool {
	l := p.u.Limits
	if l.MaxRecords > 0 && p.records >= l.MaxRecords {
		return true
	}
	if l.MaxBytes > 0 && p.count.n+int64(p.w.Buffered()) >= l.MaxBytes {
		return true
	}
	return l.MaxAge > 0 && p.u.Clock.Now().Sub(p.opened) >= l.MaxAge
}

func (p *partWriter) openPart() error {
	start := p.u.Clock.Now()
	// The first part also covers any empty indices before its first row.
	first := p.u.From
	if p.hasEnd {
		first = p.lastEnd
	}
	seq := p.u.FirstSeq + p.parts
	maxBytes := int64(0)
	if p.u.Limits.MaxBytes > 0 {
		maxBytes = p.u.Limits.MaxBytes + max(p.u.MaxRecordBytes, 0) + int64(len(p.u.Encoder.Footer()))
	}
	file, err := p.u.Sink.Open(p.ctx, output.PartRequest{
		Stream: p.u.Stream, Seq: seq, Extension: p.u.Encoder.Extension(), MaxBytes: maxBytes,
	})
	p.u.Stats.SinkBlockedNanos.Add(int64(p.u.Clock.Now().Sub(start)))
	if err != nil {
		return fmt.Errorf("open part %d: %w", seq, err)
	}
	p.file = file
	p.count = countingWriter{file: file}
	if p.w == nil {
		p.w = bufio.NewWriterSize(&p.count, writeBufferBytes)
	} else {
		p.w.Reset(&p.count)
	}
	p.opened = p.u.Clock.Now()
	p.seq, p.first, p.records = seq, first, 0

	p.u.Encoder.StartPart()
	header := p.u.Encoder.Header()
	p.header = len(header)
	if _, err := p.w.Write(header); err != nil {
		return fmt.Errorf("write part %d: %w", seq, err)
	}
	return nil
}

// closePart finishes the open part, whose range ends at end.
func (p *partWriter) closePart(end uint64) error {
	footer := p.u.Encoder.Footer()
	if _, err := p.w.Write(footer); err != nil {
		return fmt.Errorf("write part %d: %w", p.seq, err)
	}
	if err := p.w.Flush(); err != nil {
		return fmt.Errorf("write part %d: %w", p.seq, err)
	}
	done, err := p.file.Commit()
	if err != nil {
		return fmt.Errorf("commit part %d: %w", p.seq, err)
	}
	p.file = nil

	done.Stream, done.Seq = p.u.Stream, p.seq
	done.FirstIndex, done.EndIndex, done.Records = p.first, end, p.records
	done.HeaderLen, done.FooterLen = p.header, len(footer)

	start := p.u.Clock.Now()
	err = p.u.Sink.Accept(p.ctx, done)
	p.u.Stats.SinkBlockedNanos.Add(int64(p.u.Clock.Now().Sub(start)))
	if err != nil {
		return fmt.Errorf("hand over part %d: %w", p.seq, err)
	}
	p.parts++
	p.lastEnd, p.hasEnd = end, true
	p.u.Stats.Parts.Add(1)
	return nil
}

// closeLast finishes the run's final part, if one is open.
func (p *partWriter) closeLast(end uint64) error {
	if p.file == nil {
		return nil
	}
	return p.closePart(end)
}

func (p *partWriter) abort() {
	if p.file != nil {
		_ = p.file.Abort()
		p.file = nil
	}
}

func (p *partWriter) result(from uint64) Result {
	r := Result{EndIndex: from, Parts: p.parts, Records: p.total - p.records}
	if p.file == nil {
		r.Records = p.total
	}
	if p.hasEnd {
		r.EndIndex = p.lastEnd
	}
	return r
}
