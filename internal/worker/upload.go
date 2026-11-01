package worker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/wire"
)

// uploadQueue bounds parts waiting within one unit; the spool bounds their bytes.
const uploadQueue = 1024

// Spool accounts for disk space used by parts on their way to the Coordinator. When it is
// full, opening a part blocks, which backs up into generation.
type Spool struct {
	limit int64 // 0 means no limit

	mu      sync.Mutex
	used    int64
	changed chan struct{}
}

// NewSpool returns a Spool.
func NewSpool(limit int64) *Spool {
	return &Spool{limit: limit, changed: make(chan struct{})}
}

// Used returns the reserved bytes.
func (s *Spool) Used() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// Reserve waits for room. A part bigger than the whole spool still goes through once the
// spool is empty, or it could never be written.
func (s *Spool) Reserve(ctx context.Context, n int64) error {
	for {
		s.mu.Lock()
		if s.limit == 0 || s.used == 0 || s.used+n <= s.limit {
			s.used += n
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release gives back n bytes.
func (s *Spool) Release(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.used -= n
	close(s.changed)
	s.changed = make(chan struct{})
}

// spoolPart is a part being written within its reservation.
type spoolPart struct {
	output.PartFile
	spool    *Spool
	reserved int64
	done     bool
}

// Commit shrinks the reservation to what the part really took.
func (p *spoolPart) Commit() (output.FinishedPart, error) {
	fp, err := p.PartFile.Commit()
	p.done = true
	if err != nil {
		p.spool.Release(p.reserved)
		return fp, err
	}
	if p.reserved > fp.Bytes {
		p.spool.Release(p.reserved - fp.Bytes)
	}
	return fp, nil
}

// Abort discards the part and its reservation.
func (p *spoolPart) Abort() error {
	err := p.PartFile.Abort()
	if !p.done {
		p.done = true
		p.spool.Release(p.reserved)
	}
	return err
}

// spooled is a finished part waiting to be uploaded.
type spooled struct {
	part      output.FinishedPart
	extension string
	acked     chan error // set when the sink must wait for the upload
}

// uploadSink writes parts into the spool and uploads them in order, one at a time per unit.
type uploadSink struct {
	r           *Runtime
	u           *unitRun
	log         *slog.Logger
	compression string
	extension   string // of the part being written; the pipeline writes one at a time

	queue     chan spooled
	closeOnce sync.Once
	done      chan struct{}
	err       error // the failure that stopped uploads; read after done closes
}

func newUploadSink(r *Runtime, u *unitRun, log *slog.Logger) *uploadSink {
	return &uploadSink{r: r, u: u, log: log, queue: make(chan spooled, uploadQueue), done: make(chan struct{})}
}

// Open implements output.Sink.
func (s *uploadSink) Open(ctx context.Context, r output.PartRequest) (output.PartFile, error) {
	if err := s.r.spool.Reserve(ctx, r.MaxBytes); err != nil {
		return nil, err
	}
	s.extension = r.Extension
	if s.compression == output.CompressionGzip {
		s.extension += ".gz"
	}
	name := fmt.Sprintf("%s-%06d.%s", s.u.a.UnitID, r.Seq, s.extension)
	f, err := output.NewFilePart(filepath.Join(s.r.o.SpoolDir, name), s.compression)
	if err != nil {
		s.r.spool.Release(r.MaxBytes)
		return nil, err
	}
	return &spoolPart{PartFile: f, spool: s.r.spool, reserved: r.MaxBytes}, nil
}

// Accept implements output.Sink.
func (s *uploadSink) Accept(ctx context.Context, p output.FinishedPart) error {
	item := spooled{part: p, extension: s.extension}
	if !s.r.o.SpoolEnabled {
		item.acked = make(chan error, 1)
	}
	select {
	case s.queue <- item:
	case <-ctx.Done():
		s.discard(item)
		return ctx.Err()
	}
	if item.acked == nil {
		return nil
	}
	select {
	case err := <-item.acked:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// close waits for queued parts to be delivered and returns what stopped them, if anything.
func (s *uploadSink) close() error {
	s.closeOnce.Do(func() { close(s.queue) })
	<-s.done
	return s.err
}

func (s *uploadSink) discard(item spooled) {
	if err := os.Remove(item.part.Path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.log.Warn("remove spooled part", "path", item.part.Path, "err", err)
	}
	s.r.spool.Release(item.part.Bytes)
}

func (s *uploadSink) loop() {
	defer close(s.done)
	for item := range s.queue {
		if s.err != nil || s.u.ctx.Err() != nil {
			s.discard(item)
			if item.acked != nil {
				item.acked <- context.Canceled
			}
			continue
		}
		err := s.deliver(item)
		s.discard(item)
		if err == nil {
			s.u.acked.Add(item.part.Records)
		} else {
			// The rest of the unit cannot be accepted without this part. Only generation
			// stops: the lease is still good for reporting what happened.
			s.err = err
			s.u.stopRun()
		}
		if item.acked != nil {
			item.acked <- err
		}
	}
}

// deliver uploads a part until it is acknowledged. A failed attempt may still have been
// committed, so a retry asks first.
func (s *uploadSink) deliver(item spooled) error {
	u, a := s.u, s.u.a
	meta := wire.PartMeta{
		LeaseRef: a.LeaseRef, Seq: item.part.Seq, FirstIndex: item.part.FirstIndex, EndIndex: item.part.EndIndex,
		Records: item.part.Records, Bytes: item.part.Bytes, SHA256: hex.EncodeToString(item.part.SHA256[:]),
		HeaderLen: item.part.HeaderLen, FooterLen: item.part.FooterLen, Extension: item.extension,
	}
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			committed, sum, err := s.r.o.Transport.PartCommitted(u.ctx, s.r.id, a.UnitID, meta.Seq)
			if err == nil && committed {
				if sum == meta.SHA256 {
					return nil
				}
				return fmt.Errorf("part %d was committed with different content", meta.Seq)
			}
		}
		err := s.upload(meta, item.part.Path)
		switch {
		case err == nil:
			return nil
		case wire.IsFenced(err):
			u.markLost()
			return err
		case !wire.IsRetryable(err):
			return err
		case errors.Is(err, fs.ErrNotExist):
			return fmt.Errorf("spooled part is gone: %w", err)
		}
		if u.ctx.Err() != nil {
			return u.ctx.Err()
		}
		if !u.leaseValid(s.r.o.Clock.Now()) {
			u.markLost()
			return fmt.Errorf("lease timed out while uploading: %w", err)
		}
		if attempt == 0 {
			s.log.Warn("part upload failed, retrying", "seq", meta.Seq, "err", err)
		}
		if !s.r.sleep(u.ctx, Backoff(attempt, s.r.o.Rand())) {
			return u.ctx.Err()
		}
	}
}

func (s *uploadSink) upload(meta wire.PartMeta, path string) error {
	select {
	case s.r.uploads <- struct{}{}:
		defer func() { <-s.r.uploads }()
	case <-s.u.ctx.Done():
		return s.u.ctx.Err()
	}
	if s.r.o.LocalFiles {
		if _, err := os.Stat(path); err != nil {
			return err
		}
		_, err := s.r.o.Transport.UploadPart(s.u.ctx, s.r.id, meta, wire.PartBody{LocalPath: path})
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = s.r.o.Transport.UploadPart(s.u.ctx, s.r.id, meta, wire.PartBody{Reader: f})
	return err
}
