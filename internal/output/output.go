// Package output is where serialized parts go: files, a directory of parts, or
// standard output. See docs/01-architecture/generation-pipeline.md.
package output

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Compression names.
const (
	CompressionNone = "none"
	CompressionGzip = "gzip"
)

// FinishedPart describes a complete part. It covers the contiguous index range
// [FirstIndex, EndIndex), which may include indices that hold no row.
type FinishedPart struct {
	Stream               string
	Seq                  int
	Path                 string // empty when not a file, such as standard output
	FirstIndex, EndIndex uint64
	Records              uint64
	Bytes                int64 // on disk, after compression
	SHA256               [32]byte
	HeaderLen, FooterLen int // before compression
	Compression          string
}

// PartFile receives one part's bytes.
type PartFile interface {
	io.Writer
	// Commit makes the part final. The returned part has only Path, Bytes and SHA256 set.
	Commit() (FinishedPart, error)
	// Abort discards the part. It is safe after Commit, where it does nothing.
	Abort() error
}

// PartRequest says what part is about to be written.
type PartRequest struct {
	Stream    string
	Seq       int
	Extension string
	// MaxBytes bounds the part, for sinks with limited room. 0 means no bound is known.
	MaxBytes int64
}

// Sink receives finished parts in order. Open and Accept may block, which is how a slow
// destination slows generation down; they must honour ctx.
type Sink interface {
	Open(ctx context.Context, r PartRequest) (PartFile, error)
	Accept(ctx context.Context, p FinishedPart) error
}

// Buffers and queue depth for a part's background work. They bound what a part can hold in
// memory: writes block when both queues are full, which backs up into generation.
const (
	partBufferBytes = 1 << 20
	partQueueDepth  = 2
)

var partBuffers = sync.Pool{New: func() any {
	b := make([]byte, partBufferBytes)
	return &b
}}

// filePart writes to a temporary name and renames on commit, so a crash or a cancelled
// run never leaves a complete-looking file behind.
//
// Compressing, checksumming and writing cost more than encoding, and on the caller's
// goroutine they would limit a whole run to what one core can do. So they happen behind the
// caller: Write hands over a buffer, one goroutine compresses and checksums in order, and
// another writes to the file.
type filePart struct {
	final string
	f     *os.File
	sync  bool
	gz    *gzip.Writer
	hash  hash.Hash
	bytes atomic.Int64

	toHash  chan *[]byte
	toWrite chan *[]byte
	wg      sync.WaitGroup
	failure atomic.Pointer[error]
	closed  bool
	done    bool
}

// handOff passes compressed output on to the file, in buffers of its own.
type handOff struct{ p *filePart }

func (h handOff) Write(b []byte) (int, error) {
	h.p.hash.Write(b)
	h.p.enqueueWrite(b)
	return len(b), nil
}

// NewFilePart starts a part that will appear at final when committed, safely on disk.
func NewFilePart(final, compression string) (PartFile, error) {
	return newFilePart(final, compression, true)
}

// NewScratchPart is NewFilePart for a file that will be regenerated if lost, which needs
// no sync to disk.
func NewScratchPart(final, compression string) (PartFile, error) {
	return newFilePart(final, compression, false)
}

func newFilePart(final, compression string, durable bool) (PartFile, error) {
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(final)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}
	p := &filePart{
		final: final, f: f, sync: durable, hash: sha256.New(),
		toHash: make(chan *[]byte, partQueueDepth), toWrite: make(chan *[]byte, partQueueDepth),
	}
	if compression == CompressionGzip {
		// BestSpeed: output volume is the point here, and parts stay reproducible.
		gz, err := gzip.NewWriterLevel(handOff{p}, gzip.BestSpeed)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, err
		}
		p.gz = gz
	}
	p.wg.Add(2)
	go p.hashLoop()
	go p.writeLoop()
	return p, nil
}

func (p *filePart) fail(err error) { p.failure.CompareAndSwap(nil, &err) }

func (p *filePart) failed() error {
	if e := p.failure.Load(); e != nil {
		return *e
	}
	return nil
}

func getBuffer(n int) *[]byte {
	if n > partBufferBytes {
		b := make([]byte, n)
		return &b
	}
	b := partBuffers.Get().(*[]byte)
	*b = (*b)[:n]
	return b
}

func putBuffer(b *[]byte) {
	if cap(*b) == partBufferBytes {
		*b = (*b)[:partBufferBytes]
		partBuffers.Put(b)
	}
}

func (p *filePart) enqueueWrite(b []byte) {
	for len(b) > 0 {
		n := min(len(b), partBufferBytes)
		buf := getBuffer(n)
		copy(*buf, b[:n])
		p.toWrite <- buf
		b = b[n:]
	}
}

func (p *filePart) hashLoop() {
	defer p.wg.Done()
	defer close(p.toWrite)
	for buf := range p.toHash {
		if p.gz != nil {
			if _, err := p.gz.Write(*buf); err != nil {
				p.fail(err)
			}
			putBuffer(buf)
			continue
		}
		p.hash.Write(*buf)
		p.toWrite <- buf
	}
	if p.gz != nil {
		if err := p.gz.Close(); err != nil {
			p.fail(err)
		}
	}
}

// writeLoop keeps draining after a failure, so that nothing upstream stays blocked.
func (p *filePart) writeLoop() {
	defer p.wg.Done()
	for buf := range p.toWrite {
		if p.failed() == nil {
			n, err := p.f.Write(*buf)
			p.bytes.Add(int64(n))
			if err != nil {
				p.fail(fmt.Errorf("write part: %w", err))
			}
		}
		putBuffer(buf)
	}
}

// Write takes a copy, so the caller can reuse b at once. A failure shows up on a later
// Write or at Commit.
func (p *filePart) Write(b []byte) (int, error) {
	if err := p.failed(); err != nil {
		return 0, err
	}
	buf := getBuffer(len(b))
	copy(*buf, b)
	p.toHash <- buf
	return len(b), nil
}

// finish waits for everything handed over to be done.
func (p *filePart) finish() {
	if !p.closed {
		p.closed = true
		close(p.toHash)
		p.wg.Wait()
	}
}

func (p *filePart) Commit() (FinishedPart, error) {
	p.finish()
	if err := p.failed(); err != nil {
		return FinishedPart{}, errors.Join(err, p.Abort())
	}
	if p.sync {
		if err := p.f.Sync(); err != nil {
			return FinishedPart{}, errors.Join(fmt.Errorf("sync part: %w", err), p.Abort())
		}
	}
	if err := p.f.Close(); err != nil {
		_ = os.Remove(p.f.Name())
		p.done = true
		return FinishedPart{}, fmt.Errorf("close part: %w", err)
	}
	p.done = true
	if err := os.Rename(p.f.Name(), p.final); err != nil {
		_ = os.Remove(p.f.Name())
		return FinishedPart{}, fmt.Errorf("finalize part: %w", err)
	}
	out := FinishedPart{Path: p.final, Bytes: p.bytes.Load()}
	copy(out.SHA256[:], p.hash.Sum(nil))
	return out, nil
}

func (p *filePart) Abort() error {
	p.finish()
	if p.done {
		return nil
	}
	p.done = true
	return errors.Join(p.f.Close(), os.Remove(p.f.Name()))
}

// StreamDir turns a stream name into a directory name: "table:orders" → "table-orders".
func StreamDir(stream string) string { return strings.ReplaceAll(stream, ":", "-") }

// DirSink writes each part as a numbered file under a directory, by stream.
type DirSink struct {
	Dir         string
	Compression string
	// OnPart, if set, is told about every finished part.
	OnPart func(FinishedPart)
}

func partName(r PartRequest, compression string) string {
	name := fmt.Sprintf("part-%06d.%s", r.Seq, r.Extension)
	if compression == CompressionGzip {
		name += ".gz"
	}
	return name
}

// Open implements Sink.
func (s *DirSink) Open(_ context.Context, r PartRequest) (PartFile, error) {
	return NewFilePart(filepath.Join(s.Dir, StreamDir(r.Stream), partName(r, s.Compression)), s.Compression)
}

// Accept implements Sink.
func (s *DirSink) Accept(_ context.Context, p FinishedPart) error {
	p.Compression = s.Compression
	if s.OnPart != nil {
		s.OnPart(p)
	}
	return nil
}

// FileSink writes a single part to one file. Rotation must be off.
type FileSink struct {
	Path        string
	Compression string
	OnPart      func(FinishedPart)
}

// Open implements Sink.
func (s *FileSink) Open(_ context.Context, r PartRequest) (PartFile, error) {
	if r.Seq != 0 {
		return nil, fmt.Errorf("a single output file holds one part, but part %d was requested", r.Seq)
	}
	return NewFilePart(s.Path, s.Compression)
}

// Accept implements Sink.
func (s *FileSink) Accept(_ context.Context, p FinishedPart) error {
	p.Compression = s.Compression
	if s.OnPart != nil {
		s.OnPart(p)
	}
	return nil
}

// WriterSink streams a single part to a writer such as standard output.
type WriterSink struct {
	W      io.Writer
	OnPart func(FinishedPart)
}

type writerPart struct {
	w     io.Writer
	hash  hash.Hash
	bytes int64
}

// Open implements Sink.
func (s *WriterSink) Open(_ context.Context, r PartRequest) (PartFile, error) {
	if r.Seq != 0 {
		return nil, fmt.Errorf("a single output holds one part, but part %d was requested", r.Seq)
	}
	return &writerPart{w: s.W, hash: sha256.New()}, nil
}

// Accept implements Sink.
func (s *WriterSink) Accept(_ context.Context, p FinishedPart) error {
	if s.OnPart != nil {
		s.OnPart(p)
	}
	return nil
}

func (p *writerPart) Write(b []byte) (int, error) {
	p.hash.Write(b)
	n, err := p.w.Write(b)
	p.bytes += int64(n)
	return n, err
}

func (p *writerPart) Commit() (FinishedPart, error) {
	out := FinishedPart{Bytes: p.bytes}
	copy(out.SHA256[:], p.hash.Sum(nil))
	return out, nil
}

func (p *writerPart) Abort() error { return nil }
