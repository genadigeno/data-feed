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

// filePart writes to a temporary name and renames on commit, so a crash or a cancelled
// run never leaves a complete-looking file behind.
type filePart struct {
	final string
	f     *os.File
	gz    *gzip.Writer
	hash  hash.Hash
	w     io.Writer
	bytes int64
	done  bool
}

// countingWriter counts what reaches the file.
type countingWriter struct {
	w io.Writer
	n *int64
}

func (c countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	*c.n += int64(n)
	return n, err
}

// NewFilePart starts a part that will appear at final when committed.
func NewFilePart(final, compression string) (PartFile, error) {
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(final)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}
	p := &filePart{final: final, f: f, hash: sha256.New()}
	p.w = countingWriter{io.MultiWriter(f, p.hash), &p.bytes}
	if compression == CompressionGzip {
		// BestSpeed: output volume is the point here, and parts stay reproducible.
		gz, err := gzip.NewWriterLevel(p.w, gzip.BestSpeed)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, err
		}
		p.gz = gz
		p.w = gz
	}
	return p, nil
}

func (p *filePart) Write(b []byte) (int, error) { return p.w.Write(b) }

func (p *filePart) Commit() (FinishedPart, error) {
	if p.gz != nil {
		if err := p.gz.Close(); err != nil {
			return FinishedPart{}, errors.Join(err, p.Abort())
		}
	}
	if err := p.f.Sync(); err != nil {
		return FinishedPart{}, errors.Join(fmt.Errorf("sync part: %w", err), p.Abort())
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
	out := FinishedPart{Path: p.final, Bytes: p.bytes}
	copy(out.SHA256[:], p.hash.Sum(nil))
	return out, nil
}

func (p *filePart) Abort() error {
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
