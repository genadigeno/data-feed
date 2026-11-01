package output

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestFilePartAppearsOnlyOnCommit(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "sub", "part.csv")
	p, err := NewFilePart(final, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(final); err == nil {
		t.Fatal("final file exists before commit")
	}

	done, err := p.Commit()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "hello" {
		t.Fatalf("%q %v", data, err)
	}
	if done.Path != final || done.Bytes != 5 || done.SHA256 != sha256.Sum256([]byte("hello")) {
		t.Fatalf("%+v", done)
	}
	if names := listDir(t, filepath.Dir(final)); len(names) != 1 {
		t.Fatalf("leftovers: %v", names)
	}
	if err := p.Abort(); err != nil {
		t.Fatalf("abort after commit: %v", err)
	}
	if _, err := os.Stat(final); err != nil {
		t.Fatal("abort after commit removed the file")
	}
}

func TestFilePartAbortLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePart(filepath.Join(dir, "part.csv"), CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := p.Abort(); err != nil {
		t.Fatalf("second abort: %v", err)
	}
	if names := listDir(t, dir); len(names) != 0 {
		t.Fatalf("leftovers: %v", names)
	}
}

func TestGzipPartHashesWhatIsOnDisk(t *testing.T) {
	final := filepath.Join(t.TempDir(), "part.csv.gz")
	p, err := NewFilePart(final, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("compressible "), 1000)
	if _, err := p.Write(payload); err != nil {
		t.Fatal(err)
	}
	done, err := p.Commit()
	if err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if done.Bytes != int64(len(onDisk)) || done.Bytes >= int64(len(payload)) || done.SHA256 != sha256.Sum256(onDisk) {
		t.Fatalf("%+v, %d on disk", done, len(onDisk))
	}
	zr, err := gzip.NewReader(bytes.NewReader(onDisk))
	if err != nil {
		t.Fatal(err)
	}
	back, err := io.ReadAll(zr)
	if err != nil || !bytes.Equal(back, payload) {
		t.Fatalf("round trip: %v", err)
	}
}

func TestSameBytesSameGzip(t *testing.T) {
	sum := func() [32]byte {
		p, err := NewFilePart(filepath.Join(t.TempDir(), "p.gz"), CompressionGzip)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Write(bytes.Repeat([]byte("abc"), 500)); err != nil {
			t.Fatal(err)
		}
		done, err := p.Commit()
		if err != nil {
			t.Fatal(err)
		}
		return done.SHA256
	}
	first, second := sum(), sum()
	if first != second {
		t.Fatal("regenerated parts must have equal checksums")
	}
}

func TestStreamDir(t *testing.T) {
	if StreamDir("table:orders") != "table-orders" || StreamDir("main") != "main" {
		t.Fatal(StreamDir("table:orders"))
	}
}

func TestDirSinkNamesParts(t *testing.T) {
	dir := t.TempDir()
	var seen []FinishedPart
	s := &DirSink{Dir: dir, OnPart: func(p FinishedPart) { seen = append(seen, p) }}
	ctx := context.Background()
	f, err := s.Open(ctx, PartRequest{Stream: "table:orders", Seq: 3, Extension: "csv"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	done, err := f.Commit()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "table-orders", "part-000003.csv")
	if done.Path != want {
		t.Fatalf("path %q, want %q", done.Path, want)
	}
	if err := s.Accept(ctx, done); err != nil || len(seen) != 1 {
		t.Fatalf("%v %v", seen, err)
	}

	gz := &DirSink{Dir: dir, Compression: CompressionGzip}
	f, err = gz.Open(ctx, PartRequest{Stream: "main", Seq: 0, Extension: "ndjson"})
	if err != nil {
		t.Fatal(err)
	}
	done, err = f.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(done.Path) != "part-000000.ndjson.gz" {
		t.Fatalf("%q", done.Path)
	}
}

func TestSingleOutputSinksRefuseASecondPart(t *testing.T) {
	ctx := context.Background()
	for name, s := range map[string]Sink{
		"file":   &FileSink{Path: filepath.Join(t.TempDir(), "x")},
		"writer": &WriterSink{W: io.Discard},
	} {
		if _, err := s.Open(ctx, PartRequest{Seq: 1}); err == nil {
			t.Errorf("%s accepted a second part", name)
		}
		if err := s.Accept(ctx, FinishedPart{}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestWriterSinkPassesBytesThrough(t *testing.T) {
	var buf bytes.Buffer
	s := &WriterSink{W: &buf}
	p, err := s.Open(context.Background(), PartRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	done, err := p.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "abc" || done.Bytes != 3 || done.SHA256 != sha256.Sum256([]byte("abc")) {
		t.Fatalf("%q %+v", buf.String(), done)
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
}
