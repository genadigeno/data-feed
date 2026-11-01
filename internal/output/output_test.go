package output

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"runtime"
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

func TestFilePartKeepsOrderAcrossManyWrites(t *testing.T) {
	for _, compression := range []string{CompressionNone, CompressionGzip} {
		final := filepath.Join(t.TempDir(), "p.txt")
		part, err := NewFilePart(final, compression)
		if err != nil {
			t.Fatal(err)
		}
		var want bytes.Buffer
		for i := range 2000 {
			chunk := bytes.Repeat([]byte{byte(i)}, 1+i*37%5000)
			want.Write(chunk)
			if _, err := part.Write(chunk); err != nil {
				t.Fatal(err)
			}
		}
		big := bytes.Repeat([]byte("large"), partBufferBytes) // larger than a buffer
		want.Write(big)
		if _, err := part.Write(big); err != nil {
			t.Fatal(err)
		}
		done, err := part.Commit()
		if err != nil {
			t.Fatal(err)
		}
		onDisk, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if done.Bytes != int64(len(onDisk)) || done.SHA256 != sha256.Sum256(onDisk) {
			t.Fatalf("%s: size %d of %d, or wrong checksum", compression, done.Bytes, len(onDisk))
		}
		got := onDisk
		if compression == CompressionGzip {
			r, err := gzip.NewReader(bytes.NewReader(onDisk))
			if err != nil {
				t.Fatal(err)
			}
			if got, err = io.ReadAll(r); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got, want.Bytes()) {
			t.Fatalf("%s: content differs", compression)
		}
	}
}

func TestFilePartCallerMayReuseItsBuffer(t *testing.T) {
	final := filepath.Join(t.TempDir(), "p.txt")
	part, err := NewFilePart(final, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte("first")
	if _, err := part.Write(buf); err != nil {
		t.Fatal(err)
	}
	copy(buf, "XXXXX")
	if _, err := part.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(final); string(got) != "first" {
		t.Fatalf("%q", got)
	}
}

func TestFilePartAbortStopsItsGoroutines(t *testing.T) {
	dir := t.TempDir()
	before := runtime.NumGoroutine()
	for range 20 {
		part, err := NewFilePart(filepath.Join(dir, "p.txt"), CompressionGzip)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		if err := part.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := part.Abort(); err != nil {
			t.Fatalf("a second abort: %v", err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("%v %v", entries, err)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}

func TestFilePartReportsWriteFailures(t *testing.T) {
	final := filepath.Join(t.TempDir(), "p.txt")
	part, err := NewFilePart(final, CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	// Closing the file under it makes every write fail.
	if err := part.(*filePart).f.Close(); err != nil {
		t.Fatal(err)
	}
	var failed error
	for range 100 {
		if _, failed = part.Write([]byte("data")); failed != nil {
			break
		}
	}
	_, commitErr := part.Commit()
	if failed == nil && commitErr == nil {
		t.Fatal("a failed write went unnoticed")
	}
	if _, err := os.Stat(final); err == nil {
		t.Fatal("a failed part appeared complete")
	}
}
