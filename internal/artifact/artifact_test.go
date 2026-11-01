package artifact

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/pipeline"
	"github.com/genadigeno/data-feed/internal/schema"
	"github.com/genadigeno/data-feed/internal/serial"
)

func TestPartPathRefusesAnythingUnsafe(t *testing.T) {
	got, err := PartPath("ses_1", "table-orders", 3, 2, 4, "csv")
	if err != nil || got != "ses_1/table-orders/u000003-p0002.f4.csv" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := PartPath("s", "main", 0, 0, 1, "ndjson.gz"); err != nil {
		t.Fatalf("gzip extension: %v", err)
	}
	for _, c := range [][3]string{
		{"../evil", "main", "csv"}, {"a/b", "main", "csv"}, {"a\\b", "main", "csv"}, {"", "main", "csv"}, {"s", "..", "csv"},
		{"s", "table-../x", "csv"}, {"s", "table-A", "csv"}, {"s", "other", "csv"}, {"s", "main", "../x"}, {"s", "main", "c/v"},
	} {
		if _, err := PartPath(c[0], c[1], 0, 0, 1, c[2]); err == nil {
			t.Errorf("%v accepted", c)
		}
	}
	if _, err := PartPath("s", "main", -1, 0, 1, "csv"); err == nil {
		t.Error("negative sequence accepted")
	}
}

func TestStoredPathsCannotEscape(t *testing.T) {
	s := New(t.TempDir())
	for _, bad := range []string{"../datafeed.db", "a/../../x", "/etc/passwd", `..\x`} {
		if bad == `..\x` && filepath.Separator != '\\' {
			continue
		}
		if _, err := s.Open(bad); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Open(%q): %v", bad, err)
		}
		if err := s.Remove(bad); err == nil {
			t.Errorf("Remove(%q) accepted", bad)
		}
		if err := s.Promote("x", bad); err == nil {
			t.Errorf("Promote(%q) accepted", bad)
		}
	}
}

func TestPromoteOpenRemove(t *testing.T) {
	s := New(t.TempDir())
	staged, err := s.StagingFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staged.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}

	rel, err := PartPath("s1", "main", 0, 0, 1, "csv")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Promote(staged.Name(), rel); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staged.Name()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("staged file remains: %v", err)
	}

	f, err := s.Open(rel)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(f)
	f.Close()
	if string(data) != "data" {
		t.Fatalf("%q", data)
	}

	files, err := s.Scan()
	if err != nil || len(files) != 1 || files[0].RelPath != rel || files[0].Size != 4 {
		t.Fatalf("scan: %+v %v", files, err)
	}

	if err := s.Remove(rel); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(rel); err != nil {
		t.Fatalf("removing twice: %v", err)
	}
}

func TestManifestIsAtomicAndSkippedByScan(t *testing.T) {
	s := New(t.TempDir())
	if err := s.WriteManifest("../x", Manifest{}); err == nil {
		t.Fatal("unsafe session accepted")
	}
	m := Manifest{Format: "csv", Parts: []Part{{Stream: "main", Seq: 1}, {Stream: "main", Seq: 0}}}
	SortManifest(&m)
	if m.Parts[0].Seq != 0 {
		t.Fatalf("not sorted: %+v", m.Parts)
	}
	if err := s.WriteManifest("s1", m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(s.outputs(), "s1", manifestFile))
	if err != nil {
		t.Fatal(err)
	}
	var back Manifest
	if err := json.Unmarshal(data, &back); err != nil || back.Format != "csv" || len(back.Parts) != 2 {
		t.Fatalf("%+v %v", back, err)
	}
	files, err := s.Scan()
	if err != nil || len(files) != 0 {
		t.Fatalf("scan: %+v %v", files, err)
	}
}

func TestScanHandlesAMissingDirectory(t *testing.T) {
	files, err := New(filepath.Join(t.TempDir(), "nope")).Scan()
	if err != nil || len(files) != 0 {
		t.Fatalf("%+v %v", files, err)
	}
}

func part(unit string, seq int, rel string, first, end uint64, size int64) Part {
	return Part{UnitID: unit, Seq: seq, RelPath: rel, FirstIndex: first, EndIndex: end, Bytes: size}
}

func TestPlanReconcile(t *testing.T) {
	ok := part("u1", 0, "s/main/a", 0, 10, 100)
	ok2 := part("u1", 1, "s/main/b", 10, 20, 100)
	ok3 := part("u1", 2, "s/main/c", 20, 30, 100)
	other := part("u2", 0, "s/main/d", 30, 40, 100)
	file := func(rel string, size int64) FileInfo { return FileInfo{RelPath: rel, Size: size} }

	cases := []struct {
		name  string
		files []FileInfo
		parts []Part
		want  []Action
	}{
		{name: "everything matches", files: []FileInfo{file("s/main/a", 100)}, parts: []Part{ok}},
		{name: "nothing at all"},
		{
			name:  "promoted but never committed",
			files: []FileInfo{file("s/main/a", 100), file("s/main/orphan", 7)},
			parts: []Part{ok},
			want:  []Action{{Kind: ActionDeleteFile, RelPath: "s/main/orphan"}},
		},
		{
			name:  "committed part with no file",
			parts: []Part{ok},
			want: []Action{
				{Kind: ActionDropPart, UnitID: "u1", Seq: 0},
				{Kind: ActionRollbackUnit, UnitID: "u1", To: 0, NextSeq: 0},
			},
		},
		{
			name:  "truncated file",
			files: []FileInfo{file("s/main/a", 99)},
			parts: []Part{ok},
			want: []Action{
				{Kind: ActionDropPart, UnitID: "u1", Seq: 0},
				{Kind: ActionDeleteFile, RelPath: "s/main/a"},
				{Kind: ActionRollbackUnit, UnitID: "u1", To: 0, NextSeq: 0},
			},
		},
		{
			name:  "a damaged middle part takes later parts with it, not earlier ones",
			files: []FileInfo{file("s/main/a", 100), file("s/main/c", 100), file("s/main/d", 100)},
			parts: []Part{ok3, ok, ok2, other}, // out of order on purpose
			want: []Action{
				{Kind: ActionDropPart, UnitID: "u1", Seq: 1},
				{Kind: ActionDropPart, UnitID: "u1", Seq: 2},
				{Kind: ActionDeleteFile, RelPath: "s/main/c"},
				{Kind: ActionRollbackUnit, UnitID: "u1", To: 10, NextSeq: 1},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := PlanReconcile(c.files, c.parts)
			if !slices.Equal(got, c.want) {
				t.Fatalf("got  %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestApplyFilesRemovesFilesAndClearsStaging(t *testing.T) {
	s := New(t.TempDir())
	staged, err := s.StagingFile()
	if err != nil {
		t.Fatal(err)
	}
	staged.Close()
	if err := os.MkdirAll(filepath.Join(s.staging(), "local"), 0o750); err != nil {
		t.Fatal(err)
	}

	keep, _ := s.StagingFile()
	keep.Close()
	if err := s.Promote(keep.Name(), "s/main/keep"); err != nil {
		t.Fatal(err)
	}
	drop, _ := s.StagingFile()
	drop.Close()
	if err := s.Promote(drop.Name(), "s/main/drop"); err != nil {
		t.Fatal(err)
	}

	err = s.ApplyFiles([]Action{
		{Kind: ActionDeleteFile, RelPath: "s/main/drop"},
		{Kind: ActionDropPart, UnitID: "u", Seq: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	files, _ := s.Scan()
	if len(files) != 1 || files[0].RelPath != "s/main/keep" {
		t.Fatalf("files: %+v", files)
	}
	entries, err := os.ReadDir(s.staging())
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging: %v %v", entries, err)
	}
	if err := New(filepath.Join(t.TempDir(), "x")).ApplyFiles(nil); err != nil {
		t.Fatalf("no staging directory: %v", err)
	}
}

// storeSink commits parts into the store the way a Coordinator will.
type storeSink struct {
	s     *Store
	dir   string
	parts []Part
}

func (k *storeSink) Open(_ context.Context, r output.PartRequest) (output.PartFile, error) {
	return output.NewFilePart(filepath.Join(k.dir, r.Stream, partFileName(r.Seq, r.Extension)), output.CompressionNone)
}

func partFileName(seq int, ext string) string {
	return "p" + string(rune('a'+seq/26)) + string(rune('a'+seq%26)) + "." + ext
}

func (k *storeSink) Accept(_ context.Context, p output.FinishedPart) error {
	rel, err := PartPath("sess", "main", 0, p.Seq, 1, partExt(p.Path))
	if err != nil {
		return err
	}
	if err := k.s.Promote(p.Path, rel); err != nil {
		return err
	}
	k.parts = append(k.parts, Part{
		RelPath: rel, Seq: p.Seq, FirstIndex: p.FirstIndex, EndIndex: p.EndIndex, Records: p.Records,
		Bytes: p.Bytes, HeaderLen: p.HeaderLen, FooterLen: p.FooterLen,
	})
	return nil
}

func partExt(path string) string { return strings.TrimPrefix(filepath.Ext(path), ".") }

// generateParts runs the real engine and pipeline into a store.
func generateParts(t *testing.T, format string, maxRecords uint64) (*Store, []Part, serial.Encoder) {
	t.Helper()
	s, problems := schema.Decode([]byte("kind: hybrid\nfields: {id: {type: long, generator: sequence}, name: {type: string}}\n"), schema.FormatYAML)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	e, err := gen.Build(gen.Input{Schema: s, Seed: 9, ReferenceTime: time.Unix(0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	g, err := e.NewStreamGenerator(gen.StreamMain)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := serial.New(format, serial.Options{}, serial.Stream{Fields: e.Streams()[0].Fields})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store := New(dir)
	sink := &storeSink{s: store, dir: filepath.Join(dir, "scratch")}
	_, err = pipeline.Run(context.Background(), pipeline.Unit{
		Generators: []gen.StreamGenerator{g}, Stream: gen.StreamMain, End: 1000, BatchSize: 64, QueueCap: 2,
		Encoder: enc, Sink: sink, Limits: pipeline.PartLimits{MaxRecords: maxRecords},
	})
	if err != nil {
		t.Fatal(err)
	}
	if maxRecords > 0 && len(sink.parts) < 3 {
		t.Fatalf("%d parts", len(sink.parts))
	}
	return store, sink.parts, enc
}

func mergedBytes(t *testing.T, s *Store, parts []Part, enc serial.Encoder) []byte {
	t.Helper()
	m, err := s.Merged(parts, Layout{Separator: enc.Separator(), Empty: serial.EmptyDocument(enc)})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	data, err := io.ReadAll(m)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != m.Size() {
		t.Fatalf("read %d bytes, Size says %d", len(data), m.Size())
	}
	return data
}

// A merged download must equal having generated one unrotated document.
func TestMergedEqualsUnrotatedDocument(t *testing.T) {
	for _, format := range []string{serial.FormatJSON, serial.FormatNDJSON, serial.FormatXML, serial.FormatCSV} {
		t.Run(format, func(t *testing.T) {
			store, parts, enc := generateParts(t, format, 170)
			wholeStore, wholeParts, _ := generateParts(t, format, 0)
			if len(wholeParts) != 1 {
				t.Fatalf("%d parts", len(wholeParts))
			}
			f, err := wholeStore.Open(wholeParts[0].RelPath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			want, err := io.ReadAll(f)
			if err != nil {
				t.Fatal(err)
			}
			if got := mergedBytes(t, store, parts, enc); !bytes.Equal(got, want) {
				t.Fatalf("merged differs from a single document:\n%.300s\n---\n%.300s", got, want)
			}
		})
	}
}

func TestMergedDocumentsParse(t *testing.T) {
	store, parts, enc := generateParts(t, serial.FormatJSON, 170)
	var arr []map[string]any
	if err := json.Unmarshal(mergedBytes(t, store, parts, enc), &arr); err != nil || len(arr) != 1000 {
		t.Fatalf("JSON: %d %v", len(arr), err)
	}

	store, parts, enc = generateParts(t, serial.FormatXML, 170)
	var root struct {
		Records []struct {
			ID int `xml:"id"`
		} `xml:"record"`
	}
	if err := xml.Unmarshal(mergedBytes(t, store, parts, enc), &root); err != nil || len(root.Records) != 1000 {
		t.Fatalf("XML: %d %v", len(root.Records), err)
	}

	store, parts, enc = generateParts(t, serial.FormatCSV, 170)
	rows, err := csv.NewReader(bytes.NewReader(mergedBytes(t, store, parts, enc))).ReadAll()
	if err != nil || len(rows) != 1001 {
		t.Fatalf("CSV: %d %v", len(rows), err)
	}

	store, parts, enc = generateParts(t, serial.FormatNDJSON, 170)
	n := 0
	scan := bufio.NewScanner(bytes.NewReader(mergedBytes(t, store, parts, enc)))
	for scan.Scan() {
		n++
	}
	if n != 1000 {
		t.Fatalf("NDJSON: %d lines", n)
	}
}

func TestEachPartIsItselfValid(t *testing.T) {
	store, parts, _ := generateParts(t, serial.FormatJSON, 170)
	for _, p := range parts {
		f, err := store.Open(p.RelPath)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(f)
		f.Close()
		var arr []map[string]any
		if err := json.Unmarshal(data, &arr); err != nil || uint64(len(arr)) != p.Records {
			t.Fatalf("part %d: %d records, %v", p.Seq, len(arr), err)
		}
	}
}

func TestMergedSupportsSeeking(t *testing.T) {
	store, parts, enc := generateParts(t, serial.FormatJSON, 170)
	want := mergedBytes(t, store, parts, enc)

	m, err := store.Merged(parts, Layout{Separator: enc.Separator()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	for _, offset := range []int64{0, 1, 100, m.Size() / 2, m.Size() - 5, m.Size() - 1} {
		if _, err := m.Seek(offset, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 64)
		n, err := io.ReadFull(m, buf)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], want[offset:offset+int64(n)]) {
			t.Fatalf("offset %d: wrong bytes", offset)
		}
	}
	pos, err := m.Seek(-3, io.SeekEnd)
	if err != nil || pos != m.Size()-3 {
		t.Fatalf("%d %v", pos, err)
	}
	pos, err = m.Seek(1, io.SeekCurrent)
	if err != nil || pos != m.Size()-2 {
		t.Fatalf("%d %v", pos, err)
	}
	if _, err := m.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative position accepted")
	}
	if _, err := m.Seek(0, 99); err == nil {
		t.Fatal("bad whence accepted")
	}
	if _, err := m.Seek(m.Size()+10, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := m.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("past the end: %d %v", n, err)
	}
}

func TestMergedWithNoPartsIsTheEmptyDocument(t *testing.T) {
	s := New(t.TempDir())
	m, err := s.Merged(nil, Layout{Empty: []byte("[\n\n]\n")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(m)
	if err != nil || string(data) != "[\n\n]\n" {
		t.Fatalf("%q %v", data, err)
	}
}

func TestMergedRefusesWhatItCannotSplice(t *testing.T) {
	s := New(t.TempDir())
	if _, err := s.Merged([]Part{{RelPath: "a.csv.gz", Bytes: 10}}, Layout{}); err == nil {
		t.Fatal("compressed part accepted")
	}
	if _, err := s.Merged([]Part{{RelPath: "a", Bytes: 1, HeaderLen: 5}}, Layout{}); err == nil {
		t.Fatal("impossible lengths accepted")
	}

	m, err := s.Merged([]Part{{RelPath: "missing", Bytes: 4}}, Layout{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(m); err == nil {
		t.Fatal("reading a missing part succeeded")
	}
}

func TestEncodeSHA(t *testing.T) {
	var sum [32]byte
	sum[0] = 0xab
	if got := EncodeSHA(sum); !strings.HasPrefix(got, "ab00") || len(got) != 64 {
		t.Fatalf("%q", got)
	}
}
