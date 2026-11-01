package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writePart(t *testing.T, root, rel, body string) Part {
	t.Helper()
	path := filepath.Join(root, dirOutputs, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return Part{Stream: "main", RelPath: rel, Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
}

func TestVerifyFindsEveryKindOfDamage(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	good := writePart(t, root, "ses/main/u000000-p0000.f1.ndjson", "good\n")
	gone := writePart(t, root, "ses/main/u000000-p0001.f1.ndjson", "gone\n")
	short := writePart(t, root, "ses/main/u000000-p0002.f1.ndjson", "whole\n")
	flipped := writePart(t, root, "ses/main/u000000-p0003.f1.ndjson", "right\n")

	output := func(p Part) string { return filepath.Join(root, dirOutputs, filepath.FromSlash(p.RelPath)) }
	if err := os.Remove(output(gone)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output(short), []byte("wh"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output(flipped), []byte("wrong\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := s.Verify(context.Background(), []Part{good, gone, short, flipped})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{gone.RelPath: VerifyMissing, short.RelPath: VerifySize, flipped.RelPath: VerifyChecksum}
	if len(got) != len(want) {
		t.Fatalf("%+v", got)
	}
	for _, p := range got {
		if want[p.RelPath] != p.Reason {
			t.Errorf("%s: %s", p.RelPath, p.Reason)
		}
	}
}

func TestVerifyCleanAndCancelled(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	good := writePart(t, root, "ses/main/u000000-p0000.f1.ndjson", "good\n")
	got, err := s.Verify(context.Background(), []Part{good})
	if err != nil || len(got) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Verify(ctx, []Part{good}); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
}
