package datadir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesLayout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "data")
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(d.Root()) {
		t.Fatalf("root %q is not absolute", d.Root())
	}
	for _, sub := range []string{dirSessions, dirOutputs, dirStaging, filepath.Join(dirStaging, "local")} {
		if st, err := os.Stat(filepath.Join(root, sub)); err != nil || !st.IsDir() {
			t.Errorf("%s missing: %v", sub, err)
		}
	}
}

func TestSecondLockIsRefusedUntilReleased(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	first, err := d.Lock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Lock(); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v, want ErrLocked", err)
	}

	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("second release: %v", err)
	}

	again, err := d.Lock()
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	if err := again.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestFreeBytesReadsTheFilesystem(t *testing.T) {
	free, err := FreeBytes(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if free == 0 {
		t.Fatal("free space is zero")
	}
	if _, err := FreeBytes(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing path accepted")
	}
}

func TestLocalTempDirIsNotNetworkStorage(t *testing.T) {
	network, err := IsNetworkFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if network {
		t.Fatal("temp dir reported as network storage")
	}
}
