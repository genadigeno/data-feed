// Package datadir manages the Coordinator's data directory: its layout, the
// exclusive lock that keeps a second Coordinator out, and host filesystem checks.
package datadir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Subdirectories of the data directory.
const (
	lockFile    = "datagen.lock"
	dirSessions = "sessions"
	dirOutputs  = "outputs"
	dirStaging  = "staging"
	dirTLS      = "tls"
)

// ErrLocked means another process owns the data directory.
var ErrLocked = errors.New("data directory is locked by another Coordinator")

// Dir is an initialised data directory.
type Dir struct{ root string }

// Open creates the directory layout. It does not take the lock.
func Open(root string) (*Dir, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	d := &Dir{root: abs}
	for _, sub := range []string{dirSessions, dirOutputs, filepath.Join(dirStaging, "local")} {
		if err := os.MkdirAll(filepath.Join(abs, sub), 0o750); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}
	return d, nil
}

// Root returns the absolute path of the data directory.
func (d *Dir) Root() string { return d.root }

// TLSDir returns where the Coordinator keeps its generated certificate.
func (d *Dir) TLSDir() string { return filepath.Join(d.root, dirTLS) }

// Lock takes the exclusive Coordinator lock. The OS releases it when the process
// exits, however it exits, so a crash never leaves a stale lock behind.
func (d *Dir) Lock() (*Lock, error) {
	f, err := os.OpenFile(filepath.Join(d.root, lockFile), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := tryLock(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Lock is a held data directory lock.
type Lock struct{ f *os.File }

// Release gives the lock up. Calling it again is a no-op.
func (l *Lock) Release() error {
	if l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	return errors.Join(unlock(f), f.Close())
}
