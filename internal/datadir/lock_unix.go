//go:build !windows

package datadir

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLock(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return ErrLocked
	}
	if err != nil {
		return fmt.Errorf("lock data directory: %w", err)
	}
	return nil
}

func unlock(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_UN) }
