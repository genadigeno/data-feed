//go:build !windows

package datadir

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// FreeBytes returns the bytes available to this process on path's filesystem.
func FreeBytes(path string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("read free disk space: %w", err)
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
