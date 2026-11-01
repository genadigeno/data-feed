//go:build windows

package datadir

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// IsNetworkFS reports whether path is on a network drive.
func IsNetworkFS(path string) (bool, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	root, err := windows.UTF16PtrFromString(filepath.VolumeName(abs) + `\`)
	if err != nil {
		return false, err
	}
	return windows.GetDriveType(root) == windows.DRIVE_REMOTE, nil
}

// FreeBytes returns the bytes available to this process on path's filesystem.
func FreeBytes(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &free, nil, nil); err != nil {
		return 0, fmt.Errorf("read free disk space: %w", err)
	}
	return free, nil
}
