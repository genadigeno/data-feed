//go:build !linux && !windows

package sysinfo

import "golang.org/x/sys/unix"

func memoryBytes() int64 {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return int64(n)
}
