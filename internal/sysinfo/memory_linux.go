//go:build linux

package sysinfo

import (
	"os"

	"golang.org/x/sys/unix"
)

func memoryBytes() int64 {
	var physical int64
	var si unix.Sysinfo_t
	if err := unix.Sysinfo(&si); err == nil {
		physical = int64(si.Totalram) * int64(si.Unit)
	}
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		content, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit, limited := parseCgroupLimit(string(content))
		return effectiveMemory(physical, limit, limited)
	}
	return physical
}
