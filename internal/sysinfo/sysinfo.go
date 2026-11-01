// Package sysinfo reads host properties used to resolve automatic settings.
package sysinfo

import (
	"runtime"
	"strconv"
	"strings"
)

// NumCPU returns the usable CPU count (GOMAXPROCS is cgroup-aware on Linux).
func NumCPU() int { return runtime.GOMAXPROCS(0) }

// MemoryBytes returns usable memory: the cgroup limit if one applies, otherwise the
// physical total, or 0 if it cannot be determined.
func MemoryBytes() int64 { return memoryBytes() }

// parseCgroupLimit reads a cgroup memory limit file. "max" and absurdly large values
// mean no limit, which cgroup v1 reports as a huge number near 2^63.
func parseCgroupLimit(content string) (int64, bool) {
	s := strings.TrimSpace(content)
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<62 {
		return 0, false
	}
	return n, true
}

// effectiveMemory applies a cgroup limit to the physical total.
func effectiveMemory(physical, limit int64, limited bool) int64 {
	if limited && (physical == 0 || limit < physical) {
		return limit
	}
	return physical
}
