// Package buildinfo holds values stamped into the binary at link time:
//
//	go build -ldflags "-X github.com/genadigeno/data-feed/internal/buildinfo.Version=1.2.3"
package buildinfo

import "runtime"

var (
	Version = "dev"
	Commit  = "unknown"
)

// GoVersion returns the toolchain that built the binary.
func GoVersion() string { return runtime.Version() }
