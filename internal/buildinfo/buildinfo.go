// Package buildinfo holds values stamped into the binary at link time:
//
//	go build -ldflags "-X github.com/genadigeno/data-feed/internal/buildinfo.Version=1.2.3"
//
// A binary built without them, as by go install, reports what the Go toolchain recorded.
package buildinfo

import (
	"runtime"
	"runtime/debug"
	"strings"
)

var (
	Version = "dev"
	Commit  = "unknown"
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		Version, Commit = fromBuildInfo(info, Version, Commit)
	}
}

// fromBuildInfo fills in what was not stamped at link time.
func fromBuildInfo(info *debug.BuildInfo, version, commit string) (string, string) {
	if v := info.Main.Version; version == "dev" && v != "" && v != "(devel)" {
		version = strings.TrimPrefix(v, "v")
	}
	if commit == "unknown" {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 7 {
				commit = s.Value[:7]
			}
		}
	}
	return version, commit
}

// GoVersion returns the toolchain that built the binary.
func GoVersion() string { return runtime.Version() }
