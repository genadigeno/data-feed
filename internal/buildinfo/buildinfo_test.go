package buildinfo

import (
	"runtime/debug"
	"testing"
)

func TestFromBuildInfo(t *testing.T) {
	info := func(version, revision string) *debug.BuildInfo {
		b := &debug.BuildInfo{Main: debug.Module{Version: version}}
		if revision != "" {
			b.Settings = []debug.BuildSetting{{Key: "vcs.revision", Value: revision}}
		}
		return b
	}
	cases := []struct {
		name                    string
		in                      *debug.BuildInfo
		version, commit         string
		wantVersion, wantCommit string
	}{
		{"go install of a tag", info("v1.0.0", ""), "dev", "unknown", "1.0.0", "unknown"},
		{"a local build", info("(devel)", "0123456789abcdef"), "dev", "unknown", "dev", "0123456"},
		{"nothing recorded", info("", ""), "dev", "unknown", "dev", "unknown"},
		{"stamped values win", info("v9.9.9", "fedcba9876543210"), "1.0.0", "abc1234", "1.0.0", "abc1234"},
	}
	for _, c := range cases {
		v, commit := fromBuildInfo(c.in, c.version, c.commit)
		if v != c.wantVersion || commit != c.wantCommit {
			t.Errorf("%s: got %s, %s", c.name, v, commit)
		}
	}
}
