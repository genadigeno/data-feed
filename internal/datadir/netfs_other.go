//go:build !linux && !windows

package datadir

// IsNetworkFS cannot detect network storage on this platform; the rule that the
// data directory must be local is then documentation only.
func IsNetworkFS(string) (bool, error) { return false, nil }
