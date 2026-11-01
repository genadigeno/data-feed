//go:build linux

package datadir

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Filesystem magic numbers from statfs(2) that mean network storage.
var networkMagic = map[int64]bool{
	unix.NFS_SUPER_MAGIC:  true,
	unix.SMB_SUPER_MAGIC:  true,
	unix.SMB2_SUPER_MAGIC: true,
	unix.CIFS_SUPER_MAGIC: true,
	unix.AFS_SUPER_MAGIC:  true,
	unix.CODA_SUPER_MAGIC: true,
	unix.V9FS_MAGIC:       true,
}

// IsNetworkFS reports whether path is on a network filesystem.
func IsNetworkFS(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, fmt.Errorf("inspect filesystem: %w", err)
	}
	return networkMagic[int64(st.Type)], nil
}
