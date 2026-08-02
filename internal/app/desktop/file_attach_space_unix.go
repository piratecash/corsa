//go:build unix

package desktop

import "golang.org/x/sys/unix"

// attachDirFreeBytes reports the free space available to the app on the
// filesystem holding dir. The unix build constraint covers android —
// the platform where the staging budget actually matters (SAF streams
// land on app-private internal storage there).
func attachDirFreeBytes(dir string) (uint64, bool) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return uint64(st.Bavail) * uint64(st.Bsize), true
}
