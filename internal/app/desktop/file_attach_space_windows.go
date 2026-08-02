//go:build windows

package desktop

// attachDirFreeBytes: no free-space probe wired up on Windows — the
// staging path only runs for pathless picker streams (Android/iOS), so
// desktop builds just skip the budget guard.
func attachDirFreeBytes(string) (uint64, bool) {
	return 0, false
}
