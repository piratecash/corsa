//go:build windows

package secretfile

// Windows namespace confirmation requires a separately verified backend.
func (d *Dir) SyncAncestors(string) error { return ErrDurabilityUnsupported }
