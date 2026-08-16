//go:build !windows

package node

// restrictSecretFileAccess is a no-op on POSIX: writeSecretFile creates
// the temp file 0600 with O_EXCL, which IS the owner-only guarantee here.
// The Windows variant applies a DACL because mode bits protect nothing on
// NTFS.
func restrictSecretFileAccess(string) error {
	return nil
}
