//go:build !windows

package secretfile

import "os"

// create_posix.go is the POSIX half of the two platform primitives. Both are
// one line of intent here, because the mode bits mean what they say: a file
// created 0600 has owner-only mode bits from the instant it exists, with no
// separate permission step and therefore no window between creation and
// protection.
//
// Mode bits are not the whole answer on macOS: inheritable entries of the
// directory's extended ACL are applied to the new file regardless of the
// mode, so there the file is owner-only only if the directory carries no such
// entries. That is a known, unfixed limit of this writer (see the package
// comment).
//
// The Windows half needs the NT API to get the same guarantee — see
// create_windows.go.

// createExclusive creates name inside dir, owner-only from the first instant,
// failing if anything already exists under that name.
//
// O_EXCL is the exclusive create: a planted file is a refusal, not a target.
// The call resolves name through the pinned directory handle, so nothing
// outside dir can be reached and the directory cannot be swapped underneath.
func createExclusive(dir *Dir, name string) (*os.File, error) {
	return dir.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

// restrictDirectory makes the directory owner-only through the pinned handle.
func restrictDirectory(dir *Dir) error {
	return dir.root.Chmod(".", 0o700)
}
