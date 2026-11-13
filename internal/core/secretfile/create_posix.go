//go:build !windows

package secretfile

import "os"

// create_posix.go is the POSIX half of the two platform primitives. Both are
// one line of intent here, because the mode bits mean what they say: a file
// created 0600 has owner-only mode bits from the instant it exists, with no
// separate permission step and therefore no window between creation and
// protection.
//
// On macOS check the directory's ACL and the ACL actually inherited by the
// new file before any caller writes secret bytes.
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
	if err := auditCreationDirectoryACL(dir); err != nil {
		return nil, err
	}
	file, err := dir.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	err = auditNewFileACL(file)
	if err == nil {
		return file, nil
	}
	info, infoErr := file.Stat()
	_ = file.Close()
	if current, statErr := dir.root.Lstat(name); statErr == nil && infoErr == nil && os.SameFile(info, current) {
		_ = dir.root.Remove(name)
	}
	return nil, err
}

// restrictDirectory makes the directory owner-only through the pinned handle.
func restrictDirectory(dir *Dir) error {
	return dir.root.Chmod(".", 0o700)
}
