//go:build linux

package loadstand

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func auditFinalACL(file *os.File) error {
	// POSIX access ACL grants to named users/groups are limited by the mode's
	// group-class mask, checked zero above. Unknown ACL/filesystem errors refuse.
	_, err := unix.Fgetxattr(int(file.Fd()), "system.posix_acl_access", nil)
	if err == nil || errors.Is(err, unix.ENODATA) {
		return nil
	}
	return ErrUnsupportedArtifact
}
