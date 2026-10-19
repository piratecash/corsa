//go:build darwin || linux

package loadstand

import (
	"os"
	"syscall"
)

func auditPrivateFinalFD(file *os.File, directory bool) error {
	if file == nil {
		return ErrUnsupportedArtifact
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	want := os.FileMode(0600)
	if directory {
		want = 0700
	}
	if info.Mode().Perm() != want || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return ErrUnsupportedArtifact
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(owner.Uid) != uint64(os.Geteuid()) {
		return ErrUnsupportedArtifact
	}
	return auditFinalACL(file)
}
