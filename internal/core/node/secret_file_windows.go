//go:build windows

package node

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// restrictSecretFileAccess replaces the file's DACL with an owner-only
// full-access grant, protected from inheritance: on NTFS the POSIX 0600
// mode bits protect nothing — permissions come from the directory's
// inherited ACL, which typically lets other local accounts read the file.
// Called on the fresh temp file BEFORE any secret byte is written.
func restrictSecretFileAccess(path string) error {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("resolve current user: %w", err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		fmt.Sprintf("D:P(A;;FA;;;%s)", user.User.Sid.String()))
	if err != nil {
		return fmt.Errorf("build owner-only descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read owner-only dacl: %w", err)
	}
	err = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
	if err != nil {
		return fmt.Errorf("apply owner-only dacl: %w", err)
	}
	return nil
}
