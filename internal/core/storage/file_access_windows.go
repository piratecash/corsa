//go:build windows

package storage

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// restrictFileAccess replaces the file's DACL with an owner-only full-access
// grant, protected from inheritance.
//
// On NTFS the POSIX mode bits protect nothing: Go maps them to the read-only
// attribute and the actual permissions come from the directory's inherited
// ACL, which typically lets other local accounts read the file. Windows is in
// the release matrix, and internal/core/node does the same for identity
// secrets — the state database needs it for the same reason: the message
// bodies are encrypted, the rows are not.
func restrictFileAccess(path string) error {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("storage: resolve current user for %s: %w", path, err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		fmt.Sprintf("D:P(A;;FA;;;%s)", user.User.Sid.String()))
	if err != nil {
		return fmt.Errorf("storage: build owner-only descriptor for %s: %w", path, err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("storage: read owner-only dacl for %s: %w", path, err)
	}
	err = windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
	if err != nil {
		return fmt.Errorf("storage: apply owner-only dacl to %s: %w", path, err)
	}
	return nil
}
