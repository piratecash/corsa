//go:build windows

package secretfile

import (
	"fmt"
	"io/fs"
	"unsafe"

	"golang.org/x/sys/windows"
)

// audit_windows.go is the Windows half of AuditLocation. On NTFS the mode
// bits mean nothing, so "owner-only" is a statement about the file's DACL,
// read back from the file: every ACE that grants anything must grant it to
// the current user, and to nobody else — the shape createExclusive gives a
// file at birth.
//
// The ancestor directories are NOT judged here. Their POSIX rule ("nobody
// but this user or root may write") has no faithful DACL translation yet:
// the system drive's root grants Authenticated Users the right to create
// folders, much as /tmp lets everyone create names, and a rule that ignored
// that would warn on every Windows node. Until that rule is written down,
// the Windows audit covers the file, as the store's contract asks.
//
// The file's OWNER is not checked either: the owner of an object implicitly
// holds WRITE_DAC and can rewrite its DACL, so a file owned by another
// principal is not owner-only in substance even when its DACL reads so. The
// writer creates files owned by the current user; a file someone else
// planted with a user-only DACL passes this audit.

func auditLocation(abs string) error {
	_, present, err := inspectFile(abs)
	if err != nil || !present {
		return err
	}
	descriptor, err := windows.GetNamedSecurityInfo(abs, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("secretfile: audit: read the file's security descriptor: %w", err)
	}
	ownerOnly, err := grantsOnlyCurrentUser(descriptor)
	if err != nil {
		return err
	}
	if !ownerOnly {
		return &UnsafeLocationError{Component: fileComponent(), Violation: ViolationFileNotOwnerOnly}
	}
	return nil
}

// grantsOnlyCurrentUser reports whether every effective allow ACE of the
// descriptor's DACL names the current user. A NULL DACL grants everyone
// everything; a deny ACE only takes access away; an inherit-only ACE does
// not apply to the file itself. Any other ACE type (object, callback,
// audit-like) is not a shape this package writes, so it is not owner-only.
func grantsOnlyCurrentUser(descriptor *windows.SECURITY_DESCRIPTOR) (bool, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return false, fmt.Errorf("secretfile: audit: resolve current user: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return false, fmt.Errorf("secretfile: audit: read the file's dacl: %w", err)
	}
	if dacl == nil {
		return false, nil
	}
	for index := range uint32(dacl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			return false, fmt.Errorf("secretfile: audit: read an ace: %w", err)
		}
		if ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
			continue
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if !sid.Equals(user.User.Sid) {
				return false, nil
			}
		default:
			return false, nil
		}
	}
	return true, nil
}

// ownerOf is empty on Windows: the audit judges the DACL, and fs.FileInfo
// carries no owner here.
func ownerOf(fs.FileInfo) string { return "" }

// describeDirectory names the pinned directory's owner for
// ErrDirectoryNotWritable, read through the handle rather than the name.
// The mode is left out: on NTFS it says nothing about who may write.
func describeDirectory(dir *Dir) string {
	directory, err := dir.root.Open(".")
	if err != nil {
		return "owner unreadable"
	}
	defer func() { _ = directory.Close() }()
	descriptor, err := windows.GetSecurityInfo(windows.Handle(directory.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return "owner unreadable"
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return "owner unreadable"
	}
	return fmt.Sprintf("owner %s, mode not used on this platform", owner.String())
}
