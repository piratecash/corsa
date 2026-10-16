//go:build windows

package secretfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// audit_windows.go is the Windows half of AuditLocation. On NTFS the mode
// bits mean nothing, so "nobody else can change this location" is a
// statement about owners and DACLs, read back from the objects themselves and
// judged by judgeObjectSecurity (audit_acl.go, where the rule is written
// down): the file, then the directory holding it, then every directory above
// it up to the root of its volume.
//
// A reparse point anywhere on the way that redirects its name — a symlink or
// a junction, on the file or on a directory — is a violation by itself: it
// decides where the secret is read from and written to, whatever the DACLs
// on either side say. A reparse point of an unknown kind is refused too;
// cloud placeholders, deduplicated and WOF-compressed entries only store
// their own data differently and are judged like any other entry
// (judgeReparseTag). The walk is lexical: there is no link to resolve on a
// path the audit accepts.
//
// Like the POSIX half, the audit names components by role, never by path,
// and covers what exists: before the first write there is no file, and
// directories not created yet are skipped; the ones that do exist are judged
// all the same, so an unsafe location is refused before the store is ever
// created.

func auditLocation(abs string) error {
	user, err := processUserSID()
	if err != nil {
		return err
	}
	if err := auditFile(abs, user); err != nil {
		return err
	}
	return auditDirectories(abs, user)
}

func processUserSID() (string, error) {
	token, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("secretfile: audit: resolve current user: %w", err)
	}
	return token.User.Sid.String(), nil
}

// auditFile judges the file itself, if there is one. Its kind is read from
// the attributes rather than from os.FileMode: Go reports a cloud
// placeholder as irregular, while for the audit it is the file itself.
func auditFile(abs, user string) error {
	info, err := os.Lstat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("secretfile: audit the file: %w", stripPath(err))
	}
	attributes := fileAttributes(info)
	if err := judgeReparse(abs, attributes, fileComponent()); err != nil {
		return err
	}
	if attributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 || attributes&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		return &UnsafeLocationError{Component: fileComponent(), Violation: ViolationNotRegularFile}
	}
	return judgeObjectAt(abs, fileComponent(), aclRoleFile, user)
}

// auditDirectories judges every existing directory from the one holding the
// file up to the volume root, nearest first.
func auditDirectories(abs, user string) error {
	levels := 0
	for dir := filepath.Dir(abs); ; dir = filepath.Dir(dir) {
		levels++
		root := filepath.Dir(dir) == dir
		component := LocationComponent{Kind: ComponentDirectory, LevelsAbove: levels}
		if err := auditDirectory(dir, component, directoryRole(levels, root), user); err != nil {
			return err
		}
		if root {
			return nil
		}
	}
}

// directoryRole: the directory holding the file is held to the strictest
// rule, the volume root to the loosest (it cannot be deleted or renamed).
func directoryRole(levels int, root bool) aclRole {
	switch {
	case levels == 1:
		return aclRoleHoldingDirectory
	case root:
		return aclRoleVolumeRoot
	default:
		return aclRoleAncestor
	}
}

// auditDirectory judges one directory on the way; a directory not created
// yet has nothing to judge.
func auditDirectory(dir string, component LocationComponent, role aclRole, user string) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("secretfile: audit a directory on the way to the file: %w", stripPath(err))
	}
	attributes := fileAttributes(info)
	if err := judgeReparse(dir, attributes, LocationComponent{Kind: ComponentLinkRoute}); err != nil {
		return err
	}
	if attributes&syscall.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return fmt.Errorf("%w: a component on the way to the file is not a directory", ErrNotADirectory)
	}
	return judgeObjectAt(dir, component, role, user)
}

// fileAttributes are the attributes Lstat read without following the entry.
func fileAttributes(info fs.FileInfo) uint32 {
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return 0
	}
	return data.FileAttributes
}

// judgeReparse judges a reparse point by its tag (judgeReparseTag): one that
// redirects the name, or one of an unknown kind, is a violation at
// component; anything else is judged like any other entry.
func judgeReparse(path string, attributes uint32, component LocationComponent) error {
	if attributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return nil
	}
	tag, err := reparseTag(path)
	if err != nil {
		return err
	}
	if violation, found := judgeReparseTag(tag); found {
		return &UnsafeLocationError{Component: component, Violation: violation}
	}
	return nil
}

// reparseTag reads the tag of the reparse point at path. FindFirstFile
// reports it, for the entry itself, in the reserved field.
func reparseTag(path string) (uint32, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("secretfile: audit: encode a name: %w", err)
	}
	var data windows.Win32finddata
	handle, err := windows.FindFirstFile(name, &data)
	if err != nil {
		return 0, fmt.Errorf("secretfile: audit: read a reparse tag: %w", stripPath(err))
	}
	_ = windows.FindClose(handle)
	return data.Reserved0, nil
}

// judgeObjectAt reads the owner and DACL of the object at path and judges
// them for its role.
func judgeObjectAt(path string, component LocationComponent, role aclRole, user string) error {
	security, err := readObjectSecurity(path)
	if err != nil {
		return err
	}
	violation, found := judgeObjectSecurity(security, role, user)
	if !found {
		return nil
	}
	return &UnsafeLocationError{Component: component, Violation: violation, Owner: security.owner}
}

// readObjectSecurity reads an object's owner and DACL into the judgement's
// terms.
func readObjectSecurity(path string) (objectSecurity, error) {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return objectSecurity{}, fmt.Errorf("secretfile: audit: read a security descriptor: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return objectSecurity{}, fmt.Errorf("secretfile: audit: read an owner: %w", err)
	}
	security := objectSecurity{owner: owner.String()}
	dacl, _, err := descriptor.DACL()
	switch {
	case errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND), err == nil && dacl == nil:
		// No DACL, or a NULL one: everyone may do everything.
		security.nullDACL = true
		return security, nil
	case err != nil:
		return objectSecurity{}, fmt.Errorf("secretfile: audit: read a dacl: %w", err)
	}
	entries, err := daclEntries(dacl)
	if err != nil {
		return objectSecurity{}, err
	}
	security.dacl = entries
	return security, nil
}

// daclEntries reads every entry of dacl. Allow and deny entries carry their
// trustee and mask; any other type is read as unknown, which the judgement
// does not trust.
func daclEntries(dacl *windows.ACL) ([]aclEntry, error) {
	entries := make([]aclEntry, 0, dacl.AceCount)
	for index := range uint32(dacl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			return nil, fmt.Errorf("secretfile: audit: read an ace: %w", err)
		}
		entries = append(entries, entryOf(ace))
	}
	return entries, nil
}

var aceKinds = map[byte]aclEntryKind{
	windows.ACCESS_ALLOWED_ACE_TYPE: aclAllow,
	windows.ACCESS_DENIED_ACE_TYPE:  aclDeny,
}

func entryOf(ace *windows.ACCESS_ALLOWED_ACE) aclEntry {
	kind, known := aceKinds[ace.Header.AceType]
	if !known {
		return aclEntry{kind: aclUnknown}
	}
	sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	return aclEntry{
		kind:        kind,
		trustee:     sid.String(),
		mask:        uint32(ace.Mask),
		inheritOnly: ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0,
	}
}

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
