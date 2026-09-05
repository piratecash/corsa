//go:build windows

package secretfile

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

// create_windows.go is the whole Windows story for this package, kept in one
// file on purpose: it is the only code in the tree that talks to the NT API,
// and it is the only code whose correctness cannot be established by the test
// suite on a POSIX machine.
//
// Two properties are needed, and neither can be had from the os package:
//
//   - The file must be born owner-only. On NTFS the mode passed to a create
//     means nothing; permissions come from the parent's inheritable ACEs. If
//     the ACL is applied AFTER the create, there is a window in which the file
//     exists under the parent's terms — and a handle another process obtained
//     during that window keeps its access, because changing a DACL does not
//     revoke access already granted. An unpredictable name does not close
//     that: file creation in a directory is an observable event.
//     https://devblogs.microsoft.com/oldnewthing/20200320-00/?p=103579
//   - The create must be relative to the PINNED directory handle. Naming the
//     object by path reopens the whole "which directory is this now" question
//     that os.Root exists to answer, and it splits the write from the
//     protection: the ACL could land on one object and the bytes on another.
//
// NtCreateFile is the call that has both: OBJECT_ATTRIBUTES carries a
// RootDirectory handle AND a SecurityDescriptor, applied atomically at
// creation. The shape below mirrors the standard library's own os.Root
// implementation (internal/syscall/windows/at_windows.go), which does the same
// handle-relative create without the descriptor.
//
// Note for the reviewer of the next change here: an earlier attempt applied
// the descriptor to the already-open handle with SetSecurityInfo. That fails
// with access denied, because changing a DACL needs WRITE_DAC on the handle
// and a *os.File opened for writing does not carry it — which broke
// identity.Save, and with it every application start on Windows. Any handle
// used for a DACL change must be opened with WRITE_DAC explicitly, as
// restrictDirectory does below.

// ownerOnlyDescriptor builds a protected DACL granting full access to the
// current user and to nobody else. "Protected" (the P flag) is what stops the
// parent directory's inheritable ACEs from being merged back in.
func ownerOnlyDescriptor() (*windows.SECURITY_DESCRIPTOR, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("resolve current user: %w", err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		fmt.Sprintf("D:P(A;;FA;;;%s)", user.User.Sid.String()))
	if err != nil {
		return nil, fmt.Errorf("build owner-only descriptor: %w", err)
	}
	return descriptor, nil
}

// createExclusive creates name inside dir, owner-only from the first instant,
// failing if anything already exists under that name.
func createExclusive(dir *Dir, name string) (*os.File, error) {
	descriptor, err := ownerOnlyDescriptor()
	if err != nil {
		return nil, err
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, fmt.Errorf("encode temp name: %w", err)
	}

	// The directory handle is borrowed for the duration of the call only.
	directory, err := dir.root.Open(".")
	if err != nil {
		return nil, stripPath(err)
	}
	defer func() { _ = directory.Close() }()

	attributes := &windows.OBJECT_ATTRIBUTES{
		RootDirectory:      windows.Handle(directory.Fd()),
		ObjectName:         objectName,
		Attributes:         windows.OBJ_CASE_INSENSITIVE,
		SecurityDescriptor: descriptor,
	}
	attributes.Length = uint32(unsafe.Sizeof(*attributes))

	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle,
		windows.SYNCHRONIZE|windows.FILE_GENERIC_WRITE,
		attributes,
		&status,
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		// Same sharing the standard library uses for ordinary files. The
		// protection here is the descriptor, not the share mode; denying
		// sharing outright is the kind of difference that makes backup and
		// anti-malware software fail in ways nobody connects to this file.
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		// FILE_CREATE is the exclusive create: it fails if the name exists,
		// so a planted file is a refusal rather than a target.
		windows.FILE_CREATE,
		windows.FILE_SYNCHRONOUS_IO_NONALERT|
			windows.FILE_NON_DIRECTORY_FILE|
			// Refuse a reparse point at the name rather than following it.
			// Redundant under FILE_CREATE, kept because the cost is a
			// constant and the cost of being wrong is both private keys.
			windows.FILE_OPEN_REPARSE_POINT,
		0,
		0,
	)
	// The descriptor and the name buffer must outlive the call; the handle
	// borrowed above must too.
	runtime.KeepAlive(descriptor)
	runtime.KeepAlive(objectName)
	runtime.KeepAlive(directory)
	if err != nil {
		return nil, ntStatusError(err)
	}
	return os.NewFile(uintptr(handle), name), nil
}

// ntStatusError converts an NTSTATUS into the ordinary error the rest of the
// package expects.
//
// This is not cosmetic. NtCreateFile reports an NTStatus, and errors.Is on one
// answers nothing about os.ErrExist — so the retry loop in createTemp, which
// continues only on "already exists", would treat a name collision as a hard
// failure instead. The mapping mirrors the standard library's own
// ntCreateFileError.
func ntStatusError(err error) error {
	status, ok := err.(windows.NTStatus)
	if !ok {
		// Shouldn't be possible: NtCreateFile always reports NTStatus.
		return err
	}
	if status == windows.STATUS_OBJECT_NAME_COLLISION {
		return os.ErrExist
	}
	return status.Errno()
}

// restrictDirectory replaces the directory's DACL with an owner-only,
// inheritance-protected one.
//
// The handle it acts on is opened HERE, relative to the pinned directory and
// with WRITE_DAC — which is the right the DACL change requires and which no
// handle the os package hands out carries. An empty ObjectName with a
// RootDirectory is the NT way of saying "reopen this same object", so nothing
// is resolved by name and the directory cannot be swapped underneath.
func restrictDirectory(dir *Dir) error {
	descriptor, err := ownerOnlyDescriptor()
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read owner-only dacl: %w", err)
	}

	directory, err := dir.root.Open(".")
	if err != nil {
		return stripPath(err)
	}
	defer func() { _ = directory.Close() }()

	// Empty name + RootDirectory == "the object RootDirectory refers to".
	var sameObject windows.NTUnicodeString
	attributes := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(directory.Fd()),
		ObjectName:    &sameObject,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(*attributes))

	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle,
		windows.SYNCHRONIZE|windows.READ_CONTROL|windows.WRITE_DAC,
		attributes,
		&status,
		nil,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_SYNCHRONOUS_IO_NONALERT|
			windows.FILE_DIRECTORY_FILE|
			windows.FILE_OPEN_FOR_BACKUP_INTENT,
		0,
		0,
	)
	runtime.KeepAlive(attributes)
	runtime.KeepAlive(directory)
	if err != nil {
		return fmt.Errorf("reopen directory for dacl change: %w", ntStatusError(err))
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	err = windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
	runtime.KeepAlive(descriptor)
	if err != nil {
		return fmt.Errorf("apply owner-only dacl: %w", err)
	}
	return nil
}
