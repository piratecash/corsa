package secretfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// audit.go answers the question the writer cannot: is the place a secret
// lives safe to keep it in?
//
// Dir.Write makes the FILE owner-only and never writes through a link, but a
// file is only as private as the directories above it. Whoever can write into
// any of them can rename the file's directory away and put their own in its
// place, and a node that reads its protection state from there reads theirs.
// The writer cannot refuse such a directory — it is handed a parent whose
// permissions are not its to decide — so the check is a separate, explicit
// audit whose answer the caller decides what to do with.
//
// The audit names components by their ROLE (the file, the directory holding
// it, a directory n levels above it), never by path: the error travels into
// logs and diagnostics, and the operator's filesystem layout is not part of
// what it has to say. The caller knows which file it asked about.
//
// What it is NOT: a guard. It walks the path by name, so a component swapped
// after the audit is not seen. That is acceptable because nothing here
// decides where bytes land — writes still go through the pinned handle — and
// the audit runs once, when a store is loaded, to tell the operator about a
// location that was unsafe to begin with.

// ErrUnsafeLocation marks a secret file, or a directory on the way to it,
// that someone other than this process's user (or root) could change.
// AuditLocation returns it as *UnsafeLocationError.
var ErrUnsafeLocation = errors.New("secretfile: the location of a secret is writable by others")

// ErrDirectoryNotWritable is a write refused because this process may not
// create files in the target directory — a configuration problem, reported
// with the directory's owner and mode so the operator does not have to
// reconstruct it from a bare "permission denied".
var ErrDirectoryNotWritable = errors.New("secretfile: this process may not create files in the directory")

// LocationViolation is why a location failed the audit. Its text is the code
// operators search logs for.
type LocationViolation string

const (
	// ViolationSymlink: the file is a symlink. The link, not the file,
	// decides where the secret is read from.
	ViolationSymlink LocationViolation = "symlink"
	// ViolationNotRegularFile: the file is a directory, a device or another
	// kind of entry that is not a regular file.
	ViolationNotRegularFile LocationViolation = "not_regular_file"
	// ViolationFileNotOwnerOnly: the file is not exactly what the writer
	// creates — on POSIX mode 0600, on Windows a DACL that grants nobody
	// but the current user.
	ViolationFileNotOwnerOnly LocationViolation = "file_not_owner_only"
	// ViolationForeignOwner: owned by a user other than this process's. The
	// file must be this user's; a directory may also be root's.
	ViolationForeignOwner LocationViolation = "foreign_owner"
	// ViolationWritableByOthers: a directory group- or world-writable
	// without the sticky bit, so others may rename or replace what is in it.
	ViolationWritableByOthers LocationViolation = "writable_by_others"
	// ViolationStickyChildNotOwned: the entry sits in a sticky directory
	// others may write (/tmp), which protects only entries this user owns,
	// and this one is not.
	ViolationStickyChildNotOwned LocationViolation = "sticky_child_not_owned"
)

// ComponentKind is the role of an audited component on the way to the file.
type ComponentKind int

const (
	// ComponentFile is the secret file itself.
	ComponentFile ComponentKind = iota + 1
	// ComponentDirectory is a directory the file's resolved location passes
	// through; LevelsAbove says which.
	ComponentDirectory
	// ComponentLinkRoute is a symlink on the way to the file, or a
	// directory passed only while following one: it is not on the resolved
	// location, but whoever can change it can change where that leads.
	ComponentLinkRoute
)

// LocationComponent names one audited component by its role.
type LocationComponent struct {
	Kind ComponentKind
	// LevelsAbove is meaningful for ComponentDirectory only: 1 is the
	// directory holding the file, 2 its parent, and so on up to the root.
	LevelsAbove int
}

func fileComponent() LocationComponent { return LocationComponent{Kind: ComponentFile} }

func (c LocationComponent) String() string {
	switch c.Kind {
	case ComponentFile:
		return "the file"
	case ComponentDirectory:
		if c.LevelsAbove == 1 {
			return "the directory holding the file"
		}
		return fmt.Sprintf("the directory %d levels above the file", c.LevelsAbove)
	case ComponentLinkRoute:
		return "a symlink on the way to the file, or a directory holding one"
	default:
		return "an unnamed component"
	}
}

// UnsafeLocationError is one violation found by AuditLocation: where, what,
// and the owner and mode that make it one. It names no path.
type UnsafeLocationError struct {
	Component LocationComponent
	Violation LocationViolation
	// Owner is the component's owner as the platform names it ("uid 501");
	// empty where the platform's audit does not read it.
	Owner string
	Mode  fs.FileMode
}

func (e *UnsafeLocationError) Error() string {
	owner := e.Owner
	if owner == "" {
		owner = "not read"
	}
	return fmt.Sprintf("%s: %s: %s (owner %s, mode %s)", ErrUnsafeLocation, e.Component, e.Violation, owner, e.Mode)
}

// Is makes errors.Is(err, ErrUnsafeLocation) hold for every violation.
func (e *UnsafeLocationError) Is(target error) bool { return target == ErrUnsafeLocation }

// AuditLocation checks that path — a secret file, existing or about to be
// written — and every directory above it can be changed by nobody but this
// process's user or root.
//
// It returns nil for a safe location, an *UnsafeLocationError (errors.Is
// ErrUnsafeLocation) for the violation nearest the file, or another error
// when a component could not be inspected; the caller decides whether an
// unverifiable location counts as an unsafe one.
//
// A file that does not exist yet is not a violation, and neither are
// directories not created yet: the audit covers what exists. The parts that
// differ by platform — the ancestor chain on POSIX, the DACL on Windows — are
// in audit_posix.go and audit_windows.go.
func AuditLocation(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("secretfile: audit: resolve the working directory: %w", stripPath(err))
	}
	return auditLocation(abs)
}

// inspectFile reads the file at path without following it. present is false
// when there is no file yet; a symlink or a non-regular entry in its place is
// the violation returned.
func inspectFile(path string) (info fs.FileInfo, present bool, err error) {
	info, err = os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("secretfile: audit the file: %w", stripPath(err))
	case info.Mode()&fs.ModeSymlink != 0:
		return nil, false, &UnsafeLocationError{Component: fileComponent(), Violation: ViolationSymlink, Owner: ownerOf(info), Mode: info.Mode()}
	case !info.Mode().IsRegular():
		return nil, false, &UnsafeLocationError{Component: fileComponent(), Violation: ViolationNotRegularFile, Owner: ownerOf(info), Mode: info.Mode()}
	}
	return info, true, nil
}

// unsafeOrNil turns a judgement into an error without the typed-nil trap: a
// nil *UnsafeLocationError returned as error is not a nil error.
func unsafeOrNil(found *UnsafeLocationError) error {
	if found == nil {
		return nil
	}
	return found
}
