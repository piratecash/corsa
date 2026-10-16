package secretfile

// audit_acl.go is the judgement the Windows audit applies to one object on
// the way to a secret: its owner and its DACL, read into SIDs and access
// masks by audit_windows.go. It is a pure function, compiled and tested on
// every platform, because the descriptors it reads exist only on Windows.
//
// What "nobody else can change this location" means on NTFS, by role:
//
//   - the FILE (the store, the marker) is owned by the current user, SYSTEM
//     or Administrators — the owner of an object can always rewrite its
//     DACL, and an elevated writer's files belong to Administrators — and
//     every allow entry that applies to it names one of those three. The
//     writer grants the user alone, but a copy in Explorer, robocopy without
//     /SEC or a restore from a backup gives the file the inherited SYSTEM and
//     Administrators entries; neither adds anyone: SYSTEM and Administrators
//     can read any file through SeBackupPrivilege and take it through
//     SeTakeOwnershipPrivilege, and under UAC a filtered token holds
//     Administrators as deny-only.
//   - the HOLDING DIRECTORY is owned by the current user, SYSTEM,
//     Administrators or TrustedInstaller, and no one else may add a file or
//     a directory to it (a name planted before the first write would be read
//     as the store), delete its children, delete or rename it, rewrite its
//     DACL or take ownership of it.
//   - every ANCESTOR, up to and including the root of the volume, has the
//     same owners and no one else may delete its children, delete or rename
//     it, rewrite its DACL or take ownership of it. DELETE on the ROOT is
//     not judged: a volume root cannot be deleted or renamed, which is why
//     the root of a data drive where Authenticated Users hold "Modify" (no
//     FILE_DELETE_CHILD in it) passes — the directory below it does not,
//     until it is made private. Adding files or
//     directories to an ancestor is NOT a violation: the next directory on
//     the path can only be replaced after it has been moved away, which
//     needs DELETE on it or FILE_DELETE_CHILD here — both judged — so the
//     right to create SIBLINGS (Authenticated Users may create folders in
//     the root of the system drive) does not reach the path.
//
// A volume without security descriptors (FAT, exFAT) reports no DACL, which
// grants everyone everything, and is refused.
//
// The boundary is the root of the volume for every path, inside the user's
// profile or outside it: a directory owned by SYSTEM halfway up does not end
// the walk, because whoever may delete children of its parent could still
// move it away. A path outside the profile therefore passes only where its
// directories were made private — one created under C:\ inherits the
// "Authenticated Users: Modify" entry and is refused, as it should be.
//
// Rights are judged after generic rights are mapped to file rights; deny
// entries only take away and are skipped; an entry inherited by children
// only (INHERIT_ONLY) does not apply to the object itself; the placeholder
// trustees CREATOR OWNER and CREATOR GROUP grant nothing on the object they
// sit on; OWNER RIGHTS stands for the owner, already judged trusted. An entry
// of a type this audit does not read (object or callback entries) is not
// trusted. A missing DACL grants everyone everything.

// Windows access rights the judgement reads, by their winnt.h values. They
// are spelled out rather than imported so the judgement compiles everywhere.
const (
	rightAddFile         uint32 = 0x0002     // FILE_ADD_FILE (FILE_WRITE_DATA)
	rightAddSubdirectory uint32 = 0x0004     // FILE_ADD_SUBDIRECTORY (FILE_APPEND_DATA)
	rightDeleteChild     uint32 = 0x0040     // FILE_DELETE_CHILD
	rightDelete          uint32 = 0x00010000 // DELETE
	rightWriteDAC        uint32 = 0x00040000 // WRITE_DAC
	rightWriteOwner      uint32 = 0x00080000 // WRITE_OWNER
	rightGenericAll      uint32 = 0x10000000 // GENERIC_ALL
	rightGenericWrite    uint32 = 0x40000000 // GENERIC_WRITE
	fileGenericWrite     uint32 = 0x00120116 // FILE_GENERIC_WRITE
	fileAllAccess        uint32 = 0x001F01FF // FILE_ALL_ACCESS
)

// Well-known SIDs the judgement trusts or interprets.
const (
	sidLocalSystem    = "S-1-5-18"
	sidAdministrators = "S-1-5-32-544"
	// sidTrustedInstaller is the service SID of TrustedInstaller, the owner
	// of the system drive's root and of the system's own directories.
	sidTrustedInstaller = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"
	sidOwnerRights      = "S-1-3-4"
	sidCreatorOwner     = "S-1-3-0"
	sidCreatorGroup     = "S-1-3-1"
)

// rightsChangingTheLocation are the rights that let their holder change
// where a path leads, on any directory of the path.
const rightsChangingTheLocation = rightDeleteChild | rightDelete | rightWriteDAC | rightWriteOwner

// rightsAddingNames are the rights to create entries in a directory; a
// violation on the holding directory only.
const rightsAddingNames = rightAddFile | rightAddSubdirectory

type aclEntryKind int

const (
	aclAllow aclEntryKind = iota + 1
	aclDeny
	// aclUnknown: an entry of a type the audit does not read.
	aclUnknown
)

// aclEntry is one entry of a DACL as the judgement sees it.
type aclEntry struct {
	kind    aclEntryKind
	trustee string
	mask    uint32
	// inheritOnly: the entry is there for children and does not apply to
	// the object itself.
	inheritOnly bool
}

// objectSecurity is an object's owner and DACL.
type objectSecurity struct {
	owner    string
	dacl     []aclEntry
	nullDACL bool
}

// aclRole is the object's place on the way to the secret.
type aclRole int

const (
	aclRoleFile aclRole = iota + 1
	aclRoleHoldingDirectory
	aclRoleAncestor
	// aclRoleVolumeRoot: the root of the volume, the last ancestor. It
	// cannot be deleted or renamed, so DELETE on it changes nothing.
	aclRoleVolumeRoot
)

// judgeObjectSecurity returns the violation sec is, for an object in role on
// the way to a secret of user, or false.
func judgeObjectSecurity(sec objectSecurity, role aclRole, user string) (LocationViolation, bool) {
	if !trustedOwner(sec.owner, role, user) {
		return ViolationForeignOwner, true
	}
	refused := ViolationWritableByOthers
	if role == aclRoleFile {
		refused = ViolationFileNotOwnerOnly
	}
	if sec.nullDACL {
		return refused, true
	}
	for _, entry := range sec.dacl {
		if entryGrantsOthers(entry, role, user) {
			return refused, true
		}
	}
	return "", false
}

// trustedOwner: the file belongs to the user (or, written elevated, to
// Administrators, or to SYSTEM); a directory may also be TrustedInstaller's.
func trustedOwner(owner string, role aclRole, user string) bool {
	switch owner {
	case user, sidLocalSystem, sidAdministrators:
		return true
	case sidTrustedInstaller:
		return role != aclRoleFile
	default:
		return false
	}
}

// entryGrantsOthers reports whether entry lets someone the role does not
// trust change the location.
func entryGrantsOthers(entry aclEntry, role aclRole, user string) bool {
	switch {
	case entry.kind == aclUnknown:
		return true
	case entry.kind == aclDeny, entry.inheritOnly:
		return false
	case role == aclRoleFile:
		return !trustedFileTrustee(entry.trustee, user)
	case trustedTrustee(entry.trustee, user):
		return false
	default:
		return mapGenericRights(entry.mask)&rightsChangingTheDirectory(role) != 0
	}
}

// trustedFileTrustee: who may hold rights on the file itself — the user,
// and SYSTEM and Administrators, which can reach any file anyway.
func trustedFileTrustee(trustee, user string) bool {
	switch trustee {
	case user, sidLocalSystem, sidAdministrators:
		return true
	default:
		return false
	}
}

func trustedTrustee(trustee, user string) bool {
	switch trustee {
	case user, sidLocalSystem, sidAdministrators, sidTrustedInstaller, sidOwnerRights, sidCreatorOwner, sidCreatorGroup:
		return true
	default:
		return false
	}
}

var rightsChangingTheDirectoryByRole = map[aclRole]uint32{
	aclRoleHoldingDirectory: rightsChangingTheLocation | rightsAddingNames,
	aclRoleAncestor:         rightsChangingTheLocation,
	aclRoleVolumeRoot:       rightsChangingTheLocation &^ rightDelete,
}

func rightsChangingTheDirectory(role aclRole) uint32 {
	return rightsChangingTheDirectoryByRole[role]
}

// mapGenericRights expands the generic rights of an entry into the file
// rights they stand for on a file or a directory.
func mapGenericRights(mask uint32) uint32 {
	if mask&rightGenericAll != 0 {
		mask |= fileAllAccess
	}
	if mask&rightGenericWrite != 0 {
		mask |= fileGenericWrite
	}
	return mask
}

// Reparse tags (winnt.h IO_REPARSE_TAG_*) the judgement reads. A tag with
// the name-surrogate bit redirects the name to another one — a symlink, a
// mount point or junction — and is the substitution the audit refuses. A
// few tags only change how the data of the entry itself is stored and leave
// the name where it is: cloud placeholders (OneDrive Files On-Demand and the
// other cloud providers' slots), deduplicated files and WOF-compressed files.
// Every other tag is unknown to the audit and refused, not guessed at.
const (
	reparseTagNameSurrogate uint32 = 0x20000000
	reparseTagCloudMask     uint32 = 0xFFFF0FFF // IO_REPARSE_TAG_CLOUD_1..F differ in bits 12-15
	reparseTagCloud         uint32 = 0x9000001A // IO_REPARSE_TAG_CLOUD
	reparseTagDedup         uint32 = 0x80000013 // IO_REPARSE_TAG_DEDUP
	reparseTagWOF           uint32 = 0x80000017 // IO_REPARSE_TAG_WOF
)

// judgeReparseTag returns the violation a reparse point with tag is on the
// way to a secret, or false for a tag that does not redirect the name.
func judgeReparseTag(tag uint32) (LocationViolation, bool) {
	switch {
	case tag&reparseTagNameSurrogate != 0:
		return ViolationSymlink, true
	case tag&reparseTagCloudMask == reparseTagCloud, tag == reparseTagDedup, tag == reparseTagWOF:
		return "", false
	default:
		return ViolationReparsePoint, true
	}
}
