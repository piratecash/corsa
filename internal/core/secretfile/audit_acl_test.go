package secretfile

import "testing"

// audit_acl_test.go pins the judgement the Windows audit applies to an
// object's owner and DACL (audit_acl.go). The judgement is a pure function of
// SIDs and access masks, so it runs — and fails — on every platform; the
// Windows tests (audit_windows_test.go) check that the real descriptors are
// read into it.

const (
	aclTestUser      = "S-1-5-21-1000-1000-1000-1001"
	aclTestStranger  = "S-1-5-21-1000-1000-1000-1002"
	aclTestEveryone  = "S-1-1-0"
	aclTestAuthUsers = "S-1-5-11"
)

// aclTestModify is the "Modify" right icacls grants (M): read, write,
// execute and DELETE, without FILE_DELETE_CHILD.
const aclTestModify uint32 = 0x001301BF

func violationOf(v LocationViolation) *LocationViolation { return &v }

func allow(trustee string, mask uint32) aclEntry {
	return aclEntry{kind: aclAllow, trustee: trustee, mask: mask}
}

func ownedBy(owner string, entries ...aclEntry) objectSecurity {
	return objectSecurity{owner: owner, dacl: entries}
}

func TestJudgeObjectSecuritySynthetic(t *testing.T) {
	t.Parallel()
	fullControl := uint32(fileAllAccess)
	inheritOnly := allow(aclTestAuthUsers, 0x001301BF)
	inheritOnly.inheritOnly = true

	cases := []struct {
		name string
		sec  objectSecurity
		role aclRole
		want *LocationViolation
	}{
		{name: "the file as the writer leaves it", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl)), role: aclRoleFile},
		{name: "the file owned by Administrators (an elevated writer)", sec: ownedBy(sidAdministrators, allow(aclTestUser, fullControl)), role: aclRoleFile},
		{name: "the file owned by another user", sec: ownedBy(aclTestStranger, allow(aclTestUser, fullControl)), role: aclRoleFile, want: violationOf(ViolationForeignOwner)},
		{name: "the file owned by TrustedInstaller", sec: ownedBy(sidTrustedInstaller, allow(aclTestUser, fullControl)), role: aclRoleFile, want: violationOf(ViolationForeignOwner)},
		{name: "the file copied or restored with SYSTEM and Administrators inherited", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(sidLocalSystem, fullControl), allow(sidAdministrators, fullControl)), role: aclRoleFile},
		{name: "the file readable by a stranger", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestStranger, 0x00120089)), role: aclRoleFile, want: violationOf(ViolationFileNotOwnerOnly)},
		{name: "the file readable by Authenticated Users", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestAuthUsers, 0x00120089)), role: aclRoleFile, want: violationOf(ViolationFileNotOwnerOnly)},
		{name: "the file with no DACL", sec: objectSecurity{owner: aclTestUser, nullDACL: true}, role: aclRoleFile, want: violationOf(ViolationFileNotOwnerOnly)},

		{name: "the profile directory", sec: ownedBy(aclTestUser, allow(sidLocalSystem, fullControl), allow(sidAdministrators, fullControl), allow(aclTestUser, fullControl)), role: aclRoleHoldingDirectory},
		{name: "the holding directory others may add files to", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestEveryone, rightAddFile)), role: aclRoleHoldingDirectory, want: violationOf(ViolationWritableByOthers)},
		{name: "the holding directory others may add directories to", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestAuthUsers, rightAddSubdirectory)), role: aclRoleHoldingDirectory, want: violationOf(ViolationWritableByOthers)},
		{name: "the holding directory others may write generically", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestEveryone, rightGenericWrite)), role: aclRoleHoldingDirectory, want: violationOf(ViolationWritableByOthers)},
		{name: "the holding directory owned by another user", sec: ownedBy(aclTestStranger, allow(aclTestUser, fullControl)), role: aclRoleHoldingDirectory, want: violationOf(ViolationForeignOwner)},

		{name: "the volume root: Authenticated Users may only create folders", sec: ownedBy(sidTrustedInstaller, allow(sidAdministrators, fullControl), allow(sidLocalSystem, fullControl), allow("S-1-5-32-545", 0x001200A9), inheritOnly, allow(aclTestAuthUsers, rightAddSubdirectory)), role: aclRoleAncestor},
		{name: "a volume root where Authenticated Users may modify (D:\\)", sec: ownedBy(sidAdministrators, allow(sidAdministrators, fullControl), allow(sidLocalSystem, fullControl), allow(aclTestAuthUsers, aclTestModify)), role: aclRoleVolumeRoot},
		{name: "the directory below such a root, not closed", sec: ownedBy(aclTestUser, allow(aclTestUser, fullControl), allow(aclTestAuthUsers, aclTestModify)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "a volume root others may delete children of", sec: ownedBy(sidAdministrators, allow(sidAdministrators, fullControl), allow(aclTestAuthUsers, rightDeleteChild)), role: aclRoleVolumeRoot, want: violationOf(ViolationWritableByOthers)},
		{name: "a volume root others may re-permission", sec: ownedBy(sidAdministrators, allow(aclTestEveryone, rightWriteDAC)), role: aclRoleVolumeRoot, want: violationOf(ViolationWritableByOthers)},
		{name: "a volume root others may take ownership of", sec: ownedBy(sidAdministrators, allow(aclTestEveryone, rightWriteOwner)), role: aclRoleVolumeRoot, want: violationOf(ViolationWritableByOthers)},
		{name: "a volume root without a DACL (FAT, exFAT)", sec: objectSecurity{owner: sidAdministrators, nullDACL: true}, role: aclRoleVolumeRoot, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor others may add files to: siblings only", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightAddFile|rightAddSubdirectory)), role: aclRoleAncestor},
		{name: "an ancestor others may delete children of", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightDeleteChild)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor others may delete or rename", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightDelete)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor others may re-permission", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightWriteDAC)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor others may take ownership of", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightWriteOwner)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor others hold GENERIC_ALL on", sec: ownedBy(sidLocalSystem, allow(sidLocalSystem, fullControl), allow(aclTestEveryone, rightGenericAll)), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor with an ACE of a type the audit cannot read", sec: ownedBy(sidLocalSystem, aclEntry{kind: aclUnknown}), role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "an ancestor with no DACL", sec: objectSecurity{owner: sidLocalSystem, nullDACL: true}, role: aclRoleAncestor, want: violationOf(ViolationWritableByOthers)},
		{name: "a deny ACE only takes away", sec: ownedBy(sidLocalSystem, aclEntry{kind: aclDeny, trustee: aclTestEveryone, mask: rightDelete}), role: aclRoleAncestor},
		{name: "OWNER RIGHTS of a trusted owner", sec: ownedBy(aclTestUser, allow(sidOwnerRights, fullControl)), role: aclRoleHoldingDirectory},
		{name: "an inherit-only grant does not apply here", sec: ownedBy(aclTestUser, inheritOnly), role: aclRoleHoldingDirectory},
	}
	for _, tc := range cases {
		got, found := judgeObjectSecurity(tc.sec, tc.role, aclTestUser)
		switch {
		case tc.want == nil && found:
			t.Errorf("%s: judged %s, want accepted", tc.name, got)
		case tc.want != nil && !found:
			t.Errorf("%s: accepted, want %s", tc.name, *tc.want)
		case tc.want != nil && got != *tc.want:
			t.Errorf("%s: judged %s, want %s", tc.name, got, *tc.want)
		}
	}
}

// The TrustedInstaller SID is the service SID of "TrustedInstaller": the
// first three sub-authorities are the ones every Windows release shows.
func TestTheTrustedInstallerSIDIsTheServiceSID(t *testing.T) {
	t.Parallel()
	if sidTrustedInstaller != "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464" {
		t.Fatalf("sidTrustedInstaller = %s", sidTrustedInstaller)
	}
}

func TestJudgeReparseTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		tag  uint32
		want *LocationViolation
	}{
		{name: "symlink", tag: 0xA000000C, want: violationOf(ViolationSymlink)},
		{name: "mount point / junction", tag: 0xA0000003, want: violationOf(ViolationSymlink)},
		{name: "any other name surrogate", tag: 0xA0000099, want: violationOf(ViolationSymlink)},
		{name: "OneDrive Files On-Demand placeholder", tag: 0x9000001A},
		{name: "a cloud placeholder of another provider slot", tag: 0x9000701A},
		{name: "deduplicated file", tag: 0x80000013},
		{name: "WOF-compressed file", tag: 0x80000017},
		{name: "an app execution alias", tag: 0x8000001B, want: violationOf(ViolationReparsePoint)},
		{name: "an unknown tag", tag: 0x80001234, want: violationOf(ViolationReparsePoint)},
	}
	for _, tc := range cases {
		got, found := judgeReparseTag(tc.tag)
		switch {
		case tc.want == nil && found:
			t.Errorf("%s: judged %s, want accepted", tc.name, got)
		case tc.want != nil && !found:
			t.Errorf("%s: accepted, want %s", tc.name, *tc.want)
		case tc.want != nil && got != *tc.want:
			t.Errorf("%s: judged %s, want %s", tc.name, got, *tc.want)
		}
	}
}
