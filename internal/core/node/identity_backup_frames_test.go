package node

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// newBackupTestService builds a service whose data dir — and therefore whose
// identity-backup sandbox — lives entirely under t.TempDir(). ChatLogDir is
// what EffectiveDataDir derives from; leaving it empty would point the
// sandbox at the real user data directory of whoever runs the tests.
func newBackupTestService(t *testing.T) (*Service, *identity.Identity, string) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		IdentityPath:      filepath.Join(dir, "identity.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	return svc, id, dir
}

// backupDir is where the service is allowed to keep backups, and the only
// place a test may look for one.
func backupDir(t *testing.T, svc *Service) string {
	t.Helper()
	return svc.cfg.EffectiveIdentityBackupDir()
}

// TestIdentityBackupRestoreRoundtrip: the local RPC pair exports both keys
// with the record seq under a NAME, and restores them into the identity file
// exactly — no key material and no filesystem path in either frame.
func TestIdentityBackupRestoreRoundtrip(t *testing.T) {
	t.Parallel()
	svc, id, _ := newBackupTestService(t)

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "primary.json"})
	if reply.Type != "identity_backup" || reply.IdentityBackup == nil {
		t.Fatalf("backup reply: %+v", reply)
	}
	if reply.IdentityBackup.Address != id.Address || reply.IdentityBackup.Name != "primary.json" {
		t.Fatalf("backup frame = %+v", reply.IdentityBackup)
	}
	backupPath := filepath.Join(backupDir(t, svc), "primary.json")
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file missing from the sandbox: %v", err)
	}

	restore := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupName: "primary.json"})
	if restore.Type != "identity_restore" || restore.IdentityBackup == nil {
		t.Fatalf("restore reply: %+v", restore)
	}
	frame := restore.IdentityBackup
	if frame.Address != id.Address || !frame.RestartRequired || frame.BoxKeyDerived || frame.Warning != "" {
		t.Fatalf("restore frame = %+v, want same address, restart, no derived-key warning", frame)
	}
	if frame.Name != "primary.json" {
		t.Fatalf("restore frame names %q, want the backup name", frame.Name)
	}

	restored, err := identity.Load(svc.cfg.IdentityPath)
	if err != nil {
		t.Fatalf("load restored identity: %v", err)
	}
	if restored.Address != id.Address {
		t.Fatalf("restored address = %s, want %s", restored.Address, id.Address)
	}
	if identity.BoxPublicKeyBase64(restored.BoxPublicKey) != identity.BoxPublicKeyBase64(id.BoxPublicKey) {
		t.Fatal("the box key did not survive the versioned backup roundtrip")
	}
}

// TestIdentityBackupRepliesCarryNoFilesystemPath: the reply names the backup
// and nothing else. An absolute path tells an RPC caller the node's data
// layout — and on a desktop build, the operator's home directory name.
func TestIdentityBackupRepliesCarryNoFilesystemPath(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "leak-check"})
	if reply.Type != "identity_backup" {
		t.Fatalf("backup reply: %+v", reply)
	}
	restore := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupName: "leak-check"})
	if restore.Type != "identity_restore" {
		t.Fatalf("restore reply: %+v", restore)
	}
	for what, frame := range map[string]*protocol.IdentityBackupFrame{
		"backup":  reply.IdentityBackup,
		"restore": restore.IdentityBackup,
	} {
		if strings.Contains(frame.Name, dir) || strings.ContainsAny(frame.Name, `/\`) {
			t.Fatalf("%s reply leaked a filesystem path: %q", what, frame.Name)
		}
	}
}

// TestIdentityBackupRejectsPathEscapes is the whole point of the name-not-a-
// path rule: every shape of "write this file somewhere else" is refused, and
// refused BEFORE anything is written.
func TestIdentityBackupRejectsPathEscapes(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)
	outside := filepath.Join(dir, "outside.json")

	names := map[string]string{
		"parent traversal":     "../outside.json",
		"nested traversal":     "sub/../../outside.json",
		"forward slash":        "sub/outside.json",
		"backslash":            `sub\outside.json`,
		"absolute unix path":   "/tmp/outside.json",
		"absolute-ish windows": `C:\outside.json`,
		"bare dotdot":          "..",
		"bare dot":             ".",
		"leading dot":          ".hidden",
		"leading dash":         "-rf",
		"empty after trim":     "   ",
		"reserved device":      "con",
		"reserved with ext":    "nul.json",
		"too long":             strings.Repeat("a", 65),
		"newline injection":    "backup\nname",
		"null byte":            "backup\x00name",
	}
	for what, name := range names {
		for _, frameType := range []string{"identity_backup", "identity_restore"} {
			reply := svc.HandleLocalFrame(protocol.Frame{Type: frameType, BackupName: name})
			if reply.Type != "error" {
				t.Fatalf("%s accepted %s (%q): %+v", frameType, what, name, reply)
			}
		}
	}

	if _, err := os.Stat(outside); !os.IsNotExist(err) {
		t.Fatal("a rejected name still wrote a file outside the backup directory")
	}
	if _, err := os.Stat(svc.cfg.IdentityPath); !os.IsNotExist(err) {
		t.Fatal("a rejected restore touched the identity file")
	}
}

// TestIdentityBackupRefusesSymlinkedEntry: the name whitelist stops the name
// from escaping, but a symlink escapes without the name's help — an entry
// planted in the backup directory in advance would redirect the write (and,
// on restore, the read) anywhere the node process can reach.
func TestIdentityBackupRefusesSymlinkedEntry(t *testing.T) {
	t.Parallel()
	// Not "skip on Windows": whether links can be created there depends on
	// privilege, not on the OS, so an elevated shell runs this for real.
	fsprobe.RequireSymlinks(t)
	svc, _, dir := newBackupTestService(t)

	// Force the sandbox into existence, then plant the link inside it.
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "seed"}); reply.Type != "identity_backup" {
		t.Fatalf("seed backup: %+v", reply)
	}
	target := filepath.Join(dir, "victim.json")
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(backupDir(t, svc), "trap.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	for _, frameType := range []string{"identity_backup", "identity_restore"} {
		reply := svc.HandleLocalFrame(protocol.Frame{Type: frameType, BackupName: "trap.json"})
		if reply.Type != "error" {
			t.Fatalf("%s followed a symlink out of the sandbox: %+v", frameType, reply)
		}
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(contents) != "original" {
		t.Fatal("the symlink target was overwritten through the backup command")
	}
}

// TestIdentityBackupRefusesSymlinkedDirectory is the escape the entry-level
// symlink check does NOT cover: the backup directory itself is a link out of
// the data dir. Everything downstream then follows it — the chmod relaxes
// somebody else's directory, and the backup lands there with both private
// keys in it.
func TestIdentityBackupRefusesSymlinkedDirectory(t *testing.T) {
	t.Parallel()
	// Not "skip on Windows": whether links can be created there depends on
	// privilege, not on the OS, so an elevated shell runs this for real.
	fsprobe.RequireSymlinks(t)
	svc, _, dir := newBackupTestService(t)

	// A directory outside the data dir, with permissions the node must not
	// touch and content it must not replace.
	outside := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("create outside dir: %v", err)
	}
	if err := os.Chmod(outside, 0o755); err != nil {
		t.Fatalf("chmod outside dir: %v", err)
	}
	// Plant the link where the sandbox is expected.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.Symlink(outside, backupDir(t, svc)); err != nil {
		t.Fatalf("symlink sandbox: %v", err)
	}

	for _, frameType := range []string{"identity_backup", "identity_restore"} {
		reply := svc.HandleLocalFrame(protocol.Frame{Type: frameType, BackupName: "escape.json"})
		if reply.Type != "error" {
			t.Fatalf("%s wrote through a symlinked backup directory: %+v", frameType, reply)
		}
	}
	if _, err := os.Stat(filepath.Join(outside, "escape.json")); !os.IsNotExist(err) {
		t.Fatal("a backup landed outside the data directory")
	}
	info, err := os.Stat(outside)
	if err != nil {
		t.Fatalf("stat outside dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o755 {
		t.Fatalf("the node changed permissions of a directory outside its data dir: %o", perm)
	}
}

// TestIdentityBackupRefusesDirectoryLinkedToItsOwnParent is the escape that
// "must not leave the root" does NOT cover: a symlink identity-backups → "."
// stays comfortably inside the data directory and redirects every write into
// the data directory itself — where a backup named "trust-64646.json" lands
// on the node's trust store.
func TestIdentityBackupRefusesDirectoryLinkedToItsOwnParent(t *testing.T) {
	t.Parallel()
	// Not "skip on Windows": whether links can be created there depends on
	// privilege, not on the OS, so an elevated shell runs this for real.
	fsprobe.RequireSymlinks(t)
	svc, _, dir := newBackupTestService(t)

	victim := filepath.Join(dir, "trust-64646.json")
	if err := os.WriteFile(victim, []byte(`{"contacts":[]}`), 0o600); err != nil {
		t.Fatalf("create victim: %v", err)
	}
	// The sandbox is a link back to the directory that contains it.
	if err := os.Symlink(".", backupDir(t, svc)); err != nil {
		t.Fatalf("symlink sandbox to its parent: %v", err)
	}

	for _, frameType := range []string{"identity_backup", "identity_restore"} {
		reply := svc.HandleLocalFrame(protocol.Frame{Type: frameType, BackupName: "trust-64646.json"})
		if reply.Type != "error" {
			t.Fatalf("%s wrote through a sandbox linked to its own parent: %+v", frameType, reply)
		}
	}
	contents, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read victim: %v", err)
	}
	if string(contents) != `{"contacts":[]}` {
		t.Fatal("a backup overwrote the trust store through a self-linked sandbox")
	}
}

// TestIdentityBackupCannotDestroyAnotherBackup: with the temp file derived as
// "<name>.tmp", writing a backup called "recovery" deleted an existing backup
// named "recovery.tmp" — both are names a user is allowed to pick, and no
// concurrency was needed. The temp must live outside the space of valid
// backup names.
func TestIdentityBackupCannotDestroyAnotherBackup(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)

	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "recovery.tmp"}); reply.Type != "identity_backup" {
		t.Fatalf("first backup: %+v", reply)
	}
	victim := filepath.Join(backupDir(t, svc), "recovery.tmp")
	before, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read first backup: %v", err)
	}

	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "recovery"}); reply.Type != "identity_backup" {
		t.Fatalf("second backup: %+v", reply)
	}

	after, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("the second backup destroyed the first: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("the second backup overwrote the first")
	}
	// And restore must still find it — proof the survivor is a usable file,
	// not an empty husk.
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupName: "recovery.tmp"}); reply.Type != "identity_restore" {
		t.Fatalf("restore of the survivor: %+v", reply)
	}
}

// TestIdentityBackupLeavesNoTempFile: the temp is unique now, so nothing
// removes a stale one by name — the sweep has to. A leftover temp is two
// private keys under a name the user does not recognise.
func TestIdentityBackupLeavesNoTempFile(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "seed"}); reply.Type != "identity_backup" {
		t.Fatalf("seed backup: %+v", reply)
	}
	dir := backupDir(t, svc)

	// Simulate a crash between write and rename. Backdated past the sweep's
	// age threshold, which is what separates debris from a temp another node
	// sharing this data directory is writing right now.
	stale := filepath.Join(dir, secretfile.TempPrefix+"crashed")
	if err := os.WriteFile(stale, []byte("both private keys"), 0o600); err != nil {
		t.Fatalf("plant stale temp: %v", err)
	}
	old := time.Now().Add(-2 * staleSecretTempAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("backdate stale temp: %v", err)
	}

	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "next"}); reply.Type != "identity_backup" {
		t.Fatalf("second backup: %+v", reply)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("a crashed run's secret temp file survived the next backup")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := []string{}
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "next" || names[1] != "seed" {
		t.Fatalf("backup directory holds %v, want exactly the two backups", names)
	}
}

// TestSweepSparesAnotherNodesLiveTemp: nodes on different ports share one
// data directory by default, so they share the backup sandbox — and each has
// only its own in-process mutex. A sweep that deleted every temp would delete
// the one a sibling node is writing right now, and that node's rename would
// then fail. Age is what separates debris from work in flight.
func TestSweepSparesAnotherNodesLiveTemp(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "seed"}); reply.Type != "identity_backup" {
		t.Fatalf("seed backup: %+v", reply)
	}
	dir := backupDir(t, svc)

	// A sibling node's temp, created just now.
	live := filepath.Join(dir, secretfile.TempPrefix+"sibling")
	if err := os.WriteFile(live, []byte("a sibling node's secret, mid-write"), 0o600); err != nil {
		t.Fatalf("plant live temp: %v", err)
	}

	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "ours"}); reply.Type != "identity_backup" {
		t.Fatalf("backup: %+v", reply)
	}

	if _, err := os.Stat(live); err != nil {
		t.Fatalf("the sweep deleted a temp file another node is still writing: %v", err)
	}
}

// TestIdentityBackupErrorsCarryNoFilesystemPath: the success reply already
// hides the path; the error replies used to hand back a whole os.PathError,
// data directory included, to anyone who asked for a missing backup.
func TestIdentityBackupErrorsCarryNoFilesystemPath(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)

	// One real backup, so the directory exists and the failures below are
	// about the named backup rather than about a missing sandbox.
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "present"}); reply.Type != "identity_backup" {
		t.Fatalf("seed backup: %+v", reply)
	}

	cases := map[string]protocol.Frame{
		"restore of a missing backup": {Type: "identity_restore", BackupName: "absent.json"},
		"restore of a bad name":       {Type: "identity_restore", BackupName: "../escape"},
		"backup with a bad name":      {Type: "identity_backup", BackupName: "../escape"},
		"restore of a device name":    {Type: "identity_restore", BackupName: "nul"},
	}
	for what, frame := range cases {
		reply := svc.HandleLocalFrame(frame)
		if reply.Type != "error" {
			t.Fatalf("%s was accepted: %+v", what, reply)
		}
		if strings.Contains(reply.Error, dir) {
			t.Fatalf("%s leaked the data directory: %s", what, reply.Error)
		}
		if strings.Contains(reply.Error, config.IdentityBackupDirName+string(filepath.Separator)) {
			t.Fatalf("%s leaked a path inside the sandbox: %s", what, reply.Error)
		}
	}
}

// TestIdentityBackupDirectoryIsOwnerOnly: the file mode is not enough on its
// own — a readable directory hands out the names, and the names are what a
// reader needs to know a key file exists at all.
func TestIdentityBackupDirectoryIsOwnerOnly(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("this assertion is about POSIX mode bits; the NTFS statement of the same property is a DACL, asserted in internal/core/secretfile")
	}
	svc, _, _ := newBackupTestService(t)
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "modes"}); reply.Type != "identity_backup" {
		t.Fatalf("backup reply: %+v", reply)
	}
	info, err := os.Stat(backupDir(t, svc))
	if err != nil {
		t.Fatalf("stat backup dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("backup directory permissions = %o, want 0700", perm)
	}
}

// TestIdentityBackupDirectoryModeIsTightenedOnEveryCall: MkdirAll leaves an
// existing directory's mode alone, so a sandbox created by an older build or
// unpacked from an archive would stay world-readable forever.
func TestIdentityBackupDirectoryModeIsTightenedOnEveryCall(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("this assertion is about POSIX mode bits; the NTFS statement of the same property is a DACL, asserted in internal/core/secretfile")
	}
	svc, _, _ := newBackupTestService(t)
	dir := backupDir(t, svc)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("pre-create wide directory: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "tighten"}); reply.Type != "identity_backup" {
		t.Fatalf("backup reply: %+v", reply)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("pre-existing directory kept permissions %o, want 0700", perm)
	}
}

// TestIdentityRestoreLegacyKeyWarns: the legacy bare-Ed25519 branch keeps
// the address, derives the box key and OBLIGES the caller to warn — the
// frame must carry both the flag and the human warning text. The legacy key
// now has to be placed in the sandbox like any other backup.
func TestIdentityRestoreLegacyKeyWarns(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)
	legacy, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	dir := backupDir(t, svc)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "legacy.key"),
		[]byte(base64.StdEncoding.EncodeToString(legacy.PrivateKey)), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	restore := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupName: "legacy.key"})
	if restore.Type != "identity_restore" || restore.IdentityBackup == nil {
		t.Fatalf("restore reply: %+v", restore)
	}
	frame := restore.IdentityBackup
	if frame.Address != legacy.Address {
		t.Fatalf("address = %s, want %s preserved", frame.Address, legacy.Address)
	}
	if !frame.BoxKeyDerived || frame.Warning == "" || !frame.RestartRequired {
		t.Fatalf("legacy caveats missing from the frame: %+v", frame)
	}
}

// TestIdentityRestoreRejectsGarbage: a malformed JSON backup is a typed
// reject, never silently retried as a legacy key.
func TestIdentityRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)
	dir := backupDir(t, svc)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(`{"version": 99}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupName: "bad.json"})
	if reply.Type != "error" {
		t.Fatalf("a future-version backup was accepted: %+v", reply)
	}
	if _, err := os.Stat(svc.cfg.IdentityPath); !os.IsNotExist(err) {
		t.Fatal("a rejected restore touched the identity file")
	}
}

// TestIdentityBackupTightensExistingFilePermissions: WriteFile's mode
// applies only on creation — overwriting a pre-existing 0644 target must
// still end 0600, or both private keys stay world-readable.
func TestIdentityBackupTightensExistingFilePermissions(t *testing.T) {
	t.Parallel()
	svc, _, _ := newBackupTestService(t)
	dir := backupDir(t, svc)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	backupPath := filepath.Join(dir, "existing.json")
	if err := os.WriteFile(backupPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("pre-create: %v", err)
	}

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupName: "existing.json"})
	if reply.Type != "identity_backup" {
		t.Fatalf("backup reply: %+v", reply)
	}
	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("backup permissions = %o, want 0600", perm)
	}
}
