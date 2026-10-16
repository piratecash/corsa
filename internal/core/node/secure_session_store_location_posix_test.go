//go:build !windows

package node

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// secure_session_store_location_posix_test.go holds the store's reaction to
// the locations that are produced with mode bits. The criteria are the
// audit's (secretfile.AuditLocation), not "0777" alone: a group-writable
// directory, a file readable by others and a directory reached through a
// link into a shared one are refused as well. The reaction itself is the
// same on every platform (secure_session_store_location_test.go).

func chmodPinStoreForTest(t *testing.T, path string, mode fs.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
}

// pinStorePathsIn puts both files of a node into dir.
func pinStorePathsIn(dir string) pinStorePaths {
	return pinStorePaths{
		store:  filepath.Join(dir, "secure-sessions-64646.json"),
		marker: filepath.Join(dir, "identity-64646.json"+pinStoreMarkerSuffix),
	}
}

func dataDirForTest(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return dir
}

// The Docker case: the data directory is a volume mounted 0777. The node
// refuses and leaves the volume's mode as it found it.
func TestAPinStoreInAWorldWritableDirectoryIsARefusal(t *testing.T) {
	t.Parallel()
	dir := dataDirForTest(t)
	paths := pinStorePathsIn(dir)
	seed := seedPins(t, paths)
	chmodPinStoreForTest(t, dir, 0o777)

	store, journal := openPinStore(t, paths, newStoreClock())
	text := requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationWritableByOthers)
	for _, want := range []string{"Docker", "chmod", "1777"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the refusal does not explain the shared-volume fix (%q): %s", want, text)
		}
	}
	requireFailClosed(t, store, paths)
	if raw, err := os.ReadFile(paths.store); err != nil || string(raw) != string(seed) {
		t.Fatal("the refused store's pins were changed")
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o777 {
		t.Fatal("the node changed the directory's permissions itself")
	}
}

// Not only 0777: a directory its group may write is as open to replacement.
func TestAPinStoreInAGroupWritableDirectoryIsARefusal(t *testing.T) {
	t.Parallel()
	dir := dataDirForTest(t)
	paths := pinStorePathsIn(dir)
	seedPins(t, paths)
	chmodPinStoreForTest(t, dir, 0o770)

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationWritableByOthers)
}

func TestAPinStoreFileOthersMayReadIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seedPins(t, paths)
	if err := os.Chmod(paths.store, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	store, journal := openPinStore(t, paths, newStoreClock())
	if text := requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationFileNotOwnerOnly); !strings.Contains(text, "0600") {
		t.Fatalf("the refusal does not name the mode the file must have: %s", text)
	}
}

func TestAPinStoreMarkerOthersMayReadIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seedPins(t, paths)
	if err := os.Chmod(paths.marker, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.marker, secretfile.ViolationFileNotOwnerOnly)
}

// The marker next to the identity may live elsewhere than the store
// (CORSA_IDENTITY_PATH); its directory is held to the same rule.
func TestAPinStoreMarkerInAWorldWritableDirectoryIsARefusal(t *testing.T) {
	t.Parallel()
	storeDir, markerDir := dataDirForTest(t), dataDirForTest(t)
	paths := pinStorePaths{
		store:  pinStorePathsIn(storeDir).store,
		marker: pinStorePathsIn(markerDir).marker,
	}
	seedPins(t, paths)
	chmodPinStoreForTest(t, markerDir, 0o777)

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.marker, secretfile.ViolationWritableByOthers)
}

// A private-looking path whose directory is a link into a shared one: the
// audit follows the link and judges where the pins really are.
func TestAPinStoreReachedThroughALinkIntoAWorldWritableDirectoryIsARefusal(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	shared := dataDirForTest(t)
	seedPins(t, pinStorePathsIn(shared))
	chmodPinStoreForTest(t, shared, 0o777)
	private := dataDirForTest(t)
	link := filepath.Join(private, "state")
	if err := os.Symlink(shared, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	paths := pinStorePathsIn(link)

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationWritableByOthers)
}

// A sticky shared directory (/tmp, a volume made 1777) with a subdirectory
// of the node's own is the documented way to keep the store on a shared
// volume: it starts, with its pins.
func TestAPinStoreInAnOwnSubdirectoryOfAStickySharedDirectoryStarts(t *testing.T) {
	t.Parallel()
	shared := dataDirForTest(t)
	chmodPinStoreForTest(t, shared, 0o777|fs.ModeSticky)
	own := filepath.Join(shared, "corsa")
	if err := os.Mkdir(own, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	paths := pinStorePathsIn(own)
	seedPins(t, paths)

	store, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	if !store.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the restart lost a pin")
	}
}

// A data directory this process may not write into (a root 0755 directory
// for a non-root node) is not an unsafe location — nobody else can write it
// either — but its first write fails with an error that says so, instead of
// a bare "permission denied".
func TestTheFirstWriteIntoADirectoryTheNodeCannotWriteSaysSo(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	fsprobe.RequireWriteRefused(t, dir)

	store, journal := openPinStore(t, pinStorePathsIn(dir), newStoreClock())
	requireStarts(t, store)
	err := store.noteProvenInbound(domain.PeerIdentity{4})
	if err == nil {
		t.Fatal("a write into a read-only directory succeeded")
	}
	failures := journal.entries(t, "pin_store_directory_not_writable")
	if len(failures) != 1 {
		t.Fatalf("pin_store_directory_not_writable logged %d times, want once", len(failures))
	}
	reason, _ := failures[0]["error"].(string)
	if !strings.Contains(reason, "owner") || !strings.Contains(reason, "mode") || strings.Contains(reason, dir) {
		t.Fatalf("the error does not name owner and mode, or names the path: %q", reason)
	}
}
