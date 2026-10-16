//go:build !windows

package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// pin_store_marker_posix_test.go holds the marker case produced with mode
// bits: a directory next to the identity that this process may not write.

// A store from an earlier build whose marker cannot be written: the node
// refuses to start rather than run on, unable to tell a later loss of the
// store from a first start.
func TestAMarkerThatCannotBeWrittenForAnExistingStoreIsARefusal(t *testing.T) {
	t.Parallel()
	storeDir := t.TempDir()
	markerDir := filepath.Join(t.TempDir(), "identity")
	if err := os.Mkdir(markerDir, 0o500); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(markerDir, 0o700) })
	fsprobe.RequireWriteRefused(t, markerDir)
	paths := pinStorePaths{
		store:  filepath.Join(storeDir, "secure-sessions-64646.json"),
		marker: filepath.Join(markerDir, "identity-64646.json"+pinStoreMarkerSuffix),
	}
	clock := newStoreClock()
	earlier := loadSecureSessionStore(paths.store, clock.trusted())
	if err := earlier.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	store, _ := openPinStore(t, paths, clock)
	if text := refusalOf(t, store, domain.PinStoreRefusalMarkerUnwritable); !strings.Contains(text, paths.marker) {
		t.Fatalf("the refusal does not name the marker: %s", text)
	}
	requireFailClosed(t, store, paths)
}

// The directory created for the marker is private from its creation on.
func TestTheMarkerDirectoryIsCreatedPrivate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	paths := pinStorePaths{
		store:  filepath.Join(dir, "secure-sessions-64646.json"),
		marker: filepath.Join(dir, "keys", "identity-64646.json"+pinStoreMarkerSuffix),
	}
	store, _ := openPinStore(t, paths, newStoreClock())
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	info, err := os.Stat(filepath.Dir(paths.marker))
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("marker directory = %v (%v), want mode 0700", info, err)
	}
}
