//go:build !windows

package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// secure_session_store_location_posix_test.go holds the store's reaction to
// the two locations that are produced with mode bits: a world-writable data
// directory and one this process may not write into. The reaction itself is
// the same on every platform (secure_session_store_location_test.go).

// The Docker case: the data directory is a volume mounted 0777.
func TestAPinStoreInAWorldWritableDirectoryIsFlaggedAndKeepsWorking(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	store, journal := loadJournalledStore(t, filepath.Join(dir, "secure-sessions-64646.json"))
	assertUnsafeAndWorking(t, store, journal, dir, secretfile.ViolationWritableByOthers)
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

	store, journal := loadJournalledStore(t, filepath.Join(dir, "secure-sessions-64646.json"))
	if store.stats().LocationUnsafe {
		t.Fatal("a directory only its owner may write was flagged as unsafe")
	}
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
