package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// secure_session_store_location_test.go pins how the store reacts to an
// unsafe location (docs/protocol/session_v2.md, "Downgrade protection"): the
// location is audited once, when the store is loaded; a violation is a loud
// warning and the pin_store_unsafe flag — the node keeps working and keeps
// writing, since refusing to start is not this build's reaction — and
// neither the warning nor the flag names a path.

func loadJournalledStore(t *testing.T, path string) (*secureSessionStore, *storeJournal) {
	t.Helper()
	journal := &storeJournal{}
	return loadSecureSessionStoreWith(path, newStoreClock().trusted(), zerolog.New(journal)), journal
}

// assertUnsafeAndWorking is the whole reaction: flagged, warned once without
// the path, and otherwise the same store — readable, pinning, writing.
func assertUnsafeAndWorking(t *testing.T, store *secureSessionStore, journal *storeJournal, dir string, violation secretfile.LocationViolation) {
	t.Helper()
	stats := store.stats()
	if !stats.LocationUnsafe {
		t.Fatal("pin_store_unsafe is not set for an unsafe location")
	}
	if stats.Unreadable {
		t.Fatal("an unsafe location was treated as an unreadable store: v1 would be refused for every peer")
	}
	warnings := journal.entries(t, "pin_store_unsafe")
	if len(warnings) != 1 {
		t.Fatalf("pin_store_unsafe logged %d times, want once at load", len(warnings))
	}
	if warnings[0]["level"] != "warn" || warnings[0]["violation"] != string(violation) {
		t.Fatalf("warning = %v, want level warn and violation %s", warnings[0], violation)
	}
	journal.mu.Lock()
	logged := journal.buf.String()
	journal.mu.Unlock()
	if strings.Contains(logged, dir) {
		t.Fatalf("the warning names the store's path: %s", logged)
	}

	id := domain.PeerIdentity{3}
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatalf("a store in an unsafe location stopped writing: %v", err)
	}
	if !store.identityPinned(id) {
		t.Fatal("a store in an unsafe location stopped pinning")
	}
}

func TestAPinStoreInASafeLocationIsNotFlagged(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	first, _ := loadJournalledStore(t, path)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	store, journal := loadJournalledStore(t, path)
	if store.stats().LocationUnsafe {
		t.Fatal("pin_store_unsafe is set for a file this node wrote into a private directory")
	}
	if warnings := journal.entries(t, "pin_store_unsafe"); len(warnings) != 0 {
		t.Fatalf("a safe location was warned about: %v", warnings)
	}
}

// A symlink in place of the store file is reported and the store still
// loads through it: until refusing to start is decided, the reaction is the
// warning and the flag, not a lost store.
func TestAPinStoreFileThatIsASymlinkIsFlaggedAndKeepsWorking(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	dir := t.TempDir()
	original := filepath.Join(dir, "elsewhere.json")
	seed, _ := loadJournalledStore(t, original)
	if err := seed.noteProvenInbound(domain.PeerIdentity{2}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	path := filepath.Join(dir, "secure-sessions-64646.json")
	if err := os.Symlink("elsewhere.json", path); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	store, journal := loadJournalledStore(t, path)
	if !store.identityPinned(domain.PeerIdentity{2}) {
		t.Fatal("the store behind the link was not loaded")
	}
	assertUnsafeAndWorking(t, store, journal, dir, secretfile.ViolationSymlink)
}

// A memory-only store has no location to audit.
func TestAMemoryOnlyStoreIsNotFlagged(t *testing.T) {
	t.Parallel()
	store, journal := loadJournalledStore(t, "")
	if store.stats().LocationUnsafe || len(journal.entries(t, "pin_store_unsafe")) != 0 {
		t.Fatal("a store with no file was audited")
	}
}
