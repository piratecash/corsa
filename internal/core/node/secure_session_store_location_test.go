package node

import (
	"errors"
	"io/fs"
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
// store file and the marker are audited once, when the store is loaded, and
// a violation — or a location that cannot be inspected — is a refusal to
// start. The refusal names the path, the violation and the fix for the
// operator; the log line names neither the path nor anything else of the
// filesystem. Nothing is repaired, removed or moved: the pins stay as they
// are, and the store refuses v1 for every peer and writes nothing.

// openPinStoreAudited is a node start on paths whose audit is audit.
func openPinStoreAudited(t *testing.T, paths pinStorePaths, audit func(string) error) (*secureSessionStore, *storeJournal) {
	t.Helper()
	journal := &storeJournal{}
	store := openSecureSessionStore(secureSessionStoreOptions{
		path:       paths.store,
		markerPath: paths.marker,
		clock:      newStoreClock().trusted(),
		logger:     zerolog.New(journal),
		audit:      audit,
	})
	return store, journal
}

// seedPins writes one pin through a node that started on paths, so the store
// and its marker exist as a running node leaves them.
func seedPins(t *testing.T, paths pinStorePaths) []byte {
	t.Helper()
	store, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	raw, err := os.ReadFile(paths.store)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	return raw
}

// requireUnsafeRefusal is the whole reaction to an unsafe location: a typed
// refusal naming the file, its violation and the fix; one pathless log line;
// and the flag. It returns the refusal's text.
func requireUnsafeRefusal(t *testing.T, store *secureSessionStore, journal *storeJournal, file string, violation secretfile.LocationViolation) string {
	t.Helper()
	text := refusalOf(t, store, domain.PinStoreRefusalLocationUnsafe)
	err := store.startupRefusal()
	if !errors.Is(err, secretfile.ErrUnsafeLocation) {
		t.Fatalf("refusal %v does not carry the audit's ErrUnsafeLocation", err)
	}
	var refusal *PinStoreRefusalError
	if !errors.As(err, &refusal) || refusal.File != file {
		t.Fatalf("refusal %v does not name %s as its file", err, file)
	}
	for _, want := range []string{file, string(violation), processAccount()} {
		if !strings.Contains(text, want) {
			t.Fatalf("refusal does not say %q: %s", want, text)
		}
	}
	if !store.stats().LocationUnsafe {
		t.Fatal("pin_store_unsafe is not set for an unsafe location")
	}
	logged := journal.entries(t, "pin_store_refused")
	if len(logged) != 1 || logged[0]["level"] != "error" || logged[0]["reason"] != string(domain.PinStoreRefusalLocationUnsafe) || logged[0]["violation"] != string(violation) {
		t.Fatalf("pin_store_refused logged as %v, want one error with reason and violation", logged)
	}
	journal.mu.Lock()
	everything := journal.buf.String()
	journal.mu.Unlock()
	if strings.Contains(everything, filepath.Dir(file)) {
		t.Fatalf("the log names the file's directory: %s", everything)
	}
	return text
}

func TestAPinStoreInASafeLocationStarts(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seedPins(t, paths)

	store, journal := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	if store.stats().LocationUnsafe || len(journal.entries(t, "pin_store_refused")) != 0 {
		t.Fatal("a store this node wrote into a private directory was refused")
	}
	if !store.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the restart lost a pin")
	}
}

// A symlink in place of the store file decides where the pins are read
// from; the link is refused, not followed, and its target is left alone.
func TestAPinStoreFileThatIsASymlinkIsARefusal(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	original := newPinStorePaths(t)
	seed := seedPins(t, original)
	paths := pinStorePaths{store: filepath.Join(filepath.Dir(original.store), "secure-sessions-64647.json"), marker: original.marker}
	if err := os.Symlink(filepath.Base(original.store), paths.store); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationSymlink)
	requireFailClosed(t, store, paths)
	if raw, err := os.ReadFile(original.store); err != nil || string(raw) != string(seed) {
		t.Fatal("the store behind the link was changed")
	}
}

func TestAPinStoreMarkerThatIsASymlinkIsARefusal(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	paths := newPinStorePaths(t)
	seedPins(t, paths)
	elsewhere := filepath.Join(filepath.Dir(paths.marker), "elsewhere.json")
	if err := os.Rename(paths.marker, elsewhere); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := os.Symlink("elsewhere.json", paths.marker); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	store, journal := openPinStore(t, paths, newStoreClock())
	requireUnsafeRefusal(t, store, journal, paths.marker, secretfile.ViolationSymlink)
}

// A component owned by another user cannot be produced without privileges a
// test does not have; the audit's verdict is synthetic here, as in the
// secretfile tests of the judgement itself.
func TestAPinStoreOwnedByAnotherUserIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seed := seedPins(t, paths)
	foreign := func(path string) error {
		if path != paths.store {
			return nil
		}
		return &secretfile.UnsafeLocationError{
			Component: secretfile.LocationComponent{Kind: secretfile.ComponentFile},
			Violation: secretfile.ViolationForeignOwner,
			Owner:     "uid 1234",
			Mode:      0o600,
		}
	}

	store, journal := openPinStoreAudited(t, paths, foreign)
	text := requireUnsafeRefusal(t, store, journal, paths.store, secretfile.ViolationForeignOwner)
	if !strings.Contains(text, "uid 1234") || !strings.Contains(text, "chown") {
		t.Fatalf("the refusal does not name the foreign owner and how to fix it: %s", text)
	}
	requireFailClosed(t, store, paths)
	if raw, err := os.ReadFile(paths.store); err != nil || string(raw) != string(seed) {
		t.Fatal("the refused store's pins were changed")
	}
}

// The marker's location is audited like the store's.
func TestAPinStoreMarkerInAnUnsafeLocationIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seedPins(t, paths)
	unsafeMarker := func(path string) error {
		if path != paths.marker {
			return nil
		}
		return &secretfile.UnsafeLocationError{
			Component: secretfile.LocationComponent{Kind: secretfile.ComponentDirectory, LevelsAbove: 1},
			Violation: secretfile.ViolationWritableByOthers,
			Owner:     "uid 0",
			Mode:      fs.ModeDir | 0o777,
		}
	}

	store, journal := openPinStoreAudited(t, paths, unsafeMarker)
	requireUnsafeRefusal(t, store, journal, paths.marker, secretfile.ViolationWritableByOthers)
}

// A location the audit could not inspect is not taken for a safe one.
func TestAPinStoreLocationThatCannotBeInspectedIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	seedPins(t, paths)
	uninspectable := errors.New("injected: this filesystem reports no owner")

	store, _ := openPinStoreAudited(t, paths, func(string) error { return uninspectable })
	text := refusalOf(t, store, domain.PinStoreRefusalLocationUnverifiable)
	if !errors.Is(store.startupRefusal(), uninspectable) || !strings.Contains(text, paths.store) {
		t.Fatalf("refusal = %s, want it to carry the audit's error and name the store", text)
	}
	requireFailClosed(t, store, paths)
}

// A memory-only store has no location to audit.
func TestAMemoryOnlyStoreIsNotAudited(t *testing.T) {
	t.Parallel()
	audited := false
	store := openSecureSessionStore(secureSessionStoreOptions{
		clock:  newStoreClock().trusted(),
		logger: zerolog.Nop(),
		audit:  func(string) error { audited = true; return nil },
	})
	requireStarts(t, store)
	if audited || store.stats().LocationUnsafe {
		t.Fatal("a store with no file was audited")
	}
}
