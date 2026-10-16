package node

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// pin_store_marker_test.go pins the marker that tells a first start from a
// lost pin store (docs/protocol/session_v2.md, "The pin store marker"): the
// marker is written next to the identity only once a store file exists, so a
// crash at any point leaves either a first start or a store the marker can be
// rebuilt from; and a node whose marker says a store was used refuses to
// start without it — it never starts on an empty store that would let every
// pinned peer back to v1.

// pinStorePaths are the two files of one node: the store next to its peers
// file, the marker next to its identity.
type pinStorePaths struct {
	store  string
	marker string
}

func newPinStorePaths(t *testing.T) pinStorePaths {
	t.Helper()
	dir := t.TempDir()
	return pinStorePaths{
		store:  filepath.Join(dir, "secure-sessions-64646.json"),
		marker: filepath.Join(dir, "identity-64646.json"+pinStoreMarkerSuffix),
	}
}

// openPinStore is a node start on these paths with the real location audit.
func openPinStore(t *testing.T, paths pinStorePaths, clock *storeClock) (*secureSessionStore, *storeJournal) {
	t.Helper()
	journal := &storeJournal{}
	store := openSecureSessionStore(secureSessionStoreOptions{
		path:       paths.store,
		markerPath: paths.marker,
		clock:      clock.trusted(),
		logger:     zerolog.New(journal),
		audit:      secretfile.AuditLocation,
	})
	return store, journal
}

func requireStarts(t *testing.T, store *secureSessionStore) {
	t.Helper()
	if err := store.startupRefusal(); err != nil {
		t.Fatalf("startupRefusal = %v, want the node to start", err)
	}
}

// refusalOf is the text of the store's startup refusal, which must be one
// and carry reason.
func refusalOf(t *testing.T, store *secureSessionStore, reason domain.PinStoreRefusalReason) string {
	t.Helper()
	err := store.startupRefusal()
	if !errors.Is(err, domain.ErrPinStoreRefused) {
		t.Fatalf("startupRefusal = %v, want errors.Is ErrPinStoreRefused", err)
	}
	var refusal *PinStoreRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("startupRefusal = %T, want *PinStoreRefusalError", err)
	}
	if refusal.Reason != reason {
		t.Fatalf("refusal reason = %s, want %s: %v", refusal.Reason, reason, err)
	}
	return refusal.Error()
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	switch {
	case err == nil:
		return true
	case errors.Is(err, os.ErrNotExist):
		return false
	default:
		t.Fatalf("lstat: %v", err)
		return false
	}
}

func readMarker(t *testing.T, path string) pinStoreMarkerFile {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // the marker this test's store wrote
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	var marker pinStoreMarkerFile
	if err := json.Unmarshal(raw, &marker); err != nil {
		t.Fatalf("decode marker: %v", err)
	}
	return marker
}

func absPath(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	return abs
}

// requireFailClosed: a refused store treats every peer as pinned and writes
// nothing — the file it could not trust, or could not find, stays as it is.
func requireFailClosed(t *testing.T, store *secureSessionStore, paths pinStorePaths) {
	t.Helper()
	before, beforeErr := os.ReadFile(paths.store)
	if !store.identityPinned(domain.PeerIdentity{0x42}) {
		t.Fatal("a refused store let v1 through for an identity it does not know")
	}
	if err := store.noteProvenInbound(domain.PeerIdentity{0x43}); err != nil {
		t.Fatalf("a refused store failed a note instead of ignoring it: %v", err)
	}
	after, afterErr := os.ReadFile(paths.store)
	if (beforeErr == nil) != (afterErr == nil) || string(before) != string(after) {
		t.Fatal("a refused store wrote its file")
	}
}

func TestAFirstStartWritesTheMarkerWithTheFirstStoreWrite(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	store, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	if fileExists(t, paths.marker) {
		t.Fatal("the marker was written before any store file existed: a crash now would read as a lost store")
	}

	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if !fileExists(t, paths.store) || !fileExists(t, paths.marker) {
		t.Fatal("the first pin did not leave both the store and its marker")
	}
	if got := readMarker(t, paths.marker); got.Format != pinStoreMarkerFormat || got.Store != absPath(t, paths.store) {
		t.Fatalf("marker = %+v, want format %d vouching for %s", got, pinStoreMarkerFormat, paths.store)
	}
}

func TestARestartLoadsThePinsTheMarkerVouchesFor(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	restarted, _ := openPinStore(t, paths, clock)
	requireStarts(t, restarted)
	if !restarted.identityPinned(domain.PeerIdentity{1}) || restarted.identityPinned(domain.PeerIdentity{2}) {
		t.Fatal("the restart did not load exactly the stored pins")
	}
}

// The case the marker exists for: the store is gone and the node must not
// start as if it never had one.
func TestALostStoreIsARefusalNotAnEmptyStart(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if err := os.Remove(paths.store); err != nil {
		t.Fatalf("remove: %v", err)
	}

	restarted, _ := openPinStore(t, paths, clock)
	text := refusalOf(t, restarted, domain.PinStoreRefusalStoreMissing)
	if !strings.Contains(text, paths.store) || !strings.Contains(text, paths.marker) {
		t.Fatalf("the refusal does not name the store and the marker: %s", text)
	}
	requireFailClosed(t, restarted, paths)
	if fileExists(t, paths.store) || !fileExists(t, paths.marker) {
		t.Fatal("the refusal recreated the store or removed the marker")
	}
}

func TestADamagedStoreIsARefusalAndIsNotOverwritten(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if err := os.WriteFile(paths.store, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("damage: %v", err)
	}

	restarted, _ := openPinStore(t, paths, clock)
	text := refusalOf(t, restarted, domain.PinStoreRefusalStoreUnreadable)
	if !strings.Contains(text, paths.store) {
		t.Fatalf("the refusal does not name the store: %s", text)
	}
	requireFailClosed(t, restarted, paths)
}

// CORSA_SECURE_SESSION_STORE_PATH changed: the node looks for its store
// elsewhere, and the refusal says where it was last.
func TestAStoreLookedForUnderANewPathIsARefusalNamingTheOldOne(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	moved := pinStorePaths{store: filepath.Join(t.TempDir(), "pins.json"), marker: paths.marker}
	restarted, _ := openPinStore(t, moved, clock)
	if text := refusalOf(t, restarted, domain.PinStoreRefusalStoreMissing); !strings.Contains(text, absPath(t, paths.store)) || !strings.Contains(text, moved.store) {
		t.Fatalf("the refusal does not name both the old and the new store path: %s", text)
	}
	if !fileExists(t, paths.store) || fileExists(t, moved.store) {
		t.Fatal("the refusal touched the old store or created a new one")
	}
}

// A changed port with an explicit identity path: the identity and its marker
// stay, the default store path follows the port.
func TestAChangedPortWithAnExplicitIdentityPathIsARefusal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	identityPath := filepath.Join(dir, "keys", "node.json")
	if err := os.Mkdir(filepath.Dir(identityPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pathsOn := func(port string) pinStorePaths {
		cfg := config.Node{
			ListenAddress:  ":" + port,
			IdentityPath:   identityPath,
			PeersStatePath: filepath.Join(dir, "peers-"+port+".json"),
		}
		return pinStorePaths{store: secureSessionStorePath(cfg), marker: pinStoreMarkerPath(cfg)}
	}
	clock := newStoreClock()
	before, after := pathsOn("64646"), pathsOn("64647")
	if before.marker != after.marker || before.store == after.store {
		t.Fatalf("paths = %+v and %+v: the marker must follow the identity and the store the port", before, after)
	}
	first, _ := openPinStore(t, before, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	restarted, _ := openPinStore(t, after, clock)
	if text := refusalOf(t, restarted, domain.PinStoreRefusalStoreMissing); !strings.Contains(text, "secure-sessions-64646.json") {
		t.Fatalf("the refusal does not name the store of the old port: %s", text)
	}
}

// An operator who moved the store along with the configuration did what the
// refusal asks: the store loads and the marker follows it.
func TestAStoreMovedByTheOperatorLoadsAndTheMarkerFollows(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	moved := pinStorePaths{store: filepath.Join(filepath.Dir(paths.store), "secure-sessions-64647.json"), marker: paths.marker}
	if err := os.Rename(paths.store, moved.store); err != nil {
		t.Fatalf("rename: %v", err)
	}

	restarted, journal := openPinStore(t, moved, clock)
	requireStarts(t, restarted)
	if !restarted.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the moved store's pins were not loaded")
	}
	if got := readMarker(t, paths.marker); got.Store != absPath(t, moved.store) {
		t.Fatalf("marker vouches for %s, want the store's new path %s", got.Store, moved.store)
	}
	if len(journal.entries(t, "pin_store_marker_written")) != 1 {
		t.Fatal("the marker's move was not journalled")
	}
}

// A build before the marker left a store and no marker: the first start of
// this build keeps every pin and writes the marker.
func TestAStoreWithoutAMarkerIsMigratedWithItsPins(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	earlier := loadSecureSessionStore(paths.store, clock.trusted())
	if err := earlier.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if fileExists(t, paths.marker) {
		t.Fatal("a store without a marker path wrote a marker")
	}

	upgraded, journal := openPinStore(t, paths, clock)
	requireStarts(t, upgraded)
	if !upgraded.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the migration lost a pin")
	}
	if got := readMarker(t, paths.marker); got.Store != absPath(t, paths.store) {
		t.Fatalf("migrated marker = %+v, want it to vouch for %s", got, paths.store)
	}
	written := journal.entries(t, "pin_store_marker_written")
	if len(written) != 1 || written[0]["level"] != "warn" {
		t.Fatalf("the migration was journalled as %v, want one warning", written)
	}
}

// The store is the protection, the marker only says one was kept: a damaged
// marker never costs the pins.
func TestADamagedMarkerYieldsToAnIntactStore(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if err := os.WriteFile(paths.marker, []byte("garbage"), 0o600); err != nil {
		t.Fatalf("damage: %v", err)
	}

	restarted, _ := openPinStore(t, paths, clock)
	requireStarts(t, restarted)
	if !restarted.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("a damaged marker cost the pins")
	}
	if got := readMarker(t, paths.marker); got.Store != absPath(t, paths.store) {
		t.Fatalf("the damaged marker was not rewritten: %+v", got)
	}
}

// A marker of any shape says a store was kept; without the store that is a
// loss, not a first start.
func TestADamagedMarkerWithoutAStoreIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	if err := os.WriteFile(paths.marker, []byte("garbage"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	store, _ := openPinStore(t, paths, newStoreClock())
	if text := refusalOf(t, store, domain.PinStoreRefusalStoreMissing); !strings.Contains(text, paths.marker) {
		t.Fatalf("the refusal does not name the marker: %s", text)
	}
}

var errInjectedWrite = errors.New("injected write failure")

// A crash before the store lands: nothing was promised — the note failed —
// and the next start is a first start.
func TestAFailureBeforeTheStoreLandsLeavesAFirstStart(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	store, _ := openPinStore(t, paths, clock)
	store.writeFile = func([]byte) error { return errInjectedWrite }
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("note = %v, want the write failure", err)
	}
	if fileExists(t, paths.marker) {
		t.Fatal("the marker was written for a store that never landed")
	}

	restarted, _ := openPinStore(t, paths, clock)
	requireStarts(t, restarted)
	if restarted.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("a pin whose write failed was loaded")
	}
}

// A crash between the store and the marker: the store is on disk, the marker
// is not. The note is not reported durable, and the next start keeps the
// pins and writes the marker — never a refusal over a marker with no store.
func TestAFailureBetweenTheStoreAndTheMarkerKeepsThePins(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	store, _ := openPinStore(t, paths, clock)
	store.writeMarker = func([]byte) error { return errInjectedWrite }
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("note = %v, want the marker's write failure", err)
	}
	if !fileExists(t, paths.store) || fileExists(t, paths.marker) {
		t.Fatal("want the store written and the marker not")
	}

	restarted, _ := openPinStore(t, paths, clock)
	requireStarts(t, restarted)
	if !restarted.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the pin on disk was lost")
	}
	if !fileExists(t, paths.marker) {
		t.Fatal("the restart did not write the marker for the store it found")
	}
}

// A marker that failed once is written by the next write of the store.
func TestAFailedMarkerWriteIsRetriedWithTheNextStoreWrite(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	store, _ := openPinStore(t, paths, newStoreClock())
	realMarker := store.writeMarker
	store.writeMarker = func([]byte) error { return errInjectedWrite }
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err == nil {
		t.Fatal("a note whose marker failed was reported durable")
	}
	store.writeMarker = realMarker
	if err := store.noteProvenInbound(domain.PeerIdentity{2}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !fileExists(t, paths.marker) {
		t.Fatal("the marker was not retried")
	}
}

func TestAMemoryOnlyStoreHasNoMarker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := openSecureSessionStore(secureSessionStoreOptions{
		markerPath: filepath.Join(dir, "identity.json"+pinStoreMarkerSuffix),
		clock:      newStoreClock().trusted(),
		logger:     zerolog.Nop(),
		audit:      secretfile.AuditLocation,
	})
	requireStarts(t, store)
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("a memory-only store wrote %d files (%v)", len(entries), err)
	}
}

// The marker follows the identity file, with its override; a node with no
// identity file has none.
func TestThePinStoreMarkerSitsNextToTheIdentity(t *testing.T) {
	t.Parallel()
	if got := pinStoreMarkerPath(config.Node{IdentityPath: filepath.Join("data", "identity-64646.json")}); got != filepath.Join("data", "identity-64646.json"+pinStoreMarkerSuffix) {
		t.Fatalf("marker path = %s", got)
	}
	if got := pinStoreMarkerPath(config.Node{}); got != "" {
		t.Fatalf("marker path without an identity file = %q, want none", got)
	}
}

// The store was copied, not moved: the marker's old path still holds a store.
// Which of the two is current is the operator's call, not the node's — taking
// the new one silently could put a stale copy in charge.
func TestAStoreWhoseOldCopyRemainsIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	raw, err := os.ReadFile(paths.store)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	copied := pinStorePaths{store: filepath.Join(filepath.Dir(paths.store), "secure-sessions-64647.json"), marker: paths.marker}
	if err := os.WriteFile(copied.store, raw, 0o600); err != nil {
		t.Fatalf("copy: %v", err)
	}

	restarted, _ := openPinStore(t, copied, clock)
	text := refusalOf(t, restarted, domain.PinStoreRefusalTwoStores)
	if !strings.Contains(text, absPath(t, paths.store)) || !strings.Contains(text, copied.store) {
		t.Fatalf("the refusal does not name both stores: %s", text)
	}
	if got := readMarker(t, paths.marker); got.Store != absPath(t, paths.store) {
		t.Fatalf("the refusal rewrote the marker to %s", got.Store)
	}
	requireFailClosed(t, restarted, copied)
}

// One store reached under another spelling of its path (a link to its
// directory) is one store, not two: it loads and the marker follows.
func TestOneStoreUnderAnotherSpellingIsNotTwoStores(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Dir(paths.store), alias); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	aliased := pinStorePaths{store: filepath.Join(alias, filepath.Base(paths.store)), marker: paths.marker}

	restarted, _ := openPinStore(t, aliased, clock)
	requireStarts(t, restarted)
	if !restarted.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("the store under another spelling was not loaded")
	}
}

// A node configured with no store file at all (no peers file, no
// CORSA_SECURE_SESSION_STORE_PATH) under a marker that says it kept one:
// a memory-only store would be an empty start.
func TestAMemoryOnlyStoreUnderAMarkerIsARefusal(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	clock := newStoreClock()
	first, _ := openPinStore(t, paths, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}

	memoryOnly := openSecureSessionStore(secureSessionStoreOptions{
		markerPath: paths.marker,
		clock:      clock.trusted(),
		logger:     zerolog.Nop(),
		audit:      secretfile.AuditLocation,
	})
	if text := refusalOf(t, memoryOnly, domain.PinStoreRefusalStoreMissing); !strings.Contains(text, paths.marker) {
		t.Fatalf("the refusal does not name the marker: %s", text)
	}
	if !memoryOnly.identityPinned(domain.PeerIdentity{0x42}) {
		t.Fatal("a refused memory-only store let v1 through")
	}
}

// pinStorePathsOnPort are the default files of the node on port in dir: the
// identity, and so the marker, and the store all carry the port.
func pinStorePathsOnPort(dir, port string) pinStorePaths {
	cfg := config.Node{
		ListenAddress:  ":" + port,
		IdentityPath:   filepath.Join(dir, "identity-"+port+".json"),
		PeersStatePath: filepath.Join(dir, "peers-"+port+".json"),
	}
	return pinStorePaths{store: secureSessionStorePath(cfg), marker: pinStoreMarkerPath(cfg)}
}

// With the default paths a port not used before is a new node beside the
// existing ones: it starts, says loudly that it found their markers, and
// leaves their files exactly as they were. The old port, used again, is the
// old node with its pins.
func TestANewPortBesideAnExistingNodeStartsWithAWarning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	clock := newStoreClock()
	existing := pinStorePathsOnPort(dir, "64646")
	first, _ := openPinStore(t, existing, clock)
	if err := first.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	storeBefore, err := os.ReadFile(existing.store)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	markerBefore, err := os.ReadFile(existing.marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}

	newNode, journal := openPinStore(t, pinStorePathsOnPort(dir, "64647"), clock)
	requireStarts(t, newNode)
	if newNode.identityPinned(domain.PeerIdentity{1}) {
		t.Fatal("a new node took over the pins of another")
	}
	warnings := journal.entries(t, "pin_store_new_node_beside_other_nodes")
	if len(warnings) != 1 || warnings[0]["level"] != "warn" || warnings[0]["other_markers"] != float64(1) {
		t.Fatalf("the new node beside another was journalled as %v, want one warning counting one marker", warnings)
	}
	if err := newNode.noteProvenInbound(domain.PeerIdentity{2}); err != nil {
		t.Fatalf("note on the new node: %v", err)
	}
	if raw, err := os.ReadFile(existing.store); err != nil || string(raw) != string(storeBefore) {
		t.Fatal("the new node changed the other node's store")
	}
	if raw, err := os.ReadFile(existing.marker); err != nil || string(raw) != string(markerBefore) {
		t.Fatal("the new node changed the other node's marker")
	}

	again, againJournal := openPinStore(t, existing, clock)
	requireStarts(t, again)
	if !again.identityPinned(domain.PeerIdentity{1}) || again.identityPinned(domain.PeerIdentity{2}) {
		t.Fatal("the port used again did not load exactly its own pins")
	}
	if len(againJournal.entries(t, "pin_store_new_node_beside_other_nodes")) != 0 {
		t.Fatal("a node continuing on its own port was warned as a new one")
	}
}

// A first node in its directory has nothing to be warned about.
func TestAFirstNodeInItsDirectoryIsNotWarned(t *testing.T) {
	t.Parallel()
	store, journal := openPinStore(t, pinStorePathsOnPort(t.TempDir(), "64646"), newStoreClock())
	requireStarts(t, store)
	if len(journal.entries(t, "pin_store_new_node_beside_other_nodes")) != 0 {
		t.Fatal("a node alone in its directory was warned")
	}
}

// The marker's location is judged again right before the marker is written,
// so a directory that became unsafe — or was only created now — is never
// written into unjudged.
func TestTheMarkerIsNotWrittenIntoALocationThatFailsTheAuditAtWriteTime(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	loaded := false
	audit := func(path string) error {
		if loaded && path == paths.marker {
			return &secretfile.UnsafeLocationError{
				Component: secretfile.LocationComponent{Kind: secretfile.ComponentDirectory, LevelsAbove: 1},
				Violation: secretfile.ViolationWritableByOthers,
			}
		}
		return nil
	}
	store, _ := openPinStoreAudited(t, paths, audit)
	requireStarts(t, store)
	loaded = true

	if err := store.noteProvenInbound(domain.PeerIdentity{1}); !errors.Is(err, secretfile.ErrUnsafeLocation) {
		t.Fatalf("note = %v, want the marker's write refused by the audit", err)
	}
	if fileExists(t, paths.marker) {
		t.Fatal("the marker was written into a location that failed the audit")
	}
}

// The identity's directory does not exist yet (an SDK node given its key
// inline): the marker's directory is created, judged and written.
func TestTheMarkerDirectoryIsCreatedWhenMissing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	paths := pinStorePaths{
		store:  filepath.Join(dir, "secure-sessions-64646.json"),
		marker: filepath.Join(dir, "keys", "identity-64646.json"+pinStoreMarkerSuffix),
	}
	store, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	if err := store.noteProvenInbound(domain.PeerIdentity{1}); err != nil {
		t.Fatalf("note: %v", err)
	}
	if !fileExists(t, paths.marker) {
		t.Fatal("the marker was not written into the directory created for it")
	}
	restarted, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, restarted)
}
