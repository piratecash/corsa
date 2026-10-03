package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type storeClock struct{ now time.Time }

func (c *storeClock) read() time.Time { return c.now }

func newStoreClock() *storeClock { return &storeClock{now: time.Unix(1780000000, 0)} }

func TestTheDowngradeProtectionSurvivesARestart(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}

	first := loadSecureSessionStore(path, clock.read)
	if err := first.noteProvenOutbound(endpoint, id, false); err != nil {
		t.Fatalf("note: %v", err)
	}

	restarted := loadSecureSessionStore(path, clock.read)
	if !restarted.identityPinned(id) || !restarted.endpointRequiresV2(endpoint) {
		t.Fatal("the protection was lost across a restart")
	}
	if restarted.identityPinned(domain.PeerIdentity{8}) || restarted.endpointRequiresV2("192.0.2.2:64646") {
		t.Fatal("a restart protected what was never proven")
	}
}

// A full store refuses NEW protection and keeps every existing one: an
// attacker with fresh identities must not be able to push an honest pin out.
func TestAFullStoreRefusesNewPinsAndKeepsTheOld(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().read)
	for i := range maxPinnedIdentities {
		_ = store.noteProvenInbound(domain.PeerIdentity{byte(i), byte(i >> 8), byte(i >> 16), 1})
	}
	honest := domain.PeerIdentity{0, 0, 0, 1}
	_ = store.noteProvenInbound(domain.PeerIdentity{0xff, 0xff, 0xff, 0xff})
	if !store.identityPinned(honest) {
		t.Fatal("an existing pin was dropped to make room")
	}
	if store.identityPinned(domain.PeerIdentity{0xff, 0xff, 0xff, 0xff}) {
		t.Fatal("a pin was added past the bound")
	}

	for i := range maxEndpointBindings {
		_ = store.noteProvenOutbound(domain.PeerAddress("192.0.2.1:"+string(rune('a'+i%26))+string(rune(i))), honest, false)
	}
	first := domain.PeerAddress("192.0.2.1:a\x00")
	_ = store.noteProvenOutbound("198.51.100.1:64646", honest, false)
	if !store.endpointRequiresV2(first) {
		t.Fatal("a live endpoint binding was dropped to make room")
	}
	if store.endpointRequiresV2("198.51.100.1:64646") {
		t.Fatal("a binding was added past the bound")
	}
}

// The endpoint binding lasts 30 days from the last v2 session (owner's
// decision); the identity pin does not expire.
func TestAnEndpointBindingExpiresAndAnIdentityPinDoesNot(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.read)
	endpoint, id := domain.PeerAddress("192.0.2.1:64646"), domain.PeerIdentity{7}
	_ = store.noteProvenOutbound(endpoint, id, false)

	clock.now = clock.now.Add(endpointBindingTTL - time.Second)
	if !store.endpointRequiresV2(endpoint) {
		t.Fatal("the binding expired early")
	}
	clock.now = clock.now.Add(time.Second)
	if store.endpointRequiresV2(endpoint) {
		t.Fatal("the binding outlived its 30 days")
	}
	if !store.identityPinned(id) {
		t.Fatal("the identity pin expired")
	}
}

// A store file that cannot be read fails closed: every peer is protected —
// v1 refused, v2 working — rather than none.
func TestAnUnreadableStoreRefusesV1ForEveryPeer(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	store := loadSecureSessionStore(path, newStoreClock().read)
	if !store.identityPinned(domain.PeerIdentity{9}) || !store.endpointRequiresV2("192.0.2.9:64646") {
		t.Fatal("an unreadable store opened v1 instead of refusing it")
	}
	_ = store.noteProvenInbound(domain.PeerIdentity{9})
	raw, err := os.ReadFile(path) //nolint:gosec // the file this test wrote
	if err != nil || string(raw) != "not json" {
		t.Fatal("an unreadable store was overwritten with this process's partial view")
	}
}

// A protection that could not be written is reported, kept in memory, and
// written by the next note even when that note changes nothing — so a
// failed write is not silently lost at the next restart.
func TestAFailedWriteIsReportedAndRetriedUntilItLands(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.read)
	id := domain.PeerIdentity{5}
	diskFull := errors.New("disk full")
	store.writeFile = func([]byte) error { return diskFull }

	if err := store.noteProvenInbound(id); !errors.Is(err, diskFull) {
		t.Fatalf("note with a failing disk = %v, want the write error", err)
	}
	if !store.identityPinned(id) {
		t.Fatal("the protection was dropped from memory because the disk failed")
	}
	store.writeFile = store.writeToDisk
	// The same identity again: nothing new to pin, but the earlier change is
	// still unwritten and must land now.
	if err := store.noteProvenInbound(id); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !loadSecureSessionStore(path, clock.read).identityPinned(id) {
		t.Fatal("the pin that failed to write was never written: a restart loses it")
	}
}

// A binding to a contact is protected: it does not expire. After 30 days a
// v1 answer from that endpoint — whatever identity it names — is still a
// downgrade. An ordinary binding expires (owner's decision С-15).
func TestAProtectedBindingDoesNotExpire(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.read)
	contactEndpoint, otherEndpoint := domain.PeerAddress("192.0.2.1:64646"), domain.PeerAddress("192.0.2.2:64646")
	_ = store.noteProvenOutbound(contactEndpoint, domain.PeerIdentity{7}, true)
	_ = store.noteProvenOutbound(otherEndpoint, domain.PeerIdentity{8}, false)

	clock.now = clock.now.Add(endpointBindingTTL + 24*time.Hour)
	if !store.endpointRequiresV2(contactEndpoint) {
		t.Fatal("a protected binding expired after 30 days")
	}
	if store.endpointRequiresV2(otherEndpoint) {
		t.Fatal("an ordinary binding outlived its 30 days")
	}
	// A later ordinary note on the protected endpoint never lowers it.
	_ = store.noteProvenOutbound(contactEndpoint, domain.PeerIdentity{9}, false)
	clock.now = clock.now.Add(endpointBindingTTL + 24*time.Hour)
	if !store.endpointRequiresV2(contactEndpoint) {
		t.Fatal("a protected binding was lowered to an expiring one")
	}
}
