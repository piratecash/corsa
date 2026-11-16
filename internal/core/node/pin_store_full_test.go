package node

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// A full pin store refuses a new permanent pin and keeps its previous pins.
// Transport tests use operator-approved identities so the peer needs a pin.
// fillPinStore pins fake identities until the store is at its bound, and puts
// them on disk.
func fillPinStore(t *testing.T, store *secureSessionStore) {
	t.Helper()
	store.mu.Lock()
	for i := 0; len(store.identities) < maxPinnedIdentities; i++ {
		store.identities[domain.PeerIdentity{0xa0, byte(i), byte(i >> 8), byte(i >> 16)}] = identityPinEntry{pinnedAt: store.now()}
		store.changes++
	}
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatalf("persist the full store: %v", err)
	}
}

func TestAFullPinStoreRefusesANewIdentityAndKeepsEveryExistingOne(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	honest := domain.PeerIdentity{1}
	if err := store.noteProvenInbound(honest); err != nil {
		t.Fatalf("pin the honest identity: %v", err)
	}
	fillPinStore(t, store)

	newcomer := domain.PeerIdentity{0xfe, 0xfe}
	endpoint := domain.PeerAddress("198.51.100.9:64646")
	for attempt := range 2 { // the first try and a reconnect
		if err := store.noteProvenInbound(newcomer); !errors.Is(err, errPinStoreFull) {
			t.Fatalf("attempt %d, accepted: err = %v, want pin_store_full", attempt, err)
		}
		if _, err := store.noteProvenOutbound(dialledAt(endpoint), newcomer, endpointProtectionNone()); !errors.Is(err, errPinStoreFull) {
			t.Fatalf("attempt %d, dialled: err = %v, want pin_store_full", attempt, err)
		}
		if store.identityPinned(newcomer) {
			t.Fatal("a pin was added past the bound")
		}
		if store.endpointRequiresV2(endpoint) {
			t.Fatal("a refused session left an endpoint binding behind")
		}
	}

	// Existing protection keeps being served.
	if err := store.noteProvenInbound(honest); err != nil {
		t.Fatalf("an identity pinned before the store filled up was refused: %v", err)
	}
	if _, err := store.noteProvenOutbound(dialledAt("198.51.100.10:64646"), honest, endpointProtectionNone()); err != nil {
		t.Fatalf("a dial to an identity pinned before the store filled up was refused: %v", err)
	}

	stats := store.stats()
	if !stats.Full || stats.PinnedIdentities != maxPinnedIdentities || stats.PinCapacity != maxPinnedIdentities {
		t.Fatalf("stats = %+v, want a full store at its bound", stats)
	}
	if stats.PinRefusalsStoreFull != 4 {
		t.Fatalf("pin_store_full refusals = %d, want 4", stats.PinRefusalsStoreFull)
	}

	// A restart loses nothing that was protected, and does not make room.
	restarted := loadSecureSessionStore(path, clock.trusted())
	if !restarted.identityPinned(honest) {
		t.Fatal("an existing pin was lost across a restart")
	}
	if restarted.identityPinned(newcomer) {
		t.Fatal("a refused identity came back pinned after a restart")
	}
	if err := restarted.noteProvenInbound(newcomer); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("after a restart: err = %v, want pin_store_full", err)
	}
}

// A new approved identity is refused by a full listener. A peer pinned
// earlier can still reconnect using a verified TLS session.
func TestANodeWithAFullPinStoreRefusesANewIdentitysSession(t *testing.T) {
	t.Parallel()
	newID, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	knownID, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	address := freeAddress(t)
	pinnedEarlier := domain.PeerIdentityFromWire(knownID.Address)
	listener, stopListener := startTestNodeWithSetup(t, config.Node{
		ListenAddress:    address,
		BootstrapPeers:   []string{},
		ReservedPinPeers: []string{newID.Address},
		Type:             domain.NodeTypeFull,
	}, withoutOwnDials(func(svc *Service) {
		if err := svc.secureSessions.store.noteProvenInbound(pinnedEarlier); err != nil {
			t.Fatalf("pin the known peer before filling: %v", err)
		}
		fillPinStore(t, svc.secureSessions.store)
	}))
	defer stopListener()

	startDialer := func(id *identity.Identity) (*Service, func()) {
		return startTestNodeAs(t, config.Node{
			ListenAddress:  freeAddress(t),
			BootstrapPeers: []string{normalizeAddress(address)},
			Type:           domain.NodeTypeFull,
		}, id, nil)
	}
	_, stopNewcomer := startDialer(newID)
	defer stopNewcomer()
	newcomer := domain.PeerIdentityFromWire(newID.Address)
	waitForConditionMsg(t, 10*time.Second, "approved peer did not attempt a session", func() bool {
		return listener.SecureSessionStoreStats().PinRefusalsStoreFull > 0 ||
			listener.identityHasProvenConnection(newcomer)
	})
	if listener.SecureSessionStoreStats().PinRefusalsStoreFull == 0 || listener.identityHasProvenConnection(newcomer) {
		t.Fatal("the full listener admitted the approved peer without storing its pin")
	}
	if listener.secureSessions.store.identityPinned(newcomer) {
		t.Fatal("the full listener added a permanent pin past the limit")
	}

	_, stopKnown := startDialer(knownID)
	defer stopKnown()
	waitForConditionMsg(t, 10*time.Second, "an identity pinned before the store filled up could not connect", func() bool {
		return listener.identityHasProvenConnection(pinnedEarlier)
	})
}

// The dialling side also refuses an approved identity's first permanent
// pin at the hard limit and waits for room.
func TestADiallerWithAFullPinStoreDoesNotEstablishOrFallBack(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startTestNodeWithSetup(t, config.Node{
		ListenAddress:    freeAddress(t),
		BootstrapPeers:   []string{normalizeAddress(address)},
		ReservedPinPeers: []string{listener.Address()},
		Type:             domain.NodeTypeFull,
	}, func(svc *Service) { fillPinStore(t, svc.secureSessions.store) })
	defer stopDialer()

	// Refused by the connection manager's own attempts, the later ones after
	// the manager reacted to the first. A full pin store is this node's own
	// limit, so after the first refusal the
	// address waits for room (the store stays full here) and its next
	// attempts are held before any socket, not refused again.
	breached := func() bool {
		return dialer.SessionOutcomeStats().Succeeded > 0
	}
	awaitRepeatedRefusal(t, "the pin_store_full refusal, then the wait for room",
		refusalThenHolds(dialer, func(stats domain.SecureSessionStoreStats) uint64 { return stats.PinRefusalsStoreFull }),
		breached)
	if outcomes := dialer.SessionOutcomeStats(); outcomes.Succeeded > 0 {
		t.Fatalf("the dialler established a session without a stored pin: %d of %d attempts succeeded", outcomes.Succeeded, outcomes.Attempts)
	}
}
