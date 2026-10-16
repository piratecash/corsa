package node

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// pin_store_full_test.go pins the behaviour of a full pin store
// (docs/protocol/session_v2.md, "A full pin store"): existing protection is
// never lost, because evicting a pin would reopen v1 for an identity that
// proved v2; a NEW identity is not promised a pin — and a v2 session whose
// mandatory pin could not be stored is not established, fails with
// pin_store_full, and is not followed by an automatic v1 fallback in the same
// attempt.

// fillPinStore pins fake identities until the store is at its bound, and puts
// them on disk.
func fillPinStore(t *testing.T, store *secureSessionStore) {
	t.Helper()
	store.mu.Lock()
	for i := 0; len(store.identities) < maxPinnedIdentities; i++ {
		store.identities[domain.PeerIdentity{0xa0, byte(i), byte(i >> 8), byte(i >> 16)}] = store.now()
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

// The node: a new identity's v2 session to a node whose pin store is full is
// not established — not even briefly as a v2 session — and the dialler does
// not fall back to v1 in that attempt. An identity pinned earlier still
// connects over v2.
func TestANodeWithAFullPinStoreRefusesANewIdentitysSession(t *testing.T) {
	t.Parallel()
	var pinnedEarlier domain.PeerIdentity
	listener, address, stopListener := listenerNode(t, func(svc *Service) {
		fillPinStore(t, svc.secureSessions.store)
	})
	defer stopListener()

	newcomer, stopNewcomer := dialerNode(t, address, nil)
	defer stopNewcomer()
	awaitV2RefusalOrAuthentication(t, newcomer, listener, domain.PeerAddress(normalizeAddress(address)))
	if listener.SecureSessionStoreStats().PinRefusalsStoreFull == 0 {
		t.Fatal("the newcomer's attempt failed, but not on the listener's full pin store")
	}
	if nodeAuthenticatedIdentity(listener, peerIdentityOf(t, newcomer)) {
		t.Fatal("a session without a stored pin was established: the listener authenticated the newcomer")
	}
	// The listener speaks v2, so the newcomer can only go v1 towards it
	// through a v1 mark set in reaction to the refusal; the v2 handshake
	// itself ends without an error to fall back from. A mark set while the
	// failed setup is handled is there by the time the failure is recorded,
	// which is what the wait above waited for; a fallback that marked the
	// listener only later, on another attempt, would be missed here — no such
	// path exists.
	if _, v1 := marksOf(newcomer); v1 > 0 {
		t.Fatal("the dialler fell back to v1 after the pin was refused")
	}

	_, stopKnown := dialerNode(t, address, func(svc *Service) {
		pinnedEarlier = peerIdentityOf(t, svc)
		listener.secureSessions.store.mu.Lock()
		listener.secureSessions.store.identities[pinnedEarlier] = time.Now()
		listener.secureSessions.store.mu.Unlock()
	})
	defer stopKnown()
	waitForConditionMsg(t, 10*time.Second, "an identity pinned before the store filled up could not connect", func() bool {
		v2, _ := claimsOf(listener, pinnedEarlier)
		return v2 > 0
	})
}

// The dialling side: a node whose own pin store is full does not establish a
// v2 session with an identity it has not pinned, and does not fall back to v1.
func TestADiallerWithAFullPinStoreDoesNotEstablishOrFallBack(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, func(svc *Service) {
		fillPinStore(t, svc.secureSessions.store)
	})
	defer stopDialer()

	// Refused twice by the connection manager's own attempts, the second
	// after the manager reacted to the first: a fallback — inside an attempt
	// or through a v1 mark its reaction set — shows as a mark or a session.
	breached := func() bool {
		_, v1 := marksOf(dialer)
		return v1 > 0 || dialer.SessionOutcomeStats().Succeeded > 0
	}
	awaitRepeatedRefusal(t, "the pin_store_full refusal",
		func() uint64 { return dialer.SecureSessionStoreStats().PinRefusalsStoreFull },
		breached)
	if outcomes := dialer.SessionOutcomeStats(); outcomes.Succeeded > 0 {
		t.Fatalf("the dialler established a session without a stored pin: %d of %d attempts succeeded", outcomes.Succeeded, outcomes.Attempts)
	}
	if _, v1 := marksOf(dialer); v1 > 0 {
		t.Fatal("the dialler fell back to v1 after its pin was refused")
	}
}
