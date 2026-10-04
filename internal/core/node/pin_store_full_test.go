package node

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// pin_store_full_test.go pins the owner's decision of 2026-10-03 on a full pin
// store (С-5 clarified): existing protection is never lost, a NEW identity is
// not promised a pin — and a v2 session whose mandatory pin could not be
// stored is not established, fails with pin_store_full, and is not followed
// by an automatic v1 fallback in the same attempt.

// fillPinStore pins fake identities until the store is at its bound, and puts
// them on disk.
func fillPinStore(t *testing.T, store *secureSessionStore) {
	t.Helper()
	store.mu.Lock()
	for i := 0; len(store.identities) < maxPinnedIdentities; i++ {
		store.identities[domain.PeerIdentity{0xa0, byte(i), byte(i >> 8), byte(i >> 16)}] = store.clock()
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
	store := loadSecureSessionStore(path, clock.read)
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
		if err := store.noteProvenOutbound(endpoint, newcomer, false); !errors.Is(err, errPinStoreFull) {
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
	if err := store.noteProvenOutbound("198.51.100.10:64646", honest, false); err != nil {
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
	restarted := loadSecureSessionStore(path, clock.read)
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
	waitForConditionMsg(t, 10*time.Second, "the refusal was never counted", func() bool {
		return listener.SecureSessionStoreStats().PinRefusalsStoreFull > 0
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v2, v1 := authenticatedInbound(listener); v2 > 0 || v1 > 0 {
			t.Fatalf("a session without a stored pin was established: v2=%d v1=%d", v2, v1)
		}
		if _, v1 := marksOf(newcomer); v1 > 0 {
			t.Fatal("the dialler fell back to v1 after the pin was refused")
		}
		time.Sleep(50 * time.Millisecond)
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
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, func(svc *Service) {
		fillPinStore(t, svc.secureSessions.store)
	})
	defer stopDialer()
	listenerID := peerIdentityOf(t, listener)

	waitForConditionMsg(t, 10*time.Second, "the dialler never counted the refusal", func() bool {
		return dialer.SecureSessionStoreStats().PinRefusalsStoreFull > 0
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v2, legacy := claimsOf(dialer, listenerID); v2 > 0 || legacy > 0 {
			t.Fatalf("the dialler established a session without a stored pin: v2=%d v1=%d", v2, legacy)
		}
		if _, v1 := authenticatedInbound(listener); v1 > 0 {
			t.Fatal("the dialler fell back to v1 after its pin was refused")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
