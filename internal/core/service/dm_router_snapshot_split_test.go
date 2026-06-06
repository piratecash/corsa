package service

import (
	"reflect"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// newSnapshotTestRouter builds a minimal DMRouter wired enough for the
// snapshot composition path (client for MyAddress, cache for CacheReady),
// with the snapshot cache seeded the same way NewDMRouter does.
func newSnapshotTestRouter(t *testing.T) *DMRouter {
	t.Helper()
	db, cl := newTestChatLog(t)
	t.Cleanup(func() { _ = db.Close() })
	client := &DesktopClient{id: &identity.Identity{Address: "me"}, chatLog: cl}
	client.wireSubServices()

	r := &DMRouter{
		client:         client,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    make(chan struct{}),
		// statusMonitor nil → cachedNS stays zero; fine for these tests.
	}
	r.snapCache.Store(&routerSnapshotCache{gen: 0, snap: r.buildSnapshotLocked(0)})
	return r
}

// TestSnapshotSplit_StatusNotifyReusesDMCollections pins the core
// optimisation: a status-only notify must NOT rebuild the expensive DM
// collections (it reuses the cached map), yet must still reflect a live
// scalar change (sendStatus). Reusing the collection is asserted via map
// pointer identity — proving no re-copy happened on the status path.
func TestSnapshotSplit_StatusNotifyReusesDMCollections(t *testing.T) {
	r := newSnapshotTestRouter(t)

	// Populate the DM half via a sidebar notify.
	r.mu.Lock()
	r.peers["peer-1"] = &RouterPeerState{Unread: 2}
	r.peerOrder = []domain.PeerIdentity{"peer-1"}
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)

	snap1 := r.Snapshot()
	if len(snap1.Peers) != 1 {
		t.Fatalf("sidebar notify must populate Peers, got %d", len(snap1.Peers))
	}

	// Status-only notify after flipping a live scalar.
	r.mu.Lock()
	r.sendStatus = "message sent"
	r.mu.Unlock()
	r.notify(UIEventStatusUpdated)

	snap2 := r.Snapshot()
	if snap2.Generation <= snap1.Generation {
		t.Fatalf("notify must bump Generation: %d -> %d", snap1.Generation, snap2.Generation)
	}
	if len(snap2.Peers) != 1 {
		t.Fatalf("status notify dropped the cached DM peer (collections not reused): %d", len(snap2.Peers))
	}
	if snap2.SendStatus != "message sent" {
		t.Fatalf("status notify did not reflect the live sendStatus: %q", snap2.SendStatus)
	}
	// The Peers map must be the SAME backing map across both snapshots —
	// the status notify reused the cached DM half rather than rebuilding
	// it. This is the allocation-saving invariant made observable.
	if reflect.ValueOf(snap1.Peers).Pointer() != reflect.ValueOf(snap2.Peers).Pointer() {
		t.Fatal("status notify rebuilt the Peers map — DM collections must be reused on status-only updates")
	}
}

// TestSnapshotSplit_DMNotifyRebuildsCollections pins the inverse: a
// DM-data notify DOES refresh the collections (a new peer appears) and
// the rebuilt map is a distinct backing from the previous snapshot.
func TestSnapshotSplit_DMNotifyRebuildsCollections(t *testing.T) {
	r := newSnapshotTestRouter(t)

	r.mu.Lock()
	r.peers["peer-1"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"peer-1"}
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	snap1 := r.Snapshot()

	r.mu.Lock()
	r.peers["peer-2"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"peer-1", "peer-2"}
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	snap2 := r.Snapshot()

	if len(snap2.Peers) != 2 {
		t.Fatalf("DM notify must reflect the new peer, got %d", len(snap2.Peers))
	}
	if reflect.ValueOf(snap1.Peers).Pointer() == reflect.ValueOf(snap2.Peers).Pointer() {
		t.Fatal("DM notify must rebuild the Peers map, not reuse the stale one")
	}
	// snap1 must remain immutable — still one peer.
	if len(snap1.Peers) != 1 {
		t.Fatalf("prior snapshot mutated: expected 1 peer, got %d", len(snap1.Peers))
	}
}

// TestSnapshotSplit_BeepReusesBothHalves pins that a pure-signal beep
// notify rebuilds neither half (reuses both cached collections) while
// still bumping the generation.
func TestSnapshotSplit_BeepReusesBothHalves(t *testing.T) {
	r := newSnapshotTestRouter(t)

	r.mu.Lock()
	r.peers["peer-1"] = &RouterPeerState{}
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	snap1 := r.Snapshot()

	r.notify(UIEventBeep)
	snap2 := r.Snapshot()

	if snap2.Generation <= snap1.Generation {
		t.Fatal("beep must still bump Generation")
	}
	if reflect.ValueOf(snap1.Peers).Pointer() != reflect.ValueOf(snap2.Peers).Pointer() {
		t.Fatal("beep must reuse the cached DM collections")
	}
}
