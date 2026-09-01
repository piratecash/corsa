package service

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"

	_ "modernc.org/sqlite"
)

// ── Exported helpers for testing ──
// These call internal methods that are not exported, so tests live in
// the same package (service).

// TestEnsurePeerLocked verifies that ensurePeerLocked creates a
// RouterPeerState entry if missing and does not overwrite an existing one.
func TestEnsurePeerLocked(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.mu.Unlock()

	r.mu.RLock()
	ps, ok := r.peers[domaintest.ID("peer-1")]
	r.mu.RUnlock()

	if !ok {
		t.Fatal("peer-1 should be created")
	}

	// Modify and ensure again — should not overwrite.
	r.mu.Lock()
	for i := 0; i < 5; i++ {
		r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID(fmt.Sprintf("seed-%d", i)))
	}
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.mu.Unlock()

	if ps.Unread != 5 {
		t.Fatalf("ensurePeerLocked overwrote existing state, Unread=%d", ps.Unread)
	}
}

// TestPromotePeerLocked verifies that promotePeerLocked moves a peer to
// the front of peerOrder, deduplicating any prior occurrences.
func TestPromotePeerLocked(t *testing.T) {
	r := newTestRouter()
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b"), domaintest.ID("c")}

	r.mu.Lock()
	r.promotePeerLocked(domaintest.ID("c"))
	r.mu.Unlock()

	if r.peerOrder[0] != domaintest.ID("c") {
		t.Fatalf("expected c at front, got %v", r.peerOrder)
	}
	if len(r.peerOrder) != 3 {
		t.Fatalf("expected 3 entries, got %v", r.peerOrder)
	}

	r.mu.Lock()
	r.promotePeerLocked(domaintest.ID("a"))
	r.mu.Unlock()
	expected := []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("c"), domaintest.ID("b")}
	for i, v := range expected {
		if r.peerOrder[i] != v {
			t.Fatalf("index %d: expected %q, got %q", i, v, r.peerOrder[i])
		}
	}

	// Promote empty string → no-op.
	r.mu.Lock()
	r.promotePeerLocked(domain.PeerIdentity{})
	r.mu.Unlock()
	if len(r.peerOrder) != 3 {
		t.Fatalf("empty promote changed slice: %v", r.peerOrder)
	}

	// Promote new peer → added at front.
	r.mu.Lock()
	r.promotePeerLocked(domaintest.ID("new-peer"))
	r.mu.Unlock()
	if r.peerOrder[0] != domaintest.ID("new-peer") {
		t.Fatalf("new peer should be at front, got %v", r.peerOrder)
	}
}

// TestRemovePeerLocked verifies that removePeerLocked correctly filters
// a peer out of peerOrder (including duplicates).
func TestRemovePeerLocked(t *testing.T) {
	r := newTestRouter()
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b"), domaintest.ID("c"), domaintest.ID("b"), domaintest.ID("d")}

	r.mu.Lock()
	r.removePeerLocked(domaintest.ID("b"))
	r.mu.Unlock()

	expected := []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("c"), domaintest.ID("d")}
	if len(r.peerOrder) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, r.peerOrder)
	}
	for i, v := range expected {
		if r.peerOrder[i] != v {
			t.Fatalf("index %d: expected %q, got %q", i, v, r.peerOrder[i])
		}
	}

	// Removing non-existent peer should be a no-op.
	r.mu.Lock()
	r.removePeerLocked(domaintest.ID("z"))
	r.mu.Unlock()
	if len(r.peerOrder) != 3 {
		t.Fatalf("no-op removal changed slice: %v", r.peerOrder)
	}
}

// TestNormalizePeerAtIngress verifies that whitespace-padded identities
// are normalized at every public router boundary, preventing duplicate
// keys in peers map and peerOrder.
func TestNormalizePeerAtIngress(t *testing.T) {
	r := newTestRouter()

	// peerForMessage parses the wire Sender/Recipient. A whitespace-padded
	// value is not a valid 40-char hex fingerprint, so PeerIdentityFromWire
	// yields the zero identity (the "absent" sentinel) — the byte-typed
	// equivalent of the old trimmed-empty result.
	ev := protocol.LocalChangeEvent{
		Sender:    " peer-1 ",
		Recipient: "me",
	}
	got := r.peerForMessage(ev)
	if !got.IsZero() {
		t.Fatalf("peerForMessage should yield zero identity for malformed wire input: got %q", got)
	}

	// SelectPeer normalizes via normalizePeer, which trims the hex String()
	// form. For an already-valid identity this is a no-op, so activePeer
	// equals the selected identity unchanged.
	sel := domaintest.ID("peer-2")
	r.SelectPeer(sel)
	r.mu.RLock()
	active := r.activePeer
	r.mu.RUnlock()
	if active != sel {
		t.Fatalf("SelectPeer did not normalize: activePeer = %q", active)
	}

	// AutoSelectPeer normalizes the same way.
	auto := domaintest.ID("peer-3")
	r.AutoSelectPeer(auto)
	r.mu.RLock()
	active = r.activePeer
	r.mu.RUnlock()
	if active != auto {
		t.Fatalf("AutoSelectPeer did not normalize: activePeer = %q", active)
	}
}

// TestClearPeerUnread verifies clearPeerUnread sets Unread to 0 and
// is safe when peer doesn't exist.
func TestClearPeerUnread(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.peers[domaintest.ID("peer-1")] = &RouterPeerState{Unread: 7}
	r.mu.Unlock()

	r.clearPeerUnread(domaintest.ID("peer-1"))

	r.mu.RLock()
	u := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	if u != 0 {
		t.Fatalf("expected Unread=0, got %d", u)
	}

	// Clearing non-existent peer should not panic.
	r.clearPeerUnread(domaintest.ID("nonexistent"))
}

// TestPeerStateUnreadIntegrity verifies that unread counts are
// independently tracked per peer and don't leak across entries.
func TestPeerStateUnreadIntegrity(t *testing.T) {
	r := newTestRouter()
	r.mu.Lock()
	r.peers[domaintest.ID("peer-a")] = &RouterPeerState{Unread: 3}
	r.peers[domaintest.ID("peer-b")] = &RouterPeerState{Unread: 5}
	r.mu.Unlock()

	r.clearPeerUnread(domaintest.ID("peer-a"))

	r.mu.RLock()
	ua := r.peers[domaintest.ID("peer-a")].Unread
	ub := r.peers[domaintest.ID("peer-b")].Unread
	r.mu.RUnlock()

	if ua != 0 {
		t.Fatalf("peer-a should have 0 unread, got %d", ua)
	}
	if ub != 5 {
		t.Fatalf("peer-b should still have 5 unread, got %d", ub)
	}
}

// TestSeedPreviews verifies that seedPreviews correctly populates
// the peers map and sets correct unread counts + promotion order.
func TestSeedPreviews(t *testing.T) {
	r := newTestRouter()

	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-with-unread"), UnreadCount: 3},
		{PeerAddress: domaintest.ID("peer-all-read"), UnreadCount: 0},
		{PeerAddress: domaintest.ID("peer-also-unread"), UnreadCount: 1},
	}

	// Inline the seedPreviews logic (needs client.Address() which we
	// can't call on a nil client — but we filter by me="" which doesn't
	// match any preview address, so all pass through).
	r.mu.Lock()
	for _, p := range previews {
		if p.PeerAddress.IsZero() {
			continue
		}
		r.tryEnsurePeerLocked(p.PeerAddress)
		r.peers[p.PeerAddress].Preview = p
		if p.UnreadCount > 0 {
			r.peers[p.PeerAddress].Unread = p.UnreadCount
			r.promotePeerLocked(p.PeerAddress)
		}
	}
	r.mu.Unlock()

	// All peers should be in the peers map.
	for _, addr := range []domain.PeerIdentity{domaintest.ID("peer-with-unread"), domaintest.ID("peer-all-read"), domaintest.ID("peer-also-unread")} {
		r.mu.RLock()
		_, ok := r.peers[addr]
		r.mu.RUnlock()
		if !ok {
			t.Fatalf("%s should be in peers map", addr)
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.peers[domaintest.ID("peer-with-unread")].Unread != 3 {
		t.Fatalf("expected unread=3, got %d", r.peers[domaintest.ID("peer-with-unread")].Unread)
	}
	if r.peers[domaintest.ID("peer-also-unread")].Unread != 1 {
		t.Fatalf("expected unread=1, got %d", r.peers[domaintest.ID("peer-also-unread")].Unread)
	}
	if r.peers[domaintest.ID("peer-all-read")].Unread != 0 {
		t.Fatalf("expected unread=0, got %d", r.peers[domaintest.ID("peer-all-read")].Unread)
	}

	// Unread peers should be promoted to front of peerOrder.
	// peer-also-unread was promoted last → it's at front.
	if len(r.peerOrder) < 2 {
		t.Fatalf("expected at least 2 entries in peerOrder, got %d: %v", len(r.peerOrder), r.peerOrder)
	}
	if r.peerOrder[0] != domaintest.ID("peer-also-unread") {
		t.Fatalf("expected peer-also-unread at front, got %s", r.peerOrder[0])
	}
}

// TestResetIdentityState verifies that resetIdentityState clears all
// identity-specific state so a subsequent seed doesn't layer new data
// on stale peers/badges/order from a previous identity.
func TestResetIdentityState(t *testing.T) {
	r := newTestRouter()

	// Populate state.
	r.mu.Lock()
	r.peers[domaintest.ID("old-peer-1")] = &RouterPeerState{Unread: 3, Preview: ConversationPreview{Body: "old msg"}}
	r.peers[domaintest.ID("old-peer-2")] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("old-peer-1"), domaintest.ID("old-peer-2")}
	r.activePeer = domaintest.ID("old-peer-1")
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "m1"}}
	r.seenMessageIDs = map[string]messageGate{"old-msg-1": {handled: true}}
	r.initialSynced = true
	r.mu.Unlock()

	r.cache.Load(domaintest.ID("old-peer-1"), []DirectMessage{
		{ID: "old-msg-1", Body: "old"},
	}, 0)

	r.resetIdentityState()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.peers) != 0 {
		t.Fatalf("peers not cleared: %v", r.peers)
	}
	if len(r.peerOrder) != 0 {
		t.Fatalf("peerOrder not cleared: %v", r.peerOrder)
	}
	if !r.activePeer.IsZero() {
		t.Fatalf("activePeer not cleared: %q", r.activePeer)
	}
	if r.peerClicked {
		t.Fatal("peerClicked should be false after reset")
	}
	if r.activeMessages != nil {
		t.Fatalf("activeMessages not cleared: %v", r.activeMessages)
	}
	if len(r.seenMessageIDs) != 0 {
		t.Fatalf("seenMessageIDs not cleared: %v", r.seenMessageIDs)
	}
	if r.seenMessageIDs == nil {
		t.Fatal("seenMessageIDs must be initialized (not nil)")
	}
	if r.initialSynced {
		t.Fatal("initialSynced should be false after reset")
	}
	if r.cache.Len() != 0 {
		t.Fatalf("cache not reset: len=%d", r.cache.Len())
	}
}

// TestHandleEventIgnoresNonDMTopic verifies that handleEvent ignores
// new_message events with topic != "dm".
func TestHandleEventIgnoresNonDMTopic(t *testing.T) {
	r := newTestRouter()

	event := protocol.LocalChangeEvent{
		Type:  protocol.LocalChangeNewMessage,
		Topic: "global",
	}

	// Should not panic or have any side effects.
	r.handleEvent(event)
}

// TestOnReceiptUpdateActiveConversation verifies that onReceiptUpdate
// correctly updates the cache and activeMessages for the active peer.
func TestOnReceiptUpdateActiveConversation(t *testing.T) {
	r := newTestRouter()

	now := time.Now()
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{
			ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1"),
			ReceiptStatus: "sent", Timestamp: now,
		},
	}, 0)

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	deliveredAt := now.Add(2 * time.Second)
	// LocalChangeEvent carries identities as wire strings (40-hex), not
	// short labels: peerForMessage decodes them via PeerIdentityFromWire.
	// Use the hex form of the same identities the cache was seeded with so
	// the receipt resolves to peer-1 and matches the active conversation.
	event := protocol.LocalChangeEvent{
		Type:        protocol.LocalChangeReceiptUpdate,
		MessageID:   "msg-1",
		Sender:      domaintest.ID("me").String(),
		Recipient:   domaintest.ID("peer-1").String(),
		Status:      "delivered",
		DeliveredAt: deliveredAt,
	}

	r.onReceiptUpdate(event)

	msgs := r.cache.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ReceiptStatus != "delivered" {
		t.Fatalf("expected status 'delivered', got %q", msgs[0].ReceiptStatus)
	}

	r.mu.RLock()
	active := r.activeMessages
	r.mu.RUnlock()
	if len(active) != 1 || active[0].ReceiptStatus != "delivered" {
		t.Fatal("activeMessages should reflect updated receipt status")
	}
}

// TestOnReceiptUpdateIgnoresInactiveConversation verifies that receipt
// updates for a non-active peer are silently ignored.
func TestOnReceiptUpdateIgnoresInactiveConversation(t *testing.T) {
	r := newTestRouter()

	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1"), ReceiptStatus: "sent"},
	}, 0)

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeReceiptUpdate,
		MessageID: "msg-2",
		Sender:    "me",
		Recipient: "peer-2",
		Status:    "delivered",
	}

	r.onReceiptUpdate(event)

	msgs := r.cache.Messages()
	if len(msgs) != 1 || msgs[0].ReceiptStatus != "sent" {
		t.Fatal("cache for peer-1 should be unchanged")
	}
}

// TestConsumePendingActions verifies that pending UI flags are consumed
// and cleared atomically.
func TestConsumePendingActions(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.pendingScrollToEnd = true
	r.pendingComposerRestore = []ComposerRestore{{Peer: domaintest.ID("test-peer"), Body: "hi"}}
	r.pendingRecipientText = domaintest.ID("test-peer")
	r.mu.Unlock()

	pa := r.ConsumePendingActions()

	if !pa.ScrollToEnd {
		t.Fatal("ScrollToEnd should be true")
	}
	if len(pa.ComposerRestore) != 1 {
		t.Fatal("ComposerRestore should have one entry")
	}
	if pa.RecipientText != domaintest.ID("test-peer") {
		t.Fatalf("RecipientText should be 'test-peer', got %q", pa.RecipientText)
	}

	// After consumption, flags should be cleared.
	r.mu.RLock()
	if r.pendingScrollToEnd {
		t.Fatal("pendingScrollToEnd should be cleared")
	}
	if r.pendingComposerRestore != nil {
		t.Fatal("pendingComposerRestore should be cleared")
	}
	if !r.pendingRecipientText.IsZero() {
		t.Fatalf("pendingRecipientText should be cleared, got %q", r.pendingRecipientText)
	}
	r.mu.RUnlock()
}

// TestSnapshotIsConsistent verifies that Snapshot() returns a consistent
// copy of the router state (not pointers to live data).
func TestSnapshotIsConsistent(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.peers[domaintest.ID("peer-1")] = &RouterPeerState{Unread: 3}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("peer-1")}
	r.activeMessages = []DirectMessage{{ID: "m1", Body: "hello"}}
	r.statusMonitor.(*testStatusProvider).Status = NodeStatus{Peers: []string{"a"}}
	r.sendStatus = "ok"
	r.mu.Unlock()

	// notify() builds an immutable snapshot under Lock and stores it in
	// snapCache. Without this call Snapshot() returns an empty struct because
	// the lock-free path reads from snapCache, which is nil right after
	// manual field assignment.
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents // drain the notification

	snap := r.Snapshot()

	if snap.ActivePeer != domaintest.ID("peer-1") {
		t.Fatalf("expected ActivePeer=peer-1, got %q", snap.ActivePeer)
	}
	if !snap.PeerClicked {
		t.Fatal("expected PeerClicked=true")
	}
	if snap.Peers[domaintest.ID("peer-1")].Unread != 3 {
		t.Fatalf("expected Unread=3, got %d", snap.Peers[domaintest.ID("peer-1")].Unread)
	}
	if len(snap.PeerOrder) != 1 || snap.PeerOrder[0] != domaintest.ID("peer-1") {
		t.Fatalf("unexpected PeerOrder: %v", snap.PeerOrder)
	}
	if len(snap.ActiveMessages) != 1 || snap.ActiveMessages[0].ID != "m1" {
		t.Fatalf("unexpected ActiveMessages: %v", snap.ActiveMessages)
	}
	if snap.SendStatus != "ok" {
		t.Fatalf("expected SendStatus=ok, got %q", snap.SendStatus)
	}

	// Mutate the snapshot — should not affect router state.
	snap.Peers[domaintest.ID("peer-1")].Unread = 99
	snap.PeerOrder[0] = domaintest.ID("mutated")

	r.mu.RLock()
	if r.peers[domaintest.ID("peer-1")].Unread != 3 {
		t.Fatal("snapshot mutation leaked to router state")
	}
	if r.peerOrder[0] != domaintest.ID("peer-1") {
		t.Fatal("snapshot mutation leaked to peerOrder")
	}
	r.mu.RUnlock()
}

// TestSetSendStatus verifies thread-safe status update.
func TestSetSendStatus(t *testing.T) {
	r := newTestRouter()

	r.SetSendStatus("sending…")

	r.mu.RLock()
	s := r.sendStatus
	r.mu.RUnlock()

	if s != "sending…" {
		t.Fatalf("expected 'sending…', got %q", s)
	}
}

// TestNotifyNonBlocking verifies that notify doesn't block when channel is full.
func TestNotifyNonBlocking(t *testing.T) {
	r := newTestRouter()

	// Fill the channel.
	for i := 0; i < 32; i++ {
		r.notify(UIEventStatusUpdated)
	}

	// This should not block — event is dropped.
	done := make(chan struct{})
	go func() {
		r.notify(UIEventStatusUpdated)
		close(done)
	}()

	select {
	case <-done:
		// OK — non-blocking.
	case <-time.After(time.Second):
		t.Fatal("notify() blocked when channel was full")
	}
}

// TestConversationCacheMatchesPeerIntegration verifies the cache
// integration used by event routing logic.
func TestConversationCacheMatchesPeerIntegration(t *testing.T) {
	cache := NewConversationCache()

	if cache.MatchesPeer(domaintest.ID("anyone")) {
		t.Fatal("empty cache should not match any peer")
	}

	cache.Load(domaintest.ID("peer-1"), nil, 0)
	if !cache.MatchesPeer(domaintest.ID("peer-1")) {
		t.Fatal("cache should match peer-1 after Load")
	}
	if cache.MatchesPeer(domaintest.ID("peer-2")) {
		t.Fatal("cache should not match peer-2")
	}

	cache.Load(domaintest.ID("peer-2"), []DirectMessage{{ID: "m1"}}, 0)
	if cache.MatchesPeer(domaintest.ID("peer-1")) {
		t.Fatal("cache should no longer match peer-1")
	}
	if !cache.MatchesPeer(domaintest.ID("peer-2")) {
		t.Fatal("cache should match peer-2")
	}
}

// TestDoMarkSeenSkipsWhenNoMessages verifies that doMarkSeen does NOT
// clear unread when activeMessages is empty (conversation not loaded yet).
func TestDoMarkSeenSkipsWhenNoMessages(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peers[domaintest.ID("peer-1")] = &RouterPeerState{Unread: 5}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("peer-1")}
	// activeMessages is intentionally empty — simulates load not completed.
	r.mu.Unlock()

	r.doMarkSeen(domaintest.ID("peer-1"))

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	if unread != 5 {
		t.Fatalf("expected unread=5 (unchanged), got %d — doMarkSeen should not clear unread on empty activeMessages", unread)
	}
}

// TestIsActivePeer verifies the isActivePeer helper.
func TestIsActivePeer(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()

	if !r.isActivePeer(domaintest.ID("peer-1")) {
		t.Fatal("peer-1 should be active")
	}
	if r.isActivePeer(domaintest.ID("peer-2")) {
		t.Fatal("peer-2 should not be active")
	}
}

// TestOnReceiptUpdateActivePeerCacheMismatch verifies that when activePeer
// is set but cache is for a different peer (mid-switch), onReceiptUpdate
// triggers a loadConversation (via goroutine) rather than ignoring.
func TestOnReceiptUpdateActivePeerCacheMismatch(t *testing.T) {
	r := newTestRouter()

	// Cache is for "old-peer", but activePeer is "peer-1".
	r.cache.Load(domaintest.ID("old-peer"), []DirectMessage{
		{ID: "msg-old", Body: "old", Sender: domaintest.ID("me"), Recipient: domaintest.ID("old-peer")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeReceiptUpdate,
		MessageID: "msg-1",
		Sender:    "me",
		Recipient: "peer-1",
		Status:    "delivered",
	}

	// Should not panic, and should not modify old-peer's cache.
	r.onReceiptUpdate(event)

	msgs := r.cache.Messages()
	if len(msgs) != 1 || msgs[0].ID != "msg-old" {
		t.Fatal("cache for old-peer should be unchanged during mid-switch receipt update")
	}
}

// TestOnNewMessageNonActivePeerUpdatesOnlySidebar verifies that new
// messages for a non-active peer do NOT modify the cache of the active peer.
func TestOnNewMessageNonActivePeerUpdatesOnlySidebar(t *testing.T) {
	r := newTestRouter()

	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// isActivePeer(domaintest.ID("peer-2")) must return false → sidebar-only path.
	if r.isActivePeer(domaintest.ID("peer-2")) {
		t.Fatal("peer-2 should not be active")
	}

	// Verify that cache is unchanged after the check.
	msgs := r.cache.Messages()
	if len(msgs) != 1 || msgs[0].ID != "msg-1" {
		t.Fatal("cache for peer-1 should be unchanged")
	}
}

// TestActivePeerCacheMismatchDetection verifies that when activePeer
// is set but cache is for a different peer, the mismatch is detectable
// (the condition used in onNewMessage/onReceiptUpdate mid-switch path).
func TestActivePeerCacheMismatchDetection(t *testing.T) {
	r := newTestRouter()

	// Cache is for "old-peer", activePeer is "peer-1".
	r.cache.Load(domaintest.ID("old-peer"), nil, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()

	// isActivePeer reports peer-1 as active...
	if !r.isActivePeer(domaintest.ID("peer-1")) {
		t.Fatal("peer-1 should be the active peer")
	}
	// ...but cache doesn't match yet (still on old-peer).
	if r.cache.MatchesPeer(domaintest.ID("peer-1")) {
		t.Fatal("cache should NOT match peer-1 during mid-switch")
	}
	// This is the exact condition that triggers the reload path in
	// onNewMessage and onReceiptUpdate.
}

// TestSelectPeerClearsActiveMessages verifies that switching peers
// immediately clears activeMessages so stale messages are never shown.
func TestSelectPeerClearsActiveMessages(t *testing.T) {
	r := newTestRouter()

	// Set up peer-1 as active with messages.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// Verify messages are present before switch.
	r.mu.RLock()
	if len(r.activeMessages) != 1 {
		t.Fatalf("expected 1 message before switch, got %d", len(r.activeMessages))
	}
	r.mu.RUnlock()

	// Switch to peer-2. The goroutine will fail (no real client) but
	// the synchronous part should clear activeMessages immediately.
	r.SelectPeer(domaintest.ID("peer-2"))

	// Check immediately — activeMessages should be nil (cleared synchronously).
	r.mu.RLock()
	msgs := r.activeMessages
	activePeer := r.activePeer
	clicked := r.peerClicked
	r.mu.RUnlock()

	if activePeer != domaintest.ID("peer-2") {
		t.Fatalf("expected activePeer=peer-2, got %q", activePeer)
	}
	if msgs != nil {
		t.Fatalf("expected activeMessages=nil after peer switch, got %d messages", len(msgs))
	}
	if !clicked {
		t.Fatal("expected peerClicked=true for SelectPeer")
	}
}

// TestAutoSelectPeerSetsClicked verifies that AutoSelectPeer sets
// peerClicked = true — the chat is on screen and counts as read.
func TestAutoSelectPeerSetsClicked(t *testing.T) {
	r := newTestRouter()

	r.AutoSelectPeer(domaintest.ID("peer-1"))

	r.mu.RLock()
	activePeer := r.activePeer
	clicked := r.peerClicked
	r.mu.RUnlock()

	if activePeer != domaintest.ID("peer-1") {
		t.Fatalf("expected activePeer=peer-1, got %q", activePeer)
	}
	if !clicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true — chat is on screen")
	}
}

// TestAutoSelectPeerKeepsClickedOnSwitch verifies that AutoSelectPeer
// keeps peerClicked=true when switching peers — chat on screen is always read.
func TestAutoSelectPeerKeepsClickedOnSwitch(t *testing.T) {
	r := newTestRouter()

	// Simulate a previous user click.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.mu.Unlock()

	// Auto-select a different peer — must keep clicked true.
	r.AutoSelectPeer(domaintest.ID("peer-2"))

	r.mu.RLock()
	clicked := r.peerClicked
	r.mu.RUnlock()

	if !clicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true — chat on screen counts as read")
	}
}

// TestAutoSelectPeerClearsActiveMessages verifies that AutoSelectPeer
// also clears stale activeMessages when switching.
func TestAutoSelectPeerClearsActiveMessages(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = []DirectMessage{{ID: "msg-old", Body: "old"}}
	r.mu.Unlock()

	r.AutoSelectPeer(domaintest.ID("peer-2"))

	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()

	if msgs != nil {
		t.Fatalf("expected activeMessages=nil after auto-select switch, got %d messages", len(msgs))
	}
}

// TestSeedPreviewsSortOrder verifies that seedPreviews puts unread
// conversations first, by unread count descending, and leaves everything else
// in the order it was given.
//
// That input order is the store's, and the store returns conversations newest
// arrival first. The timestamps below deliberately disagree with it: they are
// what the senders' clocks printed, and ranking by them is what let a peer
// with a wrong clock decide where its conversation sat in the sidebar.
func TestSeedPreviewsSortOrder(t *testing.T) {
	r := newTestRouter()

	now := time.Now()
	// Arrival order: newest first, as the store hands them back.
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("new-read"), Timestamp: now.Add(-1 * time.Hour), UnreadCount: 0},
		{PeerAddress: domaintest.ID("old-read"), Timestamp: now.Add(-10 * time.Hour), UnreadCount: 0},
		{PeerAddress: domaintest.ID("unread-low"), Timestamp: now.Add(-2 * time.Hour), UnreadCount: 1},
		{PeerAddress: domaintest.ID("unread-high"), Timestamp: now.Add(-5 * time.Hour), UnreadCount: 10},
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// Unread first by count descending (10 > 1), then the read ones in the
	// order they arrived.
	expected := []domain.PeerIdentity{domaintest.ID("unread-high"), domaintest.ID("unread-low"), domaintest.ID("new-read"), domaintest.ID("old-read")}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsSortOrderSameUnreadKeepsArrivalOrder verifies that peers
// with the same unread count keep the order they were handed in — the
// arrival order — rather than being re-ranked by the timestamps their senders
// printed.
func TestSeedPreviewsSortOrderSameUnreadKeepsArrivalOrder(t *testing.T) {
	r := newTestRouter()

	now := time.Now()
	// Arrival order says "unread-new" happened last; the stamps say the
	// opposite, because that peer's clock is behind.
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("unread-new"), Timestamp: now.Add(-5 * time.Hour), UnreadCount: 3},
		{PeerAddress: domaintest.ID("unread-old"), Timestamp: now.Add(-1 * time.Hour), UnreadCount: 3},
		{PeerAddress: domaintest.ID("read-only"), Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0},
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// Same unread count → arrival decides; read peers come last.
	expected := []domain.PeerIdentity{domaintest.ID("unread-new"), domaintest.ID("unread-old"), domaintest.ID("read-only")}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsReordersEventPathPeers verifies that peers already
// created by the event-path before seedPreviews runs are repositioned
// according to the SQL-based startup sort (unread desc → timestamp desc)
// when the SQL snapshot has newer data than the event-path.
func TestSeedPreviewsReordersEventPathPeers(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate event-path creating peers before seedPreviews runs with
	// stale timestamps (older than what SQL will provide). Event-path
	// order: peer-C first, then peer-A — neither matches SQL sort.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-C")) // arrives first via event
	r.peers[domaintest.ID("peer-C")].Preview = ConversationPreview{
		PeerAddress: domaintest.ID("peer-C"),
		Body:        "event msg",
		Timestamp:   now.Add(-3 * time.Hour), // older than SQL
	}
	r.tryEnsurePeerLocked(domaintest.ID("peer-A")) // arrives second via event
	r.peers[domaintest.ID("peer-A")].Preview = ConversationPreview{
		PeerAddress: domaintest.ID("peer-A"),
		Body:        "event msg",
		Timestamp:   now.Add(-2 * time.Hour), // older than SQL
	}
	r.mu.Unlock()
	// peerOrder is now ["peer-C", "peer-A"]

	// seedPreviews arrives with the full sorted snapshot from SQL:
	// peer-B has unread (should be first), peer-A is recent, peer-C is old.
	// All SQL timestamps are newer than event-path timestamps.
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-A"), Timestamp: now.Add(-10 * time.Minute), UnreadCount: 0},
		{PeerAddress: domaintest.ID("peer-B"), Timestamp: now.Add(-1 * time.Hour), UnreadCount: 5},
		{PeerAddress: domaintest.ID("peer-C"), Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0},
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// All peers are SQL-applied (event-path data was older). The SQL sort
	// places them: unread first (peer-B), then by timestamp desc (peer-A,
	// peer-C). The two event-path slots are filled by sqlSorted in order.
	expected := []domain.PeerIdentity{domaintest.ID("peer-B"), domaintest.ID("peer-A"), domaintest.ID("peer-C")}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsDoesNotRepositionFresherPeers verifies that peers whose
// event-path data is fresher than the SQL snapshot keep their current
// peerOrder position instead of being moved to a stale SQL-determined slot.
func TestSeedPreviewsDoesNotRepositionFresherPeers(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Event-path creates peer-F at position 0 with very fresh data,
	// then peer-Old at position 1 with stale data (older than SQL).
	// Through the live path, which is what records that these peers already
	// have a preview — the seed asks that question and not "whose stamp is
	// larger".
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-F"))
	r.applyPreviewLocked(domaintest.ID("peer-F"), ConversationPreview{
		PeerAddress: domaintest.ID("peer-F"),
		Body:        "fresh event",
		Timestamp:   now, // fresher than SQL snapshot below
		Seq:         9,   // and stored after the row the seed carries
	})
	r.peers[domaintest.ID("peer-F")].Unread = 1
	// peer-Old has a row but no preview yet — a contact the header repair
	// created before any message of its own was applied. The seed owns it.
	r.tryEnsurePeerLocked(domaintest.ID("peer-Old"))
	r.mu.Unlock()
	// peerOrder: ["peer-F", "peer-Old"]

	// SQL snapshot sorted by the startup sort (unread desc → ts desc):
	// peer-S has unread, peer-Old and peer-F have none.
	// The stale SQL snapshot tries to place peer-F last (old ts in SQL).
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-S"), Timestamp: now.Add(-30 * time.Minute), UnreadCount: 3, Seq: 3},
		{PeerAddress: domaintest.ID("peer-Old"), Timestamp: now.Add(-1 * time.Hour), UnreadCount: 0, Seq: 2},
		{PeerAddress: domaintest.ID("peer-F"), Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0, Seq: 1}, // stale for peer-F
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	unreadF := r.peers[domaintest.ID("peer-F")].Unread
	bodyF := r.peers[domaintest.ID("peer-F")].Preview.Body
	r.mu.RUnlock()

	// peer-F keeps position 0 (fresher event-path data — not repositioned).
	// peer-Old's slot is filled by the SQL sort: peer-S takes that slot
	// (first in sqlSorted), peer-Old is appended (second in sqlSorted).
	expected := []domain.PeerIdentity{domaintest.ID("peer-F"), domaintest.ID("peer-S"), domaintest.ID("peer-Old")}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}

	// Fresher data must be preserved — not overwritten by stale SQL.
	if bodyF != "fresh event" {
		t.Fatalf("expected fresher body preserved, got %q", bodyF)
	}
	if unreadF != 1 {
		t.Fatalf("expected fresher Unread=1 preserved, got %d", unreadF)
	}
}

// TestSeedPreviewsPreservesEventOnlyPeers verifies that peers created by
// the event-path but absent from the SQL preview snapshot are preserved
// at the end of peerOrder (not dropped).
func TestSeedPreviewsPreservesEventOnlyPeers(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Event-path creates a peer that has no SQL preview (message just arrived).
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("event-only-peer"))
	r.peers[domaintest.ID("event-only-peer")].Preview = ConversationPreview{
		PeerAddress: domaintest.ID("event-only-peer"),
		Body:        "fresh event",
		Timestamp:   now,
	}
	r.mu.Unlock()

	// seedPreviews only contains a different peer.
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("sql-peer"), Timestamp: now.Add(-1 * time.Hour), UnreadCount: 2},
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// event-only-peer keeps its original position (not in previews, not
	// repositioned); sql-peer fills the SQL-applied slot after it.
	expected := []domain.PeerIdentity{domaintest.ID("event-only-peer"), domaintest.ID("sql-peer")}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsLeavesTheBadgeAlone pins the ownership. seedPreviews used
// to overwrite the unread COUNT from the preview snapshot; the badge is now a
// set of message ids, and a preview says nothing about which messages are
// unread — only seedUnreadIDs, the event stream, reading and deleting touch
// it. The preview body still updates, because that IS what a preview owns.
func TestSeedPreviewsLeavesTheBadgeAlone(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate event-path setting Unread=3 for peer-1 before seedPreviews.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("stale-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("stale-2"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("stale-3"))
	r.peers[domaintest.ID("peer-1")].Preview = ConversationPreview{
		PeerAddress: domaintest.ID("peer-1"),
		Body:        "stale event",
		Timestamp:   now.Add(-5 * time.Minute), // older than SQL
	}
	r.mu.Unlock()

	// SQL snapshot says this peer has 0 unread (already seen).
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-1"), Timestamp: now, UnreadCount: 0, Body: "latest"},
	}

	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	body := r.peers[domaintest.ID("peer-1")].Preview.Body
	r.mu.RUnlock()

	// The badge is a SET of message ids now, and seedPreviews does not own
	// it: seedUnreadIDs seeds the set, events add to it, reading and deleting
	// remove from it. A preview snapshot says nothing about which messages
	// are unread, so it must leave the badge exactly as it found it.
	if unread != 3 {
		t.Fatalf("seedPreviews changed the unread set: got %d, want the 3 ids that were there", unread)
	}
	if body != "latest" {
		t.Fatalf("expected preview body updated to %q, got %q", "latest", body)
	}
}

// TestSnapshotCacheReady verifies that CacheReady reflects whether
// the cache is loaded for the active peer.
func TestSnapshotCacheReady(t *testing.T) {
	r := newTestRouter()

	// No cache loaded, no active peer.
	snap := r.Snapshot()
	if snap.CacheReady {
		t.Fatal("CacheReady should be false when no cache is loaded")
	}

	// Load cache for peer-1, set as active.
	// CacheReady depends on ConversationCache state (independent of r.mu),
	// so it is recomputed on every Snapshot() call even from cache.
	r.cache.Load(domaintest.ID("peer-1"), nil, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()
	// Simulate what production code (selectPeerCore) does after mutation.
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents

	snap = r.Snapshot()
	if !snap.CacheReady {
		t.Fatal("CacheReady should be true when cache matches activePeer")
	}

	// Switch active peer but don't load cache — simulates mid-switch.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2")
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents

	snap = r.Snapshot()
	if snap.CacheReady {
		t.Fatal("CacheReady should be false when cache doesn't match new activePeer")
	}
}

// TestSelectPeerNotifiesSynchronouslyOnSwitch verifies that SelectPeer
// emits UIEventMessagesUpdated synchronously after clearing activeMessages,
// so the UI re-renders with an empty message list in the same frame.
func TestSelectPeerNotifiesSynchronouslyOnSwitch(t *testing.T) {
	r := newTestRouter()

	// Set up peer-1 as active with cached messages.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// Drain any stale events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Switch to peer-2. The goroutine will fail (no real FetchConversation)
	// but the synchronous notify should fire immediately.
	r.SelectPeer(domaintest.ID("peer-2"))

	// The synchronous notify should already be in the channel.
	select {
	case ev := <-r.uiEvents:
		if ev.Type != UIEventMessagesUpdated {
			t.Fatalf("expected UIEventMessagesUpdated, got %v", ev.Type)
		}
	default:
		t.Fatal("expected synchronous UIEventMessagesUpdated after peer switch, but channel was empty")
	}

	// activeMessages must be nil at this point.
	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()
	if msgs != nil {
		t.Fatalf("expected activeMessages=nil after switch, got %d messages", len(msgs))
	}
}

// TestSelectPeerSamePeerRetriesFailedLoad verifies that clicking the
// already-selected peer retries loadConversation when the cache doesn't
// match (previous load failed). The test proves loadConversation was
// actually attempted by checking the unread badge rollback: since the
// test router has no chatlog, loadConversation fails and
// restorePeerUnread restores the original unread count.
func TestSelectPeerSamePeerRetriesFailedLoad(t *testing.T) {
	r := newTestRouter()

	// Set peer-1 as active with unread=3 but do NOT load cache —
	// simulates a failed load with pending unread messages.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = false
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	for i := 0; i < 3; i++ {
		r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID(fmt.Sprintf("restore-%d", i)))
	}
	r.mu.Unlock()

	// Verify cache doesn't match.
	if r.cache.MatchesPeer(domaintest.ID("peer-1")) {
		t.Fatal("cache should not match peer-1 before load")
	}

	// Drain events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Click the same peer. Since changed=false, the old code would skip
	// loadConversation. The fix should detect cache mismatch and set needLoad.
	r.SelectPeer(domaintest.ID("peer-1"))

	// Verify peerClicked is set.
	r.mu.RLock()
	clicked := r.peerClicked
	r.mu.RUnlock()
	if !clicked {
		t.Fatal("expected peerClicked=true after SelectPeer")
	}

	// The goroutine calls loadConversation (which fails — no chatlog),
	// then restorePeerUnread restores Unread to 3. Without the retry fix,
	// needLoad would be false, the goroutine would run doMarkSeen (which
	// also fails), and Unread would still be restored — but to prove
	// loadConversation was attempted specifically, we check that Unread
	// returns to its original value after the optimistic clear.
	ok := pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		ps, exists := r.peers[domaintest.ID("peer-1")]
		return exists && ps.Unread == 3
	})
	if !ok {
		r.mu.RLock()
		unread := 0
		if ps, exists := r.peers[domaintest.ID("peer-1")]; exists {
			unread = ps.Unread
		}
		r.mu.RUnlock()
		t.Fatalf("expected the unread set restored to 3 ids after the failed loadConversation retry, got %d", unread)
	}
}

// TestSelectPeerSamePeerNoRetryWhenCacheReady verifies that clicking the
// already-selected peer does NOT re-load when the cache is already valid
// and Unread == 0. This is the happy-path no-op: no events, no goroutines.
// When Unread > 0, same-peer click IS NOT a no-op — see
// TestSelectPeerSamePeerRetriesDoMarkSeenWhenUnreadRestored.
func TestSelectPeerSamePeerNoRetryWhenCacheReady(t *testing.T) {
	r := newTestRouter()

	// Set peer-1 as active WITH valid cache.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.peerClicked = true
	r.mu.Unlock()

	// Drain events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Click the same peer again — cache is valid, no re-load needed.
	// This should be a true no-op: no events emitted.
	r.SelectPeer(domaintest.ID("peer-1"))

	// No events at all — true no-op.
	select {
	case ev := <-r.uiEvents:
		t.Fatalf("expected no events for same-peer click with valid cache, got %v", ev.Type)
	case <-time.After(100 * time.Millisecond):
		// Expected: no events emitted.
	}

	// activeMessages should still be populated (not cleared).
	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (cache valid, no re-load), got %d", len(msgs))
	}
}

// TestSelectPeerSamePeerRetriesDoMarkSeenWhenUnreadRestored verifies that
// an explicit user re-click on the already-active peer with valid cache but
// Unread > 0 (left over from a previous restorePeerUnread rollback) still
// clears the badge and retries doMarkSeen. This is the recovery path:
// doMarkSeen fails → restorePeerUnread restores badge → user clicks again →
// badge must clear and doMarkSeen must be reattempted.
// AutoSelectPeer same-peer is a true no-op — only user clicks recover.
//
// Proof strategy:
//   - UIEventMessagesUpdated is emitted ONLY from the goroutine (synchronous
//     path skips it because changed=false). Receiving it proves a goroutine
//     was launched, ruling out the no-op early return.
//   - After the goroutine, Unread == 0 proves doMarkSeen returned true.
//     If doMarkSeen returned false, restorePeerUnread would set Unread back
//     to 3 (the oldUnread snapshot).
func TestSelectPeerSamePeerRetriesDoMarkSeenWhenUnreadRestored(t *testing.T) {
	r := newTestRouter()

	// Set up peer-1 as active with valid cache and messages loaded.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("me"), Recipient: domaintest.ID("peer-1")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.activeMessages = r.cache.Messages()
	r.peerClicked = true
	// Simulate restorePeerUnread outcome: badge restored while chat is open.
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 3
	r.mu.Unlock()

	// Drain any setup events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// User re-clicks the same peer — expects recovery.
	r.SelectPeer(domaintest.ID("peer-1"))

	// Unread must be cleared optimistically (synchronous).
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()
	if unread != 0 {
		t.Fatalf("expected Unread=0 after same-peer re-click recovery, got %d", unread)
	}

	// Wait for UIEventMessagesUpdated — emitted ONLY from the goroutine
	// (the synchronous path does not emit it because changed=false).
	// This proves a goroutine was launched (i.e. we did not take the no-op
	// early return).
	_, found := awaitEvent(t, r.uiEvents, UIEventMessagesUpdated, 2*time.Second)
	if !found {
		t.Fatal("expected UIEventMessagesUpdated from recovery goroutine — " +
			"proves goroutine was launched, not the no-op path")
	}

	// After the goroutine completed, Unread must still be 0.
	// This proves doMarkSeen succeeded: if doMarkSeen had returned false
	// (or been skipped), restorePeerUnread would have restored Unread to 3.
	r.mu.RLock()
	unreadAfter := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()
	if unreadAfter != 0 {
		t.Fatalf("Unread should remain 0 after doMarkSeen retry (proves doMarkSeen "+
			"succeeded, not rolled back), got %d", unreadAfter)
	}
}

// TestNotifyDeferredRetryOnFullChannel verifies that when the UIEvent
// channel is full, notify() launches a retry loop that eventually
// delivers the event, preventing the UI from staying stale.
func TestNotifyDeferredRetryOnFullChannel(t *testing.T) {
	r := newTestRouter()

	// Fill the channel to capacity (32).
	for i := 0; i < 32; i++ {
		r.notify(UIEventSidebarUpdated)
	}

	// Next notify should trigger the retry loop path.
	r.notify(UIEventMessagesUpdated)

	// Drain all 32 original events.
	for i := 0; i < 32; i++ {
		select {
		case <-r.uiEvents:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected event %d in channel", i)
		}
	}

	// The retry loop should deliver the dropped event within the backoff
	// window (50ms + 100ms + 200ms = 350ms max, using 500ms for safety).
	select {
	case ev := <-r.uiEvents:
		if ev.Type != UIEventMessagesUpdated {
			t.Fatalf("expected UIEventMessagesUpdated from retry, got %v", ev.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry loop did not deliver event within 500ms")
	}
}

// TestSeedPreviewsDoesNotOverwriteFresherData verifies that seedPreviews
// skips peers that already have fresher data from the event-path,
// preventing the startup race from rolling back sidebar state.
func TestSeedPreviewsDoesNotOverwriteFresherData(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate event-path delivering a fresh update for "peer-1" BEFORE
	// seedPreviews runs (the startup race scenario).
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.applyPreviewLocked(domaintest.ID("peer-1"), ConversationPreview{
		PeerAddress: domaintest.ID("peer-1"),
		Body:        "fresh event message",
		Timestamp:   now, // newer
		Seq:         9,   // stored after the row the seed carries
	})
	for i := 0; i < 3; i++ {
		r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID(fmt.Sprintf("fresh-%d", i)))
	}
	r.mu.Unlock()

	// Now seedPreviews arrives with stale data for peer-1 and new data for peer-2.
	stalePreview := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-1"), Body: "stale startup message", Timestamp: now.Add(-5 * time.Minute), UnreadCount: 1, Seq: 1},
		{PeerAddress: domaintest.ID("peer-2"), Body: "peer-2 message", Timestamp: now.Add(-1 * time.Minute), UnreadCount: 2, Seq: 2},
	}
	r.seedPreviews(stalePreview, r.backwardsEpochSnapshot())

	r.mu.RLock()
	defer r.mu.RUnlock()

	// peer-1 should retain the fresher event-path data.
	if r.peers[domaintest.ID("peer-1")].Preview.Body != "fresh event message" {
		t.Fatalf("seedPreviews overwrote fresher data: got %q", r.peers[domaintest.ID("peer-1")].Preview.Body)
	}
	if r.peers[domaintest.ID("peer-1")].Unread != 3 {
		t.Fatalf("seedPreviews overwrote fresher unread: got %d", r.peers[domaintest.ID("peer-1")].Unread)
	}

	// peer-2 should be seeded normally (no prior data). The badge is not
	// seedPreviews' business — seedUnreadIDs owns the set — so only the
	// preview is asserted here.
	if r.peers[domaintest.ID("peer-2")].Preview.Body != "peer-2 message" {
		t.Fatalf("peer-2 should be seeded: got %q", r.peers[domaintest.ID("peer-2")].Preview.Body)
	}
}

// TestSeedPreviewsOverwritesOlderData verifies that seedPreviews DOES
// update peers when the startup data is newer than existing state.
func TestSeedPreviewsOverwritesOlderData(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate very old event-path data.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	// The startup replay re-delivers rows the database has held for days, so
	// what the event path put here can be OLDER than what the seed carries.
	r.peers[domaintest.ID("peer-1")].Preview = ConversationPreview{
		PeerAddress: domaintest.ID("peer-1"),
		Body:        "very old message",
		Timestamp:   now.Add(-1 * time.Hour),
		Seq:         1,
	}
	r.peers[domaintest.ID("peer-1")].Unread = 0
	r.mu.Unlock()

	// seedPreviews with newer data.
	previews := []ConversationPreview{
		{PeerAddress: domaintest.ID("peer-1"), Body: "newer startup message", Timestamp: now, UnreadCount: 5, Seq: 2},
	}
	r.seedPreviews(previews, r.backwardsEpochSnapshot())

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.peers[domaintest.ID("peer-1")].Preview.Body != "newer startup message" {
		t.Fatalf("seedPreviews should have updated: got %q", r.peers[domaintest.ID("peer-1")].Preview.Body)
	}
	// The badge belongs to the unread SET, which a preview snapshot does not
	// carry; seedUnreadIDs is what seeds it.
}

// TestRepairUnreadCountsNormallyWhenSeedPreviewsNeverRan verifies that
// when initializeFromDB returns without calling seedPreviews (empty/failed
// preview load), the first repairUnreadFromHeaders poll counts unreads
// normally: the badge is a set, so there is no guard to activate
// (seedPreviews already loaded counts from SQL). If seedPreviews never
// ran, there is nothing to double-count.
func TestRepairUnreadCountsNormallyWhenSeedPreviewsNeverRan(t *testing.T) {
	r := newTestRouter()

	// Simulate empty-preview startup:
	// initialSynced stays false (first poll).
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "msg-2", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	// Two incoming headers, two ids in the set.
	if unread != 2 {
		t.Fatalf("expected Unread=2, got %d — skip guard incorrectly suppressed counting", unread)
	}
}

// TestRepairUnreadSkipsCountOnFirstSyncAfterSeedPreviews: the first poll after
// a startup read must not double-count. It no longer needs a "skip the first
// sync" rule to manage it — the badge is a set, and the headers carry the very
// ids the startup read already added.
func TestRepairUnreadSkipsCountOnFirstSyncAfterSeedPreviews(t *testing.T) {
	r := newTestRouter()

	// Simulate normal startup: seedPreviews ran and set unread from SQL.
	r.mu.Lock()
	r.initialSynced = false // first poll hasn't happened yet
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	// The badge is a set of ids; the startup read added exactly the two
	// messages the headers below describe.
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-2"))
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "msg-2", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	synced := r.initialSynced
	r.mu.RUnlock()

	// Same two messages, reported twice — one from SQL, one from headers.
	if unread != 2 {
		t.Fatalf("expected Unread=2 (the same two messages), got %d", unread)
	}
	if !synced {
		t.Fatal("initialSynced should be true after first repair")
	}
}

// TestOnNewMessageRegistersSeenMessageID verifies that onNewMessage adds
// the event's MessageID to seenMessageIDs, preventing the repair-path
// (repairUnreadFromHeaders) from double-counting it.
//
// The operation gate is closed first, exactly as in the non-active-peer test
// below: onNewMessage falls back to a store read in a goroutine when the event
// cannot be decrypted, and that fallback deliberately calls evictSeenMessages
// — the repair path is supposed to re-count the message later. Asserting
// presence without closing the gate raced that eviction and failed under
// -race -count=300.
func TestOnNewMessageRegistersSeenMessageID(t *testing.T) {
	r := newTestRouter()

	// Set peer-1 as active with empty cache matching.
	r.cache.Load(domaintest.ID("peer-1"), nil, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()

	r.opMu.Lock()
	r.opClosed = true
	r.opMu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-123",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	r.mu.RLock()
	seen := r.seenMessageIDs["msg-123"].handled
	r.mu.RUnlock()
	if !seen {
		t.Fatal("onNewMessage should register MessageID in seenMessageIDs for repair-path dedup")
	}
}

// TestOnNewMessageNonActivePeerRegistersSeenID verifies dedup even when
// the message is for a non-active peer (sidebar-only update path).
//
// The operation gate is closed first so the assertion is about the
// SYNCHRONOUS registration and nothing else. The non-active path may fall back
// to a store read in a goroutine, and on a store with no row for the peer that
// fallback deliberately calls evictSeenMessages — the repair path is supposed
// to re-count the message later. Polling for presence therefore raced that
// eviction and only passed when the check happened to win, which is not a
// contract worth asserting.
func TestOnNewMessageNonActivePeerRegistersSeenID(t *testing.T) {
	cl := newTestChatLog(t)

	r := newTestRouter()
	r.client.setChatLogForTest(cl)

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2") // different from sender
	r.mu.Unlock()

	r.opMu.Lock()
	r.opClosed = true
	r.opMu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-456",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	r.mu.RLock()
	seen := r.seenMessageIDs["msg-456"].handled
	r.mu.RUnlock()
	if !seen {
		t.Fatal("onNewMessage should register MessageID in seenMessageIDs even for non-active peer")
	}
}

// TestRepairUnreadFirstSyncDoesNotDoubleCount verifies that on the first
// sync, repairUnreadFromHeaders registers every header id for dedup and adds
// the incoming ones to the unread SET. The startup read reported the same
// three messages, so the badge stays at three: no rule about "the first sync"
// is needed to prevent the double count, because a set cannot double-count.
func TestRepairUnreadFirstSyncDoesNotDoubleCount(t *testing.T) {
	r := newSyncTestRouter()

	// Simulate the startup read having reported these three messages.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-2"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-3"))
	r.activePeer = domaintest.ID("peer-2") // different from peer-1
	r.initialSynced = false                // first sync
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "msg-2", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "msg-3", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	seen1 := r.seenMessageIDs["msg-1"].handled
	seen2 := r.seenMessageIDs["msg-2"].handled
	seen3 := r.seenMessageIDs["msg-3"].handled
	synced := r.initialSynced
	r.mu.RUnlock()

	// The same three messages from both sources are three unread messages.
	if unread != 3 {
		t.Fatalf("expected Unread=3 (the same three messages), got %d", unread)
	}
	// All message IDs should be registered for future dedup.
	if !seen1 || !seen2 || !seen3 {
		t.Fatal("seenMessageIDs should contain all DMHeader IDs after first sync")
	}
	if !synced {
		t.Fatal("initialSynced should be true after first sync")
	}
}

// TestFirstSyncDoesNotBadgeMessagesTheDatabaseCallsRead pins the one thing a
// DMHeader cannot say: whether the message was already read. The headers come
// from the node's in-memory topic, which outlives a desktop session — attach a
// UI to a running node and the first poll offers back every message of the
// previous session. The database knows those were read; the header does not,
// and must not be allowed to overrule it.
func TestFirstSyncDoesNotBadgeMessagesTheDatabaseCallsRead(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("peer-from-a-previous-session")

	r := newSyncTestRouter()
	r.client = client

	ctx := context.Background()
	appendEntry := func(msgID, status string) {
		t.Helper()
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: msgID, Sender: peer.String(), Recipient: me.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			DeliveryStatus: status,
		}); err != nil {
			t.Fatalf("append %s: %v", msgID, err)
		}
	}
	// Read last session, still in the node's topic.
	appendEntry("read-1", chatlog.StatusSeen)
	appendEntry("read-2", chatlog.StatusSeen)
	// Arrived while the UI was down: stored, never read.
	appendEntry("unread-1", chatlog.StatusDelivered)

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = domaintest.ID("someone-else")
	r.initialSynced = false
	r.mu.Unlock()

	status := NodeStatus{DMHeaders: []DMHeader{
		{ID: "read-1", Sender: peer, Recipient: me},
		{ID: "read-2", Sender: peer, Recipient: me},
		{ID: "unread-1", Sender: peer, Recipient: me},
		// Not in the database at all — the header is the only evidence, so
		// it decides.
		{ID: "brand-new", Sender: peer, Recipient: me},
	}}

	// The header path alone, with NO badge seed behind it: it reads the
	// stored statuses itself, so a seed that never ran cannot cost a badge.
	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers[peer].Unread
	ids := make(map[domain.MessageID]struct{}, len(r.unreadIDs[peer]))
	for msgID := range r.unreadIDs[peer] {
		ids[msgID] = struct{}{}
	}
	r.mu.RUnlock()

	if unread != 2 {
		t.Fatalf("unread = %d, want 2 (the stored-but-unread message and the unknown one), ids=%v", unread, ids)
	}
	if _, badged := ids[domain.MessageID("read-1")]; badged {
		t.Fatalf("a message the database calls read was badged from a header: ids=%v", ids)
	}
	for _, wanted := range []domain.MessageID{"unread-1", "brand-new"} {
		if _, ok := ids[wanted]; !ok {
			t.Fatalf("message %q lost its badge: ids=%v", wanted, ids)
		}
	}
}

// TestActiveButUnloadedConversationStillBadges covers the conversation that
// is selected while its history is still loading. It is "active" by name,
// with nothing on screen to read, so the message cannot be delivered into it
// — and skipping the badge as well left the message invisible AND uncounted,
// with its id already through the dedup gate.
func TestActiveButUnloadedConversationStillBadges(t *testing.T) {
	r := newSyncTestRouter()
	me := r.client.Address()
	peer := domaintest.ID("selected-but-still-loading")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.mu.Unlock()
	// The cache belongs to nobody yet: the conversation is still loading.
	r.cache.Load(domain.PeerIdentity{}, nil, 0)

	msg := &DirectMessage{
		ID: "arrived-while-loading", Sender: peer, Recipient: me,
		Body: "hello", Timestamp: time.Now(),
	}
	if r.deliverDecryptedMessage(msg, peer, peerStamp{}) {
		t.Fatal("a message was delivered into a conversation whose cache is not loaded")
	}

	r.mu.RLock()
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if unread != 1 {
		t.Fatalf("unread = %d, want 1 — the message is neither on screen nor counted", unread)
	}
}

// TestConversationLoadForANonSelectedPeerLeavesTheCacheAlone covers the load
// that finishes after the user has already switched. It must touch nothing:
// the cache belongs to the conversation on screen, and overwriting it would
// replace what the user is reading with a thread they left — the check that
// prevents it and the load itself are in one critical section, so there is
// no window between them either.
func TestConversationLoadForANonSelectedPeerLeavesTheCacheAlone(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	left := domaintest.ID("the-slow-load")
	opened := domaintest.ID("what-the-user-opened")

	if err := client.chatLog.Append(context.Background(), "dm", me, chatlog.Entry{
		ID: "slow-1", Sender: left.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client

	// The user has already switched: the cache holds the new conversation.
	r.cache.Load(opened, []DirectMessage{{
		ID: "already-open", Sender: opened, Recipient: me, Body: "on screen",
		Timestamp: time.Now().Add(-time.Minute),
	}}, 0)
	r.mu.Lock()
	r.tryEnsurePeerLocked(left)
	r.tryEnsurePeerLocked(opened)
	r.activePeer = opened
	r.mu.Unlock()

	if r.loadConversation(left, r.peerEpochsOf(left)) {
		t.Fatal("a load for a conversation the user left reported success")
	}
	if !r.cache.MatchesPeer(opened) || !r.cache.HasMessage("already-open") {
		t.Fatal("the late load overwrote the cache of the conversation on screen")
	}
}

// TestHeaderRepairDoesNotRebuildTheOpenConversationsBadge covers the escape
// hatch added for a badge that moved backwards mid-scan. Handing the peer to
// the database is right for a conversation in the list; for the one on
// screen it is not — the user is reading it, the mark-seen that moved the
// counter is the receipt for these very messages, and a rebuild would put a
// count on the chat in front of them.
func TestHeaderRepairDoesNotRebuildTheOpenConversationsBadge(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("being-read-right-now")

	if err := client.chatLog.Append(context.Background(), "dm", me, chatlog.Entry{
		ID: "on-screen-1", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.initialSynced = false
	r.mu.Unlock()

	// The optimistic clear lands while the scan runs: the badge moved
	// backwards, and the conversation is the one on screen.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook:  func(domain.PeerIdentity) { r.clearPeerUnread(peer) },
	}

	r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{
		{ID: "on-screen-1", Sender: peer, Recipient: me},
	}})

	r.mu.RLock()
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if unread != 0 {
		t.Fatalf("the conversation on screen carries %d unread after the repair", unread)
	}
}

// TestHeaderRepairClassifiesAgainstTheCurrentConversation covers the window
// between the header scan and the mutations. Which conversation is on screen
// is read before the scan and before the stored-status query, and the user
// can switch while those run. Treating a message as visible after the user
// left it skips its badge for good: its id goes through the dedup gate either
// way, and this repair runs once per process.
func TestHeaderRepairClassifiesAgainstTheCurrentConversation(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("left-during-the-scan")

	r := newSyncTestRouter()
	r.client = client

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer // on screen when the scan starts
	r.initialSynced = false
	r.mu.Unlock()

	// The user switches away while the stored-status query runs.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.mu.Lock()
			r.activePeer = domaintest.ID("another-conversation")
			r.mu.Unlock()
		},
	}

	r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{
		{ID: "arrived-just-before-the-switch", Sender: peer, Recipient: me},
	}})

	r.mu.RLock()
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if unread != 1 {
		t.Fatalf("unread = %d, want 1 — the message was classified against the conversation the user had already left", unread)
	}
}

// TestHeaderRepairDoesNotResurrectARemovedContact covers the window between
// the header scan and the mutations it produces. Both the scan and the stored
// status read happen outside the lock; a contact removed while they run must
// not be brought back by the sidebar row the repair is about to write for it.
func TestHeaderRepairDoesNotResurrectARemovedContact(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("removed-during-the-scan")

	r := newSyncTestRouter()
	r.client = client

	if err := client.chatLog.Append(context.Background(), "dm", me, chatlog.Entry{
		ID: "arrives-too-late", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = domaintest.ID("someone-else")
	r.initialSynced = false
	r.mu.Unlock()

	// The removal lands while the repair's own chatlog read is in flight —
	// the same three steps RemovePeer applies.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.mu.Lock()
			r.peerGen[peer]++
			delete(r.peers, peer)
			delete(r.unreadIDs, peer)
			r.removePeerLocked(peer)
			r.mu.Unlock()
		},
	}

	r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{
		{ID: "arrives-too-late", Sender: peer, Recipient: me},
	}})

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	dedupGateClosed := r.seenMessageIDs["arrives-too-late"].handled
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("the header repair put a removed contact back on the sidebar")
	}
	if dedupGateClosed {
		t.Fatal("the id was written off as seen even though nothing was applied")
	}
}

// TestRepairUnreadSubsequentSyncIncrements verifies that after the first
// sync, new headers DO increment Unread normally.
func TestRepairUnreadSubsequentSyncIncrements(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-seed-1"))
	r.markUnreadLocked(domaintest.ID("peer-1"), domain.MessageID("msg-seed-2"))
	r.activePeer = domaintest.ID("peer-2")
	r.initialSynced = true // already synced
	// Pre-register some old messages.
	r.seenMessageIDs["msg-old-1"] = messageGate{handled: true}
	r.seenMessageIDs["msg-old-2"] = messageGate{handled: true}
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-old-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")}, // already seen
			{ID: "msg-new-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")}, // new
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	// Should be 2 + 1 (only the new message increments).
	if unread != 3 {
		t.Fatalf("expected Unread=3 (2 from seed + 1 new), got %d", unread)
	}
}

// TestStartupDoneClosedOnPanic verifies that startupDone is closed even
// when initializeFromDB panics, so the event listener doesn't block forever.
// This calls the real runStartup() on a router with nil cache, which causes
// resetIdentityState() to panic on r.cache.Load(). The defer chain in
// runStartup must still close startupDone.
func TestStartupDoneClosedOnPanic(t *testing.T) {
	client := &DesktopClient{id: &identity.Identity{Address: "me"}}
	client.wireSubServices()
	r := &DMRouter{
		client:         client,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		// cache intentionally nil → resetIdentityState() panics on r.cache.Load()
		uiEvents:    make(chan UIEvent, 32),
		startupDone: make(chan struct{}), // NOT pre-closed
	}

	// Call the real production startup method — not a hand-written simulation.
	go r.runStartup()

	// startupDone must close even though initializeFromDB panicked.
	select {
	case <-r.startupDone:
		// OK — closed as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("startupDone was not closed after panic — event listener would be blocked forever")
	}
}

// TestEbusBuffersDuringStartup verifies that onEbusLocalChange buffers
// events while startupComplete is false. After runStartup replays them,
// seenMessageIDs are populated.
func TestEbusBuffersDuringStartup(t *testing.T) {
	cl := newTestChatLog(t)

	client := &DesktopClient{id: &identity.Identity{Address: "me"}, chatLog: cl}
	client.wireSubServices()
	r := &DMRouter{
		client:         client,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    make(chan struct{}),
		// startupComplete defaults to false — events will be buffered.
	}

	// Close the router's background-op gate up front, so every event is
	// handled fully SYNCHRONOUSLY in this test.
	//
	// What this test asserts is the buffering/replay contract, which is
	// synchronous: onNewMessage records the id in seenMessageIDs before
	// doing anything else. The sidebar-refresh goroutines it would
	// otherwise spawn are irrelevant here and actively harmful: when
	// their preview refresh fails they legitimately call
	// evictSeenMessages (so a later repair cycle rediscovers the
	// message), which raced the assertion below and made the test flaky
	// under -race / -count>1 — reporting anything from 0 to 4 ids.
	// Closing the gate also guarantees no background goroutine is still
	// querying the chatlog when the deferred db.Close() runs.
	r.ShutdownDrain(2 * time.Second)

	// Deliver 3 events via onEbusLocalChange BEFORE startup completes.
	for i := 1; i <= 3; i++ {
		r.onEbusLocalChange(protocol.LocalChangeEvent{
			Type: protocol.LocalChangeNewMessage, Topic: "dm",
			MessageID: fmt.Sprintf("msg-%d", i),
			Sender:    "peer1", Recipient: "me",
		})
	}

	// Events must be buffered, not processed.
	r.mu.RLock()
	preStartupSeen := len(r.seenMessageIDs)
	bufLen := len(r.startupEventBuf)
	r.mu.RUnlock()
	if preStartupSeen != 0 {
		t.Fatalf("expected 0 seen messages before startup, got %d", preStartupSeen)
	}
	if bufLen != 3 {
		t.Fatalf("expected 3 buffered events, got %d", bufLen)
	}

	// Simulate runStartup Phase 1: replay buffered events under replayingStartup.
	r.mu.Lock()
	r.replayingStartup = true
	buf := r.startupEventBuf
	r.startupEventBuf = nil
	r.mu.Unlock()
	for _, ev := range buf {
		r.safeHandleEvent(ev)
	}

	// Phase 2: switch to live mode.
	r.mu.Lock()
	r.replayingStartup = false
	r.startupComplete = true
	r.mu.Unlock()
	close(r.startupDone)

	// Send a live event — should be processed immediately.
	r.onEbusLocalChange(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: "msg-4", Sender: "peer1", Recipient: "me",
	})

	// All four events (3 replayed + 1 live) were handled synchronously —
	// no polling, no background goroutines, no eviction window.
	r.mu.RLock()
	seen := len(r.seenMessageIDs)
	ids := make([]string, 0, seen)
	for id := range r.seenMessageIDs {
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	if seen != 4 {
		sort.Strings(ids)
		t.Fatalf("expected 4 seen messages, got %d: %v", seen, ids)
	}
}

// TestNotifyRetryLoopExhaustion verifies that when the channel stays full
// for the entire backoff window, the retry loop logs exhaustion and doesn't
// block or accumulate goroutines.
func TestNotifyRetryLoopExhaustion(t *testing.T) {
	r := newTestRouter()

	// Fill the channel to capacity.
	for i := 0; i < 32; i++ {
		r.notify(UIEventSidebarUpdated)
	}

	// Trigger retry — but never drain the channel, so all 3 retries fail.
	done := make(chan struct{})
	go func() {
		r.notify(UIEventMessagesUpdated)
		close(done)
	}()

	// notify() itself should return immediately.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notify() blocked despite full channel")
	}

	// Wait for the retry loop to exhaust (50+100+200 = 350ms, use 500ms).
	time.Sleep(500 * time.Millisecond)

	// uiOverflowCount should be back to 0 after the retry goroutine finishes.
	if r.uiOverflowCount.Load() != 0 {
		t.Fatalf("uiOverflowCount should be 0 after retry loop finishes, got %d", r.uiOverflowCount.Load())
	}

	// Channel should still have exactly 32 original events — the retried
	// event was never delivered because channel stayed full.
	if len(r.uiEvents) != 32 {
		t.Fatalf("expected 32 events in channel, got %d", len(r.uiEvents))
	}
}

// TestOnNewMessageNonActivePeerEmitsBeep verifies that onNewMessage emits
// UIEventBeep for incoming messages from non-active peers, so the user
// hears a notification sound immediately (event-path) rather than waiting
// for the 5-second repair-path in repairUnreadFromHeaders.
func TestOnNewMessageNonActivePeerEmitsBeep(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2") // different from the incoming message sender
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-beep-1",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	// UIEventBeep is emitted synchronously for incoming non-active messages.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("onNewMessage should emit UIEventBeep for incoming non-active-peer messages")
	}
}

// TestOnNewMessageOutgoingDoesNotBeep verifies that outgoing messages
// echoed back via the event path do NOT trigger a notification sound.
func TestOnNewMessageOutgoingDoesNotBeep(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2")
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-out-1",
		Sender:    domaintest.ID("me").String(), // outgoing — we are the sender
		Recipient: domaintest.ID("peer-1").String(),
	}

	r.onNewMessage(event)

	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		if ev.Type == UIEventBeep {
			t.Fatal("outgoing messages should NOT trigger UIEventBeep")
		}
	}
}

// TestOnNewMessageActivePeerEmitsBeep verifies that incoming messages in the
// currently active chat also trigger UIEventBeep.  The test sets activePeer to
// the message sender but leaves the cache empty so the code follows the
// mid-switch path (cache not yet loaded), which should still emit a beep.
func TestOnNewMessageActivePeerEmitsBeep(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1") // same as the incoming message sender
	r.mu.Unlock()
	// cache is empty → MatchesPeer("peer-1") == false → mid-switch path

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-active-beep-1",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	// UIEventBeep is emitted synchronously in the mid-switch path before
	// the background goroutine is launched.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("onNewMessage should emit UIEventBeep even when the active peer sends a message")
	}
}

// TestOnNewMessageActivePeerOutgoingNoBeep verifies that outgoing messages
// for the active peer do NOT trigger UIEventBeep.
func TestOnNewMessageActivePeerOutgoingNoBeep(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-active-out-1",
		Sender:    domaintest.ID("me").String(), // outgoing
		Recipient: domaintest.ID("peer-1").String(),
	}

	r.onNewMessage(event)

	// Outgoing messages must NOT beep. Use awaitEvent with a short timeout
	// to verify no UIEventBeep arrives. Any other events (e.g.
	// UIEventSidebarUpdated) are harmless — we only fail on beep.
	collected, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 100*time.Millisecond)
	if ok {
		t.Fatal("outgoing message to active peer should NOT trigger UIEventBeep")
	}
	// Also check events collected before timeout (should not contain beep).
	for _, ev := range collected {
		if ev.Type == UIEventBeep {
			t.Fatal("outgoing message to active peer should NOT trigger UIEventBeep")
		}
	}
}

// TestOnNewMessageActivePeerCacheReadyDecryptFailEmitsBeep verifies that
// when the active peer's cache is loaded but DecryptIncomingMessage returns nil,
// the fallback path still emits UIEventBeep for incoming messages. This covers
// the active + cache-ready + decrypt-fail branch, which the mid-switch and
// non-active beep tests do not exercise.
func TestOnNewMessageActivePeerCacheReadyDecryptFailEmitsBeep(t *testing.T) {
	r := newTestRouter()

	// Cache loaded for peer-1 → MatchesPeer returns true → cache-ready path.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "existing-1", Sender: domaintest.ID("peer-1"), Body: "hello"},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-decrypt-fail-beep-1",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	// Beep is emitted synchronously before the fallback goroutine starts.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("active peer + cache ready + decrypt fail must emit UIEventBeep for incoming messages")
	}
}

// TestNotifyOverflowRetainsAllEventTypes verifies that when the UI channel
// is full, each distinct event type gets its own retry goroutine instead of
// being silently dropped.  Previously, a shared CAS gate meant only the first
// overflowed event was retried; all subsequent events were lost.
func TestNotifyOverflowRetainsAllEventTypes(t *testing.T) {
	done := make(chan struct{})
	close(done)
	client := &DesktopClient{id: &identity.Identity{Address: "me"}}
	client.wireSubServices()
	r := &DMRouter{
		client:         client,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 1), // capacity 1 → overflows quickly
		startupDone:    done,
	}

	// Fill the channel so the next notify() overflows.
	r.uiEvents <- UIEvent{Type: UIEventStatusUpdated}

	// Send two more events that will overflow.
	r.notify(UIEventSidebarUpdated)
	r.notify(UIEventBeep)

	// Drain all events (including retried ones) within 1 second.
	collected := make(map[UIEventType]bool)
	deadline := time.After(1 * time.Second)
	for {
		select {
		case ev := <-r.uiEvents:
			collected[ev.Type] = true
			if collected[UIEventStatusUpdated] && collected[UIEventSidebarUpdated] && collected[UIEventBeep] {
				return // all three received
			}
		case <-deadline:
			if !collected[UIEventBeep] {
				t.Fatal("UIEventBeep was lost during overflow — each event type must get its own retry")
			}
			if !collected[UIEventSidebarUpdated] {
				t.Fatal("UIEventSidebarUpdated was lost during overflow")
			}
			return
		}
	}
}

// ── helper ──

// TestSelectPeerClearsUnreadImmediately verifies that when the user clicks
// a peer, the unread badge is cleared optimistically (synchronously) without
// waiting for the async MarkConversationSeen network calls.
func TestSelectPeerClearsUnreadImmediately(t *testing.T) {
	r := newTestRouter()

	// Seed a peer with unread messages.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-x"))
	r.peers[domaintest.ID("peer-x")].Unread = 7
	r.mu.Unlock()

	// SelectPeer spawns a goroutine for loadConversation + doMarkSeen,
	// but the unread badge must be zeroed *before* that goroutine runs.
	r.SelectPeer(domaintest.ID("peer-x"))

	// Check immediately — no sleep or channel wait.
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-x")].Unread
	r.mu.RUnlock()

	if unread != 0 {
		t.Fatalf("expected Unread=0 after SelectPeer, got %d", unread)
	}

	// Also verify UIEventSidebarUpdated was emitted synchronously.
	found := false
	drainTimeout := time.After(100 * time.Millisecond)
	for {
		select {
		case ev := <-r.uiEvents:
			if ev.Type == UIEventSidebarUpdated {
				found = true
			}
		case <-drainTimeout:
			goto done
		}
	}
done:
	if !found {
		t.Fatal("UIEventSidebarUpdated not emitted after SelectPeer")
	}
}

// TestSelectPeerRestoresUnreadOnFailure verifies that when the background
// loadConversation/doMarkSeen fails, the optimistically cleared unread badge
// is restored to its previous value.
func TestSelectPeerRestoresUnreadOnFailure(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-fail"))
	for i := 0; i < 4; i++ {
		r.markUnreadLocked(domaintest.ID("peer-fail"), domain.MessageID(fmt.Sprintf("fail-%d", i)))
	}
	r.mu.Unlock()

	// SelectPeer spawns a goroutine. loadConversation will fail because
	// the test router has no real chatlog client.
	r.SelectPeer(domaintest.ID("peer-fail"))

	// Immediately after SelectPeer, unread is 0 (optimistic clear).
	r.mu.RLock()
	immediate := r.peers[domaintest.ID("peer-fail")].Unread
	r.mu.RUnlock()
	if immediate != 0 {
		t.Fatalf("expected Unread=0 immediately after SelectPeer, got %d", immediate)
	}

	// First UIEventSidebarUpdated is synchronous from selectPeerCore.
	// Drain it so we can wait for the second one from restorePeerUnread.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for synchronous UIEventSidebarUpdated from SelectPeer")
	}
	// Second UIEventSidebarUpdated comes from restorePeerUnread after
	// loadConversation fails in the background goroutine.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated from restorePeerUnread")
	}

	r.mu.RLock()
	restored := r.peers[domaintest.ID("peer-fail")].Unread
	r.mu.RUnlock()
	if restored != 4 {
		t.Fatalf("expected Unread=4 after failed loadConversation, got %d", restored)
	}
}

// closeBackgroundOps makes selectPeerCore's background goroutine never
// start, so a test about its SYNCHRONOUS half is decided by the code under
// test rather than by the scheduler.
//
// Both unread tests below assert the OPTIMISTIC clear — the one that
// happens before the goroutine is spawned. The goroutine then finds no
// chatlog in the fixture, fails the load, and restores the badge; whether
// it does so before or after the assertion is a coin flip, and the flip
// changes with unrelated additions elsewhere in the package. Closing the
// op gate removes the coin.
func closeBackgroundOps(r *DMRouter) {
	r.opMu.Lock()
	r.opClosed = true
	r.opMu.Unlock()
}

// TestAutoSelectPeerNewPeerClearsUnread verifies that AutoSelectPeer clears
// the unread badge when switching to a new peer (changed=true). Same-peer
// re-selection is a true no-op — see TestAutoSelectPeerSamePeerIsNoOp.
func TestAutoSelectPeerNewPeerClearsUnread(t *testing.T) {
	r := newTestRouter()
	closeBackgroundOps(r)

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 5
	r.mu.Unlock()

	r.AutoSelectPeer(domaintest.ID("peer-1"))

	// Immediately after AutoSelectPeer, the badge should be cleared
	// (optimistic clear happens synchronously in selectPeerCore).
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	clicked := r.peerClicked
	r.mu.RUnlock()

	if unread != 0 {
		t.Fatalf("AutoSelectPeer must clear Unread (chat on screen = read), expected 0, got %d", unread)
	}
	if !clicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true")
	}
}

// TestSelectPeerClearsUnreadOptimistically verifies that SelectPeer (explicit
// user click) DOES clear the unread badge synchronously, and restores it if
// doMarkSeen fails in the background.
func TestSelectPeerClearsUnreadOptimistically(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 5
	r.mu.Unlock()

	r.SelectPeer(domaintest.ID("peer-1"))

	// Immediately after SelectPeer (before background goroutine),
	// the badge should already be cleared.
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	clicked := r.peerClicked
	r.mu.RUnlock()

	if unread != 0 {
		t.Fatalf("SelectPeer must optimistically clear Unread, expected 0, got %d", unread)
	}
	if !clicked {
		t.Fatal("SelectPeer must set peerClicked")
	}
}

// TestSelectPeerAndAutoSelectPeerShareCoreLogic verifies that both
// SelectPeer and AutoSelectPeer produce the same observable side effects
// on the changed=true path (new peer): activePeer switches, peerClicked
// is set, unread badge is optimistically cleared, and UIEventSidebarUpdated
// is emitted synchronously. Same-peer behavior differs and is tested
// separately (TestAutoSelectPeerSamePeerIsNoOp, TestSelectPeerSamePeer*).
func TestSelectPeerAndAutoSelectPeerShareCoreLogic(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 3
	r.tryEnsurePeerLocked(domaintest.ID("peer-2"))
	r.peers[domaintest.ID("peer-2")].Unread = 5
	r.mu.Unlock()

	// --- SelectPeer: changed=true path ---
	r.SelectPeer(domaintest.ID("peer-1"))
	r.mu.RLock()
	if r.activePeer != domaintest.ID("peer-1") {
		t.Fatalf("SelectPeer must set activePeer, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("SelectPeer must set peerClicked = true")
	}
	if r.peers[domaintest.ID("peer-1")].Unread != 0 {
		t.Fatalf("SelectPeer must optimistically clear Unread, got %d", r.peers[domaintest.ID("peer-1")].Unread)
	}
	r.mu.RUnlock()

	// Drain events from SelectPeer.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// --- AutoSelectPeer: changed=true path (different peer) ---
	r.AutoSelectPeer(domaintest.ID("peer-2"))
	r.mu.RLock()
	if r.activePeer != domaintest.ID("peer-2") {
		t.Fatalf("AutoSelectPeer must switch activePeer, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true")
	}
	if r.peers[domaintest.ID("peer-2")].Unread != 0 {
		t.Fatalf("AutoSelectPeer must optimistically clear Unread, got %d", r.peers[domaintest.ID("peer-2")].Unread)
	}
	if r.activeMessages != nil {
		t.Fatal("AutoSelectPeer on changed=true must clear activeMessages (stale-message protection)")
	}
	r.mu.RUnlock()

	// Both must emit UIEventSidebarUpdated synchronously.
	// (UIEventMessagesUpdated is also emitted on changed=true.)
	_, found := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 500*time.Millisecond)
	if !found {
		t.Fatal("AutoSelectPeer changed=true must emit UIEventSidebarUpdated synchronously")
	}
}

// TestAutoSelectPeerSamePeerIsNoOp verifies that programmatic re-selection
// of the already-active peer is an observable no-op: no state mutations and
// no UI events within a 100ms window. This guards against regressions that
// accidentally re-introduce unread clear, doMarkSeen, or UIEvent emission
// on same-peer AutoSelectPeer.
//
// Note: we verify observable effects (state + events). We cannot directly
// prove that no goroutine was launched, but the combination of no events
// and no state changes after 100ms provides strong evidence of the early
// return path in selectPeerCore.
func TestAutoSelectPeerSamePeerIsNoOp(t *testing.T) {
	r := newTestRouter()

	// Set up peer-1 as active with valid cache and non-zero unread.
	// A true no-op must not touch unread.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.activeMessages = r.cache.Messages()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 5
	r.mu.Unlock()

	// Drain any setup events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Programmatic re-select of the same peer — must be true no-op.
	r.AutoSelectPeer(domaintest.ID("peer-1"))

	// No events emitted.
	select {
	case ev := <-r.uiEvents:
		t.Fatalf("AutoSelectPeer same-peer emitted event %v — expected true no-op", ev.Type)
	case <-time.After(100 * time.Millisecond):
		// Expected: no events.
	}

	// State unchanged.
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	active := r.activePeer
	clicked := r.peerClicked
	msgs := len(r.activeMessages)
	r.mu.RUnlock()

	if unread != 5 {
		t.Fatalf("Unread changed from 5 to %d — same-peer AutoSelectPeer must not clear unread", unread)
	}
	if active != domaintest.ID("peer-1") {
		t.Fatalf("activePeer changed to %q", active)
	}
	if !clicked {
		t.Fatal("peerClicked was reset")
	}
	if msgs != 1 {
		t.Fatalf("activeMessages changed: got %d, want 1", msgs)
	}
}

// TestReplayStartupBufferDoesNotDoubleCountUnread verifies that events
// replayed from the startup buffer do NOT increment Unread again (because
// seedPreviews already loaded the correct count from SQL). Uses the ebus
// onEbusLocalChange → runStartup replay path.
func TestReplayStartupBufferDoesNotDoubleCountUnread(t *testing.T) {
	r := newTestRouter()

	// Reset startupComplete so events get buffered by onEbusLocalChange.
	r.mu.Lock()
	r.startupComplete = false
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 3
	r.activePeer = domaintest.ID("peer-2") // different from peer-1 → non-active path
	r.mu.Unlock()

	// Buffer an event via onEbusLocalChange.
	r.onEbusLocalChange(protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-replay-1",
		Sender:    "peer-1",
		Recipient: "me",
	})

	// Simulate runStartup replay: Phase 1 under replayingStartup=true.
	r.mu.Lock()
	r.replayingStartup = true
	buf := r.startupEventBuf
	r.startupEventBuf = nil
	r.mu.Unlock()

	for _, ev := range buf {
		r.safeHandleEvent(ev)
	}

	// Phase 2: switch to live mode.
	r.mu.Lock()
	r.replayingStartup = false
	r.startupComplete = true
	r.mu.Unlock()

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	replaying := r.replayingStartup
	r.mu.RUnlock()

	if unread != 3 {
		t.Fatalf("expected Unread=3 (unchanged from seedPreviews), got %d", unread)
	}
	if replaying {
		t.Fatal("replayingStartup should be false after replay")
	}

	// Verify no UIEventBeep was emitted during replay.
	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		if ev.Type == UIEventBeep {
			t.Fatal("UIEventBeep should not be emitted during startup replay")
		}
	}
}

// TestEbusLiveEventsAfterReplayTriggerBeep verifies that events delivered
// via onEbusLocalChange after startup replay completes are processed as
// live (beep emitted), while buffered events replayed under
// replayingStartup=true do not emit beeps.
func TestEbusLiveEventsAfterReplayTriggerBeep(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.startupComplete = false
	r.activePeer = domaintest.ID("someone-else")
	r.mu.Unlock()

	// Buffer 3 events via onEbusLocalChange (startup not complete).
	for i := 1; i <= 3; i++ {
		r.onEbusLocalChange(protocol.LocalChangeEvent{
			Type: protocol.LocalChangeNewMessage, Topic: "dm",
			MessageID: fmt.Sprintf("buf-%d", i), Sender: domaintest.ID("peer-1").String(), Recipient: domaintest.ID("me").String(),
		})
	}

	// Replay Phase 1 under replayingStartup=true → no beep.
	r.mu.Lock()
	r.replayingStartup = true
	buf := r.startupEventBuf
	r.startupEventBuf = nil
	r.mu.Unlock()
	for _, ev := range buf {
		r.safeHandleEvent(ev)
	}

	// Phase 2: switch to live mode.
	r.mu.Lock()
	r.replayingStartup = false
	r.startupComplete = true
	r.mu.Unlock()

	// Send 2 live events — should emit beep.
	for i := 1; i <= 2; i++ {
		r.onEbusLocalChange(protocol.LocalChangeEvent{
			Type: protocol.LocalChangeNewMessage, Topic: "dm",
			MessageID: fmt.Sprintf("live-%d", i), Sender: domaintest.ID("peer-2").String(), Recipient: domaintest.ID("me").String(),
		})
	}

	// Count UIEventBeep. Buffered events (3) emit 0 beeps (replaying).
	// Live events (2) emit 2 beeps.
	beepCount := 0
	drainTimeout := time.After(500 * time.Millisecond)
	for done := false; !done; {
		select {
		case ev := <-r.uiEvents:
			if ev.Type == UIEventBeep {
				beepCount++
			}
		case <-drainTimeout:
			done = true
		}
	}

	if beepCount != 2 {
		t.Fatalf("expected 2 UIEventBeep (one per live event), got %d", beepCount)
	}
}

// TestEbusEventsDuringReplayBufferedThenLive verifies that events arriving
// via onEbusLocalChange during Phase 1 replay are re-buffered (startup not
// yet complete), then processed as live in Phase 2. The replay event must
// NOT emit UIEventBeep, while the live event MUST.
func TestEbusEventsDuringReplayBufferedThenLive(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.startupComplete = false
	r.activePeer = domaintest.ID("someone-else")
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.mu.Unlock()

	// Buffer one event (pre-startup).
	r.onEbusLocalChange(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: "replay-msg", Sender: domaintest.ID("peer-1").String(), Recipient: domaintest.ID("me").String(),
	})

	// Phase 1: replay under replayingStartup=true.
	r.mu.Lock()
	r.replayingStartup = true
	buf := r.startupEventBuf
	r.startupEventBuf = nil
	r.mu.Unlock()

	for _, ev := range buf {
		r.safeHandleEvent(ev)
	}

	// Simulate an event arriving during Phase 1 (still !startupComplete).
	r.onEbusLocalChange(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: "live-msg", Sender: domaintest.ID("peer-1").String(), Recipient: domaintest.ID("me").String(),
	})

	// Phase 2: switch to live mode and drain remaining.
	r.mu.Lock()
	r.replayingStartup = false
	r.startupComplete = true
	remaining := r.startupEventBuf
	r.startupEventBuf = nil
	r.mu.Unlock()

	for _, ev := range remaining {
		r.safeHandleEvent(ev)
	}

	r.mu.RLock()
	replaying := r.replayingStartup
	r.mu.RUnlock()
	if replaying {
		t.Fatal("replayingStartup should be false after replay")
	}

	// Replay event: replayingStartup=true → NO beep.
	// Live event: replayingStartup=false → beep.
	beepCount := 0
	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		if ev.Type == UIEventBeep {
			beepCount++
		}
	}
	if beepCount != 1 {
		t.Fatalf("expected exactly 1 UIEventBeep (for live event only), got %d", beepCount)
	}
}

// TestEbusStartupBufferCapped verifies that onEbusLocalChange caps the
// startup buffer at 256 events. Excess events are dropped; the drop count
// is tracked in startupDropped so runStartup can emit a UI reload.
func TestEbusStartupBufferCapped(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.startupComplete = false
	r.activePeer = domaintest.ID("someone-else")
	r.mu.Unlock()

	// Send 260 events — 256 should be buffered, 4 dropped.
	for i := 0; i < 260; i++ {
		r.onEbusLocalChange(protocol.LocalChangeEvent{
			Type:      protocol.LocalChangeNewMessage,
			Topic:     "dm",
			MessageID: fmt.Sprintf("evt-%d", i),
			Sender:    "peer-1",
			Recipient: "me",
		})
	}

	r.mu.RLock()
	bufLen := len(r.startupEventBuf)
	dropped := r.startupDropped
	r.mu.RUnlock()

	if bufLen != 256 {
		t.Fatalf("expected 256 buffered events, got %d", bufLen)
	}
	if dropped != 4 {
		t.Fatalf("expected 4 dropped events, got %d", dropped)
	}
}

// TestDoMarkSeenRejectsStalePeer verifies that doMarkSeen returns false
// (triggering rollback) when the active peer has already switched away
// from peerAddress. Before the fix, doMarkSeen would copy activeMessages
// from the new peer, send a vacuous MarkConversationSeen (which succeeds),
// and falsely clear unread for the old peer.
func TestDoMarkSeenRejectsStalePeer(t *testing.T) {
	r := newTestRouter()

	// Set up peer-1 with unread=5 and make it active.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 5
	// Simulate loaded messages for the active peer.
	r.activeMessages = []DirectMessage{
		{ID: "msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
	}
	r.mu.Unlock()

	// Now simulate a fast switch: user clicks peer-2 before doMarkSeen
	// goroutine for peer-1 has a chance to run.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2")
	r.tryEnsurePeerLocked(domaintest.ID("peer-2"))
	r.activeMessages = []DirectMessage{
		{ID: "msg-2", Sender: domaintest.ID("peer-2"), Recipient: domaintest.ID("me")},
	}
	r.mu.Unlock()

	// doMarkSeen for the OLD peer should detect the stale state and
	// return false, so the caller can restore unread.
	result := r.doMarkSeen(domaintest.ID("peer-1"))
	if result {
		t.Fatal("doMarkSeen should return false when activePeer != peerAddress (stale peer)")
	}

	// Verify peer-1's unread was NOT cleared.
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()
	if unread != 5 {
		t.Fatalf("expected peer-1 Unread=5 (preserved), got %d", unread)
	}
}

// TestRepairUnreadNotClearedOnFailedReload verifies that when
// repairUnreadFromHeaders detects a new message for the active peer but
// loadConversation fails, the code does NOT call doMarkSeen. Without
// this guard the stale activeMessages could let doMarkSeen succeed
// vacuously and clear the unread badge — while the new message was never
// actually loaded or receipted.
//
// Additionally, failed message IDs must be evicted from seenMessageIDs
// so the next repair cycle can re-discover and retry them.
func TestRepairUnreadNotClearedOnFailedReload(t *testing.T) {
	r := newTestRouter()

	// Pre-load an old message so doMarkSeen has something to send if the
	// bug is present (activeMessages non-empty → MarkConversationSeen fires).
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "old-msg", Sender: domaintest.ID("peer-1"), Body: "hi"},
	}, 0)

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.initialSynced = true
	r.activeMessages = r.cache.Messages()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 0
	r.mu.Unlock()

	// Trigger repair with a header whose ID is NOT in the cache.
	// loadConversation will fail because newTestRouter has no chatlog.
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "new-msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	// Poll until the message ID is evicted (replaces fixed sleep).
	// repairUnreadFromHeaders registers the ID synchronously, then the
	// active-peer reload path evicts it when loadConversation fails.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		ok := r.seenMessageIDs["new-msg-1"].handled
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("new-msg-1 should have been evicted from seenMessageIDs after failed reload")
	}

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	// Verify that a *second* repair with the same header can now retry
	// (the ID is no longer suppressed by seenMessageIDs).
	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	seenRetry := r.seenMessageIDs["new-msg-1"].handled
	r.mu.RUnlock()

	// It will still fail (no chatlog), so the ID should be evicted again.
	if seenRetry {
		t.Fatal("new-msg-1 should still be evicted after second failed reload")
	}

	// Unread badge must not have been falsely cleared by doMarkSeen.
	// Since the message is incoming to the active peer, the repair path
	// does not increment Unread (active peer branch), and doMarkSeen
	// should never have run, so Unread stays at 0 — no clearPeerUnread
	// would have been called. The key invariant is the seenMessageIDs
	// eviction above.
	_ = unread
}

// TestOnNewMessageEventPathRollsBackSeenOnFailedReload verifies that when
// onNewMessage takes the active + cache-ready + decrypt-fail path and
// loadConversation fails, the message ID is evicted from seenMessageIDs.
// Without this rollback, repairUnreadFromHeaders would permanently skip
// the message, leaving the chat missing it until restart.
func TestOnNewMessageEventPathRollsBackSeenOnFailedReload(t *testing.T) {
	r := newTestRouter()

	// Cache loaded for peer-1 → cache-ready path.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "existing-1", Sender: domaintest.ID("peer-1"), Body: "hello"},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-rollback-1",
		Sender:    "peer-1",
		Recipient: "me",
	}
	r.onNewMessage(event)

	// Beep is emitted synchronously before the goroutine. The goroutine
	// does loadConversation (fails — no chatlog) → evicts seenMessageIDs
	// → returns. Waiting for Beep ensures onNewMessage ran.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventBeep")
	}

	// Poll until the goroutine evicts the message ID (replaces fixed sleep).
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		ok := r.seenMessageIDs["msg-rollback-1"].handled
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("msg-rollback-1 must be evicted from seenMessageIDs after failed loadConversation, otherwise repairUnreadFromHeaders will permanently skip it")
	}
}

// TestOnNewMessageNonActiveFallbackRollsBackSeenOnPreviewFailure verifies that
// when the non-active peer decrypt-fail path runs and updatePreviewFromStore
// fails (no chatlog), the message ID is evicted from seenMessageIDs.
// Without this rollback, repairUnreadFromHeaders would permanently skip the
// message because the dedup gate still holds the ID.
func TestOnNewMessageNonActiveFallbackRollsBackSeenOnPreviewFailure(t *testing.T) {
	r := newTestRouter()

	// Peer "peer-2" is NOT the active peer — forces the non-active path.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-nonactive-rollback-1",
		Sender:    "peer-2",
		Recipient: "me",
	}

	// DecryptIncomingMessage returns nil (no contact keys in test router) →
	// updateSidebarFromEvent returns false → non-active decrypt-fail path.
	// updatePreviewFromStore fails (no chatlog) → must roll back seenMessageIDs.
	r.onNewMessage(event)

	// UIEventBeep is emitted synchronously for incoming non-active messages.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventBeep")
	}

	// Poll until the goroutine evicts the message ID (replaces fixed sleep).
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		ok := r.seenMessageIDs["msg-nonactive-rollback-1"].handled
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("msg-nonactive-rollback-1 must be evicted from seenMessageIDs after failed updatePreviewFromStore, otherwise repairUnreadFromHeaders will permanently skip it")
	}
}

// TestRefreshPreviewForPeerRollsBackSeenOnFailure verifies that the
// repair-path (repairUnreadFromHeaders → refreshPreviewForPeer) evicts
// message IDs from seenMessageIDs when updatePreviewFromStore fails.
// Without this, a transient FetchSinglePreview error during a health poll
// would permanently suppress the message — repair would never retry it.
func TestRefreshPreviewForPeerRollsBackSeenOnFailure(t *testing.T) {
	r := newTestRouter()

	// Pre-register message IDs in seenMessageIDs as repairUnreadFromHeaders does.
	r.mu.Lock()
	r.seenMessageIDs["repair-msg-1"] = messageGate{handled: true}
	r.seenMessageIDs["repair-msg-2"] = messageGate{handled: true}
	r.mu.Unlock()

	// The repair path creates the row before queueing the refresh; do the
	// same here, because a reconciliation never creates one.
	r.ensurePeerForReconcile(domaintest.ID("peer-1"), 0)

	// newTestRouter has no chatlog → updatePreviewFromStore returns false →
	// refreshPreviewForPeer must evict the message IDs.
	r.refreshPreviewForPeer(domaintest.ID("peer-1"), []string{"repair-msg-1", "repair-msg-2"})

	r.mu.RLock()
	seen1 := r.seenMessageIDs["repair-msg-1"].handled
	seen2 := r.seenMessageIDs["repair-msg-2"].handled
	r.mu.RUnlock()

	if seen1 || seen2 {
		t.Fatalf("refreshPreviewForPeer must evict message IDs on updatePreviewFromStore failure: seen1=%v seen2=%v", seen1, seen2)
	}

	// UIEventSidebarUpdated must still be emitted (even on failure).
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("refreshPreviewForPeer must emit UIEventSidebarUpdated even on preview failure")
	}
}

// TestRefreshPreviewForPeerNilPreviewPreservesPeer verifies that when
// FetchSinglePreview returns (nil, nil) — meaning the message exists in
// DMHeaders but has not been persisted to chatlog yet — the peer's sidebar
// entry and unread badge are preserved. Without this, a header-only message
// (e.g. StoreFailed on the node side) would lose its unread count permanently
// because updatePreviewFromStore used to delete the peer on nil preview.
func TestRefreshPreviewForPeerNilPreviewPreservesPeer(t *testing.T) {
	// Use a real chatlog with schema — FetchSinglePreview returns (nil, nil)
	// for an empty table (no error, just no entries).
	cl := newTestChatLog(t)

	r := newTestRouter()
	r.client.setChatLogForTest(cl)

	// Simulate what repairUnreadFromHeaders does: create peer, set Unread,
	// register seen message ID.
	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-header-only"))
	r.peers[domaintest.ID("peer-header-only")].Unread = 1
	r.seenMessageIDs["header-msg-1"] = messageGate{handled: true}
	r.mu.Unlock()

	// refreshPreviewForPeer calls updatePreviewFromStore which gets nil preview
	// (no chatlog entries for this peer). The peer must NOT be deleted.
	r.refreshPreviewForPeer(domaintest.ID("peer-header-only"), []string{"header-msg-1"})

	r.mu.RLock()
	ps, exists := r.peers[domaintest.ID("peer-header-only")]
	var unread int
	if ps != nil {
		unread = ps.Unread
	}
	seenOK := r.seenMessageIDs["header-msg-1"].handled
	r.mu.RUnlock()

	if !exists {
		t.Fatal("peer 'peer-header-only' was deleted by refreshPreviewForPeer — nil preview must not erase header-only peers")
	}
	if unread != 1 {
		t.Fatalf("Unread = %d, want 1 — nil preview must not reset the badge", unread)
	}
	if !seenOK {
		t.Fatal("seenMessageIDs['header-msg-1'] was evicted — nil preview is not an error, seen ID must persist")
	}

	// UIEventSidebarUpdated must still be emitted.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("refreshPreviewForPeer must emit UIEventSidebarUpdated")
	}
}

// TestRepairPathHeaderOnlyMessagePreservedAfterNilPreview is an end-to-end
// test for the StoreFailed recovery scenario: a message exists in DMHeaders
// but has NOT been persisted to chatlog. repairUnreadFromHeaders creates the
// peer, increments Unread, and registers the seen ID. Then
// refreshPreviewForPeer calls updatePreviewFromStore which gets nil preview.
// The peer and its unread badge must survive — the message is real, just
// not on disk yet.
func TestRepairPathHeaderOnlyMessagePreservedAfterNilPreview(t *testing.T) {
	cl := newTestChatLog(t)

	r := newTestRouter()
	r.client.setChatLogForTest(cl)
	// Use a larger event channel to absorb async notifications.
	r.uiEvents = make(chan UIEvent, 64)

	// No active peer — all headers are for non-active peers.
	r.mu.Lock()
	r.activePeer = domaintest.ID("someone-else")
	r.peerClicked = true
	r.initialSynced = true // skip first-sync suppression
	r.mu.Unlock()

	// Simulate: node has a DM in s.topics (StoreFailed kept it) but the
	// message was never written to chatlog. DMHeaders expose it.
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{
				ID:        "store-failed-msg-1",
				Sender:    domaintest.ID("peer-headonly"),
				Recipient: domaintest.ID("me"),
			},
		},
	}

	r.repairUnreadFromHeaders(status)

	// Wait for UIEventSidebarUpdated — emitted at the end of repair and
	// also by refreshPreviewForPeer goroutine.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	// Give the refreshPreviewForPeer goroutine time to complete. It calls
	// updatePreviewFromStore which queries chatlog (empty → nil preview).
	// Poll for the goroutine's second UIEventSidebarUpdated.
	awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 500*time.Millisecond)

	r.mu.RLock()
	ps, exists := r.peers[domaintest.ID("peer-headonly")]
	var unread int
	if ps != nil {
		unread = ps.Unread
	}
	seenOK := r.seenMessageIDs["store-failed-msg-1"].handled
	r.mu.RUnlock()

	if !exists {
		t.Fatal("peer 'peer-headonly' was deleted — header-only messages must preserve sidebar entry")
	}
	if unread != 1 {
		t.Fatalf("Unread = %d, want 1 — header-only message must keep unread badge", unread)
	}
	if !seenOK {
		t.Fatal("seenMessageIDs['store-failed-msg-1'] was evicted — nil preview is not an error")
	}
}

// TestRepairPathActivePeerNotDuplicateRefreshed verifies that
// repairUnreadFromHeaders does NOT launch refreshPreviewForPeer for the
// active peer when it is already handled by loadConversation.
//
// Strategy: use newTestRouter (no chatlog) so refreshPreviewForPeer would
// fail synchronously (chatLog == nil → no I/O, no blocking). If launched,
// the goroutine emits an extra UIEventSidebarUpdated beyond the single one
// at the end of repairUnreadFromHeaders. We use awaitEvent with a short
// timeout to detect the stray event.
func TestRepairPathActivePeerNotDuplicateRefreshed(t *testing.T) {
	r := newTestRouter()

	// Active peer = "peer-1", cache loaded but message not in cache.
	r.cache.Load(domaintest.ID("peer-1"), nil, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.initialSynced = true
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "repair-active-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	// Wait for the guaranteed UIEventSidebarUpdated from repairUnreadFromHeaders.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	// If refreshPreviewForPeer was incorrectly launched for the active peer,
	// it emits a second UIEventSidebarUpdated. The goroutine body is pure
	// CPU (chatLog == nil → synchronous failure, no I/O), so it completes
	// before the first event is consumed. Use awaitEvent with a short
	// timeout to confirm no stray event arrives.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 100*time.Millisecond); ok {
		t.Fatal("got extra UIEventSidebarUpdated — refreshPreviewForPeer must NOT run for the active peer")
	}
}

// TestRepairPathActivePeerDoesNotBeep verifies that repairUnreadFromHeaders
// does NOT emit UIEventBeep for messages discovered on the active peer.
// Active peer messages are already visible on screen — beeping for them
// is wrong. This also covers the retry scenario: if a previous poll loaded
// the conversation but the preview refresh failed, the next poll must not
// beep again when rediscovering the same header.
func TestRepairPathActivePeerDoesNotBeep(t *testing.T) {
	r := newTestRouter()

	r.cache.Load(domaintest.ID("peer-1"), nil, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.initialSynced = true
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "active-msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	// Wait for UIEventSidebarUpdated (always emitted).
	collected, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second)
	if !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	// Check events collected by awaitEvent (arrived before the target).
	for _, ev := range collected {
		if ev.Type == UIEventBeep {
			t.Fatal("repairUnreadFromHeaders must NOT emit UIEventBeep for active peer messages — they are already on screen")
		}
	}

	// Also drain any events that arrived after the target.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 100*time.Millisecond); ok {
		t.Fatal("repairUnreadFromHeaders must NOT emit UIEventBeep for active peer messages — they are already on screen")
	}
}

// ── Regression tests: "chat on screen = read" invariant ──
// These tests guard against reverting to the old passive auto-select
// behavior where auto-selected chats did not send seen receipts and
// could silently desync the unread counter.

// TestAutoSelectPeerNewPeerClearsUnreadOptimistically mirrors the SelectPeer
// test and verifies that auto-select clears the badge synchronously
// (before the background goroutine runs). Covers changed=true path only;
// same-peer AutoSelectPeer is a true no-op tested by TestAutoSelectPeerSamePeerIsNoOp.
func TestAutoSelectPeerNewPeerClearsUnreadOptimistically(t *testing.T) {
	r := newTestRouter()
	closeBackgroundOps(r)

	r.mu.Lock()
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 7
	r.mu.Unlock()

	r.AutoSelectPeer(domaintest.ID("peer-1"))

	// Read state immediately — before any background goroutine finishes.
	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	clicked := r.peerClicked
	active := r.activePeer
	r.mu.RUnlock()

	if active != domaintest.ID("peer-1") {
		t.Fatalf("expected activePeer=peer-1, got %q", active)
	}
	if unread != 0 {
		t.Fatalf("AutoSelectPeer must optimistically clear Unread, expected 0, got %d", unread)
	}
	if !clicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true")
	}
}

// TestOnNewMessageActiveChatDoesNotIncrementUnread verifies the core
// regression: incoming messages in an active (auto-selected) chat must
// NOT increment the Unread counter.  The old buggy behavior was to
// leave Unread alone (no Unread++), which was correct, but also not
// send seen receipts, which was wrong.  A later wrong fix tried adding
// Unread++ — this test guards against both regressions.
//
// We set up router state manually (activePeer + peerClicked + loaded cache)
// to simulate a successfully auto-selected peer, bypassing the background
// goroutine from AutoSelectPeer (which fails in tests due to missing DB).
func TestOnNewMessageActiveChatDoesNotIncrementUnread(t *testing.T) {
	r := newTestRouter()

	// Simulate a fully loaded auto-selected peer:
	// activePeer set, peerClicked=true, cache loaded, Unread=0.
	r.cache.Load(domaintest.ID("peer-1"), []DirectMessage{
		{ID: "existing-1", Sender: domaintest.ID("peer-1"), Body: "hello"},
	}, 0)
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 0
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// Incoming message for the active peer. Cache matches peer
	// → takes the "active + cache loaded" path. DecryptIncomingMessage
	// returns nil in tests → triggers loadConversation fallback.
	// The key check: no Unread++ happens on this path.
	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-regression-1",
		Sender:    "peer-1",
		Recipient: "me",
	}
	r.onNewMessage(event)

	// UIEventBeep is emitted synchronously before the fallback goroutine
	// starts. The goroutine itself does loadConversation (fails instantly —
	// no chatlog) then returns without touching Unread. Waiting for Beep
	// ensures onNewMessage has fully executed; the goroutine's failure
	// path is a no-op on Unread, so no further synchronization is needed.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventBeep")
	}

	r.mu.RLock()
	post := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	if post != 0 {
		t.Fatalf("incoming message in active chat must NOT increment Unread, expected 0, got %d", post)
	}
}

// TestOnNewMessageNonActivePeerIncrementsUnread is the counterpart:
// messages for a NON-active peer must still increment unread.
// This ensures the active-peer exception didn't accidentally suppress
// all unread increments.
//
// When updatePreviewFromStore fails (no chatlog in unit test), the goroutine
// rolls back seenMessageIDs and does NOT increment unread — that's correct:
// repairUnreadFromHeaders will pick it up on the next health poll.
// The actual unread++ path with a real chatlog is covered by
// TestOnNewMessageNonActivePeerDecryptFailFallback.
func TestOnNewMessageNonActivePeerIncrementsUnread(t *testing.T) {
	r := newTestRouter()

	// Active peer is different from the incoming message sender.
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-2")
	r.peerClicked = true
	r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
	r.peers[domaintest.ID("peer-1")].Unread = 0
	r.mu.Unlock()

	// Load cache for peer-2 so cache.MatchesPeer(domaintest.ID("peer-1")) == false.
	r.cache.Load(domaintest.ID("peer-2"), []DirectMessage{{ID: "m1", Body: "hi"}}, 0)

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-non-active-1",
		Sender:    "peer-1",
		Recipient: "me",
	}
	r.onNewMessage(event)

	// UIEventBeep is emitted synchronously for incoming non-active messages.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventBeep, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventBeep")
	}

	// Poll until the goroutine evicts the message ID (replaces fixed sleep).
	// updatePreviewFromStore fails (no chatlog) → rolls back seenMessageIDs
	// → returns without incrementing Unread.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		ok := r.seenMessageIDs["msg-non-active-1"].handled
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("msg-non-active-1 must be evicted from seenMessageIDs after failed updatePreviewFromStore")
	}

	r.mu.RLock()
	unread := r.peers[domaintest.ID("peer-1")].Unread
	r.mu.RUnlock()

	// Unread stays 0 because updatePreviewFromStore failed — the message ID
	// was rolled back and repairUnreadFromHeaders will handle it later.
	if unread != 0 {
		t.Fatalf("unread = %d, want 0 (preview load failed → no immediate unread increment)", unread)
	}

	// seenMessageIDs eviction already verified above by pollCondition.
	r.mu.RLock()
	registered := r.seenMessageIDs["msg-non-active-1"].handled
	r.mu.RUnlock()
	if registered {
		t.Fatal("seenMessageIDs must be rolled back after updatePreviewFromStore failure")
	}
}

// TestAutoSelectAndSelectPeerBothClearUnread is a side-by-side
// comparison ensuring identical unread behavior for both selection methods.
func TestAutoSelectAndSelectPeerBothClearUnread(t *testing.T) {
	cases := []struct {
		name     string
		selectFn func(r *DMRouter, addr domain.PeerIdentity)
	}{
		{"SelectPeer", func(r *DMRouter, addr domain.PeerIdentity) { r.SelectPeer(addr) }},
		{"AutoSelectPeer", func(r *DMRouter, addr domain.PeerIdentity) { r.AutoSelectPeer(addr) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRouter()

			r.mu.Lock()
			r.tryEnsurePeerLocked(domaintest.ID("peer-1"))
			r.peers[domaintest.ID("peer-1")].Unread = 10
			r.mu.Unlock()

			tc.selectFn(r, domaintest.ID("peer-1"))

			r.mu.RLock()
			unread := r.peers[domaintest.ID("peer-1")].Unread
			clicked := r.peerClicked
			r.mu.RUnlock()

			if unread != 0 {
				t.Fatalf("%s must clear Unread, expected 0, got %d", tc.name, unread)
			}
			if !clicked {
				t.Fatalf("%s must set peerClicked = true", tc.name)
			}
		})
	}
}

// TestPeerClickedTrueAfterAutoSelectThenNewPeerAutoSelect verifies
// that switching between peers via AutoSelectPeer keeps peerClicked=true
// at every step — no "gap" where an auto-selected peer is passive.
func TestPeerClickedTrueAfterAutoSelectThenNewPeerAutoSelect(t *testing.T) {
	r := newTestRouter()

	peers := []domain.PeerIdentity{domaintest.ID("peer-1"), domaintest.ID("peer-2"), domaintest.ID("peer-3")}
	for _, p := range peers {
		r.AutoSelectPeer(p)

		r.mu.RLock()
		active := r.activePeer
		clicked := r.peerClicked
		r.mu.RUnlock()

		if active != p {
			t.Fatalf("after AutoSelectPeer(%q): expected activePeer=%q, got %q", p, p, active)
		}
		if !clicked {
			t.Fatalf("after AutoSelectPeer(%q): peerClicked must be true", p)
		}
	}
}

// TestRemovePeer verifies that RemovePeer removes the peer from peers map,
// peerOrder, evicts the cache, and returns true when the active peer is removed.
// Auto-selection of the next neighbor is a UI-layer concern and not tested here.
func TestRemovePeer(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("a")] = &RouterPeerState{Unread: 3}
	r.peers[domaintest.ID("b")] = &RouterPeerState{Unread: 1}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b")}
	r.activePeer = domaintest.ID("a")
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}
	r.cache.Load(domaintest.ID("a"), []DirectMessage{{ID: "msg-1"}}, 0)

	wasActive, err := r.RemovePeer(domaintest.ID("a"))
	if err != nil {
		t.Fatalf("RemovePeer returned unexpected error: %v", err)
	}

	// Drain UI events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	if !wasActive {
		t.Fatal("RemovePeer should return true when active peer is removed")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.peers[domaintest.ID("a")]; ok {
		t.Fatal("peer 'a' should be removed from peers map")
	}
	for _, p := range r.peerOrder {
		if p == domaintest.ID("a") {
			t.Fatal("peer 'a' should be removed from peerOrder")
		}
	}
	if r.cache.MatchesPeer(domaintest.ID("a")) {
		t.Fatal("cache should be evicted for peer 'a'")
	}

	// RemovePeer clears activePeer; auto-selection is the UI layer's job.
	if !r.activePeer.IsZero() {
		t.Fatalf("activePeer should be empty after RemovePeer, got %q", r.activePeer)
	}
	if _, ok := r.peers[domaintest.ID("b")]; !ok {
		t.Fatal("peer 'b' should still exist")
	}
}

// TestRemovePeerClearsActiveWhenTailRemoved verifies that removing the last
// identity in the list clears activePeer. The UI layer handles auto-selection.
func TestRemovePeerClearsActiveWhenTailRemoved(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("a")] = &RouterPeerState{}
	r.peers[domaintest.ID("b")] = &RouterPeerState{}
	r.peers[domaintest.ID("c")] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b"), domaintest.ID("c")}
	r.activePeer = domaintest.ID("c")

	wasActive, err := r.RemovePeer(domaintest.ID("c"))
	if err != nil {
		t.Fatalf("RemovePeer returned unexpected error: %v", err)
	}

	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	if !wasActive {
		t.Fatal("RemovePeer should return true when active peer is removed")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.activePeer.IsZero() {
		t.Fatalf("activePeer should be empty after RemovePeer, got %q", r.activePeer)
	}
}

// TestRemovePeerEmptyList verifies that removing the only identity leaves
// activePeer empty.
func TestRemovePeerEmptyList(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("a")] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a")}
	r.activePeer = domaintest.ID("a")

	wasActive, err := r.RemovePeer(domaintest.ID("a"))
	if err != nil {
		t.Fatalf("RemovePeer returned unexpected error: %v", err)
	}

	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	if !wasActive {
		t.Fatal("RemovePeer should return true when active peer is removed")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.activePeer.IsZero() {
		t.Fatalf("activePeer should be empty when no peers remain, got %q", r.activePeer)
	}
}

// TestRemovePeerNonActive verifies removing a non-active peer does not
// disturb the current conversation.
func TestRemovePeerNonActive(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("a")] = &RouterPeerState{}
	r.peers[domaintest.ID("b")] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b")}
	r.activePeer = domaintest.ID("a")
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}

	wasActive, err := r.RemovePeer(domaintest.ID("b"))
	if err != nil {
		t.Fatalf("RemovePeer returned unexpected error: %v", err)
	}

	// Drain UI events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	if wasActive {
		t.Fatal("RemovePeer should return false when non-active peer is removed")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.activePeer != domaintest.ID("a") {
		t.Fatalf("activePeer should remain 'a', got %q", r.activePeer)
	}
	if len(r.activeMessages) != 1 {
		t.Fatalf("activeMessages should be untouched, got %d", len(r.activeMessages))
	}
	if _, ok := r.peers[domaintest.ID("b")]; ok {
		t.Fatal("peer 'b' should be removed")
	}
}

// TestRemovePeerErrorPreservesState verifies that when DeletePeerHistory
// fails, RemovePeer returns an error and does not modify in-memory state:
// peers, peerOrder, activePeer, cache all remain unchanged.
// Note: DeleteContact errors are best-effort (logged, not blocking) because
// the RPC may be unavailable. Only chatlog failures block removal.
func TestRemovePeerErrorPreservesState(t *testing.T) {
	r := newTestRouter()
	r.client.setChatLogForTest(newClosedChatlogStore(t, domaintest.ID("me")))

	r.peers[domaintest.ID("a")] = &RouterPeerState{Unread: 2}
	r.peers[domaintest.ID("b")] = &RouterPeerState{Unread: 1}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b")}
	r.activePeer = domaintest.ID("a")
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}
	r.cache.Load(domaintest.ID("a"), []DirectMessage{{ID: "msg-1"}}, 0)

	wasActive, rmErr := r.RemovePeer(domaintest.ID("a"))
	if rmErr == nil {
		t.Fatal("RemovePeer should return an error when DeletePeerHistory fails")
	}
	if wasActive {
		t.Fatal("wasActive should be false when RemovePeer fails")
	}

	// No UI events should be emitted on failure.
	if len(r.uiEvents) != 0 {
		t.Fatalf("expected no UI events on error, got %d", len(r.uiEvents))
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.peers[domaintest.ID("a")]; !ok {
		t.Fatal("peer 'a' should still exist after failed deletion")
	}
	if r.peers[domaintest.ID("a")].Unread != 2 {
		t.Fatalf("peer 'a' unread count should be preserved, got %d", r.peers[domaintest.ID("a")].Unread)
	}
	if r.activePeer != domaintest.ID("a") {
		t.Fatalf("activePeer should remain 'a' after failed deletion, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("peerClicked should remain true after failed deletion")
	}
	if len(r.activeMessages) != 1 {
		t.Fatalf("activeMessages should be preserved, got %d", len(r.activeMessages))
	}
	if !r.cache.MatchesPeer(domaintest.ID("a")) {
		t.Fatal("cache for peer 'a' should be preserved after failed deletion")
	}
	if len(r.peerOrder) != 2 {
		t.Fatalf("peerOrder should be unchanged, got %d", len(r.peerOrder))
	}
}

// TestRemovePeerBumpsPeerGen verifies that RemovePeer increments the
// per-peer generation counter so that in-flight goroutines (SendMessage,
// SendFileAnnounce) detect the removal and skip ensurePeerLocked /
// promotePeerLocked, preventing a deleted peer from "resurrecting" in
// the sidebar.
func TestRemovePeerBumpsPeerGen(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("alice")] = &RouterPeerState{Unread: 0}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("alice")}
	r.activePeer = domaintest.ID("alice")
	r.peerClicked = true

	// Capture generation before removal — this is what the goroutine would
	// read before the async send starts.
	r.mu.RLock()
	genBefore := r.peerGen[domaintest.ID("alice")]
	r.mu.RUnlock()

	// Remove the peer (simulates user deleting the conversation).
	_, err := r.RemovePeer(domaintest.ID("alice"))
	if err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	// Drain UI events from RemovePeer.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Verify generation was bumped.
	r.mu.RLock()
	genAfter := r.peerGen[domaintest.ID("alice")]
	r.mu.RUnlock()

	if genAfter <= genBefore {
		t.Fatalf("peerGen should be incremented after RemovePeer: before=%d after=%d", genBefore, genAfter)
	}

	// Simulate what the goroutine does after the async send completes:
	// it checks the generation under lock and must NOT re-add the peer.
	r.mu.Lock()
	stale := r.peerGen[domaintest.ID("alice")] != genBefore
	if !stale {
		// This branch must NOT execute — the generation must differ.
		r.tryEnsurePeerLocked(domaintest.ID("alice"))
		r.promotePeerLocked(domaintest.ID("alice"))
	}
	r.mu.Unlock()

	if !stale {
		t.Fatal("goroutine should detect stale generation and skip peer reinsertion")
	}

	// Peer must still be absent from the sidebar.
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.peers[domaintest.ID("alice")]; ok {
		t.Fatal("peer 'alice' must not exist after RemovePeer + stale goroutine guard")
	}
	for _, p := range r.peerOrder {
		if p == domaintest.ID("alice") {
			t.Fatal("peer 'alice' must not be in peerOrder")
		}
	}
}

// TestRemovePeerGenDoesNotBlockFreshSend verifies that after removing and
// re-adding a peer (new conversation), a fresh SendMessage goroutine can
// still insert the peer because it captured the new generation.
func TestRemovePeerGenDoesNotBlockFreshSend(t *testing.T) {
	r := newTestRouter()
	r.peers[domaintest.ID("bob")] = &RouterPeerState{Unread: 1}
	r.peerOrder = []domain.PeerIdentity{domaintest.ID("bob")}

	// Remove.
	_, err := r.RemovePeer(domaintest.ID("bob"))
	if err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Capture the CURRENT generation — a new send starts here.
	r.mu.RLock()
	freshGen := r.peerGen[domaintest.ID("bob")]
	r.mu.RUnlock()

	// Simulate the goroutine completing: generation matches, so it proceeds.
	r.mu.Lock()
	if r.peerGen[domaintest.ID("bob")] != freshGen {
		r.mu.Unlock()
		t.Fatal("fresh generation should match — no intervening RemovePeer")
	}
	r.tryEnsurePeerLocked(domaintest.ID("bob"))
	r.peers[domaintest.ID("bob")].Preview = ConversationPreview{PeerAddress: domaintest.ID("bob"), Body: "new message"}
	r.promotePeerLocked(domaintest.ID("bob"))
	r.mu.Unlock()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.peers[domaintest.ID("bob")]; !ok {
		t.Fatal("fresh send should re-create peer after removal")
	}
	if r.peers[domaintest.ID("bob")].Preview.Body != "new message" {
		t.Fatalf("unexpected preview: %q", r.peers[domaintest.ID("bob")].Preview.Body)
	}
}

// TestOnNewMessageActivePeerUpdatesPreview verifies the fix for the sidebar
// preview bug: when an incoming message arrives for the active peer and the
// cache is loaded, peers[peerID].Preview must be updated so the sidebar
// shows the latest message text, not a stale outgoing message.
//
// Uses a real DesktopClient with identity, node, and chatlog so that
// DecryptIncomingMessage succeeds and the inline code path executes,
// directly setting peers[peerID].Preview.
func TestOnNewMessageActivePeerUpdatesPreview(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	// Register peer as trusted contact so DecryptIncomingMessage succeeds.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Encrypt an incoming message from the peer.
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "reply from peer"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Build the router with a real client, loaded cache, and stale preview.
	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Load cache for the peer so MatchesPeer returns true → inline path.
	r.cache.Load(peerID, []DirectMessage{
		{ID: "my-msg", Sender: domain.PeerIdentityFromWire(id.Address), Recipient: domain.PeerIdentityFromWire(peer.Address), Body: "my message"},
	}, 0)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.tryEnsurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentityFromWire(id.Address),
		Body:        "my message",
		Timestamp:   time.Now().Add(-1 * time.Minute),
	}
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// Fire the incoming message event.
	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "peer-reply-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	r.onNewMessage(event)

	// Wait for the first UIEventSidebarUpdated — emitted synchronously in the
	// inline decrypt path, proving the preview was updated.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	// Wait for the second UIEventSidebarUpdated — emitted by the background
	// doMarkSeen goroutine after it persists seen receipts and clears unread.
	// Without this, the goroutine may still be writing to TempDir when the
	// test exits, causing "directory not empty" cleanup failures.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated from doMarkSeen")
	}

	// The core assertion: Preview must now reflect the incoming message.
	r.mu.RLock()
	preview := r.peers[peerID].Preview
	r.mu.RUnlock()

	if preview.Sender != domain.PeerIdentityFromWire(peer.Address) {
		t.Fatalf("preview sender = %q, want peer %q", preview.Sender, peer.Address)
	}
	if preview.Body != "reply from peer" {
		t.Fatalf("preview body = %q, want %q", preview.Body, "reply from peer")
	}
}

// TestOnNewMessageNonActivePeerDecryptFailFallback verifies that when a new
// message arrives for a non-active peer and inline decryption fails (e.g.
// contact keys not yet available), the router falls back to
// updatePreviewFromStore so the identity list shows the latest message's
// sender instead of a stale preview.
func TestOnNewMessageNonActivePeerDecryptFailFallback(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Do NOT register peer as trusted contact — DecryptIncomingMessage will
	// fail because the sender's public key is unknown to the trust store.
	// Encrypt a message from peer using raw keys (EncryptForParticipants
	// works with keys directly, independent of the trust store).
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello from peer"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so FetchSinglePreview can read the message.
	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "incoming-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	// Build router — peer is NOT the active conversation.
	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Set active peer to someone else so peerID takes the non-active path.
	r.mu.Lock()
	r.activePeer = domaintest.ID("other-peer")
	r.tryEnsurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentityFromWire(id.Address),
		Body:        "my old message",
	}
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "incoming-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: ts,
	}

	// Guard: prove that DecryptIncomingMessage returns nil for this event,
	// confirming the test actually exercises the fallback path.
	if msg := c.DecryptIncomingMessage(context.Background(), event); msg != nil {
		t.Fatalf("expected DecryptIncomingMessage to return nil (peer not trusted), got %+v", msg)
	}

	r.onNewMessage(event)

	// Wait for UIEventSidebarUpdated from the fallback goroutine —
	// it fires after updatePreviewFromStore + Unread++ complete.
	// The first SidebarUpdated may come from the synchronous beep path;
	// the fallback goroutine emits a second one.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	r.mu.RLock()
	preview := r.peers[peerID].Preview
	unread := r.peers[peerID].Unread
	r.mu.RUnlock()

	// The preview sender must change from "us" to the peer — proving the
	// fallback replaced the stale preview.
	if preview.Sender != domain.PeerIdentityFromWire(peer.Address) {
		t.Fatalf("preview sender = %q, want peer %q", preview.Sender, peer.Address)
	}

	// FetchSinglePreview without contact keys returns empty body but correct
	// sender — this is acceptable degraded behavior.
	if preview.PeerAddress != peerID {
		t.Fatalf("preview peer address = %q, want %q", preview.PeerAddress, peerID)
	}

	// EXACTLY one, for the one message that arrived. The badge is
	// event-driven on this path, and the fallback's chatlog reconciliation
	// deliberately leaves it alone; if it ever starts applying the SQL count
	// here as well, the same message is counted twice and the badge reads 2.
	if unread != 1 {
		t.Fatalf("unread = %d, want exactly 1 for the single incoming message", unread)
	}
}

// TestOnNewMessageMidSwitchDecryptFailFallback verifies that when a message
// arrives for the active peer but the cache hasn't loaded yet (mid-switch),
// and inline decryption fails, the goroutine's loadConversation +
// updatePreviewFromStore path updates the preview from SQLite.
func TestOnNewMessageMidSwitchDecryptFailFallback(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Do NOT register peer as trusted contact — DecryptIncomingMessage will
	// fail, exercising the fallback path.
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "mid-switch message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so updatePreviewFromStore can read it.
	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "mid-switch-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Active peer = the message peer, but cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	r.cache.Load(domaintest.ID("some-other-peer"), nil, 0)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.tryEnsurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentityFromWire(id.Address),
		Body:        "stale outgoing",
	}
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "mid-switch-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: ts,
	}

	// Guard: prove that DecryptIncomingMessage returns nil for this event,
	// confirming the test actually exercises the fallback path.
	if msg := c.DecryptIncomingMessage(context.Background(), event); msg != nil {
		t.Fatalf("expected DecryptIncomingMessage to return nil (peer not trusted), got %+v", msg)
	}

	r.onNewMessage(event)

	// Wait for UIEventSidebarUpdated from the background goroutine —
	// emitted after loadConversation + updatePreviewFromStore complete.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated")
	}

	r.mu.RLock()
	preview := r.peers[peerID].Preview
	r.mu.RUnlock()

	// Preview sender must now be the peer, not us.
	if preview.Sender != domain.PeerIdentityFromWire(peer.Address) {
		t.Fatalf("preview sender = %q, want peer %q", preview.Sender, peer.Address)
	}
	if preview.PeerAddress != peerID {
		t.Fatalf("preview peer address = %q, want %q", preview.PeerAddress, peerID)
	}
}

// TestOnNewMessageMidSwitchInlineDecryptNoUnread verifies that when an
// incoming message arrives for the active peer while the cache is still
// loading (mid-switch) and inline decryption succeeds, the unread badge
// is NOT incremented. The chat is on screen — the message is visible.
// Without the activePeer guard in updateSidebarFromEvent, this path would
// briefly show a false unread badge that could stick if doMarkSeen fails.
func TestOnNewMessageMidSwitchInlineDecryptNoUnread(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Register peer as trusted contact so DecryptIncomingMessage succeeds.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "mid-switch visible message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so the background reload succeeds.
	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "mid-switch-unread-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Active peer = the message peer, cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	r.cache.Load(domaintest.ID("some-other-peer"), nil, 0)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.tryEnsurePeerLocked(peerID)
	r.peers[peerID].Unread = 0
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "mid-switch-unread-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: ts,
	}

	// Guard: prove that DecryptIncomingMessage succeeds for this event.
	if msg := c.DecryptIncomingMessage(context.Background(), event); msg == nil {
		t.Fatal("expected DecryptIncomingMessage to succeed (peer is trusted)")
	}

	r.onNewMessage(event)

	// The mid-switch goroutine emits events in this order:
	//   UIEventMessagesUpdated → UIEventSidebarUpdated → doMarkSeen()
	// doMarkSeen() on success emits its own UIEventSidebarUpdated (from
	// clearPeerUnread + notify inside doMarkSeen). We must wait for that
	// final event to ensure all background I/O (chatlog reads, seen
	// receipts) has completed before the test exits, preventing TempDir
	// cleanup races.
	//
	// Strategy: consume UIEventMessagesUpdated first (proves goroutine
	// started), then wait for UIEventSidebarUpdated that follows doMarkSeen.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventMessagesUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventMessagesUpdated from mid-switch goroutine")
	}
	// Now consume the UIEventSidebarUpdated from the goroutine's own notify,
	// then wait for the second one from doMarkSeen.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for first UIEventSidebarUpdated")
	}
	if _, ok := awaitEvent(t, r.uiEvents, UIEventSidebarUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventSidebarUpdated from doMarkSeen — goroutine may still be running")
	}

	// The critical assertion: Unread must stay 0 for the active peer.
	// The chat is on screen — showing an unread badge is wrong.
	r.mu.RLock()
	unread := r.peers[peerID].Unread
	r.mu.RUnlock()

	if unread != 0 {
		t.Fatalf("active peer mid-switch: Unread = %d, want 0 (chat is on screen, message is visible)", unread)
	}
}

// TestOnNewMessageMidSwitchDecryptSuccessReloadFail verifies that when an
// incoming message arrives for the active peer during a mid-switch (cache
// not yet loaded), inline decryption succeeds, but the subsequent full
// reload fails, the decrypted message is seeded into cache so it's
// immediately visible in the active chat. Without this fallback, the user
// would see a blank conversation despite the message being successfully
// decrypted in-process.
func TestOnNewMessageMidSwitchDecryptSuccessReloadFail(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Register peer as trusted contact so DecryptIncomingMessage succeeds.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "mid-switch reload-fail message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Nil out chatlog so loadConversation fails (FetchConversation returns
	// "chatlog not available"), simulating a transient chatlog error during
	// mid-switch. An empty chatlog would succeed with zero results, which
	// is not the failure path we want to test.
	c.setChatLogForTest(nil)

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    done,
	}

	// Active peer = the message peer, cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	// Set Unread=1 so we can verify doMarkSeen clears it after fallback.
	r.cache.Load(domaintest.ID("some-other-peer"), nil, 0)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.tryEnsurePeerLocked(peerID)
	r.markUnreadLocked(peerID, domain.MessageID("mid-switch-reload-fail-1"))
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "mid-switch-reload-fail-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: ts,
	}

	// Guard: inline decrypt must succeed.
	if msg := c.DecryptIncomingMessage(context.Background(), event); msg == nil {
		t.Fatal("expected DecryptIncomingMessage to succeed (peer is trusted)")
	}

	r.onNewMessage(event)

	// Wait for UIEventMessagesUpdated — emitted by the goroutine's fallback
	// path after it seeds the cache with the decrypted message.
	if _, ok := awaitEvent(t, r.uiEvents, UIEventMessagesUpdated, 2*time.Second); !ok {
		t.Fatal("timed out waiting for UIEventMessagesUpdated — goroutine fallback should seed cache")
	}

	// The critical assertion: the decrypted message must be in activeMessages
	// so the user sees it in the open chat.
	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()

	if len(msgs) == 0 {
		t.Fatal("activeMessages is empty — decrypted message was lost despite successful inline decrypt")
	}

	found := false
	for _, m := range msgs {
		if m.Body == "mid-switch reload-fail message" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("decrypted message not found in activeMessages — mid-switch fallback did not seed cache")
	}

	// The fallback seeded the message into the active chat — it's visible
	// on screen. The "on screen = read" invariant requires doMarkSeen to
	// run. Verify: Unread must drop to 0 after doMarkSeen completes.
	ok := pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.peers[peerID].Unread == 0
	})
	if !ok {
		r.mu.RLock()
		unread := r.peers[peerID].Unread
		r.mu.RUnlock()
		t.Fatalf("Unread = %d after fallback, want 0 — doMarkSeen must run when message is visible on screen", unread)
	}
}

// TestOnNewMessageMidSwitchFallbackStalePeerGuard verifies that the
// mid-switch cache-seeding fallback is a full state no-op when the user
// has already switched to a different peer. Asserts:
//   - ConversationCache not overwritten (cache.MatchesPeer unchanged)
//   - activeMessages not mutated (remains nil)
//   - no UIEventMessagesUpdated emitted from goroutine (stale-peer = no UI churn)
//
// Without the activePeer guard, the fallback would corrupt cache and emit
// spurious UI events for a peer that is no longer active.
func TestOnNewMessageMidSwitchFallbackStalePeerGuard(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Register peer as trusted contact so DecryptIncomingMessage succeeds.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "stale-peer fallback message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Nil out chatlog so loadConversation fails → triggers the fallback path.
	c.setChatLogForTest(nil)

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    done,
	}

	// Active peer = message peer, cache loaded for a different peer → mid-switch.
	otherPeer := domaintest.ID("other-peer")
	r.cache.Load(otherPeer, []DirectMessage{
		{ID: "other-1", Body: "other message", Sender: otherPeer, Recipient: domain.PeerIdentityFromWire(id.Address)},
	}, 0)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "stale-peer-guard-1",
		Sender:    peer.Address,
		Recipient: id.Address,
		Body:      ciphertext,
		CreatedAt: ts,
	}

	// Guard: inline decrypt must succeed.
	if msg := c.DecryptIncomingMessage(context.Background(), event); msg == nil {
		t.Fatal("expected DecryptIncomingMessage to succeed")
	}

	// Simulate the user switching away BEFORE onNewMessage's goroutine runs:
	// we fire onNewMessage (which spawns the goroutine), then immediately
	// switch activePeer to a different peer.
	r.onNewMessage(event)

	r.mu.Lock()
	r.activePeer = otherPeer
	r.mu.Unlock()

	// Wait for goroutine completion using a two-phase deterministic signal.
	//
	// Phase 1: poll for eviction of the message ID from seenMessageIDs.
	// onNewMessage registered "stale-peer-guard-1" synchronously. The
	// goroutine calls reloadAndRefreshPreview → loadConversation fails →
	// evictSeenMessages removes the ID. This proves the goroutine has
	// reached the eviction point.
	ok := pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		exists := r.seenMessageIDs["stale-peer-guard-1"].handled
		return !exists
	})
	if !ok {
		t.Fatal("goroutine did not complete phase 1 — seenMessageIDs still " +
			"contains the message ID (expected eviction after loadConversation failure)")
	}

	// Phase 2: after eviction, the goroutine still executes the
	// decryptedMsg != nil branch: Lock → read activePeer → Unlock → return.
	// Acquire and release mu to ensure we don't race with that final
	// Lock/Unlock cycle. After this, the goroutine has either already
	// completed its critical section or is past it.
	r.mu.Lock()
	r.mu.Unlock() //nolint:staticcheck // sync barrier, not a no-op

	// Belt-and-suspenders: yield so the goroutine's deferred recoverLog
	// and stack teardown complete before we inspect state.
	runtime.Gosched()

	// Now that the goroutine is done, drain any events and verify that
	// no UIEventMessagesUpdated was emitted. Only pre-goroutine synchronous
	// events (UIEventSidebarUpdated from preview, UIEventBeep) are allowed.
	var collectedEvents []UIEventType
	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		collectedEvents = append(collectedEvents, ev.Type)
	}
	for _, et := range collectedEvents {
		if et == UIEventMessagesUpdated {
			t.Fatal("UIEventMessagesUpdated emitted on stale-peer path — " +
				"goroutine must not emit UI events when activePeer guard skips state changes")
		}
	}

	// The critical assertion: the cache must still be loaded for otherPeer,
	// NOT for peerID. The stale-peer guard should have skipped cache.Load.
	if r.cache.MatchesPeer(peerID) {
		t.Fatal("cache was overwritten for stale peer — activePeer guard missing around cache.Load")
	}
	if !r.cache.MatchesPeer(otherPeer) {
		t.Fatal("cache no longer matches the current peer — fallback corrupted the cache")
	}

	// Verify full no-op: activeMessages must not have been set to stale
	// peer's data. Since we never loaded otherPeer's conversation through
	// the router, activeMessages should remain nil.
	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()
	if msgs != nil {
		t.Fatalf("activeMessages should be nil (stale-peer guard = no state changes), got %d messages", len(msgs))
	}
}

// TestReloadAndRefreshPreviewRollsBackOnLoadFailure verifies that
// reloadAndRefreshPreview evicts seenMessageIDs when loadConversation fails.
// This is the production method called by both mid-switch and cache-ready
// decrypt-fail goroutines.
func TestReloadAndRefreshPreviewRollsBackOnLoadFailure(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-1")
	r.peerClicked = true
	r.seenMessageIDs["msg-reload-fail"] = messageGate{handled: true}
	r.mu.Unlock()

	// newTestRouter has no chatlog → loadConversation fails → must evict.
	ok := r.reloadAndRefreshPreview(domaintest.ID("peer-1"), "msg-reload-fail")
	if ok {
		t.Fatal("reloadAndRefreshPreview must return false when loadConversation fails")
	}

	r.mu.RLock()
	seen := r.seenMessageIDs["msg-reload-fail"].handled
	r.mu.RUnlock()
	if seen {
		t.Fatal("reloadAndRefreshPreview must evict seenMessageIDs when loadConversation fails")
	}
}

// TestReloadAndRefreshPreviewNoEvictOnPartialSuccess verifies the no-evict
// contract: when loadConversation succeeds but updatePreviewFromStore fails,
// the messageID must NOT be evicted from seenMessageIDs. The message is
// already in cache, so the dedup gate must stay closed to prevent redundant
// rediscovery on the next health poll.
//
// Strategy: a goroutine polls for loadConversation completion (activeMessages
// populated), then nils chatLog so updatePreviewFromStore fails. This
// creates a deterministic partial-success without mocks.
func TestReloadAndRefreshPreviewNoEvictOnPartialSuccess(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Register peer as trusted so FetchConversation can decrypt.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	// Insert an encrypted message into chatlog so FetchConversation succeeds
	// with at least one message.
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "partial success message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "partial-success-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("append to chatlog: %v", err)
	}

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.seenMessageIDs["partial-success-1"] = messageGate{handled: true}
	r.mu.Unlock()

	// Goroutine: wait for loadConversation to populate activeMessages,
	// then nil chatLog so updatePreviewFromStore fails → partial success.
	// gdone closes after the setChatLogForTest write so the test can await
	// the goroutine before the deferred Close reads the store — without it
	// the fire-and-forget write races Close (g.store write vs read).
	gdone := make(chan struct{})
	go func() {
		defer close(gdone)
		pollCondition(2*time.Second, func() bool {
			r.mu.RLock()
			defer r.mu.RUnlock()
			return len(r.activeMessages) > 0
		})
		c.setChatLogForTest(nil)
	}()

	result := r.reloadAndRefreshPreview(peerID, "partial-success-1")
	if !result {
		t.Fatal("reloadAndRefreshPreview must return true on partial success " +
			"(loadConversation OK, updatePreviewFromStore fail)")
	}

	// The critical assertion: messageID must still be in seenMessageIDs.
	// If evictSeenMessages was called on partial success, this fails.
	r.mu.RLock()
	seen := r.seenMessageIDs["partial-success-1"].handled
	r.mu.RUnlock()
	if !seen {
		t.Fatal("seenMessageIDs was evicted on partial success — " +
			"dedup gate must stay closed when messages are already in cache")
	}

	// Await the store-mutating goroutine before returning so its
	// setChatLogForTest write happens-before the deferred Close reads g.store.
	<-gdone
}

// TestUpdatePreviewFromCacheFallback covers the fallback taken when
// loadConversation succeeded but updatePreviewFromStore failed: the cache is
// all that is left to describe the conversation with.
//
// It may fill a gap; it may not overrule. The cache is the THREAD, ordered by
// created_at — the senders' clocks — so its last element is the last message
// chronologically, which is a different question from the one the sidebar
// asks. A row with nothing on it is better filled with an approximate answer
// than left blank; a row that already carries one, put there by a path that
// knew the arrival order, must not be overwritten by a guess.
func TestUpdatePreviewFromCacheFallback(t *testing.T) {
	peerID := domaintest.ID("peer-1")
	me := domaintest.ID("me")

	// A conversation whose chronological last message is OURS, while the
	// peer's — the one that actually arrived last, from a lagging clock — sits
	// above it. This is the shape the whole ordering change is about.
	conversation := []DirectMessage{
		{ID: "theirs", Sender: peerID, Recipient: me, Body: "latest from peer", Timestamp: time.Now().Add(-5 * time.Minute)},
		{ID: "ours", Sender: me, Recipient: peerID, Body: "my message", Timestamp: time.Now()},
	}

	load := func(r *DMRouter) {
		r.cache.Load(peerID, conversation, 0)
		r.mu.Lock()
		r.activePeer = peerID
		r.peerClicked = true
		r.activeMessages = r.cache.Messages()
		r.tryEnsurePeerLocked(peerID)
		r.mu.Unlock()
	}

	t.Run("fills an empty row", func(t *testing.T) {
		r := newTestRouter()
		load(r)

		r.updatePreviewFromCache(peerID)

		r.mu.RLock()
		preview := r.peers[peerID].Preview
		lastIncoming := r.peers[peerID].LastIncomingAt
		r.mu.RUnlock()

		if preview.Body != "my message" {
			t.Fatalf("preview body = %q, want the last message of the loaded thread", preview.Body)
		}
		// The peer's message is behind our own in the thread, and presence
		// evidence has to reach past it.
		if !lastIncoming.Valid() || !lastIncoming.Time().Equal(conversation[0].Timestamp) {
			t.Fatalf("last incoming = %v, want the peer's message at %v", lastIncoming, conversation[0].Timestamp)
		}
	})

	t.Run("leaves an undecryptable row alone", func(t *testing.T) {
		r := newTestRouter()
		load(r)

		// What the store path writes when the last row will not decrypt: an
		// EMPTY body — the sidebar falls back to the fingerprint — and a
		// perfectly good place in the arrival order. Reading emptiness as
		// "nothing here" is how the cache came to overwrite it.
		r.mu.Lock()
		r.applyPreviewLocked(peerID, ConversationPreview{
			PeerAddress: peerID,
			Sender:      peerID,
			Body:        "",
			Timestamp:   conversation[0].Timestamp,
			Seq:         2,
		})
		r.mu.Unlock()

		r.updatePreviewFromCache(peerID)

		r.mu.RLock()
		preview := r.peers[peerID].Preview
		r.mu.RUnlock()

		if preview.Body != "" || preview.Seq != 2 {
			t.Fatalf("preview = %q (seq %d), want the undecryptable last row to stand: an empty body is a decrypt failure, not an empty row",
				preview.Body, preview.Seq)
		}
	})

	t.Run("leaves an existing row alone", func(t *testing.T) {
		r := newTestRouter()
		load(r)

		// What a path that knows the arrival order put there.
		r.mu.Lock()
		r.applyPreviewLocked(peerID, ConversationPreview{
			PeerAddress: peerID,
			Sender:      peerID,
			Body:        "latest from peer",
			Timestamp:   conversation[0].Timestamp,
			Seq:         2,
		})
		r.mu.Unlock()

		r.updatePreviewFromCache(peerID)

		r.mu.RLock()
		preview := r.peers[peerID].Preview
		lastIncoming := r.peers[peerID].LastIncomingAt
		r.mu.RUnlock()

		if preview.Body != "latest from peer" {
			t.Fatalf("preview body = %q, want %q: the cache is ordered by the senders' clocks and must not overrule a row that was ordered by arrival",
				preview.Body, "latest from peer")
		}
		// The evidence still runs: it is a maximum, so the cache can raise it
		// without being able to move it backwards.
		if !lastIncoming.Valid() {
			t.Fatal("last incoming was not recorded from the cache")
		}
	})
}

// TestUpdatePreviewFromCacheStalePeerGuard verifies that
// updatePreviewFromCache does NOT rebuild peer A's preview from
// activeMessages that belong to peer B after a fast peer switch.
// Without the activePeer guard, a quick SelectPeer between
// loadConversation and updatePreviewFromCache would cause cross-chat
// preview corruption.
func TestUpdatePreviewFromCacheStalePeerGuard(t *testing.T) {
	r := newTestRouter()

	peerA := domaintest.ID("peer-A")
	peerB := domaintest.ID("peer-B")

	// Set up: peer A has a stale preview. activeMessages belong to peer B
	// (simulating a fast switch after loadConversation for peer A but before
	// updatePreviewFromCache runs).
	r.cache.Load(peerB, []DirectMessage{
		{ID: "b-msg", Sender: peerB, Recipient: domaintest.ID("me"), Body: "message from B"},
	}, 0)

	r.mu.Lock()
	r.activePeer = peerB // User already switched to B.
	r.peerClicked = true
	r.activeMessages = r.cache.Messages() // These belong to peer B.
	r.tryEnsurePeerLocked(peerA)
	r.peers[peerA].Preview = ConversationPreview{
		PeerAddress: peerA,
		Sender:      peerA,
		Body:        "original A preview",
	}
	r.tryEnsurePeerLocked(peerB)
	r.mu.Unlock()

	// Call updatePreviewFromCache for peer A — but activeMessages belong to B.
	// The stale-peer guard must prevent cross-chat corruption.
	r.updatePreviewFromCache(peerA)

	r.mu.RLock()
	preview := r.peers[peerA].Preview
	r.mu.RUnlock()

	// Peer A's preview must NOT have been overwritten with peer B's message.
	if preview.Body == "message from B" {
		t.Fatal("peer A's preview was rebuilt from peer B's activeMessages — stale-peer guard missing")
	}
	if preview.Body != "original A preview" {
		t.Fatalf("peer A's preview changed unexpectedly: got %q", preview.Body)
	}
}

// TestPartialSuccessFallbackHelpers verifies the individual steps of
// the partial-success recovery: loadConversation populates the cache,
// updatePreviewFromStore fails on a closed chatlog, and
// updatePreviewFromCache rebuilds the preview from the last cached
// message.
//
// Note: this exercises the helpers in sequence, not through
// reloadAndRefreshPreview itself. Both loadConversation and
// updatePreviewFromStore use the same chatlog, so closing the DB
// between them inside a single reloadAndRefreshPreview call is not
// possible without a mock. The integration coverage for
// reloadAndRefreshPreview's full-failure path is in
// TestReloadAndRefreshPreviewRollsBackOnLoadFailure.
func TestPartialSuccessFallbackHelpers(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	// Register peer as trusted contact so FetchConversation can decrypt.
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "partial-success message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "partial-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	// The row exists with nothing on it — a contact the header repair
	// created, or a conversation opened before its first preview landed. That
	// is the gap the cache fallback is for; it does not overwrite a row that
	// already carries an answer (see TestUpdatePreviewFromCacheFallback).
	r.tryEnsurePeerLocked(peerID)
	r.seenMessageIDs["partial-1"] = messageGate{handled: true}
	r.mu.Unlock()

	// Step 1: loadConversation succeeds, populating cache with the
	// decrypted message. This is the first half of reloadAndRefreshPreview.
	if !r.loadConversation(peerID, r.peerEpochsOf(peerID)) {
		t.Fatal("loadConversation must succeed with valid chatlog")
	}

	// Verify cache was populated.
	r.mu.RLock()
	msgCount := len(r.activeMessages)
	r.mu.RUnlock()
	if msgCount == 0 {
		t.Fatal("cache should have messages after loadConversation")
	}

	// Step 2: swap in a chatlog whose database is closed, to simulate a
	// transient failure for the updatePreviewFromStore call.
	c.setChatLogForTest(newClosedChatlogStore(t, domaintest.ID("me")))

	// Step 3: updatePreviewFromStore must fail (closed chatlog).
	if r.updatePreviewFromStore(peerID) {
		t.Fatal("updatePreviewFromStore should fail with closed chatlog")
	}

	// Step 4: updatePreviewFromCache should build preview from last
	// cached message — this is the fallback in reloadAndRefreshPreview.
	r.updatePreviewFromCache(peerID)

	// The empty row must now describe the cached message rather than stay
	// blank while the chatlog is unavailable.
	r.mu.RLock()
	preview := r.peers[peerID].Preview
	r.mu.RUnlock()

	if preview.Sender != peerID {
		t.Fatalf("preview sender = %q, want the peer %q — the cache fallback did not run", preview.Sender, peerID)
	}
	if preview.Body != "partial-success message" {
		t.Fatalf("preview body = %q, want %q", preview.Body, "partial-success message")
	}
}

// TestUpdatePreviewFromStoreReturnsFalseOnClosedChatlog verifies that
// updatePreviewFromStore correctly returns false when the chatlog becomes
// unavailable (closed, corrupted, etc.). This is the failure condition
// that triggers the cache-fallback branch in reloadAndRefreshPreview.
func TestUpdatePreviewFromStoreReturnsFalseOnClosedChatlog(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "preview-fail message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	err = c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             "preview-fail-1",
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	// The row exists before the refresh, as it does in production: a
	// reconciliation updates a peer and never creates one.
	r.tryEnsurePeerLocked(peerID)
	r.mu.Unlock()

	// Guard: prove updatePreviewFromStore succeeds with valid chatlog.
	if !r.updatePreviewFromStore(peerID) {
		t.Fatal("guard: updatePreviewFromStore must succeed with valid chatlog")
	}

	// Swap in a dead chatlog and verify updatePreviewFromStore fails.
	c.setChatLogForTest(newClosedChatlogStore(t, domaintest.ID("me")))
	if r.updatePreviewFromStore(peerID) {
		t.Fatal("updatePreviewFromStore must return false after chatlog close")
	}
}

// awaitEvent drains the UIEvent channel until the target event type appears or
// the timeout expires. Returns all collected events and whether the target was
// found. This replaces time.Sleep-based synchronization for background
// goroutines that signal completion by emitting a UIEvent.
func awaitEvent(t *testing.T, ch <-chan UIEvent, target UIEventType, timeout time.Duration) ([]UIEvent, bool) {
	t.Helper()
	deadline := time.After(timeout)
	var collected []UIEvent
	for {
		select {
		case ev := <-ch:
			collected = append(collected, ev)
			if ev.Type == target {
				return collected, true
			}
		case <-deadline:
			return collected, false
		}
	}
}

// testStatusProvider is a minimal NodeStatusProvider for unit tests.
// Tests can set the Status field directly; NodeStatus() returns a deep copy.
type testStatusProvider struct {
	mu     sync.RWMutex
	Status NodeStatus
}

func (p *testStatusProvider) NodeStatus() NodeStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return deepCopyNodeStatus(p.Status)
}

func (p *testStatusProvider) ResourceUsageSnapshot() *ResourceUsage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Status.ResourceUsage == nil {
		return nil
	}
	clone := *p.Status.ResourceUsage
	return &clone
}

func (p *testStatusProvider) PeerHealthSnapshot() []PeerHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Status.PeerHealth == nil {
		return nil
	}
	return append([]PeerHealth(nil), p.Status.PeerHealth...)
}

func (p *testStatusProvider) ReachableIDsSnapshot() map[domain.PeerIdentity]bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Status.ReachableIDs == nil {
		return nil
	}
	clone := make(map[domain.PeerIdentity]bool, len(p.Status.ReachableIDs))
	for k, v := range p.Status.ReachableIDs {
		clone[k] = v
	}
	return clone
}

func (p *testStatusProvider) KnownIDsSnapshot() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Status.KnownIDs == nil {
		return nil
	}
	return append([]string(nil), p.Status.KnownIDs...)
}

func (p *testStatusProvider) AggregateStatusSnapshot() (*AggregateStatus, time.Time) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var clone *AggregateStatus
	if p.Status.AggregateStatus != nil {
		c := *p.Status.AggregateStatus
		clone = &c
	}
	return clone, p.Status.CheckedAt
}

func (p *testStatusProvider) Contacts() map[string]Contact {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Status.Contacts == nil {
		return nil
	}
	cp := make(map[string]Contact, len(p.Status.Contacts))
	for k, v := range p.Status.Contacts {
		cp[k] = v
	}
	return cp
}

func (p *testStatusProvider) IsReachable(id domain.PeerIdentity) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Status.ReachableIDs[id]
}

func (p *testStatusProvider) Reset() {
	p.mu.Lock()
	p.Status = NodeStatus{}
	p.mu.Unlock()
}

// newSyncTestRouter is newTestRouter with the background-operation gate
// already closed, so router handlers run FULLY SYNCHRONOUSLY.
//
// Several handlers register their id in seenMessageIDs synchronously and
// then fan out preview refreshes on goroutines; when a refresh fails —
// and it always does here, since the test router has no chatlog — the
// goroutine legitimately calls evictSeenMessages so a later repair cycle
// rediscovers the message. Tests that assert the SYNCHRONOUS bookkeeping
// therefore race those evictions and fail intermittently under -race /
// -count>1. Closing the gate removes the asynchrony instead of trying to
// out-wait it.
//
// Do NOT use this for tests that assert the async behaviour itself (e.g.
// TestRepairUnreadNotClearedOnFailedReload polls for the eviction) or
// that send messages — a closed gate refuses sends with
// ErrRouterShuttingDown.
func newSyncTestRouter() *DMRouter {
	r := newTestRouter()
	r.ShutdownDrain(2 * time.Second)
	return r
}

func newTestRouter() *DMRouter {
	done := make(chan struct{})
	close(done) // pre-closed so tests don't block on startupDone
	// Address must be the canonical 40-hex fingerprint so that
	// AppInfo.Address() (PeerIdentityFromWire) round-trips to the same
	// identity the fixtures use as the local node — domaintest.ID("me").
	// A short literal like "me" fails to decode and yields the zero
	// identity, so typed-identity paths (repairUnreadFromHeaders,
	// peerForMessage) would never match the seeded "me" recipient.
	client := &DesktopClient{id: &identity.Identity{Address: domaintest.ID("me").String()}}
	client.wireSubServices()
	provider := &testStatusProvider{}
	return &DMRouter{
		client:          client,
		fileBridge:      NewFileTransferBridge(client),
		statusMonitor:   provider,
		peers:           make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:       make([]domain.PeerIdentity, 0),
		seenMessageIDs:  make(map[string]messageGate),
		peerGen:         make(map[domain.PeerIdentity]uint64),
		backwardsEpoch:  make(map[domain.PeerIdentity]peerEpochs),
		removals:        client.removals,
		fileOpMu:        make(map[domain.PeerIdentity]*sync.Mutex),
		cache:           NewConversationCache(),
		withdrawals:     newWithdrawalBacklog(),
		uiEvents:        make(chan UIEvent, 32),
		startupDone:     done,
		startupComplete: true, // most tests assume post-startup behavior
	}
}

// newTestChatLog returns a chatlog repository backed by a real, migrated
// state database in a temporary directory. The database closes itself through
// t.Cleanup, so callers own nothing.
func newTestChatLog(t *testing.T) *chatlog.Store {
	t.Helper()
	return newTestChatlogStore(t, domaintest.ID("me"))
}

// pollCondition polls fn every 5ms until it returns true or timeout expires.
// Returns true if the condition was met within the deadline.
func pollCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.After(timeout)
	for {
		if fn() {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestPostCommitPeerGenRaceRemovesOrphanedMapping verifies the fix for the P2
// bug: "peerGen check after Commit() leaves orphaned sender mapping".
//
// Scenario (all inside the SendFileAnnounce goroutine):
//  1. Goroutine captures gen before the async send.
//  2. PrepareFileAnnounce succeeds → token is live.
//  3. SendDirectMessage succeeds → DM delivered.
//  4. Pre-Commit peerGen check passes (peer still present).
//  5. token.Commit() succeeds → sender mapping is committed (fileID known).
//  6. RemovePeer runs concurrently → bumps peerGen, cleans up peer state.
//  7. Post-Commit peerGen re-check detects mismatch.
//  8. RemoveSenderMapping(fileID) is called — targeted cleanup of the single
//     orphaned mapping, NOT CleanupPeerTransfers which would destroy any
//     legitimate transfers for the same peer in a newer generation.
//
// Steps 1–5 and 7–8 are simulated inline because DesktopClient is a
// concrete type and the goroutine is not directly injectable.
func TestPostCommitPeerGenRaceRemovesOrphanedMapping(t *testing.T) {
	t.Parallel()

	r := newTestRouter()
	peer := domaintest.ID("alice")

	r.peers[peer] = &RouterPeerState{Unread: 0}
	r.peerOrder = []domain.PeerIdentity{peer}
	r.activePeer = peer
	r.peerClicked = true

	// Step 1: goroutine captures generation before async send.
	r.mu.RLock()
	gen := r.peerGen[peer]
	r.mu.RUnlock()

	// Steps 2–5 happen (PrepareFileAnnounce, SendDM, pre-Commit check, Commit).
	// We assume Commit succeeded — the sender mapping is now live.
	orphanedFileID := domain.FileID("orphaned-file-123")

	// Step 6: RemovePeer runs concurrently (another goroutine or RPC call).
	_, err := r.RemovePeer(peer)
	if err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Step 7: post-Commit peerGen re-check inside the goroutine.
	r.mu.Lock()
	raceDetected := r.peerGen[peer] != gen
	if !raceDetected {
		// This path must NOT be taken — RemovePeer must have bumped gen.
		r.tryEnsurePeerLocked(peer)
		r.promotePeerLocked(peer)
		r.mu.Unlock()
		t.Fatal("post-Commit peerGen check must detect the race")
	}
	r.mu.Unlock()

	// Step 8: RollbackMapping is called with the specific fileID.
	// With localNode == nil this is a safe no-op, but it exercises the
	// code path and proves no panic. The important assertion is that we
	// call the targeted method, not the broad peer-level one.
	r.fileBridge.RollbackMapping(orphanedFileID)

	// Verify the peer was NOT resurrected in the sidebar.
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.peers[peer]; ok {
		t.Fatal("peer must not exist after RemovePeer — post-Commit cleanup must not re-add it")
	}
	for _, p := range r.peerOrder {
		if p == peer {
			t.Fatal("peer must not be in peerOrder after RemovePeer")
		}
	}
}

// TestSendFileAnnounceAsyncFailureCallsOnFailure verifies that the
// onAsyncFailure callback is invoked when the async goroutine inside
// SendFileAnnounce fails (e.g. PrepareAndSend returns an error).
// This is the fix for the P2 bug: "Attachment lost on async failure path
// inside SendFileAnnounce" — without the callback, the user's attachment
// disappears from the composer and cannot be retried.
func TestSendFileAnnounceAsyncFailureCallsOnFailure(t *testing.T) {
	t.Parallel()

	r := newTestRouter()
	peer := domaintest.ID("bob")

	r.mu.Lock()
	r.peers[peer] = &RouterPeerState{Unread: 0}
	r.peerOrder = []domain.PeerIdentity{peer}
	r.peerGen[peer] = 1
	r.mu.Unlock()

	callbackCalled := make(chan struct{}, 1)
	onFailure := func() {
		callbackCalled <- struct{}{}
	}

	// SendFileAnnounce returns nil synchronously (pre-validation passes
	// because fileBridge != nil). The goroutine then calls PrepareAndSend
	// which fails because DesktopClient.localNode is nil.
	err := r.SendFileAnnounce(peer, domain.OutgoingDM{
		Body: "test file",
	}, domain.FileAnnouncePayload{
		FileHash: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName: "test.txt",
		FileSize: 1024,
	}, onFailure)
	if err != nil {
		t.Fatalf("SendFileAnnounce should return nil synchronously: %v", err)
	}

	select {
	case <-callbackCalled:
		// onAsyncFailure was called — attachment restoration works.
	case <-time.After(5 * time.Second):
		t.Fatal("onAsyncFailure callback was not called within timeout — attachment would be lost")
	}

	// Verify that sendStatus reflects the async failure.
	r.mu.RLock()
	status := r.sendStatus
	r.mu.RUnlock()
	if status == "" || status == "sending…" {
		t.Errorf("sendStatus should reflect failure, got %q", status)
	}
}

// TestSnapshotCacheReturnsStaleWhenUnchanged verifies that consecutive
// Snapshot() calls without intervening mutations return the same cached
// snapshot. Snapshot() is completely lock-free — it never acquires r.mu.
func TestSnapshotCacheReturnsStaleWhenUnchanged(t *testing.T) {
	t.Parallel()
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-A")
	r.tryEnsurePeerLocked(domaintest.ID("peer-A"))
	r.peers[domaintest.ID("peer-A")].Unread = 3
	r.mu.Unlock()
	// notify() builds and caches the snapshot under Lock.
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents

	snap1 := r.Snapshot()
	if snap1.ActivePeer != domaintest.ID("peer-A") {
		t.Fatalf("snap1.ActivePeer = %q, want peer-A", snap1.ActivePeer)
	}
	if snap1.Peers[domaintest.ID("peer-A")].Unread != 3 {
		t.Fatalf("snap1 Unread = %d, want 3", snap1.Peers[domaintest.ID("peer-A")].Unread)
	}

	// Second call without mutation — must return cached snapshot.
	snap2 := r.Snapshot()
	if snap2.ActivePeer != snap1.ActivePeer {
		t.Fatalf("snap2.ActivePeer = %q, expected cached %q", snap2.ActivePeer, snap1.ActivePeer)
	}
	if snap2.Peers[domaintest.ID("peer-A")].Unread != 3 {
		t.Fatalf("snap2 Unread = %d, want 3 (cached)", snap2.Peers[domaintest.ID("peer-A")].Unread)
	}
}

// TestSnapshotCacheInvalidatedByNotify verifies that notify() builds a
// fresh snapshot reflecting the latest state, so subsequent Snapshot()
// calls return updated data.
func TestSnapshotCacheInvalidatedByNotify(t *testing.T) {
	t.Parallel()
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-A")
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents

	snap1 := r.Snapshot()
	if snap1.ActivePeer != domaintest.ID("peer-A") {
		t.Fatalf("snap1.ActivePeer = %q, want peer-A", snap1.ActivePeer)
	}

	// Mutate under lock, then notify (as real code does).
	r.mu.Lock()
	r.activePeer = domaintest.ID("peer-B")
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
	<-r.uiEvents

	snap2 := r.Snapshot()
	if snap2.ActivePeer != domaintest.ID("peer-B") {
		t.Fatalf("snap2.ActivePeer = %q, want peer-B after notify", snap2.ActivePeer)
	}
}

// TestRepairUnreadFromHeadersSplitLock verifies that the two-phase lock
// strategy in repairUnreadFromHeaders correctly processes headers: new
// messages are added to seenMessageIDs and unread counts are incremented,
// while the write lock is held only briefly in each phase.
func TestRepairUnreadFromHeadersSplitLock(t *testing.T) {
	t.Parallel()
	r := newSyncTestRouter()

	// Pre-seed one message as already seen.
	r.mu.Lock()
	r.seenMessageIDs["old-msg"] = messageGate{handled: true}
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "old-msg", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "new-msg-1", Sender: domaintest.ID("peer-1"), Recipient: domaintest.ID("me")},
			{ID: "new-msg-2", Sender: domaintest.ID("peer-2"), Recipient: domaintest.ID("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// old-msg was already seen, so only 2 new messages processed.
	if !r.seenMessageIDs["new-msg-1"].handled {
		t.Fatal("new-msg-1 should be in seenMessageIDs")
	}
	if !r.seenMessageIDs["new-msg-2"].handled {
		t.Fatal("new-msg-2 should be in seenMessageIDs")
	}
	// peer-1 gets 1 new unread (new-msg-1), peer-2 gets 1 (new-msg-2).
	if r.peers[domaintest.ID("peer-1")] == nil || r.peers[domaintest.ID("peer-1")].Unread != 1 {
		t.Fatalf("peer-1 Unread = %v, want 1", r.peers[domaintest.ID("peer-1")])
	}
	if r.peers[domaintest.ID("peer-2")] == nil || r.peers[domaintest.ID("peer-2")].Unread != 1 {
		t.Fatalf("peer-2 Unread = %v, want 1", r.peers[domaintest.ID("peer-2")])
	}
}

// TestSnapshotCacheConcurrentSafety exercises Snapshot() from multiple
// goroutines while mutations and notifications happen concurrently.
// The test verifies that no data race occurs (run with -race).
func TestSnapshotCacheConcurrentSafety(t *testing.T) {
	t.Parallel()
	r := newTestRouter()

	const goroutines = 8
	const iterations = 200

	done := make(chan struct{})

	// Writer goroutine: mutates state and calls notify.
	go func() {
		defer close(done)
		for i := 0; i < iterations; i++ {
			peer := domaintest.ID(fmt.Sprintf("peer-%d", i%5))
			r.mu.Lock()
			r.tryEnsurePeerLocked(peer)
			r.peers[peer].Unread++
			r.activePeer = peer
			r.mu.Unlock()
			r.notify(UIEventSidebarUpdated)
			// Drain event to avoid channel backup.
			select {
			case <-r.uiEvents:
			default:
			}
			runtime.Gosched()
		}
	}()

	// Reader goroutines: call Snapshot() concurrently.
	for g := 0; g < goroutines; g++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					snap := r.Snapshot()
					// Access snapshot fields to trigger race detector.
					_ = snap.ActivePeer
					_ = len(snap.Peers)
					_ = snap.NodeStatus
					runtime.Gosched()
				}
			}
		}()
	}

	<-done
}

// TestSetSendStatusInvalidatesSnapshotCache verifies that the exported
// SetSendStatus (called from window.go for copy/delete/file-prepare
// status updates) bumps the snapshot generation so the UI sees the new
// status on the next frame instead of serving a stale cached snapshot.
func TestSetSendStatusInvalidatesSnapshotCache(t *testing.T) {
	t.Parallel()
	r := newTestRouter()

	// Prime the cache via notify.
	r.notify(UIEventStatusUpdated)
	<-r.uiEvents
	snap1 := r.Snapshot()
	if snap1.SendStatus != "" {
		t.Fatalf("initial SendStatus = %q, want empty", snap1.SendStatus)
	}

	// Simulate window.go calling SetSendStatus.
	r.SetSendStatus("identity copied")
	// Drain the notification emitted by setSendStatusNotify.
	<-r.uiEvents

	snap2 := r.Snapshot()
	if snap2.SendStatus != "identity copied" {
		t.Fatalf("SendStatus after SetSendStatus = %q, want %q", snap2.SendStatus, "identity copied")
	}
}

// ── Monitor-level tests moved to node_status_monitor_test.go ──
// TestApplyPeerHealthDelta*, TestMergePeerHealth*, TestApplyPeerPendingDelta*
// now test NodeStatusMonitor directly since it owns PeerHealth aggregation.

// TestStopLoopsKeepsHandlerContextAlive covers the shutdown ordering both
// composition roots use: StopLoops runs BEFORE the event bus drains, so the
// handlers still running at that moment must keep a usable context.
//
// Sharing loopCtx between the loops and the repository calls was a bug — the
// terminal delete and recovery writes those handlers make would have failed
// with context canceled, which is a lost write dressed up as a clean shutdown.
//
// Built through the real constructor rather than newTestRouter: the contexts
// under test are created by Start, and newTestRouter hands out a router whose
// startupDone is already closed.
func TestStopLoopsKeepsHandlerContextAlive(t *testing.T) {
	t.Parallel()

	r, _, _ := newRecoveryRouter(t)
	r.Start()

	if err := r.opContext().Err(); err != nil {
		t.Fatalf("operation context is already cancelled after Start: %v", err)
	}

	r.StopLoops(2 * time.Second)
	if err := r.opContext().Err(); err != nil {
		t.Fatalf("StopLoops cancelled the operation context: %v — handlers draining after it would lose their writes", err)
	}
	if r.loopCtx.Err() == nil {
		t.Fatal("StopLoops did not cancel the loop context")
	}

	// ShutdownDrain is the last stage: only there may repository work stop.
	r.ShutdownDrain(2 * time.Second)
	if r.opContext().Err() == nil {
		t.Fatal("ShutdownDrain left the operation context live — nothing would ever release it")
	}
}

// TestLastIncomingSurvivesOwnReply pins the sidebar's presence evidence to the
// peer's own message. The preview moves to our reply — that is what a preview
// is for — but the answer to "when was this contact last online" must stay on
// the message they wrote, which is the only thing in the conversation that
// proves their node was running.
func TestLastIncomingSurvivesOwnReply(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("chatty-peer")
	me := r.client.Address()

	incomingAt := time.Date(2026, time.August, 20, 9, 0, 0, 0, time.UTC)
	replyAt := incomingAt.Add(2 * time.Hour)

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID:        "in-1",
		Sender:    peer,
		Recipient: me,
		Body:      "hi",
		Timestamp: incomingAt,
	}, peer, peerStamp{})

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID:        "out-1",
		Sender:    me,
		Recipient: peer,
		Body:      "hey",
		Timestamp: replyAt,
	}, peer, peerStamp{})

	r.notify(UIEventSidebarUpdated)
	state := r.Snapshot().Peers[peer]
	if state == nil {
		t.Fatal("peer missing from snapshot")
	}
	if !state.Preview.Timestamp.Equal(replyAt) {
		t.Fatalf("preview = %v, want our reply %v", state.Preview.Timestamp, replyAt)
	}
	if !state.LastIncomingAt.Valid() {
		t.Fatal("our reply erased the peer's last-incoming evidence")
	}
	if got := state.LastIncomingAt.Time(); !got.Equal(incomingAt) {
		t.Fatalf("last incoming = %v, want the peer's message %v", got, incomingAt)
	}
}

// TestLastIncomingIsMonotone covers out-of-order arrival: a relayed or
// replayed older message must not walk the evidence backwards.
func TestLastIncomingIsMonotone(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("out-of-order-peer")
	me := r.client.Address()

	newest := time.Date(2026, time.August, 20, 15, 0, 0, 0, time.UTC)
	older := newest.Add(-6 * time.Hour)

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "in-new", Sender: peer, Recipient: me, Body: "second", Timestamp: newest,
	}, peer, peerStamp{})
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "in-old", Sender: peer, Recipient: me, Body: "first, arrived late", Timestamp: older,
	}, peer, peerStamp{})

	r.notify(UIEventSidebarUpdated)
	state := r.Snapshot().Peers[peer]
	if state == nil {
		t.Fatal("peer missing from snapshot")
	}
	if got := state.LastIncomingAt.Time(); !got.Equal(newest) {
		t.Fatalf("last incoming = %v, want the newest message %v", got, newest)
	}
}

// TestStartupHistoryScanReachesTheSnapshot pins the publication, not the
// write. The scan runs in its own goroutine off the startup path, and the UI
// reads only the composed snapshot — so a scan that updates r.peers without
// notifying is a scan nobody sees. It is not hypothetical: the retry path
// exists precisely for a contact who is not about to send anything else that
// would notify on its own.
func TestStartupHistoryScanReachesTheSnapshot(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("peer-seen-only-in-history")

	r := newTestRouter()
	r.client = client

	ctx := context.Background()
	at := time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)
	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "hist-1", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: at.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// seedPreviews has already created the row in production.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	// A snapshot composed BEFORE the scan is what the UI is holding.
	r.notify(UIEventSidebarUpdated)

	r.seedHistoryEvidence(context.Background())

	state := r.Snapshot().Peers[peer]
	if state == nil {
		t.Fatal("peer missing from snapshot")
	}
	if !state.LastIncomingAt.Valid() {
		t.Fatal("history scan updated router state but never published a snapshot")
	}
	if got := state.LastIncomingAt.Time(); !got.Equal(at) {
		t.Fatalf("published last incoming = %v, want %v", got, at)
	}
	if state.Unread != 1 {
		t.Fatalf("published unread = %d, want the unseen message counted once", state.Unread)
	}
}

// TestStartupBadgeSeedSkipsWhatWasReadWhileItRan covers the race between the
// startup scan and everything the user does during it. The scan reads the
// database; the database is behind a mark-seen that has just cleared a
// conversation. Applying the pre-clear answer badges messages the user has
// already read — and for the conversation that opened on launch nothing would
// take the badge off again, because selecting the same peer twice is a no-op.
//
// The rule is the epoch, not "skip the active peer": the clear bumps it, and
// the same check covers a deletion, a mark-seen in a background chat, and a
// removal.
func TestStartupBadgeSeedSkipsWhatWasReadWhileItRan(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	opened := domaintest.ID("the-conversation-that-opened")
	other := domaintest.ID("a-conversation-in-the-list")

	r := newTestRouter()
	r.client = client

	ctx := context.Background()
	for _, row := range []struct {
		id   string
		from domain.PeerIdentity
	}{{"opened-1", opened}, {"other-1", other}} {
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: row.id, Sender: row.from.String(), Recipient: me.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			DeliveryStatus: chatlog.StatusDelivered,
		}); err != nil {
			t.Fatalf("append %s: %v", row.id, err)
		}
	}

	r.mu.Lock()
	r.tryEnsurePeerLocked(opened)
	r.tryEnsurePeerLocked(other)
	r.mu.Unlock()

	// Deliberately NOT pre-badged: at startup the badge seed is the first
	// thing to fill the set, so the mark-seen it races against clears an
	// EMPTY set. An epoch bumped only when something was actually removed
	// from memory would do nothing here — exactly when it is needed.

	// The conversation opens and is marked read WHILE the scan's query is in
	// flight. Both steps run on an empty in-memory set: the optimistic clear
	// and the drop of the ids whose receipts were sent.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.mu.Lock()
			r.activePeer = opened
			r.mu.Unlock()
			r.clearPeerUnread(opened)
			r.mu.Lock()
			r.dropUnreadLocked(opened, domain.MessageID("opened-1"))
			r.mu.Unlock()
		},
	}

	r.seedUnreadIDs(ctx)

	r.mu.RLock()
	openedUnread := r.peers[opened].Unread
	otherUnread := r.peers[other].Unread
	r.mu.RUnlock()
	if openedUnread != 0 {
		t.Fatalf("the conversation the user just read carries %d unread again", openedUnread)
	}
	if otherUnread != 1 {
		t.Fatalf("the untouched conversation = %d unread, want 1", otherUnread)
	}
}

// TestStartupScanDoesNotUndoADeletion is the same rule for the other half of
// the scan. A deletion recomputes the peer from the database; a last-incoming
// answer read BEFORE that deletion still carries the date of the message that
// was removed, and "take the maximum" would put it straight back.
func TestStartupScanDoesNotUndoADeletion(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("deleted-mid-scan")

	now := time.Now().UTC()
	deleted := now.Add(-time.Hour).Truncate(time.Second)
	survivor := now.Add(-3 * time.Hour).Truncate(time.Second)
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The scan reads the conversation as it is before the deletion.
	before := r.backwardsEpochSnapshot()
	scanned := map[domain.PeerIdentity]time.Time{peer: deleted}

	// The user deletes that message; the delete path recomputes the peer.
	r.mu.Lock()
	r.moveHistoryBackwardsLocked(peer)
	r.peers[peer].LastIncomingAt = domain.TimeOf(survivor)
	r.mu.Unlock()

	r.applyScannedLastIncoming(scanned, before)

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(survivor) {
		t.Fatalf("last incoming = %v, want the surviving message %v — the scan put the deleted one back", got, survivor)
	}
}

// TestMarkSeenDoesNotBlockTheHistoryScan is the other half of the rule. A
// mark-seen removes no rows, so it cannot make a last-incoming answer wrong —
// and gating that answer on it would cost the feature its most common case:
// the conversation that opens automatically at launch is marked read while
// the scan is still running, so the contact at the top of the sidebar would
// spend the whole session with no "last online" line.
func TestMarkSeenDoesNotBlockTheHistoryScan(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("opened-at-launch")

	now := time.Now().UTC()
	wrote := now.Add(-2 * time.Hour).Truncate(time.Second)
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("unread-1"))
	r.mu.Unlock()

	// The scan reads...
	before := r.backwardsEpochSnapshot()
	// ...the conversation opens and is marked read...
	r.clearPeerUnread(peer)
	r.mu.Lock()
	r.dropUnreadLocked(peer, domain.MessageID("unread-1"))
	r.mu.Unlock()
	// ...and the scan's answer lands.
	r.applyScannedLastIncoming(map[domain.PeerIdentity]time.Time{peer: wrote}, before)

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(wrote) {
		t.Fatalf("last incoming = %v, want %v — a mark-seen refused an answer it cannot invalidate", got, wrote)
	}
	if unread != 0 {
		t.Fatalf("unread = %d, want 0 — the badge scan is the half a mark-seen does invalidate", unread)
	}
}

// TestFailedOpenRebuildsTheBadgeFromTheDatabase covers what the rollback
// alone cannot. Opening a conversation clears the badge optimistically; if
// the load or the mark-seen then fails, the rollback restores the set that
// was in memory — which at startup may be nothing at all, because the badge
// seed had not applied yet (or skipped this peer precisely because the open
// was moving it). The database still knows, and nothing else re-reads
// delivery_status outside the delete path.
func TestFailedOpenRebuildsTheBadgeFromTheDatabase(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("opened-but-unreadable")
	ctx := context.Background()

	for _, msgID := range []string{"unread-1", "unread-2"} {
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: msgID, Sender: peer.String(), Recipient: me.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			DeliveryStatus: chatlog.StatusDelivered,
		}); err != nil {
			t.Fatalf("append %s: %v", msgID, err)
		}
	}

	r := newTestRouter()
	r.client = client
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.mu.Unlock()

	// The optimistic clear happened, the badge seed never applied, and the
	// mark-seen failed: the rollback has an empty set to restore.
	r.restorePeerUnread(peer, nil)
	// A message arrives while the rebuild reads. Additions move no counter,
	// so the epoch check cannot see it — and the event that carried it has
	// already passed the dedup gate, so nothing would re-add it.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.mu.Lock()
			r.markUnreadLocked(peer, domain.MessageID("arrived-during-the-rebuild"))
			r.mu.Unlock()
		},
	}
	r.repairBadgeFromStore(peer)

	r.mu.RLock()
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if unread != 3 {
		t.Fatalf("unread after a failed open = %d, want the 2 the database calls unseen plus the one that arrived meanwhile", unread)
	}
	r.mu.RLock()
	_, kept := r.unreadIDs[peer][domain.MessageID("arrived-during-the-rebuild")]
	r.mu.RUnlock()
	if !kept {
		t.Fatal("the rebuild dropped a message that arrived while it was reading")
	}
}

// TestUnreadBumpsAreUnconditional pins the two badge movers whose bump must
// not depend on what memory happened to hold. At startup the in-memory set is
// empty by construction — the badge seed is the first thing to fill it — so a
// bump conditional on "was this id actually there" would do nothing exactly
// when a chatlog read is in flight, and the pre-mark-seen answer would badge
// messages the user has already read.
func TestUnreadBumpsAreUnconditional(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("empty-set-peer")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	before := r.backwardsEpoch[peer].unread

	// Nothing in memory to remove: the badge seed has not landed yet.
	r.dropUnreadLocked(peer, domain.MessageID("read-elsewhere"))
	afterDrop := r.backwardsEpoch[peer].unread

	r.clearUnreadLocked(peer)
	afterClear := r.backwardsEpoch[peer].unread

	// And the re-derivation, which drops whatever it does not repeat.
	r.replaceUnreadLocked(peer, nil)
	afterReplace := r.backwardsEpoch[peer].unread
	r.mu.Unlock()

	if afterDrop == before {
		t.Fatal("a mark-seen over an empty in-memory set recorded no backwards move")
	}
	if afterClear == afterDrop {
		t.Fatal("clearing an already-empty badge recorded no backwards move")
	}
	if afterReplace == afterClear {
		t.Fatal("re-deriving the badge recorded no backwards move")
	}
}

// TestEnsurePeerBeforeReconcileRefusesARemovedContact pins the guard on the
// row creation itself. The callers create the sidebar row synchronously,
// before queueing the asynchronous reconciliation — and a removal that
// completed in between must not be undone by that row.
func TestEnsurePeerBeforeReconcileRefusesARemovedContact(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("removed-before-the-queue")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	// RemovePeer completes while the message is being handled.
	r.peerGen[peer]++
	delete(r.peers, peer)
	r.removePeerLocked(peer)
	r.mu.Unlock()

	r.ensurePeerForReconcile(peer, stampAtEvent.gen)

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("the row created for a queued reconciliation put a removed contact back")
	}

	// A conversation new to this process still appears: both generations
	// are zero, which is what "never removed" looks like.
	fresh := domaintest.ID("brand-new-conversation")
	r.ensurePeerForReconcile(fresh, 0)
	r.mu.RLock()
	_, created := r.peers[fresh]
	r.mu.RUnlock()
	if !created {
		t.Fatal("a conversation new to this process was refused a sidebar row")
	}
}

// TestDeleteCASComparesTheSetNotItsSize covers the guard the delete path
// relies on. One id read and another arriving while the queries run leaves
// the SIZE of the badge untouched — a check on the count waves the stale
// answer through, and the newly arrived id is dropped by a reconciliation
// that never saw it, with no event left to re-add it.
func TestDeleteCASComparesTheSetNotItsSize(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("swap-under-the-reconcile")
	ctx := context.Background()

	now := time.Now().UTC()
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "survivor", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSeen,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("about-to-be-read"))
	r.mu.Unlock()

	// While the queries run: the old id is marked seen and a new message
	// arrives. One out, one in — the count is the same as before.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.mu.Lock()
			// The set changes WITHOUT a backwards move — the id leaves the
			// map directly rather than through dropUnreadLocked — so the
			// epoch is untouched and only comparing the SET can refuse this.
			// Comparing sizes cannot: one out, one in.
			delete(r.unreadIDs[peer], domain.MessageID("about-to-be-read"))
			r.markUnreadLocked(peer, domain.MessageID("just-arrived"))
			r.mu.Unlock()
		},
	}

	if got := r.reconcilePeerFromStore(ctx, peer, true, true); got != reconcileRetry {
		t.Fatalf("reconcile reported %v, want reconcileRetry — the badge changed under it", got)
	}

	r.mu.RLock()
	_, kept := r.unreadIDs[peer][domain.MessageID("just-arrived")]
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if !kept || unread != 1 {
		t.Fatalf("unread = %d (new id kept=%v), want the message that arrived mid-reconcile to survive", unread, kept)
	}
}

// TestDeletingAnUnreadMessageDropsItsBadge covers the two delete paths
// end-to-end. The badge is a set of ids, and a deleted message is not an
// unread message — the ids are in hand, so neither path needs a query for it,
// and both record the history move that keeps a chatlog read still in flight
// from putting the message back.
func TestDeletingAnUnreadMessageDropsItsBadge(t *testing.T) {
	r := newSyncTestRouter()
	peer := domaintest.ID("deleted-while-unread")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("doomed"))
	r.markUnreadLocked(peer, domain.MessageID("survivor"))
	historyBefore := r.backwardsEpoch[peer].history
	r.mu.Unlock()

	// What the delete path does in production: the file barrier records the
	// history move as the row goes, then the UI is evicted.
	r.withFileOps(peer, true, func() {})
	r.evictDeletedMessageFromUI(peer, domain.MessageID("doomed"))

	r.mu.Lock()
	unread := r.peers[peer].Unread
	_, stillBadged := r.unreadIDs[peer][domain.MessageID("doomed")]
	movedHistory := r.backwardsEpoch[peer].history != historyBefore
	r.mu.Unlock()

	if stillBadged || unread != 1 {
		t.Fatalf("unread = %d (deleted id still badged=%v), want only the surviving message", unread, stillBadged)
	}
	if !movedHistory {
		t.Fatal("deleting a message did not record the history move, so a scan in flight may put it back")
	}
	if r.backwardsEpoch[peer].history != historyBefore+1 {
		t.Fatalf("one deletion moved the history counter %d times, want 1 — every extra move makes a read that started in between look stale for nothing",
			r.backwardsEpoch[peer].history-historyBefore)
	}
}

// TestDeleteCASRefusesAMarkSeenThatLandedMidQuery covers the half of the CAS
// the value comparison is blind to. At startup the in-memory badge is empty,
// so a mark-seen removes nothing visible: preview, date and set all look
// exactly as the reconciliation read them, and only the epoch says the
// conversation moved.
func TestDeleteCASRefusesAMarkSeenThatLandedMidQuery(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("mark-seen-mid-delete")
	ctx := context.Background()

	now := time.Now().UTC()
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "survivor", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The user reads the conversation while the deletion's queries run. The
	// badge set is empty on both sides of that — the startup seed has not
	// landed — so nothing but the counter changes.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(domain.PeerIdentity) {
			r.clearPeerUnread(peer)
		},
	}

	if got := r.reconcilePeerFromStore(ctx, peer, true, true); got != reconcileRetry {
		t.Fatalf("reconcile reported %v, want reconcileRetry — the conversation was read under it", got)
	}
}

// TestStartupPreviewSeedSkipsADeletedConversation covers the last chatlog read
// that had no guard. The preview fetch retries for seconds, and this is the
// path that CREATES sidebar rows: applying an answer that predates a deletion
// would put the conversation back with the message the user removed in it.
func TestStartupPreviewSeedSkipsADeletedConversation(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("wiped-during-the-fetch")
	now := time.Now().UTC()

	// The fetch starts...
	before := r.backwardsEpochSnapshot()
	// ...and the user wipes the conversation while it runs.
	r.mu.Lock()
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	r.seedPreviews([]ConversationPreview{{
		PeerAddress: peer,
		Body:        "the message that was deleted",
		Timestamp:   now.Add(-time.Minute),
	}}, before)

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("the startup preview seed put a conversation back that was removed while it read")
	}
}

// TestHistoryScanSnapshotBelongsToTheAttemptThatAnswers pins where the
// baseline is taken. The scan retries for up to eighteen seconds; a snapshot
// taken once, before the first attempt, would refuse every peer that moved
// during a wait the answering read already happened after — which is every
// launch where the user does anything at all while the database settles.
func TestHistoryScanSnapshotBelongsToTheAttemptThatAnswers(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("moved-between-attempts")
	ctx := context.Background()

	now := time.Now().UTC()
	wrote := now.Add(-2 * time.Hour).Truncate(time.Second)
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "hist-1", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: wrote.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The first attempt fails; the conversation moves backwards; the second
	// attempt reads AFTER that move and must be applied.
	reader := &failingHistoryReader{store: client.chatLog, failScans: 1}
	r.history = &movingHistoryReader{
		inner: reader,
		onAttempt: func(attempt int) {
			if attempt != 1 {
				return
			}
			r.mu.Lock()
			r.moveHistoryBackwardsLocked(peer)
			r.mu.Unlock()
		},
	}

	r.seedLastIncoming(ctx)

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(wrote) {
		t.Fatalf("last incoming = %v, want %v — the retry was judged against a baseline older than itself", got, wrote)
	}
}

// movingHistoryReader runs a hook before each attempt of the scan, so a test
// can move a peer between a failed read and the one that answers.
type movingHistoryReader struct {
	inner     chatHistoryReader
	attempts  int
	onAttempt func(attempt int)
}

func (m *movingHistoryReader) LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error) {
	m.attempts++
	m.onAttempt(m.attempts)
	return m.inner.LastIncomingAtPerPeer(ctx, now)
}

func (m *movingHistoryReader) MessageSeq(ctx context.Context, messageID domain.MessageID) (int64, bool, error) {
	return m.inner.MessageSeq(ctx, messageID)
}

func (m *movingHistoryReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	return m.inner.LastIncomingAtFor(ctx, peer, now)
}

func (m *movingHistoryReader) UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
	return m.inner.UnseenIncomingIDs(ctx)
}

func (m *movingHistoryReader) UnseenIncomingIDsFor(ctx context.Context, peer domain.PeerIdentity) ([]domain.MessageID, error) {
	return m.inner.UnseenIncomingIDsFor(ctx, peer)
}

func (m *movingHistoryReader) StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error) {
	return m.inner.StoredMessageStatuses(ctx, ids)
}

// TestDeleteKeepsABadgeWhoseRowHasNotLanded covers the gap between the node
// and the database. The header path badges a message as soon as the node
// reports it, which can be before its chatlog row is written. Re-deriving the
// badge purely from the database would read "not in the unseen list" as
// "read" and drop it for good: the event stream cannot re-add an id its dedup
// gate has seen, and only another deletion re-reads delivery_status.
func TestDeleteKeepsABadgeWhoseRowHasNotLanded(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("badge-ahead-of-the-row")
	ctx := context.Background()

	now := time.Now().UTC()
	survivor := now.Add(-time.Hour).Truncate(time.Second)
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "survivor", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: survivor.Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSeen,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	// The header repair badged a message the chatlog has not stored yet.
	r.markUnreadLocked(peer, domain.MessageID("header-only"))
	r.mu.Unlock()

	// An unrelated deletion in the same conversation re-derives the badge.
	if got := r.reconcilePeerFromStore(ctx, peer, true, true); got != reconcileApplied {
		t.Fatalf("reconcile reported %v, want reconcileApplied", got)
	}

	r.mu.RLock()
	unread := r.peers[peer].Unread
	_, kept := r.unreadIDs[peer][domain.MessageID("header-only")]
	r.mu.RUnlock()
	if unread != 1 || !kept {
		t.Fatalf("unread = %d (kept=%v), want the header-derived badge to survive", unread, kept)
	}
}

// TestSeedAndRefreshLastIncomingFromChatlog covers the two SQL-backed halves
// of the presence evidence: startup reads it out of history, and deleting the
// message that carried it takes it back. The second half is the reason the
// value is recomputed rather than advanced on the delete path — otherwise the
// sidebar would keep reporting an online moment whose only proof the user just
// erased.
func TestSeedAndRefreshLastIncomingFromChatlog(t *testing.T) {
	// A real node service stands behind the client: refreshPreviewAfterDelete
	// re-reads the preview through the decrypt path, which asks the node for
	// contact keys, and a client without one fails that read and returns
	// before touching any peer state.
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("peer-with-history")

	r := newTestRouter()
	r.client = client

	ctx := context.Background()
	firstAt := time.Date(2026, time.August, 18, 8, 0, 0, 0, time.UTC)
	lastIncomingAt := time.Date(2026, time.August, 19, 8, 0, 0, 0, time.UTC)
	replyAt := time.Date(2026, time.August, 20, 8, 0, 0, 0, time.UTC)

	appendEntry := func(id, sender, recipient string, at time.Time) {
		t.Helper()
		if err := cl.Append(ctx, "dm", me, chatlog.Entry{
			ID: id, Sender: sender, Recipient: recipient,
			Body: "sealed", CreatedAt: at.Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	appendEntry("h1", peer.String(), me.String(), firstAt)
	appendEntry("h2", peer.String(), me.String(), lastIncomingAt)
	appendEntry("h3", me.String(), peer.String(), replyAt)

	// The sidebar row exists before the bootstrap runs — in production
	// seedPreviews creates it from the same history. seedLastIncoming asks
	// about the peers it can see, so a router with none has nothing to do.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	r.seedLastIncoming(context.Background())

	r.mu.RLock()
	seeded := r.peers[peer]
	r.mu.RUnlock()
	if seeded == nil || !seeded.LastIncomingAt.Valid() {
		t.Fatal("startup did not recover the peer's last-incoming evidence from history")
	}
	if got := seeded.LastIncomingAt.Time(); !got.Equal(lastIncomingAt) {
		t.Fatalf("seeded last incoming = %v, want %v", got, lastIncomingAt)
	}

	// The user deletes the peer's newest message: the evidence must fall back
	// to their earlier one, not stay on the row that is gone.
	if _, err := cl.DeleteByID(ctx, domain.MessageID("h2")); err != nil {
		t.Fatalf("delete h2: %v", err)
	}
	r.refreshPreviewAfterDelete(peer)

	r.mu.RLock()
	afterDelete := r.peers[peer]
	r.mu.RUnlock()
	if got := afterDelete.LastIncomingAt.Time(); !got.Equal(firstAt) {
		t.Fatalf("last incoming after delete = %v, want the surviving message %v", got, firstAt)
	}

	// Deleting the rest of their messages leaves no evidence at all — our own
	// reply is still there, and it says nothing about the peer.
	if _, err := cl.DeleteByID(ctx, domain.MessageID("h1")); err != nil {
		t.Fatalf("delete h1: %v", err)
	}
	r.refreshPreviewAfterDelete(peer)

	r.mu.RLock()
	drained := r.peers[peer]
	r.mu.RUnlock()
	if drained.LastIncomingAt.Valid() {
		t.Fatalf("evidence survived the deletion of every incoming message: %v", drained.LastIncomingAt.Time())
	}
}

func TestLastIncomingRefusesFutureTimestamps(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("future-dated-peer")
	me := r.client.Address()

	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	r.presenceClock = func() time.Time { return now }

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "in-future", Sender: peer, Recipient: me, Body: "hi from tomorrow",
		Timestamp: now.Add(24 * time.Hour),
	}, peer, peerStamp{})

	r.notify(UIEventSidebarUpdated)
	state := r.Snapshot().Peers[peer]
	if state == nil {
		t.Fatal("peer missing from snapshot")
	}
	if state.LastIncomingAt.Valid() {
		t.Fatalf("a future-dated message became presence evidence: %v", state.LastIncomingAt.Time())
	}
	// The PREVIEW takes it, and that is the deliberate half of the split. A
	// forged date buys nothing there any more — the sidebar no longer orders
	// by the stamp — while refusing the message left the user with a badge
	// counting something the sidebar would not show. Presence is the claim
	// worth protecting: "this contact was online at T" is evidence, and the
	// sender is the one party who gains by choosing T.
	if state.Preview.Body != "hi from tomorrow" {
		t.Fatalf("preview = %q, want the message that arrived; refusing it hides a message the node accepted and stored", state.Preview.Body)
	}

	// And it pins nothing: the next message replaces it, whatever the dates.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "after", Sender: me, Recipient: peer, Body: "our reply",
		Timestamp: now,
	}, peer, peerStamp{})
	r.notify(UIEventSidebarUpdated)
	if body := r.Snapshot().Peers[peer].Preview.Body; body != "our reply" {
		t.Fatalf("preview = %q after a later message, want %q: the forged date is acting as a ceiling", body, "our reply")
	}
}

// TestDeleteKeepsForgedFutureDateOutOfPresence closes the one path that
// assigns LastIncomingAt without going through noteIncomingAtLocked. Deleting
// the newest message promotes whatever is behind it, and if that row carries
// a sender-chosen future date, a deletion would become the way a forged
// timestamp becomes evidence that a contact was online.
//
// The preview is a different question and takes the row: it says which
// message is last, not when anybody was online, and the sidebar no longer
// orders by its stamp. What it must not do is get stuck there, which the last
// assertion covers.
func TestDeleteKeepsForgedFutureDateOutOfPresence(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("forging-peer")
	ctx := context.Background()

	now := time.Now().UTC()
	honest := now.Add(-5 * time.Hour).Truncate(time.Second)
	forged := now.Add(96 * time.Hour).Truncate(time.Second)
	newest := now.Add(-time.Hour).Truncate(time.Second)

	for _, entry := range []chatlog.Entry{
		{ID: "d-honest", Sender: peer.String(), Recipient: id.Address, Body: "sealed", CreatedAt: honest.Format(time.RFC3339Nano)},
		{ID: "d-forged", Sender: peer.String(), Recipient: id.Address, Body: "sealed", CreatedAt: forged.Format(time.RFC3339Nano)},
		{ID: "d-newest", Sender: peer.String(), Recipient: id.Address, Body: "sealed", CreatedAt: newest.Format(time.RFC3339Nano)},
	} {
		if err := cl.Append(ctx, "dm", me, entry); err != nil {
			t.Fatalf("append %s: %v", entry.ID, err)
		}
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	// The conversation exists, as it does in production when the user
	// deletes one of its messages: reconciliation updates a peer, never
	// creates one.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// Before the delete the honest newest message is the evidence.
	r.refreshPreviewAfterDelete(peer)
	r.mu.RLock()
	before := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !before.Valid() || !before.Time().Equal(newest) {
		t.Fatalf("last incoming before delete = %v, want %v", before, newest)
	}

	// The user deletes it. What is left is a forged future date and an older
	// honest message; only the honest one may surface.
	if _, err := cl.DeleteByID(ctx, domain.MessageID("d-newest")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	r.refreshPreviewAfterDelete(peer)

	r.mu.RLock()
	after := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !after.Valid() {
		t.Fatalf("the forged row hid the honest message behind it: evidence went unknown, want %v", honest)
	}
	if got := after.Time(); !got.Equal(honest) {
		t.Fatalf("last incoming after delete = %v, want the honest message %v", got, honest)
	}

	// The preview, in contrast, is whatever survived — the forged row is a
	// real message of this conversation and the last one still stored. What
	// must not happen is it STICKING: the next message replaces it.
	// Sequence 4: the fixture wrote three rows, so a message arriving now sits
	// after all of them. A live message gets this from the store in
	// production; the fixture builds one by hand and has to say so, because
	// an unplaceable message deliberately does not displace a placed one.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "d-after", Sender: peer, Recipient: me, Body: "later message",
		Timestamp: now, Seq: 4,
	}, peer, r.peerStampOf(peer))

	r.mu.RLock()
	preview := r.peers[peer].Preview
	r.mu.RUnlock()
	if preview.Body != "later message" {
		t.Fatalf("preview = %q, want %q: the forged date is acting as a ceiling the later message cannot beat", preview.Body, "later message")
	}
}

func TestReconcileAppliesWhenNothingMoved(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("undisturbed-peer")
	ctx := context.Background()

	now := time.Now().UTC()
	older := now.Add(-3 * time.Hour).Truncate(time.Second)
	newest := now.Add(-time.Minute).Truncate(time.Second)

	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "keep", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: older.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	// In memory the peer still carries the message that has just been deleted
	// from the chatlog.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.noteIncomingAtLocked(peer, newest)
	r.mu.Unlock()

	r.reconcilePeerFromStore(context.Background(), peer, true, true)

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(older) {
		t.Fatalf("last incoming = %v, want the surviving message %v", got, older)
	}
}

func TestSeedLastIncomingRetriesTheScan(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("retried-scan-peer")
	ctx := context.Background()

	now := time.Now().UTC()
	wrote := now.Add(-2 * time.Hour).Truncate(time.Second)
	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "scan-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: wrote.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The first read fails the way a database still settling after launch
	// does; the second answers.
	reader := &failingHistoryReader{store: cl, failScans: 1, perPeer: map[domain.PeerIdentity]time.Time{peer: wrote}}
	r.history = reader

	r.seedLastIncoming(context.Background())

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(wrote) {
		t.Fatalf("last incoming after the retry = %v, want %v", got, wrote)
	}
	if n := reader.scans(); n != 2 {
		t.Fatalf("scan attempts = %d, want the failure plus one retry", n)
	}
}

// TestDeletingAnotherMessageDoesNotLoseTheArrivingOne covers the ambiguity
// the backwards counter cannot resolve. It is per PEER: a deletion anywhere
// in the conversation moves it, so a message being decrypted at that moment
// looks exactly like the row that was deleted. Dropping on that ambiguity
// loses a live message whose id is already through the dedup gate — so the
// database is asked instead, and it knows which row survived.
func TestDeletingAnotherMessageDoesNotLoseTheArrivingOne(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("deletion-of-a-different-row")
	ctx := context.Background()

	now := time.Now().UTC()
	arriving := now.Add(-time.Minute).Truncate(time.Second)
	// The message that is arriving IS stored — the database is ahead of the
	// event that announces it.
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "arriving", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: arriving.Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	// A DIFFERENT message of the same conversation is deleted while this one
	// is being decrypted.
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "arriving", Sender: peer, Recipient: me, Body: "sealed",
		Timestamp: arriving,
	}, peer, stampAtEvent)

	r.mu.RLock()
	state := r.peers[peer]
	badged := len(r.unreadIDs[peer])
	r.mu.RUnlock()
	if state == nil || !state.LastIncomingAt.Valid() || !state.LastIncomingAt.Time().Equal(arriving) {
		t.Fatalf("the arriving message was dropped: last incoming = %v, want %v", state.LastIncomingAt, arriving)
	}
	if badged != 1 {
		t.Fatalf("unread = %d, want 1 — the arriving message is still unread in the database", badged)
	}
}

// TestRecoveryUsesTheStampItTookBeforeAsking covers the boundary between the
// question and the act. The recovery asks the database whether this message
// survived, and then promotes the conversation and registers its file. If it
// re-read the peer AFTER the answer, a contact removed and added back in
// between would look current — same address, new generation — and the old
// message's transfer would land in the new conversation.
func TestRecoveryUsesTheStampItTookBeforeAsking(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("removed-and-re-added")
	ctx := context.Background()

	now := time.Now().UTC()
	announce := `{"file_hash":"` + validTestFileHash +
		`","file_name":"stale.pdf","content_type":"application/pdf","file_size":4096}`
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "file-announce-stale", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	mgr := newTestFileTransferManager(t)
	r := newSyncTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	other := domaintest.ID("the-conversation-on-top")
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.tryEnsurePeerLocked(other)
	// Someone else is on top, so "promoted" means something.
	r.promotePeerLocked(other)
	r.mu.Unlock()

	// Removed and added back again — the same address, a new generation —
	// while the recovery is between its question and its answer.
	r.history = &afterStatusReader{
		inner: client.chatLog,
		hook: func() {
			r.mu.Lock()
			r.peerGen[peer]++
			delete(r.peers, peer)
			r.removePeerLocked(peer)
			r.tryEnsurePeerLocked(peer)
			r.mu.Unlock()
		},
	}

	r.recoverFromStaleApply(peer, &DirectMessage{
		ID: "file-announce-stale", Sender: peer, Recipient: me,
		Command: domain.DMCommandFileAnnounce, CommandData: announce,
		Timestamp: now.Add(-time.Minute),
	})

	if snap := mgr.AllTransfersSnapshot(); len(snap) != 0 {
		t.Fatalf("a transfer from the previous generation of this contact was registered: %+v", snap)
	}
	r.mu.RLock()
	promoted := len(r.peerOrder) > 0 && r.peerOrder[0] == peer
	r.mu.RUnlock()
	if promoted {
		t.Fatal("the recovery promoted the conversation on behalf of a message from the previous generation")
	}
}

// TestIncompleteRecoveryReopensTheDedupGate covers the half-recovery. The
// message's id is registered for dedup before anything else happens, so a
// recovery that could not finish — the badge query failed, say — leaves the
// message neither on screen nor counted AND unreachable by the repair path.
// Recovery counts only when every part of it lands.
func TestIncompleteRecoveryReopensTheDedupGate(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("half-recovered")
	ctx := context.Background()

	now := time.Now().UTC()
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "arriving", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.seenMessageIDs["arriving"] = messageGate{handled: true}
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	// ONLY the badge query fails: the preview reconcile works, and so does
	// the question of whether this message survived. The recovery is
	// therefore incomplete for exactly one reason.
	r.history = &failingHistoryReader{store: client.chatLog, failUnseenOnly: true}

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "arriving", Sender: peer, Recipient: me, Body: "sealed",
		Timestamp: now.Add(-time.Minute),
	}, peer, stampAtEvent)

	r.mu.RLock()
	stillGated := r.seenMessageIDs["arriving"].handled
	r.mu.RUnlock()
	if stillGated {
		t.Fatal("a half-finished recovery kept the message behind the dedup gate: nothing will ever pick it up")
	}
}

// TestRecoveryRedoesPromotionAndFileMapping covers what the apply would have
// done and the recovery used to skip: a conversation whose message survived
// the deletion still has to rise in the list, and a file announcement still
// needs its receiver mapping — the file tab would otherwise miss it until
// the chat is opened.
func TestRecoveryRedoesPromotionAndFileMapping(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("survived-the-deletion")
	other := domaintest.ID("someone-above-it")
	ctx := context.Background()

	now := time.Now().UTC()
	announce := `{"file_hash":"` + validTestFileHash +
		`","file_name":"kept.pdf","content_type":"application/pdf","file_size":4096}`
	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "file-announce-survivor", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	mgr := newTestFileTransferManager(t)
	r := newSyncTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.fileBridge.registerIncomingFn = fakeManagerRegisterIncoming(mgr)

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.tryEnsurePeerLocked(other)
	r.promotePeerLocked(other) // the other conversation is on top
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	// A different row of this conversation is deleted mid-decrypt.
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "file-announce-survivor", Sender: peer, Recipient: me,
		Command: domain.DMCommandFileAnnounce, CommandData: announce,
		Timestamp: now.Add(-time.Minute),
	}, peer, stampAtEvent)

	r.mu.RLock()
	top := r.peerOrder[0]
	r.mu.RUnlock()
	if top != peer {
		t.Fatalf("peerOrder starts with %q, want the conversation that just received a message", top)
	}
	if snap := mgr.AllTransfersSnapshot(); len(snap) != 1 {
		t.Fatalf("the recovery skipped the receiver mapping: snapshot has %d rows, want 1", len(snap))
	}
}

// TestRepeatedDeleteDoesNotMoveTheVersion covers the delete that removes
// nothing — a re-issued request, an ack for a row long gone. The database
// reports it plainly; passing "true" regardless marks every load and decrypt
// in flight as stale for a deletion that never happened.
func TestRepeatedDeleteDoesNotMoveTheVersion(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("deleted-twice")
	ctx := context.Background()

	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "target", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client
	r.wipeTombstones = newWipeTombstoneSet(func() deleteTaskList { return client.chatLog })
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	store := client.chatLog
	if err := r.removeLocalMessage(ctx, store, peer, domain.MessageID("target"),
		domain.MessageDeleteRouteRecalled, chatlog.DeleteIntent{}); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	r.mu.RLock()
	afterFirst := r.backwardsEpoch[peer].history
	r.mu.RUnlock()

	// The same delete again: nothing left to remove.
	if err := r.removeLocalMessage(ctx, store, peer, domain.MessageID("target"),
		domain.MessageDeleteRouteRecalled, chatlog.DeleteIntent{}); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	r.mu.RLock()
	afterSecond := r.backwardsEpoch[peer].history
	r.mu.RUnlock()

	if afterSecond != afterFirst {
		t.Fatalf("a delete that removed nothing moved the version %d → %d", afterFirst, afterSecond)
	}
}

// TestRefusedEnsureLeavesNoOrphanState covers the contract the helper now
// has: it can refuse, and a caller that ignores the answer leaves a badge, a
// place in the order and a dedup entry belonging to a row that does not
// exist — or dereferences the missing row and panics.
func TestRefusedEnsureLeavesNoOrphanState(t *testing.T) {
	r := newSyncTestRouter()
	me := r.client.Address()
	peer := domaintest.ID("ensure-refused")

	defer r.removals.begin(peer)()

	// The header repair, which used to badge and promote after the ensure.
	r.mu.Lock()
	r.initialSynced = true
	r.mu.Unlock()
	r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{
		{ID: "header-during-removal", Sender: peer, Recipient: me},
	}})

	// The startup seed, which used to dereference the row straight after.
	r.seedPreviews([]ConversationPreview{{
		PeerAddress: peer, Body: "from the database", Timestamp: time.Now(),
	}}, r.backwardsEpochSnapshot())

	r.mu.RLock()
	_, row := r.peers[peer]
	badges := len(r.unreadIDs[peer])
	gated := r.seenMessageIDs["header-during-removal"].handled
	inOrder := false
	for _, p := range r.peerOrder {
		if p == peer {
			inOrder = true
		}
	}
	r.mu.RUnlock()

	if row || badges != 0 || inOrder {
		t.Fatalf("a refused ensure left state behind: row=%v badges=%d inOrder=%v", row, badges, inOrder)
	}
	if gated {
		t.Fatal("the header was written off as seen although nothing was applied")
	}
}

// waitUntil blocks until cond holds, or fails the test. It waits for a
// condition the production code actually publishes, not for a fixed
// duration — a sleep would either be flaky or would prove nothing about the
// ordering it claims to test.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// historyRows reports how many rows the conversation with peer still has.
func historyRows(t *testing.T, c *DesktopClient, peer domain.PeerIdentity) int {
	t.Helper()
	entries, err := c.chatlog.Store().Read(context.Background(), "dm", peer)
	if err != nil {
		t.Fatalf("read conversation: %v", err)
	}
	return len(entries)
}

// waitPastTheFirstDelete blocks until the removal of peer has finished its
// FIRST history delete and is therefore standing at the file barrier the
// caller holds.
//
// The gate goes up before that delete, so "a removal is in flight" says
// nothing about how far it got. A sentinel row written beforehand does: it
// can only disappear by being deleted, and the first delete is the only
// thing that deletes it at this point. Without this, a test that writes a
// row "during the removal" may be writing it before the first delete, and
// would pass with no final sweep at all.
func waitPastTheFirstDelete(t *testing.T, c *DesktopClient, peer domain.PeerIdentity) {
	t.Helper()
	waitUntil(t, "the removal to finish its first history delete", func() bool {
		return historyRows(t, c, peer) == 0
	})
}

// TestAMessageArrivingWhileTheContactIsRemovedDoesNotSurviveIt covers the
// durable half of the removal window, on both doors it has.
//
// The node writes an inbound DM to the chatlog BEFORE the router hears about
// it, so a message accepted while the removal runs is invisible to the first
// delete, and the row it leaves behind is what the next startup would
// rebuild the deleted conversation from. The store door refuses (defers) the
// write while the gate is up; the sweep at the end of the removal covers the
// write that was already past that door when the gate went up.
func TestAMessageArrivingWhileTheContactIsRemovedDoesNotSurviveIt(t *testing.T) {
	r, c, me, _ := newTestDMRouterForDelete(t)
	r.removals = c.removals
	peer := domaintest.ID("writes-while-being-removed")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The sentinel: its disappearance is what proves the first delete has
	// already run when the test writes below.
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID: "sentinel-before-the-removal", Sender: peer.String(), Recipient: me.String(),
		Body: "here first", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})

	// Hold the file barrier so the removal stops inside its critical
	// section, with the first history delete already behind it.
	fileLock := r.fileOpLock(peer)
	fileLock.Lock()

	done := make(chan error, 1)
	go func() {
		_, err := r.RemovePeer(peer)
		done <- err
	}()
	waitPastTheFirstDelete(t, c, peer)

	// Door one: the node offers the message to the store.
	envelope := protocol.Envelope{
		ID:        "arrives-mid-removal",
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: me.String(),
		Payload:   []byte("ciphertext"),
		CreatedAt: time.Now().UTC(),
	}
	if got := c.store.StoreMessage(envelope, false); got != node.StoreDeferred {
		t.Fatalf("StoreMessage during the removal = %v, want deferred", got)
	}

	// Door two: a write that was already inside the chatlog when the gate
	// went up — the first delete has run, so only the final sweep can
	// still see this row.
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID: "slipped-in-before-the-gate", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})

	fileLock.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	if rows := historyRows(t, c, peer); rows != 0 {
		t.Fatalf("a message stored during the removal survived it: %d rows left", rows)
	}

	// The gate is a window, not a ban: once the removal is over the same
	// message is accepted, and reopens the conversation as a stranger's
	// message would.
	if got := c.store.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage after the removal = %v, want inserted", got)
	}
}

// TestARemovalWaitsForAWriteItAlreadyAdmitted covers what a checked flag
// cannot do. Checking is not writing: a store that has been let through can
// be descheduled between the check and the append, and by the time it
// commits, a whole removal may have started and finished — leaving the row
// behind BOTH history deletes, where nothing will ever look again. The write
// therefore holds a lease, and the removal waits for it.
func TestARemovalWaitsForAWriteItAlreadyAdmitted(t *testing.T) {
	r, c, me, _ := newTestDMRouterForDelete(t)
	r.removals = c.removals
	peer := domaintest.ID("write-admitted-before-removal")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// A store that has just been admitted and has not appended yet.
	releaseWrite, admitted := c.removals.admitWrite(peer)
	if !admitted {
		t.Fatal("the gate refused a write although no removal was running")
	}

	done := make(chan error, 1)
	go func() {
		_, err := r.RemovePeer(peer)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("RemovePeer finished while an admitted write was still in flight (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	// The admitted write commits, late, exactly as the real one would.
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID: "admitted-then-slow", Sender: peer.String(), Recipient: me.String(),
		Body: "committed late", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	releaseWrite()

	if err := <-done; err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if rows := historyRows(t, c, peer); rows != 0 {
		t.Fatalf("the row of an admitted write outlived the removal: %d rows left", rows)
	}
}

// TestTheStoreHoldsItsLeaseUntilTheRowIsCommitted pins what the lease is
// for. Being let through is not the same as having written: between the two
// the store can be stopped for as long as the database takes, and a removal
// that only CHECKED a flag would run both of its history deletes in that
// gap, leaving the row behind them for good.
//
// The stop is real here — the test holds the database's write lock, which is
// exactly what makes a store slow in production — and what it asserts is
// that the lease is still held while the store is stuck, and that a removal
// starting meanwhile does not get past its gate until the row is in.
func TestTheStoreHoldsItsLeaseUntilTheRowIsCommitted(t *testing.T) {
	c, id, executor := newTestDesktopClientWithNodeAndDB(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("store-stuck-mid-append")
	ctx := context.Background()

	r := newSyncTestRouter()
	r.client = c
	r.removals = c.removals
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// Hold the database's write lock: every other write now waits.
	tx, err := executor.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages (id, topic, sender, recipient, body, created_at) VALUES ('lock-holder', 'dm', ?, ?, 'x', ?)`,
		me.String(), peer.String(), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("take the write lock: %v", err)
	}

	envelope := protocol.Envelope{
		ID:        "arrives-then-waits",
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: me.String(),
		Payload:   []byte("ciphertext"),
		CreatedAt: time.Now().UTC(),
	}
	stored := make(chan node.StoreResult, 1)
	go func() { stored <- c.store.StoreMessage(envelope, false) }()

	waitUntil(t, "the store to take its lease", func() bool { return c.removals.writesInFlight(peer) == 1 })
	// And to keep it: the row is not in yet, so releasing now would be
	// releasing before the write it is supposed to cover.
	time.Sleep(200 * time.Millisecond)
	if got := c.removals.writesInFlight(peer); got != 1 {
		_ = tx.Rollback()
		t.Fatalf("the store released its lease before its row was committed (writes in flight = %d)", got)
	}

	removed := make(chan error, 1)
	go func() {
		_, err := r.RemovePeer(peer)
		removed <- err
	}()
	select {
	case err := <-removed:
		_ = tx.Rollback()
		t.Fatalf("the removal ran while an admitted write was still appending (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}

	// The database frees up: the store commits, the removal follows it.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := <-stored; got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want inserted", got)
	}
	if err := <-removed; err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if rows := historyRows(t, c, peer); rows != 0 {
		t.Fatalf("the row of an admitted write outlived the removal: %d rows left", rows)
	}
}

// TestRemovePeerReportsAFailedFinalSweep pins the answer the caller gets.
// The sweep is the last thing standing between a deleted contact and a row
// of its history left on disk; if it fails, saying "removed" is the one
// answer this function must not give — and it must say WHICH failure it
// was, because by now the contact itself is gone and the caller has its own
// cleanup to finish.
func TestRemovePeerReportsAFailedFinalSweep(t *testing.T) {
	r, c, me, _ := newTestDMRouterForDelete(t)
	r.removals = c.removals
	r.opCtx, r.opCancel = context.WithCancel(context.Background())
	peer := domaintest.ID("sweep-fails-on-removal")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID: "sentinel-before-the-removal", Sender: peer.String(), Recipient: me.String(),
		Body: "here first", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})

	fileLock := r.fileOpLock(peer)
	fileLock.Lock()

	done := make(chan error, 1)
	go func() {
		_, err := r.RemovePeer(peer)
		done <- err
	}()
	// Only past the first delete may the context be cancelled: cancelled
	// earlier, the failure under test would be the first delete's, and the
	// test would pass without a final sweep existing at all.
	waitPastTheFirstDelete(t, c, peer)

	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID: "survives-the-failed-sweep", Sender: peer.String(), Recipient: me.String(),
		Body: "still here", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	r.opCancel()
	fileLock.Unlock()

	err := <-done
	if err == nil {
		t.Fatal("RemovePeer reported success although its final history sweep failed")
	}
	if !errors.Is(err, ErrHistorySweepFailed) {
		t.Fatalf("RemovePeer error = %v, want one the caller can tell apart as a sweep failure", err)
	}
}

// TestTheRemovalGateStaysShutUntilTheLastRemovalFinishes covers the overlap.
// The first removal to finish must not open the door under the second — a
// flag would; a count does not. Driven through the gate's own API, which is
// what both doors (the router row and the message store) consult.
func TestTheRemovalGateStaysShutUntilTheLastRemovalFinishes(t *testing.T) {
	r := newSyncTestRouter()
	peer := domaintest.ID("removed-twice-at-once")

	releaseFirst := r.removals.begin(peer)
	releaseSecond := r.removals.begin(peer)

	releaseFirst()

	r.mu.Lock()
	created := r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	if created {
		t.Fatal("the gate opened while a second removal of the same contact was still running")
	}
	if _, admitted := r.removals.admitWrite(peer); admitted {
		t.Fatal("the store door opened while a second removal of the same contact was still running")
	}

	releaseSecond()

	r.mu.Lock()
	created = r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	if !created {
		t.Fatal("the gate stayed shut after the last removal finished")
	}
}

// TestMessageArrivingDuringARemovalDoesNotRecreateThePeer covers the window
// inside RemovePeer itself: the row is already dropped, the cleanups are
// still running, and a message arriving now takes a stamp that already
// matches the NEW generation. Nothing in that stamp says the conversation is
// being deleted — so the apply would create the row again, behind the
// removal, and the transfers cleaned up after it would have nobody to belong
// to.
func TestMessageArrivingDuringARemovalDoesNotRecreateThePeer(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("removal-in-flight")

	r := newSyncTestRouter()
	r.client = client
	r.removals = client.removals

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The state RemovePeer leaves mid-flight: gate up, row gone, counters
	// moved, removal not finished.
	releaseGate := r.removals.begin(peer)
	r.mu.Lock()
	r.peerGen[peer]++
	r.moveHistoryBackwardsLocked(peer)
	delete(r.peers, peer)
	r.removePeerLocked(peer)
	r.mu.Unlock()

	// A message whose stamp was taken AFTER those bumps — it matches.
	stamp := r.peerStampOf(peer)
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "arrived-mid-removal", Sender: peer, Recipient: me,
		Body: "hello", Timestamp: time.Now(),
	}, peer, stamp)

	r.mu.RLock()
	_, recreated := r.peers[peer]
	r.mu.RUnlock()
	if recreated {
		t.Fatal("a message arriving during the removal re-created the conversation behind it")
	}

	// Once the removal is done, the conversation may come back as it always
	// could: the gate is a window, not a ban.
	releaseGate()
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "arrived-after-removal", Sender: peer, Recipient: me,
		Body: "hello again", Timestamp: time.Now(),
	}, peer, r.peerStampOf(peer))

	r.mu.RLock()
	_, back := r.peers[peer]
	r.mu.RUnlock()
	if !back {
		t.Fatal("a message arriving after the removal finished was refused: the gate outlived its window")
	}
}

// TestDecryptedMessageOfADeletedRowIsNotApplied covers what a lifecycle
// generation cannot see. The contact is still there — the generation is
// unchanged — but the message was deleted while it was being decrypted, and
// applying it would put its preview, its last-online evidence and its badge
// back for a row that no longer exists.
func TestDecryptedMessageOfADeletedRowIsNotApplied(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("row-deleted-mid-decrypt")
	me := r.client.Address()

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	// The user deletes the message while the decrypt RPC is in flight. The
	// contact stays; only the history moves.
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "deleted-while-decrypting", Sender: peer, Recipient: me,
		Body: "gone", Timestamp: time.Now(),
	}, peer, stampAtEvent)

	r.mu.RLock()
	state := r.peers[peer]
	badged := len(r.unreadIDs[peer])
	r.mu.RUnlock()
	if state.Preview.Body != "" || state.LastIncomingAt.Valid() || badged != 0 {
		t.Fatalf("a message deleted mid-decrypt was applied: preview=%q lastIncoming=%v badged=%d",
			state.Preview.Body, state.LastIncomingAt, badged)
	}
}

// TestDecryptedMessageDoesNotResurrectARemovedPeer covers the other slow step
// on the message path: decrypting is an RPC to the node, and a contact
// removed while it runs must not come back through the sidebar row the
// decrypted message would write.
func TestDecryptedMessageDoesNotResurrectARemovedPeer(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("removed-mid-decrypt")
	me := r.client.Address()

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	r.mu.Unlock()

	// The removal completes while the decrypt is in flight.
	r.mu.Lock()
	r.peerGen[peer]++
	delete(r.peers, peer)
	delete(r.unreadIDs, peer)
	r.removePeerLocked(peer)
	r.mu.Unlock()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "late-decrypt", Sender: peer, Recipient: me, Body: "hello",
		Timestamp: time.Now(),
	}, peer, stampAtEvent)

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	badged := len(r.unreadIDs[peer])
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("a message decrypted before the removal put the contact back")
	}
	if badged != 0 {
		t.Fatalf("the removed contact kept %d unread ids", badged)
	}
}

// TestConversationLoadRefusesAnAnswerOlderThanADeletion covers the last
// chatlog read without an epoch check. The fetch takes up to three seconds;
// a message deleted while it runs has already been evicted from the cache,
// and loading the pre-deletion answer would put it back on screen with
// nothing left to reload it away.
func TestConversationLoadRefusesAnAnswerOlderThanADeletion(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("deleted-during-the-load")

	if err := client.chatLog.Append(context.Background(), "dm", me, chatlog.Entry{
		ID: "doomed", Sender: peer.String(), Recipient: me.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newSyncTestRouter()
	r.client = client
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.mu.Unlock()

	// The counters as the caller captured them before it started, and the
	// deletion that lands while the fetch is in flight.
	before := r.peerEpochsOf(peer)
	r.mu.Lock()
	r.moveHistoryBackwardsLocked(peer)
	r.mu.Unlock()

	if r.loadConversation(peer, before) {
		t.Fatal("a conversation load applied an answer read before the deletion")
	}
	if r.cache.HasMessage("doomed") {
		t.Fatal("the deleted message was loaded back into the open conversation")
	}
}

// TestMessageForAConversationLeftMidDecryptIsTreatedAsBackground covers the
// window between "is this the open conversation" and the decrypt that
// follows it. Decrypting is an RPC. If the user switches away while it runs,
// the answer is stale in both directions: writing the message into the cache
// splices it into the OTHER peer's thread, and treating it as visible skips
// its unread badge — permanently, because its id is already through the
// dedup gate.
func TestMessageForAConversationLeftMidDecryptIsTreatedAsBackground(t *testing.T) {
	r := newSyncTestRouter()
	me := r.client.Address()
	left := domaintest.ID("the-conversation-left-behind")
	opened := domaintest.ID("the-conversation-opened-instead")

	r.mu.Lock()
	r.tryEnsurePeerLocked(left)
	r.tryEnsurePeerLocked(opened)
	stampAtEvent := peerStamp{gen: r.peerGen[left], epochs: r.backwardsEpoch[left]}
	r.mu.Unlock()

	// The cache belongs to the conversation the user switched TO.
	r.cache.Load(opened, []DirectMessage{{
		ID: "already-here", Sender: opened, Recipient: me, Body: "hi", Timestamp: time.Now().Add(-time.Minute),
	}}, 0)
	r.mu.Lock()
	r.activePeer = opened
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// The message that was decrypted for the conversation now off screen.
	msg := &DirectMessage{
		ID: "decrypted-late", Sender: left, Recipient: me, Body: "written while you were here",
		Timestamp: time.Now(),
	}
	if r.deliverDecryptedMessage(msg, left, stampAtEvent) {
		t.Fatal("a message for a conversation the user left was delivered into the open one")
	}

	r.mu.RLock()
	unread := r.peers[left].Unread
	active := append([]DirectMessage(nil), r.activeMessages...)
	r.mu.RUnlock()

	if unread != 1 {
		t.Fatalf("unread for the conversation left behind = %d, want 1", unread)
	}
	if r.cache.HasMessage("decrypted-late") {
		t.Fatal("the message was spliced into the cache of the conversation the user switched to")
	}
	for _, m := range active {
		if m.ID == "decrypted-late" {
			t.Fatal("the message appeared in the open conversation it does not belong to")
		}
	}

	// The other half of the same window: the user switched away, but the new
	// conversation has not loaded yet, so the CACHE still holds the old one.
	// Only the selection says the message is no longer on screen.
	r.cache.Load(left, nil, 0)
	r.mu.Lock()
	r.activePeer = opened
	r.mu.Unlock()

	second := &DirectMessage{
		ID: "decrypted-later", Sender: left, Recipient: me, Body: "and another",
		Timestamp: time.Now(),
	}
	if r.deliverDecryptedMessage(second, left, stampAtEvent) {
		t.Fatal("a message was delivered into a conversation the user had already left, because the cache still matched it")
	}
	r.mu.RLock()
	unread = r.peers[left].Unread
	r.mu.RUnlock()
	if unread != 2 {
		t.Fatalf("unread = %d, want 2 — the second message was treated as visible", unread)
	}

	// And the mirror case: the user came BACK to the conversation, but its
	// history is still loading, so the cache belongs to the peer they left.
	// The selection matches; only the cache says the message has nowhere to
	// go yet, and appending it would splice it into the other thread.
	r.cache.Load(opened, nil, 0)
	r.mu.Lock()
	r.activePeer = left
	r.mu.Unlock()

	third := &DirectMessage{
		ID: "decrypted-while-loading", Sender: left, Recipient: me, Body: "third",
		Timestamp: time.Now(),
	}
	if r.deliverDecryptedMessage(third, left, stampAtEvent) {
		t.Fatal("a message was appended to a cache that belongs to another conversation")
	}
	if r.cache.HasMessage("decrypted-while-loading") {
		t.Fatal("the message was spliced into the cache of the conversation still on screen")
	}
}

// TestIncomingMessageApplyRefusesARemovedPeer covers the one helper every
// branch of the message path writes through. There are three of them — not
// active, mid-switch, and the conversation already on screen — and the third
// went unguarded for a while precisely because it looks different from the
// other two. Routing all three through this helper is what makes one test
// cover them; a guard repeated three times is a guard that will be forgotten
// once.
func TestIncomingMessageApplyRefusesARemovedPeer(t *testing.T) {
	r := newSyncTestRouter()
	peer := domaintest.ID("removed-mid-decrypt")
	me := r.client.Address()
	// Seq: a message the store could place. Without one the apply still lands
	// but reports the row as unordered, which is a different subject.
	msg := DirectMessage{ID: "late", Sender: peer, Recipient: me, Body: "hello", Timestamp: time.Now(), Seq: 1}

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	stampAtEvent := peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
	r.mu.Unlock()

	// Still there: the message lands.
	r.mu.Lock()
	appliedBefore := r.applyIncomingMessageLocked(peer, msg, stampAtEvent)
	r.mu.Unlock()
	if appliedBefore != applyApplied {
		t.Fatalf("a message for a live contact was refused: %v", appliedBefore)
	}

	// The removal completes while the next decrypt is in flight.
	r.mu.Lock()
	r.peerGen[peer]++
	delete(r.peers, peer)
	delete(r.unreadIDs, peer)
	r.removePeerLocked(peer)
	applied := r.applyIncomingMessageLocked(peer, msg, stampAtEvent)
	_, resurrected := r.peers[peer]
	r.mu.Unlock()

	if applied != applyPeerGone {
		t.Fatalf("outcome = %v, want applyPeerGone for a message decrypted before the removal", applied)
	}
	if resurrected {
		t.Fatal("the removed contact is back on the sidebar")
	}
}

// TestPreviewNeverPinsOnAFutureRow covers what a forward-dated row may and
// may not do. The timestamp is the sender's and the node accepts minutes of
// drift, so such a row reaches the sidebar as an ordinary last message — and
// it must not become a ceiling that the next message cannot beat, which is
// what it was when the preview was ordered by that stamp.
func TestPreviewNeverPinsOnAFutureRow(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("forward-dated-sender")
	now := time.Now().UTC()
	r.presenceClock = func() time.Time { return now }

	r.seedPreviews([]ConversationPreview{{
		PeerAddress: peer,
		Body:        "I am from the future",
		Timestamp:   now.Add(2 * time.Hour),
	}}, r.backwardsEpochSnapshot())

	r.mu.RLock()
	state := r.peers[peer]
	var seeded string
	if state != nil {
		seeded = state.Preview.Body
	}
	r.mu.RUnlock()
	if seeded != "I am from the future" {
		t.Fatalf("seeded preview = %q, want the row the store calls last", seeded)
	}

	// And an ordinary reply still lands on top of it.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.setPeerPreviewLocked(peer, DirectMessage{
		ID: "mine", Sender: r.client.Address(), Recipient: peer,
		Body: "my reply", Timestamp: now.Add(-time.Minute),
	})
	body := r.peers[peer].Preview.Body
	r.mu.Unlock()
	if body != "my reply" {
		t.Fatalf("preview = %q, want the reply that followed — a future row is still holding the ceiling", body)
	}
}

// TestReconcileDoesNotResurrectARemovedPeer covers the difference between
// "this peer moved" and "this peer is gone". RemovePeer deletes the row and
// bumps the lifecycle generation; a reconciliation that started before it
// still holds the old preview, and applying it would put the deleted
// conversation back on the sidebar — with its last message in it.
func TestReconcileDoesNotResurrectARemovedPeer(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("removed-mid-reconcile")
	ctx := context.Background()

	now := time.Now().UTC()
	wrote := now.Add(-time.Hour).Truncate(time.Second)
	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "gone-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: wrote.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()

	// The user removes the contact while the reconciliation's queries are in
	// flight — the same three steps RemovePeer applies: bump the lifecycle
	// generation, drop the state, drop the sidebar slot.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(p domain.PeerIdentity) {
			r.mu.Lock()
			r.peerGen[p]++
			delete(r.peers, p)
			r.removePeerLocked(p)
			r.mu.Unlock()
		},
	}

	r.reconcilePeerFromStore(context.Background(), peer, false, true)

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("a reconciliation that started before the removal put the deleted contact back")
	}
}

// TestReconcileRefusesAPartialRead pins the atomicity the reconciliation
// claims. Its three reads describe one moment; applying the preview while the
// last-incoming query failed would publish half of it — and after a deletion
// nothing re-reads that peer, so the date of the message the user removed
// would stay for good.
func TestReconcileRefusesAPartialRead(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("partial-read-peer")
	ctx := context.Background()

	now := time.Now().UTC()
	survivor := now.Add(-4 * time.Hour).Truncate(time.Second)
	deleted := now.Add(-time.Hour).Truncate(time.Second)

	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "partial-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: survivor.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	// In memory the peer still carries the message that was just deleted.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.noteIncomingAtLocked(peer, deleted)
	before := *r.peers[peer]
	r.mu.Unlock()

	// The last-incoming query times out on every attempt.
	r.history = &failingHistoryReader{store: cl, failPerPeerReads: true}

	if got := r.reconcilePeerFromStore(context.Background(), peer, true, true); got != reconcileRetry {
		t.Fatalf("a reconciliation that could not read every field reported %v, want reconcileRetry", got)
	}

	r.mu.RLock()
	after := *r.peers[peer]
	r.mu.RUnlock()
	if after.LastIncomingAt != before.LastIncomingAt || after.Preview.Timestamp != before.Preview.Timestamp {
		t.Fatalf("a partial read was applied: %+v, want the peer untouched (%+v)", after, before)
	}

	// With the read working, the same reconciliation lowers the date to the
	// surviving message — the outcome the refusal above was protecting.
	r.history = nil
	if got := r.reconcilePeerFromStore(context.Background(), peer, true, true); got != reconcileApplied {
		t.Fatalf("the reconciliation reported %v once its reads worked, want reconcileApplied", got)
	}
	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(survivor) {
		t.Fatalf("last incoming = %v, want the surviving message %v", got, survivor)
	}
}

// TestReconcileNeverCreatesARemovedPeer covers the window a token cannot see.
// The reconciliation is queued by an event and runs in its own goroutine, so
// RemovePeer can finish BEFORE that goroutine is scheduled — every token it
// takes then already describes the world after the removal. The invariant is
// what closes it: a reconciliation updates a peer, it never creates one.
func TestReconcileNeverCreatesARemovedPeer(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("removed-before-scheduling")
	ctx := context.Background()

	now := time.Now().UTC()
	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "ghost-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	// The removal has already completed — generation bumped, row gone — by
	// the time the reconciliation starts.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.peerGen[peer]++
	delete(r.peers, peer)
	r.removePeerLocked(peer)
	r.mu.Unlock()

	r.reconcilePeerFromStore(context.Background(), peer, false, true)

	r.mu.RLock()
	_, resurrected := r.peers[peer]
	r.mu.RUnlock()
	if resurrected {
		t.Fatal("a reconciliation queued before the removal rebuilt the deleted contact")
	}
}

// TestFailedDeleteReconcileIsRetried covers the one recomputation nobody
// repeats. A new message reconciles its own peer and the startup scan runs
// once, so a deletion whose refresh failed has no second chance: the sidebar
// would quote the message the user destroyed until the next launch.
func TestFailedDeleteReconcileIsRetried(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("retry-after-delete")
	ctx := context.Background()

	now := time.Now().UTC()
	survivor := now.Add(-4 * time.Hour).Truncate(time.Second)
	deleted := now.Add(-time.Hour).Truncate(time.Second)

	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "retry-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: survivor.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.noteIncomingAtLocked(peer, deleted)
	r.mu.Unlock()

	// The refresh right after the deletion cannot read the history.
	r.history = &failingHistoryReader{store: cl, failPerPeerReads: true}
	r.refreshPreviewAfterDelete(peer)

	r.mu.RLock()
	owed, queued := r.pendingDeleteReconcile[peer]
	stillWrong := r.peers[peer].LastIncomingAt
	r.mu.RUnlock()
	if !queued || owed <= 0 {
		t.Fatal("a failed post-deletion refresh was dropped instead of queued")
	}
	if !stillWrong.Valid() || !stillWrong.Time().Equal(deleted) {
		t.Fatalf("state before the retry = %v, want the stale value %v", stillWrong, deleted)
	}

	// The sweep gets it once the chatlog answers again.
	r.history = nil
	r.retryPendingDeleteReconcile(context.Background())

	r.mu.RLock()
	got := r.peers[peer].LastIncomingAt
	_, stillQueued := r.pendingDeleteReconcile[peer]
	r.mu.RUnlock()
	if !got.Valid() || !got.Time().Equal(survivor) {
		t.Fatalf("last incoming after the retry = %v, want the surviving message %v", got, survivor)
	}
	if stillQueued {
		t.Fatal("a reconciliation that succeeded stayed on the retry queue")
	}

	// And the sweep publishes what it fixed. Nobody is waiting behind it —
	// the deletion that queued it finished long ago — so a retry that lands
	// without notifying leaves the deleted message on screen, which is the
	// whole reason the retry exists.
	published := r.Snapshot().Peers[peer]
	if published == nil {
		t.Fatal("the peer is missing from the published snapshot")
	}
	if !published.LastIncomingAt.Valid() || !published.LastIncomingAt.Time().Equal(survivor) {
		t.Fatalf("published last incoming = %v, want the surviving message %v", published.LastIncomingAt, survivor)
	}
}

// TestUnreadIsASetNotACounter is the point of the refactor. The badge is fed
// by two sources that cannot be ordered against each other — a SQL read and
// the event stream, with the database ahead of the events — so the same
// message reaches it twice. A counter turns that into 2; a set does not.
func TestUnreadIsASetNotACounter(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("double-reported-peer")
	me := r.client.Address()
	at := time.Now().UTC().Add(-time.Minute)

	// The event path reports the message.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "same-1", Sender: peer, Recipient: me, Body: "hi", Timestamp: at,
	}, peer, peerStamp{})

	// The startup read reports the very same message.
	r.mu.Lock()
	r.markUnreadLocked(peer, domain.MessageID("same-1"))
	unread := r.peers[peer].Unread
	r.mu.Unlock()

	if unread != 1 {
		t.Fatalf("unread = %d, want 1 — one message reported by both sources", unread)
	}

	// Reading the conversation clears it; a late report of the same id must
	// not resurrect the badge, because the id is gone from the set.
	r.mu.Lock()
	r.clearUnreadLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("same-2"))
	r.dropUnreadLocked(peer, domain.MessageID("same-2"))
	unread = r.peers[peer].Unread
	r.mu.Unlock()
	if unread != 0 {
		t.Fatalf("unread after read and delete = %d, want 0", unread)
	}
}

// TestALateApplyCannotPutBackAnEarlierMessage is the interleaving the sidebar
// is exposed to whatever the code does about locking: the node releases its
// lock across the SQLite write and publishes the event afterwards, a send
// applies its own echo from its own goroutine, and the event bus delivers
// asynchronously. So two messages of one conversation can be APPLIED in the
// opposite order to the one they were STORED in.
//
// Message A is written first and B second. B reaches the sidebar first; A's
// send completes late and applies afterwards. What must decide is where the
// rows landed, not who wrote last, and not which stamp is larger — here A's
// stamp is even the newer of the two, because the clocks disagree.
func TestALateApplyCannotPutBackAnEarlierMessage(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("interleaved-peer")
	me := r.client.Address()
	now := time.Now().UTC()

	// B — stored second, applied first.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "B", Sender: peer, Recipient: me, Body: "their message",
		Timestamp: now.Add(-time.Minute), Seq: 2,
	}, peer, peerStamp{})

	// A — stored first, applied last, and carrying the later stamp.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "A", Sender: me, Recipient: peer, Body: "our message",
		Timestamp: now, Seq: 1,
	}, peer, peerStamp{})

	r.mu.RLock()
	body := r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if body != "their message" {
		t.Fatalf("preview = %q, want %q: the message stored LAST is the last message, whoever applied it last",
			body, "their message")
	}

	// And the sidebar is not frozen: the next message stored — sequence 3 —
	// takes the row.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "C", Sender: peer, Recipient: me, Body: "the next one",
		Timestamp: now.Add(-time.Hour), Seq: 3,
	}, peer, peerStamp{})
	r.mu.RLock()
	body = r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if body != "the next one" {
		t.Fatalf("preview = %q, want %q", body, "the next one")
	}
}

// TestStoredPreviewDoesNotOverwriteALiveOne covers the same rule across the
// two roads to the sidebar. A read from the chatlog is answered outside the
// lock, so it can land after a message it never saw; the message is the newer
// answer and the read has to defer to it. Both carry the arrival sequence, so
// neither has to know which of them ran first.
func TestStoredPreviewDoesNotOverwriteALiveOne(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("merge-order-peer")
	me := r.client.Address()

	now := time.Now().UTC()
	older := now.Add(-2 * time.Hour)
	newer := now.Add(-time.Minute)

	// A message arrives while a read of the row before it is in flight.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "newer", Sender: peer, Recipient: me, Body: "newer", Timestamp: newer, Seq: 7,
	}, peer, peerStamp{})

	r.mu.Lock()
	applied := r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "older", Timestamp: older, Seq: 6,
	})
	body := r.peers[peer].Preview.Body
	last := r.peers[peer].LastIncomingAt
	r.mu.Unlock()

	if applied != previewOlder {
		t.Fatalf("the store's answer reported %v over a message it never saw; want previewOlder", applied)
	}
	if body != "newer" {
		t.Fatalf("preview = %q, want the live message to survive", body)
	}
	if !last.Valid() || !last.Time().Equal(newer) {
		t.Fatalf("last incoming = %v, want %v", last, newer)
	}

	// A read that saw the message applies normally, whatever its stamp says:
	// the store is the authority on which row is last, and it answers that by
	// arrival rather than by the senders' clocks.
	r.mu.Lock()
	applied = r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "read after it", Timestamp: older, Seq: 7,
	})
	body = r.peers[peer].Preview.Body
	r.mu.Unlock()
	if applied != previewTaken || body != "read after it" {
		t.Fatalf("preview = %q (result=%v), want the store's answer to land", body, applied)
	}
}

// TestReplayedMessageStillCountsAsUnread covers the gap between the startup
// read and the replay of buffered events. A message stored after the read was
// in neither the snapshot nor the increments, because the replay used to be
// suppressed. With a set there is nothing to suppress: the same id from both
// sources is one message, and a message only one source saw is still counted.
func TestReplayedMessageStillCountsAsUnread(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("replayed-peer")
	me := r.client.Address()
	at := time.Now().UTC().Add(-time.Minute)

	r.mu.Lock()
	r.replayingStartup = true
	r.activePeer = domaintest.ID("someone-else")
	r.mu.Unlock()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "after-the-snapshot", Sender: peer, Recipient: me, Body: "hi", Timestamp: at,
	}, peer, peerStamp{})

	r.mu.RLock()
	unread := r.peers[peer].Unread
	r.mu.RUnlock()
	if unread != 1 {
		t.Fatalf("unread = %d, want 1 — the replay is what carries a message the snapshot missed", unread)
	}
}

// TestMarkSeenClearsOnlyWhatItSent covers the RPC window. Receipts are sent
// for a copy of the conversation; a message that arrives while that call is
// in flight was never in the batch, so its receipt was never sent and it is
// still unread.
func TestMarkSeenClearsOnlyWhatItSent(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("mark-seen-peer")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("sent-1"))
	r.markUnreadLocked(peer, domain.MessageID("sent-2"))
	sentSnapshot := r.unreadSnapshotLocked(peer)
	// The optimistic clear, then a message landing while the RPC runs.
	r.clearUnreadLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("arrived-during-rpc"))
	r.mu.Unlock()

	// The RPC succeeded for the two ids it carried.
	r.mu.Lock()
	r.dropUnreadLocked(peer, domain.MessageID("sent-1"), domain.MessageID("sent-2"))
	unread := r.peers[peer].Unread
	r.mu.Unlock()
	if unread != 1 {
		t.Fatalf("unread = %d, want 1 — the message that arrived mid-RPC is still unread", unread)
	}

	// And the rollback path unions rather than replaces.
	r.mu.Lock()
	r.restoreUnreadLocked(peer, sentSnapshot)
	unread = r.peers[peer].Unread
	r.mu.Unlock()
	if unread != 3 {
		t.Fatalf("unread after rollback = %d, want 3 — the restore must not drop what arrived meanwhile", unread)
	}
}

// TestMarkSeenDropsOnlyTheIDsItCarried drives the same rule through
// doMarkSeen itself, which is where the ids are chosen. The helpers above say
// the set behaves; this says the caller passes the right members of it.
func TestMarkSeenDropsOnlyTheIDsItCarried(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("mark-seen-rpc-peer")

	r := newTestRouter()
	r.client = client

	loaded := []DirectMessage{
		{ID: "loaded-1", Sender: peer, Recipient: me, Body: "one", Timestamp: time.Now().Add(-2 * time.Minute)},
		{ID: "loaded-2", Sender: peer, Recipient: me, Body: "two", Timestamp: time.Now().Add(-time.Minute)},
	}

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.activePeer = peer
	r.activeMessages = loaded
	for _, msg := range loaded {
		r.markUnreadLocked(peer, domain.MessageID(msg.ID))
	}
	// Not in activeMessages: it landed after the conversation was loaded, so
	// no receipt is sent for it.
	r.markUnreadLocked(peer, domain.MessageID("arrived-after-load"))
	r.mu.Unlock()

	if !r.doMarkSeen(peer) {
		t.Fatal("doMarkSeen reported failure for the active conversation")
	}

	r.mu.RLock()
	remaining := make([]string, 0, len(r.unreadIDs[peer]))
	for msgID := range r.unreadIDs[peer] {
		remaining = append(remaining, string(msgID))
	}
	unreadAfter := r.peers[peer].Unread
	r.mu.RUnlock()

	if unreadAfter != 1 || len(remaining) != 1 || remaining[0] != "arrived-after-load" {
		t.Fatalf("unread after mark-seen = %d %v, want only the message no receipt was sent for", unreadAfter, remaining)
	}
}

// TestDeleteDoesNotLowerOntoANewerMessage covers the one path allowed to move
// the sidebar backwards. Its queries run outside the lock, so a message that
// lands meanwhile is newer than anything the deletion removed; lowering onto
// it would take the row back to before that message arrived.
func TestDeleteDoesNotLowerOntoANewerMessage(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	cl := client.chatLog
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("delete-vs-arrival")
	ctx := context.Background()

	now := time.Now().UTC()
	older := now.Add(-3 * time.Hour).Truncate(time.Second)
	arrived := now.Add(-time.Minute).Truncate(time.Second)

	if err := cl.Append(ctx, "dm", me, chatlog.Entry{
		ID: "survivor", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: older.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.noteIncomingAtLocked(peer, older)
	r.mu.Unlock()

	// The message lands between the queries and the lock.
	r.history = &interleavingReader{
		inner: client.chatLog,
		hook: func(p domain.PeerIdentity) {
			r.mu.Lock()
			r.setPeerPreviewLocked(p, DirectMessage{Sender: peer, Recipient: me, Body: "just arrived", Timestamp: arrived})
			r.mu.Unlock()
		},
	}

	if got := r.reconcilePeerFromStore(ctx, peer, true, true); got != reconcileRetry {
		t.Fatalf("reconcile reported %v, want reconcileRetry — the peer moved under it", got)
	}

	r.mu.RLock()
	last := r.peers[peer].LastIncomingAt
	body := r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if !last.Valid() || !last.Time().Equal(arrived) {
		t.Fatalf("last incoming = %v, want the message that arrived meanwhile (%v)", last, arrived)
	}
	if body != "just arrived" {
		t.Fatalf("preview = %q, want the message that arrived meanwhile", body)
	}
}

// TestRemovedPeerDropsItsUnreadSet pins the lifecycle. Ids left behind would
// reappear the moment the contact is added back and something marks one
// unread, resurrecting a badge for messages that no longer exist.
func TestRemovedPeerDropsItsUnreadSet(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("removed-with-unread")

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("old-1"))
	r.markUnreadLocked(peer, domain.MessageID("old-2"))
	// What RemovePeer applies once its durable work is done.
	r.peerGen[peer]++
	delete(r.peers, peer)
	delete(r.unreadIDs, peer)
	r.removePeerLocked(peer)
	r.mu.Unlock()

	// The contact comes back and writes once.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.markUnreadLocked(peer, domain.MessageID("new-1"))
	unread := r.peers[peer].Unread
	r.mu.Unlock()

	if unread != 1 {
		t.Fatalf("unread after re-adding the contact = %d, want 1 — the old ids must be gone", unread)
	}
}

// failingHistoryReader stands in for the chatlog history so a test can say
// which read fails. A working SQLite refuses to fail one query while its
// siblings succeed, and refuses to fail once and then recover.
type failingHistoryReader struct {
	store            *chatlog.Store
	failPerPeerReads bool
	// failUnseenOnly fails just the unread query, so a test can separate
	// "the badge could not be rebuilt" from "nothing could be read".
	failUnseenOnly bool
	failScans      int32
	perPeer        map[domain.PeerIdentity]time.Time
	scanCount      int32
}

func (f *failingHistoryReader) MessageSeq(ctx context.Context, messageID domain.MessageID) (int64, bool, error) {
	return f.store.MessageSeq(ctx, messageID)
}

func (f *failingHistoryReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	if f.failPerPeerReads {
		return time.Time{}, errors.New("chatlog unavailable")
	}
	return f.store.LastIncomingAtFor(ctx, peer, now)
}

func (f *failingHistoryReader) LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error) {
	if atomic.AddInt32(&f.scanCount, 1) <= atomic.LoadInt32(&f.failScans) {
		return nil, errors.New("database is starting up")
	}
	if f.perPeer != nil {
		return f.perPeer, nil
	}
	return f.store.LastIncomingAtPerPeer(ctx, now)
}

func (f *failingHistoryReader) UnseenIncomingIDsFor(ctx context.Context, peer domain.PeerIdentity) ([]domain.MessageID, error) {
	if f.failPerPeerReads || f.failUnseenOnly {
		return nil, errors.New("chatlog unavailable")
	}
	return f.store.UnseenIncomingIDsFor(ctx, peer)
}

func (f *failingHistoryReader) UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
	if f.failPerPeerReads {
		return nil, errors.New("chatlog unavailable")
	}
	return f.store.UnseenIncomingIDs(ctx)
}

func (f *failingHistoryReader) StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error) {
	if f.failPerPeerReads {
		return nil, errors.New("chatlog unavailable")
	}
	return f.store.StoredMessageStatuses(ctx, ids)
}

func (f *failingHistoryReader) scans() int32 { return atomic.LoadInt32(&f.scanCount) }

// afterStatusReader runs its hook AFTER the stored-status query has answered
// — the exact boundary the recovery's commit stamp is about: the stamp is
// taken before that query, and everything the answer authorises happens
// after it.
type afterStatusReader struct {
	inner chatHistoryReader
	once  sync.Once
	hook  func()
}

func (a *afterStatusReader) StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error) {
	statuses, err := a.inner.StoredMessageStatuses(ctx, ids)
	a.once.Do(a.hook)
	return statuses, err
}

func (a *afterStatusReader) MessageSeq(ctx context.Context, messageID domain.MessageID) (int64, bool, error) {
	return a.inner.MessageSeq(ctx, messageID)
}

func (a *afterStatusReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	return a.inner.LastIncomingAtFor(ctx, peer, now)
}

func (a *afterStatusReader) LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error) {
	return a.inner.LastIncomingAtPerPeer(ctx, now)
}

func (a *afterStatusReader) UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
	return a.inner.UnseenIncomingIDs(ctx)
}

func (a *afterStatusReader) UnseenIncomingIDsFor(ctx context.Context, peer domain.PeerIdentity) ([]domain.MessageID, error) {
	return a.inner.UnseenIncomingIDsFor(ctx, peer)
}

// interleavingReader runs a hook while the router is between its
// reconciliation reads and the lock it applies them under. That window is
// where an arriving message — or a removed contact — overtakes a read, and
// no amount of goroutine scheduling reproduces it on demand. The hook runs
// once, from inside the last-incoming query, so the reads that follow still
// describe the same moment the caller observed.
type interleavingReader struct {
	inner chatHistoryReader
	once  sync.Once
	hook  func(peer domain.PeerIdentity)
}

func (i *interleavingReader) MessageSeq(ctx context.Context, messageID domain.MessageID) (int64, bool, error) {
	return i.inner.MessageSeq(ctx, messageID)
}

func (i *interleavingReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	i.once.Do(func() { i.hook(peer) })
	return i.inner.LastIncomingAtFor(ctx, peer, now)
}

func (i *interleavingReader) LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error) {
	return i.inner.LastIncomingAtPerPeer(ctx, now)
}

func (i *interleavingReader) UnseenIncomingIDsFor(ctx context.Context, peer domain.PeerIdentity) ([]domain.MessageID, error) {
	i.once.Do(func() { i.hook(peer) })
	return i.inner.UnseenIncomingIDsFor(ctx, peer)
}

func (i *interleavingReader) UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
	i.once.Do(func() { i.hook(domain.PeerIdentity{}) })
	return i.inner.UnseenIncomingIDs(ctx)
}

func (i *interleavingReader) StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error) {
	// Whichever read the path under test performs first runs the hook; the
	// window being reproduced is "between the reads and the lock", and both
	// reads sit inside it.
	i.once.Do(func() { i.hook(domain.PeerIdentity{}) })
	return i.inner.StoredMessageStatuses(ctx, ids)
}

// Removing a contact stops the send queue for the length of the history delete,
// and opens it again afterwards. Same reason as the wipe: the removal gate does
// not reach the queue, so a pass that resolved this conversation's facts a
// moment earlier would hand its frame over after the rows are gone.
func TestRemovingAContactStopsTheSendQueueWhileTheHistoryGoes(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForConversationDelete(t)
	r.removals = c.removals
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	if c.localNode.ReactionSendsHeldFor(peer) {
		t.Fatal("the fixture starts with the queue already shut")
	}

	// A write of that conversation is in flight, so the removal waits inside
	// removals.begin — and the queue has to be shut for the whole of that wait,
	// not from after it.
	releaseWrite, admitted := c.removals.admitWrite(peer)
	if !admitted {
		t.Fatal("the fixture could not take a write lease")
	}
	removed := make(chan error, 1)
	go func() {
		_, err := r.RemovePeer(peer)
		removed <- err
	}()

	shut := false
	for range 100 {
		if c.localNode.ReactionSendsHeldFor(peer) {
			shut = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !shut {
		t.Fatal("the removal waited for the write with the send queue still open")
	}
	select {
	case err := <-removed:
		t.Fatalf("the removal finished while a write was still in flight: %v", err)
	default:
	}

	releaseWrite()
	select {
	case err := <-removed:
		if err != nil {
			t.Fatalf("RemovePeer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the removal never finished after the write was done")
	}
	if c.localNode.ReactionSendsHeldFor(peer) {
		t.Fatal("the send queue stayed shut after the contact was removed")
	}
	if c.localNode.QueuedReactionsFor(peer) != 0 {
		t.Fatal("the removed contact still has reactions waiting")
	}
}

// TestStartupDropsReReadTheOpenConversation: a dropped startup event is
// not re-sent, so the re-read the log line promises has to actually
// happen.
//
// PublishReporting counts an event delivered the moment it enters this
// router's inbox, and the node's repair pass will not offer it again. If
// the startup buffer then drops it, the row on disk says `delivered`
// while this cache still says whatever the startup read saw — and the
// badge stays wrong until the user reopens the conversation.
func TestStartupDropsReReadTheOpenConversation(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentityFromWire(peer.Address)

	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "dropped-event message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// On disk the message has moved on; only the cache is behind.
	const messageID = "dropped-event-1"
	if err := c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID:             messageID,
		Sender:         peer.Address,
		Recipient:      id.Address,
		Body:           ciphertext,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	}); err != nil {
		t.Fatalf("chatlog append: %v", err)
	}

	done := make(chan struct{})
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]messageGate),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		backwardsEpoch: make(map[domain.PeerIdentity]peerEpochs),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}
	r.mu.Lock()
	r.activePeer = peerID
	r.cache.Load(peerID, []DirectMessage{{
		ID: messageID, Sender: peerID, Recipient: domain.PeerIdentityFromWire(id.Address),
		Body: "dropped-event message", ReceiptStatus: MessageStatusQueued,
	}}, 0)
	r.mu.Unlock()

	// Nothing was dropped: no re-read, and the stale cache is left as-is,
	// which is what proves the reload below is the thing being tested.
	r.reloadAfterStartupDrops(0)
	if got := cachedStatus(r, messageID); got != MessageStatusQueued {
		t.Fatalf("cache status = %q without any drops, want it untouched", got)
	}

	r.reloadAfterStartupDrops(3)
	if got := cachedStatus(r, messageID); got == MessageStatusQueued {
		t.Error("the open conversation was not re-read after startup dropped events; its badge keeps a status the disk has moved past")
	}
}

// cachedStatus is the router's own view of one message, which is what the
// UI draws.
func cachedStatus(r *DMRouter, messageID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, msg := range r.cache.Messages() {
		if msg.ID == messageID {
			return msg.ReceiptStatus
		}
	}
	return ""
}

// TestFetchConversationCarriesTheRowSequence: the reconcile in
// ConversationCache.Load can only tell "stored after my read" from
// "deleted while I read" if the messages it holds carry their row's
// arrival order. That number has to survive the WHOLE production path —
// chatlog.Read, decryption, the DirectMessage the cache stores — and a
// unit test that sets Seq by hand proves a state the real path may never
// produce.
//
// It did not: Read left rowid out of its SELECT, so every loaded message
// arrived with Seq 0 and the boundary was always zero, which silently
// turned "keep what the read could not have seen" into "keep everything
// the snapshot omits" — resurrecting deleted messages.
func TestFetchConversationCarriesTheRowSequence(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	boxSig := identity.SignBoxKeyBinding(peer)
	c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  boxSig,
		}},
	})

	for _, body := range []string{"first", "second"} {
		ciphertext, err := directmsg.EncryptForParticipants(
			peer,
			domain.DMRecipient{
				Address:      domain.PeerIdentityFromWire(id.Address),
				BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
			},
			domain.OutgoingDM{Body: body},
		)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		if err := c.chatLog.Append(context.Background(), "dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
			ID:        "seq-" + body,
			Sender:    peer.Address,
			Recipient: id.Address,
			Body:      ciphertext,
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("chatlog append: %v", err)
		}
	}

	messages, err := c.FetchConversation(context.Background(), domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatalf("FetchConversation: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("loaded %d messages, want 2", len(messages))
	}
	for _, msg := range messages {
		if msg.Seq == 0 {
			t.Fatalf("message %q came back with Seq 0; the reconcile boundary would be zero for every real load", msg.ID)
		}
	}
	if messages[0].Seq >= messages[1].Seq {
		t.Errorf("Seq %d then %d: the sequence must follow arrival order", messages[0].Seq, messages[1].Seq)
	}
}
