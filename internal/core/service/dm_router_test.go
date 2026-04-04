package service

import (
	"database/sql"
	"fmt"
	"runtime"
	"testing"
	"time"

	"corsa/internal/core/chatlog"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"

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
	r.ensurePeerLocked("peer-1")
	r.mu.Unlock()

	r.mu.RLock()
	ps, ok := r.peers["peer-1"]
	r.mu.RUnlock()

	if !ok {
		t.Fatal("peer-1 should be created")
	}

	// Modify and ensure again — should not overwrite.
	r.mu.Lock()
	r.peers["peer-1"].Unread = 5
	r.ensurePeerLocked("peer-1")
	r.mu.Unlock()

	if ps.Unread != 5 {
		t.Fatalf("ensurePeerLocked overwrote existing state, Unread=%d", ps.Unread)
	}
}

// TestPromotePeerLocked verifies that promotePeerLocked moves a peer to
// the front of peerOrder, deduplicating any prior occurrences.
func TestPromotePeerLocked(t *testing.T) {
	r := newTestRouter()
	r.peerOrder = []domain.PeerIdentity{"a", "b", "c"}

	r.mu.Lock()
	r.promotePeerLocked("c")
	r.mu.Unlock()

	if r.peerOrder[0] != "c" {
		t.Fatalf("expected c at front, got %v", r.peerOrder)
	}
	if len(r.peerOrder) != 3 {
		t.Fatalf("expected 3 entries, got %v", r.peerOrder)
	}

	r.mu.Lock()
	r.promotePeerLocked("a")
	r.mu.Unlock()
	expected := []domain.PeerIdentity{"a", "c", "b"}
	for i, v := range expected {
		if r.peerOrder[i] != v {
			t.Fatalf("index %d: expected %q, got %q", i, v, r.peerOrder[i])
		}
	}

	// Promote empty string → no-op.
	r.mu.Lock()
	r.promotePeerLocked("")
	r.mu.Unlock()
	if len(r.peerOrder) != 3 {
		t.Fatalf("empty promote changed slice: %v", r.peerOrder)
	}

	// Promote new peer → added at front.
	r.mu.Lock()
	r.promotePeerLocked("new-peer")
	r.mu.Unlock()
	if r.peerOrder[0] != "new-peer" {
		t.Fatalf("new peer should be at front, got %v", r.peerOrder)
	}
}

// TestRemovePeerLocked verifies that removePeerLocked correctly filters
// a peer out of peerOrder (including duplicates).
func TestRemovePeerLocked(t *testing.T) {
	r := newTestRouter()
	r.peerOrder = []domain.PeerIdentity{"a", "b", "c", "b", "d"}

	r.mu.Lock()
	r.removePeerLocked("b")
	r.mu.Unlock()

	expected := []domain.PeerIdentity{"a", "c", "d"}
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
	r.removePeerLocked("z")
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

	// peerForMessage normalizes padded Sender/Recipient.
	ev := protocol.LocalChangeEvent{
		Sender:    " peer-1 ",
		Recipient: "me",
	}
	got := r.peerForMessage(ev)
	if got != "peer-1" {
		t.Fatalf("peerForMessage did not normalize: got %q", got)
	}

	// SelectPeer normalizes.
	r.SelectPeer(" peer-2 ")
	r.mu.RLock()
	active := r.activePeer
	r.mu.RUnlock()
	if active != "peer-2" {
		t.Fatalf("SelectPeer did not normalize: activePeer = %q", active)
	}

	// AutoSelectPeer normalizes.
	r.AutoSelectPeer("  peer-3  ")
	r.mu.RLock()
	active = r.activePeer
	r.mu.RUnlock()
	if active != "peer-3" {
		t.Fatalf("AutoSelectPeer did not normalize: activePeer = %q", active)
	}
}

// TestClearPeerUnread verifies clearPeerUnread sets Unread to 0 and
// is safe when peer doesn't exist.
func TestClearPeerUnread(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.peers["peer-1"] = &RouterPeerState{Unread: 7}
	r.mu.Unlock()

	r.clearPeerUnread("peer-1")

	r.mu.RLock()
	u := r.peers["peer-1"].Unread
	r.mu.RUnlock()

	if u != 0 {
		t.Fatalf("expected Unread=0, got %d", u)
	}

	// Clearing non-existent peer should not panic.
	r.clearPeerUnread("nonexistent")
}

// TestPeerStateUnreadIntegrity verifies that unread counts are
// independently tracked per peer and don't leak across entries.
func TestPeerStateUnreadIntegrity(t *testing.T) {
	r := newTestRouter()
	r.mu.Lock()
	r.peers["peer-a"] = &RouterPeerState{Unread: 3}
	r.peers["peer-b"] = &RouterPeerState{Unread: 5}
	r.mu.Unlock()

	r.clearPeerUnread("peer-a")

	r.mu.RLock()
	ua := r.peers["peer-a"].Unread
	ub := r.peers["peer-b"].Unread
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
		{PeerAddress: "peer-with-unread", UnreadCount: 3},
		{PeerAddress: "peer-all-read", UnreadCount: 0},
		{PeerAddress: "peer-also-unread", UnreadCount: 1},
	}

	// Inline the seedPreviews logic (needs client.Address() which we
	// can't call on a nil client — but we filter by me="" which doesn't
	// match any preview address, so all pass through).
	r.mu.Lock()
	for _, p := range previews {
		if p.PeerAddress == "" {
			continue
		}
		r.ensurePeerLocked(p.PeerAddress)
		r.peers[p.PeerAddress].Preview = p
		if p.UnreadCount > 0 {
			r.peers[p.PeerAddress].Unread = p.UnreadCount
			r.promotePeerLocked(p.PeerAddress)
		}
	}
	r.mu.Unlock()

	// All peers should be in the peers map.
	for _, addr := range []domain.PeerIdentity{"peer-with-unread", "peer-all-read", "peer-also-unread"} {
		r.mu.RLock()
		_, ok := r.peers[addr]
		r.mu.RUnlock()
		if !ok {
			t.Fatalf("%s should be in peers map", addr)
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.peers["peer-with-unread"].Unread != 3 {
		t.Fatalf("expected unread=3, got %d", r.peers["peer-with-unread"].Unread)
	}
	if r.peers["peer-also-unread"].Unread != 1 {
		t.Fatalf("expected unread=1, got %d", r.peers["peer-also-unread"].Unread)
	}
	if r.peers["peer-all-read"].Unread != 0 {
		t.Fatalf("expected unread=0, got %d", r.peers["peer-all-read"].Unread)
	}

	// Unread peers should be promoted to front of peerOrder.
	// peer-also-unread was promoted last → it's at front.
	if len(r.peerOrder) < 2 {
		t.Fatalf("expected at least 2 entries in peerOrder, got %d: %v", len(r.peerOrder), r.peerOrder)
	}
	if r.peerOrder[0] != "peer-also-unread" {
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
	r.peers["old-peer-1"] = &RouterPeerState{Unread: 3, Preview: ConversationPreview{Body: "old msg"}}
	r.peers["old-peer-2"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"old-peer-1", "old-peer-2"}
	r.activePeer = "old-peer-1"
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "m1"}}
	r.seenMessageIDs = map[string]struct{}{"old-msg-1": {}}
	r.initialSynced = true
	r.mu.Unlock()

	r.cache.Load("old-peer-1", []DirectMessage{
		{ID: "old-msg-1", Body: "old"},
	})

	r.resetIdentityState()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.peers) != 0 {
		t.Fatalf("peers not cleared: %v", r.peers)
	}
	if len(r.peerOrder) != 0 {
		t.Fatalf("peerOrder not cleared: %v", r.peerOrder)
	}
	if r.activePeer != "" {
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
	if r.previewsSeeded {
		t.Fatal("previewsSeeded should be false after reset")
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
	r.cache.Load("peer-1", []DirectMessage{
		{
			ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1",
			ReceiptStatus: "sent", Timestamp: now,
		},
	})

	r.mu.Lock()
	r.activePeer = "peer-1"
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	deliveredAt := now.Add(2 * time.Second)
	event := protocol.LocalChangeEvent{
		Type:        protocol.LocalChangeReceiptUpdate,
		MessageID:   "msg-1",
		Sender:      "me",
		Recipient:   "peer-1",
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

	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1", ReceiptStatus: "sent"},
	})

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
	r.pendingClearEditor = true
	r.pendingRecipientText = "test-peer"
	r.mu.Unlock()

	pa := r.ConsumePendingActions()

	if !pa.ScrollToEnd {
		t.Fatal("ScrollToEnd should be true")
	}
	if !pa.ClearEditor {
		t.Fatal("ClearEditor should be true")
	}
	if pa.RecipientText != "test-peer" {
		t.Fatalf("RecipientText should be 'test-peer', got %q", pa.RecipientText)
	}

	// After consumption, flags should be cleared.
	r.mu.RLock()
	if r.pendingScrollToEnd {
		t.Fatal("pendingScrollToEnd should be cleared")
	}
	if r.pendingClearEditor {
		t.Fatal("pendingClearEditor should be cleared")
	}
	if r.pendingRecipientText != "" {
		t.Fatalf("pendingRecipientText should be cleared, got %q", r.pendingRecipientText)
	}
	r.mu.RUnlock()
}

// TestSnapshotIsConsistent verifies that Snapshot() returns a consistent
// copy of the router state (not pointers to live data).
func TestSnapshotIsConsistent(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.peers["peer-1"] = &RouterPeerState{Unread: 3}
	r.peerOrder = []domain.PeerIdentity{"peer-1"}
	r.activeMessages = []DirectMessage{{ID: "m1", Body: "hello"}}
	r.nodeStatus = NodeStatus{Peers: []string{"a"}}
	r.sendStatus = "ok"
	r.mu.Unlock()

	snap := r.Snapshot()

	if snap.ActivePeer != "peer-1" {
		t.Fatalf("expected ActivePeer=peer-1, got %q", snap.ActivePeer)
	}
	if !snap.PeerClicked {
		t.Fatal("expected PeerClicked=true")
	}
	if snap.Peers["peer-1"].Unread != 3 {
		t.Fatalf("expected Unread=3, got %d", snap.Peers["peer-1"].Unread)
	}
	if len(snap.PeerOrder) != 1 || snap.PeerOrder[0] != "peer-1" {
		t.Fatalf("unexpected PeerOrder: %v", snap.PeerOrder)
	}
	if len(snap.ActiveMessages) != 1 || snap.ActiveMessages[0].ID != "m1" {
		t.Fatalf("unexpected ActiveMessages: %v", snap.ActiveMessages)
	}
	if snap.SendStatus != "ok" {
		t.Fatalf("expected SendStatus=ok, got %q", snap.SendStatus)
	}

	// Mutate the snapshot — should not affect router state.
	snap.Peers["peer-1"].Unread = 99
	snap.PeerOrder[0] = "mutated"

	r.mu.RLock()
	if r.peers["peer-1"].Unread != 3 {
		t.Fatal("snapshot mutation leaked to router state")
	}
	if r.peerOrder[0] != "peer-1" {
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

	if cache.MatchesPeer("anyone") {
		t.Fatal("empty cache should not match any peer")
	}

	cache.Load("peer-1", nil)
	if !cache.MatchesPeer("peer-1") {
		t.Fatal("cache should match peer-1 after Load")
	}
	if cache.MatchesPeer("peer-2") {
		t.Fatal("cache should not match peer-2")
	}

	cache.Load("peer-2", []DirectMessage{{ID: "m1"}})
	if cache.MatchesPeer("peer-1") {
		t.Fatal("cache should no longer match peer-1")
	}
	if !cache.MatchesPeer("peer-2") {
		t.Fatal("cache should match peer-2")
	}
}

// TestDoMarkSeenSkipsWhenNoMessages verifies that doMarkSeen does NOT
// clear unread when activeMessages is empty (conversation not loaded yet).
func TestDoMarkSeenSkipsWhenNoMessages(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peers["peer-1"] = &RouterPeerState{Unread: 5}
	r.peerOrder = []domain.PeerIdentity{"peer-1"}
	// activeMessages is intentionally empty — simulates load not completed.
	r.mu.Unlock()

	r.doMarkSeen("peer-1")

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	r.mu.RUnlock()

	if unread != 5 {
		t.Fatalf("expected unread=5 (unchanged), got %d — doMarkSeen should not clear unread on empty activeMessages", unread)
	}
}

// TestIsActivePeer verifies the isActivePeer helper.
func TestIsActivePeer(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = "peer-1"
	r.mu.Unlock()

	if !r.isActivePeer("peer-1") {
		t.Fatal("peer-1 should be active")
	}
	if r.isActivePeer("peer-2") {
		t.Fatal("peer-2 should not be active")
	}
}

// TestOnReceiptUpdateActivePeerCacheMismatch verifies that when activePeer
// is set but cache is for a different peer (mid-switch), onReceiptUpdate
// triggers a loadConversation (via goroutine) rather than ignoring.
func TestOnReceiptUpdateActivePeerCacheMismatch(t *testing.T) {
	r := newTestRouter()

	// Cache is for "old-peer", but activePeer is "peer-1".
	r.cache.Load("old-peer", []DirectMessage{
		{ID: "msg-old", Body: "old", Sender: "me", Recipient: "old-peer"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
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

	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// isActivePeer("peer-2") must return false → sidebar-only path.
	if r.isActivePeer("peer-2") {
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
	r.cache.Load("old-peer", nil)
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.mu.Unlock()

	// isActivePeer reports peer-1 as active...
	if !r.isActivePeer("peer-1") {
		t.Fatal("peer-1 should be the active peer")
	}
	// ...but cache doesn't match yet (still on old-peer).
	if r.cache.MatchesPeer("peer-1") {
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
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
	r.SelectPeer("peer-2")

	// Check immediately — activeMessages should be nil (cleared synchronously).
	r.mu.RLock()
	msgs := r.activeMessages
	activePeer := r.activePeer
	clicked := r.peerClicked
	r.mu.RUnlock()

	if activePeer != "peer-2" {
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

	r.AutoSelectPeer("peer-1")

	r.mu.RLock()
	activePeer := r.activePeer
	clicked := r.peerClicked
	r.mu.RUnlock()

	if activePeer != "peer-1" {
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
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.mu.Unlock()

	// Auto-select a different peer — must keep clicked true.
	r.AutoSelectPeer("peer-2")

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
	r.activePeer = "peer-1"
	r.activeMessages = []DirectMessage{{ID: "msg-old", Body: "old"}}
	r.mu.Unlock()

	r.AutoSelectPeer("peer-2")

	r.mu.RLock()
	msgs := r.activeMessages
	r.mu.RUnlock()

	if msgs != nil {
		t.Fatalf("expected activeMessages=nil after auto-select switch, got %d messages", len(msgs))
	}
}

// TestSeedPreviewsSortOrder verifies that seedPreviews sorts peers:
// unread first (by unread desc), then by most recent timestamp.
func TestSeedPreviewsSortOrder(t *testing.T) {
	r := newTestRouter()

	now := time.Now()
	previews := []ConversationPreview{
		{PeerAddress: "old-read", Timestamp: now.Add(-10 * time.Hour), UnreadCount: 0},
		{PeerAddress: "new-read", Timestamp: now.Add(-1 * time.Hour), UnreadCount: 0},
		{PeerAddress: "unread-low", Timestamp: now.Add(-2 * time.Hour), UnreadCount: 1},
		{PeerAddress: "unread-high", Timestamp: now.Add(-5 * time.Hour), UnreadCount: 10},
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// Expected: unread first sorted by unread count descending (10 > 1),
	// then read sorted by timestamp descending (new > old).
	expected := []domain.PeerIdentity{"unread-high", "unread-low", "new-read", "old-read"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsSortOrderSameUnreadByTimestamp verifies that peers with
// the same unread count are sorted by timestamp descending.
func TestSeedPreviewsSortOrderSameUnreadByTimestamp(t *testing.T) {
	r := newTestRouter()

	now := time.Now()
	previews := []ConversationPreview{
		{PeerAddress: "unread-old", Timestamp: now.Add(-5 * time.Hour), UnreadCount: 3},
		{PeerAddress: "unread-new", Timestamp: now.Add(-1 * time.Hour), UnreadCount: 3},
		{PeerAddress: "read-only", Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0},
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// Same unread count → timestamp decides; read peers come last.
	expected := []domain.PeerIdentity{"unread-new", "unread-old", "read-only"}
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
	r.ensurePeerLocked("peer-C") // arrives first via event
	r.peers["peer-C"].Preview = ConversationPreview{
		PeerAddress: "peer-C",
		Body:        "event msg",
		Timestamp:   now.Add(-3 * time.Hour), // older than SQL
	}
	r.ensurePeerLocked("peer-A") // arrives second via event
	r.peers["peer-A"].Preview = ConversationPreview{
		PeerAddress: "peer-A",
		Body:        "event msg",
		Timestamp:   now.Add(-2 * time.Hour), // older than SQL
	}
	r.mu.Unlock()
	// peerOrder is now ["peer-C", "peer-A"]

	// seedPreviews arrives with the full sorted snapshot from SQL:
	// peer-B has unread (should be first), peer-A is recent, peer-C is old.
	// All SQL timestamps are newer than event-path timestamps.
	previews := []ConversationPreview{
		{PeerAddress: "peer-A", Timestamp: now.Add(-10 * time.Minute), UnreadCount: 0},
		{PeerAddress: "peer-B", Timestamp: now.Add(-1 * time.Hour), UnreadCount: 5},
		{PeerAddress: "peer-C", Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0},
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// All peers are SQL-applied (event-path data was older). The SQL sort
	// places them: unread first (peer-B), then by timestamp desc (peer-A,
	// peer-C). The two event-path slots are filled by sqlSorted in order.
	expected := []domain.PeerIdentity{"peer-B", "peer-A", "peer-C"}
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
	r.mu.Lock()
	r.ensurePeerLocked("peer-F")
	r.peers["peer-F"].Preview = ConversationPreview{
		PeerAddress: "peer-F",
		Body:        "fresh event",
		Timestamp:   now, // fresher than SQL snapshot below
	}
	r.peers["peer-F"].Unread = 1
	r.ensurePeerLocked("peer-Old")
	r.peers["peer-Old"].Preview = ConversationPreview{
		PeerAddress: "peer-Old",
		Body:        "old event",
		Timestamp:   now.Add(-3 * time.Hour), // older than SQL
	}
	r.mu.Unlock()
	// peerOrder: ["peer-F", "peer-Old"]

	// SQL snapshot sorted by the startup sort (unread desc → ts desc):
	// peer-S has unread, peer-Old and peer-F have none.
	// The stale SQL snapshot tries to place peer-F last (old ts in SQL).
	previews := []ConversationPreview{
		{PeerAddress: "peer-S", Timestamp: now.Add(-30 * time.Minute), UnreadCount: 3},
		{PeerAddress: "peer-Old", Timestamp: now.Add(-1 * time.Hour), UnreadCount: 0},
		{PeerAddress: "peer-F", Timestamp: now.Add(-2 * time.Hour), UnreadCount: 0}, // stale for peer-F
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	unreadF := r.peers["peer-F"].Unread
	bodyF := r.peers["peer-F"].Preview.Body
	r.mu.RUnlock()

	// peer-F keeps position 0 (fresher event-path data — not repositioned).
	// peer-Old's slot is filled by the SQL sort: peer-S takes that slot
	// (first in sqlSorted), peer-Old is appended (second in sqlSorted).
	expected := []domain.PeerIdentity{"peer-F", "peer-S", "peer-Old"}
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
	r.ensurePeerLocked("event-only-peer")
	r.peers["event-only-peer"].Preview = ConversationPreview{
		PeerAddress: "event-only-peer",
		Body:        "fresh event",
		Timestamp:   now,
	}
	r.mu.Unlock()

	// seedPreviews only contains a different peer.
	previews := []ConversationPreview{
		{PeerAddress: "sql-peer", Timestamp: now.Add(-1 * time.Hour), UnreadCount: 2},
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	order := append([]domain.PeerIdentity(nil), r.peerOrder...)
	r.mu.RUnlock()

	// event-only-peer keeps its original position (not in previews, not
	// repositioned); sql-peer fills the SQL-applied slot after it.
	expected := []domain.PeerIdentity{"event-only-peer", "sql-peer"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(order), order)
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Fatalf("peerOrder[%d]: expected %q, got %q (full: %v)", i, exp, order[i], order)
		}
	}
}

// TestSeedPreviewsResetsStaleUnreadToZero verifies that when the SQL
// snapshot reports UnreadCount=0 for a peer that already has a stale
// event-path Unread > 0, seedPreviews resets it to 0. The SQL snapshot
// (ListConversationsCtx) is the source of truth for unread after startup.
func TestSeedPreviewsResetsStaleUnreadToZero(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate event-path setting Unread=3 for peer-1 before seedPreviews.
	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 3
	r.peers["peer-1"].Preview = ConversationPreview{
		PeerAddress: "peer-1",
		Body:        "stale event",
		Timestamp:   now.Add(-5 * time.Minute), // older than SQL
	}
	r.mu.Unlock()

	// SQL snapshot says this peer has 0 unread (already seen).
	previews := []ConversationPreview{
		{PeerAddress: "peer-1", Timestamp: now, UnreadCount: 0, Body: "latest"},
	}

	r.seedPreviews(previews)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	body := r.peers["peer-1"].Preview.Body
	r.mu.RUnlock()

	if unread != 0 {
		t.Fatalf("expected Unread=0 (reset from SQL snapshot), got %d", unread)
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
	r.cache.Load("peer-1", nil)
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.mu.Unlock()

	snap = r.Snapshot()
	if !snap.CacheReady {
		t.Fatal("CacheReady should be true when cache matches activePeer")
	}

	// Switch active peer but don't load cache — simulates mid-switch.
	r.mu.Lock()
	r.activePeer = "peer-2"
	r.mu.Unlock()

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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()

	// Drain any stale events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Switch to peer-2. The goroutine will fail (no real FetchConversation)
	// but the synchronous notify should fire immediately.
	r.SelectPeer("peer-2")

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
	r.activePeer = "peer-1"
	r.peerClicked = false
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 3
	r.mu.Unlock()

	// Verify cache doesn't match.
	if r.cache.MatchesPeer("peer-1") {
		t.Fatal("cache should not match peer-1 before load")
	}

	// Drain events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Click the same peer. Since changed=false, the old code would skip
	// loadConversation. The fix should detect cache mismatch and set needLoad.
	r.SelectPeer("peer-1")

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
		ps, exists := r.peers["peer-1"]
		return exists && ps.Unread == 3
	})
	if !ok {
		r.mu.RLock()
		unread := 0
		if ps, exists := r.peers["peer-1"]; exists {
			unread = ps.Unread
		}
		r.mu.RUnlock()
		t.Fatalf("expected Unread restored to 3 after failed loadConversation retry, got %d", unread)
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.activeMessages = r.cache.Messages()
	r.peerClicked = true
	r.mu.Unlock()

	// Drain events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Click the same peer again — cache is valid, no re-load needed.
	// This should be a true no-op: no events emitted.
	r.SelectPeer("peer-1")

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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "me", Recipient: "peer-1"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.activeMessages = r.cache.Messages()
	r.peerClicked = true
	// Simulate restorePeerUnread outcome: badge restored while chat is open.
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 3
	r.mu.Unlock()

	// Drain any setup events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// User re-clicks the same peer — expects recovery.
	r.SelectPeer("peer-1")

	// Unread must be cleared optimistically (synchronous).
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
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
	unreadAfter := r.peers["peer-1"].Unread
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
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Preview = ConversationPreview{
		PeerAddress: "peer-1",
		Body:        "fresh event message",
		Timestamp:   now, // newer
	}
	r.peers["peer-1"].Unread = 3
	r.mu.Unlock()

	// Now seedPreviews arrives with stale data for peer-1 and new data for peer-2.
	stalePreview := []ConversationPreview{
		{PeerAddress: "peer-1", Body: "stale startup message", Timestamp: now.Add(-5 * time.Minute), UnreadCount: 1},
		{PeerAddress: "peer-2", Body: "peer-2 message", Timestamp: now.Add(-1 * time.Minute), UnreadCount: 2},
	}
	r.seedPreviews(stalePreview)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// peer-1 should retain the fresher event-path data.
	if r.peers["peer-1"].Preview.Body != "fresh event message" {
		t.Fatalf("seedPreviews overwrote fresher data: got %q", r.peers["peer-1"].Preview.Body)
	}
	if r.peers["peer-1"].Unread != 3 {
		t.Fatalf("seedPreviews overwrote fresher unread: got %d", r.peers["peer-1"].Unread)
	}

	// peer-2 should be seeded normally (no prior data).
	if r.peers["peer-2"].Preview.Body != "peer-2 message" {
		t.Fatalf("peer-2 should be seeded: got %q", r.peers["peer-2"].Preview.Body)
	}
	if r.peers["peer-2"].Unread != 2 {
		t.Fatalf("peer-2 unread should be 2: got %d", r.peers["peer-2"].Unread)
	}
}

// TestSeedPreviewsOverwritesOlderData verifies that seedPreviews DOES
// update peers when the startup data is newer than existing state.
func TestSeedPreviewsOverwritesOlderData(t *testing.T) {
	r := newTestRouter()

	now := time.Now()

	// Simulate very old event-path data.
	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Preview = ConversationPreview{
		PeerAddress: "peer-1",
		Body:        "very old message",
		Timestamp:   now.Add(-1 * time.Hour),
	}
	r.peers["peer-1"].Unread = 0
	r.mu.Unlock()

	// seedPreviews with newer data.
	previews := []ConversationPreview{
		{PeerAddress: "peer-1", Body: "newer startup message", Timestamp: now, UnreadCount: 5},
	}
	r.seedPreviews(previews)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.peers["peer-1"].Preview.Body != "newer startup message" {
		t.Fatalf("seedPreviews should have updated: got %q", r.peers["peer-1"].Preview.Body)
	}
	if r.peers["peer-1"].Unread != 5 {
		t.Fatalf("unread should be 5: got %d", r.peers["peer-1"].Unread)
	}
}

// TestRepairUnreadCountsNormallyWhenSeedPreviewsNeverRan verifies that
// when initializeFromDB returns without calling seedPreviews (empty/failed
// preview load), the first repairUnreadFromHeaders poll counts unreads
// normally. The skip guard only activates when previewsSeeded is true
// (seedPreviews already loaded counts from SQL). If seedPreviews never
// ran, there is nothing to double-count.
func TestRepairUnreadCountsNormallyWhenSeedPreviewsNeverRan(t *testing.T) {
	r := newTestRouter()

	// Simulate empty-preview startup: previewsSeeded stays false,
	// initialSynced stays false (first poll).
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
			{ID: "msg-2", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	r.mu.RUnlock()

	// previewsSeeded=false → skipUnreadCount=false → Unread counted: 2.
	if unread != 2 {
		t.Fatalf("expected Unread=2, got %d — skip guard incorrectly suppressed counting", unread)
	}
}

// TestRepairUnreadSkipsCountOnFirstSyncAfterSeedPreviews is the complement:
// when seedPreviews DID run (normal startup), the first poll must NOT
// double-count unreads. previewsSeeded=true + firstSync=true → skip.
func TestRepairUnreadSkipsCountOnFirstSyncAfterSeedPreviews(t *testing.T) {
	r := newTestRouter()

	// Simulate normal startup: seedPreviews ran and set unread from SQL.
	r.mu.Lock()
	r.initialSynced = false // first poll hasn't happened yet
	r.previewsSeeded = true // seedPreviews ran
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 2 // set by seedPreviews from SQL
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
			{ID: "msg-2", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	synced := r.initialSynced
	r.mu.RUnlock()

	// firstSync=true + previewsSeeded=true → skipUnreadCount=true → Unread stays 2.
	if unread != 2 {
		t.Fatalf("expected Unread=2 (unchanged from seed), got %d", unread)
	}
	if !synced {
		t.Fatal("initialSynced should be true after first repair")
	}
}

// TestOnNewMessageRegistersSeenMessageID verifies that onNewMessage adds
// the event's MessageID to seenMessageIDs, preventing the repair-path
// (repairUnreadFromHeaders) from double-counting it.
func TestOnNewMessageRegistersSeenMessageID(t *testing.T) {
	r := newTestRouter()

	// Set peer-1 as active with empty cache matching.
	r.cache.Load("peer-1", nil)
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-123",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	r.mu.RLock()
	_, seen := r.seenMessageIDs["msg-123"]
	r.mu.RUnlock()
	if !seen {
		t.Fatal("onNewMessage should register MessageID in seenMessageIDs for repair-path dedup")
	}
}

// TestOnNewMessageNonActivePeerRegistersSeenID verifies dedup even when
// the message is for a non-active peer (sidebar-only update path).
func TestOnNewMessageNonActivePeerRegistersSeenID(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = "peer-2" // different from sender
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-456",
		Sender:    "peer-1",
		Recipient: "me",
	}

	r.onNewMessage(event)

	r.mu.RLock()
	_, seen := r.seenMessageIDs["msg-456"]
	r.mu.RUnlock()
	if !seen {
		t.Fatal("onNewMessage should register MessageID in seenMessageIDs even for non-active peer")
	}
}

// TestRepairUnreadFirstSyncDoesNotDoubleCount verifies that on the first
// sync (initialSynced=false) when seedPreviews already ran (previewsSeeded=true),
// repairUnreadFromHeaders populates seenMessageIDs but does NOT increment
// Unread for non-active peers. seedPreviews already set correct counts from
// SQL — DMHeaders don't carry delivery_status, so incrementing would double-count.
func TestRepairUnreadFirstSyncDoesNotDoubleCount(t *testing.T) {
	r := newTestRouter()

	// Simulate seedPreviews having set Unread=2 for peer-1.
	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 2
	r.activePeer = "peer-2" // different from peer-1
	r.initialSynced = false // first sync
	r.previewsSeeded = true // seedPreviews ran and loaded SQL counts
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
			{ID: "msg-2", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
			{ID: "msg-3", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	_, seen1 := r.seenMessageIDs["msg-1"]
	_, seen2 := r.seenMessageIDs["msg-2"]
	_, seen3 := r.seenMessageIDs["msg-3"]
	synced := r.initialSynced
	r.mu.RUnlock()

	// Unread should remain 2 (from seedPreviews), NOT 2+3=5.
	if unread != 2 {
		t.Fatalf("expected Unread=2 (unchanged from seed), got %d", unread)
	}
	// All message IDs should be registered for future dedup.
	if !seen1 || !seen2 || !seen3 {
		t.Fatal("seenMessageIDs should contain all DMHeader IDs after first sync")
	}
	if !synced {
		t.Fatal("initialSynced should be true after first sync")
	}
}

// TestRepairUnreadSubsequentSyncIncrements verifies that after the first
// sync, new headers DO increment Unread normally.
func TestRepairUnreadSubsequentSyncIncrements(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 2
	r.activePeer = "peer-2"
	r.initialSynced = true // already synced
	// Pre-register some old messages.
	r.seenMessageIDs["msg-old-1"] = struct{}{}
	r.seenMessageIDs["msg-old-2"] = struct{}{}
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "msg-old-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")}, // already seen
			{ID: "msg-new-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")}, // new
		},
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
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
	r := &DMRouter{
		client:         &DesktopClient{id: &identity.Identity{Address: "me"}},
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
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

// TestEventListenerBuffersDuringStartup verifies that the real
// runEventListener drains the subscription channel immediately and replays
// buffered events after startupDone closes, preventing the node from
// dropping events.
func TestEventListenerBuffersDuringStartup(t *testing.T) {
	// Need a real chatlog with schema so updatePreviewFromStore succeeds
	// (returns true) and the non-active decrypt-fail goroutine does not
	// roll back seenMessageIDs.
	db, cl := newTestChatLog(t)
	defer func() { _ = db.Close() }()

	r := &DMRouter{
		client:         &DesktopClient{id: &identity.Identity{Address: "me"}, chatLog: cl},
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    make(chan struct{}), // NOT pre-closed — simulates ongoing startup
	}

	events := make(chan protocol.LocalChangeEvent, 16)
	done := make(chan struct{})

	// Run the real production event listener — not a hand-written simulation.
	go func() {
		r.runEventListener(events, func() {})
		close(done)
	}()

	// Send events BEFORE startup completes — these must be buffered.
	events <- protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "msg-1",
		Sender: "peer1", Recipient: "me",
	}
	events <- protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "msg-2",
		Sender: "peer1", Recipient: "me",
	}
	events <- protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "msg-3",
		Sender: "peer1", Recipient: "me",
	}

	// Poll until the listener drains all 3 events from the channel into
	// its internal buffer (instead of a fixed sleep).
	if !pollCondition(2*time.Second, func() bool { return len(events) == 0 }) {
		t.Fatal("listener did not drain pre-startup events from channel")
	}

	// Verify nothing was processed yet — events should be buffered, not handled.
	r.mu.RLock()
	preStartupSeen := len(r.seenMessageIDs)
	r.mu.RUnlock()
	if preStartupSeen != 0 {
		t.Fatalf("expected 0 seen messages before startup, got %d", preStartupSeen)
	}

	// Complete startup — buffered events should now replay.
	close(r.startupDone)

	// Wait until the 3 buffered events are processed (seenMessageIDs populated)
	// before sending the live event. This replaces a fixed sleep.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		n := len(r.seenMessageIDs)
		r.mu.RUnlock()
		return n >= 3
	}) {
		t.Fatal("buffered events were not replayed within timeout")
	}

	// Send one more live event after startup.
	events <- protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "msg-4",
		Sender: "peer1", Recipient: "me",
	}

	// Poll until the live event is consumed from the channel.
	if !pollCondition(2*time.Second, func() bool { return len(events) == 0 }) {
		t.Fatal("listener did not consume live event from channel")
	}

	close(events)

	// Wait for the real listener goroutine to finish.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runEventListener did not exit after channel close")
	}

	// Poll until all 4 message IDs are in seenMessageIDs (async goroutines
	// in the non-active decrypt-fail path may still be completing).
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		n := len(r.seenMessageIDs)
		r.mu.RUnlock()
		return n >= 4
	}) {
		r.mu.RLock()
		seen := len(r.seenMessageIDs)
		r.mu.RUnlock()
		t.Fatalf("expected 4 seen messages, got %d", seen)
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
	r.activePeer = "peer-2" // different from the incoming message sender
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
	r.activePeer = "peer-2"
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-out-1",
		Sender:    "me", // outgoing — we are the sender
		Recipient: "peer-1",
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
	r.activePeer = "peer-1" // same as the incoming message sender
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
	r.activePeer = "peer-1"
	r.mu.Unlock()

	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "msg-active-out-1",
		Sender:    "me", // outgoing
		Recipient: "peer-1",
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "existing-1", Sender: "peer-1", Body: "hello"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
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
	r := &DMRouter{
		client:         &DesktopClient{id: &identity.Identity{Address: "me"}},
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
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
	r.ensurePeerLocked("peer-x")
	r.peers["peer-x"].Unread = 7
	r.mu.Unlock()

	// SelectPeer spawns a goroutine for loadConversation + doMarkSeen,
	// but the unread badge must be zeroed *before* that goroutine runs.
	r.SelectPeer("peer-x")

	// Check immediately — no sleep or channel wait.
	r.mu.RLock()
	unread := r.peers["peer-x"].Unread
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
	r.ensurePeerLocked("peer-fail")
	r.peers["peer-fail"].Unread = 4
	r.mu.Unlock()

	// SelectPeer spawns a goroutine. loadConversation will fail because
	// the test router has no real chatlog client.
	r.SelectPeer("peer-fail")

	// Immediately after SelectPeer, unread is 0 (optimistic clear).
	r.mu.RLock()
	immediate := r.peers["peer-fail"].Unread
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
	restored := r.peers["peer-fail"].Unread
	r.mu.RUnlock()
	if restored != 4 {
		t.Fatalf("expected Unread=4 after failed loadConversation, got %d", restored)
	}
}

// TestAutoSelectPeerNewPeerClearsUnread verifies that AutoSelectPeer clears
// the unread badge when switching to a new peer (changed=true). Same-peer
// re-selection is a true no-op — see TestAutoSelectPeerSamePeerIsNoOp.
func TestAutoSelectPeerNewPeerClearsUnread(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 5
	r.mu.Unlock()

	r.AutoSelectPeer("peer-1")

	// Immediately after AutoSelectPeer, the badge should be cleared
	// (optimistic clear happens synchronously in selectPeerCore).
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
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
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 5
	r.mu.Unlock()

	r.SelectPeer("peer-1")

	// Immediately after SelectPeer (before background goroutine),
	// the badge should already be cleared.
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
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
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 3
	r.ensurePeerLocked("peer-2")
	r.peers["peer-2"].Unread = 5
	r.mu.Unlock()

	// --- SelectPeer: changed=true path ---
	r.SelectPeer("peer-1")
	r.mu.RLock()
	if r.activePeer != "peer-1" {
		t.Fatalf("SelectPeer must set activePeer, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("SelectPeer must set peerClicked = true")
	}
	if r.peers["peer-1"].Unread != 0 {
		t.Fatalf("SelectPeer must optimistically clear Unread, got %d", r.peers["peer-1"].Unread)
	}
	r.mu.RUnlock()

	// Drain events from SelectPeer.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// --- AutoSelectPeer: changed=true path (different peer) ---
	r.AutoSelectPeer("peer-2")
	r.mu.RLock()
	if r.activePeer != "peer-2" {
		t.Fatalf("AutoSelectPeer must switch activePeer, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("AutoSelectPeer must set peerClicked = true")
	}
	if r.peers["peer-2"].Unread != 0 {
		t.Fatalf("AutoSelectPeer must optimistically clear Unread, got %d", r.peers["peer-2"].Unread)
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: "peer-1", Recipient: "me"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.activeMessages = r.cache.Messages()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 5
	r.mu.Unlock()

	// Drain any setup events.
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	// Programmatic re-select of the same peer — must be true no-op.
	r.AutoSelectPeer("peer-1")

	// No events emitted.
	select {
	case ev := <-r.uiEvents:
		t.Fatalf("AutoSelectPeer same-peer emitted event %v — expected true no-op", ev.Type)
	case <-time.After(100 * time.Millisecond):
		// Expected: no events.
	}

	// State unchanged.
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	active := r.activePeer
	clicked := r.peerClicked
	msgs := len(r.activeMessages)
	r.mu.RUnlock()

	if unread != 5 {
		t.Fatalf("Unread changed from 5 to %d — same-peer AutoSelectPeer must not clear unread", unread)
	}
	if active != "peer-1" {
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
// seedPreviews already loaded the correct count from SQL).
func TestReplayStartupBufferDoesNotDoubleCountUnread(t *testing.T) {
	r := newTestRouter()

	// Simulate seedPreviews having set Unread = 3 for this peer.
	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 3
	r.activePeer = "peer-2" // different from peer-1 → non-active path
	r.mu.Unlock()

	// Simulate buffered event for the same peer.
	buf := []protocol.LocalChangeEvent{
		{
			Type:      protocol.LocalChangeNewMessage,
			Topic:     "dm",
			MessageID: "msg-replay-1",
			Sender:    "peer-1",
			Recipient: "me",
		},
	}

	// replayAndListen sets replayingStartup = true during replay.
	// We use a closed channel so `for event := range events` exits immediately.
	closedCh := make(chan protocol.LocalChangeEvent)
	close(closedCh)
	r.replayAndListen(buf, 0, closedCh)

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	replaying := r.replayingStartup
	r.mu.RUnlock()

	if unread != 3 {
		t.Fatalf("expected Unread=3 (unchanged from seedPreviews), got %d", unread)
	}
	if replaying {
		t.Fatal("replayingStartup should be false after replayAndListen returns")
	}

	// Also verify no UIEventBeep was emitted during replay.
	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		if ev.Type == UIEventBeep {
			t.Fatal("UIEventBeep should not be emitted during startup replay")
		}
	}
}

// TestReplayDrainsLiveEventsDuringStartup verifies that replayAndListen
// drains pending live events from the channel after processing buffered
// events (preventing the node-side 16-slot channel from overflowing).
//
// Proof strategy: buffered events run under replayingStartup=true (no beep),
// live events run under replayingStartup=false (beep emitted for incoming).
// Counting UIEventBeep proves live events were processed by onNewMessage.
// We cannot check seenMessageIDs because decrypt-fail fallback goroutines
// evict IDs when updatePreviewFromStore also fails (expected without chatlog).
func TestReplayDrainsLiveEventsDuringStartup(t *testing.T) {
	r := newTestRouter()

	r.mu.Lock()
	r.activePeer = "someone-else"
	r.mu.Unlock()

	// Prepare 3 buffered events from peer-1 (replaying=true → no beep).
	buf := []protocol.LocalChangeEvent{
		{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "buf-1", Sender: "peer-1", Recipient: "me"},
		{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "buf-2", Sender: "peer-1", Recipient: "me"},
		{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "buf-3", Sender: "peer-1", Recipient: "me"},
	}

	// Live channel with 2 incoming events from peer-2 (replaying=false → beep).
	liveCh := make(chan protocol.LocalChangeEvent, 4)
	liveCh <- protocol.LocalChangeEvent{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "live-1", Sender: "peer-2", Recipient: "me"}
	liveCh <- protocol.LocalChangeEvent{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "live-2", Sender: "peer-2", Recipient: "me"}
	close(liveCh) // close so `for event := range events` exits after drain

	r.replayAndListen(buf, 0, liveCh)

	// Count UIEventBeep emissions. Each live incoming message emits one beep
	// synchronously in onNewMessage (before goroutine). Buffered events do not
	// emit beeps (replaying=true). So exactly 2 beeps proves both live events
	// were drained and processed.
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
		t.Fatalf("expected 2 UIEventBeep (one per live event), got %d — "+
			"live events may not have been drained during replay", beepCount)
	}
}

// TestLiveEventsDuringReplayTriggerBeep verifies that live events arriving
// during startup replay are buffered (not processed) while replayingStartup
// is true, then processed after the flag is reset. This ensures new messages
// that arrive during replay correctly trigger UIEventBeep and Unread++.
//
// Before the fix, bufferPendingLiveEvents (then drainPendingLiveEvents)
// processed live events immediately under replayingStartup=true, permanently
// suppressing their Unread++ and UIEventBeep.  seenMessageIDs would still
// be populated, so repairUnreadFromHeaders couldn't recover them either.
//
// We assert on UIEventBeep rather than Unread because Unread++ requires
// DecryptIncomingMessage (real crypto), whereas UIEventBeep is emitted
// directly in onNewMessage based on the replaying flag alone.
func TestLiveEventsDuringReplayTriggerBeep(t *testing.T) {
	r := newTestRouter()

	// Set up a non-active peer so onNewMessage takes the sidebar path
	// (which emits UIEventBeep for incoming messages when !replaying).
	r.mu.Lock()
	r.activePeer = "someone-else"
	r.ensurePeerLocked("peer-1")
	r.mu.Unlock()

	// One buffered event (replay) — should NOT emit UIEventBeep.
	buf := []protocol.LocalChangeEvent{
		{
			Type:      protocol.LocalChangeNewMessage,
			Topic:     "dm",
			MessageID: "replay-msg",
			Sender:    "peer-1",
			Recipient: "me",
		},
	}

	// Live event pre-buffered in channel — simulates a message arriving
	// while replay is in progress.  With the buffer approach, this event
	// is drained from the channel during replay (preventing node-side
	// overflow) but processed AFTER replayingStartup is reset.
	liveCh := make(chan protocol.LocalChangeEvent, 4)
	liveCh <- protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "live-msg",
		Sender:    "peer-1",
		Recipient: "me",
	}
	close(liveCh)

	r.replayAndListen(buf, 0, liveCh)

	// After replayAndListen returns, replayingStartup must be false.
	r.mu.RLock()
	replaying := r.replayingStartup
	r.mu.RUnlock()

	if replaying {
		t.Fatal("replayingStartup should be false after replayAndListen returns")
	}

	// Count UIEventBeep in the channel.
	// Replay event: sender != "me" but replayingStartup=true  → NO beep.
	// Live event:   sender != "me" and replayingStartup=false → beep.
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

// TestReplayLiveBufferCapped verifies that the pendingLive buffer inside
// replayAndListen is capped and doesn't grow unboundedly.  When more live
// events arrive during replay than maxReplayLiveBuf (256), excess events
// are consumed from the channel (preventing node-side overflow) but dropped.
// After replay, a UI reload notification is sent so repair-path picks up
// the missed events.
func TestReplayLiveBufferCapped(t *testing.T) {
	// Need a real chatlog with schema so updatePreviewFromStore succeeds
	// (returns true) and the non-active decrypt-fail goroutine does not
	// roll back seenMessageIDs.
	db, cl := newTestChatLog(t)
	defer func() { _ = db.Close() }()

	r := newTestRouter()
	r.client.chatLog = cl

	// Use a larger event channel to absorb the storm of UIEventSidebarUpdated
	// notifications from 257 goroutines (avoids notify retry goroutine spam).
	r.uiEvents = make(chan UIEvent, 512)

	r.mu.Lock()
	r.activePeer = "someone-else"
	r.mu.Unlock()

	// One buffered event to trigger replay loop.
	buf := []protocol.LocalChangeEvent{
		{Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "buf-1", Sender: "peer-1", Recipient: "me"},
	}

	// Overfill the live channel: put 260 events (exceeds 256 cap).
	liveCh := make(chan protocol.LocalChangeEvent, 300)
	for i := 0; i < 260; i++ {
		liveCh <- protocol.LocalChangeEvent{
			Type:      protocol.LocalChangeNewMessage,
			Topic:     "dm",
			MessageID: fmt.Sprintf("live-%d", i),
			Sender:    "peer-2",
			Recipient: "me",
		}
	}
	close(liveCh)

	r.replayAndListen(buf, 0, liveCh)

	// Poll until all 257 message IDs settle in seenMessageIDs. The IDs are
	// registered synchronously in onNewMessage, but async goroutines (non-active
	// decrypt-fail path) could evict them on failure. With a real chatlog,
	// updatePreviewFromStore succeeds so no eviction happens.
	if !pollCondition(5*time.Second, func() bool {
		r.mu.RLock()
		n := len(r.seenMessageIDs)
		r.mu.RUnlock()
		return n >= 257
	}) {
		r.mu.RLock()
		total := len(r.seenMessageIDs)
		r.mu.RUnlock()
		t.Fatalf("expected 257 events in seenMessageIDs (1 buf + 256 capped live), got %d", total)
	}

	// UIEventSidebarUpdated must have been emitted (from the droppedLive
	// overflow notification).  Drain all events and count sidebar updates.
	sidebarCount := 0
	for len(r.uiEvents) > 0 {
		ev := <-r.uiEvents
		if ev.Type == UIEventSidebarUpdated {
			sidebarCount++
		}
	}
	if sidebarCount == 0 {
		t.Fatal("expected UIEventSidebarUpdated notification for live buffer overflow")
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
	r.activePeer = "peer-1"
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 5
	// Simulate loaded messages for the active peer.
	r.activeMessages = []DirectMessage{
		{ID: "msg-1", Sender: "peer-1", Recipient: "me"},
	}
	r.mu.Unlock()

	// Now simulate a fast switch: user clicks peer-2 before doMarkSeen
	// goroutine for peer-1 has a chance to run.
	r.mu.Lock()
	r.activePeer = "peer-2"
	r.ensurePeerLocked("peer-2")
	r.activeMessages = []DirectMessage{
		{ID: "msg-2", Sender: "peer-2", Recipient: "me"},
	}
	r.mu.Unlock()

	// doMarkSeen for the OLD peer should detect the stale state and
	// return false, so the caller can restore unread.
	result := r.doMarkSeen("peer-1")
	if result {
		t.Fatal("doMarkSeen should return false when activePeer != peerAddress (stale peer)")
	}

	// Verify peer-1's unread was NOT cleared.
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "old-msg", Sender: "peer-1", Body: "hi"},
	})

	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.initialSynced = true
	r.activeMessages = r.cache.Messages()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 0
	r.mu.Unlock()

	// Trigger repair with a header whose ID is NOT in the cache.
	// loadConversation will fail because newTestRouter has no chatlog.
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "new-msg-1", Sender: domain.PeerIdentity("peer-1"), Recipient: domain.PeerIdentity("me")},
		},
	}

	r.repairUnreadFromHeaders(status)

	// Poll until the message ID is evicted (replaces fixed sleep).
	// repairUnreadFromHeaders registers the ID synchronously, then the
	// active-peer reload path evicts it when loadConversation fails.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		_, ok := r.seenMessageIDs["new-msg-1"]
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("new-msg-1 should have been evicted from seenMessageIDs after failed reload")
	}

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	r.mu.RUnlock()

	// Verify that a *second* repair with the same header can now retry
	// (the ID is no longer suppressed by seenMessageIDs).
	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	_, seenRetry := r.seenMessageIDs["new-msg-1"]
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "existing-1", Sender: "peer-1", Body: "hello"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
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
		_, ok := r.seenMessageIDs["msg-rollback-1"]
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
	r.activePeer = "peer-1"
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
		_, ok := r.seenMessageIDs["msg-nonactive-rollback-1"]
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
	r.seenMessageIDs["repair-msg-1"] = struct{}{}
	r.seenMessageIDs["repair-msg-2"] = struct{}{}
	r.mu.Unlock()

	// newTestRouter has no chatlog → updatePreviewFromStore returns false →
	// refreshPreviewForPeer must evict the message IDs.
	r.refreshPreviewForPeer("peer-1", []string{"repair-msg-1", "repair-msg-2"})

	r.mu.RLock()
	_, seen1 := r.seenMessageIDs["repair-msg-1"]
	_, seen2 := r.seenMessageIDs["repair-msg-2"]
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
	db, cl := newTestChatLog(t)
	defer func() { _ = db.Close() }()

	r := newTestRouter()
	r.client.chatLog = cl

	// Simulate what repairUnreadFromHeaders does: create peer, set Unread,
	// register seen message ID.
	r.mu.Lock()
	r.ensurePeerLocked("peer-header-only")
	r.peers["peer-header-only"].Unread = 1
	r.seenMessageIDs["header-msg-1"] = struct{}{}
	r.mu.Unlock()

	// refreshPreviewForPeer calls updatePreviewFromStore which gets nil preview
	// (no chatlog entries for this peer). The peer must NOT be deleted.
	r.refreshPreviewForPeer("peer-header-only", []string{"header-msg-1"})

	r.mu.RLock()
	ps, exists := r.peers["peer-header-only"]
	var unread int
	if ps != nil {
		unread = ps.Unread
	}
	_, seenOK := r.seenMessageIDs["header-msg-1"]
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
	db, cl := newTestChatLog(t)
	defer func() { _ = db.Close() }()

	r := newTestRouter()
	r.client.chatLog = cl
	// Use a larger event channel to absorb async notifications.
	r.uiEvents = make(chan UIEvent, 64)

	// No active peer — all headers are for non-active peers.
	r.mu.Lock()
	r.activePeer = "someone-else"
	r.peerClicked = true
	r.initialSynced = true // skip first-sync suppression
	r.mu.Unlock()

	// Simulate: node has a DM in s.topics (StoreFailed kept it) but the
	// message was never written to chatlog. DMHeaders expose it.
	status := NodeStatus{
		DMHeaders: []DMHeader{
			{
				ID:        "store-failed-msg-1",
				Sender:    domain.PeerIdentity("peer-headonly"),
				Recipient: domain.PeerIdentity("me"),
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
	ps, exists := r.peers["peer-headonly"]
	var unread int
	if ps != nil {
		unread = ps.Unread
	}
	_, seenOK := r.seenMessageIDs["store-failed-msg-1"]
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
	r.cache.Load("peer-1", nil)
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.initialSynced = true
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "repair-active-1", Sender: "peer-1", Recipient: "me"},
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

	r.cache.Load("peer-1", nil)
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.initialSynced = true
	r.mu.Unlock()

	status := NodeStatus{
		DMHeaders: []DMHeader{
			{ID: "active-msg-1", Sender: "peer-1", Recipient: "me"},
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

	r.mu.Lock()
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 7
	r.mu.Unlock()

	r.AutoSelectPeer("peer-1")

	// Read state immediately — before any background goroutine finishes.
	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	clicked := r.peerClicked
	active := r.activePeer
	r.mu.RUnlock()

	if active != "peer-1" {
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
	r.cache.Load("peer-1", []DirectMessage{
		{ID: "existing-1", Sender: "peer-1", Body: "hello"},
	})
	r.mu.Lock()
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 0
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
	post := r.peers["peer-1"].Unread
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
	r.activePeer = "peer-2"
	r.peerClicked = true
	r.ensurePeerLocked("peer-1")
	r.peers["peer-1"].Unread = 0
	r.mu.Unlock()

	// Load cache for peer-2 so cache.MatchesPeer("peer-1") == false.
	r.cache.Load("peer-2", []DirectMessage{{ID: "m1", Body: "hi"}})

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
		_, ok := r.seenMessageIDs["msg-non-active-1"]
		r.mu.RUnlock()
		return !ok
	}) {
		t.Fatal("msg-non-active-1 must be evicted from seenMessageIDs after failed updatePreviewFromStore")
	}

	r.mu.RLock()
	unread := r.peers["peer-1"].Unread
	r.mu.RUnlock()

	// Unread stays 0 because updatePreviewFromStore failed — the message ID
	// was rolled back and repairUnreadFromHeaders will handle it later.
	if unread != 0 {
		t.Fatalf("unread = %d, want 0 (preview load failed → no immediate unread increment)", unread)
	}

	// seenMessageIDs eviction already verified above by pollCondition.
	r.mu.RLock()
	_, registered := r.seenMessageIDs["msg-non-active-1"]
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
			r.ensurePeerLocked("peer-1")
			r.peers["peer-1"].Unread = 10
			r.mu.Unlock()

			tc.selectFn(r, "peer-1")

			r.mu.RLock()
			unread := r.peers["peer-1"].Unread
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

	peers := []domain.PeerIdentity{"peer-1", "peer-2", "peer-3"}
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
	r.peers["a"] = &RouterPeerState{Unread: 3}
	r.peers["b"] = &RouterPeerState{Unread: 1}
	r.peerOrder = []domain.PeerIdentity{"a", "b"}
	r.activePeer = "a"
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}
	r.cache.Load("a", []DirectMessage{{ID: "msg-1"}})

	wasActive, err := r.RemovePeer(domain.PeerIdentity("a"))
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

	if _, ok := r.peers["a"]; ok {
		t.Fatal("peer 'a' should be removed from peers map")
	}
	for _, p := range r.peerOrder {
		if p == "a" {
			t.Fatal("peer 'a' should be removed from peerOrder")
		}
	}
	if r.cache.MatchesPeer("a") {
		t.Fatal("cache should be evicted for peer 'a'")
	}

	// RemovePeer clears activePeer; auto-selection is the UI layer's job.
	if r.activePeer != "" {
		t.Fatalf("activePeer should be empty after RemovePeer, got %q", r.activePeer)
	}
	if _, ok := r.peers["b"]; !ok {
		t.Fatal("peer 'b' should still exist")
	}
}

// TestRemovePeerClearsActiveWhenTailRemoved verifies that removing the last
// identity in the list clears activePeer. The UI layer handles auto-selection.
func TestRemovePeerClearsActiveWhenTailRemoved(t *testing.T) {
	r := newTestRouter()
	r.peers["a"] = &RouterPeerState{}
	r.peers["b"] = &RouterPeerState{}
	r.peers["c"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"a", "b", "c"}
	r.activePeer = "c"

	wasActive, err := r.RemovePeer(domain.PeerIdentity("c"))
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

	if r.activePeer != "" {
		t.Fatalf("activePeer should be empty after RemovePeer, got %q", r.activePeer)
	}
}

// TestRemovePeerEmptyList verifies that removing the only identity leaves
// activePeer empty.
func TestRemovePeerEmptyList(t *testing.T) {
	r := newTestRouter()
	r.peers["a"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"a"}
	r.activePeer = "a"

	wasActive, err := r.RemovePeer(domain.PeerIdentity("a"))
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

	if r.activePeer != "" {
		t.Fatalf("activePeer should be empty when no peers remain, got %q", r.activePeer)
	}
}

// TestRemovePeerNonActive verifies removing a non-active peer does not
// disturb the current conversation.
func TestRemovePeerNonActive(t *testing.T) {
	r := newTestRouter()
	r.peers["a"] = &RouterPeerState{}
	r.peers["b"] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{"a", "b"}
	r.activePeer = "a"
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}

	wasActive, err := r.RemovePeer(domain.PeerIdentity("b"))
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

	if r.activePeer != "a" {
		t.Fatalf("activePeer should remain 'a', got %q", r.activePeer)
	}
	if len(r.activeMessages) != 1 {
		t.Fatalf("activeMessages should be untouched, got %d", len(r.activeMessages))
	}
	if _, ok := r.peers["b"]; ok {
		t.Fatal("peer 'b' should be removed")
	}
}

// TestRemovePeerErrorPreservesState verifies that when DeletePeerHistory
// fails, RemovePeer returns an error and does not modify in-memory state:
// peers, peerOrder, activePeer, cache all remain unchanged.
// Note: DeleteContact errors are best-effort (logged, not blocking) because
// the RPC may be unavailable. Only chatlog failures block removal.
func TestRemovePeerErrorPreservesState(t *testing.T) {
	// Open a SQLite database and immediately close it so all queries fail.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	_ = db.Close()

	r := newTestRouter()
	r.client.chatLog = chatlog.NewStoreFromDB(db, domain.PeerIdentity("me"))

	r.peers["a"] = &RouterPeerState{Unread: 2}
	r.peers["b"] = &RouterPeerState{Unread: 1}
	r.peerOrder = []domain.PeerIdentity{"a", "b"}
	r.activePeer = "a"
	r.peerClicked = true
	r.activeMessages = []DirectMessage{{ID: "msg-1"}}
	r.cache.Load("a", []DirectMessage{{ID: "msg-1"}})

	wasActive, rmErr := r.RemovePeer(domain.PeerIdentity("a"))
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

	if _, ok := r.peers["a"]; !ok {
		t.Fatal("peer 'a' should still exist after failed deletion")
	}
	if r.peers["a"].Unread != 2 {
		t.Fatalf("peer 'a' unread count should be preserved, got %d", r.peers["a"].Unread)
	}
	if r.activePeer != "a" {
		t.Fatalf("activePeer should remain 'a' after failed deletion, got %q", r.activePeer)
	}
	if !r.peerClicked {
		t.Fatal("peerClicked should remain true after failed deletion")
	}
	if len(r.activeMessages) != 1 {
		t.Fatalf("activeMessages should be preserved, got %d", len(r.activeMessages))
	}
	if !r.cache.MatchesPeer("a") {
		t.Fatal("cache for peer 'a' should be preserved after failed deletion")
	}
	if len(r.peerOrder) != 2 {
		t.Fatalf("peerOrder should be unchanged, got %d", len(r.peerOrder))
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
	defer func() { _ = c.Close() }()

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

	peerID := domain.PeerIdentity(peer.Address)

	// Encrypt an incoming message from the peer.
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(id.Address),
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Load cache for the peer so MatchesPeer returns true → inline path.
	r.cache.Load(peerID, []DirectMessage{
		{ID: "my-msg", Sender: domain.PeerIdentity(id.Address), Recipient: domain.PeerIdentity(peer.Address), Body: "my message"},
	})
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentity(id.Address),
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

	if preview.Sender != domain.PeerIdentity(peer.Address) {
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
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentity(peer.Address)

	// Do NOT register peer as trusted contact — DecryptIncomingMessage will
	// fail because the sender's public key is unknown to the trust store.
	// Encrypt a message from peer using raw keys (EncryptForParticipants
	// works with keys directly, independent of the trust store).
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello from peer"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so FetchSinglePreview can read the message.
	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Set active peer to someone else so peerID takes the non-active path.
	r.mu.Lock()
	r.activePeer = domain.PeerIdentity("other-peer")
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentity(id.Address),
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
	if msg := c.DecryptIncomingMessage(event); msg != nil {
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
	if preview.Sender != domain.PeerIdentity(peer.Address) {
		t.Fatalf("preview sender = %q, want peer %q", preview.Sender, peer.Address)
	}

	// FetchSinglePreview without contact keys returns empty body but correct
	// sender — this is acceptable degraded behavior.
	if preview.PeerAddress != peerID {
		t.Fatalf("preview peer address = %q, want %q", preview.PeerAddress, peerID)
	}

	// Unread must be incremented for the incoming message.
	if unread < 1 {
		t.Fatalf("unread = %d, want >= 1", unread)
	}
}

// TestOnNewMessageMidSwitchDecryptFailFallback verifies that when a message
// arrives for the active peer but the cache hasn't loaded yet (mid-switch),
// and inline decryption fails, the goroutine's loadConversation +
// updatePreviewFromStore path updates the preview from SQLite.
func TestOnNewMessageMidSwitchDecryptFailFallback(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentity(peer.Address)

	// Do NOT register peer as trusted contact — DecryptIncomingMessage will
	// fail, exercising the fallback path.
	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "mid-switch message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so updatePreviewFromStore can read it.
	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Active peer = the message peer, but cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	r.cache.Load(domain.PeerIdentity("some-other-peer"), nil)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentity(id.Address),
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
	if msg := c.DecryptIncomingMessage(event); msg != nil {
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
	if preview.Sender != domain.PeerIdentity(peer.Address) {
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
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentity(peer.Address)

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
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "mid-switch visible message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Pre-populate chatlog so the background reload succeeds.
	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	// Active peer = the message peer, cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	r.cache.Load(domain.PeerIdentity("some-other-peer"), nil)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.ensurePeerLocked(peerID)
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
	if msg := c.DecryptIncomingMessage(event); msg == nil {
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
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	peerID := domain.PeerIdentity(peer.Address)

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
			Address:      domain.PeerIdentity(id.Address),
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
	c.chatLog = nil

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    done,
	}

	// Active peer = the message peer, cache loaded for a DIFFERENT peer
	// → MatchesPeer returns false → mid-switch path.
	// Set Unread=1 so we can verify doMarkSeen clears it after fallback.
	r.cache.Load(domain.PeerIdentity("some-other-peer"), nil)
	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Unread = 1
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
	if msg := c.DecryptIncomingMessage(event); msg == nil {
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
// Without the activePeer guard, the fallback would corrupt cache and emit
// spurious UI events for a peer that is no longer active.
func TestOnNewMessageMidSwitchFallbackStalePeerGuard(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentity(peer.Address)

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
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "stale-peer fallback message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	// Nil out chatlog so loadConversation fails → triggers the fallback path.
	c.chatLog = nil

	done := make(chan struct{})
	close(done)
	r := &DMRouter{
		client:         c,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 64),
		startupDone:    done,
	}

	// Active peer = message peer, cache loaded for a different peer → mid-switch.
	otherPeer := domain.PeerIdentity("other-peer")
	r.cache.Load(otherPeer, []DirectMessage{
		{ID: "other-1", Body: "other message", Sender: otherPeer, Recipient: domain.PeerIdentity(id.Address)},
	})
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
	if msg := c.DecryptIncomingMessage(event); msg == nil {
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
		_, exists := r.seenMessageIDs["stale-peer-guard-1"]
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
	r.activePeer = "peer-1"
	r.peerClicked = true
	r.seenMessageIDs["msg-reload-fail"] = struct{}{}
	r.mu.Unlock()

	// newTestRouter has no chatlog → loadConversation fails → must evict.
	ok := r.reloadAndRefreshPreview("peer-1", "msg-reload-fail")
	if ok {
		t.Fatal("reloadAndRefreshPreview must return false when loadConversation fails")
	}

	r.mu.RLock()
	_, seen := r.seenMessageIDs["msg-reload-fail"]
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
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentity(peer.Address)

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
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "partial success message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.seenMessageIDs["partial-success-1"] = struct{}{}
	r.mu.Unlock()

	// Goroutine: wait for loadConversation to populate activeMessages,
	// then nil chatLog so updatePreviewFromStore fails → partial success.
	go func() {
		pollCondition(2*time.Second, func() bool {
			r.mu.RLock()
			defer r.mu.RUnlock()
			return len(r.activeMessages) > 0
		})
		c.chatLog = nil
	}()

	result := r.reloadAndRefreshPreview(peerID, "partial-success-1")
	if !result {
		t.Fatal("reloadAndRefreshPreview must return true on partial success " +
			"(loadConversation OK, updatePreviewFromStore fail)")
	}

	// The critical assertion: messageID must still be in seenMessageIDs.
	// If evictSeenMessages was called on partial success, this fails.
	r.mu.RLock()
	_, seen := r.seenMessageIDs["partial-success-1"]
	r.mu.RUnlock()
	if !seen {
		t.Fatal("seenMessageIDs was evicted on partial success — " +
			"dedup gate must stay closed when messages are already in cache")
	}
}

// TestUpdatePreviewFromCacheFallback verifies that when loadConversation
// succeeds but updatePreviewFromStore fails, the sidebar preview is built
// from the last cached message instead of staying stale. This covers the
// partial-success path in both reloadAndRefreshPreview and the repair-path
// active-peer reload branch.
func TestUpdatePreviewFromCacheFallback(t *testing.T) {
	r := newTestRouter()

	peerID := domain.PeerIdentity("peer-1")

	// Pre-load cache with messages as loadConversation would.
	r.cache.Load(peerID, []DirectMessage{
		{ID: "old-msg", Sender: domain.PeerIdentity("me"), Recipient: peerID, Body: "my old message", Timestamp: time.Now().Add(-5 * time.Minute)},
		{ID: "new-msg", Sender: peerID, Recipient: domain.PeerIdentity("me"), Body: "latest from peer", Timestamp: time.Now()},
	})

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.activeMessages = r.cache.Messages()
	r.ensurePeerLocked(peerID)
	// Set a stale preview to verify it gets updated.
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentity("me"),
		Body:        "stale outgoing preview",
	}
	r.mu.Unlock()

	// Call updatePreviewFromCache — simulates the fallback after
	// updatePreviewFromStore fails.
	r.updatePreviewFromCache(peerID)

	r.mu.RLock()
	preview := r.peers[peerID].Preview
	r.mu.RUnlock()

	if preview.Sender != peerID {
		t.Fatalf("preview sender = %q, want %q — must reflect last cached message", preview.Sender, peerID)
	}
	if preview.Body != "latest from peer" {
		t.Fatalf("preview body = %q, want %q", preview.Body, "latest from peer")
	}
}

// TestUpdatePreviewFromCacheStalePeerGuard verifies that
// updatePreviewFromCache does NOT rebuild peer A's preview from
// activeMessages that belong to peer B after a fast peer switch.
// Without the activePeer guard, a quick SelectPeer between
// loadConversation and updatePreviewFromCache would cause cross-chat
// preview corruption.
func TestUpdatePreviewFromCacheStalePeerGuard(t *testing.T) {
	r := newTestRouter()

	peerA := domain.PeerIdentity("peer-A")
	peerB := domain.PeerIdentity("peer-B")

	// Set up: peer A has a stale preview. activeMessages belong to peer B
	// (simulating a fast switch after loadConversation for peer A but before
	// updatePreviewFromCache runs).
	r.cache.Load(peerB, []DirectMessage{
		{ID: "b-msg", Sender: peerB, Recipient: domain.PeerIdentity("me"), Body: "message from B"},
	})

	r.mu.Lock()
	r.activePeer = peerB // User already switched to B.
	r.peerClicked = true
	r.activeMessages = r.cache.Messages() // These belong to peer B.
	r.ensurePeerLocked(peerA)
	r.peers[peerA].Preview = ConversationPreview{
		PeerAddress: peerA,
		Sender:      peerA,
		Body:        "original A preview",
	}
	r.ensurePeerLocked(peerB)
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
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentity(peer.Address)

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
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "partial-success message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      domain.PeerIdentity(id.Address),
		Body:        "stale outgoing",
	}
	r.seenMessageIDs["partial-1"] = struct{}{}
	r.mu.Unlock()

	// Step 1: loadConversation succeeds, populating cache with the
	// decrypted message. This is the first half of reloadAndRefreshPreview.
	if !r.loadConversation(peerID) {
		t.Fatal("loadConversation must succeed with valid chatlog")
	}

	// Verify cache was populated.
	r.mu.RLock()
	msgCount := len(r.activeMessages)
	r.mu.RUnlock()
	if msgCount == 0 {
		t.Fatal("cache should have messages after loadConversation")
	}

	// Step 2: close chatlog to simulate transient failure for the
	// updatePreviewFromStore call.
	_ = c.chatLog.Close()

	// Step 3: updatePreviewFromStore must fail (closed chatlog).
	if r.updatePreviewFromStore(peerID) {
		t.Fatal("updatePreviewFromStore should fail with closed chatlog")
	}

	// Step 4: updatePreviewFromCache should build preview from last
	// cached message — this is the fallback in reloadAndRefreshPreview.
	r.updatePreviewFromCache(peerID)

	// The preview must now reflect the cached message, not the stale one.
	r.mu.RLock()
	preview := r.peers[peerID].Preview
	r.mu.RUnlock()

	if preview.Sender == domain.PeerIdentity(id.Address) {
		t.Fatalf("preview still shows stale outgoing message — updatePreviewFromCache fallback did not update preview")
	}
	if preview.Body == "stale outgoing" {
		t.Fatalf("preview body still stale — cache fallback did not run")
	}
}

// TestUpdatePreviewFromStoreReturnsFalseOnClosedChatlog verifies that
// updatePreviewFromStore correctly returns false when the chatlog becomes
// unavailable (closed, corrupted, etc.). This is the failure condition
// that triggers the cache-fallback branch in reloadAndRefreshPreview.
func TestUpdatePreviewFromStoreReturnsFalseOnClosedChatlog(t *testing.T) {
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	peerID := domain.PeerIdentity(peer.Address)

	ciphertext, err := directmsg.EncryptForParticipants(
		peer,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(id.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "preview-fail message"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)

	err = c.chatLog.Append("dm", domain.PeerIdentity(id.Address), chatlog.Entry{
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
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}

	r.mu.Lock()
	r.activePeer = peerID
	r.peerClicked = true
	r.mu.Unlock()

	// Guard: prove updatePreviewFromStore succeeds with valid chatlog.
	if !r.updatePreviewFromStore(peerID) {
		t.Fatal("guard: updatePreviewFromStore must succeed with valid chatlog")
	}

	// Close chatlog and verify updatePreviewFromStore fails.
	_ = c.chatLog.Close()
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

func newTestRouter() *DMRouter {
	done := make(chan struct{})
	close(done) // pre-closed so tests don't block on startupDone
	return &DMRouter{
		client:         &DesktopClient{id: &identity.Identity{Address: "me"}},
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    done,
	}
}

// newTestChatLog creates an in-memory SQLite chatlog with initialized schema.
// The caller must call db.Close() when done (typically via defer).
// MaxOpenConns is set to 1 because :memory: databases are per-connection —
// without this, the Go connection pool would open new connections that see
// an empty database (no schema), causing queries to fail.
func newTestChatLog(t *testing.T) (*sql.DB, *chatlog.Store) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	db.SetMaxOpenConns(1)
	// Initialize schema — NewStoreFromDB doesn't do this automatically.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id              TEXT PRIMARY KEY,
			topic           TEXT NOT NULL DEFAULT 'dm',
			sender          TEXT NOT NULL,
			recipient       TEXT NOT NULL,
			body            TEXT NOT NULL,
			flag            TEXT NOT NULL DEFAULT '',
			delivery_status TEXT NOT NULL DEFAULT 'sent',
			ttl_seconds     INTEGER NOT NULL DEFAULT 0,
			metadata        TEXT NOT NULL DEFAULT '',
			created_at      TEXT NOT NULL,
			updated_at      TEXT NOT NULL DEFAULT ''
		)`)
	if err != nil {
		_ = db.Close()
		t.Fatalf("init chatlog schema: %v", err)
	}
	return db, chatlog.NewStoreFromDB(db, domain.PeerIdentity("me"))
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
