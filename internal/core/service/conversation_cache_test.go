package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestCacheLoadAndMessages(t *testing.T) {
	cache := NewConversationCache()

	msgs := []DirectMessage{
		{ID: "m1", Sender: domaintest.ID("alice"), Recipient: domaintest.ID("bob"), Body: "hello", Timestamp: time.Now()},
		{ID: "m2", Sender: domaintest.ID("bob"), Recipient: domaintest.ID("alice"), Body: "hi", Timestamp: time.Now()},
	}
	cache.Load(domaintest.ID("bob"), msgs, 0)

	if got := cache.PeerAddress(); got != domaintest.ID("bob") {
		t.Fatalf("PeerAddress: got %q, want %q", got, "bob")
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("Len: got %d, want 2", got)
	}

	out := cache.Messages()
	if len(out) != 2 {
		t.Fatalf("Messages: got %d, want 2", len(out))
	}
	if out[0].ID != "m1" || out[1].ID != "m2" {
		t.Fatalf("Messages: unexpected IDs: %q, %q", out[0].ID, out[1].ID)
	}
}

func TestCacheLoadReplacesOld(t *testing.T) {
	cache := NewConversationCache()

	cache.Load(domaintest.ID("alice"), []DirectMessage{
		{ID: "old-1", Sender: domaintest.ID("alice"), Body: "old"},
	}, 0)
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "new-1", Sender: domaintest.ID("bob"), Body: "new"},
	}, 0)

	if got := cache.PeerAddress(); got != domaintest.ID("bob") {
		t.Fatalf("PeerAddress after reload: got %q, want %q", got, "bob")
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("Len after reload: got %d, want 1", got)
	}
	if cache.HasMessage("old-1") {
		t.Fatal("old message should not be in cache after reload")
	}
	if !cache.HasMessage("new-1") {
		t.Fatal("new message should be in cache after reload")
	}
}

func TestCacheAppendMessageIdempotent(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), nil, 0)

	msg := DirectMessage{ID: "m1", Sender: domaintest.ID("alice"), Recipient: domaintest.ID("bob"), Body: "hello"}

	if !cache.AppendMessage(msg) {
		t.Fatal("first append should return true")
	}
	if cache.AppendMessage(msg) {
		t.Fatal("second append of same ID should return false")
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("Len after idempotent append: got %d, want 1", got)
	}
}

func TestCacheAppendPreservesOrder(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", Body: "first"},
	}, 0)

	cache.AppendMessage(DirectMessage{ID: "m2", Body: "second"})
	cache.AppendMessage(DirectMessage{ID: "m3", Body: "third"})

	msgs := cache.Messages()
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].ID != "m1" || msgs[1].ID != "m2" || msgs[2].ID != "m3" {
		t.Fatalf("unexpected order: %q, %q, %q", msgs[0].ID, msgs[1].ID, msgs[2].ID)
	}
}

func TestCacheUpdateStatusMonotonic(t *testing.T) {
	cache := NewConversationCache()
	now := time.Now()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "sent"},
	}, 0)

	// sent → delivered: should succeed.
	deliveredAt := now.Add(1 * time.Second)
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(deliveredAt), true) {
		t.Fatal("sent → delivered should succeed")
	}

	// delivered → seen: should succeed.
	seenAt := now.Add(2 * time.Second)
	if !cache.UpdateStatus("m1", "seen", domain.TimeOf(seenAt), true) {
		t.Fatal("delivered → seen should succeed")
	}

	// seen → delivered: regression should fail.
	if cache.UpdateStatus("m1", "delivered", domain.TimeOf(now), true) {
		t.Fatal("seen → delivered regression should fail")
	}

	// Verify final state.
	msgs := cache.Messages()
	if msgs[0].ReceiptStatus != "seen" {
		t.Fatalf("expected status 'seen', got %q", msgs[0].ReceiptStatus)
	}
	if !msgs[0].DeliveredAt.Valid() || !msgs[0].DeliveredAt.Time().Equal(seenAt) {
		t.Fatalf("unexpected DeliveredAt: %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusSameRankReplacesNilDeliveredAt(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered"},
	}, 0)

	// Same status "delivered" but with a real timestamp — should succeed.
	realTime := time.Now()
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(realTime), true) {
		t.Fatal("same rank with nil→real DeliveredAt should succeed")
	}
	msgs := cache.Messages()
	if !msgs[0].DeliveredAt.Valid() || !msgs[0].DeliveredAt.Time().Equal(realTime) {
		t.Fatalf("expected real DeliveredAt, got %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusSameRankReplacesZeroDeliveredAt(t *testing.T) {
	cache := NewConversationCache()
	zeroTime := time.Time{}
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered", DeliveredAt: domain.TimeOf(zeroTime)},
	}, 0)

	// Same status "delivered" but with a real timestamp — should succeed.
	realTime := time.Now()
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(realTime), true) {
		t.Fatal("same rank with zero→real DeliveredAt should succeed")
	}
	msgs := cache.Messages()
	if !msgs[0].DeliveredAt.Valid() || !msgs[0].DeliveredAt.Time().Equal(realTime) {
		t.Fatalf("expected real DeliveredAt, got %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusSameRankRejectsNilToNil(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered"},
	}, 0)

	// Same status "delivered" with invalid DeliveredAt — should reject (no improvement).
	if cache.UpdateStatus("m1", "delivered", domain.OptionalTime{}, true) {
		t.Fatal("same rank with invalid→invalid DeliveredAt should be rejected")
	}
}

func TestCacheUpdateStatusSameRankReplacesRealWithReal(t *testing.T) {
	cache := NewConversationCache()
	existingTime := time.Now()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered", DeliveredAt: domain.TimeOf(existingTime)},
	}, 0)

	// Same status "delivered" with another real timestamp — should accept.
	// This covers the case where a synthetic DeliveredAt (message Timestamp)
	// is later replaced by a real receipt time.
	newTime := existingTime.Add(1 * time.Second)
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(newTime), true) {
		t.Fatal("same rank with real→real DeliveredAt should succeed")
	}
	msgs := cache.Messages()
	if !msgs[0].DeliveredAt.Time().Equal(newTime) {
		t.Fatalf("expected new DeliveredAt, got %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusNotFound(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", ReceiptStatus: "sent"},
	}, 0)

	if cache.UpdateStatus("nonexistent", "delivered", domain.OptionalTime{}, true) {
		t.Fatal("update for nonexistent message should return false")
	}
}

func TestCacheMatchesPeer(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), nil, 0)

	if !cache.MatchesPeer(domaintest.ID("bob")) {
		t.Fatal("should match loaded peer")
	}
	if cache.MatchesPeer(domaintest.ID("alice")) {
		t.Fatal("should not match different peer")
	}
}

func TestCacheMessagesReturnsCopy(t *testing.T) {
	cache := NewConversationCache()
	cache.Load(domaintest.ID("bob"), []DirectMessage{
		{ID: "m1", Body: "original"},
	}, 0)

	// Modify the returned slice.
	msgs := cache.Messages()
	msgs[0].Body = "modified"

	// Cache should be unaffected.
	original := cache.Messages()
	if original[0].Body != "original" {
		t.Fatal("Messages() should return a copy, not a reference")
	}
}

func TestCacheEmptyState(t *testing.T) {
	cache := NewConversationCache()

	if got := cache.PeerAddress(); !got.IsZero() {
		t.Fatalf("empty cache PeerAddress: got %q, want empty", got)
	}
	if got := cache.Len(); got != 0 {
		t.Fatalf("empty cache Len: got %d, want 0", got)
	}
	if got := cache.Messages(); got != nil {
		t.Fatalf("empty cache Messages: got %v, want nil", got)
	}
	if cache.HasMessage("anything") {
		t.Fatal("empty cache should not have any message")
	}
	if cache.MatchesPeer(domaintest.ID("anyone")) {
		t.Fatal("empty cache should not match any peer")
	}
}

func TestStatusRank(t *testing.T) {
	if statusRank("") >= statusRank("sent") {
		t.Fatal("empty < sent")
	}
	if statusRank("sent") >= statusRank("delivered") {
		t.Fatal("sent < delivered")
	}
	if statusRank("delivered") >= statusRank("seen") {
		t.Fatal("delivered < seen")
	}
}

// TestLoadKeepsTheHigherStatusOfTheSameConversation reproduces the window
// a reload opens: the read takes seconds, and what arrives meanwhile is
// applied to the cache immediately.
//
// The snapshot the read returns is therefore not necessarily the newer
// answer. Putting it back wholesale rolls the badge back to `queued` for
// a message the peer has already taken — and nothing corrects that, since
// the bus counted the event delivered when the router took it, so no
// repair pass and no re-send will offer it again.
func TestLoadKeepsTheHigherStatusOfTheSameConversation(t *testing.T) {
	peer := domaintest.ID("peer-1")
	other := domaintest.ID("peer-2")
	deliveredAt := domain.TimeOf(time.Now().UTC())

	for _, tc := range []struct {
		name     string
		cached   string
		loaded   string
		wantKept string
	}{
		{"a live delivered outranks a queued snapshot", MessageStatusDelivered, MessageStatusQueued, MessageStatusDelivered},
		{"a live seen outranks a delivered snapshot", MessageStatusSeen, MessageStatusDelivered, MessageStatusSeen},
		{"a newer snapshot still wins", MessageStatusSent, MessageStatusSeen, MessageStatusSeen},
		{"an unknown cached status never wins", "", MessageStatusSent, MessageStatusSent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewConversationCache()
			// The state the read started from.
			cache.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: MessageStatusQueued}}, 0)
			// What the live handler applied while the read ran.
			cache.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: tc.cached, DeliveredAt: deliveredAt}}, 0)

			// The snapshot, taken before that and arriving after it.
			cache.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: tc.loaded}}, 0)

			messages := cache.Messages()
			if len(messages) != 1 {
				t.Fatalf("cache holds %d messages, want 1", len(messages))
			}
			if got := messages[0].ReceiptStatus; got != tc.wantKept {
				t.Errorf("status = %q, want %q: a reload must not move a message backwards", got, tc.wantKept)
			}
		})
	}

	// A live receipt applied through UpdateStatus — the ordinary path —
	// must be recorded as REAL, or the reload below cannot tell it from a
	// synthesised one and the distinction is useless exactly where it is
	// needed.
	viaUpdate := NewConversationCache()
	viaUpdate.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: MessageStatusSent}}, 0)
	receiptTime := domain.TimeOf(time.Now().UTC())
	if !viaUpdate.UpdateStatus("m1", MessageStatusDelivered, receiptTime, true) {
		t.Fatal("the live receipt was not applied")
	}
	viaUpdate.Load(peer, []DirectMessage{{
		ID: "m1", ReceiptStatus: MessageStatusDelivered,
		DeliveredAt: domain.TimeOf(time.Now().UTC().Add(time.Hour)),
	}}, 0)
	if got := viaUpdate.Messages()[0].DeliveredAt.Time(); !got.Equal(receiptTime.Time()) {
		t.Errorf("DeliveredAt = %v, want the receipt time %v: a receipt applied live must survive a reload", got, receiptTime.Time())
	}

	// Two REAL receipts for one message share the recipient's clock, so
	// the later one is genuinely newer and the snapshot's earlier one must
	// not roll it back.
	twoReceipts := NewConversationCache()
	first := domain.TimeOf(time.Now().UTC().Add(-time.Minute))
	second := domain.TimeOf(time.Now().UTC())
	twoReceipts.Load(peer, []DirectMessage{{
		ID: "m1", ReceiptStatus: MessageStatusDelivered,
		DeliveredAt: first, DeliveredAtFromReceipt: true,
	}}, 0)
	if !twoReceipts.UpdateStatus("m1", MessageStatusDelivered, second, true) {
		t.Fatal("the second receipt was not applied")
	}
	twoReceipts.Load(peer, []DirectMessage{{
		ID: "m1", ReceiptStatus: MessageStatusDelivered,
		DeliveredAt: first, DeliveredAtFromReceipt: true,
	}}, 0)
	if got := twoReceipts.Messages()[0].DeliveredAt.Time(); !got.Equal(second.Time()) {
		t.Errorf("DeliveredAt = %v, want the newer receipt %v: two receipts share one clock", got, second.Time())
	}

	// Equal rank still keeps the RECEIPT's timestamp over a synthesised
	// one — chosen by origin, not by which is larger. The synthetic value
	// is the sender's clock and the receipt is the recipient's, so this
	// case is deliberately built with the synthetic one AHEAD: a sender
	// running an hour fast is exactly when comparing times gets it wrong.
	fromReceipt := domain.TimeOf(time.Now().UTC())
	synthetic := domain.TimeOf(time.Now().UTC().Add(time.Hour))
	stamped := NewConversationCache()
	stamped.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: MessageStatusDelivered}}, 0)
	stamped.Load(peer, []DirectMessage{{
		ID: "m1", ReceiptStatus: MessageStatusDelivered,
		DeliveredAt: fromReceipt, DeliveredAtFromReceipt: true,
	}}, 0)
	stamped.Load(peer, []DirectMessage{{
		ID: "m1", ReceiptStatus: MessageStatusDelivered, DeliveredAt: synthetic,
	}}, 0)
	if got := stamped.Messages()[0].DeliveredAt.Time(); !got.Equal(fromReceipt.Time()) {
		t.Errorf("DeliveredAt = %v, want the receipt time %v: a synthesised time from a fast sender's clock must not replace a receipt", got, fromReceipt.Time())
	}

	// Switching conversations carries nothing over: a different peer's
	// message with the same id is a different message.
	cache := NewConversationCache()
	cache.Load(peer, []DirectMessage{{ID: "m1", ReceiptStatus: MessageStatusSeen}}, 0)
	cache.Load(other, []DirectMessage{{ID: "m1", ReceiptStatus: MessageStatusQueued}}, 0)
	if got := cache.Messages()[0].ReceiptStatus; got != MessageStatusQueued {
		t.Errorf("status = %q after switching peers, want the loaded %q", got, MessageStatusQueued)
	}
}

// TestLoadKeepsWhatTheReadCouldNotHaveSeen: the snapshot is a window, not
// the whole truth.
//
// A message stored while FetchConversation runs — one the user just sent,
// or one that arrived — is in the cache and not in the snapshot. Dropping
// it takes it off the screen with nothing to put it back: for a message to
// an offline recipient the next event may be days away. A message the read
// DID cover and did not return is a different case entirely: it was
// deleted, and bringing it back would be worse than losing it.
func TestLoadKeepsWhatTheReadCouldNotHaveSeen(t *testing.T) {
	peer := domaintest.ID("peer-1")

	cache := NewConversationCache()
	cache.Load(peer, []DirectMessage{
		{ID: "old", Seq: 10, ReceiptStatus: MessageStatusDelivered},
		{ID: "deleted-during-read", Seq: 11, ReceiptStatus: MessageStatusDelivered},
	}, 0)
	// What the read is authoritative about, captured before it starts —
	// exactly as loadConversation does.
	authoritativeUpTo := cache.HighestSeq()

	// Arrived while the read was running: no row yet (Seq 0) and a row
	// beyond that boundary.
	cache.AppendForPeer(peer, DirectMessage{ID: "just-sent", ReceiptStatus: MessageStatusQueued})
	cache.AppendForPeer(peer, DirectMessage{ID: "stored-later", Seq: 12, ReceiptStatus: MessageStatusSent})

	// The snapshot the read returns: taken before those two, and without
	// the row the user deleted meanwhile.
	cache.Load(peer, []DirectMessage{{ID: "old", Seq: 10, ReceiptStatus: MessageStatusDelivered}}, authoritativeUpTo)

	kept := map[string]bool{}
	for _, msg := range cache.Messages() {
		kept[msg.ID] = true
	}
	for _, id := range []string{"old", "just-sent", "stored-later"} {
		if !kept[id] {
			t.Errorf("%q was dropped by a reload that could not have seen it", id)
		}
	}
	if kept["deleted-during-read"] {
		t.Error("a message the read covered and did not return was resurrected; the snapshot is authoritative about its own range")
	}
}
