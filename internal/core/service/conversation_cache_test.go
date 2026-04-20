package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestCacheLoadAndMessages(t *testing.T) {
	cache := NewConversationCache()

	msgs := []DirectMessage{
		{ID: "m1", Sender: "alice", Recipient: "bob", Body: "hello", Timestamp: time.Now()},
		{ID: "m2", Sender: "bob", Recipient: "alice", Body: "hi", Timestamp: time.Now()},
	}
	cache.Load("bob", msgs)

	if got := cache.PeerAddress(); got != "bob" {
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

	cache.Load("alice", []DirectMessage{
		{ID: "old-1", Sender: "alice", Body: "old"},
	})
	cache.Load("bob", []DirectMessage{
		{ID: "new-1", Sender: "bob", Body: "new"},
	})

	if got := cache.PeerAddress(); got != "bob" {
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
	cache.Load("bob", nil)

	msg := DirectMessage{ID: "m1", Sender: "alice", Recipient: "bob", Body: "hello"}

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
	cache.Load("bob", []DirectMessage{
		{ID: "m1", Body: "first"},
	})

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
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "sent"},
	})

	// sent → delivered: should succeed.
	deliveredAt := now.Add(1 * time.Second)
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(deliveredAt)) {
		t.Fatal("sent → delivered should succeed")
	}

	// delivered → seen: should succeed.
	seenAt := now.Add(2 * time.Second)
	if !cache.UpdateStatus("m1", "seen", domain.TimeOf(seenAt)) {
		t.Fatal("delivered → seen should succeed")
	}

	// seen → delivered: regression should fail.
	if cache.UpdateStatus("m1", "delivered", domain.TimeOf(now)) {
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
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered"},
	})

	// Same status "delivered" but with a real timestamp — should succeed.
	realTime := time.Now()
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(realTime)) {
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
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered", DeliveredAt: domain.TimeOf(zeroTime)},
	})

	// Same status "delivered" but with a real timestamp — should succeed.
	realTime := time.Now()
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(realTime)) {
		t.Fatal("same rank with zero→real DeliveredAt should succeed")
	}
	msgs := cache.Messages()
	if !msgs[0].DeliveredAt.Valid() || !msgs[0].DeliveredAt.Time().Equal(realTime) {
		t.Fatalf("expected real DeliveredAt, got %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusSameRankRejectsNilToNil(t *testing.T) {
	cache := NewConversationCache()
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered"},
	})

	// Same status "delivered" with invalid DeliveredAt — should reject (no improvement).
	if cache.UpdateStatus("m1", "delivered", domain.OptionalTime{}) {
		t.Fatal("same rank with invalid→invalid DeliveredAt should be rejected")
	}
}

func TestCacheUpdateStatusSameRankReplacesRealWithReal(t *testing.T) {
	cache := NewConversationCache()
	existingTime := time.Now()
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "delivered", DeliveredAt: domain.TimeOf(existingTime)},
	})

	// Same status "delivered" with another real timestamp — should accept.
	// This covers the case where a synthetic DeliveredAt (message Timestamp)
	// is later replaced by a real receipt time.
	newTime := existingTime.Add(1 * time.Second)
	if !cache.UpdateStatus("m1", "delivered", domain.TimeOf(newTime)) {
		t.Fatal("same rank with real→real DeliveredAt should succeed")
	}
	msgs := cache.Messages()
	if !msgs[0].DeliveredAt.Time().Equal(newTime) {
		t.Fatalf("expected new DeliveredAt, got %v", msgs[0].DeliveredAt)
	}
}

func TestCacheUpdateStatusNotFound(t *testing.T) {
	cache := NewConversationCache()
	cache.Load("bob", []DirectMessage{
		{ID: "m1", ReceiptStatus: "sent"},
	})

	if cache.UpdateStatus("nonexistent", "delivered", domain.OptionalTime{}) {
		t.Fatal("update for nonexistent message should return false")
	}
}

func TestCacheMatchesPeer(t *testing.T) {
	cache := NewConversationCache()
	cache.Load("bob", nil)

	if !cache.MatchesPeer("bob") {
		t.Fatal("should match loaded peer")
	}
	if cache.MatchesPeer("alice") {
		t.Fatal("should not match different peer")
	}
}

func TestCacheMessagesReturnsCopy(t *testing.T) {
	cache := NewConversationCache()
	cache.Load("bob", []DirectMessage{
		{ID: "m1", Body: "original"},
	})

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

	if got := cache.PeerAddress(); got != "" {
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
	if cache.MatchesPeer("anyone") {
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
