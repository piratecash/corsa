package chatlog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestNewStoreBindsIdentityAndExecutor(t *testing.T) {
	// Where the file lives, what it is called and whether it is healthy are
	// storage's concerns now — see internal/core/storage. All the repository
	// keeps is the identity it filters rows by and the injected executor.
	identity := domain.PeerIdentityFromWire("abcdef0123456789abcdef0123456789abcdef01")
	s := newTestStore(t, identity)

	if s.db == nil {
		t.Fatal("expected an injected executor")
	}
	if s.identityAddr != identity {
		t.Fatalf("identityAddr = %s, want %s", s.identityAddr, identity)
	}
}

func TestAppendAndRead(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1122334455667788112233445566778811223344"

	entry := Entry{
		ID:        "msg-001",
		Sender:    peerAddr,
		Recipient: selfAddr,
		Body:      "hello encrypted body",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}

	if err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, err := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peerAddr))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].ID != "msg-001" {
		t.Fatalf("expected id=msg-001, got %s", entries[0].ID)
	}
	if entries[0].Body != "hello encrypted body" {
		t.Fatalf("body mismatch")
	}
	if entries[0].Flag != "immutable" {
		t.Fatalf("flag mismatch: got %s", entries[0].Flag)
	}
}

func TestAppendReportNewDistinguishesInsertFromDuplicate(t *testing.T) {
	selfAddr := "abcdef0123456789abcdef0123456789abcdef01"
	peerAddr := "1234567890abcdef1234567890abcdef12345678"

	s := storeFor(t, selfAddr)

	entry := Entry{
		ID:        "dedup-001",
		Sender:    peerAddr,
		Recipient: selfAddr,
		Body:      "test body",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	// First insert should report new.
	inserted, err := s.AppendReportNew(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if !inserted {
		t.Fatal("first append should report inserted=true")
	}

	// Duplicate insert should report not-new.
	inserted, err = s.AppendReportNew(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry)
	if err != nil {
		t.Fatalf("duplicate append: %v", err)
	}
	if inserted {
		t.Fatal("duplicate append should report inserted=false")
	}

	// Only one row in the database.
	entries, err := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peerAddr))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after duplicate append, got %d", len(entries))
	}
}

func TestAppendMultipleMessages(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1122334455667788112233445566778811223344"

	for i := 0; i < 5; i++ {
		entry := Entry{
			ID:        fmt.Sprintf("msg-%d", i),
			Sender:    selfAddr,
			Recipient: peerAddr,
			Body:      "body",
			CreatedAt: time.Now().UTC().Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
		}
		if err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	entries, err := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peerAddr))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
}

func TestReadLastN(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1122334455667788112233445566778811223344"

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		entry := Entry{
			ID:        fmt.Sprintf("msg-%d", i),
			Sender:    selfAddr,
			Recipient: peerAddr,
			Body:      "body",
			CreatedAt: base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
		}
		_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry)
	}

	last3, err := s.ReadLast(context.Background(), "dm", domain.PeerIdentityFromWire(peerAddr), 3)
	if err != nil {
		t.Fatalf("read last: %v", err)
	}
	if len(last3) != 3 {
		t.Fatalf("expected 3, got %d", len(last3))
	}
	// Last 3 should be entries 7,8,9 in ascending order.
	if last3[0].ID != "msg-7" {
		t.Fatalf("expected msg-7, got %s", last3[0].ID)
	}
	if last3[2].ID != "msg-9" {
		t.Fatalf("expected msg-9, got %s", last3[2].ID)
	}
}

func TestSeparateConversationsPerPeer(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m1", Sender: peer1, Recipient: selfAddr, Body: "from peer1", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m2", Sender: peer2, Recipient: selfAddr, Body: "from peer2", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	entries1, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer1))
	entries2, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer2))

	if len(entries1) != 1 || entries1[0].ID != "m1" {
		t.Fatalf("peer1 entries wrong: %v", entries1)
	}
	if len(entries2) != 1 || entries2[0].ID != "m2" {
		t.Fatalf("peer2 entries wrong: %v", entries2)
	}
}

func TestGlobalMessages(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"

	_ = s.Append(context.Background(), "global", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "g1", Sender: selfAddr, Recipient: "*", Body: "broadcast", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	entries, err := s.Read(context.Background(), "global", domain.PeerIdentity{})
	if err != nil {
		t.Fatalf("read global: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "g1" {
		t.Fatalf("global entries wrong: %v", entries)
	}
}

func TestListConversations(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	t1 := time.Now().UTC().Add(-time.Hour)
	t2 := time.Now().UTC()

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m1", Sender: peer1, Recipient: selfAddr, Body: "old", CreatedAt: t1.Format(time.RFC3339Nano)})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m2", Sender: peer2, Recipient: selfAddr, Body: "new", CreatedAt: t2.Format(time.RFC3339Nano)})

	convs, err := s.ListConversations(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(convs) != 2 {
		t.Fatalf("expected 2 conversations, got %d", len(convs))
	}
	// Both are unread (status=sent by default), so sorted by last message.
	// peer2 has newer message.
	if convs[0].PeerAddress != peer2 {
		t.Fatalf("expected peer2 first, got %s", convs[0].PeerAddress)
	}
	if convs[1].PeerAddress != peer1 {
		t.Fatalf("expected peer1 second, got %s", convs[1].PeerAddress)
	}
}

func TestHasEntryID(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "msg-abc", Sender: peer, Recipient: selfAddr, Body: "test", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	if !s.HasEntryID(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("msg-abc")) {
		t.Fatal("expected HasEntryID to return true")
	}
	if s.HasEntryID(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("msg-xyz")) {
		t.Fatal("expected HasEntryID to return false for non-existent ID")
	}
}

func TestReadEmptyStore(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	entries, err := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire("1111111111111111111111111111111111111111"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if entries != nil {
		t.Fatalf("expected nil entries for empty store, got %v", entries)
	}
}

func TestSeparateDatabasesAreIsolated(t *testing.T) {
	s1 := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")
	s2 := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s1.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "port9999", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	_ = s2.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m2", Sender: peer, Recipient: selfAddr, Body: "port8888", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	e1, _ := s1.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	e2, _ := s2.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))

	if len(e1) != 1 || e1[0].Body != "port9999" {
		t.Fatalf("first database entries wrong: %+v", e1)
	}
	if len(e2) != 1 || e2[0].Body != "port8888" {
		t.Fatalf("second database entries wrong: %+v", e2)
	}
}

func TestDuplicateInsertIgnored(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	entry := Entry{
		ID:        "dup-1",
		Sender:    peer,
		Recipient: selfAddr,
		Body:      "first",
		CreatedAt: "2026-01-01T00:00:00Z",
	}

	if err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Second append with same ID should be silently ignored (INSERT OR IGNORE).
	entry.Body = "duplicate"
	if err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), entry); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after duplicate, got %d", len(entries))
	}
	if entries[0].Body != "first" {
		t.Fatalf("expected original body, got %s", entries[0].Body)
	}
}

// --- Delivery status tests ---

func TestAppendSetsDeliveryStatus(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-s1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("expected status=%q, got %q", StatusSent, entries[0].DeliveryStatus)
	}
}

func TestAppendDefaultsToSentStatus(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	// Append without setting DeliveryStatus — should default to "sent".
	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-default", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("expected default status=%q, got %q", StatusSent, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatus(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-1", Sender: selfAddr, Recipient: peer,
		Body: "first", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-2", Sender: peer, Recipient: selfAddr,
		Body: "second", CreatedAt: "2026-01-01T00:01:00Z", DeliveryStatus: StatusDelivered,
	})

	updated, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("msg-1"), StatusDelivered)
	if err != nil {
		t.Fatalf("update status: %v", err)
	}
	if !updated {
		t.Fatal("expected update to return true")
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("msg-1 status expected %q, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}
	if entries[1].DeliveryStatus != StatusDelivered {
		t.Fatalf("msg-2 status expected %q, got %q", StatusDelivered, entries[1].DeliveryStatus)
	}
	// Bodies must survive the update.
	if entries[0].Body != "first" || entries[1].Body != "second" {
		t.Fatalf("bodies corrupted after update: %+v", entries)
	}
}

func TestUpdateStatusNotFoundReturnsFalse(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})

	updated, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("nonexistent"), StatusDelivered)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updated {
		t.Fatal("expected false for missing message ID")
	}
}

func TestUpdateStatusToSeen(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "msg-1", Sender: peer, Recipient: selfAddr,
		Body: "incoming", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered,
	})

	updated, _ := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("msg-1"), StatusSeen)
	if !updated {
		t.Fatal("expected update to return true")
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("expected %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatusMonotonic(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	// Start at "sent".
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "mono-1", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})

	// Advance sent → delivered: should succeed.
	ok, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("mono-1"), StatusDelivered)
	if err != nil {
		t.Fatalf("sent→delivered error: %v", err)
	}
	if !ok {
		t.Fatal("sent→delivered should return true")
	}

	// Advance delivered → seen: should succeed.
	ok, _ = s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("mono-1"), StatusSeen)
	if !ok {
		t.Fatal("delivered→seen should return true")
	}

	// Attempt regression seen → delivered: should be silently rejected.
	ok, err = s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("mono-1"), StatusDelivered)
	if err != nil {
		t.Fatalf("seen→delivered error: %v", err)
	}
	if ok {
		t.Fatal("seen→delivered should return false (regression)")
	}

	// Attempt regression seen → sent: should also be rejected.
	ok, _ = s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("mono-1"), StatusSent)
	if ok {
		t.Fatal("seen→sent should return false (regression)")
	}

	// Verify status is still "seen".
	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("status should still be %q after regression attempts, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatusDeliveredCannotRegressToSent(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "mono-2", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered,
	})

	ok, _ := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("mono-2"), StatusSent)
	if ok {
		t.Fatal("delivered→sent should return false")
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("expected %q, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}
}

func TestListConversationsIncludesUnreadCount(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peerA := "1111111111111111111111111111111111111111"
	peerB := "2222222222222222222222222222222222222222"

	// peerA: 2 incoming delivered (unread), 1 outgoing sent.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "a1", Sender: peerA, Recipient: selfAddr, Body: "hi", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "a2", Sender: peerA, Recipient: selfAddr, Body: "hey", CreatedAt: "2026-01-01T00:01:00Z", DeliveryStatus: StatusDelivered})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "a3", Sender: selfAddr, Recipient: peerA, Body: "yo", CreatedAt: "2026-01-01T00:02:00Z", DeliveryStatus: StatusSent})

	// peerB: 1 incoming seen (read).
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "b1", Sender: peerB, Recipient: selfAddr, Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSeen})

	convs, err := s.ListConversations(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(convs) != 2 {
		t.Fatalf("expected 2 conversations, got %d", len(convs))
	}

	// Conversations with unread messages should come first.
	if convs[0].PeerAddress != peerA {
		t.Fatalf("expected peerA first (has unread), got %s", convs[0].PeerAddress)
	}
	if convs[0].UnreadCount != 2 {
		t.Fatalf("expected 2 unread for peerA, got %d", convs[0].UnreadCount)
	}
	if convs[1].UnreadCount != 0 {
		t.Fatalf("expected 0 unread for peerB, got %d", convs[1].UnreadCount)
	}
}

func TestListConversationsSortsUnreadFirst(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peerOld := "1111111111111111111111111111111111111111"
	peerNew := "2222222222222222222222222222222222222222"

	// peerOld: old message, but unread.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "o1", Sender: peerOld, Recipient: selfAddr, Body: "old", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered})

	// peerNew: newer message, but already seen.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "n1", Sender: peerNew, Recipient: selfAddr, Body: "new", CreatedAt: "2026-01-02T00:00:00Z", DeliveryStatus: StatusSeen})

	convs, _ := s.ListConversations(context.Background())
	// peerOld should be first despite older timestamp because it has unread.
	if convs[0].PeerAddress != peerOld {
		t.Fatalf("expected unread conversation first, got %s", convs[0].PeerAddress)
	}
}

func TestReadLastEntry(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "first", CreatedAt: "2026-01-01T00:00:00Z"})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "m2", Sender: selfAddr, Recipient: peer, Body: "second", CreatedAt: "2026-01-01T00:01:00Z"})

	entry, err := s.ReadLastEntry(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if err != nil {
		t.Fatalf("read last entry: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if entry.ID != "m2" {
		t.Fatalf("expected m2, got %s", entry.ID)
	}
}

func TestReadLastEntryEmpty(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	entry, err := s.ReadLastEntry(context.Background(), "dm", domain.PeerIdentityFromWire("1111111111111111111111111111111111111111"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry != nil {
		t.Fatalf("expected nil entry for empty conversation, got %+v", entry)
	}
}

func TestReadLastEntryPerPeer(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "p1-old", Sender: peer1, Recipient: selfAddr, Body: "old", CreatedAt: "2026-01-01T00:00:00Z"})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "p1-new", Sender: selfAddr, Recipient: peer1, Body: "new", CreatedAt: "2026-01-01T00:01:00Z"})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{ID: "p2-only", Sender: peer2, Recipient: selfAddr, Body: "only", CreatedAt: "2026-01-01T00:00:30Z"})

	result, err := s.ReadLastEntryPerPeer(context.Background())
	if err != nil {
		t.Fatalf("read last per peer: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(result))
	}
	if result[peer1].ID != "p1-new" {
		t.Fatalf("expected p1-new for peer1, got %s", result[peer1].ID)
	}
	if result[peer2].ID != "p2-only" {
		t.Fatalf("expected p2-only for peer2, got %s", result[peer2].ID)
	}
}

// TestReadLastEntryPerPeerDeterministicOnEqualTimestamp verifies that when
// two messages for the same peer share the exact same created_at timestamp,
// ReadLastEntryPerPeer returns the most recently inserted one (highest rowid).
func TestReadLastEntryPerPeerDeterministicOnEqualTimestamp(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"
	sameTS := "2026-03-24T12:00:00Z"

	// Insert two messages with identical created_at for the same peer.
	// The second insert has a higher rowid and should be the one returned.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "dup-ts-first", Sender: selfAddr, Recipient: peer,
		Body: "first insert", CreatedAt: sameTS,
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "dup-ts-second", Sender: selfAddr, Recipient: peer,
		Body: "second insert", CreatedAt: sameTS,
	})

	result, err := s.ReadLastEntryPerPeer(context.Background())
	if err != nil {
		t.Fatalf("ReadLastEntryPerPeer: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(result))
	}
	if result[peer].ID != "dup-ts-second" {
		t.Fatalf("expected dup-ts-second (most recent insert), got %s", result[peer].ID)
	}
}

// TestReadLastEntryDeterministicOnEqualTimestamp verifies the same
// tiebreaker for ReadLastEntry within a single conversation.
func TestReadLastEntryDeterministicOnEqualTimestamp(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"
	sameTS := "2026-03-24T12:00:00Z"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "same-ts-a", Sender: selfAddr, Recipient: peer,
		Body: "a", CreatedAt: sameTS,
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "same-ts-b", Sender: selfAddr, Recipient: peer,
		Body: "b", CreatedAt: sameTS,
	})

	entry, err := s.ReadLastEntry(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if err != nil {
		t.Fatalf("ReadLastEntry: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if entry.ID != "same-ts-b" {
		t.Fatalf("expected same-ts-b (most recent insert), got %s", entry.ID)
	}
}

// --- Integrity check tests ---

func TestFlagCheckConstraint(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	// All valid flags should be accepted.
	validFlags := []string{FlagNone, FlagImmutable, FlagSenderDelete, FlagAnyDelete, FlagAutoDeleteTTL}
	for i, flag := range validFlags {
		err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
			ID: fmt.Sprintf("flag-%d", i), Sender: selfAddr, Recipient: peer,
			Body: "test", CreatedAt: fmt.Sprintf("2026-01-01T00:%02d:00Z", i), Flag: flag,
		})
		if err != nil {
			t.Fatalf("valid flag %q rejected: %v", flag, err)
		}
	}

	// Invalid flag: INSERT OR IGNORE silently ignores CHECK violations,
	// so the row should not be inserted (no error, but no row either).
	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "flag-bad", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T01:00:00Z", Flag: "invalid-flag",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.HasEntryID(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("flag-bad")) {
		t.Fatal("invalid flag should not have been inserted")
	}
}

func TestDeliveryStatusCheckConstraint(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	// All valid statuses should be accepted.
	for i, status := range []string{StatusSent, StatusDelivered, StatusSeen} {
		err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
			ID: fmt.Sprintf("status-%d", i), Sender: selfAddr, Recipient: peer,
			Body: "test", CreatedAt: fmt.Sprintf("2026-01-01T00:%02d:00Z", i), DeliveryStatus: status,
		})
		if err != nil {
			t.Fatalf("valid status %q rejected: %v", status, err)
		}
	}

	// Invalid delivery_status via direct UPDATE should fail.
	_, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("status-0"), "invalid-status")
	if err == nil {
		t.Fatal("expected error for invalid delivery_status, got nil")
	}
}

func TestTTLSecondsStored(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "ttl-1", Sender: selfAddr, Recipient: peer,
		Body: "ephemeral", CreatedAt: "2026-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 1 {
		t.Fatalf("expected 1, got %d", len(entries))
	}
	if entries[0].TTLSeconds != 3600 {
		t.Fatalf("expected ttl=3600, got %d", entries[0].TTLSeconds)
	}
	if entries[0].Flag != FlagAutoDeleteTTL {
		t.Fatalf("expected flag=%q, got %q", FlagAutoDeleteTTL, entries[0].Flag)
	}
}

func TestMetadataStored(t *testing.T) {
	s := storeFor(t, "aabbccdd11223344aabbccdd11223344aabbccdd")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	meta := `{"edited":true,"edit_at":"2026-01-01T00:05:00Z"}`
	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "meta-1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
		Metadata: meta,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].Metadata != meta {
		t.Fatalf("expected metadata=%q, got %q", meta, entries[0].Metadata)
	}
}

func TestDeleteExpired(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"

	// Message with TTL=1 second created 2 hours ago — should be expired.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "expired-1", Sender: selfAddr, Recipient: peer,
		Body: "old ephemeral", CreatedAt: "2020-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 1,
	})

	// Message with TTL=999999 seconds created recently — should survive.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "alive-1", Sender: selfAddr, Recipient: peer,
		Body: "still alive", CreatedAt: "2099-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 999999,
	})

	// Regular message (no TTL) — should survive.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "normal-1", Sender: selfAddr, Recipient: peer,
		Body: "permanent", CreatedAt: "2020-01-01T00:00:00Z",
	})

	deleted, err := s.DeleteExpired(context.Background())
	if err != nil {
		t.Fatalf("delete expired: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 2 {
		t.Fatalf("expected 2 surviving entries, got %d", len(entries))
	}

	ids := map[string]bool{}
	for _, e := range entries {
		ids[e.ID] = true
	}
	if ids["expired-1"] {
		t.Fatal("expired message should have been deleted")
	}
	if !ids["alive-1"] || !ids["normal-1"] {
		t.Fatal("non-expired messages should survive")
	}
}

func TestDeleteByID(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "del-1", Sender: selfAddr, Recipient: peer,
		Body: "to delete", CreatedAt: "2026-01-01T00:00:00Z",
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "keep-1", Sender: selfAddr, Recipient: peer,
		Body: "to keep", CreatedAt: "2026-01-01T00:01:00Z",
	})

	ok, err := s.DeleteByID(context.Background(), domain.MessageID("del-1"))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !ok {
		t.Fatal("expected true for existing message")
	}

	// Deleting non-existent returns false.
	ok, _ = s.DeleteByID(context.Background(), domain.MessageID("nonexistent"))
	if ok {
		t.Fatal("expected false for non-existent message")
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 1 || entries[0].ID != "keep-1" {
		t.Fatalf("unexpected entries after delete: %+v", entries)
	}
}

func TestDeleteByPeer(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	identityA := domain.PeerIdentityFromWire("1111111111111111111111111111111111111111")
	identityB := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")

	// Messages with identityA (both directions).
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "a-out-1", Sender: selfAddr, Recipient: identityA.String(),
		Body: "outgoing to A", CreatedAt: "2026-01-01T00:00:00Z",
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "a-in-1", Sender: identityA.String(), Recipient: selfAddr,
		Body: "incoming from A", CreatedAt: "2026-01-01T00:01:00Z",
	})

	// Messages with identityB — must survive deletion of identityA.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "b-out-1", Sender: selfAddr, Recipient: identityB.String(),
		Body: "outgoing to B", CreatedAt: "2026-01-01T00:02:00Z",
	})

	n, err := s.DeleteByPeer(context.Background(), identityA)
	if err != nil {
		t.Fatalf("delete identity: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", n)
	}

	// identityA conversation should be empty.
	entries, _ := s.Read(context.Background(), "dm", identityA)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for identityA, got %d", len(entries))
	}

	// identityB conversation should be intact.
	entries, _ = s.Read(context.Background(), "dm", identityB)
	if len(entries) != 1 || entries[0].ID != "b-out-1" {
		t.Fatalf("identityB entries unexpected: %+v", entries)
	}

	// Deleting already-empty identity returns 0.
	n, err = s.DeleteByPeer(context.Background(), identityA)
	if err != nil {
		t.Fatalf("delete empty: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 deleted, got %d", n)
	}

	// Empty identity returns error.
	_, err = s.DeleteByPeer(context.Background(), domain.PeerIdentity{})
	if err == nil {
		t.Fatal("expected error for empty identity")
	}
}

// --- End-to-end receipt→DB→read flow tests ---

// TestReceiptFlowSentToDeliveredToSeen simulates the full lifecycle of a
// message status through the chatlog: append with "sent", update to
// "delivered" (simulating receipt arrival), update to "seen", then verify
// the status is correctly persisted and read back at each step.
func TestReceiptFlowSentToDeliveredToSeen(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1122334455667788112233445566778811223344"

	// Step 1: Outgoing message appended with "sent".
	err := s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "flow-1", Sender: selfAddr, Recipient: peer,
		Body: "encrypted-body", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("step1: expected %q, got %q", StatusSent, entries[0].DeliveryStatus)
	}

	// Step 2: Receipt arrives — update to "delivered".
	ok, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("flow-1"), StatusDelivered)
	if err != nil {
		t.Fatalf("update to delivered: %v", err)
	}
	if !ok {
		t.Fatal("step2: expected update to return true")
	}

	entries, _ = s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("step2: expected %q, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}

	// Step 3: Seen receipt arrives — update to "seen".
	ok, _ = s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("flow-1"), StatusSeen)
	if !ok {
		t.Fatal("step3: expected update to return true")
	}

	entries, _ = s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("step3: expected %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}

	// Step 4: Late "delivered" receipt — must be rejected (monotonic).
	ok, _ = s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("flow-1"), StatusDelivered)
	if ok {
		t.Fatal("step4: late delivered should be rejected after seen")
	}

	entries, _ = s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("step4: status should still be %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

// TestStatusSurvivesStoreReopen verifies that delivery_status persisted in
// SQLite survives closing and reopening the store (simulating a node restart).
func TestStatusSurvivesStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	self := domain.PeerIdentityFromWire("aabbccdd11223344aabbccdd11223344aabbccdd")
	selfAddr := self.String()
	peer := "1122334455667788112233445566778811223344"

	// Open store, write message, update status, close.
	s1 := newTestStoreAt(t, path, self)
	_ = s1.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "persist-1", Sender: selfAddr, Recipient: peer,
		Body: "encrypted", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusSent,
	})
	_, _ = s1.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("persist-1"), StatusDelivered)

	// Reopen the same file — status must survive.
	s2 := newTestStoreAt(t, path, self)

	entries, err := s2.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("expected %q after reopen, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}
}

// TestUnreadCountReflectsStatusUpdates verifies that ListConversations()
// correctly reflects unread counts as delivery_status changes.
func TestUnreadCountReflectsStatusUpdates(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1122334455667788112233445566778811223344"

	// Two incoming messages with "delivered" status (unread).
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "unread-1", Sender: peer, Recipient: selfAddr,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusDelivered,
	})
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "unread-2", Sender: peer, Recipient: selfAddr,
		Body: "world", CreatedAt: "2026-01-01T00:01:00Z",
		DeliveryStatus: StatusDelivered,
	})

	convs, _ := s.ListConversations(context.Background())
	if len(convs) != 1 {
		t.Fatalf("expected 1 conversation, got %d", len(convs))
	}
	if convs[0].UnreadCount != 2 {
		t.Fatalf("expected 2 unread before seen, got %d", convs[0].UnreadCount)
	}

	// Mark first message as seen.
	if _, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("unread-1"), StatusSeen); err != nil {
		t.Fatalf("update status unread-1: %v", err)
	}

	convs, _ = s.ListConversations(context.Background())
	if convs[0].UnreadCount != 1 {
		t.Fatalf("expected 1 unread after marking one seen, got %d", convs[0].UnreadCount)
	}

	// Mark second message as seen.
	if _, err := s.UpdateStatus(context.Background(), "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("unread-2"), StatusSeen); err != nil {
		t.Fatalf("update status unread-2: %v", err)
	}

	convs, _ = s.ListConversations(context.Background())
	if convs[0].UnreadCount != 0 {
		t.Fatalf("expected 0 unread after marking all seen, got %d", convs[0].UnreadCount)
	}
}

// TestMessageBodyStoredAsIs verifies that the body stored in SQLite is the
// exact ciphertext passed to Append — no transformation, no cleartext leak.
// In a real scenario, outgoing DMs are encrypted with the sender's own key
// before storage. This test ensures the chatlog layer doesn't alter the body.
func TestMessageBodyStoredAsIs(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1122334455667788112233445566778811223344"

	// Simulate encrypted body (base64-encoded ciphertext).
	ciphertext := "U2VhbGVkQm94eyJub25jZSI6IjEyMyIsImNpcGhlcnRleHQiOiJhYmMifQ=="

	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(selfAddr), Entry{
		ID: "enc-1", Sender: selfAddr, Recipient: peer,
		Body: ciphertext, CreatedAt: "2026-01-01T00:00:00Z",
	})

	entries, _ := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(peer))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Body != ciphertext {
		t.Fatalf("body was altered in storage: expected %q, got %q", ciphertext, entries[0].Body)
	}

	// Verify it does NOT contain the cleartext "hello" or "SealedBox" prefix
	// in a way that would suggest decryption happened during storage.
	// (This is a sanity check — the chatlog layer should be a dumb store.)
}

// TestOutgoingMessageStoredEncrypted uses real cryptographic operations to
// verify that when an outgoing DM is encrypted via directmsg.EncryptForParticipants
// and stored in the chatlog, the body in the database is ciphertext — not plaintext.
// This is a critical security invariant: the cleartext must never leak into SQLite.
func TestOutgoingMessageStoredEncrypted(t *testing.T) {
	// Generate two identities: sender and recipient.
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}

	s := storeFor(t, sender.Address)

	plaintext := "this is a secret message that must never appear in cleartext"

	// Encrypt via the real directmsg envelope (X25519 + AES-256-GCM + ed25519 signature).
	ciphertext, err := directmsg.EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: plaintext},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Store the encrypted envelope in the chatlog (same path as SendDirectMessage).
	err = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire(sender.Address), Entry{
		ID:             "enc-outgoing-1",
		Sender:         sender.Address,
		Recipient:      recipient.Address,
		Body:           ciphertext,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Read back from SQLite.
	entries, err := s.Read(context.Background(), "dm", domain.PeerIdentityFromWire(recipient.Address))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	storedBody := entries[0].Body

	// 1) The stored body must NOT contain the plaintext.
	if strings.Contains(storedBody, plaintext) {
		t.Fatal("SECURITY: plaintext found in stored body — message is not encrypted!")
	}

	// 2) The stored body must be valid base64 (it's a sealed envelope).
	raw, err := base64.RawURLEncoding.DecodeString(storedBody)
	if err != nil {
		t.Fatalf("stored body is not valid base64: %v", err)
	}

	// 3) The decoded content must be a valid sealed envelope (JSON with "version": "dm-v1").
	var envelope map[string]interface{}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("stored body is not valid JSON envelope: %v", err)
	}
	if envelope["version"] != "dm-v1" {
		t.Fatalf("expected envelope version dm-v1, got %v", envelope["version"])
	}

	// 4) The sender must be able to decrypt their own message (sender-part).
	decrypted, err := directmsg.DecryptForIdentity(
		sender,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		storedBody,
	)
	if err != nil {
		t.Fatalf("sender cannot decrypt own stored message: %v", err)
	}
	if decrypted.Body != plaintext {
		t.Fatalf("decrypted body mismatch: got %q, want %q", decrypted.Body, plaintext)
	}

	// 5) The recipient must also be able to decrypt (recipient-part).
	decrypted2, err := directmsg.DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		storedBody,
	)
	if err != nil {
		t.Fatalf("recipient cannot decrypt stored message: %v", err)
	}
	if decrypted2.Body != plaintext {
		t.Fatalf("recipient decrypted body mismatch: got %q, want %q", decrypted2.Body, plaintext)
	}

	// 6) A third party must NOT be able to decrypt the stored message.
	thirdParty, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate third party: %v", err)
	}
	_, err = directmsg.DecryptForIdentity(
		thirdParty,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		storedBody,
	)
	if err == nil {
		t.Fatal("SECURITY: third party was able to decrypt the stored message!")
	}
}

// TestCtxReadersRespectCancellation verifies that the chatlog readers
// (Read, ListConversations, ReadLastEntry, ReadLastEntryPerPeer) return an
// error when the context is already cancelled, rather than proceeding with
// the SQLite query.
func TestCtxReadersRespectCancellation(t *testing.T) {
	s := storeFor(t, "abcdef0123456789abcdef0123456789abcdef01")

	// Insert a message so queries have data to return if they ignore ctx.
	_ = s.Append(context.Background(), "dm", domain.PeerIdentityFromWire("abcdef0123456789abcdef0123456789abcdef01"), Entry{
		ID:             "msg-1",
		Sender:         "peer-1",
		Recipient:      "abcdef0123456789abcdef0123456789abcdef01",
		Body:           "hello",
		CreatedAt:      time.Now().Format(time.RFC3339Nano),
		DeliveryStatus: StatusDelivered,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	if _, err := s.Read(ctx, "dm", domaintest.ID("peer-1")); err == nil {
		t.Fatal("Read should return error on cancelled context")
	}

	if _, err := s.ReadLast(ctx, "dm", domaintest.ID("peer-1"), 1); err == nil {
		t.Fatal("ReadLast should return error on cancelled context")
	}

	if _, err := s.ListConversations(ctx); err == nil {
		t.Fatal("ListConversations should return error on cancelled context")
	}

	if _, err := s.ReadLastEntry(ctx, "dm", domaintest.ID("peer-1")); err == nil {
		t.Fatal("ReadLastEntry should return error on cancelled context")
	}

	if _, err := s.ReadLastEntryPerPeer(ctx); err == nil {
		t.Fatal("ReadLastEntryPerPeer should return error on cancelled context")
	}
}

// TestLastIncomingAtIgnoresOwnMessages covers the presence question the
// sidebar asks: when did this peer last write to us? The thread's newest row
// is our own reply, so anything derived from "the last message" would report
// no peer activity at all.
func TestLastIncomingAtIgnoresOwnMessages(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peerReplied := "1111111111111111111111111111111111111111"
	peerSilent := "2222222222222222222222222222222222222222"
	ctx := context.Background()
	self := domain.PeerIdentityFromWire(selfAddr)

	// The ordinary shape: they wrote twice, we answered last.
	_ = s.Append(ctx, "dm", self, Entry{ID: "r1", Sender: peerReplied, Recipient: selfAddr, Body: "hi", CreatedAt: "2026-01-01T10:00:00Z"})
	_ = s.Append(ctx, "dm", self, Entry{ID: "r2", Sender: peerReplied, Recipient: selfAddr, Body: "you there?", CreatedAt: "2026-01-01T11:00:00Z"})
	_ = s.Append(ctx, "dm", self, Entry{ID: "r3", Sender: selfAddr, Recipient: peerReplied, Body: "yes", CreatedAt: "2026-01-02T09:00:00Z"})

	// A contact we wrote to who never answered has no evidence at all.
	_ = s.Append(ctx, "dm", self, Entry{ID: "s1", Sender: selfAddr, Recipient: peerSilent, Body: "hello?", CreatedAt: "2026-01-03T09:00:00Z"})

	wantReplied := time.Date(2026, time.January, 1, 11, 0, 0, 0, time.UTC)

	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	perPeer, err := s.LastIncomingAtPerPeer(ctx, now)
	if err != nil {
		t.Fatalf("last incoming per peer: %v", err)
	}
	if got, ok := perPeer[domain.PeerIdentityFromWire(peerReplied)]; !ok || !got.Equal(wantReplied) {
		t.Fatalf("per-peer last incoming = %v (present=%v), want %v", got, ok, wantReplied)
	}
	if got, ok := perPeer[domain.PeerIdentityFromWire(peerSilent)]; ok {
		t.Fatalf("outgoing-only conversation reported peer activity: %v", got)
	}

	got, err := s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(peerReplied), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.Equal(wantReplied) {
		t.Fatalf("single-peer last incoming = %v, want %v", got, wantReplied)
	}

	// MAX over no rows is a NULL row, not zero rows: the empty case must come
	// back as "no evidence", not as an error.
	silent, err := s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(peerSilent), now)
	if err != nil {
		t.Fatalf("last incoming for silent peer: %v", err)
	}
	if !silent.IsZero() {
		t.Fatalf("outgoing-only conversation reported %v, want zero", silent)
	}
}

// TestLastIncomingAtOrdersChronologically pins the comparison to real time
// rather than to string order. created_at is stored as RFC3339 text, and text
// order is not time order:
//
//   - "…00Z" sorts ABOVE "…00.5Z", because 'Z' > '.', so a whole second beats
//     the later half-second that follows it;
//   - a zone offset shifts the instant by hours while the text still sorts by
//     its printed digits, so "…23:00:00+03:00" (20:00Z) sorts above "…21:00Z"
//     which actually came later.
//
// Both forms reach the table: incoming rows carry the timestamp the sender
// printed, in whatever zone their node used.
func TestLastIncomingAtOrdersChronologically(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	subsecond := "1111111111111111111111111111111111111111"
	offset := "2222222222222222222222222222222222222222"
	ctx := context.Background()
	self := domain.PeerIdentityFromWire(selfAddr)

	appendIncoming := func(id, sender, createdAt string) {
		t.Helper()
		if err := s.Append(ctx, "dm", self, Entry{
			ID: id, Sender: sender, Recipient: selfAddr, Body: "sealed", CreatedAt: createdAt,
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	// Same second, the later one carrying a fraction.
	appendIncoming("s1", subsecond, "2026-01-01T10:00:00Z")
	appendIncoming("s2", subsecond, "2026-01-01T10:00:00.5Z")
	wantSubsecond := time.Date(2026, time.January, 1, 10, 0, 0, 500000000, time.UTC)

	// Different zones: 21:00Z is 22 minutes after 23:22:00+03:00.
	appendIncoming("o1", offset, "2026-01-01T23:22:00+03:00")
	appendIncoming("o2", offset, "2026-01-01T21:00:00Z")
	wantOffset := time.Date(2026, time.January, 1, 21, 0, 0, 0, time.UTC)

	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	perPeer, err := s.LastIncomingAtPerPeer(ctx, now)
	if err != nil {
		t.Fatalf("last incoming per peer: %v", err)
	}
	if got := perPeer[domain.PeerIdentityFromWire(subsecond)]; !got.Equal(wantSubsecond) {
		t.Fatalf("per-peer sub-second: got %v, want %v", got, wantSubsecond)
	}
	if got := perPeer[domain.PeerIdentityFromWire(offset)]; !got.Equal(wantOffset) {
		t.Fatalf("per-peer zone offset: got %v, want %v", got, wantOffset)
	}

	got, err := s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(subsecond), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.Equal(wantSubsecond) {
		t.Fatalf("single-peer sub-second: got %v, want %v", got, wantSubsecond)
	}
	got, err = s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(offset), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.Equal(wantOffset) {
		t.Fatalf("single-peer zone offset: got %v, want %v", got, wantOffset)
	}
}

// TestLastIncomingAtSkipsFutureRows covers the timestamp a sender can choose
// freely. A future-dated message must not become presence evidence — but it
// must also not HIDE the valid evidence behind it. Returning only the newest
// row and letting the caller reject it loses the older, honest message: the
// contact reads as "never seen" while its proof sits in the history.
func TestLastIncomingAtSkipsFutureRows(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	liar := "1111111111111111111111111111111111111111"
	onlyFuture := "2222222222222222222222222222222222222222"
	ctx := context.Background()
	self := domain.PeerIdentityFromWire(selfAddr)
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	appendIncoming := func(id, sender string, at time.Time) {
		t.Helper()
		if err := s.Append(ctx, "dm", self, Entry{
			ID: id, Sender: sender, Recipient: selfAddr, Body: "sealed",
			CreatedAt: at.Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	honest := now.Add(-4 * time.Hour)
	appendIncoming("l1", liar, honest)
	appendIncoming("l2", liar, now.Add(72*time.Hour))
	appendIncoming("f1", onlyFuture, now.Add(48*time.Hour))

	perPeer, err := s.LastIncomingAtPerPeer(ctx, now)
	if err != nil {
		t.Fatalf("last incoming per peer: %v", err)
	}
	if got := perPeer[domain.PeerIdentityFromWire(liar)]; !got.Equal(honest) {
		t.Fatalf("per-peer: got %v, want the honest message behind the forged one (%v)", got, honest)
	}
	if got, ok := perPeer[domain.PeerIdentityFromWire(onlyFuture)]; ok {
		t.Fatalf("a peer whose only message is future-dated reported evidence: %v", got)
	}

	got, err := s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(liar), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.Equal(honest) {
		t.Fatalf("single-peer: got %v, want %v", got, honest)
	}
	got, err = s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(onlyFuture), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("single-peer future-only: got %v, want zero", got)
	}
}

// TestLastIncomingAtKeepsNanosecondOrder pins the comparison to the full
// stored precision. julianday is a float: two timestamps inside the same
// microsecond collapse to one value, and whatever tie-breaks them next
// (rowid — insertion order, not time order) can hand back the older message.
// RFC3339Nano stores nanoseconds, so nanoseconds are what has to decide.
func TestLastIncomingAtKeepsNanosecondOrder(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"
	ctx := context.Background()
	self := domain.PeerIdentityFromWire(selfAddr)
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	base := time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)
	newest := base.Add(2 * time.Nanosecond)
	// The insertion order is fixed and deliberately the REVERSE of time
	// order, so a rowid tie-break has to pick the wrong row rather than
	// getting it right by luck. Ranging over a map here would randomise that
	// order and let the regression pass on roughly half the runs.
	ordered := []struct {
		id string
		at time.Time
	}{
		{id: "n2", at: newest},
		{id: "n1", at: base.Add(1 * time.Nanosecond)},
	}
	for _, entry := range ordered {
		if err := s.Append(ctx, "dm", self, Entry{
			ID: entry.id, Sender: peer, Recipient: selfAddr, Body: "sealed",
			CreatedAt: entry.at.Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("append %s: %v", entry.id, err)
		}
	}

	perPeer, err := s.LastIncomingAtPerPeer(ctx, now)
	if err != nil {
		t.Fatalf("last incoming per peer: %v", err)
	}
	if got := perPeer[domain.PeerIdentityFromWire(peer)]; !got.Equal(newest) {
		t.Fatalf("per-peer nanosecond order: got %v, want %v", got, newest)
	}
	got, err := s.LastIncomingAtFor(ctx, domain.PeerIdentityFromWire(peer), now)
	if err != nil {
		t.Fatalf("last incoming for: %v", err)
	}
	if !got.Equal(newest) {
		t.Fatalf("single-peer nanosecond order: got %v, want %v", got, newest)
	}
}

// TestUnseenIncomingIDsReportsOnlyUnseenIncoming pins the query that seeds the
// sidebar badge. It answers with IDS rather than a count because the consumer
// keeps a set: the database is ahead of the event stream, so the same message
// arrives from both, and only an id can be recognised as already counted.
func TestUnseenIncomingIDsReportsOnlyUnseenIncoming(t *testing.T) {
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := storeFor(t, selfAddr)

	peer := "1111111111111111111111111111111111111111"
	other := "2222222222222222222222222222222222222222"
	ctx := context.Background()
	self := domain.PeerIdentityFromWire(selfAddr)

	add := func(id, sender, recipient, status string) {
		t.Helper()
		if err := s.Append(ctx, "dm", self, Entry{
			ID: id, Sender: sender, Recipient: recipient, Body: "sealed",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), DeliveryStatus: status,
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	add("in-unseen", peer, selfAddr, StatusDelivered)
	add("in-seen", peer, selfAddr, StatusSeen)
	add("out-1", selfAddr, peer, StatusSent) // ours: never unread
	add("other-unseen", other, selfAddr, StatusDelivered)

	byPeer, err := s.UnseenIncomingIDs(ctx)
	if err != nil {
		t.Fatalf("unseen incoming ids: %v", err)
	}
	if got := byPeer[domain.PeerIdentityFromWire(peer)]; len(got) != 1 || got[0] != "in-unseen" {
		t.Fatalf("peer ids = %v, want only the unseen incoming one", got)
	}
	if got := byPeer[domain.PeerIdentityFromWire(other)]; len(got) != 1 {
		t.Fatalf("other peer ids = %v, want one", got)
	}
	if _, ours := byPeer[self]; ours {
		t.Fatal("our own outgoing message was reported as unread")
	}

	// The same question about ids the database does not hold: the header path
	// asks it to learn that a message's read state is not the header's to
	// decide.
	statuses, err := s.StoredMessageStatuses(ctx, []domain.MessageID{"in-seen", "in-unseen", "never-stored"})
	if err != nil {
		t.Fatalf("stored message statuses: %v", err)
	}
	if got := statuses["in-seen"]; got != StatusSeen {
		t.Fatalf("status of the read message = %q, want %q", got, StatusSeen)
	}
	if got, ok := statuses["in-unseen"]; !ok || got == StatusSeen {
		t.Fatalf("status of the unread message = %q (stored=%v), want a stored non-seen status", got, ok)
	}
	if _, ok := statuses["never-stored"]; ok {
		t.Fatal("an id the database never stored was reported as stored")
	}

	ids, err := s.UnseenIncomingIDsFor(ctx, domain.PeerIdentityFromWire(peer))
	if err != nil {
		t.Fatalf("unseen incoming ids for: %v", err)
	}
	if len(ids) != 1 || ids[0] != "in-unseen" {
		t.Fatalf("single-peer ids = %v, want only the unseen incoming one", ids)
	}

	// After the conversation is read, the badge query has nothing to report.
	if _, err := s.UpdateStatus(ctx, "dm", domain.PeerIdentityFromWire(peer), domain.MessageID("in-unseen"), StatusSeen); err != nil {
		t.Fatalf("mark seen: %v", err)
	}
	ids, err = s.UnseenIncomingIDsFor(ctx, domain.PeerIdentityFromWire(peer))
	if err != nil {
		t.Fatalf("unseen incoming ids for: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids after marking seen = %v, want none", ids)
	}
}
