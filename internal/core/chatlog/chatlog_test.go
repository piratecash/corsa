package chatlog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/directmsg"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
)

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "abcdef0123456789abcdef0123456789abcdef01", ":64646")
	defer func() { _ = s.Close() }()

	if s.db == nil {
		t.Fatal("expected db to be non-nil")
	}
	if s.identityAddr != "abcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("unexpected identityAddr: %s", s.identityAddr)
	}

	// Verify database file is created with expected name.
	expected := filepath.Join(dir, "chatlog-abcdef01-64646.db")
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("expected db file at %s: %v", expected, err)
	}
}

func TestAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

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

	if err := s.Append("dm", selfAddr, entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, err := s.Read("dm", peerAddr)
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

func TestAppendMultipleMessages(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

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
		if err := s.Append("dm", selfAddr, entry); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	entries, err := s.Read("dm", peerAddr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
}

func TestReadLastN(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

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
		_ = s.Append("dm", selfAddr, entry)
	}

	last3, err := s.ReadLast("dm", peerAddr, 3)
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
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	_ = s.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer1, Recipient: selfAddr, Body: "from peer1", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	_ = s.Append("dm", selfAddr, Entry{ID: "m2", Sender: peer2, Recipient: selfAddr, Body: "from peer2", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	entries1, _ := s.Read("dm", peer1)
	entries2, _ := s.Read("dm", peer2)

	if len(entries1) != 1 || entries1[0].ID != "m1" {
		t.Fatalf("peer1 entries wrong: %v", entries1)
	}
	if len(entries2) != 1 || entries2[0].ID != "m2" {
		t.Fatalf("peer2 entries wrong: %v", entries2)
	}
}

func TestGlobalMessages(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"

	_ = s.Append("global", selfAddr, Entry{ID: "g1", Sender: selfAddr, Recipient: "*", Body: "broadcast", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	entries, err := s.Read("global", "")
	if err != nil {
		t.Fatalf("read global: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "g1" {
		t.Fatalf("global entries wrong: %v", entries)
	}
}

func TestListConversations(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	t1 := time.Now().UTC().Add(-time.Hour)
	t2 := time.Now().UTC()

	_ = s.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer1, Recipient: selfAddr, Body: "old", CreatedAt: t1.Format(time.RFC3339Nano)})
	_ = s.Append("dm", selfAddr, Entry{ID: "m2", Sender: peer2, Recipient: selfAddr, Body: "new", CreatedAt: t2.Format(time.RFC3339Nano)})

	convs, err := s.ListConversations()
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
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s.Append("dm", selfAddr, Entry{ID: "msg-abc", Sender: peer, Recipient: selfAddr, Body: "test", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	if !s.HasEntryID("dm", peer, "msg-abc") {
		t.Fatal("expected HasEntryID to return true")
	}
	if s.HasEntryID("dm", peer, "msg-xyz") {
		t.Fatal("expected HasEntryID to return false for non-existent ID")
	}
}

func TestReadEmptyStore(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	entries, err := s.Read("dm", "1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if entries != nil {
		t.Fatalf("expected nil entries for empty store, got %v", entries)
	}
}

func TestPortIsolation(t *testing.T) {
	dir := t.TempDir()
	s1 := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s1.Close() }()
	s2 := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":8888")
	defer func() { _ = s2.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s1.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "port9999", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	_ = s2.Append("dm", selfAddr, Entry{ID: "m2", Sender: peer, Recipient: selfAddr, Body: "port8888", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})

	e1, _ := s1.Read("dm", peer)
	e2, _ := s2.Read("dm", peer)

	if len(e1) != 1 || e1[0].Body != "port9999" {
		t.Fatalf("port 9999 entries wrong")
	}
	if len(e2) != 1 || e2[0].Body != "port8888" {
		t.Fatalf("port 8888 entries wrong")
	}
}

func TestAppendCreatesDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "deep", "nested", "chatlog")

	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	if s.db == nil {
		t.Fatal("expected db to be non-nil after creating nested directory")
	}

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1111111111111111111111111111111111111111"

	entry := Entry{
		ID:        "msg-autodir",
		Sender:    peerAddr,
		Recipient: selfAddr,
		Body:      "should create dir",
		CreatedAt: "2026-03-24T00:00:00Z",
	}

	if err := s.Append("dm", selfAddr, entry); err != nil {
		t.Fatalf("Append should work after auto-create directory, got: %v", err)
	}

	entries, err := s.Read("dm", peerAddr)
	if err != nil {
		t.Fatalf("read after auto-create: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "msg-autodir" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestDuplicateInsertIgnored(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	entry := Entry{
		ID:        "dup-1",
		Sender:    peer,
		Recipient: selfAddr,
		Body:      "first",
		CreatedAt: "2026-01-01T00:00:00Z",
	}

	if err := s.Append("dm", selfAddr, entry); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Second append with same ID should be silently ignored (INSERT OR IGNORE).
	entry.Body = "duplicate"
	if err := s.Append("dm", selfAddr, entry); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after duplicate, got %d", len(entries))
	}
	if entries[0].Body != "first" {
		t.Fatalf("expected original body, got %s", entries[0].Body)
	}
}

// --- Delivery status tests ---

func TestAppendSetsDeliveryStatus(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	err := s.Append("dm", selfAddr, Entry{
		ID: "msg-s1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("expected status=%q, got %q", StatusSent, entries[0].DeliveryStatus)
	}
}

func TestAppendDefaultsToSentStatus(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	// Append without setting DeliveryStatus — should default to "sent".
	err := s.Append("dm", selfAddr, Entry{
		ID: "msg-default", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("expected default status=%q, got %q", StatusSent, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatus(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "msg-1", Sender: selfAddr, Recipient: peer,
		Body: "first", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "msg-2", Sender: peer, Recipient: selfAddr,
		Body: "second", CreatedAt: "2026-01-01T00:01:00Z", DeliveryStatus: StatusDelivered,
	})

	updated, err := s.UpdateStatus("dm", peer, "msg-1", StatusDelivered)
	if err != nil {
		t.Fatalf("update status: %v", err)
	}
	if !updated {
		t.Fatal("expected update to return true")
	}

	entries, _ := s.Read("dm", peer)
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
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "msg-1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})

	updated, err := s.UpdateStatus("dm", peer, "nonexistent", StatusDelivered)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updated {
		t.Fatal("expected false for missing message ID")
	}
}

func TestUpdateStatusToSeen(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "msg-1", Sender: peer, Recipient: selfAddr,
		Body: "incoming", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered,
	})

	updated, _ := s.UpdateStatus("dm", peer, "msg-1", StatusSeen)
	if !updated {
		t.Fatal("expected update to return true")
	}

	entries, _ := s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("expected %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatusMonotonic(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	// Start at "sent".
	_ = s.Append("dm", selfAddr, Entry{
		ID: "mono-1", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSent,
	})

	// Advance sent → delivered: should succeed.
	ok, err := s.UpdateStatus("dm", peer, "mono-1", StatusDelivered)
	if err != nil {
		t.Fatalf("sent→delivered error: %v", err)
	}
	if !ok {
		t.Fatal("sent→delivered should return true")
	}

	// Advance delivered → seen: should succeed.
	ok, _ = s.UpdateStatus("dm", peer, "mono-1", StatusSeen)
	if !ok {
		t.Fatal("delivered→seen should return true")
	}

	// Attempt regression seen → delivered: should be silently rejected.
	ok, err = s.UpdateStatus("dm", peer, "mono-1", StatusDelivered)
	if err != nil {
		t.Fatalf("seen→delivered error: %v", err)
	}
	if ok {
		t.Fatal("seen→delivered should return false (regression)")
	}

	// Attempt regression seen → sent: should also be rejected.
	ok, _ = s.UpdateStatus("dm", peer, "mono-1", StatusSent)
	if ok {
		t.Fatal("seen→sent should return false (regression)")
	}

	// Verify status is still "seen".
	entries, _ := s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("status should still be %q after regression attempts, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

func TestUpdateStatusDeliveredCannotRegressToSent(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "mono-2", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered,
	})

	ok, _ := s.UpdateStatus("dm", peer, "mono-2", StatusSent)
	if ok {
		t.Fatal("delivered→sent should return false")
	}

	entries, _ := s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("expected %q, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}
}

func TestListConversationsIncludesUnreadCount(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peerA := "1111111111111111111111111111111111111111"
	peerB := "2222222222222222222222222222222222222222"

	// peerA: 2 incoming delivered (unread), 1 outgoing sent.
	_ = s.Append("dm", selfAddr, Entry{ID: "a1", Sender: peerA, Recipient: selfAddr, Body: "hi", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered})
	_ = s.Append("dm", selfAddr, Entry{ID: "a2", Sender: peerA, Recipient: selfAddr, Body: "hey", CreatedAt: "2026-01-01T00:01:00Z", DeliveryStatus: StatusDelivered})
	_ = s.Append("dm", selfAddr, Entry{ID: "a3", Sender: selfAddr, Recipient: peerA, Body: "yo", CreatedAt: "2026-01-01T00:02:00Z", DeliveryStatus: StatusSent})

	// peerB: 1 incoming seen (read).
	_ = s.Append("dm", selfAddr, Entry{ID: "b1", Sender: peerB, Recipient: selfAddr, Body: "hello", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusSeen})

	convs, err := s.ListConversations()
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peerOld := "1111111111111111111111111111111111111111"
	peerNew := "2222222222222222222222222222222222222222"

	// peerOld: old message, but unread.
	_ = s.Append("dm", selfAddr, Entry{ID: "o1", Sender: peerOld, Recipient: selfAddr, Body: "old", CreatedAt: "2026-01-01T00:00:00Z", DeliveryStatus: StatusDelivered})

	// peerNew: newer message, but already seen.
	_ = s.Append("dm", selfAddr, Entry{ID: "n1", Sender: peerNew, Recipient: selfAddr, Body: "new", CreatedAt: "2026-01-02T00:00:00Z", DeliveryStatus: StatusSeen})

	convs, _ := s.ListConversations()
	// peerOld should be first despite older timestamp because it has unread.
	if convs[0].PeerAddress != peerOld {
		t.Fatalf("expected unread conversation first, got %s", convs[0].PeerAddress)
	}
}

func TestReadLastEntry(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "first", CreatedAt: "2026-01-01T00:00:00Z"})
	_ = s.Append("dm", selfAddr, Entry{ID: "m2", Sender: selfAddr, Recipient: peer, Body: "second", CreatedAt: "2026-01-01T00:01:00Z"})

	entry, err := s.ReadLastEntry("dm", peer)
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
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	entry, err := s.ReadLastEntry("dm", "1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry != nil {
		t.Fatalf("expected nil entry for empty conversation, got %+v", entry)
	}
}

func TestReadLastEntryPerPeer(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer1 := "1111111111111111111111111111111111111111"
	peer2 := "2222222222222222222222222222222222222222"

	_ = s.Append("dm", selfAddr, Entry{ID: "p1-old", Sender: peer1, Recipient: selfAddr, Body: "old", CreatedAt: "2026-01-01T00:00:00Z"})
	_ = s.Append("dm", selfAddr, Entry{ID: "p1-new", Sender: selfAddr, Recipient: peer1, Body: "new", CreatedAt: "2026-01-01T00:01:00Z"})
	_ = s.Append("dm", selfAddr, Entry{ID: "p2-only", Sender: peer2, Recipient: selfAddr, Body: "only", CreatedAt: "2026-01-01T00:00:30Z"})

	result, err := s.ReadLastEntryPerPeer()
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1111111111111111111111111111111111111111"
	sameTS := "2026-03-24T12:00:00Z"

	// Insert two messages with identical created_at for the same peer.
	// The second insert has a higher rowid and should be the one returned.
	_ = s.Append("dm", selfAddr, Entry{
		ID: "dup-ts-first", Sender: selfAddr, Recipient: peer,
		Body: "first insert", CreatedAt: sameTS,
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "dup-ts-second", Sender: selfAddr, Recipient: peer,
		Body: "second insert", CreatedAt: sameTS,
	})

	result, err := s.ReadLastEntryPerPeer()
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1111111111111111111111111111111111111111"
	sameTS := "2026-03-24T12:00:00Z"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "same-ts-a", Sender: selfAddr, Recipient: peer,
		Body: "a", CreatedAt: sameTS,
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "same-ts-b", Sender: selfAddr, Recipient: peer,
		Body: "b", CreatedAt: sameTS,
	})

	entry, err := s.ReadLastEntry("dm", peer)
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

func TestIntegrityCheckRecovery(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"

	// Create a valid store and write some data.
	s := NewStore(dir, selfAddr, ":9999")
	_ = s.Append("dm", selfAddr, Entry{
		ID: "m1", Sender: "1111111111111111111111111111111111111111",
		Recipient: selfAddr, Body: "test", CreatedAt: "2026-01-01T00:00:00Z",
	})
	_ = s.Close()

	// Corrupt the database file.
	dbPath := filepath.Join(dir, "chatlog-aabbccdd-9999.db")
	if err := os.WriteFile(dbPath, []byte("this is not a valid sqlite file"), 0600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	// NewStore should detect corruption, rename and create fresh DB.
	s2 := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s2.Close() }()

	if s2.db == nil {
		t.Fatal("expected db to be non-nil after recovery")
	}

	// The corrupt file should be renamed.
	if _, err := os.Stat(dbPath + ".corrupt"); err != nil {
		t.Fatalf("expected corrupt file backup: %v", err)
	}

	// Fresh database should be empty.
	entries, _ := s2.Read("dm", "1111111111111111111111111111111111111111")
	if entries != nil {
		t.Fatalf("expected empty store after recovery, got %d entries", len(entries))
	}
}

func TestFlagCheckConstraint(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	// All valid flags should be accepted.
	validFlags := []string{FlagNone, FlagImmutable, FlagSenderDelete, FlagAnyDelete, FlagAutoDeleteTTL}
	for i, flag := range validFlags {
		err := s.Append("dm", selfAddr, Entry{
			ID: fmt.Sprintf("flag-%d", i), Sender: selfAddr, Recipient: peer,
			Body: "test", CreatedAt: fmt.Sprintf("2026-01-01T00:%02d:00Z", i), Flag: flag,
		})
		if err != nil {
			t.Fatalf("valid flag %q rejected: %v", flag, err)
		}
	}

	// Invalid flag: INSERT OR IGNORE silently ignores CHECK violations,
	// so the row should not be inserted (no error, but no row either).
	err := s.Append("dm", selfAddr, Entry{
		ID: "flag-bad", Sender: selfAddr, Recipient: peer,
		Body: "test", CreatedAt: "2026-01-01T01:00:00Z", Flag: "invalid-flag",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.HasEntryID("dm", peer, "flag-bad") {
		t.Fatal("invalid flag should not have been inserted")
	}
}

func TestDeliveryStatusCheckConstraint(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	// All valid statuses should be accepted.
	for i, status := range []string{StatusSent, StatusDelivered, StatusSeen} {
		err := s.Append("dm", selfAddr, Entry{
			ID: fmt.Sprintf("status-%d", i), Sender: selfAddr, Recipient: peer,
			Body: "test", CreatedAt: fmt.Sprintf("2026-01-01T00:%02d:00Z", i), DeliveryStatus: status,
		})
		if err != nil {
			t.Fatalf("valid status %q rejected: %v", status, err)
		}
	}

	// Invalid delivery_status via direct UPDATE should fail.
	_, err := s.UpdateStatus("dm", peer, "status-0", "invalid-status")
	if err == nil {
		t.Fatal("expected error for invalid delivery_status, got nil")
	}
}

func TestTTLSecondsStored(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	err := s.Append("dm", selfAddr, Entry{
		ID: "ttl-1", Sender: selfAddr, Recipient: peer,
		Body: "ephemeral", CreatedAt: "2026-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 3600,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
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
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	defer func() { _ = s.Close() }()

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	meta := `{"reply_to":"msg-abc","edited":true}`
	err := s.Append("dm", selfAddr, Entry{
		ID: "meta-1", Sender: selfAddr, Recipient: peer,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
		Metadata: meta,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
	if entries[0].Metadata != meta {
		t.Fatalf("expected metadata=%q, got %q", meta, entries[0].Metadata)
	}
}

func TestDeleteExpired(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1111111111111111111111111111111111111111"

	// Message with TTL=1 second created 2 hours ago — should be expired.
	_ = s.Append("dm", selfAddr, Entry{
		ID: "expired-1", Sender: selfAddr, Recipient: peer,
		Body: "old ephemeral", CreatedAt: "2020-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 1,
	})

	// Message with TTL=999999 seconds created recently — should survive.
	_ = s.Append("dm", selfAddr, Entry{
		ID: "alive-1", Sender: selfAddr, Recipient: peer,
		Body: "still alive", CreatedAt: "2099-01-01T00:00:00Z",
		Flag: FlagAutoDeleteTTL, TTLSeconds: 999999,
	})

	// Regular message (no TTL) — should survive.
	_ = s.Append("dm", selfAddr, Entry{
		ID: "normal-1", Sender: selfAddr, Recipient: peer,
		Body: "permanent", CreatedAt: "2020-01-01T00:00:00Z",
	})

	deleted, err := s.DeleteExpired()
	if err != nil {
		t.Fatalf("delete expired: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	entries, _ := s.Read("dm", peer)
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1111111111111111111111111111111111111111"

	_ = s.Append("dm", selfAddr, Entry{
		ID: "del-1", Sender: selfAddr, Recipient: peer,
		Body: "to delete", CreatedAt: "2026-01-01T00:00:00Z",
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "keep-1", Sender: selfAddr, Recipient: peer,
		Body: "to keep", CreatedAt: "2026-01-01T00:01:00Z",
	})

	ok, err := s.DeleteByID("del-1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !ok {
		t.Fatal("expected true for existing message")
	}

	// Deleting non-existent returns false.
	ok, _ = s.DeleteByID("nonexistent")
	if ok {
		t.Fatal("expected false for non-existent message")
	}

	entries, _ := s.Read("dm", peer)
	if len(entries) != 1 || entries[0].ID != "keep-1" {
		t.Fatalf("unexpected entries after delete: %+v", entries)
	}
}

func TestDeleteByPeer(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	identityA := domain.PeerIdentity("1111111111111111111111111111111111111111")
	identityB := domain.PeerIdentity("2222222222222222222222222222222222222222")

	// Messages with identityA (both directions).
	_ = s.Append("dm", selfAddr, Entry{
		ID: "a-out-1", Sender: selfAddr, Recipient: string(identityA),
		Body: "outgoing to A", CreatedAt: "2026-01-01T00:00:00Z",
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "a-in-1", Sender: string(identityA), Recipient: selfAddr,
		Body: "incoming from A", CreatedAt: "2026-01-01T00:01:00Z",
	})

	// Messages with identityB — must survive deletion of identityA.
	_ = s.Append("dm", selfAddr, Entry{
		ID: "b-out-1", Sender: selfAddr, Recipient: string(identityB),
		Body: "outgoing to B", CreatedAt: "2026-01-01T00:02:00Z",
	})

	n, err := s.DeleteByPeer(identityA)
	if err != nil {
		t.Fatalf("delete identity: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", n)
	}

	// identityA conversation should be empty.
	entries, _ := s.Read("dm", string(identityA))
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for identityA, got %d", len(entries))
	}

	// identityB conversation should be intact.
	entries, _ = s.Read("dm", string(identityB))
	if len(entries) != 1 || entries[0].ID != "b-out-1" {
		t.Fatalf("identityB entries unexpected: %+v", entries)
	}

	// Deleting already-empty identity returns 0.
	n, err = s.DeleteByPeer(identityA)
	if err != nil {
		t.Fatalf("delete empty: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 deleted, got %d", n)
	}

	// Empty identity returns error.
	_, err = s.DeleteByPeer("")
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1122334455667788112233445566778811223344"

	// Step 1: Outgoing message appended with "sent".
	err := s.Append("dm", selfAddr, Entry{
		ID: "flow-1", Sender: selfAddr, Recipient: peer,
		Body: "encrypted-body", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSent {
		t.Fatalf("step1: expected %q, got %q", StatusSent, entries[0].DeliveryStatus)
	}

	// Step 2: Receipt arrives — update to "delivered".
	ok, err := s.UpdateStatus("dm", peer, "flow-1", StatusDelivered)
	if err != nil {
		t.Fatalf("update to delivered: %v", err)
	}
	if !ok {
		t.Fatal("step2: expected update to return true")
	}

	entries, _ = s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusDelivered {
		t.Fatalf("step2: expected %q, got %q", StatusDelivered, entries[0].DeliveryStatus)
	}

	// Step 3: Seen receipt arrives — update to "seen".
	ok, _ = s.UpdateStatus("dm", peer, "flow-1", StatusSeen)
	if !ok {
		t.Fatal("step3: expected update to return true")
	}

	entries, _ = s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("step3: expected %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}

	// Step 4: Late "delivered" receipt — must be rejected (monotonic).
	ok, _ = s.UpdateStatus("dm", peer, "flow-1", StatusDelivered)
	if ok {
		t.Fatal("step4: late delivered should be rejected after seen")
	}

	entries, _ = s.Read("dm", peer)
	if entries[0].DeliveryStatus != StatusSeen {
		t.Fatalf("step4: status should still be %q, got %q", StatusSeen, entries[0].DeliveryStatus)
	}
}

// TestStatusSurvivesStoreReopen verifies that delivery_status persisted in
// SQLite survives closing and reopening the store (simulating a node restart).
func TestStatusSurvivesStoreReopen(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1122334455667788112233445566778811223344"

	// Open store, write message, update status, close.
	s1 := NewStore(dir, selfAddr, ":9999")
	_ = s1.Append("dm", selfAddr, Entry{
		ID: "persist-1", Sender: selfAddr, Recipient: peer,
		Body: "encrypted", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusSent,
	})
	_, _ = s1.UpdateStatus("dm", peer, "persist-1", StatusDelivered)
	_ = s1.Close()

	// Reopen store — status should survive.
	s2 := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s2.Close() }()

	entries, err := s2.Read("dm", peer)
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
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1122334455667788112233445566778811223344"

	// Two incoming messages with "delivered" status (unread).
	_ = s.Append("dm", selfAddr, Entry{
		ID: "unread-1", Sender: peer, Recipient: selfAddr,
		Body: "hello", CreatedAt: "2026-01-01T00:00:00Z",
		DeliveryStatus: StatusDelivered,
	})
	_ = s.Append("dm", selfAddr, Entry{
		ID: "unread-2", Sender: peer, Recipient: selfAddr,
		Body: "world", CreatedAt: "2026-01-01T00:01:00Z",
		DeliveryStatus: StatusDelivered,
	})

	convs, _ := s.ListConversations()
	if len(convs) != 1 {
		t.Fatalf("expected 1 conversation, got %d", len(convs))
	}
	if convs[0].UnreadCount != 2 {
		t.Fatalf("expected 2 unread before seen, got %d", convs[0].UnreadCount)
	}

	// Mark first message as seen.
	if _, err := s.UpdateStatus("dm", peer, "unread-1", StatusSeen); err != nil {
		t.Fatalf("update status unread-1: %v", err)
	}

	convs, _ = s.ListConversations()
	if convs[0].UnreadCount != 1 {
		t.Fatalf("expected 1 unread after marking one seen, got %d", convs[0].UnreadCount)
	}

	// Mark second message as seen.
	if _, err := s.UpdateStatus("dm", peer, "unread-2", StatusSeen); err != nil {
		t.Fatalf("update status unread-2: %v", err)
	}

	convs, _ = s.ListConversations()
	if convs[0].UnreadCount != 0 {
		t.Fatalf("expected 0 unread after marking all seen, got %d", convs[0].UnreadCount)
	}
}

// TestMessageBodyStoredAsIs verifies that the body stored in SQLite is the
// exact ciphertext passed to Append — no transformation, no cleartext leak.
// In a real scenario, outgoing DMs are encrypted with the sender's own key
// before storage. This test ensures the chatlog layer doesn't alter the body.
func TestMessageBodyStoredAsIs(t *testing.T) {
	dir := t.TempDir()
	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	s := NewStore(dir, selfAddr, ":9999")
	defer func() { _ = s.Close() }()

	peer := "1122334455667788112233445566778811223344"

	// Simulate encrypted body (base64-encoded ciphertext).
	ciphertext := "U2VhbGVkQm94eyJub25jZSI6IjEyMyIsImNpcGhlcnRleHQiOiJhYmMifQ=="

	_ = s.Append("dm", selfAddr, Entry{
		ID: "enc-1", Sender: selfAddr, Recipient: peer,
		Body: ciphertext, CreatedAt: "2026-01-01T00:00:00Z",
	})

	entries, _ := s.Read("dm", peer)
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
	dir := t.TempDir()

	// Generate two identities: sender and recipient.
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender identity: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient identity: %v", err)
	}

	s := NewStore(dir, sender.Address, ":9999")
	defer func() { _ = s.Close() }()

	plaintext := "this is a secret message that must never appear in cleartext"

	// Encrypt via the real directmsg envelope (X25519 + AES-256-GCM + ed25519 signature).
	ciphertext, err := directmsg.EncryptForParticipants(
		sender,
		recipient.Address,
		identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		plaintext,
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Store the encrypted envelope in the chatlog (same path as SendDirectMessage).
	err = s.Append("dm", sender.Address, Entry{
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
	entries, err := s.Read("dm", recipient.Address)
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

func TestNilDBOperationsAreSafe(t *testing.T) {
	// A store with nil db (e.g. failed to open) should not panic.
	s := &Store{identityAddr: "aabbccdd11223344aabbccdd11223344aabbccdd"}

	if err := s.Append("dm", s.identityAddr, Entry{ID: "x"}); err == nil {
		t.Fatal("expected error from nil db Append")
	}

	entries, err := s.Read("dm", "peer")
	if err != nil || entries != nil {
		t.Fatalf("expected nil/nil from nil db Read, got %v/%v", entries, err)
	}

	convs, err := s.ListConversations()
	if err != nil || convs != nil {
		t.Fatalf("expected nil/nil from nil db ListConversations")
	}

	if s.HasEntryID("dm", "peer", "x") {
		t.Fatal("expected false from nil db HasEntryID")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("expected nil error from nil db Close, got %v", err)
	}
}

// TestCtxReadersRespectCancellation verifies that the context-aware
// chatlog readers (ReadCtx, ListConversationsCtx, ReadLastEntryCtx,
// ReadLastEntryPerPeerCtx) return an error when the context is already
// cancelled, rather than proceeding with the SQLite query.
func TestCtxReadersRespectCancellation(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "abcdef0123456789abcdef0123456789abcdef01", ":64646")
	defer func() { _ = s.Close() }()

	// Insert a message so queries have data to return if they ignore ctx.
	_ = s.Append("dm", "abcdef0123456789abcdef0123456789abcdef01", Entry{
		ID:             "msg-1",
		Sender:         "peer-1",
		Recipient:      "abcdef0123456789abcdef0123456789abcdef01",
		Body:           "hello",
		CreatedAt:      time.Now().Format(time.RFC3339Nano),
		DeliveryStatus: StatusDelivered,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	if _, err := s.ReadCtx(ctx, "dm", "peer-1"); err == nil {
		t.Fatal("ReadCtx should return error on cancelled context")
	}

	if _, err := s.ReadLastCtx(ctx, "dm", "peer-1", 1); err == nil {
		t.Fatal("ReadLastCtx should return error on cancelled context")
	}

	if _, err := s.ListConversationsCtx(ctx); err == nil {
		t.Fatal("ListConversationsCtx should return error on cancelled context")
	}

	if _, err := s.ReadLastEntryCtx(ctx, "dm", "peer-1"); err == nil {
		t.Fatal("ReadLastEntryCtx should return error on cancelled context")
	}

	if _, err := s.ReadLastEntryPerPeerCtx(ctx); err == nil {
		t.Fatal("ReadLastEntryPerPeerCtx should return error on cancelled context")
	}
}
