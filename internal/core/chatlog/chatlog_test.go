package chatlog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	s := NewStore("/tmp/test", "abcdef0123456789abcdef0123456789abcdef01", ":64646")
	if s.identityShort != "abcdef01" {
		t.Fatalf("expected identityShort=abcdef01, got %s", s.identityShort)
	}
	if s.port != "64646" {
		t.Fatalf("expected port=64646, got %s", s.port)
	}
}

func TestAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

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
}

func TestAppendMultipleMessages(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1122334455667788112233445566778811223344"

	for i := 0; i < 5; i++ {
		entry := Entry{
			ID:        "msg-" + string(rune('A'+i)),
			Sender:    selfAddr,
			Recipient: peerAddr,
			Body:      "body",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
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

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peerAddr := "1122334455667788112233445566778811223344"

	for i := 0; i < 10; i++ {
		entry := Entry{
			ID:        "msg-" + string(rune('0'+i)),
			Sender:    selfAddr,
			Recipient: peerAddr,
			Body:      "body",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
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
	// Last 3 should be entries 7,8,9
	if last3[0].ID != "msg-7" {
		t.Fatalf("expected msg-7, got %s", last3[0].ID)
	}
}

func TestSeparateFilesPerPeer(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

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
	// Should be sorted by last message, newest first
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

func TestReadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

	entries, err := s.Read("dm", "1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if entries != nil {
		t.Fatalf("expected nil entries for non-existent file, got %v", entries)
	}
}

func TestFileFormat(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	_ = s.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "hello", CreatedAt: "2026-01-01T00:00:00Z"})

	// Read raw file to verify JSONL format
	path := filepath.Join(dir, "dm-aabbccdd-"+peer+"-9999.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	var e Entry
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatalf("unmarshal line: %v", err)
	}
	if e.ID != "m1" || e.Body != "hello" {
		t.Fatalf("unexpected entry: %+v", e)
	}
}

func TestPortIsolation(t *testing.T) {
	dir := t.TempDir()
	s1 := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")
	s2 := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":8888")

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
	// When CORSA_CHATLOG_DIR points to a non-existent directory,
	// Append should create it automatically instead of failing.
	base := t.TempDir()
	dir := filepath.Join(base, "deep", "nested", "chatlog")

	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

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
		t.Fatalf("Append should auto-create directory, got: %v", err)
	}

	// Verify the directory was created
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected a directory")
	}

	// Verify the entry is readable
	entries, err := s.Read("dm", peerAddr)
	if err != nil {
		t.Fatalf("read after auto-create: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "msg-autodir" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestMalformedLinesSkipped(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "aabbccdd11223344aabbccdd11223344aabbccdd", ":9999")

	selfAddr := "aabbccdd11223344aabbccdd11223344aabbccdd"
	peer := "1111111111111111111111111111111111111111"

	// Write a valid entry
	_ = s.Append("dm", selfAddr, Entry{ID: "m1", Sender: peer, Recipient: selfAddr, Body: "good", CreatedAt: "2026-01-01T00:00:00Z"})

	// Append a malformed line directly to file
	path := filepath.Join(dir, "dm-aabbccdd-"+peer+"-9999.jsonl")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = f.WriteString("this is not json\n")
	_ = f.Close()

	// Append another valid entry
	_ = s.Append("dm", selfAddr, Entry{ID: "m2", Sender: peer, Recipient: selfAddr, Body: "also good", CreatedAt: "2026-01-02T00:00:00Z"})

	entries, err := s.Read("dm", peer)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 valid entries (malformed skipped), got %d", len(entries))
	}
	if entries[0].ID != "m1" || entries[1].ID != "m2" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}
