package filetransfer

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestDeleteLocalCopyReceiverKeepsTheMapping is the whole point of the
// operation: the file goes, the mapping stays, and it stays in the one state
// a fresh download can start from — so the message keeps its attachment and
// the user can ask the peer for it again.
func TestDeleteLocalCopyReceiverKeepsTheMapping(t *testing.T) {
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	receivedDir := filepath.Join(downloadDir, "received")
	if err := os.MkdirAll(receivedDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	completed := filepath.Join(receivedDir, "photo.png")
	if err := os.WriteFile(completed, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	manager := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}
	manager.receiverMaps["file-1"] = &receiverFileMapping{
		FileID:        "file-1",
		FileHash:      "hash123",
		FileName:      "photo.png",
		FileSize:      10,
		BytesReceived: 10,
		NextOffset:    10,
		ServingEpoch:  7,
		Sender:        domaintest.ID("alice"),
		State:         receiverCompleted,
		CompletedPath: completed,
	}

	if err := manager.DeleteLocalCopy("file-1"); err != nil {
		t.Fatalf("DeleteLocalCopy: %v", err)
	}

	if _, err := os.Stat(completed); !os.IsNotExist(err) {
		t.Fatalf("the downloaded file is still on disk: %v", err)
	}
	mapping, ok := manager.receiverMaps["file-1"]
	if !ok {
		t.Fatal("the mapping was removed: the message would lose its attachment and could not be downloaded again")
	}
	if mapping.State != receiverAvailable {
		t.Fatalf("state = %s, want %s so a fresh download may start", mapping.State, receiverAvailable)
	}
	if mapping.CompletedPath != "" || mapping.BytesReceived != 0 || mapping.NextOffset != 0 || mapping.ServingEpoch != 0 {
		t.Fatalf("mapping was not reset for a new download: %+v", mapping)
	}
	if path := manager.ReceiverFilePath("file-1"); path != "" {
		t.Fatalf("ReceiverFilePath = %q, want empty: nothing is on disk any more", path)
	}
}

// TestDeleteLocalCopyErasesEvenWhenAnotherMessageHasTheSameName guards the
// ownership rule the erasure asks about: the file is unlinked because THIS
// mapping stopped pointing at it, and a different message's file with a
// different content is not touched.
func TestDeleteLocalCopyLeavesAnotherMessagesFileAlone(t *testing.T) {
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	receivedDir := filepath.Join(downloadDir, "received")
	if err := os.MkdirAll(receivedDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	shared := filepath.Join(receivedDir, "photo.png")
	if err := os.WriteFile(shared, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	manager := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}
	for _, id := range []domain.FileID{"file-1", "file-2"} {
		manager.receiverMaps[id] = &receiverFileMapping{
			FileID:        id,
			FileHash:      "hash123",
			FileName:      "photo.png",
			Sender:        domaintest.ID("alice"),
			State:         receiverCompleted,
			CompletedPath: shared,
		}
	}

	if err := manager.DeleteLocalCopy("file-1"); err != nil {
		t.Fatalf("DeleteLocalCopy: %v", err)
	}

	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("the file another message still shows was erased: %v", err)
	}
	if state := manager.receiverMaps["file-1"].State; state != receiverAvailable {
		t.Fatalf("state = %s, want %s", state, receiverAvailable)
	}
	if state := manager.receiverMaps["file-2"].State; state != receiverCompleted {
		t.Fatalf("the other message's mapping moved: %s", state)
	}
}

// TestDeleteLocalCopyRefusesAnOutgoingFile: what this node holds for a file
// it sent is the transmit blob every re-download is served from — shared
// between messages with the same content and impossible to get back — so the
// operation refuses rather than half-deleting it.
func TestDeleteLocalCopyRefusesAnOutgoingFile(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	if err := os.MkdirAll(transmitDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hash := "aabbccdd11223344556677889900aabb11223344556677889900aabbccddeeff"
	blob := filepath.Join(transmitDir, hash+".png")
	if err := os.WriteFile(blob, []byte("image data"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	store.mu.Lock()
	store.refs[hash] = 1
	store.mu.Unlock()

	manager := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		stopCh:       make(chan struct{}),
	}
	manager.senderMaps["file-1"] = &senderFileMapping{
		FileID:    "file-1",
		FileHash:  hash,
		FileName:  "photo.png",
		FileSize:  10,
		Recipient: domaintest.ID("bob"),
		State:     senderCompleted,
		CreatedAt: time.Now(),
	}

	if err := manager.DeleteLocalCopy("file-1"); !errors.Is(err, ErrOutgoingCopy) {
		t.Fatalf("err = %v, want ErrOutgoingCopy", err)
	}

	if _, err := os.Stat(blob); err != nil {
		t.Fatalf("the transmit blob was erased: %v", err)
	}
	mapping, ok := manager.senderMaps["file-1"]
	if !ok {
		t.Fatal("the sender mapping was removed")
	}
	if mapping.State != senderCompleted {
		t.Fatalf("state = %s, want it untouched (%s)", mapping.State, senderCompleted)
	}
	store.mu.Lock()
	refs := store.refs[hash]
	store.mu.Unlock()
	if refs != 1 {
		t.Fatalf("blob refs = %d, want 1: a refused delete must not release anything", refs)
	}
	if path := manager.SenderFilePath("file-1"); path == "" {
		t.Fatal("SenderFilePath is empty: the file can no longer be served")
	}
}

// TestDeleteLocalCopyWithNothingOnDisk: a transfer that never wrote a file
// reports the sentinel rather than a failure, because for the user "there is
// nothing here" and "it is gone now" are the same outcome.
func TestDeleteLocalCopyWithNothingOnDisk(t *testing.T) {
	manager := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  t.TempDir(),
		stopCh:       make(chan struct{}),
	}
	manager.receiverMaps["file-1"] = &receiverFileMapping{
		FileID: "file-1",
		State:  receiverDownloading,
		Sender: domaintest.ID("alice"),
	}

	if err := manager.DeleteLocalCopy("file-1"); !errors.Is(err, ErrNoLocalCopy) {
		t.Fatalf("err = %v, want ErrNoLocalCopy", err)
	}
	if err := manager.DeleteLocalCopy("no-such-file"); !errors.Is(err, ErrNoLocalCopy) {
		t.Fatalf("err = %v, want ErrNoLocalCopy", err)
	}
	if state := manager.receiverMaps["file-1"].State; state != receiverDownloading {
		t.Fatalf("a download in flight was disturbed: %s", state)
	}
}
