package filetransfer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestCleanupPeerTransfersSenderReleasesRef verifies that cleaning up a peer
// removes sender mappings and releases transmit file ref counts. If the ref
// count drops to zero the transmit file is deleted.
func TestCleanupPeerTransfersSenderReleasesRef(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	downloadDir := filepath.Join(dir, "downloads")
	_ = os.MkdirAll(transmitDir, 0o700)

	// Create a transmit file.
	hash := "aabbccdd11223344556677889900aabb11223344556677889900aabbccddeeff"
	transmitPath := filepath.Join(transmitDir, hash+".png")
	_ = os.WriteFile(transmitPath, []byte("image data"), 0o600)

	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	store.mu.Lock()
	store.refs[hash] = 1
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	peerBob := domain.PeerIdentity("bob")
	peerAlice := domain.PeerIdentity("alice")

	// Sender mapping: we sent file to bob (announced, not yet completed).
	m.senderMaps["file-1"] = &senderFileMapping{
		FileID:    "file-1",
		FileHash:  hash,
		FileName:  "photo.png",
		FileSize:  1000,
		Recipient: peerBob,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Another sender mapping: same hash to alice (different recipient).
	store.mu.Lock()
	store.refs[hash] = 2
	store.mu.Unlock()
	m.senderMaps["file-2"] = &senderFileMapping{
		FileID:    "file-2",
		FileHash:  hash,
		FileName:  "photo.png",
		FileSize:  1000,
		Recipient: peerAlice,
		State:     senderServing,
		CreatedAt: time.Now(),
	}

	// Cleanup bob.
	m.CleanupPeerTransfers(peerBob)

	// file-1 should be removed.
	if _, ok := m.senderMaps["file-1"]; ok {
		t.Error("file-1 should be removed after cleanup")
	}

	// file-2 (alice) should still exist.
	if _, ok := m.senderMaps["file-2"]; !ok {
		t.Error("file-2 (alice) should NOT be removed")
	}

	// Transmit file should still exist (alice still references it).
	if _, err := os.Stat(transmitPath); os.IsNotExist(err) {
		t.Error("transmit file should still exist (ref count > 0)")
	}

	// Now cleanup alice.
	m.CleanupPeerTransfers(peerAlice)

	if _, ok := m.senderMaps["file-2"]; ok {
		t.Error("file-2 should be removed after cleanup")
	}

	// Transmit file should be deleted (ref count == 0).
	if _, err := os.Stat(transmitPath); !os.IsNotExist(err) {
		t.Error("transmit file should be deleted when last ref removed")
	}
}

// TestCleanupPeerTransfersReceiverDeletesFiles verifies that cleaning up a
// peer removes receiver mappings and deletes both completed and partial
// download files.
func TestCleanupPeerTransfersReceiverDeletesFiles(t *testing.T) {
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	receivedDir := filepath.Join(downloadDir, "received")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(receivedDir, 0o700)
	_ = os.MkdirAll(partialDir, 0o700)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	peerAlice := domain.PeerIdentity("alice")
	peerBob := domain.PeerIdentity("bob")

	// Completed download from alice.
	completedFile := filepath.Join(receivedDir, "hash123.pdf")
	_ = os.WriteFile(completedFile, []byte("pdf content"), 0o600)
	m.receiverMaps["file-a1"] = &receiverFileMapping{
		FileID:        "file-a1",
		FileHash:      "hash123",
		FileName:      "doc.pdf",
		Sender:        peerAlice,
		State:         receiverCompleted,
		CompletedPath: completedFile,
	}

	// Partial download from alice.
	partialFile := filepath.Join(partialDir, "file-a2.part")
	_ = os.WriteFile(partialFile, []byte("partial"), 0o600)
	m.receiverMaps["file-a2"] = &receiverFileMapping{
		FileID:   "file-a2",
		FileHash: "hash456",
		FileName: "video.mp4",
		Sender:   peerAlice,
		State:    receiverDownloading,
	}

	// Download from bob — should NOT be touched.
	bobFile := filepath.Join(receivedDir, "hash789.txt")
	_ = os.WriteFile(bobFile, []byte("bob data"), 0o600)
	m.receiverMaps["file-b1"] = &receiverFileMapping{
		FileID:        "file-b1",
		FileHash:      "hash789",
		FileName:      "notes.txt",
		Sender:        peerBob,
		State:         receiverCompleted,
		CompletedPath: bobFile,
	}

	// Cleanup alice.
	m.CleanupPeerTransfers(peerAlice)

	// Alice's entries should be gone.
	if _, ok := m.receiverMaps["file-a1"]; ok {
		t.Error("file-a1 should be removed")
	}
	if _, ok := m.receiverMaps["file-a2"]; ok {
		t.Error("file-a2 should be removed")
	}

	// Alice's files should be deleted.
	if _, err := os.Stat(completedFile); !os.IsNotExist(err) {
		t.Error("completed download from alice should be deleted")
	}
	if _, err := os.Stat(partialFile); !os.IsNotExist(err) {
		t.Error("partial download from alice should be deleted")
	}

	// Bob's entry and file should still exist.
	if _, ok := m.receiverMaps["file-b1"]; !ok {
		t.Error("file-b1 (bob) should NOT be removed")
	}
	if _, err := os.Stat(bobFile); os.IsNotExist(err) {
		t.Error("bob's download should NOT be deleted")
	}
}

// TestCleanupPeerTransfersCompletedSenderReleasesRef verifies that cleanup
// releases the transmit ref for a completed sender mapping. Completed mappings
// hold a ref because the blob must stay on disk for potential re-downloads.
// CleanupPeerTransfers (identity deletion) is one of the legitimate paths
// that releases the ref and deletes the blob.
func TestCleanupPeerTransfersCompletedSenderReleasesRef(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	_ = os.MkdirAll(transmitDir, 0o700)

	hash := "ccddee1122334455667788990011aabb22334455667788990011aabbccddeeff"
	transmitPath := filepath.Join(transmitDir, hash+".jpg")
	_ = os.WriteFile(transmitPath, []byte("image"), 0o600)

	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	// Completed mapping holds a ref.
	store.mu.Lock()
	store.refs[hash] = 1
	store.mu.Unlock()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		stopCh:       make(chan struct{}),
	}

	peer := domain.PeerIdentity("bob")
	m.senderMaps["file-c1"] = &senderFileMapping{
		FileID:    "file-c1",
		FileHash:  hash,
		Recipient: peer,
		State:     senderCompleted,
	}

	m.CleanupPeerTransfers(peer)

	if _, ok := m.senderMaps["file-c1"]; ok {
		t.Error("file-c1 should be removed from maps")
	}

	// Verify ref was released and blob deleted (no other refs).
	store.mu.Lock()
	refCount := store.refs[hash]
	store.mu.Unlock()
	if refCount != 0 {
		t.Errorf("ref count should be 0 after cleanup, got %d", refCount)
	}

	if _, err := os.Stat(transmitPath); !os.IsNotExist(err) {
		t.Error("transmit blob should be deleted after cleanup — sole ref holder removed")
	}
}

// TestCleanupPeerTransfersNilStoreDoesNotPanic verifies that
// CleanupPeerTransfers does not panic when m.store is nil and sender
// mappings with active ref counts exist. This happens in tests and
// utility constructions that instantiate Manager without
// a file store.
func TestCleanupPeerTransfersNilStoreDoesNotPanic(t *testing.T) {
	peer := domain.PeerIdentity("peer-nil-store")

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        nil, // intentionally nil
		stopCh:       make(chan struct{}),
	}

	// Active sender mapping — would need Release on a non-nil store.
	m.senderMaps["file-nil"] = &senderFileMapping{
		FileID:    "file-nil",
		FileHash:  "aabbccdd",
		Recipient: peer,
		State:     senderServing,
	}

	// Must not panic.
	m.CleanupPeerTransfers(peer)

	if _, ok := m.senderMaps["file-nil"]; ok {
		t.Error("sender mapping should be removed")
	}
}
