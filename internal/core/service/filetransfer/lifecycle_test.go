package filetransfer

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/domain"
)

// testDownloadDir creates the directory layout required by Manager:
// <root>/downloads/ and <root>/downloads/partial/.
func testDownloadDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatalf("create download dirs: %v", err)
	}
	return downloadDir
}

// commandCollector is a sendCommand callback that records every sent payload.
type commandCollector struct {
	sent []domain.FileCommandPayload
}

func (c *commandCollector) send(_ domain.PeerIdentity, payload domain.FileCommandPayload) error {
	c.sent = append(c.sent, payload)
	return nil
}

func (c *commandCollector) hasSent(action domain.FileAction) bool {
	for _, p := range c.sent {
		if p.Command == action {
			return true
		}
	}
	return false
}

// newTestTransferManager creates a Manager wired to the given
// downloadDir and command collector. The collector may be nil (noop send).
func newTestTransferManager(t *testing.T, downloadDir string, cc *commandCollector) *Manager {
	t.Helper()
	send := func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error { return nil }
	if cc != nil {
		send = cc.send
	}
	return &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand:  send,
		stopCh:       make(chan struct{}),
	}
}

// testReceiverMapping creates a receiverFileMapping pre-filled with common
// test defaults including a custom chunkSize. Unlike the production
// constructor newReceiverMapping, this helper does not set ContentType and
// accepts chunkSize directly. Caller should override fields as needed
// before inserting into the manager's receiverMaps.
func testReceiverMapping(fileID domain.FileID, sender domain.PeerIdentity, fileHash string, fileName string, fileSize uint64, chunkSize uint32, state receiverState) *receiverFileMapping {
	return &receiverFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  fileName,
		FileSize:  fileSize,
		Sender:    sender,
		State:     state,
		ChunkSize: chunkSize,
		CreatedAt: time.Now(),
	}
}

// hashContent returns the hex-encoded SHA-256 of data.
func hashContent(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// TestReceiverFullLifecycle exercises the complete receiver state machine:
// Available → Downloading → Verifying → WaitingAck → Completed.
// A single-chunk file is downloaded, hash-verified, moved to final location,
// file_downloaded sent, and file_downloaded_ack received.
func TestReceiverFullLifecycle(t *testing.T) {
	t.Parallel()

	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("lifecycle-sender-identity-1234")
	fileID := domain.FileID("lifecycle-test-file")

	fileContent := []byte("Hello, this is the complete file content for lifecycle test!")
	fileSize := uint64(len(fileContent))
	fileHash := hashContent(fileContent)

	// Step 1: Register incoming file — state should be Available.
	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, fileHash, "lifecycle.txt", fileSize, 1024, receiverAvailable,
	)

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverAvailable {
		t.Fatalf("initial state = %s, want %s", mapping.State, receiverAvailable)
	}

	// Step 2: Start download — state should transition to Downloading.
	mapping.State = receiverDownloading
	mapping.LastChunkAt = time.Now()
	mapping.ChunkRetries = 0

	if mapping.State != receiverDownloading {
		t.Fatalf("after start, state = %s, want %s", mapping.State, receiverDownloading)
	}

	// Step 3: Receive chunk_response with full file content — triggers
	// onDownloadComplete → Verifying → hash check → WaitingAck.
	encoded := base64.RawURLEncoding.EncodeToString(fileContent)
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping = m.receiverMaps[fileID]
	if mapping.State != receiverWaitingAck {
		t.Fatalf("after download complete, state = %s, want %s", mapping.State, receiverWaitingAck)
	}

	// Verify file_downloaded was sent.
	if !cc.hasSent(domain.FileActionDownloaded) {
		t.Error("file_downloaded command should have been sent")
	}

	// Verify the completed file exists in the downloads directory.
	if mapping.CompletedPath == "" {
		t.Fatal("CompletedPath should be set after successful verification")
	}
	if _, err := os.Stat(mapping.CompletedPath); os.IsNotExist(err) {
		t.Errorf("completed file should exist at %s", mapping.CompletedPath)
	}

	// Partial file should have been moved (no longer exists).
	partialPath := partialDownloadPath(downloadDir, fileID)
	if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
		t.Error("partial file should have been moved after verification")
	}

	// Step 4: Receive file_downloaded_ack — state should transition to Completed.
	m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{
		FileID: fileID,
	})

	mapping = m.receiverMaps[fileID]
	if mapping.State != receiverCompleted {
		t.Fatalf("after ack, state = %s, want %s", mapping.State, receiverCompleted)
	}

	if mapping.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
}

// TestEOFByBytesReceivedEqualsFileSize verifies that download completes
// exactly when BytesReceived reaches FileSize, including for chunk-aligned
// files where the last chunk is exactly ChunkSize bytes.
func TestEOFByBytesReceivedEqualsFileSize(t *testing.T) {
	t.Parallel()

	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("eof-sender")
	fileID := domain.FileID("eof-chunk-aligned")
	chunkSize := uint32(64)

	// File size is exactly 2 * chunkSize — last chunk is full-sized.
	fileContent := make([]byte, chunkSize*2)
	for i := range fileContent {
		fileContent[i] = byte(i % 256)
	}

	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, hashContent(fileContent), "aligned.bin",
		uint64(len(fileContent)), chunkSize, receiverDownloading,
	)

	// Send first chunk (0..63).
	chunk1 := fileContent[:chunkSize]
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   base64.RawURLEncoding.EncodeToString(chunk1),
	})

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverDownloading {
		t.Fatalf("after chunk 1, state = %s, want downloading", mapping.State)
	}
	if mapping.BytesReceived != uint64(chunkSize) {
		t.Fatalf("BytesReceived = %d, want %d", mapping.BytesReceived, chunkSize)
	}

	// Send second (last) chunk (64..127) — exactly fills FileSize.
	chunk2 := fileContent[chunkSize:]
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: uint64(chunkSize),
		Data:   base64.RawURLEncoding.EncodeToString(chunk2),
	})

	mapping = m.receiverMaps[fileID]
	// Should have transitioned through verifying → waitingAck.
	if mapping.State != receiverWaitingAck {
		t.Errorf("after final chunk, state = %s, want %s", mapping.State, receiverWaitingAck)
	}
	if !cc.hasSent(domain.FileActionDownloaded) {
		t.Error("file_downloaded should have been sent after last chunk")
	}
}

// TestSHA256VerificationPass verifies that a file with correct SHA-256 hash
// passes verification and transitions to WaitingAck.
func TestSHA256VerificationPass(t *testing.T) {
	t.Parallel()

	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	fileContent := []byte("sha256 verification test content")
	sender := domain.PeerIdentity("hash-pass-sender")
	fileID := domain.FileID("hash-pass-test")

	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, hashContent(fileContent), "hashtest.dat",
		uint64(len(fileContent)), 4096, receiverDownloading,
	)

	encoded := base64.RawURLEncoding.EncodeToString(fileContent)
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverWaitingAck {
		t.Errorf("state = %s, want %s (hash verification should pass)", mapping.State, receiverWaitingAck)
	}
}

// TestSHA256VerificationFail verifies that a file with incorrect SHA-256 hash
// is rejected and transitions to Failed.
func TestSHA256VerificationFail(t *testing.T) {
	t.Parallel()

	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	fileContent := []byte("tampered content")
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"

	sender := domain.PeerIdentity("hash-fail-sender")
	fileID := domain.FileID("hash-fail-test")

	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, wrongHash, "tampered.dat",
		uint64(len(fileContent)), 4096, receiverDownloading,
	)

	encoded := base64.RawURLEncoding.EncodeToString(fileContent)
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverFailed {
		t.Errorf("state = %s, want %s (hash mismatch should fail)", mapping.State, receiverFailed)
	}

	// Partial file should have been cleaned up.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
		t.Error("partial file should be removed after hash verification failure")
	}
}

// TestDefaultChunkSizeWireSizeUnderLimit verifies that a DefaultChunkSize
// (16 KB) chunk, after base64 encoding and JSON wrapping, fits within
// maxRelayBodyBytes (64 KB).
func TestDefaultChunkSizeWireSizeUnderLimit(t *testing.T) {
	t.Parallel()

	const maxRelayBodyBytes = 65_536

	// Simulate maximum-size chunk_response.
	chunkData := make([]byte, domain.DefaultChunkSize)
	encoded := base64.RawURLEncoding.EncodeToString(chunkData)

	// Build the wire representation of a chunk_response payload.
	resp := domain.ChunkResponsePayload{
		FileID: domain.FileID("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"),
		Offset: 999_999_999, // large offset to test size
		Data:   encoded,
	}

	wireJSON, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal chunk_response: %v", err)
	}

	// Wrap in FileCommandPayload (adds command field).
	cmdPayload := domain.FileCommandPayload{
		Command: domain.FileActionChunkResp,
		Data:    json.RawMessage(wireJSON),
	}
	fullWire, err := json.Marshal(cmdPayload)
	if err != nil {
		t.Fatalf("marshal file command payload: %v", err)
	}

	// The encrypted+base64 payload adds ~44 bytes overhead (32 eph key +
	// 12 nonce) + GCM tag (16) + base64 expansion (4/3).
	// Approximate overhead: (len(fullWire) + 60) * 4 / 3.
	encryptedEstimate := (len(fullWire) + 60) * 4 / 3

	// Add FileCommandFrame header overhead (SRC, DST, TTL, Time, Nonce,
	// Signature fields). Conservative estimate: ~500 bytes.
	totalEstimate := encryptedEstimate + 500

	if totalEstimate >= maxRelayBodyBytes {
		t.Errorf("estimated wire size %d exceeds maxRelayBodyBytes %d (DefaultChunkSize=%d too large)",
			totalEstimate, maxRelayBodyBytes, domain.DefaultChunkSize)
	}

	// Verify we're using at least 25% of the budget (sanity check
	// that the chunk is meaningfully sized).
	if totalEstimate < maxRelayBodyBytes/4 {
		t.Errorf("estimated wire size %d is less than 25%% of maxRelayBodyBytes — chunk could be larger",
			totalEstimate)
	}
}

// TestStaleChunkResponseDoesNotCorruptPartialFile verifies that a
// chunk_response with an offset that does not match the expected NextOffset
// is rejected BEFORE any disk write. Previously the offset check ran after
// writeChunkToFile, allowing a delayed/duplicate response to overwrite data
// inside the .part file and silently corrupt the download.
func TestStaleChunkResponseDoesNotCorruptPartialFile(t *testing.T) {
	t.Parallel()

	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	sender := domain.PeerIdentity("stale-chunk-sender")
	fileID := domain.FileID("stale-chunk-test")
	chunkSize := uint32(64)

	// File of 3 chunks.
	fileContent := make([]byte, chunkSize*3)
	for i := range fileContent {
		fileContent[i] = byte(i % 256)
	}

	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, hashContent(fileContent), "stale.bin",
		uint64(len(fileContent)), chunkSize, receiverDownloading,
	)

	// Deliver chunk 0 (offset 0) — this is the expected offset.
	chunk0 := fileContent[:chunkSize]
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   base64.RawURLEncoding.EncodeToString(chunk0),
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != uint64(chunkSize) {
		t.Fatalf("NextOffset = %d, want %d", mapping.NextOffset, chunkSize)
	}

	// Read the partial file content after chunk 0.
	partialPath := partialDownloadPath(downloadDir, fileID)
	beforeStale, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial file: %v", err)
	}

	// Now deliver a STALE chunk_response with offset=0 (duplicate of chunk 0)
	// but with DIFFERENT data. If the bug exists, this would overwrite bytes
	// 0..63 in the partial file with garbage.
	poisonData := make([]byte, chunkSize)
	for i := range poisonData {
		poisonData[i] = 0xFF
	}
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0, // stale — expected is chunkSize
		Data:   base64.RawURLEncoding.EncodeToString(poisonData),
	})

	// NextOffset must NOT have changed (stale response was ignored).
	mapping = m.receiverMaps[fileID]
	if mapping.NextOffset != uint64(chunkSize) {
		t.Errorf("NextOffset changed to %d after stale response, want %d", mapping.NextOffset, chunkSize)
	}

	// The partial file must be UNCHANGED — no disk write should have happened.
	afterStale, err := os.ReadFile(partialPath)
	if err != nil {
		t.Fatalf("read partial file after stale: %v", err)
	}

	if len(beforeStale) != len(afterStale) {
		t.Fatalf("partial file size changed: %d → %d", len(beforeStale), len(afterStale))
	}
	for i := range beforeStale {
		if beforeStale[i] != afterStale[i] {
			t.Fatalf("partial file corrupted at byte %d: 0x%02X → 0x%02X (stale chunk wrote to disk)",
				i, beforeStale[i], afterStale[i])
		}
	}
}

// TestReceiverRetryBackoffOnStall verifies that when a chunk request gets no
// response and the stall timeout expires, the retry mechanism fires with
// appropriate backoff.
func TestReceiverRetryBackoffOnStall(t *testing.T) {
	t.Parallel()

	sender := domain.PeerIdentity("stall-sender")
	fileID := domain.FileID("stall-test")

	var retryCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  t.TempDir(),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				retryCount++
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	stalledTime := time.Now().Add(-2 * chunkRequestStallTimeout)
	m.receiverMaps[fileID] = testReceiverMapping(
		fileID, sender, "somehash", "stall.bin", 100000, 1024, receiverDownloading,
	)
	m.receiverMaps[fileID].LastChunkAt = stalledTime

	// Run tickReceiverMappings to simulate the background loop detecting the stall.
	m.tickReceiverMappings()

	if retryCount == 0 {
		t.Error("expected retry after stall timeout, but no chunk_request was sent")
	}

	mapping := m.receiverMaps[fileID]
	if mapping.ChunkRetries != 1 {
		t.Errorf("ChunkRetries = %d, want 1", mapping.ChunkRetries)
	}
}

// TestCancelDuringVerifyDoesNotResurrectTransfer verifies that if the user
// cancels a download while onDownloadComplete is running (state =
// receiverVerifying), the completion path does not overwrite the cancelled
// state with receiverWaitingAck. Before the fix, onDownloadComplete blindly
// set receiverWaitingAck after hash verification without re-checking state,
// causing a cancelled transfer to come back to life.
//
// The race window: onDownloadComplete sets receiverVerifying and drops the
// lock for hash I/O. CancelDownload runs on another goroutine, sees
// receiverVerifying, resets to receiverAvailable. onDownloadComplete finishes
// verification, re-acquires the lock, and must detect that the state is no
// longer receiverVerifying.
//
// We simulate this by manually setting receiverVerifying (mimicking the first
// half of onDownloadComplete) and cancelling, then calling onDownloadComplete
// on a mapping already in receiverAvailable.
func TestCancelDuringVerifyDoesNotResurrectTransfer(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("cancel-verify-race")

	// Build a valid partial file so hash verification would succeed.
	content := []byte("complete file content for cancel-verify test")
	hash := hashContent(content)

	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, content, 0o600); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	// Register in verifying state (simulates the first half of
	// onDownloadComplete having already run).
	rm := testReceiverMapping(fileID, sender, hash, "test.bin", uint64(len(content)), 1024, receiverVerifying)
	m.receiverMaps[fileID] = rm

	// Cancel while "verification is running" (lock is not held by verifier).
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload should succeed in verifying state: %v", err)
	}
	if rm.State != receiverAvailable {
		t.Fatalf("state after cancel = %s, want available", rm.State)
	}

	// Now simulate onDownloadComplete being called on the cancelled mapping.
	// It must detect the state is no longer downloading and exit early.
	m.onDownloadComplete(fileID, partialPath, hash, sender)

	m.mu.Lock()
	finalState := m.receiverMaps[fileID].State
	m.mu.Unlock()

	if finalState == receiverWaitingAck {
		t.Fatal("onDownloadComplete resurrected cancelled transfer to waiting_ack")
	}
	if finalState != receiverAvailable {
		t.Fatalf("state = %s, want available (cancel should be preserved)", finalState)
	}

	// file_downloaded must NOT have been sent.
	if cc.hasSent(domain.FileActionDownloaded) {
		t.Fatal("file_downloaded should not be sent for a cancelled transfer")
	}
}

// TestMarkReceiverFailedRespectsCancel verifies that markReceiverFailed does
// not overwrite a cancelled (available) state with failed. Before the fix,
// markReceiverFailed blindly set receiverFailed regardless of current state.
func TestMarkReceiverFailedRespectsCancel(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("fail-cancel-race")
	rm := &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName:  "test.bin",
		FileSize:  1024,
		Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:     receiverAvailable, // cancelled
		CreatedAt: time.Now(),
	}
	m.receiverMaps[fileID] = rm

	// markReceiverFailed expects receiverVerifying, but state is available.
	// Generation 0 matches the zero-value mapping — the guard rejects on state mismatch.
	m.markReceiverFailed(rm, receiverVerifying, 0)

	if rm.State != receiverAvailable {
		t.Fatalf("state = %s, want available (cancel should be preserved)", rm.State)
	}
}
