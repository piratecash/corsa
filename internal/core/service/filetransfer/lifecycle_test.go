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

	"github.com/piratecash/corsa/internal/core/domain"
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
	// onDownloadComplete now requires the caller to hold the
	// per-mapping writePartialMu (see its doc comment). Acquire it
	// here to honour the contract; the early-return branch makes the
	// lock functionally inert in this case.
	rm.writePartialMu.Lock()
	m.onDownloadComplete(fileID, partialPath, hash, sender)
	rm.writePartialMu.Unlock()

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

// TestFinalizeVerifiedDownloadStaleGenerationDoesNotResurrect verifies that a
// stale verifier goroutine cannot promote a newly restarted download attempt
// to waitingAck. The scenario:
//
//  1. Verifier A starts for fileID X, captures generation=G1, drops the lock
//     for hash verification and os.Rename (the I/O gap).
//  2. User cancels the download → state back to receiverAvailable, Generation
//     bumped to G2.
//  3. User immediately restarts the same fileID → a new attempt creates a
//     fresh download that advances to receiverVerifying with Generation=G3.
//  4. Verifier A re-acquires the lock for the final transition. The old
//     state-only check (state == receiverVerifying) would match because the
//     NEW verifier put it there. Without a generation guard, Verifier A
//     would overwrite CompletedPath with the old blob's path, transition the
//     new attempt to waitingAck, and send file_downloaded for a file the
//     user already abandoned.
//
// The fix: finalizeVerifiedDownload checks both state AND generation. A
// stale verifier with generation != mapping.Generation must abort and clean
// up its renamed blob without touching the live mapping.
func TestFinalizeVerifiedDownloadStaleGenerationDoesNotResurrect(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("stale-verifier-race")

	oldContent := []byte("old attempt's verified blob")
	oldHash := hashContent(oldContent)
	newContent := []byte("new attempt's in-flight content")
	newHash := hashContent(newContent)

	// Simulate post-rename state after Verifier A's successful rename.
	// The mapping now reflects the NEW attempt (different hash, different
	// generation), but Verifier A still holds a captured generation from
	// the previous attempt and a completedPath pointing to the stale blob.
	newGeneration := uint64(42)
	rm := testReceiverMapping(fileID, sender, newHash, "file.bin", uint64(len(newContent)), 1024, receiverVerifying)
	rm.Generation = newGeneration
	m.receiverMaps[fileID] = rm

	// Write the stale blob to the final downloads location, as if Verifier A
	// had already completed os.Rename of its own .part file.
	staleCompletedPath := completedDownloadPath(downloadDir, "file.bin", oldHash)
	if err := os.MkdirAll(filepath.Dir(staleCompletedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(staleCompletedPath, oldContent, 0o600); err != nil {
		t.Fatalf("write stale completed blob: %v", err)
	}
	staleInfo, err := os.Lstat(staleCompletedPath)
	if err != nil {
		t.Fatalf("lstat stale completed blob: %v", err)
	}

	// Verifier A's captured generation from before the cancel+restart race.
	staleGeneration := newGeneration - 10

	proceeded := m.finalizeVerifiedDownload(fileID, rm, staleGeneration, staleCompletedPath, staleInfo, sender)
	if proceeded {
		t.Fatal("finalizeVerifiedDownload must return false when generation is stale")
	}

	// New attempt's state must be preserved — NOT promoted to waitingAck.
	m.mu.Lock()
	finalState := rm.State
	finalCompletedPath := rm.CompletedPath
	finalGeneration := rm.Generation
	m.mu.Unlock()

	if finalState != receiverVerifying {
		t.Errorf("state = %s, want verifying (new attempt must be preserved)", finalState)
	}
	if finalCompletedPath != "" {
		t.Errorf("CompletedPath = %q, want empty (stale verifier must not overwrite new attempt's path)", finalCompletedPath)
	}
	if finalGeneration != newGeneration {
		t.Errorf("Generation = %d, want %d (must not be touched by stale verifier)", finalGeneration, newGeneration)
	}

	// file_downloaded must NOT be sent for the stale blob.
	if cc.hasSent(domain.FileActionDownloaded) {
		t.Fatal("file_downloaded must not be sent by a stale verifier")
	}

	// The stale completed file must be cleaned up by the stale verifier so
	// it does not leak into the downloads directory.
	if _, err := os.Stat(staleCompletedPath); !os.IsNotExist(err) {
		t.Errorf("stale completed blob should be removed, stat err = %v", err)
	}
}

// TestFinalizeVerifiedDownloadAfterCleanupAborts is the regression
// pin for the delete-during-verify race flagged by the reviewer:
//
//  1. A receiver is in receiverVerifying after the final
//     chunk_response triggered onDownloadComplete.
//  2. A user-driven CleanupTransferByMessageID (e.g. delete from
//     the chat thread or file tab) removes receiverMaps[fileID]
//     while the verifier is still hashing / renaming outside
//     m.mu.
//  3. The verifier's pre-cleanup pointer still has State ==
//     receiverVerifying and Generation == G (those fields are
//     on the orphaned struct).
//
// Without the map-ownership check in finalizeVerifiedDownload the
// stale state/generation comparison passes and the verifier:
//   - mutates State on the dead pointer (harmless),
//   - persists (harmless — the entry is gone),
//   - DISPATCHES file_downloaded to the sender (the bug — peer
//     thinks transfer succeeded for a transfer the user deleted).
//
// With the check, finalize sees receiverMaps[fileID] is missing
// (or no longer points at the captured mapping), aborts, and
// cleans up the renamed completedPath via removeOwnedFileInDownloadDir.
func TestFinalizeVerifiedDownloadAfterCleanupAborts(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("finalize-after-cleanup")

	content := []byte("verified bytes ready for finalize")
	hash := hashContent(content)

	generation := uint64(11)
	rm := testReceiverMapping(fileID, sender, hash, "file.bin", uint64(len(content)), 1024, receiverVerifying)
	rm.Generation = generation
	m.receiverMaps[fileID] = rm

	completedPath := completedDownloadPath(downloadDir, "file.bin", hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("write completed blob: %v", err)
	}
	completedInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat completed blob: %v", err)
	}

	// Simulate the user-driven cleanup running between the verifier's
	// rename and finalize. CleanupTransferByMessageID drops
	// receiverMaps[fileID] under m.mu and queues the on-disk file
	// removal — but the verifier's `mapping` pointer is still
	// receiverVerifying / generation 11.
	m.CleanupTransferByMessageID(fileID)

	// Sanity: the cleanup actually deleted the on-disk file we just
	// wrote, since CleanupTransferByMessageID's path-cleanup branch
	// runs synchronously via safeRemoveInDownloadDir.
	if _, err := os.Stat(completedPath); !os.IsNotExist(err) {
		// Recreate the file so the test can verify the verifier's
		// own cleanup branch behaves correctly. In production the
		// race window has the verifier's renamed file briefly
		// present after cleanup ran (the rename happens before
		// finalize's lock); the test mimics that ordering by
		// putting the file back here.
		if err := os.WriteFile(completedPath, content, 0o600); err != nil {
			t.Fatalf("recreate completed blob: %v", err)
		}
		completedInfo, err = os.Lstat(completedPath)
		if err != nil {
			t.Fatalf("re-lstat completed blob: %v", err)
		}
	}

	// Finalize with the captured (now-stale) pointer + generation.
	// MUST return false: the entry is gone from receiverMaps so the
	// map-ownership check fires regardless of state/generation.
	if m.finalizeVerifiedDownload(fileID, rm, generation, completedPath, completedInfo, sender) {
		t.Fatal("finalizeVerifiedDownload must return false when receiverMaps entry was deleted by cleanup")
	}

	// MUST NOT have sent file_downloaded — the user's delete
	// intent would otherwise be undermined by a wire-side ack from
	// our verifier saying "transfer succeeded".
	if cc.hasSent(domain.FileActionDownloaded) {
		t.Fatal("file_downloaded must NOT be dispatched after cleanup removed the mapping")
	}

	// MUST have unlinked the renamed completed file (verifier's
	// own cleanup branch using the captured Fstat identity).
	if _, err := os.Stat(completedPath); !os.IsNotExist(err) {
		t.Errorf("verifier did not clean up its renamed file after aborting; stat err = %v", err)
	}
}

// TestFinalizeVerifiedDownloadAfterReplaceMappingAborts covers the
// pointer-mismatch case: cleanup deleted the old mapping AND a
// fresh receive registered a NEW mapping for the same FileID
// before the stale verifier ran finalize. State and Generation on
// the new mapping might match the stale verifier's captured values
// by coincidence (especially generation: a fresh manager always
// starts at low generations), so without the pointer check
// finalize would happily transition the NEW mapping's state and
// dispatch file_downloaded for an unrelated re-registration.
func TestFinalizeVerifiedDownloadAfterReplaceMappingAborts(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("finalize-after-replace")
	content := []byte("verified bytes ready")
	hash := hashContent(content)

	// Old mapping — verifier captures this pointer.
	staleGeneration := uint64(7)
	staleMapping := testReceiverMapping(fileID, sender, hash, "file.bin", uint64(len(content)), 1024, receiverVerifying)
	staleMapping.Generation = staleGeneration
	m.receiverMaps[fileID] = staleMapping

	completedPath := completedDownloadPath(downloadDir, "file.bin", hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("write completed blob: %v", err)
	}
	completedInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat completed blob: %v", err)
	}

	// Cleanup removes the old mapping...
	m.CleanupTransferByMessageID(fileID)
	// ...and a fresh register-receive for the same FileID populates
	// a NEW mapping pointer with the SAME Generation by coincidence.
	freshMapping := testReceiverMapping(fileID, sender, hash, "file.bin", uint64(len(content)), 1024, receiverVerifying)
	freshMapping.Generation = staleGeneration
	m.receiverMaps[fileID] = freshMapping

	// Recreate the renamed file so the verifier's cleanup branch
	// has something to consider unlinking.
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("recreate completed blob: %v", err)
	}

	// Finalize with the OLD pointer + matching state/generation.
	// MUST return false because receiverMaps[fileID] != staleMapping.
	if m.finalizeVerifiedDownload(fileID, staleMapping, staleGeneration, completedPath, completedInfo, sender) {
		t.Fatal("finalizeVerifiedDownload must return false when receiverMaps[fileID] points at a different (replaced) mapping")
	}

	// The fresh mapping must NOT have been moved to waiting_ack
	// by the stale verifier.
	m.mu.Lock()
	current := m.receiverMaps[fileID]
	freshState := current.State
	m.mu.Unlock()
	if freshState != receiverVerifying {
		t.Errorf("fresh mapping state = %s, want %s — stale verifier mutated a foreign mapping",
			freshState, receiverVerifying)
	}
	if cc.hasSent(domain.FileActionDownloaded) {
		t.Fatal("file_downloaded must NOT be dispatched when finalize aborts on pointer mismatch")
	}
}

// TestFinalizeVerifiedDownloadHappyPath verifies that finalizeVerifiedDownload
// completes normally when state and generation still match the verifier's
// captured values — the common case after a successful hash verify + rename.
func TestFinalizeVerifiedDownloadHappyPath(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("finalize-happy-path")

	content := []byte("happy path verified content")
	hash := hashContent(content)

	generation := uint64(7)
	rm := testReceiverMapping(fileID, sender, hash, "file.bin", uint64(len(content)), 1024, receiverVerifying)
	rm.Generation = generation
	m.receiverMaps[fileID] = rm

	completedPath := completedDownloadPath(downloadDir, "file.bin", hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("write completed blob: %v", err)
	}
	completedInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat completed blob: %v", err)
	}

	if !m.finalizeVerifiedDownload(fileID, rm, generation, completedPath, completedInfo, sender) {
		t.Fatal("finalizeVerifiedDownload must return true on matching generation")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if rm.State != receiverWaitingAck {
		t.Errorf("state = %s, want waiting_ack", rm.State)
	}
	if rm.CompletedPath != completedPath {
		t.Errorf("CompletedPath = %q, want %q", rm.CompletedPath, completedPath)
	}
	if !cc.hasSent(domain.FileActionDownloaded) {
		t.Error("file_downloaded must be sent on happy path")
	}
	if _, err := os.Stat(completedPath); err != nil {
		t.Errorf("completed blob must remain on disk, stat err = %v", err)
	}
}

// TestFinalizeVerifiedDownloadFiresCompletionCallback verifies that
// finalizeVerifiedDownload invokes the OnReceiverDownloadComplete
// callback exactly once on the happy path with the metadata captured
// from the receiver mapping. The desktop UI relies on this hook to
// play download-done.mp3 — silently dropping it would leave the user
// without an audible cue when a transfer finishes in the background.
func TestFinalizeVerifiedDownloadFiresCompletionCallback(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	var (
		got   []ReceiverDownloadCompletedEvent
		calls int
	)
	m.onReceiverDownloadComplete = func(ev ReceiverDownloadCompletedEvent) {
		calls++
		got = append(got, ev)
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("finalize-callback-fires")

	content := []byte("notify-me content")
	hash := hashContent(content)

	generation := uint64(11)
	rm := testReceiverMapping(fileID, sender, hash, "doc.bin", uint64(len(content)), 1024, receiverVerifying)
	rm.Generation = generation
	rm.ContentType = "application/octet-stream"
	m.receiverMaps[fileID] = rm

	completedPath := completedDownloadPath(downloadDir, "doc.bin", hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("write completed blob: %v", err)
	}
	completedInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat completed blob: %v", err)
	}

	if !m.finalizeVerifiedDownload(fileID, rm, generation, completedPath, completedInfo, sender) {
		t.Fatal("finalizeVerifiedDownload must return true on matching generation")
	}

	if calls != 1 {
		t.Fatalf("OnReceiverDownloadComplete fired %d times; want 1", calls)
	}
	ev := got[0]
	if ev.FileID != fileID {
		t.Errorf("event.FileID = %q, want %q", ev.FileID, fileID)
	}
	if ev.Sender != sender {
		t.Errorf("event.Sender = %q, want %q", ev.Sender, sender)
	}
	if ev.FileName != "doc.bin" {
		t.Errorf("event.FileName = %q, want %q", ev.FileName, "doc.bin")
	}
	if ev.FileSize != uint64(len(content)) {
		t.Errorf("event.FileSize = %d, want %d", ev.FileSize, len(content))
	}
	if ev.ContentType != "application/octet-stream" {
		t.Errorf("event.ContentType = %q, want %q", ev.ContentType, "application/octet-stream")
	}
}

// TestFinalizeVerifiedDownloadDoesNotFireCallbackOnStaleGeneration
// guards against an audible cue being played for a transfer that the
// user already cancelled or replaced via cancel+restart. The verifier
// running on the stale generation must abort BEFORE the callback path
// — the desktop UI would otherwise notify the user about a download
// that never produced a usable file.
func TestFinalizeVerifiedDownloadDoesNotFireCallbackOnStaleGeneration(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	calls := 0
	m.onReceiverDownloadComplete = func(ReceiverDownloadCompletedEvent) {
		calls++
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("finalize-callback-stale")

	content := []byte("stale generation content")
	hash := hashContent(content)

	const currentGeneration uint64 = 9
	const staleGeneration uint64 = 7
	rm := testReceiverMapping(fileID, sender, hash, "stale.bin", uint64(len(content)), 1024, receiverVerifying)
	rm.Generation = currentGeneration
	m.receiverMaps[fileID] = rm

	completedPath := completedDownloadPath(downloadDir, "stale.bin", hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}
	if err := os.WriteFile(completedPath, content, 0o600); err != nil {
		t.Fatalf("write completed blob: %v", err)
	}
	completedInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat completed blob: %v", err)
	}

	if m.finalizeVerifiedDownload(fileID, rm, staleGeneration, completedPath, completedInfo, sender) {
		t.Fatal("finalizeVerifiedDownload must return false when generation is stale")
	}
	if calls != 0 {
		t.Fatalf("OnReceiverDownloadComplete fired %d times on stale generation; want 0", calls)
	}
}

// TestFinalizeVerifiedDownloadStaleCleanupPreservesNewAttemptFile verifies
// that when a stale verifier aborts on generation mismatch, it does NOT
// delete a file at completedPath that belongs to a different attempt.
//
// The race: cancel+restart of the same fileID resolves to the same
// completedPath because FileName and FileHash match. Sequence:
//
//  1. Verifier A hashes its .part and os.Renames it to completedPath
//     (inode I_A). User cancels.
//  2. User restarts the same fileID. A new attempt produces its own .part,
//     Verifier B verifies it and os.Renames to the same completedPath —
//     the rename atomically unlinks I_A and places inode I_B there.
//  3. Verifier A finally enters finalizeVerifiedDownload, sees generation
//     mismatch, and attempts to clean up "its" file at completedPath.
//
// Before the fix: Verifier A's cleanup unconditionally unlinked the path,
// destroying the new attempt's legitimate file. After the fix: the stale
// branch uses os.SameFile against the pre-rename Fstat identity captured
// by verifyPartialIntegrity — the inodes differ, so the stale verifier
// leaves the file alone.
func TestFinalizeVerifiedDownloadStaleCleanupPreservesNewAttemptFile(t *testing.T) {
	downloadDir := testDownloadDir(t)
	cc := &commandCollector{}
	m := newTestTransferManager(t, downloadDir, cc)

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("stale-cleanup-preserves-new")

	fileName := "shared.bin"
	hash := "d0c0ffee0000d0c0ffee0000d0c0ffee0000d0c0ffee0000d0c0ffee00000123"
	completedPath := completedDownloadPath(downloadDir, fileName, hash)
	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		t.Fatalf("mkdir completed dir: %v", err)
	}

	// Step 1: Verifier A writes its file at completedPath, captures identity.
	oldContent := []byte("old attempt's verified bytes")
	if err := os.WriteFile(completedPath, oldContent, 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	staleVerifierInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat old file: %v", err)
	}

	// Step 2: New attempt atomically replaces the file via a tmp + rename,
	// which is exactly what os.Rename from a .part file does. This unlinks
	// Verifier A's inode and places a new inode at completedPath.
	newContent := []byte("new attempt's legitimately verified bytes")
	tmpPath := completedPath + ".tmp-new-attempt"
	if err := os.WriteFile(tmpPath, newContent, 0o600); err != nil {
		t.Fatalf("write new tmp: %v", err)
	}
	if err := os.Rename(tmpPath, completedPath); err != nil {
		t.Fatalf("atomic rename new over old: %v", err)
	}
	newInfo, err := os.Lstat(completedPath)
	if err != nil {
		t.Fatalf("lstat new file: %v", err)
	}
	// Sanity: the new attempt really did change the inode. On some
	// filesystems os.Rename over an existing target may reuse the inode;
	// if that ever happens this test becomes a no-op and we surface it.
	if os.SameFile(staleVerifierInfo, newInfo) {
		t.Skip("filesystem reused inode across rename; cannot exercise this race here")
	}

	// The mapping reflects the NEW attempt — verifying with a fresh
	// generation. The stale verifier holds an older generation.
	newGeneration := uint64(77)
	rm := testReceiverMapping(fileID, sender, hash, fileName, uint64(len(newContent)), 1024, receiverVerifying)
	rm.Generation = newGeneration
	m.receiverMaps[fileID] = rm

	staleGeneration := newGeneration - 5

	// Step 3: Verifier A enters finalize with its own pre-rename Fstat
	// identity — the stale branch must NOT delete the new attempt's file.
	proceeded := m.finalizeVerifiedDownload(fileID, rm, staleGeneration, completedPath, staleVerifierInfo, sender)
	if proceeded {
		t.Fatal("finalizeVerifiedDownload must return false when generation is stale")
	}

	// The new attempt's file must survive untouched.
	survived, err := os.ReadFile(completedPath)
	if err != nil {
		t.Fatalf("new attempt's file was deleted by stale verifier: %v", err)
	}
	if string(survived) != string(newContent) {
		t.Errorf("file content = %q, want %q (stale verifier corrupted it)", survived, newContent)
	}

	// State must be unchanged and file_downloaded must not be sent.
	m.mu.Lock()
	finalState := rm.State
	finalGen := rm.Generation
	m.mu.Unlock()
	if finalState != receiverVerifying {
		t.Errorf("state = %s, want verifying (stale verifier must not touch new attempt)", finalState)
	}
	if finalGen != newGeneration {
		t.Errorf("Generation = %d, want %d (must not be touched)", finalGen, newGeneration)
	}
	if cc.hasSent(domain.FileActionDownloaded) {
		t.Fatal("file_downloaded must not be sent by a stale verifier")
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
