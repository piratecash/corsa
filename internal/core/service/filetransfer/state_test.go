package filetransfer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Sender state machine tests
// ---------------------------------------------------------------------------

// TestSenderStateAnnouncedToServingToCompleted verifies the full sender state
// machine lifecycle: Announced → Serving → Completed. HandleChunkRequest
// transitions to Serving, HandleFileDownloaded transitions to Completed.
func TestSenderStateAnnouncedToServingToCompleted(t *testing.T) {
	dir := t.TempDir()

	fileHash := "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"
	fileContent := []byte("file content for lifecycle test")
	filePath := dir + "/" + fileHash + ".bin"
	if err := writeTestFile(filePath, fileContent); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentPayloads []domain.FileCommandPayload
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("lifecycle-file-id")

	// Start in Announced.
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "lifecycle.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// 1. HandleChunkRequest → transitions to Serving.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[fileID].State != senderServing {
		t.Fatalf("after chunk_request: state = %s, want serving", m.senderMaps[fileID].State)
	}
	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sentPayloads))
	}
	if sentPayloads[0].Command != domain.FileActionChunkResp {
		t.Errorf("expected chunk_response command, got %s", sentPayloads[0].Command)
	}

	// Completed mapping holds a ref — set it to match what Acquire produces.
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()

	// 2. HandleFileDownloaded → transitions to Completed and sends ack.
	// The ref is NOT released — the blob stays on disk for re-downloads.
	sentPayloads = nil
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID})

	if m.senderMaps[fileID].State != senderCompleted {
		t.Fatalf("after file_downloaded: state = %s, want completed", m.senderMaps[fileID].State)
	}
	if m.senderMaps[fileID].CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 file_downloaded_ack, got %d", len(sentPayloads))
	}
	if sentPayloads[0].Command != domain.FileActionDownloadedAck {
		t.Errorf("expected file_downloaded_ack command, got %s", sentPayloads[0].Command)
	}
}

// TestSenderCompletedReservesChunkRequest verifies that a chunk_request for a
// completed mapping is accepted. The transmit blob stays on disk after the
// first transfer, so re-downloads (new device, retry) must be served.
func TestSenderCompletedReservesChunkRequest(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("completed-redownload")
	fileHash := sha256Hex([]byte("redownload-content"))

	// Write transmit blob.
	blobData := []byte("redownload-content-data")
	if err := os.WriteFile(filepath.Join(transmitDir, fileHash+".bin"), blobData, 0o600); err != nil {
		t.Fatal(err)
	}

	// Set up a completed mapping with a ref.
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()

	var sentPayloads []domain.FileCommandPayload
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    fileHash,
		FileName:    "photo.jpg",
		FileSize:    uint64(len(blobData)),
		Recipient:   receiver,
		State:       senderCompleted,
		CreatedAt:   time.Now().Add(-time.Hour),
		CompletedAt: time.Now().Add(-30 * time.Minute),
	}

	// chunk_request on a completed mapping must succeed.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[fileID].State != senderServing {
		t.Fatalf("after chunk_request on completed: state = %s, want serving", m.senderMaps[fileID].State)
	}
	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sentPayloads))
	}
	if sentPayloads[0].Command != domain.FileActionChunkResp {
		t.Errorf("expected chunk_response command, got %s", sentPayloads[0].Command)
	}
}

// TestSenderTombstoneResurrectedAtServeTime verifies that a tombstoned mapping
// whose blob reappears on disk is resurrected to completed at chunk_request
// time (runtime resurrection). The filesystem is the source of truth — the
// state field alone must not block serving when the blob exists.
func TestSenderTombstoneResurrectedAtServeTime(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("tombstone-resurrect-runtime")
	fileHash := sha256Hex([]byte("runtime-resurrect-content"))

	// Write the transmit blob on disk (simulates re-send or manual restore).
	blobData := []byte("runtime-resurrect-content-data")
	if err := os.WriteFile(filepath.Join(transmitDir, fileHash+".bin"), blobData, 0o600); err != nil {
		t.Fatal(err)
	}

	// No ref in the store — the tombstone had released it.
	var sentPayloads []domain.FileCommandPayload
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    fileHash,
		FileName:    "restored.jpg",
		FileSize:    uint64(len(blobData)),
		Recipient:   receiver,
		State:       senderTombstone,
		CreatedAt:   time.Now().Add(-2 * time.Hour),
		CompletedAt: time.Now().Add(-time.Hour),
	}

	// chunk_request on a tombstone mapping with blob on disk must succeed.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[fileID].State != senderServing {
		t.Fatalf("after chunk_request on tombstone with blob: state = %s, want serving", m.senderMaps[fileID].State)
	}
	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sentPayloads))
	}
	if sentPayloads[0].Command != domain.FileActionChunkResp {
		t.Errorf("expected chunk_response command, got %s", sentPayloads[0].Command)
	}

	// Verify that Acquire re-established the ref.
	store.mu.Lock()
	refs := store.refs[fileHash]
	store.mu.Unlock()
	if refs != 1 {
		t.Errorf("expected ref count 1 after resurrection, got %d", refs)
	}
}

// TestSenderTombstoneNoBlobStillRejected verifies that a tombstoned mapping
// without a blob on disk is still rejected — the filesystem says "no".
func TestSenderTombstoneNoBlobStillRejected(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("tombstone-no-blob")

	var sentPayloads []domain.FileCommandPayload
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName:    "gone.jpg",
		FileSize:    1024,
		Recipient:   receiver,
		State:       senderTombstone,
		CreatedAt:   time.Now().Add(-2 * time.Hour),
		CompletedAt: time.Now().Add(-time.Hour),
	}

	// chunk_request on a tombstone without blob must fail.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[fileID].State != senderTombstone {
		t.Fatalf("tombstone without blob: state = %s, want tombstone", m.senderMaps[fileID].State)
	}
	if len(sentPayloads) != 0 {
		t.Fatalf("expected 0 responses for rejected tombstone, got %d", len(sentPayloads))
	}
}

// TestSenderIdempotentFileDownloaded verifies that a duplicate file_downloaded
// for an already-completed mapping re-sends the ack without side effects
// (no double Release, no state change).
func TestSenderIdempotentFileDownloaded(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("idempotent-file")
	completedAt := time.Now().Add(-time.Hour)

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    "hash-x",
		FileName:    "file.bin",
		FileSize:    100,
		Recipient:   receiver,
		State:       senderCompleted,
		CompletedAt: completedAt,
	}

	var acksSent int
	m.sendCommand = func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
		acksSent++
		return nil
	}

	// Send duplicate file_downloaded.
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID})

	// Should re-send ack.
	if acksSent != 1 {
		t.Errorf("expected 1 ack re-sent for duplicate file_downloaded, got %d", acksSent)
	}

	// CompletedAt should NOT be updated (idempotent).
	if !m.senderMaps[fileID].CompletedAt.Equal(completedAt) {
		t.Error("CompletedAt should not change on duplicate file_downloaded")
	}
}

// TestSenderTombstonedFileIDIgnoresChunkRequest verifies that a chunk_request
// for a tombstoned FileID is silently ignored (no chunk_response sent).
func TestSenderTombstonedFileIDIgnoresChunkRequest(t *testing.T) {
	var commandsSent int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			commandsSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity-1234567890ab")
	fileID := domain.FileID("tombstoned-file")

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    "hash-tomb",
		FileName:    "old.bin",
		FileSize:    100,
		Recipient:   receiver,
		State:       senderTombstone,
		CompletedAt: time.Now().Add(-time.Hour),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if commandsSent != 0 {
		t.Errorf("tombstoned file should ignore chunk_request, got %d commands", commandsSent)
	}
}

// TestSenderFileDownloadedFromNonRecipientIgnored verifies that a
// file_downloaded from a peer that is not the mapping's Recipient
// is silently ignored.
func TestSenderFileDownloadedFromNonRecipientIgnored(t *testing.T) {
	var acksSent int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			acksSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	legitimateReceiver := domain.PeerIdentity("legitimate-receiver-1234567890")
	attacker := domain.PeerIdentity("attacker-identity-123456789012")
	fileID := domain.FileID("auth-check-file")

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  "hash-auth",
		FileName:  "secret.bin",
		FileSize:  100,
		Recipient: legitimateReceiver,
		State:     senderServing,
		CreatedAt: time.Now(),
	}

	// Attacker tries to send file_downloaded.
	m.HandleFileDownloaded(attacker, domain.FileDownloadedPayload{FileID: fileID})

	if acksSent != 0 {
		t.Errorf("file_downloaded from non-recipient should not trigger ack, got %d", acksSent)
	}
	if m.senderMaps[fileID].State != senderServing {
		t.Errorf("state should remain serving, got %s", m.senderMaps[fileID].State)
	}
}

// ---------------------------------------------------------------------------
// Receiver state machine tests
// ---------------------------------------------------------------------------

// TestReceiverFileDownloadedAckTransitionsToCompleted verifies that
// HandleFileDownloadedAck transitions from WaitingAck → Completed.
func TestReceiverFileDownloadedAckTransitionsToCompleted(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("waiting-ack-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "hash-wait",
		FileName:  "doc.pdf",
		FileSize:  50000,
		Sender:    sender,
		State:     receiverWaitingAck,
		CreatedAt: time.Now(),
	}

	m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID})

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverCompleted {
		t.Errorf("state = %s, want completed", mapping.State)
	}
	if mapping.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
}

// TestReceiverFileDownloadedAckIgnoredInOtherStates verifies that
// HandleFileDownloadedAck is a no-op when the receiver is not in WaitingAck.
func TestReceiverFileDownloadedAckIgnoredInOtherStates(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("wrong-state-file")

	states := []receiverState{
		receiverAvailable,
		receiverDownloading,
		receiverVerifying,
		receiverCompleted,
		receiverFailed,
		receiverWaitingRoute,
	}

	for _, state := range states {
		m.receiverMaps[fileID] = &receiverFileMapping{
			FileID:    fileID,
			FileHash:  "hash",
			Sender:    sender,
			State:     state,
			CreatedAt: time.Now(),
		}

		m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID})

		if m.receiverMaps[fileID].State != state {
			t.Errorf("state %s should not change on ack, got %s", state, m.receiverMaps[fileID].State)
		}
	}
}

// TestReceiverFileDownloadedAckFromWrongSenderIgnored verifies that an ack
// from a peer that is not the mapping's Sender is silently ignored.
func TestReceiverFileDownloadedAckFromWrongSenderIgnored(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	realSender := domain.PeerIdentity("real-sender-identity-1234567890")
	fakeSender := domain.PeerIdentity("fake-sender-identity-1234567890")
	fileID := domain.FileID("wrong-sender-ack")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID: fileID,
		Sender: realSender,
		State:  receiverWaitingAck,
	}

	m.HandleFileDownloadedAck(fakeSender, domain.FileDownloadedAckPayload{FileID: fileID})

	if m.receiverMaps[fileID].State != receiverWaitingAck {
		t.Errorf("ack from wrong sender should not change state, got %s", m.receiverMaps[fileID].State)
	}
}

// TestReceiverWaitingRouteResumesOnPeerReachable verifies that
// tickReceiverMappings transitions a WaitingRoute mapping back to
// Downloading when the sender becomes reachable again.
func TestReceiverWaitingRouteResumesOnPeerReachable(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true // sender is back online
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			chunkRequested = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-who-went-offline-1234567")
	fileID := domain.FileID("waiting-route-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileHash:   "hash-wr",
		FileName:   "resume.bin",
		FileSize:   100000,
		Sender:     sender,
		State:      receiverWaitingRoute,
		CreatedAt:  time.Now(),
		NextOffset: 50000, // partial download
		ChunkSize:  domain.DefaultChunkSize,
	}

	m.tickReceiverMappings()

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverDownloading {
		t.Errorf("state = %s, want downloading (resumed)", mapping.State)
	}
	if !chunkRequested {
		t.Error("chunk_request should be sent on resume from WaitingRoute")
	}
}

// TestReceiverWaitingRouteStaysWhenPeerOffline verifies that WaitingRoute
// mappings remain paused when the sender is still unreachable.
func TestReceiverWaitingRouteStaysWhenPeerOffline(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false // sender still offline
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			t.Error("should not send any command while peer is offline")
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("offline-sender-identity-1234567")
	fileID := domain.FileID("stays-waiting-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		Sender:     sender,
		State:      receiverWaitingRoute,
		NextOffset: 10000,
		ChunkSize:  domain.DefaultChunkSize,
	}

	m.tickReceiverMappings()

	if m.receiverMaps[fileID].State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route", m.receiverMaps[fileID].State)
	}
}

// TestReceiverWaitingAckTimeoutTransitionsToCompleted verifies that after
// exceeding max retries in WaitingAck state, the receiver transitions to
// Completed as a fallback.
func TestReceiverWaitingAckTimeoutTransitionsToCompleted(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("ack-timeout-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 21, // > 20 threshold
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: 0,
	}

	m.tickReceiverMappings()

	if m.receiverMaps[fileID].State != receiverCompleted {
		t.Errorf("state = %s, want completed (fallback after max retries)", m.receiverMaps[fileID].State)
	}
}

// TestReceiverWaitingAckRetryFileDownloaded verifies that tickReceiverMappings
// re-sends file_downloaded with exponential backoff.
func TestReceiverWaitingAckRetryFileDownloaded(t *testing.T) {
	var downloadedSent int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var inner json.RawMessage
			if err := json.Unmarshal(payload.Data, &inner); err == nil {
				downloadedSent++
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("retry-file-downloaded")

	// Backoff expired — should trigger retry.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 2,
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: initialRetryTimeout,
	}

	m.tickReceiverMappings()

	if downloadedSent != 1 {
		t.Errorf("expected 1 file_downloaded retry, got %d", downloadedSent)
	}

	mapping := m.receiverMaps[fileID]
	if mapping.DownloadedRetries != 3 {
		t.Errorf("DownloadedRetries = %d, want 3", mapping.DownloadedRetries)
	}

	// Backoff should double.
	expectedBackoff := initialRetryTimeout * time.Duration(retryBackoffMultiplier)
	if mapping.DownloadedBackoff != expectedBackoff {
		t.Errorf("DownloadedBackoff = %v, want %v", mapping.DownloadedBackoff, expectedBackoff)
	}
}

// TestReceiverChunkResponseFromWrongSenderIgnored verifies that a
// chunk_response from a peer that is not the mapping's Sender is dropped.
func TestReceiverChunkResponseFromWrongSenderIgnored(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	realSender := domain.PeerIdentity("real-sender-identity-1234567890")
	fakeSender := domain.PeerIdentity("fake-sender-identity-1234567890")
	fileID := domain.FileID("wrong-sender-chunk")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "hash",
		FileSize:  10000,
		Sender:    realSender,
		State:     receiverDownloading,
		ChunkSize: domain.DefaultChunkSize,
	}

	m.HandleChunkResponse(fakeSender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   "dGVzdA",
	})

	// State should not change, bytes not advanced.
	if m.receiverMaps[fileID].BytesReceived != 0 {
		t.Error("chunk from wrong sender should not advance BytesReceived")
	}
}

// TestStartDownload_ResumeRollbackPreservesProgress verifies that when a
// transfer resumes from waiting_route with a valid partial file but the first
// chunk_request fails, the rollback restores the original state and offset
// instead of wiping them to zero. This prevents desynchronizing on-disk
// partial data from in-memory state.
func TestStartDownload_ResumeRollbackPreservesProgress(t *testing.T) {
	downloadDir := createPartialDir(t)
	fileID := domain.FileID("resume-rollback")

	// Create a valid partial file so the offset is preserved through
	// the partial file sanity check.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 65536), 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated transient send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      validHash,
		FileName:      "partial.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    65536,
		BytesReceived: 65536,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	err := m.StartDownload(fileID)
	if err == nil {
		t.Fatal("StartDownload should fail when sendCommand fails")
	}

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingRoute {
		t.Errorf("state after rollback = %s, want waiting_route", rm.State)
	}
	if rm.NextOffset != 65536 {
		t.Errorf("NextOffset after rollback = %d, want 65536 (preserved)", rm.NextOffset)
	}
	if rm.BytesReceived != 65536 {
		t.Errorf("BytesReceived after rollback = %d, want 65536 (preserved)", rm.BytesReceived)
	}
}

// TestStartDownload_FreshStartRollbackResetsToZero verifies that when a
// fresh download (from available state) fails on the first chunk_request,
// the rollback correctly resets to available with zero progress.
func TestStartDownload_FreshStartRollbackResetsToZero(t *testing.T) {
	dir := t.TempDir()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("fresh-rollback")
	validHash := "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  validHash,
		FileName:  "fresh.bin",
		FileSize:  100000,
		Sender:    sender,
		State:     receiverAvailable,
		ChunkSize: domain.DefaultChunkSize,
		CreatedAt: time.Now(),
	}

	err := m.StartDownload(fileID)
	if err == nil {
		t.Fatal("StartDownload should fail when sendCommand fails")
	}

	rm := m.receiverMaps[fileID]
	if rm.State != receiverAvailable {
		t.Errorf("state after rollback = %s, want available", rm.State)
	}
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset after rollback = %d, want 0", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived after rollback = %d, want 0", rm.BytesReceived)
	}
}

// TestStartDownload_ResumeFallsBackWhenPartialFileMissing verifies that
// resuming a waiting_route transfer falls back to offset 0 when the partial
// file has been deleted externally, instead of downloading from a hole.
func TestStartDownload_ResumeFallsBackWhenPartialFileMissing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Do NOT create a partial file — simulate external deletion.

	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var req domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &req); err == nil {
				requestedOffset = req.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("missing-partial")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      validHash,
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    65536,
		BytesReceived: 65536,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	if requestedOffset != 0 {
		t.Errorf("requested offset = %d, want 0 (fresh start due to missing partial)", requestedOffset)
	}

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0", rm.BytesReceived)
	}
}

// TestStartDownload_ResumeFallsBackWhenPartialFileTruncated verifies that
// resuming falls back to offset 0 when the partial file is shorter than
// the persisted NextOffset.
func TestStartDownload_ResumeFallsBackWhenPartialFileTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create partial dir and a truncated file (only 1000 bytes, but
	// NextOffset claims 65536).
	partialDir := filepath.Join(dir, "partial")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	partialPath := filepath.Join(partialDir, "truncated-partial.part")
	if err := os.WriteFile(partialPath, make([]byte, 1000), 0o600); err != nil {
		t.Fatal(err)
	}

	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var req domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &req); err == nil {
				requestedOffset = req.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	// Use a file ID that produces the same partial path as our truncated file.
	fileID := domain.FileID("truncated-partial")
	validHash := "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      validHash,
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    65536,
		BytesReceived: 65536,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	if requestedOffset != 0 {
		t.Errorf("requested offset = %d, want 0 (fresh start due to truncated partial)", requestedOffset)
	}

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", rm.NextOffset)
	}
}

// TestStartDownload_RestartFromZeroTruncatesStalePartial verifies that when
// a download restarts from offset 0 (because the partial file was missing or
// truncated), the stale .part file is removed before new chunks arrive.
// Before the fix, writeChunkToFile used WriteAt which only overwrote the
// prefix — stale trailing bytes from a previous larger attempt remained on
// disk, causing hash verification to fail after all correct chunks landed.
func TestStartDownload_RestartFromZeroTruncatesStalePartial(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	partialDir := filepath.Join(dir, "partial")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		t.Fatal(err)
	}

	fileID := domain.FileID("stale-partial-truncate")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	// Create a large stale .part file (simulates a previous failed attempt
	// that got further than where we'll resume from).
	partialPath := partialDownloadPath(dir, fileID)
	staleContent := make([]byte, 8192)
	for i := range staleContent {
		staleContent[i] = 0xDE // recognizable poison byte
	}
	if err := os.WriteFile(partialPath, staleContent, 0o600); err != nil {
		t.Fatal(err)
	}

	// Mapping claims NextOffset=4096 but partial file is 8192 bytes.
	// prepareResumeLocked sees file is at least NextOffset bytes → valid resume.
	// But we're testing a fresh StartDownload from receiverAvailable which
	// resets offset to 0 — the stale file MUST be cleaned up.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(_ domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  validHash,
		FileName:  "test.bin",
		FileSize:  100000,
		Sender:    sender,
		State:     receiverAvailable,
		ChunkSize: domain.DefaultChunkSize,
		CreatedAt: time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	// The stale partial file should have been removed.
	if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
		t.Fatalf("stale .part file should be removed on restart from offset 0, stat err: %v", err)
	}
}

// TestStartDownload_ResumeSucceedsWhenPartialFileValid verifies that
// resuming proceeds from the persisted offset when the partial file
// exists and has sufficient size.
func TestStartDownload_ResumeSucceedsWhenPartialFileValid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create partial dir and a file with exactly NextOffset bytes.
	partialDir := filepath.Join(dir, "partial")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	fileID := domain.FileID("valid-partial")
	partialPath := partialDownloadPath(dir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 65536), 0o600); err != nil {
		t.Fatal(err)
	}

	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var req domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &req); err == nil {
				requestedOffset = req.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	validHash := "c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8f1f2f3f4f5f6f7f8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      validHash,
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    65536,
		BytesReceived: 65536,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	if requestedOffset != 65536 {
		t.Errorf("requested offset = %d, want 65536 (resume from persisted offset)", requestedOffset)
	}

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 65536 {
		t.Errorf("NextOffset = %d, want 65536 (preserved)", rm.NextOffset)
	}
	if rm.BytesReceived != 65536 {
		t.Errorf("BytesReceived = %d, want 65536 (preserved)", rm.BytesReceived)
	}
}

// TestStartDownload_OversizedPartialFileResetsOffset verifies that when the
// .part file on disk is larger than the announced FileSize, prepareResumeLocked
// resets the offset to 0 instead of trusting the corrupted partial size.
func TestStartDownload_OversizedPartialFileResetsOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	partialDir := filepath.Join(dir, "partial")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		t.Fatal(err)
	}

	fileID := domain.FileID("oversized-partial")
	const fileSize uint64 = 50000

	// Create a .part file larger than FileSize — corruption scenario.
	partialPath := partialDownloadPath(dir, fileID)
	oversizedData := make([]byte, fileSize+10000)
	if err := os.WriteFile(partialPath, oversizedData, 0o600); err != nil {
		t.Fatal(err)
	}

	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var req domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &req); err == nil {
				requestedOffset = req.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	validHash := "f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8"

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      validHash,
		FileName:      "test.bin",
		FileSize:      fileSize,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    fileSize, // claims fully downloaded
		BytesReceived: fileSize,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	if err := m.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}

	// Offset must be reset to 0 — the oversized partial cannot be trusted.
	if requestedOffset != 0 {
		t.Errorf("requested offset = %d, want 0 (oversized partial must reset)", requestedOffset)
	}

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0", rm.BytesReceived)
	}
}

// ---------------------------------------------------------------------------
// tickReceiverMappings tests
// ---------------------------------------------------------------------------

// TestRetryPendingDownloads_RollsBackOnSendFailure verifies that when
// tickReceiverMappings auto-resumes a waitingRoute transfer but the
// requestNextChunk call fails, the mapping is rolled back to waitingRoute
// instead of being stranded in downloading with no request in flight.
func TestRetryPendingDownloads_RollsBackOnSendFailure(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("retry-rollback-file")

	// Create a valid partial file so the offset is preserved through
	// the partial file sanity check.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 32768), 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated network failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    32768,
		BytesReceived: 32768,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route (rolled back after send failure)", rm.State)
	}
	if rm.NextOffset != 32768 {
		t.Errorf("NextOffset = %d, want 32768 (preserved)", rm.NextOffset)
	}
	if rm.BytesReceived != 32768 {
		t.Errorf("BytesReceived = %d, want 32768 (preserved)", rm.BytesReceived)
	}
}

// TestRetryPendingDownloads_SuccessfulResumeStaysDownloading verifies the
// happy path: when requestNextChunk succeeds, the mapping remains in
// downloading state after tickReceiverMappings completes.
func TestRetryPendingDownloads_SuccessfulResumeStaysDownloading(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("retry-success-file")

	// Create a valid partial file so offset is preserved.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 32768), 0o644); err != nil {
		t.Fatalf("write partial file: %v", err)
	}

	sendCalled := false
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sendCalled = true
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    32768,
		BytesReceived: 32768,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverDownloading {
		t.Errorf("state = %s, want downloading (send succeeded)", rm.State)
	}
	if !sendCalled {
		t.Error("sendCommand was not called, expected chunk request to be sent")
	}
}

// TestDeferredResumeSkippedAfterCancel verifies that when a transfer is
// cancelled between the tick snapshot (under lock) and the deferred action
// execution (outside lock), the resume action is skipped — no chunk_request
// is sent and no partial file is deleted. This tests the receiverStateIs
// guard added to the deferred action loop.
func TestDeferredResumeSkippedAfterCancel(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("cancel-race-file")

	// Create a valid partial file.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 16384), 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}

	chunkRequestSent := false
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			chunkRequestSent = true
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    16384,
		BytesReceived: 16384,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	// Simulate the race: prepareResumeLocked transitions to downloading,
	// then CancelDownload resets to available before the deferred action runs.
	// We do this by directly calling the phases that tickReceiverMappings does.

	// Phase 1: snapshot under lock (mimics what tick does).
	m.mu.Lock()
	rm := m.receiverMaps[fileID]
	snap := m.prepareResumeLocked(fileID, rm)
	m.saveMappingsLocked()
	m.mu.Unlock()

	// Phase 2: concurrent cancel between unlock and deferred execution.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}

	// Phase 3: deferred execution — should be skipped by receiverStateIs.
	// The original mapping had generation 0; CancelDownload increments it,
	// so both state and generation mismatch.
	if m.receiverStateIs(fileID, receiverDownloading, 0) {
		t.Fatal("receiverStateIs should return false after cancel")
	}

	// Verify no chunk_request was sent and partial file was already
	// cleaned by CancelDownload (not by the deferred action).
	_ = snap // snap would have been used by truncatePartialFile + sendChunkWithRollback
	if chunkRequestSent {
		t.Error("chunk_request should not be sent after cancel")
	}
}

// TestDeferredActionStateGuard_SkipsOnMismatch verifies that the structural
// requiredState field on receiverTickAction prevents execution when the mapping
// has transitioned to a different state between snapshot and dispatch.
func TestDeferredActionStateGuard_SkipsOnMismatch(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("guard-mismatch")

	var actionExecuted bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			actionExecuted = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-guard-test")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "aaaa",
		FileName:  "test.bin",
		FileSize:  10000,
		Sender:    sender,
		State:     receiverFailed, // actual state
		ChunkSize: domain.DefaultChunkSize,
		CreatedAt: time.Now(),
	}

	// Action was created when state was receiverDownloading, but since then
	// the mapping moved to receiverFailed. The guard must skip it.
	action := receiverTickAction{
		kind:          actionRetryChunk,
		requiredState: receiverDownloading, // does NOT match actual state
		generation:    0,
		fileID:        fileID,
		sender:        sender,
		offset:        0,
		chunkSize:     domain.DefaultChunkSize,
	}

	// Simulate the dispatch loop.
	if !m.receiverStateIs(action.fileID, action.requiredState, action.generation) {
		// Guard correctly skipped — this is expected.
	} else {
		m.executeReceiverAction(action)
	}

	if actionExecuted {
		t.Error("action should not execute when requiredState does not match actual state")
	}
}

// TestDeferredActionStateGuard_ExecutesOnMatch verifies that when
// requiredState matches the mapping's actual state, the action executes.
func TestDeferredActionStateGuard_ExecutesOnMatch(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("guard-match")

	var ackSent bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			ackSent = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-guard-match")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "bbbb",
		FileName:      "test.bin",
		FileSize:      10000,
		Sender:        sender,
		State:         receiverWaitingAck, // matches requiredState
		ChunkSize:     domain.DefaultChunkSize,
		CompletedPath: filepath.Join(downloadDir, "test.bin"),
		CreatedAt:     time.Now(),
	}

	action := receiverTickAction{
		kind:          actionRetryFileDownloaded,
		requiredState: receiverWaitingAck,
		generation:    0,
		fileID:        fileID,
		sender:        sender,
	}

	// Simulate the dispatch loop.
	if m.receiverStateIs(action.fileID, action.requiredState, action.generation) {
		m.executeReceiverAction(action)
	}

	if !ackSent {
		t.Error("action should execute when requiredState matches actual state")
	}
}

// TestDeferredCleanupSkippedAfterReRegister verifies that a stale
// actionCleanupFailed from a previous generation does not delete the .part
// file of a freshly re-registered and restarted transfer for the same fileID.
// This is the regression test for the P2 "deferred cleanup deletes freshly
// restarted .part file" bug.
func TestDeferredCleanupSkippedAfterReRegister(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("cleanup-reregister")

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-cleanup-regen")

	// Phase 1: create a failed mapping (generation 0) and capture a
	// deferred cleanup action from the tick snapshot.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "aaaa",
		FileName:  "test.bin",
		FileSize:  100000,
		Sender:    sender,
		State:     receiverFailed,
		ChunkSize: domain.DefaultChunkSize,
		CreatedAt: time.Now(),
		// Generation deliberately 0 (default).
	}

	staleAction := receiverTickAction{
		kind:          actionCleanupFailed,
		requiredState: receiverFailed,
		generation:    0, // captured from the old mapping
		fileID:        fileID,
	}

	// Phase 2: simulate CleanupPeerTransfers removing the failed mapping,
	// then a new RegisterFileReceive + StartDownload creating a fresh
	// .part file. We simulate this by deleting and re-inserting with a
	// new generation.
	delete(m.receiverMaps, fileID)
	m.nextGeneration++
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileHash:   "bbbb",
		FileName:   "test.bin",
		FileSize:   200000,
		Sender:     sender,
		State:      receiverDownloading,
		ChunkSize:  domain.DefaultChunkSize,
		CreatedAt:  time.Now(),
		Generation: m.nextGeneration,
	}

	// Create the fresh .part file that the new download is writing to.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}

	// Phase 3: the stale cleanup action runs — it must be skipped because
	// the generation no longer matches.
	if m.receiverStateIs(staleAction.fileID, staleAction.requiredState, staleAction.generation) {
		t.Fatal("receiverStateIs should return false: state is downloading, not failed, and generation changed")
	}

	// Verify .part file survived.
	if _, err := os.Stat(partialPath); os.IsNotExist(err) {
		t.Error("partial file was deleted by stale cleanup — generation guard failed")
	}
}

// TestVerifyCleanupSkippedAfterCancelAndRestart verifies that a stale
// onDownloadComplete verifier does not delete the .part file of a freshly
// restarted transfer for the same fileID. The race:
//  1. onDownloadComplete transitions mapping to receiverVerifying, captures generation
//  2. User cancels (bumps generation) and restarts download → new .part file
//  3. Old verifier's integrity check fails → markReceiverFailed skips (state mismatch)
//  4. Old verifier's removePartialIfOwned must also skip (generation mismatch)
func TestVerifyCleanupSkippedAfterCancelAndRestart(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("verify-cancel-restart")
	sender := domain.PeerIdentity("sender-verify-cancel")

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	// Phase 1: simulate onDownloadComplete capturing generation under lock
	// and transitioning to receiverVerifying.
	m.nextGeneration++
	oldGeneration := m.nextGeneration
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:     fileID,
		FileHash:   "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
		FileName:   "test.bin",
		FileSize:   100000,
		Sender:     sender,
		State:      receiverVerifying,
		ChunkSize:  domain.DefaultChunkSize,
		CreatedAt:  time.Now(),
		Generation: oldGeneration,
	}

	// Phase 2: user cancels and immediately starts a new download.
	// CancelDownload resets state and bumps generation.
	m.nextGeneration++
	newGeneration := m.nextGeneration
	m.receiverMaps[fileID].State = receiverDownloading
	m.receiverMaps[fileID].Generation = newGeneration
	m.receiverMaps[fileID].BytesReceived = 4096
	m.receiverMaps[fileID].NextOffset = 4096

	// New .part file being written by the restarted download.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}

	// Phase 3: the old verifier's integrity check failed, so it calls
	// markReceiverFailed + removePartialIfOwned. Simulate this by
	// checking receiverStateIs with the OLD generation.
	//
	// markReceiverFailed would check mapping.State != receiverVerifying
	// (it's receiverDownloading now), so it skips the transition.
	// removePartialIfOwned checks receiverStateIs(fileID, receiverFailed, oldGeneration)
	// which returns false because state is receiverDownloading and generation differs.

	if m.receiverStateIs(fileID, receiverFailed, oldGeneration) {
		t.Fatal("receiverStateIs should return false: state is downloading with new generation")
	}

	// Also verify that even if the state were receiverFailed, the generation mismatch
	// would still prevent cleanup.
	m.receiverMaps[fileID].State = receiverFailed
	if m.receiverStateIs(fileID, receiverFailed, oldGeneration) {
		t.Fatal("receiverStateIs should return false: generation mismatch despite matching state")
	}

	// Restore to downloading for the final check.
	m.receiverMaps[fileID].State = receiverDownloading

	// Verify .part file survived — the stale verifier's cleanup was skipped.
	if _, err := os.Stat(partialPath); os.IsNotExist(err) {
		t.Fatal("partial file was deleted by stale verifier — generation guard failed")
	}
}

// TestMarkReceiverFailedRejectsStaleGeneration verifies that markReceiverFailed
// refuses to transition a mapping to receiverFailed when the caller holds an
// outdated generation. This closes the race where a stale verifier goroutine
// (from a cancelled download) tries to mark the mapping as failed after the
// user has cancelled and restarted the same fileID — the restart bumps the
// generation, so the stale goroutine's generation no longer matches.
func TestMarkReceiverFailedRejectsStaleGeneration(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("gen-race-mark-failed")

	// Simulate: first download reached receiverVerifying with generation 1.
	staleGeneration := uint64(1)
	// User cancelled and restarted — new attempt is also in receiverVerifying
	// but with generation 2.
	currentGeneration := uint64(2)

	rm := &receiverFileMapping{
		FileID:     fileID,
		FileHash:   "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
		FileName:   "test.bin",
		FileSize:   2048,
		Sender:     domain.PeerIdentity("sender-gen-race-1234567890abcd"),
		State:      receiverVerifying,
		CreatedAt:  time.Now(),
		Generation: currentGeneration,
	}
	m.receiverMaps[fileID] = rm

	// Stale verifier calls markReceiverFailed with old generation.
	m.markReceiverFailed(rm, receiverVerifying, staleGeneration)

	// State must remain receiverVerifying — the stale goroutine was rejected.
	if rm.State != receiverVerifying {
		t.Fatalf("state = %s, want receiverVerifying; stale generation should be rejected", rm.State)
	}

	// Current generation must succeed.
	m.markReceiverFailed(rm, receiverVerifying, currentGeneration)

	if rm.State != receiverFailed {
		t.Fatalf("state = %s, want receiverFailed; current generation should be accepted", rm.State)
	}
}

// TestRetryPendingDownloads_RollbackDoesNotAffectOtherWaitingRoute verifies
// that when one waitingRoute transfer is resumed and fails, other
// waitingRoute transfers remain untouched (they weren't picked up due to
// the concurrent download limit of 1).
func TestRetryPendingDownloads_RollbackDoesNotAffectOtherWaitingRoute(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)

	// Create valid partial files for both transfers.
	for _, fid := range []domain.FileID{"wr-file-a", "wr-file-b"} {
		pp := partialDownloadPath(downloadDir, fid)
		if err := os.WriteFile(pp, make([]byte, 16384), 0o644); err != nil {
			t.Fatalf("write partial file %s: %v", fid, err)
		}
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated network failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	// Two waitingRoute transfers. With maxConcurrentDownloads=1, only
	// one should be picked. After rollback, both should be waitingRoute.
	for _, fid := range []domain.FileID{"wr-file-a", "wr-file-b"} {
		m.receiverMaps[fid] = &receiverFileMapping{
			FileID:        fid,
			FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
			FileName:      "test.bin",
			FileSize:      100000,
			Sender:        sender,
			State:         receiverWaitingRoute,
			NextOffset:    16384,
			BytesReceived: 16384,
			ChunkSize:     domain.DefaultChunkSize,
			CreatedAt:     time.Now(),
		}
	}

	m.tickReceiverMappings()

	for _, fid := range []domain.FileID{"wr-file-a", "wr-file-b"} {
		rm := m.receiverMaps[fid]
		if rm.State != receiverWaitingRoute {
			t.Errorf("%s: state = %s, want waiting_route", fid, rm.State)
		}
	}
}

// ---------------------------------------------------------------------------
// tickReceiverMappings partial file validation tests
// ---------------------------------------------------------------------------

// createPartialDir creates the partial/ subdirectory under downloadDir and
// returns the base downloadDir path. Helper for tests that exercise the
// partial file sanity check.
func createPartialDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "partial"), 0o755); err != nil {
		t.Fatalf("create partial dir: %v", err)
	}
	return dir
}

// TestRetryPendingDownloads_ResetsOffsetWhenPartialFileMissing verifies that
// auto-resume resets NextOffset and BytesReceived to 0 when the .part file
// does not exist, instead of requesting from a hole.
func TestRetryPendingDownloads_ResetsOffsetWhenPartialFileMissing(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var cr domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &cr); err == nil {
				requestedOffset = cr.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("partial-missing")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    32768,
		BytesReceived: 32768,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}
	// No .part file created — it is missing.

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverDownloading {
		t.Fatalf("state = %s, want downloading", rm.State)
	}
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0 (reset because partial file missing)", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0 (reset because partial file missing)", rm.BytesReceived)
	}
	if requestedOffset != 0 {
		t.Errorf("requested offset = %d, want 0 (restart from beginning)", requestedOffset)
	}
}

// TestRetryPendingDownloads_ResetsOffsetWhenPartialFileTruncated verifies that
// auto-resume resets to offset 0 when the .part file exists but is shorter
// than the persisted NextOffset.
func TestRetryPendingDownloads_ResetsOffsetWhenPartialFileTruncated(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("partial-truncated")

	// Create a truncated partial file (1000 bytes when NextOffset is 32768).
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 1000), 0o644); err != nil {
		t.Fatalf("write truncated partial: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    32768,
		BytesReceived: 32768,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0 (reset because partial file truncated)", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0 (reset because partial file truncated)", rm.BytesReceived)
	}
}

// TestRetryPendingDownloads_PreservesOffsetWhenPartialFileValid verifies that
// auto-resume keeps the persisted offset when the .part file is large enough.
func TestRetryPendingDownloads_PreservesOffsetWhenPartialFileValid(t *testing.T) {
	t.Parallel()

	downloadDir := createPartialDir(t)
	fileID := domain.FileID("partial-valid")

	// Create a valid partial file (>= NextOffset bytes).
	partialPath := partialDownloadPath(downloadDir, fileID)
	if err := os.WriteFile(partialPath, make([]byte, 32768), 0o644); err != nil {
		t.Fatalf("write valid partial: %v", err)
	}

	var requestedOffset uint64
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			var cr domain.ChunkRequestPayload
			if err := json.Unmarshal(payload.Data, &cr); err == nil {
				requestedOffset = cr.Offset
			}
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    32768,
		BytesReceived: 32768,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.NextOffset != 32768 {
		t.Errorf("NextOffset = %d, want 32768 (preserved, partial file valid)", rm.NextOffset)
	}
	if rm.BytesReceived != 32768 {
		t.Errorf("BytesReceived = %d, want 32768 (preserved, partial file valid)", rm.BytesReceived)
	}
	if requestedOffset != 32768 {
		t.Errorf("requested offset = %d, want 32768 (resume from persisted offset)", requestedOffset)
	}
}

// TestRetryPendingDownloads_ZeroOffsetSkipsPartialCheck verifies that when
// NextOffset is 0 (fresh download that was paused before any data arrived),
// no partial file check is needed and the resume proceeds normally.
func TestRetryPendingDownloads_ZeroOffsetSkipsPartialCheck(t *testing.T) {
	t.Parallel()

	// No downloadDir needed — check should be skipped entirely.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("zero-offset-file")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2",
		FileName:      "test.bin",
		FileSize:      100000,
		Sender:        sender,
		State:         receiverWaitingRoute,
		NextOffset:    0,
		BytesReceived: 0,
		ChunkSize:     domain.DefaultChunkSize,
		CreatedAt:     time.Now(),
	}

	m.tickReceiverMappings()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverDownloading {
		t.Errorf("state = %s, want downloading", rm.State)
	}
}

// ---------------------------------------------------------------------------
// Cancel download tests
// ---------------------------------------------------------------------------

// TestCancelDownloadRejectsWaitingAck verifies that CancelDownload refuses to
// reset a transfer in receiverWaitingAck. In this state the receiver has
// already sent file_downloaded and the sender may have transitioned to
// senderCompleted (releasing the transmit file). Resetting to available would
// advertise a re-download the protocol cannot fulfill.
func TestCancelDownloadRejectsWaitingAck(t *testing.T) {
	dir := t.TempDir()
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  dir,
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("ack-pending-file")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName:      "report.pdf",
		FileSize:      1024,
		Sender:        domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:         receiverWaitingAck,
		CompletedPath: filepath.Join(dir, "report.pdf"),
		CreatedAt:     time.Now(),
	}

	err := m.CancelDownload(fileID)
	if err == nil {
		t.Fatal("CancelDownload should reject waitingAck state")
	}

	// State must remain unchanged.
	if m.receiverMaps[fileID].State != receiverWaitingAck {
		t.Errorf("state = %s, want waiting_ack", m.receiverMaps[fileID].State)
	}
}

// TestCancelDownloadAllowedStates verifies that CancelDownload succeeds for
// each of the permitted in-progress states.
func TestCancelDownloadAllowedStates(t *testing.T) {
	allowedStates := []receiverState{
		receiverDownloading,
		receiverVerifying,
		receiverWaitingRoute,
	}

	for _, state := range allowedStates {
		t.Run(string(state), func(t *testing.T) {
			dir := t.TempDir()
			m := &Manager{
				senderMaps:   make(map[domain.FileID]*senderFileMapping),
				receiverMaps: make(map[domain.FileID]*receiverFileMapping),
				downloadDir:  dir,
				stopCh:       make(chan struct{}),
			}

			fileID := domain.FileID("cancel-" + string(state))
			m.receiverMaps[fileID] = &receiverFileMapping{
				FileID:    fileID,
				FileHash:  "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				FileName:  "test.bin",
				FileSize:  1024,
				Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
				State:     state,
				CreatedAt: time.Now(),
			}

			if err := m.CancelDownload(fileID); err != nil {
				t.Fatalf("CancelDownload should allow state %s: %v", state, err)
			}

			if m.receiverMaps[fileID].State != receiverAvailable {
				t.Errorf("state = %s, want available", m.receiverMaps[fileID].State)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RestartDownload tests
// ---------------------------------------------------------------------------

// TestRestartDownloadFromFailed verifies that RestartDownload resets a failed
// receiver mapping to available state with zeroed progress and bumped generation.
func TestRestartDownloadFromFailed(t *testing.T) {
	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	fileID := domain.FileID("restart-test-file")
	sender := domain.PeerIdentity("restart-sender-identity-1234")

	// Set the manager's generation counter to match the mapping so the
	// bump assertion is meaningful.
	m.nextGeneration = 5
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName:      "restart.txt",
		FileSize:      4096,
		Sender:        sender,
		State:         receiverFailed,
		BytesReceived: 2048,
		NextOffset:    2048,
		ChunkRetries:  3,
		Generation:    5,
		CompletedPath: "/old/partial/path",
		CreatedAt:     time.Now().Add(-time.Hour),
	}

	if err := m.RestartDownload(fileID); err != nil {
		t.Fatalf("RestartDownload failed: %v", err)
	}

	rm := m.receiverMaps[fileID]
	if rm.State != receiverAvailable {
		t.Errorf("state = %s, want %s", rm.State, receiverAvailable)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0", rm.BytesReceived)
	}
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", rm.NextOffset)
	}
	if rm.ChunkRetries != 0 {
		t.Errorf("ChunkRetries = %d, want 0", rm.ChunkRetries)
	}
	if rm.CompletedPath != "" {
		t.Errorf("CompletedPath = %q, want empty", rm.CompletedPath)
	}
	if rm.Generation <= 5 {
		t.Errorf("Generation = %d, want > 5 (should be bumped)", rm.Generation)
	}
}

// TestRestartDownloadRejectsNonFailed verifies that RestartDownload refuses
// to reset mappings in states other than failed.
func TestRestartDownloadRejectsNonFailed(t *testing.T) {
	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	sender := domain.PeerIdentity("restart-reject-sender-1234")

	nonFailedStates := []receiverState{
		receiverAvailable, receiverDownloading, receiverVerifying,
		receiverWaitingAck, receiverCompleted, receiverWaitingRoute,
	}

	for _, state := range nonFailedStates {
		t.Run(string(state), func(t *testing.T) {
			fileID := domain.FileID("restart-reject-" + string(state))
			m.receiverMaps[fileID] = &receiverFileMapping{
				FileID:    fileID,
				Sender:    sender,
				State:     state,
				CreatedAt: time.Now(),
			}

			err := m.RestartDownload(fileID)
			if err == nil {
				t.Fatalf("RestartDownload should reject state %s", state)
			}
			if m.receiverMaps[fileID].State != state {
				t.Errorf("state changed from %s to %s after rejected restart",
					state, m.receiverMaps[fileID].State)
			}
		})
	}
}

// TestRestartDownloadMissingMapping verifies that RestartDownload returns
// an error for unknown file IDs.
func TestRestartDownloadMissingMapping(t *testing.T) {
	downloadDir := testDownloadDir(t)
	m := newTestTransferManager(t, downloadDir, nil)

	err := m.RestartDownload(domain.FileID("nonexistent-file"))
	if err == nil {
		t.Fatal("RestartDownload should fail for nonexistent file ID")
	}
}

// ---------------------------------------------------------------------------
// Stale serving slot reclamation
// ---------------------------------------------------------------------------

// TestReclaimStalledServingSlots verifies that senderServing mappings with no
// chunk activity for longer than senderServingStallTimeout are reverted back
// to senderAnnounced, freeing the serving slot.
func TestReclaimStalledServingSlots(t *testing.T) {
	dir := t.TempDir()

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: filepath.Join(dir, "transfers.json"),
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	recipient := domain.PeerIdentity("receiver-identity-1234567890ab")

	// Stale entry: LastServedAt well past the timeout.
	staleID := domain.FileID("stale-serving")
	m.senderMaps[staleID] = &senderFileMapping{
		FileID:       staleID,
		FileHash:     "aabb",
		FileName:     "stale.bin",
		FileSize:     100,
		Recipient:    recipient,
		State:        senderServing,
		CreatedAt:    time.Now().Add(-20 * time.Minute),
		LastServedAt: time.Now().Add(-15 * time.Minute),
	}

	// Fresh entry: LastServedAt within the timeout.
	freshID := domain.FileID("fresh-serving")
	m.senderMaps[freshID] = &senderFileMapping{
		FileID:       freshID,
		FileHash:     "ccdd",
		FileName:     "fresh.bin",
		FileSize:     200,
		Recipient:    recipient,
		State:        senderServing,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
		LastServedAt: time.Now().Add(-1 * time.Minute),
	}

	// Non-serving entry: should not be touched.
	announcedID := domain.FileID("announced-entry")
	m.senderMaps[announcedID] = &senderFileMapping{
		FileID:       announcedID,
		FileHash:     "eeff",
		FileName:     "announced.bin",
		FileSize:     300,
		Recipient:    recipient,
		State:        senderAnnounced,
		CreatedAt:    time.Now().Add(-30 * time.Minute),
		LastServedAt: time.Now().Add(-30 * time.Minute),
	}

	m.tickSenderMappings()

	// Stale entry should revert to announced.
	if m.senderMaps[staleID].State != senderAnnounced {
		t.Errorf("stale entry: state = %s, want announced", m.senderMaps[staleID].State)
	}

	// Fresh entry should remain serving.
	if m.senderMaps[freshID].State != senderServing {
		t.Errorf("fresh entry: state = %s, want serving", m.senderMaps[freshID].State)
	}

	// Announced entry should stay announced.
	if m.senderMaps[announcedID].State != senderAnnounced {
		t.Errorf("announced entry: state = %s, want announced", m.senderMaps[announcedID].State)
	}
}

// TestReclaimStalledServingSlotsPersistence verifies that the LastServedAt
// field survives a save/load cycle, so the stall detection works correctly
// after a node restart.
func TestReclaimStalledServingSlotsPersistence(t *testing.T) {
	dir := t.TempDir()

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	mappingsPath := filepath.Join(dir, "transfers.json")
	lastServed := time.Now().Add(-15 * time.Minute)
	fileID := domain.FileID("persist-stale")

	// Save a serving mapping with an old LastServedAt.
	m1 := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}
	validHash := sha256Hex([]byte("persist-stale-content"))

	// Create the transmit file on disk so startup reconciliation does not
	// tombstone the mapping (active senders with missing blobs are now
	// detected and tombstoned on load).
	if err := os.WriteFile(filepath.Join(dir, validHash+".bin"), []byte("persist-stale-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	m1.senderMaps[fileID] = &senderFileMapping{
		FileID:       fileID,
		FileHash:     validHash,
		FileName:     "persist.bin",
		FileSize:     512,
		Recipient:    domain.PeerIdentity("receiver-identity-1234567890ab"),
		State:        senderServing,
		CreatedAt:    time.Now().Add(-20 * time.Minute),
		LastServedAt: lastServed,
	}

	m1.mu.Lock()
	m1.saveMappingsLocked()
	m1.mu.Unlock()

	// Reload into a new manager.
	m2 := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		mappingsPath: mappingsPath,
		downloadDir:  dir,
		stopCh:       make(chan struct{}),
	}
	m2.loadMappings()

	sm, ok := m2.senderMaps[fileID]
	if !ok {
		t.Fatal("sender mapping not found after reload")
	}
	if sm.State != senderServing {
		t.Fatalf("state after reload = %s, want serving", sm.State)
	}

	// The persisted LastServedAt should round-trip correctly (within 1s tolerance).
	diff := sm.LastServedAt.Sub(lastServed)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("LastServedAt drift = %v, want < 1s", diff)
	}
}

// ---------------------------------------------------------------------------
// waiting_ack retry counter advancement with offline sender
// ---------------------------------------------------------------------------

// TestWaitingAckAdvancesRetryCounterWhenSenderOffline verifies that
// tickReceiverMappings increments DownloadedRetries and advances backoff
// even when the sender is unreachable. Before the fix, the early `continue`
// on peerReachable==false skipped the counter entirely, leaving the transfer
// stuck in waiting_ack forever.
func TestWaitingAckAdvancesRetryCounterWhenSenderOffline(t *testing.T) {
	var downloadedSent int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false // sender always offline
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			downloadedSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("offline-ack-retry")

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 5,
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: initialRetryTimeout,
	}

	m.tickReceiverMappings()

	// Counter must have advanced even though sender is offline.
	mapping := m.receiverMaps[fileID]
	if mapping.DownloadedRetries != 6 {
		t.Errorf("DownloadedRetries = %d, want 6", mapping.DownloadedRetries)
	}

	// Backoff must have doubled.
	expectedBackoff := initialRetryTimeout * time.Duration(retryBackoffMultiplier)
	if mapping.DownloadedBackoff != expectedBackoff {
		t.Errorf("DownloadedBackoff = %v, want %v", mapping.DownloadedBackoff, expectedBackoff)
	}

	// No actual send should have occurred — sender is offline.
	if downloadedSent != 0 {
		t.Errorf("expected 0 sends (sender offline), got %d", downloadedSent)
	}

	// State must remain waiting_ack (not yet exhausted).
	if mapping.State != receiverWaitingAck {
		t.Errorf("state = %s, want waiting_ack", mapping.State)
	}
}

// TestWaitingAckCompletesLocallyAfterMaxRetriesWithOfflineSender verifies
// that after exhausting all retries (>20) the mapping transitions to
// receiverCompleted even when the sender has been offline for every tick.
func TestWaitingAckCompletesLocallyAfterMaxRetriesWithOfflineSender(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("offline-ack-exhaust")

	// Set retries to 20 — next tick should push it to 21 and trigger completion.
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 20,
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: maxRetryTimeout,
		CompletedPath:     "/some/completed/file.bin",
	}

	m.tickReceiverMappings()

	mapping := m.receiverMaps[fileID]
	if mapping.State != receiverCompleted {
		t.Fatalf("state = %s, want completed", mapping.State)
	}
	if mapping.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
}
