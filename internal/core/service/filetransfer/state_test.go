package filetransfer

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-1234567890ab")
	fileID := domain.FileID("lifecycle-file-id")

	// Start in Announced.
	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:       ftOutgoingRef(m, fileID, receiver),
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

	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != senderServing {
		t.Fatalf("after chunk_request: state = %s, want serving", m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
	}
	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sentPayloads))
	}
	if sentPayloads[0].Command != domain.FileActionChunkResp {
		t.Errorf("expected chunk_response command, got %s", sentPayloads[0].Command)
	}
	var served domain.ChunkResponsePayload
	if err := json.Unmarshal(sentPayloads[0].Data, &served); err != nil {
		t.Fatalf("unmarshal chunk_response: %v", err)
	}

	// Completed mapping holds a ref — set it to match what Acquire produces.
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()

	// 2. HandleFileDownloaded → transitions to Completed and sends ack.
	// The ref is NOT released — the blob stays on disk for re-downloads.
	// The receiver echoes the epoch its chunk_response carried.
	sentPayloads = nil
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: served.Epoch})

	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != senderCompleted {
		t.Fatalf("after file_downloaded: state = %s, want completed", m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
	}
	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].CompletedAt.IsZero() {
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

	receiver := domaintest.ID("receiver-identity-1234567890ab")
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:         ftOutgoingRef(m, fileID, receiver),
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

	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != senderServing {
		t.Fatalf("after chunk_request on completed: state = %s, want serving", m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
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

	receiver := domaintest.ID("receiver-identity-1234567890ab")
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:         ftOutgoingRef(m, fileID, receiver),
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

	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != senderServing {
		t.Fatalf("after chunk_request on tombstone with blob: state = %s, want serving", m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
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

	receiver := domaintest.ID("receiver-identity-1234567890ab")
	fileID := domain.FileID("tombstone-no-blob")

	var sentPayloads []domain.FileCommandPayload
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentPayloads = append(sentPayloads, payload)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:         ftOutgoingRef(m, fileID, receiver),
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

	if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != senderTombstone {
		t.Fatalf("tombstone without blob: state = %s, want tombstone", m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-1234567890ab")
	fileID := domain.FileID("idempotent-file")
	completedAt := time.Now().Add(-time.Hour)

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:          ftOutgoingRef(m, fileID, receiver),
		FileID:       fileID,
		FileHash:     "hash-x",
		FileName:     "file.bin",
		FileSize:     100,
		Recipient:    receiver,
		State:        senderCompleted,
		CompletedAt:  completedAt,
		ServingEpoch: 3,
	}

	var acksSent int
	m.sendCommand = func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
		acksSent++
		return nil
	}

	// Send duplicate file_downloaded of the completed run.
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: 3})

	// Should re-send ack.
	if acksSent != 1 {
		t.Errorf("expected 1 ack re-sent for duplicate file_downloaded, got %d", acksSent)
	}

	// CompletedAt should NOT be updated (idempotent).
	if !m.senderMaps[ftOutgoingRef(m, fileID, receiver)].CompletedAt.Equal(completedAt) {
		t.Error("CompletedAt should not change on duplicate file_downloaded")
	}
}

// TestSenderTombstonedFileIDIgnoresChunkRequest verifies that a chunk_request
// for a tombstoned FileID is silently ignored (no chunk_response sent).
func TestSenderTombstonedFileIDIgnoresChunkRequest(t *testing.T) {
	var commandsSent int
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			commandsSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-1234567890ab")
	fileID := domain.FileID("tombstoned-file")

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:         ftOutgoingRef(m, fileID, receiver),
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			acksSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	legitimateReceiver := domaintest.ID("legitimate-receiver-1234567890")
	attacker := domaintest.ID("attacker-identity-123456789012")
	fileID := domain.FileID("auth-check-file")

	m.senderMaps[ftOutgoingRef(m, fileID, legitimateReceiver)] = &senderFileMapping{
		Ref:       ftOutgoingRef(m, fileID, legitimateReceiver),
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
	if m.senderMaps[ftOutgoingRef(m, fileID, legitimateReceiver)].State != senderServing {
		t.Errorf("state should remain serving, got %s", m.senderMaps[ftOutgoingRef(m, fileID, legitimateReceiver)].State)
	}
}

// TestSenderFileDownloadedRejectedOutsideServingRun verifies that
// HandleFileDownloaded rejects file_downloaded when the mapping is in
// senderAnnounced or senderTombstone. Only senderServing (normal
// completion) and senderCompleted (idempotent re-ack) should accept.
func TestSenderFileDownloadedRejectedOutsideServingRun(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state senderState
	}{
		{"announced", senderAnnounced},
		{"tombstone", senderTombstone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var acksSent int
			m := &Manager{
				localID:      ftFixtureIdentity(),
				senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
				receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
				sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
					acksSent++
					return nil
				},
				stopCh: make(chan struct{}),
			}

			receiver := domaintest.ID("receiver-state-guard-01234")
			fileID := domain.FileID("state-guard-file")

			m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
				Ref:       ftOutgoingRef(m, fileID, receiver),
				FileID:    fileID,
				FileHash:  "hash-state-guard",
				FileName:  "doc.bin",
				FileSize:  100,
				Recipient: receiver,
				State:     tc.state,
				CreatedAt: time.Now(),
			}

			m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID})

			if acksSent != 0 {
				t.Errorf("file_downloaded in state %s should not trigger ack, got %d", tc.state, acksSent)
			}
			if m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State != tc.state {
				t.Errorf("state should remain %s, got %s", tc.state, m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Receiver state machine tests
// ---------------------------------------------------------------------------

// TestReceiverFileDownloadedAckTransitionsToCompleted verifies that
// HandleFileDownloadedAck transitions from WaitingAck → Completed.
func TestReceiverFileDownloadedAckTransitionsToCompleted(t *testing.T) {
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("waiting-ack-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:          ftIncomingRef(m, fileID, sender),
		PartialPath:  partialDownloadPath(m.downloadDir, fileID),
		FileID:       fileID,
		FileHash:     "hash-wait",
		FileName:     "doc.pdf",
		FileSize:     50000,
		Sender:       sender,
		State:        receiverWaitingAck,
		ServingEpoch: 2,
		CreatedAt:    time.Now(),
	}

	m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID, Epoch: 2})

	mapping := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
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
		m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
			Ref:         ftIncomingRef(m, fileID, sender),
			PartialPath: partialDownloadPath(m.downloadDir, fileID),
			FileID:      fileID,
			FileHash:    "hash",
			Sender:      sender,
			State:       state,
			CreatedAt:   time.Now(),
		}

		m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID})

		if m.receiverMaps[ftIncomingRef(m, fileID, sender)].State != state {
			t.Errorf("state %s should not change on ack, got %s", state, m.receiverMaps[ftIncomingRef(m, fileID, sender)].State)
		}
	}
}

// TestReceiverFileDownloadedAckFromWrongSenderIgnored verifies that an ack
// from a peer that is not the mapping's Sender is silently ignored.
func TestReceiverFileDownloadedAckFromWrongSenderIgnored(t *testing.T) {
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	realSender := domaintest.ID("real-sender-identity-1234567890")
	fakeSender := domaintest.ID("fake-sender-identity-1234567890")
	fileID := domain.FileID("wrong-sender-ack")

	m.receiverMaps[ftIncomingRef(m, fileID, realSender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, realSender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		Sender:      realSender,
		State:       receiverWaitingAck,
	}

	m.HandleFileDownloadedAck(fakeSender, domain.FileDownloadedAckPayload{FileID: fileID})

	if m.receiverMaps[ftIncomingRef(m, fileID, realSender)].State != receiverWaitingAck {
		t.Errorf("ack from wrong sender should not change state, got %s", m.receiverMaps[ftIncomingRef(m, fileID, realSender)].State)
	}
}

// TestReceiverWaitingRouteResumesOnPeerReachable verifies that
// tickReceiverMappings transitions a WaitingRoute mapping back to
// Downloading when the sender becomes reachable again.
func TestReceiverWaitingRouteResumesOnPeerReachable(t *testing.T) {
	var chunkRequested bool
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true // sender is back online
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			chunkRequested = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-who-went-offline-1234567")
	fileID := domain.FileID("waiting-route-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "hash-wr",
		FileName:    "resume.bin",
		FileSize:    100000,
		Sender:      sender,
		State:       receiverWaitingRoute,
		CreatedAt:   time.Now(),
		NextOffset:  50000, // partial download
		ChunkSize:   domain.DefaultChunkSize,
	}

	m.tickReceiverMappings()

	mapping := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false // sender still offline
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			t.Error("should not send any command while peer is offline")
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("offline-sender-identity-1234567")
	fileID := domain.FileID("stays-waiting-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		Sender:      sender,
		State:       receiverWaitingRoute,
		NextOffset:  10000,
		ChunkSize:   domain.DefaultChunkSize,
	}

	m.tickReceiverMappings()

	if m.receiverMaps[ftIncomingRef(m, fileID, sender)].State != receiverWaitingRoute {
		t.Errorf("state = %s, want waiting_route", m.receiverMaps[ftIncomingRef(m, fileID, sender)].State)
	}
}

// TestReceiverWaitingAckTimeoutTransitionsToCompleted verifies that after
// exceeding max retries in WaitingAck state, the receiver transitions to
// Completed as a fallback.
func TestReceiverWaitingAckTimeoutTransitionsToCompleted(t *testing.T) {
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("ack-timeout-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:               ftIncomingRef(m, fileID, sender),
		PartialPath:       partialDownloadPath(m.downloadDir, fileID),
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 21, // > 20 threshold
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: 0,
	}

	m.tickReceiverMappings()

	if m.receiverMaps[ftIncomingRef(m, fileID, sender)].State != receiverCompleted {
		t.Errorf("state = %s, want completed (fallback after max retries)", m.receiverMaps[ftIncomingRef(m, fileID, sender)].State)
	}
}

// TestReceiverWaitingAckRetryFileDownloaded verifies that tickReceiverMappings
// re-sends file_downloaded with exponential backoff.
func TestReceiverWaitingAckRetryFileDownloaded(t *testing.T) {
	var downloadedSent int
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("retry-file-downloaded")

	// Backoff expired — should trigger retry.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:               ftIncomingRef(m, fileID, sender),
		PartialPath:       partialDownloadPath(m.downloadDir, fileID),
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

	mapping := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	realSender := domaintest.ID("real-sender-identity-1234567890")
	fakeSender := domaintest.ID("fake-sender-identity-1234567890")
	fileID := domain.FileID("wrong-sender-chunk")

	m.receiverMaps[ftIncomingRef(m, fileID, realSender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, realSender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "hash",
		FileSize:    10000,
		Sender:      realSender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
	}

	m.HandleChunkResponse(fakeSender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   "dGVzdA",
	})

	// State should not change, bytes not advanced.
	if m.receiverMaps[ftIncomingRef(m, fileID, realSender)].BytesReceived != 0 {
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated transient send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated send failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("fresh-rollback")
	validHash := "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    validHash,
		FileName:    "fresh.bin",
		FileSize:    100000,
		Sender:      sender,
		State:       receiverAvailable,
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
	}

	err := m.StartDownload(fileID)
	if err == nil {
		t.Fatal("StartDownload should fail when sendCommand fails")
	}

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("missing-partial")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	// Use a file ID that produces the same partial path as our truncated file.
	fileID := domain.FileID("truncated-partial")
	validHash := "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(_ domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    validHash,
		FileName:    "test.bin",
		FileSize:    100000,
		Sender:      sender,
		State:       receiverAvailable,
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	validHash := "c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8f1f2f3f4f5f6f7f8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	validHash := "f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8"

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated network failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			chunkRequestSent = true
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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
	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
	snap := m.prepareResumeLocked(ftIncomingRef(m, fileID, sender), rm)
	m.saveMappingsLocked()
	m.mu.Unlock()

	// Phase 2: concurrent cancel between unlock and deferred execution.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}

	// Phase 3: deferred execution — should be skipped by receiverStateIs.
	// The original mapping had generation 0; CancelDownload increments it,
	// so both state and generation mismatch.
	if m.receiverStateIs(ftIncomingRef(m, fileID, sender), receiverDownloading, 0) {
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			actionExecuted = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-guard-test")
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "aaaa",
		FileName:    "test.bin",
		FileSize:    10000,
		Sender:      sender,
		State:       receiverFailed, // actual state
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
	}

	// Action was created when state was receiverDownloading, but since then
	// the mapping moved to receiverFailed. The guard must skip it.
	action := receiverTickAction{
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionRetryChunk,
		requiredState: receiverDownloading, // does NOT match actual state
		generation:    0,
		fileID:        ftIncomingRef(m, fileID, sender),
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			ackSent = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-guard-match")
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionRetryFileDownloaded,
		requiredState: receiverWaitingAck,
		generation:    0,
		fileID:        ftIncomingRef(m, fileID, sender),
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool { return true },
		stopCh:        make(chan struct{}),
	}

	sender := domaintest.ID("sender-cleanup-regen")

	// Phase 1: create a failed mapping (generation 0) and capture a
	// deferred cleanup action from the tick snapshot.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "aaaa",
		FileName:    "test.bin",
		FileSize:    100000,
		Sender:      sender,
		State:       receiverFailed,
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
		// Generation deliberately 0 (default).
	}

	staleAction := receiverTickAction{
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionCleanupFailed,
		requiredState: receiverFailed,
		generation:    0, // captured from the old mapping
		fileID:        ftIncomingRef(m, fileID, sender),
	}

	// Phase 2: simulate CleanupPeerTransfers removing the failed mapping,
	// then a new RegisterFileReceive + StartDownload creating a fresh
	// .part file. We simulate this by deleting and re-inserting with a
	// new generation.
	delete(m.receiverMaps, ftIncomingRef(m, fileID, sender))
	m.nextGeneration++
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "bbbb",
		FileName:    "test.bin",
		FileSize:    200000,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
		Generation:  m.nextGeneration,
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
	sender := domaintest.ID("sender-verify-cancel")

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
		FileName:    "test.bin",
		FileSize:    100000,
		Sender:      sender,
		State:       receiverVerifying,
		ChunkSize:   domain.DefaultChunkSize,
		CreatedAt:   time.Now(),
		Generation:  oldGeneration,
	}

	// Phase 2: user cancels and immediately starts a new download.
	// CancelDownload resets state and bumps generation.
	m.nextGeneration++
	newGeneration := m.nextGeneration
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].State = receiverDownloading
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].Generation = newGeneration
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].BytesReceived = 4096
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].NextOffset = 4096

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

	if m.receiverStateIs(ftIncomingRef(m, fileID, sender), receiverFailed, oldGeneration) {
		t.Fatal("receiverStateIs should return false: state is downloading with new generation")
	}

	// Also verify that even if the state were receiverFailed, the generation mismatch
	// would still prevent cleanup.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].State = receiverFailed
	if m.receiverStateIs(ftIncomingRef(m, fileID, sender), receiverFailed, oldGeneration) {
		t.Fatal("receiverStateIs should return false: generation mismatch despite matching state")
	}

	// Restore to downloading for the final check.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)].State = receiverDownloading

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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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
		Sender:     domaintest.ID("sender-gen-race-1234567890abcd"),
		State:      receiverVerifying,
		CreatedAt:  time.Now(),
		Generation: currentGeneration,
	}
	rm.Ref = ftIncomingRef(m, fileID, rm.Sender)
	rm.PartialPath = partialDownloadPath(m.downloadDir, fileID)
	m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-gen-race-1234567890abcd"))] = rm

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

// TestRetryPendingDownloads_RollbackRestoresAllWaitingRouteOnSendFailure
// verifies that when multiple waitingRoute transfers are resumed in the
// same tick and the chunk_request send fails, every transfer rolls back
// to waitingRoute. The receiver no longer enforces a concurrent download
// cap, so all eligible transfers are picked up together — rollback must
// stay correct in that broader scenario.
func TestRetryPendingDownloads_RollbackRestoresAllWaitingRouteOnSendFailure(t *testing.T) {
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated network failure")
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")

	// Two waitingRoute transfers — both are picked up in the same tick
	// (no concurrency cap). The send fails for each, and the rollback
	// path must restore both back to waitingRoute.
	for _, fid := range []domain.FileID{"wr-file-a", "wr-file-b"} {
		m.receiverMaps[ftIncomingRef(m, fid, sender)] = &receiverFileMapping{
			Ref:           ftIncomingRef(m, fid, sender),
			PartialPath:   partialDownloadPath(m.downloadDir, fid),
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
		rm := m.receiverMaps[ftIncomingRef(m, fid, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("partial-missing")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	sender := domaintest.ID("sender-identity-1234567890abcd")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		peerReachable: func(peer domain.PeerIdentity) bool {
			return true
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("zero-offset-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("ack-pending-file")
	m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd")),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
		FileID:        fileID,
		FileHash:      "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		FileName:      "report.pdf",
		FileSize:      1024,
		Sender:        domaintest.ID("sender-identity-1234567890abcd"),
		State:         receiverWaitingAck,
		CompletedPath: filepath.Join(dir, "report.pdf"),
		CreatedAt:     time.Now(),
	}

	err := m.CancelDownload(fileID)
	if err == nil {
		t.Fatal("CancelDownload should reject waitingAck state")
	}

	// State must remain unchanged.
	if m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))].State != receiverWaitingAck {
		t.Errorf("state = %s, want waiting_ack", m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))].State)
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
				localID:      ftFixtureIdentity(),
				senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
				receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
				downloadDir:  dir,
				stopCh:       make(chan struct{}),
			}

			fileID := domain.FileID("cancel-" + string(state))
			m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))] = &receiverFileMapping{
				Ref:         ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd")),
				PartialPath: partialDownloadPath(m.downloadDir, fileID),
				FileID:      fileID,
				FileHash:    "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				FileName:    "test.bin",
				FileSize:    1024,
				Sender:      domaintest.ID("sender-identity-1234567890abcd"),
				State:       state,
				CreatedAt:   time.Now(),
			}

			if err := m.CancelDownload(fileID); err != nil {
				t.Fatalf("CancelDownload should allow state %s: %v", state, err)
			}

			if m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))].State != receiverAvailable {
				t.Errorf("state = %s, want available", m.receiverMaps[ftIncomingRef(m, fileID, domaintest.ID("sender-identity-1234567890abcd"))].State)
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
	sender := domaintest.ID("restart-sender-identity-1234")

	// Set the manager's generation counter to match the mapping so the
	// bump assertion is meaningful.
	m.nextGeneration = 5
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:           ftIncomingRef(m, fileID, sender),
		PartialPath:   partialDownloadPath(m.downloadDir, fileID),
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

	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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

	sender := domaintest.ID("restart-reject-sender-1234")

	nonFailedStates := []receiverState{
		receiverAvailable, receiverDownloading, receiverVerifying,
		receiverWaitingAck, receiverCompleted, receiverWaitingRoute,
	}

	for _, state := range nonFailedStates {
		t.Run(string(state), func(t *testing.T) {
			fileID := domain.FileID("restart-reject-" + string(state))
			m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
				Ref:         ftIncomingRef(m, fileID, sender),
				PartialPath: partialDownloadPath(m.downloadDir, fileID),
				FileID:      fileID,
				Sender:      sender,
				State:       state,
				CreatedAt:   time.Now(),
			}

			err := m.RestartDownload(fileID)
			if err == nil {
				t.Fatalf("RestartDownload should reject state %s", state)
			}
			if m.receiverMaps[ftIncomingRef(m, fileID, sender)].State != state {
				t.Errorf("state changed from %s to %s after rejected restart",
					state, m.receiverMaps[ftIncomingRef(m, fileID, sender)].State)
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		mappingsPath: filepath.Join(dir, "transfers.json"),
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	recipient := domaintest.ID("receiver-identity-1234567890ab")

	// Stale entry: LastServedAt well past the timeout.
	staleID := domain.FileID("stale-serving")
	m.senderMaps[ftOutgoingRef(m, staleID, recipient)] = &senderFileMapping{
		Ref:          ftOutgoingRef(m, staleID, recipient),
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
	m.senderMaps[ftOutgoingRef(m, freshID, recipient)] = &senderFileMapping{
		Ref:          ftOutgoingRef(m, freshID, recipient),
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
	m.senderMaps[ftOutgoingRef(m, announcedID, recipient)] = &senderFileMapping{
		Ref:          ftOutgoingRef(m, announcedID, recipient),
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
	if m.senderMaps[ftOutgoingRef(m, staleID, recipient)].State != senderAnnounced {
		t.Errorf("stale entry: state = %s, want announced", m.senderMaps[ftOutgoingRef(m, staleID, recipient)].State)
	}

	// Fresh entry should remain serving.
	if m.senderMaps[ftOutgoingRef(m, freshID, recipient)].State != senderServing {
		t.Errorf("fresh entry: state = %s, want serving", m.senderMaps[ftOutgoingRef(m, freshID, recipient)].State)
	}

	// Announced entry should stay announced.
	if m.senderMaps[ftOutgoingRef(m, announcedID, recipient)].State != senderAnnounced {
		t.Errorf("announced entry: state = %s, want announced", m.senderMaps[ftOutgoingRef(m, announcedID, recipient)].State)
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
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

	m1.senderMaps[ftOutgoingRef(m1, fileID, domaintest.ID("receiver-identity-1234567890ab"))] = &senderFileMapping{
		Ref:          ftOutgoingRef(m1, fileID, domaintest.ID("receiver-identity-1234567890ab")),
		FileID:       fileID,
		FileHash:     validHash,
		FileName:     "persist.bin",
		FileSize:     512,
		Recipient:    domaintest.ID("receiver-identity-1234567890ab"),
		State:        senderServing,
		CreatedAt:    time.Now().Add(-20 * time.Minute),
		LastServedAt: lastServed,
	}

	m1.mu.Lock()
	m1.saveMappingsLocked()
	m1.mu.Unlock()

	// Reload into a new manager.
	m2 := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		mappingsPath: mappingsPath,
		downloadDir:  dir,
		stopCh:       make(chan struct{}),
	}
	m2.loadMappings()

	sm, ok := m2.senderMaps[ftOutgoingRef(m2, fileID, domaintest.ID("receiver-identity-1234567890ab"))]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false // sender always offline
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			downloadedSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("offline-ack-retry")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:               ftIncomingRef(m, fileID, sender),
		PartialPath:       partialDownloadPath(m.downloadDir, fileID),
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 5,
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: initialRetryTimeout,
	}

	m.tickReceiverMappings()

	// Counter must have advanced even though sender is offline.
	mapping := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
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
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		peerReachable: func(peer domain.PeerIdentity) bool {
			return false
		},
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-1234567890abcd")
	fileID := domain.FileID("offline-ack-exhaust")

	// Set retries to 20 — next tick should push it to 21 and trigger completion.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:               ftIncomingRef(m, fileID, sender),
		PartialPath:       partialDownloadPath(m.downloadDir, fileID),
		FileID:            fileID,
		Sender:            sender,
		State:             receiverWaitingAck,
		DownloadedRetries: 20,
		DownloadedSentAt:  time.Now().Add(-time.Hour),
		DownloadedBackoff: maxRetryTimeout,
		CompletedPath:     "/some/completed/file.bin",
	}

	m.tickReceiverMappings()

	mapping := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
	if mapping.State != receiverCompleted {
		t.Fatalf("state = %s, want completed", mapping.State)
	}
	if mapping.CompletedAt.IsZero() {
		t.Error("CompletedAt should be set")
	}
}

// ---------------------------------------------------------------------------
// Serving epoch / stale file_downloaded replay defense
// ---------------------------------------------------------------------------
//
// A re-download reuses the same FileID as the prior completed run. A delayed
// file_downloaded from the prior run is therefore indistinguishable from a
// legitimate completion of the current run by FileID alone. The sender
// defends against this by bumping a monotonic ServingEpoch on every genuine
// transition into senderServing and requiring incoming file_downloaded
// messages to echo the current epoch. The tests below pin this invariant.

// TestSenderValidateChunkRequestBumpsServingEpochOnTransition verifies that
// the sender's ServingEpoch is bumped exactly once per genuine transition
// into senderServing — not on continuation chunks of the same serving run.
func TestSenderValidateChunkRequestBumpsServingEpochOnTransition(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	fileHash := sha256Hex([]byte("serving-epoch-bump"))
	blobData := []byte("serving-epoch-bump-content")
	if err := os.WriteFile(filepath.Join(transmitDir, fileHash+".bin"), blobData, 0o600); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
		stopCh:       make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-epoch-bump-1")
	fileID := domain.FileID("epoch-bump-file")

	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:       ftOutgoingRef(m, fileID, receiver),
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "x.bin",
		FileSize:  uint64(len(blobData)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Announced → Serving: epoch 0 → 1.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{FileID: fileID, Offset: 0, Size: domain.DefaultChunkSize})
	if got := m.senderMaps[ftOutgoingRef(m, fileID, receiver)].ServingEpoch; got != 1 {
		t.Fatalf("after first chunk_request: ServingEpoch = %d, want 1", got)
	}

	// Continuation (already serving): no bump.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{FileID: fileID, Offset: 0, Size: domain.DefaultChunkSize})
	if got := m.senderMaps[ftOutgoingRef(m, fileID, receiver)].ServingEpoch; got != 1 {
		t.Fatalf("after continuation chunk_request: ServingEpoch = %d, want 1 (no bump)", got)
	}

	// Force completion then trigger a re-download: epoch 1 → 2.
	m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State = senderCompleted
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{FileID: fileID, Offset: 0, Size: domain.DefaultChunkSize})
	if got := m.senderMaps[ftOutgoingRef(m, fileID, receiver)].ServingEpoch; got != 2 {
		t.Fatalf("after re-download chunk_request: ServingEpoch = %d, want 2", got)
	}
}

// TestSenderChunkResponseCarriesCurrentEpoch verifies that the outgoing
// chunk_response payload stamps the sender's current ServingEpoch so the
// receiver can stash it and echo it later in file_downloaded.
func TestSenderChunkResponseCarriesCurrentEpoch(t *testing.T) {
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	fileHash := sha256Hex([]byte("chunk-epoch-wire"))
	blobData := []byte("chunk-epoch-wire-data")
	if err := os.WriteFile(filepath.Join(transmitDir, fileHash+".bin"), blobData, 0o600); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()

	var sent []domain.ChunkResponsePayload
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command != domain.FileActionChunkResp {
				return nil
			}
			var cr domain.ChunkResponsePayload
			if err := json.Unmarshal(payload.Data, &cr); err != nil {
				t.Fatalf("unmarshal chunk_response: %v", err)
			}
			sent = append(sent, cr)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-chunk-wire-1")
	fileID := domain.FileID("chunk-wire-file")

	// Pre-set epoch to 7; the sender bumps on announced→serving so final
	// observed wire epoch is 8.
	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:          ftOutgoingRef(m, fileID, receiver),
		FileID:       fileID,
		FileHash:     fileHash,
		FileName:     "x.bin",
		FileSize:     uint64(len(blobData)),
		Recipient:    receiver,
		State:        senderAnnounced,
		ServingEpoch: 7,
		CreatedAt:    time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{FileID: fileID, Offset: 0, Size: domain.DefaultChunkSize})

	if len(sent) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sent))
	}
	if sent[0].Epoch != 8 {
		t.Errorf("chunk_response.Epoch = %d, want 8 (was 7, bumped on transition)", sent[0].Epoch)
	}
}

// TestSenderHandleFileDownloadedRejectsStaleEpochReplay is the core
// regression for the bug: a delayed file_downloaded carrying an epoch from
// a prior completed serving run must not flip the active re-download back
// to senderCompleted, clear PreServeState, or trigger an ack.
func TestSenderHandleFileDownloadedRejectsStaleEpochReplay(t *testing.T) {
	var acksSent int
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionDownloadedAck {
				acksSent++
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domaintest.ID("receiver-identity-stale-replay")
	fileID := domain.FileID("stale-epoch-file")

	// Active re-download: state=serving, ServingEpoch=5, origin=completed.
	m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
		Ref:           ftOutgoingRef(m, fileID, receiver),
		FileID:        fileID,
		FileHash:      "deadbeef",
		FileName:      "doc.bin",
		FileSize:      100,
		Recipient:     receiver,
		State:         senderServing,
		PreServeState: senderCompleted,
		ServingEpoch:  5,
		CreatedAt:     time.Now().Add(-time.Hour),
		LastServedAt:  time.Now(),
	}

	// Stale file_downloaded from a prior run (epoch 4).
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: 4})

	sm := m.senderMaps[ftOutgoingRef(m, fileID, receiver)]
	if sm.State != senderServing {
		t.Errorf("state = %s, want senderServing (stale replay must not flip)", sm.State)
	}
	if sm.PreServeState != senderCompleted {
		t.Errorf("PreServeState = %s, want senderCompleted (stale replay must not clear)", sm.PreServeState)
	}
	if sm.ServingEpoch != 5 {
		t.Errorf("ServingEpoch = %d, want 5 (unchanged)", sm.ServingEpoch)
	}
	if !sm.CompletedAt.IsZero() {
		t.Errorf("CompletedAt should remain zero on stale replay, got %v", sm.CompletedAt)
	}
	if acksSent != 0 {
		t.Errorf("stale replay must not trigger ack, got %d acks", acksSent)
	}

	// Legitimate file_downloaded for the current epoch still completes.
	m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: 5})
	sm = m.senderMaps[ftOutgoingRef(m, fileID, receiver)]
	if sm.State != senderCompleted {
		t.Errorf("after matching-epoch file_downloaded: state = %s, want senderCompleted", sm.State)
	}
	if sm.PreServeState != "" {
		t.Errorf("after matching-epoch completion: PreServeState = %s, want empty", sm.PreServeState)
	}
	if acksSent != 1 {
		t.Errorf("after matching-epoch completion: acks = %d, want 1", acksSent)
	}
}

// captureFileTransferLog redirects the package logger into a buffer for the
// rest of the test. Tests that use it must not call t.Parallel: the logger is
// process-global, and a parallel test would read it while it is swapped.
func captureFileTransferLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var captured bytes.Buffer
	restore := log.Logger
	log.Logger = zerolog.New(&captured).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = restore })
	return &captured
}

// loggedLineWithMessage decodes the single captured log line whose message
// contains fragment. A rejection that is not logged, or logged twice, fails.
func loggedLineWithMessage(t *testing.T, captured *bytes.Buffer, fragment string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(captured.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("captured log line is not JSON: %v\n%s", err, raw)
		}
		if message, _ := line["message"].(string); strings.Contains(message, fragment) {
			found = append(found, line)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one log line containing %q, got %d:\n%s", fragment, len(found), captured.String())
	}
	return found[0]
}

// assertEpochMismatchLogged checks that a dropped completion-edge frame was
// reported at warn level with the transfer, the peer and both epochs, so the
// rejection can be traced back to the run it did not belong to.
func assertEpochMismatchLogged(
	t *testing.T,
	captured *bytes.Buffer,
	fragment string,
	peerField string,
	fileID domain.FileID,
	peer domain.PeerIdentity,
	receivedEpoch, currentEpoch uint64,
) {
	t.Helper()
	line := loggedLineWithMessage(t, captured, fragment)
	if line["level"] != "warn" {
		t.Errorf("level = %v, want warn", line["level"])
	}
	if line["file_id"] != logID(string(fileID)) {
		t.Errorf("file_id = %v, want %s", line["file_id"], logID(string(fileID)))
	}
	if line[peerField] != peer.String() {
		t.Errorf("%s = %v, want %s", peerField, line[peerField], peer.String())
	}
	if line["received_epoch"] != float64(receivedEpoch) {
		t.Errorf("received_epoch = %v, want %d", line["received_epoch"], receivedEpoch)
	}
	if line["current_epoch"] != float64(currentEpoch) {
		t.Errorf("current_epoch = %v, want %d", line["current_epoch"], currentEpoch)
	}
}

// TestSenderHandleFileDownloadedRejectsEpochOfAnotherRun pins the rule that
// file_downloaded completes a serving run only when it names that run. Every
// build that can reach this node stamps the epoch it learned from
// chunk_response, so a frame without one (0 on the wire) is not "an older
// peer" but a frame that does not belong to the run the sender is in — the
// same as any other mismatching epoch. Before this rule a missing epoch
// bypassed the check and let a stale frame finish an active re-download.
func TestSenderHandleFileDownloadedRejectsEpochOfAnotherRun(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        senderState
		servingEpoch uint64
		wireEpoch    uint64
	}{
		{"serving run 3, frame without epoch", senderServing, 3, 0},
		{"completed run 3, frame without epoch", senderCompleted, 3, 0},
		{"serving run 3, frame of run 2", senderServing, 3, 2},
		{"serving run 3, frame of a future run", senderServing, 3, 4},
		{"no run recorded, frame of run 5", senderCompleted, 0, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured := captureFileTransferLog(t)
			var acksSent int
			m := &Manager{
				localID:      ftFixtureIdentity(),
				senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
				receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
				sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
					if payload.Command == domain.FileActionDownloadedAck {
						acksSent++
					}
					return nil
				},
				stopCh: make(chan struct{}),
			}

			receiver := domaintest.ID("receiver-identity-epoch-run-01")
			fileID := domain.FileID("epoch-of-another-run-file")
			completedAt := time.Now().Add(-time.Hour)
			m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
				Ref:           ftOutgoingRef(m, fileID, receiver),
				FileID:        fileID,
				FileHash:      "cafebabe",
				FileName:      "doc.bin",
				FileSize:      100,
				Recipient:     receiver,
				State:         tc.state,
				PreServeState: preServeStateFor(tc.state),
				ServingEpoch:  tc.servingEpoch,
				CreatedAt:     time.Now().Add(-2 * time.Hour),
				CompletedAt:   completedAt,
				LastServedAt:  time.Now(),
			}

			m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: tc.wireEpoch})

			sm := m.senderMaps[ftOutgoingRef(m, fileID, receiver)]
			if sm.State != tc.state {
				t.Errorf("state = %s, want %s (a frame of another run must not move the mapping)", sm.State, tc.state)
			}
			if sm.PreServeState != preServeStateFor(tc.state) {
				t.Errorf("PreServeState = %q, want %q", sm.PreServeState, preServeStateFor(tc.state))
			}
			if !sm.CompletedAt.Equal(completedAt) {
				t.Errorf("CompletedAt = %v, want %v (unchanged)", sm.CompletedAt, completedAt)
			}
			if sm.ServingEpoch != tc.servingEpoch {
				t.Errorf("ServingEpoch = %d, want %d (unchanged)", sm.ServingEpoch, tc.servingEpoch)
			}
			if acksSent != 0 {
				t.Errorf("a frame of another run must not be acked, got %d acks", acksSent)
			}
			assertEpochMismatchLogged(t, captured, "file_downloaded epoch mismatch", "recipient",
				fileID, receiver, tc.wireEpoch, tc.servingEpoch)
		})
	}
}

// preServeStateFor is the PreServeState a mapping in state carries: only a
// serving run remembers where it came from.
func preServeStateFor(state senderState) senderState {
	if state == senderServing {
		return senderCompleted
	}
	return ""
}

// TestSenderHandleFileDownloadedAcceptsEpochOfTheRun is the other half of the
// rule: the frame that names the sender's run completes it (or is re-acked
// when the run already completed), and the ack carries that same epoch back.
// A mapping with no recorded run (persisted before epochs existed) still pairs
// with a receiver that recorded none either — both sides hold 0.
func TestSenderHandleFileDownloadedAcceptsEpochOfTheRun(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        senderState
		servingEpoch uint64
	}{
		{"serving run 3 completes", senderServing, 3},
		{"completed run 3 is re-acked", senderCompleted, 3},
		{"no run recorded on either side", senderCompleted, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var acks []domain.FileDownloadedAckPayload
			m := &Manager{
				localID:      ftFixtureIdentity(),
				senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
				receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
				sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
					var ack domain.FileDownloadedAckPayload
					if err := json.Unmarshal(payload.Data, &ack); err != nil {
						t.Fatalf("unmarshal file_downloaded_ack: %v", err)
					}
					acks = append(acks, ack)
					return nil
				},
				stopCh: make(chan struct{}),
			}

			receiver := domaintest.ID("receiver-identity-epoch-run-02")
			fileID := domain.FileID("epoch-of-the-run-file")
			m.senderMaps[ftOutgoingRef(m, fileID, receiver)] = &senderFileMapping{
				Ref:           ftOutgoingRef(m, fileID, receiver),
				FileID:        fileID,
				FileHash:      "cafebabe",
				FileName:      "doc.bin",
				FileSize:      100,
				Recipient:     receiver,
				State:         tc.state,
				PreServeState: preServeStateFor(tc.state),
				ServingEpoch:  tc.servingEpoch,
				CreatedAt:     time.Now().Add(-time.Hour),
				LastServedAt:  time.Now(),
			}

			m.HandleFileDownloaded(receiver, domain.FileDownloadedPayload{FileID: fileID, Epoch: tc.servingEpoch})

			if got := m.senderMaps[ftOutgoingRef(m, fileID, receiver)].State; got != senderCompleted {
				t.Errorf("state = %s, want %s", got, senderCompleted)
			}
			if len(acks) != 1 {
				t.Fatalf("want 1 file_downloaded_ack, got %d", len(acks))
			}
			if acks[0].Epoch != tc.servingEpoch {
				t.Errorf("ack epoch = %d, want %d (the run it completes)", acks[0].Epoch, tc.servingEpoch)
			}
		})
	}
}

// TestReceiverHandleFileDownloadedAckRejectsEpochOfAnotherRun is the
// receiver-side mirror: an ack completes the waiting_ack run only when it
// echoes the epoch that run sent in file_downloaded. The sender echoes it
// verbatim, so an ack without one cannot be the answer to the current run.
func TestReceiverHandleFileDownloadedAckRejectsEpochOfAnotherRun(t *testing.T) {
	for _, tc := range []struct {
		name         string
		servingEpoch uint64
		wireEpoch    uint64
	}{
		{"run 9, ack without epoch", 9, 0},
		{"run 9, ack of run 8", 9, 8},
		{"no run recorded, ack of run 8", 0, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captured := captureFileTransferLog(t)
			m := &Manager{
				localID:      ftFixtureIdentity(),
				senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
				receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
				sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
				stopCh:       make(chan struct{}),
			}

			sender := domaintest.ID("sender-identity-ack-run-0001")
			fileID := domain.FileID("ack-of-another-run-file")
			m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
				Ref:          ftIncomingRef(m, fileID, sender),
				PartialPath:  partialDownloadPath(m.downloadDir, fileID),
				FileID:       fileID,
				FileHash:     "deadbeef",
				FileName:     "doc.bin",
				FileSize:     100,
				Sender:       sender,
				State:        receiverWaitingAck,
				ServingEpoch: tc.servingEpoch,
				CreatedAt:    time.Now(),
			}

			m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID, Epoch: tc.wireEpoch})

			rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
			if rm.State != receiverWaitingAck {
				t.Errorf("state = %s, want %s", rm.State, receiverWaitingAck)
			}
			if !rm.CompletedAt.IsZero() {
				t.Errorf("CompletedAt = %v, want zero", rm.CompletedAt)
			}
			assertEpochMismatchLogged(t, captured, "file_downloaded_ack epoch mismatch", "sender",
				fileID, sender, tc.wireEpoch, tc.servingEpoch)
		})
	}
}

// epochWireFrame is one file command in flight between the two managers of
// TestEveryCompletionFrameCarriesTheServingRunEpoch.
type epochWireFrame struct {
	from    domain.PeerIdentity
	to      domain.PeerIdentity
	payload domain.FileCommandPayload
}

// epochWire connects managers through a FIFO instead of direct calls, so no
// handler runs inside another handler's send and every frame is observed in
// the order it was emitted — including the ones the wire then loses.
type epochWire struct {
	t        *testing.T
	peers    map[domain.PeerIdentity]*Manager
	queue    []epochWireFrame
	emitted  []domain.FileCommandPayload
	loseNext map[domain.FileAction]bool
}

func (w *epochWire) sendCommandFrom(from domain.PeerIdentity) func(domain.PeerIdentity, domain.FileCommandPayload) error {
	return func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
		w.emitted = append(w.emitted, payload)
		w.queue = append(w.queue, epochWireFrame{from: from, to: dst, payload: payload})
		return nil
	}
}

func (w *epochWire) deliverAll() {
	w.t.Helper()
	const maxFrames = 1000
	for delivered := 0; len(w.queue) > 0; delivered++ {
		if delivered == maxFrames {
			w.t.Fatalf("wire did not drain after %d frames", maxFrames)
		}
		frame := w.queue[0]
		w.queue = w.queue[1:]
		if w.loseNext[frame.payload.Command] {
			delete(w.loseNext, frame.payload.Command)
			continue
		}
		w.peers[frame.to].HandleLocalFileCommand(frame.from, &frame.payload)
	}
}

// takeEmitted returns the frames emitted since the last call and forgets them.
func (w *epochWire) takeEmitted() []domain.FileCommandPayload {
	emitted := w.emitted
	w.emitted = nil
	return emitted
}

// TestEveryCompletionFrameCarriesTheServingRunEpoch is the proof behind
// rejecting frames without an epoch: two real managers run a multi-chunk
// download, a re-download of the same FileID, a lost ack and the receiver's
// retry of file_downloaded, and every chunk_response, file_downloaded and
// file_downloaded_ack either of them emits carries the non-zero epoch of the
// sender's current serving run. No path of this build sends 0 to a peer that
// knows the run.
func TestEveryCompletionFrameCarriesTheServingRunEpoch(t *testing.T) {
	root := t.TempDir()
	transmitDir := filepath.Join(root, "transmit")
	downloadDir := filepath.Join(root, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	senderID := domaintest.ID("sender-identity-epoch-wire-01")
	receiverID := domaintest.ID("receiver-identity-epoch-wire1")
	fileID := domain.FileID("epoch-wire-file")
	content := []byte("twenty-bytes-payload")
	fileHash := sha256Hex(content)

	wire := &epochWire{t: t, peers: make(map[domain.PeerIdentity]*Manager), loseNext: make(map[domain.FileAction]bool)}
	sender := NewFileTransferManager(Config{
		LocalID:      ftFixtureIdentityFor(senderID),
		Store:        store,
		MappingsPath: filepath.Join(root, "sender-transfers.json"),
		SendCommand:  wire.sendCommandFrom(senderID),
	})
	receiver := NewFileTransferManager(Config{
		LocalID:      ftFixtureIdentityFor(receiverID),
		DownloadDir:  downloadDir,
		MappingsPath: filepath.Join(root, "receiver-transfers.json"),
		SendCommand:  wire.sendCommandFrom(receiverID),
	})
	wire.peers[senderID] = sender
	wire.peers[receiverID] = receiver

	if err := os.WriteFile(filepath.Join(transmitDir, fileHash+".bin"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.refs[fileHash] = 1
	store.mu.Unlock()
	sender.senderMaps[ftOutgoingRef(sender, fileID, receiverID)] = &senderFileMapping{
		Ref:       ftOutgoingRef(sender, fileID, receiverID),
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "doc.bin",
		FileSize:  uint64(len(content)),
		Recipient: receiverID,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}
	if err := receiver.RegisterFileReceive(fileID, fileHash, "doc.bin", "application/octet-stream", uint64(len(content)), senderID); err != nil {
		t.Fatalf("RegisterFileReceive: %v", err)
	}
	// Small chunks so the run spans several chunk_responses.
	receiver.receiverMaps[ftIncomingRef(receiver, fileID, senderID)].ChunkSize = 8

	assertRunFramesCarry := func(run string, wantEpoch uint64) {
		t.Helper()
		if got := sender.senderMaps[ftOutgoingRef(sender, fileID, receiverID)].ServingEpoch; got != wantEpoch {
			t.Fatalf("%s: sender ServingEpoch = %d, want %d", run, got, wantEpoch)
		}
		seen := make(map[domain.FileAction]int)
		for _, frame := range wire.takeEmitted() {
			if frame.Command == domain.FileActionChunkReq {
				continue
			}
			var stamped struct {
				Epoch uint64 `json:"epoch"`
			}
			if err := json.Unmarshal(frame.Data, &stamped); err != nil {
				t.Fatalf("%s: unmarshal %s: %v", run, frame.Command, err)
			}
			if stamped.Epoch != wantEpoch {
				t.Errorf("%s: %s carries epoch %d, want %d", run, frame.Command, stamped.Epoch, wantEpoch)
			}
			seen[frame.Command]++
		}
		for _, command := range []domain.FileAction{domain.FileActionChunkResp, domain.FileActionDownloaded, domain.FileActionDownloadedAck} {
			if seen[command] == 0 {
				t.Errorf("%s: no %s was emitted, so its epoch was never checked", run, command)
			}
		}
	}

	// Run 1: first download.
	if err := receiver.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload: %v", err)
	}
	wire.deliverAll()
	if got := receiver.receiverMaps[ftIncomingRef(receiver, fileID, senderID)].State; got != receiverCompleted {
		t.Fatalf("run 1: receiver state = %s, want %s", got, receiverCompleted)
	}
	assertRunFramesCarry("run 1", 1)

	// Run 2: re-download of the same FileID; the first ack is lost and the
	// receiver's retry of file_downloaded is answered by the completed sender.
	if err := receiver.DeleteLocalCopy(fileID); err != nil {
		t.Fatalf("DeleteLocalCopy: %v", err)
	}
	if err := receiver.StartDownload(fileID); err != nil {
		t.Fatalf("StartDownload (re-download): %v", err)
	}
	wire.loseNext[domain.FileActionDownloadedAck] = true
	wire.deliverAll()
	if got := receiver.receiverMaps[ftIncomingRef(receiver, fileID, senderID)].State; got != receiverWaitingAck {
		t.Fatalf("run 2 after lost ack: receiver state = %s, want %s", got, receiverWaitingAck)
	}
	receiver.sendFileDownloaded(ftIncomingRef(receiver, fileID, senderID), senderID)
	wire.deliverAll()
	if got := receiver.receiverMaps[ftIncomingRef(receiver, fileID, senderID)].State; got != receiverCompleted {
		t.Fatalf("run 2 after retry: receiver state = %s, want %s", got, receiverCompleted)
	}
	if got := sender.senderMaps[ftOutgoingRef(sender, fileID, receiverID)].State; got != senderCompleted {
		t.Fatalf("run 2: sender state = %s, want %s", got, senderCompleted)
	}
	assertRunFramesCarry("run 2", 2)
}

// TestReceiverChunkResponseStashesServingEpoch verifies that the receiver
// records the sender's epoch from each chunk_response so it can be echoed
// later in file_downloaded.
func TestReceiverChunkResponseStashesServingEpoch(t *testing.T) {
	dir := t.TempDir()

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-epoch-stash-1")
	fileID := domain.FileID("epoch-stash-file")
	payload := []byte("epoch-stash-payload-some-bytes")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    sha256Hex(payload),
		FileName:    "doc.bin",
		FileSize:    uint64(len(payload)),
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
		NextOffset:  0,
		CreatedAt:   time.Now(),
		LastChunkAt: time.Now(),
	}

	// Send a chunk_response carrying epoch 42.
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   base64.RawURLEncoding.EncodeToString(payload),
		Epoch:  42,
	})

	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].ServingEpoch; got != 42 {
		t.Fatalf("receiver ServingEpoch = %d, want 42", got)
	}
}

// TestReceiverChunkResponseEpochMonotonic verifies that the receiver's
// cached ServingEpoch never regresses. A delayed chunk_response from an
// older serving run (lower non-zero epoch) must not overwrite a newer
// epoch already observed. Without this guard, file_downloaded would echo
// the stale epoch and the sender would reject the legitimate completion
// as a replay.
func TestReceiverChunkResponseEpochMonotonic(t *testing.T) {
	dir := t.TempDir()

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-epoch-mono-01")
	fileID := domain.FileID("epoch-monotonic")

	// Build a payload large enough for multiple chunks.
	chunkA := []byte("aaaa")
	chunkB := []byte("bbbb")
	full := append(chunkA, chunkB...)

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    sha256Hex(full),
		FileName:    "doc.bin",
		FileSize:    uint64(len(full)),
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   uint32(len(chunkA)),
		NextOffset:  0,
		CreatedAt:   time.Now(),
		LastChunkAt: time.Now(),
	}

	// First chunk arrives with epoch 5 (current serving run).
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   base64.RawURLEncoding.EncodeToString(chunkA),
		Epoch:  5,
	})

	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].ServingEpoch; got != 5 {
		t.Fatalf("after chunk@0 epoch=5: ServingEpoch = %d, want 5", got)
	}

	// Second chunk arrives with epoch 3 (stale — delayed from an older run
	// that happened to match the current NextOffset after a resume).
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: uint64(len(chunkA)),
		Data:   base64.RawURLEncoding.EncodeToString(chunkB),
		Epoch:  3,
	})

	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].ServingEpoch; got != 5 {
		t.Errorf("after chunk@4 epoch=3: ServingEpoch = %d, want 5 (must not regress)", got)
	}
}

// TestReceiverSendFileDownloadedEchoesEpoch verifies that file_downloaded
// emitted by the receiver carries the stashed ServingEpoch.
func TestReceiverSendFileDownloadedEchoesEpoch(t *testing.T) {
	var sent []domain.FileDownloadedPayload
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command != domain.FileActionDownloaded {
				return nil
			}
			var dl domain.FileDownloadedPayload
			if err := json.Unmarshal(payload.Data, &dl); err != nil {
				t.Fatalf("unmarshal file_downloaded: %v", err)
			}
			sent = append(sent, dl)
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-echo-epoch-01")
	fileID := domain.FileID("echo-epoch-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:          ftIncomingRef(m, fileID, sender),
		PartialPath:  partialDownloadPath(m.downloadDir, fileID),
		FileID:       fileID,
		FileHash:     "deadbeef",
		FileName:     "doc.bin",
		FileSize:     100,
		Sender:       sender,
		State:        receiverWaitingAck,
		ServingEpoch: 11,
		CreatedAt:    time.Now(),
	}

	m.sendFileDownloaded(ftIncomingRef(m, fileID, sender), sender)

	if len(sent) != 1 {
		t.Fatalf("expected 1 file_downloaded, got %d", len(sent))
	}
	if sent[0].Epoch != 11 {
		t.Errorf("file_downloaded.Epoch = %d, want 11", sent[0].Epoch)
	}
	if sent[0].FileID != fileID {
		t.Errorf("file_downloaded.FileID = %s, want %s", sent[0].FileID, fileID)
	}
}

// TestReceiverHandleFileDownloadedAckRejectsStaleEpoch verifies that a
// late ack carrying a prior-run epoch does not flip an active waiting_ack
// mapping to completed. This is the receiver-side mirror of the sender's
// stale-replay defense.
func TestReceiverHandleFileDownloadedAckRejectsStaleEpoch(t *testing.T) {
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-stale-ack-01")
	fileID := domain.FileID("stale-ack-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:          ftIncomingRef(m, fileID, sender),
		PartialPath:  partialDownloadPath(m.downloadDir, fileID),
		FileID:       fileID,
		FileHash:     "deadbeef",
		FileName:     "doc.bin",
		FileSize:     100,
		Sender:       sender,
		State:        receiverWaitingAck,
		ServingEpoch: 9,
		CreatedAt:    time.Now(),
	}

	// Stale ack with prior-run epoch must not transition to completed.
	m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID, Epoch: 8})
	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].State; got != receiverWaitingAck {
		t.Fatalf("after stale ack: state = %s, want receiverWaitingAck", got)
	}

	// Matching ack transitions to completed.
	m.HandleFileDownloadedAck(sender, domain.FileDownloadedAckPayload{FileID: fileID, Epoch: 9})
	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].State; got != receiverCompleted {
		t.Fatalf("after matching ack: state = %s, want receiverCompleted", got)
	}
}

// TestReceiverCancelDownloadResetsServingEpoch verifies that a cancel
// clears the stashed epoch so the next download starts fresh and learns
// the sender's current epoch from its first chunk_response.
func TestReceiverCancelDownloadResetsServingEpoch(t *testing.T) {
	dir := t.TempDir()

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand:  func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error { return nil },
		stopCh:       make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-cancel-reset-1")
	fileID := domain.FileID("cancel-reset-file")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:          ftIncomingRef(m, fileID, sender),
		PartialPath:  partialDownloadPath(m.downloadDir, fileID),
		FileID:       fileID,
		FileHash:     "deadbeef",
		FileName:     "doc.bin",
		FileSize:     100,
		Sender:       sender,
		State:        receiverDownloading,
		ChunkSize:    domain.DefaultChunkSize,
		ServingEpoch: 13,
		CreatedAt:    time.Now(),
	}

	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}
	if got := m.receiverMaps[ftIncomingRef(m, fileID, sender)].ServingEpoch; got != 0 {
		t.Errorf("after CancelDownload: ServingEpoch = %d, want 0", got)
	}
}

// TestSendFileDownloadedSuppressedAfterStateChange verifies that
// sendFileDownloaded does NOT emit a file_downloaded command when the
// mapping is no longer in receiverWaitingAck. This covers the TOCTOU
// race where a concurrent state change (e.g. HandleFileDownloadedAck
// completing the transfer, or CleanupPeerTransfers resetting it) happens
// between the tick's action snapshot and sendFileDownloaded's own lock
// acquisition.
//
// Note: CancelDownload explicitly rejects receiverWaitingAck (it is a
// terminal-like state), so the race scenario is modelled by directly
// transitioning the mapping to receiverCompleted — the same effect a
// concurrent HandleFileDownloadedAck would produce.
func TestSendFileDownloadedSuppressedAfterStateChange(t *testing.T) {
	dir := t.TempDir()
	var sent int

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionDownloaded {
				sent++
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	sender := domaintest.ID("sender-identity-cancel-guard-01")
	fileID := domain.FileID("cancel-guard-file")

	// Start in receiverWaitingAck — the state that triggers ack retries.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:          ftIncomingRef(m, fileID, sender),
		PartialPath:  partialDownloadPath(m.downloadDir, fileID),
		FileID:       fileID,
		FileHash:     "deadbeef",
		FileName:     "doc.bin",
		FileSize:     100,
		Sender:       sender,
		State:        receiverWaitingAck,
		ChunkSize:    domain.DefaultChunkSize,
		ServingEpoch: 7,
		Generation:   1,
		CreatedAt:    time.Now(),
	}

	// Sanity: sendFileDownloaded sends when mapping is in waitingAck.
	m.sendFileDownloaded(ftIncomingRef(m, fileID, sender), sender)
	if sent != 1 {
		t.Fatalf("expected 1 send in waitingAck, got %d", sent)
	}

	// Simulate the race: HandleFileDownloadedAck (or similar) transitions
	// the mapping to receiverCompleted between the tick snapshot and
	// sendFileDownloaded's lock acquisition.
	m.mu.Lock()
	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
	rm.State = receiverCompleted
	rm.CompletedAt = time.Now()
	m.mu.Unlock()

	// A deferred retry action from the old waitingAck tick would call
	// sendFileDownloaded here. It must NOT send — the mapping is no
	// longer in receiverWaitingAck.
	m.sendFileDownloaded(ftIncomingRef(m, fileID, sender), sender)
	if sent != 1 {
		t.Errorf("expected no additional send after state change, got %d total", sent)
	}
}

// TestSendFileDownloadedSuppressedWhenMappingMissing verifies that
// sendFileDownloaded is a no-op when the mapping has been deleted
// (e.g. CleanupPeerTransfers raced with a deferred action).
func TestSendFileDownloadedSuppressedWhenMappingMissing(t *testing.T) {
	var sent int
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	// No mapping exists — sendFileDownloaded must not panic or send.
	m.sendFileDownloaded(ftIncomingRef(m, "nonexistent", domaintest.ID("sender")), domaintest.ID("sender"))
	if sent != 0 {
		t.Errorf("expected 0 sends for missing mapping, got %d", sent)
	}
}

// TestDeferredChunkRetrySuppressedAfterCancel verifies that a deferred
// actionRetryChunk does not emit a chunk_request when the mapping has been
// canceled between the tick snapshot and executeReceiverAction dispatch.
// The receiverStateIs guard inside executeReceiverAction detects the
// generation bump from CancelDownload and suppresses the stale action.
func TestDeferredChunkRetrySuppressedAfterCancel(t *testing.T) {
	dir := t.TempDir()
	var chunkReqs int

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkReqs++
			}
			return nil
		},
		stopCh:         make(chan struct{}),
		nextGeneration: 1, // must match mapping Generation so CancelDownload bumps to 2
	}

	sender := domaintest.ID("sender-deferred-chunk-guard-01")
	fileID := domain.FileID("deferred-chunk-guard")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "deadbeef",
		FileName:    "doc.bin",
		FileSize:    100,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
		Generation:  1,
		CreatedAt:   time.Now(),
	}

	// Build a deferred action that the tick loop would have created.
	action := receiverTickAction{
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionRetryChunk,
		requiredState: receiverDownloading,
		generation:    1,
		fileID:        ftIncomingRef(m, fileID, sender),
		sender:        sender,
		offset:        0,
		chunkSize:     domain.DefaultChunkSize,
	}

	// Simulate CancelDownload racing between the outer guard and dispatch.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}

	// The action's generation no longer matches — executeReceiverAction
	// must skip the chunk_request.
	m.executeReceiverAction(action)

	if chunkReqs != 0 {
		t.Errorf("expected 0 chunk_requests after cancel, got %d", chunkReqs)
	}
}

// TestDeferredResumeSuppressedAfterCancel verifies that a deferred
// actionResume does not emit a chunk_request or truncate the partial
// file when the mapping has been canceled.
func TestDeferredResumeSuppressedAfterCancel(t *testing.T) {
	dir := t.TempDir()
	var chunkReqs int

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if payload.Command == domain.FileActionChunkReq {
				chunkReqs++
			}
			return nil
		},
		stopCh:         make(chan struct{}),
		nextGeneration: 1, // must match mapping Generation so CancelDownload bumps to 2
	}

	sender := domaintest.ID("sender-deferred-resume-guard-01")
	fileID := domain.FileID("deferred-resume-guard")

	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "deadbeef",
		FileName:    "doc.bin",
		FileSize:    100,
		Sender:      sender,
		State:       receiverWaitingRoute,
		ChunkSize:   domain.DefaultChunkSize,
		Generation:  1,
		CreatedAt:   time.Now(),
	}

	action := receiverTickAction{
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionResume,
		requiredState: receiverWaitingRoute,
		generation:    1,
		fileID:        ftIncomingRef(m, fileID, sender),
		sender:        sender,
		snap: resumeSnapshot{
			partialPath: partialDownloadPath(m.downloadDir, fileID),
			fileID:      ftIncomingRef(m, fileID, sender),
			sender:      sender,
			startOffset: 0,
			chunkSize:   domain.DefaultChunkSize,
			prevState:   receiverWaitingRoute,
			generation:  1,
		},
	}

	// Cancel before the deferred action executes.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}

	m.executeReceiverAction(action)

	if chunkReqs != 0 {
		t.Errorf("expected 0 chunk_requests after cancel, got %d", chunkReqs)
	}
}

// TestRollbackSkippedAfterGenerationBump verifies that sendChunkWithRollback
// does not roll back a mapping when CancelDownload has bumped the generation
// between the send attempt and the rollback path. Without the generation
// guard, the stale rollback could corrupt a new download that reuses the same
// FileID and has already reached receiverDownloading with a newer generation.
func TestRollbackSkippedAfterGenerationBump(t *testing.T) {
	dir := t.TempDir()

	sendErr := fmt.Errorf("simulated network failure")
	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return sendErr
		},
		stopCh:         make(chan struct{}),
		nextGeneration: 1, // must match the mapping's Generation so CancelDownload bumps to 2
	}

	sender := domaintest.ID("sender-rollback-gen-01")
	fileID := domain.FileID("rollback-gen-guard")

	// Set up a mapping at generation 1 in receiverDownloading.
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "deadbeef",
		FileName:    "doc.bin",
		FileSize:    100,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
		Generation:  1,
		CreatedAt:   time.Now(),
	}

	// Snapshot taken at generation 1.
	snap := resumeSnapshot{
		partialPath:       partialDownloadPath(m.downloadDir, fileID),
		fileID:            ftIncomingRef(m, fileID, sender),
		sender:            sender,
		prevState:         receiverWaitingRoute,
		prevOffset:        0,
		prevBytesReceived: 0,
		startOffset:       0,
		chunkSize:         domain.DefaultChunkSize,
		generation:        1,
	}

	// Simulate CancelDownload + new download reaching receiverDownloading
	// at generation 2 before the stale rollback runs.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}
	// Simulate a new download starting for the same FileID.
	// CancelDownload already bumped generation (1 → 2) and reset to
	// receiverAvailable. The new download transitions to receiverDownloading
	// at the bumped generation, with fresh progress.
	m.mu.Lock()
	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
	rm.State = receiverDownloading
	rm.NextOffset = 4096
	rm.BytesReceived = 4096
	m.mu.Unlock()

	// sendChunkWithRollback fails (sendErr) and attempts rollback.
	// The rollback must be skipped because generation no longer matches.
	_ = m.sendChunkWithRollback(snap)

	m.mu.Lock()
	defer m.mu.Unlock()
	rmAfter := m.receiverMaps[ftIncomingRef(m, fileID, sender)]

	if rmAfter.State != receiverDownloading {
		t.Errorf("expected state receiverDownloading, got %s", rmAfter.State)
	}
	if rmAfter.NextOffset != 4096 {
		t.Errorf("expected NextOffset=4096 (new download progress preserved), got %d", rmAfter.NextOffset)
	}
	if rmAfter.BytesReceived != 4096 {
		t.Errorf("expected BytesReceived=4096 (new download progress preserved), got %d", rmAfter.BytesReceived)
	}
}

// TestGuardedResumeRollbackSkippedAfterGenerationBump verifies that
// guardedResume (the deferred actionResume path) does not roll back a
// mapping when send fails and the generation was bumped by CancelDownload.
// This is the deferred-action counterpart to TestRollbackSkippedAfterGenerationBump
// which tests the synchronous sendChunkWithRollback path.
func TestGuardedResumeRollbackSkippedAfterGenerationBump(t *testing.T) {
	dir := t.TempDir()

	m := &Manager{
		localID:      ftFixtureIdentity(),
		senderMaps:   make(map[domain.MessageRef]*senderFileMapping),
		receiverMaps: make(map[domain.MessageRef]*receiverFileMapping),
		downloadDir:  dir,
		sendCommand: func(_ domain.PeerIdentity, _ domain.FileCommandPayload) error {
			return fmt.Errorf("simulated network failure")
		},
		stopCh:         make(chan struct{}),
		nextGeneration: 1,
	}

	sender := domaintest.ID("sender-guarded-resume-gen-01")
	fileID := domain.FileID("guarded-resume-gen-guard")

	// Mapping at generation 1, state receiverDownloading (resume already
	// transitioned from waitingRoute → downloading in the tick).
	m.receiverMaps[ftIncomingRef(m, fileID, sender)] = &receiverFileMapping{
		Ref:         ftIncomingRef(m, fileID, sender),
		PartialPath: partialDownloadPath(m.downloadDir, fileID),
		FileID:      fileID,
		FileHash:    "deadbeef",
		FileName:    "doc.bin",
		FileSize:    100,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   domain.DefaultChunkSize,
		Generation:  1,
		CreatedAt:   time.Now(),
	}

	action := receiverTickAction{
		partialPath:   partialDownloadPath(m.downloadDir, fileID),
		kind:          actionResume,
		requiredState: receiverDownloading,
		generation:    1,
		fileID:        ftIncomingRef(m, fileID, sender),
		sender:        sender,
		snap: resumeSnapshot{
			partialPath:       partialDownloadPath(m.downloadDir, fileID),
			fileID:            ftIncomingRef(m, fileID, sender),
			sender:            sender,
			startOffset:       0,
			chunkSize:         domain.DefaultChunkSize,
			prevState:         receiverWaitingRoute,
			prevOffset:        0,
			prevBytesReceived: 0,
			generation:        1,
		},
	}

	// CancelDownload bumps generation 1 → 2, resets to receiverAvailable.
	if err := m.CancelDownload(fileID); err != nil {
		t.Fatalf("CancelDownload: %v", err)
	}

	// New download starts at generation 2, reaches receiverDownloading with progress.
	m.mu.Lock()
	rm := m.receiverMaps[ftIncomingRef(m, fileID, sender)]
	rm.State = receiverDownloading
	rm.NextOffset = 8192
	rm.BytesReceived = 8192
	m.mu.Unlock()

	// The stale deferred action fires. guardedResume's state check sees
	// generation 2 ≠ 1 and suppresses the send entirely. Even if we force
	// the state to match (both receiverDownloading), the generation mismatch
	// prevents both the send and any rollback.
	m.executeReceiverAction(action)

	m.mu.Lock()
	defer m.mu.Unlock()
	rmAfter := m.receiverMaps[ftIncomingRef(m, fileID, sender)]

	if rmAfter.State != receiverDownloading {
		t.Errorf("expected state receiverDownloading, got %s", rmAfter.State)
	}
	if rmAfter.NextOffset != 8192 {
		t.Errorf("expected NextOffset=8192 (new download progress preserved), got %d", rmAfter.NextOffset)
	}
	if rmAfter.BytesReceived != 8192 {
		t.Errorf("expected BytesReceived=8192 (new download progress preserved), got %d", rmAfter.BytesReceived)
	}
}
