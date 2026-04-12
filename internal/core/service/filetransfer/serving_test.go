package filetransfer

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestHandleChunkRequest_ServingConcurrencyLimitEnforced verifies that
// HandleChunkRequest rejects new chunk_requests when maxConcurrentServing
// mappings are already in senderServing state. This is the regression test
// for the declared-but-unenforced concurrency limit.
func TestHandleChunkRequest_ServingConcurrencyLimitEnforced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	// Fill up to the concurrency limit with senderServing mappings.
	for i := 0; i < maxConcurrentServing; i++ {
		fid := domain.FileID(fmt.Sprintf("serving-file-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("hash-%d", i),
			FileName:  fmt.Sprintf("file-%d.bin", i),
			FileSize:  10000,
			Recipient: receiver,
			State:     senderServing,
			CreatedAt: time.Now(),
		}
	}

	// Add one more mapping in senderAnnounced — this is the one we'll try.
	overflowID := domain.FileID("overflow-file")
	m.senderMaps[overflowID] = &senderFileMapping{
		FileID:    overflowID,
		FileHash:  "overflow-hash",
		FileName:  "overflow.bin",
		FileSize:  10000,
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Attempt to request a chunk — should be rejected by the limit.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: overflowID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// Mapping should remain in senderAnnounced (not transitioned to serving).
	if m.senderMaps[overflowID].State != senderAnnounced {
		t.Errorf("overflow mapping should remain announced, got %s", m.senderMaps[overflowID].State)
	}

	// No chunk_response should have been sent.
	if sentCount != 0 {
		t.Errorf("expected 0 chunk_responses when at concurrency limit, got %d", sentCount)
	}
}

// TestHandleChunkRequest_AlreadyServingNotBlockedByLimit verifies that a
// mapping already in senderServing state is served even when the concurrency
// limit is fully used — it already holds a slot.
func TestHandleChunkRequest_AlreadyServingNotBlockedByLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a real file so ReadChunk succeeds.
	fileHash := "c1c2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6c1c2"
	fileContent := []byte("test chunk data for serving")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	// Fill concurrency slots with senderServing mappings (including ours).
	for i := 0; i < maxConcurrentServing-1; i++ {
		fid := domain.FileID(fmt.Sprintf("serving-file-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("hash-%d", i),
			FileName:  fmt.Sprintf("file-%d.bin", i),
			FileSize:  10000,
			Recipient: receiver,
			State:     senderServing,
			CreatedAt: time.Now(),
		}
	}

	// This mapping is already serving — it holds the last slot.
	activeID := domain.FileID("active-serving")
	m.senderMaps[activeID] = &senderFileMapping{
		FileID:    activeID,
		FileHash:  fileHash,
		FileName:  "test.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderServing,
		CreatedAt: time.Now(),
	}

	// Request chunk for the already-serving file — should succeed despite
	// being at the limit.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: activeID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 1 {
		t.Errorf("expected 1 chunk_response for already-serving file, got %d", sentCount)
	}
}

func TestHandleChunkRequest_RepeatedOffsetDoesNotAdvanceSenderProgress(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	fileContent := []byte("sender progress should not double-count repeated offsets")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-progress")
	fileID := domain.FileID("repeated-offset-progress")
	chunkSize := uint32(8)
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "progress.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   chunkSize,
	})

	bytesTransferred, totalSize, state, found := m.SenderProgress(fileID)
	if !found {
		t.Fatal("SenderProgress: mapping not found")
	}
	if totalSize != uint64(len(fileContent)) {
		t.Fatalf("SenderProgress totalSize = %d, want %d", totalSize, len(fileContent))
	}
	if state != string(senderServing) {
		t.Fatalf("SenderProgress state = %q, want %q", state, senderServing)
	}
	if bytesTransferred != uint64(chunkSize) {
		t.Fatalf("SenderProgress after first chunk = %d, want %d", bytesTransferred, chunkSize)
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   chunkSize,
	})

	bytesTransferred, _, _, found = m.SenderProgress(fileID)
	if !found {
		t.Fatal("SenderProgress after retry: mapping not found")
	}
	if bytesTransferred != uint64(chunkSize) {
		t.Errorf("SenderProgress after repeated offset = %d, want %d", bytesTransferred, chunkSize)
	}

	if m.senderMaps[fileID].BytesServed != uint64(chunkSize)*2 {
		t.Errorf("BytesServed = %d, want %d", m.senderMaps[fileID].BytesServed, uint64(chunkSize)*2)
	}
	if m.senderMaps[fileID].ProgressBytes != uint64(chunkSize) {
		t.Errorf("ProgressBytes = %d, want %d", m.senderMaps[fileID].ProgressBytes, chunkSize)
	}
}

// TestHandleChunkRequest_BelowLimitAllowed verifies that chunk_requests below
// the concurrency limit transition normally to senderServing.
func TestHandleChunkRequest_BelowLimitAllowed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "d1d2d3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6d1d2"
	fileContent := []byte("chunk data for below-limit test")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	fileID := domain.FileID("new-file")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "test.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[fileID].State != senderServing {
		t.Errorf("mapping should transition to serving, got %s", m.senderMaps[fileID].State)
	}
	if sentCount != 1 {
		t.Errorf("expected 1 chunk_response, got %d", sentCount)
	}
}

// TestHandleChunkRequest_ReadChunkFailureRollsBackState verifies that when
// ReadChunk fails, the mapping state is rolled back from senderServing to its
// previous value so the serving slot is freed.
func TestHandleChunkRequest_ReadChunkFailureRollsBackState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Do NOT create any file — ReadChunk will fail with file-not-found.
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")
	fileID := domain.FileID("readfail-file")
	fileHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "missing.bin",
		FileSize:  10000,
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// State should be rolled back to senderAnnounced so the serving slot is freed.
	if m.senderMaps[fileID].State != senderAnnounced {
		t.Errorf("state after ReadChunk failure = %s, want announced (rolled back)", m.senderMaps[fileID].State)
	}
	if sentCount != 0 {
		t.Errorf("no chunk_response should be sent on ReadChunk failure, got %d", sentCount)
	}
}

// TestHandleChunkRequest_SendFailureRollsBackState verifies that when
// sendCommand fails, the mapping state is rolled back from senderServing to
// its previous value so the serving slot is freed.
func TestHandleChunkRequest_SendFailureRollsBackState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "e1e2e3e4e5e6e7e8f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8"
	fileContent := []byte("chunk data for send-failure test")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return fmt.Errorf("simulated send failure")
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")
	fileID := domain.FileID("sendfail-file")

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "test.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// State should be rolled back to senderAnnounced so the serving slot is freed.
	if m.senderMaps[fileID].State != senderAnnounced {
		t.Errorf("state after sendCommand failure = %s, want announced (rolled back)", m.senderMaps[fileID].State)
	}
}

// TestHandleChunkRequest_SendFailureFreesSlotForNextTransfer verifies the
// end-to-end consequence: a failed serve attempt must not permanently block
// a new transfer from acquiring the serving slot.
func TestHandleChunkRequest_SendFailureFreesSlotForNextTransfer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8"
	fileContent := []byte("slot-leak integration test data")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	sendFails := true
	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			if sendFails {
				return fmt.Errorf("transient failure")
			}
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	// Fill concurrency slots to maxConcurrentServing - 1 with real serving mappings.
	for i := 0; i < maxConcurrentServing-1; i++ {
		fid := domain.FileID(fmt.Sprintf("serving-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("hash-%d", i),
			FileName:  fmt.Sprintf("file-%d.bin", i),
			FileSize:  10000,
			Recipient: receiver,
			State:     senderServing,
			CreatedAt: time.Now(),
		}
	}

	// This mapping will fail its send — it should not consume the last slot.
	failID := domain.FileID("will-fail")
	m.senderMaps[failID] = &senderFileMapping{
		FileID:    failID,
		FileHash:  fileHash,
		FileName:  "fail.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: failID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// Now fix sends and try a different announced file — it should succeed
	// because the failed attempt freed the slot.
	sendFails = false

	successHash := "a0b0c0d0e0f0a1b1c1d1e1f1a2b2c2d2e2f2a3b3c3d3e3f3a4b4c4d4e4f4a5b5"
	successContent := []byte("success data")
	successPath := filepath.Join(dir, successHash+".bin")
	if err := os.WriteFile(successPath, successContent, 0o600); err != nil {
		t.Fatal(err)
	}

	successID := domain.FileID("should-succeed")
	m.senderMaps[successID] = &senderFileMapping{
		FileID:    successID,
		FileHash:  successHash,
		FileName:  "success.bin",
		FileSize:  uint64(len(successContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: successID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[successID].State != senderServing {
		t.Errorf("second transfer should succeed after failed attempt freed slot, state = %s", m.senderMaps[successID].State)
	}
	if sentCount != 1 {
		t.Errorf("expected 1 successful chunk_response, got %d", sentCount)
	}
}

// ---------------------------------------------------------------------------
// Offset validation
// ---------------------------------------------------------------------------

// TestHandleChunkRequest_RejectsOffsetBeyondFileSize verifies that a
// chunk_request with offset >= FileSize is rejected without reading from disk
// or transitioning the mapping to senderServing.
func TestHandleChunkRequest_RejectsOffsetBeyondFileSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "f1f2f3f4f5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6f1f2"
	fileContent := []byte("small file content")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")
	fileID := domain.FileID("offset-beyond-eof")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "test.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Request at exact FileSize — should be rejected.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: uint64(len(fileContent)),
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 0 {
		t.Errorf("expected 0 sends for offset==FileSize, got %d", sentCount)
	}
	if m.senderMaps[fileID].State != senderAnnounced {
		t.Errorf("state should remain announced, got %s", m.senderMaps[fileID].State)
	}

	// Request well beyond FileSize — should also be rejected.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: uint64(len(fileContent)) + 10000,
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 0 {
		t.Errorf("expected 0 sends for offset>FileSize, got %d", sentCount)
	}
}

// TestHandleChunkRequest_ClampsChunkSizeToRemainingBytes verifies that when
// the requested chunk extends past EOF the sender clamps the read to the
// remaining bytes instead of relying on ReadChunk's EOF handling.
func TestHandleChunkRequest_ClampsChunkSizeToRemainingBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	fileHash := "a1a2a3a4a5a6b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1a2"
	fileContent := []byte("exactly 27 bytes of content")
	filePath := filepath.Join(dir, fileHash+".bin")
	if err := os.WriteFile(filePath, fileContent, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	var sentCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			sentCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")
	fileID := domain.FileID("clamp-chunk-size")
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "test.bin",
		FileSize:  uint64(len(fileContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Request a full 16KB chunk starting at offset 20. Only 7 bytes remain.
	// The sender should clamp and still produce a valid chunk_response.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 20,
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 1 {
		t.Errorf("expected 1 chunk_response for clamped read, got %d", sentCount)
	}
	if m.senderMaps[fileID].State != senderServing {
		t.Errorf("state should be serving, got %s", m.senderMaps[fileID].State)
	}
	// BytesServed should reflect the clamped amount (27 - 20 = 7).
	if m.senderMaps[fileID].BytesServed != 7 {
		t.Errorf("BytesServed = %d, want 7", m.senderMaps[fileID].BytesServed)
	}
}
