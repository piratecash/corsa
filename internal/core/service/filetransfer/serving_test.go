package filetransfer

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestHandleChunkRequest_ServesBatchLargerThanFormerServingLimit verifies
// that the sender accepts and serves a large batch of distinct chunk_requests
// from the same receiver in one go — significantly more than the historical
// 16-mapping serving cap. This pins the post-cap behaviour: the sender no
// longer silently drops over-the-limit requests, so the receiver never burns
// retry budget against an artificial bound.
func TestHandleChunkRequest_ServesBatchLargerThanFormerServingLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const batchSize = 32 // 2x the historical maxConcurrentServing (16).

	// Each file needs its own content-addressed blob on disk so ReadChunk
	// returns real bytes for every concurrent serve.
	hashes := make([]string, batchSize)
	contents := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		// Distinct deterministic 32-byte hex hashes.
		hashes[i] = fmt.Sprintf(
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"+
				"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
			i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i,
			i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i,
		)
		contents[i] = []byte(fmt.Sprintf("payload for file %d", i))
		filePath := filepath.Join(dir, hashes[i]+".bin")
		if err := os.WriteFile(filePath, contents[i], 0o600); err != nil {
			t.Fatal(err)
		}
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
			if payload.Command == domain.FileActionChunkResp {
				sentCount++
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	for i := 0; i < batchSize; i++ {
		fid := domain.FileID(fmt.Sprintf("batch-file-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  hashes[i],
			FileName:  fmt.Sprintf("file-%d.bin", i),
			FileSize:  uint64(len(contents[i])),
			Recipient: receiver,
			State:     senderAnnounced,
			CreatedAt: time.Now(),
		}
	}

	for i := 0; i < batchSize; i++ {
		fid := domain.FileID(fmt.Sprintf("batch-file-%d", i))
		m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
			FileID: fid,
			Offset: 0,
			Size:   domain.DefaultChunkSize,
		})
	}

	if sentCount != batchSize {
		t.Errorf("served %d/%d chunk_responses; expected every batch request to be served (no concurrency cap)", sentCount, batchSize)
	}

	for i := 0; i < batchSize; i++ {
		fid := domain.FileID(fmt.Sprintf("batch-file-%d", i))
		if got := m.senderMaps[fid].State; got != senderServing {
			t.Errorf("file %s state = %s, want senderServing", fid, got)
		}
	}
}

// TestHandleChunkRequest_RevivalGatedByMappingQuota verifies that reviving
// a senderCompleted mapping into senderServing is gated by maxFileMappings.
// Terminal mappings (completed, tombstone) are excluded from the active
// quota and live for tombstoneTTL (30 days), so without this gate an
// authorised peer could batch-revive an unbounded number of old transfers
// and trivially overshoot the quota that PrepareFileAnnounce enforces on
// fresh announces.
func TestHandleChunkRequest_RevivalGatedByMappingQuota(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	revivalHash := "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"
	revivalContent := []byte("revival blob content")
	if err := os.WriteFile(filepath.Join(dir, revivalHash+".bin"), revivalContent, 0o600); err != nil {
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
			if payload.Command == domain.FileActionChunkResp {
				sentCount++
			}
			return nil
		},
		stopCh: make(chan struct{}),
	}

	receiver := domain.PeerIdentity("receiver-identity")

	// Saturate the active quota with senderAnnounced mappings — these
	// count toward activeSenderCountLocked and represent in-flight
	// first-time transfers competing for the same cap.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("active-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("active-hash-%d", i),
			FileName:  fmt.Sprintf("active-%d.bin", i),
			FileSize:  10000,
			Recipient: receiver,
			State:     senderAnnounced,
			CreatedAt: time.Now(),
		}
	}

	// One extra senderCompleted mapping kept around for re-download.
	revivalID := domain.FileID("revival-target")
	m.senderMaps[revivalID] = &senderFileMapping{
		FileID:      revivalID,
		FileHash:    revivalHash,
		FileName:    "revival.bin",
		FileSize:    uint64(len(revivalContent)),
		Recipient:   receiver,
		State:       senderCompleted,
		CreatedAt:   time.Now(),
		CompletedAt: time.Now(),
	}

	// Active count is already at the cap → revival must be rejected,
	// no chunk_response sent, mapping stays senderCompleted.
	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: revivalID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 0 {
		t.Errorf("revival at quota cap should not send chunk_response, got %d", sentCount)
	}
	if got := m.senderMaps[revivalID].State; got != senderCompleted {
		t.Errorf("revival rejected: state = %s, want senderCompleted (unchanged)", got)
	}

	// Drop one announced mapping → quota frees by exactly one slot.
	delete(m.senderMaps, domain.FileID("active-0"))

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: revivalID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if sentCount != 1 {
		t.Errorf("after freeing a slot, revival should serve one chunk_response, got %d", sentCount)
	}
	if got := m.senderMaps[revivalID].State; got != senderServing {
		t.Errorf("after revival: state = %s, want senderServing", got)
	}
}

// TestHandleChunkRequest_TombstoneRevivalGatedBeforeAcquire verifies that
// the mapping quota gate fires before tombstone resurrection. A rejected
// request must not Acquire a transmit-blob ref it never gets to use —
// otherwise repeated rejected requests would pin blobs alive for 30 days
// (until tombstoneTTL) just by exhausting their retry budget.
func TestHandleChunkRequest_TombstoneRevivalGatedBeforeAcquire(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	tombstoneHash := "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
	tombstoneContent := []byte("orphaned blob present on disk")
	if err := os.WriteFile(filepath.Join(dir, tombstoneHash+".bin"), tombstoneContent, 0o600); err != nil {
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

	receiver := domain.PeerIdentity("receiver-identity")

	// Saturate the active quota.
	for i := 0; i < maxFileMappings; i++ {
		fid := domain.FileID(fmt.Sprintf("active-%d", i))
		m.senderMaps[fid] = &senderFileMapping{
			FileID:    fid,
			FileHash:  fmt.Sprintf("active-hash-%d", i),
			FileName:  fmt.Sprintf("active-%d.bin", i),
			FileSize:  10000,
			Recipient: receiver,
			State:     senderAnnounced,
			CreatedAt: time.Now(),
		}
	}

	tombstoneID := domain.FileID("tombstone-target")
	m.senderMaps[tombstoneID] = &senderFileMapping{
		FileID:      tombstoneID,
		FileHash:    tombstoneHash,
		FileName:    "tombstone.bin",
		FileSize:    uint64(len(tombstoneContent)),
		Recipient:   receiver,
		State:       senderTombstone,
		CreatedAt:   time.Now(),
		CompletedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: tombstoneID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// The mapping must remain in senderTombstone — quota gate fired
	// before the resurrection block.
	if got := m.senderMaps[tombstoneID].State; got != senderTombstone {
		t.Errorf("tombstone state changed to %s; quota gate should reject before resurrection", got)
	}

	// And the store must not hold a ref for this hash — Acquire was
	// never called.
	store.mu.Lock()
	gotRef := store.refs[tombstoneHash]
	store.mu.Unlock()
	if gotRef != 0 {
		t.Errorf("store ref for tombstone hash = %d, want 0 (Acquire must not run on rejected revival)", gotRef)
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

// TestHandleChunkRequest_AnnouncedTransitionsToServing verifies the basic
// happy path: a chunk_request for a senderAnnounced mapping promotes it to
// senderServing and triggers exactly one chunk_response send.
func TestHandleChunkRequest_AnnouncedTransitionsToServing(t *testing.T) {
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

// TestHandleChunkRequest_SendFailureDoesNotPoisonOtherTransfers verifies
// that a failed serve attempt for one file does not interfere with a later
// chunk_request for a different file. The first request rolls back to its
// previous state on send failure; the second request must transition into
// serving and complete its chunk_response normally.
func TestHandleChunkRequest_SendFailureDoesNotPoisonOtherTransfers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	failHash := "f1f2f3f4f5f6f7f8a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8"
	failContent := []byte("first transfer fails its send")
	if err := os.WriteFile(filepath.Join(dir, failHash+".bin"), failContent, 0o600); err != nil {
		t.Fatal(err)
	}

	successHash := "a0b0c0d0e0f0a1b1c1d1e1f1a2b2c2d2e2f2a3b3c3d3e3f3a4b4c4d4e4f4a5b5"
	successContent := []byte("second transfer succeeds")
	if err := os.WriteFile(filepath.Join(dir, successHash+".bin"), successContent, 0o600); err != nil {
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

	failID := domain.FileID("will-fail")
	m.senderMaps[failID] = &senderFileMapping{
		FileID:    failID,
		FileHash:  failHash,
		FileName:  "fail.bin",
		FileSize:  uint64(len(failContent)),
		Recipient: receiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(receiver, domain.ChunkRequestPayload{
		FileID: failID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if m.senderMaps[failID].State != senderAnnounced {
		t.Errorf("failed transfer should roll back to announced, state = %s", m.senderMaps[failID].State)
	}

	// Second transfer with sendCommand restored — must succeed independently.
	sendFails = false

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
		t.Errorf("second transfer state = %s, want senderServing", m.senderMaps[successID].State)
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
