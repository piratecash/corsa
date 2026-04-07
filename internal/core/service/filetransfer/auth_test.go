package filetransfer

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestUnauthorizedChunkRequestDropped verifies that a chunk_request from a
// peer whose identity does not match the mapping's Recipient is silently
// dropped. The sender must never serve file data to an unauthorized requester,
// even if the requester somehow knows a valid FileID.
func TestUnauthorizedChunkRequestDropped(t *testing.T) {
	var mu sync.Mutex
	var commandCount int

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			mu.Lock()
			commandCount++
			mu.Unlock()
			return nil
		},
		stopCh: make(chan struct{}),
	}

	legitimateReceiver := domain.PeerIdentity("11ed04572e7d37cbfb36f297e3027c72ae14b385")
	attacker := domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	fileID := domain.FileID("bf9e6c24-63ee-48b3-84fd-12cedb7f999a")

	// Register a sender mapping: file announced to legitimateReceiver.
	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  "abc123",
		FileName:  "secret.pdf",
		FileSize:  100000,
		Recipient: legitimateReceiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	// Attacker sends chunk_request with a valid FileID but wrong identity.
	m.HandleChunkRequest(attacker, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	// No chunk_response should be sent.
	mu.Lock()
	sent := commandCount
	mu.Unlock()

	if sent != 0 {
		t.Errorf("expected 0 commands sent to attacker, got %d", sent)
	}

	// Mapping state should remain unchanged (no transition to serving).
	mapping := m.senderMaps[fileID]
	if mapping.State != senderAnnounced {
		t.Errorf("state should remain announced, got %s", mapping.State)
	}
	if mapping.BytesServed != 0 {
		t.Errorf("bytes served should be 0, got %d", mapping.BytesServed)
	}
}

// TestUnknownFileIDChunkRequestDropped verifies that a chunk_request for a
// FileID not present in senderMaps is silently dropped.
func TestUnknownFileIDChunkRequestDropped(t *testing.T) {
	commandsSent := 0

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			commandsSent++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	unknownFileID := domain.FileID("00000000-0000-0000-0000-000000000000")
	attacker := domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	m.HandleChunkRequest(attacker, domain.ChunkRequestPayload{
		FileID: unknownFileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if commandsSent != 0 {
		t.Errorf("expected 0 commands sent for unknown file ID, got %d", commandsSent)
	}
}

// TestLegitimateReceiverGetsChunk verifies that the authorized receiver
// CAN request chunks — the mirror case of the unauthorized test above.
func TestLegitimateReceiverGetsChunk(t *testing.T) {
	dir := t.TempDir()

	// Create a transmit file.
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	fileHash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	chunkData := []byte("hello world chunk data for testing")
	storePath := dir + "/" + fileHash + ".txt"
	if err := writeTestFile(storePath, chunkData); err != nil {
		t.Fatalf("write test file: %v", err)
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

	legitimateReceiver := domain.PeerIdentity("11ed04572e7d37cbfb36f297e3027c72ae14b385")
	fileID := domain.FileID("test-file-id")

	m.senderMaps[fileID] = &senderFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "test.txt",
		FileSize:  uint64(len(chunkData)),
		Recipient: legitimateReceiver,
		State:     senderAnnounced,
		CreatedAt: time.Now(),
	}

	m.HandleChunkRequest(legitimateReceiver, domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: 0,
		Size:   domain.DefaultChunkSize,
	})

	if len(sentPayloads) != 1 {
		t.Fatalf("expected 1 chunk_response, got %d", len(sentPayloads))
	}

	if sentPayloads[0].Command != domain.FileActionChunkResp {
		t.Errorf("expected command %s, got %s", domain.FileActionChunkResp, sentPayloads[0].Command)
	}

	// State should transition to serving.
	if m.senderMaps[fileID].State != senderServing {
		t.Errorf("state should be serving, got %s", m.senderMaps[fileID].State)
	}
}

// TestRegisterFileReceive_RejectsMalformedHash verifies that RegisterFileReceive
// rejects file_announce payloads with an invalid SHA-256 hash and does not
// persist a receiver mapping. This prevents impossible-to-complete transfers
// from reaching the UI.
func TestRegisterFileReceive_RejectsMalformedHash(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")

	malformedHashes := []struct {
		name string
		hash string
	}{
		{"empty", ""},
		{"too_short", "abcdef1234"},
		{"too_long", "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2ff"},
		{"non_hex_chars", "zzzze3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"},
		{"glob_wildcard", "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2*3d4e5f6a1b2c3d4e5f6e1e"},
		{"path_traversal", "../../../../../../etc/passwd/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}

	for _, tc := range malformedHashes {
		fid := domain.FileID("malformed-" + tc.name)
		err := m.RegisterFileReceive(fid, tc.hash, "test.bin", "application/octet-stream", 1000, sender)
		if err == nil {
			t.Errorf("hash %q (%s): expected error, got nil", tc.hash, tc.name)
		}
		if _, exists := m.receiverMaps[fid]; exists {
			t.Errorf("hash %q (%s): mapping should not be persisted", tc.hash, tc.name)
		}
	}
}

// TestRegisterFileReceive_AcceptsValidHash verifies that a well-formed
// 64-character hex SHA-256 hash is accepted and the mapping is created.
func TestRegisterFileReceive_AcceptsValidHash(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("valid-hash-file")
	validHash := "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"

	err := m.RegisterFileReceive(fileID, validHash, "test.bin", "application/octet-stream", 1000, sender)
	if err != nil {
		t.Fatalf("RegisterFileReceive with valid hash: %v", err)
	}

	rm, exists := m.receiverMaps[fileID]
	if !exists {
		t.Fatal("mapping should be created for valid hash")
	}
	if rm.State != receiverAvailable {
		t.Errorf("state = %s, want available", rm.State)
	}
	if rm.FileHash != validHash {
		t.Errorf("hash = %s, want %s", rm.FileHash, validHash)
	}
}

// TestRegisterFileReceive_RejectsZeroSize verifies that a file_announce with
// file_size=0 is rejected. A zero-size transfer would immediately trigger
// onDownloadComplete with nothing to verify, creating a degenerate path.
func TestRegisterFileReceive_RejectsZeroSize(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("zero-size-file")
	validHash := "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"

	err := m.RegisterFileReceive(fileID, validHash, "test.bin", "application/octet-stream", 0, sender)
	if err == nil {
		t.Fatal("RegisterFileReceive should reject zero file size")
	}
	if _, exists := m.receiverMaps[fileID]; exists {
		t.Error("mapping should not be persisted for zero-size file")
	}
}

// TestRegisterFileReceive_RejectsEmptyFileName verifies that a file_announce
// with an empty or whitespace-only file name is rejected. Such transfers would
// appear as downloadable in the UI but with a degenerate "unnamed" placeholder.
func TestRegisterFileReceive_RejectsEmptyFileName(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	validHash := "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"

	emptyNames := []struct {
		name  string
		input string
	}{
		{"empty_string", ""},
		{"only_spaces", "   "},
		{"only_tabs", "\t\t"},
		{"whitespace_mix", " \t \n "},
	}

	for _, tc := range emptyNames {
		fid := domain.FileID("empty-name-" + tc.name)
		err := m.RegisterFileReceive(fid, validHash, tc.input, "application/octet-stream", 1000, sender)
		if err == nil {
			t.Errorf("name %q (%s): expected error, got nil", tc.input, tc.name)
		}
		if _, exists := m.receiverMaps[fid]; exists {
			t.Errorf("name %q (%s): mapping should not be persisted", tc.input, tc.name)
		}
	}
}

// TestRegisterFileReceive_AcceptsSanitizableFileName verifies that a non-empty
// file name that sanitizes to something other than "unnamed" is accepted,
// even if it contains path separators or other characters that get stripped.
func TestRegisterFileReceive_AcceptsSanitizableFileName(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	validHash := "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6e1e2"

	// Path traversal attempt — SanitizeFileName extracts "evil.txt", which is valid.
	fileID := domain.FileID("sanitizable-name")
	err := m.RegisterFileReceive(fileID, validHash, "../../../evil.txt", "application/octet-stream", 1000, sender)
	if err != nil {
		t.Fatalf("RegisterFileReceive should accept sanitizable name: %v", err)
	}
	if m.receiverMaps[fileID].FileName == "../../../evil.txt" {
		t.Error("file name should have been sanitized")
	}
}

// TestRegisterFileReceive_DuplicateReturnsNil verifies that re-registering
// an already-known file ID returns nil (idempotent), not an error.
func TestRegisterFileReceive_DuplicateReturnsNil(t *testing.T) {
	t.Parallel()

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		stopCh:       make(chan struct{}),
	}

	sender := domain.PeerIdentity("sender-identity-1234567890abcd")
	fileID := domain.FileID("dup-file")
	validHash := "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8"

	if err := m.RegisterFileReceive(fileID, validHash, "test.bin", "application/octet-stream", 1000, sender); err != nil {
		t.Fatalf("first register: %v", err)
	}

	if err := m.RegisterFileReceive(fileID, validHash, "test.bin", "application/octet-stream", 1000, sender); err != nil {
		t.Errorf("duplicate register should return nil, got: %v", err)
	}
}

func writeTestFile(path string, data []byte) error {
	return writeChunkToFile(path, 0, data, uint64(len(data)))
}
