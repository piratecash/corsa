package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// sha256Hex returns the hex-encoded SHA-256 of data. Used to generate valid
// 64-char hex hashes that pass domain.ValidateFileHash in test fixtures.
func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestAtomicWriteJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "test.json")

	data := map[string]string{"key": "value"}
	if err := atomicWriteJSON(path, data); err != nil {
		t.Fatalf("atomicWriteJSON failed: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["key"] != "value" {
		t.Errorf("expected key=value, got key=%s", got["key"])
	}

	// Temp file should not exist after successful rename.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful write")
	}
}

func TestSenderMappingRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	original := &senderFileMapping{
		FileID:       "msg-123",
		FileHash:     "abc123def456",
		FileName:     "photo.jpg",
		FileSize:     1024000,
		ContentType:  "image/jpeg",
		Recipient:    "peer-bob",
		State:        senderServing,
		CreatedAt:    now,
		CompletedAt:  time.Time{},
		BytesServed:  512000,
		TransmitPath: ".corsa/transmit/abc123def456.jpg",
	}

	entry := senderMappingToEntry(original)
	restored := entryToSenderMapping(entry)

	if restored.FileID != original.FileID {
		t.Errorf("FileID: got %q, want %q", restored.FileID, original.FileID)
	}
	if restored.FileHash != original.FileHash {
		t.Errorf("FileHash: got %q, want %q", restored.FileHash, original.FileHash)
	}
	if restored.FileName != original.FileName {
		t.Errorf("FileName: got %q, want %q", restored.FileName, original.FileName)
	}
	if restored.FileSize != original.FileSize {
		t.Errorf("FileSize: got %d, want %d", restored.FileSize, original.FileSize)
	}
	if restored.Recipient != original.Recipient {
		t.Errorf("Recipient: got %q, want %q", restored.Recipient, original.Recipient)
	}
	if restored.State != original.State {
		t.Errorf("State: got %q, want %q", restored.State, original.State)
	}
	if restored.BytesServed != original.BytesServed {
		t.Errorf("BytesServed: got %d, want %d", restored.BytesServed, original.BytesServed)
	}
	if restored.ContentType != original.ContentType {
		t.Errorf("ContentType: got %q, want %q", restored.ContentType, original.ContentType)
	}
	if restored.TransmitPath != original.TransmitPath {
		t.Errorf("TransmitPath: got %q, want %q", restored.TransmitPath, original.TransmitPath)
	}
}

func TestReceiverMappingRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	original := &receiverFileMapping{
		FileID:            "msg-456",
		FileHash:          "xyz789",
		FileName:          "doc.pdf",
		FileSize:          2048000,
		ContentType:       "application/pdf",
		Sender:            "peer-alice",
		State:             receiverWaitingAck,
		CreatedAt:         now,
		BytesReceived:     2048000,
		NextOffset:        2048000,
		ChunkSize:         domain.DefaultChunkSize,
		LastChunkAt:       now,
		DownloadedRetries: 3,
		DownloadedBackoff: 120 * time.Second,
		DownloadedSentAt:  now,
	}

	entry := receiverMappingToEntry(original)
	restored := entryToReceiverMapping(entry)

	if restored.FileID != original.FileID {
		t.Errorf("FileID: got %q, want %q", restored.FileID, original.FileID)
	}
	if restored.Sender != original.Sender {
		t.Errorf("Sender: got %q, want %q", restored.Sender, original.Sender)
	}
	if restored.State != original.State {
		t.Errorf("State: got %q, want %q", restored.State, original.State)
	}
	if restored.BytesReceived != original.BytesReceived {
		t.Errorf("BytesReceived: got %d, want %d", restored.BytesReceived, original.BytesReceived)
	}
	if restored.NextOffset != original.NextOffset {
		t.Errorf("NextOffset: got %d, want %d", restored.NextOffset, original.NextOffset)
	}
	if restored.DownloadedRetries != original.DownloadedRetries {
		t.Errorf("DownloadedRetries: got %d, want %d", restored.DownloadedRetries, original.DownloadedRetries)
	}
	if restored.DownloadedBackoff != original.DownloadedBackoff {
		t.Errorf("DownloadedBackoff: got %v, want %v", restored.DownloadedBackoff, original.DownloadedBackoff)
	}
	if !restored.DownloadedSentAt.Equal(original.DownloadedSentAt) {
		t.Errorf("DownloadedSentAt: got %v, want %v", restored.DownloadedSentAt, original.DownloadedSentAt)
	}
	if restored.ContentType != original.ContentType {
		t.Errorf("ContentType: got %q, want %q", restored.ContentType, original.ContentType)
	}
}

func TestLoadMappings_NoFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.json")

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	hashes := m.loadMappings()
	if hashes != nil {
		t.Errorf("expected nil hashes for nonexistent file, got %v", hashes)
	}
	if len(m.senderMaps) != 0 {
		t.Errorf("expected 0 sender maps, got %d", len(m.senderMaps))
	}
}

func TestLoadMappings_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(path, []byte("not json"), 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	hashes := m.loadMappings()
	if hashes != nil {
		t.Errorf("expected nil hashes for invalid JSON, got %v", hashes)
	}
}

func TestSaveThenLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	now := time.Now().Truncate(time.Second)

	// Use valid 64-char hex hashes that pass domain.ValidateFileHash.
	senderHash := sha256Hex([]byte("sender-content"))
	receiverHash := sha256Hex([]byte("receiver-content"))

	// Create manager with some mappings.
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	m.senderMaps["file-1"] = &senderFileMapping{
		FileID:      "file-1",
		FileHash:    senderHash,
		FileName:    "test.txt",
		FileSize:    100,
		ContentType: "text/plain",
		Recipient:   "bob",
		State:       senderAnnounced,
		CreatedAt:   now,
	}

	m.receiverMaps["file-2"] = &receiverFileMapping{
		FileID:        "file-2",
		FileHash:      receiverHash,
		FileName:      "image.png",
		FileSize:      50000,
		ContentType:   "image/png",
		Sender:        "alice",
		State:         receiverDownloading,
		CreatedAt:     now,
		BytesReceived: 25000,
		NextOffset:    25000,
		ChunkSize:     domain.DefaultChunkSize,
	}

	// Save.
	m.saveMappingsLocked()

	// Verify file exists.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("mappings file should exist: %v", err)
	}

	// Load into fresh manager.
	m2 := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	hashes := m2.loadMappings()

	// Verify sender loaded.
	sm, ok := m2.senderMaps["file-1"]
	if !ok {
		t.Fatal("sender mapping file-1 not loaded")
	}
	if sm.FileHash != senderHash {
		t.Errorf("sender FileHash: got %q, want %q", sm.FileHash, senderHash)
	}
	if sm.Recipient != "bob" {
		t.Errorf("sender Recipient: got %q, want %q", sm.Recipient, "bob")
	}
	if sm.State != senderAnnounced {
		t.Errorf("sender State: got %q, want %q", sm.State, senderAnnounced)
	}

	// Verify receiver loaded.
	rm, ok := m2.receiverMaps["file-2"]
	if !ok {
		t.Fatal("receiver mapping file-2 not loaded")
	}
	if rm.Sender != "alice" {
		t.Errorf("receiver Sender: got %q, want %q", rm.Sender, "alice")
	}
	if rm.BytesReceived != 25000 {
		t.Errorf("receiver BytesReceived: got %d, want %d", rm.BytesReceived, 25000)
	}
	if rm.State != receiverDownloading {
		t.Errorf("receiver State: got %q, want %q", rm.State, receiverDownloading)
	}

	// Active hashes: all non-tombstone senders hold a ref.
	if count, ok := hashes[senderHash]; !ok || count != 1 {
		t.Errorf("expected sender hash ref count 1, got %d (exists=%v)", count, ok)
	}
}

func TestLoadMappings_VersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	pf := persistedTransferFile{
		Version:   999,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	hashes := m.loadMappings()
	if hashes != nil {
		t.Errorf("expected nil for version mismatch, got %v", hashes)
	}
}

// TestSaveMappings_CompletedSenderHoldsRefCount verifies that a completed
// sender mapping produces a ref count in activeHashes. The transmit blob must
// remain available for re-downloads as long as the DM message exists.
func TestSaveMappings_CompletedSenderHoldsRefCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	doneHash := sha256Hex([]byte("done-content"))

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	m.senderMaps["file-done"] = &senderFileMapping{
		FileID:      "file-done",
		FileHash:    doneHash,
		FileName:    "done.txt",
		FileSize:    100,
		ContentType: "text/plain",
		Recipient:   "bob",
		State:       senderCompleted,
		CreatedAt:   time.Now(),
		CompletedAt: time.Now(),
	}

	m.saveMappingsLocked()

	// Load and verify completed sender DOES produce a ref count.
	m2 := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}

	hashes := m2.loadMappings()
	if count, ok := hashes[doneHash]; !ok || count != 1 {
		t.Errorf("completed sender should hold a ref count of 1, got %d (exists=%v)", count, ok)
	}
}

func TestSaveMappings_EmptyPath(t *testing.T) {
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: "",
	}

	// Should be a no-op, not panic.
	m.saveMappingsLocked()

	hashes := m.loadMappings()
	if hashes != nil {
		t.Error("expected nil for empty path")
	}
}

func TestTransfersMappingsPath(t *testing.T) {
	tests := []struct {
		name         string
		identity     domain.PeerIdentity
		listenAddr   domain.ListenAddress
		wantContains string
	}{
		{
			name:         "standard identity and port",
			identity:     "11ed0457abcdef1234567890abcdef1234567890",
			listenAddr:   ":64647",
			wantContains: "transfers-11ed0457-64647.json",
		},
		{
			name:         "short identity",
			identity:     "abcd",
			listenAddr:   ":9999",
			wantContains: "transfers-abcd-9999.json",
		},
		{
			name:         "full address with host",
			identity:     "aabbccddee112233445566778899aabbccddeeff",
			listenAddr:   "127.0.0.1:64646",
			wantContains: "transfers-aabbccdd-64646.json",
		},
		{
			name:         "no port in listen address",
			identity:     "1234567890abcdef",
			listenAddr:   "noport",
			wantContains: "transfers-12345678-default.json",
		},
	}

	dataDir := t.TempDir()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TransfersMappingsPath(dataDir, tt.identity, tt.listenAddr)
			if !strings.Contains(got, tt.wantContains) {
				t.Errorf("TransfersMappingsPath(%q, %q, %q) = %q, want to contain %q",
					dataDir, tt.identity, tt.listenAddr, got, tt.wantContains)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Reconcile verifying on startup tests
// ---------------------------------------------------------------------------

// TestReconcileVerifying_CompletedFileExists verifies that a receiverVerifying
// entry is promoted to receiverWaitingAck when the completed file exists in
// the downloads directory. This simulates a crash between os.Rename and the
// persistence of waitingAck state.
func TestReconcileVerifying_CompletedFileExists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}

	fileName := "report.pdf"

	// Simulate the completed file already sitting in downloads (post-rename).
	completedFile := filepath.Join(downloadDir, fileName)
	content := []byte("completed content")
	h := sha256.Sum256(content)
	fileHash := hex.EncodeToString(h[:])
	if err := os.WriteFile(completedFile, content, 0o644); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("crash-after-rename")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  fileName,
		FileSize:  uint64(len(content)),
		Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.reconcileVerifyingOnStartup()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingAck {
		t.Fatalf("state = %s, want waiting_ack", rm.State)
	}
	if rm.CompletedPath == "" {
		t.Fatal("CompletedPath should be set after reconciliation")
	}
}

// TestReconcileVerifying_PartialFileExists verifies that a receiverVerifying
// entry is reset to receiverWaitingRoute when the .part file exists but the
// completed file does not. This simulates a crash before os.Rename.
func TestReconcileVerifying_PartialFileExists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}

	fileID := domain.FileID("crash-before-rename")
	fileHash := "b1b2b3b4b5b6b1b2b3b4b5b6b1b2b3b4b5b6b1b2b3b4b5b6b1b2b3b4b5b6b1b2"

	// Write a partial file with some data.
	partialPath := partialDownloadPath(downloadDir, fileID)
	partialData := make([]byte, 4096)
	if err := os.WriteFile(partialPath, partialData, 0o600); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "data.bin",
		FileSize:  100000,
		Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.reconcileVerifyingOnStartup()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingRoute {
		t.Fatalf("state = %s, want waiting_route", rm.State)
	}
	if rm.NextOffset != 4096 {
		t.Errorf("NextOffset = %d, want 4096", rm.NextOffset)
	}
}

// TestReconcileVerifying_NoFilesOnDisk verifies that a receiverVerifying
// entry is marked failed when neither partial nor completed file exists.
func TestReconcileVerifying_NoFilesOnDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	fileID := domain.FileID("crash-no-files")
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "c1c2c3c4c5c6c1c2c3c4c5c6c1c2c3c4c5c6c1c2c3c4c5c6c1c2c3c4c5c6c1c2",
		FileName:  "gone.txt",
		FileSize:  1024,
		Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.reconcileVerifyingOnStartup()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverFailed {
		t.Fatalf("state = %s, want failed", rm.State)
	}
	if rm.CompletedAt.IsZero() {
		t.Fatal("CompletedAt must be set so tombstoneTTL cleanup uses the correct retention window")
	}
	// CompletedAt should be recent (within the last second), not zero-value
	// which would cause tickReceiverMappings to delete the entry immediately.
	if time.Since(rm.CompletedAt) > time.Second {
		t.Fatalf("CompletedAt = %v, expected a recent timestamp", rm.CompletedAt)
	}
}

// TestReconcileVerifying_OversizedPartialFileResetsOffset verifies that a
// receiverVerifying entry with a .part file larger than FileSize is recovered
// to receiverWaitingRoute with NextOffset=0. An oversized partial indicates
// corruption or tampering — trusting its size would produce an impossible
// resume offset.
func TestReconcileVerifying_OversizedPartialFileResetsOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}

	fileID := domain.FileID("crash-oversized-part")
	fileHash := "d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2"
	const fileSize uint64 = 10000

	// Write a .part file that exceeds FileSize — simulates corruption.
	partialPath := partialDownloadPath(downloadDir, fileID)
	oversizedData := make([]byte, fileSize+5000)
	if err := os.WriteFile(partialPath, oversizedData, 0o600); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		stopCh:       make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  fileHash,
		FileName:  "oversized.bin",
		FileSize:  fileSize,
		Sender:    domain.PeerIdentity("sender-identity-1234567890abcd"),
		State:     receiverVerifying,
		CreatedAt: time.Now(),
	}

	m.reconcileVerifyingOnStartup()

	rm := m.receiverMaps[fileID]
	if rm.State != receiverWaitingRoute {
		t.Fatalf("state = %s, want waiting_route", rm.State)
	}
	if rm.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0 (oversized partial must reset)", rm.NextOffset)
	}
	if rm.BytesReceived != 0 {
		t.Errorf("BytesReceived = %d, want 0", rm.BytesReceived)
	}
}

// ---------------------------------------------------------------------------
// Metadata validation on load tests
// ---------------------------------------------------------------------------

// TestLoadMappings_RejectsReceiverInvalidHash verifies that loadMappings
// skips receiver entries with a malformed file hash, matching the validation
// enforced by RegisterFileReceive at the network boundary.
func TestLoadMappings_RejectsReceiverInvalidHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "bad-hash-recv",
				FileHash: "not-a-valid-hex-hash",
				FileName: "doc.pdf",
				FileSize: 1024,
				Peer:     "sender-1234567890abcdef",
				Role:     "receiver",
				State:    string(receiverAvailable),
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	if _, exists := m.receiverMaps["bad-hash-recv"]; exists {
		t.Fatal("receiver entry with invalid hash should be skipped")
	}
}

// TestLoadMappings_RejectsReceiverZeroSize verifies that loadMappings
// skips receiver entries with FileSize == 0.
func TestLoadMappings_RejectsReceiverZeroSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	validHash := sha256Hex([]byte("zero-size-test"))

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "zero-size-recv",
				FileHash: validHash,
				FileName: "empty.bin",
				FileSize: 0,
				Peer:     "sender-1234567890abcdef",
				Role:     "receiver",
				State:    string(receiverAvailable),
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	if _, exists := m.receiverMaps["zero-size-recv"]; exists {
		t.Fatal("receiver entry with zero file size should be skipped")
	}
}

// TestLoadMappings_RejectsReceiverEmptyName verifies that loadMappings
// skips receiver entries where the original file name was empty.
func TestLoadMappings_RejectsReceiverEmptyName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	validHash := sha256Hex([]byte("empty-name-test"))

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "empty-name-recv",
				FileHash: validHash,
				FileName: "",
				FileSize: 512,
				Peer:     "sender-1234567890abcdef",
				Role:     "receiver",
				State:    string(receiverAvailable),
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	if _, exists := m.receiverMaps["empty-name-recv"]; exists {
		t.Fatal("receiver entry with empty file name should be skipped")
	}
}

// TestLoadMappings_RejectsSenderInvalidHash verifies that loadMappings
// skips sender entries with a malformed file hash.
func TestLoadMappings_RejectsSenderInvalidHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "bad-hash-send",
				FileHash: "ZZZZ-not-hex",
				FileName: "test.txt",
				FileSize: 100,
				Peer:     "recipient-1234567890abcdef",
				Role:     "sender",
				State:    string(senderAnnounced),
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	hashes := m.loadMappings()

	if _, exists := m.senderMaps["bad-hash-send"]; exists {
		t.Fatal("sender entry with invalid hash should be skipped")
	}
	if len(hashes) != 0 {
		t.Errorf("expected no active hashes, got %v", hashes)
	}
}

// TestLoadMappings_AcceptsValidEntries verifies that valid sender and
// receiver entries pass the new validation and load normally.
func TestLoadMappings_AcceptsValidEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	sHash := sha256Hex([]byte("valid-sender"))
	rHash := sha256Hex([]byte("valid-receiver"))

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "valid-send",
				FileHash: sHash,
				FileName: "report.pdf",
				FileSize: 2048,
				Peer:     "recipient-abc",
				Role:     "sender",
				State:    string(senderAnnounced),
			},
			{
				FileID:   "valid-recv",
				FileHash: rHash,
				FileName: "photo.jpg",
				FileSize: 4096,
				Peer:     "sender-xyz",
				Role:     "receiver",
				State:    string(receiverAvailable),
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	hashes := m.loadMappings()

	if _, ok := m.senderMaps["valid-send"]; !ok {
		t.Error("valid sender entry should be loaded")
	}
	if _, ok := m.receiverMaps["valid-recv"]; !ok {
		t.Error("valid receiver entry should be loaded")
	}
	if count := hashes[sHash]; count != 1 {
		t.Errorf("expected sender hash ref count 1, got %d", count)
	}
}

// TestLoadMappings_RejectsSenderUnknownState verifies that loadMappings
// skips sender entries with a state string that is not one of the known
// senderState values. A corrupted or tampered transfers.json could contain
// arbitrary state strings that the runtime state machine cannot handle.
func TestLoadMappings_RejectsSenderUnknownState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	validHash := sha256Hex([]byte("sender-unknown-state"))

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "bogus-sender",
				FileHash: validHash,
				FileName: "test.bin",
				FileSize: 100,
				Peer:     "recipient-1234567890abcdef",
				Role:     "sender",
				State:    "bogus_state",
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	hashes := m.loadMappings()

	if _, exists := m.senderMaps["bogus-sender"]; exists {
		t.Fatal("sender entry with unknown state should be skipped")
	}
	if len(hashes) != 0 {
		t.Errorf("expected no active hashes, got %v", hashes)
	}
}

// TestLoadMappings_RejectsReceiverUnknownState verifies that loadMappings
// skips receiver entries with a state string that is not one of the known
// receiverState values.
func TestLoadMappings_RejectsReceiverUnknownState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	validHash := sha256Hex([]byte("receiver-unknown-state"))

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:   "bogus-receiver",
				FileHash: validHash,
				FileName: "photo.jpg",
				FileSize: 4096,
				Peer:     "sender-1234567890abcdef",
				Role:     "receiver",
				State:    "invented_state",
			},
		},
	}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	if _, exists := m.receiverMaps["bogus-receiver"]; exists {
		t.Fatal("receiver entry with unknown state should be skipped")
	}
}

// TestLoadMappings_AcceptsAllKnownSenderStates verifies that loadMappings
// accepts all defined senderState values.
func TestLoadMappings_AcceptsAllKnownSenderStates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	states := []senderState{senderAnnounced, senderServing, senderCompleted, senderTombstone}
	entries := make([]persistedTransferEntry, len(states))
	for i, st := range states {
		entries[i] = persistedTransferEntry{
			FileID:   domain.FileID(fmt.Sprintf("sender-%s", st)),
			FileHash: sha256Hex([]byte(fmt.Sprintf("sender-state-%s", st))),
			FileName: "file.bin",
			FileSize: 100,
			Peer:     "recipient-1234567890abcdef",
			Role:     "sender",
			State:    string(st),
		}
	}

	pf := persistedTransferFile{Version: persistVersion, UpdatedAt: time.Now(), Transfers: entries}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	for _, st := range states {
		fid := domain.FileID(fmt.Sprintf("sender-%s", st))
		sm, ok := m.senderMaps[fid]
		if !ok {
			t.Errorf("sender with state %q should be loaded", st)
			continue
		}
		if sm.State != st {
			t.Errorf("sender %s: state = %q, want %q", fid, sm.State, st)
		}
	}
}

// TestLoadMappings_AcceptsAllKnownReceiverStates verifies that loadMappings
// accepts all defined receiverState values (none are rejected by the state
// validation gate). receiverVerifying is excluded from the post-load state
// assertion because reconcileVerifyingOnStartup transitions it to failed
// when no files exist on disk — that behavior is covered by the dedicated
// TestReconcileVerifying_* tests.
func TestLoadMappings_AcceptsAllKnownReceiverStates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "transfers.json")

	states := []receiverState{
		receiverAvailable, receiverDownloading, receiverVerifying,
		receiverWaitingAck, receiverCompleted, receiverFailed, receiverWaitingRoute,
	}
	entries := make([]persistedTransferEntry, len(states))
	for i, st := range states {
		entries[i] = persistedTransferEntry{
			FileID:   domain.FileID(fmt.Sprintf("recv-%s", st)),
			FileHash: sha256Hex([]byte(fmt.Sprintf("recv-state-%s", st))),
			FileName: "file.bin",
			FileSize: 100,
			Peer:     "sender-1234567890abcdef",
			Role:     "receiver",
			State:    string(st),
		}
	}

	pf := persistedTransferFile{Version: persistVersion, UpdatedAt: time.Now(), Transfers: entries}
	data, _ := json.Marshal(pf)
	_ = os.WriteFile(path, data, 0o600)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		mappingsPath: path,
	}
	m.loadMappings()

	for _, st := range states {
		fid := domain.FileID(fmt.Sprintf("recv-%s", st))
		rm, ok := m.receiverMaps[fid]
		if !ok {
			t.Errorf("receiver with state %q should be loaded (not rejected by state validation)", st)
			continue
		}
		// reconcileVerifyingOnStartup mutates verifying entries after load;
		// the reconciliation outcome is tested separately.
		if st == receiverVerifying {
			continue
		}
		if rm.State != st {
			t.Errorf("receiver %s: state = %q, want %q", fid, rm.State, st)
		}
	}
}

// TestEntryToReceiverMapping_ZeroChunkSizeNormalized verifies that
// entryToReceiverMapping backfills ChunkSize=0 with DefaultChunkSize.
// Without this normalization, HandleChunkResponse rejects every non-empty
// chunk as oversized because len(chunk) > 0 == mapping.ChunkSize.
func TestEntryToReceiverMapping_ZeroChunkSizeNormalized(t *testing.T) {
	t.Parallel()

	entry := persistedTransferEntry{
		Role:      "receiver",
		FileID:    "zero-chunk-file",
		FileHash:  "a1a2a3a4a5a6a7a8b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8",
		FileName:  "data.bin",
		FileSize:  100000,
		Peer:      "sender-identity-1234567890abcd",
		State:     string(receiverWaitingRoute),
		ChunkSize: 0, // legacy or corrupted entry
		CreatedAt: time.Now(),
	}

	rm := entryToReceiverMapping(entry)
	if rm.ChunkSize != domain.DefaultChunkSize {
		t.Errorf("ChunkSize = %d, want %d (DefaultChunkSize)", rm.ChunkSize, domain.DefaultChunkSize)
	}
}

// TestEntryToReceiverMapping_NonZeroChunkSizePreserved verifies that
// entryToReceiverMapping preserves a valid non-zero ChunkSize.
func TestEntryToReceiverMapping_NonZeroChunkSizePreserved(t *testing.T) {
	t.Parallel()

	entry := persistedTransferEntry{
		Role:      "receiver",
		FileID:    "custom-chunk-file",
		FileHash:  "b1b2b3b4b5b6b7b8c1c2c3c4c5c6c7c8d1d2d3d4d5d6d7d8e1e2e3e4e5e6e7e8",
		FileName:  "data.bin",
		FileSize:  50000,
		Peer:      "sender-identity-1234567890abcd",
		State:     string(receiverDownloading),
		ChunkSize: 8192,
		CreatedAt: time.Now(),
	}

	rm := entryToReceiverMapping(entry)
	if rm.ChunkSize != 8192 {
		t.Errorf("ChunkSize = %d, want 8192 (preserved)", rm.ChunkSize)
	}
}

// ---------------------------------------------------------------------------
// Startup reconciliation: active sender mapping with missing transmit file
// ---------------------------------------------------------------------------

// TestLoadMappingsTombstonesActiveSenderWithMissingBlob simulates a scenario
// where the transmit blob was externally deleted while the mapping was active.
// On disk the mapping is still in "announced" state, but the transmit file
// is missing. loadMappings must detect this and tombstone the mapping so it
// never attempts to serve chunks for a nonexistent blob.
func TestLoadMappingsTombstonesActiveSenderWithMissingBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	// Persist a sender mapping in "announced" state — simulating a crash
	// that prevented the senderCompleted state from being saved.
	mappingsPath := filepath.Join(dir, "transfers.json")
	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:      "orphaned-sender",
				FileHash:    hash,
				FileName:    "ghost.bin",
				FileSize:    5000,
				ContentType: "application/octet-stream",
				Peer:        "recipient-identity-1234567890ab",
				Role:        "sender",
				State:       string(senderAnnounced),
				CreatedAt:   time.Now().Add(-time.Hour),
			},
		},
	}
	if err := atomicWriteJSON(mappingsPath, pf); err != nil {
		t.Fatalf("write test mappings: %v", err)
	}

	// Do NOT create the transmit file — simulating post-Release deletion.

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}

	activeHashes := m.loadMappings()

	// The mapping must be loaded but tombstoned.
	sm, ok := m.senderMaps["orphaned-sender"]
	if !ok {
		t.Fatal("sender mapping should be present after loadMappings")
	}
	if sm.State != senderTombstone {
		t.Errorf("state = %s, want tombstone (transmit file missing)", sm.State)
	}

	// The hash must NOT be in activeHashes — tombstoned mappings don't hold refs.
	if count, exists := activeHashes[hash]; exists && count > 0 {
		t.Errorf("activeHashes[%s] = %d, want 0 (tombstoned mapping should not hold refs)", hash, count)
	}
}

// TestLoadMappingsResurrectsTombstoneWhenBlobExists verifies that a tombstoned
// sender mapping is resurrected to senderCompleted when the transmit blob is
// found on disk. This handles the migration from the old behavior (Release on
// complete deleted the blob → tombstone on restart) to the new behavior (blob
// stays for re-downloads).
func TestLoadMappingsResurrectsTombstoneWhenBlobExists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"

	// The blob IS on disk (restored or re-sent).
	if err := os.WriteFile(filepath.Join(transmitDir, hash+".bin"), []byte("file-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Persist a tombstoned sender mapping.
	mappingsPath := filepath.Join(dir, "transfers.json")
	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:      "resurrected-sender",
				FileHash:    hash,
				FileName:    "photo.png",
				FileSize:    9,
				ContentType: "image/png",
				Peer:        "recipient-identity-1234567890ab",
				Role:        "sender",
				State:       string(senderTombstone),
				CreatedAt:   time.Now().Add(-time.Hour),
				CompletedAt: time.Now().Add(-30 * time.Minute),
			},
		},
	}
	if err := atomicWriteJSON(mappingsPath, pf); err != nil {
		t.Fatalf("write test mappings: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}

	activeHashes := m.loadMappings()

	// The mapping must be resurrected to completed.
	sm, ok := m.senderMaps["resurrected-sender"]
	if !ok {
		t.Fatal("sender mapping should be present after loadMappings")
	}
	if sm.State != senderCompleted {
		t.Errorf("state = %s, want completed (blob is on disk, should be resurrected)", sm.State)
	}

	// The hash must be in activeHashes — resurrected mapping holds a ref.
	if count, ok := activeHashes[hash]; !ok || count != 1 {
		t.Errorf("activeHashes[%s] = %d, want 1 (resurrected mapping holds ref)", hash, count)
	}
}

// TestLoadMappingsKeepsActiveSenderWithExistingBlob verifies that when the
// transmit file exists, the active sender mapping is loaded normally.
func TestLoadMappingsKeepsActiveSenderWithExistingBlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	hash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	// Create the transmit file so it exists on disk.
	if err := os.WriteFile(filepath.Join(transmitDir, hash+".bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	mappingsPath := filepath.Join(dir, "transfers.json")
	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{
			{
				FileID:      "healthy-sender",
				FileHash:    hash,
				FileName:    "real.bin",
				FileSize:    4,
				ContentType: "application/octet-stream",
				Peer:        "recipient-identity-1234567890ab",
				Role:        "sender",
				State:       string(senderServing),
				CreatedAt:   time.Now().Add(-time.Hour),
			},
		},
	}
	if err := atomicWriteJSON(mappingsPath, pf); err != nil {
		t.Fatalf("write test mappings: %v", err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}

	activeHashes := m.loadMappings()

	sm, ok := m.senderMaps["healthy-sender"]
	if !ok {
		t.Fatal("sender mapping should be present after loadMappings")
	}
	if sm.State != senderServing {
		t.Errorf("state = %s, want serving (transmit file exists)", sm.State)
	}

	if activeHashes[hash] != 1 {
		t.Errorf("activeHashes[%s] = %d, want 1", hash, activeHashes[hash])
	}
}

// TestOnDownloadCompleteProceedsOnPersistFailure verifies that when the
// receiverVerifying → receiverWaitingAck transition fails to persist, the
// in-memory state stays at receiverWaitingAck and file_downloaded IS sent.
//
// Rolling back to receiverVerifying would strand the transfer for the entire
// session: tickReceiverMappings has no case for receiverVerifying, and the
// .part file is gone after rename. The completed file is already durably
// stored — keeping waitingAck in memory lets the ack retry cycle proceed.
// If the process crashes, reconcileVerifyingOnStartup Case 1 finds the
// completed file and reconstructs waitingAck from disk.
func TestOnDownloadCompleteProceedsOnPersistFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}

	// Create an unwritable directory so saveMappingsLockedErr fails.
	readonlyDir := filepath.Join(dir, "readonly")
	if err := os.MkdirAll(readonlyDir, 0o500); err != nil {
		t.Fatal(err)
	}
	unwritablePath := filepath.Join(readonlyDir, "transfers.json")

	// Track whether sendCommand was called — it MUST be called now.
	var sendCalled bool
	sendFn := func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
		sendCalled = true
		return nil
	}

	content := []byte("verified-file-content-for-persist-test")
	h := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(h[:])

	fileID := domain.FileID("persist-rollback-test")
	sender := domain.PeerIdentity("sender-identity-1234567890ab")

	// Create the partial file (onDownloadComplete will verify and rename it).
	partialPath := filepath.Join(downloadDir, "partial", string(fileID)+".part")
	if err := os.WriteFile(partialPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		mappingsPath: unwritablePath,
		sendCommand:  sendFn,
		stopCh:       make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:        fileID,
		FileHash:      expectedHash,
		FileName:      "test.bin",
		FileSize:      uint64(len(content)),
		Sender:        sender,
		State:         receiverDownloading,
		CreatedAt:     time.Now(),
		BytesReceived: uint64(len(content)),
	}

	// Call onDownloadComplete — persist will fail but transfer must proceed.
	m.onDownloadComplete(fileID, partialPath, expectedHash, sender)

	// Mapping must be receiverWaitingAck — persist failure is non-fatal
	// after the completed file is durably renamed.
	m.mu.Lock()
	mapping := m.receiverMaps[fileID]
	state := mapping.State
	completedPath := mapping.CompletedPath
	m.mu.Unlock()

	if state != receiverWaitingAck {
		t.Errorf("state = %s, want receiverWaitingAck (persist failure is non-fatal after rename)", state)
	}
	if completedPath == "" {
		t.Error("CompletedPath must be set — completed file is on disk")
	}
	if !sendCalled {
		t.Error("sendCommand must be called — file_downloaded notifies sender that receiver has the file")
	}
}

// TestReconcileVerifyingPersistsToDisk verifies that after startup
// reconciliation repairs a receiverVerifying entry, the repaired state is
// written to transfers.json immediately. Without this, the same entry is
// reloaded as receiverVerifying on every restart — CompletedAt resets each
// boot and the tombstone TTL never ages out.
func TestReconcileVerifyingPersistsToDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	if err := os.MkdirAll(filepath.Join(downloadDir, "partial"), 0o700); err != nil {
		t.Fatal(err)
	}
	mappingsPath := filepath.Join(dir, "transfers.json")

	fileID := domain.FileID("reconcile-persist-test")
	sender := domain.PeerIdentity("sender-identity-1234567890ab")
	fileHash := "d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2d3d4d5d6d1d2"

	// Write a transfers.json with a receiverVerifying entry (no files on disk
	// → Case 3 → failed).
	initial := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{{
			FileID:    fileID,
			FileHash:  fileHash,
			FileName:  "missing.bin",
			FileSize:  1024,
			Peer:      sender,
			Role:      "receiver",
			State:     "verifying",
			CreatedAt: time.Now().Add(-time.Hour),
		}},
	}
	data, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mappingsPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a manager and trigger loadMappings (via constructor).
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}
	m.loadMappings()

	// In-memory state: must be failed.
	rm := m.receiverMaps[fileID]
	if rm == nil {
		t.Fatal("receiver mapping not loaded")
	}
	if rm.State != receiverFailed {
		t.Fatalf("in-memory state = %s, want failed", rm.State)
	}

	// Read transfers.json back from disk and verify persisted state.
	diskData, err := os.ReadFile(mappingsPath)
	if err != nil {
		t.Fatalf("read persisted file: %v", err)
	}
	var onDisk persistedTransferFile
	if err := json.Unmarshal(diskData, &onDisk); err != nil {
		t.Fatalf("unmarshal persisted file: %v", err)
	}

	if len(onDisk.Transfers) != 1 {
		t.Fatalf("expected 1 transfer on disk, got %d", len(onDisk.Transfers))
	}
	if onDisk.Transfers[0].State != "failed" {
		t.Errorf("on-disk state = %q, want \"failed\"", onDisk.Transfers[0].State)
	}
	if onDisk.Transfers[0].CompletedAt.IsZero() {
		t.Error("on-disk CompletedAt must be set so tombstone TTL ages correctly")
	}
}

// TestSenderTombstonePersistsToDisk verifies that when loadMappings tombstones
// an active sender mapping whose transmit blob is missing, the repaired state
// is written to transfers.json immediately. Without persisting, the same entry
// would be reloaded in its old active state on every restart, CompletedAt
// would reset on each boot, and the tombstone retention TTL would never age.
func TestSenderTombstonePersistsToDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	transmitDir := filepath.Join(dir, "transmit")
	store, err := NewFileStore(transmitDir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	fileID := domain.FileID("sender-persist-tombstone")
	hash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	mappingsPath := filepath.Join(dir, "transfers.json")

	// Write a transfers.json with an active sender entry whose blob does not
	// exist on disk — simulating a crash between store.Release and persist.
	initial := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: []persistedTransferEntry{{
			FileID:    fileID,
			FileHash:  hash,
			FileName:  "ghost.bin",
			FileSize:  4096,
			Peer:      domain.PeerIdentity("recipient-identity-1234567890ab"),
			Role:      "sender",
			State:     string(senderAnnounced),
			CreatedAt: time.Now().Add(-time.Hour),
		}},
	}
	data, marshalErr := json.Marshal(initial)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if writeErr := os.WriteFile(mappingsPath, data, 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}

	// Do NOT create the transmit file — blob is missing.

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		store:        store,
		downloadDir:  filepath.Join(dir, "downloads"),
		mappingsPath: mappingsPath,
		stopCh:       make(chan struct{}),
	}
	m.loadMappings()

	// In-memory state: must be tombstone.
	sm := m.senderMaps[fileID]
	if sm == nil {
		t.Fatal("sender mapping not loaded")
	}
	if sm.State != senderTombstone {
		t.Fatalf("in-memory state = %s, want tombstone", sm.State)
	}

	// Read transfers.json back from disk and verify persisted state.
	diskData, readErr := os.ReadFile(mappingsPath)
	if readErr != nil {
		t.Fatalf("read persisted file: %v", readErr)
	}
	var onDisk persistedTransferFile
	if unmarshalErr := json.Unmarshal(diskData, &onDisk); unmarshalErr != nil {
		t.Fatalf("unmarshal persisted file: %v", unmarshalErr)
	}

	if len(onDisk.Transfers) != 1 {
		t.Fatalf("expected 1 transfer on disk, got %d", len(onDisk.Transfers))
	}
	if onDisk.Transfers[0].State != string(senderTombstone) {
		t.Errorf("on-disk state = %q, want %q", onDisk.Transfers[0].State, senderTombstone)
	}
	if onDisk.Transfers[0].CompletedAt.IsZero() {
		t.Error("on-disk CompletedAt must be set so tombstone TTL ages correctly")
	}
}
