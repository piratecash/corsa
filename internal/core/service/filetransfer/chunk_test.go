package filetransfer

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/domain"
)

// TestHandleChunkResponse_RejectsOversizedChunk verifies that a chunk_response
// whose decoded payload exceeds the mapping's ChunkSize is dropped without
// writing to disk. A malicious or misconfigured sender could push chunks
// larger than requested, causing memory/disk pressure and advancing NextOffset
// by an unexpected amount.
func TestHandleChunkResponse_RejectsOversizedChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("malicious-sender")
	fileID := domain.FileID("oversized-test")
	chunkSize := uint32(1024) // 1 KB requested

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "somehash",
		FileName:  "test.bin",
		FileSize:  1_000_000,
		Sender:    sender,
		State:     receiverDownloading,
		ChunkSize: chunkSize,
		CreatedAt: time.Now(),
	}

	// Create a chunk that is 2x the requested size.
	oversizedData := make([]byte, chunkSize*2)
	for i := range oversizedData {
		oversizedData[i] = 0xAB
	}
	encoded := base64.RawURLEncoding.EncodeToString(oversizedData)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	// Mapping should be unchanged — offset not advanced.
	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != 0 {
		t.Errorf("NextOffset should remain 0 after oversized chunk, got %d", mapping.NextOffset)
	}
	if mapping.BytesReceived != 0 {
		t.Errorf("BytesReceived should remain 0 after oversized chunk, got %d", mapping.BytesReceived)
	}

	// Partial file should not have been created.
	partialPath := partialDownloadPath(downloadDir, fileID)
	if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
		t.Error("partial file should not exist after rejecting oversized chunk")
	}
}

// TestHandleChunkResponse_AcceptsExactSizeChunk verifies that a chunk whose
// decoded size equals exactly ChunkSize is accepted normally.
func TestHandleChunkResponse_AcceptsExactSizeChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("good-sender")
	fileID := domain.FileID("exact-size-test")
	chunkSize := uint32(512)
	fileSize := uint64(2048)

	var nextChunkRequested bool
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			nextChunkRequested = true
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "somehash",
		FileName:  "test.bin",
		FileSize:  fileSize,
		Sender:    sender,
		State:     receiverDownloading,
		ChunkSize: chunkSize,
		CreatedAt: time.Now(),
	}

	// Exact-size chunk.
	exactData := make([]byte, chunkSize)
	encoded := base64.RawURLEncoding.EncodeToString(exactData)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != uint64(chunkSize) {
		t.Errorf("NextOffset should advance to %d, got %d", chunkSize, mapping.NextOffset)
	}
	if mapping.BytesReceived != uint64(chunkSize) {
		t.Errorf("BytesReceived should be %d, got %d", chunkSize, mapping.BytesReceived)
	}
	if !nextChunkRequested {
		t.Error("next chunk should have been requested")
	}
}

// TestHandleChunkResponse_AcceptsSmallerLastChunk verifies that a chunk
// smaller than ChunkSize is accepted (this is normal for the last chunk
// of a file).
func TestHandleChunkResponse_AcceptsSmallerLastChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("good-sender")
	fileID := domain.FileID("small-last-chunk")
	chunkSize := uint32(1024)
	fileSize := uint64(100) // entire file smaller than one chunk

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  strings.Repeat("a", 64), // placeholder
		FileName:  "tiny.bin",
		FileSize:  fileSize,
		Sender:    sender,
		State:     receiverDownloading,
		ChunkSize: chunkSize,
		CreatedAt: time.Now(),
	}

	smallData := make([]byte, fileSize)
	encoded := base64.RawURLEncoding.EncodeToString(smallData)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != fileSize {
		t.Errorf("NextOffset should advance to %d, got %d", fileSize, mapping.NextOffset)
	}
}

// TestHandleChunkResponse_RejectsOneByteOverLimit verifies the boundary:
// ChunkSize+1 bytes is rejected.
func TestHandleChunkResponse_RejectsOneByteOverLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("boundary-sender")
	fileID := domain.FileID("boundary-test")
	chunkSize := uint32(256)

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:    fileID,
		FileHash:  "somehash",
		FileName:  "test.bin",
		FileSize:  1_000_000,
		Sender:    sender,
		State:     receiverDownloading,
		ChunkSize: chunkSize,
		CreatedAt: time.Now(),
	}

	// One byte over the limit.
	overData := make([]byte, chunkSize+1)
	encoded := base64.RawURLEncoding.EncodeToString(overData)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != 0 {
		t.Errorf("NextOffset should remain 0 after chunk_size+1 rejection, got %d", mapping.NextOffset)
	}
}

// TestHandleChunkResponse_RejectsEmptyChunkBeforeComplete verifies that a
// zero-length chunk_response is dropped when the download is not yet complete.
// Without this check, an empty chunk at the expected offset would:
//   - pass writeChunkToFile (no bytes written),
//   - refresh LastChunkAt (defeating stall detection),
//   - leave NextOffset unchanged,
//   - immediately re-request the same offset,
//
// creating a tight no-progress loop (livelock).
func TestHandleChunkResponse_RejectsEmptyChunkBeforeComplete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("livelock-sender")
	fileID := domain.FileID("empty-chunk-test")
	chunkSize := uint32(1024)
	fileSize := uint64(4096)

	var requestCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			requestCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	initialTime := time.Now().Add(-10 * time.Second)
	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:      fileID,
		FileHash:    "somehash",
		FileName:    "test.bin",
		FileSize:    fileSize,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   chunkSize,
		CreatedAt:   time.Now(),
		LastChunkAt: initialTime,
	}

	// Send empty chunk at offset 0.
	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   "", // empty base64 → zero bytes
	})

	mapping := m.receiverMaps[fileID]

	// NextOffset must not advance (was already 0, stays 0).
	if mapping.NextOffset != 0 {
		t.Errorf("NextOffset should remain 0, got %d", mapping.NextOffset)
	}

	// BytesReceived must not advance.
	if mapping.BytesReceived != 0 {
		t.Errorf("BytesReceived should remain 0, got %d", mapping.BytesReceived)
	}

	// LastChunkAt must NOT be refreshed — stall detection must still work.
	if mapping.LastChunkAt != initialTime {
		t.Error("LastChunkAt should not be refreshed by empty chunk")
	}

	// No next chunk should have been requested.
	if requestCount != 0 {
		t.Errorf("no chunk request should be sent after empty chunk, got %d", requestCount)
	}
}

// TestHandleChunkResponse_RejectsUndersizedNonFinalChunk verifies that a
// non-final chunk smaller than ChunkSize is rejected. A truncated response
// would shift all subsequent offsets and guarantee a hash mismatch at
// verification — rejecting early avoids wasting bandwidth.
func TestHandleChunkResponse_RejectsUndersizedNonFinalChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("truncating-sender")
	fileID := domain.FileID("undersize-chunk-test")
	chunkSize := uint32(1024)
	fileSize := uint64(4096)

	var requestCount int
	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			requestCount++
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:      fileID,
		FileHash:    strings.Repeat("b", 64),
		FileName:    "test.bin",
		FileSize:    fileSize,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   chunkSize,
		CreatedAt:   time.Now(),
		LastChunkAt: time.Now(),
	}

	// Send 500 bytes for a non-final chunk (ChunkSize=1024, FileSize=4096).
	// offset 0 + 500 = 500 < 4096 → not the last chunk → undersized.
	truncatedData := make([]byte, 500)
	encoded := base64.RawURLEncoding.EncodeToString(truncatedData)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != 0 {
		t.Errorf("NextOffset should remain 0 (undersized rejected), got %d", mapping.NextOffset)
	}
	if requestCount != 0 {
		t.Errorf("no chunk request should be sent after undersized rejection, got %d", requestCount)
	}
}

// TestHandleChunkResponse_AcceptsUndersizedFinalChunk verifies that the
// last chunk of a file is allowed to be smaller than ChunkSize. The sender
// clamps the chunk to the remaining bytes, so this is normal behaviour.
func TestHandleChunkResponse_AcceptsUndersizedFinalChunk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	downloadDir := filepath.Join(dir, "downloads")
	partialDir := filepath.Join(downloadDir, "partial")
	_ = os.MkdirAll(partialDir, 0o700)

	sender := domain.PeerIdentity("final-chunk-sender")
	fileID := domain.FileID("final-undersize-chunk")
	chunkSize := uint32(1024)
	fileSize := uint64(500) // entire file is smaller than one chunk

	m := &Manager{
		senderMaps:   make(map[domain.FileID]*senderFileMapping),
		receiverMaps: make(map[domain.FileID]*receiverFileMapping),
		downloadDir:  downloadDir,
		sendCommand: func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error {
			return nil
		},
		stopCh: make(chan struct{}),
	}

	m.receiverMaps[fileID] = &receiverFileMapping{
		FileID:      fileID,
		FileHash:    strings.Repeat("c", 64),
		FileName:    "small.bin",
		FileSize:    fileSize,
		Sender:      sender,
		State:       receiverDownloading,
		ChunkSize:   chunkSize,
		CreatedAt:   time.Now(),
		LastChunkAt: time.Now(),
	}

	// 500 bytes for a 500-byte file → final chunk → undersized is OK.
	data := make([]byte, fileSize)
	encoded := base64.RawURLEncoding.EncodeToString(data)

	m.HandleChunkResponse(sender, domain.ChunkResponsePayload{
		FileID: fileID,
		Offset: 0,
		Data:   encoded,
	})

	mapping := m.receiverMaps[fileID]
	if mapping.NextOffset != fileSize {
		t.Errorf("NextOffset should advance to %d (final chunk accepted), got %d", fileSize, mapping.NextOffset)
	}
}
