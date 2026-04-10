package filetransfer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Receiver state machine
// ---------------------------------------------------------------------------

// receiverState tracks a single file being downloaded from a sender.
type receiverState string

const (
	receiverAvailable    receiverState = "available"     // file_announce received, not yet downloading
	receiverDownloading  receiverState = "downloading"   // actively requesting chunks
	receiverVerifying    receiverState = "verifying"     // all chunks received, verifying hash
	receiverWaitingAck   receiverState = "waiting_ack"   // file_downloaded sent, awaiting ack
	receiverCompleted    receiverState = "completed"     // file_downloaded_ack received
	receiverFailed       receiverState = "failed"        // download failed (hash mismatch, etc.)
	receiverWaitingRoute receiverState = "waiting_route" // sender offline, paused
)

// validReceiverStates enumerates all known receiver states. Used by loadMappings
// to reject persisted entries with corrupted or unknown state strings.
var validReceiverStates = map[receiverState]struct{}{
	receiverAvailable:    {},
	receiverDownloading:  {},
	receiverVerifying:    {},
	receiverWaitingAck:   {},
	receiverCompleted:    {},
	receiverFailed:       {},
	receiverWaitingRoute: {},
}

// ---------------------------------------------------------------------------
// Receiver file mapping
// ---------------------------------------------------------------------------

// receiverFileMapping tracks a file being received from a specific sender.
type receiverFileMapping struct {
	FileID        domain.FileID
	FileHash      string
	FileName      string
	FileSize      uint64
	ContentType   string
	Sender        domain.PeerIdentity
	State         receiverState
	CreatedAt     time.Time
	CompletedAt   time.Time
	BytesReceived uint64
	NextOffset    uint64
	ChunkSize     uint32
	LastChunkAt   time.Time

	// Retry state for chunk_request resends (stall recovery).
	ChunkRetries int

	// Retry state for file_downloaded resends.
	DownloadedSentAt  time.Time
	DownloadedRetries int
	DownloadedBackoff time.Duration

	// CompletedPath is the final path after successful download and
	// verification. Used by cleanup to locate the file on disk.
	CompletedPath string

	// Generation is incremented every time the mapping is recycled
	// (re-registered or restarted for the same FileID). Deferred actions
	// capture the generation at creation time and skip execution if it has
	// changed, preventing stale cleanup from deleting a freshly created
	// .part file that belongs to a newer transfer attempt.
	Generation uint64

	// ServingEpoch is the sender's epoch observed on the most recent
	// chunk_response for this FileID. It is echoed back in file_downloaded
	// so the sender can reject stale replays from a prior serving run of
	// the same FileID (re-downloads reuse the FileID but bump the sender
	// epoch). A zero value means no epoch has been observed yet (legacy
	// sender, or no chunk_response has arrived since the mapping was
	// registered / reset); in that case file_downloaded carries Epoch=0
	// and the sender falls back to the legacy (un-gated) accept path.
	ServingEpoch uint64
}

// newReceiverMapping creates a receiverFileMapping with all domain invariants
// enforced. All production code must use this constructor instead of raw
// struct literals to ensure consistent normalization. Test code may use
// literals directly when testing edge cases.
func newReceiverMapping(
	fileID domain.FileID,
	fileHash string,
	fileName string,
	fileSize uint64,
	contentType string,
	sender domain.PeerIdentity,
	state receiverState,
) *receiverFileMapping {
	rm := &receiverFileMapping{
		FileID:      fileID,
		FileHash:    fileHash,
		FileName:    fileName,
		FileSize:    fileSize,
		ContentType: contentType,
		Sender:      sender,
		State:       state,
		CreatedAt:   time.Now(),
		ChunkSize:   domain.DefaultChunkSize,
	}
	return rm
}

// normalizeReceiverMapping enforces domain invariants on a restored mapping.
// Called after deserialization to fix zero/invalid values that would cause
// silent failures at runtime (e.g. ChunkSize=0 makes HandleChunkResponse
// reject every non-empty chunk as oversized).
func normalizeReceiverMapping(rm *receiverFileMapping) {
	if rm.ChunkSize == 0 {
		rm.ChunkSize = domain.DefaultChunkSize
	}
	if rm.NextOffset > rm.FileSize {
		rm.NextOffset = 0
		rm.BytesReceived = 0
	}
	if rm.BytesReceived > rm.FileSize {
		rm.BytesReceived = rm.FileSize
	}
}

// ---------------------------------------------------------------------------
// Receiver-side public operations
// ---------------------------------------------------------------------------

// RegisterFileReceive registers a new incoming file transfer after receiving
// a file_announce DM. Does not start downloading — call StartDownload.
// Returns an error if the announce metadata is invalid (malformed hash,
// empty file name, zero size) to prevent impossible-to-complete transfers
// from being persisted.
func (m *Manager) RegisterFileReceive(
	fileID domain.FileID,
	fileHash, fileName, contentType string,
	fileSize uint64,
	sender domain.PeerIdentity,
) error {
	if err := domain.ValidateFileHash(fileHash); err != nil {
		return fmt.Errorf("file %s: invalid announce metadata: %w", fileID, err)
	}
	if fileSize == 0 {
		return fmt.Errorf("file %s: invalid announce metadata: file size is zero", fileID)
	}

	// Sanitize the file name at the network boundary to prevent path
	// traversal attacks. The original name arrives from the remote peer
	// inside the encrypted file_announce payload.
	safeFileName := domain.SanitizeFileName(fileName)

	if safeFileName == "unnamed" && strings.TrimSpace(fileName) == "" {
		return fmt.Errorf("file %s: invalid announce metadata: file name is empty", fileID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.receiverMaps[fileID]; exists {
		return nil // already registered
	}

	if m.activeReceiverCountLocked() >= maxFileMappings {
		return fmt.Errorf("receiver mapping limit reached (%d)", maxFileMappings)
	}

	rm := newReceiverMapping(
		fileID, fileHash, safeFileName, fileSize, contentType, sender,
		receiverAvailable,
	)
	m.nextGeneration++
	rm.Generation = m.nextGeneration
	m.receiverMaps[fileID] = rm

	m.saveMappingsLocked()

	log.Info().
		Str("file_id", string(fileID)).
		Str("sender", string(sender)).
		Str("file_name", safeFileName).
		Msg("file_transfer: registered receiver mapping")

	return nil
}

// StartDownload begins downloading a file by sending the first chunk_request.
// Transitions from available → downloading.
func (m *Manager) StartDownload(fileID domain.FileID) error {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[fileID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no receiver mapping for %s", fileID)
	}

	if mapping.State != receiverAvailable && mapping.State != receiverWaitingRoute {
		m.mu.Unlock()
		return fmt.Errorf("cannot start download in state %s", mapping.State)
	}

	// Check concurrent download limit.
	if m.activeDownloadCountLocked() >= maxConcurrentDownloads {
		m.mu.Unlock()
		return fmt.Errorf("concurrent download limit reached (%d)", maxConcurrentDownloads)
	}

	// Verify the sender is reachable before starting — no point in
	// transitioning to downloading and burning retry budget if the
	// peer is offline.
	if m.peerReachable != nil && !m.peerReachable(mapping.Sender) {
		mapping.State = receiverWaitingRoute
		m.saveMappingsLocked()
		m.mu.Unlock()
		return fmt.Errorf("sender %s is not reachable, waiting for route", mapping.Sender)
	}

	// Ensure partial download directory exists.
	partialDir := filepath.Join(m.downloadDir, "partial")
	if err := os.MkdirAll(partialDir, 0o700); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("create partial dir: %w", err)
	}

	// For a fresh start from available, ensure the offset is 0 before
	// prepareResumeLocked captures the snapshot.
	if mapping.State == receiverAvailable {
		mapping.NextOffset = 0
		mapping.BytesReceived = 0
	}

	// Reset ack-retry counters (only relevant for StartDownload, not
	// for background auto-resume which preserves them).
	mapping.DownloadedRetries = 0
	mapping.DownloadedBackoff = 0
	mapping.DownloadedSentAt = time.Time{}

	snap := m.prepareResumeLocked(fileID, mapping)
	m.saveMappingsLocked()
	m.mu.Unlock()

	m.truncatePartialFile(snap)

	log.Info().
		Str("file_id", string(fileID)).
		Str("sender", string(snap.sender)).
		Uint64("offset", snap.startOffset).
		Uint32("chunk_size", snap.chunkSize).
		Msg("file_transfer: sending chunk_request")

	if err := m.sendChunkWithRollback(snap); err != nil {
		return fmt.Errorf("initial chunk request: %w", err)
	}

	return nil
}

// HandleChunkResponse processes a chunk_response from the sender.
// Writes chunk data to the partial file and requests the next chunk.
func (m *Manager) HandleChunkResponse(
	senderIdentity domain.PeerIdentity,
	resp domain.ChunkResponsePayload,
) {
	m.mu.Lock()
	prep, err := m.validateChunkResponseLocked(senderIdentity, resp)
	if err != nil {
		return // validateChunkResponseLocked already unlocked and logged
	}

	// Write chunk to partial file (outside lock). If the write fails, the
	// state machine stays at the previous offset and stall recovery retries.
	if err := writeChunkToFile(prep.partialPath, prep.offset, prep.chunkData, prep.fileSize); err != nil {
		log.Error().Err(err).Str("file_id", string(resp.FileID)).Msg("file_transfer: write chunk failed")
		return
	}

	// Commit progress under lock with re-validation.
	m.mu.Lock()
	nextOffset, ok := m.commitChunkProgressLocked(resp.FileID, prep.offset, len(prep.chunkData))
	m.mu.Unlock()

	if !ok {
		return
	}

	if nextOffset >= prep.fileSize {
		m.onDownloadComplete(resp.FileID, prep.partialPath, prep.fileHash, prep.sender)
		return
	}

	if err := m.requestNextChunk(resp.FileID, prep.sender, nextOffset, prep.chunkSize); err != nil {
		log.Warn().Err(err).
			Str("file_id", string(resp.FileID)).
			Uint64("offset", nextOffset).
			Msg("file_transfer: request next chunk failed, will retry via stall detector")
	}
}

// HandleFileDownloadedAck processes a file_downloaded_ack from the sender.
// Transitions from waiting_ack → completed.
//
// Epoch gating: if the mapping observed a non-zero ServingEpoch during the
// current run, the incoming ack must echo the same value. This rejects a
// stale ack left over from a prior serving run of the same FileID (e.g.
// cancel + restart cycle that rebound a new epoch). Epoch==0 on either
// side is the legacy path and accepted unconditionally for wire
// compatibility during rolling upgrades.
func (m *Manager) HandleFileDownloadedAck(
	senderIdentity domain.PeerIdentity,
	ack domain.FileDownloadedAckPayload,
) {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[ack.FileID]
	if !ok {
		m.mu.Unlock()
		return
	}

	if senderIdentity != mapping.Sender {
		m.mu.Unlock()
		return
	}

	if mapping.State != receiverWaitingAck {
		m.mu.Unlock()
		return
	}

	if ack.Epoch != 0 && mapping.ServingEpoch != 0 && ack.Epoch != mapping.ServingEpoch {
		currentEpoch := mapping.ServingEpoch
		m.mu.Unlock()
		log.Warn().
			Str("file_id", string(ack.FileID)).
			Str("sender", string(senderIdentity)).
			Uint64("received_epoch", ack.Epoch).
			Uint64("current_epoch", currentEpoch).
			Msg("file_transfer: file_downloaded_ack epoch mismatch — dropping stale ack")
		return
	}

	mapping.State = receiverCompleted
	mapping.CompletedAt = time.Now()
	m.saveMappingsLocked()
	m.mu.Unlock()

	log.Info().
		Str("file_id", string(ack.FileID)).
		Str("sender", string(senderIdentity)).
		Msg("file_transfer: transfer completed (receiver)")
}

// CancelDownload aborts an active download and resets the receiver mapping
// to available state. The partial file is deleted from disk. The user can
// re-initiate the download later by clicking the download button again.
func (m *Manager) CancelDownload(fileID domain.FileID) error {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[fileID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no receiver mapping for %s", fileID)
	}

	// Only cancel in-progress or paused downloads. Completed, failed, and
	// waitingAck are terminal-like: in waitingAck the receiver has already
	// sent file_downloaded and the sender may have transitioned to
	// senderCompleted (releasing the transmit file). Resetting to available
	// would advertise a re-download that the protocol cannot fulfill.
	switch mapping.State {
	case receiverDownloading, receiverVerifying, receiverWaitingRoute:
		// Allowed — proceed with cancellation.
	default:
		m.mu.Unlock()
		return fmt.Errorf("cannot cancel download in state %s", mapping.State)
	}

	mapping.State = receiverAvailable
	mapping.BytesReceived = 0
	mapping.NextOffset = 0
	mapping.ChunkRetries = 0
	mapping.DownloadedRetries = 0
	mapping.DownloadedBackoff = 0
	mapping.DownloadedSentAt = time.Time{}
	// Forget the stashed serving epoch: the next download is a new run
	// and must learn the sender's current epoch from its next
	// chunk_response. Leaving the previous epoch here would cause the
	// receiver to echo it on completion, which the sender would reject
	// as stale (its ServingEpoch is now strictly larger).
	mapping.ServingEpoch = 0
	m.nextGeneration++
	mapping.Generation = m.nextGeneration
	mapping.CompletedPath = ""
	m.saveMappingsLocked()
	m.mu.Unlock()

	// Clean up partial file outside the lock (I/O operation).
	partialPath := partialDownloadPath(m.downloadDir, fileID)
	if err := os.Remove(partialPath); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("path", partialPath).
			Msg("file_transfer: remove partial file on cancel failed")
	}

	log.Info().Str("file_id", string(fileID)).Msg("file_transfer: download cancelled")
	return nil
}

// RestartDownload resets a failed receiver mapping back to available state
// so the user can re-initiate the download. The partial file was already
// deleted when the mapping transitioned to failed, so no I/O cleanup is
// needed. After restart, the user triggers a fresh download via
// StartDownload (the download button in UI).
func (m *Manager) RestartDownload(fileID domain.FileID) error {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[fileID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no receiver mapping for %s", fileID)
	}

	if mapping.State != receiverFailed {
		m.mu.Unlock()
		return fmt.Errorf("cannot restart download in state %s (only failed)", mapping.State)
	}

	mapping.State = receiverAvailable
	mapping.BytesReceived = 0
	mapping.NextOffset = 0
	mapping.ChunkRetries = 0
	mapping.DownloadedRetries = 0
	mapping.DownloadedBackoff = 0
	mapping.DownloadedSentAt = time.Time{}
	// Forget the stashed serving epoch: the next download is a new run
	// and must learn the sender's current epoch from its next
	// chunk_response. Leaving the previous epoch here would cause the
	// receiver to echo it on completion, which the sender would reject
	// as stale (its ServingEpoch is now strictly larger).
	mapping.ServingEpoch = 0
	m.nextGeneration++
	mapping.Generation = m.nextGeneration
	mapping.CompletedPath = ""
	m.saveMappingsLocked()
	m.mu.Unlock()

	log.Info().Str("file_id", string(fileID)).Msg("file_transfer: failed download restarted")
	return nil
}

// ForceRetryChunk forces an immediate retry of the pending chunk request
// for the given file ID. Accepts transfers in receiverDownloading or
// receiverWaitingRoute state. When the transfer is paused in waiting_route
// and the sender is reachable again, the mapping is transitioned back to
// downloading and the next chunk is requested — mirroring the auto-resume
// logic in tickReceiverMappings. Returns an error if the sender is still
// unreachable, the download slot limit is reached, or the transfer is in
// an incompatible state.
func (m *Manager) ForceRetryChunk(fileID domain.FileID) error {
	m.mu.Lock()
	rm, ok := m.receiverMaps[fileID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("file %s: not found", fileID)
	}

	switch rm.State {
	case receiverDownloading:
		// Already active — proceed to reachability check below.
	case receiverWaitingRoute:
		// Paused after route loss. Check download slot availability before
		// attempting to resume, consistent with tickReceiverMappings.
		if m.activeDownloadCountLocked() >= maxConcurrentDownloads {
			m.mu.Unlock()
			return fmt.Errorf("file %s: concurrent download limit reached (%d)", fileID, maxConcurrentDownloads)
		}
	default:
		m.mu.Unlock()
		return fmt.Errorf("file %s: cannot retry in state %s", fileID, rm.State)
	}

	sender := rm.Sender

	if m.peerReachable != nil && !m.peerReachable(sender) {
		if rm.State != receiverWaitingRoute {
			rm.State = receiverWaitingRoute
			m.saveMappingsLocked()
		}
		m.mu.Unlock()
		return fmt.Errorf("file %s: sender %s is not reachable", fileID, sender)
	}

	if rm.State == receiverDownloading {
		// Already downloading — just re-send the chunk request. No
		// partial file check needed: the file is actively being written.
		offset := rm.NextOffset
		chunkSize := rm.ChunkSize
		rm.LastChunkAt = time.Now()
		rm.ChunkRetries = 0
		m.saveMappingsLocked()
		m.mu.Unlock()

		if err := m.requestNextChunk(fileID, sender, offset, chunkSize); err != nil {
			return fmt.Errorf("file %s: chunk request failed: %w", fileID, err)
		}
		return nil
	}

	// waitingRoute → downloading with partial file validation and rollback.
	snap := m.prepareResumeLocked(fileID, rm)
	m.saveMappingsLocked()
	m.mu.Unlock()

	m.truncatePartialFile(snap)

	if err := m.sendChunkWithRollback(snap); err != nil {
		return fmt.Errorf("file %s: chunk request failed: %w", fileID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Receiver internal helpers
// ---------------------------------------------------------------------------

// resumeSnapshot holds the pre-transition state captured by
// prepareResumeLocked so that sendChunkWithRollback can restore it
// if the initial chunk_request fails.
type resumeSnapshot struct {
	fileID            domain.FileID
	sender            domain.PeerIdentity
	prevState         receiverState
	prevOffset        uint64
	prevBytesReceived uint64
	startOffset       uint64
	chunkSize         uint32
	truncatePartial   bool   // true when restarting from offset 0 with a stale .part on disk
	generation        uint64 // mapping generation at snapshot time; rollback is skipped if it changed
}

// prepareResumeLocked validates the partial file, resets the offset when
// the file is missing or truncated, captures the pre-transition state, and
// transitions the mapping to receiverDownloading.
//
// Must be called with m.mu held. Does NOT release the lock — the caller is
// responsible for calling saveMappingsLocked and m.mu.Unlock after one or
// more prepareResumeLocked calls.
func (m *Manager) prepareResumeLocked(
	fileID domain.FileID,
	rm *receiverFileMapping,
) resumeSnapshot {
	// Validate the partial file before trusting the persisted offset.
	// If the .part file is missing or shorter than NextOffset, reset to
	// offset 0 so we don't download from a hole that only fails at hash
	// verification.
	resumeOffset := rm.NextOffset
	if resumeOffset > 0 {
		partialPath := partialDownloadPath(m.downloadDir, fileID)
		info, err := os.Stat(partialPath)
		if err != nil || uint64(info.Size()) < resumeOffset {
			log.Warn().
				Str("file_id", string(fileID)).
				Uint64("expected_offset", resumeOffset).
				Err(err).
				Msg("file_transfer: partial file missing or truncated, restarting from offset 0")
			resumeOffset = 0
		} else if uint64(info.Size()) > rm.FileSize {
			// Partial file larger than announced FileSize — corrupted or
			// tampered. Restart from scratch to avoid requesting chunks at
			// an impossible offset.
			log.Warn().
				Str("file_id", string(fileID)).
				Uint64("partial_size", uint64(info.Size())).
				Uint64("file_size", rm.FileSize).
				Msg("file_transfer: partial file exceeds file size, restarting from offset 0")
			resumeOffset = 0
		}
	}

	// Reset offset/bytes BEFORE capturing the snapshot so that rollback
	// preserves the corrected values (not the stale pre-check values).
	// When starting from offset 0, the caller must truncate any existing
	// .part file — writeChunkToFile uses WriteAt which only overwrites the
	// prefix, leaving stale trailing bytes from a previous attempt intact.
	needTruncate := false
	if resumeOffset == 0 {
		rm.NextOffset = 0
		rm.BytesReceived = 0
		needTruncate = true
	}

	snap := resumeSnapshot{
		fileID:            fileID,
		sender:            rm.Sender,
		prevState:         rm.State,
		prevOffset:        rm.NextOffset,
		prevBytesReceived: rm.BytesReceived,
		startOffset:       resumeOffset,
		chunkSize:         rm.ChunkSize,
		truncatePartial:   needTruncate,
		generation:        rm.Generation,
	}

	rm.State = receiverDownloading
	rm.LastChunkAt = time.Now()
	rm.ChunkRetries = 0

	return snap
}

// truncatePartialFile removes a stale .part file when restarting a download
// from offset 0. writeChunkToFile uses WriteAt which only overwrites the
// prefix of an existing file — any trailing bytes from a previous larger
// attempt would remain, causing hash verification to fail. Must be called
// WITHOUT m.mu held (performs file I/O).
func (m *Manager) truncatePartialFile(snap resumeSnapshot) {
	if !snap.truncatePartial {
		return
	}
	path := partialDownloadPath(m.downloadDir, snap.fileID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("file_id", string(snap.fileID)).
			Msg("file_transfer: remove stale partial file on restart failed")
	}
}

// sendChunkWithRollback sends the initial chunk_request for a resume and
// rolls back the mapping to its previous state on failure. Must be called
// WITHOUT m.mu held.
//
// The rollback is guarded by both state AND generation: if CancelDownload
// resets the mapping (bumping generation) and a new download reaches
// receiverDownloading before the stale rollback runs, the generation
// mismatch prevents the old snapshot from corrupting the new transfer.
func (m *Manager) sendChunkWithRollback(snap resumeSnapshot) error {
	err := m.requestNextChunk(snap.fileID, snap.sender, snap.startOffset, snap.chunkSize)
	if err == nil {
		return nil
	}

	m.mu.Lock()
	if rm, exists := m.receiverMaps[snap.fileID]; exists &&
		rm.State == receiverDownloading &&
		rm.Generation == snap.generation {
		rm.State = snap.prevState
		rm.NextOffset = snap.prevOffset
		rm.BytesReceived = snap.prevBytesReceived
		m.saveMappingsLocked()
	}
	m.mu.Unlock()

	return err
}

// chunkReceivePrep holds validated, immutable parameters captured under lock
// for a single chunk_response processing cycle. Analogous to chunkServePrep
// on the sender side.
type chunkReceivePrep struct {
	chunkData   []byte
	partialPath string
	fileHash    string
	fileSize    uint64
	chunkSize   uint32
	sender      domain.PeerIdentity
	offset      uint64
}

// validateChunkResponseLocked performs all pre-I/O validation for an incoming
// chunk_response under the mutex: sender identity, state, decode, size
// bounds, empty-chunk guard, and offset match. On success it copies the
// needed fields into a chunkReceivePrep and unlocks. On failure it unlocks,
// logs, and returns a non-nil error.
//
// Must be called with m.mu held. Always releases m.mu before returning.
func (m *Manager) validateChunkResponseLocked(
	senderIdentity domain.PeerIdentity,
	resp domain.ChunkResponsePayload,
) (chunkReceivePrep, error) {
	mapping, ok := m.receiverMaps[resp.FileID]
	if !ok {
		m.mu.Unlock()
		return chunkReceivePrep{}, fmt.Errorf("unknown file")
	}

	if senderIdentity != mapping.Sender {
		m.mu.Unlock()
		return chunkReceivePrep{}, fmt.Errorf("wrong sender")
	}

	if mapping.State != receiverDownloading {
		m.mu.Unlock()
		return chunkReceivePrep{}, fmt.Errorf("not downloading")
	}

	chunkData, err := base64.RawURLEncoding.DecodeString(resp.Data)
	if err != nil {
		m.mu.Unlock()
		log.Error().Err(err).Str("file_id", string(resp.FileID)).Msg("file_transfer: decode chunk data failed")
		return chunkReceivePrep{}, fmt.Errorf("decode: %w", err)
	}

	if uint32(len(chunkData)) > mapping.ChunkSize {
		m.mu.Unlock()
		log.Warn().
			Str("file_id", string(resp.FileID)).
			Int("chunk_bytes", len(chunkData)).
			Uint32("max_chunk_size", mapping.ChunkSize).
			Msg("file_transfer: chunk_response exceeds requested size, dropping")
		return chunkReceivePrep{}, fmt.Errorf("oversized chunk")
	}

	// Undersize guard: a non-final chunk must deliver exactly ChunkSize
	// bytes. A truncated response would shift all subsequent offsets and
	// guarantee a hash mismatch at verification — reject early instead of
	// wasting bandwidth on a doomed transfer. The last chunk is exempt
	// because the sender clamps it to the remaining bytes.
	endOffset := mapping.NextOffset + uint64(len(chunkData))
	if uint32(len(chunkData)) < mapping.ChunkSize && endOffset < mapping.FileSize {
		m.mu.Unlock()
		log.Warn().
			Str("file_id", string(resp.FileID)).
			Int("chunk_bytes", len(chunkData)).
			Uint32("expected_chunk_size", mapping.ChunkSize).
			Uint64("end_offset", endOffset).
			Uint64("file_size", mapping.FileSize).
			Msg("file_transfer: non-final chunk_response is undersized, dropping")
		return chunkReceivePrep{}, fmt.Errorf("undersized chunk")
	}

	if len(chunkData) == 0 && mapping.BytesReceived < mapping.FileSize {
		m.mu.Unlock()
		log.Warn().
			Str("file_id", string(resp.FileID)).
			Uint64("offset", resp.Offset).
			Msg("file_transfer: empty chunk_response before transfer complete, dropping")
		return chunkReceivePrep{}, fmt.Errorf("empty chunk")
	}

	if resp.Offset != mapping.NextOffset {
		m.mu.Unlock()
		log.Warn().
			Str("file_id", string(resp.FileID)).
			Uint64("resp_offset", resp.Offset).
			Uint64("expected_offset", mapping.NextOffset).
			Msg("file_transfer: stale/duplicate chunk_response, ignoring")
		return chunkReceivePrep{}, fmt.Errorf("offset mismatch")
	}

	// Stash the sender's serving epoch so file_downloaded can echo it for
	// replay defense. The cache must be strictly monotonic: a delayed
	// chunk_response from an older serving run (lower epoch) must not
	// overwrite a newer epoch already observed. Without this, the
	// completion would echo the stale epoch and the sender would reject
	// the legitimate file_downloaded as a replay.
	//
	// Zero is skipped entirely: a legacy sender (epoch=0) or a mid-upgrade
	// sender regressing to zero must not erase a non-zero epoch that the
	// receiver already learned from an earlier chunk in this run.
	if resp.Epoch > mapping.ServingEpoch {
		mapping.ServingEpoch = resp.Epoch
	}

	log.Info().
		Str("file_id", string(resp.FileID)).
		Uint64("offset", resp.Offset).
		Int("chunk_bytes", len(chunkData)).
		Uint64("bytes_received_so_far", mapping.BytesReceived+uint64(len(chunkData))).
		Uint64("file_size", mapping.FileSize).
		Uint64("serving_epoch", mapping.ServingEpoch).
		Msg("file_transfer: chunk_response received")

	prep := chunkReceivePrep{
		chunkData:   chunkData,
		partialPath: partialDownloadPath(m.downloadDir, resp.FileID),
		fileHash:    mapping.FileHash,
		fileSize:    mapping.FileSize,
		chunkSize:   mapping.ChunkSize,
		sender:      mapping.Sender,
		offset:      resp.Offset,
	}

	m.mu.Unlock()
	return prep, nil
}

// commitChunkProgressLocked re-validates the mapping after a disk write
// (cancel or restart could have occurred during the unlock window), advances
// BytesReceived/NextOffset, and persists. Returns the new NextOffset and
// true on success, or 0 and false if the mapping was concurrently modified.
//
// Must be called with m.mu held. Does NOT release the lock.
func (m *Manager) commitChunkProgressLocked(
	fileID domain.FileID,
	offset uint64,
	bytesWritten int,
) (uint64, bool) {
	mapping, ok := m.receiverMaps[fileID]
	if !ok || mapping.State != receiverDownloading {
		return 0, false
	}
	// A concurrent cancel+restart could have reset NextOffset to 0.
	if offset != mapping.NextOffset {
		return 0, false
	}

	mapping.BytesReceived += uint64(bytesWritten)
	mapping.NextOffset = offset + uint64(bytesWritten)
	mapping.LastChunkAt = time.Now()
	mapping.ChunkRetries = 0
	m.saveMappingsLocked()
	return mapping.NextOffset, true
}

// receiverStateIs briefly acquires the mutex and checks whether the receiver
// mapping for fileID exists, is in the expected state, and belongs to the
// expected generation. Used by the deferred action loop in
// tickReceiverMappings to skip actions whose mapping was cancelled, removed,
// or recycled between the snapshot (under lock) and execution (outside lock).
// The generation check prevents a stale deferred action from affecting a
// mapping that was deleted and re-created for the same fileID.
func (m *Manager) receiverStateIs(fileID domain.FileID, expected receiverState, generation uint64) bool {
	m.mu.Lock()
	rm, ok := m.receiverMaps[fileID]
	result := ok && rm.State == expected && rm.Generation == generation
	m.mu.Unlock()
	return result
}

// markReceiverFailed transitions a receiver mapping to the failed state and
// persists the change, but only if the mapping is still in the expected state
// AND belongs to the expected generation. The generation check prevents a
// stale verifier goroutine from corrupting a newer download attempt for the
// same fileID: if CancelDownload resets the mapping (bumping generation) and
// a new download reaches receiverVerifying, the old goroutine must not
// transition the new attempt to receiverFailed.
// Must be called with m.mu NOT held.
func (m *Manager) markReceiverFailed(mapping *receiverFileMapping, expectedState receiverState, generation uint64) {
	m.mu.Lock()
	if mapping.State != expectedState || mapping.Generation != generation {
		m.mu.Unlock()
		return
	}
	mapping.State = receiverFailed
	mapping.CompletedAt = time.Now()
	m.saveMappingsLocked()
	m.mu.Unlock()
}

// activeReceiverCountLocked returns the number of receiver mappings that
// occupy a live quota slot. Terminal states (completed, failed) are kept
// for dedup/history until tombstoneTTL but must not block new announces.
//
// Caller must hold m.mu.
func (m *Manager) activeReceiverCountLocked() int {
	n := 0
	for _, rm := range m.receiverMaps {
		if rm.State != receiverCompleted && rm.State != receiverFailed {
			n++
		}
	}
	return n
}

// activeDownloadCountLocked returns the number of receiver mappings in the
// downloading state. Must be called with m.mu held.
func (m *Manager) activeDownloadCountLocked() int {
	count := 0
	for _, rm := range m.receiverMaps {
		if rm.State == receiverDownloading {
			count++
		}
	}
	return count
}

// backfillCompletedPath resolves the on-disk location of a completed download
// when CompletedPath is empty (legacy entries persisted before the field was
// added). Only probes when the mapping is in a completed or waiting_ack state
// and has a non-empty file name. Returns the resolved path or empty string.
func (m *Manager) backfillCompletedPath(rm *receiverFileMapping) string {
	if rm.CompletedPath != "" {
		return rm.CompletedPath
	}
	if rm.FileName == "" {
		return ""
	}
	if rm.State != receiverCompleted && rm.State != receiverWaitingAck {
		return ""
	}
	return resolveExistingDownload(m.downloadDir, rm.FileName, rm.FileHash)
}

// requestNextChunk sends a chunk_request to the sender.
func (m *Manager) requestNextChunk(
	fileID domain.FileID,
	sender domain.PeerIdentity,
	offset uint64,
	size uint32,
) error {
	reqData, err := json.Marshal(domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: offset,
		Size:   size,
	})
	if err != nil {
		return fmt.Errorf("marshal chunk request: %w", err)
	}

	payload := domain.FileCommandPayload{
		Command: domain.FileActionChunkReq,
		Data:    reqData,
	}

	return m.sendCommand(sender, payload)
}

// onDownloadComplete handles the transition from downloading → verifying → waiting_ack.
func (m *Manager) onDownloadComplete(
	fileID domain.FileID,
	partialPath, expectedHash string,
	sender domain.PeerIdentity,
) {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[fileID]
	if !ok {
		m.mu.Unlock()
		return
	}
	// Only transition from downloading → verifying. If the mapping has
	// been canceled (reset to available) or failed between the caller
	// dropping the lock and this point, do not overwrite the new state.
	if mapping.State != receiverDownloading {
		m.mu.Unlock()
		return
	}
	mapping.State = receiverVerifying
	generation := mapping.Generation
	m.saveMappingsLocked()
	m.mu.Unlock()

	// failVerification marks the mapping as failed (if this verifier still
	// owns it by generation), removes the .part file, and logs the reason.
	// Five verification steps share this cleanup path — the helper avoids
	// repeating markReceiverFailed + removePartial + return in each branch.
	failVerification := func(err error, msg string) {
		m.markReceiverFailed(mapping, receiverVerifying, generation)
		log.Error().Err(err).Str("file_id", string(fileID)).Msg(msg)
		// Delete .part only if this verifier still owns the mapping.
		if m.receiverStateIs(fileID, receiverFailed, generation) {
			_ = os.Remove(partialPath)
		}
	}

	// Verify the partial file is not a symlink and matches the expected
	// SHA-256 hash using a single file descriptor. This eliminates the
	// TOCTOU window that existed when checking the path with Lstat and
	// then reopening it for hashing — a local attacker could have swapped
	// the file between those two operations.
	//
	// verifyPartialIntegrity returns the Fstat identity of the verified
	// fd so we can detect post-verification swaps before os.Rename.
	verifiedInfo, err := verifyPartialIntegrity(partialPath, expectedHash)
	if err != nil {
		failVerification(err, "file_transfer: partial file integrity check failed")
		return
	}

	// Move to downloads directory with the original file name.
	// Both MkdirAll and Rename must succeed before acknowledging the
	// transfer — if the verified file cannot be persisted, the sender
	// must not release its transmit copy.
	completedPath := completedDownloadPath(m.downloadDir, mapping.FileName, mapping.FileHash)

	// Verify the completed path stays within the downloads directory.
	if err := ensureWithinDir(m.downloadDir, completedPath); err != nil {
		failVerification(err, "file_transfer: completed path escapes download directory")
		return
	}

	if err := os.MkdirAll(filepath.Dir(completedPath), 0o700); err != nil {
		failVerification(err, "file_transfer: create download dir failed")
		return
	}

	// Re-verify that the file at partialPath is the same inode we just
	// hashed. This closes the TOCTOU window between verifyPartialIntegrity
	// (which closed its fd) and os.Rename: a local attacker who swaps the
	// verified file for a symlink or different file in that gap will be
	// detected by the inode mismatch.
	if err := verifyFileIdentity(partialPath, verifiedInfo); err != nil {
		failVerification(err, "file_transfer: partial file identity changed before rename")
		return
	}

	if err := os.Rename(partialPath, completedPath); err != nil {
		failVerification(err, "file_transfer: move completed file failed")
		return
	}

	// Finalize the verified download: guard on state+generation, then
	// transition to waitingAck and send file_downloaded. verifiedInfo is
	// the pre-rename Fstat identity — since os.Rename preserves the inode,
	// it also identifies the file now at completedPath and is used by the
	// stale-cleanup branch to avoid deleting a different attempt's file
	// that may have atomically overwritten completedPath.
	if !m.finalizeVerifiedDownload(fileID, mapping, generation, completedPath, verifiedInfo, sender) {
		return
	}

	log.Info().
		Str("file_id", string(fileID)).
		Str("path", completedPath).
		Msg("file_transfer: file verified and stored")
}

// finalizeVerifiedDownload is the post-rename tail of onDownloadComplete.
// It checks that the mapping is still owned by this verifier (state AND
// generation), transitions to receiverWaitingAck, and sends file_downloaded.
// Returns true if the transition and send were attempted, false if the
// verifier is stale (cancelled or superseded by a restart of the same
// fileID) and the completed file was cleaned up.
//
// Generation guard rationale: a concurrent CancelDownload resets the
// mapping to available and bumps Generation; the user may then restart
// the same fileID, and a new attempt can advance back to receiverVerifying.
// Without this guard a stale verifier goroutine would see the matching
// receiverVerifying state, overwrite CompletedPath with its old blob's
// path, transition the NEW attempt to waitingAck, and send file_downloaded
// for a file the user explicitly abandoned. The verify-failure path uses
// the same combined state+generation check via markReceiverFailed.
//
// File-identity cleanup rationale: cancel+restart of the same fileID
// resolves to the same completedPath (identical FileName+FileHash). If
// the stale verifier reaches the mismatch branch after a NEW attempt has
// already renamed its own verified file into place, a path-only unlink
// would delete the new attempt's legitimate file. verifiedInfo captures
// the inode identity of the file this verifier renamed; removeOwnedFile
// uses os.SameFile to only unlink when completedPath still points at our
// inode. If another attempt's atomic rename replaced our inode, we skip.
func (m *Manager) finalizeVerifiedDownload(
	fileID domain.FileID,
	mapping *receiverFileMapping,
	generation uint64,
	completedPath string,
	verifiedInfo os.FileInfo,
	sender domain.PeerIdentity,
) bool {
	m.mu.Lock()
	if mapping.State != receiverVerifying || mapping.Generation != generation {
		currentState := mapping.State
		currentGeneration := mapping.Generation
		m.mu.Unlock()
		log.Info().
			Str("file_id", string(fileID)).
			Str("state", string(currentState)).
			Uint64("verifier_generation", generation).
			Uint64("current_generation", currentGeneration).
			Msg("file_transfer: verification completed but transfer was cancelled or restarted, aborting")
		// The completed file was already renamed to completedPath. Clean it
		// up only if our inode is still there: a newer attempt may have
		// atomically overwritten completedPath with its own legitimately
		// verified file, in which case we must not delete it.
		m.removeOwnedFileInDownloadDir(completedPath, verifiedInfo, "post-verify cancel cleanup")
		return false
	}
	mapping.State = receiverWaitingAck
	mapping.CompletedPath = completedPath
	mapping.DownloadedSentAt = time.Now()
	mapping.DownloadedBackoff = initialRetryTimeout

	if err := m.saveMappingsLockedErr(); err != nil {
		// Persist failed but the completed file is already durably stored at
		// completedPath. Rolling back to receiverVerifying would strand the
		// transfer for the entire session: tickReceiverMappings has no case
		// for receiverVerifying, and the .part file is gone after rename.
		//
		// Instead, keep the in-memory state at receiverWaitingAck and proceed
		// with sending file_downloaded. The mapping is live in memory and will
		// drive the ack retry cycle normally. If the process crashes before a
		// successful persist, reconcileVerifyingOnStartup Case 1 will find
		// the completed file and reconstruct waitingAck from disk.
		//
		// This mirrors PrepareAndSend where Commit failure after a sent DM
		// is non-fatal — the in-memory mapping serves the current session.
		log.Error().Err(err).
			Str("file_id", string(fileID)).
			Str("completed_path", completedPath).
			Msg("file_transfer: persist waiting_ack failed after rename — proceeding in memory, will reconcile on restart")
	}
	m.mu.Unlock()

	m.sendFileDownloaded(fileID, sender)
	return true
}

// sendFileDownloaded sends a file_downloaded command to the sender. It
// attaches the ServingEpoch stashed on the receiver mapping from the most
// recent chunk_response; the sender uses this to reject stale replays from
// a prior serving run of the same FileID.
//
// The function verifies that the mapping is still in receiverWaitingAck
// and marshals the wire payload in a single lock acquisition. This
// guarantees that if CancelDownload (or any other state mutation) runs
// concurrently, it either completes before the lock — causing the guard
// to fail — or blocks until after the payload is committed. A generation
// check is not needed because CancelDownload rejects receiverWaitingAck
// (only downloading/verifying/waitingRoute are cancellable), so the state
// alone is a sufficient guard.
//
// If no mapping exists for fileID (cleanup race), the command is not sent.
// Epoch=0 is legal and means the receiver never observed a non-zero epoch
// — the sender treats it as legacy.
func (m *Manager) sendFileDownloaded(fileID domain.FileID, sender domain.PeerIdentity) {
	m.mu.Lock()
	mapping, ok := m.receiverMaps[fileID]
	if !ok || mapping.State != receiverWaitingAck {
		m.mu.Unlock()
		return
	}
	epoch := mapping.ServingEpoch
	gen := mapping.Generation

	// Marshal under lock so the decision and the wire bytes are committed
	// atomically — no TOCTOU gap for concurrent state mutations.
	dlData, err := json.Marshal(domain.FileDownloadedPayload{
		FileID: fileID,
		Epoch:  epoch,
	})
	if err != nil {
		m.mu.Unlock()
		log.Error().Err(err).Msg("file_transfer: marshal file_downloaded failed")
		return
	}
	payload := domain.FileCommandPayload{
		Command: domain.FileActionDownloaded,
		Data:    dlData,
	}
	m.mu.Unlock()

	if err := m.sendCommand(sender, payload); err != nil {
		log.Debug().Err(err).
			Str("file_id", string(fileID)).
			Uint64("serving_epoch", epoch).
			Uint64("generation", gen).
			Msg("file_transfer: send file_downloaded failed")
	}
}

// writeChunkToFile writes chunk data at the specified offset in the partial file.
// Validates that the resulting file size stays within maxPartialDownloadStorage
// to prevent denial-of-service via sparse file allocation.
//
// Symlink defense (two layers):
//
//  1. Kernel no-follow where available: openNoFollow uses O_NOFOLLOW on
//     platforms that support it, making the kernel reject the open with ELOOP
//     if the final path component is a symlink. This prevents the file at the
//     symlink target from being created or opened in the first place — closing
//     the window where O_CREATE would follow the symlink before any user-space
//     check could run.
//
//  2. verifyNotSymlink (defense in depth): after the open, Lstat on the path
//     is compared with Fstat on the fd. This catches the TOCTOU race where an
//     attacker replaces a regular file with a symlink between open and write.
func writeChunkToFile(path string, offset uint64, data []byte, expectedFileSize uint64) error {
	dataLen := uint64(len(data))

	// Guard against uint64 overflow: if offset + dataLen wraps around,
	// the sum will be smaller than either operand.
	if dataLen > 0 && offset > ^uint64(0)-dataLen {
		return fmt.Errorf("chunk offset+length overflows uint64: offset=%d len=%d", offset, dataLen)
	}

	endOffset := offset + dataLen
	if endOffset > expectedFileSize {
		return fmt.Errorf("chunk would exceed expected file size: offset=%d len=%d expected=%d", offset, len(data), expectedFileSize)
	}
	if endOffset > maxPartialDownloadStorage {
		return fmt.Errorf("chunk would exceed storage limit: %d > %d", endOffset, maxPartialDownloadStorage)
	}

	f, err := openNoFollow(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Defense in depth: even though openNoFollow already rejected symlinks
	// at the kernel level, verify inode identity to catch a TOCTOU swap
	// that occurred between open and this point (e.g. an attacker replaces
	// the regular file with a symlink after the open succeeded).
	if err := verifyNotSymlink(path, f); err != nil {
		return fmt.Errorf("partial file symlink check: %w", err)
	}

	if _, err := f.WriteAt(data, int64(offset)); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Receiver tick — periodic maintenance
// ---------------------------------------------------------------------------

// receiverTickAction describes a deferred side-effect collected during the
// locked scan phase of tickReceiverMappings. Actions that require I/O (network
// sends, file removal) are executed after the lock is released.
//
// requiredState is the receiver state that must still hold when the action
// executes. Between the snapshot (under lock) and execution (outside lock),
// CancelDownload or CleanupPeerTransfers may have changed or removed the
// mapping — the dispatch loop skips actions whose state no longer matches.
type receiverTickAction struct {
	kind          receiverTickActionKind
	requiredState receiverState
	generation    uint64 // mapping generation at snapshot time; stale actions are skipped
	fileID        domain.FileID
	sender        domain.PeerIdentity
	offset        uint64
	chunkSize     uint32
	snap          resumeSnapshot // only for actionResume
}

type receiverTickActionKind int

const (
	actionRetryChunk          receiverTickActionKind = iota // re-send chunk_request
	actionCleanupFailed                                     // delete partial file after max retries
	actionRetryFileDownloaded                               // re-send file_downloaded
	actionResume                                            // resume downloading from waitingRoute
)

// tickReceiverMappings performs all periodic maintenance on receiver mappings
// in a single pass: stall detection for active downloads, ack retries with
// backoff, auto-resume for waiting_route transfers, and cleanup of completed
// entries. The single-pass design ensures that every state is handled exactly
// once and new per-state rules cannot be silently missed.
func (m *Manager) tickReceiverMappings() {
	now := time.Now()

	m.mu.Lock()

	var actions []receiverTickAction
	changed := false
	activeDownloads := m.activeDownloadCountLocked()

	for id, rm := range m.receiverMaps {
		switch rm.State {

		// --- Active download: stall detection ---
		case receiverDownloading:
			if now.Sub(rm.LastChunkAt) < chunkRequestStallTimeout {
				continue
			}

			// Sender unreachable → park in waitingRoute (frees the download
			// slot). The transfer auto-resumes when routes recover.
			if m.peerReachable != nil && !m.peerReachable(rm.Sender) {
				rm.State = receiverWaitingRoute
				changed = true
				log.Info().
					Str("file_id", string(rm.FileID)).
					Str("sender", string(rm.Sender)).
					Msg("file_transfer: sender unreachable, pausing download")
				continue
			}

			rm.ChunkRetries++
			rm.LastChunkAt = now

			if rm.ChunkRetries > maxChunkRequestRetries {
				rm.State = receiverFailed
				rm.CompletedAt = now
				changed = true
				actions = append(actions, receiverTickAction{
					kind:          actionCleanupFailed,
					requiredState: receiverFailed,
					generation:    rm.Generation,
					fileID:        rm.FileID,
				})
				continue
			}

			changed = true
			actions = append(actions, receiverTickAction{
				kind:          actionRetryChunk,
				requiredState: receiverDownloading,
				generation:    rm.Generation,
				fileID:        rm.FileID,
				sender:        rm.Sender,
				offset:        rm.NextOffset,
				chunkSize:     rm.ChunkSize,
			})

		// --- Ack retry: advance counter unconditionally, send only when reachable ---
		case receiverWaitingAck:
			if now.Sub(rm.DownloadedSentAt) < rm.DownloadedBackoff {
				continue
			}

			// Counter and backoff advance regardless of sender reachability.
			// Without this, an offline sender would prevent the retry budget
			// from draining, leaving the transfer stuck forever.
			rm.DownloadedSentAt = now
			rm.DownloadedRetries++
			rm.DownloadedBackoff *= time.Duration(retryBackoffMultiplier)
			if rm.DownloadedBackoff > maxRetryTimeout {
				rm.DownloadedBackoff = maxRetryTimeout
			}
			changed = true

			if rm.DownloadedRetries > 20 {
				rm.State = receiverCompleted
				rm.CompletedAt = now
				log.Info().
					Str("file_id", string(rm.FileID)).
					Str("sender", string(rm.Sender)).
					Msg("file_transfer: waiting_ack retries exhausted, completing locally")
				continue
			}

			// Skip the actual send when the sender is offline — the counter
			// still advanced above so the budget drains on schedule.
			if m.peerReachable != nil && !m.peerReachable(rm.Sender) {
				continue
			}

			actions = append(actions, receiverTickAction{
				kind:          actionRetryFileDownloaded,
				requiredState: receiverWaitingAck,
				generation:    rm.Generation,
				fileID:        rm.FileID,
				sender:        rm.Sender,
			})

		// --- Auto-resume: sender reappeared + download slot available ---
		case receiverWaitingRoute:
			if activeDownloads >= maxConcurrentDownloads {
				continue
			}
			if m.peerReachable != nil && !m.peerReachable(rm.Sender) {
				continue
			}

			snap := m.prepareResumeLocked(rm.FileID, rm)
			changed = true
			activeDownloads++
			actions = append(actions, receiverTickAction{
				kind:          actionResume,
				requiredState: receiverDownloading,
				generation:    rm.Generation,
				fileID:        rm.FileID,
				sender:        rm.Sender,
				snap:          snap,
			})

			log.Info().
				Str("file_id", string(rm.FileID)).
				Str("sender", string(rm.Sender)).
				Uint64("offset", snap.startOffset).
				Msg("file_transfer: sender reachable again, resuming download")

		// --- TTL cleanup for terminal receiver mappings ---
		case receiverCompleted, receiverFailed:
			if now.Sub(rm.CompletedAt) > tombstoneTTL {
				delete(m.receiverMaps, id)
				changed = true
			}
		}
	}

	if changed {
		m.saveMappingsLocked()
	}
	m.mu.Unlock()

	// Execute deferred I/O actions outside the lock.
	// Each action carries requiredState+generation — the receiver state that
	// must still hold at execution time. Each action kind has its own guard
	// inside the dispatch method so the state check and the I/O commitment
	// are in the same call, minimising the TOCTOU window between check and send.
	for _, a := range actions {
		m.executeReceiverAction(a)
	}
}

// executeReceiverAction dispatches a single deferred receiver tick action.
// Called outside the lock after tickReceiverMappings has released m.mu.
//
// Actions that perform network I/O use guardedChunkRetry / guardedResume /
// sendFileDownloaded — each of which checks state+generation AND marshals
// the wire payload in a single lock acquisition, then sends outside the lock.
// This eliminates the TOCTOU gap that existed when receiverStateIs and the
// send were separate operations: CancelDownload can no longer sneak between
// the decision and the I/O because the payload is committed before the lock
// is released.
//
// actionCleanupFailed only touches a local file and is idempotent, so
// it does not need a state guard.
func (m *Manager) executeReceiverAction(a receiverTickAction) {
	switch a.kind {
	case actionRetryChunk:
		m.guardedChunkRetry(a)

	case actionCleanupFailed:
		log.Warn().Str("file_id", string(a.fileID)).Msg("file_transfer: download failed after max chunk retries")
		partialPath := partialDownloadPath(m.downloadDir, a.fileID)
		_ = os.Remove(partialPath)

	case actionRetryFileDownloaded:
		m.sendFileDownloaded(a.fileID, a.sender)

	case actionResume:
		m.guardedResume(a)
	}
}

// guardedChunkRetry atomically validates receiver state+generation and
// marshals the chunk_request payload in a single lock acquisition. The
// network send happens after the lock is released, but the go/no-go
// decision and the wire bytes are committed while the mapping cannot
// change. This closes the TOCTOU that existed when receiverStateIs and
// requestNextChunk were separate unlocked calls.
func (m *Manager) guardedChunkRetry(a receiverTickAction) {
	m.mu.Lock()
	rm, ok := m.receiverMaps[a.fileID]
	if !ok || rm.State != a.requiredState || rm.Generation != a.generation {
		m.mu.Unlock()
		return
	}

	// Marshal the payload while the lock guarantees state consistency.
	payload, err := buildChunkRequestPayload(a.fileID, a.offset, a.chunkSize)
	m.mu.Unlock()

	if err != nil {
		log.Warn().Err(err).Str("file_id", string(a.fileID)).Msg("file_transfer: marshal stalled chunk retry failed")
		return
	}

	log.Info().
		Str("file_id", string(a.fileID)).
		Uint64("offset", a.offset).
		Msg("file_transfer: retrying stalled chunk request")

	if err := m.sendCommand(a.sender, payload); err != nil {
		log.Warn().Err(err).Str("file_id", string(a.fileID)).Msg("file_transfer: stalled chunk retry failed")
	}
}

// guardedResume atomically validates receiver state+generation and marshals
// the chunk_request payload in a single lock acquisition, then sends outside
// the lock. On send failure, the rollback is also generation-guarded.
//
// truncatePartialFile runs before the guarded send — it is idempotent local
// I/O and safe to execute even if state changed (the file is already stale).
func (m *Manager) guardedResume(a receiverTickAction) {
	// Idempotent local I/O — safe without guard.
	m.truncatePartialFile(a.snap)

	m.mu.Lock()
	rm, ok := m.receiverMaps[a.fileID]
	if !ok || rm.State != a.requiredState || rm.Generation != a.generation {
		m.mu.Unlock()
		return
	}

	payload, err := buildChunkRequestPayload(a.snap.fileID, a.snap.startOffset, a.snap.chunkSize)
	m.mu.Unlock()

	if err != nil {
		log.Warn().Err(err).Str("file_id", string(a.fileID)).Msg("file_transfer: marshal resume chunk request failed")
		return
	}

	if sendErr := m.sendCommand(a.sender, payload); sendErr != nil {
		// Rollback with generation guard — prevents stale rollback from
		// corrupting a newer transfer that reuses the same FileID.
		m.mu.Lock()
		if rm, exists := m.receiverMaps[a.snap.fileID]; exists &&
			rm.State == receiverDownloading &&
			rm.Generation == a.snap.generation {
			rm.State = a.snap.prevState
			rm.NextOffset = a.snap.prevOffset
			rm.BytesReceived = a.snap.prevBytesReceived
			m.saveMappingsLocked()
		}
		m.mu.Unlock()

		log.Warn().Err(sendErr).
			Str("file_id", string(a.fileID)).
			Uint64("offset", a.snap.startOffset).
			Msg("file_transfer: resume chunk request failed, rolled back to waiting_route")
	}
}

// buildChunkRequestPayload marshals a chunk_request FileCommandPayload.
// Pure computation, safe to call under a mutex.
func buildChunkRequestPayload(fileID domain.FileID, offset uint64, size uint32) (domain.FileCommandPayload, error) {
	reqData, err := json.Marshal(domain.ChunkRequestPayload{
		FileID: fileID,
		Offset: offset,
		Size:   size,
	})
	if err != nil {
		return domain.FileCommandPayload{}, fmt.Errorf("marshal chunk request: %w", err)
	}
	return domain.FileCommandPayload{
		Command: domain.FileActionChunkReq,
		Data:    reqData,
	}, nil
}
