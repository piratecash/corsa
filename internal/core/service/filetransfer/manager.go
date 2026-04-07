package filetransfer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// ---------------------------------------------------------------------------
// State machine states
// ---------------------------------------------------------------------------

// senderState tracks a single file being served to a recipient.
type senderState string

const (
	senderAnnounced senderState = "announced" // file_announce sent, awaiting first chunk_request
	senderServing   senderState = "serving"   // actively sending chunks
	senderCompleted senderState = "completed" // file_downloaded received and acked
	senderTombstone senderState = "tombstone" // transfer expired/canceled, kept for dedup
)

// validSenderStates enumerates all known sender states. Used by loadMappings
// to reject persisted entries with corrupted or unknown state strings.
var validSenderStates = map[senderState]struct{}{
	senderAnnounced: {},
	senderServing:   {},
	senderCompleted: {},
	senderTombstone: {},
}

// ---------------------------------------------------------------------------
// File mappings
// ---------------------------------------------------------------------------

// senderFileMapping tracks a file being sent to a specific recipient.
type senderFileMapping struct {
	FileID       domain.FileID
	FileHash     string
	FileName     string
	FileSize     uint64
	ContentType  string
	Recipient    domain.PeerIdentity
	State        senderState
	CreatedAt    time.Time
	CompletedAt  time.Time
	BytesServed  uint64    // cumulative bytes sent via chunk_response
	LastServedAt time.Time // last time a chunk was successfully served
	TransmitPath string    // internal-only, never exposed in RPC/protocol
}

// ---------------------------------------------------------------------------
// Resource limits
// ---------------------------------------------------------------------------

const (
	maxPartialDownloadStorage uint64 = 1 << 30 // 1 GB
	maxConcurrentDownloads    int    = 1
	maxConcurrentServing      int    = 16
	maxFileMappings           int    = 256
	tombstoneTTL                     = 30 * 24 * time.Hour // 30 days
	initialRetryTimeout              = 60 * time.Second
	maxRetryTimeout                  = 600 * time.Second
	retryBackoffMultiplier           = 2
	chunkRequestStallTimeout         = 30 * time.Second
	maxChunkRequestRetries           = 10
	senderServingStallTimeout        = 10 * time.Minute // release abandoned serving slots
)

// ---------------------------------------------------------------------------
// FileTransferManager
// ---------------------------------------------------------------------------

// Manager handles the application-level file transfer logic:
// chunk request/response, integrity verification, resume, and state machine
// transitions. It operates independently of the DM pipeline — file commands
// are never stored in chatlog.
//
// Persistence: sender and receiver mappings are persisted to a JSON file
// (<dataDir>/transfers-<identity_short>-<port>.json) after every state
// transition. On startup, the mappings are loaded from disk and the FileStore
// ref counts are rebuilt from active sender entries.
type Manager struct {
	mu                 sync.Mutex
	senderMaps         map[domain.FileID]*senderFileMapping
	receiverMaps       map[domain.FileID]*receiverFileMapping
	pendingSenderSlots int    // sender slots reserved via PrepareFileAnnounce but not yet committed; protected by mu
	nextGeneration     uint64 // monotonic counter for receiver mapping generations; protected by mu
	store              *FileStore
	downloadDir        string
	mappingsPath       string // path to persisted transfers JSON
	localID            *identity.Identity
	sendCommand        func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error
	peerBoxKey         func(domain.PeerIdentity) (string, bool) // returns base64 box key
	peerReachable      func(domain.PeerIdentity) bool           // true if peer is in route table or has direct session
	stopCh             chan struct{}
}

// Config holds dependencies for Manager.
type Config struct {
	Store         *FileStore
	DownloadDir   string
	MappingsPath  string // path to persisted transfers JSON; empty disables persistence
	LocalID       *identity.Identity
	SendCommand   func(dst domain.PeerIdentity, payload domain.FileCommandPayload) error
	PeerBoxKey    func(domain.PeerIdentity) (string, bool)
	PeerReachable func(domain.PeerIdentity) bool // true if peer has route or direct session
}

// NewFileTransferManager creates a new manager. It loads persisted mappings
// from disk (if the file exists) and rebuilds FileStore ref counts from
// active sender entries. Call Start() to begin background goroutines.
func NewFileTransferManager(cfg Config) *Manager {
	m := &Manager{
		senderMaps:    make(map[domain.FileID]*senderFileMapping),
		receiverMaps:  make(map[domain.FileID]*receiverFileMapping),
		store:         cfg.Store,
		downloadDir:   cfg.DownloadDir,
		mappingsPath:  cfg.MappingsPath,
		localID:       cfg.LocalID,
		sendCommand:   cfg.SendCommand,
		peerBoxKey:    cfg.PeerBoxKey,
		peerReachable: cfg.PeerReachable,
		stopCh:        make(chan struct{}),
	}

	// Restore persisted state and rebuild FileStore ref counts.
	// Blob cleanup is NOT performed at startup — transmit blobs are
	// deleted only when identity or message is removed.
	activeHashes := m.loadMappings()
	if cfg.Store != nil {
		cfg.Store.ValidateOnStartup(activeHashes)
	}

	return m
}

// Start launches background goroutines for periodic maintenance.
func (m *Manager) Start() {
	go m.retryLoop()
}

// Stop terminates background goroutines.
func (m *Manager) Stop() {
	close(m.stopCh)
}

// ---------------------------------------------------------------------------
// Sender-side operations
// ---------------------------------------------------------------------------

// SenderAnnounceToken is an opaque handle returned by PrepareFileAnnounce.
// It holds a reserved sender slot and a transmit-file pending reservation.
// The caller MUST call either Commit (on success) or Rollback (on failure).
// Rollback is idempotent and safe to defer unconditionally — it is a no-op
// after Commit.
type SenderAnnounceToken struct {
	manager     *Manager
	fileHash    string
	fileName    string
	fileSize    uint64
	contentType string
	committed   bool
}

// Commit turns the reservation into a real sender mapping. fileID is the
// MessageID of the DM that was successfully sent; recipient is the peer
// the DM was addressed to.
func (t *SenderAnnounceToken) Commit(fileID domain.FileID, recipient domain.PeerIdentity) error {
	if t.committed {
		return fmt.Errorf("sender announce token already committed")
	}

	t.manager.mu.Lock()
	defer t.manager.mu.Unlock()

	if _, exists := t.manager.senderMaps[fileID]; exists {
		return fmt.Errorf("file mapping already exists for %s", fileID)
	}

	if err := t.manager.store.Acquire(t.fileHash); err != nil {
		return fmt.Errorf("transmit file not available for hash %s: %w", t.fileHash, err)
	}

	t.manager.senderMaps[fileID] = &senderFileMapping{
		FileID:      fileID,
		FileHash:    t.fileHash,
		FileName:    t.fileName,
		FileSize:    t.fileSize,
		ContentType: t.contentType,
		Recipient:   recipient,
		State:       senderAnnounced,
		CreatedAt:   time.Now(),
	}

	// Mark committed and release the pending slot before attempting persist.
	// The in-memory mapping and Acquire'd ref must survive even if the disk
	// write fails — the caller (PrepareAndSend) may have already sent the
	// DM to the recipient, who now holds a file card. Rolling back the
	// mapping here would leave the recipient with an unserviceable announce
	// and trigger defer Rollback (committed=false) which would also remove
	// the transmit blob. Keeping the mapping in memory lets the transfer
	// work for the current session; reconciliation on restart handles the
	// rest.
	t.committed = true

	// Release the pending slot — the committed ref in senderMaps now
	// occupies the quota instead.
	if t.manager.pendingSenderSlots > 0 {
		t.manager.pendingSenderSlots--
	}

	// Release the FileStore pending reservation — Acquire has already
	// incremented the committed ref count, so RemoveUnreferenced will
	// respect that ref. Without this, pending[hash] leaks forever and
	// blocks transmit-file cleanup after the transfer completes.
	t.manager.store.ReleasePending(t.fileHash)

	if err := t.manager.saveMappingsLockedErr(); err != nil {
		log.Error().Err(err).
			Str("file_id", string(fileID)).
			Str("file_hash", t.fileHash).
			Msg("file_transfer: sender mapping committed in memory but persist failed — transfer may not survive restart")
		return fmt.Errorf("persist sender mapping for %s: %w", fileID, err)
	}

	log.Info().
		Str("file_id", string(fileID)).
		Str("recipient", string(recipient)).
		Str("file_name", t.fileName).
		Uint64("file_size", t.fileSize).
		Msg("file_transfer: registered sender mapping")

	return nil
}

// Rollback releases the reserved sender slot and transmit-file pending
// reservation, then removes the orphaned transmit blob if no other mapping
// or reservation references it. Safe to call multiple times; no-op after
// Commit.
func (t *SenderAnnounceToken) Rollback() {
	if t.committed {
		return
	}

	t.manager.mu.Lock()
	if t.manager.pendingSenderSlots > 0 {
		t.manager.pendingSenderSlots--
	}
	t.manager.mu.Unlock()

	// Release pending BEFORE RemoveUnreferenced — otherwise the pending
	// counter blocks deletion.
	t.manager.store.ReleasePending(t.fileHash)
	t.manager.store.RemoveUnreferenced(t.fileHash)

	t.committed = true // prevent double-rollback
}

// activeSenderCountLocked returns the number of sender mappings that
// occupy a live quota slot: announced and serving. Terminal states
// (completed, tombstone) are kept for dedup/history until tombstoneTTL
// but must not block new file announces.
//
// Caller must hold m.mu.
func (m *Manager) activeSenderCountLocked() int {
	n := 0
	for _, sm := range m.senderMaps {
		if sm.State == senderAnnounced || sm.State == senderServing {
			n++
		}
	}
	return n
}

// PrepareFileAnnounce atomically validates that a file announce can proceed
// and reserves all necessary resources (sender quota slot + transmit-file
// StoreFileForTransmit copies the source file into the transmit directory
// with content-addressed naming, without incrementing the ref count.
// The ref is acquired later by PrepareFileAnnounce → Commit → Acquire.
// Between EnsureStored and Acquire, the pending counter protects the blob
// from premature deletion.
func (m *Manager) StoreFileForTransmit(sourcePath string) (string, error) {
	if m.store == nil {
		return "", fmt.Errorf("transmit store unavailable: sender operations require a configured file store")
	}
	return m.store.EnsureStored(sourcePath)
}

// TransmitFileSize returns the byte size of the stored transmit blob for the
// given hash. The size comes from the persisted copy in the transmit directory,
// not from the original source file — so it always matches the hash.
func (m *Manager) TransmitFileSize(fileHash string) (uint64, error) {
	if m.store == nil {
		return 0, fmt.Errorf("transmit store unavailable: sender operations require a configured file store")
	}
	return m.store.FileSize(fileHash)
}

// RemoveUnreferencedTransmitFile deletes the transmit blob for the given hash
// only if no active sender mapping or pending announce references it. This is
// the cleanup entry point for callers that placed a blob via StoreFileForTransmit
// (EnsureStored) but never reached PrepareFileAnnounce, so no token exists to
// Rollback.
func (m *Manager) RemoveUnreferencedTransmitFile(fileHash string) {
	if m.store == nil {
		return
	}
	m.store.RemoveUnreferenced(fileHash)
}

// pending reservation). Returns a token that the caller uses to either
// Commit (after DM sent successfully) or Rollback (on any failure).
//
// This encapsulates the entire transmit-file and sender-mapping lifecycle
// so that dm_router only deals with sending the DM.
func (m *Manager) PrepareFileAnnounce(
	fileHash, fileName, contentType string,
	fileSize uint64,
) (*SenderAnnounceToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.store == nil {
		return nil, fmt.Errorf("transmit store unavailable: sender operations require a configured file store")
	}

	if m.activeSenderCountLocked()+m.pendingSenderSlots >= maxFileMappings {
		return nil, fmt.Errorf("file mapping limit reached (%d)", maxFileMappings)
	}

	// Atomically verify the transmit blob exists and place the pending
	// reservation. This eliminates the TOCTOU window where a concurrent
	// Rollback could delete the file between HasFile and ReservePending.
	if !m.store.ReservePendingIfExists(fileHash) {
		return nil, fmt.Errorf("transmit file not found for hash %s", fileHash)
	}

	m.pendingSenderSlots++

	return &SenderAnnounceToken{
		manager:     m,
		fileHash:    fileHash,
		fileName:    fileName,
		fileSize:    fileSize,
		contentType: contentType,
	}, nil
}

// chunkServePrep holds the validated, immutable parameters captured under
// lock for a single chunk_request → chunk_response cycle. Separating the
// locked validation from the unlocked I/O keeps the critical section small
// and ensures every guard runs under the same defer Unlock.
type chunkServePrep struct {
	fileHash  string
	fileSize  uint64
	chunkSize uint32
	offset    uint64
	prevState senderState
	recipient domain.PeerIdentity
}

// validateChunkRequest performs all pre-I/O validation for a chunk_request.
// Must be called with m.mu held; the caller is responsible for unlocking
// (via defer m.mu.Unlock()). On success it transitions the mapping to
// senderServing and returns the chunkServePrep. On failure it returns a
// non-nil error — the caller must not proceed with I/O.
func (m *Manager) validateChunkRequest(
	senderIdentity domain.PeerIdentity,
	req domain.ChunkRequestPayload,
) (chunkServePrep, error) {
	if m.store == nil {
		return chunkServePrep{}, fmt.Errorf("transmit store unavailable")
	}

	mapping, ok := m.senderMaps[req.FileID]
	if !ok {
		return chunkServePrep{}, fmt.Errorf("unknown file")
	}

	if senderIdentity != mapping.Recipient {
		return chunkServePrep{}, fmt.Errorf("unauthorized: expected %s, got %s", mapping.Recipient, senderIdentity)
	}

	// The filesystem is the source of truth for serveability, not the
	// state field. A tombstoned mapping whose blob reappeared on disk
	// (re-send of same content, manual restore, etc.) is resurrected
	// to completed so the file can be served without waiting for restart.
	// Acquire is atomic (lock + stat + ref increment) — no TOCTOU window.
	if mapping.State == senderTombstone {
		if err := m.store.Acquire(mapping.FileHash); err != nil {
			return chunkServePrep{}, fmt.Errorf("tombstone: blob not available: %w", err)
		}
		log.Info().
			Str("file_id", string(req.FileID)).
			Str("file_hash", mapping.FileHash).
			Msg("file_transfer: tombstone sender has blob on disk, resurrecting to completed")
		mapping.State = senderCompleted
		m.saveMappingsLocked()
	}

	// senderCompleted is re-servable: the transmit blob is kept on disk
	// so the same file can be re-downloaded (new device, retry, etc.).
	// The mapping transitions back to senderServing for the duration of
	// the re-download and will return to senderCompleted on the next
	// file_downloaded.

	// Gate new transitions (announced/completed → serving) on the concurrency
	// limit. Mappings already serving hold their slot.
	if mapping.State != senderServing {
		if serving := m.activeServingCountLocked(); serving >= maxConcurrentServing {
			return chunkServePrep{}, fmt.Errorf("serving limit reached (%d/%d)", serving, maxConcurrentServing)
		}
	}

	if req.Offset >= mapping.FileSize {
		return chunkServePrep{}, fmt.Errorf("offset %d out of range (file size %d)", req.Offset, mapping.FileSize)
	}

	// --- Validation passed — transition to serving. ---

	prevState := mapping.State
	mapping.State = senderServing
	mapping.LastServedAt = time.Now()

	// Clamp chunk size to the remaining bytes.
	chunkSize := req.Size
	if chunkSize == 0 || chunkSize > domain.DefaultChunkSize {
		chunkSize = domain.DefaultChunkSize
	}
	remaining := mapping.FileSize - req.Offset
	if uint64(chunkSize) > remaining {
		chunkSize = uint32(remaining)
	}

	return chunkServePrep{
		fileHash:  mapping.FileHash,
		fileSize:  mapping.FileSize,
		chunkSize: chunkSize,
		offset:    req.Offset,
		prevState: prevState,
		recipient: senderIdentity,
	}, nil
}

// HandleChunkRequest processes a chunk_request from the receiver.
// The flow is: lock → validate → unlock → I/O → lock → update.
func (m *Manager) HandleChunkRequest(
	senderIdentity domain.PeerIdentity,
	req domain.ChunkRequestPayload,
) {
	// Phase 1: validate under lock.
	m.mu.Lock()
	prep, err := m.validateChunkRequest(senderIdentity, req)
	m.mu.Unlock()

	if err != nil {
		log.Debug().Err(err).
			Str("file_id", string(req.FileID)).
			Str("sender", string(senderIdentity)).
			Msg("file_transfer: chunk_request rejected")
		return
	}

	// rollbackState restores the mapping to its pre-serving state so the
	// serving slot is freed when a transient I/O or send error occurs.
	rollbackState := func() {
		m.mu.Lock()
		if sm, ok := m.senderMaps[req.FileID]; ok && sm.State == senderServing {
			sm.State = prep.prevState
			m.saveMappingsLocked()
		}
		m.mu.Unlock()
	}

	// Phase 2: I/O without lock.
	data, err := m.store.ReadChunk(prep.fileHash, prep.offset, prep.chunkSize)
	if err != nil {
		log.Error().Err(err).Str("file_id", string(req.FileID)).Msg("file_transfer: read chunk failed")
		rollbackState()
		return
	}

	log.Info().
		Str("file_id", string(req.FileID)).
		Str("receiver", string(senderIdentity)).
		Uint64("offset", prep.offset).
		Int("chunk_bytes", len(data)).
		Msg("file_transfer: serving chunk_response")

	respData, err := json.Marshal(domain.ChunkResponsePayload{
		FileID: req.FileID,
		Offset: prep.offset,
		Data:   base64.RawURLEncoding.EncodeToString(data),
	})
	if err != nil {
		log.Error().Err(err).Msg("file_transfer: marshal chunk response failed")
		rollbackState()
		return
	}

	if err := m.sendCommand(senderIdentity, domain.FileCommandPayload{
		Command: domain.FileActionChunkResp,
		Data:    respData,
	}); err != nil {
		log.Warn().Err(err).
			Str("file_id", string(req.FileID)).
			Str("receiver", string(senderIdentity)).
			Msg("file_transfer: send chunk_response failed")
		rollbackState()
		return
	}

	// Phase 3: update served bytes under lock.
	m.mu.Lock()
	if sm, ok := m.senderMaps[req.FileID]; ok {
		sm.BytesServed += uint64(len(data))
		sm.LastServedAt = time.Now()
		m.saveMappingsLocked()
	}
	m.mu.Unlock()
}

// HandleFileDownloaded processes a file_downloaded from the receiver.
// Marks the mapping as completed and sends file_downloaded_ack.
func (m *Manager) HandleFileDownloaded(
	senderIdentity domain.PeerIdentity,
	downloaded domain.FileDownloadedPayload,
) {
	m.mu.Lock()
	mapping, ok := m.senderMaps[downloaded.FileID]
	if !ok {
		m.mu.Unlock()
		return
	}

	if senderIdentity != mapping.Recipient {
		m.mu.Unlock()
		return
	}

	// Idempotent: re-send ack without side effects on duplicate.
	wasAlreadyCompleted := mapping.State == senderCompleted
	if !wasAlreadyCompleted {
		mapping.State = senderCompleted
		mapping.CompletedAt = time.Now()
		m.saveMappingsLocked()

		// The transmit blob is NOT released here. The blob must remain on
		// disk as long as the DM message referencing this file exists —
		// another device or a re-download from the same receiver may request
		// the file again. The ref is released only when the identity is
		// removed (CleanupPeerTransfers) or the mapping expires after
		// tombstoneTTL (tickSenderMappings).
	}
	m.mu.Unlock()

	// Send file_downloaded_ack.
	ackData, err := json.Marshal(domain.FileDownloadedAckPayload(downloaded))
	if err != nil {
		log.Error().Err(err).Msg("file_transfer: marshal file_downloaded_ack failed")
		return
	}

	payload := domain.FileCommandPayload{
		Command: domain.FileActionDownloadedAck,
		Data:    ackData,
	}

	if err := m.sendCommand(senderIdentity, payload); err != nil {
		log.Debug().Err(err).Str("file_id", string(downloaded.FileID)).Msg("file_transfer: send file_downloaded_ack failed")
	}

	if !wasAlreadyCompleted {
		log.Info().
			Str("file_id", string(downloaded.FileID)).
			Str("recipient", string(senderIdentity)).
			Msg("file_transfer: transfer completed (sender)")
	}
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// safeRemoveInDownloadDir removes a file only if it resides within the
// download directory. Logs a warning and skips removal if the path escapes
// the directory boundary (e.g. tampered persistence data). Silently ignores
// already-deleted files. The context string is included in log messages to
// identify the caller.
func (m *Manager) safeRemoveInDownloadDir(path, context string) {
	if path == "" {
		return
	}
	if err := ensureWithinDir(m.downloadDir, path); err != nil {
		log.Warn().Err(err).Str("path", path).
			Msgf("file_transfer: %s skipped — path escapes download dir", context)
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Str("path", path).
			Msgf("file_transfer: %s failed", context)
	}
}

// activeServingCountLocked returns the number of sender mappings in the
// serving state. Must be called with m.mu held.
func (m *Manager) activeServingCountLocked() int {
	count := 0
	for _, sm := range m.senderMaps {
		if sm.State == senderServing {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// retryLoop performs all periodic maintenance on file transfer mappings.
// Every tick it runs a single pass over sender mappings (stall reclaim,
// tombstone cleanup) and a single pass over receiver mappings (chunk stall
// detection, ack retries, auto-resume, completed cleanup).
func (m *Manager) retryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.tickSenderMappings()
			m.tickReceiverMappings()
		case <-m.stopCh:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Unified sender tick — single pass over senderMaps
// ---------------------------------------------------------------------------

// tickSenderMappings performs all periodic maintenance on sender mappings in a
// single pass: reclaims stalled serving slots and removes expired tombstones.
// Combining these into one scan eliminates the class of bugs where a new
// state-dependent rule is added in one scan but forgotten in the other.
func (m *Manager) tickSenderMappings() {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	changed := false

	for id, sm := range m.senderMaps {
		switch sm.State {
		case senderServing:
			// Reclaim abandoned slots: if no chunk was served within the
			// stall timeout, the receiver likely disappeared. Revert to
			// announced so new chunk_request messages can re-acquire it.
			if now.Sub(sm.LastServedAt) >= senderServingStallTimeout {
				sm.State = senderAnnounced
				changed = true
				log.Info().
					Str("file_id", string(sm.FileID)).
					Str("recipient", string(sm.Recipient)).
					Dur("stalled_for", now.Sub(sm.LastServedAt)).
					Msg("file_transfer: reclaimed stalled serving slot → announced")
			}

		case senderTombstone, senderCompleted:
			if now.Sub(sm.CompletedAt) > tombstoneTTL {
				// Release the transmit file ref when the mapping expires.
				// The blob is deleted only if no other mapping holds a ref
				// for the same hash (e.g. re-send of the same content).
				if m.store != nil {
					m.store.Release(sm.FileHash)
				}
				delete(m.senderMaps, id)
				changed = true
			}
		}
	}

	if changed {
		m.saveMappingsLocked()
	}
}

// ---------------------------------------------------------------------------
// Command dispatch (called by FileRouter for local delivery)
// ---------------------------------------------------------------------------

// HandleLocalFileCommand dispatches a decrypted file command payload to the
// appropriate handler based on the command type.
func (m *Manager) HandleLocalFileCommand(
	senderIdentity domain.PeerIdentity,
	payload *domain.FileCommandPayload,
) {
	switch payload.Command {
	case domain.FileActionChunkReq:
		var req domain.ChunkRequestPayload
		if err := json.Unmarshal(payload.Data, &req); err != nil {
			log.Debug().Err(err).Msg("file_transfer: unmarshal chunk_request failed")
			return
		}
		m.HandleChunkRequest(senderIdentity, req)

	case domain.FileActionChunkResp:
		var resp domain.ChunkResponsePayload
		if err := json.Unmarshal(payload.Data, &resp); err != nil {
			log.Debug().Err(err).Msg("file_transfer: unmarshal chunk_response failed")
			return
		}
		m.HandleChunkResponse(senderIdentity, resp)

	case domain.FileActionDownloaded:
		var dl domain.FileDownloadedPayload
		if err := json.Unmarshal(payload.Data, &dl); err != nil {
			log.Debug().Err(err).Msg("file_transfer: unmarshal file_downloaded failed")
			return
		}
		m.HandleFileDownloaded(senderIdentity, dl)

	case domain.FileActionDownloadedAck:
		var ack domain.FileDownloadedAckPayload
		if err := json.Unmarshal(payload.Data, &ack); err != nil {
			log.Debug().Err(err).Msg("file_transfer: unmarshal file_downloaded_ack failed")
			return
		}
		m.HandleFileDownloadedAck(senderIdentity, ack)

	default:
		log.Debug().Str("command", string(payload.Command)).Msg("file_transfer: unknown command")
	}
}

// ---------------------------------------------------------------------------
// Query methods (for RPC observability)
// ---------------------------------------------------------------------------

// SenderMappingCount returns the number of active sender file mappings.
func (m *Manager) SenderMappingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.senderMaps)
}

// ReceiverMappingCount returns the number of active receiver file mappings.
func (m *Manager) ReceiverMappingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.receiverMaps)
}

// ActiveServingCount returns the number of files currently being served.
func (m *Manager) ActiveServingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeServingCountLocked()
}

// ActiveDownloadCount returns the number of files currently being downloaded.
func (m *Manager) ActiveDownloadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeDownloadCountLocked()
}

// RemoveSenderMapping removes a single sender mapping by fileID, releasing
// the transmit file ref if the mapping is not in a terminal state. Returns
// true if the mapping existed and was removed.
//
// Use this for targeted cleanup when a specific committed mapping must be
// revoked (e.g. stale goroutine detected post-Commit). Prefer this over
// CleanupPeerTransfers when only one mapping is orphaned — the broad
// peer-level cleanup would destroy legitimate transfers for the same peer.
func (m *Manager) RemoveSenderMapping(fileID domain.FileID) bool {
	m.mu.Lock()

	mapping, exists := m.senderMaps[fileID]
	if !exists {
		m.mu.Unlock()
		return false
	}

	// All states except tombstone hold a ref to the transmit blob.
	// Tombstone mappings either lost their blob (crash recovery) or were
	// explicitly released, so no ref to drop.
	var releaseHash string
	if mapping.State != senderTombstone {
		releaseHash = mapping.FileHash
	}

	delete(m.senderMaps, fileID)
	m.saveMappingsLocked()
	m.mu.Unlock()

	if releaseHash != "" && m.store != nil {
		m.store.Release(releaseHash)
	}

	log.Info().
		Str("file_id", string(fileID)).
		Msg("file_transfer: removed orphaned sender mapping")

	return true
}

// CleanupPeerTransfers removes all sender and receiver mappings associated
// with the given peer identity. For sender mappings where the peer is the
// recipient, the transmit file ref count is released (and the file deleted
// if no other mapping references it). For receiver mappings where the peer
// is the sender, completed downloads and partial files are deleted.
//
// Called when a chat/identity is deleted from the UI.
func (m *Manager) CleanupPeerTransfers(peer domain.PeerIdentity) {
	m.mu.Lock()

	// Collect sender mappings to remove.
	var senderHashes []string
	var senderIDs []domain.FileID
	for id, mapping := range m.senderMaps {
		if mapping.Recipient == peer {
			// All states except tombstone hold a ref to the transmit blob.
			if mapping.State != senderTombstone {
				senderHashes = append(senderHashes, mapping.FileHash)
			}
			senderIDs = append(senderIDs, id)
		}
	}

	// Collect receiver mappings to remove.
	type receiverCleanup struct {
		fileID        domain.FileID
		completedPath string
		state         receiverState
	}
	var receiverEntries []receiverCleanup
	for id, mapping := range m.receiverMaps {
		if mapping.Sender == peer {
			cp := m.backfillCompletedPath(mapping)
			receiverEntries = append(receiverEntries, receiverCleanup{
				fileID:        id,
				completedPath: cp,
				state:         mapping.State,
			})
		}
	}

	// Remove from maps.
	for _, id := range senderIDs {
		delete(m.senderMaps, id)
	}
	for _, entry := range receiverEntries {
		delete(m.receiverMaps, entry.fileID)
	}

	changed := len(senderIDs) > 0 || len(receiverEntries) > 0
	if changed {
		m.saveMappingsLocked()
	}
	m.mu.Unlock()

	// Release transmit file refs outside the lock (may trigger file I/O).
	if m.store != nil {
		for _, hash := range senderHashes {
			m.store.Release(hash)
		}
	}

	// Delete downloaded and partial files.
	for _, entry := range receiverEntries {
		// Delete completed download — safeRemoveInDownloadDir verifies the
		// path stays within the download directory before removing.
		m.safeRemoveInDownloadDir(entry.completedPath, "peer cleanup")

		// Delete partial download (may exist if download was in progress).
		partial := partialDownloadPath(m.downloadDir, entry.fileID)
		if err := os.Remove(partial); err != nil && !os.IsNotExist(err) {
			log.Warn().Err(err).Str("path", partial).Msg("file_transfer: cleanup partial download failed")
		}
	}

	if changed {
		log.Info().
			Str("peer", string(peer)).
			Int("sender_removed", len(senderIDs)).
			Int("receiver_removed", len(receiverEntries)).
			Msg("file_transfer: peer transfers cleaned up")
	}
}

// SenderProgress returns the transfer progress for an outgoing file.
// Returns zeros and found=false if the file ID is not tracked.
func (m *Manager) SenderProgress(fileID domain.FileID) (bytesServed, totalSize uint64, state string, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mapping, ok := m.senderMaps[fileID]
	if !ok {
		return 0, 0, "", false
	}
	return mapping.BytesServed, mapping.FileSize, string(mapping.State), true
}

// ReceiverProgress returns the download progress for an incoming file.
// Returns zeros and found=false if the file ID is not tracked.
func (m *Manager) ReceiverProgress(fileID domain.FileID) (bytesReceived, totalSize uint64, state string, found bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mapping, ok := m.receiverMaps[fileID]
	if !ok {
		return 0, 0, "", false
	}
	return mapping.BytesReceived, mapping.FileSize, string(mapping.State), true
}
