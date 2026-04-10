package filetransfer

import (
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
// Persistable transfer entry
// ---------------------------------------------------------------------------

// persistedTransferEntry is the on-disk representation of a single file
// transfer mapping (sender or receiver). It captures all fields needed to
// resume or display transfer state after a node restart.
//
// Design rationale:
//   - Single struct for both roles avoids divergent schemas. The Role field
//     distinguishes sender from receiver.
//   - TransmitPath is stored only for senders (points to <dataDir>/transmit/<hash>.<ext>).
//   - Retry-related fields are stored only for receivers in waiting_ack state.
//   - time.Time fields serialize as RFC 3339 via json.Marshal default behavior.
//   - time.Duration fields serialize as nanosecond int64 (json.Marshal default).
type persistedTransferEntry struct {
	// Common fields.
	FileID      domain.FileID       `json:"file_id"`
	FileHash    string              `json:"file_hash"`
	FileName    string              `json:"file_name"`
	FileSize    uint64              `json:"file_size"`
	ContentType string              `json:"content_type"`
	Peer        domain.PeerIdentity `json:"peer"`  // recipient (sender role) or sender (receiver role)
	Role        string              `json:"role"`  // "sender" or "receiver"
	State       string              `json:"state"` // senderState or receiverState as string
	CreatedAt   time.Time           `json:"created_at"`
	CompletedAt time.Time           `json:"completed_at,omitempty"`

	// Sender-specific.
	BytesServed   uint64    `json:"bytes_served,omitempty"`
	LastServedAt  time.Time `json:"last_served_at,omitempty"`
	TransmitPath  string    `json:"transmit_path,omitempty"`
	PreServeState string    `json:"pre_serve_state,omitempty"`

	// Receiver-specific.
	BytesReceived    uint64        `json:"bytes_received,omitempty"`
	NextOffset       uint64        `json:"next_offset,omitempty"`
	ChunkSize        uint32        `json:"chunk_size,omitempty"`
	LastChunkAt      time.Time     `json:"last_chunk_at,omitempty"`
	ChunkRetries     int           `json:"chunk_retries,omitempty"`
	RetryCount       int           `json:"retry_count,omitempty"`
	RetryBackoff     time.Duration `json:"retry_backoff,omitempty"`
	DownloadedSentAt time.Time     `json:"downloaded_sent_at,omitempty"`
	CompletedPath    string        `json:"completed_path,omitempty"`

	// Shared: epoch of the current serving run. For senders it is the
	// monotonic counter bumped on every genuine transition into
	// senderServing; for receivers it is the most recent value observed
	// from a chunk_response (echoed back in file_downloaded). Must survive
	// restart so that the epoch counter remains strictly monotonic across
	// process lifetimes — otherwise a post-restart re-download could
	// collide with a still-live stale file_downloaded from a prior run.
	// Omitted when zero (legacy / never served).
	ServingEpoch uint64 `json:"serving_epoch,omitempty"`
}

// persistedTransferFile is the top-level JSON structure written to disk.
type persistedTransferFile struct {
	Version   int                      `json:"version"`
	UpdatedAt time.Time                `json:"updated_at"`
	Transfers []persistedTransferEntry `json:"transfers"`
}

const persistVersion = 1

// ---------------------------------------------------------------------------
// Conversion: in-memory ↔ persisted
// ---------------------------------------------------------------------------

func senderMappingToEntry(m *senderFileMapping) persistedTransferEntry {
	return persistedTransferEntry{
		FileID:        m.FileID,
		FileHash:      m.FileHash,
		FileName:      m.FileName,
		FileSize:      m.FileSize,
		ContentType:   m.ContentType,
		Peer:          m.Recipient,
		Role:          "sender",
		State:         string(m.State),
		CreatedAt:     m.CreatedAt,
		CompletedAt:   m.CompletedAt,
		BytesServed:   m.BytesServed,
		LastServedAt:  m.LastServedAt,
		TransmitPath:  m.TransmitPath,
		PreServeState: string(m.PreServeState),
		ServingEpoch:  m.ServingEpoch,
	}
}

func receiverMappingToEntry(m *receiverFileMapping) persistedTransferEntry {
	return persistedTransferEntry{
		FileID:           m.FileID,
		FileHash:         m.FileHash,
		FileName:         m.FileName,
		FileSize:         m.FileSize,
		ContentType:      m.ContentType,
		Peer:             m.Sender,
		Role:             "receiver",
		State:            string(m.State),
		CreatedAt:        m.CreatedAt,
		CompletedAt:      m.CompletedAt,
		BytesReceived:    m.BytesReceived,
		NextOffset:       m.NextOffset,
		ChunkSize:        m.ChunkSize,
		LastChunkAt:      m.LastChunkAt,
		ChunkRetries:     m.ChunkRetries,
		RetryCount:       m.DownloadedRetries,
		RetryBackoff:     m.DownloadedBackoff,
		DownloadedSentAt: m.DownloadedSentAt,
		CompletedPath:    m.CompletedPath,
		ServingEpoch:     m.ServingEpoch,
	}
}

func entryToSenderMapping(e persistedTransferEntry) *senderFileMapping {
	return &senderFileMapping{
		FileID:        e.FileID,
		FileHash:      e.FileHash,
		FileName:      e.FileName,
		FileSize:      e.FileSize,
		ContentType:   e.ContentType,
		Recipient:     e.Peer,
		State:         senderState(e.State),
		PreServeState: senderState(e.PreServeState),
		CreatedAt:     e.CreatedAt,
		CompletedAt:   e.CompletedAt,
		BytesServed:   e.BytesServed,
		LastServedAt:  e.LastServedAt,
		TransmitPath:  e.TransmitPath,
		ServingEpoch:  e.ServingEpoch,
	}
}

func entryToReceiverMapping(e persistedTransferEntry) *receiverFileMapping {
	rm := &receiverFileMapping{
		FileID:            e.FileID,
		FileHash:          e.FileHash,
		FileName:          e.FileName,
		FileSize:          e.FileSize,
		ContentType:       e.ContentType,
		Sender:            e.Peer,
		State:             receiverState(e.State),
		CreatedAt:         e.CreatedAt,
		CompletedAt:       e.CompletedAt,
		BytesReceived:     e.BytesReceived,
		NextOffset:        e.NextOffset,
		ChunkSize:         e.ChunkSize,
		LastChunkAt:       e.LastChunkAt,
		ChunkRetries:      e.ChunkRetries,
		DownloadedRetries: e.RetryCount,
		DownloadedBackoff: e.RetryBackoff,
		DownloadedSentAt:  e.DownloadedSentAt,
		CompletedPath:     e.CompletedPath,
		ServingEpoch:      e.ServingEpoch,
	}
	normalizeReceiverMapping(rm)
	return rm
}

// ---------------------------------------------------------------------------
// Save / Load
// ---------------------------------------------------------------------------

// saveMappingsLocked writes the current sender and receiver maps to disk as
// an atomic JSON file (write to temp → rename). Must be called with m.mu held.
func (m *Manager) saveMappingsLocked() {
	if err := m.saveMappingsLockedErr(); err != nil {
		log.Error().Err(err).Str("path", m.mappingsPath).
			Msg("file_transfer: save mappings failed")
	}
}

// saveMappingsLockedErr persists all sender and receiver mappings to disk
// and returns an error if the write fails. Used by Commit where a persist
// failure must be observable so the caller can roll back the in-memory state.
// Must be called with m.mu held.
func (m *Manager) saveMappingsLockedErr() error {
	if m.mappingsPath == "" {
		return nil
	}

	entries := make([]persistedTransferEntry, 0, len(m.senderMaps)+len(m.receiverMaps))

	for _, sm := range m.senderMaps {
		entries = append(entries, senderMappingToEntry(sm))
	}
	for _, rm := range m.receiverMaps {
		entries = append(entries, receiverMappingToEntry(rm))
	}

	pf := persistedTransferFile{
		Version:   persistVersion,
		UpdatedAt: time.Now(),
		Transfers: entries,
	}

	return atomicWriteJSON(m.mappingsPath, pf)
}

// loadMappings restores persisted sender and receiver mappings from disk.
// Called once during initialization, before Start().
//
// Returns activeHashes — ref counts from non-tombstone sender mappings —
// so ValidateOnStartup can rebuild the FileStore ref table.
func (m *Manager) loadMappings() (activeHashes map[string]int) {
	if m.mappingsPath == "" {
		return nil
	}

	data, err := os.ReadFile(m.mappingsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Warn().Err(err).Str("path", m.mappingsPath).
			Msg("file_transfer: read mappings file failed")
		return nil
	}

	var pf persistedTransferFile
	if err := json.Unmarshal(data, &pf); err != nil {
		log.Error().Err(err).Str("path", m.mappingsPath).
			Msg("file_transfer: unmarshal mappings file failed, starting fresh")
		return nil
	}

	if pf.Version != persistVersion {
		log.Warn().
			Int("file_version", pf.Version).
			Int("expected_version", persistVersion).
			Str("path", m.mappingsPath).
			Msg("file_transfer: mappings version mismatch, starting fresh")
		return nil
	}

	activeHashes = make(map[string]int)
	senderRepaired := false

	for _, entry := range pf.Transfers {
		switch entry.Role {
		case "sender":
			sm := entryToSenderMapping(entry)

			// Validate state before inserting into the state machine.
			if _, ok := validSenderStates[sm.State]; !ok {
				log.Warn().
					Str("file_id", string(sm.FileID)).
					Str("state", string(sm.State)).
					Msg("file_transfer: persisted sender entry has unknown state, skipping")
				continue
			}

			// Validate PreServeState: stall reclaim restores
			// sm.State = sm.PreServeState, so an invalid value would
			// move the mapping into an arbitrary state string. Clear
			// it — the reclaim fallback treats empty as senderAnnounced.
			if sm.PreServeState != "" {
				if _, ok := validSenderStates[sm.PreServeState]; !ok {
					log.Warn().
						Str("file_id", string(sm.FileID)).
						Str("pre_serve_state", string(sm.PreServeState)).
						Msg("file_transfer: persisted sender entry has invalid pre_serve_state, clearing")
					sm.PreServeState = ""
					senderRepaired = true
				}
			}

			// Validate hash before using it as a ref-count key or glob pattern.
			if err := domain.ValidateFileHash(sm.FileHash); err != nil {
				log.Warn().Err(err).
					Str("file_id", string(sm.FileID)).
					Msg("file_transfer: persisted sender entry has invalid file hash, skipping")
				continue
			}

			// Sanitize persisted file name — the JSON file could be tampered with.
			sm.FileName = domain.SanitizeFileName(sm.FileName)

			// All sender mappings except tombstone hold a ref to the transmit
			// file. The blob must remain on disk as long as the DM message
			// exists — another device or re-download may request the file.
			//
			// If the blob is missing (crash or external deletion), tombstone
			// the mapping so loadMappings records the loss. The blob is NOT
			// deleted here — it may not exist at all, or may belong to
			// another process. Runtime resurrection in validateChunkRequest
			// will recover the mapping if the blob reappears later.
			if sm.State != senderTombstone {
				if m.store != nil && !m.store.HasFile(sm.FileHash) {
					log.Warn().
						Str("file_id", string(sm.FileID)).
						Str("file_hash", sm.FileHash).
						Str("prev_state", string(sm.State)).
						Msg("file_transfer: sender mapping has no transmit file on disk, tombstoning")
					sm.State = senderTombstone
					// PreServeState is only meaningful for senderServing; a
					// tombstone must not carry a stale serving origin around.
					sm.PreServeState = ""
					sm.CompletedAt = time.Now()
					senderRepaired = true
				} else {
					activeHashes[sm.FileHash]++
				}
			} else {
				// Tombstone with blob on disk: the blob has reappeared
				// (re-send of same content, manual restore, etc.).
				// Resurrect to completed so the file can be served again.
				if m.store != nil && m.store.HasFile(sm.FileHash) {
					log.Info().
						Str("file_id", string(sm.FileID)).
						Str("file_hash", sm.FileHash).
						Msg("file_transfer: tombstone sender has transmit file on disk, resurrecting to completed")
					sm.State = senderCompleted
					// Same invariant: senderCompleted does not carry
					// PreServeState; the next chunk_request will capture
					// the pre-serve state under lock when promoting to
					// senderServing.
					sm.PreServeState = ""
					// Reset CompletedAt to now so the 30-day tombstoneTTL
					// in tickSenderMappings restarts from the resurrection
					// moment. Without this, an old tombstone (CompletedAt
					// weeks ago) resurrected at startup would be purged on
					// the very next maintenance tick, defeating the
					// resurrection path for older entries.
					sm.CompletedAt = time.Now()
					senderRepaired = true
					activeHashes[sm.FileHash]++
				}
				// Tombstone without blob: no ref, no cleanup — blob
				// deletion happens only via identity/message removal.
			}

			m.senderMaps[sm.FileID] = sm

		case "receiver":
			rm := entryToReceiverMapping(entry)

			// Validate state before inserting into the state machine.
			if _, ok := validReceiverStates[rm.State]; !ok {
				log.Warn().
					Str("file_id", string(rm.FileID)).
					Str("state", string(rm.State)).
					Msg("file_transfer: persisted receiver entry has unknown state, skipping")
				continue
			}

			// Re-apply the same metadata checks enforced by RegisterFileReceive
			// at the network boundary. A tampered or corrupted transfers.json
			// must not resurrect entries that runtime code would refuse to register.
			if err := domain.ValidateFileHash(rm.FileHash); err != nil {
				log.Warn().Err(err).
					Str("file_id", string(rm.FileID)).
					Msg("file_transfer: persisted receiver entry has invalid file hash, skipping")
				continue
			}
			if rm.FileSize == 0 {
				log.Warn().
					Str("file_id", string(rm.FileID)).
					Msg("file_transfer: persisted receiver entry has zero file size, skipping")
				continue
			}

			// Sanitize persisted file name — the JSON file could be tampered with.
			rm.FileName = domain.SanitizeFileName(rm.FileName)

			if rm.FileName == "unnamed" && strings.TrimSpace(entry.FileName) == "" {
				log.Warn().
					Str("file_id", string(rm.FileID)).
					Msg("file_transfer: persisted receiver entry has empty file name, skipping")
				continue
			}

			// Backfill CompletedPath for entries persisted before the field
			// was added. Without this, cleanup cannot locate the downloaded
			// file on disk.
			rm.CompletedPath = m.backfillCompletedPath(rm)
			// Validate that CompletedPath (from JSON or backfill) stays inside downloads.
			if rm.CompletedPath != "" && m.downloadDir != "" {
				if err := ensureWithinDir(m.downloadDir, rm.CompletedPath); err != nil {
					log.Warn().
						Str("file_id", string(rm.FileID)).
						Str("path", rm.CompletedPath).
						Msg("file_transfer: persisted completed_path escapes download dir, clearing")
					rm.CompletedPath = ""
				}
			}
			// Assign a unique generation so deferred actions created for
			// this mapping can be distinguished from actions belonging to
			// a future re-registration of the same fileID.
			m.nextGeneration++
			rm.Generation = m.nextGeneration
			m.receiverMaps[rm.FileID] = rm

		default:
			log.Warn().Str("role", entry.Role).Str("file_id", string(entry.FileID)).
				Msg("file_transfer: unknown role in persisted entry, skipping")
		}
	}

	// Reconcile orphaned receiverVerifying entries. A crash between
	// os.Rename (moving .part → downloads) and persisting receiverWaitingAck
	// leaves the mapping in receiverVerifying with the completed file already
	// on disk. Without recovery, the transfer is stuck forever.
	//
	// Strategy:
	//   1. If the completed file exists in downloads → promote to waitingAck
	//      (same as if the rename+persist had completed atomically).
	//   2. If the .part file exists → reset to downloading so the retry loop
	//      can re-verify or re-download from the current offset.
	//   3. Neither file exists → mark failed (unrecoverable).
	repaired := m.reconcileVerifyingOnStartup()

	// Persist repaired states so the next restart does not re-reconcile from
	// stale disk state. Without this, tombstoned/failed entries get CompletedAt
	// reset on every boot and the retention TTL never ages out on disk.
	if repaired || senderRepaired {
		m.saveMappingsLocked()
	}

	log.Info().
		Int("sender_count", len(m.senderMaps)).
		Int("receiver_count", len(m.receiverMaps)).
		Bool("receiver_reconciled", repaired).
		Bool("sender_reconciled", senderRepaired).
		Msg("file_transfer: loaded persisted mappings")

	return activeHashes
}

// reconcileVerifyingOnStartup recovers receiver mappings that were persisted
// in receiverVerifying state — indicating a crash during onDownloadComplete.
// Returns true if any mapping was repaired so the caller can persist the
// updated state immediately. Called once during loadMappings, before Start().
// No lock needed because background goroutines have not been launched yet.
func (m *Manager) reconcileVerifyingOnStartup() bool {
	repaired := false
	for _, rm := range m.receiverMaps {
		if rm.State != receiverVerifying {
			continue
		}

		repaired = true

		// Case 1: the final rename completed but the state update was lost.
		// Probe the downloads directory for the completed file.
		completedPath := resolveExistingDownload(m.downloadDir, rm.FileName, rm.FileHash)
		if completedPath != "" {
			rm.State = receiverWaitingAck
			rm.CompletedPath = completedPath
			rm.DownloadedSentAt = time.Time{} // will fire on next tickReceiverMappings tick
			rm.DownloadedBackoff = initialRetryTimeout
			log.Info().
				Str("file_id", string(rm.FileID)).
				Str("path", completedPath).
				Msg("file_transfer: reconciled verifying → waiting_ack (completed file found)")
			continue
		}

		// Case 2: the rename did not happen — the .part file may still exist.
		// Reset to waitingRoute so the retry loop can re-request chunks from
		// the current offset (or 0 if the partial file is gone/truncated/oversized).
		partialPath := partialDownloadPath(m.downloadDir, rm.FileID)
		if info, err := os.Stat(partialPath); err == nil {
			partialSize := uint64(info.Size())

			// A .part larger than FileSize is corrupted or tampered.
			// Restart from scratch — the stale file will be truncated by
			// prepareResumeLocked when the resume fires.
			if partialSize > rm.FileSize {
				log.Warn().
					Str("file_id", string(rm.FileID)).
					Uint64("partial_size", partialSize).
					Uint64("file_size", rm.FileSize).
					Msg("file_transfer: partial file exceeds file size, resetting to offset 0")
				partialSize = 0
			}

			// Clear any stale CompletedPath that may have been persisted or
			// backfilled unconditionally: this branch established that no
			// completed file exists on disk, so CompletedPath must not leak
			// into later cleanup paths (e.g. CleanupPeerTransfers) where it
			// could delete an unrelated file in the downloads directory.
			rm.CompletedPath = ""
			rm.State = receiverWaitingRoute
			rm.NextOffset = partialSize
			rm.BytesReceived = partialSize
			log.Info().
				Str("file_id", string(rm.FileID)).
				Uint64("offset", rm.NextOffset).
				Msg("file_transfer: reconciled verifying → waiting_route (partial file found)")
			continue
		}

		// Case 3: neither file exists — unrecoverable without re-announce.
		// Same rationale as Case 2: zero CompletedPath so stale backfilled
		// state cannot be mistaken for a real completed download later.
		rm.CompletedPath = ""
		rm.State = receiverFailed
		rm.CompletedAt = time.Now()
		log.Warn().
			Str("file_id", string(rm.FileID)).
			Msg("file_transfer: reconciled verifying → failed (no files on disk)")
	}
	return repaired
}

// ---------------------------------------------------------------------------
// Atomic JSON write
// ---------------------------------------------------------------------------

// atomicWriteJSON marshals v to indented JSON and writes it atomically:
// write to a temporary file in the same directory, then rename over the target.
// This prevents corruption if the process crashes mid-write.
func atomicWriteJSON(path string, v interface{}) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		// Clean up temp file on rename failure.
		if removeErr := os.Remove(tmpPath); removeErr != nil {
			log.Warn().Err(removeErr).Str("path", tmpPath).
				Msg("file_transfer: remove temp file failed during cleanup")
		}
		return fmt.Errorf("rename temp to target: %w", err)
	}

	return nil
}

// TransfersMappingsPath builds the identity-scoped path for the transfers JSON
// file, following the same naming convention as chatlog:
//
//	<dataDir>/transfers-<identity_short>-<port>.json
//
// This allows multiple node identities on the same machine to coexist without
// collisions. The file sits alongside identity-*.json, trust-*.json, etc.
func TransfersMappingsPath(dataDir string, identityAddr domain.PeerIdentity, listenAddress domain.ListenAddress) string {
	short := string(identityAddr)
	if len(short) > 8 {
		short = short[:8]
	}

	port := "default"
	addr := string(listenAddress)
	if idx := strings.LastIndex(addr, ":"); idx >= 0 && idx < len(addr)-1 {
		port = addr[idx+1:]
	}

	fileName := fmt.Sprintf("transfers-%s-%s.json", short, port)
	return filepath.Join(dataDir, fileName)
}
