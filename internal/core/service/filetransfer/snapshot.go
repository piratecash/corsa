package filetransfer

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// RPC snapshot types — used by fetch_file_transfers / fetch_file_mapping
// ---------------------------------------------------------------------------

// TransferSnapshot is a JSON-serializable snapshot of a single transfer
// (sender or receiver). Used by the RPC layer for observability.
type TransferSnapshot struct {
	FileID      domain.FileID       `json:"file_id"`
	FileHash    string              `json:"file_hash"`
	FileName    string              `json:"file_name"`
	FileSize    uint64              `json:"file_size"`
	ContentType string              `json:"content_type"`
	Peer        domain.PeerIdentity `json:"peer"`
	Direction   string              `json:"direction"` // "send" or "receive"
	State       string              `json:"state"`
	Bytes       uint64              `json:"bytes_transferred"`
	CreatedAt   string              `json:"created_at"`
	CompletedAt string              `json:"completed_at,omitempty"`
}

// SenderMappingEntry is a JSON-serializable snapshot of a sender file
// mapping. TransmitPath is intentionally excluded from the JSON output.
// This type exists as a separate RPC contract from TransferSnapshot —
// the JSON field names differ (recipient vs peer, bytes_served vs
// bytes_transferred) and must remain stable for client compatibility.
type SenderMappingEntry struct {
	FileID      domain.FileID       `json:"file_id"`
	FileHash    string              `json:"file_hash"`
	FileName    string              `json:"file_name"`
	FileSize    uint64              `json:"file_size"`
	ContentType string              `json:"content_type"`
	Recipient   domain.PeerIdentity `json:"recipient"`
	State       string              `json:"state"`
	BytesServed uint64              `json:"bytes_served"`
	// ProgressBytes is the highest contiguous byte position ever served.
	// Unlike BytesServed (which is a cumulative bandwidth counter that grows
	// on every chunk_response, including re-served offsets), ProgressBytes
	// only advances when the sender serves data beyond the previous high-water
	// mark. Use this field for progress display; use BytesServed for bandwidth
	// accounting.
	ProgressBytes uint64 `json:"progress_bytes"`
	CreatedAt     string `json:"created_at"`
	CompletedAt   string `json:"completed_at,omitempty"`
}

// ---------------------------------------------------------------------------
// Snapshot builders
// ---------------------------------------------------------------------------

// senderToTransferEntry converts a sender mapping to a TransferSnapshot.
func senderToTransferEntry(sm *senderFileMapping) TransferSnapshot {
	e := TransferSnapshot{
		FileID:      sm.FileID,
		FileHash:    sm.FileHash,
		FileName:    sm.FileName,
		FileSize:    sm.FileSize,
		ContentType: sm.ContentType,
		Peer:        sm.Recipient,
		Direction:   "send",
		State:       string(sm.State),
		Bytes:       sm.ProgressBytes,
		CreatedAt:   sm.CreatedAt.UTC().Format(time.RFC3339),
	}
	if !sm.CompletedAt.IsZero() {
		e.CompletedAt = sm.CompletedAt.UTC().Format(time.RFC3339)
	}
	return e
}

// receiverToTransferEntry converts a receiver mapping to a TransferSnapshot.
func receiverToTransferEntry(rm *receiverFileMapping) TransferSnapshot {
	e := TransferSnapshot{
		FileID:      rm.FileID,
		FileHash:    rm.FileHash,
		FileName:    rm.FileName,
		FileSize:    rm.FileSize,
		ContentType: rm.ContentType,
		Peer:        rm.Sender,
		Direction:   "receive",
		State:       string(rm.State),
		Bytes:       rm.BytesReceived,
		CreatedAt:   rm.CreatedAt.UTC().Format(time.RFC3339),
	}
	if !rm.CompletedAt.IsZero() {
		e.CompletedAt = rm.CompletedAt.UTC().Format(time.RFC3339)
	}
	return e
}

// senderToMappingEntry converts a sender mapping to a SenderMappingEntry.
func senderToMappingEntry(sm *senderFileMapping) SenderMappingEntry {
	e := SenderMappingEntry{
		FileID:        sm.FileID,
		FileHash:      sm.FileHash,
		FileName:      sm.FileName,
		FileSize:      sm.FileSize,
		ContentType:   sm.ContentType,
		Recipient:     sm.Recipient,
		State:         string(sm.State),
		BytesServed:   sm.BytesServed,
		ProgressBytes: sm.ProgressBytes,
		CreatedAt:     sm.CreatedAt.UTC().Format(time.RFC3339),
	}
	if !sm.CompletedAt.IsZero() {
		e.CompletedAt = sm.CompletedAt.UTC().Format(time.RFC3339)
	}
	return e
}

// ---------------------------------------------------------------------------
// Manager snapshot methods
// ---------------------------------------------------------------------------

// isSenderTerminal returns true for sender states that represent a finished
// transfer and should not appear in the active/pending snapshot.
func isSenderTerminal(s senderState) bool {
	return s == senderCompleted || s == senderTombstone
}

// isReceiverTerminal returns true for receiver states that represent a
// finished transfer and should not appear in the active/pending snapshot.
func isReceiverTerminal(s receiverState) bool {
	return s == receiverCompleted || s == receiverFailed
}

// TransfersSnapshot returns a list of active and pending sender/receiver
// mappings as JSON-serializable entries. Terminal states (completed,
// tombstone, failed) are excluded — the RPC contract promises only
// active/pending transfers. TransmitPath is never exposed.
func (m *Manager) TransfersSnapshot() []TransferSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]TransferSnapshot, 0, len(m.senderMaps)+len(m.receiverMaps))
	for _, sm := range m.senderMaps {
		if isSenderTerminal(sm.State) {
			continue
		}
		entries = append(entries, senderToTransferEntry(sm))
	}
	for _, rm := range m.receiverMaps {
		if isReceiverTerminal(rm.State) {
			continue
		}
		entries = append(entries, receiverToTransferEntry(rm))
	}
	return entries
}

// AllTransfersSnapshot returns ALL sender/receiver mappings as
// JSON-serializable entries, INCLUDING terminal states (completed,
// failed, tombstone). The UI file tab needs full history — completed
// transfers shown with size/date and the option to delete the
// originating chat message; failed receiver transfers shown so the
// user can decide to restart or remove them. Use TransfersSnapshot
// when only active/pending transfers should appear (e.g. the existing
// fetchFileTransfers RPC for observability).
//
// State remains the discriminator the UI filters on (downloading vs
// completed vs failed). TransmitPath is never exposed for senders.
func (m *Manager) AllTransfersSnapshot() []TransferSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]TransferSnapshot, 0, len(m.senderMaps)+len(m.receiverMaps))
	for _, sm := range m.senderMaps {
		entries = append(entries, senderToTransferEntry(sm))
	}
	for _, rm := range m.receiverMaps {
		entries = append(entries, receiverToTransferEntry(rm))
	}
	return entries
}

// MappingsSnapshot returns active and pending sender file mappings as
// JSON-serializable entries. Terminal states (completed, tombstone) are
// excluded to match the active/pending contract. TransmitPath is never
// exposed.
func (m *Manager) MappingsSnapshot() []SenderMappingEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]SenderMappingEntry, 0, len(m.senderMaps))
	for _, sm := range m.senderMaps {
		if isSenderTerminal(sm.State) {
			continue
		}
		entries = append(entries, senderToMappingEntry(sm))
	}
	return entries
}
