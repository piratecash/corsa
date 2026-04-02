package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"corsa/internal/core/protocol"
)

// queueStateVersion tracks the queue-state file schema.  Bump this when the
// on-disk format changes in a way that requires a one-time migration on load.
//
//	0 — implicit; pre-canonicalisation era (pending keyed by dial address)
//	1 — pending keyed by primary peer address; orphaned section added
const queueStateVersion = 1

type queueStateFile struct {
	Version            int                         `json:"version"`
	Pending            map[string][]pendingFrame   `json:"pending,omitempty"`
	Orphaned           map[string][]pendingFrame   `json:"orphaned,omitempty"`
	RelayRetry         map[string]relayAttempt     `json:"relay_retry,omitempty"`
	RelayMessages      []protocol.Envelope         `json:"relay_messages,omitempty"`
	RelayReceipts      []protocol.DeliveryReceipt  `json:"relay_receipts,omitempty"`
	OutboundState      map[string]outboundDelivery `json:"outbound_state,omitempty"`
	RelayForwardStates []relayForwardState         `json:"relay_forward_states,omitempty"`
}

func loadQueueState(path string) (queueStateFile, error) {
	state := queueStateFile{
		Pending:       map[string][]pendingFrame{},
		Orphaned:      map[string][]pendingFrame{},
		RelayRetry:    map[string]relayAttempt{},
		OutboundState: map[string]outboundDelivery{},
	}
	if path == "" {
		return state, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read queue state %s: %w", path, err)
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode queue state %s: %w", path, err)
	}
	if state.Pending == nil {
		state.Pending = map[string][]pendingFrame{}
	}
	if state.Orphaned == nil {
		state.Orphaned = map[string][]pendingFrame{}
	}
	if state.RelayRetry == nil {
		state.RelayRetry = map[string]relayAttempt{}
	}
	if state.OutboundState == nil {
		state.OutboundState = map[string]outboundDelivery{}
	}
	return state, nil
}

// saveQueueState atomically writes the queue state to disk. It marshals the
// state to a temporary file in the same directory, then renames it over the
// target path. Rename on the same filesystem is atomic on POSIX, guaranteeing
// that concurrent readers (or a crash mid-write) never observe a partial or
// corrupted JSON file. Without atomic writes, overlapping persistQueueState
// calls (e.g. persistRelayState + emitDeliveryReceipt) can interleave
// os.WriteFile truncate-and-write operations, producing malformed JSON.
func saveQueueState(path string, state queueStateFile) error {
	if path == "" {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create queue state directory: %w", err)
	}

	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal queue state: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".queue-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp queue state file: %w", err)
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp queue state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp queue state: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename queue state: %w", err)
	}
	return nil
}
