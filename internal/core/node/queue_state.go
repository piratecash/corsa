package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// ErrQueuePersistFailed is the sentinel wrapped around every non-nil error
// returned from saveQueueState.  Callers must use errors.Is(err,
// ErrQueuePersistFailed) to distinguish queue-persistence failures from
// other errors rather than comparing the string message.  Keeping the
// sentinel stable makes the contract visible at the type level: the
// queueStatePersister's retry decision and the graceful-shutdown error
// reporting both depend on this marker.
var ErrQueuePersistFailed = errors.New("queue state persist failed")

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
// corrupted JSON file. The queueStatePersister serialises the writer side via
// flushM so concurrent Save invocations cannot happen from the persister, but
// FlushSync from a graceful-shutdown path and a racing Run writer on another
// persister instance still need atomicity — the rename guarantees it.
func saveQueueState(path string, state queueStateFile) error {
	if path == "" {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return wrapQueuePersistErr("create queue state directory", err)
	}

	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return wrapQueuePersistErr("marshal queue state", err)
	}

	tmp, err := os.CreateTemp(dir, ".queue-state-*.tmp")
	if err != nil {
		return wrapQueuePersistErr("create temp queue state file", err)
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return wrapQueuePersistErr("write temp queue state", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return wrapQueuePersistErr("close temp queue state", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return wrapQueuePersistErr("rename queue state", err)
	}
	return nil
}

// wrapQueuePersistErr joins ErrQueuePersistFailed with the underlying OS or
// encoding error so callers can do both: errors.Is(err, ErrQueuePersistFailed)
// to recognise the failure category, and errors.Unwrap / errors.As to reach
// the original cause for structured logging.  errors.Join guarantees both
// targets remain reachable without sacrificing the human-readable message.
func wrapQueuePersistErr(stage string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrQueuePersistFailed, stage, cause)
}
