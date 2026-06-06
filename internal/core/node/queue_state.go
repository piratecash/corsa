package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// queueStateMarshalBufPool recycles the scratch buffer used to marshal the
// queue-state file.  json.Marshal allocates a fresh, repeatedly-grown []byte
// on every call; under sustained route churn this file is re-marshalled
// often over a large pending queue, and profiling attributed a multi-GB
// slice of cumulative alloc_space to bytes.growSlice on this exact path.
// A pooled, reused buffer fed to json.Encoder amortises that growth across
// writes, so steady-state marshals reuse already-sized backing storage and
// stop churning the allocator / GC.  The pool is concurrency-safe, so the
// rare overlap of a loop write with a FlushSync write each get their own
// buffer.
var queueStateMarshalBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// queueStateBufRetainCap is the upper bound on a buffer's capacity that we
// return to the pool.  A one-off enormous queue would otherwise pin its
// oversized backing array in the pool indefinitely; buffers grown past this
// are dropped for the GC to reclaim, while typical-sized ones are retained.
//
// Sizing: the whole point of the pool is to STOP re-growing the buffer on
// every write, so the retain cap must sit above the real steady-state
// payload — if it is below, the pool drops the buffer each time and we are
// back to per-write bytes.growSlice (the cap would then only trade
// allocation for the MinWriteInterval frequency cut, not eliminate it).
// Production inuse_space profiling showed a ~24 MiB bytes.growSlice live on
// this path, so an 8 MiB cap would have dropped the buffer every write on
// those nodes.  64 MiB covers the observed payload with headroom while still
// reclaiming a pathological one-off (a queue that momentarily ballooned far
// past normal).  sync.Pool entries are GC-collectable under memory pressure,
// so retaining a 64 MiB buffer does not permanently pin it.  If a node's
// steady-state queue legitimately grows past this, revisit with an adaptive
// retain (track a payload high-water mark) rather than a larger constant.
const queueStateBufRetainCap = 64 << 20 // 64 MiB

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

	// Compact encode (NOT MarshalIndent): the queue-state file is
	// machine-read on restart, never by a human, so pretty-printing is
	// pure overhead. Under sustained route churn this file is re-written
	// often over a potentially large pending queue; MarshalIndent's
	// whitespace roughly doubles both the allocation and the bytes
	// written each time (profiling flagged this path at multi-GB
	// cumulative alloc_space).
	//
	// Marshal through a pooled, reused buffer rather than json.Marshal so
	// the per-write []byte growth is amortised instead of re-allocated
	// every call (see queueStateMarshalBufPool). json.Encoder.Encode
	// appends a single trailing newline, which the json.Unmarshal reader
	// in loadQueueState ignores as trailing whitespace — the output stays
	// byte-for-byte loadable.
	buf := queueStateMarshalBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		// Drop pathologically large buffers instead of pinning them in the
		// pool; retain typical-sized ones for reuse.
		if buf.Cap() <= queueStateBufRetainCap {
			queueStateMarshalBufPool.Put(buf)
		}
	}()
	if err := json.NewEncoder(buf).Encode(state); err != nil {
		return wrapQueuePersistErr("marshal queue state", err)
	}
	payload := buf.Bytes()

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
