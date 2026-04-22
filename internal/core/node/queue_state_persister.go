package node

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// defaultQueueStatePersistDebounce is the default quiet window the
// background persister waits after the first MarkDirty before snapshotting
// and writing to disk.  Subsequent MarkDirty calls that land inside the
// window are absorbed into the same write.  The value trades durability
// latency against write amplification — small enough that a reconnect storm
// driving tens of mutations collapses into one or two writes, large enough
// that interactive UI state changes don't appear lossy.
const defaultQueueStatePersistDebounce = 200 * time.Millisecond

// queueStatePersisterDeps bundles the collaborators required by
// queueStatePersister.  The single opts struct is the contract between the
// persister and whatever owns the on-disk file and the in-memory queue
// state.  Tests supply fake implementations; NewService wires the real
// Service methods.  The flag list is explicit at the construction site so
// Go's field-less struct literal cannot silently forget one of them.
type queueStatePersisterDeps struct {
	// Path is the on-disk queue-state file location.  An empty string is
	// valid and makes Save a no-op at the saveQueueState layer; the
	// persister does not treat it specially.
	Path string
	// DebounceInterval is the quiet window observed after a wake
	// notification before the persister takes the snapshot and calls
	// Save.  Zero collapses into no debouncing and is allowed but
	// discouraged in production.
	DebounceInterval time.Duration
	// Snapshot produces an immutable copy of the current queue state.
	// Implementations must acquire any in-memory lock themselves (the
	// Service wiring takes s.peerMu.RLock so UI-tickers and other readers
	// continue to run in parallel with persistence).  The persister
	// never holds a lock when it calls Snapshot.
	Snapshot func() queueStateFile
	// Save writes the snapshot to disk.  Returning an error only
	// triggers an error log and re-arms the dirty flag; the persister
	// does not retry on its own clock — the dirty flag is the
	// authoritative source of "there is something to save".
	Save func(path string, state queueStateFile) error
	// Wait blocks until d has elapsed or ctx is cancelled, whichever is
	// first.  Returns true when the debounce window elapsed cleanly
	// and false when ctx was cancelled mid-wait.  Pulled out so tests
	// drive the persister without wall-clock races.
	Wait func(ctx context.Context, d time.Duration) bool
}

// queueStatePersister owns the background writer for the on-disk queue
// state file.  Every mutation site in the Service must call MarkDirty
// instead of taking a snapshot and calling saveQueueState inline so the
// hot path never blocks on disk I/O or on competing writers of s.peerMu.
//
// Concurrency contract:
//   - MarkDirty is safe from any goroutine and never blocks.
//   - Run executes exactly once per persister lifetime and owns every
//     Snapshot / Save call under normal operation.
//   - FlushSync is a synchronous escape hatch for tests and for the
//     graceful-shutdown path that needs the last snapshot durable before
//     a process exits.
type queueStatePersister struct {
	deps   queueStatePersisterDeps
	dirty  atomic.Int32
	wakeCh chan struct{}
	flushM sync.Mutex // serialises FlushSync against itself and the Run writer.
}

func newQueueStatePersister(deps queueStatePersisterDeps) *queueStatePersister {
	return &queueStatePersister{
		deps:   deps,
		wakeCh: make(chan struct{}, 1),
	}
}

// MarkDirty signals that in-memory queue state has changed and should be
// persisted.  Callers may invoke it while holding s.peerMu.Lock — the call
// neither allocates nor touches the lock.  Multiple calls between two Run
// iterations coalesce into a single write.
//
// Nil-safe on purpose: the call site is a hot-path mutation (relay,
// routing, peer lifecycle) that fires from dozens of places.  Production
// Service instances are always constructed through NewService which wires
// a real persister, but the test suite has a large fleet of
// struct-literal *Service fixtures that intentionally bypass NewService
// to isolate individual behaviours.  Those fixtures should not have to
// know every transitive MarkDirty site, and panicking there would
// conflate "persistence not wired" with "real bug".  When p is nil we
// silently drop — no in-memory state is lost because the fixtures do not
// observe the on-disk file; any test that does observe it wires a
// persister explicitly (see persist_regression_test.go and the service
// path in newTestService).
func (p *queueStatePersister) MarkDirty() {
	if p == nil {
		return
	}
	p.dirty.Store(1)
	select {
	case p.wakeCh <- struct{}{}:
	default:
		// Wake is already queued; MarkDirty is idempotent.
	}
}

// Run drives the writer loop until ctx is cancelled.  On cancellation the
// loop performs one final flush when dirty so no committed in-memory
// state is lost on graceful shutdown.  Exactly one goroutine must call
// Run per persister.
func (p *queueStatePersister) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.finalFlush()
			return
		case <-p.wakeCh:
		}

		// Debounce: let additional MarkDirty calls pile up into the same
		// write window.  Wait returns false when ctx was cancelled mid
		// debounce — flush whatever is buffered and exit so the caller
		// observes clean shutdown semantics.
		if !p.deps.Wait(ctx, p.deps.DebounceInterval) {
			p.finalFlush()
			return
		}

		p.flushFromLoop()
	}
}

// flushFromLoop performs one dirty-guarded snapshot+save.  The dirty flag
// is cleared before the snapshot so a MarkDirty racing with the write is
// captured by the next iteration rather than being lost.
func (p *queueStatePersister) flushFromLoop() {
	p.flushM.Lock()
	defer p.flushM.Unlock()

	if p.dirty.Swap(0) == 0 {
		return
	}
	snapshot := p.deps.Snapshot()
	if err := p.deps.Save(p.deps.Path, snapshot); err != nil {
		log.Error().
			Str("path", p.deps.Path).
			Err(err).
			Msg("queue_state_persist_failed")
		// Re-arm dirty and wake so the next loop iteration retries the
		// write.  Without the wake re-arm the channel would stay empty
		// and Run would block until some unrelated MarkDirty arrives.
		p.dirty.Store(1)
		select {
		case p.wakeCh <- struct{}{}:
		default:
		}
	}
}

// finalFlush writes the outstanding dirty snapshot without debouncing.
// Called from Run on ctx cancellation and indirectly from FlushSync when
// a test or shutdown path needs the disk file current before returning.
func (p *queueStatePersister) finalFlush() {
	p.flushM.Lock()
	defer p.flushM.Unlock()

	if p.dirty.Swap(0) == 0 {
		return
	}
	snapshot := p.deps.Snapshot()
	if err := p.deps.Save(p.deps.Path, snapshot); err != nil {
		log.Error().
			Str("path", p.deps.Path).
			Err(err).
			Msg("queue_state_final_flush_failed")
		// Re-arm so a caller inspecting dirty after shutdown observes
		// that the state did not reach disk.  Production callers do
		// not inspect it, but tests do.
		p.dirty.Store(1)
	}
}

// FlushSync is a synchronous flush: if dirty, takes a snapshot and writes
// to disk on the calling goroutine.  Use only from the graceful-shutdown
// path (the caller must have already cancelled Run's context and waited
// for Run to return) and from tests that need to read the queue-state
// file back after asserting in-memory state.
//
// Nil-safe for symmetry with MarkDirty: the graceful-shutdown sequence in
// Run calls FlushSync through s.queuePersist, which struct-literal test
// fixtures may leave unset.
func (p *queueStatePersister) FlushSync() {
	if p == nil {
		return
	}
	p.finalFlush()
}

// realQueueStatePersistWait is the production implementation of Wait: a
// select on ctx.Done vs. time.After.  Kept out of queueStatePersisterDeps
// as a named function so NewService wiring is a clear reference rather
// than an anonymous closure at the call site.
func realQueueStatePersistWait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
