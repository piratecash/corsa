package node

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestHotPathMutationDoesNotBlockOnDiskSave is the headline regression test
// for the reconnect-storm fetch_network_stats stall.  Before the async
// persister landed, every hot-path mutation (trackRelayMessage, queuePeerFrame,
// etc.) took s.peerMu.Lock, called queueStateSnapshotLocked *under the lock*, and
// then wrote the snapshot to disk on the calling goroutine.  During a
// reconnect storm (72 pending addresses, 100+ concurrent mutations) this
// starved RLock-readers for seconds on Docker-constrained GOMAXPROCS because
// Go's RWMutex uses writer-preference: a queued writer blocks new readers.
//
// The new contract is: mutation sites must finish promptly regardless of how
// slow saveQueueState is, because they only MarkDirty the persister — the
// expensive snapshot+write happens in a dedicated background goroutine under
// RLock.
//
// The test pins the Save implementation to block for a full second.  If any
// mutation ends up synchronously driving Save, the test takes >= 1s and
// fails; with the correct wiring it completes in milliseconds.
func TestHotPathMutationDoesNotBlockOnDiskSave(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Replace the default persister with one whose Save blocks on a gate
	// channel.  The gate is opened only after the mutation burst completes,
	// proving the mutations never waited on Save.
	saveGate := make(chan struct{})
	var saveCount atomic.Int32
	svc.queuePersist = newQueueStatePersister(queueStatePersisterDeps{
		Path:             "test-regression.json",
		DebounceInterval: 1 * time.Millisecond,
		Snapshot: func() queueStateFile {
			// queueStateSnapshotLocked now reads under deliveryMu (pending,
			// orphaned, relayRetry, receipts, outbound all live in the
			// delivery domain after the Phase 2 split).
			svc.deliveryMu.RLock()
			defer svc.deliveryMu.RUnlock()
			return svc.queueStateSnapshotLocked()
		},
		Save: func(path string, state queueStateFile) error {
			saveCount.Add(1)
			<-saveGate
			return nil
		},
		Wait: realQueueStatePersistWait,
	})

	// Start the persister goroutine so MarkDirty has a consumer.  Cancelled
	// before Cleanup drains the background waitgroup.
	persisterCtx, persisterCancel := context.WithCancel(context.Background())
	persisterDone := make(chan struct{})
	go func() {
		defer close(persisterDone)
		svc.queuePersist.Run(persisterCtx)
	}()
	t.Cleanup(func() {
		close(saveGate) // let any in-flight Save return.
		persisterCancel()
		<-persisterDone
	})

	// Burst 100 concurrent trackRelayMessage calls.  Each mutation takes
	// s.peerMu.Lock, mutates s.relayRetry, Unlocks, and MarkDirty's.  If the
	// old synchronous persist code path were still here, every mutation
	// would block on Save and the burst would take >= 1s.
	const bursts = 100
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(bursts)
	for i := 0; i < bursts; i++ {
		go func(idx int) {
			defer wg.Done()
			svc.trackRelayMessage(protocol.Envelope{
				ID:        protocol.MessageID("regression-msg-" + strconv.Itoa(idx)),
				Topic:     "dm",
				Recipient: "recipient-" + strconv.Itoa(idx),
			})
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 100 sequential synchronous writes would take at least 1s (the Save
	// gate never fires during the burst).  Any result below 200ms proves
	// the writes are detached from the hot path.  The threshold is
	// intentionally loose so that a slow CI box with GOMAXPROCS=1 doesn't
	// flap without masking the regression it guards against.
	const maxTolerated = 200 * time.Millisecond
	if elapsed > maxTolerated {
		t.Fatalf("hot-path mutation burst took %s; expected < %s (sync disk I/O regressed)", elapsed, maxTolerated)
	}

	// Sanity: Save may or may not have been entered yet depending on the
	// debounce window vs. test scheduling, but it must not have returned
	// because the gate is still closed.  If saveCount > 1 before the gate
	// opens, the persister ran multiple concurrent writes, violating the
	// single-writer contract.
	if got := saveCount.Load(); got > 1 {
		t.Fatalf("persister issued %d concurrent Save calls; expected at most 1", got)
	}
}

// TestReaderNotStarvedDuringMutationBurst verifies the specific RLock
// starvation scenario that caused the 7-second fetch_network_stats stall.
// A stream of hot-path mutations (each one taking s.peerMu.Lock briefly)
// concurrently with a reader that takes s.peerMu.RLock.  Under the old
// synchronous-persist code path, the Lock hold time included the snapshot
// deep-copy of every map (pending, orphaned, relayRetry, relayMessages,
// relayReceipts, outbound, relayStates), which under writer-preference
// semantics would block readers for hundreds of ms.  Under the new
// contract the Lock hold time is just the mutation itself.
//
// The assertion is on the MAX reader latency, not the average: a single
// spike >= 500ms is enough to reproduce the UI stall.
func TestReaderNotStarvedDuringMutationBurst(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Seed a realistically sized queue so queueStateSnapshotLocked would
	// have been expensive under the old code.  s.relayRetry lives under
	// s.deliveryMu after the Phase 2 step 5 split, not s.peerMu.
	svc.deliveryMu.Lock()
	for i := 0; i < 72; i++ {
		svc.relayRetry[relayMessageKey(protocol.MessageID("seed-"+strconv.Itoa(i)))] = relayAttempt{
			FirstSeen: time.Now().UTC(),
		}
	}
	svc.deliveryMu.Unlock()

	// The persister runs with a no-op Save so we isolate the reader-vs-mutation
	// contention from disk I/O entirely — the test only measures s.deliveryMu
	// latency (the snapshot closure takes that lock, not s.peerMu).
	svc.queuePersist = newQueueStatePersister(queueStatePersisterDeps{
		Path:             "test-reader-regression.json",
		DebounceInterval: 5 * time.Millisecond,
		Snapshot: func() queueStateFile {
			// queueStateSnapshotLocked now reads under deliveryMu (pending,
			// orphaned, relayRetry, receipts, outbound all live in the
			// delivery domain after the Phase 2 split).
			svc.deliveryMu.RLock()
			defer svc.deliveryMu.RUnlock()
			return svc.queueStateSnapshotLocked()
		},
		Save: func(path string, state queueStateFile) error { return nil },
		Wait: realQueueStatePersistWait,
	})
	persisterCtx, persisterCancel := context.WithCancel(context.Background())
	persisterDone := make(chan struct{})
	go func() {
		defer close(persisterDone)
		svc.queuePersist.Run(persisterCtx)
	}()
	t.Cleanup(func() {
		persisterCancel()
		<-persisterDone
	})

	// Launch two pressures:
	//   1. A continuous writer burst hammering s.peerMu.Lock via trackRelayMessage.
	//   2. A reader calling SubscriberCount (cheap RLock reader) and
	//      measuring its own acquisition + body latency.
	stop := make(chan struct{})

	var writerWg sync.WaitGroup
	writerWg.Add(8)
	for w := 0; w < 8; w++ {
		go func(workerID int) {
			defer writerWg.Done()
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				svc.trackRelayMessage(protocol.Envelope{
					ID:        protocol.MessageID("burst-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(seq)),
					Topic:     "dm",
					Recipient: "recipient",
				})
				seq++
			}
		}(w)
	}

	var readerMaxNs atomic.Int64
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for i := 0; i < 100; i++ {
			select {
			case <-stop:
				return
			default:
			}
			readerStart := time.Now()
			_ = svc.SubscriberCount("recipient")
			elapsed := time.Since(readerStart).Nanoseconds()
			for {
				prev := readerMaxNs.Load()
				if elapsed <= prev {
					break
				}
				if readerMaxNs.CompareAndSwap(prev, elapsed) {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	readerWg.Wait()
	close(stop)
	writerWg.Wait()

	maxReaderLatency := time.Duration(readerMaxNs.Load())
	// 200ms is the same threshold the prior-art stall exceeded by an
	// order of magnitude (7s).  The test catches a regression back to
	// snapshot-under-Lock semantics without being tight enough to flap
	// on slow CI hardware.
	const maxTolerated = 200 * time.Millisecond
	if maxReaderLatency > maxTolerated {
		t.Fatalf("reader latency spike %s exceeded tolerance %s during mutation burst", maxReaderLatency, maxTolerated)
	}
}

// TestQueueStatePersisterNilSafeOnHotPath pins the nil-safety contract of
// MarkDirty and FlushSync.  Production Service instances wire a persister
// in NewService, but the test suite has a large fleet of struct-literal
// *Service fixtures that intentionally bypass NewService — and any of them
// that exercises a hot-path mutation site (relay, routing, peer lifecycle)
// transitively calls svc.queuePersist.MarkDirty.  A refactor that makes
// MarkDirty panic on nil receiver would crash a broad swath of otherwise
// well-scoped tests (e.g. TestTTLExpiryExposesBackupAndTriggersDrain).
//
// This test asserts the contract directly so the nil-safe guard does not
// silently regress.
func TestQueueStatePersisterNilSafeOnHotPath(t *testing.T) {
	t.Parallel()

	var p *queueStatePersister // nil on purpose.

	// Both methods must be callable without panic when the pointer is nil;
	// no exceptional control flow, no channel traffic, just a no-op.
	p.MarkDirty()
	p.FlushSync()
}
