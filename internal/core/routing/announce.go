package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
)

const (
	// DefaultAnnounceInterval is the periodic announcement interval.
	// Every 30 seconds, the node sends its routing table to all peers
	// that support mesh_routing_v1.
	DefaultAnnounceInterval = 30 * time.Second

	// ForcedFullSyncMultiplier controls how often a forced full sync is
	// sent to each peer: every ForcedFullSyncMultiplier * DefaultAnnounceInterval.
	// At 30s interval this means a full sync roughly every 5 minutes.
	ForcedFullSyncMultiplier = 10

	// MinForcedFullSyncInterval is the minimum time between forced full
	// sync attempts for a single peer. Prevents flood when a peer is
	// repeatedly marked NeedsFullResync.
	MinForcedFullSyncInterval = DefaultAnnounceInterval
)

// PeerSender abstracts the ability to send routing announcement frames
// to a specific peer. node.Service implements this interface to decouple
// the routing package from the network layer.
//
// Two wire frames are carried by two distinct methods so that the choice
// of wire format lives on the call site and cannot silently flip inside
// the implementation. The v1 loop uses only SendAnnounceRoutes;
// SendRoutesUpdate is wired as a scaffold for a future capability-gated
// v2 incremental path. Until that work lands, the invariant "connect-time
// sync and forced full sync always use SendAnnounceRoutes" is enforced by
// the call sites in this file (sendFullAnnounce / sendIncrementalAnnounce)
// and by the node-side connect path; see docs/routing.md for the durable
// announce-plane file map.
type PeerSender interface {
	// SendAnnounceRoutes sends a list of AnnounceEntry items as a legacy
	// announce_routes frame to the peer identified by peerAddress.
	//
	// This is the legacy v1 path. It is used for:
	//   - connect-time full sync (always, regardless of any future v2
	//     capability — initial sync after session establishment is always
	//     the legacy frame so mixed-version networks never see an
	//     unexpected routes_update);
	//   - periodic forced full sync in the announce loop;
	//   - delta updates for peers that have not negotiated the v2
	//     mesh_routing capability.
	//
	// ctx is the caller's request/cycle context: a pre-cancelled ctx
	// fails fast without touching the transport, and a mid-flight cancel
	// during the inbound sync-flush wait aborts the send rather than
	// consuming the full syncFlushTimeout. This is the contract that
	// makes fanoutAnnounceRoutes cancellable end-to-end — a stuck
	// inbound hairpin socket no longer pins the caller for 5 s once
	// its cycle/shutdown context has cancelled.
	//
	// peerAddress is the transport address used by node.Service to
	// locate the session. Returns true if the frame was enqueued
	// successfully. A false return collapses ctx-cancel, ctx-deadline,
	// transport timeout, writer-done, buffer-full and unregistered-conn
	// into one negative outcome; the PeerSender implementation handles
	// observability internally.
	SendAnnounceRoutes(ctx context.Context, peerAddress PeerAddress, routes []AnnounceEntry) bool

	// SendRoutesUpdate sends a v2 routes_update frame carrying an
	// incremental delta to the peer identified by peerAddress.
	//
	// This method is scaffolding for the v2 capability-gated incremental
	// path. In v1 it MUST NOT be called by the announce loop: the v1
	// loop always uses SendAnnounceRoutes for both full sync and delta
	// to preserve mixed-version compatibility on the wire.
	//
	// Contract for future v2 callers (not enforced in v1 code paths):
	//   - only peers that negotiated the v2 mesh_routing capability
	//     receive routes_update;
	//   - SendRoutesUpdate MUST NOT be used for the first sync after
	//     session establishment — initial sync is always legacy
	//     announce_routes;
	//   - SendRoutesUpdate MUST NOT be used for forced full resync —
	//     forced full also goes through SendAnnounceRoutes.
	//
	// Until the v2 wire frame is implemented, implementations return
	// false and log a single warn per peer session so accidental call
	// sites are visible without flooding the log. Semantics of ctx,
	// peerAddress, and the bool return value match SendAnnounceRoutes.
	SendRoutesUpdate(ctx context.Context, peerAddress PeerAddress, delta []AnnounceEntry) bool
}

// AnnounceLoop runs periodic and triggered routing announcements. It
// owns a background goroutine that wakes every DefaultAnnounceInterval
// and sends the local routing table to all capable peers. Triggered
// updates (connect, disconnect) bypass the timer and send immediately.
//
// The loop uses per-peer announce state (via AnnounceStateRegistry) to:
//   - aggregate raw table entries into canonical peer-specific snapshots;
//   - compare each snapshot with the previously sent one;
//   - send only changed entries (delta) to peers that already received
//     a full sync;
//   - suppress no-op sends when the snapshot is unchanged;
//   - force periodic full sync at configurable intervals.
//
// The loop is designed to be started once from node.Service.Run and
// stopped on context cancellation.
type AnnounceLoop struct {
	table    *Table
	sender   PeerSender
	interval time.Duration

	// triggerCh receives signals to send an immediate update.
	// Buffered to 1 so multiple rapid triggers coalesce.
	triggerCh chan struct{}

	// peersFn returns the current list of peers that support
	// mesh_routing_v1. Each entry is (transport_address, identity).
	peersFn func() []AnnounceTarget

	// stateRegistry manages per-peer announce send state. Owned by the
	// AnnounceLoop but architecturally belongs to the service layer.
	stateRegistry *AnnounceStateRegistry

	mu      sync.Mutex
	running bool

	// cycleCounter provides a monotonic announce_cycle_id for log correlation.
	cycleCounter atomic.Uint64
}

// AnnounceTarget identifies a peer for announcement purposes.
//
// Capabilities is an immutable per-cycle snapshot of the peer's negotiated
// capability set, captured by the peersFn implementation under the same
// peer-state lock that produced Address and Identity. The snapshot is taken
// at cycle start precisely so that per-peer goroutines inside
// announceToAllPeers can decide on a wire format (legacy announce_routes vs
// future v2 routes_update) without re-entering the Service's peer mutex per
// peer — that re-entry pattern collides with writer-preferring sync.RWMutex
// semantics and has been observed to starve reads under load.
//
// AnnounceLoop and any downstream consumer MUST treat Capabilities as
// read-only; producers build a fresh slice per target so mutation here cannot
// corrupt session state. Consumers that need a specific capability check
// should range over the slice directly rather than re-fetching capabilities
// from the Service. Until routing-announce v2 lands, the announce loop does
// not branch on Capabilities at all: the field is plumbed so that v2 can be
// added by a single call-site change in announceToAllPeers without touching
// the peersFn contract again.
type AnnounceTarget struct {
	// Address is the transport address used to enqueue frames.
	Address PeerAddress
	// Identity is the peer's Ed25519 fingerprint — used for split horizon.
	Identity PeerIdentity
	// Capabilities is the peer's negotiated capability snapshot for this
	// announce cycle. See the type doc for ownership and mutation rules.
	Capabilities []PeerCapability
}

// AnnounceLoopOption configures the AnnounceLoop.
type AnnounceLoopOption func(*AnnounceLoop)

// WithAnnounceInterval overrides the default periodic interval.
func WithAnnounceInterval(d time.Duration) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.interval = d
	}
}

// WithStateRegistry injects an existing state registry. When not set,
// a default registry is created.
func WithStateRegistry(r *AnnounceStateRegistry) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.stateRegistry = r
	}
}

// NewAnnounceLoop creates a new loop. peersFn is called on every tick to
// discover which peers should receive announcements.
func NewAnnounceLoop(
	table *Table,
	sender PeerSender,
	peersFn func() []AnnounceTarget,
	opts ...AnnounceLoopOption,
) *AnnounceLoop {
	a := &AnnounceLoop{
		table:     table,
		sender:    sender,
		interval:  DefaultAnnounceInterval,
		triggerCh: make(chan struct{}, 1),
		peersFn:   peersFn,
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.stateRegistry == nil {
		a.stateRegistry = NewAnnounceStateRegistry()
	}
	return a
}

// StateRegistry returns the per-peer announce state registry. Used by
// node.Service to integrate session lifecycle events (connect, disconnect)
// with the announce state.
func (a *AnnounceLoop) StateRegistry() *AnnounceStateRegistry {
	return a.stateRegistry
}

// Run starts the periodic announce loop. It blocks until ctx is cancelled.
func (a *AnnounceLoop) Run(ctx context.Context) {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
	}()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.announceToAllPeers(ctx)
		case <-a.triggerCh:
			a.announceToAllPeers(ctx)
			// Reset the ticker so we don't double-announce shortly after
			// a triggered update.
			ticker.Reset(a.interval)
		}
	}
}

// TriggerUpdate requests an immediate announcement cycle. Safe to call
// from any goroutine. Multiple rapid calls coalesce into a single cycle.
func (a *AnnounceLoop) TriggerUpdate() {
	select {
	case a.triggerCh <- struct{}{}:
	default:
		// Already pending — coalesce.
	}
}

// PendingTrigger reports whether a triggered update is queued but not yet
// consumed by the announce loop. Intended for unit tests that verify
// TriggerUpdate was called without running the full loop.
func (a *AnnounceLoop) PendingTrigger() bool {
	select {
	case <-a.triggerCh:
		// Was pending — put it back so the loop still sees it.
		a.triggerCh <- struct{}{}
		return true
	default:
		return false
	}
}

// announceToAllPeers sends the routing table to every capable peer,
// applying split horizon per peer and per-peer delta/cache logic.
//
// Per-peer work runs in its own goroutine so that a single stuck inbound
// socket — bounded per peer by syncFlushTimeout inside
// sender.SendAnnounceRoutes — cannot serialise delivery to the rest. The
// wall-clock of a cycle is the slowest peer, not N × slowest. AnnouncePeerState
// is thread-safe (embedded mutex) and each goroutine operates on its own
// peer's state, so there is no cross-peer contention. Cycle-level counters
// become atomic; the summary log waits for every goroutine to finish.
func (a *AnnounceLoop) announceToAllPeers(ctx context.Context) {
	cycleID := a.cycleCounter.Add(1)

	// Refresh TTL of own-origin direct routes unconditionally.
	a.table.RefreshDirectPeers()

	peers := a.peersFn()
	if len(peers) == 0 {
		return
	}

	// Periodic eviction of stale disconnected peer state.
	a.stateRegistry.EvictStale()

	var (
		totalRaw         atomic.Int32
		totalAggregated  atomic.Int32
		totalDelta       atomic.Int32
		skippedNoop      atomic.Int32
		forcedFull       atomic.Int32
		coalescedTrigger atomic.Int32
	)

	now := a.stateRegistry.clock()
	forcedFullSyncInterval := time.Duration(ForcedFullSyncMultiplier) * a.interval

	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, peer := range peers {
		go func(peer AnnounceTarget) {
			defer wg.Done()
			defer crashlog.DeferRecover()

			// Early-abort when the cycle context is cancelled — avoids
			// blocking a per-peer goroutine for up to syncFlushTimeout
			// when shutdown is already in progress.
			if ctx.Err() != nil {
				return
			}

			peerState := a.stateRegistry.GetOrCreate(peer.Identity)

			// Build peer-specific raw entries from table.
			rawRoutes := a.table.AnnounceTo(peer.Identity)
			totalRaw.Add(int32(len(rawRoutes)))

			// Build canonical aggregated snapshot.
			snapshot := BuildAnnounceSnapshot(rawRoutes)
			totalAggregated.Add(int32(len(snapshot.Entries)))

			// Read peer state atomically to decide send mode.
			view := peerState.View()
			needsFull := view.NeedsFullResync || view.LastSentSnapshot == nil

			// Check if periodic forced full sync is due.
			if !needsFull && !view.LastSuccessfulFullSyncAt.IsZero() {
				if now.Sub(view.LastSuccessfulFullSyncAt) > forcedFullSyncInterval {
					needsFull = true
				}
			}

			if needsFull {
				// Rate limit forced full sync attempts — but only when the
				// peer already has a baseline. A peer that never received
				// any data (LastSentSnapshot==nil, e.g. after a failed
				// first attempt) must retry without delay.
				if view.LastSentSnapshot != nil &&
					!view.LastFullSyncAttemptAt.IsZero() &&
					now.Sub(view.LastFullSyncAttemptAt) < MinForcedFullSyncInterval {
					// Too soon — skip this cycle for this peer.
					coalescedTrigger.Add(1)
					return
				}

				a.sendFullAnnounce(ctx, cycleID, peer, peerState, snapshot, now)
				forcedFull.Add(1)
				return
			}

			// Delta path.
			delta := ComputeDelta(view.LastSentSnapshot, snapshot)
			totalDelta.Add(int32(len(delta)))

			if len(delta) == 0 {
				// No changes — suppress send.
				skippedNoop.Add(1)
				return
			}

			a.sendIncrementalAnnounce(ctx, cycleID, peer, peerState, snapshot, delta, now)
		}(peer)
	}
	wg.Wait()

	log.Debug().
		Uint64("announce_cycle_id", cycleID).
		Int("peers", len(peers)).
		Int("raw_routes_count", int(totalRaw.Load())).
		Int("aggregated_routes_count", int(totalAggregated.Load())).
		Int("delta_routes_count", int(totalDelta.Load())).
		Int("announce_skipped_noop", int(skippedNoop.Load())).
		Int("announce_forced_full", int(forcedFull.Load())).
		Int("announce_trigger_coalesced", int(coalescedTrigger.Load())).
		Msg("announce_cycle_complete")
}

// sendFullAnnounce sends a complete announce snapshot to the peer and
// updates cache state on success via thread-safe Record* methods.
func (a *AnnounceLoop) sendFullAnnounce(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	snapshot *AnnounceSnapshot,
	now time.Time,
) {
	state.RecordFullSyncAttempt(now)

	// Empty snapshot after split horizon / empty table: record a successful
	// full-sync baseline without sending a wire frame. The peer learns
	// nothing new, and the cache is primed so subsequent cycles use delta.
	if len(snapshot.Entries) == 0 {
		state.RecordFullSyncSuccess(snapshot, now)
		return
	}

	if !a.sender.SendAnnounceRoutes(ctx, peer.Address, snapshot.Entries) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("routes", len(snapshot.Entries)).
			Msg("announce_full_send_failed")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	state.RecordFullSyncSuccess(snapshot, now)
}

// sendIncrementalAnnounce sends only changed entries to the peer and
// updates cache to the full new snapshot on success via thread-safe
// Record* methods.
func (a *AnnounceLoop) sendIncrementalAnnounce(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	fullSnapshot *AnnounceSnapshot,
	delta []AnnounceEntry,
	now time.Time,
) {
	if !a.sender.SendAnnounceRoutes(ctx, peer.Address, delta) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("delta_routes", len(delta)).
			Msg("announce_delta_send_failed")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	// Cache stores the full snapshot, not just the delta payload.
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}
