package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	// DefaultAnnounceInterval is the periodic announcement interval.
	// Every 30 seconds, the node sends its routing table to all peers
	// that support mesh_routing_v1.
	DefaultAnnounceInterval = 30 * time.Second

	// ForcedFullSyncMultiplier controls how often a forced full sync is
	// sent to each peer: every ForcedFullSyncMultiplier * AnnounceInterval,
	// capped by DefaultTTL/2 (see effectiveForcedFullSyncInterval).
	// At the default 30s interval this means a full sync every 60 seconds,
	// which is exactly DefaultTTL/2 — the upper bound that keeps learned
	// routes on neighbors alive between cycles even on a single dropped
	// sync. The invariant
	//
	//   effective forced-full-sync interval <= DefaultTTL / 2
	//
	// is enforced by capping the computed value at DefaultTTL/2; if
	// ForcedFullSyncMultiplier or AnnounceInterval are configured such
	// that their product exceeds DefaultTTL/2, the cap kicks in and
	// forced syncs continue to fire at TTL/2 cadence. The cap means
	// raising AnnounceInterval (e.g. for dense meshes via
	// CORSA_ANNOUNCE_INTERVAL_SECONDS) cannot push forced syncs past
	// the freshness boundary; in that mode delta cycles are suppressed
	// because each tick lands at or after the forced-full-sync deadline.
	// See docs/routing.md "Refresh interval invariant" for the full
	// contract.
	ForcedFullSyncMultiplier = 2

	// MinForcedFullSyncInterval is the minimum time between forced full
	// sync attempts for a single peer. Prevents flood when a peer is
	// repeatedly marked NeedsFullResync.
	MinForcedFullSyncInterval = DefaultAnnounceInterval
)

// EffectiveForcedFullSyncInterval computes the forced-full-sync cadence
// from the announce interval, capped at DefaultTTL/2 so the freshness
// invariant always holds even when operators raise AnnounceInterval
// beyond the multiplier's natural reach (e.g. via
// CORSA_ANNOUNCE_INTERVAL_SECONDS in dense meshes).
//
// The cap is the safety net for the "Refresh interval invariant" in
// docs/routing.md: forced full sync MUST fire at most one DefaultTTL/2
// apart so a single dropped announce cannot leave a learned route to
// silently age out on the receiver. Pure function, safe to call from
// any goroutine; exported for monitoring code that wants to project
// next-forced-sync deadlines from the configured interval.
func EffectiveForcedFullSyncInterval(announceInterval time.Duration) time.Duration {
	candidate := time.Duration(ForcedFullSyncMultiplier) * announceInterval
	maxAllowed := DefaultTTL / 2
	if candidate > maxAllowed {
		return maxAllowed
	}
	return candidate
}

// PeerSender abstracts the ability to send routing announcement frames
// to a specific peer. node.Service implements this interface to decouple
// the routing package from the network layer.
//
// Two wire frames are carried by two distinct methods so that the choice
// of wire format lives on the call site and cannot silently flip inside
// the implementation. The call-site contract is:
//   - connect-time and forced-full paths always use SendAnnounceRoutes
//     (legacy announce_routes wire frame) — this is the "First-sync
//     wire-frame invariant" documented in docs/routing.md;
//   - the delta path chooses between SendAnnounceRoutes (peer on v1 only)
//     and SendRoutesUpdate (peer on v2) based on the peer's capability
//     snapshot captured at cycle start — see AnnounceLoop.announceToAllPeers.
//
// The invariant "the implementation never silently flips the wire format"
// is preserved: each call site decides once, and the interface provides no
// method that could accept the wrong frame. See docs/routing.md for the
// durable announce-plane file map.
type PeerSender interface {
	// SendAnnounceRoutes sends a list of AnnounceEntry items as a legacy
	// announce_routes frame to the peer identified by peerAddress.
	//
	// This is the legacy v1 wire path. It is used for:
	//   - connect-time full sync (always, regardless of v2 capability —
	//     initial sync after session establishment is always the legacy
	//     frame so mixed-version networks never see an unexpected
	//     routes_update);
	//   - periodic forced full sync in the announce loop;
	//   - delta updates for peers that have NOT negotiated the
	//     CapMeshRoutingV2 capability.
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
	// This method is the v2 capability-gated incremental path. The
	// announce loop picks SendRoutesUpdate only when all of the following
	// hold at cycle start:
	//   - the peer's negotiated capability snapshot contains both
	//     CapMeshRoutingV1 and CapMeshRoutingV2;
	//   - the cycle is a delta send (the peer already has a baseline);
	//   - the crosscheck between AnnouncePeerState.Capabilities and
	//     AnnounceTarget.Capabilities agrees (divergence forces a full
	//     resync via MarkInvalid — see announceToAllPeers).
	//
	// SendRoutesUpdate MUST NOT be used for:
	//   - the first sync after session establishment (initial sync is
	//     always legacy announce_routes — the peer has no baseline to
	//     diff against);
	//   - forced full resync (forced full is always legacy).
	//
	// Semantics of ctx, peerAddress, and the bool return value match
	// SendAnnounceRoutes.
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

	// overloadGate is the optional CPU/backlog backpressure callback.
	// When non-nil and reporting true at cycle start, delta cycles for
	// non-due peers are skipped; forced-full-sync still fires on
	// schedule so freshness invariants hold. nil disables the throttle.
	overloadGate OverloadGate

	// overloadCycles counts cycles where the overload gate actually
	// shed work — at least one peer was skipped specifically because
	// of overload. The increment fires post-wg, gated by a per-cycle
	// `suppressedAny` atomic.Bool that any per-peer goroutine sets
	// when it hits the overload-suppression early-return arm. Cycles
	// where the gate engaged but every peer was forced-due (initial
	// sync, periodic forced-full deadline) do NOT advance the
	// counter because no delta would have been sent anyway and
	// overload didn't change behaviour. Forced-only wakes (operator
	// AnnounceInterval > tick, isDeltaCycle false) are also
	// excluded for the same reason. Surfaced through
	// OverloadCycleCount() and `fetchRouteSummary.overload.engaged_cycles`.
	overloadCycles atomic.Uint64

	// noopSuppressedTotal counts per-peer delta computations that
	// reached the ComputeDelta call AND produced an empty delta — the
	// "snapshot unchanged, nothing to send" branch. This is strictly
	// the no-op DELTA branch: forced-full early-returns, !isDeltaCycle
	// early-returns, overload-gate early-returns, and v2-downgrade
	// branches do NOT advance the counter. The counter is the
	// observable signal a test needs to distinguish "follow-up
	// trigger reached the delta path and was correctly suppressed"
	// from "follow-up trigger took an early-return path and never
	// touched the delta logic" — both shapes leave the wire-call
	// count unchanged, so without this counter the no-op suppression
	// test cannot tell whether the path under test actually fired.
	// Monotonic; safe for concurrent reads.
	noopSuppressedTotal atomic.Uint64

	// lastDeltaCycleAt tracks the wall-clock of the most recent
	// delta-cycle wake. When the operator-configured AnnounceInterval
	// exceeds EffectiveForcedFullSyncInterval (capped at DefaultTTL/2),
	// the loop ticker fires more often than AnnounceInterval — every
	// tick still checks forced-full deadlines for freshness, but
	// per-peer delta sends are suppressed until at least
	// AnnounceInterval has elapsed since the previous delta cycle.
	// This is what preserves the operator's "less frequent delta
	// computation" intent while keeping the freshness invariant
	// (forced-full ≤ TTL/2). Written only by the Run goroutine via
	// announceToAllPeers; read by the per-peer goroutines spawned
	// inside that call after the value is captured into a stack
	// variable, so no concurrency.
	lastDeltaCycleAt time.Time

	mu      sync.Mutex
	running bool

	// cycleCounter provides a monotonic announce_cycle_id for log correlation.
	cycleCounter atomic.Uint64
}

// OverloadCycleCount returns the cumulative number of cycles where
// the OverloadGate actually shed work — i.e. cycles where AT LEAST
// ONE peer was skipped specifically because of overload (a delta-due
// peer that the gate suppressed). Cycles where the gate engaged but
// every peer happened to be forced-due (no delta to suppress) do
// NOT advance the counter; neither do forced-only wakes where the
// gate was true but no delta cycle was due to begin with. The
// counter is therefore a strict "gate actually saved CPU" signal,
// not a proxy for "host briefly looked overloaded".
//
// Monotonic; safe for concurrent reads. Zero when no gate is wired
// or backpressure has not engaged yet. Surfaced through
// `fetchRouteSummary.overload.engaged_cycles`.
func (a *AnnounceLoop) OverloadCycleCount() uint64 {
	return a.overloadCycles.Load()
}

// NoopSuppressedTotal returns the cumulative number of per-peer
// announce computations that reached the delta path AND produced an
// empty delta — i.e. the snapshot was unchanged from the last sent
// state and the wire send was correctly suppressed. The counter does
// NOT advance for early-return paths (forced-full short-circuit,
// !isDeltaCycle, overload gate, v2-downgrade): those branches never
// invoke ComputeDelta and so cannot have been suppressed by the no-op
// check. Tests that need to verify "the follow-up trigger really did
// reach the no-op suppression branch, not some other early-return"
// read this counter — the wire-call count is the same in both shapes
// and so cannot disambiguate them on its own.
//
// Monotonic; safe for concurrent reads. Internal observability
// signal — not surfaced through RPC.
func (a *AnnounceLoop) NoopSuppressedTotal() uint64 {
	return a.noopSuppressedTotal.Load()
}

// TickInterval reports the actual ticker period the Run loop uses.
// The result is computed by chooseTickInterval and is ALWAYS a
// divisor of EffectiveForcedFullSyncInterval(a.interval) — that
// guarantee is the freshness invariant (see chooseTickInterval
// doc-comment): forced-full deadlines land exactly on a tick, so
// actual_max_gap between successive forced full syncs equals the cap
// itself rather than cap + tick. Pure read; safe for concurrent
// calls. Exported for tests and operational diagnostics.
func (a *AnnounceLoop) TickInterval() time.Duration {
	return chooseTickInterval(a.interval, EffectiveForcedFullSyncInterval(a.interval))
}

// minProductionTick is the tick floor for production cadences (both
// announceInterval and forced ≥ 1s). It guards against pathological
// non-second-aligned inputs producing sub-second gcd that would turn
// time.NewTicker into a busy loop. Sub-second cadences (test
// fixtures) bypass the floor — they need fine granularity by design.
//
// Why 1 second specifically: production AnnounceInterval flows from
// CORSA_ANNOUNCE_INTERVAL_SECONDS (whole-integer seconds) and
// `EffectiveForcedFullSyncInterval` is itself derived from the
// operator value, so in production both inputs are whole-second
// multiples and `1s` always divides the cap (DefaultTTL/2 = 60s).
// The floor is therefore a no-op in normal operation; it only
// engages on direct misuse like
// `WithAnnounceInterval(59*time.Second + 1*time.Nanosecond)` where
// gcd would otherwise drop to 1ns and starve the announce loop.
const minProductionTick = time.Second

// chooseTickInterval picks the announce-loop ticker period so that
// BOTH `forced` and `announceInterval` are integer multiples of the
// returned tick. This is the gcd of the two cadences (with `forced`
// dominating when the operator asks for an interval longer than the
// freshness cap allows). Two invariants come from this:
//
//  1. Forced-full deadlines land EXACTLY on a tick boundary
//     (`forced % tick == 0`) — actual_max_gap between successive
//     forced syncs equals `forced` itself. This preserves the
//     freshness invariant `forced ≤ DefaultTTL/2`.
//  2. Delta-cycle deadlines land EXACTLY on a tick boundary
//     (`announceInterval % tick == 0`) — operator-set
//     AnnounceInterval is the actual delta cadence rather than being
//     coarsened up to the next tick alignment.
//
// Why both: an earlier "largest divisor of forced ≤ interval"
// version of this helper preserved invariant (1) but broke (2). With
// announceInterval=45s and forced=60s, that version returned 30s
// (largest divisor of 60 below 45). 45 doesn't divide 30, so on each
// tick `now - lastDeltaCycleAt >= 45s` would only fire at every
// SECOND tick — effective delta cadence collapsed to 60s, defeating
// the operator's "fewer delta cycles" intent. Worse, every delta-due
// tick aligned with a forced-due tick (60s LCM), so the delta path
// was always pre-empted by forced-full and no standalone delta sends
// happened. gcd is the natural fix: the largest period that divides
// both cadences cleanly.
//
// Algorithm:
//
//   - If announceInterval ≥ forced, the operator is asking for delta
//     cadence equal to or slower than the freshness cap allows. The
//     cap dominates: tick = forced. (Delta cycles still respect
//     announceInterval through lastDeltaCycleAt, so tick=forced
//     means delta fires whenever forced fires plus when interval has
//     elapsed since the previous delta — typically every cap when
//     interval ≥ cap.)
//   - Otherwise tick = gcd(announceInterval, forced).
//   - If both inputs are ≥ minProductionTick (1s) AND the gcd above
//     came out below that floor (non-second-aligned input), clamp up
//     to minProductionTick. Since 1s divides any whole-second cap,
//     invariant (1) is preserved; invariant (2) may suffer minor
//     aliasing for the edge-case input (the operator already passed
//     a malformed value), but the loop cannot busy-spin.
//
// Examples (forced computed via EffectiveForcedFullSyncInterval =
// min(2 × announceInterval, DefaultTTL/2)):
//
//	announceInterval=30s, forced=60s → gcd=30s (operator value, divides both)
//	announceInterval=25s, forced=50s → gcd=25s (operator value, divides both)
//	announceInterval=20s, forced=40s → gcd=20s (operator value, divides both)
//	announceInterval=45s, forced=60s → gcd=15s (delta every 45s, forced every 60s)
//	announceInterval=50s, forced=60s → gcd=10s (delta every 50s, forced every 60s)
//	announceInterval=55s, forced=60s → gcd=5s  (delta every 55s, forced every 60s)
//	announceInterval=120s, forced=60s → tick=60s (cap dominates; delta cadence = 120s
//	                                              from per-peer lastDeltaCycleAt check)
//	announceInterval=59s+1ns, forced=60s → gcd=1ns → floor=1s (busy-loop guard)
//	announceInterval=10ms (test), forced=20ms → gcd=10ms (sub-second, no floor)
func chooseTickInterval(announceInterval, forced time.Duration) time.Duration {
	if announceInterval <= 0 || forced <= 0 {
		// Defensive: should never happen in production. Fall back to
		// the safer of the two (smaller is safer for invariant).
		if forced > 0 {
			return forced
		}
		return announceInterval
	}
	if announceInterval >= forced {
		return forced
	}
	tick := gcdDuration(announceInterval, forced)

	// Busy-loop guard: when both cadences are clearly production
	// values (≥ 1s) but the gcd dropped below 1s due to
	// non-second-aligned input (e.g. 59s + 1ns), clamp up to 1s.
	// Sub-second cadences (test fixtures, both inputs < 1s) bypass
	// the floor by design — they need fine granularity. See the
	// minProductionTick doc-comment for the rationale.
	if announceInterval >= minProductionTick && forced >= minProductionTick && tick < minProductionTick {
		return minProductionTick
	}
	return tick
}

// gcdDuration computes the greatest common divisor of two positive
// time.Duration values via the Euclidean algorithm. Both arguments
// must be > 0 — the caller is chooseTickInterval which already
// validated this. Pure function; safe for concurrent calls.
func gcdDuration(a, b time.Duration) time.Duration {
	for b > 0 {
		a, b = b, a%b
	}
	return a
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

// OverloadGate reports whether the host node is currently
// CPU/backlog-saturated. When the gate returns true at the start of
// an announce cycle, the loop suppresses delta sends to non-due peers
// (those that don't need a forced full sync this cycle) — the
// freshness invariant for forced-full-sync is preserved, but
// CPU-cheap mid-cycle delta computation is skipped. Implementations
// MUST be cheap (atomic load or single map probe) since the gate is
// queried at every cycle start; expensive sampling logic should run
// in a separate goroutine and publish results into an atomic flag.
//
// nil OverloadGate disables the throttle entirely (default behaviour
// for tests and any caller that has not wired backlog monitoring).
type OverloadGate interface {
	IsOverloaded() bool
}

// WithOverloadGate wires a backlog/CPU monitor to the announce loop.
// When the gate reports overloaded at cycle start, peers that don't
// need a forced full sync this cycle have their delta send skipped —
// forced full syncs continue to fire on schedule so freshness
// invariants hold. See OverloadGate doc-comment for the contract.
func WithOverloadGate(g OverloadGate) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.overloadGate = g
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

	// chooseTickInterval picks the ticker period so forced-full
	// deadlines land EXACTLY on a tick (tick is a divisor of the
	// freshness cap), not "deadline + up to one tick" later. This
	// preserves the freshness invariant for any operator-configured
	// AnnounceInterval, including non-divisors like 45s — see
	// chooseTickInterval doc-comment for the exact algorithm.
	// Per-peer delta suppression via lastDeltaCycleAt restores the
	// operator's "fewer delta cycles" intent on top of this faster
	// tick cadence.
	tickInterval := chooseTickInterval(a.interval, EffectiveForcedFullSyncInterval(a.interval))
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.announceToAllPeers(ctx)
		case <-a.triggerCh:
			// Triggered update is a "propagate now" signal — force
			// this cycle to be a delta cycle regardless of how
			// recently the previous one ran. Clearing lastDeltaCycleAt
			// short-circuits the delta-suppression check below.
			//
			// The ticker is INTENTIONALLY NOT reset here. An earlier
			// version called `ticker.Reset(tickInterval)` to coalesce
			// "trigger then immediate periodic tick" pairs, but that
			// silently broke the freshness invariant: with
			// triggers arriving slightly faster than tickInterval,
			// each Reset pushed the next periodic tick forward, and
			// the periodic forced-full deadline check kept getting
			// delayed until a trigger happened to land past the cap.
			// Example with AnnounceInterval=60s, forcedCap=60s,
			// triggers at 59s and 118s: the natural tick at 60s was
			// reset to 119s; the trigger at 59s saw deadline-not-due;
			// the trigger at 118s saw deadline-elapsed-by-58s and
			// fired forced full at 118s — gap 118s, 2× the cap.
			// Receivers in the single-dropped-sync scenario lost
			// learned routes silently between these stretched syncs.
			//
			// With no Reset, the ticker fires on its natural schedule
			// independently of triggers; back-to-back trigger+tick is
			// harmlessly suppressed by per-peer state checks
			// (lastDeltaCycleAt for delta, LastSuccessfulFullSyncAt
			// for forced).
			a.lastDeltaCycleAt = time.Time{}
			a.announceToAllPeers(ctx)
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
//
// Mode selection (v1 vs v2) is derived at the start of each per-peer step
// from the per-cycle AnnounceTarget.Capabilities (captured by peersFn at
// cycle start under the peer-state lock that produced Address and Identity).
// AnnouncePeerState.capabilities is the persistent mirror used by the
// classification, but it is reconciled to the same target caps at the very
// start of each per-peer goroutine via UpdateCapabilities. Without that
// reconciliation the persistent snapshot can drift away from the chosen
// session: peersFn dedupes overlapping sessions for one identity by map
// iteration order, so successive cycles can pick different sessions; a
// partial close (a routing-capable session that previously refreshed the
// snapshot disappears while an older routing-capable session remains) leaves
// the persistent caps describing a session that is no longer the target.
// Reconciling at cycle start makes classifyDeltaMode read two agreeing
// snapshots in production. The divergence path remains as a defensive net:
// if a future caller introduces a second writer to AnnouncePeerState.capabilities
// or routes around the cycle-time sync, MarkInvalid + legacy delta on the
// current cycle is the safe fallback. Forced-full and connect-time paths
// are unaffected — they always use SendAnnounceRoutes regardless of
// capability (see sendFullAnnounce / sendLegacyFull / docs/routing.md
// "First-sync wire-frame invariant").
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
		totalRaw               atomic.Int32
		totalAggregated        atomic.Int32
		totalDelta             atomic.Int32
		skippedNoop            atomic.Int32
		forcedFull             atomic.Int32
		coalescedTrigger       atomic.Int32
		deltaV2                atomic.Int32
		deltaV1                atomic.Int32
		modeDivergence         atomic.Int32
		v2DowngradedNoBaseline atomic.Int32
	)

	now := a.stateRegistry.clock()
	forcedFullSyncInterval := EffectiveForcedFullSyncInterval(a.interval)

	// Determine if THIS wake corresponds to a delta-cycle boundary.
	// The ticker may wake more often than a.interval when the operator
	// sets AnnounceInterval beyond DefaultTTL/2 — we still wake at the
	// TTL/2 cap to catch forced-full deadlines, but per-peer delta
	// sends are emitted only on wakes that cross a.interval since the
	// previous delta cycle. This restores the operator's "less
	// frequent delta computation" intent without violating the
	// freshness invariant. lastDeltaCycleAt is touched only by the
	// Run goroutine (triggerCh resets it; this method advances it),
	// so no concurrency despite the per-peer goroutines below.
	isDeltaCycle := a.lastDeltaCycleAt.IsZero() || now.Sub(a.lastDeltaCycleAt) >= a.interval
	if isDeltaCycle {
		a.lastDeltaCycleAt = now
	}

	// Backlog/CPU backpressure. When the gate is engaged, delta sends
	// for peers that don't owe a forced full sync this cycle are
	// skipped — the node still emits forced-full-sync to peers whose
	// deadline has elapsed, preserving the TTL/2 freshness invariant.
	//
	// overloadCycles counter: incremented post-wg only when at least
	// one peer was actually skipped due to overload during this
	// cycle. This is the strict "gate shed work" semantic — if every
	// peer happened to be forced-due (no delta would have been sent
	// anyway), overload had no effect and the counter does not
	// advance. The per-peer suppression decision sets `suppressedAny`
	// from inside its goroutine; the cycle-level increment runs after
	// wg.Wait() so the boolean reflects the full cycle, not a
	// partial state.
	overloaded := a.overloadGate != nil && a.overloadGate.IsOverloaded()
	var suppressedAny atomic.Bool

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

			// Reconcile persistent capability snapshot with the per-cycle
			// AnnounceTarget caps. peersFn (in production: node.routingCapablePeers)
			// dedupes overlapping sessions for one identity by map iteration
			// order, so two cycles can pick different sessions when several
			// routing-capable sessions for the same peer have different
			// negotiated caps. A partial close (the last cap-refreshing
			// session disappears while an older routing-capable session
			// remains) can also leave AnnouncePeerState.capabilities
			// describing a session that is no longer the chosen target.
			// Syncing here makes the persistent snapshot a derived view of
			// the same session this cycle picked: classifyDeltaMode below
			// reads two agreeing capability lists and the divergence path
			// stays purely defensive. Without this sync, classifyDeltaMode
			// would keep firing divergence; MarkInvalid only flips
			// needsFullResync and never refreshes caps, so the peer would
			// be stuck in legacy/forced-full forever.
			a.stateRegistry.UpdateCapabilities(peer.Identity, peer.Capabilities)

			// Read peer state atomically to decide send mode. View is
			// O(1) (lock + struct copy) — far cheaper than the
			// per-peer table scan in AnnounceTo + BuildAnnounceSnapshot
			// below, so the view-based needsFull decision can short-
			// circuit the expensive scan when the cycle won't actually
			// emit anything.
			view := peerState.View()
			needsFull := view.NeedsFullResync || view.LastSentSnapshot == nil

			// Check if periodic forced full sync is due. Inclusive (>=) so
			// the cycle that lands exactly at multiplier*interval triggers
			// the full sync — matching the cadence the
			// "Refresh interval invariant" promises (docs/routing.md). With
			// strict `>` the first eligible cycle would be one tick later,
			// turning the effective cadence into (multiplier+1)*interval
			// and pushing the next refresh past DefaultTTL/2.
			if !needsFull && !view.LastSuccessfulFullSyncAt.IsZero() {
				if now.Sub(view.LastSuccessfulFullSyncAt) >= forcedFullSyncInterval {
					needsFull = true
				}
			}

			// Phase 0 short-circuit before the expensive scan: if the
			// peer doesn't owe a forced full sync this cycle AND
			// either (a) the operator-set AnnounceInterval has not
			// elapsed since the last delta cycle, OR (b) the overload
			// gate is engaged, skip the per-peer table scan and
			// snapshot build entirely. AnnounceTo walks t.routes for
			// O(total_routes_in_table) per peer; on a 5000-entry table
			// with 33 peers this is ~165k entry comparisons per cycle
			// even when nothing will be sent. The early return here
			// converts that into O(1) per peer when nothing is due.
			if !needsFull && (!isDeltaCycle || overloaded) {
				// Mark cycle as having actually shed work only if
				// THIS peer was skipped specifically because of
				// overload — i.e. it was a delta-due peer the gate
				// suppressed. The other early-return reason
				// (!isDeltaCycle on forced-only wakes) is not
				// overload-related and must not count.
				if isDeltaCycle && overloaded {
					suppressedAny.Store(true)
				}
				return
			}

			// Build peer-specific raw entries from table.
			rawRoutes := a.table.AnnounceTo(peer.Identity)
			totalRaw.Add(int32(len(rawRoutes)))

			// Build canonical aggregated snapshot.
			snapshot := BuildAnnounceSnapshot(rawRoutes)
			totalAggregated.Add(int32(len(snapshot.Entries)))

			if needsFull {
				// Rate limit forced full sync attempts — but only when the
				// peer already has a baseline. A peer that never received
				// any data (LastSentSnapshot==nil, e.g. after a failed
				// first attempt) must retry without delay.
				//
				// The rate-limit window is clamped to forcedFullSyncInterval
				// so it never exceeds the documented forced-full cadence.
				// Without the clamp, an operator setting
				// CORSA_ANNOUNCE_INTERVAL_SECONDS=10s would get
				// forcedFullSyncInterval=20s from EffectiveForcedFullSyncInterval
				// (and exported helper would project a 20s deadline), but
				// after the first successful sync the second forced-full
				// attempt at the 20s deadline would be rate-limited by the
				// fixed MinForcedFullSyncInterval=30s — actual cadence
				// silently stretches to 30s, contradicting the helper's
				// projection. With the clamp, rate-limit window =
				// min(MinForcedFullSyncInterval, forcedFullSyncInterval),
				// so it is at most the cap and the helper's projection
				// stays honest. Default 30s interval is unchanged
				// (forcedFullSyncInterval=60s clamped against
				// MinForcedFullSyncInterval=30s ≥ rate-limit=30s).
				rateLimitWindow := MinForcedFullSyncInterval
				if forcedFullSyncInterval < rateLimitWindow {
					rateLimitWindow = forcedFullSyncInterval
				}
				if view.LastSentSnapshot != nil &&
					!view.LastFullSyncAttemptAt.IsZero() &&
					now.Sub(view.LastFullSyncAttemptAt) < rateLimitWindow {
					// Too soon — skip this cycle for this peer.
					coalescedTrigger.Add(1)
					return
				}

				a.sendFullAnnounce(ctx, cycleID, peer, peerState, snapshot, now)
				forcedFull.Add(1)
				return
			}

			// Delta path. Reaching this point already implies isDeltaCycle
			// is true and overload is not engaged (early-return above).
			delta := ComputeDelta(view.LastSentSnapshot, snapshot)
			totalDelta.Add(int32(len(delta)))

			if len(delta) == 0 {
				// No changes — suppress send. Two counters fire here:
				//   - skippedNoop is the per-cycle log field
				//     (`announce_skipped_noop`), reset every cycle;
				//   - noopSuppressedTotal is the cumulative loop
				//     counter exposed via NoopSuppressedTotal() so
				//     tests can prove the follow-up trigger reached
				//     the delta branch (a wire-count assertion alone
				//     cannot distinguish "delta computed and empty"
				//     from "early-return on !isDeltaCycle / overload /
				//     forced-full short-circuit").
				skippedNoop.Add(1)
				a.noopSuppressedTotal.Add(1)
				return
			}

			// Mode selection. Both sources of the peer's capability set must
			// agree on CapMeshRoutingV2 before the cycle may pick the v2 wire
			// path. Disagreement is treated as "state changed underneath us":
			// MarkInvalid forces the next cycle to take the forced-full path,
			// and this cycle falls back to legacy delta so the peer still
			// learns the change under whichever wire format it actually
			// understands. CapMeshRoutingV2 is opt-in on top of v1; a peer
			// that advertises v2 without v1 is treated as v1-only because the
			// first-sync invariant (legacy announce_routes) is gated on v1.
			mode := classifyDeltaMode(view.CapabilitiesSnapshot, peer.Capabilities)
			// Wire-baseline gate: v2 routes_update may be sent only after a
			// legacy announce_routes frame has actually gone out to this peer
			// in the current session. The empty-snapshot short-circuit in
			// connect-time and forced-full paths records a successful baseline
			// locally without emitting any wire frame, so HasSentWireBaseline
			// stays false and the peer's v2 receive gate would drop the first
			// routes_update and emit request_resync. Force the legacy delta
			// path here so the very first observable wire frame is the
			// baseline, exactly as required by the symmetric receive-side
			// invariant. After the legacy send succeeds, MarkWireBaselineSent
			// in sendIncrementalAnnounce flips the flag and subsequent cycles
			// take the v2 path normally.
			if mode == deltaModeV2 && !view.HasSentWireBaseline {
				v2DowngradedNoBaseline.Add(1)
				log.Debug().
					Uint64("announce_cycle_id", cycleID).
					Str("peer_identity", string(peer.Identity)).
					Str("peer_address", string(peer.Address)).
					Msg("announce_v2_downgraded_no_wire_baseline")
				mode = deltaModeV1
			}
			switch mode {
			case deltaModeV2:
				deltaV2.Add(1)
				a.sendIncrementalAnnounceV2(ctx, cycleID, peer, peerState, snapshot, delta, now)
			case deltaModeV1:
				deltaV1.Add(1)
				a.sendIncrementalAnnounce(ctx, cycleID, peer, peerState, snapshot, delta, now)
			case deltaModeDivergence:
				modeDivergence.Add(1)
				a.stateRegistry.MarkInvalid(peer.Identity)
				log.Warn().
					Uint64("announce_cycle_id", cycleID).
					Str("peer_identity", string(peer.Identity)).
					Str("peer_address", string(peer.Address)).
					Msg("announce_mode_divergence_fallback_to_legacy_delta")
				a.sendIncrementalAnnounce(ctx, cycleID, peer, peerState, snapshot, delta, now)
			}
		}(peer)
	}
	wg.Wait()

	// Increment the operator-visible overload counter only when the
	// gate actually shed work this cycle — at least one peer was
	// skipped specifically because of overload (a delta-due peer
	// suppressed by the gate). Cycles where the gate engaged but
	// every peer happened to be forced-due (no delta would have been
	// sent anyway) do NOT advance the counter.
	if suppressedAny.Load() {
		a.overloadCycles.Add(1)
	}

	log.Debug().
		Uint64("announce_cycle_id", cycleID).
		Int("peers", len(peers)).
		Int("raw_routes_count", int(totalRaw.Load())).
		Int("aggregated_routes_count", int(totalAggregated.Load())).
		Int("delta_routes_count", int(totalDelta.Load())).
		Int("announce_skipped_noop", int(skippedNoop.Load())).
		Int("announce_forced_full", int(forcedFull.Load())).
		Int("announce_trigger_coalesced", int(coalescedTrigger.Load())).
		Int("announce_delta_v1", int(deltaV1.Load())).
		Int("announce_delta_v2", int(deltaV2.Load())).
		Int("announce_mode_divergence", int(modeDivergence.Load())).
		Int("announce_v2_downgraded_no_wire_baseline", int(v2DowngradedNoBaseline.Load())).
		Bool("announce_overload_engaged", overloaded).
		Bool("announce_overload_suppressed_any", suppressedAny.Load()).
		Msg("announce_cycle_complete")
}

// deltaMode enumerates the wire-format outcomes of mode selection on a
// delta send. Kept internal to the routing package because the tri-state
// (v1, v2, divergence) is an implementation detail of announceToAllPeers.
type deltaMode int

const (
	// deltaModeV1 means legacy announce_routes is used — either because
	// the peer did not advertise CapMeshRoutingV2 or because v2 was not
	// advertised alongside v1 (see type doc on CapMeshRoutingV2).
	deltaModeV1 deltaMode = iota
	// deltaModeV2 means both sources agree on CapMeshRoutingV2 and the
	// cycle takes the v2 routes_update wire path.
	deltaModeV2
	// deltaModeDivergence means the two capability sources disagree. The
	// peer is marked NeedsFullResync and the cycle falls back to legacy
	// delta so the convergence signal still lands.
	deltaModeDivergence
)

// classifyDeltaMode derives the wire-format mode for a v1+ delta send.
// Both inputs must contain CapMeshRoutingV2 for v2 to be picked, and v2
// is meaningful only when CapMeshRoutingV1 is also advertised in that
// same input. In production both inputs are equal: the announce loop
// reconciles AnnouncePeerState.capabilities to the per-cycle
// AnnounceTarget.Capabilities via UpdateCapabilities at the start of
// each per-peer goroutine, so by the time this function runs both
// arguments come from the same chosen target. The two-input shape and
// the deltaModeDivergence outcome are kept as a defensive net for any
// future caller that bypasses the cycle-time sync — see the doc comment
// on AnnounceLoop.announceToAllPeers for the reconciliation contract.
func classifyDeltaMode(stateCaps []PeerCapability, targetCaps []PeerCapability) deltaMode {
	stateV2 := hasCapV2Both(stateCaps)
	targetV2 := hasCapV2Both(targetCaps)
	switch {
	case stateV2 && targetV2:
		return deltaModeV2
	case !stateV2 && !targetV2:
		return deltaModeV1
	default:
		return deltaModeDivergence
	}
}

// hasCapV2Both reports whether the capability slice contains both
// CapMeshRoutingV1 and CapMeshRoutingV2. v2 without v1 is treated as
// v1-only because the first-sync invariant (legacy announce_routes)
// is gated on v1.
func hasCapV2Both(caps []PeerCapability) bool {
	var v1, v2 bool
	for _, c := range caps {
		switch c {
		case domain.CapMeshRoutingV1:
			v1 = true
		case domain.CapMeshRoutingV2:
			v2 = true
		}
	}
	return v1 && v2
}

// sendFullAnnounce sends a complete announce snapshot to the peer and
// updates cache state on success via thread-safe Record* methods.
//
// Forced-full / connect-time / periodic-forced-full MUST use the legacy
// announce_routes wire frame. See the "First-sync wire-frame invariant"
// section in docs/routing.md for the normative contract: the first
// full-sync after session establishment — and every later forced full
// resync — is always legacy regardless of peer capabilities, because the
// peer has no baseline to diff a v2 routes_update against. Wire-send goes
// through sendLegacyFull so the single helper is the only in-package call
// site that invokes PeerSender.SendAnnounceRoutes for full sync; do NOT
// add a branch on peer.Capabilities here that picks SendRoutesUpdate.
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
	// This short-circuit predates the first-sync invariant and is orthogonal
	// to the "First-sync wire-frame invariant" section in docs/routing.md:
	// empty-baseline intentionally emits neither legacy nor v2 wire frame.
	if len(snapshot.Entries) == 0 {
		state.RecordFullSyncSuccess(snapshot, now)
		return
	}

	if !a.sendLegacyFull(ctx, peer, snapshot) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("routes", len(snapshot.Entries)).
			Msg("announce_full_send_failed")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	// A non-empty legacy announce_routes frame went out — the peer has now
	// observed a wire baseline in this session, so the v2 receive gate is
	// open. Subsequent deltas may pick the v2 wire frame when caps agree.
	state.MarkWireBaselineSent()
	state.RecordFullSyncSuccess(snapshot, now)
}

// sendLegacyFull sends a full snapshot via the legacy announce_routes wire
// frame. This is the only helper allowed for forced-full / connect-time /
// periodic-forced-full paths inside the routing package; see the
// "First-sync wire-frame invariant" section in docs/routing.md for the
// full rationale.
//
// Naming is deliberate: any future change that wants a v2 routes_update
// branch on full sync would have to either rename or bypass this helper,
// and both options are a loud review tripwire. The helper does not inspect
// peer.Capabilities — full sync is always legacy, by definition. The v2
// delta path uses PeerSender.SendRoutesUpdate directly from
// sendIncrementalAnnounce (once the v2 frame lands), never via this helper.
//
// ctx propagates the caller's cancellation/deadline down to the transport;
// see PeerSender.SendAnnounceRoutes doc for the ctx contract. Returns the
// same bool the sender returned — collapsing every transport-drop class to
// a single negative outcome so the caller can keep the success/failure
// path uniform with the delta-send helper.
func (a *AnnounceLoop) sendLegacyFull(
	ctx context.Context,
	peer AnnounceTarget,
	snapshot *AnnounceSnapshot,
) bool {
	return a.sender.SendAnnounceRoutes(ctx, peer.Address, snapshot.Entries)
}

// sendIncrementalAnnounce sends only changed entries to the peer via the
// legacy announce_routes wire frame and updates cache to the full new
// snapshot on success via thread-safe Record* methods. Used when the
// peer has not negotiated CapMeshRoutingV2 (v1-only delta path) and as
// the mode-divergence fallback — see announceToAllPeers for the
// classification rules.
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
			Msg("announce_delta_send_failed_v1")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	// A legacy announce_routes frame went out — even on the v1 delta path,
	// the peer's v2 receive gate (if any) treats this arrival as the wire
	// baseline. Flip the flag so future cycles may pick the v2 frame when
	// both capability sources agree.
	state.MarkWireBaselineSent()
	// Cache stores the full snapshot, not just the delta payload.
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}

// sendIncrementalAnnounceV2 sends the delta via the v2 routes_update wire
// frame. Only picked by announceToAllPeers when both capability sources
// agree on CapMeshRoutingV2 and a baseline exists on the peer side — the
// first-sync invariant guarantees the baseline is always delivered via
// the legacy announce_routes frame first (see sendFullAnnounce /
// sendLegacyFull).
//
// On success the cache is updated exactly like the v1 delta path: the
// full new snapshot is stored so the next cycle can compute a fresh
// diff. A send failure leaves the cache untouched; the next cycle
// retries with a fresh delta recomputation.
func (a *AnnounceLoop) sendIncrementalAnnounceV2(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	fullSnapshot *AnnounceSnapshot,
	delta []AnnounceEntry,
	now time.Time,
) {
	if !a.sender.SendRoutesUpdate(ctx, peer.Address, delta) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("delta_routes", len(delta)).
			Msg("announce_delta_send_failed_v2")
		return
	}
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}
