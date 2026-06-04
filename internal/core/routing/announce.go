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

	// DefaultTriggerMinSpacing is the production minimum spacing
	// between two TRIGGER-driven announce cycles (TriggerUpdate).
	// Under a route-churn storm (peer flap, quarantine invalidation,
	// withdrawal cascades) every table mutation calls TriggerUpdate;
	// without pacing each trigger immediately runs announceToAllPeers
	// — a full per-peer delta computation pass — many times per
	// second, and the emitted deltas make every neighbour do the same
	// and echo the churn back. Pacing coalesces all triggers inside
	// the window into ONE deferred cycle at the window boundary: the
	// local table is already updated (forwarding never uses the
	// withdrawn route), neighbours learn at most
	// DefaultTriggerMinSpacing later.
	//
	// The PERIODIC ticker and the forced-full-sync deadline machinery
	// are deliberately untouched by pacing — the freshness invariant
	// (forced full ≤ TTL/2) holds exactly as before. Zero disables
	// pacing (legacy immediate behaviour; the routing-package tests
	// that drive cycles via back-to-back TriggerUpdate calls rely on
	// it, so NewAnnounceLoop defaults to 0 and production opts in via
	// WithTriggerMinSpacing — see node.Service announce-loop wiring).
	DefaultTriggerMinSpacing = 5 * time.Second

	// DigestRoundTripGrace bounds how long the reconnect path defers the
	// first forced full sync to a peer while a route_sync_digest_v1 is in
	// flight awaiting its summary (Phase 3 §4.5). It must cover a
	// worst-case mesh round trip (request enqueue + WAN RTT + remote
	// handler + reply) so the reconnect-triggered announce cycle holds off
	// the full sync long enough for a match to arrive — without letting a
	// silent or slow peer defer the freshness-restoring full sync for
	// long: on no summary within the grace the next periodic cycle
	// full-syncs as the safe degradation path. A match summary replaces
	// this short window with the cadence-length window
	// (EffectiveForcedFullSyncInterval) via ConfirmPeerDigestMatch.
	//
	// MarkPeerDigestPending clamps the grace to the loop's
	// EffectiveForcedFullSyncInterval so a node configured with a very
	// short AnnounceInterval (cadence < grace) never suppresses for longer
	// than one forced-full cadence — the freshness invariant holds.
	DigestRoundTripGrace = 5 * time.Second
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
// Three wire frames are carried by three distinct methods so that the
// choice of wire format lives on the call site and cannot silently flip
// inside the implementation. The call-site contract is:
//   - connect-time and forced-full paths emit a self-contained baseline
//     frame: SendAnnounceRoutes (legacy announce_routes) for v1/v2
//     peers, or SendRouteAnnounceV3 with kind="full" for peers that
//     advertise the v3 triplet (v1 + v3 + relay). Both qualify as
//     valid baselines under the "First-sync wire-frame invariant"
//     documented in docs/routing.md; the delta-only frames
//     (SendRoutesUpdate, SendRouteAnnounceV3 kind="delta") are
//     forbidden here because a fresh session has no baseline to
//     diff against;
//   - the delta path chooses between SendAnnounceRoutes (peer on v1
//     only), SendRoutesUpdate (peer on v2), and SendRouteAnnounceV3
//     with kind="delta" (peer on v3) based on the peer's capability
//     snapshot captured at cycle start — see
//     AnnounceLoop.announceToAllPeers and the HasSentWireBaseline /
//     HasSentWireBaselineV3 gates that downgrade to v1 until the
//     matching upgraded baseline has actually gone on the wire.
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
	//   - connect-time full sync to v1/v2 peers (peers that have NOT
	//     negotiated CapMeshRoutingV3 — v3-triplet peers take the
	//     SendRouteAnnounceV3 kind="full" baseline path instead, per
	//     the First-sync wire-frame invariant in docs/routing.md);
	//   - periodic forced full sync in the announce loop, same v3-vs-
	//     legacy split as connect-time;
	//   - delta updates for peers that have NOT negotiated the
	//     CapMeshRoutingV2 capability.
	// Either way the connect-time / forced-full path NEVER picks a
	// delta frame (SendRoutesUpdate or kind="delta") — both are
	// deltas and would arrive at a peer with no baseline.
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
	//   - the peer's negotiated capability snapshot contains the FULL
	//     v2 triplet — CapMeshRoutingV1 + CapMeshRoutingV2 +
	//     CapMeshRelayV1 (see hasCapV2Triplet). Relay is non-optional
	//     because the node-side dispatch
	//     (dispatchAnnouncePlaneFrameWithCaps) gates this method on the
	//     same triplet — a relay-less v2 classification would route to
	//     a sender that refuses the frame with no legacy fallback;
	//   - the cycle is a delta send (the peer already has a baseline);
	//   - the crosscheck between AnnouncePeerState.Capabilities and
	//     AnnounceTarget.Capabilities agrees (divergence forces a full
	//     resync via MarkInvalid — see announceToAllPeers).
	//
	// SendRoutesUpdate MUST NOT be used for:
	//   - the first sync after session establishment (initial sync is
	//     always a self-contained baseline — legacy announce_routes
	//     for v1/v2 peers or SendRouteAnnounceV3 kind="full" for v3-
	//     triplet peers, per the First-sync wire-frame invariant);
	//   - forced full resync (same baseline contract: legacy or v3
	//     kind="full", never a delta).
	//
	// Semantics of ctx, peerAddress, and the bool return value match
	// SendAnnounceRoutes.
	SendRoutesUpdate(ctx context.Context, peerAddress PeerAddress, delta []AnnounceEntry) bool

	// SendRouteAnnounceV3 sends the Phase 4 compact-announce wire frame
	// (route_announce_v3, overview §7.1) to the peer identified by
	// peerAddress. kind discriminates a self-contained baseline ("full")
	// from an incremental update ("delta") — see
	// protocol.RouteAnnounceV3KindFull / KindDelta. epoch is the local
	// route_announce_v3 epoch counter (Table.Epoch()), shipped on every
	// frame so the receiver can detect a peer table reset (overview
	// §7.1 "Epoch handling"). entries carry the per-uplink claims with
	// the Origin field dropped — the receiver synthesises Origin as the
	// sending identity on ingest (sender-originated semantic — see
	// handleRouteAnnounceV3 in internal/core/node).
	//
	// This is the v3 capability-gated wire path. The announce loop
	// picks SendRouteAnnounceV3 over SendRoutesUpdate /
	// SendAnnounceRoutes when both peers advertise the FULL v3
	// triplet — CapMeshRoutingV1 + CapMeshRoutingV3 + CapMeshRelayV1
	// — see PeerSupportsV3 / hasCapV3Triplet. The relay cap is
	// non-optional because the send-side dispatch in
	// node.dispatchAnnouncePlaneFrameWithCaps gates this method on
	// v1+v3+relay (mirror of the inbound / outbound receive
	// dispatchers). A relay-less v3 peer therefore falls back through
	// the classifier to legacy (Round-20 fallback contract) rather
	// than being routed to a sender that would refuse the frame.
	// v3 is meaningful only when v1 is also negotiated: v1 gates the
	// legacy fallback the mixed-version flow depends on.
	//
	// First-sync behaviour. Unlike SendRoutesUpdate, SendRouteAnnounceV3
	// MAY carry the very first session sync when kind == "full": v3
	// kind="full" is self-contained and replaces the prior view of the
	// sender's table for this session. The first-sync invariant
	// documented on SendAnnounceRoutes (always legacy announce_routes
	// for initial sync against v2 peers) applies specifically to the
	// v1/v2 wire pair; for v3-capable pairs the connect-time /
	// forced-full path is allowed to ship a v3 kind="full" frame and
	// the peer's receive-side baseline gate accepts it directly
	// (handleRouteAnnounceV3). SendRouteAnnounceV3 with kind="delta"
	// MUST NOT be called before a v3 kind="full" has gone out to the
	// peer this session — the receiver would treat the early delta as
	// a desync and reply with request_resync.
	//
	// Semantics of ctx, peerAddress, and the bool return value match
	// SendAnnounceRoutes. Mocks are regenerated from this interface via
	// `make mocks`; the mockery entry for PeerSender in `.mockery.yaml`
	// has no method list, so adding methods here flows through on the
	// next regen.
	SendRouteAnnounceV3(ctx context.Context, peerAddress PeerAddress, kind string, epoch uint64, entries []AnnounceEntry) bool
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

	// triggerMinSpacing is the minimum wall-clock spacing between two
	// trigger-driven cycles. A trigger arriving earlier than
	// triggerMinSpacing after the previous trigger-driven cycle is
	// NOT dropped — it is deferred to the window boundary, and every
	// further trigger inside the window coalesces into that one
	// deferred cycle. Zero disables pacing (every trigger runs
	// immediately — the pre-pacing behaviour). See
	// DefaultTriggerMinSpacing for rationale; only the Run goroutine
	// reads it after construction.
	triggerMinSpacing time.Duration

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

	// digestSuppressionMu protects digestSuppression. Kept separate
	// from a.mu (which governs the Run/running lifecycle) so per-peer
	// goroutines spawned by announceToAllPeers can poll the
	// suppression map without contending against shutdown or startup
	// paths.
	digestSuppressionMu sync.Mutex

	// digestSuppression records, per peer, the deadline past which the
	// forced-full-sync may fire again AND the digest we emitted to that
	// peer on reconnect. The stored digest is the correlation anchor:
	// ConfirmPeerDigestMatch only extends the window when an inbound
	// route_sync_summary_v1 echoes EXACTLY the digest we sent, so a
	// stale, unsolicited, or empty-echo summary cannot suppress a
	// forced full sync it has no relation to (Phase 3 §4.5 trust
	// invariant; UnmarshalRouteSyncSummaryFrame's "empty echo = no
	// correlation" contract).
	//
	// Armed short on reconnect by MarkPeerDigestPending; extended to one
	// forced-full cadence by ConfirmPeerDigestMatch on a correlated
	// match. nil until first arm so idle nodes pay zero allocation.
	//
	// Suppression applies ONLY to soft (session-boundary) resyncs and
	// the periodic full-sync deadline; the initial sync
	// (LastSentSnapshot == nil) and HARD resyncs (request_resync /
	// MarkInvalid, view.ResyncIsHard) bypass the gate so the peer always
	// gets a baseline when it truly needs one.
	digestSuppression map[PeerIdentity]digestSuppressionEntry
}

// digestSuppressionEntry is the per-peer suppression record: the
// deadline, the digest we emitted (the correlation anchor for an
// inbound summary echo), and whether a correlated match has already
// been consumed. See AnnounceLoop.digestSuppression.
type digestSuppressionEntry struct {
	until    time.Time
	expected string
	// confirmed is set once ConfirmPeerDigestMatch has accepted a
	// correlated echo and extended the window. It makes the confirm
	// single-shot: a duplicate or replayed summary echoing the same
	// digest within the window cannot keep pushing `until` forward, so
	// one correlated reply suppresses AT MOST one forced-full cadence
	// (Phase 3 §4.5 invariant).
	confirmed bool
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

// ConfirmPeerDigestMatch correlates an inbound route_sync_summary_v1
// echo against the digest THIS node emitted to the peer on reconnect,
// and — only on a match — extends the suppression window to one
// forced-full cadence. It returns true when the summary correlated and
// suppression was (re)armed, false when it was ignored.
//
// Correlation is the whole point (Phase 3 §4.5 trust invariant): a
// summary may suppress a forced full sync ONLY if it is a reply to a
// digest we actually sent. The handler must therefore have an active
// pending entry (armed by MarkPeerDigestPending) whose stored digest
// equals `echo`. A summary is IGNORED when:
//
//   - no pending entry exists (unsolicited — we never sent a digest);
//   - the entry was already confirmed once (single-shot — a duplicate or
//     replayed summary must not keep extending the window, so one reply
//     defers at most one forced-full cadence);
//   - the pending entry has already elapsed (the round trip overran the
//     grace; the safe path already full-synced or will);
//   - `echo` is empty (UnmarshalRouteSyncSummaryFrame's documented
//     "no correlation, drop the update" contract);
//   - `echo` differs from the digest we sent (stale or spoofed).
//
// On a correlated match the window length is derived from THIS loop's
// EffectiveForcedFullSyncInterval (= min(2*AnnounceInterval,
// DefaultTTL/2)) anchored at `now`, never a caller-supplied deadline,
// so custom operator intervals stay honest and the window never exceeds
// one cadence (beyond which the digest is too stale to trust).
//
// The delta path keeps firing regardless, so any real state change still
// propagates; only the byte-heavy periodic re-sync is elided. Suppression
// is consulted only for soft (session-boundary) resyncs and the periodic
// deadline branch — initial sync (LastSentSnapshot == nil) and HARD
// resyncs (view.ResyncIsHard) bypass the gate, see announceToAllPeers.
//
// `now` is injected so tests drive a deterministic clock; production
// passes time.Now().UTC(). Safe to call from any goroutine.
func (a *AnnounceLoop) ConfirmPeerDigestMatch(peer PeerIdentity, echo string, now time.Time) bool {
	if peer == "" || echo == "" {
		return false
	}
	a.digestSuppressionMu.Lock()
	defer a.digestSuppressionMu.Unlock()
	if a.digestSuppression == nil {
		return false
	}
	entry, ok := a.digestSuppression[peer]
	if !ok || entry.confirmed || !now.Before(entry.until) || entry.expected != echo {
		return false
	}
	entry.until = now.Add(EffectiveForcedFullSyncInterval(a.interval))
	entry.confirmed = true
	a.digestSuppression[peer] = entry
	return true
}

// MarkPeerDigestPending arms a SHORT suppression window when a
// route_sync_digest_v1 has just been emitted to the peer on reconnect
// and we are awaiting its summary. Without it the reconnect's own
// TriggerUpdate cycle would full-sync the peer before the summary could
// possibly arrive, which is the exact race that left Phase 3 incremental
// sync ineffective on its intended path.
//
// The window is DigestRoundTripGrace, clamped to the loop's
// EffectiveForcedFullSyncInterval so a node with a very short
// AnnounceInterval never defers a full sync past one forced-full cadence
// (the freshness invariant). On a correlated match ConfirmPeerDigestMatch
// extends this to the cadence-length window; on a mismatch or no reply
// the grace elapses (or ClearPeerDigestSuppression removes it) and the
// next cycle full-syncs — the documented safe degradation.
//
// `sentDigest` is the digest emitted to the peer in the same reconnect
// step; it is stored as the correlation anchor so only a summary echoing
// exactly this value can extend the window (see ConfirmPeerDigestMatch).
//
// `now` is injected for deterministic tests; production passes
// time.Now().UTC(). Safe to call from any goroutine. No-op when peer is
// empty or the derived window is non-positive (loop without a configured
// interval).
func (a *AnnounceLoop) MarkPeerDigestPending(peer PeerIdentity, now time.Time, sentDigest string) {
	if peer == "" {
		return
	}
	grace := DigestRoundTripGrace
	if cadence := EffectiveForcedFullSyncInterval(a.interval); grace > cadence {
		grace = cadence
	}
	if grace <= 0 {
		return
	}
	a.digestSuppressionMu.Lock()
	defer a.digestSuppressionMu.Unlock()
	if a.digestSuppression == nil {
		a.digestSuppression = make(map[PeerIdentity]digestSuppressionEntry)
	}
	a.digestSuppression[peer] = digestSuppressionEntry{
		until:    now.Add(grace),
		expected: sentDigest,
	}
}

// ClearPeerDigestSuppression removes any active suppression window for the
// peer so the next announce cycle is free to forced-full-sync immediately.
// Called when a route_sync_summary_v1 reports match=false: the digests
// diverged, so a pending grace armed on reconnect must not keep deferring
// the resync. Safe to call from any goroutine; no-op for absent entries.
func (a *AnnounceLoop) ClearPeerDigestSuppression(peer PeerIdentity) {
	if peer == "" {
		return
	}
	a.digestSuppressionMu.Lock()
	defer a.digestSuppressionMu.Unlock()
	if a.digestSuppression == nil {
		return
	}
	delete(a.digestSuppression, peer)
}

// isDigestSuppressionActive reports whether the periodic forced-
// full-sync to the peer is currently suppressed. Returns false
// for absent entries and for entries whose deadline has elapsed
// (the helper evicts stale entries on the way out so the map
// does not grow unbounded as peers come and go).
//
// Production caller: announceToAllPeers — see the
// `needsFull && view.LastSentSnapshot != nil && !view.ResyncIsHard`
// guard around the forced-full deadline branch.
func (a *AnnounceLoop) isDigestSuppressionActive(peer PeerIdentity, now time.Time) bool {
	a.digestSuppressionMu.Lock()
	defer a.digestSuppressionMu.Unlock()
	if a.digestSuppression == nil {
		return false
	}
	entry, ok := a.digestSuppression[peer]
	if !ok {
		return false
	}
	if !now.Before(entry.until) {
		delete(a.digestSuppression, peer)
		return false
	}
	return true
}

// IsDigestSuppressionActiveForTest is the public test seam for
// node-package integration tests that need to assert "the
// summary handler armed suppression for this peer". The
// "ForTest" suffix is the conventional Go signal that
// production code must NOT call this — production reads route
// through the internal isDigestSuppressionActive helper called
// from the announce-cycle goroutine.
//
// Safe to call from any goroutine.
func (a *AnnounceLoop) IsDigestSuppressionActiveForTest(peer PeerIdentity, now time.Time) bool {
	return a.isDigestSuppressionActive(peer, now)
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
// announceToAllPeers can decide on a wire format without re-entering the
// Service's peer mutex per peer — that re-entry pattern collides with
// writer-preferring sync.RWMutex semantics and has been observed to starve
// reads under load.
//
// Wire-format selection lives in announceToAllPeers's per-peer goroutine and
// reads from this snapshot via classifyDeltaMode + the
// HasSentWireBaseline / HasSentWireBaselineV3 gates: a peer with the v3
// triplet (mesh_routing_v1 + mesh_routing_v3 + mesh_relay_v1) AND a v3
// baseline already on the wire gets route_announce_v3 kind="delta"; a peer
// with v1+v2+relay AND a v2-side baseline gets routes_update; everything
// else falls back to legacy announce_routes. Forced-full / connect-time
// take a parallel split (sendV3Full vs sendLegacyFull) — neither path picks
// a delta. See the PeerSender doc-comment in this file for the full
// dispatch matrix and docs/routing.md "First-sync wire-frame invariant"
// for the baseline contract.
//
// AnnounceLoop and any downstream consumer MUST treat Capabilities as
// read-only; producers build a fresh slice per target so mutation here cannot
// corrupt session state. Consumers that need a specific capability check
// should range over the slice directly rather than re-fetching capabilities
// from the Service.
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

// WithTriggerMinSpacing enables trigger pacing: at most one
// trigger-driven announce cycle per d, with in-window triggers
// deferred (not dropped) to the window boundary and coalesced.
// d <= 0 keeps the legacy immediate behaviour. The periodic ticker
// and forced-full deadlines are unaffected. See
// DefaultTriggerMinSpacing for the production rationale.
func WithTriggerMinSpacing(d time.Duration) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.triggerMinSpacing = d
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

	// Trigger pacing state (all owned by this goroutine). When a
	// trigger arrives less than triggerMinSpacing after the previous
	// trigger-driven cycle, it is deferred: pacer is armed for the
	// remainder of the window and every further trigger inside the
	// window coalesces into that one pending cycle. The deferred
	// cycle is byte-identical in effect to an immediate one — only
	// its start time moves. See DefaultTriggerMinSpacing for why
	// this exists (CPU churn under route-flap storms).
	var (
		lastTriggeredCycle time.Time
		pacer              *time.Timer
		pacerC             <-chan time.Time
	)
	stopPacer := func() {
		if pacer != nil {
			pacer.Stop()
			pacer = nil
			pacerC = nil
		}
	}
	defer stopPacer()

	// runTriggeredCycle is the body shared by the immediate and the
	// deferred (pacer-fired) trigger paths.
	//
	// Triggered update is a "propagate now" signal — force this
	// cycle to be a delta cycle regardless of how recently the
	// previous one ran. Clearing lastDeltaCycleAt short-circuits the
	// delta-suppression check in announceToAllPeers.
	//
	// The ticker is INTENTIONALLY NOT reset here. An earlier version
	// called `ticker.Reset(tickInterval)` to coalesce "trigger then
	// immediate periodic tick" pairs, but that silently broke the
	// freshness invariant: with triggers arriving slightly faster
	// than tickInterval, each Reset pushed the next periodic tick
	// forward, and the periodic forced-full deadline check kept
	// getting delayed until a trigger happened to land past the cap.
	// Example with AnnounceInterval=60s, forcedCap=60s, triggers at
	// 59s and 118s: the natural tick at 60s was reset to 119s; the
	// trigger at 59s saw deadline-not-due; the trigger at 118s saw
	// deadline-elapsed-by-58s and fired forced full at 118s — gap
	// 118s, 2× the cap. Receivers in the single-dropped-sync
	// scenario lost learned routes silently between these stretched
	// syncs.
	//
	// With no Reset, the ticker fires on its natural schedule
	// independently of triggers; back-to-back trigger+tick is
	// harmlessly suppressed by per-peer state checks
	// (lastDeltaCycleAt for delta, LastSuccessfulFullSyncAt for
	// forced). The same argument covers trigger PACING: deferring a
	// trigger never touches the ticker, so the forced-full cadence
	// is provably unchanged by the pacing window.
	runTriggeredCycle := func() {
		a.lastDeltaCycleAt = time.Time{}
		a.announceToAllPeers(ctx)
		lastTriggeredCycle = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.announceToAllPeers(ctx)
		case <-a.triggerCh:
			if a.triggerMinSpacing <= 0 {
				// Pacing disabled — legacy immediate path.
				runTriggeredCycle()
				continue
			}
			if pacerC != nil {
				// A deferred cycle is already pending; this trigger
				// coalesces into it.
				continue
			}
			if wait := a.triggerMinSpacing - time.Since(lastTriggeredCycle); !lastTriggeredCycle.IsZero() && wait > 0 {
				// Inside the cooldown window — defer to the boundary.
				pacer = time.NewTimer(wait)
				pacerC = pacer.C
				continue
			}
			runTriggeredCycle()
		case <-pacerC:
			stopPacer()
			// Drain a trigger that may have queued while the pacer
			// was pending — this cycle covers it (coalescing), and
			// leaving it queued would immediately re-arm the pacer
			// for a redundant follow-up cycle.
			select {
			case <-a.triggerCh:
			default:
			}
			runTriggeredCycle()
		}
	}
}

// TriggerUpdate requests an announcement cycle. Safe to call from any
// goroutine. Multiple rapid calls coalesce into a single cycle. With
// trigger pacing enabled (WithTriggerMinSpacing) the cycle may be
// deferred by up to the pacing window — "propagate soon", not
// "propagate now"; without pacing it runs immediately.
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
		deltaV3                atomic.Int32
		deltaV2                atomic.Int32
		deltaV1                atomic.Int32
		modeDivergence         atomic.Int32
		v2DowngradedNoBaseline atomic.Int32
		v3DowngradedNoBaseline atomic.Int32
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

			// Phase 3 PR 12.5 — digest-match suppression. When a
			// route_sync exchange for this peer is in flight or
			// already reported match=true (and the suppression
			// deadline has not elapsed), elide the forced full
			// sync. The delta path still runs in this cycle, so any
			// state that did change is still propagated; only the
			// byte-heavy full re-confirmation is saved.
			//
			// The reconnect full sync is the PRIMARY path this gate
			// targets (docs/protocol/route_sync.md): on reconnect
			// the session hook arms a short pending window via
			// MarkPeerDigestPending and emits the digest, so this
			// very cycle (fired by the reconnect TriggerUpdate) sees
			// an active window and holds off the full sync until the
			// summary lands. A match extends the window for one
			// cadence; a mismatch / silence clears it and the next
			// cycle full-syncs.
			//
			// Two conditions exempt the gate so a peer never misses a
			// needed baseline:
			//
			//   - view.LastSentSnapshot == nil: the peer has no prior
			//     baseline (first-ever sync, or a hard reset).
			//     Suppression would leave it with no routing state.
			//   - view.ResyncIsHard: an explicit request_resync or a
			//     consistency-loss MarkInvalid set this. The peer is
			//     demanding a fresh full table; a digest hint must not
			//     suppress it. A soft (session-boundary) resync, by
			//     contrast, IS suppressible — that is the reconnect
			//     optimisation.
			if needsFull && view.LastSentSnapshot != nil && !view.ResyncIsHard && a.isDigestSuppressionActive(peer.Identity, now) {
				needsFull = false
			}

			// Phase 0 short-circuit before the expensive scan: if the
			// peer doesn't owe a forced full sync this cycle AND
			// either (a) the operator-set AnnounceInterval has not
			// elapsed since the last delta cycle, OR (b) the overload
			// gate is engaged, skip the per-peer table scan and
			// snapshot build entirely. AnnounceTo walks the route store for
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
			// Wire-baseline gates. Each delta generation needs the
			// matching baseline already on the wire:
			//   - v2 routes_update needs a prior legacy announce_routes
			//     (HasSentWireBaseline) so the peer's v2 receive gate
			//     accepts it.
			//   - v3 route_announce_v3 kind="delta" needs a prior v3
			//     kind="full" (HasSentWireBaselineV3) so the peer's
			//     handleRouteAnnounceV3 baseline gate accepts it.
			// The empty-snapshot short-circuit in connect-time and
			// forced-full paths records a baseline locally without
			// emitting any wire frame, so the matching flag stays false
			// and the first non-empty delta is downgraded to legacy. After
			// the appropriate full send succeeds, MarkWireBaselineSent /
			// MarkWireBaselineV3Sent flips the flag and subsequent cycles
			// take the upgraded path normally.
			if mode == deltaModeV3 && !view.HasSentWireBaselineV3 {
				// Round-13 fix: use the dedicated v3 counter, not
				// v2DowngradedNoBaseline. Sharing the v2 metric
				// inflated v2 rollout telemetry and made the v3
				// rollout invisible — operators reading the cycle
				// log line saw v3 baseline downgrades counted
				// against v2's no-baseline budget, hiding any
				// actual v3 baseline lag.
				v3DowngradedNoBaseline.Add(1)
				log.Debug().
					Uint64("announce_cycle_id", cycleID).
					Str("peer_identity", string(peer.Identity)).
					Str("peer_address", string(peer.Address)).
					Msg("announce_v3_downgraded_no_wire_baseline")
				// Fall back to the highest gen the peer can accept whose
				// baseline IS established — same legacy-v1 fallback the v2
				// downgrade uses, which keeps the mixed-version code path
				// minimal until the v3 baseline lands.
				mode = deltaModeV1
			}
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
			case deltaModeV3:
				deltaV3.Add(1)
				a.sendIncrementalAnnounceV3(ctx, cycleID, peer, peerState, snapshot, delta, now)
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
		Int("announce_delta_v3", int(deltaV3.Load())).
		Int("announce_mode_divergence", int(modeDivergence.Load())).
		Int("announce_v2_downgraded_no_wire_baseline", int(v2DowngradedNoBaseline.Load())).
		Int("announce_v3_downgraded_no_wire_baseline", int(v3DowngradedNoBaseline.Load())).
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
	// the peer did not advertise the full v2 triplet
	// (CapMeshRoutingV1 + CapMeshRoutingV2 + CapMeshRelayV1, see
	// hasCapV2Triplet) or because v2 was not advertised alongside v1
	// (see type doc on CapMeshRoutingV2). A relay-less v1+v2 peer
	// also lands here — the Round-21 triplet rule keeps v2
	// classification in lock-step with the SendRoutesUpdate
	// dispatch gate so no frame can be silently dropped.
	deltaModeV1 deltaMode = iota
	// deltaModeV2 means both sources agree on the full v2 triplet
	// (CapMeshRoutingV1 + CapMeshRoutingV2 + CapMeshRelayV1) and the
	// cycle takes the v2 routes_update wire path.
	deltaModeV2
	// deltaModeDivergence means the two capability sources disagree. The
	// peer is marked NeedsFullResync and the cycle falls back to legacy
	// delta so the convergence signal still lands.
	deltaModeDivergence
	// deltaModeV3 means both sources agree on the full v3 triplet
	// (CapMeshRoutingV1 + CapMeshRoutingV3 + CapMeshRelayV1, see
	// hasCapV3Triplet — Phase 4) and the cycle takes the
	// route_announce_v3 wire path with kind="delta". The compact
	// frame drops the redundant Origin field (overview §7.1) and
	// saves ~50% of wire bandwidth on full sync after the
	// per-Identity collapse from Phase 0.x. Gated on
	// HasSentWireBaselineV3 — see the v3 mode-selection note in
	// announceToAllPeers.
	deltaModeV3
)

// routeAnnounceV3KindFull / routeAnnounceV3KindDelta are the wire-protocol
// "kind" discriminator strings the v3 send path passes through to
// PeerSender.SendRouteAnnounceV3. Defined here as routing-local
// constants to keep the routing package free of a protocol-layer
// import (the routing package is deliberately wire-format-agnostic —
// no other file under internal/core/routing/ imports
// internal/core/protocol). These constants MUST stay byte-equal to
// protocol.RouteAnnounceV3KindFull / KindDelta; the doc-comments on
// the protocol constants reference this contract.
const (
	routeAnnounceV3KindFull  = "full"
	routeAnnounceV3KindDelta = "delta"
)

// classifyDeltaMode derives the wire-format mode for a v1+ delta send.
// Both inputs must contain the FULL v2 triplet (CapMeshRoutingV1 +
// CapMeshRoutingV2 + CapMeshRelayV1 — see hasCapV2Triplet) for v2 to
// be picked; the same shape applies to v3 (hasCapV3Triplet). Relay is
// part of each triplet because the send-side dispatch in
// node.dispatchAnnouncePlaneFrameWithCaps gates SendRoutesUpdate /
// SendRouteAnnounceV3 on it, so a triplet-incomplete classification
// would route to a sender that refuses the frame with no legacy
// fallback. In production both inputs are equal: the announce loop
// reconciles AnnouncePeerState.capabilities to the per-cycle
// AnnounceTarget.Capabilities via UpdateCapabilities at the start of
// each per-peer goroutine, so by the time this function runs both
// arguments come from the same chosen target. The two-input shape and
// the deltaModeDivergence outcome are kept as a defensive net for any
// future caller that bypasses the cycle-time sync — see the doc comment
// on AnnounceLoop.announceToAllPeers for the reconciliation contract.
func classifyDeltaMode(stateCaps []PeerCapability, targetCaps []PeerCapability) deltaMode {
	stateV3 := hasCapV3Triplet(stateCaps)
	targetV3 := hasCapV3Triplet(targetCaps)
	stateV2 := hasCapV2Triplet(stateCaps)
	targetV2 := hasCapV2Triplet(targetCaps)
	// Pick the highest generation that BOTH cap snapshots agree on.
	// Disagreement on either v3 or v2 between the two snapshots is the
	// defensive "state changed underneath us" signal — fall through to
	// deltaModeDivergence so announceToAllPeers can MarkInvalid and
	// re-sync on the next cycle.
	switch {
	case stateV3 && targetV3:
		return deltaModeV3
	case stateV3 != targetV3:
		return deltaModeDivergence
	case stateV2 && targetV2:
		return deltaModeV2
	case !stateV2 && !targetV2:
		return deltaModeV1
	default:
		return deltaModeDivergence
	}
}

// hasCapV3Triplet reports whether the capability slice contains the
// FULL v3 wire triplet — CapMeshRoutingV1 + CapMeshRoutingV3 +
// CapMeshRelayV1. v3 without v1 is v1-absent (the legacy fallback any
// v3 negotiation falls back to in mixed-version pairs gates on v1 —
// see the CapMeshRoutingV3 doc-comment). v3 without relay is rejected
// because the send-side dispatch in node.dispatchAnnouncePlaneFrameWithCaps
// gates SendRouteAnnounceV3 on v1+v3+relay (same triplet the inbound /
// outbound receive dispatchers require), so a v3-without-relay
// classification here would let AnnounceLoop pick the v3 path, then
// the send-side gate would reject the frame with no legacy fallback
// — the snapshot would be silently dropped. This Round-20 alignment
// is the routing-package counterpart of the node-package
// peerSupportsRoutingV3 Round-19 fix.
func hasCapV3Triplet(caps []PeerCapability) bool {
	var v1, v3, relay bool
	for _, c := range caps {
		switch c {
		case domain.CapMeshRoutingV1:
			v1 = true
		case domain.CapMeshRoutingV3:
			v3 = true
		case domain.CapMeshRelayV1:
			relay = true
		}
	}
	return v1 && v3 && relay
}

// PeerSupportsV3 reports whether the cap snapshot includes the FULL
// v3 wire triplet (v1 + v3 + relay) — the v3 send-side admission gate
// used by sendFullAnnounce inside the routing package. Same semantic
// as hasCapV3Triplet but exported because the routing-package emit
// path goes through this single entrypoint.
//
// Note on ownership: the node-layer connect-time path
// (sendConnectTimeFullSync) does NOT call this helper. The node
// package owns a parallel helper, Service.peerSupportsRoutingV3
// (capabilities.go), that walks the session / inbound-conn cap
// snapshot directly because it needs read-locked access to peerMu
// state. Both helpers MUST agree on the same triplet contract —
// peerSupportsRoutingV3's doc-comment cross-references this one and
// the node-side test peer_supports_routing_v3_test.go pins the same
// positive/negative cases the routing-side test below does. A
// future change that loosens one helper must loosen the other in
// lockstep, otherwise the node-side connect-time dispatch and the
// routing-side cycle dispatch will disagree on what counts as v3-
// capable and silently drop frames at the layer boundary.
func PeerSupportsV3(caps []PeerCapability) bool {
	return hasCapV3Triplet(caps)
}

// hasCapV2Triplet reports whether the capability slice contains the
// FULL v2 wire triplet — CapMeshRoutingV1 + CapMeshRoutingV2 +
// CapMeshRelayV1. v2 without v1 is v1-absent (the first-sync
// invariant gates on v1). v2 without relay is rejected because the
// send-side dispatch in node.dispatchAnnouncePlaneFrameWithCaps
// gates SendRoutesUpdate on v1+v2+relay (same triplet the inbound /
// outbound receive dispatchers require), so a v2-without-relay
// classification here would let AnnounceLoop pick the v2 delta path,
// then the send-side gate would reject the frame with no legacy
// fallback — the snapshot would be silently dropped and the cache
// would stay stale across cycles. Round-21 alignment: mirrors the
// Round-20 hasCapV3Triplet fix for the v2 ladder; both ladders now
// agree with the production send-side dispatch contract.
func hasCapV2Triplet(caps []PeerCapability) bool {
	var v1, v2, relay bool
	for _, c := range caps {
		switch c {
		case domain.CapMeshRoutingV1:
			v1 = true
		case domain.CapMeshRoutingV2:
			v2 = true
		case domain.CapMeshRelayV1:
			relay = true
		}
	}
	return v1 && v2 && relay
}

// sendFullAnnounce sends a complete announce snapshot to the peer and
// updates cache state on success via thread-safe Record* methods.
//
// Forced-full / connect-time / periodic-forced-full picks the wire frame
// from peer capabilities:
//
//   - For peers that advertise the FULL v3 triplet (CapMeshRoutingV1 +
//     CapMeshRoutingV3 + CapMeshRelayV1 — see PeerSupportsV3 /
//     hasCapV3Triplet), the snapshot ships as route_announce_v3 with
//     kind="full" via sendV3Full. Relay is required because the send-
//     side dispatch in node.dispatchAnnouncePlaneFrameWithCaps gates
//     SendRouteAnnounceV3 on the same triplet — a v3-without-relay peer
//     would be classified as v3 here, get its frame refused at the
//     send-side gate, and silently drop the snapshot with no legacy
//     fallback. The v3 compact frame is self-contained (carries the
//     full table; no Origin field, single frame type for full and
//     delta), so kind="full" correctly serves as the connect-time /
//     forced-full baseline for v3-capable pairs without violating the
//     first-sync invariant that guards the legacy v1/v2 pair.
//   - Otherwise sendLegacyFull dispatches the legacy announce_routes
//     frame. See the "First-sync wire-frame invariant" section in
//     docs/routing.md for the v1/v2 normative contract: the first
//     full-sync — and every later forced full resync — to a peer that
//     can ONLY receive v1/v2 MUST be legacy, because v2 routes_update
//     cannot bootstrap and the peer has no baseline to diff against.
//
// Naming kept: sendLegacyFull is the only in-package call site that
// invokes PeerSender.SendAnnounceRoutes for full sync; do NOT add a
// branch on peer.Capabilities inside sendLegacyFull that picks
// SendRoutesUpdate. The v3 upgrade goes through sendV3Full instead.
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
	// empty-baseline intentionally emits neither legacy nor v2 nor v3 wire
	// frame — the relevant baseline flag stays false on purpose so the
	// next non-empty cycle re-emits a real full frame.
	if len(snapshot.Entries) == 0 {
		state.RecordFullSyncSuccess(snapshot, now)
		return
	}

	if PeerSupportsV3(peer.Capabilities) {
		if !a.sendV3Full(ctx, peer, snapshot) {
			log.Debug().
				Uint64("announce_cycle_id", cycleID).
				Str("peer_identity", string(peer.Identity)).
				Str("peer_address", string(peer.Address)).
				Int("routes", len(snapshot.Entries)).
				Msg("announce_v3_full_send_failed")
			return
		}
		// A non-empty v3 kind="full" frame went out — the peer has now
		// observed a v3 baseline in this session, so the v3 receive gate
		// in handleRouteAnnounceV3 is open and subsequent cycles may
		// pick v3 kind="delta".
		state.MarkWireBaselineV3Sent()
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

// sendV3Full is the v3-generation counterpart of sendLegacyFull: the
// single helper that invokes PeerSender.SendRouteAnnounceV3 with
// kind="full" for the forced-full / periodic-forced-full path. Lives
// here so future readers see one v3-full call site (mirroring
// sendLegacyFull's positioning) and any naming review tripwires for
// "do not internally upgrade SendAnnounceRoutes to v3" stay visible.
// epoch is captured from the local routing table — it ships on every
// v3 frame and lets the receiver detect a table-generation reset (see
// docs/protocol/route_announce_v3.md "Epoch handling").
func (a *AnnounceLoop) sendV3Full(
	ctx context.Context,
	peer AnnounceTarget,
	snapshot *AnnounceSnapshot,
) bool {
	return a.sender.SendRouteAnnounceV3(ctx, peer.Address, routeAnnounceV3KindFull, a.table.Epoch(), snapshot.Entries)
}

// sendLegacyFull sends a full snapshot via the legacy announce_routes wire
// frame. It is the v1/v2 half of the forced-full / connect-time /
// periodic-forced-full baseline path; sendFullAnnounce dispatches to it
// when the peer has NOT negotiated the v3 triplet, and to sendV3Full
// (route_announce_v3 with kind="full") when it has. Either way the
// dispatch produces a self-contained baseline frame — never a delta —
// per the "First-sync wire-frame invariant" section in docs/routing.md.
//
// Naming is deliberate: any future change that wants a v2 routes_update
// or v3 kind="delta" branch on full sync would have to either rename or
// bypass this helper, and both options are a loud review tripwire. The
// helper does not inspect peer.Capabilities — that decision lives one
// level up in sendFullAnnounce. The v2 delta path uses
// PeerSender.SendRoutesUpdate directly from sendIncrementalAnnounceV2;
// the v3 delta path uses PeerSender.SendRouteAnnounceV3 with
// kind="delta" from sendIncrementalAnnounceV3. Neither delta path goes
// through this helper.
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
// first-sync invariant guarantees the baseline was already delivered
// as a self-contained frame (legacy announce_routes for v1/v2 peers,
// route_announce_v3 with kind="full" for v3-triplet peers) via
// sendFullAnnounce → sendLegacyFull / sendV3Full. The HasSentWireBaseline
// gate in announceToAllPeers prevents this helper from firing until that
// baseline has actually gone on the wire.
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

// sendIncrementalAnnounceV3 sends the delta via the Phase 4 compact
// route_announce_v3 wire frame with kind="delta". Only picked by
// announceToAllPeers when classifyDeltaMode returns deltaModeV3 AND the
// peer has already received a v3 kind="full" baseline this session
// (HasSentWireBaselineV3) — the gate guarantees the receiver's
// baseline check in handleRouteAnnounceV3 accepts the delta. epoch is
// captured from the local routing table so the receiver can detect a
// table-generation reset (docs/protocol/route_announce_v3.md "Epoch
// handling").
//
// On success the cache is updated exactly like the v1 / v2 delta path:
// the full new snapshot is stored so the next cycle can compute a
// fresh diff. A send failure leaves the cache untouched; the next
// cycle retries with a recomputed delta.
func (a *AnnounceLoop) sendIncrementalAnnounceV3(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	fullSnapshot *AnnounceSnapshot,
	delta []AnnounceEntry,
	now time.Time,
) {
	if !a.sender.SendRouteAnnounceV3(ctx, peer.Address, routeAnnounceV3KindDelta, a.table.Epoch(), delta) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("delta_routes", len(delta)).
			Msg("announce_delta_send_failed_v3")
		return
	}
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}
