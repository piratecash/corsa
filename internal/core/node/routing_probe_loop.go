// routing_probe_loop.go hosts the Phase 2 PR 11.3c probe sender —
// the goroutine that walks Questionable (Identity, Uplink) pairs on
// a HealthProbeInterval cadence and emits route_probe_v1 frames to
// the corresponding direct uplinks (overview §7.6,
// docs/protocol/route_health.md).
//
// Three concerns live here:
//
//   - probeRegistry: thread-safe map of outstanding probes. The
//     registry mints probe IDs, remembers (target, uplink, sentAt)
//     so the receive path can compute round-trip RTT, and arms a
//     HealthProbeTimeout watcher that fires MarkProbeFailure when
//     no ack arrives in time. The registry has its own mutex —
//     deliberately separate from routing.Table.t.mu — so the
//     sender critical section never spans wire I/O.
//   - Service.probeTick: one cadence pass. Snapshots health under
//     routing.Table.t.mu.RLock() (via HealthSnapshot), exits the
//     lock, then enumerates Questionable pairs and sends probes
//     through the standard sendFrameToIdentity helper. The Phase 0
//     overload gate is honoured: when overload is engaged, the
//     tick is a no-op (overview §4.7 Resolved decision #4 in the
//     phase file).
//   - Service.probeLoop: long-running goroutine started by
//     Service.Run, ticks every HealthProbeInterval until the
//     supplied context cancels.
//
// Architectural invariants from CLAUDE.md and phase-2 §2.5:
//
//   - Probe and probe_ack are P2P wire commands; they travel
//     through dispatchNetworkFrame (PR 11.3b), never through the
//     CommandTable. This file does NOT introduce any RPC surface.
//   - Wire sends go through sendFrameToIdentity (which funnels
//     into the per-connection designated writer). No conn.Write
//     call site is added.
//   - probeRegistry.mu is never held during the wire send
//     — Register returns the ID, sendFrameToIdentity runs unlocked,
//     the timeout watcher serialises through the mutex only on its
//     own deadline.
//   - The receive-side handleRouteProbeAck (defined in
//     routing_probe.go) resolves outstanding probes through the
//     same registry to get target+uplink+RTT before calling
//     routing.Table.MarkProbeAck — that thread-through replaces the
//     PR 11.3b stub.
package node

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// maxProbesPerTick bounds how many route_probe_v1 frames a single
// probeTick may emit. With HealthProbeInterval at 30s, a node holding
// many thousands of Questionable pairs would otherwise emit one
// thousands-frame burst every 30s; the budget spreads that across ticks
// so probe load is bounded by burst size as well as rate. Sized well
// above a healthy node's steady-state Questionable set.
const maxProbesPerTick = 64

// outstandingProbe records the bookkeeping for a probe that has been
// sent but whose ack has not yet arrived.
type outstandingProbe struct {
	// Target is the destination identity carried in the
	// route_probe_v1 frame.
	Target domain.PeerIdentity

	// Uplink is the direct peer to which the probe was sent —
	// also the key for the (Identity, Uplink) RouteHealthState
	// pair that will be updated on ack receipt.
	Uplink domain.PeerIdentity

	// SentAt is the wall-clock time when the probe entered the
	// write queue. The receive path computes RTT as
	// now − SentAt; clock injection for tests is via the
	// probeRegistry.now closure.
	SentAt time.Time

	// timer is the HealthProbeTimeout watcher. It fires
	// MarkProbeFailure if the ack does not arrive in time.
	// Cancelled by Resolve when the ack arrives first.
	timer *time.Timer
}

// probeRegistry owns the outstanding-probe map. Its mutex protects
// nextID + pending; the timeout watcher (time.AfterFunc) also takes
// the mutex to delete its own entry, so cancellation by Resolve is
// race-safe.
type probeRegistry struct {
	mu      sync.Mutex
	nextID  uint64
	pending map[uint64]*outstandingProbe

	// now is the clock used to stamp SentAt — injectable for tests.
	// Defaults to time.Now in newProbeRegistry.
	now func() time.Time

	// onTimeout is invoked when the HealthProbeTimeout watcher
	// fires without a matching ack. Set once in newProbeRegistry;
	// production callers pass routing.Table.MarkProbeFailure.
	onTimeout func(target, uplink domain.PeerIdentity)

	// timeout is the per-probe deadline. Production value is
	// routing.HealthProbeTimeout; tests can shorten it.
	timeout time.Duration
}

// newProbeRegistry builds a probeRegistry with the supplied timeout
// budget and timeout callback. now defaults to time.Now when nil;
// callers that need deterministic time pass an explicit closure.
func newProbeRegistry(timeout time.Duration, now func() time.Time, onTimeout func(target, uplink domain.PeerIdentity)) *probeRegistry {
	if now == nil {
		now = time.Now
	}
	if onTimeout == nil {
		onTimeout = func(domain.PeerIdentity, domain.PeerIdentity) {}
	}
	return &probeRegistry{
		pending:   make(map[uint64]*outstandingProbe),
		now:       now,
		onTimeout: onTimeout,
		timeout:   timeout,
	}
}

// Register reserves a probe ID for the (target, uplink) pair,
// stamps SentAt, and arms the timeout watcher. Returns the
// minted probe ID — the caller embeds it in the route_probe_v1
// frame before sending.
//
// The minted ID is always non-zero so that wire frames with
// probe_id=0 (the JSON default) are unambiguously distinguishable
// from a registered probe.
func (r *probeRegistry) Register(target, uplink domain.PeerIdentity) uint64 {
	r.mu.Lock()
	r.nextID++
	if r.nextID == 0 {
		r.nextID = 1
	}
	id := r.nextID
	op := &outstandingProbe{
		Target: target,
		Uplink: uplink,
		SentAt: r.now(),
	}
	op.timer = time.AfterFunc(r.timeout, func() {
		r.fireTimeout(id)
	})
	r.pending[id] = op
	r.mu.Unlock()
	return id
}

// fireTimeout is invoked by the AfterFunc watcher when the
// HealthProbeTimeout elapses without an ack. It atomically checks
// that the entry still exists (i.e., Resolve has not already
// removed it) and dispatches onTimeout outside the mutex so
// downstream calls to routing.Table.MarkProbeFailure do not nest
// the registry mutex inside t.mu.
func (r *probeRegistry) fireTimeout(probeID uint64) {
	r.mu.Lock()
	op, ok := r.pending[probeID]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.pending, probeID)
	target, uplink := op.Target, op.Uplink
	r.mu.Unlock()
	r.onTimeout(target, uplink)
}

// Resolve looks up an outstanding probe by ID and removes it from
// the pending map. Returns target, uplink, and the round-trip
// duration measured as r.now()−SentAt. The boolean is false when
// the probe ID is unknown — typically because the timeout fired
// first or the ack is a stray.
//
// Cancels the timeout watcher so MarkProbeFailure is not called
// for the same probe.
//
// Prefer ResolveMatching for ack ingest — Resolve is the
// uplink-agnostic variant kept for clean-removal call sites
// (timeout cancellation paths, eviction). The uplink-mismatch
// guard belongs on Resolve's caller path, NOT inside Resolve,
// so the timeout watcher (which has no senderIdentity to
// compare) keeps working unchanged.
func (r *probeRegistry) Resolve(probeID uint64) (target, uplink domain.PeerIdentity, rtt time.Duration, ok bool) {
	r.mu.Lock()
	op, exists := r.pending[probeID]
	if !exists {
		r.mu.Unlock()
		return domain.PeerIdentity{}, domain.PeerIdentity{}, 0, false
	}
	delete(r.pending, probeID)
	target, uplink = op.Target, op.Uplink
	rtt = r.now().Sub(op.SentAt)
	r.mu.Unlock()
	op.timer.Stop()
	return target, uplink, rtt, true
}

// ResolveMatching is the ack-ingest entry point: it resolves the
// outstanding probe only when expectedUplink matches the uplink
// the probe was originally sent to. Mismatch returns ok=false
// AND LEAVES the pending entry intact, so a malicious or buggy
// peer that guesses a monotonic probe_id cannot consume someone
// else's outstanding probe by replying with a spoofed ack — the
// real uplink's ack (or timeout) still applies.
//
// Atomicity: the match check and the delete happen inside one
// mutex acquisition, so a concurrent timeout fire cannot race
// against the resolve. The matched-path call equivalent to
// Resolve(probeID) but with the uplink predicate.
func (r *probeRegistry) ResolveMatching(probeID uint64, expectedUplink domain.PeerIdentity) (target domain.PeerIdentity, rtt time.Duration, ok bool) {
	r.mu.Lock()
	op, exists := r.pending[probeID]
	if !exists {
		r.mu.Unlock()
		return domain.PeerIdentity{}, 0, false
	}
	if op.Uplink != expectedUplink {
		// Mismatch: keep the entry, drop the ack. The real
		// uplink will resolve on its own ack arrival or its
		// timeout fire.
		r.mu.Unlock()
		return domain.PeerIdentity{}, 0, false
	}
	delete(r.pending, probeID)
	target = op.Target
	rtt = r.now().Sub(op.SentAt)
	r.mu.Unlock()
	op.timer.Stop()
	return target, rtt, true
}

// Cancel removes a pending probe by ID without invoking onTimeout.
// Used by sendProbe when the wire send fails after Register: a
// probe that never reached the peer must not produce a synthetic
// MarkProbeFailure when the timeout fires.
//
// No-op when the probe ID is unknown (timeout already fired and
// removed the entry, or never registered).
func (r *probeRegistry) Cancel(probeID uint64) {
	r.mu.Lock()
	op, ok := r.pending[probeID]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.pending, probeID)
	r.mu.Unlock()
	op.timer.Stop()
}

// HasOutstanding returns true when a probe for (target, uplink) is
// already in flight. Used by the sender to enforce the per-pair
// rate limit (no more than one outstanding probe per pair).
func (r *probeRegistry) HasOutstanding(target, uplink domain.PeerIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, op := range r.pending {
		if op.Target == target && op.Uplink == uplink {
			return true
		}
	}
	return false
}

// Len returns the number of outstanding probes. Test- and
// observability-only.
func (r *probeRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// probeLoop is the long-running goroutine that schedules probe
// sends on a HealthProbeInterval cadence. Started by Service.Run
// alongside the announce loop and TTL ticker. Returns when ctx
// cancels.
func (s *Service) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(routing.HealthProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.probeTick()
		}
	}
}

// probeTick runs one scheduling pass:
//
//  1. Age every tracked health state through the passive timeline
//     (Good→Questionable at 60 s idle, Questionable→Bad at 122 s,
//     and Bad→Dead at 182 s — but the Dead step only for pairs that
//     have been Confirmed at least once; never-confirmed pairs cap at
//     Bad, see routing.applyIdleTick). Without this step Good pairs
//     would never transition without an explicit probe failure or
//     hop_ack event, leaving the active probe path inert.
//  2. Snapshot the post-tick states and schedule a probe for
//     every Questionable pair that does not already have an
//     outstanding probe.
//
// Overload gate scope (review concern P2#2): the gate disables
// the probe-send step (#2) ONLY. Passive aging (#1) runs every
// tick regardless of overload because the documented contract
// for overload is "active probes paused; routes still age and
// converge to Bad/Dead via the slower passive hop_ack timeline".
// If aging itself paused under overload, stale Good routes would
// stay selectable precisely when the node is most degraded,
// defeating the graceful-degradation invariant the gate exists
// to provide.
//
// Routes accumulating failures during overload converge to Bad
// via the passive 122 s hop_ack timeline instead of the active
// probe path (3 × 30 s = 90 s) — graceful degradation by design.
// The same passive fallback covers pairs shed by the per-tick probe
// budget (maxProbesPerTick): see probeTick's send loop.
func (s *Service) probeTick() {
	if s.probeRegistry == nil || s.routingTable == nil {
		return
	}

	// Step 1 — passive aging. Runs unconditionally; the overload
	// gate covers only the probe send below. HealthSnapshot taken
	// after this is a deep copy and is unaffected by any
	// concurrent state-machine mutations that land between the
	// tick and the scan.
	s.routingTable.TickHealth()

	// Step 2 — probe send. Skipped when the overload gate is
	// engaged; the aging done in step 1 carries the state
	// machine forward on the passive timeline.
	if s.overloadMonitor != nil && s.overloadMonitor.IsOverloaded() {
		return
	}

	// Enumerate only the Questionable pairs: probeTick acts on nothing
	// else, and deep-copying the whole health set (HealthSnapshot) just to
	// filter it down was a dominant alloc_space source on dense nodes
	// (healthStore.snapshotLocked). QuestionableHealthTargets allocates
	// only for the Questionable subset.
	targets := s.routingTable.QuestionableHealthTargets()
	if len(targets) == 0 {
		return
	}
	// Per-tick probe budget: cap how many probes a single tick may put
	// on the wire so a node holding thousands of Questionable pairs
	// (large mesh, post-partition churn) cannot emit a thousands-frame
	// burst every HealthProbeInterval. Only frames that actually crossed
	// the wire (sendProbe == true) charge the budget, so capability-less
	// pairs do not starve capable ones.
	//
	// Invariant caveat: the fast active-detection path (3 × 30s = 90s to
	// Bad) holds only for pairs that are actually probed each tick — i.e.
	// when the live Questionable set is at or below maxProbesPerTick.
	// Beyond the budget, the EXCESS pairs are deferred and converge via
	// the slower passive hop_ack timeline (122s) instead — the same
	// graceful-degradation fallback the overload gate uses. Selection of
	// which pairs get deferred is best-effort (map iteration order), NOT
	// a stable round-robin: there is no fairness cursor, so a given pair
	// is not guaranteed a probe slot on any particular tick. This is an
	// accepted trade-off (active probing is an optimisation over the
	// always-present passive timeline); a stable round-robin cursor is a
	// possible future refinement if active coverage of very large
	// Questionable sets becomes important.
	sent := 0
	for _, tgt := range targets {
		if sent >= maxProbesPerTick {
			break
		}
		if s.probeRegistry.HasOutstanding(tgt.Identity, tgt.Uplink) {
			continue
		}
		if s.sendProbe(tgt.Identity, tgt.Uplink) {
			sent++
		}
	}
}

// sendProbe registers a probe ID for (target, uplink) and emits a
// route_probe_v1 frame to the uplink through sendFrameToIdentity.
//
// Capability gate (review feedback): the function bails out
// early when the uplink does not advertise
// mesh_route_probe_v1. Without this check we would Register an
// outstanding probe and then sendFrameToIdentity would silently
// skip the send (its own capability filter), leaving the timeout
// watcher to fire MarkProbeFailure on a probe the peer never
// received. That would penalise mixed-version peers for not
// negotiating an optional capability — directly contradicting
// the additive-capability invariant documented in
// docs/protocol/route_health.md
//
// When no capable connection exists, sendProbe returns without
// touching the registry. The pair will be re-evaluated at the
// next probeTick and continues to age through the passive hop_ack
// timeline (Good→Questionable→Bad→Dead) — that path remains
// unaffected by capability negotiation.
// sendProbe returns true when a route_probe_v1 frame actually crossed
// the wire (and an outstanding entry was registered), false on any
// bail-out (no capable uplink, marshal failure, send failure). The
// boolean lets probeTick charge its per-tick budget only for probes
// that genuinely went out.
func (s *Service) sendProbe(target, uplink domain.PeerIdentity) bool {
	// Capability check BEFORE Register. peerSendableConnectionsLocked
	// returns the same candidate set sendFrameToIdentity itself would
	// use, filtered by the required capability — so a bail-out here
	// means there is no capable session and sendFrameToIdentity
	// below would silently skip anyway. Without this short-circuit
	// the registry would arm a timeout that never sees an ack,
	// firing MarkProbeFailure on a probe the peer never received
	// (review concern P2#4).
	s.peerMu.RLock()
	candidates := s.peerSendableConnectionsLocked(uplink, domain.CapMeshRouteProbeV1, time.Now().UTC())
	s.peerMu.RUnlock()
	if len(candidates) == 0 {
		log.Debug().
			Str("target_identity", target.String()).
			Str("uplink", uplink.String()).
			Msg("route_probe_skipped_no_capability")
		return false
	}

	probeID := s.probeRegistry.Register(target, uplink)

	probe := protocol.RouteProbeFrame{
		Type:           protocol.RouteProbeFrameType,
		ProbeID:        probeID,
		TargetIdentity: target,
	}
	raw, err := protocol.MarshalRouteProbeFrame(probe)
	if err != nil {
		log.Warn().
			Err(err).
			Uint64("probe_id", probeID).
			Str("target_identity", target.String()).
			Str("uplink", uplink.String()).
			Msg("route_probe_marshal_failed")
		// Marshal failed AFTER Register — cancel the pending
		// entry so its timeout watcher does not later fire
		// MarkProbeFailure on a probe that never went on the wire.
		s.probeRegistry.Cancel(probeID)
		return false
	}
	frame := protocol.Frame{
		Type:    protocol.RouteProbeFrameType,
		RawLine: string(raw) + "\n",
	}
	// sendFrameToIdentity re-applies the capability gate. The
	// double check is defensive — production code may race
	// between the check above and the actual send, but the
	// session-capability snapshot is stable for the negotiated
	// lifetime of the session so a race is purely theoretical.
	//
	// Race window between the capability check above and the
	// actual send (queue full, conn nil, session torn down,
	// inbound write failed): sendFrameToIdentity returns false,
	// the peer never sees the probe, and a timeout would falsely
	// fire MarkProbeFailure (review concern P2#1). Cancel the
	// pending entry on send failure so the negative signal
	// reflects only probes that genuinely crossed the wire.
	if !s.sendFrameToIdentity(uplink, frame, domain.CapMeshRouteProbeV1) {
		s.probeRegistry.Cancel(probeID)
		return false
	}
	return true
}
