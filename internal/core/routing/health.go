package routing

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// RouteHealth describes the active reachability state of a route to a
// specific identity via a specific direct-peer uplink. The state
// machine is driven by hop_ack timestamps and probe results.
//
// Health is a Phase 2 primitive (docs/protocol/route_health.md):
// it answers "is identity X currently reachable through uplink U?",
// which is a different question from peerHealth — "is the direct peer
// itself alive?". A direct peer can be peerHealth.Score>0 (TCP session
// alive, pongs flowing) while a specific route through that peer is
// Dead because the relay chain beyond the peer fell over.
type RouteHealth uint8

const (
	// HealthGood — uplink confirmed via recent hop_ack or
	// probe_ack(reachable=true). Route used normally, full TTL.
	HealthGood RouteHealth = iota

	// HealthQuestionable — no recent confirmation for this pair.
	// Probes are sent every HealthProbeInterval; route deprioritized
	// in CompositeScore but still selectable when no better path
	// exists.
	HealthQuestionable

	// HealthBad — unresponsive: no hop_ack for HealthBadAfter OR
	// ProbeFailures crossed HealthProbeFailureThreshold. Route
	// excluded from selection unless no other uplinks remain (last
	// resort before gossip fallback).
	HealthBad

	// HealthDead — fully timed out (no response for HealthDeadAfter).
	// Route locally invalidated and stop being announced. Wire
	// withdrawal sent only if this node is the origin of the route;
	// transit routes are silently dropped and converge via TTL expiry
	// or origin's own withdrawal — the zero-trust budget forbids
	// transit nodes from authoritatively withdrawing on origin's
	// behalf.
	HealthDead
)

// String returns the lower-case wire-format label used by RPC
// observability and structured logs.
func (h RouteHealth) String() string {
	switch h {
	case HealthGood:
		return "good"
	case HealthQuestionable:
		return "questionable"
	case HealthBad:
		return "bad"
	case HealthDead:
		return "dead"
	default:
		return "unknown"
	}
}

// State machine timings. The probe-driven Bad transition can fire
// before the passive hop_ack-driven one — 3 probe failures × 15 s
// probe cadence = 45 s, while passive hop_ack idle reaches Bad at
// 122 s. The asymmetry is intentional: active checks (probes) should
// detect failure faster than passive observation (hop_ack idle), so
// that an active prober gets a fast quality signal while a node that
// hasn't probed yet still has the slower passive timeline as a
// fallback. See docs/protocol/route_health.md.
const (
	// HealthQuestionableAfter — hop_ack idle period before
	// Good→Questionable.
	HealthQuestionableAfter = 60 * time.Second

	// HealthBadAfter — total hop_ack idle period before
	// Questionable→Bad via the passive timeline.
	HealthBadAfter = 122 * time.Second

	// HealthDeadAfter — total hop_ack idle period before Bad→Dead.
	HealthDeadAfter = 182 * time.Second

	// HealthProbeFailureThreshold — number of consecutive probe
	// failures (timeout or reachable=false) that transitions a
	// non-Good state to Bad regardless of the hop_ack timeline.
	HealthProbeFailureThreshold = 3

	// HealthProbeInterval — cadence at which Questionable pairs are
	// probed. Used by the Phase 2 probe sender (PR 11.3); declared
	// here so the state machine and the sender share one constant.
	HealthProbeInterval = 15 * time.Second

	// HealthProbeTimeout — per-probe send-to-ack budget. After this
	// elapses without a probe_ack, the pair gets one
	// applyProbeFailure tick. Hooked up by PR 11.3.
	HealthProbeTimeout = 5 * time.Second
)

// RouteHealthState tracks the active reachability of a
// (target identity, uplink peer) pair. Key shape is per-pair —
// adapted from the original iter-1.5 (Identity, Origin, NextHop)
// triple to match the per-uplink storage introduced in Phase 1
// (overview §4.1). Origin field was dropped from the routing storage
// dedup key, so per-Origin health was dropped along with it.
//
// Ownership: lives inside healthStore inside routing.Table under
// t.mu. Mutating methods (apply*) must be called with t.mu held in
// W mode; read methods may be called under R mode. The atomic.Pointer
// snapshot path used by fetchRouteHealth RPC reads via snapshotLocked
// under R mode and stores a deep copy outside the lock.
type RouteHealthState struct {
	// Identity is the destination identity this route targets.
	Identity domain.PeerIdentity

	// Uplink is the direct-peer fingerprint through which we learned
	// (or use) this route. The Phase 1 per-(Identity, Uplink) storage
	// guarantees each pair has at most one claim, so a single
	// RouteHealthState per pair is the natural shape.
	Uplink domain.PeerIdentity

	// Health is the current state-machine state. Transitions are
	// always paired with a TransitionAt update.
	Health RouteHealth

	// LastHopAck is the timeline reference used by applyIdleTick to
	// age the pair through Good→Questionable→Bad→Dead. It is
	// stamped at creation (ensureLocked) so the passive timeline has
	// a well-defined origin even before any confirmation arrives,
	// and re-stamped on every real confirmation (applyHopAck /
	// applyProbeAck(reachable=true)).
	//
	// Note: this field is NOT user-visible as a "last confirmation"
	// signal. The wire / RPC contract (docs/protocol/route_health.md
	// — last_hop_ack must be omitted for never-confirmed pairs) is
	// driven by the Confirmed flag below: fetchRouteHealth gates
	// emission of last_hop_ack on Confirmed==true, not on the
	// timestamp itself. Keeping the timer reference and the
	// "have we ever been confirmed?" semantic in separate fields
	// avoids the PR 11.15 / 11.16 regression where stamping
	// LastHopAck=zero for unconfirmed pairs forced applyIdleTick
	// into a TransitionAt-fallback that re-anchored on every
	// state transition.
	LastHopAck time.Time

	// Confirmed flips from false to true the first time the pair
	// receives positive evidence of reachability: a relay_hop_ack
	// (applyHopAck) or a route_probe_ack_v1 with reachable=true
	// (applyProbeAck). Negative evidence (probe timeouts,
	// probe_ack(reachable=false)) and passive idle ticks never
	// flip this back to false — once a confirmation is on record,
	// the historical fact persists for the lifetime of the entry.
	// Eviction (evictUplinkLocked) drops the whole state and
	// effectively resets Confirmed because a brand-new state is
	// created on the next admission.
	//
	// PR 11.15 P3 / 11.16: fetchRouteHealth uses this flag to
	// decide whether to emit last_hop_ack on the wire. Pairs
	// seeded by UpdateRoute (Announcement → Questionable) have
	// Confirmed=false until probe activity or real hop_ack
	// traffic arrives, so they correctly appear as never-confirmed
	// in operator tooling.
	Confirmed bool

	// LastProbe is the timestamp of the most recent probe
	// observation for this pair — either an ack arrival
	// (applyProbeAck) or a probe-failure tick (applyProbeFailure
	// for a timeout that elapsed without an ack). Send-side
	// timestamps live on the outstanding-probe registry
	// (probeRegistry.outstandingProbe.SentAt in
	// internal/core/node/routing_probe_loop.go) and are NOT
	// reflected here — the registry resolves them on ack ingest
	// to compute RTT, but the health state only records that an
	// observation arrived. fetchRouteHealth surfaces LastProbe so
	// operators can distinguish "no probe activity yet" (zero
	// value) from "probe activity is ongoing but no recent ack"
	// (non-zero but stale).
	LastProbe time.Time

	// ProbeFailures is the consecutive failure counter. Reset to
	// zero on any successful confirmation (hop_ack or
	// probe_ack(reachable=true)); incremented on probe timeout or
	// probe_ack(reachable=false). Crosses HealthProbeFailureThreshold
	// to force Bad even when LastHopAck is still recent.
	ProbeFailures int

	// RTT is the EWMA estimate of round-trip latency for this pair.
	// Updated by UpdateRTT (see score.go) using samples from hop_ack
	// turnaround (PR 11.2) and probe round-trip timing (PR 11.3).
	// Sender-measured, never trusts payload RTT supplied by the
	// remote — zero-trust budget §4.2.
	RTT time.Duration

	// TransitionAt is the wall-clock time of the most recent
	// state change. Used by RPC observability ("transitioned 12s
	// ago") and by debugging tooling.
	TransitionAt time.Time
}

// applyHopAck advances the state machine on relay_hop_ack reception
// for the (Identity, Uplink) pair. Always restores Good and resets
// ProbeFailures — hop_ack is the strongest passive confirmation.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyHopAck(now time.Time) {
	s.LastHopAck = now
	s.Confirmed = true // first real positive evidence; sticky thereafter.
	s.ProbeFailures = 0
	if s.Health != HealthGood {
		s.TransitionAt = now
		s.Health = HealthGood
	}
}

// applyProbeAck advances the state machine on route_probe_ack
// reception.
//
//   - reachable=true behaves like hop_ack: restore Good, reset
//     ProbeFailures, and refresh LastHopAck (the ack proves the
//     uplink is currently reachable for this target).
//   - reachable=false is negative evidence about the route, NOT
//     positive evidence about the uplink itself. ProbeFailures is
//     incremented; if it crosses HealthProbeFailureThreshold the
//     pair transitions to Bad. A pair in Bad is pulled back to
//     Questionable (active probe activity beats passive idle
//     bookkeeping). A pair in Dead STAYS Dead — review concern
//     P2#3: Lookup filters out Dead claims, and a "I can't reach
//     X through this uplink" answer should not silently re-enable
//     a route that was previously excluded from selection. Only
//     genuine reachability evidence (hop_ack or reachable=true
//     probe ack) can resurrect Dead.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyProbeAck(reachable bool, now time.Time) {
	s.LastProbe = now
	if reachable {
		s.LastHopAck = now
		s.Confirmed = true // first real positive evidence; sticky thereafter.
		s.ProbeFailures = 0
		if s.Health != HealthGood {
			s.TransitionAt = now
			s.Health = HealthGood
		}
		return
	}
	s.ProbeFailures++
	if s.Health == HealthDead {
		// Dead is sticky against negative evidence. The
		// counter still bumps so observability sees the
		// failure, but the state machine does not regress.
		return
	}
	if s.Health == HealthBad {
		s.TransitionAt = now
		s.Health = HealthQuestionable
	}
	if s.ProbeFailures >= HealthProbeFailureThreshold && s.Health != HealthBad {
		s.TransitionAt = now
		s.Health = HealthBad
	}
}

// applyProbeFailure advances the state machine on probe timeout — no
// route_probe_ack arrived within HealthProbeTimeout. Behaves the same
// as applyProbeAck(reachable=false): increments ProbeFailures and
// bumps LastProbe so observability can distinguish "no probe activity
// has ever been observed" (LastProbe zero) from "probes are being
// fired but none acks back" (LastProbe non-zero, no LastHopAck
// refresh). Dead remains sticky — see applyProbeAck's doc-comment for
// the rationale.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyProbeFailure(now time.Time) {
	s.LastProbe = now
	s.ProbeFailures++
	if s.ProbeFailures >= HealthProbeFailureThreshold && s.Health != HealthBad && s.Health != HealthDead {
		s.TransitionAt = now
		s.Health = HealthBad
	}
}

// applyIdleTick performs the periodic passive-timeline transition:
// 60 s of hop_ack idle promotes Good→Questionable, 122 s promotes to
// Bad, 182 s promotes to Dead. Probe-driven transitions are handled
// separately by applyProbeAck / applyProbeFailure.
//
// The transitions are evaluated in reverse-severity order (Dead first)
// because a single tick may have to skip Bad if the pair was sitting
// in Questionable for far too long without any tick in between.
//
// LastHopAck is the timeline reference and is ALWAYS non-zero for
// any state created by ensureLocked — the placeholder is stamped
// at creation time so the passive timeline measures "age since
// creation" until a real confirmation arrives. The user-visible
// "have we ever been confirmed?" distinction is carried by the
// separate Confirmed flag (see RouteHealthState doc-comment), so
// the RPC layer can omit last_hop_ack for never-confirmed pairs
// while the timer logic here stays simple and correct.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyIdleTick(now time.Time) {
	idle := now.Sub(s.LastHopAck)
	switch {
	case idle >= HealthDeadAfter && s.Health != HealthDead:
		s.TransitionAt = now
		s.Health = HealthDead
	case idle >= HealthBadAfter && s.Health != HealthBad && s.Health != HealthDead:
		s.TransitionAt = now
		s.Health = HealthBad
	case idle >= HealthQuestionableAfter && s.Health == HealthGood:
		s.TransitionAt = now
		s.Health = HealthQuestionable
	}
}

// healthKey identifies a tracked (Identity, Uplink) pair.
type healthKey struct {
	Identity domain.PeerIdentity
	Uplink   domain.PeerIdentity
}

// healthStore owns the RouteHealthState map. It is a leaf data
// structure with no own mutex — locking is provided by the owning
// routing.Table.t.mu. The "Locked" suffix on every method documents
// the lock-ownership contract: callers must hold t.mu in the
// indicated mode before invoking.
type healthStore struct {
	// states maps (Identity, Uplink) → state. Owned by the parent
	// Table; never accessed without t.mu held.
	states map[healthKey]*RouteHealthState
}

// newHealthStore returns an empty health store ready for use as a
// Table leaf.
func newHealthStore() *healthStore {
	return &healthStore{
		states: make(map[healthKey]*RouteHealthState),
	}
}

// isDeadLocked reports whether the (Identity, Uplink) pair is
// currently marked HealthDead. Returns false for absent entries
// (cold pairs / never-seen claims) and for any non-Dead label.
//
// Backs the Phase 2 announce-side suppression contract on the
// HealthDead constant: a Dead pair is filtered out of
// AnnounceProjectionFor / AnnounceableFor so locally-dead routes
// are no longer advertised to neighbours. The Lookup-side filter
// (table_lookup.go::Lookup) and this announce-side filter share
// the same per-pair signal — both consult the health store under
// t.mu without taking the lock themselves; callers must already
// hold t.mu in R mode.
//
// Returns false (not a Dead signal) when the parent healthStore
// is nil — defensive check that mirrors the lookupHealthLocked
// pattern used by the Lookup path.
func (s *healthStore) isDeadLocked(identity, uplink PeerIdentity) bool {
	if s == nil {
		return false
	}
	state := s.states[healthKey{Identity: identity, Uplink: uplink}]
	return state != nil && state.Health == HealthDead
}

// getLocked returns the state for the pair, or nil when no state is
// tracked yet. Callers must hold t.mu in R mode.
func (s *healthStore) getLocked(identity, uplink domain.PeerIdentity) *RouteHealthState {
	return s.states[healthKey{Identity: identity, Uplink: uplink}]
}

// ensureLocked returns the state for the pair, creating a fresh
// HealthGood entry stamped at `now` when none exists. Used by writer
// paths (hop_ack ingest, probe ack ingest, UpdateRoute admission)
// that need to upsert without a separate exists-check. Callers
// must hold t.mu in W mode.
//
// The fresh entry stamps LastHopAck=now so the passive-timeline
// machinery (applyIdleTick) has a well-defined idle origin. The
// "have we ever been really confirmed?" distinction is carried by
// the separate Confirmed flag: ensureLocked leaves it false, and
// only applyHopAck / applyProbeAck(reachable=true) flip it true.
// Wire / RPC layers (fetchRouteHealth — see
// internal/core/rpc/routing_commands.go) gate emission of
// last_hop_ack on Confirmed==true so a placeholder created by
// UpdateRoute (Announcement → Questionable, Confirmed=false) is
// correctly reported as never-confirmed.
//
// PR 11.15 P3 / 11.16 (regression history): an earlier iteration
// tried to leave LastHopAck=zero on fresh ensure and fall back to
// TransitionAt inside applyIdleTick. That broke the passive
// timeline because TransitionAt is rewritten on every transition,
// so a never-confirmed pair would re-anchor at each step and
// never escalate Questionable → Bad on the documented timeline.
// The Confirmed-flag split keeps applyIdleTick's reference stable
// (LastHopAck is monotonic-or-stays) and moves the user-visible
// "confirmed?" question to its own field.
func (s *healthStore) ensureLocked(identity, uplink domain.PeerIdentity, now time.Time) *RouteHealthState {
	k := healthKey{Identity: identity, Uplink: uplink}
	if state, ok := s.states[k]; ok {
		return state
	}
	state := &RouteHealthState{
		Identity:     identity,
		Uplink:       uplink,
		Health:       HealthGood,
		LastHopAck:   now,
		TransitionAt: now,
		// Confirmed left at false — fresh placeholder has no
		// positive evidence yet. applyHopAck /
		// applyProbeAck(reachable=true) flip it true on the
		// first real confirmation.
	}
	s.states[k] = state
	return state
}

// evictUplinkLocked drops the state for the (Identity, Uplink) pair.
// Invoked when the routeStore drops the corresponding UplinkClaim
// (single uplink withdrew its route to Identity, others remain).
// Callers must hold t.mu in W mode.
func (s *healthStore) evictUplinkLocked(identity, uplink domain.PeerIdentity) {
	delete(s.states, healthKey{Identity: identity, Uplink: uplink})
}

// evictIdentityLocked drops every state for the given identity.
// Invoked when the routeStore drops the identity entirely (last
// uplink withdrew). Iterates the full map; the operation is rare
// enough (only happens on full identity teardown) that O(N) cost is
// acceptable. Callers must hold t.mu in W mode.
func (s *healthStore) evictIdentityLocked(identity domain.PeerIdentity) {
	for k := range s.states {
		if k.Identity == identity {
			delete(s.states, k)
		}
	}
}

// lenLocked returns the number of tracked pairs. Test- and
// observability-only.
func (s *healthStore) lenLocked() int {
	return len(s.states)
}

// snapshotLocked returns a deep copy of every tracked state.
// Backs Table.HealthSnapshot, which is called synchronously per
// fetchRouteHealth RPC request (see Table.HealthSnapshot's
// doc-comment and docs/locking.md for the locking contract).
// There is no atomic.Pointer cache between this method and the
// RPC handler — the handler takes t.mu.RLock() on each request,
// walks the healthStore here, and releases. Returns nil for an
// empty store so callers can compare to nil cheaply.
//
// Callers must hold t.mu in R mode.
func (s *healthStore) snapshotLocked() []RouteHealthState {
	if len(s.states) == 0 {
		return nil
	}
	out := make([]RouteHealthState, 0, len(s.states))
	for _, state := range s.states {
		out = append(out, *state)
	}
	return out
}
