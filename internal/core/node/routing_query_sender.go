// routing_query_sender.go hosts the Phase 2 PR 11.4c on-demand
// route_query_v1 fan-out. Unlike the probe sender (which runs on a
// periodic ticker), the query sender is driven by an explicit
// call from a caller that just discovered no usable route to a
// target identity — typically a relay path whose Lookup returned
// only Bad/Dead claims or an empty set.
//
// Two concerns live here:
//
//   - queryRateLimit: a per-target sliding-window guard that caps
//     route_query_v1 emission at queryFanOutLimit per target per
//     queryRateWindow. The cap protects neighbours from a single
//     node's misbehaving relay loop firing a query storm.
//   - Service.SendRouteQuery: the public fan-out entry point.
//     Picks up to queryFanOutLimit directly connected peers that
//     advertise mesh_route_query_v1, mints a unique query ID per
//     send, and emits route_query_v1 to each via the standard
//     sendFrameToIdentity helper. Returns the number of peers the
//     frame was enqueued to; zero means rate-limited or no capable
//     neighbours.
//
// Outstanding-query bookkeeping (query ID → target+sender) is
// deliberately absent because route_query_response_v1 ingest goes
// through the standard UpdateRoute admission pipeline — there is
// nothing to correlate beyond the wire frame itself. This keeps
// the sender lean.
//
// CLAUDE.md / phase-2 §2.5 invariants preserved:
//
//   - Wire-vs-RPC: SendRouteQuery is a Service-level helper used
//     by P2P wire paths (relay-failure-driven trigger). It does
//     NOT appear on the CommandTable surface; observability around
//     query activity will live on fetchRouteHealth (PR 11.5) where
//     queryRateLimit counters can be surfaced as a structured field.
//   - Single-writer: every send goes through sendFrameToIdentity
//     (capability-gated, funnels into the designated writer).
//   - Capability gating: sendFrameToIdentity itself filters peers
//     by required capability; the rate-limit guard runs upstream
//     so a burst of unrelated capability-mismatch failures does
//     not exhaust the window.
package node

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const (
	// queryFanOutLimit caps how many directly connected peers a
	// single SendRouteQuery call fans out to. Three balances
	// multi-path coverage (three independent answers give us a
	// quorum-like signal) against neighbour load.
	queryFanOutLimit = 3

	// queryRateWindow is the sliding window length for the
	// per-target rate limit. queryFanOutLimit emissions per window
	// is the production budget per phase-2 §4.4 (overview §7.5).
	queryRateWindow = 30 * time.Second
)

// queryRateLimit tracks per-target send timestamps and answers
// HasCapacity / Record under its own mutex. The mutex is disjoint
// from routing.Table.t.mu so Lookup-driven callers never nest
// locks while checking rate limits.
//
// The structure also owns a separate per-target "in-flight" set
// used by triggerRouteQueryAsync to cap concurrent spawn count
// independently of the emission budget. Spawn throttling and
// emission throttling are intentionally disjoint:
//
//   - Emission budget (emits + Record + HasCapacity) caps how
//     many route_query_v1 frames we put on the wire per target per
//     window, regardless of how many goroutines tried.
//   - Spawn throttling (inFlight + TryReserveInFlight +
//     releaseInFlight) caps how many concurrent goroutines exist
//     for the same target, regardless of the emission budget.
//
// Mixing the two budgets — using Record as both a spawn reservation
// AND an emission stamp — caused the PR 11.12 P1 regression: a
// burst of concurrent triggers could consume the entire emission
// budget on reservations alone, leaving SendRouteQuery to find
// HasCapacity=false and emit nothing. Keeping them disjoint lets
// SendRouteQuery's existing HasCapacity / Record contract stay
// untouched.
type queryRateLimit struct {
	mu sync.Mutex
	// emits maps target identity → recent send timestamps. Entries
	// older than queryRateWindow are trimmed lazily on every
	// Record / HasCapacity call.
	emits map[domain.PeerIdentity][]time.Time
	// inFlight marks targets for which a triggerRouteQueryAsync
	// goroutine is currently running. A trigger that finds the
	// flag already set returns without spawning, so a burst of N
	// concurrent triggers for the same target produces exactly one
	// goroutine. The flag is cleared by releaseInFlight in the
	// spawned goroutine's defer.
	inFlight map[domain.PeerIdentity]struct{}
	// now is the clock used for trimming; injectable for tests.
	now func() time.Time
	// window is the sliding-window length; injectable for tests
	// (production = queryRateWindow).
	window time.Duration
	// limit is the per-window emission cap; injectable for tests
	// (production = queryFanOutLimit).
	limit int
}

// newQueryRateLimit constructs a queryRateLimit with production
// defaults. Pass non-nil now to override the clock and non-zero
// window/limit to override defaults — used by tests that need
// deterministic timing or relaxed budgets.
func newQueryRateLimit(now func() time.Time, window time.Duration, limit int) *queryRateLimit {
	if now == nil {
		now = time.Now
	}
	if window <= 0 {
		window = queryRateWindow
	}
	if limit <= 0 {
		limit = queryFanOutLimit
	}
	return &queryRateLimit{
		emits:    make(map[domain.PeerIdentity][]time.Time),
		inFlight: make(map[domain.PeerIdentity]struct{}),
		now:      now,
		window:   window,
		limit:    limit,
	}
}

// TryReserveInFlight atomically tests-and-sets the in-flight flag
// for target. Returns true if the caller is the first concurrent
// trigger for target and may proceed to spawn a goroutine; false
// if another trigger is already in flight and the caller must
// return immediately.
//
// Callers that get true MUST eventually call releaseInFlight(target)
// — the spawned goroutine should defer the release.
//
// Spawn throttling lives here rather than as an atomic on Service
// because target-keyed state needs a map, and the rate limit's
// mutex already covers the same per-target slice in emits. Reusing
// the mutex keeps the locking contract simple: one mutex per
// queryRateLimit covers both emission stamps and in-flight flags.
func (r *queryRateLimit) TryReserveInFlight(target domain.PeerIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.inFlight[target]; ok {
		return false
	}
	r.inFlight[target] = struct{}{}
	return true
}

// releaseInFlight clears the in-flight flag for target. Must be
// called by the goroutine that previously got true from
// TryReserveInFlight, typically from a defer. The function is a
// no-op when the flag is not set, so double-release in odd
// shutdown paths is safe.
//
// Lowercase: only the spawning code path inside this package
// releases. External callers (tests, RPC) have no business
// flipping spawn state.
func (r *queryRateLimit) releaseInFlight(target domain.PeerIdentity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inFlight, target)
}

// IsInFlight is a test-only observer for the in-flight flag.
// Production paths use TryReserveInFlight / releaseInFlight; this
// helper lets tests assert "the flag was/was not set" without
// reaching into the unexported map.
func (r *queryRateLimit) IsInFlight(target domain.PeerIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.inFlight[target]
	return ok
}

// trimLocked drops entries older than r.window from emits[target].
// Caller must hold r.mu.
func (r *queryRateLimit) trimLocked(target domain.PeerIdentity, now time.Time) {
	cutoff := now.Add(-r.window)
	stamps := r.emits[target]
	idx := 0
	for ; idx < len(stamps); idx++ {
		if !stamps[idx].Before(cutoff) {
			break
		}
	}
	if idx == 0 {
		return
	}
	if idx >= len(stamps) {
		delete(r.emits, target)
		return
	}
	r.emits[target] = append([]time.Time(nil), stamps[idx:]...)
}

// HasCapacity reports whether at least one more emission for target
// fits inside the sliding window. The caller should check this
// before generating wire bytes — every HasCapacity call also trims
// expired stamps so a long-quiet target reclaims its budget
// automatically.
func (r *queryRateLimit) HasCapacity(target domain.PeerIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.trimLocked(target, now)
	return len(r.emits[target]) < r.limit
}

// Record stamps a send at now() for the given target. Returns true
// when the stamp fits inside the budget (and was recorded); false
// when the call would push the window past the limit (no stamp
// recorded, caller must NOT send).
func (r *queryRateLimit) Record(target domain.PeerIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.trimLocked(target, now)
	if len(r.emits[target]) >= r.limit {
		return false
	}
	r.emits[target] = append(r.emits[target], now)
	return true
}

// PendingCount returns the current emission count inside the window
// for target. Test- and observability-only.
func (r *queryRateLimit) PendingCount(target domain.PeerIdentity) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trimLocked(target, r.now())
	return len(r.emits[target])
}

// SendRouteQuery emits route_route_query_v1 to up to
// queryFanOutLimit directly connected peers that advertise
// mesh_route_query_v1. Returns the number of peers the frame was
// successfully enqueued to.
//
// Returns 0 when:
//
//   - the rate-limit window for target is full (queryFanOutLimit
//     emissions within queryRateWindow);
//   - no directly connected peer advertises mesh_route_query_v1;
//   - the Service was not wired with a queryRateLimit (defensive,
//     should not happen in production after NewService).
//
// Caller contract: invoke when local Lookup confirms there is no
// usable route to target (all uplinks Bad/Dead). The fan-out is
// intentionally narrow — three independent peer answers give
// receivers enough cross-validation under the standard Announcement
// admission rules, and the rate limit protects neighbours.
//
// Each peer gets a distinct query ID drawn from a monotonic counter;
// IDs are non-zero so receivers can distinguish from JSON-default 0.
func (s *Service) SendRouteQuery(target domain.PeerIdentity) int {
	// Identity validation: refuse to emit route_query_v1 for a
	// malformed target. RouteEntry.Validate only checks
	// non-empty, but the announce-plane (handleAnnounceRoutes)
	// uses identity.IsValidAddress to drop junk; the symmetric
	// guard belongs here too so a caller passing "*", whitespace,
	// or a non-hex string does not consume the per-target rate
	// budget for a query no responder will accept.
	if !identity.IsValidAddress(string(target)) {
		return 0
	}
	if s.queryRateLimit == nil {
		log.Warn().
			Str("target_identity", string(target)).
			Msg("route_query_skipped_no_rate_limit")
		return 0
	}
	if !s.queryRateLimit.HasCapacity(target) {
		return 0
	}

	// Collect capable direct peers under peerMu.RLock. We snapshot
	// the identities here and release the lock before any wire
	// I/O — the standard "no service-domain lock during send"
	// invariant from docs/locking.md.
	candidates := s.peersWithRouteQueryCap()
	if len(candidates) == 0 {
		return 0
	}
	if len(candidates) > queryFanOutLimit {
		candidates = candidates[:queryFanOutLimit]
	}

	// Record one stamp per peer we actually try to send to. We
	// account against the budget on best-effort basis: if send
	// fails (no usable connection) the stamp is still consumed,
	// which intentionally treats "tried and failed" the same as
	// "tried and succeeded" for rate-limit accounting. This
	// prevents a node with broken neighbours from rapidly retrying
	// against the same broken set.
	sent := 0
	now := time.Now().UTC()
	issuedAt := now.Format(time.RFC3339)
	for _, uplink := range candidates {
		if !s.queryRateLimit.Record(target) {
			break
		}

		queryID := s.nextRouteQueryID()
		query := protocol.RouteQueryFrame{
			Type:           protocol.RouteQueryFrameType,
			QueryID:        queryID,
			TargetIdentity: target,
			IssuedAt:       issuedAt,
		}
		raw, err := protocol.MarshalRouteQueryFrame(query)
		if err != nil {
			log.Warn().Err(err).
				Uint64("query_id", queryID).
				Str("target_identity", string(target)).
				Str("uplink", string(uplink)).
				Msg("route_query_marshal_failed")
			continue
		}
		frame := protocol.Frame{
			Type:    protocol.RouteQueryFrameType,
			RawLine: string(raw) + "\n",
		}
		if s.sendFrameToIdentity(uplink, frame, domain.CapMeshRouteQueryV1) {
			sent++
		}
	}
	return sent
}

// peersWithRouteQueryCap returns the identities of directly
// connected peers that advertise the FULL triplet required for a
// route_query round trip to be useful AND have a currently
// usable transport (connected health, not stalled). Both gates
// are applied:
//
//   - mesh_route_query_v1 — gates the query/response wire frames.
//   - mesh_relay_v1       — without it the peer cannot accept
//     relay_message frames addressed to us, so any route ingested
//     from its response would be data-plane-unusable.
//   - mesh_routing_v1     — required for transit forwarding
//     (Hops>1). Since route_query_response_v1 always ingests the
//     responder as a transit next-hop with Hops=BestHops+1, we
//     filter responder candidates by routing capability too.
//
// Health gate (PR 11.21 P2#2): the SendRouteQuery loop records
// one rate-limit stamp per peer it tries, BEFORE the actual
// sendFrameToIdentity attempt (`tried==spent` accounting — see
// the loop comment above Record). sendFrameToIdentity itself
// runs through peerSendableConnectionsLocked, which filters out
// sessions whose health is nil / disconnected / stalled. Without
// the same health gate here, a pre-activation or stalled
// triplet-capable session would appear as a query candidate,
// consume a budget slot, and then silently fail at the send
// layer — burning through the 30s per-target window without
// emitting anything. Aligning candidate discovery with the
// sendable contract closes that loop: a session needs both the
// capabilities AND a live health entry to count as a candidate.
//
// Filtering by all three caps at fan-out time mirrors the
// receive-side ingest guard (handleRouteQueryResponse) — a
// mixed-cap peer with only mesh_route_query_v1 could otherwise
// repeatedly refresh unusable routes that the relay path would
// have to drop when it tries to forward.
//
// Snapshot under peerMu.RLock, returned to caller without the
// lock — caller MUST NOT mutate the returned slice while it may
// still be observed concurrently.
func (s *Service) peersWithRouteQueryCap() []domain.PeerIdentity {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	seen := make(map[domain.PeerIdentity]struct{})
	out := make([]domain.PeerIdentity, 0, queryFanOutLimit)

	requiredCaps := []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	}
	now := time.Now().UTC()

	// Outbound sessions come first — they are the more stable
	// connections that AnnounceLoop prefers; route_query_v1 is
	// best answered by peers we keep open.
	for _, session := range s.sessions {
		if session == nil || session.peerIdentity == "" {
			continue
		}
		if !sessionHasAllCapabilities(session.capabilities, requiredCaps) {
			continue
		}
		if !s.peerSessionSendableLocked(session, now) {
			continue
		}
		if _, ok := seen[session.peerIdentity]; ok {
			continue
		}
		seen[session.peerIdentity] = struct{}{}
		out = append(out, session.peerIdentity)
		if len(out) >= queryFanOutLimit {
			return out
		}
	}

	// Inbound connections — secondary, only if we don't have
	// enough outbound capable peers yet. The callback returns
	// false to stop iteration: keep iterating while we still have
	// room under queryFanOutLimit.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity == "" {
			return true
		}
		if !sessionHasAllCapabilities(info.capabilities, requiredCaps) {
			return true
		}
		if !s.inboundConnSendableLocked(info, now) {
			return true
		}
		if _, ok := seen[info.identity]; ok {
			return true
		}
		seen[info.identity] = struct{}{}
		out = append(out, info.identity)
		// Continue while we still have capacity; stop once we've
		// hit the fan-out limit.
		return len(out) < queryFanOutLimit
	})

	return out
}

// peerSessionSendableLocked mirrors the health gate inside
// peerSendableConnectionsLocked for a single outbound session:
// the session is sendable iff it has a connected, non-stalled
// peerHealth entry under its (resolved) address. Caller MUST
// hold s.peerMu (R or W). Used by peersWithRouteQueryCap and
// hasAnyPeerWithRouteQueryCap so query-candidate discovery
// does not diverge from the actual send-path eligibility (PR
// 11.21 P2#2).
func (s *Service) peerSessionSendableLocked(sess *peerSession, now time.Time) bool {
	if sess == nil {
		return false
	}
	health := s.health[s.resolveHealthAddress(sess.address)]
	if health == nil || !health.Connected {
		return false
	}
	if s.computePeerStateAtLocked(health, now) == peerStateStalled {
		return false
	}
	return true
}

// inboundConnSendableLocked is the inbound-side counterpart of
// peerSessionSendableLocked. The check uses the inbound conn's
// own address — inbound entries do not run through dialOrigin
// remapping, so the lookup is direct.
func (s *Service) inboundConnSendableLocked(info connInfo, now time.Time) bool {
	health := s.health[s.resolveHealthAddress(info.address)]
	if health == nil || !health.Connected {
		return false
	}
	if s.computePeerStateAtLocked(health, now) == peerStateStalled {
		return false
	}
	return true
}

// hasAnyPeerWithRouteQueryCap is the early-exit companion to
// peersWithRouteQueryCap. Returns true on the FIRST session (or
// inbound conn) that advertises the full triplet; false only if
// the whole peer set has been scanned without a match.
//
// Used by triggerRouteQueryAsync's pre-check to short-circuit
// before spawning a goroutine that SendRouteQuery would
// immediately exit on len(candidates)==0. Without this guard, a
// mixed-version network with no triplet-capable neighbours would
// spawn one no-op goroutine per relay miss — PR 11.14 P3
// regression scenario.
//
// peersWithRouteQueryCap could be reused here, but its return
// value materialises up to queryFanOutLimit identities and a
// dedup map; the boolean variant skips both allocations and
// returns immediately on first hit. The two helpers stay
// consistent because they share the same capability list, the
// same health/sendable gate (PR 11.21 P2#2), and the same lock
// contract — diverging the filters would let the pre-check
// admit a session the real fan-out cannot use.
//
// Snapshot under peerMu.RLock; caller MUST NOT hold any other
// service-domain mutex.
func (s *Service) hasAnyPeerWithRouteQueryCap() bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	requiredCaps := []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	}
	now := time.Now().UTC()

	for _, session := range s.sessions {
		if session == nil || session.peerIdentity == "" {
			continue
		}
		if !sessionHasAllCapabilities(session.capabilities, requiredCaps) {
			continue
		}
		if !s.peerSessionSendableLocked(session, now) {
			continue
		}
		return true
	}

	found := false
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity == "" {
			return true
		}
		if !sessionHasAllCapabilities(info.capabilities, requiredCaps) {
			return true
		}
		if !s.inboundConnSendableLocked(info, now) {
			return true
		}
		found = true
		return false // stop iteration
	})
	return found
}

// sessionHasAllCapabilities returns true when every capability in
// required is present in caps. Linear scan inside the negotiated
// list (typically <10 entries) is cheap enough that a map is not
// worth the allocation.
func sessionHasAllCapabilities(caps []domain.Capability, required []domain.Capability) bool {
	for _, want := range required {
		if !sessionCapabilitiesContain(caps, want) {
			return false
		}
	}
	return true
}

// peerHasCapabilities checks whether the given peer identity has
// all required capabilities on at least one currently negotiated
// session (outbound preferred; falls back to inbound). Used by
// the route_query_response_v1 ingest path to reject responses
// from peers that lack the relay/routing caps required to make
// the ingested transit route usable.
//
// Snapshot under peerMu.RLock; caller MUST NOT hold any other
// service-domain mutex.
func (s *Service) peerHasCapabilities(peer domain.PeerIdentity, required ...domain.Capability) bool {
	if peer == "" || len(required) == 0 {
		return false
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	for _, session := range s.sessions {
		if session == nil || session.peerIdentity != peer {
			continue
		}
		if sessionHasAllCapabilities(session.capabilities, required) {
			return true
		}
	}

	found := false
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity != peer {
			return true
		}
		if sessionHasAllCapabilities(info.capabilities, required) {
			found = true
			return false // stop iteration
		}
		return true
	})
	return found
}

// sessionCapabilitiesContain checks whether the negotiated capability
// list contains the required capability. Linear scan is fine — the
// list never exceeds half a dozen entries.
func sessionCapabilitiesContain(caps []domain.Capability, want domain.Capability) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}

// nextRouteQueryID returns a fresh non-zero query ID. The counter
// is atomic so concurrent SendRouteQuery callers do not collide.
// The "skip zero" branch keeps wire frames with query_id=0 (the
// JSON default for a missing field) unambiguously distinguishable
// from a registered query.
func (s *Service) nextRouteQueryID() uint64 {
	id := s.queryIDCounter.Add(1)
	if id == 0 {
		return s.queryIDCounter.Add(1)
	}
	return id
}

// triggerRouteQueryAsync fires SendRouteQuery for the target in a
// background goroutine. Designed for the relay-side hot path
// (TableRouter.Route and tryForwardViaRoutingTable) which calls
// Lookup, gets either an empty result or only unusable claims,
// and wants to nudge route discovery without blocking the relay
// decision.
//
// Spawn throttling (PR 11.12 P1#1, supersedes PR 11.11 P3#5):
// the helper claims the per-target in-flight flag before
// spawning. Concurrent triggers for the same target that see the
// flag already set return immediately without spawning a
// goroutine, so a burst of N callers produces exactly one
// goroutine. The spawned goroutine clears the flag in a defer.
//
// Crucially, spawn throttling is SEPARATE from the emission
// budget. The previous design used Record as both a spawn
// reservation and an emission stamp, which caused a burst of
// concurrent triggers to consume the entire emission budget on
// reservations alone — the spawned goroutine then found
// HasCapacity=false in SendRouteQuery and emitted nothing. With
// the in-flight flag the emission budget stays untouched by
// triggerRouteQueryAsync; SendRouteQuery emits per its normal
// HasCapacity / Record contract.
//
// Sequential-churn guards. Two pre-checks run BEFORE the
// in-flight reservation so that a steady-state pathology
// (exhausted budget, or no triplet-capable neighbours in a
// mixed-version network) does not spawn a no-op goroutine per
// relay miss:
//
//   - PR 11.13 P3a (HasCapacity pre-check). When the per-target
//     emission budget is full, the spawned SendRouteQuery would
//     immediately return 0 via its own HasCapacity check and
//     release the in-flight flag, repeating goroutine churn
//     proportional to relay-miss rate. The pre-check returns
//     here so only one trigger per recovered emission slot
//     makes it past this point.
//
//   - PR 11.14 P3 (hasAnyPeerWithRouteQueryCap pre-check). When
//     no neighbour advertises the full route_query triplet
//     (mesh_route_query_v1 + mesh_relay_v1 + mesh_routing_v1),
//     SendRouteQuery would return 0 on len(candidates)==0 —
//     same churn pattern. The pre-check stops the spawn early.
//
// Both pre-checks have a narrow race window: another caller
// might consume the last emission slot, or the last triplet-
// capable session might disconnect, between the pre-check and
// the goroutine's actual emission attempt. In that case the
// spawned goroutine exits cleanly via its own no-op paths, and
// the in-flight flag is released — same behaviour as before.
// The pre-checks trade that single missed spawn (one per state
// change) for absence of churn (the high-cost pathology that
// the budget / peer-set is stuck in the "no" state for many
// consecutive misses).
//
// Safe to call from any goroutine and from any locking context —
// the goroutine itself does not hold the caller's stack frame
// and the SendRouteQuery body uses its own (disjoint) mutexes.
func (s *Service) triggerRouteQueryAsync(target domain.PeerIdentity) {
	// Reject malformed identity before the goroutine spawn —
	// the goroutine itself would bail out via SendRouteQuery's
	// own validation, but doing the check upfront keeps the
	// "no spawn for invalid input" invariant explicit and
	// avoids the goroutine-creation overhead on the relay hot
	// path.
	if !identity.IsValidAddress(string(target)) {
		return
	}
	if s.queryRateLimit == nil {
		return
	}
	// Cheap budget pre-check (PR 11.13 P3a). HasCapacity is a
	// single mutex-protected map lookup + trim — strictly cheaper
	// than spawning a goroutine that would immediately exit on
	// the same check inside SendRouteQuery.
	if !s.queryRateLimit.HasCapacity(target) {
		return
	}
	// No-capable-peers pre-check (PR 11.14 P3).
	// hasAnyPeerWithRouteQueryCap stops at the first triplet-
	// capable peer; in a healthy network with any capable
	// neighbour this is O(1). In a mixed-version network with
	// none, the cost is one scan of s.sessions + inbound conns
	// (~24 max in production), still strictly cheaper than the
	// alternative goroutine churn.
	if !s.hasAnyPeerWithRouteQueryCap() {
		return
	}
	// Atomic spawn-throttle: TryReserveInFlight is a tested-and-
	// set under the rate limit's mutex. Concurrent callers for
	// the same target see at most one true return until the
	// spawned goroutine releases the flag, so the goroutine count
	// for this target is bounded by 1 at any instant — strictly
	// tighter than the previous "consume one emission slot"
	// design and without leaking budget the goroutine still needs.
	if !s.queryRateLimit.TryReserveInFlight(target) {
		return
	}
	go func() {
		defer crashlog.DeferRecover()
		defer s.queryRateLimit.releaseInFlight(target)
		s.SendRouteQuery(target)
	}()
}
