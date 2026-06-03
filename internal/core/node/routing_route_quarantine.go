package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// ---------------------------------------------------------------------------
// Per-peer route quarantine (Part 2 of probation + quarantine design)
// ---------------------------------------------------------------------------
//
// Background: docs/refactoring/route-withdrawal-grace-period.md
//
// What this does:
//   - Detects when a peer's session is unstable (disconnect rate
//     exceeds threshold).
//   - Marks that peer as "route-quarantined" for a cooldown window.
//   - While quarantined:
//       * Inbound routing announcements from the peer are dropped
//         (the peer's view of the network is not trusted).
//       * Transit routes learned through the peer are LOCALLY
//         invalidated at arm time (tombstoned, no wire withdrawals
//         — see invalidateTransitOnQuarantineLocked). Without this,
//         claims with a TTL longer than the quarantine (route TTL
//         120s vs 60s base cooldown) would silently become
//         selectable again when the quarantine expires — including
//         claims the peer tried to withdraw or correct DURING the
//         quarantine, when we were dropping its frames. Post-expiry
//         transit therefore resumes only from FRESH announcements.
//       * Any residual transit claims are additionally skipped by
//         the relay-forward path (we do not pick the peer as
//         next-hop for indirect destinations) — defence in depth
//         for claims admitted between the gate check and arm.
//       * The direct route to the peer itself stays in the routing
//         table — the peer is still reachable as itself, only its
//         transit knowledge is muted.
//       * The TCP session is NOT closed.
//       * Push / relay traffic targeting the peer keeps working.
//   - After the cooldown elapses, the quarantine clears and normal
//     processing resumes. If the peer re-triggers quarantine within
//     a recidivism window, the cooldown grows exponentially up to
//     a cap.
//
// IMPORTANT — what this DELIBERATELY does NOT do (and what changed
// from the pre-quarantine behaviour):
//
//   - Does NOT close the TCP session as a reaction to instability.
//     Pre-quarantine code closed the connection (directly or via
//     CM reconnect cycles) whenever a peer looked flaky, and the
//     close itself was a frequent source of more flap — fix-the-flap
//     attempts generated new flap. Quarantine breaks that loop: the
//     transport stays alive, the routing trust pauses.
//   - Does NOT call RemoveDirectPeer or fan out withdrawal frames
//     to neighbours. Our outbound seqno is not bumped, our
//     neighbours see no change in our routing snapshot for this
//     peer, and `routing_seqno_flap_hold_down` does not engage.
//   - Does NOT terminate ongoing data-plane work targeting the
//     peer (push_message, relay_message). The peer is still a
//     valid destination — we just stop believing what they tell us
//     about other destinations.
//
// In short: "I hear you as a direct peer, and I will keep writing
// to you, but I do not trust your view of the rest of the network
// for now". This is the principal departure from the pre-quarantine
// world, where instability triggered disconnect/reconnect cycles
// that themselves became the next instability signal.

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const (
	// quarantineDisconnectThreshold is the number of disconnect
	// events within quarantineDisconnectWindow that arms quarantine.
	// Sized so a single network blip + recovery does NOT trigger it
	// (one cycle = 2 events: disconnect + reconnect, except reconnect
	// inside the withdrawal grace window is silent and does not bump
	// the counter). Storm pattern (cm_session_setup_failed every
	// 2-3s) easily passes the threshold in tens of seconds.
	quarantineDisconnectThreshold = 4

	// quarantineDisconnectWindow is the sliding window over which
	// disconnects are counted. 60s catches a sustained storm without
	// false-positive on rare separated network glitches.
	quarantineDisconnectWindow = 60 * time.Second

	// quarantineBaseDuration is the cooldown on first trigger.
	quarantineBaseDuration = 60 * time.Second

	// quarantineMaxDuration caps the exponential growth for repeat
	// offenders.
	quarantineMaxDuration = 30 * time.Minute

	// quarantineRecidivismWindow is the window after a quarantine
	// expires during which a re-trigger doubles the duration. If the
	// peer stays calm for longer, the next trigger resets to base.
	quarantineRecidivismWindow = 10 * time.Minute

	// chattyAnnounceWindow is the sliding window over which inbound
	// announce-plane frames are counted toward the chatty_routes
	// quarantine trigger. Matches the design doc table
	// (docs/refactoring/route-withdrawal-grace-period.md): "Inbound
	// chatty routes | 50 announce/sec | 10s".
	chattyAnnounceWindow = 10 * time.Second

	// chattyAnnounceThreshold is the number of inbound announce
	// frames in chattyAnnounceWindow that arms chatty_routes
	// quarantine. Encodes the 50 announce/sec target rate over the
	// 10s window: 50 * 10 = 500 frames. Tuned so a legitimate
	// chunked full-sync (a few dozen frames at peer connect) is
	// well under the line, while a peer that re-announces every
	// few ms crosses it within a second of starting the flood.
	//
	// NOTE: the announce-route rate limiter (announce_ratelimit.go)
	// is route-based (routes/sec), not frame-based, so this is an
	// orthogonal signal: a peer can send 500 single-entry frames
	// per 10s without tripping the route-based limiter (which
	// would allow 200 ROUTES/sec = much higher entry budget) yet
	// still arm chatty_routes here. That's by design — many small
	// announce frames are the "chatty" pattern the doc targets.
	chattyAnnounceThreshold = 500

	// chattyReArmDebounce throttles the chatty_routes re-arm path
	// so a sustained flood does NOT bump Strikes / extend Until /
	// emit a Warn log on EVERY frame past the threshold. Without
	// this, the protection itself becomes a lock+log storm: armed
	// at frame 500, then armRouteQuarantineLocked fires on every
	// subsequent frame (501, 502, 503...) — each call acquires
	// peerMu in Write mode, bumps Strikes, calls nextStrikeCount
	// (which keeps returning prev+1 because LastArmed is fresh),
	// recomputes Until, and writes a zerolog.Warn under the lock.
	// At 50 announce/sec for one minute that is 3000 redundant
	// writes; strikes hit the duration cap (computeQuarantineDuration
	// max) within seconds, and the log/lock contention noise
	// dwarfs the original misbehaviour.
	//
	// Set to chattyAnnounceWindow so a sustained chatty peer
	// re-arms at most once per window. The first frame past the
	// threshold arms cleanly (no debounce on the first arm — see
	// shouldArmChattyLocked's no-entry / not-active branches);
	// subsequent re-arms are paced. Strikes still escalate over
	// time on a peer that refuses to quiet down, just bounded
	// (one bump per debounce instead of one per frame).
	chattyReArmDebounce = chattyAnnounceWindow
)

// ---------------------------------------------------------------------------
// State types (stored on Service, all guarded by s.peerMu)
// ---------------------------------------------------------------------------

// routeQuarantineEntry is the per-peer record for an active or
// recently-expired quarantine.
type routeQuarantineEntry struct {
	// Until is the wall-clock instant before which the peer's
	// routing input is suppressed. Zero means "not active right
	// now" (e.g. left only for recidivism tracking).
	Until time.Time

	// Reason is the trigger string for diagnostics
	// ("disconnect_storm", "setup_failure_cycle", "chatty_routes").
	Reason string

	// LastArmed is the most recent moment armRouteQuarantineLocked
	// promoted this entry to active. Used by the recidivism check
	// in computeQuarantineDuration.
	LastArmed time.Time

	// Strikes is the number of times quarantine has been armed
	// within quarantineRecidivismWindow. Resets to 1 if the peer
	// stayed calm for longer than the window.
	Strikes int
}

// ---------------------------------------------------------------------------
// Public API (called from receive-path and relay-forward gates)
// ---------------------------------------------------------------------------

// IsPeerInRouteQuarantine reports whether the peer's routing
// announcements are currently being dropped. Safe for concurrent
// readers — takes the peer-domain read lock.
//
// Callers:
//   - applyAnnounceEntries (drop inbound routing snapshots)
//   - tryForwardViaRoutingTable (skip quarantined peers as next-hop)
//   - applyRoutesUpdate / similar receive-path entry points
func (s *Service) IsPeerInRouteQuarantine(peer domain.PeerIdentity) bool {
	if peer == "" {
		return false
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.isPeerInRouteQuarantineLocked(peer, time.Now())
}

// isPeerInRouteQuarantineLocked is the lock-already-held variant.
// Used internally by helpers that already own peerMu.
func (s *Service) isPeerInRouteQuarantineLocked(peer domain.PeerIdentity, now time.Time) bool {
	entry, ok := s.peerQuarantine[peer]
	if !ok {
		return false
	}
	if entry.Until.IsZero() {
		return false
	}
	return now.Before(entry.Until)
}

// routeIsBlockedByQuarantine is the shared gate used by every relay
// next-hop selection site (tryForwardViaRoutingTable, TableRouter.Route).
// Returns true when the route MUST be skipped because the next-hop
// is route-quarantined as a transit peer.
//
// Direct destinations (hops == 1) ALWAYS pass — recipient is the
// quarantined peer itself, and quarantine deliberately suppresses
// transit trust without blocking data-plane delivery to the peer.
// See routing_route_quarantine.go's file-level docstring and the
// contract tables in docs/refactoring/route-withdrawal-grace-period.md.
//
// Sharing the gate across call-sites guarantees the
// "skip transit only" rule lives in one place — earlier passes
// added quarantine to TableRouter.Route without the matching gate
// in tryForwardViaRoutingTable, leaving a hole that this helper
// closes by construction.
func (s *Service) routeIsBlockedByQuarantine(nextHop domain.PeerIdentity, hops int) bool {
	if hops <= 1 {
		return false
	}
	return s.IsPeerInRouteQuarantine(nextHop)
}

// ---------------------------------------------------------------------------
// Detection (called from session lifecycle hooks)
// ---------------------------------------------------------------------------

// recordPeerDisconnectLocked appends a disconnect timestamp to the
// peer's sliding-window history and prunes entries older than
// quarantineDisconnectWindow. Caller must hold s.peerMu.
//
// Lazy-allocates the history map and per-peer slice on first use.
func (s *Service) recordPeerDisconnectLocked(peer domain.PeerIdentity, now time.Time) {
	if peer == "" {
		return
	}
	if s.peerDisconnectHistory == nil {
		s.peerDisconnectHistory = make(map[domain.PeerIdentity][]time.Time)
	}
	cutoff := now.Add(-quarantineDisconnectWindow)
	hist := s.peerDisconnectHistory[peer]
	pruned := hist[:0]
	for _, ts := range hist {
		if ts.After(cutoff) {
			pruned = append(pruned, ts)
		}
	}
	pruned = append(pruned, now)
	s.peerDisconnectHistory[peer] = pruned
}

// disconnectRateExceedsLocked returns true when the per-peer
// sliding window contains at least quarantineDisconnectThreshold
// events. Caller must hold s.peerMu.
func (s *Service) disconnectRateExceedsLocked(peer domain.PeerIdentity, now time.Time) bool {
	if peer == "" {
		return false
	}
	hist := s.peerDisconnectHistory[peer]
	if len(hist) < quarantineDisconnectThreshold {
		return false
	}
	// Defensive re-prune in case caller did not call record* recently.
	cutoff := now.Add(-quarantineDisconnectWindow)
	count := 0
	for _, ts := range hist {
		if ts.After(cutoff) {
			count++
		}
	}
	return count >= quarantineDisconnectThreshold
}

// armRouteQuarantineLocked places the peer into quarantine for a
// cooldown duration computed by computeQuarantineDuration. Caller
// must hold s.peerMu.
//
// Idempotent on already-active quarantine: if the entry's Until is
// in the future, ONLY the strike counter is bumped and the timer
// is re-armed from `now` so a sustained problem cannot let the
// quarantine elapse mid-storm.
//
// On the inactive→active transition the peer's transit claims are
// locally invalidated (invalidateTransitOnQuarantineLocked) so they
// cannot outlive the quarantine via TTL. Re-arms during an active
// quarantine skip the invalidation: inbound announcements are being
// dropped while quarantined, so no new transit claims can have been
// admitted since the transition.
func (s *Service) armRouteQuarantineLocked(peer domain.PeerIdentity, reason string, now time.Time) {
	if peer == "" {
		return
	}
	if s.peerQuarantine == nil {
		s.peerQuarantine = make(map[domain.PeerIdentity]routeQuarantineEntry)
	}
	newlyActive := !s.isPeerInRouteQuarantineLocked(peer, now)
	entry := s.peerQuarantine[peer]
	entry.Strikes = nextStrikeCount(entry, now)
	dur := computeQuarantineDuration(entry.Strikes)
	entry.Until = now.Add(dur)
	entry.LastArmed = now
	entry.Reason = reason
	s.peerQuarantine[peer] = entry

	log.Warn().
		Str("peer", string(peer)).
		Str("reason", reason).
		Dur("duration", dur).
		Int("strikes", entry.Strikes).
		Msg("route_quarantine_armed")

	if newlyActive {
		s.invalidateTransitOnQuarantineLocked(peer)
	}
}

// invalidateTransitOnQuarantineLocked tombstones every transit claim
// whose NextHop is the freshly quarantined peer. Caller must hold
// s.peerMu (lock order peerMu → routingTable.mu is the canonical
// one — see executeDeferredWithdrawal).
//
// Why this exists: the quarantine gate in isPeerInRouteQuarantineLocked
// only blocks while now < Until, but the route TTL (route claim
// lifetime, 120s) is longer than the base cooldown (60s). Without
// local invalidation, claims learned BEFORE the quarantine — stale
// by definition, and possibly withdrawn or corrected by the peer
// during the quarantine while we were dropping its frames — become
// selectable again the moment Until passes, for up to another TTL.
// Tombstoning at arm time guarantees post-quarantine transit resumes
// only from announcements received AFTER the quarantine expired.
//
// What this deliberately does NOT do (consistent with the file-level
// contract): no wire withdrawals, no seqno bump, no fanout, no
// TriggerUpdate. InvalidateTransitRoutes is the silent local variant
// (unlike RemoveDirectPeer) and does not touch the direct route —
// the peer itself stays reachable. Neighbours simply stop seeing the
// invalidated identities refreshed in our periodic announcements and
// age them out via their own TTL — no withdrawal storm.
//
// The route-table-changed event and the pending-queue drain for
// exposed backup routes run on a background goroutine: both touch
// other lock domains (event subscribers, deliveryMu) and have no
// ordering requirement against the quarantine state we just wrote.
func (s *Service) invalidateTransitOnQuarantineLocked(peer domain.PeerIdentity) {
	if s.routingTable == nil {
		// Minimal test fixtures build &Service{} without a table.
		return
	}
	invalidated, exposed := s.routingTable.InvalidateTransitRoutes(peer)
	if invalidated == 0 && len(exposed) == 0 {
		return
	}
	log.Info().
		Str("peer", string(peer)).
		Int("transit_invalidated", invalidated).
		Msg("route_quarantine_transit_invalidated")

	s.goBackground(func() {
		if invalidated > 0 && s.eventBus != nil {
			s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
				Reason:    domain.RouteChangeTransitInvalidated,
				PeerID:    peer,
				Withdrawn: invalidated,
			})
		}
		s.triggerDrainForExposed(exposed)
	})
}

// nextStrikeCount applies the recidivism rule: if the previous
// quarantine armed recently (within quarantineRecidivismWindow),
// strike count grows; otherwise it resets to 1.
func nextStrikeCount(entry routeQuarantineEntry, now time.Time) int {
	if entry.LastArmed.IsZero() {
		return 1
	}
	if now.Sub(entry.LastArmed) > quarantineRecidivismWindow {
		return 1
	}
	if entry.Strikes < 1 {
		return 1
	}
	return entry.Strikes + 1
}

// computeQuarantineDuration returns the cooldown for the given
// strike count. First strike = base; doubles per strike up to the
// cap.
func computeQuarantineDuration(strikes int) time.Duration {
	if strikes <= 1 {
		return quarantineBaseDuration
	}
	d := quarantineBaseDuration
	for i := 1; i < strikes; i++ {
		d *= 2
		if d >= quarantineMaxDuration {
			return quarantineMaxDuration
		}
	}
	return d
}

// maybeArmRouteQuarantineOnCloseLocked is the convenience entry
// point called from onPeerSessionClosed: it records the disconnect,
// checks the rate, and arms quarantine if exceeded. Caller must
// hold s.peerMu (the existing close-path already does).
func (s *Service) maybeArmRouteQuarantineOnCloseLocked(peer domain.PeerIdentity, now time.Time) {
	s.recordPeerDisconnectLocked(peer, now)
	if s.disconnectRateExceedsLocked(peer, now) {
		s.armRouteQuarantineLocked(peer, "disconnect_storm", now)
	}
}

// recordPeerAnnounceLocked appends an inbound-announce timestamp
// to the peer's sliding-window history, prunes ageouts from the
// front, and clamps the slice length to chattyAnnounceThreshold.
// Caller must hold s.peerMu.
//
// Bounded history (chattyAnnounceThreshold cap):
//
//	A sustained flood would otherwise grow the slice unboundedly
//	(rate × window timestamps) and turn every record call into an
//	O(n) prune scan PLUS the O(n) scan in announceRateExceedsLocked
//	— so each frame past the threshold gets MORE expensive even
//	after the debounce gates the arm-log path. For a 10k frames/s
//	flood on a single peer that is O(10k × 10s) = O(100k) per call,
//	a per-second cost that compounds with lock contention on
//	peerMu. Once we have threshold in-window timestamps, additional
//	timestamps don't change the boolean "exceeds threshold"
//	decision; the oldest can safely be dropped. Capping at
//	threshold turns the steady-state cost into O(1) per frame
//	(front-trim 0, append, copy-down by 1).
//
//	The 24-byte time.Time × 500 = ~12 KB worst case per peer is a
//	predictable memory footprint, and the ring-buffer-like
//	cap-eviction reuses the underlying array (copy then re-slice)
//	to keep allocations off the hot path.
//
// Pruning order:
//
//	Entries are inserted in monotonically-increasing time order
//	(callers pass `now` from time.Now or a deterministic test
//	anchor), so all expired entries form a prefix. Front-trim via
//	a single linear scan (drop = number of expired entries at the
//	head) is sufficient; we do NOT need to re-scan the tail or
//	allocate a fresh slice. Steady-state under flood: drop == 0,
//	append == O(1), cap-evict == O(1).
//
// Lazy-allocates the outer map and per-peer slice on first use.
// Symmetric to recordPeerDisconnectLocked but with the explicit
// upper bound — disconnect events are naturally rate-limited by
// session lifecycle so the same bound was not required there.
func (s *Service) recordPeerAnnounceLocked(peer domain.PeerIdentity, now time.Time) {
	if peer == "" {
		return
	}
	if s.peerAnnounceHistory == nil {
		s.peerAnnounceHistory = make(map[domain.PeerIdentity][]time.Time)
	}
	cutoff := now.Add(-chattyAnnounceWindow)
	hist := s.peerAnnounceHistory[peer]

	// Front-trim ageouts. Monotonic-time invariant lets us scan
	// the prefix only.
	drop := 0
	for drop < len(hist) && !hist[drop].After(cutoff) {
		drop++
	}
	if drop > 0 {
		hist = hist[drop:]
	}

	// Append the new event.
	hist = append(hist, now)

	// Bounded-cap eviction: keep only the most recent threshold
	// entries. copy-down preserves the underlying array (no
	// per-frame allocation) and re-slices the length.
	if len(hist) > chattyAnnounceThreshold {
		excess := len(hist) - chattyAnnounceThreshold
		copy(hist, hist[excess:])
		hist = hist[:chattyAnnounceThreshold]
	}

	s.peerAnnounceHistory[peer] = hist
}

// announceRateExceedsLocked returns true when the per-peer sliding
// window contains at least chattyAnnounceThreshold events. Caller
// must hold s.peerMu.
func (s *Service) announceRateExceedsLocked(peer domain.PeerIdentity, now time.Time) bool {
	if peer == "" {
		return false
	}
	hist := s.peerAnnounceHistory[peer]
	if len(hist) < chattyAnnounceThreshold {
		return false
	}
	cutoff := now.Add(-chattyAnnounceWindow)
	count := 0
	for _, ts := range hist {
		if ts.After(cutoff) {
			count++
		}
	}
	return count >= chattyAnnounceThreshold
}

// recordInboundAnnounceAndMaybeArm is the receive-handler entry
// point for the chatty_routes quarantine trigger. Called from
// handleAnnounceRoutes / handleRoutesUpdate / handleRouteAnnounceV3
// / handleRequestResync for every WELL-FORMED inbound frame from a
// known sender. The
// position is BEFORE the announceLimiter check, BEFORE the
// quarantine gate, and BEFORE peerState.GetOrCreate — so the
// signal counts every announce-plane frame the peer puts on the
// wire (matching the doc's "inbound announce_routes per second"
// definition).
//
// Lock contract: a single peerMu.Lock spans recordPeerAnnounceLocked,
// the shouldArmChattyLocked decision, and — when the gate allows —
// armRouteQuarantineLocked. armRouteQuarantineLocked emits its
// route_quarantine_armed Warn log INSIDE this critical section
// (see armRouteQuarantineLocked). That is acceptable here because
// the debounce gate below caps the rate of arm-and-log to at most
// one per chattyReArmDebounce per peer. The only frame-rate path
// is recordPeerAnnounceLocked itself (one map prune + append + a
// slice scan), which keeps the critical section short.
//
// Re-arm throttling: armRouteQuarantineLocked is idempotent on
// active quarantine — it bumps Strikes, re-arms Until, and writes
// a Warn log line. Calling it on EVERY frame past the threshold
// would turn the protection into a lock+log storm: at 50 frames/s
// for a minute that is 3000 redundant arm calls, each contending
// for peerMu in Write mode and writing a log line. shouldArmChattyLocked
// gates this: arm cleanly on the threshold crossing (no active
// quarantine, or active quarantine with LastArmed older than
// chattyReArmDebounce); skip otherwise. A peer that stays chatty
// continues to escalate Strikes, just paced to once per debounce
// window.
func (s *Service) recordInboundAnnounceAndMaybeArm(peer domain.PeerIdentity, now time.Time) {
	if peer == "" {
		return
	}
	s.peerMu.Lock()
	s.recordPeerAnnounceLocked(peer, now)
	if s.shouldArmChattyLocked(peer, now) {
		s.armRouteQuarantineLocked(peer, "chatty_routes", now)
	}
	s.peerMu.Unlock()
}

// shouldArmChattyLocked decides whether to invoke
// armRouteQuarantineLocked for the chatty_routes trigger on the
// current frame. Returns false in three cases:
//
//  1. Sliding window has not crossed chattyAnnounceThreshold
//     yet — no trigger at all.
//  2. Quarantine is already active for this peer AND less than
//     chattyReArmDebounce has passed since the last arm — this
//     is the throttle that bounds the per-frame storm to
//     one arm per debounce window.
//
// Returns true in two cases:
//
//	A. No existing quarantine entry (first arm), or entry exists
//	   but is fully expired AND outside the debounce — clean
//	   threshold crossing.
//	B. Active quarantine but LastArmed older than the debounce —
//	   sustained chatty peer "earns" the next strike bump.
//
// Note: the LastArmed check uses the unified entry across all
// quarantine reasons (disconnect_storm / setup_failure_cycle /
// chatty_routes). If a peer was JUST armed for disconnect_storm,
// the chatty trigger skips re-arming for a debounce window — the
// peer is already quarantined, and a second arm-pass over the
// same fresh state would only bump Strikes for no signal benefit.
// Caller must hold s.peerMu.
func (s *Service) shouldArmChattyLocked(peer domain.PeerIdentity, now time.Time) bool {
	if !s.announceRateExceedsLocked(peer, now) {
		return false
	}
	entry, exists := s.peerQuarantine[peer]
	if !exists {
		return true
	}
	if entry.LastArmed.IsZero() {
		return true
	}
	return now.Sub(entry.LastArmed) >= chattyReArmDebounce
}

// purgePeerAnnounceHistoryLocked drops history slices that contain
// no in-window events. Symmetric to purgePeerDisconnectHistoryLocked
// — a peer that goes silent forever leaves stale empty slices in
// the map without this sweep. Caller must hold s.peerMu.
func (s *Service) purgePeerAnnounceHistoryLocked(now time.Time) {
	if s.peerAnnounceHistory == nil {
		return
	}
	cutoff := now.Add(-chattyAnnounceWindow)
	for peer, hist := range s.peerAnnounceHistory {
		stillRelevant := false
		for _, ts := range hist {
			if ts.After(cutoff) {
				stillRelevant = true
				break
			}
		}
		if !stillRelevant {
			delete(s.peerAnnounceHistory, peer)
		}
	}
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

// purgeExpiredQuarantineLocked removes entries whose Until has
// passed AND whose LastArmed is older than quarantineRecidivismWindow.
// Entries with an expired Until but a recent LastArmed are kept so
// the strike counter survives — recidivism rule.
//
// Called opportunistically; not on a strict timer. Worst case: map
// grows by O(peers we ever saw), each entry is ~40 bytes, and the
// next recordPeer call still walks the per-peer history slice. Not
// a hot-path concern.
func (s *Service) purgeExpiredQuarantineLocked(now time.Time) {
	if s.peerQuarantine == nil {
		return
	}
	for peer, entry := range s.peerQuarantine {
		untilElapsed := entry.Until.IsZero() || now.After(entry.Until)
		recidivismCold := entry.LastArmed.IsZero() ||
			now.Sub(entry.LastArmed) > quarantineRecidivismWindow
		if untilElapsed && recidivismCold {
			delete(s.peerQuarantine, peer)
		}
	}
}

// purgePeerDisconnectHistoryLocked drops history slices that contain
// no in-window events. recordPeerDisconnectLocked already prunes
// the slice on every new event for the SAME peer, but a peer that
// stays disconnected forever leaves a stale slice in the map.
// Caller must hold s.peerMu.
func (s *Service) purgePeerDisconnectHistoryLocked(now time.Time) {
	if s.peerDisconnectHistory == nil {
		return
	}
	cutoff := now.Add(-quarantineDisconnectWindow)
	for peer, hist := range s.peerDisconnectHistory {
		stillRelevant := false
		for _, ts := range hist {
			if ts.After(cutoff) {
				stillRelevant = true
				break
			}
		}
		if !stillRelevant {
			delete(s.peerDisconnectHistory, peer)
		}
	}
}

// purgeLastResyncAcceptedLocked drops request_resync debounce stamps
// older than the debounce window — they can no longer influence the
// accept decision. Caller must hold s.peerMu.
func (s *Service) purgeLastResyncAcceptedLocked(now time.Time) {
	if s.lastResyncAccepted == nil {
		return
	}
	for peer, last := range s.lastResyncAccepted {
		if now.Sub(last) >= requestResyncAcceptDebounce {
			delete(s.lastResyncAccepted, peer)
		}
	}
}

// purgeRouteQuarantineState is the periodic-cleanup entry point
// wired from bootstrapLoop's eviction-sweep tick. Takes peerMu.Lock
// for a short window covering all four purge helpers — they touch
// the same domain and are O(map size), which is bounded by the
// number of peers we have ever observed.
func (s *Service) purgeRouteQuarantineState() {
	now := time.Now()
	s.peerMu.Lock()
	s.purgeExpiredQuarantineLocked(now)
	s.purgePeerDisconnectHistoryLocked(now)
	s.purgePeerAnnounceHistoryLocked(now)
	s.purgeLastResyncAcceptedLocked(now)
	s.peerMu.Unlock()
}
