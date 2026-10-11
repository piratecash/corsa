package node

import (
	"time"
)

// announceRateLimiter enforces per-peer rate limits on RECEIVED
// announce-plane frames (Phase 4 13.7, phase-4-compact-wire-signed.md
// §3.7). Mirrors relayRateLimiter (ratelimit.go) but is sized for
// announce traffic and keyed by penaltySubject (penalty_subject.go): the
// identity a v2 session proved, or — for a legacy session — the address
// this node dialled, the source IP, or the connection for loopback. None
// of these changes when a peer reconnects on a fresh source port, so a
// misbehaving peer cannot reset its bucket that way. The identity a legacy
// session merely NAMES is never the key: a bucket shared by every session
// claiming one identity let any legacy peer that named it spend the real
// owner's budget and get the owner's announcements dropped.
//
// Protected frames:
//   - announce_routes (legacy v1) / routes_update (v2) /
//     route_announce_v3 — every receive call to handleAnnounceRoutes /
//     handleRoutesUpdate / handleRouteAnnounceV3 charges
//     max(1, len(entries)) tokens against the bucket. Withdrawals
//     (Hops == HopsInfinity entries) live INSIDE these frames, so
//     the per-entry tokens also cover them.
//   - request_resync — charges 1 token. A peer that has exhausted its
//     announce budget should still be able to ask for a resync (or a
//     single request_resync per cycle wouldn't matter for DoS), but
//     we charge a token to keep the budget unified.
//   - route_poison_v1 — charges 1 token BEFORE the base64+ed25519
//     verify and the Table.InvalidateUplinkClaim mutation. Sharing
//     the announce bucket means a peer cannot flip from announce
//     frames to poison frames to dodge the throttle.
//   - route_poison_v2 (batched) — charges len(identities) tokens (one per
//     listed identity), so the batch is cost-equivalent to the v1 fan-out it
//     replaces and an authenticated peer gets no multiplicative bypass.
//
// Budgeting is by ROUTE COUNT, not by frame count (Round-10 fix). The
// earlier per-frame budget silently truncated legitimate chunked
// full-syncs: the send-side chunker caps at maxRoutesPerAnnounceFrame
// (= 100) routes per frame, so a full-sync of N routes ships as
// ceil(N/100) frames; the original 30-frame burst dropped frame 31
// and onward (~routes 3001+) while the sender — without any feedback
// channel for receive-side rate-limit drops — recorded the snapshot
// as delivered and stopped retrying until the next forced-full
// cadence. Charging by entry count makes the bound match the actual
// resource (per-entry trust classification + UpdateRoute admission
// work) and lets a single large full-sync clear in one burst.
//
// Behaviour on exhaustion: the frame is dropped at the receive-handler
// entry with a warn-log carrying sender identity, frame type, and the
// current bucket state. Storage is not touched; no event is published.
//
// Time is read only through the injected clock (the wall clock in
// production), so refill and stale-bucket cleanup can be driven
// deterministically in tests.
type announceRateLimiter struct {
	buckets *tokenBucketSet[penaltySubject]
	// idleAge is how long a bucket may go untouched before sweepIdle evicts
	// it. The constructor fixes it next to the bucket policy instead of the
	// sweep's caller choosing it, so "never swept sooner than a full refill"
	// is checked against exactly what production wires.
	idleAge time.Duration
}

// announceBurstRoutesPerPeer is the maximum number of route entries
// (across any combination of announce-plane frames) a peer may
// deliver in a burst before tokens replenish. Sized at 10,000 routes:
// covers a single full-sync of ~10K identities through the
// 100-route-per-frame chunker without throttling, while still
// cutting off a sustained flood within seconds. A mesh growing past
// this scale grows the constant as a one-line operator change.
//
// Control frames request_resync / route_poison_v1 and announce frames
// with empty Entries each charge exactly 1 token. route_poison_v2 is the
// exception: it charges len(identities) tokens (one per listed identity) so
// the batch is throttle-equivalent to the v1 fan-out it replaces. So the
// per-peer per-second control-frame ceiling is announceRefillRoutesPerSec.
const announceBurstRoutesPerPeer = 10000

// announceRefillRoutesPerSec is the number of announce tokens added
// per second per peer. 200 routes/s = 12,000/min sustained — well
// above the observed steady-state announce rate (one
// AnnounceProjectionFor pass per cycle, fed through the per-peer
// AnnounceTo delta, almost always < 100 entries/cycle on a healthy
// mesh) but far below a flood (a single CPU JSON-parses on the order
// of MB/s, so a true flood saturates well above this). A peer that
// has exhausted the burst replenishes at this rate; full burst
// refills in announceBurstRoutesPerPeer / announceRefillRoutesPerSec
// = 50 seconds.
const announceRefillRoutesPerSec = 200

// announceLimiterCleanupAge is the inactivity window after which a
// per-peer bucket entry is removed from the map by sweepIdle (run on
// every bootstrapLoop tick). Tuned long enough to survive a
// reconnect cycle so the abuse history persists across short
// outages, short enough that a long-departed peer's slot frees up.
// It must also stay at least as long as a drained bucket takes to
// refill (announceBurstRoutesPerPeer / announceRefillRoutesPerSec =
// 50 s): an eviction hands the peer a full bucket.
const announceLimiterCleanupAge = 30 * time.Minute

// newAnnounceRateLimiter builds a limiter whose buckets refill and age by
// clock. Production passes time.Now; nil falls back to it (see
// newTokenBucketSet).
func newAnnounceRateLimiter(clock func() time.Time) *announceRateLimiter {
	return &announceRateLimiter{
		buckets: newTokenBucketSet[penaltySubject](tokenBucketPolicy{
			burst:           announceBurstRoutesPerPeer,
			refillPerSecond: announceRefillRoutesPerSec,
		}, clock),
		idleAge: announceLimiterCleanupAge,
	}
}

// allow checks whether an incoming announce-plane frame charged to the
// given penalty subject should be accepted. cost is the per-frame token
// charge expressed in route-entry units: announce-plane frames pass
// max(1, len(entries)) so a single large full-sync frame correctly
// drains proportional to its work; request_resync / route_poison_v1 and
// empty announce frames pass 1, while route_poison_v2 passes
// len(identities) (one per listed identity), so they still consume the
// per-peer rate (and a flood of control frames is bounded by
// announceRefillRoutesPerSec). cost <= 0 is charged as 1 (by
// tokenBucketSet.take) so a buggy caller never silently bypasses the limiter.
//
// Returns true and decrements `cost` tokens on success. Returns false
// when the bucket holds fewer than `cost` tokens — the receive
// handler MUST drop the frame and log the throttle event. The bucket
// is NOT partially drained on rejection (an all-or-nothing reservation
// avoids a slow attacker draining the bucket without ever fitting a
// full frame).
func (rl *announceRateLimiter) allow(subject penaltySubject, cost int) bool {
	if subject.IsZero() {
		// No subject to key the bucket on (sentinel — receive
		// handlers reject empty senders anyway, but the rate limit
		// is defence-in-depth). Accept rather than block, so a
		// validation gate downstream surfaces the malformed-input
		// signal instead.
		return true
	}
	// A cost below 1 (a caller that forgot to count entries) is charged as
	// 1 by take; the canonical caller passes max(1, len(entries)).
	return rl.buckets.take(subject, cost)
}

// sweepIdle evicts buckets idle for longer than the limiter's idle age.
// Mirrors relayRateLimiter.sweepIdle; the limiter's caller is
// responsible for invoking this periodically (the bucket map otherwise
// grows monotonically with the set of identities seen during the
// process lifetime).
func (rl *announceRateLimiter) sweepIdle() {
	rl.buckets.evictIdle(rl.idleAge)
}
