package node

import (
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// announceRateLimiter enforces per-peer rate limits on RECEIVED
// announce-plane frames (Phase 4 13.7, phase-4-compact-wire-signed.md
// §3.7). Mirrors relayRateLimiter (ratelimit.go) but is sized for
// announce traffic and keyed by peer IDENTITY (not transport address)
// so a misbehaving peer cannot reset its bucket by reconnecting on a
// fresh port.
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
type announceRateLimiter struct {
	mu      sync.Mutex
	buckets map[domain.PeerIdentity]*tokenBucket
}

// announceBurstRoutesPerPeer is the maximum number of route entries
// (across any combination of announce-plane frames) a peer may
// deliver in a burst before tokens replenish. Sized at 10,000 routes:
// covers a single full-sync of ~10K identities through the
// 100-route-per-frame chunker without throttling, while still
// cutting off a sustained flood within seconds. A mesh growing past
// this scale grows the constant as a one-line operator change.
//
// Control frames (request_resync, route_poison_v1) and announce
// frames with empty Entries each charge exactly 1 token, so the
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
// per-peer bucket entry is removed from the map (called by the
// existing cleanup loop on a sweep). Tuned long enough to survive a
// reconnect cycle so the abuse history persists across short
// outages, short enough that a long-departed peer's slot frees up.
const announceLimiterCleanupAge = 30 * time.Minute

func newAnnounceRateLimiter() *announceRateLimiter {
	return &announceRateLimiter{
		buckets: make(map[domain.PeerIdentity]*tokenBucket),
	}
}

// allow checks whether an incoming announce-plane frame from the
// given peer identity should be accepted. cost is the per-frame token
// charge expressed in route-entry units: announce-plane frames pass
// max(1, len(entries)) so a single large full-sync frame correctly
// drains proportional to its work; control frames
// (request_resync, route_poison_v1) and empty announce frames pass 1
// so they still consume the per-peer rate (and a flood of control
// frames is bounded by announceRefillRoutesPerSec). cost <= 0 is
// clamped to 1 so a buggy caller never silently bypasses the limiter.
//
// Returns true and decrements `cost` tokens on success. Returns false
// when the bucket holds fewer than `cost` tokens — the receive
// handler MUST drop the frame and log the throttle event. The bucket
// is NOT partially drained on rejection (an all-or-nothing reservation
// avoids a slow attacker draining the bucket without ever fitting a
// full frame).
func (rl *announceRateLimiter) allow(identity domain.PeerIdentity, cost int) bool {
	if identity.IsZero() {
		// No identity to key the bucket on (sentinel — receive
		// handlers reject empty senders anyway, but the rate limit
		// is defence-in-depth). Accept rather than block, so a
		// validation gate downstream surfaces the malformed-input
		// signal instead.
		return true
	}
	if cost < 1 {
		// Defensive clamp: a caller that passes 0 / negative cost
		// (forgot to count entries, off-by-one) would otherwise
		// silently bypass the limiter forever. Treating it as 1 keeps
		// the call accounted for; the canonical caller passes
		// max(1, len(entries)).
		cost = 1
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[identity]
	now := time.Now()
	if !ok {
		b = &tokenBucket{
			tokens:     announceBurstRoutesPerPeer,
			lastRefill: now,
		}
		rl.buckets[identity] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * announceRefillRoutesPerSec
	if b.tokens > announceBurstRoutesPerPeer {
		b.tokens = announceBurstRoutesPerPeer
	}
	b.lastRefill = now

	if b.tokens < float64(cost) {
		return false
	}
	b.tokens -= float64(cost)
	return true
}

// cleanup removes stale buckets for peers that haven't been seen
// recently. Mirrors relayRateLimiter.cleanup; the limiter's caller is
// responsible for invoking this periodically (the bucket map otherwise
// grows monotonically with the set of identities seen during the
// process lifetime).
func (rl *announceRateLimiter) cleanup(maxAge time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for id, b := range rl.buckets {
		if b.lastRefill.Before(cutoff) {
			delete(rl.buckets, id)
		}
	}
}
