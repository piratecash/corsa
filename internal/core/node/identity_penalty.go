package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// identity_penalty.go is the ban score of identities proven by a v2 session
// on the loopback inbound class (loopback_inbound.go). There the source IP is
// shared by every onion peer and cannot be scored, and the connection is gone
// at the next reconnect; the proven identity is the one key that stays with
// the peer that misbehaved and with nobody else. Only a v2 proof reaches it:
// an identity a legacy hello claims is never charged.
//
// The score works as the IP score does — the same increments, threshold and
// ban length — so a violation costs a peer the same on either entry.

// identityPenaltyPolicy bounds the score and the store.
type identityPenaltyPolicy struct {
	threshold int
	banFor    time.Duration
	// idleAge is how long a score nobody added to is kept.
	idleAge time.Duration
	// capacity bounds the identities held. A fresh identity costs an
	// attacker nothing, so the store is bounded by count; at capacity the
	// unbanned entry scored longest ago gives way, and a ban never does.
	capacity int
}

// productionIdentityPenaltyPolicy is the policy a node runs with. Filling the
// store takes capacity v2 handshakes, each followed by a violation, all
// within the class's admission rate of ten per second: about seven minutes at
// least, for a few hundred kilobytes of memory.
func productionIdentityPenaltyPolicy() identityPenaltyPolicy {
	return identityPenaltyPolicy{
		threshold: banThreshold,
		banFor:    banDuration,
		idleAge:   banScoreIdleTTL,
		capacity:  4096,
	}
}

// identityPenalty is the score of one identity.
type identityPenalty struct {
	score      int
	lastScored time.Time
	banned     bool
	// bannedUntil is meaningful only while banned is set.
	bannedUntil time.Time
}

// identityPenaltyVerdict is what one charge did.
type identityPenaltyVerdict struct {
	score int
	// justBanned is set by the charge that crossed the threshold.
	justBanned bool
	banned     bool
	until      time.Time
	// unrecorded is set when the store was full of banned identities, so
	// the charge of a new one could not be kept: eviction never lifts a ban.
	unrecorded bool
}

// provenIdentityPenalties holds the scores. Its own leaf mutex: taken with no
// domain mutex held, nothing called under it. The gauges are atomics kept
// up to date on every change, so the diagnostics read them without the lock
// and no change walks the store.
type provenIdentityPenalties struct {
	policy identityPenaltyPolicy
	clock  func() time.Time

	mu      sync.Mutex
	entries map[domain.PeerIdentity]*identityPenalty
	// bannedCount is how many entries are banned; only banLocked and
	// deleteLocked change it.
	bannedCount int

	scoredGauge atomic.Int64
	bannedGauge atomic.Int64
}

func newProvenIdentityPenalties(policy identityPenaltyPolicy, clock func() time.Time) *provenIdentityPenalties {
	if clock == nil {
		clock = time.Now
	}
	return &provenIdentityPenalties{
		policy:  policy,
		clock:   clock,
		entries: make(map[domain.PeerIdentity]*identityPenalty),
	}
}

// charge adds delta to the score of id and bans it once the score reaches the
// threshold.
func (p *provenIdentityPenalties) charge(id domain.PeerIdentity, delta int) identityPenaltyVerdict {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock()
	entry, ok := p.entryLocked(id, now)
	if !ok {
		return identityPenaltyVerdict{score: delta, unrecorded: true}
	}
	entry.score += delta
	entry.lastScored = now
	verdict := identityPenaltyVerdict{score: entry.score}
	if entry.score >= p.policy.threshold && !entry.banned {
		p.banLocked(entry, now)
		verdict.justBanned = true
	}
	verdict.banned = entry.banned
	verdict.until = entry.bannedUntil
	return verdict
}

// entryLocked is the entry of id, created when there is none — at capacity in
// place of the unbanned entry scored longest ago. ok is false when every entry
// is banned: a ban is never lifted to make room. Caller holds p.mu.
func (p *provenIdentityPenalties) entryLocked(id domain.PeerIdentity, now time.Time) (*identityPenalty, bool) {
	if entry, ok := p.entries[id]; ok {
		return entry, true
	}
	if len(p.entries) >= p.policy.capacity {
		p.sweepLocked(now)
	}
	if len(p.entries) >= p.policy.capacity && !p.evictOldestUnbannedLocked() {
		return nil, false
	}
	entry := &identityPenalty{}
	p.entries[id] = entry
	p.publishGaugesLocked()
	return entry, true
}

// evictOldestUnbannedLocked drops the unbanned entry scored longest ago and
// reports whether there was one. Caller holds p.mu.
func (p *provenIdentityPenalties) evictOldestUnbannedLocked() bool {
	var (
		oldestID domain.PeerIdentity
		oldestAt time.Time
		found    bool
	)
	for id, entry := range p.entries {
		if entry.banned {
			continue
		}
		if !found || entry.lastScored.Before(oldestAt) {
			oldestID, oldestAt, found = id, entry.lastScored, true
		}
	}
	if found {
		p.deleteLocked(oldestID)
	}
	return found
}

// banLocked bans entry from now. Caller holds p.mu.
func (p *provenIdentityPenalties) banLocked(entry *identityPenalty, now time.Time) {
	entry.banned = true
	entry.bannedUntil = now.Add(p.policy.banFor)
	p.bannedCount++
	p.publishGaugesLocked()
}

// deleteLocked forgets id. Caller holds p.mu.
func (p *provenIdentityPenalties) deleteLocked(id domain.PeerIdentity) {
	entry, ok := p.entries[id]
	if !ok {
		return
	}
	if entry.banned {
		p.bannedCount--
	}
	delete(p.entries, id)
	p.publishGaugesLocked()
}

// isBanned reports whether id is banned now. An expired ban is forgotten
// together with its score, as an expired IP ban is.
func (p *provenIdentityPenalties) isBanned(id domain.PeerIdentity) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.entries[id]
	if !ok || !entry.banned {
		return false
	}
	if p.clock().Before(entry.bannedUntil) {
		return true
	}
	p.deleteLocked(id)
	return false
}

// sweep forgets expired bans and scores idle for longer than the idle age;
// bootstrapLoop runs it on every tick.
func (p *provenIdentityPenalties) sweep() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sweepLocked(p.clock())
}

// sweepLocked is sweep under p.mu.
func (p *provenIdentityPenalties) sweepLocked(now time.Time) {
	for id, entry := range p.entries {
		if p.expiredLocked(entry, now) {
			p.deleteLocked(id)
		}
	}
}

// expiredLocked reports whether an entry has nothing left to hold: a ban that
// ran out, or a score that was not added to for the idle age. Caller holds
// p.mu.
func (p *provenIdentityPenalties) expiredLocked(entry *identityPenalty, now time.Time) bool {
	if entry.banned {
		return !now.Before(entry.bannedUntil)
	}
	return now.Sub(entry.lastScored) >= p.policy.idleAge
}

// publishGaugesLocked stores the counts for the lock-free readers. Caller
// holds p.mu.
func (p *provenIdentityPenalties) publishGaugesLocked() {
	p.scoredGauge.Store(int64(len(p.entries)))
	p.bannedGauge.Store(int64(p.bannedCount))
}

// gauges are the identities scored and banned as of the last change or
// sweep. Lock-free.
func (p *provenIdentityPenalties) gauges() (scored, banned int) {
	return int(p.scoredGauge.Load()), int(p.bannedGauge.Load())
}
