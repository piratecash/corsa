package node

import (
	mathrand "math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// first_hop_guards.go pins WHICH neighbours this node hands its own
// privacy-sensitive datagrams to, and keeps pinning the same ones.
//
// # Why pinning and not rotating
//
// The intuition is that a first hop should change often so nobody accumulates a
// history. It is wrong, and the wrongness is measured rather than argued. Tor
// walked this exact path and turned back after Øverlier & Syverson showed that a
// SINGLE hostile relay found a hidden service in minutes, simply by making the
// victim build new circuits until the right first hop came up.
//
// The arithmetic (Tor guard-spec): with an adversary holding k/N of the network,
// the fraction of compromised choices is F = (k/N)², and after C INDEPENDENT
// choices the probability of being caught at least once is 1 − (1 − F)^C. Rotation
// does not change the risk per choice. It changes the NUMBER of choices, and the
// cumulative probability rises monotonically towards one.
//
// It bites harder here than in Tor. Tor flips the coin when it builds a circuit;
// a datagram probe would flip it on every attempt, and there are dozens of
// attempts an hour. C grows orders of magnitude faster, so the limit is reached
// that much sooner.
//
// # What this does NOT do
//
// It does not hide anything. The chosen neighbour still sees `dst` and, on a
// session we dialled, our proven identity. Guards bound HOW MANY nodes ever
// learn that, not whether one does. Closing it needs onion routing, which is a
// separate document and a separate protocol.
//
// # The rules, and the failure each one avoids
//
//  1. A persistent SAMPLED set, capped by count and by a share of the observed
//     neighbourhood. The cap is the real bound on how many neighbours learn
//     anything about us over a long period.
//  2. A neighbour becomes CONFIRMED only when a frame actually went through it,
//     never when one was merely aimed at it. Tor: "keeps us from committing to a
//     guard before we actually use it for sensitive traffic."
//  3. A failure does NOT change the set. Tor's own third fix: clients dropped a
//     guard on ordinary network hiccups and thereby rotated exactly as much as
//     if they had rotated on purpose. Reconnects are normal life in a mesh, so a
//     failure arms a retry schedule and nothing else — and when OUR connectivity
//     is what failed, not even that.
//  4. Dates are randomised. The moment of a rotation is itself metadata, and it
//     correlates across nodes if everybody rotates on a round number.
//  5. Inbound neighbours are preferred when nothing else separates them: on a
//     session THEY dialled, our identity was never proven to them.
//
// Rule 5 is only half-effective today and the half that is missing is named
// here rather than hidden: this decides WHICH NEIGHBOUR carries the frame, and
// the send path separately decides WHICH CONNECTION to that neighbour it goes
// out on — where the outbound tier is preferred (peerSendableConnectionsLocked).
// So a neighbour we hold both connections to still receives the frame over the
// session where our identity is proven. Changing that tier order is a decision
// about all traffic, not about presence, and it belongs to whoever owns the
// send path.
//
// Reference: docs/protocol/presence.md §4.2.

const (
	// guardPrimaryCount is how many guards are kept hot. Tor's PRIMARY_GUARDS
	// is 3, of which one is actually used and the rest are a warm reserve for
	// the moment the first is unusable — which is what stops a single failure
	// from becoming a fresh choice.
	guardPrimaryCount = 3

	// maxSampledGuards bounds the set by count. Tor allows 60 against a
	// network of thousands; a mesh neighbourhood is orders of magnitude
	// smaller, and the number that matters is "how many neighbours ever learn
	// we are asking about somebody", so it is kept small deliberately.
	maxSampledGuards = 12

	// maxSampledFraction bounds the set by SHARE of the neighbourhood, which
	// is the bound that survives the network changing size. Tor uses 20% for
	// the same reason: a count-only cap is meaningless on a small network,
	// where 12 entries could be everybody we can see.
	maxSampledFraction = 0.2

	// guardLifetime is how long a sampled entry may stay before it is retired.
	// It is long on purpose — this is the parameter rotation would shorten,
	// and shortening it is the mistake the file header is about. It exists at
	// all so a set assembled on one network does not follow the node forever.
	guardLifetime = 60 * 24 * time.Hour

	// guardDateFuzz is the share of a lifetime that dates are randomised
	// within, matching Tor's RAND(now, LIFETIME/10).
	guardDateFuzz = guardLifetime / 10
)

// guardRetryLadder is how long a guard that failed is left alone before it is
// offered again. It never removes the guard — that is the whole point of rule 3
// — it only stops a dead neighbour from being retried on every probe.
//
// The last value is the ceiling: a guard that has been down for hours is still
// in the set, still ahead of every unsampled neighbour, and is still tried once
// an hour in case it comes back.
var guardRetryLadder = []time.Duration{
	time.Minute,
	10 * time.Minute,
	time.Hour,
}

// guardEntry is one sampled neighbour. Times are wall-clock because the set is
// durable and must survive a restart, which a monotonic reading does not.
type guardEntry struct {
	// Identity is the neighbour.
	Identity domain.PeerIdentity
	// SampledAt is when it entered the set, fuzzed.
	SampledAt time.Time
	// ConfirmedAt is when a frame FIRST actually went through it. Zero means
	// sampled but never used, and the distinction is rule 2: an entry that was
	// only ever aimed at has told nobody anything.
	//
	// It is a RECORD and never a rank. The value is fuzzed by up to a tenth of
	// the guard lifetime — six days — so ordering by it is ordering by a
	// random number: a spare confirmed a minute after the primary lands
	// earlier than the primary about half the time, and the primary never
	// comes back to the front after its back-off. That is an unintended
	// rotation produced by the anti-correlation measure itself. ConfirmedSeq
	// is what orders.
	ConfirmedAt time.Time
	// ConfirmedSeq is the confirmation ORDER: 1 for the first guard that ever
	// carried a frame, 2 for the next, and so on. Zero means unconfirmed.
	//
	// A counter rather than a timestamp because the property wanted here is
	// "who committed first", which must be exact and stable, while every
	// stored DATE in this file is deliberately imprecise. Persisted, because a
	// restart that renumbered would reorder the set.
	ConfirmedSeq uint64
	// RetryAt is when a failed guard may be offered again. Zero means it is
	// not in back-off.
	RetryAt time.Time
	// Failures counts CONSECUTIVE failures and indexes the retry ladder. A
	// successful use clears it.
	Failures int
	// Inbound records whether the connection at sampling time was one the
	// neighbour dialled to us. Kept for the sampling preference; it is a
	// snapshot of that moment, not a live reading.
	Inbound bool
}

// guardCandidate is one neighbour offered to the set: a peer the send path
// could hand a datagram to right now.
type guardCandidate struct {
	// Identity is the neighbour.
	Identity domain.PeerIdentity
	// ConnectedAt is the age of its oldest usable connection. Older is
	// preferred — the same stability bias the send path's own ranking uses.
	ConnectedAt time.Time
	// Inbound is true when at least one of its connections was dialled by
	// THEM. See rule 5.
	Inbound bool
}

// firstHopGuardPersister is what the set writes itself to. An interface so the
// policy above is testable without a filesystem, and so a node with no
// configured path can pass a no-op rather than the policy checking for one.
type firstHopGuardPersister interface {
	// Persist stores the whole set. Errors are the caller's to log: a set
	// that failed to reach disk is still correct for this process.
	Persist(entries []guardEntry) error
}

// firstHopGuards is the sampled set. Its mutex is a leaf and is never held
// across the persist, mirroring how the trust and intent stores write from a
// snapshot taken under their lock.
type firstHopGuards struct {
	clock     func() time.Time
	persister firstHopGuardPersister
	// fuzz returns a random offset in [0, d). Injected so the randomised
	// dates of rule 4 can be pinned in a test without pinning them in
	// production, where being predictable is the failure.
	fuzz func(d time.Duration) time.Duration

	mu      sync.Mutex
	entries []guardEntry
	// confirmSeq issues ConfirmedSeq. Seeded from the highest value on disk so
	// a restart continues the sequence instead of restarting it and putting
	// the next confirmation ahead of everything already confirmed.
	confirmSeq uint64
	// stats is the observable behaviour of the policy: which of its rules are
	// actually firing on a live node. See guardStats.
	stats guardStats
	// lastPrimary is the leading hop the previous Pick returned, so a CHANGE
	// can be counted. Not persisted: the count is about this process's
	// behaviour, and a restart is not a rotation.
	lastPrimary domain.PeerIdentity
}

// guardStats counts what the policy DOES, so it can be checked by observation
// rather than only by unit test.
//
// The plan for this subsystem says it is verified statistically, and that is
// not a stylistic preference: the properties here are "the first hop stopped
// changing" and "the set stopped growing", neither of which an assertion on a
// fixture can establish for a real network. Without these a silent failure —
// the primary rotating every hour, the set walking up to its cap, every probe
// leaving through a neighbour outside the set — looks exactly like success.
type guardStats struct {
	// Admitted counts neighbours ever taken into the sampled set.
	Admitted uint64
	// Confirmed counts guards that have carried a frame.
	Confirmed uint64
	// PrimaryChanges counts how often the FIRST preferred hop changed. On a
	// healthy node this stops at one; a number that keeps climbing is the
	// rotation this policy exists to prevent.
	PrimaryChanges uint64
	// BackOffs counts failures that armed a retry delay.
	BackOffs uint64
	// OutsideSetUses counts frames carried by a neighbour that was NOT in the
	// set at the time — the policy's own miss rate, and the number that says
	// whether the cap means anything.
	OutsideSetUses uint64
	// Retired counts entries dropped at the end of their lifetime.
	Retired uint64
}

func newFirstHopGuards(clock func() time.Time, persister firstHopGuardPersister, seed []guardEntry) *firstHopGuards {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	guards := &firstHopGuards{
		clock:     clock,
		persister: persister,
		// rand/v2 top-level source: this fuzz hides a schedule from a
		// correlating observer, it does not protect a secret.
		fuzz: func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return time.Duration(mathrand.Int64N(int64(d)))
		},
		entries: append([]guardEntry(nil), seed...),
	}
	for _, entry := range guards.entries {
		if entry.ConfirmedSeq > guards.confirmSeq {
			guards.confirmSeq = entry.ConfirmedSeq
		}
	}
	return guards
}

// Pick returns the first hops to prefer, most preferred first, and tops the set
// up from the live neighbourhood when it is short.
//
// The returned slice is the PRIMARY set: the guards that are usable right now,
// in the order rule 2 gives them. It can be empty, and an empty answer is not a
// failure — it means the ranking decides this send, which is what happens on a
// node that has just started and has no confirmed guard yet.
func (g *firstHopGuards) Pick(live []guardCandidate) []domain.PeerIdentity {
	now := g.clock()

	g.mu.Lock()
	changed := g.retireExpiredLocked(now)
	usable := liveByIdentity(live)
	if g.topUpLocked(live, usable, now) {
		changed = true
	}
	primary := g.primaryLocked(usable, now)
	g.notePrimaryLocked(primary)
	snapshot := append([]guardEntry(nil), g.entries...)
	g.mu.Unlock()

	if changed {
		g.persist(snapshot)
	}
	return primary
}

// NoteUsed records that a frame really went out through this neighbour. It is
// the only thing that CONFIRMS a guard (rule 2) and the only thing that clears
// a back-off.
//
// A neighbour that is NOT in the set is admitted here rather than ignored, and
// that is a correction of a real hole. The cap claims to bound how many
// neighbours ever learn we are asking about somebody; a hop that carried a
// frame has learned exactly that, so leaving it unrecorded made the stored set
// an understatement of who knows and the cap a number about bookkeeping rather
// than about exposure. It happens whenever no guard had a route to the
// destination — the preference is not a filter, so the ranking picks somebody
// else and the frame goes.
//
// When either cap is already reached such a neighbour cannot be recorded, and
// that is counted rather than swallowed: OutsideSetUses is the policy's own
// miss rate, and a node where it climbs has a stated bound that is not holding.
//
// `neighbourhood` is how many neighbours are usable right now, and it is passed
// in for the same reason the top-up takes it: the fractional cap is a statement
// about the CURRENT network, and a set that could not see it would enforce only
// the absolute number.
func (g *firstHopGuards) NoteUsed(identity domain.PeerIdentity, neighbourhood int) {
	if identity.IsZero() {
		return
	}
	now := g.clock()

	g.mu.Lock()
	index, found := g.indexOfLocked(identity)
	if !found {
		// BOTH caps, not just the absolute one. Admitting an outsider against
		// `maxSampledGuards` alone let the fractional cap be walked around
		// entirely: with ten neighbours the ordinary top-up stops at three,
		// and successful fallback hops could then grow the set to twelve —
		// more than the whole neighbourhood's 20 % share, which is exactly the
		// bound the public description promises.
		if g.roomLocked(neighbourhood) <= 0 {
			g.stats.OutsideSetUses++
			g.mu.Unlock()
			log.Warn().Str("next_hop", identity.String()).
				Msg("first_hop_guard_outside_full_set")
			return
		}
		g.entries = append(g.entries, guardEntry{
			Identity:  identity,
			SampledAt: now.Add(-g.fuzz(guardDateFuzz)),
		})
		g.stats.Admitted++
		g.stats.OutsideSetUses++
		index = len(g.entries) - 1
	}
	entry := g.entries[index]
	changed := entry.Failures != 0 || !entry.RetryAt.IsZero() || !found
	entry.Failures = 0
	entry.RetryAt = time.Time{}
	if entry.ConfirmedSeq == 0 {
		g.confirmSeq++
		entry.ConfirmedSeq = g.confirmSeq
		// Fuzzed like every other stored date: the confirmation moment is
		// itself metadata (rule 4). It is a record, not the rank — the
		// sequence number above is the rank.
		entry.ConfirmedAt = now.Add(-g.fuzz(guardDateFuzz))
		g.stats.Confirmed++
		changed = true
	}
	g.entries[index] = entry
	snapshot := append([]guardEntry(nil), g.entries...)
	g.mu.Unlock()

	if changed {
		g.persist(snapshot)
	}
}

// NoteUnusable records that a send through this guard did not happen.
//
// localConnectivity is the whole reason this takes an argument. Tor learned
// that a client whose own internet died would blame every guard in turn and
// rebuild its set from nothing — INTERNET_LIKELY_DOWN_INTERVAL exists for
// exactly that. When our own connectivity is gone, the guards have told us
// nothing about themselves and the set is not touched at all.
func (g *firstHopGuards) NoteUnusable(identity domain.PeerIdentity, localConnectivity bool) {
	if identity.IsZero() || !localConnectivity {
		return
	}
	now := g.clock()

	g.mu.Lock()
	index, found := g.indexOfLocked(identity)
	if !found {
		g.mu.Unlock()
		return
	}
	entry := g.entries[index]
	entry.Failures++
	entry.RetryAt = now.Add(guardRetryDelay(entry.Failures))
	g.stats.BackOffs++
	// The entry is NOT removed and NOT demoted. Rule 3: a failure is a reason
	// to wait, never a reason to choose again.
	g.entries[index] = entry
	snapshot := append([]guardEntry(nil), g.entries...)
	g.mu.Unlock()

	g.persist(snapshot)
}

// Entries returns a copy of the set. Read by the diagnostic RPC and by tests;
// nothing on a send path reads it.
func (g *firstHopGuards) Entries() []guardEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]guardEntry(nil), g.entries...)
}

// Inspect answers the diagnostic: the set, its counters, and which entries are
// currently eligible — all from ONE critical section, and WITHOUT changing
// anything.
//
// Both halves of that matter. Pick is a mutator: it retires expired entries,
// tops the set up from the live neighbourhood, moves counters and triggers a
// persist. A diagnostic built on it made the first request able to CREATE the
// rows it then reported (and report them beside `admitted: 0`, read a moment
// earlier), and made looking at the policy change the policy — the observer
// counting its own primary changes. A read-only twin costs a few lines and
// removes both.
func (g *firstHopGuards) Inspect(live []guardCandidate) ([]guardEntry, map[domain.PeerIdentity]struct{}, guardStats) {
	now := g.clock()
	usable := liveByIdentity(live)

	g.mu.Lock()
	defer g.mu.Unlock()
	eligible := make(map[domain.PeerIdentity]struct{}, guardPrimaryCount)
	for _, hop := range g.primaryLocked(usable, now) {
		eligible[hop] = struct{}{}
	}
	return append([]guardEntry(nil), g.entries...), eligible, g.stats
}

// Stats returns the counters. See guardStats for why they exist.
func (g *firstHopGuards) Stats() guardStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

// notePrimaryLocked records a change of the leading first hop.
//
// Only the FIRST entry is watched, because that is the one that carries the
// traffic; the other two are a warm reserve whose order changing costs nothing.
// A node whose count keeps rising is rotating, which is the failure the whole
// file is about — and it is invisible without this.
func (g *firstHopGuards) notePrimaryLocked(primary []domain.PeerIdentity) {
	var leader domain.PeerIdentity
	if len(primary) > 0 {
		leader = primary[0]
	}
	if leader == g.lastPrimary {
		return
	}
	// The very first choice is not a change: there was nothing to change from.
	if !g.lastPrimary.IsZero() {
		g.stats.PrimaryChanges++
		log.Debug().
			Str("from", g.lastPrimary.String()).
			Str("to", leader.String()).
			Msg("first_hop_guard_primary_changed")
	}
	g.lastPrimary = leader
}

// primaryLocked builds the ordered hot set: confirmed guards first in
// confirmation order, then sampled-but-unconfirmed in sampling order, skipping
// anything that is not live right now or is inside its back-off.
//
// Confirmed-first is what makes the choice STICK. A set that re-derived its
// order from whichever guard happened to look best this second would be a
// rotation with extra steps.
func (g *firstHopGuards) primaryLocked(usable map[domain.PeerIdentity]guardCandidate, now time.Time) []domain.PeerIdentity {
	eligible := make([]guardEntry, 0, len(g.entries))
	for _, entry := range g.entries {
		if _, live := usable[entry.Identity]; !live {
			continue
		}
		if !entry.RetryAt.IsZero() && now.Before(entry.RetryAt) {
			continue
		}
		eligible = append(eligible, entry)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		return guardEntryBefore(eligible[i], eligible[j])
	})
	if len(eligible) > guardPrimaryCount {
		eligible = eligible[:guardPrimaryCount]
	}
	out := make([]domain.PeerIdentity, 0, len(eligible))
	for _, entry := range eligible {
		out = append(out, entry.Identity)
	}
	return out
}

// guardEntryBefore orders the hot set: confirmed before unconfirmed, and
// confirmed entries among themselves in CONFIRMATION ORDER — the sequence
// number, never the stored date. The date is fuzzed by up to six days, so
// sorting on it silently re-orders the set every time a second guard is
// confirmed (see guardEntry.ConfirmedAt).
//
// Everything else compares EQUAL, and that is the point rather than an
// omission. The sort is stable and runs over g.entries in slice order, which is
// ADMISSION order — inbound first, then the oldest connection, as
// guardCandidateBefore decided when the entry was sampled. Re-deriving a rank
// here from the stored dates would silently discard that decision, and
// SampledAt in particular cannot serve as one: it is deliberately fuzzed by up
// to a tenth of the guard lifetime, so sorting on it would scramble the
// admission preference by days.
func guardEntryBefore(left, right guardEntry) bool {
	leftConfirmed := left.ConfirmedSeq != 0
	rightConfirmed := right.ConfirmedSeq != 0
	if leftConfirmed != rightConfirmed {
		return leftConfirmed
	}
	if leftConfirmed && left.ConfirmedSeq != right.ConfirmedSeq {
		return left.ConfirmedSeq < right.ConfirmedSeq
	}
	return false
}

// topUpLocked admits new neighbours until the hot set can be filled or a cap is
// reached. Reports whether the set changed.
//
// It runs only when there are not enough LIVE guards to fill PRIMARY, which is
// what keeps the set from growing on a healthy node: a working guard is never a
// reason to sample another one.
func (g *firstHopGuards) topUpLocked(live []guardCandidate, usable map[domain.PeerIdentity]guardCandidate, now time.Time) bool {
	if g.liveEligibleCountLocked(usable, now) >= guardPrimaryCount {
		return false
	}
	room := g.roomLocked(len(live))
	if room <= 0 {
		return false
	}

	fresh := make([]guardCandidate, 0, len(live))
	for _, candidate := range live {
		if candidate.Identity.IsZero() {
			continue
		}
		if _, sampled := g.indexOfLocked(candidate.Identity); sampled {
			continue
		}
		fresh = append(fresh, candidate)
	}
	sort.SliceStable(fresh, func(i, j int) bool {
		return guardCandidateBefore(fresh[i], fresh[j])
	})
	if len(fresh) > room {
		fresh = fresh[:room]
	}
	g.stats.Admitted += uint64(len(fresh))
	for _, candidate := range fresh {
		g.entries = append(g.entries, guardEntry{
			Identity: candidate.Identity,
			// Fuzzed backwards, never forwards: a future date would make the
			// entry sort after everything and look newly sampled forever.
			SampledAt: now.Add(-g.fuzz(guardDateFuzz)),
			Inbound:   candidate.Inbound,
		})
	}
	return len(fresh) > 0
}

// guardCandidateBefore ranks neighbours for ADMISSION to the set: inbound
// first (rule 5), then the oldest connection, then identity for determinism.
func guardCandidateBefore(left, right guardCandidate) bool {
	if left.Inbound != right.Inbound {
		return left.Inbound
	}
	if !left.ConnectedAt.Equal(right.ConnectedAt) {
		// A zero connectedAt means "unknown", and it must sort LAST rather
		// than first — the zero time is older than everything.
		if left.ConnectedAt.IsZero() {
			return false
		}
		if right.ConnectedAt.IsZero() {
			return true
		}
		return left.ConnectedAt.Before(right.ConnectedAt)
	}
	return left.Identity.Compare(right.Identity) < 0
}

// roomLocked is how many more entries both caps allow.
func (g *firstHopGuards) roomLocked(neighbourhood int) int {
	allowed := maxSampledGuards
	// The fractional cap only binds where it is the SMALLER of the two, and
	// it is floored at PRIMARY: on a node with three neighbours, 20% is zero
	// and a zero cap would mean no guards and therefore no policy at all.
	byFraction := int(float64(neighbourhood) * maxSampledFraction)
	if byFraction < guardPrimaryCount {
		byFraction = guardPrimaryCount
	}
	if byFraction < allowed {
		allowed = byFraction
	}
	return allowed - len(g.entries)
}

// liveEligibleCountLocked counts guards that could be used right now.
func (g *firstHopGuards) liveEligibleCountLocked(usable map[domain.PeerIdentity]guardCandidate, now time.Time) int {
	count := 0
	for _, entry := range g.entries {
		if _, live := usable[entry.Identity]; !live {
			continue
		}
		if !entry.RetryAt.IsZero() && now.Before(entry.RetryAt) {
			continue
		}
		count++
	}
	return count
}

// retireExpiredLocked drops entries past their lifetime. Reports whether
// anything went.
func (g *firstHopGuards) retireExpiredLocked(now time.Time) bool {
	kept := g.entries[:0]
	dropped := false
	for _, entry := range g.entries {
		if entry.SampledAt.IsZero() || now.Sub(entry.SampledAt) < guardLifetime {
			kept = append(kept, entry)
			continue
		}
		dropped = true
		g.stats.Retired++
	}
	g.entries = kept
	return dropped
}

func (g *firstHopGuards) indexOfLocked(identity domain.PeerIdentity) (int, bool) {
	for i, entry := range g.entries {
		if entry.Identity == identity {
			return i, true
		}
	}
	return 0, false
}

// persist writes a snapshot taken under the lock, from outside it — the store
// touches the filesystem, and no leaf mutex in this codebase is held across
// disk I/O.
func (g *firstHopGuards) persist(entries []guardEntry) {
	if g.persister == nil {
		return
	}
	if err := g.persister.Persist(entries); err != nil {
		// A set that did not reach the persister is still correct for this
		// process; the cost is one re-sampling after the next restart.
		log.Warn().Err(err).Msg("first_hop_guards_persist_failed")
	}
}

// guardRetryDelay maps a consecutive-failure count onto the ladder, holding at
// its last value.
func guardRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures > len(guardRetryLadder) {
		failures = len(guardRetryLadder)
	}
	return guardRetryLadder[failures-1]
}

func liveByIdentity(live []guardCandidate) map[domain.PeerIdentity]guardCandidate {
	out := make(map[domain.PeerIdentity]guardCandidate, len(live))
	for _, candidate := range live {
		if candidate.Identity.IsZero() {
			continue
		}
		out[candidate.Identity] = candidate
	}
	return out
}
