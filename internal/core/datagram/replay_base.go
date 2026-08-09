package datagram

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// replay_base.go implements the layer's ONE anti-replay memory: the five-minute
// window every routed datagram lives in. Bounded, in-memory, no disk — a key
// here is a memory of "I have seen this frame", nothing more.
//
// The split between Has and Reserve is the same one that protects the file
// command nonce cache (node.nonceCache): Has is a read-only early filter
// that takes nothing, Reserve is the atomic check-and-reserve that runs
// immediately before the first mutating operation. Inserting on the early
// probe would let an unauthenticated frame poison the cache and suppress the
// genuine one; hence a key is only ever taken after the frame is proven
// authentic and deliverable (§4.1).
//
// The cache keeps its records in TWO indexes beside the key map, and each of
// them answers exactly one question of §5 that the other cannot:
//
//   - an EXPIRY HEAP ordered by the moment a record may be deleted, so "is
//     anything removable" costs O(1) whatever the insertion order was;
//   - a per-OWNER age list, so "the oldest disposable record of the noisiest
//     neighbour" is found by walking that neighbour's own records and nobody
//     else's.
//
// Both replaced a bounded walk over one global insertion-ordered list, and both
// defects that walk had were anti-replay defects rather than slow paths — see
// sweepExpiredLocked and evictOldestOfLocked.
//
// Reference: docs/refactoring/datagram-transport.md §2.2, §4.1, §5.

const (
	// DefaultBaseReplayCapacity bounds the live entry count. Matched to the
	// file-command nonce cache: both hold five minutes of authenticated
	// frames from all neighbours at once.
	DefaultBaseReplayCapacity = 10_000

	// baseHeldReservationFallbackGrace is used only if the class table ever
	// stops answering; the base replay window is the conservative reading.
	baseHeldReservationFallbackGrace = domain.DatagramBaseReplayWindow

	// baseSweepRemovalBudget bounds the work ONE Reserve may spend freeing
	// room. The cache is on the receive path of every datagram, so cleanup has
	// to stay O(1) amortised: capacity is the hard bound, the pass inside
	// Reserve is best-effort, and a full pass is available to the owner of the
	// cache as SweepExpired.
	//
	// The budget counts records REMOVED and not records inspected, and that
	// distinction is the whole fix: a bounded pass that removes nothing now
	// proves that nothing was removable, because the heap it walks is ordered by
	// exactly that. The previous budget counted inspections over an
	// insertion-ordered list, so a full cache whose oldest 128 records were live
	// answered `rejected` to every neighbour while expired records sat behind
	// them.
	baseSweepRemovalBudget = 128
)

// BaseReplayCacheConfig configures the base cache. Everything the type needs
// lives in one struct rather than a chain of setters, so a call site shows
// all of it at once (CLAUDE.md).
type BaseReplayCacheConfig struct {
	// Clock is the injectable time source, following the project
	// convention (routing.Table, node.rotatingHashDedup). Defaults to
	// time.Now.
	Clock func() time.Time

	// Capacity is the maximum number of live entries. Defaults to
	// DefaultBaseReplayCapacity.
	Capacity int
}

// BaseReplayCacheMetrics is the counter set of the base cache. Overflow in
// particular has to be observable: a cache that silently refuses traffic
// looks exactly like a network that lost it (§5, §9).
type BaseReplayCacheMetrics struct {
	Reserved          uint64
	Duplicates        uint64
	Committed         uint64
	Released          uint64
	StaleReleases     uint64
	RejectedCapacity  uint64
	RejectedNoisyPeer uint64
	EvictedNoisyPeer  uint64
	ExpiredSwept      uint64
	// AbandonedReservations counts reservations the watchdog reclaimed: a
	// branch that never reached Commit or Release. It is separate from
	// ExpiredSwept because the two mean different things to an operator — one
	// is the cache doing its job, the other is a lost pipeline branch.
	AbandonedReservations uint64
}

// baseHeldReservationGrace is how long past replay_until an UNCOMMITTED
// reservation may still belong to a live operation: the whole hop budget of
// the slowest class — queue residence plus write grace (§4.2). After it the
// frame is long dead by Validity and no operation over this key can still be
// running, so the record is a lost branch rather than a quarantine.
//
// The cache needs this watchdog because nothing else will ever come back for
// the key: nothing survives a restart, and there is no retry owner outside the
// receive path. Without it one panicked pipeline branch per key would hold its
// slot until the process restarts, and DefaultBaseReplayCapacity such branches
// would make Reserve answer `rejected` to every neighbour forever.
func baseHeldReservationGrace() time.Duration {
	residence, err := domain.QueueResidence(domain.DatagramClassBulk)
	if err != nil {
		return baseHeldReservationFallbackGrace
	}
	grace, err := domain.WriteGrace(domain.DatagramClassBulk)
	if err != nil {
		return baseHeldReservationFallbackGrace
	}
	return residence + grace
}

// baseEntry is one replay key in the base cache.
//
// A record carries no verdict, only whether it is still RESERVED: the two
// states differ in what the cache still owes the key (an unfinished branch that
// may yet release it), never in what the receive path is told about a duplicate.
type baseEntry struct {
	retention ReplayRetention
	incoming  IngressPeer
	rsv       ReservationToken

	// olderSameOwner and newerSameOwner link the records of ONE owner in
	// arrival order. The list is per owner and not global because the only
	// question ever asked of it is "the oldest disposable record of THIS
	// neighbour", and a global list makes that question cost a walk over
	// everybody else's records (evictOldestOfLocked).
	olderSameOwner *baseEntry
	newerSameOwner *baseEntry

	// expiryIndex is this record's position in the expiry heap, maintained by
	// the heap helpers so a record can be pulled out of the middle of it when
	// Release, a lazy expiry or an eviction removes it.
	expiryIndex int

	committed bool
}

// obligations counts what still keeps the record alive past its deadline. The
// only obligation a replay record can have is an unfinished reservation: a
// pipeline branch that has taken the key and reached neither Commit nor Release.
// It is expressed as a count rather than a bool because ReplayRetention owns the
// phase rule (RemovableAt) and must not have to know which dimension produced
// the number.
func (e *baseEntry) obligations() int {
	if e.committed {
		return 0
	}
	return 1
}

// abandoned reports that the only obligation of this record — an unfinished
// reservation — can no longer belong to a live operation. See
// baseHeldReservationGrace for why the base plane has to decide this itself.
//
// The grace is passed in rather than read here so the cache computes it ONCE,
// at construction: this is asked on every heap comparison, and a value derived
// from the class table per comparison would make the ordering pay for a lookup
// it can never answer differently.
func (e *baseEntry) abandoned(now time.Time, grace time.Duration) bool {
	if e.committed {
		return false
	}
	return now.After(e.retention.Until().Add(grace))
}

// ownerBucket is every record charged to ONE owner, oldest first, plus how many
// there are. The count is not a second source of truth: insertLocked and
// removeLocked are the only mutators of both, and they move them together.
type ownerBucket struct {
	oldest *baseEntry
	newest *baseEntry
	count  int
}

// BaseReplayCache is the layer's ONE anti-replay memory: bounded, in-memory, no
// durable custody. Safe for concurrent use.
//
// It is named by every caller through this concrete type — the pipeline, the
// node's plane and the tests alike — because there is no second implementation
// and no interface to write one behind (see "What the memory is NOT" in
// replay_cache.go).
type BaseReplayCache struct {
	clock func() time.Time

	entries map[domain.ReplayKey]*baseEntry
	// owners is the fairness accounting of §5, keyed by the QUOTA OWNER and
	// never by the arrival value. Two arrivals of one neighbour over two
	// channels — a reconnect, or two parallel sessions — are ONE bucket, which
	// is what stops a reconnect from renewing a neighbour's share of the cache;
	// two neighbours presenting one name are two, because the name is not what
	// anybody is billed by (ingressOwner).
	owners map[ingressOwner]ownerBucket
	// expiry is a binary MIN-HEAP over removableFromLocked: the record that may
	// be deleted soonest is always at index 0, whatever order the records
	// arrived in.
	expiry    []*baseEntry
	metrics   BaseReplayCacheMetrics
	heldGrace time.Duration
	capacity  int
	sequence  uint64
	mu        sync.Mutex
}

// NewBaseReplayCache builds the base cache. Zero fields in the config fall
// back to the normative defaults rather than to zero values, because a zero
// capacity would disable anti-replay silently.
//
// The replay WINDOW is not a field of the config: it is wire-normative, the
// receive path clamps replay_until against it before the cache ever sees the
// value (ComputeDeadlines, through BaseReplayDeadline), and a per-instance
// window would let one node hold a key for a different span than the node
// beside it.
func NewBaseReplayCache(cfg BaseReplayCacheConfig) *BaseReplayCache {
	cache := &BaseReplayCache{
		clock:     cfg.Clock,
		entries:   make(map[domain.ReplayKey]*baseEntry),
		owners:    make(map[ingressOwner]ownerBucket),
		capacity:  cfg.Capacity,
		heldGrace: baseHeldReservationGrace(),
	}
	if cache.clock == nil {
		cache.clock = time.Now
	}
	if cache.capacity <= 0 {
		cache.capacity = DefaultBaseReplayCapacity
	}
	// The heap is sized up front, so Reserve allocates the record and nothing
	// else: the receive path must not pay for the index growing under it. The
	// reservation is itself bounded by the normative capacity, because Capacity
	// is a knob nobody sanity-checks and a mistake in it must cost memory as the
	// records arrive rather than all of it at construction.
	preallocated := cache.capacity
	if preallocated > DefaultBaseReplayCapacity {
		preallocated = DefaultBaseReplayCapacity
	}
	cache.expiry = make([]*baseEntry, 0, preallocated)
	return cache
}

// Len returns the number of records currently held, expired-but-retained
// ones included.
func (c *BaseReplayCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// ownerLoadLocked reports how many records the OWNER of an arrival currently
// holds — not its channel. Two arrivals of one neighbour over two channels
// answer the same number, because they are one bucket (ingressOwner).
//
// The fairness rule of §5 reads the buckets directly (noisiestOwnerLocked); this
// is the same number as one question, and its only callers are the tests of that
// rule, which reach it through export_test.go. Caller holds c.mu.
func (c *BaseReplayCache) ownerLoadLocked(incoming IngressPeer) int {
	return c.owners[incoming.owner()].count
}

// Metrics returns a snapshot of the counters.
func (c *BaseReplayCache) Metrics() BaseReplayCacheMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metrics
}

// Has is the read-only early probe. It never inserts, never reserves and
// never extends a lifetime — poisoning the cache from an unauthenticated
// frame has to be impossible by construction, not by discipline.
func (c *BaseReplayCache) Has(_ context.Context, key domain.ReplayKey) HasResult {
	now := c.clock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, occupied := c.lookupLocked(key, now); !occupied {
		return hasMissResult()
	}
	return hasHitResult()
}

// Reserve is the atomic check-and-reserve. The slot is taken PHYSICALLY
// here, which is precisely why Commit later cannot fail for lack of room.
func (c *BaseReplayCache) Reserve(_ context.Context, key domain.ReplayKey, incoming IngressPeer, replayUntil time.Time) ReserveResult {
	now := c.clock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, occupied := c.lookupLocked(key, now); occupied {
		c.metrics.Duplicates++
		return reserveDuplicateResult()
	}
	if !c.makeRoomLocked(incoming, now) {
		return reserveRejectedResult(fmt.Errorf("%w: %d live entries", ErrReplayCacheCapacity, len(c.entries)))
	}

	c.sequence++
	entry := &baseEntry{
		retention: NewReplayRetention(replayUntil),
		incoming:  incoming,
		// The generation is store-wide, not per key: a per-key counter
		// would restart at zero once Release deleted the record, and the
		// next reservation of that key would carry the previous token —
		// the ABA hole the token exists to close.
		rsv: newReservationToken(key, c.sequence),
	}
	c.insertLocked(key, entry)
	c.metrics.Reserved++
	return reservedResult(entry.rsv)
}

// Commit settles a held reservation: the key stays remembered for the whole
// freshness window and the branch that held it owes nothing further.
//
// It records no verdict, and that is the point rather than a simplification.
// What the node DID with the frame — forwarded it, delivered it locally,
// refused it — is a fact about that one arrival, and the only reader this
// memory ever has is the duplicate check, which drops the repeat on all three
// alike. The counters of §10 are where the three outcomes are told apart, and
// they are told apart there per FRAME rather than per surviving cache record.
func (c *BaseReplayCache) Commit(_ context.Context, rsv ReservationToken) MutationResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[rsv.ReplayKey()]
	if !ok {
		return notAppliedResult(fmt.Errorf("%w: %s", ErrUnknownReplayKey, rsv))
	}
	if entry.rsv != rsv {
		return notAppliedResult(fmt.Errorf("%w: %s", ErrStaleReservation, rsv))
	}
	entry.committed = true
	// A committed record owes nothing, so it becomes removable at its own
	// deadline instead of a whole watchdog grace later: its heap key has just
	// decreased and the index has to learn it here, or the sweep would keep
	// walking past a record that is already free to go.
	c.expiryFixLocked(entry)
	c.metrics.Committed++
	return appliedResult()
}

// Release frees a reservation taken by this caller.
//
// A stale token is a no-op reported as ok: by the time a late Release
// arrives, the key may already belong to a newer reservation held by a
// parallel handler of the same frame, and cancelling THAT one is the bug the
// token exists to prevent (§4.1). A committed reservation is a no-op too —
// its fate is already fixed, and dropping the key would re-open a frame the
// node has already acted on.
func (c *BaseReplayCache) Release(_ context.Context, rsv ReservationToken) MutationResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[rsv.ReplayKey()]
	switch {
	case !ok:
		c.metrics.StaleReleases++
		return appliedResult()
	case entry.rsv != rsv:
		c.metrics.StaleReleases++
		return appliedResult()
	case entry.committed:
		c.metrics.StaleReleases++
		return appliedResult()
	default:
		c.removeLocked(rsv.ReplayKey(), entry)
		c.metrics.Released++
		return appliedResult()
	}
}

// SweepExpired removes every record that is past its deadline and owes nothing,
// and reports how many went. The owner of the cache runs it periodically
// (node.datagramMaintenanceLoop); the receive path only ever runs the same pass
// under the removal budget, inside Reserve — so a plane that stops receiving
// would otherwise hold every record it has until traffic returns.
//
// The context is unused here on purpose: this pass is arithmetic over an
// in-memory heap and cannot block, so there is nothing to abandon. The parameter
// stays because the pass is driven from a loop the node JOINS on shutdown, and
// the rule "everything that loop reaches is cancellable" is worth more without
// an exception to remember.
func (c *BaseReplayCache) SweepExpired(_ context.Context) int {
	now := c.clock()

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.sweepExpiredLocked(now, 0)
}

// lookupLocked returns the record of a key and whether the key is still
// OCCUPIED, i.e. whether Reserve would answer duplicate. A record that is
// past its deadline and owes nothing is semantically gone even though it may
// still be physically present, and it is dropped here so Has and Reserve can
// never disagree about the same key.
func (c *BaseReplayCache) lookupLocked(key domain.ReplayKey, now time.Time) (*baseEntry, bool) {
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if entry.retention.AliveAt(now) {
		return entry, true
	}
	if c.removableLocked(entry, now) {
		c.removeLocked(key, entry)
		c.metrics.ExpiredSwept++
		return nil, false
	}
	// Cleanup-only phase with an unfinished reservation: the key stays
	// taken (quarantine), a repeat of the same frame keeps getting
	// duplicate, and the initiator's retry with a fresh salt carries a
	// different key and does not depend on this one.
	return entry, true
}

// makeRoomLocked ensures the new reservation fits. It reports false when the
// caller must be refused.
//
// Overflow is charged to the NOISIEST neighbour rather than flushed
// globally (§5): a peer that filled the cache is refused itself, while a
// quiet neighbour is admitted by evicting the oldest disposable record of
// the noisy one. A global flush would let one peer erase everyone else's
// anti-replay memory, which is the attack this rule exists to stop.
//
// "Noisiest" is decided per OWNER and never per arrival: a neighbour that
// spreads its records over N reconnects and parallel sessions used to read as N
// quiet neighbours, so it stayed under the rule while the honest peer on one
// channel was picked as the victim — and the records taken from that peer are
// the anti-replay memory protecting frames it has already seen.
func (c *BaseReplayCache) makeRoomLocked(incoming IngressPeer, now time.Time) bool {
	if len(c.entries) < c.capacity {
		return true
	}
	c.sweepExpiredLocked(now, baseSweepRemovalBudget)
	if len(c.entries) < c.capacity {
		return true
	}

	owner := incoming.owner()
	noisiest, ok := c.noisiestOwnerLocked()
	if !ok {
		// A full cache with no recorded load is an internal inconsistency, and
		// the caller is refused either way — but a refusal nobody counts looks
		// exactly like a network that lost the frame (§5).
		c.metrics.RejectedCapacity++
		return false
	}
	if noisiest == owner {
		c.metrics.RejectedNoisyPeer++
		return false
	}
	if !c.evictOldestOfLocked(noisiest) {
		c.metrics.RejectedCapacity++
		return false
	}
	c.metrics.EvictedNoisyPeer++
	return true
}

// sweepExpiredLocked removes records that may already be deleted, EARLIEST
// DEADLINE FIRST, and stops after `budget` removals. A non-positive budget means
// the full pass.
//
// The heap is what makes a bounded pass honest. Deadlines are not monotonic in
// insertion order — base_until is measured from the SIGNED auth.time, so a frame
// authored five minutes ago is inserted last and expires first — so a walk from
// the oldest INSERTED record spends its budget on live records while expired ones
// sit behind them, refuses the reservation, and refuses the next one for the same
// reason. A full cache then answered `rejected` to EVERY neighbour until the
// records at the front of that walk expired on their own, which is not a slow
// path but an anti-replay outage: nothing new could be remembered, so nothing new
// was protected from replay.
func (c *BaseReplayCache) sweepExpiredLocked(now time.Time, budget int) int {
	removed := 0
	for len(c.expiry) > 0 {
		if budget > 0 && removed == budget {
			break
		}
		entry := c.expiry[0]
		if !now.After(c.removableFromLocked(entry)) {
			// The heap is ordered by exactly this moment, so the first record
			// that is not removable yet proves that none of the rest is either.
			break
		}
		if !c.removableLocked(entry, now) {
			// Unreachable while the two rules agree, and they are written to
			// agree (removableFromLocked). Stopping rather than skipping is what
			// keeps a future disagreement a missed record instead of a spin under
			// the mutex.
			break
		}
		c.removeLocked(entry.rsv.ReplayKey(), entry)
		removed++
	}
	c.metrics.ExpiredSwept += uint64(removed)
	return removed
}

// removableLocked is the single rule every sweep uses: a record past its
// deadline that owes nothing, or one whose only obligation is a reservation
// the watchdog has declared abandoned.
func (c *BaseReplayCache) removableLocked(entry *baseEntry, now time.Time) bool {
	if entry.retention.RemovableAt(now, entry.obligations()) {
		return true
	}
	if entry.abandoned(now, c.heldGrace) {
		c.metrics.AbandonedReservations++
		return true
	}
	return false
}

// removableFromLocked is removableLocked restated as a MOMENT, and it is the key
// the expiry heap is ordered by: a record is removable exactly when `now` is
// strictly after this instant.
//
// The two must stay equivalent, and they are equivalent term by term. A COMMITTED
// record owes nothing, so RemovableAt reduces to CleanupOnlyAt — strictly after
// replay_until — and abandoned() is false for it by definition. An UNCOMMITTED one
// owes its reservation, so RemovableAt is false whatever the time, and only the
// watchdog can free it: strictly after replay_until plus the grace.
func (c *BaseReplayCache) removableFromLocked(entry *baseEntry) time.Time {
	if entry.committed {
		return entry.retention.Until()
	}
	return entry.retention.Until().Add(c.heldGrace)
}

// noisiestOwnerLocked returns the bucket holding the most records. Ties break on
// bucket order so the victim of an overflow is reproducible rather than
// map-iteration dependent.
//
// It walks the per-owner tally and not the records, so its cost is one entry per
// neighbour with live state and never one per record. The tie keeps the GREATEST
// key under ingressOwner.compare, whose least is the local bucket — so a tie never
// victimises this node's own frame, which is the direction the reverse plane's
// twin takes as well (busiestUpstreamLocked).
func (c *BaseReplayCache) noisiestOwnerLocked() (ingressOwner, bool) {
	var (
		noisiest ingressOwner
		best     int
		found    bool
	)
	for owner, bucket := range c.owners {
		switch {
		case !found, bucket.count > best:
			noisiest, best, found = owner, bucket.count, true
		case bucket.count == best && noisiest.compare(owner) < 0:
			noisiest = owner
		}
	}
	return noisiest, found
}

// evictOldestOfLocked drops the oldest record of one owner that is safe to
// drop. A held reservation is never a victim: someone owns that token right
// now, and evicting it would hand the same key to a second instance of the
// same frame.
//
// The walk spans EVERY channel of that owner and nobody else's records, which is
// what makes it both correct and cheap: it is the noisy neighbour's own record
// that goes, and the only records skipped are that same neighbour's reservations
// still in flight — a handful by construction, since each of them is one frame
// being processed right now. The previous bounded walk over a global
// insertion-ordered list could miss a perfectly evictable record behind 128
// foreign ones and answer "nothing to evict" while the cache was full of them.
func (c *BaseReplayCache) evictOldestOfLocked(owner ingressOwner) bool {
	for entry := c.owners[owner].oldest; entry != nil; entry = entry.newerSameOwner {
		if entry.committed {
			c.removeLocked(entry.rsv.ReplayKey(), entry)
			return true
		}
	}
	return false
}

// insertLocked and removeLocked are the ONLY ways a record enters or leaves the
// cache, so neither index can drift from the key map it summarises.
func (c *BaseReplayCache) insertLocked(key domain.ReplayKey, entry *baseEntry) {
	c.entries[key] = entry
	c.attachToOwnerLocked(entry)
	c.expiryPushLocked(entry)
}

func (c *BaseReplayCache) removeLocked(key domain.ReplayKey, entry *baseEntry) {
	delete(c.entries, key)
	c.detachFromOwnerLocked(entry)
	c.expiryRemoveLocked(entry)
}

// attachToOwnerLocked appends the record at the newest end of its owner's list.
func (c *BaseReplayCache) attachToOwnerLocked(entry *baseEntry) {
	owner := entry.incoming.owner()
	bucket := c.owners[owner]
	entry.olderSameOwner = bucket.newest
	entry.newerSameOwner = nil
	if bucket.newest != nil {
		bucket.newest.newerSameOwner = entry
	}
	bucket.newest = entry
	if bucket.oldest == nil {
		bucket.oldest = entry
	}
	bucket.count++
	c.owners[owner] = bucket
}

// detachFromOwnerLocked unlinks the record and forgets its share of the bucket.
func (c *BaseReplayCache) detachFromOwnerLocked(entry *baseEntry) {
	owner := entry.incoming.owner()
	bucket, known := c.owners[owner]
	if !known {
		return
	}
	if entry.olderSameOwner != nil {
		entry.olderSameOwner.newerSameOwner = entry.newerSameOwner
	} else {
		bucket.oldest = entry.newerSameOwner
	}
	if entry.newerSameOwner != nil {
		entry.newerSameOwner.olderSameOwner = entry.olderSameOwner
	} else {
		bucket.newest = entry.olderSameOwner
	}
	entry.olderSameOwner, entry.newerSameOwner = nil, nil

	bucket.count--
	if bucket.count <= 0 {
		// The empty bucket is deleted rather than left at zero: the key is an
		// admission key, which an attacker mints one of per address this node
		// dials, and keeping zero entries would make this the unbounded map the
		// cache is careful not to be.
		delete(c.owners, owner)
		return
	}
	c.owners[owner] = bucket
}

// ---------------------------------------------------------------------------
// The expiry heap
// ---------------------------------------------------------------------------
//
// A binary min-heap over removableFromLocked, written out rather than driven
// through container/heap because the ordering needs the cache's own grace and
// because the interface indirection would allocate on a path that must not.

func (c *BaseReplayCache) expiryPushLocked(entry *baseEntry) {
	entry.expiryIndex = len(c.expiry)
	c.expiry = append(c.expiry, entry)
	c.expiryUpLocked(entry.expiryIndex)
}

func (c *BaseReplayCache) expiryRemoveLocked(entry *baseEntry) {
	index := entry.expiryIndex
	if index < 0 || index >= len(c.expiry) || c.expiry[index] != entry {
		return
	}
	last := len(c.expiry) - 1
	moved := c.expiry[last]
	c.expiry[last] = nil
	c.expiry = c.expiry[:last]
	entry.expiryIndex = -1
	if index == last {
		return
	}
	c.expiry[index] = moved
	moved.expiryIndex = index
	if !c.expiryDownLocked(index) {
		c.expiryUpLocked(index)
	}
}

// expiryFixLocked restores the ordering after a record's key changed.
func (c *BaseReplayCache) expiryFixLocked(entry *baseEntry) {
	if entry.expiryIndex < 0 {
		return
	}
	if !c.expiryDownLocked(entry.expiryIndex) {
		c.expiryUpLocked(entry.expiryIndex)
	}
}

func (c *BaseReplayCache) expiryUpLocked(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !c.expiryLessLocked(index, parent) {
			return
		}
		c.expirySwapLocked(index, parent)
		index = parent
	}
}

// expiryDownLocked sifts a record towards the leaves and reports whether it
// moved, which is what lets expiryFixLocked try the other direction only when
// this one did nothing.
func (c *BaseReplayCache) expiryDownLocked(index int) bool {
	start := index
	for {
		left := 2*index + 1
		if left >= len(c.expiry) {
			break
		}
		smallest := left
		if right := left + 1; right < len(c.expiry) && c.expiryLessLocked(right, left) {
			smallest = right
		}
		if !c.expiryLessLocked(smallest, index) {
			break
		}
		c.expirySwapLocked(index, smallest)
		index = smallest
	}
	return index > start
}

func (c *BaseReplayCache) expiryLessLocked(first, second int) bool {
	return c.removableFromLocked(c.expiry[first]).Before(c.removableFromLocked(c.expiry[second]))
}

func (c *BaseReplayCache) expirySwapLocked(first, second int) {
	c.expiry[first], c.expiry[second] = c.expiry[second], c.expiry[first]
	c.expiry[first].expiryIndex = first
	c.expiry[second].expiryIndex = second
}
