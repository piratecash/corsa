package node

import (
	"container/list"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Session-peer identity records: the bounded, memory-only half of the
// record store.
//
// docs/protocol/identity-lookup.md §4 names three holders of a signed
// identity record: the owner (disk), the interlocutor — a trusted contact —
// (disk), and the owner's direct peers, for whom the record is SESSION
// MEMORY. The store used to persist all three in one map, so every
// push_identity from every session peer a relay ever held became a row in
// trust-<port>.json that nothing removed: forget() only reaches records
// behind a contact, and a relay with thousands of distinct peers over its
// life carried all of their records in memory and rewrote all of them to
// disk on every new one.
//
// This cache holds the third class under a budget. Nothing about it is
// needed across a restart: the datagram plane is self-certifying (the
// frame carries auth.pubkey), a first DM carries the sender's keys, a
// session peer re-pushes its record right after auth, and an addressed
// get_identity recovers any record on demand. What the cache preserves is
// the seq gate for the lifetime of the entry — a stale or downgraded
// record from an identity we are currently talking to is refused, and a
// dm:false revocation holds — and it is the only reader of these records
// besides the knowledge maps, which are filled at import and bounded on
// their own.
//
// Order is by acceptance time (a higher-seq replacement re-enters at the
// back), which makes the TTL sweep and the budget eviction the same
// operation: pop the front. Nothing touches an entry on read, so the order
// never diverges from storedAt, and "oldest accepted first" is exactly the
// entry whose seq gate has had the longest time to matter.
//
// Budget. One entry is priced at its structural size plus the bytes it
// references: the signed body and signature (verbatim, ~350–450 B for a
// record with dm keys), and the parsed body's key strings (a second copy of
// the base64 material, ~200 B), so ~1 KiB per record. A relay serves on the
// order of a hundred session peers at a time and answers lookups for the
// targets its users talk to; 4 096 records / 4 MiB is a working set an
// order of magnitude above that, and the TTL exists for the node that never
// reaches the count: a record whose owner has been gone for a day is not a
// session memory any more. The values are constants because the cost is
// paid in memory alone — there is no disk to protect and no operator
// question a knob would answer better than the gauge does.
const (
	maxSessionIdentityRecords     = 4096
	maxSessionIdentityRecordBytes = 4 << 20
	sessionIdentityRecordTTL      = 24 * time.Hour
)

// sessionRecordStructBytes is what one entry costs before the bytes it
// points at: the map key, the list element and the record struct.
var sessionRecordStructBytes = domain.SizeOfAll(trustRecordKey{}, list.Element{}, sessionRecordEntry{})

type sessionRecordEntry struct {
	key    trustRecordKey
	record trustedIdentityRecord
	bytes  int
}

// sessionRecordStats is the cache's own account of itself for the resource
// breakdown: what it holds, and how much it has had to let go and why.
// Evicted counts budget evictions, expired counts TTL sweeps; both are
// monotonic for the life of the process.
type sessionRecordStats struct {
	count int
	bytes int
	// protected is filled at read time by protectedCountLocked rather than
	// maintained as entries come and go: what the live set holds changes
	// when a session or a lookup ends, and this cache is never told about
	// that, so an incrementally tracked counter would drift away from the
	// oracle it claims to describe.
	protected    int
	evicted      uint64
	evictedBytes uint64
	expired      uint64
}

// sessionRecordCache has no mutex of its own: it is a leaf of trustStore
// and every method requires trustStore.mu held in write mode (the *Locked
// suffix). The clock is the store's.
type sessionRecordCache struct {
	entries map[trustRecordKey]*list.Element
	order   *list.List // Front: oldest accepted; Back: newest.
	// protection names the addresses the node is currently talking to or
	// asking about, whose records must survive both the TTL and the
	// budget: such a record IS the seq floor for its address, and dropping
	// it under a live peer would let that peer's own earlier record — the
	// predecessor of a revocation — merge as `inserted` and bring a
	// withdrawn box key back. Protection is the cache's correctness
	// boundary, not an optimisation.
	//
	// Every removal this cache performs for TTL or budget goes through
	// protection.dropIfUnprotected, which holds the check and the removal
	// in one critical section. Two weaker shapes were tried and are both
	// wrong: a set handed to the cache periodically is stale for the whole
	// interval after a session comes up, and a lock-free read followed by
	// a removal leaves the interleaving where the pin lands in between and
	// the record is deleted under a session that is already live.
	//
	// nil means nothing is protected, which is what a store built without
	// a Service around it gets.
	protection *recordProtection
	maxCount   int
	maxBytes   int
	ttl        time.Duration
	stats      sessionRecordStats
}

func newSessionRecordCache(maxCount, maxBytes int, ttl time.Duration) *sessionRecordCache {
	return &sessionRecordCache{
		entries:  make(map[trustRecordKey]*list.Element),
		order:    list.New(),
		maxCount: maxCount,
		maxBytes: maxBytes,
		ttl:      ttl,
	}
}

// sessionRecordBytes prices one record: the structural cost plus every
// byte the entry keeps alive through its strings and slices.
func sessionRecordBytes(key trustRecordKey, stored trustedIdentityRecord) int {
	return int(sessionRecordStructBytes) +
		len(key.network) + len(key.address) +
		len(stored.record.Body) + len(stored.record.Sig) +
		len(stored.body.PubKey) + len(stored.body.BoxKey) + len(stored.body.BoxSig)
}

// getLocked returns the cached record for key. It does not refresh the
// entry's position: the order is acceptance order by design.
func (c *sessionRecordCache) getLocked(key trustRecordKey) (trustedIdentityRecord, bool) {
	element, ok := c.entries[key]
	if !ok {
		return trustedIdentityRecord{}, false
	}
	return element.Value.(*sessionRecordEntry).record, true
}

// putLocked stores or replaces the record for key at the back of the
// order and then enforces the budget from the front. The record being
// stored is never the one evicted: the budget is applied after the
// insert, and an entry costs less than either ceiling.
func (c *sessionRecordCache) putLocked(key trustRecordKey, stored trustedIdentityRecord) {
	if element, ok := c.entries[key]; ok {
		c.removeLocked(element)
	}
	entry := &sessionRecordEntry{key: key, record: stored, bytes: sessionRecordBytes(key, stored)}
	inserted := c.order.PushBack(entry)
	c.entries[key] = inserted
	c.stats.count++
	c.stats.bytes += entry.bytes
	for c.stats.count > c.maxCount || c.stats.bytes > c.maxBytes {
		if !c.evictOneLocked(inserted) {
			// Everything left is protected (or is the record we just
			// accepted). The overshoot is bounded by the protected set
			// and is visible as identity_record_cache_protected beside
			// the count; the alternative — dropping a live peer's seq
			// floor to honour a byte budget — is the downgrade this
			// cache exists to prevent.
			return
		}
	}
}

// evictOneLocked drops the oldest droppable entry and reports whether it
// found one. A candidate that turns out to be protected AT THE MOMENT OF
// THE REMOVAL is skipped and the walk continues, because the answer that
// matters is the one that holds while the entry is being deleted, not the
// one read a moment earlier.
func (c *sessionRecordCache) evictOneLocked(keep *list.Element) bool {
	for element := c.order.Front(); element != nil; {
		next := element.Next()
		if element == keep {
			element = next
			continue
		}
		entry := element.Value.(*sessionRecordEntry)
		if c.dropLocked(element) {
			c.stats.evicted++
			c.stats.evictedBytes += uint64(entry.bytes)
			return true
		}
		element = next
	}
	return false
}

// dropLocked removes the entry unless its address is protected, and does
// both under the protection's own lock so a pin cannot land between the
// decision and the removal. Reports whether the entry was removed.
func (c *sessionRecordCache) dropLocked(element *list.Element) bool {
	if c.protection == nil {
		c.removeLocked(element)
		return true
	}
	entry := element.Value.(*sessionRecordEntry)
	return c.protection.dropIfUnprotected(entry.key.address, func() {
		c.removeLocked(element)
	})
}

// protectedCountLocked counts the entries the live set currently holds.
// Walked on demand for the breakdown rather than tracked incrementally: the
// answer changes when a session or a lookup ends, which this cache is never
// told about, so a counter maintained here would drift from the truth. The
// lock-free read is right here and only here — a diagnostic one instant
// stale costs nothing, while an eviction decided that way costs a floor.
func (c *sessionRecordCache) protectedCountLocked() int {
	if c.protection == nil {
		return 0
	}
	protected := 0
	for key := range c.entries {
		if c.protection.contains(key.address) {
			protected++
		}
	}
	return protected
}

// deleteLocked drops the record for key, if any, and reports whether it
// was present.
func (c *sessionRecordCache) deleteLocked(key trustRecordKey) bool {
	element, ok := c.entries[key]
	if !ok {
		return false
	}
	c.removeLocked(element)
	return true
}

// takeLocked removes and returns the record for key — the promotion path,
// when the identity becomes a contact and its record moves to disk.
func (c *sessionRecordCache) takeLocked(key trustRecordKey) (trustedIdentityRecord, bool) {
	element, ok := c.entries[key]
	if !ok {
		return trustedIdentityRecord{}, false
	}
	stored := element.Value.(*sessionRecordEntry).record
	c.removeLocked(element)
	return stored, true
}

// deleteAddressLocked drops every network's record for an address. Runs
// when a contact is forgotten: whatever the address's record is, on any
// network, it must not outlive the deletion.
func (c *sessionRecordCache) deleteAddressLocked(address string) {
	for key, element := range c.entries {
		if key.address == address {
			c.removeLocked(element)
		}
	}
}

// sweepLocked drops entries accepted more than ttl ago, except those the
// live set protects — for a peer still on a session, the TTL is not
// evidence that its record is stale, only that the record has been quiet.
// Entries are in acceptance order, so the walk stops at the first one
// still inside its TTL; a protected entry is stepped over, not stopped
// at, and is swept by a later pass once its address leaves the set.
func (c *sessionRecordCache) sweepLocked(now time.Time) int {
	swept := 0
	for element := c.order.Front(); element != nil; {
		entry := element.Value.(*sessionRecordEntry)
		if now.Sub(entry.record.storedAt) <= c.ttl {
			break
		}
		next := element.Next()
		if c.dropLocked(element) {
			c.stats.expired++
			swept++
		}
		element = next
	}
	return swept
}

func (c *sessionRecordCache) removeLocked(element *list.Element) {
	entry := element.Value.(*sessionRecordEntry)
	delete(c.entries, entry.key)
	c.order.Remove(element)
	c.stats.count--
	c.stats.bytes -= entry.bytes
}
