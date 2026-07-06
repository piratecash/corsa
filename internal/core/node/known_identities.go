package node

import "container/list"

// known_identities.go bounds the s.known accumulator.
//
// s.known records every identity this node has observed (DM senders and
// recipients in storeIncomingMessage, contacts and handshake peers via
// addKnownIdentity). It is read in only two places: the fetch_identities RPC
// listing and the first-sight check that fires an IdentityAdded event. It has
// NO routing, gossip, or verification role.
//
// As a plain map it only ever grew: one entry per distinct identity ever seen,
// never evicted. On a long-lived relay that transits many parties this is a
// slow but unbounded climb. Bounding it is safe because the authoritative
// contact list lives in the persistent trust store (fetch_trusted_contacts),
// not here — evicting an entry can only drop a transit-seen identity from the
// diagnostic listing, never a trusted contact (which is re-seeded into the set
// at startup and re-added whenever the peer is seen again).
//
// Eviction is LRU, not FIFO: Add on an already-present identity moves it back
// to the most-recently-used end, so an identity that keeps being observed is
// never evicted ahead of a truly idle one. Only the least-recently-seen entry
// is dropped when the set is full.

// maxKnownIdentities caps the live set. Sized well above any realistic desktop
// contact/correspondent count (so a normal client never evicts) while bounding
// a busy relay: 50k entries ≈ a few MiB ceiling.
const maxKnownIdentities = 50_000

// boundedKnownIdentities is an LRU set capped at a fixed capacity. It is NOT
// internally synchronised: it is a knowledgeMu-domain field (docs/locking.md)
// and every caller already holds s.knowledgeMu, exactly as the raw map it
// replaced did. Construct via newBoundedKnownIdentities; the zero value is not
// usable.
type boundedKnownIdentities struct {
	// order is a recency list: Front is least-recently-used, Back is
	// most-recently-used. nodes maps each member to its element for O(1)
	// lookup, move-to-back, and eviction.
	order    *list.List
	nodes    map[string]*list.Element
	capacity int
	// pinned members are exempt from capacity eviction. Used for
	// trust-store contacts: their box/pub-key knowledge must never be
	// dropped by transit-identity churn (a trusted contact's DM could
	// otherwise fail sender verification until the next contact sync).
	// Pin on trust (NewService seed / trustContact), Unpin on revoke
	// (deleteTrustedContactFrame → trust.forget) — the set mirrors the
	// live trust store, which is what keeps it (and thus the whole
	// bound) at "capacity + current trust store size".
	pinned map[string]struct{}
	// onEvict, when non-nil, runs for every address dropped by capacity
	// eviction. It executes under the caller's knowledgeMu hold — keep it
	// to cheap map writes. Used to keep the knowledgeMu-domain key maps
	// (boxKeys / pubKeys / boxSigs) subsets of this set, which is what
	// makes THEIR growth bounded too.
	onEvict func(address string)
}

func newBoundedKnownIdentities(capacity int) *boundedKnownIdentities {
	if capacity < 1 {
		capacity = 1
	}
	return &boundedKnownIdentities{
		order:    list.New(),
		nodes:    make(map[string]*list.Element),
		pinned:   make(map[string]struct{}),
		capacity: capacity,
	}
}

// Add inserts address and reports whether it was newly added (false = already
// present). An already-present address is promoted to most-recently-used. When
// the set is at capacity the least-recently-used entry is evicted first, so
// membership never exceeds capacity.
func (b *boundedKnownIdentities) Add(address string) bool {
	if el, ok := b.nodes[address]; ok {
		b.order.MoveToBack(el)
		return false
	}
	if len(b.nodes) >= b.capacity {
		b.evictOldestLocked()
	}
	b.nodes[address] = b.order.PushBack(address)
	return true
}

// Pin marks address as exempt from capacity eviction (adding it first if
// absent). Membership above capacity is tolerated when everything else is
// pinned — the pinned set is bounded by the persistent trust store.
func (b *boundedKnownIdentities) Pin(address string) {
	if address == "" {
		return
	}
	b.pinned[address] = struct{}{}
	b.Add(address)
}

// Unpin removes the eviction exemption. The address stays a regular LRU
// member and is reclaimed by ordinary capacity eviction once it ages out
// — matching "network-learned knowledge" semantics for an ex-contact.
// No-op when the address was never pinned.
//
// Pins are the only way membership exceeds capacity (Add tolerates the
// overshoot when everything evictable is pinned), and Add reclaims at
// most one entry per insert — never enough to shrink an oversized set.
// So Unpin, the only operation that turns an over-capacity set drainable
// again, drains it back to the bound here.
func (b *boundedKnownIdentities) Unpin(address string) {
	delete(b.pinned, address)
	for len(b.nodes) > b.capacity {
		before := len(b.nodes)
		b.evictOldestLocked()
		if len(b.nodes) == before {
			return // everything remaining is pinned — nothing to drain
		}
	}
}

// Has reports membership without affecting recency.
func (b *boundedKnownIdentities) Has(address string) bool {
	_, ok := b.nodes[address]
	return ok
}

// Snapshot returns a copy of the current members in unspecified order.
func (b *boundedKnownIdentities) Snapshot() []string {
	out := make([]string, 0, len(b.nodes))
	for address := range b.nodes {
		out = append(out, address)
	}
	return out
}

// Len is the current member count.
func (b *boundedKnownIdentities) Len() int {
	return len(b.nodes)
}

// evictOldestLocked drops the least-recently-used non-pinned member and
// fires onEvict for it. The Locked suffix matches the project convention
// even though synchronisation is the caller's knowledgeMu, not an
// internal mutex.
func (b *boundedKnownIdentities) evictOldestLocked() {
	for el := b.order.Front(); el != nil; el = el.Next() {
		address := el.Value.(string)
		if _, ok := b.pinned[address]; ok {
			continue
		}
		b.order.Remove(el)
		delete(b.nodes, address)
		if b.onEvict != nil {
			b.onEvict(address)
		}
		return
	}
}
