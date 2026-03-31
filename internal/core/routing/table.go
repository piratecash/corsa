package routing

import (
	"sync"
	"time"
)

const (
	// DefaultTTL is the default route lifetime.
	DefaultTTL = 120 * time.Second
)

// Table is the local distance-vector routing table. It stores routes keyed
// by destination identity, with deduplication based on the (Identity, Origin,
// NextHop) triple.
//
// localOrigin is this node's identity (Ed25519 fingerprint). It is used as
// the Origin field for direct routes and as the scope for the monotonic
// SeqNo counter. Only routes where Origin == localOrigin may have their
// SeqNo advanced by this table — this enforces the per-origin SeqNo
// invariant at the data structure level.
//
// All public methods are safe for concurrent use.
type Table struct {
	mu     sync.RWMutex
	routes map[string][]RouteEntry // identity -> routes

	// localOrigin is this node's Ed25519 fingerprint. Used as Origin for
	// direct routes created by AddDirectPeer. Required for AddDirectPeer
	// and RemoveDirectPeer operations.
	localOrigin string

	// seqCounters tracks the next SeqNo to use for own-origin routes,
	// keyed by destination identity. Only the owner of localOrigin may
	// increment these counters.
	seqCounters map[string]uint64

	defaultTTL time.Duration

	// clock is used for time-dependent operations, allowing tests to inject
	// a controllable clock.
	clock func() time.Time
}

// TableOption configures optional Table parameters.
type TableOption func(*Table)

// WithClock overrides the default time source, useful for deterministic tests.
func WithClock(clock func() time.Time) TableOption {
	return func(t *Table) {
		t.clock = clock
	}
}

// WithDefaultTTL overrides the default route TTL.
func WithDefaultTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.defaultTTL = d
	}
}

// WithLocalOrigin sets this node's identity. Required for AddDirectPeer
// and RemoveDirectPeer. The localOrigin is used as the Origin field for
// direct routes and scopes the monotonic SeqNo counter.
func WithLocalOrigin(identity string) TableOption {
	return func(t *Table) {
		t.localOrigin = identity
	}
}

// NewTable creates an empty routing table with the given options.
func NewTable(opts ...TableOption) *Table {
	t := &Table{
		routes:      make(map[string][]RouteEntry),
		seqCounters: make(map[string]uint64),
		defaultTTL:  DefaultTTL,
		clock:       time.Now,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// UpdateRoute inserts or updates a route in the table. The dedup key is
// (Identity, Origin, NextHop). If an existing entry shares the same triple:
//
//   - If incoming SeqNo > existing SeqNo: replace unconditionally.
//   - If incoming SeqNo == existing SeqNo: replace only if the incoming
//     source has a higher trust rank or fewer hops.
//   - If incoming SeqNo < existing SeqNo: reject (stale announcement).
//
// Returns (true, nil) if the route was accepted, (false, nil) if rejected
// by SeqNo/trust rules, or (false, err) if the entry is malformed.
func (t *Table) UpdateRoute(entry RouteEntry) (bool, error) {
	if err := entry.Validate(); err != nil {
		return false, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	if entry.ExpiresAt.IsZero() {
		entry.ExpiresAt = now.Add(t.defaultTTL)
	}

	existing := t.routes[entry.Identity]
	idx := findByTriple(existing, entry.DedupKey())

	accepted := false

	if idx < 0 {
		t.routes[entry.Identity] = append(existing, entry)
		accepted = true
	} else {
		old := &existing[idx]

		if entry.SeqNo > old.SeqNo {
			existing[idx] = entry
			accepted = true
		} else if entry.SeqNo == old.SeqNo {
			if entry.Source.TrustRank() > old.Source.TrustRank() {
				existing[idx] = entry
				accepted = true
			} else if entry.Source == old.Source && entry.Hops < old.Hops {
				existing[idx] = entry
				accepted = true
			}
		}
	}

	if accepted {
		t.syncSeqCounterLocked(entry)
	}

	return accepted, nil
}

// syncSeqCounterLocked ensures the monotonic SeqNo counter stays ahead of
// any accepted own-origin entry. Without this, a table pre-populated via
// UpdateRoute (e.g., restored from snapshot) could have a higher SeqNo than
// the counter, causing the next AddDirectPeer/RemoveDirectPeer to emit a
// stale seq and break monotonicity.
//
// Must be called with t.mu held.
func (t *Table) syncSeqCounterLocked(entry RouteEntry) {
	if t.localOrigin == "" || entry.Origin != t.localOrigin {
		return
	}
	if entry.SeqNo > t.seqCounters[entry.Identity] {
		t.seqCounters[entry.Identity] = entry.SeqNo
	}
}

// WithdrawRoute marks a specific route as withdrawn by setting hops to
// HopsInfinity. This should be called when processing an incoming withdrawal
// from the wire (hops=16, incremented SeqNo). For local peer disconnects,
// use RemoveDirectPeer instead.
//
// Returns true if the withdrawal was applied.
func (t *Table) WithdrawRoute(identity, origin, nextHop string, seqNo uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	existing := t.routes[identity]
	key := RouteTriple{Identity: identity, Origin: origin, NextHop: nextHop}
	idx := findByTriple(existing, key)

	if idx < 0 {
		return false
	}

	old := &existing[idx]
	if seqNo <= old.SeqNo {
		return false
	}

	existing[idx].Hops = HopsInfinity
	existing[idx].SeqNo = seqNo
	existing[idx].ExpiresAt = t.clock().Add(t.defaultTTL)
	return true
}

// RemoveDirectPeerResult describes the outcome of RemoveDirectPeer.
type RemoveDirectPeerResult struct {
	// Withdrawals contains wire-ready AnnounceEntry items for direct routes
	// that this node originated. SeqNo is already incremented and Hops is
	// set to HopsInfinity. The caller sends these as-is in announce_routes
	// frames — no further seq arithmetic is needed.
	Withdrawals []AnnounceEntry

	// TransitInvalidated is the count of transit routes (learned via
	// announcement or hop_ack) that were silently marked as withdrawn
	// locally. No wire withdrawal is emitted for these — the originating
	// node is responsible for its own withdrawals.
	TransitInvalidated int
}

// AddDirectPeer registers a directly connected peer in the routing table.
// It creates a direct route (Hops=1, Source=RouteSourceDirect) with
// Origin set to this node's localOrigin and an auto-incremented SeqNo
// from the monotonic counter.
//
// Idempotent: if the peer already has an active (non-withdrawn) direct
// route originated by this node, AddDirectPeer refreshes the TTL and
// returns the existing entry without incrementing SeqNo. This prevents
// unnecessary SeqNo churn and triggered updates on duplicate connect
// events or additional sessions to the same peer identity.
//
// SeqNo is only incremented when the route is new or was previously
// withdrawn (reconnect after disconnect).
//
// Returns the created/existing RouteEntry and nil error on success.
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) AddDirectPeer(peerIdentity string) (RouteEntry, error) {
	if t.localOrigin == "" {
		return RouteEntry{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return RouteEntry{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := RouteTriple{
		Identity: peerIdentity,
		Origin:   t.localOrigin,
		NextHop:  peerIdentity,
	}
	existing := t.routes[peerIdentity]
	idx := findByTriple(existing, key)

	// Already active — refresh TTL only, no SeqNo bump.
	if idx >= 0 && !existing[idx].IsWithdrawn() {
		existing[idx].ExpiresAt = t.clock().Add(t.defaultTTL)
		return existing[idx], nil
	}

	// New route or re-activation after withdrawal — increment SeqNo.
	seq := t.nextSeqLocked(peerIdentity)

	entry := RouteEntry{
		Identity:  peerIdentity,
		Origin:    t.localOrigin,
		NextHop:   peerIdentity,
		Hops:      1,
		SeqNo:     seq,
		Source:    RouteSourceDirect,
		ExpiresAt: t.clock().Add(t.defaultTTL),
	}

	if idx < 0 {
		t.routes[peerIdentity] = append(existing, entry)
	} else {
		existing[idx] = entry
	}

	return entry, nil
}

// RemoveDirectPeer handles a peer disconnect. It withdraws the direct
// route originated by this node (with an auto-incremented SeqNo) and
// silently invalidates all transit routes learned through that peer.
//
// The returned Withdrawals are wire-ready: SeqNo is already incremented,
// Hops is HopsInfinity, and the entries are in AnnounceEntry form. The
// caller sends them as-is in announce_routes frames.
//
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) RemoveDirectPeer(peerIdentity string) (RemoveDirectPeerResult, error) {
	if t.localOrigin == "" {
		return RemoveDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return RemoveDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	var result RemoveDirectPeerResult

	for _, routes := range t.routes {
		for i := range routes {
			r := &routes[i]
			if r.NextHop != peerIdentity || r.Hops >= HopsInfinity {
				continue
			}

			if r.Source == RouteSourceDirect && r.Origin == t.localOrigin {
				seq := t.nextSeqLocked(r.Identity)
				r.Hops = HopsInfinity
				r.SeqNo = seq

				result.Withdrawals = append(result.Withdrawals, AnnounceEntry{
					Identity: r.Identity,
					Origin:   r.Origin,
					Hops:     HopsInfinity,
					SeqNo:    seq,
				})
			} else {
				result.TransitInvalidated++
				r.Hops = HopsInfinity
			}
		}
	}

	return result, nil
}

// nextSeqLocked increments and returns the next SeqNo for a given identity.
// Must be called with t.mu held.
func (t *Table) nextSeqLocked(identity string) uint64 {
	t.seqCounters[identity]++
	return t.seqCounters[identity]
}

// TickTTL removes expired routes from the table. Should be called
// periodically (e.g., every second or every few seconds).
func (t *Table) TickTTL() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	for identity, routes := range t.routes {
		n := 0
		for i := range routes {
			if !routes[i].IsExpired(now) {
				routes[n] = routes[i]
				n++
			}
		}
		if n == 0 {
			delete(t.routes, identity)
		} else {
			t.routes[identity] = routes[:n]
		}
	}
}

// Announceable returns routes suitable for announcing to a specific peer,
// applying split horizon: routes learned from excludeVia are omitted.
// Withdrawn and expired routes are also excluded.
//
// Split horizon rule: routes where NextHop == excludeVia are not included
// in the announcement. We do NOT send fake hops=16 withdrawals — that
// would violate the per-origin SeqNo invariant.
func (t *Table) Announceable(excludeVia string) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	var result []RouteEntry

	for _, routes := range t.routes {
		for _, r := range routes {
			if r.NextHop == excludeVia {
				continue
			}
			if r.IsWithdrawn() || r.IsExpired(now) {
				continue
			}
			result = append(result, r)
		}
	}
	return result
}

// AnnounceTo returns the wire-safe projection of routes to announce to
// a specific peer, applying split horizon and the +1 hop rule. This is
// the preferred method for building announce_routes frames — it ensures
// the boundary between model and wire format stays in the routing package.
func (t *Table) AnnounceTo(excludeVia string) []AnnounceEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	var result []AnnounceEntry

	for _, routes := range t.routes {
		for _, r := range routes {
			if r.NextHop == excludeVia {
				continue
			}
			if r.IsWithdrawn() || r.IsExpired(now) {
				continue
			}
			result = append(result, r.ToAnnounceEntry())
		}
	}
	return result
}

// Lookup returns all non-withdrawn, non-expired routes for the given identity,
// sorted by preference: source priority (direct > hop_ack > announcement),
// then by hops ascending within the same source tier.
func (t *Table) Lookup(identity string) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	routes := t.routes[identity]
	var result []RouteEntry
	for _, r := range routes {
		if !r.IsWithdrawn() && !r.IsExpired(now) {
			result = append(result, r)
		}
	}

	sortRoutes(result)
	return result
}

// Snapshot returns an immutable point-in-time view of the entire table.
// The returned Snapshot is safe to read without locks.
func (t *Table) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	snap := Snapshot{
		Routes:  make(map[string][]RouteEntry, len(t.routes)),
		TakenAt: now,
	}

	for identity, routes := range t.routes {
		copied := make([]RouteEntry, len(routes))
		copy(copied, routes)
		snap.Routes[identity] = copied
	}
	return snap
}

// Size returns the total number of route entries (including withdrawn).
func (t *Table) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	total := 0
	for _, routes := range t.routes {
		total += len(routes)
	}
	return total
}

// ActiveSize returns the number of non-withdrawn, non-expired entries.
func (t *Table) ActiveSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	total := 0
	for _, routes := range t.routes {
		for _, r := range routes {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				total++
			}
		}
	}
	return total
}

// findByTriple returns the index of the entry with the given triple, or -1.
func findByTriple(routes []RouteEntry, key RouteTriple) int {
	for i := range routes {
		if routes[i].Identity == key.Identity &&
			routes[i].Origin == key.Origin &&
			routes[i].NextHop == key.NextHop {
			return i
		}
	}
	return -1
}

// sortRoutes sorts routes by preference: source priority first
// (direct > hop_ack > announcement), then by hops ascending.
func sortRoutes(routes []RouteEntry) {
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && isBetter(&routes[j], &routes[j-1]); j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}
