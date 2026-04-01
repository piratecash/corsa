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
	routes map[PeerIdentity][]RouteEntry // identity -> routes

	// localOrigin is this node's Ed25519 fingerprint. Used as Origin for
	// direct routes created by AddDirectPeer. Required for AddDirectPeer
	// and RemoveDirectPeer operations.
	localOrigin PeerIdentity

	// seqCounters tracks the next SeqNo to use for own-origin routes,
	// keyed by destination identity. Only the owner of localOrigin may
	// increment these counters.
	seqCounters map[PeerIdentity]uint64

	defaultTTL time.Duration

	// clock is used for time-dependent operations, allowing tests to inject
	// a controllable clock.
	clock func() time.Time

	// flapState tracks per-peer disconnect frequency to detect link
	// flapping. When a peer exceeds flapThreshold disconnects within
	// flapWindow, subsequent reconnections apply penalizedTTL instead
	// of defaultTTL.
	flapState map[PeerIdentity]*peerFlapState

	// Flap detection tuning. Set via options; defaults applied in NewTable.
	flapWindow       time.Duration
	flapThreshold    int
	holdDownDuration time.Duration
	penalizedTTL     time.Duration
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
func WithLocalOrigin(identity PeerIdentity) TableOption {
	return func(t *Table) {
		t.localOrigin = identity
	}
}

// WithFlapWindow overrides the time window for counting disconnect events.
func WithFlapWindow(d time.Duration) TableOption {
	return func(t *Table) {
		t.flapWindow = d
	}
}

// WithFlapThreshold overrides the number of disconnects within flapWindow
// that triggers hold-down.
func WithFlapThreshold(n int) TableOption {
	return func(t *Table) {
		t.flapThreshold = n
	}
}

// WithHoldDownDuration overrides how long a peer stays in hold-down after
// flap detection triggers.
func WithHoldDownDuration(d time.Duration) TableOption {
	return func(t *Table) {
		t.holdDownDuration = d
	}
}

// WithPenalizedTTL overrides the shortened TTL applied to routes created
// during hold-down.
func WithPenalizedTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.penalizedTTL = d
	}
}

// NewTable creates an empty routing table with the given options.
func NewTable(opts ...TableOption) *Table {
	t := &Table{
		routes:           make(map[PeerIdentity][]RouteEntry),
		seqCounters:      make(map[PeerIdentity]uint64),
		flapState:        make(map[PeerIdentity]*peerFlapState),
		defaultTTL:       DefaultTTL,
		clock:            time.Now,
		flapWindow:       DefaultFlapWindow,
		flapThreshold:    DefaultFlapThreshold,
		holdDownDuration: DefaultHoldDownDuration,
		penalizedTTL:     DefaultPenalizedTTL,
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

	// Direct routes must originate from this node. A RouteSourceDirect entry
	// with a foreign Origin would outrank all announcement/hop_ack routes in
	// Lookup and never be eligible for own-origin withdrawal on disconnect.
	if entry.Source == RouteSourceDirect && t.localOrigin != "" && entry.Origin != t.localOrigin {
		return false, ErrDirectForeignOrigin
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
			// A withdrawal tombstone (Hops >= HopsInfinity) must not be
			// replaced by a same-SeqNo update, even if the update has
			// higher trust or fewer hops. Only a strictly newer SeqNo
			// from the origin may supersede a withdrawal. Without this
			// guard, a delayed hop_ack (higher trust rank) could
			// resurrect a withdrawn lineage.
			if old.Hops >= HopsInfinity {
				// Already withdrawn — reject same-seq replacement.
			} else if entry.Source.TrustRank() > old.Source.TrustRank() {
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
func (t *Table) WithdrawRoute(identity, origin, nextHop PeerIdentity, seqNo uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	existing := t.routes[identity]
	key := RouteTriple{Identity: identity, Origin: origin, NextHop: nextHop}
	idx := findByTriple(existing, key)

	if idx < 0 {
		// No active route for this triple — store a tombstone so that a
		// delayed older announcement with a lower SeqNo cannot resurrect
		// the withdrawn lineage. The tombstone expires via normal TTL.
		tombstone := RouteEntry{
			Identity:  identity,
			Origin:    origin,
			NextHop:   nextHop,
			Hops:      HopsInfinity,
			SeqNo:     seqNo,
			Source:    RouteSourceAnnouncement,
			ExpiresAt: now.Add(t.defaultTTL),
		}
		t.routes[identity] = append(existing, tombstone)
		return true
	}

	old := &existing[idx]
	if seqNo <= old.SeqNo {
		return false
	}

	existing[idx].Hops = HopsInfinity
	existing[idx].SeqNo = seqNo
	existing[idx].ExpiresAt = now.Add(t.defaultTTL)
	return true
}

// AddDirectPeerResult describes the outcome of AddDirectPeer.
type AddDirectPeerResult struct {
	// Entry is the route entry that was created or refreshed.
	Entry RouteEntry

	// Penalized is true when the peer triggered flap detection and the
	// route was created with a shortened TTL. The caller can use this
	// to decide whether to delay or suppress the announcement.
	Penalized bool
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
// Flap dampening: if the peer is in hold-down (too many recent
// disconnects), the route is created with penalizedTTL instead of
// defaultTTL, and Result.Penalized is set. The caller can use this
// to suppress or delay the triggered announcement.
//
// Returns AddDirectPeerResult and nil error on success.
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) AddDirectPeer(peerIdentity PeerIdentity) (AddDirectPeerResult, error) {
	if t.localOrigin == "" {
		return AddDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return AddDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	penalized := t.isPeerInHoldDownLocked(peerIdentity, now)

	key := RouteTriple{
		Identity: peerIdentity,
		Origin:   t.localOrigin,
		NextHop:  peerIdentity,
	}
	existing := t.routes[peerIdentity]
	idx := findByTriple(existing, key)

	// Already active — refresh TTL only, no SeqNo bump.
	if idx >= 0 && !existing[idx].IsWithdrawn() {
		ttl := t.defaultTTL
		if penalized {
			ttl = t.penalizedTTL
		}
		existing[idx].ExpiresAt = now.Add(ttl)
		return AddDirectPeerResult{Entry: existing[idx], Penalized: penalized}, nil
	}

	// New route or re-activation after withdrawal — increment SeqNo.
	seq := t.nextSeqLocked(peerIdentity)

	ttl := t.defaultTTL
	if penalized {
		ttl = t.penalizedTTL
	}

	entry := RouteEntry{
		Identity:  peerIdentity,
		Origin:    t.localOrigin,
		NextHop:   peerIdentity,
		Hops:      1,
		SeqNo:     seq,
		Source:    RouteSourceDirect,
		ExpiresAt: now.Add(ttl),
	}

	if idx < 0 {
		t.routes[peerIdentity] = append(existing, entry)
	} else {
		existing[idx] = entry
	}

	return AddDirectPeerResult{Entry: entry, Penalized: penalized}, nil
}

// isPeerInHoldDownLocked checks if the peer is currently in flap hold-down.
// Must be called with t.mu held.
func (t *Table) isPeerInHoldDownLocked(peerIdentity PeerIdentity, now time.Time) bool {
	fs := t.flapState[peerIdentity]
	if fs == nil {
		return false
	}
	return now.Before(fs.holdDownUntil)
}

// recordWithdrawalLocked tracks a disconnect event for flap detection.
// If the withdrawal count within flapWindow crosses flapThreshold,
// hold-down is activated. Must be called with t.mu held.
func (t *Table) recordWithdrawalLocked(peerIdentity PeerIdentity, now time.Time) {
	fs := t.flapState[peerIdentity]
	if fs == nil {
		fs = &peerFlapState{}
		t.flapState[peerIdentity] = fs
	}

	fs.withdrawTimes = append(fs.withdrawTimes, now)

	// Trim events outside the window.
	cutoff := now.Add(-t.flapWindow)
	trimmed := fs.withdrawTimes[:0]
	for _, wt := range fs.withdrawTimes {
		if !wt.Before(cutoff) {
			trimmed = append(trimmed, wt)
		}
	}
	fs.withdrawTimes = trimmed

	if len(fs.withdrawTimes) >= t.flapThreshold {
		fs.holdDownUntil = now.Add(t.holdDownDuration)
	}
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
func (t *Table) RemoveDirectPeer(peerIdentity PeerIdentity) (RemoveDirectPeerResult, error) {
	if t.localOrigin == "" {
		return RemoveDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return RemoveDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.recordWithdrawalLocked(peerIdentity, now)

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
				r.ExpiresAt = now.Add(t.defaultTTL)

				result.Withdrawals = append(result.Withdrawals, AnnounceEntry{
					Identity: r.Identity,
					Origin:   r.Origin,
					Hops:     HopsInfinity,
					SeqNo:    seq,
				})
			} else {
				result.TransitInvalidated++
				r.Hops = HopsInfinity
				r.ExpiresAt = now.Add(t.defaultTTL)
			}
		}
	}

	return result, nil
}

// InvalidateTransitRoutes sets hops=HopsInfinity on all non-direct routes
// whose NextHop matches peerIdentity. Unlike RemoveDirectPeer this does
// NOT generate wire withdrawals and does NOT touch direct routes — it is
// a defense-in-depth cleanup for peers that had mesh_routing_v1 (could
// advertise routes) but not mesh_relay_v1 (no direct route was created).
// Returns the number of transit routes invalidated.
func (t *Table) InvalidateTransitRoutes(peerIdentity PeerIdentity) int {
	if peerIdentity == "" {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	invalidated := 0
	for _, routes := range t.routes {
		for i := range routes {
			r := &routes[i]
			if r.NextHop != peerIdentity || r.Hops >= HopsInfinity {
				continue
			}
			if r.Source == RouteSourceDirect {
				continue
			}
			r.Hops = HopsInfinity
			r.ExpiresAt = now.Add(t.defaultTTL)
			invalidated++
		}
	}
	return invalidated
}

// nextSeqLocked increments and returns the next SeqNo for a given identity.
// Must be called with t.mu held.
func (t *Table) nextSeqLocked(identity PeerIdentity) uint64 {
	t.seqCounters[identity]++
	return t.seqCounters[identity]
}

// TickTTL removes expired routes from the table and cleans up stale
// flap detection state. Should be called periodically (e.g., every
// second or every few seconds).
//
// Only ExpiresAt is checked — withdrawn (Hops >= HopsInfinity) entries
// are kept until their ExpiresAt elapses. This preserves tombstones
// created by WithdrawRoute that guard against resurrection from delayed
// lower-SeqNo announcements. RemoveDirectPeer and InvalidateTransitRoutes
// set a short ExpiresAt on withdrawn entries so they are cleaned up
// promptly without breaking tombstone semantics.
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

	// Clean up flap state where both hold-down has expired and all
	// withdrawal timestamps are outside the window.
	cutoff := now.Add(-t.flapWindow)
	for peer, fs := range t.flapState {
		if !now.Before(fs.holdDownUntil) {
			// Trim stale withdrawal events.
			n := 0
			for _, wt := range fs.withdrawTimes {
				if !wt.Before(cutoff) {
					fs.withdrawTimes[n] = wt
					n++
				}
			}
			fs.withdrawTimes = fs.withdrawTimes[:n]

			if n == 0 {
				delete(t.flapState, peer)
			}
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
func (t *Table) Announceable(excludeVia PeerIdentity) []RouteEntry {
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
func (t *Table) AnnounceTo(excludeVia PeerIdentity) []AnnounceEntry {
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
func (t *Table) Lookup(identity PeerIdentity) []RouteEntry {
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
// All fields (routes, counts, flap state) are captured under a single
// lock acquisition, ensuring a self-consistent response for RPC consumers.
func (t *Table) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	snap := Snapshot{
		Routes:  make(map[PeerIdentity][]RouteEntry, len(t.routes)),
		TakenAt: now,
	}

	totalEntries := 0
	activeEntries := 0
	for ident, routes := range t.routes {
		copied := make([]RouteEntry, len(routes))
		copy(copied, routes)
		snap.Routes[ident] = copied
		totalEntries += len(routes)
		for _, r := range routes {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				activeEntries++
			}
		}
	}
	snap.TotalEntries = totalEntries
	snap.ActiveEntries = activeEntries

	// Capture flap state atomically with routes.
	cutoff := now.Add(-t.flapWindow)
	for peer, fs := range t.flapState {
		recentCount := 0
		for _, wt := range fs.withdrawTimes {
			if !wt.Before(cutoff) {
				recentCount++
			}
		}
		inHoldDown := now.Before(fs.holdDownUntil)
		if recentCount == 0 && !inHoldDown {
			continue
		}
		snap.FlapState = append(snap.FlapState, FlapEntry{
			PeerIdentity:      peer,
			RecentWithdrawals: recentCount,
			InHoldDown:        inHoldDown,
			HoldDownUntil:     fs.holdDownUntil,
		})
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

// FlapSnapshot returns the current flap detection state for all tracked peers.
// Stale entries are filtered: withdrawals outside the flap window are trimmed,
// and peers with no recent withdrawals and no active hold-down are excluded.
// This avoids reporting false positives between TickTTL cleanup cycles.
func (t *Table) FlapSnapshot() []FlapEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	cutoff := now.Add(-t.flapWindow)
	var entries []FlapEntry
	for peer, fs := range t.flapState {
		// Count only withdrawals within the current flap window.
		recentCount := 0
		for _, wt := range fs.withdrawTimes {
			if !wt.Before(cutoff) {
				recentCount++
			}
		}
		inHoldDown := now.Before(fs.holdDownUntil)

		// Skip peers with no recent withdrawals and no active hold-down.
		if recentCount == 0 && !inHoldDown {
			continue
		}

		entries = append(entries, FlapEntry{
			PeerIdentity:      peer,
			RecentWithdrawals: recentCount,
			InHoldDown:        inHoldDown,
			HoldDownUntil:     fs.holdDownUntil,
		})
	}
	return entries
}

// LocalOrigin returns this node's identity string.
func (t *Table) LocalOrigin() PeerIdentity {
	return t.localOrigin
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
