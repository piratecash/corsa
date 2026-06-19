package routing

// defaultRouteChangeLogCapacity is the change-journal ring size. It must
// comfortably exceed the number of route/health changes a busy node accumulates
// within one forced-full-sync window (≤ DefaultTTL/2), so a peer synced at the
// previous forced-full is still served incrementally rather than tripping the
// overflow force-full. Sized generously for the shadow stage and recalibrated
// from the observed changes/window metric. ~16k changes is well above a dense
// node's per-minute churn.
const defaultRouteChangeLogCapacity = 16384

// routeChangeLog is the Phase 3 (announce delta-cursor) change journal: a
// fixed-size ring of destination identities whose announce projection MIGHT
// have changed. Each route/health mutation that today marks the table dirty
// appends the affected identity here; a per-peer cursor later reads the set of
// identities changed since it last synced (sinceLocked).
//
// DEPLOY-1 SCOPE (shadow): the log is recorded and read only to validate
// completeness — the authoritative delta is still ComputeDelta over the
// rebuilt snapshot. The shadow check asserts that every identity ComputeDelta
// actually emitted is covered by the log; a miss means a mutation site was not
// wired and is reported (it is the whole point of the shadow stage). The log
// does NOT yet drive what is sent, so an incomplete wiring is observable but
// harmless.
//
// Concurrency: the log has no mutex of its own. It is owned by routing.Table
// and every method requires the caller to hold t.mu (write mode for record*,
// at least read for sinceLocked — though sinceLocked is in practice called
// under the same write lock as AnnounceProjectionFor). This mirrors the
// snapDirtyIDs / healthStore ownership model.
type routeChangeLog struct {
	// ring holds the last len(ring) appended identities. The entry for
	// monotonic sequence number s lives at ring[s % len(ring)].
	ring []PeerIdentity

	// head is the total number of appends; the next sequence number to
	// assign equals head, and the valid (still-in-ring) sequence range is
	// [head-len(ring), head).
	head uint64

	// fullResetSeq is the head value at the most recent bulk mutation
	// (recordFullLocked). A cursor strictly below it cannot be served from
	// the ring (the bulk change touched an unbounded identity set that was
	// never enumerated into the ring) and must force a full sync.
	fullResetSeq uint64
}

// newRouteChangeLog returns a change log with the given ring capacity. A
// capacity < 1 is clamped to 1 so the modulo is always well-defined.
func newRouteChangeLog(capacity int) *routeChangeLog {
	if capacity < 1 {
		capacity = 1
	}
	return &routeChangeLog{ring: make([]PeerIdentity, capacity)}
}

// recordLocked appends a single-identity change. Caller must hold t.mu in
// write mode. A zero identity is ignored (no destination to attribute).
func (l *routeChangeLog) recordLocked(identity PeerIdentity) {
	if identity.IsZero() {
		return
	}
	l.ring[l.head%uint64(len(l.ring))] = identity
	l.head++
}

// recordFullLocked marks a bulk mutation that touched an unbounded identity
// set (the analogue of markSnapFullDirtyLocked): any peer whose cursor is at or
// below the reset point must force a full sync because the changed identities
// were not enumerated into the ring. Caller must hold t.mu in write mode.
//
// The reset OCCUPIES a sequence position (head advances by one) so that a peer
// caught up to the old head (cursor == old head) is distinguishable from a peer
// that re-synced after the reset (cursor == new head): the former force-fulls
// (cursor < fullResetSeq), the latter does not (cursor == fullResetSeq). The
// occupied ring slot is left zero and is never enumerated — any cursor that
// would reach it is strictly below fullResetSeq and force-fulls first.
func (l *routeChangeLog) recordFullLocked() {
	l.ring[l.head%uint64(len(l.ring))] = PeerIdentity{}
	l.head++
	l.fullResetSeq = l.head
}

// headLocked returns the current head (the cursor value a freshly-synced peer
// should store). Caller must hold t.mu.
func (l *routeChangeLog) headLocked() uint64 {
	return l.head
}

// sinceUpToLocked returns the DISTINCT identities changed in the sequence range
// [cursor, bound) and needFull. `bound` is a head value captured earlier under
// the same t.mu (e.g. atomically with the announce projection), so the result
// reflects exactly the mutations that snapshot saw — not any that landed after.
//
// needFull is true when the range cannot be served incrementally:
//   - cursor is strictly below a bulk reset (fullResetSeq), or
//   - the cursor fell out of the ring (live head-cursor > capacity, i.e. the
//     peer is further behind than the ring can remember; checked against the
//     LIVE head because ring slots for [cursor, bound) are overwritten relative
//     to the live head, not bound).
//
// In both cases the caller must force a full sync; `changed` is nil.
//
// A cursor at or ahead of bound (the steady-state "nothing changed" case)
// returns an empty set, needFull=false. bound is clamped to the live head
// defensively. Caller must hold t.mu.
func (l *routeChangeLog) sinceUpToLocked(cursor, bound uint64) (changed []PeerIdentity, needFull bool) {
	if bound > l.head {
		bound = l.head
	}
	if cursor >= bound {
		return nil, false
	}
	if cursor < l.fullResetSeq || l.head-cursor > uint64(len(l.ring)) {
		return nil, true
	}
	seen := make(map[PeerIdentity]struct{})
	for s := cursor; s < bound; s++ {
		id := l.ring[s%uint64(len(l.ring))]
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		changed = append(changed, id)
	}
	return changed, false
}
