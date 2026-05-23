package routing

// This file holds the public Table-level entry points into flap
// detection. The actual state machine lives in FlapDetector (see
// flap.go); these wrappers exist because the flap APIs are part of
// Table's public surface and because they own the t.mu acquisition
// that FlapDetector's *Locked methods require.

// RecordSuccessfulRouteAdd clears the consecutive-flap counter for a
// peer that has demonstrated stability (e.g. a successful announce-plane
// round-trip after the hold-down expired). The withdrawTimes history
// is left untouched so a single successful add does not erase the
// flap-rate evidence inside the current flapWindow — that history
// trims itself naturally as the window slides forward. Callers in
// node.Service invoke this on the post-handshake path after a
// successful announce_routes exchange.
//
// Empty peerIdentity short-circuits as a no-op so callers can pass
// zero-value identities through without a nil-check.
func (t *Table) RecordSuccessfulRouteAdd(peerIdentity PeerIdentity) {
	if peerIdentity == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// FlapDetector.clearStableLocked returns whether any field
	// actually changed. consecutiveFlaps and lastFlapAt do not
	// appear in Snapshot directly, but they alter the future
	// evolution of holdDownUntil (subsequent flap-bursts that
	// would have escalated now reset to base duration), and
	// FlapEntry.HoldDownUntil is part of the snapshot. Mark
	// dirty only when the fields actually changed to avoid a
	// republish on a no-op call.
	if t.flap.clearStableLocked(peerIdentity) {
		t.dirty.Store(true)
	}
}

// FlapSnapshot returns the current flap detection state for all tracked peers.
// Stale entries are filtered: withdrawals outside the flap window are trimmed,
// and peers with no recent withdrawals and no active hold-down are excluded.
// This avoids reporting false positives between TickTTL cleanup cycles.
func (t *Table) FlapSnapshot() []FlapEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.flap.snapshotLocked(t.clock())
}
