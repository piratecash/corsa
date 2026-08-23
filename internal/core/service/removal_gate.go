package service

import (
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
)

// removalGate arbitrates between removing a conversation and writing to it.
//
// One gate, two doors. The router asks it before creating a sidebar row, so
// nothing rebuilds the conversation behind the removal. The message store
// asks it before writing an inbound DM, because that is the only door the
// node's own writes go through: the node stores a message BEFORE the router
// hears about it, so a message accepted while the removal runs would land in
// the database no matter what the router refuses afterwards, and the next
// startup would build the deleted conversation out of it.
//
// The store DEFERS such a message rather than dropping it: deferred keeps it
// with the sender, who re-delivers once the removal is over — and then it is
// simply a new message to a conversation that no longer exists, which opens
// it again, exactly as a message from any stranger would. The gate is a
// window, not a ban.
//
// A flag that a writer merely CHECKS is not enough. Checking is not writing:
// between the two, a whole removal can start and finish, and the write then
// lands behind both of its history deletes, where nothing will ever look
// again. So a write takes a LEASE (admitWrite) that it holds until its row
// is committed, and begin does not return until every lease already handed
// out for that conversation is back. After begin returns, the removal knows
// two things it could not know before: no write is in progress, and no new
// one will be admitted.
//
// Counted rather than flagged in both directions: two removals of the same
// contact can overlap, and the first to finish must not open the door under
// the second; several writes to one conversation can be in flight at once.
type removalGate struct {
	mu       sync.Mutex
	drained  *sync.Cond
	removals map[domain.PeerIdentity]int
	writers  map[domain.PeerIdentity]int
}

func newRemovalGate() *removalGate {
	g := &removalGate{
		removals: make(map[domain.PeerIdentity]int),
		writers:  make(map[domain.PeerIdentity]int),
	}
	g.drained = sync.NewCond(&g.mu)
	return g
}

// begin marks a removal as started, waits for the writes already admitted to
// this conversation to finish, and returns its release.
//
// Called BEFORE any waiting the removal does — a gate raised after a lock or
// a disk write has been waited for is a gate with a hole exactly the size of
// that wait. The wait for the admitted writes is bounded by a single chatlog
// append; the removal is holding no lock of its own at this point, which is
// why this is the first thing it does.
func (g *removalGate) begin(peer domain.PeerIdentity) func() {
	if g == nil || peer.IsZero() {
		return func() {}
	}
	g.mu.Lock()
	g.removals[peer]++
	// From here on admitWrite refuses, so this waits for a set that can
	// only shrink.
	for g.writers[peer] > 0 {
		g.drained.Wait()
	}
	g.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if left := g.removals[peer] - 1; left > 0 {
				g.removals[peer] = left
			} else {
				delete(g.removals, peer)
			}
			g.mu.Unlock()
			g.drained.Broadcast()
		})
	}
}

// admitWrite takes a lease on writing to this conversation. It reports false
// when a removal is in flight — the caller must not write, and must say so
// to whoever offered the message. When it reports true the caller MUST call
// the returned release once its write is committed or abandoned: a removal
// may already be waiting on it.
func (g *removalGate) admitWrite(peer domain.PeerIdentity) (func(), bool) {
	if g == nil || peer.IsZero() {
		return func() {}, true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.removals[peer] > 0 {
		return nil, false
	}
	g.writers[peer]++

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if left := g.writers[peer] - 1; left > 0 {
				g.writers[peer] = left
			} else {
				delete(g.writers, peer)
			}
			g.mu.Unlock()
			g.drained.Broadcast()
		})
	}, true
}

// writesInFlight reports how many admitted writes this conversation still
// has open. The lease is invisible from outside otherwise: whether the store
// holds it across its append — the whole point of it — cannot be observed
// through admitWrite, and a property nothing can observe is a property no
// test can pin.
func (g *removalGate) writesInFlight(peer domain.PeerIdentity) int {
	if g == nil || peer.IsZero() {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.writers[peer]
}

// removing reports whether a removal of this conversation is in flight.
// For callers that decide and act under one lock of their own (the router
// creating a sidebar row) — a caller that goes on to do I/O needs a lease,
// not an answer.
func (g *removalGate) removing(peer domain.PeerIdentity) bool {
	if g == nil || peer.IsZero() {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.removals[peer] > 0
}
