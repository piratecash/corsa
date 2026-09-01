package node

import (
	"sync"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// emissionLane decides who writes the emission journal next.
//
// Dropping the old journal mutex let both writers reach SQLite together,
// which is not the same as writing together: SQLite takes one writer at a
// time, and the pool (internal/core/storage) is not clamped, so every
// stamp can hold a connection of its own and sit in the busy handler. Who
// gets the write lock next is then decided by retry timing, not by need —
// and the write with the most need is the pre-wire withdrawal, because a
// frame the user is waiting on cannot go out until it lands, and a clear
// that runs out of busy_timeout WITHHOLDS the message. A reconnect
// replaying a whole conversation is exactly when both happen at once.
//
// Three rules, and each of them exists because a weaker version of this
// lane was found not to hold:
//
//   - PRIORITY. A withdrawal is served before any bookkeeping. Bookkeeping
//     records a frame that has already gone; a withdrawal is what a frame
//     is waiting for.
//   - FIFO WITHIN A CLASS. Turns are taken in arrival order. A mutex plus
//     a condition variable is not a queue: a writer that has just released
//     the lane and wants it back competes with whoever was already waiting
//     and can win repeatedly, so "the next turn goes to the waiter" was
//     true only by luck.
//   - ONE STATEMENT PER TURN. A turn writes at most maxIdsPerLaneWrite
//     ids; a writer with more goes to the BACK of the queue for the rest.
//     A backlog replay confirms a whole conversation in one call, and
//     while a turn was a whole call, a message typed a moment later waited
//     for every statement of it.
//
// Coalescing sits on top: when a withdrawal takes its turn it absorbs the
// withdrawals queued behind it, up to one statement's worth. Several
// senders reconnecting at once then cost one write rather than one each.
// It is capped at one statement deliberately — an unbounded batch would
// re-create the very defect the third rule removes, by tying a newcomer's
// fate to a whole conversation's worth of ids.
//
// The size of the problem is worth recording so nobody trusts the lane
// for more than it is: measured, 64 goroutines writing full-conversation
// stamps in a loop cost the worst pre-wire clear on the same database
// under 3 ms — these statements are short and the write lock changes
// hands quickly. The lane is not there to rescue a routine send. It is
// there because the cost of losing that race is asymmetric, and because
// bounding the queue is worth having unconditionally: a reconnect backlog
// otherwise puts an unbounded number of goroutines on the database.
//
// What the lane does NOT promise is a fast write. The state database is
// shared with every other subsystem, so a statement inside the lane can
// still be parked on another writer's transaction for the whole busy
// timeout. Bounding that would mean clamping the pool for the whole
// database, which would serialise reads as well — a decision for the
// storage layer, not for delivery.
type emissionLane struct {
	mu   sync.Mutex
	free *sync.Cond
	// busy is "a statement is inside the journal right now".
	busy bool
	// queue is every writer waiting for a turn, in arrival order.
	queue []*laneWaiter
	// bookkeeping bounds how many stamps may be WAITING. Overflow costs
	// nothing to correctness because the debt it declines is a pure
	// function of state that repairLocalDeliveryRecord re-derives.
	bookkeeping chan struct{}
}

// laneWaiter is one writer's claim on the journal.
type laneWaiter struct {
	urgent bool
	// ids not yet written. A turn takes one statement's worth off the
	// front; whatever is left sends the waiter to the back of the queue.
	ids []protocol.MessageID
	// absorbed is set when another withdrawal's turn took these ids into
	// its own statement. The absorber reports the result through done.
	absorbed bool
	done     chan struct{}
	err      error
}

// maxIdsPerLaneWrite is how many ids one turn may write. It matches the
// journal's own statement chunk (chatlog: emissionMarkBatch = 128, which
// is bounded by how many placeholders SQLite takes), so ONE turn is ONE
// statement. If the journal ever chunks smaller, a turn becomes several
// statements and the wait degrades in proportion — it does not break, but
// the guarantee weakens, so the two are worth keeping equal.
const maxIdsPerLaneWrite = 128

// maxWaitingStampWrites is how many bookkeeping writes may queue for the
// lane. A reconnect confirms a whole conversation in batches, and the
// repair pass re-derives whatever the lane turns away, so this bounds
// goroutines rather than gating throughput.
const maxWaitingStampWrites = 8

func newEmissionLane() *emissionLane {
	lane := &emissionLane{bookkeeping: make(chan struct{}, maxWaitingStampWrites)}
	lane.free = sync.NewCond(&lane.mu)
	return lane
}

// runPreWire withdraws the claim on ids with priority: it is served before
// any bookkeeping, in arrival order among withdrawals, and it never waits
// for more than one statement per writer ahead of it. It is never turned
// away — the caller's only alternative to writing is withholding the
// user's message.
//
// The price of coalescing is shared fate: a failed statement fails every
// caller whose ids were in it, so a withdrawal that would have succeeded
// alone can be reported as failed. That errs towards withholding the
// frame, which is the safe direction, and the next tick tries again.
func (l *emissionLane) runPreWire(ids []protocol.MessageID, write func([]protocol.MessageID) error) error {
	if l == nil {
		return write(ids)
	}
	return l.take(true, ids, write)
}

// runBookkeeping records ids unless the lane already has its fill of
// waiting bookkeeping, in which case it reports admitted=false and writes
// NOTHING. A caller that is not admitted must treat the record as still
// owed, exactly as it treats a failed write.
func (l *emissionLane) runBookkeeping(ids []protocol.MessageID, write func([]protocol.MessageID) error) (admitted bool, err error) {
	if l == nil {
		return true, write(ids)
	}
	select {
	case l.bookkeeping <- struct{}{}:
	default:
		return false, nil
	}
	defer func() { <-l.bookkeeping }()
	return true, l.take(false, ids, write)
}

// take queues for the lane and writes ids a statement at a time, going to
// the back of the queue between statements.
func (l *emissionLane) take(urgent bool, ids []protocol.MessageID, write func([]protocol.MessageID) error) error {
	me := &laneWaiter{urgent: urgent, ids: ids, done: make(chan struct{})}

	l.mu.Lock()
	l.queue = append(l.queue, me)
	for {
		if me.absorbed {
			// Another withdrawal's statement covered these ids.
			l.mu.Unlock()
			<-me.done
			return me.err
		}
		if !l.busy && l.nextLocked() == me {
			break
		}
		l.free.Wait()
	}

	for {
		// Our turn: leave the queue, take one statement's worth, and let
		// the withdrawals queued behind us ride along if they fit.
		l.removeLocked(me)
		l.busy = true
		batch := me.ids
		if len(batch) > maxIdsPerLaneWrite {
			batch, me.ids = me.ids[:maxIdsPerLaneWrite], me.ids[maxIdsPerLaneWrite:]
		} else {
			me.ids = nil
		}
		var absorbed []*laneWaiter
		if urgent {
			absorbed, batch = l.absorbLocked(batch)
		}
		l.mu.Unlock()

		err := write(batch)

		l.mu.Lock()
		l.busy = false
		more := err == nil && len(me.ids) > 0
		if more {
			// The rest of a long write waits its turn again, BEHIND
			// everyone who arrived meanwhile. Re-entering as if it had
			// never left is how a big writer starves a small one.
			l.queue = append(l.queue, me)
		}
		l.mu.Unlock()
		l.free.Broadcast()
		for _, waiter := range absorbed {
			waiter.err = err
			close(waiter.done)
		}
		if !more {
			return err
		}

		l.mu.Lock()
		for l.busy || l.nextLocked() != me {
			l.free.Wait()
		}
	}
}

// nextLocked is whose turn it is: the oldest withdrawal if any is waiting,
// otherwise the oldest writer of any kind.
//
// Caller MUST hold l.mu.
func (l *emissionLane) nextLocked() *laneWaiter {
	for _, waiter := range l.queue {
		if waiter.urgent {
			return waiter
		}
	}
	if len(l.queue) > 0 {
		return l.queue[0]
	}
	return nil
}

// absorbLocked takes the withdrawals queued behind this turn into its
// statement, while they fit in one.
//
// Caller MUST hold l.mu.
func (l *emissionLane) absorbLocked(batch []protocol.MessageID) ([]*laneWaiter, []protocol.MessageID) {
	var absorbed []*laneWaiter
	for _, waiter := range l.queue {
		if !waiter.urgent || len(batch)+len(waiter.ids) > maxIdsPerLaneWrite {
			continue
		}
		batch = append(batch, waiter.ids...)
		waiter.ids = nil
		waiter.absorbed = true
		absorbed = append(absorbed, waiter)
	}
	for _, waiter := range absorbed {
		l.removeLocked(waiter)
	}
	return absorbed, batch
}

// removeLocked drops one waiter from the queue, keeping arrival order.
//
// Caller MUST hold l.mu.
func (l *emissionLane) removeLocked(target *laneWaiter) {
	for i, waiter := range l.queue {
		if waiter == target {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			return
		}
	}
}

// waiting reports how many writers of each kind are queued for the lane.
// A test that has to arrange "queued behind a statement in flight" needs
// to know when they actually are, and sleeping instead is the difference
// between proving the ordering and proving nothing.
func (l *emissionLane) waiting() (urgent, bookkeeping int) {
	l.mu.Lock()
	for _, waiter := range l.queue {
		if waiter.urgent {
			urgent++
		}
	}
	l.mu.Unlock()
	return urgent, len(l.bookkeeping)
}
