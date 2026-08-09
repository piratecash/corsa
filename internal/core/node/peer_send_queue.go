package node

// peer_send_queue.go owns the UPPER of the two outbound queues on the
// session path. A frame handed to an outbound session lands in
// peerSession.sendCh first; only the servePeerSession loop moves it into
// the NetCore writer queue. Both queues therefore have to answer for the
// frames they hold: a frame that dies in the upper queue never reaches
// NetCore, so NetCore cannot possibly report its fate.

import (
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// peerSendItem is one element of the outbound session queue: the frame plus
// the optional outbound contract that must survive the hop into the NetCore
// queue. The same ticket pointer is handed to NetCore on dequeue, which is what
// carries the frame's send deadline and write grace across BOTH queues instead
// of losing them at the boundary. A ticket-less item (ticket == nil) is a
// legacy frame and behaves exactly as before.
// The frame is embedded rather than held in a named field so that the queue
// element reads as "a frame that additionally carries a contract" at every
// call site, instead of forcing a rename of every existing .Type / .ID
// access on the outbound path.
type peerSendItem struct {
	protocol.Frame
	ticket *netcore.WriteTicket
}

// legacyPeerSendItem wraps a frame that carries no outbound contract.
func legacyPeerSendItem(frame protocol.Frame) peerSendItem {
	return peerSendItem{Frame: frame}
}

// tracked reports whether the item carries an outbound contract. Tracked
// items always take the managed writer path in servePeerSession, never the
// request/reply path: the contract exists to bound how long the frame may sit
// before its socket write, and the request path would hold the serve loop
// waiting for an answer that a one-way frame has no reason to send.
func (item peerSendItem) tracked() bool {
	return item.ticket != nil
}

// enqueueSend offers item to the session's outbound queue.
//
// Ownership protocol for sendCh — the reason this is a mutex and not a
// closed channel:
//
//   - producers (enqueuePeerFrame, the identity-addressed walk of
//     sendFrameToIdentity, the announce plane, the pending-ring flush) run on
//     arbitrary goroutines and must never block and never panic;
//   - the session owner must be able to declare the queue dead and then
//     account for every element left in it.
//
// Closing sendCh would satisfy the second point and break the first: a
// producer racing the close panics on send. So the channel is never closed.
// Instead sendClosed is flipped under sendMu, and every producer performs
// its non-blocking send while holding the same mutex. The flip therefore
// waits out any producer already inside the send, and any producer that
// arrives later is refused. Once closeSendQueue has flipped the flag, no
// new element can appear, so draining after the flip is guaranteed to see
// the whole residue.
//
// Refusal is reported to the caller (false) rather than swallowed, because
// the caller owns the fallback policy (pending ring, inbound fallback): a
// frame the queue would not accept was evicted at the door and provably never
// started a write, and only the caller knows whether it is worth another route.
func (ps *peerSession) enqueueSend(item peerSendItem) bool {
	if ps == nil {
		return false
	}
	return ps.offerSend(item)
}

// offerSend performs the guarded non-blocking offer. It takes ps.sendMu itself
// — the name carries no *Locked suffix for exactly that reason — and it is a
// function of its own so the mutex is held for the offer and for nothing else.
func (ps *peerSession) offerSend(item peerSendItem) bool {
	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()
	if ps.sendClosed {
		return false
	}
	select {
	case ps.sendCh <- item:
		return true
	default:
		return false
	}
}

// closeSendQueue fences off every producer and discards the whole residue of
// the upper queue. Called on every exit of servePeerSession (whatever the
// cause) and from peerSession.Close() for sessions that die before the serve
// loop ever starts.
//
// The drain is not bookkeeping: without it the frames of a dead session stay
// referenced by the buffered channel for as long as anything holds the session,
// and a peer that flaps leaves one such buffer per attempt.
//
// Idempotent by construction: the fence is monotonic and the drain is a
// non-blocking sweep, which is what makes it safe for both the serve loop and a
// concurrent Close() to call it.
func (ps *peerSession) closeSendQueue() {
	if ps == nil {
		return
	}
	ps.sendMu.Lock()
	ps.sendClosed = true
	ps.sendMu.Unlock()

	// Producers are already refused by the flag above, so nothing can be added
	// behind the sweep.
	for {
		select {
		case <-ps.sendCh:
		default:
			return
		}
	}
}

// sendQueueLen reports the number of frames waiting in the upper queue.
// Diagnostics and tests only.
func (ps *peerSession) sendQueueLen() int {
	if ps == nil {
		return 0
	}
	return len(ps.sendCh)
}
