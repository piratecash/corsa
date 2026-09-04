package node

// peer_send_queue.go owns the UPPER of the two outbound queues on the
// session path. A frame handed to an outbound session lands in
// peerSession.sendCh first; only the servePeerSession loop moves it into
// the NetCore writer queue. Both queues therefore have to answer for the
// frames they hold: a frame that dies in the upper queue never reaches
// NetCore, so NetCore cannot possibly report its fate.

import (
	"errors"
	"time"

	"github.com/rs/zerolog/log"

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
	// delivery names the sender-owned dispatch this frame belongs to, for
	// the frames that carry one of OUR outgoing messages. Zero for
	// everything else — transit traffic, announce-plane frames, receipts.
	//
	// It travels WITH the frame because the confirmation happens at the
	// far end of the queue, in the session's writer loop, and the two
	// facts it needs are decided here: which message, and which attempt.
	// Reading the clock at the writer instead made every sink of one
	// dispatch look like a separate attempt, so a single send charged the
	// backoff several times over.
	delivery deliveryDispatchRef
	// writeAck, when non-nil, is closed by the netcore writer once this
	// frame's bytes have left the process. It travels with the element for
	// the same reason the ticket does: the write happens at the far end of
	// two queues, and a side table keyed by queue element would have to be
	// kept in sync with both.
	//
	// Nil for everything except a liveness probe. Only that caller has to
	// tell "they did not answer" from "we never managed to ask", because only
	// it turns silence into a claim about another person. See
	// netcore.SendTrackedObserved.
	//
	// If this queue discards the item — the session closed, the ring
	// overflowed — the channel is simply never closed, which is the correct
	// answer: the frame did not reach a socket.
	writeAck chan struct{}
}

// deliveryDispatchRef identifies one attempt at one of our own messages.
// The zero value means "not one of ours"; see peerSendItem.delivery.
type deliveryDispatchRef struct {
	Envelope     protocol.Envelope
	DispatchedAt time.Time
}

// carriesDelivery reports whether the item belongs to a sender-owned
// dispatch whose confirmation the writer owes.
func (i peerSendItem) carriesDelivery() bool {
	return i.delivery.Envelope.ID != "" && !i.delivery.DispatchedAt.IsZero()
}

// legacyPeerSendItem wraps a frame that carries no outbound contract.
func legacyPeerSendItem(frame protocol.Frame) peerSendItem {
	return peerSendItem{Frame: frame}
}

// deliveryPeerSendItem wraps a frame that carries one of our own outgoing
// messages, so the session writer can confirm the delivery once NetCore
// accepts it.
func deliveryPeerSendItem(frame protocol.Frame, envelope protocol.Envelope, dispatchedAt time.Time) peerSendItem {
	return peerSendItem{Frame: frame, delivery: deliveryDispatchRef{Envelope: envelope, DispatchedAt: dispatchedAt}}
}

// recordDeliveryRefusedByWriter logs a frame the writer would not take.
//
// It writes NOTHING durable, and that is the point of the two-bit model
// rather than an omission. After a refusal the row already says both of
// the things its readers need: the never-emitted claim came off at the
// gate, so a deletion asks the peer rather than skipping them; and no
// on-wire stamp was added, so the sender still reads the message as
// queued. There is no fact left to record — and therefore none that can be
// recorded wrongly, which is what every earlier version of this function
// got wrong in a different way.
//
// The status is kept in the log line because it is worth reading during an
// incident, not because anything branches on it.
func (s *Service) recordDeliveryRefusedByWriter(item peerSendItem, status netcore.SendStatus) {
	if !item.carriesDelivery() {
		return
	}
	log.Debug().Str("message_id", string(item.delivery.Envelope.ID)).
		Str("status", status.String()).
		Msg("delivery_refused_by_writer")
}

// sendErrorProvesNothingWasWritten is the same question for the inbound
// writer, which reports sentinels instead of statuses (network_consumer.go
// maps one to the other).
//
// ErrUnknownConn / ErrUnregisteredWrite are exact for a stronger reason
// than the others: there was no registered connection to enqueue onto, so
// there was no queue for the frame to sit in.
// A marshal failure is exact for the same reason: the frame never became
// bytes, so there was nothing to enqueue.
func sendErrorProvesNothingWasWritten(err error) bool {
	switch {
	case errors.Is(err, netcore.ErrSendBufferFull),
		errors.Is(err, netcore.ErrUnknownConn),
		errors.Is(err, ErrUnregisteredWrite),
		errors.Is(err, protocol.ErrFrameTooLarge),
		errors.Is(err, errDeliveryWithheld):
		return true
	default:
		return false
	}
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
// It RETURNS the deliveries the residue was carrying, because a frame
// discarded here provably never reached NetCore while the durable
// never-emitted claim for it was already withdrawn — the retry tick
// withdraws before it hands the envelope to the sinks (emitDueDelivery).
// Left unaccounted, the row says the message left this machine and a
// restart before the next retry reads it as sent. Sessions are torn down
// by a Service, which is what turns this list back into a claim; see
// discardSendQueue and recordStrandedDeliveries.
func (ps *peerSession) closeSendQueue() []deliveryDispatchRef {
	if ps == nil {
		return nil
	}
	ps.sendMu.Lock()
	ps.sendClosed = true
	ps.sendMu.Unlock()

	// Producers are already refused by the flag above, so nothing can be added
	// behind the sweep.
	var stranded []deliveryDispatchRef
	for {
		select {
		case item := <-ps.sendCh:
			if item.carriesDelivery() {
				stranded = append(stranded, item.delivery)
			}
		default:
			return stranded
		}
	}
}

// discardSendQueue fences the queue and hands whatever it was holding to
// the session's stranded sink. Every teardown path goes through here so
// the accounting cannot be forgotten at one of them.
func (ps *peerSession) discardSendQueue() {
	stranded := ps.closeSendQueue()
	if ps == nil || ps.onStranded == nil || len(stranded) == 0 {
		return
	}
	ps.onStranded(stranded)
}

// recordStrandedDeliveries answers for frames that died in a session queue.
//
// This is the third place a delivery frame can fail to reach the wire,
// alongside a writer refusal and a refused inbound write, and it is the
// one with no error to classify: a discarded queue element provably never
// reached NetCore, so it is exact by construction.
func (s *Service) recordStrandedDeliveries(stranded []deliveryDispatchRef) {
	for _, ref := range stranded {
		log.Debug().Str("message_id", string(ref.Envelope.ID)).
			Msg("delivery_stranded_in_discarded_session_queue")
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
