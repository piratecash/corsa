package node

// outbound_delivery_gate.go answers ONE question for every frame this node
// puts on the wire: is this frame carrying a message we own, and if so,
// what does the delivery subsystem owe it?
//
// It exists because that question used to be answered at each send site
// separately, and each site is a place the answer could be forgotten. Five
// review rounds found the same defect five times, in the session queue, in
// the inbound-direct write, in the pending ring, in the gossip fan-out and
// in the online-trigger drain — never the same code twice, always the same
// bug: a frame carrying one of our messages reached a writer without
// passing clearedToWrite, so it could be handed to the recipient AFTER its
// author recalled it, with the deletion recording it as never emitted and
// scheduling no peer-side delete. The copy stayed with them for good.
//
// The cure is not another patched call site. It is that the delivery
// reference travels WITH the frame and is filled in at the admission
// points, so a send path cannot bypass the gate by forgetting to pass
// something — there is nothing left for it to pass.
//
// Two functions carry the whole contract:
//
//   - frameEnvelope reads a message's identity out of a frame. ONE reader,
//     because the identity lives in two shapes (flat fields on the relay
//     frames, a nested Item on push_message) and every place that open-coded
//     the choice got a different subset right.
//   - deliveryRefForFrame turns that identity into a dispatch reference,
//     and returns the zero value for everything that is not ours.

import (
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// messageCarryingFrameTypes are the frame types that carry a user message
// and can therefore belong to a delivery. Everything else — receipts,
// announce-plane traffic, hop acks, notices — has no message identity to
// read and is invisible to this file.
//
// It is a set rather than a chain of ORs because the chain was written out
// three times with three different memberships, and the one that omitted
// push_message is why a cancelled message stayed in the pending ring.
var messageCarryingFrameTypes = map[string]struct{}{
	"send_message":  {},
	"relay_message": {},
	"push_message":  {},
}

// carriesMessage reports whether the frame type can name a message.
func carriesMessage(frame protocol.Frame) bool {
	_, ok := messageCarryingFrameTypes[frame.Type]
	return ok
}

// frameEnvelope reads the message identity out of a frame, whichever shape
// it is in.
//
// send_message and relay_message carry it in the flat fields; push_message
// carries it in Item (gossipPushFrame). The flat field wins when both are
// present, because a relay frame's own fields describe THIS hop while the
// Item, if any, is the payload it forwards.
//
// The returned envelope is not the full message — only what the delivery
// paths read: which message, between whom, on what topic. A frame that
// carries no message at all returns the zero envelope, and every caller
// treats that as "not ours".
func frameEnvelope(frame protocol.Frame) protocol.Envelope {
	if !carriesMessage(frame) {
		return protocol.Envelope{}
	}
	envelope := protocol.Envelope{
		ID:        protocol.MessageID(frame.ID),
		Topic:     frame.Topic,
		Sender:    frame.Address,
		Recipient: frame.Recipient,
	}
	if frame.Item == nil {
		return envelope
	}
	if envelope.ID == "" {
		envelope.ID = protocol.MessageID(frame.Item.ID)
	}
	if envelope.Sender == "" {
		envelope.Sender = frame.Item.Sender
	}
	// The recipient was the field the open-coded readers kept missing, and
	// it is the one the queued → sent event needs: an emission event with
	// an empty recipient does not match any conversation, so the sender's
	// bubble stayed on "queued" until the receipt arrived.
	if envelope.Recipient == "" {
		envelope.Recipient = frame.Item.Recipient
	}
	return envelope
}

// frameMessageID is frameEnvelope for the callers that only need the id —
// the pending-ring bookkeeping, which matches frames against a withdrawn
// message.
func frameMessageID(frame protocol.Frame) protocol.MessageID {
	return frameEnvelope(frame).ID
}

// deliveryRefForFrame returns the dispatch this frame belongs to, or the
// zero ref when the frame is not one of ours to answer for.
//
// "Ours" is ONE condition: the message was AUTHORED here. Transit traffic
// passes through untouched — confirming it would make this node charge and
// announce a delivery it does not own.
//
// It deliberately does NOT require a live retry entry, and that distinction
// is the whole reason this comment is long. The gate's other job is to
// refuse a message its author has RECALLED, and a recall is precisely what
// removes the entry (delivery_cancel.go) while leaving a withdrawal shadow
// behind. Requiring an entry meant a frame extracted from the pending ring
// a moment before the recall got the zero ref, passed the gate untouched,
// and handed the recalled message to the recipient — the same defect the
// gate exists to prevent, reached from the other side.
//
// An entry-less id is handled correctly downstream: claimEmissionLocked
// reads the withdrawal shadow and the freeze BEFORE it looks for an entry,
// and confirmEnvelopeOnWire is a documented no-op without one.
//
// now stamps the attempt. A caller that is dispatching to SEVERAL sinks at
// once passes its own ref instead, so one dispatch is charged once however
// many writers take it; this is the fallback for the paths that carry a
// frame on their own — a pending-ring flush, a drain triggered by the
// recipient coming online — where the frame IS the whole attempt.
func (s *Service) deliveryRefForFrame(frame protocol.Frame, now time.Time) deliveryDispatchRef {
	envelope := frameEnvelope(frame)
	if envelope.ID == "" || s.identity == nil || envelope.Sender != s.identity.Address {
		return deliveryDispatchRef{}
	}
	s.deliveryMu.RLock()
	if entry, awaiting := s.awaitingDelivered[envelope.ID]; awaiting {
		// Prefer the envelope the retry engine is holding: it is the
		// authoritative one, and the frame's copy is a projection that may
		// have lost fields on its way through the wire shapes.
		envelope = entry.Envelope
	}
	s.deliveryMu.RUnlock()
	return deliveryDispatchRef{Envelope: envelope, DispatchedAt: now}
}

// writeDeliveryFrameToInbound is the OTHER choke point: the writes that go
// straight to an authenticated inbound connection, with no session queue
// between them and the socket.
//
// A session-queued frame is gated when the serve loop dequeues it, which is
// the right moment — the frame can sit there long enough for its author to
// recall it. These writes have no such gap, so both halves happen here: the
// pre-wire gate immediately before, the confirmation immediately after.
//
// ref may be the zero value, in which case it is derived from the frame.
// A caller fanning ONE attempt out to several sinks passes its own so the
// attempt is charged once.
//
// Returns the write error unchanged; the delivery accounting is a side
// effect, and a caller that does not care can ignore it exactly as before.
func (s *Service) writeDeliveryFrameToInbound(connID domain.ConnID, frame protocol.Frame, ref deliveryDispatchRef) error {
	now := time.Now().UTC()
	if ref.Envelope.ID == "" {
		ref = s.deliveryRefForFrame(frame, now)
	}
	if !s.clearedToWrite(ref, now) {
		// Frozen by a wipe, withdrawn since the frame was built, or its
		// durable claim could not be withdrawn. Reported as a refusal so
		// the caller does not count it as delivered.
		log.Info().Str("message_id", string(ref.Envelope.ID)).
			Msg("inbound_write_withheld_by_delivery_gate")
		return errDeliveryWithheld
	}
	err := s.sendFrameViaNetwork(s.runCtx, connID, frame)
	if ref.Envelope.ID == "" {
		return err
	}
	if err == nil {
		s.confirmEnvelopeOnWire(ref.Envelope, ref.DispatchedAt)
	} else {
		// Nothing to record either way: the row already says both of the
		// things its two readers need — not confirmed on the wire, and
		// possibly handed to a writer.
		log.Debug().Str("message_id", string(ref.Envelope.ID)).Err(err).
			Msg("delivery_inbound_write_unconfirmed")
	}
	return err
}

// errDeliveryWithheld is what writeDeliveryFrameToInbound returns when the
// gate refused the frame. It is not a transport failure: nothing was
// attempted, and the retry engine still owns the message.
var errDeliveryWithheld = errors.New("delivery withheld by the pre-wire gate")

// withDeliveryRef fills in the dispatch a queued item belongs to when its
// producer did not name one.
//
// This is the admission-point half of the contract. A producer that knows
// the dispatch — the retry tick, which fans one attempt out to several
// sinks — passes it and keeps one attempt looking like one attempt.
// A producer that does not is not required to know: the item is completed
// here, from the frame it is already carrying.
func (s *Service) withDeliveryRef(item peerSendItem, now time.Time) peerSendItem {
	if item.carriesDelivery() {
		return item
	}
	item.delivery = s.deliveryRefForFrame(item.Frame, now)
	return item
}
