package node

import (
	"context"
	"errors"

	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// This prototype callback returns positive evidence only for a confirmed
// insert or the exact already stored envelope. It is not MessageStore's
// generic, memory-capable result.
type DirectDMV2ReceiptOutcome uint8

const (
	DirectDMV2ReceiptPending DirectDMV2ReceiptOutcome = iota
	DirectDMV2ReceiptInserted
	DirectDMV2ReceiptDuplicate
)

type directDMV2ReceiptStore func(protocol.Envelope, string, string) (DirectDMV2ReceiptOutcome, error)

type directDMV2ListenerStore interface {
	StoreDirectDMV2ForReceipt(protocol.Envelope, string, string) (DirectDMV2ReceiptOutcome, error)
}

func (s *Service) directDMV2ListenerEnabled() bool {
	_, hasStore := s.messageStore.(directDMV2ListenerStore)
	return s.directDMV2ListenerTest && hasStore && !s.cfg.DisableDirectMessages
}

var (
	errDirectDMV2Listener = errors.New("direct DM v2 listener is not eligible")
	errDirectDMV2Message  = errors.New("direct DM v2 envelope did not pass admission")
	errDirectDMV2Pending  = errors.New("direct DM v2 store did not confirm the envelope")
)

// directDMV2ListenerLocked selects the listener-side authority. The caller
// holds peerMu.
func (s *Service) directDMV2ListenerLocked(id domain.ConnID, sender domain.PeerIdentity, capability domain.Capability) (*netcore.NetCore, *connauth.State) {
	entry := s.connEntryByIDLocked(id)
	if entry == nil || entry.core == nil || entry.core.Dir() != netcore.Inbound ||
		!entry.receive.ReceiveReady() || !capsContain(entry.core.CapabilitiesRef(), capability) {
		return nil, nil
	}
	auth := entry.core.Auth()
	proof, proven := auth.ProvenIdentity()
	peer, ok := proof.Identity()
	if !proven || !ok || peer != sender || auth.Hello.PubKey == "" {
		return nil, nil
	}
	return entry.core, auth
}

// receiveDirectDMV2OnListener is wired by the overlay's opt-in dispatcher.
// Only the original protected writer may send
// this receipt, with no fallback to subscribers, gossip or address lookup.
func (s *Service) receiveDirectDMV2OnListener(ctx context.Context, id domain.ConnID, capability domain.Capability, msg incomingMessage, store directDMV2ReceiptStore) error {
	if s.identity == nil || s.dropsInboundDM(msg) || msg.Topic != "dm" ||
		msg.Recipient != s.identity.Address || msg.Sender == s.identity.Address ||
		len(msg.Body) > maxPeerCommandBodyBytes || capability == "" || store == nil {
		return errDirectDMV2Message
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sender := domain.PeerIdentityFromWire(msg.Sender)
	s.peerMu.RLock()
	core, auth := s.directDMV2ListenerLocked(id, sender, capability)
	s.peerMu.RUnlock()
	if core == nil {
		return errDirectDMV2Listener
	}
	binding := directmsg.V2Binding{
		Network: networkName, ID: string(msg.ID), Topic: msg.Topic,
		Flag: string(msg.Flag), Sender: msg.Sender, Recipient: msg.Recipient,
	}
	meta, err := directmsg.VerifyV2(binding, auth.Hello.PubKey, msg.Body)
	now := time.Now().UTC()
	if err != nil || !msg.CreatedAt.Equal(time.Unix(meta.CreatedAt, 0)) ||
		msg.CreatedAt.After(now.Add(s.effectiveClockDrift())) ||
		meta.ExpiresAt <= now.Unix() || meta.ExpiresAt-meta.CreatedAt != int64(msg.TTLSeconds) {
		return errDirectDMV2Message
	}
	envelope := protocol.Envelope{
		ID: msg.ID, Topic: msg.Topic, Sender: msg.Sender, Recipient: msg.Recipient,
		Flag: msg.Flag, CreatedAt: msg.CreatedAt, TTLSeconds: msg.TTLSeconds, Payload: []byte(msg.Body),
	}
	stored, err := store(envelope, networkName, auth.Hello.PubKey)
	if err != nil {
		return err
	}
	if stored != DirectDMV2ReceiptInserted && stored != DirectDMV2ReceiptDuplicate {
		return errDirectDMV2Pending
	}
	if stored == DirectDMV2ReceiptInserted {
		event := protocol.LocalChangeEvent{
			Type: protocol.LocalChangeNewMessage, Topic: msg.Topic, MessageID: string(msg.ID),
			Sender: msg.Sender, Recipient: msg.Recipient, Body: msg.Body, Flag: string(msg.Flag),
			CreatedAt: msg.CreatedAt.Format(time.RFC3339Nano), TTLSeconds: msg.TTLSeconds,
		}
		s.emitLocalChange(event)
	}
	receipt, ok := s.prepareOwnDeliveryReceipt(deliveredReceiptFor(s.identity.Address, msg), true)
	if !ok {
		return errDirectDMV2Message
	}
	item := receiptFrame(receipt)
	data, err := protocol.MarshalFrameLineBytes(protocol.Frame{
		Type: "push_delivery_receipt", Recipient: receipt.Recipient, Receipt: &item,
	})
	if err != nil {
		return err
	}

	s.peerMu.RLock()
	current, currentAuth := s.directDMV2ListenerLocked(id, sender, capability)
	s.peerMu.RUnlock()
	if current != core || currentAuth != auth {
		return errDirectDMV2Listener
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = (inboundSendSelection{core: core}).sendCapturedInbound(ctx, data, true)
	return err
}
