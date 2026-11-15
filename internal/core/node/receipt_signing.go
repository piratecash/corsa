package node

import (
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type negotiatedReceiptPolicy interface {
	ReceiptSignaturePolicy(domain.MessageRef) (required, negotiated bool, err error)
}

// SupportsSignedReceipts reports the negotiated capability of a proved
// connection to the final recipient, rather than a relay advertisement.
func (s *Service) SupportsSignedReceipts(peer domain.PeerIdentity) bool {
	if peer.IsZero() {
		return false
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.provenPeerWriterLocked(peer, domain.CapSignedDirectReceiptsV1) != nil
}

func (s *Service) localReceiptSignaturePolicy(ref domain.MessageRef) (required, negotiated bool, err error) {
	if policy, ok := s.messageStore.(negotiatedReceiptPolicy); ok {
		return policy.ReceiptSignaturePolicy(ref)
	}
	if policy, ok := s.messageStore.(receiptSignaturePolicy); ok {
		required, err := policy.RequiresSignedReceipt(ref)
		return required, false, err
	}
	negotiated = s.liveNegotiatedReceiptPolicy(ref)
	return negotiated, negotiated, nil
}

func (s *Service) liveNegotiatedReceiptPolicy(ref domain.MessageRef) bool {
	s.deliveryMu.RLock()
	_, sent := s.signedSentDMIDs[ref.Key()]
	seen := s.awaitingSeenAck[messageIdentity{MessageID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient}]
	protectedSeen := seen != nil && seen.Receipt.NegotiatedSignature
	s.deliveryMu.RUnlock()
	if sent || protectedSeen {
		return true
	}
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()
	for _, message := range s.topics[ref.Topic] {
		if message.SignedReceiptRequired && envelopeRef(message) == ref {
			return true
		}
	}
	return false
}

// rememberSentReceiptPolicyLocked keeps the requirement alive as long as the
// sent id may admit a late receipt, even after delivered clears its retry.
// The caller holds deliveryMu, which also protects the sent-id LRU callback.
func (s *Service) rememberSentReceiptPolicyLocked(message protocol.Envelope) {
	key := envelopeRef(message).Key()
	if message.SignedReceiptRequired {
		if s.signedSentDMIDs == nil {
			s.signedSentDMIDs = make(map[string]struct{})
			s.sentDMIDs.onEvict = func(key string) { delete(s.signedSentDMIDs, key) }
		}
		s.signedSentDMIDs[key] = struct{}{}
	}
	s.sentDMIDs.Add(key)
}

// requiresLiveReceiptSignatureLocked covers in-memory delivery queues and
// late receipts for recently sent messages.
// The caller holds peerMu and deliveryMu.
func (s *Service) requiresLiveReceiptSignatureLocked(receipt protocol.DeliveryReceipt) bool {
	id, ok := confirmedMessageOf(receipt)
	if !ok {
		return false
	}
	if receipt.Status == protocol.ReceiptStatusSeenAck {
		entry := s.awaitingSeenAck[id]
		return entry != nil && entry.Receipt.Signature != ""
	}
	if entry := s.awaitingDelivered[id]; entry != nil &&
		(entry.Envelope.SignedReceiptRequired || directmsg.HasV2EnvelopePrefix(entry.Envelope.Payload)) {
		return true
	}
	_, required := s.signedSentDMIDs[deliveryMessageRef(id).Key()]
	return required
}

func (s *Service) sendNegotiatedSignedReceipt(receipt protocol.DeliveryReceipt) {
	if receipt.Signature == "" || receipt.Sender != s.identity.Address {
		return
	}
	peer, err := domain.ParsePeerIdentity(receipt.Recipient)
	if err != nil || peer.IsZero() {
		return
	}
	data, err := protocol.MarshalFrameLineBytes(relayReceiptFrame(receipt))
	if err != nil {
		return
	}
	s.peerMu.RLock()
	writer := s.provenPeerWriterLocked(peer, domain.CapSignedDirectReceiptsV1)
	direct := writer != nil && writer.SendRaw(data) == netcore.SendOK
	s.peerMu.RUnlock()
	if !direct {
		s.sendSignedReceiptControl(peer, receipt)
	}
}

const (
	signedReceiptControlCommand = "signed_delivery_receipt"
	// Covers the canonical receipt (short JSON and one signature) and its
	// two sealed copies without admitting general control DMs at this size.
	maxSignedReceiptControlBody = 4 << 10
)

// Legacy relays preserve the sealed body of a control DM even though they do
// not preserve the extra fields of a receipt. The original retry owner sends
// a fresh carrier on the next attempt if this live route drops the packet.
func (s *Service) sendSignedReceiptControl(peer domain.PeerIdentity, receipt protocol.DeliveryReceipt) {
	s.knowledgeMu.RLock()
	box, pub, binding := s.boxKeys[receipt.Recipient], s.pubKeys[receipt.Recipient], s.boxSigs[receipt.Recipient]
	s.knowledgeMu.RUnlock()
	if identity.VerifyBoxKeyBinding(receipt.Recipient, pub, box, binding) != nil {
		return
	}
	id, err := protocol.NewMessageID()
	if err != nil {
		return
	}
	payload, err := json.Marshal(receiptFrame(receipt))
	if err != nil {
		return
	}
	body, err := directmsg.EncryptForMessage(s.identity, domain.DMRecipient{Address: peer, BoxKeyBase64: box},
		domain.OutgoingDM{Command: signedReceiptControlCommand, CommandData: string(payload)}, protocol.TopicControlDM, string(id))
	if err != nil || len(body) > maxSignedReceiptControlBody {
		return
	}
	s.storeMessageFrame(protocol.Frame{
		Type: "send_control_message", Topic: protocol.TopicControlDM, ID: string(id), Address: receipt.Sender,
		Recipient: receipt.Recipient, Flag: string(protocol.MessageFlagSenderDelete), Body: body, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Service) acceptSignedReceiptControl(message incomingMessage, plain *directmsg.PlainMessage) (bool, int, string) {
	if len(message.Body) > maxSignedReceiptControlBody {
		return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
	}
	var item protocol.ReceiptFrame
	if err := json.Unmarshal([]byte(plain.CommandData), &item); err != nil {
		return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
	}
	receipt, err := receiptFromReceiptFrame(item)
	if err != nil || receipt.Signature == "" || receipt.Sender != message.Sender || receipt.Recipient != message.Recipient {
		return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
	}
	id, ok := confirmedMessageOf(receipt)
	if !ok {
		return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
	}
	_, negotiated, err := s.localReceiptSignaturePolicy(deliveryMessageRef(id))
	if err != nil {
		return false, 0, protocol.ErrCodeStoreDeferred
	}
	if !negotiated {
		return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
	}
	if receipt.Status == protocol.ReceiptStatusSeen {
		if !s.authenticateReceiptSignature(receipt) {
			return false, 0, protocol.ErrCodeInvalidSendDeliveryReceipt
		}
		// The seen acknowledgment may need the same relay path even if
		// this peer's box key was absent or evicted from our cache.
		s.importVerifiedSenderKeys(message)
	}
	outcome := s.storeDeliveryReceipt(receipt)
	if !outcome.ackable {
		return false, outcome.count, protocol.ErrCodeStoreDeferred
	}
	return true, outcome.count, ""
}

// sendSignedDirectDelivery is only called for a locally generated delivered
// receipt. Its signed wire copy goes directly to the original message sender.
// relay_delivery_receipt has no hop ack, so the new copy needs no ack_delete
// capability and is never queued on a path through legacy peers.
func (s *Service) sendSignedDirectDelivery(receipt protocol.DeliveryReceipt) {
	if s.identity == nil || receipt.Sender != s.identity.Address || receipt.Signature != "" ||
		receipt.Status != protocol.ReceiptStatusDelivered {
		return
	}
	peer, err := domain.ParsePeerIdentity(receipt.Recipient)
	if err != nil || peer.IsZero() {
		return
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	writer := s.provenPeerWriterLocked(peer, domain.CapSignedDirectDeliveryV1)
	if writer == nil || !writer.HasCapability(domain.CapMeshRelayV1) {
		return
	}
	signed, ok := s.prepareOwnDeliveryReceipt(receipt, true)
	if !ok {
		return
	}
	data, err := protocol.MarshalFrameLineBytes(relayReceiptFrame(signed))
	if err == nil && writer.SendRaw(data) != netcore.SendOK {
		log.Debug().Str("message_id", string(receipt.MessageID)).Msg("signed_direct_delivery_queue_unavailable")
	}
}

// prepareOwnDeliveryReceipt is called only at local receipt creation sites.
// A frame received from a peer must never acquire our signature here.
func (s *Service) prepareOwnDeliveryReceipt(receipt protocol.DeliveryReceipt, required bool) (protocol.DeliveryReceipt, bool) {
	if receipt.Sender != s.identity.Address {
		return receipt, !required
	}
	id, ok := confirmedMessageOf(receipt)
	if !ok {
		return receipt, false
	}
	ref := deliveryMessageRef(id)
	if !receipt.NegotiatedSignature && (!required || receipt.Status == protocol.ReceiptStatusSeenAck) {
		persisted, negotiated, err := s.localReceiptSignaturePolicy(ref)
		if err != nil {
			log.Warn().Str("message_id", ref.ID).Err(err).Msg("receipt_signing_policy_unavailable")
			return receipt, false
		}
		required = required || persisted
		receipt.NegotiatedSignature = negotiated
	}
	if !required && !receipt.NegotiatedSignature {
		return receipt, true
	}
	receipt.DeliveredAt = receipt.DeliveredAt.UTC().Truncate(time.Second)
	signature, err := protocol.SignDeliveryReceipt(domain.NetworkID(networkName), ref, receipt, s.identity)
	if err != nil {
		log.Warn().Str("message_id", ref.ID).Err(err).Msg("receipt_signing_failed")
		return receipt, false
	}
	receipt.Signature = signature
	receipt.SignerPublicKey = identity.PublicKeyBase64(s.identity.PublicKey)
	return receipt, true
}

// authenticateReceiptSignature verifies a signed copy using the attached
// self-certifying key, or a key already known to this node.
func (s *Service) authenticateReceiptSignature(receipt protocol.DeliveryReceipt) bool {
	if receipt.Signature == "" {
		return receipt.SignerPublicKey == ""
	}
	id, ok := confirmedMessageOf(receipt)
	if !ok {
		return false
	}
	pubKey := receipt.SignerPublicKey
	if pubKey == "" {
		pubKey, _ = s.knownPubKey(receipt.Sender)
		if receipt.Sender == s.identity.Address {
			pubKey = identity.PublicKeyBase64(s.identity.PublicKey)
		}
	}
	return protocol.VerifyDeliveryReceipt(domain.NetworkID(networkName), deliveryMessageRef(id), receipt, pubKey, receipt.Signature) == nil
}
