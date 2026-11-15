package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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
	if policy, ok := s.messageStore.(receiptSignaturePolicy); ok && !required {
		var err error
		required, err = policy.RequiresSignedReceipt(ref)
		if err != nil {
			log.Warn().Str("message_id", ref.ID).Err(err).Msg("receipt_signing_policy_unavailable")
			return receipt, false
		}
	}
	if !required {
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
