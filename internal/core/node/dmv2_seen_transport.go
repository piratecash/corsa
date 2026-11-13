package node

import (
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func (s *Service) handleDirectDMV2ListenerSeen(id domain.ConnID, receipt protocol.DeliveryReceipt) bool {
	return s.handleDirectDMV2Seen(receipt, func(peer domain.PeerIdentity) (*netcore.NetCore, string) {
		core, auth := s.directDMV2ListenerLocked(id, peer, domain.CapDirectDMV2)
		if core == nil || !core.AcceptingFrames() {
			return nil, ""
		}
		return core, auth.Hello.PubKey
	})
}

func (s *Service) handleDirectDMV2DialerSeen(session *peerSession, receipt protocol.DeliveryReceipt) bool {
	return s.handleDirectDMV2Seen(receipt, func(peer domain.PeerIdentity) (*netcore.NetCore, string) {
		proof, proven := session.provenIdentity()
		identity, ok := proof.Identity()
		if !proven || !ok || identity != peer || !session.authOK ||
			session.eligibilityEnded.Load() || !session.receive.ReceiveReady() ||
			!capsContain(session.capabilities, domain.CapDirectDMV2) {
			return nil, ""
		}
		core := s.coreForIDLocked(session.connID)
		if core == nil || core != session.netCore || !core.AcceptingFrames() {
			return nil, ""
		}
		return core, session.proven.Intro.PubKey
	})
}

// current reads the authority of this exact connection with peerMu held.
func (s *Service) handleDirectDMV2Seen(receipt protocol.DeliveryReceipt, current func(domain.PeerIdentity) (*netcore.NetCore, string)) bool {
	if !s.directDMV2ListenerEnabled() || receipt.Recipient != s.identity.Address ||
		(receipt.Status != protocol.ReceiptStatusSeen && receipt.Status != protocol.ReceiptStatusSeenAck) {
		return false
	}
	required, err := s.directDMV2ReceiptRequired(receipt)
	if err != nil {
		return true
	}
	if !required {
		return false
	}
	peer, err := domain.ParsePeerIdentity(receipt.Sender)
	if err != nil || peer.IsZero() || receipt.Signature == "" {
		return true
	}
	s.peerMu.RLock()
	core, key := current(peer)
	s.peerMu.RUnlock()
	if core == nil || receipt.SignerPublicKey != key {
		return true
	}
	if receipt.Status == protocol.ReceiptStatusSeenAck {
		s.storeDeliveryReceipt(receipt)
		return true
	}
	s.storeDeliveryReceiptWithSeenReply(receipt, func(ack protocol.DeliveryReceipt) {
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		writer, currentKey := current(peer)
		if writer == core && currentKey == key {
			_ = sendDirectDMV2Receipt(core, ack)
		}
	})
	return true
}

func (s *Service) directDMV2ReceiptRequired(receipt protocol.DeliveryReceipt) (bool, error) {
	policy, ok := s.messageStore.(receiptSignaturePolicy)
	if !ok {
		return false, errors.New("direct dm-v2 receipt policy unavailable")
	}
	id, _ := confirmedMessageOf(receipt)
	return policy.RequiresSignedReceipt(deliveryMessageRef(id))
}

func sendDirectDMV2Receipt(core *netcore.NetCore, receipt protocol.DeliveryReceipt) error {
	item := receiptFrame(receipt)
	data, err := protocol.MarshalFrameLineBytes(protocol.Frame{
		Type: "push_delivery_receipt", Recipient: receipt.Recipient, Receipt: &item,
	})
	if err != nil {
		return err
	}
	return netcore.SendStatusToError(core.SendRaw(data))
}

// isDirectDMV2OwnSeen identifies the signed local receipt class emitted by
// the opt-in dm-v2 store. This class uses the durable seen journal and
// direct writer, so it must never enter the relay/subscriber backlog.
func (s *Service) isDirectDMV2OwnSeen(receipt protocol.DeliveryReceipt) bool {
	return s.directDMV2ListenerEnabled() && receipt.Sender == s.identity.Address &&
		receipt.Status == protocol.ReceiptStatusSeen && receipt.Signature != ""
}

// tryDirectDMV2OwnSeen selects the unique proved writer in either direction.
// An unavailable writer leaves the durable retry in charge.
func (s *Service) tryDirectDMV2OwnSeen(receipt protocol.DeliveryReceipt) {
	peer, err := domain.ParsePeerIdentity(receipt.Recipient)
	if err != nil || peer.IsZero() {
		return
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if writer := s.directDMV2RetryWriterLocked(peer); writer != nil {
		_ = sendDirectDMV2Receipt(writer, receipt)
	}
}

// retryDueDirectDMV2Seen snapshots due rows without routing under deliveryMu.
// The commit rechecks the original entry while holding peerMu then deliveryMu.
func (s *Service) retryDueDirectDMV2Seen(now time.Time) {
	if !s.directDMV2ListenerEnabled() {
		return
	}
	type candidate struct {
		id    messageIdentity
		entry *seenAckRetryEntry
	}
	var due []candidate
	s.deliveryMu.RLock()
	for id, entry := range s.awaitingSeenAck {
		if !entry.NextAttemptAt.After(now) && s.isDirectDMV2OwnSeen(entry.Receipt) {
			due = append(due, candidate{id, entry})
		}
	}
	s.deliveryMu.RUnlock()
	for _, item := range due {
		s.retryDirectDMV2Seen(item.id, item.entry, now)
	}
}

func (s *Service) retryDirectDMV2Seen(id messageIdentity, entry *seenAckRetryEntry, now time.Time) {
	peer, err := domain.ParsePeerIdentity(entry.Receipt.Recipient)
	if err != nil || peer.IsZero() {
		return
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	writer := s.directDMV2RetryWriterLocked(peer)
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if s.awaitingSeenAck[id] != entry || entry.NextAttemptAt.After(now) {
		return
	}
	if entry.Attempts >= s.deliveryRetryMaxAttempts() {
		delete(s.awaitingSeenAck, id)
		log.Warn().Str("message_id", string(id.MessageID)).Str("recipient", entry.Receipt.Recipient).
			Int("attempts", entry.Attempts).Msg("seen_ack_retry_exhausted")
		return
	}
	if writer == nil || sendDirectDMV2Receipt(writer, entry.Receipt) != nil {
		entry.NextAttemptAt = now.Add(deliveryHoldPollInterval)
		return
	}
	entry.Attempts++
	entry.NextAttemptAt = now.Add(deliveryRetryBackoff(entry.Attempts))
	log.Info().Str("message_id", string(id.MessageID)).Str("recipient", entry.Receipt.Recipient).
		Str("status", entry.Receipt.Status).Msg("seen_receipt_retry_resend")
}
