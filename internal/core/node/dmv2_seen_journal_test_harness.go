//go:build s6_direct_dmv2_qa

package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// DirectDMV2SeenRetryTest reads the exact retry without running the scheduler.
func (s *Service) DirectDMV2SeenRetryTest(ref domain.MessageRef) (protocol.DeliveryReceipt, bool) {
	key := messageIdentity{
		MessageID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient,
	}
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	if entry := s.awaitingSeenAck[key]; entry != nil {
		return entry.Receipt, true
	}
	return protocol.DeliveryReceipt{}, false
}

// DirectDMV2SeenAckTest calls the same authenticated store as the wire handlers.
func (s *Service) DirectDMV2SeenAckTest(receipt protocol.DeliveryReceipt) (accepted, ackable bool) {
	outcome := s.storeDeliveryReceipt(receipt)
	return outcome.stored, outcome.ackable
}

// CloseDirectDMV2SeenJournalTest drains owned activity before closing SQLite.
func (s *Service) CloseDirectDMV2SeenJournalTest() {
	s.runCancel()
	s.closeBackgroundAdmission()
	s.WaitBackground()
}
