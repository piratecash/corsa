//go:build s6_direct_dmv2_qa

package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"time"
)

// DirectDMV2SeenScheduleTest reads the exact pending retry and its budget.
func (s *Service) DirectDMV2SeenScheduleTest(ref domain.MessageRef) (int, time.Time, bool) {
	id := messageIdentity{MessageID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient}
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	entry := s.awaitingSeenAck[id]
	if entry == nil {
		return 0, time.Time{}, false
	}
	return entry.Attempts, entry.NextAttemptAt, true
}

// DirectDMV2SeenGenericCopiesTest checks that a direct receipt did not
// also enter the generic recipient backlog or relay retry map.
func (s *Service) DirectDMV2SeenGenericCopiesTest(receipt protocol.DeliveryReceipt) (bool, bool) {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	key := receiptKeyOf(receipt)
	_, relaying := s.relayRetry[relayReceiptKey(receipt)]
	for _, item := range s.receipts[receipt.Recipient] {
		if receiptKeyOf(item) == key {
			return true, relaying
		}
	}
	return false, relaying
}
