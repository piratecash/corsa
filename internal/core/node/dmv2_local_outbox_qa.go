//go:build s6_direct_dmv2_qa

package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"time"
)

func (s *Service) EnableDirectDMV2OutboxForTest() {
	s.directDMV2ListenerTest = true
	s.directDMV2WriteSlots = make(chan struct{}, 2)
}

// DirectDMV2OutboxForTest snapshots an exact retry after normal registration.
func (s *Service) DirectDMV2OutboxForTest(ref domain.MessageRef) (protocol.Envelope, bool) {
	key := messageIdentity{
		MessageID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient,
	}
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	if entry := s.awaitingDelivered[key]; entry != nil {
		envelope := entry.Envelope
		envelope.Payload = append([]byte(nil), envelope.Payload...)
		return envelope, true
	}
	return protocol.Envelope{}, false
}

func (s *Service) TryDirectDMV2RetryForTest(ref domain.MessageRef) bool {
	envelope, ok := s.DirectDMV2OutboxForTest(ref)
	return ok && s.dispatchEnvelopeRetry(envelope, time.Now().UTC())
}
