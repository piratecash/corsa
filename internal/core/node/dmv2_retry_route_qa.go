//go:build s6_direct_dmv2_qa

package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"time"
)

// RunDirectDMV2RetryTickForTest calls the normal planner and dispatch.
func (s *Service) RunDirectDMV2RetryTickForTest(now time.Time) {
	s.retryDueDeliveries(now)
}

// SetDirectDMV2RetryClockForTest keeps the claim clock on the same timeline
// as the explicit test ticks. Install it before any outbox activity.
func (s *Service) SetDirectDMV2RetryClockForTest(now func() time.Time) {
	s.deliveryClock = now
}

func (s *Service) DirectDMV2NextAttemptForTest(ref domain.MessageRef) (time.Time, bool) {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	key := messageIdentity{MessageID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient}
	if entry := s.awaitingDelivered[key]; entry != nil {
		return entry.NextAttemptAt, true
	}
	return time.Time{}, false
}

func (s *Service) SetDirectDMV2BeforeArmForTest(f func()) {
	s.directDMV2BeforeArmTest = f
}

// DirectDMV2TCPWriterTest reports the direction of the unique current writer.
// The lookup requires actual session proof, capability and readiness.
func (s *Service) DirectDMV2TCPWriterTest(recipient string, dialled bool) bool {
	writer := s.directDMV2RetryWriter(recipient)
	if writer == nil {
		return false
	}
	if dialled {
		return writer.Dir() == netcore.Outbound
	}
	return writer.Dir() == netcore.Inbound
}
