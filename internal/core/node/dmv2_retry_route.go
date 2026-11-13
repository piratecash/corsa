package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// directDMV2RetryWriter chooses exactly one current writer for the final
// recipient. An absent or ambiguous match holds the message in the outbox.
func (s *Service) directDMV2RetryWriter(recipient string) *netcore.NetCore {
	if s.directDMV2WriteSlots == nil || s.runCtx.Err() != nil {
		return nil
	}
	identity, err := domain.ParsePeerIdentity(recipient)
	if err != nil || identity.IsZero() {
		return nil
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.directDMV2RetryWriterLocked(identity)
}

func (s *Service) directDMV2RetryWriterLocked(identity domain.PeerIdentity) *netcore.NetCore {
	var selected *netcore.NetCore
	for _, session := range s.sessions {
		if session.peerIdentity != identity || !session.authOK || session.eligibilityEnded.Load() ||
			!session.receive.ReceiveReady() || !capsContain(session.capabilities, domain.CapDirectDMV2) {
			continue
		}
		proof, proven := session.provenIdentity()
		peer, ok := proof.Identity()
		if !proven || !ok || peer != identity {
			continue
		}
		core := s.coreForIDLocked(session.connID)
		if core == nil || core != session.netCore || !core.AcceptingFrames() {
			continue
		}
		if selected != nil && selected != core {
			return nil
		}
		selected = core
	}
	for id := range s.conns {
		core, _ := s.directDMV2ListenerLocked(id, identity, domain.CapDirectDMV2)
		if core == nil || !core.AcceptingFrames() {
			continue
		}
		if selected != nil && selected != core {
			return nil
		}
		selected = core
	}
	return selected
}

func (s *Service) dispatchDirectDMV2Retry(envelope protocol.Envelope, dispatchedAt time.Time) bool {
	if s.identity == nil || envelope.Topic != "dm" || envelope.Sender != s.identity.Address {
		return false
	}
	core := s.directDMV2RetryWriter(envelope.Recipient)
	if core == nil {
		return false
	}
	item := messageFrame(envelope)
	data, err := protocol.MarshalFrameLineBytes(protocol.Frame{
		Type: "push_message", Topic: envelope.Topic, Item: &item,
	})
	if err != nil {
		return false
	}
	select {
	case s.directDMV2WriteSlots <- struct{}{}:
	default:
		return false
	}
	if !s.goBackground(func() {
		defer func() { <-s.directDMV2WriteSlots }()
		if s.directDMV2RetryWriter(envelope.Recipient) != core {
			return
		}
		if core.SendRawSyncCtx(s.runCtx, data) == netcore.SendOK {
			s.confirmEnvelopeOnWire(envelope, dispatchedAt)
		}
	}) {
		<-s.directDMV2WriteSlots
		return false
	}
	// Admission to the bounded job is only an attempt. Do not roll back
	// the claim on an ambiguous write result or mark it as on the wire.
	return true
}

func (s *Service) measureMissingDirectDMV2(reading recipientReachability, candidates []dueCandidate) {
	if reading.directDMV2 == nil {
		return
	}
	for _, candidate := range candidates {
		if directmsg.HasV2EnvelopePrefix(candidate.env.Payload) {
			recipient := candidate.env.Recipient
			if _, measured := reading.directDMV2[recipient]; !measured {
				reading.directDMV2[recipient] = s.directDMV2RetryWriter(recipient) != nil
			}
		}
	}
}

func (s *Service) wakeDirectDMV2Inbound(id domain.ConnID, peer domain.PeerIdentity) {
	if s.directDMV2WriteSlots == nil {
		return
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	core, _ := s.directDMV2ListenerLocked(id, peer, domain.CapDirectDMV2)
	if core == nil {
		return
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	s.wakeDirectDMV2Locked(peer, core)
}

// wakeDirectDMV2Locked requires peerMu followed by deliveryMu. Only the
// selected original writer can make this peer's held outbox eligible.
func (s *Service) wakeDirectDMV2Locked(peer domain.PeerIdentity, core *netcore.NetCore) {
	if peer.IsZero() || s.directDMV2RetryWriterLocked(peer) != core {
		return
	}
	s.directDMV2WakeSeq++
	now := s.deliveryNow()
	for id, entry := range s.awaitingDelivered {
		if entry.Envelope.Recipient != peer.String() ||
			!directmsg.HasV2EnvelopePrefix(entry.Envelope.Payload) || s.deliveryFrozenLocked(id) {
			continue
		}
		entry.directWakeSeq = s.directDMV2WakeSeq
		if entry.Hold == holdUnreachable && entry.NextAttemptAt.After(now) {
			entry.NextAttemptAt = now
		}
	}
}
