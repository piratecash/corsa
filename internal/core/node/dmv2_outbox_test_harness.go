//go:build s6_direct_dmv2_qa

package node

import (
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// DirectDMV2OutboxTestSession exposes a proved session and the retry witness
// to a package service integration test. It is compiled only by this overlay.
type DirectDMV2OutboxTestSession struct {
	service *Service
	session *peerSession
}

func (s *Service) AttachDirectDMV2OutboxTestSession(proven sessionv2.ProvenSession) (*DirectDMV2OutboxTestSession, error) {
	role, err := proven.Role()
	if err != nil || role != sessionv2.RoleDialer {
		return nil, errors.New("test needs a proved dialer session")
	}
	peer, err := proven.Peer()
	if err != nil {
		return nil, err
	}
	conn, err := proven.Conn()
	if err != nil {
		return nil, err
	}
	address := domain.PeerAddress(conn.RemoteAddr().String())
	session := &peerSession{
		conn: conn, address: address, connID: s.allocateConnID("direct_dm_v2_outbox_test", address),
		proven: &peer, sendCh: make(chan peerSendItem, 8),
	}
	if err := s.attachOutboundNetCore(session); err != nil {
		return nil, err
	}
	test := &DirectDMV2OutboxTestSession{service: s, session: session}
	s.peerMu.Lock()
	applyWelcomeMetadata(session, peer.Intro, []domain.Capability{domain.CapDirectDMV2})
	s.peerMu.Unlock()
	if err := s.completeOutboundAuth(session); err != nil {
		test.Close()
		return nil, err
	}
	return test, nil
}

func (t *DirectDMV2OutboxTestSession) Close() {
	_ = t.session.Close()
	t.service.runCancel()
	t.service.closeBackgroundAdmission()
	t.service.WaitBackground()
}

func (t *DirectDMV2OutboxTestSession) envelope(ref domain.MessageRef) (protocol.Envelope, bool) {
	key := messageIdentityOf(protocol.Envelope{
		Topic: ref.Topic, ID: protocol.MessageID(ref.ID), Sender: ref.Sender, Recipient: ref.Recipient,
	})
	t.service.deliveryMu.RLock()
	defer t.service.deliveryMu.RUnlock()
	entry := t.service.awaitingDelivered[key]
	if entry == nil {
		return protocol.Envelope{}, false
	}
	envelope := entry.Envelope
	envelope.Payload = append([]byte(nil), envelope.Payload...)
	return envelope, true
}

func (t *DirectDMV2OutboxTestSession) HasRetry(ref domain.MessageRef) bool {
	_, ok := t.envelope(ref)
	return ok
}

func (t *DirectDMV2OutboxTestSession) Dispatch(frame protocol.Frame) {
	t.service.dispatchPeerSessionFrame(t.session.address, t.session, frame)
}

func (t *DirectDMV2OutboxTestSession) SendBarrier() error {
	return t.service.sendSessionFrameViaNetwork(t.service.runCtx, t.session, protocol.Frame{Type: "ping"})
}

func (t *DirectDMV2OutboxTestSession) PublishForRetryTest() bool {
	return t.service.registerCMSession(t.session, t.session.address)
}

func (t *DirectDMV2OutboxTestSession) CloseTransportForRetryTest() {
	_ = t.session.Close()
}
