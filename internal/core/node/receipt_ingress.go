package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// directReceiptInboundWriterLocked checks the proof on the connection that
// actually delivered the receipt. The caller holds peerMu.
func (s *Service) directReceiptInboundWriterLocked(connID domain.ConnID, sender string, legacyOnly bool) *netcore.NetCore {
	core := s.coreForIDLocked(connID)
	if core == nil || core.Dir() != netcore.Inbound || !s.receiveForIDLocked(connID).ReceiveReady() ||
		!core.AcceptingFrames() || legacyOnly && core.HasCapability(domain.CapSignedDirectReceiptsV1) {
		return nil
	}
	proof, proven := core.Auth().ProvenIdentity()
	peer, ok := proof.Identity()
	if !proven || !ok || peer != domain.PeerIdentityFromWire(sender) {
		return nil
	}
	return core
}

// directReceiptOutboundWriterLocked checks the proof on the session that
// delivered the receipt, including while it is in authenticated setup.
// The caller holds peerMu.
func (s *Service) directReceiptOutboundWriterLocked(session *peerSession, sender string, legacyOnly bool) *netcore.NetCore {
	if session == nil || !session.authOK || session.eligibilityEnded.Load() ||
		session.closedBy.Load() != nil || !session.receive.ReceiveReady() ||
		legacyOnly && capsContain(session.capabilities, domain.CapSignedDirectReceiptsV1) {
		return nil
	}
	core := s.coreForIDLocked(session.connID)
	if core == nil || core != session.netCore || core.Dir() != netcore.Outbound ||
		s.receiveForIDLocked(session.connID) != session.receive || !core.AcceptingFrames() {
		return nil
	}
	proof, proven := session.provenIdentity()
	peer, ok := proof.Identity()
	if !proven || !ok || peer != domain.PeerIdentityFromWire(sender) {
		return nil
	}
	return core
}
