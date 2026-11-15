package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOutboundPeerPromotionUsesAuthenticatedOriginalTransportDuringSetup(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	attach := func(id domain.ConnID) *peerSession {
		t.Helper()
		local, remote := net.Pipe()
		t.Cleanup(func() { _ = remote.Close() })
		session := &peerSession{address: "192.0.2.22:64646", connID: id, conn: local}
		if err := svc.attachOutboundNetCore(session); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	session := attach(5701)
	other := attach(5702)
	announce := func(session *peerSession, address domain.PeerAddress) {
		svc.dispatchPeerSessionFrame(session.address, session, protocol.Frame{
			Type: "announce_peer", NodeType: string(config.NodeTypeFull), Peers: []string{string(address)},
		})
	}
	learned := func(address domain.PeerAddress) bool {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		_, ok := svc.findKnownPeerByIPLocked(address)
		return ok
	}
	const refused = domain.PeerAddress("203.0.113.121:64646")
	const accepted = domain.PeerAddress("203.0.113.122:64646")
	announce(session, refused)
	if learned(refused) {
		t.Fatal("outbound setup learned an address before authentication")
	}
	if err := svc.markSessionHandshakeComplete(session); err != nil {
		t.Fatal(err)
	}
	announce(session, accepted)
	if !learned(accepted) {
		t.Fatal("authenticated setup must accept announcements before active session publication")
	}
	impostor := &peerSession{address: session.address, connID: session.connID, authOK: true, netCore: other.netCore}
	announce(impostor, refused)
	session.eligibilityEnded.Store(true)
	announce(session, refused)
	if err := svc.markSessionHandshakeComplete(other); err != nil {
		t.Fatal(err)
	}
	other.netCore.ShutSendQueue()
	announce(other, refused)
	if learned(refused) {
		t.Fatal("refused outbound announcement changed the peer directory")
	}
	if !learned(accepted) {
		t.Fatal("closing later removed a previously accepted address")
	}
}
