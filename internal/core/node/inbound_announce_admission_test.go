package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

func TestInboundPeerPromotionRequiresOriginalOpenCore(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	register := func() domain.ConnID {
		t.Helper()
		server, client := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		if err := svc.registerInboundConn(server, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { svc.unregisterInboundConn(server) })
		id, ok := svc.connIDFor(server)
		if !ok {
			t.Fatal("missing registered connection")
		}
		return id
	}
	id := register()
	otherID := register()
	core := svc.netCoreForID(id)
	if core == nil {
		t.Fatal("missing registered core")
	}
	const accepted = domain.PeerAddress("203.0.113.101:64646")
	const refused = domain.PeerAddress("203.0.113.102:64646")
	if !svc.promotePeerAddressFromInbound(accepted, id, core) {
		t.Fatal("live original core was refused")
	}
	if svc.promotePeerAddressFromInbound(refused, otherID, core) {
		t.Fatal("another core accepted work from the original core")
	}

	core.ShutSendQueue()
	select {
	case <-core.WriterExited():
		t.Fatal("closing the admission gate unexpectedly joined the writer")
	default:
	}
	if svc.promotePeerAddressFromInbound(refused, id, core) {
		t.Fatal("draining core accepted more peer-directory work")
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if _, ok := svc.findKnownPeerByIPLocked(accepted); !ok {
		t.Error("work accepted before closing the gate did not survive")
	}
	if _, ok := svc.findKnownPeerByIPLocked(refused); ok {
		t.Error("refused work changed the peer directory")
	}
}
