package node

import (
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

func newTrackedInboundHealthConn(t *testing.T, svc *Service, address domain.PeerAddress) (domain.ConnID, *netcore.NetCore) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	if err := svc.registerInboundConn(local, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.unregisterInboundConn(local) })
	id, ok := svc.connIDFor(local)
	if !ok {
		t.Fatal("missing inbound registration")
	}
	core := svc.netCoreForID(id)
	if core == nil {
		t.Fatal("missing inbound core")
	}
	core.ApplyOpts(netcore.Options{Address: address, Identity: idPeerB})
	svc.trackInboundConnect(id, address, idPeerB)
	return id, core
}

func TestInboundHeartbeatUsesPongFromItsOwnConnection(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("192.0.2.27:64646")
	firstID, first := newTrackedInboundHealthConn(t, svc, address)
	otherID, other := newTrackedInboundHealthConn(t, svc, address)
	sent := time.Now()
	svc.markInboundPeerWrite(firstID, first, "ping")
	svc.markInboundPeerRead(otherID, other, "pong")
	if live, replied := svc.inboundHeartbeatStatus(firstID, first, sent); !live || replied {
		t.Fatalf("another connection satisfied heartbeat: live=%v replied=%v", live, replied)
	}
	if live, replied := svc.inboundHeartbeatStatus(otherID, other, sent); !live || !replied {
		t.Fatalf("original connection lost its pong: live=%v replied=%v", live, replied)
	}
	svc.markInboundPeerRead(firstID, first, "pong")
	if live, replied := svc.inboundHeartbeatStatus(firstID, first, sent); !live || !replied {
		t.Fatalf("heartbeat ignored the original pong: live=%v replied=%v", live, replied)
	}

	svc.peerMu.RLock()
	health := *svc.health[address]
	svc.peerMu.RUnlock()
	svc.peerActivityNanos.Delete(address)
	first.ShutSendQueue()
	svc.markInboundPeerRead(firstID, first, "pong")
	svc.markInboundPeerRead(firstID, first, "ping")
	svc.markInboundPeerWrite(firstID, first, "ping")
	svc.markInboundPeerWrite(firstID, first, "pong")
	svc.markInboundPeerRead(otherID, first, "pong")
	svc.peerMu.RLock()
	after := *svc.health[address]
	svc.peerMu.RUnlock()
	if health.LastPongAt != after.LastPongAt || health.LastUsefulReceiveAt != after.LastUsefulReceiveAt ||
		health.LastPingAt != after.LastPingAt || health.LastUsefulSendAt != after.LastUsefulSendAt {
		t.Fatal("closed or mismatched core changed the shared peer health")
	}
	if _, claimed := svc.peerActivityNanos.Load(address); claimed {
		t.Fatal("refused activity claimed the throttle interval")
	}
	if live, _ := svc.inboundHeartbeatStatus(firstID, first, sent); live {
		t.Fatal("closed gate still participated in heartbeat")
	}
}
