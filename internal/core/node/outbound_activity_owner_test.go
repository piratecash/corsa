package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOutboundActivityKeepsTheOriginalTransport(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("192.0.2.26:64646")
	first := newReceiveOwnerSession(t, svc, 5711, address)
	timestamps := func() (time.Time, time.Time) {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		health := svc.health[address]
		if health == nil {
			return time.Time{}, time.Time{}
		}
		return health.LastPongAt, health.LastUsefulReceiveAt
	}
	pong := protocol.Frame{Type: "pong"}

	// A transport used for setup already has a ConnID and NetCore.
	svc.peerMu.Lock()
	delete(svc.sessions, address)
	svc.peerMu.Unlock()
	svc.markSessionRead(first, pong)
	firstPong, _ := timestamps()
	if firstPong.IsZero() {
		t.Fatal("registered transport could not record a setup pong")
	}

	replacement := newReceiveOwnerSession(t, svc, 5712, address)
	svc.markSessionRead(first, pong)
	svc.markSessionUsefulReceive(first)
	pongAfterOld, usefulAfterOld := timestamps()
	if pongAfterOld != firstPong || !usefulAfterOld.IsZero() {
		t.Fatal("old transport changed replacement health")
	}
	if _, claimed := svc.peerActivityNanos.Load(address); claimed {
		t.Fatal("old transport consumed the replacement's throttle interval")
	}

	svc.markSessionUsefulReceive(replacement)
	_, replacementUseful := timestamps()
	if replacementUseful.IsZero() {
		t.Fatal("live replacement did not record useful receive activity")
	}
	svc.peerActivityNanos.Delete(address)
	replacement.netCore.ShutSendQueue()
	svc.markSessionRead(replacement, pong)
	svc.markSessionUsefulReceive(replacement)
	pongAfterClose, usefulAfterClose := timestamps()
	if pongAfterClose != firstPong || usefulAfterClose != replacementUseful {
		t.Fatal("transport with a closed gate changed peer health")
	}
	if _, claimed := svc.peerActivityNanos.Load(address); claimed {
		t.Fatal("closed gate consumed a throttle interval")
	}
}

func TestOutboundWriteActivityRequiresAcceptedWriteAndOriginalTransport(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("192.0.2.28:64646")
	old := newReceiveOwnerSession(t, svc, 5721, address)
	current := newReceiveOwnerSession(t, svc, 5722, address)
	timestamps := func() (time.Time, time.Time) {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		health := svc.health[address]
		if health == nil {
			return time.Time{}, time.Time{}
		}
		return health.LastPingAt, health.LastUsefulSendAt
	}
	svc.markSessionWrite(old, "ping")
	svc.markSessionWrite(old, "get_peers")
	if ping, useful := timestamps(); !ping.IsZero() || !useful.IsZero() {
		t.Fatal("completion from replaced transport changed peer health")
	}
	if _, claimed := svc.peerActivityNanos.Load(address); claimed {
		t.Fatal("completion from replaced transport consumed a throttle interval")
	}

	pong := protocol.Frame{Type: "pong"}
	if err := svc.sendSessionFrameViaNetwork(t.Context(), current, pong); err != nil {
		t.Fatal(err)
	}
	_, useful := timestamps()
	if useful.IsZero() {
		t.Fatal("accepted pong did not record send activity")
	}

	svc.peerActivityNanos.Delete(address)
	current.netCore.ShutSendQueue()
	if err := svc.sendSessionFrameViaNetwork(t.Context(), current, pong); err != nil {
		t.Fatalf("best-effort drop returned an error: %v", err)
	}
	if _, err := svc.peerSessionRequest(current, protocol.Frame{Type: "ping"}, "pong", false); err == nil {
		t.Fatal("blocking ping unexpectedly sent on a closed gate")
	}
	if ping, after := timestamps(); !ping.IsZero() || after != useful {
		t.Fatal("refused writes changed peer health")
	}
	if _, claimed := svc.peerActivityNanos.Load(address); claimed {
		t.Fatal("refused writes consumed a throttle interval")
	}
}
