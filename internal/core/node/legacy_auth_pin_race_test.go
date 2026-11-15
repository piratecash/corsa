package node

import (
	"bufio"
	"context"
	"io"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// Stop auth after its last pin check and verified-state write, before its
// hello-route write. A concurrent v2 proof must close the real v1 transport;
// resuming the handler must not deliver auth_ok or leave registration residue.
func TestAV2ProofClosesLegacyAuthAlreadyPastThePinCheck(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	svc.disableRateLimiting = true
	svc.routeWithdrawalGracePeriodTest = -1
	peer, owner := sessionv2test.NewProvenPeer(t)
	proof := sessionv2test.Proof(t, peer)
	server, client := tcpPair(t)
	done := make(chan struct{})
	locked := false
	t.Cleanup(func() {
		if locked {
			svc.gossipMu.Unlock()
		}
		_ = client.Close()
		_ = server.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("legacy handler did not finish cleanup")
		}
	})
	go func() {
		defer close(done)
		svc.handleConn(server, nil)
	}()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	writeJSONFrame(t, client, protocol.Frame{
		Type: "hello", Client: "node", Version: 30, MinimumProtocolVersion: 28,
		Address: owner.Address, PubKey: identity.PublicKeyBase64(owner.PublicKey),
		BoxKey: identity.BoxPublicKeyBase64(owner.BoxPublicKey), BoxSig: identity.SignBoxKeyBinding(owner),
	})
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("missing legacy challenge: %#v", welcome)
	}
	var connID domain.ConnID
	svc.peerMu.RLock()
	svc.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity == peer.Identity {
			connID = info.id
		}
		return true
	})
	svc.peerMu.RUnlock()
	core := svc.netCoreForID(connID)
	if core == nil {
		t.Fatal("hello identity is not visible to the v2 close sweep")
	}

	svc.gossipMu.Lock()
	locked = true
	writeJSONFrame(t, client, protocol.Frame{
		Type: "auth_session", Address: owner.Address,
		Signature: identity.SignPayload(owner, protocol.SessionAuthPayload(welcome.Challenge, owner.Address)),
	})
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for core.Auth() == nil || !core.Auth().Verified {
		select {
		case <-deadline.C:
			t.Fatal("auth never passed its final pin check")
		case <-done:
			t.Fatal("handler ended before the registration barrier")
		case <-tick.C:
		}
	}
	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.onIdentityProvenV2(ctx, proof); err != nil {
		t.Fatal(err)
	}
	select {
	case <-core.WriterExited():
	default:
		t.Fatal("proof returned before the legacy transport writer exited")
	}
	svc.gossipMu.Unlock()
	locked = false
	if body, err := io.ReadAll(reader); err != nil || len(body) != 0 {
		t.Fatalf("closed legacy transport received bytes or failed to end: bytes=%q err=%v", body, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("closed legacy handler did not drain")
	}
	if svc.netCoreForID(connID) != nil || svc.identityRelaySessionCount(peer.Identity) != 0 {
		t.Fatal("closed legacy authentication retained registry or relay membership")
	}
	svc.gossipMu.RLock()
	for _, sub := range svc.subs[owner.Address] {
		if sub.connID == connID {
			t.Error("closed legacy authentication retained its hello route")
		}
	}
	svc.gossipMu.RUnlock()
	svc.peerMu.RLock()
	for address, refs := range svc.inboundHealthRefs {
		if refs != 0 {
			t.Errorf("closed legacy authentication retained health references: %s=%d", address, refs)
		}
	}
	svc.peerMu.RUnlock()
	if !svc.identityPinnedV2(peer.Identity) {
		t.Fatal("legacy cleanup removed the v2 pin")
	}
}
