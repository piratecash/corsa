package node

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func newReceiveOwnerSession(t *testing.T, svc *Service, id domain.ConnID, address domain.PeerAddress) *peerSession {
	t.Helper()
	local, remote := net.Pipe()
	session := &peerSession{address: address, peerIdentity: idPeerB, connID: id, conn: local, authOK: true, capabilities: legacyRoutingCaps, sendCh: make(chan peerSendItem, 8), inboxCh: make(chan protocol.Frame, 8), errCh: make(chan error, 1)}
	if err := svc.attachOutboundNetCore(session); err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() { defer close(drained); _, _ = io.Copy(io.Discard, remote) }()
	t.Cleanup(func() { _ = session.Close(); _ = remote.Close(); <-drained })
	svc.peerMu.Lock()
	svc.sessions[address] = session
	entry := svc.conns[id]
	shared := entry != nil && entry.receive == session.receive
	svc.peerMu.Unlock()
	if !shared {
		t.Fatal("outbound session and actual ConnID entry have divergent receive owners")
	}
	return session
}
func receiveOwnerDelta(target domain.PeerIdentity) protocol.Frame {
	return protocol.Frame{Type: "routes_update", AnnounceRoutes: []protocol.AnnounceRouteFrame{{Identity: target.String(), Origin: idPeerB.String(), Hops: 1, SeqNo: 1}}}
}
func TestReceiveBaselineBelongsToActualConnectionAcrossAdditionalAndGrace(t *testing.T) {
	for _, grace := range []bool{false, true} {
		name := "additional-live"
		if grace {
			name = "grace-reconnect"
		}
		t.Run(name, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			old := newReceiveOwnerSession(t, svc, 5701, "192.0.2.71:64646")
			svc.onPeerSessionEstablished(idPeerB, old.capabilities)
			svc.dispatchPeerSessionFrame(old.address, old, legacyBaselineFrame(idPeerB, idTargetX))
			if !routeLearnedVia(svc, idTargetX, idPeerB) {
				t.Fatal("old actual connection never established a working baseline")
			}
			if grace {
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				svc.onPeerSessionClosed(idPeerB, old.penaltySubject(), old.capabilities)
			} else {
				defer svc.onPeerSessionClosed(idPeerB, old.penaltySubject(), old.capabilities)
			}
			fresh := newReceiveOwnerSession(t, svc, 5702, "192.0.2.72:64646")
			svc.onPeerSessionEstablished(idPeerB, fresh.capabilities)
			defer svc.onPeerSessionClosed(idPeerB, fresh.penaltySubject(), fresh.capabilities)
			svc.dispatchPeerSessionFrame(fresh.address, fresh, receiveOwnerDelta(idTargetY))
			if routeLearnedVia(svc, idTargetY, idPeerB) {
				t.Fatal("new connection inherited another connection's baseline")
			}
			svc.dispatchPeerSessionFrame(fresh.address, fresh, legacyBaselineFrame(idPeerB, idTargetY))
			svc.dispatchPeerSessionFrame(fresh.address, fresh, receiveOwnerDelta(idPeerC))
			if !routeLearnedVia(svc, idPeerC, idPeerB) {
				t.Fatal("fresh own baseline did not authorize fresh delta")
			}
		})
	}
}
func TestReceiveNilOrClosedOwnerCannotEstablishBaseline(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	noOwner := &peerSession{address: "192.0.2.80:64646", peerIdentity: idPeerB, authOK: true, capabilities: legacyRoutingCaps}
	svc.dispatchPeerSessionFrame(noOwner.address, noOwner, legacyBaselineFrame(idPeerB, idTargetX))
	if routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("nil owner fell back to identity registry")
	}
	closed := newReceiveOwnerSession(t, svc, 5801, "192.0.2.81:64646")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	svc.dispatchPeerSessionFrame(closed.address, closed, legacyBaselineFrame(idPeerB, idTargetY))
	if routeLearnedVia(svc, idTargetY, idPeerB) || closed.receive.ReceiveReady() {
		t.Fatal("late closed connection republished receive evidence")
	}
}
func TestReceiveCurrentEpochIsPerActualConnection(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	old := newReceiveOwnerSession(t, svc, 5901, "192.0.2.91:64646")
	fresh := newReceiveOwnerSession(t, svc, 5902, "192.0.2.92:64646")
	full := func(session *peerSession, epoch uint64) {
		svc.handleRouteAnnounceV3(sessionRoutingSender(session), session.address, protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: epoch})
	}
	full(old, 100)
	full(fresh, 1)
	delta := protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindDelta, Epoch: 2, Entries: []protocol.RouteAnnounceV3Entry{{Identity: idTargetX.String(), Hops: 1, SeqNo: 1}}}
	for range 2 {
		svc.handleRouteAnnounceV3(sessionRoutingSender(fresh), fresh.address, delta)
		if routeLearnedVia(svc, idTargetX, idPeerB) {
			t.Fatal("higher-epoch delta inherited an older baseline")
		}
	}
	if epoch, known := old.receive.KnownV3Epoch(); !known || epoch != 100 {
		t.Fatal("another connection rewrote old connection's epoch")
	}
	full(fresh, 2)
	svc.handleRouteAnnounceV3(sessionRoutingSender(fresh), fresh.address, delta)
	if !routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("accepted current full did not enable current delta")
	}
}
func baselineDuringContactsFarEnd(ctx context.Context, peer domain.PeerIdentity) farEndFunc {
	return func(remote net.Conn, inbox chan<- protocol.Frame, errs chan<- error) {
		reader := bufio.NewReader(remote)
		send := func(frame protocol.Frame) bool {
			select {
			case inbox <- frame:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			request, err := protocol.ParseFrameLine(line)
			if err != nil {
				return
			}
			switch request.Type {
			case "get_peers":
				if !send(protocol.Frame{Type: "peers"}) {
					return
				}
			case "fetch_contacts":
				if !send(legacyBaselineFrame(peer, idTargetX)) || !send(protocol.Frame{Type: "contacts"}) {
					return
				}
			}
		}
	}
}
func TestReceiveSetupEvidenceCannotEscapeEvictedCMGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reached, gate, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	var rig *evictionRig
	rig = newEvictionRig(t, evictionRigOptions{farEnd: baselineDuringContactsFarEnd(ctx, newEvictionSession().peerIdentity), beforeEstablished: func(info SessionInfo) {
		<-built
		info.Session.capabilities = legacyRoutingCaps
		rig.svc.cmSessionRegisterBarrier = func() { close(reached); <-gate }
	}})
	close(built)
	awaitClosed(t, reached, "setup baseline before CM publication")
	if !rig.session.receive.HasReceivedBaseline() {
		t.Fatal("setup baseline was not accepted on the actual session")
	}
	if !rig.cm.RetainOnly(ctx, evictionPinAddress) {
		t.Fatal("CM eviction was not queued")
	}
	release()
	rig.awaitSessionGoroutine(t)
	if rig.session.receive.ReceiveReady() || rig.counters().registered || rig.counters().relay != 0 {
		t.Fatal("evicted setup published evidence or unbalanced session accounting")
	}
	fresh := newReceiveOwnerSession(t, rig.svc, 6002, "192.0.2.102:64646")
	fresh.peerIdentity = rig.session.peerIdentity
	delta := receiveOwnerDelta(idTargetY)
	delta.AnnounceRoutes[0].Origin = fresh.peerIdentity.String()
	rig.svc.dispatchPeerSessionFrame(fresh.address, fresh, delta)
	if routeLearnedVia(rig.svc, idTargetY, fresh.peerIdentity) {
		t.Fatal("stale CM setup authorized replacement delta")
	}
	rig.svc.dispatchPeerSessionFrame(fresh.address, fresh, legacyBaselineFrame(fresh.peerIdentity, idTargetX))
	rig.svc.dispatchPeerSessionFrame(fresh.address, fresh, delta)
	if !routeLearnedVia(rig.svc, idTargetY, fresh.peerIdentity) {
		t.Fatal("replacement baseline did not enable otherwise eligible delta")
	}
}

func TestReceiveInboundDispatcherUsesRegisteredConnectionOwner(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	register := func(id domain.ConnID, port int) domain.ConnID {
		conn := newNoIOConn(t, &net.TCPAddr{IP: net.ParseIP("192.0.2.120"), Port: port})
		core := newTestNetCore(t, id, conn, netcore.Inbound, netcore.Options{Address: domain.PeerAddress(conn.RemoteAddr().String()), Identity: idPeerB, Caps: legacyRoutingCaps})
		core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: idPeerB.String()}})
		svc.peerMu.Lock()
		svc.registerInboundConnLocked(conn, core, nil, nil)
		svc.peerMu.Unlock()
		return id
	}
	old, fresh := register(6101, 5101), register(6102, 5102)
	dispatchInboundLegacyFrame(t, svc, old, legacyBaselineFrame(idPeerB, idTargetX))
	if !routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("old inbound baseline was not actually accepted")
	}
	dispatchInboundLegacyFrame(t, svc, fresh, receiveOwnerDelta(idTargetY))
	if routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("new inbound connection inherited baseline")
	}
	dispatchInboundLegacyFrame(t, svc, fresh, legacyBaselineFrame(idPeerB, idTargetX))
	dispatchInboundLegacyFrame(t, svc, fresh, receiveOwnerDelta(idTargetY))
	if !routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("own inbound full did not enable dispatcher delta")
	}
}

func TestReceiveSetupBaselineSurvivesActualCMRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	rig := newEvictionRig(t, evictionRigOptions{farEnd: baselineDuringContactsFarEnd(ctx, newEvictionSession().peerIdentity), beforeEstablished: func(info SessionInfo) { info.Session.capabilities = legacyRoutingCaps }})
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !rig.counters().registered {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("actual CM registration did not complete")
		}
	}
	delta := receiveOwnerDelta(idTargetY)
	delta.AnnounceRoutes[0].Origin = rig.session.peerIdentity.String()
	rig.svc.dispatchPeerSessionFrame(rig.session.address, rig.session, delta)
	if !routeLearnedVia(rig.svc, idTargetY, rig.session.peerIdentity) {
		t.Fatal("actual successful CM registration erased setup baseline")
	}
}

func TestReceiveFailedSetupEvidenceCannotAuthorizeReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.runCtx = ctx
	session := newReceiveOwnerSession(t, svc, 6201, "192.0.2.121:64646")
	session.inboxCh = make(chan protocol.Frame)
	// Exercise the real request owner: a full frame is consumed while awaiting
	// contacts, then transport failure prevents successful setup publication.
	go func() {
		session.inboxCh <- legacyBaselineFrame(idPeerB, idTargetX)
		session.errCh <- io.ErrUnexpectedEOF
	}()
	if err := svc.syncPeerSession(session, false, peerExchangePathSessionOutbound); err == nil {
		t.Fatal("failed setup succeeded")
	}
	if !session.receive.HasReceivedBaseline() {
		t.Fatal("failed-setup premise never accepted full")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := newReceiveOwnerSession(t, svc, 6202, "192.0.2.122:64646")
	svc.dispatchPeerSessionFrame(fresh.address, fresh, receiveOwnerDelta(idTargetY))
	if routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("failed setup authorized replacement")
	}
	svc.dispatchPeerSessionFrame(fresh.address, fresh, legacyBaselineFrame(idPeerB, idTargetX))
	svc.dispatchPeerSessionFrame(fresh.address, fresh, receiveOwnerDelta(idTargetY))
	if !routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("replacement full did not enable eligible delta")
	}
}
