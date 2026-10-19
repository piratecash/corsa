package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// This drives the real sync request owner and unsolicited dispatcher before
// the lifecycle hook, on both call paths. No direct MarkBaselineReceived call
// can vouch for the test: the accepted baseline must arrive during contacts.
func TestAcceptedSetupBaselineSurvivesSameConnectionPublication(t *testing.T) {
	for _, cm := range []bool{false, true} {
		name := "legacy"
		if cm {
			name = "CM"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			svc := newTestService(t, config.NodeTypeFull)
			svc.runCtx = ctx
			local, remote := net.Pipe()
			defer func() { _ = remote.Close() }()
			session := &peerSession{address: domain.PeerAddress("192.0.2.50:64646"), peerIdentity: idPeerB, connID: 5501, conn: local, authOK: true, capabilities: legacyRoutingCaps, sendCh: make(chan peerSendItem, 8), inboxCh: make(chan protocol.Frame, 8), errCh: make(chan error, 1)}
			if err := svc.attachOutboundNetCore(session); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := session.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Error(err)
				}
			}()
			readerDone := make(chan struct{})
			go func() { defer close(readerDone); svc.readPeerSession(bufio.NewReader(local), session) }()
			peerDone := make(chan struct{})
			go func() {
				defer close(peerDone)
				reader := bufio.NewReader(remote)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					request, err := protocol.ParseFrameLine(line)
					if err != nil {
						return
					}
					switch request.Type {
					case "get_peers":
						raw, err := protocol.MarshalFrameLine(protocol.Frame{Type: "peers"})
						if err != nil {
							return
						}
						if _, err := remote.Write([]byte(raw)); err != nil {
							return
						}
					case "fetch_contacts":
						baseline, err := protocol.MarshalFrameLine(legacyBaselineFrame(idPeerB, idTargetX))
						if err != nil {
							return
						}
						if _, err := remote.Write([]byte(baseline)); err != nil {
							return
						}
						contacts, err := protocol.MarshalFrameLine(protocol.Frame{Type: "contacts"})
						if err != nil {
							return
						}
						if _, err := remote.Write([]byte(contacts)); err != nil {
							return
						}
					}
				}
			}()
			defer func() { _ = session.Close(); _ = remote.Close(); <-readerDone; <-peerDone }()
			if cm {
				if err := svc.initPeerSession(session); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := svc.syncPeerSession(session, false, peerExchangePathSessionOutbound); err != nil {
					t.Fatal(err)
				}
			}
			if !routeLearnedVia(svc, idTargetX, idPeerB) {
				t.Fatal("setup did not dispatch the actual interleaved baseline")
			}
			svc.peerMu.Lock()
			svc.sessions[session.address] = session
			svc.peerMu.Unlock()
			svc.onPeerSessionEstablished(session.peerIdentity, session.capabilities)
			defer svc.onPeerSessionClosed(session.peerIdentity, session.penaltySubject(), session.capabilities)
			svc.dispatchPeerSessionFrame(session.address, session, protocol.Frame{Type: "routes_update", AnnounceRoutes: []protocol.AnnounceRouteFrame{{Identity: idTargetY.String(), Origin: idPeerB.String(), Hops: 1, SeqNo: 1}}})
			if !routeLearnedVia(svc, idTargetY, idPeerB) {
				t.Fatal("publication erased receive baseline already accepted by this connection during setup")
			}
		})
	}
}
