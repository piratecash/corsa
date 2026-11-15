package node

import (
	"bufio"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// Drive the original read -> dispatch boundary on a real legacy TCP pair.
// The final line is read before the v2 pin closes the transport, then held
// until the close has joined the writer. No production scheduling hook is used.
func TestLegacyLineReadBeforePinCannotDispatchAfterTransportRelease(t *testing.T) {
	for _, command := range []string{"announce_peer", "auth_session", "ping", protocol.DatagramFrameType} {
		t.Run(command, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			svc.routeWithdrawalGracePeriodTest = -1
			peer, owner := sessionv2test.NewProvenPeer(t)
			proof := sessionv2test.Proof(t, peer)
			server, client := tcpPair(t)
			if err := server.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := svc.registerInboundConn(server, nil); err != nil {
				t.Fatal(err)
			}
			connID, found := svc.connIDFor(server)
			if !found {
				t.Fatal("legacy transport was not registered")
			}
			t.Cleanup(func() {
				if address := svc.inboundPeerAddress(connID); address != "" {
					svc.trackInboundDisconnectWithPresenceEvidence(connID, address, nil)
				}
				svc.unregisterInboundConn(server)
				svc.removeSubscriberConnID(connID)
				svc.clearConnAuth(connID)
			})
			nodeReader, peerReader := bufio.NewReader(server), bufio.NewReader(client)
			readLine := func(frame protocol.Frame) string {
				t.Helper()
				data, err := json.Marshal(frame)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.Write(append(data, '\n')); err != nil {
					t.Fatal(err)
				}
				line, err := nodeReader.ReadString('\n')
				if err != nil {
					t.Fatal(err)
				}
				return line
			}
			hello := protocol.Frame{
				Type: "hello", Client: "node", Version: 30, MinimumProtocolVersion: 28,
				Address: owner.Address, PubKey: identity.PublicKeyBase64(owner.PublicKey),
				BoxKey: identity.BoxPublicKeyBase64(owner.BoxPublicKey), BoxSig: identity.SignBoxKeyBinding(owner),
			}
			if !svc.handleCommand(connID, readLine(hello)) {
				t.Fatal("legacy hello refused before pin")
			}
			welcome := readJSONTestFrame(t, peerReader)
			if welcome.Type != "welcome" || welcome.Challenge == "" {
				t.Fatal("missing legacy challenge")
			}
			auth := protocol.Frame{Type: "auth_session", Address: owner.Address,
				Signature: identity.SignPayload(owner, protocol.SessionAuthPayload(welcome.Challenge, owner.Address))}
			if !svc.handleCommand(connID, readLine(auth)) || readJSONTestFrame(t, peerReader).Type != "auth_ok" {
				t.Fatal("legacy signature was not accepted before pin")
			}
			core := svc.netCoreForID(connID)
			if core == nil || !core.Auth().Verified {
				t.Fatal("missing original authenticated transport")
			}
			candidate := domain.PeerAddress("203.0.113.92:64646")
			frame := protocol.Frame{Type: command, NodeType: "full", Peers: []string{string(candidate)}}
			if command == "auth_session" {
				frame = auth
			}
			line := readLine(frame)
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
				t.Fatal("pin publication did not release the legacy transport")
			}
			accepted := svc.handleCommand(connID, line)
			svc.peerMu.RLock()
			_, learned := svc.findKnownPeerByIPLocked(candidate)
			svc.peerMu.RUnlock()
			if learned {
				t.Error("released legacy transport still changed peer discovery")
			}
			if accepted {
				t.Error("line read before pin was accepted after transport release")
			}
		})
	}
}
