package node

import (
	"net"
	"reflect"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Native functional capability regression only: the real local selection,
// projection, original inbound metadata and original outbound metadata are
// exercised. This fixture neither mints proof/auth/F1 Sessions nor claims a
// full TLS/publication/512-population or physical-resource result.
func TestOwnedFindNodeHandshakeCapabilitySelection(t *testing.T) {
	f1, relay := string(domain.CapOverlayFindNodeV1), string(domain.CapMeshRelayV1)
	for _, tc := range []struct {
		name                         string
		selected                     bool
		remote                       []string
		wantF1, wantRelay, wantTotal int
	}{
		{"default-off", false, []string{f1, relay}, 0, 1, 1},
		{"selected-peer-present", true, []string{f1, relay}, 1, 1, 2},
		{"selected-peer-absent", true, []string{relay}, 0, 1, 1},
		{"selected-duplicate", true, []string{f1, f1, relay, relay}, 1, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{conns: make(map[netcore.ConnID]*connEntry), connIDByNetConn: make(map[net.Conn]netcore.ConnID)}
			if tc.selected {
				// Same existing private pre-handshake selection, no setter/API.
				svc.ownedFindNodeAdvertiseTest = true
			}
			local := svc.localHandshakeCapabilities() // before any peerMu
			wire := localHandshakeCapabilityStrings(svc.localHandshakeCapabilityNames())
			if !reflect.DeepEqual(wire, domain.CapabilityStrings(local)) {
				t.Fatal("wire and typed local sets diverged", wire, local)
			}
			wireF1 := 0
			for _, name := range wire {
				if name == f1 {
					wireF1++
				}
			}
			wantWire := 0
			if tc.selected {
				wantWire = 1
			}
			if wireF1 != wantWire {
				t.Fatal("missing/duplicate local F1 advertisement", wire)
			}
			if !tc.selected && !reflect.DeepEqual(local, localCapabilities(svc.cfg.EnableMeshRoutingV3, svc.localDatagramAdvertise())) {
				t.Fatal("ordinary role set changed")
			}
			if svc.findNode != nil || svc.bridgeCapture != nil {
				t.Fatal("capability selection installed DHT")
			}
			assertCaps := func(label string, caps []domain.Capability) {
				t.Helper()
				gotF1, gotRelay := 0, 0
				for _, cap := range caps {
					if cap == domain.CapOverlayFindNodeV1 {
						gotF1++
					}
					if cap == domain.CapMeshRelayV1 {
						gotRelay++
					}
				}
				if gotF1 != tc.wantF1 || gotRelay != tc.wantRelay || len(caps) != tc.wantTotal {
					t.Fatal(label, caps, tc)
				}
			}
			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			defer func() { _ = server.Close() }()
			inbound := newTestNetCore(t, netcore.ConnID(1), server, netcore.Inbound, netcore.Options{})
			svc.setTestConnEntryLocked(server, &connEntry{core: inbound})
			hello := protocol.Frame{Type: "hello", Address: datagramTestDstHex, Capabilities: tc.remote}
			svc.rememberConnPeerAddr(inbound.ConnID(), hello, "10.0.0.1:55555")
			assertCaps("original inbound negotiation", inbound.CapabilitiesRef())
			if inbound.Auth() != nil {
				t.Fatal("metadata negotiation fabricated auth")
			}

			otherClient, otherServer := net.Pipe()
			defer func() { _ = otherClient.Close() }()
			defer func() { _ = otherServer.Close() }()
			outbound := newTestNetCore(t, netcore.ConnID(2), otherServer, netcore.Outbound, netcore.Options{})
			session := &peerSession{address: "10.0.0.2:64646", netCore: outbound}
			welcome := protocol.Frame{Type: "welcome", Address: datagramTestDstHex, Capabilities: tc.remote}
			svc.peerMu.Lock()
			applyWelcomeMetadata(session, welcome, local)
			svc.peerMu.Unlock()
			assertCaps("original outbound session negotiation", session.capabilities)
			assertCaps("same outbound NetCore mirror", outbound.CapabilitiesRef())
			if session.authOK || outbound.Auth() != nil || svc.sessionHasCapability(session, domain.CapOverlayFindNodeV1) {
				t.Fatal("negotiated metadata bypassed original authentication gate")
			}
		})
	}
}
