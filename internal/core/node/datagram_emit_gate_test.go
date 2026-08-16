package node

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_emit_gate_test.go pins §4.3 line 574 where the node implements it:
// "the metadata of a candidate describes ONE concrete connection — the one the
// send will try — and comes from the same helper".
//
// The scheduler judges a peer by the HEAD of its connection list
// (PeerMetadata.SendableConnection) and runs the full gate set over it: the
// role gate of §6 and the last-hop dtype gate of §6.1. The emitter then walks
// the peer's connections until one queue
// accepts. Walking them UNFILTERED meant the bytes could leave over a second
// socket of the same peer whose advertised set and declared dtypes nobody had
// looked at — a candidate admitted on the strength of one connection, delivered
// through another that would have been refused.
//
// Every case below is the same shape: the first connection passes the gates and
// refuses the frame at its queue, the second one would take it and does not
// pass. The control case flips the second connection's declaration and nothing
// else, which is what proves the walk really does fall back when the gate
// allows it — otherwise "no fall-back happened" would be indistinguishable from
// "there is no fall-back at all".

// declarationsWithout returns the standard fixture declarations minus one
// capability name.
func declarationsWithout(missing domain.CapabilityName) *netcore.HandshakeDeclarations {
	full := datagramPeerDeclarations()
	kept := make([]domain.CapabilityName, 0, len(full.AdvertisedNames))
	for _, name := range full.AdvertisedNames {
		if name == missing {
			continue
		}
		kept = append(kept, name)
	}
	return &netcore.HandshakeDeclarations{AdvertisedNames: kept, DeclaredDTypes: full.DeclaredDTypes}
}

// declarationsHandlingNoDType returns the standard declarations with an
// EXPLICITLY EMPTY dtypes field: §6.1's "I speak the envelope and handle no
// type at all", which the last-hop gate must refuse for every dtype without
// exception — there is no set a peer is credited with for free.
func declarationsHandlingNoDType() *netcore.HandshakeDeclarations {
	declarations := datagramPeerDeclarations()
	declarations.DeclaredDTypes = domain.ExplicitDTypes(nil)
	return &declarations
}

// datagramEmitFixture wires one peer with an outbound session that passes the
// gates and an inbound connection described by `fallback`, and returns the
// emitter plus the outbound session whose queue the test jams.
func datagramEmitFixture(
	t *testing.T,
	frame protocol.DatagramFrame,
	peer domain.PeerIdentity,
	fallback *netcore.HandshakeDeclarations,
) (datagramFrameEmitter, *peerSession, datagram.OutboundFrame) {
	t.Helper()

	svc := newDatagramLayerService(t, true)
	now := time.Now().UTC()
	addresses := installDatagramPeer(t, svc, peer,
		datagramPeerConn{version: 27, connectedAt: now.Add(-time.Hour)},
		datagramPeerConn{version: 27, connectedAt: now.Add(-time.Minute), inbound: true, declared: fallback},
	)
	requireDatagramPlane(t, svc)

	svc.peerMu.RLock()
	session := svc.sessions[addresses[false]]
	svc.peerMu.RUnlock()
	if session == nil {
		t.Fatal("the fixture did not install the outbound session")
	}

	return datagramEmitterOf(svc), session, datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: now.Add(5 * time.Second),
	}
}

// TestEmitToNeverFallsBackToAnUngatedConnection is the finding.
//
// The mutation it kills is the original code: emitting over
// datagramSendTargetsLocked — every live connection of the peer — instead of
// over the frame-gated list. Under that mutation each case below queues the
// frame on the inbound connection and EmitTo reports true.
func TestEmitToNeverFallsBackToAnUngatedConnection(t *testing.T) {
	t.Parallel()

	destination := domain.PeerIdentityFromWire(datagramTestDstHex)
	// A relay is any peer that is not the destination, which is the whole
	// definition of the transit role (§2.2 rule 2).
	relay := domain.PeerIdentityFromWire("1111111111111111111111111111111111111111")

	cases := map[string]struct {
		frame    protocol.DatagramFrame
		peer     domain.PeerIdentity
		refused  *netcore.HandshakeDeclarations
		admitted *netcore.HandshakeDeclarations
	}{
		// §6: a transit candidate must advertise
		// mesh_datagram_transit_v1. The destination is somebody else, so the
		// peer is judged as transit on both of its connections.
		"transit capability": {
			frame:    newNodeDatagram(t, nil),
			peer:     relay,
			refused:  declarationsWithout(datagram.CapabilityDatagramTransitV1),
			admitted: nil,
		},
		// §6: without mesh_datagram_v1 the command does not exist for the peer
		// at all, in either role — the destination included.
		"endpoint capability": {
			frame:    newNodeDatagram(t, nil),
			peer:     destination,
			refused:  declarationsWithout(datagram.CapabilityDatagramV1),
			admitted: nil,
		},
		// §4.3 / §6.1: the last-hop dtype gate. An explicitly empty `dtypes`
		// refuses every type without exception.
		"last-hop dtype": {
			frame:    newNodeDatagram(t, nil),
			peer:     destination,
			refused:  declarationsHandlingNoDType(),
			admitted: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Control: with a fall-back connection that DOES pass the gates,
			// a jammed first queue still gets the frame out. Without this the
			// assertion below would pass on a plane that never falls back.
			emitter, session, out := datagramEmitFixture(t, tc.frame, tc.peer, tc.admitted)
			session.closeSendQueue()
			if !emitter.EmitTo(context.Background(), out) {
				t.Fatal("the gated fall-back connection did not take a frame its first connection refused")
			}

			// The finding: the same jam, with the fall-back connection failing
			// exactly one gate.
			emitter, session, out = datagramEmitFixture(t, tc.frame, tc.peer, tc.refused)
			session.closeSendQueue()
			if emitter.EmitTo(context.Background(), out) {
				t.Fatal("the frame left over a connection that never passed the frame's gates")
			}
		})
	}
}

// TestEmitToAppliesTheGateToTheFirstConnectionToo pins the other half: the
// filter is not a fall-back-only rule.
//
// If the head connection itself stops passing the gates between the moment the
// scheduler judged it and the moment the frame is emitted — the queue is not
// instantaneous — the frame must not go out over it either. Fail-closed is the
// only safe side here: the alternative is sending a frame to a socket that
// never claimed to speak the plane.
func TestEmitToAppliesTheGateToTheFirstConnectionToo(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	frame := newNodeDatagram(t, nil)

	svc := newDatagramLayerService(t, true)
	installDatagramPeer(t, svc, peer, datagramPeerConn{
		version:     27,
		connectedAt: time.Now().UTC(),
		declared:    declarationsWithout(datagram.CapabilityDatagramV1),
	})
	requireDatagramPlane(t, svc)

	if datagramEmitterOf(svc).EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: time.Now().UTC().Add(5 * time.Second),
	}) {
		t.Fatal("the only connection of the peer failed the role gate and the frame was queued on it anyway")
	}
}

// TestEmitToLeavesTheResponsePlaneUngated pins the exemption, which is a
// property of the plane and not a hole.
//
// A `response` has no candidate: its next hop is the `upstream` of the
// reverse-state record (§4.2), which the layer never puts through
// AdmitCandidate. There is therefore no metadata for the sockets to agree with,
// and demanding mesh_datagram_transit_v1 from the neighbour a reply is owed to
// would drop exactly the answers the reverse state exists to deliver.
func TestEmitToLeavesTheResponsePlaneUngated(t *testing.T) {
	t.Parallel()

	upstream := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")
	frame := newDatagramResponseFrame(t)

	svc := newDatagramLayerService(t, true)
	installDatagramPeer(t, svc, upstream, datagramPeerConn{
		version:     27,
		connectedAt: time.Now().UTC(),
		declared:    declarationsWithout(datagram.CapabilityDatagramTransitV1),
	})
	requireDatagramPlane(t, svc)

	if !datagramEmitterOf(svc).EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      upstream,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: time.Now().UTC().Add(5 * time.Second),
	}) {
		t.Fatal("a response was refused because its upstream neighbour does not relay other people's datagrams")
	}
}

// newDatagramResponseFrame builds the one unsigned mode: a response carries no
// auth, no route_policy and a `dst` that is the one-shot label of the request
// it answers (§2.1.1).
func newDatagramResponseFrame(t *testing.T) protocol.DatagramFrame {
	t.Helper()
	subject, err := domain.ParsePeerIdentity(datagramTestDstHex)
	if err != nil {
		t.Fatalf("ParsePeerIdentity(subject): %v", err)
	}
	label, err := domain.ParsePeerIdentity("3333333333333333333333333333333333333333")
	if err != nil {
		t.Fatalf("ParsePeerIdentity(label): %v", err)
	}
	return protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion,
		Mode:    domain.DatagramModeResponse,
		Class:   domain.DatagramClassControl,
		Src:     subject,
		Dst:     label,
		TTL:     9,
		DType:   domain.DType("fixture_alt_response"),
		Payload: []byte{0x01, 0x02, 0x03},
	}
}
