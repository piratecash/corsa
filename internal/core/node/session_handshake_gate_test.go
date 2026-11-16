package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// The dispatch gate charges a datagram before it tests capabilities. Keep
// the same frame and session for the negative and positive controls.
func TestOutboundSessionDatagramBeforeReadyIsOffThePlane(t *testing.T) {
	svc := newDatagramSessionService(t)
	session := welcomedOutboundSession(t, svc, "198.51.100.77:64646", datagramTestDstHex, domain.CapMeshDatagramV1)
	wire := mustDatagramLine(t, newNodeDatagram(t, nil))

	svc.peerMu.Lock()
	session.authOK = false
	svc.peerMu.Unlock()
	svc.dispatchSessionDatagramLine(session, wire)
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("%d datagrams reached the conveyor before readiness", got)
	}
	if got := datagramDropCount(svc, datagram.DropPlaneNotNegotiated); got != 1 {
		t.Fatalf("plane_not_negotiated drops = %d, want 1", got)
	}

	svc.peerMu.Lock()
	session.authOK = true
	svc.peerMu.Unlock()
	svc.dispatchSessionDatagramLine(session, wire)
	if got := datagramObservedCount(svc); got != 1 {
		t.Fatalf("conveyor observed %d datagrams after readiness, want 1", got)
	}
	stats := datagramAdmissionStats(svc)
	if stats.Admitted != 2 || stats.AdmittedBytes != uint64(2*len(wire)) {
		t.Fatalf("admission = %+v, want both wire frames charged", stats)
	}
}

// TestSessionCapabilityRequiresACompletedHandshake pins the predicate directly,
// on a session built through the PRODUCTION derivation: applyWelcomeMetadata
// assigns the intersection, and until the handshake is marked complete every
// member of that intersection must still answer false.
//
// The "assigned" assertion in the middle is what makes the first loop mean
// something: without it the test would also pass on a fixture whose welcome
// negotiated nothing at all, which is the state the old fail-closed argument
// relied on and the exact reason the window went unnoticed.
//
// The mutation this kills: answering from len(session.capabilities) — the shape
// the helper's doc used to justify — instead of from the explicit state.
func TestSessionCapabilityRequiresACompletedHandshake(t *testing.T) {
	svc := newDatagramSessionService(t)

	negotiated := []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshRelayV1}
	names := make([]string, 0, len(negotiated))
	for _, capability := range negotiated {
		names = append(names, string(capability))
	}

	session := &peerSession{
		address: domain.PeerAddress("198.51.100.77:64646"),
		sendCh:  make(chan peerSendItem, 4),
		inboxCh: make(chan protocol.Frame, 8),
		errCh:   make(chan error, 4),
	}
	// Resolved outside peerMu, exactly as openPeerSession resolves it.
	advertise := svc.localDatagramAdvertise()
	svc.peerMu.Lock()
	applyWelcomeMetadata(session, protocol.Frame{
		Version:      config.ProtocolVersion,
		Address:      datagramTestDstHex,
		Capabilities: names,
	}, localCapabilities(svc.cfg.EnableMeshRoutingV3, advertise))
	svc.peerMu.Unlock()

	svc.peerMu.RLock()
	assigned := len(session.capabilities)
	svc.peerMu.RUnlock()
	if assigned != len(negotiated) {
		t.Fatalf("the welcome negotiated %d capabilities, want %d: the fixture cannot tell 'assigned' from 'in force'", assigned, len(negotiated))
	}

	for _, capability := range negotiated {
		if svc.sessionHasCapability(session, capability) {
			t.Fatalf("%s counted as negotiated before session readiness", capability)
		}
	}

	if err := svc.markSessionHandshakeComplete(session); err != nil {
		t.Fatal(err)
	}

	for _, capability := range negotiated {
		if !svc.sessionHasCapability(session, capability) {
			t.Fatalf("%s stopped being negotiated after the handshake completed: the gate became a permanent refusal", capability)
		}
	}
	if svc.sessionHasCapability(session, domain.CapMeshRoutingV3) {
		t.Fatal("a capability the welcome never carried must stay refused after the handshake completes")
	}
}
