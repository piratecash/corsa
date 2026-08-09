package node

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// session_handshake_gate_test.go pins ONE rule on the dialled direction: a
// capability the welcome negotiated is not IN FORCE until the handshake that
// negotiated it has COMPLETED.
//
// The two moments are a round trip apart and the reader runs across both of
// them. openPeerSession publishes the negotiated set from applyWelcomeMetadata
// as soon as the welcome validates, then sends auth_session and waits for
// auth_ok; readPeerSession was started before the welcome and dispatches every
// line it reads in between. A `datagram` pipelined into that window used to be
// judged by a capability set that was already full, so it reached the datagram
// pipeline — and every hook behind it — on a connection whose handshake had not
// finished.
//
// The INBOUND direction has no such window and needs no fix here: every
// p2pWireCommand (including `datagram`) is answered auth_required by
// dispatchNetworkFrame until isConnAuthenticated is true, the datagram diversion
// repeats that check itself in dispatchInboundDatagramWire, and
// inboundDatagramNeighbour derives its budget key from the PROVEN identity, so an
// unauthenticated inbound connection is not even billable.

// ---------------------------------------------------------------------------
// The window, through the production handshake
// ---------------------------------------------------------------------------

// preAuthDatagramScript is the remote half of the dial, and its every step is
// driven by a line THIS node wrote. That is what makes the test deterministic
// without a single sleep:
//
//   - reading our `auth_session` proves applyWelcomeMetadata has run, because
//     openPeerSession publishes the negotiated set between validating the welcome
//     and signing the challenge. So the datagram written next lands with the
//     capability set already assigned — the only state in which the defect is
//     observable at all;
//   - the datagram is written BEFORE auth_ok, and readPeerSession processes the
//     socket line by line on one goroutine, dispatching a diverted datagram
//     inline. Its dispatch therefore completes before the auth_ok line is even
//     read, hence strictly before session.authOK can be written;
//   - reading the first request that follows auth_ok — get_peers or
//     fetch_contacts, whichever shouldRequestPeers chose, and exactly one line
//     either way — proves the handshake completed, because openPeerSession only
//     reaches syncPeerSession after authenticatePeerSession returned. The
//     datagram written next is the positive control;
//   - the final `error` reply ends syncPeerSession, so openPeerSession returns and
//     the assertions run on a settled node.
type preAuthDatagramScript struct {
	welcome  string
	datagram string
	authOK   string
	refusal  string
}

func (script preAuthDatagramScript) run(ln net.Listener) error {
	conn, err := ln.Accept()
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
	reader := bufio.NewReader(conn)

	steps := []struct {
		expectLine string
		write      string
	}{
		{expectLine: "hello", write: script.welcome},
		// The pre-auth datagram: the negotiated set is published, auth_ok is not
		// sent yet.
		{expectLine: "auth_session", write: script.datagram + script.authOK},
		// The post-handshake datagram plus the reply that ends the session.
		{expectLine: "the first request after auth_ok", write: script.datagram + script.refusal},
	}
	for _, step := range steps {
		if _, err := reader.ReadString('\n'); err != nil {
			return fmt.Errorf("read %s: %w", step.expectLine, err)
		}
		if _, err := conn.Write([]byte(step.write)); err != nil {
			return fmt.Errorf("write after %s: %w", step.expectLine, err)
		}
	}
	return nil
}

// TestOutboundSessionDatagramBeforeAuthOKIsOffThePlane is the finding: the same
// datagram, from the same peer, on the same session, must be refused before the
// handshake completes and served after it.
//
// Both halves are asserted on ONE node so neither can pass for the wrong reason:
// a gate that refuses everything fails the positive control, and a gate that
// refuses nothing fails the negative one.
//
// The admission ledger is asserted too, and it is the third leg: BOTH frames are
// charged. A refusal that ran above the §4.1 step 1 charge would be the cheapest
// verdict on the node and also its only free one, which is the hole
// datagram_budget_key_test.go closed — this gate must not reopen it.
//
// The mutation this kills: dropping the authOK requirement from
// sessionHasCapability. The pre-auth datagram is then judged by a set
// applyWelcomeMetadata has already filled, reaches the conveyor, and Observed
// counts two frames instead of one.
func TestOutboundSessionDatagramBeforeAuthOKIsOffThePlane(t *testing.T) {
	svc := newDatagramSessionService(t)

	// The line the peer pipelines. mustDatagramLine already terminates it, which
	// is what lets the script concatenate it with the frame that follows.
	line := mustDatagramLine(t, newNodeDatagram(t, nil))

	welcome, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Address:                datagramTestDstHex,
		// authenticatePeerSession refuses a welcome without a challenge before it
		// writes anything, which would collapse the script's second step.
		Challenge: "corsa-session-handshake-gate",
		// mesh_datagram_v1 and nothing else: the routing triplet would make
		// openPeerSession emit a connect-time full sync, adding a line the script
		// would have to answer for no gain.
		Capabilities: []string{string(domain.CapMeshDatagramV1)},
	})
	if err != nil {
		t.Fatalf("marshal welcome: %v", err)
	}
	authOK, err := protocol.MarshalFrameLine(protocol.Frame{Type: "auth_ok"})
	if err != nil {
		t.Fatalf("marshal auth_ok: %v", err)
	}
	// Any error reply ends the request the sync issued, and with it
	// openPeerSession. The code is irrelevant to this test — what matters is
	// that the session terminates on a frame the script controls, not on a
	// timeout.
	refusal, err := protocol.MarshalFrameLine(protocol.Frame{
		Type: "error",
		Code: protocol.ErrCodeUnknownCommand,
	})
	if err != nil {
		t.Fatalf("marshal error reply: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	scriptErr := make(chan error, 1)
	go func() {
		scriptErr <- preAuthDatagramScript{
			welcome:  welcome,
			datagram: line,
			authOK:   authOK,
			refusal:  refusal,
		}.run(ln)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := svc.openPeerSession(ctx, domain.PeerAddress(ln.Addr().String())); err == nil {
		t.Fatal("the scripted peer answers the post-auth request with an error: openPeerSession must surface it")
	}
	if err := <-scriptErr; err != nil {
		t.Fatalf("scripted peer: %v", err)
	}

	if got := datagramObservedCount(svc); got != 1 {
		t.Fatalf("the conveyor decided on %d datagrams, want exactly 1 — the pre-auth frame must not reach it and the post-handshake one must", got)
	}
	if got := datagramDropCount(svc, datagram.DropPlaneNotNegotiated); got != 1 {
		t.Fatalf("plane_not_negotiated drops = %d, want 1: a frame that arrived before auth_ok is off the plane and must be counted as such", got)
	}
	stats := datagramAdmissionStats(svc)
	if stats.Admitted != 2 {
		t.Fatalf("charged %d frames, want 2 — the refusal must stand BELOW the §4.1 step 1 charge, or it is a free load channel", stats.Admitted)
	}
	if stats.AdmittedBytes != uint64(2*len(line)) {
		t.Fatalf("charged %d bytes, want %d (the whole wire line, twice)", stats.AdmittedBytes, 2*len(line))
	}
}

// ---------------------------------------------------------------------------
// The gate itself
// ---------------------------------------------------------------------------

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
	}, svc.cfg.EnableMeshRoutingV3, advertise)
	svc.peerMu.Unlock()

	svc.peerMu.RLock()
	assigned := len(session.capabilities)
	svc.peerMu.RUnlock()
	if assigned != len(negotiated) {
		t.Fatalf("the welcome negotiated %d capabilities, want %d: the fixture cannot tell 'assigned' from 'in force'", assigned, len(negotiated))
	}

	for _, capability := range negotiated {
		if svc.sessionHasCapability(session, capability) {
			t.Fatalf("%s counted as negotiated before auth_ok arrived", capability)
		}
	}

	svc.markSessionHandshakeComplete(session)

	for _, capability := range negotiated {
		if !svc.sessionHasCapability(session, capability) {
			t.Fatalf("%s stopped being negotiated after the handshake completed: the gate became a permanent refusal", capability)
		}
	}
	if svc.sessionHasCapability(session, domain.CapMeshRoutingV3) {
		t.Fatal("a capability the welcome never carried must stay refused after the handshake completes")
	}
}
