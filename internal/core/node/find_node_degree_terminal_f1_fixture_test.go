package node

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Narrow real-TLS fixture: negotiated F1 is bound from its genuine proof, then
// degree witness is minted by the unchanged sole production publication root.
// This does NOT exercise Node registry/F1-open writer closure or online intake.
func ownedTerminalBoundF1Fixture(t *testing.T, s, dialer *Service) (*proofTransportLifetime, findnode.Session) {
	t.Helper()
	server, client := tcpPair(t)
	rawS := &peekedConn{Conn: server, reader: server}
	rawD := &peekedConn{Conn: client, reader: client}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	type accepted struct {
		proof sessionv2.ProvenSession
		err   error
	}
	result := make(chan accepted, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		_ = rawS.Close()
		_ = rawD.Close()
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Error("owned F1 accept did not join")
		}
	})
	welcome, hello := s.welcomeFrame("", ""), dialer.nodeHelloFrame()
	welcome.Capabilities = append(welcome.Capabilities, string(domain.CapOverlayFindNodeV1))
	hello.Capabilities = append(hello.Capabilities, string(domain.CapOverlayFindNodeV1))
	go func() {
		defer close(joined)
		proof, err := sessionv2.Accept(ctx, rawS, s.secureSessions.local, welcome, s.secureSessions.certificates, sessionv2.DefaultTimeouts)
		result <- accepted{proof, err}
	}()
	if _, err := sessionv2.Dial(ctx, rawD, dialer.secureSessions.local, hello, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts); err != nil {
		t.Fatal(err)
	}
	var got accepted
	select {
	case got = <-result:
	case <-ctx.Done():
		t.Fatal("owned F1 accept timeout", ctx.Err())
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	peer, err := got.proof.Peer()
	if err != nil {
		t.Fatal(err)
	}
	protected, err := got.proof.Conn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.secureSessions.store.noteProvenInboundBinding(peer.Identity); err != nil {
		t.Fatal(err)
	}
	l := &proofTransportLifetime{token: s.proofToken, bridgeTerminal: s.bridgeTerminal, role: sessionv2.RoleListener}
	wire := &peekedConn{Conn: protected, reader: protected}
	l.evidence = &proofTransportEvidence{session: got.proof, protected: protected, raw: rawS, wire: wire, group: domain.SourceGroupLoopback, identity: peer.Identity}
	if !rawS.bindProofTransport(l) || !wire.bindProofTransport(l) {
		t.Fatal("owned F1 original binding refused")
	}
	t.Cleanup(l.end)
	s.publishDegreeTransport(l)
	if l.degreeWitness == nil || l.degreeWitness.origin != l {
		t.Fatal("owned F1 fixture did not pass genuine degree mint")
	}
	bound, err := findnode.NewSession(got.proof, domain.ConnID(1), domain.SourceGroupLoopback, 0, []domain.Capability{domain.CapOverlayFindNodeV1})
	if err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	l.findNode = &findNodeConnectionBinding{session: bound, published: true, group: domain.SourceGroupLoopback, identity: peer.Identity}
	l.mu.Unlock()
	return l, bound
}

func TestOwnedTerminalGenuineBoundF1EndsEvenWhenClockRefuses(t *testing.T) {
	s, clock, _ := ownedTerminalTestService(t, 1)
	d, _, _ := ownedTerminalTestService(t, 1)
	l, bound := ownedTerminalBoundF1Fixture(t, s, d)
	classify := func(findnode.SessionInfo) (findnode.SessionClass, error) { return findnode.ClassOrdinary, nil }
	if _, err := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, []findnode.Session{bound}, 0, classify); err != nil {
		t.Fatal("positive F1 fixture was not live", err)
	}
	clock.Store(-1)
	l.end()
	if !l.ended || l.evidence != nil || l.entry != nil || l.peer != nil || l.degreeWitness != nil {
		t.Fatal("owned F1 common cleanup incomplete")
	}
	if _, err := findnode.NewPrioritySnapshot(domain.PeerIdentity{99}, []findnode.Session{bound}, 0, classify); err == nil {
		t.Fatal("genuine bound F1 remained live after failed clock End")
	}
}
