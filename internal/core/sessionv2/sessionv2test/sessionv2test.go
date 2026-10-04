// Package sessionv2test gives other packages' tests a proven v2 peer the only
// way one exists: by running the real handshake. sessionv2.ProvenIdentity has
// no constructor outside the handshake, and this package does not add one —
// it dials and accepts over loopback TCP and hands back what the handshake
// proved. Nothing here can forge a proof, so importing it cannot weaken the
// guarantee the type exists for.
package sessionv2test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

const network = domain.NetworkID("gazeta-devnet")

var timeouts = sessionv2.Timeouts{TLS: 5 * time.Second, Proof: 5 * time.Second}

// ProvenPeer runs a v2 handshake in which owner dials a fresh listener and
// returns the peer the listener proved — owner, with its proof. The sockets
// are closed when the test ends.
func ProvenPeer(t testing.TB, owner *identity.Identity) sessionv2.Peer {
	t.Helper()
	listenerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("sessionv2test: identity: %v", err)
	}
	dialerLocal := local(t, owner)
	listenerLocal := local(t, listenerID)
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatalf("sessionv2test: certificates: %v", err)
	}
	dialed, accepted := loopbackPair(t)

	ctx := context.Background()
	type result struct {
		session sessionv2.ProvenSession
		err     error
	}
	done := make(chan result, 1)
	go func() {
		session, err := sessionv2.Accept(ctx, accepted, listenerLocal, protocol.Frame{Version: 31}, certificates, timeouts)
		done <- result{session, err}
	}()
	if _, err := sessionv2.Dial(ctx, dialed, dialerLocal, protocol.Frame{Version: 31}, sessionv2.AnyPeer(), timeouts); err != nil {
		t.Fatalf("sessionv2test: dial: %v", err)
	}
	got := <-done
	if got.err != nil {
		t.Fatalf("sessionv2test: accept: %v", got.err)
	}
	peer, err := got.session.Peer()
	if err != nil {
		t.Fatalf("sessionv2test: peer: %v", err)
	}
	return peer
}

// NewProvenPeer is ProvenPeer for a freshly generated identity, returned
// with the peer so the test can sign as it.
func NewProvenPeer(t testing.TB) (sessionv2.Peer, *identity.Identity) {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("sessionv2test: identity: %v", err)
	}
	return ProvenPeer(t, owner), owner
}

// Proof is the proof of a peer ProvenPeer returned; it fails the test if the
// handshake somehow produced none.
func Proof(t testing.TB, peer sessionv2.Peer) sessionv2.ProvenIdentity {
	t.Helper()
	proof, ok := peer.Proof()
	if !ok {
		t.Fatal("sessionv2test: the handshake produced no proof")
	}
	return proof
}

func local(t testing.TB, id *identity.Identity) sessionv2.Local {
	t.Helper()
	l, err := sessionv2.NewLocal(id, network)
	if err != nil {
		t.Fatalf("sessionv2test: local: %v", err)
	}
	return l
}

func loopbackPair(t testing.TB) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("sessionv2test: listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn
	}()
	dialed, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("sessionv2test: dial: %v", err)
	}
	other := <-accepted
	if other == nil {
		_ = dialed.Close()
		t.Fatal("sessionv2test: accept failed")
	}
	t.Cleanup(func() { _ = dialed.Close(); _ = other.Close() })
	return dialed, other
}
