package sessionv2

import (
	"context"
	"testing"
)

// A Peer anybody can build is a description, not a proof: only the handshake
// that verified session_proof sets the token, so a caller holding a
// ProvenIdentity holds the result of that verification and nothing less.
func TestOnlyTheHandshakeMintsAProvenIdentity(t *testing.T) {
	ctx := context.Background()
	dialer, listener := newNode(t), newNode(t)
	dc, lc := connPair(t)
	accepted := acceptAsync(ctx, lc, listener)
	dialed := <-dialAsync(ctx, dc, dialer, ExpectPeer(listener.peer))
	got := <-accepted
	if dialed.err != nil || got.err != nil {
		t.Fatalf("dial %v, accept %v", dialed.err, got.err)
	}

	for name, tc := range map[string]struct {
		session ProvenSession
		want    node
	}{
		"dialer sees the listener": {dialed.session, listener},
		"listener sees the dialer": {got.session, dialer},
	} {
		t.Run(name, func(t *testing.T) {
			peer, err := tc.session.Peer()
			if err != nil {
				t.Fatalf("peer: %v", err)
			}
			proof, ok := peer.Proof()
			if !ok {
				t.Fatal("a peer the handshake proved carries no proof")
			}
			if id, _ := proof.Identity(); id != tc.want.peer {
				t.Fatalf("proof names %s, want %s", id, tc.want.peer)
			}
		})
	}

	if _, ok := (Peer{Identity: dialer.peer}).Proof(); ok {
		t.Fatal("a Peer literal must not carry a proof")
	}
	if _, ok := (ProvenIdentity{}).Identity(); ok {
		t.Fatal("the zero ProvenIdentity must name nobody")
	}
}
