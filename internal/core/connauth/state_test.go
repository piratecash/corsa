package connauth

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// A verified auth_session admits the connection but proves nothing that may
// be attributed to the identity: the signature names neither verifier nor
// connection and can be relayed (N1, session-security-v2 С-9).
func TestVerifyAuthSessionProvesNoIdentity(t *testing.T) {
	t.Parallel()
	id, hello := testIdentityHello(t)
	state, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth: %v", err)
	}
	verified, reply, ok := VerifyAuthSession(state, protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: identity.SignPayload(id, protocol.SessionAuthPayload(state.Challenge, id.Address)),
	})
	if !ok {
		t.Fatalf("VerifyAuthSession failed: %+v", reply)
	}
	if !verified.Verified {
		t.Fatal("a valid auth_session must still admit the connection")
	}
	if _, proven := verified.ProvenIdentity(); proven {
		t.Fatal("a relayable auth_session must not read as a v2 proof")
	}
}

func TestProvenIdentityComesOnlyFromAV2Proof(t *testing.T) {
	t.Parallel()
	peer, owner := sessionv2test.NewProvenPeer(t)
	proof := sessionv2test.Proof(t, peer)
	other, _ := sessionv2test.NewProvenPeer(t)
	hello := protocol.Frame{Address: owner.Address}

	cases := []struct {
		name  string
		state *State
		want  bool
	}{
		{"no state", nil, false},
		{"challenge issued, nothing verified", &State{Hello: hello, Challenge: "c"}, false},
		{"verified by a v1 signature", &State{Hello: hello, Verified: true}, false},
		{"a zero proof", ProvenBySessionV2(hello, sessionv2.ProvenIdentity{}), false},
		{"a proof of somebody else's identity", ProvenBySessionV2(hello, sessionv2test.Proof(t, other)), false},
		{"the v2 proof of the hello's identity", ProvenBySessionV2(hello, proof), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.state.ProvenIdentity()
			if ok != tc.want {
				t.Fatalf("ProvenIdentity() ok = %v, want %v", ok, tc.want)
			}
			if ok && got != proof {
				t.Fatal("ProvenIdentity() returned a different proof")
			}
		})
	}
}
