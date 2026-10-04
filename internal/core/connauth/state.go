package connauth

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// State holds the authentication state for a single inbound connection.
// Instances are immutable after creation — the pointer is swapped
// atomically via AuthStore.SetConnAuthState, never mutated in place.
// This enables snapshot-based concurrent reads without additional locking.
//
// Verified admits the connection to the authenticated command set, whichever
// way it was reached. It is NOT a proof that may be attributed to the
// identity: a v1 auth_session signs `corsa-session-auth-v1|<challenge>|<address>`,
// which names neither the verifier nor the connection, so a node the
// identity's owner dials can relay the owner's signature here. Only a v2
// session proves the identity over this connection, and only
// ProvenBySessionV2 records that proof (ProvenIdentity).
type State struct {
	Hello     protocol.Frame
	Challenge string
	Verified  bool
	// proof is the v2 handshake's own result, never a flag set beside it:
	// a sessionv2.ProvenIdentity exists only where session_proof verified.
	proof sessionv2.ProvenIdentity
}

// ProvenBySessionV2 is the verified state of a connection whose identity a
// v2 session proved; proof is that session's result for this very peer.
func ProvenBySessionV2(hello protocol.Frame, proof sessionv2.ProvenIdentity) *State {
	return &State{Hello: hello, Verified: true, proof: proof}
}

// ProvenIdentity returns the v2 proof of the hello's identity over this
// connection; ok is false for a v1 connection, for an unverified one and for
// a proof that names anybody other than the hello — a contradiction read the
// closed way.
func (s *State) ProvenIdentity() (sessionv2.ProvenIdentity, bool) {
	if s == nil || !s.Verified {
		return sessionv2.ProvenIdentity{}, false
	}
	id, ok := s.proof.Identity()
	if !ok || id != domain.PeerIdentityFromWire(s.Hello.Address) {
		return sessionv2.ProvenIdentity{}, false
	}
	return s.proof, true
}
