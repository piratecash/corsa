package connauth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// HasIdentityFields returns true if the hello frame contains all four
// server-verifiable identity fields: Address, PubKey, BoxKey, BoxSig.
// This replaces the old requiresSessionAuth check which inspected
// frame.Client (attacker-controlled). Checking these fields eliminates
// GAP-0: the auth path is triggered by cryptographic material, not by
// a string the attacker chooses.
func HasIdentityFields(frame protocol.Frame) bool {
	return strings.TrimSpace(frame.Address) != "" &&
		strings.TrimSpace(frame.PubKey) != "" &&
		strings.TrimSpace(frame.BoxKey) != "" &&
		strings.TrimSpace(frame.BoxSig) != ""
}

// SessionAuthPayload constructs the byte payload that the peer signs
// with its Ed25519 key during the auth_session handshake.
func SessionAuthPayload(challenge, address string) []byte {
	return []byte("corsa-session-auth-v1|" + challenge + "|" + address)
}

// PrepareAuth validates the hello identity fields (box key binding)
// and generates a random challenge string for the auth_session handshake.
// On success the caller must store the returned State via AuthStore.SetConnAuthState.
func PrepareAuth(hello protocol.Frame) (*State, error) {
	if strings.TrimSpace(hello.Address) == "" ||
		strings.TrimSpace(hello.PubKey) == "" ||
		strings.TrimSpace(hello.BoxKey) == "" ||
		strings.TrimSpace(hello.BoxSig) == "" {
		return nil, fmt.Errorf("missing identity fields for authenticated session")
	}
	if err := identity.VerifyBoxKeyBinding(hello.Address, hello.PubKey, hello.BoxKey, hello.BoxSig); err != nil {
		return nil, err
	}
	challenge, err := randomChallenge()
	if err != nil {
		return nil, err
	}
	return &State{
		Hello:     hello,
		Challenge: challenge,
	}, nil
}

// VerifyAuthSession checks the auth_session frame's Ed25519 signature
// against the pending auth state. Returns a verified copy of the state
// and an auth_ok reply on success.
//
// On failure returns a nil state, an error reply frame, and false.
// The caller is responsible for ban scoring when the error code is
// ErrCodeInvalidAuthSignature.
func VerifyAuthSession(state *State, frame protocol.Frame) (*State, protocol.Frame, bool) {
	if state == nil {
		return nil, protocol.Frame{
			Type: "error",
			Code: protocol.ErrCodeAuthRequired,
		}, false
	}
	if state.Verified {
		return state, protocol.Frame{
			Type:    "auth_ok",
			Address: state.Hello.Address,
			Status:  "ok",
		}, true
	}
	if strings.TrimSpace(frame.Address) != strings.TrimSpace(state.Hello.Address) {
		return nil, protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidAuthSignature,
			Error: "authenticated address mismatch",
		}, false
	}
	if err := identity.VerifyPayload(
		state.Hello.Address,
		state.Hello.PubKey,
		SessionAuthPayload(state.Challenge, state.Hello.Address),
		frame.Signature,
	); err != nil {
		return nil, protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidAuthSignature,
			Error: err.Error(),
		}, false
	}

	verified := &State{
		Hello:    state.Hello,
		Verified: true,
	}
	return verified, protocol.Frame{
		Type:    "auth_ok",
		Address: state.Hello.Address,
		Status:  "ok",
	}, true
}

func randomChallenge() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
