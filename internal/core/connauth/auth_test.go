package connauth

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// testIdentityHello generates a real identity and returns a hello frame
// with all identity fields populated and a valid box key signature.
func testIdentityHello(t *testing.T) (*identity.Identity, protocol.Frame) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id, protocol.Frame{
		Type:    "hello",
		Client:  "node",
		Address: id.Address,
		PubKey:  identity.PublicKeyBase64(id.PublicKey),
		BoxKey:  identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:  identity.SignBoxKeyBinding(id),
	}
}

func TestHasIdentityFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		frame protocol.Frame
		want  bool
	}{
		{
			name:  "all fields present",
			frame: protocol.Frame{Address: "addr", PubKey: "pub", BoxKey: "box", BoxSig: "sig"},
			want:  true,
		},
		{
			name:  "missing address",
			frame: protocol.Frame{PubKey: "pub", BoxKey: "box", BoxSig: "sig"},
			want:  false,
		},
		{
			name:  "missing pubkey",
			frame: protocol.Frame{Address: "addr", BoxKey: "box", BoxSig: "sig"},
			want:  false,
		},
		{
			name:  "missing boxkey",
			frame: protocol.Frame{Address: "addr", PubKey: "pub", BoxSig: "sig"},
			want:  false,
		},
		{
			name:  "missing boxsig",
			frame: protocol.Frame{Address: "addr", PubKey: "pub", BoxKey: "box"},
			want:  false,
		},
		{
			name:  "all empty",
			frame: protocol.Frame{},
			want:  false,
		},
		{
			name:  "whitespace only",
			frame: protocol.Frame{Address: "  ", PubKey: " ", BoxKey: "  ", BoxSig: " "},
			want:  false,
		},
		{
			name:  "client mobile with identity fields",
			frame: protocol.Frame{Client: "mobile", Address: "addr", PubKey: "pub", BoxKey: "box", BoxSig: "sig"},
			want:  true,
		},
		{
			name:  "client field does not influence result",
			frame: protocol.Frame{Client: "node"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasIdentityFields(tt.frame)
			if got != tt.want {
				t.Errorf("HasIdentityFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSessionAuthPayload(t *testing.T) {
	t.Parallel()

	payload := SessionAuthPayload("challenge123", "address456")
	expected := []byte("corsa-session-auth-v1|challenge123|address456")
	if string(payload) != string(expected) {
		t.Errorf("SessionAuthPayload = %q, want %q", payload, expected)
	}
}

func TestSessionAuthPayload_Deterministic(t *testing.T) {
	t.Parallel()

	a := SessionAuthPayload("c", "a")
	b := SessionAuthPayload("c", "a")
	if string(a) != string(b) {
		t.Errorf("SessionAuthPayload is not deterministic: %q != %q", a, b)
	}
}

func TestPrepareAuth_Success(t *testing.T) {
	t.Parallel()

	_, hello := testIdentityHello(t)

	state, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth: %v", err)
	}
	if state == nil {
		t.Fatal("PrepareAuth returned nil state")
	}
	if state.Hello.Address != hello.Address {
		t.Errorf("state.Hello.Address = %q, want %q", state.Hello.Address, hello.Address)
	}
	if state.Challenge == "" {
		t.Error("state.Challenge is empty")
	}
	if state.Verified {
		t.Error("state.Verified should be false after PrepareAuth")
	}
}

func TestPrepareAuth_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		frame protocol.Frame
	}{
		{"empty frame", protocol.Frame{}},
		{"missing address", protocol.Frame{PubKey: "p", BoxKey: "b", BoxSig: "s"}},
		{"missing pubkey", protocol.Frame{Address: "a", BoxKey: "b", BoxSig: "s"}},
		{"missing boxkey", protocol.Frame{Address: "a", PubKey: "p", BoxSig: "s"}},
		{"missing boxsig", protocol.Frame{Address: "a", PubKey: "p", BoxKey: "b"}},
		{"whitespace only", protocol.Frame{Address: " ", PubKey: " ", BoxKey: " ", BoxSig: " "}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := PrepareAuth(tt.frame)
			if err == nil {
				t.Error("PrepareAuth should fail for missing fields")
			}
			if state != nil {
				t.Error("state should be nil on error")
			}
		})
	}
}

func TestPrepareAuth_InvalidBoxSig(t *testing.T) {
	t.Parallel()

	_, hello := testIdentityHello(t)
	hello.BoxSig = "invalid-signature"

	state, err := PrepareAuth(hello)
	if err == nil {
		t.Error("PrepareAuth should fail for invalid box signature")
	}
	if state != nil {
		t.Error("state should be nil on error")
	}
}

func TestPrepareAuth_UniqueChallenge(t *testing.T) {
	t.Parallel()

	_, hello := testIdentityHello(t)

	state1, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth #1: %v", err)
	}
	state2, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth #2: %v", err)
	}
	if state1.Challenge == state2.Challenge {
		t.Error("two PrepareAuth calls produced the same challenge — randomChallenge is broken")
	}
}

func TestVerifyAuthSession_NilState(t *testing.T) {
	t.Parallel()

	verified, reply, ok := VerifyAuthSession(nil, protocol.Frame{})
	if ok {
		t.Fatal("should fail for nil state")
	}
	if verified != nil {
		t.Error("verified should be nil")
	}
	if reply.Code != protocol.ErrCodeAuthRequired {
		t.Errorf("reply.Code = %q, want %q", reply.Code, protocol.ErrCodeAuthRequired)
	}
}

func TestVerifyAuthSession_AlreadyVerified(t *testing.T) {
	t.Parallel()

	state := &State{
		Hello:    protocol.Frame{Address: "addr"},
		Verified: true,
	}
	verified, reply, ok := VerifyAuthSession(state, protocol.Frame{})
	if !ok {
		t.Fatal("should succeed for already-verified state")
	}
	if verified != state {
		t.Error("should return same state pointer")
	}
	if reply.Type != "auth_ok" {
		t.Errorf("reply.Type = %q, want auth_ok", reply.Type)
	}
}

func TestVerifyAuthSession_AddressMismatch(t *testing.T) {
	t.Parallel()

	state := &State{
		Hello:     protocol.Frame{Address: "alice"},
		Challenge: "challenge",
	}
	frame := protocol.Frame{
		Type:    "auth_session",
		Address: "bob",
	}
	verified, reply, ok := VerifyAuthSession(state, frame)
	if ok {
		t.Fatal("should fail for address mismatch")
	}
	if verified != nil {
		t.Error("verified should be nil")
	}
	if reply.Code != protocol.ErrCodeInvalidAuthSignature {
		t.Errorf("reply.Code = %q, want %q", reply.Code, protocol.ErrCodeInvalidAuthSignature)
	}
}

func TestVerifyAuthSession_InvalidSignature(t *testing.T) {
	t.Parallel()

	id, hello := testIdentityHello(t)
	_ = id

	state := &State{
		Hello:     hello,
		Challenge: "test-challenge",
	}
	frame := protocol.Frame{
		Type:      "auth_session",
		Address:   hello.Address,
		Signature: "invalid-signature",
	}
	verified, reply, ok := VerifyAuthSession(state, frame)
	if ok {
		t.Fatal("should fail for invalid signature")
	}
	if verified != nil {
		t.Error("verified should be nil")
	}
	if reply.Code != protocol.ErrCodeInvalidAuthSignature {
		t.Errorf("reply.Code = %q, want %q", reply.Code, protocol.ErrCodeInvalidAuthSignature)
	}
}

func TestVerifyAuthSession_FullHandshake(t *testing.T) {
	t.Parallel()

	id, hello := testIdentityHello(t)

	// Step 1: PrepareAuth
	state, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth: %v", err)
	}

	// Step 2: Sign the challenge (simulating client)
	signature := identity.SignPayload(id, SessionAuthPayload(state.Challenge, id.Address))

	// Step 3: VerifyAuthSession
	frame := protocol.Frame{
		Type:      "auth_session",
		Address:   id.Address,
		Signature: signature,
	}
	verified, reply, ok := VerifyAuthSession(state, frame)
	if !ok {
		t.Fatalf("VerifyAuthSession failed: %+v", reply)
	}
	if verified == nil {
		t.Fatal("verified state is nil")
	}
	if !verified.Verified {
		t.Error("verified.Verified should be true")
	}
	if verified.Hello.Address != id.Address {
		t.Errorf("verified.Hello.Address = %q, want %q", verified.Hello.Address, id.Address)
	}
	if reply.Type != "auth_ok" {
		t.Errorf("reply.Type = %q, want auth_ok", reply.Type)
	}

	// Immutability: original state must NOT be mutated.
	if state.Verified {
		t.Error("original state.Verified was mutated — immutability violated")
	}
}

func TestVerifyAuthSession_WrongKey(t *testing.T) {
	t.Parallel()

	_, hello := testIdentityHello(t)

	state, err := PrepareAuth(hello)
	if err != nil {
		t.Fatalf("PrepareAuth: %v", err)
	}

	// Sign with a DIFFERENT identity (attacker scenario).
	attacker, err := identity.Generate()
	if err != nil {
		t.Fatalf("attacker identity: %v", err)
	}
	signature := identity.SignPayload(attacker, SessionAuthPayload(state.Challenge, hello.Address))

	frame := protocol.Frame{
		Type:      "auth_session",
		Address:   hello.Address,
		Signature: signature,
	}
	verified, reply, ok := VerifyAuthSession(state, frame)
	if ok {
		t.Fatal("should fail when signed by wrong key")
	}
	if verified != nil {
		t.Error("verified should be nil")
	}
	if reply.Code != protocol.ErrCodeInvalidAuthSignature {
		t.Errorf("reply.Code = %q, want %q", reply.Code, protocol.ErrCodeInvalidAuthSignature)
	}
}
