package sessionv2

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// proof_test.go pins the session_proof bytes against the published vectors
// (docs/protocol/session_v2.md, "Vectors"): seed 01…20 is the listener,
// 21…40 the dialer, the exporter is 55…74, the network gazeta-devnet.

const vectorNetwork = domain.NetworkID("gazeta-devnet")

func byteRun(from byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = from + byte(i)
	}
	return out
}

func vectorKey(t *testing.T, seedFrom byte) (ed25519.PrivateKey, identity.PublicKey) {
	t.Helper()
	private := ed25519.NewKeyFromSeed(byteRun(seedFrom, ed25519.SeedSize))
	public, err := identity.ParsePublicKey(private.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatalf("vector key: %v", err)
	}
	return private, public
}

func TestProofPayloadAndSignaturesMatchTheVectors(t *testing.T) {
	exporter := byteRun(0x55, ExporterLength)
	cases := []struct {
		name    string
		seed    byte
		role    Role
		payload string
		sig     string
		frame   string
	}{
		{
			name:    "listener",
			seed:    0x01,
			role:    RoleListener,
			payload: "636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740255565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374",
			sig:     "74e805b8cf5ef320a547f1511ec1468f0694bd2a8719444db76aca4512adef006f8be1d5429b9fe913cb9d7a7e641208a9de09e3c480f19df2c8d9de831e680a",
			frame:   `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"}`,
		},
		{
			name:    "dialer",
			seed:    0x21,
			role:    RoleDialer,
			payload: "636f7273612d73657373696f6e2d76322d70726f6f6600000d67617a6574612d6465766e65740155565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f7071727374",
			sig:     "914a3f9ae1cf2b98e50fd76ece8d65eb528a0d497b94e56deb1dd12c89cb612e9d8eff853a45fde89b34ec940b1450fac3059f8b09bf7dc29dcdae556d9a200f",
			frame:   `{"type":"session_proof","signature":"kUo_muHPK5jlD9duzo1l61KKDUl7lOVt6x3RLInLYS6djv-FOkX96Js07JQLFFD6wwWfiwm_fcKdza5VbZogDw"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			private, public := vectorKey(t, tc.seed)
			payload := proofPayload(vectorNetwork, tc.role, exporter)
			if got := hex.EncodeToString(payload); got != tc.payload {
				t.Fatalf("payload\n got  %s\n want %s", got, tc.payload)
			}
			signature := ed25519.Sign(private, payload)
			if got := hex.EncodeToString(signature); got != tc.sig {
				t.Fatalf("signature\n got  %s\n want %s", got, tc.sig)
			}
			frame, err := marshalProofFrame(signature)
			if err != nil || string(frame) != tc.frame {
				t.Fatalf("frame %s (%v), want %s", frame, err, tc.frame)
			}
			parsed, err := parseProofFrame(frame)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !verifyProof(public, vectorNetwork, tc.role, exporter, parsed) {
				t.Fatal("the vector signature does not verify")
			}
		})
	}
}

// N1–N5: a proof verifies only for its own exporter, role, key and domain.
func TestProofRefusesTheNegativeVectors(t *testing.T) {
	exporter := byteRun(0x55, ExporterLength)
	listenerPrivate, listenerPublic := vectorKey(t, 0x01)
	_, dialerPublic := vectorKey(t, 0x21)
	listenerProof := ed25519.Sign(listenerPrivate, proofPayload(vectorNetwork, RoleListener, exporter))

	otherExporter := byteRun(0x56, ExporterLength)
	v1Signature := ed25519.Sign(listenerPrivate, connauth.SessionAuthPayload(base64.RawURLEncoding.EncodeToString(exporter), listenerPublic.Fingerprint()))

	cases := map[string]bool{
		"N1 another exporter":           verifyProof(listenerPublic, vectorNetwork, RoleListener, otherExporter, listenerProof),
		"N2 reflected to the other end": verifyProof(listenerPublic, vectorNetwork, RoleDialer, exporter, listenerProof),
		"N3 role byte swapped":          verifyProof(listenerPublic, vectorNetwork, RoleDialer, exporter, listenerProof),
		"N4 another key":                verifyProof(dialerPublic, vectorNetwork, RoleListener, exporter, listenerProof),
		"N5 a v1 signature as proof":    verifyProof(listenerPublic, vectorNetwork, RoleListener, exporter, v1Signature),
		"another network":               verifyProof(listenerPublic, domain.NetworkID("gazeta-mainnet"), RoleListener, exporter, listenerProof),
	}
	for name, verified := range cases {
		if verified {
			t.Errorf("%s: verified, want refused", name)
		}
	}
}

// The proof frame is parsed strictly: exactly type and signature, an
// 86-character unpadded base64url signature of 64 bytes.
func TestProofFrameIsParsedStrictly(t *testing.T) {
	good := `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"}`
	if _, err := parseProofFrame([]byte(good)); err != nil {
		t.Fatalf("good frame refused: %v", err)
	}
	bad := map[string]string{
		"extra field":      `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg","address":"x"}`,
		"other type":       `{"type":"auth_session","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"}`,
		"padded signature": `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg=="}`,
		"std alphabet":     `{"type":"session_proof","signature":"dOgFuM9e8yClR/FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi+HVQpuf6RPLnXp+ZBIIqd4J48SA8Z3yyNnegx5oCg"}`,
		"short signature":  `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5o"}`,
		"no signature":     `{"type":"session_proof"}`,
		"trailing data":    `{"type":"session_proof","signature":"dOgFuM9e8yClR_FRHsFGjwaUvSqHGURNt2rKRRKt7wBvi-HVQpuf6RPLnXp-ZBIIqd4J48SA8Z3yyNnegx5oCg"} {}`,
	}
	for name, raw := range bad {
		if _, err := parseProofFrame([]byte(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
