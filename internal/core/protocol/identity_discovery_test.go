package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Pinned wire vectors for the two lookup proof signatures. Ed25519 is
// deterministic, so the exact transcript AND the exact signature are both
// stable; a change in either is a wire-breaking change of the transcript
// contract, not a refactor.
const (
	vecRequesterTranscriptHex = "636f7273612d6c6f6f6b75702d7265717565737465722d763100000d67617a6574612d6465766e6574a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3000000006a18a50065b60673d6ed884bf01c2c222d82ada0740f29ac00f39d89f345eb1613bb2fa02ee883a214a6a697"
	vecRequesterSigB64        = "gbClEI1h3sAYz3iBxbhvBtxHCYHdUcBSkPaxkqAWDtO0RJYoE-se17j2AUu488TbpRMbbjMhll41gcx2xpJhCA"

	vecRequestPayload      = `{"required":["target_proof"],"v":1,"target_proof":true}`
	vecRequestPayloadSHA   = "85d64fb179d9485cf3e1d46c9aff6c3a2fbf1e4c6c0b99cfee433ad237bd86b7"
	vecTargetTranscriptHex = "636f7273612d7461726765742d70726f6f662d763100000d67617a6574612d6465766e6574a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b385d64fb179d9485cf3e1d46c9aff6c3a2fbf1e4c6c0b99cfee433ad237bd86b73449e8e5de6813b04e6cb3472e2d45c55714bdc8bef42b0ef6bb40dfa65724af"
	vecTargetProofB64      = "y94J1YdgEGVxf6gZ0NYYlsopoKXDZJqpDLAX3Y5jIkSYm4I-qVRu4ujh6wf591Eiy-NHiWI-9KRD2KsAgHlKAA"

	vecOwnerAddress = "65b60673d6ed884bf01c2c222d82ada0740f29ac"
	vecDstAddress   = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	vecIssuedAt     = uint64(1780000000)
)

// vectorOwner derives the deterministic vector identity: Ed25519 seed
// bytes 0x01…0x20, box pair derived from the seed.
func vectorOwner(t *testing.T) *identity.Identity {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	owner, err := identity.FromPrivateKeyBase64(base64.StdEncoding.EncodeToString(priv))
	if err != nil {
		t.Fatalf("vector owner: %v", err)
	}
	if owner.Address != vecOwnerAddress {
		t.Fatalf("vector owner address drifted: %s", owner.Address)
	}
	return owner
}

func vectorAttemptID() domain.PeerIdentity {
	var attemptID domain.PeerIdentity
	for i := range attemptID {
		attemptID[i] = byte(0xA0 + i)
	}
	return attemptID
}

func vectorRecord(t *testing.T, owner *identity.Identity) SignedIdentityRecord {
	t.Helper()
	record, err := BuildSignedIdentityRecord(owner, IdentityRecordSpec{
		Network:  testRecordNetwork,
		DM:       true,
		DTypes:   domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity, domain.DTypePostIdentity, domain.DTypePushIdentity}),
		IssuedAt: vecIssuedAt,
		Seq:      3,
	})
	if err != nil {
		t.Fatalf("vector record: %v", err)
	}
	return record
}

// TestIdentityLookupVectors pins the byte-exact transcripts and signatures
// of requester_sig and target_proof. These are WIRE-normative: two builds
// disagreeing here reject each other's proofs.
//
// These vectors moved once, when a fixed payload padding was added, and moved
// straight back when it was removed: padding lengths hides nothing while the
// payload itself travels as readable JSON, so a transit reads `sealed` directly
// rather than inferring it from a size. See docs/protocol/identity-lookup.md
// §6.1 for the measurement.
func TestIdentityLookupVectors(t *testing.T) {
	owner := vectorOwner(t)
	attemptID := vectorAttemptID()
	dst, _ := domain.ParsePeerIdentity(vecDstAddress)
	requester, _ := domain.ParsePeerIdentity(owner.Address)

	t.Run("requester transcript and signature", func(t *testing.T) {
		transcript := LookupRequesterTranscript(testRecordNetwork, attemptID, vecIssuedAt, requester, dst)
		if got := hex.EncodeToString(transcript); got != vecRequesterTranscriptHex {
			t.Errorf("transcript drifted:\n got %s\nwant %s", got, vecRequesterTranscriptHex)
		}
		sig := SignLookupRequester(owner, testRecordNetwork, attemptID, vecIssuedAt, requester, dst)
		if got := base64.RawURLEncoding.EncodeToString(sig); got != vecRequesterSigB64 {
			t.Errorf("signature drifted: %s", got)
		}
	})

	t.Run("request payload and target proof", func(t *testing.T) {
		request, err := BuildGetIdentityPayload(GetIdentityPayload{V: domain.IdentityLookupSchemaVersion, TargetProof: true})
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if string(request) != vecRequestPayload {
			t.Errorf("request payload drifted: %s", request)
		}
		qhash := sha256.Sum256(request)
		if got := hex.EncodeToString(qhash[:]); got != vecRequestPayloadSHA {
			t.Errorf("request hash drifted: %s", got)
		}

		record := vectorRecord(t, owner)
		transcript := TargetProofTranscript(testRecordNetwork, attemptID, qhash, record)
		if got := hex.EncodeToString(transcript); got != vecTargetTranscriptHex {
			t.Errorf("proof transcript drifted:\n got %s\nwant %s", got, vecTargetTranscriptHex)
		}
		proof := SignTargetProof(owner, testRecordNetwork, attemptID, request, record)
		if got := base64.RawURLEncoding.EncodeToString(proof); got != vecTargetProofB64 {
			t.Errorf("proof signature drifted: %s", got)
		}

		body, err := VerifyIdentityRecord(record, testRecordNetwork, requester)
		if err != nil {
			t.Fatalf("verify record: %v", err)
		}
		if err := VerifyTargetProof(proof, body, testRecordNetwork, attemptID, qhash, record); err != nil {
			t.Errorf("pinned proof does not verify: %v", err)
		}
	})
}

// TestGetIdentityPayloadRoundtrip: full request with the requester triple
// survives build → parse, and the triple verifies.
func TestGetIdentityPayloadRoundtrip(t *testing.T) {
	owner := vectorOwner(t)
	attemptID := vectorAttemptID()
	dst, _ := domain.ParsePeerIdentity(vecDstAddress)
	requester, _ := domain.ParsePeerIdentity(owner.Address)
	sig := SignLookupRequester(owner, testRecordNetwork, attemptID, vecIssuedAt, requester, dst)

	raw, err := BuildGetIdentityPayload(GetIdentityPayload{
		V:                 domain.IdentityLookupSchemaVersion,
		MinSeq:            7,
		TargetProof:       true,
		Requester:         requester,
		RequesterIssuedAt: vecIssuedAt,
		RequesterSig:      sig,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err := ParseGetIdentityPayload(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if parsed.MinSeq != 7 || !parsed.TargetProof || !parsed.RequiresTargetProof() {
		t.Errorf("fields lost: min_seq=%d target_proof=%v", parsed.MinSeq, parsed.TargetProof)
	}
	if !parsed.UnderstoodRequirements() {
		t.Error("builder emitted a requirement the parser does not understand")
	}
	if parsed.Requester != requester || parsed.RequesterIssuedAt != vecIssuedAt {
		t.Error("requester triple lost")
	}
	if err := VerifyLookupRequester(identity.PublicKeyBase64(owner.PublicKey), testRecordNetwork, attemptID, parsed, dst); err != nil {
		t.Errorf("requester triple does not verify: %v", err)
	}

	// The same triple against a different attempt id must fail — that is
	// the whole anti-replay binding.
	other := attemptID
	other[0] ^= 0xFF
	if err := VerifyLookupRequester(identity.PublicKeyBase64(owner.PublicKey), testRecordNetwork, other, parsed, dst); !errors.Is(err, ErrLookupProofInvalid) {
		t.Errorf("foreign attempt accepted: %v", err)
	}
}

// TestGetIdentityPayloadRequesterAllOrNothing: a bare requester (or any
// partial triple) drops the whole payload.
func TestGetIdentityPayloadRequesterAllOrNothing(t *testing.T) {
	cases := map[string]string{
		"bare requester":  `{"v":1,"requester":"` + vecOwnerAddress + `"}`,
		"missing sig":     `{"v":1,"requester":"` + vecOwnerAddress + `","requester_issued_at":1}`,
		"missing address": `{"v":1,"requester_issued_at":1,"requester_sig":"AAAA"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseGetIdentityPayload([]byte(raw)); !errors.Is(err, ErrLookupPayloadMalformed) {
				t.Errorf("err = %v, want ErrLookupPayloadMalformed", err)
			}
		})
	}
}

// TestGetIdentityPayloadUnknownRequirement: the endpoint must be able to
// see that it does not understand a requirement — silence is decided on
// this answer.
func TestGetIdentityPayloadUnknownRequirement(t *testing.T) {
	parsed, err := ParseGetIdentityPayload([]byte(`{"v":1,"required":["target_proof","locators"]}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.UnderstoodRequirements() {
		t.Error("unknown requirement reported as understood")
	}
	if !parsed.RequiresTargetProof() {
		t.Error("required list alone must demand the proof")
	}
}

// TestGetIdentityPayloadCaps: the 512-byte budget both ways.
func TestGetIdentityPayloadCaps(t *testing.T) {
	oversized := `{"v":1,"required":["` + strings.Repeat("a", domain.MaxGetIdentityPayloadBytes) + `"]}`
	if _, err := ParseGetIdentityPayload([]byte(oversized)); !errors.Is(err, ErrLookupPayloadTooLarge) {
		t.Errorf("parse err = %v, want ErrLookupPayloadTooLarge", err)
	}
	if _, err := BuildGetIdentityPayload(GetIdentityPayload{
		V:        domain.IdentityLookupSchemaVersion,
		Required: []string{strings.Repeat("a", domain.MaxGetIdentityPayloadBytes)},
	}); !errors.Is(err, ErrLookupPayloadTooLarge) {
		t.Errorf("build err = %v, want ErrLookupPayloadTooLarge", err)
	}
}

// TestGetIdentityPayloadUnknownVersionDropped: v=2 is a future build; the
// endpoint drops silently, it never guesses.
func TestGetIdentityPayloadUnknownVersionDropped(t *testing.T) {
	if _, err := ParseGetIdentityPayload([]byte(`{"v":2}`)); !errors.Is(err, ErrLookupVersionUnsupported) {
		t.Errorf("err = %v, want ErrLookupVersionUnsupported", err)
	}
}

// TestPostIdentityPayloadRoundtrip covers both boundary shapes the spec
// names: a maximal record WITH the proof and one without.
func TestPostIdentityPayloadRoundtrip(t *testing.T) {
	owner := vectorOwner(t)
	attemptID := vectorAttemptID()

	// A body padded to the cap exercises the answer budget end to end.
	padding := strings.Repeat("x", domain.MaxIdentityRecordBodyBytes-400)
	record := signedBodyWithFields(t, owner, map[string]any{"padding": padding})
	request, err := BuildGetIdentityPayload(GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	proof := SignTargetProof(owner, testRecordNetwork, attemptID, request, record)

	for _, tc := range []struct {
		name  string
		proof []byte
	}{
		{"with proof", proof},
		{"without proof", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := BuildPostIdentityPayload(PostIdentityPayload{V: 1, Record: record, TargetProof: tc.proof})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if len(raw) > domain.MaxPostIdentityPayloadBytes {
				t.Fatalf("payload %d bytes exceeds budget", len(raw))
			}
			parsed, err := ParsePostIdentityPayload(raw)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if string(parsed.Record.Body) != string(record.Body) {
				t.Error("record bytes not preserved")
			}
			if tc.proof != nil {
				ownerID, _ := domain.ParsePeerIdentity(owner.Address)
				body, err := VerifyIdentityRecord(parsed.Record, testRecordNetwork, ownerID)
				if err != nil {
					t.Fatalf("verify record: %v", err)
				}
				if err := VerifyTargetProof(parsed.TargetProof, body, testRecordNetwork, attemptID, sha256.Sum256(request), parsed.Record); err != nil {
					t.Errorf("proof does not verify after roundtrip: %v", err)
				}
			}
		})
	}
}

// TestPostIdentityPayloadRejects: missing record, duplicate keys, unknown
// version.
func TestPostIdentityPayloadRejects(t *testing.T) {
	if _, err := ParsePostIdentityPayload([]byte(`{"v":1}`)); !errors.Is(err, ErrLookupPayloadMalformed) {
		t.Errorf("missing record: %v", err)
	}
	if _, err := ParsePostIdentityPayload([]byte(`{"v":1,"v":1,"record":{}}`)); !errors.Is(err, ErrLookupPayloadMalformed) {
		t.Errorf("duplicate key: %v", err)
	}
	if _, err := ParsePostIdentityPayload([]byte(`{"v":9,"record":{}}`)); !errors.Is(err, ErrLookupVersionUnsupported) {
		t.Errorf("unknown version: %v", err)
	}
}

// TestVerifyTargetProofBindings: the proof must fail on a foreign attempt,
// a different request, a swapped record and a foreign signer.
func TestVerifyTargetProofBindings(t *testing.T) {
	owner := vectorOwner(t)
	stranger := newTestRecordOwner(t)
	attemptID := vectorAttemptID()
	record := vectorRecord(t, owner)
	ownerID, _ := domain.ParsePeerIdentity(owner.Address)
	body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerID)
	if err != nil {
		t.Fatalf("verify record: %v", err)
	}
	request, _ := BuildGetIdentityPayload(GetIdentityPayload{V: 1, TargetProof: true})
	proof := SignTargetProof(owner, testRecordNetwork, attemptID, request, record)
	qhash := sha256.Sum256(request)

	if err := VerifyTargetProof(proof, body, testRecordNetwork, attemptID, qhash, record); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}

	otherAttempt := attemptID
	otherAttempt[19] ^= 0x01
	if err := VerifyTargetProof(proof, body, testRecordNetwork, otherAttempt, qhash, record); !errors.Is(err, ErrLookupProofInvalid) {
		t.Error("proof accepted for a foreign attempt — replay is open")
	}

	otherRequest := sha256.Sum256([]byte(`{"v":1}`))
	if err := VerifyTargetProof(proof, body, testRecordNetwork, attemptID, otherRequest, record); !errors.Is(err, ErrLookupProofInvalid) {
		t.Error("proof accepted for a different request")
	}

	swapped := vectorRecord(t, owner)
	swapped.Body = []byte(strings.Replace(string(swapped.Body), `"seq":3`, `"seq":9`, 1))
	if err := VerifyTargetProof(proof, body, testRecordNetwork, attemptID, qhash, swapped); !errors.Is(err, ErrLookupProofInvalid) {
		t.Error("proof accepted for a swapped record")
	}

	forged := SignTargetProof(stranger, testRecordNetwork, attemptID, request, record)
	if err := VerifyTargetProof(forged, body, testRecordNetwork, attemptID, qhash, record); !errors.Is(err, ErrLookupProofInvalid) {
		t.Error("proof by a non-target accepted")
	}
}

// TestPushIdentityPayloadRoundtrip: the push payload carries the record and
// nothing else; a missing record is malformed.
func TestPushIdentityPayloadRoundtrip(t *testing.T) {
	owner := vectorOwner(t)
	record := vectorRecord(t, owner)

	raw, err := BuildPushIdentityPayload(PushIdentityPayload{V: 1, Record: record})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err := ParsePushIdentityPayload(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if string(parsed.Record.Body) != string(record.Body) {
		t.Error("record bytes not preserved")
	}

	if _, err := ParsePushIdentityPayload([]byte(`{"v":1}`)); !errors.Is(err, ErrLookupPayloadMalformed) {
		t.Errorf("missing record: %v", err)
	}
}
