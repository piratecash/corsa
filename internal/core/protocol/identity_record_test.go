package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const testRecordNetwork = domain.NetworkID("gazeta-devnet")

func newTestRecordOwner(t *testing.T) *identity.Identity {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return owner
}

func ownerPeerIdentity(t *testing.T, owner *identity.Identity) domain.PeerIdentity {
	t.Helper()
	parsed, err := domain.ParsePeerIdentity(owner.Address)
	if err != nil {
		t.Fatalf("parse owner address: %v", err)
	}
	return parsed
}

func buildTestRecord(t *testing.T, owner *identity.Identity, spec IdentityRecordSpec) SignedIdentityRecord {
	t.Helper()
	record, err := BuildSignedIdentityRecord(owner, spec)
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	return record
}

func defaultRecordSpec() IdentityRecordSpec {
	return IdentityRecordSpec{
		Network:  testRecordNetwork,
		DM:       true,
		DTypes:   domain.ExplicitDTypes([]domain.DType{"get_identity", "post_identity"}),
		IssuedAt: 1780000000,
		Seq:      3,
	}
}

// TestIdentityRecordRoundtrip is the positive path: build → marshal →
// parse → verify, with every field surviving the trip.
func TestIdentityRecordRoundtrip(t *testing.T) {
	owner := newTestRecordOwner(t)
	record := buildTestRecord(t, owner, defaultRecordSpec())

	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := ParseSignedIdentityRecord(encoded)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	body, err := VerifyIdentityRecord(parsed, testRecordNetwork, ownerPeerIdentity(t, owner))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}

	if body.Address.String() != owner.Address {
		t.Errorf("address = %s, want %s", body.Address, owner.Address)
	}
	if !body.DM {
		t.Error("dm lost in roundtrip")
	}
	if string(body.PubKey) != identity.PublicKeyBase64(owner.PublicKey) {
		t.Error("pubkey lost in roundtrip")
	}
	if string(body.BoxKey) != identity.BoxPublicKeyBase64(owner.BoxPublicKey) {
		t.Error("boxkey lost in roundtrip")
	}
	if body.Seq != 3 || body.IssuedAt != 1780000000 {
		t.Errorf("seq/issued_at = %d/%d, want 3/1780000000", body.Seq, body.IssuedAt)
	}
	if body.DTypes.Declaration() != domain.DTypeDeclarationExplicit || body.DTypes.Len() != 2 {
		t.Errorf("dtypes = %v (%s)", body.DTypes.Types(), body.DTypes.Declaration())
	}
}

// TestIdentityRecordKeyless covers the dm:false branch both ways: a keyless
// record verifies without box material, and box fields present alongside
// dm:false invalidate the record.
func TestIdentityRecordKeyless(t *testing.T) {
	owner := newTestRecordOwner(t)
	spec := defaultRecordSpec()
	spec.DM = false
	record := buildTestRecord(t, owner, spec)

	body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerPeerIdentity(t, owner))
	if err != nil {
		t.Fatalf("keyless verify: %v", err)
	}
	if body.DM || body.BoxKey != "" || body.BoxSig != "" {
		t.Errorf("keyless record leaked box material: dm=%v boxkey=%q", body.DM, body.BoxKey)
	}

	// Box fields present with dm=false: signed honestly by the owner, still
	// invalid — the two statements contradict each other.
	withBox := signedBodyWithFields(t, owner, map[string]any{
		"dm":     false,
		"boxkey": identity.BoxPublicKeyBase64(owner.BoxPublicKey),
		"boxsig": identity.SignBoxKeyBinding(owner),
	})
	if _, err := VerifyIdentityRecord(withBox, testRecordNetwork, ownerPeerIdentity(t, owner)); !errors.Is(err, ErrIdentityRecordBoxFields) {
		t.Errorf("dm=false with box fields: err = %v, want ErrIdentityRecordBoxFields", err)
	}

	// dm=true with box fields missing is the mirror violation.
	noBox := signedBodyWithFields(t, owner, map[string]any{"dm": true, "boxkey": nil, "boxsig": nil})
	if _, err := VerifyIdentityRecord(noBox, testRecordNetwork, ownerPeerIdentity(t, owner)); !errors.Is(err, ErrIdentityRecordBoxFields) {
		t.Errorf("dm=true without box fields: err = %v, want ErrIdentityRecordBoxFields", err)
	}
}

// signedBodyWithFields builds a record whose body is assembled from raw
// fields (starting from a sane base) and honestly signed by the owner —
// the tool for making records that are cryptographically valid but
// structurally illegal.
func signedBodyWithFields(t *testing.T, owner *identity.Identity, overrides map[string]any) SignedIdentityRecord {
	t.Helper()
	fields := map[string]any{
		"address":   owner.Address,
		"pubkey":    identity.PublicKeyBase64(owner.PublicKey),
		"dm":        true,
		"boxkey":    identity.BoxPublicKeyBase64(owner.BoxPublicKey),
		"boxsig":    identity.SignBoxKeyBinding(owner),
		"issued_at": uint64(1780000000),
		"seq":       uint64(1),
	}
	for name, value := range overrides {
		if value == nil {
			delete(fields, name)
			continue
		}
		fields[name] = value
	}
	if dm, ok := fields["dm"].(bool); ok && !dm {
		if _, overridden := overrides["boxkey"]; !overridden {
			delete(fields, "boxkey")
		}
		if _, overridden := overrides["boxsig"]; !overridden {
			delete(fields, "boxsig")
		}
	}
	body, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal body fields: %v", err)
	}
	return signRawBody(owner, body)
}

func signRawBody(owner *identity.Identity, body []byte) SignedIdentityRecord {
	sigB64 := identity.SignPayload(owner, identityRecordSignedBytes(testRecordNetwork, body))
	sig, _ := base64.RawURLEncoding.DecodeString(sigB64)
	return SignedIdentityRecord{Version: domain.IdentityRecordVersion, Body: body, Sig: sig}
}

// TestIdentityRecordUnknownFieldsPreserved is the extension mechanism
// itself: a body with fields this build does not know verifies (the
// signature covers the bytes) and the unknown content survives verbatim in
// Body for storage and forwarding.
func TestIdentityRecordUnknownFieldsPreserved(t *testing.T) {
	owner := newTestRecordOwner(t)
	record := signedBodyWithFields(t, owner, map[string]any{
		"future_field": "opaque-value",
		"locators":     []string{"a", "b"},
	})

	body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerPeerIdentity(t, owner))
	if err != nil {
		t.Fatalf("verify with unknown fields: %v", err)
	}
	if body.Seq != 1 {
		t.Errorf("seq = %d, want 1", body.Seq)
	}
	if !strings.Contains(string(record.Body), "future_field") {
		t.Error("unknown field not preserved in body bytes")
	}

	// The envelope ignores unknown keys the same way.
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	withExtra := strings.Replace(string(encoded), `{"body":`, `{"envelope_ext":1,"body":`, 1)
	if _, err := ParseSignedIdentityRecord([]byte(withExtra)); err != nil {
		t.Errorf("envelope with unknown field rejected: %v", err)
	}
}

// TestIdentityRecordDuplicateKeysRejected covers both levels: the envelope
// and the body.
func TestIdentityRecordDuplicateKeysRejected(t *testing.T) {
	owner := newTestRecordOwner(t)
	record := buildTestRecord(t, owner, defaultRecordSpec())
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	dupEnvelope := strings.Replace(string(encoded), `{"body":`, fmt.Sprintf(`{"v":%d,"body":`, domain.IdentityRecordVersion), 1)
	if _, err := ParseSignedIdentityRecord([]byte(dupEnvelope)); !errors.Is(err, ErrIdentityRecordMalformed) {
		t.Errorf("duplicate envelope key: err = %v, want ErrIdentityRecordMalformed", err)
	}

	dupBody := signRawBody(owner, []byte(fmt.Sprintf(
		`{"address":%q,"address":%q,"pubkey":%q,"dm":false,"issued_at":1,"seq":1}`,
		owner.Address, owner.Address, identity.PublicKeyBase64(owner.PublicKey))))
	if _, err := VerifyIdentityRecord(dupBody, testRecordNetwork, ownerPeerIdentity(t, owner)); !errors.Is(err, ErrIdentityRecordMalformed) {
		t.Errorf("duplicate body key: err = %v, want ErrIdentityRecordMalformed", err)
	}
}

// TestIdentityRecordSizeCaps: the body cap is authoritative and checked
// before cryptography; the envelope cap bounds the whole object.
func TestIdentityRecordSizeCaps(t *testing.T) {
	owner := newTestRecordOwner(t)

	oversized := signedBodyWithFields(t, owner, map[string]any{
		"padding": strings.Repeat("x", domain.MaxIdentityRecordBodyBytes),
	})
	if _, err := VerifyIdentityRecord(oversized, testRecordNetwork, ownerPeerIdentity(t, owner)); !errors.Is(err, ErrIdentityRecordTooLarge) {
		t.Errorf("oversized body: err = %v, want ErrIdentityRecordTooLarge", err)
	}
	encoded, err := json.Marshal(oversized)
	if err != nil {
		t.Fatalf("marshal oversized: %v", err)
	}
	if _, err := ParseSignedIdentityRecord(encoded); !errors.Is(err, ErrIdentityRecordTooLarge) {
		t.Errorf("oversized wire object: err = %v, want ErrIdentityRecordTooLarge", err)
	}

	// The issue path refuses to produce an oversized record: eight maximal
	// dtype names blow the body budget on the owner's machine, not in the
	// network.
	names := make([]domain.DType, 0, domain.MaxIdentityRecordDTypes)
	for i := 0; i < domain.MaxIdentityRecordDTypes; i++ {
		names = append(names, domain.DType(strings.Repeat("a", domain.MaxDTypeLen-1)+fmt.Sprintf("%d", i)))
	}
	spec := defaultRecordSpec()
	spec.DTypes = domain.ExplicitDTypes(names)
	if _, err := BuildSignedIdentityRecord(owner, spec); err != nil {
		// Either outcome is lawful here: the build fits (names are ~512 B
		// total, well under the cap) or it reports the cap. What must not
		// happen is a silent truncation — so a returned record must verify.
		t.Logf("build with max dtypes: %v", err)
	} else if record := mustBuild(t, owner, spec); len(record.Body) > domain.MaxIdentityRecordBodyBytes {
		t.Errorf("issued body %d bytes exceeds cap", len(record.Body))
	}
}

func mustBuild(t *testing.T, owner *identity.Identity, spec IdentityRecordSpec) SignedIdentityRecord {
	t.Helper()
	return buildTestRecord(t, owner, spec)
}

// TestIdentityRecordVerificationOrder covers each failure the §4.1 order
// names, with the sentinel that identifies it.
func TestIdentityRecordVerificationOrder(t *testing.T) {
	owner := newTestRecordOwner(t)
	stranger := newTestRecordOwner(t)
	record := buildTestRecord(t, owner, defaultRecordSpec())
	ownerID := ownerPeerIdentity(t, owner)

	t.Run("tampered body fails signature", func(t *testing.T) {
		tampered := record
		tampered.Body = []byte(strings.Replace(string(record.Body), `"seq":3`, `"seq":4`, 1))
		if _, err := VerifyIdentityRecord(tampered, testRecordNetwork, ownerID); !errors.Is(err, ErrIdentityRecordSignature) {
			t.Errorf("err = %v, want ErrIdentityRecordSignature", err)
		}
	})

	t.Run("wrong network fails signature", func(t *testing.T) {
		if _, err := VerifyIdentityRecord(record, "other-net", ownerID); !errors.Is(err, ErrIdentityRecordSignature) {
			t.Errorf("err = %v, want ErrIdentityRecordSignature", err)
		}
	})

	t.Run("foreign signature fails", func(t *testing.T) {
		forged := record
		sigB64 := identity.SignPayload(stranger, identityRecordSignedBytes(testRecordNetwork, record.Body))
		forged.Sig, _ = base64.RawURLEncoding.DecodeString(sigB64)
		if _, err := VerifyIdentityRecord(forged, testRecordNetwork, ownerID); !errors.Is(err, ErrIdentityRecordSignature) {
			t.Errorf("err = %v, want ErrIdentityRecordSignature", err)
		}
	})

	t.Run("address not fingerprint of pubkey", func(t *testing.T) {
		// The stranger signs a body claiming the OWNER's address with the
		// stranger's pubkey: the signature verifies, the fingerprint does not.
		body := []byte(fmt.Sprintf(`{"address":%q,"pubkey":%q,"dm":false,"issued_at":1,"seq":1}`,
			owner.Address, identity.PublicKeyBase64(stranger.PublicKey)))
		sigB64 := identity.SignPayload(stranger, identityRecordSignedBytes(testRecordNetwork, body))
		sig, _ := base64.RawURLEncoding.DecodeString(sigB64)
		forged := SignedIdentityRecord{Version: domain.IdentityRecordVersion, Body: body, Sig: sig}
		if _, err := VerifyIdentityRecord(forged, testRecordNetwork, ownerID); !errors.Is(err, ErrIdentityRecordKeyMismatch) {
			t.Errorf("err = %v, want ErrIdentityRecordKeyMismatch", err)
		}
	})

	t.Run("address mismatch with expected", func(t *testing.T) {
		if _, err := VerifyIdentityRecord(record, testRecordNetwork, ownerPeerIdentity(t, stranger)); !errors.Is(err, ErrIdentityRecordAddressMismatch) {
			t.Errorf("err = %v, want ErrIdentityRecordAddressMismatch", err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		bumped := record
		bumped.Version = 2
		if _, err := VerifyIdentityRecord(bumped, testRecordNetwork, ownerID); !errors.Is(err, ErrIdentityRecordVersionUnsupported) {
			t.Errorf("err = %v, want ErrIdentityRecordVersionUnsupported", err)
		}
	})
}

// TestIdentityRecordDTypesBounds: the record-level dtypes contract — three
// wire forms plus the degrade-to-absent bounds rule.
func TestIdentityRecordDTypesBounds(t *testing.T) {
	owner := newTestRecordOwner(t)
	ownerID := ownerPeerIdentity(t, owner)

	verifyDTypes := func(t *testing.T, override any) domain.DeclaredDTypeSet {
		t.Helper()
		record := signedBodyWithFields(t, owner, map[string]any{"dtypes": override})
		body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerID)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		return body.DTypes
	}

	t.Run("absent field declares nothing", func(t *testing.T) {
		record := signedBodyWithFields(t, owner, map[string]any{"dtypes": nil})
		body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerID)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if body.DTypes.Declaration() != domain.DTypeDeclarationAbsent {
			t.Errorf("declaration = %s, want absent", body.DTypes.Declaration())
		}
	})

	t.Run("explicit empty is the empty set said out loud", func(t *testing.T) {
		set := verifyDTypes(t, []string{})
		if set.Declaration() != domain.DTypeDeclarationExplicit || set.Len() != 0 {
			t.Errorf("got %s/%d, want explicit/0", set.Declaration(), set.Len())
		}
	})

	t.Run("duplicates collapse", func(t *testing.T) {
		set := verifyDTypes(t, []string{"get_identity", "get_identity"})
		if set.Len() != 1 {
			t.Errorf("len = %d, want 1", set.Len())
		}
	})

	t.Run("too many names degrade to absent", func(t *testing.T) {
		names := make([]string, domain.MaxIdentityRecordDTypes+1)
		for i := range names {
			names[i] = fmt.Sprintf("t%d", i)
		}
		if set := verifyDTypes(t, names); set.Declaration() != domain.DTypeDeclarationAbsent {
			t.Errorf("declaration = %s, want absent", set.Declaration())
		}
	})

	t.Run("invalid name degrades to absent", func(t *testing.T) {
		if set := verifyDTypes(t, []string{"UPPER"}); set.Declaration() != domain.DTypeDeclarationAbsent {
			t.Errorf("declaration = %s, want absent", set.Declaration())
		}
	})

	t.Run("mistyped field is malformation", func(t *testing.T) {
		record := signedBodyWithFields(t, owner, map[string]any{"dtypes": 5})
		if _, err := VerifyIdentityRecord(record, testRecordNetwork, ownerID); !errors.Is(err, ErrIdentityRecordMalformed) {
			t.Errorf("err = %v, want ErrIdentityRecordMalformed", err)
		}
	})
}

// TestBuildSignedIdentityRecordRefusesSeqZero: seq 0 is "no record stored",
// never an issued record.
func TestBuildSignedIdentityRecordRefusesSeqZero(t *testing.T) {
	owner := newTestRecordOwner(t)
	spec := defaultRecordSpec()
	spec.Seq = 0
	if _, err := BuildSignedIdentityRecord(owner, spec); err == nil {
		t.Fatal("seq 0 accepted")
	}
}
