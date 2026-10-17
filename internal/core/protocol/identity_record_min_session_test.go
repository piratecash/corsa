package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// A record issued with a declared min_session carries it as a JSON integer
// inside the SIGNED body, and every receiver reads it back from those bytes.
func TestIdentityRecordCarriesDeclaredMinSession(t *testing.T) {
	owner := newTestRecordOwner(t)
	spec := defaultRecordSpec()
	spec.MinSession = domain.DeclaredMinSession(domain.SessionVersionSecure)
	record := buildTestRecord(t, owner, spec)

	if !strings.Contains(string(record.Body), `"min_session":2`) {
		t.Fatalf("signed body %s does not state min_session 2", record.Body)
	}
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
	if version, declared := body.MinSession.Version(); !declared || version != domain.SessionVersionSecure {
		t.Fatalf("min_session = (%d, %v), want (2, true)", version, declared)
	}
}

// Without a declaration the field is absent from the signed bytes — not a
// zero — and the receiver reads it as absent.
func TestIdentityRecordWithoutMinSessionOmitsTheField(t *testing.T) {
	owner := newTestRecordOwner(t)
	record := buildTestRecord(t, owner, defaultRecordSpec())

	if strings.Contains(string(record.Body), "min_session") {
		t.Fatalf("signed body %s states min_session although none was declared", record.Body)
	}
	body, err := VerifyIdentityRecord(record, testRecordNetwork, ownerPeerIdentity(t, owner))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, declared := body.MinSession.Version(); declared {
		t.Fatal("an absent min_session was read as declared")
	}
}

// The field is the owner's signed word, so a value that is not a positive
// integer of the session-version range is a broken record; a version above
// the ones this build speaks is a future build's statement and is kept.
func TestIdentityRecordMinSessionValues(t *testing.T) {
	owner := newTestRecordOwner(t)
	expected := ownerPeerIdentity(t, owner)

	accepted := map[string]struct {
		value any
		want  domain.SessionVersion
	}{
		"legacy stated explicitly": {value: 1, want: domain.SessionVersionLegacy},
		"secure session":           {value: 2, want: domain.SessionVersionSecure},
		"a future version":         {value: 3, want: domain.SessionVersion(3)},
	}
	for name, tc := range accepted {
		t.Run(name, func(t *testing.T) {
			record := signedBodyWithFields(t, owner, map[string]any{"min_session": tc.value})
			body, err := VerifyIdentityRecord(record, testRecordNetwork, expected)
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			if version, declared := body.MinSession.Version(); !declared || version != tc.want {
				t.Fatalf("min_session = (%d, %v), want (%d, true)", version, declared, tc.want)
			}
		})
	}

	malformed := map[string]any{
		"zero":         0,
		"negative":     -2,
		"fraction":     2.5,
		"string":       "2",
		"boolean":      true,
		"null":         json.RawMessage("null"),
		"out of range": 256,
	}
	for name, value := range malformed {
		t.Run(name, func(t *testing.T) {
			record := signedBodyWithFields(t, owner, map[string]any{"min_session": value})
			if _, err := VerifyIdentityRecord(record, testRecordNetwork, expected); !errors.Is(err, ErrIdentityRecordMalformed) {
				t.Fatalf("min_session %v: err = %v, want ErrIdentityRecordMalformed", value, err)
			}
		})
	}
}

// The owner never signs a declaration no receiver accepts: a zero version
// would make the whole record malformed everywhere.
func TestBuildSignedIdentityRecordRefusesZeroMinSession(t *testing.T) {
	owner := newTestRecordOwner(t)
	spec := defaultRecordSpec()
	spec.MinSession = domain.DeclaredMinSession(domain.SessionVersion(0))
	if _, err := BuildSignedIdentityRecord(owner, spec); !errors.Is(err, ErrIdentityRecordMalformed) {
		t.Fatalf("err = %v, want ErrIdentityRecordMalformed", err)
	}
}
