package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
)

func interimRequest() Request {
	version := domain.ReleaseVersion{Major: 2, Minor: 5, Build: 71}
	return Request{Build: activeBuild, Tag: &version}
}

func interimApprovalFor(m Manifest) InterimApproval {
	return InterimApproval{
		Schema: 1, Version: "2.5.71", SourceSHA256: strings.Repeat("a", 64),
		Decision:              InterimDecision{Date: "2026-10-30", Authority: "test-only explicit interim publication decision; S6 remains incomplete"},
		OutstandingConditions: append([]ConditionID{}, outstandingConditions(m.Conditions)...),
		DeferredConditions:    append([]ConditionID{}, deferredConditions(m.Conditions)...),
	}
}

func interimJSON(t *testing.T, a InterimApproval) []byte {
	t.Helper()
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestInterimApprovalStrictSchemaAndAttribution(t *testing.T) {
	m := decodeDoc(t, deferTor(t, validManifestDoc()))
	a := interimApprovalFor(m)
	raw := string(interimJSON(t, a))
	if !strings.Contains(raw, `"source_sha":"`+a.SourceSHA256+`"`) || strings.Contains(raw, `"source_sha256"`) {
		t.Fatal("approval did not encode the exact lowercase-letter/underscore source_sha key")
	}
	got, err := DecodeInterimApproval(strings.NewReader(raw + " \n\t"))
	if err != nil || !reflect.DeepEqual(got, &a) {
		t.Fatalf("valid approval: %+v, %v", got, err)
	}
	change := func(from, to string) string {
		t.Helper()
		if !strings.Contains(raw, from) {
			t.Fatalf("fixture missing %q", from)
		}
		return strings.Replace(raw, from, to, 1)
	}
	for _, test := range []struct{ name, input string }{
		{"wrong-schema", change(`"schema":1`, `"schema":2`)},
		{"null-schema", change(`"schema":1`, `"schema":null`)},
		{"wrong-version", change(`"version":"2.5.71"`, `"version":"2.5.72"`)},
		{"abbreviated-digest", change(a.SourceSHA256, "abcd")},
		{"uppercase-digest", change(a.SourceSHA256, strings.Repeat("A", 64))},
		{"zero-digest", change(a.SourceSHA256, strings.Repeat("0", 64))},
		{"empty-authority", change(a.Decision.Authority, " ")},
		{"null-decision", change(`"decision":{"date":"2026-10-30","authority":"`+a.Decision.Authority+`"}`, `"decision":null`)},
		{"bad-date", change("2026-10-30", "2026-02-30")},
		{"noncanonical-date", change("2026-10-30", "2026-1-3")},
		{"long-authority", change(a.Decision.Authority, strings.Repeat("x", 4097))},
		{"null-outstanding", change(`"outstanding_conditions":`+string(mustInterimJSON(t, a.OutstandingConditions)), `"outstanding_conditions":null`)},
		{"missing-deferred", change(`,"deferred_conditions":["tor_phase_measurement"]`, "")},
		{"null-deferred", change(`"deferred_conditions":["tor_phase_measurement"]`, `"deferred_conditions":null`)},
		{"unknown-top-field", strings.TrimSuffix(raw, "}") + `,"approved":true}`},
		{"unknown-decision-field", change(`"date":"2026-10-30"`, `"date":"2026-10-30","number":1`)},
		{"duplicate-schema", change(`"schema":1`, `"schema":1,"schema":1`)},
		{"duplicate-authority", change(`"date":"2026-10-30"`, `"date":"2026-10-30","authority":"earlier decision"`)},
		{"case-folded-key", change(`"schema":1`, `"Schema":1`)},
		{"wrong-array-type", change(`"deferred_conditions":["tor_phase_measurement"]`, `"deferred_conditions":"tor_phase_measurement"`)},
		{"unknown-id", change(`"crypto_review"`, `"made_up_gate"`)},
		{"duplicate-id", change(`"crypto_review"`, `"crypto_review","crypto_review"`)},
		{"unrecorded-deferral", change(`"deferred_conditions":["tor_phase_measurement"]`, `"deferred_conditions":["crypto_review"]`)},
		{"lone-surrogate-attribution", change(a.Decision.Authority, `\ud800`)},
		{"nul-attribution", change(a.Decision.Authority, `\u0000`)},
		{"raw-invalid-utf8", change(a.Decision.Authority, string([]byte{0xff}))},
		{"trailing-document", raw + `{}`},
		{"oversize", raw + strings.Repeat(" ", interimApprovalLimit)},
		{"top-null", "null"},
		{"truncated", raw[:len(raw)-1]},
	} {
		t.Run(test.name, func(t *testing.T) {
			if approval, err := DecodeInterimApproval(strings.NewReader(test.input)); !errors.Is(err, ErrInterimApprovalInvalid) || approval != nil {
				t.Fatalf("invalid approval accepted: %+v, %v", approval, err)
			}
		})
	}
	if a, err := DecodeInterimApproval(io.MultiReader(strings.NewReader(raw), interimErrorReader{})); !errors.Is(err, ErrInterimApprovalInvalid) || a != nil {
		t.Fatalf("reader failure accepted: %+v %v", a, err)
	}
}

type interimErrorReader struct{}

func (interimErrorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func mustInterimJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestInterimEligibilityIsOnlyExplicitV2571BuildAndTag(t *testing.T) {
	if !InterimEligible(interimRequest()) {
		t.Fatal("exact interim build/tag refused")
	}
	for _, test := range []struct {
		name   string
		change func(*Request)
	}{
		{"no-tag", func(r *Request) { r.Tag = nil }},
		{"old-tag", func(r *Request) { r.Tag.Build = 70 }},
		{"future-tag", func(r *Request) { r.Tag.Build = 72 }},
		{"old-build", func(r *Request) { r.Build.Release.Build = 70 }},
		{"future-build-and-tag", func(r *Request) { r.Build.Release.Build = 72; r.Tag.Build = 72 }},
		{"protocol30", func(r *Request) { r.Build.Protocol = 30 }},
		{"protocol32", func(r *Request) { r.Build.Protocol = 32 }},
		{"minimum26", func(r *Request) { r.Build.Minimum = 26 }},
		{"minimum29", func(r *Request) { r.Build.Minimum = 29 }},
		{"activation30", func(r *Request) { r.Build.Activation = 30 }},
		{"activation32", func(r *Request) { r.Build.Activation = 32 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := interimRequest()
			test.change(&r)
			if InterimEligible(r) {
				t.Fatal("ineligible build/tag accepted", r)
			}
			m := decodeDoc(t, deferTor(t, validManifestDoc()))
			a := interimApprovalFor(m)
			if _, err := EvaluateInterim(m, r, a, a.SourceSHA256); !errors.Is(err, ErrInterimNotEligible) {
				t.Fatalf("pure evaluator did not enforce eligibility: %v", err)
			}
		})
	}
}

func TestInterimPreservesCommittedS6StateAndNormalRefusal(t *testing.T) {
	raw, err := os.ReadFile(committedManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeManifest(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	before := mustInterimJSON(t, m)
	a := interimApprovalFor(m)
	wantApproval := interimJSON(t, a)
	v, err := EvaluateInterim(m, interimRequest(), a, a.SourceSHA256)
	if err != nil || !v.SessionV2Active || v.ReleaseDecision != nil || len(v.Outstanding) == 0 ||
		!slices.Equal(v.Outstanding, a.OutstandingConditions) || !slices.Equal(v.Deferred, a.DeferredConditions) {
		t.Fatalf("interim corrupted the S6 snapshot: %+v %v", v, err)
	}
	if _, err := Evaluate(context.Background(), interimRequest(), m, nil); !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("normal S6 gate no longer refuses: %v", err)
	}
	// Returned reporting slices do not grant mutation of the manifest or approval.
	v.Outstanding[0] = "changed-only-in-returned-report"
	if !bytes.Equal(before, mustInterimJSON(t, m)) || !bytes.Equal(wantApproval, interimJSON(t, a)) {
		t.Fatal("interim evaluation mutated its inputs")
	}
	if InterimApprovalPath != "packaging/interim-release-v2.5.71.json" {
		t.Fatal("interim approval became an arbitrary path")
	}
}

func TestInterimRejectsInvalidManifestAndChangedSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Manifest, *InterimApproval, *string)
		want   error
	}{
		{"different-digest", func(_ *Manifest, _ *InterimApproval, d *string) { *d = strings.Repeat("b", 64) }, ErrInterimSourceMismatch},
		{"zero-observed-digest", func(_ *Manifest, _ *InterimApproval, d *string) { *d = strings.Repeat("0", 64) }, ErrInterimSourceMismatch},
		{"malformed-observed-digest", func(_ *Manifest, _ *InterimApproval, d *string) { *d = "not a digest" }, ErrInterimSourceMismatch},
		{"missing-outstanding", func(_ *Manifest, a *InterimApproval, _ *string) {
			a.OutstandingConditions = a.OutstandingConditions[1:]
		}, ErrInterimApprovalInvalid},
		{"reordered-outstanding", func(_ *Manifest, a *InterimApproval, _ *string) {
			a.OutstandingConditions[0], a.OutstandingConditions[1] = a.OutstandingConditions[1], a.OutstandingConditions[0]
		}, ErrInterimApprovalInvalid},
		{"omitted-tor-deferral", func(_ *Manifest, a *InterimApproval, _ *string) { a.DeferredConditions = []ConditionID{} }, ErrInterimApprovalInvalid},
		{"duplicated-outstanding", func(_ *Manifest, a *InterimApproval, _ *string) {
			a.OutstandingConditions[1] = a.OutstandingConditions[0]
		}, ErrInterimApprovalInvalid},
		{"nil-outstanding", func(_ *Manifest, a *InterimApproval, _ *string) { a.OutstandingConditions = nil }, ErrInterimApprovalInvalid},
		{"typed-wrong-version", func(_ *Manifest, a *InterimApproval, _ *string) { a.Version = "2.5.72" }, ErrInterimApprovalInvalid},
		{"typed-empty-decision", func(_ *Manifest, a *InterimApproval, _ *string) { a.Decision = InterimDecision{} }, ErrInterimApprovalInvalid},
		{"invalid-manifest-schema", func(m *Manifest, _ *InterimApproval, _ *string) { m.Schema = 99 }, ErrManifestInvalid},
		{"invalid-manifest-status", func(m *Manifest, _ *InterimApproval, _ *string) { m.Conditions[0].Status = "complete" }, ErrManifestInvalid},
		{"invalid-manifest-condition-set", func(m *Manifest, _ *InterimApproval, _ *string) { m.Conditions = m.Conditions[1:] }, ErrManifestInvalid},
		{"forged-crypto-deferral", func(m *Manifest, _ *InterimApproval, _ *string) {
			m.Conditions[0].Status = ConditionDeferred
			m.Conditions[0].Evidence = &Evidence{Report: TorDeferralDecision}
		}, ErrManifestInvalid},
		{"different-activation", func(m *Manifest, _ *InterimApproval, _ *string) { m.ActivationProtocolVersion = 32 }, ErrInterimNotEligible},
		{"stale-obligation-snapshot", func(m *Manifest, _ *InterimApproval, _ *string) {
			hash := CommitHash(candidateCommit)
			m.Conditions[0].Status = ConditionDone
			m.Conditions[0].Evidence = &Evidence{Report: "actual changed evidence", Commit: &hash}
		}, ErrInterimApprovalInvalid},
		{"normal-decision-is-not-interim", func(m *Manifest, _ *InterimApproval, _ *string) {
			m.ReleaseDecision = &OwnerDecision{Number: 1, Date: "2026-10-30", Version: "2.5.71"}
		}, ErrInterimApprovalInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := decodeDoc(t, deferTor(t, validManifestDoc()))
			a := interimApprovalFor(m)
			digest := a.SourceSHA256
			test.change(&m, &a, &digest)
			if _, err := EvaluateInterim(m, interimRequest(), a, digest); !errors.Is(err, test.want) {
				t.Fatalf("want %v, got %v", test.want, err)
			}
		})
	}
}

func TestFutureReleaseStillUsesNormalAdmission(t *testing.T) {
	r := interimRequest()
	r.Build.Release.Build = 72
	r.Tag.Build = 72
	d := validManifestDoc().markAllDone().withReleaseDecision()
	d["release_decision"].(map[string]any)["version"] = "2.5.72"
	m := decodeDoc(t, d)
	a := interimApprovalFor(m)
	if _, err := EvaluateInterim(m, r, a, a.SourceSHA256); !errors.Is(err, ErrInterimNotEligible) {
		t.Fatalf("old profile admitted future version: %v", err)
	}
	noDecision := decodeDoc(t, validManifestDoc().markAllDone())
	if _, err := Evaluate(context.Background(), r, noDecision, nil); !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("future normal decision requirement lost: %v", err)
	}
	tree := NewMockReleaseTree(t)
	tree.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	tree.EXPECT().IsRegularFile(mock.Anything, "docs/release-notes/RELEASE-v2.5.72.md").Return(true, nil).Once()
	if verdict, err := Evaluate(context.Background(), r, m, tree); err != nil || verdict.ReleaseDecision == nil {
		t.Fatalf("fully admitted future normal release changed: %+v %v", verdict, err)
	}
}
