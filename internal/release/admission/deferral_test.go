package admission

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/mock"
)

func deferTor(t *testing.T, d manifestDoc) manifestDoc {
	t.Helper()
	c := d.conditionByID(t, "tor_phase_measurement")
	c["status"] = "deferred"
	c["evidence"] = map[string]any{"report": string(TorDeferralDecision), "commit": nil}
	return d
}

func TestOnlyRecordedTorDecisionCanDeferACondition(t *testing.T) {
	for _, id := range RequiredConditions() {
		t.Run(string(id), func(t *testing.T) {
			d := validManifestDoc()
			c := d.conditionByID(t, id)
			c["status"] = "deferred"
			c["evidence"] = map[string]any{"report": string(TorDeferralDecision), "commit": nil}
			_, err := DecodeManifest(bytes.NewReader(d.encode(t)))
			if id == "tor_phase_measurement" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("unapproved deferral %s: %v", id, err)
			}
		})
	}
	for _, evidence := range []any{nil, map[string]any{"report": "unrecorded decision"}, map[string]any{"report": string(TorDeferralDecision), "commit": candidateCommit}} {
		d := deferTor(t, validManifestDoc())
		d.conditionByID(t, "tor_phase_measurement")["evidence"] = evidence
		if _, err := DecodeManifest(bytes.NewReader(d.encode(t))); !errors.Is(err, ErrManifestInvalid) {
			t.Fatalf("invalid deferral evidence accepted: %v", evidence)
		}
	}
}

func TestTorDeferralPreservesEveryOtherReleaseObligation(t *testing.T) {
	// This independent list catches deleting an obligation from both the gate
	// and its manifest. Tor remains listed even when explicitly deferred.
	want := []ConditionID{"crypto_review", "tor_phase_measurement", "first_contact_latency", "compat_real_releases", "load_run_on_candidate", "probe_per_contract", "tls_first_switch", "pin_store_marker", "unsafe_pin_store_refusal", "dial_paths_unified", "loopback_inbound_class", "self_record_min_session", "owner_session_choices", "owner_release_shape", "network_observation_plan", "final_candidate_checks", "release_notes"}
	if !slices.Equal(RequiredConditions(), want) {
		t.Fatal("release obligation set changed", RequiredConditions())
	}
	for _, id := range want {
		if id == "tor_phase_measurement" {
			continue
		}
		t.Run(string(id), func(t *testing.T) {
			d := deferTor(t, validManifestDoc().markAllDone().withReleaseDecision())
			d.conditionByID(t, id)["status"] = "partial"
			v, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), nil)
			if !errors.Is(err, ErrAdmissionNotGranted) || !slices.Equal(v.Outstanding, []ConditionID{id}) || !slices.Equal(v.Deferred, []ConditionID{"tor_phase_measurement"}) {
				t.Fatalf("deferral bypassed %s: %+v, %v", id, v, err)
			}
		})
	}
	d := deferTor(t, validManifestDoc().markAllDone())
	if _, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), nil); !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatal("deferral bypassed release decision", err)
	}
	d.withReleaseDecision()
	if _, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), nil); !errors.Is(err, ErrCandidateUnverified) {
		t.Fatal("deferral bypassed candidate custody", err)
	}
	v, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), unchangedHistory(t))
	if err != nil || len(v.Outstanding) != 0 || !slices.Equal(v.Deferred, []ConditionID{"tor_phase_measurement"}) {
		t.Fatalf("otherwise completed release with deferral: %+v %v", v, err)
	}
}

func TestInvalidTypedDeferralStaysOutstanding(t *testing.T) {
	m := openManifest(t)
	for i := range m.Conditions {
		if m.Conditions[i].ID == "crypto_review" {
			m.Conditions[i].Status = ConditionDeferred
			m.Conditions[i].Evidence = &Evidence{Report: TorDeferralDecision}
		}
	}
	v, err := Evaluate(context.Background(), activeRequest(), m, nil)
	if !errors.Is(err, ErrAdmissionNotGranted) || !slices.Equal(v.Outstanding, []ConditionID{"crypto_review"}) || len(v.Deferred) != 0 {
		t.Fatalf("typed invalid deferral: %+v %v", v, err)
	}
}

func TestTorDeferralRejectsUnrecordedTypedEvidence(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*Condition)
	}{
		{"wrong-executor", func(c *Condition) { c.Executor = ExecutorOwner }},
		{"no-evidence", func(c *Condition) { c.Evidence = nil }},
		{"wrong-report", func(c *Condition) { c.Evidence.Report = "other decision" }},
		{"code-commit", func(c *Condition) { h := CommitHash(candidateCommit); c.Evidence.Commit = &h }},
	} {
		t.Run(change.name, func(t *testing.T) {
			m := decodeDoc(t, deferTor(t, validManifestDoc().markAllDone().withReleaseDecision()))
			for i := range m.Conditions {
				if m.Conditions[i].ID == "tor_phase_measurement" {
					change.apply(&m.Conditions[i])
				}
			}
			v, err := Evaluate(context.Background(), activeRequest(), m, nil)
			if !errors.Is(err, ErrAdmissionNotGranted) || !slices.Equal(v.Outstanding, []ConditionID{"tor_phase_measurement"}) || len(v.Deferred) != 0 {
				t.Fatalf("invalid typed Tor exemption: %+v %v", v, err)
			}
		})
	}
}

func TestCommittedDeferralIsUnverifiedAndCannotRelease(t *testing.T) {
	if TorDeferralDecision != "docs/operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23" {
		t.Fatal("recorded decision changed")
	}
	f, err := os.Open(committedManifestPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	m, err := DecodeManifest(f)
	if err != nil {
		t.Fatal(err)
	}
	if m.Schema != 2 || m.ReleaseDecision != nil {
		t.Fatal("unexpected schema or release approval")
	}
	for _, c := range m.Conditions {
		switch c.ID {
		case "tor_phase_measurement":
			if c.Status != ConditionDeferred || !c.ownerDeferred() {
				t.Fatal("Tor became completed or lost its recorded decision", c)
			}
		case "crypto_review":
			if c.Status != ConditionNotDone || c.Evidence != nil {
				t.Fatal("crypto review unexpectedly completed", c)
			}
		case "unsafe_pin_store_refusal":
			if c.Status != ConditionPartial {
				t.Fatal("POSIX obligation unexpectedly completed", c)
			}
		case "first_contact_latency":
			if c.Status != ConditionNotDone {
				t.Fatal("clearnet first-contact obligation unexpectedly completed", c)
			}
		}
	}
	v, err := Evaluate(context.Background(), activeRequest(), m, nil)
	if !errors.Is(err, ErrAdmissionNotGranted) || !slices.Equal(v.Deferred, []ConditionID{"tor_phase_measurement"}) || slices.Contains(v.Outstanding, "tor_phase_measurement") {
		t.Fatalf("committed deferral: %+v %v", v, err)
	}
	d := validManifestDoc()
	d["schema"] = 1
	if _, err = DecodeManifest(bytes.NewReader(d.encode(t))); !errors.Is(err, ErrManifestInvalid) {
		t.Fatal("old schema accepted", err)
	}
}

func TestTorDeferralStillRequiresCandidateAndReleaseCustody(t *testing.T) {
	m := decodeDoc(t, deferTor(t, validManifestDoc().markAllDone().withReleaseDecision()))
	t.Run("changed-candidate", func(t *testing.T) {
		_, err := Evaluate(context.Background(), activeRequest(), m, candidateTree(t, []string{"internal/core/node/service.go"}))
		if !errors.Is(err, ErrCandidateChanged) {
			t.Fatal("candidate change accepted", err)
		}
	})
	t.Run("missing-notes", func(t *testing.T) {
		h := NewMockReleaseTree(t)
		h.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return([]string(nil), nil)
		h.EXPECT().IsRegularFile(mock.Anything, candidateNotes).Return(false, nil)
		_, err := Evaluate(context.Background(), activeRequest(), m, h)
		if !errors.Is(err, ErrReleaseNotesMissing) {
			t.Fatal("missing or nonregular notes accepted", err)
		}
	})
	t.Run("wrong-version", func(t *testing.T) {
		d := deferTor(t, validManifestDoc().markAllDone().withReleaseDecision())
		d["release_decision"].(map[string]any)["version"] = "2.5.72"
		_, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), nil)
		if !errors.Is(err, ErrReleaseVersionMismatch) {
			t.Fatal("wrong release decision accepted", err)
		}
	})
}

func TestTypedTorDeferralRequiresCurrentSchema(t *testing.T) {
	m := decodeDoc(t, deferTor(t, validManifestDoc().markAllDone().withReleaseDecision()))
	for _, schema := range []ManifestSchema{1, 3} {
		m.Schema = schema
		if _, err := Evaluate(context.Background(), activeRequest(), m, nil); !errors.Is(err, ErrManifestInvalid) {
			t.Fatalf("typed schema %d accepted: %v", schema, err)
		}
	}
}
