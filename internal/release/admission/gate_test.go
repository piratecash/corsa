package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// repoRoot is the module root as seen from this package's directory, where
// `go test` runs; the committed manifest and workflows are read from there.
const repoRoot = "../../.."

const candidateCommit = "0123456789abcdef0123456789abcdef01234567"

const candidateNotes = "docs/release-notes/RELEASE-v2.5.71.md"

func committedManifestPath() string {
	return filepath.Join(repoRoot, filepath.FromSlash(ManifestPath))
}

// activeBuild is a build that runs the secure session in transition mode,
// the shape of the current dev build.
var activeBuild = BuildVersions{
	Protocol:   31,
	Minimum:    28,
	Activation: 31,
	Release:    domain.ReleaseVersion{Major: 2, Minor: 5, Build: 71},
}

func activeRequest() Request {
	return Request{Build: activeBuild}
}

// candidateTree is a tree that differs from any commit in changed and has
// the release notes of the build version in place. The gate may consult it
// or not: a refusal is reached before history, so neither call is required.
func candidateTree(t *testing.T, changed []string) *MockReleaseTree {
	t.Helper()
	tree := NewMockReleaseTree(t)
	tree.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return(changed, nil).Maybe()
	tree.EXPECT().IsRegularFile(mock.Anything, candidateNotes).Return(true, nil).Maybe()
	tree.EXPECT().IsRegularFile(mock.Anything, mock.Anything).Return(false, nil).Maybe()
	return tree
}

// unchangedHistory is a tree identical to the candidate commit, with the
// release notes of the build version in place.
func unchangedHistory(t *testing.T) *MockReleaseTree {
	t.Helper()
	return candidateTree(t, nil)
}

// manifestDoc is a manifest as a generic JSON document, so a test can break
// its shape in ways the typed struct could not express.
type manifestDoc map[string]any

func validManifestDoc() manifestDoc {
	conditions := make([]any, 0, len(RequiredConditions()))
	for _, id := range RequiredConditions() {
		conditions = append(conditions, map[string]any{
			"id":        string(id),
			"condition": "condition " + string(id),
			"executor":  "developer",
			"check":     "check " + string(id),
			"evidence":  nil,
			"status":    "not_done",
		})
	}
	return manifestDoc{
		"schema":                      1,
		"activation_protocol_version": 31,
		"release_decision":            nil,
		"conditions":                  conditions,
	}
}

func (d manifestDoc) condition(t *testing.T, i int) map[string]any {
	t.Helper()
	return d["conditions"].([]any)[i].(map[string]any)
}

func (d manifestDoc) conditionByID(t *testing.T, id ConditionID) map[string]any {
	t.Helper()
	for _, c := range d["conditions"].([]any) {
		if c.(map[string]any)["id"] == string(id) {
			return c.(map[string]any)
		}
	}
	t.Fatalf("fixture has no condition %q", id)
	return nil
}

func (d manifestDoc) markAllDone() manifestDoc {
	for _, c := range d["conditions"].([]any) {
		c.(map[string]any)["status"] = "done"
		c.(map[string]any)["evidence"] = map[string]any{"report": "report of the run", "commit": candidateCommit}
	}
	return d
}

func (d manifestDoc) withReleaseDecision() manifestDoc {
	d["release_decision"] = map[string]any{"number": 70, "date": "2026-11-02", "version": "2.5.71"}
	return d
}

func (d manifestDoc) encode(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("encode manifest fixture: %v", err)
	}
	return raw
}

func decodeDoc(t *testing.T, d manifestDoc) Manifest {
	t.Helper()
	m, err := DecodeManifest(bytes.NewReader(d.encode(t)))
	if err != nil {
		t.Fatalf("DecodeManifest of a valid fixture: %v", err)
	}
	return m
}

func openManifest(t *testing.T) Manifest {
	t.Helper()
	return decodeDoc(t, validManifestDoc().markAllDone().withReleaseDecision())
}

// TestCommittedManifestKeepsReleaseClosed pins the state the repository is
// in today: the secure session is active in the build and no condition of
// its release has been met, so the gate refuses. Opening the gate therefore
// takes a reviewed change to the manifest AND to this test, never an edit
// of one file that slips through.
func TestCommittedManifestKeepsReleaseClosed(t *testing.T) {
	admission, err := Admit(context.Background(), committedManifestPath(), Request{Build: CurrentBuild()}, nil)
	if !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("Admit(committed manifest, current build) error = %v, want ErrAdmissionNotGranted", err)
	}
	if !admission.SessionV2Active {
		t.Fatal("the current build is expected to run the secure session; the gate must be live for it")
	}
	if admission.ReleaseDecision != nil {
		t.Fatalf("committed manifest names release decision %+v; none has been taken", *admission.ReleaseDecision)
	}
	// The conditions met today: the owner's choice of release order, and the
	// checks confirmed on the commit their evidence names. Closing another one
	// is a deliberate edit here as well as in the manifest.
	doneToday := []ConditionID{
		"owner_release_shape", "compat_real_releases", "probe_per_contract", "tls_first_switch",
		"pin_store_marker", "dial_paths_unified", "self_record_min_session",
	}
	var want []ConditionID
	for _, id := range RequiredConditions() {
		if !slices.Contains(doneToday, id) {
			want = append(want, id)
		}
	}
	if !slices.Equal(admission.Outstanding, want) {
		t.Fatalf("outstanding = %v, want %v", admission.Outstanding, want)
	}
}

// TestRequiredConditionsCoverTheOwnerDecisions: each owner decision that
// names a release precondition has its own condition, so none of them can
// be met by a line inside another.
func TestRequiredConditionsCoverTheOwnerDecisions(t *testing.T) {
	for _, id := range []ConditionID{
		"first_contact_latency",
		"pin_store_marker",
		"dial_paths_unified",
		"loopback_inbound_class",
		"unsafe_pin_store_refusal",
		"network_observation_plan",
		"owner_release_shape",
	} {
		if !slices.Contains(RequiredConditions(), id) {
			t.Errorf("required conditions %v lack %q", RequiredConditions(), id)
		}
	}
}

// TestCurrentBuildIsReadFromTheVersionConstants guards the input of the
// gate: it must be the constants the node is built with, not a copy that
// could drift from them.
func TestCurrentBuildIsReadFromTheVersionConstants(t *testing.T) {
	release, err := domain.ParseReleaseVersion(config.CorsaVersion)
	if err != nil {
		t.Fatalf("parse config.CorsaVersion: %v", err)
	}
	got := CurrentBuild()
	want := BuildVersions{
		Protocol:   domain.ProtocolVersion(config.ProtocolVersion),
		Minimum:    domain.ProtocolVersion(config.MinimumProtocolVersion),
		Activation: domain.ProtocolVersion(config.ProtocolVersionSecureSession),
		Release:    release,
	}
	if got != want {
		t.Fatalf("CurrentBuild() = %+v, want %+v", got, want)
	}
}

func TestGateOpensOnlyWithEveryConditionDoneAndAReleaseDecision(t *testing.T) {
	cases := []struct {
		name string
		doc  func(t *testing.T) manifestDoc
		open bool
	}{
		{
			name: "all conditions done and a release decision",
			doc:  func(*testing.T) manifestDoc { return validManifestDoc().markAllDone().withReleaseDecision() },
			open: true,
		},
		{
			name: "all conditions done without a release decision",
			doc:  func(*testing.T) manifestDoc { return validManifestDoc().markAllDone() },
		},
		{
			name: "a release decision with nothing done",
			doc:  func(*testing.T) manifestDoc { return validManifestDoc().withReleaseDecision() },
		},
		{
			name: "one condition not done",
			doc: func(t *testing.T) manifestDoc {
				d := validManifestDoc().markAllDone().withReleaseDecision()
				d.condition(t, 3)["status"] = "not_done"
				return d
			},
		},
		{
			name: "one condition partial",
			doc: func(t *testing.T) manifestDoc {
				d := validManifestDoc().markAllDone().withReleaseDecision()
				d.condition(t, 0)["status"] = "partial"
				return d
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admission, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, tc.doc(t)), unchangedHistory(t))
			if tc.open {
				if err != nil {
					t.Fatalf("Evaluate error = %v, want an open gate", err)
				}
				if len(admission.Outstanding) != 0 {
					t.Fatalf("open gate reports outstanding conditions %v", admission.Outstanding)
				}
				return
			}
			if !errors.Is(err, ErrAdmissionNotGranted) {
				t.Fatalf("Evaluate error = %v, want ErrAdmissionNotGranted", err)
			}
		})
	}
}

func TestRefusalNamesWhatIsOutstanding(t *testing.T) {
	d := validManifestDoc().markAllDone()
	d.condition(t, 1)["status"] = "partial"
	admission, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), unchangedHistory(t))
	if !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("Evaluate error = %v, want ErrAdmissionNotGranted", err)
	}
	want := []ConditionID{RequiredConditions()[1]}
	if !slices.Equal(admission.Outstanding, want) {
		t.Fatalf("outstanding = %v, want %v", admission.Outstanding, want)
	}
	if admission.ReleaseDecision != nil {
		t.Fatal("no release decision in the manifest, but the admission reports one")
	}
}

// TestBuildBelowTheRecordedActivationIsRefused: the manifest records that
// the secure session is activated; a build that rolls the activation back
// (protocol below it) is not a way to publish around the conditions.
func TestBuildBelowTheRecordedActivationIsRefused(t *testing.T) {
	rolledBack := activeBuild
	rolledBack.Protocol = 30
	_, err := Evaluate(context.Background(), Request{Build: rolledBack}, openManifest(t), unchangedHistory(t))
	if !errors.Is(err, ErrActivationRollback) {
		t.Fatalf("Evaluate(protocol 30, manifest activating at 31) error = %v, want ErrActivationRollback", err)
	}
}

func TestV2OnlyBuildRequiresTheConditions(t *testing.T) {
	v2Only := activeBuild
	v2Only.Minimum = 31
	_, err := Evaluate(context.Background(), Request{Build: v2Only}, decodeDoc(t, validManifestDoc()), unchangedHistory(t))
	if !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("Evaluate(v2-only build) error = %v, want ErrAdmissionNotGranted", err)
	}
}

func TestInconsistentBuildVersionsAreRefused(t *testing.T) {
	broken := activeBuild
	broken.Minimum = 32
	_, err := Evaluate(context.Background(), Request{Build: broken}, openManifest(t), unchangedHistory(t))
	if !errors.Is(err, ErrBuildVersions) {
		t.Fatalf("Evaluate(minimum above protocol) error = %v, want ErrBuildVersions", err)
	}
}

// TestManifestForAnotherActivationIsRefused keeps a granted admission from
// carrying over to a different activation of the secure session.
func TestManifestForAnotherActivationIsRefused(t *testing.T) {
	d := validManifestDoc().markAllDone().withReleaseDecision()
	d["activation_protocol_version"] = 32
	_, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), unchangedHistory(t))
	if !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("Evaluate(manifest for activation 32 on a build activating at 31) error = %v, want ErrManifestInvalid", err)
	}
}

// TestReleaseDecisionCoversOneVersion: the owner decides to release a
// version, not "whatever is built next".
func TestReleaseDecisionCoversOneVersion(t *testing.T) {
	d := validManifestDoc().markAllDone().withReleaseDecision()
	d["release_decision"].(map[string]any)["version"] = "2.5.72"
	_, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, d), unchangedHistory(t))
	if !errors.Is(err, ErrReleaseVersionMismatch) {
		t.Fatalf("Evaluate(decision for 2.5.72, build 2.5.71) error = %v, want ErrReleaseVersionMismatch", err)
	}
}

func TestTagMustNameTheBuildVersion(t *testing.T) {
	matching := domain.ReleaseVersion{Major: 2, Minor: 5, Build: 71}
	if _, err := Evaluate(context.Background(), Request{Build: activeBuild, Tag: &matching}, openManifest(t), unchangedHistory(t)); err != nil {
		t.Fatalf("Evaluate(tag v2.5.71, build 2.5.71) error = %v, want an open gate", err)
	}
	other := domain.ReleaseVersion{Major: 2, Minor: 5, Build: 72}
	_, err := Evaluate(context.Background(), Request{Build: activeBuild, Tag: &other}, openManifest(t), unchangedHistory(t))
	if !errors.Is(err, ErrReleaseVersionMismatch) {
		t.Fatalf("Evaluate(tag v2.5.72, build 2.5.71) error = %v, want ErrReleaseVersionMismatch", err)
	}
	_, err = Evaluate(context.Background(), Request{Build: activeBuild, Tag: &other}, decodeDoc(t, validManifestDoc()), unchangedHistory(t))
	if !errors.Is(err, ErrReleaseVersionMismatch) {
		t.Fatalf("Evaluate(tag v2.5.72, closed manifest) error = %v, want ErrReleaseVersionMismatch", err)
	}
}

// TestFinalCandidateMayDifferOnlyInReleaseInputs: the checks were run on
// the candidate commit; after them only the manifest, the release notes and
// the test pinning the manifest may change, or the checks are stale.
func TestFinalCandidateMayDifferOnlyInReleaseInputs(t *testing.T) {
	releaseInputsOnly := NewMockReleaseTree(t)
	releaseInputsOnly.EXPECT().FilesChangedSince(mock.Anything, CommitHash(candidateCommit)).
		Return([]string{ManifestPath, candidateNotes, "internal/release/admission/gate_test.go"}, nil).Once()
	releaseInputsOnly.EXPECT().IsRegularFile(mock.Anything, candidateNotes).Return(true, nil).Maybe()
	if _, err := Evaluate(context.Background(), activeRequest(), openManifest(t), releaseInputsOnly); err != nil {
		t.Fatalf("Evaluate(only release inputs changed) error = %v, want an open gate", err)
	}
	// History must be asked exactly once, about the final candidate commit.
	releaseInputsOnly.AssertExpectations(t)

	for _, changed := range []string{"internal/core/node/service.go", "docs/release-notes/RELEASE-v2.4.70.md"} {
		history := candidateTree(t, []string{ManifestPath, changed})
		_, err := Evaluate(context.Background(), activeRequest(), openManifest(t), history)
		if !errors.Is(err, ErrCandidateChanged) {
			t.Fatalf("Evaluate(%s changed after the candidate) error = %v, want ErrCandidateChanged", changed, err)
		}
	}

	broken := NewMockReleaseTree(t)
	broken.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return(nil, errors.New("unknown revision")).Maybe()
	broken.EXPECT().IsRegularFile(mock.Anything, candidateNotes).Return(true, nil).Maybe()
	_, err := Evaluate(context.Background(), activeRequest(), openManifest(t), broken)
	if !errors.Is(err, ErrCandidateUnverified) {
		t.Fatalf("Evaluate(history unavailable) error = %v, want ErrCandidateUnverified", err)
	}

	_, err = Evaluate(context.Background(), activeRequest(), openManifest(t), nil)
	if !errors.Is(err, ErrCandidateUnverified) {
		t.Fatalf("Evaluate(no history) error = %v, want ErrCandidateUnverified", err)
	}
}

// TestReleaseNotesOfTheVersionMustBeARegularFile: the release workflow
// publishes docs/release-notes/RELEASE-v<version>.md as the release body,
// so the gate requires exactly that file, and not a link to something else.
func TestReleaseNotesOfTheVersionMustBeARegularFile(t *testing.T) {
	missing := NewMockReleaseTree(t)
	missing.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	missing.EXPECT().IsRegularFile(mock.Anything, candidateNotes).Return(false, nil).Once()
	_, err := Evaluate(context.Background(), activeRequest(), openManifest(t), missing)
	if !errors.Is(err, ErrReleaseNotesMissing) {
		t.Fatalf("Evaluate(no regular release notes file) error = %v, want ErrReleaseNotesMissing", err)
	}
	// The tree must be inspected exactly once, at the notes of this version.
	missing.AssertExpectations(t)

	unreadable := NewMockReleaseTree(t)
	unreadable.EXPECT().FilesChangedSince(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	unreadable.EXPECT().IsRegularFile(mock.Anything, mock.Anything).Return(true, errors.New("permission denied")).Maybe()
	_, err = Evaluate(context.Background(), activeRequest(), openManifest(t), unreadable)
	if !errors.Is(err, ErrCandidateUnverified) {
		t.Fatalf("Evaluate(release notes not inspectable) error = %v, want ErrCandidateUnverified", err)
	}
}

// TestOwnerConditionMayBeDoneWithoutACommit: an owner decision is not a
// check of code, so it carries no commit; developer and expert work does.
func TestOwnerConditionMayBeDoneWithoutACommit(t *testing.T) {
	d := validManifestDoc()
	c := d.condition(t, 0)
	c["executor"] = "owner"
	c["status"] = "done"
	c["evidence"] = map[string]any{"report": "owner decision", "commit": nil}
	decodeDoc(t, d)
}

// TestClosedGateDoesNotNeedHistory: a refusal is reached without git, so
// the gate refuses the same way on a shallow checkout.
func TestClosedGateDoesNotNeedHistory(t *testing.T) {
	_, err := Evaluate(context.Background(), activeRequest(), decodeDoc(t, validManifestDoc()), nil)
	if !errors.Is(err, ErrAdmissionNotGranted) {
		t.Fatalf("Evaluate(closed manifest, no history) error = %v, want ErrAdmissionNotGranted", err)
	}
}

func TestMissingManifestIsRefused(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")
	_, err := Admit(context.Background(), missing, activeRequest(), unchangedHistory(t))
	if !errors.Is(err, ErrManifestUnreadable) {
		t.Fatalf("Admit(missing manifest) error = %v, want ErrManifestUnreadable", err)
	}
}

// rewriteFirst replaces the first occurrence of old in the encoded fixture,
// failing when it is absent so a mutation never silently tests nothing.
func rewriteFirst(t *testing.T, raw []byte, old, replacement string) []byte {
	t.Helper()
	if !bytes.Contains(raw, []byte(old)) {
		t.Fatalf("fixture has no %q to rewrite", old)
	}
	return bytes.Replace(raw, []byte(old), []byte(replacement), 1)
}

func TestCorruptManifestIsRefused(t *testing.T) {
	cases := []struct {
		name string
		raw  func(t *testing.T) []byte
	}{
		{name: "empty", raw: func(*testing.T) []byte { return nil }},
		{name: "not json", raw: func(*testing.T) []byte { return []byte("release: yes") }},
		{name: "truncated", raw: func(t *testing.T) []byte {
			raw := validManifestDoc().encode(t)
			return raw[:len(raw)/2]
		}},
		{name: "trailing document", raw: func(t *testing.T) []byte {
			raw := validManifestDoc().encode(t)
			return append(raw, []byte(`{"schema":1}`)...)
		}},
		{name: "top-level array", raw: func(*testing.T) []byte { return []byte(`[]`) }},
		{name: "unknown top-level field", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_granted"] = true
			return d.encode(t)
		}},
		{name: "unknown condition field", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["waived"] = true
			return d.encode(t)
		}},
		{name: "duplicate top-level key", raw: func(t *testing.T) []byte {
			return rewriteFirst(t, validManifestDoc().encode(t), `"schema":1`, `"schema":1,"schema":1`)
		}},
		{name: "duplicate condition key", raw: func(t *testing.T) []byte {
			return rewriteFirst(t, validManifestDoc().encode(t), `"status":"not_done"`, `"status":"done","status":"not_done"`)
		}},
		{name: "key in another case", raw: func(t *testing.T) []byte {
			return rewriteFirst(t, validManifestDoc().encode(t), `"status":"not_done"`, `"Status":"not_done"`)
		}},
		{name: "case variant next to the key", raw: func(t *testing.T) []byte {
			return rewriteFirst(t, validManifestDoc().encode(t), `"status":"not_done"`, `"status":"not_done","STATUS":"done"`)
		}},
		{name: "wrong schema", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["schema"] = 2
			return d.encode(t)
		}},
		{name: "schema missing", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			delete(d, "schema")
			return d.encode(t)
		}},
		{name: "activation version missing", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			delete(d, "activation_protocol_version")
			return d.encode(t)
		}},
		{name: "conditions missing", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			delete(d, "conditions")
			return d.encode(t)
		}},
		{name: "a required condition dropped", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["conditions"] = d["conditions"].([]any)[1:]
			return d.encode(t)
		}},
		{name: "a condition listed twice", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			conditions := d["conditions"].([]any)
			d["conditions"] = append(conditions, conditions[0])
			return d.encode(t)
		}},
		{name: "a condition the gate does not know", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["id"] = "something_else"
			return d.encode(t)
		}},
		{name: "unknown executor", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["executor"] = "anyone"
			return d.encode(t)
		}},
		{name: "unknown status", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["status"] = "waived"
			return d.encode(t)
		}},
		{name: "status missing", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			delete(d.condition(t, 0), "status")
			return d.encode(t)
		}},
		{name: "blank condition text", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["condition"] = "  "
			return d.encode(t)
		}},
		{name: "blank check", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["check"] = ""
			return d.encode(t)
		}},
		{name: "done without evidence", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["status"] = "done"
			return d.encode(t)
		}},
		{name: "done with a blank report", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["status"] = "done"
			d.condition(t, 0)["evidence"] = map[string]any{"report": " ", "commit": nil}
			return d.encode(t)
		}},
		{name: "evidence as a bare string", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["evidence"] = "see the report"
			return d.encode(t)
		}},
		{name: "malformed evidence commit", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["evidence"] = map[string]any{"report": "r", "commit": "HEAD"}
			return d.encode(t)
		}},
		{name: "abbreviated evidence commit", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["evidence"] = map[string]any{"report": "r", "commit": "7ee6c65"}
			return d.encode(t)
		}},
		{name: "developer condition done without a commit", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["status"] = "done"
			d.condition(t, 0)["evidence"] = map[string]any{"report": "r", "commit": nil}
			return d.encode(t)
		}},
		{name: "external expert condition done without a commit", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d.condition(t, 0)["executor"] = "external_expert"
			d.condition(t, 0)["status"] = "done"
			d.condition(t, 0)["evidence"] = map[string]any{"report": "r", "commit": nil}
			return d.encode(t)
		}},
		{name: "final candidate done without a commit", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			c := d.conditionByID(t, "final_candidate_checks")
			c["executor"] = "owner"
			c["status"] = "done"
			c["evidence"] = map[string]any{"report": "all green", "commit": nil}
			return d.encode(t)
		}},
		{name: "release decision without a number", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = map[string]any{"date": "2026-11-02", "version": "2.5.71"}
			return d.encode(t)
		}},
		{name: "release decision with a non-positive number", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = map[string]any{"number": 0, "date": "2026-11-02", "version": "2.5.71"}
			return d.encode(t)
		}},
		{name: "release decision with a malformed date", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = map[string]any{"number": 70, "date": "2 Nov 2026", "version": "2.5.71"}
			return d.encode(t)
		}},
		{name: "release decision without a version", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = map[string]any{"number": 70, "date": "2026-11-02"}
			return d.encode(t)
		}},
		{name: "release decision with a malformed version", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = map[string]any{"number": 70, "date": "2026-11-02", "version": "latest"}
			return d.encode(t)
		}},
		{name: "release decision as a bare flag", raw: func(t *testing.T) []byte {
			d := validManifestDoc()
			d["release_decision"] = true
			return d.encode(t)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeManifest(bytes.NewReader(tc.raw(t)))
			if !errors.Is(err, ErrManifestInvalid) {
				t.Fatalf("DecodeManifest error = %v, want ErrManifestInvalid", err)
			}
		})
	}
}

// TestCorruptManifestIsRefusedForEveryBuild: a broken manifest is a defect
// in the release inputs whatever the build, so it never reads as "nothing
// to check".
func TestCorruptManifestIsRefusedForEveryBuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.json")
	writeFile(t, path, []byte(`{"schema":1,`))
	rolledBack := activeBuild
	rolledBack.Protocol = 30
	for _, build := range []BuildVersions{activeBuild, rolledBack} {
		if _, err := Admit(context.Background(), path, Request{Build: build}, unchangedHistory(t)); !errors.Is(err, ErrManifestInvalid) {
			t.Fatalf("Admit(corrupt manifest, %+v) error = %v, want ErrManifestInvalid", build, err)
		}
	}
}

func TestRequiredConditionsAreDistinctAndNonEmpty(t *testing.T) {
	ids := RequiredConditions()
	if len(ids) == 0 {
		t.Fatal("no required conditions: the gate would open on a release decision alone")
	}
	seen := make(map[ConditionID]bool, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(string(id)) == "" || seen[id] {
			t.Fatalf("required condition %q is empty or listed twice in %v", id, ids)
		}
		seen[id] = true
	}
	if !seen[finalCandidateCondition] {
		t.Fatalf("required conditions %v lack the final candidate condition %q", ids, finalCandidateCondition)
	}
}

// TestRequiredConditionsReturnsACopy keeps a caller from shrinking the set
// the gate checks against.
func TestRequiredConditionsReturnsACopy(t *testing.T) {
	ids := RequiredConditions()
	ids[0] = "tampered"
	if RequiredConditions()[0] == "tampered" {
		t.Fatal("RequiredConditions exposes the gate's own slice")
	}
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
