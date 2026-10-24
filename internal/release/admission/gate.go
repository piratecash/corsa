package admission

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// finalCandidateCondition is the condition whose evidence names the commit
// every check ran on; the tree being released is compared with it.
const finalCandidateCondition ConditionID = "final_candidate_checks"

// requiredConditions are the conditions a release that activates the
// secure session must meet; refusals report them in this order. Removing
// one is a code change on purpose: the manifest alone cannot lower the bar.
var requiredConditions = []ConditionID{
	"crypto_review",
	"tor_phase_measurement",
	"first_contact_latency",
	"compat_real_releases",
	"load_run_on_candidate",
	"probe_per_contract",
	"tls_first_switch",
	"pin_store_marker",
	"unsafe_pin_store_refusal",
	"dial_paths_unified",
	"loopback_inbound_class",
	"self_record_min_session",
	"owner_session_choices",
	"owner_release_shape",
	"network_observation_plan",
	finalCandidateCondition,
	"release_notes",
}

// releaseNotesPath is the notes file the release workflow publishes as the
// body of the release of version.
func releaseNotesPath(version domain.ReleaseVersion) string {
	return "docs/release-notes/RELEASE-v" + version.String() + ".md"
}

// releaseInputs are the only paths that may change between the final
// candidate and the released tree: recording the evidence and the decision
// and writing this version's notes do not change what was checked.
func releaseInputs(version domain.ReleaseVersion) []string {
	return []string{
		ManifestPath,
		releaseNotesPath(version),
		"internal/release/admission/gate_test.go",
	}
}

// RequiredConditions returns a copy of the conditions the gate requires.
func RequiredConditions() []ConditionID {
	return slices.Clone(requiredConditions)
}

func isRequired(id ConditionID) bool {
	return slices.Contains(requiredConditions, id)
}

var (
	// ErrAdmissionNotGranted is a valid manifest that does not admit the
	// build: a condition is not done or the release decision is missing.
	ErrAdmissionNotGranted = errors.New("release admission not granted")
	// ErrActivationRollback is a build below the activation the manifest
	// records: turning the secure session off again is not a way around
	// its release conditions.
	ErrActivationRollback = errors.New("build rolls back the recorded secure session activation")
	// ErrReleaseVersionMismatch is a tag or a release decision that names a
	// version other than the one being built.
	ErrReleaseVersionMismatch = errors.New("release version mismatch")
	// ErrCandidateChanged is a tree that differs from the final candidate
	// in more than the release inputs, so the checks do not cover it.
	ErrCandidateChanged = errors.New("tree changed after the final candidate")
	// ErrReleaseNotesMissing is a release without its notes file as a
	// regular file at the path the workflow publishes.
	ErrReleaseNotesMissing = errors.New("release notes missing")
	// ErrCandidateUnverified is a final candidate the gate could not
	// compare the tree with.
	ErrCandidateUnverified = errors.New("final candidate could not be compared with the tree")
	// ErrBuildVersions is a build whose version constants do not describe
	// a session mode.
	ErrBuildVersions = errors.New("build version constants inconsistent")
)

// BuildVersions are the version constants a build is compiled with.
type BuildVersions struct {
	Protocol   domain.ProtocolVersion
	Minimum    domain.ProtocolVersion
	Activation domain.ProtocolVersion
	Release    domain.ReleaseVersion
}

// CurrentBuild returns the constants of this build, read from config so the
// gate judges exactly what the node will run and announce.
func CurrentBuild() BuildVersions {
	return BuildVersions{
		Protocol:   domain.ProtocolVersion(config.ProtocolVersion),
		Minimum:    domain.ProtocolVersion(config.MinimumProtocolVersion),
		Activation: domain.ProtocolVersion(config.ProtocolVersionSecureSession),
		Release: domain.ReleaseVersion{
			Major: config.ClientVersionMajor,
			Minor: config.ClientVersionMinor,
			Build: config.ClientVersionBuild,
		},
	}
}

// Request is what is about to be published.
type Request struct {
	Build BuildVersions
	// Tag is the release tag being published, nil outside a tag build.
	Tag *domain.ReleaseVersion
}

// ReleaseTree is the tree about to be released, as the gate inspects it.
type ReleaseTree interface {
	// FilesChangedSince lists the paths that differ between commit and the
	// tree, including files git does not track; it fails when commit is not
	// an ancestor of the released commit or the tree hides changes.
	FilesChangedSince(ctx context.Context, commit CommitHash) ([]string, error)
	// IsRegularFile reports whether path is a regular file, not a link.
	IsRegularFile(ctx context.Context, path string) (bool, error)
}

// sessionV2Active reports whether the build runs the secure session at all,
// using the same derivation the node uses to choose its session mode.
func (b BuildVersions) sessionV2Active() (bool, error) {
	mode, err := sessionv2.ModeFor(int(b.Protocol), int(b.Minimum), int(b.Activation))
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrBuildVersions, err)
	}
	return mode != sessionv2.ModeLegacyOnly, nil
}

// Admission is the gate's verdict and what it was based on.
type Admission struct {
	// SessionV2Active is whether the build runs the secure session.
	SessionV2Active bool
	// Outstanding are the required conditions not yet done, in the order
	// of RequiredConditions.
	Outstanding []ConditionID
	// Deferred lists explicitly deferred work separately from successful checks.
	Deferred []ConditionID
	// ReleaseDecision is the owner's decision to release, nil if none.
	ReleaseDecision *OwnerDecision
}

// Evaluate decides whether req may be published under manifest m. history
// is consulted only once everything else admits the release, to prove the
// tree is the final candidate plus release inputs; a refusal never needs it.
func Evaluate(ctx context.Context, req Request, m Manifest, history ReleaseTree) (Admission, error) {
	if m.Schema != CurrentManifestSchema {
		return Admission{}, fmt.Errorf("%w: schema %d, this gate reads schema %d", ErrManifestInvalid, m.Schema, CurrentManifestSchema)
	}
	build := req.Build
	if m.ActivationProtocolVersion != build.Activation {
		return Admission{}, fmt.Errorf("%w: manifest covers activation at protocol %d, the build activates at %d",
			ErrManifestInvalid, m.ActivationProtocolVersion, build.Activation)
	}
	active, err := build.sessionV2Active()
	if err != nil {
		return Admission{}, err
	}
	if req.Tag != nil && *req.Tag != build.Release {
		return Admission{}, fmt.Errorf("%w: tag %s, build %s", ErrReleaseVersionMismatch, req.Tag, build.Release)
	}
	admission := Admission{
		SessionV2Active: active,
		Outstanding:     outstandingConditions(m.Conditions),
		Deferred:        deferredConditions(m.Conditions),
		ReleaseDecision: m.ReleaseDecision,
	}
	if !active {
		return admission, fmt.Errorf("%w: protocol %d is below the activation at %d",
			ErrActivationRollback, build.Protocol, m.ActivationProtocolVersion)
	}
	if len(admission.Outstanding) > 0 || admission.ReleaseDecision == nil {
		return admission, ErrAdmissionNotGranted
	}
	decided, err := admission.ReleaseDecision.releaseVersion()
	if err != nil {
		return admission, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	if decided != build.Release {
		return admission, fmt.Errorf("%w: release decision covers %s, build %s",
			ErrReleaseVersionMismatch, decided, build.Release)
	}
	if err := verifyFinalCandidate(ctx, m.Conditions, build.Release, history); err != nil {
		return admission, err
	}
	if err := verifyReleaseNotes(ctx, build.Release, history); err != nil {
		return admission, err
	}
	return admission, nil
}

func outstandingConditions(conditions []Condition) []ConditionID {
	done := make(map[ConditionID]bool, len(conditions))
	for _, c := range conditions {
		done[c.ID] = c.Status == ConditionDone || c.ownerDeferred()
	}
	var outstanding []ConditionID
	for _, id := range requiredConditions {
		if !done[id] {
			outstanding = append(outstanding, id)
		}
	}
	return outstanding
}

func deferredConditions(conditions []Condition) []ConditionID {
	var deferred []ConditionID
	for _, c := range conditions {
		if c.ownerDeferred() {
			deferred = append(deferred, c.ID)
		}
	}
	return deferred
}

// verifyFinalCandidate requires the released tree to differ from the final
// candidate commit in release inputs only.
func verifyFinalCandidate(ctx context.Context, conditions []Condition, version domain.ReleaseVersion, history ReleaseTree) error {
	commit, err := finalCandidateCommit(conditions)
	if err != nil {
		return err
	}
	if history == nil {
		return fmt.Errorf("%w: no history source", ErrCandidateUnverified)
	}
	changed, err := history.FilesChangedSince(ctx, commit)
	if err != nil {
		return fmt.Errorf("%w: commit %s: %w", ErrCandidateUnverified, commit, err)
	}
	for _, file := range changed {
		if !slices.Contains(releaseInputs(version), file) {
			return fmt.Errorf("%w: %s changed since commit %s", ErrCandidateChanged, file, commit)
		}
	}
	return nil
}

func finalCandidateCommit(conditions []Condition) (CommitHash, error) {
	for _, c := range conditions {
		if c.ID == finalCandidateCondition && c.Evidence != nil && c.Evidence.Commit != nil {
			return *c.Evidence.Commit, nil
		}
	}
	return "", fmt.Errorf("%w: %s names no commit", ErrCandidateUnverified, finalCandidateCondition)
}

// verifyReleaseNotes requires this version's notes as a regular file: the
// workflow publishes it as the release body, and a link could point it at
// any file in the runner.
func verifyReleaseNotes(ctx context.Context, version domain.ReleaseVersion, tree ReleaseTree) error {
	path := releaseNotesPath(version)
	regular, err := tree.IsRegularFile(ctx, path)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrCandidateUnverified, path, err)
	}
	if !regular {
		return fmt.Errorf("%w: %s is not a regular file", ErrReleaseNotesMissing, path)
	}
	return nil
}

// Admit loads the manifest at path and evaluates req against it. A manifest
// that cannot be read or is invalid refuses every build.
func Admit(ctx context.Context, path string, req Request, history ReleaseTree) (Admission, error) {
	f, err := os.Open(path)
	if err != nil {
		return Admission{}, fmt.Errorf("%w: %w", ErrManifestUnreadable, err)
	}
	defer func() { _ = f.Close() }() // opened read-only; a close error loses nothing

	m, err := DecodeManifest(f)
	if err != nil {
		return Admission{}, err
	}
	return Evaluate(ctx, req, m, history)
}
