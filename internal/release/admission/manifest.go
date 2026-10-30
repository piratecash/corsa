// Package admission decides whether the current build may be published as a
// release. A build that runs the secure session (protocol v2) may be
// published only once every condition listed in the committed manifest is
// met, each with evidence, a separate owner decision to release that version
// is recorded, and the tree is the final candidate plus release inputs.
// A build below the activation the manifest records is refused, so turning the
// session off is no way around the conditions. The decision is fail-closed:
// a missing, unreadable or malformed manifest refuses every build.
package admission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ManifestPath is the committed manifest, relative to the repository root.
// It is a constant rather than a flag so that the release workflow cannot be
// pointed at a different file without a reviewed change to this package.
const ManifestPath = "packaging/release-admission.json"

// ManifestSchema is the version of the manifest format.
type ManifestSchema int

// CurrentManifestSchema is the only schema this gate reads. A manifest in a
// newer format is refused rather than half-understood.
const CurrentManifestSchema ManifestSchema = 2

// ConditionID names one release condition. The set the gate requires lives
// in code (RequiredConditions), so dropping a condition from the manifest
// alone does not shrink what is checked.
type ConditionID string

// Executor is who has to carry a condition out.
type Executor string

const (
	ExecutorOwner          Executor = "owner"
	ExecutorDeveloper      Executor = "developer"
	ExecutorExternalExpert Executor = "external_expert"
)

var knownExecutors = map[Executor]bool{
	ExecutorOwner:          true,
	ExecutorDeveloper:      true,
	ExecutorExternalExpert: true,
}

// ConditionStatus distinguishes completed checks from incomplete work and the
// single Tor measurement explicitly deferred by the owner. Deferred is not done.
type ConditionStatus string

const (
	ConditionNotDone  ConditionStatus = "not_done"
	ConditionPartial  ConditionStatus = "partial"
	ConditionDone     ConditionStatus = "done"
	ConditionDeferred ConditionStatus = "deferred"
)

var knownStatuses = map[ConditionStatus]bool{
	ConditionNotDone:  true,
	ConditionPartial:  true,
	ConditionDone:     true,
	ConditionDeferred: true,
}

// TorDeferralDecision is the recorded owner decision, not a measurement report.
// Only this exact decision and condition may use deferred; no general waiver is
// accepted from a manifest. Changing this scope requires a reviewed code change.
const TorDeferralDecision EvidenceRef = "docs/operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23"

func (c Condition) ownerDeferred() bool {
	return c.ID == "tor_phase_measurement" && c.Status == ConditionDeferred &&
		c.Executor == ExecutorDeveloper && c.Evidence != nil &&
		c.Evidence.Report == TorDeferralDecision && c.Evidence.Commit == nil
}

// EvidenceRef points at what confirms a condition: a report, a test run, a
// recorded decision, a document.
type EvidenceRef string

// CommitHash is a full git commit hash, 40 lowercase hex digits. An
// abbreviation can become ambiguous as history grows, and evidence must keep
// naming the commit it was checked on.
type CommitHash string

var commitHashPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Evidence is what confirms a condition. Commit is the commit the check was
// run on, nil when the evidence is not tied to the code (an owner decision).
// It names the checked commit rather than the release tag because a report
// stays valid while only the release inputs change after it.
type Evidence struct {
	Report EvidenceRef `json:"report"`
	Commit *CommitHash `json:"commit"`
}

// OwnerDecisionNumber is the number of an owner's recorded decision.
type OwnerDecisionNumber int

// DecisionDate is the calendar day of an owner decision, YYYY-MM-DD.
type DecisionDate string

const decisionDateLayout = "2006-01-02"

// ReleaseVersionText is a release version as written in the manifest,
// "MAJOR.MINOR.BUILD".
type ReleaseVersionText string

// OwnerDecision is the owner's decision to publish one release version. It
// is a decision of its own: meeting the conditions does not imply it, and it
// does not carry over to another version.
type OwnerDecision struct {
	Number  OwnerDecisionNumber `json:"number"`
	Date    DecisionDate        `json:"date"`
	Version ReleaseVersionText  `json:"version"`
}

// Condition is one release condition and its current state.
type Condition struct {
	ID          ConditionID     `json:"id"`
	Requirement string          `json:"condition"`
	Executor    Executor        `json:"executor"`
	Check       string          `json:"check"`
	Evidence    *Evidence       `json:"evidence"`
	Status      ConditionStatus `json:"status"`
}

// Manifest is the committed record of the release conditions for one
// activation of the secure session.
type Manifest struct {
	Schema                    ManifestSchema         `json:"schema"`
	ActivationProtocolVersion domain.ProtocolVersion `json:"activation_protocol_version"`
	// ReleaseDecision is nil until the owner decides to release.
	ReleaseDecision *OwnerDecision `json:"release_decision"`
	Conditions      []Condition    `json:"conditions"`
}

var (
	// ErrManifestUnreadable is a manifest that could not be read at all.
	ErrManifestUnreadable = errors.New("release admission manifest unreadable")
	// ErrManifestInvalid is a manifest that was read but is not a valid
	// statement of the release conditions.
	ErrManifestInvalid = errors.New("release admission manifest invalid")
)

// DecodeManifest reads and validates a manifest. Unknown fields, repeated
// keys, keys in another case, trailing data, an unknown executor or status,
// a condition outside the required set, a missing or repeated required
// condition, a done condition without evidence and a malformed release
// decision are all ErrManifestInvalid.
func DecodeManifest(r io.Reader) (Manifest, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrManifestUnreadable, err)
	}
	if err := checkObjectKeys(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	// A second document after the first would otherwise be ignored, and the
	// file a reviewer reads would not be the file the gate judged.
	if decoder.InputOffset() != int64(len(bytes.TrimRight(raw, " \t\r\n"))) {
		return Manifest{}, fmt.Errorf("%w: data after the manifest object", ErrManifestInvalid)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	return m, nil
}

func (m Manifest) validate() error {
	if m.Schema != CurrentManifestSchema {
		return fmt.Errorf("schema %d, this gate reads schema %d", m.Schema, CurrentManifestSchema)
	}
	if m.ActivationProtocolVersion <= 0 {
		return errors.New("activation_protocol_version is missing")
	}
	if err := validateConditionSet(m.Conditions); err != nil {
		return err
	}
	for _, c := range m.Conditions {
		if err := c.validate(); err != nil {
			return fmt.Errorf("condition %q: %w", c.ID, err)
		}
	}
	if m.ReleaseDecision != nil {
		if err := m.ReleaseDecision.validate(); err != nil {
			return fmt.Errorf("release_decision: %w", err)
		}
	}
	return nil
}

// validateConditionSet requires the manifest to list exactly the conditions
// the gate knows, each once.
func validateConditionSet(conditions []Condition) error {
	listed := make(map[ConditionID]bool, len(conditions))
	for _, c := range conditions {
		if !isRequired(c.ID) {
			return fmt.Errorf("condition %q is not one this gate requires", c.ID)
		}
		if listed[c.ID] {
			return fmt.Errorf("condition %q is listed twice", c.ID)
		}
		listed[c.ID] = true
	}
	for _, id := range requiredConditions {
		if !listed[id] {
			return fmt.Errorf("required condition %q is missing", id)
		}
	}
	return nil
}

func (c Condition) validate() error {
	if !knownExecutors[c.Executor] {
		return fmt.Errorf("unknown executor %q", c.Executor)
	}
	if !knownStatuses[c.Status] {
		return fmt.Errorf("unknown status %q", c.Status)
	}
	if strings.TrimSpace(c.Requirement) == "" {
		return errors.New("condition text is empty")
	}
	if strings.TrimSpace(c.Check) == "" {
		return errors.New("check is empty")
	}
	if c.Evidence != nil {
		if err := c.Evidence.validate(); err != nil {
			return fmt.Errorf("evidence: %w", err)
		}
	}
	if c.Status == ConditionDeferred {
		if !c.ownerDeferred() {
			return errors.New("deferred is restricted to the recorded owner Tor measurement decision")
		}
		return nil
	}
	if c.Status != ConditionDone {
		return nil
	}
	if c.Evidence == nil {
		return errors.New("marked done without evidence")
	}
	if c.ID == finalCandidateCondition && c.Evidence.Commit == nil {
		return errors.New("marked done without the candidate commit the checks ran on")
	}
	if checksCode[c.Executor] && c.Evidence.Commit == nil {
		return fmt.Errorf("%s work marked done without the commit it was checked on", c.Executor)
	}
	return nil
}

// checksCode are the executors whose work is a check of the code, so its
// evidence names the commit checked; an owner decision does not.
var checksCode = map[Executor]bool{
	ExecutorDeveloper:      true,
	ExecutorExternalExpert: true,
}

func (e Evidence) validate() error {
	if strings.TrimSpace(string(e.Report)) == "" {
		return errors.New("report is empty")
	}
	if e.Commit != nil && !commitHashPattern.MatchString(string(*e.Commit)) {
		return fmt.Errorf("commit %q is not a full lowercase hex commit hash", *e.Commit)
	}
	return nil
}

func (d OwnerDecision) validate() error {
	if d.Number <= 0 {
		return fmt.Errorf("decision number %d is not a recorded decision", d.Number)
	}
	if _, err := time.Parse(decisionDateLayout, string(d.Date)); err != nil {
		return fmt.Errorf("decision date %q is not YYYY-MM-DD", d.Date)
	}
	if _, err := d.releaseVersion(); err != nil {
		return err
	}
	return nil
}

func (d OwnerDecision) releaseVersion() (domain.ReleaseVersion, error) {
	version, err := domain.ParseReleaseVersion(string(d.Version))
	if err != nil {
		return domain.ReleaseVersion{}, fmt.Errorf("decision version: %w", err)
	}
	return version, nil
}

// manifestKeyPattern is the only spelling a manifest key may take. Every
// field name is lowercase ASCII, and encoding/json matches keys without
// regard to case (Unicode folding included): "Status" next to "status"
// would be read as the same field, the last one winning silently.
var manifestKeyPattern = regexp.MustCompile(`^[a-z_]+$`)

// checkObjectKeys walks one JSON value and refuses any object with a
// repeated key or a key outside manifestKeyPattern. encoding/json keeps the
// last of repeated keys, so the value a reviewer reads first need not be the
// one the gate judges.
func checkObjectKeys(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, _ := keyToken.(string) // object keys are always strings in a valid stream
			if !manifestKeyPattern.MatchString(key) {
				return fmt.Errorf("key %q is not lowercase ASCII", key)
			}
			if seen[key] {
				return fmt.Errorf("key %q is repeated", key)
			}
			seen[key] = true
			if err := checkObjectKeys(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := checkObjectKeys(decoder); err != nil {
				return err
			}
		}
	}
	_, err = decoder.Token() // the closing delimiter
	return err
}
