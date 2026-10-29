package admission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/piratecash/corsa/internal/core/domain"
)

// InterimApprovalPath is the only approval for the closed publication profile
// for 2.5.71. It is not a replacement for the S6 admission manifest.
const InterimApprovalPath = "packaging/interim-release-v2.5.71.json"

const interimApprovalLimit = 32 * 1024

var (
	ErrInterimApprovalInvalid = errors.New("interim release approval invalid")
	ErrInterimNotEligible     = errors.New("build and tag not eligible for interim 2.5.71")
	ErrInterimSourceMismatch  = errors.New("interim source digest mismatch")

	interimSourcePattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// InterimDecision attributes the separate interim publication decision. It is
// neither an expert verdict nor the manifest's owner decision completing S6.
type InterimDecision struct {
	Date      DecisionDate `json:"date"`
	Authority string       `json:"authority"`
}

// InterimApproval binds one explicit publication decision to a source digest
// and the exact uncompleted S6 obligations the owner acknowledges. Arrays must
// be present, including [] when empty. Order is the ordinary gate's order.
type InterimApproval struct {
	Schema                int                `json:"schema"`
	Version               ReleaseVersionText `json:"version"`
	SourceSHA256          string             `json:"source_sha"`
	Decision              InterimDecision    `json:"decision"`
	OutstandingConditions []ConditionID      `json:"outstanding_conditions"`
	DeferredConditions    []ConditionID      `json:"deferred_conditions"`
}

// DecodeInterimApproval reads only the strict, bounded approval schema. It
// starts no Git, filesystem or other work and does not grant publication.
func DecodeInterimApproval(r io.Reader) (*InterimApproval, error) {
	raw, err := io.ReadAll(io.LimitReader(r, interimApprovalLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read: %w", ErrInterimApprovalInvalid, err)
	}
	if len(raw) > interimApprovalLimit || !utf8.Valid(raw) {
		return nil, fmt.Errorf("%w: size or UTF-8", ErrInterimApprovalInvalid)
	}
	if err := checkObjectKeys(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInterimApprovalInvalid, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var approval InterimApproval
	if err := decoder.Decode(&approval); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInterimApprovalInvalid, err)
	}
	if decoder.InputOffset() != int64(len(bytes.TrimRight(raw, " \t\r\n"))) {
		return nil, fmt.Errorf("%w: trailing data", ErrInterimApprovalInvalid)
	}
	if err := approval.validate(); err != nil {
		return nil, err
	}
	return &approval, nil
}

func (a InterimApproval) validate() error {
	if a.Schema != 1 || a.Version != "2.5.71" || !validInterimDigest(a.SourceSHA256) {
		return fmt.Errorf("%w: schema, version or source SHA256", ErrInterimApprovalInvalid)
	}
	date, err := time.Parse(decisionDateLayout, string(a.Decision.Date))
	if err != nil || date.Year() < 1 || date.Format(decisionDateLayout) != string(a.Decision.Date) {
		return fmt.Errorf("%w: decision date", ErrInterimApprovalInvalid)
	}
	authority := a.Decision.Authority
	if !utf8.ValidString(authority) || strings.TrimSpace(authority) == "" || len(authority) > 4096 ||
		strings.ContainsAny(authority, "\x00\ufffd") {
		return fmt.Errorf("%w: decision authority text", ErrInterimApprovalInvalid)
	}
	if a.OutstandingConditions == nil || a.DeferredConditions == nil ||
		len(a.OutstandingConditions)+len(a.DeferredConditions) > len(requiredConditions) {
		return fmt.Errorf("%w: condition arrays missing or too large", ErrInterimApprovalInvalid)
	}
	seen := make(map[ConditionID]bool)
	for _, group := range [][]ConditionID{a.OutstandingConditions, a.DeferredConditions} {
		for _, id := range group {
			if !isRequired(id) || seen[id] {
				return fmt.Errorf("%w: unknown or repeated condition %q", ErrInterimApprovalInvalid, id)
			}
			seen[id] = true
		}
	}
	for _, id := range a.DeferredConditions {
		if id != "tor_phase_measurement" {
			return fmt.Errorf("%w: unrecorded deferral %q", ErrInterimApprovalInvalid, id)
		}
	}
	return nil
}

func validInterimDigest(digest string) bool {
	return interimSourcePattern.MatchString(digest) && digest != strings.Repeat("0", 64)
}

// InterimEligible is true only for the explicit 2.5.71 release tag and its
// fixed secure-session build shape. A tagless or future build uses normal S6
// admission; no environment or mutable version range selects this profile.
func InterimEligible(req Request) bool {
	want := domain.ReleaseVersion{Major: 2, Minor: 5, Build: 71}
	return req.Tag != nil && *req.Tag == want && req.Build.Release == want &&
		req.Build.Protocol == 31 && req.Build.Minimum == 28 && req.Build.Activation == 31
}

// EvaluateInterim checks only the closed pure publication policy. The caller
// must obtain digest from the committed tree and independently bind that tree
// to the clean worktree/index, tag/event/HEAD, regular approval and truthful
// version notes. This function cannot inspect or establish those I/O facts or
// downstream CI success.
//
// A nil error permits this interim CI/publication path, NOT S6 completion. The
// returned ordinary obligation snapshot stays outstanding/deferred, with no
// fabricated S6 ReleaseDecision. The caller must use a separate interim report,
// never the ordinary "applicable checks complete" success banner.
func EvaluateInterim(manifest Manifest, req Request, approval InterimApproval, digest string) (Admission, error) {
	if err := manifest.validate(); err != nil {
		return Admission{}, fmt.Errorf("%w: %w", ErrManifestInvalid, err)
	}
	if !InterimEligible(req) || manifest.ActivationProtocolVersion != req.Build.Activation {
		return Admission{}, ErrInterimNotEligible
	}
	verdict := Admission{
		SessionV2Active: true,
		Outstanding:     outstandingConditions(manifest.Conditions),
		Deferred:        deferredConditions(manifest.Conditions),
	}
	if err := approval.validate(); err != nil {
		return verdict, err
	}
	if manifest.ReleaseDecision != nil {
		return verdict, fmt.Errorf("%w: a recorded S6 release decision uses normal admission", ErrInterimApprovalInvalid)
	}
	if !validInterimDigest(digest) || digest != approval.SourceSHA256 {
		return verdict, ErrInterimSourceMismatch
	}
	if !slices.Equal(approval.OutstandingConditions, verdict.Outstanding) ||
		!slices.Equal(approval.DeferredConditions, verdict.Deferred) {
		return verdict, fmt.Errorf("%w: S6 outstanding/deferred snapshot differs", ErrInterimApprovalInvalid)
	}
	return verdict, nil
}
