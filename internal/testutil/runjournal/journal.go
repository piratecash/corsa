package runjournal

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// New checks the configuration and returns a journal. It touches no disk: the
// directory is created by the first write, so constructing a journal for a
// listing or a ledger has no side effect.
func New(cfg Config) (*Journal, error) {
	switch {
	case cfg.Dir == "":
		return nil, fmt.Errorf("%w: no directory", ErrInvalidConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: no clock", ErrInvalidConfig)
	case len(cfg.Sources) == 0:
		// A record that names no sources binds its number to no code, which is
		// exactly the record a resumed sweep cannot check.
		return nil, fmt.Errorf("%w: no source snapshot", ErrInvalidConfig)
	}
	stamps := make([]SourceStamp, 0, len(cfg.Sources))
	seen := make(map[FingerprintName]struct{}, len(cfg.Sources))
	for _, snapshot := range cfg.Sources {
		stamp := snapshot.Stamp()
		if stamp.Stamp == "" {
			return nil, fmt.Errorf("%w: a snapshot that was never taken", ErrInvalidConfig)
		}
		if _, duplicate := seen[stamp.Name]; duplicate {
			return nil, fmt.Errorf("%w: two snapshots named %s", ErrInvalidConfig, stamp.Name)
		}
		seen[stamp.Name] = struct{}{}
		stamps = append(stamps, stamp)
	}
	return &Journal{
		dir:     cfg.Dir,
		sources: append([]Snapshot(nil), cfg.Sources...),
		stamps:  canonicalStamps(stamps),
		clock:   cfg.Clock,
		link:    os.Link,
	}, nil
}

// canonicalStamps orders stamps by name, so the order in which a driver listed
// its snapshots is not mistaken for a difference in sources.
//
// The order is total (name, then stamp), so even a record that repeats a name
// has one canonical form rather than whatever an unstable sort leaves.
func canonicalStamps(stamps []SourceStamp) []SourceStamp {
	sorted := append([]SourceStamp(nil), stamps...)
	slices.SortFunc(sorted, func(a, b SourceStamp) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Stamp, b.Stamp))
	})
	return sorted
}

// Dir is the journal directory.
func (j *Journal) Dir() string { return j.dir }

// Stamps are the source stamps every record of this journal carries, by name.
func (j *Journal) Stamps() []SourceStamp { return append([]SourceStamp(nil), j.stamps...) }

// RecordCompleted records the result of one configuration. If a completed
// result already exists it is left untouched and ErrAlreadyRecorded is
// returned: a rerun goes into a separate directory, never over a result.
func (j *Journal) RecordCompleted(ctx context.Context, key ConfigKey, body []byte) error {
	return j.recordCompletedBy(ctx, key, body, "")
}

// recordCompletedBy records a result written by run ("" outside Sweep).
func (j *Journal) recordCompletedBy(ctx context.Context, key ConfigKey, body []byte, run sweepRun) error {
	raw, err := j.encode(ctx, key, OutcomeCompleted, "", body, run)
	if err != nil {
		return err
	}
	err = publishNoReplace(ctx, j.dir, key.resultName(), raw, j.link)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%w: %s", ErrAlreadyRecorded, key)
	}
	return err
}

// RecordFailure records one failed attempt with its reason. Every attempt gets
// a file of its own, so a failure can never replace a result, and an attempt
// that broke differently stays as evidence of its own.
func (j *Journal) RecordFailure(ctx context.Context, key ConfigKey, reason string) error {
	raw, err := j.encode(ctx, key, OutcomeFailed, reason, nil, "")
	if err != nil {
		return err
	}
	token, err := attemptToken()
	if err != nil {
		return err
	}
	err = publishNoReplace(ctx, j.dir, key.attemptName(token), raw, j.link)
	if errors.Is(err, fs.ErrExist) {
		// Sixty-four random bits colliding is not a race, it is a broken
		// random source; it must not pass as an ordinary outcome.
		return fmt.Errorf("%w: attempt token %s of %s is already taken: %w", ErrInconsistentJournal, token, key, err)
	}
	return err
}

func (j *Journal) encode(ctx context.Context, key ConfigKey, outcome Outcome, detail string, body []byte, run sweepRun) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("recording %s: %w", key, err)
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	return encodeRecord(recordHeader{
		ConfigID:    key.ID(),
		Measurement: key.Measurement,
		Label:       key.Label,
		Params:      key.canonicalParams(),
		Sources:     j.Stamps(),
		Outcome:     outcome,
		Detail:      detail,
		RecordedAt:  j.clock.Now().UTC(),
		BodyBytes:   len(body),
		Run:         run,
	}, body)
}

// Inspect is the only way a driver may ask "has this already been done". The
// answer is VERIFIED: a completed or failed status is reported only for a
// record that checks against itself and against the key and sources asked
// for. Anything else is an error, never a status — a record that does not
// verify is neither a result to skip nor an absence to rerun.
func (j *Journal) Inspect(ctx context.Context, key ConfigKey) (Inspection, error) {
	names, err := j.listNames(ctx)
	if err != nil {
		return Inspection{}, err
	}
	return j.inspectAmong(ctx, key, names)
}

// inspectAmong inspects one configuration given a listing of the directory.
// The result file is read directly rather than looked up in the listing, so a
// result published after the listing is still seen.
func (j *Journal) inspectAmong(ctx context.Context, key ConfigKey, names []string) (Inspection, error) {
	if err := ctx.Err(); err != nil {
		return Inspection{}, fmt.Errorf("inspecting %s: %w", key, err)
	}
	if err := key.Validate(); err != nil {
		return Inspection{}, err
	}
	header, _, err := j.loadVerified(ctx, key, key.resultName(), OutcomeCompleted)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := requireNoQuarantineMarks(key, names); err != nil {
			return Inspection{}, err
		}
		return j.inspectAttempts(ctx, key, names)
	case err != nil:
		return Inspection{}, err
	}
	return j.inspectQuarantine(ctx, key, header)
}

// inspectAttempts reports the failed attempts of a configuration that has no
// result. Every attempt is verified, not only the latest: an attempt of other
// parameters or sources under this configuration's stem is a directory
// problem, not a failure to retry.
func (j *Journal) inspectAttempts(ctx context.Context, key ConfigKey, names []string) (Inspection, error) {
	attempts := key.attemptNames(names)
	if len(attempts) == 0 {
		return Inspection{Status: StatusMissing}, nil
	}
	var latest recordHeader
	for _, name := range attempts {
		if err := ctx.Err(); err != nil {
			return Inspection{}, fmt.Errorf("inspecting %s: %w", key, err)
		}
		header, _, err := j.loadVerified(ctx, key, name, OutcomeFailed)
		if err != nil {
			return Inspection{}, err
		}
		// Names are in byte order, so on equal timestamps the later name wins:
		// arbitrary, but the same answer on every reading.
		if !header.RecordedAt.Before(latest.RecordedAt) {
			latest = header
		}
	}
	return Inspection{Status: StatusFailed, Evidence: &Evidence{
		RecordedAt: latest.RecordedAt,
		Failure:    &Failure{Detail: latest.Detail, Attempts: len(attempts)},
	}}, nil
}

// Result reads a completed result back, verified exactly as Inspect verifies
// it. ErrNotRecorded when there is none, whatever attempts may exist.
func (j *Journal) Result(ctx context.Context, key ConfigKey) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("reading %s: %w", key, err)
	}
	if err := key.Validate(); err != nil {
		return Result{}, err
	}
	header, body, err := j.loadVerified(ctx, key, key.resultName(), OutcomeCompleted)
	if errors.Is(err, fs.ErrNotExist) {
		return Result{}, fmt.Errorf("%w: %s", ErrNotRecorded, key)
	}
	if err != nil {
		return Result{}, err
	}
	return Result{Key: header.key(), Sources: header.Sources, RecordedAt: header.RecordedAt, Body: body}, nil
}

// loadVerified reads one file of the journal and verifies it against itself
// and against what was asked for. A missing file is returned as an error
// satisfying errors.Is(err, fs.ErrNotExist) and nothing else.
func (j *Journal) loadVerified(ctx context.Context, key ConfigKey, name string, want Outcome) (recordHeader, []byte, error) {
	if err := ctx.Err(); err != nil {
		return recordHeader{}, nil, fmt.Errorf("reading %s: %w", name, err)
	}
	raw, err := os.ReadFile(filepath.Join(j.dir, name)) //nolint:gosec // a name this package derived
	if errors.Is(err, fs.ErrNotExist) {
		return recordHeader{}, nil, fs.ErrNotExist
	}
	if err != nil {
		return recordHeader{}, nil, fmt.Errorf("reading %s: %w", name, err)
	}
	header, body, err := decodeRecord(raw)
	if err != nil {
		return recordHeader{}, nil, fmt.Errorf("%s: %w", name, err)
	}
	if err := j.matchRecord(key, name, header, want); err != nil {
		return recordHeader{}, nil, err
	}
	return header, body, nil
}

// matchRecord names the FIRST field in which a self-consistent record differs
// from the request, with both values.
//
// The identifier is compared last, after every field it is derived from, and
// is DEFENSIVE, unreachable code today: decodeRecord already refuses a header
// whose stated identifier differs from its derived one, and equal fields
// derive equal identifiers. It stays so that a future change of either path
// fails closed here instead of passing a record no field check objected to.
func (j *Journal) matchRecord(key ConfigKey, name string, header recordHeader, want Outcome) error {
	wantParams, gotParams := key.canonicalParams(), header.key().canonicalParams()
	gotStamps := canonicalStamps(header.Sources)
	checks := []struct {
		field     MismatchField
		same      bool
		want, got string
	}{
		{MismatchMeasurement, key.Measurement == header.Measurement, string(key.Measurement), string(header.Measurement)},
		{MismatchLabel, key.Label == header.Label, string(key.Label), string(header.Label)},
		{MismatchParams, slices.Equal(wantParams, gotParams), renderParams(wantParams), renderParams(gotParams)},
		{MismatchSources, slices.Equal(j.stamps, gotStamps), renderStamps(j.stamps), renderStamps(gotStamps)},
		{MismatchOutcome, want == header.Outcome, string(want), string(header.Outcome)},
		{MismatchConfigID, key.ID() == header.ConfigID, string(key.ID()), string(header.ConfigID)},
	}
	for _, check := range checks {
		if !check.same {
			return &MismatchError{ConfigID: key.ID(), File: name, Field: check.field, Want: check.want, Got: check.got}
		}
	}
	return nil
}

func renderParams(params []Param) string {
	rendered := make([]string, 0, len(params))
	for _, param := range params {
		rendered = append(rendered, fmt.Sprintf("%q=%q", param.Name, param.Value))
	}
	return "{" + strings.Join(rendered, ", ") + "}"
}

func renderStamps(stamps []SourceStamp) string {
	rendered := make([]string, 0, len(stamps))
	for _, stamp := range stamps {
		rendered = append(rendered, fmt.Sprintf("%s:%s", stamp.Name, stamp.Stamp))
	}
	return "{" + strings.Join(rendered, ", ") + "}"
}

// listNames lists the journal directory in byte order.
func (j *Journal) listNames(ctx context.Context) ([]string, error) {
	return listDir(ctx, j.dir)
}

// listDir lists a directory in byte order. A directory that was never created
// is an empty listing, not an error: a journal nobody wrote holds nothing.
func listDir(ctx context.Context, dir string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("listing the journal: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("listing the journal: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}

// attemptNames picks this configuration's attempt files out of a listing.
func (k ConfigKey) attemptNames(names []string) []string {
	prefix := k.fileStem() + attemptInfix
	var attempts []string
	for _, name := range names {
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, attemptSuffix) {
			attempts = append(attempts, name)
		}
	}
	return attempts
}
