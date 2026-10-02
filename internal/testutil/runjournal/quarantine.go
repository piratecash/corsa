package runjournal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
)

// quarantine.go keeps the verdict on results a sweep recorded while its
// sources changed or before it could check them.
//
// Such a result stays on disk — it is what was measured, and rule 2 never
// replaces a result — but its stamp may name code that was not measured. The
// stamp alone cannot tell: once the tree is restored, a fresh snapshot has the
// same stamp, and a resumed sweep would verify the result and skip it as done.
// So the doubt is written down next to the result, and only the operator's
// decision, also written down, lifts it. Both are published no-replace, so
// neither can be rewritten after the fact.

// quarantine marks a completed result as recorded while the sources changed.
// Marking it twice keeps the first mark: the doubt is the same.
func (j *Journal) quarantine(ctx context.Context, key ConfigKey, detail string) error {
	return j.publishMark(ctx, key, key.quarantineName(), OutcomeQuarantined, detail, "")
}

// ReleaseQuarantine records the operator's decision to keep a quarantined
// result as done. The decision is required and kept verbatim: the journal
// does not judge whether a source change mattered, it only refuses to decide
// that on its own. A result that is not quarantined has nothing to release.
func (j *Journal) ReleaseQuarantine(ctx context.Context, key ConfigKey, decision string) error {
	if decision == "" {
		return fmt.Errorf("%w: releasing %s needs the operator's stated decision", ErrInvalidConfig, key)
	}
	inspection, err := j.Inspect(ctx, key)
	if err != nil {
		return err
	}
	if inspection.Status != StatusQuarantined {
		return fmt.Errorf("%w: %s is %s", ErrNotQuarantined, key, inspection.Status)
	}
	raw, err := j.encode(ctx, key, OutcomeReleased, decision, nil, "")
	if err != nil {
		return err
	}
	err = publishNoReplace(ctx, j.dir, key.releaseName(), raw, j.link)
	if errors.Is(err, fs.ErrExist) {
		// Inspect saw no release a moment ago: another writer released it
		// in between, which is the same decision reached twice.
		return fmt.Errorf("%w: %s was released concurrently", ErrNotQuarantined, key)
	}
	return err
}

// inspectQuarantine turns a verified result into its status. A result is
// in doubt when it carries a quarantine mark, or when the sweep run that wrote
// it has not vouched for it; the operator's release lifts either doubt. A
// release with no doubt to lift is never written by the journal and is
// refused.
func (j *Journal) inspectQuarantine(ctx context.Context, key ConfigKey, result recordHeader) (Inspection, error) {
	doubt, err := j.doubtOf(ctx, key, result.Run)
	if err != nil {
		return Inspection{}, err
	}
	released, err := j.markPresent(ctx, key, key.releaseName(), OutcomeReleased, "")
	if err != nil {
		return Inspection{}, err
	}
	switch {
	case doubt == nil && released:
		return Inspection{}, fmt.Errorf("%w: %s has a release and nothing to release", ErrInconsistentJournal, key)
	case doubt != nil && !released:
		return Inspection{Status: StatusQuarantined, Evidence: &Evidence{RecordedAt: result.RecordedAt, Quarantine: doubt}}, nil
	default:
		return Inspection{Status: StatusCompleted, Evidence: &Evidence{RecordedAt: result.RecordedAt}}, nil
	}
}

// doubtOf is why a recorded result is not done yet, or nil. A source change
// seen at the end of a sweep outranks the mere absence of a check. The doubt
// of an unchecked result lives in the result itself — the run that wrote it —
// so no exit path, not even a killed process, has to write anything for it.
func (j *Journal) doubtOf(ctx context.Context, key ConfigKey, writer sweepRun) (*Quarantine, error) {
	mark, _, err := j.loadVerified(ctx, key, key.quarantineName(), OutcomeQuarantined)
	switch {
	case err == nil:
		return &Quarantine{Detail: mark.Detail}, nil
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	if writer == "" {
		// Recorded outside Sweep: the caller of RecordCompleted vouches.
		return nil, nil
	}
	vouched, err := j.markPresent(ctx, key, key.verifiedName(writer), OutcomeVerified, writer)
	if err != nil || vouched {
		return nil, err
	}
	return &Quarantine{Detail: unverifiedDetail}, nil
}

// markPresent reports whether a verified mark of the given outcome, written
// by run, exists under name.
func (j *Journal) markPresent(ctx context.Context, key ConfigKey, name string, outcome Outcome, run sweepRun) (bool, error) {
	header, _, err := j.loadVerified(ctx, key, name, outcome)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	case header.Run != run:
		return false, fmt.Errorf("%w: %s was written by run %q, its name says %q", ErrInconsistentJournal, name, header.Run, run)
	default:
		return true, nil
	}
}

// requireNoQuarantineMarks refuses a mark that is only ever written beside a
// result when there is no result.
func requireNoQuarantineMarks(key ConfigKey, names []string) error {
	verifiedPrefix := key.fileStem() + verifiedInfix
	for _, name := range names {
		if name == key.quarantineName() || name == key.releaseName() || strings.HasPrefix(name, verifiedPrefix) {
			return fmt.Errorf("%w: %s holds %s and no result", ErrInconsistentJournal, key, name)
		}
	}
	return nil
}

// publishMark writes a no-replace mark beside a result. A mark already there
// is the same statement made before, and is accepted once it verifies.
func (j *Journal) publishMark(ctx context.Context, key ConfigKey, name string, outcome Outcome, detail string, run sweepRun) error {
	raw, err := j.encode(ctx, key, outcome, detail, nil, run)
	if err != nil {
		return err
	}
	err = publishNoReplace(ctx, j.dir, name, raw, j.link)
	if errors.Is(err, fs.ErrExist) {
		_, err = j.markPresent(ctx, key, name, outcome, run)
	}
	return err
}

// unverifiedDetail is the reason a result recorded by a sweep stays in doubt
// when that sweep never vouched for it.
const unverifiedDetail = "recorded by a sweep run that has not vouched for it: it ended before re-reading its sources (cancelled, stopped by a defect, or killed)"

// markVerified vouches for the results run recorded itself. A rival's result
// is never vouched for here, even with equal fingerprints: this run's check
// read this run's working directory, not the one the rival measured from.
func (j *Journal) markVerified(ctx context.Context, recorded []ConfigKey, run sweepRun) error {
	var failures []error
	for _, key := range recorded {
		if err := j.publishMark(ctx, key, key.verifiedName(run), OutcomeVerified, "", run); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", key, err))
		}
	}
	if len(failures) > 0 {
		// Fail-closed: a result without its run's mark stays quarantined.
		return fmt.Errorf("sources verified, but these results stay quarantined: %w", errors.Join(failures...))
	}
	return nil
}

// quarantineRecordedBy marks the results this run recorded as measured while
// its sources changed, and reports the ones it could not mark. An unmarked
// one still reads as quarantined (its run never vouched for it), but with
// the wrong reason — so the caller says so.
func (j *Journal) quarantineRecordedBy(ctx context.Context, recorded []ConfigKey, detail string) ([]ConfigID, error) {
	var marked []ConfigID
	var failures []error
	for _, key := range recorded {
		if err := j.quarantine(ctx, key, detail); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", key, err))
			continue
		}
		marked = append(marked, key.ID())
	}
	if len(failures) > 0 {
		return marked, fmt.Errorf("source change NOT recorded on these results (they stay quarantined as unverified): %w", errors.Join(failures...))
	}
	return marked, nil
}
