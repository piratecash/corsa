package runjournal

import (
	"context"
	"errors"
	"fmt"
)

// SelectAll executes every configuration.
func SelectAll() Selection { return func(int, ConfigKey) bool { return true } }

// SelectRange executes the half-open slice [from, to) of the enumeration in
// its declared order — the way a grid is cut into calls.
func SelectRange(from, to int) Selection {
	return func(index int, _ ConfigKey) bool { return index >= from && index < to }
}

// SelectIDs executes only the named configurations, so one expensive
// configuration can be run on its own.
func SelectIDs(ids ...ConfigID) Selection {
	wanted := make(map[ConfigID]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	return func(_ int, key ConfigKey) bool {
		_, ok := wanted[key.ID()]
		return ok
	}
}

// sweepAction is what one configuration came to in one call.
type sweepAction uint8

const (
	actionUnset sweepAction = iota
	actionAlreadyRecorded
	actionNotSelected
	actionExecuted
	actionExecutedRival
	actionQuarantined
	// actionRecorded — this run measured the configuration and recorded the
	// result itself, so this run's final source check vouches for it.
	actionRecorded
)

// Sweep is the loop every driver shares: keep the sources, then for each
// configuration resume or select, measure, record — and finally read the
// ledger of the whole set back from the journal.
//
// It stops on a stand defect (a record that does not verify, a write that
// fails, an interruption) and never on a refused configuration: a Step error
// is recorded as that configuration's failure and the sweep goes on, because
// stopping the grid on one broken point would lose every point after it.
func (j *Journal) Sweep(ctx context.Context, keys []ConfigKey, selection Selection, step Step) (SweepReport, error) {
	if selection == nil || step == nil {
		return SweepReport{}, fmt.Errorf("%w: a sweep needs a selection and a step", ErrInvalidConfig)
	}
	if len(keys) == 0 {
		return SweepReport{}, ErrEmptyEnumeration
	}
	if err := RequireDistinct(keys); err != nil {
		return SweepReport{}, err
	}
	if err := j.KeepSources(ctx); err != nil {
		return SweepReport{}, err
	}

	run, err := newSweepRun()
	if err != nil {
		return SweepReport{}, err
	}
	var report SweepReport
	var recorded []ConfigKey
	for index, key := range keys {
		action, err := j.sweepOne(ctx, index, key, selection, step, run)
		if action == actionRecorded {
			recorded = append(recorded, key)
		}
		if err != nil {
			// The check still runs when it can: a defect is no reason to
			// leave the points measured before it for the operator.
			return report, errors.Join(fmt.Errorf("%s: %w", key, err), j.settle(ctx, &report, recorded, run))
		}
		report.note(key.ID(), action)
	}

	changed := j.settle(ctx, &report, recorded, run)

	ledger, err := j.Ledger(ctx, keys)
	if err != nil {
		return report, errors.Join(changed, fmt.Errorf("reading the ledger back: %w", err))
	}
	report.Ledger = ledger
	switch {
	case changed != nil:
		return report, changed
	case len(ledger.Quarantined) > 0:
		return report, fmt.Errorf("%w: %d result(s), see the ledger; ReleaseQuarantine records the decision",
			ErrQuarantined, len(ledger.Quarantined))
	default:
		return report, nil
	}
}

// verifySourcesUnchanged re-reads every snapshot's tree after the sweep. Each
// record of the sweep carries the stamps taken before it, so a tree edited
// while it ran would leave those stamps naming code that was never measured.
// The records stay on disk — they are what was measured — but the sweep is
// reported as failed, every result it recorded is quarantined, and the
// operator decides what the stamp is worth (ReleaseQuarantine).
func (j *Journal) verifySourcesUnchanged(ctx context.Context) error {
	for _, snapshot := range j.sources {
		if err := snapshot.VerifyTree(ctx); err != nil {
			return fmt.Errorf("after the sweep: %w", err)
		}
	}
	return nil
}

func (j *Journal) sweepOne(ctx context.Context, index int, key ConfigKey, selection Selection, step Step, run sweepRun) (sweepAction, error) {
	inspection, err := j.Inspect(ctx, key)
	if err != nil {
		return actionUnset, err
	}
	switch inspection.Status {
	case StatusCompleted:
		return actionAlreadyRecorded, nil
	case StatusQuarantined:
		// Neither done nor re-measurable: the result name is taken for good.
		return actionQuarantined, nil
	}
	if !selection(index, key) {
		return actionNotSelected, nil
	}

	body, stepErr := step(ctx, key)
	if err := ctx.Err(); err != nil {
		// An interrupted measurement is neither a result nor a refusal of the
		// configuration; recording it as a failure would blame the point for
		// the operator's cancel.
		return actionUnset, fmt.Errorf("interrupted: %w", err)
	}
	if stepErr != nil {
		if err := j.RecordFailure(ctx, key, stepErr.Error()); err != nil {
			return actionUnset, fmt.Errorf("recording the failure: %w", err)
		}
		return actionExecuted, nil
	}
	// The result carries its run, and is in doubt until that run vouches
	// for it (settle): no exit — not even a killed process — can leave a
	// result a resume takes as done without its writer's source check.
	return j.recordMeasured(ctx, key, body, run)
}

// settle is the end of every sweep that recorded anything, early or not: it
// re-reads the sources and either vouches for what this run recorded or
// marks it as measured while they changed. A cancelled context cannot read
// anything; the results then stay unvouched, which IS the quarantine, so
// nothing is written — a cleanup on a dead context would only trade one
// unchecked write for another.
func (j *Journal) settle(ctx context.Context, report *SweepReport, recorded []ConfigKey, run sweepRun) error {
	if ctx.Err() != nil {
		return nil
	}
	changed := j.verifySourcesUnchanged(ctx)
	if changed == nil {
		return j.markVerified(ctx, recorded, run)
	}
	marked, err := j.quarantineRecordedBy(ctx, recorded, changed.Error())
	report.Quarantined = append(report.Quarantined, marked...)
	return errors.Join(changed, err)
}

// recordMeasured records a fresh result. Losing the race to another writer is
// not an error — both measured the same configuration — but "the name is
// taken" says nothing about what is in it: the rival's record is accepted only
// once it verifies against this key and these sources.
func (j *Journal) recordMeasured(ctx context.Context, key ConfigKey, body []byte, run sweepRun) (sweepAction, error) {
	err := j.recordCompletedBy(ctx, key, body, run)
	switch {
	case err == nil:
		return actionRecorded, nil
	case !errors.Is(err, ErrAlreadyRecorded):
		return actionUnset, fmt.Errorf("recording the result: %w", err)
	}
	inspection, err := j.Inspect(ctx, key)
	if err != nil {
		return actionUnset, fmt.Errorf("a concurrent writer recorded this configuration: %w", err)
	}
	switch inspection.Status {
	case StatusCompleted:
		return actionExecutedRival, nil
	case StatusQuarantined:
		// The rival's run has not vouched for its result (yet, or ever), or
		// saw its sources change. Only it can lift that; this run cannot.
		return actionQuarantined, nil
	default:
		return actionUnset, fmt.Errorf("%w: a concurrent writer took the result name and the journal reports %s",
			ErrInconsistentJournal, inspection.Status)
	}
}

func (r *SweepReport) note(id ConfigID, action sweepAction) {
	switch action {
	case actionAlreadyRecorded:
		r.AlreadyRecorded = append(r.AlreadyRecorded, id)
	case actionExecuted, actionRecorded:
		r.Executed = append(r.Executed, id)
	case actionExecutedRival:
		r.Executed = append(r.Executed, id)
		r.RecordedByRival = append(r.RecordedByRival, id)
	case actionQuarantined:
		r.Quarantined = append(r.Quarantined, id)
	case actionNotSelected, actionUnset:
		// Nothing happened to it in this call; the ledger says what it is.
	}
}

// newSweepRun draws a fresh run token.
func newSweepRun() (sweepRun, error) {
	token, err := attemptToken()
	if err != nil {
		return "", fmt.Errorf("naming the sweep run: %w", err)
	}
	return sweepRun(token), nil
}
