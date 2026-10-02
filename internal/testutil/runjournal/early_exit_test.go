package runjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// early_exit_test.go pins that a result recorded by a sweep counts as done
// only once that sweep has re-read its sources at the end and found them
// unchanged. Any exit before that check — a cancel, a stand defect, a crash
// that runs no code at all — leaves the result in quarantine, so a resume
// with equal stamps cannot take it as done.

// resumeOverRecorded opens a second journal on dir with the same sources and
// sweeps keys again, returning what it measured beside its report and error.
func resumeOverRecorded(t *testing.T, dir string, sources []Snapshot, keys []ConfigKey) ([]ConfigID, SweepReport, error) {
	t.Helper()
	var measured []ConfigID
	report, err := newJournal(t, dir, sources).Sweep(context.Background(), keys, SelectAll(),
		func(_ context.Context, key ConfigKey) ([]byte, error) {
			measured = append(measured, key.ID())
			return []byte("resumed"), nil
		})
	return measured, report, err
}

func TestACancelledSweepLeavesItsResultsQuarantined(t *testing.T) {
	sources := standSources(t, "package a", "package b")
	dir := t.TempDir()
	keys := gridKeys(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := newJournal(t, dir, sources).Sweep(ctx, keys, SelectAll(), func(_ context.Context, key ConfigKey) ([]byte, error) {
		if key.ID() == keys[1].ID() {
			cancel()
		}
		return []byte("x"), nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sweep = %v, want context.Canceled", err)
	}

	measured, report, err := resumeOverRecorded(t, dir, sources, keys)
	if !errors.Is(err, ErrQuarantined) {
		t.Fatalf("resume after a cancelled sweep = %v, want ErrQuarantined: its result was never checked against the sources", err)
	}
	if !slices.Equal(report.Ledger.Quarantined, idsOf(keys[0])) {
		t.Fatalf("quarantined %v, want the result the cancelled sweep recorded, %v", report.Ledger.Quarantined, idsOf(keys[0]))
	}
	if !slices.Equal(measured, idsOf(keys[1])) {
		t.Fatalf("resume measured %v, want only the interrupted point %v", measured, idsOf(keys[1]))
	}
}

// A crash runs no cleanup, so the protection cannot live in an exit path: the
// doubt must already be on disk when the result is. sweepOne is the whole
// per-point step of a sweep with nothing after it — exactly what a process
// killed before the end of Sweep leaves behind.
func TestAResultLeftByACrashedSweepIsQuarantined(t *testing.T) {
	sources := standSources(t, "package a", "package b")
	dir := t.TempDir()
	keys := gridKeys(1)
	crashed := newJournal(t, dir, sources)
	if _, err := crashed.sweepOne(context.Background(), 0, keys[0], SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return []byte("x"), nil }, testSweepRun(t)); err != nil {
		t.Fatalf("sweepOne: %v", err)
	}

	measured, report, err := resumeOverRecorded(t, dir, sources, keys)
	if !errors.Is(err, ErrQuarantined) || !slices.Equal(report.Ledger.Quarantined, idsOf(keys...)) {
		t.Fatalf("resume after a crash = %v, quarantined %v; want ErrQuarantined for %v", err, report.Ledger.Quarantined, idsOf(keys...))
	}
	if len(measured) != 0 {
		t.Fatalf("resume re-measured %v: a recorded result is never replaced", measured)
	}
	if detail := report.Ledger.QuarantineDetail[keys[0].ID()]; detail == "" {
		t.Fatal("the quarantine of an unverified result carries no reason")
	}
}

// A stand defect stops the sweep while its context is alive: the sources can
// still be checked, so the results recorded before the defect are verified
// rather than left for the operator.
func TestASweepStoppedByADefectStillVerifiesWhatItRecorded(t *testing.T) {
	sources := standSources(t, "package a", "package b")
	dir := t.TempDir()
	keys := gridKeys(2)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	overwriteFile(t, filepath.Join(dir, keys[1].resultName()), []byte("not a record"))

	journal := newJournal(t, dir, sources)
	_, err := journal.Sweep(context.Background(), keys, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return []byte("x"), nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("sweep over a corrupt record = %v, want ErrCorruptRecord", err)
	}
	inspection, err := journal.Inspect(context.Background(), keys[0])
	if err != nil || inspection.Status != StatusCompleted {
		t.Fatalf("result recorded before the defect: %v, %v; want completed — the sources were checked and unchanged", inspection.Status, err)
	}
}

func TestACleanSweepLeavesItsResultsDone(t *testing.T) {
	sources := standSources(t, "package a", "package b")
	dir := t.TempDir()
	keys := gridKeys(2)
	if _, err := newJournal(t, dir, sources).Sweep(context.Background(), keys, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return []byte("x"), nil }); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	measured, report, err := resumeOverRecorded(t, dir, sources, keys)
	if err != nil || !report.Ledger.Complete() || len(measured) != 0 {
		t.Fatalf("resume of a clean sweep: err %v, measured %v, ledger %s", err, measured, report.Ledger.Summary())
	}
}

// Two executors measure one configuration from two working directories whose
// contents — and so fingerprints — are identical. The winner records the
// result and stops before its own source check; the loser checks ITS
// directory and passes. That check says nothing about the winner's directory,
// so the winner's result must stay quarantined: only the executor that wrote
// a result can vouch for it.
func TestALosersCheckDoesNotVouchForTheWinnersUncheckedResult(t *testing.T) {
	winnerSources := standSources(t, "package a", "package b")
	loserSources := standSources(t, "package a", "package b")
	if !slices.Equal(stampsOf(winnerSources), stampsOf(loserSources)) {
		t.Fatal("the scenario needs two directories with equal fingerprints")
	}
	dir := t.TempDir()
	keys := gridKeys(1)
	winner := newJournal(t, dir, winnerSources)
	loser := newJournal(t, dir, loserSources)

	report, err := loser.Sweep(context.Background(), keys, SelectAll(), func(ctx context.Context, key ConfigKey) ([]byte, error) {
		// The winner measures and records the same point meanwhile, then
		// stops: sweepOne is its whole per-point step with no settle after.
		if _, err := runWinnerStoppedBeforeSettle(ctx, winner, key); err != nil {
			return nil, err
		}
		return []byte("loser"), nil
	})
	if !errors.Is(err, ErrQuarantined) {
		t.Fatalf("loser's sweep = %v, want ErrQuarantined: the recorded result was never checked by its writer", err)
	}
	inspection, err := loser.Inspect(context.Background(), keys[0])
	if err != nil || inspection.Status != StatusQuarantined {
		t.Fatalf("winner's unchecked result is %v (%v), want quarantined", inspection.Status, err)
	}
	if !slices.Equal(report.Ledger.Quarantined, idsOf(keys...)) {
		t.Fatalf("quarantined %v, want %v", report.Ledger.Quarantined, idsOf(keys...))
	}
}

func stampsOf(sources []Snapshot) []SourceStamp {
	stamps := make([]SourceStamp, 0, len(sources))
	for _, snapshot := range sources {
		stamps = append(stamps, snapshot.Stamp())
	}
	return stamps
}

func runWinnerStoppedBeforeSettle(ctx context.Context, winner *Journal, key ConfigKey) (sweepAction, error) {
	run, err := newSweepRun()
	if err != nil {
		return actionUnset, err
	}
	return winner.sweepOne(ctx, 0, key, SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
		return []byte("winner"), nil
	}, run)
}

func testSweepRun(t *testing.T) sweepRun {
	t.Helper()
	run, err := newSweepRun()
	if err != nil {
		t.Fatalf("sweep run: %v", err)
	}
	return run
}

// The counterpart: a winner that finishes its own sweep vouches for its own
// result, and the loser counts it as done without quarantining anything.
func TestAWinnerThatCheckedItsSourcesIsCountedByTheLoser(t *testing.T) {
	dir := t.TempDir()
	keys := gridKeys(1)
	winner := newJournal(t, dir, standSources(t, "package a", "package b"))
	loser := newJournal(t, dir, standSources(t, "package a", "package b"))

	report, err := loser.Sweep(context.Background(), keys, SelectAll(), func(ctx context.Context, _ ConfigKey) ([]byte, error) {
		if _, err := winner.Sweep(ctx, keys, SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
			return []byte("winner"), nil
		}); err != nil {
			return nil, err
		}
		return []byte("loser"), nil
	})
	if err != nil {
		t.Fatalf("loser's sweep after the winner vouched for its result: %v", err)
	}
	if !slices.Equal(report.RecordedByRival, idsOf(keys...)) || !report.Ledger.Complete() {
		t.Fatalf("rival %v, ledger %s; want the winner's result counted as done", report.RecordedByRival, report.Ledger.Summary())
	}
}
