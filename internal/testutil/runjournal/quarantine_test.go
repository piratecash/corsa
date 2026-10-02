package runjournal

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// quarantine_test.go pins what happens to results a sweep recorded while its
// sources changed: they stay on disk (they are what was measured), but they
// do not count as done — not even after the tree is restored and a resumed
// sweep finds the stamps equal again — until the operator decides.

// restoredTreeRun is what the resumed sweep left: the journal, the keys, its
// report and error, and what it measured.
type restoredTreeRun struct {
	journal  *Journal
	keys     []ConfigKey
	report   SweepReport
	err      error
	measured []ConfigID
}

// restoredTreeSweep runs the reviewer's scenario: a grid of two, the tree is
// edited while the first point is measured, the sweep fails, the edit is
// undone, and a second journal over a fresh snapshot of the restored tree
// resumes.
func restoredTreeSweep(t *testing.T) restoredTreeRun {
	t.Helper()
	ctx := context.Background()
	base := t.TempDir()
	original := map[string]string{"pkg/code.go": "package pkg"}
	writeTree(t, base, original)
	spec := SourceSpec{Name: "measured", Base: base, Roots: []SourceRoot{{Dir: "pkg", Recursive: true, Filter: NonTestGo}}}
	stand := snapshotOf(t, "stand", "package stand")
	dir := t.TempDir()
	keys := gridKeys(2)

	before, err := TakeSnapshot(ctx, spec)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	first := newJournal(t, dir, []Snapshot{before, stand})
	_, err = first.Sweep(ctx, keys, SelectAll(), func(_ context.Context, key ConfigKey) ([]byte, error) {
		if key.ID() == keys[0].ID() {
			writeTree(t, base, map[string]string{"pkg/code.go": "package pkg // edited mid-sweep"})
		}
		return []byte("measured under an edited tree"), nil
	})
	if !errors.Is(err, ErrSourcesChanged) {
		t.Fatalf("sweep over an edited tree = %v, want ErrSourcesChanged", err)
	}

	writeTree(t, base, original)
	after, err := TakeSnapshot(ctx, spec)
	if err != nil {
		t.Fatalf("snapshot of the restored tree: %v", err)
	}
	if after.Stamp() != before.Stamp() {
		t.Fatalf("restored tree stamp %v, want the original %v: the scenario needs equal stamps", after.Stamp(), before.Stamp())
	}
	resumed := newJournal(t, dir, []Snapshot{after, stand})
	var measured []ConfigID
	report, err := resumed.Sweep(ctx, keys, SelectAll(), func(_ context.Context, key ConfigKey) ([]byte, error) {
		measured = append(measured, key.ID())
		return []byte("x"), nil
	})
	return restoredTreeRun{journal: resumed, keys: keys, report: report, err: err, measured: measured}
}

func TestResultsOfASweepWhoseSourcesChangedAreNotResumedAsDone(t *testing.T) {
	run := restoredTreeSweep(t)
	keys, report, err, measured := run.keys, run.report, run.err, run.measured

	if !errors.Is(err, ErrQuarantined) {
		t.Fatalf("resumed sweep after restoring the tree = %v, want ErrQuarantined: it must not succeed on results nobody vouched for", err)
	}
	if len(measured) != 0 {
		t.Fatalf("resume re-measured %v: a recorded result is never replaced", measured)
	}
	if got, want := report.Ledger.Quarantined, idsOf(keys...); !slices.Equal(got, want) {
		t.Fatalf("quarantined %v, want %v", got, want)
	}
	if len(report.Ledger.Completed) != 0 || report.Ledger.Complete() {
		t.Fatalf("completed %v: a quarantined result must not count as done", report.Ledger.Completed)
	}
	if len(report.AlreadyRecorded) != 0 {
		t.Fatalf("already recorded %v: quarantined results were taken as done", report.AlreadyRecorded)
	}
	for _, id := range report.Ledger.Quarantined {
		if report.Ledger.QuarantineDetail[id] == "" {
			t.Fatalf("quarantine of %s carries no reason", id)
		}
	}
}

func TestReleasingAQuarantineIsTheOperatorsExplicitDecision(t *testing.T) {
	ctx := context.Background()
	run := restoredTreeSweep(t)
	journal, keys := run.journal, run.keys

	inspection, err := journal.Inspect(ctx, keys[0])
	if err != nil || inspection.Status != StatusQuarantined {
		t.Fatalf("Inspect = %v, %v; want quarantined", inspection.Status, err)
	}
	if err := journal.ReleaseQuarantine(ctx, keys[0], ""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("release without a stated decision = %v, want ErrInvalidConfig", err)
	}
	for _, key := range keys {
		if err := journal.ReleaseQuarantine(ctx, key, "edit was a comment; results kept"); err != nil {
			t.Fatalf("release %s: %v", key, err)
		}
	}
	if err := journal.ReleaseQuarantine(ctx, keys[0], "again"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("second release = %v, want ErrNotQuarantined", err)
	}

	report, err := journal.Sweep(ctx, keys, SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
		t.Fatal("a released result was measured again")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("sweep after the operator released the quarantine: %v", err)
	}
	if !report.Ledger.Complete() || len(report.Ledger.Quarantined) != 0 {
		t.Fatalf("ledger after release: %s", report.Ledger.Summary())
	}
}

func TestReleaseRefusesAResultThatWasNeverQuarantined(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := gridKey("grid", "a")
	if err := journal.RecordCompleted(ctx, key, []byte("x")); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := journal.ReleaseQuarantine(ctx, key, "nothing to release"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("release of a clean result = %v, want ErrNotQuarantined", err)
	}
}
