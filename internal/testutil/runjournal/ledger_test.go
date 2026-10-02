package runjournal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLedgerCountsFourStatesApart(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	keys := gridKeys(4)
	if err := journal.RecordCompleted(ctx, keys[0], []byte("ok")); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := journal.RecordFailure(ctx, keys[1], "node did not start"); err != nil {
		t.Fatalf("record failure: %v", err)
	}

	ledger, err := journal.Ledger(ctx, keys)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	assertIDs(t, "expected", ledger.Expected, idsOf(keys...))
	assertIDs(t, "completed", ledger.Completed, idsOf(keys[0]))
	assertIDs(t, "failed", ledger.Failed, idsOf(keys[1]))
	assertIDs(t, "missing", ledger.Missing, idsOf(keys[2], keys[3]))
	if ledger.FailureDetail[keys[1].ID()] != "node did not start" {
		t.Fatalf("failure detail %q", ledger.FailureDetail[keys[1].ID()])
	}
	if ledger.Complete() {
		t.Fatalf("a ledger with failed and missing configurations reports complete")
	}
	summary := ledger.Summary()
	for _, fragment := range []string{"expected 4", "completed 1", "failed 1", "missing 2"} {
		if !strings.Contains(summary, fragment) {
			t.Fatalf("summary %q lacks %q", summary, fragment)
		}
	}
}

func TestLedgerRefusesARecordThatDoesNotVerify(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keys := gridKeys(2)
	if err := newJournal(t, dir, standSources(t, "package a", "package b")).RecordCompleted(ctx, keys[0], nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	_, err := newJournal(t, dir, standSources(t, "package c", "package b")).Ledger(ctx, keys)
	if !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("ledger over a record of other sources = %v, want ErrRecordMismatch", err)
	}
}

func TestLedgerReportsStrangersWithoutTouchingThem(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	older := gridKey("older-grid", "1")
	if err := journal.RecordCompleted(ctx, older, []byte("old")); err != nil {
		t.Fatalf("record: %v", err)
	}
	ledger, err := journal.Ledger(ctx, gridKeys(2))
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if !slices.Equal(ledger.Strangers, []string{older.resultName()}) {
		t.Fatalf("strangers %v, want the older grid's record", ledger.Strangers)
	}
	if _, err := os.Stat(filepath.Join(dir, older.resultName())); err != nil {
		t.Fatalf("a stranger was touched: %v", err)
	}
}

// TestLedgerDescribesTheWholeSetAcrossBatches is rule 4: a grid run in two
// calls must end with a ledger that says the whole grid is done, not one that
// reports the first call's half as missing.
func TestLedgerDescribesTheWholeSetAcrossBatches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sources := standSources(t, "package a", "package b")
	keys := gridKeys(4)
	measure := func(_ context.Context, key ConfigKey) ([]byte, error) { return []byte(key.Label), nil }

	first, err := newJournal(t, dir, sources).Sweep(ctx, keys, SelectRange(0, 2), measure)
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	assertIDs(t, "first batch executed", first.Executed, idsOf(keys[0], keys[1]))
	assertIDs(t, "first batch missing", first.Ledger.Missing, idsOf(keys[2], keys[3]))

	second, err := newJournal(t, dir, sources).Sweep(ctx, keys, SelectRange(2, 4), measure)
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	assertIDs(t, "second batch executed", second.Executed, idsOf(keys[2], keys[3]))
	assertIDs(t, "second batch already recorded", second.AlreadyRecorded, idsOf(keys[0], keys[1]))
	assertIDs(t, "completed", second.Ledger.Completed, idsOf(keys...))
	assertIDs(t, "missing", second.Ledger.Missing, nil)
	if !second.Ledger.Complete() {
		t.Fatalf("the whole grid is recorded and the ledger says incomplete:\n%s", second.Ledger.Summary())
	}
}

func TestSweepRecordsAStepErrorAsAFailureAndContinues(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	keys := gridKeys(3)
	step := func(_ context.Context, key ConfigKey) ([]byte, error) {
		if key.ID() == keys[1].ID() {
			return nil, errors.New("listener did not bind")
		}
		return []byte("ok"), nil
	}

	report, err := journal.Sweep(ctx, keys, SelectAll(), step)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	assertIDs(t, "executed", report.Executed, idsOf(keys...))
	assertIDs(t, "completed", report.Ledger.Completed, idsOf(keys[0], keys[2]))
	assertIDs(t, "failed", report.Ledger.Failed, idsOf(keys[1]))
	if report.Ledger.FailureDetail[keys[1].ID()] != "listener did not bind" {
		t.Fatalf("failure detail %q", report.Ledger.FailureDetail[keys[1].ID()])
	}

	retried, err := journal.Sweep(ctx, keys, SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
		return []byte("ok"), nil
	})
	if err != nil {
		t.Fatalf("retry sweep: %v", err)
	}
	assertIDs(t, "retry executed only the failure", retried.Executed, idsOf(keys[1]))
	if !retried.Ledger.Complete() {
		t.Fatalf("after a successful retry the ledger is incomplete:\n%s", retried.Ledger.Summary())
	}
}

// TestSweepSkipsOnlyAVerifiedRecord is rule 3 at the level a driver sees: a
// record of other sources under the expected name stops the sweep before any
// step runs, instead of passing for a result.
func TestSweepSkipsOnlyAVerifiedRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keys := gridKeys(2)
	if err := newJournal(t, dir, standSources(t, "package a", "package b")).RecordCompleted(ctx, keys[0], nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	stepped := 0
	_, err := newJournal(t, dir, standSources(t, "package a2", "package b")).Sweep(ctx, keys, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { stepped++; return nil, nil })
	if !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("sweep over a record of other sources = %v, want ErrRecordMismatch", err)
	}
	if stepped != 0 {
		t.Fatalf("%d step(s) ran after the journal refused a record", stepped)
	}
}

func TestSweepRefusesDuplicateConfigurations(t *testing.T) {
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	keys := []ConfigKey{gridKey("grid", "1"), gridKey("grid", "1")}
	_, err := journal.Sweep(context.Background(), keys, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return nil, nil })
	if !errors.Is(err, ErrDuplicateConfig) {
		t.Fatalf("sweep = %v, want ErrDuplicateConfig", err)
	}
}

func TestSweepDoesNotRecordAnInterruptionAsAFailure(t *testing.T) {
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	keys := gridKeys(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := journal.Sweep(ctx, keys, SelectAll(), func(stepCtx context.Context, _ ConfigKey) ([]byte, error) {
		cancel()
		return nil, stepCtx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sweep = %v, want context.Canceled", err)
	}
	ledger, err := journal.Ledger(context.Background(), keys)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	assertIDs(t, "missing", ledger.Missing, idsOf(keys...))
}

func TestSweepCountsAVerifiedRivalRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sources := standSources(t, "package a", "package b")
	rival := newJournal(t, dir, sources)
	keys := gridKeys(1)

	report, err := newJournal(t, dir, sources).Sweep(ctx, keys, SelectAll(),
		func(stepCtx context.Context, key ConfigKey) ([]byte, error) {
			if err := rival.RecordCompleted(stepCtx, key, []byte("rival")); err != nil {
				return nil, err
			}
			return []byte("mine"), nil
		})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	assertIDs(t, "recorded by rival", report.RecordedByRival, idsOf(keys...))
	assertIDs(t, "completed", report.Ledger.Completed, idsOf(keys...))
	result, err := rival.Result(ctx, keys[0])
	if err != nil || string(result.Body) != "rival" {
		t.Fatalf("result %q, %v: the rival's record was replaced", result.Body, err)
	}
}

func TestSweepRefusesARivalRecordOfOtherSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rival := newJournal(t, dir, standSources(t, "package other", "package b"))
	keys := gridKeys(1)

	_, err := newJournal(t, dir, standSources(t, "package a", "package b")).Sweep(ctx, keys, SelectAll(),
		func(stepCtx context.Context, key ConfigKey) ([]byte, error) {
			if err := rival.RecordCompleted(stepCtx, key, []byte("rival")); err != nil {
				return nil, err
			}
			return []byte("mine"), nil
		})
	if !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("sweep = %v, want ErrRecordMismatch for a rival of other sources", err)
	}
}

func TestSweepKeepsItsSourcesBesideTheJournal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	if _, err := journal.Sweep(ctx, gridKeys(1), SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	for _, stamp := range journal.Stamps() {
		if err := VerifyKept(ctx, dir, stamp); err != nil {
			t.Fatalf("kept sources %s: %v", stamp.Name, err)
		}
	}
}

func TestSelections(t *testing.T) {
	keys := gridKeys(4)
	cases := map[string]struct {
		selection Selection
		want      []ConfigID
	}{
		"all":   {SelectAll(), idsOf(keys...)},
		"range": {SelectRange(1, 3), idsOf(keys[1], keys[2])},
		"ids":   {SelectIDs(keys[3].ID(), keys[0].ID()), idsOf(keys[0], keys[3])},
	}
	for name, testCase := range cases {
		var got []ConfigID
		for index, key := range keys {
			if testCase.selection(index, key) {
				got = append(got, key.ID())
			}
		}
		assertIDs(t, name, got, testCase.want)
	}
}

func assertIDs(t *testing.T, what string, got, want []ConfigID) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func TestLedgerOfNothingIsRefused(t *testing.T) {
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	if _, err := journal.Ledger(context.Background(), nil); !errors.Is(err, ErrEmptyEnumeration) {
		t.Fatalf("Ledger(nil) = %v, want ErrEmptyEnumeration", err)
	}
	_, err := journal.Sweep(context.Background(), nil, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return nil, nil })
	if !errors.Is(err, ErrEmptyEnumeration) {
		t.Fatalf("Sweep(nil) = %v, want ErrEmptyEnumeration", err)
	}
	if (Ledger{}).Complete() {
		t.Fatalf("an empty ledger reports complete")
	}
}

func TestLedgerWithFailuresAndNothingMissingIsNotComplete(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	keys := gridKeys(2)
	if err := journal.RecordCompleted(ctx, keys[0], nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := journal.RecordFailure(ctx, keys[1], "x"); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	ledger, err := journal.Ledger(ctx, keys)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if len(ledger.Missing) != 0 || ledger.Complete() {
		t.Fatalf("missing %v, complete %v: a failed configuration is not done", ledger.Missing, ledger.Complete())
	}
}

func TestTheJournalsOwnEntriesAreNotStrangers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	keys := gridKeys(1)
	if _, err := journal.Sweep(ctx, keys, SelectAll(),
		func(context.Context, ConfigKey) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	writeTree(t, dir, map[string]string{".DS_Store": "", ".pending-orphan": "half a record"})

	ledger, err := journal.Ledger(ctx, keys)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if len(ledger.Strangers) != 0 {
		t.Fatalf("strangers %v: the kept sources and dot entries belong to the journal", ledger.Strangers)
	}
	if !slices.Equal(ledger.Pending, []string{".pending-orphan"}) {
		t.Fatalf("pending %v, want the orphaned temporary reported", ledger.Pending)
	}
}

// TestSweepRefusesSourcesThatChangedDuringIt: every record of the sweep is
// stamped with the snapshot taken before it, so an edit of the tree while it
// ran makes that stamp a label for code that was not measured.
func TestSweepRefusesSourcesThatChangedDuringIt(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	writeTree(t, base, map[string]string{"pkg/code.go": "package pkg"})
	measured, err := TakeSnapshot(ctx, SourceSpec{
		Name: "measured", Base: base, Roots: []SourceRoot{{Dir: "pkg", Recursive: true, Filter: NonTestGo}},
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	journal := newJournal(t, t.TempDir(), []Snapshot{measured, snapshotOf(t, "stand", "package stand")})

	_, err = journal.Sweep(ctx, gridKeys(1), SelectAll(), func(context.Context, ConfigKey) ([]byte, error) {
		writeTree(t, base, map[string]string{"pkg/code.go": "package pkg // edited mid-sweep"})
		return []byte("x"), nil
	})
	if !errors.Is(err, ErrSourcesChanged) {
		t.Fatalf("sweep over sources edited mid-run = %v, want ErrSourcesChanged", err)
	}
}
