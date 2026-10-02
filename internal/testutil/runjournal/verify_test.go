package runjournal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- rule 3: each field of the verification is load-bearing on its own -------

func TestInspectRefusesABodyEditedInPlace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordCompleted(ctx, key, []byte("p50=12ms")); err != nil {
		t.Fatalf("record: %v", err)
	}
	path := filepath.Join(dir, key.resultName())
	// Same length, one byte different: only the checksum can tell.
	overwriteFile(t, path, replaceOnce(t, readFile(t, path), "p50=12ms", "p50=19ms"))

	if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("inspect of a body edited in place = %v, want ErrCorruptRecord", err)
	}
}

func TestInspectRefusesAFailureDetailEditedInPlace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordFailure(ctx, key, "node 3 did not start"); err != nil {
		t.Fatalf("record: %v", err)
	}
	path := filepath.Join(dir, attemptFiles(t, dir, key)[0])
	overwriteFile(t, path, replaceOnce(t, readFile(t, path), "node 3", "node 4"))

	if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("inspect of an edited failure = %v, want ErrCorruptRecord", err)
	}
}

func TestInspectRefusesAResealedHeaderThatMisstatesTheBodyLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordCompleted(ctx, key, []byte("body")); err != nil {
		t.Fatalf("record: %v", err)
	}
	path := filepath.Join(dir, key.resultName())
	header, body, err := decodeRecord(readFile(t, path))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	header.BodyBytes++
	resealed, err := encodeRecord(header, body)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	overwriteFile(t, path, resealed)

	if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("inspect = %v, want ErrCorruptRecord for a misstated body length", err)
	}
}

func TestInspectRefusesAHeaderWithAnUnknownField(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordCompleted(ctx, key, []byte("body")); err != nil {
		t.Fatalf("record: %v", err)
	}
	path := filepath.Join(dir, key.resultName())
	raw := readFile(t, path)
	lines := bytes.SplitN(raw, []byte{'\n'}, 4)
	headerLine := bytes.Replace(lines[1], []byte(`{"config_id"`), []byte(`{"verdict":"good","config_id"`), 1)
	overwriteFile(t, path, sealRecord(headerLine, lines[3]))

	if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("inspect of a header with an unknown field = %v, want ErrCorruptRecord", err)
	}
}

func TestInspectRefusesAFailedAttemptUnderTheResultName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordFailure(ctx, key, "x"); err != nil {
		t.Fatalf("record: %v", err)
	}
	moveRecord(t, dir, attemptFiles(t, dir, key)[0], key.resultName())

	_, err := journal.Inspect(ctx, key)
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) || mismatch.Field != MismatchOutcome {
		t.Fatalf("inspect = %v, want an outcome mismatch: a failure under the result name is not a result", err)
	}
}

func TestInspectNamesTheFieldOfAForeignRecordUnderTheExpectedName(t *testing.T) {
	asked := gridKey("grid", "64")
	cases := map[MismatchField]ConfigKey{
		MismatchMeasurement: {Measurement: "other", Label: asked.Label, Params: asked.Params},
		MismatchLabel:       {Measurement: asked.Measurement, Label: "control", Params: asked.Params},
		MismatchParams:      gridKey("grid", "128"),
	}
	for field, recorded := range cases {
		t.Run(string(field), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			journal := newJournal(t, dir, standSources(t, "package a", "package b"))
			if err := journal.RecordCompleted(ctx, recorded, []byte("x")); err != nil {
				t.Fatalf("record: %v", err)
			}
			moveRecord(t, dir, recorded.resultName(), asked.resultName())

			_, err := journal.Inspect(ctx, asked)
			var mismatch *MismatchError
			if !errors.As(err, &mismatch) || mismatch.Field != field {
				t.Fatalf("inspect = %v, want a %s mismatch", err, field)
			}
		})
	}
}

func TestInspectVerifiesEveryAttemptNotOnlyTheLatest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := newSteppingClock()
	key := gridKey("grid", "64")
	older := newJournalWithClock(t, dir, standSources(t, "package old", "package b"), clock)
	current := newJournalWithClock(t, dir, standSources(t, "package a", "package b"), clock)
	if err := older.RecordFailure(ctx, key, "from other sources"); err != nil {
		t.Fatalf("older attempt: %v", err)
	}
	if err := current.RecordFailure(ctx, key, "from these sources"); err != nil {
		t.Fatalf("current attempt: %v", err)
	}

	if _, err := current.Inspect(ctx, key); !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("inspect = %v, want ErrRecordMismatch for the older attempt of other sources", err)
	}
}

// TestLatestAttemptIsChosenByTimeNotByName arranges the names in the order
// OPPOSITE to the times, so a reader that picks the last name reports the
// oldest attempt. Random tokens would make that a coin toss; the files are
// renamed to fixed tokens instead.
func TestLatestAttemptIsChosenByTimeNotByName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	for _, reason := range []string{"older", "newer"} {
		if err := journal.RecordFailure(ctx, key, reason); err != nil {
			t.Fatalf("record %s: %v", reason, err)
		}
		for _, name := range attemptFiles(t, dir, key) {
			if !strings.Contains(name, "0000000000000000") && !strings.Contains(name, "ffffffffffffffff") {
				token := map[string]string{"older": "ffffffffffffffff", "newer": "0000000000000000"}[reason]
				moveRecord(t, dir, name, key.attemptName(token))
			}
		}
	}

	inspection, err := journal.Inspect(ctx, key)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.Evidence == nil || inspection.Evidence.Failure == nil ||
		inspection.Evidence.Failure.Detail != "newer" {
		t.Fatalf("evidence %+v, want the attempt written last", inspection.Evidence)
	}
}

func TestANonRecordFileUnderAConfigurationStemIsNotAnAttempt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	stray := key.fileStem() + attemptInfix + "notes.txt"
	writeTree(t, dir, map[string]string{stray: "operator notes"})

	inspection, err := journal.Inspect(ctx, key)
	if err != nil || inspection.Status != StatusMissing {
		t.Fatalf("inspection %+v, %v: a non-record file was read as an attempt", inspection, err)
	}
	ledger, err := journal.Ledger(ctx, []ConfigKey{key})
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if len(ledger.Strangers) != 1 || ledger.Strangers[0] != stray {
		t.Fatalf("strangers %v, want the stray file", ledger.Strangers)
	}
}

func TestRecordedAtIsStoredInUTC(t *testing.T) {
	ctx := context.Background()
	zone := time.FixedZone("UTC+3", 3*60*60)
	clock := fixedClock{at: time.Date(2026, 10, 2, 15, 0, 0, 0, zone)}
	journal := newJournalWithClock(t, t.TempDir(), standSources(t, "package a", "package b"), clock)
	key := gridKey("grid", "64")
	if err := journal.RecordCompleted(ctx, key, nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	result, err := journal.Result(ctx, key)
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if result.RecordedAt.Location() != time.UTC || !result.RecordedAt.Equal(clock.at) {
		t.Fatalf("recorded at %v, want %v in UTC", result.RecordedAt, clock.at)
	}
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

func TestNewTakesSourcesInAnyOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sources := standSources(t, "package a", "package b")
	key := gridKey("grid", "64")
	if err := newJournal(t, dir, sources).RecordCompleted(ctx, key, nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	reversed := newJournal(t, dir, []Snapshot{sources[1], sources[0]})
	inspection, err := reversed.Inspect(ctx, key)
	if err != nil || inspection.Status != StatusCompleted {
		t.Fatalf("inspection %+v, %v: the order snapshots were listed in was taken for other sources", inspection, err)
	}
}

func TestCompletedEvidenceCarriesNoFailure(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordCompleted(ctx, key, nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	inspection, err := journal.Inspect(ctx, key)
	if err != nil || inspection.Evidence == nil || inspection.Evidence.Failure != nil {
		t.Fatalf("inspection %+v, %v: a completed result must carry evidence and no failure", inspection, err)
	}
}

// --- names the filesystem can hold --------------------------------------------

// TestALongLabelDoesNotLoseTheMeasurement: a name the filesystem refuses would
// surface only when the result is written — after the measurement it names.
func TestALongLabelDoesNotLoseTheMeasurement(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := ConfigKey{
		Measurement: Measurement(strings.Repeat("m", 300)),
		Label:       Label(strings.Repeat("grid point ", 40)),
		Params:      []Param{{Name: "nodes", Value: "64"}},
	}
	if longest := len(key.attemptName(strings.Repeat("f", 16))); longest > maxFileNameBytes {
		t.Fatalf("the longest file name of a key is %d bytes, the limit is %d", longest, maxFileNameBytes)
	}
	if err := journal.RecordFailure(ctx, key, "first try"); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if err := journal.RecordCompleted(ctx, key, []byte("measured")); err != nil {
		t.Fatalf("record completed: %v", err)
	}
	if result, err := journal.Result(ctx, key); err != nil || string(result.Body) != "measured" {
		t.Fatalf("result %q, %v", result.Body, err)
	}
}

func TestTwoLongLabelsWithACommonPrefixKeepApart(t *testing.T) {
	prefix := strings.Repeat("x", 500)
	first := ConfigKey{Measurement: "m", Label: Label(prefix + "a")}
	second := ConfigKey{Measurement: "m", Label: Label(prefix + "b")}
	if first.resultName() == second.resultName() {
		t.Fatalf("two configurations share the file name %s", first.resultName())
	}
}

// --- hard links ---------------------------------------------------------------

func TestSweepRefusesAFilesystemWithoutHardLinksBeforeAnyStep(t *testing.T) {
	fakes := map[string]linkFunc{
		"link refused": func(oldname, newname string) error {
			return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: errors.ErrUnsupported}
		},
		// A filesystem that "links" by copying over whatever is there would let
		// every writer win; it must be refused, not trusted.
		"link replaces": func(oldname, newname string) error {
			raw, err := os.ReadFile(oldname) //nolint:gosec // a path the journal produced
			if err != nil {
				return err
			}
			return os.WriteFile(newname, raw, 0o600)
		},
	}
	for name, fake := range fakes {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			journal := newJournal(t, dir, standSources(t, "package a", "package b"))
			journal.link = fake
			stepped := 0
			_, err := journal.Sweep(context.Background(), gridKeys(2), SelectAll(),
				func(context.Context, ConfigKey) ([]byte, error) { stepped++; return nil, nil })
			if !errors.Is(err, ErrNoHardLinks) {
				t.Fatalf("sweep = %v, want ErrNoHardLinks", err)
			}
			if stepped != 0 {
				t.Fatalf("%d step(s) ran on a filesystem that cannot keep a result", stepped)
			}
			if err := journal.KeepSources(context.Background()); !errors.Is(err, ErrNoHardLinks) {
				t.Fatalf("KeepSources = %v, want ErrNoHardLinks", err)
			}
		})
	}
}

func TestTheHardLinkProbeLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	if err := journal.KeepSources(context.Background()); err != nil {
		t.Fatalf("keep: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != keptSourcesDir {
			t.Fatalf("%s is left in the journal after the probe", entry.Name())
		}
	}
}

// --- invariants that may never be reported as ordinary outcomes ---------------

func TestAnAttemptTokenCollisionIsAnInconsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	journal.link = func(oldname, newname string) error {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: fs.ErrExist}
	}
	err := journal.RecordFailure(ctx, gridKey("grid", "64"), "x")
	if !errors.Is(err, ErrInconsistentJournal) {
		t.Fatalf("RecordFailure on a taken attempt name = %v, want ErrInconsistentJournal", err)
	}
}

func TestMismatchErrorNamesBothValues(t *testing.T) {
	err := error(&MismatchError{ConfigID: "0011223344556677", File: "f", Field: MismatchLabel, Want: "a", Got: "b"})
	if !errors.Is(err, ErrRecordMismatch) || !strings.Contains(err.Error(), fmt.Sprintf("%s is b", MismatchLabel)) {
		t.Fatalf("mismatch error %q", err)
	}
}
