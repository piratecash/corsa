package runjournal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestNewRefusesAnIncompleteConfiguration(t *testing.T) {
	sources := standSources(t, "package measured", "package stand")
	clock := newSteppingClock()
	duplicateNames := []Snapshot{sources[0], snapshotOf(t, "measured", "package other")}
	cases := map[string]Config{
		"no dir":          {Sources: sources, Clock: clock},
		"no sources":      {Dir: t.TempDir(), Clock: clock},
		"no clock":        {Dir: t.TempDir(), Sources: sources},
		"duplicate names": {Dir: t.TempDir(), Sources: duplicateNames, Clock: clock},
		"empty snapshot":  {Dir: t.TempDir(), Sources: []Snapshot{{}}, Clock: clock},
	}
	for name, config := range cases {
		if _, err := New(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: New = %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestCompletedResultIsNeverOverwritten(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")

	if err := journal.RecordCompleted(ctx, key, []byte("first")); err != nil {
		t.Fatalf("first record: %v", err)
	}
	if err := journal.RecordCompleted(ctx, key, []byte("second")); !errors.Is(err, ErrAlreadyRecorded) {
		t.Fatalf("second record = %v, want ErrAlreadyRecorded", err)
	}
	result, err := journal.Result(ctx, key)
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if string(result.Body) != "first" {
		t.Fatalf("result body is %q, the first record was replaced", result.Body)
	}
}

// TestConcurrentWritersOfOneConfigurationExactlyOneWins is the race a
// sequential test cannot see: every writer measured the same configuration and
// all of them reach the journal at once. Each writer has its own Journal value,
// as separate processes would.
func TestConcurrentWritersOfOneConfigurationExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sources := standSources(t, "package a", "package b")
	const rounds, writers = 100, 8

	journals := make([]*Journal, writers)
	for index := range journals {
		journals[index] = newJournal(t, dir, sources)
	}

	for round := range rounds {
		key := gridKey("race", strconv.Itoa(round))
		errs := make([]error, writers)
		start := make(chan struct{})
		var done sync.WaitGroup
		for writer := range writers {
			done.Add(1)
			go func() {
				defer done.Done()
				<-start
				errs[writer] = journals[writer].RecordCompleted(ctx, key, fmt.Appendf(nil, "writer %d", writer))
			}()
		}
		close(start)
		done.Wait()

		winner := -1
		for writer, err := range errs {
			switch {
			case err == nil && winner >= 0:
				t.Fatalf("round %d: writers %d and %d both recorded the same configuration", round, winner, writer)
			case err == nil:
				winner = writer
			case !errors.Is(err, ErrAlreadyRecorded):
				t.Fatalf("round %d: writer %d failed with %v, want ErrAlreadyRecorded", round, writer, err)
			}
		}
		if winner < 0 {
			t.Fatalf("round %d: no writer recorded the configuration", round)
		}
		result, err := journals[0].Result(ctx, key)
		if err != nil {
			t.Fatalf("round %d: result: %v", round, err)
		}
		if want := fmt.Sprintf("writer %d", winner); string(result.Body) != want {
			t.Fatalf("round %d: kept body %q, the winner wrote %q", round, result.Body, want)
		}
	}
}

func TestRetryAfterFailureCannotReplaceSuccess(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")

	if err := journal.RecordFailure(ctx, key, "node 3 did not start"); err != nil {
		t.Fatalf("first failure: %v", err)
	}
	if err := journal.RecordCompleted(ctx, key, []byte("measured")); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if err := journal.RecordFailure(ctx, key, "late duplicate failure"); err != nil {
		t.Fatalf("a failure after success must still be kept as evidence: %v", err)
	}

	inspection, err := journal.Inspect(ctx, key)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.Status != StatusCompleted {
		t.Fatalf("status %s after a success, want completed", inspection.Status)
	}
	result, err := journal.Result(ctx, key)
	if err != nil || string(result.Body) != "measured" {
		t.Fatalf("result = %q, %v; the success was replaced", result.Body, err)
	}
}

func TestFailedAttemptsAreKeptApartAndTheLatestIsReported(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")

	for _, reason := range []string{"first reason", "second reason", "third reason"} {
		if err := journal.RecordFailure(ctx, key, reason); err != nil {
			t.Fatalf("record %q: %v", reason, err)
		}
	}
	inspection, err := journal.Inspect(ctx, key)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.Status != StatusFailed || inspection.Evidence == nil {
		t.Fatalf("inspection %+v, want failed with evidence", inspection)
	}
	if inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Attempts != 3 || inspection.Evidence.Failure.Detail != "third reason" {
		t.Fatalf("evidence %+v, want 3 attempts and the latest reason", *inspection.Evidence)
	}
	if _, err := journal.Result(ctx, key); !errors.Is(err, ErrNotRecorded) {
		t.Fatalf("Result of a failed configuration = %v, want ErrNotRecorded", err)
	}
}

func TestInspectReportsMissingWithoutEvidence(t *testing.T) {
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	inspection, err := journal.Inspect(context.Background(), gridKey("grid", "64"))
	if err != nil {
		t.Fatalf("inspect of an empty journal: %v", err)
	}
	if inspection.Status != StatusMissing || inspection.Evidence != nil {
		t.Fatalf("inspection %+v, want missing with no evidence", inspection)
	}
}

func TestRecordCarriesArbitraryTextAndBytes(t *testing.T) {
	ctx := context.Background()
	journal := newJournal(t, t.TempDir(), standSources(t, "package a", "package b"))
	detail := "line one\n# status=completed\r\n\ttab \"quoted\" \x00 end"
	body := []byte("runjournal-record/v1\n{\"x\":1}\nsha256=00\n\x00\xff binary tail")
	key := ConfigKey{Measurement: "m", Label: "ü × ′", Params: []Param{{Name: "a=b", Value: "c\nd"}}}

	if err := journal.RecordFailure(ctx, key, detail); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	inspection, err := journal.Inspect(ctx, key)
	if err != nil || inspection.Evidence == nil || inspection.Evidence.Failure == nil || inspection.Evidence.Failure.Detail != detail {
		t.Fatalf("inspection %+v, %v: the detail did not survive verbatim", inspection, err)
	}
	if err := journal.RecordCompleted(ctx, key, body); err != nil {
		t.Fatalf("record completed: %v", err)
	}
	result, err := journal.Result(ctx, key)
	if err != nil || !bytes.Equal(result.Body, body) {
		t.Fatalf("body %q, %v: the body did not survive verbatim", result.Body, err)
	}
	if len(result.Sources) != 2 || result.Sources[0].Name != "measured" || result.Sources[1].Name != "stand" {
		t.Fatalf("record sources %+v, want both fingerprints", result.Sources)
	}
}

// --- rule 3: a skip happens only after the record verified ------------------

func TestInspectRefusesARecordOfOtherSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := gridKey("grid", "64")
	original := newJournal(t, dir, standSources(t, "package a", "package b"))
	if err := original.RecordCompleted(ctx, key, []byte("old code")); err != nil {
		t.Fatalf("record: %v", err)
	}

	changed := newJournal(t, dir, standSources(t, "package a // edited", "package b"))
	_, err := changed.Inspect(ctx, key)
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("inspect under other sources = %v, want a MismatchError", err)
	}
	if mismatch.Field != MismatchSources {
		t.Fatalf("mismatch on %s, want sources", mismatch.Field)
	}
}

func TestInspectRefusesAFailedAttemptOfOtherSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := gridKey("grid", "64")
	if err := newJournal(t, dir, standSources(t, "package a", "package b")).RecordFailure(ctx, key, "x"); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	_, err := newJournal(t, dir, standSources(t, "package a", "package b2")).Inspect(ctx, key)
	if !errors.Is(err, ErrRecordMismatch) {
		t.Fatalf("inspect = %v, want ErrRecordMismatch", err)
	}
}

func TestInspectRefusesARecordOfOtherParametersUnderTheExpectedName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	recorded, asked := gridKey("grid", "64"), gridKey("grid", "128")
	if err := journal.RecordCompleted(ctx, recorded, []byte("64 nodes")); err != nil {
		t.Fatalf("record: %v", err)
	}
	moveRecord(t, dir, recorded.resultName(), asked.resultName())

	_, err := journal.Inspect(ctx, asked)
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) || mismatch.Field != MismatchParams {
		t.Fatalf("inspect = %v, want a parameter mismatch", err)
	}
}

func TestInspectRefusesARecordThatDoesNotVerifyAgainstItself(t *testing.T) {
	key := gridKey("grid", "64")
	tamperings := map[string]func(t *testing.T, raw []byte) []byte{
		"edited parameter": func(t *testing.T, raw []byte) []byte {
			t.Helper()
			return replaceOnce(t, raw, `"value":"64"`, `"value":"65"`)
		},
		"truncated body": func(t *testing.T, raw []byte) []byte {
			t.Helper()
			return raw[:len(raw)-1]
		},
		"extended body": func(t *testing.T, raw []byte) []byte {
			t.Helper()
			return append(raw, '!')
		},
		"unknown format": func(t *testing.T, raw []byte) []byte {
			t.Helper()
			return replaceOnce(t, raw, "runjournal-record/v1", "runjournal-record/v9")
		},
		"empty file": func(*testing.T, []byte) []byte { return nil },
		"resealed edited header": func(t *testing.T, raw []byte) []byte {
			t.Helper()
			header, body, err := decodeRecord(raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			header.Params[0].Value = "65"
			resealed, err := encodeRecord(header, body)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			return resealed
		},
	}

	for name, tamper := range tamperings {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			journal := newJournal(t, dir, standSources(t, "package a", "package b"))
			if err := journal.RecordCompleted(ctx, key, []byte("measured body")); err != nil {
				t.Fatalf("record: %v", err)
			}
			path := filepath.Join(dir, key.resultName())
			raw, err := os.ReadFile(path) //nolint:gosec // a file this test wrote
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if err := os.WriteFile(path, tamper(t, raw), 0o600); err != nil {
				t.Fatalf("tamper: %v", err)
			}
			if _, err := journal.Inspect(ctx, key); !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("inspect of a tampered record = %v, want ErrCorruptRecord", err)
			}
		})
	}
}

func TestEncodedRecordRoundTrips(t *testing.T) {
	key := gridKey("grid", "64")
	header := recordHeader{
		ConfigID: key.ID(), Measurement: key.Measurement, Label: key.Label, Params: key.canonicalParams(),
		Sources: []SourceStamp{{Name: "measured", Stamp: "0011223344556677"}},
		Outcome: OutcomeCompleted, BodyBytes: 4,
	}
	raw, err := encodeRecord(header, []byte("body"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, body, err := decodeRecord(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(body) != "body" || decoded.ConfigID != header.ConfigID || decoded.Outcome != OutcomeCompleted {
		t.Fatalf("round trip lost data: %+v %q", decoded, body)
	}
}

func TestCancelledContextWritesNothing(t *testing.T) {
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := gridKey("grid", "64")

	if err := journal.RecordCompleted(ctx, key, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordCompleted under a cancelled context = %v, want context.Canceled", err)
	}
	inspection, err := journal.Inspect(context.Background(), key)
	if err != nil || inspection.Status != StatusMissing {
		t.Fatalf("inspection %+v, %v: a cancelled write left a record", inspection, err)
	}
}

func TestWritesLeaveNoPendingFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	journal := newJournal(t, dir, standSources(t, "package a", "package b"))
	key := gridKey("grid", "64")
	if err := journal.RecordFailure(ctx, key, "x"); err != nil {
		t.Fatalf("failure: %v", err)
	}
	if err := journal.RecordCompleted(ctx, key, []byte("y")); err != nil {
		t.Fatalf("completed: %v", err)
	}
	if err := journal.RecordCompleted(ctx, key, []byte("z")); !errors.Is(err, ErrAlreadyRecorded) {
		t.Fatalf("second completed = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			t.Fatalf("a temporary file %s outlived its write", entry.Name())
		}
	}
}

func moveRecord(t *testing.T, dir, from, to string) {
	t.Helper()
	if err := os.Rename(filepath.Join(dir, from), filepath.Join(dir, to)); err != nil {
		t.Fatalf("move %s to %s: %v", from, to, err)
	}
}

func replaceOnce(t *testing.T, raw []byte, old, replacement string) []byte {
	t.Helper()
	if !bytes.Contains(raw, []byte(old)) {
		t.Fatalf("the record does not contain %q", old)
	}
	return bytes.Replace(raw, []byte(old), []byte(replacement), 1)
}
