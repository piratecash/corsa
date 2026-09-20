package overlaysim

// runstore_reference_test.go pins the journal the final drivers depend on for
// being resumable at all. Every fixture here is a case where the store failing
// quietly would not break a run — it would produce a REPORT THAT LOOKS
// COMPLETE and describes something else, which is the failure mode this whole
// sweep exists to avoid.
//
// ⚠️ Mutations that must break these fixtures, named so a reader can apply them:
//
//	id derived from the label alone (or from a counter) — the per-parameter
//	    fixture stops seeing a difference;
//	writeRun stat-then-write instead of O_CREATE|O_EXCL — the concurrent
//	    fixture stops refusing;
//	writeRun allowed to replace a completed record — the same fixture;
//	inspectRun skipping on the file NAME — the mismatched-parameter, the
//	    mismatched-sources and the truncated-body fixtures all stop refusing;
//	the checksum taken over the body alone — the edited-header fixture passes;
//	the identifier believed rather than re-derived — the same;
//	the ledger folding failed into missing (or into completed) — the four-state
//	    fixture stops separating them.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// referenceKey is the fixture configuration the cases below vary one field of.
func referenceKey() runKey {
	return runKey{
		Measurement: "M6",
		Label:       "grid/A′ base",
		Params: []runParam{
			{Name: "shape", Value: "1k×8"},
			{Name: "seed", Value: "1"},
			{Name: "policy", Value: "C1/v1"},
			{Name: "branch", Value: "A′"},
			{Name: "capacity_k", Value: "4"},
		},
		Sources: "5b5c0a961c163b2e",
	}
}

func referenceRecord(outcome runOutcome, detail string) runRecord {
	return runRecord{
		Key:     referenceKey(),
		Outcome: outcome,
		Detail:  detail,
		Body:    []string{"coverage 0.83", "probes 12034", "recovery 0.77"},
	}
}

// TestRunIdentifierIsDerivedFromEveryParameter is rule 1: the identifier names
// the CONFIGURATION, so a change in any one input has to move it.
//
// ⚠️ Every parameter separately, not "the parameters together". A digest taken
// over the first two fields passes a check that varies the third only if the
// third is never varied, which is exactly how a sweep comes to skip a run it
// never made.
func TestRunIdentifierIsDerivedFromEveryParameter(t *testing.T) {
	t.Parallel()

	base := referenceKey()
	if base.ID() != referenceKey().ID() {
		t.Fatal("the same configuration hashed to two identifiers: nothing below can hold")
	}

	t.Run("each parameter moves it", func(t *testing.T) {
		t.Parallel()
		for index := range base.Params {
			changed := referenceKey()
			changed.Params[index].Value += "x"
			if changed.ID() == base.ID() {
				t.Errorf("changing %s did not change the identifier %s — a run under the new value "+
					"would be taken for a run under the old one",
					base.Params[index].Name, base.ID())
			}
		}
	})

	t.Run("the measurement and the label move it", func(t *testing.T) {
		t.Parallel()
		other := referenceKey()
		other.Measurement = "M4"
		if other.ID() == base.ID() {
			t.Error("two measurements share one identifier")
		}
		// ⚠️ The label matters even when every parameter is equal: a control and
		// a grid point can carry identical fields and still be different runs —
		// the omniscient control and its branch differ by what they are FOR.
		control := referenceKey()
		control.Label = "control/omniscient"
		if control.ID() == base.ID() {
			t.Error("a control and a grid point with identical parameters share one identifier")
		}
	})

	t.Run("the sources stamp does NOT move it", func(t *testing.T) {
		t.Parallel()
		// The identifier names the configuration; the same configuration
		// measured from other sources is that configuration measured again, and
		// it must land on the same name so the mismatch is SEEN (as a refusal)
		// rather than appearing as a run nobody has made.
		other := referenceKey()
		other.Sources = "0000000000000000"
		if other.ID() != base.ID() {
			t.Error("the sources stamp entered the identifier: a version bump would silently " +
				"create a second file for one configuration, and the first would read as missing")
		}
	})

	t.Run("the parameter ORDER moves it", func(t *testing.T) {
		t.Parallel()
		swapped := referenceKey()
		swapped.Params[0], swapped.Params[1] = swapped.Params[1], swapped.Params[0]
		if swapped.ID() == base.ID() {
			t.Error("the canonical form is order-independent, so a driver that reorders its " +
				"parameters would keep the identifier while the report's columns moved")
		}
	})
}

// TestABodyThatWouldNotReadBackIsFlattenedBeforeItIsWritten is the regression of
// the defect of 2026-09-19: the M6 driver put multi-line values into body lines
// (a played phase line, a derivation rule), the header then promised 23 result
// lines while the file physically held 26, and every one of those 23 records
// became unreadable — its own line count and its own checksum both refused it.
//
// ⚠️ The journal CAUGHT it: a resume into the same directory reported "the header
// promises 23 result lines and the body holds 26" instead of skipping quietly.
// That is the behaviour rule 3 exists for, and it is why the numbers were
// re-measured rather than repaired by hand. This fixture stops the write side
// from producing such a file at all.
func TestABodyThatWouldNotReadBackIsFlattenedBeforeItIsWritten(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	key := referenceKey()
	body := []string{
		"phases F1 [0,256)\nF2 [256,257)\nF3 [257,513)",
		"rule\twith a tab",
		"# param seed=99", // a body line dressed as a header
	}
	if err := recordOutcome(dir, key, runCompleted, "broke\nover two lines", body); err != nil {
		t.Fatalf("writing: %v", err)
	}

	record, err := readRun(filepath.Join(dir, key.FileName()))
	if err != nil {
		t.Fatalf("the record does not read back, which is the whole defect: %v", err)
	}
	if len(record.Body) != len(body) {
		t.Fatalf("wrote %d body lines and read back %d", len(body), len(record.Body))
	}
	for index, line := range record.Body {
		if strings.ContainsAny(line, "\n\r\t") {
			t.Errorf("body line %d still carries a line break or a tab: %q", index, line)
		}
	}
	if strings.HasPrefix(record.Body[2], "# ") {
		t.Error("a body line that looks like a header was written as one")
	}
	if strings.Contains(record.Detail, "\n") {
		t.Errorf("the failure detail is multi-line: %q", record.Detail)
	}
	// And the content survives: flattening must not lose what was measured.
	if !strings.Contains(record.Body[0], "F3 [257,513)") {
		t.Errorf("flattening dropped part of the line: %q", record.Body[0])
	}
	if outcome, detail := inspectRun(dir, key); outcome != runCompleted {
		t.Fatalf("the flattened record does not verify: %s (%s)", outcome, detail)
	}
}

// TestTheStampIsDerivedFromTheSourcesAndTheSourcesAreKept is the P2 of
// 2026-09-20: a version stamp typed in by the operator identifies nothing.
//
// ⚠️ Mutations that must break it: the stamp taken from a constant or from the
// clock (a change to a file stops moving it); a file left out of the digest;
// the snapshot saved without the file contents; the manifest trusted instead of
// compared when the stamp directory already exists.
func TestTheStampIsDerivedFromTheSourcesAndTheSourcesAreKept(t *testing.T) {
	t.Parallel()

	sources := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(sources, name), []byte(content), 0o600); err != nil {
			t.Fatalf("preparing %s: %v", name, err)
		}
	}
	write("a.go", "package p\n\nfunc A() {}\n")
	write("b.go", "package p\n\nfunc B() {}\n")
	write("notes.txt", "not a source file")

	first, err := takeSourceSnapshot(sources)
	if err != nil {
		t.Fatalf("stamping: %v", err)
	}
	if len(first.Files) != 2 {
		t.Fatalf("the snapshot holds %d files, want the two .go ones", len(first.Files))
	}
	again, err := takeSourceSnapshot(sources)
	if err != nil {
		t.Fatalf("stamping again: %v", err)
	}
	if first.Stamp != again.Stamp {
		t.Fatalf("the same sources stamped twice as %s and %s", first.Stamp, again.Stamp)
	}

	t.Run("a change in any file moves the stamp", func(t *testing.T) {
		write("b.go", "package p\n\nfunc B() { _ = 1 }\n")
		changed, err := takeSourceSnapshot(sources)
		if err != nil {
			t.Fatalf("stamping: %v", err)
		}
		if changed.Stamp == first.Stamp {
			t.Fatal("an edited source file left the stamp where it was: a batch measured with " +
				"other code would carry the old version and the comparison would pass")
		}
		write("b.go", "package p\n\nfunc B() {}\n") // back, for the cases below
	})

	t.Run("a saved snapshot is verified by its FILES, not by its listing", func(t *testing.T) {
		// ⚠️ P2 of 2026-09-20: a matching MANIFEST.sha256 says what the snapshot
		// CLAIMS to hold, and a snapshot whose .go file was deleted or edited
		// afterwards claims exactly the same thing.
		journal := t.TempDir()
		saved, err := first.save(journal)
		if err != nil {
			t.Fatalf("saving: %v", err)
		}

		for _, damage := range []struct {
			name string
			do   func()
			want string
		}{
			{
				name: "a kept file was edited, the listing untouched",
				do: func() {
					if err := os.WriteFile(filepath.Join(saved, "a.go"),
						[]byte("package p\n\nfunc A() { panic(1) }\n"), 0o600); err != nil {
						t.Fatalf("editing: %v", err)
					}
				},
				want: "is not the code that was measured",
			},
			{
				name: "a kept file was removed, the listing untouched",
				do: func() {
					if err := os.Remove(filepath.Join(saved, "a.go")); err != nil {
						t.Fatalf("removing: %v", err)
					}
				},
				want: "is missing a.go",
			},
			{
				name: "a file nobody listed was added",
				do: func() {
					if err := os.WriteFile(filepath.Join(saved, "extra.go"),
						[]byte("package p\n"), 0o600); err != nil {
						t.Fatalf("adding: %v", err)
					}
				},
				want: "which its listing does not name",
			},
		} {
			t.Run(damage.name, func(t *testing.T) {
				// Each case starts from a good snapshot of its own.
				own := t.TempDir()
				fresh, err := first.save(own)
				if err != nil {
					t.Fatalf("saving: %v", err)
				}
				saved = fresh
				damage.do()
				_, err = first.save(own)
				if err == nil {
					t.Fatalf("a snapshot where %s was accepted: the listing was believed over "+
						"the files", damage.name)
				}
				if !strings.Contains(err.Error(), damage.want) {
					t.Errorf("the refusal does not say what is wrong (%q is not in %q)",
						damage.want, err)
				}
			})
		}
	})

	t.Run("the copy comes from the bytes that were hashed", func(t *testing.T) {
		// ⚠️ P2 of 2026-09-20: the previous version hashed the files and then
		// read them AGAIN to copy them, so an edit between the two reads
		// produced a snapshot whose listing and whose files disagreed. Here the
		// working tree is changed after the snapshot is taken and before it is
		// saved; what lands must still be what was hashed.
		own := t.TempDir()
		taken, err := takeSourceSnapshot(sources)
		if err != nil {
			t.Fatalf("stamping: %v", err)
		}
		write("a.go", "package p\n\nfunc A() { /* edited after the snapshot */ }\n")
		defer write("a.go", "package p\n\nfunc A() {}\n")

		saved, err := taken.save(own)
		if err != nil {
			t.Fatalf("saving after the working tree moved: %v", err)
		}
		kept, err := os.ReadFile(filepath.Join(saved, "a.go"))
		if err != nil {
			t.Fatalf("reading the kept copy: %v", err)
		}
		if string(kept) != "package p\n\nfunc A() {}\n" {
			t.Errorf("the snapshot saved the EDITED file (%q): its listing and its files would "+
				"disagree, and the listing is what the stamp comes from", kept)
		}
	})

	t.Run("the snapshot keeps the code, not only its digest", func(t *testing.T) {
		journal := t.TempDir()
		saved, err := first.save(journal)
		if err != nil {
			t.Fatalf("saving: %v", err)
		}
		if filepath.Base(saved) != first.Stamp {
			t.Errorf("the snapshot went to %q, which is not named for the stamp %s", saved, first.Stamp)
		}
		kept, err := os.ReadFile(filepath.Join(saved, "a.go"))
		if err != nil {
			t.Fatalf("the snapshot does not hold the source: %v", err)
		}
		if string(kept) != "package p\n\nfunc A() {}\n" {
			t.Errorf("the kept copy differs from what was measured: %q", kept)
		}
		// Idempotent: the same sources saved again find their snapshot there.
		if _, err := first.save(journal); err != nil {
			t.Errorf("saving the same snapshot twice: %v", err)
		}
		// ⚠️ And an edited snapshot directory is REFUSED, not trusted: it is
		// named for its own content, so the two can only disagree by hand.
		if err := os.WriteFile(filepath.Join(saved, "MANIFEST.sha256"), []byte("# tampered\n"), 0o600); err != nil {
			t.Fatalf("tampering: %v", err)
		}
		if _, err := first.save(journal); err == nil {
			t.Error("a snapshot directory whose listing no longer matches its stamp was accepted")
		}
	})
}

// TestARetryCannotLoseARaceWithASuccess is the P2 of 2026-09-20: the old
// failed-then-replace path read a status and then opened the name with O_TRUNC,
// so two processes could both see `failed`, one write a result and the other
// truncate it.
func TestARetryCannotLoseARaceWithASuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	key := referenceKey()

	// A failure is recorded first — the state that used to unlock replacement.
	if err := recordOutcome(dir, key, runFailed, "the graph could not be built", nil); err != nil {
		t.Fatalf("recording the failure: %v", err)
	}
	if outcome, detail := inspectRun(dir, key); outcome != runFailed ||
		!strings.Contains(detail, "could not be built") {
		t.Fatalf("a recorded attempt reads as %s (%s)", outcome, detail)
	}

	// Retry A wins and records the result.
	if err := recordOutcome(dir, key, runCompleted, "", []string{"coverage 0.83"}); err != nil {
		t.Fatalf("the retry was refused: %v", err)
	}

	// ⚠️ Retry B, which read the SAME failure before A ran, now tries to write.
	// It must be refused — and refused in a way its caller can recognise, so the
	// sweep counts the configuration as completed rather than failing the grid.
	err := recordOutcome(dir, key, runCompleted, "", []string{"coverage 0.11"})
	if err == nil {
		t.Fatal("a second retry replaced a completed result: this is the race the attempt files " +
			"exist to remove")
	}
	if !errors.Is(err, errRunExists) {
		t.Errorf("the refusal is not recognisable as one (%v), so a caller cannot tell it from a "+
			"stand defect", err)
	}

	record, readErr := readRun(filepath.Join(dir, key.FileName()))
	if readErr != nil {
		t.Fatalf("reading the surviving result: %v", readErr)
	}
	if strings.Join(record.Body, "|") != "coverage 0.83" {
		t.Fatalf("the surviving result is %q — the loser overwrote the winner", record.Body)
	}
	if outcome, _ := inspectRun(dir, key); outcome != runCompleted {
		t.Errorf("with a result on disk the configuration reads as %s", outcome)
	}

	t.Run("every attempt is kept, and they do not collide", func(t *testing.T) {
		other := t.TempDir()
		attempted := referenceKey()
		for range 3 {
			if err := recordOutcome(other, attempted, runFailed, "broke again", nil); err != nil {
				t.Fatalf("recording an attempt: %v", err)
			}
		}
		files, err := filepath.Glob(filepath.Join(other, attempted.failureGlob()))
		if err != nil {
			t.Fatalf("listing the attempts: %v", err)
		}
		if len(files) != 3 {
			t.Fatalf("%d attempt files for three attempts: a retry overwrote an earlier one",
				len(files))
		}
		outcome, detail := inspectRun(other, attempted)
		if outcome != runFailed || !strings.Contains(detail, "3 attempts") {
			t.Errorf("three recorded attempts read as %s (%s)", outcome, detail)
		}
	})
}

// TestAConcurrentRecordIsVerifiedBeforeItIsBelieved is the P2 of 2026-09-20:
// losing the create race says another process made the NAME exist, and nothing
// about what is in it.
//
// ⚠️ Mutation that must break it: counting the configuration completed on
// errRunExists alone.
func TestAConcurrentRecordIsVerifiedBeforeItIsBelieved(t *testing.T) {
	t.Parallel()

	t.Run("a rival's finished record of the same version is accepted", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if err := recordOutcome(dir, referenceKey(), runCompleted, "", []string{"coverage 0.83"}); err != nil {
			t.Fatalf("the rival's write: %v", err)
		}
		if err := confirmConcurrentRecord(dir, referenceKey()); err != nil {
			t.Errorf("a verifying record of the same configuration was refused: %v", err)
		}
	})

	t.Run("a rival still writing is NOT a result", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// The file exists and the body has not arrived yet: exactly what a
		// create-then-write loses the race to.
		partial := renderRun(referenceRecord(runCompleted, ""))
		if index := strings.Index(partial, "# checksum="); index > 0 {
			partial = partial[:index]
		}
		if err := os.WriteFile(filepath.Join(dir, referenceKey().FileName()), []byte(partial), 0o600); err != nil {
			t.Fatalf("planting the half-written record: %v", err)
		}
		err := confirmConcurrentRecord(dir, referenceKey())
		if err == nil {
			t.Fatal("a half-written record was counted as a completed result")
		}
		if !strings.Contains(err.Error(), "does NOT verify") {
			t.Errorf("the refusal does not say the record failed to verify: %v", err)
		}
		if !strings.Contains(err.Error(), "still finishing") {
			t.Errorf("the refusal does not tell the operator that a writer may simply be in "+
				"flight: %v", err)
		}
	})

	t.Run("a rival of ANOTHER VERSION is not this call's result", func(t *testing.T) {
		t.Parallel()
		// ⚠️ The sources stamp is not part of the file NAME, so a process
		// running other code lands on exactly this name. Only the comparison
		// inside the record catches it.
		dir := t.TempDir()
		rival := referenceKey()
		rival.Sources = "0f0f0f0f0f0f0f0f"
		if rival.FileName() != referenceKey().FileName() {
			t.Fatal("the stamp entered the file name, so this fixture no longer exercises the case")
		}
		if err := recordOutcome(dir, rival, runCompleted, "", []string{"coverage 0.83"}); err != nil {
			t.Fatalf("the rival's write: %v", err)
		}
		err := confirmConcurrentRecord(dir, referenceKey())
		if err == nil {
			t.Fatal("a record produced by another version was counted as this call's result")
		}
		for _, want := range []string{"0f0f0f0f0f0f0f0f", "5b5c0a961c163b2e"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not name the stamp %s: %v", want, err)
			}
		}
	})

	// ⚠️ The loop's own path is exercised POSITIVELY here: the rival appears
	// between the initial inspect and the write — the only moment at which this
	// can happen at all — writes a verifying record, and the sweep keeps it and
	// counts the configuration as resumed rather than measured.
	//
	// The REFUSAL is checked at the level of confirmConcurrentRecord above, and
	// deliberately not through runSweep: the loop reports a refusal with
	// t.Fatalf, and a fixture that drove it would have to fake a *testing.T.
	// What matters is that the loop consults the function — which it does, and
	// this case shows the consultation happening.
	t.Run("the sweep keeps a rival's verifying record and counts it as resumed", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		key := referenceKey()
		key.Sources = "stamp"

		var out strings.Builder
		selection := runSelection{Prefix: "T", Dir: dir, Sources: "stamp", Keeping: true,
			From: 0, To: -1}
		ledger := runSweep(t, selection, []runKey{key}, &out,
			func(int, runKey) ([]string, string, error) {
				if err := recordOutcome(dir, key, runCompleted, "", []string{"the rival's"}); err != nil {
					t.Fatalf("the rival's write: %v", err)
				}
				return []string{"mine"}, "done", nil
			})

		if len(ledger.Completed) != 1 || len(ledger.Skipped) != 1 {
			t.Fatalf("completed %d, of them from disk %d; want the configuration counted once and "+
				"as resumed", len(ledger.Completed), len(ledger.Skipped))
		}
		if !strings.Contains(out.String(), "that record verifies and is kept") {
			t.Errorf("the log does not say the rival's record was verified:\n%s", out.String())
		}
		record, err := readRun(filepath.Join(dir, key.FileName()))
		if err != nil {
			t.Fatalf("reading what survived: %v", err)
		}
		if strings.Join(record.Body, "|") != "the rival's" {
			t.Errorf("the surviving record is %q — the loser overwrote the winner", record.Body)
		}
	})
}

// TestARunRoundTripsThroughItsFile is the plain property everything else stands
// on: what was written is what is read back.
func TestARunRoundTripsThroughItsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	written := referenceRecord(runCompleted, "")
	path, err := writeRun(dir, written)
	if err != nil {
		t.Fatalf("writing: %v", err)
	}
	if got := filepath.Base(path); !strings.Contains(got, written.Key.ID()) {
		t.Errorf("the file is called %q and does not carry the identifier %s — it cannot be found "+
			"from a command line", got, written.Key.ID())
	}

	read, err := readRun(path)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if read.Outcome != runCompleted {
		t.Errorf("read back as %s, written as %s", read.Outcome, written.Outcome)
	}
	if read.Key.ID() != written.Key.ID() {
		t.Errorf("read back as configuration %s, written as %s", read.Key.ID(), written.Key.ID())
	}
	if read.Key.Sources != written.Key.Sources {
		t.Errorf("read back from sources %q, written from %q", read.Key.Sources, written.Key.Sources)
	}
	if strings.Join(read.Body, "|") != strings.Join(written.Body, "|") {
		t.Errorf("the body came back as %q, written as %q", read.Body, written.Body)
	}
	if outcome, detail := inspectRun(dir, referenceKey()); outcome != runCompleted {
		t.Errorf("inspectRun says %s (%s) about a run it should recognise", outcome, detail)
	}
}

// TestACompletedRunIsNeverOverwritten is rule 2, and it is the rule that cost a
// presented measurement in the M2 incident of 2026-09-19.
func TestACompletedRunIsNeverOverwritten(t *testing.T) {
	t.Parallel()

	t.Run("a second write of a completed run is refused", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := writeRun(dir, referenceRecord(runCompleted, "")); err != nil {
			t.Fatalf("first write: %v", err)
		}
		second := referenceRecord(runCompleted, "")
		second.Body = []string{"coverage 0.11"} // a DIFFERENT result under the same key
		if _, err := writeRun(dir, second); err == nil {
			t.Fatal("a completed result was replaced: an accidental rerun destroys evidence that " +
				"has already been presented")
		}
		// And the first result is still there, unchanged.
		read, err := readRun(filepath.Join(dir, referenceKey().FileName()))
		if err != nil {
			t.Fatalf("reading after the refused write: %v", err)
		}
		if strings.Join(read.Body, "|") != "coverage 0.83|probes 12034|recovery 0.77" {
			t.Fatalf("the refused write still changed the file: %q", read.Body)
		}
	})

	// ⚠️ A failure no longer shares a name with a result, so a retry does not
	// replace anything — it creates the result for the first time. That is what
	// removed the race of 2026-09-20 (TestARetryCannotLoseARaceWithASuccess);
	// here the point is only that a recorded failure does not BLOCK the retry.
	t.Run("a recorded failure does not block the retry, and is kept beside it", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := writeRun(dir, referenceRecord(runFailed, "the graph could not be built")); err != nil {
			t.Fatalf("writing the failure: %v", err)
		}
		if _, err := writeRun(dir, referenceRecord(runCompleted, "")); err != nil {
			t.Fatalf("a retry of a failed configuration was refused: %v", err)
		}
		if outcome, _ := inspectRun(dir, referenceKey()); outcome != runCompleted {
			t.Fatalf("after the retry the run reads as %s", outcome)
		}
		// The attempt stays on disk: what broke is evidence of its own.
		attempts, err := filepath.Glob(filepath.Join(dir, referenceKey().failureGlob()))
		if err != nil || len(attempts) != 1 {
			t.Errorf("the failed attempt was removed by the retry (%v, %d files)", err, len(attempts))
		}
	})
}

// TestARunIsSkippedOnlyAfterItIsVerified is rule 3, case by case. Each case
// plants a file that a name-based skip would accept.
func TestARunIsSkippedOnlyAfterItIsVerified(t *testing.T) {
	t.Parallel()

	// plant writes a file under the EXPECTED name whose contents are produced by
	// `spoil` — that is, the exact shape of an accident or an edit.
	plant := func(t *testing.T, spoil func(string) string) string {
		t.Helper()
		dir := t.TempDir()
		path := filepath.Join(dir, referenceKey().FileName())
		text := renderRun(referenceRecord(runCompleted, ""))
		if err := os.WriteFile(path, []byte(spoil(text)), 0o600); err != nil {
			t.Fatalf("planting: %v", err)
		}
		return dir
	}

	for _, fixture := range []struct {
		name  string
		spoil func(string) string
		want  string
	}{
		{
			// A hand-edited parameter leaves the identifier and the checksum
			// describing the old value, so the file is internally inconsistent
			// and is refused before anything is compared with the expectation.
			name: "a parameter was edited in place",
			spoil: func(text string) string {
				return strings.Replace(text, "# param seed=1", "# param seed=2", 1)
			},
			want: "the header was edited",
		},
		{
			name: "the sources stamp was edited in place",
			spoil: func(text string) string {
				return strings.Replace(text, "# sources=5b5c0a961c163b2e", "# sources=deadbeefdeadbeef", 1)
			},
			want: "checksum",
		},
		{
			name: "the body is truncated",
			spoil: func(text string) string {
				return strings.Replace(text, "recovery 0.77\n", "", 1)
			},
			want: "truncated",
		},
		{
			name: "a result line was edited",
			spoil: func(text string) string {
				return strings.Replace(text, "coverage 0.83", "coverage 0.99", 1)
			},
			want: "checksum",
		},
		{
			name: "the format tag is wrong",
			spoil: func(text string) string {
				return strings.Replace(text, "# "+runRecordFormat, "# overlaysim-run/v0", 1)
			},
			want: runRecordFormat,
		},
		{
			name: "the run never said whether it finished",
			spoil: func(text string) string {
				return strings.Replace(text, "# status=completed\n", "", 1)
			},
			want: "whether the run finished",
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			dir := plant(t, fixture.spoil)
			outcome, detail := inspectRun(dir, referenceKey())
			if outcome != runUnreadable {
				t.Fatalf("a file that %s was accepted as %s — a run under other conditions would "+
					"be skipped as this one", fixture.name, outcome)
			}
			if !strings.Contains(detail, fixture.want) {
				t.Errorf("the refusal does not name what is wrong (%q is not in %q): an operator "+
					"cannot act on it", fixture.want, detail)
			}
		})
	}

	// ⚠️ The hardest case: parameters edited AND the checksum recomputed over
	// them, so the file is internally consistent and simply describes another
	// configuration. Only re-deriving the identifier catches it.
	t.Run("a consistent file of other parameters, under the expected name", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		other := referenceKey()
		other.Params[1].Value = "4" // seed 4, not 1
		consistent := runRecord{Key: other, Outcome: runCompleted, Body: []string{"coverage 0.83"}}
		if err := os.WriteFile(filepath.Join(dir, referenceKey().FileName()),
			[]byte(renderRun(consistent)), 0o600); err != nil {
			t.Fatalf("planting: %v", err)
		}
		outcome, detail := inspectRun(dir, referenceKey())
		if outcome != runUnreadable {
			t.Fatalf("a self-consistent record of seed 4 was accepted as seed 1 (%s): the file "+
				"name was believed over the file", outcome)
		}
		if !strings.Contains(detail, "identif") && !strings.Contains(detail, "seed") {
			t.Errorf("the refusal explains nothing: %q", detail)
		}
	})

	// ⚠️ The realistic version of a version mismatch: a file written correctly,
	// by an earlier sweep, from OTHER SOURCES. Nothing about it is malformed —
	// only the comparison with what is being asked for can refuse it, and the
	// refusal has to name both stamps or the operator cannot act on it.
	t.Run("a well-formed record made from other sources", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		older := referenceRecord(runCompleted, "")
		older.Key.Sources = "0f0f0f0f0f0f0f0f"
		if _, err := writeRun(dir, older); err != nil {
			t.Fatalf("writing the older record: %v", err)
		}
		outcome, detail := inspectRun(dir, referenceKey())
		if outcome != runUnreadable {
			t.Fatalf("a run from other sources was accepted as %s: the numbers of two versions "+
				"would be presented as one measurement", outcome)
		}
		for _, want := range []string{"0f0f0f0f0f0f0f0f", "5b5c0a961c163b2e"} {
			if !strings.Contains(detail, want) {
				t.Errorf("the refusal does not name the stamp %s: %q", want, detail)
			}
		}
	})

	t.Run("a file nobody wrote is missing, not unreadable", func(t *testing.T) {
		t.Parallel()
		if outcome, _ := inspectRun(t.TempDir(), referenceKey()); outcome != runMissing {
			t.Fatalf("an empty directory reports %s", outcome)
		}
	})
}

// TestTheLedgerSeparatesTheFourStates is rule 4. The arithmetic is the whole
// claim: expected = completed + failed + missing, with the resumed part of
// completed shown apart.
func TestTheLedgerSeparatesTheFourStates(t *testing.T) {
	t.Parallel()

	key := func(label string) runKey {
		made := referenceKey()
		made.Label = label
		return made
	}
	ledger := newRunLedger()
	for _, label := range []string{"a", "b", "c", "d", "e"} {
		ledger.expect(key(label))
	}
	ledger.note(key("a"), runCompleted, "", false)
	ledger.note(key("b"), runCompleted, "", true) // resumed from disk
	ledger.note(key("c"), runFailed, "the sample could not be drawn", false)
	ledger.note(key("d"), runMissing, "", false)
	ledger.note(key("e"), runMissing, "", false)

	if got := len(ledger.Completed); got != 2 {
		t.Errorf("completed %d, want 2", got)
	}
	if got := len(ledger.Skipped); got != 1 {
		t.Errorf("resumed from disk %d, want 1 — the work a resume saved is its own number", got)
	}
	if got := len(ledger.Failed); got != 1 {
		t.Errorf("failed %d, want 1: a failure folded into missing loses the only record that it "+
			"was tried at all", got)
	}
	if got := len(ledger.Missing); got != 2 {
		t.Errorf("missing %d, want 2", got)
	}
	if ledger.Complete() {
		t.Error("a ledger with two missing runs reports itself complete")
	}

	summary := ledger.Summary()
	for _, want := range []string{"expected 5", "completed 2", "failed 1", "missing 2",
		"the sample could not be drawn"} {
		if !strings.Contains(summary, want) {
			t.Errorf("the summary does not say %q:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "STAND DEFECT") {
		t.Errorf("the arithmetic of a well-formed ledger reports a defect:\n%s", summary)
	}
}

// TestTheSummaryCountsTheWholeSetAndNotThisCallsSlice is the P2 of 2026-09-20,
// as the scenario that exposed it: a grid run batch by batch, whose last batch
// reported every finished batch as missing.
//
// ⚠️ Mutation that must break it: marking everything outside the selection
// missing without consulting the journal.
func TestTheSummaryCountsTheWholeSetAndNotThisCallsSlice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keys := make([]runKey, 0, 6)
	for _, label := range []string{"a", "b", "c", "d", "e", "f"} {
		made := referenceKey()
		made.Label = label
		made.Sources = "stamp"
		keys = append(keys, made)
	}

	batch := func(from, to int) *runLedger {
		t.Helper()
		var out strings.Builder
		selection := runSelection{Prefix: "T", Dir: dir, Sources: "stamp", Keeping: true,
			From: from, To: to}
		return runSweep(t, selection, keys, &out,
			func(index int, key runKey) ([]string, string, error) {
				return []string{"measured " + key.Label}, "done", nil
			})
	}

	first := batch(0, 2)
	if len(first.Completed) != 2 || len(first.Missing) != 4 {
		t.Fatalf("after the first batch: completed %d, missing %d, want 2 and 4",
			len(first.Completed), len(first.Missing))
	}

	// ⚠️ The second batch measures c and d — and must still SEE a and b.
	second := batch(2, 4)
	if len(second.Completed) != 4 {
		t.Fatalf("after the second batch the journal holds 4 results and the summary counts %d: "+
			"the batches that finished earlier were reported as missing",
			len(second.Completed))
	}
	if len(second.Skipped) != 2 {
		t.Errorf("%d results were recognised as already on disk, want the 2 of the first batch",
			len(second.Skipped))
	}
	if len(second.Missing) != 2 {
		t.Errorf("missing %d after two batches, want the 2 not yet run", len(second.Missing))
	}
	if len(second.NotSelected) != 4 {
		t.Errorf("%d configurations were outside this call's selection, want 4", len(second.NotSelected))
	}

	// The last batch finishes the set, and the summary says so.
	third := batch(4, 6)
	if !third.Complete() {
		t.Fatalf("after every batch the ledger still reports %d missing", len(third.Missing))
	}
	summary := third.Summary()
	if !strings.Contains(summary, "completed 6") || strings.Contains(summary, "MISSING") {
		t.Errorf("the finished grid does not read as finished:\n%s", summary)
	}
	if !strings.Contains(summary, "outside its selection") {
		t.Errorf("the summary does not separate what this call did from what the journal holds:\n%s",
			summary)
	}
}

// TestAnEnumerationMayNotCollide is the assertion every driver owes before it
// runs anything: two configurations sharing an identifier share a file, and the
// second would be taken for the first — that is, skipped.
func TestAnEnumerationMayNotCollide(t *testing.T) {
	t.Parallel()

	good := []runKey{referenceKey(), func() runKey { k := referenceKey(); k.Label = "other"; return k }()}
	if err := requireDistinctKeys(good); err != nil {
		t.Fatalf("two distinct configurations were called a collision: %v", err)
	}
	if err := requireDistinctKeys([]runKey{referenceKey(), referenceKey()}); err == nil {
		t.Fatal("a repeated configuration passed: the second run would be skipped as the first")
	}
}

// TestTheSelectionCutsTheEnumerationWithoutChangingIt pins what makes a sweep
// runnable inside a call that is capped at ≈180 s: a range and a name, over an
// order that does not move.
func TestTheSelectionCutsTheEnumerationWithoutChangingIt(t *testing.T) {
	t.Parallel()

	keys := make([]runKey, 0, 6)
	for _, label := range []string{"a", "b", "c", "d", "e", "f"} {
		made := referenceKey()
		made.Label = label
		keys = append(keys, made)
	}

	t.Run("a range takes a half-open slice", func(t *testing.T) {
		t.Parallel()
		selection := runSelection{From: 2, To: 4}
		var taken []string
		for index, key := range keys {
			if selection.selects(index, key) {
				taken = append(taken, key.Label)
			}
		}
		if strings.Join(taken, "") != "cd" {
			t.Errorf("range 2-4 took %v, want c and d", taken)
		}
	})

	t.Run("a name takes exactly that configuration", func(t *testing.T) {
		t.Parallel()
		selection := runSelection{From: 0, To: -1, Only: []string{keys[3].ID()}}
		var taken []string
		for index, key := range keys {
			if selection.selects(index, key) {
				taken = append(taken, key.Label)
			}
		}
		if strings.Join(taken, "") != "d" {
			t.Errorf("selecting by identifier took %v, want d only", taken)
		}
	})

	t.Run("a label selects too, because an identifier is unreadable by hand", func(t *testing.T) {
		t.Parallel()
		selection := runSelection{From: 0, To: -1, Only: []string{"f"}}
		if !selection.selects(5, keys[5]) || selection.selects(0, keys[0]) {
			t.Error("selecting by label did not take exactly the labelled configuration")
		}
	})

	t.Run("no selection takes everything", func(t *testing.T) {
		t.Parallel()
		selection := runSelection{From: 0, To: -1}
		for index, key := range keys {
			if !selection.selects(index, key) {
				t.Fatalf("an unselected sweep dropped %s", key.Label)
			}
		}
	})
}

// TestStrangersInTheDirectoryAreReportedAndNotRemoved: a directory carrying runs
// of an older enumeration is a directory whose totals mean nothing, so the fact
// is surfaced — and the files are left alone, because the store cannot know
// which of the two grids is the one worth keeping.
func TestStrangersInTheDirectoryAreReportedAndNotRemoved(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	expected := referenceKey()
	if _, err := writeRun(dir, referenceRecord(runCompleted, "")); err != nil {
		t.Fatalf("writing the expected run: %v", err)
	}
	stale := referenceKey()
	stale.Label = "grid/an older configuration"
	if _, err := writeRun(dir, runRecord{Key: stale, Outcome: runCompleted, Body: []string{"x"}}); err != nil {
		t.Fatalf("writing the stale run: %v", err)
	}

	strangers, err := strangersInDirectory(dir, []runKey{expected})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(strangers) != 1 || !strings.Contains(strangers[0], stale.ID()) {
		t.Fatalf("the stale run was not reported as a stranger: %v", strangers)
	}
	if _, err := os.Stat(filepath.Join(dir, stale.FileName())); err != nil {
		t.Fatalf("the stranger was removed: %v", err)
	}
}
