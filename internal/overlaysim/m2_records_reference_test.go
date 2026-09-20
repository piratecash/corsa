package overlaysim

// m2_records_reference_test.go holds the claim the record file exists to make:
// that a finished M2 run, saved and loaded back, answers EVERY hop limit exactly
// as the live run would have.
//
// ⚠️ The claim is checked against the live reports, not against a second copy of
// the file. A round trip compared with itself proves that writing and reading
// agree, which is true of any pair of broken halves.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// m2RecordFixture is one real measurement, small enough to run in a unit test
// and real enough to contain every outcome the format must carry.
func m2RecordFixture(t *testing.T) (m2Records, routingReport, routingReport) {
	t.Helper()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	const (
		seed  = uint64(1)
		quota = 1
		want  = 200
	)

	g := buildGraph(sh, seed, quota, policyCandidateC1)
	sample, err := drawM2Pairs(g, want, seed)
	if err != nil {
		t.Fatalf("drawing the sample: %v", err)
	}
	if len(sample.Pairs) == 0 {
		t.Fatal("the fixture drew no pairs, so it proves nothing")
	}

	everyone := func(int32) bool { return true }
	structural := func(i int32) bool { return g.roles[i] == roleStructural }

	full, err := measureRouting(g, everyone, referenceComponents(g, everyone), sample.Pairs, noBudget)
	if err != nil {
		t.Fatalf("full graph: %v", err)
	}
	half, err := measureRouting(g, structural, referenceComponents(g, structural), sample.Pairs, noBudget)
	if err != nil {
		t.Fatalf("structural half: %v", err)
	}

	records := m2Records{
		Key: m2RunKey{
			Shape: sh.name, Nodes: sh.nodes, Degree: sh.degree, Budget: sh.budget,
			Policy: policyCandidateC1.String(), Quota: quota, Seed: seed,
			PairsRequested: want, PairSeed: seed, Eligible: sample.Eligible,
			Sources: "fixture",
		},
		Pairs: pairRecordsOf(sample, full, half),
	}
	return records, full, half
}

func sameResults(a, b routingResult) bool {
	return a.Outcome == b.Outcome && a.Hops == b.Hops && a.Stopped == b.Stopped
}

func compareReports(t *testing.T, what string, want, got routingReport) {
	t.Helper()

	if len(want.ByPair) != len(got.ByPair) {
		t.Fatalf("%s: %d pairs against %d", what, len(want.ByPair), len(got.ByPair))
	}
	for index := range want.ByPair {
		if !sameResults(want.ByPair[index], got.ByPair[index]) {
			t.Fatalf("%s: pair %d is %v/%d hops/stopped %d in the run and %v/%d hops/stopped %d "+
				"after the round trip", what, index,
				want.ByPair[index].Outcome, want.ByPair[index].Hops, want.ByPair[index].Stopped,
				got.ByPair[index].Outcome, got.ByPair[index].Hops, got.ByPair[index].Stopped)
		}
	}
	if want.Lengths != got.Lengths {
		t.Fatalf("%s: lengths %v against %v", what, want.Lengths, got.Lengths)
	}
	for _, outcome := range []routingOutcome{
		routingSuccess, routingDeadEnd, routingNoPath, routingBudgetSpent,
	} {
		if want.Outcomes[outcome] != got.Outcomes[outcome] {
			t.Fatalf("%s: %v counted %d times in the run and %d after the round trip",
				what, outcome, want.Outcomes[outcome], got.Outcomes[outcome])
		}
	}
}

// TestM2RecordsRecomputeEveryLimitAsTheRunWould is the acceptance.
func TestM2RecordsRecomputeEveryLimitAsTheRunWould(t *testing.T) {
	t.Parallel()

	records, full, half := m2RecordFixture(t)

	path, err := writeM2Records(t.TempDir(), records)
	if err != nil {
		t.Fatalf("writing the records: %v", err)
	}
	loaded, err := readM2Records(path)
	if err != nil {
		t.Fatalf("reading them back: %v", err)
	}

	if loaded.Key != records.Key {
		t.Fatalf("the key changed in the round trip:\n  wrote %+v\n  read  %+v",
			records.Key, loaded.Key)
	}

	rebuiltFull, rebuiltHalf := reportsFromRecords(loaded)
	compareReports(t, "full graph, unlimited", full, rebuiltFull)
	compareReports(t, "structural half, unlimited", half, rebuiltHalf)

	// Every limit from zero to past the longest walk, because the interesting
	// values are the boundaries: exactly enough, and one short.
	longest := 0
	for _, result := range full.ByPair {
		longest = max(longest, result.Hops)
	}
	for _, result := range half.ByPair {
		longest = max(longest, result.Hops)
	}

	for limit := 0; limit <= longest+2; limit++ {
		wantFull, errWant := underHopLimit(full, limit)
		if errWant != nil {
			t.Fatalf("L=%d on the live full report: %v", limit, errWant)
		}
		gotFull, errGot := underHopLimit(rebuiltFull, limit)
		if errGot != nil {
			t.Fatalf("L=%d on the loaded full report: %v", limit, errGot)
		}
		compareReports(t, fmt.Sprintf("full graph, L=%d", limit), wantFull, gotFull)

		wantHalf, errWant := underHopLimit(half, limit)
		if errWant != nil {
			t.Fatalf("L=%d on the live half report: %v", limit, errWant)
		}
		gotHalf, errGot := underHopLimit(rebuiltHalf, limit)
		if errGot != nil {
			t.Fatalf("L=%d on the loaded half report: %v", limit, errGot)
		}
		compareReports(t, fmt.Sprintf("structural half, L=%d", limit), wantHalf, gotHalf)

		// M2-L and M2-G are what the report prints, so the comparison of the two
		// graphs is checked at every limit too — not only the two reports apart.
		if live, loadedPair := compareLengths(wantFull, wantHalf),
			compareLengths(gotFull, gotHalf); live != loadedPair {
			t.Fatalf("L=%d: compared lengths %+v against %+v", limit, live, loadedPair)
		}
	}
}

// TestM2RecordsWithoutHopsOrStoppedCannotDoIt is the negative control: it shows
// that the two columns an aggregate-only log would have dropped are exactly the
// ones the recomputation needs. Without it, "the records are sufficient" would
// be a claim about columns nobody tried to remove.
func TestM2RecordsWithoutHopsOrStoppedCannotDoIt(t *testing.T) {
	t.Parallel()

	records, full, _ := m2RecordFixture(t)

	t.Run("hops dropped", func(t *testing.T) {
		mutated := m2Records{Key: records.Key, Pairs: make([]m2PairRecord, len(records.Pairs))}
		copy(mutated.Pairs, records.Pairs)
		for index := range mutated.Pairs {
			mutated.Pairs[index].Full.Hops = 0
		}

		rebuilt, _ := reportsFromRecords(mutated)
		limit := 2
		wantLimited, err := underHopLimit(full, limit)
		if err != nil {
			t.Fatalf("live: %v", err)
		}
		gotLimited, err := underHopLimit(rebuilt, limit)
		if err != nil {
			// A no-path pair carrying hops is refused outright, which is also a
			// detection — the point is that it does not silently agree.
			return
		}
		if wantLimited.Outcomes[routingBudgetSpent] == gotLimited.Outcomes[routingBudgetSpent] &&
			wantLimited.Lengths == gotLimited.Lengths {
			t.Fatal("zeroing the hop column changed nothing at L=2 — then the column is not " +
				"what the limit is applied to, and the format is proving the wrong thing")
		}
	})

	t.Run("stopped dropped", func(t *testing.T) {
		mutated := m2Records{Key: records.Key, Pairs: make([]m2PairRecord, len(records.Pairs))}
		copy(mutated.Pairs, records.Pairs)
		for index := range mutated.Pairs {
			mutated.Pairs[index].Full.Stopped = -1
		}

		rebuilt, _ := reportsFromRecords(mutated)
		differs := false
		for index := range full.ByPair {
			if !sameResults(full.ByPair[index], rebuilt.ByPair[index]) {
				differs = true
				break
			}
		}
		if !differs {
			t.Fatal("dropping the stopping node changed nothing — then an untouched pair is not " +
				"reproduced exactly, and the format is missing a column it claims to carry")
		}
	})
}

// TestM2RecordsOnDiskRecomputeALimit is the check on the REAL output of a
// sweep, not on a fixture this file wrote a moment ago.
//
// ⚠️ It runs only when M2_RECORDS names a directory, because there is nothing to
// check otherwise — and it says so rather than passing silently. What it proves
// is the property the P2 asked for: a saved run can be loaded back and a hop
// limit applied to it, file by file.
func TestM2RecordsOnDiskRecomputeALimit(t *testing.T) {
	t.Parallel()

	dir, keeping := m2RecordsDestination()
	if !keeping {
		t.Skip("M2_RECORDS is unset: no saved run to load")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}

	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tsv") {
			continue
		}
		path := filepath.Join(dir, entry.Name())

		records, readErr := readM2Records(path)
		if readErr != nil {
			t.Fatalf("%s: %v", entry.Name(), readErr)
		}
		if len(records.Pairs) == 0 {
			t.Fatalf("%s holds no pairs", entry.Name())
		}
		if records.Key.Sources == "" {
			t.Fatalf("%s does not say which sources produced it", entry.Name())
		}

		full, half := reportsFromRecords(records)
		for _, limit := range []int{0, 1, 3, 18} {
			limitedFull, limitErr := underHopLimit(full, limit)
			if limitErr != nil {
				t.Fatalf("%s: L=%d on the full graph: %v", entry.Name(), limit, limitErr)
			}
			limitedHalf, limitErr := underHopLimit(half, limit)
			if limitErr != nil {
				t.Fatalf("%s: L=%d on the half: %v", entry.Name(), limit, limitErr)
			}
			// The pair population must survive a limit: a limit changes
			// outcomes, never how many pairs were asked about.
			if len(limitedFull.ByPair) != len(records.Pairs) ||
				len(limitedHalf.ByPair) != len(records.Pairs) {
				t.Fatalf("%s: L=%d lost pairs (%d and %d of %d)", entry.Name(), limit,
					len(limitedFull.ByPair), len(limitedHalf.ByPair), len(records.Pairs))
			}
			if compared := compareLengths(limitedFull, limitedHalf); compared.Mismatch != "" {
				t.Fatalf("%s: L=%d: %s", entry.Name(), limit, compared.Mismatch)
			}
		}
		checked++
	}

	if checked == 0 {
		t.Fatalf("%s holds no record files — the sweep wrote nothing to load", dir)
	}
	t.Logf("recomputed L ∈ {0,1,3,18} from %d saved runs in %s", checked, dir)
}

// TestM2SwappingTheGraphsChangesTheAnswer is why the header is required
// exactly: it shows the swap the reader now refuses is NOT harmless.
//
// ⚠️ Without this, "we refuse a swapped header" would be a rule defending
// against something nobody showed to matter — the same class as a test that
// cannot be made red.
func TestM2SwappingTheGraphsChangesTheAnswer(t *testing.T) {
	t.Parallel()

	records, full, half := m2RecordFixture(t)

	swapped := m2Records{Key: records.Key, Pairs: make([]m2PairRecord, len(records.Pairs))}
	for index, record := range records.Pairs {
		swapped.Pairs[index] = m2PairRecord{
			Index: record.Index, Source: record.Source, Target: record.Target,
			Full: record.Half, Half: record.Full,
		}
	}

	swappedFull, swappedHalf := reportsFromRecords(swapped)

	if swappedFull.Outcomes[routingSuccess] == full.Outcomes[routingSuccess] &&
		swappedHalf.Outcomes[routingSuccess] == half.Outcomes[routingSuccess] {
		t.Fatal("exchanging the two graphs changed no success count — then this fixture cannot " +
			"show what the header guards, and the guard is untested")
	}

	live := compareLengths(full, half)
	exchanged := compareLengths(swappedFull, swappedHalf)
	// ⚠️ These two are often EQUAL, and that is the point rather than a
	// weakness: M2-G is a ratio of two medians that swap places, so the headline
	// figure cannot be used to detect the exchange. The counts below can.
	t.Logf("M2-G as measured %s, with the graphs exchanged %s — equal ratios are expected, which "+
		"is exactly why M2-G is not a detector for this", live.Ratio(), exchanged.Ratio())

	// The half is the harder graph, so reading it as the full network moves the
	// success rate the wrong way — which is exactly the failure that would go
	// unnoticed: every number still exists and none of them looks broken.
	if swappedFull.Outcomes[routingSuccess] > full.Outcomes[routingSuccess] {
		t.Fatalf("the exchanged 'full graph' arrived MORE often (%d) than the real one (%d) — "+
			"the fixture does not have the asymmetry this guard is about",
			swappedFull.Outcomes[routingSuccess], full.Outcomes[routingSuccess])
	}
}

// TestM2RecordsAreNotOverwrittenByARerun is the guard that would have saved the
// presented measurement.
//
// What happened (P2 of 2026-09-19): a sweep started with M2_RECORDS pointing at
// the presented directory and M2_SOURCES unset replaced 160 of 180 files with
// records carrying "sources=unstamped", and nothing said so until the hashes
// were verified again. Two rules now make that impossible, and both are checked
// here rather than written down.
func TestM2RecordsAreNotOverwrittenByARerun(t *testing.T) {
	records, _, _ := m2RecordFixture(t)
	dir := t.TempDir()

	path, err := writeM2Records(dir, records)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading what was written: %v", err)
	}

	// A second run of the same key — the case that overwrote the evidence.
	changed := records
	changed.Key.Sources = "a different version"
	if _, err := writeM2Records(dir, changed); err == nil {
		t.Fatal("the rerun was allowed to overwrite a presented record")
	} else if !strings.Contains(err.Error(), "SEPARATE directory") {
		t.Fatalf("the refusal does not say what to do instead: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading after the refused write: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("the refused write changed the file anyway")
	}

	// A separate directory is the sanctioned way, and it must work.
	if _, err := writeM2Records(filepath.Join(dir, "rerun"), changed); err != nil {
		t.Fatalf("writing the rerun into its own directory: %v", err)
	}

	// The explicit override exists, and it is explicit.
	t.Setenv("M2_RECORDS_OVERWRITE", "1")
	if _, err := writeM2Records(dir, changed); err != nil {
		t.Fatalf("M2_RECORDS_OVERWRITE=1 should allow it: %v", err)
	}
}

// TestM2RecordsSurviveConcurrentWritersOfOneName is the case a sequential test
// cannot reach: two runs of the same key started together.
//
// ⚠️ With the first version — os.Stat, then write — both writers saw no file,
// both went on, and the second replaced the first's evidence with nobody passing
// the override. The window is small and it is exactly the window that cost 160
// records, so it is checked by racing writers rather than argued about.
func TestM2RecordsSurviveConcurrentWritersOfOneName(t *testing.T) {
	// Not parallel: it owns the override variable for its duration.
	t.Setenv("M2_RECORDS_OVERWRITE", "")

	records, _, _ := m2RecordFixture(t)
	dir := t.TempDir()

	const writers = 8
	var (
		start      sync.WaitGroup
		done       sync.WaitGroup
		mu         sync.Mutex
		succeeded  []string
		refusals   int
		otherError error
	)
	start.Add(1)

	for writer := range writers {
		done.Add(1)
		go func(writer int) {
			defer done.Done()

			attempt := records
			// Each writer is distinguishable in the file itself, so the survivor
			// can be attributed to exactly one of them.
			attempt.Key.Sources = fmt.Sprintf("writer-%d", writer)

			start.Wait()
			_, err := writeM2Records(dir, attempt)

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded = append(succeeded, attempt.Key.Sources)
			case strings.Contains(err.Error(), "SEPARATE directory"):
				refusals++
			default:
				otherError = err
			}
		}(writer)
	}

	start.Done()
	done.Wait()

	if otherError != nil {
		t.Fatalf("a writer failed for an unexpected reason: %v", otherError)
	}
	if len(succeeded) != 1 {
		t.Fatalf("exactly one writer must create the name; %d did (%v)", len(succeeded), succeeded)
	}
	if refusals != writers-1 {
		t.Fatalf("the other %d writers must be refused; %d were", writers-1, refusals)
	}

	// The survivor's content is intact and attributable: a torn or interleaved
	// file would either fail to parse or carry somebody else's stamp.
	loaded, err := readM2Records(filepath.Join(dir, m2RecordFileName(records.Key)))
	if err != nil {
		t.Fatalf("the file left behind does not parse: %v", err)
	}
	if loaded.Key.Sources != succeeded[0] {
		t.Fatalf("the file says %q, the writer that won was %q",
			loaded.Key.Sources, succeeded[0])
	}
	if len(loaded.Pairs) != len(records.Pairs) {
		t.Fatalf("the file holds %d pairs of %d — it was written over",
			len(loaded.Pairs), len(records.Pairs))
	}
}

// TestM2RecordsRefuseToBeWrittenWithoutAVersion is the second rule: keeping
// records without saying which sources produced them is refused outright.
func TestM2RecordsRefuseToBeWrittenWithoutAVersion(t *testing.T) {
	t.Parallel()

	if _, err := m2SourcesStampFrom("", true); err == nil {
		t.Fatal("a run keeping records was allowed to do so without a version stamp")
	} else if !strings.Contains(err.Error(), "M2_SOURCES") {
		t.Fatalf("the refusal does not name what is missing: %v", err)
	}

	// Without records, an absent stamp is not an error — and it reads as absent
	// rather than as "current".
	stamp, err := m2SourcesStampFrom("", false)
	if err != nil {
		t.Fatalf("a run that keeps nothing needs no stamp: %v", err)
	}
	if stamp != "unstamped" {
		t.Fatalf("an absent version must read as absent, got %q", stamp)
	}

	if stamp, err := m2SourcesStampFrom("  f2a5dfdb2b807b2a  ", true); err != nil ||
		stamp != "f2a5dfdb2b807b2a" {
		t.Fatalf("a given stamp must arrive trimmed and intact: %q, %v", stamp, err)
	}
}

// TestM2RecordsRefuseAFileThatCannotBeTrusted covers every refusal in the
// reader. Each case is one way a file can look fine and mean something else.
func TestM2RecordsRefuseAFileThatCannotBeTrusted(t *testing.T) {
	t.Parallel()

	good := strings.Join([]string{
		"# " + m2RecordFormat,
		"# shape=1k×8 nodes=1000 degree=8 budget_B=16",
		"# policy=C1/v1 quota=1 seed=1",
		"# pairs_requested=2 pair_seed=1 eligible=500 pairs_written=2",
		"# hop_budget=none sources=fixture",
		m2RecordColumns,
		"0\t1\t2\tsuccess\t3\t2\tdead_end\t1\t7",
		"1\t2\t1\tno_path\t0\t2\tsuccess\t4\t1",
	}, "\n")

	swappedColumns := "pair\tsource\ttarget\thalf_outcome\thalf_hops\thalf_stopped\t" +
		"full_outcome\tfull_hops\tfull_stopped"

	if _, err := parseM2Records(strings.NewReader(good)); err != nil {
		t.Fatalf("the well-formed fixture was refused: %v", err)
	}

	for _, bad := range []struct {
		name    string
		file    string
		mustSay string
	}{
		{
			name:    "another format",
			file:    strings.Replace(good, m2RecordFormat, "m2-records/v0", 1),
			mustSay: "refused",
		},
		{
			name:    "measured under a budget",
			file:    strings.Replace(good, "hop_budget=none", "hop_budget=18", 1),
			mustSay: "not in the record",
		},
		{
			name:    "no budget stated",
			file:    strings.Replace(good, "# hop_budget=none sources=fixture", "# sources=fixture", 1),
			mustSay: "under which hop budget",
		},
		{
			name:    "truncated body",
			file:    strings.Replace(good, "\n1\t2\t1\tno_path\t0\t2\tsuccess\t4\t1", "", 1),
			mustSay: "truncated",
		},
		{
			name:    "indices out of order",
			file:    strings.Replace(good, "\n1\t2\t1\tno_path", "\n7\t2\t1\tno_path", 1),
			mustSay: "sample order is broken",
		},
		{
			name:    "a column short",
			file:    strings.Replace(good, "0\t1\t2\tsuccess\t3\t2\tdead_end\t1\t7", "0\t1\t2\tsuccess\t3\t2\tdead_end\t1", 1),
			mustSay: "nine columns",
		},
		{
			name:    "an outcome nobody defined",
			file:    strings.Replace(good, "dead_end\t1\t7", "gave_up\t1\t7", 1),
			mustSay: "not an outcome",
		},
		{
			name:    "no column header",
			file:    strings.Replace(good, m2RecordColumns+"\n", "", 1),
			mustSay: "column header",
		},
		{
			// The P2 case: the two graphs swap places, header and data
			// together. Nothing about the numbers looks wrong afterwards —
			// the half is read as the full network and M2-G comes out
			// inverted — so the header must be required exactly.
			name: "the two graphs swapped, header and data",
			file: strings.Replace(
				strings.Replace(good, m2RecordColumns, swappedColumns, 1),
				"0\t1\t2\tsuccess\t3\t2\tdead_end\t1\t7",
				"0\t1\t2\tdead_end\t1\t7\tsuccess\t3\t2", 1),
			mustSay: "which column belongs to which graph",
		},
		{
			name:    "the two graphs swapped in the header only",
			file:    strings.Replace(good, m2RecordColumns, swappedColumns, 1),
			mustSay: "which column belongs to which graph",
		},
		{
			name:    "a column renamed",
			file:    strings.Replace(good, "full_hops", "hops_full", 1),
			mustSay: "which column belongs to which graph",
		},
	} {
		t.Run(bad.name, func(t *testing.T) {
			_, err := parseM2Records(strings.NewReader(bad.file))
			if err == nil {
				t.Fatalf("the reader accepted %q", bad.name)
			}
			if !strings.Contains(err.Error(), bad.mustSay) {
				t.Fatalf("the refusal does not say why: %v", err)
			}
		})
	}
}
