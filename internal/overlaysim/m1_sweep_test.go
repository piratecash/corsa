package overlaysim

// m1_sweep_test.go is the M1 run itself: the shapes of the measurement plan,
// several seeds, and the quota swept from zero to the desired degree.
//
// Run it with
//
//	go test ./internal/overlaysim/ -run TestM1ConnectivitySweep -timeout 30m -v
//
// The report prints its own inputs first. A table that does not name the
// parameters it came from cannot be reproduced, and an irreproducible number is
// not a measurement.
//
// ⚠️ This produces NUMBERS, not a decision. The quota is not approved by a green
// run: the threshold belongs to the owner (21-anonymity-transport.md §4.3.4″.8
// п.1), a negative result is an acceptable outcome, and O5 stays open.

import (
	"fmt"
	"strings"
	"testing"
)

// sweepShapes are the forms of 13-measurements.md §8.2, the same ones the
// resource numbers were taken on, so the two sets of results describe the same
// networks.
//
// budget is the hard ceiling B. It is set to twice the desired degree: a node
// that wants d neighbours must be able to accept roughly as many again before
// refusing, or the model would be measuring a saturated network rather than a
// working one. B is not an approved value — 15-overlay-parameters.md §2 keeps
// it in ranges — so it is stated here as an input, not assumed.
var sweepShapes = []shape{
	{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
	{name: "10k×8", nodes: 10_000, degree: 8, budget: 16},
	{name: "10k×64", nodes: 10_000, degree: 64, budget: 128},
	{name: "64k×8", nodes: 64_000, degree: 8, budget: 16},
}

// sweepSeeds are the independent draws. Five, because one graph is an anecdote
// and the spread between seeds is itself part of the answer.
var sweepSeeds = []uint64{1, 2, 3, 4, 5}

// sweepColumns names every column of the report ONCE, next to the row format
// that fills it. A header and a body maintained apart drift, and a column
// mislabelled in a saved report is worse than a missing one.
//
// ⚠️ The report must carry every measured field. An earlier version computed
// QuotaMin, QuotaMedian and Unfilled and printed none of them, so the saved
// tables could not answer two questions the model promises: HOW DEEP the quota
// shortfall goes (QuotaMet says how many nodes missed it, not by how much — a
// network where everyone is one short and one where a tenth have none look
// identical), and how many nodes never reached their desired degree at all.
var sweepColumns = []string{
	"quota", "seed", "Q nodes", "Q comps", "Q share", "Q isolat", "base cmp",
	"quota✓", "q min", "q med", "deg avg", "deg max", "at B", "unfilled",
}

const (
	sweepHeaderFormat = "%5s %6s %8s %9s %8s %9s %8s %7s %6s %6s %8s %8s %7s %9s\n"
	sweepRowFormat    = "%5d %6d %8d %9d %8.4f %9d %8d %7.3f %6d %6d %8.2f %8d %6.1f%% %8.1f%%\n"
)

// The ATTRIBUTION of the shortfall goes in its own table. Without it the report
// can show `at B` rising while `quota✓` falls and nothing more — two aggregates
// moving together, which is not a cause.
//
// ⚠️ TWO populations, side by side and never mixed. `short` is every node below
// the quota; `iso Q` is the Q nodes with NO structural neighbour — the ones that
// actually break connectivity. An earlier version reported only the first and
// the results explained the second with it.
//
// ⚠️ `full pre` and `full mid` are the split of one earlier column. Pre means
// the node was already at its degree BEFORE it looked at anyone — it never
// searched. Mid means it searched, and ran out of capacity along the way. Only
// `full pre` supports "did not look for structural neighbours at all".
//
// Shares are of their own population, counted independently, so they sum to
// more than 100 %: one node can be refused for budget in one bucket and fill up
// in another.
var attributionColumns = []string{
	"quota", "seed",
	"short", "full pre", "full mid", "cand B", "absent", "leftovr",
	"iso Q", "full pre", "full mid", "cand B", "absent", "leftovr",
}

const (
	attributionHeaderFormat = "%5s %6s %8s %8s %8s %7s %7s %8s %7s %8s %8s %7s %7s %8s\n"
	attributionRowFormat    = "%5d %6d %8d %7.1f%% %7.1f%% %6.1f%% %6.1f%% %7.1f%% %7d %7.1f%% %7.1f%% %6.1f%% %6.1f%% %7.1f%%\n"
)

func sweepHeader() string {
	cells := make([]any, len(sweepColumns))
	for i, name := range sweepColumns {
		cells[i] = name
	}
	return fmt.Sprintf(sweepHeaderFormat, cells...)
}

func formatSweepRow(report runReport) string {
	return fmt.Sprintf(sweepRowFormat,
		report.Quota, report.Seed, report.StructuralNodes, report.Structural.Components,
		report.Structural.LargestShare(), report.Structural.Isolated, report.Base.Components,
		report.QuotaMet, report.QuotaMin, report.QuotaMedian,
		report.MeanDegree, report.MaxDegree, report.AtBudget*100, report.Unfilled*100)
}

func attributionHeader() string {
	cells := make([]any, len(attributionColumns))
	for i, name := range attributionColumns {
		cells[i] = name
	}
	return fmt.Sprintf(attributionHeaderFormat, cells...)
}

func formatAttributionRow(report runReport) string {
	cells := []any{report.Quota, report.Seed}
	for _, slice := range []shortfallSlice{report.ShortOfQuota, report.IsolatedStructural} {
		cells = append(cells, slice.Nodes,
			slice.FullBeforeSearch*100, slice.FilledDuringSearch*100,
			slice.CandidateAtBudget*100, slice.NoCandidate*100,
			slice.LeftoverIgnoredQuota*100)
	}
	return fmt.Sprintf(attributionRowFormat, cells...)
}

// TestM1ConnectivitySweep is the measurement.
//
// It asserts almost nothing on purpose. The only things it fails on are
// harness faults — a broken budget, an empty structural half — because the
// question "is this connectivity acceptable" is not one a test may answer.
func TestM1ConnectivitySweep(t *testing.T) {
	if testing.Short() {
		t.Skip("M1 sweep is a measurement, not a unit test")
	}

	var out strings.Builder

	fmt.Fprintf(&out, "\nM1 — connectivity of the structural (Q) subgraph\n")
	fmt.Fprintf(&out, "model: docs/refactoring/dht/21-m1-connectivity-model.md\n")
	fmt.Fprintf(&out, "role:  docs/protocol/overlay_role.md §3 (SHA-256, 20 raw NodeID bytes)\n\n")
	fmt.Fprintf(&out, "inputs\n")
	for _, sh := range sweepShapes {
		fmt.Fprintf(&out, "  shape %-7s N=%-6d desired degree d=%-3d budget B=%d\n",
			sh.name, sh.nodes, sh.degree, sh.budget)
	}
	fmt.Fprintf(&out, "  seeds %v\n", sweepSeeds)
	fmt.Fprintf(&out, "  quota swept 0..d inclusive\n\n")

	for _, sh := range sweepShapes {
		fmt.Fprintf(&out, "── %s ──────────────────────────────────────────────\n", sh.name)
		out.WriteString(sweepHeader())

		// Built alongside the main table and printed after it: the attribution
		// is a different question about the same runs, and one row of
		// twenty-eight columns is a row nobody reads.
		var attribution strings.Builder

		for quota := range sh.degree + 1 {
			minShare, maxIsolated := 1.0, 0
			for _, seed := range sweepSeeds {
				report := measure(sh, seed, quota)

				if report.StructuralNodes == 0 {
					t.Fatalf("%s seed=%d: the structural half is empty — the harness is broken, "+
						"not the network", sh.name, seed)
				}
				if report.MaxInitiated > sh.degree {
					t.Fatalf("%s seed=%d quota=%d: a node initiated %d links against a desired "+
						"degree of %d", sh.name, seed, quota, report.MaxInitiated, sh.degree)
				}
				if report.MaxDegree > sh.budget {
					t.Fatalf("%s seed=%d quota=%d: degree %d exceeds budget %d",
						sh.name, seed, quota, report.MaxDegree, sh.budget)
				}

				if share := report.Structural.LargestShare(); share < minShare {
					minShare = share
				}
				if report.Structural.Isolated > maxIsolated {
					maxIsolated = report.Structural.Isolated
				}

				out.WriteString(formatSweepRow(report))
				attribution.WriteString(formatAttributionRow(report))
			}
			fmt.Fprintf(&out, "%5d %6s worst over seeds: Q share %.4f, isolated %d\n",
				quota, "—", minShare, maxIsolated)
		}
		fmt.Fprintf(&out, "\nwhy the Q quota went unmet — %s\n", sh.name)
		fmt.Fprintf(&out, "  left: every node below the quota. right: Q nodes with NO structural "+
			"neighbour.\n  shares are of their own population and are counted independently, so "+
			"they do not sum to 100 %%.\n")
		out.WriteString(attributionHeader())
		out.WriteString(attribution.String())
		fmt.Fprintf(&out, "\n")
	}

	// The verdict is a REPORT of the sweep, not a judgement on it.
	//
	// ⚠️ It is an INTERSECTION of the quotas that worked, not the largest of the
	// per-shape minima. Two earlier versions were wrong here in two different
	// ways, and both looked reasonable:
	//
	//  1. sweeping one global quota and SKIPPING shapes whose degree was
	//     smaller — at quota 9 only the degree-64 shape was still in scope and
	//     the run announced "9". A shape that cannot be asked has not answered;
	//  2. taking max(per-shape minimum). That assumes success is MONOTONE in
	//     the quota — that a shape connected at q stays connected at every
	//     larger q. Nothing here guarantees it: raising the quota rebuilds the
	//     links and redistributes the budget, so a shape can connect at 3 and
	//     come apart at 4. max(1, 2) = 2 is not a quota either shape was ever
	//     observed to satisfy.
	//
	// So: collect the set of working quotas per shape, intersect over the range
	// every shape can be asked about, and take the smallest survivor.
	fmt.Fprintf(&out, "quotas giving ONE structural component, per shape (all seeds):\n")

	working := make([]map[int]bool, 0, len(sweepShapes))
	limit := -1
	for _, sh := range sweepShapes {
		if limit == -1 || sh.degree < limit {
			limit = sh.degree
		}

		ok := make(map[int]bool)
		for quota := range sh.degree + 1 {
			good := true
			for _, seed := range sweepSeeds {
				if measure(sh, seed, quota).Structural.Components != 1 {
					good = false
					break
				}
			}
			if good {
				ok[quota] = true
			}
		}
		working = append(working, ok)
		fmt.Fprintf(&out, "  %-7s %s\n", sh.name, describeQuotaSet(ok, sh.degree))
	}

	if common, found := smallestCommonQuota(working, limit); found {
		fmt.Fprintf(&out, "\nsmallest quota satisfying EVERY shape and seed: %d "+
			"(intersection over 0..%d, the range every shape can be asked about)\n", common, limit)
	} else {
		fmt.Fprintf(&out, "\nsmallest quota satisfying EVERY shape and seed: NONE in 0..%d — "+
			"a negative result for THIS model and THIS selection policy, and not a reason to relax "+
			"a constraint\n", limit)
	}

	fmt.Fprintf(&out, "\n⚠️ Numbers only. The quota is not approved by this run; O5 stays open and G2 stays closed.\n")

	t.Log(out.String())
}

// smallestCommonQuota intersects the per-shape sets of working quotas over
// 0..limit and returns the smallest survivor.
//
// Separated from the run so it can be tested against NON-MONOTONE inputs: the
// bug it replaces was invisible on the data we happened to have, because every
// shape either worked everywhere or nowhere.
func smallestCommonQuota(working []map[int]bool, limit int) (int, bool) {
	if len(working) == 0 || limit < 0 {
		return 0, false
	}
	for quota := 0; quota <= limit; quota++ {
		all := true
		for _, ok := range working {
			if !ok[quota] {
				all = false
				break
			}
		}
		if all {
			return quota, true
		}
	}
	return 0, false
}

// describeQuotaSet renders the working set as it is rather than as a threshold:
// "3+" would be a claim about quotas nobody checked.
func describeQuotaSet(ok map[int]bool, degree int) string {
	if len(ok) == 0 {
		return fmt.Sprintf("NONE in 0..%d", degree)
	}
	values := make([]string, 0, len(ok))
	for quota := range degree + 1 {
		if ok[quota] {
			values = append(values, fmt.Sprintf("%d", quota))
		}
	}
	if len(values) == degree+1 {
		return fmt.Sprintf("all of 0..%d", degree)
	}
	return strings.Join(values, ",")
}
