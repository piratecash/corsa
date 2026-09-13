package overlaysim

// policy_sweep_test.go is the comparative experiment of §2.4: the same
// identifiers, seeds, shapes and quotas run through three neighbour-selection
// policies, to answer whether the isolation of structural nodes can be removed
// by changing the SELECTION — without raising B and without softening the
// "one component" criterion.
//
// Run one shape at a time; each is a subtest:
//
//	go test ./internal/overlaysim/ -run 'TestM1PolicyComparison/1k' -timeout 30m -v
//
// The report has three parts, in this order: the FULL record of every
// (seed, quota, policy) run, the attribution of every one of them, and only
// then the summary. A summary is a lossy view; publishing it alone leaves the
// reader unable to check what an improvement was paid for, or why a policy did
// not help.
//
// ⚠️ NUMBERS, not a decision. A policy is not adopted by winning here: the
// choice belongs to the owner (16a), the criterion is not restated after the
// run, and O5 stays open.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// --- the full record --------------------------------------------------------

// detailColumns is every measurement of one run, kept because the summary
// cannot answer the questions the comparison exists for: what the improvement
// cost, and where the second pass ran out of room.
var detailColumns = []string{
	"quota", "seed", "Q comps", "Q share", "iso Q", "base cmp",
	"quota✓", "q min", "q med", "short",
	"links", "2p add", "2p none", "2p ownB", "init max", "deg avg", "deg max", "at B", "unfilled",
}

const (
	detailHeaderFormat = "%5s %6s %8s %8s %7s %8s %7s %6s %6s %8s %9s %8s %8s %8s %8s %8s %8s %7s %9s\n"
	detailRowFormat    = "%5d %6d %8d %8.4f %7d %8d %7.3f %6d %6d %8d %9d %8d %8d %8d %8d %8.2f %8d %6.1f%% %8.1f%%\n"
)

func detailHeader() string {
	cells := make([]any, len(detailColumns))
	for i, name := range detailColumns {
		cells[i] = name
	}
	return fmt.Sprintf(detailHeaderFormat, cells...)
}

func formatDetailRow(report runReport) string {
	return fmt.Sprintf(detailRowFormat,
		report.Quota, report.Seed,
		report.Structural.Components, report.Structural.LargestShare(),
		report.Structural.Isolated, report.Base.Components,
		report.QuotaMet, report.QuotaMin, report.QuotaMedian, report.ShortOfQuota.Nodes,
		report.Links, report.SecondPassLinks, report.SecondPassStuck,
		report.SecondPassOutOfBudget, report.MaxInitiated, report.MeanDegree, report.MaxDegree,
		report.AtBudget*100, report.Unfilled*100)
}

// --- the summary ------------------------------------------------------------

// policyColumns is the comparison table. Two blocks: what the policy achieved,
// and what it cost.
//
// ⚠️ The cost columns are not decoration. A policy that removes isolation by
// spending every spare slot in the network has answered a different question
// from the one asked, and without `links`, `deg avg` and `at B` beside
// `iso Q` the table would not show it.
var policyColumns = []string{
	"quota", "policy",
	"Q comps", "Q share", "iso Q", "short",
	"links", "+links", "deg avg", "deg max", "at B", "init max",
	"full pre", "cand B",
}

const (
	policyHeaderFormat = "%5s %-16s %8s %8s %7s %8s %9s %8s %8s %8s %7s %9s %9s %8s\n"
	policyRowFormat    = "%5d %-16s %8d %8.4f %7d %8d %9d %+8.1f%% %8.2f %8d %6.1f%% %9d %8.1f%% %7.1f%%\n"
)

func policyHeader() string {
	cells := make([]any, len(policyColumns))
	for i, name := range policyColumns {
		cells[i] = name
	}
	return fmt.Sprintf(policyHeaderFormat, cells...)
}

// summariseSeeds reduces one policy's runs at one quota to a single row.
//
// ⚠️ It mixes two ways of summarising ON PURPOSE, and each column says which:
// the WORST seed for the connectivity figures, because a policy must not pass
// by averaging away one draw that came apart, and the MEAN for the price,
// because what a policy costs is what it costs typically.
//
// ⚠️ The isolated count is kept in its OWN maximum. An earlier version stored
// it on the running "worst" report, and a later seed with a smaller largest
// component replaced that report wholesale — so a draw with ten isolated nodes
// was reported as the two of the seed that overwrote it. The maximum of one
// column and the argmin of another are different reductions and cannot share a
// variable.
func summariseSeeds(reports []runReport) runReport {
	if len(reports) == 0 {
		return runReport{}
	}

	worst := reports[0]
	worstShare := reports[0].Structural.LargestShare()
	maxIsolated := 0
	links := 0

	for _, report := range reports {
		if share := report.Structural.LargestShare(); share < worstShare {
			worstShare = share
			worst = report
		}
		if report.Structural.Isolated > maxIsolated {
			maxIsolated = report.Structural.Isolated
		}
		links += report.Links
	}

	worst.Structural.Isolated = maxIsolated
	worst.Links = links / len(reports)
	return worst
}

// formatPolicyRow prints one policy's summary. baselineLinks is what the same
// shape and quota cost under the baseline, so the price of a policy is stated
// next to what it bought rather than left for a reader to subtract.
func formatPolicyRow(report runReport, baselineLinks int) string {
	extra := 0.0
	if baselineLinks > 0 {
		extra = (float64(report.Links) - float64(baselineLinks)) / float64(baselineLinks) * 100
	}

	return fmt.Sprintf(policyRowFormat,
		report.Quota, report.Policy.String(),
		report.Structural.Components, report.Structural.LargestShare(),
		report.Structural.Isolated, report.ShortOfQuota.Nodes,
		report.Links, extra, report.MeanDegree, report.MaxDegree,
		report.AtBudget*100, report.MaxInitiated,
		report.IsolatedStructural.FullBeforeSearch*100,
		report.IsolatedStructural.CandidateAtBudget*100)
}

// comparisonQuotas is the grid of quotas a shape is asked about.
//
// Every quota 0..d for the degree-eight shapes — that is where the baseline
// failed and where the answer has to be exact. For a degree-64 shape the grid
// is COARSE and says so: the run costs three policies × five seeds × sixty-five
// quotas there, and the shape is already connected at quota zero under every
// policy, so a fine grid buys nothing it does not already have.
//
// ⚠️ A coarse grid can miss a quota at which a policy stops working. That is a
// stated limit of this table, not something the numbers quietly cover — and it
// is why nothing may be claimed about the quotas BETWEEN the tested ones:
// success is not monotone in the quota (raising it rebuilds the links), so a
// gap is unmeasured, not implied.
//
// M1_QUOTAS closes that gap without weakening anything: it names an explicit
// range ("0-32") or list ("0,3,7"), so the fine grid can be run in chunks that
// fit a single invocation. It changes WHICH quotas are asked, never what is
// measured or how the criterion is applied.
func comparisonQuotas(sh shape) ([]int, error) {
	requested, present, err := quotasFromEnvironment()
	if err != nil {
		return nil, err
	}
	if !present {
		if sh.degree <= 8 {
			quotas := make([]int, 0, sh.degree+1)
			for quota := range sh.degree + 1 {
				quotas = append(quotas, quota)
			}
			return quotas, nil
		}

		quotas := []int{0}
		for quota := 1; quota <= sh.degree; quota *= 2 {
			quotas = append(quotas, quota)
		}
		if last := quotas[len(quotas)-1]; last != sh.degree {
			quotas = append(quotas, sh.degree)
		}
		return quotas, nil
	}

	inRange := make([]int, 0, len(requested))
	for _, quota := range requested {
		if quota >= 0 && quota <= sh.degree {
			inRange = append(inRange, quota)
		}
	}

	// ⚠️ An empty grid is a REFUSAL, not a run of nothing. M1_QUOTAS=9-10 on a
	// degree-eight shape used to produce zero measurements and a green test —
	// the shape of result that looks like a clean pass and says nothing at all.
	if len(inRange) == 0 {
		return nil, fmt.Errorf("no quota of %v is within 0..%d for shape %s",
			requested, sh.degree, sh.name)
	}
	return inRange, nil
}

// quotasFromEnvironment parses M1_QUOTAS into three DISTINGUISHABLE outcomes:
// absent (use the default grid), parsed, or malformed.
//
// ⚠️ The first version folded "malformed" into "absent" and silently ran the
// default grid. That is the worst of the three: the operator asked for one
// measurement, got another, and nothing said so — and a report that names the
// quotas it used still looks perfectly correct, because it IS correct about a
// run nobody ordered.
func quotasFromEnvironment() (quotas []int, present bool, err error) {
	raw := strings.TrimSpace(os.Getenv("M1_QUOTAS"))
	if raw == "" {
		return nil, false, nil
	}

	if from, to, isRange := strings.Cut(raw, "-"); isRange {
		low, lowErr := strconv.Atoi(strings.TrimSpace(from))
		if lowErr != nil {
			return nil, true, fmt.Errorf("M1_QUOTAS=%q: %q is not a number", raw, from)
		}
		high, highErr := strconv.Atoi(strings.TrimSpace(to))
		if highErr != nil {
			return nil, true, fmt.Errorf("M1_QUOTAS=%q: %q is not a number", raw, to)
		}
		if low > high {
			return nil, true, fmt.Errorf("M1_QUOTAS=%q: the range runs backwards (%d > %d)",
				raw, low, high)
		}

		quotas := make([]int, 0, high-low+1)
		for quota := low; quota <= high; quota++ {
			quotas = append(quotas, quota)
		}
		return quotas, true, nil
	}

	fields := strings.Split(raw, ",")
	quotas = make([]int, 0, len(fields))
	for _, field := range fields {
		quota, convErr := strconv.Atoi(strings.TrimSpace(field))
		if convErr != nil {
			return nil, true, fmt.Errorf("M1_QUOTAS=%q: %q is not a number", raw, field)
		}
		quotas = append(quotas, quota)
	}
	if len(quotas) == 0 {
		return nil, true, fmt.Errorf("M1_QUOTAS=%q names no quota", raw)
	}
	return quotas, true, nil
}

// TestM1PolicyComparison runs the three policies over the same inputs.
//
// It fails only on harness faults — a broken budget, an empty structural half.
// Whether a policy's result is acceptable is not a question a test may answer.
func TestM1PolicyComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("the policy comparison is a measurement, not a unit test")
	}

	for _, sh := range sweepShapes {
		t.Run(sh.name, func(t *testing.T) {
			quotas, err := comparisonQuotas(sh)
			if err != nil {
				t.Fatalf("%s: %v", sh.name, err)
			}

			// Every run is kept. Rendering reads from this rather than from a
			// counter updated in flight, which is how the isolated maximum was
			// lost once already.
			runs := map[policy][]runReport{}

			for _, selection := range allPolicies {
				for _, quota := range quotas {
					for _, seed := range sweepSeeds {
						report := measure(sh, seed, quota, selection)

						if report.StructuralNodes == 0 {
							t.Fatalf("%s seed=%d: the structural half is empty", sh.name, seed)
						}
						if report.MaxDegree > sh.budget {
							t.Fatalf("%s %s seed=%d quota=%d: degree %d exceeds budget %d",
								selection, sh.name, seed, quota, report.MaxDegree, sh.budget)
						}
						if selection != policySecondPass && report.MaxInitiated > sh.degree {
							t.Fatalf("%s %s seed=%d quota=%d: initiated %d over desired degree %d",
								selection, sh.name, seed, quota, report.MaxInitiated, sh.degree)
						}

						runs[selection] = append(runs[selection], report)
					}
				}
			}

			var out strings.Builder

			fmt.Fprintf(&out, "\nM1 policy comparison — %s\n", sh.name)
			fmt.Fprintf(&out, "model: docs/refactoring/dht/21-m1-connectivity-model.md §2.4\n")
			fmt.Fprintf(&out, "role:  docs/protocol/overlay_role.md §3\n\n")
			fmt.Fprintf(&out, "inputs\n")
			fmt.Fprintf(&out, "  shape %-7s N=%-6d desired degree d=%-3d budget B=%d\n",
				sh.name, sh.nodes, sh.degree, sh.budget)
			fmt.Fprintf(&out, "  seeds %v\n", sweepSeeds)
			fmt.Fprintf(&out, "  quotas %v\n", quotas)
			fmt.Fprintf(&out, "  policies: %s, %s, %s\n\n",
				policyBaseline, policyInitiatedLimit, policySecondPass)

			// 1. The full record, per policy: every seed, every quota, every
			//    field the model measures.
			for _, selection := range allPolicies {
				fmt.Fprintf(&out, "══ %s — every run ═══════════════════════════════\n", selection)
				out.WriteString(detailHeader())
				for _, report := range runs[selection] {
					out.WriteString(formatDetailRow(report))
				}

				fmt.Fprintf(&out, "\nwhy the quota went unmet — %s\n", selection)
				fmt.Fprintf(&out, "  left: every node below the quota. right: Q nodes with NO "+
					"structural neighbour.\n  shares are of their own population and are counted "+
					"independently, so they do not sum to 100 %%.\n")
				out.WriteString(attributionHeader())
				for _, report := range runs[selection] {
					out.WriteString(formatAttributionRow(report))
				}
				fmt.Fprintf(&out, "\n")
			}

			// 2. The summary, which is a VIEW of the rows above and nothing the
			//    rows above cannot be re-derived from.
			fmt.Fprintf(&out, "══ summary ══════════════════════════════════════\n")
			fmt.Fprintf(&out, "  iso Q is the WORST seed (a policy must not pass by averaging away "+
				"one disconnected draw);\n")
			fmt.Fprintf(&out, "  Q comps/Q share/short/deg/at B/init max/attribution come from the "+
				"worst-share seed;\n")
			fmt.Fprintf(&out, "  links and +links are the MEAN over seeds — a price is what it "+
				"costs typically, not at its peak.\n\n")
			out.WriteString(policyHeader())

			// The SAME criterion the baseline sweep applies: a quota works only
			// if EVERY seed came out in exactly one structural component.
			// Restating it per policy, or after seeing the rows, would be
			// fitting — so it is computed here, unchanged, for all three.
			connecting := map[policy]map[int]bool{}
			for _, selection := range allPolicies {
				connecting[selection] = map[int]bool{}
			}

			for quotaIndex, quota := range quotas {
				baselineLinks := 0

				for _, selection := range allPolicies {
					first := quotaIndex * len(sweepSeeds)
					seeds := runs[selection][first : first+len(sweepSeeds)]

					oneComponentEverywhere := true
					for _, report := range seeds {
						if report.Quota != quota {
							t.Fatalf("%s: run %d is quota %d, expected %d — the record and the "+
								"summary have drifted apart", selection, first, report.Quota, quota)
						}
						if report.Structural.Components != 1 {
							oneComponentEverywhere = false
						}
					}
					if oneComponentEverywhere {
						connecting[selection][quota] = true
					}

					summary := summariseSeeds(seeds)
					if selection == policyBaseline {
						baselineLinks = summary.Links
					}
					out.WriteString(formatPolicyRow(summary, baselineLinks))
				}
				fmt.Fprintf(&out, "\n")
			}

			fmt.Fprintf(&out, "quotas giving ONE structural component on EVERY seed:\n")
			if len(quotas) != sh.degree+1 {
				fmt.Fprintf(&out, "  ⚠️ %d of the %d quotas 0..d were tested. Nothing is claimed "+
					"about the untested ones: success is not monotone in the quota.\n",
					len(quotas), sh.degree+1)
			}
			for _, selection := range allPolicies {
				working := make([]int, 0, len(quotas))
				for _, quota := range quotas {
					if connecting[selection][quota] {
						working = append(working, quota)
					}
				}
				if len(working) == 0 {
					fmt.Fprintf(&out, "  %-16s NONE of %v\n", selection, quotas)
					continue
				}
				fmt.Fprintf(&out, "  %-16s %v\n", selection, working)
			}
			fmt.Fprintf(&out, "\n")

			fmt.Fprintf(&out, "⚠️ Numbers only. No policy is adopted by this table; the choice is "+
				"the owner's (16a), and O5 stays open.\n")
			t.Log(out.String())
		})
	}
}
