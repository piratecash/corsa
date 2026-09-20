package overlaysim

// m2_sweep_test.go is the DRIVER of the final M2 (routing) and M5 (targets with
// no structural neighbour) measurement on the candidate.
//
// Why a separate file, and why only now: the instruments have existed since
// 2026-09-15 — the greedy walk (routing_test.go), the pair sample
// (m2_pairs_test.go), the neighbourless-target count (m5_test.go) — and every
// one of them is held by its own reference fixtures. What did NOT exist is the
// loop that builds the candidate's graphs and asks them the question. Without it
// "M2 is ready" meant "the instrument is ready", and no number could be produced
// at all.
//
// This file therefore does as little as a file can: it builds, draws, measures
// and prints. It decides nothing — no threshold, no acceptance, no policy. It
// fails ONLY on a stand defect (a sample that could not be drawn, two graphs
// measured on different samples, adjacency that makes a neighbour count
// meaningless), because "the candidate routes badly" is a result and results do
// not fail tests.
//
// ⚠️ Three properties are load-bearing and each is a rule from the contract, not
// a convenience:
//
//  1. ONE pair sample per (shape, seed) is used by BOTH policies and BOTH
//     graphs. drawM2Pairs reads g.roles and nothing else, and roles come from
//     the identifiers, so the candidate and its base are asked about the same
//     pairs. A difference in the answer is then a difference in routing.
//  2. The sample is NEVER filtered by the M1 result. Measuring path length only
//     where the structural half came out connected would select the graphs that
//     already worked (m2_pairs_test.go says this in full).
//  3. The walk runs with NO hop budget. The experimental limit L of §5.3 is the
//     owner's, chosen after the lengths are seen, and underHopLimit recomputes
//     any L from the finished run — proven equivalent to a re-run pair by pair
//     in m2_pairs_reference_test.go. Measuring under a guessed L would destroy
//     exactly the record that makes the choice possible.

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// m2DefaultPairs is the sample size when nothing is asked for.
//
// ⚠️ It is a STAND DEFAULT, not an agreed parameter: §5.3 fixes that the sample
// is drawn from a seed with both ends Q and is shared by the rules, and says
// nothing about how many. 500 ordered pairs is what the report prints and what
// the owner can overrule with M2_PAIRS; the number is in every header so no
// table can be read without it.
const m2DefaultPairs = 500

func m2PairsFromEnvironment(t *testing.T) int {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("M2_PAIRS"))
	if raw == "" {
		return m2DefaultPairs
	}
	var want int
	if _, err := fmt.Sscanf(raw, "%d", &want); err != nil || want <= 0 {
		// The malformed case is fatal for the same reason M1_QUOTAS makes it
		// fatal: an operator who asked for one measurement and silently got
		// another cannot tell from the report, which is correct about a run
		// nobody ordered.
		t.Fatalf("M2_PAIRS=%q is not a positive number of pairs", raw)
	}
	return want
}

// m2ShapesFromEnvironment selects shapes by name, because one call of this
// stand has a wall-clock ceiling and the sweep has to be cut somewhere the
// report can name.
func m2ShapesFromEnvironment(t *testing.T) []shape {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("M2_SHAPES"))
	if raw == "" {
		return []shape{sweepShapes[0], sweepShapes[1]}
	}

	byName := map[string]shape{}
	for _, sh := range sweepShapes {
		byName[sh.name] = sh
	}

	selected := make([]shape, 0, len(sweepShapes))
	for _, field := range strings.Split(raw, ",") {
		name := strings.TrimSpace(field)
		sh, known := byName[name]
		if !known {
			t.Fatalf("M2_SHAPES=%q names %q, which is not one of the sweep shapes", raw, name)
		}
		selected = append(selected, sh)
	}
	return selected
}

// m2Policies is the pair the measurement compares: the candidate and the base
// it differs from by ONE rule. baseline is not here on purpose — it is the
// control that the stand has not moved (TestComparisonBaseHasNotMoved), not a
// routing comparand.
var m2Policies = []policy{policyInitiatedLimit, policyCandidateC1}

// m2RecordsDestination says where the per-pair records go.
//
// ⚠️ An unset destination is NOT an error and NOT silent: the run goes ahead and
// the log says, in the header and again at the end, that the numbers in it
// cannot be recomputed under any hop limit. That is the P2 of 2026-09-19 — a
// sweep that keeps only medians and shares has destroyed the record §5.3 needs,
// and the only thing worse than losing it is losing it quietly.
func m2RecordsDestination() (dir string, keeping bool) {
	dir = strings.TrimSpace(os.Getenv("M2_RECORDS"))
	return dir, dir != ""
}

// m2SourcesStamp is the version the operator ran with, carried into every file.
//
// ⚠️ When records are being kept, a missing stamp is FATAL rather than the
// string "unstamped" (P2 of 2026-09-19). The permissive version cost a
// presented measurement: a sweep started with M2_RECORDS set and M2_SOURCES
// unset overwrote 160 of 180 presented files with records nobody could attribute
// to a version, and the damage was invisible until the hashes were checked
// again. A run that cannot say which sources produced it must not be able to
// write into the evidence at all.
func m2SourcesStamp(t *testing.T, keepingRecords bool) string {
	t.Helper()

	stamp, err := m2SourcesStampFrom(os.Getenv("M2_SOURCES"), keepingRecords)
	if err != nil {
		t.Fatal(err)
	}
	return stamp
}

// m2SourcesStampFrom is the rule itself, kept apart from the environment and
// from t.Fatal so that it can be tested — a guard whose refusal nobody can
// exercise is a guard nobody has checked.
func m2SourcesStampFrom(raw string, keepingRecords bool) (string, error) {
	stamp := strings.TrimSpace(raw)
	if stamp == "" && keepingRecords {
		return "", fmt.Errorf(
			"M2_RECORDS is set but M2_SOURCES is not: records that do not name the sources they " +
				"came from cannot be attributed to a version, and unattributable files in the " +
				"evidence directory are worse than none")
	}
	if stamp == "" {
		return "unstamped", nil
	}
	return stamp, nil
}

// TestM2RoutingAndM5OnTheCandidate is the run. Every row is one graph.
func TestM2RoutingAndM5OnTheCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("the routing measurement is a measurement, not a unit test")
	}

	want := m2PairsFromEnvironment(t)
	recordsDir, keepingRecords := m2RecordsDestination()
	// Resolved once, before any graph is built: a run that may not write must
	// fail before it spends minutes measuring.
	sourcesStamp := m2SourcesStamp(t, keepingRecords)

	for _, sh := range m2ShapesFromEnvironment(t) {
		quotas, err := comparisonQuotas(sh)
		if err != nil {
			t.Fatalf("%s: %v", sh.name, err)
		}

		t.Run(sh.name, func(t *testing.T) {
			var out strings.Builder

			fmt.Fprintf(&out, "\nM2 routing + M5 neighbourless targets — %s\n", sh.name)
			fmt.Fprintf(&out, "contract: docs/refactoring/dht/21-m1-candidate-c1.md §5.3, §5.5.3\n\n")
			fmt.Fprintf(&out, "inputs\n")
			fmt.Fprintf(&out, "  shape %-7s N=%-6d desired degree d=%-3d budget B=%d\n",
				sh.name, sh.nodes, sh.degree, sh.budget)
			fmt.Fprintf(&out, "  seeds %v   quotas %v\n", sweepSeeds, quotas)
			fmt.Fprintf(&out, "  rules %v — base is initiated-limit, candidate differs by ONE rule\n",
				m2Policies)
			fmt.Fprintf(&out, "  pair sample: %d ordered pairs, both ends Q, seed = the graph seed, "+
				"SHARED by both rules and both graphs\n", want)
			fmt.Fprintf(&out, "  hop budget: NONE — L is recomputed from the finished run (§5.3)\n")
			if keepingRecords {
				fmt.Fprintf(&out, "  per-pair records: %s (format %s, sources %s) — every pair, "+
					"both graphs, outcome/hops/stopping node\n\n",
					recordsDir, m2RecordFormat, sourcesStamp)
			} else {
				fmt.Fprintf(&out, "  ⚠️ PER-PAIR RECORDS NOT KEPT (M2_RECORDS unset): the numbers "+
					"below are aggregates, and NO hop limit L can be recomputed from them — "+
					"choosing L would mean running the sweep again\n\n")
			}

			fmt.Fprintf(&out, "  M2-R success   M2-D dead end   no path   |   M2-L on pairs successful in BOTH   M2-G = half/full\n")
			fmt.Fprintf(&out, "%-16s %5s %5s  %8s %8s  %8s %8s  %8s %8s   %-26s %-26s %6s   %-30s\n",
				"rule", "quota", "seed", "R full", "R half", "D full", "D half", "NP full", "NP half",
				"L full", "L half", "M2-G", "M5 targets without a Q neighbour")

			for _, selection := range m2Policies {
				for _, quota := range quotas {
					for _, seed := range sweepSeeds {
						g := buildGraph(sh, seed, quota, selection)

						sample, sampleErr := drawM2Pairs(g, want, seed)
						if sampleErr != nil {
							t.Fatalf("%s %s quota=%d seed=%d: %v",
								selection, sh.name, quota, seed, sampleErr)
						}
						if len(sample.Pairs) == 0 {
							// No pair could be drawn — a finding about the
							// graph, printed and carried on with.
							fmt.Fprintf(&out, "%-16s %5d %5d  %s\n", selection, quota, seed, sample)
							continue
						}

						everyone := func(int32) bool { return true }
						structural := func(i int32) bool { return g.roles[i] == roleStructural }

						full, fullErr := measureRouting(g, everyone,
							referenceComponents(g, everyone), sample.Pairs, noBudget)
						if fullErr != nil {
							t.Fatalf("%s %s quota=%d seed=%d: full graph: %v",
								selection, sh.name, quota, seed, fullErr)
						}
						half, halfErr := measureRouting(g, structural,
							referenceComponents(g, structural), sample.Pairs, noBudget)
						if halfErr != nil {
							t.Fatalf("%s %s quota=%d seed=%d: structural half: %v",
								selection, sh.name, quota, seed, halfErr)
						}

						lengths := compareLengths(full, half)
						if lengths.Mismatch != "" {
							t.Fatalf("%s %s quota=%d seed=%d: %s",
								selection, sh.name, quota, seed, lengths.Mismatch)
						}

						m5, m5Err := measureNeighbourlessTargets(g)
						if m5Err != nil {
							t.Fatalf("%s %s quota=%d seed=%d: M5: %v",
								selection, sh.name, quota, seed, m5Err)
						}

						if keepingRecords {
							key := m2RunKey{
								Shape: sh.name, Nodes: sh.nodes, Degree: sh.degree,
								Budget: sh.budget, Policy: selection.String(), Quota: quota,
								Seed: seed, PairsRequested: want, PairSeed: seed,
								Eligible: sample.Eligible, Sources: sourcesStamp,
							}
							path, writeErr := writeM2Records(recordsDir, m2Records{
								Key:   key,
								Pairs: pairRecordsOf(sample, full, half),
							})
							if writeErr != nil {
								// Losing the record is a stand defect, not a
								// result: the run would print numbers nobody can
								// reapply a limit to, which is the very thing
								// this file was changed to stop.
								t.Fatalf("%s %s quota=%d seed=%d: keeping the records: %v",
									selection, sh.name, quota, seed, writeErr)
							}
							_ = path
						}

						fmt.Fprintf(&out,
							"%-16s %5d %5d  %8s %8s  %8s %8s  %8s %8s   %-26s %-26s %6s   all %s / Q %s\n",
							selection, quota, seed,
							full.share(routingSuccess), half.share(routingSuccess),
							full.share(routingDeadEnd), half.share(routingDeadEnd),
							full.share(routingNoPath), half.share(routingNoPath),
							lengths.Full, lengths.Half, lengths.Ratio(),
							m5.All.Share(), m5.Structural.Share())

						if sample.Short {
							fmt.Fprintf(&out, "%-16s %5d %5d  ⚠️ SHORT SAMPLE: %d of %d pairs "+
								"(%d eligible nodes)\n",
								selection, quota, seed, len(sample.Pairs), sample.Requested,
								sample.Eligible)
						}
					}
				}
			}

			fmt.Fprintf(&out, "\n  R = share of searches that arrived; D = share that stopped in a "+
				"local minimum; NP = target in another component (established by BFS before the "+
				"walk, never confused with a dead end).\n")
			fmt.Fprintf(&out, "  ⚠️ The three are printed side by side and never averaged into one "+
				"another: a run where the length fell and the success rate fell with it is TWO "+
				"results.\n")
			fmt.Fprintf(&out, "  ⚠️ M2-G is the normative figure of M2 (§5.4). No threshold is "+
				"applied here — acceptability is the owner's, and the registry holds none.\n")
			fmt.Fprintf(&out, "  ⚠️ M5 is printed per graph because a target with no structural "+
				"neighbour is unreachable anonymously AND is not told so (21a-min §4.3.4″.5).\n")
			if keepingRecords {
				fmt.Fprintf(&out, "  Per-pair records for every row above are in %s — any L is "+
					"recomputed from them without running anything again "+
					"(m2_records_reference_test.go).\n", recordsDir)
			} else {
				fmt.Fprintf(&out, "  ⚠️ NO PER-PAIR RECORDS WERE KEPT for the rows above. The hop "+
					"limit L cannot be chosen from this log.\n")
			}

			t.Log(out.String())
		})
	}
}
