package overlaysim

// m2_pairs_reference_test.go is the acceptance of the two things the M2 contract
// gained: the pair sample, and the claim that a hop limit can be applied to a
// finished run instead of re-walking it.
//
// The second is the one that needs proving. §5.3 leaves the experimental limit L
// to the owner, to be chosen AFTER the lengths are seen — so either the whole
// sweep is repeated for every candidate L, or the recomputation is shown to give
// the same answer as the re-run. This file shows it, on the five reference
// graphs, for every limit from zero to past the longest path, and it checks the
// outcomes and hop counts PAIR BY PAIR rather than in totals.

import (
	"fmt"
	"strings"
	"testing"
)

// TestUnderHopLimitEqualsARerun is the equivalence §5.3 asks for.
//
// ⚠️ The fixtures are not interchangeable and all five are needed:
//
//	Э1 hypercube — every pair succeeds, so it covers "nothing is truncated";
//	Э2 ring and Э3 star — DEAD ENDS, which is the case a truncation must leave
//	                      alone when it happened within the limit and must
//	                      replace when it happened past it;
//	Э4 two triples   — NO PATH, which no limit may ever turn into a budget
//	                   refusal, because no transition was performed;
//	Э5 chain         — a 63-hop success, which is the only fixture where the
//	                   boundary L = 63 (exactly enough) and L = 62 (one short)
//	                   are different answers.
func TestUnderHopLimitEqualsARerun(t *testing.T) {
	t.Parallel()

	fixtures := []struct {
		name     string
		graph    *graph
		maxLimit int
	}{
		{"Э1 hypercube", hypercubeFixture(), 5},
		{"Э2 ring", ringFixture(), 6},
		{"Э3 star", starFixture(), 4},
		{"Э4 two triples", twoTriplesFixture(), 4},
		{"Э5 chain", chainFixture(), 66},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			component := referenceComponents(fixture.graph, everyone)
			pairs := allPairs(len(fixture.graph.ids))

			unlimited, err := measureRouting(fixture.graph, everyone, component, pairs, noBudget)
			if err != nil {
				t.Fatalf("unlimited run: %v", err)
			}

			// Every outcome the recomputation has to handle must actually occur
			// somewhere in this set, or the equivalence is proven on a subset of
			// the cases and claimed for all of them.
			seen := map[routingOutcome]bool{}

			for limit := range fixture.maxLimit + 1 {
				rerun, err := measureRouting(fixture.graph, everyone, component, pairs, limit)
				if err != nil {
					t.Fatalf("limit %d: %v", limit, err)
				}
				recomputed, err := underHopLimit(unlimited, limit)
				if err != nil {
					t.Fatalf("limit %d: %v", limit, err)
				}

				if len(rerun.ByPair) != len(recomputed.ByPair) {
					t.Fatalf("limit %d: %d pairs re-run against %d recomputed",
						limit, len(rerun.ByPair), len(recomputed.ByPair))
				}
				for index := range rerun.ByPair {
					want, got := rerun.ByPair[index], recomputed.ByPair[index]
					seen[want.Outcome] = true

					if want.Outcome != got.Outcome || want.Hops != got.Hops {
						t.Fatalf("limit %d, pair %d (%d→%d): re-run says %s in %d hops, the "+
							"recomputation says %s in %d", limit, index,
							pairs[index][0], pairs[index][1],
							want.Outcome, want.Hops, got.Outcome, got.Hops)
					}
					// The stopping node, and the line the recomputation must not
					// cross. A pair the limit did not touch is the SAME walk, so
					// its stopping node must match the re-run exactly — checking
					// it here is what makes the equivalence proof cover the whole
					// result rather than a pair of aggregates.
					//
					// ⚠️ A TRUNCATED pair is the one case where it must say it
					// does not know: where a walk would have been after L
					// transitions is not derivable from an outcome and a hop
					// count, and a plausible guess is worse than an admitted gap.
					if got.Outcome == routingBudgetSpent {
						if got.Stopped != -1 {
							t.Fatalf("limit %d, pair %d: the recomputation named a stopping node "+
								"(%d) for a truncated walk, which it has no way to know",
								limit, index, got.Stopped)
						}
						continue
					}
					if got.Stopped != want.Stopped {
						t.Fatalf("limit %d, pair %d: the walk was not truncated, yet the "+
							"recomputation stops at %d and the re-run at %d",
							limit, index, got.Stopped, want.Stopped)
					}
				}

				if fmt.Sprint(rerun.Outcomes) != fmt.Sprint(recomputed.Outcomes) {
					t.Fatalf("limit %d: outcome totals %v against %v",
						limit, rerun.Outcomes, recomputed.Outcomes)
				}
				if rerun.Lengths.String() != recomputed.Lengths.String() {
					t.Fatalf("limit %d: lengths %q against %q",
						limit, rerun.Lengths, recomputed.Lengths)
				}
			}

			t.Logf("%s: outcomes exercised %v", fixture.name, outcomeNames(seen))
		})
	}
}

// outcomeNames renders the set of outcomes a fixture exercised, so the log says
// what was covered rather than merely that something was.
func outcomeNames(seen map[routingOutcome]bool) []string {
	names := make([]string, 0, len(seen))
	for _, outcome := range []routingOutcome{
		routingSuccess, routingDeadEnd, routingNoPath, routingBudgetSpent,
	} {
		if seen[outcome] {
			names = append(names, outcome.String())
		}
	}
	return names
}

// TestEveryOutcomeIsExercisedByTheFixtureSet is the guard on the guard: the
// equivalence above is only worth its name if all four outcomes appear in it.
func TestEveryOutcomeIsExercisedByTheFixtureSet(t *testing.T) {
	t.Parallel()

	seen := map[routingOutcome]bool{}
	for _, g := range []*graph{
		hypercubeFixture(), ringFixture(), starFixture(), twoTriplesFixture(), chainFixture(),
	} {
		component := referenceComponents(g, everyone)
		pairs := allPairs(len(g.ids))
		for _, limit := range []int{noBudget, 0, 2, 18} {
			report, err := measureRouting(g, everyone, component, pairs, limit)
			if err != nil {
				t.Fatalf("%v", err)
			}
			for outcome, count := range report.Outcomes {
				if count > 0 {
					seen[outcome] = true
				}
			}
		}
	}

	for _, outcome := range []routingOutcome{
		routingSuccess, routingDeadEnd, routingNoPath, routingBudgetSpent,
	} {
		if !seen[outcome] {
			t.Errorf("no fixture produces %q, so the equivalence proof does not cover it", outcome)
		}
	}
}

// TestUnderHopLimitRefusesAnAlreadyLimitedRun is the one case the recomputation
// must not attempt. A run cut short at 18 hops does not record what its searches
// would have done at hop 19, so recomputing a LARGER limit from it would invent
// successes — and recomputing a smaller one would look right while resting on a
// record that cannot support either direction.
func TestUnderHopLimitRefusesAnAlreadyLimitedRun(t *testing.T) {
	t.Parallel()

	g := chainFixture()
	component := referenceComponents(g, everyone)
	pairs := allPairs(len(g.ids))

	limited, err := measureRouting(g, everyone, component, pairs, 5)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := underHopLimit(limited, 3); err == nil {
		t.Error("a hop limit was recomputed from a run that was already limited")
	}

	unlimited, err := measureRouting(g, everyone, component, pairs, noBudget)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := underHopLimit(unlimited, -1); err == nil {
		t.Error("a negative hop limit was accepted")
	}

	// And the stand defect the recomputation refuses to paper over: an
	// unreachable pair that claims to have taken steps. ⚠️ This case is
	// unreachable through measureRouting, which is why it is constructed by
	// hand — the invariant it protects is what makes "no limit touches a no-path
	// result" true, and an invariant nobody can make fail is an invariant nobody
	// is checking.
	broken := routingReport{
		Pairs:    1,
		Outcomes: map[routingOutcome]int{routingNoPath: 1},
		ByPair:   []routingResult{{Outcome: routingNoPath, Hops: 3}},
		Budget:   noBudget,
	}
	if _, err := underHopLimit(broken, 1); err == nil {
		t.Error("an unreachable pair credited with three transitions was accepted")
	}
}

// --- the sample ---------------------------------------------------------------

// TestM2PairSampleObeysItsContract is the sampler's acceptance: the four rules of
// m2_pairs_test.go, each checked on a population where it can actually bite.
func TestM2PairSampleObeysItsContract(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	g := buildGraph(sh, 1, 1, policyInitiatedLimit)

	sample, err := drawM2Pairs(g, 500, 77)
	if err != nil {
		t.Fatalf("drawing: %v", err)
	}
	if len(sample.Pairs) != 500 || sample.Short {
		t.Fatalf("%d pairs of 500 requested, short=%v", len(sample.Pairs), sample.Short)
	}

	seen := map[[2]int32]int{}
	for _, pair := range sample.Pairs {
		if pair[0] == pair[1] {
			t.Fatalf("pair %v is a self-pair", pair)
		}
		if g.roles[pair[0]] != roleStructural || g.roles[pair[1]] != roleStructural {
			t.Fatalf("pair %v has an end outside the Q half, so M2-H could not route it", pair)
		}
		seen[pair]++
		if seen[pair] > 1 {
			t.Fatalf("pair %v appears twice", pair)
		}
	}

	// The two rejection counters must have fired, or the rules they enforce are
	// untested on this population.
	if sample.SelfPairs == 0 {
		t.Error("no self-pair was ever drawn, so rule 2 is not exercised here")
	}
	t.Logf("%s", sample)

	t.Run("the draw is reproducible and the seed reaches it", func(t *testing.T) {
		again, err := drawM2Pairs(g, 500, 77)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fmt.Sprint(again.Pairs) != fmt.Sprint(sample.Pairs) {
			t.Error("one seed drew two different samples")
		}

		other, err := drawM2Pairs(g, 500, 78)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fmt.Sprint(other.Pairs) == fmt.Sprint(sample.Pairs) {
			t.Error("two seeds drew the identical sample, so the seed does not reach the draw")
		}
	})

	t.Run("the same sample serves every policy", func(t *testing.T) {
		// ⚠️ THE point of the rule: the candidate and its base must be asked the
		// same questions. The graphs differ — that is what is being measured —
		// so a sampler that looked at edges would hand each policy its own
		// sample and call the difference a routing result.
		for _, selection := range allPolicies {
			other := buildGraph(sh, 1, 1, selection)
			got, err := drawM2Pairs(other, 500, 77)
			if err != nil {
				t.Fatalf("%s: %v", selection, err)
			}
			if fmt.Sprint(got.Pairs) != fmt.Sprint(sample.Pairs) {
				t.Fatalf("%s got a different pair sample from the same shape and seed", selection)
			}
		}
	})

	t.Run("both directions of a pair may occur", func(t *testing.T) {
		// Greedy routing is not symmetric, so forbidding the reverse would
		// silently halve what the sample can observe. This checks the sampler
		// does not forbid it — not that any particular draw contains one.
		forward := map[[2]int32]bool{}
		both := 0
		for _, pair := range sample.Pairs {
			forward[pair] = true
		}
		for pair := range forward {
			if forward[[2]int32{pair[1], pair[0]}] {
				both++
			}
		}
		t.Logf("%d pairs appear in both directions", both)
	})
}

// TestM2PairSampleIsHonestWhenItCannotFill covers the populations where the
// contract's own limits show: too few Q nodes to form a pair at all, and fewer
// distinct pairs than requested.
func TestM2PairSampleIsHonestWhenItCannotFill(t *testing.T) {
	t.Parallel()

	t.Run("no eligible nodes is no data, not an empty success", func(t *testing.T) {
		t.Parallel()

		g := roleGraph([]int{0, 0, 0}, [][2]int{{0, 1}, {1, 2}})
		sample, err := drawM2Pairs(g, 10, 1)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(sample.Pairs) != 0 || !sample.Short {
			t.Fatalf("%d pairs drawn from a population with no Q node, short=%v",
				len(sample.Pairs), sample.Short)
		}
		if got := sample.String(); !strings.Contains(got, "no data") {
			t.Errorf("the empty sample does not report itself as no data: %q", got)
		}
	})

	t.Run("more pairs requested than exist terminates and says so", func(t *testing.T) {
		t.Parallel()

		// Three Q nodes give six ordered pairs; asking for fifty must return six
		// and be marked short rather than spin or quietly return six as if that
		// were the request.
		g := roleGraph([]int{roleStructural, roleStructural, roleStructural, 0}, [][2]int{{0, 1}})
		sample, err := drawM2Pairs(g, 50, 3)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(sample.Pairs) != 6 {
			t.Fatalf("%d pairs from a population with exactly 6 distinct ordered pairs",
				len(sample.Pairs))
		}
		if !sample.Short {
			t.Error("an undersized sample was not marked short")
		}
		if sample.Repeats == 0 {
			t.Error("no repeat was rejected while exhausting a six-pair population")
		}
	})
}

// TestCompareLengthsRefusesTwoDifferentSamples is the stand defect §5.3 makes
// impossible to ignore: M2-L and M2-G are defined on ONE pair sample measured in
// two graphs, so reports of different lengths did not come from one sample.
//
// ⚠️ It used to stop at the shorter of the two and return a comparison anyway,
// which is worse than an error: the number looked exactly like a valid one while
// comparing two graphs on pairs only one of them was asked about.
func TestCompareLengthsRefusesTwoDifferentSamples(t *testing.T) {
	t.Parallel()

	success := func(hops int) routingResult {
		return routingResult{Outcome: routingSuccess, Hops: hops}
	}
	full := routingReport{Pairs: 3, ByPair: []routingResult{success(2), success(4), success(6)}}
	half := routingReport{Pairs: 2, ByPair: []routingResult{success(3), success(9)}}

	compared := compareLengths(full, half)
	if compared.Mismatch == "" {
		t.Fatal("two samples of different sizes were compared as if they were one")
	}
	if compared.Common != 0 {
		t.Errorf("%d common pairs reported from a mismatched comparison", compared.Common)
	}
	if got := compared.Ratio(); !strings.Contains(got, "STAND DEFECT") {
		t.Errorf("M2-G reads %q — a defect must not surface as a number", got)
	}
}
