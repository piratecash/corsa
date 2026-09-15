package overlaysim

// routing_reference_test.go is the acceptance of the routing measurer against
// the graphs of docs/refactoring/dht/21-m1-candidate-c1.md §5.5.1 (Э1…Э5).
//
// ⚠️ Every expected number below was COMPUTED before it was written down. An
// earlier draft of the plan asserted "a ring always succeeds" and "a star
// reaches in two hops" — both are false, and a measurer validated against them
// would have been validated against nothing.

import (
	"fmt"
	"testing"
)

// referenceGraph builds a graph from byte values: the value goes into the TOP
// byte of the identifier and the remaining 19 bytes are zero, so XOR ordering
// equals numeric ordering of those values and the expectations are checkable by
// hand.
func referenceGraph(values []byte, edges [][2]int) *graph {
	g := &graph{
		ids:                  make([]nodeID, len(values)),
		roles:                make([]int, len(values)),
		adjacency:            make([][]int32, len(values)),
		structuralNeighbours: make([]int, len(values)),
		initiated:            make([]int, len(values)),
		shortfall:            make([]quotaShortfall, len(values)),
	}
	for i, value := range values {
		g.ids[i][0] = value
		// Every node is structural here: these fixtures test the walk, not the
		// role split, and a half-empty membership would hide walk defects.
		g.roles[i] = roleStructural
	}
	for _, edge := range edges {
		a, b := int32(edge[0]), int32(edge[1])
		g.adjacency[a] = append(g.adjacency[a], b)
		g.adjacency[b] = append(g.adjacency[b], a)
	}
	return g
}

// everyone is the membership predicate for the whole graph.
func everyone(int32) bool { return true }

// referenceComponents labels components under a membership predicate. Kept
// separate from structuralComponents because these fixtures are not built by
// the model and have no roles to speak of.
func referenceComponents(g *graph, inGraph func(int32) bool) []int {
	labels := make([]int, len(g.adjacency))
	for i := range labels {
		labels[i] = -1
	}
	next := 0
	for i := range labels {
		if !inGraph(int32(i)) || labels[i] != -1 {
			continue
		}
		stack := []int32{int32(i)}
		labels[i] = next
		for len(stack) > 0 {
			u := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, v := range g.adjacency[u] {
				if inGraph(v) && labels[v] == -1 {
					labels[v] = next
					stack = append(stack, v)
				}
			}
		}
		next++
	}
	return labels
}

// allPairs enumerates every ordered pair of distinct nodes.
func allPairs(nodes int) [][2]int32 {
	pairs := make([][2]int32, 0, nodes*(nodes-1))
	for s := range nodes {
		for t := range nodes {
			if s != t {
				pairs = append(pairs, [2]int32{int32(s), int32(t)})
			}
		}
	}
	return pairs
}

func hypercubeFixture() *graph {
	values := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	edges := [][2]int{}
	for a := range 8 {
		for b := a + 1; b < 8; b++ {
			if a^b == 1 || a^b == 2 || a^b == 4 {
				edges = append(edges, [2]int{a, b})
			}
		}
	}
	return referenceGraph(values, edges)
}

func chainFixture() *graph {
	values := make([]byte, 64)
	edges := make([][2]int, 0, 63)
	for i := range 64 {
		values[i] = byte(i)
		if i > 0 {
			edges = append(edges, [2]int{i - 1, i})
		}
	}
	return referenceGraph(values, edges)
}

// TestRoutingReferenceGraphs is Э1…Э4.
func TestRoutingReferenceGraphs(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		name           string
		graph          *graph
		wantSuccess    int
		wantDeadEnd    int
		wantNoPath     int
		wantLengths    map[int]int // hops → how many successful pairs
		expectAllShort bool
	}{
		{
			// Э1 — the ONLY fixture where greedy always arrives. Each step
			// flips one differing bit, so the distance strictly falls and the
			// length equals the number of differing bits.
			name:        "Э1 hypercube on three bits — always arrives",
			graph:       hypercubeFixture(),
			wantSuccess: 56,
			wantLengths: map[int]int{1: 24, 2: 24, 3: 8},
		},
		{
			// ⚠️ Э2 — a ring is NOT a success fixture. It is the fixture for a
			// legitimate refusal: the plan once claimed the opposite.
			name:        "Э2 ring of eight — legitimate dead ends",
			graph:       referenceGraph([]byte{0, 1, 2, 3, 4, 5, 6, 7}, [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 0}}),
			wantSuccess: 40,
			wantDeadEnd: 16,
		},
		{
			// Э3 — connected, and still a dead end for three pairs: from leaf
			// 1 to leaf 2 the only neighbour is centre 7, and XOR(7,2)=5 is
			// FARTHER than XOR(1,2)=3.
			name:        "Э3 star with a centre — connected yet stuck",
			graph:       referenceGraph([]byte{7, 1, 2, 4}, [][2]int{{0, 1}, {0, 2}, {0, 3}}),
			wantSuccess: 9,
			wantDeadEnd: 3,
			wantLengths: map[int]int{1: 6, 2: 3},
		},
		{
			// Э4 — the other half of the pair: unreachable must never be
			// reported as a dead end.
			name:        "Э4 two disjoint triangles — no path is not a dead end",
			graph:       referenceGraph([]byte{0, 1, 2, 8, 9, 10}, [][2]int{{0, 1}, {1, 2}, {0, 2}, {3, 4}, {4, 5}, {3, 5}}),
			wantSuccess: 12,
			wantNoPath:  18,
			wantLengths: map[int]int{1: 12},
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			nodes := len(fixture.graph.adjacency)
			component := referenceComponents(fixture.graph, everyone)

			report, err := measureRouting(fixture.graph, everyone, component,
				allPairs(nodes), noBudget)
			if err != nil {
				t.Fatalf("measurer reported a defect: %v", err)
			}

			for outcome, want := range map[routingOutcome]int{
				routingSuccess:     fixture.wantSuccess,
				routingDeadEnd:     fixture.wantDeadEnd,
				routingNoPath:      fixture.wantNoPath,
				routingBudgetSpent: 0,
			} {
				if got := report.Outcomes[outcome]; got != want {
					t.Errorf("%s: %d, want %d", outcome, got, want)
				}
			}

			if fixture.wantLengths != nil {
				lengths := map[int]int{}
				for _, hops := range report.HopsByPair {
					lengths[hops]++
				}
				if fmt.Sprint(lengths) != fmt.Sprint(fixture.wantLengths) {
					t.Errorf("lengths %v, want %v", lengths, fixture.wantLengths)
				}
			}
		})
	}
}

// TestRoutingStarSpecificPairs pins the individual pairs the plan names, so a
// measurer that got the right totals for the wrong reasons still fails.
func TestRoutingStarSpecificPairs(t *testing.T) {
	t.Parallel()

	// Index 0 is the centre 0x07; leaves 0x01, 0x02, 0x04 are indices 1, 2, 3.
	g := referenceGraph([]byte{7, 1, 2, 4}, [][2]int{{0, 1}, {0, 2}, {0, 3}})
	component := referenceComponents(g, everyone)

	for _, want := range []struct {
		source, target int32
		outcome        routingOutcome
		hops           int
	}{
		{1, 2, routingDeadEnd, 0}, // 1 → 2: the centre is farther than staying
		{2, 1, routingDeadEnd, 0},
		{3, 1, routingDeadEnd, 0}, // leaf 0x04 → leaf 0x01
		{3, 2, routingSuccess, 2}, // leaf 0x04 → leaf 0x02 DOES arrive, via the centre
		{1, 0, routingSuccess, 1}, // leaf → centre
		{0, 3, routingSuccess, 1}, // centre → leaf
	} {
		got, err := greedyRoute(g, everyone, component, want.source, want.target, noBudget)
		if err != nil {
			t.Fatalf("%d→%d: %v", want.source, want.target, err)
		}
		if got.Outcome != want.outcome || got.Hops != want.hops {
			t.Errorf("%d→%d: %s in %d hops, want %s in %d",
				want.source, want.target, got.Outcome, got.Hops, want.outcome, want.hops)
		}
	}
}

// TestRoutingChainAndBudget is Э5 plus the two budget edges the plan calls out:
// a long legitimate path, success exactly on the last permitted transition, and
// the budget spent without attempting the next one.
func TestRoutingChainAndBudget(t *testing.T) {
	t.Parallel()

	g := chainFixture()
	component := referenceComponents(g, everyone)
	const source, target = int32(63), int32(0)

	t.Run("a long monotone path is legitimate, not a defect", func(t *testing.T) {
		t.Parallel()

		got, err := greedyRoute(g, everyone, component, source, target, noBudget)
		if err != nil {
			t.Fatalf("defect reported on a correct walk: %v", err)
		}
		if got.Outcome != routingSuccess || got.Hops != 63 {
			t.Fatalf("%s in %d hops, want success in 63", got.Outcome, got.Hops)
		}
	})

	t.Run("success on the LAST permitted transition is a success", func(t *testing.T) {
		t.Parallel()

		got, err := greedyRoute(g, everyone, component, source, target, 63)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if got.Outcome != routingSuccess || got.Hops != 63 {
			t.Fatalf("%s in %d hops, want success in 63 with a budget of exactly 63",
				got.Outcome, got.Hops)
		}
	})

	t.Run("the transition over the budget is never performed", func(t *testing.T) {
		t.Parallel()

		// ⚠️ The plan's own example: a budget of 18 stops AFTER 18 performed
		// transitions and BEFORE attempting the 19th.
		got, err := greedyRoute(g, everyone, component, source, target, 18)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if got.Outcome != routingBudgetSpent || got.Hops != 18 {
			t.Fatalf("%s after %d hops, want budget spent after exactly 18",
				got.Outcome, got.Hops)
		}
		if want := int32(63 - 18); got.Stopped != want {
			t.Fatalf("stopped at %d, want %d — the 19th transition was performed after all",
				got.Stopped, want)
		}
	})

	t.Run("one hop short of the target is still budget spent", func(t *testing.T) {
		t.Parallel()

		got, err := greedyRoute(g, everyone, component, source, target, 62)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if got.Outcome != routingBudgetSpent || got.Hops != 62 {
			t.Fatalf("%s after %d hops, want budget spent after 62", got.Outcome, got.Hops)
		}
	})

	t.Run("the whole chain, as computed", func(t *testing.T) {
		t.Parallel()

		report, err := measureRouting(g, everyone, component, allPairs(64), noBudget)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if report.Outcomes[routingSuccess] != 590 || report.Outcomes[routingDeadEnd] != 3442 {
			t.Fatalf("success %d, dead ends %d; want 590 and 3442",
				report.Outcomes[routingSuccess], report.Outcomes[routingDeadEnd])
		}
		if report.Lengths.Max != 63 {
			t.Fatalf("longest successful path %d, want 63", report.Lengths.Max)
		}
	})
}

// TestRoutingSourceEqualsTarget is the degenerate case: no transition is made,
// and it is a success rather than a dead end.
func TestRoutingSourceEqualsTarget(t *testing.T) {
	t.Parallel()

	g := hypercubeFixture()
	component := referenceComponents(g, everyone)

	for _, budget := range []int{noBudget, 0, 5} {
		got, err := greedyRoute(g, everyone, component, 3, 3, budget)
		if err != nil {
			t.Fatalf("budget %d: %v", budget, err)
		}
		if got.Outcome != routingSuccess || got.Hops != 0 {
			t.Fatalf("budget %d: %s in %d hops, want success in 0", budget, got.Outcome, got.Hops)
		}
	}
}

// TestRoutingEmptySampleSaysNoData guards the reporting rule: nothing measured
// must not print as zero.
func TestRoutingEmptySampleSaysNoData(t *testing.T) {
	t.Parallel()

	g := hypercubeFixture()
	component := referenceComponents(g, everyone)

	report, err := measureRouting(g, everyone, component, nil, noBudget)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if got := report.Lengths.String(); got != "no data" {
		t.Errorf("empty sample lengths: %q, want %q", got, "no data")
	}
	if got := report.share(routingSuccess); got != "no data" {
		t.Errorf("empty sample share: %q, want %q", got, "no data")
	}

	compared := compareLengths(report, report)
	if compared.Common != 0 || compared.Ratio() != "no data" {
		t.Errorf("empty comparison: common %d, ratio %q", compared.Common, compared.Ratio())
	}
}

// TestRoutingComparesOnlyPairsSuccessfulInBoth is M2-L and M2-G: the two graphs
// are compared on the intersection, never on the union.
func TestRoutingComparesOnlyPairsSuccessfulInBoth(t *testing.T) {
	t.Parallel()

	full := routingReport{Pairs: 3, HopsByPair: map[int]int{0: 2, 1: 4, 2: 6}}
	half := routingReport{Pairs: 3, HopsByPair: map[int]int{0: 3, 2: 9}} // pair 1 failed

	compared := compareLengths(full, half)
	if compared.Common != 2 {
		t.Fatalf("common pairs %d, want 2 — pair 1 succeeded in one graph only", compared.Common)
	}
	if compared.Full.Pairs != 2 || compared.Half.Pairs != 2 {
		t.Fatalf("full %v, half %v — both sides must use the same pairs",
			compared.Full, compared.Half)
	}
	// Medians over {2,6} and {3,9}: the lower of the two middles for an even
	// sample, by the same rule on both sides.
	if compared.Full.Median != 2 || compared.Half.Median != 3 {
		t.Fatalf("medians full %d half %d, want 2 and 3", compared.Full.Median, compared.Half.Median)
	}
	if got := compared.Ratio(); got != "1.50" {
		t.Fatalf("M2-G ratio %q, want %q", got, "1.50")
	}
}

// TestRoutingSeparatesTheTwoGraphsOnTheSamePairs is the §5.3 requirement that
// the full graph and the structural half are measured on ONE pair sample.
func TestRoutingSeparatesTheTwoGraphsOnTheSamePairs(t *testing.T) {
	t.Parallel()

	// ⚠️ The fixture had to be computed, not imagined: a first attempt used the
	// path 0x00—0x01—0x02—0x03, where greedy fails even in the FULL graph
	// (from 0x00 the only neighbour 0x01 is farther from 0x02 than 0x00 is).
	//
	// This one works: index 1 carries 0x02 and is ¬Q, so the pair 0x00 → 0x03
	// walks through it in the full graph and has no path at all in the
	// structural half, while 0x00 → 0x01 succeeds in both.
	//
	//   index:  0      1      2      3
	//   id:     0x00   0x02   0x03   0x01
	//   edges:  0—1, 1—2, 0—3
	g := referenceGraph([]byte{0x00, 0x02, 0x03, 0x01}, [][2]int{{0, 1}, {1, 2}, {0, 3}})
	g.roles[1] = 0 // 0x02 is ¬Q

	structural := func(i int32) bool { return g.roles[i] == roleStructural }
	pairs := [][2]int32{{0, 2}, {0, 3}}

	fullReport, err := measureRouting(g, everyone, referenceComponents(g, everyone), pairs, noBudget)
	if err != nil {
		t.Fatalf("full graph: %v", err)
	}
	halfReport, err := measureRouting(g, structural, referenceComponents(g, structural), pairs,
		noBudget)
	if err != nil {
		t.Fatalf("structural half: %v", err)
	}

	if fullReport.Outcomes[routingSuccess] != 2 {
		t.Errorf("full graph: %d successes, want 2", fullReport.Outcomes[routingSuccess])
	}
	if halfReport.Outcomes[routingNoPath] != 1 || halfReport.Outcomes[routingSuccess] != 1 {
		t.Errorf("structural half: %d no-path and %d success, want 1 and 1",
			halfReport.Outcomes[routingNoPath], halfReport.Outcomes[routingSuccess])
	}

	compared := compareLengths(fullReport, halfReport)
	if compared.Common != 1 {
		t.Fatalf("common successful pairs %d, want 1", compared.Common)
	}
}
