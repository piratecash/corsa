package overlaysim

// m5_reference_test.go is the acceptance of the M5 measurer: small graphs whose
// numerators AND denominators are known by hand, plus the two confusions M5 is
// most likely to fall into — counting the target's own role as a neighbour, and
// reporting isolation of the Q-subgraph under M5's name.
//
// ⚠️ Roles are written as literals: roleStructural is Q, and 0 is ¬Q — the model
// deliberately has no constant for the second (model_test.go), because "not
// structural" is the whole of its definition.

import (
	"strings"
	"testing"
)

const roleOther = 0 // ¬Q, spelled out here only to keep the fixtures readable.

// TestM5ReferenceGraphs walks the fixtures of the contract: empty, a lone Q
// node, one edge of each role combination, and a mixed graph.
func TestM5ReferenceGraphs(t *testing.T) {
	type expected struct {
		targets      int
		without      int
		share        string
		distribution string
	}
	cases := []struct {
		name  string
		roles []int
		edges [][2]int
		// all, structural and other are the three slices of the report.
		all, structural, other expected
	}{
		{
			name:       "Э-M5-1 empty graph: nothing measured is not zero failures",
			roles:      nil,
			edges:      nil,
			all:        expected{0, 0, "no data", "no data"},
			structural: expected{0, 0, "no data", "no data"},
			other:      expected{0, 0, "no data", "no data"},
		},
		{
			name:  "Э-M5-2 lone Q node: structural itself, still without a Q neighbour",
			roles: []int{roleStructural},
			edges: nil,
			// ⚠️ 100 %, not 0 %: the node's own role is not its neighbourhood.
			all:        expected{1, 1, "100.0%", "0:1"},
			structural: expected{1, 1, "100.0%", "0:1"},
			other:      expected{0, 0, "no data", "no data"},
		},
		{
			name:       "Э-M5-3 Q—Q: both ends see one Q neighbour",
			roles:      []int{roleStructural, roleStructural},
			edges:      [][2]int{{0, 1}},
			all:        expected{2, 0, "0.0%", "1:2"},
			structural: expected{2, 0, "0.0%", "1:2"},
			other:      expected{0, 0, "no data", "no data"},
		},
		{
			name:  "Э-M5-4 Q—¬Q: one edge, and the two ends disagree about it",
			roles: []int{roleStructural, roleOther},
			edges: [][2]int{{0, 1}},
			// The Q end has a ¬Q neighbour and is therefore WITHOUT; the ¬Q end
			// has a Q neighbour and is not. Same edge, opposite answers — this is
			// the case a role filter applied at the wrong end gets backwards.
			all:        expected{2, 1, "50.0%", "0:1 1:1"},
			structural: expected{1, 1, "100.0%", "0:1"},
			other:      expected{1, 0, "0.0%", "1:1"},
		},
		{
			name:       "Э-M5-5 ¬Q—¬Q: an edge that reaches nobody structural",
			roles:      []int{roleOther, roleOther},
			edges:      [][2]int{{0, 1}},
			all:        expected{2, 2, "100.0%", "0:2"},
			structural: expected{0, 0, "no data", "no data"},
			other:      expected{2, 2, "100.0%", "0:2"},
		},
		{
			name: "Э-M5-6 mixed: a Q node without a Q neighbour and a ¬Q pair nobody structural touches",
			roles: []int{
				roleStructural, roleStructural, roleStructural, roleStructural,
				roleOther, roleOther, roleOther, roleOther,
			},
			edges: [][2]int{{0, 1}, {1, 2}, {2, 4}, {4, 5}, {3, 5}, {6, 7}},
			// counts: 0→1, 1→2, 2→1, 3→0, 4→1, 5→1, 6→0, 7→0
			all:        expected{8, 3, "37.5%", "0:3 1:4 2:1"},
			structural: expected{4, 1, "25.0%", "0:1 1:2 2:1"},
			other:      expected{4, 2, "50.0%", "0:2 1:2"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			g := roleGraph(testCase.roles, testCase.edges)
			report, err := measureNeighbourlessTargets(g)
			if err != nil {
				t.Fatalf("measuring %s: %v", testCase.name, err)
			}

			slices := []struct {
				label string
				got   m5Slice
				want  expected
			}{
				{"all", report.All, testCase.all},
				{"Q", report.Structural, testCase.structural},
				{"¬Q", report.NonStructural, testCase.other},
			}
			for _, slice := range slices {
				if slice.got.Targets != slice.want.targets {
					t.Errorf("%s: denominator %d targets, want %d",
						slice.label, slice.got.Targets, slice.want.targets)
				}
				if slice.got.Without != slice.want.without {
					t.Errorf("%s: numerator %d without a Q neighbour, want %d",
						slice.label, slice.got.Without, slice.want.without)
				}
				if got := slice.got.Share(); got != slice.want.share {
					t.Errorf("%s: share %q, want %q", slice.label, got, slice.want.share)
				}
				if got := slice.got.Distribution(); got != slice.want.distribution {
					t.Errorf("%s: distribution %q, want %q",
						slice.label, got, slice.want.distribution)
				}
			}

			// The two slices must exhaust the population: a target belongs to
			// exactly one of them, so a role filter that drops or duplicates
			// nodes shows up here rather than in a plausible-looking share.
			if sum := report.Structural.Targets + report.NonStructural.Targets; sum != report.All.Targets {
				t.Errorf("Q + ¬Q = %d targets, total says %d", sum, report.All.Targets)
			}
			if sum := report.Structural.Without + report.NonStructural.Without; sum != report.All.Without {
				t.Errorf("Q + ¬Q = %d without, total says %d", sum, report.All.Without)
			}
		})
	}
}

// TestM5CountsEachEdgeOnceAndOnlyFromTheOtherEnd is the no-double-counting
// identity: a Q—Q edge is seen by both ends, a Q—¬Q edge only by its ¬Q end, a
// ¬Q—¬Q edge by neither.
func TestM5CountsEachEdgeOnceAndOnlyFromTheOtherEnd(t *testing.T) {
	fixtures := []struct {
		name  string
		roles []int
		edges [][2]int
	}{
		{"Э-M5-3", []int{roleStructural, roleStructural}, [][2]int{{0, 1}}},
		{"Э-M5-4", []int{roleStructural, roleOther}, [][2]int{{0, 1}}},
		{"Э-M5-5", []int{roleOther, roleOther}, [][2]int{{0, 1}}},
		{
			"Э-M5-6",
			[]int{
				roleStructural, roleStructural, roleStructural, roleStructural,
				roleOther, roleOther, roleOther, roleOther,
			},
			[][2]int{{0, 1}, {1, 2}, {2, 4}, {4, 5}, {3, 5}, {6, 7}},
		},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			g := roleGraph(fixture.roles, fixture.edges)
			report, err := measureNeighbourlessTargets(g)
			if err != nil {
				t.Fatalf("measuring: %v", err)
			}

			total := 0
			for _, count := range report.PerTarget {
				total += count
			}
			bothQ, mixed, _ := structuralEdgeMix(g)
			if want := 2*bothQ + mixed; total != want {
				t.Errorf("Q-neighbour counts sum to %d, but %d Q—Q edges and %d mixed edges "+
					"can only produce %d", total, bothQ, mixed, want)
			}
		})
	}
}

// TestM5IsNotIsolationOfTheStructuralSubgraph is requirement 1 made executable.
// The numerators coincide on the Q slice and the MEASUREMENTS still differ,
// because the populations differ — and the difference is not a rounding detail:
// on this fixture two of the three unreachable targets are ¬Q nodes that the
// subgraph analysis never looks at.
func TestM5IsNotIsolationOfTheStructuralSubgraph(t *testing.T) {
	g := roleGraph(
		[]int{
			roleStructural, roleStructural, roleStructural, roleStructural,
			roleOther, roleOther, roleOther, roleOther,
		},
		[][2]int{{0, 1}, {1, 2}, {2, 4}, {4, 5}, {3, 5}, {6, 7}},
	)

	report, err := measureNeighbourlessTargets(g)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}
	structural := analyseComponents(g, func(i int32) bool { return g.roles[i] == roleStructural })

	// Same numerator on the Q slice: a Q node with no Q neighbour is isolated in
	// the induced subgraph and is a target without a Q neighbour. If this ever
	// diverges, one of the two is counting something else.
	if structural.Isolated != report.Structural.Without {
		t.Fatalf("Q nodes isolated in the subgraph: %d, Q targets without a Q neighbour: %d — "+
			"these are the same nodes and must agree",
			structural.Isolated, report.Structural.Without)
	}

	// Different population: the subgraph has only Q members, M5 asks every node.
	if structural.Nodes != report.Structural.Targets {
		t.Fatalf("subgraph holds %d nodes, Q slice holds %d targets",
			structural.Nodes, report.Structural.Targets)
	}
	if report.All.Targets <= structural.Nodes {
		t.Fatalf("M5 population %d is not larger than the subgraph population %d — the fixture "+
			"cannot show the distinction", report.All.Targets, structural.Nodes)
	}
	if report.All.Without <= structural.Isolated {
		t.Fatalf("M5 counts %d targets without a Q neighbour, subgraph isolation counts %d — "+
			"the fixture must contain ¬Q targets that isolation cannot see",
			report.All.Without, structural.Isolated)
	}

	// And therefore the two shares are different numbers. Reporting one as the
	// other is the mistake this test exists to make impossible.
	if report.All.Share() == report.Structural.Share() {
		t.Fatalf("total share %s equals the Q share %s — the fixture no longer distinguishes "+
			"the two measurements", report.All.Share(), report.Structural.Share())
	}
	if got, want := report.All.Share(), "37.5%"; got != want {
		t.Errorf("share of ALL targets without a Q neighbour = %s, want %s", got, want)
	}
	if got, want := report.Structural.Share(), "25.0%"; got != want {
		t.Errorf("share of Q targets without a Q neighbour = %s, want %s", got, want)
	}
}

// TestM5AgreesWithTheMaintainedCounter is requirement 4: the independent recount
// and the counter maintained during construction must agree on a real built
// graph, and a disagreement must stop the measurement instead of reporting a
// number.
func TestM5AgreesWithTheMaintainedCounter(t *testing.T) {
	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	g := buildGraph(sh, 1, 0, policyBaseline)

	report, err := measureNeighbourlessTargets(g)
	if err != nil {
		t.Fatalf("measuring a built graph: %v", err)
	}
	for i, count := range report.PerTarget {
		if count != g.structuralNeighbours[i] {
			t.Fatalf("node %d: recount %d, maintained counter %d", i, count, g.structuralNeighbours[i])
		}
	}

	// A drifted counter is a stand defect, not a measurement.
	g.structuralNeighbours[7]++
	if _, err := measureNeighbourlessTargets(g); err == nil {
		t.Fatal("a counter that disagrees with the edges produced a report instead of an error")
	} else if !strings.Contains(err.Error(), "node 7") {
		t.Fatalf("error does not name the disagreeing node: %v", err)
	}
}

// TestM5OnABuiltGraphKeepsThePopulationsApart checks the invariants that must
// hold on any built graph, without asserting any C1-style number: those belong
// to point B of the queue, on an agreed candidate.
func TestM5OnABuiltGraphKeepsThePopulationsApart(t *testing.T) {
	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	g := buildGraph(sh, 4, 2, policyBaseline)

	report, err := measureNeighbourlessTargets(g)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	if report.All.Targets != sh.nodes {
		t.Errorf("denominator %d, want every node of the graph (%d)", report.All.Targets, sh.nodes)
	}
	if sum := report.Structural.Targets + report.NonStructural.Targets; sum != sh.nodes {
		t.Errorf("Q + ¬Q = %d targets, graph holds %d", sum, sh.nodes)
	}
	if report.Structural.Targets == 0 || report.NonStructural.Targets == 0 {
		t.Fatal("one of the roles is empty — the shape cannot demonstrate the split")
	}

	for _, slice := range []m5Slice{report.All, report.Structural, report.NonStructural} {
		sum := 0
		for _, targets := range slice.ByCount {
			sum += targets
		}
		if sum != slice.Targets {
			t.Errorf("distribution covers %d targets, slice holds %d", sum, slice.Targets)
		}
		if len(slice.ByCount) > 0 && slice.ByCount[0] != slice.Without {
			t.Errorf("distribution says %d targets with zero Q neighbours, numerator says %d",
				slice.ByCount[0], slice.Without)
		}
	}

	structural := analyseComponents(g, func(i int32) bool { return g.roles[i] == roleStructural })
	if structural.Isolated != report.Structural.Without {
		t.Errorf("isolated Q nodes %d, Q targets without a Q neighbour %d — on a built graph "+
			"these are the same set", structural.Isolated, report.Structural.Without)
	}

	total := 0
	for _, count := range report.PerTarget {
		total += count
	}
	bothQ, mixed, _ := structuralEdgeMix(g)
	if want := 2*bothQ + mixed; total != want {
		t.Errorf("Q-neighbour counts sum to %d, edges allow %d", total, want)
	}
}

// TestM5RejectsAdjacencyItCannotCount covers the three bookkeeping defects. Each
// would silently change a count rather than fail, which is why they are errors
// and not results.
func TestM5RejectsAdjacencyItCannotCount(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(g *graph)
		wants   string
	}{
		{
			name:    "self-loop",
			corrupt: func(g *graph) { g.adjacency[0] = append(g.adjacency[0], 0) },
			wants:   "own neighbour",
		},
		{
			name: "one edge listed twice",
			corrupt: func(g *graph) {
				g.adjacency[0] = append(g.adjacency[0], 1)
				g.adjacency[1] = append(g.adjacency[1], 0)
			},
			wants: "more than once",
		},
		{
			name:    "edge present at one end only",
			corrupt: func(g *graph) { g.adjacency[0] = append(g.adjacency[0], 2) },
			wants:   "disagree",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			g := roleGraph(
				[]int{roleStructural, roleStructural, roleOther},
				[][2]int{{0, 1}},
			)
			testCase.corrupt(g)

			_, err := measureNeighbourlessTargets(g)
			if err == nil {
				t.Fatalf("%s produced a report instead of an error", testCase.name)
			}
			if !strings.Contains(err.Error(), testCase.wants) {
				t.Fatalf("error %q does not explain %s", err, testCase.name)
			}
		})
	}
}

// TestM5EmptyPopulationSaysNoData keeps "nothing was measured" from arriving as
// "nothing is wrong". A graph of ¬Q nodes only has an empty Q slice, and that
// slice must not read 0.0 %.
func TestM5EmptyPopulationSaysNoData(t *testing.T) {
	g := roleGraph([]int{roleOther, roleOther}, [][2]int{{0, 1}})
	report, err := measureNeighbourlessTargets(g)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	if got := report.Structural.Share(); got != "no data" {
		t.Errorf("empty Q population share = %q, want %q", got, "no data")
	}
	if got := report.Structural.Distribution(); got != "no data" {
		t.Errorf("empty Q population distribution = %q, want %q", got, "no data")
	}
	if got := report.Structural.String(); got != "no data" {
		t.Errorf("empty Q population = %q, want %q", got, "no data")
	}
	if strings.Contains(report.String(), "0.0% ") && report.Structural.Targets == 0 {
		t.Errorf("empty population rendered as a percentage: %s", report)
	}

	// The populated slice still prints a real number, so "no data" is not a
	// blanket answer.
	if got, want := report.NonStructural.Share(), "100.0%"; got != want {
		t.Errorf("¬Q share = %q, want %q", got, want)
	}
}

// TestM5DistributionRendersWhatItCounted guards the histogram format itself: it
// is the part of the report that is read by eye, and a format test that only
// looks for a substring would pass on a shifted rendering.
func TestM5DistributionRendersWhatItCounted(t *testing.T) {
	g := roleGraph(
		[]int{roleStructural, roleStructural, roleStructural, roleOther},
		[][2]int{{0, 1}, {1, 2}},
	)
	report, err := measureNeighbourlessTargets(g)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	// counts: 0→1, 1→2, 2→1, 3→0
	want := histogramOf(map[int]int{0: 1, 1: 2, 2: 1})
	if got := report.All.Distribution(); got != want {
		t.Errorf("distribution %q, want %q", got, want)
	}
	if got, want := report.All.String(),
		"1 of 4 without a Q neighbour (25.0%), distribution 0:1 1:2 2:1"; got != want {
		t.Errorf("slice rendered as %q, want %q", got, want)
	}
}
