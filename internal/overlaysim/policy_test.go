package overlaysim

// policy_test.go is the acceptance of the POLICY COMPARISON, §2.4 of
// docs/refactoring/dht/21-m1-connectivity-model.md: the baseline still produces
// the numbers already published, every policy honours B at both ends of every
// edge, and the three are told apart by what they DO rather than by the size of
// the answer they give.
//
// ⚠️ None of this says which policy is better. That is a number, and the number
// belongs to the owner (16a). O5 stays open either way.

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// allPolicies is the comparison set. Kept in one place so a policy added later
// cannot quietly skip the invariants below.
//
// ⚠️ C1/v1 joined it when the candidate was implemented, and joining THIS list
// is what subjects it to the budget, symmetry and initiated-limit invariants
// below without anybody remembering to add it to each of them.
var allPolicies = []policy{
	policyBaseline, policyInitiatedLimit, policySecondPass, policyCandidateC1,
}

// --- the full-depth shapes sit beside the plan, not inside it ----------------

// TestFullDepthShapesSitBesideThePlanNotInsideIt pins the full-depth forms and
// the d = 16 candidate's: exactly three full-depth forms, B = 2d, deep enough
// to reach every populated XOR level, and reachable by name from the M1
// comparison and the M2 driver — while the measurement plan itself, whose
// shapes TestM1ConnectivitySweep intersects its verdict over, stays as it was.
//
// ⚠️ The plan is pinned literally. A shape slipped into sweepShapes would not
// fail any other test; it would quietly change the cross-shape verdict that
// the published M1 results were read from.
func TestFullDepthShapesSitBesideThePlanNotInsideIt(t *testing.T) {
	plan := []shape{
		{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
		{name: "10k×8", nodes: 10_000, degree: 8, budget: 16},
		{name: "10k×64", nodes: 10_000, degree: 64, budget: 128},
		{name: "64k×8", nodes: 64_000, degree: 8, budget: 16},
	}
	fullDepth := []shape{
		{name: "1k×12", nodes: 1_000, degree: 12, budget: 24},
		{name: "1k×16", nodes: 1_000, degree: 16, budget: 32},
		{name: "10k×16", nodes: 10_000, degree: 16, budget: 32},
	}

	if !slices.Equal(sweepShapes, plan) {
		t.Fatalf("the measurement plan moved: sweepShapes = %v, want %v", sweepShapes, plan)
	}
	if !slices.Equal(fullDepthShapes, fullDepth) {
		t.Fatalf("fullDepthShapes = %v, want exactly the full-depth forms and the d = 16 candidate's: %v",
			fullDepthShapes, fullDepth)
	}
	for _, sh := range fullDepthShapes {
		if sh.budget != 2*sh.degree {
			t.Fatalf("%s: B = %d, the full-depth forms fix B = 2d = %d", sh.name, sh.budget, 2*sh.degree)
		}
		if levels := int(math.Ceil(math.Log2(float64(sh.nodes)))); sh.degree < levels+2 {
			t.Fatalf("%s: d = %d is below ⌈log₂N⌉ + 2 = %d, so the table cannot reach every "+
				"populated level and the experiment would not test what it claims", sh.name,
				sh.degree, levels+2)
		}
		if slices.ContainsFunc(sweepShapes, func(planned shape) bool { return planned.name == sh.name }) {
			t.Fatalf("%s is in the measurement plan: TestM1ConnectivitySweep would intersect "+
				"its verdict over it", sh.name)
		}
	}

	if got := comparisonShapes(); !slices.Equal(got, slices.Concat(plan, fullDepth)) {
		t.Fatalf("comparisonShapes() = %v, want the plan in its order followed by the "+
			"full-depth forms", got)
	}

	t.Run("M2_SHAPES names the full-depth forms", func(t *testing.T) {
		t.Setenv("M2_SHAPES", "1k×12,1k×16,10k×16")
		if got := m2ShapesFromEnvironment(t); !slices.Equal(got, fullDepth) {
			t.Fatalf("M2_SHAPES=1k×12,1k×16,10k×16 selected %v", got)
		}
	})
	t.Run("the M2 default is still the two d = 8 forms", func(t *testing.T) {
		t.Setenv("M2_SHAPES", "")
		if got := m2ShapesFromEnvironment(t); !slices.Equal(got, plan[:2]) {
			t.Fatalf("unset M2_SHAPES selected %v, want %v", got, plan[:2])
		}
	})
}

// TestEveryDriverAcceptsExactlyTheShapesItIsAllowed pins, driver by driver,
// which drivers measure the d = 16 candidate: M2/M5, M4 and M3-a take the
// d = 16 forms by name, M6 does not (its grid is registered for d = 8 alone),
// and every driver run with its variable unset measures exactly what it measured
// before the forms existed.
//
// ⚠️ The refusal is the half that matters. One shared list of accepted names
// would have opened M6 to the new forms as a side effect, and nothing else in
// the package would have noticed.
func TestEveryDriverAcceptsExactlyTheShapesItIsAllowed(t *testing.T) {
	candidate := []shape{
		{name: "1k×16", nodes: 1_000, degree: 16, budget: 32},
		{name: "10k×16", nodes: 10_000, degree: 16, budget: 32},
	}
	tenK8 := []shape{{name: "10k×8", nodes: 10_000, degree: 8, budget: 16}}
	smallPlan := []shape{
		{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
		{name: "10k×8", nodes: 10_000, degree: 8, budget: 16},
	}

	for _, driver := range []struct {
		selection shapeSelection
		fallback  []shape
		takesD16  bool
	}{
		{m2ShapeSelection(), smallPlan, true},
		{m4ShapeSelection(), tenK8, true},
		{m3aShapeSelection(), tenK8, true},
		{m6ShapeSelection(), smallPlan, false},
	} {
		name := driver.selection.Variable
		t.Run(name+" unset keeps the old default", func(t *testing.T) {
			got, err := driver.selection.parse("")
			if err != nil {
				t.Fatalf("%s unset: %v", name, err)
			}
			if !slices.Equal(got, driver.fallback) {
				t.Fatalf("%s unset selected %v, want %v", name, got, driver.fallback)
			}
		})
		t.Run(name+" on the d = 16 forms", func(t *testing.T) {
			got, err := driver.selection.parse(" 1k×16 , 10k×16 ")
			if !driver.takesD16 {
				if err == nil {
					t.Fatalf("%s accepted the d = 16 forms (%v) — this driver does not "+
						"measure the d = 16 candidate", name, got)
				}
				if !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "1k×16") {
					t.Fatalf("%s: the refusal does not name the variable and the form: %v", name, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s refused the d = 16 forms: %v", name, err)
			}
			if !slices.Equal(got, candidate) {
				t.Fatalf("%s=1k×16,10k×16 selected %v, want %v", name, got, candidate)
			}
		})
		t.Run(name+" refuses a name nobody defined", func(t *testing.T) {
			if got, err := driver.selection.parse("1k×14"); err == nil {
				t.Fatalf("%s=1k×14 was accepted as %v — d = 12–14 is not a candidate to measure", name, got)
			}
		})
	}
}

// TestSingleQuotaSelectionRunsOnlyWhatWasAsked covers the quota M4 and M3-a are
// measured at. They run only after the neighbour-selection quota is fixed, and
// the quota is a run input, so it is read from M4_QUOTA / M3A_QUOTA rather than
// edited into the sources — which would change the stamp a second time.
//
// ⚠️ Same three outcomes as M1_QUOTAS: absent runs the default, malformed or
// out of range is REFUSED. A quota the operator did not ask for must not run
// as if they had.
func TestSingleQuotaSelectionRunsOnlyWhatWasAsked(t *testing.T) {
	d8 := []shape{{name: "10k×8", nodes: 10_000, degree: 8, budget: 16}}
	d16 := []shape{{name: "10k×16", nodes: 10_000, degree: 16, budget: 32}}
	both := slices.Concat(d8, d16)

	for _, selection := range []quotaSelection{m4QuotaSelection(), m3aQuotaSelection()} {
		name := selection.Variable
		if selection.Fallback != 1 {
			t.Fatalf("%s: the default quota is %d, the measured configuration is quota 1",
				name, selection.Fallback)
		}
		for _, good := range []struct {
			raw    string
			shapes []shape
			want   int
		}{
			{"", d16, 1},
			{" 4 ", d16, 4},
			{"16", d16, 16},
			{"0", both, 0},
			{"8", both, 8},
		} {
			got, err := selection.parse(good.raw, good.shapes)
			if err != nil {
				t.Fatalf("%s=%q on %v: %v", name, good.raw, good.shapes, err)
			}
			if got != good.want {
				t.Fatalf("%s=%q on %v gave %d, want %d", name, good.raw, good.shapes, got, good.want)
			}
		}
		for _, bad := range []struct {
			raw    string
			shapes []shape
		}{
			{"two", d16},
			{"1,2", d16},
			{"0-4", d16},
			{"-1", d16},
			{"17", d16},
			{"9", both}, // within d = 16, beyond d = 8: a run over both may not clip one away
		} {
			if got, err := selection.parse(bad.raw, bad.shapes); err == nil {
				t.Fatalf("%s=%q on %v was accepted as %d", name, bad.raw, bad.shapes, got)
			} else if !strings.Contains(err.Error(), name) {
				t.Fatalf("%s=%q: the refusal does not name the variable: %v", name, bad.raw, err)
			}
		}
	}
}

// --- the quota grid is an instruction, not a suggestion ----------------------

// TestQuotaGridRejectsWhatItCannotRun covers the three outcomes M1_QUOTAS must
// keep apart: absent, parsed, malformed.
//
// ⚠️ The first version had only two, folding "malformed" into "absent": a typo
// ran the DEFAULT grid and reported it, correctly, as the grid it used. The
// operator asked for one measurement and got another, with nothing anywhere
// saying so. And M1_QUOTAS=9-10 on a degree-eight shape produced an empty grid,
// zero measurements and a PASS — a green result that measured nothing.
func TestQuotaGridRejectsWhatItCannotRun(t *testing.T) {
	// No t.Parallel anywhere here: these set an environment variable, which is
	// process-wide.

	small := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	large := shape{name: "10k×64", nodes: 10_000, degree: 64, budget: 128}

	t.Run("absent asks for the default grid", func(t *testing.T) {
		t.Setenv("M1_QUOTAS", "")

		quotas, err := comparisonQuotas(small)
		if err != nil {
			t.Fatalf("unset M1_QUOTAS is not an error: %v", err)
		}
		if len(quotas) != small.degree+1 {
			t.Fatalf("default grid for d=%d is %v", small.degree, quotas)
		}

		coarse, err := comparisonQuotas(large)
		if err != nil {
			t.Fatalf("unset M1_QUOTAS is not an error: %v", err)
		}
		if len(coarse) >= large.degree+1 {
			t.Fatalf("the degree-64 grid should be coarse by default, got %d quotas", len(coarse))
		}
	})

	for _, bad := range []struct {
		name  string
		value string
	}{
		{"a typo in a range", "0-1O"},        // letter O, not zero
		{"a typo in a list", "0,1,two"},      //
		{"a backwards range", "32-8"},        //
		{"a range of nothing", "-"},          //
		{"a range with a missing end", "4-"}, //
		{"a list of nothing", ","},           //
		{"words", "all"},                     //
		{"a float", "0-2.5"},                 //
		{"out of range entirely", "9-10"},    // valid syntax, nothing to run
		{"negative only", "-3,-1"},           // parses as a range "3,-1"→bad
		{"beyond the degree", "100,200"},     //
	} {
		t.Run("rejected: "+bad.name, func(t *testing.T) {
			t.Setenv("M1_QUOTAS", bad.value)

			quotas, err := comparisonQuotas(small)
			if err == nil {
				t.Fatalf("M1_QUOTAS=%q was accepted and produced %v — a value the operator did "+
					"not ask for must not run as if they had", bad.value, quotas)
			}
			if len(quotas) != 0 {
				t.Fatalf("M1_QUOTAS=%q errored but still returned %v", bad.value, quotas)
			}
			if !strings.Contains(err.Error(), bad.value) && !strings.Contains(err.Error(), "9 10") &&
				!strings.Contains(err.Error(), "100 200") {
				t.Errorf("M1_QUOTAS=%q: the error does not name the offending value: %v",
					bad.value, err)
			}
		})
	}

	for _, good := range []struct {
		name  string
		value string
		want  []int
	}{
		{"a range", "2-5", []int{2, 3, 4, 5}},
		{"a single quota as a list", "3", []int{3}},
		{"a list", "0,3,7", []int{0, 3, 7}},
		{"a range clipped to the shape", "6-12", []int{6, 7, 8}},
		{"whitespace is tolerated", " 1 , 2 ", []int{1, 2}},
	} {
		t.Run("accepted: "+good.name, func(t *testing.T) {
			t.Setenv("M1_QUOTAS", good.value)

			quotas, err := comparisonQuotas(small)
			if err != nil {
				t.Fatalf("M1_QUOTAS=%q: %v", good.value, err)
			}
			if fmt.Sprint(quotas) != fmt.Sprint(good.want) {
				t.Fatalf("M1_QUOTAS=%q gave %v, want %v", good.value, quotas, good.want)
			}
		})
	}

	t.Run("a clipped range still says which quotas ran", func(t *testing.T) {
		t.Setenv("M1_QUOTAS", "6-12")

		// Clipping is allowed — asking a degree-eight shape about quota 12 is
		// not a mistake, it is out of its range — but the run must then be
		// about 6..8 and the report must print that, which is what the caller
		// uses. The guard here is that clipping never silently empties.
		quotas, err := comparisonQuotas(small)
		if err != nil {
			t.Fatalf("clipping is not an error: %v", err)
		}
		if len(quotas) == 0 {
			t.Fatal("clipping produced an empty grid without an error")
		}
	})
}

// --- the summary must not lose what it summarises ---------------------------

// TestSummaryKeepsTheWorstOfEachColumnSeparately fixes a defect the summary had
// by construction: the isolated count was carried on the same variable as the
// worst-share report, so a later seed with a smaller largest component replaced
// it wholesale and took a larger isolated count down with it.
//
// The maximum of one column and the argmin of another are different reductions.
// Sharing a variable between them silently reports the wrong number, and the
// number it drops is the one the whole comparison is about.
func TestSummaryKeepsTheWorstOfEachColumnSeparately(t *testing.T) {
	t.Parallel()

	report := func(share float64, isolated, links int) runReport {
		// Largest/Nodes gives the share; the exact pair does not matter, only
		// the ordering it produces.
		return runReport{
			Structural: componentReport{
				Nodes:    1000,
				Largest:  int(share * 1000),
				Isolated: isolated,
			},
			Links: links,
		}
	}

	t.Run("a later, worse-share seed does not erase an earlier maximum", func(t *testing.T) {
		t.Parallel()

		// Exactly the order from the review: ten isolated first, then a seed
		// with only two isolated but a smaller largest component.
		got := summariseSeeds([]runReport{
			report(0.99, 10, 100),
			report(0.50, 2, 200),
		})

		if got.Structural.Isolated != 10 {
			t.Errorf("isolated %d, the worst seed had 10", got.Structural.Isolated)
		}
		if share := got.Structural.LargestShare(); share != 0.5 {
			t.Errorf("largest share %v, the worst seed had 0.5", share)
		}
		if got.Links != 150 {
			t.Errorf("links %d, the mean of 100 and 200 is 150", got.Links)
		}
	})

	t.Run("the same seeds in the other order give the same summary", func(t *testing.T) {
		t.Parallel()

		// The reduction must not depend on which seed happened to run first.
		forwards := summariseSeeds([]runReport{
			report(0.99, 10, 100),
			report(0.50, 2, 200),
		})
		backwards := summariseSeeds([]runReport{
			report(0.50, 2, 200),
			report(0.99, 10, 100),
		})

		if forwards.Structural.Isolated != backwards.Structural.Isolated ||
			forwards.Structural.LargestShare() != backwards.Structural.LargestShare() ||
			forwards.Links != backwards.Links {
			t.Fatalf("order changed the summary: %+v vs %+v",
				forwards.Structural, backwards.Structural)
		}
	})

	t.Run("an empty set of runs summarises to nothing rather than panicking", func(t *testing.T) {
		t.Parallel()

		if got := summariseSeeds(nil); got.Links != 0 || got.Structural.Isolated != 0 {
			t.Fatalf("empty summary: %+v", got)
		}
	})
}

// --- the baseline is a control, so it must not have moved -------------------

// TestBaselineReproducesThePublishedRun pins the numbers that
// 21-m1-connectivity-results.md §2 already reports. Adding a policy parameter
// to the model is exactly the kind of change that shifts the control by
// accident, and a comparison against a moved baseline compares nothing.
func TestBaselineReproducesThePublishedRun(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}

	// Worst over the five published seeds, as the results table reports it.
	for _, want := range []struct {
		quota    int
		isolated int
		share    string
	}{
		{quota: 0, isolated: 2, share: "0.9961"},
		{quota: 3, isolated: 3, share: "0.9940"},
		{quota: 8, isolated: 5, share: "0.9901"},
	} {
		worstIsolated, minShare := 0, 1.0
		for _, seed := range sweepSeeds {
			report := measure(sh, seed, want.quota, policyBaseline)
			if report.Structural.Isolated > worstIsolated {
				worstIsolated = report.Structural.Isolated
			}
			if share := report.Structural.LargestShare(); share < minShare {
				minShare = share
			}
		}

		if worstIsolated != want.isolated {
			t.Errorf("1k×8 quota %d: worst isolated %d, the published run says %d",
				want.quota, worstIsolated, want.isolated)
		}
		if got := fmt.Sprintf("%.4f", minShare); got != want.share {
			t.Errorf("1k×8 quota %d: worst Q share %s, the published run says %s",
				want.quota, got, want.share)
		}
	}
}

// --- invariants every policy owes -------------------------------------------

// TestEveryPolicyRespectsTheBudgetAtBothEnds is the one guarantee no policy is
// allowed to buy its improvement with. B is a ceiling on TOTAL connections and
// it applies to the node that dialled and to the node that answered alike.
func TestEveryPolicyRespectsTheBudgetAtBothEnds(t *testing.T) {
	t.Parallel()

	shapes := []shape{
		{name: "200×4", nodes: 200, degree: 4, budget: 8},
		{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
		// Deliberately tight: B only one above d leaves almost no room for the
		// second pass, which is where a policy that "just adds links" breaks.
		{name: "500×6/B7", nodes: 500, degree: 6, budget: 7},
	}

	for _, selection := range allPolicies {
		for _, sh := range shapes {
			for _, quota := range []int{0, sh.degree / 2, sh.degree} {
				g := buildGraph(sh, 1, quota, selection)

				seen := make([]map[int32]int, sh.nodes)
				for i := range seen {
					seen[i] = map[int32]int{}
				}

				for i := range sh.nodes {
					u := int32(i)
					if degree := len(g.adjacency[i]); degree > sh.budget {
						t.Fatalf("%s %s quota=%d: node %d has degree %d over budget %d",
							selection, sh.name, quota, i, degree, sh.budget)
					}
					for _, v := range g.adjacency[i] {
						if v == u {
							t.Fatalf("%s %s: node %d links to itself", selection, sh.name, i)
						}
						seen[i][v]++
						if seen[i][v] > 1 {
							t.Fatalf("%s %s: node %d links to %d twice", selection, sh.name, i, v)
						}
					}
				}

				// Symmetry is what makes "both ends" meaningful: a link the
				// other end does not know about would consume no slot there.
				for i := range sh.nodes {
					for _, v := range g.adjacency[i] {
						if seen[v][int32(i)] != 1 {
							t.Fatalf("%s %s: %d lists %d as a neighbour, %d does not list %d back",
								selection, sh.name, i, v, v, i)
						}
					}
				}
			}
		}
	}
}

// TestInitiatedLinksStayWithinThePolicyThatOwnsThem states the OTHER limit,
// which is not the same for every policy — and saying so explicitly is the point.
func TestInitiatedLinksStayWithinThePolicyThatOwnsThem(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	const quota = 8

	for _, selection := range allPolicies {
		g := buildGraph(sh, 1, quota, selection)

		maxInitiated, maxDegree := 0, 0
		for i := range sh.nodes {
			if g.initiated[i] > maxInitiated {
				maxInitiated = g.initiated[i]
			}
			if len(g.adjacency[i]) > maxDegree {
				maxDegree = len(g.adjacency[i])
			}
		}

		switch selection {
		case policySecondPass:
			// The repair pass is ALLOWED past the desired degree — that is its
			// declared cost (§2.4) — but never past B.
			if maxDegree > sh.budget {
				t.Fatalf("%s: degree %d over budget %d", selection, maxDegree, sh.budget)
			}
		default:
			if maxInitiated > sh.degree {
				t.Fatalf("%s: a node initiated %d links against a desired degree of %d",
					selection, maxInitiated, sh.degree)
			}
		}
	}
}

// --- the policies are different things, not different numbers ---------------

// TestPoliciesAreDistinguishableOnAFixedGraph checks each policy by the
// MECHANISM its rules describe, on one small graph with fixed inputs.
//
// ⚠️ Comparing only the outcome would let two policies pass as "different"
// because a graph came out differently, and would let a policy that silently
// did nothing pass as "the same". Each assertion below is a property the rules
// of §2.4 promise and the other policies cannot satisfy.
func TestPoliciesAreDistinguishableOnAFixedGraph(t *testing.T) {
	t.Parallel()

	sh := shape{name: "300×4", nodes: 300, degree: 4, budget: 8}
	const quota = 4

	baseline := buildGraph(sh, 1, quota, policyBaseline)
	initiated := buildGraph(sh, 1, quota, policyInitiatedLimit)
	second := buildGraph(sh, 1, quota, policySecondPass)

	countLinks := func(g *graph) int {
		total := 0
		for i := range sh.nodes {
			total += len(g.adjacency[i])
		}
		return total / 2
	}

	t.Run("the baseline lets incoming links end a node's search", func(t *testing.T) {
		t.Parallel()

		// The defining property of policy 1 and the reason the isolation
		// exists: a node at its desired degree stops, even though most of that
		// degree was other nodes' doing.
		stoppedByOthers := 0
		for i := range sh.nodes {
			if len(baseline.adjacency[i]) >= sh.degree && baseline.initiated[i] < sh.degree {
				stoppedByOthers++
			}
		}
		if stoppedByOthers == 0 {
			t.Fatal("no node was stopped by incoming links — this graph cannot show the " +
				"difference the other policies are meant to remove")
		}
		t.Logf("baseline: %d nodes stopped short by links others made to them", stoppedByOthers)
	})

	t.Run("the initiated limit stops counting other nodes' links", func(t *testing.T) {
		t.Parallel()

		// Under policy 2 the same nodes keep going. Any node that still
		// initiated fewer than d links must have been stopped by B or by
		// running out of candidates — never by its own incoming links.
		for i := range sh.nodes {
			if initiated.initiated[i] >= sh.degree {
				continue
			}
			if len(initiated.adjacency[i]) < sh.budget {
				continue // ran out of candidates, which the rules allow
			}
			// At B: allowed. Anything else would mean incoming links still
			// consumed the search.
			if len(initiated.adjacency[i]) != sh.budget {
				t.Fatalf("node %d initiated %d of %d links and sits at degree %d, neither at "+
					"budget %d nor out of candidates",
					i, initiated.initiated[i], sh.degree, len(initiated.adjacency[i]), sh.budget)
			}
		}

		if countLinks(initiated) <= countLinks(baseline) {
			t.Fatalf("initiated-limit built %d links, baseline %d — the policy that stops "+
				"counting incoming links cannot build fewer",
				countLinks(initiated), countLinks(baseline))
		}
	})

	t.Run("the second pass spends free B, which the baseline never does", func(t *testing.T) {
		t.Parallel()

		// Policy 3's signature: a node past its DESIRED degree. The baseline
		// cannot produce one by initiating — only by being dialled — so the
		// test looks for a node that went past d by its own repair links.
		repaired, overDesired := 0, 0
		for i := range sh.nodes {
			if second.shortfall[i].SecondPassLinks > 0 {
				repaired++
			}
			if second.initiated[i] > sh.degree {
				overDesired++
			}
		}

		if repaired == 0 {
			t.Fatal("the second pass added no link at all — either every node met its quota " +
				"in the first pass, in which case this graph proves nothing, or the pass is inert")
		}
		if overDesired == 0 {
			t.Fatal("no node initiated past its desired degree — the second pass is supposed " +
				"to spend free B, and this graph does not show it doing so")
		}
		for i := range sh.nodes {
			if baseline.initiated[i] > sh.degree {
				t.Fatalf("baseline node %d initiated %d links past the desired degree %d — "+
					"then the property above does not separate the policies",
					i, baseline.initiated[i], sh.degree)
			}
		}

		t.Logf("second pass: %d nodes repaired, %d went past the desired degree, %d links "+
			"against the baseline's %d", repaired, overDesired, countLinks(second), countLinks(baseline))
	})

	t.Run("all three see the same identifiers and roles", func(t *testing.T) {
		t.Parallel()

		// Without this the comparison would be between different networks and
		// every difference below would be unattributable.
		for i := range sh.nodes {
			if baseline.ids[i] != initiated.ids[i] || baseline.ids[i] != second.ids[i] {
				t.Fatalf("node %d has a different identifier under different policies", i)
			}
			if baseline.roles[i] != initiated.roles[i] || baseline.roles[i] != second.roles[i] {
				t.Fatalf("node %d has a different role under different policies", i)
			}
		}
	})
}

// TestSecondPassFailuresAreTwoDifferentThings separates the two ways the repair
// can leave a node short: the network had nobody left to give it, or the node
// had no room left to take. Only the second is about capacity, and the whole
// reading of the comparison depends on telling them apart — a merged counter
// would let "the second pass ran out of candidates" stand in for "the second
// pass ran out of B", which points at a different fix entirely.
func TestSecondPassFailuresAreTwoDifferentThings(t *testing.T) {
	t.Parallel()

	t.Run("running out of one's own budget is recorded as that", func(t *testing.T) {
		t.Parallel()

		// ⚠️ BOTH shapes, because a node can run out of budget in two places:
		// before the repair starts, and part-way through it. A tight B is
		// almost entirely the first — checking only that shape left the
		// second unguarded, and a mutation moving it to the other label went
		// through unnoticed.
		for _, sh := range []shape{
			{name: "1k×8/B9", nodes: 1_000, degree: 8, budget: 9},
			{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
		} {
			g := buildGraph(sh, 1, sh.degree, policySecondPass)

			outOfBudget, foundNobody := 0, 0
			for i := range sh.nodes {
				reasons := g.shortfall[i]
				if reasons.SecondPassOutOfBudget > 0 {
					outOfBudget++
					if degree := len(g.adjacency[i]); degree != sh.budget {
						t.Fatalf("%s: node %d is credited with running out of budget at degree "+
							"%d of %d", sh.name, i, degree, sh.budget)
					}
				}
				if reasons.SecondPassFoundNobody > 0 {
					foundNobody++

					// The labels must not be interchangeable. A node at its
					// ceiling did not "find nobody" — it had nowhere to put
					// them, and the two point at different fixes.
					if degree := len(g.adjacency[i]); degree >= sh.budget {
						t.Fatalf("%s: node %d is credited with finding nobody while sitting at "+
							"budget %d", sh.name, i, sh.budget)
					}
				}
			}

			if outOfBudget == 0 {
				t.Fatalf("%s: nobody ran out of budget — this fixture cannot show the capacity "+
					"case at all", sh.name)
			}
			t.Logf("%s: out of own budget %d, found nobody %d", sh.name, outOfBudget, foundNobody)
		}
	})

	t.Run("running out of candidates is recorded as that", func(t *testing.T) {
		t.Parallel()

		// A quota larger than the structural half of a tiny network: room to
		// spare, nobody left to spend it on.
		sh := shape{name: "12×8/B24", nodes: 12, degree: 8, budget: 24}
		g := buildGraph(sh, 1, sh.degree, policySecondPass)

		foundNobody := 0
		for i := range sh.nodes {
			if g.shortfall[i].SecondPassFoundNobody == 0 {
				continue
			}
			foundNobody++

			// It must really have had room — otherwise this is the budget case
			// wearing the other label.
			if degree := len(g.adjacency[i]); degree >= sh.budget {
				t.Fatalf("node %d is credited with finding nobody while sitting at budget %d",
					i, sh.budget)
			}
		}

		if foundNobody == 0 {
			t.Fatal("nobody ran out of candidates in a network of twelve with a quota of eight")
		}
	})

	t.Run("every node the repair left short says why", func(t *testing.T) {
		t.Parallel()

		// Completeness, the same rule the first pass follows: a shortfall with
		// no reason attached is what lets a plausible story replace a
		// measurement.
		for _, sh := range []shape{
			{name: "1k×8/B9", nodes: 1_000, degree: 8, budget: 9},
			{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
			{name: "12×8/B24", nodes: 12, degree: 8, budget: 24},
		} {
			g := buildGraph(sh, 1, sh.degree, policySecondPass)

			for i := range sh.nodes {
				if g.structuralNeighbours[i] >= sh.degree {
					continue
				}
				reasons := g.shortfall[i]
				if reasons.SecondPassOutOfBudget == 0 && reasons.SecondPassFoundNobody == 0 {
					t.Fatalf("%s: node %d is short after the repair with neither reason recorded: "+
						"%+v", sh.name, i, reasons)
				}
			}
		}
	})
}

// TestSecondPassIgnoresConnectivityAndLooksOnlyAtItsOwnQuota guards the rule
// that keeps the whole experiment honest: the repair may be triggered by a
// node's own unmet quota and by nothing else.
//
// A pass that consulted the component analysis would be arranging the very
// result the run then reports.
func TestSecondPassIgnoresConnectivityAndLooksOnlyAtItsOwnQuota(t *testing.T) {
	t.Parallel()

	sh := shape{name: "300×4", nodes: 300, degree: 4, budget: 8}

	// Quota zero: nobody's quota is unmet, so the repair has no trigger and
	// the graph must come out identical to the baseline — however
	// disconnected the structural subgraph is.
	baseline := buildGraph(sh, 1, 0, policyBaseline)
	second := buildGraph(sh, 1, 0, policySecondPass)

	structural := analyseComponents(second, func(i int32) bool {
		return second.roles[i] == roleStructural
	})
	if structural.Components < 2 {
		t.Fatalf("the structural subgraph is in %d component(s) — with nothing to repair, this "+
			"case cannot show that connectivity is not what triggers the pass", structural.Components)
	}

	for i := range sh.nodes {
		if len(baseline.adjacency[i]) != len(second.adjacency[i]) {
			t.Fatalf("node %d has %d neighbours under the baseline and %d under the second pass, "+
				"with a quota of zero: the pass fired on something other than an unmet quota",
				i, len(baseline.adjacency[i]), len(second.adjacency[i]))
		}
		if second.shortfall[i].SecondPassLinks != 0 {
			t.Fatalf("node %d got %d repair links with a quota of zero",
				i, second.shortfall[i].SecondPassLinks)
		}
	}
}
