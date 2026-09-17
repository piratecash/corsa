package overlaysim

// m3a_reference_test.go is the acceptance of the M3-a scenario: the four
// populations whose answers are known in advance (no Q, only Q, mixed, too few
// suitable guards), plus the three properties that make a skew measurement
// trustworthy — the classification is never bypassed, the draw repeats, and the
// denominators are the populations they claim to be.

import (
	"strings"
	"testing"
)

// m3aShape is small enough for the short tests and still large enough for the
// role split to be a population rather than an anecdote.
func m3aShape() shape {
	return shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
}

func m3aGuards() guardModel {
	return guardModel{SetSize: 3, ConfirmedPrefix: 1, Requests: 20, TargetSeed: 7}
}

// TestM3aDrawHitsTheRequestedShareWithoutAssigningRoles is requirement 1: the
// composition comes from SELECTION, and every identifier in the population is
// one the generator actually produced, classified by the one classifier.
func TestM3aDrawHitsTheRequestedShareWithoutAssigningRoles(t *testing.T) {
	sh := m3aShape()
	for _, share := range []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1} {
		people, err := drawSkewedPopulation(sh, 3, 55, share)
		if err != nil {
			t.Fatalf("share %.2f: %v", share, err)
		}

		if people.Nodes != sh.nodes {
			t.Errorf("share %.2f: %d identifiers, want %d", share, people.Nodes, sh.nodes)
		}
		if want := int(float64(sh.nodes)*share + 0.5); people.Structural != want {
			t.Errorf("share %.2f: %d Q nodes, want %d", share, people.Structural, want)
		}

		// Every identifier must be classified, not labelled: recount with the
		// classifier and compare.
		counted := 0
		for _, id := range people.IDs {
			if roleOf(id) == roleStructural {
				counted++
			}
		}
		if counted != people.Structural {
			t.Errorf("share %.2f: the population claims %d Q, the classifier finds %d",
				share, people.Structural, counted)
		}

		// And every identifier must come from the model's own generator: replay
		// the draw and check that the population is a SUBSEQUENCE of it.
		produced := make(map[nodeID]bool, people.Drawn)
		for index := range people.Drawn {
			produced[makeNodeID(people.Seed, index)] = true
		}
		for i, id := range people.IDs {
			if !produced[id] {
				t.Fatalf("share %.2f: identifier %d was not produced by makeNodeID(%d, 0…%d) — "+
					"the population was fabricated, not selected",
					share, i, people.Seed, people.Drawn)
			}
		}
	}

	if _, err := drawSkewedPopulation(sh, 1, 55, 1.5); err == nil {
		t.Error("a Q share of 1.5 was accepted")
	}
	if _, err := drawSkewedPopulation(sh, 1, 55, -0.1); err == nil {
		t.Error("a negative Q share was accepted")
	}
}

// TestM3aReferencePopulations walks the four fixtures of requirement 5.
func TestM3aReferencePopulations(t *testing.T) {
	sh := m3aShape()
	model := m3aGuards()

	t.Run("no Q at all: an empty structural plane is NO DATA, not a pass", func(t *testing.T) {
		run, err := runSkewPoint(skewSetup{Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 0, Quota: 1, Policy: policyBaseline, Guards: model, Workload: skewUniformTargets, Requesters: 20, RequesterSeed: 77})
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		if run.Population.Structural != 0 {
			t.Fatalf("population holds %d Q nodes at share 0", run.Population.Structural)
		}
		if run.Connectivity.Nodes != 0 {
			t.Errorf("the Q-subgraph covers %d nodes at share 0", run.Connectivity.Nodes)
		}
		// ⚠️ The trap this fixture exists for: an empty subgraph trivially has
		// no disconnected pair, and a criterion reading "components ≤ 1" would
		// call the worst possible population a success.
		if got := describeConnectivity(run.Connectivity); got != "no data" {
			t.Errorf("empty Q-subgraph rendered as %q, want %q", got, "no data")
		}
		if run.Connectivity.Components > 1 {
			t.Errorf("an empty subgraph reported %d components", run.Connectivity.Components)
		}
		if got := run.Neighbourless.Structural.Share(); got != "no data" {
			t.Errorf("M5 over an empty Q population = %q, want %q", got, "no data")
		}
		// Every node is ¬Q, so every target lacks a Q neighbour: unreachable
		// anonymously, and M5 must say so rather than shrug.
		if got, want := run.Neighbourless.All.Share(), "100.0%"; got != want {
			t.Errorf("M5 over all targets = %q, want %q", got, want)
		}
		// First hops, on the other hand, are plentiful.
		if run.RefusalsSampled.Refused() != 0 {
			t.Errorf("first-hop refusals with a network of nothing but ¬Q: %s", run.RefusalsSampled)
		}
	})

	t.Run("only Q: no first hops exist at all", func(t *testing.T) {
		run, err := runSkewPoint(skewSetup{Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 1, Quota: 1, Policy: policyBaseline, Guards: model, Workload: skewUniformTargets, Requesters: 20, RequesterSeed: 77})
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		if run.Population.Structural != sh.nodes {
			t.Fatalf("population holds %d Q nodes at share 1, want %d",
				run.Population.Structural, sh.nodes)
		}
		if run.Connectivity.Nodes != sh.nodes {
			t.Errorf("the Q-subgraph covers %d nodes, want %d", run.Connectivity.Nodes, sh.nodes)
		}
		if got := run.Neighbourless.NonStructural.Share(); got != "no data" {
			t.Errorf("M5 over an empty ¬Q population = %q, want %q", got, "no data")
		}
		// Guard sets are drawn from ¬Q neighbours, of which there are none.
		if run.RequestersShortOfGuards != run.RequestersMeasured {
			t.Errorf("%d of %d requesters could not fill a guard set, want all of them",
				run.RequestersShortOfGuards, run.RequestersMeasured)
		}
		if run.RefusalsSampled.Refused() != run.RefusalsSampled.Requests {
			t.Errorf("refusals %d of %d requests, want all", run.RefusalsSampled.Refused(),
				run.RefusalsSampled.Requests)
		}
		// ⚠️ And they are refusals of an EMPTY set, not refusals caused by
		// excluding the target: the skew removed first hops from the network,
		// which is a different finding from the rule biting.
		if run.RefusalsSampled.RefusedByTargetExclusion != 0 {
			t.Errorf("%d refusals blamed on the target exclusion, want 0 — there was nobody "+
				"suitable to exclude", run.RefusalsSampled.RefusedByTargetExclusion)
		}
	})

	t.Run("mixed: all three measurements produce numbers", func(t *testing.T) {
		run, err := runSkewPoint(skewSetup{Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 0.5, Quota: 1, Policy: policyBaseline, Guards: model, Workload: skewUniformTargets, Requesters: 20, RequesterSeed: 77})
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		if run.Connectivity.Nodes != run.Population.Structural {
			t.Errorf("the Q-subgraph covers %d nodes, the population has %d Q",
				run.Connectivity.Nodes, run.Population.Structural)
		}
		if describeConnectivity(run.Connectivity) == "no data" {
			t.Error("a mixed population produced no connectivity data")
		}
		for label, share := range map[string]string{
			"all": run.Neighbourless.All.Share(),
			"Q":   run.Neighbourless.Structural.Share(),
			"¬Q":  run.Neighbourless.NonStructural.Share(),
		} {
			if share == "no data" {
				t.Errorf("M5 %s slice is empty in a mixed population", label)
			}
		}
		if run.RefusalsSampled.Requests != run.RequestersMeasured*model.Requests {
			t.Errorf("M4 denominator %d, want %d requesters × %d requests",
				run.RefusalsSampled.Requests, run.RequestersMeasured, model.Requests)
		}
	})

	t.Run("too few suitable guards: an undersized set is measured, not hidden", func(t *testing.T) {
		// The same population, with a guard model asking for more members than a
		// node of degree 8 can supply from its ¬Q neighbours.
		greedy := guardModel{SetSize: 9, ConfirmedPrefix: 1, Requests: 20, TargetSeed: 7}
		run, err := runSkewPoint(skewSetup{Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 0.5, Quota: 1, Policy: policyBaseline, Guards: greedy, Workload: skewUniformTargets, Requesters: 20, RequesterSeed: 77})
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		if run.RequestersShortOfGuards == 0 {
			t.Fatalf("no requester was short of guards with a set size of %d and degree %d",
				greedy.SetSize, sh.degree)
		}
		if run.RequestersShortOfGuards > run.RequestersMeasured {
			t.Errorf("%d requesters short of %d measured", run.RequestersShortOfGuards,
				run.RequestersMeasured)
		}
		if !strings.Contains(run.String(), "short of the requested set size") {
			t.Errorf("the run does not report the undersized sets:\n%s", run)
		}
	})
}

// TestM3aRunsAreReproducible is requirement 6: the same parameters give the same
// population and the same report, byte for byte.
func TestM3aRunsAreReproducible(t *testing.T) {
	sh := m3aShape()
	model := m3aGuards()

	first, err := runSkewPoint(skewSetup{Shape: sh, Seed: 4, ShuffleSeed: 55, RequestedShare: 0.3, Quota: 1, Policy: policyInitiatedLimit, Guards: model, Workload: skewUniformTargets, Requesters: 15, RequesterSeed: 77})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := runSkewPoint(skewSetup{Shape: sh, Seed: 4, ShuffleSeed: 55, RequestedShare: 0.3, Quota: 1, Policy: policyInitiatedLimit, Guards: model, Workload: skewUniformTargets, Requesters: 15, RequesterSeed: 77})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if first.String() != second.String() {
		t.Errorf("two runs of the same parameters differ:\n%s\n\n%s", first, second)
	}
	for i, id := range first.Population.IDs {
		if id != second.Population.IDs[i] {
			t.Fatalf("identifier %d differs between two draws of the same seed", i)
		}
	}

	// A different seed must actually change the population, or "reproducible"
	// would be indistinguishable from "constant".
	other, err := runSkewPoint(skewSetup{Shape: sh, Seed: 5, ShuffleSeed: 55, RequestedShare: 0.3, Quota: 1, Policy: policyInitiatedLimit, Guards: model, Workload: skewUniformTargets, Requesters: 15, RequesterSeed: 77})
	if err != nil {
		t.Fatalf("third run: %v", err)
	}
	if other.String() == first.String() {
		t.Error("two different seeds produced the same run")
	}
}

// TestM3aReportRecordsItsParameters is requirement 2: a skew number is unusable
// without the population, the seed, both shares, the policy, the quota, B, and
// the guard assumptions behind its M4 part.
func TestM3aReportRecordsItsParameters(t *testing.T) {
	sh := m3aShape()
	run, err := runSkewPoint(skewSetup{
		Shape: sh, Seed: 2, ShuffleSeed: 55, RequestedShare: 0.25, Quota: 1,
		Policy: policyInitiatedLimit, Guards: m3aGuards(), Workload: skewUniformTargets,
		Requesters: 10, RequesterSeed: 77,
	})
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	rendered := run.String()
	for _, want := range []string{
		"N=1000", "d=8", "B=16",
		"seed 2", "shuffle seed 55", "sampled with seed 77",
		"Q requested 0.250",
		"actual 0.250",
		"identifiers drawn",
		"policy initiated-limit",
		"quota 1",
		"Q-subgraph:",
		"M5 all:", "M4 confirmed only:", "M4 whole set:",
		"ASSUMED, NOT SIMULATED",
		"LOWER BOUND",
		"PROPOSAL, not adopted",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the run report does not carry %q:\n%s", want, rendered)
		}
	}
}

// TestM3aDenominatorsAreThePopulationsTheyClaim guards the three counts that a
// skew makes easy to get wrong: M5 asks every node, the Q-subgraph covers
// exactly the Q nodes, and M4's denominator is requesters × requests.
func TestM3aDenominatorsAreThePopulationsTheyClaim(t *testing.T) {
	sh := m3aShape()
	model := m3aGuards()

	for _, share := range []float64{0.1, 0.5, 0.9} {
		run, err := runSkewPoint(skewSetup{Shape: sh, Seed: 6, ShuffleSeed: 55, RequestedShare: share, Quota: 1, Policy: policyBaseline, Guards: model, Workload: skewUniformTargets, Requesters: 12, RequesterSeed: 77})
		if err != nil {
			t.Fatalf("share %.2f: %v", share, err)
		}

		if run.Neighbourless.All.Targets != sh.nodes {
			t.Errorf("share %.2f: M5 denominator %d, want every node (%d)",
				share, run.Neighbourless.All.Targets, sh.nodes)
		}
		if run.Neighbourless.Structural.Targets != run.Population.Structural {
			t.Errorf("share %.2f: M5 Q slice holds %d targets, the population has %d Q",
				share, run.Neighbourless.Structural.Targets, run.Population.Structural)
		}
		if run.Connectivity.Nodes != run.Population.Structural {
			t.Errorf("share %.2f: the Q-subgraph covers %d nodes, the population has %d Q",
				share, run.Connectivity.Nodes, run.Population.Structural)
		}
		if run.RefusalsSampled.Requests != run.RequestersMeasured*model.Requests {
			t.Errorf("share %.2f: M4 denominator %d, want %d × %d",
				share, run.RefusalsSampled.Requests, run.RequestersMeasured, model.Requests)
		}
		if sum := run.RefusalsSampled.Served + run.RefusalsSampled.Refused(); sum != run.RefusalsSampled.Requests {
			t.Errorf("share %.2f: served + refused = %d, requests = %d",
				share, sum, run.RefusalsSampled.Requests)
		}
	}
}

// TestM3aSweepIsAProposalNotAParameterSet: the range, the step and the guard
// model are the things to AGREE on. The test checks the proposal says so and
// that it is internally consistent — it deliberately does not run it.
func TestM3aSweepIsAProposalNotAParameterSet(t *testing.T) {
	proposal := proposedSkewSweep()

	rendered := proposal.String()
	for _, want := range []string{
		"PROPOSED skew sweep",
		"AWAITING AGREEMENT",
		"not an adopted parameter set",
		// ⚠️ The open list is checked ITEM BY ITEM, not by the phrase "open
		// decisions". A heading can survive an edit that empties the list under
		// it, and the whole reason the list exists is that these particular
		// questions must reach the owner before a run: index §0.2 item 2 is not
		// agreed.
		"OPEN, and to be settled BEFORE the runs",
		"which population is NORMATIVE",
		"LOWER BOUND",
		"guardPrimaryCount = 3",
		"None of the three implemented is measured user load",
		"RESULT of this experiment, not an input to it",
		// The candidate the acceptance numbers belong to, and its base, both
		// named — a sweep that does not say which rule it ran describes no graph.
		"C1/v1 (CANDIDATE",
		"initiated-limit (comparison base",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the proposal does not say %q:\n%s", want, rendered)
		}
	}

	points := proposal.Points()
	if len(points) != 19 {
		t.Errorf("the proposal visits %d shares, want 19 (0.05…0.95 step 0.05)", len(points))
	}
	if points[0] != proposal.FromShare {
		t.Errorf("the sweep starts at %.2f, the proposal says %.2f", points[0], proposal.FromShare)
	}
	if last := points[len(points)-1]; last > proposal.ToShare+1e-9 {
		t.Errorf("the sweep ends at %.2f, past the proposed %.2f", last, proposal.ToShare)
	}
	for _, share := range points {
		if share <= 0 || share >= 1 {
			t.Errorf("proposed share %.2f is degenerate — 0 and 1 are the fixtures above, not "+
				"points of a sensitivity sweep", share)
		}
	}
}

// TestM3aOrderDoesNotCarryTheSkew is the fix for a real defect of the first
// version: acceptance order correlates with role (mixed head, pure-majority
// tail), and the model builds in index order, so the skew arrived ADDED TO a
// change of processing order. The shuffle separates them, and this test measures
// the correlation rather than trusting it.
func TestM3aOrderDoesNotCarryTheSkew(t *testing.T) {
	sh := m3aShape()
	const share = 0.1

	people, err := drawSkewedPopulation(sh, 3, 55, share)
	if err != nil {
		t.Fatalf("drawing: %v", err)
	}

	// Where do the Q nodes sit? Under acceptance order they would all be in the
	// head: at a 10 % share the minority fills up after roughly 200 draws and
	// everything after that is ¬Q. Split the population in half and compare.
	head, tail := 0, 0
	for i, id := range people.IDs {
		if roleOf(id) != roleStructural {
			continue
		}
		if i < len(people.IDs)/2 {
			head++
			continue
		}
		tail++
	}
	if tail == 0 {
		t.Fatalf("all %d Q nodes are in the first half: the order still carries the skew", head)
	}
	// Balanced within a wide margin — this is a randomised order, not a
	// guaranteed split.
	if head > 3*tail || tail > 3*head {
		t.Errorf("Q nodes split %d in the first half against %d in the second: the order still "+
			"correlates with the role", head, tail)
	}

	// The shuffle is reproducible, and a different shuffle seed gives a
	// different order of the SAME multiset of identifiers.
	same, err := drawSkewedPopulation(sh, 3, 55, share)
	if err != nil {
		t.Fatalf("redrawing: %v", err)
	}
	other, err := drawSkewedPopulation(sh, 3, 56, share)
	if err != nil {
		t.Fatalf("drawing with another shuffle seed: %v", err)
	}

	identical, reordered := true, false
	for i := range people.IDs {
		if people.IDs[i] != same.IDs[i] {
			identical = false
		}
		if people.IDs[i] != other.IDs[i] {
			reordered = true
		}
	}
	if !identical {
		t.Error("the same shuffle seed produced a different order")
	}
	if !reordered {
		t.Error("a different shuffle seed produced the same order")
	}
	if other.Structural != people.Structural {
		t.Errorf("the shuffle changed the composition: %d Q against %d",
			other.Structural, people.Structural)
	}
}

// TestM3aRequestersAreSampledNotPrefixed: which nodes are measured must not be
// decided by construction order either — a prefix of the index order is a
// prefix of "who got to spend budget first".
func TestM3aRequestersAreSampledNotPrefixed(t *testing.T) {
	const nodes, count = 1_000, 20

	sample := sampleRequesters(nodes, count, 77)
	if len(sample) != count {
		t.Fatalf("sampled %d requesters, want %d", len(sample), count)
	}

	seen := make(map[int32]bool, count)
	prefix := 0
	for _, node := range sample {
		if seen[node] {
			t.Fatalf("node %d sampled twice", node)
		}
		seen[node] = true
		if int(node) < count {
			prefix++
		}
	}
	// A prefix selection would put all of them below `count`; a sample over a
	// thousand nodes should put almost none there.
	if prefix > count/2 {
		t.Errorf("%d of %d sampled requesters came from the first %d indices — this looks like "+
			"a prefix, not a sample", prefix, count, count)
	}

	repeat := sampleRequesters(nodes, count, 77)
	for i := range sample {
		if sample[i] != repeat[i] {
			t.Fatalf("the same seed sampled differently at position %d", i)
		}
	}
	different := sampleRequesters(nodes, count, 78)
	same := true
	for i := range sample {
		if sample[i] != different[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("two seeds sampled the same requesters")
	}

	// Asking for more than exists gives everyone, once.
	all := sampleRequesters(5, 99, 1)
	if len(all) != 5 {
		t.Errorf("sampling 99 of 5 nodes returned %d", len(all))
	}
}

// TestM3aKeepsBothPopulationsOfM4 is the second half of the fix: the confirmed
// reading must survive into the result. Under a load that aims at the confirmed
// guard, an UNCONFIRMED spare rescues the whole-set reading and not the
// confirmed-only one — which is precisely the price of the open sub-question,
// and it would be invisible if only one aggregate were kept.
func TestM3aKeepsBothPopulationsOfM4(t *testing.T) {
	sh := m3aShape()
	// Two members, one confirmed: the spare exists but has never carried a frame.
	model := guardModel{SetSize: 2, ConfirmedPrefix: 1, Requests: 20, TargetSeed: 7}

	run, err := runSkewPoint(skewSetup{
		Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 0.5, Quota: 1,
		Policy: policyBaseline, Guards: model, Workload: skewTargetsTheConfirmedGuard,
		Requesters: 20, RequesterSeed: 77,
	})
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	if run.RefusalsConfirmed.Requests != run.RefusalsSampled.Requests {
		t.Fatalf("the two populations were asked different questions: %d requests against %d",
			run.RefusalsConfirmed.Requests, run.RefusalsSampled.Requests)
	}
	if run.RefusalsConfirmed.Requests == 0 {
		t.Fatal("no requests were measured")
	}

	// Every request targets the confirmed member, so the confirmed-only reading
	// has nobody left after the exclusion.
	if run.RefusalsConfirmed.Refused() != run.RefusalsConfirmed.Requests {
		t.Errorf("confirmed-only refused %d of %d, want all — every request aimed at the one "+
			"confirmed member", run.RefusalsConfirmed.Refused(), run.RefusalsConfirmed.Requests)
	}
	if run.RefusalsSampled.Served == 0 {
		t.Fatal("the unconfirmed spare rescued nothing — the fixture no longer shows the " +
			"difference between the two readings")
	}
	if run.RefusalsSampled.Refused() >= run.RefusalsConfirmed.Refused() {
		t.Errorf("whole set refused %d, confirmed only %d — the spare must help the whole-set "+
			"reading and only it",
			run.RefusalsSampled.Refused(), run.RefusalsConfirmed.Refused())
	}

	// And both are printed: a result that keeps a number it does not show is the
	// same defect in a different place.
	rendered := run.String()
	for _, want := range []string{"M4 confirmed only:", "M4 whole set:", "aims at the requester's confirmed guard"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the run does not report %q:\n%s", want, rendered)
		}
	}

	// Under the neutral load the two readings need not differ — the point is
	// that the difference is measured, not that it is always there.
	uniform, err := runSkewPoint(skewSetup{
		Shape: sh, Seed: 1, ShuffleSeed: 55, RequestedShare: 0.5, Quota: 1,
		Policy: policyBaseline, Guards: model, Workload: skewUniformTargets,
		Requesters: 20, RequesterSeed: 77,
	})
	if err != nil {
		t.Fatalf("running the uniform load: %v", err)
	}
	if uniform.RefusalsConfirmed.Requests != uniform.RefusalsSampled.Requests {
		t.Errorf("uniform load: %d requests against %d",
			uniform.RefusalsConfirmed.Requests, uniform.RefusalsSampled.Requests)
	}
}

// TestM3aClassificationSurvivesTheWholePath checks that no stage between drawing
// and building reassigns a role — the property requirement 1 is protecting.
func TestM3aClassificationSurvivesTheWholePath(t *testing.T) {
	sh := m3aShape()
	people, err := drawSkewedPopulation(sh, 8, 55, 0.4)
	if err != nil {
		t.Fatalf("drawing: %v", err)
	}
	g := buildGraphOnIDs(people.IDs, sh, 1, policyBaseline, nil, nil)

	structural := 0
	for i := range g.roles {
		if g.roles[i] != roleOf(g.ids[i]) {
			t.Fatalf("node %d carries role %d, the classifier says %d",
				i, g.roles[i], roleOf(g.ids[i]))
		}
		if g.ids[i] != people.IDs[i] {
			t.Fatalf("node %d was built on a different identifier than it was drawn with", i)
		}
		if g.roles[i] == roleStructural {
			structural++
		}
	}
	if structural != people.Structural {
		t.Errorf("the built graph holds %d Q nodes, the population %d", structural, people.Structural)
	}
}

// TestM3aRunCarriesBothReadingsTheirGapAndTheSetsBehindThem is the M3-a half of
// the presentation rule.
//
// One aggregate is not a result: §4.3.4″.3 leaves the normative population open,
// so a point of the sweep has to hand over both readings, their difference over
// the SAME requests, and enough about the sets to say where the difference came
// from. The composition digest is the last of those — an aggregate cannot carry
// two hundred listings, but it can prove which two hundred sets it used.
func TestM3aRunCarriesBothReadingsTheirGapAndTheSetsBehindThem(t *testing.T) {
	t.Parallel()

	setup := skewSetup{
		Shape:          shape{name: "600×6", nodes: 600, degree: 6, budget: 12},
		Seed:           21,
		ShuffleSeed:    22,
		RequestedShare: 0.5,
		Quota:          1,
		Policy:         policyCandidateC1,
		Guards:         guardModel{SetSize: 3, ConfirmedPrefix: 1, Requests: 40, TargetSeed: 7},
		// ⚠️ The load that FORCES the two readings apart. Under uniform targets
		// they may coincide, and a test that only ever saw them coincide would
		// pass against a measurer that kept one aggregate.
		Workload:      skewTargetsTheConfirmedGuard,
		Requesters:    50,
		RequesterSeed: 23,
	}

	run, err := runSkewPoint(setup)
	if err != nil {
		t.Fatalf("running the point: %v", err)
	}

	if run.RefusalsConfirmed.Requests != run.RefusalsSampled.Requests {
		t.Fatalf("the two readings answered %d and %d requests",
			run.RefusalsConfirmed.Requests, run.RefusalsSampled.Requests)
	}
	if run.RefusalsConfirmed.Requests == 0 {
		t.Fatal("no request was measured, so nothing here is exercised")
	}
	if run.MembersTotal == 0 || run.ConfirmedTotal == 0 {
		t.Fatalf("%d guard members of which %d confirmed — the composition is not being recorded",
			run.MembersTotal, run.ConfirmedTotal)
	}
	if run.ConfirmedTotal >= run.MembersTotal {
		t.Fatalf("%d of %d members are confirmed — then the two readings cannot differ and this "+
			"fixture proves nothing", run.ConfirmedTotal, run.MembersTotal)
	}

	rendered := run.String()
	for _, want := range []string{
		"M4 confirmed only:",
		"M4 whole set:",
		"M4 gap:",
		"pp (confirmed-only minus whole-set, over the same",
		"DECLARED confirmed",
		"DECLARED by the guard model, not observed in any network",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the point does not carry %q:\n%s", want, rendered)
		}
	}

	t.Run("the digest identifies the sets, not the run", func(t *testing.T) {
		again, err := runSkewPoint(setup)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if again.SetsDigest != run.SetsDigest {
			t.Error("one setup produced two different set digests")
		}

		// A different requester sample is a different set of sets, and the
		// digest has to show it — otherwise it would identify the parameters
		// rather than the composition, which is what a name already does.
		other := setup
		other.RequesterSeed = 99
		different, err := runSkewPoint(other)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if different.SetsDigest == run.SetsDigest {
			t.Error("two different requester samples produced the same set digest")
		}
	})

	t.Logf("%s", run)
}
