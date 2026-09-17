package overlaysim

// m6_reference_test.go is the acceptance of the M6 counters: the seven fixtures
// of the contract (docs/refactoring/dht/21-m6-bucket-discovery-measurement.md
// §6) plus the properties that keep the three axes apart and keep the control
// labelled as a control.
//
// ⚠️ There is no discovery policy here either. Every fixture hands the owner an
// explicit candidate queue, so what is being tested is the accounting.

import (
	"fmt"
	"strings"
	"testing"
)

func m6Shape() shape {
	return shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
}

// m6Fixture builds the graph the fixtures share and a setup over it.
func m6Fixture(t *testing.T, events []m6Event) (*graph, m6Setup) {
	t.Helper()
	sh := m6Shape()
	return buildGraph(sh, 1, 1, policyBaseline), m6Setup{
		Shape:       sh,
		Seed:        1,
		Policy:      policyBaseline,
		Quota:       1,
		Capacity:    1,
		NearFrom:    4,
		ProbeBudget: 0, // 0 = no cap; the budget fixture sets its own
		Membership:  "whole network",
		Events:      events,
	}
}

// offers is a shorthand for n probe events by one owner.
func offers(owner int32, count int) []m6Event {
	events := make([]m6Event, 0, count)
	for range count {
		events = append(events, m6Event{Kind: m6Offer, Node: owner})
	}
	return events
}

// candidatesAtLevels finds, for the owner, one node per bucket level, so a
// fixture can say "a reachable candidate" without knowing the identifiers.
func candidatesAtLevels(g *graph, owner int32, levels, howMany int) []int32 {
	found := make([]int32, 0, howMany)
	seen := map[int]bool{}
	for node := range g.ids {
		candidate := int32(node)
		if candidate == owner || len(g.adjacency[candidate]) >= 16 {
			continue
		}
		level := levelOf(g.ids[owner], g.ids[candidate], levels)
		if level < 0 || seen[level] {
			continue
		}
		seen[level] = true
		found = append(found, candidate)
		if len(found) == howMany {
			break
		}
	}
	return found
}

// candidatesAtOneLevel finds several nodes that belong in the SAME bucket of the
// owner — what a genuine replacement needs: the level that lost a slot is the
// only level that can recover it.
func candidatesAtOneLevel(g *graph, owner int32, levels, howMany int) (int, []int32) {
	byLevel := map[int][]int32{}
	for node := range g.ids {
		candidate := int32(node)
		if candidate == owner || len(g.adjacency[candidate]) >= 16 {
			continue
		}
		level := levelOf(g.ids[owner], g.ids[candidate], levels)
		if level < 0 {
			continue
		}
		byLevel[level] = append(byLevel[level], candidate)
		if len(byLevel[level]) == howMany {
			return level, byLevel[level]
		}
	}
	return -1, nil
}

// TestM6ReferenceFixtures walks the seven cases the contract names.
func TestM6ReferenceFixtures(t *testing.T) {
	const owner = int32(0)

	t.Run("no candidates: not a probe, and not a filled slot", func(t *testing.T) {
		g, setup := m6Fixture(t, offers(owner, 3))
		source := newScriptedCandidates("empty", map[int32][]int32{})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if got := report.Probes.Probes(); got != 0 {
			t.Errorf("%d probes were counted with nobody to probe", got)
		}
		if got := report.Probes.Outcomes[m6NoCandidate]; got != 3 {
			t.Errorf("%d empty offers recorded, want 3", got)
		}
		// ⚠️ Cost with nothing filled is "no data": probes were not spent, and
		// zero would read as "free".
		if got := report.Probes.PerFilledSlot(); got != "no data" {
			t.Errorf("cost = %q, want %q", got, "no data")
		}
		if got := report.Final.Share(); !strings.HasPrefix(got, "0/8") {
			t.Errorf("coverage = %q, want an empty table of 8 slots", got)
		}
	})

	t.Run("a reachable candidate fills its own level", func(t *testing.T) {
		g, setup := m6Fixture(t, offers(owner, 1))
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 1)
		if len(candidates) == 0 {
			t.Fatal("the fixture graph offers no reachable candidate")
		}
		source := newScriptedCandidates("one candidate", map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if report.Probes.Filled() != 1 || report.Probes.Probes() != 1 {
			t.Fatalf("one reachable candidate gave %d probes and %d filled slots",
				report.Probes.Probes(), report.Probes.Filled())
		}
		if !strings.Contains(report.Probes.PerFilledSlot(), "1.00 probes/slot") {
			t.Errorf("cost = %q, want one probe per slot", report.Probes.PerFilledSlot())
		}
		level := levelOf(g.ids[owner], g.ids[candidates[0]], setup.Shape.degree)
		if report.Final.Held[level] != 1 {
			t.Errorf("level %d holds %d, want the candidate that belongs there",
				level, report.Final.Held[level])
		}
		if report.Final.Empty() != setup.Shape.degree-1 {
			t.Errorf("%d empty levels, want %d", report.Final.Empty(), setup.Shape.degree-1)
		}
	})

	t.Run("a refused probe costs a probe and fills nothing", func(t *testing.T) {
		g, setup := m6Fixture(t, append(offers(owner, 1), m6Event{Kind: m6Offer, Node: owner}))
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 2)
		if len(candidates) < 2 {
			t.Fatal("the fixture graph offers too few candidates")
		}
		// The first candidate is offline before it is ever offered, the second
		// is reachable — so one probe is spent on a refusal and one fills.
		events := []m6Event{
			{Kind: m6Depart, Node: candidates[0]},
			{Kind: m6Offer, Node: owner},
			{Kind: m6Offer, Node: owner},
		}
		setup.Events = events
		source := newScriptedCandidates("one dead, one live", map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if got := report.Probes.Outcomes[m6CandidateUnreachable]; got != 1 {
			t.Errorf("%d unreachable candidates recorded, want 1", got)
		}
		if report.Probes.Probes() != 2 {
			t.Errorf("%d probes, want 2 — a failed probe is still a probe",
				report.Probes.Probes())
		}
		if report.Probes.Filled() != 1 {
			t.Errorf("%d slots filled, want 1", report.Probes.Filled())
		}
		if !strings.Contains(report.Probes.PerFilledSlot(), "2.00 probes/slot") {
			t.Errorf("cost = %q, want two probes per filled slot",
				report.Probes.PerFilledSlot())
		}
	})

	t.Run("the probe budget stops the scenario without spending a probe", func(t *testing.T) {
		g, setup := m6Fixture(t, offers(owner, 5))
		setup.ProbeBudget = 2
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 5)
		source := newScriptedCandidates("five candidates", map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if report.Probes.Probes() != 2 {
			t.Errorf("%d probes against a budget of 2", report.Probes.Probes())
		}
		if got := report.Probes.Outcomes[m6ProbeBudgetSpent]; got != 3 {
			t.Errorf("%d attempts refused by the budget, want 3", got)
		}
		// ⚠️ A budget refusal is NOT a probe and must not inflate the cost.
		if !strings.Contains(report.Probes.PerFilledSlot(), "(2 probes") {
			t.Errorf("cost = %q, want the two probes only", report.Probes.PerFilledSlot())
		}
	})

	t.Run("a neighbour leaves: the slot empties and the loss is counted", func(t *testing.T) {
		g, setup := m6Fixture(t, nil)
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 1)
		setup.Events = []m6Event{
			{Kind: m6Offer, Node: owner},
			{Kind: m6Depart, Node: candidates[0]},
		}
		source := newScriptedCandidates("one candidate that leaves", map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if report.LostSlots() != 1 {
			t.Errorf("%d slots lost, want 1", report.LostSlots())
		}
		if report.Before.filled() != 1 {
			t.Errorf("coverage before churn = %d filled, want 1", report.Before.filled())
		}
		if report.AfterChurn.filled() != 0 {
			t.Errorf("coverage after churn = %d filled, want 0", report.AfterChurn.filled())
		}
		// ⚠️ Nothing was attempted afterwards, so recovery is INCOMPLETE and
		// says so — it is a result, not a case to skip.
		if report.RecoveryComplete() {
			t.Error("recovery reported complete with no probe after the departure")
		}
		if !strings.Contains(report.recoveryLine(), "STILL SHORT at L") {
			t.Errorf("recovery line = %q, want the missing slot named", report.recoveryLine())
		}
	})

	t.Run("a replacement arrives AT THE LOST LEVEL: recovery completes", func(t *testing.T) {
		g, setup := m6Fixture(t, nil)
		// Two candidates in ONE bucket — the lost level is the only level that
		// can recover it — plus a survivor somewhere else, so the fixture also
		// tells recovery from a rebuild.
		lostLevel, pair := candidatesAtOneLevel(g, owner, setup.Shape.degree, 2)
		if lostLevel < 0 {
			t.Fatal("the fixture graph has no bucket with two candidates")
		}
		survivor := int32(-1)
		for _, candidate := range candidatesAtLevels(g, owner, setup.Shape.degree, 8) {
			if levelOf(g.ids[owner], g.ids[candidate], setup.Shape.degree) != lostLevel {
				survivor = candidate
				break
			}
		}
		if survivor < 0 {
			t.Fatal("the fixture graph offers no candidate outside the lost level")
		}

		setup.Events = []m6Event{
			{Kind: m6Offer, Node: owner}, // fills the level that will be lost
			{Kind: m6Offer, Node: owner}, // fills another level — the survivor
			{Kind: m6Depart, Node: pair[0]},
			{Kind: m6Offer, Node: owner}, // the replacement, at the lost level
		}
		source := newScriptedCandidates("one leaves, a replacement in the same bucket",
			map[int32][]int32{owner: {pair[0], survivor, pair[1]}})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if report.Before.filled() != 2 {
			t.Fatalf("coverage before churn = %d filled, want 2", report.Before.filled())
		}
		if report.LostByLevel[lostLevel] != 1 || report.RefilledByLevel[lostLevel] != 1 {
			t.Fatalf("level %d lost %d and refilled %d, want 1 and 1",
				lostLevel, report.LostByLevel[lostLevel], report.RefilledByLevel[lostLevel])
		}
		if report.FilledElsewhere != 0 {
			t.Errorf("%d fills credited elsewhere, want none — the replacement belongs to the "+
				"lost level", report.FilledElsewhere)
		}
		if !report.RecoveryComplete() {
			t.Errorf("recovery not complete: %s", report.recoveryLine())
		}
		// ⚠️ THE SURVIVOR IS THE POINT. One of the two slots was never lost, so
		// it must still be there right after the departure — a node that starts
		// over is not a node that recovered.
		if report.AfterChurn.filled() != 1 {
			t.Errorf("coverage after churn = %d filled, want 1 — the slot that was not lost must "+
				"survive, otherwise this is a rebuild and not a recovery",
				report.AfterChurn.filled())
		}
		if report.Final.filled() != 2 {
			t.Errorf("final coverage %d filled, want 2 (the survivor plus the replacement)",
				report.Final.filled())
		}
		// The recovery cost is a SEPARATE ledger: the probes spent before the
		// churn are not part of it.
		if got := report.ProbesAfterChurn.Probes(); got != 1 {
			t.Errorf("recovery spent %d probes, want 1", got)
		}
		if got := report.Probes.Probes(); got != 3 {
			t.Errorf("the whole scenario spent %d probes, want 3", got)
		}
	})

	t.Run("a fill in ANOTHER bucket is not recovery", func(t *testing.T) {
		// ⚠️ The negative case of the one above, and the reason the accounting
		// is per level at all: totals cannot tell these two apart. Coverage ends
		// at exactly the number it started with, and the node is still unable to
		// route through the level it lost.
		g, setup := m6Fixture(t, nil)
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 2)
		if len(candidates) < 2 {
			t.Fatal("the fixture graph offers too few candidates")
		}
		lostLevel := levelOf(g.ids[owner], g.ids[candidates[0]], setup.Shape.degree)
		otherLevel := levelOf(g.ids[owner], g.ids[candidates[1]], setup.Shape.degree)
		if lostLevel == otherLevel {
			t.Fatal("the two candidates share a bucket; the fixture needs different ones")
		}

		setup.Events = []m6Event{
			{Kind: m6Offer, Node: owner},
			{Kind: m6Depart, Node: candidates[0]},
			{Kind: m6Offer, Node: owner}, // fills a level that lost nothing
		}
		source := newScriptedCandidates("replacement in the wrong bucket",
			map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}

		if report.Probes.Filled() != 2 {
			t.Fatalf("%d slots filled over the scenario, want 2", report.Probes.Filled())
		}
		if report.Final.filled() != report.Before.filled() {
			t.Fatalf("final coverage %d against %d before churn — the fixture no longer shows "+
				"that totals hide the difference", report.Final.filled(), report.Before.filled())
		}
		// Same totals, and yet nothing was recovered.
		if report.Refilled() != 0 {
			t.Errorf("%d slots credited as recovered, want 0 — the fill landed at level %d and "+
				"level %d is the one that lost a slot", report.Refilled(), otherLevel, lostLevel)
		}
		if report.FilledElsewhere != 1 {
			t.Errorf("%d fills counted as coverage-not-recovery, want 1", report.FilledElsewhere)
		}
		if report.RecoveryComplete() {
			t.Error("filling a different bucket was reported as a completed recovery")
		}
		if report.LostByLevel[lostLevel] != 1 || report.RefilledByLevel[lostLevel] != 0 {
			t.Errorf("level %d: lost %d, refilled %d, want 1 and 0",
				lostLevel, report.LostByLevel[lostLevel], report.RefilledByLevel[lostLevel])
		}
		if !strings.Contains(report.recoveryLine(), fmt.Sprintf("STILL SHORT at L%d:1", lostLevel)) {
			t.Errorf("the report does not name the level still missing: %s", report.recoveryLine())
		}
		if !strings.Contains(report.recoveryLine(), "coverage, NOT recovery") {
			t.Errorf("the report does not separate coverage from recovery: %s", report.recoveryLine())
		}
	})

	t.Run("no replacement: incomplete recovery is reported, not skipped", func(t *testing.T) {
		g, setup := m6Fixture(t, nil)
		candidates := candidatesAtLevels(g, owner, setup.Shape.degree, 1)
		setup.Events = []m6Event{
			{Kind: m6Offer, Node: owner},
			{Kind: m6Depart, Node: candidates[0]},
			{Kind: m6Offer, Node: owner},
			{Kind: m6Offer, Node: owner},
		}
		source := newScriptedCandidates("nobody to replace with", map[int32][]int32{owner: candidates})

		report, err := runBucketScenario(g, setup, owner, everyone, source)
		if err != nil {
			t.Fatalf("running: %v", err)
		}
		if report.LostSlots() != 1 || report.Refilled() != 0 {
			t.Fatalf("lost %d, refilled %d, want 1 and 0", report.LostSlots(), report.Refilled())
		}
		if report.RecoveryComplete() {
			t.Error("recovery reported complete with no replacement")
		}
		if got := report.ProbesAfterChurn.Outcomes[m6NoCandidate]; got != 2 {
			t.Errorf("%d empty offers after the churn, want 2", got)
		}
		if !strings.Contains(report.String(), "STILL SHORT at L") {
			t.Errorf("the report hides the unfinished recovery:\n%s", report)
		}
	})
}

// TestM6CoverageSeparatesNearAndFar: §3.3′ expects organic knowledge to cover far
// levels well and near ones badly, so a single average is the one number that
// must not be the answer.
func TestM6CoverageSeparatesNearAndFar(t *testing.T) {
	coverage := newBucketCoverage(8, 1, 4)
	// Far levels (0…3) full, near levels (4…7) empty — the shape §3.3′ predicts.
	for level := range 4 {
		coverage.Held[level] = 1
	}

	near, far := coverage.halves()
	if got, want := far.Share(), "4/4 = 100.0%"; got != want {
		t.Errorf("far coverage %q, want %q", got, want)
	}
	if got, want := near.Share(), "0/4 = 0.0%"; got != want {
		t.Errorf("near coverage %q, want %q", got, want)
	}
	if got, want := coverage.Share(), "4/8 = 50.0%"; got != want {
		t.Errorf("overall coverage %q, want %q", got, want)
	}
	if coverage.Empty() != 4 {
		t.Errorf("%d empty levels, want 4", coverage.Empty())
	}

	// Under-filled is its own count: a level holding one of three is neither
	// empty nor covered.
	roomy := newBucketCoverage(3, 3, 1)
	roomy.Held = []int{0, 1, 3}
	if roomy.Empty() != 1 || roomy.UnderFilled() != 1 {
		t.Errorf("empty %d, under-filled %d, want 1 and 1", roomy.Empty(), roomy.UnderFilled())
	}
	if got, want := roomy.Share(), "4/9 = 44.4%"; got != want {
		t.Errorf("coverage %q, want %q", got, want)
	}

	// A table with no slots is "no data", never a fully covered table.
	if got := newBucketCoverage(0, 1, 0).Share(); got != "no data" {
		t.Errorf("empty table = %q, want %q", got, "no data")
	}
}

// TestM6OmniscientSourceIsLabelledAControl is the one property this file cannot
// afford to lose: a source that may offer any node of the simulation is an upper
// bound, not a discovery mechanism, and the report must say so where the numbers
// are read.
func TestM6OmniscientSourceIsLabelledAControl(t *testing.T) {
	const owner = int32(0)
	g, setup := m6Fixture(t, offers(owner, 20))
	setup.Membership = "whole network"

	report, err := runBucketScenario(g, setup, owner, everyone, newOmniscientCandidates(len(g.ids)))
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	rendered := report.String()
	for _, want := range []string{
		"CONTROL",
		"CONTROL RESULT UNDER THE STATED CONSTRAINTS",
		"NOT a variant",
		// ⚠️ And the claim it must NOT make. "Upper bound" says no mechanism can
		// do better, which needs a proof the stand does not have; the control is
		// bounded by the same k, B, probe ceiling and graph as everything else.
		"not claimed to be a mathematical upper bound",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the report does not mark the omniscient source as a control (%q):\n%s",
				want, rendered)
		}
	}

	// A scripted source must NOT carry that label, or the warning would be
	// noise that means nothing.
	scripted := newScriptedCandidates("organic", map[int32][]int32{owner: {1, 2}})
	plain, err := runBucketScenario(g, setup, owner, everyone, scripted)
	if err != nil {
		t.Fatalf("running the scripted source: %v", err)
	}
	if strings.Contains(plain.String(), "CONTROL") {
		t.Errorf("a scripted source was reported as a control:\n%s", plain)
	}
}

// TestM6ComparesMembershipsUnderIdenticalConditions: the whole network and the Q
// half must differ ONLY by who is inside — same population, same events, same
// candidate stream.
func TestM6ComparesMembershipsUnderIdenticalConditions(t *testing.T) {
	const owner = int32(0)
	g := buildGraph(m6Shape(), 1, 1, policyBaseline)

	// An owner that is itself structural, so the Q half is a membership it
	// belongs to.
	structuralOwner := owner
	for node := range g.ids {
		if g.roles[node] == roleStructural {
			structuralOwner = int32(node)
			break
		}
	}

	candidates := candidatesAtLevels(g, structuralOwner, m6Shape().degree, 8)
	events := offers(structuralOwner, len(candidates))

	measure := func(membership string, inGraph func(int32) bool) m6Report {
		_, setup := m6Fixture(t, events)
		setup.Membership = membership
		source := newScriptedCandidates("identical stream", map[int32][]int32{structuralOwner: append([]int32(nil), candidates...)})
		report, err := runBucketScenario(g, setup, structuralOwner, inGraph, source)
		if err != nil {
			t.Fatalf("running %s: %v", membership, err)
		}
		return report
	}

	whole := measure("whole network", everyone)
	half := measure("Q half", func(i int32) bool { return g.roles[i] == roleStructural })

	if whole.Setup.Seed != half.Setup.Seed || whole.Setup.Quota != half.Setup.Quota {
		t.Error("the two memberships were measured under different setups")
	}
	if len(whole.Setup.Events) != len(half.Setup.Events) {
		t.Error("the two memberships saw different event sequences")
	}
	// The half can only be worse or equal: it draws from a subset of the same
	// stream, and candidates outside it are not probed at all.
	if half.Probes.Filled() > whole.Probes.Filled() {
		t.Errorf("the Q half filled %d slots against %d for the whole network — the half cannot "+
			"do better on the same stream", half.Probes.Filled(), whole.Probes.Filled())
	}

	// ⚠️ An exact identity rather than an inequality: a candidate outside the
	// membership costs NO connection, so the difference in probes must be
	// exactly the number of ¬Q candidates in the shared stream. An inequality
	// would pass a measurer that probes everybody in both runs.
	outside := 0
	for _, candidate := range candidates {
		if g.roles[candidate] != roleStructural {
			outside++
		}
	}
	if outside == 0 {
		t.Fatal("the shared stream holds no ¬Q candidate, so it cannot show the difference")
	}
	if got, want := whole.Probes.Probes()-half.Probes.Probes(), outside; got != want {
		t.Errorf("the whole network spent %d probes and the Q half %d — a difference of %d, but "+
			"%d candidates of the shared stream lie outside the half and must cost nothing",
			whole.Probes.Probes(), half.Probes.Probes(), got, want)
	}
}

// TestM6ReportRecordsEveryInput: the contract says a number here is a function of
// its inputs, so the report must carry them.
func TestM6ReportRecordsEveryInput(t *testing.T) {
	const owner = int32(0)
	g, setup := m6Fixture(t, []m6Event{
		{Kind: m6Offer, Node: owner},
		{Kind: m6Depart, Node: 7},
		{Kind: m6Arrive, Node: 7},
	})
	setup.ProbeBudget = 5
	source := newScriptedCandidates("organic", map[int32][]int32{owner: {3}})

	report, err := runBucketScenario(g, setup, owner, everyone, source)
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	rendered := report.String()
	for _, want := range []string{
		"model m6/v0-draft",
		"seed 1",
		"policy baseline",
		"quota 1",
		"bucket capacity 1",
		"near from level 4",
		"probe budget 5",
		"membership whole network",
		"churn: offer(0) depart(7) arrive(7)",
		"scripted candidate stream",
		// ⚠️ The stream ITSELF, not its name: the queue is consumed as the
		// scenario runs, so a report without this snapshot cannot say what the
		// node was offered.
		"as given: 0←[3]",
		// ⚠️ And the trace of what was actually offered, bound to the event
		// that asked for it.
		"offers: #0:3/",
		"coverage before churn:",
		"after churn:",
		"cost:",
		"recovery:",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the report does not carry %q:\n%s", want, rendered)
		}
	}

	// ⚠️ Per-level coverage for ALL THREE snapshots. A share and a near/far
	// aggregate cannot say WHICH level is empty, and §3.3′ is a statement about
	// individual levels.
	if got := strings.Count(rendered, "by level: L0:"); got != 3 {
		t.Errorf("%d per-level lines in the report, want 3 (before churn, after it, final):\n%s",
			got, rendered)
	}
	for level := range setup.Shape.degree {
		if !strings.Contains(rendered, fmt.Sprintf("L%d:", level)) {
			t.Errorf("level %d is missing from the per-level coverage:\n%s", level, rendered)
		}
	}
}

// TestM6ReportOutlivesItsInputs: the event sequence is an input the caller can
// edit afterwards, and the candidate queue is consumed while running. Neither
// may be able to rewrite a published number.
func TestM6ReportOutlivesItsInputs(t *testing.T) {
	const owner = int32(0)
	g, setup := m6Fixture(t, []m6Event{
		{Kind: m6Offer, Node: owner},
		{Kind: m6Depart, Node: 7},
	})
	source := newScriptedCandidates("organic", map[int32][]int32{owner: {3, 4}})

	report, err := runBucketScenario(g, setup, owner, everyone, source)
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	before := report.String()

	setup.Events[0] = m6Event{Kind: m6Arrive, Node: 999}
	setup.Events = append(setup.Events, m6Event{Kind: m6Depart, Node: 1})

	if after := report.String(); after != before {
		t.Errorf("editing the event slice after the run rewrote the report:\n before %s\n after  %s",
			before, after)
	}
}

// TestM6SourcesWithTheSameNameAreDistinguishable: a name is chosen by whoever
// ran the scenario. Two different streams called "organic" must not produce
// reports that look alike.
func TestM6SourcesWithTheSameNameAreDistinguishable(t *testing.T) {
	const owner = int32(0)
	g, setup := m6Fixture(t, offers(owner, 2))

	first, err := runBucketScenario(g, setup, owner, everyone,
		newScriptedCandidates("organic", map[int32][]int32{owner: {3, 4}}))
	if err != nil {
		t.Fatalf("running the first stream: %v", err)
	}
	second, err := runBucketScenario(g, setup, owner, everyone,
		newScriptedCandidates("organic", map[int32][]int32{owner: {5, 6}}))
	if err != nil {
		t.Fatalf("running the second stream: %v", err)
	}

	if first.Source == second.Source {
		t.Errorf("two different streams describe themselves identically: %q", first.Source)
	}
	if first.offerTrace() == second.offerTrace() {
		t.Errorf("two different streams left the same offer trace: %q", first.offerTrace())
	}
	if first.String() == second.String() {
		t.Errorf("two different streams produced identical reports:\n%s", first)
	}
}

// TestM6RejectsWhatItCannotMeasure keeps stand defects out of the results.
func TestM6RejectsWhatItCannotMeasure(t *testing.T) {
	g, setup := m6Fixture(t, nil)
	source := newScriptedCandidates("empty", map[int32][]int32{})

	if _, err := runBucketScenario(g, setup, int32(len(g.ids)+1), everyone, source); err == nil {
		t.Error("an owner outside the graph was accepted")
	}

	setup.Capacity = 0
	if _, err := runBucketScenario(g, setup, 0, everyone, source); err == nil {
		t.Error("a bucket capacity of zero was accepted")
	}

	setup.Capacity = 1
	setup.Events = []m6Event{{Kind: m6Offer, Node: 5}}
	if _, err := runBucketScenario(g, setup, 0, everyone, source); err == nil {
		t.Error("an offer addressed to another node was accepted")
	}
}
