package overlaysim

// m6_model_reference_test.go is the acceptance of the filling model: the level
// arithmetic against INDEPENDENTLY computed expectations, and the scenarios that
// the contract's separation of the three moments stands or falls on — detection,
// the budget freed by it, a return, and a fill that lands in the wrong bucket.
//
// ⚠️ Two rules this file follows and the older fixtures did not:
//
//  1. an expectation is never computed by the function under test. The level
//     table below is written out from the BIT PATTERNS of fixed identifiers, so
//     a levelOf that is wrong in the same way as the expectation cannot pass;
//  2. every scenario is paired with the mutation that should break it, and the
//     mutation is named in the comment — a scenario nobody has made fail is a
//     scenario nobody has checked.

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
)

// --- the level arithmetic, against hand-written expectations ---------------------

// fixedID builds an identifier from the bytes given, zero-filled. The tests
// below then state which BIT first differs, by reading the bytes, and never by
// asking levelOf.
func fixedID(prefix ...byte) nodeID {
	var id nodeID
	copy(id[:], prefix)
	return id
}

// TestLevelOfAgainstHandComputedPairs is the independent reference §5.5 п.9 asks
// for, applied to the bucket arithmetic.
//
// ⚠️ EVERY expectation here was derived from the bit pattern in the comment, not
// from a run. levelOf walks bits from the top of byte 0: bit 0 is 0x80 of byte 0,
// bit 7 is 0x01 of byte 0, bit 8 is 0x80 of byte 1.
func TestLevelOfAgainstHandComputedPairs(t *testing.T) {
	t.Parallel()

	const levels = 8 // d, so the window is levels 0…7

	for _, want := range []struct {
		name      string
		owner     nodeID
		candidate nodeID
		level     int
		why       string
	}{
		{
			name:      "level 0 — the very first bit differs",
			owner:     fixedID(0x00),
			candidate: fixedID(0x80),
			level:     0,
			why:       "0x00 = 00000000, 0x80 = 10000000: they differ at bit 0",
		},
		{
			name:      "level 0 is decided before anything else",
			owner:     fixedID(0x7f, 0xff),
			candidate: fixedID(0xff, 0xff),
			level:     0,
			why:       "0x7f = 01111111, 0xff = 11111111: bit 0 differs and the rest is irrelevant",
		},
		{
			name:      "level 3 — the first three bits agree",
			owner:     fixedID(0x00),
			candidate: fixedID(0x10),
			level:     3,
			why:       "0x00 = 00000000, 0x10 = 00010000: bits 0,1,2 agree, bit 3 differs",
		},
		{
			name:      "level d−1 — the last bit inside the window",
			owner:     fixedID(0x00),
			candidate: fixedID(0x01),
			level:     7,
			why:       "0x00 = 00000000, 0x01 = 00000001: bits 0…6 agree, bit 7 differs",
		},
		{
			name:      "one past the window — bit 8 is outside levels 0…7",
			owner:     fixedID(0x00, 0x00),
			candidate: fixedID(0x00, 0x80),
			level:     -1,
			why: "the first eight bits agree; the first difference is bit 8, which this " +
				"table has no level for",
		},
		{
			name:      "far past the window",
			owner:     fixedID(0x00, 0x00, 0x00),
			candidate: fixedID(0x00, 0x00, 0x01),
			level:     -1,
			why:       "the first difference is bit 23",
		},
		{
			name:      "identical identifiers have no level at all",
			owner:     fixedID(0xab, 0xcd, 0xef),
			candidate: fixedID(0xab, 0xcd, 0xef),
			level:     -1,
			why: "nothing differs anywhere, which is NOT the same finding as 'differs past the " +
				"window' — both are outside the table, and a model that told them apart would be " +
				"claiming knowledge it has no way to have",
		},
		{
			name:      "the zero identifier is not special",
			owner:     fixedID(0xff, 0xff),
			candidate: fixedID(0xff, 0x7f),
			level:     -1,
			why:       "0xff and 0xff agree; 0xff = 11111111 against 0x7f = 01111111 differ at bit 8",
		},
	} {
		t.Run(want.name, func(t *testing.T) {
			t.Parallel()

			if got := levelOf(want.owner, want.candidate, levels); got != want.level {
				t.Errorf("level %d, want %d — %s", got, want.level, want.why)
			}
			// The relation is symmetric: which of the two is the owner cannot
			// change where the first difference is.
			if got := levelOf(want.candidate, want.owner, levels); got != want.level {
				t.Errorf("reversed: level %d, want %d — %s", got, want.level, want.why)
			}
		})
	}

	t.Run("a wider window sees what a narrower one cannot", func(t *testing.T) {
		t.Parallel()

		// ⚠️ The pair that is -1 at d = 8 is level 8 at d = 9. Without this the
		// table above would pass against an implementation that answered -1 for
		// everything past level 7 REGARDLESS of the window it was given.
		owner, candidate := fixedID(0x00, 0x00), fixedID(0x00, 0x80)
		if got := levelOf(owner, candidate, 9); got != 8 {
			t.Errorf("with a window of 9 levels: %d, want 8", got)
		}
		if got := levelOf(owner, candidate, 8); got != -1 {
			t.Errorf("with a window of 8 levels: %d, want -1", got)
		}
	})

	t.Run("a window of zero levels has no level for anybody", func(t *testing.T) {
		t.Parallel()

		if got := levelOf(fixedID(0x00), fixedID(0xff), 0); got != -1 {
			t.Errorf("level %d in a table with no levels, want -1", got)
		}
	})
}

// --- the scenarios ----------------------------------------------------------------

func m6ModelShape() shape {
	return shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
}

// m6ModelBase is the configuration the scenarios vary one field of. Every value
// here is a PROPOSAL of §5 and none is agreed; the fixtures use small tick counts
// because they check mechanisms, not the grid.
func m6ModelBase() m6ModelConfig {
	return m6ModelConfig{
		Shape:        m6ModelShape(),
		Seed:         1,
		Policy:       policyInitiatedLimit,
		Quota:        1,
		Branch:       branchA,
		Capacity:     4,
		NearFrom:     4,
		NearFromRule: "CONTROL d/2 — the fixtures do not derive it",
		Repair:       4,
		// ⚠️ 8, and the choice is not arbitrary. A refresh is served before a
		// filling probe out of the same ceiling R (stand assumption 2), so a
		// cadence short enough to make every level due every tick lets refresh
		// eat the WHOLE ceiling and the node never fills anything. Measured at
		// C = 2, R = 4, d = 8: a node with a full table spent all four probes on
		// refresh, and the "from scratch" control — which has nothing to refresh
		// — out-filled the run that kept its memory. That is a real consequence
		// of the assumption and it is written down in the contract (§6.5); here
		// the fixture simply stays out of the degenerate corner, so the other
		// properties are measured rather than swamped by it.
		Cadence:          8,
		StaleTicks:       8,
		ShelfFirst:       true,
		ExchangeRecords:  4,
		ExchangeEvery:    4,
		AddressedRecords: 2,
		RatePair:         1,
		RateNode:         4,
		Churn:            churnShock,
		ChurnShare:       0.2,
		ReturnShare:      0,
		ReturnAfter:      4,
		JoinMaxWait:      8,
		Ticks:            12,
		ChurnAt:          4,
		Membership:       "whole network",
	}
}

func runM6Model(t *testing.T, config m6ModelConfig) *m6ModelReport {
	t.Helper()

	return runM6ModelOn(t, buildGraph(config.Shape, config.Seed, config.Quota, config.Policy),
		config, everybody)
}

// runM6ModelOn runs one scenario on a GIVEN graph under a given membership, so
// two views of one network can be compared without rebuilding it.
func runM6ModelOn(
	t *testing.T, g *graph, config m6ModelConfig, member func(nodeID) bool,
) *m6ModelReport {
	t.Helper()

	network, err := newM6Network(g, config, member)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	return report
}

// TestM6DetectionIsTheOnlyWayALossIsNoticed is П-4 made executable, and it is the
// scenario the whole rewrite of that section was for.
//
// ⚠️ Mutation that must break it: freeing the budget (or dropping the record) at
// the DEPARTURE instead of at the detection. Under that mutation the claimed and
// the actual coverage are equal at the churn moment, the detection delays are all
// zero, and the C = ∞ branch below stops differing from the C = 2 one at all.
func TestM6DetectionIsTheOnlyWayALossIsNoticed(t *testing.T) {
	t.Parallel()

	report := runM6Model(t, m6ModelBase())

	if report.ClaimedBefore <= report.ActualBefore {
		t.Fatalf("at the churn moment the nodes claimed %d records and %d were alive — a "+
			"departure must not empty anybody's table by itself",
			report.ClaimedBefore, report.ActualBefore)
	}
	if len(report.DetectionDelays) == 0 {
		t.Fatal("no loss was ever detected with a finite cadence, so nothing here is exercised")
	}

	late := 0
	for _, delay := range report.DetectionDelays {
		if delay < 0 {
			t.Fatalf("a loss was detected %d ticks before the departure", delay)
		}
		if delay > 0 {
			late++
		}
	}
	if late == 0 {
		t.Fatal("every loss was detected in the same tick as the departure — then this run cannot " +
			"show that detection is a separate moment")
	}
	t.Logf("%s", report.DetectionDelaySummary())
	t.Logf("%s", report.CoverageLine())
}

// TestM6BudgetIsFreedByDetectionAndNotByDeparture is the second half of П-4, and
// the one with a consequence for the whole network: while the loss is unnoticed,
// the survivor still holds the slot.
func TestM6BudgetIsFreedByDetectionAndNotByDeparture(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)

	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	before := make([]int, len(network.all))
	for _, node := range network.all {
		before[node] = network.heldEdges(node)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	freed := 0
	for _, node := range network.all {
		if network.heldEdges(node) < before[node] {
			freed++
		}
	}
	if freed == 0 {
		t.Fatal("no node ever freed a slot, so detection is not reaching the budget at all")
	}

	// ⚠️ And the control that gives the claim its meaning — stated PRECISELY,
	// because the obvious version of it is false.
	//
	// With C = ∞ there is no refresh, so nothing already IN a table is ever
	// re-checked and no held record is ever dropped. That is the degenerate
	// recovery axis the negative control exists to show. What C = ∞ does NOT buy
	// is "no detection at all": П-4 says a loss is detected by an unsuccessful
	// PROBE, and a FILLING probe that happens to reach a departed peer is an
	// unsuccessful probe like any other. Such a node was never in the table, so
	// nothing is lost — but the edge to it is real, and it is freed.
	//
	// An earlier version of this test asserted that nobody frees anything at
	// C = ∞, and it passed for the wrong reason: nodes were dead-locked on an
	// empty near level and made no filling probes at all.
	silent := config
	silent.Cadence = 0
	silentNetwork, err := newM6Network(buildGraph(config.Shape, config.Seed, config.Quota,
		config.Policy), silent, everybody)
	if err != nil {
		t.Fatalf("preparing the control: %v", err)
	}
	silentReport, err := silentNetwork.Run()
	if err != nil {
		t.Fatalf("running the control: %v", err)
	}

	if sumOf(silentReport.LostByLevel) != 0 {
		t.Fatalf("with C = ∞ the control dropped %d held records — nothing re-checks a record "+
			"that is already in a table, so none may be lost", sumOf(silentReport.LostByLevel))
	}
	if len(silentReport.DetectionDelays) == 0 {
		t.Log("with C = ∞ no departed peer was probed at all in this run")
	}
	if !strings.Contains(silentReport.RecoveryLine(), "negative control") {
		t.Errorf("the C = ∞ report does not say its degenerate recovery is the expected result:\n%s",
			silentReport.RecoveryLine())
	}

	// The cadence is what makes the difference, and it has to be visible: with a
	// finite C, records held in tables ARE lost, and with C = ∞ none is.
	if sumOf(report.LostByLevel) == 0 {
		t.Fatal("with a finite cadence nothing was lost either, so the two branches cannot be " +
			"told apart in this run")
	}

	t.Logf("with a cadence: %d nodes freed a slot and %d held records were lost; with C = ∞: "+
		"%d held records lost", freed, sumOf(report.LostByLevel), sumOf(silentReport.LostByLevel))
}

// TestM6RecoveryIsCountedAgainstTheLevelThatLostASlot guards the accounting that
// makes the third axis mean anything.
//
// ⚠️ Mutation that must break it: crediting any fill after the churn as recovery.
// Under it RefilledByLevel exceeds LostByLevel at some level and FilledElsewhere
// falls to zero.
func TestM6RecoveryIsCountedAgainstTheLevelThatLostASlot(t *testing.T) {
	t.Parallel()

	// ⚠️ Branch A′, not A. Under A a node's pool is its own handful of
	// neighbours, and a short run puts every one of them in the table before the
	// churn — after which there is nothing left to fill ANY level with, and the
	// distinction between recovery and coverage cannot show itself. The fixture
	// has to be able to fill something for the accounting to be exercised at all.
	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Ticks = 20
	report := runM6Model(t, config)

	if sumOf(report.LostByLevel) == 0 {
		t.Fatal("nothing was lost, so the accounting is not exercised")
	}
	for level := range report.LostByLevel {
		if report.RefilledByLevel[level] > report.LostByLevel[level] {
			t.Errorf("level %d: %d refills credited against %d losses — recovery cannot exceed "+
				"what was lost", level, report.RefilledByLevel[level], report.LostByLevel[level])
		}
	}
	if report.FilledElsewhere == 0 {
		t.Fatal("no fill landed at a level that lost nothing — then this run cannot show that " +
			"coverage and recovery are counted apart")
	}
	if !strings.Contains(report.RecoveryLine(), "coverage, NOT recovery") {
		t.Errorf("the report does not separate coverage from recovery:\n%s", report.RecoveryLine())
	}
	t.Logf("%s", report.RecoveryLine())
}

// TestM6AReturnKeepsItsTableAndANewcomerDoesNot is §5.9.2: coming back is not the
// same as arriving, and the difference is the table the returning node never lost.
func TestM6AReturnKeepsItsTableAndANewcomerDoesNot(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ReturnShare = 1
	config.ReturnAfter = 3
	config.Ticks = 20

	report := runM6Model(t, config)

	if report.ArrivalsOffered == 0 {
		t.Fatal("nobody was offered a return, so this run proves nothing about returns")
	}
	if report.ArrivalsAdmitted == 0 {
		t.Fatal("no return was admitted — the entry queue refused every one, which is a legitimate " +
			"outcome but not the one this fixture needs")
	}

	// ⚠️ The measurement, not an argument: a node that comes back under its own
	// NodeID still holds the records it had, because it was offline rather than
	// wiped. Counted at the moment of admission, so a later refresh cannot be
	// mistaken for memory.
	if report.ReturnedWithATable == 0 {
		t.Errorf("every one of the %d returns came back empty-handed — a return is not a rebuild "+
			"(П-6), and this is the number that says so", report.ReturnedEmptyHanded)
	}
	if report.NewcomersAdmitted != 0 {
		t.Errorf("%d newcomers were admitted in a shock run — the reserve is only drawn on under "+
			"compensated load, so this fixture is not measuring what it claims",
			report.NewcomersAdmitted)
	}
	// And the entry queue is a real gate: a return can be refused and give up,
	// which §5.9.2 requires to be counted rather than dropped quietly.
	if report.ArrivalsAdmitted+report.GaveUpJoining > report.ArrivalsOffered {
		t.Errorf("%d admitted plus %d gave up, from %d offered", report.ArrivalsAdmitted,
			report.GaveUpJoining, report.ArrivalsOffered)
	}
	t.Logf("%s; returns with a table: %d, empty-handed: %d",
		report.PopulationLine(), report.ReturnedWithATable, report.ReturnedEmptyHanded)
}

// TestM6FromScratchIsAControlAndSaysWhatItErases is П-6.
//
// ⚠️ The comparison is only worth something because everything else is held
// fixed: the same graph, the same seed, the same events, the same branch. The one
// difference is the memory, and the report has to say so where the numbers are.
func TestM6FromScratchIsAControlAndSaysWhatItErases(t *testing.T) {
	t.Parallel()

	main := runM6Model(t, m6ModelBase())

	control := m6ModelBase()
	control.StartEmpty = true
	scratch := runM6Model(t, control)

	if !strings.Contains(scratch.Config.String(), "FIRST FILLING, not recovery") {
		t.Errorf("the control does not name itself a first filling:\n%s", scratch.Config)
	}
	if sumOf(scratch.LostByLevel) != 0 {
		t.Errorf("the ‘from scratch’ control counted %d losses — with the tables cleared there is "+
			"nothing to have lost, and calling it recovery is exactly what П-6 forbids",
			sumOf(scratch.LostByLevel))
	}
	if sumOf(main.LostByLevel) == 0 {
		t.Fatal("the main run lost nothing, so the two cannot be compared")
	}

	// The control must actually differ: same inputs, different memory.
	mainClaimed, scratchClaimed := 0, 0
	for _, level := range main.Levels {
		mainClaimed += level.Claimed
	}
	for _, level := range scratch.Levels {
		scratchClaimed += level.Claimed
	}
	if scratchClaimed >= mainClaimed {
		t.Errorf("the control finished with %d claimed records against the main run's %d — "+
			"starting from nothing cannot be as good as keeping what you had, so either the "+
			"clearing did not happen or the main run learned nothing before the churn",
			scratchClaimed, mainClaimed)
	}
	t.Logf("main run: %d claimed records; from scratch: %d", mainClaimed, scratchClaimed)

	t.Run("clearing after a loss was already detected is refused", func(t *testing.T) {
		// ⚠️ The invariant the erasure rests on, made to fail. The control wipes
		// the tables at the FIRST departure, before anybody has served, so
		// nothing can have been detected — and if the clearing ever moves later,
		// the run would silently become "recovery, then a rebuild", which is
		// neither of the two things П-6 compares.
		g := buildGraph(m6ModelShape(), 1, 1, policyInitiatedLimit)
		network, err := newM6Network(g, control, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		network.report.LostByLevel[2] = 1

		if err := network.clearForTheFromScratchControl(); err == nil {
			t.Error("the control cleared the tables with a detected loss already on the books")
		} else if !strings.Contains(err.Error(), "FIRST FILLING") {
			t.Errorf("the refusal does not explain itself: %v", err)
		}
	})
}

// TestM6BranchesAreNestedAndSayWhatTheyReveal is П-1: A ⊂ A′ ⊂ C by construction,
// so the pool can only grow, and every branch states its disclosure next to its
// cost.
func TestM6BranchesAreNestedAndSayWhatTheyReveal(t *testing.T) {
	t.Parallel()

	pools := map[m6Branch]int{}
	for _, branch := range []m6Branch{branchA, branchB, branchAPrime, branchC} {
		config := m6ModelBase()
		config.Branch = branch
		report := runM6Model(t, config)

		median, ok := medianOf(report.PoolByOwner)
		if !ok {
			t.Fatalf("%s: no pool was measured", branch)
		}
		pools[branch] = median

		rendered := report.String()
		if !strings.Contains(rendered, "ANALYTICAL") {
			t.Errorf("%s: S(u) is printed without saying it is analysis:\n%s", branch, rendered)
		}
		if !strings.Contains(rendered, branch.Reveals()) {
			t.Errorf("%s: the report does not carry what the branch reveals", branch)
		}
	}

	if pools[branchAPrime] < pools[branchA] {
		t.Errorf("A′ offers a median pool of %d against A's %d — A′ contains A",
			pools[branchAPrime], pools[branchA])
	}
	if pools[branchC] < pools[branchAPrime] {
		t.Errorf("C offers a median pool of %d against A′'s %d — C contains A′",
			pools[branchC], pools[branchAPrime])
	}
	// ⚠️ And the disclosure warning must be on A′ and C and NOT on A, or it is
	// noise that means nothing.
	if strings.Contains(branchA.Reveals(), "⚠️") {
		t.Error("branch A carries a disclosure warning it has nothing to warn about")
	}
	t.Logf("median S(u): A=%d B=%d A′=%d C=%d",
		pools[branchA], pools[branchB], pools[branchAPrime], pools[branchC])
}

// TestM6ReportsPopulationOfEachLevelAsAnalysis is the extra requirement of П-5:
// an empty near level means nothing until the reader knows whether anybody lives
// there.
func TestM6ReportsPopulationOfEachLevelAsAnalysis(t *testing.T) {
	t.Parallel()

	report := runM6Model(t, m6ModelBase())

	if len(report.Levels) != m6ModelShape().degree {
		t.Fatalf("%d levels reported, the shape has %d", len(report.Levels), m6ModelShape().degree)
	}
	// Level 0 is half the network and level d−1 is 1/2^d of it: the population
	// must fall steeply, and if it does not, the number is not a population.
	if report.Levels[0].Population <= report.Levels[len(report.Levels)-1].Population {
		t.Errorf("level 0 holds %d and level %d holds %d — the near levels are the sparse ones",
			report.Levels[0].Population, len(report.Levels)-1,
			report.Levels[len(report.Levels)-1].Population)
	}
	if !strings.Contains(report.CoverageLine(), "pop ") {
		t.Errorf("the coverage line does not carry the level population:\n%s", report.CoverageLine())
	}

	t.Run("the derived near border comes from the measured pool, not a formula", func(t *testing.T) {
		border, rule := derivedNearFrom(report.PoolByOwner, m6ModelShape().degree)
		if border < 0 || border > m6ModelShape().degree {
			t.Fatalf("derived border %d is outside 0…%d", border, m6ModelShape().degree)
		}
		if !strings.Contains(rule, "MEASURED") {
			t.Errorf("the rule does not say the pool was measured: %q", rule)
		}

		// An empty reference falls back to the CONTROL and says so, rather than
		// deriving a border from nothing.
		fallback, fallbackRule := derivedNearFrom(nil, 8)
		if fallback != 4 || !strings.Contains(fallbackRule, "CONTROL d/2") {
			t.Errorf("with no reference pool: border %d, rule %q", fallback, fallbackRule)
		}
	})
}

// TestM6RateLimitIsOneDefinitionWithTwoHalves is §5.1.2 п.4: the addressed branch
// is bounded per asker×level AND per responder, and the report shows refusals.
func TestM6RateLimitIsOneDefinitionWithTwoHalves(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchC
	// Tight enough that the limit has to bite somewhere.
	config.RatePair = 1
	config.RateNode = 1
	report := runM6Model(t, config)

	if report.AddressedAnswers == 0 {
		t.Fatal("no addressed request was ever answered, so the limit is not exercised")
	}
	if report.AddressedRateLimited == 0 {
		t.Fatal("no responder ever refused with r_pair = r_node = 1 — then the rate limit is not " +
			"being applied at the responder")
	}
	// ⚠️ And the two refusal counters must not be the same number: a refusal at
	// one responder that the next responder then serves is load the limit shed
	// WITHOUT an unanswered request, and merging the two would report an outage
	// where the limit simply did its job.
	if report.AddressedRefused > report.AddressedRateLimited {
		t.Errorf("%d unanswered requests against %d refusals at a responder — a request cannot go "+
			"unanswered more often than a responder refuses it",
			report.AddressedRefused, report.AddressedRateLimited)
	}
	if !strings.Contains(report.Config.String(), "the single definition") {
		t.Errorf("the report does not point at the one place the limit is defined:\n%s",
			report.Config)
	}
	t.Logf("addressed: %d answered, %d refusals at a responder, %d requests nobody answered",
		report.AddressedAnswers, report.AddressedRateLimited, report.AddressedRefused)
}

// TestM6RejectsConfigurationsItCannotMeasure keeps stand defects out of results.
func TestM6RejectsConfigurationsItCannotMeasure(t *testing.T) {
	t.Parallel()

	g := buildGraph(m6ModelShape(), 1, 1, policyBaseline)

	for _, bad := range []struct {
		name  string
		edit  func(*m6ModelConfig)
		match string
	}{
		{"no slots", func(c *m6ModelConfig) { c.Capacity = 0 }, "no slots"},
		{"no ticks", func(c *m6ModelConfig) { c.Ticks = 0 }, "measures nothing"},
		{"a near border past d", func(c *m6ModelConfig) { c.NearFrom = 99 }, "outside"},
	} {
		config := m6ModelBase()
		bad.edit(&config)
		if _, err := newM6Network(g, config, everybody); err == nil {
			t.Errorf("%s was accepted", bad.name)
		} else if !strings.Contains(err.Error(), bad.match) {
			t.Errorf("%s: the error does not explain itself: %v", bad.name, err)
		}
	}

	if _, err := newM6Network(g, m6ModelBase(), func(nodeID) bool { return false }); err == nil {
		t.Error("an empty membership was accepted")
	}
}

// TestM6OmniscientControlIsNotAMechanismAndNotAnUpperBound is §4.4 in the network
// model.
//
// ⚠️ Two claims, and the second is the one a reviewer took away: the control must
// be labelled a control WHEREVER its numbers are read, and it must NOT be called
// a mathematical upper bound. It is bounded by the same k, the same B, the same
// ceiling R and the same graph as everything else, so "no mechanism can beat it"
// is a statement about all possible mechanisms that nothing here proves.
func TestM6OmniscientControlIsNotAMechanismAndNotAnUpperBound(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.OmniscientControl = true
	control := runM6Model(t, config)
	branch := runM6Model(t, m6ModelBase())

	rendered := control.String()
	for _, want := range []string{
		"CONTROL — omniscient source",
		"not a branch and not a mechanism",
		"CONTROL RESULT UNDER THE STATED CONSTRAINTS",
		"NOT claimed to be a mathematical upper bound",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the control report does not carry %q:\n%s", want, rendered)
		}
	}
	// And an ordinary branch must NOT carry the label, or it is noise.
	if strings.Contains(branch.String(), "CONTROL — omniscient") {
		t.Errorf("branch A was reported as the omniscient control:\n%s", branch)
	}

	// What the control OWES is a bigger AVAILABLE population — that is what
	// "knows everybody" means, and it is a property of the source.
	//
	// ⚠️ IT DOES NOT OWE A BIGGER MEASURED POOL, and requiring one was a category
	// error in the earlier version of this fixture. S(u) counts what a mechanism
	// ACTUALLY handed the node, and the offers are bounded by the ceiling R and
	// by how many ticks the run lasted: a source that could name a thousand nodes
	// still only names as many as it gets probes for. Asserting a strict
	// inequality on the measured pool compared an availability against a rate and
	// passed for reasons neither quantity explains.
	controlPotential, ok := medianOf(control.PoolPotential)
	branchPotential, alsoOK := medianOf(branch.PoolPotential)
	if !ok || !alsoOK {
		t.Fatal("no potential pool was measured for one of the two runs")
	}
	if controlPotential <= branchPotential {
		t.Errorf("the omniscient control could reach a median of %d candidates against branch A's "+
			"%d — a source that may offer anybody must have more of them AVAILABLE, so the "+
			"control is not being applied", controlPotential, branchPotential)
	}

	// The measured pool is a RESULT and is printed as one. The only thing it owes
	// is that the control's offers reach it at all — they used to be dropped,
	// which made a table filled with strangers report a pool of the owner's own
	// neighbours (see TestM6TheMeasuredPoolCountsTheControlsOffers).
	controlPool, ok := medianOf(control.PoolByOwner)
	branchPool, alsoOK := medianOf(branch.PoolByOwner)
	if !ok || !alsoOK {
		t.Fatal("no pool was measured for one of the two runs")
	}

	// ⚠️ AND NOTHING IS ASSERTED ABOUT WHICH FILLS MORE SLOTS. An earlier version
	// required the control to fill more, on the reasoning that perfect knowledge
	// cannot be worse — and the model refutes it: measured at 1k×8 with the base
	// configuration, the control fills a few thousand records against branch A's
	// roughly ten thousand (the run logs both numbers below rather than pinning
	// them here, because they move with every parameter of the fixture).
	// The reason is in the ledger and it is a real property of the network, not
	// of the stand: the control probes STRANGERS, and in a network where most
	// nodes sit at their ceiling B about half of those probes are refused
	// (19 723 of 41 600 in that run), while branch A only ever probes peers it is
	// ALREADY CONNECTED TO, which cannot refuse for lack of budget.
	//
	// This is exactly why the control is NOT a mathematical upper bound, and it
	// is worth more than the assertion it replaced: the word "upper bound" would
	// have been contradicted by the stand's own numbers.
	controlFilled, branchFilled := 0, 0
	for _, level := range control.Levels {
		controlFilled += level.Claimed
	}
	for _, level := range branch.Levels {
		branchFilled += level.Claimed
	}
	t.Logf("median AVAILABLE population: control %d, branch A %d", controlPotential, branchPotential)
	t.Logf("median MEASURED pool S(u) — a result, bounded by R and by the run's length: "+
		"control %d, branch A %d", controlPool, branchPool)
	t.Logf("⚠️ records filled — a RESULT, not a requirement: control %d, branch A %d "+
		"(the control's probes go to strangers, and a stranger at its ceiling B refuses)",
		controlFilled, branchFilled)

}

// TestBranchCAnswersOnlyTheLevelThatWasAsked is the independent reference for
// what a branch-C responder may hand over.
//
// ⚠️ It replaces a test that pinned an ERROR. The earlier implementation
// translated the asked level into ONE bucket of the responder and returned that
// bucket whole, and the test asserted the translation rather than the answer —
// so it agreed with the mistake in both of its forms: a wider range disclosed
// than the asker asked for, and, when the asked level equals the level the
// responder sees the asker at, the wrong bucket entirely. The property that
// matters is not which bucket was read; it is that EVERY record returned lies on
// the level the asker named, relative to the ASKER.
func TestBranchCAnswersOnlyTheLevelThatWasAsked(t *testing.T) {
	t.Parallel()

	const levels = 8

	// A hand-built network: identifiers chosen so the level of every pair can be
	// read off the top byte. Node 0 asks, node 1 answers.
	build := func(ids []nodeID, table []int32) *m6Network {
		g := &graph{
			ids:                  ids,
			roles:                make([]int, len(ids)),
			adjacency:            make([][]int32, len(ids)),
			structuralNeighbours: make([]int, len(ids)),
			initiated:            make([]int, len(ids)),
			shortfall:            make([]quotaShortfall, len(ids)),
		}
		network := &m6Network{
			g:      g,
			ids:    ids,
			roles:  g.roles,
			config: m6ModelConfig{Shape: shape{degree: levels}, Capacity: 8, AddressedRecords: 8},
			states: map[int32]*m6NodeState{},
		}
		state := newM6NodeState(1, levels, 8, 4)
		for _, member := range table {
			level := levelOf(ids[1], ids[member], levels)
			if level < 0 {
				t.Fatalf("fixture: node %d has no level in the responder's table", member)
			}
			state.Table.members[level][member] = struct{}{}
			state.Table.Coverage.Held[level]++
		}
		network.states[1] = state
		return network
	}

	t.Run("the case the old translation got backwards", func(t *testing.T) {
		t.Parallel()

		// Asker 0x00…, responder 0x80…, record 0xc0…
		//   0x00 vs 0xc0 = 00000000 vs 11000000 → the record is on the ASKER's
		//                                          level 0;
		//   0x80 vs 0xc0 = 10000000 vs 11000000 → it sits in the RESPONDER's
		//                                          bucket 1;
		//   0x80 vs 0x00                        → the responder sees the asker at
		//                                          level 0.
		// The old rule read the responder's bucket 0 and found nothing there.
		ids := []nodeID{fixedID(0x00), fixedID(0x80), fixedID(0xc0)}
		network := build(ids, []int32{2})

		got := network.recordsOnLevel(1, 0, 0)
		if len(got) != 1 || got[0] != 2 {
			t.Fatalf("answer %v, want the one record that is on the asker's level 0", got)
		}
	})

	t.Run("nothing outside the named level is ever returned", func(t *testing.T) {
		t.Parallel()

		// A responder holding records on several of the asker's levels at once.
		ids := []nodeID{
			fixedID(0x00), // 0: the asker
			fixedID(0x80), // 1: the responder
			fixedID(0xc0), // 2: asker level 0
			fixedID(0x90), // 3: asker level 0
			fixedID(0x40), // 4: asker level 1
			fixedID(0x20), // 5: asker level 2
			fixedID(0x10), // 6: asker level 3
		}
		network := build(ids, []int32{2, 3, 4, 5, 6})

		for level, want := range map[int][]int32{
			0: {2, 3},
			1: {4},
			2: {5},
			3: {6},
			4: nil,
		} {
			got := network.recordsOnLevel(1, 0, level)
			for _, member := range got {
				if actual := levelOf(ids[0], ids[member], levels); actual != level {
					t.Errorf("level %d: record %d is on the asker's level %d — the answer leaves "+
						"the range that was asked for", level, member, actual)
				}
			}
			if len(got) != len(want) {
				t.Errorf("level %d: %d records, want %d (%v)", level, len(got), len(want), want)
			}
		}
	})

	t.Run("the cap n applies after the filter, not before", func(t *testing.T) {
		t.Parallel()

		ids := []nodeID{fixedID(0x00), fixedID(0x80), fixedID(0xc0), fixedID(0x90), fixedID(0xa0)}
		network := build(ids, []int32{2, 3, 4})
		network.config.AddressedRecords = 2

		got := network.recordsOnLevel(1, 0, 0)
		if len(got) != 2 {
			t.Fatalf("%d records against n = 2", len(got))
		}
		// And the two are the CLOSEST to the asker, not an arbitrary pair:
		// 0x90 and 0xa0 are nearer to 0x00 than 0xc0 is.
		if got[0] != 3 || got[1] != 4 {
			t.Errorf("answer %v, want the two records closest to the asker (3=0x90, 4=0xa0)", got)
		}
	})

	t.Run("the answer does not depend on the order the table was filled", func(t *testing.T) {
		t.Parallel()

		// ⚠️ The determinism the contract needs and Go maps do not give: the
		// records live in a map, so an answer taken in iteration order changes
		// between runs of ONE seed — and with it every probe that follows.
		ids := []nodeID{
			fixedID(0x00), fixedID(0x80),
			fixedID(0xc0), fixedID(0x90), fixedID(0xa0), fixedID(0xe0), fixedID(0xf0),
		}
		forwards := build(ids, []int32{2, 3, 4, 5, 6})
		backwards := build(ids, []int32{6, 5, 4, 3, 2})
		forwards.config.AddressedRecords = 3
		backwards.config.AddressedRecords = 3

		first := forwards.recordsOnLevel(1, 0, 0)
		for range 8 {
			if got := forwards.recordsOnLevel(1, 0, 0); fmt.Sprint(got) != fmt.Sprint(first) {
				t.Fatalf("two identical requests answered %v and %v", first, got)
			}
			if got := backwards.recordsOnLevel(1, 0, 0); fmt.Sprint(got) != fmt.Sprint(first) {
				t.Fatalf("the same table filled in the other order answered %v against %v",
					got, first)
			}
		}
	})
}

// TestM6RefreshRotatesThroughTheMembersOfALevel is finding-driven: a refresh that
// always probed the same member would never find a dead record sitting beside a
// live one, and the bias would land squarely on the two quantities П-4 exists to
// produce.
func TestM6RefreshRotatesThroughTheMembersOfALevel(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Cadence = 1
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	// A level holding three members, filled by hand so the rotation is the only
	// thing under test.
	state := network.states[0]
	const level = 2
	for _, member := range []int32{11, 22, 33} {
		state.Table.members[level][member] = struct{}{}
		state.Table.Coverage.Held[level]++
	}
	// ⚠️ The fixture fills the level by hand, so it must also start its cadence
	// clock by hand: a level is due C ticks after it first held a record, and
	// filling it behind the model's back leaves the clock at zero. Setting it
	// back by C makes the level due now, which is what this test is about.
	state.LastRefreshed[level] = -config.Cadence

	seen := map[int32]int{}
	for tick := range 3 {
		network.tick = tick
		gotLevel, member, ok := network.levelDueForRefresh(state)
		if !ok {
			t.Fatalf("tick %d: the cadence found nothing to refresh", tick)
		}
		if gotLevel != level {
			// Another level may legitimately come first; skip it and keep going.
			continue
		}
		seen[member]++
	}
	if len(seen) < 2 {
		t.Fatalf("three consecutive refreshes of one level touched %d distinct members (%v) — a "+
			"dead record beside a live one would never be found", len(seen), seen)
	}
}

// TestM6RateLimitBitesOnBothHalvesSeparately is the other half of §5.1.2 п. 4.
//
// ⚠️ Testing "some request was refused" with both halves tight proves only that
// ONE of them works — and it was the per-pair one, so the per-responder half was
// untested while the test's name claimed otherwise. Each half is now loosened in
// turn, so a refusal can only have come from the other.
func TestM6RateLimitBitesOnBothHalvesSeparately(t *testing.T) {
	t.Parallel()

	const loose = 1 << 20

	t.Run("the per-responder half alone", func(t *testing.T) {
		t.Parallel()

		config := m6ModelBase()
		config.Branch = branchC
		config.RatePair = loose
		config.RateNode = 1
		report := runM6Model(t, config)

		if report.AddressedAnswers == 0 {
			t.Fatal("no addressed request was answered, so nothing is exercised")
		}
		if report.AddressedRateLimited == 0 {
			t.Error("with r_node = 1 and r_pair unbounded no responder ever refused — the " +
				"per-responder half of the limit is not applied")
		}
	})

	t.Run("the per-pair half alone", func(t *testing.T) {
		t.Parallel()

		config := m6ModelBase()
		config.Branch = branchC
		config.RatePair = 1
		config.RateNode = loose
		report := runM6Model(t, config)

		if report.AddressedAnswers == 0 {
			t.Fatal("no addressed request was answered, so nothing is exercised")
		}
		if report.AddressedRateLimited == 0 {
			t.Error("with r_pair = 1 and r_node unbounded no responder ever refused — the " +
				"per-pair half of the limit is not applied")
		}
	})
}

// TestM6MeasuresTheQHalfUnderTheSameConditions is §4.4 in the network model, and
// it was missing entirely: every fixture ran the whole network, while the grid of
// §5.9.5 is 20 configurations × TWO POPULATIONS.
func TestM6MeasuresTheQHalfUnderTheSameConditions(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)

	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
	half := config
	half.Membership = "Q half"

	whole, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing the whole network: %v", err)
	}
	wholeReport, err := whole.Run()
	if err != nil {
		t.Fatalf("running the whole network: %v", err)
	}

	network, err := newM6Network(g, half, structural)
	if err != nil {
		t.Fatalf("preparing the Q half: %v", err)
	}
	halfReport, err := network.Run()
	if err != nil {
		t.Fatalf("running the Q half: %v", err)
	}

	if halfReport.Members == 0 || halfReport.Members >= wholeReport.Members {
		t.Fatalf("the Q half holds %d members against the whole network's %d",
			halfReport.Members, wholeReport.Members)
	}
	// ⚠️ The denominator of the population line is the MEMBERSHIP, not the
	// shape's N: printing N here would invent half a network of missing nodes.
	if !strings.Contains(halfReport.PopulationLine(),
		fmt.Sprintf("ORIGINAL %s population", half.Membership)) {
		t.Errorf("the population line does not name its own membership:\n%s",
			halfReport.PopulationLine())
	}
	if strings.Contains(halfReport.PopulationLine(),
		fmt.Sprintf("the %d ORIGINAL", config.Shape.nodes)) {
		t.Errorf("the Q half is measured against the whole shape's N:\n%s",
			halfReport.PopulationLine())
	}
	t.Logf("whole: %s\nhalf:  %s", wholeReport.PopulationLine(), halfReport.PopulationLine())
}

// TestM6ReferencePoolNeedsAGraphAndNotARun is §5.1.1: the border of the near
// levels is derived from the pool of branch A, which is a property of the built
// graph — a node's own edges — and therefore costs no scenario run at all.
//
// ⚠️ That is also why the run registry counts the reference pool as zero runs.
func TestM6ReferencePoolNeedsAGraphAndNotARun(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)

	border, rule, err := referencePool(g, config, everybody)
	if err != nil {
		t.Fatalf("deriving: %v", err)
	}
	if border < 0 || border > config.Shape.degree {
		t.Fatalf("derived border %d is outside 0…%d", border, config.Shape.degree)
	}
	if !strings.Contains(rule, "MEASURED") {
		t.Errorf("the rule does not say the pool was measured: %q", rule)
	}

	// The same graph must give the same border however the CONFIGURATION varies,
	// because the reference is branch A on the full graph and nothing else. A
	// border that moved with the branch would compare the four branches at
	// different levels — the very thing §5.1.1 forbids.
	for _, branch := range []m6Branch{branchB, branchAPrime, branchC} {
		other := config
		other.Branch = branch
		other.Churn = churnShrink
		got, _, err := referencePool(g, other, everybody)
		if err != nil {
			t.Fatalf("%s: %v", branch, err)
		}
		if got != border {
			t.Errorf("%s derived border %d, branch A derived %d — the reference must be one for "+
				"the whole comparison", branch, got, border)
		}
	}
	t.Logf("near levels start at %d — %s", border, rule)
}

// TestM6AnEmptyNearLevelDoesNotFreezeTheNode is the fixture for the deadlock the
// service order used to have.
//
// ⚠️ It was not a rare case. The order serves the NEAREST level first, and at
// 1k×8 level 7 is populated by ≈4 nodes of a thousand — so "the nearest level has
// no candidate" is the normal state, and the node stopped there and left every
// other bucket empty while its own acquaintances sat unused for the whole run.
// The fixture below makes it unmissable: the near level has NOBODY, the far level
// has somebody, and the far level must fill.
func TestM6AnEmptyNearLevelDoesNotFreezeTheNode(t *testing.T) {
	t.Parallel()

	const levels = 4
	// Identifiers chosen so the owner has a reachable neighbour on level 0 and
	// NOTHING anywhere nearer: 0x00 against 0x80 differs at bit 0, and no other
	// node exists at all.
	ids := []nodeID{fixedID(0x00), fixedID(0x80)}
	g := &graph{
		ids:                  ids,
		roles:                []int{roleStructural, roleStructural},
		adjacency:            [][]int32{{1}, {0}},
		structuralNeighbours: []int{1, 1},
		initiated:            []int{1, 0},
		shortfall:            make([]quotaShortfall, 2),
	}

	config := m6ModelBase()
	config.Shape = shape{name: "2×4", nodes: 2, degree: levels, budget: 8}
	config.Capacity = 1
	config.NearFrom = 2
	config.Churn = churnNone
	config.Ticks = 4
	config.Cadence = 0 // no refresh: filling only, so the fixture measures one thing

	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	// Levels 1…3 have nobody in this network and are served FIRST by the order of
	// §5.9.3. Level 0 has the one neighbour, and it must end up there.
	if got := report.Levels[0].Claimed; got == 0 {
		t.Fatalf("level 0 holds %d records: the nearer levels are empty and the node never got "+
			"past them — %s", got, report.CoverageLine())
	}
	for level := 1; level < levels; level++ {
		if got := report.Levels[level].Claimed; got != 0 {
			t.Errorf("level %d holds %d records in a network where nobody belongs there",
				level, got)
		}
	}
	t.Logf("%s", report.CoverageLine())
}

// TestM6ANewcomerArrivesWithExactlyOneLinkAtBothEnds is §5.9.2 for the arrival
// side.
//
// ⚠️ A reserve node already sits in the built graph with a full set of edges.
// Joining must not hand them over: it did, because `joined` was the only gate,
// so a newcomer instantly had a neighbourhood nobody had met and nobody's budget
// had paid for — free acquaintances at one end and a heldEdges count that no
// longer matched the adjacency at the other.
func TestM6ANewcomerArrivesWithExactlyOneLinkAtBothEnds(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Churn = churnCompensated
	config.ChurnShare = 0.02
	config.ChurnAt = 1
	config.Ticks = 10

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	if report.NewcomersAdmitted == 0 {
		t.Fatal("no newcomer was admitted, so this fixture proves nothing")
	}

	checked := 0
	for _, node := range network.reserve {
		if !network.joined[node] {
			continue
		}
		checked++

		// A newcomer's connections are exactly those that were ESTABLISHED: the
		// starting link admit() gave it, plus any later newcomer that chose it as
		// a host. It has no edges in the built graph at all — the reserve is drawn
		// beyond the population — so there is nothing to leak.
		neighbours := network.neighboursOf(node)
		if len(neighbours) == 0 && len(network.states[node].Released) == 0 {
			t.Fatalf("newcomer %d was admitted with no link at all and released none", node)
		}
		for _, peer := range neighbours {
			// The link is reciprocal: a starting link occupies a slot at BOTH
			// ends (§5.9.2). ⚠️ Unless the peer RELEASED it — a node that
			// detected this one gone while it was offline keeps the release when
			// it comes back, because §5.9.2 says a return does not resurrect a
			// released edge. That asymmetry is the model working, not a leak.
			if network.holdsEdge(peer, node) {
				continue
			}
			if network.states[peer] != nil {
				if _, dropped := network.states[peer].Released[node]; dropped {
					continue
				}
			}
			t.Fatalf("newcomer %d believes it is linked to %d, %d does not agree, and %d never "+
				"released it", node, peer, peer, peer)
		}

		// And nobody who was not linked to it sees it at all.
		//
		// ⚠️ ONE asymmetry is legitimate and has to be allowed for: a release
		// happens at the end that DETECTED the loss (П-4), so the newcomer may
		// have dropped a peer that still believes it holds the link. Anything
		// else would be a connection nobody ever established.
		linked := map[int32]struct{}{}
		for _, peer := range neighbours {
			linked[peer] = struct{}{}
		}
		released := network.states[node].Released
		for _, other := range network.all {
			if other == node {
				continue
			}
			if _, expected := linked[other]; expected {
				continue
			}
			if _, dropped := released[other]; dropped {
				continue
			}
			if network.holdsEdge(other, node) {
				t.Fatalf("node %d holds a connection to newcomer %d that was never established",
					other, node)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no admitted newcomer was found to check")
	}
	t.Logf("%d newcomers checked; %s", checked, report.PopulationLine())
}

// TestM6TheQHalfDiffersOnlyInWhoIsMeasured is §4.4, and it checks the CONDITIONS
// rather than the labels.
//
// ⚠️ The earlier version compared the denominator printed in the population line,
// which is the one thing that is SUPPOSED to differ. Underneath, three conditions
// were changing at once: the budget (edges to a ¬Q peer stopped occupying B), the
// candidates, and the churn trace (departures were drawn from the membership, so
// one seed gave two different event sequences). A comparison of two runs that
// differ in three things cannot attribute anything to the half.
func TestM6TheQHalfDiffersOnlyInWhoIsMeasured(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ReturnShare = 0.5
	config.Ticks = 14

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	whole, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing the whole network: %v", err)
	}
	half := config
	half.Membership = "Q half"
	halfNetwork, err := newM6Network(g, half, structural)
	if err != nil {
		t.Fatalf("preparing the Q half: %v", err)
	}

	// 1. The BUDGET is physical and identical before either run starts.
	for _, node := range whole.all {
		if whole.heldEdges(node) != halfNetwork.heldEdges(node) {
			t.Fatalf("node %d starts with %d held slots in the whole network and %d in the Q "+
				"half — an edge occupies a slot whether or not the other end is measured",
				node, whole.heldEdges(node), halfNetwork.heldEdges(node))
		}
	}

	wholeReport, err := whole.Run()
	if err != nil {
		t.Fatalf("running the whole network: %v", err)
	}
	halfReport, err := halfNetwork.Run()
	if err != nil {
		t.Fatalf("running the Q half: %v", err)
	}

	// 2. The CHURN TRACE is one trace: the same nodes left at the same ticks.
	for node := range whole.departedAt {
		if whole.departedAt[node] != halfNetwork.departedAt[node] {
			t.Fatalf("node %d left at tick %d in the whole network and at %d in the Q half — one "+
				"seed must give one event sequence, or the two runs are different experiments",
				node, whole.departedAt[node], halfNetwork.departedAt[node])
		}
	}
	if wholeReport.Departed != halfReport.Departed {
		t.Errorf("%d departures against %d", wholeReport.Departed, halfReport.Departed)
	}

	// 3. And the thing that IS allowed to differ: who is measured.
	if halfReport.Members >= wholeReport.Members || halfReport.Members == 0 {
		t.Fatalf("the Q half holds %d members against %d", halfReport.Members, wholeReport.Members)
	}
	t.Logf("whole: %s\nhalf:  %s", wholeReport.PopulationLine(), halfReport.PopulationLine())
}

// TestM6ReconnectingNeverExceedsTheOwnersBudget is the last of the six, and the
// only one whose consequence is an invariant rather than a number.
//
// Re-establishing a released edge costs the owner a slot. The slot it freed on
// detection may since have gone to a newcomer, so the owner's own ceiling has to
// be checked — and classifyProbe only ever looks at the candidate's side.
func TestM6ReconnectingNeverExceedsTheOwnersBudget(t *testing.T) {
	t.Parallel()

	t.Run("a full owner refuses to re-connect", func(t *testing.T) {
		t.Parallel()

		config := m6ModelBase()
		g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
		network, err := newM6Network(g, config, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}

		// The exact sequence the invariant is about: detect a departure, let the
		// freed slot go elsewhere, then meet the old neighbour again.
		const owner = int32(0)
		state := network.states[owner]
		peer := g.adjacency[owner][0]

		network.online[peer] = false
		network.departedAt[peer] = 0
		network.probe(owner, state, peer, 0, false)
		if _, released := state.Released[peer]; !released {
			t.Fatal("the departure was not detected, so the rest of the fixture means nothing")
		}
		freed := network.heldEdges(owner)

		// Somebody else took the freed slot — modelled directly, because what
		// took it does not matter to the invariant.
		// Fill the owner to its ceiling with connections it does not really have:
		// what took the freed slot does not matter to the invariant.
		for filler := int32(0); network.heldEdges(owner) < config.Shape.budget; filler++ {
			if filler != owner {
				network.held[owner][filler] = struct{}{}
			}
		}
		network.online[peer] = true
		clear(state.TriedThisTick)

		network.probe(owner, state, peer, 0, false)
		if got := network.heldEdges(owner); got != config.Shape.budget {
			t.Errorf("the owner holds %d slots against a ceiling of %d — re-connecting must not "+
				"exceed it", got, config.Shape.budget)
		}
		if network.report.Probes.Outcomes[m6OwnerAtBudget] == 0 {
			t.Error("the refusal was not recorded as the OWNER being at its ceiling — which end " +
				"was full points at a different fix")
		}
		if state.Table.holds(peer) {
			t.Error("the record was stored although the connection could not be re-established")
		}
		t.Logf("freed to %d on detection, refused the re-connect at the ceiling %d",
			freed, config.Shape.budget)
	})

	t.Run("no node ever exceeds B over a whole run", func(t *testing.T) {
		t.Parallel()

		// The same invariant, over a run with every moving part switched on.
		config := m6ModelBase()
		config.Branch = branchC
		config.Churn = churnCompensated
		config.ChurnShare = 0.02
		config.ChurnAt = 1
		config.ReturnShare = 0.5
		config.ReturnAfter = 2
		config.Ticks = 16

		g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
		network, err := newM6Network(g, config, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		if _, err := network.Run(); err != nil {
			t.Fatalf("running: %v", err)
		}

		for _, node := range network.all {
			if held := network.heldEdges(node); held > config.Shape.budget {
				t.Fatalf("node %d holds %d slots against a ceiling of %d",
					node, held, config.Shape.budget)
			}
		}
	})
}

// m6DirectFixture builds a network without running it, for the invariants that
// can only be reached by driving probe() a step at a time.
func m6DirectFixture(t *testing.T, config m6ModelConfig) *m6Network {
	t.Helper()

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	return network
}

// TestM6AShelvedRecordSurvivesAFailedProbe is the shelf's whole reason to exist,
// and it used to be destroyed by the first attempt to use it.
//
// ⚠️ Handing a record out removed it from the shelf. A peer that had simply not
// come back yet therefore lost its record for good: detectLoss returns early for
// a peer already released, so nothing put it back, and branch A and the
// omniscient source both skip released peers. A return inside T_stale could no
// longer be used by anybody — the record was gone before the window it was kept
// for had passed.
func TestM6AShelvedRecordSurvivesAFailedProbe(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.StaleTicks = 16
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
	if level < 0 {
		t.Fatal("the fixture's peer has no level in the owner's table")
	}

	// Fill the slot, then lose it.
	network.probe(owner, state, peer, level, false)
	if !state.Table.holds(peer) {
		t.Fatalf("the fixture could not fill the slot: %s", network.report.Probes)
	}
	network.online[peer] = false
	network.departedAt[peer] = 0
	clear(state.TriedThisTick)
	network.probe(owner, state, peer, level, true)

	if len(state.Shelf) != 1 || state.Shelf[0].Node != peer {
		t.Fatalf("the shelf holds %v after the detection, want the one lost record", state.Shelf)
	}
	detectedAt := state.Shelf[0].DetectedAt

	// A probe from the shelf while the peer is STILL away. The record must
	// survive it.
	network.tick = 2
	clear(state.TriedThisTick)
	candidate, _, ok := network.candidateFor(owner, state, level)
	if !ok || candidate != peer {
		t.Fatalf("the shelf offered %v (ok=%v), want the lost peer %d", candidate, ok, peer)
	}
	network.probe(owner, state, candidate, level, false)

	if len(state.Shelf) != 1 || state.Shelf[0].Node != peer {
		t.Fatalf("an unsuccessful probe emptied the shelf: %v", state.Shelf)
	}
	if state.Shelf[0].DetectedAt != detectedAt {
		t.Errorf("the shelf life restarted at %d, it is counted from the FIRST detection at %d",
			state.Shelf[0].DetectedAt, detectedAt)
	}

	// The peer returns inside T_stale, and now the record can be used.
	network.tick = 4
	network.online[peer] = true
	clear(state.TriedThisTick)
	candidate, _, ok = network.candidateFor(owner, state, level)
	if !ok || candidate != peer {
		t.Fatalf("after the return the shelf offered %v (ok=%v), want %d", candidate, ok, peer)
	}
	network.probe(owner, state, candidate, level, false)

	if !state.Table.holds(peer) {
		t.Fatal("the returned peer was not taken back into the table")
	}
	if len(state.Shelf) != 0 {
		t.Errorf("the shelf still holds %v after the record was used again", state.Shelf)
	}
	if _, released := state.Released[peer]; released {
		t.Error("the peer is still marked as released after the connection was re-established")
	}
}

// TestM6AFullOwnerCannotOpenANewConnection is the budget check applied where it
// belongs: to ANY probe that would open a connection, not only to a re-connect.
//
// ⚠️ It sat under "this peer was released", so a stranger from A′ or C went
// straight through: an owner already at its ceiling verified records it could
// never have opened a connection for.
func TestM6AFullOwnerCannotOpenANewConnection(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// A stranger: online, with room of its own, and not connected to the owner.
	stranger := int32(-1)
	for _, node := range network.all {
		if node == owner || network.holdsEdge(owner, node) || !network.online[node] {
			continue
		}
		if network.heldEdges(node) >= config.Shape.budget {
			continue
		}
		if levelOf(network.ids[owner], network.ids[node], config.Shape.degree) < 0 {
			continue
		}
		stranger = node
		break
	}
	if stranger < 0 {
		t.Fatal("the fixture found no reachable stranger with room")
	}

	// The owner is full. ⚠️ The stranger must NOT be among the fillers, or it
	// would count as a connection the owner already holds and be exempt for the
	// right reason — the fixture would then pass while testing nothing.
	for filler := int32(0); network.heldEdges(owner) < config.Shape.budget; filler++ {
		if filler != owner && filler != stranger {
			network.held[owner][filler] = struct{}{}
		}
	}

	level := levelOf(network.ids[owner], network.ids[stranger], config.Shape.degree)
	network.probe(owner, state, stranger, level, false)

	if state.Table.holds(stranger) {
		t.Error("a full owner stored a record it could not have opened a connection for")
	}
	if network.report.Probes.Outcomes[m6OwnerAtBudget] == 0 {
		t.Error("the refusal was not recorded as the owner being at its ceiling")
	}
	if got := network.heldEdges(owner); got != config.Shape.budget {
		t.Errorf("the owner holds %d slots against a ceiling of %d", got, config.Shape.budget)
	}
}

// TestM6OneCandidateFromTwoNeighboursCostsOneProbeATick closes the last gap in
// the retry model: `spent` guarded the acquaintances and the omniscient source
// and not the records an exchange had handed over, so one node offered by two
// neighbours could spend the ceiling R twice in a tick.
func TestM6OneCandidateFromTwoNeighboursCostsOneProbeATick(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// The same record offered twice, as two neighbours would.
	candidate := int32(-1)
	for _, node := range network.all {
		if node == owner || network.holdsEdge(owner, node) {
			continue
		}
		if levelOf(network.ids[owner], network.ids[node], config.Shape.degree) >= 0 {
			candidate = node
			break
		}
	}
	if candidate < 0 {
		t.Fatal("the fixture found no candidate with a level")
	}
	state.Offered = append(state.Offered, candidate, candidate)

	level := levelOf(network.ids[owner], network.ids[candidate], config.Shape.degree)
	// The queue is consulted only after the node's own acquaintances, so the
	// ones on this level are marked as already tried this tick — otherwise the
	// fixture would be testing branch A.
	for _, peer := range network.neighboursOf(owner) {
		if levelOf(network.ids[owner], network.ids[peer], config.Shape.degree) == level {
			state.TriedThisTick[peer] = struct{}{}
		}
	}

	first, _, ok := network.fromBranch(owner, state, level)
	if !ok || first != candidate {
		t.Fatalf("the queue offered %v (ok=%v), want %d", first, ok, candidate)
	}
	network.probe(owner, state, first, level, false)

	// Within the SAME tick the duplicate must not be handed out again.
	if again, _, ok := network.fromBranch(owner, state, level); ok && again == candidate {
		t.Error("the same candidate was offered twice in one tick — one node offered by two " +
			"neighbours would spend the ceiling R twice on it")
	}

	// ⚠️ And the duplicate is KEPT, not dropped: on a later tick it is a
	// legitimate candidate again.
	held := 0
	for _, offered := range state.Offered {
		if offered == candidate {
			held++
		}
	}
	if held == 0 {
		t.Error("the skipped record was discarded instead of kept for a later tick")
	}
}

// TestM6TheQHalfHoldsUnderCompensatedLoad is the case the earlier Q-half test did
// not reach: with arrivals and departures every tick, the unmeasured half still
// has to behave as it does in the full-graph run, or the two runs differ in the
// physical network as well as in what is measured.
func TestM6TheQHalfHoldsUnderCompensatedLoad(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Churn = churnCompensated
	config.ChurnShare = 0.02
	config.ChurnAt = 1
	config.ReturnShare = 0.5
	config.ReturnAfter = 3
	config.Ticks = 14

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	whole, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing the whole network: %v", err)
	}
	half := config
	half.Membership = "Q half"
	halfNetwork, err := newM6Network(g, half, structural)
	if err != nil {
		t.Fatalf("preparing the Q half: %v", err)
	}

	// The reserve is drawn beyond the population, so both runs get the SAME
	// newcomers — identifiers and roles alike.
	if len(whole.reserve) != len(halfNetwork.reserve) || len(whole.reserve) == 0 {
		t.Fatalf("reserves of %d and %d nodes", len(whole.reserve), len(halfNetwork.reserve))
	}
	for index := range whole.reserve {
		node := whole.reserve[index]
		if halfNetwork.reserve[index] != node || whole.ids[node] != halfNetwork.ids[node] {
			t.Fatalf("the two runs were given different reserves at position %d", index)
		}
	}

	wholeReport, err := whole.Run()
	if err != nil {
		t.Fatalf("running the whole network: %v", err)
	}
	halfReport, err := halfNetwork.Run()
	if err != nil {
		t.Fatalf("running the Q half: %v", err)
	}

	// The exogenous events are one sequence: the same nodes left at the same
	// ticks, and the same arrivals were offered.
	for _, node := range whole.all {
		if whole.departedAt[node] != halfNetwork.departedAt[node] {
			t.Fatalf("node %d left at tick %d in the whole network and at %d in the Q half",
				node, whole.departedAt[node], halfNetwork.departedAt[node])
		}
	}
	if wholeReport.Departed != halfReport.Departed ||
		wholeReport.ArrivalsOffered != halfReport.ArrivalsOffered {
		t.Errorf("departures %d against %d, arrivals offered %d against %d",
			wholeReport.Departed, halfReport.Departed,
			wholeReport.ArrivalsOffered, halfReport.ArrivalsOffered)
	}

	// And the scenario was actually played: an under-provisioned reserve would
	// have made the comparison one between two different churns.
	for _, report := range []*m6ModelReport{wholeReport, halfReport} {
		if report.ArrivalsNotOffered != 0 {
			t.Errorf("%d arrivals could not be offered — the reserve ran out and the run did not "+
				"play the churn it was asked to play", report.ArrivalsNotOffered)
		}
	}
	if wholeReport.ArrivalsOffered == 0 {
		t.Fatal("no arrival was offered at all, so the compensated mode is not exercised")
	}

	// ⚠️ And the property that makes the two runs comparable at all: the
	// UNMEASURED half goes on running the mechanism. A frozen ¬Q half would never
	// detect its own losses and never free its own budget, so the room a host has
	// for a newcomer would differ between the two runs — a physical difference on
	// top of the measured one.
	// ⚠️ Counted over the ORIGINAL population only. An admitted newcomer gets a
	// state wherever it came from, so including the reserve would let the check
	// pass on newcomers alone — which is how it first passed against the very
	// mutation it was written to catch.
	working := 0
	for _, node := range halfNetwork.all {
		if int(node) >= len(g.ids) {
			continue
		}
		if structural(halfNetwork.ids[node]) || halfNetwork.states[node] == nil {
			continue
		}
		if len(halfNetwork.states[node].Released) > 0 ||
			halfNetwork.states[node].Table.Coverage.filled() > 0 {
			working++
		}
	}
	if working == 0 {
		t.Error("not one node outside the Q half kept a table or detected a loss — the unmeasured " +
			"half is frozen, and the two runs then differ in the physical network as well as in " +
			"what is measured")
	}
	t.Logf("%d unmeasured nodes went on running the mechanism", working)
	t.Logf("whole: %s\nhalf:  %s", wholeReport.PopulationLine(), halfReport.PopulationLine())
}

// TestM6SuOnlyCountsWhatWasActuallyOffered is §5.1.1: S(u) is the pool a node was
// GIVEN, not the union of everything its neighbours happen to hold.
func TestM6SuOnlyCountsWhatWasActuallyOffered(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ExchangeRecords = 2 // small m, so the gap is unmissable
	config.Ticks = 8
	report := runM6Model(t, config)

	measured, ok := medianOf(report.PoolByOwner)
	potential, alsoOK := medianOf(report.PoolPotential)
	if !ok || !alsoOK {
		t.Fatal("one of the two pools was not measured")
	}
	if measured > potential {
		t.Errorf("the measured pool (%d) exceeds the potential one (%d) — what was handed over "+
			"cannot be more than what could have been", measured, potential)
	}
	if measured == potential {
		t.Errorf("measured and potential pools are both %d — with m = %d the exchange hands over "+
			"a fraction of a neighbour's table, so counting the whole table as S(u) is exactly "+
			"the error this separation exists for", measured, config.ExchangeRecords)
	}

	rendered := report.PoolSummary()
	for _, want := range []string{"MEASURED from what was actually offered", "ANALYTICAL and NOT S(u)"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the pool summary does not carry %q:\n%s", want, rendered)
		}
	}
	t.Logf("%s", rendered)
}

// TestM6MeasuredAndPhysicalCostsAreKeptApart is the aggregate split.
//
// ⚠️ Every node of the network runs the mechanism, so a ¬Q node's probes are
// real work — but putting them in the same ledger as the Q half's coverage
// prices one population's effort against another's result. The fixture drives a
// single unmeasured node and checks that exactly one of the two aggregates moves.
func TestM6MeasuredAndPhysicalCostsAreKeptApart(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	network, err := newM6Network(g, config, structural)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	// An UNMEASURED node with a candidate to probe.
	outsider := int32(-1)
	for _, node := range network.all {
		if network.measured(node) || network.states[node] == nil {
			continue
		}
		if len(network.neighboursOf(node)) > 0 {
			outsider = node
			break
		}
	}
	if outsider < 0 {
		t.Fatal("the fixture found no unmeasured node with a neighbour")
	}

	before := network.report
	if before.Probes.Probes() != 0 || before.PhysicalProbes.Probes() != 0 {
		t.Fatal("the fixture starts with probes already counted")
	}

	state := network.states[outsider]
	peer := network.neighboursOf(outsider)[0]
	level := levelOf(network.ids[outsider], network.ids[peer], config.Shape.degree)
	network.probe(outsider, state, peer, level, false)

	if network.report.PhysicalProbes.Probes() != 1 {
		t.Errorf("the physical ledger counted %d probes, want 1 — an unmeasured node's work is "+
			"still work", network.report.PhysicalProbes.Probes())
	}
	if network.report.Probes.Probes() != 0 {
		t.Errorf("the MEASURED ledger counted %d probes for a node outside the membership",
			network.report.Probes.Probes())
	}

	// The same for a detection: it belongs to the physical total and not to the
	// measured population's recovery accounting.
	network.online[peer] = false
	network.departedAt[peer] = 0
	clear(state.TriedThisTick)
	network.probe(outsider, state, peer, level, true)

	if network.report.PhysicalDetections == 0 {
		t.Error("an unmeasured node detected a loss and the physical total did not move")
	}
	if len(network.report.DetectionDelays) != 0 {
		t.Errorf("%d detection delays were credited to the measured population for a node "+
			"outside it", len(network.report.DetectionDelays))
	}
	if sumOf(network.report.LostByLevel) != 0 {
		t.Errorf("%d losses were credited to the measured population for a node outside it",
			sumOf(network.report.LostByLevel))
	}
}

// TestM6AnExchangedRecordSurvivesATemporaryRefusal is the queue's half of the
// rule the shelf already follows.
//
// ⚠️ The record cost an exchange. Dropping it because the owner's ceiling
// happened to be full this tick throws away something that was paid for — and
// under the single-exchange control there is no second exchange to get it again.
func TestM6AnExchangedRecordSurvivesATemporaryRefusal(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ExchangeOnce = true // the control where losing a record is unrecoverable
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// A record handed over by an exchange, at a level the owner can use.
	candidate := int32(-1)
	for _, node := range network.all {
		if node == owner || network.holdsEdge(owner, node) || !network.online[node] {
			continue
		}
		// ⚠️ With room of its OWN: the refusal under test is the owner's ceiling,
		// and a candidate at its own would refuse for the other reason and leave
		// the fixture proving nothing.
		if network.heldEdges(node) >= config.Shape.budget {
			continue
		}
		if levelOf(network.ids[owner], network.ids[node], config.Shape.degree) >= 0 {
			candidate = node
			break
		}
	}
	if candidate < 0 {
		t.Fatal("the fixture found no usable candidate with room of its own")
	}
	state.Offered = append(state.Offered, candidate)
	level := levelOf(network.ids[owner], network.ids[candidate], config.Shape.degree)

	// The owner is full, so the probe is refused for a reason that will pass.
	spare := map[int32]struct{}{}
	for peer := range network.held[owner] {
		spare[peer] = struct{}{}
	}
	for filler := int32(0); network.heldEdges(owner) < config.Shape.budget; filler++ {
		if filler != owner && filler != candidate {
			network.held[owner][filler] = struct{}{}
		}
	}

	// ⚠️ The record is taken THROUGH fromBranch, not handed to probe() directly:
	// where the entry leaves the queue is exactly what is under test, and a
	// fixture that bypasses the hand-out cannot see it.
	for _, peer := range network.neighboursOf(owner) {
		if levelOf(network.ids[owner], network.ids[peer], config.Shape.degree) == level {
			state.TriedThisTick[peer] = struct{}{}
		}
	}
	taken, _, ok := network.fromBranch(owner, state, level)
	if !ok || taken != candidate {
		t.Fatalf("the queue offered %v (ok=%v), want %d", taken, ok, candidate)
	}
	network.probe(owner, state, taken, level, false)
	if network.report.Probes.Outcomes[m6OwnerAtBudget] == 0 {
		t.Fatal("the fixture did not produce the temporary refusal it needs")
	}

	held := 0
	for _, offered := range state.Offered {
		if offered == candidate {
			held++
		}
	}
	if held == 0 {
		t.Fatal("a record paid for by an exchange was discarded on a TEMPORARY refusal — the " +
			"ceiling frees on the next tick, and under a single exchange there is no way to get " +
			"it again")
	}

	// The budget frees, and the record is still there to be used.
	network.held[owner] = spare
	network.tick++
	clear(state.TriedThisTick)
	// The queue is consulted after the node's own acquaintances, so the ones on
	// this level are marked as already tried — otherwise the retry would be
	// answered by branch A and the fixture would test nothing.
	for _, peer := range network.neighboursOf(owner) {
		if levelOf(network.ids[owner], network.ids[peer], config.Shape.degree) == level {
			state.TriedThisTick[peer] = struct{}{}
		}
	}
	again, _, ok := network.fromBranch(owner, state, level)
	if !ok || again != candidate {
		t.Fatalf("after the budget freed, the queue offered %v (ok=%v), want %d",
			again, ok, candidate)
	}
	network.probe(owner, state, again, level, false)
	if !state.Table.holds(candidate) {
		t.Fatalf("the record was not used on the retry: %s", network.report.Probes.breakdown())
	}

	// And NOW it leaves the queue: the outcome is final.
	for _, offered := range state.Offered {
		if offered == candidate {
			t.Error("the record stayed in the queue after it was stored in the table")
		}
	}
}

// TestM6ReconnectingRestoresBothSides is the other half of "a connection is
// re-established": if both ends had released it, both have to take the slot back.
//
// ⚠️ Otherwise the candidate's slot stays free while the owner counts the link as
// restored — and the candidate can hand that same slot to a newcomer.
func TestM6ReconnectingRestoresBothSides(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)

	// Both ends detect each other gone — which happens whenever two nodes are
	// offline at different moments and each probes the other.
	network.probe(owner, state, peer, level, false)
	network.online[peer] = false
	network.departedAt[peer] = 0
	clear(state.TriedThisTick)
	network.probe(owner, state, peer, level, true)

	peerState := network.states[peer]
	delete(network.held[peer], owner)
	peerState.Released[owner] = struct{}{}

	if network.holdsEdge(owner, peer) || network.holdsEdge(peer, owner) {
		t.Fatal("the fixture did not get both ends to release the link")
	}
	ownerBefore, peerBefore := network.heldEdges(owner), network.heldEdges(peer)

	// The peer returns and the owner re-establishes the connection.
	network.online[peer] = true
	network.tick++
	clear(state.TriedThisTick)
	network.probe(owner, state, peer, level, false)

	if !network.holdsEdge(owner, peer) {
		t.Fatalf("the owner did not restore the link: %s", network.report.Probes.breakdown())
	}
	if !network.holdsEdge(peer, owner) {
		t.Error("only the initiator's side was restored — the peer's slot is still free, and it " +
			"can hand the same slot to a newcomer while the owner counts the link as back")
	}
	if got := network.heldEdges(owner); got != ownerBefore+1 {
		t.Errorf("the owner holds %d slots, was %d", got, ownerBefore)
	}
	if got := network.heldEdges(peer); got != peerBefore+1 {
		t.Errorf("the peer holds %d slots, was %d", got, peerBefore)
	}
}

// TestM6TheDenominatorIsTheOriginalPopulation: the reserve is sized for the
// length of the run, so counting it among "all participants" made the share of
// the network that is online depend on how long the run was asked to be.
func TestM6TheDenominatorIsTheOriginalPopulation(t *testing.T) {
	t.Parallel()

	short := m6ModelBase()
	short.Churn = churnCompensated
	short.ChurnShare = 0.02
	short.ChurnAt = 1
	short.Ticks = 8

	long := short
	long.Ticks = 24 // a bigger reserve, and nothing else

	first := runM6Model(t, short)
	second := runM6Model(t, long)

	if first.ReserveSize >= second.ReserveSize {
		t.Fatalf("reserves of %d and %d — the longer run must provision more, or this fixture "+
			"does not vary what it claims to", first.ReserveSize, second.ReserveSize)
	}
	if first.Members != second.Members {
		t.Errorf("the original population counts %d members in one run and %d in the other — the "+
			"denominator moved with the RESERVE, which is sized for the ticks",
			first.Members, second.Members)
	}
	if first.Members != short.Shape.nodes {
		t.Errorf("the measured population is %d of a %d-node network, and the membership is "+
			"everybody", first.Members, short.Shape.nodes)
	}
	if !strings.Contains(first.PopulationLine(), "admitted from a reserve of") {
		t.Errorf("the population line does not separate the reserve:\n%s", first.PopulationLine())
	}
}

// TestM6TheReserveNeverRepeatsAnIdentifier is the last of the five.
//
// ⚠️ Continuing the generator's index sequence past N is NOT enough: a population
// built by rejection — M3-a draws a skewed share that way — walks the generator
// far past N to fill itself, so indices above N are already in the graph. The
// reserve would then re-introduce an existing NodeID as a separate node, and two
// nodes would share one identity.
func TestM6TheReserveNeverRepeatsAnIdentifier(t *testing.T) {
	t.Parallel()

	sh := shape{name: "600×6", nodes: 600, degree: 6, budget: 12}
	config := m6ModelBase()
	config.Shape = sh
	config.Churn = churnCompensated
	config.ChurnShare = 0.05
	config.Ticks = 12

	// A population drawn by REJECTION at a strong skew: exactly the case where
	// the generator was walked far past N.
	people, err := drawSkewedPopulation(sh, config.Seed, 55, 0.1)
	if err != nil {
		t.Fatalf("drawing the population: %v", err)
	}
	if people.Drawn <= sh.nodes {
		t.Fatalf("the draw examined %d candidates for %d nodes — without rejection this fixture "+
			"cannot show the collision it is about", people.Drawn, sh.nodes)
	}

	g := buildGraphOnIDs(people.IDs, sh, config.Quota, config.Policy, nil, nil)
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	if len(network.reserve) == 0 {
		t.Fatal("no reserve was drawn")
	}

	seen := make(map[nodeID]int32, len(network.ids))
	for _, node := range network.all {
		id := network.ids[node]
		if first, already := seen[id]; already {
			t.Fatalf("nodes %d and %d share one NodeID — the reserve re-introduced an identifier "+
				"the graph already held", first, node)
		}
		seen[id] = node
	}
	t.Logf("%d nodes, %d of them reserve, all identifiers distinct (%d candidates were examined "+
		"to draw the population)", len(network.all), len(network.reserve), people.Drawn)
}

// TestM6ARestoredLinkCanBeLostAgain is the full cycle П-4 has to survive more
// than once.
//
// ⚠️ A re-connect used to clear the detected-loss record at the INITIATOR only.
// The other end went on believing it had already buried this peer, so when the
// peer left AGAIN its detectLoss returned early: no budget freed, no loss
// counted. One stale burial switched detection off for that pair for the rest of
// the run — and nothing in the report would have said so.
func TestM6ARestoredLinkCanBeLostAgain(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]
	peerState := network.states[peer]
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
	peerLevel := levelOf(network.ids[peer], network.ids[owner], config.Shape.degree)

	// Both hold each other, then both detect the other gone.
	network.probe(owner, state, peer, level, false)
	network.probe(peer, peerState, owner, peerLevel, false)

	network.online[peer] = false
	network.departedAt[peer] = 0
	clear(state.TriedThisTick)
	network.probe(owner, state, peer, level, true)

	delete(network.held[peer], owner)
	peerState.Released[owner] = struct{}{}

	// The peer returns and the owner re-establishes the link.
	network.online[peer] = true
	network.tick = 2
	clear(state.TriedThisTick)
	network.probe(owner, state, peer, level, false)
	if !network.holdsEdge(peer, owner) {
		t.Fatalf("the link was not restored at the peer's end: %s",
			network.report.Probes.breakdown())
	}
	if _, stale := peerState.Released[owner]; stale {
		t.Fatal("the peer still has this node marked as already lost — its next detection would " +
			"return early, free nothing and count nothing")
	}

	// ⚠️ THE PART THAT WAS MISSING: the owner leaves again, and the peer has to
	// be able to detect it a SECOND time.
	heldBefore := network.heldEdges(peer)
	lostBefore := sumOf(network.report.LostByLevel)
	network.online[owner] = false
	network.departedAt[owner] = 3
	network.tick = 4
	clear(peerState.TriedThisTick)
	network.probe(peer, peerState, owner, peerLevel, true)

	if _, detected := peerState.Released[owner]; !detected {
		t.Error("the peer did not detect the second departure at all")
	}
	if got := network.heldEdges(peer); got != heldBefore-1 {
		t.Errorf("the peer holds %d slots, was %d — the budget of the second loss was never freed",
			got, heldBefore)
	}
	if sumOf(network.report.LostByLevel) == lostBefore {
		t.Error("the second loss was not counted")
	}
}

// TestM6ReturningIsDrawnPerDepartureAndNotPerIdentity is the churn model's own
// honesty check.
//
// ⚠️ Keying the coin on the node made it PERMANENT: one identity always came
// back and another never did. Under repeated churn the never-returning ones
// disappear and the survivors are steadily enriched with returners, so `ret` stops
// describing the share of DEPARTURES that return and starts describing a standing
// property of identities — a different assumption, and not the one the contract
// makes.
func TestM6ReturningIsDrawnPerDepartureAndNotPerIdentity(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnShrink // repeated departures, nobody admitted back in
	config.ChurnShare = 0.05
	config.ChurnAt = 0
	config.ReturnShare = 0.5
	config.ReturnAfter = 2
	config.Ticks = 24

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	// Count how often each node was scheduled to return, and how often it left.
	departures := map[int32]int{}
	returns := map[int32]int{}
	for tick := range config.Ticks {
		network.tick = tick
		before := map[int32]bool{}
		for _, node := range network.all {
			before[node] = network.online[node]
		}
		if err := network.applyChurn(); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		for _, node := range network.all {
			if before[node] && !network.online[node] {
				departures[node]++
			}
		}
		for _, node := range network.returning[tick+config.ReturnAfter] {
			returns[node]++
		}
		// Put the returns back by hand: the point here is the DRAW, not the
		// entry queue.
		for _, node := range network.returning[tick] {
			network.online[node] = true
		}
	}

	repeated, mixed := 0, 0
	for node, left := range departures {
		if left < 2 {
			continue
		}
		repeated++
		if back := returns[node]; back > 0 && back < left {
			mixed++
		}
	}
	if repeated == 0 {
		t.Fatal("no node departed twice, so this fixture cannot tell a per-departure draw from a " +
			"per-identity one")
	}
	if mixed == 0 {
		t.Errorf("of %d nodes that departed more than once, not one returned from some departures "+
			"and not from others — the coin is a property of the IDENTITY, and `ret` then stops "+
			"describing the share of departures that return", repeated)
	}
	t.Logf("%d nodes departed more than once; %d of them returned from some departures and not "+
		"others", repeated, mixed)
}

// TestM6BackgroundChurnDecisionsAreExogenous is the answer to "the same seed
// still does not give the same trace".
//
// ⚠️ It cannot be "the realised departures are identical" — they are not, and
// claiming so would be false: a node that is offline when its turn comes does not
// leave. What IS identical is the sequence of DECISIONS, because each is drawn
// from (tick, node) alone. The fixture forces the two runs apart on purpose — one
// of them loses a node early — and then checks that the decisions still match
// while the realised churn is allowed to differ.
func TestM6BackgroundChurnDecisionsAreExogenous(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnShrink
	config.ChurnShare = 0.03
	config.ChurnAt = 0
	config.Ticks = 12

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)

	first, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	second, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	// ⚠️ The divergence, introduced deliberately: one run starts with a node
	// already gone, exactly as a differing admission would leave it.
	second.online[7] = false

	firstReport, err := first.Run()
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	secondReport, err := second.Run()
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if firstReport.DeparturesDecided != secondReport.DeparturesDecided {
		t.Errorf("%d departure decisions against %d — the decisions are drawn from (tick, node) "+
			"and must not depend on who happened to be online",
			firstReport.DeparturesDecided, secondReport.DeparturesDecided)
	}
	if firstReport.DeparturesDecided == 0 {
		t.Fatal("no departure was decided, so this fixture measures nothing")
	}
	// And the realised churn is ALLOWED to differ — that is the conditional part,
	// and it is reported as its own number rather than hidden.
	t.Logf("decisions %d in both runs; realised %d and %d",
		firstReport.DeparturesDecided, firstReport.Departed, secondReport.Departed)
}

// TestM6OneResponderIsAskedOnce covers the duplicate in the responder list.
func TestM6OneResponderIsAskedOnce(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchC
	config.RatePair = 1
	config.RateNode = 1
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]

	// The same node is both a neighbour AND a record in the table — the ordinary
	// case, since a node usually stores the peers it is connected to.
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
	state.Table.members[level][peer] = struct{}{}
	state.Table.Coverage.Held[level]++

	known := network.knownTo(owner, state)
	seen := map[int32]int{}
	for _, node := range known {
		seen[node]++
		if seen[node] > 1 {
			t.Fatalf("node %d appears %d times in the responder list — its rate limit would be "+
				"charged once per copy, and the load a limit sheds would depend on how the asker "+
				"STORES its knowledge", node, seen[node])
		}
	}
	if seen[peer] != 1 {
		t.Fatalf("the peer that is both a neighbour and a record appears %d times", seen[peer])
	}
}

// TestM6OfflineOwnersAreMemoryNotCoverage: a departed node's table may point at
// perfectly live peers, and counting it as coverage mixes what the working
// network can route through with what an absent node happens to remember.
func TestM6OfflineOwnersAreMemoryNotCoverage(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// Fill the owner's table with peers that are and stay ONLINE.
	filled := 0
	for _, peer := range network.neighboursOf(owner) {
		level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
		if level < 0 {
			continue
		}
		network.probe(owner, state, peer, level, false)
		if state.Table.holds(peer) {
			filled++
		}
	}
	if filled == 0 {
		t.Fatal("the fixture filled no slot")
	}

	before := network.coverageByLevel()
	claimedBefore, retainedBefore := 0, 0
	for _, level := range before {
		claimedBefore += level.Claimed
		retainedBefore += level.RetainedOffline
	}

	// ⚠️ The OWNER leaves; its records still point at live nodes.
	network.online[owner] = false
	after := network.coverageByLevel()
	claimedAfter, retainedAfter := 0, 0
	for _, level := range after {
		claimedAfter += level.Claimed
		retainedAfter += level.RetainedOffline
	}

	if claimedAfter != claimedBefore-filled {
		t.Errorf("claimed coverage went from %d to %d after the OWNER left, expected it to drop "+
			"by its %d records — an offline node's table is memory, not coverage",
			claimedBefore, claimedAfter, filled)
	}
	if retainedAfter != retainedBefore+filled {
		t.Errorf("retained-offline went from %d to %d, expected it to gain the owner's %d records",
			retainedBefore, retainedAfter, filled)
	}
	if !strings.Contains(coverageLineOf(after, "x"), "memory, NOT coverage") {
		t.Errorf("the coverage line does not separate the two:\n%s", coverageLineOf(after, "x"))
	}
}

// TestM6EveryInvariantHoldsAfterAFullRun is the sweep across the whole model, and
// it exists because of how the last three rounds of defects were found.
//
// ⚠️ Every one of them was in a place that a PREVIOUS edit had changed but not
// walked: the budget counter beside the adjacency, the aggregates after the
// mechanism moved to every node, the queue after the retry rule, the second end
// after the re-connect. Each was caught by writing a new fixture aimed at the
// specific hole — which only works if somebody guesses the right hole.
//
// This test guesses nothing. It runs the model with every moving part switched on
// and then asserts the consistencies that must hold NO MATTER WHICH of them moved:
// a change that breaks one of them fails here without anybody having to know in
// advance where to look. It replaces none of the targeted fixtures — it catches
// the class they each catch one instance of.
func TestM6EveryInvariantHoldsAfterAFullRun(t *testing.T) {
	t.Parallel()

	// ⚠️ THE CHURN FORM IS A DIMENSION OF THE SWEEP, not a fixed setting. It ran
	// on churnCompensated alone, so every property that the shock and the shrink
	// reach by a different path went unchecked — and one of them was wrong: the
	// shock filled `leaving` without booking a single decision, so its report
	// said "0 departures decided … 200 actually left". A sweep that fixes one of
	// the model's switches is a sweep with a blind side.
	// ⚠️ And the SCHEDULE is a dimension too, for the same reason: the phased
	// schedule of §5.9.1 decides the onset and the length from the data, and a
	// sweep pinned to the flat schedule would never see a boundary decision
	// interact with the mechanism.
	type setup struct {
		branch m6Branch
		churn  m6ChurnForm
		phased bool
	}
	var setups []setup
	for _, branch := range []m6Branch{branchA, branchB, branchAPrime, branchC} {
		for _, churn := range []m6ChurnForm{churnShock, churnShrink, churnCompensated} {
			setups = append(setups, setup{branch, churn, false}, setup{branch, churn, true})
		}
	}

	for _, each := range setups {
		branch, churnForm, phased := each.branch, each.churn, each.phased
		schedule := "flat"
		if phased {
			schedule = "phased"
		}
		t.Run(fmt.Sprintf("%s/%s/%s", branch, churnForm, schedule), func(t *testing.T) {
			t.Parallel()

			config := m6ModelBase()
			config.Branch = branch
			config.Churn = churnForm
			config.ChurnShare = 0.02
			config.ChurnAt = 1
			config.ReturnShare = 0.5
			config.ReturnAfter = 3
			config.Ticks = 18
			if phased {
				config.Ticks, config.ChurnAt = 0, 0
				config.Phases = &m6PhasePlan{FillTicks: 6, IdleTicks: 3, RecoveryTicks: 6, CadenceTicks: 6}
			}

			g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
			structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
			network, err := newM6Network(g, config, structural)
			if err != nil {
				t.Fatalf("preparing: %v", err)
			}
			report, err := network.Run()
			if err != nil {
				t.Fatalf("running: %v", err)
			}

			budget := config.Shape.budget
			for _, node := range network.all {
				held := network.heldEdges(node)

				// 1. The ceiling, at every node, whatever opened or closed a
				//    connection.
				if held > budget {
					t.Fatalf("node %d holds %d connections against a ceiling of %d",
						node, held, budget)
				}

				state := network.states[node]
				if state == nil {
					continue
				}

				for peer := range network.held[node] {
					// 2. A link is symmetric UNLESS the other end released it —
					//    a release happens at the end that detected the loss.
					if !network.holdsEdge(peer, node) {
						if other := network.states[peer]; other != nil {
							if _, dropped := other.Released[node]; dropped {
								continue
							}
						}
						t.Fatalf("node %d holds a link to %d, %d does not hold it back, and %d "+
							"never released it", node, peer, peer, peer)
					}
					// 3. Held and released are disjoint: a node cannot both hold
					//    a link and have buried it.
					if _, dropped := state.Released[peer]; dropped {
						t.Fatalf("node %d both holds %d and has it marked as lost", node, peer)
					}
				}

				// 4. Every record sits in the bucket its identifier belongs to.
				for level, members := range state.Table.members {
					if state.Table.Coverage.Held[level] != len(members) {
						t.Fatalf("node %d level %d: the counter says %d and the level holds %d",
							node, level, state.Table.Coverage.Held[level], len(members))
					}
					if len(members) > config.Capacity {
						t.Fatalf("node %d level %d holds %d records against a capacity of %d",
							node, level, len(members), config.Capacity)
					}
					for member := range members {
						actual := levelOf(network.ids[node], network.ids[member],
							config.Shape.degree)
						if actual != level {
							t.Fatalf("node %d keeps %d at level %d; it belongs at %d",
								node, member, level, actual)
						}
					}
				}

				// 5. The queue holds nothing PERMANENTLY unusable, and never the
				//    owner itself.
				//
				//    ⚠️ It may well hold a record the table already has: §5.1.0
				//    sends no exclusion list, so a neighbour can hand back what we
				//    already know, and the repeat costs a probe and lands in
				//    "already known". That is the price of not disclosing our own
				//    table, and asserting the queue is free of such records would
				//    be asserting the opposite of the contract.
				for _, offered := range state.Offered {
					if offered == node {
						t.Fatalf("node %d has itself in its offer queue", node)
					}
					if _, never := state.Exhausted[offered]; never {
						t.Fatalf("node %d keeps %d in the queue although no bucket can ever hold it",
							node, offered)
					}
				}

				// 6. Nothing sits on the shelf past T_stale, and everything on it
				//    is a record this node actually buried.
				//
				//    ⚠️ ONLINE owners only. An offline node does nothing at all,
				//    expiry included; its shelf is swept on the tick it comes
				//    back. Asserting it for everybody would be asserting that an
				//    absent node keeps working.
				for _, shelved := range state.Shelf {
					if network.online[node] && network.tick-shelved.DetectedAt > config.StaleTicks {
						t.Fatalf("node %d keeps %d on the shelf %d ticks after detecting it, "+
							"T_stale is %d", node, shelved.Node,
							network.tick-shelved.DetectedAt, config.StaleTicks)
					}
					if _, dropped := state.Released[shelved.Node]; !dropped {
						t.Fatalf("node %d shelved %d without having released it",
							node, shelved.Node)
					}
				}

				// 7. Recovery cannot exceed what was lost, per level.
				for level := range state.LostByLevel {
					if state.RefilledByLevel[level] > state.LostByLevel[level] {
						t.Fatalf("node %d level %d: %d refills against %d losses",
							node, level, state.RefilledByLevel[level], state.LostByLevel[level])
					}
				}
			}

			// 8. The measured aggregates are a SUBSET of the physical ones. This
			//    is the invariant the aggregate split exists for, and it holds
			//    however the two are counted.
			if report.Probes.Probes() > report.PhysicalProbes.Probes() {
				t.Errorf("the measured population spent %d probes and the whole network %d",
					report.Probes.Probes(), report.PhysicalProbes.Probes())
			}
			if len(report.DetectionDelays) > report.PhysicalDetections {
				t.Errorf("%d measured detections against %d in the whole network",
					len(report.DetectionDelays), report.PhysicalDetections)
			}
			if sumOf(report.LostByLevel) > report.PhysicalLost {
				t.Errorf("%d measured losses against %d in the whole network",
					sumOf(report.LostByLevel), report.PhysicalLost)
			}

			// 9. The population arithmetic: nobody is admitted who was not
			//    offered, and nobody both joins and gives up.
			if report.ArrivalsAdmitted+report.GaveUpJoining > report.ArrivalsOffered {
				t.Errorf("%d admitted plus %d gave up, from %d offered",
					report.ArrivalsAdmitted, report.GaveUpJoining, report.ArrivalsOffered)
			}
			// ⚠️ Against the SUM: a joined newcomer's departure is realised load
			// but its decision was drawn over the reserve, so the headline figure
			// alone is the wrong bound.
			decided := report.DeparturesDecided + report.DeparturesDecidedInReserve
			if report.Departed > decided {
				t.Errorf("%d nodes left against %d decisions to leave (%d over the original "+
					"population, %d over the reserve)", report.Departed, decided,
					report.DeparturesDecided, report.DeparturesDecidedInReserve)
			}

			// 10. Coverage adds up: every record of a measured node is either
			//     coverage (its owner is online) or memory (its owner is not).
			records := 0
			for _, owner := range network.owners {
				if state := network.states[owner]; state != nil {
					records += state.Table.Coverage.filled()
				}
			}
			counted := 0
			for _, level := range report.Levels {
				counted += level.Claimed + level.RetainedOffline
				if level.Actual > level.Claimed {
					t.Errorf("a level reports %d alive of %d claimed", level.Actual, level.Claimed)
				}
			}
			if counted != records {
				t.Errorf("the coverage report accounts for %d records, the measured tables hold %d",
					counted, records)
			}
		})
	}
}

// TestM6WithNoCadenceARepeatStillDetects pins the exact semantics of the
// negative control, because the obvious wording of it is false.
//
// ⚠️ `C = ∞` switches off the SCHEDULED REFRESH. It does not, and cannot, switch
// off detection: П-4 says a loss is found by an unsuccessful probe, and a probe
// is a probe whatever prompted it. Branch A never re-probes a record it already
// holds, so there the control does look like "held records are never
// re-checked" — but A′ and C hand back records the owner already has (§5.1.0
// sends no exclusion list, and the repeat costs a probe), and such a repeat finds
// a dead record exactly as a refresh would.
//
// The claim in the contract is therefore branch-dependent, and this fixture is
// what keeps the two readings honest.
func TestM6WithNoCadenceARepeatStillDetects(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Cadence = 0 // ∞ — the negative control
	config.Capacity = 4
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
	if level < 0 {
		t.Fatal("the fixture's peer has no level")
	}

	network.probe(owner, state, peer, level, false)
	if !state.Table.holds(peer) {
		t.Fatalf("the fixture could not fill the slot: %s", network.report.Probes.breakdown())
	}
	if state.Table.Coverage.Held[level] >= config.Capacity {
		t.Fatal("the fixture needs a free slot at that level, so the repeat path is reachable")
	}

	// The peer dies. With no cadence nothing will ever refresh it...
	network.online[peer] = false
	network.departedAt[peer] = 0
	network.tick = 1
	clear(state.TriedThisTick)

	if _, _, due := network.levelDueForRefresh(state); due {
		t.Fatal("a refresh came due with the cadence switched off")
	}

	// ...but a neighbour hands the same record back, and that repeat is a probe.
	state.Offered = append(state.Offered, peer)
	for _, other := range network.neighboursOf(owner) {
		if levelOf(network.ids[owner], network.ids[other], config.Shape.degree) == level {
			state.TriedThisTick[other] = struct{}{}
		}
	}
	delete(state.TriedThisTick, peer)

	candidate, _, ok := network.fromBranch(owner, state, level)
	if !ok || candidate != peer {
		t.Fatalf("the queue offered %v (ok=%v), want the repeat %d", candidate, ok, peer)
	}
	network.probe(owner, state, candidate, level, false)

	if _, detected := state.Released[peer]; !detected {
		t.Fatal("the repeat did not detect the dead record — a probe is a probe whatever " +
			"prompted it, and П-4 has no other way of finding a loss")
	}
	if sumOf(network.report.LostByLevel) == 0 {
		t.Error("the loss was not counted with the cadence off; the control switches off the " +
			"SCHEDULED refresh, not detection")
	}
	t.Logf("with C = ∞ a repeat still detected: %s", network.report.Probes.breakdown())
}

// TestM6AReturnIsHonouredEvenWithoutADeparture is the compensation arithmetic
// stated honestly.
//
// ⚠️ A return is a promise made when the node LEFT. A tick can have no departures
// and a return that has come due, and cancelling it would mean a node that said it
// was coming back never does. So the arrivals offered are max(departures, returns
// due) rather than the departures — which is NOT the exact compensation §5.9.2
// describes, and the report says so instead of summing the two into a number that
// reads as exact.
func TestM6AReturnIsHonouredEvenWithoutADeparture(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnCompensated
	config.ChurnShare = 0.01
	config.ChurnAt = 0
	config.ReturnShare = 1 // every departure promises a return
	config.ReturnAfter = 3
	config.Ticks = 8
	network := m6DirectFixture(t, config)

	// A return that has come due on a tick with NO departures: arranged directly,
	// because the point is the arithmetic and not how the queue got there.
	network.config.ChurnShare = 0
	network.tick = 4
	network.returning[4] = []int32{network.all[1], network.all[2]}

	before := *network.report
	if err := network.applyChurn(); err != nil {
		t.Fatalf("applying churn: %v", err)
	}

	if got := network.report.Departed - before.Departed; got != 0 {
		t.Fatalf("%d departures on a tick with none configured", got)
	}
	if got := network.report.ReturnsOffered - before.ReturnsOffered; got != 2 {
		t.Errorf("%d returns offered, want 2 — a return is honoured whether or not the quota has "+
			"room for it", got)
	}
	if got := network.report.NewcomersOffered - before.NewcomersOffered; got != 0 {
		t.Errorf("%d newcomers offered on a tick with no departures", got)
	}

	// The tick offered two arrivals against no departure: that is the surplus.
	if got := network.report.OfferedSurplus - before.OfferedSurplus; got != 2 {
		t.Errorf("the surplus of offers over departures grew by %d, want 2", got)
	}

	// And the report names the rule (decision 3.1(a)) and does not present the
	// total as exact compensation.
	line := network.report.PopulationLine()
	for _, want := range []string{
		"returns honoured as promised at their departure, offered UNCONDITIONALLY",
		"max(departures − returns due, 0)",
		"NOT exact compensation is claimed",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the population line does not carry %q:\n%s", want, line)
		}
	}
}

// TestM6TheFirstRefreshWaitsForTheCadence: a cadence whose first period is zero
// is not the cadence being swept.
//
// ⚠️ "Never refreshed" used to mean "due now", so every level got one unplanned
// refresh the moment it was filled — identical for C = 64 and C = 256. Both
// branches of the sweep then began with the same free probe and the same dent in
// the filling budget, and the difference between them started one refresh late.
func TestM6TheFirstRefreshWaitsForTheCadence(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Cadence = 6
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	peer := network.neighboursOf(owner)[0]
	level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)

	network.tick = 0
	network.probe(owner, state, peer, level, false)
	if !state.Table.holds(peer) {
		t.Fatalf("the fixture could not fill the slot: %s", network.report.Probes.breakdown())
	}

	// Not due in the tick it was filled, nor at any tick before C.
	for tick := range config.Cadence {
		network.tick = tick
		if got, _, due := network.levelDueForRefresh(state); due && got == level {
			t.Fatalf("level %d came due for a refresh at tick %d, %d ticks after it was filled, "+
				"with C = %d", level, tick, tick, config.Cadence)
		}
	}

	// And due exactly at C.
	network.tick = config.Cadence
	got, _, due := network.levelDueForRefresh(state)
	if !due || got != level {
		t.Errorf("at tick %d the level is not due (due=%v, level=%d), C = %d",
			network.tick, due, got, config.Cadence)
	}
}

// TestM6TheReturnDecisionIsExogenous is the other half of "the same seed gives
// the same load".
//
// ⚠️ The decision to leave was already keyed on (tick, node). The decision to
// RETURN was not: it was drawn from the running count of realised departures, and
// that counter skips events that did not happen. So as soon as two runs differed
// in one admission, the very same departure of the very same node at the very
// same tick drew a different coin in each — the randomness became a function of
// the mechanism under comparison, which is the one thing a load must not be.
//
// The fixture makes exactly that happen: one run has a node already gone, so an
// early departure decision is skipped there and the counters diverge. Every
// departure the two runs SHARE must still agree about returning.
func TestM6TheReturnDecisionIsExogenous(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnShrink
	config.ChurnShare = 0.04
	config.ChurnAt = 0
	config.ReturnShare = 0.5
	config.ReturnAfter = 3
	config.Ticks = 10

	play := func(skip int) (departed map[int32]int, returning map[int32]bool) {
		network := m6DirectFixture(t, config)
		// ⚠️ The divergence, introduced on purpose: these nodes are already gone,
		// so their departure decisions are skipped here and not in the other run.
		// Enough of them that at least one decision is certain to be skipped —
		// one node would only diverge the runs about a third of the time, and a
		// fixture that fails to diverge proves nothing either way.
		for node := range skip {
			network.online[int32(node)] = false
		}
		departed = map[int32]int{}
		returning = map[int32]bool{}

		for tick := range config.Ticks {
			network.tick = tick
			before := map[int32]bool{}
			for _, node := range network.all {
				before[node] = network.online[node]
			}
			if err := network.applyChurn(); err != nil {
				t.Fatalf("tick %d: %v", tick, err)
			}
			for _, node := range network.all {
				if before[node] && !network.online[node] {
					departed[node] = tick
				}
			}
			for _, scheduled := range network.returning[tick+config.ReturnAfter] {
				returning[scheduled] = true
			}
		}
		return departed, returning
	}

	first, firstReturns := play(0)
	second, secondReturns := play(50)

	shared, disagreed := 0, 0
	for node, tick := range first {
		// ⚠️ ASK WHETHER THE EVENT HAPPENED, not what tick is stored. A missing
		// key reads as tick 0, so `second[node] != tick` silently accepted every
		// node that departed at tick 0 in one run and never departed in the other
		// — precisely the nodes this fixture switches off to force the divergence.
		other, departedThere := second[node]
		if !departedThere || other != tick {
			continue // not the same event; nothing to compare
		}
		shared++
		if firstReturns[node] != secondReturns[node] {
			disagreed++
			if disagreed <= 3 {
				t.Errorf("node %d left at tick %d in both runs, and the return decision differs "+
					"(%v against %v) — the coin is keyed on something the runs do not share",
					node, tick, firstReturns[node], secondReturns[node])
			}
		}
	}
	if shared == 0 {
		t.Fatal("the two runs share no departure event, so this fixture compares nothing")
	}
	if len(first) == len(second) {
		t.Fatalf("both runs realised %d departures — the fixture failed to make them diverge, "+
			"and the defect it guards only appears once they do", len(first))
	}
	t.Logf("%d shared departure events, %d disagreements; realised departures %d against %d",
		shared, disagreed, len(first), len(second))
}

// TestM6AnUnreachableStrangerIsNotALostEdge is the exact sequence the review
// named: A′ hands over an unknown candidate, the candidate is offline, the probe
// fails, the candidate comes back and the next probe succeeds.
//
// ⚠️ THE FAILED PROBE USED TO CREATE A CONNECTION OUT OF NOTHING. detectLoss
// marked every unreachable candidate `Released`, whether or not the owner had
// ever held an edge to it, and `probe` reads that flag as "this edge was mine
// and I gave it up". The second probe therefore took the re-connect branch:
// permanent edges at both ends, two slots of B spent, and a ShelfHit for a
// record that had never been on a shelf. Nothing in the run distinguished such
// an invented connection from an established one afterwards.
func TestM6AnUnreachableStrangerIsNotALostEdge(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// A genuine STRANGER: no edge in either direction, no record, and room at
	// both ends so the probe turns on reachability alone.
	stranger := int32(-1)
	for _, node := range network.all {
		if node == owner || network.holdsEdge(owner, node) || network.holdsEdge(node, owner) {
			continue
		}
		if state.Table.holds(node) || network.heldEdges(node) >= config.Shape.budget {
			continue
		}
		if levelOf(network.ids[owner], network.ids[node], config.Shape.degree) >= 0 {
			stranger = node
			break
		}
	}
	if stranger < 0 {
		t.Fatal("the fixture found no stranger with room at both ends")
	}
	level := levelOf(network.ids[owner], network.ids[stranger], config.Shape.degree)
	state.Offered = append(state.Offered, stranger)

	network.online[stranger] = false
	network.probe(owner, state, stranger, level, false)

	if _, released := state.Released[stranger]; released {
		t.Fatal("an unreachable STRANGER was recorded as a released edge — the owner never held " +
			"one, so there was nothing to release")
	}
	if len(state.Shelf) != 0 {
		t.Fatalf("the failed probe shelved %d records; a stranger was never in the table, so "+
			"there is no record to keep", len(state.Shelf))
	}
	for _, lost := range state.LostByLevel {
		if lost != 0 {
			t.Fatal("the failed probe counted a loss; nothing was held, so nothing was lost")
		}
	}
	if network.report.StrangersUnreachable != 1 {
		t.Fatalf("the report counted %d unreachable strangers, want 1 — the outcome has to be "+
			"visible as itself and not folded into the losses",
			network.report.StrangersUnreachable)
	}

	// The candidate comes back and is probed again, in a later tick.
	network.online[stranger] = true
	network.tick++
	state.TriedThisTick = map[int32]struct{}{}
	network.probe(owner, state, stranger, level, false)

	if !state.Table.holds(stranger) {
		t.Fatal("the successful probe stored no record — the fixture measured something else")
	}
	if network.holdsEdge(owner, stranger) || network.holdsEdge(stranger, owner) {
		t.Fatal("a verified RECORD became a permanent CONNECTION: the probe restored an edge " +
			"that had never existed, and both ends now spend a slot of B on it")
	}
	if network.report.ShelfHits != 0 {
		t.Fatalf("%d shelf hits for a record that was never shelved", network.report.ShelfHits)
	}
}

// TestM6TheEventStreamDoesNotDependOnTheRunLength keys churn on the tick and the
// identity, and on nothing else.
//
// ⚠️ The key used to be `tick*len(nodes)+node`, and the node array carries the
// newcomer reserve, which is sized for the run's LENGTH. Two runs of the same
// graph and the same seed that differed only in how long they lasted therefore
// drew different coins for the same node in the same tick — the shared prefix of
// a long run and a short one was not the same experiment, so nothing measured
// across durations could be compared.
func TestM6TheEventStreamDoesNotDependOnTheRunLength(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	base.Churn = churnCompensated // the reserve exists, and its size follows Ticks
	base.ChurnShare = 0.05
	base.ChurnAt = 0
	base.ReturnShare = 0.5
	base.ReturnAfter = 3

	const window = 6
	play := func(ticks int) (
		departed map[nodeID]int, returns map[nodeID]bool, reserve int, report *m6ModelReport,
	) {
		config := base
		config.Ticks = ticks
		network := m6DirectFixture(t, config)

		departed = map[nodeID]int{}
		returns = map[nodeID]bool{}
		for tick := range window {
			network.tick = tick
			before := map[int32]bool{}
			for _, node := range network.all {
				before[node] = network.online[node]
			}
			if err := network.applyChurn(); err != nil {
				t.Fatalf("tick %d: %v", tick, err)
			}
			for _, node := range network.all {
				if before[node] && !network.online[node] {
					departed[network.ids[node]] = tick
				}
			}
			for _, scheduled := range network.returning[tick+config.ReturnAfter] {
				returns[network.ids[scheduled]] = true
			}
		}
		return departed, returns, len(network.reserve), network.report
	}

	shortDepartures, shortReturns, shortReserve, shortReport := play(window)
	longDepartures, longReturns, longReserve, longReport := play(window * 8)

	if shortReserve == longReserve {
		t.Fatalf("both runs drew a reserve of %d — the fixture failed to make the arrays differ, "+
			"and the defect it guards only appears when they do", shortReserve)
	}
	if len(shortDepartures) == 0 {
		t.Fatal("nothing departed in the shared window, so this fixture compares nothing")
	}
	if !maps.Equal(shortDepartures, longDepartures) {
		t.Errorf("the shared window realised %d departures in the short run and %d in the long "+
			"one, and they are not the same events — the churn stream depends on the duration",
			len(shortDepartures), len(longDepartures))
	}
	if !maps.Equal(shortReturns, longReturns) {
		t.Errorf("%d returns were scheduled in the short run and %d in the long one, over the "+
			"same window — the return coin depends on the duration",
			len(shortReturns), len(longReturns))
	}
	// ⚠️ AND THE REPORTED LOAD, not only the realised churn. A draw is made for
	// every identifier the run carries, so the counter used to grow with the
	// reserve — the same window of the same network reported more "decisions to
	// leave" merely because the run was going to last longer. The headline figure
	// is now the original population's, and the reserve's draws are their own
	// number with their own denominator.
	if shortReport.DeparturesDecided != longReport.DeparturesDecided {
		t.Errorf("the same window decided %d departures with a reserve of %d and %d with a "+
			"reserve of %d — the reported load depends on the length of the run",
			shortReport.DeparturesDecided, shortReserve,
			longReport.DeparturesDecided, longReserve)
	}
	if shortReport.DeparturesDecided == 0 {
		t.Fatal("no departure was decided over the original population, so this compares nothing")
	}
	if shortReport.DeparturesDecidedInReserve == longReport.DeparturesDecidedInReserve {
		t.Fatalf("both runs drew %d decisions over their reserves — the fixture needs the two "+
			"reserves to differ for the split to mean anything",
			shortReport.DeparturesDecidedInReserve)
	}

	t.Logf("reserve %d against %d, %d identical departures and %d identical returns over %d ticks; "+
		"load %d in both, reserve draws %d against %d",
		shortReserve, longReserve, len(shortDepartures), len(shortReturns), window,
		shortReport.DeparturesDecided, shortReport.DeparturesDecidedInReserve,
		longReport.DeparturesDecidedInReserve)
}

// TestM6TheOfferedPrefixOfTheReserveKeepsTheMix checks the composition of what is
// actually OFFERED, not of the array as a whole.
//
// ⚠️ THE RESERVE REPEATED THE BIAS M3-a §5.5.5 EXISTS TO REMOVE. Filling two role
// quotas in the generator's order gives a head at the natural rate and, once the
// minority quota is full, a tail of pure majority. The totals were right and
// every prefix was wrong — and only a prefix is ever offered, because arrivals
// are paced by the churn. The same words are written above drawSkewedPopulation;
// the reserve was written without them.
func TestM6TheOfferedPrefixOfTheReserveKeepsTheMix(t *testing.T) {
	t.Parallel()

	sh := shape{name: "600×6", nodes: 600, degree: 6, budget: 12}
	config := m6ModelBase()
	config.Shape = sh
	config.Churn = churnCompensated
	config.ChurnShare = 0.05
	config.Ticks = 20

	// A strongly skewed population: at a 10 % Q share the quota order would give
	// a mixed head and a long ¬Q tail.
	people, err := drawSkewedPopulation(sh, config.Seed, 55, 0.1)
	if err != nil {
		t.Fatalf("drawing the population: %v", err)
	}
	g := buildGraphOnIDs(people.IDs, sh, config.Quota, config.Policy, nil, nil)

	ids, roles := drawM6Reserve(g, config, config.Ticks)
	if len(ids) < 100 {
		t.Fatalf("the reserve is %d identifiers, too few to talk about prefixes", len(ids))
	}
	want := people.ActualShare()

	structural := 0
	worst, worstShare, worstAt := 0.0, 0.0, 0
	for position, role := range roles {
		if role == roleStructural {
			structural++
		}
		length := position + 1
		if length < 40 { // below this the rounding of one node dominates
			continue
		}
		got := float64(structural) / float64(length)
		if gap := math.Abs(got - want); gap > worst {
			worst, worstShare, worstAt = gap, got, length
		}
	}
	// One node of slack on the shortest prefix measured, and the pacing holds it
	// there for every longer one.
	if worst > 1.0/40.0 {
		t.Errorf("the prefix of %d identifiers is %.1f %% Q against the population's %.1f %% — "+
			"the offered head does not carry the mix of the reserve",
			worstAt, 100*worstShare, 100*want)
	}

	// ⚠️ And a longer run EXTENDS this order rather than reshuffling it: the
	// pacing is a function of the population's share, never of the total drawn.
	longer := config
	longer.Ticks = config.Ticks * 4
	longIDs, _ := drawM6Reserve(g, longer, longer.Ticks)
	if len(longIDs) <= len(ids) {
		t.Fatalf("the longer run drew %d identifiers against %d — the fixture needs it to be "+
			"bigger", len(longIDs), len(ids))
	}
	for position, id := range ids {
		if longIDs[position] != id {
			t.Fatalf("position %d holds a different identifier once the run is longer — the "+
				"order of the reserve depends on how many are drawn", position)
		}
	}
	t.Logf("population %.1f %% Q; worst prefix deviation %.2f pp at length %d; the first %d of "+
		"%d identifiers are unchanged by quadrupling the run",
		100*want, 100*worst, worstAt, len(ids), len(longIDs))
}

// TestM6TheReportDoesNotDenyDetectionUnderCInfinity checks the printed text, not
// the mechanism.
//
// ⚠️ A report that contradicts the model is a defect of the same class as a
// wrong number: §6.7 settled that C = ∞ switches off the SCHEDULED refresh and
// not detection, because §5.1.0 charges a probe for a record a neighbour hands
// back — so in A′ and C a repeat can still find a dead record. The signature line
// and the detection line went on printing "no refresh, therefore no detection,
// therefore no recovery" for every branch.
func TestM6TheReportDoesNotDenyDetectionUnderCInfinity(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Cadence = 0 // C = ∞

	signature := config.String()
	if strings.Contains(signature, "no refresh, therefore no detection") {
		t.Error("the configuration line still says C = ∞ means no detection")
	}
	for _, want := range []string{"SCHEDULED refresh is off", "A′ and C", "ALREADY HELD"} {
		if !strings.Contains(signature, want) {
			t.Errorf("the configuration line does not say %q, so a reader cannot tell which "+
				"branches can still detect a loss", want)
		}
	}

	// A′ with C = ∞ and no loss found: a MEASUREMENT, because a repeat could have
	// found one.
	empty := m6ModelReport{Config: config}
	if line := empty.DetectionDelaySummary(); strings.Contains(line, "no loss is ever detected") ||
		!strings.Contains(line, "MEASUREMENT") {
		t.Errorf("A′ with C = ∞ and no detection reports %q — it has to read as a result, not as "+
			"a property of the control", line)
	}

	// A with C = ∞: there the claim is true, and it must still be made.
	plain := config
	plain.Branch = branchA
	// ⚠️ And it must say WHICH loss, not "no loss": in A and B a FILLING probe can still find a
	// peer gone and free its edge. Only the per-level TABLE loss is what the absent refresh
	// hides, and an over-broad claim here is the same defect as the over-broad claim §6.7
	// removed — one branch wider.
	plainLine := (m6ModelReport{Config: plain}).DetectionDelaySummary()
	if !strings.Contains(plainLine, "per-level table loss cannot be detected") {
		t.Errorf("A with C = ∞ reports %q — this branch re-probes an already held record by no "+
			"other path, and the control's expected result has to be stated", plainLine)
	}
	if !strings.Contains(plainLine, "filling probe can still find a peer gone") {
		t.Errorf("A with C = ∞ reports %q — it claims more than is true: the absent refresh hides "+
			"table losses, not every departure", plainLine)
	}

	// And the branch that CAN detect must be able to say so: a detected loss
	// under C = ∞ prints delays, not an excuse.
	found := m6ModelReport{Config: config, DetectionDelays: []int{2, 5}}
	if line := found.DetectionDelaySummary(); !strings.Contains(line, "2 losses detected") {
		t.Errorf("a detected loss under C = ∞ reports %q", line)
	}
}

// TestM6ARecordWithoutAConnectionIsRestoredAsARecord starts where the stranger
// fixture cannot: from a record that was successfully STORED and never backed by
// an edge.
//
// ⚠️ A RECORD AND A CONNECTION WERE ONE FLAG. A candidate offered by A′ fills a
// bucket slot and establishes nothing; when its subject departs, what is lost is
// the record. `detectLoss` still wrote `Released`, and `probe` reads that as "an
// edge of mine was freed", so the next successful probe re-established a link
// that had never existed — the graph changed and a slot of B went at each end,
// off the back of a table entry. The two facts are now separate sets, and only
// the edge one may restore an edge.
func TestM6ARecordWithoutAConnectionIsRestoredAsARecord(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.StaleTicks = 64 // the shelf must outlive the absence, or nothing is kept
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	if network.heldEdges(owner) >= config.Shape.budget {
		t.Fatal("the owner starts at its ceiling, so no probe of its can succeed")
	}

	// A candidate the owner has NO edge to in either direction, with room in the
	// bucket it belongs to and room of its own.
	candidate, level := int32(-1), -1
	for _, node := range network.all {
		if node == owner || network.holdsEdge(owner, node) || network.holdsEdge(node, owner) {
			continue
		}
		if !network.online[node] || network.heldEdges(node) >= config.Shape.budget {
			continue
		}
		at := levelOf(network.ids[owner], network.ids[node], config.Shape.degree)
		if at < 0 || len(state.Table.members[at]) >= config.Capacity {
			continue
		}
		candidate, level = node, at
		break
	}
	if candidate < 0 {
		t.Fatal("the fixture found no candidate without an edge and with room at both ends")
	}
	budgetBefore := network.heldEdges(owner)

	// 1. Stored as a RECORD, and the fixture checks that is what happened.
	state.Offered = append(state.Offered, candidate)
	network.probe(owner, state, candidate, level, false)
	if !state.Table.holds(candidate) {
		t.Fatal("the record was not stored, so the fixture measures nothing")
	}
	if network.holdsEdge(owner, candidate) || network.holdsEdge(candidate, owner) {
		t.Fatal("storing a record established an edge — the fixture needs a record WITHOUT one")
	}

	// 2. The subject departs and a refresh finds it gone.
	network.tick++
	network.online[candidate] = false
	state.TriedThisTick = map[int32]struct{}{}
	network.probe(owner, state, candidate, level, true)

	if state.Table.holds(candidate) {
		t.Fatal("the lost record stayed in the table")
	}
	if len(state.Shelf) != 1 || state.Shelf[0].Node != candidate {
		t.Fatalf("the lost record was not shelved: %v", state.Shelf)
	}
	if _, edge := state.ReleasedEdge[candidate]; edge {
		t.Fatal("a record-only loss was recorded as a released EDGE — no edge was ever held, " +
			"and this flag is what lets a later probe re-establish one")
	}
	if network.heldEdges(owner) != budgetBefore {
		t.Fatalf("the owner's held slots went from %d to %d on a record-only loss — nothing was "+
			"released, so nothing may be freed", budgetBefore, network.heldEdges(owner))
	}

	// 3. The subject returns and the shelved record is probed again.
	network.tick++
	network.online[candidate] = true
	state.TriedThisTick = map[int32]struct{}{}
	network.probe(owner, state, candidate, level, false)

	if !state.Table.holds(candidate) {
		t.Fatal("the record was not restored")
	}
	if network.holdsEdge(owner, candidate) || network.holdsEdge(candidate, owner) {
		t.Fatal("restoring a RECORD created a permanent CONNECTION: the graph changed and both " +
			"ends spend a slot of B on a link that never existed")
	}
	if network.heldEdges(owner) != budgetBefore {
		t.Fatalf("the owner's held slots moved from %d to %d while only a record was restored",
			budgetBefore, network.heldEdges(owner))
	}
	// ⚠️ And the burial has to be lifted anyway, or branch A and the omniscient
	// source would go on skipping this peer and its NEXT departure would never be
	// detected.
	if _, buried := state.Released[candidate]; buried {
		t.Fatal("the peer is still marked as detected-gone although its record is back")
	}
	if len(state.Shelf) != 0 {
		t.Fatalf("the restored record is still on the shelf: %v", state.Shelf)
	}
	if network.report.ShelfHits != 1 {
		t.Fatalf("%d shelf hits, want 1 — the record came back off the shelf",
			network.report.ShelfHits)
	}
}

// TestM6TheHostOfANewcomerDoesNotDependOnTheReserve keeps the arrival path out of
// the run-length dependency the churn keys were just freed from.
//
// ⚠️ `admit` drew the starting index modulo len(n.all) and scanned n.all, and
// n.all carries the whole UNJOINED reserve, whose size follows Ticks. An offset
// landing in that tail made the linear scan run off the end and wrap to the FIRST
// nodes of the population, so a longer horizon aimed a larger share of newcomers
// at the same few hosts. The early topology of a run depended on how long the run
// was going to be.
func TestM6TheHostOfANewcomerDoesNotDependOnTheReserve(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	base.Churn = churnCompensated
	base.ChurnShare = 0.05

	hostOf := func(ticks int) (nodeID, nodeID, int) {
		config := base
		config.Ticks = ticks
		network := m6DirectFixture(t, config)
		if len(network.reserve) == 0 {
			t.Fatal("no reserve was drawn")
		}
		newcomer := network.reserve[0]
		if !network.admit(m6Pending{Node: newcomer}) {
			t.Fatal("the newcomer was not admitted")
		}
		if got := network.heldEdges(newcomer); got != 1 {
			t.Fatalf("the newcomer holds %d edges, want the one starting link", got)
		}
		var host int32
		for peer := range network.held[newcomer] {
			host = peer
		}
		return network.ids[newcomer], network.ids[host], len(network.reserve)
	}

	shortNewcomer, shortHost, shortReserve := hostOf(base.Ticks)
	longNewcomer, longHost, longReserve := hostOf(base.Ticks * 8)

	if shortReserve == longReserve {
		t.Fatalf("both runs drew a reserve of %d — the fixture needs them to differ", shortReserve)
	}
	if shortNewcomer != longNewcomer {
		t.Fatal("the two runs offer different newcomers, so the hosts are not comparable")
	}
	if shortHost != longHost {
		t.Errorf("the same newcomer joined %x with a reserve of %d and %x with a reserve of %d — "+
			"the choice of host depends on the length of the run",
			shortHost[:4], shortReserve, longHost[:4], longReserve)
	}
}

// TestM6TheOmniscientControlLeavesTheBackgroundAlone checks WHOSE candidates the
// control changes.
//
// ⚠️ It scanned n.owners — the MEASURED membership — so in the Q-half run the
// unmeasured ¬Q nodes, which run the mechanism like everybody else, were offered
// Q candidates only. mayTake places no restriction on an unmeasured owner at all,
// so the control was quietly changing the background network as well as the
// measured half: a second difference beside the one it exists to isolate.
func TestM6TheOmniscientControlLeavesTheBackgroundAlone(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.OmniscientControl = true
	config.Membership = "Q half"

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
	network, err := newM6Network(g, config, structural)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	pick := func(measured bool) int32 {
		for _, node := range network.all {
			if network.measured(node) == measured && network.states[node] != nil {
				return node
			}
		}
		t.Fatalf("no owner with measured=%v", measured)
		return -1
	}

	// An UNMEASURED owner must be able to receive an unmeasured candidate.
	background := pick(false)
	sawUnmeasured := false
	state := network.states[background]
	for range 200 {
		candidate, ok := network.fromOmniscience(background, state, -1)
		if !ok {
			break
		}
		if !network.measured(candidate) {
			sawUnmeasured = true
			break
		}
		state.TriedThisTick[candidate] = struct{}{}
	}
	if !sawUnmeasured {
		t.Error("an unmeasured ¬Q owner was never offered an unmeasured candidate — the control " +
			"is restricting the background network, which mayTake does not")
	}

	// A MEASURED owner must never receive one.
	measured := pick(true)
	state = network.states[measured]
	for range 200 {
		candidate, ok := network.fromOmniscience(measured, state, -1)
		if !ok {
			break
		}
		if !network.measured(candidate) {
			t.Fatalf("a measured Q owner was offered the unmeasured node %d", candidate)
		}
		state.TriedThisTick[candidate] = struct{}{}
	}
}

// TestM6TheOmniscientOfferOrderDoesNotDependOnTheReserve is the host-choice
// fixture's twin, for the other scan that walked n.all.
//
// ⚠️ SAME DEFECT CLASS, SECOND PLACE. Moving the control from the measured
// membership to the physical population kept `% len(n.all)` and a walk over
// n.all — and n.all carries the unjoined reserve, sized for config.Ticks. An
// offset landing in that tail ran off the end and wrapped to the FIRST nodes of
// the population, so the order the control offered candidates in was a function
// of how long the run was going to be. Fixing one scan does not fix the class.
func TestM6TheOmniscientOfferOrderDoesNotDependOnTheReserve(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	base.OmniscientControl = true
	base.Churn = churnCompensated
	base.ChurnShare = 0.05

	const owner = int32(0)
	offers := func(ticks int) ([]nodeID, int) {
		config := base
		config.Ticks = ticks
		network := m6DirectFixture(t, config)
		state := network.states[owner]

		var seen []nodeID
		for range 12 {
			candidate, ok := network.fromOmniscience(owner, state, -1)
			if !ok {
				break
			}
			seen = append(seen, network.ids[candidate])
			// ⚠️ Marked as tried rather than stored: storing it would change the
			// table and the second run would be answering a different question.
			state.TriedThisTick[candidate] = struct{}{}
		}
		return seen, len(network.reserve)
	}

	shortOffers, shortReserve := offers(base.Ticks)
	longOffers, longReserve := offers(base.Ticks * 8)

	if shortReserve == longReserve {
		t.Fatalf("both runs drew a reserve of %d — the fixture needs them to differ", shortReserve)
	}
	if len(shortOffers) < 12 {
		t.Fatalf("the control offered only %d candidates, too few to compare", len(shortOffers))
	}
	if !slices.Equal(shortOffers, longOffers) {
		first := 0
		for first < len(shortOffers) && first < len(longOffers) &&
			shortOffers[first] == longOffers[first] {
			first++
		}
		t.Errorf("the offers diverge at position %d (%x against %x) with reserves of %d and %d — "+
			"the order of the control's candidates depends on the length of the run",
			first, shortOffers[first][:4], longOffers[first][:4], shortReserve, longReserve)
	}
}

// TestM6TheWholeReportDoesNotDenyDetectionUnderCInfinity checks the FULL report,
// not the two lines that were fixed by name.
//
// ⚠️ The previous round corrected the configuration signature and the detection
// line and left RecoveryLine printing "C = ∞ switches detection off" whenever the
// axis came out empty. A reader reaches that line precisely when there is nothing
// to read in the other two, so the contradiction sat in the most-read place. This
// fixture asserts over report.String(), which is the only thing that cannot be
// fixed one line at a time.
func TestM6TheWholeReportDoesNotDenyDetectionUnderCInfinity(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.Cadence = 0 // C = ∞
	config.Churn = churnShock
	empty := m6ModelReport{
		Config:           config,
		Probes:           newProbeLedger(),
		ProbesAfterChurn: newProbeLedger(),
		PhysicalProbes:   newProbeLedger(),
		LostByLevel:      make([]int, config.Shape.degree),
		RefilledByLevel:  make([]int, config.Shape.degree),
	}

	whole := empty.String()
	for _, forbidden := range []string{
		"no refresh, therefore no detection",
		"C = ∞ switches detection off",
		"no loss is ever detected",
	} {
		if strings.Contains(whole, forbidden) {
			t.Errorf("the report of A′ with C = ∞ still says %q somewhere:\n%s", forbidden, whole)
		}
	}
	if !strings.Contains(empty.RecoveryLine(), "MEASUREMENT") {
		t.Errorf("the recovery line reads %q — for A′ an empty axis is a result, because a repeat "+
			"handed back by a neighbour is a paid probe", empty.RecoveryLine())
	}

	// And the branch where the claim IS true must still make it.
	plain := config
	plain.Branch = branchA
	quiet := empty
	quiet.Config = plain
	if !strings.Contains(quiet.RecoveryLine(), "no other path") {
		t.Errorf("branch A with C = ∞ reads %q — there the control's expected result has to be "+
			"stated, not left as a bare 'no data'", quiet.RecoveryLine())
	}
}

// TestM6ThePopulationLineSeparatesTheOriginalFromTheNewcomers is about a sentence
// that was arithmetically true and said the wrong thing.
//
// ⚠️ The line printed the COMBINED online count against the ORIGINAL population's
// denominator: "800 of 1000 in the original population" while 200 of those 800
// were newcomers from the reserve. A run that lost a third of its nodes and
// replaced them read as a run that had lost nothing, and the qualifier that
// followed did not repair the number in front of it. Three numbers, named apart.
func TestM6ThePopulationLineSeparatesTheOriginalFromTheNewcomers(t *testing.T) {
	t.Parallel()

	report := m6ModelReport{
		Config:               m6ModelBase(),
		Members:              1000,
		Departed:             300,
		ArrivalsOffered:      300,
		NewcomersOffered:     300,
		ArrivalsAdmitted:     300,
		OnlineByTick:         []int{1000},
		OnlineFromPopulation: 700,
		OnlineFromReserve:    300,
		ReserveMeasured:      400,
	}
	line := report.PopulationLine()

	if !strings.Contains(line, "700 of the 1000 ORIGINAL") {
		t.Errorf("the line does not show how many of the ORIGINAL population are left:\n%s", line)
	}
	if !strings.Contains(line, "300 admitted from a reserve") {
		t.Errorf("the line does not show the newcomers apart:\n%s", line)
	}
	if !strings.Contains(line, "= 1000 in total") {
		t.Errorf("the line does not show the total:\n%s", line)
	}
	// The exact reading the old wording invited: the full original population
	// still online.
	if strings.Contains(line, "1000 of the 1000 ORIGINAL") {
		t.Errorf("the line claims the original population is intact although 300 of it left:\n%s",
			line)
	}
	if !strings.Contains(line, "NOT the original composition") {
		t.Errorf("the line does not warn that a steady total is not a steady composition:\n%s",
			line)
	}
}

// TestM6AZeroExchangeIntervalIsRefusedAtTheDoor is a guard against a
// configuration that does not terminate, and it is checked at construction
// because there is nowhere later to check it.
//
// ⚠️ NOT A MISSING VALIDATION FOR ITS OWN SAKE. With T_exch = 0 and repeatable
// exchanges, a live neighbour that hands over nothing is still a successful
// exchange, so fromBranch searches again; the interval is what would have made
// that neighbour ineligible for the rest of the tick, and at zero it never does.
// The search recurses forever and the ceiling R cannot stop it: R bounds PROBES,
// and control never returns to the loop that spends them. The contract's
// T_exch = 64 never reaches this, which is why nothing in the grid would have
// found it.
func TestM6AZeroExchangeIntervalIsRefusedAtTheDoor(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	g := buildGraph(base.Shape, base.Seed, base.Quota, base.Policy)

	for _, branch := range []m6Branch{branchAPrime, branchC} {
		config := base
		config.Branch = branch
		config.ExchangeEvery = 0
		if _, err := newM6Network(g, config, everybody); err == nil {
			t.Errorf("branch %s accepted T_exch = 0 — the candidate search does not terminate "+
				"under it, so the run would hang rather than measure", branch)
		}

		// ⚠️ The SAME zero is fine under the single-exchange control: there the
		// neighbour is asked once ever and the interval is never consulted. A
		// guard that rejected it would forbid a control the contract defines.
		once := config
		once.ExchangeOnce = true
		if _, err := newM6Network(g, once, everybody); err != nil {
			t.Errorf("branch %s rejected T_exch = 0 under the single-exchange control, where the "+
				"interval is never read: %v", branch, err)
		}
	}

	// And branches that never exchange do not read the field at all.
	for _, branch := range []m6Branch{branchA, branchB} {
		config := base
		config.Branch = branch
		config.ExchangeEvery = 0
		if _, err := newM6Network(g, config, everybody); err != nil {
			t.Errorf("branch %s rejected T_exch = 0 although it performs no exchange: %v",
				branch, err)
		}
	}

	// The contract's own value passes, so the guard is a floor and not a wall.
	valid := base
	valid.Branch = branchAPrime
	valid.ExchangeEvery = 64
	if _, err := newM6Network(g, valid, everybody); err != nil {
		t.Errorf("the contract's T_exch = 64 was rejected: %v", err)
	}
}

// TestM6TheMeasuredPoolCountsTheControlsOffers is the small fixture S(u) was
// missing: one offer adds one, the same offer again adds nothing.
//
// ⚠️ THE CONTROL WAS THE ONE SOURCE THAT DID NOT REPORT ITS OFFERS. The exchange
// and the addressed answer both call noteReachable; fromOmniscience handed over a
// candidate and said nothing, and poolOf counts Reachable plus the owner's own
// neighbours. So a run whose table the control had filled with strangers reported
// a pool of exactly the neighbours the owner started with — S(u), the measured
// quantity П-5 derives the near border from, was blind to the very source under
// measurement.
func TestM6TheMeasuredPoolCountsTheControlsOffers(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.OmniscientControl = true
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]
	before := network.poolOf(owner)

	first, ok := network.fromOmniscience(owner, state, -1)
	if !ok {
		t.Fatal("the control offered nobody")
	}
	if network.holdsEdge(owner, first) {
		t.Fatal("the control offered a neighbour, which the pool already counted — the fixture " +
			"needs a stranger to show the difference")
	}
	if got := network.poolOf(owner); got != before+1 {
		t.Fatalf("the pool went from %d to %d after one offer of a stranger, want %d — an offer "+
			"the control actually made is part of the MEASURED pool", before, got, before+1)
	}

	// ⚠️ The SAME candidate again. S(u) is a set of identities, not a count of
	// hand-outs: counting the repeat would make the pool grow with the length of
	// the run rather than with what the source can reach.
	again, ok := network.fromOmniscience(owner, state, -1)
	if !ok || again != first {
		t.Fatalf("the control offered %v (ok=%v) on the repeat, want the same %d again — the "+
			"fixture needs the repeat to measure the repeat", again, ok, first)
	}
	if got := network.poolOf(owner); got != before+1 {
		t.Fatalf("the pool moved to %d on a REPEATED offer of the same node, want %d",
			got, before+1)
	}

	// And the availability is the other quantity, printed apart: everything the
	// control may take, whether or not a probe was ever spent on it.
	potential := network.poolPotentialOf(owner)
	if potential <= before+1 {
		t.Errorf("the control's available population is %d against a measured pool of %d — "+
			"'knows everybody' is a statement about availability and has to be visibly larger",
			potential, before+1)
	}
	t.Logf("measured pool %d → %d after one offer, unchanged on the repeat; available population %d",
		before, before+1, potential)
}

// TestM6ThePopulationLineKeepsEachNumberWithItsOwnDenominator is about two
// populations printed in one sentence.
//
// ⚠️ EVERY CHURN COUNTER IS PHYSICAL and every online count is MEASURED, and the
// line printed the first against the denominator of the second: "45 decided of
// 507" where the 45 had been drawn over 1000 nodes. In the Q-half run the load of
// the whole network was attributed to the Q half — the same class of error as
// pricing one population's work against another's coverage, which the probe
// ledgers were split for two rounds earlier.
func TestM6ThePopulationLineKeepsEachNumberWithItsOwnDenominator(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnCompensated
	config.ChurnShare = 0.02
	config.ChurnAt = 1
	config.Ticks = 10

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	half := config
	half.Membership = "Q half"
	whole := runM6ModelOn(t, g, config, everybody)
	part := runM6ModelOn(t, g, half, structural)

	if part.Members == 0 || part.Members >= whole.Members {
		t.Fatalf("the Q half holds %d members against %d — the fixture needs them to differ",
			part.Members, whole.Members)
	}

	// ⚠️ The decision count is PHYSICAL, so narrowing the membership must not
	// move it by one. If it does, the counter is measuring the view instead of
	// the network.
	if part.DeparturesDecided != whole.DeparturesDecided {
		t.Errorf("%d departure decisions in the Q-half run against %d in the whole-network run "+
			"of the same graph and seed — the decisions are drawn over the physical population "+
			"and must not depend on who is measured",
			part.DeparturesDecided, whole.DeparturesDecided)
	}
	if part.DeparturesDecided == 0 {
		t.Fatal("no departure was decided, so this fixture compares nothing")
	}

	line := part.PopulationLine()
	// The churn half names the physical population…
	if !strings.Contains(line, fmt.Sprintf("its %d original nodes", config.Shape.nodes)) {
		t.Errorf("the churn half of the line does not name the PHYSICAL population of %d:\n%s",
			config.Shape.nodes, line)
	}
	if !strings.Contains(line, "WHOLE PHYSICAL network") {
		t.Errorf("the churn half is not labelled as physical:\n%s", line)
	}
	// …and the online half names the measured one.
	if !strings.Contains(line, fmt.Sprintf("the %d ORIGINAL %s population", part.Members,
		half.Membership)) {
		t.Errorf("the online half does not name the MEASURED population of %d:\n%s",
			part.Members, line)
	}
	// The exact sentence the defect produced: the physical decision count read
	// against the measured denominator.
	if strings.Contains(line, fmt.Sprintf("%d departures decided over the %d",
		part.DeparturesDecided, part.Members)) {
		t.Errorf("the physical decision count is printed against the measured denominator:\n%s",
			line)
	}
	t.Logf("%s", line)
}

// TestM6AShockReportsItsDecisions covers the churn form the invariant sweep used
// to skip.
//
// ⚠️ The shock filled `leaving` and booked no decision at all, so the report read
// "0 departures decided … 200 actually left" — a sentence that describes a broken
// model rather than a different churn form. The shock's decision IS the
// selection: the count is exact by construction and everybody chosen is online,
// so the two numbers coincide, and that is worth printing rather than hiding.
func TestM6AShockReportsItsDecisions(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnShock
	config.ChurnShare = 0.2
	config.ChurnAt = 1
	config.ReturnShare = 0
	config.Ticks = 6

	report := runM6Model(t, config)

	if report.Departed == 0 {
		t.Fatal("nothing left under a 20 % shock, so this fixture measures nothing")
	}
	if report.DeparturesDecided < report.Departed {
		t.Errorf("%d departures decided against %d realised — a shock decides exactly whom it "+
			"takes, so the decisions cannot be fewer",
			report.DeparturesDecided, report.Departed)
	}
	if report.DeparturesDecidedInReserve != 0 {
		t.Errorf("%d shock decisions were booked against the reserve, which has not joined",
			report.DeparturesDecidedInReserve)
	}
	// ⚠️ Anchored on the phrase that precedes it: "0 departures decided" is a
	// substring of "200 departures decided", and a fixture that matched it would
	// fail on a correct report.
	if strings.Contains(report.PopulationLine(), "network: 0 departures decided") {
		t.Errorf("the shock reports no decisions beside its departures:\n%s",
			report.PopulationLine())
	}
	t.Logf("%s", report.PopulationLine())
}

// TestM6AReturnDueInThePastIsRefusedAtTheDoor guards a configuration that
// silently deletes the axis it claims to measure.
//
// ⚠️ applyChurn reads the returns due THIS tick before it decides who leaves, so
// a departure in tick t with T_back = 0 files its return into t — a slot already
// read and never read again; T_back < 0 files it into the past outright. The run
// still counts the departures and still prints `ret`, so a reader sees a recovery
// scenario where not one return ever happens: at ret = 1 and T_back = 0 every
// departed node is promised back and none arrives. The contract's T_back = 32
// never reaches this, which is why nothing in the grid would have caught it.
func TestM6AReturnDueInThePastIsRefusedAtTheDoor(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	base.Churn = churnShock
	base.ChurnShare = 0.2
	base.ChurnAt = 1
	g := buildGraph(base.Shape, base.Seed, base.Quota, base.Policy)

	for _, after := range []int{0, -1, -32} {
		config := base
		config.ReturnShare = 1
		config.ReturnAfter = after
		if _, err := newM6Network(g, config, everybody); err == nil {
			t.Errorf("T_back = %d was accepted with returns enabled — every return is filed into "+
				"a tick that has already been read, so none of them happens and the recovery "+
				"axis is empty while the report still claims ret = 1", after)
		}
	}

	// ⚠️ With no returns the field is never read, and forbidding it there would
	// reject a scenario the contract defines: ret = 0 is one of the two agreed
	// values.
	for _, after := range []int{0, -1} {
		config := base
		config.ReturnShare = 0
		config.ReturnAfter = after
		if _, err := newM6Network(g, config, everybody); err != nil {
			t.Errorf("T_back = %d was rejected although ret = 0 never reads it: %v", after, err)
		}
	}

	// And the contract's own value passes, so the guard is a floor, not a wall.
	valid := base
	valid.ReturnShare = 0.5
	valid.ReturnAfter = 32
	if _, err := newM6Network(g, valid, everybody); err != nil {
		t.Errorf("the contract's T_back = 32 was rejected: %v", err)
	}

	// The property behind the guard, stated as a run: with a legal T_back EVERY
	// promised return is honoured.
	//
	// ⚠️ "At least one" would have been the wrong assertion. One shock, ret = 1
	// and a due tick inside the window means every departure owes exactly one
	// return, so the fixture can demand the exact count — and a queue that loses
	// most of its entries, which is what the guarded defect does to a subset,
	// would satisfy "at least one" and fail here.
	honoured := base
	honoured.ReturnShare = 1
	honoured.ReturnAfter = 1
	honoured.Ticks = 6
	report := runM6ModelOn(t, g, honoured, everybody)
	if report.Departed == 0 {
		t.Fatal("nothing left under the shock, so this fixture measures nothing")
	}
	if report.ReturnsOffered != report.Departed {
		t.Errorf("%d nodes left with ret = 1 and T_back = 1, and %d returns were offered — with "+
			"one shock and a due tick inside the run every departure owes exactly one return",
			report.Departed, report.ReturnsOffered)
	}
	// ⚠️ And OFFERED is not ARRIVED: a return that never leaves the queue is the
	// same missing measurement in a different place. A return needs no host and
	// no free slot (stand assumption 7), so under one shock all of them land.
	returned := report.ReturnedWithATable + report.ReturnedEmptyHanded
	if returned != report.Departed {
		t.Errorf("%d nodes left and %d came back (%d with a table, %d empty-handed) — every "+
			"promised return is due inside this run and nothing can refuse one",
			report.Departed, returned, report.ReturnedWithATable, report.ReturnedEmptyHanded)
	}
	if report.NewcomersOffered != 0 {
		t.Errorf("%d newcomers were offered under a shock, which has no compensation quota",
			report.NewcomersOffered)
	}
}

// TestM6ADetectionIsCountedOncePerLossWhateverWasHeld pins the accounting of
// a probe that finds a departed peer, on the three things the owner may have
// held — an EDGE without a record, a RECORD without an edge, both — and on a
// second probe of the same peer. The expectations come from the construction
// of each case, not from the totals: a detection is one departed peer found
// gone (whatever was held), a lost slot is one table entry dropped, and
// neither is ever counted twice for the same peer.
//
// ⚠️ Mutations that must break it (all three proven): counting a detection
// only when a slot was lost (case 1 then shows zero detections); counting a
// detection per probe rather than per loss (the repeat in case 3 shows two);
// counting the edge loss as a lost slot (case 1 shows a lost slot).
func TestM6ADetectionIsCountedOncePerLossWhateverWasHeld(t *testing.T) {
	t.Parallel()

	type counters struct {
		detections, phaseDetections, physicalDetections, lostSlots, physicalLost, shelved, held int
	}
	read := func(network *m6Network, owner int32) counters {
		lost := 0
		for _, count := range network.report.LostByLevel {
			lost += count
		}
		return counters{
			detections:         len(network.report.DetectionDelays),
			phaseDetections:    network.schedule.current().Detections,
			physicalDetections: network.report.PhysicalDetections,
			lostSlots:          lost,
			physicalLost:       network.report.PhysicalLost,
			shelved:            len(network.states[owner].Shelf),
			held:               network.heldEdges(owner),
		}
	}
	// depart takes the peer offline the way departShare does: offline, with
	// the departure tick recorded, so that a detection can be dated.
	depart := func(network *m6Network, peer int32) {
		network.tick++
		network.online[peer] = false
		network.departedAt[peer] = network.tick
	}
	probeGone := func(t *testing.T, network *m6Network, owner, peer int32, refresh bool) {
		t.Helper()
		state := network.states[owner]
		state.TriedThisTick = map[int32]struct{}{}
		level := levelOf(network.ids[owner], network.ids[peer], network.config.Shape.degree)
		if got := network.probe(owner, state, peer, level, refresh); got != m6CandidateUnreachable {
			t.Fatalf("the probe of the departed peer ended in %q, not unreachable", got)
		}
	}
	const owner = int32(0)

	t.Run("an edge without a record: one detection, no lost slot, the edge released", func(t *testing.T) {
		network := m6DirectFixture(t, m6ModelBase())
		state := network.states[owner]
		peer := network.neighboursOf(owner)[0]
		if !network.holdsEdge(owner, peer) || state.Table.holds(peer) {
			t.Fatal("the fixture needs a held edge and an empty table")
		}
		before := read(network, owner)
		depart(network, peer)
		probeGone(t, network, owner, peer, false)
		after := read(network, owner)

		want := before
		want.detections, want.phaseDetections, want.physicalDetections = before.detections+1,
			before.phaseDetections+1, before.physicalDetections+1
		want.held = before.held - 1
		if after != want {
			t.Fatalf("edge-only loss:\n got  %+v\n want %+v", after, want)
		}
		if _, released := state.ReleasedEdge[peer]; !released {
			t.Fatal("the released edge is not marked as one")
		}
	})

	t.Run("a record without an edge: one detection, one lost slot, no edge released", func(t *testing.T) {
		config := m6ModelBase()
		config.Branch = branchAPrime
		config.StaleTicks = 64
		network := m6DirectFixture(t, config)
		state := network.states[owner]
		// A stranger with room at both ends, stored as a record by a probe.
		peer, level := int32(-1), -1
		for _, node := range network.all {
			if node == owner || network.holdsEdge(owner, node) || network.holdsEdge(node, owner) ||
				!network.online[node] || network.heldEdges(node) >= config.Shape.budget {
				continue
			}
			at := levelOf(network.ids[owner], network.ids[node], config.Shape.degree)
			if at < 0 || len(state.Table.members[at]) >= config.Capacity {
				continue
			}
			peer, level = node, at
			break
		}
		if peer < 0 {
			t.Fatal("no stranger with room at both ends")
		}
		state.Offered = append(state.Offered, peer)
		if got := network.probe(owner, state, peer, level, false); got != m6SlotFilled || network.holdsEdge(owner, peer) {
			t.Fatalf("the record was not stored without an edge: %q, edge=%v", got, network.holdsEdge(owner, peer))
		}
		before := read(network, owner)
		depart(network, peer)
		probeGone(t, network, owner, peer, true)
		after := read(network, owner)

		want := before
		want.detections, want.phaseDetections, want.physicalDetections = before.detections+1,
			before.phaseDetections+1, before.physicalDetections+1
		want.lostSlots, want.physicalLost, want.shelved = before.lostSlots+1, before.physicalLost+1, before.shelved+1
		if after != want {
			t.Fatalf("record-only loss:\n got  %+v\n want %+v", after, want)
		}
		if _, released := state.ReleasedEdge[peer]; released {
			t.Fatal("a record-only loss was marked as a released edge")
		}
	})

	t.Run("an edge with a record: one detection, one lost slot, the edge released — and never twice", func(t *testing.T) {
		network := m6DirectFixture(t, m6ModelBase())
		state := network.states[owner]
		peer := network.neighboursOf(owner)[0]
		level := levelOf(network.ids[owner], network.ids[peer], network.config.Shape.degree)
		if level < 0 {
			t.Fatal("the first neighbour shares the whole prefix; the fixture needs another")
		}
		if got := network.probe(owner, state, peer, level, false); got != m6SlotFilled {
			t.Fatalf("the neighbour was not stored as a record: %q", got)
		}
		if !network.holdsEdge(owner, peer) || !state.Table.holds(peer) {
			t.Fatal("the fixture needs both an edge and a record")
		}
		before := read(network, owner)
		depart(network, peer)
		probeGone(t, network, owner, peer, true)
		after := read(network, owner)

		want := before
		want.detections, want.phaseDetections, want.physicalDetections = before.detections+1,
			before.phaseDetections+1, before.physicalDetections+1
		want.lostSlots, want.physicalLost, want.shelved = before.lostSlots+1, before.physicalLost+1, before.shelved+1
		want.held = before.held - 1
		if after != want {
			t.Fatalf("edge-and-record loss:\n got  %+v\n want %+v", after, want)
		}

		// The same peer probed again while still gone: a probe is spent, nothing
		// is detected or lost a second time.
		probesBefore := network.report.Probes.Probes()
		network.tick++
		probeGone(t, network, owner, peer, false)
		if again := read(network, owner); again != after {
			t.Fatalf("a repeated probe of the same departed peer changed the accounting:\n got  %+v\n want %+v",
				again, after)
		}
		if got := network.report.Probes.Probes(); got != probesBefore+1 {
			t.Fatalf("the repeated probe was not charged: %d probes, want %d", got, probesBefore+1)
		}
	})
}
