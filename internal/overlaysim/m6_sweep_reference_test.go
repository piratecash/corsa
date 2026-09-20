package overlaysim

// m6_sweep_reference_test.go pins the M6 DRIVER: the enumeration, its
// arithmetic, what a run stores, and the one property of a δ pair the task names
// explicitly — the two halves read ONE input stream. The model underneath is
// pinned by the m6_*_reference_test.go family and reviewed to completion on
// 2026-09-19; it is not re-checked here.
//
// ⚠️ Mutations that must break these fixtures:
//
//	a grid axis dropped, or a twenty-first added — the arithmetic fixture;
//	`R` without a ceiling, `C = ∞` or the single exchange counted AGAIN as a
//	    control beyond the grid — the same (400 would read as 460, which is
//	    exactly the count the registry §5.1 was written to correct);
//	the δ pair counted as one run instead of two — the scenario-run fixture;
//	a population dropped — the population fixture;
//	the two δ halves given two recordings, or one half allowed to modify it —
//	    the one-stream fixture;
//	the α control run without its pair's boundaries — the pairing fixture;
//	the recovery window kept in the reference-pool reading — the regression
//	    fixture below (this is the defect the driver found on 2026-09-19).

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// m6SweepShapes is the pair the grid is defined over, as registry §5.1 counts
// it.
func m6SweepShapes() []shape { return []shape{sweepShapes[0], sweepShapes[1]} }

// TestM6SweepArithmeticIs520 is the contract as a number: 400 + 80 + 40, each
// term read off the enumeration rather than trusted.
func TestM6SweepArithmeticIs520(t *testing.T) {
	t.Parallel()

	configs := m6SweepEnumeration(m6SweepShapes(), sweepSeeds)
	byKind := m6RunsByKind(configs)

	if got, want := byKind[m6Grid], 400; got != want {
		t.Errorf("the screened grid is %d scenario runs, §5.9.5 counts %d = 20 configurations × "+
			"2 populations × 2 shapes × 5 seeds", got, want)
	}
	if got, want := byKind[m6ControlBeyondGrid], 80; got != want {
		t.Errorf("the controls beyond the grid are %d scenario runs, registry §5.2 counts %d = "+
			"4 controls × 2 × 2 × 5", got, want)
	}
	if got, want := byKind[m6DeltaPair], 40; got != want {
		t.Errorf("δ is %d scenario runs, registry §5.2.2 counts %d = 20 pairs × 2 halves", got, want)
	}
	if got, want := m6ScenarioRuns(configs), 520; got != want {
		t.Fatalf("the sweep is %d scenario runs and the registry's accepted volume is %d", got, want)
	}

	// Configurations and runs are different units, and the registry counts both.
	if got, want := len(configs), 500; got != want {
		t.Errorf("%d configurations, want %d = 25 variants × 2 populations × 2 shapes × 5 seeds "+
			"(a δ pair is ONE configuration and TWO runs)", got, want)
	}
}

// TestM6GridIsTheTwentyScreenedConfigurations checks the axes by name, because a
// count alone passes when one axis is measured twice and another not at all.
func TestM6GridIsTheTwentyScreenedConfigurations(t *testing.T) {
	t.Parallel()

	variants := m6GridVariants()
	if len(variants) != 20 {
		t.Fatalf("the screened grid holds %d configurations, §5.9.5 counts 20", len(variants))
	}

	names := map[string]bool{}
	for _, variant := range variants {
		if variant.Kind != m6Grid {
			t.Errorf("%q is in the grid list but is a %s", variant.Name, variant.Kind)
		}
		if names[variant.Name] {
			t.Errorf("the grid holds %q twice", variant.Name)
		}
		names[variant.Name] = true
	}

	// The four branches, the capacity, the two repair ceilings, the two
	// cadences, the eight churn points and the three exchange points.
	for _, want := range []string{
		"branch/A", "branch/B", "branch/A-prime (base)", "branch/C",
		"k/1",
		"R/1", "R/no-ceiling (control inside the grid)",
		"C/256", "C/infinite (negative control inside the grid)",
		"churn/shock-f0.05-ret0.0", "churn/shock-f0.05-ret0.5",
		"churn/shock-f0.50-ret0.0", "churn/shock-f0.50-ret0.5",
		"churn/compensated-ret0.0", "churn/compensated-ret0.5",
		"churn/shrink-ret0.0", "churn/shrink-ret0.5",
		"m/2", "m/8", "exchange/once (control inside the grid)",
	} {
		if !names[want] {
			t.Errorf("the grid does not hold %q", want)
		}
	}

	// ⚠️ And the three that ARE controls but live INSIDE the 400 are counted
	// once. A grid variant carrying the kind m6ControlBeyondGrid would add 60
	// runs that §5.1 says are already there.
	for _, inside := range []string{"R/no-ceiling (control inside the grid)",
		"C/infinite (negative control inside the grid)", "exchange/once (control inside the grid)"} {
		for _, variant := range variants {
			if variant.Name == inside && variant.Kind != m6Grid {
				t.Errorf("%q is counted beyond the grid; registry §5.1 puts it inside the 400", inside)
			}
		}
	}
}

// TestM6ControlsBeyondTheGridAreTheFourOfTheRegistry.
func TestM6ControlsBeyondTheGridAreTheFourOfTheRegistry(t *testing.T) {
	t.Parallel()

	controls := m6ControlVariants()
	if len(controls) != 4 {
		t.Fatalf("%d controls beyond the grid, registry §5.2 counts four", len(controls))
	}
	names := map[string]bool{}
	for _, control := range controls {
		if control.Kind != m6ControlBeyondGrid {
			t.Errorf("%q is in the control list and is a %s", control.Name, control.Kind)
		}
		names[control.Name] = true
	}
	for _, want := range []string{
		"control/omniscient-source",
		"control/alpha-recovery-after-cleared-state",
		"control/local-repeat-filter",
		"control/near-from-d-half-branch-C",
	} {
		if !names[want] {
			t.Errorf("the controls do not hold %q", want)
		}
	}

	// Each control has to actually change the base, or it would be a duplicate
	// of a grid run wearing another name — which is 20 runs of nothing.
	base := m6GridBase(sweepShapes[0], 1)
	for _, control := range controls {
		changed := base
		control.Apply(&changed)
		// Compared through the configuration's own rendering: m6ModelConfig
		// carries slices and is not comparable, and a renderer that printed
		// less than it holds would be a defect of its own.
		if changed.String() == base.String() {
			t.Errorf("the control %q changes nothing in the base configuration", control.Name)
		}
	}

	// ⚠️ The d/2 control for branch C must differ from the GRID's branch C,
	// whose border is DERIVED from the measured pool (П-5). If the grid also
	// used d/2 the control would be twenty duplicate runs.
	branchC, control := base, base
	for _, variant := range m6GridVariants() {
		if variant.Name == "branch/C" {
			variant.Apply(&branchC)
		}
	}
	for _, variant := range controls {
		if variant.Name == "control/near-from-d-half-branch-C" {
			variant.Apply(&control)
		}
	}
	if branchC.NearFromRule != "" {
		t.Errorf("the grid's branch C fixes the near-level border to %q; П-5 derives it from the "+
			"MEASURED pool and the d/2 control is what makes the comparison", branchC.NearFromRule)
	}
	if !strings.Contains(control.NearFromRule, "CONTROL d/2") {
		t.Errorf("the control's border rule is %q and does not say it is the control value",
			control.NearFromRule)
	}
}

// TestM6EveryConfigurationIsMeasuredOnBothPopulations: the Q half is the
// question the overlay is for, and the full graph is what it is compared with.
func TestM6EveryConfigurationIsMeasuredOnBothPopulations(t *testing.T) {
	t.Parallel()

	configs := m6SweepEnumeration(m6SweepShapes(), sweepSeeds)
	seen := map[string]map[m6Population]int{}
	for _, config := range configs {
		bucket := config.Variant.Name + "|" + config.Shape.name
		if seen[bucket] == nil {
			seen[bucket] = map[m6Population]int{}
		}
		seen[bucket][config.Population]++
	}
	for bucket, populations := range seen {
		if populations[m6WholeNetwork] != len(sweepSeeds) ||
			populations[m6StructuralHalf] != len(sweepSeeds) {
			t.Errorf("%s is measured %d times on the whole network and %d on the Q half, want %d "+
				"each", bucket, populations[m6WholeNetwork], populations[m6StructuralHalf],
				len(sweepSeeds))
		}
	}
	// And the two populations are genuinely two filters.
	if m6WholeNetwork.member()(makeNodeID(1, 0)) != true {
		t.Error("the whole-network membership excludes a node")
	}
	structural := m6StructuralHalf.member()
	for index := range 64 {
		id := makeNodeID(1, index)
		if structural(id) != (roleOf(id) == roleStructural) {
			t.Fatalf("the Q-half membership does not follow the one classifier at node %d", index)
		}
	}
}

// TestM6RunKeyCarriesEveryInput: the variant, the population, the shape, the
// seed and the BASE the variant departs from.
func TestM6RunKeyCarriesEveryInput(t *testing.T) {
	t.Parallel()

	base := m6SweepConfig{Variant: m6GridVariants()[2], Population: m6WholeNetwork,
		Shape: sweepShapes[0], Seed: 1}
	id := base.Key("v").ID()

	for _, changed := range []struct {
		name   string
		config m6SweepConfig
	}{
		{"variant", m6SweepConfig{Variant: m6GridVariants()[0], Population: m6WholeNetwork, Shape: sweepShapes[0], Seed: 1}},
		{"population", m6SweepConfig{Variant: m6GridVariants()[2], Population: m6StructuralHalf, Shape: sweepShapes[0], Seed: 1}},
		{"shape", m6SweepConfig{Variant: m6GridVariants()[2], Population: m6WholeNetwork, Shape: sweepShapes[1], Seed: 1}},
		{"seed", m6SweepConfig{Variant: m6GridVariants()[2], Population: m6WholeNetwork, Shape: sweepShapes[0], Seed: 2}},
	} {
		if changed.config.Key("v").ID() == id {
			t.Errorf("changing the %s left the identifier at %s", changed.name, id)
		}
	}

	key := base.Key("v")
	for _, want := range []string{"kind", "variant", "population", "policy", "quota", "phases",
		"recovery_window_W_rec", "base_branch", "base_capacity_k", "base_repair_R",
		"base_cadence_C", "base_churn", "base_exchange_m", "base_stale_ticks"} {
		if _, ok := key.Param(want); !ok {
			t.Errorf("the run key does not carry %s", want)
		}
	}

	keys := make([]runKey, 0, 500)
	for _, config := range m6SweepEnumeration(m6SweepShapes(), sweepSeeds) {
		keys = append(keys, config.Key("v"))
	}
	if err := requireDistinctKeys(keys); err != nil {
		t.Fatalf("the enumeration collides: %v", err)
	}
}

// TestM6TheReferencePoolCanBeTakenForTheAgreedGrid is the REGRESSION of the
// defect the driver found on 2026-09-19.
//
// ⚠️ Counterexample, verbatim: referencePool copies the caller's configuration,
// sets the churn to none and — before the fix — kept RecoveryWindow. A window is
// only legal under a churn form (with no onset there is no interval to read), so
// the constructor refused it, and the border of the near levels could not be
// derived for ANY configuration of the agreed grid, every one of which carries
// W_rec = 1024 (decision 3.5(ii)).
//
// ⚠️ Dependent measurements to repeat: NONE. referencePool had no caller that
// carried a window — the fixtures derive nothing — so no published number came
// from the broken path. This fixture is what stops it coming back.
func TestM6TheReferencePoolCanBeTakenForTheAgreedGrid(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m6GridQuota, policyCandidateC1)
	config := m6GridBase(sh, 1)
	if config.RecoveryWindow == 0 {
		t.Fatal("the grid base carries no recovery window, so this fixture exercises nothing")
	}

	border, rule, err := referencePool(g, config, everybody)
	if err != nil {
		t.Fatalf("the reference pool of §5.1.1 could not be taken for the agreed grid: %v", err)
	}
	if border < 0 || border > sh.degree {
		t.Errorf("the derived border is level %d, outside 0…%d", border, sh.degree)
	}
	if !strings.Contains(rule, "S_ref") {
		t.Errorf("the border rule is %q and does not say it was derived from the measured pool — "+
			"a number alone cannot say whether it is П-5 or the d/2 control", rule)
	}
	// ⚠️ And the derived border is not silently the control value: if it were,
	// the d/2 control for branch C would be twenty duplicate runs and nobody
	// would see it from the report.
	t.Logf("derived near-level border for %s seed 1: %d (%s); the d/2 control value is %d",
		sh.name, border, rule, sh.degree/2)

	// ⚠️ THE SECOND COUNTEREXAMPLE, found the same day and of the same shape:
	// every configuration of the sweep must be able to take its reference, and
	// two of them could not. The ‘local repeat filter’ control was REFUSED
	// (the constructor allows the filter only on an adaptive A′ run, and the
	// reference sets branch A); the omniscient control was worse — it was
	// ACCEPTED and measured the OMNISCIENT source's pool, so its border would
	// have been derived at a different level from every other configuration's
	// while the report said they shared one.
	//
	// ⚠️ Dependent measurement repeated: the one omniscient run taken before
	// the fix (1k×8, seed 1, whole network) — set aside under runs-m6-superseded
	// and measured again. Nothing else had been taken.
	for _, variant := range append(m6GridVariants(), m6ControlVariants()...) {
		for _, population := range []m6Population{m6WholeNetwork, m6StructuralHalf} {
			candidate := m6SweepConfig{Variant: variant, Population: population, Shape: sh, Seed: 1}
			model, err := candidate.config(g)
			if err != nil {
				t.Errorf("%s on the %s: the reference pool cannot be taken, so the near-level "+
					"border of §5.1.1 cannot be derived: %v", variant.Name, population, err)
				continue
			}
			if model.NearFrom != border && !strings.Contains(model.NearFromRule, "CONTROL") {
				t.Errorf("%s on the %s derived border %d and the reference is %d: the border is "+
					"taken ONCE from branch A over the full graph and applied to every branch and "+
					"both populations, or two tables are compared at different levels",
					variant.Name, population, model.NearFrom, border)
			}
		}
	}
}

// TestM6TheAlphaControlIsPairedToItsBase: the pairing is compulsory, and the
// driver supplies it. A control that plays a different scenario measures the
// scenario, not the memory.
func TestM6TheAlphaControlIsPairedToItsBase(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m6GridQuota, policyCandidateC1)
	alpha := m6SweepConfig{Population: m6WholeNetwork, Shape: sh, Seed: 1}
	for _, control := range m6ControlVariants() {
		if control.Name == "control/alpha-recovery-after-cleared-state" {
			alpha.Variant = control
		}
	}
	if alpha.Variant.Name == "" {
		t.Fatal("the α control is not in the control list")
	}

	// Without the boundaries the model refuses it: that refusal is the contract.
	model, err := alpha.config(g)
	if err != nil {
		t.Fatalf("building the α configuration: %v", err)
	}
	if !model.StartEmpty {
		t.Fatal("the α control does not clear the state, so it is not the control at all")
	}
	if _, err := newM6Network(g, model, everybody); err == nil {
		t.Error("the α control was accepted WITHOUT its pair's boundaries: its own stop rules " +
			"would land F4 on other ticks, and every churn event is keyed on the tick")
	}

	// And with the pair's boundaries it is accepted. The pair is run on a SHORT
	// plan here: what is being checked is that the control is pinned to its
	// pair, and the contract's 1537 ticks would make a fixture a measurement.
	// ⚠️ The branch of m6BoundariesFor that RUNS the paired base when it is not
	// already known is exercised by the sweep itself (its log shows the α
	// control completing); what is exercised here is the selection and the
	// lookup, which is where a control could silently stop being paired.
	short := model
	short.StartEmpty = false
	short.Phases = &m6PhasePlan{FillTicks: 10, IdleTicks: 4, RecoveryTicks: 10, CadenceTicks: 6}
	short.RecoveryWindow = 8
	short.Branch = branchAPrime
	pair, err := newM6Network(g, short, everybody)
	if err != nil {
		t.Fatalf("preparing the short paired base: %v", err)
	}
	paired, err := pair.Run()
	if err != nil {
		t.Fatalf("running the short paired base: %v", err)
	}

	known := map[string][]m6PhaseBoundary{pairingKey(alpha): paired.PhaseBoundaries()}
	boundaries, extra, err := m6BoundariesFor(g, alpha, known)
	if err != nil {
		t.Fatalf("pairing the α control with its base: %v", err)
	}
	if len(boundaries) == 0 {
		t.Fatal("the pairing produced no boundaries")
	}
	if extra != "" {
		t.Errorf("a pairing taken from the cache reported an extra run: %q", extra)
	}

	control := short
	control.StartEmpty = true
	control.ReplayPhases = boundaries
	if _, err := newM6Network(g, control, everybody); err != nil {
		t.Errorf("the α control with its pair's boundaries is refused: %v", err)
	}

	// Nothing else replays: the whole grid plays its OWN boundaries (decision
	// 3.3(в)), because churn draws are keyed on τ = tick − onset.
	for _, variant := range append(m6GridVariants(), m6DeltaVariant()) {
		other := m6SweepConfig{Variant: variant, Population: m6WholeNetwork, Shape: sh, Seed: 1}
		spans, _, err := m6BoundariesFor(g, other, map[string][]m6PhaseBoundary{})
		if err != nil {
			t.Fatalf("%s: %v", variant.Name, err)
		}
		if spans != nil {
			t.Errorf("%s is pinned to another run's boundaries; only the α control is", variant.Name)
		}
	}
}

// TestM6ThePairingMarkersAreMarkersAndNotNames: the control is paired with the
// base through TYPED FLAGS, because the alternatives both break silently — a
// comparison on the name unpairs the control the day the label changes (and the
// label is part of every run's file name), and an index into the slice re-pairs
// it the day a variant is inserted before it.
func TestM6ThePairingMarkersAreMarkersAndNotNames(t *testing.T) {
	t.Parallel()

	bases, replays := 0, 0
	for _, variant := range m6AllVariants() {
		if variant.PairsAsBase {
			bases++
			if variant.Kind != m6Grid {
				t.Errorf("%q is marked as the base and is a %s: the base is a grid configuration",
					variant.Name, variant.Kind)
			}
		}
		if variant.ReplaysTheBase {
			replays++
			if variant.Kind != m6ControlBeyondGrid {
				t.Errorf("%q replays the base and is a %s: only the α control does",
					variant.Name, variant.Kind)
			}
		}
	}
	if bases != 1 {
		t.Errorf("%d configurations are marked as the base; exactly one is", bases)
	}
	if replays != 1 {
		t.Errorf("%d configurations replay the base; exactly one does (decision 3.3(в))", replays)
	}
	if got := m6BaseVariant(); !got.PairsAsBase {
		t.Fatal("m6BaseVariant did not find the base")
	}
	// And the base really is the base configuration: it changes nothing.
	unchanged := m6GridBase(sweepShapes[0], 1)
	changed := unchanged
	m6BaseVariant().Apply(&changed)
	if changed.String() != unchanged.String() {
		t.Error("the configuration marked as the base departs from the base")
	}
}

// TestM6TheRecoveryPhraseSeparatesAbsenceFromFailure: "nothing was lost inside
// the window" and "the losses were not made good" are different results, and
// folding them into one line made the α control read as a failure of recovery.
func TestM6TheRecoveryPhraseSeparatesAbsenceFromFailure(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		name   string
		window *m6RecoveryWindow
		want   string
	}{
		{"no window", nil, "no window asked for"},
		{"recovered", &m6RecoveryWindow{Ticks: 1024, LostEvents: 5, RecoveredAtTau: 17}, "recovery at τ=17"},
		{"nothing to recover from", &m6RecoveryWindow{Ticks: 1024, LostEvents: 0, RecoveredAtTau: -1},
			"NOT APPLICABLE"},
		{"not reached", &m6RecoveryWindow{Ticks: 1024, LostEvents: 5, RecoveredAtTau: -1}, "NOT REACHED"},
	} {
		if got := m6RecoveryPhrase(fixture.window); !strings.Contains(got, fixture.want) {
			t.Errorf("%s: the line reads %q and does not say %q", fixture.name, got, fixture.want)
		}
	}
	// ⚠️ And the two negative cases must not read alike, or the distinction is
	// only in the source.
	absent := m6RecoveryPhrase(&m6RecoveryWindow{Ticks: 1024, LostEvents: 0, RecoveredAtTau: -1})
	failed := m6RecoveryPhrase(&m6RecoveryWindow{Ticks: 1024, LostEvents: 5, RecoveredAtTau: -1})
	if absent == failed {
		t.Error("a window with nothing lost reads exactly like one whose losses were not made good")
	}
}

// TestM6TheDeltaHalvesReadOneStream is the property the task names for δ: the
// INPUT STREAM OF BOTH HALVES IS IDENTICAL. Without it the pair answers "what
// does memory buy" with two different questions and the difference means
// nothing.
func TestM6TheDeltaHalvesReadOneStream(t *testing.T) {
	t.Parallel()

	// A short plan: the property is about what the two halves are fed, not about
	// the contract's tick counts.
	sh := sweepShapes[0]
	model := m6GridBase(sh, 1)
	model.Phases = &m6PhasePlan{FillTicks: 10, IdleTicks: 4, RecoveryTicks: 10, CadenceTicks: 6}
	model.RecoveryWindow = 8
	model.Membership = m6WholeNetwork.String()
	g := buildGraph(sh, 1, m6GridQuota, policyCandidateC1)
	border, rule, err := referencePool(g, model, everybody)
	if err != nil {
		t.Fatalf("deriving the border: %v", err)
	}
	model.NearFrom, model.NearFromRule = border, rule

	outcomes, err := runM6DeltaPair(g, model, everybody)
	if err != nil {
		t.Fatalf("running the δ pair: %v", err)
	}
	if len(outcomes.Reports) != 2 {
		t.Fatalf("a δ pair produced %d reports, want two halves", len(outcomes.Reports))
	}
	kept, cleared := outcomes.Reports[0], outcomes.Reports[1]

	// ⚠️ ONE object, not two equal ones: two recordings that happen to agree
	// today would be two recordings tomorrow.
	if kept.Config.Stream == nil || cleared.Config.Stream == nil {
		t.Fatal("a half of the δ pair read no recorded stream at all")
	}
	if kept.Config.Stream != cleared.Config.Stream {
		t.Fatal("the two halves were given two recordings: the pair would answer two questions")
	}
	if kept.Config.Stream.fingerprint() != cleared.Config.Stream.fingerprint() {
		t.Fatal("the halves' recordings do not fingerprint alike")
	}

	// One half keeps its memory, the other is cleared — the ONE difference.
	if kept.Config.StartEmpty || !cleared.Config.StartEmpty {
		t.Errorf("the halves are %t/%t on StartEmpty; the pair is memory kept against memory "+
			"cleared", kept.Config.StartEmpty, cleared.Config.StartEmpty)
	}

	// And the comparator agrees the stream was shared and unmodified.
	if !outcomes.Comparison.Holds(claimSameRecordedStream) {
		t.Errorf("the comparator does not hold the one-stream claim:\n%s", outcomes.Comparison)
	}
	if !outcomes.Comparison.Holds(claimSameInputs) {
		t.Errorf("the two halves differ in more than the memory mode:\n%s", outcomes.Comparison)
	}
	// ⚠️ A replay must not ask a responder: then the recording is no longer the
	// one thing both halves read. compareM6Runs fails the stream claim for it,
	// which the assertion above already covers; this states the reason in the
	// numbers too.
	for name, report := range map[string]*m6ModelReport{"kept": kept, "cleared": cleared} {
		if report.ExchangesDone > 0 || report.AddressedAnswers > 0 {
			t.Errorf("the %s half performed %d exchanges and %d addressed answers beside the "+
				"recording", name, report.ExchangesDone, report.AddressedAnswers)
		}
	}

	// The stored record has to carry both halves and the verdicts, or the file
	// would be unreadable as a pair.
	body := strings.Join(m6OutcomeBody(m6SweepConfig{
		Variant: m6DeltaVariant(), Population: m6WholeNetwork, Shape: sh, Seed: 1,
	}, outcomes), "\n")
	for _, want := range []string{"kept.", "cleared.", "pair scenario_runs=2", "claim "} {
		if !strings.Contains(body, want) {
			t.Errorf("the stored δ record does not carry %q", want)
		}
	}
}

// TestM6TheDeltaGateOnBigShapesIsNamedNotDropped: an unresolved point of the
// registry is recorded as a refusal with its reason, so no ledger can report a
// complete grid.
func TestM6TheDeltaGateOnBigShapesIsNamedNotDropped(t *testing.T) {
	t.Parallel()

	small := m6SweepConfig{Variant: m6DeltaVariant(), Population: m6WholeNetwork,
		Shape: sweepShapes[0], Seed: 1}
	if gate := m6GateFor(small); gate != "" {
		t.Errorf("δ on %s is gated: %s", small.Shape.name, gate)
	}

	big := small
	big.Shape = sweepShapes[1]
	gate := m6GateFor(big)
	if gate == "" {
		t.Fatalf("δ on %s is not gated: the comparator needs the offer trace of BOTH replaying "+
			"halves, ≈3 GB each, which registry §5.2.2 names as open", big.Shape.name)
	}
	if !strings.Contains(gate, "5.2.2") || !strings.Contains(gate, "trace") {
		t.Errorf("the gate does not name the open point it comes from: %q", gate)
	}

	// Nothing else is gated: a gate that caught grid runs would silently shrink
	// the 400.
	for _, variant := range append(m6GridVariants(), m6ControlVariants()...) {
		for _, sh := range m6SweepShapes() {
			config := m6SweepConfig{Variant: variant, Population: m6WholeNetwork, Shape: sh, Seed: 1}
			if gate := m6GateFor(config); gate != "" {
				t.Errorf("%s on %s is gated (%s): the grid and its controls are not cut by this "+
					"driver", variant.Name, sh.name, gate)
			}
		}
	}
}

// TestM6AStoredRunCarriesTheThreeAxesApart: coverage, cost and recovery are
// three results and are never averaged into one another.
func TestM6AStoredRunCarriesTheThreeAxesApart(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m6GridQuota, policyCandidateC1)
	config := m6SweepConfig{Variant: m6GridVariants()[0], Population: m6WholeNetwork,
		Shape: sh, Seed: 1}
	model, err := config.config(g)
	if err != nil {
		t.Fatalf("building the configuration: %v", err)
	}
	// A short plan keeps the fixture a fixture.
	model.Phases = &m6PhasePlan{FillTicks: 10, IdleTicks: 4, RecoveryTicks: 10, CadenceTicks: 6}
	model.RecoveryWindow = 8

	network, err := newM6Network(g, model, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}

	body := strings.Join(m6RunBody("", report), "\n")
	for _, want := range []string{
		"phases ", "near_from level=", "coverage_at_churn claimed=", "coverage_level_0 ",
		"probes measured=", "physical lost=", "detection count=", "recovery lost=",
		"churn departed=", "arrivals returns_offered=", "exchange done=", "addressed answers=",
		"pool_at_start ", "window from=", "window_frames ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the stored run does not carry %q:\n%s", want, body)
		}
	}
	// ⚠️ An absent distribution reads as absent, never as zero: "no losses were
	// detected" and "the median delay is 0" are different facts.
	if got := quantiles(nil); !strings.Contains(got, "no_data") {
		t.Errorf("an empty distribution stores as %q", got)
	}

	// ⚠️ EVERY window field is checked BY VALUE, not by the presence of a line
	// (P2 of 2026-09-20). The previous fixture asked only whether the words
	// "window from=" appeared, so six counters could be — and were — dropped
	// from the record without a single test going red.
	window := report.Window
	if window == nil {
		t.Fatal("the run produced no recovery window, so this fixture checks nothing")
	}
	for field, want := range map[string]int{
		"lost_events":             window.LostEvents,
		"refilled_events":         window.RefilledEvents,
		"unique_lost":             window.UniqueLostRecords,
		"unique_refilled":         window.UniqueRefilledRecords,
		"detections":              window.DetectionEvents,
		"probes":                  window.Probes.Probes(),
		"refreshes":               window.Refreshes,
		"residual_online":         window.ResidualDeficit,
		"residual_offline":        window.ResidualDeficitOffline,
		"undetected_dead":         window.UndetectedDeadRecords,
		"exchanges_served":        window.ExchangesServed,
		"exchanges_refused":       window.ExchangesRefused,
		"addressed_answers":       window.AddressedAnswers,
		"addressed_rate_limited":  window.AddressedRateLimited,
		"addressed_refused":       window.AddressedRefused,
		"repeats_filtered":        window.RepeatsFiltered,
	} {
		if got := storedField(body, field); got != fmt.Sprint(want) {
			t.Errorf("the record stores %s=%s and the window counted %d", field, got, want)
		}
	}
}

// storedField reads one `name=value` from a stored record body.
func storedField(body, name string) string {
	match := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `=(\S+)`).FindStringSubmatch(body)
	if match == nil {
		return "ABSENT"
	}
	return match[1]
}

// TestM6TheWindowFramesAreTheWindowsAndNotTheWholeRuns is the other half of the
// P2 of 2026-09-20: the six frame counters must come from the WINDOW's ledger.
//
// ⚠️ Storing the whole-run totals under the window's names would pass every
// "the line is present" check and would be wrong in exactly the way the
// contract's own note says: the run's length differs between configurations, so
// its totals are not comparable over the same 1024 ticks. This fixture is red
// under that substitution because it demands the two DIFFER on a run where they
// must.
func TestM6TheWindowFramesAreTheWindowsAndNotTheWholeRuns(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m6GridQuota, policyCandidateC1)
	// Branch A′ exchanges records, so the frame counters are non-zero; the
	// window opens at the churn onset, so the exchanges of the filling phase
	// are outside it and the two totals must part.
	config := m6SweepConfig{Variant: m6BaseVariant(), Population: m6WholeNetwork, Shape: sh, Seed: 1}
	model, err := config.config(g)
	if err != nil {
		t.Fatalf("building the configuration: %v", err)
	}
	model.Phases = &m6PhasePlan{FillTicks: 12, IdleTicks: 4, RecoveryTicks: 10, CadenceTicks: 6}
	model.RecoveryWindow = 8

	network, err := newM6Network(g, model, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	report, err := network.Run()
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	window := report.Window
	if window == nil {
		t.Fatal("no recovery window")
	}
	if report.ExchangesDone == 0 {
		t.Fatal("the run served no exchange at all, so window and total cannot be told apart here")
	}
	if window.ExchangesServed == report.ExchangesDone {
		t.Errorf("the window counted every exchange of the run (%d): the window opens at the "+
			"onset, so the filling phase's exchanges are outside it — this reads as the whole-run "+
			"total wearing the window's name", window.ExchangesServed)
	}

	body := strings.Join(m6RunBody("", report), "\n")
	if got := storedField(body, "exchanges_served"); got != fmt.Sprint(window.ExchangesServed) {
		t.Errorf("the record stores exchanges_served=%s, the window counted %d, the whole run %d",
			got, window.ExchangesServed, report.ExchangesDone)
	}
	// And the whole-run counters keep their own line, so both are readable and
	// neither is mistaken for the other.
	if got := storedField(body, "done"); got != fmt.Sprint(report.ExchangesDone) {
		t.Errorf("the whole-run exchange total stores as %s, want %d", got, report.ExchangesDone)
	}
}
