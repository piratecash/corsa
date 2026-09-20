package overlaysim

// m4_sweep_reference_test.go pins the M4 DRIVER — the enumeration it runs and
// the properties of one measured configuration. The instrument underneath it is
// pinned by m4_reference_test.go and is not re-checked here; what is checked is
// the thing that did not exist before this round: the loop that decides WHICH
// configurations are measured and puts the agreed loads to them.
//
// ⚠️ Mutations that must break these fixtures:
//
//	a load dropped from the enumeration (or a fourth added) — the composition
//	    fixture;
//	the k = 1 of the third load applied to all three, or to none — the guard
//	    model fixture;
//	the candidate dropped from the policy pair, or the base dropped — the
//	    composition fixture;
//	only one population accumulated — the both-populations fixture;
//	the two populations accumulated over different workloads — the same;
//	the measurement writing into the guard set (a NoteUsed, a rotation, a
//	    top-up) — the set-unchanged fixture;
//	the guard model left out of the run key — the identifier fixture.

import (
	"strings"
	"testing"
)

// TestM4EnumerationIsTheAgreedGrid is the composition, counted and named.
//
// ⚠️ Counted AND named. A count alone passes when one load is measured twice and
// another not at all, which is the shape of the mistake the M3-a proposal
// already made once (the sweep that quietly ran the base instead of the
// candidate).
func TestM4EnumerationIsTheAgreedGrid(t *testing.T) {
	t.Parallel()

	configs := m4SweepEnumeration([]shape{m4SweepShape()})

	if got, want := len(configs), 3*len(sweepSeeds)*len(m4SweepPolicies); got != want {
		t.Fatalf("the enumeration holds %d configurations, want %d = 3 loads × %d seeds × %d rules",
			got, want, len(sweepSeeds), len(m4SweepPolicies))
	}

	loads := map[m4SweepLoad]int{}
	policies := map[policy]int{}
	seeds := map[uint64]int{}
	for _, config := range configs {
		loads[config.Load]++
		policies[config.Policy]++
		seeds[config.Seed]++
		if config.Shape.name != m4SweepShape().name {
			t.Fatalf("a configuration is on %s, the sweep asked for %s",
				config.Shape.name, m4SweepShape().name)
		}
	}

	// The three loads §5.5.4 names, each exactly as often as the others.
	for _, load := range []m4SweepLoad{m4LoadUniform, m4LoadPopular, m4LoadMainContactInSet} {
		if got, want := loads[load], len(sweepSeeds)*len(m4SweepPolicies); got != want {
			t.Errorf("the load %q appears %d times, want %d — the three declared loads are "+
				"measured on the same inputs or their shares cannot be compared", load, got, want)
		}
	}
	if len(loads) != 3 {
		t.Errorf("the sweep runs %d loads; §5.5.4 declares three", len(loads))
	}

	// ⚠️ BOTH rules, and the candidate FIRST: the acceptance numbers are the
	// candidate's, and the base is run on identical inputs so a difference has
	// somewhere to come from. A sweep of the base alone would describe another
	// graph (index §0.2 — measurements are tied to the candidate).
	if m4SweepPolicies[0] != policyCandidateC1 {
		t.Errorf("the first rule of the pair is %s; the candidate's numbers are the acceptance "+
			"numbers and it comes first", m4SweepPolicies[0])
	}
	if policies[policyCandidateC1] == 0 || policies[policyInitiatedLimit] == 0 {
		t.Errorf("the pair is %v: the candidate and its one-rule-apart base are both required",
			m4SweepPolicies)
	}
	if policies[policyBaseline] != 0 {
		t.Error("baseline is in the M4 grid: its role is the golden control of the stand " +
			"(registry §2.1), not a comparand")
	}
	for _, seed := range sweepSeeds {
		if seeds[seed] == 0 {
			t.Errorf("seed %d is not measured: one graph is an anecdote", seed)
		}
	}

	// The ordering contract of _RANGE: the three loads of one (policy, seed) are
	// adjacent, so a contiguous range builds the fewest graphs.
	for index := 0; index+2 < len(configs); index += 3 {
		first := configs[index]
		for offset := 1; offset < 3; offset++ {
			next := configs[index+offset]
			if next.Seed != first.Seed || next.Policy != first.Policy {
				t.Fatalf("configurations %d…%d do not share a graph: a range would rebuild it",
					index, index+2)
			}
		}
	}
}

// TestM4TheDegenerateLoadCarriesItsOwnGuardModel: k = 1 IS the third load, not a
// target distribution measured under the ordinary model.
func TestM4TheDegenerateLoadCarriesItsOwnGuardModel(t *testing.T) {
	t.Parallel()

	if got := m4LoadMainContactInSet.guards().SetSize; got != 1 {
		t.Errorf("the degenerate load is measured with k = %d; §5.5.4 says the main contact is "+
			"inside the set AT k = 1, which is what makes the refusal deterministic", got)
	}
	for _, load := range []m4SweepLoad{m4LoadUniform, m4LoadPopular} {
		if got, want := load.guards().SetSize, m4SweepGuards().SetSize; got != want {
			t.Errorf("the load %q is measured with k = %d and the model says %d: the ordinary "+
				"loads share one set size or their refusal shares are not comparable",
				load, got, want)
		}
	}
	// And the model is otherwise untouched, or two loads would differ in more
	// than the one thing that names them.
	degenerate, ordinary := m4LoadMainContactInSet.guards(), m4SweepGuards()
	if degenerate.Requests != ordinary.Requests || degenerate.TargetSeed != ordinary.TargetSeed {
		t.Error("the degenerate load changed the request count or the target seed as well as k")
	}
}

// TestM4RunKeyCarriesEveryInput: a parameter outside the key is a parameter a
// resumed sweep will not notice has changed.
func TestM4RunKeyCarriesEveryInput(t *testing.T) {
	t.Parallel()

	base := m4SweepConfig{Shape: m4SweepShape(), Seed: 1, Policy: policyCandidateC1, Load: m4LoadUniform}
	id := base.Key("v").ID()

	for _, changed := range []struct {
		name   string
		config m4SweepConfig
	}{
		{"shape", m4SweepConfig{Shape: sweepShapes[0], Seed: 1, Policy: policyCandidateC1, Load: m4LoadUniform}},
		{"seed", m4SweepConfig{Shape: m4SweepShape(), Seed: 2, Policy: policyCandidateC1, Load: m4LoadUniform}},
		{"policy", m4SweepConfig{Shape: m4SweepShape(), Seed: 1, Policy: policyInitiatedLimit, Load: m4LoadUniform}},
		{"load", m4SweepConfig{Shape: m4SweepShape(), Seed: 1, Policy: policyCandidateC1, Load: m4LoadPopular}},
		{"load (k=1)", m4SweepConfig{Shape: m4SweepShape(), Seed: 1, Policy: policyCandidateC1, Load: m4LoadMainContactInSet}},
	} {
		if changed.config.Key("v").ID() == id {
			t.Errorf("changing the %s left the identifier at %s: the two runs would share a file "+
				"and the second would be skipped as the first", changed.name, id)
		}
	}

	// The guard model has to be IN the key, or a run made under another set size
	// would be resumed as this one.
	key := base.Key("v")
	for _, want := range []string{"guard_set_size_k", "guard_confirmed_prefix",
		"requests_per_requester", "target_seed", "requesters", "requester_seed", "quota"} {
		if _, ok := key.Param(want); !ok {
			t.Errorf("the run key does not carry %s", want)
		}
	}
	// And the popular load's head, which nothing else records.
	popular := m4SweepConfig{Shape: m4SweepShape(), Seed: 1, Policy: policyCandidateC1, Load: m4LoadPopular}
	for _, want := range []string{"head_size", "head_share"} {
		if _, ok := popular.Key("v").Param(want); !ok {
			t.Errorf("the popular-contacts key does not carry %s: the head is the shape of the "+
				"load and a run under another head is another measurement", want)
		}
	}

	if err := requireDistinctKeys(func() []runKey {
		keys := make([]runKey, 0, 30)
		for _, config := range m4SweepEnumeration([]shape{m4SweepShape()}) {
			keys = append(keys, config.Key("v"))
		}
		return keys
	}()); err != nil {
		t.Fatalf("the enumeration collides: %v", err)
	}
}

// TestM4AConfigurationReportsBothPopulationsOverTheSameRequests is the property
// the whole driver exists for: the open sub-question of §4.3.4″.3 is MEASURED,
// twice, on one workload — and never answered by omission.
func TestM4AConfigurationReportsBothPopulationsOverTheSameRequests(t *testing.T) {
	t.Parallel()

	// A cheap form: the property is about the loop, not about the network.
	config := m4SweepConfig{Shape: sweepShapes[0], Seed: 1, Policy: policyCandidateC1, Load: m4LoadUniform}
	g := buildGraph(config.Shape, config.Seed, m4SweepQuota, config.Policy)

	run, err := measureM4Configuration(g, config)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	if run.RequestersMeasured != m4SweepRequesters {
		t.Fatalf("%d requesters measured, %d asked for", run.RequestersMeasured, m4SweepRequesters)
	}
	if run.Confirmed.Requests != run.Sampled.Requests {
		t.Fatalf("the two readings answered %d and %d requests: their difference would be a "+
			"difference in what was asked", run.Confirmed.Requests, run.Sampled.Requests)
	}
	want := m4SweepRequesters * m4SweepGuards().Requests
	if run.Confirmed.Requests != want {
		t.Errorf("%d requests per population, want %d = %d requesters × %d requests",
			run.Confirmed.Requests, want, m4SweepRequesters, m4SweepGuards().Requests)
	}
	for name, slice := range map[string]m4Slice{"confirmed": run.Confirmed, "whole set": run.Sampled} {
		if slice.Served+slice.Refused() != slice.Requests {
			t.Errorf("%s: served %d + refused %d ≠ %d requests — the denominator is not the "+
				"requests put to the rule", name, slice.Served, slice.Refused(), slice.Requests)
		}
	}

	// ⚠️ The two readings must be able to DIFFER, or the fixture would pass on a
	// driver that accumulated one of them twice. Under the uniform load they may
	// legitimately coincide, so the degenerate probe is used: with k = 1 and the
	// target inside the set, the confirmed reading and the whole-set reading are
	// forced apart only when the set holds an unconfirmed spare — under k = 3 it
	// does.
	forced := m4SweepConfig{Shape: sweepShapes[0], Seed: 1, Policy: policyCandidateC1,
		Load: m4LoadMainContactInSet}
	degenerate, err := measureM4Configuration(g, forced)
	if err != nil {
		t.Fatalf("measuring the degenerate load: %v", err)
	}
	if degenerate.Confirmed.Refused() != degenerate.Confirmed.Requests {
		t.Errorf("under \"the only guard is the target\" the refusal is %s, and §5.5.4 says it is "+
			"total", degenerate.Confirmed.RefusalShare())
	}
	if degenerate.Confirmed.RefusedNobodySuitable != 0 {
		t.Errorf("%d refusals were attributed to an empty set: with k = 1 and a suitable member "+
			"that IS the target, every refusal belongs to the exclusion (S24а)",
			degenerate.Confirmed.RefusedNobodySuitable)
	}

	// The body the journal stores has to carry both readings and the gap, or the
	// file would settle the open question the log refused to settle.
	body := strings.Join(run.Body(), "\n")
	for _, want := range []string{"confirmed_only requests=", "whole_set requests=", "gap_pp=",
		"bound=lower"} {
		if !strings.Contains(body, want) {
			t.Errorf("the stored run does not carry %q:\n%s", want, body)
		}
	}
}

// TestM4TheMeasurementDoesNotTouchTheGuardSets is rule 2 of §4.3.4″.3 applied to
// the DRIVER: the set is not changed and not topped up, not for a request and
// not after it. The instrument is checked for this in m4_reference_test.go; here
// the loop around it is, because a driver is where a NoteUsed would be added.
func TestM4TheMeasurementDoesNotTouchTheGuardSets(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m4SweepQuota, policyCandidateC1)
	model := m4SweepGuards()

	requesters := sampleRequesters(sh.nodes, 8, m4SweepRequesterSeed)
	before := make([]string, 0, len(requesters))
	for _, requester := range requesters {
		before = append(before, describeSet(g, buildGuardSet(g, requester, model)))
	}

	if _, err := measureM4Configuration(g, m4SweepConfig{
		Shape: sh, Seed: 1, Policy: policyCandidateC1, Load: m4LoadUniform,
	}); err != nil {
		t.Fatalf("measuring: %v", err)
	}

	for index, requester := range requesters {
		after := describeSet(g, buildGuardSet(g, requester, model))
		if after != before[index] {
			t.Errorf("node %d's guard set changed across the measurement:\n  before %s\n  after  %s",
				requester, before[index], after)
		}
	}
}

// TestM4TheSetsDigestSeesTheComposition: the token that lets two runs be
// compared has to move when the sets move, and stay when they do not.
func TestM4TheSetsDigestSeesTheComposition(t *testing.T) {
	t.Parallel()

	sh := sweepShapes[0]
	g := buildGraph(sh, 1, m4SweepQuota, policyCandidateC1)
	other := buildGraph(sh, 2, m4SweepQuota, policyCandidateC1)

	same := func(graph *graph) string {
		run, err := measureM4Configuration(graph, m4SweepConfig{
			Shape: sh, Seed: 1, Policy: policyCandidateC1, Load: m4LoadUniform,
		})
		if err != nil {
			t.Fatalf("measuring: %v", err)
		}
		return run.SetsDigest
	}

	once, twice := same(g), same(g)
	if once != twice {
		t.Fatalf("the digest of one run's sets is not reproducible: %s then %s", once, twice)
	}
	if same(g) == same(other) {
		t.Error("two graphs produced the same sets digest: the token cannot tell two experiments " +
			"apart, and the gap between the populations is a fact about the members")
	}

	// ⚠️ Order is part of it: the order decides who carries a served request.
	first := newSetsDigest()
	first.add(snapshotGuardSet(g, buildGuardSet(g, 0, m4SweepGuards())))
	first.add(snapshotGuardSet(g, buildGuardSet(g, 1, m4SweepGuards())))
	second := newSetsDigest()
	second.add(snapshotGuardSet(g, buildGuardSet(g, 1, m4SweepGuards())))
	second.add(snapshotGuardSet(g, buildGuardSet(g, 0, m4SweepGuards())))
	if first.digest() == second.digest() {
		t.Error("the digest ignores the order the sets were measured in")
	}
}
