package overlaysim

// m3a_sweep_reference_test.go pins the M3-a DRIVER: the sweep it walks and what
// one point of it stores. The scenario underneath — the rejection draw, the
// shuffle, the requester sample, the two aggregates — is pinned by
// m3a_reference_test.go and is not re-checked here.
//
// ⚠️ Mutations that must break these fixtures:
//
//	the degenerate shares 0 and 1 put back into the sweep — the range fixture
//	    (they are reference cases, not points of sensitivity: at Q = 0 there is
//	    nothing to connect and a criterion reading that as one component would
//	    call the worst population a success);
//	the step changed, or an endpoint dropped — the same;
//	one of the two loads dropped — the composition fixture (and with it the
//	    only load under which the two readings of §4.3.4″.3 are forced apart);
//	the base run instead of the candidate, or the candidate dropped — the
//	    composition fixture;
//	the share left out of the run key, or rounded differently in the key than
//	    in the report — the identifier fixture;
//	one aggregate stored and the other dropped — the stored-point fixture.

import (
	"math"
	"strings"
	"testing"
)

// TestM3ASweepIsTheAgreedRange is what the owner agreed for the stand on
// 2026-09-16: the bounds, the step, the form, the seeds, the requesters.
func TestM3ASweepIsTheAgreedRange(t *testing.T) {
	t.Parallel()

	sweep := m3aAgreedSweep()
	points := sweep.Points()

	if got := len(points); got != 19 {
		t.Fatalf("the sweep visits %d shares, the agreed range is 19 points (0.05…0.95 step 0.05)",
			got)
	}
	if math.Abs(points[0]-0.05) > 1e-9 || math.Abs(points[len(points)-1]-0.95) > 1e-9 {
		t.Errorf("the range runs %.3f…%.3f, agreed 0.05…0.95", points[0], points[len(points)-1])
	}
	for index := 1; index < len(points); index++ {
		if step := points[index] - points[index-1]; math.Abs(step-0.05) > 1e-9 {
			t.Errorf("the step between %.3f and %.3f is %.3f, agreed 0.05",
				points[index-1], points[index], step)
		}
	}
	// ⚠️ The degenerate ends are NOT points of the sweep. They are reference
	// cases (§5.5.5): at Q = 0 there is no structural plane to be sensitive, and
	// a sweep that included them would report "no data" as a measurement.
	for _, share := range points {
		if share <= 0 || share >= 1 {
			t.Errorf("the sweep visits the degenerate share %.3f", share)
		}
	}

	if len(sweep.Shapes) != 1 || sweep.Shapes[0].name != "10k×8" {
		t.Errorf("the sweep runs on %v, agreed 10k×8", sweep.Shapes)
	}
	if len(sweep.Seeds) != len(sweepSeeds) {
		t.Errorf("the sweep runs %d seeds, the stand's draws are %v", len(sweep.Seeds), sweepSeeds)
	}
	if sweep.Requesters != 200 {
		t.Errorf("%d requesters per point, agreed 200", sweep.Requesters)
	}
	if sweep.Quota != 1 {
		t.Errorf("quota %d, the proposal names 1", sweep.Quota)
	}

	// ⚠️ BOTH loads. The second one — "every request aims at the requester's
	// confirmed guard" — is the only load under which the confirmed-only and the
	// whole-set readings are FORCED apart; without it the open sub-question of
	// §4.3.4″.3 is not measurable at all.
	if len(sweep.Workloads) != 2 {
		t.Fatalf("the sweep runs %d loads, the contract names two", len(sweep.Workloads))
	}
	loads := map[skewWorkload]bool{}
	for _, load := range sweep.Workloads {
		loads[load] = true
	}
	if !loads[skewUniformTargets] || !loads[skewTargetsTheConfirmedGuard] {
		t.Errorf("the loads are %v; the neutral one and the one that forces the two readings "+
			"apart are both required", sweep.Workloads)
	}

	// ⚠️ The candidate FIRST: the acceptance numbers are its, and the base is
	// run on identical inputs so a difference has somewhere to come from.
	if len(sweep.Policies) != 2 || sweep.Policies[0] != policyCandidateC1 {
		t.Errorf("the rules are %v; the candidate's numbers are the acceptance numbers and it "+
			"comes first, with initiated-limit on the same inputs", sweep.Policies)
	}
	if sweep.Policies[1] != policyInitiatedLimit {
		t.Errorf("the comparison base is %s, the candidate differs by ONE rule from "+
			"initiated-limit", sweep.Policies[1])
	}

	// The three seeds of the draw are kept apart, which is what stops a skew
	// being measured together with an ordering (§5.5.5).
	if sweep.ShuffleSeed == sweep.RequesterSeed {
		t.Error("the order seed and the requester seed are the same value: the two biases the " +
			"contract separates would move together")
	}
}

// TestM3AEnumerationCoversTheGridAndKeepsLoadsAdjacent is the driver's own
// contract: 19 × 5 × 2 × 2, and an order a range can cut.
func TestM3AEnumerationCoversTheGridAndKeepsLoadsAdjacent(t *testing.T) {
	t.Parallel()

	sweep := m3aAgreedSweep()
	configs := m3aSweepEnumeration(sweep)

	want := len(sweep.Points()) * len(sweep.Seeds) * len(sweep.Workloads) * len(sweep.Policies)
	if len(configs) != want {
		t.Fatalf("%d points enumerated, want %d = 19 shares × %d seeds × %d loads × %d rules",
			len(configs), want, len(sweep.Seeds), len(sweep.Workloads), len(sweep.Policies))
	}
	if want != 380 {
		t.Fatalf("the grid is %d points; the registry §4.1 counts 380", want)
	}

	// Every (share, seed, policy) is measured under BOTH loads: a share
	// measured under one load and not the other says nothing about the open
	// sub-question, so the pair has to be adjacent for a partial sweep to be
	// readable.
	for index := 0; index+1 < len(configs); index += 2 {
		first, second := configs[index], configs[index+1]
		if first.Share != second.Share || first.Seed != second.Seed || first.Policy != second.Policy {
			t.Fatalf("points %d and %d are not the two loads of one graph: %v vs %v",
				index, index+1, first, second)
		}
		if first.Load == second.Load {
			t.Fatalf("points %d and %d run the same load twice", index, index+1)
		}
	}

	// And the expensive rule is the outer axis, so a range can be sized: one
	// C1/v1 graph on 10k×8 costs ≈6 s and its base ≈27 ms.
	half := len(configs) / 2
	for index, config := range configs {
		wantPolicy := sweep.Policies[0]
		if index >= half {
			wantPolicy = sweep.Policies[1]
		}
		if config.Policy != wantPolicy {
			t.Fatalf("point %d runs %s; the rules are meant to be contiguous blocks so a range "+
				"can be sized to one of them", index, config.Policy)
		}
	}
}

// TestM3ARunKeyCarriesEveryInput: the share, the seeds, the guard model.
func TestM3ARunKeyCarriesEveryInput(t *testing.T) {
	t.Parallel()

	base := m3aSweepConfig{Shape: sweepShapes[1], Seed: 1, Share: 0.25,
		Policy: policyCandidateC1, Load: skewUniformTargets}
	id := base.Key("v").ID()

	for _, changed := range []struct {
		name   string
		config m3aSweepConfig
	}{
		{"share", m3aSweepConfig{Shape: sweepShapes[1], Seed: 1, Share: 0.30, Policy: policyCandidateC1, Load: skewUniformTargets}},
		{"seed", m3aSweepConfig{Shape: sweepShapes[1], Seed: 2, Share: 0.25, Policy: policyCandidateC1, Load: skewUniformTargets}},
		{"policy", m3aSweepConfig{Shape: sweepShapes[1], Seed: 1, Share: 0.25, Policy: policyInitiatedLimit, Load: skewUniformTargets}},
		{"load", m3aSweepConfig{Shape: sweepShapes[1], Seed: 1, Share: 0.25, Policy: policyCandidateC1, Load: skewTargetsTheConfirmedGuard}},
		{"shape", m3aSweepConfig{Shape: sweepShapes[0], Seed: 1, Share: 0.25, Policy: policyCandidateC1, Load: skewUniformTargets}},
	} {
		if changed.config.Key("v").ID() == id {
			t.Errorf("changing the %s left the identifier at %s", changed.name, id)
		}
	}

	key := base.Key("v")
	for _, want := range []string{"requested_q_share", "shuffle_seed", "requesters",
		"requester_seed", "guard_set_size_k", "guard_confirmed_prefix", "requests_per_requester",
		"target_seed", "quota"} {
		if _, ok := key.Param(want); !ok {
			t.Errorf("the run key does not carry %s: a run made under another value would be "+
				"resumed as this one", want)
		}
	}
	// ⚠️ The share is rendered by ONE function, so the key and the report spell
	// it the same way and can be joined.
	if got, _ := key.Param("requested_q_share"); got != shareToken(0.25) {
		t.Errorf("the key spells the share %q and the report spells it %q", got, shareToken(0.25))
	}

	keys := make([]runKey, 0, 380)
	for _, config := range m3aSweepEnumeration(m3aAgreedSweep()) {
		keys = append(keys, config.Key("v"))
	}
	if err := requireDistinctKeys(keys); err != nil {
		t.Fatalf("the enumeration collides: %v", err)
	}
	// ⚠️ And the float shares must not collide through their rendering: 19
	// distinct shares, 19 distinct tokens.
	tokens := map[string]bool{}
	for _, share := range m3aAgreedSweep().Points() {
		tokens[shareToken(share)] = true
	}
	if len(tokens) != 19 {
		t.Errorf("the 19 shares render to %d distinct tokens: two points would share a file",
			len(tokens))
	}
}

// TestM3AAStoredPointKeepsBothReadings is what the journal owes: a point whose
// file settles the open question the log refused to settle would be worse than
// no file.
func TestM3AAStoredPointKeepsBothReadings(t *testing.T) {
	t.Parallel()

	// A cheap form: the property is about what the driver stores, not about the
	// 10k network.
	config := m3aSweepConfig{Shape: sweepShapes[0], Seed: 1, Share: 0.25,
		Policy: policyCandidateC1, Load: skewTargetsTheConfirmedGuard}
	run, err := runSkewPoint(config.setup())
	if err != nil {
		t.Fatalf("running the point: %v", err)
	}

	body := strings.Join(m3aRunBody(run), "\n")
	for _, want := range []string{
		"population requested_q_share=0.25", "actual_q_share=",
		"q_subgraph", "m5_all", "m5_structural", "m5_non_structural",
		"guards requesters=", "digest=",
		"confirmed_only requests=", "whole_set requests=", "gap_pp=", "bound=lower",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the stored point does not carry %q:\n%s", want, body)
		}
	}
	if run.RefusalsConfirmed.Requests != run.RefusalsSampled.Requests {
		t.Fatalf("the two readings answered %d and %d requests",
			run.RefusalsConfirmed.Requests, run.RefusalsSampled.Requests)
	}

	// ⚠️ Under THIS load the two readings must be able to differ, or the sweep
	// would be storing one number twice. The unconfirmed spare rescues the
	// whole-set population and not the confirmed-only one — that difference IS
	// the price of the open decision.
	if run.RefusalsConfirmed.Refused() == run.RefusalsSampled.Refused() {
		t.Errorf("under \"every request aims at the confirmed guard\" the two populations refused "+
			"equally often (%d): the load that exists to force them apart did not, so the sweep "+
			"cannot measure the open sub-question at all", run.RefusalsConfirmed.Refused())
	}

	// An empty Q-subgraph renders as words, not as a count that reads like
	// success.
	if got := describeConnectivity(componentReport{}); got != "no data" {
		t.Errorf("an empty Q-subgraph stores as %q; at a share where the structural plane cannot "+
			"exist that must read as no data", got)
	}
}
