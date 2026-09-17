package overlaysim

// m4_reference_test.go is the acceptance of the M4 measurer: sets small enough
// that the expected refusal is obvious, plus the three properties that make the
// measurement trustworthy — the set is not modified, no fallback exists, and the
// denominator counts every request rather than only the refused ones.

import (
	"strings"
	"testing"
)

// m4Graph is the role layout the fixtures share: node 0…3 are ¬Q (eligible to
// be first hops), node 4 is Q (never eligible, whatever its other flags say).
func m4Graph() *graph {
	return transitGraph([]int{roleOther, roleOther, roleOther, roleOther, roleStructural})
}

// TestM4ReferenceSets walks the fixtures of requirement 6. Every expectation is
// stated for BOTH populations, because the whole point of the open sub-question
// is that they can differ.
func TestM4ReferenceSets(t *testing.T) {
	type expected struct {
		outcome  m4Outcome
		firstHop int32
	}
	cases := []struct {
		name               string
		members            []guardMember
		target             int32
		confirmed, sampled expected
	}{
		{
			name:      "empty set: refused, and the target had nothing to do with it",
			members:   nil,
			target:    1,
			confirmed: expected{m4RefusedNobodySuitable, -1},
			sampled:   expected{m4RefusedNobodySuitable, -1},
		},
		{
			name:      "the only guard IS the target: the degenerate k = 1 of S24а",
			members:   []guardMember{eligibleGuard(1, true)},
			target:    1,
			confirmed: expected{m4RefusedByTargetExclusion, -1},
			sampled:   expected{m4RefusedByTargetExclusion, -1},
		},
		{
			name:      "the only guard is somebody else: served",
			members:   []guardMember{eligibleGuard(1, true)},
			target:    2,
			confirmed: expected{m4Served, 1},
			sampled:   expected{m4Served, 1},
		},
		{
			name:      "two suitable guards, one of them the target: the other one carries it",
			members:   []guardMember{eligibleGuard(1, true), eligibleGuard(2, true)},
			target:    1,
			confirmed: expected{m4Served, 2},
			sampled:   expected{m4Served, 2},
		},
		{
			name: "the spare is not suitable: it does not rescue the request",
			members: []guardMember{
				eligibleGuard(1, true),
				{Node: 2, Alive: false, TransitCapable: true, IdentityProven: true, Confirmed: true},
			},
			target:    1,
			confirmed: expected{m4RefusedByTargetExclusion, -1},
			sampled:   expected{m4RefusedByTargetExclusion, -1},
		},
		{
			name: "the spare is Q: the structural half never carries a first hop",
			members: []guardMember{
				eligibleGuard(1, true),
				eligibleGuard(4, true), // node 4 is Q
			},
			target:    1,
			confirmed: expected{m4RefusedByTargetExclusion, -1},
			sampled:   expected{m4RefusedByTargetExclusion, -1},
		},
		{
			name: "nobody suitable at all, target outside the set: NOT an exclusion refusal",
			members: []guardMember{
				{Node: 1, Alive: true, TransitCapable: false, IdentityProven: true, Confirmed: true},
				{Node: 2, Alive: true, TransitCapable: true, IdentityProven: false, Confirmed: true},
			},
			target:    3,
			confirmed: expected{m4RefusedNobodySuitable, -1},
			sampled:   expected{m4RefusedNobodySuitable, -1},
		},
		{
			name: "confirmed and sampled disagree: this gap IS the open sub-question",
			members: []guardMember{
				eligibleGuard(1, true),  // confirmed, and it is the target
				eligibleGuard(2, false), // suitable, never used yet
			},
			target: 1,
			// Taking only confirmed members refuses; allowing the whole set
			// serves the request through a member that is not confirmed yet —
			// and would become confirmed BECAUSE of this target (§4.3.4″.3).
			confirmed: expected{m4RefusedByTargetExclusion, -1},
			sampled:   expected{m4Served, 2},
		},
	}

	g := m4Graph()
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			set := guardSet{Name: "fixture", Members: testCase.members}

			for _, population := range []struct {
				label         string
				onlyConfirmed bool
				want          expected
			}{
				{"confirmed only", true, testCase.confirmed},
				{"whole set", false, testCase.sampled},
			} {
				got := selectFirstHop(g, set, testCase.target, population.onlyConfirmed)
				if got.Outcome != population.want.outcome {
					t.Errorf("%s: outcome %v, want %v",
						population.label, got.Outcome, population.want.outcome)
				}
				if got.FirstHop != population.want.firstHop {
					t.Errorf("%s: first hop %d, want %d",
						population.label, got.FirstHop, population.want.firstHop)
				}
				if got.Outcome != m4Served && got.FirstHop != -1 {
					t.Errorf("%s: a refusal named hop %d — that is the forbidden fallback",
						population.label, got.FirstHop)
				}
				if got.FirstHop == testCase.target {
					t.Errorf("%s: the target itself was chosen as the first hop",
						population.label)
				}
			}
		})
	}
}

// TestM4ChoosesOnlyInsideTheSet is rule 5 made executable: whatever is returned
// must be a member of the set that passed every condition.
func TestM4ChoosesOnlyInsideTheSet(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "mixed", Members: []guardMember{
		eligibleGuard(1, true),
		{Node: 2, Alive: false, TransitCapable: true, IdentityProven: true, Confirmed: true},
		eligibleGuard(3, false),
		eligibleGuard(4, true), // Q
	}}

	for target := int32(0); target < 5; target++ {
		for _, onlyConfirmed := range []bool{true, false} {
			result := selectFirstHop(g, set, target, onlyConfirmed)
			if result.Outcome != m4Served {
				continue
			}

			var chosen *guardMember
			for i := range set.Members {
				if set.Members[i].Node == result.FirstHop {
					chosen = &set.Members[i]
					break
				}
			}
			if chosen == nil {
				t.Fatalf("target %d: chose node %d, which is not in the set",
					target, result.FirstHop)
			}
			if !chosen.suitable(g) {
				t.Fatalf("target %d: chose unsuitable member %d", target, result.FirstHop)
			}
			if onlyConfirmed && !chosen.Confirmed {
				t.Fatalf("target %d: confirmed-only population chose unconfirmed member %d",
					target, result.FirstHop)
			}
		}
	}
}

// TestM4LeavesTheSetAndTheWorkloadUntouched is rule 2. The measurement must not
// rotate, top up, promote or reorder anything — so the inputs are compared
// before and after, field by field.
func TestM4LeavesTheSetAndTheWorkloadUntouched(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(2, false),
		{Node: 3, Alive: false, TransitCapable: true, IdentityProven: true},
	}}
	load := uniformWorkload(9, len(g.roles), 200)

	before := describeSet(g, set)
	targetsBefore := append([]int32(nil), load.Targets...)

	if _, err := measureFirstHopRefusals(g, set, load); err != nil {
		t.Fatalf("measuring: %v", err)
	}

	if after := describeSet(g, set); after != before {
		t.Errorf("the guard set changed during the measurement:\n before %s\n after  %s",
			before, after)
	}
	if len(load.Targets) != len(targetsBefore) {
		t.Fatalf("the workload changed length: %d, was %d", len(load.Targets), len(targetsBefore))
	}
	for i, target := range load.Targets {
		if target != targetsBefore[i] {
			t.Fatalf("workload request %d changed from %d to %d", i, targetsBefore[i], target)
		}
	}
}

// TestM4MainContactInSetRefusesEverything is the readiness criterion: when the
// only suitable guard is always the target, the refusal is 100 % and it is
// attributed to the exclusion, not to an empty set.
func TestM4MainContactInSetRefusesEverything(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "k=1", Members: []guardMember{eligibleGuard(1, true)}}
	load := mainContactInSetWorkload(1, 500)

	report, err := measureFirstHopRefusals(g, set, load)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	for _, slice := range []struct {
		label string
		got   m4Slice
	}{
		{"confirmed only", report.Confirmed},
		{"whole set", report.Sampled},
	} {
		if slice.got.Requests != 500 {
			t.Errorf("%s: denominator %d, want 500", slice.label, slice.got.Requests)
		}
		if slice.got.Served != 0 {
			t.Errorf("%s: served %d requests, want none", slice.label, slice.got.Served)
		}
		if slice.got.RefusedByTargetExclusion != 500 {
			t.Errorf("%s: %d refusals by exclusion, want 500",
				slice.label, slice.got.RefusedByTargetExclusion)
		}
		if slice.got.RefusedNobodySuitable != 0 {
			t.Errorf("%s: %d refusals blamed on an empty set, want none — the set had a "+
				"suitable member, it was just the target",
				slice.label, slice.got.RefusedNobodySuitable)
		}
		if got, want := slice.got.RefusalShare(), "500/500 = 100.0%"; got != want {
			t.Errorf("%s: refusal share %q, want %q", slice.label, got, want)
		}
		if got, want := slice.got.ExclusionShare(), "500/500 = 100.0%"; got != want {
			t.Errorf("%s: exclusion share %q, want %q", slice.label, got, want)
		}
	}
}

// TestM4SeparatesTheTwoRefusals keeps requirement 4 honest on a mixed workload:
// half the requests hit the target that is in the set, half do not, and a set
// with a second suitable member serves the latter.
func TestM4SeparatesTheTwoRefusals(t *testing.T) {
	g := m4Graph()

	// One suitable member (node 1). Requests alternate between node 1 (which
	// forces the exclusion refusal) and node 3 (which is served by node 1).
	set := guardSet{Name: "single suitable", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(4, true), // Q — present, never suitable
	}}
	targets := make([]int32, 0, 100)
	for i := range 100 {
		if i%2 == 0 {
			targets = append(targets, 1)
			continue
		}
		targets = append(targets, 3)
	}
	load := workload{Name: "alternating", Parameters: "50 requests to node 1, 50 to node 3", Targets: targets}

	report, err := measureFirstHopRefusals(g, set, load)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}
	if report.Sampled.Served != 50 {
		t.Errorf("served %d, want 50", report.Sampled.Served)
	}
	if report.Sampled.RefusedByTargetExclusion != 50 {
		t.Errorf("refused by exclusion %d, want 50", report.Sampled.RefusedByTargetExclusion)
	}
	if report.Sampled.RefusedNobodySuitable != 0 {
		t.Errorf("refused for an empty set %d, want 0", report.Sampled.RefusedNobodySuitable)
	}
	if got, want := report.Sampled.RefusalShare(), "50/100 = 50.0%"; got != want {
		t.Errorf("refusal share %q, want %q", got, want)
	}

	// Now the same workload against a set whose every member is unsuitable: the
	// refusals must move entirely to the other bucket.
	dead := guardSet{Name: "all dead", Members: []guardMember{
		{Node: 1, Alive: false, TransitCapable: true, IdentityProven: true, Confirmed: true},
	}}
	deadReport, err := measureFirstHopRefusals(g, dead, load)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}
	if deadReport.Sampled.RefusedNobodySuitable != 100 {
		t.Errorf("refused for an empty set %d, want 100", deadReport.Sampled.RefusedNobodySuitable)
	}
	if deadReport.Sampled.RefusedByTargetExclusion != 0 {
		t.Errorf("refused by exclusion %d, want 0 — nothing was suitable to exclude",
			deadReport.Sampled.RefusedByTargetExclusion)
	}
}

// TestM4EmptySampleSaysNoData: a report over zero requests must not read 0 %.
func TestM4EmptySampleSaysNoData(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "pinned", Members: []guardMember{eligibleGuard(1, true)}}
	load := workload{Name: "nothing", Parameters: "no requests", Targets: nil}

	report, err := measureFirstHopRefusals(g, set, load)
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}
	for _, got := range []string{
		report.Sampled.RefusalShare(),
		report.Sampled.ExclusionShare(),
		report.Sampled.NobodySuitableShare(),
		report.Sampled.String(),
		report.Confirmed.String(),
	} {
		if got != "no data" {
			t.Errorf("empty sample rendered as %q, want %q", got, "no data")
		}
	}
}

// TestM4WorkloadsAreReproducibleAndDeclared covers requirement 5: the generators
// are deterministic, they honour the parameters they print, and they say out
// loud that those parameters are assumed rather than measured.
func TestM4WorkloadsAreReproducibleAndDeclared(t *testing.T) {
	const nodes, requests = 64, 10_000

	loads := []workload{
		uniformWorkload(3, nodes, requests),
		popularContactWorkload(3, nodes, requests, 4, 0.8),
		mainContactInSetWorkload(1, requests),
	}
	repeats := []workload{
		uniformWorkload(3, nodes, requests),
		popularContactWorkload(3, nodes, requests, 4, 0.8),
		mainContactInSetWorkload(1, requests),
	}

	for i, load := range loads {
		if len(load.Targets) != requests {
			t.Errorf("%s: %d requests, want %d", load.Name, len(load.Targets), requests)
		}
		for _, target := range load.Targets {
			if target < 0 || int(target) >= nodes {
				t.Fatalf("%s: target %d outside the network of %d nodes",
					load.Name, target, nodes)
			}
		}
		for j, target := range load.Targets {
			if target != repeats[i].Targets[j] {
				t.Fatalf("%s: request %d differs between two runs of the same seed: %d vs %d",
					load.Name, j, target, repeats[i].Targets[j])
			}
		}
		if load.Parameters == "" {
			t.Errorf("%s: parameters are not recorded", load.Name)
		}
		if !strings.Contains(load.String(), "NOT measured user load") {
			t.Errorf("%s: the report line does not say the distribution is assumed: %s",
				load.Name, load)
		}
	}

	// The popular workload must actually concentrate the way it claims: the head
	// takes headShare of the requests directly, plus its share of the uniform
	// tail. The expectation is derived from the DECLARED parameters, so a
	// generator that ignores them fails here rather than passing quietly.
	const headSize, headShare = 4, 0.8
	popular := popularContactWorkload(3, nodes, requests, headSize, headShare)
	inHead := 0
	for _, target := range popular.Targets {
		if int(target) < headSize {
			inHead++
		}
	}
	want := headShare + (1-headShare)*float64(headSize)/float64(nodes)
	got := float64(inHead) / float64(requests)
	if got < want-0.02 || got > want+0.02 {
		t.Errorf("head took %.3f of requests, the declared parameters predict %.3f", got, want)
	}

	// And the uniform workload must not concentrate: the same head is a plain
	// headSize/nodes slice of it.
	uniform := uniformWorkload(3, nodes, requests)
	inHead = 0
	for _, target := range uniform.Targets {
		if int(target) < headSize {
			inHead++
		}
	}
	if got, want := float64(inHead)/float64(requests), float64(headSize)/float64(nodes); got < want-0.02 || got > want+0.02 {
		t.Errorf("uniform head took %.3f of requests, want about %.3f", got, want)
	}
}

// TestM4WorkloadsDifferInRefusalRate checks that the three loads actually
// exercise different regimes of the rule — otherwise measuring three of them
// proves nothing. ⚠️ The numbers here are properties of the FIXTURE, not
// findings about the network: the guard set is an input.
func TestM4WorkloadsDifferInRefusalRate(t *testing.T) {
	roles := make([]int, 64)
	for i := range roles {
		roles[i] = roleOther
	}
	g := transitGraph(roles)

	// A set of two suitable members, one of which (node 1) is a popular contact.
	set := guardSet{Name: "two suitable", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(2, true),
	}}

	uniform, err := measureFirstHopRefusals(g, set, uniformWorkload(11, len(roles), 5_000))
	if err != nil {
		t.Fatalf("uniform: %v", err)
	}
	if uniform.Sampled.Refused() != 0 {
		t.Errorf("two suitable members cannot refuse: %s", uniform.Sampled)
	}

	// With a single suitable member, a popular head that contains it refuses far
	// more often than uniform targeting does.
	single := guardSet{Name: "one suitable", Members: []guardMember{eligibleGuard(1, true)}}
	popular, err := measureFirstHopRefusals(g, single, popularContactWorkload(11, len(roles), 5_000, 4, 0.8))
	if err != nil {
		t.Fatalf("popular: %v", err)
	}
	uniformSingle, err := measureFirstHopRefusals(g, single, uniformWorkload(11, len(roles), 5_000))
	if err != nil {
		t.Fatalf("uniform single: %v", err)
	}
	if popular.Sampled.Refused() <= uniformSingle.Sampled.Refused() {
		t.Errorf("a popular head containing the only guard refused %d times, uniform targeting "+
			"%d — the two loads are not exercising different regimes",
			popular.Sampled.Refused(), uniformSingle.Sampled.Refused())
	}

	main, err := measureFirstHopRefusals(g, single, mainContactInSetWorkload(1, 5_000))
	if err != nil {
		t.Fatalf("main contact: %v", err)
	}
	if main.Sampled.Refused() != 5_000 {
		t.Errorf("main contact inside the set refused %d of 5000", main.Sampled.Refused())
	}
}

// TestM4RejectsInputsItCannotMeasure: a set or a workload naming a node the
// graph does not have is a stand defect, not a refusal.
func TestM4RejectsInputsItCannotMeasure(t *testing.T) {
	g := m4Graph()

	_, err := measureFirstHopRefusals(g,
		guardSet{Name: "out of range", Members: []guardMember{eligibleGuard(99, true)}},
		uniformWorkload(1, len(g.roles), 10))
	if err == nil || !strings.Contains(err.Error(), "guard set") {
		t.Fatalf("a member outside the graph produced %v, want an error naming the set", err)
	}

	_, err = measureFirstHopRefusals(g,
		guardSet{Name: "fine", Members: []guardMember{eligibleGuard(1, true)}},
		workload{Name: "bad", Parameters: "one impossible target", Targets: []int32{99}})
	if err == nil || !strings.Contains(err.Error(), "workload") {
		t.Fatalf("a target outside the graph produced %v, want an error naming the workload", err)
	}
}

// TestM4ReportNamesItsInputs: a refusal rate without the set and the
// distribution behind it is unreadable, so the rendered report must carry both —
// and the set means its COMPOSITION, not a label somebody chose for it.
func TestM4ReportNamesItsInputs(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(4, false), // Q, and not confirmed — both must be visible
	}}
	report, err := measureFirstHopRefusals(g, set, mainContactInSetWorkload(1, 10))
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	rendered := report.String()
	for _, want := range []string{
		`guard set "pinned"`,
		"main contact inside the set",
		"NOT measured user load",
		"confirmed only:",
		"whole set:",
		"10/10 = 100.0%",
		// The composition, member by member: without it the two populations
		// cannot be explained, only reported.
		"1{¬Q alive:true transit:true identity:true confirmed:true}",
		"4{Q alive:true transit:true identity:true confirmed:false}",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("report does not carry %q:\n%s", want, rendered)
		}
	}
}

// TestM4ReportSnapshotOutlivesTheInput: the report is a record, so editing the
// input afterwards must not rewrite it. ⚠️ This is the same property the rule
// demands of the measurer itself (§4.3.4″.3 п.2), applied to the published
// result: a number and the set it was taken on must stay together.
func TestM4ReportSnapshotOutlivesTheInput(t *testing.T) {
	g := m4Graph()
	set := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(2, false),
	}}

	report, err := measureFirstHopRefusals(g, set, uniformWorkload(2, len(g.roles), 50))
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}
	renderedBefore := report.String()
	digestBefore := report.Set.Digest()

	// Every flag the measurement read, changed afterwards — plus the role in the
	// graph, which the snapshot also had to capture.
	set.Members[0].Alive = false
	set.Members[0].Confirmed = false
	set.Members[1].IdentityProven = false
	set.Members[1].Confirmed = true
	set.Members = append(set.Members, eligibleGuard(3, true))
	g.roles[1] = roleStructural

	if got := report.String(); got != renderedBefore {
		t.Errorf("editing the input after the measurement rewrote the report:\n before %s\n after  %s",
			renderedBefore, got)
	}
	if got := report.Set.Digest(); got != digestBefore {
		t.Errorf("digest changed from %s to %s after the input was edited", digestBefore, got)
	}
	if len(report.Set.Members) != 2 {
		t.Errorf("the snapshot grew to %d members with the input", len(report.Set.Members))
	}
	if !report.Set.Members[0].Alive || !report.Set.Members[0].Confirmed {
		t.Error("the snapshot followed the input's flags instead of keeping its own")
	}
	if report.Set.Members[1].Role != roleOther {
		t.Error("the snapshot followed the graph's role instead of the one the measurement used")
	}
}

// TestM4SetsWithTheSameNameAreDistinguishable is the point of the digest: a name
// is chosen by whoever ran the experiment and proves nothing about what was
// measured.
func TestM4SetsWithTheSameNameAreDistinguishable(t *testing.T) {
	g := m4Graph()
	load := mainContactInSetWorkload(1, 100)

	// Same name, and the difference is exactly the one that moves the gap
	// between the two populations: whether the spare is confirmed.
	first := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(2, true),
	}}
	second := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(1, true),
		eligibleGuard(2, false),
	}}

	firstReport, err := measureFirstHopRefusals(g, first, load)
	if err != nil {
		t.Fatalf("measuring the first set: %v", err)
	}
	secondReport, err := measureFirstHopRefusals(g, second, load)
	if err != nil {
		t.Fatalf("measuring the second set: %v", err)
	}

	// The two really do measure differently — otherwise the test would prove
	// nothing about telling them apart.
	if firstReport.Confirmed.Refused() == secondReport.Confirmed.Refused() {
		t.Fatalf("both sets refuse %d of %d in the confirmed population — the fixture no longer "+
			"depends on the difference", firstReport.Confirmed.Refused(), load.Targets[0])
	}
	if firstReport.Set.Digest() == secondReport.Set.Digest() {
		t.Errorf("two different sets named %q share the digest %s",
			first.Name, firstReport.Set.Digest())
	}
	if firstReport.String() == secondReport.String() {
		t.Errorf("two different sets named %q render identically:\n%s", first.Name, firstReport)
	}
	if !strings.Contains(secondReport.Set.Composition(), "2{¬Q alive:true transit:true identity:true confirmed:false}") {
		t.Errorf("the composition does not show the unconfirmed spare: %s",
			secondReport.Set.Composition())
	}

	// And the digest is stable: the same composition measured twice is the same
	// artefact, otherwise it identifies nothing.
	repeat, err := measureFirstHopRefusals(g, second, load)
	if err != nil {
		t.Fatalf("repeating: %v", err)
	}
	if repeat.Set.Digest() != secondReport.Set.Digest() {
		t.Errorf("the same set produced digests %s and %s",
			secondReport.Set.Digest(), repeat.Set.Digest())
	}

	// Order is part of the set — it decides who carries a served request — so a
	// reordering is a different artefact, not the same one.
	reordered := guardSet{Name: "pinned", Members: []guardMember{
		eligibleGuard(2, false),
		eligibleGuard(1, true),
	}}
	reorderedReport, err := measureFirstHopRefusals(g, reordered, load)
	if err != nil {
		t.Fatalf("measuring the reordered set: %v", err)
	}
	if reorderedReport.Set.Digest() == secondReport.Set.Digest() {
		t.Errorf("reordering the members kept the digest %s", reorderedReport.Set.Digest())
	}
}

// TestM4GapIsStatedInPercentagePoints is the presentation rule of §4.3.4″.3: the
// two readings are shown side by side AND their difference is computed, over the
// same requests, in percentage points.
//
// ⚠️ It exists because leaving the subtraction to the reader is how a hidden
// choice of population gets made. A report that prints two shares and no
// difference invites the reader to pick one — and a reader who subtracts shares
// taken over different request sets gets a number that means nothing.
func TestM4GapIsStatedInPercentagePoints(t *testing.T) {
	t.Parallel()

	g := m4Graph()

	// One confirmed member which is also the target, and one unconfirmed spare.
	// Under the confirmed-only reading every request refuses; under the
	// whole-set reading the spare serves every one. That is the widest the gap
	// can be, and it is the case the open sub-question is about.
	set := guardSet{
		Name: "one confirmed member, which is the target; one unconfirmed spare",
		Members: []guardMember{
			eligibleGuard(1, true),
			eligibleGuard(2, false),
		},
	}
	report, err := measureFirstHopRefusals(g, set, mainContactInSetWorkload(1, 200))
	if err != nil {
		t.Fatalf("measuring: %v", err)
	}

	if got, want := report.Confirmed.Refused(), 200; got != want {
		t.Fatalf("confirmed-only refused %d of 200", got)
	}
	if got := report.Sampled.Refused(); got != 0 {
		t.Fatalf("whole set refused %d of 200, want 0 — the unconfirmed spare serves them", got)
	}
	if got, want := report.GapPP(), "+100.0 pp (confirmed-only minus whole-set, over the same 200 requests)"; got != want {
		t.Errorf("gap %q, want %q", got, want)
	}

	// The refusal that the gap is about must be attributed to the RULE, not to
	// an unusable set: the confirmed member was suitable until the target
	// excluded it.
	if report.Confirmed.RefusedByTargetExclusion != 200 ||
		report.Confirmed.RefusedNobodySuitable != 0 {
		t.Errorf("confirmed-only refusals: %d by exclusion, %d nobody suitable — the whole 200 "+
			"belong to the exclusion", report.Confirmed.RefusedByTargetExclusion,
			report.Confirmed.RefusedNobodySuitable)
	}

	rendered := report.String()
	for _, want := range []string{
		"confirmed only:",
		"whole set:",
		"gap:",
		"+100.0 pp",
		// ⚠️ And the scope, next to the numbers rather than in a footnote.
		"DECLARED by the guard model, not observed in any network",
		"not a measured refusal rate of the tree",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the report does not carry %q:\n%s", want, rendered)
		}
	}
}

// TestM4GapRefusesIncomparableReadings covers the two cases where no difference
// may be printed at all.
func TestM4GapRefusesIncomparableReadings(t *testing.T) {
	t.Parallel()

	if got := refusalGap(m4Slice{}, m4Slice{}); got != "no data" {
		t.Errorf("two empty readings: %q, want %q", got, "no data")
	}
	if got := refusalGap(m4Slice{Requests: 10}, m4Slice{}); got != "no data" {
		t.Errorf("one empty reading: %q, want %q", got, "no data")
	}

	// ⚠️ Different denominators are the dangerous case: both shares exist, both
	// look fine, and subtracting them compares answers to different questions.
	got := refusalGap(m4Slice{Requests: 10, RefusedNobodySuitable: 5},
		m4Slice{Requests: 20, RefusedNobodySuitable: 5})
	if !strings.Contains(got, "NOT COMPARABLE") {
		t.Errorf("unequal denominators: %q, want a refusal to compare", got)
	}
	if strings.Contains(got, "pp") {
		t.Errorf("a difference was printed for unequal denominators: %q", got)
	}
}
