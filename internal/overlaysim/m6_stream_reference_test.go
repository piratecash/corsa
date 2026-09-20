package overlaysim

// m6_stream_reference_test.go are the references for the candidate-stream
// comparison: what an ADAPTIVE pair (main mode against the ‘from scratch’
// control) can and cannot hold, per source, with the first divergence named;
// and what the RECORDED-STREAM pair holds by construction.
//
// The acceptance the references serve: either the same stream is proven, or a
// concrete counterexample stands beside a ready proposal. Both halves are
// here — the adaptive references are the counterexample, source by source,
// and the recorded-stream references are the proposal made to run.

import (
	"fmt"
	"strings"
	"testing"
)

// m6StreamSources are the five sources every pair is checked under.
func m6StreamSources() []struct {
	name       string
	branch     m6Branch
	omniscient bool
} {
	return []struct {
		name       string
		branch     m6Branch
		omniscient bool
	}{
		{"A", branchA, false},
		{"B", branchB, false},
		{"A′", branchAPrime, false},
		{"C", branchC, false},
		{"omniscient", branchA, true},
	}
}

// m6PairBase is the phased fixture for pairs: short enough for five sources
// times two pairs, long enough to have an F3 with something in it.
func m6PairBase(branch m6Branch, omniscient bool) m6ModelConfig {
	config := m6PhasedBase()
	config.Branch = branch
	config.OmniscientControl = omniscient
	config.TraceOffers = true
	config.Phases = &m6PhasePlan{FillTicks: 10, IdleTicks: 4, RecoveryTicks: 10, CadenceTicks: 6}
	return config
}

// pairOnBoundaries prepares the main run and its ‘from scratch’ control on
// one graph, both replaying the boundaries of `boundaries`, and compares them
// in lockstep.
func pairOnBoundaries(
	t *testing.T, g *graph, config m6ModelConfig, boundaries []m6PhaseBoundary,
) (*m6Comparison, *m6Network, *m6Network) {
	t.Helper()
	config.ReplayPhases = boundaries
	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing the main run: %v", err)
	}
	scratch := config
	scratch.StartEmpty = true
	control, err := newM6Network(g, scratch, everybody)
	if err != nil {
		t.Fatalf("preparing the control: %v", err)
	}
	comparison, err := compareM6Runs(main, control)
	if err != nil {
		t.Fatalf("comparing: %v", err)
	}
	return comparison, main, control
}

// adaptivePair runs the adaptive main once for its boundaries, then the pair.
func adaptivePair(t *testing.T, config m6ModelConfig) (*m6Comparison, *m6Network, *m6Network) {
	t.Helper()
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base := runM6ModelOn(t, g, config, everybody)
	return pairOnBoundaries(t, g, config, base.PhaseBoundaries())
}

// recordedPair records the adaptive main, then runs the replayed pair on its
// recording and its boundaries.
func recordedPair(t *testing.T, config m6ModelConfig) (*m6Comparison, *m6Network, *m6Network, *m6RecordedStream) {
	t.Helper()
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	replay := config
	replay.Stream = stream
	comparison, main, control := pairOnBoundaries(t, g, replay, recording.PhaseBoundaries())
	return comparison, main, control, stream
}

func requireClaims(t *testing.T, comparison *m6Comparison, kinds ...m6ClaimKind) {
	t.Helper()
	for _, kind := range kinds {
		if !comparison.Holds(kind) {
			t.Errorf("the claim %q does not hold:\n%s", kind, comparison)
		}
	}
}

// TestM6AdaptivePairsNameTheirFirstDivergence is the counterexample, source by
// source: what the adaptive pair holds (inputs, boundaries, decisions, the
// pool at the onset, identical offers before it) and where and why it parts.
//
// ⚠️ For A′ and C the claim that fails is "same responder tables at the
// onset" — the source of those branches is another node's memory, which the
// clearing emptied. That is the precise discrepancy with "the same candidate
// stream" of П-6/§5.4, and it is a property of the branches, not of the
// implementation.
func TestM6AdaptivePairsNameTheirFirstDivergence(t *testing.T) {
	t.Parallel()

	for _, source := range m6StreamSources() {
		source := source
		t.Run(source.name, func(t *testing.T) {
			t.Parallel()
			config := m6PairBase(source.branch, source.omniscient)
			comparison, _, control := adaptivePair(t, config)

			requireClaims(t, comparison, claimSameInputs, claimSameBoundaries, claimSameDecisions,
				claimSameRealisedChurn, claimSamePoolAtOnset, claimOffersSameBeforeOnset)
			if comparison.Divergence == nil {
				t.Fatalf("the offers never diverged — the control did not clear anything:\n%s", comparison)
			}
			if comparison.Divergence.Tick < comparison.OnsetTick || comparison.OnsetTick < 0 {
				t.Fatalf("the first divergence is at tick %d, the onset at %d:\n%s",
					comparison.Divergence.Tick, comparison.OnsetTick, comparison)
			}
			if comparison.Holds(claimOffersSameAfterOnset) {
				t.Fatalf("the comparison claims the offers stayed identical after the onset while it "+
					"recorded a divergence:\n%s", comparison)
			}

			tables := comparison.claim(claimSameResponderTablesAtOnset)
			switch {
			case source.branch.ExchangesRecords() && !source.omniscient:
				// The counterexample: the responder tables differ, so the
				// stream is not the same, whatever the first offer divergence
				// was caused by.
				if tables.Status != claimFails {
					t.Fatalf("%s: the responder tables at the onset did not differ — then the "+
						"cleared tables handed over the same records, which cannot be:\n%s",
						source.name, comparison)
				}
				if !strings.Contains(control.report.StreamLine(), "NOT THE SAME") {
					t.Errorf("the control's report claims a stream it does not have:\n%s",
						control.report.StreamLine())
				}
			default:
				if tables.Status != claimNotApplicable {
					t.Fatalf("%s hands over no other node's records, yet the responder-table claim "+
						"is %s", source.name, tables.Status)
				}
				if comparison.Divergence.Cause != causeLocalMemory {
					t.Fatalf("%s: the first divergence is attributed to %s; with the pool identical "+
						"at the onset it can only be the owner's memory:\n%s", source.name,
						comparison.Divergence.Cause, comparison)
				}
				if first := comparison.FirstByCause[causeResponderTable]; first != nil {
					t.Fatalf("%s: a divergence was attributed to a responder's table, and this "+
						"source has no responders: %s", source.name, first)
				}
			}
			t.Logf("%s:\n%s", source.name, comparison)
		})
	}
}

// TestM6RecordedStreamPairsHoldTheStreamFixed is the proposal made to run: on
// one recording, the kept-memory and the cleared halves consume the same
// external stream, the comparison proves it claim by claim, and every offer
// divergence is attributed to the owner's memory — no responder, no drifted
// pool.
//
// ⚠️ Mutations that must break it: the clearing resetting the consumption
// state (the recorded-stream claim fails at the onset); a replay consulting a
// responder (a responder-table cause appears); the scratch half not being
// cleared (the offers never diverge).
func TestM6RecordedStreamPairsHoldTheStreamFixed(t *testing.T) {
	t.Parallel()

	for _, source := range m6StreamSources() {
		source := source
		t.Run(source.name, func(t *testing.T) {
			t.Parallel()
			config := m6PairBase(source.branch, source.omniscient)
			comparison, main, control, stream := recordedPair(t, config)

			requireClaims(t, comparison, claimSameInputs, claimSameBoundaries, claimSameDecisions,
				claimSameRealisedChurn, claimSameRecordedStream, claimOffersSameBeforeOnset)
			for _, kind := range []m6ClaimKind{claimSamePoolAtOnset, claimSameResponderTablesAtOnset} {
				if comparison.claim(kind).Status != claimNotApplicable {
					t.Errorf("%q should not apply to a replay:\n%s", kind, comparison)
				}
			}
			if comparison.Divergence == nil {
				t.Fatalf("the offers never diverged — the cleared half learned nothing:\n%s", comparison)
			}
			if comparison.Divergence.Tick < comparison.OnsetTick {
				t.Fatalf("the halves diverged at tick %d, before the onset %d:\n%s",
					comparison.Divergence.Tick, comparison.OnsetTick, comparison)
			}
			for cause, first := range comparison.FirstByCause {
				if cause != causeLocalMemory {
					t.Errorf("a divergence attributed to %s in a replayed pair: %s", cause, first)
				}
			}

			// The cleared half actually refilled from the recording, and says how
			// far the recording carried it.
			if control.report.ProbesAfterChurn.Filled() == 0 {
				t.Fatalf("the cleared half filled nothing after the onset:\n%s", control.report)
			}
			if !strings.Contains(control.config.String(), "PAIRED CONTROL") ||
				!strings.Contains(control.config.String(), "NOT a measurement") {
				t.Errorf("the replay does not name itself a control:\n%s", control.config)
			}
			if !strings.Contains(control.report.StreamLine(), "BY CONSTRUCTION") {
				t.Errorf("the replay's report does not state the construction:\n%s",
					control.report.StreamLine())
			}
			t.Logf("%s: %s\n%s\nkept memory: %d filled after onset, %d owners exhausted; cleared: %d "+
				"filled after onset, %d owners exhausted", source.name, stream, comparison,
				main.report.ProbesAfterChurn.Filled(), main.report.StreamExhaustedOwners,
				control.report.ProbesAfterChurn.Filled(), control.report.StreamExhaustedOwners)
		})
	}
}

// TestM6ARecordedStreamReplaysUnderItsOwnRules pins the consumption rules on a
// hand-built stream: a handed entry is offered once and consumed on a final
// outcome, a repeat of it costs a probe and lands in "already known", a pool
// entry is re-offered whenever the owner does not hold it, and an exhausted
// owner is counted.
//
// ⚠️ Mutations that must break it: consuming a pool entry; not consuming a
// handed entry on "already known"; consuming on a temporary refusal.
func TestM6ARecordedStreamReplaysUnderItsOwnRules(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	const owner = int32(0)
	pool := base.neighboursOf(owner)[0]
	handed := int32(-1)
	for _, candidate := range base.joinedOrder {
		if candidate != owner && !base.holdsEdge(owner, candidate) &&
			levelOf(base.ids[owner], base.ids[candidate], config.Shape.degree) >= 0 &&
			base.heldEdges(candidate) < config.Shape.budget {
			handed = candidate
			break
		}
	}
	if handed < 0 {
		// Everybody starts at B on the built graph; free one slot at a stranger.
		for _, candidate := range base.joinedOrder {
			if candidate != owner && !base.holdsEdge(owner, candidate) &&
				levelOf(base.ids[owner], base.ids[candidate], config.Shape.degree) >= 0 {
				handed = candidate
				break
			}
		}
	}
	stream := &m6RecordedStream{Branch: branchAPrime, IDs: base.ids, Members: base.trace.Members, ByOwner: map[int32][]m6StreamEntry{
		owner: {
			{Tick: 0, Source: offerAcquaintance, Candidate: pool},
			{Tick: 0, Source: offerExchange, Candidate: handed},
			{Tick: 0, Source: offerExchange, Candidate: handed}, // the §5.1.0 repeat
			{Tick: 3, Source: offerExchange, Candidate: handed}, // not available before tick 3
		},
	}}
	config.Stream = stream
	config.Repair = 0 // no ceiling: the fixture drives probes one at a time anyway
	network, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing the replay: %v", err)
	}
	for peer := range network.held[handed] {
		delete(network.held[handed], peer)
		break
	}
	state := network.states[owner]
	fingerprint := stream.fingerprint()

	offers := func() []int32 {
		clear(state.TriedThisTick)
		taken := make([]int32, 0, 4)
		for network.probeOnceInTheNetwork(owner, state) {
			taken = append(taken, network.trace.Offers[len(network.trace.Offers)-1].Peer)
			clear(state.TriedThisTick)
			if len(taken) > 8 {
				t.Fatal("the replay keeps offering: a consumed entry is being offered again")
			}
		}
		return taken
	}

	// Tick 0: the pool member (fills its level), the handed record (fills),
	// then its repeat (already known — a paid probe), then nothing.
	network.tick = 0
	got := offers()
	want := []int32{pool, handed, handed}
	if !equalNodes(got, want) {
		t.Fatalf("tick 0 offered %v, want %v (pool, handed, repeat)", got, want)
	}
	if !state.Table.holds(pool) || !state.Table.holds(handed) {
		t.Fatalf("after tick 0 the table holds pool=%v handed=%v", state.Table.holds(pool),
			state.Table.holds(handed))
	}
	if network.report.Probes.Outcomes[m6AlreadyKnown] != 1 {
		t.Fatalf("the repeat did not land in ‘already known’: %s", network.report.Probes)
	}
	if len(network.consumed[owner]) != 2 {
		t.Fatalf("%d entries consumed after tick 0, want the two handed ones", len(network.consumed[owner]))
	}

	// Tick 1: nothing is available — the pool member is held, the handed
	// entries are consumed, the tick-3 entry is not yet available.
	network.tick = 1
	if got := offers(); len(got) != 0 {
		t.Fatalf("tick 1 offered %v, want nothing", got)
	}
	// Owner 0 is not exhausted yet — an entry is still to come at tick 3 —
	// and every other participant, for whom the recording holds nothing, is.
	others := len(network.owners) - 1
	if exhausted, _, consumed := network.streamExhaustion(); exhausted != others || consumed != 2 {
		t.Fatalf("exhaustion reads %d owners / %d consumed, want %d / 2 (owner 0 still has an entry "+
			"due; everybody else has an empty recording)", exhausted, consumed, others)
	}

	// The owner loses its table (the ‘from scratch’ clearing): the pool member
	// is offered again, the consumed handed entries are not.
	network.tick = 2
	state.Table = newM6Table(owner, config.Shape.degree, config.Capacity, config.NearFrom)
	if got := offers(); !equalNodes(got, []int32{pool}) {
		t.Fatalf("after clearing, tick 2 offered %v, want the pool member only", got)
	}

	// Tick 3: the late entry becomes available; the record is already held
	// again? No — it was cleared, so it fills, and is consumed.
	network.tick = 3
	if got := offers(); !equalNodes(got, []int32{handed}) {
		t.Fatalf("tick 3 offered %v, want the late handed entry", got)
	}
	if len(network.consumed[owner]) != 3 {
		t.Fatalf("%d entries consumed after tick 3, want 3", len(network.consumed[owner]))
	}
	// Now the recording has nothing left for this owner: every handed entry
	// is consumed and the pool member is held.
	if exhausted, _, consumed := network.streamExhaustion(); exhausted != others+1 || consumed != 3 {
		t.Fatalf("exhaustion reads %d owners / %d consumed at the end, want %d / 3", exhausted,
			consumed, others+1)
	}
	if stream.fingerprint() != fingerprint {
		t.Fatal("the replay modified the recording")
	}
}

// TestM6AnAbsentRecordingIsAnEmptyOne pins the exhaustion counter: an owner
// with no key in the recording, an owner with an explicitly empty list and a
// recording empty for everybody are the same finding — nothing to be offered
// — and are counted alike.
//
// ⚠️ Mutation that must break it: skipping owners whose key is absent.
func TestM6AnAbsentRecordingIsAnEmptyOne(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	participants := 0
	for _, owner := range base.owners {
		if base.states[owner] != nil {
			participants++
		}
	}

	replayOn := func(stream *m6RecordedStream) (exhausted, consumed int) {
		t.Helper()
		replay := config
		replay.Stream = stream
		network, err := newM6Network(g, replay, everybody)
		if err != nil {
			t.Fatalf("preparing the replay: %v", err)
		}
		exhausted, _, consumed = network.streamExhaustion()
		return exhausted, consumed
	}

	// Empty for everybody: every participant is exhausted from the start.
	empty := &m6RecordedStream{Branch: config.Branch, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{}}
	if exhausted, consumed := replayOn(empty); exhausted != participants || consumed != 0 {
		t.Fatalf("an empty recording reads %d exhausted of %d participants, %d consumed",
			exhausted, participants, consumed)
	}

	// An absent key and an explicit empty list are the same finding.
	explicit := &m6RecordedStream{Branch: config.Branch, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{0: {}, 1: {}}}
	exhaustedExplicit, _ := replayOn(explicit)
	exhaustedAbsent, _ := replayOn(empty)
	if exhaustedExplicit != exhaustedAbsent {
		t.Fatalf("an explicit empty list counts %d exhausted, an absent key %d — the same finding "+
			"is counted differently", exhaustedExplicit, exhaustedAbsent)
	}

	// One owner with a live entry is the one owner not exhausted.
	one := &m6RecordedStream{Branch: config.Branch, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{
			0: {{Tick: 0, Source: offerAcquaintance, Candidate: base.neighboursOf(0)[0]}},
		}}
	if exhausted, _ := replayOn(one); exhausted != participants-1 {
		t.Fatalf("with one live entry %d of %d participants read as exhausted, want %d",
			exhausted, participants, participants-1)
	}
}

// TestM6DifferentRespondersAreJudgedByReachability pins the classifier on
// the case the content of an answer must NOT decide: two runs asked different
// responders. The choice of responder is settled by whether each responder
// was reachable in the other run's world at the start of the tick — read off
// the snapshot, never assumed — and by nothing else.
//
// ⚠️ Mutation that must break it: treating a responder whose reachability
// was not confirmed as "available in both runs".
func TestM6DifferentRespondersAreJudgedByReachability(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchAPrime, false)
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	control, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	const owner = int32(0)
	neighbours := main.neighboursOf(owner)
	first, second := neighbours[0], neighbours[1]
	asked := func(peer int32, handed ...int32) *m6OfferEntry {
		return &m6OfferEntry{Tick: 0, Owner: owner, Source: offerExchange, Level: -1, Peer: peer, Handed: handed}
	}

	// Both responders connected in both worlds: the choice is the asker's.
	worlds := m6WorldPair{main: snapshotWorldOf(main), control: snapshotWorldOf(control)}
	cause, detail := classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, asked(first, 7), asked(second, 9))
	if cause != causeLocalMemory {
		t.Fatalf("with both responders reachable in both worlds the cause is %s: %s", cause, detail)
	}

	// The control's responder is not connected to the owner in the main run's
	// world: reachability is not established there, and memory must not be
	// blamed.
	delete(main.held[owner], second)
	delete(main.held[second], owner)
	worlds = m6WorldPair{main: snapshotWorldOf(main), control: snapshotWorldOf(control)}
	cause, detail = classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, asked(first, 7), asked(second, 9))
	if cause != causeNetworkState {
		t.Fatalf("with the control's responder unreachable in the main run's world the cause is %s: %s",
			cause, detail)
	}
	if !strings.Contains(detail, "MISSING from the world") {
		t.Errorf("the detail does not name the unreachable responder: %s", detail)
	}

	// And the SAME responder handing over different records is the
	// responder's table — content, which only that case is about.
	cause, _ = classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, asked(first, 7), asked(first, 9))
	if cause != causeResponderTable {
		t.Fatalf("the same responder handing over different records is attributed to %s", cause)
	}

	// ⚠️ Only when the same QUESTION was asked. An addressed request names a
	// level; the same responder asked for two levels answers from one
	// unchanged table, and neither two different answers nor two identical
	// empty ones are evidence about that table.
	addressed := func(level int, handed ...int32) *m6OfferEntry {
		return &m6OfferEntry{Tick: 0, Owner: owner, Source: offerAddressed, Level: level, Peer: first,
			Handed: handed}
	}
	for _, levels := range []struct {
		name         string
		left, right  *m6OfferEntry
		wantCause    m6DivergenceCause
		wantMentions string
	}{
		{"different levels, different records", addressed(7, 11), addressed(6, 12), causeLocalMemory, "choice of level"},
		{"different levels, both empty", addressed(7), addressed(6), causeLocalMemory, "choice of level"},
		{"same level, different records", addressed(7, 11), addressed(7, 12), causeResponderTable, "same question"},
	} {
		cause, detail := classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, levels.left, levels.right)
		if cause != levels.wantCause || !strings.Contains(detail, levels.wantMentions) {
			t.Errorf("%s: attributed to %s (%s), want %s", levels.name, cause, detail, levels.wantCause)
		}
	}
}

// TestM6AReplayedHandOutCountsTowardsTheMeasuredPool pins S(u) under replay:
// a candidate handed from the recording is part of what the owner was
// actually offered, whether or not the probe went through — the same rule
// the exchange, the addressed answer and the omniscient control follow — and
// handing the same candidate again does not count it twice.
//
// ⚠️ Mutation that must break it: handing out from the recording without
// booking the candidate as reachable.
func TestM6AReplayedHandOutCountsTowardsTheMeasuredPool(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	const owner = int32(0)
	// A stranger at its ceiling B: the probe is refused and no connection or
	// record comes of it — the offer still happened.
	stranger := int32(-1)
	for _, candidate := range base.joinedOrder {
		if candidate != owner && !base.holdsEdge(owner, candidate) &&
			levelOf(base.ids[owner], base.ids[candidate], config.Shape.degree) >= 0 &&
			base.heldEdges(candidate) >= config.Shape.budget {
			stranger = candidate
			break
		}
	}
	if stranger < 0 {
		t.Fatal("the fixture found no stranger sitting at its ceiling")
	}
	stream := &m6RecordedStream{Branch: branchAPrime, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{
			owner: {{Tick: 0, Source: offerExchange, Candidate: stranger}},
		}}
	replay := config
	replay.Stream = stream
	network, err := newM6Network(g, replay, everybody)
	if err != nil {
		t.Fatalf("preparing the replay: %v", err)
	}
	state := network.states[owner]
	before := network.poolOf(owner)

	network.tick = 0
	clear(state.TriedThisTick)
	if !network.probeOnceInTheNetwork(owner, state) {
		t.Fatal("the replay offered nothing")
	}
	if state.Table.holds(stranger) || network.holdsEdge(owner, stranger) {
		t.Fatalf("the fixture's probe went through (record %v, edge %v); it needs a refusal",
			state.Table.holds(stranger), network.holdsEdge(owner, stranger))
	}
	if got := network.poolOf(owner); got != before+1 {
		t.Fatalf("S(u) went %d → %d after a refused hand-out, want %d: the offer vanished from the "+
			"measured pool", before, got, before+1)
	}

	// The temporary refusal kept the entry; handing it out again on the next
	// tick does not grow the pool.
	network.tick = 1
	clear(state.TriedThisTick)
	if !network.probeOnceInTheNetwork(owner, state) {
		t.Fatal("the entry was not offered again after a temporary refusal")
	}
	if got := network.poolOf(owner); got != before+1 {
		t.Fatalf("S(u) is %d after the second hand-out of the same candidate, want %d", got, before+1)
	}
}

// TestM6AnAdmittedNewcomerIsInTheWorldTheOffersWereMadeAgainst pins the
// moment the comparison snapshots the world: AFTER the tick's churn and
// admissions, BEFORE serving. A newcomer admitted identically in both runs
// brings its host an edge in both; a host that offers it in one run and not
// in the other did so out of memory, and a snapshot taken before the tick —
// where that edge does not exist yet — would have blamed the world.
//
// ⚠️ Mutation that must break it: snapshotting before prepareTick.
func TestM6AnAdmittedNewcomerIsInTheWorldTheOffersWereMadeAgainst(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnCompensated
	config.ChurnShare = 0.05
	config.ChurnAt = 0
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	control, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}

	// Two identical runs, in lockstep, until a tick admits a newcomer.
	for tick := 0; tick < 12; tick++ {
		main.tick, control.tick = tick, tick
		joinedBefore := len(main.joinedOrder)
		if err := main.prepareTick(); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		if err := control.prepareTick(); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		if len(main.joinedOrder) == joinedBefore {
			main.serveTick()
			control.serveTick()
			continue
		}

		newcomer := main.joinedOrder[len(main.joinedOrder)-1]
		var host int32 = -1
		for peer := range main.held[newcomer] {
			host = peer
		}
		if host < 0 || !control.holdsEdge(host, newcomer) {
			t.Fatalf("the newcomer %d was not admitted identically (host %d)", newcomer, host)
		}
		worlds := worldsBeforeServing(main, control)
		offer := &m6OfferEntry{Tick: tick, Owner: host, Source: offerAcquaintance,
			Level: levelOf(main.ids[host], main.ids[newcomer], config.Shape.degree), Peer: newcomer}
		cause, detail := classifyDivergence(main, control, worlds, sourcePairingOf(main, control), host, offer, nil)
		if cause != causeLocalMemory {
			t.Fatalf("tick %d: host %d offering the newcomer %d in one run only is attributed to %s: %s",
				tick, host, newcomer, cause, detail)
		}
		return
	}
	t.Fatal("no newcomer was admitted in 12 ticks of compensated load; the fixture proves nothing")
}

// TestM6AQueueChoiceIsNotAResponderTable pins the queue classification: the
// origin of a queue record is a responder, but WHY a different record was
// taken is decided by the level served, the owner's tried-set and the queue
// contents — and only the last of those points away from the owner, without
// settling whether a table or the asker's exchange timing differed.
//
// ⚠️ Mutation that must break it: attributing every queue divergence to the
// responder's table.
func TestM6AQueueChoiceIsNotAResponderTable(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchAPrime, false)
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	control, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	const owner = int32(0)
	a, b := int32(500), int32(600)
	queued := func(level int, peer int32) *m6OfferEntry {
		return &m6OfferEntry{Tick: 0, Owner: owner, Source: offerQueue, Level: level, Peer: peer}
	}

	// Identical queues in both runs.
	main.states[owner].Offered = []int32{a, b}
	control.states[owner].Offered = []int32{a, b}
	worlds := worldsBeforeServing(main, control)

	cause, detail := classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, queued(3, a), queued(5, b))
	if cause != causeLocalMemory || !strings.Contains(detail, "level") {
		t.Fatalf("a different level served from identical queues is attributed to %s: %s", cause, detail)
	}
	cause, detail = classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, queued(3, a), queued(3, b))
	if cause != causeLocalMemory || !strings.Contains(detail, "tried-set") {
		t.Fatalf("a different pick from identical queues at the same level is attributed to %s: %s",
			cause, detail)
	}

	// ⚠️ The same records in another ORDER: the queue is walked in order, so
	// the picks differ under one and the same local state, and the order the
	// answers arrived in is not the owner's memory — not established.
	main.states[owner].Offered = []int32{a, b}
	control.states[owner].Offered = []int32{b, a}
	worlds = worldsBeforeServing(main, control)
	cause, detail = classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, queued(3, a), queued(3, b))
	if cause != causeUndetermined || !strings.Contains(detail, "another order: true") {
		t.Fatalf("reordered queues with the same local state are attributed to %s: %s", cause, detail)
	}

	// The control's pick is absent from the main run's queue: a responder is
	// the origin, but which responder fact differed is not established.
	control.states[owner].Offered = []int32{a, b}
	main.states[owner].Offered = []int32{a}
	worlds = worldsBeforeServing(main, control)
	cause, detail = classifyDivergence(main, control, worlds, sourcePairingOf(main, control), owner, queued(3, a), queued(3, b))
	if cause != causeUndetermined {
		t.Fatalf("a pick absent from the other run's queue is attributed to %s: %s", cause, detail)
	}
}

// TestM6TheRecordedStreamClaimIsJudgedOnBothHalves pins the recorded-stream
// claim against the two ways it could stand falsely: two recordings that
// agree up to the onset and differ after it, and a replay set against an
// adaptive run — in either order.
//
// ⚠️ Mutation that must break it: judging the claim from the main half's
// recording alone.
func TestM6TheRecordedStreamClaimIsJudgedOnBothHalves(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchA, false)
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	onset := recording.Trace.OnsetTick
	if onset < 0 {
		t.Fatal("the recording has no onset")
	}
	boundaries := recording.PhaseBoundaries()

	prepare := func(name string, cfg m6ModelConfig) *m6Network {
		t.Helper()
		cfg.ReplayPhases = boundaries
		network, err := newM6Network(g, cfg, everybody)
		if err != nil {
			t.Fatalf("%s: preparing: %v", name, err)
		}
		return network
	}

	t.Run("recordings that differ only after the onset", func(t *testing.T) {
		t.Parallel()
		// A second recording: identical up to and including the onset tick,
		// with one candidate changed after it.
		altered := &m6RecordedStream{
			Branch: stream.Branch, Omniscient: stream.Omniscient, IDs: stream.IDs,
			Members: stream.Members, Exchanges: stream.Exchanges, Answers: stream.Answers,
			ByOwner: map[int32][]m6StreamEntry{},
		}
		changed := false
		for owner, entries := range stream.ByOwner {
			copied := append([]m6StreamEntry(nil), entries...)
			for index := range copied {
				if !changed && int(copied[index].Tick) > onset {
					copied[index].Candidate = (copied[index].Candidate + 1) % int32(config.Shape.nodes)
					changed = true
				}
			}
			altered.ByOwner[owner] = copied
		}
		if !changed {
			t.Fatal("the recording has no entry after the onset to alter")
		}
		if stream.fingerprint() == altered.fingerprint() {
			t.Fatal("the altered recording fingerprints the same")
		}

		left := config
		left.Stream = stream
		right := config
		right.Stream = altered
		right.StartEmpty = true
		verdict, err := compareM6Runs(prepare("main", left), prepare("control", right))
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		if verdict.Holds(claimSameRecordedStream) {
			t.Fatalf("two recordings that differ after the onset kept the recorded-stream claim:\n%s", verdict)
		}
	})

	// ⚠️ Owner's P2 (round 31): the claim was refused for a mixed or different
	// pair, but the CLASSIFIER still explained every divergence of queue/pool
	// candidates as "both halves read one recording, the owner's memory let a
	// different one through" — a verdict that is false when the inputs differ,
	// and it landed in Divergence and FirstByCause. The two cases below have
	// the SAME memory in both halves and different inputs; the cause must say
	// so, and "local memory" must not appear anywhere in the verdict.
	t.Run("same memory, two recordings whose first available candidate differs: the cause is the inputs", func(t *testing.T) {
		t.Parallel()
		altered := &m6RecordedStream{
			Branch: stream.Branch, Omniscient: stream.Omniscient, IDs: stream.IDs,
			Members: stream.Members, Exchanges: stream.Exchanges, Answers: stream.Answers,
			ByOwner: map[int32][]m6StreamEntry{},
		}
		// One owner's FIRST entry names another node of the SAME level, one the
		// recording never offers it: the first candidate the owner is offered
		// for that level differs, the memory does not.
		var victim int32 = -1
		for owner, entries := range stream.ByOwner {
			copied := append([]m6StreamEntry(nil), entries...)
			if victim < 0 && len(copied) > 0 {
				original := copied[0].Candidate
				level := levelOf(stream.IDs[owner], stream.IDs[original], config.Shape.degree)
				offered := map[int32]struct{}{}
				for _, entry := range copied {
					offered[entry.Candidate] = struct{}{}
				}
				for node := int32(0); int(node) < config.Shape.nodes; node++ {
					_, already := offered[node]
					if node == owner || already ||
						levelOf(stream.IDs[owner], stream.IDs[node], config.Shape.degree) != level {
						continue
					}
					copied[0].Candidate = node
					victim = owner
					break
				}
			}
			altered.ByOwner[owner] = copied
		}
		if victim < 0 || stream.fingerprint() == altered.fingerprint() {
			t.Fatal("the fixture could not build a recording that differs in a first candidate")
		}
		left := config
		left.Stream = stream
		right := config
		right.Stream = altered // memory KEPT in both halves
		verdict, err := compareM6Runs(prepare("main", left), prepare("control", right))
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		if verdict.Holds(claimSameRecordedStream) {
			t.Fatalf("two different recordings kept the recorded-stream claim:\n%s", verdict)
		}
		if verdict.Divergence == nil {
			t.Fatalf("the two halves never diverged although owner %d is offered different candidates first", victim)
		}
		if verdict.Divergence.Cause != causeDifferentInputs {
			t.Fatalf("the first divergence (owner %d) is blamed on %s; the halves read DIFFERENT recordings "+
				"and the memory is the same, so the cause is the inputs:\n%s", verdict.Divergence.Owner,
				verdict.Divergence.Cause, verdict.Divergence)
		}
		if first := verdict.FirstByCause[causeLocalMemory]; first != nil {
			t.Fatalf("a divergence was attributed to LOCAL MEMORY between two different recordings: %s", first)
		}
		if !strings.Contains(verdict.String(), "DIFFERENT INPUTS") {
			t.Fatalf("the verdict does not name the different inputs:\n%s", verdict)
		}
	})

	t.Run("a replay against an adaptive run, both orders", func(t *testing.T) {
		t.Parallel()
		replayed := config
		replayed.Stream = stream
		adaptive := config // the same memory mode: the difference is the source alone
		for _, order := range []struct {
			name        string
			left, right m6ModelConfig
		}{{"replay then adaptive", replayed, adaptive}, {"adaptive then replay", adaptive, replayed}} {
			verdict, err := compareM6Runs(prepare(order.name, order.left), prepare(order.name, order.right))
			if err != nil {
				t.Fatalf("%s: comparing: %v", order.name, err)
			}
			if verdict.Holds(claimSameRecordedStream) {
				t.Errorf("%s: a replay against an adaptive run kept the recorded-stream claim:\n%s",
					order.name, verdict)
			}
			if verdict.claim(claimSameRecordedStream).Status != claimFails {
				t.Errorf("%s: the recorded-stream claim is %s, want a failure that names the mixed "+
					"pair", order.name, verdict.claim(claimSameRecordedStream).Status)
			}
			if verdict.Divergence == nil {
				t.Fatalf("%s: a replay and an adaptive run never diverged", order.name)
			}
			if verdict.Divergence.Cause != causeDifferentInputs {
				t.Errorf("%s: the first divergence is blamed on %s; a mixed pair reads different sources "+
					"and no cause but the inputs is provable:\n%s", order.name, verdict.Divergence.Cause,
					verdict.Divergence)
			}
			for cause, first := range verdict.FirstByCause {
				if cause != causeDifferentInputs {
					t.Errorf("%s: a divergence of a mixed pair was attributed to %s: %s", order.name, cause, first)
				}
			}
		}
	})
}

// TestM6ExhaustionIsCountedOverTheParticipants pins the denominator of the
// exhaustion line: the measured owners that TOOK PART, the same population
// the numerator is counted over — never the measured reserve as a whole, most
// of which never joins under compensated load. The reserve that never joined
// is shown apart.
//
// ⚠️ Mutation that must break it: printing the exhausted count against the
// original population plus the whole measured reserve.
func TestM6ExhaustionIsCountedOverTheParticipants(t *testing.T) {
	t.Parallel()

	// Compensated load with a long horizon: a big reserve, of which only a
	// small prefix joins.
	config := m6ModelBase()
	config.Churn = churnCompensated
	config.ChurnShare = 0.02
	config.ChurnAt = 0
	config.Ticks = 40
	// Returns fill most of the compensation quota, so most of the reserve —
	// sized for the case nobody returns — never joins.
	config.ReturnShare = 1
	config.ReturnAfter = 2
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	// An EMPTY recording over the whole index space: every participant runs
	// dry from the first tick.
	empty := &m6RecordedStream{Branch: config.Branch, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{}}
	replay := config
	replay.Stream = empty
	report := runM6ModelOn(t, g, replay, everybody)

	if report.ReserveSize == 0 || report.OnlineFromReserve == report.ReserveMeasured {
		t.Fatalf("the fixture needs a reserve that joined only partly: %d of %d joined",
			report.OnlineFromReserve, report.ReserveMeasured)
	}
	if report.StreamExhaustedOwners != report.StreamParticipants {
		t.Fatalf("an empty recording left %d of %d participants exhausted, want all of them",
			report.StreamExhaustedOwners, report.StreamParticipants)
	}
	if report.StreamParticipants >= report.Members+report.ReserveMeasured {
		t.Fatalf("%d participants against %d measured owners in total — the fixture's reserve joined "+
			"entirely, and the two denominators cannot be told apart",
			report.StreamParticipants, report.Members+report.ReserveMeasured)
	}
	line := report.StreamLine()
	want := fmt.Sprintf("%d of the %d measured owners that took part", report.StreamExhaustedOwners,
		report.StreamParticipants)
	if !strings.Contains(line, want) {
		t.Errorf("the report does not count the exhausted owners against the participants (%q):\n%s",
			want, line)
	}
	if !strings.Contains(line, fmt.Sprintf("%d measured reserve identifiers never joined",
		report.Members+report.ReserveMeasured-report.StreamParticipants)) {
		t.Errorf("the report does not show the reserve that never joined apart:\n%s", line)
	}
}

// TestM6TheClearingResetsTheRefreshCursor pins the ‘from scratch’ control as a
// FIRST filling: a refresh cursor accumulated in the table's previous life
// must not decide which member of a refilled level the first refresh probes.
// Two nodes with identical tables must refresh the same member first, whether
// one of them was cleared after many refreshes or started fresh.
//
// ⚠️ Mutation that must break it: clearing the clock but not the cursor.
func TestM6TheClearingResetsTheRefreshCursor(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.StartEmpty = true
	fresh := m6DirectFixture(t, config)
	worn := m6DirectFixture(t, config)

	const owner = int32(0)
	// Fill one level with several members on both, identically.
	byLevel := map[int][]int32{}
	for _, peer := range fresh.neighboursOf(owner) {
		candidate := levelOf(fresh.ids[owner], fresh.ids[peer], config.Shape.degree)
		if candidate >= 0 {
			byLevel[candidate] = append(byLevel[candidate], peer)
		}
	}
	var members []int32
	level := -1
	for candidate := 0; candidate < config.Shape.degree; candidate++ {
		if len(byLevel[candidate]) >= 3 {
			level, members = candidate, byLevel[candidate][:3]
			break
		}
	}
	if level < 0 {
		t.Fatal("the fixture found no level with three acquaintances")
	}
	fill := func(n *m6Network) {
		state := n.states[owner]
		for _, peer := range members {
			clear(state.TriedThisTick)
			n.probe(owner, state, peer, level, false)
		}
		if len(state.Table.members[level]) != 3 {
			t.Fatalf("the level holds %d members, want 3", len(state.Table.members[level]))
		}
	}
	fill(worn)

	// Wear the cursor: several refreshes of that level on the worn node.
	wornState := worn.states[owner]
	for refresh := 0; refresh < 2; refresh++ {
		worn.tick = (refresh + 1) * config.Cadence
		clear(wornState.TriedThisTick)
		if _, _, due := worn.levelDueForRefresh(wornState); !due {
			t.Fatalf("refresh %d: the level is not due", refresh)
		}
	}
	if wornState.RefreshCursor[level] == 0 {
		t.Fatal("the cursor did not advance; the fixture wore nothing")
	}

	// The clearing, then the same refill on both.
	worn.tick = 3 * config.Cadence
	if err := worn.clearForTheFromScratchControl(); err != nil {
		t.Fatalf("clearing: %v", err)
	}
	fresh.tick = worn.tick
	fill(worn)
	fill(fresh)

	// The first refresh after the fill must pick the same member on both.
	worn.tick += config.Cadence
	fresh.tick = worn.tick
	clear(wornState.TriedThisTick)
	clear(fresh.states[owner].TriedThisTick)
	_, wornPick, wornDue := worn.levelDueForRefresh(wornState)
	_, freshPick, freshDue := fresh.levelDueForRefresh(fresh.states[owner])
	if !wornDue || !freshDue {
		t.Fatalf("the level is not due for a refresh (worn %v, fresh %v)", wornDue, freshDue)
	}
	if wornPick != freshPick {
		t.Fatalf("the cleared node refreshes %d first and the fresh node %d: the cursor of the "+
			"table's previous life survived the clearing", wornPick, freshPick)
	}
}

// TestM6APermanentlyUnusableEntryDoesNotKeepAStreamAlive pins the exhaustion
// count against the hand-out's permanent exclusions: a pool entry whose
// candidate shares the owner's whole addressable prefix is written off by its
// first probe (no bucket can ever hold it), is never offered again, and must
// not count as "left" — while a temporary refusal (tried this tick) must.
//
// ⚠️ Mutation that must break it: counting entries without consulting the
// owner's Exhausted set.
func TestM6APermanentlyUnusableEntryDoesNotKeepAStreamAlive(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchB // aims at no level, so the unusable candidate is offered
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	const owner, other = int32(0), int32(1)
	unusable := int32(-1)
	for _, candidate := range base.joinedOrder {
		if candidate != owner && levelOf(base.ids[owner], base.ids[candidate], config.Shape.degree) < 0 {
			unusable = candidate
			break
		}
	}
	if unusable < 0 {
		t.Fatal("the fixture found no node sharing the owner's whole addressable prefix")
	}
	live := base.neighboursOf(other)[0]

	stream := &m6RecordedStream{Branch: branchB, IDs: base.ids, Members: base.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{
			owner: {{Tick: 0, Source: offerAcquaintance, Candidate: unusable}},
			other: {{Tick: 0, Source: offerAcquaintance, Candidate: live}},
		}}
	replay := config
	replay.Stream = stream
	network, err := newM6Network(g, replay, everybody)
	if err != nil {
		t.Fatalf("preparing the replay: %v", err)
	}
	state := network.states[owner]
	_, participants, _ := network.streamExhaustion()

	// Before anything is probed the owner's entry counts as left.
	if exhausted, _, _ := network.streamExhaustion(); exhausted != participants-2 {
		t.Fatalf("before the first tick %d of %d participants read as exhausted, want %d (two owners "+
			"hold a live entry)", exhausted, participants, participants-2)
	}

	// Tick 0: the entry is offered once and written off for good.
	network.tick = 0
	clear(state.TriedThisTick)
	if !network.probeOnceInTheNetwork(owner, state) {
		t.Fatal("the unusable entry was not offered at all")
	}
	if _, never := state.Exhausted[unusable]; !never {
		t.Fatalf("the probe did not write %d off although no bucket can hold it", unusable)
	}

	// Tick 1: nothing is offered, and the owner reads as exhausted; the other
	// owner's live entry keeps it out of the count.
	network.tick = 1
	clear(state.TriedThisTick)
	if network.probeOnceInTheNetwork(owner, state) {
		t.Fatal("the written-off entry was offered again")
	}
	if exhausted, _, _ := network.streamExhaustion(); exhausted != participants-1 {
		t.Fatalf("after the write-off %d of %d participants read as exhausted, want %d: the "+
			"unusable entry still counts as left", exhausted, participants, participants-1)
	}

	// And a TEMPORARY refusal is not an exclusion: the other owner's entry,
	// tried this tick, is still left.
	network.states[other].TriedThisTick[live] = struct{}{}
	if exhausted, _, _ := network.streamExhaustion(); exhausted != participants-1 {
		t.Fatalf("an entry tried this tick was counted as gone: %d exhausted, want %d",
			exhausted, participants-1)
	}
}

// TestM6RecordStreamRefusesWhatItCannotRecord is the door for recordings and
// replays.
func TestM6RecordStreamRefusesWhatItCannotRecord(t *testing.T) {
	t.Parallel()

	untraced := runM6Model(t, m6ModelBase())
	if _, err := recordStream(untraced); err == nil {
		t.Error("a run without an offer trace was recorded")
	}

	config := m6ModelBase()
	config.TraceOffers = true
	traced := runM6Model(t, config)
	stream, err := recordStream(traced)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	if stream.Entries() == 0 {
		t.Fatal("the recording is empty")
	}

	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	mismatched := config
	mismatched.Stream = stream
	mismatched.Branch = branchAPrime
	if _, err := newM6Network(g, mismatched, everybody); err == nil ||
		!strings.Contains(err.Error(), "recorded from") {
		t.Errorf("a replay under another branch was accepted: %v", err)
	}

	replay := config
	replay.Stream = stream
	replayed := runM6ModelOn(t, g, replay, everybody)
	if _, err := recordStream(replayed); err == nil {
		t.Error("a replay was recorded as if it were a source")
	}

	// ⚠️ A recording is pinned to the identifiers it was made over. The same
	// index in another population is another participant; on a smaller
	// network it is nobody.
	t.Run("another seed is another population", func(t *testing.T) {
		other := buildGraph(config.Shape, config.Seed+1, config.Quota, config.Policy)
		foreign := replay
		foreign.Seed = config.Seed + 1
		if _, err := newM6Network(other, foreign, everybody); err == nil ||
			!strings.Contains(err.Error(), "different identifier") {
			t.Errorf("a recording of seed %d was replayed over seed %d: %v", config.Seed, config.Seed+1, err)
		}
	})
	t.Run("a smaller population is out of range", func(t *testing.T) {
		small := shape{name: "256×8", nodes: 256, degree: 8, budget: 16}
		short := replay
		short.Shape = small
		if _, err := newM6Network(buildGraph(small, config.Seed, config.Quota, config.Policy),
			short, everybody); err == nil || !strings.Contains(err.Error(), "identifiers") {
			t.Errorf("a recording over %d identifiers was replayed over %d: %v",
				len(stream.IDs), small.nodes, err)
		}
	})
	t.Run("a different reserve is a different index space", func(t *testing.T) {
		// Compensated load draws a reserve sized for the horizon; the recording
		// was made without one, so the indices past the population mean nothing
		// to it.
		withReserve := replay
		withReserve.Churn = churnCompensated
		if _, err := newM6Network(g, withReserve, everybody); err == nil ||
			!strings.Contains(err.Error(), "identifiers") {
			t.Errorf("a recording without a reserve was replayed over a run with one: %v", err)
		}
	})
}

// TestM6AReplayNeverHandsAMeasuredOwnerANonMember is the membership gate of
// the replay, both halves: a recording made under another membership is
// refused at the door, and — fail-closed — an entry naming a non-member is
// never handed to a measured owner even when the recording is compatible.
//
// ⚠️ Mutations that must break it: dropping the membership marks from the
// compatibility check; dropping mayTake from the hand-out.
func TestM6AReplayNeverHandsAMeasuredOwnerANonMember(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.TraceOffers = true
	config.Membership = "Q half"
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	// A full-graph recording on the same identifiers is refused for the Q run.
	full := config
	full.Membership = "whole network"
	recording := runM6ModelOn(t, g, full, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	replay := config
	replay.Stream = stream
	if _, err := newM6Network(g, replay, structural); err == nil ||
		!strings.Contains(err.Error(), "another membership") {
		t.Fatalf("a full-graph recording was accepted for a Q-half replay: %v", err)
	}

	// A compatible recording whose entries name a non-member: the Q owner is
	// never offered it.
	q, err := newM6Network(g, config, structural)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	owner, outsider := int32(-1), int32(-1)
	for _, node := range q.joinedOrder {
		if owner < 0 && structural(q.ids[node]) {
			owner = node
		}
	}
	for _, node := range q.joinedOrder {
		if !structural(q.ids[node]) && node != owner &&
			levelOf(q.ids[owner], q.ids[node], config.Shape.degree) >= 0 {
			outsider = node
			break
		}
	}
	if owner < 0 || outsider < 0 {
		t.Fatal("the fixture found no Q owner or no ¬Q candidate with a level")
	}
	forged := &m6RecordedStream{
		Branch: config.Branch, IDs: q.ids, Members: q.trace.Members,
		ByOwner: map[int32][]m6StreamEntry{
			owner: {{Tick: 0, Source: offerAcquaintance, Candidate: outsider}},
		},
	}
	replay.Stream = forged
	network, err := newM6Network(g, replay, structural)
	if err != nil {
		t.Fatalf("preparing the replay: %v", err)
	}
	state := network.states[owner]
	network.tick = 0
	// Bounded: without the gate the forged entry is re-offered for ever (a
	// temporary refusal keeps it), and a hang is not the failure wanted here.
	for attempt := 0; attempt < 8; attempt++ {
		clear(state.TriedThisTick)
		if !network.probeOnceInTheNetwork(owner, state) {
			break
		}
	}
	if state.Table.holds(outsider) {
		t.Fatalf("the Q owner %d stored the ¬Q candidate %d from the recording", owner, outsider)
	}
	for _, offer := range network.trace.Offers {
		if offer.Owner == owner && offer.Peer == outsider {
			t.Fatalf("the Q owner %d was offered the ¬Q candidate %d: %s", owner, outsider, offer)
		}
	}
}

// TestM6AMissingEdgeIsBlamedOnTheWorldAndNotOnMemory is the cause classifier
// on the case a live owner is not enough for: both owners serve, but one
// run's world lacks the edge the other run's offer came over. That is network
// state, and the comparison must say so rather than blame the owner's memory.
//
// ⚠️ Mutation that must break it: attributing "no offer from a live owner" to
// memory without checking the source's availability.
func TestM6AMissingEdgeIsBlamedOnTheWorldAndNotOnMemory(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchA, false)
	config.Churn = churnNone
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base := runM6ModelOn(t, g, config, everybody)
	config.ReplayPhases = base.PhaseBoundaries()

	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	control, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	// The one difference: owner 0 and its first acquaintance are not
	// connected in the control's world. Both owners stay online and serve.
	const owner = int32(0)
	peer := control.neighboursOf(owner)[0]
	delete(control.held[owner], peer)
	delete(control.held[peer], owner)

	comparison, err := compareM6Runs(main, control)
	if err != nil {
		t.Fatalf("comparing: %v", err)
	}
	if comparison.Holds(claimSameHeldEdges) {
		t.Fatalf("the held-edge claim held although an edge was removed:\n%s", comparison)
	}
	first := comparison.FirstByCause[causeNetworkState]
	if first == nil {
		t.Fatalf("no divergence was attributed to the world:\n%s", comparison)
	}
	if first.Owner != owner && first.Owner != peer {
		t.Fatalf("the world was blamed for owner %d, the edge was removed between %d and %d:\n%s",
			first.Owner, owner, peer, comparison)
	}
	for _, divergence := range []*m6Divergence{comparison.FirstByCause[causeLocalMemory]} {
		if divergence != nil && (divergence.Owner == owner || divergence.Owner == peer) &&
			divergence.Tick <= first.Tick {
			t.Fatalf("memory was blamed for the owner whose edge is missing, before the world was:\n%s",
				comparison)
		}
	}
	if !strings.Contains(first.Detail, "MISSING from the world") {
		t.Errorf("the detail does not name the missing source: %s", first)
	}
	t.Logf("%s", comparison)

	t.Run("a live owner with no source at all", func(t *testing.T) {
		// The path the reviewer named: one run's owner makes NO offer. It is
		// online and serving; what it lacks is every edge. Blaming its memory
		// would be wrong, and the classifier has to look at the source.
		main, err := newM6Network(g, config, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		control, err := newM6Network(g, config, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		for peer := range control.held[owner] {
			delete(control.held[peer], owner)
		}
		control.held[owner] = map[int32]struct{}{}

		comparison, err := compareM6Runs(main, control)
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		if comparison.Divergence == nil || comparison.Divergence.Owner != owner ||
			comparison.Divergence.Control != nil {
			t.Fatalf("expected owner %d to make no offer in the control:\n%s", owner, comparison)
		}
		if comparison.Divergence.Cause != causeNetworkState {
			t.Fatalf("a live owner with no edges was blamed on %s:\n%s", comparison.Divergence.Cause,
				comparison)
		}
	})
}

// TestM6TheComparisonNamesTheClaimThatFails checks the checker: a pair that
// differs in something other than memory fails the inputs claim and says so,
// and a pair on two graphs is refused.
func TestM6TheComparisonNamesTheClaimThatFails(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchA, false)
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	base := runM6ModelOn(t, g, config, everybody)
	config.ReplayPhases = base.PhaseBoundaries()

	main, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	other := config
	other.StartEmpty = true
	other.Cadence = config.Cadence * 2
	control, err := newM6Network(g, other, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	comparison, err := compareM6Runs(main, control)
	if err != nil {
		t.Fatalf("comparing: %v", err)
	}
	inputs := comparison.claim(claimSameInputs)
	if inputs.Status != claimFails || !strings.Contains(comparison.String(), "same inputs") ||
		!strings.Contains(comparison.String(), "NO LONGER HOLDS") {
		t.Fatalf("a pair differing in the cadence passed the inputs claim:\n%s", comparison)
	}

	// ⚠️ Compared unrounded: 0.011 and 0.014 both print as 0.01.
	rounding := config
	rounding.ChurnShare = 0.011
	alike := config
	alike.ChurnShare = 0.014
	alike.StartEmpty = true
	pair := func(name string, left, right m6ModelConfig, leftMember, rightMember func(nodeID) bool) {
		t.Helper()
		one, err := newM6Network(g, left, leftMember)
		if err != nil {
			t.Fatalf("%s: preparing: %v", name, err)
		}
		two, err := newM6Network(g, right, rightMember)
		if err != nil {
			t.Fatalf("%s: preparing: %v", name, err)
		}
		verdict, err := compareM6Runs(one, two)
		if err != nil {
			t.Fatalf("%s: comparing: %v", name, err)
		}
		if verdict.Holds(claimSameInputs) {
			t.Errorf("%s: the inputs claim held for different inputs:\n%s", name, verdict)
		}
	}
	pair("shares that round alike", rounding, alike, everybody, everybody)

	// ⚠️ Compared by what the predicate says about every identifier, not by
	// its name: two different memberships can carry the same label.
	scratch := config
	scratch.StartEmpty = true
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
	pair("memberships with one name", config, scratch, everybody, structural)

	// ⚠️ And the two interval claims are judged independently: a pair that
	// differs from the first tick fails BOTH, not only the one the first
	// divergence fell into.
	looser := config
	looser.StartEmpty = true
	looser.Repair = config.Repair + 1
	one, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	two, err := newM6Network(g, looser, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	verdict, err := compareM6Runs(one, two)
	if err != nil {
		t.Fatalf("comparing: %v", err)
	}
	if verdict.Holds(claimOffersSameBeforeOnset) || verdict.Holds(claimOffersSameAfterOnset) {
		t.Errorf("a pair that differs from tick 0 kept an interval claim:\n%s", verdict)
	}
	if verdict.Divergence == nil || verdict.Divergence.Tick != 0 {
		t.Errorf("the first divergence is not at tick 0:\n%s", verdict)
	}

	// ⚠️ One graph is not one index space: under compensated churn the reserve
	// is sized for the horizon, so two runs of different lengths carry
	// different numbers of identifiers, and every per-node check would read
	// past the shorter one. Refused as an error, in either order.
	short := m6ModelBase()
	short.Churn = churnCompensated
	short.TraceOffers = true
	short.Ticks = 6
	long := short
	long.Ticks = 48
	for _, order := range []struct {
		name        string
		left, right m6ModelConfig
	}{{"short then long", short, long}, {"long then short", long, short}} {
		left, err := newM6Network(g, order.left, everybody)
		if err != nil {
			t.Fatalf("%s: preparing: %v", order.name, err)
		}
		right, err := newM6Network(g, order.right, everybody)
		if err != nil {
			t.Fatalf("%s: preparing: %v", order.name, err)
		}
		if len(left.ids) == len(right.ids) {
			t.Fatalf("%s: both runs carry %d identifiers — the fixture did not make the reserves differ",
				order.name, len(left.ids))
		}
		if _, err := compareM6Runs(left, right); err == nil ||
			!strings.Contains(err.Error(), "not the same space") {
			t.Errorf("%s: runs with different reserves were compared (err=%v)", order.name, err)
		}
	}

	elsewhere := buildGraph(config.Shape, config.Seed+1, config.Quota, config.Policy)
	stranger, err := newM6Network(elsewhere, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	fresh, err := newM6Network(g, config, everybody)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	if _, err := compareM6Runs(fresh, stranger); err == nil {
		t.Error("two runs on different graphs were compared")
	}
}

// TestM6AMissingOwnerIsNotAnEmptyExposure pins the exposure comparison on the
// composition of the snapshots, not only on their contents: an owner present
// in one snapshot and absent from the other is a divergence on its own —
// separate from an owner whose exposure is empty on both sides, which is
// agreement — and it is one whichever side is missing it. Otherwise an
// incomplete snapshot (a tracing bug, or a cleared half that recorded fewer
// owners) passes the ‘same pool’ and ‘same recorded stream’ claims.
//
// ⚠️ Mutations that must break it: walking only the main snapshot (an owner
// only in the control goes unnoticed); the replay path reading only the
// tables half (a missing owner lands in the world half and is ignored);
// treating a missing owner as an empty exposure.
func TestM6AMissingOwnerIsNotAnEmptyExposure(t *testing.T) {
	t.Parallel()

	t.Run("the divergence lists absent owners apart from differing ones", func(t *testing.T) {
		empty := m6Exposure{ByNeighbour: map[int32][]int32{}}
		some := m6Exposure{Acquaintances: []int32{7}, ByNeighbour: map[int32][]int32{}}
		main := map[int32]m6Exposure{1: empty, 2: some, 3: some}
		control := map[int32]m6Exposure{1: empty, 2: empty, 4: some}
		absent, world, tables := exposureDivergence(main, control)
		if !equalNodes(absent, []int32{3, 4}) {
			t.Fatalf("absent owners: got %v, want [3 4] — one missing from each side", absent)
		}
		if !equalNodes(world, []int32{2}) || len(tables) != 0 {
			t.Fatalf("differing owners: world %v tables %v, want world [2] only", world, tables)
		}
		if absent, world, tables := exposureDivergence(main, main); len(absent)+len(world)+len(tables) != 0 {
			t.Fatalf("a snapshot compared with itself diverges: absent %v world %v tables %v",
				absent, world, tables)
		}
	})

	config := m6PairBase(branchA, false)
	config.Churn = churnNone
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}

	// Two fresh networks whose onset snapshots are written by hand, so that
	// the check sees exactly the composition the case names.
	pair := func(t *testing.T, replay bool) (*m6Network, *m6Network) {
		t.Helper()
		prepared := config
		prepared.ReplayPhases = recording.PhaseBoundaries()
		if replay {
			prepared.Stream = stream
		}
		main, err := newM6Network(g, prepared, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		control, err := newM6Network(g, prepared, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		for _, network := range []*m6Network{main, control} {
			network.trace.OnsetTick = 3
			for _, owner := range network.owners {
				exposure := m6Exposure{ByNeighbour: map[int32][]int32{}}
				if !replay {
					exposure.Acquaintances = []int32{owner + 1}
				}
				network.trace.ExposureAtOnset[owner] = exposure
			}
		}
		return main, control
	}
	check := func(main, control *m6Network) *m6Comparison {
		comparison := newM6Comparison()
		comparison.prepareStreamClaims(main, control)
		comparison.checkExposureAtOnset(main, control)
		return comparison
	}

	for _, mode := range []struct {
		name   string
		replay bool
		claim  m6ClaimKind
	}{
		{"adaptive pair, the pool claim", false, claimSamePoolAtOnset},
		{"replayed pair, the recorded-stream claim", true, claimSameRecordedStream},
	} {
		t.Run(mode.name, func(t *testing.T) {
			main, control := pair(t, mode.replay)
			if got := check(main, control); !got.Holds(mode.claim) {
				t.Fatalf("identical snapshots fail the claim:\n%s", got)
			}

			const owner = int32(0)
			for _, side := range []struct {
				name string
				from func(main, control *m6Network) *m6Network
			}{
				{"missing from the control", func(_, control *m6Network) *m6Network { return control }},
				{"missing from the main run", func(main, _ *m6Network) *m6Network { return main }},
			} {
				main, control := pair(t, mode.replay)
				delete(side.from(main, control).trace.ExposureAtOnset, owner)
				got := check(main, control)
				if got.Holds(mode.claim) {
					t.Fatalf("%s: owner %d is %s and the claim still holds:\n%s", mode.name, owner,
						side.name, got)
				}
				if detail := got.claim(mode.claim).Detail; !strings.Contains(detail, "missing") {
					t.Fatalf("%s: the failure does not say the owner is missing (it is not an empty "+
						"exposure): %s", side.name, detail)
				}
			}

			// An owner with NOTHING to be offered — an empty exposure on both
			// sides — is agreement, not absence.
			main, control = pair(t, mode.replay)
			main.trace.ExposureAtOnset[owner] = m6Exposure{ByNeighbour: map[int32][]int32{}}
			control.trace.ExposureAtOnset[owner] = m6Exposure{ByNeighbour: map[int32][]int32{}}
			if got := check(main, control); !got.Holds(mode.claim) {
				t.Fatalf("an owner with an empty exposure on both sides fails the claim:\n%s", got)
			}
			// …and that owner missing from one side is still absence.
			delete(control.trace.ExposureAtOnset, owner)
			if got := check(main, control); got.Holds(mode.claim) {
				t.Fatalf("an owner with an empty stream, missing from the control, passes:\n%s", got)
			}
		})
	}
}

// TestM6APairWithoutAnOnsetDoesNotClaimWhatItNeverChecked pins the verdict
// of a pair whose onset never came (no churn form, or a churn that took
// nobody): the claims that are judged AT the onset — the pool, the responder
// tables — and the claim about the interval AFTER it were never checked, and
// a verdict that left them at their initial HOLDS was reporting checks it had
// not made. They are ‘not applicable’ with the reason; the offer claim for
// the interval actually played and the recorded-stream claim (fingerprints,
// no responder consulted) are still judged. A pair with an onset is the
// positive control: there the same claims are applicable.
//
// ⚠️ Mutation that must break it: leaving the onset claims at HOLDS when
// OnsetTick stays -1.
func TestM6APairWithoutAnOnsetDoesNotClaimWhatItNeverChecked(t *testing.T) {
	t.Parallel()

	atOnset := []m6ClaimKind{claimSamePoolAtOnset, claimSameResponderTablesAtOnset, claimOffersSameAfterOnset}
	requireNotApplicableForWantOfAnOnset := func(t *testing.T, comparison *m6Comparison, kinds ...m6ClaimKind) {
		t.Helper()
		for _, kind := range kinds {
			claim := comparison.claim(kind)
			// A claim a replay pair had already set aside keeps its own reason;
			// every other one has to name the missing onset.
			if claim.Status != claimNotApplicable ||
				(kind == claimOffersSameAfterOnset && !strings.Contains(claim.Detail, "onset")) {
				t.Fatalf("the claim %q is %s (%q) although the onset never came:\n%s", kind,
					claim.Status, claim.Detail, comparison)
			}
		}
		if comparison.OnsetTick >= 0 || strings.Contains(comparison.String(), "onset at tick -1") {
			t.Fatalf("the verdict reports an onset it never had:\n%s", comparison)
		}
	}

	for _, branch := range []m6Branch{branchAPrime, branchC} {
		t.Run("adaptive pair without churn, "+branch.String(), func(t *testing.T) {
			t.Parallel()
			config := m6PairBase(branch, false)
			config.Churn = churnNone
			comparison, _, _ := adaptivePair(t, config)
			requireNotApplicableForWantOfAnOnset(t, comparison, atOnset...)
			// The interval that WAS played is judged: two identical runs agree.
			if before := comparison.claim(claimOffersSameBeforeOnset); before.Status != claimHolds {
				t.Fatalf("the offers of the played interval were not judged as holding:\n%s", comparison)
			}
			requireClaims(t, comparison, claimSameInputs, claimSameBoundaries, claimSameHeldEdges)
		})
	}

	t.Run("replayed pair without churn keeps the recorded-stream check", func(t *testing.T) {
		t.Parallel()
		config := m6PairBase(branchAPrime, false)
		config.Churn = churnNone
		comparison, _, _, _ := recordedPair(t, config)
		requireNotApplicableForWantOfAnOnset(t, comparison, atOnset...)
		if stream := comparison.claim(claimSameRecordedStream); stream.Status != claimHolds {
			t.Fatalf("the recorded-stream claim was not judged independently of the onset:\n%s", comparison)
		}
	})

	t.Run("a pair with an onset is the positive control", func(t *testing.T) {
		t.Parallel()
		comparison, _, _ := adaptivePair(t, m6PairBase(branchAPrime, false))
		if comparison.OnsetTick < 0 {
			t.Fatalf("the shock pair had no onset:\n%s", comparison)
		}
		for _, kind := range atOnset {
			if comparison.claim(kind).Status == claimNotApplicable {
				t.Fatalf("the claim %q is not applicable although the onset came at tick %d:\n%s",
					kind, comparison.OnsetTick, comparison)
			}
		}
	})
}

// TestM6AOneSidedOnsetIsAMismatchAndNotAnAbsence pins the onset verdict on
// BOTH halves: an onset that came in one run and not in the other, or on
// different ticks, is a mismatch the verdict names with both ticks — never
// "no onset", which is true only when neither half had one — and the verdict
// is the same whichever half is passed first. The claims judged at or after
// the onset fail on it rather than being set aside.
//
// ⚠️ Mutation that must break it: reading the onset off the main half alone
// (the control's onset then reads as "no onset" in one order and as a shared
// onset in the other).
func TestM6AOneSidedOnsetIsAMismatchAndNotAnAbsence(t *testing.T) {
	t.Parallel()

	atOnset := []m6ClaimKind{claimSamePoolAtOnset, claimSameResponderTablesAtOnset, claimOffersSameAfterOnset}
	g := buildGraph(m6ModelShape(), 1, 1, policyInitiatedLimit)

	requireMismatch := func(t *testing.T, comparison *m6Comparison, left, right int) {
		t.Helper()
		verdict := comparison.String()
		if strings.Contains(verdict, "NO ONSET") || comparison.OnsetTick < 0 {
			t.Fatalf("a one-sided or unequal onset was reported as no onset at all:\n%s", verdict)
		}
		if !strings.Contains(verdict, "MISMATCH") {
			t.Fatalf("the verdict does not name the onset mismatch:\n%s", verdict)
		}
		for _, kind := range atOnset {
			claim := comparison.claim(kind)
			if claim.Status != claimFails || !strings.Contains(claim.Detail, "onset") {
				t.Fatalf("the claim %q is %s (%q) although the onsets differ:\n%s", kind, claim.Status,
					claim.Detail, verdict)
			}
		}
		for _, tick := range []int{left, right} {
			want := fmt.Sprintf("tick %d", tick)
			if tick < 0 {
				want = "never"
			}
			if !strings.Contains(comparison.claim(claimSamePoolAtOnset).Detail, want) {
				t.Fatalf("the mismatch does not name %q:\n%s", want, verdict)
			}
		}
	}
	bothOrders := func(t *testing.T, name string, one, other m6ModelConfig, oneOnset, otherOnset int) {
		t.Helper()
		for _, order := range []struct {
			name        string
			left, right m6ModelConfig
			lo, ro      int
		}{{"main first", one, other, oneOnset, otherOnset}, {"control first", other, one, otherOnset, oneOnset}} {
			main, err := newM6Network(g, order.left, everybody)
			if err != nil {
				t.Fatalf("%s, %s: preparing: %v", name, order.name, err)
			}
			control, err := newM6Network(g, order.right, everybody)
			if err != nil {
				t.Fatalf("%s, %s: preparing: %v", name, order.name, err)
			}
			comparison, err := compareM6Runs(main, control)
			if err != nil {
				t.Fatalf("%s, %s: comparing: %v", name, order.name, err)
			}
			requireMismatch(t, comparison, order.lo, order.ro)
		}
	}

	// The flat fixture schedule: the onset is the churn tick, and a shock share
	// of zero takes nobody, so that half never has one.
	// Branch A′, so that all three onset claims apply and every one of them
	// has to fail on the mismatch.
	flat := m6ModelBase()
	flat.Branch = branchAPrime
	flat.TraceOffers = true
	flat.Ticks, flat.ChurnAt = 12, 4

	t.Run("onset in one half only", func(t *testing.T) {
		t.Parallel()
		none, some := flat, flat
		none.ChurnShare, some.ChurnShare = 0, 0.25
		bothOrders(t, "one-sided", none, some, -1, 4)
	})

	t.Run("onsets on different ticks", func(t *testing.T) {
		t.Parallel()
		early, late := flat, flat
		late.ChurnAt = 6
		bothOrders(t, "different ticks", early, late, 4, 6)
	})

	t.Run("a shared onset is neither", func(t *testing.T) {
		t.Parallel()
		main, err := newM6Network(g, flat, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		control, err := newM6Network(g, flat, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		comparison, err := compareM6Runs(main, control)
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		if comparison.OnsetTick != 4 || strings.Contains(comparison.String(), "MISMATCH") ||
			strings.Contains(comparison.String(), "NO ONSET") {
			t.Fatalf("two identical halves do not share their onset at tick 4:\n%s", comparison)
		}
	})
}

// TestM6AReplayRefusesAnEntryOfANonStreamSource is the door for the SOURCE of
// a recorded entry: recordStream only ever writes the four external sources
// (acquaintance, omniscient, exchange, addressed), but a hand-built recording
// can carry anything — refresh (the zero value, the easiest to leave by
// accident), shelf, queue, or a number that names no source — and the replay
// used to accept it and hand the candidate out as a queue record. Every entry
// is checked when the network is built; the four external sources pass.
//
// ⚠️ Mutation that must break it: checking identifiers and membership only.
func TestM6AReplayRefusesAnEntryOfANonStreamSource(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchAPrime, false)
	config.Churn = churnNone
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	const owner = int32(0)
	if len(stream.ByOwner[owner]) == 0 {
		t.Fatal("the recording has no entry for owner 0 to tamper with")
	}
	withSource := func(source m6OfferSource) *m6RecordedStream {
		tampered := *stream
		tampered.ByOwner = map[int32][]m6StreamEntry{}
		for node, entries := range stream.ByOwner {
			tampered.ByOwner[node] = append([]m6StreamEntry(nil), entries...)
		}
		tampered.ByOwner[owner][0].Source = source
		return &tampered
	}

	for _, entry := range []struct {
		source   m6OfferSource
		accepted bool
	}{
		{offerAcquaintance, true}, {offerOmniscient, true}, {offerExchange, true}, {offerAddressed, true},
		{offerRefresh, false}, {offerShelf, false}, {offerQueue, false}, {m6OfferSource(255), false},
	} {
		replay := config
		replay.Stream = withSource(entry.source)
		replay.ReplayPhases = recording.PhaseBoundaries()
		_, err := newM6Network(g, replay, everybody)
		switch {
		case entry.accepted && err != nil:
			t.Errorf("an entry of the external source %s was refused: %v", entry.source, err)
		case !entry.accepted && err == nil:
			t.Errorf("an entry of source %d (%s) was accepted as part of the recorded stream",
				entry.source, entry.source)
		case !entry.accepted && !strings.Contains(err.Error(), "source"):
			t.Errorf("the refusal of source %d does not name the source: %v", entry.source, err)
		}
	}
}

// TestM6AReplayRefusesAMalformedRecording completes the door for a hand-built
// recording (П-5): an owner index or a candidate index outside the recorded
// identifiers, a negative tick, and ticks that DECREASE within one owner are
// refused when the network is built. recordStream never produces any of
// these; without the door an out-of-range candidate panicked in the hand-out
// and a decreasing tick delayed an available entry until the earlier one
// became available. Equal ticks are in order and pass.
//
// ⚠️ Mutation that must break it: checking the source of the entries only.
func TestM6AReplayRefusesAMalformedRecording(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchAPrime, false)
	config.Churn = churnNone
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	const owner = int32(0)
	if len(stream.ByOwner[owner]) < 2 {
		t.Fatal("the recording has fewer than two entries for owner 0")
	}
	copyOf := func() *m6RecordedStream {
		tampered := *stream
		tampered.ByOwner = map[int32][]m6StreamEntry{}
		for node, entries := range stream.ByOwner {
			tampered.ByOwner[node] = append([]m6StreamEntry(nil), entries...)
		}
		return &tampered
	}
	tooFar := int32(len(stream.IDs))

	for _, entry := range []struct {
		name     string
		tamper   func(*m6RecordedStream)
		accepted bool
		names    string
	}{
		{"candidate index beyond the identifiers", func(s *m6RecordedStream) {
			s.ByOwner[owner][0].Candidate = tooFar
		}, false, "candidate"},
		{"owner index beyond the identifiers", func(s *m6RecordedStream) {
			s.ByOwner[tooFar] = []m6StreamEntry{{Tick: 0, Candidate: 1, Source: offerAcquaintance}}
		}, false, "owner"},
		{"negative tick", func(s *m6RecordedStream) {
			s.ByOwner[owner][0].Tick = -1
		}, false, "tick"},
		{"ticks decreasing within one owner", func(s *m6RecordedStream) {
			s.ByOwner[owner][0].Tick, s.ByOwner[owner][1].Tick = 5, 2
		}, false, "tick"},
		{"equal ticks within one owner", func(s *m6RecordedStream) {
			// The first entry moved up to the second's tick: equal, still in
			// order with everything after it.
			s.ByOwner[owner][0].Tick = s.ByOwner[owner][1].Tick
		}, true, ""},
		{"untouched recording", func(*m6RecordedStream) {}, true, ""},
	} {
		replay := config
		replay.Stream = copyOf()
		entry.tamper(replay.Stream)
		replay.ReplayPhases = recording.PhaseBoundaries()
		_, err := newM6Network(g, replay, everybody)
		switch {
		case entry.accepted && err != nil:
			t.Errorf("%s: refused: %v", entry.name, err)
		case !entry.accepted && err == nil:
			t.Errorf("%s: accepted", entry.name)
		case !entry.accepted && !strings.Contains(err.Error(), entry.names):
			t.Errorf("%s: the refusal does not name the %s: %v", entry.name, entry.names, err)
		}
	}
}

// TestM6TheFingerprintCoversEveryFieldOfARecording pins the fingerprint
// against the fields a replay or the report reads: the identifiers, the
// membership marks, every entry (tick, source, candidate) — and the metadata
// that used to be left out: the source the stream was recorded from
// (Branch, Omniscient) and the cost the recording run paid for it (Exchanges,
// Answers). Two recordings that differ in any ONE of these are two
// recordings, and the ‘same recorded stream’ claim must not hold across them.
//
// ⚠️ Mutation that must break it: hashing the identifiers, marks and entries
// only.
func TestM6TheFingerprintCoversEveryFieldOfARecording(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchAPrime, false)
	config.Churn = churnNone
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, everybody)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	const owner = int32(0)
	if len(stream.ByOwner[owner]) == 0 {
		t.Fatal("the recording has no entry for owner 0")
	}
	base := stream.fingerprint()
	if base != stream.fingerprint() {
		t.Fatal("the fingerprint is not deterministic")
	}
	copyOf := func() *m6RecordedStream {
		tampered := *stream
		tampered.IDs = append([]nodeID(nil), stream.IDs...)
		tampered.Members = append([]bool(nil), stream.Members...)
		tampered.ByOwner = map[int32][]m6StreamEntry{}
		for node, entries := range stream.ByOwner {
			tampered.ByOwner[node] = append([]m6StreamEntry(nil), entries...)
		}
		return &tampered
	}

	seen := map[string]string{base: "the untouched recording"}
	for _, field := range []struct {
		name   string
		tamper func(*m6RecordedStream)
	}{
		{"Branch", func(s *m6RecordedStream) { s.Branch = branchC }},
		{"Omniscient", func(s *m6RecordedStream) { s.Omniscient = !s.Omniscient }},
		{"Exchanges", func(s *m6RecordedStream) { s.Exchanges++ }},
		{"Answers", func(s *m6RecordedStream) { s.Answers++ }},
		{"IDs", func(s *m6RecordedStream) { s.IDs[0][0] ^= 0x80 }},
		{"Members", func(s *m6RecordedStream) { s.Members[0] = !s.Members[0] }},
		{"an entry's tick", func(s *m6RecordedStream) { s.ByOwner[owner][0].Tick++ }},
		{"an entry's source", func(s *m6RecordedStream) { s.ByOwner[owner][0].Source = offerAddressed }},
		{"an entry's candidate", func(s *m6RecordedStream) { s.ByOwner[owner][0].Candidate ^= 1 }},
		{"an owner's entry count", func(s *m6RecordedStream) { s.ByOwner[owner] = s.ByOwner[owner][1:] }},
	} {
		tampered := copyOf()
		field.tamper(tampered)
		got := tampered.fingerprint()
		if other, clash := seen[got]; clash {
			t.Errorf("changing %s leaves the fingerprint equal to that of %s", field.name, other)
		}
		seen[got] = "the recording with " + field.name + " changed"
	}
}

// TestM6TheReplayExposureAppliesTheMeasurementFilter pins the exposure of a
// replay against the hand-out: a candidate the measurement never lets a
// measured owner take (a ¬Q candidate for a Q owner) is not part of what the
// recording can offer that owner, so it is not in its exposure either — the
// same mayTake the hand-out and the exhaustion count apply. An unmeasured
// owner is under no such restriction and keeps the entry. The MEMORY filters
// (held, tried this tick, exhausted) stay out: the exposure is the source
// before memory.
//
// ⚠️ Mutation that must break it: listing every available, unconsumed entry
// without mayTake.
func TestM6TheReplayExposureAppliesTheMeasurementFilter(t *testing.T) {
	t.Parallel()

	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
	config := m6PairBase(branchAPrime, false)
	config.Churn = churnNone
	config.Membership = "structural (Q) half"
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	recording := runM6ModelOn(t, g, config, structural)
	stream, err := recordStream(recording)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	pick := func(measured bool) int32 {
		for index, id := range stream.IDs {
			if structural(id) == measured && len(stream.ByOwner[int32(index)]) > 0 {
				return int32(index)
			}
		}
		t.Fatalf("no owner with measured=%v and a recorded entry", measured)
		return -1
	}
	measuredOwner, unmeasuredOwner := pick(true), pick(false)
	var nonMember int32 = -1
	for index, id := range stream.IDs {
		if !structural(id) && int32(index) != unmeasuredOwner {
			nonMember = int32(index)
			break
		}
	}
	if nonMember < 0 {
		t.Fatal("no ¬Q identifier to plant")
	}
	// One entry of each owner is made to name a ¬Q candidate.
	for _, owner := range []int32{measuredOwner, unmeasuredOwner} {
		entries := append([]m6StreamEntry(nil), stream.ByOwner[owner]...)
		entries[0].Candidate = nonMember
		stream.ByOwner[owner] = entries
	}
	replay := config
	replay.Stream = stream
	replay.ReplayPhases = recording.PhaseBoundaries()
	network, err := newM6Network(g, replay, structural)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	network.tick = int(stream.ByOwner[measuredOwner][0].Tick)
	if other := int(stream.ByOwner[unmeasuredOwner][0].Tick); other > network.tick {
		network.tick = other
	}

	if got := network.availableFromStream(measuredOwner); containsNode(got, nonMember) {
		t.Fatalf("the exposure of measured owner %d lists ¬Q candidate %d, which the hand-out never "+
			"gives it: %v", measuredOwner, nonMember, got)
	}
	if got := network.availableFromStream(unmeasuredOwner); !containsNode(got, nonMember) {
		t.Fatalf("the exposure of unmeasured owner %d lost ¬Q candidate %d, although the measurement "+
			"places no restriction on it: %v", unmeasuredOwner, nonMember, got)
	}
	// And memory is NOT applied: an entry the owner already holds stays in
	// the exposure, because the exposure is the source before memory.
	held := stream.ByOwner[measuredOwner]
	for _, entry := range held[1:] {
		if !network.mayTake(measuredOwner, entry.Candidate) || int(entry.Tick) > network.tick {
			continue
		}
		state := network.states[measuredOwner]
		level := levelOf(network.ids[measuredOwner], network.ids[entry.Candidate], config.Shape.degree)
		if level < 0 {
			continue
		}
		state.Table.members[level][entry.Candidate] = struct{}{}
		state.Table.Coverage.Held[level]++
		if got := network.availableFromStream(measuredOwner); !containsNode(got, entry.Candidate) {
			t.Fatalf("a held candidate %d dropped out of the exposure — a memory filter was applied", entry.Candidate)
		}
		return
	}
	t.Fatal("no entry of the measured owner could be planted as held")
}

// TestM6ADifferentResponderUnderAnExhaustedQuotaIsLoadNotMemory is the
// owner's P2 (round 32): the owner's memory and contacts [v, w] are the same
// in both halves; in one half an earlier asker of the tick has used up v's
// quota (r_node), so the owner is answered by w; in the other, by v. The
// start-of-tick snapshot cannot see this — the quota counters are reset every
// tick and move during serving — so the classifier blamed LOCAL MEMORY. The
// cause is the load on the responder: network state.
//
// ⚠️ Both orders of the halves, and the verdict is read where the report reads
// it — Divergence and FirstByCause — not only from classifyDivergence.
//
// ⚠️ Mutations that must break it: judging an addressed responder by the
// held edge alone; not recording the responders skipped for quota; recording
// them but ignoring them in the classifier.
func TestM6ADifferentResponderUnderAnExhaustedQuotaIsLoadNotMemory(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchC, false)
	config.RateNode = 1 // one answer per responder per tick: the second asker is refused
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)

	prepare := func() *m6Network {
		t.Helper()
		network, err := newM6Network(g, config, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		return network
	}
	// An owner with two known responders that hold a record on the level asked:
	// v first in the contact order, w after it.
	const owner = int32(0)
	probe := prepare()
	state := probe.states[owner]
	known := probe.knownTo(owner, state)
	if len(known) < 2 {
		t.Fatalf("owner %d knows %d nodes, the fixture needs two responders", owner, len(known))
	}
	v, w := known[0], known[1]
	level := config.NearFrom // a near level: the addressed request is allowed there

	// serve makes the owner's addressed request in `n` at tick 0 and returns
	// the offer entry it produced.
	serve := func(n *m6Network, quotaSpentOn int32) *m6OfferEntry {
		t.Helper()
		n.tick = 0
		if quotaSpentOn >= 0 {
			// An earlier asker of this tick already took v's one answer: the
			// same thing addressedRequest would leave behind, written as state.
			n.answersByNode[quotaSpentOn] = config.RateNode
		}
		before := len(n.trace.Offers)
		n.addressedRequest(owner, n.states[owner], level)
		offers := n.trace.Offers[before:]
		for index := range offers {
			if offers[index].Source == offerAddressed {
				return &offers[index]
			}
		}
		t.Fatalf("no addressed request was made in this half (offers: %v)", offers)
		return nil
	}

	for _, order := range []struct {
		name             string
		mainSpent        int32 // whose quota an earlier asker used up in the main half
		controlSpent     int32
		mainResponder    int32
		controlResponder int32
	}{
		{"v exhausted in the control", -1, v, v, w},
		{"v exhausted in the main run", v, -1, w, v},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			main, control := prepare(), prepare()
			worlds := worldsBeforeServing(main, control) // taken BEFORE serving, as the comparator does
			mainOffer := serve(main, order.mainSpent)
			controlOffer := serve(control, order.controlSpent)
			if mainOffer.Peer != order.mainResponder || controlOffer.Peer != order.controlResponder {
				t.Fatalf("the fixture asked %d / %d, want %d / %d — the quota did not steer the choice",
					mainOffer.Peer, controlOffer.Peer, order.mainResponder, order.controlResponder)
			}

			comparison := newM6Comparison()
			comparison.Pairing = sourcePairingOf(main, control)
			comparison.checkOffers(main, control, worlds, 0, []m6OfferEntry{*mainOffer}, []m6OfferEntry{*controlOffer})
			if comparison.Divergence == nil {
				t.Fatal("no divergence recorded although the responders differ")
			}
			if comparison.Divergence.Cause == causeLocalMemory {
				t.Fatalf("the same memory and the same contacts, a different responder only because an earlier "+
					"asker used up a quota — and the verdict blames LOCAL MEMORY: %s", comparison.Divergence)
			}
			if comparison.Divergence.Cause != causeNetworkState {
				t.Fatalf("the cause is %s, want the load on the responder (network state): %s",
					comparison.Divergence.Cause, comparison.Divergence)
			}
			if !strings.Contains(comparison.Divergence.Detail, "quota") {
				t.Errorf("the detail does not name the quota: %s", comparison.Divergence.Detail)
			}
			if first := comparison.FirstByCause[causeLocalMemory]; first != nil {
				t.Fatalf("FirstByCause attributes it to memory: %s", first)
			}
			if first := comparison.FirstByCause[causeNetworkState]; first == nil {
				t.Fatal("FirstByCause has no entry for the network state")
			}
		})
	}
}
