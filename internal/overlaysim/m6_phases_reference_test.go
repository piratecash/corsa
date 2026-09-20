package overlaysim

// m6_phases_reference_test.go are the references for the phased schedule of
// §5.9.1 (m6_schedule_test.go) and for the candidate-stream comparison of the
// ‘from scratch’ control (m6_trace_test.go).
//
// ⚠️ Every reference here checks BEHAVIOUR — boundaries, stop reasons, event
// sequences — and only then the text of the report. A boundary is checked
// against an INDEPENDENT recomputation of the rule from the same signature the
// schedule reads, so a schedule that stopped on the right tick for the wrong
// reason, or on the wrong tick with the right reason, is caught either way.

import (
	"fmt"
	"strings"
	"testing"
)

// m6PhasedBase is the phased fixture: the contract's shape and the base
// branch, with a SHORT plan so the run stays a fixture. The proportions matter
// more than the numbers: T_idle small enough that a converged F1 stops on it,
// T_cad long enough to hold several cadence periods.
func m6PhasedBase() m6ModelConfig {
	config := m6ModelBase()
	config.Ticks, config.ChurnAt = 0, 0
	config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 40, CadenceTicks: 24}
	return config
}

// phaseNamed finds one phase of a played schedule.
func phaseNamed(t *testing.T, phases []m6PhaseRecord, want m6Phase) m6PhaseRecord {
	t.Helper()
	for _, phase := range phases {
		if phase.Phase == want {
			return phase
		}
	}
	t.Fatalf("the run played no %s: %s", want, phaseLine(phases))
	return m6PhaseRecord{}
}

// requirePhaseOrder asserts the played phases are exactly `want`, contiguous
// from tick 0, each at least one tick long.
func requirePhaseOrder(t *testing.T, phases []m6PhaseRecord, want ...m6Phase) {
	t.Helper()
	if len(phases) != len(want) {
		t.Fatalf("played %d phases, want %d:\n%s", len(phases), len(want), phaseLine(phases))
	}
	next := 0
	for index, phase := range phases {
		if phase.Phase != want[index] {
			t.Fatalf("phase %d is %s, want %s", index, phase.Phase, want[index])
		}
		if phase.From != next {
			t.Fatalf("%s starts at tick %d, the previous phase ended at %d", phase.Phase, phase.From, next)
		}
		if phase.Ticks() < 1 {
			t.Fatalf("%s lasted %d ticks", phase.Phase, phase.Ticks())
		}
		if phase.Stop == stopNotYet {
			t.Fatalf("%s is recorded as still running after the run ended", phase.Phase)
		}
		next = phase.To
	}
}

// TestM6PhasesFollowTheTransitionsOfTheContract is §5.9.1 as a sequence: F1 →
// F2 → F3 → F4, contiguous, with churn landing exactly on F2's tick and nowhere
// before it, and F4 exactly T_cad ticks long.
//
// ⚠️ Mutations that must break it: churn keyed on a configured tick instead of
// the phase boundary (departures appear before F2); F2 longer than one tick; F4
// stopping early on any rule (its length stops equalling T_cad).
func TestM6PhasesFollowTheTransitionsOfTheContract(t *testing.T) {
	t.Parallel()

	config := m6PhasedBase()
	report := runM6Model(t, config)

	requirePhaseOrder(t, report.Phases, phaseFill, phaseChurn, phaseRecovery, phaseCadence)
	fill := phaseNamed(t, report.Phases, phaseFill)
	churn := phaseNamed(t, report.Phases, phaseChurn)
	recovery := phaseNamed(t, report.Phases, phaseRecovery)
	cadence := phaseNamed(t, report.Phases, phaseCadence)

	if churn.Ticks() != 1 || churn.Stop != stopByConstruction {
		t.Errorf("F2 lasted %d ticks and stopped for %q; a shock is one tick by construction",
			churn.Ticks(), churn.Stop)
	}
	if cadence.Ticks() != config.Phases.CadenceTicks || cadence.Stop != stopWindowEnd {
		t.Errorf("F4 lasted %d ticks and stopped for %q, want exactly T_cad=%d and the end of "+
			"the window", cadence.Ticks(), cadence.Stop, config.Phases.CadenceTicks)
	}
	if len(report.OnlineByTick) != cadence.To {
		t.Errorf("the run played %d ticks, the last phase ends at %d",
			len(report.OnlineByTick), cadence.To)
	}

	// Churn lands on F2's tick and on no other: the scenario trace says when
	// anybody left.
	for _, events := range report.Trace.Scenario {
		left := len(events.Departed) > 0
		if left && events.Tick != churn.From {
			t.Errorf("tick %d realised %d departures; the shock belongs to F2 at tick %d",
				events.Tick, len(events.Departed), churn.From)
		}
		if events.Tick == churn.From && !left {
			t.Errorf("nobody left in the F2 tick %d", churn.From)
		}
	}
	if fill.Departed != 0 || churn.Departed != report.Departed {
		t.Errorf("F1 booked %d departures and F2 %d, the run had %d — the per-phase ledgers do "+
			"not agree with the boundaries", fill.Departed, churn.Departed, report.Departed)
	}
	if recovery.From != churn.To || cadence.From != recovery.To {
		t.Errorf("phases are not contiguous:\n%s", report.PhaseLine())
	}
	t.Logf("\n%s", report.PhaseLine())
}

// TestM6PhaseBoundariesMatchAnIndependentRecomputation drives the run one tick
// at a time and keeps its OWN idle counter from the same signatures the
// schedule reads. Every boundary the schedule decided has to be the one the
// rule gives.
//
// ⚠️ Mutation that must break it: an off-by-one in the idle run (comparing
// against the wrong tick, or counting the first tick of a phase as idle),
// which moves a boundary by one tick with the right reason attached.
func TestM6PhaseBoundariesMatchAnIndependentRecomputation(t *testing.T) {
	t.Parallel()

	config := m6PhasedBase()
	network := m6DirectFixture(t, config)
	plan := *config.Phases

	// The recomputation: which phase this tick belongs to, when it started, and
	// how many unchanged ticks it has seen.
	phase := phaseFill
	started := 0
	idle := 0
	watched := make([]int, config.Shape.degree)
	expectBoundary := func(tick int, reason m6StopReason) {
		t.Helper()
		closed := phaseNamed(t, network.schedule.trace(), phase)
		if closed.To != tick+1 || closed.Stop != reason {
			t.Fatalf("tick %d: the rule closes %s at %d for %q, the schedule recorded %s",
				tick, phase, tick+1, reason, closed)
		}
	}
	expectOpen := func(tick int) {
		t.Helper()
		played := network.schedule.trace()
		last := played[len(played)-1]
		if last.Phase != phase || last.Stop != stopNotYet {
			t.Fatalf("tick %d: the rule keeps %s open, the schedule recorded %s", tick, phase, last)
		}
	}

	for tick := 0; ; tick++ {
		network.tick = tick
		done, err := network.step()
		if err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		elapsed := tick + 1 - started

		closes := stopNotYet
		switch phase {
		case phaseFill, phaseRecovery:
			// ⚠️ Recomputed WITHOUT claimedByLevel / deficitByLevel: those are
			// what the schedule itself reads, and a defect in them would be
			// agreed with here instead of found.
			signature := independentClaimed(network)
			budget := plan.FillTicks
			if phase == phaseRecovery {
				signature = independentDeficit(network)
				budget = plan.RecoveryTicks
			}
			if equalCounts(signature, watched) {
				idle++
			} else {
				idle = 0
			}
			watched = signature
			switch {
			case idle >= plan.IdleTicks:
				closes = stopIdle
			case elapsed >= budget:
				closes = stopElapsed
			}
		case phaseChurn:
			closes = stopByConstruction
		case phaseCadence:
			if elapsed >= plan.CadenceTicks {
				closes = stopWindowEnd
			}
		}

		if closes == stopNotYet {
			if done {
				t.Fatalf("tick %d: the run ended while the rule keeps %s open", tick, phase)
			}
			expectOpen(tick)
			continue
		}
		expectBoundary(tick, closes)
		if phase == phaseCadence {
			if !done {
				t.Fatalf("tick %d: F4 closed and the run went on", tick)
			}
			break
		}
		if done {
			t.Fatalf("tick %d: the run ended after %s", tick, phase)
		}
		started, idle = tick+1, 0
		switch phase {
		case phaseFill:
			phase = phaseChurn
		case phaseChurn:
			phase = phaseRecovery
			watched = independentDeficit(network)
		default:
			phase = phaseCadence
		}
	}
	t.Logf("\n%s", phaseLine(network.schedule.trace()))
}

// TestM6EachEarlyStopHasItsOwnReason exercises every exit of §5.9.1
// separately, with the evidence for each: an idle stop shows T_idle unchanged
// ticks, an elapsed stop shows the whole budget used, and F4 shows the whole
// window used even when nothing at all is happening.
func TestM6EachEarlyStopHasItsOwnReason(t *testing.T) {
	t.Parallel()

	t.Run("F1 stops on T_idle unchanged ticks once filling has converged", func(t *testing.T) {
		t.Parallel()
		config := m6PhasedBase()
		fill := phaseNamed(t, runM6Model(t, config).Phases, phaseFill)
		if fill.Stop != stopIdle || fill.IdleRun != config.Phases.IdleTicks {
			t.Fatalf("F1 stopped for %q with an idle run of %d, want the idle rule at T_idle=%d: %s",
				fill.Stop, fill.IdleRun, config.Phases.IdleTicks, fill)
		}
		if fill.Ticks() >= config.Phases.FillTicks {
			t.Fatalf("F1 used its whole budget of %d — then the idle rule did not stop it", fill.Ticks())
		}
	})

	t.Run("F1 stops on T_fill when nothing converges in time", func(t *testing.T) {
		t.Parallel()
		config := m6PhasedBase()
		config.Phases = &m6PhasePlan{FillTicks: 3, IdleTicks: 40, RecoveryTicks: 8, CadenceTicks: 8}
		fill := phaseNamed(t, runM6Model(t, config).Phases, phaseFill)
		if fill.Stop != stopElapsed || fill.Ticks() != 3 {
			t.Fatalf("F1 stopped for %q after %d ticks, want the budget of 3: %s",
				fill.Stop, fill.Ticks(), fill)
		}
		if fill.IdleRun >= config.Phases.IdleTicks {
			t.Fatalf("F1 shows an idle run of %d against T_idle=%d — the wrong rule fired",
				fill.IdleRun, config.Phases.IdleTicks)
		}
	})

	t.Run("F3 watches the deficit and not the coverage", func(t *testing.T) {
		t.Parallel()
		// ⚠️ THE DISCRIMINATING CASE. Compensated load with C = ∞ in branch A:
		// nobody ever detects a table loss, so the per-level deficit is zero on
		// every tick — while newcomers keep joining and filling their tables, so
		// the COVERAGE changes on every tick. §5.9.1 says F3 stops on the
		// deficit, so it must end after exactly T_idle ticks here; a schedule
		// watching coverage would run F3 to T_rec.
		config := m6PhasedBase()
		config.Cadence = 0
		config.Churn = churnCompensated
		config.ChurnShare = 0.05
		config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 40, CadenceTicks: 8}

		network := m6DirectFixture(t, config)
		var coverageChanges int
		var previous []int
		for tick := 0; ; tick++ {
			network.tick = tick
			done, err := network.step()
			if err != nil {
				t.Fatalf("tick %d: %v", tick, err)
			}
			played := network.schedule.trace()
			if current := played[len(played)-1]; current.Phase == phaseRecovery {
				signature := network.claimedByLevel()
				if previous != nil && !equalCounts(signature, previous) {
					coverageChanges++
				}
				previous = signature
			}
			if done {
				break
			}
		}
		recovery := phaseNamed(t, network.schedule.trace(), phaseRecovery)
		if sumOf(network.report.LostByLevel) != 0 {
			t.Fatalf("%d losses were detected with C = ∞ in branch A; the fixture needs a deficit "+
				"that never moves", sumOf(network.report.LostByLevel))
		}
		if coverageChanges == 0 {
			t.Fatal("coverage never changed during F3, so this run cannot tell the two rules apart")
		}
		if recovery.Stop != stopIdle || recovery.Ticks() != config.Phases.IdleTicks {
			t.Fatalf("F3 stopped for %q after %d ticks while coverage changed %d times inside it; "+
				"the deficit rule stops it at T_idle=%d: %s", recovery.Stop, recovery.Ticks(),
				coverageChanges, config.Phases.IdleTicks, recovery)
		}
	})

	t.Run("F3 stops on T_rec while the deficit keeps moving", func(t *testing.T) {
		t.Parallel()
		// Background churn with a finite cadence: losses are detected and refilled
		// throughout, the deficit never holds still for T_idle ticks.
		config := m6PhasedBase()
		config.Churn = churnShrink
		config.ChurnShare = 0.03
		config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 3, RecoveryTicks: 12, CadenceTicks: 8}
		recovery := phaseNamed(t, runM6Model(t, config).Phases, phaseRecovery)
		if recovery.Stop != stopElapsed || recovery.Ticks() != config.Phases.RecoveryTicks {
			t.Fatalf("F3 stopped for %q after %d ticks, want the budget T_rec=%d: %s",
				recovery.Stop, recovery.Ticks(), config.Phases.RecoveryTicks, recovery)
		}
		if recovery.IdleRun >= config.Phases.IdleTicks {
			t.Fatalf("F3 shows an idle run of %d against T_idle=%d — the wrong rule fired",
				recovery.IdleRun, config.Phases.IdleTicks)
		}
	})

	t.Run("F4 has no early stop even when nothing happens", func(t *testing.T) {
		t.Parallel()
		// ⚠️ THE STRONGEST CASE FOR THE WINDOW: C = ∞, a shock, branch A. After
		// the shock nothing changes for the rest of the run — no refresh, no
		// detection, no arrival — and F4 must still play every one of its T_cad
		// ticks, because the window is what makes C = ∞ comparable with C = 64.
		config := m6PhasedBase()
		config.Cadence = 0
		config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 3, RecoveryTicks: 40, CadenceTicks: 30}
		report := runM6Model(t, config)
		cadence := phaseNamed(t, report.Phases, phaseCadence)
		if cadence.Stop != stopWindowEnd || cadence.Ticks() != config.Phases.CadenceTicks {
			t.Fatalf("F4 stopped for %q after %d ticks, want the whole window of %d: %s",
				cadence.Stop, cadence.Ticks(), config.Phases.CadenceTicks, cadence)
		}
		if cadence.Probes.Probes() != 0 || cadence.Refreshes != 0 {
			t.Fatalf("F4 spent %d probes (%d refreshes) in a run with C = ∞ and nothing left to "+
				"fill — the fixture is not the static case it claims to be", cadence.Probes.Probes(),
				cadence.Refreshes)
		}
	})

	t.Run("a run with no churn form plays F1 and F4 only", func(t *testing.T) {
		t.Parallel()
		config := m6PhasedBase()
		config.Churn = churnNone
		report := runM6Model(t, config)
		requirePhaseOrder(t, report.Phases, phaseFill, phaseCadence)
		if report.Departed != 0 {
			t.Fatalf("%d nodes left a run with no churn form", report.Departed)
		}
	})
}

// TestM6TheCommonWindowCountsRefreshesPerCadence is what F4 is for: the same
// window for every C, the refreshes counted INSIDE it, and the count ordered by
// the cadence — ∞ gives none, the shorter cadence gives more than the longer.
//
// ⚠️ The sweep plays its OWN boundaries (decision 3.3(в), review package §4).
// F1 stops on its own data and the refresh competes with filling for the
// ceiling R, so a short cadence can move the end of F1 — and the churn is
// drawn on τ = tick − onset, so a moved onset is the SAME shock on a later
// tick: the scenario traces agree τ for τ without any replay. Replay stays
// the ‘from scratch’ control's instrument (its emptied deficit would end F3
// early), not the sweep's. Before the decision the shock was drawn on the
// absolute tick and this fixture had to replay the base run's boundaries onto
// the sweep to hold the scenario still.
func TestM6TheCommonWindowCountsRefreshesPerCadence(t *testing.T) {
	t.Parallel()

	base := m6PhasedBase()
	base.Cadence = 8
	base.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 20, CadenceTicks: 32}
	reference := runM6Model(t, base)
	referenceEvents := relativeScenario(t, reference)

	refreshes := map[int]int{}
	onsets := map[int]int{}
	for _, cadence := range []int{0, 8, 16} {
		config := base
		config.Cadence = cadence
		report := runM6Model(t, config)

		window := phaseNamed(t, report.Phases, phaseCadence)
		if window.Ticks() != base.Phases.CadenceTicks || window.Stop != stopWindowEnd {
			t.Fatalf("C=%d: F4 lasted %d ticks and stopped for %q, want the common window of %d "+
				"ended by its own rule", cadence, window.Ticks(), window.Stop, base.Phases.CadenceTicks)
		}
		refreshes[cadence] = window.Refreshes
		onsets[cadence] = report.Trace.OnsetTick

		// The same shock at the same τ, whatever tick F1 ended on: the draws
		// agree by the key, and — no arrivals before a shock — so do the
		// realised departures.
		events := relativeScenario(t, report)
		taus := requireSameDraws(t, referenceEvents, events)
		if tau, what, differ := firstRealisedDifference(referenceEvents, events, taus); differ {
			t.Fatalf("C=%d: %s differ from the base run's at τ=%d", cadence, what, tau)
		}
		for _, phase := range report.Phases {
			if phase.Stop == stopReplayed {
				t.Fatalf("C=%d: %s was replayed; the sweep decides its own boundaries", cadence, phase.Phase)
			}
		}
	}

	if refreshes[0] != 0 {
		t.Errorf("C = ∞ refreshed %d times inside the window", refreshes[0])
	}
	if refreshes[8] <= refreshes[16] || refreshes[16] == 0 {
		t.Errorf("refreshes inside the common window: C=8 gave %d, C=16 gave %d — the shorter "+
			"cadence must refresh more, and both must refresh at all", refreshes[8], refreshes[16])
	}
	t.Logf("refreshes inside the %d-tick window: C=∞ %d, C=8 %d, C=16 %d; onsets C=∞ %d, C=8 %d, C=16 %d",
		base.Phases.CadenceTicks, refreshes[0], refreshes[8], refreshes[16],
		onsets[0], onsets[8], onsets[16])
}

// TestM6TheFromScratchControlKeepsTheScenario is decision 3б of the run
// registry made checkable: what "the same scenario, the same candidate stream"
// means, proven where it holds and named where it cannot.
func TestM6TheFromScratchControlKeepsTheScenario(t *testing.T) {
	t.Parallel()

	t.Run("under the phased schedule the control needs the main run's boundaries", func(t *testing.T) {
		t.Parallel()
		config := m6PhasedBase()
		config.StartEmpty = true
		_, err := newM6Network(buildGraph(config.Shape, config.Seed, config.Quota, config.Policy),
			config, everybody)
		if err == nil || !strings.Contains(err.Error(), "boundaries") {
			t.Fatalf("a ‘from scratch’ control without replayed boundaries was accepted (err=%v) — "+
				"its own F3 rule would stop on an emptied deficit and move every tick after it", err)
		}
	})

	t.Run("the clearing leaves the world alone", func(t *testing.T) {
		t.Parallel()
		// ⚠️ Asserted on the state itself, not on the report: the graph, who is
		// online, who has joined, the reserve cursor, the returns scheduled and
		// the entry queue are the WORLD, and the control may touch none of them.
		config := m6ModelBase()
		config.StartEmpty = true
		config.Churn = churnCompensated
		config.ChurnShare = 0.05
		config.ChurnAt = 0
		config.ReturnShare = 0.5
		config.ReturnAfter = 3
		network := m6DirectFixture(t, config)
		for network.tick = 0; network.tick < 3; network.tick++ {
			if _, err := network.step(); err != nil {
				t.Fatalf("tick %d: %v", network.tick, err)
			}
		}
		before := snapshotWorld(network)
		if network.reserveAt == 0 || len(network.returning) == 0 {
			t.Fatalf("after three ticks of compensated load nothing was drawn from the reserve or "+
				"scheduled to return (%s) — the snapshot would not notice a rewind", before)
		}
		// A second clearing on a network that has detected nothing since is the
		// same operation as the first; the point is what it does NOT change.
		for _, node := range network.all {
			if state := network.states[node]; state != nil {
				for level := range state.LostByLevel {
					state.LostByLevel[level], state.RefilledByLevel[level] = 0, 0
				}
			}
		}
		for level := range network.report.LostByLevel {
			network.report.LostByLevel[level], network.report.RefilledByLevel[level] = 0, 0
		}
		if err := network.clearForTheFromScratchControl(); err != nil {
			t.Fatalf("clearing: %v", err)
		}
		if after := snapshotWorld(network); after != before {
			t.Fatalf("the clearing changed the world:\n before %s\n after  %s", before, after)
		}
	})

	t.Run("branch A: same scenario, same exposure, offers differ only by memory", func(t *testing.T) {
		t.Parallel()
		main, control := runMainAndFromScratch(t, m6PhasedBase())
		requireSameScenario(t, main, control)

		// The exposure at the onset — the pool the source draws from BEFORE
		// memory filters it — is identical for every owner: in branch A it is
		// the held edges, which the clearing does not touch.
		_, world, tables := exposureDivergence(main.Trace.ExposureAtOnset, control.Trace.ExposureAtOnset)
		if len(world) > 0 || len(tables) > 0 {
			t.Fatalf("the exposure at the onset differs for %d owners in the world half and %d in "+
				"the tables half — branch A has no tables half, so the source itself moved",
				len(world), len(tables))
		}

		// The offers are identical before the onset and MUST differ from it:
		// that difference is the memory being measured, and a control whose
		// offers never diverged did not clear anything.
		divergence, differs := firstOfferDivergence(main.Trace.Offers, control.Trace.Offers)
		if !differs {
			t.Fatal("the offer traces never diverged — the control learned nothing new, so the " +
				"clearing did not happen")
		}
		if divergence.Tick < main.Trace.OnsetTick {
			t.Fatalf("the offers diverged BEFORE the onset tick %d, where the two runs are supposed "+
				"to be the same run: %s", main.Trace.OnsetTick, divergence)
		}
		// And the first thing the control is offered that the main run was not
		// comes from the SHARED exposure — memory let it through, no new source
		// supplied it.
		if divergence.Control == nil || divergence.Control.Source != offerAcquaintance {
			t.Fatalf("the first divergence is not the control taking an acquaintance: %s", divergence)
		}
		exposed := main.Trace.ExposureAtOnset[divergence.Control.Owner].Acquaintances
		if !containsNode(exposed, divergence.Control.Peer) {
			t.Fatalf("the control was offered %d, which is not in the shared exposure %v of owner %d",
				divergence.Control.Peer, exposed, divergence.Control.Owner)
		}
		// The report claims only what is held still by construction — boundaries
		// and decisions — and sends the reader to the trace comparison for the
		// rest; the identity of the whole scenario above is a RESULT of this
		// shock run, not a promise of the control.
		if !strings.Contains(control.StreamLine(), "the same at the onset by construction") ||
			!strings.Contains(control.StreamLine(), "recomputed here") {
			t.Errorf("the control's report does not say what the trace proves and no more:\n%s",
				control.StreamLine())
		}
		t.Logf("onset tick %d; first divergence: %s", main.Trace.OnsetTick, divergence)
	})

	t.Run("branch A′: the source itself differs and the report says so", func(t *testing.T) {
		t.Parallel()
		// ⚠️ THE NAMED DISCREPANCY. In A′ the exposure includes what each
		// neighbour would hand over FROM ITS TABLE, and the clearing emptied
		// every measured table — so at the onset the control's neighbours have
		// nothing to hand over where the main run's had m records. The scenario
		// is the same, the held edges are the same, and the candidate stream is
		// NOT: a difference between the two runs cannot be attributed to the
		// owner's memory alone, and the report must not claim it can.
		config := m6PhasedBase()
		config.Branch = branchAPrime
		// A′ keeps discovering for the whole of T_fill, so the plan is kept short:
		// the exposure is the point here, not the length of the run.
		config.Phases = &m6PhasePlan{FillTicks: 12, IdleTicks: 4, RecoveryTicks: 12, CadenceTicks: 8}
		main, control := runMainAndFromScratch(t, config)
		requireSameScenario(t, main, control)

		_, world, tables := exposureDivergence(main.Trace.ExposureAtOnset, control.Trace.ExposureAtOnset)
		if len(world) > 0 {
			t.Fatalf("the world half of the exposure differs for %d owners — the held edges moved, "+
				"which the clearing must not do", len(world))
		}
		if len(tables) == 0 {
			t.Fatal("the tables half of the exposure is identical — then the main run's neighbours " +
				"had nothing in their tables at the onset, and the fixture shows nothing")
		}
		owner := tables[0]
		mainHanded, controlHanded := 0, 0
		for _, handed := range main.Trace.ExposureAtOnset[owner].ByNeighbour {
			mainHanded += len(handed)
		}
		for _, handed := range control.Trace.ExposureAtOnset[owner].ByNeighbour {
			controlHanded += len(handed)
		}
		if controlHanded >= mainHanded {
			t.Fatalf("owner %d: the control's neighbours would hand %d records against the main "+
				"run's %d — the cleared tables should hand fewer", owner, controlHanded, mainHanded)
		}
		if !strings.Contains(control.StreamLine(), "NOT THE SAME") ||
			!strings.Contains(control.String(), "cannot be attributed to memory alone") {
			t.Errorf("the A′ control's report claims or implies an equivalent stream:\n%s",
				control.StreamLine())
		}
		t.Logf("%d of %d owners see a different exchange exposure at the onset; owner %d: %d "+
			"records from the main run's neighbours, %d from the control's", len(tables),
			len(main.Trace.ExposureAtOnset), owner, mainHanded, controlHanded)
	})

	t.Run("background churn: decisions and boundaries agree, realised departures are a result", func(t *testing.T) {
		t.Parallel()
		config := m6PhasedBase()
		config.Churn = churnCompensated
		config.ChurnShare = 0.03
		config.ReturnShare = 0.5
		config.ReturnAfter = 3
		main, control := runMainAndFromScratch(t, config)

		if len(main.OnlineByTick) != len(control.OnlineByTick) {
			t.Fatalf("the main run played %d ticks and the control %d",
				len(main.OnlineByTick), len(control.OnlineByTick))
		}
		for index := range main.Trace.Scenario {
			left, right := main.Trace.Scenario[index], control.Trace.Scenario[index]
			if !equalNodes(left.Decided, right.Decided) {
				t.Fatalf("tick %d: the departure DECISIONS differ — %v against %v", left.Tick,
					left.Decided, right.Decided)
			}
		}
		// The realised half may differ: a decision lands on a node that one run
		// admitted and the other did not, because budget was freed at different
		// moments. That is a consequence of behaviour and is logged, not hidden.
		if tick, why, differs := firstScenarioDivergence(main.Trace.Scenario, control.Trace.Scenario); differs {
			t.Logf("realised scenario first differs at tick %d: %s", tick, why)
		} else {
			t.Log("the realised scenario is identical in both runs")
		}
	})
}

// runMainAndFromScratch runs the main mode and its ‘from scratch’ control on
// one graph, with traces on and the control replaying the main run's
// boundaries — the pairing every comparison of П-6 has to be made in.
func runMainAndFromScratch(t *testing.T, config m6ModelConfig) (main, control *m6ModelReport) {
	t.Helper()
	config.TraceOffers = true
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	main = runM6ModelOn(t, g, config, everybody)

	scratch := config
	scratch.StartEmpty = true
	scratch.ReplayPhases = main.PhaseBoundaries()
	control = runM6ModelOn(t, g, scratch, everybody)
	return main, control
}

// requireSameScenario asserts the exogenous script and the phase boundaries
// are identical, tick for tick.
func requireSameScenario(t *testing.T, main, control *m6ModelReport) {
	t.Helper()
	if tick, why, differs := firstScenarioDivergence(main.Trace.Scenario, control.Trace.Scenario); differs {
		t.Fatalf("the scenario differs at tick %d: %s", tick, why)
	}
	if len(main.Phases) != len(control.Phases) {
		t.Fatalf("the main run played %d phases and the control %d", len(main.Phases), len(control.Phases))
	}
	for index := range main.Phases {
		if main.Phases[index].boundary() != control.Phases[index].boundary() {
			t.Fatalf("%s: main %d–%d, control %d–%d", main.Phases[index].Phase,
				main.Phases[index].From, main.Phases[index].To,
				control.Phases[index].From, control.Phases[index].To)
		}
	}
	if main.Trace.OnsetTick != control.Trace.OnsetTick || main.Trace.OnsetTick < 0 {
		t.Fatalf("onset ticks: main %d, control %d", main.Trace.OnsetTick, control.Trace.OnsetTick)
	}
	if sumOf(main.LostByLevel) == 0 {
		t.Fatal("the main run lost nothing, so there is nothing to compare memory against")
	}
}

// snapshotWorld digests the state the ‘from scratch’ control must not touch.
func snapshotWorld(n *m6Network) string {
	held, online, joined := 0, 0, 0
	for node := range n.all {
		held += len(n.held[node])
		if n.online[node] {
			online++
		}
		if n.joined[node] {
			joined++
		}
	}
	returning := 0
	for _, due := range n.returning {
		returning += len(due)
	}
	return fmt.Sprintf("held edges %d, online %d, joined %d (order %d), reserve cursor %d, "+
		"returns scheduled %d, queue %d, tick %d",
		held, online, joined, len(n.joinedOrder), n.reserveAt, returning, len(n.queue), n.tick)
}

func containsNode(nodes []int32, wanted int32) bool {
	for _, node := range nodes {
		if node == wanted {
			return true
		}
	}
	return false
}

// TestM6TheReportShowsPhaseBoundariesAndReasons is the report half: the
// boundaries and reasons are printed, a flat run names itself as not the grid
// scenario, and a replayed run says its boundaries were not its own.
func TestM6TheReportShowsPhaseBoundariesAndReasons(t *testing.T) {
	t.Parallel()

	flat := runM6Model(t, m6ModelBase())
	if !strings.Contains(flat.String(), "not §5.9.1") || !strings.Contains(flat.String(), "FLAT run") {
		t.Errorf("a flat run does not name itself as the fixture schedule:\n%s", flat.PhaseLine())
	}

	config := m6PhasedBase()
	main, control := runMainAndFromScratch(t, config)
	for _, phase := range main.Phases {
		want := fmt.Sprintf("%s: ", phase.Phase)
		if !strings.Contains(main.String(), want) || !strings.Contains(main.String(), phase.Stop.String()) {
			t.Errorf("the report does not print %s with its reason %q:\n%s", phase.Phase, phase.Stop,
				main.PhaseLine())
		}
	}
	if !strings.Contains(main.String(), "T_fill=40, T_idle=4, T_rec=40, T_cad=24") {
		t.Errorf("the report does not print the plan:\n%s", main.Config)
	}
	if !strings.Contains(control.String(), "REPLAYED") {
		t.Errorf("the control's report does not say its boundaries were replayed:\n%s",
			control.PhaseLine())
	}
}

// TestM6RefusesASecondScheduleAndABrokenPlan is the door: one schedule per
// run, every phase parameter at least one tick, a replay that matches the
// churn form.
func TestM6RefusesASecondScheduleAndABrokenPlan(t *testing.T) {
	t.Parallel()

	g := buildGraph(m6ModelShape(), 1, 1, policyInitiatedLimit)
	refuse := func(name string, config m6ModelConfig, want string) {
		t.Helper()
		_, err := newM6Network(g, config, everybody)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: accepted (err=%v), want a refusal mentioning %q", name, err, want)
		}
	}

	both := m6ModelBase()
	both.Phases = &m6PhasePlan{FillTicks: 4, IdleTicks: 2, RecoveryTicks: 4, CadenceTicks: 4}
	refuse("a plan beside Ticks/ChurnAt", both, "one schedule")

	orphan := m6ModelBase()
	orphan.ReplayPhases = []m6PhaseBoundary{{Phase: phaseFill, From: 0, To: 4}}
	refuse("replay without a plan", orphan, "without a phase plan")

	zero := m6PhasedBase()
	zero.Phases = &m6PhasePlan{FillTicks: 4, IdleTicks: 0, RecoveryTicks: 4, CadenceTicks: 4}
	refuse("T_idle = 0", zero, "T_idle = 0")

	mismatched := m6PhasedBase()
	mismatched.ReplayPhases = []m6PhaseBoundary{
		{Phase: phaseFill, From: 0, To: 4}, {Phase: phaseCadence, From: 4, To: 8},
	}
	refuse("a churn-free trace on a shock run", mismatched, "plays 4 phases")

	gap := m6PhasedBase()
	gap.ReplayPhases = []m6PhaseBoundary{
		{Phase: phaseFill, From: 0, To: 4}, {Phase: phaseChurn, From: 5, To: 6},
		{Phase: phaseRecovery, From: 6, To: 8}, {Phase: phaseCadence, From: 8, To: 12},
	}
	refuse("a gap between replayed phases", gap, "expected it to start at tick 4")

	// ⚠️ Replay suppresses the run's own stop rules, so the plan has to be
	// enforced on the pinned spans themselves: otherwise a trace could stretch
	// F2, cut F4 short or overrun a budget, and the report would print the
	// plan beside a schedule that was never played.
	plan := *m6PhasedBase().Phases // T_fill=40, T_idle=4, T_rec=40, T_cad=24
	spans := func(fill, churn, recovery, cadence int) []m6PhaseBoundary {
		return []m6PhaseBoundary{
			{Phase: phaseFill, From: 0, To: fill},
			{Phase: phaseChurn, From: fill, To: fill + churn},
			{Phase: phaseRecovery, From: fill + churn, To: fill + churn + recovery},
			{Phase: phaseCadence, From: fill + churn + recovery, To: fill + churn + recovery + cadence},
		}
	}
	for _, broken := range []struct {
		name  string
		spans []m6PhaseBoundary
		want  string
	}{
		{"F1 longer than T_fill", spans(plan.FillTicks+1, 1, 8, plan.CadenceTicks), "F1 filling lasts 41"},
		{"F2 longer than one tick", spans(8, 2, 8, plan.CadenceTicks), "F2 churn onset lasts 2"},
		{"F3 longer than T_rec", spans(8, 1, plan.RecoveryTicks+1, plan.CadenceTicks), "F3 recovery lasts 41"},
		{"F4 shorter than T_cad", spans(8, 1, 8, plan.CadenceTicks-1), "F4 cadence lasts 23"},
		{"F4 longer than T_cad", spans(8, 1, 8, plan.CadenceTicks+1), "F4 cadence lasts 25"},
	} {
		config := m6PhasedBase()
		config.ReplayPhases = broken.spans
		refuse(broken.name, config, broken.want)
	}
	// And the spans a run of this plan can actually produce pass.
	legal := m6PhasedBase()
	legal.ReplayPhases = spans(plan.FillTicks, 1, plan.RecoveryTicks, plan.CadenceTicks)
	if _, err := newM6Network(g, legal, everybody); err != nil {
		t.Errorf("spans at the plan's own limits are refused: %v", err)
	}

	// And the contract's own numbers pass the door.
	contract := m6PhasedBase()
	agreed := m6ContractPhases()
	contract.Phases = &agreed
	if _, err := newM6Network(g, contract, everybody); err != nil {
		t.Errorf("the contract's plan is refused: %v", err)
	}
}

// TestM6ARefusedRefreshIsStillCountedAsScheduled is the F4 counter on the path
// the outcome ledger alone would hide: a refresh of a record the owner holds
// WITHOUT an edge, while the owner sits at its ceiling B. The probe is refused
// at the owner (m6OwnerAtBudget), its cost is on the ledger — and the refresh
// was scheduled all the same, so F4's "how many refreshes fell inside the
// window" must count it, with the refusal shown beside it.
//
// ⚠️ Mutation that must break it: counting refreshes after the probe returns
// (the refused one then never reaches the counter).
func TestM6ARefusedRefreshIsStillCountedAsScheduled(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	network := m6DirectFixture(t, config)

	const owner = int32(0)
	state := network.states[owner]

	// A record without an edge: a node the owner is NOT connected to, stored
	// while the owner still has room.
	stranger := int32(-1)
	level := -1
	for _, candidate := range network.joinedOrder {
		if candidate == owner || network.holdsEdge(owner, candidate) {
			continue
		}
		if level = levelOf(network.ids[owner], network.ids[candidate], config.Shape.degree); level >= 0 {
			stranger = candidate
			break
		}
	}
	if stranger < 0 {
		t.Fatal("the fixture found no stranger with a level in the owner's table")
	}
	// Storing a record costs the stranger no slot (П-2), but classifyProbe
	// still asks for room at its end; on the built graph everybody starts at
	// B, so one of its edges is dropped to make the store possible.
	for peer := range network.held[stranger] {
		delete(network.held[stranger], peer)
		break
	}
	network.probe(owner, state, stranger, level, false)
	if !state.Table.holds(stranger) || network.holdsEdge(owner, stranger) {
		t.Fatalf("the fixture could not store a record without an edge: held=%v, %s",
			network.holdsEdge(owner, stranger), network.report.Probes)
	}

	// Now the owner sits at its ceiling, and the level falls due.
	for _, candidate := range network.joinedOrder {
		if network.heldEdges(owner) >= config.Shape.budget {
			break
		}
		if candidate != owner && candidate != stranger {
			network.held[owner][candidate] = struct{}{}
		}
	}
	for other := range state.Table.members[level] {
		if other != stranger {
			state.Table.drop(other)
		}
	}
	network.tick = config.Cadence
	state.LastRefreshed[level] = 0
	clear(state.TriedThisTick)
	before := network.report.Probes.Outcomes[m6OwnerAtBudget]
	if !network.probeOnceInTheNetwork(owner, state) {
		t.Fatal("the node did nothing although a level was due for a refresh")
	}

	phase := network.schedule.current()
	if got := network.report.Probes.Outcomes[m6OwnerAtBudget]; got != before+1 {
		t.Fatalf("the refresh was not refused at the owner's ceiling (owner-at-B went %d → %d), so "+
			"this fixture is not on the path it claims", before, got)
	}
	if phase.Refreshes != 1 || phase.RefreshesRefused != 1 {
		t.Fatalf("the phase counts %d refreshes scheduled and %d refused; the refused refresh must "+
			"appear in both", phase.Refreshes, phase.RefreshesRefused)
	}
	if !strings.Contains(phase.String(), "1 refreshes scheduled (1 of them refused") {
		t.Errorf("the phase line does not show the refusal beside the count: %s", phase)
	}
}

// TestM6TheOmniscientExposureIsTheJoinedPopulation pins what the exposure of
// the omniscient CONTROL is: the source of that control is the whole joined
// population under mayTake (fromOmniscience), not the owner's held edges, so
// its exposure at the onset has to be recorded from that source — in the order
// the source walks it, before the owner's memory filters it. Reading the
// neighbours instead would let the ‘same pool at the onset’ claim miss a
// difference in the joined population and blame a difference in edges that the
// source never depends on.
//
// ⚠️ Mutations that must break it: recording the held edges as the control's
// exposure (a joined non-neighbour is missing; an edge change moves the pool;
// an admitted node goes unnoticed); walking the index space padded with the
// unjoined reserve (a reserve identifier shows up); skipping mayTake (a ¬Q
// candidate is exposed to a Q owner).
func TestM6TheOmniscientExposureIsTheJoinedPopulation(t *testing.T) {
	t.Parallel()

	config := m6PairBase(branchA, true)
	config.Churn = churnCompensated
	config.ChurnShare = 0.05
	g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }

	prepare := func(t *testing.T, member func(nodeID) bool) *m6Network {
		t.Helper()
		network, err := newM6Network(g, config, member)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		if len(network.reserve) == 0 {
			t.Fatal("the fixture needs an unjoined reserve to prove it is not exposed")
		}
		return network
	}
	notANeighbour := func(network *m6Network, owner int32) int32 {
		for _, candidate := range network.joinedOrder {
			if candidate != owner && !network.holdsEdge(owner, candidate) {
				return candidate
			}
		}
		t.Fatal("every joined node is a neighbour of the owner — the fixture is too small")
		return -1
	}

	t.Run("a joined non-neighbour the measurement allows is in the pool, in the walk order", func(t *testing.T) {
		network := prepare(t, everybody)
		const owner = int32(0)
		stranger := notANeighbour(network, owner)
		exposure := network.exposureOf(owner)
		if !containsNode(exposure.Omniscient, stranger) {
			t.Fatalf("joined node %d, not a neighbour of %d, is missing from the omniscient exposure %v",
				stranger, owner, exposure.Omniscient)
		}
		if containsNode(exposure.Omniscient, owner) {
			t.Fatalf("the owner %d is exposed to itself", owner)
		}
		if len(exposure.Acquaintances) != 0 || len(exposure.ByNeighbour) != 0 {
			t.Fatalf("the control's exposure carries the branch halves (%d acquaintances, %d tables) "+
				"although the control does not draw from them", len(exposure.Acquaintances),
				len(exposure.ByNeighbour))
		}
		// The order is the source's own: the walk from the owner-dependent
		// offset, so the first exposed node is the first the control would offer.
		start := int(m6Random(config.Seed, "omniscient", int(owner)) % uint64(len(network.joinedOrder)))
		first := network.joinedOrder[start]
		if first == owner {
			first = network.joinedOrder[(start+1)%len(network.joinedOrder)]
		}
		if len(exposure.Omniscient) == 0 || exposure.Omniscient[0] != first {
			t.Fatalf("the exposure does not start where the walk starts: got %v, the walk begins at %d",
				exposure.Omniscient, first)
		}
	})

	t.Run("the unjoined reserve is not exposed", func(t *testing.T) {
		network := prepare(t, everybody)
		for _, owner := range network.owners {
			for _, candidate := range network.exposureOf(owner).Omniscient {
				if !network.joined[candidate] {
					t.Fatalf("owner %d is exposed to %d, which has not joined", owner, candidate)
				}
			}
		}
	})

	t.Run("a ¬Q candidate is not exposed to a measured Q owner", func(t *testing.T) {
		network := prepare(t, structural)
		for _, owner := range network.owners {
			if !network.measured(owner) {
				continue
			}
			for _, candidate := range network.exposureOf(owner).Omniscient {
				if !network.mayTake(owner, candidate) {
					t.Fatalf("measured owner %d is exposed to %d, which the measurement does not let it take",
						owner, candidate)
				}
			}
		}
	})

	t.Run("changing the edges alone does not change the pool", func(t *testing.T) {
		network := prepare(t, everybody)
		const owner = int32(0)
		before := network.exposureOf(owner)
		for peer := range network.held[owner] {
			delete(network.held[peer], owner)
		}
		network.held[owner] = map[int32]struct{}{}
		after := network.exposureOf(owner)
		if !before.equal(after) {
			t.Fatalf("removing every edge of %d changed its omniscient exposure:\n before %v\n after  %v",
				owner, before.Omniscient, after.Omniscient)
		}
	})

	t.Run("a change of the joined population is found by the comparison", func(t *testing.T) {
		base := runM6ModelOn(t, g, config, everybody)
		replay := config
		replay.ReplayPhases = base.PhaseBoundaries()
		main, err := newM6Network(g, replay, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		control, err := newM6Network(g, replay, everybody)
		if err != nil {
			t.Fatalf("preparing: %v", err)
		}
		// The one difference: a reserve identifier has joined the control's
		// world — without an edge, so the held-edge claim stays untouched and
		// only the population differs.
		extra := control.reserve[0]
		control.joinedOrder = append(control.joinedOrder, extra)
		control.joined[extra] = true
		control.online[extra] = true

		comparison, err := compareM6Runs(main, control)
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		if comparison.Holds(claimSamePoolAtOnset) {
			t.Fatalf("the pool claim held although the control's joined population differs:\n%s",
				comparison)
		}
		// The population difference is found where it is — in the pool at the
		// onset — and not read off the edges: up to the onset no edge differs,
		// so the held-edge claim cannot fail EARLIER than the pool claim. (From
		// the onset on the edges do drift, because the control offers from a
		// different walk: a consequence of behaviour, as in every adaptive pair.)
		pool, edges := comparison.claim(claimSamePoolAtOnset), comparison.claim(claimSameHeldEdges)
		if pool.Status != claimFails || pool.Tick != main.trace.OnsetTick {
			t.Fatalf("the pool claim did not fail at the onset tick %d: %s", main.trace.OnsetTick, pool)
		}
		if edges.Status == claimFails && edges.Tick < pool.Tick {
			t.Fatalf("the held-edge claim failed at tick %d, before the pool claim at %d, although "+
				"no edge differed before the onset:\n%s", edges.Tick, pool.Tick, comparison)
		}
	})
}

// TestM6TheOnsetExposureIncludesTheNewcomersAdmittedThatTick pins the MOMENT
// of the onset snapshot: after the tick's churn AND its admissions, before
// anybody serves — the state every offer of the onset tick is made against
// (§6.16, §6.17.4). A newcomer admitted in the onset tick is part of the
// omniscient control's walk and of its host's acquaintances when the tick is
// served, so it has to be in the recorded exposure; a snapshot taken between
// the departures and the admissions lacks it, and the ‘same pool at the onset’
// claim is then judged on a pool the source never drew from.
//
// ⚠️ Mutation that must break it: snapshotting inside applyChurn, before
// admitFromQueue.
func TestM6TheOnsetExposureIncludesTheNewcomersAdmittedThatTick(t *testing.T) {
	t.Parallel()

	for _, source := range []struct {
		name       string
		omniscient bool
	}{{"omniscient control", true}, {"branch A", false}} {
		t.Run(source.name, func(t *testing.T) {
			t.Parallel()
			config := m6PairBase(branchA, source.omniscient)
			config.Churn, config.ChurnShare, config.ReturnShare = churnCompensated, 0.05, 0
			g := buildGraph(config.Shape, config.Seed, config.Quota, config.Policy)
			base := runM6ModelOn(t, g, config, everybody)
			config.ReplayPhases = base.PhaseBoundaries()
			network, err := newM6Network(g, config, everybody)
			if err != nil {
				t.Fatalf("preparing: %v", err)
			}
			onset := base.Trace.OnsetTick
			if onset < 0 {
				t.Fatal("the base run had no onset")
			}
			for network.tick = 0; network.tick < onset; network.tick++ {
				if err := network.prepareTick(); err != nil {
					t.Fatalf("tick %d: %v", network.tick, err)
				}
				network.serveTick()
			}
			// The onset tick, PREPARED but not served: this is the state the
			// snapshot describes.
			if err := network.prepareTick(); err != nil {
				t.Fatalf("onset tick: %v", err)
			}
			if network.trace.OnsetTick != onset {
				t.Fatalf("the snapshot was taken in tick %d, the onset is %d", network.trace.OnsetTick, onset)
			}
			if network.schedule.current().Admitted == 0 {
				t.Fatal("nobody was admitted in the onset tick — the fixture cannot tell the two moments apart")
			}

			// Every newcomer admitted THIS tick: joined, online, exactly one edge.
			admitted := 0
			for _, node := range network.joinedOrder {
				if int(node) < len(g.ids) || !network.online[node] || len(network.held[node]) != 1 {
					continue
				}
				var host int32
				for peer := range network.held[node] {
					host = peer
				}
				admitted++
				if source.omniscient {
					for _, owner := range []int32{host, 0} {
						if owner == node {
							continue
						}
						if !containsNode(network.trace.ExposureAtOnset[owner].Omniscient, node) {
							t.Fatalf("newcomer %d, admitted in the onset tick %d, is missing from owner %d's "+
								"omniscient exposure — the snapshot was taken before the admissions",
								node, onset, owner)
						}
					}
					continue
				}
				if !containsNode(network.trace.ExposureAtOnset[host].Acquaintances, node) {
					t.Fatalf("newcomer %d, admitted in the onset tick %d with host %d, is missing from "+
						"the host's acquaintances in the snapshot — the snapshot was taken before the "+
						"admissions", node, onset, host)
				}
			}
			if admitted == 0 {
				t.Fatal("no admitted newcomer with a single edge was found; the fixture is not what it claims")
			}
			// And the snapshot still describes the world BEFORE serving: nothing
			// has been offered in the onset tick yet.
			for _, offer := range network.trace.Offers {
				if offer.Tick == onset {
					t.Fatalf("an offer was recorded in the onset tick before serving: %s", offer)
				}
			}
		})
	}
}

// independentClaimed and independentDeficit recompute the two stop-rule
// signatures WITHOUT the functions the schedule reads (claimedByLevel,
// deficitByLevel): straight from each online node's table members and its
// loss/refill counters. A defect in the schedule's own signature — counting
// an offline node, reading the coverage counter instead of the members —
// then disagrees with these instead of being agreed with.
func independentClaimed(network *m6Network) []int {
	claimed := make([]int, network.config.Shape.degree)
	for node, state := range network.states {
		if state == nil || !network.online[node] || !network.joined[node] {
			continue
		}
		for level, members := range state.Table.members {
			claimed[level] += len(members)
		}
	}
	return claimed
}

func independentDeficit(network *m6Network) []int {
	deficit := make([]int, network.config.Shape.degree)
	for node, state := range network.states {
		if state == nil || !network.online[node] || !network.joined[node] {
			continue
		}
		for level := range deficit {
			if left := state.LostByLevel[level] - state.RefilledByLevel[level]; left > 0 {
				deficit[level] += left
			}
		}
	}
	return deficit
}

// TestM6AnOfflineNodesDeficitIsNotInTheStopSignature is the discriminating
// case for the independent recomputation: a node that detected losses and
// then LEFT carries a deficit its table still remembers, and the F3 rule
// must not read it — an offline owner is memory, not coverage (§6.2). The
// schedule's signature and the independent one have to agree on that, and
// the planted deficit must be visible in neither.
//
// ⚠️ Mutation that must break it: deficitByLevel summing over every joined
// node whether or not it is online.
func TestM6AnOfflineNodesDeficitIsNotInTheStopSignature(t *testing.T) {
	t.Parallel()

	config := m6PhasedBase()
	network := m6DirectFixture(t, config)
	// Step until F3 is open: the shock has landed and there are offline nodes.
	for tick := 0; ; tick++ {
		network.tick = tick
		done, err := network.step()
		if err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		if done {
			t.Fatal("the run ended before F3 opened")
		}
		if network.schedule.current().Phase == phaseRecovery {
			break
		}
	}
	var offline int32 = -1
	for _, node := range network.joinedOrder {
		if !network.online[node] && network.states[node] != nil {
			offline = node
			break
		}
	}
	if offline < 0 {
		t.Fatal("no offline node after the shock")
	}
	before := network.deficitByLevel()
	if !equalCounts(before, independentDeficit(network)) {
		t.Fatalf("the schedule's deficit signature %v disagrees with the independent one %v before "+
			"anything was planted", before, independentDeficit(network))
	}
	// The offline node remembers a loss it never refilled.
	network.states[offline].LostByLevel[0] += 3
	after := network.deficitByLevel()
	if !equalCounts(after, before) {
		t.Fatalf("an offline node's deficit moved the schedule's signature from %v to %v", before, after)
	}
	if !equalCounts(after, independentDeficit(network)) {
		t.Fatalf("the schedule's deficit signature %v disagrees with the independent one %v", after,
			independentDeficit(network))
	}
	// The same for coverage: the offline node's table is memory, not coverage.
	claimedBefore := network.claimedByLevel()
	if !equalCounts(claimedBefore, independentClaimed(network)) {
		t.Fatalf("claimed %v disagrees with the independent recomputation %v", claimedBefore,
			independentClaimed(network))
	}
}

// TestM6ThePhaseLedgersAddUpToTheRunTotals pins the two sets of counters
// against each other (П-8): what the phases booked, summed, is what the run
// reports — probes, refreshes, detections, refills, departures, admissions —
// with the per-phase facts the transitions imply (F1 books no departure and
// no detection; refused refreshes are a subset of scheduled ones; the last
// phase ends where the run ends). ProbesAfterChurn is judged against the
// FIRST REALISED DEPARTURE, read off the network tick by tick — not against
// F2: under background churn the onset tick can take nobody, so the after-
// churn ledger starts later than the phase does, and with no departure at all
// it stays empty while the run still probes.
//
// The refreshes a phase books are checked EXACTLY against an independent
// count — the refresh offers of measured owners in the offer trace, by tick
// — and the refused ones against the owner-at-ceiling outcomes of the same
// phase; the Q-half case has UNMEASURED participants that refill, detect and
// refresh, and none of that work may reach the measured phase sums.
//
// ⚠️ Mutations that must break it: booking a phase counter for unmeasured
// owners (the Q-half case: a sum exceeds the measured total); starting
// ProbesAfterChurn at F2 rather than at the first departure (the late-onset
// and share-0 cases); counting a scheduled refresh twice (the exact count
// against the trace).
func TestM6ThePhaseLedgersAddUpToTheRunTotals(t *testing.T) {
	t.Parallel()

	structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
	type expectation struct {
		name        string
		config      m6ModelConfig
		member      func(nodeID) bool
		wantOnset   bool // a realised departure happens at all
		wantLate    bool // …and later than the tick F2 opened on
		wantRefused bool // some refresh is refused at the owner's ceiling
	}
	base := func(branch m6Branch, form m6ChurnForm) m6ModelConfig {
		config := m6PhasedBase()
		config.Branch, config.Churn = branch, form
		config.ReturnShare = 0.5
		config.TraceOffers = true
		return config
	}
	cases := []expectation{
		{"A′ shock", base(branchAPrime, churnShock), everybody, true, false, true},
		{"A′ shock, Q-half — unmeasured participants work too", base(branchAPrime, churnShock), structural, true, false, true},
		{"C compensated", base(branchC, churnCompensated), everybody, true, false, false},
		{"A shrinking", base(branchA, churnShrink), everybody, true, false, false},
		{"A no churn form — no onset", base(branchA, churnNone), everybody, false, false, false},
	}
	noOne := base(branchAPrime, churnShock)
	noOne.ChurnShare = 0
	cases = append(cases, expectation{"A′ shock of share 0 — F2 opens, nobody leaves", noOne, everybody, false, false, false})
	late := base(branchAPrime, churnCompensated)
	late.ChurnShare = 0.0004 // ≈0.4 decisions a tick on 1k: the onset tick usually takes nobody
	cases = append(cases, expectation{"A′ background churn that takes nobody on the onset tick", late, everybody, true, true, false})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := buildGraph(tc.config.Shape, tc.config.Seed, tc.config.Quota, tc.config.Policy)
			network, err := newM6Network(g, tc.config, tc.member)
			if err != nil {
				t.Fatalf("preparing: %v", err)
			}
			// Step the run by hand, booking the measured probes of every tick
			// and the tick the first departure landed on.
			probesPerTick := make([]int, 0, 256)
			firstDeparture := -1
			for tick := 0; ; tick++ {
				network.tick = tick
				before := network.report.Probes.Probes()
				done, err := network.step()
				if err != nil {
					t.Fatalf("tick %d: %v", tick, err)
				}
				probesPerTick = append(probesPerTick, network.report.Probes.Probes()-before)
				if firstDeparture < 0 && network.churnSeen {
					firstDeparture = tick
				}
				if done {
					break
				}
			}
			network.report.Phases = network.schedule.trace()
			network.collect()
			report := network.report
			phases := report.Phases

			// The scenario trace agrees on the first departure.
			traced := -1
			for _, events := range report.Trace.Scenario {
				if len(events.Departed) > 0 {
					traced = events.Tick
					break
				}
			}
			if traced != firstDeparture {
				t.Fatalf("the first departure is tick %d by churnSeen and tick %d by the scenario trace",
					firstDeparture, traced)
			}
			if (firstDeparture >= 0) != tc.wantOnset {
				t.Fatalf("a realised departure at tick %d, the case wants onset=%v", firstDeparture, tc.wantOnset)
			}
			churn := -1
			for _, phase := range phases {
				if phase.Phase == phaseChurn {
					churn = phase.From
				}
			}
			if tc.wantLate && (churn < 0 || firstDeparture <= churn) {
				t.Fatalf("the case wants a late first departure: F2 opened at %d, the first departure is %d",
					churn, firstDeparture)
			}

			// 1. Sums of the phase ledgers against the run totals.
			var probes, refreshes, refused, detections, refilled, departed, admitted int
			last := 0
			for index, phase := range phases {
				if phase.From != last {
					t.Fatalf("phase %d (%s) starts at %d, the previous ended at %d", index, phase.Phase, phase.From, last)
				}
				last = phase.To
				probes += phase.Probes.Probes()
				refreshes += phase.Refreshes
				refused += phase.RefreshesRefused
				detections += phase.Detections
				refilled += phase.Refilled
				departed += phase.Departed
				admitted += phase.Admitted
				if phase.RefreshesRefused > phase.Refreshes {
					t.Fatalf("%s: %d refreshes refused out of %d scheduled", phase.Phase, phase.RefreshesRefused, phase.Refreshes)
				}
				if phase.Phase == phaseFill && (phase.Departed != 0 || phase.Detections != 0) {
					t.Fatalf("F1 booked %d departures and %d detections", phase.Departed, phase.Detections)
				}
			}
			if last != len(report.OnlineByTick) {
				t.Fatalf("the last phase ends at %d, the run has %d ticks", last, len(report.OnlineByTick))
			}
			sumRefilled := 0
			for _, count := range report.RefilledByLevel {
				sumRefilled += count
			}
			for _, check := range []struct {
				name        string
				phases, run int
			}{
				{"probes", probes, report.Probes.Probes()},
				{"detections", detections, len(report.DetectionDelays)},
				{"refilled", refilled, sumRefilled},
				{"departed", departed, report.Departed},
				{"admitted", admitted, report.ArrivalsAdmitted},
			} {
				if check.phases != check.run {
					t.Errorf("%s: the phases book %d, the run reports %d", check.name, check.phases, check.run)
				}
			}

			// 1a. Refreshes EXACTLY, per phase, against the offer trace: every
			// refresh offer of a measured owner in the phase's ticks is one
			// scheduled refresh, no more and no fewer; the refused ones are a
			// subset of the phase's owner-at-ceiling outcomes.
			for _, phase := range phases {
				expected := 0
				for _, offer := range report.Trace.Offers {
					if offer.Source == offerRefresh && network.measured(offer.Owner) &&
						offer.Tick >= phase.From && offer.Tick < phase.To {
						expected++
					}
				}
				if phase.Refreshes != expected {
					t.Errorf("%s: %d refreshes booked, the trace holds %d refresh offers of measured owners",
						phase.Phase, phase.Refreshes, expected)
				}
				if atCeiling := phase.Probes.Outcomes[m6OwnerAtBudget]; phase.RefreshesRefused > atCeiling {
					t.Errorf("%s: %d refreshes refused, but only %d probes ended at the owner's ceiling",
						phase.Phase, phase.RefreshesRefused, atCeiling)
				}
			}
			if refreshes == 0 && tc.config.Cadence > 0 && report.Probes.Probes() > 0 {
				t.Errorf("no refresh was scheduled in a run with cadence %d", tc.config.Cadence)
			}
			if tc.wantRefused && refused == 0 {
				t.Errorf("the case expects refreshes refused at the owner's ceiling, none was booked")
			}

			// 1b. Unmeasured participants: their refills, detections and
			// refreshes exist and stay OUT of the measured sums.
			{
				unmeasuredRefills, unmeasuredRefreshOffers := 0, 0
				for node, state := range network.states {
					if state == nil || network.measured(node) {
						continue
					}
					for _, count := range state.RefilledByLevel {
						unmeasuredRefills += count
					}
				}
				for _, offer := range report.Trace.Offers {
					if offer.Source == offerRefresh && !network.measured(offer.Owner) {
						unmeasuredRefreshOffers++
					}
				}
				if network.report.Members < len(g.ids) {
					if unmeasuredRefills == 0 || unmeasuredRefreshOffers == 0 ||
						report.PhysicalDetections <= len(report.DetectionDelays) {
						t.Fatalf("the Q-half case has no unmeasured work to exclude: %d refills, %d refresh "+
							"offers, %d physical against %d measured detections", unmeasuredRefills,
							unmeasuredRefreshOffers, report.PhysicalDetections, len(report.DetectionDelays))
					}
					if refilled+unmeasuredRefills == refilled {
						t.Fatalf("unmeasured refills are %d and would not change a sum", unmeasuredRefills)
					}
				} else if unmeasuredRefills != 0 || unmeasuredRefreshOffers != 0 {
					t.Fatalf("a whole-network run has unmeasured work: %d refills, %d refresh offers",
						unmeasuredRefills, unmeasuredRefreshOffers)
				}
			}

			// 2. ProbesAfterChurn from the first REALISED departure.
			want := 0
			if firstDeparture >= 0 {
				for _, count := range probesPerTick[firstDeparture:] {
					want += count
				}
			}
			if got := report.ProbesAfterChurn.Probes(); got != want {
				t.Errorf("ProbesAfterChurn is %d, the probes from the first departure (tick %d) on are %d",
					got, firstDeparture, want)
			}
			if !tc.wantOnset && (report.ProbesAfterChurn.Probes() != 0 || report.Probes.Probes() == 0) {
				t.Errorf("without a departure the after-churn ledger is %d and the run's %d", report.ProbesAfterChurn.Probes(), report.Probes.Probes())
			}
			t.Logf("first departure at tick %d (F2 at %d); %d ticks; probes %d, after churn %d",
				firstDeparture, churn, len(probesPerTick), report.Probes.Probes(), report.ProbesAfterChurn.Probes())
		})
	}
}

// TestM6TheStopRulesAtTheirEdges pins two edges of the stop rules on a
// schedule-only fixture — every node offline, so nothing ever changes and the
// signatures stay at their starting values — with the boundaries and reasons
// written out (П-1, П-2):
//
//	П-1  T_idle and the phase budget are reached on the SAME tick: the phase
//	     stops for idle (the rule's order), with the idle run equal to T_idle;
//	П-2  T_idle = 1: the first tick of F1 and of F3 is compared against the
//	     phase's starting signature and, unchanged, already closes the phase.
//
// Both are what the implementation does today; the contract's §5.9.1 names
// neither. They are pinned so that a change is a decision, not a drift.
//
// ⚠️ Mutations that must break it: checking the budget before the idle run
// (П-1 reports stopElapsed); starting the idle run at the second tick of a
// phase (П-2 closes F1 at tick 2).
func TestM6TheStopRulesAtTheirEdges(t *testing.T) {
	t.Parallel()

	frozen := func(t *testing.T, plan m6PhasePlan) *m6ModelReport {
		t.Helper()
		config := m6PhasedBase()
		config.Phases = &plan
		network := m6DirectFixture(t, config)
		for _, node := range network.all {
			network.online[node] = false
		}
		for tick := 0; ; tick++ {
			network.tick = tick
			done, err := network.step()
			if err != nil {
				t.Fatalf("tick %d: %v", tick, err)
			}
			if done {
				break
			}
		}
		network.report.Phases = network.schedule.trace()
		return network.report
	}
	requirePhases := func(t *testing.T, report *m6ModelReport, want []m6PhaseRecord) {
		t.Helper()
		if len(report.Phases) != len(want) {
			t.Fatalf("%d phases played, %d expected:\n%s", len(report.Phases), len(want), phaseLine(report.Phases))
		}
		for index, expected := range want {
			got := report.Phases[index]
			if got.Phase != expected.Phase || got.From != expected.From || got.To != expected.To ||
				got.Stop != expected.Stop || got.IdleRun != expected.IdleRun {
				t.Fatalf("phase %d: got %s [%d,%d) %q idle %d, want %s [%d,%d) %q idle %d",
					index, got.Phase, got.From, got.To, got.Stop, got.IdleRun,
					expected.Phase, expected.From, expected.To, expected.Stop, expected.IdleRun)
			}
		}
	}

	t.Run("П-1: T_idle and the budget reached on the same tick — idle wins, in F1 and in F3", func(t *testing.T) {
		t.Parallel()
		plan := m6PhasePlan{FillTicks: 4, IdleTicks: 4, RecoveryTicks: 4, CadenceTicks: 3}
		requirePhases(t, frozen(t, plan), []m6PhaseRecord{
			{Phase: phaseFill, From: 0, To: 4, Stop: stopIdle, IdleRun: 4},
			{Phase: phaseChurn, From: 4, To: 5, Stop: stopByConstruction},
			{Phase: phaseRecovery, From: 5, To: 9, Stop: stopIdle, IdleRun: 4},
			{Phase: phaseCadence, From: 9, To: 12, Stop: stopWindowEnd},
		})
	})

	t.Run("П-1 control: the budget alone, one tick short of T_idle", func(t *testing.T) {
		t.Parallel()
		plan := m6PhasePlan{FillTicks: 3, IdleTicks: 4, RecoveryTicks: 3, CadenceTicks: 3}
		requirePhases(t, frozen(t, plan), []m6PhaseRecord{
			{Phase: phaseFill, From: 0, To: 3, Stop: stopElapsed, IdleRun: 3},
			{Phase: phaseChurn, From: 3, To: 4, Stop: stopByConstruction},
			{Phase: phaseRecovery, From: 4, To: 7, Stop: stopElapsed, IdleRun: 3},
			{Phase: phaseCadence, From: 7, To: 10, Stop: stopWindowEnd},
		})
	})

	t.Run("П-2: T_idle = 1 — the first unchanged tick of F1 and of F3 closes the phase", func(t *testing.T) {
		t.Parallel()
		plan := m6PhasePlan{FillTicks: 8, IdleTicks: 1, RecoveryTicks: 8, CadenceTicks: 2}
		requirePhases(t, frozen(t, plan), []m6PhaseRecord{
			{Phase: phaseFill, From: 0, To: 1, Stop: stopIdle, IdleRun: 1},
			{Phase: phaseChurn, From: 1, To: 2, Stop: stopByConstruction},
			{Phase: phaseRecovery, From: 2, To: 3, Stop: stopIdle, IdleRun: 1},
			{Phase: phaseCadence, From: 3, To: 5, Stop: stopWindowEnd},
		})
	})
}
