package overlaysim

// m6_window_reference_test.go are the references for the recovery window of
// decision 3.5(ii) (m6_window_test.go): one interval [onset, onset + W_rec)
// for every configuration with churn, events booked by the tick they happened
// in, both ends of the interval exercised, a window that does not fit refused,
// and the criterion kept apart from the stop rule of F3.

import (
	"strings"
	"testing"
)

// TestM6TheRecoveryWindowIsTheSameForEveryCadence: three values of C, three
// (possibly) different F3 lengths, ONE window — starting at each run's onset
// and W_rec long — and the comparator's claim that says so for a pair.
//
// ⚠️ Mutation that must break it: opening the window at the start of F3 (or
// F4) instead of the onset, or sizing it from the phases instead of W_rec.
func TestM6TheRecoveryWindowIsTheSameForEveryCadence(t *testing.T) {
	t.Parallel()

	base := m6PhasedBase()
	base.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 20, CadenceTicks: 32}
	base.RecoveryWindow = 20

	for _, cadence := range []int{0, 8, 16} {
		config := base
		config.Cadence = cadence
		report := runM6Model(t, config)
		window := report.Window
		if window == nil {
			t.Fatalf("C=%d: no window in the report although one was asked for", cadence)
		}
		recovery := phaseNamed(t, report.Phases, phaseRecovery)
		if window.From != report.Trace.OnsetTick || window.To != window.From+base.RecoveryWindow ||
			window.Ticks != base.RecoveryWindow || !window.Closed {
			t.Fatalf("C=%d: window [%d, %d) of %d ticks, closed=%v; want [%d, %d) closed",
				cadence, window.From, window.To, window.Ticks, window.Closed,
				report.Trace.OnsetTick, report.Trace.OnsetTick+base.RecoveryWindow)
		}
		if recovery.From != window.From+1 {
			t.Fatalf("C=%d: F3 starts at %d, the window at %d — the window starts at the ONSET (F2), "+
				"not at F3", cadence, recovery.From, window.From)
		}
		t.Logf("C=%d: window [%d, %d), F3 %d ticks (%s); %s", cadence, window.From, window.To,
			recovery.Ticks(), recovery.Stop, window)
	}

	t.Run("the comparator claims the same window for a pair, and fails it for an odd one", func(t *testing.T) {
		t.Parallel()
		main := base
		main.Cadence = 8
		main.TraceOffers = true
		g := buildGraph(main.Shape, main.Seed, main.Quota, main.Policy)
		reference := runM6ModelOn(t, g, main, everybody)

		pair := func(controlWindow int) *m6Comparison {
			left, err := newM6Network(g, main, everybody)
			if err != nil {
				t.Fatalf("main: %v", err)
			}
			control := main
			control.StartEmpty = true
			control.ReplayPhases = reference.PhaseBoundaries()
			control.RecoveryWindow = controlWindow
			right, err := newM6Network(g, control, everybody)
			if err != nil {
				t.Fatalf("control: %v", err)
			}
			verdict, err := compareM6Runs(left, right)
			if err != nil {
				t.Fatalf("comparing: %v", err)
			}
			return verdict
		}
		if verdict := pair(base.RecoveryWindow); !verdict.Holds(claimSameRecoveryWindow) {
			t.Fatalf("a pair with one W_rec does not share a window:\n%s", verdict)
		}
		verdict := pair(0)
		claim := verdict.claim(claimSameRecoveryWindow)
		if claim.Status != claimFails {
			t.Fatalf("a control without a window passed the window claim: %s", claim)
		}
		t.Logf("odd pair: %s", claim)
	})
}

// TestM6TheRecoveryWindowMatchesAnIndependentRecomputation drives the run one
// tick at a time and keeps its OWN ledger from the report's cumulative
// counters: what the window booked must be exactly the events of the ticks
// [onset, onset + W_rec), with an event on the first tick (inside) and one on
// the tick after the last (outside) so that both ends are exercised, and the
// two remainders recomputed from the node states at the closing tick.
//
// ⚠️ Mutations that must break it: booking by the tick AFTER the event
// (closing one tick early), including tick To, opening at F3, reading the
// residual before the closing tick has been served.
func TestM6TheRecoveryWindowMatchesAnIndependentRecomputation(t *testing.T) {
	t.Parallel()

	config := m6PhasedBase()
	config.Churn = churnShrink // losses keep coming, so both ends of the window see events
	config.ChurnShare = 0.03
	config.ReturnShare = 0.5
	config.ReturnAfter = 3
	config.Cadence = 4
	config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 20, CadenceTicks: 24}
	config.RecoveryWindow = 10
	network := m6DirectFixture(t, config)

	type ledger struct {
		lost, refilled, detections, probes, refreshes int
	}
	var inside ledger
	var atFrom, atTo ledger
	clearedAt, recoveredAt := -1, -1
	residualAtClose, offlineAtClose, undetectedAtClose := -1, -1, -1
	previous := ledger{}
	report := network.report
	var window *m6RecoveryWindow

	for network.tick = 0; ; network.tick++ {
		done, err := network.step()
		if err != nil {
			t.Fatalf("tick %d: %v", network.tick, err)
		}
		if network.window != nil {
			window = network.window
		}
		now := ledger{
			lost:       sumOf(report.LostByLevel),
			refilled:   sumOf(report.RefilledByLevel),
			detections: len(report.DetectionDelays),
			probes:     report.Probes.Probes(),
		}
		for _, phase := range network.schedule.trace() {
			now.refreshes += phase.Refreshes
		}
		delta := ledger{
			lost: now.lost - previous.lost, refilled: now.refilled - previous.refilled,
			detections: now.detections - previous.detections, probes: now.probes - previous.probes,
			refreshes: now.refreshes - previous.refreshes,
		}
		previous = now

		if window != nil {
			switch network.tick {
			case window.From:
				atFrom = delta
			case window.To:
				atTo = delta
			}
			if network.tick >= window.From && network.tick < window.To {
				inside.lost += delta.lost
				inside.refilled += delta.refilled
				inside.detections += delta.detections
				inside.probes += delta.probes
				inside.refreshes += delta.refreshes
				// The two indicators, recomputed from the states at the end of
				// the tick: the known deficit of the ONLINE owners (narrow), and
				// recovery — every measured owner's deficit, online or not, zero
				// AND no online owner holding a record of a departed node.
				residual, offline, undetected := 0, 0, 0
				for _, owner := range network.owners {
					state := network.states[owner]
					if state == nil {
						continue
					}
					deficit := 0
					for level := range state.LostByLevel {
						deficit += max(state.LostByLevel[level]-state.RefilledByLevel[level], 0)
					}
					if !network.online[owner] {
						offline += deficit
						continue
					}
					residual += deficit
					for _, level := range state.Table.members {
						for member := range level {
							if !network.online[member] {
								undetected++
							}
						}
					}
				}
				if clearedAt < 0 && inside.lost > 0 && residual == 0 {
					clearedAt = network.tick - window.From
				}
				if recoveredAt < 0 && inside.lost > 0 && residual+offline == 0 && undetected == 0 {
					recoveredAt = network.tick - window.From
				}
				if network.tick == window.To-1 {
					residualAtClose, offlineAtClose, undetectedAtClose = residual, offline, undetected
				}
			}
		}
		if done {
			break
		}
	}
	if window == nil || !window.Closed {
		t.Fatalf("the window never opened or never closed: %v", window)
	}
	if atFrom.lost == 0 || atFrom.detections == 0 {
		t.Fatalf("no loss was detected in the onset tick %d, so the INSIDE end of the window is not "+
			"exercised by this fixture", window.From)
	}
	if atTo.lost == 0 && atTo.detections == 0 && atTo.probes == 0 {
		t.Fatalf("nothing happened in tick %d, so the OUTSIDE end of the window is not exercised", window.To)
	}
	booked := ledger{
		lost: window.LostEvents, refilled: window.RefilledEvents, detections: window.DetectionEvents,
		probes: window.Probes.Probes(), refreshes: window.Refreshes,
	}
	if booked != inside {
		t.Fatalf("the window booked %+v, the recomputation over ticks [%d, %d) gives %+v (the onset "+
			"tick carried %+v, the tick after the window %+v)", booked, window.From, window.To, inside,
			atFrom, atTo)
	}
	if window.ResidualDeficit != residualAtClose || window.ResidualDeficitOffline != offlineAtClose ||
		window.UndetectedDeadRecords != undetectedAtClose {
		t.Fatalf("at the closing tick the window says residual %d / offline %d / undetected %d, the states "+
			"say %d / %d / %d", window.ResidualDeficit, window.ResidualDeficitOffline,
			window.UndetectedDeadRecords, residualAtClose, offlineAtClose, undetectedAtClose)
	}
	if window.KnownDeficitClearedAtTau != clearedAt || window.RecoveredAtTau != recoveredAt {
		t.Fatalf("the window says known deficit cleared at τ=%d / recovered at τ=%d, the recomputation "+
			"says %d / %d", window.KnownDeficitClearedAtTau, window.RecoveredAtTau, clearedAt, recoveredAt)
	}
	t.Logf("window [%d, %d): %+v; onset tick %+v, tick after %+v; residual %d (+%d offline), undetected "+
		"%d, known deficit cleared at τ=%d, recovered at τ=%d", window.From, window.To, inside, atFrom,
		atTo, residualAtClose, offlineAtClose, undetectedAtClose, clearedAt, recoveredAt)
}

// TestM6ARecoveryWindowThatDoesNotFitIsRefused is the door: the plan's own
// shortest run, the REPLAYED boundaries, the flat schedule, and a form with no
// churn — each refused with the reason, never cut short.
//
// ⚠️ Mutation that must break it: checking the plan's minimum for a replayed
// run (a pinned F3 of one tick then passes a window the run cannot hold), or
// the plan's budgets instead of its minimum.
func TestM6ARecoveryWindowThatDoesNotFitIsRefused(t *testing.T) {
	t.Parallel()

	g := buildGraph(m6ModelShape(), 1, 1, policyInitiatedLimit)
	cases := []struct {
		name    string
		config  func() m6ModelConfig
		refused bool
		reason  string
	}{
		{
			name: "plan: 1 + min(64, 8) + 1000 = 1009 < 1024 — refused",
			config: func() m6ModelConfig {
				c := m6PhasedBase()
				c.Phases = &m6PhasePlan{FillTicks: 256, IdleTicks: 64, RecoveryTicks: 8, CadenceTicks: 1000}
				c.RecoveryWindow = 1024
				return c
			},
			refused: true, reason: "does not fit",
		},
		{
			name: "plan: 1 + min(64, 8) + 1024 = 1033 ≥ 1024 — accepted",
			config: func() m6ModelConfig {
				c := m6PhasedBase()
				c.Phases = &m6PhasePlan{FillTicks: 256, IdleTicks: 64, RecoveryTicks: 8, CadenceTicks: 1024}
				c.RecoveryWindow = 1024
				return c
			},
		},
		{
			name: "replay: pinned F3 of 1 tick and F4 of 1020 = 1022 < 1024 — refused although the plan would fit",
			config: func() m6ModelConfig {
				c := m6PhasedBase()
				c.Phases = &m6PhasePlan{FillTicks: 256, IdleTicks: 64, RecoveryTicks: 256, CadenceTicks: 1020}
				c.StartEmpty = true
				c.ReplayPhases = []m6PhaseBoundary{
					{Phase: phaseFill, From: 0, To: 10}, {Phase: phaseChurn, From: 10, To: 11},
					{Phase: phaseRecovery, From: 11, To: 12}, {Phase: phaseCadence, From: 12, To: 1032},
				}
				c.RecoveryWindow = 1024
				return c
			},
			refused: true, reason: "REPLAYED",
		},
		{
			name: "replay: pinned F3 of 3 ticks and F4 of 1020 = 1024 — accepted",
			config: func() m6ModelConfig {
				c := m6PhasedBase()
				c.Phases = &m6PhasePlan{FillTicks: 256, IdleTicks: 64, RecoveryTicks: 256, CadenceTicks: 1020}
				c.StartEmpty = true
				c.ReplayPhases = []m6PhaseBoundary{
					{Phase: phaseFill, From: 0, To: 10}, {Phase: phaseChurn, From: 10, To: 11},
					{Phase: phaseRecovery, From: 11, To: 14}, {Phase: phaseCadence, From: 14, To: 1034},
				}
				c.RecoveryWindow = 1024
				return c
			},
		},
		{
			name: "flat: 12 ticks with churn at 4 hold 8, a window of 9 is refused",
			config: func() m6ModelConfig {
				c := m6ModelBase()
				c.RecoveryWindow = 9
				return c
			},
			refused: true, reason: "flat schedule",
		},
		{
			name: "flat: a window of 8 fits exactly",
			config: func() m6ModelConfig {
				c := m6ModelBase()
				c.RecoveryWindow = 8
				return c
			},
		},
		{
			name: "no churn form: there is no onset to start a window from",
			config: func() m6ModelConfig {
				c := m6ModelBase()
				c.Churn = churnNone
				c.RecoveryWindow = 4
				return c
			},
			refused: true, reason: "no onset",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newM6Network(g, tc.config(), everybody)
			switch {
			case tc.refused && err == nil:
				t.Fatal("accepted; want a refusal at the door")
			case tc.refused && !strings.Contains(err.Error(), tc.reason):
				t.Fatalf("refused for the wrong reason: %v (want %q)", err, tc.reason)
			case !tc.refused && err != nil:
				t.Fatalf("refused: %v", err)
			}
		})
	}
}

// TestM6F3MayCloseBeforeTheFirstRefreshAndTheWindowStillReadsIt is the
// scenario the review named (package §3.5): when every level is younger than
// C − T_idle at the onset, no scheduled refresh falls inside the first T_idle
// ticks of F3, the deficit does not move, and F3 closes by idle before the
// first refresh — a SCENARIO, printed, not a defect. The window, being W_rec
// from the onset, still reads the refreshes and the losses that come later,
// and the report keeps the stop rule apart from the criterion.
//
// ⚠️ Mutation that must break it: reading the recovery axis off F3 alone
// (the losses of F4 vanish), or calling an idle stop of F3 "recovered".
func TestM6F3MayCloseBeforeTheFirstRefreshAndTheWindowStillReadsIt(t *testing.T) {
	t.Parallel()

	config := m6PhasedBase()
	config.Branch = branchA // a held record is re-probed by the cadence and by nothing else
	config.Cadence = 64     // longer than F1 can be: every level is younger than C − T_idle at the onset
	config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 40, CadenceTicks: 100}
	config.RecoveryWindow = 80
	report := runM6Model(t, config)

	recovery := phaseNamed(t, report.Phases, phaseRecovery)
	if recovery.Stop != stopIdle || recovery.Refreshes != 0 {
		t.Fatalf("F3 stopped for %q after %d ticks with %d refreshes; the fixture wants an idle "+
			"stop before any refresh", recovery.Stop, recovery.Ticks(), recovery.Refreshes)
	}
	window := report.Window
	if window == nil || !window.Closed {
		t.Fatalf("no closed window: %v", window)
	}
	if window.Refreshes == 0 || window.LostEvents == 0 {
		t.Fatalf("the window saw %d refreshes and %d losses after F3 had closed on nothing — the "+
			"later refreshes are the point of reading W_rec from the onset", window.Refreshes,
			window.LostEvents)
	}
	if window.RecoveredAtTau >= 0 && window.RecoveredAtTau <= recovery.Ticks() {
		t.Fatalf("the criterion is said to be reached at τ=%d, inside an F3 (%d ticks) that "+
			"detected no table loss", window.RecoveredAtTau, recovery.Ticks())
	}

	line := report.RecoveryLine()
	for _, want := range []string{"STOP RULE", "NOT the recovery criterion", "recovery window ["} {
		if !strings.Contains(line, want) {
			t.Errorf("the recovery line does not say %q:\n%s", want, line)
		}
	}
	reached := strings.Contains(line, "recovery criterion reached at τ=")
	notReached := strings.Contains(line, "recovery criterion NOT REACHED within")
	if reached == notReached {
		t.Errorf("the recovery line must say exactly one of reached / not reached:\n%s", line)
	}
	if window.RecoveredAtTau < 0 && !notReached {
		t.Errorf("the criterion was not reached and the line does not say so:\n%s", line)
	}
	t.Logf("F3: %d ticks, %s; %s", recovery.Ticks(), recovery.Stop, window)
}

// windowFixture is a network with a window opened by hand on tick `from`, so
// the criterion can be judged on a state the fixture composes exactly.
func windowFixture(t *testing.T, config m6ModelConfig, from, ticks int) (*m6Network, *m6RecoveryWindow) {
	t.Helper()
	network := m6DirectFixture(t, config)
	network.window = newM6RecoveryWindow(from, ticks)
	network.report.Window = network.window
	network.churnSeen = true
	return network, network.window
}

// TestM6RecoveryIsNotTheClearingOfTheOnlineOwnersDeficit is the owner's P2 on
// the criterion (2026-09-18): the KNOWN deficit of the owners ONLINE at the
// moment can reach zero without a single refill — the only owner with a
// deficit leaves — or with one refill while other dead records are still
// undetected. Neither is recovery. The window now reports the two apart: the
// narrow indicator under its own name, and recovery as "every detected loss
// of every measured owner refilled AND no online owner holds a record of a
// departed node".
//
// ⚠️ Mutations that must break it: judging recovery over the online owners
// only (case 1 passes as recovery); ignoring the undetected dead records
// (case 2 passes as recovery); dropping an offline owner's deficit.
func TestM6RecoveryIsNotTheClearingOfTheOnlineOwnersDeficit(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Cadence = 0

	// deficitOwner stores one record at `owner` and detects its loss, so the
	// owner carries a deficit of one at that level.
	deficitOwner := func(t *testing.T, n *m6Network, owner int32) (peer int32, level int) {
		t.Helper()
		state := n.states[owner]
		peer = n.neighboursOf(owner)[0]
		level = levelOf(n.ids[owner], n.ids[peer], config.Shape.degree)
		n.probe(owner, state, peer, level, false)
		if !state.Table.holds(peer) {
			t.Fatalf("owner %d could not store %d: %s", owner, peer, n.report.Probes.breakdown())
		}
		n.online[peer] = false
		n.departedAt[peer] = n.tick
		clear(state.TriedThisTick)
		n.probe(owner, state, peer, level, false)
		if state.LostByLevel[level] != 1 {
			t.Fatalf("owner %d did not detect the loss of %d", owner, peer)
		}
		return peer, level
	}

	t.Run("the only owner with a deficit leaves: the known deficit clears, recovery is not reached", func(t *testing.T) {
		t.Parallel()
		n, window := windowFixture(t, config, 4, 8)
		n.tick = 4
		const owner = int32(0)
		deficitOwner(t, n, owner)
		if window.LostEvents != 1 {
			t.Fatalf("the window booked %d losses, want 1", window.LostEvents)
		}
		n.closeRecoveryWindowIfDue()
		if window.KnownDeficitClearedAtTau >= 0 || window.RecoveredAtTau >= 0 {
			t.Fatalf("with the deficit still open nothing may be reached: cleared %d, recovered %d",
				window.KnownDeficitClearedAtTau, window.RecoveredAtTau)
		}

		// The owner departs. Nobody refilled anything.
		n.tick = 5
		n.online[owner] = false
		n.departedAt[owner] = 5
		n.closeRecoveryWindowIfDue()
		if window.KnownDeficitClearedAtTau != 1 {
			t.Fatalf("the known deficit of the ONLINE owners is zero once the owner left; the narrow "+
				"indicator says %d, want τ=1", window.KnownDeficitClearedAtTau)
		}
		if window.RecoveredAtTau >= 0 {
			t.Fatalf("recovery was declared at τ=%d although the only detected loss was never refilled — "+
				"its owner merely left", window.RecoveredAtTau)
		}
		// Through the end of the window the verdict stays: not reached, and the
		// deficit is reported as carried by an OFFLINE owner.
		for n.tick = 6; n.tick < window.To; n.tick++ {
			n.closeRecoveryWindowIfDue()
		}
		if !window.Closed || window.RecoveredAtTau >= 0 {
			t.Fatalf("closed=%v, recovered=%d", window.Closed, window.RecoveredAtTau)
		}
		if window.ResidualDeficit != 0 || window.ResidualDeficitOffline != 1 {
			t.Fatalf("at the end the online residual is %d and the offline residual %d; want 0 and 1 — the "+
				"deficit left with its owner and is still owed", window.ResidualDeficit,
				window.ResidualDeficitOffline)
		}
		line := window.String()
		for _, want := range []string{"NOT REACHED", "known deficit of the online owners first zero at τ=1"} {
			if !strings.Contains(line, want) {
				t.Errorf("the window line does not say %q:\n%s", want, line)
			}
		}
	})

	t.Run("one loss refilled while another dead record is undetected: not recovery until it is found and refilled", func(t *testing.T) {
		t.Parallel()
		n, window := windowFixture(t, config, 4, 8)
		n.tick = 4
		const owner = int32(0)
		state := n.states[owner]
		gone, level := deficitOwner(t, n, owner)

		// A second record, of a node that dies UNDETECTED: the owner never
		// probes it in this fixture.
		var second int32 = -1
		for _, peer := range n.neighboursOf(owner) {
			if peer != gone && levelOf(n.ids[owner], n.ids[peer], config.Shape.degree) != level {
				second = peer
				break
			}
		}
		if second < 0 {
			t.Fatal("no second neighbour on another level")
		}
		clear(state.TriedThisTick)
		n.probe(owner, state, second, levelOf(n.ids[owner], n.ids[second], config.Shape.degree), false)
		if !state.Table.holds(second) {
			t.Fatalf("could not store the second record: %s", n.report.Probes.breakdown())
		}
		n.online[second] = false
		n.departedAt[second] = 4

		// The first loss is refilled: the departed node comes back and is
		// re-stored at the level that lost it.
		n.tick = 5
		n.online[gone] = true
		clear(state.TriedThisTick)
		n.probe(owner, state, gone, level, false)
		if state.RefilledByLevel[level] != 1 {
			t.Fatalf("the refill was not counted: lost %v, refilled %v", state.LostByLevel, state.RefilledByLevel)
		}
		n.closeRecoveryWindowIfDue()
		if window.KnownDeficitClearedAtTau != 1 {
			t.Fatalf("the known deficit is zero after the refill; the narrow indicator says %d, want τ=1",
				window.KnownDeficitClearedAtTau)
		}
		if window.RecoveredAtTau >= 0 {
			t.Fatalf("recovery was declared at τ=%d while the owner still holds a record of the departed "+
				"node %d it has not found out about", window.RecoveredAtTau, second)
		}

		// The second loss is detected and refilled (the node returns): now it is recovery.
		n.tick = 6
		clear(state.TriedThisTick)
		secondLevel := levelOf(n.ids[owner], n.ids[second], config.Shape.degree)
		n.probe(owner, state, second, secondLevel, false) // detects the loss
		n.online[second] = true
		clear(state.TriedThisTick)
		n.probe(owner, state, second, secondLevel, false) // refills it
		n.closeRecoveryWindowIfDue()
		if window.RecoveredAtTau != 2 {
			t.Fatalf("every detected loss refilled and no dead record held: recovery says τ=%d, want 2 "+
				"(lost %v, refilled %v, undetected %d)", window.RecoveredAtTau, state.LostByLevel,
				state.RefilledByLevel, n.measuredUndetectedDeadRecords())
		}
		if window.KnownDeficitClearedAtTau != 1 {
			t.Fatalf("the narrow indicator moved to %d; it is the FIRST τ", window.KnownDeficitClearedAtTau)
		}
	})
}

// TestM6TheRecoveryWindowCountsTheMechanismsFramesByTheirTick is the owner's
// second P2 (2026-09-18): the frames of the mechanism — exchanges served and
// refused, addressed answers, refusals at a responder, requests nobody
// answered, repeats filtered — inside the window, by the tick of the event,
// kept APART from the probes (no cost model sums a frame with a probe), and
// checked against an independent recomputation with an event on the onset
// tick and one on the tick after the window.
//
// ⚠️ Mutations that must break it: booking a frame outside the window,
// booking exchanges served for refused ones, adding frames into the probe
// ledger.
func TestM6TheRecoveryWindowCountsTheMechanismsFramesByTheirTick(t *testing.T) {
	t.Parallel()

	type frames struct {
		served, refused, answers, rateLimited, unanswered, filtered int
	}
	read := func(r *m6ModelReport) frames {
		return frames{
			served: r.ExchangesDone, refused: r.ExchangesRefused, answers: r.AddressedAnswers,
			rateLimited: r.AddressedRateLimited, unanswered: r.AddressedRefused, filtered: r.RepeatsFiltered,
		}
	}
	minus := func(a, b frames) frames {
		return frames{a.served - b.served, a.refused - b.refused, a.answers - b.answers,
			a.rateLimited - b.rateLimited, a.unanswered - b.unanswered, a.filtered - b.filtered}
	}
	plus := func(a, b frames) frames {
		return frames{a.served + b.served, a.refused + b.refused, a.answers + b.answers,
			a.rateLimited + b.rateLimited, a.unanswered + b.unanswered, a.filtered + b.filtered}
	}

	cases := []struct {
		name   string
		branch m6Branch
		filter bool
	}{
		{name: "A′ with the local repeat filter: exchanges and filtered repeats", branch: branchAPrime, filter: true},
		{name: "C: addressed answers, refusals at a responder and unanswered requests", branch: branchC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := m6PhasedBase()
			config.Branch = tc.branch
			config.LocalRepeatFilter = tc.filter
			config.Churn = churnShrink
			config.ChurnShare = 0.03
			config.ReturnShare = 0.5
			config.ReturnAfter = 3
			config.Cadence = 4
			config.ExchangeEvery = 2
			config.RatePair = 1
			config.RateNode = 1 // tight: refusals at responders and unanswered requests happen
			config.Phases = &m6PhasePlan{FillTicks: 40, IdleTicks: 4, RecoveryTicks: 20, CadenceTicks: 24}
			config.RecoveryWindow = 10
			network := m6DirectFixture(t, config)
			report := network.report

			var inside, atFrom, atTo frames
			previous := frames{}
			var window *m6RecoveryWindow
			for network.tick = 0; ; network.tick++ {
				done, err := network.step()
				if err != nil {
					t.Fatalf("tick %d: %v", network.tick, err)
				}
				if network.window != nil {
					window = network.window
				}
				now := read(report)
				delta := minus(now, previous)
				previous = now
				if window != nil {
					switch network.tick {
					case window.From:
						atFrom = delta
					case window.To:
						atTo = delta
					}
					if network.tick >= window.From && network.tick < window.To {
						inside = plus(inside, delta)
					}
				}
				if done {
					break
				}
			}
			if window == nil || !window.Closed {
				t.Fatalf("no closed window: %v", window)
			}
			booked := frames{
				served: window.ExchangesServed, refused: window.ExchangesRefused,
				answers: window.AddressedAnswers, rateLimited: window.AddressedRateLimited,
				unanswered: window.AddressedRefused, filtered: window.RepeatsFiltered,
			}
			if booked != inside {
				t.Fatalf("the window booked %+v, the recomputation over [%d, %d) gives %+v", booked,
					window.From, window.To, inside)
			}
			// Both ends exercised, on the frames this branch produces.
			exercised := func(f frames) bool {
				if tc.branch == branchC {
					return f.answers > 0 && f.rateLimited > 0
				}
				return f.served > 0 && f.filtered > 0
			}
			if !exercised(atFrom) || !exercised(atTo) {
				t.Fatalf("the ends of the window are not exercised: onset tick %+v, tick after %+v", atFrom, atTo)
			}
			if tc.branch == branchC && (inside.unanswered == 0 || inside.rateLimited == 0) {
				t.Fatalf("branch C produced no refusals inside the window: %+v", inside)
			}
			line := window.String()
			if !strings.Contains(line, "frames") || !strings.Contains(line, "not summed with the probes") {
				t.Errorf("the window line does not print the frames apart from the probes:\n%s", line)
			}
			t.Logf("window [%d, %d): frames %+v; onset tick %+v, tick after %+v", window.From, window.To,
				inside, atFrom, atTo)
		})
	}
}
