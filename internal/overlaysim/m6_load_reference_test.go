package overlaysim

// m6_load_reference_test.go are the references for the EXTERNAL LOAD of §5.9.2
// under the owner's decisions of 2026-09-18 (review package §4):
//
//	3.3 (в) — every churn draw (a departure, the return of that departure, the
//	          host of a newcomer) is keyed on τ = tick − onset and the identity,
//	          so two runs with the same seed and load parameters draw the same
//	          DECISIONS at the same τ however long their F1 lasted and whatever
//	          the mechanism spent inside;
//	3.1 (a) — the compensated form offers the returns due UNCONDITIONALLY and
//	          tops up with newcomers to the number of departures; the report
//	          keeps every count of §5.9.2 apart and names the surplus.
//
// ⚠️ Two different statements, checked apart: the DRAWS agree by construction;
// the REALISED events agree only while the online sets agree. Under a form
// with no arrivals but returns (shrink + ret) the online sets are a function of
// the draws alone, so the realised events agree in full; under compensated
// load a newcomer needs a host with a free slot, admissions depend on what the
// mechanism detected, and from the first differing admission the realised
// departures may part — that dependency is named here, not hidden behind "the
// same seed".

import (
	"testing"
)

// m6RelativeScenario is a scenario trace re-keyed on τ = tick − onset, with
// the ticks before the onset dropped (they carry no churn), and the number of
// identifiers the run carried — the reserve is sized for the horizon, so two
// runs of different length draw over index spaces of different size.
type m6RelativeScenario struct {
	events map[int]m6TickEvents
	ids    int
}

// relativeScenario re-keys a run's scenario trace on τ.
func relativeScenario(t *testing.T, report *m6ModelReport) m6RelativeScenario {
	t.Helper()
	onset := report.Trace.OnsetTick
	if onset < 0 {
		t.Fatalf("the run had no onset: %s", report.PhaseLine())
	}
	relative := m6RelativeScenario{events: map[int]m6TickEvents{}, ids: len(report.Trace.IDs)}
	for _, events := range report.Trace.Scenario {
		if events.Tick < onset {
			if len(events.Decided)+len(events.Departed)+len(events.ReturnsDue)+len(events.NewcomersOffered) > 0 {
				t.Fatalf("tick %d, before the onset %d, carries churn events: %+v", events.Tick, onset, events)
			}
			continue
		}
		relative.events[events.Tick-onset] = events
	}
	return relative
}

// below keeps the nodes with an index under `limit`.
func below(nodes []int32, limit int) []int32 {
	kept := make([]int32, 0, len(nodes))
	for _, node := range nodes {
		if int(node) < limit {
			kept = append(kept, node)
		}
	}
	return kept
}

// requireSameDraws asserts the departure DECISIONS agree at every τ both runs
// played, and returns how many τ were compared.
//
// ⚠️ Compared over the index space BOTH runs carry. A departure is drawn over
// every identifier of the run, the unjoined reserve included (stand assumption
// 21), and the reserve is sized for the horizon — a longer run draws over more
// identifiers, and those extra draws land on nobody. The shorter reserve is a
// prefix of the longer (assumption 1), so the common space is a prefix too.
func requireSameDraws(t *testing.T, left, right m6RelativeScenario) int {
	t.Helper()
	compared, decided := 0, 0
	shared := min(left.ids, right.ids)
	for tau := 0; ; tau++ {
		one, inLeft := left.events[tau]
		other, inRight := right.events[tau]
		if !inLeft || !inRight {
			break
		}
		compared++
		decided += len(one.Decided)
		if !equalNodes(below(one.Decided, shared), below(other.Decided, shared)) {
			t.Fatalf("τ=%d: the departure DECISIONS differ, %v against %v — the draw is keyed on "+
				"something the runs do not share", tau, one.Decided, other.Decided)
		}
	}
	if compared == 0 {
		t.Fatal("the runs share no τ, so nothing was compared")
	}
	if decided == 0 {
		t.Fatal("no departure was decided on the shared interval, so this fixture compares nothing")
	}
	return compared
}

// firstRealisedDifference finds the first τ at which the REALISED events part.
func firstRealisedDifference(left, right m6RelativeScenario, taus int) (int, string, bool) {
	for tau := range taus {
		one, other := left.events[tau], right.events[tau]
		switch {
		case !equalNodes(one.Departed, other.Departed):
			return tau, "realised departures", true
		case !equalNodes(one.ReturnsDue, other.ReturnsDue):
			return tau, "returns due", true
		case !equalNodes(one.NewcomersOffered, other.NewcomersOffered):
			return tau, "newcomers offered", true
		}
	}
	return 0, "", false
}

// TestM6ExternalEventsAreKeyedOnTheTickSinceTheOnset is decision 3.3(в): the
// same seed and load give the same churn at the same τ, whatever tick the
// onset fell on.
//
// ⚠️ Mutation that must break it: keying any of the three draws (leave,
// return, join) on the absolute tick — the runs with different onsets then
// decide different departures at the same τ, and the shock takes different
// nodes.
func TestM6ExternalEventsAreKeyedOnTheTickSinceTheOnset(t *testing.T) {
	t.Parallel()

	t.Run("background with returns: two onsets, the same draws AND the same realised events", func(t *testing.T) {
		t.Parallel()
		base := m6ModelBase()
		base.Churn = churnShrink
		base.ChurnShare = 0.03
		base.ReturnShare = 0.5
		base.ReturnAfter = 3
		const played = 10

		play := func(onset int) m6RelativeScenario {
			config := base
			config.ChurnAt = onset
			config.Ticks = onset + played
			return relativeScenario(t, runM6Model(t, config))
		}
		early, late := play(2), play(6)
		taus := requireSameDraws(t, early, late)
		if taus != played {
			t.Fatalf("compared %d τ, want %d", taus, played)
		}
		// No arrivals but returns, and a return needs no host: the online sets
		// are a function of the draws alone, so the realised events agree too.
		if tau, what, differ := firstRealisedDifference(early, late, taus); differ {
			t.Fatalf("τ=%d: %s differ although nothing but the onset tick was changed:\n early %+v\n "+
				"late  %+v", tau, what, early.events[tau], late.events[tau])
		}
		returns := 0
		for tau := range taus {
			returns += len(early.events[tau].ReturnsDue)
		}
		if returns == 0 {
			t.Fatal("no return came due on the shared interval, so the return key was not exercised")
		}
	})

	t.Run("shock: two onsets, the same set of nodes leaves", func(t *testing.T) {
		t.Parallel()
		base := m6ModelBase()
		play := func(onset int) m6RelativeScenario {
			config := base
			config.ChurnAt = onset
			config.Ticks = onset + 6
			return relativeScenario(t, runM6Model(t, config))
		}
		early, late := play(2), play(6)
		if len(early.events[0].Departed) == 0 {
			t.Fatal("the shock took nobody")
		}
		if !equalNodes(early.events[0].Departed, late.events[0].Departed) {
			t.Fatalf("the shock took %d nodes at onset 2 and %d at onset 6, and not the same ones",
				len(early.events[0].Departed), len(late.events[0].Departed))
		}
	})

	t.Run("phased: F1 of different lengths, the same draws at the same τ", func(t *testing.T) {
		t.Parallel()
		base := m6PhasedBase()
		base.Churn = churnShrink
		base.ChurnShare = 0.03
		base.ReturnShare = 0.5
		base.ReturnAfter = 3
		play := func(fill int) *m6ModelReport {
			config := base
			// T_idle beyond T_fill: F1 runs to its budget, so its length is the
			// budget and the onset moves with it.
			config.Phases = &m6PhasePlan{FillTicks: fill, IdleTicks: 100, RecoveryTicks: 12, CadenceTicks: 12}
			return runM6Model(t, config)
		}
		short, long := play(3), play(7)
		if short.Trace.OnsetTick == long.Trace.OnsetTick {
			t.Fatalf("both runs began churn at tick %d — the fixture failed to move the onset",
				short.Trace.OnsetTick)
		}
		taus := requireSameDraws(t, relativeScenario(t, short), relativeScenario(t, long))
		if tau, what, differ := firstRealisedDifference(relativeScenario(t, short),
			relativeScenario(t, long), taus); differ {
			t.Fatalf("τ=%d: %s differ between F1 of 3 and of 7 ticks", tau, what)
		}
		t.Logf("onsets %d and %d, %d τ compared", short.Trace.OnsetTick, long.Trace.OnsetTick, taus)
	})

	t.Run("a longer horizon does not change the prefix already formed", func(t *testing.T) {
		t.Parallel()
		base := m6ModelBase()
		base.Churn = churnCompensated
		base.ChurnShare = 0.05
		base.ReturnShare = 0.5
		base.ReturnAfter = 3
		base.ChurnAt = 3
		play := func(ticks int) m6RelativeScenario {
			config := base
			config.Ticks = ticks
			return relativeScenario(t, runM6Model(t, config))
		}
		short, long := play(12), play(36)
		taus := requireSameDraws(t, short, long)
		if taus != 12-3 {
			t.Fatalf("compared %d τ, want the short run's %d", taus, 12-3)
		}
		if tau, what, differ := firstRealisedDifference(short, long, taus); differ {
			t.Fatalf("τ=%d: %s differ between a run of 12 and of 36 ticks — the prefix depends on "+
				"the horizon", tau, what)
		}
	})

	t.Run("compensated load: the draws agree; the realised events agree while the admissions do", func(t *testing.T) {
		t.Parallel()
		base := m6ModelBase()
		base.Churn = churnCompensated
		base.ChurnShare = 0.05
		base.ReturnShare = 0.5
		base.ReturnAfter = 3
		const played = 12

		type outcome struct {
			events   m6RelativeScenario
			admitted []int // ArrivalsAdmitted at the end of every τ
		}
		play := func(onset int) outcome {
			config := base
			config.ChurnAt = onset
			config.Ticks = onset + played
			network := m6DirectFixture(t, config)
			result := outcome{events: m6RelativeScenario{events: map[int]m6TickEvents{}, ids: len(network.ids)}}
			for network.tick = 0; network.tick < config.Ticks; network.tick++ {
				if _, err := network.step(); err != nil {
					t.Fatalf("tick %d: %v", network.tick, err)
				}
				if network.tick < onset {
					continue
				}
				result.events.events[network.tick-onset] = network.trace.Scenario[network.tick]
				result.admitted = append(result.admitted, network.report.ArrivalsAdmitted)
			}
			return result
		}
		early, late := play(2), play(6)
		taus := requireSameDraws(t, early.events, late.events)
		tau, what, differ := firstRealisedDifference(early.events, late.events, taus)
		if !differ {
			t.Logf("the realised events agreed over all %d τ (the admissions happened to agree too)", taus)
			return
		}
		// ⚠️ The dependency, named: realised events may part only AFTER the
		// admissions parted — a newcomer needs a host with a free slot, and
		// what freed a slot is the mechanism, which the onset tick moved.
		for before := 0; before <= tau; before++ {
			if early.admitted[before] != late.admitted[before] {
				t.Logf("τ=%d: %s differ; the admissions had parted at τ=%d (%d against %d) — the "+
					"realised events are conditional on the online sets, and this is that dependency",
					tau, what, before, early.admitted[before], late.admitted[before])
				return
			}
		}
		t.Fatalf("τ=%d: %s differ although the admissions agreed up to there — a realised "+
			"difference with no admission behind it is a draw keyed on the absolute tick", tau, what)
	})
}

// TestM6TheModelsInternalWorkDoesNotMoveTheExternalDraws is the second half of
// 3.3(в): the draws of the load have purposes of their own, so what the
// mechanism spends inside — which branch, whether the control knows everybody,
// how often it refreshes, how many probes it may make — never reaches them.
//
// ⚠️ Mutation that must break it: sharing a purpose (or a counter) between an
// internal draw and an external one — the omniscient walk's offset with the
// shock's shuffle, say — so that switching the control on changes who leaves.
func TestM6TheModelsInternalWorkDoesNotMoveTheExternalDraws(t *testing.T) {
	t.Parallel()

	base := m6ModelBase()
	base.Churn = churnShrink
	base.ChurnShare = 0.03
	base.ReturnShare = 0.5
	base.ReturnAfter = 3
	base.ChurnAt = 3
	base.Ticks = 15

	variants := map[string]func(*m6ModelConfig){
		"branch A":           func(c *m6ModelConfig) { c.Branch = branchA },
		"branch A′":          func(c *m6ModelConfig) { c.Branch = branchAPrime },
		"branch C":           func(c *m6ModelConfig) { c.Branch = branchC },
		"omniscient control": func(c *m6ModelConfig) { c.OmniscientControl = true },
		"C = ∞":              func(c *m6ModelConfig) { c.Cadence = 0 },
		"R = 1":              func(c *m6ModelConfig) { c.Repair = 1 },
		"no ceiling":         func(c *m6ModelConfig) { c.Repair = 0 },
	}
	reference := relativeScenario(t, runM6Model(t, base))
	for name, vary := range variants {
		config := base
		vary(&config)
		events := relativeScenario(t, runM6Model(t, config))
		taus := requireSameDraws(t, reference, events)
		// Shrink with returns: the online sets follow the draws alone, so even
		// the realised events must agree whatever the mechanism did.
		if tau, what, differ := firstRealisedDifference(reference, events, taus); differ {
			t.Errorf("%s: τ=%d, %s differ from the base — the mechanism's own work moved the load",
				name, tau, what)
		}
	}
}

// TestM6TheCompensationLedgerKeepsEveryCountApart is decision 3.1(a) as
// arithmetic: every count §5.9.2 wants apart is in the report, the splits add
// up, and the surplus of offers over departures is exactly what the scenario
// trace gives tick by tick.
//
// ⚠️ Mutations that must break it: newcomers offered to the departures
// regardless of the returns (the offers stop adding up to max(departures,
// returns due)); the surplus counted per run instead of per tick; a return
// admitted but not counted as one.
func TestM6TheCompensationLedgerKeepsEveryCountApart(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Churn = churnCompensated
	config.ChurnShare = 0.02
	config.ReturnShare = 0.8 // enough returns that some ticks have more due than departures
	config.ReturnAfter = 2
	config.ChurnAt = 0
	config.Ticks = 30
	config.JoinMaxWait = 3
	report := runM6Model(t, config)

	if report.ArrivalsOffered != report.ReturnsOffered+report.NewcomersOffered {
		t.Errorf("offered %d ≠ returns %d + newcomers %d", report.ArrivalsOffered, report.ReturnsOffered,
			report.NewcomersOffered)
	}
	if report.ArrivalsAdmitted != report.ReturnsAdmitted+report.NewcomersAdmitted {
		t.Errorf("admitted %d ≠ returns %d + newcomers %d", report.ArrivalsAdmitted,
			report.ReturnsAdmitted, report.NewcomersAdmitted)
	}
	if report.ArrivalsOffered != report.ArrivalsAdmitted+report.GaveUpJoining+report.PendingAtEnd {
		t.Errorf("offered %d ≠ admitted %d + gave up %d + still waiting %d", report.ArrivalsOffered,
			report.ArrivalsAdmitted, report.GaveUpJoining, report.PendingAtEnd)
	}

	// Recomputed from the scenario trace: per tick, offered = max(departed,
	// returns due) (less what the reserve could not offer), surplus = the
	// excess of the returns due over the departures.
	offered, surplus, ticksWithSurplus := 0, 0, 0
	for _, events := range report.Trace.Scenario {
		departed, due := len(events.Departed), len(events.ReturnsDue)
		offered += max(departed, due)
		surplus += max(due-departed, 0)
		if due > departed {
			ticksWithSurplus++
		}
	}
	if ticksWithSurplus == 0 {
		t.Fatal("no tick had more returns due than departures, so the surplus rule is not exercised")
	}
	if report.ArrivalsOffered+report.ArrivalsNotOffered != offered {
		t.Errorf("offered %d (+%d the reserve could not offer) ≠ Σ max(departed, returns due) = %d",
			report.ArrivalsOffered, report.ArrivalsNotOffered, offered)
	}
	if report.OfferedSurplus != surplus {
		t.Errorf("surplus %d ≠ Σ max(returns due − departed, 0) = %d", report.OfferedSurplus, surplus)
	}
	if report.ReturnsAdmitted == 0 {
		t.Fatal("no return was admitted, so the split of the admissions is not exercised")
	}
	t.Logf("offered %d = %d returns + %d newcomers; admitted %d = %d + %d; gave up %d, waiting %d; "+
		"surplus %d over %d ticks", report.ArrivalsOffered, report.ReturnsOffered,
		report.NewcomersOffered, report.ArrivalsAdmitted, report.ReturnsAdmitted,
		report.NewcomersAdmitted, report.GaveUpJoining, report.PendingAtEnd, report.OfferedSurplus,
		ticksWithSurplus)
}
