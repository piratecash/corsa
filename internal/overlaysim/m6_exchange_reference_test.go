package overlaysim

// m6_exchange_reference_test.go are the references for the interval of §5.1.0
// as the RESPONDER'S rule (decision 3.6(b), review package §4): the responder
// stamps what it serves, refuses inside T_exch, a refusal moves no stamp, the
// two directions are independent, and the ‘from scratch’ clearing keeps the
// clocks of both sides — and for the comparator, which reads the responder's
// limiter as world state.

import (
	"strings"
	"testing"
)

// exchangePair prepares an A′ fixture and picks an owner with a live
// neighbour; every OTHER neighbour of the owner is stamped "asked just now"
// before each request so that the request under test goes to `peer` alone.
type exchangePair struct {
	network     *m6Network
	owner, peer int32
}

func newExchangePair(t *testing.T, config m6ModelConfig) exchangePair {
	t.Helper()
	network := m6DirectFixture(t, config)
	for _, owner := range network.owners {
		for _, peer := range network.neighboursOf(owner) {
			if network.visible(owner, peer) && network.online[peer] && network.states[peer] != nil {
				return exchangePair{network: network, owner: owner, peer: peer}
			}
		}
	}
	t.Fatal("no owner with a live neighbour")
	return exchangePair{}
}

// ask has `from` request an exchange from `to` at the given tick and reports
// whether one was SERVED.
func (p exchangePair) ask(tick int, from, to int32) bool {
	n := p.network
	n.tick = tick
	state := n.states[from]
	for _, other := range n.neighboursOf(from) {
		if other != to {
			state.LastExchange[other] = tick
		}
	}
	served := n.report.ExchangesDone
	n.exchangeWithANeighbour(from, state)
	return n.report.ExchangesDone > served
}

// forgetAsking erases the asker's planning stamp for `to`, so the next request
// is SENT and the responder's rule alone decides — the way a node that lost
// its planning memory, or a hand-built asker, would behave.
func (p exchangePair) forgetAsking(from, to int32) {
	delete(p.network.states[from].LastExchange, to)
}

// TestM6TheExchangeIntervalIsTheRespondersRule is §5.1.0 at the responder: the
// six cases the decision names, on one pair of nodes.
//
// ⚠️ Mutations that must break it: judging the interval at the asker only
// (the forgotten stamp then gets a second exchange), stamping the responder
// on a refusal (the refused request pushes the next served one out), a
// symmetric stamp (v→u blocked by u→v).
func TestM6TheExchangeIntervalIsTheRespondersRule(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ExchangeEvery = 4
	pair := newExchangePair(t, config)
	n, u, v := pair.network, pair.owner, pair.peer

	// 1. The first exchange is served, and BOTH sides stamp it.
	if !pair.ask(10, u, v) {
		t.Fatal("the first request was not served")
	}
	if got := n.states[v].ServedExchange[u]; got != 10 {
		t.Fatalf("the responder's stamp for the asker is %d, want 10", got)
	}
	if got := n.states[u].LastExchange[v]; got != 10 {
		t.Fatalf("the asker's planning stamp is %d, want 10", got)
	}

	// 2. Forbidden before the boundary — at the RESPONDER: the asker's
	//    planning stamp is erased, the request is sent, and refused.
	pair.forgetAsking(u, v)
	refused := n.report.ExchangesRefused
	if pair.ask(12, u, v) {
		t.Fatal("a request 2 ticks after the last served exchange was served (T_exch = 4)")
	}
	if n.report.ExchangesRefused != refused+1 {
		t.Fatalf("the refusal was not counted: %d refused, want %d", n.report.ExchangesRefused, refused+1)
	}
	// 3. A refusal moves no stamp at the responder…
	if got := n.states[v].ServedExchange[u]; got != 10 {
		t.Fatalf("the refused request moved the responder's stamp to %d; it must stay at 10", got)
	}
	// …and the asker backs off from the request it made.
	if got := n.states[u].LastExchange[v]; got != 12 {
		t.Fatalf("the asker's planning stamp after the refused request is %d, want 12", got)
	}
	if pair.ask(13, u, v) {
		t.Fatal("the asker sent a request 1 tick after being refused; it plans by its own stamp")
	}

	// 4. Independent directions: v asks u right away and is served.
	if !pair.ask(11, v, u) {
		t.Fatal("v → u was blocked although only u → v had been served")
	}

	// 5. Allowed exactly at T_exch after the last SERVED exchange (tick 10 + 4),
	//    although a request was refused at 12: refusals do not extend it.
	pair.forgetAsking(u, v)
	if !pair.ask(14, u, v) {
		t.Fatal("a request exactly T_exch after the last served exchange was refused — the refusal at " +
			"tick 12 extended the interval")
	}
	if got := n.states[v].ServedExchange[u]; got != 14 {
		t.Fatalf("the responder's stamp after the second served exchange is %d, want 14", got)
	}

	// 6. The ‘from scratch’ clearing keeps the clocks of BOTH sides: u (the
	//    asker) and v (a responder) are both measured owners and both cleared.
	n.tick = 15
	if err := n.clearForTheFromScratchControl(); err != nil {
		t.Fatalf("clearing: %v", err)
	}
	if got := n.states[v].ServedExchange[u]; got != 14 {
		t.Fatalf("the clearing erased the responder's stamp (now %d)", got)
	}
	if got := n.states[u].LastExchange[v]; got != 14 {
		t.Fatalf("the clearing erased the asker's planning stamp (now %d)", got)
	}
	if pair.ask(16, u, v) {
		t.Fatal("the cleared asker exchanged again 2 ticks after its last exchange — the clearing " +
			"restarted its clock")
	}
	pair.forgetAsking(u, v)
	if pair.ask(16, u, v) {
		t.Fatal("the cleared responder served an asker 2 ticks after the last served exchange — the " +
			"clearing restarted the responder's clock")
	}
	// The asker backed off at 16; the responder's stamp is still 14, so at 18
	// a request (sent — planning stamp erased again) is served.
	pair.forgetAsking(u, v)
	if !pair.ask(18, u, v) {
		t.Fatal("T_exch after the last served exchange, the cleared pair still could not exchange")
	}

	t.Run("a refused request is a frame of the recovery window only inside it", func(t *testing.T) {
		t.Parallel()
		pair := newExchangePair(t, config)
		n, u, v := pair.network, pair.owner, pair.peer
		n.window = newM6RecoveryWindow(20, 4) // [20, 24)
		if !pair.ask(18, u, v) {
			t.Fatal("the first request was not served") // served, outside the window
		}
		pair.forgetAsking(u, v)
		if pair.ask(21, u, v) {
			t.Fatal("served 3 ticks after the last served exchange") // refused, inside
		}
		pair.forgetAsking(u, v)
		if !pair.ask(22, u, v) {
			t.Fatal("not served exactly T_exch after the last served exchange") // served, inside
		}
		pair.forgetAsking(u, v)
		if pair.ask(24, u, v) {
			t.Fatal("served 2 ticks after the last served exchange") // refused, outside
		}
		if n.window.ExchangesRefused != 1 || n.window.ExchangesServed != 1 {
			t.Fatalf("the window booked %d refused / %d served; want 1 / 1 — the exchange at tick 18 and the "+
				"refusal at tick 24 are outside [20, 24)", n.window.ExchangesRefused, n.window.ExchangesServed)
		}
	})

	t.Run("under the single-exchange control the responder never serves the same asker twice", func(t *testing.T) {
		t.Parallel()
		once := config
		once.ExchangeOnce = true
		pair := newExchangePair(t, once)
		if !pair.ask(3, pair.owner, pair.peer) {
			t.Fatal("the first request was not served")
		}
		pair.forgetAsking(pair.owner, pair.peer)
		if pair.ask(100, pair.owner, pair.peer) {
			t.Fatal("the responder served a second exchange to the same asker under the control")
		}
		if !pair.ask(4, pair.peer, pair.owner) {
			t.Fatal("the other direction was blocked by the first")
		}
	})
}

// TestM6TheComparatorReadsTheRespondersLimiterAsWorld: an exchange the main
// run made is NOT available in the control's world when the responder there
// had served that asker inside T_exch — the limiter is world state, like the
// edge the exchange goes over.
//
// ⚠️ Mutation that must break it: availableIn judging an exchange by the edge
// alone.
func TestM6TheComparatorReadsTheRespondersLimiterAsWorld(t *testing.T) {
	t.Parallel()

	config := m6ModelBase()
	config.Branch = branchAPrime
	config.ExchangeEvery = 4
	pair := newExchangePair(t, config)
	n, u, v := pair.network, pair.owner, pair.peer

	offer := &m6OfferEntry{Tick: 12, Owner: u, Source: offerExchange, Level: -1, Peer: v}
	n.tick = 12

	world := snapshotWorldOf(n)
	if got := availableIn(n, world, u, offer); got != availableInWorld {
		t.Fatalf("with no stamp at the responder the exchange is %s, want available", got)
	}

	n.states[v].ServedExchange[u] = 10
	world = snapshotWorldOf(n)
	if got := availableIn(n, world, u, offer); got != missingFromWorld {
		t.Fatalf("with the responder having served this asker 2 ticks ago (T_exch = 4) the exchange "+
			"is %s, want missing from the world", got)
	}

	n.states[v].ServedExchange[u] = 8
	world = snapshotWorldOf(n)
	if got := availableIn(n, world, u, offer); got != availableInWorld {
		t.Fatalf("with the responder's last served exchange exactly T_exch ago the exchange is %s, "+
			"want available", got)
	}

	// The snapshot is a COPY: a stamp written after it does not change the verdict.
	n.states[v].ServedExchange[u] = 11
	if got := availableIn(n, world, u, offer); got != availableInWorld {
		t.Fatalf("a stamp written after the snapshot changed the verdict to %s", got)
	}
}

// TestM6TheLocalRepeatFilterIsAControlThatPaysNoProbeAndDetectsNothing is
// decision 3.2(c) on one record: a neighbour hands back a record the owner
// already holds, and the record's node has died. Under the base of the grid
// (§5.1.0) the repeat is a paid probe and finds the death; under the ‘local
// repeat filter’ control the repeat is dropped at the owner — received and
// counted, not paid — and confirms nothing: no probe, no detection, the
// level's clock and the tried-set untouched, the queue empty.
//
// ⚠️ Mutations that must break it: filtering in the base (the base stops
// detecting); the filter marking the node tried or refreshing the clock (it
// would be confirming a record it never probed); the filter not counting.
func TestM6TheLocalRepeatFilterIsAControlThatPaysNoProbeAndDetectsNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		filter         bool
		probes         int
		detects        bool
		filtered       int
		queueLeft      int
		triedTheRepeat bool
	}{
		// The unreachable outcome is TEMPORARY for the queue (the node may come
		// back), so the record stays queued after the probe.
		{name: "base: the repeat is a paid probe and detects", probes: 1, detects: true, queueLeft: 1, triedTheRepeat: true},
		{name: "control: the repeat is filtered — no probe, no detection, counted", filter: true, filtered: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := m6ModelBase()
			config.Branch = branchAPrime
			config.Cadence = 0 // ∞: nothing but the repeat could re-probe the record
			config.LocalRepeatFilter = tc.filter
			network := m6DirectFixture(t, config)

			const owner = int32(0)
			state := network.states[owner]
			peer := network.neighboursOf(owner)[0]
			level := levelOf(network.ids[owner], network.ids[peer], config.Shape.degree)
			network.probe(owner, state, peer, level, false)
			if !state.Table.holds(peer) {
				t.Fatalf("the fixture could not store the record: %s", network.report.Probes.breakdown())
			}
			probesBefore := network.report.Probes.Probes()
			clockBefore := state.LastRefreshed[level]

			// The record's node dies, and a neighbour hands the record back.
			network.online[peer] = false
			network.departedAt[peer] = 0
			network.tick = 1
			clear(state.TriedThisTick)
			state.Offered = append(state.Offered, peer)
			for _, other := range network.neighboursOf(owner) {
				if levelOf(network.ids[owner], network.ids[other], config.Shape.degree) == level {
					state.TriedThisTick[other] = struct{}{}
				}
			}
			delete(state.TriedThisTick, peer)
			// No exchange this tick: every neighbour was asked just now.
			for _, other := range network.neighboursOf(owner) {
				state.LastExchange[other] = network.tick
			}

			candidate, _, ok := network.fromBranch(owner, state, level)
			if tc.filter && ok && candidate == peer {
				t.Fatalf("the control offered the held record %d for a probe", peer)
			}
			if !tc.filter && (!ok || candidate != peer) {
				t.Fatalf("the base offered %v (ok=%v), want the repeat %d", candidate, ok, peer)
			}
			if ok {
				network.probe(owner, state, candidate, level, false)
			}

			_, detected := state.Released[peer]
			_, tried := state.TriedThisTick[peer]
			got := struct {
				probes         int
				detects        bool
				filtered       int
				queueLeft      int
				triedTheRepeat bool
			}{
				probes: network.report.Probes.Probes() - probesBefore, detects: detected,
				filtered: network.report.RepeatsFiltered, queueLeft: len(state.Offered), triedTheRepeat: tried,
			}
			want := struct {
				probes         int
				detects        bool
				filtered       int
				queueLeft      int
				triedTheRepeat bool
			}{tc.probes, tc.detects, tc.filtered, tc.queueLeft, tc.triedTheRepeat}
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
			if state.LastRefreshed[level] != clockBefore {
				t.Fatalf("the level's cadence clock moved from %d to %d — a repeat confirms nothing",
					clockBefore, state.LastRefreshed[level])
			}
			if tc.filter && sumOf(network.report.LostByLevel) != 0 {
				t.Fatalf("the control counted %d losses without a probe", sumOf(network.report.LostByLevel))
			}
		})
	}

	t.Run("the control is refused outside adaptive A′", func(t *testing.T) {
		t.Parallel()
		g := buildGraph(m6ModelShape(), 1, 1, policyInitiatedLimit)
		for _, branch := range []m6Branch{branchA, branchB, branchC} {
			config := m6ModelBase()
			config.Branch = branch
			config.LocalRepeatFilter = true
			if _, err := newM6Network(g, config, everybody); err == nil {
				t.Errorf("branch %s accepted the local repeat filter", branch)
			}
		}
	})
}

// TestM6TheReportKnowsTheRepeatFilterUnderCInfinity is the owner's P2 (round
// 30): with C = ∞ and no detected loss the report said, for every A′/C run,
// that a repeat handed back "is a paid probe and COULD have detected" — but
// the ‘local repeat filter’ control drops a held record BEFORE a probe, so
// there the repeat confirms nothing, pays nothing and detects nothing. Three
// readings, three wordings: A/B (no path re-probes a held record), A′/C base
// (the repeat is a paid probe), A′ with the filter (the repeat is dropped;
// probes of OTHER candidates still detect — the report must not claim that
// nothing is ever detected).
//
// ⚠️ Mutation that must break it: the filter reading collapsing into the base
// reading in any one of the three places — the detection line, the recovery
// axis, the configuration signature.
func TestM6TheReportKnowsTheRepeatFilterUnderCInfinity(t *testing.T) {
	t.Parallel()

	const paid = "paid probe and COULD have detected"
	cases := []struct {
		name       string
		branch     m6Branch
		filter     bool
		mustHave   []string
		mustNot    []string
		configSays []string
	}{
		{
			name: "A′ base: the repeat is a paid probe", branch: branchAPrime,
			mustHave:   []string{paid, "MEASUREMENT"},
			mustNot:    []string{"dropped at the owner"},
			configSays: []string{"repeat handed back by a neighbour is a paid probe"},
		},
		{
			name: "C base: the repeat is a paid probe", branch: branchC,
			mustHave:   []string{paid, "MEASUREMENT"},
			mustNot:    []string{"dropped at the owner"},
			configSays: []string{"repeat handed back by a neighbour is a paid probe"},
		},
		{
			name: "A′ with the local repeat filter: the repeat is dropped before a probe", branch: branchAPrime, filter: true,
			mustHave: []string{"dropped at the owner before a probe", "not paid", "cannot detect",
				"confirms nothing", "any OTHER candidate"},
			mustNot:    []string{paid},
			configSays: []string{"‘local repeat filter’ control a repeat handed back by a neighbour is dropped", "dropped at the owner before any probe"},
		},
		{
			name: "A: no path re-probes a held record", branch: branchA,
			mustHave: []string{"by no other path"},
			mustNot:  []string{paid, "dropped at the owner"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := m6ModelBase()
			config.Branch = tc.branch
			config.Cadence = 0
			config.LocalRepeatFilter = tc.filter
			// A report with NO detected loss: the wording under test is the one a
			// reader gets when the axis is empty.
			report := m6ModelReport{
				Config:      config,
				LostByLevel: make([]int, config.Shape.degree), RefilledByLevel: make([]int, config.Shape.degree),
			}
			lines := map[string]string{
				"detection line": report.DetectionDelaySummary(),
				"recovery axis":  report.recoveryAxisLine(),
			}
			for name, line := range lines {
				for _, want := range tc.mustHave {
					if !strings.Contains(line, want) {
						t.Errorf("the %s does not say %q:\n%s", name, want, line)
					}
				}
				for _, banned := range tc.mustNot {
					if strings.Contains(line, banned) {
						t.Errorf("the %s says %q, which is false for this configuration:\n%s", name, banned, line)
					}
				}
			}
			signature := config.String()
			for _, want := range tc.configSays {
				if !strings.Contains(signature, want) {
					t.Errorf("the configuration signature does not say %q:\n%s", want, signature)
				}
			}
			if tc.filter && strings.Contains(signature, "in A′ and C a repeat handed back by a neighbour is a paid probe") {
				t.Errorf("the configuration signature keeps the base reading under the filter:\n%s", signature)
			}
		})
	}

	t.Run("the full report of A′ with the filter at C = ∞ carries the filter reading end to end", func(t *testing.T) {
		t.Parallel()
		config := m6ModelBase()
		config.Branch = branchAPrime
		config.Cadence = 0
		config.LocalRepeatFilter = true
		report := runM6Model(t, config)
		text := report.String()
		if strings.Contains(text, paid) {
			t.Errorf("the full report says a repeat %q under the filter:\n%s", paid, text)
		}
		for _, want := range []string{"repeats of held records filtered", "CONTROL ‘local repeat filter’"} {
			if !strings.Contains(text, want) {
				t.Errorf("the full report does not say %q", want)
			}
		}
		t.Logf("detected %d losses with the filter on; detection line: %s", len(report.DetectionDelays),
			report.DetectionDelaySummary())
	})
}
