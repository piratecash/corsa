package overlaysim

// m6_engine_test.go is the tick loop behind m6_model_test.go: who is offered to
// whom, what a probe changes, when a loss is DETECTED, and what churn does to
// the graph.
//
// The rules it implements, each from a named place in the contract:
//
//	§5.1   four branches, nested, differing only in what a node may be offered;
//	§5.1.0 A′ exchange: m records, once per neighbour per T_exch, chosen as the
//	       ones closest to the asker, with no exclusion list;
//	§5.1.2 the addressed request of branch C, answered from the RESPONDER'S OWN
//	       TABLE, under the single rate-limit definition (r_pair and r_node);
//	§5.4   three separate moments — departure, detection, shelf expiry — with B
//	       freed at DETECTION and never at departure;
//	§5.7   the cadence in ticks, and C = ∞ as the negative control;
//	§5.9.2 the churn forms, the entry queue, and what a return does and does not
//	       bring back;
//	§5.9.3 the service order: levels short after churn first, shelf before or
//	       after new candidates, then the source's own order.

import (
	"fmt"
	"sort"
)

// m6StandAssumptions lists what the model DECIDED where the contract left a gap.
// Every one of them changes the numbers, so they travel with the report rather
// than living in a commit message.
const m6StandAssumptions = "⚠️ STAND ASSUMPTIONS, not contract — every one of them moves the " +
	"numbers: (1) newcomers come from a reserve of identifiers drawn BEYOND the built population, " +
	"from the same generator, with the role mix preserved by selection and every identifier the " +
	"graph already holds excluded, and the ROLE OF EACH POSITION is paced by the population's " +
	"share so that every prefix — and only a prefix is ever offered — carries the mix, the same " +
	"order for a long run and a short one; a newcomer has no edges and gets exactly one " +
	"established starting link; (2) a refresh probe is served BEFORE a filling probe out of the same ceiling " +
	"R — ⚠️ WITH A PRICE: when the cadence is short relative to d/R, refresh takes the whole " +
	"ceiling and the node never fills anything (measured at C=2, R=4, d=8), so C and R have to be " +
	"chosen together; (3) among equally deficient levels the NEARER one is served first, and a " +
	"level with no candidate does not stop the node — the next in the order is tried in the same " +
	"tick; (4) a candidate is written off PERMANENTLY only when no bucket can ever hold it; every " +
	"other refusal is temporary, retried on a later tick and never twice within one, whichever " +
	"source offered it; (5) a record handed over by an exchange leaves the queue only on a FINAL " +
	"outcome, and a repeat the neighbour hands back COSTS a probe (§5.1.0 sends no exclusion " +
	"list); (6) a refresh rotates through the members of a level; (7) a RETURN needs no new " +
	"starting link, and a re-connect clears BOTH the slot and the record of the detected loss at " +
	"BOTH ends; (8) an addressed request goes to ONE responder, de-duplicated across neighbours " +
	"and table, answered with the records on the level the ASKER named, closest first, ties by " +
	"index; (9) a probe needs a free slot at EACH END THAT WOULD HAVE TO OPEN ONE, asked " +
	"separately; (10) EVERY node of the physical network runs the mechanism, measured or not, and " +
	"the membership decides only whose table is reported and which candidates a measured node may " +
	"take; (11) a background departure is DECIDED per (tick, node) and is conditional on the node " +
	"being online, and the RETURN of that departure is drawn from the SAME (tick, node) — the " +
	"decisions are a function of the seed, the realised departures are not, so no comparison can " +
	"change another comparison's load; (12) a return is a PROMISE made when the node left and is " +
	"honoured even when the compensation quota is smaller, so the arrivals offered are " +
	"max(departures, returns due) and returns and newcomers are counted apart; (13) a level's " +
	"cadence clock starts when the level first holds a record, so the first refresh falls due C " +
	"ticks later rather than in the tick of the fill; (14) C = ∞ switches off the SCHEDULED " +
	"refresh, not detection: in A and B a held record is then never re-probed, while in A′ and C " +
	"a repeat handed back by a neighbour is a paid probe and may still find it dead; " +
	"(15) a probe that finds nobody is a LOSS only where something was held, and WHAT was held " +
	"decides what comes back: no edge and no record is an unreachable STRANGER, counted as " +
	"itself; a record without an edge is shelved and restored as a RECORD, never as a link; only " +
	"a loss that freed an established edge may re-establish one, and ANY restoration lifts the " +
	"burial; (16) every per-(tick, node) coin is keyed on the tick and the IDENTITY, and a " +
	"every scan of the network — a newcomer's host, the omniscient control's candidates — walks the " +
	"JOINED population and never an index space padded with the unjoined reserve, so neither the " +
	"event stream, nor the early topology, nor the order of offers depends on how long the run " +
	"is; (17) the omniscient CONTROL offers from the " +
	"physical population under the same mayTake as every branch: it changes what an owner knows, " +
	"never who the unmeasured background may connect to; (18) a repeatable exchange " +
	"needs T_exch ≥ 1: at zero the same neighbour is eligible again within the tick and the " +
	"candidate search does not terminate, so the interval is refused at construction — R bounds " +
	"probes, not the search that asks for them; (19) EVERY source reports what it " +
	"handed over, the omniscient control included, so S(u) measures the mechanism under " +
	"measurement; what a source COULD have offered is the potential pool and is printed apart — " +
	"for the control that is the whole joined population it may take; (20) a departure is DRAWN over every " +
	"identifier the run carries, because sampling the joined or the online would make the draw a " +
	"function of the mechanism — but it is REPORTED by population: the load figure is the " +
	"original population's, and the reserve's draws are their own number with the reserve as " +
	"their denominator; (21) a SHOCK decides too — " +
	"its decision is the selection itself, so decisions and realised departures coincide there by " +
	"construction; (22) the phase machine of §5.9.1 " +
	"(T_fill / T_idle / T_rec / T_cad and the early-stop rules) is NOT implemented: the run is a " +
	"flat tick count with one churn moment, and the phases remain work after decision 4б"

// newM6Network prepares the scenario. It does not run it.
func newM6Network(g *graph, config m6ModelConfig, member func(nodeID) bool) (*m6Network, error) {
	if config.Capacity < 1 {
		return nil, fmt.Errorf("bucket capacity %d: a table with no slots measures nothing",
			config.Capacity)
	}
	if config.Ticks < 1 {
		return nil, fmt.Errorf("a run of %d ticks measures nothing", config.Ticks)
	}
	if config.NearFrom < 0 || config.NearFrom > config.Shape.degree {
		return nil, fmt.Errorf("near levels start at %d, outside 0…%d",
			config.NearFrom, config.Shape.degree)
	}
	// ⚠️ T_exch = 0 DOES NOT TERMINATE, and it is the interval that has to say
	// so, because nothing downstream can. A repeatable exchange with a live
	// neighbour is an event even when the neighbour hands over nothing, so
	// exchangeWithANeighbour reports success and fromBranch searches again; with
	// no interval the same neighbour is eligible again in the same tick, and the
	// search recurses forever. The ceiling R does not bound it — R counts probes,
	// and control never returns to the loop that spends them. The contract's
	// T_exch = 64 never lands here, which is exactly why the guard belongs at the
	// door rather than in the reading of any one run.
	if config.Branch.ExchangesRecords() && !config.ExchangeOnce && config.ExchangeEvery < 1 {
		return nil, fmt.Errorf(
			"branch %s repeats exchanges every %d ticks: an interval below one tick lets the same "+
				"neighbour be asked again within the tick, and the candidate search never returns",
			config.Branch, config.ExchangeEvery)
	}

	// The reserve is drawn BEYOND the built population (§5.9.2: newcomers come
	// from a reserve of identifiers, not from a quarter of the network held
	// back), with the same generator and the role mix of the graph preserved by
	// SELECTION — the same rule M3-a uses to build a population of a given share.
	reserveIDs, reserveRoles := drawM6Reserve(g, config)

	nodes := len(g.ids) + len(reserveIDs)
	network := &m6Network{
		g:             g,
		config:        config,
		member:        member,
		ids:           append(append([]nodeID(nil), g.ids...), reserveIDs...),
		roles:         append(append([]int(nil), g.roles...), reserveRoles...),
		states:        map[int32]*m6NodeState{},
		online:        make([]bool, nodes),
		joined:        make([]bool, nodes),
		held:          make([]map[int32]struct{}, nodes),
		returning:     map[int][]int32{},
		answersByNode: map[int32]int{},
		answersByPair: map[[3]int32]int{},
		clearedAt:     -1,
		report: &m6ModelReport{
			Config:           config,
			Probes:           newProbeLedger(),
			ProbesAfterChurn: newProbeLedger(),
			PhysicalProbes:   newProbeLedger(),
			LostByLevel:      make([]int, config.Shape.degree),
			RefilledByLevel:  make([]int, config.Shape.degree),
		},
	}
	for node := range network.held {
		network.held[node] = map[int32]struct{}{}
	}

	for node := range g.ids {
		network.all = append(network.all, int32(node))
		if member(g.ids[node]) {
			network.owners = append(network.owners, int32(node))
		}
	}
	if len(network.owners) == 0 {
		return nil, fmt.Errorf("the membership %q holds no node", config.Membership)
	}
	for index := range reserveIDs {
		node := int32(len(g.ids) + index)
		network.all = append(network.all, node)
		network.reserve = append(network.reserve, node)
		if member(reserveIDs[index]) {
			network.owners = append(network.owners, node)
		}
	}

	// Everybody in the built graph is online and connected from the start; the
	// reserve is neither until it joins.
	//
	// ⚠️ joinedOrder is kept BESIDE the flag, and it is what every scan of "the
	// network" walks. Scanning n.all and skipping the unjoined means indexing a
	// space that carries the whole reserve, and the reserve is sized for the
	// run's LENGTH: a starting offset landing in that tail wraps to the FIRST
	// nodes of the population, so the order of offers — like the choice of a
	// newcomer's host before it — depended on how long the run was going to be.
	for node := range g.ids {
		network.joinedOrder = append(network.joinedOrder, int32(node))
		network.joined[node] = true
		network.online[node] = true
		for _, peer := range g.adjacency[node] {
			network.held[node][peer] = struct{}{}
		}
	}

	// ⚠️ A STATE FOR EVERY node of the physical network, not only the measured
	// ones. A node outside the membership still runs the mechanism: it keeps its
	// own table, detects its own losses and frees its own budget. Freezing the
	// unmeasured half instead would have changed what hosts have room for a
	// newcomer, so the Q-half run and the full-graph run would differ in the
	// PHYSICAL network as well as in what is measured.
	//
	// What the membership does change is WHOSE candidates are restricted: a
	// measured node may only take members (that is the half being measured), and
	// an unmeasured node goes on as it would have without the measurement.
	for _, node := range network.all {
		if !network.joined[node] {
			continue
		}
		network.states[node] = newM6NodeState(node,
			config.Shape.degree, config.Capacity, config.NearFrom)
	}

	// ⚠️ The ORIGINAL population only. The reserve has not joined anything yet,
	// and counting it would make the denominator depend on the requested length
	// of the run — the reserve is sized for the ticks.
	for _, node := range network.owners {
		if int(node) < len(g.ids) {
			network.report.Members++
			continue
		}
		network.report.ReserveMeasured++
	}
	network.report.ReserveSize = len(network.reserve)

	// Nobody has left yet. ⚠️ Allocated HERE rather than at the start of Run:
	// it is state of the network, and a fixture that drives probe() directly —
	// the only way to reach some of the invariants — must find it there.
	network.departedAt = make([]int, nodes)
	for node := range network.departedAt {
		network.departedAt[node] = -1
	}

	// ⚠️ S(u) under the branch-A definition, taken HERE — on the built graph,
	// before a tick has run and before any churn. That is what makes it a
	// property of the network rather than of the run, and §5.1.1 requires the
	// border of the near levels to come from exactly such a quantity, fixed once
	// and applied to every branch and both populations.
	for _, owner := range network.owners {
		if !network.joined[owner] {
			continue
		}
		network.report.PoolAtStart = append(network.report.PoolAtStart,
			len(network.neighboursOf(owner)))
	}
	return network, nil
}

// drawM6Reserve produces the newcomers: identifiers from the model's own
// generator, continuing past the population, SELECTED so the share of Q in the
// reserve matches the share in the graph.
//
// ⚠️ Selected, never relabelled — the same rule as M3-a §5.5.5, and for the same
// reason: a reserve built by assigning roles would measure a labelling rather
// than the classifier, and compensation would quietly shift the role mix of the
// population it is supposed to hold steady.
func drawM6Reserve(g *graph, config m6ModelConfig) ([]nodeID, []int) {
	if config.Churn != churnCompensated {
		return nil, nil
	}

	// Enough for the whole run: every tick may take f_bg of the online
	// population, and a reserve that runs out turns the scenario into a
	// different one (see ArrivalsNotOffered).
	wanted := int(float64(len(g.ids))*config.ChurnShare*float64(config.Ticks) + 0.5)
	if wanted <= 0 {
		return nil, nil
	}

	structural := 0
	for _, role := range g.roles {
		if role == roleStructural {
			structural++
		}
	}

	// ⚠️ EVERY identifier the graph already holds is excluded EXPLICITLY, and
	// continuing the index sequence is not enough on its own. A population can be
	// built by REJECTION — M3-a draws a skewed share that way — and then the
	// generator was walked far past N to fill it, so indices above N are already
	// in the graph. Without this set the reserve would re-introduce an existing
	// NodeID as a separate node, and two nodes would share one identity.
	taken := make(map[nodeID]struct{}, len(g.ids))
	for _, id := range g.ids {
		taken[id] = struct{}{}
	}

	// ⚠️ THE ORDER OF THE RESERVE IS PART OF ITS COMPOSITION, and quota-filling
	// gets the order wrong even when the totals are right. Accepting identifiers
	// in generator order until each quota is full starts close to the natural mix
	// and then, once the minority quota is exhausted, leaves a TAIL of nothing but
	// the majority. The reserve as a whole matched the graph; the PREFIX actually
	// offered — and on a short or lightly-churned run only a prefix is ever
	// offered — did not. On a skewed population that is the same bias M3-a §5.5.5
	// was written to remove.
	//
	// So the role of each POSITION is decided first, by an even spacing of the
	// graph's share (position p takes a structural node when the running quota
	// crosses an integer), and the identifier is then drawn from the generator's
	// own order within that role. The spacing uses the SHARE and not the total, so
	// position p gets the same role and the same identifier however long the run
	// is: every prefix carries the graph's mix, and a longer reserve extends this
	// one rather than reshuffling it.
	share := func(positions int) int {
		return positions * structural / len(g.roles)
	}
	byRole := map[int][]nodeID{}
	index := len(g.ids)
	nextOfRole := func(want int) nodeID {
		for len(byRole[want]) == 0 {
			candidate := makeNodeID(config.Seed, index)
			index++
			if _, already := taken[candidate]; already {
				continue
			}
			taken[candidate] = struct{}{}
			byRole[roleOf(candidate)] = append(byRole[roleOf(candidate)], candidate)
		}
		drawn := byRole[want][0]
		byRole[want] = byRole[want][1:]
		return drawn
	}

	ids := make([]nodeID, 0, wanted)
	roles := make([]int, 0, wanted)
	for position := 0; position < wanted; position++ {
		role := roleOther
		if share(position+1) > share(position) {
			role = roleStructural
		}
		ids = append(ids, nextOfRole(role))
		roles = append(roles, role)
	}
	return ids, roles
}

// referencePool is §5.1.1's S_ref together with the border derived from it.
//
// ⚠️ It needs a GRAPH and not a run: branch A offers a node its own edges, so the
// pool is known the moment the graph exists. That is why the run registry counts
// the reference pool as zero scenario runs — measuring it costs the graph build
// that every other run needs anyway, and not a pass of the scenario.
func referencePool(g *graph, config m6ModelConfig, member func(nodeID) bool) (int, string, error) {
	reference := config
	reference.Branch = branchA
	reference.Churn = churnNone
	reference.StartEmpty = false

	network, err := newM6Network(g, reference, member)
	if err != nil {
		return 0, "", err
	}
	border, rule := derivedNearFrom(network.report.PoolAtStart, config.Shape.degree)
	return border, rule, nil
}

// visible says whether `peer` is in `owner`'s branch-A pool: a node it is
// actually connected to, and one the measurement allows it to take.
//
// ⚠️ The MEMBERSHIP filter applies to a MEASURED owner and not to the rest of the
// network. The Q half is a question about the structural nodes — "can they fill
// their tables out of each other" — and restricting the unmeasured half as well
// would stop it behaving as it does in the full-graph run, which is the one thing
// the comparison needs held still.
func (n *m6Network) visible(owner, peer int32) bool {
	if peer == owner {
		return false
	}
	if _, connected := n.held[owner][peer]; !connected {
		return false
	}
	return n.mayTake(owner, peer)
}

// mayTake is the measurement filter: a measured node may only take candidates
// from the membership under measurement.
func (n *m6Network) mayTake(owner, candidate int32) bool {
	if !n.member(n.ids[owner]) {
		return true
	}
	return n.member(n.ids[candidate])
}

// heldEdges is how many connections a node believes it holds — derived from the
// one state rather than counted beside it.
func (n *m6Network) heldEdges(node int32) int { return len(n.held[node]) }

// holdsEdge answers the SAME question from the SAME state, which is the whole
// point of having one: a probe to a peer the node is connected to opens nothing,
// and detecting that peer's departure frees exactly what it held.
func (n *m6Network) holdsEdge(owner, peer int32) bool {
	_, connected := n.held[owner][peer]
	return connected
}

// Run plays the scenario and returns the report.
func (n *m6Network) Run() (*m6ModelReport, error) {
	for n.tick = 0; n.tick < n.config.Ticks; n.tick++ {
		n.answersByNode = map[int32]int{}
		n.answersByPair = map[[3]int32]int{}

		if err := n.applyChurn(); err != nil {
			return nil, err
		}
		n.admitFromQueue()

		// ⚠️ EVERY node of the physical network that is online, not only the
		// measured ones: an unmeasured node still detects its own losses and
		// frees its own budget, and a frozen half would change what room a host
		// has for a newcomer — a physical difference on top of the measured one.
		for _, node := range n.all {
			if !n.online[node] || n.states[node] == nil {
				continue
			}
			n.serve(node)
		}

		fromPopulation, fromReserve := n.countOnline()
		n.report.OnlineByTick = append(n.report.OnlineByTick, fromPopulation+fromReserve)
		n.report.OnlineFromPopulation = fromPopulation
		n.report.OnlineFromReserve = fromReserve
	}

	n.collect()
	return n.report, nil
}

// countOnline splits the measured population from the measured reserve that has
// actually joined. ⚠️ Two numbers, because one denominator cannot serve both: the
// reserve is sized for the length of the run, so folding it into "all
// participants" makes the share of the network that is online depend on how long
// the run was asked to be.
func (n *m6Network) countOnline() (fromPopulation, fromReserve int) {
	for _, owner := range n.owners {
		if !n.online[owner] {
			continue
		}
		if int(owner) < len(n.g.ids) {
			fromPopulation++
			continue
		}
		fromReserve++
	}
	return fromPopulation, fromReserve
}

// --- churn -------------------------------------------------------------------------

func (n *m6Network) applyChurn() error {
	// Returns due this tick are offered first and fill the compensation quota
	// (§5.9.2: "сперва возвраты, чей срок наступил, остаток — новые узлы").
	//
	// ⚠️ A RETURN IS A PROMISE MADE AT THE DEPARTURE, and it is honoured even
	// when it does not fit the quota: a tick can have no departures and a return
	// that came due, and cancelling it would mean a node that said it was coming
	// back never does. So the offered total is max(departures, returns due), not
	// the departures — and because that is NOT the exact compensation §5.9.2
	// describes, the two are counted apart and printed apart rather than summed
	// into a single "offered" that would read as exact.
	returned := 0
	for _, node := range n.returning[n.tick] {
		n.queue = append(n.queue, m6Pending{Node: node, Since: n.tick, Returning: true})
		n.report.ArrivalsOffered++
		n.report.ReturnsOffered++
		returned++
	}

	switch n.config.Churn {
	case churnNone:
		return nil

	case churnShock:
		if n.tick != n.config.ChurnAt {
			return nil
		}
		_, err := n.departShare(n.config.ChurnShare, "shock")
		return err

	case churnCompensated, churnShrink:
		if n.tick < n.config.ChurnAt {
			return nil
		}
		left, err := n.departShare(n.config.ChurnShare, "background")
		if err != nil {
			return err
		}
		if n.config.Churn == churnShrink {
			return nil
		}
		// ⚠️ As many arrivals are OFFERED as departed — the returns above count
		// towards that number, and the REMAINDER comes from the reserve. Whether
		// any of them gets in is a separate number, which is why the report
		// shows offered and admitted apart.
		for range max(left-returned, 0) {
			if n.reserveAt >= len(n.reserve) {
				// ⚠️ Counted PER ARRIVAL, not once per tick: a tick in which ten
				// nodes left and none could be offered is a different finding
				// from one where a single arrival was missing, and a per-tick
				// counter reports the two identically. A non-zero total means the
				// scenario was under-provisioned and did not play the churn it
				// was asked to play — the report says so in those words.
				n.report.ArrivalsNotOffered++
				continue
			}
			node := n.reserve[n.reserveAt]
			n.reserveAt++
			n.queue = append(n.queue, m6Pending{Node: node, Since: n.tick})
			n.report.ArrivalsOffered++
			n.report.NewcomersOffered++
		}
		return nil
	}
	return nil
}

// noteDepartureDecision books one decision to leave, by POPULATION.
//
// ⚠️ The DRAW itself is over every identifier the run carries — that is what
// keeps it exogenous, and sampling the joined or the online would be exactly the
// dependence on the mechanism that П-4 needs gone. What is split is the
// REPORTING: a decision drawn over a reserve identifier is not load on the
// network, and the reserve is sized for config.Ticks, so one counter for both
// made the reported load a function of the run's length.
func (n *m6Network) noteDepartureDecision(node int32) {
	if int(node) < len(n.g.ids) {
		n.report.DeparturesDecided++
		return
	}
	n.report.DeparturesDecidedInReserve++
}

// departShare applies this tick's departures.
//
// ⚠️ THE DECISION IS EXOGENOUS, THE DEPARTURE IS CONDITIONAL, and the difference
// is what makes two runs comparable at all. A background departure is drawn PER
// NODE from (tick, node) — not by shuffling whoever happens to be online — so
// the sequence of DECISIONS is a function of the seed and nothing else. Whether a
// decision takes effect depends on the node being online, which is a fact about
// the run.
//
// Why it had to change: departures used to be sampled from the current online
// list, so as soon as two runs differed in one admission their online lists
// differed, the next draw sampled from different populations, and everything
// after that diverged. The two runs were then compared across different churn —
// and "the same seed gives the same trace" was not true of them.
//
// ⚠️ A SHOCK is exact rather than per-node: §5.9.2 says a share f leaves at one
// tick, and at that single moment the exact count is the point. It is safe
// because a shock run has no arrivals before it, so both runs reach the shock
// with identical online sets.
func (n *m6Network) departShare(share float64, purpose string) (int, error) {
	leaving := make([]int32, 0, len(n.all))

	if purpose == "shock" {
		live := make([]int32, 0, len(n.all))
		for _, node := range n.all {
			if n.online[node] {
				live = append(live, node)
			}
		}
		count := int(float64(len(live))*share + 0.5)
		if count > len(live) {
			count = len(live)
		}
		order := make([]int32, len(live))
		copy(order, live)
		for i := len(order) - 1; i > 0; i-- {
			j := int(m6Random(n.config.Seed, purpose+"/leave", n.tick*1000+i) % uint64(i+1))
			order[i], order[j] = order[j], order[i]
		}
		leaving = append(leaving, order[:count]...)
		// ⚠️ A SHOCK DECIDES TOO, and the decision is the selection itself. The
		// count is exact by construction and everybody chosen is online, so here
		// decisions and realised departures coincide — but leaving the counter at
		// zero made the report say "0 departures decided … 200 actually left",
		// which reads as a broken model rather than as a different churn form.
		for _, node := range leaving {
			n.noteDepartureDecision(node)
		}
	} else {
		// One Bernoulli draw per node per tick, keyed by (tick, node). The set of
		// DECISIONS is identical in every run of this seed; only their effect
		// depends on who is online.
		for _, node := range n.all {
			draw := m6RandomEvent(n.config.Seed, purpose+"/leave", n.tick, n.ids[node]) % 1_000_000
			if float64(draw)/1_000_000 >= share {
				continue
			}
			n.noteDepartureDecision(node)
			if !n.online[node] || !n.joined[node] {
				continue
			}
			leaving = append(leaving, node)
		}
	}

	for _, node := range leaving {
		n.online[node] = false
		n.departedAt[node] = n.tick

		// ⚠️ The return is drawn for THIS DEPARTURE EVENT, keyed on (tick, node) —
		// the same key the departure decision uses, and for the same reason.
		//
		// Keying it on the node alone made the coin permanent: one node always
		// came back and another never did, so repeated churn steadily enriched
		// the survivors with returners and `ret` stopped describing the share of
		// DEPARTURES that return. Keying it on the COUNT of realised departures
		// was no better: that counter skips events that did not happen, so once
		// two runs differed in a single admission, the same departure of the same
		// node drew a different coin in each — the randomness became a function
		// of the mechanism being compared. The key is the tick and the IDENTITY,
		// never an offset into an array whose length the configuration decides
		// (see m6RandomEvent).
		if n.config.ReturnShare > 0 {
			draw := m6RandomEvent(n.config.Seed, purpose+"/return", n.tick, n.ids[node]) % 1000
			if float64(draw)/1000 < n.config.ReturnShare {
				back := n.tick + n.config.ReturnAfter
				n.returning[back] = append(n.returning[back], node)
			}
		}
		n.report.Departed++

		// ⚠️ The graph is NOT touched. The node's edges stay where they are and
		// every survivor still believes it has them, until a probe says
		// otherwise. That belief is the thing the two coverage numbers measure.
	}

	count := len(leaving)
	if !n.churnSeen && count > 0 {
		// ⚠️ Taken right AFTER the departures land and before anybody has
		// probed: that is the moment the two coverage numbers are furthest
		// apart, and the gap between them is exactly what the network still
		// believes and no longer has.
		n.report.LevelsAtChurn = n.coverageByLevel()
		for _, level := range n.report.LevelsAtChurn {
			n.report.ClaimedBefore += level.Claimed
			n.report.ActualBefore += level.Actual
		}
		n.churnSeen = true
		if n.config.StartEmpty {
			if err := n.clearForTheFromScratchControl(); err != nil {
				return 0, err
			}
		}
	}
	return count, nil
}

// admitFromQueue tries to give every waiting node its ONE starting link.
//
// ⚠️ A starting link is a named assumption of the stand (§5.9.2): with no link
// at all a newcomer is invisible to branches A and B forever and the compensated
// mode would degenerate. It occupies a slot at BOTH ends, and if nobody has room
// the node waits — and after T_join_max it gives up, which is a counted outcome
// and not a silent drop.
func (n *m6Network) admitFromQueue() {
	remaining := n.queue[:0]
	for _, pending := range n.queue {
		if n.admit(pending) {
			continue
		}
		if n.tick-pending.Since >= n.config.JoinMaxWait {
			n.report.GaveUpJoining++
			continue
		}
		remaining = append(remaining, pending)
	}
	n.queue = remaining
}

func (n *m6Network) admit(pending m6Pending) bool {
	if pending.Returning {
		// ⚠️ A RETURN NEEDS NO STARTING LINK, and this is a decision of the
		// stand, recorded in m6StandAssumptions. §5.9.2 gives the starting link
		// to a NEWCOMER, which "arrives with an empty table and no edges"; a
		// returning node has edges — the ones survivors have not released.
		// Requiring it to find a host anyway made the return fail whenever the
		// returning node's own heldEdges sat at B, which is almost always, since
		// its counter never fell when it left. The recovery axis would then have
		// been measuring that artefact.
		n.online[pending.Node] = true
		if n.states[pending.Node] != nil && n.states[pending.Node].Table.Coverage.filled() > 0 {
			n.report.ReturnedWithATable++
		} else {
			n.report.ReturnedEmptyHanded++
		}
		n.report.ArrivalsAdmitted++
		return true
	}

	// ⚠️ THE HOST IS DRAWN FROM THE POPULATION THAT EXISTS, and the key is the
	// newcomer's IDENTITY. Scanning n.all from an offset taken modulo its length
	// meant scanning the whole unjoined reserve too: the reserve is sized for the
	// run's LENGTH, so a longer horizon moved the starting offset and, whenever it
	// landed in the unjoined tail, the linear scan ran off the end and wrapped to
	// the FIRST nodes of the population. A large reserve therefore aimed a large
	// share of newcomers at the same few hosts, and the early topology of a run
	// depended on how long the run was going to be.
	if len(n.joinedOrder) == 0 {
		return false
	}

	host := int32(-1)
	start := int(m6RandomEvent(n.config.Seed, "join", n.tick, n.ids[pending.Node]) %
		uint64(len(n.joinedOrder)))
	for offset := range n.joinedOrder {
		candidate := n.joinedOrder[(start+offset)%len(n.joinedOrder)]
		if candidate == pending.Node || !n.online[candidate] {
			continue
		}
		if n.heldEdges(candidate) >= n.config.Shape.budget {
			continue
		}
		host = candidate
		break
	}
	if host == -1 {
		return false
	}
	if n.heldEdges(pending.Node) >= n.config.Shape.budget {
		return false
	}

	n.online[pending.Node] = true
	n.report.NewcomersAdmitted++
	// A new node arrives with an empty table and no edges anyone knows of.
	n.joined[pending.Node] = true
	n.joinedOrder = append(n.joinedOrder, pending.Node)
	n.states[pending.Node] = newM6NodeState(pending.Node,
		n.config.Shape.degree, n.config.Capacity, n.config.NearFrom)

	n.held[pending.Node][host] = struct{}{}
	n.held[host][pending.Node] = struct{}{}
	n.report.ArrivalsAdmitted++
	return true
}

// clearForTheFromScratchControl is П-6's control, and the whole of it.
//
// ⚠️ WHAT IS ERASED, at the moment of the first departure and not before: every
// measured node's bucket table, its shelf, its per-level refresh stamps, the
// candidates an exchange had already handed it, its record of whom it has
// already probed, and when it last exchanged with each neighbour.
//
// ⚠️ THE LAST TWO MATTER MORE THAN THEY LOOK. They are memory like the table is:
// a node that forgot its records but remembered whom it had asked could never
// find those nodes again, and the control would measure "a node that lost its
// table AND its ability to refill it" — which is not first filling, it is
// paralysis. Leaving them in place cost the control its meaning once already.
//
// ⚠️ WHAT IS NOT: the graph, the event sequence, the candidate stream and its
// internal state, who is online, and the budget already released. The control
// differs from the main run in the node's MEMORY and in nothing else; rewinding
// the world as well would make the comparison meaningless.
func (n *m6Network) clearForTheFromScratchControl() error {
	// ⚠️ The invariant that makes the erasure above complete, asserted instead
	// of re-zeroed. The control clears at the FIRST departure, before anybody
	// has served a tick, so no loss can have been detected yet — and a mutation
	// that removed a re-zeroing here could not be made to fail, because there
	// was never anything to zero. If this ever fires, the clearing has moved and
	// the control is comparing two different things.
	if sumOf(n.report.LostByLevel) != 0 || sumOf(n.report.RefilledByLevel) != 0 {
		return fmt.Errorf(
			"the ‘from scratch’ control cleared the tables after %d losses had already been "+
				"detected: the control is meant to be a FIRST FILLING, and a run that starts "+
				"with recovery already under way is not one",
			sumOf(n.report.LostByLevel))
	}

	for _, owner := range n.owners {
		state := n.states[owner]
		if state == nil {
			continue
		}
		state.Table = newM6Table(owner, n.config.Shape.degree, n.config.Capacity, n.config.NearFrom)
		state.Shelf = nil
		state.Offered = nil
		state.Exhausted = map[int32]struct{}{}
		state.TriedThisTick = map[int32]struct{}{}
		state.LastExchange = map[int32]int{}
		for level := range state.LastRefreshed {
			state.LastRefreshed[level] = -1
			state.LostByLevel[level] = 0
			state.RefilledByLevel[level] = 0
		}
		state.FilledElsewhere = 0
	}
	n.report.FilledElsewhere = 0
	n.clearedAt = n.tick
	return nil
}

// --- serving one node --------------------------------------------------------------

func (n *m6Network) serve(owner int32) {
	state := n.states[owner]
	// A temporary refusal is retried on a LATER tick, not on this one: the reset
	// belongs here, at the start of the node's turn.
	clear(state.TriedThisTick)
	n.expireShelf(state)

	ceiling := n.config.Repair
	for spent := 0; ceiling <= 0 || spent < ceiling; spent++ {
		if !n.probeOnceInTheNetwork(owner, state) {
			return
		}
	}
}

// expireShelf drops records T_stale ticks after they were DETECTED gone.
func (n *m6Network) expireShelf(state *m6NodeState) {
	kept := state.Shelf[:0]
	for _, shelved := range state.Shelf {
		if n.tick-shelved.DetectedAt >= n.config.StaleTicks {
			if n.measured(state.Table.Owner) {
				n.report.ShelfExpired++
			}
			continue
		}
		kept = append(kept, shelved)
	}
	state.Shelf = kept
}

// probeOnceInTheNetwork performs at most one probe and reports whether it did
// anything at all. A tick that finds nothing to do costs nothing.
func (n *m6Network) probeOnceInTheNetwork(owner int32, state *m6NodeState) bool {
	// 1. Refresh, because it is the ONLY thing that detects a loss (П-4). With
	//    C = ∞ this branch never fires, nothing is ever detected, and the two
	//    coverage numbers drift apart without limit — the expected result of
	//    the negative control.
	if level, target, ok := n.levelDueForRefresh(state); ok {
		n.probe(owner, state, target, level, true)
		return true
	}

	// 2. Fill, walking the levels in the service order of §5.9.3 and stopping at
	//    the first one a candidate can be found for.
	//
	// ⚠️ EVERY level in turn, not just the first. An earlier version picked one
	// level and gave up on the whole node when that level had no candidate — so a
	// near level with nobody to put in it (and at 1k×8 level 7 is populated by
	// ≈4 nodes, so that is the normal case) froze the node for the rest of the
	// run and left every OTHER bucket empty while its acquaintances sat unused.
	// The coverage axis would have measured that deadlock and called it organic
	// knowledge.
	for _, level := range n.levelsToServe(state) {
		candidate, ok := n.candidateFor(owner, state, level)
		if !ok {
			continue
		}
		n.probe(owner, state, candidate, level, false)
		return true
	}

	// Nothing anywhere: the source has nobody for any level this node still has
	// room for. That is an outcome of the branch, and it is recorded once per
	// attempt rather than once per level.
	n.record(owner, m6NoCandidate)
	return false
}

// levelDueForRefresh finds a level the cadence says is stale and a member of it
// to probe.
//
// ⚠️ The member ROTATES, and that is not tidiness. Taking the lowest index every
// time means a level whose lowest-indexed member is alive is refreshed for ever
// without the dead records beside it ever being touched — detection would then
// be systematically blind to exactly the records П-4 exists to find, and the
// "claimed against actually alive" gap would be biased in one direction by the
// measurer rather than by the mechanism. The cursor is per level and advances on
// every refresh, so the members are visited in turn and the choice stays
// reproducible.
func (n *m6Network) levelDueForRefresh(state *m6NodeState) (int, int32, bool) {
	if n.config.Cadence <= 0 {
		return 0, -1, false
	}
	for level := state.Table.Coverage.Levels - 1; level >= 0; level-- {
		if len(state.Table.members[level]) == 0 {
			continue
		}
		// ⚠️ The clock starts when the level first holds a record, not at -1.
		// Treating "never refreshed" as "due now" gave every level ONE unplanned
		// refresh the moment it was filled — identical for C = 64 and C = 256,
		// so the two branches of the cadence sweep began with the same free
		// probe and the same dent in the filling budget. See markLevelFilled.
		if n.tick-state.LastRefreshed[level] < n.config.Cadence {
			continue
		}
		members := make([]int32, 0, len(state.Table.members[level]))
		for member := range state.Table.members[level] {
			members = append(members, member)
		}
		sort.Slice(members, func(a, b int) bool { return members[a] < members[b] })

		chosen := members[state.RefreshCursor[level]%len(members)]
		state.RefreshCursor[level]++
		state.LastRefreshed[level] = n.tick
		return level, chosen, true
	}
	return 0, -1, false
}

// levelsToServe is the service order of §5.9.3, as a LIST: the levels still
// short of what they lost, by descending deficit and then by NEARNESS; after
// them every other level with a free slot, nearest first.
//
// ⚠️ A list and not a single answer, because a level with no candidate must not
// stop the node — see probeOnceInTheNetwork.
func (n *m6Network) levelsToServe(state *m6NodeState) []int {
	type want struct {
		level   int
		deficit int
	}

	short := make([]want, 0, len(state.LostByLevel))
	for level := range state.LostByLevel {
		deficit := state.LostByLevel[level] - state.RefilledByLevel[level]
		if deficit <= 0 || state.Table.Coverage.Held[level] >= state.Table.Coverage.Capacity {
			continue
		}
		short = append(short, want{level: level, deficit: deficit})
	}
	sort.Slice(short, func(a, b int) bool {
		if short[a].deficit != short[b].deficit {
			return short[a].deficit > short[b].deficit
		}
		// Ties go to the NEARER level: those are the scarce ones by construction
		// (19 §3.3′), so serving them first is the order the contract names.
		return short[a].level > short[b].level
	})

	order := make([]int, 0, state.Table.Coverage.Levels)
	seen := make([]bool, state.Table.Coverage.Levels)
	for _, one := range short {
		order = append(order, one.level)
		seen[one.level] = true
	}
	for level := state.Table.Coverage.Levels - 1; level >= 0; level-- {
		if seen[level] || state.Table.Coverage.Held[level] >= state.Table.Coverage.Capacity {
			continue
		}
		order = append(order, level)
	}
	return order
}

// candidateFor produces one candidate for the level, by the branch's rules and
// the shelf order of §5.9.3 п.2.
func (n *m6Network) candidateFor(owner int32, state *m6NodeState, level int) (int32, bool) {
	// ⚠️ THE ENTRY STAYS ON THE SHELF until the probe succeeds or T_stale expires.
	// Handing it over used to remove it, so the FIRST unsuccessful probe — a peer
	// that simply had not come back yet — destroyed the record for good: detectLoss
	// returns early for a peer already released, so it was never put back, and
	// branch A and the omniscient source both skip released peers. A return inside
	// T_stale could no longer be used by anybody, which is the one thing the shelf
	// exists for.
	fromShelf := func() (int32, bool) {
		for _, shelved := range state.Shelf {
			if shelved.Level != level || n.spent(state, shelved.Node) {
				continue
			}
			return shelved.Node, true
		}
		return -1, false
	}

	if n.config.ShelfFirst {
		if node, ok := fromShelf(); ok {
			return node, true
		}
	}
	if node, ok := n.fromBranch(owner, state, level); ok {
		return node, true
	}
	if !n.config.ShelfFirst {
		return fromShelf()
	}
	return -1, false
}

// fromBranch is П-1 made executable: what each branch may offer, and nothing
// beyond it.
func (n *m6Network) fromBranch(owner int32, state *m6NodeState, level int) (int32, bool) {
	if n.config.OmniscientControl {
		return n.fromOmniscience(owner, state, level)
	}

	wanted := level
	if !n.config.Branch.CanTargetALevel() {
		// ⚠️ Branch B learns the NodeID at the handshake, so it cannot aim. It
		// offers whoever is next and the level it lands in is an accident — the
		// property §5.3 predicts will leave near levels empty.
		wanted = -1
	}

	if node, ok := n.fromAcquaintances(owner, state, wanted); ok {
		return node, true
	}
	if n.config.Branch == branchA || n.config.Branch == branchB {
		return -1, false
	}

	// A′ and C: whatever an earlier exchange or answer already handed over.
	//
	// ⚠️ THE RECORD IS NOT REMOVED HERE. A record that does not fit right now is
	// kept, one already probed this tick is skipped, and the entry leaves the
	// queue only on a FINAL outcome — it was stored, it was already known, or no
	// bucket can ever hold it. Removing it on hand-out lost it to any temporary
	// refusal: a ceiling that frees on the next tick, a bucket that empties. The
	// single-exchange control felt that hardest — the record came at a price and
	// there was no second exchange to get it again.
	for _, offered := range state.Offered {
		// ⚠️ A record already in the table is NOT skipped here, and that is the
		// contract rather than an oversight: §5.1.0 chose to send no exclusion
		// list, and it says in as many words that repeats are possible and land
		// in the outcome "candidate already known". The probe is the PRICE of not
		// disclosing the asker's own table, so skipping it quietly would make A′
		// look cheaper than the choice it embodies.
		//
		// (Branch A is different: re-probing a peer already in the table is what
		// a REFRESH is, and it is scheduled by the cadence rather than by the
		// filling loop.)
		if n.spent(state, offered) {
			continue
		}
		if wanted >= 0 && levelOf(n.ids[owner], n.ids[offered], n.config.Shape.degree) != wanted {
			continue
		}
		return offered, true
	}

	if n.exchangeWithANeighbour(owner, state) {
		return n.fromBranch(owner, state, level)
	}
	if n.config.Branch == branchC && level >= n.config.NearFrom && n.addressedRequest(owner, state, level) {
		return n.fromBranch(owner, state, level)
	}
	return -1, false
}

// fromOmniscience is the CONTROL of §4.4: anybody in the membership may be
// offered.
//
// ⚠️ It is kept in its own function, next to the branches and not inside them,
// because the one thing that must never happen is a control quietly reported as
// a mechanism. Everything else — k, B, the ceiling R, the graph — is unchanged,
// which is what makes the comparison a comparison and what stops the result from
// being an upper bound over mechanisms in general.
func (n *m6Network) fromOmniscience(owner int32, state *m6NodeState, level int) (int32, bool) {
	// ⚠️ The scan starts at an OWNER-DEPENDENT offset, and that is not
	// decoration. Walking the network from index 0 makes every owner prefer the
	// same low-index nodes, those fill to their ceiling B, and from then on the
	// control spends its probes on "candidate at B" — measured: it filled FEWER
	// slots than branch A, which offers each node only its own handful of
	// neighbours. A control that loses to the thing it is a control for is
	// measuring its own scan order, not what perfect knowledge buys.
	// ⚠️ THE PHYSICAL POPULATION, filtered by mayTake like every other source.
	// Walking n.owners instead handed the control a second difference nobody
	// asked for: in the Q-half run the UNMEASURED ¬Q nodes — which run the
	// mechanism like everyone else — were offered Q candidates only, although
	// mayTake places no restriction on an unmeasured owner at all. The control is
	// supposed to change what an owner KNOWS, not who the background network is
	// allowed to connect to. The walk is over the JOINED population, for the same
	// reason the host scan is: an index space padded with the unjoined reserve
	// makes the order of offers a function of the run's length.
	if len(n.joinedOrder) == 0 {
		return -1, false
	}
	start := int(m6Random(n.config.Seed, "omniscient", int(owner)) % uint64(len(n.joinedOrder)))
	for offset := range n.joinedOrder {
		candidate := n.joinedOrder[(start+offset)%len(n.joinedOrder)]
		if candidate == owner || state.Table.holds(candidate) ||
			n.spent(state, candidate) {
			continue
		}
		if !n.mayTake(owner, candidate) {
			continue
		}
		// ⚠️ Already detected as gone. Knowing WHO EXISTS is not the same as
		// having no memory: without this the control re-probed every departed
		// node on every tick for the rest of the run and spent most of its
		// ceiling R on peers it had itself just buried — which is why it filled
		// fewer slots than branch A, whose pool drops a released peer the moment
		// it is detected.
		if _, released := state.Released[candidate]; released {
			continue
		}
		if level >= 0 && levelOf(n.ids[owner], n.ids[candidate], n.config.Shape.degree) != level {
			continue
		}
		// ⚠️ AN OFFER IS AN OFFER, whoever makes it. S(u) is what a mechanism
		// ACTUALLY handed the node, and the control hands over candidates like
		// any branch — it was the only source that did not say so, so a run whose
		// table was filled with strangers reported a pool of the owner's own
		// neighbours and nothing else. What the control could have offered is a
		// different quantity and is printed as the potential pool.
		n.noteReachable(state, candidate)
		return candidate, true
	}
	return -1, false
}

// spent says whether this owner should skip the candidate right now: either it
// can never be used, or it has already been probed during this tick.
func (n *m6Network) spent(state *m6NodeState, candidate int32) bool {
	if _, never := state.Exhausted[candidate]; never {
		return true
	}
	_, already := state.TriedThisTick[candidate]
	return already
}

// fromAcquaintances is branch A: the node's own links.
func (n *m6Network) fromAcquaintances(owner int32, state *m6NodeState, wanted int) (int32, bool) {
	for _, peer := range n.neighboursOf(owner) {
		if !n.visible(owner, peer) || state.Table.holds(peer) || n.spent(state, peer) {
			continue
		}
		if wanted >= 0 && levelOf(n.ids[owner], n.ids[peer], n.config.Shape.degree) != wanted {
			continue
		}
		return peer, true
	}
	return -1, false
}

// exchangeWithANeighbour is §5.1.0: one neighbour hands over m records of ITS
// table, the ones closest to the asker, no more often than once per T_exch — or
// once ever, under the control.
func (n *m6Network) exchangeWithANeighbour(owner int32, state *m6NodeState) bool {
	for _, peer := range n.neighboursOf(owner) {
		if !n.visible(owner, peer) || !n.online[peer] || n.states[peer] == nil {
			continue
		}
		last, done := state.LastExchange[peer]
		if done && (n.config.ExchangeOnce || n.tick-last < n.config.ExchangeEvery) {
			continue
		}
		state.LastExchange[peer] = n.tick
		if n.measured(owner) {
			n.report.ExchangesDone++
		}

		// ⚠️ No exclusion list travels with the request: the responder answers
		// from its current table and a repeat lands in "already known". Sending
		// one would be cheaper in probes and would disclose the ASKER'S table.
		handed := n.closestFromTable(n.states[peer], owner, n.config.ExchangeRecords)
		state.Offered = append(state.Offered, handed...)
		n.noteReachable(state, handed...)
		return true
	}
	return false
}

// addressedRequest is branch C: up to n records of a NAMED level, from a node
// already known, answered out of its own table and nothing else.
func (n *m6Network) addressedRequest(owner int32, state *m6NodeState, level int) bool {
	refused := false
	for _, responder := range n.knownTo(owner, state) {
		if !n.online[responder] || n.states[responder] == nil {
			continue
		}
		// The single rate-limit definition (§5.1.2 п. 4): both halves, and BOTH
		// AT THE RESPONDER. One alone is not a limit — per-pair leaves the total
		// unbounded across identities, per-node lets one asker eat it all.
		//
		// ⚠️ The responder is IN the per-pair key. An earlier version keyed it on
		// (asker, level) alone, which is a limit on the ASKER across every
		// responder in the network — the opposite of what §5.1.2 defines, and it
		// would have made the addressed branch look far cheaper to bound than it
		// is.
		pair := [3]int32{responder, owner, int32(level)}
		if n.answersByPair[pair] >= n.config.RatePair ||
			n.answersByNode[responder] >= n.config.RateNode {
			// ⚠️ TWO different facts, counted in two places. A refusal AT THIS
			// RESPONDER is load the limit shed, and it happened — so it is
			// counted here, once per responder. Whether the REQUEST ended up
			// unanswered is a different question, answered after the loop:
			// counting the request as refused per skipped responder would
			// inflate the branch's cost by the size of the contact list, and
			// counting nothing at all would hide the limit doing its job.
			if n.measured(owner) {
				n.report.AddressedRateLimited++
			}
			refused = true
			continue
		}

		answer := n.recordsOnLevel(responder, owner, level)
		if len(answer) == 0 {
			// The responder holds nothing on that level. ⚠️ Still an answer, and
			// it still costs the responder its rate-limit slot: refusing to
			// count an empty answer would let an asker sweep every contact it
			// has for free.
			n.answersByPair[pair]++
			n.answersByNode[responder]++
			if n.measured(owner) {
				n.report.AddressedAnswers++
			}
			return false
		}

		n.answersByPair[pair]++
		n.answersByNode[responder]++
		if n.measured(owner) {
			n.report.AddressedAnswers++
		}
		state.Offered = append(state.Offered, answer...)
		n.noteReachable(state, answer...)
		return true
	}
	if refused && n.measured(owner) {
		n.report.AddressedRefused++
	}
	return false
}

// recordsOnLevel is what the responder may hand over: the records of ITS OWN
// table that lie on the level the ASKER named, relative to the ASKER, capped at
// n and in a deterministic order.
//
// ⚠️ THE FILTER IS RELATIVE TO THE ASKER, and an earlier version got this wrong
// in a way that broke the branch twice over. It translated the asked level into
// one bucket of the responder and handed that bucket over whole. That bucket is
// not the answer:
//
//   - for L above the level the responder sees the asker at, the responder's
//     bucket is a WIDER range than the asker asked for — records outside the
//     named range were disclosed, which is exactly the limit branch C is
//     supposed to be measured against;
//   - for L equal to it, the bucket is simply the wrong one. A record on the
//     asker's level L differs from the asker at bit L, and so does the
//     responder, so record and responder AGREE at bit L and sit DEEPER than it.
//     Asker 00…, responder 80…, record c0…: the record is on the asker's level 0
//     and in the responder's bucket 1, and the translation named bucket 0.
//
// Both the coverage and the disclosure were wrong, and in opposite directions.
// The responder can compute the filter honestly: the asker's NodeID travels with
// the request anyway (§5.1.2 п. 5), so "which of my records are on your level L"
// is a question it can answer exactly.
//
// ⚠️ The ORDER is a decision of the stand, not of the contract: §5.1.2 says "up
// to n records" and names no order. Taking them in map order made the answer —
// and every probe that followed from it — depend on Go's map iteration, so two
// runs of one seed diverged. The rule here is the one §5.1.0 already gives A′:
// closest to the asker first, ties by node index.
func (n *m6Network) recordsOnLevel(responder, asker int32, level int) []int32 {
	matching := make([]int32, 0, n.config.AddressedRecords*2)
	for _, bucket := range n.states[responder].Table.members {
		for member := range bucket {
			if member == asker {
				continue
			}
			if levelOf(n.ids[asker], n.ids[member], n.config.Shape.degree) != level {
				continue
			}
			matching = append(matching, member)
		}
	}
	sortByDistanceTo(n, asker, matching)

	if len(matching) > n.config.AddressedRecords {
		matching = matching[:n.config.AddressedRecords]
	}
	return matching
}

// sortByDistanceTo puts the nodes closest to `target` first, ties by index.
// ⚠️ The tie-break is what makes the order total: without it the result would
// still depend on the order the callers happened to collect the records in, and
// both callers collect them by walking a map.
func sortByDistanceTo(n *m6Network, target int32, nodes []int32) {
	sort.Slice(nodes, func(a, b int) bool {
		if xorLess(n.ids[target], n.ids[nodes[a]], n.ids[nodes[b]]) {
			return true
		}
		if xorLess(n.ids[target], n.ids[nodes[b]], n.ids[nodes[a]]) {
			return false
		}
		return nodes[a] < nodes[b]
	})
}

// knownTo is who the node can address a request to: its neighbours and whoever
// is already in its table. ⚠️ A stranger cannot be asked — there is nothing to
// send the request over (§5.1.2 п.1).
// ⚠️ DEDUPLICATED. A neighbour is usually in the table as well, so the same
// identity appeared twice — and when its rate limit was spent, addressedRequest
// counted a refusal for each copy. The load a limit sheds would then depend on
// how the asker happens to STORE its knowledge, not on how often it asked.
func (n *m6Network) knownTo(owner int32, state *m6NodeState) []int32 {
	seen := make(map[int32]struct{}, 16)
	known := make([]int32, 0, 16)
	add := func(node int32) {
		if _, already := seen[node]; already {
			return
		}
		seen[node] = struct{}{}
		known = append(known, node)
	}

	for _, peer := range n.neighboursOf(owner) {
		if n.visible(owner, peer) {
			add(peer)
		}
	}
	for _, level := range state.Table.members {
		for member := range level {
			add(member)
		}
	}
	sort.Slice(known, func(a, b int) bool { return known[a] < known[b] })
	return known
}

// closestFromTable is the A′ selection rule: the records of the responder's
// table closest to the asker's NodeID. Those are the ones that close the asker's
// NEAR levels, and the asker's NodeID is in the request anyway.
func (n *m6Network) closestFromTable(state *m6NodeState, asker int32, want int) []int32 {
	all := make([]int32, 0, 32)
	for _, level := range state.Table.members {
		for member := range level {
			if member != asker {
				all = append(all, member)
			}
		}
	}
	sortByDistanceTo(n, asker, all)

	if len(all) > want {
		all = all[:want]
	}
	return all
}

// dropFromOffered removes a record from the queue on a FINAL outcome, every copy
// of it: the same node can have been handed over by two neighbours, and it is one
// record however many times it arrived.
func dropFromOffered(state *m6NodeState, node int32) {
	kept := state.Offered[:0]
	for _, offered := range state.Offered {
		if offered != node {
			kept = append(kept, offered)
		}
	}
	state.Offered = kept
}

// dropFromShelf removes a record that has been successfully used again. ⚠️ The
// only other way off the shelf is expiry, and neither path rewrites DetectedAt:
// the shelf life is counted from the FIRST detection, not from the last attempt
// to use the record.
func (n *m6Network) dropFromShelf(state *m6NodeState, node int32) bool {
	kept := state.Shelf[:0]
	for _, shelved := range state.Shelf {
		if shelved.Node != node {
			kept = append(kept, shelved)
		}
	}
	dropped := len(kept) != len(state.Shelf)
	state.Shelf = kept
	return dropped
}

// noteReachable records what a mechanism ACTUALLY handed this node. It is the
// measured S(u) of §5.1.1, and the reason it is accumulated rather than computed
// at the end: what a neighbour holds is not what it gave, and `m`, `n` and the
// level a request named are exactly the difference between the two.
func (n *m6Network) noteReachable(state *m6NodeState, offered ...int32) {
	for _, node := range offered {
		state.Reachable[node] = struct{}{}
	}
}

// --- one probe -----------------------------------------------------------------------

// record books a probe. ⚠️ TWICE, into two different aggregates: once into the
// physical total, and — only when the node doing the probing is MEASURED — into
// the cost of the measured population.
//
// Every node of the network runs the mechanism, so without the split the Q half
// was reported with the cost of the WHOLE network beside the coverage of the
// half: one population's work priced against another's result.
func (n *m6Network) record(owner int32, outcome m6Outcome) {
	n.report.PhysicalProbes.add(outcome)
	if !n.measured(owner) {
		return
	}
	n.report.Probes.add(outcome)
	if n.churnSeen {
		n.report.ProbesAfterChurn.add(outcome)
	}
}

// measured says whether this node's work belongs in the measured aggregates.
func (n *m6Network) measured(node int32) bool { return n.member(n.ids[node]) }

// probe is the one place a probe happens, and therefore the one place a loss can
// be DETECTED.
func (n *m6Network) probe(owner int32, state *m6NodeState, candidate int32, level int, refresh bool) {
	state.TriedThisTick[candidate] = struct{}{}
	if levelOf(n.ids[owner], n.ids[candidate], n.config.Shape.degree) < 0 {
		// Shares the whole prefix this table can address: no bucket will ever
		// hold it, so this refusal is permanent and the candidate is written off
		// for good. Every OTHER refusal is temporary and is retried later.
		state.Exhausted[candidate] = struct{}{}
		dropFromOffered(state, candidate)
	}

	// ⚠️ A probe needs a free slot at EACH END THAT WOULD HAVE TO OPEN ONE, and
	// the two ends are asked separately because `held` is per node: a release
	// happens at the end that detected the loss (П-4), so one side can believe
	// the connection is gone while the other still holds it.
	//
	//	the owner does not hold it      → it must open one, so its ceiling applies;
	//	the candidate still holds it    → nothing to open at its end, whatever its
	//	                                  ceiling says.
	//
	// Both halves were wrong before. The owner's ceiling was checked only for a
	// re-connect, so a stranger from A′ or C went through an owner already at B;
	// and the candidate's ceiling was applied even to a peer that had never
	// released the edge, so a returning neighbour sitting at its own ceiling
	// could not be taken back — which is the one thing the shelf exists for.
	_, released := state.ReleasedEdge[candidate]
	ownerHolds := n.holdsEdge(owner, candidate)
	candidateHolds := n.holdsEdge(candidate, owner)

	if !ownerHolds && n.heldEdges(owner) >= n.config.Shape.budget {
		n.record(owner, m6OwnerAtBudget)
		return
	}

	heldBudget := func(node int32) int { return n.heldEdges(node) }
	if candidateHolds {
		heldBudget = func(node int32) int {
			if node == candidate {
				return 0
			}
			return n.heldEdges(node)
		}
	}

	outcome, filled := classifyProbe(n.ids, state.Table, candidate,
		func(node int32) bool { return n.online[node] },
		heldBudget,
		n.config.Shape.budget)

	// ⚠️ A refresh finds an ABSENCE, not a slot: the candidate is already held,
	// so "already known" is what a successful refresh looks like. Only the
	// unreachable answer carries information, and that information is a loss.
	n.record(owner, outcome)

	if outcome == m6CandidateUnreachable {
		n.detectLoss(owner, state, candidate)
		return
	}
	if outcome == m6AlreadyKnown {
		// Final for the queue: the record is in the table, so there is nothing
		// left to do with the copy that was handed over.
		dropFromOffered(state, candidate)
		return
	}
	if outcome != m6SlotFilled {
		// Temporary: the ceiling may free, the bucket may empty. The record stays
		// in the queue for a later tick.
		return
	}
	dropFromOffered(state, candidate)

	// The fill re-establishes the edge if this is a record the owner had
	// released after detecting it gone — that is what "the released edges have to
	// be found again by probing" means, and it is the shelf's whole purpose. The
	// ceiling was checked above, so this cannot exceed it.
	if filled >= 0 && len(state.Table.members[filled]) == 1 {
		// The level has just become non-empty: this is where its cadence clock
		// starts, so the first refresh falls due C ticks from here.
		state.LastRefreshed[filled] = n.tick
	}

	// ⚠️ A BURIAL IS LIFTED BY THE RECORD COMING BACK, whether or not a
	// connection comes with it. Leaving `Released` set after a record-only
	// restoration is the stale-burial bug in its other form: branch A and the
	// omniscient source skip a released peer, and detectLoss returns early for
	// one, so the peer would be unreachable to every source and its NEXT
	// departure would never be detected.
	if _, buried := state.Released[candidate]; buried {
		delete(state.Released, candidate)
		delete(state.ReleasedEdge, candidate)
		if n.dropFromShelf(state, candidate) && n.measured(owner) {
			n.report.ShelfHits++
		}
	}

	if released {
		// ⚠️ BOTH SIDES, and both PARTS of each side's state. A connection is
		// re-established, not half-established:
		//
		//   - the slot, or the candidate's end stays free and it can hand the
		//     same slot to a newcomer while the owner counts the link as back;
		//   - the RECORD OF THE DETECTED LOSS, or the candidate still believes it
		//     has already buried this peer — and when the peer departs AGAIN,
		//     detectLoss returns early, no budget is freed and the new loss is
		//     never counted. A stale burial silently switches detection off for
		//     that pair for the rest of the run.
		//
		// The candidate's room was checked above (it is exempt only when it still
		// holds the edge), so this cannot exceed its ceiling.
		n.held[owner][candidate] = struct{}{}
		n.held[candidate][owner] = struct{}{}
		if other := n.states[candidate]; other != nil {
			// ⚠️ BOTH PARTS at the other end too. Clearing its burial without
			// clearing its shelf leaves an orphan: a shelved record for a peer it
			// no longer considers lost, which nothing can ever take off the shelf
			// again — the only path that does is the re-connect branch, and that
			// branch is now unreachable for it. Dropping the record costs it
			// nothing: the connection is back, so branch A can offer the peer
			// again and a probe re-stores it.
			delete(other.Released, owner)
			delete(other.ReleasedEdge, owner)
			n.dropFromShelf(other, owner)
		}
	}

	if !n.churnSeen || filled < 0 {
		return
	}
	if state.RefilledByLevel[filled] < state.LostByLevel[filled] {
		state.RefilledByLevel[filled]++
		if n.measured(owner) {
			n.report.RefilledByLevel[filled]++
		}
		return
	}
	// ⚠️ A fill at a level that lost nothing is coverage, not recovery. Counting
	// it as recovery would let a node "recover" level 0 by filling level 7.
	state.FilledElsewhere++
	if n.measured(owner) {
		n.report.FilledElsewhere++
	}
}

// detectLoss is the SECOND of the three moments of П-4, and the only one the
// node itself experiences.
func (n *m6Network) detectLoss(owner int32, state *m6NodeState, gone int32) {
	if _, already := state.Released[gone]; already {
		// Detected before: the probe still cost a connection attempt, but the
		// loss is not counted twice and no second budget is freed.
		return
	}

	// ⚠️ THREE DIFFERENT THINGS USED TO BE ONE. A probe that finds nobody home
	// can mean:
	//
	//	the owner holds an EDGE to it   → a connection is gone: free the slot;
	//	the owner holds a RECORD of it  → a table entry is gone: shelve it;
	//	the owner holds NEITHER         → a STRANGER did not answer. Nothing was
	//	                                  lost, because nothing was held.
	//
	// Marking the stranger `Released` was not merely a miscount: `probe` reads
	// that flag as "this edge was mine and I gave it up", so the next successful
	// probe of the same candidate took the re-connect branch and MANUFACTURED a
	// permanent connection at both ends, with a shelf hit for a record that was
	// never on a shelf. A candidate offered by A′, C or the omniscient source is
	// a stranger by construction, and one offline tick was enough.
	_, edge := n.held[owner][gone]
	if !edge && !state.Table.holds(gone) {
		n.report.PhysicalStrangersUnreachable++
		if n.measured(owner) {
			n.report.StrangersUnreachable++
		}
		return
	}

	lost := state.Table.drop(gone)
	for _, level := range lost {
		state.LostByLevel[level]++
		if n.measured(owner) {
			n.report.LostByLevel[level]++
		}
		n.report.PhysicalLost++
		state.Shelf = append(state.Shelf,
			m6Shelved{Node: gone, Level: level, DetectedAt: n.tick})
	}

	// ⚠️ THE BUDGET IS FREED HERE — at the detection, not at the departure. The
	// difference is the point of П-4: while the loss is undetected the survivor
	// still holds the slot, so a long detection delay shrinks the free budget of
	// the whole network, and the report shows the two facts next to each other.
	state.Released[gone] = struct{}{}
	if edge {
		// ⚠️ ONLY a loss that freed a real connection may be re-established as
		// one. Setting this for a record-only loss made the next successful probe
		// take the re-connect branch and MANUFACTURE an edge: a candidate stored
		// from an A′ offer, gone and found again, came back as a permanent link
		// with a slot of B spent at each end, although nothing but a table entry
		// had ever existed.
		delete(n.held[owner], gone)
		state.ReleasedEdge[gone] = struct{}{}
	}

	if left := n.departedAt[gone]; left >= 0 {
		n.report.PhysicalDetections++
		if n.measured(owner) {
			n.report.DetectionDelays = append(n.report.DetectionDelays, n.tick-left)
		}
	}
}

// --- collecting the result --------------------------------------------------------

// coverageByLevel is the two numbers of П-4 over every measured node, PER LEVEL:
// what the nodes believe they hold, and how much of it is actually alive. The
// analytical population of each level comes along, because an empty near level
// means nothing until the reader knows whether anybody lives there (П-5).
func (n *m6Network) coverageByLevel() []m6LevelCoverage {
	levels := make([]m6LevelCoverage, n.config.Shape.degree)

	for _, owner := range n.owners {
		state := n.states[owner]
		if state == nil {
			continue
		}

		// ⚠️ AN OFFLINE OWNER'S TABLE IS MEMORY, NOT COVERAGE. Its records may
		// point at perfectly live nodes, and counting them as "actually
		// available" would mix what the working network can route through with
		// what an absent node happens to remember. It is counted, apart: it is
		// what a return brings back (П-6).
		live := n.online[owner]

		for level := range levels {
			if live {
				levels[level].Slots += n.config.Capacity
			}
			for member := range state.Table.members[level] {
				if !live {
					levels[level].RetainedOffline++
					continue
				}
				levels[level].Claimed++
				if n.online[member] {
					levels[level].Actual++
				}
			}
		}
		if !live {
			continue
		}

		for _, peer := range n.owners {
			if peer == owner || !n.joined[peer] {
				continue
			}
			level := levelOf(n.ids[owner], n.ids[peer], n.config.Shape.degree)
			if level < 0 {
				continue
			}
			levels[level].PopulationJoined++
			// ⚠️ ONLINE, not merely joined: after churn a level whose inhabitants
			// have left is not a level the node failed to cover, and the two
			// readings are printed side by side so the difference is visible.
			if n.online[peer] {
				levels[level].Population++
			}
		}
	}
	return levels
}

func (n *m6Network) collect() {
	n.report.Levels = n.coverageByLevel()

	for _, owner := range n.owners {
		if n.states[owner] == nil {
			continue
		}
		// S(u) at the END: for A′ and C this is a RESULT OF THE MECHANISM — the
		// pool grew because records were exchanged — and §5.1.1 says so. The
		// border of the near levels comes from PoolAtStart instead.
		n.report.PoolByOwner = append(n.report.PoolByOwner, n.poolOf(owner))
		n.report.PoolPotential = append(n.report.PoolPotential, n.poolPotentialOf(owner))
	}
}

// poolOf is the MEASURED S(u): the node's own connections plus every record a
// mechanism actually handed it.
//
// ⚠️ It used to union the WHOLE table of every neighbour, which credited a node
// with knowledge no mechanism had given it: under a single exchange of four
// records it counted everything the neighbour knew, ignored `n` and the level a
// request named, and counted a record handed over twice as two. That quantity is
// still worth printing — it bounds what the branch COULD deliver — so it is kept
// as poolPotentialOf and labelled analytical.
func (n *m6Network) poolOf(owner int32) int {
	state := n.states[owner]
	pool := make(map[int32]struct{}, len(state.Reachable)+8)
	for node := range state.Reachable {
		pool[node] = struct{}{}
	}
	for _, peer := range n.neighboursOf(owner) {
		if n.mayTake(owner, peer) {
			pool[peer] = struct{}{}
		}
	}
	delete(pool, owner)
	return len(pool)
}

// poolPotentialOf is what the branch could have handed over with no `m`, no `n`
// and no level filter. ⚠️ ANALYTICAL and never S(u).
func (n *m6Network) poolPotentialOf(owner int32) int {
	state := n.states[owner]
	pool := map[int32]struct{}{}

	// ⚠️ THE CONTROL'S POTENTIAL IS ITS DEFINITION: every joined node it is
	// allowed to take. That number, and not the measured S(u), is what "knows
	// everybody" means — the measured pool is bounded by the ceiling R and by how
	// many ticks the run lasted, so availability and offers have to be read as
	// two quantities.
	if n.config.OmniscientControl {
		for _, candidate := range n.joinedOrder {
			if candidate != owner && n.mayTake(owner, candidate) {
				pool[candidate] = struct{}{}
			}
		}
		return len(pool)
	}

	for _, peer := range n.neighboursOf(owner) {
		if !n.mayTake(owner, peer) {
			continue
		}
		pool[peer] = struct{}{}
		if n.config.Branch != branchAPrime && n.config.Branch != branchC {
			continue
		}
		if n.states[peer] == nil {
			continue
		}
		for _, level := range n.states[peer].Table.members {
			for member := range level {
				pool[member] = struct{}{}
			}
		}
	}
	if n.config.Branch == branchC {
		for _, known := range n.knownTo(owner, state) {
			if n.states[known] == nil {
				continue
			}
			for _, level := range n.states[known].Table.members {
				for member := range level {
					pool[member] = struct{}{}
				}
			}
		}
	}
	delete(pool, owner)
	return len(pool)
}

// derivedNearFrom is П-5: ⌊log₂ S_ref⌋, from the MEASURED pool rather than from
// a formula, and taken ONCE from a named reference so both populations are
// compared at the same levels.
func derivedNearFrom(referencePool []int, degree int) (int, string) {
	median, ok := medianOf(referencePool)
	if !ok || median < 1 {
		return degree / 2, "CONTROL d/2 — the reference pool was empty, so nothing could be derived"
	}
	level := 0
	for 1<<(level+1) <= median {
		level++
	}
	if level > degree {
		level = degree
	}
	return level, fmt.Sprintf("⌊log₂ S_ref⌋ with S_ref = %d, the MEASURED median pool of branch A "+
		"over the full graph, fixed once and applied to every branch and both populations "+
		"(§5.1.1)", median)
}
