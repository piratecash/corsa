package overlaysim

// m6_trace_test.go is what the model records so that two runs can be COMPARED
// event by event rather than by their totals — the evidence behind "the same
// scenario, the same candidate stream, different memory" of П-6.
//
// Two traces, kept apart because they answer different questions:
//
//	the SCENARIO trace — what the world did to the network, tick by tick: the
//	departures decided, the departures realised, the returns that came due,
//	the newcomers offered. Always recorded; it is small.
//
//	the OFFER trace — what a SOURCE handed each measured owner and where it came
//	from, plus a snapshot of every measured owner's EXPOSURE at the churn
//	onset: the raw pool the source would draw from before the owner's own
//	memory filters it. Recorded only when asked, because it is large.
//
// ⚠️ What the exposure is depends on the branch, and that is the finding the
// trace exists to make checkable. In A, B and the omniscient control the
// exposure is WORLD state — the edges the owner holds, the joined population —
// which the "from scratch" control does not touch, so at the onset the two runs
// expose the same pool to every owner. In A′ and C the exposure includes what
// each neighbour would hand over FROM ITS OWN TABLE, and the neighbour's table
// is memory: clearing it changes the source. The trace records both halves so a
// reference can show exactly where the streams part.

import (
	"fmt"
	"sort"
)

// m6TickEvents is one tick of the scenario trace.
type m6TickEvents struct {
	Tick int
	// Decided is every departure decision drawn this tick (EXOGENOUS: a
	// function of the seed, the tick and the identity); Departed the subset
	// realised, which is conditional on the node being online.
	Decided  []int32
	Departed []int32
	// ReturnsDue lists the nodes whose promised return fell due this tick, and
	// NewcomersOffered the reserve identifiers the compensation offered.
	ReturnsDue       []int32
	NewcomersOffered []int32
}

func (e m6TickEvents) equal(other m6TickEvents) bool {
	return e.Tick == other.Tick && equalNodes(e.Decided, other.Decided) &&
		equalNodes(e.Departed, other.Departed) && equalNodes(e.ReturnsDue, other.ReturnsDue) &&
		equalNodes(e.NewcomersOffered, other.NewcomersOffered)
}

func equalNodes(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

// m6OfferSource names where a candidate came from.
type m6OfferSource uint8

const (
	// offerRefresh — the cadence chose a member of a level already held.
	offerRefresh m6OfferSource = iota
	// offerShelf — a record shelved after a detected loss (§5.9.3 п.2).
	offerShelf
	// offerAcquaintance — one of the owner's own held edges (branch A/B).
	offerAcquaintance
	// offerQueue — a record an earlier exchange or addressed answer handed over
	// and that had not been probed yet (A′/C).
	offerQueue
	// offerOmniscient — the control's walk over the joined population.
	offerOmniscient
	// offerExchange — not a candidate but the EXCHANGE itself: the neighbour
	// asked and the records it handed over (A′/C, §5.1.0).
	offerExchange
	// offerAddressed — the ADDRESSED REQUEST itself: the responder, the level
	// named, and the records it answered with (C, §5.1.2).
	offerAddressed
)

func (s m6OfferSource) String() string {
	switch s {
	case offerRefresh:
		return "refresh"
	case offerShelf:
		return "shelf"
	case offerAcquaintance:
		return "acquaintance"
	case offerQueue:
		return "queue"
	case offerOmniscient:
		return "omniscient"
	case offerExchange:
		return "exchange"
	case offerAddressed:
		return "addressed"
	default:
		// ⚠️ Named as what it is: a value that names no source must not print
		// as the last known one, or a tampered recording reads as a valid one.
		return fmt.Sprintf("unknown source %d", uint8(s))
	}
}

// m6OfferEntry is one entry of the offer trace.
type m6OfferEntry struct {
	Tick   int
	Owner  int32
	Source m6OfferSource
	// Level is the level the owner was serving (-1 when the branch cannot aim);
	// Peer is the candidate offered, or the neighbour/responder asked; Handed
	// is what an exchange or an addressed answer returned.
	Level  int
	Peer   int32
	Handed []int32
	// QuotaSkipped is, for an ADDRESSED request, the responders the asker
	// passed over because their rate limit (r_pair / r_node) was already spent
	// when it asked — in the asker's contact order, before the one it asked.
	// ⚠️ Recorded at the moment of the choice, because nothing else can tell
	// it later: the quota counters are reset every tick and move while the
	// tick is served, so a snapshot at the start of the tick sees them at
	// zero (owner's P2, round 32). Diagnostic only — not part of `equal`, not
	// part of the recorded stream.
	QuotaSkipped []int32
}

func (o m6OfferEntry) equal(other m6OfferEntry) bool {
	return o.Tick == other.Tick && o.Owner == other.Owner && o.Source == other.Source &&
		o.Level == other.Level && o.Peer == other.Peer && equalNodes(o.Handed, other.Handed)
}

func (o m6OfferEntry) String() string {
	if len(o.Handed) > 0 || o.Source == offerExchange || o.Source == offerAddressed {
		return fmt.Sprintf("tick %d owner %d %s with %d (level %d) handed %v",
			o.Tick, o.Owner, o.Source, o.Peer, o.Level, o.Handed)
	}
	return fmt.Sprintf("tick %d owner %d %s offered %d for level %d",
		o.Tick, o.Owner, o.Source, o.Peer, o.Level)
}

// m6Exposure is what the source of ONE owner would draw from at a given moment,
// before the owner's memory (its table, its shelf, whom it tried) filters it.
type m6Exposure struct {
	// Acquaintances is the branch-A pool: the held edges the measurement lets
	// the owner take, in the order the source walks them.
	Acquaintances []int32
	// ByNeighbour is, for A′ and C, what each acquaintance would hand over from
	// its table right now (m closest records), keyed by the neighbour. ⚠️ THIS
	// HALF IS ANOTHER NODE'S MEMORY, not world state.
	ByNeighbour map[int32][]int32
	// Recorded is the replay's exposure: the entries of the recorded stream
	// available and unconsumed right now (m6_stream_test.go).
	Recorded []int32
	// Omniscient is the CONTROL's exposure: the joined population the
	// measurement lets the owner take, in the order fromOmniscience walks it.
	// The control never draws from the owner's edges, so for it the other halves
	// stay empty.
	Omniscient []int32
}

func (e m6Exposure) equal(other m6Exposure) bool {
	if !equalNodes(e.Acquaintances, other.Acquaintances) || len(e.ByNeighbour) != len(other.ByNeighbour) ||
		!equalNodes(e.Recorded, other.Recorded) || !equalNodes(e.Omniscient, other.Omniscient) {
		return false
	}
	for peer, handed := range e.ByNeighbour {
		if !equalNodes(handed, other.ByNeighbour[peer]) {
			return false
		}
	}
	return true
}

// m6Trace is everything a run records for a comparison.
type m6Trace struct {
	Scenario []m6TickEvents
	Offers   []m6OfferEntry
	// ExposureAtOnset is every measured owner's exposure in the churn-onset
	// tick, taken AFTER the control's clearing (so it shows what the control
	// actually sees) and before anybody has served that tick. OnsetTick says
	// which tick that was; -1 when the run had no churn.
	ExposureAtOnset map[int32]m6Exposure
	OnsetTick       int
	// IDs is what every node index in the traces means: the run's identifiers,
	// population then reserve. A recording pins itself to them.
	IDs []nodeID
	// Members marks, index by index, which of those identifiers the run
	// measured; a recording carries it so a replay under another membership
	// is refused.
	Members []bool
}

func newM6Trace() *m6Trace {
	return &m6Trace{ExposureAtOnset: map[int32]m6Exposure{}, OnsetTick: -1}
}

// m6StreamDivergence is where two offer traces first part, and why.
type m6StreamDivergence struct {
	// Index is the position in the offer traces, Tick and Owner where it was.
	Index int
	Tick  int
	Owner int32
	// Main and Control are the two entries that differ; either may be absent
	// when one trace is a prefix of the other.
	Main, Control *m6OfferEntry
}

func (d m6StreamDivergence) String() string {
	render := func(offer *m6OfferEntry) string {
		if offer == nil {
			return "<nothing>"
		}
		return offer.String()
	}
	return fmt.Sprintf("offer #%d (tick %d, owner %d): main %s | control %s",
		d.Index, d.Tick, d.Owner, render(d.Main), render(d.Control))
}

// firstOfferDivergence compares two offer traces entry by entry.
func firstOfferDivergence(main, control []m6OfferEntry) (m6StreamDivergence, bool) {
	for index := 0; index < len(main) || index < len(control); index++ {
		var left, right *m6OfferEntry
		if index < len(main) {
			left = &main[index]
		}
		if index < len(control) {
			right = &control[index]
		}
		if left != nil && right != nil && left.equal(*right) {
			continue
		}
		divergence := m6StreamDivergence{Index: index, Main: left, Control: right}
		if left != nil {
			divergence.Tick, divergence.Owner = left.Tick, left.Owner
		} else {
			divergence.Tick, divergence.Owner = right.Tick, right.Owner
		}
		return divergence, true
	}
	return m6StreamDivergence{}, false
}

// firstScenarioDivergence compares two scenario traces tick by tick and names
// the first field that differs.
func firstScenarioDivergence(main, control []m6TickEvents) (int, string, bool) {
	for index := 0; index < len(main) || index < len(control); index++ {
		switch {
		case index >= len(main):
			return control[index].Tick, "the main run ended, the control went on", true
		case index >= len(control):
			return main[index].Tick, "the control ended, the main run went on", true
		case main[index].equal(control[index]):
			continue
		}
		left, right := main[index], control[index]
		switch {
		case !equalNodes(left.Decided, right.Decided):
			return left.Tick, fmt.Sprintf("departure DECISIONS differ: %v against %v — the "+
				"exogenous draw itself moved, which no memory may do", left.Decided, right.Decided), true
		case !equalNodes(left.Departed, right.Departed):
			return left.Tick, fmt.Sprintf("REALISED departures differ: %v against %v — a decision "+
				"landed on a node that was online in one run and not in the other", left.Departed,
				right.Departed), true
		case !equalNodes(left.ReturnsDue, right.ReturnsDue):
			return left.Tick, fmt.Sprintf("returns due differ: %v against %v", left.ReturnsDue,
				right.ReturnsDue), true
		default:
			return left.Tick, fmt.Sprintf("newcomers offered differ: %v against %v",
				left.NewcomersOffered, right.NewcomersOffered), true
		}
	}
	return 0, "", false
}

// exposureDivergence lists the owners whose exposure at the onset differs, and
// says which half differed — the world half (held edges for a branch, the
// joined population for the omniscient control) or the neighbours' tables —
// and, APART from both, the owners one snapshot has and the other has not.
//
// ⚠️ The walk is over the UNION of the two key sets, and an absent owner is
// never read as an empty exposure: an empty exposure is a finding about the
// owner ("nothing to be offered"), an absent owner is a finding about the
// snapshot (a half that recorded fewer owners, or a tracing bug), and a
// comparison that conflated them let an incomplete snapshot pass the ‘same
// pool’ and ‘same recorded stream’ claims — whichever side was missing it.
func exposureDivergence(main, control map[int32]m6Exposure) (absent, world, tables []int32) {
	for owner := range control {
		if _, both := main[owner]; !both {
			absent = append(absent, owner)
		}
	}
	for owner, left := range main {
		right, both := control[owner]
		switch {
		case !both:
			absent = append(absent, owner)
		case !equalNodes(left.Acquaintances, right.Acquaintances) ||
			!equalNodes(left.Omniscient, right.Omniscient):
			world = append(world, owner)
		case !left.equal(right):
			tables = append(tables, owner)
		}
	}
	sort.Slice(absent, func(a, b int) bool { return absent[a] < absent[b] })
	sort.Slice(world, func(a, b int) bool { return world[a] < world[b] })
	sort.Slice(tables, func(a, b int) bool { return tables[a] < tables[b] })
	return absent, world, tables
}

// --- recording ---------------------------------------------------------------------

func (n *m6Network) traceTick() *m6TickEvents {
	if len(n.trace.Scenario) == 0 || n.trace.Scenario[len(n.trace.Scenario)-1].Tick != n.tick {
		n.trace.Scenario = append(n.trace.Scenario, m6TickEvents{Tick: n.tick})
	}
	return &n.trace.Scenario[len(n.trace.Scenario)-1]
}

// noteOffer books one entry of the offer trace, when the run was asked to keep
// one. ⚠️ For EVERY node of the physical network, measured or not: the trace is
// what a recorded stream is made of, and a replay has to serve the unmeasured
// half from the same recording, or the Q-half replay would give that half a
// different source from the full-graph replay. Readers that want the measured
// population filter by membership.
func (n *m6Network) noteOffer(owner int32, source m6OfferSource, level int, peer int32, handed []int32) {
	n.noteOfferSkipping(owner, source, level, peer, handed, nil)
}

// noteOfferSkipping is noteOffer for an addressed request, with the
// responders skipped for quota carried into the trace.
func (n *m6Network) noteOfferSkipping(
	owner int32, source m6OfferSource, level int, peer int32, handed, quotaSkipped []int32,
) {
	if n.recording != nil {
		// The DIRECT recording (decision 3.4): the same entries recordStream
		// would derive from the trace, written now, without the trace.
		n.recording.appendOffer(n.tick, owner, source, peer, handed)
	}
	if !n.config.TraceOffers {
		return
	}
	n.trace.Offers = append(n.trace.Offers, m6OfferEntry{
		Tick: n.tick, Owner: owner, Source: source, Level: level, Peer: peer,
		Handed:       append([]int32(nil), handed...),
		QuotaSkipped: append([]int32(nil), quotaSkipped...),
	})
}

// snapshotExposureAtOnset records every measured owner's exposure. It is called
// once, in the onset tick, after the control has cleared and before anybody
// has served.
func (n *m6Network) snapshotExposureAtOnset() {
	n.trace.OnsetTick = n.tick
	if !n.config.TraceOffers {
		return
	}
	for _, owner := range n.owners {
		if n.states[owner] == nil {
			continue
		}
		n.trace.ExposureAtOnset[owner] = n.exposureOf(owner)
	}
}

// exposureOf is the raw pool of one owner right now, read from the SOURCE the
// run actually draws from: the held edges the measurement lets it take, and —
// in A′ and C — what each of those neighbours would hand over from its table
// if asked this instant; for the omniscient control, the joined population.
func (n *m6Network) exposureOf(owner int32) m6Exposure {
	exposure := m6Exposure{ByNeighbour: map[int32][]int32{}}
	if n.config.Stream != nil {
		exposure.Recorded = n.availableFromStream(owner)
		return exposure
	}
	if n.config.OmniscientControl {
		// ⚠️ The control does not draw from the owner's edges, so its exposure
		// is not the neighbours: it is the walk fromOmniscience makes — the
		// JOINED population from the owner-dependent offset, under mayTake —
		// before the owner's memory (table, spent, buried) filters it. Reading
		// the edges here let the ‘same pool at the onset’ claim miss an admitted
		// node and blame an edge the control never looks at.
		exposure.Omniscient = n.omniscientWalk(owner)
		return exposure
	}
	for _, peer := range n.neighboursOf(owner) {
		if !n.visible(owner, peer) {
			continue
		}
		exposure.Acquaintances = append(exposure.Acquaintances, peer)
		if !n.config.Branch.ExchangesRecords() || n.states[peer] == nil {
			continue
		}
		exposure.ByNeighbour[peer] = n.closestFromTable(n.states[peer], owner, n.config.ExchangeRecords)
	}
	return exposure
}

// StreamLine says, in the report, what the trace can and cannot prove about
// the candidate stream of this run against its counterpart.
func (r m6ModelReport) StreamLine() string {
	if r.Config.Stream != nil {
		mode := "memory KEPT"
		if r.Config.StartEmpty {
			mode = "memory CLEARED at the onset"
		}
		return fmt.Sprintf("PAIRED CONTROL with a recorded stream, %s: the external stream is the "+
			"same in both halves of the pair BY CONSTRUCTION (one recording, consumption state "+
			"outside the node, untouched by the clearing); refresh and shelf run natively as the "+
			"memory under measurement. At the end %d of the %d measured owners that took part had no "+
			"entry left to be offered (%d measured reserve identifiers never joined and are not in "+
			"that denominator) and %d handed entries were consumed. ⚠️ Whether the pair actually "+
			"agreed on boundaries, decisions and stream is stated by the comparison of the two runs "+
			"(m6_compare_test.go), not by this line; and neither half measures the recorded branch",
			mode, r.StreamExhaustedOwners, r.StreamParticipants,
			r.Members+r.ReserveMeasured-r.StreamParticipants, r.StreamConsumed)
	}
	if !r.Config.StartEmpty {
		return "not a ‘from scratch’ control; the scenario trace is kept for comparison"
	}
	// ⚠️ ONLY WHAT IS HELD STILL BY CONSTRUCTION IS CLAIMED HERE: the graph,
	// the phase boundaries (replayed) and the departure DECISIONS (a function
	// of the seed, the tick and the identity). Realised departures, returns due
	// and newcomers offered are RECOMPUTED in the control, and under background
	// churn a single admission that went differently changes who is online,
	// hence which decisions take effect, hence the returns and the compensation
	// after it. Whether those agree is answered by the trace comparison of the
	// two runs, never by this line.
	const heldStill = "What is held still by construction: the graph, the phase boundaries " +
		"(replayed from the main run) and the departure DECISIONS; realised departures, returns " +
		"due and newcomers offered are recomputed here and agree only where the online sets " +
		"agree — the scenario trace comparison says whether they did"
	if r.Config.Branch.ExchangesRecords() {
		return "⚠️ THE CANDIDATE STREAM IS NOT THE SAME AS THE MAIN RUN'S in this branch and the " +
			"report does not claim it is: the source of A′/C is what NEIGHBOURS hand over from " +
			"THEIR tables, the clearing erased those tables for every measured node, so from the " +
			"onset tick the control asks a different source. " + heldStill + "; the held-edge " +
			"half of every owner's exposure is the same at the onset. The difference between " +
			"the two runs is memory PLUS the emptied source, and it cannot be attributed to " +
			"memory alone (see the contract, §6.16)"
	}
	return "the candidate stream is the same at the onset by construction: the source of this " +
		"branch is world state (held edges / the joined population) that the clearing does not " +
		"touch. " + heldStill + ". ⚠️ From the onset on, the OFFERS differ — that is the memory " +
		"being measured — and the held edges drift apart as detection frees budget at different " +
		"moments in the two runs, which is a consequence of behaviour and is measured, not hidden"
}
