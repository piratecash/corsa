package overlaysim

// m6_model_test.go is the filling-and-recovery model of
// docs/refactoring/dht/21-m6-bucket-discovery-measurement.md §5: the four
// candidate branches (П-1), the shelf and the three separated moments (П-4), the
// repair ceiling R, the cadence C in ticks (П-7), the churn forms and the
// arrival/return of nodes (§5.9.2), and the measured pool S(u) with the
// analytical level population (П-5).
//
// ⚠️ IT IS ONE TICK LOOP OVER EVERY MEASURED NODE, not one owner with a scripted
// queue. That is forced by the contract rather than chosen: branch A′ has a
// neighbour hand over records from ITS table, and branch C has a responder
// answer from ITS table, so the other nodes' tables have to exist and be filled
// by the same mechanism. A model where only one node has a table would be
// measuring a mechanism nobody else is running.
//
// ⚠️ NO NODE EVER READS THE SIMULATION. Everything a node may offer comes from
// its own edges or from another node's table, both of which it got by paying for
// them. The two quantities that ARE computed from global knowledge — S(u) and
// the population of a level — are marked analytical wherever they are printed,
// because they answer "what could this node have found", not "what did it know".
//
// ⚠️ Instrument only, and still not an agreed contract: О-M6-1…7 are open, no
// threshold is proposed, and a run of branch C is not agreement to weaken §0.2
// (§5.8).

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// m6ModelRevision travels with every number this file produces. It is separate
// from m6ModelVersion — that one labels the counters, this one the FILLING
// MODEL, and the two were written at different times against different open
// questions.
const m6ModelRevision = "m6-model/v0-draft (П-1…П-7 proposed, none agreed)"

// --- branches ------------------------------------------------------------------

// m6Branch is the candidate source of П-1. The four are NESTED — A ⊂ A′ ⊂ C and
// B ⊂ A — and that is their whole value: the difference between two neighbouring
// branches is the price of the next mechanism.
type m6Branch int

const (
	// branchA — the node's own acquaintances: the peers it is linked to in the
	// built graph, incoming links included. No mechanism, no disclosure: the
	// knowledge is already there.
	branchA m6Branch = iota

	// branchB — a subset of A: the same peers, but the node learns a NodeID only
	// when the session is established (П-3 mode B), so it cannot aim a probe at
	// a level it needs.
	//
	// ⚠️ That, and not a smaller pool, is what makes B a separate branch. The
	// difference between A and B in this model is TARGETING, and the expected
	// consequence is the one §5.3 predicts: near levels get filled by accident
	// or not at all.
	branchB

	// branchAPrime — A plus records a neighbour hands over about ITS OWN
	// neighbourhood. ⚠️ A mechanism, not organic knowledge: the neighbour
	// discloses whom it knows near us, it costs a frame, and repeating the
	// exchange walks that neighbour's table sector by sector.
	branchAPrime

	// branchC — A′ plus an addressed request for up to n records from a NAMED
	// near level. It weakens §0.2 knowingly, and the price is what the run is
	// for; running it is not agreement to weaken anything (§5.8).
	branchC
)

func (b m6Branch) String() string {
	switch b {
	case branchA:
		return "A (own acquaintances)"
	case branchB:
		return "B (mutual introduction, level unknown before the probe)"
	case branchAPrime:
		return "A′ (A + records exchanged with a neighbour)"
	default:
		return "C (A′ + addressed request for a near level)"
	}
}

// Reveals is printed next to every cost figure the branch produced. A cost
// without its disclosure is half the comparison: the branches differ in price
// AND in what they give away, and only one of the two is a number.
func (b m6Branch) Reveals() string {
	switch b {
	case branchA:
		return "reveals nothing beyond the links the node already has"
	case branchB:
		return "reveals nothing beyond the fact of a session; the NodeID arrives at the handshake, " +
			"so a probe cannot be aimed at a level"
	case branchAPrime:
		return "⚠️ the neighbour reveals WHOM IT KNOWS NEAR US — a sector of its own table, and a " +
			"repeated exchange walks more of it"
	default:
		return "⚠️ addressed enumeration of a named near level, answered from the responder's own " +
			"table: §0.2 is weakened knowingly and the price is what this run measures"
	}
}

// CanTargetALevel says whether the branch may ask for a particular level.
func (b m6Branch) CanTargetALevel() bool { return b != branchB }

// ExchangesRecords says whether this branch asks a neighbour for records of its
// table — the §5.1.0 exchange, which only A′ and C perform.
func (b m6Branch) ExchangesRecords() bool { return b == branchAPrime || b == branchC }

// everybody is the membership predicate for the whole network. ⚠️ Named apart
// from the routing measurer's `everyone`, which selects by INDEX: membership here
// is a property of the identifier, because the reserve of newcomers lives beyond
// the built graph and has no index in it.
func everybody(nodeID) bool { return true }

// --- one node's state -----------------------------------------------------------

// m6Shelved is a record set aside after its node was DETECTED gone.
//
// ⚠️ It is stamped with the detection tick, not the departure tick. The node has
// no way of knowing when the peer actually left (П-4: three separate moments),
// so a shelf life counted from the departure would be a shelf life counted from
// something the owner never observed.
type m6Shelved struct {
	Node       int32
	Level      int
	DetectedAt int
}

// m6NodeState is one measured node.
type m6NodeState struct {
	Table *m6Table
	// Shelf holds records whose node was found gone, until T_stale ticks after
	// the DETECTION.
	Shelf []m6Shelved
	// LostByLevel and RefilledByLevel are this node's recovery accounting, per
	// level, counted from the first departure onwards.
	LostByLevel     []int
	RefilledByLevel []int
	// FilledElsewhere counts post-churn fills at levels that lost nothing: real
	// coverage, and NOT recovery.
	FilledElsewhere int

	// LastRefreshed is the tick each level's cadence clock was last set: the tick
	// it was refreshed, or — for a level that has never been refreshed — the tick
	// it first came to hold a record.
	//
	// ⚠️ Starting it at "never" made every level due for a refresh the instant it
	// was filled, so C = 64 and C = 256 both began with one unplanned refresh and
	// the same dent in the filling budget. A cadence whose first period is zero
	// is not the cadence being swept.
	//
	// RefreshCursor rotates WHICH member of the level the next refresh probes —
	// see levelDueForRefresh for why a fixed choice would blind detection.
	LastRefreshed []int
	RefreshCursor []int
	// Released records which neighbours this node has already detected as gone,
	// so a budget is freed once and the loss counted once.
	Released map[int32]struct{}
	// ReleasedEdge is the SUBSET of Released whose loss also freed an
	// established connection, and therefore the only set a successful probe may
	// re-establish an edge from.
	//
	// ⚠️ A RECORD AND A CONNECTION ARE NOT THE SAME LOSS. A record stored from
	// an A′ or C offer occupies a bucket slot and no edge; when its subject goes
	// away, what is lost is the record. Reading one flag for both meant the next
	// successful probe "restored" a connection that had never existed — it
	// changed the graph and spent B at both ends, off the back of a table entry.
	ReleasedEdge map[int32]struct{}
	// LastExchange is the tick of the last A′ exchange with each neighbour, and
	// Offered the candidates that exchange produced and that have not been
	// probed yet.
	LastExchange map[int32]int
	Offered      []int32
	// Exhausted is every node this owner can never use: the ones whose
	// identifier shares the whole prefix its table can address, so there is no
	// bucket for them at any time.
	//
	// ⚠️ IT HOLDS ONLY PERMANENT REFUSALS. An earlier version put every probed
	// candidate here, temporary refusals included — a peer that happened to be at
	// its ceiling B, or a bucket that happened to be full — and never offered it
	// again. Two things went wrong: a node lost recoveries a real node would
	// make, and the "from scratch" control came out AHEAD of the main run,
	// because clearing that memory handed it a second chance the main run never
	// got. A control that beats the thing it controls for is a control measuring
	// the stand.
	Exhausted map[int32]struct{}
	// TriedThisTick keeps one tick from spending its whole ceiling R on the same
	// candidate. It is cleared at the start of every tick: a temporary refusal is
	// retried LATER, which is what "temporary" means.
	TriedThisTick map[int32]struct{}
	// Reachable is every node this one was ever ACTUALLY offered: its own
	// acquaintances, plus every record an exchange or an addressed answer really
	// handed over.
	//
	// ⚠️ It is the measured S(u), and it replaced a calculation that unioned the
	// WHOLE table of every neighbour. That calculation ignored `m`, `n`, the
	// level a request named and which records were handed over twice — under a
	// single four-record exchange it credited the asker with everything the
	// neighbour knew. What a node could in principle have learned is a different
	// quantity, kept apart as the analytical potential.
	Reachable map[int32]struct{}
}

func newM6NodeState(owner int32, levels, capacity, nearFrom int) *m6NodeState {
	state := &m6NodeState{
		Table:           newM6Table(owner, levels, capacity, nearFrom),
		LostByLevel:     make([]int, levels),
		RefilledByLevel: make([]int, levels),
		LastRefreshed:   make([]int, levels),
		RefreshCursor:   make([]int, levels),
		Released:        map[int32]struct{}{},
		ReleasedEdge:    map[int32]struct{}{},
		LastExchange:    map[int32]int{},
		Exhausted:       map[int32]struct{}{},
		TriedThisTick:   map[int32]struct{}{},
		Reachable:       map[int32]struct{}{},
	}
	return state
}

// --- configuration ---------------------------------------------------------------

// m6ChurnForm is §5.9.2. The three are not variants of one number: a shock and a
// shrinking network ask different questions, and only one of the three even
// claims to hold the population steady.
type m6ChurnForm int

const (
	// churnNone — no departures at all. The control the filling phase is
	// measured under.
	churnNone m6ChurnForm = iota
	// churnShock — a share f leaves at one tick.
	churnShock
	// churnCompensated — a share f_bg leaves every tick and as many arrivals are
	// OFFERED. ⚠️ Offered, not guaranteed: an arrival needs a receiving side
	// with free B and may be refused, which is why the report shows offered,
	// admitted and the actual online count apart.
	churnCompensated
	// churnShrink — departures with no compensation. ⚠️ Named for what it is:
	// this measures degradation, not a steady state.
	churnShrink
)

func (f m6ChurnForm) String() string {
	switch f {
	case churnNone:
		return "none"
	case churnShock:
		return "shock"
	case churnCompensated:
		return "compensated load (arrivals are OFFERED, not guaranteed)"
	default:
		return "SHRINKING NETWORK — degradation, not a steady state"
	}
}

// m6ModelConfig is every input of one measurement. Every field is printed with
// the result: §5.9.5 makes the grid a decision of the owner's, so a number whose
// point in the grid is not recorded cannot be placed in it.
type m6ModelConfig struct {
	Shape  shape
	Seed   uint64
	Policy policy
	Quota  int

	Branch   m6Branch
	Capacity int // k
	NearFrom int
	// NearFromRule records HOW NearFrom was obtained: derived from the measured
	// pool (П-5) or set to the control value d/2. The number alone cannot say.
	NearFromRule string

	// Repair is R, the probes a node may spend per tick. 0 means NO CEILING,
	// which is the control of §5.4 and not a default.
	Repair int
	// Cadence is C in ticks. 0 means ∞ — the NEGATIVE control: with no refresh
	// there is no detection at all (П-4), so the shelf stays empty and the
	// recovery axis degenerates. That is the expected result of the control and
	// cannot be the base of the grid.
	Cadence int
	// StaleTicks is T_stale, counted FROM THE DETECTION.
	StaleTicks int
	// ShelfFirst is the service-order choice of §5.9.3 п.2, left open there:
	// true probes shelved records before new candidates, false probes them last.
	ShelfFirst bool

	// ExchangeRecords is m and ExchangeEvery is T_exch, both A′ and C only.
	// ExchangeOnce is the control: one exchange per neighbour ever.
	ExchangeRecords int
	ExchangeEvery   int
	ExchangeOnce    bool

	// AddressedRecords is n, RatePair is r_pair and RateNode is r_node — the
	// SINGLE definition of the rate limit lives in §5.1.2 п.4 and is carried
	// here, never restated elsewhere.
	AddressedRecords int
	RatePair         int
	RateNode         int

	Churn      m6ChurnForm
	ChurnShare float64
	// ReturnShare is ret and ReturnAfter is T_back.
	ReturnShare float64
	ReturnAfter int
	// JoinMaxWait is T_join_max: after this many ticks in the entry queue a node
	// gives up, which is a counted outcome and not a silent drop.
	JoinMaxWait int

	// Ticks is how long the scenario runs, ChurnAt the tick a shock lands on.
	Ticks    int
	ChurnAt  int
	Measured int // how many owners are measured; 0 = every member

	// OmniscientControl replaces the branch with a source that may offer ANY
	// member of the network.
	//
	// ⚠️ A CONTROL, never a branch (§4.4). It says what the same k, the same B,
	// the same ceiling R and the same graph would give a node that simply knew
	// everybody. It is NOT a mathematical upper bound — that would be a claim
	// about every possible mechanism, and it needs a proof this stand does not
	// have.
	OmniscientControl bool

	// StartEmpty is the "from scratch" CONTROL of П-6.
	//
	// ⚠️ It is not a second experiment: the graph, the event sequence and the
	// candidate stream are the same, and the ONE difference is that the tables
	// are cleared. What exactly is cleared, and when, is stated in
	// clearForTheFromScratchControl — a control whose erasure is undocumented
	// measures an unknown difference.
	StartEmpty bool

	Membership string
}

func (c m6ModelConfig) String() string {
	cadence := fmt.Sprintf("%d ticks", c.Cadence)
	if c.Cadence <= 0 {
		cadence = "∞ — NEGATIVE CONTROL: the SCHEDULED refresh is off, detection is NOT. In A and B " +
			"nothing else re-probes a held record, so no loss is found; in A′ and C a repeat handed " +
			"back by a neighbour is a paid probe (§5.1.0) and may find the record dead"
	}
	repair := fmt.Sprintf("%d probes/tick", c.Repair)
	if c.Repair <= 0 {
		repair = "no ceiling — CONTROL showing the storm 19 §5 forbids"
	}
	exchange := "not used by this branch"
	if c.Branch.ExchangesRecords() {
		exchange = fmt.Sprintf("m=%d records, one exchange per neighbour every %d ticks",
			c.ExchangeRecords, c.ExchangeEvery)
		if c.ExchangeOnce {
			exchange = fmt.Sprintf("m=%d records, ONE exchange per neighbour ever — CONTROL for "+
				"the price of repeating it", c.ExchangeRecords)
		}
	}
	addressed := "not used by this branch"
	if c.Branch == branchC {
		addressed = fmt.Sprintf("n=%d records per answer, rate limit AT THE RESPONDER: r_pair=%d "+
			"answer(s)/tick per asker×level and r_node=%d answers/tick in total (§5.1.2 п.4 — the "+
			"single definition)", c.AddressedRecords, c.RatePair, c.RateNode)
	}
	branchLine := c.Branch.String()
	reveals := c.Branch.Reveals()
	if c.OmniscientControl {
		branchLine = "CONTROL — omniscient source over the whole membership"
		reveals = "⚠️ not a branch and not a mechanism: no discovery rule of 19 §3 can see the " +
			"whole network. It is a CONTROL RESULT UNDER THE STATED CONSTRAINTS (same k, same B, " +
			"same ceiling R, same graph), and it is NOT claimed to be a mathematical upper bound"
	}
	start := "the node keeps what it knew (П-6, main mode)"
	if c.StartEmpty {
		start = "CONTROL ‘from scratch’: tables and shelves cleared at the churn tick — FIRST " +
			"FILLING, not recovery (П-6)"
	}

	return fmt.Sprintf(
		"model %s\n  shape %s (N=%d, d=%d, B=%d), seed %d, policy %s, quota %d, membership %s\n"+
			"  branch %s\n    %s\n  bucket capacity k=%d, near levels from %d (%s)\n"+
			"  repair ceiling R: %s\n  cadence C: %s\n  shelf: T_stale=%d ticks AFTER DETECTION, "+
			"shelved records probed %s\n  A′ exchange: %s\n  addressed request: %s\n"+
			"  churn: %s, share %.2f at tick %d; returns %.2f after %d ticks; entry queue gives up "+
			"after %d ticks\n  run: %d ticks; start: %s",
		m6ModelRevision, c.Shape.name, c.Shape.nodes, c.Shape.degree, c.Shape.budget, c.Seed,
		c.Policy, c.Quota, c.Membership, branchLine, reveals, c.Capacity, c.NearFrom,
		c.NearFromRule, repair, cadence, c.StaleTicks, shelfOrder(c.ShelfFirst), exchange,
		addressed, c.Churn, c.ChurnShare, c.ChurnAt, c.ReturnShare, c.ReturnAfter, c.JoinMaxWait,
		c.Ticks, start)
}

func shelfOrder(first bool) string {
	if first {
		return "BEFORE new candidates (§5.9.3 п.2, the proposal)"
	}
	return "AFTER new candidates — the named alternative: the departed peer is treated as more " +
		"likely dead than returning"
}

// m6Random is the model's reproducible source, with a domain of its own.
// m6RandomEvent keys a per-(tick, node) decision on the TICK and the NODE'S
// IDENTITY, with no quantity in between.
//
// ⚠️ The obvious `tick*len(nodes)+node` is wrong here: the node array includes
// the newcomer reserve, and the reserve is sized for the run's length. Two runs
// of the same graph and the same seed that differ only in how long they last
// therefore drew different coins for the SAME node in the SAME tick of their
// common prefix — the events became a function of the duration, which is exactly
// what a comparison across durations must hold fixed.
func m6RandomEvent(seed uint64, purpose string, tick int, id nodeID) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(tick))
	material := append([]byte("corsa/overlay/sim/m6/event/"+purpose+"/v1"), buf[:]...)
	digest := sha256.Sum256(append(material, id[:]...))
	return binary.LittleEndian.Uint64(digest[:8])
}

func m6Random(seed uint64, purpose string, index int) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(index))
	digest := sha256.Sum256(append([]byte("corsa/overlay/sim/m6/"+purpose+"/v1"), buf[:]...))
	return binary.LittleEndian.Uint64(digest[:8])
}

// --- the network -----------------------------------------------------------------

// m6Pending is a node waiting in the entry queue of §5.9.2.
type m6Pending struct {
	Node int32
	// Since is the tick it first asked to join; Returning tells a node coming
	// back under its OWN NodeID from a newcomer.
	Since     int
	Returning bool
}

// m6Network is the whole scenario state.
type m6Network struct {
	g      *graph
	config m6ModelConfig
	// member decides the MEASURED population, and it takes an IDENTIFIER rather
	// than an index.
	//
	// ⚠️ The reserve lives beyond the built graph, so an index-based predicate
	// would be asked about nodes the graph does not have — and a predicate that
	// reads g.roles would panic on exactly the newcomers compensation depends on.
	// Membership is a property of the identifier (Q is computed from it), so the
	// identifier is what it is given.
	member func(nodeID) bool

	// all is every node of the physical network, and owners the MEASURED
	// membership. ⚠️ The two are not interchangeable: the graph, the budget, the
	// churn trace and the reserve are facts about `all`, while `owners` decides
	// only what is measured. Mixing them made the Q half a different experiment
	// instead of a different view of one.
	all []int32

	// joinedOrder is every node that HAS joined, in the order it did, and it is
	// the index space every scan of "the network" walks.
	//
	// ⚠️ NOT `all` with the unjoined filtered out. `all` carries the whole
	// newcomer reserve, whose size follows config.Ticks, so an offset taken
	// modulo its length and then walked past the unjoined tail wraps to the FIRST
	// nodes of the population. Both the choice of a newcomer's host and the order
	// the omniscient control offers candidates in became functions of how long
	// the run was going to be.
	joinedOrder []int32

	// ids and roles cover the RESERVE as well as the built graph.
	//
	// ⚠️ The reserve is drawn beyond the population, not carved out of it. Taking
	// newcomers from inside the graph meant a compensated run started with a
	// quarter of the network artificially offline and their edges already dead —
	// so the scenario measured a hole it had dug itself, and the reserve ran out
	// while departures continued. Identifiers keep coming from the one generator
	// and the role mix is preserved by selection, exactly as M3-a builds a skewed
	// population.
	ids   []nodeID
	roles []int

	// states holds the table of every MEMBER. Non-members have none: a node
	// outside the membership under measurement is not running this mechanism.
	states map[int32]*m6NodeState
	// owners is the measured set, in a fixed order — the order a tick walks
	// them in, and therefore part of the result (§5.9.3).
	owners []int32

	online []bool
	// joined marks the nodes anybody has ever met.
	joined []bool

	// held is THE state of established connections: held[u] is what u believes
	// it is connected to, and nothing else answers that question.
	//
	// ⚠️ It replaced a counter beside the built adjacency, and the split was the
	// root of three separate defects. The counter said how many slots a node
	// held; the adjacency said who they were — and the two disagreed the moment
	// anything changed. A newcomer's edges in the built graph were "not visible"
	// in one place and "already paid for" in another, so a probe to a peer it had
	// never connected to counted as free, and detecting that peer's departure
	// freed a slot nobody had ever occupied.
	//
	// Being per-node rather than symmetric is deliberate: a release happens at
	// the END THAT DETECTED it (П-4), and the departed peer, being offline, goes
	// on believing it holds the edge.
	held []map[int32]struct{}
	// departedAt is when a node actually left. ⚠️ ANALYSIS ONLY: it exists to
	// measure the detection delay, and no path a node takes may read it — that
	// would be the free detection П-4 was rewritten to forbid.
	departedAt []int
	// churnSeen turns on the recovery accounting; clearedAt records when the
	// "from scratch" control wiped the tables, or -1.
	churnSeen bool
	clearedAt int

	tick int
	// returning[t] lists nodes due back at tick t.
	returning map[int][]int32
	queue     []m6Pending
	// reserve is the identifier reserve newcomers are drawn from, and reserveAt
	// the next unused one.
	reserve   []int32
	reserveAt int

	// answersByNode and answersByPair are the two halves of the ONE rate-limit
	// definition of §5.1.2 п. 4, both counted AT THE RESPONDER: answers per
	// responder per tick, and answers per (responder, asker, level) per tick.
	answersByNode map[int32]int
	answersByPair map[[3]int32]int

	report *m6ModelReport
}

// neighboursOf is branch A's pool: the connections the node believes it holds,
// in a deterministic order. Incoming links count — the node knows who dialled it.
//
// ⚠️ It reads `held` and nothing else. That is the whole of the fix for three
// separate defects: a newcomer's `held` starts empty, so its edges in the built
// graph are invisible at both ends without a flag to remember; a released peer
// leaves `held` on detection, so the pool drops it without a second list; and the
// budget is `len(held)`, so "who do I know" and "how many slots do I hold" can no
// longer disagree.
func (n *m6Network) neighboursOf(owner int32) []int32 {
	pool := make([]int32, 0, len(n.held[owner]))
	for peer := range n.held[owner] {
		pool = append(pool, peer)
	}
	// Map order is not an order. Two runs of one seed must offer the same
	// candidates in the same sequence, or every probe after the first diverges.
	sort.Slice(pool, func(a, b int) bool { return pool[a] < pool[b] })
	return pool
}

// --- the report --------------------------------------------------------------------

// m6LevelCoverage is one level summarised over the measured nodes, with the two
// coverage numbers П-4 requires kept apart.
type m6LevelCoverage struct {
	// Claimed is what the ONLINE owners believe they hold; Actual is how many of
	// those records point at a node that is still online.
	//
	// ⚠️ The gap between them IS the measurement of "how long a node lives with
	// a dead record", and local repair sees only the first. Merging them would
	// delete the cost of not detecting.
	Claimed int
	Actual  int
	Slots   int

	// RetainedOffline is what owners who are themselves OFFLINE still hold.
	//
	// ⚠️ Kept out of Claimed and Actual on purpose. A departed node's table is
	// memory, not coverage: counting it as "actually available" mixes what the
	// working network can route through with what an absent node happens to
	// remember. It is still worth a number — it is what a return brings back
	// (П-6) — so it is reported, apart.
	RetainedOffline int

	// Population is how many nodes belong in this level, summed over the ONLINE
	// owners and counting only peers that are themselves online.
	//
	// ⚠️ ANALYTICAL: computed over the whole population, unavailable to any node.
	// Without it an empty near level reads as a failure of discovery where there
	// is simply nobody to find (П-5). ⚠️ And ONLINE, not merely joined: after
	// churn a level whose inhabitants have left is not a level a node failed to
	// cover.
	Population int
	// PopulationJoined is the same count over everyone who has ever joined,
	// departed nodes included — the population the level HAD.
	PopulationJoined int
}

// m6ModelReport is one measured scenario.
type m6ModelReport struct {
	Config m6ModelConfig

	// Levels is the per-level coverage at the end of the run, and LevelsAtChurn
	// the same thing at the moment the first departure landed — PER LEVEL in
	// both cases, because a share and a near/far aggregate summarise two
	// different holes identically while §3.3′ is a statement about individual
	// levels.
	Levels        []m6LevelCoverage
	LevelsAtChurn []m6LevelCoverage
	// ClaimedBefore and ActualBefore are the totals of LevelsAtChurn, kept as
	// their own fields because the gap between them is the headline of П-4.
	ClaimedBefore, ActualBefore int

	// Probes and ProbesAfterChurn are the cost of the MEASURED population.
	//
	// ⚠️ Measured, not physical, and the distinction became load-bearing the
	// moment every node of the network started running the mechanism: an
	// unmeasured node's probes are real, but putting them in the same ledger as
	// the Q half's coverage would price one population's work against another's
	// result. Physical totals live in their own fields below.
	Probes           probeLedger
	ProbesAfterChurn probeLedger

	// PhysicalProbes, PhysicalLost and PhysicalDetections are the WHOLE network,
	// measured population included. They exist so the two can be read apart:
	// what the half cost, and what the network around it was doing while the half
	// was measured.
	PhysicalProbes     probeLedger
	PhysicalLost       int
	PhysicalDetections int

	// StrangersUnreachable counts probes that found NOBODY where the owner held
	// neither an edge nor a record — an offer from A′, C or the omniscient source
	// that happened to name an offline node.
	//
	// ⚠️ Its own number, because it is NOT a loss: nothing was held, so nothing
	// is released, shelved or recovered. Counting it as a loss used to invent a
	// release, and the next successful probe of the same candidate then "restored"
	// a connection that had never existed.
	StrangersUnreachable         int
	PhysicalStrangersUnreachable int
	// ReserveMeasured counts reserve nodes inside the membership, ReserveSize the
	// reserve as a whole, and OnlineFromReserve how many of them had joined by
	// the end.
	//
	// ⚠️ OnlineFromPopulation is kept BESIDE the total rather than derived from
	// it: the report has to be able to say "as many nodes as before, but not the
	// same ones", and a single total cannot.
	ReserveMeasured, ReserveSize, OnlineFromReserve int
	OnlineFromPopulation                            int

	LostByLevel     []int
	RefilledByLevel []int
	FilledElsewhere int

	// DetectionDelays is, per detected loss, the ticks from the departure to the
	// detection. ⚠️ A MEASURED quantity, never a parameter (П-4).
	DetectionDelays []int
	// ShelfHits counts records taken back off the shelf and refilled, ShelfExpired
	// those dropped after T_stale.
	ShelfHits, ShelfExpired int

	// Departed, ArrivalsOffered, ArrivalsAdmitted and GaveUpJoining are the four
	// numbers §5.9.2 insists are shown apart, plus the online count per tick.
	//
	// ArrivalsNotOffered counts the arrivals compensation could not even OFFER,
	// one per arrival rather than one per tick, because the identifier reserve
	// ran out. ⚠️ A different finding from an offer refused at the door, and
	// invisible without its own counter: both leave the population short, and
	// only one of the two is about the network.
	//
	// ⚠️ A non-zero value means THE SCENARIO WAS UNDER-PROVISIONED — the run did
	// not play the churn it was asked to play — and the report says so in those
	// words rather than leaving a reader to infer it from a shortfall.
	Departed, ArrivalsOffered, ArrivalsAdmitted, GaveUpJoining int
	ArrivalsNotOffered                                         int
	// ReturnsOffered and NewcomersOffered split ArrivalsOffered.
	//
	// ⚠️ They are printed apart because their totals are NOT the exact
	// compensation §5.9.2 describes: a return is a promise made at the departure
	// and is honoured even when the tick had fewer departures than returns due,
	// so the offered total is max(departures, returns due). Summing them into one
	// "offered" would read as exact compensation and would be wrong.
	ReturnsOffered, NewcomersOffered int
	// DeparturesDecided counts the EXOGENOUS decisions to leave — the draws the
	// seed determines, whether or not the node was online to act on them.
	//
	// ⚠️ It is reported beside Departed because the two answer different
	// questions: what the load ASKED the network to do, and what the network
	// actually did. Under background churn the decision sequence is identical in
	// every run of one seed; the realised departures are conditional on who was
	// online, and that difference is a result rather than a defect.
	//
	// ⚠️ IT COUNTS THE ORIGINAL POPULATION, and the decisions drawn over the
	// newcomer reserve are counted apart. A draw is made for every identifier the
	// run carries, and the reserve is sized for config.Ticks — so folding the two
	// together made the reported LOAD grow with the length of the run while the
	// network it described did not change. The realised churn was right; the
	// figure standing beside it was not.
	DeparturesDecided int
	// DeparturesDecidedInReserve is the same draw over the reserve identifiers,
	// joined and unjoined alike. Its denominator is ReserveSize, printed with it,
	// because the reserve is an artefact of the run's length rather than a
	// population the network has.
	DeparturesDecidedInReserve int
	OnlineByTick               []int

	// ReturnedWithATable, ReturnedEmptyHanded and NewcomersAdmitted are the
	// boundary §5.9.2 draws between an honest recovery and a free one, counted
	// AT THE MOMENT OF ADMISSION.
	//
	// ⚠️ A returning node kept the table it never lost — it was offline, not
	// wiped — while a newcomer arrives with nothing. Measured here rather than
	// argued, because "the return keeps its memory" is the single property that
	// separates П-6's main mode from its control, and a model that quietly reset
	// a returning table would look identical in every other number.
	ReturnedWithATable, ReturnedEmptyHanded, NewcomersAdmitted int

	// Members is how many nodes of the ORIGINAL population the membership holds.
	//
	// ⚠️ The un-joined reserve is NOT in it. Counting it made a fully compensated
	// network read as a fraction of "all participants", and worse, made the
	// denominator depend on the requested length of the run — the reserve is
	// sized for the ticks. A node from the reserve counts once it has actually
	// joined, and then it is reported as an arrival, which is a different line.
	Members int

	// PoolAtStart is S(u) under the BRANCH-A definition, measured on the built
	// graph before a single tick: a property of the network, and therefore the
	// one §5.1.1 says the near-level border must be derived from.
	//
	// ⚠️ It is kept apart from PoolByOwner on purpose. For A′ and C the pool at
	// the END is a RESULT OF THE MECHANISM — it grew because records were
	// exchanged — and deriving a border from it would move the border with the
	// branch, so the two populations and the four branches would be compared at
	// different levels.
	PoolAtStart []int
	// PoolPotential is what a node COULD have learned if every exchange and every
	// addressed answer had handed over everything the other side held: the union
	// of the neighbours' tables, ignoring `m`, `n` and the level asked for.
	//
	// ⚠️ ANALYTICAL, and kept only as an upper shape next to the measured pool.
	// It used to be reported AS S(u), which credited a node with knowledge no
	// mechanism had given it.
	PoolPotential []int
	// PoolByOwner is S(u): the DISTINCT nodes the branch could offer this owner,
	// deduplicated and excluding the owner. ⚠️ Analytical, and for A′ and C it
	// is a result of the mechanism rather than a property of the network.
	PoolByOwner []int

	// AddressedAnswers counts the branch-C requests a responder answered — an
	// EMPTY answer included, because it cost the responder a slot of its limit.
	//
	// ⚠️ The two refusal counters are different questions and are never merged.
	// AddressedRateLimited counts refusals AT A RESPONDER — the load the limit
	// actually shed, which is what §5.1.2 bounds. AddressedRefused counts
	// REQUESTS that ended with nobody answering — the availability cost. A
	// request refused by one responder and then served by another appears in the
	// first and not in the second, and reporting only one of the two would
	// either hide the limit doing its job or make it look like an outage.
	AddressedAnswers, AddressedRateLimited, AddressedRefused int
	// ExchangesDone counts A′ exchanges actually performed.
	ExchangesDone int
}

func medianOf(values []int) (int, bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	return sorted[len(sorted)/2], true
}

// PoolSummary renders S(u). ⚠️ Median and tails, never a mean: §5.1.1 asks for
// the distribution because the border of the near levels is derived from it, and
// a mean over a skewed pool would move that border.
func (r m6ModelReport) PoolSummary() string {
	median, ok := medianOf(r.PoolByOwner)
	if !ok {
		return "no data"
	}
	sorted := append([]int(nil), r.PoolByOwner...)
	sort.Ints(sorted)
	line := fmt.Sprintf("S(u) over %d owners, MEASURED from what was actually offered: min %d, "+
		"median %d, p90 %d, max %d (analysis — counted over the whole population, no node can "+
		"compute it about itself)",
		len(sorted), sorted[0], median, sorted[int(0.9*float64(len(sorted)-1))],
		sorted[len(sorted)-1])
	if potential, ok := medianOf(r.PoolPotential); ok {
		line += fmt.Sprintf("\n    potential pool, median %d — what the branch COULD have handed "+
			"over with no m/n and no level filter; ANALYTICAL and NOT S(u)", potential)
	}
	return line
}

// DetectionDelaySummary is the measured lag of П-4.
func (r m6ModelReport) DetectionDelaySummary() string {
	median, ok := medianOf(r.DetectionDelays)
	if !ok {
		// ⚠️ C = ∞ switches off the SCHEDULED refresh, not detection, and what
		// that implies is BRANCH-DEPENDENT: §5.1.0 charges a probe for a record a
		// neighbour hands back, so in A′ and C an empty result is a measurement
		// and not a property of the control. Saying "therefore no detection" for
		// every branch was the report contradicting the contract.
		repeats := r.Config.Branch.ExchangesRecords()
		switch {
		case r.Config.Cadence <= 0 && repeats:
			return "no loss was detected in this run. ⚠️ With C = ∞ the scheduled refresh is off, " +
				"but a record this branch is handed again is still a paid probe and COULD have " +
				"detected one: this is a MEASUREMENT, not a property of the negative control"
		case r.Config.Cadence <= 0:
			return "no data — with C = ∞ the scheduled refresh is off and this branch re-probes a " +
				"held record by no other path, so no loss can be detected; the expected result of " +
				"the negative control, not a missing measurement"
		}
		return "no data — no loss was detected in this run"
	}
	sorted := append([]int(nil), r.DetectionDelays...)
	sort.Ints(sorted)
	return fmt.Sprintf("%d losses detected: min %d, median %d, max %d ticks from departure to "+
		"detection", len(sorted), sorted[0], median, sorted[len(sorted)-1])
}

// CoverageLine is the two coverage numbers side by side, per level, at the end
// of the run.
func (r m6ModelReport) CoverageLine() string {
	return coverageLineOf(r.Levels, "nothing was measured")
}

// coverageLineOf renders one snapshot. ⚠️ Claimed and actually-alive are printed
// TOGETHER and never merged: their gap is what a node lives with while a loss is
// undetected, and local repair sees only the first of the two.
func coverageLineOf(levels []m6LevelCoverage, empty string) string {
	if len(levels) == 0 {
		return "no data (" + empty + ")"
	}
	parts := make([]string, 0, len(levels))
	claimed, actual, slots, retained := 0, 0, 0, 0
	for level, cover := range levels {
		parts = append(parts, fmt.Sprintf("L%d:%d/%d of %d (pop %d online of %d joined)",
			level, cover.Claimed, cover.Actual, cover.Slots, cover.Population,
			cover.PopulationJoined))
		claimed += cover.Claimed
		actual += cover.Actual
		slots += cover.Slots
		retained += cover.RetainedOffline
	}
	if slots == 0 {
		return "no data (" + empty + ")"
	}
	return fmt.Sprintf(
		"ONLINE owners: claimed %d/%d = %.1f%%, ACTUALLY ALIVE %d/%d = %.1f%% (the gap is what the "+
			"absence of detection costs); offline owners still hold %d records — memory, NOT "+
			"coverage\n    by level, claimed/alive of slots: %s",
		claimed, slots, float64(claimed)/float64(slots)*100,
		actual, slots, float64(actual)/float64(slots)*100, retained, strings.Join(parts, " "))
}

// RecoveryLine reports the third axis. ⚠️ Per level: filling a level that lost
// nothing is coverage, not recovery, and totals cannot tell the two apart.
func (r m6ModelReport) RecoveryLine() string {
	lost, refilled := sumOf(r.LostByLevel), sumOf(r.RefilledByLevel)
	if lost == 0 {
		// ⚠️ Branch-dependent, like the detection line and the configuration
		// signature: C = ∞ switches off the SCHEDULED refresh, and in A′ and C a
		// record a neighbour hands back is still a paid probe that may find it
		// dead (§5.1.0, §6.7). Saying "detection is off" here contradicted the
		// contract in the one line a reader checks when the axis is empty.
		repeats := r.Config.Branch.ExchangesRecords()
		switch {
		case r.Config.Cadence <= 0 && r.Config.Churn != churnNone && repeats:
			return "nothing was DETECTED as lost in this run. ⚠️ With C = ∞ the scheduled refresh " +
				"is off, but a repeat handed back by a neighbour is a paid probe and COULD have " +
				"detected a loss: the empty axis is a MEASUREMENT, not a property of the control"
		case r.Config.Cadence <= 0 && r.Config.Churn != churnNone:
			return "no data (nothing was DETECTED as lost — with C = ∞ this branch re-probes a " +
				"held record by no other path, so the recovery axis degenerates; the expected " +
				"result of the negative control)"
		}
		return "no data (nothing was lost)"
	}
	short := make([]string, 0, len(r.LostByLevel))
	for level, was := range r.LostByLevel {
		if gap := was - r.RefilledByLevel[level]; gap > 0 {
			short = append(short, fmt.Sprintf("L%d:%d", level, gap))
		}
	}
	line := fmt.Sprintf("recovered %d of %d detected losses", refilled, lost)
	if len(short) > 0 {
		line += ", STILL SHORT at " + strings.Join(short, " ")
	}
	if r.FilledElsewhere > 0 {
		line += fmt.Sprintf("; %d slots filled at levels that lost nothing — coverage, NOT recovery",
			r.FilledElsewhere)
	}
	return line + fmt.Sprintf("\n    shelf: %d records reused, %d expired after T_stale; "+
		"recovery cost %s", r.ShelfHits, r.ShelfExpired, r.ProbesAfterChurn)
}

// PopulationLine is the three numbers §5.9.2 forbids merging.
func (r m6ModelReport) PopulationLine() string {
	final := 0
	if len(r.OnlineByTick) > 0 {
		final = r.OnlineByTick[len(r.OnlineByTick)-1]
	}
	// ⚠️ The denominator is the MEMBERSHIP under measurement, not the shape's N.
	// On the Q half those differ by about a factor of two, and printing N there
	// would invent half a network of missing nodes; under compensated load the
	// held-back reserve would do the same again.
	// ⚠️ TWO POPULATIONS IN ONE LINE, AND EACH NUMBER STANDS BESIDE ITS OWN.
	// Every churn counter here is PHYSICAL — a departure of an unmeasured node is
	// a real departure, and the draw is made for every identifier the run carries
	// — while the online counts come from the MEASURED membership. Printing the
	// physical load against the measured denominator attributed the whole
	// network's churn to the Q half: "45 decided of 507" where the 45 were drawn
	// over 1000. The halves are labelled and each carries its own denominator.
	//
	// ⚠️ The denominator of the online half is the ORIGINAL measured population,
	// and so is the NUMERATOR beside it. Printing the combined online count against that
	// denominator said "%d of %d in the original population" about a figure that
	// included newcomers: a run that lost a third of its nodes and replaced them
	// read as a run that had lost nothing. Three numbers, named separately.
	line := fmt.Sprintf(
		"load on the WHOLE PHYSICAL network: %d departures decided over its %d original nodes "+
			"(%d more drawn over a reserve of %d, which is sized for the run's length and is NOT "+
			"network load), %d actually left; %d arrivals OFFERED (%d returns honoured as "+
			"promised at their departure, %d newcomers from the compensation quota — ⚠️ their sum "+
			"is max(departures, returns due), NOT exact compensation), %d admitted, %d gave up "+
			"after the entry queue; "+
			"online at the end, MEASURED: %d of the %d ORIGINAL %s population + %d admitted from a reserve "+
			"of %d = %d in total — ⚠️ the total holding steady is NOT the original composition "+
			"holding steady",
		r.DeparturesDecided, r.Config.Shape.nodes, r.DeparturesDecidedInReserve, r.ReserveSize,
		r.Departed, r.ArrivalsOffered, r.ReturnsOffered,
		r.NewcomersOffered, r.ArrivalsAdmitted, r.GaveUpJoining,
		r.OnlineFromPopulation, r.Members, r.Config.Membership, r.OnlineFromReserve,
		r.ReserveMeasured, final)
	if r.ArrivalsNotOffered > 0 {
		line += fmt.Sprintf("\n    ⚠️ SCENARIO INCOMPLETE: %d arrivals could not be OFFERED at "+
			"all — the identifier reserve ran out, so the run did not play the churn it was "+
			"asked to play", r.ArrivalsNotOffered)
	}
	return line
}

func (r m6ModelReport) String() string {
	addressed := ""
	if r.Config.Branch == branchC {
		addressed = fmt.Sprintf("\n  addressed: %d answers given (empty ones included), %d "+
			"refusals at a responder by the rate limit, %d requests that nobody answered",
			r.AddressedAnswers, r.AddressedRateLimited, r.AddressedRefused)
	}
	exchanges := ""
	if r.Config.Branch.ExchangesRecords() {
		exchanges = fmt.Sprintf("\n  exchanges: %d performed", r.ExchangesDone)
	}
	return fmt.Sprintf(
		"%s\n  at the churn moment: %s\n  coverage (final):    %s\n  cost (MEASURED population): "+
			"%s%s%s\n  cost (whole physical network, measured included): %s; %d losses detected "+
			"altogether\n  unreachable STRANGERS (no edge, no record — not losses): %d measured, "+
			"%d physical\n  detection: %s\n  recovery: %s\n  population: %s\n  pool: %s\n  %s",
		r.Config, coverageLineOf(r.LevelsAtChurn, "at the churn moment nothing had been lost yet"),
		r.CoverageLine(), r.Probes, exchanges, addressed,
		r.PhysicalProbes, r.PhysicalDetections,
		r.StrangersUnreachable, r.PhysicalStrangersUnreachable,
		r.DetectionDelaySummary(), r.RecoveryLine(), r.PopulationLine(), r.PoolSummary(),
		m6StandAssumptions)
}
