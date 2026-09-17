package overlaysim

// m6_test.go is the M6 instrument of
// docs/refactoring/dht/21-m6-bucket-discovery-measurement.md — bucket coverage,
// the cost of filling a slot, and recovery after churn.
//
// ⚠️ THE DISCOVERY MECHANISM IS NOT IMPLEMENTED HERE, AND THAT IS THE POINT.
// 19-bucket-discovery.md §3 lists four candidates for it and says the choice is
// not made. A model that "just fills buckets" would implement one of them, and
// the numbers would then describe a guess of mine rather than the comparison M6
// exists for. So the candidate stream ARRIVES AS AN INPUT (candidateSource) and
// this file only counts what it costs.
//
// The probe outcome is arithmetic, not policy: does the candidate lie on the
// level being filled, is it reachable, has it room under B, is the bucket
// already full, is it already known. Nothing else.
//
// ⚠️ NO NODE SEES THE SIMULATION'S IDENTIFIERS. A source that may offer anybody
// exists (omniscientCandidates) and is labelled a CONTROL: it says what perfect
// knowledge buys with everything else held fixed, and is never reported as a
// mechanism — nor as a mathematical upper bound, which would be a claim about
// every possible mechanism and needs a proof this stand does not have.
//
// ⚠️ Instrument only. O-M6-1…O-M6-7 of the contract are open; no threshold is
// proposed and a working instrument is not a passed M6.

import (
	"fmt"
	"sort"
	"strings"
)

// m6ModelVersion is recorded with every measurement. "v0-draft" says out loud
// that the contract behind it is not agreed yet.
const m6ModelVersion = "m6/v0-draft"

// --- axis 1: coverage ---------------------------------------------------------

// bucketCoverage is one node's table: how many slots of each level are taken.
type bucketCoverage struct {
	// Levels is d — levels 0…d-1, the geometry of the M1 model §2.2.
	Levels int
	// Capacity is k, slots per level. ⚠️ An INPUT (O-M6-2): the M1 model has one
	// neighbour per level, Kademlia has k.
	Capacity int
	// NearFrom splits near from far levels. ⚠️ An INPUT (O-M6-5): §3.3′ demands
	// the split and gives no threshold, so picking one here would be fitting the
	// zone to the result.
	NearFrom int
	// Held counts the members of each level.
	Held []int
}

func newBucketCoverage(levels, capacity, nearFrom int) bucketCoverage {
	return bucketCoverage{
		Levels:   levels,
		Capacity: capacity,
		NearFrom: nearFrom,
		Held:     make([]int, levels),
	}
}

func (c bucketCoverage) slots() int { return c.Levels * c.Capacity }

func (c bucketCoverage) filled() int {
	total := 0
	for _, held := range c.Held {
		total += held
	}
	return total
}

// Empty and UnderFilled are reported separately from the share: a level with one
// member of four is not the same finding as a level with none, and an average
// hides both.
func (c bucketCoverage) Empty() int {
	count := 0
	for _, held := range c.Held {
		if held == 0 {
			count++
		}
	}
	return count
}

func (c bucketCoverage) UnderFilled() int {
	count := 0
	for _, held := range c.Held {
		if held > 0 && held < c.Capacity {
			count++
		}
	}
	return count
}

// Share is filled slots over all slots. ⚠️ "no data" when there are no slots at
// all — a table of zero levels is not a fully covered table.
func (c bucketCoverage) Share() string {
	if c.slots() == 0 {
		return "no data"
	}
	return fmt.Sprintf("%d/%d = %.1f%%", c.filled(), c.slots(),
		float64(c.filled())/float64(c.slots())*100)
}

// halves is the near/far split §3.3′ asks for: organic knowledge is expected to
// cover far levels well and near ones badly, and one number would hide exactly
// the zone the measurement exists for.
func (c bucketCoverage) halves() (near, far bucketCoverage) {
	near = bucketCoverage{Capacity: c.Capacity, NearFrom: c.NearFrom}
	far = bucketCoverage{Capacity: c.Capacity, NearFrom: c.NearFrom}
	for level, held := range c.Held {
		if level >= c.NearFrom {
			near.Levels++
			near.Held = append(near.Held, held)
			continue
		}
		far.Levels++
		far.Held = append(far.Held, held)
	}
	return near, far
}

// perLevel is the coverage the task actually asks for: level → held/capacity.
//
// ⚠️ Shares and near/far aggregates are summaries, and two different holes
// inside one zone summarise identically — "3/4 near" says nothing about WHICH
// level is empty, while §3.3′ is a statement about individual levels.
func (c bucketCoverage) perLevel() string {
	if c.Levels == 0 {
		return "no data"
	}
	parts := make([]string, 0, c.Levels)
	for level, held := range c.Held {
		parts = append(parts, fmt.Sprintf("L%d:%d/%d", level, held, c.Capacity))
	}
	return strings.Join(parts, " ")
}

func (c bucketCoverage) String() string {
	if c.Levels == 0 {
		return "no data"
	}
	near, far := c.halves()
	return fmt.Sprintf("%s, empty levels %d, under-filled %d (near ≥%d: %s, far: %s)\n    by level: %s",
		c.Share(), c.Empty(), c.UnderFilled(), c.NearFrom, near.Share(), far.Share(), c.perLevel())
}

// --- axis 2: cost -------------------------------------------------------------

// m6Outcome is what one attempt to fill a slot ended in. The first five are
// PROBES — a connection was attempted; the last two are not, and they stay out
// of the cost denominator because no connection was spent on them.
type m6Outcome int

const (
	m6SlotFilled m6Outcome = iota
	m6CandidateUnreachable
	m6CandidateAtBudget
	// m6OwnerAtBudget — the OWNER has no room. It only arises where a probe
	// would re-establish a connection the owner had released after detecting the
	// peer gone: an ordinary fill stores a record and costs no slot (П-2), while
	// bringing a released edge back does. ⚠️ Kept apart from m6CandidateAtBudget
	// because WHICH END was full points at a different fix, and a merged counter
	// would blame the network for the node's own ceiling.
	m6OwnerAtBudget
	m6BucketFull
	m6AlreadyKnown
	// m6NoCandidate — the source offered nobody. Not a probe.
	m6NoCandidate
	// m6ProbeBudgetSpent — the node's probe allowance is used up. Not a probe.
	m6ProbeBudgetSpent
)

func (o m6Outcome) String() string {
	switch o {
	case m6SlotFilled:
		return "slot filled"
	case m6CandidateUnreachable:
		return "candidate unreachable"
	case m6CandidateAtBudget:
		return "candidate at B"
	case m6OwnerAtBudget:
		return "owner at B (re-connect refused)"
	case m6BucketFull:
		return "bucket full"
	case m6AlreadyKnown:
		return "already known"
	case m6NoCandidate:
		return "no candidate offered"
	default:
		return "probe budget spent"
	}
}

// isProbe says whether a connection was actually attempted.
func (o m6Outcome) isProbe() bool { return o <= m6AlreadyKnown }

// probeLedger is axis 2: what the filled slots cost.
type probeLedger struct {
	Outcomes map[m6Outcome]int
}

func newProbeLedger() probeLedger { return probeLedger{Outcomes: map[m6Outcome]int{}} }

func (l probeLedger) Probes() int {
	total := 0
	for outcome, count := range l.Outcomes {
		if outcome.isProbe() {
			total += count
		}
	}
	return total
}

func (l probeLedger) Filled() int { return l.Outcomes[m6SlotFilled] }

// PerFilledSlot is the M6 cost figure.
//
// ⚠️ "no data" when nothing was filled. Neither 0 nor ∞ would be honest: probes
// were spent and no slot came of it, and that reading must survive into the
// report as a result rather than as a suspicious number.
func (l probeLedger) PerFilledSlot() string {
	if l.Filled() == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.2f probes/slot (%d probes, %d filled)",
		float64(l.Probes())/float64(l.Filled()), l.Probes(), l.Filled())
}

func (l probeLedger) breakdown() string {
	if len(l.Outcomes) == 0 {
		return "no data"
	}
	kinds := make([]int, 0, len(l.Outcomes))
	for outcome := range l.Outcomes {
		kinds = append(kinds, int(outcome))
	}
	sort.Ints(kinds)

	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s:%d", m6Outcome(kind), l.Outcomes[m6Outcome(kind)]))
	}
	return strings.Join(parts, " ")
}

func (l probeLedger) String() string {
	return fmt.Sprintf("%s; outcomes %s", l.PerFilledSlot(), l.breakdown())
}

func (l probeLedger) add(outcome m6Outcome) { l.Outcomes[outcome]++ }

// --- the table under measurement ----------------------------------------------

// m6Table is one node's buckets. It holds node indices, never a view of the
// simulation: what may enter it comes from a candidateSource.
type m6Table struct {
	Owner    int32
	Coverage bucketCoverage
	members  []map[int32]struct{}
}

func newM6Table(owner int32, levels, capacity, nearFrom int) *m6Table {
	table := &m6Table{
		Owner:    owner,
		Coverage: newBucketCoverage(levels, capacity, nearFrom),
		members:  make([]map[int32]struct{}, levels),
	}
	for i := range table.members {
		table.members[i] = map[int32]struct{}{}
	}
	return table
}

func (t *m6Table) holds(node int32) bool {
	for _, level := range t.members {
		if _, ok := level[node]; ok {
			return true
		}
	}
	return false
}

// levelOf is the bucket a candidate belongs in: the first bit where the two
// identifiers differ. Returns -1 when the candidate is outside the table's
// levels — identical prefixes longer than d have nowhere to go.
func levelOf(owner, candidate nodeID, levels int) int {
	for level := range levels {
		if bitAt(owner, level) != bitAt(candidate, level) {
			return level
		}
	}
	return -1
}

// drop removes a departed node and reports WHICH levels it emptied a slot in.
// The levels, not the count: recovery is a per-level question.
func (t *m6Table) drop(node int32) []int {
	var lost []int
	for level, members := range t.members {
		if _, ok := members[node]; !ok {
			continue
		}
		delete(members, node)
		t.Coverage.Held[level]--
		lost = append(lost, level)
	}
	return lost
}

// --- candidate sources ---------------------------------------------------------

// candidateSource is the undecided mechanism of 19 §3, kept OUTSIDE the model.
// Comparing the variants later means comparing implementations of this
// interface with everything else unchanged.
type candidateSource interface {
	// Next offers the next candidate for `owner`, or false when it has nobody.
	Next(owner int32) (int32, bool)
	// Describe goes into the report: a cost figure without its candidate stream
	// cannot be compared with anything.
	Describe() string
}

// scriptedCandidates offers exactly what the scenario says, in order. The
// fixtures use it, because a fixture whose candidate stream is computed by a
// policy tests the policy.
//
// ⚠️ The queue is CONSUMED as the scenario runs, so the source cannot describe
// itself afterwards — hence the snapshot taken at construction. A name is not an
// input: two scenarios called "organic" with different queues would otherwise be
// indistinguishable in the report.
type scriptedCandidates struct {
	Name     string
	Queue    map[int32][]int32
	snapshot string
}

// newScriptedCandidates freezes the stream into the description before anything
// consumes it.
func newScriptedCandidates(name string, queue map[int32][]int32) *scriptedCandidates {
	owners := make([]int, 0, len(queue))
	for owner := range queue {
		owners = append(owners, int(owner))
	}
	sort.Ints(owners)

	parts := make([]string, 0, len(owners))
	for _, owner := range owners {
		offered := queue[int32(owner)]
		ids := make([]string, 0, len(offered))
		for _, candidate := range offered {
			ids = append(ids, fmt.Sprint(candidate))
		}
		parts = append(parts, fmt.Sprintf("%d←[%s]", owner, strings.Join(ids, " ")))
	}
	stream := strings.Join(parts, " ")
	if stream == "" {
		stream = "empty"
	}

	copied := make(map[int32][]int32, len(queue))
	for owner, offered := range queue {
		copied[owner] = append([]int32(nil), offered...)
	}
	return &scriptedCandidates{Name: name, Queue: copied, snapshot: stream}
}

func (s *scriptedCandidates) Next(owner int32) (int32, bool) {
	queue := s.Queue[owner]
	if len(queue) == 0 {
		return -1, false
	}
	s.Queue[owner] = queue[1:]
	return queue[0], true
}

func (s *scriptedCandidates) Describe() string {
	return fmt.Sprintf("scripted candidate stream %q, as given: %s", s.Name, s.snapshot)
}

// omniscientCandidates may offer ANY node of the simulation.
//
// ⚠️ CONTROL ONLY. No mechanism of 19 §3 can do this — the node is handed
// knowledge it has no way to acquire — so the number it produces is a CONTROL
// RESULT UNDER THE STATED CONSTRAINTS and nothing more.
//
// ⚠️ It is NOT called a mathematical upper bound, and the difference is not
// pedantry. An upper bound is a claim that no mechanism can do better, and that
// claim would need a proof this stand does not have: the control is bounded by
// the same k, the same B, the same probe ceiling and the same graph, and a
// mechanism that changed any of those could land outside it. What the control
// does say is what perfect knowledge buys WITH EVERYTHING ELSE HELD FIXED.
type omniscientCandidates struct {
	Nodes int
	next  map[int32]int
}

func newOmniscientCandidates(nodes int) *omniscientCandidates {
	return &omniscientCandidates{Nodes: nodes, next: map[int32]int{}}
}

func (o *omniscientCandidates) Next(owner int32) (int32, bool) {
	for o.next[owner] < o.Nodes {
		candidate := int32(o.next[owner])
		o.next[owner]++
		if candidate != owner {
			return candidate, true
		}
	}
	return -1, false
}

func (o *omniscientCandidates) Describe() string {
	return fmt.Sprintf("CONTROL — omniscient source over all %d simulated nodes; no discovery "+
		"mechanism of 19 §3 can do this, so this is a CONTROL RESULT UNDER THE STATED "+
		"CONSTRAINTS (same k, same B, same probe ceiling, same graph) and NOT a variant. ⚠️ It is "+
		"not claimed to be a mathematical upper bound: that would need a proof this stand does "+
		"not have", o.Nodes)
}

// --- the scenario --------------------------------------------------------------

type m6EventKind int

const (
	// m6Offer — the owner takes one candidate from the source and probes it.
	m6Offer m6EventKind = iota
	// m6Depart — a node leaves the network.
	m6Depart
	// m6Arrive — a node (re)joins.
	m6Arrive
)

func (k m6EventKind) String() string {
	switch k {
	case m6Offer:
		return "offer"
	case m6Depart:
		return "depart"
	default:
		return "arrive"
	}
}

// m6Event is one step of the reproducible scenario. The whole sequence is
// recorded in the report: churn that is not written down cannot be compared
// between the full graph and the Q half.
type m6Event struct {
	Kind m6EventKind
	// Node is the owner for an offer, the departing or arriving node otherwise.
	Node int32
}

func (e m6Event) String() string { return fmt.Sprintf("%s(%d)", e.Kind, e.Node) }

// m6Setup records every input of a measurement.
type m6Setup struct {
	Shape    shape
	Seed     uint64
	Policy   policy
	Quota    int
	Capacity int
	NearFrom int
	// ProbeBudget caps probes per owner. ⚠️ An input (O-M6-4 keeps company with
	// it: the repair rate ceiling of 19 §5 is not chosen either).
	ProbeBudget int
	// Membership names which half is being measured, for the report.
	Membership string
	Events     []m6Event
}

func (s m6Setup) String() string {
	steps := make([]string, 0, len(s.Events))
	for _, event := range s.Events {
		steps = append(steps, event.String())
	}
	sequence := strings.Join(steps, " ")
	if sequence == "" {
		sequence = "none"
	}
	return fmt.Sprintf(
		"model %s, shape %s (N=%d, d=%d, B=%d), seed %d, policy %s, quota %d, bucket capacity %d, "+
			"near from level %d, probe budget %d, membership %s\n  churn: %s",
		m6ModelVersion, s.Shape.name, s.Shape.nodes, s.Shape.degree, s.Shape.budget, s.Seed,
		s.Policy, s.Quota, s.Capacity, s.NearFrom, s.ProbeBudget, s.Membership, sequence)
}

// m6Offered is one entry of the offer trace: which candidate the source produced
// at which event, and what the probe ended in.
//
// ⚠️ It exists because the candidate stream is an INPUT and a consumed queue
// cannot be recovered from the result. Without the trace two scenarios differing
// only in their stream produce reports that cannot be told apart.
type m6Offered struct {
	Event     int
	Candidate int32
	Outcome   m6Outcome
}

func (o m6Offered) String() string {
	if o.Candidate < 0 {
		return fmt.Sprintf("#%d:—/%s", o.Event, o.Outcome)
	}
	return fmt.Sprintf("#%d:%d/%s", o.Event, o.Candidate, o.Outcome)
}

// m6Report is one measured scenario.
type m6Report struct {
	Setup  m6Setup
	Source string
	// Offers is the trace of what the source actually produced, bound to the
	// event that asked for it.
	Offers []m6Offered

	Probes probeLedger
	// Before, AfterChurn and Final are axis 3: coverage at the three moments
	// that make recovery readable.
	Before, AfterChurn, Final bucketCoverage

	// LostByLevel and RefilledByLevel are the recovery accounting.
	//
	// ⚠️ PER LEVEL, not in totals. A bucket table is not a pile of slots: losing
	// level 0 and filling a previously empty level 7 leaves the node exactly as
	// unable to route as it was, and a comparison of totals would call that
	// "recovered". Only a fill at a level that is still short of what it lost
	// counts as recovery.
	LostByLevel     []int
	RefilledByLevel []int
	// FilledElsewhere counts fills after the churn at levels that lost nothing.
	// They are real coverage and NOT recovery, so they are reported apart.
	FilledElsewhere int
	// ProbesAfterChurn is the part of the cost spent after the first departure.
	ProbesAfterChurn probeLedger
}

func sumOf(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

// LostSlots and Refilled are the totals, derived from the per-level accounting
// rather than counted alongside it — two counters of the same thing drift.
func (r m6Report) LostSlots() int { return sumOf(r.LostByLevel) }
func (r m6Report) Refilled() int  { return sumOf(r.RefilledByLevel) }

// RecoveryComplete is a RESULT, not a precondition: an unfinished recovery stays
// in the table and says which LEVEL is still missing.
func (r m6Report) RecoveryComplete() bool {
	if r.LostSlots() == 0 {
		return false
	}
	for level, lost := range r.LostByLevel {
		if r.RefilledByLevel[level] < lost {
			return false
		}
	}
	return true
}

// outstanding lists the levels that have not recovered what they lost.
func (r m6Report) outstanding() string {
	parts := make([]string, 0, len(r.LostByLevel))
	for level, lost := range r.LostByLevel {
		if short := lost - r.RefilledByLevel[level]; short > 0 {
			parts = append(parts, fmt.Sprintf("L%d:%d", level, short))
		}
	}
	return strings.Join(parts, " ")
}

func (r m6Report) recoveryLine() string {
	if r.LostSlots() == 0 {
		return "no data (nothing was lost)"
	}
	status := fmt.Sprintf("recovered %d of %d lost slots", r.Refilled(), r.LostSlots())
	if short := r.outstanding(); short != "" {
		status += fmt.Sprintf(", STILL SHORT at %s", short)
	}
	if r.FilledElsewhere > 0 {
		status += fmt.Sprintf("; %d slots filled at levels that lost nothing — coverage, NOT "+
			"recovery", r.FilledElsewhere)
	}
	return fmt.Sprintf("%s; recovery cost %s", status, r.ProbesAfterChurn)
}

func (r m6Report) offerTrace() string {
	if len(r.Offers) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(r.Offers))
	for _, offer := range r.Offers {
		parts = append(parts, offer.String())
	}
	return strings.Join(parts, " ")
}

func (r m6Report) String() string {
	return fmt.Sprintf(
		"%s\n  source: %s\n  offers: %s\n  coverage before churn: %s\n  after churn:           %s\n"+
			"  final:                 %s\n  cost:     %s\n  recovery: %s",
		r.Setup, r.Source, r.offerTrace(), r.Before, r.AfterChurn, r.Final,
		r.Probes, r.recoveryLine())
}

// runBucketScenario plays one scripted scenario against one owner's table.
//
// `inGraph` selects the membership — the whole network or the structural half —
// so the two can be compared under IDENTICAL conditions: same population, same
// events, same candidate stream.
//
// ⚠️ Recovery starts from the SURVIVING table. There is no branch that rebuilds
// it: a node that starts over is not a node that recovered, and the difference
// is the measurement.
func runBucketScenario(
	g *graph, setup m6Setup, owner int32, inGraph func(int32) bool, source candidateSource,
) (m6Report, error) {
	if int(owner) >= len(g.ids) {
		return m6Report{}, fmt.Errorf("owner %d is outside the graph of %d nodes",
			owner, len(g.ids))
	}
	if setup.Capacity < 1 {
		return m6Report{}, fmt.Errorf("bucket capacity %d: a table with no slots measures nothing",
			setup.Capacity)
	}

	levels := setup.Shape.degree
	table := newM6Table(owner, levels, setup.Capacity, setup.NearFrom)

	// The event sequence is copied into the report: an input the caller can edit
	// afterwards is not a record of what was measured.
	setup.Events = append([]m6Event(nil), setup.Events...)
	report := m6Report{
		Setup:            setup,
		Source:           source.Describe(),
		Probes:           newProbeLedger(),
		ProbesAfterChurn: newProbeLedger(),
		LostByLevel:      make([]int, levels),
		RefilledByLevel:  make([]int, levels),
	}

	// online is the liveness of the scenario. ⚠️ An INPUT: the M1 model has no
	// notion of a node being up, so it is declared by the events and nothing
	// else.
	online := make([]bool, len(g.ids))
	for i := range online {
		online[i] = true
	}

	probesSpent := 0
	churnSeen := false

	for index, event := range setup.Events {
		switch event.Kind {
		case m6Depart:
			// Before is taken BEFORE the first departure is applied; AfterChurn
			// follows the LAST one. Everything filled from the first departure
			// onwards is examined for recovery — per level.
			if !churnSeen {
				churnSeen = true
				report.Before = copyCoverage(table.Coverage)
			}
			online[event.Node] = false
			for _, level := range table.drop(event.Node) {
				report.LostByLevel[level]++
			}
			report.AfterChurn = copyCoverage(table.Coverage)
			continue

		case m6Arrive:
			online[event.Node] = true
			continue
		}

		if event.Node != owner {
			return m6Report{}, fmt.Errorf("offer addressed to node %d, the table belongs to %d",
				event.Node, owner)
		}

		outcome, candidate, level := probeOnce(g, table, setup, inGraph, source, online, &probesSpent)
		report.Probes.add(outcome)
		report.Offers = append(report.Offers,
			m6Offered{Event: index, Candidate: candidate, Outcome: outcome})

		if !churnSeen {
			continue
		}
		report.ProbesAfterChurn.add(outcome)
		if outcome != m6SlotFilled {
			continue
		}
		// ⚠️ A fill is recovery only where something was lost. Filling a level
		// that never lost anything is coverage, and calling it recovery would
		// let a node "recover" level 0 by filling level 7.
		if level >= 0 && report.RefilledByLevel[level] < report.LostByLevel[level] {
			report.RefilledByLevel[level]++
			continue
		}
		report.FilledElsewhere++
	}

	if !churnSeen {
		report.Before = copyCoverage(table.Coverage)
		report.AfterChurn = copyCoverage(table.Coverage)
	}
	report.Final = copyCoverage(table.Coverage)
	return report, nil
}

// classifyProbe is the WHOLE arithmetic of one probe, and the only copy of it.
//
// ⚠️ It is shared by the scripted single-owner scenario below and by the
// tick-driven network of m6_model_test.go on purpose. Two engines with two
// copies of "what does this probe mean" is how the two would come to disagree
// about a filled slot, and every number of all three axes is denominated in
// probes.
//
// The world arrives as identifiers plus three functions rather than as a graph:
// the identifier space is WIDER than the built graph in the network model, which
// draws its newcomers beyond the population. The scripted
// scenario reads liveness from its event list and budget from the adjacency,
// while the network holds both itself and releases budget on DETECTION (П-4).
// Neither may impose its own answer on the other.
//
// It returns the outcome and the level that was filled (-1 when none was).
func classifyProbe(
	ids []nodeID, table *m6Table, candidate int32,
	online func(int32) bool, heldBudget func(int32) int, budget int,
) (m6Outcome, int) {
	if !online(candidate) {
		return m6CandidateUnreachable, -1
	}
	if heldBudget(candidate) >= budget {
		return m6CandidateAtBudget, -1
	}
	if table.holds(candidate) {
		return m6AlreadyKnown, -1
	}

	level := levelOf(ids[table.Owner], ids[candidate], table.Coverage.Levels)
	if level < 0 {
		// Shares the whole prefix the table can address: nowhere to put it.
		return m6BucketFull, -1
	}
	if table.Coverage.Held[level] >= table.Coverage.Capacity {
		return m6BucketFull, -1
	}

	table.members[level][candidate] = struct{}{}
	table.Coverage.Held[level]++
	return m6SlotFilled, level
}

// probeOnce is one attempt to fill a slot in the SCRIPTED scenario.
//
// It returns the outcome, the candidate that was probed (-1 when none was) and
// the level that was filled (-1 when none was) — the caller needs all three to
// keep the trace and the per-level recovery accounting.
func probeOnce(
	g *graph, table *m6Table, setup m6Setup, inGraph func(int32) bool,
	source candidateSource, online []bool, probesSpent *int,
) (m6Outcome, int32, int) {
	if setup.ProbeBudget > 0 && *probesSpent >= setup.ProbeBudget {
		return m6ProbeBudgetSpent, -1, -1
	}

	candidate, ok := source.Next(table.Owner)
	for ok && !inGraph(candidate) {
		// A candidate outside the membership under measurement is not a
		// candidate here; it costs nothing, because no connection was made.
		candidate, ok = source.Next(table.Owner)
	}
	if !ok {
		return m6NoCandidate, -1, -1
	}

	// From here a connection is attempted, so every path below is a probe.
	*probesSpent++

	outcome, level := classifyProbe(g.ids, table, candidate,
		func(node int32) bool { return online[node] },
		func(node int32) int { return len(g.adjacency[node]) },
		setup.Shape.budget)
	return outcome, candidate, level
}

// copyCoverage freezes a snapshot. Without the copy every "before" and "after"
// in the report would alias the live table and show the same final numbers —
// a recovery measurement that cannot see what was lost.
func copyCoverage(c bucketCoverage) bucketCoverage {
	copied := c
	copied.Held = append([]int(nil), c.Held...)
	return copied
}
