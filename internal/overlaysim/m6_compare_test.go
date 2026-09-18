package overlaysim

// m6_compare_test.go is the automatic comparability check for a pair of runs
// — the main mode against its ‘from scratch’ control, adaptive or replayed.
//
// It steps both networks in LOCKSTEP and checks named CLAIMS over event
// sequences, tick by tick: the phase each tick belongs to, the departure
// decisions, the realised churn, the held edges of every node, the source's
// exposure at the churn onset, and the offers themselves. When a claim stops
// holding the result says which one, at which tick, and — for the offers —
// where the streams first parted (tick, owner, source, candidate) and WHY:
// the owner's local memory, a responder's table, or network state that had
// already changed.
//
// ⚠️ The claims are deliberately separate, because they are different
// statements and only some of them are expected to hold:
//
//	same pool at the onset      ≠   same sequence of offers;
//	same recorded stream        ≠   same choice of candidate under memory;
//	same departure decisions    ≠   same realised departures.
//
// A comparison that folded them into one verdict would either fail on the
// difference being measured (memory changes the offers — that is the point)
// or pass on a difference nobody asked for.

import (
	"fmt"
	"reflect"
	"strings"
)

// m6ClaimKind names one statement of the comparison.
type m6ClaimKind int

const (
	// claimSameInputs — the two configurations differ in the memory mode and
	// the replayed boundaries and in nothing else, on the same graph.
	claimSameInputs m6ClaimKind = iota
	// claimSameBoundaries — every tick is in the same phase in both runs, and
	// the runs end on the same tick.
	claimSameBoundaries
	// claimSameDecisions — the departure DECISIONS of every tick are the same.
	claimSameDecisions
	// claimSameRealisedChurn — the realised departures, the returns due and
	// the newcomers offered of every tick are the same.
	claimSameRealisedChurn
	// claimSameHeldEdges — every node holds the same edges at the end of every
	// tick (network state has not drifted).
	claimSameHeldEdges
	// claimSamePoolAtOnset — adaptive pairs: the POOL half of every owner's
	// exposure (held edges the owner may take) is the same at the onset. ⚠️ A
	// statement about the source's input, not about the offers.
	claimSamePoolAtOnset
	// claimSameResponderTablesAtOnset — adaptive A′/C pairs: what each
	// neighbour would hand over from its table at the onset is the same.
	claimSameResponderTablesAtOnset
	// claimSameRecordedStream — replay pairs: both halves read one recording
	// and neither modified it.
	claimSameRecordedStream
	// claimOffersSameBeforeOnset — the offer sequences are identical up to the
	// tick the control clears.
	claimOffersSameBeforeOnset
	// claimOffersSameAfterOnset — the offer sequences stay identical after the
	// onset. ⚠️ EXPECTED TO FAIL for a control worth having: the failure
	// carries the first divergence and its cause.
	claimOffersSameAfterOnset
)

func (k m6ClaimKind) String() string {
	switch k {
	case claimSameInputs:
		return "same inputs (configuration and graph)"
	case claimSameBoundaries:
		return "same phase boundaries"
	case claimSameDecisions:
		return "same departure decisions"
	case claimSameRealisedChurn:
		return "same realised churn (departures, returns due, newcomers offered)"
	case claimSameHeldEdges:
		return "same held edges at every node"
	case claimSamePoolAtOnset:
		return "same pool at the onset (the world half of the exposure: held edges for a branch, the joined population for the omniscient control)"
	case claimSameResponderTablesAtOnset:
		return "same responder tables at the onset (what neighbours would hand over)"
	case claimSameRecordedStream:
		return "same recorded stream, unmodified by either run"
	case claimOffersSameBeforeOnset:
		return "offers identical before the onset"
	default:
		return "offers identical after the onset"
	}
}

// m6ClaimStatus is what the comparison found for one claim.
type m6ClaimStatus int

const (
	claimHolds m6ClaimStatus = iota
	claimFails
	// claimNotApplicable — the claim does not describe this pair (a replay has
	// no responder tables; an adaptive pair has no recording).
	claimNotApplicable
)

func (s m6ClaimStatus) String() string {
	switch s {
	case claimHolds:
		return "HOLDS"
	case claimFails:
		return "NO LONGER HOLDS"
	default:
		return "not applicable"
	}
}

// m6Claim is one statement with its verdict and, when it fails, where.
type m6Claim struct {
	Kind   m6ClaimKind
	Status m6ClaimStatus
	// Tick is the first tick the claim failed on.
	Tick   int
	Detail string
}

func (c m6Claim) String() string {
	switch c.Status {
	case claimFails:
		return fmt.Sprintf("%s: %s from tick %d — %s", c.Kind, c.Status, c.Tick, c.Detail)
	case claimNotApplicable:
		return fmt.Sprintf("%s: %s — %s", c.Kind, c.Status, c.Detail)
	}
	return fmt.Sprintf("%s: %s", c.Kind, c.Status)
}

// m6DivergenceCause classifies the first offer divergence.
type m6DivergenceCause int

const (
	// causeLocalMemory — the owner's own table, shelf, cadence clock, exchange
	// stamps or tried-set let a different candidate through, from the same
	// input.
	causeLocalMemory m6DivergenceCause = iota
	// causeResponderTable — another node's table handed over different
	// records (A′ exchange, C addressed answer, or the queue they fill).
	causeResponderTable
	// causeNetworkState — the world had already changed: the candidate is in
	// one run's held edges and not the other's, or the owner exists in one run
	// only (admissions differed).
	causeNetworkState
	// causeUndetermined — the live state does not settle it: what a responder
	// would have handed over in the other run is not observable after the
	// fact, so the comparison says so rather than guessing.
	causeUndetermined
)

func (c m6DivergenceCause) String() string {
	switch c {
	case causeLocalMemory:
		return "LOCAL MEMORY of the owner"
	case causeResponderTable:
		return "a RESPONDER'S TABLE (another node's memory)"
	case causeNetworkState:
		return "NETWORK STATE that had already changed"
	default:
		return "NOT ESTABLISHED from the live state"
	}
}

// m6WorldPair is both runs' snapshots for one tick.
type m6WorldPair struct {
	main, control m6WorldSnapshot
}

// worldsBeforeServing is the one place the comparison snapshots: after
// prepareTick on both runs, before serveTick on either.
func worldsBeforeServing(main, control *m6Network) m6WorldPair {
	return m6WorldPair{main: snapshotWorldOf(main), control: snapshotWorldOf(control)}
}

// m6WorldSnapshot is a run at the moment serving starts: who was online and
// which edges every node held (world), and what each node's §5.1.0 queue held
// (memory, kept so a queue choice can be judged against the queue it was made
// from). The cause of a divergence is judged against it, because by the end
// of the tick the serving itself has moved the edges (a detection releases
// one) and the state would blame the world for what the owner's own probes
// did.
type m6WorldSnapshot struct {
	online []bool
	held   []map[int32]struct{}
	queues map[int32][]int32
}

func snapshotWorldOf(n *m6Network) m6WorldSnapshot {
	snapshot := m6WorldSnapshot{
		online: append([]bool(nil), n.online...),
		held:   make([]map[int32]struct{}, len(n.held)),
		queues: make(map[int32][]int32, len(n.states)),
	}
	for node, edges := range n.held {
		copied := make(map[int32]struct{}, len(edges))
		for peer := range edges {
			copied[peer] = struct{}{}
		}
		snapshot.held[node] = copied
	}
	for node, state := range n.states {
		snapshot.queues[node] = append([]int32(nil), state.Offered...)
	}
	return snapshot
}

// m6Availability is what a run can say about the SOURCE of an offer another
// run made — reachability in the world, never the content of an answer.
type m6Availability int

const (
	// availableInWorld — the source is world state (a held edge, the joined
	// population, a responder online and connected) and it was there at the
	// start of the tick.
	availableInWorld m6Availability = iota
	// missingFromWorld — the source is world state and it was NOT there.
	missingFromWorld
	// memoryOnly — the source is the owner's memory (a held record, the
	// shelf, a consumed stream entry, a responder reachable only through the
	// table): its absence is memory, not the world.
	memoryOnly
)

func (a m6Availability) String() string {
	switch a {
	case availableInWorld:
		return "available in the world"
	case missingFromWorld:
		return "MISSING from the world"
	default:
		return "a matter of memory (not the world)"
	}
}

// availableIn says what the given run can say about the source of an offer
// the other run made for `owner`, against the world as it stood at the start
// of the tick.
//
// ⚠️ The distinction that matters is WORLD against MEMORY. A refresh needs a
// record in the table and a shelf offer a shelved record — both are memory,
// so their absence in the other run is the memory being measured and never a
// reason to blame the network. An acquaintance needs a held edge and the
// omniscient walk a joined member — both are world, and their absence means
// the world had changed. An exchange or an addressed request needs its
// RESPONDER: online and connected is world; reachable only through the
// asker's table (addressed) is memory. ⚠️ What the responder would have
// ANSWERED is a different question, not answered here — see
// contentFromResponder.
func availableIn(n *m6Network, world m6WorldSnapshot, owner int32, offer *m6OfferEntry) m6Availability {
	switch offer.Source {
	case offerRefresh, offerShelf, offerQueue:
		return memoryOnly
	case offerAcquaintance, offerOmniscient:
		if n.config.Stream != nil {
			// The recording is the world of a replay; every entry of it was
			// available to both halves alike, and whether it was consumed is
			// behaviour.
			return memoryOnly
		}
		var there bool
		if offer.Source == offerAcquaintance {
			_, there = world.held[owner][offer.Peer]
		} else {
			there = n.joined[offer.Peer]
		}
		if there && n.mayTake(owner, offer.Peer) {
			return availableInWorld
		}
		return missingFromWorld
	default: // offerExchange, offerAddressed: the responder as a node
		if !world.online[offer.Peer] || n.states[offer.Peer] == nil {
			return missingFromWorld
		}
		if _, edge := world.held[owner][offer.Peer]; edge {
			return availableInWorld
		}
		if offer.Source == offerExchange {
			// An exchange goes over a held edge and nothing else.
			return missingFromWorld
		}
		// An addressed request can also go to a node known from the table.
		return memoryOnly
	}
}

// contentFromResponder says whether what the offer DELIVERED came from another
// node's table — an exchange, an addressed answer, or (under an adaptive
// branch) the queue they fill — so that a divergence involving it cannot be
// settled from the live state: the answer the other run would have received
// is not observable after the fact.
func contentFromResponder(n *m6Network, offer *m6OfferEntry) bool {
	switch offer.Source {
	case offerExchange, offerAddressed:
		return true
	case offerQueue:
		return n.config.Stream == nil
	default:
		return false
	}
}

// causeFromAvailability folds the findings into one cause: the world is
// blamed when a world source was missing; nothing is concluded when a
// responder's content is involved (its answer in the other run is not
// observable); memory is blamed only when every source involved was there.
func causeFromAvailability(contentUnknown bool, findings ...m6Availability) m6DivergenceCause {
	for _, finding := range findings {
		if finding == missingFromWorld {
			return causeNetworkState
		}
	}
	if contentUnknown {
		return causeUndetermined
	}
	return causeLocalMemory
}

// m6Divergence is the first offer on which the two runs part.
type m6Divergence struct {
	Tick  int
	Owner int32
	// Main and Control are the two offers at the same position of the tick's
	// sequence; one may be absent when one run made more offers than the
	// other.
	Main, Control *m6OfferEntry
	Cause         m6DivergenceCause
	Detail        string
}

func (d m6Divergence) String() string {
	render := func(offer *m6OfferEntry) string {
		if offer == nil {
			return "<no offer>"
		}
		return fmt.Sprintf("%s → %d (level %d)", offer.Source, offer.Peer, offer.Level)
	}
	return fmt.Sprintf("tick %d, owner %d: main %s | control %s; cause: %s — %s",
		d.Tick, d.Owner, render(d.Main), render(d.Control), d.Cause, d.Detail)
}

// m6Comparison is the result of one paired run.
type m6Comparison struct {
	Claims []m6Claim
	// Divergence is the earliest offer divergence of any owner; FirstByCause
	// the earliest of each cause, because the first owner to diverge does not
	// always diverge for the most telling reason. Offers are compared PER
	// OWNER, so a shifted sequence of one owner does not read as a divergence
	// of every owner served after it. ⚠️ Neither stops the checking: the
	// interval claims are judged on every tick to the end.
	Divergence   *m6Divergence
	FirstByCause map[m6DivergenceCause]*m6Divergence
	// diverged marks the owners whose sequences have already parted.
	diverged map[int32]struct{}
	// OnsetTick is the onset the two halves SHARE — the tick both reached it
	// on — or, when they did not, the earlier of the two so that the offers
	// from there on are judged under the after-onset claim, which the
	// mismatch has already failed. -1 while neither half has had one.
	// MainOnset and ControlOnset are each half's own; the verdict reads both.
	OnsetTick               int
	MainOnset, ControlOnset int
	Ticks                   int
}

func (c *m6Comparison) claim(kind m6ClaimKind) *m6Claim {
	for index := range c.Claims {
		if c.Claims[index].Kind == kind {
			return &c.Claims[index]
		}
	}
	return nil
}

// Holds says whether a claim held for the whole run.
func (c *m6Comparison) Holds(kind m6ClaimKind) bool {
	claim := c.claim(kind)
	return claim != nil && claim.Status == claimHolds
}

func (c *m6Comparison) fail(kind m6ClaimKind, tick int, detail string) {
	claim := c.claim(kind)
	if claim.Status != claimHolds {
		return
	}
	claim.Status, claim.Tick, claim.Detail = claimFails, tick, detail
}

// noteOnset reads the onset off BOTH halves in the tick just served. Only an
// onset both halves reached on this same tick is a shared one, compared
// there and then; an onset one half reached and the other did not — or the
// two on different ticks — is a MISMATCH, failed on the claims judged at or
// after the onset with both ticks named, and never read as "no onset". The
// verdict is the same whichever half is passed first.
func (c *m6Comparison) noteOnset(main, control *m6Network, tick int) {
	mainNow, controlNow := main.trace.OnsetTick == tick, control.trace.OnsetTick == tick
	if !mainNow && !controlNow {
		return
	}
	if mainNow {
		c.MainOnset = tick
	}
	if controlNow {
		c.ControlOnset = tick
	}
	if c.OnsetTick < 0 {
		c.OnsetTick = tick
	}
	if mainNow && controlNow {
		c.checkExposureAtOnset(main, control)
		return
	}
	// One-sided on this tick: the other half either had its onset earlier
	// (then the mismatch is already recorded) or has not had one (yet, or
	// ever). Either way the halves do not share an onset.
	c.failOnsetMismatch(tick)
}

// failOnsetMismatch fails every claim that presupposes a shared onset, naming
// each half's own onset ("never" for a half that has not had one by now).
func (c *m6Comparison) failOnsetMismatch(tick int) {
	name := func(onset int) string {
		if onset < 0 {
			return "never"
		}
		return fmt.Sprintf("tick %d", onset)
	}
	detail := fmt.Sprintf("ONSET MISMATCH — the main run's onset: %s, the control's: %s; the halves "+
		"do not share an onset, so nothing at or after it is comparable (this is not the absence of "+
		"an onset)", name(c.MainOnset), name(c.ControlOnset))
	for _, kind := range []m6ClaimKind{claimSamePoolAtOnset, claimSameResponderTablesAtOnset,
		claimOffersSameAfterOnset, claimSameRecordedStream} {
		claim := c.claim(kind)
		switch {
		case claim.Status == claimHolds:
			c.fail(kind, tick, detail)
		case claim.Status == claimFails && strings.HasPrefix(claim.Detail, "ONSET MISMATCH"):
			// The other half's onset arrived later: the verdict names both
			// ticks, not "never" for a half that did have one.
			claim.Detail = detail
		}
	}
}

// noOnset sets aside the claims a pair without an onset never checked. The
// pool and the responder tables are compared AT the onset and the after-onset
// interval starts there; when no departure ever landed — no churn form, or a
// share that took nobody — none of that happened, and a claim left at its
// initial HOLDS would report a check that was not made. The offers of the
// interval actually played are judged under the before-onset claim (every
// tick routes there while the onset is unknown), and the recorded-stream
// claim is judged on its own — fingerprints and responders — whether or not
// an onset came.
func (c *m6Comparison) noOnset() {
	const why = "the onset never came (no departure landed in either run), so there is no onset " +
		"snapshot and no after-onset interval; the offers of the whole run are judged as ‘before " +
		"the onset’"
	for _, kind := range []m6ClaimKind{claimSamePoolAtOnset, claimSameResponderTablesAtOnset,
		claimOffersSameAfterOnset} {
		if c.claim(kind).Status == claimHolds {
			c.notApplicable(kind, why)
		}
	}
}

func (c *m6Comparison) notApplicable(kind m6ClaimKind, why string) {
	claim := c.claim(kind)
	claim.Status, claim.Detail = claimNotApplicable, why
}

// String renders the verdict claim by claim. ⚠️ A failed claim is named as
// the statement that no longer holds, so a reader knows exactly which part of
// "the same scenario, the same stream, different memory" the pair lost.
func (c *m6Comparison) String() string {
	onset := fmt.Sprintf("onset at tick %d", c.OnsetTick)
	switch {
	case c.MainOnset < 0 && c.ControlOnset < 0:
		onset = "NO ONSET in either half (no departure landed)"
	case c.MainOnset != c.ControlOnset:
		name := func(onset int) string {
			if onset < 0 {
				return "never"
			}
			return fmt.Sprintf("tick %d", onset)
		}
		onset = fmt.Sprintf("ONSET MISMATCH (main %s, control %s)", name(c.MainOnset), name(c.ControlOnset))
	}
	lines := []string{fmt.Sprintf("comparison over %d ticks, %s:", c.Ticks, onset)}
	for _, claim := range c.Claims {
		lines = append(lines, "    "+claim.String())
	}
	if c.Divergence == nil {
		lines = append(lines, "    the offer sequences never diverged")
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "    first offer divergence: "+c.Divergence.String())
	for cause := causeLocalMemory; cause <= causeUndetermined; cause++ {
		if first := c.FirstByCause[cause]; first != nil && first != c.Divergence {
			lines = append(lines, fmt.Sprintf("    first divergence caused by %s: %s", cause, first))
		}
	}
	lines = append(lines, fmt.Sprintf("    %d owners diverged in total", len(c.diverged)))
	return strings.Join(lines, "\n")
}

// --- the check -------------------------------------------------------------------------

// newM6Comparison is a verdict with every claim still standing.
func newM6Comparison() *m6Comparison {
	comparison := &m6Comparison{
		OnsetTick:    -1,
		MainOnset:    -1,
		ControlOnset: -1,
		FirstByCause: map[m6DivergenceCause]*m6Divergence{},
		diverged:     map[int32]struct{}{},
	}
	for kind := claimSameInputs; kind <= claimOffersSameAfterOnset; kind++ {
		comparison.Claims = append(comparison.Claims, m6Claim{Kind: kind, Status: claimHolds})
	}
	return comparison
}

// compareM6Runs plays two prepared networks in lockstep and returns the
// verdict. Both must be fresh (not yet run), on the same graph, with the offer
// trace on.
func compareM6Runs(main, control *m6Network) (*m6Comparison, error) {
	if main.g != control.g {
		return nil, fmt.Errorf("the two runs are on different graphs")
	}
	// ⚠️ ONE INDEX SPACE, or nothing. Every per-node check below indexes the
	// control's arrays by the main run's node indices; on one graph the two
	// still differ when their reserves do — compensated churn sizes the
	// reserve for the horizon — and the checks would then read past the end
	// of the shorter run instead of reporting anything. Refused up front, in
	// either order of the arguments.
	if err := sameIndexSpace(main, control); err != nil {
		return nil, err
	}
	if !main.config.TraceOffers || !control.config.TraceOffers {
		return nil, fmt.Errorf("both runs need TraceOffers: the comparison is over the offer sequences")
	}
	if len(main.report.OnlineByTick) > 0 || len(control.report.OnlineByTick) > 0 {
		return nil, fmt.Errorf("a comparison steps the runs itself; one of them has already been run")
	}

	comparison := newM6Comparison()
	comparison.checkInputs(main, control)
	comparison.prepareStreamClaims(main, control)

	// Both halves' recordings are fingerprinted before the run and checked
	// again after it: neither may have been modified.
	fingerprints := streamFingerprints(main, control)

	for tick := 0; ; tick++ {
		main.tick, control.tick = tick, tick
		offersBefore := len(main.trace.Offers)
		controlOffersBefore := len(control.trace.Offers)

		// ⚠️ The world is snapshotted AFTER the churn and the admissions of the
		// tick and BEFORE anybody serves: that is the state every offer of the
		// tick was made against. A snapshot taken before the tick lacks the
		// edge an admitted newcomer brought — identically in both runs — and
		// would blame the world for a choice the owner's memory made.
		if err := main.prepareTick(); err != nil {
			return nil, fmt.Errorf("main run, tick %d: %w", tick, err)
		}
		if err := control.prepareTick(); err != nil {
			return nil, fmt.Errorf("control run, tick %d: %w", tick, err)
		}
		worlds := worldsBeforeServing(main, control)
		mainDone := main.serveTick()
		controlDone := control.serveTick()
		comparison.Ticks = tick + 1

		comparison.noteOnset(main, control, tick)
		comparison.checkPhase(main, control, tick)
		comparison.checkScenario(main, control, tick)
		comparison.checkHeldEdges(main, control, tick)
		comparison.checkOffers(main, control, worlds, tick,
			main.trace.Offers[offersBefore:], control.trace.Offers[controlOffersBefore:])

		if mainDone || controlDone {
			if mainDone != controlDone {
				comparison.fail(claimSameBoundaries, tick, "one run ended and the other went on")
			}
			break
		}
	}

	if comparison.MainOnset < 0 && comparison.ControlOnset < 0 {
		comparison.noOnset()
	}
	if after := streamFingerprints(main, control); after != fingerprints {
		comparison.fail(claimSameRecordedStream, comparison.Ticks-1, "a recording was modified during the run")
	}
	// ⚠️ A replay that asked a responder has a second source beside the
	// recording, and the stream is no longer the one thing both halves read.
	for _, network := range []*m6Network{main, control} {
		if network.config.Stream != nil &&
			(network.report.ExchangesDone > 0 || network.report.AddressedAnswers > 0) {
			comparison.fail(claimSameRecordedStream, comparison.Ticks-1, fmt.Sprintf("a replay "+
				"performed %d exchanges and %d addressed answers beside the recording",
				network.report.ExchangesDone, network.report.AddressedAnswers))
		}
	}
	for _, network := range []*m6Network{main, control} {
		network.report.Phases = network.schedule.trace()
		network.collect()
	}
	return comparison, nil
}

// streamFingerprints digests both halves' recordings (empty for an adaptive
// half), so a modification of either shows.
func streamFingerprints(main, control *m6Network) [2]string {
	var prints [2]string
	for index, network := range []*m6Network{main, control} {
		if network.config.Stream != nil {
			prints[index] = network.config.Stream.fingerprint()
		}
	}
	return prints
}

// sameIndexSpace says whether the two runs name the same participants by the
// same indices — population and reserve alike.
func sameIndexSpace(main, control *m6Network) error {
	if len(main.ids) != len(control.ids) {
		return fmt.Errorf("the runs carry %d and %d identifiers (population and reserve): their "+
			"node indices are not the same space, and nothing per node can be compared",
			len(main.ids), len(control.ids))
	}
	for index, id := range main.ids {
		if id != control.ids[index] {
			return fmt.Errorf("index %d names a different identifier in the two runs: their node "+
				"indices are not the same space", index)
		}
	}
	return nil
}

// checkInputs compares the configurations field by field, allowing only the
// differences that define the pair. ⚠️ The index space itself is checked
// before the comparison starts (sameIndexSpace); here only what the same
// indices MEAN — the membership — is compared.
//
// ⚠️ Compared as VALUES, never through the printed configuration: the report
// rounds shares to two decimals, so ChurnShare 0.011 and 0.014 print alike,
// and the membership prints as its name while two different predicates can
// share one. The predicate is compared by what it says about every identifier
// the run carries, reserve included.
func (c *m6Comparison) checkInputs(main, control *m6Network) {
	left, right := main.config, control.config
	right.StartEmpty = left.StartEmpty
	right.ReplayPhases = left.ReplayPhases
	if !reflect.DeepEqual(left, right) {
		c.fail(claimSameInputs, 0, "the configurations differ beyond the memory mode and the replayed "+
			"boundaries (compared field by field, unrounded):\n"+left.String()+"\n-----\n"+right.String())
	}
	if left.Stream != right.Stream {
		c.fail(claimSameInputs, 0, "one run replays a recorded stream and the other does not, or "+
			"they replay different recordings")
	}
	for index, id := range main.ids {
		if main.member(id) != control.member(id) {
			c.fail(claimSameInputs, 0, fmt.Sprintf("the memberships differ at index %d although both "+
				"are named %q", index, left.Membership))
			return
		}
	}
}

// prepareStreamClaims marks the claims that do not describe this pair.
//
// ⚠️ The recorded-stream claim is judged on BOTH halves directly — the
// recording has to be present in both and identical in content — and never
// inferred from the main half alone. A replay compared with an adaptive run,
// or two replays of two recordings that agree only up to the onset, would
// otherwise keep the claim standing on the strength of the onset exposure
// and the main recording's immutability, while the sequences after the onset
// come from different sources. The inputs claim fails there too, but the
// claims are stated as independent and each has to be right on its own.
func (c *m6Comparison) prepareStreamClaims(main, control *m6Network) {
	mainStream, controlStream := main.config.Stream, control.config.Stream
	switch {
	case mainStream == nil && controlStream == nil:
		c.notApplicable(claimSameRecordedStream, "an adaptive pair replays nothing")
	case mainStream == nil || controlStream == nil:
		c.fail(claimSameRecordedStream, 0, "one half replays a recording and the other adapts: they "+
			"do not read one stream")
		c.notApplicable(claimSamePoolAtOnset, "a replay and an adaptive run have no common pool to compare")
		c.notApplicable(claimSameResponderTablesAtOnset, "a replay and an adaptive run have no common "+
			"responders to compare")
		return
	case mainStream.fingerprint() != controlStream.fingerprint():
		c.fail(claimSameRecordedStream, 0, "the two halves replay DIFFERENT recordings: same or not "+
			"up to the onset, their sequences after it come from different sources")
	}
	if mainStream != nil {
		c.notApplicable(claimSamePoolAtOnset, "a replay has no adaptive pool; see the recorded stream claim")
		c.notApplicable(claimSameResponderTablesAtOnset, "a replay consults no responder: the "+
			"recording carries their answers as recorded")
		return
	}
	if !main.config.Branch.ExchangesRecords() || main.config.OmniscientControl {
		c.notApplicable(claimSameResponderTablesAtOnset, "this source hands over no other node's records")
	}
}

func (c *m6Comparison) checkPhase(main, control *m6Network, tick int) {
	left, right := main.schedule.current(), control.schedule.current()
	if left.Phase != right.Phase || left.From != right.From {
		c.fail(claimSameBoundaries, tick, fmt.Sprintf("main is in %s (from %d), control in %s (from %d)",
			left.Phase, left.From, right.Phase, right.From))
	}
}

func (c *m6Comparison) checkScenario(main, control *m6Network, tick int) {
	left := main.trace.Scenario[len(main.trace.Scenario)-1]
	right := control.trace.Scenario[len(control.trace.Scenario)-1]
	if !equalNodes(left.Decided, right.Decided) {
		c.fail(claimSameDecisions, tick, fmt.Sprintf("%v against %v", left.Decided, right.Decided))
	}
	switch {
	case !equalNodes(left.Departed, right.Departed):
		c.fail(claimSameRealisedChurn, tick, fmt.Sprintf("realised departures %v against %v — a "+
			"decision landed on a node online in one run only", left.Departed, right.Departed))
	case !equalNodes(left.ReturnsDue, right.ReturnsDue):
		c.fail(claimSameRealisedChurn, tick, fmt.Sprintf("returns due %v against %v", left.ReturnsDue,
			right.ReturnsDue))
	case !equalNodes(left.NewcomersOffered, right.NewcomersOffered):
		c.fail(claimSameRealisedChurn, tick, fmt.Sprintf("newcomers offered %v against %v",
			left.NewcomersOffered, right.NewcomersOffered))
	}
}

func (c *m6Comparison) checkHeldEdges(main, control *m6Network, tick int) {
	if !c.Holds(claimSameHeldEdges) {
		return
	}
	for node := range main.held {
		if len(main.held[node]) != len(control.held[node]) {
			c.fail(claimSameHeldEdges, tick, fmt.Sprintf("node %d holds %d edges in the main run and "+
				"%d in the control", node, len(main.held[node]), len(control.held[node])))
			return
		}
		for peer := range main.held[node] {
			if _, both := control.held[node][peer]; !both {
				c.fail(claimSameHeldEdges, tick, fmt.Sprintf("node %d holds %d in the main run only",
					node, peer))
				return
			}
		}
	}
}

// checkExposureAtOnset compares every owner's exposure, half by half, in the
// tick the control cleared — after the clearing and before anybody served.
func (c *m6Comparison) checkExposureAtOnset(main, control *m6Network) {
	if main.config.Stream != nil {
		if main.trace.OnsetTick != control.trace.OnsetTick {
			c.fail(claimSameRecordedStream, main.tick, "the two halves reached the onset on different ticks")
		}
		// Availability at the onset: the same OWNERS, and for each the same
		// entries, unconsumed in both — the clearing must not have touched the
		// consumption state, and neither half may have recorded fewer owners.
		// ⚠️ All three parts of the divergence are read: a replay exposure has
		// only the recorded half, so a content difference lands in `tables`,
		// but a missing owner lands in `absent` and used to be dropped here.
		absent, world, differing := exposureDivergence(main.trace.ExposureAtOnset,
			control.trace.ExposureAtOnset)
		switch {
		case len(absent) > 0:
			c.fail(claimSameRecordedStream, main.tick, fmt.Sprintf("%d owners are missing from one "+
				"half's onset snapshot (not an empty stream — the owner is not there), first %d",
				len(absent), absent[0]))
		case len(world)+len(differing) > 0:
			differing = append(world, differing...)
			c.fail(claimSameRecordedStream, main.tick, fmt.Sprintf("%d owners have different entries "+
				"available at the onset, first %d", len(differing), differing[0]))
		}
		return
	}
	absent, world, tables := exposureDivergence(main.trace.ExposureAtOnset, control.trace.ExposureAtOnset)
	if len(absent) > 0 {
		c.fail(claimSamePoolAtOnset, main.tick, fmt.Sprintf("%d owners are missing from one half's "+
			"onset snapshot (not an empty pool — the owner is not there), first %d", len(absent),
			absent[0]))
	}
	if len(world) > 0 {
		c.fail(claimSamePoolAtOnset, main.tick, fmt.Sprintf("%d owners see a different pool, first %d",
			len(world), world[0]))
	}
	if len(tables) > 0 {
		c.fail(claimSameResponderTablesAtOnset, main.tick, fmt.Sprintf("%d of %d owners would be handed "+
			"different records by their neighbours, first owner %d", len(tables),
			len(main.trace.ExposureAtOnset), tables[0]))
	}
}

// checkOffers compares this tick's offers OWNER BY OWNER, position by
// position within each owner, and records every owner's first divergence with
// its cause, read off the live state.
func (c *m6Comparison) checkOffers(
	main, control *m6Network, worlds m6WorldPair, tick int, left, right []m6OfferEntry,
) {
	byOwner := func(offers []m6OfferEntry) (map[int32][]m6OfferEntry, []int32) {
		grouped := map[int32][]m6OfferEntry{}
		order := make([]int32, 0, 64)
		for _, offer := range offers {
			if _, seen := grouped[offer.Owner]; !seen {
				order = append(order, offer.Owner)
			}
			grouped[offer.Owner] = append(grouped[offer.Owner], offer)
		}
		return grouped, order
	}
	mains, order := byOwner(left)
	controls, controlOrder := byOwner(right)
	for _, owner := range controlOrder {
		if _, seen := mains[owner]; !seen {
			order = append(order, owner)
		}
	}

	// ⚠️ EVERY owner is compared on EVERY tick, whether or not it has diverged
	// before: the two interval claims (before / after the onset) are judged
	// independently, and an owner that parted before the onset and keeps
	// differing after it is a failure of the second claim too. Only the
	// diagnostic bookkeeping — the owner's FIRST divergence and its cause — is
	// taken once per owner.
	for _, owner := range order {
		one, other := mains[owner], controls[owner]
		for index := 0; index < len(one) || index < len(other); index++ {
			var mainOffer, controlOffer *m6OfferEntry
			if index < len(one) {
				mainOffer = &one[index]
			}
			if index < len(other) {
				controlOffer = &other[index]
			}
			if mainOffer != nil && controlOffer != nil && mainOffer.equal(*controlOffer) {
				continue
			}
			c.noteDivergence(main, control, worlds, tick, owner, mainOffer, controlOffer)
			break
		}
	}
}

// noteDivergence fails the interval claim this tick belongs to and, for an
// owner that has not diverged before, records its first divergence with the
// cause read off the live state.
func (c *m6Comparison) noteDivergence(
	main, control *m6Network, worlds m6WorldPair, tick int, owner int32,
	mainOffer, controlOffer *m6OfferEntry,
) {
	divergence := &m6Divergence{Tick: tick, Owner: owner, Main: mainOffer, Control: controlOffer}
	divergence.Cause, divergence.Detail = classifyDivergence(main, control, worlds, owner, mainOffer, controlOffer)

	kind := claimOffersSameAfterOnset
	if c.OnsetTick < 0 || tick < c.OnsetTick {
		kind = claimOffersSameBeforeOnset
	}
	c.fail(kind, tick, divergence.String())

	if _, already := c.diverged[owner]; already {
		return
	}
	c.diverged[owner] = struct{}{}
	if c.FirstByCause[divergence.Cause] == nil {
		c.FirstByCause[divergence.Cause] = divergence
	}
	if c.Divergence == nil {
		c.Divergence = divergence
	}
}

// classifyQueueChoice separates WHERE a queue record came from (a responder)
// from WHY a different one was taken: the level the owner was serving, the
// owner's tried-set, or the queue contents themselves.
//
// ⚠️ A queue divergence is the responder's table only when the queues were
// different. Two identical queues can still yield two different candidates —
// the owner asked for another level, or had already tried the other one this
// tick — and that is the owner's. When one candidate is absent from the other
// run's queue the origin is a responder, but whether it is that responder's
// table or the asker's exchange timing is not established from here.
func classifyQueueChoice(worlds m6WorldPair, owner int32, one, other *m6OfferEntry) (m6DivergenceCause, string) {
	if one.Level != other.Level {
		return causeLocalMemory, fmt.Sprintf("the owner served level %d in the main run and level "+
			"%d in the control, and took from its queue accordingly", one.Level, other.Level)
	}
	mainQueue, controlQueue := worlds.main.queues[owner], worlds.control.queues[owner]
	if equalNodes(mainQueue, controlQueue) {
		// ⚠️ IDENTICAL AS SEQUENCES, not as sets. The queue is walked in
		// order, so [a, b] and [b, a] yield different candidates under one and
		// the same filter — that is the order the answers arrived in, not the
		// owner's memory. Only an identical sequence leaves the owner's filters
		// as the sole explanation.
		return causeLocalMemory, "the queues were identical, in order, when serving began; the " +
			"owner's tried-set or table let a different one through"
	}
	return causeUndetermined, fmt.Sprintf("the queues differed when serving began (main's %d in the "+
		"control's queue: %v; control's %d in the main run's queue: %v; same contents in another "+
		"order: %v): the records came from responders, but whether a table, the order the "+
		"answers arrived in, or the asker's exchange timing differed is not established from here",
		one.Peer, containsNode(controlQueue, one.Peer), other.Peer, containsNode(mainQueue, other.Peer),
		sameNodesAnyOrder(mainQueue, controlQueue))
}

func sameNodesAnyOrder(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[int32]int, len(a))
	for _, node := range a {
		counts[node]++
	}
	for _, node := range b {
		counts[node]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

// classifyDivergence names the cause of the first divergence from the state
// of both networks at the end of the tick.
//
// ⚠️ The rules are conservative in one direction: a difference that CAN be
// explained by the owner's own memory is attributed to it only when the
// candidate involved is available to both runs — otherwise the world differs
// and the world is blamed. Exchange timing is the asker's memory (the stamps,
// assumption 27); what an exchange or an answer HANDS OVER is the responder's.
func classifyDivergence(
	main, control *m6Network, worlds m6WorldPair, owner int32, one, other *m6OfferEntry,
) (m6DivergenceCause, string) {
	replayed := main.config.Stream != nil
	switch {
	case one == nil || other == nil:
		// ⚠️ A live owner with nothing to offer is NOT enough for "memory": its
		// source may have gone — the edge to the one candidate released, the
		// record dropped. The offer the other run made is checked for
		// availability in this run before memory is blamed.
		present, absentIn, who := one, control, "control"
		if one == nil {
			present, absentIn, who = other, main, "main"
		}
		absentWorld := worlds.control
		if one == nil {
			absentWorld = worlds.main
		}
		if absentIn.states[owner] == nil || !absentIn.online[owner] {
			return causeNetworkState, fmt.Sprintf("owner %d is not serving in the %s run at all "+
				"(admissions or departures differed)", owner, who)
		}
		finding := availableIn(absentIn, absentWorld, owner, present)
		cause := causeFromAvailability(contentFromResponder(absentIn, present), finding)
		return cause, fmt.Sprintf("the %s run's owner %d made no offer here; the other run's %s %d "+
			"in the %s run at the start of the tick: %s", who, owner, present.Source, present.Peer,
			who, finding)

	case one.Source != other.Source:
		// Different paths from the same inputs is memory — provided each run's
		// offer was available in the other run's WORLD too.
		mainInControl := availableIn(control, worlds.control, owner, one)
		controlInMain := availableIn(main, worlds.main, owner, other)
		contentUnknown := contentFromResponder(control, one) || contentFromResponder(main, other)
		return causeFromAvailability(contentUnknown, mainInControl, controlInMain), fmt.Sprintf(
			"different sources (%s against %s); the main run's offer in the control's world: %s; "+
				"the control's offer in the main run's world: %s", one.Source, other.Source,
			mainInControl, controlInMain)
	}

	// Same source, different candidate or different records.
	switch one.Source {
	case offerRefresh, offerShelf:
		return causeLocalMemory, "the refresh cursor / table or the shelf of this owner differ"
	case offerAcquaintance, offerOmniscient, offerQueue:
		if replayed {
			// One recording feeds both: whichever entry was let through, the
			// input was the same, so the filter is the owner's.
			return causeLocalMemory, "both candidates come from the one recording; the owner's " +
				"table, tried-set or consumption let a different one through"
		}
		if one.Source == offerQueue {
			return classifyQueueChoice(worlds, owner, one, other)
		}
		mainInControl := availableIn(control, worlds.control, owner, one)
		controlInMain := availableIn(main, worlds.main, owner, other)
		if mainInControl == availableInWorld && controlInMain == availableInWorld {
			return causeLocalMemory, "both candidates are in both runs' pools at the start of the " +
				"tick; the owner's table (or tried-set) let a different one through"
		}
		return causeNetworkState, fmt.Sprintf("candidate %d in the control's world: %s; candidate "+
			"%d in the main run's world: %s", one.Peer, mainInControl, other.Peer, controlInMain)
	default: // offerExchange, offerAddressed
		if one.Peer != other.Peer {
			// Different responders: the divergence is the CHOICE of responder,
			// and the choice is settled by reachability alone — the content of
			// either answer is beside the point. Reachability is read off the
			// world snapshot, so it is established, not assumed.
			mainInControl := availableIn(control, worlds.control, owner, one)
			controlInMain := availableIn(main, worlds.main, owner, other)
			cause := causeFromAvailability(false, mainInControl, controlInMain)
			return cause, fmt.Sprintf("different responders asked (%d against %d); the main run's "+
				"responder in the control's world: %s; the control's responder in the main run's "+
				"world: %s", one.Peer, other.Peer, mainInControl, controlInMain)
		}
		if one.Level != other.Level {
			// ⚠️ The same responder was asked DIFFERENT QUESTIONS. An addressed
			// request names a level, and a cleared owner may well ask for
			// another one than the remembering owner did; two different answers
			// from one unchanged table — or two identical empty ones — say
			// nothing about the table. The choice of level is the owner's.
			return causeLocalMemory, fmt.Sprintf("the same responder %d was asked for level %d in "+
				"the main run and level %d in the control: the asker's choice of level differs, and "+
				"the answers cannot be compared as evidence about the responder's table",
				one.Peer, one.Level, other.Level)
		}
		return causeResponderTable, fmt.Sprintf("the same node %d, asked the same question (level "+
			"%d), handed over %v in the main run and %v in the control", one.Peer, one.Level,
			one.Handed, other.Handed)
	}
}
