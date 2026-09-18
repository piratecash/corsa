package overlaysim

// m6_schedule_test.go is the run schedule of the filling model: WHEN a tick
// belongs to which phase of §5.9.1, when churn begins, and when the run stops.
//
// Two schedules exist and the configuration names which one a run is under:
//
//	flat    — a fixed number of ticks with churn landing at a named tick. It is
//	          the schedule every mechanism fixture uses, because a fixture wants
//	          to know exactly which tick it is looking at. ⚠️ It is NOT the grid
//	          scenario, and a report under it says so.
//	phased  — F1 filling → F2 churn → F3 recovery → F4 cadence, with T_fill,
//	          T_idle, T_rec and T_cad and the early-stop rules of §5.9.1. The
//	          length of the run is then a RESULT: it depends on what the network
//	          did, and the report prints the boundary of every phase together
//	          with the reason it ended.
//
// ⚠️ A data-dependent schedule is a second thing two runs can differ in. The
// "from scratch" control of П-6 has nothing to recover, so its own F3 rule would
// stop early and its F4 — and every churn tick keyed on the tick number — would
// land elsewhere than the main run's. A control that plays a different scenario
// is not a control, so under the phased schedule it REPLAYS the main run's
// boundaries instead of deciding its own, and the constructor refuses a control
// without them.

import (
	"fmt"
	"strings"
)

// m6Phase names a phase of §5.9.1, plus the flat fixture schedule, which is one
// undivided stretch of ticks and is named so a report cannot pass it off as F1.
type m6Phase int

const (
	phaseFlat m6Phase = iota
	phaseFill
	phaseChurn
	phaseRecovery
	phaseCadence
)

func (p m6Phase) String() string {
	switch p {
	case phaseFlat:
		return "FLAT run (fixture schedule, not §5.9.1)"
	case phaseFill:
		return "F1 filling"
	case phaseChurn:
		return "F2 churn onset"
	case phaseRecovery:
		return "F3 recovery"
	default:
		return "F4 cadence"
	}
}

// m6StopReason is why a phase ended. It is a type and not a string because the
// reference tests assert on it, and a test asserting on prose is a test of the
// prose.
type m6StopReason int

const (
	// stopNotYet — the phase is still running.
	stopNotYet m6StopReason = iota
	// stopIdle — the watched quantity (coverage in F1, the per-level deficit in
	// F3) did not change for T_idle consecutive ticks.
	stopIdle
	// stopElapsed — the phase used its whole tick budget (T_fill, T_rec, or the
	// flat run's tick count).
	stopElapsed
	// stopByConstruction — F2 is one tick: the tick a shock lands on, or the
	// tick background churn begins and then continues through F3 and F4.
	stopByConstruction
	// stopWindowEnd — F4 ended because the common window T_cad ended. ⚠️ The
	// ONLY way out of F4: §5.9.1 gives that phase no early stop, so that every
	// value of C is measured over the same number of ticks.
	stopWindowEnd
	// stopReplayed — the boundary was taken from a pinned trace of another run
	// and not decided by this run's own data.
	stopReplayed
)

func (s m6StopReason) String() string {
	switch s {
	case stopNotYet:
		return "still running"
	case stopIdle:
		return "the watched quantity did not change for T_idle consecutive ticks"
	case stopElapsed:
		return "the phase's own tick budget ran out"
	case stopByConstruction:
		return "by construction — one onset tick"
	case stopWindowEnd:
		return "end of the common window T_cad (no early stop exists in this phase)"
	default:
		return "REPLAYED from the pinned boundaries of another run, not decided here"
	}
}

// m6PhasePlan is §5.9.1 as parameters: every one of them is a proposal of the
// contract, none is agreed, and all four are printed with every number.
type m6PhasePlan struct {
	// FillTicks is T_fill, the most F1 may last; IdleTicks is T_idle, the run of
	// unchanged ticks that ends F1 and F3 early; RecoveryTicks is T_rec, the
	// most F3 may last; CadenceTicks is T_cad, the common window of F4.
	FillTicks     int
	IdleTicks     int
	RecoveryTicks int
	CadenceTicks  int
}

// m6ContractPhases is the §5.9.1 proposal. ⚠️ Proposed, not agreed.
func m6ContractPhases() m6PhasePlan {
	return m6PhasePlan{FillTicks: 256, IdleTicks: 64, RecoveryTicks: 256, CadenceTicks: 1024}
}

func (p m6PhasePlan) validate() error {
	// ⚠️ Every threshold is a floor of one. Zero is not "disabled": with
	// T_idle = 0 the first tick of F1 already counts as an idle run, so the
	// phase would end after one tick and call it convergence; a zero budget
	// would play a phase of no ticks and report a boundary that isn't one.
	for _, value := range []struct {
		name  string
		ticks int
	}{
		{"T_fill", p.FillTicks}, {"T_idle", p.IdleTicks},
		{"T_rec", p.RecoveryTicks}, {"T_cad", p.CadenceTicks},
	} {
		if value.ticks < 1 {
			return fmt.Errorf("%s = %d: every phase parameter of §5.9.1 needs at least one tick, "+
				"a zero would end the phase before it measured anything", value.name, value.ticks)
		}
	}
	return nil
}

func (p m6PhasePlan) String() string {
	return fmt.Sprintf("phases §5.9.1 — T_fill=%d, T_idle=%d, T_rec=%d, T_cad=%d (proposed, not "+
		"agreed); the length of the run is a RESULT of the early-stop rules, printed per phase",
		p.FillTicks, p.IdleTicks, p.RecoveryTicks, p.CadenceTicks)
}

// m6PhaseBoundary is one phase's span, [From, To) in ticks. A slice of them is
// what a control REPLAYS instead of deciding its own.
type m6PhaseBoundary struct {
	Phase    m6Phase
	From, To int
}

// m6PhaseRecord is one played phase: where it was, why it ended, and what
// happened inside it — the measured population's probes and refreshes apart
// from the physical churn, as everywhere else in the report.
//
// ⚠️ The per-phase ledgers are what makes F4 a measurement rather than a tail:
// §5.9.1 gives every C the same window so the three cadence branches differ in
// exactly "how many refreshes fell inside the window and what they cost", and
// those two numbers have to be read off THIS phase and not off the whole run.
type m6PhaseRecord struct {
	Phase m6Phase
	// From and To bound the phase as [From, To) in ticks; To is set when it
	// ends.
	From, To int
	Stop     m6StopReason
	// IdleRun is how many consecutive unchanged ticks the phase had seen when it
	// ended — the evidence for stopIdle, and the counter-evidence otherwise.
	IdleRun int

	// Probes, Refreshes, RefreshesRefused, Detections and Refilled are the
	// MEASURED population's. Departed and Admitted are PHYSICAL, like every
	// churn counter of the report.
	//
	// ⚠️ Refreshes counts every refresh the cadence SCHEDULED, whatever the
	// probe then came to; RefreshesRefused is the subset the owner could not
	// even attempt because it sat at its ceiling B and the record had no
	// standing edge. Counting only completed refreshes under-reported F4's
	// attempts by exactly that subset while their cost was still on the ledger.
	Probes           probeLedger
	Refreshes        int
	RefreshesRefused int
	Detections       int
	Refilled         int
	Departed         int
	Admitted         int
}

func (r m6PhaseRecord) boundary() m6PhaseBoundary {
	return m6PhaseBoundary{Phase: r.Phase, From: r.From, To: r.To}
}

func (r m6PhaseRecord) Ticks() int { return r.To - r.From }

func (r m6PhaseRecord) String() string {
	span := fmt.Sprintf("ticks %d–%d (%d)", r.From, r.To-1, r.Ticks())
	if r.Ticks() == 1 {
		span = fmt.Sprintf("tick %d", r.From)
	}
	return fmt.Sprintf("%s: %s, stopped — %s (idle run %d); measured: %d probes, %d refreshes "+
		"scheduled (%d of them refused at the owner's ceiling B), %d detections, %d refilled; "+
		"physical: %d departed, %d admitted",
		r.Phase, span, r.Stop, r.IdleRun, r.Probes.Probes(), r.Refreshes, r.RefreshesRefused,
		r.Detections, r.Refilled, r.Departed, r.Admitted)
}

// m6Schedule is what the tick loop asks: how long at most, when churn begins,
// which record this tick books into, and whether the run is over.
type m6Schedule interface {
	// horizon is the most ticks the run can last, known before the first tick;
	// the newcomer reserve is sized for it.
	horizon() int
	// churnOnset is the tick churn begins, once the schedule knows it. Under the
	// phased schedule it is the tick after F1, which F1's own data decides.
	churnOnset() (int, bool)
	// current is the record the tick being served books into.
	current() *m6PhaseRecord
	// afterTick closes the tick and reports whether the run is over.
	afterTick(n *m6Network) bool
	trace() []m6PhaseRecord
	String() string
}

// --- flat --------------------------------------------------------------------------

// m6FlatSchedule is a fixed tick count with churn at a named tick.
type m6FlatSchedule struct {
	ticks, churnAt int
	whole          m6PhaseRecord
}

func newM6FlatSchedule(ticks, churnAt int) *m6FlatSchedule {
	return &m6FlatSchedule{
		ticks:   ticks,
		churnAt: churnAt,
		whole:   m6PhaseRecord{Phase: phaseFlat, Probes: newProbeLedger()},
	}
}

func (s *m6FlatSchedule) horizon() int            { return s.ticks }
func (s *m6FlatSchedule) churnOnset() (int, bool) { return s.churnAt, true }
func (s *m6FlatSchedule) current() *m6PhaseRecord { return &s.whole }
func (s *m6FlatSchedule) trace() []m6PhaseRecord  { return []m6PhaseRecord{s.whole} }
func (s *m6FlatSchedule) afterTick(n *m6Network) bool {
	if n.tick+1 < s.ticks {
		return false
	}
	s.whole.To = n.tick + 1
	s.whole.Stop = stopElapsed
	return true
}

func (s *m6FlatSchedule) String() string {
	return fmt.Sprintf("FLAT — %d ticks, churn at tick %d; ⚠️ the fixture schedule, NOT the phased "+
		"scenario of §5.9.1: no early stop and no common cadence window", s.ticks, s.churnAt)
}

// --- phased ----------------------------------------------------------------------------

// m6PhasedSchedule plays F1 → F2 → F3 → F4, or F1 → F4 when the run has no
// churn form at all.
type m6PhasedSchedule struct {
	plan  m6PhasePlan
	churn m6ChurnForm
	// replay pins every boundary to another run's; nil means this run decides.
	replay []m6PhaseBoundary

	played []m6PhaseRecord
	// watched is the signature at the end of the previous tick, idleRun how many
	// ticks in a row it has not changed.
	watched []int
	idleRun int

	onset      int
	onsetKnown bool
}

// newM6PhasedSchedule opens F1. `levels` sizes the coverage signature F1 starts
// from: every table is empty before the first tick, so the starting signature
// is all zeros, and the first tick of F1 is compared against it like any other.
func newM6PhasedSchedule(
	plan m6PhasePlan, churn m6ChurnForm, replay []m6PhaseBoundary, levels int,
) (*m6PhasedSchedule, error) {
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if err := validateReplay(replay, churn, plan); err != nil {
		return nil, err
	}
	schedule := &m6PhasedSchedule{plan: plan, churn: churn, replay: replay}
	schedule.open(phaseFill, 0, make([]int, levels))
	return schedule, nil
}

// validateReplay checks that a pinned trace is one this run could have played
// UNDER ITS OWN PLAN: the same phases in the same order, contiguous from tick
// 0, and every span inside what the plan allows — F1 at most T_fill, F2
// exactly one tick, F3 at most T_rec, F4 exactly T_cad.
//
// ⚠️ Replay suppresses the run's own stop rules, so it is the only place the
// plan can still be enforced. Without the length checks a pinned trace could
// stretch F2 over several ticks, cut F4 short or run F1 past its budget, and
// the report would then print the plan's T_cad beside a window that was
// never played. A trace of a shock run pinned onto a background run is refused
// for the same reason: the phases would not even be the same ones.
func validateReplay(replay []m6PhaseBoundary, churn m6ChurnForm, plan m6PhasePlan) error {
	if replay == nil {
		return nil
	}
	type allowed struct {
		phase    m6Phase
		min, max int
	}
	wanted := []allowed{
		{phaseFill, 1, plan.FillTicks}, {phaseChurn, 1, 1},
		{phaseRecovery, 1, plan.RecoveryTicks}, {phaseCadence, plan.CadenceTicks, plan.CadenceTicks},
	}
	if churn == churnNone {
		wanted = []allowed{{phaseFill, 1, plan.FillTicks}, {phaseCadence, plan.CadenceTicks, plan.CadenceTicks}}
	}
	if len(replay) != len(wanted) {
		return fmt.Errorf("replaying %d phase boundaries where the churn form %s plays %d phases",
			len(replay), churn, len(wanted))
	}
	next := 0
	for index, boundary := range replay {
		if boundary.Phase != wanted[index].phase {
			return fmt.Errorf("replayed boundary %d is %s, the schedule expects %s here",
				index, boundary.Phase, wanted[index].phase)
		}
		if boundary.From != next {
			return fmt.Errorf("replayed %s starts at tick %d, the schedule expected it to start "+
				"at tick %d", boundary.Phase, boundary.From, next)
		}
		ticks := boundary.To - boundary.From
		if ticks < wanted[index].min || ticks > wanted[index].max {
			return fmt.Errorf("replayed %s lasts %d ticks (%d–%d), the plan allows %d…%d: a "+
				"replayed boundary must be one this plan could have played, or the report would "+
				"print the plan beside a window that was never played", boundary.Phase, ticks,
				boundary.From, boundary.To, wanted[index].min, wanted[index].max)
		}
		next = boundary.To
	}
	return nil
}

func (s *m6PhasedSchedule) open(phase m6Phase, from int, watched []int) {
	s.played = append(s.played, m6PhaseRecord{Phase: phase, From: from, Probes: newProbeLedger()})
	s.watched = watched
	s.idleRun = 0
}

// horizon is the sum of every phase's budget: the run can never be longer, and
// the reserve has to be sized before the run knows how long it will be.
func (s *m6PhasedSchedule) horizon() int {
	if s.churn == churnNone {
		return s.plan.FillTicks + s.plan.CadenceTicks
	}
	return s.plan.FillTicks + 1 + s.plan.RecoveryTicks + s.plan.CadenceTicks
}

func (s *m6PhasedSchedule) churnOnset() (int, bool) { return s.onset, s.onsetKnown }
func (s *m6PhasedSchedule) current() *m6PhaseRecord {
	return &s.played[len(s.played)-1]
}
func (s *m6PhasedSchedule) trace() []m6PhaseRecord { return append([]m6PhaseRecord(nil), s.played...) }

func (s *m6PhasedSchedule) String() string {
	line := s.plan.String()
	if s.replay != nil {
		line += "; ⚠️ boundaries REPLAYED from another run — this run's own early-stop rules were " +
			"NOT consulted, so that a control plays the same scenario as the run it controls for"
	}
	return line
}

// afterTick decides whether the phase being played ends with this tick, and
// opens the next one.
//
// ⚠️ The idle rule compares the signature at the END of this tick with the one
// at the end of the previous tick (or, for the first tick of a phase, with the
// state the phase started in). T_idle unchanged ticks in a row end the phase;
// one changed tick resets the run to zero. The signature is the phase's own:
// F1 watches coverage, F3 watches the per-level deficit — §5.9.1 is explicit
// that F3 must not be stopped by total coverage, because filling somebody
// else's bucket would otherwise end the recovery phase (§4.3).
func (s *m6PhasedSchedule) afterTick(n *m6Network) bool {
	phase := s.current()
	elapsed := n.tick + 1 - phase.From

	reason := stopNotYet
	switch phase.Phase {
	case phaseFill:
		reason = s.idleOrElapsed(n.claimedByLevel(), elapsed, s.plan.FillTicks)
	case phaseChurn:
		reason = stopByConstruction
	case phaseRecovery:
		reason = s.idleOrElapsed(n.deficitByLevel(), elapsed, s.plan.RecoveryTicks)
	case phaseCadence:
		// ⚠️ No early stop, whatever the data says. The window is what makes the
		// three values of C comparable.
		if elapsed >= s.plan.CadenceTicks {
			reason = stopWindowEnd
		}
	}
	if s.replay != nil {
		reason = s.replayedVerdict(n.tick+1, len(s.played)-1)
	}
	if reason == stopNotYet {
		return false
	}

	phase.To = n.tick + 1
	phase.Stop = reason
	phase.IdleRun = s.idleRun
	return s.openNext(n, phase.Phase, phase.To)
}

// replayedVerdict ends the phase exactly where the pinned trace says, and
// nowhere else — the run's own rule was computed above only to keep the idle
// counter honest in the record.
func (s *m6PhasedSchedule) replayedVerdict(nextTick, index int) m6StopReason {
	if nextTick < s.replay[index].To {
		return stopNotYet
	}
	return stopReplayed
}

func (s *m6PhasedSchedule) idleOrElapsed(signature []int, elapsed, budget int) m6StopReason {
	if s.watched != nil && equalCounts(signature, s.watched) {
		s.idleRun++
	} else {
		s.idleRun = 0
	}
	s.watched = signature

	switch {
	case s.idleRun >= s.plan.IdleTicks:
		return stopIdle
	case elapsed >= budget:
		return stopElapsed
	}
	return stopNotYet
}

func equalCounts(a, b []int) bool {
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

// openNext is the transition table of §5.9.1. It returns true when there is
// nothing left to open.
func (s *m6PhasedSchedule) openNext(n *m6Network, closed m6Phase, from int) bool {
	switch closed {
	case phaseFill:
		if s.churn == churnNone {
			s.open(phaseCadence, from, nil)
			return false
		}
		s.onset, s.onsetKnown = from, true
		s.open(phaseChurn, from, nil)
	case phaseChurn:
		// F3 starts watching from the state the onset tick left behind, so a
		// deficit that appeared during the onset tick is not counted as a change
		// F3 made.
		s.open(phaseRecovery, from, n.deficitByLevel())
	case phaseRecovery:
		s.open(phaseCadence, from, nil)
	default:
		return true
	}
	return false
}

// phaseLine renders the played phases for the report, one per line.
func phaseLine(phases []m6PhaseRecord) string {
	if len(phases) == 0 {
		return "no data (the run has not been played)"
	}
	lines := make([]string, 0, len(phases))
	for _, phase := range phases {
		lines = append(lines, "    "+phase.String())
	}
	return strings.Join(lines, "\n")
}

// boundariesOf extracts what a control needs to replay a run.
func boundariesOf(phases []m6PhaseRecord) []m6PhaseBoundary {
	boundaries := make([]m6PhaseBoundary, 0, len(phases))
	for _, phase := range phases {
		boundaries = append(boundaries, phase.boundary())
	}
	return boundaries
}
