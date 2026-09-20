package overlaysim

// m6_window_test.go is the RECOVERY WINDOW of decision 3.5(ii) (review package
// §4, 2026-09-18): the recovery axis of §4.3 is read on the half-open interval
// [onset, onset + W_rec) of ticks — the same interval for every configuration
// with a churn form, C = ∞ included — INDEPENDENTLY of where the phases of
// §5.9.1 put their boundaries.
//
// Why a window beside the phases: the length of F3 is a RESULT (its idle rule
// may close it after T_idle ticks or its budget after T_rec), so F3 ∪ F4 is a
// different span for every configuration and the sums of detections and
// refills over it are functions of that length. A comparison needs a span
// given by the INPUT. W_rec is that input — a parameter of this experiment,
// fixed at 1024 for the grid because that equals the contract's T_cad, and NOT
// derived from T_cad: a later change of the cadence window must not silently
// move the comparison window.
//
// What the window does NOT do: it never decides when the run stops, and it is
// not a phase. The phases still decide the length of the run; F3 is still
// reported as the time the deficit took to converge, with its own stop reason;
// and reaching the end of F3 — by idle or by budget — is not the recovery
// criterion. The criterion is stated below and judged inside the window.

import (
	"fmt"
	"strings"
)

// m6RecoveryWindow is the ledger of [From, To) = [onset, onset + Ticks).
//
// ⚠️ Every event is booked by the TICK IT HAPPENED IN: an event in the onset
// tick is inside, an event in tick onset + W_rec is outside. The state the
// window opens on is the state at the START of the onset tick, before that
// tick's churn (nothing has been lost yet — asserted, not assumed); the state
// it closes on is the state at the END of tick To − 1, after every node has
// served it.
type m6RecoveryWindow struct {
	// Ticks is W_rec, From the onset tick, To = From + Ticks (exclusive).
	Ticks    int
	From, To int
	// Closed says whether the run reached To − 1; a run that ended earlier is
	// refused at the door, so a report never carries an open window.
	Closed bool

	// The MEASURED population's events inside the window. LostEvents and
	// RefilledEvents count per (owner, level) like the recovery axis; a
	// record lost, refilled and lost again is three events — so the UNIQUE
	// counts stand beside them: distinct (owner, record) pairs detected lost,
	// and distinct (owner, record) pairs stored as recovery.
	LostEvents, RefilledEvents int
	UniqueLostRecords          int
	UniqueRefilledRecords      int
	DetectionEvents            int
	// Probes is the measured cost in PROBES inside the window, Refreshes the
	// refreshes the cadence scheduled inside it.
	Probes    probeLedger
	Refreshes int
	// The mechanism's FRAMES inside the window, measured owners, by the tick
	// of the event (owner's P2, 2026-09-18): exchanges served and refused at
	// the responder's interval (A′/C), addressed answers (empty ones
	// included), refusals at a responder under its rate limit and requests
	// nobody answered (C), and repeats filtered at the owner (the 3.2(c)
	// control). ⚠️ Kept APART from the probes and never summed with them: a
	// frame and a probe are different units, and the contract names no cost
	// model that converts one into the other.
	ExchangesServed, ExchangesRefused                        int
	AddressedAnswers, AddressedRateLimited, AddressedRefused int
	RepeatsFiltered                                          int

	// At the end of the window: ResidualDeficit is Σ max(lost − refilled, 0)
	// per level over the measured owners ONLINE then — records they know they
	// lost and have not replaced; ResidualDeficitOffline is the same sum over
	// the measured owners OFFLINE then — a deficit that LEFT with its owner
	// and is still owed; UndetectedDeadRecords is claimed − actually alive
	// over the online owners — records pointing at departed nodes they have
	// not yet found out about. Three remainders: what recovery still owes
	// online, what it owes offline, and what detection still owes.
	ResidualDeficit        int
	ResidualDeficitOffline int
	UndetectedDeadRecords  int
	// KnownDeficitClearedAtTau is the NARROW indicator: the first τ = tick −
	// From at which, after at least one detected loss, the known deficit of
	// the measured owners ONLINE at that moment was zero. ⚠️ It is NOT
	// recovery and is never read as its time (owner's P2, 2026-09-18): it
	// clears when the only owner with a deficit LEAVES, and it clears while
	// dead records the owners have not yet detected are still held. It is
	// kept because it is what the F3 stop rule watches, in the measured
	// population's reading.
	KnownDeficitClearedAtTau int
	// RecoveredAtTau is the recovery criterion: the first τ at which, after at
	// least one detected loss, EVERY measured owner's deficit — online or
	// offline — was zero AND no measured online owner held a record of a
	// departed node. −1 when not met inside the window — printed as "not
	// reached within W_rec ticks", never as a delay equal to the window.
	RecoveredAtTau int

	// lostRecords and refilledRecords back the unique counts.
	lostRecords     map[[2]int32]struct{}
	refilledRecords map[[2]int32]struct{}
}

func newM6RecoveryWindow(from, ticks int) *m6RecoveryWindow {
	return &m6RecoveryWindow{
		Ticks:                    ticks,
		From:                     from,
		To:                       from + ticks,
		Probes:                   newProbeLedger(),
		KnownDeficitClearedAtTau: -1,
		RecoveredAtTau:           -1,
		lostRecords:              map[[2]int32]struct{}{},
		refilledRecords:          map[[2]int32]struct{}{},
	}
}

// contains says whether events of this tick are inside the window.
func (w *m6RecoveryWindow) contains(tick int) bool {
	return w != nil && tick >= w.From && tick < w.To
}

// noteLost books one detected loss of a measured owner's record at one level.
func (w *m6RecoveryWindow) noteLost(owner, gone int32) {
	w.LostEvents++
	w.lostRecords[[2]int32{owner, gone}] = struct{}{}
	w.UniqueLostRecords = len(w.lostRecords)
}

// noteRefilled books one fill counted as recovery.
func (w *m6RecoveryWindow) noteRefilled(owner, stored int32) {
	w.RefilledEvents++
	w.refilledRecords[[2]int32{owner, stored}] = struct{}{}
	w.UniqueRefilledRecords = len(w.refilledRecords)
}

// String renders the window for the report, the two remainders and the
// criterion named apart from the phases.
func (w *m6RecoveryWindow) String() string {
	if w == nil {
		return "no recovery window (the run asked for none)"
	}
	if !w.Closed {
		return fmt.Sprintf("recovery window [%d, %d) OPEN — the run has not reached its end", w.From, w.To)
	}
	criterion := fmt.Sprintf("recovery criterion NOT REACHED within %d ticks of the onset (at every tick "+
		"some measured owner, online or offline, still owed a detected loss, or some online owner still "+
		"held a record of a departed node)", w.Ticks)
	switch {
	case w.RecoveredAtTau >= 0:
		criterion = fmt.Sprintf("recovery criterion reached at τ=%d (every detected loss of every "+
			"measured owner refilled AND no online owner holding a record of a departed node)",
			w.RecoveredAtTau)
	case w.LostEvents == 0:
		criterion = fmt.Sprintf("no loss was DETECTED inside the window, so there was nothing to recover "+
			"from — not a recovery, and not a delay of %d ticks", w.Ticks)
	}
	narrow := "the known deficit of the online owners never reached zero"
	if w.KnownDeficitClearedAtTau >= 0 {
		narrow = fmt.Sprintf("known deficit of the online owners first zero at τ=%d — a NARROW indicator "+
			"(it clears when an owner with a deficit leaves and while undetected dead records are still "+
			"held), NOT recovery and not its time", w.KnownDeficitClearedAtTau)
	}
	return fmt.Sprintf("recovery window [%d, %d) = %d ticks from the onset, the same for every "+
		"configuration: %d losses detected (%d distinct records), %d refilled as recovery (%d distinct "+
		"records), %d detections of departed nodes; cost inside the window in PROBES %s, %d refreshes "+
		"scheduled; frames inside the window (not summed with the probes — no cost model converts one "+
		"into the other): %d exchanges served, %d refused at the responder's interval, %d addressed "+
		"answers, %d refusals at a responder's rate limit, %d requests nobody answered, %d repeats "+
		"filtered at the owner; at the end of the window the measured online owners still owe %d "+
		"records (residual deficit), the measured OFFLINE owners took %d owed records with them, and "+
		"the online owners still hold %d records of departed nodes they have not found out about "+
		"(undetected); %s; %s",
		w.From, w.To, w.Ticks, w.LostEvents, w.UniqueLostRecords, w.RefilledEvents,
		w.UniqueRefilledRecords, w.DetectionEvents, w.Probes, w.Refreshes, w.ExchangesServed,
		w.ExchangesRefused, w.AddressedAnswers, w.AddressedRateLimited, w.AddressedRefused,
		w.RepeatsFiltered, w.ResidualDeficit, w.ResidualDeficitOffline, w.UndetectedDeadRecords,
		criterion, narrow)
}

// --- capacity: the window has to fit, and a run that cannot hold it is refused ---

// minimumTicksAfterOnset is the fewest ticks a run under this plan plays from
// the onset tick on, by its own rules: F2 (one tick) + F3 (at least
// min(T_idle, T_rec): the idle rule needs T_idle unchanged ticks, the budget
// stops it at T_rec) + F4 (exactly T_cad).
func (p m6PhasePlan) minimumTicksAfterOnset() int {
	return 1 + min(p.IdleTicks, p.RecoveryTicks) + p.CadenceTicks
}

// ticksAfterOnsetOfReplay reads the fit off PINNED boundaries: the ticks from
// the start of F2 to the end of the last phase. ⚠️ Not the plan's minimum —
// a replayed F3 may be one tick long (validateReplay allows [1, T_rec]), so a
// plan that would fit the window by its own rules can carry boundaries that do
// not.
func ticksAfterOnsetOfReplay(replay []m6PhaseBoundary) (int, bool) {
	for _, boundary := range replay {
		if boundary.Phase == phaseChurn {
			return replay[len(replay)-1].To - boundary.From, true
		}
	}
	return 0, false
}

// validateRecoveryWindow is the door: a window is refused where the run cannot
// hold it in full, because a window cut short is a different window and a
// report that printed W_rec beside it would be wrong.
func validateRecoveryWindow(config m6ModelConfig) error {
	window := config.RecoveryWindow
	if window <= 0 {
		return nil
	}
	if config.Churn == churnNone {
		return fmt.Errorf("a recovery window of %d ticks was asked for under churn form %s: with no "+
			"onset there is no recovery axis and no window", window, config.Churn)
	}
	fits := func(available int, how string) error {
		if available >= window {
			return nil
		}
		return fmt.Errorf("the recovery window W_rec = %d does not fit: %s gives %d ticks from the "+
			"onset; a window cut short is another window, so the run is refused rather than measured "+
			"over less", window, how, available)
	}
	if config.Phases == nil {
		return fits(config.Ticks-config.ChurnAt, fmt.Sprintf("the flat schedule (%d ticks, churn at %d)",
			config.Ticks, config.ChurnAt))
	}
	if config.ReplayPhases != nil {
		available, ok := ticksAfterOnsetOfReplay(config.ReplayPhases)
		if !ok {
			return fmt.Errorf("the replayed boundaries carry no F2, so the window has no onset to start from")
		}
		return fits(available, "the REPLAYED boundaries (F2 to the end of the last phase)")
	}
	plan := *config.Phases
	return fits(plan.minimumTicksAfterOnset(), fmt.Sprintf("the plan's own rules at their shortest "+
		"(1 + min(T_idle=%d, T_rec=%d) + T_cad=%d)", plan.IdleTicks, plan.RecoveryTicks, plan.CadenceTicks))
}

// --- the hooks the tick loop calls ---------------------------------------------------

// openRecoveryWindowIfDue opens the window at the START of the onset tick,
// before that tick's churn. ⚠️ Nothing may have been lost yet: the window is
// meant to see the whole of the recovery, and a run that had detected losses
// before its onset is not the scenario the window describes.
func (n *m6Network) openRecoveryWindowIfDue() error {
	if n.config.RecoveryWindow <= 0 || n.window != nil {
		return nil
	}
	onset, known := n.schedule.churnOnset()
	if !known || n.tick != onset {
		return nil
	}
	if sumOf(n.report.LostByLevel) != 0 || sumOf(n.report.RefilledByLevel) != 0 {
		return fmt.Errorf("the recovery window opens at the onset tick %d, but %d losses had already "+
			"been detected before it", onset, sumOf(n.report.LostByLevel))
	}
	n.window = newM6RecoveryWindow(onset, n.config.RecoveryWindow)
	n.report.Window = n.window
	return nil
}

// closeRecoveryWindowIfDue closes the window at the END of tick To − 1, after
// every node has served, and books the criterion for this tick on the way:
// the criterion is judged at the end of every tick inside the window.
func (n *m6Network) closeRecoveryWindowIfDue() {
	window := n.window
	if !window.contains(n.tick) {
		return
	}
	if window.LostEvents > 0 {
		online, offline := n.measuredResidualDeficit()
		if window.KnownDeficitClearedAtTau < 0 && online == 0 {
			window.KnownDeficitClearedAtTau = n.tick - window.From
		}
		// ⚠️ Recovery is judged over EVERY measured owner, offline ones
		// included, and over the dead records not yet detected: an owner that
		// left takes its deficit with it and does not clear it, and a dead
		// record nobody has probed is a loss that is not recovered (owner's
		// P2, 2026-09-18).
		if window.RecoveredAtTau < 0 && online+offline == 0 && n.measuredUndetectedDeadRecords() == 0 {
			window.RecoveredAtTau = n.tick - window.From
		}
	}
	if n.tick != window.To-1 {
		return
	}
	window.ResidualDeficit, window.ResidualDeficitOffline = n.measuredResidualDeficit()
	window.UndetectedDeadRecords = n.measuredUndetectedDeadRecords()
	window.Closed = true
}

// measuredResidualDeficit is Σ max(lost − refilled, 0) per level over the
// measured owners, split into the owners ONLINE now and the owners OFFLINE
// now. The online half is what the narrow indicator watches (the same
// reading as coverageByLevel); the sum of both is what recovery owes — an
// offline owner's deficit left with it and is still a loss nobody repaired.
func (n *m6Network) measuredResidualDeficit() (online, offline int) {
	for _, owner := range n.owners {
		state := n.states[owner]
		if state == nil {
			continue
		}
		deficit := 0
		for level := range state.LostByLevel {
			deficit += max(state.LostByLevel[level]-state.RefilledByLevel[level], 0)
		}
		if n.online[owner] {
			online += deficit
			continue
		}
		offline += deficit
	}
	return online, offline
}

// noteFrame books one frame of the mechanism inside the window, by the tick
// of the event, for a measured owner; `counter` is the window's field.
func (n *m6Network) noteFrame(owner int32, counter *int) {
	if n.measured(owner) && n.window.contains(n.tick) {
		*counter++
	}
}

// measuredUndetectedDeadRecords is claimed − actually alive over the measured
// owners online now: the records that point at departed nodes and have not
// been detected — what detection still owes at this moment.
func (n *m6Network) measuredUndetectedDeadRecords() int {
	dead := 0
	for _, owner := range n.owners {
		state := n.states[owner]
		if state == nil || !n.online[owner] {
			continue
		}
		for _, level := range state.Table.members {
			for member := range level {
				if !n.online[member] {
					dead++
				}
			}
		}
	}
	return dead
}

// requireWindowClosed is asked when the run ends: a window still open means the
// schedule played fewer ticks after the onset than the door computed, which is
// a defect of the door and not a result.
func (n *m6Network) requireWindowClosed() error {
	if n.window == nil || n.window.Closed {
		return nil
	}
	return fmt.Errorf("the run ended at tick %d with the recovery window [%d, %d) still open: the "+
		"schedule played fewer ticks after the onset than the window needs", n.tick, n.window.From,
		n.window.To)
}

// windowLine is the report's line for the window, beside — never instead of —
// the phases: F3's length and reason are the schedule's finding, the criterion
// is the window's.
func (r m6ModelReport) windowLine() string {
	lines := []string{r.Window.String()}
	for _, phase := range r.Phases {
		if phase.Phase != phaseRecovery {
			continue
		}
		lines = append(lines, fmt.Sprintf("F3 lasted %d ticks and ended because %s — that is the STOP "+
			"RULE of §5.9.1, a statement about the convergence of the deficit, and NOT the recovery "+
			"criterion; whether recovery was reached is stated by the window above", phase.Ticks(),
			phase.Stop))
	}
	return strings.Join(lines, "\n    ")
}
