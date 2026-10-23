package trustedclock

import "time"

// This source is deliberately private and inactive. New/NewSystem never opt in.
// It measures evaluated jumpActive state, not unseen physical wall jumps. The
// caller must attribute a nonzero process instance; a restart is a new epoch.
// Sources have the same trusted suspend-inclusive contract as Sources.SinceBoot.
// No observation value participates in the security clock's time selection.
type guardObservation struct {
	instance                                         [16]byte
	initialized                                      bool
	invalid                                          string
	start                                            time.Duration
	maximum                                          time.Duration // comparison floor only; never an endpoint
	nextRead, reads, sequence, pending, pendingPairs uint64
	totalLower, totalUpper                           time.Duration // signed transition contributions
	current                                          *guardEpisode
	maxClosedLower, maxClosedUpper                   time.Duration
	closedCount                                      uint64
}

// One caller owns each token through provider return AND validation. No token
// is exposed outside this package, and there is no central token map or log.
type guardRead struct {
	owner                      *guardObservation
	id                         uint64
	floor                      time.Duration
	duration, registered, done bool
}

type guardEpisode struct {
	owner                                      *guardObservation
	entryL, entryU, exitL, exitU               time.Duration
	entryDone, exitDone, entryValid, exitValid bool
	closed, finalized                          bool
}

type guardCompletion struct {
	owner                          *guardObservation
	episode                        *guardEpisode
	lower                          time.Duration
	entry, valid, registered, done bool
}

// A COMPLETE result alone certifies its counts/bounds. Unknown snapshots do not
// present a partial count as proof of no closed episodes. Cut is a fresh source
// sample, never a value persisted into the transition sums. This epoch summary
// does not reconstruct UTC windows, bridge restart/gaps or select a threshold.
type guardDurationSnapshot struct {
	Status, Reason                 string
	Instance                       [16]byte
	CoverageStart, Cut             time.Duration
	Sequence                       uint64
	Guarded, Active                bool
	TotalLower, TotalUpper         time.Duration
	CurrentLower, CurrentUpper     time.Duration
	MaxClosedLower, MaxClosedUpper time.Duration
	ClosedCount                    uint64
}

const guardMaxUint = ^uint64(0)
const guardMaxDuration = time.Duration(1<<63 - 1)
const guardMinDuration = -guardMaxDuration - 1

// newObservedClock is a package-private construction seam, not installation on
// an already published Clock. Public production callers continue to use New.
func newObservedClock(s Sources, instance [16]byte) *Clock {
	c := &Clock{wall: s.Wall, sinceBoot: s.SinceBoot, observation: &guardObservation{instance: instance}}
	o := c.observation
	if instance == ([16]byte{}) {
		o.fail("missing-instance")
	}
	if s.SinceBoot == nil {
		o.fail("unsupported-source")
		return c
	}
	// Original constructor source order and guard initialization are unchanged.
	token := c.beginGuardRead(true)
	reading, err := s.SinceBoot()
	c.mu.Lock()
	c.validateGuardReadLocked(token, reading, err)
	c.retireGuardReadLocked(token)
	c.mu.Unlock()
	if err != nil {
		return c
	}
	wall := wallReading(c.observedWall())
	c.start = &anchor{wall: wall, sinceBoot: reading}
	c.lastSinceBoot = reading
	// Additional genuine cut starts observation while jumpActive is known false,
	// before the constructor publishes c. It does not re-anchor the clock.
	token = c.beginGuardRead(true)
	cut, cutErr := s.SinceBoot()
	c.mu.Lock()
	valid := c.validateGuardReadLocked(token, cut, cutErr)
	c.retireGuardReadLocked(token)
	if valid && o.invalid == "" {
		o.start, o.initialized = cut, true
	}
	c.mu.Unlock()
	return c
}

// All helpers named Locked require c.mu. The leaf is NEVER held across a
// provider invocation. Invalid is sticky; later success cannot bridge a gap.
func (o *guardObservation) fail(reason string) {
	if o.invalid == "" {
		o.invalid = reason
	}
}
func (o *guardObservation) increment(p *uint64) bool {
	if *p == guardMaxUint {
		o.fail("counter-overflow")
		return false
	}
	*p = *p + 1
	return true
}
func (o *guardObservation) add(p *time.Duration, delta time.Duration) {
	if delta > 0 && *p > guardMaxDuration-delta || delta < 0 && *p < guardMinDuration-delta {
		o.fail("duration-overflow")
		return
	}
	*p += delta
}
func (c *Clock) beginGuardRead(duration bool) *guardRead {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.beginGuardReadLocked(duration)
}
func (c *Clock) beginGuardReadLocked(duration bool) *guardRead {
	o := c.observation
	r := &guardRead{owner: o, floor: o.maximum, duration: duration}
	if o.nextRead == guardMaxUint || o.reads == guardMaxUint {
		o.fail("read-overflow")
		return r
	}
	o.nextRead++
	o.reads++
	r.id, r.registered = o.nextRead, true
	return r
}
func (c *Clock) validateGuardReadLocked(r *guardRead, value time.Duration, err error) bool {
	o := c.observation
	if r == nil || r.owner != o || r.done || !r.registered || r.id == 0 {
		o.fail("invalid-read-owner")
		return false
	}
	if !r.duration {
		return true
	}
	if err != nil {
		o.fail("source-error")
		return false
	}
	if value < 0 {
		o.fail("negative-source")
		return false
	}
	if value < r.floor {
		o.fail("source-regression")
		return false
	}
	if value > o.maximum {
		o.maximum = value
	}
	return true
}
func (c *Clock) retireGuardReadLocked(r *guardRead) {
	o := c.observation
	if r == nil || r.owner != o || r.done {
		o.fail("duplicate-read-retirement")
		return
	}
	r.done = true
	if !r.registered {
		return
	}
	if o.reads == 0 {
		o.fail("read-underflow")
		return
	}
	o.reads--
}
func (c *Clock) observedWall() time.Time {
	token := c.beginGuardRead(false)
	wall := c.wall()
	c.mu.Lock()
	c.validateGuardReadLocked(token, 0, nil)
	c.retireGuardReadLocked(token)
	c.mu.Unlock()
	return wall
}

func (c *Clock) observedNow() time.Time {
	wall := wallReading(c.observedWall())
	if c.start == nil {
		return wall
	}
	token := c.beginGuardRead(true)
	reading, readErr := c.sinceBoot()
	c.mu.Lock()
	valid := c.validateGuardReadLocked(token, reading, readErr)
	c.retireGuardReadLocked(token)
	previous := c.jumpActive
	// These are the original guard branches and results. Observation validity,
	// maximum, brackets and errors cannot change any security decision here.
	if readErr == nil && reading > c.lastSinceBoot {
		c.lastSinceBoot = reading
	}
	trusted := c.start.wall.Add(c.lastSinceBoot - c.start.sinceBoot)
	ahead := wall.Sub(trusted)
	result := trusted
	if ahead <= JumpTolerance {
		c.jumpActive = false
		result = wall
	} else if !c.jumpActive {
		c.jumpActive = true
		c.jumpsDetected++
		c.unreported = &Jump{Wall: wall, Trusted: trusted, Ahead: ahead}
	}
	var completion *guardCompletion
	if previous != c.jumpActive {
		completion = c.guardTransitionLocked(reading, valid, c.jumpActive)
	}
	c.mu.Unlock()
	if completion != nil {
		c.completeGuardTransition(completion)
	}
	return result
}

func (c *Clock) guardTransitionLocked(reading time.Duration, valid, entry bool) *guardCompletion {
	o := c.observation
	if !o.increment(&o.sequence) {
		valid = false
	}
	if !o.initialized || reading < o.start {
		valid = false
		o.fail("invalid-transition-lower")
	}
	lower := time.Duration(0)
	if valid {
		lower = reading - o.start
	}
	p := &guardCompletion{owner: o, lower: lower, entry: entry, valid: valid}
	p.registered = o.increment(&o.pending)
	if entry {
		if o.current != nil {
			o.fail("overlapping-entry")
			p.valid = false
		}
		e := &guardEpisode{owner: o, entryL: lower}
		o.current, p.episode = e, e
		if valid {
			o.add(&o.totalUpper, -lower)
		}
	} else {
		e := o.current
		o.current = nil
		p.episode = e
		if e == nil || e.owner != o || e.closed {
			o.fail("missing-entry")
			p.valid = false
			return p
		}
		e.closed, e.exitL = true, lower
		if !o.increment(&o.pendingPairs) {
			p.valid = false
		}
		if valid {
			o.add(&o.totalLower, lower)
		}
	}
	return p
}
func (c *Clock) completeGuardTransition(p *guardCompletion) {
	token := c.beginGuardRead(true)
	upper, err := c.sinceBoot()
	c.mu.Lock()
	defer c.mu.Unlock()
	valid := c.validateGuardReadLocked(token, upper, err)
	c.retireGuardReadLocked(token)
	c.finishGuardTransitionLocked(p, upper, valid)
}
func (c *Clock) finishGuardTransitionLocked(p *guardCompletion, upper time.Duration, valid bool) {
	o := c.observation
	if p == nil || p.owner != o || p.done {
		o.fail("duplicate-transition-completion")
		return
	}
	p.done = true
	if p.registered {
		if o.pending == 0 {
			o.fail("pending-underflow")
		} else {
			o.pending--
		}
	}
	valid = valid && p.valid && o.initialized && upper >= o.start && upper-o.start >= p.lower
	if !valid {
		o.fail("invalid-transition-bracket")
	}
	u := time.Duration(0)
	if valid {
		u = upper - o.start
	}
	e := p.episode
	if e == nil || e.owner != o {
		o.fail("invalid-episode-owner")
		return
	}
	if p.entry {
		if e.entryDone {
			o.fail("duplicate-entry-result")
			return
		}
		e.entryDone, e.entryValid, e.entryU = true, valid, u
		if valid {
			o.add(&o.totalLower, -u)
		}
	} else {
		if e.exitDone {
			o.fail("duplicate-exit-result")
			return
		}
		e.exitDone, e.exitValid, e.exitU = true, valid, u
		if valid {
			o.add(&o.totalUpper, u)
		}
	}
	c.reduceGuardEpisodeLocked(e)
}
func (c *Clock) reduceGuardEpisodeLocked(e *guardEpisode) {
	o := c.observation
	if !e.closed || !e.entryDone || !e.exitDone || e.finalized {
		return
	}
	e.finalized = true
	if o.pendingPairs == 0 {
		o.fail("pair-underflow")
	} else {
		o.pendingPairs--
	}
	if !e.entryValid || !e.exitValid {
		return
	}
	lower, upper := e.exitL-e.entryU, e.exitU-e.entryL
	if lower < 0 {
		lower = 0
	}
	if upper < lower {
		o.fail("invalid-episode-bounds")
		return
	}
	if lower > o.maxClosedLower {
		o.maxClosedLower = lower
	}
	if upper > o.maxClosedUpper {
		o.maxClosedUpper = upper
	}
	o.increment(&o.closedCount)
}

// guardDuration takes one genuine fresh cut. It does not wait for other reads,
// spin, change persisted signed totals or infer coverage from lastSinceBoot.
func (c *Clock) guardDuration() guardDurationSnapshot {
	if c.observation == nil {
		return guardDurationSnapshot{Status: "INACTIVE"}
	}
	c.mu.Lock()
	o := c.observation
	unknown := func(status, reason string) guardDurationSnapshot {
		return guardDurationSnapshot{Status: status, Reason: reason, Instance: o.instance}
	}
	if o.invalid != "" || !o.initialized {
		r := unknown("UNKNOWN", o.invalid)
		c.mu.Unlock()
		return r
	}
	if o.reads != 0 || o.pending != 0 || o.pendingPairs != 0 {
		r := unknown("IN_FLIGHT", "unresolved-provider-or-transition")
		c.mu.Unlock()
		return r
	}
	sequence, active := o.sequence, c.jumpActive
	token := c.beginGuardReadLocked(true)
	c.mu.Unlock()
	cut, err := c.sinceBoot()
	c.mu.Lock()
	defer c.mu.Unlock()
	valid := c.validateGuardReadLocked(token, cut, err)
	stable := o.reads == 1 && o.pending == 0 && o.pendingPairs == 0 && o.sequence == sequence && c.jumpActive == active
	c.retireGuardReadLocked(token)
	if !valid || o.invalid != "" {
		return unknown("UNKNOWN", o.invalid)
	}
	if !stable || o.reads != 0 {
		return unknown("UNSTABLE_CUT", "overlap-or-state-change")
	}
	if cut < o.start {
		o.fail("cut-before-coverage")
		return unknown("UNKNOWN", o.invalid)
	}
	t := cut - o.start
	lower, upper := o.totalLower, o.totalUpper
	currentLower, currentUpper := time.Duration(0), time.Duration(0)
	if active {
		e := o.current
		if e == nil || !e.entryDone || !e.entryValid {
			o.fail("missing-current-bracket")
			return unknown("UNKNOWN", o.invalid)
		}
		currentLower, currentUpper = t-e.entryU, t-e.entryL
		if currentLower < 0 {
			currentLower = 0
		}
		// Checked additions on local copies only: T is not persisted.
		o.add(&lower, t)
		o.add(&upper, t)
	}
	if lower < 0 {
		lower = 0
	}
	if upper < lower || currentUpper < currentLower {
		o.fail("invalid-snapshot-bounds")
	}
	if o.invalid != "" {
		return unknown("UNKNOWN", o.invalid)
	}
	return guardDurationSnapshot{Status: "COMPLETE", Instance: o.instance, CoverageStart: o.start, Cut: cut, Sequence: sequence,
		Guarded: c.start != nil, Active: active, TotalLower: lower, TotalUpper: upper,
		CurrentLower: currentLower, CurrentUpper: currentUpper, MaxClosedLower: o.maxClosedLower,
		MaxClosedUpper: o.maxClosedUpper, ClosedCount: o.closedCount}
}
