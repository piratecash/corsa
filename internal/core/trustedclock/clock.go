// Package trustedclock is the time a node's terms run on when a wall clock
// set forward must not end them early (docs/protocol/session_v2.md, "The
// clock of the terms").
//
// The trusted time of a running process is the wall clock read at its start
// plus how far a clock that keeps running through sleep has moved since. A
// wall clock that runs more than JumpTolerance ahead of that is a jump — a
// forged NTP answer or an operator's step — and the excess is not counted:
// Now answers the trusted time instead and the jump is reported. Sleep moves
// both clocks alike and is counted in full. A wall clock set back is taken as
// it is and is no jump; whoever stamps times with it must never move a stamp
// back (the secure-session store keeps the later last_v2), or a return of the
// clock — no jump either — would end at once what was stamped meanwhile.
//
// At the start there is nothing to hold the wall clock against: a step made
// before or during a restart is taken as it is. So is every step on a
// platform without a clock since boot (Stats.Guarded is false there).
//
// Known limits, kept on purpose because re-anchoring would let an attacker
// step the clock forward and simply wait:
//   - A wall clock WRONG at the start — a device without a battery-backed
//     clock started before NTP synchronised — is the anchor. The honest
//     correction later is a jump that never ends (JumpActive stays true), the
//     process runs Δ behind the real time, everything it stamps lags by Δ,
//     and after the next restart those terms end Δ early (at once for Δ of
//     30 days or more). A JumpActive that lasts is the operator's cue to
//     check the clock and restart once it is right.
//   - A clock since boot that stops answering mid-run holds its last
//     reading; an hour later the moving wall clock is a jump that lasts until
//     the source answers again — a false one, failing toward terms that end
//     later, never earlier.
package trustedclock

import (
	"errors"
	"sync"
	"time"
)

// JumpTolerance is how far the wall clock may run ahead of the trusted time
// and still be taken as it is: an honest correction, or the drift between
// the two clocks over a long uptime, stays well within it.
const JumpTolerance = time.Hour

// ErrSinceBootUnsupported is SinceBoot on a platform without a clock since
// boot that keeps running through sleep.
var ErrSinceBootUnsupported = errors.New("trustedclock: no clock since boot that runs through sleep on this platform")

// Sources are the two clocks the trusted time is made of.
type Sources struct {
	// Wall is the wall clock.
	Wall func() time.Time
	// SinceBoot reads the time since boot on a clock that keeps running
	// through sleep; nil, or an error at the start, leaves the clock
	// unguarded.
	SinceBoot func() (time.Duration, error)
}

// Jump is a wall clock found running ahead of the trusted time by more than
// JumpTolerance.
type Jump struct {
	// Wall is what the wall clock read when the jump was found.
	Wall time.Time
	// Trusted is the trusted time then, the time answered instead.
	Trusted time.Time
	// Ahead is how far the wall clock ran ahead of it.
	Ahead time.Duration
}

// Stats is the state of the guard for diagnostics.
type Stats struct {
	// Guarded reports that a clock since boot was read at the start, so a
	// jump in this process is found and not counted.
	Guarded bool
	// JumpsDetected counts the jumps found since the start: a wall clock
	// that stays ahead is one jump, and one that came back and jumps again
	// is another.
	JumpsDetected uint64
	// JumpActive reports that the wall clock is ahead now and the trusted
	// time is being answered instead.
	JumpActive bool
}

// anchor is the pair of readings taken at the start.
type anchor struct {
	wall      time.Time
	sinceBoot time.Duration
}

// Clock answers the trusted time. Safe for concurrent use; mu is a leaf held
// for the bookkeeping only, never across a read of the sources.
type Clock struct {
	wall      func() time.Time
	sinceBoot func() (time.Duration, error)
	// start is nil on an unguarded clock.
	start *anchor

	mu sync.Mutex
	// lastSinceBoot is the highest good reading of sinceBoot: a source that
	// stops answering holds it, so the trusted time does not run ahead of
	// what was measured, and a concurrent Now whose older reading reaches
	// mu later does not move it back.
	lastSinceBoot time.Duration
	jumpActive    bool
	jumpsDetected uint64
	// unreported is the jump found and not yet handed out for the journal.
	unreported *Jump
}

// New reads both sources once, as the start of the trusted time.
func New(sources Sources) *Clock {
	clock := &Clock{wall: sources.Wall, sinceBoot: sources.SinceBoot}
	if sources.SinceBoot == nil {
		return clock
	}
	reading, err := sources.SinceBoot()
	if err != nil {
		return clock
	}
	clock.start = &anchor{wall: wallReading(sources.Wall()), sinceBoot: reading}
	clock.lastSinceBoot = reading
	return clock
}

// NewSystem is the trusted time on this machine's own clocks.
func NewSystem() *Clock {
	return New(Sources{Wall: time.Now, SinceBoot: SinceBoot})
}

// wallReading drops the monotonic reading time.Now carries: with it,
// time.Time.Sub would measure the process's monotonic clock, which a step of
// the wall clock does not move, and no jump would ever be seen.
func wallReading(at time.Time) time.Time { return at.Round(0) }

// Now is the trusted time: the wall clock, unless it runs more than
// JumpTolerance ahead of the start plus the time since it on the clock since
// boot — then that sum.
func (c *Clock) Now() time.Time {
	wall := wallReading(c.wall())
	if c.start == nil {
		return wall
	}
	reading, readErr := c.sinceBoot()

	c.mu.Lock()
	defer c.mu.Unlock()
	if readErr == nil && reading > c.lastSinceBoot {
		c.lastSinceBoot = reading
	}
	trusted := c.start.wall.Add(c.lastSinceBoot - c.start.sinceBoot)
	ahead := wall.Sub(trusted)
	if ahead <= JumpTolerance {
		c.jumpActive = false
		return wall
	}
	if !c.jumpActive {
		c.jumpActive = true
		c.jumpsDetected++
		c.unreported = &Jump{Wall: wall, Trusted: trusted, Ahead: ahead}
	}
	return trusted
}

// Wall is the wall clock as it reads, for a figure that says when it was
// read rather than one that decides a term.
func (c *Clock) Wall() time.Time { return c.wall() }

// TakeUnreportedJump hands out the jump found since the last call, once, so
// the caller journals it outside its own locks.
func (c *Clock) TakeUnreportedJump() (Jump, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unreported == nil {
		return Jump{}, false
	}
	jump := *c.unreported
	c.unreported = nil
	return jump, true
}

// Stats is the state of the guard.
func (c *Clock) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Guarded: c.start != nil, JumpsDetected: c.jumpsDetected, JumpActive: c.jumpActive}
}
