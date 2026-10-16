package trustedclock

import (
	"errors"
	"testing"
	"time"
)

// fakeSources is a device whose wall clock and clock since boot the test
// moves by hand: pass is time going by (asleep or awake, both clocks move),
// stepWall is somebody setting the wall clock (only it moves).
type fakeSources struct {
	wall      time.Time
	sinceBoot time.Duration
	err       error
}

var errSourceGone = errors.New("source gone")

func newFakeSources() *fakeSources {
	return &fakeSources{wall: time.Unix(1780000000, 0).UTC(), sinceBoot: 3 * time.Hour}
}

func (f *fakeSources) readWall() time.Time { return f.wall }

func (f *fakeSources) readSinceBoot() (time.Duration, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.sinceBoot, nil
}

func (f *fakeSources) pass(d time.Duration) {
	f.wall = f.wall.Add(d)
	f.sinceBoot += d
}

func (f *fakeSources) stepWall(d time.Duration) { f.wall = f.wall.Add(d) }

func (f *fakeSources) clock() *Clock {
	return New(Sources{Wall: f.readWall, SinceBoot: f.readSinceBoot})
}

func assertNow(t *testing.T, clock *Clock, want time.Time) {
	t.Helper()
	if got := clock.Now(); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
}

func assertStats(t *testing.T, clock *Clock, want Stats) {
	t.Helper()
	if got := clock.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
}

// A wall clock set 60 days ahead in a running process is not counted: the
// trusted time keeps going from the start by the clock since boot, and the
// step is one detected jump, reported once.
func TestAWallClockStepForwardIsNotCounted(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	start := device.wall
	clock := device.clock()

	device.pass(time.Hour)
	device.stepWall(60 * 24 * time.Hour)
	assertNow(t, clock, start.Add(time.Hour))
	assertStats(t, clock, Stats{Guarded: true, JumpsDetected: 1, JumpActive: true})

	jump, reported := clock.TakeUnreportedJump()
	if !reported {
		t.Fatal("a detected jump was not handed out for the journal")
	}
	if jump.Ahead != 60*24*time.Hour || !jump.Trusted.Equal(start.Add(time.Hour)) || !jump.Wall.Equal(device.wall) {
		t.Fatalf("jump = %+v, want 60 days ahead of %v at wall %v", jump, start.Add(time.Hour), device.wall)
	}
	if _, again := clock.TakeUnreportedJump(); again {
		t.Fatal("one jump was handed out twice")
	}

	device.pass(2 * time.Hour)
	assertNow(t, clock, start.Add(3*time.Hour))
	assertStats(t, clock, Stats{Guarded: true, JumpsDetected: 1, JumpActive: true})
	if _, again := clock.TakeUnreportedJump(); again {
		t.Fatal("a jump still in force was reported as a new one")
	}
}

// Sleep moves the clock since boot with the wall clock: ten days asleep count
// in full and are no jump.
func TestSleepCountsInFullAndIsNoJump(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	clock := device.clock()

	device.pass(10 * 24 * time.Hour)
	assertNow(t, clock, device.wall)
	assertStats(t, clock, Stats{Guarded: true})
	if _, reported := clock.TakeUnreportedJump(); reported {
		t.Fatal("sleep was reported as a jump")
	}
}

// Up to JumpTolerance ahead is an honest correction and is taken as it is;
// anything past it is a jump.
func TestAStepWithinTheToleranceIsTakenAsIs(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	start := device.wall
	clock := device.clock()

	device.stepWall(JumpTolerance)
	assertNow(t, clock, device.wall)
	assertStats(t, clock, Stats{Guarded: true})

	device.stepWall(time.Nanosecond)
	assertNow(t, clock, start)
	assertStats(t, clock, Stats{Guarded: true, JumpsDetected: 1, JumpActive: true})
}

// A wall clock set back is taken as it is and is no jump; keeping stamps from
// moving back is the caller's part.
func TestAStepBackIsTakenAsIsAndIsNoJump(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	clock := device.clock()

	device.stepWall(-10 * 24 * time.Hour)
	assertNow(t, clock, device.wall)
	assertStats(t, clock, Stats{Guarded: true})
}

// A jump the wall clock comes back from ends; the next one is a new jump.
func TestAJumpCorrectedBackEndsAndTheNextIsCountedAgain(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	clock := device.clock()

	device.stepWall(60 * 24 * time.Hour)
	_ = clock.Now()
	device.stepWall(-60 * 24 * time.Hour)
	assertNow(t, clock, device.wall)
	assertStats(t, clock, Stats{Guarded: true, JumpsDetected: 1})

	device.stepWall(60 * 24 * time.Hour)
	_ = clock.Now()
	assertStats(t, clock, Stats{Guarded: true, JumpsDetected: 2, JumpActive: true})
}

// Without a clock since boot there is nothing to tell a step from time going
// by: the wall clock is taken as it is, and the stats say the guard is off.
func TestWithoutAClockSinceBootTheWallClockIsTakenAsIs(t *testing.T) {
	t.Parallel()
	for name, sources := range map[string]func(*fakeSources) Sources{
		"no source": func(f *fakeSources) Sources { return Sources{Wall: f.readWall} },
		"source failed": func(f *fakeSources) Sources {
			f.err = errSourceGone
			return Sources{Wall: f.readWall, SinceBoot: f.readSinceBoot}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			device := newFakeSources()
			clock := New(sources(device))
			device.stepWall(60 * 24 * time.Hour)
			assertNow(t, clock, device.wall)
			assertStats(t, clock, Stats{})
		})
	}
}

// A clock since boot that stops answering after the start holds its last
// reading: the trusted time does not move past it, so nothing ends early
// while it is gone; it resumes when the source does.
func TestAClockSinceBootThatFailsLaterHoldsItsLastReading(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	start := device.wall
	clock := device.clock()

	device.pass(time.Hour)
	assertNow(t, clock, start.Add(time.Hour))
	device.err = errSourceGone
	device.pass(2 * time.Hour)
	assertNow(t, clock, start.Add(time.Hour))

	device.err = nil
	assertNow(t, clock, device.wall)
}

// Wall is the wall clock as it reads, for figures that say when they were
// read rather than decide a term.
func TestWallIsTheWallClockAsItReads(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	clock := device.clock()
	device.stepWall(60 * 24 * time.Hour)
	if got := clock.Wall(); !got.Equal(device.wall) {
		t.Fatalf("Wall() = %v, want %v", got, device.wall)
	}
}

// Readings of concurrent Now calls may reach the bookkeeping out of order: a
// later call holding an older reading must not move the trusted time back.
func TestAnOlderReadingDoesNotMoveTheTrustedTimeBack(t *testing.T) {
	t.Parallel()
	device := newFakeSources()
	start := device.wall
	clock := device.clock()
	device.stepWall(60 * 24 * time.Hour)

	device.sinceBoot += time.Hour
	assertNow(t, clock, start.Add(time.Hour))
	device.sinceBoot -= 30 * time.Minute
	assertNow(t, clock, start.Add(time.Hour))
}
