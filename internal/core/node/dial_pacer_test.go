package node

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"
)

// virtualPacerTime runs a dialPacer in virtual time on the test goroutine:
// every wait the pacer asks for advances the manual clock by exactly that
// much and fires at once, so the instant a grant is observed at is the instant
// the pacer decided it, with no scheduler or timer granularity in between.
//
// A pacer that never grants would keep asking for waits, and virtual time
// would run on until the test binary's -timeout; after fails the test once
// maxVirtualPacerWaits waits pile up instead.
type virtualPacerTime struct {
	t     *testing.T
	clock *manualTestClock
	start time.Time
	// waits records every duration Acquire asked to sleep, in order.
	waits []time.Duration
}

// maxVirtualPacerWaits is far above what any test here asks for (a handful)
// and far below what it takes to notice a runaway loop.
const maxVirtualPacerWaits = 1000

func newVirtualPacerTime(t *testing.T) *virtualPacerTime {
	clock := newManualTestClock()
	return &virtualPacerTime{t: t, clock: clock, start: clock.now()}
}

func (v *virtualPacerTime) after(d time.Duration) <-chan time.Time {
	if len(v.waits) >= maxVirtualPacerWaits {
		v.t.Fatalf("the pacer asked to wait %d times without granting enough tokens; last waits %v",
			len(v.waits), v.waits[len(v.waits)-3:])
	}
	v.waits = append(v.waits, d)
	v.clock.advance(d)
	fired := make(chan time.Time, 1)
	fired <- v.clock.now()
	return fired
}

func (v *virtualPacerTime) newPacer(interval time.Duration, burst int) *dialPacer {
	return newDialPacer(interval, burst, dialPacerOpts{clock: v.clock.now, after: v.after})
}

// grantOffsetsHangGuard bounds each Acquire in real time. A pacer on the
// injected timer grants in microseconds; one that waits on a real timer
// instead never sees the virtual clock move and would otherwise hang until
// the test binary's -timeout. Nothing is measured against it.
const grantOffsetsHangGuard = 5 * time.Second

// grantOffsets acquires n tokens and returns the virtual instant of each
// grant as an offset from the clock's start.
func (v *virtualPacerTime) grantOffsets(t *testing.T, pacer *dialPacer, n int) []time.Duration {
	t.Helper()
	offsets := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), grantOffsetsHangGuard)
		granted := pacer.Acquire(ctx)
		cancel()
		if !granted {
			t.Fatalf("acquire %d not granted within %v of real time; virtual clock at +%v after waits %v: "+
				"the pacer is not waiting on the injected timer, or never grants",
				i, grantOffsetsHangGuard, v.clock.now().Sub(v.start), v.waits)
		}
		offsets = append(offsets, v.clock.now().Sub(v.start))
	}
	return offsets
}

func requireOffsets(t *testing.T, got []time.Duration, want ...time.Duration) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grant instants = %v, want %v", got, want)
	}
}

const testPacerInterval = 100 * time.Millisecond

func TestDialPacer_BurstIsGrantedAtOnce(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 3)

	requireOffsets(t, v.grantOffsets(t, pacer, 3), 0, 0, 0)
	if len(v.waits) != 0 {
		t.Fatalf("the burst waited %v, want no wait at all", v.waits)
	}
}

// A bucket that is full stops earning: the time it sat full is not banked as
// a fraction of the next token. Otherwise a pacer built some time before its
// first dial hands out its burst and then a paced token early. Here the
// bucket stops being full and is drained at the same instant, 30ms in, so the
// first paced grant comes one interval after that.
func TestDialPacer_TimeSpentFullIsNotCreditedToAFreshBucket(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 2)

	v.clock.advance(30 * time.Millisecond)

	requireOffsets(t, v.grantOffsets(t, pacer, 4),
		30*time.Millisecond,
		30*time.Millisecond,
		130*time.Millisecond,
		230*time.Millisecond,
	)
}

// The same invariant after an idle period: a bucket refilled to capacity and
// then left full keeps no part of the interval it was in when it filled.
func TestDialPacer_TimeSpentFullIsNotCreditedAfterIdling(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 2)

	requireOffsets(t, v.grantOffsets(t, pacer, 4), 0, 0, 100*time.Millisecond, 200*time.Millisecond)

	// Ten and a half intervals idle: enough to refill the bucket several
	// times over, ending half-way through an interval.
	v.clock.advance(1050 * time.Millisecond)

	requireOffsets(t, v.grantOffsets(t, pacer, 3),
		1250*time.Millisecond,
		1250*time.Millisecond,
		1350*time.Millisecond,
	)
}

// Accrual starts when the bucket stops being full — at the first take, not at
// the drain. The two takes from the full bucket are 60ms apart, so a pacer
// that started accruing at the drain would grant at 160ms instead of 100ms.
func TestDialPacer_AccrualStartsAtTheFirstTakeNotAtTheDrain(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 2)

	requireOffsets(t, v.grantOffsets(t, pacer, 1), 0)
	v.clock.advance(60 * time.Millisecond)
	requireOffsets(t, v.grantOffsets(t, pacer, 2), 60*time.Millisecond, 100*time.Millisecond)
}

// A bucket that is not full keeps the part of an interval it has already
// earned. Every take after the burst falls between interval boundaries, so
// the remainder, the wait for the next token and the whole-interval count are
// all on the path: the take at 60ms waits 40ms, the 150ms gap credits one
// token and keeps 50ms, which makes the next token due at 300ms.
func TestDialPacer_PartlyEarnedTokenIsKeptBetweenOffBoundaryTakes(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 2)

	requireOffsets(t, v.grantOffsets(t, pacer, 2), 0, 0)
	v.clock.advance(60 * time.Millisecond)
	requireOffsets(t, v.grantOffsets(t, pacer, 1), 100*time.Millisecond)
	v.clock.advance(150 * time.Millisecond)
	requireOffsets(t, v.grantOffsets(t, pacer, 2), 250*time.Millisecond, 300*time.Millisecond)
}

func TestDialPacer_SteadyStateGrantsAreExactlyOneIntervalApart(t *testing.T) {
	v := newVirtualPacerTime(t)
	pacer := v.newPacer(testPacerInterval, 1)

	requireOffsets(t, v.grantOffsets(t, pacer, 5),
		0,
		100*time.Millisecond,
		200*time.Millisecond,
		300*time.Millisecond,
		400*time.Millisecond,
	)
	requireOffsets(t, v.waits, testPacerInterval, testPacerInterval, testPacerInterval, testPacerInterval)
}

// burst 0 (and a negative burst, normalised to it) starts empty: the first
// token is earned one interval after construction, and the bucket still holds
// one token so a refill is not thrown away.
func TestDialPacer_ZeroBurstStartsEmpty(t *testing.T) {
	for _, burst := range []int{0, -1} {
		v := newVirtualPacerTime(t)
		pacer := v.newPacer(testPacerInterval, burst)

		requireOffsets(t, v.grantOffsets(t, pacer, 3),
			100*time.Millisecond,
			200*time.Millisecond,
			300*time.Millisecond,
		)
	}
}

func TestDialPacer_CancelledWaitReturnsFalseAndSpendsNothing(t *testing.T) {
	clock := newManualTestClock()
	var waits []time.Duration
	never := make(chan time.Time)
	pacer := newDialPacer(testPacerInterval, 1, dialPacerOpts{
		clock: clock.now,
		after: func(d time.Duration) <-chan time.Time {
			waits = append(waits, d)
			return never
		},
	})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, granted := pacer.tryAcquire(); !granted {
		t.Fatal("the burst token was refused")
	}
	if pacer.Acquire(cancelled) {
		t.Fatal("an empty bucket granted a token to a cancelled caller")
	}
	if !reflect.DeepEqual(waits, []time.Duration{testPacerInterval}) {
		t.Fatalf("waits = %v, want one wait of %v", waits, testPacerInterval)
	}

	// Exactly one token is earned in the next interval: the refused caller
	// took nothing, and nothing more than that one token is there. tryAcquire
	// rather than Acquire, so a regression fails here instead of blocking on
	// the timer that never fires.
	clock.advance(testPacerInterval)
	if _, granted := pacer.tryAcquire(); !granted {
		t.Fatal("the token earned after the cancelled wait was not available")
	}
	if wait, granted := pacer.tryAcquire(); granted || wait != testPacerInterval {
		t.Fatalf("tryAcquire = (%v, %v) right after taking the earned token, want (%v, false)",
			wait, granted, testPacerInterval)
	}
}

// A clock that reads earlier than the refill anchor would put the next token
// further away than one interval. The wait is still capped at one interval,
// so the caller re-checks instead of sleeping past the time it is owed.
func TestDialPacer_WaitNeverExceedsOneIntervalWhenTheClockStepsBack(t *testing.T) {
	clock := newManualTestClock()
	var waits []time.Duration
	never := make(chan time.Time)
	pacer := newDialPacer(testPacerInterval, 0, dialPacerOpts{
		clock: clock.now,
		after: func(d time.Duration) <-chan time.Time {
			waits = append(waits, d)
			return never
		},
	})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	clock.advance(-time.Second)

	if pacer.Acquire(cancelled) {
		t.Fatal("an empty bucket granted a token to a cancelled caller")
	}
	if !reflect.DeepEqual(waits, []time.Duration{testPacerInterval}) {
		t.Fatalf("waits = %v, want one wait of exactly %v", waits, testPacerInterval)
	}
}

// A clock that steps back credits nothing and, above all, takes nothing: the
// tokens the bucket holds are still granted, and once the clock is past the
// original anchor again the next token comes one interval after it.
func TestDialPacer_ClockSteppingBackKeepsTheTokensHeld(t *testing.T) {
	clock := newManualTestClock()
	pacer := newDialPacer(testPacerInterval, 3, dialPacerOpts{clock: clock.now})

	if _, granted := pacer.tryAcquire(); !granted {
		t.Fatal("the first burst token was refused")
	}
	clock.advance(-time.Second)
	for i := 0; i < 2; i++ {
		if wait, granted := pacer.tryAcquire(); !granted {
			t.Fatalf("take %d after the clock stepped back refused (wait %v): tokens were removed", i, wait)
		}
	}

	clock.advance(time.Second + testPacerInterval - time.Millisecond)
	if wait, granted := pacer.tryAcquire(); granted || wait != time.Millisecond {
		t.Fatalf("tryAcquire 1ms before the token is due = (%v, %v), want (1ms, false)", wait, granted)
	}
	clock.advance(time.Millisecond)
	if wait, granted := pacer.tryAcquire(); !granted {
		t.Fatalf("the token due one interval after the original anchor was refused (wait %v)", wait)
	}
}

// However long the gap, a refill credits at most the room left in the
// bucket. This pins that clamp. The overflow it guards against — adding every
// interval of the gap first and capping afterwards, which wraps the token
// count — is unreachable in practice on 64-bit: it takes the 1ns interval and
// the ~292-year gap used here. On 32-bit an int wraps after ~2^31 intervals,
// which at the production 300ms is still about twenty years of idling.
func TestDialPacer_RefillAfterAnEnormousGapFillsTheBucket(t *testing.T) {
	clock := newManualTestClock()
	pacer := newDialPacer(time.Nanosecond, 2, dialPacerOpts{clock: clock.now})

	if _, granted := pacer.tryAcquire(); !granted {
		t.Fatal("the burst token was refused")
	}
	clock.advance(time.Duration(math.MaxInt64))

	for i := 0; i < 2; i++ {
		if wait, granted := pacer.tryAcquire(); !granted {
			t.Fatalf("take %d after the gap = (%v, false), want a full bucket of 2 tokens", i, wait)
		}
	}
	if _, granted := pacer.tryAcquire(); granted {
		t.Fatal("a third token was granted at the same instant, above the capacity of 2")
	}
}

func TestDialPacer_NonPositiveIntervalDisablesPacing(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		if pacer := newDialPacer(interval, 3, dialPacerOpts{}); pacer != nil {
			t.Fatalf("newDialPacer(%v) = %+v, want nil", interval, pacer)
		}
	}
}

func TestDialPacer_NilTimeSourcesFallBackToTheRealClock(t *testing.T) {
	pacer := newDialPacer(time.Hour, 1, dialPacerOpts{})
	if pacer.clock == nil || pacer.after == nil {
		t.Fatal("a nil clock or timer was kept instead of falling back to the real one")
	}
	if pacer.lastRefill.IsZero() {
		t.Fatal("the refill anchor was not read from the fallback clock")
	}
	if _, granted := pacer.tryAcquire(); !granted {
		t.Fatal("the burst token was refused")
	}
}
