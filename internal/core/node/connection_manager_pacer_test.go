package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// ---------------------------------------------------------------------------
// Dial pacer — global rate limiter on outbound dial goroutines.
//
// Without the pacer, fill() spawns N dial workers in a single tight loop
// (one per free slot). When the CM is in a reconnect storm against the
// same handful of bootstrap nodes, this produces a thundering herd: every
// 2-3 seconds, 7 simultaneous TCP handshakes + welcome + auth + peer-side
// announce_routes flush. CPU climbs to 100% while no session ever
// completes setup.
//
// The pacer is a token bucket:
//   - capacity = max(DialPacerBurst, 1)
//   - refill rate = 1 token per DialPacerInterval
//
// Production (service.go) wires burst 3 and 300ms. The config default,
// interval 0, disables the pacer.
//
// Acquired before every dial worker (fill, retryAfterBackoff, reconnect)
// EXCEPT for ManualPeerRequested — operator-driven actions bypass the
// pacer so `addpeer` is immediate.
//
// These tests run the pacer on pacerTestTime: the manager's clock
// (NowFn) and the pacer's timer (DialPacerAfterFn) move only when the test
// advances them, so every dial instant asserted below is exact and no
// assertion depends on how fast the machine is.
// ---------------------------------------------------------------------------

// pacerDial is one DialFn call: whom it dialled and the virtual instant it
// did so, as an offset from the clock's start.
type pacerDial struct {
	address domain.PeerAddress
	at      time.Duration
}

// pacerTestTime is a hand-driven clock with timers that the dial workers'
// goroutines may use concurrently. Unlike manualTestClock it is guarded by a
// mutex, because the pacer reads it from every dial worker at once.
//
// The test decides when time moves; it only advances once the manager has
// settled, i.e. every paced dial worker has either dialled or armed a timer.
// changed is signalled after every arm and every dial so the test waits for
// that state instead of sleeping.
type pacerTestTime struct {
	mu      sync.Mutex
	start   time.Time
	at      time.Time
	timers  []pacerTestTimer
	dials   []pacerDial
	changed chan struct{}
}

type pacerTestTimer struct {
	deadline time.Time
	fired    chan time.Time
}

func newPacerTestTime() *pacerTestTime {
	start := time.Unix(1780000000, 0).UTC()
	return &pacerTestTime{start: start, at: start, changed: make(chan struct{}, 1)}
}

func (c *pacerTestTime) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *pacerTestTime) after(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	timer := pacerTestTimer{deadline: c.at.Add(d), fired: make(chan time.Time, 1)}
	if d <= 0 {
		timer.fired <- c.at
	} else {
		c.timers = append(c.timers, timer)
	}
	c.mu.Unlock()
	c.signal()
	return timer.fired
}

// advance moves the clock and fires every timer that is due by then.
func (c *pacerTestTime) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advanceLocked(c.at.Add(d))
}

// advanceToNextDeadline moves the clock to the earliest armed deadline, so
// a waiter observes time at the very instant its pacer asked to be woken —
// never later, which would hide a token granted too early. There must be an
// armed timer.
func (c *pacerTestTime) advanceToNextDeadline(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers) == 0 {
		t.Fatal("no armed timer to advance to")
	}
	next := c.timers[0].deadline
	for _, timer := range c.timers[1:] {
		if timer.deadline.Before(next) {
			next = timer.deadline
		}
	}
	c.advanceLocked(next)
}

// advanceLocked sets the clock to target and fires every timer due by then.
// Caller holds c.mu.
func (c *pacerTestTime) advanceLocked(target time.Time) {
	c.at = target
	pending := c.timers[:0]
	for _, timer := range c.timers {
		if timer.deadline.After(c.at) {
			pending = append(pending, timer)
			continue
		}
		timer.fired <- c.at
	}
	c.timers = pending
}

func (c *pacerTestTime) recordDial(address domain.PeerAddress) {
	c.mu.Lock()
	c.dials = append(c.dials, pacerDial{address: address, at: c.at.Sub(c.start)})
	c.mu.Unlock()
	c.signal()
}

func (c *pacerTestTime) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// snapshot returns the dials so far and the number of armed timers.
func (c *pacerTestTime) snapshot() ([]pacerDial, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]pacerDial(nil), c.dials...), len(c.timers)
}

// waitSettled blocks until settled reports true for the current dials and
// armed timers. The timeout only turns a hang into a failure; nothing is
// measured against it.
func (c *pacerTestTime) waitSettled(t *testing.T, desc string, settled func(dials []pacerDial, armed int) bool) []pacerDial {
	t.Helper()
	guard := time.After(10 * time.Second)
	for {
		dials, armed := c.snapshot()
		if settled(dials, armed) {
			return dials
		}
		select {
		case <-c.changed:
		case <-guard:
			t.Fatalf("timed out waiting for %s: dials=%v armed timers=%d", desc, dials, armed)
		}
	}
}

// advanceUntilDialled steps the clock from deadline to deadline until want
// dials have happened, waiting for the manager to settle after each step. A
// pacer that keeps re-arming without granting would otherwise step virtual
// time on until the test binary's -timeout, so the number of steps is bounded
// by maxSteps.
func (c *pacerTestTime) advanceUntilDialled(t *testing.T, want, maxSteps int, settled func(dials []pacerDial, armed int) bool) []pacerDial {
	t.Helper()
	dials := c.waitSettled(t, "the first dials", settled)
	for step := 0; len(dials) < want; step++ {
		if step == maxSteps {
			_, armed := c.snapshot()
			t.Fatalf("%d dials after %d deadline steps, want %d: dials=%v armed timers=%d",
				len(dials), maxSteps, want, dials, armed)
		}
		c.advanceToNextDeadline(t)
		dials = c.waitSettled(t, "the next paced dial", settled)
	}
	return dials
}

// pacedCMConfig is testCMConfig with the manager's clock and the pacer's
// timer on vt and every dial recorded there.
func pacedCMConfig(vt *pacerTestTime, interval time.Duration, burst int, addresses ...string) *testCMBuilder {
	b := testCMConfig(addresses...)
	b.Cfg.DialPacerInterval = interval
	b.Cfg.DialPacerBurst = burst
	b.Cfg.NowFn = vt.now
	b.Cfg.DialPacerAfterFn = vt.after
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		vt.recordDial(a[0])
		session := fakePeerSession(a[0], domaintest.ID("id-"+string(a[0])))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}
	return b
}

func dialInstants(dials []pacerDial) []time.Duration {
	instants := make([]time.Duration, len(dials))
	for i, dial := range dials {
		instants[i] = dial.at
	}
	return instants
}

// TestCM_DialPacer_LimitsSpawnRate verifies that fill() does NOT launch all
// N dial workers at once: the burst dials at once, and every further dial
// comes exactly one interval after the previous one.
//
// The manager is built some time before bootstrap completes, as it is in
// production (NewService builds it, bootstrap finishes later). The time its
// full bucket waited must not shorten the first paced interval.
func TestCM_DialPacer_LimitsSpawnRate(t *testing.T) {
	t.Parallel()

	const (
		peers    = 5
		burst    = 2
		interval = 100 * time.Millisecond
		idle     = 30 * time.Millisecond
	)
	addrs := make([]string, peers)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	vt := newPacerTestTime()
	b := pacedCMConfig(vt, interval, burst, addrs...)
	b.Cfg.MaxSlotsFn = func() int { return peers }

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	vt.advance(idle)
	cm.NotifyBootstrapReady()

	allWorkersSettled := func(dials []pacerDial, armed int) bool { return len(dials)+armed == peers }
	dials := vt.advanceUntilDialled(t, peers, 2*peers, allWorkersSettled)

	want := []time.Duration{idle, idle, idle + interval, idle + 2*interval, idle + 3*interval}
	if got := dialInstants(dials); !reflect.DeepEqual(got, want) {
		t.Fatalf("dial instants = %v, want %v (burst %d at once, then one per %v)", got, want, burst, interval)
	}
}

// TestCM_DialPacer_ManualBypasses checks that a ManualPeerRequested dial
// fires at once while a paced dial is still waiting for a token that time,
// which never moves in this test, would have to earn.
func TestCM_DialPacer_ManualBypasses(t *testing.T) {
	t.Parallel()

	paced := mustAddrN(0)
	vt := newPacerTestTime()
	b := pacedCMConfig(vt, time.Second, 0, paced)
	b.Cfg.MaxSlotsFn = func() int { return 4 }

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()
	vt.waitSettled(t, "the paced dial to wait for a token", func(dials []pacerDial, armed int) bool {
		return len(dials) == 0 && armed == 1
	})

	manual := mustAddr("10.0.0.99:64646")
	cm.EmitSlot(ManualPeerRequested{
		Address:       manual,
		DialAddresses: []domain.PeerAddress{manual},
	})

	dials := vt.waitSettled(t, "the manual dial", func(dials []pacerDial, _ int) bool { return len(dials) == 1 })
	if want := []pacerDial{{address: manual, at: 0}}; !reflect.DeepEqual(dials, want) {
		t.Fatalf("dials = %v, want only the manual peer at the start instant %v", dials, want)
	}
	if _, armed := vt.snapshot(); armed != 1 {
		t.Fatalf("armed timers = %d, want the paced dial still waiting", armed)
	}
}

// TestCM_DialPacer_RetryWaitsForAToken verifies that a retry after a failed
// dial takes a pacer token like any other paced dial: the first dial took
// the only token, so the bucket stopped being full at that instant and the
// retry comes exactly one interval after it instead of as soon as its (zero)
// backoff ends.
func TestCM_DialPacer_RetryWaitsForAToken(t *testing.T) {
	t.Parallel()

	const interval = 100 * time.Millisecond
	peer := mustAddrN(0)
	vt := newPacerTestTime()
	b := pacedCMConfig(vt, interval, 1, peer)
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	// The backoff is zero so retryAfterBackoff's own wait, which is not on the
	// virtual clock, ends at once and the pacer is the only thing holding the
	// retry back.
	b.Cfg.BackoffFn = func(int) time.Duration { return 0 }
	var dialled atomic.Int32
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		vt.recordDial(a[0])
		if dialled.Add(1) == 1 {
			return DialResult{}, errors.New("first dial refused")
		}
		session := fakePeerSession(a[0], domaintest.ID("id-"+string(a[0])))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// One slot, one attempt in flight at a time: it has either dialled twice
	// or dialled once and is waiting on a timer.
	attemptSettled := func(dials []pacerDial, armed int) bool { return len(dials)+armed == 2 }
	dials := vt.advanceUntilDialled(t, 2, 2, attemptSettled)

	want := []pacerDial{{address: mustAddr(peer), at: 0}, {address: mustAddr(peer), at: interval}}
	if !reflect.DeepEqual(dials, want) {
		t.Fatalf("dials = %v, want the first dial at once and the retry one interval later %v", dials, want)
	}
}

// TestCM_DialPacer_BurstAllowsParallelStart verifies that with burst=N,
// the first N dials are issued at the same instant (no pacing delay between
// them). This is the cold-start optimisation: when there are no pending
// dials, we should not artificially slow down filling empty slots.
func TestCM_DialPacer_BurstAllowsParallelStart(t *testing.T) {
	t.Parallel()

	const burst = 3
	addrs := make([]string, burst)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	vt := newPacerTestTime()
	b := pacedCMConfig(vt, 500*time.Millisecond, burst, addrs...)
	b.Cfg.MaxSlotsFn = func() int { return burst }

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	dials := vt.waitSettled(t, "the burst dials", func(dials []pacerDial, _ int) bool { return len(dials) == burst })
	if got, want := dialInstants(dials), []time.Duration{0, 0, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dial instants = %v, want %v", got, want)
	}
	if _, armed := vt.snapshot(); armed != 0 {
		t.Fatalf("armed timers = %d, want no dial waiting on the pacer", armed)
	}
}

// TestCM_DialPacer_NilDisabled ensures the pacer is opt-in: a
// configuration with DialPacerInterval=0 behaves exactly like before
// (no rate limit). Critical so existing tests and production code without
// explicit pacer config are not accidentally throttled.
func TestCM_DialPacer_NilDisabled(t *testing.T) {
	t.Parallel()

	const peers = 4
	addrs := make([]string, peers)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	b := testCMConfig(addrs...)
	b.Cfg.MaxSlotsFn = func() int { return peers }
	// DialPacerInterval zero → disabled.
	b.Cfg.DialPacerInterval = 0
	b.Cfg.DialPacerBurst = 0
	var dialCount atomic.Int32
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		dialCount.Add(1)
		session := fakePeerSession(a[0], domaintest.ID("id-"+string(a[0])))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	if cm.pacer != nil {
		t.Fatalf("pacer = %+v with DialPacerInterval 0, want nil", cm.pacer)
	}
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 5*time.Second, "all dials fired", func() bool {
		return dialCount.Load() >= int32(peers)
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// mustAddrN returns "10.0.0.{i+1}:64646" for use as a synthetic test peer.
// Starts at .1 to avoid the .0 broadcast/network edge that some parsers
// treat as invalid, and caps at .254.
func mustAddrN(i int) string {
	if i < 0 || i > 253 {
		panic("mustAddrN: index out of range")
	}
	return fmt.Sprintf("10.0.0.%d:64646", i+1)
}
