package trustedclock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// Only test sources own these seams; production has no mutable hook. A result
// is captured before a scripted pause, modelling a late completed older read.
type guardTestDevice struct {
	mu                   sync.Mutex
	wall                 time.Time
	boot                 time.Duration
	err                  error
	wallCalls, bootCalls int
	afterBoot            func(int)
	onWall               func()
}

func newGuardTestDevice() *guardTestDevice {
	return &guardTestDevice{wall: time.Unix(1780000000, 0).UTC()}
}
func (d *guardTestDevice) sources() Sources {
	return Sources{Wall: func() time.Time {
		d.mu.Lock()
		d.wallCalls++
		w := d.wall
		d.mu.Unlock()
		if d.onWall != nil {
			d.onWall()
		}
		return w
	}, SinceBoot: func() (time.Duration, error) {
		d.mu.Lock()
		d.bootCalls++
		n, value, err := d.bootCalls, d.boot, d.err
		d.mu.Unlock()
		if d.afterBoot != nil {
			d.afterBoot(n)
		}
		return value, err
	}}
}
func (d *guardTestDevice) at(boot time.Duration, active bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.boot = boot
	d.wall = time.Unix(1780000000, 0).UTC().Add(boot)
	if active {
		d.wall = d.wall.Add(2 * JumpTolerance)
	}
}
func (d *guardTestDevice) fail(err error) { d.mu.Lock(); d.err = err; d.mu.Unlock() }
func guardTestInstance(n byte) [16]byte   { var id [16]byte; id[0] = n; return id }
func guardRequireComplete(t *testing.T, c *Clock) guardDurationSnapshot {
	t.Helper()
	s := c.guardDuration()
	if s.Status != "COMPLETE" {
		t.Fatalf("covered cut unavailable: %+v", s)
	}
	return s
}
func guardWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("controlled caller did not reach its scripted boundary")
	}
}

// Cleanup owns only workers actually registered before their go statement.
// It releases ALL scripted blockers before waiting for any owner. An arbitrary
// production mutex deadlock cannot be forcibly undone: a missing join is an
// additional test failure, never an eligible semantic-only mutation result.
func guardOwnedCleanup(t *testing.T, releases ...func()) func(<-chan struct{}) {
	t.Helper()
	var owners []<-chan struct{}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
		guardJoinWorkers(t, owners)
	})
	return func(done <-chan struct{}) { owners = append(owners, done) }
}

// One original three-second ceiling is shared across the whole join pass.
// Error rather than Fatal makes every remaining owner observable on timeout.
func guardJoinWorkers(t *testing.T, owners []<-chan struct{}) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	joined := true
	for i, done := range owners {
		select {
		case <-done:
			continue
		default:
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Errorf("owned worker %d did not join within cleanup 3s", i)
			joined = false
			continue
		}
		timer := time.NewTimer(remaining)
		select {
		case <-done:
		case <-timer.C:
			t.Errorf("owned worker %d did not join within cleanup 3s", i)
			joined = false
		}
		timer.Stop()
	}
	return joined
}

func TestS6GuardDurationOriginalBehavior(t *testing.T) {
	t.Run("nil-path-source-count", func(t *testing.T) {
		d := newGuardTestDevice()
		c := New(d.sources())
		for _, active := range []bool{false, true, true, false} {
			d.at(time.Minute, active)
			_ = c.Now()
		}
		if d.bootCalls != 5 || d.wallCalls != 5 || c.guardDuration().Status != "INACTIVE" {
			t.Fatal("nil observer changed original source calls", d.bootCalls, d.wallCalls)
		}
	})
	t.Run("same-original-decisions", func(t *testing.T) {
		a, b := newGuardTestDevice(), newGuardTestDevice()
		plain := New(a.sources())
		observed := newObservedClock(b.sources(), guardTestInstance(1))
		for i, active := range []bool{false, true, true, false, true, false} {
			at := time.Duration(i) * time.Minute
			a.at(at, active)
			b.at(at, active)
			if !plain.Now().Equal(observed.Now()) || plain.Stats() != observed.Stats() {
				t.Fatal("observer changed original security time or bookkeeping", i)
			}
			p, pok := plain.TakeUnreportedJump()
			q, qok := observed.TakeUnreportedJump()
			if pok != qok || p != q {
				t.Fatal("observer changed journal handoff", i, p, q)
			}
		}
		a.fail(errSourceGone)
		b.fail(errSourceGone)
		a.at(4*time.Hour, true)
		b.at(4*time.Hour, true)
		if !plain.Now().Equal(observed.Now()) || plain.Stats() != observed.Stats() {
			t.Fatal("observation error changed original hold-last fallback")
		}
		if s := observed.guardDuration(); s.Status != "UNKNOWN" || s.Reason != "source-error" {
			t.Fatal("original read failure was not covered", s)
		}
	})
	t.Run("tolerance-boundary", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(2))
		d.mu.Lock()
		d.wall = d.wall.Add(JumpTolerance)
		d.mu.Unlock()
		_ = c.Now()
		if c.Stats().JumpActive {
			t.Fatal("tolerance equality changed")
		}
		d.mu.Lock()
		d.wall = d.wall.Add(time.Nanosecond)
		d.mu.Unlock()
		_ = c.Now()
		if !c.Stats().JumpActive || guardRequireComplete(t, c).Sequence != 1 {
			t.Fatal("tolerance+1 lost real transition")
		}
	})
}

func TestS6GuardDurationTrueTransitions(t *testing.T) {
	for _, row := range []struct {
		name    string
		times   []time.Duration
		maximum time.Duration
	}{
		{"two-thirty-five", []time.Duration{0, 35 * time.Minute, 40 * time.Minute, 75 * time.Minute}, 35 * time.Minute},
		{"sixty-and-ten", []time.Duration{0, 60 * time.Minute, 65 * time.Minute, 75 * time.Minute}, 60 * time.Minute},
	} {
		t.Run(row.name, func(t *testing.T) {
			d := newGuardTestDevice()
			c := newObservedClock(d.sources(), guardTestInstance(3))
			initial := guardRequireComplete(t, c)
			if initial.ClosedCount != 0 || initial.MaxClosedUpper != 0 || initial.TotalUpper != 0 {
				t.Fatal("fabricated initial episode", initial)
			}
			for i, at := range row.times {
				d.at(at, i%2 == 0)
				_ = c.Now()
				_ = c.Now()
			}
			s := guardRequireComplete(t, c)
			if s.Active || s.Sequence != 4 || s.ClosedCount != 2 || s.TotalLower != 70*time.Minute || s.TotalUpper != s.TotalLower || s.CurrentUpper != 0 || s.MaxClosedLower != row.maximum || s.MaxClosedUpper != row.maximum {
				t.Fatal("total/current/closed-maximum conflated", s)
			}
		})
	}
	t.Run("fresh-current-no-persisted-cut", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(4))
		d.at(time.Minute, true)
		_ = c.Now()
		for _, at := range []time.Duration{5 * time.Minute, 7 * time.Minute, 7 * time.Minute} {
			d.at(at, true)
			s := guardRequireComplete(t, c)
			if s.TotalLower != at-time.Minute || s.TotalUpper != s.TotalLower || s.CurrentLower != s.TotalLower || s.CurrentUpper != s.TotalUpper || s.ClosedCount != 0 {
				t.Fatal("cut cached or accumulated into persisted sums", at, s)
			}
		}
	})
}

func TestS6GuardDurationLateOldRead(t *testing.T) {
	t.Run("same-state-older-arrives-last", func(t *testing.T) {
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		track := guardOwnedCleanup(t, releaseOnce)
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			if n == 3 {
				close(started)
				<-release
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(5))
		d.at(time.Minute, false)
		track(done)
		go func() { defer close(done); _ = c.Now() }()
		guardWait(t, started)
		d.at(2*time.Minute, false)
		_ = c.Now()
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("unvalidated original reading did not exclude cut", s)
		}
		releaseOnce()
		guardWait(t, done)
		s := guardRequireComplete(t, c)
		if s.TotalUpper != 0 || c.lastSinceBoot != 2*time.Minute {
			t.Fatal("overlap called regression or moved trusted clock back", s, c.lastSinceBoot)
		}
	})
	t.Run("reverse-paired-completions-across-newer-episode", func(t *testing.T) {
		entryStarted, exitStarted := make(chan struct{}), make(chan struct{})
		entryRelease, exitRelease := make(chan struct{}), make(chan struct{})
		entryDone, exitDone := make(chan struct{}), make(chan struct{})
		releaseEntry := sync.OnceFunc(func() { close(entryRelease) })
		releaseExit := sync.OnceFunc(func() { close(exitRelease) })
		track := guardOwnedCleanup(t, releaseEntry, releaseExit)
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			switch n {
			case 4:
				close(entryStarted)
				<-entryRelease
			case 6:
				close(exitStarted)
				<-exitRelease
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(6))
		d.at(0, true)
		track(entryDone)
		go func() { defer close(entryDone); _ = c.Now() }()
		guardWait(t, entryStarted)
		d.at(35*time.Minute, false)
		track(exitDone)
		go func() { defer close(exitDone); _ = c.Now() }()
		guardWait(t, exitStarted)
		d.at(40*time.Minute, true)
		_ = c.Now()
		d.at(75*time.Minute, false)
		_ = c.Now()
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("pending old pair certified max", s)
		}
		releaseExit()
		guardWait(t, exitDone)
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("exit alone consumed delayed entry", s)
		}
		releaseEntry()
		guardWait(t, entryDone)
		s := guardRequireComplete(t, c)
		if s.Sequence != 4 || s.ClosedCount != 2 || s.TotalLower != 70*time.Minute || s.TotalUpper != s.TotalLower || s.CurrentUpper != 0 || s.MaxClosedLower != 35*time.Minute || s.MaxClosedUpper != 35*time.Minute {
			t.Fatal("old pair stolen, counted twice or reordered", s)
		}
	})
}

func TestS6GuardDurationFreshCut(t *testing.T) {
	t.Run("wall-provider-in-flight", func(t *testing.T) {
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		track := guardOwnedCleanup(t, releaseOnce)
		d := newGuardTestDevice()
		var c *Clock
		d.onWall = func() {
			if c != nil {
				close(started)
				<-release
			}
		}
		c = newObservedClock(d.sources(), guardTestInstance(22))
		track(done)
		go func() { defer close(done); _ = c.Now() }()
		guardWait(t, started)
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("original wall provider omitted from coverage", s)
		}
		releaseOnce()
		guardWait(t, done)
		_ = guardRequireComplete(t, c)
	})
	t.Run("returned-error-awaits-validation", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(7))
		// The real owner primitive obtains the real provider result, but keeps its
		// token through delayed validation. No external completion API is added.
		r := c.beginGuardRead(true)
		d.fail(errSourceGone)
		value, err := c.sinceBoot()
		d.fail(nil)
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("returned provider error vanished before validation", s)
		}
		c.mu.Lock()
		c.validateGuardReadLocked(r, value, err)
		c.retireGuardReadLocked(r)
		c.mu.Unlock()
		if s := c.guardDuration(); s.Status != "UNKNOWN" || s.Reason != "source-error" {
			t.Fatal("later success bridged source error", s)
		}
	})
	t.Run("returned-older-success-awaits-validation", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(8))
		d.at(time.Minute, false)
		r := c.beginGuardRead(true)
		value, err := c.sinceBoot()
		d.at(2*time.Minute, false)
		_ = c.Now()
		if s := c.guardDuration(); s.Status != "IN_FLIGHT" {
			t.Fatal("returned older result vanished", s)
		}
		c.mu.Lock()
		valid := c.validateGuardReadLocked(r, value, err)
		c.retireGuardReadLocked(r)
		c.mu.Unlock()
		if !valid {
			t.Fatal("overlap-only result was compared to later maximum")
		}
		_ = guardRequireComplete(t, c)
	})
	t.Run("transition-during-cut", func(t *testing.T) {
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		track := guardOwnedCleanup(t, releaseOnce)
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			if n == 3 {
				close(started)
				<-release
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(9))
		var cut guardDurationSnapshot
		track(done)
		go func() { defer close(done); cut = c.guardDuration() }()
		guardWait(t, started)
		d.at(time.Minute, true)
		_ = c.Now()
		releaseOnce()
		guardWait(t, done)
		if cut.Status != "UNSTABLE_CUT" {
			t.Fatal("torn state/time cut certified complete", cut)
		}
		_ = guardRequireComplete(t, c)
	})
	t.Run("other-provider-still-live-at-cut-finish", func(t *testing.T) {
		cutStarted, readStarted := make(chan struct{}), make(chan struct{})
		cutRelease, readRelease := make(chan struct{}), make(chan struct{})
		cutDone, readDone := make(chan struct{}), make(chan struct{})
		releaseCut := sync.OnceFunc(func() { close(cutRelease) })
		releaseRead := sync.OnceFunc(func() { close(readRelease) })
		track := guardOwnedCleanup(t, releaseCut, releaseRead)
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			switch n {
			case 3:
				close(cutStarted)
				<-cutRelease
			case 4:
				close(readStarted)
				<-readRelease
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(10))
		var cut guardDurationSnapshot
		track(cutDone)
		go func() { defer close(cutDone); cut = c.guardDuration() }()
		guardWait(t, cutStarted)
		track(readDone)
		go func() { defer close(readDone); _ = c.Now() }()
		guardWait(t, readStarted)
		releaseCut()
		guardWait(t, cutDone)
		if cut.Status != "UNSTABLE_CUT" {
			t.Fatal("cut excluded another provider as if its own", cut)
		}
		releaseRead()
		guardWait(t, readDone)
		_ = guardRequireComplete(t, c)
	})
}

func TestS6GuardDurationClockFailureCoverage(t *testing.T) {
	t.Run("cut-read-error", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(19))
		d.fail(errSourceGone)
		if s := c.guardDuration(); s.Status != "UNKNOWN" || s.Reason != "source-error" {
			t.Fatal("cut read error called covered", s)
		}
		d.fail(nil)
		if s := c.guardDuration(); s.Status != "UNKNOWN" {
			t.Fatal("cut gap recovered", s)
		}
	})
	t.Run("transition-post-read-error", func(t *testing.T) {
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			if n == 3 {
				d.fail(errSourceGone)
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(20))
		d.at(time.Minute, true)
		got := c.Now()
		if !got.Equal(time.Unix(1780000000, 0).UTC().Add(time.Minute)) || !c.Stats().JumpActive {
			t.Fatal("post-read error changed original decision", got, c.Stats())
		}
		d.fail(nil)
		if s := c.guardDuration(); s.Status != "UNKNOWN" || s.Reason != "source-error" {
			t.Fatal("transition post error lost", s)
		}
		c.mu.Lock()
		pending := c.observation.pending
		c.mu.Unlock()
		if pending != 0 {
			t.Fatal("failed post-read leaked owner", pending)
		}
	})
	t.Run("initial-coverage-cut-error", func(t *testing.T) {
		d := newGuardTestDevice()
		d.afterBoot = func(n int) {
			if n == 1 {
				d.fail(errSourceGone)
			}
		}
		c := newObservedClock(d.sources(), guardTestInstance(21))
		if !c.Stats().Guarded || c.guardDuration().Status != "UNKNOWN" {
			t.Fatal("coverage initialization error changed guard or fabricated coverage")
		}
	})
	for _, kind := range []string{"error", "negative", "regression"} {
		t.Run(kind, func(t *testing.T) {
			d := newGuardTestDevice()
			c := newObservedClock(d.sources(), guardTestInstance(11))
			d.at(time.Minute, false)
			_ = c.Now()
			switch kind {
			case "error":
				d.fail(errSourceGone)
			case "negative":
				d.at(-time.Second, false)
			case "regression":
				d.at(time.Second, false)
			}
			_ = c.Now()
			first := c.guardDuration()
			d.fail(nil)
			d.at(2*time.Minute, false)
			_ = c.Now()
			later := c.guardDuration()
			if first.Status != "UNKNOWN" || later.Status != "UNKNOWN" || first.Reason != later.Reason || later.ClosedCount != 0 {
				t.Fatal("invalid epoch recovered or exposed partial history", first, later)
			}
			fresh := newObservedClock(d.sources(), guardTestInstance(12))
			s := guardRequireComplete(t, fresh)
			if s.Instance == first.Instance || s.TotalUpper != 0 || s.CoverageStart != 2*time.Minute {
				t.Fatal("restart stitched prior gap", s, first)
			}
		})
	}
	t.Run("unsupported-and-failed-initialization", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(Sources{Wall: d.sources().Wall}, guardTestInstance(13))
		if c.guardDuration().Status != "UNKNOWN" || c.Stats().Guarded {
			t.Fatal("unsupported initialization called covered")
		}
		d.fail(errors.New("initial failure"))
		c = newObservedClock(d.sources(), guardTestInstance(14))
		if c.guardDuration().Status != "UNKNOWN" || c.Stats().Guarded {
			t.Fatal("failed constructor called covered")
		}
		d.fail(nil)
		c = newObservedClock(d.sources(), [16]byte{})
		if c.guardDuration().Status != "UNKNOWN" {
			t.Fatal("unattributable instance called covered")
		}
	})
	t.Run("post-read-invalid-bracket", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(15))
		c.mu.Lock()
		p := c.guardTransitionLocked(time.Minute, true, true)
		c.mu.Unlock()
		// Same internal completion owner as Now; a post-read cannot precede L.
		c.completeGuardTransition(p)
		if s := c.guardDuration(); s.Status != "UNKNOWN" {
			t.Fatal("U<L bracket called covered", s)
		}
		c.mu.Lock()
		pending := c.observation.pending
		c.mu.Unlock()
		if pending != 0 {
			t.Fatal("invalid completion leaked its own pending slot", pending)
		}
	})
	t.Run("counter-arithmetic-and-duplicate-exhaustion", func(t *testing.T) {
		for _, kind := range []string{"read", "sequence", "pending", "pair", "count", "arithmetic", "duplicate-read", "duplicate-completion"} {
			t.Run(kind, func(t *testing.T) {
				d := newGuardTestDevice()
				c := newObservedClock(d.sources(), guardTestInstance(16))
				c.mu.Lock()
				switch kind {
				case "read":
					c.observation.nextRead = guardMaxUint
				case "sequence":
					c.observation.sequence = guardMaxUint
					c.guardTransitionLocked(0, true, true)
				case "pending":
					c.observation.pending = guardMaxUint
					c.guardTransitionLocked(0, true, true)
				case "pair":
					c.guardTransitionLocked(0, true, true)
					c.observation.pendingPairs = guardMaxUint
					c.guardTransitionLocked(0, true, false)
				case "count":
					e := &guardEpisode{owner: c.observation, entryDone: true, exitDone: true, entryValid: true, exitValid: true, closed: true}
					c.observation.pendingPairs = 1
					c.observation.closedCount = guardMaxUint
					c.reduceGuardEpisodeLocked(e)
				case "arithmetic":
					c.observation.totalLower = guardMaxDuration
					c.observation.add(&c.observation.totalLower, 1)
				case "duplicate-read":
					r := c.beginGuardReadLocked(true)
					c.validateGuardReadLocked(r, 0, nil)
					c.retireGuardReadLocked(r)
					c.retireGuardReadLocked(r)
				case "duplicate-completion":
					p := c.guardTransitionLocked(0, true, true)
					c.finishGuardTransitionLocked(p, 0, true)
					c.finishGuardTransitionLocked(p, 0, true)
				}
				c.mu.Unlock()
				if kind == "read" {
					_ = c.Now()
				}
				if s := c.guardDuration(); s.Status != "UNKNOWN" {
					t.Fatal("exhaustion/duplicate silently wrapped or counted twice", kind, s)
				}
			})
		}
	})
}

func TestS6GuardDurationLeafAndSuspend(t *testing.T) {
	t.Run("every-source-reenters-stats", func(t *testing.T) {
		d := newGuardTestDevice()
		var c *Clock
		d.onWall = func() {
			if c != nil {
				_ = c.Stats()
			}
		}
		d.afterBoot = func(int) {
			if c != nil {
				_ = c.Stats()
			}
		}
		c = newObservedClock(d.sources(), guardTestInstance(17))
		done := make(chan struct{})
		track := guardOwnedCleanup(t)
		track(done)
		go func() {
			defer close(done)
			d.at(0, true)
			_ = c.Now()
			_ = c.Wall()
			_ = c.guardDuration()
			d.at(2*time.Hour, false)
			_ = c.Now()
		}()
		guardWait(t, done)
		if s := guardRequireComplete(t, c); s.TotalLower != 2*time.Hour || s.MaxClosedLower != 2*time.Hour {
			t.Fatal("suspend duration lost", s)
		}
	})
	t.Run("concurrent-real-owners", func(t *testing.T) {
		d := newGuardTestDevice()
		c := newObservedClock(d.sources(), guardTestInstance(18))
		d.at(time.Minute, true)
		_ = c.Now()
		stop := make(chan struct{})
		stopOnce := sync.OnceFunc(func() { close(stop) })
		track := guardOwnedCleanup(t, stopOnce)
		var owners []<-chan struct{}
		for i := 0; i < 8; i++ {
			done := make(chan struct{})
			track(done)
			owners = append(owners, done)
			go func() {
				defer close(done)
				for j := 0; j < 32; j++ {
					select {
					case <-stop:
						return
					default:
					}
					_ = c.Now()
					_ = c.Stats()
					s := c.guardDuration()
					if s.Status != "COMPLETE" && s.Status != "IN_FLIGHT" && s.Status != "UNSTABLE_CUT" {
						t.Errorf("valid concurrent source invalidated: %+v", s)
						return
					}
				}
			}()
		}
		if !guardJoinWorkers(t, owners) {
			return
		}
		_ = guardRequireComplete(t, c)
	})
}
