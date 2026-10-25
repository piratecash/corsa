package connbudget

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type totalTestClock struct {
	mu    sync.Mutex
	at    time.Duration
	err   error
	calls int
	after func(int)
}

func (d *totalTestClock) read() (time.Duration, error) {
	d.mu.Lock()
	d.calls++
	n, at, err := d.calls, d.at, d.err
	d.mu.Unlock()
	if d.after != nil {
		d.after(n)
	}
	return at, err
}
func (d *totalTestClock) set(at time.Duration, err error) {
	d.mu.Lock()
	d.at, d.err = at, err
	d.mu.Unlock()
}
func (d *totalTestClock) count() int { d.mu.Lock(); defer d.mu.Unlock(); return d.calls }
func totalKey(n byte) totalSourceKey {
	return totalSourceKey{anchor: "test-anchor", epoch: [16]byte{n}, clockDomain: [16]byte{2}, configuration: [16]byte{3}}
}
func totalObserved(t *testing.T, cfg Config, d *totalTestClock, capacity int) *Budget {
	t.Helper()
	b, err := newObservedBudget(cfg, totalKey(1), capacity, d.read)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func totalTake(t *testing.T, b *Budget, dir Direction) *Reservation {
	t.Helper()
	r, err := b.Reserve(dir)
	if err != nil {
		t.Fatal("original admission refused", err)
	}
	return r
}
func totalComplete(t *testing.T, b *Budget) totalFullSnapshot {
	t.Helper()
	s := b.fullSnapshot()
	if s.status != "COMPLETE" || s.pastOnly {
		t.Fatal("current cut not complete", s)
	}
	return s
}
func totalBounds(t *testing.T, s totalFullSnapshot, lower, upper time.Duration) {
	t.Helper()
	lo, hi, ok := totalFullDurationBounds(s)
	if !ok || lo != lower || hi != upper {
		t.Fatalf("covered prefix bounds=(%v,%v,%v), want(%v,%v,true): %+v", lo, hi, ok, lower, upper, s)
	}
}
func totalWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("controlled owner boundary unavailable")
	}
}
func totalCleanup(t *testing.T, releases ...func()) func(<-chan struct{}) {
	t.Helper()
	var owners []<-chan struct{}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
		end := time.Now().Add(3 * time.Second)
		for i, done := range owners {
			select {
			case <-done:
				continue
			default:
			}
			left := time.Until(end)
			if left <= 0 {
				t.Errorf("owned worker %d cleanup join missing", i)
				continue
			}
			timer := time.NewTimer(left)
			select {
			case <-done:
			case <-timer.C:
				t.Errorf("owned worker %d cleanup join missing", i)
			}
			timer.Stop()
		}
	})
	return func(ch <-chan struct{}) { owners = append(owners, ch) }
}

func TestTotalFullActualReservationOwner(t *testing.T) {
	t.Run("last-slot-all-directions", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 3}, d, 8)
		a := totalTake(t, b, DirectionOutbound)
		c := totalTake(t, b, DirectionInbound)
		d.set(10*time.Second, nil)
		r := totalTake(t, b, DirectionAuxiliary)
		d.set(20*time.Second, nil)
		if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
			t.Fatal("original total refusal changed", err)
		}
		d.set(30*time.Second, nil)
		r.Release()
		d.set(40*time.Second, nil)
		s := totalComplete(t, b)
		if len(s.rows) != 2 || !s.rows[0].full || s.rows[0].count != 3 || s.rows[1].full || s.rows[1].count != 2 {
			t.Fatal("missing actual entry/exit", s)
		}
		totalBounds(t, s, 20*time.Second, 20*time.Second)
		if stats := b.Snapshot(); stats.Used != 2 || stats.RefusedTotal != 1 {
			t.Fatal("actual counters changed", stats)
		}
		a.Release()
		c.Release()
		if b.Snapshot().Used != 0 {
			t.Fatal("actual release lost")
		}
	})
	t.Run("refusal-is-not-full", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 3, OutboundReserve: 2}, d, 8)
		a := totalTake(t, b, DirectionInbound)
		if _, err := b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrOutboundReserved) {
			t.Fatal(err)
		}
		d.set(time.Second, nil)
		s := totalComplete(t, b)
		if len(s.rows) != 0 || s.cut.full || b.Snapshot().Used != 1 {
			t.Fatal("reserve refusal invented total fullness", s)
		}
		totalBounds(t, s, 0, 0)
		a.Release()
		other := totalObserved(t, Config{Total: 3, MaxOutbound: 1}, d, 8)
		r := totalTake(t, other, DirectionOutbound)
		if _, err := other.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal(err)
		}
		if cut := totalComplete(t, other); len(cut.rows) != 0 || cut.cut.full {
			t.Fatal("direction refusal invented fullness", cut)
		}
		x := totalTake(t, other, DirectionInbound)
		y := totalTake(t, other, DirectionAuxiliary)
		if _, err := other.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal("original precedence changed", err)
		}
		if stats := other.Snapshot(); stats.RefusedDirection != 2 || stats.RefusedTotal != 0 {
			t.Fatal("original counters changed", stats)
		}
		r.Release()
		x.Release()
		y.Release()
	})
	t.Run("double-release-no-observation", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		r := totalTake(t, b, DirectionOutbound)
		d.set(time.Second, nil)
		r.Release()
		calls := d.count()
		r.Release()
		r.Release()
		if d.count() != calls || b.Snapshot().Used != 0 {
			t.Fatal("CAS lost exactly-once observation")
		}
		if s := totalComplete(t, b); len(s.rows) != 2 {
			t.Fatal("double release added transition", s)
		}
	})
	t.Run("unknown-direction-no-mutation", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		calls := d.count()
		if _, err := b.Reserve(Direction(9)); !errors.Is(err, ErrUnknownDirection) {
			t.Fatal(err)
		}
		if d.count() != calls || b.Snapshot().RefusedUnknown != 1 || b.Snapshot().Used != 0 {
			t.Fatal("original pre-admission refusal changed")
		}
		if s := totalComplete(t, b); len(s.rows) != 0 {
			t.Fatal("invalid direction invented mutation", s)
		}
	})
}

func TestTotalFullCoverageAndDuration(t *testing.T) {
	t.Run("disjoint-without-poll", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		d.set(time.Second, nil)
		a := totalTake(t, b, DirectionOutbound)
		d.set(2*time.Second, nil)
		a.Release()
		d.set(4*time.Second, nil)
		c := totalTake(t, b, DirectionInbound)
		d.set(7*time.Second, nil)
		c.Release()
		d.set(10*time.Second, nil)
		s := totalComplete(t, b)
		if len(s.rows) != 4 {
			t.Fatal("missed between-read episode", s)
		}
		totalBounds(t, s, 4*time.Second, 4*time.Second)
	})
	t.Run("open-clipped-to-fresh-cut", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		d.set(2*time.Second, nil)
		r := totalTake(t, b, DirectionAuxiliary)
		d.set(9*time.Second, nil)
		s := totalComplete(t, b)
		if !s.cut.full {
			t.Fatal("open episode lost", s)
		}
		totalBounds(t, s, 7*time.Second, 7*time.Second)
		r.Release()
	})
	t.Run("uncertain-transition-brackets", func(t *testing.T) {
		d := &totalTestClock{}
		d.after = func(n int) {
			if n == 2 {
				d.set(4*time.Second, nil)
			}
			if n == 4 {
				d.set(9*time.Second, nil)
			}
		}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		d.set(time.Second, nil)
		r := totalTake(t, b, DirectionOutbound)
		d.set(6*time.Second, nil)
		r.Release()
		d.set(12*time.Second, nil)
		totalBounds(t, totalComplete(t, b), 2*time.Second, 8*time.Second)
	})
	t.Run("sealed-prefix-survives-capacity-gap", func(t *testing.T) {
		d := &totalTestClock{}
		b := totalObserved(t, Config{Total: 1}, d, 2)
		a := totalTake(t, b, DirectionOutbound)
		d.set(time.Second, nil)
		sealed := totalComplete(t, b)
		d.set(2*time.Second, nil)
		a.Release()
		d.set(3*time.Second, nil)
		r := totalTake(t, b, DirectionAuxiliary)
		s := b.fullSnapshot()
		if s.status != "UNKNOWN" || s.reason != "transition-capacity-gap" || !s.pastOnly || s.cut != sealed.cut || len(s.rows) != 1 || s.rows[0] != sealed.rows[0] {
			t.Fatal("lost or extended sealed prefix", s, sealed)
		}
		totalBounds(t, s, time.Second, time.Second)
		r.Release()
		if b.Snapshot().Used != 0 {
			t.Fatal("observation gap broke release")
		}
	})
}

func TestTotalFullCompletionOwnership(t *testing.T) {
	t.Run("reverse-post-completions", func(t *testing.T) {
		entryStarted, exitStarted := make(chan struct{}), make(chan struct{})
		entryBlock, exitBlock := make(chan struct{}), make(chan struct{})
		entryDone, exitDone := make(chan struct{}), make(chan struct{})
		releaseEntry := sync.OnceFunc(func() { close(entryBlock) })
		releaseExit := sync.OnceFunc(func() { close(exitBlock) })
		track := totalCleanup(t, releaseEntry, releaseExit)
		d := &totalTestClock{}
		d.after = func(n int) {
			if n == 4 {
				close(entryStarted)
				<-entryBlock
			}
			if n == 6 {
				close(exitStarted)
				<-exitBlock
			}
		}
		b := totalObserved(t, Config{Total: 2}, d, 8)
		a := totalTake(t, b, DirectionOutbound)
		d.set(time.Second, nil)
		track(entryDone)
		go func() {
			defer close(entryDone)
			r, err := b.Reserve(DirectionInbound)
			if err != nil {
				t.Error(err)
				return
			}
			r.Release()
		}()
		totalWait(t, entryStarted)
		d.set(10*time.Second, nil)
		track(exitDone)
		go func() { defer close(exitDone); a.Release() }()
		totalWait(t, exitStarted)
		d.set(20*time.Second, nil)
		c := totalTake(t, b, DirectionAuxiliary)
		d.set(30*time.Second, nil)
		c.Release()
		if s := b.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("pending owners certified current cut", s)
		}
		releaseExit()
		totalWait(t, exitDone)
		if s := b.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("completion stole entry", s)
		}
		releaseEntry()
		totalWait(t, entryDone)
		s := totalComplete(t, b)
		if len(s.rows) != 4 || b.Snapshot().Used != 0 {
			t.Fatal("real ownership lost", s)
		}
		totalBounds(t, s, 19*time.Second, 19*time.Second)
	})
	t.Run("blocked-pre-read-retained", func(t *testing.T) {
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		track := totalCleanup(t, release)
		d := &totalTestClock{}
		d.after = func(n int) {
			if n == 2 {
				close(started)
				<-block
			}
		}
		b := totalObserved(t, Config{Total: 2}, d, 8)
		track(done)
		go func() {
			defer close(done)
			r, err := b.Reserve(DirectionOutbound)
			if err != nil {
				t.Error(err)
				return
			}
			r.Release()
		}()
		totalWait(t, started)
		d.set(2*time.Second, nil)
		r := totalTake(t, b, DirectionInbound)
		if s := b.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("pending pre-read excluded", s)
		}
		release()
		totalWait(t, done)
		r.Release()
		_ = totalComplete(t, b)
	})
	t.Run("transition-during-cut", func(t *testing.T) {
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		track := totalCleanup(t, release)
		d := &totalTestClock{}
		d.after = func(n int) {
			if n == 2 {
				close(started)
				<-block
			}
		}
		b := totalObserved(t, Config{Total: 1}, d, 8)
		var cut totalFullSnapshot
		track(done)
		go func() { defer close(done); cut = b.fullSnapshot() }()
		totalWait(t, started)
		d.set(time.Second, nil)
		r := totalTake(t, b, DirectionOutbound)
		release()
		totalWait(t, done)
		if cut.status != "UNSTABLE_CUT" || !cut.pastOnly || len(cut.rows) != 0 {
			t.Fatal("torn cut certified", cut)
		}
		r.Release()
	})
}

func TestTotalFullClockAndLoss(t *testing.T) {
	for _, kind := range []string{"clock-error", "negative", "regression", "unsupported", "missing-epoch", "capacity-arithmetic", "read-overflow", "sequence-overflow", "owner-count-loss"} {
		t.Run(kind, func(t *testing.T) {
			d := &totalTestClock{}
			key := totalKey(1)
			capacity := 8
			clock := d.read
			if kind == "unsupported" {
				clock = nil
			}
			if kind == "missing-epoch" {
				key.epoch = [16]byte{}
			}
			if kind == "capacity-arithmetic" {
				capacity = int(^uint(0) >> 1)
			}
			b, err := newObservedBudget(Config{Total: 1}, key, capacity, clock)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "clock-error":
				d.set(0, errors.New("controlled unavailable"))
			case "negative":
				d.set(-1, nil)
			case "regression":
				d.set(time.Second, nil)
				_ = totalComplete(t, b)
				d.set(0, nil)
			// Failure-state injection only; no fabricated valid event/interval.
			case "read-overflow":
				b.mu.Lock()
				b.fullObservation.nextRead = totalMaxUint
				b.mu.Unlock()
			case "sequence-overflow":
				b.mu.Lock()
				b.fullObservation.sequence = totalMaxUint
				b.mu.Unlock()
			case "owner-count-loss":
				b.mu.Lock()
				b.outbound = 1
				b.mu.Unlock()
			}
			if kind != "owner-count-loss" {
				r := totalTake(t, b, DirectionOutbound)
				r.Release()
			}
			s := b.fullSnapshot()
			if s.status != "UNKNOWN" || !s.pastOnly {
				t.Fatal("untrusted source certified", kind, s)
			}
			if kind != "owner-count-loss" && b.Snapshot().Used != 0 {
				t.Fatal("observer fault changed admission/release")
			}
			if again := b.fullSnapshot(); again.status != "UNKNOWN" || again.reason != s.reason || again.cut != s.cut {
				t.Fatal("invalid source recovered silently", again, s)
			}
		})
	}
}

func TestTotalFullOrdinaryAndDisabled(t *testing.T) {
	b, err := New(Config{Total: 1})
	if err != nil {
		t.Fatal(err)
	}
	if b.fullObservation != nil || b.fullSnapshot().status != "INACTIVE" {
		t.Fatal("ordinary budget activated")
	}
	r := totalTake(t, b, DirectionOutbound)
	r.Release()
	var missing *Budget
	nilReservation, err := missing.Reserve(DirectionInbound)
	if err != nil {
		t.Fatal(err)
	}
	nilReservation.Release()
	if missing.fullSnapshot().status != "INACTIVE" {
		t.Fatal("nil budget source activated")
	}
	d := &totalTestClock{}
	disabled := totalObserved(t, Config{MaxOutbound: 1}, d, 8)
	r = totalTake(t, disabled, DirectionOutbound)
	if _, err := disabled.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
		t.Fatal("disabled direction limit changed", err)
	}
	r.Release()
	s := disabled.fullSnapshot()
	if s.status != "DISABLED" || d.count() != 0 {
		t.Fatal("disabled observed clock called", s, d.count())
	}
	if _, _, ok := totalFullDurationBounds(s); ok {
		t.Fatal("disabled gave zero duration proof")
	}
	if _, err := newObservedBudget(Config{Total: -1}, totalKey(1), 8, d.read); !errors.Is(err, ErrInvalidConfig) || d.count() != 0 {
		t.Fatal("invalid original configuration not authoritative", err)
	}
}

func TestTotalFullProviderOutsideOwnerLeaf(t *testing.T) {
	var b *Budget
	var held atomic.Bool
	d := &totalTestClock{}
	clock := func() (time.Duration, error) {
		if b != nil {
			if !b.mu.TryLock() {
				held.Store(true)
			} else {
				b.mu.Unlock()
			}
		}
		return d.read()
	}
	var err error
	b, err = newObservedBudget(Config{Total: 1}, totalKey(1), 8, clock)
	if err != nil {
		t.Fatal(err)
	}
	r := totalTake(t, b, DirectionOutbound)
	_ = totalComplete(t, b)
	r.Release()
	if held.Load() {
		t.Fatal("duration provider called under leaf")
	}
}
