package node

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type k5TestClock struct {
	mu    sync.Mutex
	at    time.Duration
	err   error
	calls int
	after func(int)
}

func (d *k5TestClock) read() (time.Duration, error) {
	d.mu.Lock()
	d.calls++
	n, at, err := d.calls, d.at, d.err
	d.mu.Unlock()
	if d.after != nil {
		d.after(n)
	}
	return at, err
}
func (d *k5TestClock) set(at time.Duration, err error) {
	d.mu.Lock()
	d.at, d.err = at, err
	d.mu.Unlock()
}
func k5TestKey(epoch byte) k5SourceKey {
	return k5SourceKey{anchor: "test-anchor", epoch: [16]byte{epoch}, clockDomain: [16]byte{2}, configuration: [16]byte{3}, kind: k5HandshakeNode}
}
func k5Complete(t *testing.T, h *inboundHandshakes) handshakeFullSnapshot {
	t.Helper()
	s := h.fullSnapshot()
	if s.status != "COMPLETE" {
		t.Fatalf("handshake full cut unavailable: %+v", s)
	}
	return s
}
func k5Admit(t *testing.T, h *inboundHandshakes, s handshakeSource) func() {
	t.Helper()
	release, err := h.enter(s)
	if err != nil {
		t.Fatal("unexpected original admission refusal", err)
	}
	return release
}
func k5RefusedBy(t *testing.T, err error) listenerRefusalReason {
	t.Helper()
	var refusal *listenerRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("got %v, want a listener refusal", err)
	}
	return refusal.reason
}
func k5Wait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("K5 controlled caller missed boundary")
	}
}
func k5OwnedCleanup(t *testing.T, releases ...func()) func(<-chan struct{}) {
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
				t.Errorf("K5 owned worker %d failed cleanup join", i)
				continue
			}
			timer := time.NewTimer(left)
			select {
			case <-done:
			case <-timer.C:
				t.Errorf("K5 owned worker %d failed cleanup join", i)
			}
			timer.Stop()
		}
	})
	return func(done <-chan struct{}) { owners = append(owners, done) }
}

func TestK5HandshakeNodeActualOwner(t *testing.T) {
	t.Run("last-slot-not-first-refusal", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 2, loopback: 1, perSource: 1}, k5TestKey(1), 8, d.read)
		a := k5Admit(t, h, handshakeSource{})
		if len(k5Complete(t, h).rows) != 0 {
			t.Fatal("partial node occupancy became full")
		}
		d.set(time.Second, nil)
		b := k5Admit(t, h, handshakeSource{})
		s := k5Complete(t, h)
		if !s.cut.full || len(s.rows) != 1 || s.rows[0].count != 2 || s.rows[0].lower != time.Second {
			t.Fatal("last admitted slot did not start node fullness", s)
		}
		if release, err := h.enter(handshakeSource{loopback: true}); release != nil || k5RefusedBy(t, err) != refusalHandshakesNode {
			t.Fatal("node-before-loopback refusal changed", err)
		}
		d.set(11*time.Second, nil)
		a()
		a()
		b()
		b()
		s = k5Complete(t, h)
		ranges, ok := handshakeFullRanges(s)
		if !ok || len(ranges) != 1 || ranges[0] != (handshakeFullRange{time.Second, 11 * time.Second, time.Second, 11 * time.Second}) || len(s.rows) != 2 || h.inFlight != 0 || h.loopbackRunning() != 0 {
			t.Fatal("first release/once ownership or interval changed", s, ranges, h.inFlight)
		}
	})
	t.Run("loopback-and-source-not-node", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 4, loopback: 1, perSource: 1}, k5TestKey(2), 8, d.read)
		a := k5Admit(t, h, handshakeSource{loopback: true})
		if release, err := h.enter(handshakeSource{loopback: true}); release != nil || k5RefusedBy(t, err) != refusalHandshakesLoopback {
			t.Fatal("loopback refusal identity changed", err)
		}
		source := handshakeSource{group: "external", perSource: true}
		r, err := h.admitSource(source)
		if err != nil {
			t.Fatal(err)
		}
		if release, err := h.admitSource(source); release != nil || k5RefusedBy(t, err) != refusalHandshakesPerIP {
			t.Fatal("source refusal identity changed", err)
		}
		r()
		r()
		a()
		if s := k5Complete(t, h); len(s.rows) != 0 || s.cut.full {
			t.Fatal("other ceiling mislabeled node-full", s)
		}
	})
	t.Run("nil-root-original-behavior", func(t *testing.T) {
		h := newInboundHandshakes(handshakeBounds{node: 1, loopback: 1, perSource: 1})
		r := k5Admit(t, h, handshakeSource{loopback: true})
		if h.fullObservation != nil || h.fullSnapshot().status != "INACTIVE" || h.loopbackRunning() != 1 {
			t.Fatal("ordinary root activated observation")
		}
		r()
		r()
		if h.inFlight != 0 || h.loopbackRunning() != 0 {
			t.Fatal("ordinary once release changed")
		}
	})
	t.Run("clock-error-never-skips-original-release", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(3), 8, d.read)
		r := k5Admit(t, h, handshakeSource{})
		d.set(time.Second, errors.New("source failure"))
		r()
		r()
		if h.inFlight != 0 || h.fullSnapshot().status != "UNKNOWN" {
			t.Fatal("observation error changed original release or fabricated coverage", h.inFlight)
		}
		d.set(2*time.Second, nil)
		r = k5Admit(t, h, handshakeSource{})
		r()
		if h.inFlight != 0 || h.fullSnapshot().status != "UNKNOWN" {
			t.Fatal("later success bridged gap or altered gate")
		}
	})
}

func TestK5HandshakeNodeCompletionOwnership(t *testing.T) {
	t.Run("concurrent-real-owners", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 4, loopback: 2}, k5TestKey(20), 512, d.read)
		stop := make(chan struct{})
		stopOnce := sync.OnceFunc(func() { close(stop) })
		track := k5OwnedCleanup(t, stopOnce)
		var owners []<-chan struct{}
		for i := 0; i < 8; i++ {
			done := make(chan struct{})
			owners = append(owners, done)
			track(done)
			go func() {
				defer close(done)
				for j := 0; j < 32; j++ {
					select {
					case <-stop:
						return
					default:
					}
					release, err := h.enter(handshakeSource{})
					if err != nil {
						var refusal *listenerRefusalError
						if !errors.As(err, &refusal) || refusal.reason != refusalHandshakesNode {
							t.Error("concurrent original refusal changed", err)
							return
						}
						continue
					}
					release()
					release()
				}
			}()
		}
		for _, done := range owners {
			k5Wait(t, done)
		}
		s := k5Complete(t, h)
		if h.inFlight != 0 || s.cut.full {
			t.Fatal("concurrent original once owners lost", s, h.inFlight)
		}
	})
	t.Run("reverse-across-newer-episode", func(t *testing.T) {
		entryStarted, exitStarted := make(chan struct{}), make(chan struct{})
		entryBlock, exitBlock := make(chan struct{}), make(chan struct{})
		entryDone, exitDone := make(chan struct{}), make(chan struct{})
		releaseEntry := sync.OnceFunc(func() { close(entryBlock) })
		releaseExit := sync.OnceFunc(func() { close(exitBlock) })
		track := k5OwnedCleanup(t, releaseEntry, releaseExit)
		d := &k5TestClock{}
		d.after = func(n int) {
			switch n {
			case 4:
				close(entryStarted)
				<-entryBlock
			case 6:
				close(exitStarted)
				<-exitBlock
			}
		}
		h := newObservedInboundHandshakes(handshakeBounds{node: 2}, k5TestKey(4), 8, d.read)
		a := k5Admit(t, h, handshakeSource{})
		d.set(time.Second, nil)
		track(entryDone)
		go func() {
			defer close(entryDone)
			r, err := h.enter(handshakeSource{})
			if err != nil {
				t.Error(err)
				return
			}
			r()
		}()
		k5Wait(t, entryStarted)
		d.set(10*time.Second, nil)
		track(exitDone)
		go func() { defer close(exitDone); a() }()
		k5Wait(t, exitStarted)
		d.set(20*time.Second, nil)
		c := k5Admit(t, h, handshakeSource{})
		d.set(30*time.Second, nil)
		c()
		if s := h.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("unresolved closed pair certified prefix", s)
		}
		releaseExit()
		k5Wait(t, exitDone)
		if s := h.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("exit stole delayed entry slot", s)
		}
		releaseEntry()
		k5Wait(t, entryDone)
		s := k5Complete(t, h)
		ranges, ok := handshakeFullRanges(s)
		if !ok || len(s.rows) != 4 || len(ranges) != 2 || ranges[0].definiteStart != time.Second || ranges[0].definiteEnd != 10*time.Second || ranges[1].definiteStart != 20*time.Second || ranges[1].definiteEnd != 30*time.Second || h.inFlight != 0 {
			t.Fatal("caller sequence slot overwritten or gate ownership lost", s, ranges, h.inFlight)
		}
	})
	t.Run("past-prefix-survives-capacity-gap", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(5), 2, d.read)
		a := k5Admit(t, h, handshakeSource{})
		d.set(time.Second, nil)
		sealed := k5Complete(t, h)
		d.set(2*time.Second, nil)
		a()
		d.set(3*time.Second, nil)
		b := k5Admit(t, h, handshakeSource{})
		s := h.fullSnapshot()
		if s.status != "UNKNOWN" || s.reason != "transition-capacity-gap" || !s.pastOnly || s.cut != sealed.cut || len(s.rows) != 1 || s.rows[0] != sealed.rows[0] {
			t.Fatal("overflow erased or extended sealed prefix", s, sealed)
		}
		ranges, ok := handshakeFullRanges(s)
		if !ok || len(ranges) != 1 || ranges[0].definiteEnd != time.Second {
			t.Fatal("past open interval extended through gap", ranges)
		}
		b()
		if h.inFlight != 0 {
			t.Fatal("capacity gap prevented original release")
		}
	})
}

func TestK5HandshakeNodeReadCoverage(t *testing.T) {
	t.Run("late-overlapping-pre-read", func(t *testing.T) {
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		track := k5OwnedCleanup(t, release)
		d := &k5TestClock{}
		d.after = func(n int) {
			if n == 2 {
				close(started)
				<-block
			}
		}
		h := newObservedInboundHandshakes(handshakeBounds{node: 2}, k5TestKey(6), 8, d.read)
		d.set(time.Second, nil)
		track(done)
		go func() {
			defer close(done)
			r, err := h.enter(handshakeSource{})
			if err != nil {
				t.Error(err)
				return
			}
			r()
		}()
		k5Wait(t, started)
		d.set(2*time.Second, nil)
		r := k5Admit(t, h, handshakeSource{})
		if s := h.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("old provider token vanished", s)
		}
		release()
		k5Wait(t, done)
		r()
		_ = k5Complete(t, h)
	})
	t.Run("returned-error-before-validation", func(t *testing.T) {
		d := &k5TestClock{}
		h := newObservedInboundHandshakes(handshakeBounds{node: 2}, k5TestKey(7), 8, d.read)
		r := h.beginFullRead()
		d.set(0, errors.New("returned error"))
		at, err := h.fullClock()
		d.set(time.Second, nil)
		if s := h.fullSnapshot(); s.status != "IN_FLIGHT" {
			t.Fatal("returned unvalidated error excluded from cut", s)
		}
		h.mu.Lock()
		h.validateFullReadLocked(&r, at, err)
		h.retireFullReadLocked(&r)
		h.mu.Unlock()
		if s := h.fullSnapshot(); s.status != "UNKNOWN" || s.reason != "clock-error" {
			t.Fatal("validated error not sticky", s)
		}
	})
	t.Run("fresh-cut-and-nontransition-read", func(t *testing.T) {
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		track := k5OwnedCleanup(t, release)
		d := &k5TestClock{}
		d.after = func(n int) {
			if n == 2 {
				close(started)
				<-block
			}
		}
		h := newObservedInboundHandshakes(handshakeBounds{node: 2}, k5TestKey(8), 8, d.read)
		var cut handshakeFullSnapshot
		track(done)
		go func() { defer close(done); cut = h.fullSnapshot() }()
		k5Wait(t, started)
		d.set(time.Second, nil)
		r := k5Admit(t, h, handshakeSource{})
		release()
		k5Wait(t, done)
		if cut.status != "COMPLETE" || cut.cut.full || len(cut.rows) != 0 {
			t.Fatal("validated nontransition incorrectly vetoed cut", cut)
		}
		r()
	})
	t.Run("transition-during-cut", func(t *testing.T) {
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		track := k5OwnedCleanup(t, release)
		d := &k5TestClock{}
		d.after = func(n int) {
			if n == 2 {
				close(started)
				<-block
			}
		}
		h := newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(9), 8, d.read)
		var cut handshakeFullSnapshot
		track(done)
		go func() { defer close(done); cut = h.fullSnapshot() }()
		k5Wait(t, started)
		d.set(time.Second, nil)
		r := k5Admit(t, h, handshakeSource{})
		release()
		k5Wait(t, done)
		if cut.status != "UNSTABLE_CUT" || !cut.pastOnly || len(cut.rows) != 0 {
			t.Fatal("transition certified torn cut", cut)
		}
		r()
		_ = k5Complete(t, h)
	})
	t.Run("provider-never-under-owner-leaf", func(t *testing.T) {
		d := &k5TestClock{}
		var h *inboundHandshakes
		var held atomic.Bool
		clock := func() (time.Duration, error) {
			if h != nil {
				if !h.mu.TryLock() {
					held.Store(true)
				} else {
					h.mu.Unlock()
				}
			}
			return d.read()
		}
		h = newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(10), 8, clock)
		r := k5Admit(t, h, handshakeSource{})
		_ = k5Complete(t, h)
		r()
		if held.Load() {
			t.Fatal("provider called with owner leaf held")
		}
	})
}

func TestK5HandshakeNodeInvalidEpoch(t *testing.T) {
	for _, kind := range []string{"negative", "regression", "missing-epoch", "unsupported", "capacity-arithmetic", "read-overflow", "transition-overflow", "pending-overflow", "post-error", "cut-error", "duplicate-completion", "untracked-count"} {
		t.Run(kind, func(t *testing.T) {
			d := &k5TestClock{}
			key := k5TestKey(11)
			capacity := 8
			clock := d.read
			if kind == "missing-epoch" {
				key.epoch = [16]byte{}
			}
			if kind == "unsupported" {
				clock = nil
			}
			if kind == "capacity-arithmetic" {
				capacity = int(^uint(0) >> 1)
			}
			if kind == "post-error" {
				d.after = func(n int) {
					if n == 2 {
						d.set(0, errors.New("post-read failure"))
					}
				}
			}
			h := newObservedInboundHandshakes(handshakeBounds{node: 1}, key, capacity, clock)
			if kind == "negative" {
				d.set(-1, nil)
			}
			if kind == "regression" {
				d.set(time.Second, nil)
				_ = k5Complete(t, h)
				d.set(0, nil)
			}
			if kind == "read-overflow" {
				h.mu.Lock()
				h.fullObservation.nextRead = k5MaxUint
				h.mu.Unlock()
			}
			if kind == "transition-overflow" {
				h.mu.Lock()
				h.fullObservation.sequence = k5MaxUint
				h.mu.Unlock()
			}
			if kind == "pending-overflow" {
				h.mu.Lock()
				h.fullObservation.pending = k5MaxUint
				h.mu.Unlock()
			}
			if kind == "cut-error" {
				d.set(0, errors.New("cut failure"))
				_ = h.fullSnapshot()
			}
			if kind == "untracked-count" {
				h.mu.Lock()
				h.inFlight = 1
				h.mu.Unlock()
			}
			switch kind {
			case "duplicate-completion":
				h.mu.Lock()
				h.inFlight++
				p := h.fullMutationLocked(0, true)
				h.mu.Unlock()
				h.finishFullTransition(&p)
				h.mu.Lock()
				h.completeFullRowLocked(&p, 0, true)
				h.mu.Unlock()
				h.leave(false)
			case "untracked-count":
				h.leave(false)
			default:
				r, err := h.enter(handshakeSource{})
				if err != nil {
					t.Fatal("observation invalidity changed real gate", err)
				}
				r()
			}
			s := h.fullSnapshot()
			if s.status != "UNKNOWN" || h.inFlight != 0 {
				t.Fatal("invalid coverage recovered or original release failed", kind, s, h.inFlight)
			}
			// A fresh epoch does not inherit the failed source prefix or duration.
			d.set(2*time.Second, nil)
			fresh := newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(12), 8, d.read)
			if x := k5Complete(t, fresh); x.key.epoch == s.key.epoch || len(x.rows) != 0 {
				t.Fatal("restart stitched epoch", x, s)
			}
		})
	}
}
