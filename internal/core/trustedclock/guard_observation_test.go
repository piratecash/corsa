package trustedclock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestS6GuardPublicConstruction(t *testing.T) {
	t.Run("nil-and-default", func(t *testing.T) {
		var absent *Clock
		if v := absent.GuardDurationSnapshot(); v.Status != "INACTIVE" || v.Guarded || v.Instance != ([16]byte{}) {
			t.Fatal("nil clock certified coverage", v)
		}
		d := newGuardTestDevice()
		c := New(d.sources())
		if d.bootCalls != 1 || d.wallCalls != 1 || c.observation != nil || c.GuardDurationSnapshot().Status != "INACTIVE" {
			t.Fatal("ordinary constructor opted in or changed calls")
		}
	})
	t.Run("value-copy", func(t *testing.T) {
		d := newGuardTestDevice()
		c := NewWithGuardObservation(d.sources(), guardTestInstance(91))
		d.at(time.Second, true)
		_ = c.Now()
		got, want := c.GuardDurationSnapshot(), c.guardDuration()
		if got != want || got.Status != "COMPLETE" || !got.Active || got.Sequence != 1 {
			t.Fatal("public snapshot changed original fields", got, want)
		}
		got.Instance[0] = 0
		if c.GuardDurationSnapshot().Instance != guardTestInstance(91) {
			t.Fatal("public value aliases original instance")
		}
	})
	t.Run("first-read-fallback", func(t *testing.T) {
		calls, walls := 0, 0
		raw := time.Unix(1780000000, 0)
		c := NewWithGuardObservation(Sources{Wall: func() time.Time { walls++; return raw }, SinceBoot: func() (time.Duration, error) { calls++; return 0, errors.New("unavailable") }}, guardTestInstance(92))
		if calls != 1 || walls != 0 || c.start != nil {
			t.Fatal("initial failure rebuilt or anchored clock", calls, walls)
		}
		if got := c.Now(); !got.Equal(raw) || calls != 1 || walls != 1 || c.Stats().Guarded {
			t.Fatal("original unguarded fallback changed", got, c.Stats())
		}
		if v := c.GuardDurationSnapshot(); v.Status != "UNKNOWN" || v.Reason == "" || v.Guarded {
			t.Fatal("first read loss certified coverage", v)
		}
	})
	for _, mode := range []string{"second-read-error", "second-read-regression"} {
		t.Run(mode, func(t *testing.T) {
			calls, walls := 0, 0
			initial := time.Unix(1780000000, 0)
			raw := initial
			c := NewWithGuardObservation(Sources{Wall: func() time.Time { walls++; return raw }, SinceBoot: func() (time.Duration, error) {
				calls++
				if calls == 1 {
					return time.Second, nil
				}
				if calls == 2 {
					if mode == "second-read-error" {
						return 0, errors.New("cut lost")
					}
					return 0, nil
				}
				return 2 * time.Second, nil
			}}, guardTestInstance(93))
			if calls != 2 || walls != 1 || c.start == nil || c.start.sinceBoot != time.Second || !c.start.wall.Equal(initial) {
				t.Fatal("observed constructor rebuilt original anchor", calls, walls)
			}
			raw = initial.Add(2*JumpTolerance + time.Second)
			if now := c.Now(); !now.Equal(initial.Add(time.Second)) {
				t.Fatal("observation failure changed secure time", now)
			}
			stats := c.Stats()
			if !stats.Guarded || !stats.JumpActive || stats.JumpsDetected != 1 {
				t.Fatal("initial cut failure cleared original guard", stats)
			}
			if v := c.GuardDurationSnapshot(); v.Status != "UNKNOWN" || v.Reason == "" || v.Guarded || v.Sequence != 0 || v.ClosedCount != 0 {
				t.Fatal("UNKNOWN placeholder filled from security Stats", v)
			}
		})
	}
}

func TestS6GuardPublicUnavailableCopies(t *testing.T) {
	t.Run("in-flight", func(t *testing.T) {
		d := newGuardTestDevice()
		c := NewWithGuardObservation(d.sources(), guardTestInstance(94))
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		own := guardOwnedCleanup(t, unblock)
		d.onWall = func() { close(entered); <-release }
		own(done)
		go func() { defer close(done); _ = c.Now() }()
		guardWait(t, entered)
		got, want := c.GuardDurationSnapshot(), c.guardDuration()
		if got != want || got.Status != "IN_FLIGHT" || got.Guarded || got.Sequence != 0 || !c.Stats().Guarded {
			t.Fatal("inflight placeholder promoted to coverage", got, want, c.Stats())
		}
		unblock()
		guardWait(t, done)
	})
	t.Run("sticky-read-loss", func(t *testing.T) {
		d := newGuardTestDevice()
		c := NewWithGuardObservation(d.sources(), guardTestInstance(95))
		d.at(time.Second, true)
		_ = c.Now()
		d.fail(errors.New("cut unavailable"))
		got, want := c.GuardDurationSnapshot(), c.guardDuration()
		if got != want || got.Status != "UNKNOWN" || got.Sequence != 0 || got.Guarded || !c.Stats().JumpActive {
			t.Fatal("unknown readout filled from partial counts or Stats", got, want)
		}
		d.fail(nil)
		if c.GuardDurationSnapshot() != got {
			t.Fatal("later successful cut erased original loss")
		}
	})
}
