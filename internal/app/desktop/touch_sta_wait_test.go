package desktop

import (
	"go/ast"
	"testing"
	"time"
)

// clockedWait drives tkPumpedWait with a fake clock. The wait is what moves
// time forward, because that is what it does on the machine: nothing else on
// this thread runs while it blocks.
type clockedWait struct {
	now time.Time
	// takes is how long each wait actually blocks for, in order. A wait
	// shorter than the timeout it was given is an incoming COM call arriving
	// mid-wait; the wait returning the whole timeout is the deadline.
	takes    []time.Duration
	broken   bool
	timeouts []time.Duration
	pumps    int
	// pumpAt records the number of waits that had happened before each pump,
	// which is the only way to see the ORDER the two run in.
	pumpAt []int
	slept  []time.Duration
}

func (c *clockedWait) clock() time.Time { return c.now }

func (c *clockedWait) pump() {
	c.pumps++
	c.pumpAt = append(c.pumpAt, len(c.timeouts))
}

func (c *clockedWait) wait(timeout time.Duration) bool {
	c.timeouts = append(c.timeouts, timeout)
	if c.broken {
		return false
	}
	took := timeout
	if len(c.takes) > 0 {
		took = c.takes[0]
		c.takes = c.takes[1:]
		if took > timeout {
			took = timeout
		}
	}
	c.now = c.now.Add(took)
	return true
}

func (c *clockedWait) sleep(d time.Duration) {
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
}

func (c *clockedWait) run(d time.Duration) {
	tkPumpedWait(d, c.clock, c.pump, c.wait, c.sleep)
}

func newClockedWait(takes ...time.Duration) *clockedWait {
	return &clockedWait{now: time.Unix(1000, 0), takes: takes}
}

// The whole point of the change: the queue is drained before the thread ever
// blocks. A wait entered first would sit on a Showing callback that arrived
// while the caller was setting the wait up — and the pane raises Showing
// BEFORE the panel is on screen, so that callback is exactly the answer the
// waiting code is about to spend a second and a half looking for.
func TestPumpedWaitDrainsTheQueueBeforeItEverBlocks(t *testing.T) {
	c := newClockedWait()
	c.run(150 * time.Millisecond)
	if len(c.pumpAt) == 0 || c.pumpAt[0] != 0 {
		t.Fatalf("the first pump happened after %v waits, want before any of them: a wait entered "+
			"with a callback already queued blocks on the answer it is waiting for", c.pumpAt)
	}
}

// Waking is not the end of the wait, it is the reason to pump: every return
// from the wait means input arrived, and input that is never dispatched is a
// COM call that is never delivered.
func TestPumpedWaitPumpsEveryTimeTheWaitWakes(t *testing.T) {
	c := newClockedWait(10*time.Millisecond, 20*time.Millisecond, 30*time.Millisecond)
	c.run(150 * time.Millisecond)
	if len(c.timeouts) != 4 {
		t.Fatalf("the wait ran %d times, want 4 (three early wakes and the one that reaches the deadline)", len(c.timeouts))
	}
	if c.pumps != len(c.timeouts)+1 {
		t.Fatalf("%d pumps for %d waits, want one before each wait and one after the last: "+
			"a wake that is not followed by a dispatch leaves the call sitting in the queue", c.pumps, len(c.timeouts))
	}
}

// Re-entering the wait must not restart the clock. Three callbacks during a
// 150ms settle would otherwise cost 600ms, and the legacy verify — eight polls
// — turns into an unbounded stall on a busy pane.
func TestPumpedWaitAsksOnlyForTheTimeThatIsLeft(t *testing.T) {
	c := newClockedWait(10*time.Millisecond, 20*time.Millisecond)
	start := c.now
	c.run(150 * time.Millisecond)

	want := []time.Duration{150 * time.Millisecond, 140 * time.Millisecond, 120 * time.Millisecond}
	if len(c.timeouts) != len(want) {
		t.Fatalf("waits: got %v, want %v", c.timeouts, want)
	}
	for i, w := range want {
		if c.timeouts[i] != w {
			t.Errorf("wait %d was given %v, want %v — each wait gets the time still owed, not the whole delay again", i, c.timeouts[i], w)
		}
	}
	if got := c.now.Sub(start); got != 150*time.Millisecond {
		t.Errorf("the wait took %v in total, want exactly 150ms", got)
	}
}

// A wait the OS refuses returns at once. Retrying it is not a wait, it is a
// spin: the legacy path would burn a core for 1.2s per show attempt. Sleeping
// the remainder is worse than pumping and no worse than the code being
// replaced, which is the right trade when the primitive is gone.
func TestPumpedWaitFallsBackToASleepWhenTheWaitIsBroken(t *testing.T) {
	c := newClockedWait()
	c.broken = true
	c.run(150 * time.Millisecond)

	if len(c.timeouts) != 1 {
		t.Fatalf("a broken wait was called %d times, want 1: a wait that fails instantly and is retried is a busy loop", len(c.timeouts))
	}
	if len(c.slept) != 1 || c.slept[0] != 150*time.Millisecond {
		t.Fatalf("slept %v, want one 150ms sleep covering the whole remaining delay", c.slept)
	}
}

// The fallback still owes the caller a drain — the queue is not serviced by
// anything else on this thread, and the sleep it just took is precisely when
// things piled up in it.
func TestPumpedWaitPumpsOnceMoreAfterTheFallbackSleep(t *testing.T) {
	c := newClockedWait()
	c.broken = true
	c.run(150 * time.Millisecond)
	if c.pumps != 2 {
		t.Fatalf("%d pumps around the fallback sleep, want 2 (one before the wait, one after the sleep)", c.pumps)
	}
}

// Partial progress before the break must not be paid for twice: the sleep
// covers what is left, not what was asked for.
func TestPumpedWaitSleepsOnlyTheRemainderWhenTheWaitBreaksLate(t *testing.T) {
	c := &clockedWait{now: time.Unix(1000, 0)}
	rounds := 0
	tkPumpedWait(150*time.Millisecond, c.clock, c.pump, func(timeout time.Duration) bool {
		rounds++
		if rounds == 1 {
			c.now = c.now.Add(40 * time.Millisecond)
			return true
		}
		return false
	}, c.sleep)

	if len(c.slept) != 1 || c.slept[0] != 110*time.Millisecond {
		t.Fatalf("slept %v after 40ms had already been waited out, want a single 110ms sleep", c.slept)
	}
}

// A delay that is already spent is still a chance to dispatch, and costs
// nothing. It must not become a wait with a zero or negative timeout, which
// polls rather than waits.
func TestPumpedWaitStillDrainsAnEmptyDelay(t *testing.T) {
	for _, d := range []time.Duration{0, -1 * time.Second} {
		c := newClockedWait()
		c.run(d)
		if c.pumps != 1 {
			t.Errorf("d=%v: %d pumps, want exactly 1", d, c.pumps)
		}
		if len(c.timeouts) != 0 {
			t.Errorf("d=%v: the wait was entered with %v, want not at all", d, c.timeouts)
		}
		if len(c.slept) != 0 {
			t.Errorf("d=%v: slept %v, want nothing", d, c.slept)
		}
	}
}

// A wait that wakes without time passing — a message the pump cannot remove,
// for instance — must still terminate at the deadline rather than looping on
// a queue that never empties.
func TestPumpedWaitStopsAtTheDeadlineEvenWhenTheWakesNeverStop(t *testing.T) {
	c := &clockedWait{now: time.Unix(1000, 0)}
	rounds := 0
	tkPumpedWait(150*time.Millisecond, c.clock, c.pump, func(timeout time.Duration) bool {
		rounds++
		if rounds > 100 {
			t.Fatal("tkPumpedWait never reached its deadline")
		}
		// Half the remaining time each round: time advances, the deadline is
		// approached, and the loop must not need the wait to reach it exactly.
		c.now = c.now.Add(timeout / 2)
		if timeout/2 == 0 {
			c.now = c.now.Add(timeout)
		}
		return true
	}, c.sleep)
	if rounds < 2 {
		t.Fatalf("the loop gave up after %d wait(s); it is supposed to keep waiting out the remainder", rounds)
	}
}

// The service thread is a single-threaded apartment (see
// TestInputPaneIsActivatedFromASingleThreadedApartment) and everything it
// blocks on has to keep dispatching. time.Sleep does not, so it may not appear
// anywhere in tkKeyboardService — not as a call, and not handed to something
// that will call it, which is how the legacy verify used it: passed as
// tkAwaitVisible's sleep argument, where a check for CallExpr sees nothing.
//
// This is a source-level guard for the same reason the two beside it are: the
// file is //go:build windows and this suite never compiles it.
func TestTheServiceThreadNeverSleepsInsteadOfPumping(t *testing.T) {
	f := tkWindowsAST(t)

	sleeps := 0
	ast.Inspect(f, func(n ast.Node) bool {
		d, ok := n.(*ast.FuncDecl)
		if !ok || d.Name.Name != "tkKeyboardService" {
			return true
		}
		ast.Inspect(d.Body, func(m ast.Node) bool {
			se, ok := m.(*ast.SelectorExpr)
			if !ok || se.Sel.Name != "Sleep" {
				return true
			}
			if id, ok := se.X.(*ast.Ident); ok && id.Name == "time" {
				sleeps++
			}
			return true
		})
		return false
	})
	if sleeps > 0 {
		t.Errorf("tkKeyboardService reaches time.Sleep in %d place(s): a single-threaded apartment "+
			"that sleeps delivers no COM calls, so the pane's Showing callback — raised before the panel "+
			"is even on screen — cannot arrive during the wait that is looking for it", sleeps)
	}

	// And the replacement has to be the pumping one. A tkStaWait that quietly
	// went back to sleeping would satisfy the check above and change nothing.
	var staWait *ast.FuncDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if d, ok := n.(*ast.FuncDecl); ok && d.Name.Name == "tkStaWait" {
			staWait = d
		}
		return true
	})
	if staWait == nil {
		t.Fatal("no tkStaWait in touch_keyboard_windows.go — the service thread's waits have nothing pumping behind them")
	}

	pumped := false
	ast.Inspect(staWait.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "tkPumpedWait" {
			return true
		}
		// tkStaPump has to be the pump it is given; a wait loop calling
		// something else is not draining this thread's queue.
		for _, a := range call.Args {
			if ai, ok := a.(*ast.Ident); ok && ai.Name == "tkStaPump" {
				pumped = true
			}
		}
		return true
	})
	if !pumped {
		t.Error("tkStaWait does not drive tkPumpedWait with tkStaPump; whatever it waits with is not dispatching this apartment's messages")
	}

	// The check above passes for a tkStaWait that sleeps first and reaches
	// tkPumpedWait afterwards, or never — which is the defect back under a
	// name that says otherwise. tkStaWait is allowed exactly one kind of
	// sleep: the fallback for a machine where the Win32 calls it needs are
	// missing, and that one sits behind a Find() check. Handing time.Sleep to
	// tkPumpedWait is not a call and is not counted; it is the same fallback,
	// for the broken-wait case, and the loop is what decides whether to use it.
	timeSleepCall := func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return false
		}
		se, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || se.Sel.Name != "Sleep" {
			return false
		}
		id, ok := se.X.(*ast.Ident)
		return ok && id.Name == "time"
	}
	mentionsFind := func(x ast.Expr) bool {
		found := false
		ast.Inspect(x, func(n ast.Node) bool {
			if se, ok := n.(*ast.SelectorExpr); ok && se.Sel.Name == "Find" {
				found = true
			}
			return true
		})
		return found
	}

	excused := map[ast.Node]bool{}
	ast.Inspect(staWait.Body, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || !mentionsFind(is.Cond) {
			return true
		}
		ast.Inspect(is.Body, func(m ast.Node) bool {
			if timeSleepCall(m) {
				excused[m] = true
			}
			return true
		})
		return true
	})

	unguarded := 0
	ast.Inspect(staWait.Body, func(n ast.Node) bool {
		if timeSleepCall(n) && !excused[n] {
			unguarded++
		}
		return true
	})
	if unguarded > 0 {
		t.Errorf("tkStaWait calls time.Sleep in %d place(s) that no missing-primitive check guards: "+
			"a wait that sleeps on a machine where it could have pumped is the original defect wearing "+
			"the name of the fix", unguarded)
	}
}
