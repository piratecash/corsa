package desktop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
	"time"

	"gioui.org/io/event"
)

// The trigger a closed menu hands focus back to can be gone. "⋯" buttons are
// cached per peer and per message ID, so the widget survives — but a widget the
// frame does not lay out is not a focus target, and Gio undoes the FocusCmd in
// the same Router.Frame that accepts it. The user is then left with focus on
// nothing: no origin for Tab, nothing for Narrator to announce, no key that
// gets it back.
//
// A message deleted under its own open menu is the case that brought this in:
// dropStaleMsgMenu closes the menu, and by then the bubble carrying the trigger
// has already left the frame.
func TestMenuFocusFallsBackWhenTheTriggerIsNoLongerDrawn(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	// A trigger the harness never lays out: the "⋯" of a deleted message.
	h.state.open(new(int))
	h.frame()
	if h.focused() != 0 {
		t.Fatal("harness bug: the menu was supposed to hold focus before it closed")
	}

	h.closingFrame()
	h.closedFrame()
	if h.router.Source().Focused(h.fallback) {
		t.Fatal("the fallback must wait for the frame that reports whether the trigger took focus; " +
			"jumping straight to it would override every trigger that is perfectly fine")
	}

	h.closedFrame()
	if !h.router.Source().Focused(h.fallback) {
		t.Fatal("a trigger that is no longer laid out cannot take focus, and Gio drops it again the same " +
			"frame — the close must land focus on the fallback instead of leaving the keyboard user with nothing")
	}
}

// The check must be exactly that: a check. A trigger that is still on screen
// keeps the focus it was handed, and the fallback never runs.
func TestMenuFocusKeepsATriggerThatTookFocus(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	h.closingFrame()

	for i := 0; i < 3; i++ {
		h.closedFrame()
		if !h.router.Source().Focused(trigger) {
			t.Fatalf("frame %d: a trigger that is still drawn must keep the focus handed back to it", i)
		}
	}
}

// Somebody else claiming focus before the check frame is not a missing trigger.
// The restore already yields to a handler that speaks up first; the check has
// to yield on the same terms or it would undo that a frame later.
func TestMenuFocusDoesNotFallBackWhenAnotherHandlerTookFocus(t *testing.T) {
	h := newMenuHarness(3, true)
	claimed := new(int)
	h.extra = []event.Tag{claimed}
	h.fallback = new(int)
	h.state.open(new(int)) // a trigger that is no longer drawn
	h.frame()
	h.closingFrame()
	h.closedFrame() // hands focus to the trigger, which cannot take it

	// A handler claims focus in the gap — Reply focusing the composer, a
	// confirmation focusing its own field.
	h.frameStealingFocus(nil, claimed)

	h.closedFrame()
	if !h.router.Source().Focused(claimed) {
		t.Fatal("the fallback must stand down for a handler that has already claimed focus")
	}
}

// A menu with no trigger at all — nothing recorded the widget that opened it —
// used to leave focus wherever the close dropped it, which is nowhere. There is
// nothing to check in that case, so the fallback applies at once.
func TestMenuFocusFallsBackWhenThereIsNoTriggerAtAll(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	h.state.open(nil)
	h.frame()
	h.closingFrame()

	h.closedFrame()
	if !h.router.Source().Focused(h.fallback) {
		t.Fatal("with no trigger to hand focus to, the close must still leave focus somewhere reachable")
	}
}

// Focus emptied ON PURPOSE is not a missing trigger. dismissOnOutsideTap clears
// editor focus so the touch keyboard's blur-driven hide can fire; reading that
// as "the trigger was not there" and moving focus into the composer would
// cancel the hide in trackEditorFocus and leave the keyboard up.
func TestMenuFocusAbandonsTheCheckAfterADeliberateClear(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	h.state.open(new(int)) // a trigger that is no longer drawn
	h.frame()
	h.closingFrame()
	h.closedFrame() // hands focus to the trigger, which cannot take it

	h.state.abandonRestore() // the outside tap clears focus

	h.closedFrame()
	if h.router.Source().Focused(h.fallback) {
		t.Fatal("a focus cleared on purpose must not be read as a trigger that is missing")
	}
}

// The tap that closes a menu is often the tap that dismisses the touch keyboard,
// and the deliberate clear runs ABOVE the handler that closes the menu — so the
// frame that abandons the restore is a frame the menu is still OPEN on, with the
// hand-back itself still armed. Cancelling only the check there changes nothing:
// the next frame finds the menu shut and empty focus, hands focus to the trigger,
// and lands it in the composer when the trigger has left the frame — which is
// exactly what cancels the blur-driven hide and leaves the keyboard up.
func TestMenuFocusAbandonsTheRestoreWhenTheClosingTapClearedFocus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger func(h *menuHarness) event.Tag
	}{
		{"the trigger is still drawn", func(h *menuHarness) event.Tag { return h.background }},
		{"the trigger has left the frame", func(h *menuHarness) event.Tag { return new(int) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMenuHarness(3, true)
			h.fallback = new(int)
			trigger := tc.trigger(h)
			h.state.open(trigger)
			h.frame()
			if h.focused() != 0 {
				t.Fatal("harness bug: the menu was supposed to hold focus before the tap that closes it")
			}

			h.closingFrameAfterAnOutsideTap()

			for i := 0; i < 3; i++ {
				h.closedFrame()
				src := h.router.Source()
				if src.Focused(trigger) {
					t.Fatalf("frame %d: focus went back to the trigger after a tap that emptied it on "+
						"purpose — the menu was still open when that clear ran, so dropping the check "+
						"alone leaves the hand-back itself to fire the moment the menu shuts", i)
				}
				if src.Focused(h.fallback) {
					t.Fatalf("frame %d: focus landed in the composer after a tap that emptied it on "+
						"purpose — trackEditorFocus reads a focused editor as a reason to cancel the "+
						"blur-driven hide, and the touch keyboard the tap asked to dismiss stays up", i)
				}
				if !src.Focused(nil) {
					t.Fatalf("frame %d: a focus emptied on purpose has to survive the close, and "+
						"something in this window took it instead", i)
				}
			}
		})
	}
}

// Cancelling a restore is not cancelling the menu. The tap that OPENS a menu is
// itself a tap outside every editor, so the same clear can arrive on the frame
// the menu is opening on: forgetting the pending claim there would leave a menu
// opened by finger holding no focus, with nothing for Narrator to announce and no
// key that walks it.
func TestMenuFocusStillClaimsFocusForAMenuOpenedByTheClearingTap(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	h.state.open(h.background)
	h.state.abandonRestore() // the same outside tap, evaluated above the open

	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("the menu did not claim focus on its first frame, it is on %d: abandonRestore cancels "+
			"a hand-back that is on its way out, not the claim of a menu on its way in", got)
	}
}

// What a cancelled restore costs while the menu is still open: the menu stops
// holding focus, because after that clear it genuinely does not hold it, and only
// a menu that HELD focus restores it. Nothing is stranded — the menu's key
// filters carry no focus target, so a navigation key pulls focus back in — and a
// menu that has taken focus back is whole again, including the hand-back its next
// close owes the trigger.
func TestMenuFocusLetsAKeyBringBackAMenuTheClearingTapLeftUnfocused(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	claimed := new(int)
	h.extra = []event.Tag{claimed}
	trigger := h.background
	h.state.open(trigger)
	h.frame()

	h.openFrameAfterAnOutsideTap()
	if got := h.focused(); got != -1 {
		t.Fatalf("the open menu pulled focus back onto item %d on the very frame the tap emptied it: "+
			"that is the yank the dismissed keyboard was trying to get away from, and it undoes a clear "+
			"the user asked for", got)
	}

	h.frame(keyDown)
	if got := h.focused(); got != 0 {
		t.Fatalf("a navigation key did not bring focus back into the still-open menu, it is on %d: the "+
			"cancelled restore left the menu unreachable rather than merely unfocused", got)
	}

	// The menu is holding focus again, so its close owes the trigger a hand-back.
	h.frameStealingFocus(nil, claimed)
	h.closedFrame() // a handler has focus: the close waits out its one grace frame
	h.frameStealingFocus(nil, nil)

	h.closedFrame()
	if !h.router.Source().Focused(trigger) {
		t.Fatal("the close after a menu took focus back handed nothing to the trigger: cancelling the " +
			"earlier restore has to leave the menu whole, not half-cancelled")
	}
}

// The parts of the restore, pinned as a set. held and verify each have a frame
// sequence above that proves why they must go; settle does not, because settle is
// only ever READ while held is set, so a stale one costs nothing until some later
// sequence sets held again — a close whose one frame of grace is spent before it
// starts, and a hand-back quietly dropped. A flag that outlives its owner is
// worth pinning where it is written rather than waiting for that sequence.
//
// The other half of the claim is what abandonRestore must NOT touch: pending,
// because the tap that opens a menu is a tap outside every editor too and the
// clear can land on the frame the menu is opening on, and trigger, because it is
// still where a restore this menu earns again belongs.
func TestMenuFocusAbandonClearsEveryPartOfTheRestoreAndNothingElse(t *testing.T) {
	trigger := new(int)
	m := menuFocusState{pending: true, trigger: trigger, held: true, settle: true, verify: true}
	m.abandonRestore()

	if m.held || m.settle || m.verify {
		t.Errorf("abandonRestore left held=%v settle=%v verify=%v: every one of them is a piece of the "+
			"restore this call exists to cancel", m.held, m.settle, m.verify)
	}
	if !m.pending {
		t.Error("abandonRestore dropped the claim of a menu that is opening, which the same outside tap " +
			"can be the cause of")
	}
	if m.trigger != trigger {
		t.Error("abandonRestore forgot the trigger, so a restore this menu earns again later would land " +
			"in the composer instead of on the widget the user opened the menu from")
	}
}

// Reopening a menu in the gap between the hand-back and its check must not have
// the check answered by the new menu's focus: the question was about the old
// menu's trigger, and by then nobody is asking it.
func TestMenuFocusDropsTheCheckWhenTheMenuReopens(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	h.state.open(new(int)) // a trigger that is no longer drawn
	h.frame()
	h.closingFrame()
	h.closedFrame() // hands focus to the trigger, which cannot take it

	// The user opens the menu again on another row before the check frame.
	h.state.open(h.background)
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("harness bug: the reopened menu was supposed to hold focus, it is on %d", got)
	}
	h.closingFrame()
	h.closedFrame()
	if !h.router.Source().Focused(h.background) {
		t.Fatal("a stale check must not survive a reopen and steer the next close to the fallback")
	}
}

// The check is a question about one hand-back, so it has to be answered once.
// Left armed, it stops being a check and becomes a standing rule that focus may
// never be empty in this window: any later clear — for any reason, long after
// the menu is gone — would pull focus into the composer, and on a tablet that
// raises the on-screen keyboard nobody asked for.
func TestMenuFocusChecksTheHandBackOnlyOnce(t *testing.T) {
	h := newMenuHarness(3, true)
	h.fallback = new(int)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	h.closingFrame()
	h.closedFrame() // hands focus back to the trigger
	h.closedFrame() // the check frame: the trigger has it, so nothing to do

	// Much later, something empties focus for reasons of its own.
	h.frameStealingFocus(nil, nil)

	h.closedFrame()
	if h.router.Source().Focused(h.fallback) {
		t.Fatal("the hand-back check must be spent after the frame that reads it; a check left armed " +
			"grabs focus back into the composer on any later frame that happens to have none")
	}
}

// dismissOnOutsideTap has to report whether it actually cleared focus: the
// cancellation above hangs off that answer, and a bool nobody returns compiles
// just as well as one that is right.
func TestDismissOnOutsideTapReportsTheClear(t *testing.T) {
	t.Run("clears when our keyboard is up", func(t *testing.T) {
		s := new(touchKeyboardState)
		s.shownByUs.Store(true)
		s.noteWindowTouchPress(1)
		if !s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0))) {
			t.Fatal("an outside tap while our keyboard is up clears focus and must say so")
		}
	})
	t.Run("no keyboard of ours, no clear", func(t *testing.T) {
		s := new(touchKeyboardState)
		s.noteWindowTouchPress(1)
		if s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0))) {
			t.Fatal("no focus is cleared when the keyboard is not ours to dismiss")
		}
	})
	t.Run("no outside tap, no clear", func(t *testing.T) {
		s := new(touchKeyboardState)
		s.shownByUs.Store(true)
		if s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0))) {
			t.Fatal("nothing was tapped outside, so nothing was cleared")
		}
	})
	t.Run("suppressed evaluation, no clear", func(t *testing.T) {
		s := new(touchKeyboardState)
		s.shownByUs.Store(true)
		s.noteWindowTouchPress(1)
		s.noteExplicitEditorFocus()
		if s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0))) {
			t.Fatal("a suppressed evaluation clears nothing")
		}
	})
}

// The wiring the tests above cannot see: layout() has to pass a fallback to
// every overlay restore, and it has to USE the answer dismissOnOutsideTap returns.
// Dropping the result of a bool-returning call is silent in Go, so nothing but
// a check like this notices the cancellation going missing.
func TestLayoutWiresTheRestoreFallbackAndItsCancellation(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "window.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing window.go: %v", err)
	}

	var layoutFn *ast.FuncDecl
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if ok && fd.Name.Name == "layout" && fd.Recv != nil {
			layoutFn = fd
		}
		return true
	})
	if layoutFn == nil {
		t.Fatal("no (*Window).layout in window.go — this guard can no longer see the code it protects")
	}

	// addressOfAField reports whether an expression is &w.something — the shape
	// of a real focus target, and not of nil or of a placeholder value.
	addressOfAField := func(x ast.Expr) bool {
		ue, ok := x.(*ast.UnaryExpr)
		if !ok || ue.Op != token.AND {
			return false
		}
		_, ok = ue.X.(*ast.SelectorExpr)
		return ok
	}

	// method reports the method name of a w.<field>.<method>(...) call.
	method := func(ce *ast.CallExpr, name string) bool {
		se, ok := ce.Fun.(*ast.SelectorExpr)
		return ok && se.Sel.Name == name
	}

	restores := 0
	ast.Inspect(layoutFn.Body, func(n ast.Node) bool {
		ce, ok := n.(*ast.CallExpr)
		if !ok || !method(ce, "restoreOnClose") {
			return true
		}
		restores++
		if len(ce.Args) != 2 {
			t.Errorf("restoreOnClose is called with %d arguments: without a fallback, a close whose "+
				"trigger has left the frame lands focus on nothing", len(ce.Args))
			return true
		}
		if !addressOfAField(ce.Args[1]) {
			t.Errorf("restoreOnClose's fallback argument is %s, not the address of a widget on this "+
				"window: a fallback the call does not actually supply compiles and reads as wired up, "+
				"while a close whose trigger has left the frame still lands focus on nothing",
				types.ExprString(ce.Args[1]))
		}
		return true
	})
	if restores != 4 {
		t.Errorf("layout() calls restoreOnClose %d times, want 4 (the peer menu, message menu, identity panel and console modal)", restores)
	}

	// The cancellation has to hang off the call's own result. Anything else —
	// an unconditional abandonRestore, or none — means an outside tap either
	// stops cancelling or cancels checks it has nothing to do with.
	var guarded []string
	ast.Inspect(layoutFn.Body, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || is.Init != nil {
			return true
		}
		ce, ok := is.Cond.(*ast.CallExpr)
		if !ok || !method(ce, "dismissOnOutsideTap") {
			return true
		}
		ast.Inspect(is.Body, func(n ast.Node) bool {
			if ce, ok := n.(*ast.CallExpr); ok && method(ce, "abandonRestore") {
				if se, ok := ce.Fun.(*ast.SelectorExpr); ok {
					if inner, ok := se.X.(*ast.SelectorExpr); ok {
						guarded = append(guarded, inner.Sel.Name)
					}
				}
			}
			return true
		})
		return true
	})

	for _, want := range []string{"peerMenuFocus", "msgMenuFocus"} {
		found := false
		for _, got := range guarded {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("layout() does not cancel %s's pending restore check under dismissOnOutsideTap: "+
				"the deliberate focus clear would be read as a trigger that is no longer drawn, focus would "+
				"move to the composer, and the blur-driven keyboard hide would be cancelled with it", want)
		}
	}
}
