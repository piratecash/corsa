package desktop

import (
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// The round this file belongs to answered a question the previous one had
// deliberately left standing. tkHandlerMu makes each pane callback's generation
// agree with its own position in the command queue, and that is all a mutex can
// do here: COM may execute two methods of an in-proc MTA object at the same
// time and promises nothing about which of them reaches a user lock first, so a
// Showing RAISED before a Hiding may take the lock AFTER it. Both commands are
// then individually well-formed, the service applies them in queue order, and
// the session ends on the verdict of the event that happened FIRST — paneVisible
// true over a keyboard that closed, or false under one that is up.
//
// Nothing in the process can rank the two. The inverted Hiding carries exactly
// the generation a legitimately earlier Hiding carries, an in-flight counter
// cannot help (a callback may be preempted between COM raising the event and its
// first instruction, so "no overlap" does not imply "in order"), and a probe at
// the event itself is blind — the Showing arrives BEFORE its pane is on screen,
// so "about to appear" and "already gone" read the same.
//
// So the order is not reconstructed. It is overruled, later, by the screen: the
// occlusion monitor already samples the pane every 250ms, and where it sees a
// disagreement that no event still in flight could explain it says so, through
// the queue, and the service decides against the pane's own geometry. The
// asymmetry that makes this work is that IFrameworkInputPane::Location answers
// GLOBALLY — a rectangle somewhere might belong to another app and proves
// nothing about us, but no rectangle at all is a fact about the whole desktop,
// and therefore about us. It is also the only probe that can settle the "down"
// direction at all, since a zero OccludedRect means only that the pane does not
// occlude THIS window.
//
// The guards below hold the placement that fix consists of: that a terminal
// session state can be reached without an event; that the "down" conclusion is
// never drawn from anything weaker than Location, and never at all when Location
// cannot answer; that the correction does not re-arm the close window it exists
// to get past; that it is bound to the monitor that reported, to the pane
// session boundary it was formed at, and to a session that has pane events in
// the first place; that the monitor still never touches
// the STA-owned pane itself; and that the "up" direction stays out of the way of
// an ordinary close, which legitimately shows a pane after paneVisible is gone.

// tkCmdCase returns the command-queue case selecting on the named tkCmd* kind.
func tkCmdCase(t *testing.T, f *ast.File, kind string) *ast.CaseClause {
	t.Helper()
	var out *ast.CaseClause
	ast.Inspect(f, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, e := range cc.List {
			if id, ok := e.(*ast.Ident); ok && id.Name == kind {
				out = cc
			}
		}
		return true
	})
	if out == nil {
		t.Fatalf("no `case %s` in the command-queue switch: with it goes the only path by which a "+
			"session's final state can be corrected against the pane, and an inverted callback pair "+
			"strands the window under a closed keyboard for the rest of its life", kind)
	}
	return out
}

// A session that ends only when a Hiding says so ends whenever the Hiding was
// APPLIED, which under an inverted pair is not when the keyboard closed.
func TestASessionCanEndWithoutTheEventThatWouldHaveEndedIt(t *testing.T) {
	f := tkWindowsAST(t)
	cc := tkCmdCase(t, f, "tkCmdPaneTruth")

	var cleared bool
	ast.Inspect(cc, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		if tkCallName(call.Fun) != "cmd.kbd.paneVisible.Store" {
			return true
		}
		if id, ok := call.Args[0].(*ast.Ident); ok && id.Name == "false" {
			cleared = true
		}
		return true
	})
	if !cleared {
		t.Error("tkCmdPaneTruth never clears paneVisible: then the only writer of that terminal state is " +
			"the Hiding event, and an event applied out of order leaves the record saying a keyboard is " +
			"up that is not — with no re-show ever scheduled, because nothing else clears it")
	}
}

// The "down" direction is the one with no local evidence: a zero OccludedRect
// is equally consistent with a pane docked past this window. Only the pane's own
// geometry can settle it, and only in the negative.
func TestThePaneIsDeclaredDownOnlyOnItsOwnGeometry(t *testing.T) {
	f := tkWindowsAST(t)
	cc := tkCmdCase(t, f, "tkCmdPaneTruth")

	probes := tkCallsTo(cc, "tkFwPaneLocationVisible")
	if len(probes) != 1 {
		t.Fatalf("tkCmdPaneTruth calls tkFwPaneLocationVisible %d times, want exactly 1: the correction "+
			"either draws its conclusion from something weaker than the pane's own geometry, or asks "+
			"twice and can act on two different answers", len(probes))
	}
	ast.Inspect(cc, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		if tkCallName(call.Fun) != "cmd.kbd.paneVisible.Store" {
			return true
		}
		id, ok := call.Args[0].(*ast.Ident)
		if !ok || id.Name != "false" {
			return true
		}
		if call.Pos() < probes[0].Pos() {
			t.Errorf("tkCmdPaneTruth clears paneVisible (offset %d) before asking the pane where it is "+
				"(offset %d): the monitor's zero rect says only that nothing occludes THAT window, so a "+
				"session ended on it alone is a session ended under a keyboard docked past the window",
				call.Pos(), probes[0].Pos())
		}
		return true
	})
}

// An HRESULT error and an absent pane are not "no keyboard". They are no answer,
// and no answer must leave the session exactly as it was.
func TestAnUnanswerableProbeEndsNothing(t *testing.T) {
	f := tkWindowsAST(t)
	cc := tkCmdCase(t, f, "tkCmdPaneTruth")

	var guard *ast.IfStmt
	ast.Inspect(cc, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Init == nil {
			return true
		}
		if !tkMentions(ifs.Init, "tkFwPaneLocationVisible") {
			return true
		}
		guard = ifs
		return true
	})
	if guard == nil {
		t.Fatal("the tkFwPaneLocationVisible result is not bound in an if-init inside tkCmdPaneTruth: " +
			"whatever reads it now, the two-valued answer (visible, ok) has to be branched on where it " +
			"is produced or one of its two values goes unread")
	}

	assign, ok := guard.Init.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) != 2 {
		t.Fatalf("the tkFwPaneLocationVisible init binds %T with the wrong shape: it returns (visible, ok) "+
			"and both halves are load-bearing — 'not visible' and 'could not tell' are different answers", guard.Init)
	}
	for _, lhs := range assign.Lhs {
		id, ok := lhs.(*ast.Ident)
		if !ok {
			t.Fatalf("the tkFwPaneLocationVisible result is destructured into %T, not plain names", lhs)
		}
		if !tkMentions(guard.Cond, id.Name) {
			t.Errorf("the guard on the pane probe ignores %q: an unread 'ok' makes an HRESULT error read "+
				"as 'no keyboard on the desktop', and the session is then ended on a failure to ask", id.Name)
		}
	}
	// The shape, not just the names: "stop if a pane is up OR if we could not
	// tell" and "stop if a pane is up AND we could tell" read alike and mention
	// the same two values, but the second ends the session on every failure to
	// ask — which, on a machine where Location is erroring, is every time.
	bin, isOr := guard.Cond.(*ast.BinaryExpr)
	if !isOr || bin.Op != token.LOR {
		t.Fatalf("the guard on the pane probe is %T (%v), want a disjunction: a pane that IS up and a "+
			"pane that could not be asked about are independent reasons to do nothing, so either alone "+
			"has to stop the correction", guard.Cond, tkOpOf(guard.Cond))
	}
	want := map[string]bool{tkNeg(assign.Lhs[0]): true, "!" + tkNeg(assign.Lhs[1]): true}
	got := map[string]bool{tkNeg(bin.X): true, tkNeg(bin.Y): true}
	if len(want) != 2 || len(got) != 2 || !tkSameSet(want, got) {
		t.Errorf("the guard on the pane probe reads %q, want the visible result plain and the ok result "+
			"negated: a probe that could not answer must stop the correction just as firmly as one that "+
			"answered 'a pane is up'", tkKeys(got))
	}
	if guard.Else != nil {
		t.Error("the pane probe's guard has an else branch: the correction must fall THROUGH on a " +
			"conclusive negative and stop on anything else, so that every future statement added to the " +
			"case is behind the proof rather than beside it")
	}
	if len(guard.Body.List) != 1 {
		t.Fatalf("the pane probe's guard body has %d statements, want exactly 1 (the break): anything "+
			"else runs when the answer was 'a pane is up' or 'cannot tell', which are the two cases "+
			"where nothing at all should happen", len(guard.Body.List))
	}
	if br, ok := guard.Body.List[0].(*ast.BranchStmt); !ok || br.Tok.String() != "break" {
		t.Errorf("the pane probe's guard body is %T, want a break: an inconclusive probe must leave the "+
			"session untouched and let the monitor ask again", guard.Body.List[0])
	}
}

// The hide window exists so a re-tap landing DURING a close still re-shows.
// Arming it from a correction that fires seconds after the close would suppress
// the very re-show the correction unblocks.
func TestTheCorrectionDoesNotReArmTheCloseWindow(t *testing.T) {
	f := tkWindowsAST(t)
	cc := tkCmdCase(t, f, "tkCmdPaneTruth")

	for _, name := range []string{"tkHideDeadlineNs", "tkOrphanHidingNs"} {
		if tkMentions(cc, name) {
			t.Errorf("tkCmdPaneTruth touches %s: this correction runs at least a zero streak after the "+
				"pane went, so a close window armed here is a close that never happened, and the next "+
				"tap is read as landing mid-animation", name)
		}
	}
}

// A monitor that has been replaced is describing a session that is no longer
// there, and a session with no pane events cannot have had them inverted.
//
// Neither of those is a session binding, which is the distinction this round
// turns on. One monitor serves EVERY session of a window: tkEnsureOcclusionMonitor
// bumps the ping and returns when one is already running, so the reporter's id
// survives a close and a reopen unchanged, and expectHiding is a property of the
// kind of session, not of which one. A verdict formed while the old pane was
// down therefore passes both guards after the user has reopened the keyboard —
// and is applied to the new session, whose pane has been accepted but is not on
// screen yet, so Location says "down", the correction ends it, and clearing
// shownByUs with it disarms the deferred check that is the last thing that would
// have noticed a TryShow accepted and never realized. The tap is swallowed, by
// the very machinery that exists so taps are not swallowed.
//
// Only a boundary counter read when the verdict was FORMED can separate the two
// panes, so that is what the report carries and what the gate rejects it on.
func TestTheCorrectionIsBoundToItsReporterAndToAPaneSession(t *testing.T) {
	f := tkWindowsAST(t)
	cc := tkCmdCase(t, f, "tkCmdPaneTruth")

	for _, want := range []struct{ name, why string }{
		{"released", "a destroyed window's state was already cleaned, and its HWND may have been reused"},
		{"monitorOwner", "a sample from a monitor that has since been replaced describes a session that ended"},
		{"expectHiding", "a legacy session raises no Showing/Hiding pair, so it has no inversion to correct — " +
			"and it is torn down by its zero streak through tkCmdOwnerExpire instead"},
	} {
		if !tkMentions(cc, want.name) {
			t.Errorf("tkCmdPaneTruth does not consult %s: %s", want.name, want.why)
		}
	}

	// The boundary is checked in the case's FIRST statement, ahead of every
	// write: a correction that has already ended a session is not made harmless
	// by discovering afterwards that it was addressed to a different one.
	gate, ok := cc.Body[0].(*ast.IfStmt)
	if !ok {
		t.Fatalf("tkCmdPaneTruth opens with %T, not the guard: everything this case does is terminal for "+
			"a session, so anything ahead of the guard is applied to whatever session happens to be "+
			"running when the command comes up", cc.Body[0])
	}
	bound := false
	ast.Inspect(gate.Cond, func(n ast.Node) bool {
		bin, ok := n.(*ast.BinaryExpr)
		if !ok || bin.Op != token.NEQ {
			return true
		}
		got := map[string]bool{tkOperand(bin.X): true, tkOperand(bin.Y): true}
		if got["cmd.kbd.occlusionEpoch.Load"] && got["cmd.epoch"] {
			bound = true
		}
		return true
	})
	if !bound {
		t.Error("the tkCmdPaneTruth guard does not compare cmd.epoch against the occlusion epoch standing " +
			"now: monitorOwner and expectHiding both outlive the session the report was about, so with " +
			"no boundary check a verdict about the pane that closed is applied to the pane the user has " +
			"just reopened — ending a session mid-show and disarming the deferred TryShow check with it")
	}

	// ...and both reports carry a boundary read BEFORE the sample they are
	// drawn from, which is the same epoch, and the same reasoning, as a publish.
	fn := tkFuncDecl(t, f, "tkEnsureOcclusionMonitor")
	reports := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || tkCallName(call.Fun) != "tkEnqueue" || len(call.Args) != 1 {
			return true
		}
		lit, ok := call.Args[0].(*ast.CompositeLit)
		if !ok || !tkMentions(lit, "tkCmdPaneTruth") {
			return true
		}
		reports++
		for _, el := range lit.Elts {
			kv, ok := el.(*ast.KeyValueExpr)
			if !ok || tkCallName(kv.Key) != "epoch" {
				continue
			}
			if id, ok := kv.Value.(*ast.Ident); !ok || id.Name != "epoch" {
				t.Errorf("a tkCmdPaneTruth report (offset %d) carries an epoch it did not take from the "+
					"poll: read at the enqueue instead, it certifies when the report was WRITTEN rather "+
					"than when the pane was LOOKED AT, and a boundary landing between those two lets a "+
					"verdict about the old pane be applied to the new one", call.Pos())
			}
			return true
		}
		t.Errorf("a tkCmdPaneTruth report (offset %d) carries no epoch: the gate then has nothing to "+
			"check it against, and the report outlives the pane it describes", call.Pos())
		return true
	})
	if reports != 2 {
		t.Errorf("the monitor forms %d tkCmdPaneTruth reports, want exactly 2 (the pane is up though the "+
			"state says closed, and the pane is down though the state says open): a direction that is "+
			"never reported is an inversion that is never corrected", reports)
	}
}

// The pane object belongs to the service thread's apartment. The monitor runs in
// one of its own, which is the entire reason the correction is a command rather
// than a check the monitor could make on the spot.
func TestTheMonitorNeverAsksThePaneItself(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkEnsureOcclusionMonitor")

	for _, name := range []string{"fwPane", "tkFwPaneLocationVisible"} {
		if tkMentions(fn.Body, name) {
			t.Errorf("the occlusion monitor mentions %s: that object was activated on the service "+
				"thread's apartment and calling into it from this goroutine is the marshaling bug this "+
				"file has spent rounds on — the monitor reports, the service asks", name)
		}
	}
}

// The wait for a Hiding that may already have been spent was, before this round,
// unbounded: sleep a second, loop, forever, with paneVisible the only exit and
// nothing left to clear it.
func TestTheWaitForAHidingReportsInsteadOfOnlySleeping(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkEnsureOcclusionMonitor")

	var wait *ast.IfStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if call, ok := ifs.Cond.(*ast.CallExpr); ok && tkCallName(call.Fun) == "kbd.expectHiding.Load" {
			wait = ifs
		}
		return true
	})
	if wait == nil {
		t.Fatal("the monitor no longer branches on kbd.expectHiding: that branch is where a WinRT " +
			"session waits out a zero streak for the Hiding that ends it, and where a spent Hiding " +
			"leaves it waiting for good")
	}
	if !tkEnqueuesKind(wait.Body, "tkCmdPaneTruth") {
		t.Error("the wait for a Hiding enqueues no tkCmdPaneTruth: if that event was applied out of " +
			"order it is not coming again, and this branch is then a sleep with no exit — the window " +
			"keeps its padding under a keyboard that closed and never asks for another")
	}
}

// An ordinary close clears paneVisible while the pane is still on screen shutting
// down. Those samples contradict the state for entirely legitimate reasons.
func TestAClosingPaneIsNotMistakenForOneTheStateLost(t *testing.T) {
	f := tkWindowsAST(t)
	fn := tkFuncDecl(t, f, "tkEnsureOcclusionMonitor")

	var guard *ast.IfStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || !tkMentions(ifs.Cond, "platformKeyboardClosing") {
			return true
		}
		guard = ifs
		return true
	})
	if guard == nil {
		t.Fatal("nothing in the monitor consults platformKeyboardClosing: a pane still animating shut " +
			"occludes the window while paneVisible is already false, and without that exclusion the " +
			"disagreement streak matures on an ordinary hide and republishes padding under it")
	}
	if !tkMentions(guard.Cond, "paneVisible") {
		t.Error("the close exclusion is not part of the disagreement test itself: it has to gate the " +
			"same condition that counts the streak, or the streak simply pauses and resumes across a " +
			"close rather than being reset by it")
	}

	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || tkCallName(call.Fun) != "tkEnqueue" || len(call.Args) != 1 {
			return true
		}
		lit, ok := call.Args[0].(*ast.CompositeLit)
		if !ok || !tkMentions(lit, "tkCmdPaneTruth") || !tkMentions(lit, "sawPane") {
			return true
		}
		found = true
		if !tkEnclosing(fn.Body, call, func(m ast.Node) bool {
			ifs, ok := m.(*ast.IfStmt)
			return ok && tkMentions(ifs.Cond, "tkPaneTruthStreak")
		}) {
			t.Errorf("the pane-is-up report (offset %d) is not behind a streak of samples: one sample "+
				"taken in the ordinary gap between a pane appearing and its Showing being applied is "+
				"not a race, and reporting it costs a session bump for nothing", call.Pos())
		}
		return true
	})
	if !found {
		t.Error("the monitor never reports a pane it can SEE over the window while the state says the " +
			"session is closed: that is the same inversion the other way round, and tkCmdPublish then " +
			"refuses every sample it takes, so the composer stays under the keyboard until it goes")
	}
}

// tkEnqueuesKind reports whether n contains a tkEnqueue of a command literal
// naming this kind.
func tkEnqueuesKind(n ast.Node, kind string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok || tkCallName(call.Fun) != "tkEnqueue" || len(call.Args) != 1 {
			return true
		}
		if lit, ok := call.Args[0].(*ast.CompositeLit); ok && tkMentions(lit, kind) {
			found = true
		}
		return true
	})
	return found
}

// tkNeg renders an identifier, or a negated one as "!name", and "" for anything
// else — enough to tell `up || !ok` from `up && ok` without a type checker.
func tkNeg(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.UnaryExpr:
		if x.Op == token.NOT {
			if inner, ok := x.X.(*ast.Ident); ok {
				return "!" + inner.Name
			}
		}
	}
	return ""
}

// tkOperand renders one side of a comparison as a dotted name, unwrapping a
// call so `x.Load()` reads as "x.Load" — enough to tell which two things a
// guard compares without a type checker.
func tkOperand(e ast.Expr) string {
	if call, ok := e.(*ast.CallExpr); ok {
		return tkCallName(call.Fun)
	}
	return tkCallName(e)
}

// tkOpOf renders a binary expression's operator, for a legible failure.
func tkOpOf(e ast.Expr) string {
	if bin, ok := e.(*ast.BinaryExpr); ok {
		return bin.Op.String()
	}
	return "(not a binary expression)"
}

func tkSameSet(a, b map[string]bool) bool {
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func tkKeys(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, " / ")
}
