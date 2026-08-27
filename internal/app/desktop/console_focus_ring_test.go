package desktop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"image"
	"testing"

	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget/material"
)

// console_focus_ring_test.go covers keyboard containment: while the console
// modal is up, the focus traversal must not reach the window underneath, where
// enough presses of Tab would land on the composer and Enter would send
// whatever was typed.
//
// Containment is structural rather than a list of items. Gio decides what the
// traversal can reach from what each widget DECLARES — a widget registers its
// key.FocusFilter through gtx.Event, and a disabled Source returns from
// gtx.Event without registering anything — so the window under the modal is
// laid out with input disabled and simply is not in the walk.
//
// The walk itself is Router.MoveFocus, which is exactly what app.Window calls
// for a Tab nothing handled.

// consoleWalkHarness lays the console modal over a composer, the way
// Window.layout does, and walks the focus the way the platform does.
type consoleWalkHarness struct {
	w      *Window
	router input.Router
}

func newConsoleWalkHarness(t *testing.T) *consoleWalkHarness {
	t.Helper()
	h := &consoleWalkHarness{w: newConsoleWithCommands(t)}
	openConsoleForTest(h.w)
	return h
}

func (h *consoleWalkHarness) frame() layout.Context {
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      h.router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	// Exactly how Window.layout hands the window under a modal to its widgets.
	under := h.w.disableUnderConsoleModal(gtx)
	material.Editor(h.w.theme, &h.w.messageEditor, "").Layout(under)
	if h.w.consoleModalVisible() {
		h.w.layoutConsoleOverlay(gtx)
	}
	h.router.Frame(gtx.Ops)
	return gtx
}

// walk moves the focus one step and returns the frame that shows where it
// landed.
func (h *consoleWalkHarness) walk(dir key.FocusDirection) layout.Context {
	h.router.MoveFocus(dir)
	return h.frame()
}

// The window under an open console declares no focus targets at all.
func TestConsoleModalTakesInputAwayFromTheWindowUnderIt(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	gtx := layout.Context{
		Ops: new(op.Ops),
		// A real Source: Enabled() is false without one, which would make the
		// assertion below pass for the wrong reason.
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}

	if !w.disableUnderConsoleModal(gtx).Enabled() {
		t.Fatal("the window is disabled with no console modal open")
	}

	openConsoleForTest(w)
	if w.disableUnderConsoleModal(gtx).Enabled() {
		t.Fatal("the window under the console modal still accepts input, so the focus walk can leave it")
	}
}

// The disabling has to be wired into the frame, and no test that lays widgets
// out by hand can see that: each of them applies the helper itself. So this
// one reads the source.
//
// Two helpers, two surfaces, one rule: whatever a modal surface covers is
// laid out with input disabled, or the focus walk leaves that surface for the
// composer underneath it. The frame is layout() plus the function it hands
// the window's own surfaces to, so both are searched — moving the call from
// one to the other is a refactor, dropping it is the bug.
func TestLayoutDisablesTheWindowUnderModalSurfaces(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "window.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing window.go: %v", err)
	}
	frame := map[string]bool{"layout": true, "layoutWindowSurfaces": true}
	var bodies []*ast.BlockStmt
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if ok && fd.Recv != nil && frame[fd.Name.Name] {
			bodies = append(bodies, fd.Body)
		}
		return true
	})
	if len(bodies) != len(frame) {
		t.Fatalf("found %d of the %d frame functions in window.go — this guard can no longer see the code it protects",
			len(bodies), len(frame))
	}

	called := map[string]bool{}
	for _, body := range bodies {
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				called[sel.Sel.Name] = true
			}
			return true
		})
	}
	for _, helper := range []string{"disableUnderConsoleModal", "disableUnderImageViewer"} {
		if !called[helper] {
			t.Errorf("the frame never calls %s: what that surface covers keeps its focus targets, "+
				"and the focus walk can leave the surface for the composer", helper)
		}
	}
}

// Reading a background widget's clicks is enough to make it Tab-reachable:
// Clickable.Clicked registers the widget's key.FocusFilter, which is what puts
// it in the traversal — laying it out with input disabled afterwards does not
// take that back. So Window.handleActions must stop before its own controls
// while the modal is open.
func TestBackgroundActionsAreNotReadWhileTheConsoleIsOpen(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	w.handleActions(gtx)
	// Everything the window draws goes down with input disabled, exactly as
	// Window.layout does it.
	under := w.disableUnderConsoleModal(gtx)
	w.sendButton.Layout(under, func(gtx layout.Context) layout.Dimensions {
		return layout.Dimensions{Size: image.Pt(40, 20)}
	})
	w.layoutConsoleOverlay(gtx)
	router.Frame(gtx.Ops)

	// Walk as far as the modal has controls; Send must never come up.
	for i := 0; i < 30; i++ {
		router.MoveFocus(key.FocusForward)
		next := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.handleActions(next)
		under := w.disableUnderConsoleModal(next)
		w.sendButton.Layout(under, func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: image.Pt(40, 20)}
		})
		w.layoutConsoleOverlay(next)
		router.Frame(next.Ops)

		if next.Focused(&w.sendButton) {
			t.Fatalf("focus step %d reached the Send button under the modal: Enter there posts the hidden draft", i+1)
		}
	}
}

// And end to end: walking the focus, in either direction and as many times as
// the modal has controls, never lands on the composer underneath.
func TestFocusWalkNeverLeavesTheConsoleModal(t *testing.T) {
	for _, dir := range []key.FocusDirection{key.FocusForward, key.FocusBackward} {
		h := newConsoleWalkHarness(t)
		h.frame()
		h.frame()

		for i := 0; i < 30; i++ {
			gtx := h.walk(dir)
			if gtx.Focused(&h.w.messageEditor) {
				t.Fatalf("focus step %d (%v) reached the composer under the modal", i+1, dir)
			}
		}
	}
}

// Every control the console draws stays reachable. An earlier cut contained
// the walk with a fixed list of items, which made everything NOT on that list
// — the Copy button of a history entry, the donate rows, the per-file actions
// — unreachable by keyboard.
func TestFocusWalkReachesControlsNoListWouldCarry(t *testing.T) {
	h := newConsoleWalkHarness(t)
	h.frame()
	h.frame()

	entries := h.w.consoleModal.consoleHistory()
	if len(entries) == 0 {
		t.Fatal("test setup: the console has no history entry to reach")
	}
	// A history row's Copy button belongs to no fixed list: the rows come and
	// go with the commands the user runs.
	want := []event.Tag{&entries[0].CopyButton}

	reached := false
	for i := 0; i < 30 && !reached; i++ {
		gtx := h.walk(key.FocusForward)
		for _, tag := range want {
			reached = reached || gtx.Focused(tag)
		}
	}
	if !reached {
		t.Fatal("walking the focus never reached a history entry's Copy button: the modal's own controls are unreachable")
	}
}
