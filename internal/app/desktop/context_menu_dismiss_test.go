package desktop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"image"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// consumeComposerFocus is the whole of the "may the composer take focus?"
// decision, so the truth table IS the contract. The row that matters is the
// last one: a long-press opens the menu and finishes the row's Clickable, which
// asks for the composer; honouring that ask puts focus on an editor underneath
// the overlay, where Enter sends the draft instead of activating the
// highlighted item.
func TestConsumeComposerFocusDropsTheRequestUnderAnOpenMenu(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		pending, raiseKeyboard, menuOpen bool
		wantFocus, wantRaise             bool
	}{
		{"no request, nothing happens", false, false, false, false, false},
		{"no request cannot raise the keyboard either", false, true, false, false, false},
		{"plain request focuses, keyboard stays down", true, false, false, true, false},
		{"touch-driven request focuses and raises", true, true, false, true, true},
		{"a menu is up: the request is dropped", true, false, true, false, false},
		{"a menu is up: the keyboard is dropped with it", true, true, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			focus, raise := consumeComposerFocus(tc.pending, tc.raiseKeyboard, tc.menuOpen)
			if focus != tc.wantFocus || raise != tc.wantRaise {
				t.Fatalf("consumeComposerFocus(%v, %v, %v) = (%v, %v), want (%v, %v)",
					tc.pending, tc.raiseKeyboard, tc.menuOpen, focus, raise, tc.wantFocus, tc.wantRaise)
			}
		})
	}
}

// The menus and identity details are full-window overlays over the composer,
// so every one of them has to answer the question. A guard that only knew
// about the contact menu would let focus move into the editor beneath them.
func TestContextMenuOpenCoversEveryComposerOverlay(t *testing.T) {
	peer := domain.PeerIdentity{}
	copy(peer[:], "11ab110000000000000000000000000000000000")

	w := &Window{}
	if w.contextMenuOpen() {
		t.Fatal("no menu is open on a fresh window")
	}
	w.contextMenuPeer = peer
	if !w.contextMenuOpen() {
		t.Fatal("the identity menu counts as open")
	}
	w.contextMenuPeer = domain.PeerIdentity{}
	w.msgContextMsg = &service.DirectMessage{}
	if !w.contextMenuOpen() {
		t.Fatal("the message menu counts as open")
	}
	w.msgContextMsg = nil
	w.identityPanelVisible = true
	if !w.contextMenuOpen() {
		t.Fatal("the my-identity panel counts as open")
	}
}

// The decision above is worthless where it is not applied, and layout() is not
// callable from a test — it wants a live app.Window, a router snapshot and a
// GPU-less frame. So this reads the source instead, and checks the one thing
// that cannot be inferred from behaviour reachable here: that the FocusCmd for
// the composer is gated on consumeComposerFocus, fed with BOTH pending flags
// and with contextMenuOpen.
//
// Gating at this single point is deliberate. Every setter of the flag runs from
// inside a widget laid out before the menu opens on the same frame — the row's
// Clickable completes at the top of layoutRecipientButton while openMenu runs
// further down it, and the "⋯" button is nested inside the row's own Clickable,
// so both fire — which leaves no setter able to see the menu it is competing
// with.
func TestLayoutGatesComposerFocusOnTheMenuGuard(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "window.go", nil, parser.SkipObjectResolution)
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

	// sel renders w.x / w.x() shaped expressions as "w.x", and anything else as
	// "", so the assertions below compare names rather than syntax trees.
	sel := func(e ast.Expr) string {
		if call, ok := e.(*ast.CallExpr); ok {
			e = call.Fun
		}
		se, ok := e.(*ast.SelectorExpr)
		if !ok {
			return ""
		}
		id, ok := se.X.(*ast.Ident)
		if !ok {
			return ""
		}
		return id.Name + "." + se.Sel.Name
	}

	// cleared reports whether stmt is `<target> = false`.
	cleared := func(stmt ast.Stmt, target string) bool {
		as, ok := stmt.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 || as.Tok != token.ASSIGN {
			return false
		}
		id, ok := as.Rhs[0].(*ast.Ident)
		return ok && id.Name == "false" && sel(as.Lhs[0]) == target
	}

	guards := 0
	ast.Inspect(layoutFn, func(n ast.Node) bool {
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, stmt := range block.List {
			as, ok := stmt.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 {
				continue
			}
			call, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok {
				continue
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != "consumeComposerFocus" {
				continue
			}
			guards++
			if len(call.Args) != 3 {
				t.Fatalf("consumeComposerFocus called with %d arguments, want 3", len(call.Args))
			}
			for j, want := range []string{"w.focusComposerPending", "w.composerKeyboardPending", "w.contextMenuOpen"} {
				if got := sel(call.Args[j]); got != want {
					t.Errorf("argument %d is %q, want %q — the guard is being fed something other than the state it is supposed to weigh", j, got, want)
				}
			}
			// Both flags are cleared right here, at this statement level, on
			// every path. A request the guard turned down must be GONE: one
			// left pending would be honoured a frame or two later, which is
			// the frame restoreOnClose is handing focus back to the trigger
			// on, and a keyboard flag left standing would raise the keyboard
			// on whatever focused the composer next.
			if i+2 >= len(block.List) ||
				!cleared(block.List[i+1], "w.focusComposerPending") ||
				!cleared(block.List[i+2], "w.composerKeyboardPending") {
				t.Error("the two statements after the guard are not the unconditional clearing of both pending flags")
			}
		}
		return true
	})
	if guards != 1 {
		t.Fatalf("layout() calls consumeComposerFocus %d times, want exactly 1: the composer's focus request has one consumer and that is the point of it", guards)
	}

	// And no unguarded path survives: the composer FocusCmd must not sit under
	// a bare `if w.focusComposerPending`.
	ast.Inspect(layoutFn, func(n ast.Node) bool {
		ifst, ok := n.(*ast.IfStmt)
		if !ok || sel(ifst.Cond) != "w.focusComposerPending" {
			return true
		}
		ast.Inspect(ifst.Body, func(n ast.Node) bool {
			if lit, ok := n.(*ast.CompositeLit); ok && sel(lit.Type) == "key.FocusCmd" {
				t.Error("a key.FocusCmd still runs straight off w.focusComposerPending, bypassing the open-menu guard")
			}
			return true
		})
		return false
	})
}

// menuDismissHarness lays out one context-menu overlay against a real
// input.Router, because everything this test claims is a claim about Gio's
// bookkeeping: that a tap in the clear area reaches the dismiss target, and
// that focus survives only for tags the frame both draws and filters for. The
// second half is the whole mechanism of the fix — an overlay that returns
// before its card leaves its items out of the frame, so the router drops their
// focus at Frame time and the NEXT frame's restoreOnClose finds focus free.
type menuDismissHarness struct {
	w      *Window
	router *input.Router
	ops    *op.Ops
	layout func(layout.Context) layout.Dimensions
}

func (h *menuDismissHarness) frame() {
	h.ops.Reset()
	gtx := layout.Context{
		Ops:         h.ops,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 800)},
		Source:      h.router.Source(),
	}
	h.layout(gtx)
	h.router.Frame(h.ops)
}

// tapClear presses in the top-left corner, far from any card this suite places,
// which is where the dismiss area is the only thing under the finger.
func (h *menuDismissHarness) tapClear() {
	h.router.Queue(pointer.Event{
		Kind:     pointer.Press,
		Source:   pointer.Touch,
		Position: f32.Pt(4, 4),
	})
}

func (h *menuDismissHarness) focusedAny(items []event.Tag) bool {
	src := h.router.Source()
	for _, it := range items {
		if src.Focused(it) {
			return true
		}
	}
	return false
}

func newDismissHarness(t *testing.T, msg bool) *menuDismissHarness {
	t.Helper()
	w := &Window{theme: newAppTheme()}
	h := &menuDismissHarness{w: w, router: new(input.Router), ops: new(op.Ops)}
	if msg {
		w.msgContextMsg = &service.DirectMessage{ID: "m1", Body: "hi"}
		w.msgContextPos = image.Pt(300, 400)
		w.msgMenuFocus.open(new(int))
		h.layout = w.layoutMsgContextMenuOverlay
		return h
	}
	var peer domain.PeerIdentity
	copy(peer[:], "11ab110000000000000000000000000000000000")
	w.contextMenuPeer = peer
	w.contextMenuPos = image.Pt(300, 400)
	w.peerMenuFocus.open(new(int))
	h.layout = w.layoutContextMenuOverlay
	return h
}

// A tap outside the menu closes it, and the closed menu must not be laid out
// again on the frame that closed it. Continuing past the dismissal measures and
// draws a card for state that has just been cleared — for the identity menu,
// a header for the zero peer — and, because the items are drawn, keeps their
// focus alive for a frame nobody can interact with.
func TestDismissedMenusStopBeforeTheirCard(t *testing.T) {
	for _, tc := range []struct {
		name  string
		msg   bool
		items func(*Window) []event.Tag
		open  func(*Window) bool
	}{
		{
			name:  "identity menu",
			items: func(w *Window) []event.Tag { return []event.Tag{&w.ctxMenuAlias, &w.ctxMenuCopy, &w.ctxMenuDelete} },
			open:  func(w *Window) bool { return !w.contextMenuPeer.IsZero() },
		},
		{
			name:  "message menu",
			msg:   true,
			items: func(w *Window) []event.Tag { return []event.Tag{&w.msgCtxReply, &w.msgCtxCopy} },
			open:  func(w *Window) bool { return w.msgContextMsg != nil },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDismissHarness(t, tc.msg)
			items := tc.items(h.w)

			// Open and settled: the menu owns focus and its dismiss area is
			// registered, which is what makes the next tap deliverable.
			h.frame()
			if !h.focusedAny(items) {
				t.Fatal("an open menu must hold focus on one of its own items before this test means anything")
			}

			h.tapClear()
			h.frame()
			if tc.open(h.w) {
				t.Fatal("a tap in the clear area must close the menu")
			}
			if h.focusedAny(items) {
				t.Fatal("the dismissing frame still drew the menu's items: the closed menu got another frame of layout, " +
					"so its focus survives and restoreOnClose has to wait for a frame nothing requests")
			}
		})
	}
}

// The control case for the test above: an ordinary frame with no tap must keep
// drawing the menu. Without this, an overlay that returned unconditionally
// would pass every assertion up there.
func TestUndismissedMenusKeepTheirFocus(t *testing.T) {
	h := newDismissHarness(t, false)
	items := []event.Tag{&h.w.ctxMenuAlias, &h.w.ctxMenuCopy, &h.w.ctxMenuDelete}

	h.frame()
	h.frame()
	if h.w.contextMenuPeer.IsZero() {
		t.Fatal("a frame with no tap in it must leave the menu open")
	}
	if !h.focusedAny(items) {
		t.Fatal("a menu that was not dismissed must still be drawn, and still hold focus")
	}
}
