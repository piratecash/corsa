package desktop

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"

	"gioui.org/io/event"

	"github.com/piratecash/corsa/internal/core/service"
)

// The item list is rebuilt from live state on EVERY frame, so the row holding
// focus can leave it without anybody closing anything: "Delete chat for
// everyone" drops out the instant the peer goes offline, "Delete message" the
// instant contextMenuDeleteEnabled stops agreeing. The frame then ends without
// mentioning the tag that holds focus, Gio drops it in Router.Frame, and an
// OPEN menu is left with no focus at all — Tab walking the background, Enter
// reaching whatever sits under the overlay, Narrator announcing nothing. The
// menu has to notice and take focus back.
func TestMenuFocusFollowsWhenTheFocusedItemLeavesTheList(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	h.frame(keyDown)
	h.frame(keyDown)
	if got := h.focused(); got != 2 {
		t.Fatalf("harness bug: focus was supposed to sit on the last item before it goes away, got %d", got)
	}

	// The peer goes offline: the row holding focus is simply not in this
	// frame's list.
	h.openFrame(h.items[:2])
	if got := h.focused(); got != 0 {
		t.Fatalf("an open menu whose focused item left the list must take focus back, it landed on %d — "+
			"a visible menu with no focus is exactly the state this contract exists to prevent", got)
	}
	if !h.state.held {
		t.Fatal("the reclaimed focus must be recorded as held, or the close will not hand it back to the trigger")
	}
}

// The control for the test above, and the reason it cannot simply reset to the
// top whenever the list changes: a list that shrinks somewhere OTHER than under
// the focused item leaves focus alone. Yanking it to the first row on every
// rebuild would move the user's place each time a peer's presence flickered.
func TestMenuFocusStaysPutWhenAShrinkingListKeepsItsItem(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	h.frame(keyDown)
	if got := h.focused(); got != 1 {
		t.Fatalf("harness bug: focus was supposed to sit on item 1, got %d", got)
	}

	h.openFrame(h.items[:2])
	if got := h.focused(); got != 1 {
		t.Fatalf("the focused item is still in the list, so focus must not move; it went to %d", got)
	}
}

// And the reclaim is only for a menu that HELD focus, on the same reasoning as
// the deferred-draw path: one opened by finger while the user was typing has no
// claim on the keyboard, and a presence change flickering its item list must not
// be what finally yanks the caret out of the composer.
func TestMenuFocusDoesNotClaimFocusWhenAListItNeverHeldShrinks(t *testing.T) {
	h := newMenuHarness(3, true)
	elsewhere := new(int)
	h.extra = []event.Tag{elsewhere}
	// A touch-opened menu: no open() call, so nothing is pending and nothing is
	// held. Focus sits where the user left it.
	h.frameStealingFocus(h.items, elsewhere)
	h.frame()
	if !h.router.Source().Focused(elsewhere) {
		t.Fatal("harness bug: focus was supposed to be outside the menu")
	}

	h.openFrame(h.items[:2])
	if got := h.focused(); got != -1 {
		t.Fatalf("a menu that never held focus must not claim it because its list shrank, it took item %d", got)
	}
	if !h.router.Source().Focused(elsewhere) {
		t.Fatal("focus must still be where the user left it")
	}
}

// Leaving focus alone is not the same as refusing it forever. A navigation key
// is the user asking to operate the menu they opened by finger, and the
// background is not the menu's to walk while it is up, so the first Tab pulls
// focus in rather than stepping from a position the menu does not own.
func TestMenuFocusPullsFocusInWhenANeverHeldMenuIsNavigated(t *testing.T) {
	h := newMenuHarness(3, true)
	elsewhere := new(int)
	h.extra = []event.Tag{elsewhere}
	h.frameStealingFocus(h.items, elsewhere)
	h.frame()
	if h.focused() != -1 {
		t.Fatal("harness bug: focus was supposed to be outside the menu")
	}

	h.state.trigger = h.background
	h.frame(keyTab)
	if got := h.focused(); got != 0 {
		t.Fatalf("the first navigation key must pull focus into the menu, it landed on %d", got)
	}

	// And from that moment the menu HOLDS focus like any other, so closing it
	// hands focus back to the "⋯" button rather than leaving it nowhere: Gio
	// drops the items' focus at Frame time and only a recorded hold restores it.
	h.closingFrame()
	h.closedFrame()
	if !h.router.Source().Focused(h.background) {
		t.Fatal("a menu that pulled focus in must hand it back to the trigger on close, " +
			"or the user is left with focus nowhere at all")
	}
}

// Reopening the menu re-arms first-item focus, even though the two menus share
// their widgets. Long-pressing a second bubble while the first bubble's menu is
// still up reuses the very same Reply/Copy/Delete tags, so the item holding
// focus is still in the list and every "focus is already ours" shortcut agrees
// there is nothing to do — leaving the new menu highlighted at whatever row the
// old one was left on.
func TestMenuFocusResetsToTheFirstItemWhenTheMenuIsReopened(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	h.frame(keyDown)
	if got := h.focused(); got != 1 {
		t.Fatalf("harness bug: focus was supposed to sit on item 1 of the first menu, got %d", got)
	}

	// A second bubble is long-pressed: same widgets, new subject.
	h.state.open(h.background)
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("opening a menu must start at its first item whatever the last one was left on, focus is on %d", got)
	}
	if h.state.pending {
		t.Fatal("the first-item claim must be consumed on the frame that issues it, or it re-fires later")
	}
}

// Entering a confirmation sub-view swaps the WHOLE item set for a different one.
// The click handlers that open the delete and clear-chat confirmations set their
// flag and nothing else — no reopen() — so the only thing standing between a
// keyboard user and a focusless confirmation dialog is this reclaim.
func TestMenuFocusLandsOnTheFirstItemOfASwappedSubview(t *testing.T) {
	h := newMenuHarness(5, true)
	main, sub := h.items[:3], h.items[3:]
	h.state.open(h.background)
	h.openFrame(main)
	h.openFrame(main, keyDown)
	if got := h.focused(); got != 1 {
		t.Fatalf("harness bug: focus was supposed to sit on item 1 of the main list, got %d", got)
	}

	// "Delete chat" is activated: the next frame draws the confirmation's own
	// two items instead, and not one tag from the list before it.
	h.openFrame(sub)
	if got := h.focused(); got != 3 {
		t.Fatalf("a sub-view that swapped the item set must be focused at its own first item, focus is on %d", got)
	}
	if got := len(sub); got != 2 {
		t.Fatalf("harness bug: the sub-view was supposed to have 2 items, it has %d", got)
	}
}

// The message menu acts on a COPY of the message taken when it opened, so
// nothing about it goes stale on its own — the row behind it does. An incoming
// message_delete, a peer-side wipe or a local clear removes the row while the
// overlay keeps offering actions for it: Reply would quote a message that is
// gone, focusing the composer and raising the on-screen keyboard on the way,
// only for dropStaleReply to drop the quote a frame later; Delete would dispatch
// a delete command for an ID the conversation no longer has.
func TestDropStaleMsgMenu(t *testing.T) {
	t.Parallel()

	present := []service.DirectMessage{{ID: "aaa", Body: "hello"}, {ID: "bbb", Body: "world"}}

	for _, tc := range []struct {
		name       string
		msgs       []service.DirectMessage
		cacheReady bool
		menuID     string // empty: no menu is open
		wantOpen   bool
		why        string
	}{
		{
			name: "no menu is open", msgs: present, cacheReady: true,
			why: "nothing to close, and nothing to dereference either",
		},
		{
			name: "the message is still there", msgs: present, cacheReady: true,
			menuID: "aaa", wantOpen: true,
			why: "an open menu on a live row must survive every frame it is drawn on",
		},
		{
			name: "the message was deleted", msgs: present[1:], cacheReady: true,
			menuID: "aaa",
			why:    "the row is gone: the menu offers Reply and Delete for a message that no longer exists",
		},
		{
			name: "the conversation was cleared", msgs: nil, cacheReady: true,
			menuID: "aaa",
			why: "an empty conversation leaves msgCacheByID nil, which is a miss for every ID — " +
				"a nil map must not be read as \"cannot tell\"",
		},
		{
			name: "the cache is still loading", msgs: nil, cacheReady: false,
			menuID: "aaa", wantOpen: true,
			why: "a transiently empty snapshot mid conversation-load is not a deletion, and closing " +
				"the menu under the user's finger on it would be a worse bug than the one being fixed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &Window{snap: service.RouterSnapshot{
				ActiveMessages: tc.msgs,
				CacheReady:     tc.cacheReady,
			}}
			w.rebuildMsgCache()
			if tc.menuID != "" {
				w.msgContextMsg = &service.DirectMessage{ID: tc.menuID, Body: "hello"}
			}

			w.dropStaleMsgMenu()

			if got := w.msgContextMsg != nil; got != tc.wantOpen {
				t.Fatalf("menu open = %v, want %v: %s", got, tc.wantOpen, tc.why)
			}
		})
	}
}

// The clear is worthless unless the handlers that act on msgContextMsg see it.
// Both of them — handleReplyContextClicks and, via handleActions,
// handleMsgContextMenuActions — open with a nil check and return, which is how a
// click already queued against the vanished row is discarded rather than acted
// on. Run the clear after them and that click is honoured for one more frame,
// which is the whole defect. layout() is not callable from a test, so this reads
// the source for the one thing behaviour reachable here cannot show: the order.
func TestLayoutDropsAStaleMsgMenuBeforeItsHandlers(t *testing.T) {
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

	// Every w.something(...) call in the body, in source order.
	type call struct {
		pos  token.Pos
		name string
	}
	var calls []call
	ast.Inspect(layoutFn.Body, func(n ast.Node) bool {
		ce, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		se, ok := ce.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := se.X.(*ast.Ident); ok && id.Name == "w" {
			calls = append(calls, call{ce.Pos(), se.Sel.Name})
		}
		return true
	})
	sort.Slice(calls, func(i, j int) bool { return calls[i].pos < calls[j].pos })

	first := func(name string) int {
		for i, c := range calls {
			if c.name == name {
				return i
			}
		}
		return -1
	}
	count := func(name string) int {
		n := 0
		for _, c := range calls {
			if c.name == name {
				n++
			}
		}
		return n
	}

	if n := count("dropStaleMsgMenu"); n != 1 {
		t.Fatalf("layout() calls w.dropStaleMsgMenu %d times, want exactly 1 — "+
			"the stale-menu check has one place it belongs and that is the point of it", n)
	}
	drop := first("dropStaleMsgMenu")
	for _, after := range []string{"handleReplyContextClicks", "handleActions"} {
		at := first(after)
		if at < 0 {
			t.Fatalf("layout() no longer calls w.%s — this guard is asserting an order between things that are not both there", after)
		}
		if drop > at {
			t.Errorf("w.dropStaleMsgMenu runs AFTER w.%s: a click queued against a row that has just been "+
				"deleted is acted on for one more frame, which is the bug the clear exists to close", after)
		}
	}

	// And it stays paired with the reply-side clear it mirrors: both read the
	// same cache, both want the snapshot rebuildMsgCache has just digested.
	if r := first("dropStaleReply"); r < 0 {
		t.Error("layout() no longer calls w.dropStaleReply")
	} else if rc := first("rebuildMsgCache"); rc < 0 || rc > drop {
		t.Error("w.dropStaleMsgMenu runs before w.rebuildMsgCache: it would test this frame's menu against " +
			"the PREVIOUS frame's cache and close a menu one frame after the row came back, or keep one open " +
			"a frame after it went away")
	}
}
