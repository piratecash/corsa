package desktop

import (
	"testing"

	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// menuHarness drives menuFocusState against a real input.Router, because every
// claim this contract makes is a claim about Gio's focus bookkeeping: that a
// FocusCmd for a tag the frame has not registered yet still sticks, that focus
// survives only for tags a frame both makes visible and filters for, and that a
// key event goes to the FIRST caller that filters for it. A fake queue would
// agree with whatever the code did.
type menuHarness struct {
	router *input.Router
	ops    *op.Ops
	state  menuFocusState
	items  []event.Tag
	// background stands in for anything outside the menu: the "⋯" trigger a
	// closed menu hands focus back to, and the control an escaping Tab would
	// otherwise walk into.
	background event.Tag
	// extra are further tags laid out every frame. Gio drops focus for any tag
	// a frame does not lay out, so a stand-in for "some other control has focus"
	// has to keep being drawn or the test would measure the drop, not the code.
	extra  []event.Tag
	arrows bool
	// leftover reports whether a navigation key was still readable after drive
	// ran. Gio hands each key event to the first caller whose filter matches and
	// then drops it, and app.Window runs its own focus traversal only for a Tab
	// that NOTHING handled — so a leftover Tab is a Tab that escapes the menu.
	leftover bool
	// fallback stands in for the composer: where focus goes when the trigger is
	// no longer laid out to take it back. Left nil by tests that are not about
	// that, which is also the "window has no fallback to offer" case.
	fallback event.Tag
}

func newMenuHarness(items int, arrows bool) *menuHarness {
	h := &menuHarness{
		router:     new(input.Router),
		ops:        new(op.Ops),
		background: new(int),
		arrows:     arrows,
	}
	for i := 0; i < items; i++ {
		h.items = append(h.items, new(int))
	}
	return h
}

// layoutTag mirrors what widget.Clickable does for focus purposes: it makes the
// tag visible in the frame's ops AND registers a focus filter. Gio's Frame drops
// focus unless a tag does both, so a harness that skipped either half would
// report focus loss the real widgets never suffer.
func layoutTag(gtx layout.Context, tag event.Tag) {
	event.Op(gtx.Ops, tag)
	gtx.Event(key.FocusFilter{Target: tag})
}

// openFrame runs one frame of an OPEN menu: keys arrive, drive reads them, then
// the items lay out. items may be nil, which is the deferred-draw case (the
// on-screen keyboard leaves no room, so the overlay returns before the card).
func (h *menuHarness) openFrame(items []event.Tag, keys ...key.Event) bool {
	for _, k := range keys {
		h.router.Queue(k)
	}
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	esc := h.state.drive(gtx, items, h.arrows)
	_, h.leftover = gtx.Event(
		key.Filter{Name: key.NameEscape},
		key.Filter{Name: key.NameTab, Optional: key.ModShift},
		key.Filter{Name: key.NameUpArrow},
		key.Filter{Name: key.NameDownArrow},
	)
	h.layoutRest(gtx, items)
	h.router.Frame(h.ops)
	return esc
}

// layoutRest draws the menu's items and everything standing in for the rest of
// the window.
func (h *menuHarness) layoutRest(gtx layout.Context, items []event.Tag) {
	for _, it := range items {
		layoutTag(gtx, it)
	}
	layoutTag(gtx, h.background)
	for _, t := range h.extra {
		layoutTag(gtx, t)
	}
	if h.fallback != nil {
		layoutTag(gtx, h.fallback)
	}
}

// frame is openFrame for the common case: the menu's whole item list is drawn.
func (h *menuHarness) frame(keys ...key.Event) bool {
	return h.openFrame(h.items, keys...)
}

// closingFrame is the frame a menu closes on. The close happens below layout's
// closed-menu check, so restoreOnClose does NOT run yet; the items are gone from
// the frame, which is what makes Gio drop their focus at the end of it.
func (h *menuHarness) closingFrame() {
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	h.layoutRest(gtx, nil)
	h.router.Frame(h.ops)
}

// closingFrameAfterAnOutsideTap is the frame a menu closes on when the tap that
// closed it also dismissed the touch keyboard. It runs layout's order: the menu
// is still open when the deliberate clear and its abandonRestore run near the
// top, the item's own handler closes the menu below that, and the overlay is
// therefore never reached — so the items are not laid out and drive never runs.
func (h *menuHarness) closingFrameAfterAnOutsideTap() {
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	gtx.Execute(key.FocusCmd{}) // a nil Tag empties focus, as dismissOnOutsideTap does
	h.state.abandonRestore()
	h.layoutRest(gtx, nil)
	h.router.Frame(h.ops)
}

// openFrameAfterAnOutsideTap is a frame the menu stays OPEN on although the tap
// that landed on it emptied focus: layout clears and abandons near its top, and
// the overlay below still draws the items and runs drive over them.
func (h *menuHarness) openFrameAfterAnOutsideTap() {
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	gtx.Execute(key.FocusCmd{})
	h.state.abandonRestore()
	h.state.drive(gtx, h.items, h.arrows)
	h.layoutRest(gtx, h.items)
	h.router.Frame(h.ops)
}

// closedFrame is any frame after the close: layout's check finds the menu shut
// and offers focus back to the trigger.
func (h *menuHarness) closedFrame() {
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	h.state.restoreOnClose(gtx, h.fallback)
	h.layoutRest(gtx, nil)
	h.router.Frame(h.ops)
}

// frameStealingFocus is an ordinary frame in which some handler outside the menu
// takes focus for itself — a Reply item focusing the composer, or the user
// clicking a background control. It runs drive first, exactly as the overlay
// does, so the menu's key filters stay registered: Gio matches an incoming key
// against the PREVIOUS frame's filters, so a frame that skipped drive would make
// the next frame's key undeliverable for a reason that has nothing to do with
// the code under test.
func (h *menuHarness) frameStealingFocus(items []event.Tag, steal event.Tag) {
	h.ops.Reset()
	gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
	h.state.drive(gtx, items, h.arrows)
	gtx.Execute(key.FocusCmd{Tag: steal})
	h.layoutRest(gtx, items)
	h.router.Frame(h.ops)
}

// focused reports the index of the focused item, or -1 when focus is anywhere
// else (including nowhere).
func (h *menuHarness) focused() int {
	src := h.router.Source()
	for i, it := range h.items {
		if src.Focused(it) {
			return i
		}
	}
	return -1
}

func menuKey(name key.Name, mods key.Modifiers) key.Event {
	return key.Event{Name: name, Modifiers: mods, State: key.Press}
}

var (
	keyTab      = menuKey(key.NameTab, 0)
	keyShiftTab = menuKey(key.NameTab, key.ModShift)
	keyEscape   = menuKey(key.NameEscape, 0)
	keyDown     = menuKey(key.NameDownArrow, 0)
	keyUp       = menuKey(key.NameUpArrow, 0)
)

// Opening a menu must put focus INSIDE it. This is the whole defect: openMenu
// used to focus the message composer sitting under the overlay, so a keyboard
// or Narrator user held focus on a widget they could not see, and the next
// Enter sent the draft instead of activating a menu item.
func TestMenuFocusClaimsFirstItemOnOpen(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("opening a menu must focus its first item, focus landed on %d", got)
	}
	// And it claims focus exactly once: a menu the user has since Tabbed away
	// from must not be yanked back to the top every frame.
	h.frame(keyTab)
	h.frame()
	if got := h.focused(); got != 1 {
		t.Fatalf("first-item focus must be a one-shot on open, not re-applied every frame; focus is on %d", got)
	}
}

// Tab and Shift+Tab cycle within the menu and wrap. Wrapping is the point: the
// alternative is walking off the end into the background controls, which is
// exactly what an open menu must not allow.
func TestMenuFocusTabCyclesAndWraps(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	for _, step := range []struct {
		k    key.Event
		want int
		why  string
	}{
		{keyTab, 1, "Tab moves to the next item"},
		{keyTab, 2, "Tab moves to the next item"},
		{keyTab, 0, "Tab past the last item wraps to the first, it does not leave the menu"},
		{keyShiftTab, 2, "Shift+Tab before the first item wraps to the last"},
		{keyShiftTab, 1, "Shift+Tab moves to the previous item"},
	} {
		h.frame(step.k)
		if got := h.focused(); got != step.want {
			t.Fatalf("%s: want item %d, got %d", step.why, step.want, got)
		}
	}
}

// The menu consumes the navigation keys it acts on. That consumption IS the
// containment: Gio only runs its built-in focus traversal for a Tab that no
// handler took, so leaving the event behind would move focus twice — once
// inside the menu, once into the background.
func TestMenuFocusConsumesTabSoGioDoesNotAlsoTraverse(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	for _, k := range []key.Event{keyTab, keyShiftTab, keyDown, keyUp, keyEscape} {
		h.frame(k)
		if h.leftover {
			t.Fatalf("%v was still readable after drive: Gio would run its own focus traversal on it and walk out of the menu", k.Name)
		}
	}
}

// Control for the test above: the probe it relies on does see an unconsumed key,
// so a passing containment test means drive took the event, not that the harness
// never delivered one.
func TestMenuHarnessProbeSeesAnUnconsumedTab(t *testing.T) {
	h := newMenuHarness(3, true)
	probe := func() bool {
		h.ops.Reset()
		gtx := layout.Context{Ops: h.ops, Source: h.router.Source()}
		_, ok := gtx.Event(key.Filter{Name: key.NameTab, Optional: key.ModShift})
		h.layoutRest(gtx, h.items)
		h.router.Frame(h.ops)
		return ok
	}
	// The first pass only registers the filter: Gio matches an arriving key
	// against the filters of the frame before it.
	probe()
	h.router.Queue(keyTab)
	if !probe() {
		t.Fatal("the harness never delivered a Tab, so the containment test above proves nothing")
	}
}

// Focus that has drifted outside an open menu is pulled back in rather than
// stepped from. The background is not the menu's to walk while it is up.
func TestMenuFocusPullsBackWhenFocusDriftedOut(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	h.frame(keyTab)
	h.frameStealingFocus(h.items, h.background)
	if h.focused() != -1 {
		t.Fatal("harness bug: focus was supposed to leave the menu")
	}
	h.frame(keyTab)
	if got := h.focused(); got != 0 {
		t.Fatalf("navigating while focus sits outside the menu must pull it back to the first item, got %d", got)
	}
}

// Up/Down navigate a plain item list, but must be left alone while the menu
// embeds a text editor: the alias editor needs the arrow keys for its caret.
func TestMenuFocusArrowsYieldToAnEmbeddedEditor(t *testing.T) {
	with := newMenuHarness(3, true)
	with.state.open(with.background)
	with.frame()
	with.frame(keyDown)
	if got := with.focused(); got != 1 {
		t.Fatalf("Down must move to the next item in a plain item list, got %d", got)
	}
	with.frame(keyUp)
	if got := with.focused(); got != 0 {
		t.Fatalf("Up must move to the previous item in a plain item list, got %d", got)
	}

	without := newMenuHarness(3, false)
	without.state.open(without.background)
	without.frame()
	without.frame(keyDown)
	if got := without.focused(); got != 0 {
		t.Fatalf("Down belongs to the alias editor's caret while it is on screen, focus must not move; it went to %d", got)
	}
	if !without.leftover {
		t.Fatal("Down must be left in the queue for the editor to read when the menu does not navigate on arrows")
	}
}

// Escape is reported to the caller, which decides what it means. Key releases
// are not key presses: a menu opened by an Enter whose release is still in
// flight must not read that release as navigation.
func TestMenuFocusReportsEscapeAndIgnoresReleases(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	if !h.frame(keyEscape) {
		t.Fatal("Escape must be reported so the caller can back out of a sub-view or close the menu")
	}
	if h.frame() {
		t.Fatal("Escape must be reported on the frame it arrives, not latched")
	}
	h.frame(key.Event{Name: key.NameTab, State: key.Release})
	if got := h.focused(); got != 0 {
		t.Fatalf("a key RELEASE must not navigate, focus moved to %d", got)
	}
}

// The deferred-draw path is the one this contract most easily loses: when the
// on-screen keyboard leaves no room the overlay returns before laying out a
// single item. Escape has to keep working there — a keyboard user is otherwise
// stuck with a menu they can neither see nor dismiss — and the pending
// first-item focus has to survive for the frame the room comes back.
func TestMenuFocusSurvivesADeferredDraw(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	// One deferred frame to register the filters — nothing, in Gio or anywhere
	// else, can receive a key pressed before it existed.
	h.openFrame(nil)
	if !h.openFrame(nil, keyEscape) {
		t.Fatal("Escape must be reported even while the menu's draw is deferred for want of room")
	}
	if got := h.focused(); got != -1 {
		t.Fatalf("no item is laid out during a deferred draw, so none can hold focus; got %d", got)
	}
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("the room came back: the pending first-item focus must still be honoured, got %d", got)
	}
}

// The same path taken by a menu that had ALREADY claimed focus. This is the
// harder half and the one the first fix missed: the keyboard animating up
// takes the room away MID-menu, the overlay defers, Gio drops the focus of
// every item it did not lay out, and the menu is left visible-but-dead — Tab
// walking the background, Enter reaching whatever is under the overlay. The
// deferred frame has to re-arm the claim it just lost, not forget it.
func TestMenuFocusReclaimedAfterADeferredDrawInterruptsIt(t *testing.T) {
	h := newMenuHarness(3, true)
	h.state.open(h.background)
	h.frame()
	h.frame(keyTab)
	if got := h.focused(); got != 1 {
		t.Fatalf("harness bug: the menu was supposed to hold focus on item 1 before the keyboard took the room, got %d", got)
	}

	// The keyboard comes up: two frames with no room for a single item.
	h.openFrame(nil)
	h.openFrame(nil)
	if got := h.focused(); got != -1 {
		t.Fatalf("nothing is laid out during a deferred draw, so nothing can hold focus; got %d", got)
	}

	// The room comes back.
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("a menu that held focus must take it back when it can draw again, focus landed on %d — "+
			"a visible menu with focus on the background is the defect this file exists to prevent", got)
	}
}

// Closing while deferred still returns focus to the "⋯" button. Without the
// re-arm the deferred frame forgets the menu ever held focus, and the close
// then leaves focus wherever Gio dropped it — which is nowhere.
func TestMenuFocusReturnsToTriggerAfterClosingWhileDeferred(t *testing.T) {
	h := newMenuHarness(3, true)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	if h.focused() != 0 {
		t.Fatal("harness bug: the menu was supposed to hold focus before the room went away")
	}
	h.openFrame(nil) // the keyboard takes the room
	h.closingFrame() // and the user dismisses the menu while it is deferred
	h.closedFrame()
	if !h.router.Source().Focused(trigger) {
		t.Fatal("a menu closed while its draw was deferred must still hand focus back to the trigger")
	}
}

// And the re-arm is only for a menu that HELD focus. One the user opened by
// finger has no claim on the keyboard: yanking focus into it the moment the
// on-screen keyboard finishes animating is the opposite of what they asked for,
// and it would fire on every menu opened by touch.
func TestMenuFocusDeferredDrawDoesNotGrabFocusItNeverHad(t *testing.T) {
	h := newMenuHarness(3, true)
	elsewhere := new(int)
	h.extra = []event.Tag{elsewhere}
	// A touch-opened menu: no open() call, so nothing is pending and nothing
	// is held. Focus sits where the user left it.
	h.frameStealingFocus(h.items, elsewhere)
	h.frame()
	if !h.router.Source().Focused(elsewhere) {
		t.Fatal("harness bug: focus was supposed to be outside the menu")
	}

	h.openFrame(nil) // the keyboard takes the room away
	h.frame()        // and gives it back
	if got := h.focused(); got != -1 {
		t.Fatalf("a menu that never held focus must not claim it when the room returns, it took item %d", got)
	}
}

// A menu that held focus hands it back to the "⋯" button that opened it, so the
// user resumes where they left off instead of at the top of the window.
func TestMenuFocusReturnsToTriggerOnClose(t *testing.T) {
	h := newMenuHarness(3, true)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	h.frame(keyTab)
	if h.focused() != 1 {
		t.Fatal("harness bug: the menu was supposed to hold focus before closing")
	}
	h.closingFrame()
	h.closedFrame()
	if !h.router.Source().Focused(trigger) {
		t.Fatal("closing a menu that held focus must return focus to the trigger that opened it")
	}
	// And only once: a later frame must not keep re-focusing the trigger and
	// fighting whatever the user has moved on to.
	h.frameStealingFocus(nil, new(int))
	h.state.trigger = new(int)
	h.closedFrame()
	if h.router.Source().Focused(h.state.trigger) {
		t.Fatal("the restore is a one-shot; it must not re-fire on every closed frame")
	}
}

// A menu the user never keyboard-touched leaves focus exactly where it was.
// Right-clicking a bubble while typing must not move focus out of the composer.
func TestMenuFocusLeavesFocusAloneWhenItNeverHeldIt(t *testing.T) {
	h := newMenuHarness(3, true)
	other := new(int)
	h.extra = []event.Tag{other}
	h.state.trigger = h.background
	h.state.held = false
	h.frameStealingFocus(nil, other)

	h.closedFrame()
	if !h.router.Source().Focused(other) {
		t.Fatal("a menu that never held focus must not move it on close")
	}
}

// When another handler deliberately claims focus as the menu closes — Reply
// focuses the composer, confirming a delete does too — the restore stands down
// rather than yanking focus back to the "⋯" button.
func TestMenuFocusYieldsToAHandlerThatClaimedFocus(t *testing.T) {
	h := newMenuHarness(3, true)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	claimed := new(int)
	h.extra = []event.Tag{claimed}

	// The closing frame: the menu is gone and something else takes focus.
	h.frameStealingFocus(nil, claimed)

	for i := 0; i < 4; i++ {
		h.closedFrame()
		if !h.router.Source().Focused(claimed) {
			t.Fatalf("frame %d: the restore must yield to a handler that already claimed focus", i)
		}
	}
}

// A close that lands mid-frame keeps the menu's focus until the end of that
// frame, because Gio drops focus for an unlaid tag in Router.Frame. The restore
// has to wait that out instead of reading it as "somebody else has focus" and
// standing down for good.
func TestMenuFocusWaitsOutTheFrameItsOwnFocusIsDroppedIn(t *testing.T) {
	h := newMenuHarness(3, true)
	trigger := h.background
	h.state.open(trigger)
	h.frame()
	if h.focused() != 0 {
		t.Fatal("harness bug: the menu was supposed to hold focus")
	}
	// restoreOnClose runs while the menu's own item is still the focus owner,
	// on the very frame that stops drawing it.
	h.closedFrame()
	h.closedFrame()
	if !h.router.Source().Focused(trigger) {
		t.Fatal("the restore must survive the frame in which the menu's own focus is still being dropped")
	}
}

// The item lists must describe the sub-view that is actually on screen, in draw
// order, with the same precedence contextMenuCard uses. A list that describes a
// different sub-view would focus a widget the user cannot see.
func TestPeerMenuItemsFollowTheVisibleSubview(t *testing.T) {
	w := &Window{}
	base := []event.Tag{&w.ctxMenuAlias, &w.ctxMenuCopy, &w.ctxMenuDelete}
	for _, tc := range []struct {
		name  string
		setup func()
		want  []event.Tag
	}{
		{"item list", func() {}, base},
		{
			"delete confirmation wins over everything",
			func() { w.showDeleteConfirm, w.showClearChatConfirm, w.showAliasEditor = true, true, true },
			[]event.Tag{&w.ctxMenuDeleteConfirm, &w.ctxMenuDeleteCancel},
		},
		{
			"clear-chat confirmation wins over the alias editor",
			func() { w.showClearChatConfirm, w.showAliasEditor = true, true },
			[]event.Tag{&w.ctxMenuClearChatConfirm, &w.ctxMenuClearChatCancel},
		},
		{
			"alias editor comes first in its own sub-view: it is where the alias is typed",
			func() { w.showAliasEditor = true },
			[]event.Tag{&w.aliasEditor, &w.ctxMenuAliasSave, &w.ctxMenuAliasCancel},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w.showDeleteConfirm, w.showClearChatConfirm, w.showAliasEditor = false, false, false
			tc.setup()
			got := w.peerMenuItems()
			if len(got) != len(tc.want) {
				t.Fatalf("want %d items, got %d", len(tc.want), len(got))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("item %d is not the widget the card draws there", i)
				}
			}
		})
	}
}

// "Clear chat" is only drawn as a real, clickable row for an online peer;
// offline it is contextMenuItemDisabled, a bare label with no Clickable and so
// no focus target at all. Focusing it would strand Tab on a widget that cannot
// be activated and cannot be seen to be focused.
func TestMenuItemsExcludeRowsThatHaveNoFocusTarget(t *testing.T) {
	w := &Window{contextMenuPeer: domain.PeerIdentity{}}
	for _, it := range w.peerMenuItems() {
		if it == &w.ctxMenuClearChat {
			t.Fatal("the offline peer's Clear chat row is a disabled label with no Clickable; it must not be in the focus ring")
		}
	}
	// Same rule for the message menu's Delete row.
	if w.contextMenuDeleteEnabled() {
		t.Fatal("harness bug: Delete was supposed to be disabled here")
	}
	got := w.msgMenuItems()
	want := []event.Tag{&w.msgCtxReply, &w.msgCtxCopy}
	if len(got) != len(want) {
		t.Fatalf("a disabled Delete row owns no focus target and must be absent: want %d items, got %d", len(want), len(got))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("message menu item %d is not the widget the card draws there", i)
		}
	}
}

// Escape steps back out of a confirmation or the alias editor rather than
// closing the menu outright — the same thing that sub-view's own Cancel does —
// and only closes once there is nothing left to step back into. Each step back
// re-arms first-item focus, because the widget that had it is about to leave the
// frame with the sub-view.
func TestEscapeStepsBackOutOfSubviewsBeforeClosing(t *testing.T) {
	// contextMenuPeer IS the open flag of the peer menu, so this identity has to
	// be a real one: a zero peer is a menu that was already shut before Escape,
	// and every claim below about the menu surviving would hold vacuously.
	peer := domain.PeerIdentity{1}
	w := &Window{contextMenuPeer: peer}
	for _, tc := range []struct {
		name  string
		setup func()
		open  func() bool
	}{
		{"delete confirmation", func() { w.showDeleteConfirm = true }, func() bool { return w.showDeleteConfirm }},
		{"clear-chat confirmation", func() { w.showClearChatConfirm = true }, func() bool { return w.showClearChatConfirm }},
		{"alias editor", func() { w.showAliasEditor = true }, func() bool { return w.showAliasEditor }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w.contextMenuPeer = peer
			w.showDeleteConfirm, w.showClearChatConfirm, w.showAliasEditor = false, false, false
			w.peerMenuFocus = menuFocusState{}
			w.ctxMenuList.Position = layout.Position{First: 3, Offset: 17}
			w.lastCtxMenuMode = 9
			tc.setup()

			w.escapePeerMenu()

			if tc.open() {
				t.Fatal("Escape must close the sub-view it is showing")
			}
			if w.contextMenuPeer.IsZero() {
				t.Fatal("Escape closed the whole menu instead of stepping back into its item list: " +
					"stepping out of a sub-view is what that sub-view's own Cancel does, and Cancel " +
					"leaves the menu up")
			}
			if !w.peerMenuFocus.pending {
				t.Fatal("stepping back swaps the whole item set; the item list that comes back must take focus")
			}
			if w.lastCtxMenuMode != 0 || w.ctxMenuList.Position != (layout.Position{}) {
				t.Fatal("the restored item list must start at the top, not at the sub-view's scroll offset")
			}
		})
	}
}

// With no sub-view showing, Escape closes the menu. The message menu has no
// sub-views at all, so Escape always closes it.
func TestEscapeClosesAMenuWithNothingToStepBackInto(t *testing.T) {
	w := &Window{}
	w.contextMenuPeer = domain.PeerIdentity{1}
	w.escapePeerMenu()
	if !w.contextMenuPeer.IsZero() {
		t.Fatal("Escape on the item list must close the identity menu")
	}

	msg := service.DirectMessage{ID: "m1"}
	w.msgContextMsg = &msg
	w.escapeMsgMenu()
	if w.msgContextMsg != nil {
		t.Fatal("Escape must close the message menu, which has no sub-views to step back into")
	}
}
