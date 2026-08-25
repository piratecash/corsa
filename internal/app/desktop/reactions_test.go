package desktop

import (
	"context"
	"fmt"
	"image"
	"slices"
	"strings"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// reactionHarness drives the REAL message overlay, because everything claimed
// below is a claim about where the pill lands and what the router delivers to
// it — neither of which a unit test on the component can answer.
type reactionHarness struct {
	w      *Window
	router *input.Router
	ops    *op.Ops
	width  int
	height int
	// under stands in for the chat list the overlay covers: it asks for scroll
	// over the whole window, the way material.List does, and scrolled records
	// whether any reached it.
	under    event.Tag
	scrolled bool
}

func newReactionHarness(t *testing.T, height int) *reactionHarness {
	t.Helper()
	w := &Window{theme: newAppTheme(), language: "en"}
	w.msgCtxMenuList.Axis = layout.Vertical
	w.msgContextMsg = &service.DirectMessage{ID: "m1", Body: "hi"}
	w.msgContextPos = image.Pt(10, 10)
	w.msgMenuFocus.open(new(int))
	return &reactionHarness{w: w, router: new(input.Router), ops: new(op.Ops), width: 400, height: height}
}

func (h *reactionHarness) frame(events ...event.Event) {
	for _, ev := range events {
		h.router.Queue(ev)
	}
	h.ops.Reset()
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(h.width, h.height)},
	}
	if h.under != nil {
		area := clip.Rect{Max: gtx.Constraints.Max}.Push(gtx.Ops)
		event.Op(gtx.Ops, h.under)
		area.Pop()
		for {
			ev, ok := gtx.Event(pointer.Filter{
				Target:  h.under,
				Kinds:   pointer.Scroll,
				ScrollY: pointer.ScrollRange{Min: -1e6, Max: 1e6},
			})
			if !ok {
				break
			}
			if pe, ok := ev.(pointer.Event); ok && pe.Kind == pointer.Scroll {
				h.scrolled = true
			}
		}
	}
	if h.w.msgContextMsg != nil {
		h.w.layoutMsgContextMenuOverlay(gtx)
	}
	h.router.Frame(h.ops)
}

// frameCtx is a context on the CURRENT router state, for asking about focus
// between frames without laying anything out.
func (h *reactionHarness) frameCtx() layout.Context {
	return layout.Context{
		Ops:         new(op.Ops),
		Source:      h.router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(h.width, h.height)},
	}
}

func (h *reactionHarness) tap(at f32.Point) {
	h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
	h.frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})
}

// The pill is anchored where the gesture happened and sits ABOVE the menu card,
// so a tap at the anchor plus half a slot lands on the first quick reaction.
// That is the whole placement contract in one gesture: drawn, positioned, on
// top, and reachable.
func TestQuickReactionIsWhereTheGestureLeftIt(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()
	if !h.w.reactionRow.shown {
		t.Fatal("a full-height window did not draw the reaction pill")
	}

	slot := int(ui.ReactionSlotSideDp)
	first := f32.Pt(float32(10+5+slot/2), float32(10+5+slot/2))

	// The press and the release are read on separate frames on purpose, and it
	// is what tells the pill from the backdrop underneath it. The backdrop
	// dismisses on the PRESS; a slot is a widget.Clickable and fires on the
	// RELEASE. A test that only looked at the end state would pass just as well
	// with no pill drawn at all.
	h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: first})
	if h.w.msgContextMsg == nil {
		t.Fatalf("the press at %v fell through to the backdrop: no slot was drawn there", first)
	}

	h.frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: first})
	h.frame()
	if h.w.msgContextMsg != nil {
		t.Fatalf("a tap at %v did not reach the first quick reaction: the overlay is still open", first)
	}

	// The control for the paragraph above: away from the pill, the same press
	// dismisses on the spot. Without this the press assertion would pass on a
	// backdrop that simply ignored presses.
	control := newReactionHarness(t, 800)
	control.frame()
	control.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: f32.Pt(390, 700)})
	if control.w.msgContextMsg != nil {
		t.Fatal("a press on bare backdrop did not dismiss, so the press assertion above proves nothing")
	}
}

// Chips are drawn from the cache this window keeps, never from a read inside a
// frame: a chat view lays out dozens of bubbles per frame and a database call
// per bubble is a call in the wrong place.
func TestChipsAreDrawnFromTheLoadedConversation(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("11", 20))
	other := domain.PeerIdentityFromWire(strings.Repeat("22", 20))
	w := &Window{msgReactionState: map[domain.MessageID][]domain.Reaction{
		"m1": {
			{Emoji: "👍", Actors: []domain.PeerIdentity{self, other}, Mine: true},
			{Emoji: "🔥", Actors: []domain.PeerIdentity{other}},
		},
	}}

	got := w.messageReactions(service.DirectMessage{ID: "m1"})
	if len(got) != 2 {
		t.Fatalf("drew %d chips, want 2: %#v", len(got), got)
	}
	if got[0].Emoji != "👍" || got[0].Count != 2 || !got[0].Mine {
		t.Fatalf("first chip = %#v, want 👍 counted twice and marked as mine", got[0])
	}
	if got[1].Mine {
		t.Fatal("a reaction this user did not make is marked as theirs")
	}
	if w.messageReactions(service.DirectMessage{ID: "m2"}) != nil {
		t.Fatal("a message with no reactions was given a chip row")
	}
}

// A window with no router behind it — every test in this package builds one,
// and so does the first frame after launch — must still answer a tap by closing
// the surfaces rather than by panicking on a nil dependency.
func TestReactingWithoutARouterClosesTheSurfacesAnyway(t *testing.T) {
	message := service.DirectMessage{ID: "m1", Body: "hi"}
	w := &Window{}
	w.msgContextMsg = &message

	w.applyReaction("🔥")

	if w.msgContextMsg != nil {
		t.Fatal("choosing a reaction left the message menu open")
	}
	if got := w.messageReactions(message); len(got) != 0 {
		t.Fatalf("a reaction was recorded with nowhere to record it: %#v", got)
	}
}

// "More" swaps the pill for the full panel, and Escape steps back to the pill
// rather than closing the message menu outright — the same rule the identity
// menu applies to its own sub-views.
func TestMoreOpensThePanelAndEscapeStepsBackToTheRow(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()

	slot := int(ui.ReactionSlotSideDp)
	gap := 5
	more := f32.Pt(float32(10+5+7*(slot+gap)+slot/2), float32(10+5+slot/2))
	h.tap(more)

	if !h.w.reactionPickerOpen() {
		t.Fatalf("a tap at %v on the more button did not open the emoji panel", more)
	}
	if h.w.msgContextMsg == nil {
		t.Fatal("opening the panel closed the menu it belongs to")
	}

	h.frame(key.Event{Name: key.NameEscape, State: key.Press})
	if h.w.reactionPickerOpen() {
		t.Fatal("Escape left the emoji panel open")
	}
	if h.w.msgContextMsg == nil {
		t.Fatal("Escape closed the whole menu instead of stepping back to the pill")
	}

	h.frame(key.Event{Name: key.NameEscape, State: key.Press})
	if h.w.msgContextMsg != nil {
		t.Fatal("a second Escape with no sub-view left did not close the menu")
	}
}

// The panel belongs to one OPENING of the menu. Nothing clears the flag when
// that menu closes — by design, since nine paths close it — so the flag has to
// stop meaning anything by itself.
//
// Keying it by message ID was the first cut and failed on the one case it
// mattered for: pressing the backdrop clears msgContextMsg and leaves the flag,
// so reopening the menu on THAT SAME message came back up as a panel, on the
// query the user had just walked away from. openMsgMenu stores a fresh copy of
// the message each time, so a pointer is the menu's identity and every close
// invalidates it.
func TestThePanelDoesNotOutliveTheMenuItBelongsTo(t *testing.T) {
	message := service.DirectMessage{ID: "m1"}
	open := func(w *Window) {
		copied := message
		w.msgContextMsg = &copied
	}

	t.Run("another message", func(t *testing.T) {
		w := &Window{}
		open(w)
		w.openReactionPicker()
		if !w.reactionPickerOpen() {
			t.Fatal("the panel did not open on the menu it was asked for")
		}

		other := service.DirectMessage{ID: "m2"}
		w.msgContextMsg = &other
		if w.reactionPickerOpen() {
			t.Fatal("the panel followed the menu to a different message")
		}
	})

	t.Run("the same message, after the backdrop closed it", func(t *testing.T) {
		w := &Window{}
		open(w)
		w.openReactionPicker()
		w.reactionRow.panel.Search.SetText("pizza")

		// What the backdrop handler does, and what every other close does too.
		w.msgContextMsg = nil

		open(w)
		if w.reactionPickerOpen() {
			t.Fatal("reopening the same message came straight back up as the emoji panel")
		}

		// And pressing "more" again starts the panel clean rather than resuming
		// the abandoned search — which is only true because the reopen went
		// through openReactionPicker instead of skipping it.
		w.openReactionPicker()
		if query := w.reactionRow.panel.Search.Text(); query != "" {
			t.Fatalf("the reopened panel resumed the abandoned query %q", query)
		}
	})
}

// The two panels can be on screen at once — a message menu opens over a
// composer whose picker is already up — so they cannot share a state. Sharing
// one aliased their search fields and their grid buttons onto each other, and
// handleEmojiActions drains the composer's clicks BEFORE the overlay is laid
// out: a tap meant as a reaction was inserted into the draft instead.
func TestTheTwoEmojiPanelsDoNotShareAState(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	w.msgContextMsg = &service.DirectMessage{ID: "m1"}
	w.emojiPicker.visible = true
	w.emojiPicker.panel.Search.SetText("pizza")

	w.openReactionPicker()

	if got := w.reactionRow.panel.Search.Text(); got != "" {
		t.Fatalf("the reaction panel opened on the composer's query %q", got)
	}
	if got := w.emojiPicker.query(); got != "pizza" {
		t.Fatalf("opening the reaction panel cleared the composer's query, leaving %q", got)
	}
	if &w.reactionRow.panel == &w.emojiPicker.panel {
		t.Fatal("both panels are the same state")
	}
	if got := emojiChoicesFor(&w.reactionRow.panel, true, w.emojiPicker.recents); len(got) == 1 {
		t.Fatal("the reaction panel's grid is showing the composer's single search match")
	}
}

// The reaction panel's search field is an editor like any other. Left out of
// the list, its focus read as "no editor focused", and the touch-keyboard
// tracker asked the keyboard this window had raised to come down 400ms after
// the user tapped into the field — on a Windows tablet, mid-word.
func TestEveryEditorCountsAsAnEditor(t *testing.T) {
	w := &Window{}
	want := map[event.Tag]string{
		&w.messageEditor:            "the composer",
		&w.identitySearchEditor:     "the identity search",
		&w.aliasEditor:              "the alias editor",
		&w.emojiPicker.panel.Search: "the composer picker's search",
		&w.reactionRow.panel.Search: "the reaction panel's search",
	}

	got := w.editorTags()
	if len(got) != len(want) {
		t.Fatalf("the window lists %d editors, want %d", len(got), len(want))
	}
	for _, tag := range got {
		delete(want, tag)
	}
	for _, missing := range want {
		t.Fatalf("%s is not counted as an editor: the keyboard will close under it", missing)
	}

	// And the focus check reads that list, rather than a condition of its own
	// that could drift away from it.
	router := new(input.Router)
	ops := new(op.Ops)
	frame := func(focus event.Tag) layout.Context {
		ops.Reset()
		gtx := layout.Context{
			Ops:         ops,
			Source:      router.Source(),
			Constraints: layout.Constraints{Max: image.Pt(400, 400)},
		}
		if focus != nil {
			gtx.Execute(key.FocusCmd{Tag: focus})
			event.Op(ops, focus)
			gtx.Event(key.FocusFilter{Target: focus})
		}
		router.Frame(ops)
		return gtx
	}

	if w.anyEditorFocused(frame(nil)) {
		t.Fatal("nothing is focused, yet the window reports an editor is")
	}
	frame(&w.reactionRow.panel.Search)
	if !w.anyEditorFocused(frame(&w.reactionRow.panel.Search)) {
		t.Fatal("focus in the reaction panel's search does not count as an editor")
	}
}

// A floating surface has to be in the way of the backdrop behind it. Only its
// interactive widgets register for input, so without a catch-all a press on its
// padding, its header or the gap between two blocks falls straight through to
// the backdrop — whose job is to dismiss. The design closes on a press BEHIND
// the popup, not on one inside it.
func TestPressingBlankSurfaceDoesNotDismiss(t *testing.T) {
	slot := int(ui.ReactionSlotSideDp)
	gap := 5

	t.Run("the pill's padding", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		h.frame()

		// Two pixels inside the pill's top-left corner: its 5dp padding, which
		// belongs to no slot.
		at := f32.Pt(12, 12)
		h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		if h.w.msgContextMsg == nil {
			t.Fatalf("a press at %v on the pill's own padding closed the overlay", at)
		}
	})

	t.Run("the panel's header", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		h.frame()
		more := f32.Pt(float32(10+5+7*(slot+gap)+slot/2), float32(10+5+slot/2))
		h.tap(more)
		if !h.w.reactionPickerOpen() {
			t.Fatal("precondition: the panel did not open")
		}

		// Left of the close button, on the header's title row.
		at := f32.Pt(120, float32(10+1+8+10))
		h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		if h.w.msgContextMsg == nil {
			t.Fatalf("a press at %v on the panel's own header closed the overlay", at)
		}
		if !h.w.reactionPickerOpen() {
			t.Fatalf("a press at %v on the panel's own header closed the panel", at)
		}
	})
}

// A keyboard user who opened the menu could reach the seven quick reactions and
// nothing else: the panel's ring was its search field and close button only, and
// the grid answered no key at all.
//
// The grid is one ring stop — the cell the cursor is on — and the arrows move
// within it. Enter needs no wiring: a focused widget.Clickable activates itself.
func TestTheFullPickerIsReachableFromTheKeyboard(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()

	slot := int(ui.ReactionSlotSideDp)
	more := f32.Pt(float32(10+5+7*(slot+5)+slot/2), float32(10+5+slot/2))
	h.tap(more)
	if !h.w.reactionPickerOpen() {
		t.Fatal("precondition: the panel did not open")
	}

	panel := &h.w.reactionRow.panel
	tab := key.Event{Name: key.NameTab, State: key.Press}
	// Each key needs its own frame: focus moves at Frame time.
	tabs := func(n int) {
		for range n {
			h.frame(tab)
		}
		h.frame()
	}

	h.frame()
	if !h.router.Source().Focused(&panel.Search) {
		t.Fatal("the panel did not open with focus in its search field")
	}

	// The ring is search, close, the nine category chips, then the grid's one
	// stop. Tabbing to the end of it must land in the grid.
	tabs(len(h.w.msgMenuItems()) - 1)
	if !panel.ChoiceFocused(h.frameCtx()) {
		t.Fatalf("Tabbing the whole ring never reached the grid (cursor %d)", panel.Cursor())
	}

	// Arrows walk the grid. Right by one, then down by a row.
	press := func(name key.Name) { h.frame(key.Event{Name: name, State: key.Press}); h.frame() }
	press(key.NameRightArrow)
	if got := panel.Cursor(); got != 1 {
		t.Fatalf("Right moved the cursor to %d, want 1", got)
	}
	before := panel.Cursor()
	press(key.NameDownArrow)
	if got := panel.Cursor(); got <= before+1 {
		t.Fatalf("Down moved the cursor to %d, want a whole row past %d", got, before)
	}
	if !panel.ChoiceFocused(h.frameCtx()) {
		t.Fatal("the cursor moved but the keyboard focus did not follow it")
	}

	// And Enter on the focused cell picks that reaction, which closes the
	// overlay — the same outcome as tapping it.
	h.frame(key.Event{Name: key.NameReturn, State: key.Press})
	h.frame(key.Event{Name: key.NameReturn, State: key.Release})
	h.frame()
	if h.w.msgContextMsg != nil {
		t.Fatal("Enter on the focused grid cell did not pick a reaction")
	}
}

// Nine categories are few enough to Tab through, so they go in the ring whole.
// Without them a keyboard user was stuck in whichever category the panel opened
// on: search finds an emoji by name, but nothing reaches animals, food or flags
// by browsing.
func TestCategoriesAreReachableFromTheKeyboard(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()
	slot := int(ui.ReactionSlotSideDp)
	h.tap(f32.Pt(float32(10+5+7*(slot+5)+slot/2), float32(10+5+slot/2)))
	if !h.w.reactionPickerOpen() {
		t.Fatal("precondition: the panel did not open")
	}

	// One frame for the ring to claim focus. A Tab arriving on the frame that
	// CLAIMS focus is swallowed by design (see drive's pending branch), so
	// counting Tabs before that has happened is off by one.
	h.frame()
	panel := &h.w.reactionRow.panel
	if !h.router.Source().Focused(&panel.Search) {
		t.Fatal("the panel did not open with focus in its search field")
	}
	chips := panel.CategoryTags(h.w.emojiPickerCategories())
	if len(chips) != len(emojiCategoryOrder) {
		t.Fatalf("the ring offers %d category chips, want all %d", len(chips), len(emojiCategoryOrder))
	}

	ring := h.w.msgMenuItems()
	found := 0
	for _, item := range ring {
		for _, chip := range chips {
			if item == chip {
				found++
			}
		}
	}
	if found != len(chips) {
		t.Fatalf("the focus ring lists %d of the %d category chips", found, len(chips))
	}

	// And activating one really switches the grid. The ring opens on the search
	// field, so Tabbing to the animals chip's position in it lands on that chip.
	animals := chips[slices.Index(emojiCategoryOrder, emojiCategoryAnimals)]
	steps := slices.Index(ring, animals)
	if steps < 1 {
		t.Fatalf("the animals chip is at ring position %d, which is not reachable by Tab", steps)
	}
	for range steps {
		h.frame(key.Event{Name: key.NameTab, State: key.Press})
	}
	h.frame(key.Event{Name: key.NameReturn, State: key.Press})
	h.frame(key.Event{Name: key.NameReturn, State: key.Release})
	h.frame()

	if got := panel.Category(); got != string(emojiCategoryAnimals) {
		t.Fatalf("Enter on the animals chip left the panel on %q", got)
	}
}

// The pill is a fixed 365dp of seven slots, and 365dp does not fit every phone.
// On a 320dp screen the "more" button was drawn entirely past the right edge —
// placeMenu can only clamp a block that FITS — and the focus ring went on
// listing it, so Tab walked to a button nobody could see.
//
// The row now takes as many quick choices as the width holds, and the ring is
// built from that same set.
func TestNarrowWindowDropsQuickSlotsRatherThanDrawingThemOffscreen(t *testing.T) {
	const windowW = 320
	h := newReactionHarness(t, 800)
	h.width = windowW
	h.frame()

	if !h.w.reactionRow.shown {
		t.Fatal("a 320dp window drew no pill at all; it has room for a shorter one")
	}
	quick := h.w.reactionRow.quick
	if len(quick) >= len(defaultQuickReactions) {
		t.Fatalf("the pill kept all %d slots on a %ddp window", len(quick), windowW)
	}
	if len(quick) == 0 {
		t.Fatal("the pill kept no slots at all, so it is a lone more button")
	}

	if got := h.w.reactionRowSize(h.frameCtx()).X; got > windowW {
		t.Fatalf("the pill is %dpx wide in a %dpx window: its right end is off screen", got, windowW)
	}

	// And the ring lists exactly what was drawn. A tag the frame does not draw
	// loses its focus at Frame time and the ring pulls it back every frame.
	ring := h.w.msgMenuItems()
	for _, dropped := range defaultQuickReactions[len(quick):] {
		for _, item := range h.w.reactionRow.row.Tags([]string{dropped}) {
			if item == &h.w.reactionRow.row.More {
				continue
			}
			if slices.Contains(ring, item) {
				t.Fatalf("the ring still lists the slot for %q, which was not drawn", dropped)
			}
		}
	}
}

// Nine chips need 243dp; a panel narrower than that scrolls them. The chip the
// ring Tabs to is then usually NOT in the frame, and Gio drops the focus of any
// tag the frame did not draw — so the ring pulled focus back to the search field
// and the last categories were unreachable.
//
// Asking gtx.Focused which chip to scroll to cannot fix it: focus moves at Frame
// time, so on the frame the ring issues its FocusCmd nothing is focused yet. The
// ring has to SAY where it sent focus (menuFocusState.want), the same way the
// menu card's scroller is told.
func TestKeyboardReachesCategoriesOnARowTooNarrowToSpreadThem(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.width = 160
	h.frame()
	slot := int(ui.ReactionSlotSideDp)
	h.tap(f32.Pt(float32(10+5+len(h.w.reactionRow.quick)*(slot+5)+slot/2), float32(10+5+slot/2)))
	if !h.w.reactionPickerOpen() {
		t.Fatal("precondition: the panel did not open")
	}
	h.frame()

	panel := &h.w.reactionRow.panel
	if !h.router.Source().Focused(&panel.Search) {
		t.Fatal("the panel did not open with focus in its search field")
	}
	if panel.Row.Position.Count == 0 || panel.Row.Position.Count >= len(emojiCategoryOrder) {
		t.Fatalf("precondition: a %ddp panel laid out %d of %d chips, so its row does not scroll",
			h.width, panel.Row.Position.Count, len(emojiCategoryOrder))
	}

	// Tab to the LAST category — the one furthest off the end of the row.
	chips := panel.CategoryTags(h.w.emojiPickerCategories())
	last := chips[len(chips)-1]
	steps := slices.Index(h.w.msgMenuItems(), last)
	for range steps {
		h.frame(key.Event{Name: key.NameTab, State: key.Press})
	}
	h.frame()

	if !h.router.Source().Focused(last) {
		t.Fatalf("Tabbing %d times did not hold focus on the last category: the row never scrolled to it", steps)
	}
}

// An open overlay covers the chat, so the wheel over it belongs to the overlay
// — and it does, though not because anything filters for scroll.
//
// A Gio area that registers an event.Op is OPAQUE to pointer routing: nothing
// below it is considered for any pointer event, whatever filters that area's
// own tag declared. The backdrop and the two surfaces each register one, so the
// wheel stops at them and never reaches the message list. Adding pointer.Scroll
// to their filters would only hand them events they have nothing to do with.
//
// Worth a test all the same: the property is load-bearing and invisible in the
// code, and the closed-overlay control below is what proves the harness would
// notice if it stopped holding.
func TestOverlaySwallowsTheWheel(t *testing.T) {
	slot := int(ui.ReactionSlotSideDp)
	wheel := func(h *reactionHarness, at f32.Point) {
		h.frame(pointer.Event{
			Kind:     pointer.Scroll,
			Source:   pointer.Mouse,
			Position: at,
			Scroll:   f32.Pt(0, 40),
		})
	}

	// The control: with no overlay up, the same wheel at the same place reaches
	// the chat. Without it every case below would pass on an inert harness.
	t.Run("with no overlay, the chat scrolls", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		var chat int
		h.under = &chat
		h.w.msgContextMsg = nil
		h.frame()
		wheel(h, f32.Pt(380, 700))
		if !h.scrolled {
			t.Fatal("the chat did not scroll with nothing covering it: this harness proves nothing")
		}
	})

	for _, tc := range []struct {
		name string
		at   f32.Point
		open bool
	}{
		{name: "over the pill's padding", at: f32.Pt(12, 12)},
		{name: "over the dimmed backdrop", at: f32.Pt(380, 700)},
		{name: "over the panel's header", at: f32.Pt(120, 29), open: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newReactionHarness(t, 800)
			var chat int
			h.under = &chat
			h.frame()
			if tc.open {
				h.tap(f32.Pt(float32(10+5+7*(slot+5)+slot/2), float32(10+5+slot/2)))
				if !h.w.reactionPickerOpen() {
					t.Fatal("precondition: the panel did not open")
				}
			}

			wheel(h, tc.at)
			if h.scrolled {
				t.Fatalf("a wheel at %v reached the chat list under the overlay", tc.at)
			}
		})
	}
}

// The quick-reaction count is worked out against the window LESS an 8dp edge,
// and placement has to honour the same edge or the reservation is a fiction:
// placeMenu only clamps into [0, windowW-blockW], so a menu opened near the left
// edge put the pill flush against it.
func TestPlacedOverlayKeepsOffTheWindowEdges(t *testing.T) {
	slot := int(ui.ReactionSlotSideDp)

	t.Run("opened hard against the left edge", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		h.w.msgContextPos = image.Pt(0, 0)
		h.frame()

		// Inside the window but inside the reserved edge: this is backdrop, and
		// the backdrop dismisses on the press.
		at := f32.Pt(float32(msgOverlayEdgeDp)/2, float32(msgOverlayEdgeDp)/2)
		h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		if h.w.msgContextMsg != nil {
			t.Fatalf("a press at %v landed on the pill: it is flush against the window edge", at)
		}
	})

	t.Run("the first slot is still where the edge puts it", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		h.w.msgContextPos = image.Pt(0, 0)
		h.frame()

		// Half a slot in from the pill's own origin, which is the edge.
		at := f32.Pt(float32(msgOverlayEdgeDp+5+slot/2), float32(msgOverlayEdgeDp+5+slot/2))
		h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		if h.w.msgContextMsg == nil {
			t.Fatalf("a press at %v fell through to the backdrop: no slot was drawn there", at)
		}
	})

	t.Run("opened hard against the right edge", func(t *testing.T) {
		h := newReactionHarness(t, 800)
		h.w.msgContextPos = image.Pt(h.width, 0)
		h.frame()

		at := f32.Pt(float32(h.width)-float32(msgOverlayEdgeDp)/2, float32(msgOverlayEdgeDp)+10)
		h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		if h.w.msgContextMsg != nil {
			t.Fatalf("a press at %v landed on the pill: it is flush against the right edge", at)
		}
	})
}

// Every surface this overlay places goes through one width rule and one
// placement rule, so none of them can end up outside the window.
//
// The menu card was the one that did not: the pill and the panel were made to
// fit and the card kept a flat 180dp, so on a 160dp window the two surfaces it
// is placed WITH stayed inside while the card ran 28px off the edge under them.
func TestEveryOverlaySurfaceStaysInsideTheWindow(t *testing.T) {
	gtx := layout.Context{
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 800)},
	}
	const edge = msgOverlayEdgeDp
	wants := map[string]int{
		"the menu card": msgMenuWidthDp,
		"the pill":      365,
		"the panel":     390,
	}

	// From zero up, because the previous cut of this test started above the
	// degenerate range and missed a window with no usable width at all.
	// Anchors at both extremes and in the middle: placeMenu flips and clamps,
	// and the edge has to survive all three.
	for windowW := 0; windowW <= 500; windowW++ {
		if !msgOverlayFitsWidth(gtx, windowW) {
			// Nothing is drawn at all down here; TestAWindowWithNoUsableWidth
			// covers what the overlay does instead.
			continue
		}
		for name, want := range wants {
			width := msgOverlayWidth(gtx, want, windowW)
			if width > windowW-2*edge {
				t.Fatalf("%s is %dpx wide in a %dpx window, more than the %dpx between its edges",
					name, width, windowW, windowW-2*edge)
			}
			for _, anchorX := range []int{0, windowW / 2, windowW} {
				x, _ := placeMsgOverlay(gtx, image.Pt(anchorX, 10), image.Pt(width, 40), windowW, 800)
				if x < edge || x+width > windowW-edge {
					t.Fatalf("%s spans %d..%d in a %dpx window anchored at %d, outside the %dpx edges",
						name, x, x+width, windowW, anchorX, edge)
				}
			}
		}
	}
}

// A window with no usable width has to be treated like one with no usable
// height: nothing drawn, an EMPTY focus ring, and Escape still working.
//
// Letting the width collapse on its own gave every surface a size of zero, so
// nothing was drawn — but the overlay still counted as open. The chat stayed
// dimmed behind a menu that was not there, the ring went on listing widgets no
// frame mentioned, and Escape was the only way out of a state with nothing on
// screen to explain it.
func TestAWindowWithNoUsableWidthDrawsNoOverlay(t *testing.T) {
	for _, windowW := range []int{0, 1, msgOverlayEdgeDp, 2 * msgOverlayEdgeDp, 2*msgOverlayEdgeDp + 4} {
		t.Run(fmt.Sprintf("%dpx", windowW), func(t *testing.T) {
			h := newReactionHarness(t, 800)
			h.width = windowW
			h.frame()

			if h.w.reactionRow.shown {
				t.Fatal("the pill was drawn in a window with no room across it")
			}

			// The overlay hands the ring an EMPTY item list while it draws
			// nothing, so no focus is claimed for a widget no frame mentions.
			// Observed through the result rather than through msgMenuItems,
			// which reports what COULD be in the ring rather than what the
			// deferred path passed it.
			source := h.router.Source()
			for _, item := range append(h.w.reactionRow.row.Tags(defaultQuickReactions),
				&h.w.msgCtxReply, &h.w.msgCtxCopy, &h.w.msgCtxDelete) {
				if source.Focused(item) {
					t.Fatal("the ring claimed focus for a widget the overlay did not draw")
				}
			}

			// Nothing is drawn, so nothing is dimmed either: a 40% wash over a
			// chat with no menu on it reads as an application that has hung.
			if got := msgOverlayScrim(false); got != ui.MenuPopupScrimNone {
				t.Fatalf("the backdrop tints at %v while the overlay draws nothing", got)
			}

			// The press-catcher stays, though, so a tap is still a way out —
			// alongside Escape, exactly as when the height is the problem. A
			// window with no area at all is skipped: there is nowhere for a
			// press to land, which is a property of the window and not of this.
			if windowW > 0 {
				h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: f32.Pt(0, 400)})
				if h.w.msgContextMsg != nil {
					t.Fatal("a press did not close an overlay that had nothing to show")
				}
				h.w.msgContextMsg = &service.DirectMessage{ID: "m1"}
				h.frame()
			}
			h.frame(key.Event{Name: key.NameEscape, State: key.Press})
			if h.w.msgContextMsg != nil {
				t.Fatal("Escape did not close an overlay that had nothing to show")
			}
		})
	}
}

// Every width in this overlay is summed in PIXELS from the terms its surfaces
// draw, never by converting a total in dp. The two differ at fractional
// densities, and the gate that admitted a window is compared against the card
// that has to fit in it: at 1.5 px/dp the card's own 2×Dp(1) + 2×Dp(6) is 22px
// while Dp(14) is 21, so "more than 14dp" let through a window that left the
// card's content exactly nothing.
func TestOverlayWidthsSurviveFractionalDensities(t *testing.T) {
	for _, dpi := range []float32{1, 1.25, 1.5, 2, 2.5, 3, 3.5} {
		t.Run(fmt.Sprintf("%gpx per dp", dpi), func(t *testing.T) {
			gtx := layout.Context{Metric: unit.Metric{PxPerDp: dpi, PxPerSp: dpi}}
			chrome := msgMenuCardChromePx(gtx)

			for windowW := range 600 {
				if !msgOverlayFitsWidth(gtx, windowW) {
					continue
				}
				width := msgOverlayWidth(gtx, gtx.Dp(unit.Dp(msgMenuWidthDp)), windowW)
				if width <= chrome {
					t.Fatalf("a %dpx window passed the gate but gives the card %dpx, which is its %dpx of chrome and nothing else",
						windowW, width, chrome)
				}
			}
		})
	}
}

// The open panel needs far more width than the menu card it replaces: its
// header carries a close button laid out at an exact 44dp square, which cannot
// shrink and cannot be clipped into something a finger can still hit.
//
// The width gate asked only for the CARD's 14dp of chrome, so narrowing the
// window while the panel was open kept drawing it at widths where the close
// button hung outside its own surface.
func TestNarrowingTheWindowStepsBackFromTheOpenPanel(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()
	slot := int(ui.ReactionSlotSideDp)
	h.tap(f32.Pt(float32(10+5+len(h.w.reactionRow.quick)*(slot+5)+slot/2), float32(10+5+slot/2)))
	if !h.w.reactionPickerOpen() {
		t.Fatal("precondition: the panel did not open")
	}

	// Wide enough for the menu card, nowhere near enough for the header.
	h.width = 60
	h.frame()

	if h.w.reactionPickerOpen() {
		t.Fatalf("the panel stayed open in a %dpx window, where its close button does not fit", h.width)
	}
	if h.w.msgContextMsg == nil {
		t.Fatal("stepping back from the panel closed the whole menu")
	}
}

// The panel's own floor, at every density: a surface the gate admits must be
// able to draw the header it is required to have.
func TestOpenPanelIsNeverDrawnNarrowerThanItsHeader(t *testing.T) {
	for _, dpi := range []float32{1, 1.25, 1.5, 2, 2.5, 3} {
		t.Run(fmt.Sprintf("%gpx per dp", dpi), func(t *testing.T) {
			gtx := layout.Context{
				Metric:      unit.Metric{PxPerDp: dpi, PxPerSp: dpi},
				Constraints: layout.Constraints{Max: image.Pt(0, 800)},
			}
			w := &Window{}
			floor := ui.EmojiPickerMinWidth(gtx, ui.EmojiPickerModeReaction)
			closeSide := gtx.Dp(unit.Dp(44))

			for windowW := range 500 {
				gtx.Constraints.Max.X = windowW
				size := w.reactionPickerSize(gtx, 800)
				if size.X == 0 {
					continue
				}
				if size.X < floor {
					t.Fatalf("a %dpx window drew the panel at %dpx, below its %dpx floor", windowW, size.X, floor)
				}
				if size.X < closeSide {
					t.Fatalf("a %dpx window drew the panel at %dpx, narrower than its %dpx close button",
						windowW, size.X, closeSide)
				}
			}
		})
	}
}

// A window too short for both gives the room to the menu. Reply, Copy and
// Delete are the only way to act on a message; the pill is a shortcut to
// something the menu reaches anyway.
func TestAShortWindowKeepsTheMenuAndDropsThePill(t *testing.T) {
	h := newReactionHarness(t, 60)
	h.frame()

	if h.w.reactionRow.shown {
		t.Fatal("a 60px window drew the pill, leaving the menu nothing")
	}
	for _, tag := range h.w.reactionRow.row.Tags(defaultQuickReactions) {
		for _, item := range h.w.msgMenuItems() {
			if item == tag {
				t.Fatal("the focus ring still lists a slot the frame does not draw, so focus is dropped every frame")
			}
		}
	}
}

// With the panel up the ring is its search field and its close button, and the
// arrows go back to the caret. A ring that walked the grid would take a minute
// to Tab past; one that kept the arrows would make the search box unusable.
func TestTheOpenPanelOwnsTheRingAndLeavesTheArrowsAlone(t *testing.T) {
	w := &Window{}
	w.msgContextMsg = &service.DirectMessage{ID: "m1"}
	w.openReactionPicker()

	got := w.msgMenuItems()
	if got[0] != &w.reactionRow.panel.Search {
		t.Fatal("the ring does not start at the search field, which is where picking starts")
	}
	if !slices.Contains(got, event.Tag(&w.reactionRow.panel.Close)) {
		t.Fatal("the ring has no way to close the panel")
	}

	// What must NOT be in it: the surfaces the panel is standing over. Their
	// widgets are not drawn while it is up, and Gio drops the focus of any tag
	// the frame does not draw.
	for _, hidden := range append(w.reactionRow.row.Tags(defaultQuickReactions),
		&w.msgCtxReply, &w.msgCtxCopy, &w.msgCtxDelete) {
		if slices.Contains(got, hidden) {
			t.Fatal("the ring lists a widget the open panel is covering")
		}
	}

	if w.msgMenuNavKeys().Arrows {
		t.Fatal("the ring took Up/Down from the panel's search field and its grid cursor")
	}
}

// Sending ends the composing gesture, so every surface that gesture put on
// screen comes down with it. A menu left standing over a sent message is one the
// user has to dismiss by hand before they can see what they just sent.
//
// The menu goes with the pill rather than being left behind: the pill is drawn
// as part of that menu, so closing one and not the other leaves a menu with a
// hole where its top half was.
func TestSendingClosesTheReactionSurfaces(t *testing.T) {
	h := newReactionHarness(t, 800)
	h.frame()
	h.w.openReactionPicker()
	h.frame()
	if !h.w.reactionPickerOpen() {
		t.Fatal("the picker did not open; the test would prove nothing")
	}

	// Through the real entry point, not the helper: what is claimed is that
	// SENDING closes them, and a test that called closeEmojiSurfaces directly
	// would still pass with the call removed from triggerSend.
	//
	// The composer is empty, so triggerSend does its dismissal and then finds
	// nothing to send — which is the whole of the path being pinned here.
	h.w.triggerSend(h.frameCtx())

	if h.w.reactionPickerOpen() {
		t.Fatal("the reaction picker survived the send")
	}
	if h.w.reactionRow.pickerFor != nil {
		t.Fatal("the panel is still keyed to a menu that is gone")
	}
	if h.w.msgContextMsg != nil {
		t.Fatal("the message menu the pill belongs to survived the send")
	}
	if h.w.emojiPicker.visible {
		t.Fatal("the composer's emoji picker survived the send")
	}
}

// recordingReactionRouter stands in for the router so a reaction decision can be
// observed without a node or a database behind it.
type recordingReactionRouter struct {
	toggled     []string
	unsupported bool
	// unsupportedPeers answers per conversation, because "this peer's build
	// cannot take reactions" is a property of a peer and the window has to keep
	// them apart when the user switches chats.
	unsupportedPeers map[domain.PeerIdentity]bool
	statuses         []string
}

func (r *recordingReactionRouter) MessageReactions(context.Context, domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error) {
	return nil, nil
}

func (r *recordingReactionRouter) ToggleReaction(
	_ context.Context, _ domain.PeerIdentity, messageID domain.MessageID, emoji string, _ time.Time,
) (domain.ReactionFact, error) {
	r.toggled = append(r.toggled, string(messageID)+" "+emoji)
	return domain.ReactionFact{}, nil
}

func (r *recordingReactionRouter) ReactionsUnsupportedBy(peer domain.PeerIdentity) bool {
	return r.unsupported || r.unsupportedPeers[peer]
}

func (r *recordingReactionRouter) SetSendStatus(status string) {
	r.statuses = append(r.statuses, status)
}

func (r *recordingReactionRouter) SetSendStatusIfCurrent(expected, replacement string) bool {
	if len(r.statuses) == 0 || r.statuses[len(r.statuses)-1] != expected {
		return false
	}
	r.statuses = append(r.statuses, replacement)
	return true
}

// The chips under a message are drawn as buttons, with a button's semantics, so
// a press on one has to reach the decision — as a TOGGLE of that emoji, the
// shortest way to say "me too" or to take it back.
//
// Observed at the router and not at the widget, and that is forced rather than
// chosen: widget.Clickable drains and DISCARDS pending clicks at the top of its
// own Layout, so an unwired row swallows a press exactly as silently as a wired
// one. The call below is the only place the two differ.
func TestTappingAChipReachesTheDecision(t *testing.T) {
	router := &recordingReactionRouter{}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	message := service.DirectMessage{ID: "m1", Body: "hi"}
	w.msgReactionState = map[domain.MessageID][]domain.Reaction{
		domain.MessageID(message.ID): {{Emoji: "👍", Actors: []domain.PeerIdentity{{}}, Mine: true}},
	}
	bubble := w.bubbleReactions(message)
	if bubble == nil {
		t.Fatal("a message with a reaction drew no chip row")
	}

	var gioRouter input.Router
	frame := func(events ...event.Event) {
		for _, ev := range events {
			gioRouter.Queue(ev)
		}
		ops := new(op.Ops)
		bubble(layout.Context{
			Ops:         ops,
			Source:      gioRouter.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(400, 200)},
		})
		gioRouter.Frame(ops)
	}

	frame()
	// The centre of the row, which at one pixel per dp is one chip wide.
	at := f32.Pt(23, 11)
	frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
	frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})
	// The drain runs before the row lays out, so the click the release frame
	// records is taken by the frame after it.
	frame()

	if len(router.toggled) != 1 || router.toggled[0] != "m1 👍" {
		t.Fatalf("the tap reached the decision as %v, want one toggle of 👍 on m1", router.toggled)
	}
}

// A peer whose build cannot receive reactions is the one case where the chip on
// screen will never be seen by anyone else, and the whole reason the transport
// can tell that apart from "offline". Saying nothing would make it look exactly
// like a reaction that has arrived.
func TestAReactionThePeerCannotReceiveIsReportedAsLocal(t *testing.T) {
	router := &recordingReactionRouter{unsupported: true}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))

	w.toggleReactionOn("m1", "👍")

	if len(router.statuses) != 1 || router.statuses[0] != w.t("status.reaction_local_only") {
		t.Fatalf("the user was told %v", router.statuses)
	}
}

// The pill fills EVERY slot the user already holds, and marks focus with a ring
// rather than a second fill.
//
// Both halves come from one bug report: a message with five reactions showed two
// filled circles — one for the single emoji the code bothered to mark, one for
// wherever the keyboard happened to be — and read as "two chosen, and the other
// three did not take".
func TestThePillMarksEveryReactionAlreadyHeld(t *testing.T) {
	w := &Window{theme: newAppTheme(), language: "en"}
	message := service.DirectMessage{ID: "m1"}
	w.msgContextMsg = &message
	w.reactionRow.quick = defaultQuickReactions

	for _, quick := range defaultQuickReactions {
		if w.holdsReaction(quick) {
			t.Fatalf("a message with no reactions marked %q as held", quick)
		}
	}

	w.msgReactionState = map[domain.MessageID][]domain.Reaction{
		"m1": {
			// Somebody else's 👍 is not ours and must not be marked.
			{Emoji: "👍", Actors: []domain.PeerIdentity{{}}},
			{Emoji: "❤️", Actors: []domain.PeerIdentity{{}}, Mine: true},
			{Emoji: "🔥", Actors: []domain.PeerIdentity{{}}, Mine: true},
			{Emoji: "🙏", Actors: []domain.PeerIdentity{{}}, Mine: true},
			// One we hold that the pill does not offer: no slot to mark.
			{Emoji: "🐢", Actors: []domain.PeerIdentity{{}}, Mine: true},
		},
	}
	marked := make([]string, 0, len(defaultQuickReactions))
	for _, quick := range defaultQuickReactions {
		if w.holdsReaction(quick) {
			marked = append(marked, quick)
		}
	}
	want := []string{"❤️", "🔥", "🙏"}
	if !slices.Equal(marked, want) {
		t.Fatalf("the pill marks %v, want every one this user holds %v", marked, want)
	}
}

// The refusal is learned AFTER the tap: a tap queues the fact, and the node
// finds out that the peer's build cannot take it a second later, when the
// debounced frame goes out. Without the asynchronous half the first reaction to
// an old client always looks delivered, and the user is told about it only on
// the next tap, about the previous reaction.
func TestALaterRefusalStillTellsTheUserTheReactionStayedHere(t *testing.T) {
	router := &recordingReactionRouter{}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))

	// The tap: nothing is known about the peer's build yet, so nothing is said.
	w.toggleReactionOn("m1", "👍")
	if len(router.statuses) != 0 {
		t.Fatalf("the user was told %v before anything was known", router.statuses)
	}

	// The node learns, publishes, and the layout goroutine picks the flag up.
	router.unsupported = true
	w.noteReactionsChanged()
	w.reloadStaleReactions()

	if len(router.statuses) != 1 || router.statuses[0] != w.t("status.reaction_local_only") {
		t.Fatalf("the user was told %v, want the reaction-stayed-here notice", router.statuses)
	}

	// And once, not on every later merge in the same conversation: the status
	// line is shared with everything else the window has to say.
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	if len(router.statuses) != 1 {
		t.Fatalf("the notice was repeated: %v", router.statuses)
	}
}

// The refusal names a peer; the flag that crosses into the layout goroutine
// does not. So a refusal learned for A while B is open is picked up by B's
// frame, which asks about B and finds nothing to say — and A's notice is lost
// until something else happens there. Entering a conversation has to ask about
// THAT conversation.
//
// And the line is ONE line for the whole window, so entering a conversation that
// can take reactions has to take the notice off it: a check per conversation
// leaves a notice about A standing in B.
func TestARefusalLearnedInAnotherChatIsToldOnReturn(t *testing.T) {
	router := &recordingReactionRouter{unsupportedPeers: map[domain.PeerIdentity]bool{}}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	first := domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	second := domain.PeerIdentityFromWire(strings.Repeat("bb", 20))
	notice := w.t("status.reaction_local_only")
	told := func() int {
		count := 0
		for _, status := range router.statuses {
			if status == notice {
				count++
			}
		}
		return count
	}
	onScreen := func() string {
		if len(router.statuses) == 0 {
			return ""
		}
		return router.statuses[len(router.statuses)-1]
	}

	// The user is looking at the second conversation when the node learns the
	// FIRST one cannot take reactions.
	w.snap.ActivePeer = second
	w.lastChatPeer = second
	router.unsupportedPeers[first] = true
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	if told() != 0 {
		t.Fatalf("the open conversation was told about another one: %v", router.statuses)
	}

	// Going back to the first is where it has to be said.
	w.snap.ActivePeer = first
	w.resetReplyOnPeerChange()
	if told() != 1 || onScreen() != notice {
		t.Fatalf("returning to the refused conversation said %v", router.statuses)
	}

	// Walking into a conversation that CAN take them takes the notice off the
	// line: it is about the chat the user has left.
	w.snap.ActivePeer = second
	w.resetReplyOnPeerChange()
	if onScreen() != "" {
		t.Fatalf("the notice about another chat stayed on screen: %q", onScreen())
	}

	// And coming back does not say it again: it is one fact about that
	// conversation, and the line is shared with everything else.
	w.snap.ActivePeer = first
	w.resetReplyOnPeerChange()
	if told() != 1 {
		t.Fatalf("the notice was repeated on every visit: %v", router.statuses)
	}

	// Two refused conversations are two facts, and walking between them must
	// say each once — not re-announce whichever was not the last one seen.
	router.unsupportedPeers[second] = true
	w.snap.ActivePeer = second
	w.resetReplyOnPeerChange()
	if told() != 2 {
		t.Fatalf("the second refused conversation was not announced: %v", router.statuses)
	}
	w.snap.ActivePeer = first
	w.resetReplyOnPeerChange()
	w.snap.ActivePeer = second
	w.resetReplyOnPeerChange()
	if told() != 2 {
		t.Fatalf("walking between two refused conversations kept repeating: %v", router.statuses)
	}
}

// Choosing an emoji as a reaction is choosing it: it joins the recents the same
// as picking one for the draft does. The panel serves both surfaces, so a
// recents list that only learns from the composer is one the reaction picker
// never fills.
func TestReactingWithAnEmojiPutsItInTheRecents(t *testing.T) {
	router := &recordingReactionRouter{}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router, prefs: &Preferences{}}
	w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))

	w.toggleReactionOn("m1", "🔥")

	if len(w.emojiPicker.recents) == 0 || w.emojiPicker.recents[0] != "🔥" {
		t.Fatalf("the recents are %v, want the reaction's emoji at the head", w.emojiPicker.recents)
	}
	if !w.emojiPicker.recentSavePending {
		t.Fatal("the new recents were never scheduled to be written")
	}
	// Taking it back counts too: the user picked that emoji to act on, and which
	// way the toggle went is not what "recently used" is about.
	w.toggleReactionOn("m1", "👍")
	if w.emojiPicker.recents[0] != "👍" {
		t.Fatalf("the recents are %v, want the last emoji used at the head", w.emojiPicker.recents)
	}
}

// The notice is a claim about the peer's build, and when that claim stops being
// true it has to be taken back: after they upgrade — a reaction from them proves
// it — the status line would otherwise keep telling the user their reactions do
// not arrive. Taken back CONDITIONALLY, because the status line is shared: an
// unrelated message written since must not be wiped by news about reactions.
func TestTheLocalOnlyNoticeIsTakenBackWhenThePeerCanReceiveAgain(t *testing.T) {
	notice := (&Window{theme: newAppTheme(), language: "en"}).t("status.reaction_local_only")

	for _, tc := range []struct {
		name string
		// wroteSince is what something else put on the status line between the
		// notice and the peer upgrading.
		wroteSince string
		want       string
	}{
		{name: "the notice is still what the line says", want: ""},
		{name: "something else has written since", wroteSince: "sending…", want: "sending…"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := &recordingReactionRouter{unsupportedPeers: map[domain.PeerIdentity]bool{}}
			w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
			peer := domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
			w.snap.ActivePeer = peer

			router.unsupportedPeers[peer] = true
			w.noteReactionsChanged()
			w.reloadStaleReactions()
			if len(router.statuses) != 1 || router.statuses[0] != notice {
				t.Fatalf("the user was told %v", router.statuses)
			}
			if tc.wroteSince != "" {
				router.SetSendStatus(tc.wroteSince)
			}

			// They upgrade, and the next thing that reloads the conversation
			// finds it.
			delete(router.unsupportedPeers, peer)
			w.noteReactionsChanged()
			w.reloadStaleReactions()

			if last := router.statuses[len(router.statuses)-1]; last != tc.want {
				t.Fatalf("the status line says %q, want %q", last, tc.want)
			}
		})
	}
}

// The notice is taken back by comparing against what is on the line, so the
// comparison has to use the string that was WRITTEN. Re-translating it looks
// equivalent and is not: a user who changes language between the notice and the
// peer upgrading leaves a line in the old language that the new translation
// never matches — and the notice then stays up for good.
func TestTheLocalOnlyNoticeIsTakenBackAfterALanguageChange(t *testing.T) {
	router := &recordingReactionRouter{unsupportedPeers: map[domain.PeerIdentity]bool{}}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	peer := domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	w.snap.ActivePeer = peer

	router.unsupportedPeers[peer] = true
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	english := w.t("status.reaction_local_only")
	if len(router.statuses) != 1 || router.statuses[0] != english {
		t.Fatalf("the user was told %v", router.statuses)
	}

	// The user switches language, and only then does the peer upgrade.
	w.language = "ru"
	if w.t("status.reaction_local_only") == english {
		t.Fatal("the fixture needs two languages that differ on this string")
	}
	delete(router.unsupportedPeers, peer)
	w.noteReactionsChanged()
	w.reloadStaleReactions()

	if last := router.statuses[len(router.statuses)-1]; last != "" {
		t.Fatalf("the line still says %q after the peer could receive again", last)
	}
}

// With no conversation open — Back in the compact layout, or the last contact
// removed — the notice describes a chat that is not on screen, and in the
// two-panel layout the status line still is. The visible line goes; what has
// been announced per conversation does NOT, because this state is reached by an
// ordinary Back and re-entering must not repeat it.
func TestTheLocalOnlyNoticeGoesWhenNoConversationIsOpen(t *testing.T) {
	router := &recordingReactionRouter{unsupportedPeers: map[domain.PeerIdentity]bool{}}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	peer := domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	notice := w.t("status.reaction_local_only")
	told := func() int {
		count := 0
		for _, status := range router.statuses {
			if status == notice {
				count++
			}
		}
		return count
	}
	w.snap.ActivePeer = peer
	w.lastChatPeer = peer

	router.unsupportedPeers[peer] = true
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	if told() != 1 {
		t.Fatalf("the user was told %v", router.statuses)
	}

	// Back: nothing is open.
	w.snap.ActivePeer = domain.PeerIdentity{}
	w.resetReplyOnPeerChange()
	if last := router.statuses[len(router.statuses)-1]; last != "" {
		t.Fatalf("the line still says %q with no conversation open", last)
	}

	// Coming back in does not say it again: it is one fact about that
	// conversation, and Back is not a new conversation.
	w.snap.ActivePeer = peer
	w.resetReplyOnPeerChange()
	if told() != 1 {
		t.Fatalf("an ordinary Back made the notice repeat: %v", router.statuses)
	}
}

// Removing the contact IS a new conversation next time, so what was announced
// about them is dropped where the contact is — not where the UI happens to show
// no chat, which a removal with other chats beside it never reaches.
func TestRemovingAContactLetsTheNoticeBeSaidAgainOnReAdd(t *testing.T) {
	router := &recordingReactionRouter{unsupportedPeers: map[domain.PeerIdentity]bool{}}
	w := &Window{theme: newAppTheme(), language: "en", reactionRouter: router}
	peer := domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	notice := w.t("status.reaction_local_only")
	told := func() int {
		count := 0
		for _, status := range router.statuses {
			if status == notice {
				count++
			}
		}
		return count
	}
	w.snap.ActivePeer = peer
	w.lastChatPeer = peer

	router.unsupportedPeers[peer] = true
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	if told() != 1 {
		t.Fatalf("the user was told %v", router.statuses)
	}

	// The contact is removed — with another chat beside it, so the UI never
	// passes through "nothing open" — and later added again.
	w.forgetPeerReactionNotice(peer)
	if len(w.reactionsLocalOnlyFor) != 0 {
		t.Fatalf("%d conversations are still marked as announced", len(w.reactionsLocalOnlyFor))
	}
	w.noteReactionsChanged()
	w.reloadStaleReactions()
	if told() != 2 {
		t.Fatalf("a re-added contact was not told again: %v", router.statuses)
	}
}

// The quick row is longest first and trimmed to what the window can hold, so
// adding a choice costs the tail on a narrow screen rather than a pill hanging
// off its edge. Both halves are pinned here because the list is edited by hand
// and it is the ONE place where "one more emoji" changes layout.
func TestTheQuickRowOffersItsChoicesLongestFirst(t *testing.T) {
	w := &Window{theme: newAppTheme(), language: "en"}
	if defaultQuickReactions[0] != "👍" || defaultQuickReactions[1] != "👌" {
		t.Fatalf("the row starts with %v, want 👍 then 👌", defaultQuickReactions[:2])
	}
	// The one choice that is deliberately LAST: a narrow window drops the tail,
	// and a "no" offered before a "yes" is not what the pill is for.
	if last := defaultQuickReactions[len(defaultQuickReactions)-1]; last != "👎" {
		t.Fatalf("the row ends with %q, want 👎", last)
	}

	// A phone: fewer slots than the list holds, taken from the front.
	phone := testGtxAt(412)
	onPhone := w.quickReactionsFor(phone, 412)
	if len(onPhone) == 0 || len(onPhone) >= len(defaultQuickReactions) {
		t.Fatalf("a 412dp window offered %d of %d choices", len(onPhone), len(defaultQuickReactions))
	}
	for i, emoji := range onPhone {
		if emoji != defaultQuickReactions[i] {
			t.Fatalf("the trimmed row is %v, want the head of %v", onPhone, defaultQuickReactions)
		}
	}

	// A desktop window: all of them.
	wide := testGtxAt(1200)
	if got := w.quickReactionsFor(wide, 1200); len(got) != len(defaultQuickReactions) {
		t.Fatalf("a 1200dp window offered %d of %d choices", len(got), len(defaultQuickReactions))
	}
}

func testGtxAt(width int) layout.Context {
	return layout.Context{
		Ops:         new(op.Ops),
		Source:      new(input.Router).Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(width, 800)},
	}
}

// A press on a tail slot survives the window being narrowed under it.
//
// The overlay recomputes the quick list for THIS frame's width before the
// presses of the previous frame are read, so a narrower window drops the slot
// the user actually pressed — and a press nobody asks about is not merely
// unanswered: Gio discards it at Frame time, and the tap is lost.
func TestATapOnATailSlotSurvivesTheWindowNarrowing(t *testing.T) {
	router := &recordingReactionRouter{}
	h := newReactionHarness(t, 800)
	h.w.reactionRouter = router
	h.w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	h.width = 1200
	h.frame()

	wide := append([]string{}, h.w.reactionRow.quick...)
	if len(wide) < 2 {
		t.Fatalf("a 1200dp window laid out %d slots", len(wide))
	}
	tail := wide[len(wide)-1]

	// Press the last slot, then narrow the window so it is gone by the frame
	// that reads the press.
	slot := int(ui.ReactionSlotSideDp)
	gap := 5
	x := 10 + gap + (len(wide)-1)*(slot+gap) + slot/2
	h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: f32.Pt(float32(x), float32(10+gap+slot/2))})
	h.width = 412
	h.frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: f32.Pt(float32(x), float32(10+gap+slot/2))})

	if narrow := h.w.reactionRow.quick; len(narrow) >= len(wide) {
		t.Fatalf("the fixture needs the narrow window to drop slots: %d then %d", len(wide), len(narrow))
	}
	if len(router.toggled) != 1 || router.toggled[0] != "m1 "+tail {
		t.Fatalf("the tap on the tail slot was reported as %v, want one toggle of %s", router.toggled, tail)
	}
}

// The whole row can go between press and release — the window narrows, or a
// keyboard comes up and leaves no height — and the press is still the user's.
// The handler used to return before draining when the row had no room, so Gio
// discarded the event at Frame time and the tap was lost.
func TestATapSurvivesTheWholeRowLosingItsRoom(t *testing.T) {
	router := &recordingReactionRouter{}
	h := newReactionHarness(t, 800)
	h.w.reactionRouter = router
	h.w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	h.frame()
	if !h.w.reactionRow.shown {
		t.Fatal("precondition: the pill was not drawn")
	}

	slot := int(ui.ReactionSlotSideDp)
	at := f32.Pt(float32(10+5+slot/2), float32(10+5+slot/2))
	h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})

	// No room at all by the time the release is read: not one slot fits across
	// the window, so the pill is not drawn.
	h.width = 40
	h.frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})

	if h.w.reactionRow.shown {
		t.Fatal("the fixture needs the row to have lost its room")
	}
	if len(router.toggled) != 1 || router.toggled[0] != "m1 "+defaultQuickReactions[0] {
		t.Fatalf("the tap was reported as %v, want one toggle of %s", router.toggled, defaultQuickReactions[0])
	}
}

// The whole OVERLAY can go between press and release — the keyboard comes up and
// leaves less than the menu's own minimum — and the press is still the user's.
// The deferred branch used to return before draining, so Gio discarded the event
// at Frame time and the tap was lost.
func TestATapSurvivesTheWholeOverlayBeingDeferred(t *testing.T) {
	router := &recordingReactionRouter{}
	h := newReactionHarness(t, 800)
	h.w.reactionRouter = router
	h.w.snap.ActivePeer = domain.PeerIdentityFromWire(strings.Repeat("aa", 20))
	h.frame()
	if !h.w.reactionRow.shown {
		t.Fatal("precondition: the pill was not drawn")
	}

	slot := int(ui.ReactionSlotSideDp)
	at := f32.Pt(float32(10+5+slot/2), float32(10+5+slot/2))
	h.frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})

	// A keyboard takes the room: under the menu's own minimum the whole overlay
	// is deferred, not merely drawn without its pill.
	h.height = 40
	h.frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})

	if len(router.toggled) != 1 || router.toggled[0] != "m1 "+defaultQuickReactions[0] {
		t.Fatalf("the tap was reported as %v, want one toggle of %s", router.toggled, defaultQuickReactions[0])
	}
}
