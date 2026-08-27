package desktop

import (
	"image"
	"testing"

	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// scrollHarness drives a menu that does not fit: real layout.List, real
// layout.Flex, real input.Router, real menuFocusState. The existing focus tests
// lay every tag out flat and unclipped, which is exactly the arrangement that
// hides this defect — focus walked correctly there because there was no
// viewport for a row to fall outside of.
//
// The frame below is the overlay's, in the overlay's order: drive first (so the
// focus move is known before anything is measured), then the card recorded into
// a macro and replayed, then into. Nothing is scrolled during the measurement,
// only after it, so what these tests read is the offset the NEXT frame draws
// with — which is the whole shape of the fix.
type scrollHarness struct {
	router *input.Router
	ops    *op.Ops
	list   layout.List
	scroll menuScroll
	state  menuFocusState
	items  []event.Tag
	rowH   int
	view   int
	// stray adds one Flex child that does NOT go through menuScroll.row, to
	// exercise the self-check in flex.
	stray bool
	// grab is focused at the END of a frame, standing in for a click landing on
	// a row: focus moves, but not because the menu's keyboard contract moved it.
	grab event.Tag
}

func newScrollHarness(rows, rowH, view int) *scrollHarness {
	h := &scrollHarness{
		router: new(input.Router),
		ops:    new(op.Ops),
		list:   layout.List{Axis: layout.Vertical},
		rowH:   rowH,
		view:   view,
	}
	for i := 0; i < rows; i++ {
		h.items = append(h.items, new(int))
	}
	// The menu is open, as the overlay's callers leave it. No trigger: where
	// focus goes when the menu CLOSES is restoreOnClose's business, not this
	// file's.
	h.state.open(nil)
	return h
}

// children builds the menu's rows the way the real sub-views do: every child
// through menuScroll.row, each one registering its tag exactly as a Clickable
// would.
func (h *scrollHarness) children() []layout.FlexChild {
	out := make([]layout.FlexChild, 0, len(h.items)+1)
	for _, it := range h.items {
		tag := it
		out = append(out, h.scroll.row(tag, func(gtx layout.Context) layout.Dimensions {
			layoutTag(gtx, tag)
			return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, h.rowH)}
		}))
	}
	if h.stray {
		out = append(out, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, h.rowH)}
		}))
	}
	return out
}

// frame runs one frame of the open menu and reports whether the list moved.
func (h *scrollHarness) frame(keys ...key.Event) bool {
	for _, k := range keys {
		h.router.Queue(k)
	}
	h.ops.Reset()
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Constraints: layout.Constraints{Max: image.Pt(200, h.view)},
	}
	h.state.drive(gtx, h.items, menuNavKeys{Arrows: true, Tab: true})

	measure := gtx
	measure.Constraints.Min = image.Point{}
	measure.Constraints.Max.Y = h.view
	macro := op.Record(measure.Ops)
	h.scroll.begin(measure.Constraints.Max.Y)
	h.list.Layout(measure, 1, func(gtx layout.Context, _ int) layout.Dimensions {
		return h.scroll.flex(gtx, h.children()...)
	})
	call := macro.Stop()
	call.Add(gtx.Ops)

	moved := h.scroll.into(&h.list, h.state.want)
	if h.grab != nil {
		gtx.Execute(key.FocusCmd{Tag: h.grab})
		h.grab = nil
	}
	h.router.Frame(h.ops)
	return moved
}

func (h *scrollHarness) offset() int { return h.list.Position.Offset }

func (h *scrollHarness) focused() int {
	src := h.router.Source()
	for i, it := range h.items {
		if src.Focused(it) {
			return i
		}
	}
	return -1
}

// onScreen fails unless row i, as THIS frame's geometry has it, lies inside the
// viewport. The pixel expectations spelled out in the tests above are worth
// keeping where the arithmetic is itself the subject; everywhere else this is
// the actual contract, and it goes on meaning the same thing when a row changes
// height.
func (h *scrollHarness) onScreen(t *testing.T, i int) {
	t.Helper()
	top, bot := i*h.rowH, (i+1)*h.rowH
	off := h.offset()
	if top < off || bot > off+h.view {
		t.Fatalf("row %d is at %d..%d, outside the visible %d..%d", i, top, bot, off, off+h.view)
	}
}

// deferFrame is the overlay with no room to draw: drive runs on an EMPTY item
// list and the card is not laid out at all, so Gio drops every row's focus at
// Frame time. Nothing measures and nothing scrolls — the list keeps the offset
// it had, which is the point of running one.
func (h *scrollHarness) deferFrame() {
	h.ops.Reset()
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Constraints: layout.Constraints{Max: image.Pt(200, h.view)},
	}
	h.state.drive(gtx, nil, menuNavKeys{Arrows: true, Tab: true})
	h.router.Frame(h.ops)
}

// The defect itself. Six 40px rows in a 100px viewport: Tab walks focus down
// past the fold, and before this fix the list sat at offset 0 for all of it —
// focus on a row drawn outside the clip, Enter activating something invisible.
func TestTabScrollsTheFocusedRowIntoView(t *testing.T) {
	h := newScrollHarness(6, 40, 100)

	// Opening focuses the first row, which is already at the top: nothing to
	// scroll, and no wasted invalidate.
	if moved := h.frame(); moved {
		t.Fatal("opening the menu scrolled a list already showing its first row")
	}
	if got := h.focused(); got != 0 {
		t.Fatalf("open focused row %d, want 0", got)
	}
	if got := h.offset(); got != 0 {
		t.Fatalf("open offset = %d, want 0", got)
	}

	// Rows 1 (40..80) and 2 (80..120): the second one crosses the fold at 100.
	h.frame(keyTab)
	if got := h.offset(); got != 0 {
		t.Fatalf("after Tab to row 1 offset = %d, want 0 (row 1 is already visible)", got)
	}
	h.frame(keyTab)
	if got, want := h.offset(), 20; got != want {
		t.Fatalf("after Tab to row 2 offset = %d, want %d (bottom of row 2 at the fold)", got, want)
	}
	if got := h.focused(); got != 2 {
		t.Fatalf("focus is on row %d, want 2", got)
	}

	// Walk to the last row: the offset must reach the end of the content and
	// stop there rather than scrolling past it.
	h.frame(keyTab)
	h.frame(keyTab)
	h.frame(keyTab)
	if got := h.focused(); got != 5 {
		t.Fatalf("focus is on row %d, want 5", got)
	}
	if got, want := h.offset(), 140; got != want {
		t.Fatalf("at the last row offset = %d, want %d (240 content - 100 viewport)", got, want)
	}

	// Wrapping around to the top must come back with it.
	h.frame(keyTab)
	if got := h.focused(); got != 0 {
		t.Fatalf("after wrapping, focus is on row %d, want 0", got)
	}
	if got := h.offset(); got != 0 {
		t.Fatalf("after wrapping, offset = %d, want 0", got)
	}
}

// Shift+Tab off the top row wraps to the bottom, and the list has to follow it
// the whole way — the backwards direction is where a "scroll down by one row"
// shortcut would silently do nothing.
func TestShiftTabScrollsBackwards(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.frame(keyShiftTab)
	if got := h.focused(); got != 5 {
		t.Fatalf("Shift+Tab from row 0 focused row %d, want 5", got)
	}
	if got, want := h.offset(), 140; got != want {
		t.Fatalf("offset = %d, want %d", got, want)
	}
	// Row 4 spans 160..200, which the viewport at 140 already shows whole. An
	// already-visible row is left alone in either direction.
	h.frame(keyShiftTab)
	if got, want := h.offset(), 140; got != want {
		t.Fatalf("after Shift+Tab to row 4 offset = %d, want %d", got, want)
	}
	// Rows 3, 2 and 1 each start above the viewport, so from here on the list
	// pulls DOWN to the focused row's top rather than pushing its bottom to the
	// fold. Row 1 spans 40..80.
	h.frame(keyShiftTab)
	h.frame(keyShiftTab)
	h.frame(keyShiftTab)
	if got := h.focused(); got != 1 {
		t.Fatalf("focus is on row %d, want 1", got)
	}
	if got, want := h.offset(), 40; got != want {
		t.Fatalf("offset = %d, want %d (row 1's top pulled to the viewport top)", got, want)
	}
}

// A menu that fits must never be scrolled. There is nothing off screen, and a
// list nudged from zero would show a menu clipped at the top for no reason.
func TestAMenuThatFitsIsNeverScrolled(t *testing.T) {
	h := newScrollHarness(4, 40, 300)
	for i := 0; i < 6; i++ {
		if moved := h.frame(keyTab); moved {
			t.Fatalf("frame %d scrolled a menu that fits", i)
		}
		if got := h.offset(); got != 0 {
			t.Fatalf("frame %d left offset %d, want 0", i, got)
		}
	}
}

// Focus that the menu contract did not move must not move the list. A tap lands
// on a row the user can already see, and yanking the list under their finger
// because focus changed would fight them.
func TestPointerFocusDoesNotScroll(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.grab = h.items[5]
	h.frame()
	if got := h.focused(); got != 5 {
		t.Fatalf("the stand-in tap did not land: focus is on row %d", got)
	}
	// The next frame sees focus on row 5 with no navigation key: drive holds
	// still, want stays nil, and so does the list.
	if moved := h.frame(); moved {
		t.Fatal("a focus move the menu did not make scrolled the list")
	}
	if got := h.offset(); got != 0 {
		t.Fatalf("offset = %d, want 0", got)
	}
}

// The measurement is only as good as its coverage. A Flex child added without
// going through menuScroll.row shifts every row below it, so the spans would
// describe a menu that is not on screen — flex catches the mismatch and
// everything downstream declines to scroll.
func TestAnUnmeasuredRowDisablesScrolling(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.stray = true
	h.frame()
	for i := 0; i < 4; i++ {
		if moved := h.frame(keyTab); moved {
			t.Fatalf("frame %d scrolled from spans that do not describe the card", i)
		}
	}
	if got := h.offset(); got != 0 {
		t.Fatalf("offset = %d, want 0", got)
	}
	if h.scroll.ok {
		t.Fatal("flex accepted a measurement it never took")
	}
}

// A sub-view that returns before laying its Flex out — layoutAliasEditorMenu on
// a submitted alias — must not leave the previous frame's spans usable.
func TestAbandonedMeasurementIsNotReused(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.frame(keyTab)
	h.frame(keyTab)
	if !h.scroll.ok {
		t.Fatal("a normally measured frame should be usable")
	}
	before := h.offset()

	// The abandoned frame: begin runs, flex never does.
	h.scroll.begin(100)
	if h.scroll.into(&h.list, h.items[5]) {
		t.Fatal("scrolled from a measurement that was abandoned")
	}
	if got := h.offset(); got != before {
		t.Fatalf("offset = %d, want %d", got, before)
	}
}

func TestMenuScrollOffset(t *testing.T) {
	tests := []struct {
		name                       string
		cur, view, total, top, bot int
		want                       int
	}{
		{"already visible, no movement", 0, 100, 240, 40, 80, 0},
		{"row below the fold pushes up", 0, 100, 240, 80, 120, 20},
		{"row above the viewport pulls down", 140, 100, 240, 40, 80, 40},
		{"exactly at the bottom edge stays put", 20, 100, 240, 80, 120, 20},
		{"the last row lands on the last offset", 0, 100, 240, 200, 240, 140},
		{"a row taller than the viewport shows its top", 0, 100, 400, 120, 300, 120},
		{"a row taller than the viewport, reached from below", 300, 100, 400, 120, 300, 120},
		{"content shorter than the viewport never scrolls", 0, 300, 160, 120, 160, 0},
		{"an over-scrolled offset is pulled back", 500, 100, 240, 200, 240, 140},
		{"a negative offset is clamped away", -50, 100, 240, 0, 40, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := menuScrollOffset(tc.cur, tc.view, tc.total, tc.top, tc.bot)
			if got != tc.want {
				t.Fatalf("menuScrollOffset(%d, %d, %d, %d, %d) = %d, want %d",
					tc.cur, tc.view, tc.total, tc.top, tc.bot, got, tc.want)
			}
		})
	}
}

// into is the gate every caller goes through, and each of its refusals stands
// for a real frame: an unmeasured card, a frame that moved no focus, a viewport
// that has collapsed, a menu that fits, and a tag that is not in this sub-view
// at all (the item set is rebuilt every frame, so a stale want is ordinary).
func TestIntoRefusesWhatItCannotAnswer(t *testing.T) {
	rows := []menuRow{{tag: new(int), top: 0, bot: 40}, {tag: new(int), top: 40, bot: 200}}
	base := menuScroll{rows: rows, view: 100, y: 200, ok: true}

	cases := []struct {
		name string
		s    menuScroll
		tag  event.Tag
	}{
		{"unmeasured", menuScroll{rows: rows, view: 100, y: 200}, rows[1].tag},
		{"no focus move this frame", base, nil},
		{"collapsed viewport", menuScroll{rows: rows, view: 0, y: 200, ok: true}, rows[1].tag},
		{"content fits", menuScroll{rows: rows, view: 400, y: 200, ok: true}, rows[1].tag},
		{"tag not in this sub-view", base, new(int)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.s
			var l layout.List
			if s.into(&l, tc.tag) {
				t.Fatal("into moved a list it had no grounds to move")
			}
			if l.Position != (layout.Position{}) {
				t.Fatalf("into wrote %+v to a list it reported it had not moved", l.Position)
			}
		})
	}

	// And the case it does answer, for contrast.
	s := base
	var l layout.List
	if !s.into(&l, rows[1].tag) {
		t.Fatal("into refused the one case it exists for")
	}
	// rows[1] is 160 tall in a 100 viewport, so it is the oversized case: the
	// offset lands on its TOP rather than pushing its bottom to the fold.
	if l.Position.Offset != 40 || l.Position.First != 0 {
		t.Fatalf("Position = %+v, want First 0 Offset 40", l.Position)
	}
}

// begin's job is to distrust everything the last frame left behind. rows and y
// are the obvious two. ok is the one that is easy to leave out, because today's
// only early return — layoutAliasEditorMenu on a submitted alias — happens
// BEFORE its first row, so an empty span list refuses on its own and a stale ok
// costs nothing. A sub-view that returned after its first row would not be so
// lucky, and the frame that returns early is written nowhere near the guard that
// decides whether to trust it.
func TestBeginDistrustsWhatTheLastFrameLeft(t *testing.T) {
	s := menuScroll{
		rows: []menuRow{{tag: new(int), top: 0, bot: 40}},
		view: 100,
		y:    240,
		ok:   true,
	}
	s.begin(100)
	if len(s.rows) != 0 {
		t.Errorf("begin kept %d span(s) from the last frame; they describe a card that is about to be replaced", len(s.rows))
	}
	if s.y != 0 {
		t.Errorf("begin left the running content offset at %d, so this frame's first row is measured as though it came after the last frame's", s.y)
	}
	if s.ok {
		t.Error("begin left the last frame's measurement trusted, so a sub-view that returns before its Flex would be scrolled by spans nobody took this frame")
	}
}

// The card can change shape between frames — a sub-view swap, a row that leaves
// the list, an alias long enough to grow the editor. The spans are cumulative
// and looked up in order, so last frame's are found FIRST and this frame's are
// never reached: the list scrolls by arithmetic that describes a menu which is
// no longer on screen.
func TestEachFrameScrollsByTheCardItJustDrew(t *testing.T) {
	h := newScrollHarness(6, 20, 100)
	h.frame()

	// The same six rows at twice the height. Every row below the first now sits
	// somewhere the previous frame's spans do not describe.
	h.rowH = 40
	h.frame(keyTab)
	if got, want := len(h.scroll.rows), len(h.items); got != want {
		t.Fatalf("the frame measured %d rows, want %d — the previous frame's spans are still in the list", got, want)
	}
	h.frame(keyTab)
	if got := h.focused(); got != 2 {
		t.Fatalf("focus is on row %d, want 2", got)
	}
	h.onScreen(t, 2)
}

// A focus move is spent on the frame that makes it. Left set, it is re-asserted
// on every frame after — and the first thing that asks for a different offset,
// the user dragging the menu with a finger, is undone on the very next frame by
// a keyboard move that happened seconds ago.
func TestAStaleFocusMoveDoesNotFightTheUser(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.frame(keyTab)
	if !h.frame(keyTab) {
		t.Fatal("the Tab that reaches row 2 should have scrolled; the rest of this test would prove nothing")
	}

	// The finger takes over.
	h.list.Position.Offset = 140
	if moved := h.frame(); moved {
		t.Fatal("a frame that moved no focus scrolled the list")
	}
	if got := h.offset(); got != 140 {
		t.Fatalf("offset = %d, want 140: the list was dragged there and nothing on this frame asked for anywhere else", got)
	}
}

// A menu with no room to draw lays out no items, so Gio drops their focus and
// drive re-arms rather than forgetting (see its empty-items branch). The frame
// that gets the room back focuses the first row again — while the list is still
// wherever it had been scrolled before the keyboard came up, which is exactly
// the state where a focused row and a viewport disagree.
func TestFocusReclaimedAfterADeferredDrawIsScrolledIntoView(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.frame(keyTab)
	h.frame(keyTab)
	h.frame(keyTab)
	if h.offset() == 0 {
		t.Fatal("the list should be scrolled by now; the rest of this test would prove nothing")
	}

	h.deferFrame()
	if got := h.focused(); got != -1 {
		t.Fatalf("row %d still holds focus after a frame that laid out no items", got)
	}
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("focus came back to row %d, want 0", got)
	}
	h.onScreen(t, 0)
}

// Focus can leave the menu without the menu having moved it. items is rebuilt
// every frame from live state, so the row holding focus can simply stop being in
// the list — the peer goes offline and "Delete chat for both sides" leaves, a row
// opens a confirmation and the whole set is swapped. drive pulls focus back to
// the first row, and the list has to follow it there for the same reason it
// follows a Tab.
func TestFocusPulledBackIntoTheMenuIsScrolledIntoView(t *testing.T) {
	h := newScrollHarness(6, 40, 100)
	h.frame()
	h.frame(keyTab)
	h.frame(keyTab)
	h.frame(keyTab)
	before := h.offset()
	if before == 0 {
		t.Fatal("the list should be scrolled by now; the rest of this test would prove nothing")
	}

	// A tag the card never draws: Router.Frame runs the FocusCmd and then drops
	// it for want of a handler, which leaves focus on nothing — the state drive's
	// last branch exists for.
	h.grab = new(int)
	h.frame()
	if got := h.focused(); got != -1 {
		t.Fatalf("row %d holds focus, but it was handed to a tag this frame never drew", got)
	}
	if got := h.offset(); got != before {
		t.Fatalf("offset moved to %d on a frame the menu moved no focus on", got)
	}
	h.frame()
	if got := h.focused(); got != 0 {
		t.Fatalf("focus came back to row %d, want 0", got)
	}
	h.onScreen(t, 0)
}

// A menu that fits keeps the offset it was handed, including one a longer
// sub-view left behind. layout.List clamps an offset its content cannot honour
// when it lays that content out, so there is nothing here to correct — and
// correcting it anyway would have the keyboard contract writing to a list on
// frames where there is no row it needs to see.
func TestAFittingMenuKeepsTheOffsetItWasGiven(t *testing.T) {
	rows := []menuRow{{tag: new(int), top: 0, bot: 40}, {tag: new(int), top: 40, bot: 200}}
	s := menuScroll{rows: rows, view: 400, y: 200, ok: true}
	l := layout.List{Axis: layout.Vertical, Position: layout.Position{Offset: 60}}
	if s.into(&l, rows[1].tag) {
		t.Fatal("into scrolled a menu with nothing off screen")
	}
	if got := l.Position.Offset; got != 60 {
		t.Fatalf("offset = %d, want 60 — the list was left where somebody else put it", got)
	}
}

// menuOverlayHarness drives the REAL overlays — layoutContextMenuOverlay and
// layoutMsgContextMenuOverlay — in a window too short for their menus.
//
// scrollHarness above pins the mechanism; this pins the WIRING. Every piece of
// the fix that lives outside menuScroll is only reachable from here: the
// begin at the List.Layout call site inside contextMenuCard, the into after the
// measuring macro, and the tags that layoutContextMenuItems hands row against
// the ones peerMenuItems hands drive. Those two lists are written out
// separately and would drift apart in silence — the spans would describe rows
// that focus never visits, and into would find no match and decline, which
// looks exactly like a menu that fits.
type menuOverlayHarness struct {
	w      *Window
	router *input.Router
	ops    *op.Ops
	layout func(layout.Context) layout.Dimensions
	list   *layout.List
	scroll *menuScroll
	height int
}

func newOverlayHarness(msg bool, height int) *menuOverlayHarness {
	w := &Window{theme: newAppTheme()}
	// newWindow sets these; a Window built field-by-field would otherwise get
	// the zero Axis, which is Horizontal, and measure the card against the
	// cross constraint instead of scrolling it.
	w.ctxMenuList.Axis = layout.Vertical
	w.msgCtxMenuList.Axis = layout.Vertical
	h := &menuOverlayHarness{w: w, router: new(input.Router), ops: new(op.Ops), height: height}
	if msg {
		// The message menu's Delete row is actionable whenever a menu is
		// open and a router exists to carry the action out, so the
		// harness needs one to lay out the real set of rows.
		w.router = &service.DMRouter{}
		w.msgContextMsg = &service.DirectMessage{ID: "m1", Body: "hi"}
		w.msgContextPos = image.Pt(300, 400)
		w.msgMenuFocus.open(new(int))
		h.layout = w.layoutMsgContextMenuOverlay
		h.list, h.scroll = &w.msgCtxMenuList, &w.msgCtxMenuScroll
		return h
	}
	var peer domain.PeerIdentity
	copy(peer[:], "11ab110000000000000000000000000000000000")
	w.contextMenuPeer = peer
	w.contextMenuPos = image.Pt(300, 400)
	w.peerMenuFocus.open(new(int))
	h.layout = w.layoutContextMenuOverlay
	h.list, h.scroll = &w.ctxMenuList, &w.ctxMenuScroll
	return h
}

func (h *menuOverlayHarness) frame(keys ...key.Event) {
	for _, k := range keys {
		h.router.Queue(k)
	}
	h.ops.Reset()
	h.layout(layout.Context{
		Ops:         h.ops,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, h.height)},
		Source:      h.router.Source(),
	})
	h.router.Frame(h.ops)
}

// overflows fails the test unless the menu really is taller than the room it
// was given. Without it every assertion below would still pass on a window
// that turned out to be big enough — and pass just as well with the fix ripped
// out.
func (h *menuOverlayHarness) overflows(t *testing.T) {
	t.Helper()
	if !h.scroll.ok {
		t.Fatal("the card was not measured, so this test proves nothing")
	}
	if h.scroll.y <= h.scroll.view {
		t.Fatalf("the menu fits (%d content in %d viewport): this test needs one that does not",
			h.scroll.y, h.scroll.view)
	}
}

func (h *menuOverlayHarness) focused(tag event.Tag) bool {
	return h.router.Source().Focused(tag)
}

// redrew reports whether the frame just laid out asked for another one.
//
// The offset is applied AFTER the card has been measured, so the frame that
// scrolls is not the frame that shows the result — and a keyboard user
// pressing Tab generates nothing that would draw the next one by itself. An
// invalidate that goes missing therefore leaves the menu looking exactly like
// the bug this whole file is about, until some unrelated event redraws it.
func (h *menuOverlayHarness) redrew() bool {
	_, ok := h.router.WakeupTime()
	return ok
}

// onScreen is the contract itself, asked of the real card: the row focus is on
// lies inside the viewport the List is currently showing. Asserting that rather
// than a pixel offset is what keeps this test about the defect — the menus'
// row heights are free to change without anyone having to re-derive a number
// here, and a row left half-cut still fails.
func (h *menuOverlayHarness) onScreen(t *testing.T, what string, tag event.Tag) {
	t.Helper()
	off := h.list.Position.Offset
	for _, r := range h.scroll.rows {
		if r.tag != tag {
			continue
		}
		if r.top < off || r.bot > off+h.scroll.view {
			t.Fatalf("%s is at %d..%d, outside the visible %d..%d",
				what, r.top, r.bot, off, off+h.scroll.view)
		}
		return
	}
	t.Fatalf("%s was never measured, so nothing could have scrolled to it", what)
}

func TestTheRealMenusScrollTheirFocusedRowIntoView(t *testing.T) {
	t.Run("identity menu", func(t *testing.T) {
		h := newOverlayHarness(false, 120)
		h.frame()
		h.overflows(t)
		if !h.focused(&h.w.ctxMenuAlias) {
			t.Fatal("an open menu must focus its first row")
		}
		if got := h.list.Position.Offset; got != 0 {
			t.Fatalf("open offset = %d, want 0", got)
		}
		// Delete is the row this matters for: it sits at the bottom, below the
		// fold, and it is the one Enter must not activate unseen.
		h.frame(keyTab)
		h.frame(keyTab)
		if !h.focused(&h.w.ctxMenuDelete) {
			t.Fatal("two Tabs from the first row must land on Delete")
		}
		if h.list.Position.Offset == 0 {
			t.Fatal("the list never moved: focus walked below the fold on its own")
		}
		if !h.redrew() {
			t.Fatal("the frame that scrolled did not ask to be drawn again")
		}
		h.onScreen(t, "Delete", &h.w.ctxMenuDelete)

		// And a frame that changes nothing must not ask for another: an
		// invalidate on every frame of an open menu is a spin.
		h.frame()
		if h.redrew() {
			t.Fatal("a frame that scrolled nothing asked for a redraw")
		}
		// And back, so the walk is not one-way.
		h.frame(keyShiftTab)
		h.frame(keyShiftTab)
		if !h.focused(&h.w.ctxMenuAlias) {
			t.Fatal("two Shift+Tabs must come back to the first row")
		}
		h.onScreen(t, "Set alias", &h.w.ctxMenuAlias)
	})

	t.Run("message menu", func(t *testing.T) {
		h := newOverlayHarness(true, 60)
		h.frame()
		h.overflows(t)
		if !h.focused(&h.w.msgCtxReply) {
			t.Fatal("an open menu must focus its first row")
		}
		if got := h.list.Position.Offset; got != 0 {
			t.Fatalf("open offset = %d, want 0", got)
		}
		// Delete is the last row and is always actionable, so Shift+Tab
		// off the top wraps straight to it — below the fold, which is
		// exactly the row this scrolling exists for.
		h.frame(keyShiftTab)
		if !h.focused(&h.w.msgCtxDelete) {
			t.Fatal("Shift+Tab from the first row must wrap to the last")
		}
		if h.list.Position.Offset == 0 {
			t.Fatal("the list never moved: focus wrapped to a row below the fold")
		}
		h.onScreen(t, "Delete message", &h.w.msgCtxDelete)
	})
}
