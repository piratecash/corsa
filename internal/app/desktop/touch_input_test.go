package desktop

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"image"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/font"
	"gioui.org/font/gofont"
	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestMain injects a deterministic non-zero window handle so requestTouchKeyboard
// exercises its show path regardless of platform (the real one returns the
// foreground HWND, which a headless test process does not own).
func TestMain(m *testing.M) {
	activeWindowHandleHook = func() uintptr { return 1 }
	os.Exit(m.Run())
}

func longPressCtx(now time.Time) layout.Context {
	router := new(input.Router)
	return layout.Context{
		Ops:    new(op.Ops),
		Now:    now,
		Source: router.Source(),
	}
}

func touchPress(id pointer.ID, pos f32.Point) pointer.Event {
	return pointer.Event{
		Kind:      pointer.Press,
		Source:    pointer.Touch,
		Buttons:   pointer.ButtonPrimary,
		PointerID: id,
		Position:  pos,
	}
}

func TestLongPressFiresAfterHold(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	cursor := image.Pt(100, 200)

	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, cursor)
	if !rc.touchDown {
		t.Fatal("touch press should arm long-press tracking")
	}
	if rc.pressCursor != cursor {
		t.Fatal("press must capture the window-level cursor position")
	}

	// Not matured yet: no trigger.
	if rc.longPressTriggered(longPressCtx(start.Add(longPressDuration / 2))) {
		t.Fatal("long press must not fire before longPressDuration")
	}

	// Matured: fires exactly once.
	if !rc.longPressTriggered(longPressCtx(start.Add(longPressDuration))) {
		t.Fatal("long press should fire after longPressDuration")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("long press must not fire twice for one hold")
	}
}

func TestLongPressCancelledByMovement(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	slop := float32(20)
	cursor := image.Pt(0, 0)

	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, slop, cursor)

	// Small wiggle within slop keeps tracking alive.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Drag, Source: pointer.Touch, PointerID: 1, Position: f32.Pt(15, 12),
	}, start, slop, cursor)
	if !rc.touchDown {
		t.Fatal("movement within slop must not cancel the hold")
	}

	// Scroll-sized movement cancels.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Drag, Source: pointer.Touch, PointerID: 1, Position: f32.Pt(60, 10),
	}, start, slop, cursor)
	if rc.touchDown {
		t.Fatal("movement beyond slop must cancel the hold")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("cancelled hold must not fire")
	}
}

func TestLongPressFiresWhenReleaseMaturesInSameFrame(t *testing.T) {
	rc := new(rightClickState)
	frame := time.Unix(1000, 0) // one delayed frame: press+release both land here

	// Press: event time 0.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(10, 10), Time: 0,
	}, frame, 20, image.Pt(10, 10))

	// Release in the SAME frame, but its EVENT time is longPressDuration later
	// (the frame was delayed and batched them). Frame-time maturity is 0, so
	// only event-time maturity can save it.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Release, Source: pointer.Touch, PointerID: 1, Time: longPressDuration,
	}, frame, 20, image.Pt(10, 10))

	if !rc.longPressTriggered(longPressCtx(frame)) {
		t.Fatal("a hold matured by event time must fire even when release lands in the same delayed frame")
	}
	if rc.longPressTriggered(longPressCtx(frame)) {
		t.Fatal("long press must fire exactly once")
	}

	// Contrast: a genuinely short tap (release well before longPressDuration)
	// in one frame must NOT fire.
	rc2 := new(rightClickState)
	rc2.handleTouchLongPress(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(10, 10), Time: 0,
	}, frame, 20, image.Pt(10, 10))
	rc2.handleTouchLongPress(pointer.Event{
		Kind: pointer.Release, Source: pointer.Touch, PointerID: 1, Time: longPressDuration / 5,
	}, frame, 20, image.Pt(10, 10))
	if rc2.longPressTriggered(longPressCtx(frame)) {
		t.Fatal("a short tap must not fire a long press")
	}
}

func TestLateReleaseDoesNotBypassMultiTouchGuard(t *testing.T) {
	w := &Window{touchPressPos: map[pointer.ID]image.Point{}}
	rc := new(rightClickState)
	frame := time.Unix(1000, 0) // one delayed frame batches all three events

	// Finger 1 press.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(10, 10), Time: 0,
	}, frame, 20, image.Pt(10, 10))
	// A second finger tapped concurrently this frame (scroll/zoom intent).
	w.multiTouchAt = frame
	// Late release of finger 1: event time is past longPressDuration, which
	// would set matured=true and clear touchDown.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Release, Source: pointer.Touch, PointerID: 1, Time: longPressDuration,
	}, frame, 20, image.Pt(10, 10))

	w.cancelLongPressOnMultiTouch(rc)
	if rc.longPressTriggered(longPressCtx(frame)) {
		t.Fatal("a second finger in the same delayed frame must cancel even a matured long press")
	}
}

func TestLongPressCancelledByRelease(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	cursor := image.Pt(0, 0)
	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, cursor)
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Release, Source: pointer.Touch, PointerID: 1,
	}, start, 20, cursor)
	if rc.touchDown {
		t.Fatal("Release must cancel the hold")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("hold cancelled by Release must not fire")
	}
}

func TestLongPressCancelledByGrabCancel(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)

	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, image.Pt(10, 10))

	// Gio broadcasts Cancel when a gesture grabs the pointer; the event
	// stream for this hold is over (its Release will never arrive), so
	// the hold must die — see the limitation note in handleTouchLongPress.
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Cancel, Source: pointer.Touch, PointerID: 1,
	}, start, 20, image.Pt(10, 10))
	if rc.touchDown {
		t.Fatal("Cancel must cancel the hold")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("cancelled hold must not fire")
	}
}

func TestLongPressIgnoresMouse(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	rc.handleTouchLongPress(pointer.Event{
		Kind:      pointer.Press,
		Source:    pointer.Mouse,
		Buttons:   pointer.ButtonPrimary,
		PointerID: 1,
		Position:  f32.Pt(10, 10),
	}, start, 20, image.Pt(0, 0))
	if rc.touchDown {
		t.Fatal("mouse press must not arm long-press tracking")
	}
}

func TestLongPressSecondFingerCancels(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	cursor := image.Pt(0, 0)

	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, cursor)
	// Second finger down: multi-touch is scroll/zoom intent, not a menu.
	rc.handleTouchLongPress(touchPress(2, f32.Pt(80, 80)), start, 20, cursor)
	if rc.touchDown {
		t.Fatal("second finger press must cancel the hold")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("multi-touch hold must not fire")
	}
}

func TestLongPressIgnoresOtherPointerEvents(t *testing.T) {
	rc := new(rightClickState)
	start := time.Unix(1000, 0)
	slop := float32(20)
	cursor := image.Pt(0, 0)

	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, slop, cursor)

	// Move and release of a DIFFERENT pointer must not disturb tracking
	// (its press already cancelled the hold in real event streams, but the
	// state machine itself must be ID-safe for reordered deliveries).
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Drag, Source: pointer.Touch, PointerID: 7, Position: f32.Pt(300, 300),
	}, start, slop, cursor)
	if !rc.touchDown {
		t.Fatal("drag of another pointer ID must not cancel the hold")
	}
	rc.handleTouchLongPress(pointer.Event{
		Kind: pointer.Release, Source: pointer.Touch, PointerID: 7,
	}, start, slop, cursor)
	if !rc.touchDown {
		t.Fatal("release of another pointer ID must not cancel the hold")
	}

	if !rc.longPressTriggered(longPressCtx(start.Add(longPressDuration))) {
		t.Fatal("hold must still fire after unrelated pointer noise")
	}
}

func TestPressAnchorPrefersPerPointerPosition(t *testing.T) {
	w := &Window{
		lastCursorPos:   image.Pt(500, 500),
		pointerPressPos: map[pointer.ID]pressPoint{3: {pos: image.Pt(40, 60)}},
	}
	if got := w.pressAnchor(pointer.Event{PointerID: 3}); got != image.Pt(40, 60) {
		t.Fatalf("anchor = %v, want the pointer's press position", got)
	}
	// Unknown pointer falls back to the global tracker position.
	if got := w.pressAnchor(pointer.Event{PointerID: 9}); got != image.Pt(500, 500) {
		t.Fatalf("anchor fallback = %v, want lastCursorPos", got)
	}
}

// keyboardInsetDp answers about the screen and only the screen: the whole
// published occlusion, bounded by the container, with nothing set aside for
// anybody's content.
func TestKeyboardInsetDpReportsTheWholeOcclusion(t *testing.T) {
	kbd := new(touchKeyboardState)
	gtx := layout.Context{
		Metric:      unit.Metric{PxPerDp: 2, PxPerSp: 2},
		Constraints: layout.Constraints{Max: image.Pt(800, 1000)}, // 500dp tall
	}
	if occ := keyboardInsetDp(gtx, kbd); occ != 0 {
		t.Fatalf("inset without occlusion = %v, want 0", occ)
	}
	kbd.publishOccludedDp(200)
	if occ := keyboardInsetDp(gtx, kbd); occ != unit.Dp(200) {
		t.Fatalf("inset = %v, want 200dp", occ)
	}
	// No reserve, and this is the case the reserve existed for. It answered
	// 404dp here so the composer would keep 96dp; only 50dp is physically
	// clear. It never lifted the composer a single dp — see
	// TestInputRowStaysAboveKeyboard — and every overlay reading the same
	// number was told about 96dp of room the keyboard was standing on.
	kbd.publishOccludedDp(450)
	if occ := keyboardInsetDp(gtx, kbd); occ != unit.Dp(450) {
		t.Fatalf("inset = %v, want the raw 450dp occlusion", occ)
	}
	// There is no floor either. A floor is the same bug one constant
	// smaller: reserving 48dp under a 490dp keyboard claims 48dp of room
	// where 10dp exists, so 38dp of a menu's only row is drawn under the
	// keyboard, visible and unable to receive a touch. menuOverlayRoom is
	// the single place that decides what to do about a degenerate answer.
	kbd.publishOccludedDp(490)
	if occ := keyboardInsetDp(gtx, kbd); occ != unit.Dp(490) {
		t.Fatalf("inset = %v, want the raw 490dp occlusion", occ)
	}
	// A container shorter than the keyboard is fully covered: the inset
	// saturates at its height, never exceeds it and never goes negative.
	small := gtx
	small.Constraints.Max = image.Pt(800, 60) // 30dp tall
	if occ := keyboardInsetDp(small, kbd); occ != unit.Dp(30) {
		t.Fatalf("inset in a fully covered container = %v, want 30dp", occ)
	}
}

// The chrome yields exactly when the strip the keyboard leaves free cannot
// hold both it and the tail the CONTENT measured, and it is laid out either
// way so it still reads its events on the frames it is not drawn on.
func TestKeyboardYieldingChromeDropsChromeOnlyWhenTheStripIsShort(t *testing.T) {
	kbd := new(touchKeyboardState)
	const chromeDp = 60
	// What a real frame reports: a 24dp conversation label over a composer
	// card that costs 120dp once its send-status and reply rows are counted.
	const tailDp = 144
	laidOut := 0
	chrome := func(gtx layout.Context) layout.Dimensions {
		laidOut++
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, gtx.Dp(chromeDp))}
	}
	ctx := func(heightDp int) layout.Context {
		return layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(400, heightDp)},
		}
	}
	// Nothing measured yet: the chrome stays whatever the keyboard does.
	// Yielding destroys a header, so it needs evidence, not the lack of it.
	kbd.publishOccludedDp(450)
	if dims := keyboardYieldingChrome(ctx(500), kbd, chrome); dims.Size.Y != chromeDp {
		t.Fatalf("chrome before any tail was measured = %v, want it kept", dims.Size.Y)
	}
	kbd.noteTailPx(tailDp)
	endTailFrame(kbd)

	kbd.publishOccludedDp(0)
	if dims := keyboardYieldingChrome(ctx(300), kbd, chrome); dims.Size.Y != chromeDp {
		t.Fatalf("chrome without a keyboard = %v, want %ddp", dims.Size.Y, chromeDp)
	}
	// 500 - 296 = 204 free, exactly 60 of chrome and the 144 that was
	// measured: the last height at which the header can stay.
	kbd.publishOccludedDp(296)
	if dims := keyboardYieldingChrome(ctx(500), kbd, chrome); dims.Size.Y != chromeDp {
		t.Fatalf("chrome with room = %v, want %ddp", dims.Size.Y, chromeDp)
	}
	// 500 - 297 = 203 free, one dp short: the chrome goes. The constant this
	// replaced held the header until 344 because it believed the row below
	// was 96dp tall, and every height in between drew the input row under
	// the keyboard — 330 among them, which is the r70 finding.
	kbd.publishOccludedDp(297)
	if dims := keyboardYieldingChrome(ctx(500), kbd, chrome); dims.Size.Y != 0 {
		t.Fatalf("chrome on a short strip = %v, want it to yield", dims.Size.Y)
	}
	// Four calls, four layouts, including the one that was not drawn.
	// Dropping the DRAW and not the layout is what keeps the chrome's own
	// state (clicks, hovers, editors) advancing on the frames where it is
	// invisible, so it does not come back stale.
	if laidOut != 4 {
		t.Fatalf("chrome laid out %d times, want 4 — it must run every frame", laidOut)
	}
}

// endTailFrame closes a measured frame in the tests that only care about the
// number. The ones that care about the redraw it asks for run a router: see
// TestTailChangeAsksForTheFrameThatActsOnIt.
func endTailFrame(kbd *touchKeyboardState) {
	kbd.endTailFrame(layout.Context{Ops: new(op.Ops)})
}

// A frame that measured a different tail must ask for the frame that acts on
// it. Nothing else will: Gio draws in response to input, and the paste or the
// send-status row that changed the height has already been drawn.
func TestTailChangeAsksForTheFrameThatActsOnIt(t *testing.T) {
	router := new(input.Router)
	ops := new(op.Ops)
	frame := func(kbd *touchKeyboardState, tailPx int) bool {
		ops.Reset()
		gtx := layout.Context{Ops: ops, Source: router.Source()}
		if tailPx > 0 {
			kbd.noteTailPx(tailPx)
		}
		kbd.endTailFrame(gtx)
		router.Frame(ops)
		_, wake := router.WakeupTime()
		return wake
	}
	kbd := new(touchKeyboardState)
	if !frame(kbd, 144) {
		t.Fatal("the frame that measured a new tail must request the next one — " +
			"app.Window.Invalidate is a no-op mid-frame, so this has to be an InvalidateCmd")
	}
	if !frame(kbd, 200) {
		t.Fatal("the composer grew (a pasted line, a send-status row): the header decides on the tail, so that frame must be asked for")
	}
	if frame(kbd, 200) {
		t.Fatal("an unchanged tail changes no decision and must not spin the frame loop")
	}
	if !frame(kbd, 0) {
		t.Fatal("the rows stopped being laid out: the chrome comes back, and that frame must be asked for too")
	}
}

// The tail is what the last COMPLETED frame measured: readable all through the
// frame that acts on it, replaced only at a frame boundary, and never carried
// over from a frame in which nobody measured anything.
func TestKeyboardTailPublishesOneFrameBehind(t *testing.T) {
	kbd := new(touchKeyboardState)

	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 0 {
		t.Fatalf("tail before anything was laid out = %d, want 0", got)
	}
	kbd.noteTailPx(24)
	kbd.noteTailPx(120)
	if got := kbd.requiredTailPx(); got != 0 {
		t.Fatalf("tail = %d during the frame that measures it, want the last frame's 0 — "+
			"the chrome decided before these rows existed", got)
	}
	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 144 {
		t.Fatalf("tail on the next frame = %d, want 144", got)
	}
	// The same measurement again changes no decision and must not spin the
	// frame loop.
	kbd.noteTailPx(144)
	endTailFrame(kbd)
	if got, want := kbd.requiredTailPx(), 144; got != want {
		t.Fatalf("tail = %d, want %d", got, want)
	}
	// The panel stops being laid out (tab switched away, window closing): the
	// tail goes back to "not measured" rather than protecting rows that are
	// no longer on screen.
	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 0 {
		t.Fatalf("tail after a frame that measured nothing = %d, want 0", got)
	}
}

// A row is laid out inside a panel the keyboard has already shrunk, so the
// height it draws at is a measure of the squeeze and not of the row. Measuring
// that number would let the chrome confirm its own decision forever, which is
// the r70 bug wearing the refactor's clothes.
func TestKeyboardTailRowMeasuresWhatTheRowNeedsNotTheSqueeze(t *testing.T) {
	kbd := new(touchKeyboardState)
	kbd.publishOccludedDp(330)
	ranAt := []int{}
	// A card: 120dp when it can have it, clamped to the room it is offered
	// when it cannot — which is what layout.Flex does to its own result.
	row := func(gtx layout.Context) layout.Dimensions {
		h := min(gtx.Dp(120), gtx.Constraints.Max.Y)
		ranAt = append(ranAt, gtx.Constraints.Max.Y)
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, h)}
	}
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 72)},
	}
	if dims := keyboardTailRow(kbd, row)(gtx); dims.Size.Y != 72 {
		t.Fatalf("the row drew %ddp tall, want the 72 it was offered — the wrapper must not change the layout", dims.Size.Y)
	}
	if want := []int{72 + 330, 72}; len(ranAt) != 2 || ranAt[0] != want[0] || ranAt[1] != want[1] {
		t.Fatalf("row ran at %v, want %v — measure with the keyboard's room handed back, then draw for real last "+
			"so the widget ends the frame holding the state of the pass that was shown", ranAt, want)
	}
	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 120 {
		t.Fatalf("measured tail = %d, want 120: 72 is how far the keyboard squeezed the row, and a chrome that reads it keeps its place on exactly the frames it has to give it up", got)
	}
}

// With nothing occluded there is no squeeze to undo, and the row must be laid
// out exactly once — these are editors and buttons, not free functions.
func TestKeyboardTailRowLaysTheRowOutOnceWithNoKeyboard(t *testing.T) {
	kbd := new(touchKeyboardState)
	runs := 0
	row := func(gtx layout.Context) layout.Dimensions {
		runs++
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, gtx.Dp(120))}
	}
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 400)},
	}
	keyboardTailRow(kbd, row)(gtx)
	if runs != 1 {
		t.Fatalf("row laid out %d times with no keyboard, want 1", runs)
	}
	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 120 {
		t.Fatalf("tail = %d, want 120", got)
	}
}

// keyboardMeasureTail counts a widget without letting it touch the frame: no
// events reach it and nothing it draws or registers survives.
func TestKeyboardMeasureTailCountsWithoutDrawing(t *testing.T) {
	type measuredTagT struct{}
	tag := new(measuredTagT)
	kbd := new(touchKeyboardState)
	router := new(input.Router)
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Min: image.Pt(400, 300), Max: image.Pt(400, 300)},
	}
	keyboardMeasureTail(gtx, kbd, func(gtx layout.Context) layout.Dimensions {
		if gtx.Enabled() {
			t.Error("the measuring pass must deliver no events")
		}
		if gtx.Constraints.Min.Y != 0 {
			t.Errorf("measuring Min.Y = %d, want 0 — a container asked for a minimum reports the minimum, not its content", gtx.Constraints.Min.Y)
		}
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return tapTarget(gtx, tag, image.Pt(gtx.Constraints.Max.X, 40))
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		)
	})
	endTailFrame(kbd)
	if got := kbd.requiredTailPx(); got != 52 {
		t.Fatalf("measured tail = %d, want 52 (40 + 12)", got)
	}
	router.Frame(ops)
	if span := probeColumns(router, 100, 300, tag)[0]; span.top >= 0 {
		t.Fatalf("the measured widget answers taps at rows %d..%d — its macro must be dropped", span.top, span.bottom)
	}
}

// tapTarget registers a hit area exactly the size of the widget so a probe can
// find where it was DRAWN. Nothing here clips, which is the point: a Rigid
// child that outgrows its Flex is drawn past the bottom of the box, and that
// overflow is precisely what has to be measured.
func tapTarget(gtx layout.Context, tag event.Tag, size image.Point) layout.Dimensions {
	defer clip.Rect(image.Rectangle{Max: size}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, tag)
	gtx.Event(pointer.Filter{Target: tag, Kinds: pointer.Press})
	return layout.Dimensions{Size: size}
}

// probeSpan is the [top, bottom) range of window rows in which a tap reaches a
// tag, or (-1, -1) when none does.
type probeSpan struct {
	top    int
	bottom int
}

// probeColumns taps every row of the frame at x and reports, per tag, the rows
// that reach it. It reads the frame the layout produced instead of repeating
// the layout's arithmetic.
//
// All the tags are drained on every row, and they have to be: the router keeps
// an event until somebody filters for it, so a pass that drained one tag at a
// time would find the previous pass's undelivered presses waiting at the first
// row it looked at and report a hit there.
func probeColumns(router *input.Router, x, height int, tags ...event.Tag) []probeSpan {
	spans := make([]probeSpan, len(tags))
	for i := range spans {
		spans[i] = probeSpan{top: -1, bottom: -1}
	}
	for y := 0; y < height; y++ {
		pos := f32.Pt(float32(x)+0.5, float32(y)+0.5)
		router.Queue(
			pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary, PointerID: 1, Position: pos},
			pointer.Event{Kind: pointer.Release, Source: pointer.Touch, PointerID: 1, Position: pos},
		)
		for i, tag := range tags {
			hit := false
			for {
				if _, ok := router.Event(pointer.Filter{Target: tag, Kinds: pointer.Press}); !ok {
					break
				}
				hit = true
			}
			if !hit {
				continue
			}
			if spans[i].top < 0 {
				spans[i].top = y
			}
			spans[i].bottom = y + 1
		}
	}
	return spans
}

// The stand-in window is 500dp tall at 1px/dp, with the same 4/6dp margins as
// window.go. The row heights are the real ones' order of magnitude: a header
// of 60 with its 6dp spacer, a per-conversation label of 24, and a composer of
// 120 — which is what layoutComposerCard really costs once its send-status and
// reply rows are counted. The label and the composer are marked with
// keyboardTailRow here exactly as layoutMain marks them, so the probe exercises
// the measurement and not a number written down twice.
const (
	probeWinDp    = 500
	probeChromeDp = 60
	probeSpacerDp = 6
	probeLabelDp  = 24
	probeInputDp  = 120
)

// layoutProbeFrames lays the stand-in window out frames times into one router
// and returns it for probing. Two is the smallest number that says anything
// about the steady state: the chrome yields on the tail the PREVIOUS frame
// measured, so frame 1 is always the "nothing measured yet" frame in which it
// stays put no matter how tall the keyboard is.
func layoutProbeFrames(kbd *touchKeyboardState, chromeTag, inputTag event.Tag, frames int) *input.Router {
	router := new(input.Router)
	for i := 0; i < frames; i++ {
		layoutProbeFrame(router, kbd, chromeTag, inputTag)
	}
	return router
}

// layoutProbeFrame reproduces window.go's chrome/content chain with stand-in
// widgets and records one frame into router.
func layoutProbeFrame(router *input.Router, kbd *touchKeyboardState, chromeTag, inputTag event.Tag) {
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, probeWinDp)},
	}
	inset := layout.Inset{
		Top:    unit.Dp(4),
		Bottom: unit.Dp(4),
		Left:   unit.Dp(6),
		Right:  unit.Dp(6),
	}
	inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return keyboardYieldingChrome(gtx, kbd, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{
						Axis: layout.Vertical,
					}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return tapTarget(gtx, chromeTag, image.Pt(gtx.Constraints.Max.X, gtx.Dp(probeChromeDp)))
						}),
						layout.Rigid(layout.Spacer{Height: unit.Dp(probeSpacerDp)}.Layout),
					)
				})
			}),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{
					Bottom: keyboardInsetDp(gtx, kbd),
				}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{
						Axis: layout.Vertical,
					}.Layout(gtx,
						layout.Rigid(keyboardTailRow(kbd, layout.Spacer{Height: unit.Dp(probeLabelDp)}.Layout)),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return layout.Dimensions{Size: gtx.Constraints.Max}
						}),
						layout.Rigid(keyboardTailRow(kbd, func(gtx layout.Context) layout.Dimensions {
							// Like a real card, and this clamp is the whole
							// point: the row draws at its own height but
							// REPORTS only the room it was offered, because
							// layout.Flex ends in cs.Constrain(sz). Measuring
							// that reported height is what let the header
							// confirm its own decision at 330dp. The tail hands
							// the keyboard's room back before asking, so it
							// sees the full 120 while this pass sees the
							// squeeze.
							dims := tapTarget(gtx, inputTag, image.Pt(gtx.Constraints.Max.X, gtx.Dp(probeInputDp)))
							dims.Size = gtx.Constraints.Constrain(dims.Size)
							return dims
						})),
					)
				})
			}),
		)
	})
	// At the END, where window.go and console_window.go do it — the frame that
	// measures a new tail is the frame that has to ask for the next one.
	kbd.endTailFrame(gtx)
	router.Frame(ops)
}

// probeExpect is the layout above worked out by hand, so the sweep compares two
// independent derivations rather than the code with itself. Everything here is
// in window coordinates: 4dp of margin, then the chrome if it was kept, then
// the content panel with the keyboard's bite taken out of its bottom.
func probeExpect(occl int) (chromeKept bool, inputTop, inputBottom int) {
	const box = probeWinDp - 8                    // the window inside its margins
	const chromeH = probeChromeDp + probeSpacerDp // header plus its spacer
	const tail = probeLabelDp + probeInputDp      // what keyboardTailRow measures

	// keyboardInsetDp answers about the container, so the yield decision sees
	// an occlusion clamped to the whole box.
	occlC := occl
	if occlC > box {
		occlC = box
	}
	chromeKept = occlC == 0 || box-occlC >= chromeH+tail
	chromeEff := 0
	if chromeKept {
		chromeEff = chromeH
	}
	// The panel keeps whatever the keyboard does not cover. Below the tail the
	// inner Flex overflows instead of shrinking: the input keeps its height and
	// is drawn past the panel's bottom edge, which is what keeps the TOP of the
	// row reachable when nothing else can be saved. The label above it is a
	// Spacer and Spacers DO obey their constraints, so on a panel thinner than
	// the label the row starts even higher — that is the one place where the
	// measured tail over-states what the layout will really spend, and it errs
	// by keeping the row further from the keyboard.
	inner := box - chromeEff - occl
	if inner < 0 {
		inner = 0
	}
	labelH := probeLabelDp
	if inner < labelH {
		labelH = inner
	}
	gap := inner - labelH - probeInputDp // what the scrolling middle row gets
	if gap < 0 {
		gap = 0
	}
	inputTop = 4 + chromeEff + labelH + gap
	return chromeKept, inputTop, inputTop + probeInputDp
}

// This is the r69/r70 test: not padding arithmetic and not a handful of sampled
// heights, but the input row's actual coordinates against the keyboard's top
// edge at EVERY occlusion the window can see, taken by tapping the frame. The
// four-case version of this test passed while 330dp was broken, because it
// jumped from 200 straight to 350.
func TestInputRowStaysAboveKeyboard(t *testing.T) {
	type chromeTagT struct{}
	type inputTagT struct{}
	// Above this the window physically cannot hold the row: 4dp of margin plus
	// a 144dp tail is 148dp, and the keyboard's top edge reaches it at 352.
	const lastClearOccl = probeWinDp - (4 + probeLabelDp + probeInputDp)
	for occl := 0; occl <= probeWinDp; occl++ {
		kbd := new(touchKeyboardState)
		kbd.publishOccludedDp(int32(occl))
		chromeTag, inputTag := new(chromeTagT), new(inputTagT)
		router := layoutProbeFrames(kbd, chromeTag, inputTag, 2)

		spans := probeColumns(router, 100, probeWinDp, chromeTag, inputTag)
		chrome, in := spans[0], spans[1]
		wantChrome, wantTop, wantBottom := probeExpect(occl)

		if got := chrome.top >= 0; got != wantChrome {
			t.Fatalf("occl %d: chrome drawn = %v (rows %d..%d), want %v",
				occl, got, chrome.top, chrome.bottom, wantChrome)
		}
		if wantChrome && (chrome.top != 4 || chrome.bottom != 4+probeChromeDp) {
			t.Fatalf("occl %d: chrome at %d..%d, want %d..%d",
				occl, chrome.top, chrome.bottom, 4, 4+probeChromeDp)
		}
		if in.top != wantTop || in.bottom != wantBottom {
			t.Fatalf("occl %d: input row at %d..%d, want %d..%d",
				occl, in.top, in.bottom, wantTop, wantBottom)
		}
		kbdTop := probeWinDp - occl
		clear := in.bottom <= kbdTop
		if want := occl <= lastClearOccl; clear != want {
			t.Fatalf("occl %d: row %d..%d against a keyboard starting at %d: clear = %v, want %v",
				occl, in.top, in.bottom, kbdTop, clear, want)
		}
	}
}

// The r70 finding, kept as its own case so the number in the review report is
// findable in the code. The old reserve believed the row below the header was
// 96dp tall, so at 330dp of occlusion it left the header up: 66dp of chrome
// plus a 96dp promise fits in the 162dp strip. The row is really 144dp, it was
// laid out at 94..214, and the keyboard's edge is at 170 — the bottom 44dp of
// it took no taps. Measuring the row instead of promising it yields the header
// here, which lifts the row by the header's whole height.
func TestHeaderYieldsAtTheOcclusionTheReserveMissed(t *testing.T) {
	type chromeTagT struct{}
	type inputTagT struct{}
	kbd := new(touchKeyboardState)
	kbd.publishOccludedDp(330)
	chromeTag, inputTag := new(chromeTagT), new(inputTagT)
	router := layoutProbeFrames(kbd, chromeTag, inputTag, 2)

	spans := probeColumns(router, 100, probeWinDp, chromeTag, inputTag)
	chrome, in := spans[0], spans[1]
	if chrome.top >= 0 {
		t.Fatalf("chrome drawn at %d..%d, want it to yield at 330dp", chrome.top, chrome.bottom)
	}
	if in.top != 46 || in.bottom != 166 {
		t.Fatalf("input row at %d..%d, want 46..166 (it was 94..214 under the reserve)", in.top, in.bottom)
	}
	if kbdTop := probeWinDp - 330; in.bottom > kbdTop {
		t.Fatalf("input row bottom %d is under a keyboard starting at %d", in.bottom, kbdTop)
	}
}

// The empirical half of the no-oscillation argument at keyboardYieldingChrome:
// once the tail has been measured the layout must not move again, however many
// frames are drawn at the same occlusion.
// The first frame measures rows nobody had measured before, and the decision
// that reads them belongs to the next frame. That frame has to be REQUESTED
// while this one is still being laid out: Gio draws in response to input, and
// the input that brought the keyboard up has already been drawn. Asserted on
// the probe rather than on the state alone, because what this pins is WHERE
// the publish happens — from the top of the frame the request would be made
// one frame too late, which is the r71 finding.
func TestProbeWindowAsksForTheFrameItsMeasurementNeeds(t *testing.T) {
	type chromeTagT struct{}
	type inputTagT struct{}
	kbd := new(touchKeyboardState)
	kbd.publishOccludedDp(330)
	router := new(input.Router)
	chromeTag, inputTag := new(chromeTagT), new(inputTagT)

	layoutProbeFrame(router, kbd, chromeTag, inputTag)
	if _, wake := router.WakeupTime(); !wake {
		t.Fatal("the frame that first measured the label and the composer must ask for the frame that acts on them — " +
			"otherwise the composer stays under the keyboard until something unrelated redraws")
	}
	// And exactly one is owed, ever. The second frame is the one that yields
	// the chrome, which hands the composer more room — but the rows are measured
	// with the keyboard's room already handed back, so what they report does not
	// depend on the decision it feeds. The tail comes out at the same 144 and
	// the loop is allowed to go back to sleep.
	layoutProbeFrame(router, kbd, chromeTag, inputTag)
	if _, wake := router.WakeupTime(); wake {
		t.Fatal("the tail did not change on the frame that acted on it: the loop must be allowed to sleep, not spin")
	}
}

// A window whose panel has no protected rows at all keeps its chrome, whatever
// the keyboard does. This is the console's other tabs — Peers, Traffic, Files,
// Info, Donate carry no input, so a keyboard raised by hand over one of them
// must not cost the user the tab strip, which is the only navigation they
// have. No tab check does this: the tail simply reports nothing.
func TestChromeStaysOnAPanelWithNoInputRow(t *testing.T) {
	kbd := new(touchKeyboardState)
	kbd.publishOccludedDp(300)
	chrome := func(gtx layout.Context) layout.Dimensions {
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, gtx.Dp(60))}
	}
	ctx := func() layout.Context {
		return layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(400, 400)},
		}
	}
	// The console tab: the input row reports, and 100dp of free strip holds
	// neither it nor the tabs above it.
	kbd.noteTailPx(144)
	endTailFrame(kbd)
	if dims := keyboardYieldingChrome(ctx(), kbd, chrome); dims.Size.Y != 0 {
		t.Fatalf("tab strip = %ddp with an input row that does not fit, want it to yield", dims.Size.Y)
	}
	// A tab with no input at all: this frame measures nothing.
	endTailFrame(kbd)
	if dims := keyboardYieldingChrome(ctx(), kbd, chrome); dims.Size.Y != 60 {
		t.Fatalf("tab strip = %ddp on a tab with no input row, want it kept at 60 — "+
			"a hand-raised keyboard there takes the only navigation away and buys nothing", dims.Size.Y)
	}
}

func TestProbeWindowSettlesAfterTheFirstFrame(t *testing.T) {
	type chromeTagT struct{}
	type inputTagT struct{}
	for _, occl := range []int32{0, 200, 282, 283, 330, 352, 353, 440, 500} {
		settled := func(frames int) [2]probeSpan {
			kbd := new(touchKeyboardState)
			kbd.publishOccludedDp(occl)
			chromeTag, inputTag := new(chromeTagT), new(inputTagT)
			router := layoutProbeFrames(kbd, chromeTag, inputTag, frames)
			spans := probeColumns(router, 100, probeWinDp, chromeTag, inputTag)
			return [2]probeSpan{spans[0], spans[1]}
		}
		if two, five := settled(2), settled(5); two != five {
			t.Fatalf("occl %d: frame 2 laid out %v but frame 5 laid out %v — the decision is oscillating",
				occl, two, five)
		}
	}
}

func TestKeyboardDebounceBlocks(t *testing.T) {
	win := int64(keyboardShowDebounce)
	// Within the window after a real prior show → blocked.
	if !keyboardDebounceBlocks(1000+win-1, 1000) {
		t.Fatal("a second show inside the debounce window must be blocked")
	}
	// At/after the window → allowed.
	if keyboardDebounceBlocks(1000+win, 1000) {
		t.Fatal("a show at the debounce boundary must be allowed")
	}
	// First show (last == 0) with a large now → allowed.
	if keyboardDebounceBlocks(1<<60, 0) {
		t.Fatal("the first show must never be blocked")
	}
	// Backward clock step: now < last must be treated as a fresh request,
	// NOT blocked — otherwise every tap is rejected until the wall clock
	// climbs back past the stale stamp.
	if keyboardDebounceBlocks(500, 1_000_000_000) {
		t.Fatal("a backward clock step must not lock out the keyboard")
	}
	// Exactly equal timestamps (coarse clock) → treated as fresh, allowed.
	if keyboardDebounceBlocks(1000, 1000) {
		t.Fatal("now == last must be allowed, not blocked")
	}
}

func TestOutsideTapCancelResetsDebounce(t *testing.T) {
	// Scenario the reviewer flagged: editor → button → editor. The button
	// (outside tap) cancels the first show via showGen; if the debounce stamp
	// is not also cleared, the quick editor re-tap is swallowed and the editor
	// ends up focused with NO keyboard.
	kbd := new(touchKeyboardState)

	requestTouchKeyboard(kbd) // first editor tap
	g1 := kbd.showGen.Load()
	if g1 == 0 {
		t.Fatal("first tap must dispatch a show (bump showGen)")
	}
	if kbd.lastShow.Load() == 0 {
		t.Fatal("first tap must set the debounce stamp")
	}

	kbd.cancelPendingShow() // the outside tap on the button
	if kbd.lastShow.Load() != 0 {
		t.Fatal("cancel must clear the debounce stamp")
	}
	if kbd.showGen.Load() != g1+1 {
		t.Fatal("cancel must bump showGen to invalidate the in-flight show")
	}

	requestTouchKeyboard(kbd) // editor re-tap, well within the 300ms window
	if got := kbd.showGen.Load(); got != g1+2 {
		t.Fatalf("re-tap after an outside-tap cancel must dispatch a FRESH show: showGen=%d, want %d", got, g1+2)
	}

	// Contrast: without a cancel, a rapid re-tap is still debounced (the
	// debounce must keep collapsing duplicate dispatches from one physical
	// tap — the fix only affects the post-cancel case).
	other := new(touchKeyboardState)
	requestTouchKeyboard(other)
	// On a coarse wall clock (Windows serves time.Now from the shared-page
	// _SYSTEM_TIME, which the kernel advances once per clock interrupt, so
	// UnixNano moves in 0.5-15.6ms ticks) the re-tap below can draw the SAME
	// stamp as the dispatch above, and an equal stamp is deliberately read as
	// a fresh tap -- the backward-clock guard in keyboardDebounceBlocks,
	// because swallowing a real tap is the unrecoverable direction. Step the
	// stamp back one nanosecond so the re-tap reads now > last on any clock
	// and this assertion exercises the debounce, not the clock's resolution.
	other.lastShow.Store(other.lastShow.Load() - 1)
	before := other.showGen.Load()
	requestTouchKeyboard(other) // within 300ms, no cancel
	if other.showGen.Load() != before {
		t.Fatal("a rapid re-tap without a cancel must still be debounced")
	}
}

func TestRequestTouchKeyboardIgnoresForeignForeground(t *testing.T) {
	prev := activeWindowHandleHook
	defer func() { activeWindowHandleHook = prev }()

	kbd := new(touchKeyboardState)
	kbd.viewHwnd.Store(100) // this window's own handle (from ViewEvent)

	// A different window of ours is foreground → the tap is being processed by
	// the wrong window's state; it must NOT dispatch, rebind, OR invalidate
	// this window's pending hide (hideGen).
	activeWindowHandleHook = func() uintptr { return 200 }
	before := kbd.showGen.Load()
	beforeHide := kbd.hideGen.Load()
	requestTouchKeyboard(kbd)
	if kbd.showGen.Load() != before {
		t.Fatal("a tap while a DIFFERENT window is foreground must not dispatch a show")
	}
	if kbd.hideGen.Load() != beforeHide {
		t.Fatal("a foreign-window event must NOT bump hideGen (would cancel this window's pending hide)")
	}
	if kbd.hwnd.Load() != 0 {
		t.Fatalf("state must not be rebound to a foreign window's handle, got %d", kbd.hwnd.Load())
	}

	// This window is foreground → dispatch, binding to its OWN handle.
	activeWindowHandleHook = func() uintptr { return 100 }
	requestTouchKeyboard(kbd)
	if kbd.showGen.Load() == before {
		t.Fatal("a tap while THIS window is foreground must dispatch")
	}
	if kbd.hwnd.Load() != 100 {
		t.Fatalf("state must bind to its own handle, got %d", kbd.hwnd.Load())
	}
}

func TestPlaceMenuKeepsMenuAboveKeyboard(t *testing.T) {
	const windowW, availH = 400, 300 // usable height above the keyboard
	const menuW, menuH = 180, 120

	// Anchor comfortably inside the usable area → placed as-is.
	if x, y := placeMenu(50, 40, menuW, menuH, windowW, availH); x != 50 || y != 40 {
		t.Fatalf("in-bounds anchor moved: got (%d,%d), want (50,40)", x, y)
	}

	// Anchor near the bottom of the usable area → flips up, whole menu fits.
	_, y := placeMenu(50, 260, menuW, menuH, windowW, availH)
	if y+menuH > availH {
		t.Fatalf("menu bottom %d exceeds usable height %d (must flip up)", y+menuH, availH)
	}

	// Anchor BELOW the usable area (keyboard appeared under it): flipping up
	// leaves the menu bottom == anchor, still under the keyboard, so the final
	// clamp must pull the whole menu into the usable area.
	_, y = placeMenu(50, 350, menuW, menuH, windowW, availH) // 350 > availH(300)
	if y < 0 || y+menuH > availH {
		t.Fatalf("anchor below usable area: y=%d, menu bottom=%d still under keyboard (avail %d)", y, y+menuH, availH)
	}

	// Horizontal overflow flips left and clamps to >= 0.
	if x, _ := placeMenu(windowW-10, 40, menuW, menuH, windowW, availH); x != windowW-menuW {
		t.Fatalf("right overflow: x=%d, want %d", x, windowW-menuW)
	}
}

func TestBlurCancelsPendingShow(t *testing.T) {
	s := new(touchKeyboardState)
	s.showGen.Store(5) // a show is pending/in-flight with generation 5

	now := time.Unix(1000, 0)
	s.trackEditorFocus(longPressCtx(now), true)                                          // focus
	s.trackEditorFocus(longPressCtx(now), false)                                         // blur begins
	s.trackEditorFocus(longPressCtx(now.Add(keyboardHideDelay+time.Millisecond)), false) // blur confirmed

	if s.showGen.Load() == 5 {
		t.Fatal("a confirmed blur (past the hide delay) must invalidate the pending show generation")
	}
	if s.lastShow.Load() != 0 {
		t.Fatal("a confirmed blur must also reset the debounce stamp")
	}
}

func TestKeyboardSessionIdle(t *testing.T) {
	kbd := new(touchKeyboardState)
	if !keyboardSessionIdle(kbd) {
		t.Fatal("fresh state must be idle")
	}
	kbd.shownByUs.Store(true)
	if keyboardSessionIdle(kbd) {
		t.Fatal("owned session is not idle — a failing monitor must keep retrying")
	}
	kbd.shownByUs.Store(false)
	kbd.publishOccludedDp(240)
	if keyboardSessionIdle(kbd) {
		t.Fatal("published padding is not idle — someone must eventually clear it")
	}
	endKeyboardSession(kbd)
	if !keyboardSessionIdle(kbd) {
		t.Fatal("after endKeyboardSession the state must be idle")
	}
	// USER-opened keyboard: Showing fired (paneVisible), but shownByUs is
	// false and — floating, or before the first successful sample — the
	// padding is zero. That session is LIVE: a failing monitor must keep
	// retrying, or the composer stays under the keyboard forever (no new
	// Showing will ever come).
	kbd.paneVisible.Store(true)
	if keyboardSessionIdle(kbd) {
		t.Fatal("a live pane session (user-opened keyboard) must not read as idle")
	}
	endKeyboardSession(kbd)
	if !keyboardSessionIdle(kbd) {
		t.Fatal("endKeyboardSession must clear paneVisible too")
	}
}

func TestPublishOccludedDpInvalidatesOnChange(t *testing.T) {
	kbd := new(touchKeyboardState)
	calls := 0
	kbd.setInvalidate(func() { calls++ })
	kbd.publishOccludedDp(100)
	kbd.publishOccludedDp(100) // unchanged — must not wake the frame loop
	kbd.publishOccludedDp(0)
	if calls != 2 {
		t.Fatalf("invalidate calls = %d, want 2", calls)
	}
}

func TestRequestTouchKeyboardDebounceIsPerWindow(t *testing.T) {
	a, b := new(touchKeyboardState), new(touchKeyboardState)
	requestTouchKeyboard(a)
	first := a.lastShow.Load()
	if first == 0 {
		t.Fatal("first request must pass the debounce")
	}
	requestTouchKeyboard(a)
	if a.lastShow.Load() != first {
		t.Fatal("repeat request within 1s must be debounced")
	}
	// The other window's request must NOT be swallowed by a's debounce —
	// its monitor and occlusion state still need to start.
	requestTouchKeyboard(b)
	if b.lastShow.Load() == 0 {
		t.Fatal("another window's request must pass independently")
	}
}

func TestTrackEditorFocusHidesAfterDelay(t *testing.T) {
	s := new(touchKeyboardState)
	start := time.Unix(1000, 0)

	s.trackEditorFocus(longPressCtx(start), true)
	if !s.focusSeen {
		t.Fatal("focused frame must mark focusSeen")
	}

	// Blur arms the pending hide but does not fire it immediately.
	s.trackEditorFocus(longPressCtx(start.Add(50*time.Millisecond)), false)
	if !s.hidePending {
		t.Fatal("blur must arm the pending hide")
	}

	// Refocus (focus hop between editors) cancels the pending hide.
	s.trackEditorFocus(longPressCtx(start.Add(100*time.Millisecond)), true)
	if s.hidePending {
		t.Fatal("refocus must cancel the pending hide")
	}

	// Blur again and wait out the delay: hide fires and tracking resets.
	blur := start.Add(200 * time.Millisecond)
	s.trackEditorFocus(longPressCtx(blur), false)
	s.trackEditorFocus(longPressCtx(blur.Add(keyboardHideDelay)), false)
	if s.hidePending || s.focusSeen {
		t.Fatal("hide must fire and reset tracking after keyboardHideDelay")
	}
}

func TestOutsideTapDetection(t *testing.T) {
	s := new(touchKeyboardState)

	// Press claimed by an editor area — not an outside tap.
	s.noteWindowTouchPress(1)
	s.noteEditorTouchPress(1)
	if s.outsideTapPending() {
		t.Fatal("press inside an editor area must not count as outside tap")
	}

	// Unclaimed press — outside tap; records reset after evaluation.
	s.noteWindowTouchPress(2)
	if !s.outsideTapPending() {
		t.Fatal("unclaimed press must count as an outside tap")
	}
	if s.outsideTapPending() {
		t.Fatal("records must reset after evaluation")
	}

	// Mixed frame: one editor tap plus one outside tap → outside wins.
	s.noteWindowTouchPress(3)
	s.noteEditorTouchPress(3)
	s.noteWindowTouchPress(4)
	if !s.outsideTapPending() {
		t.Fatal("frame with an unclaimed press must count as outside tap")
	}
}

func TestOutsideTapCancelsPendingShow(t *testing.T) {
	s := new(touchKeyboardState)
	genBefore := s.showGen.Load()

	// Outside tap must bump the generation even though nothing is shown
	// yet — an in-flight show (150ms settle) checks it before TryShow.
	s.noteWindowTouchPress(1)
	s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0)))
	if s.showGen.Load() == genBefore {
		t.Fatal("outside tap must invalidate pending show requests")
	}
}

func TestExplicitEditorFocusSuppressesDismissal(t *testing.T) {
	s := new(touchKeyboardState)

	// Tap outside editors + handler focuses an editor in the same frame.
	s.noteWindowTouchPress(1)
	s.noteExplicitEditorFocus()
	gen := s.showGen.Load()
	s.dismissOnOutsideTap(longPressCtx(time.Unix(1000, 0)))
	if s.showGen.Load() != gen {
		t.Fatal("suppressed evaluation must not cancel pending shows")
	}

	// Suppression is one-shot: the next outside tap dismisses again.
	s.noteWindowTouchPress(2)
	s.dismissOnOutsideTap(longPressCtx(time.Unix(1001, 0)))
	if s.showGen.Load() == gen {
		t.Fatal("suppression must only cover one evaluation")
	}
}

func TestCancelLongPressOnMultiTouch(t *testing.T) {
	w := &Window{touchPressPos: map[pointer.ID]image.Point{}}
	rc := new(rightClickState)
	start := time.Unix(1000, 0)

	// One active touch: hold survives the guard and fires.
	w.touchPressPos[1] = image.Pt(10, 10)
	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, image.Pt(10, 10))
	w.cancelLongPressOnMultiTouch(rc)
	if !rc.touchDown {
		t.Fatal("single-touch hold must survive the multi-touch guard")
	}

	// Second finger lands in a DIFFERENT card's area: this rc never sees
	// its Press, but the window-wide map does — the guard cancels.
	w.touchPressPos[2] = image.Pt(300, 300)
	w.cancelLongPressOnMultiTouch(rc)
	if rc.touchDown {
		t.Fatal("second finger elsewhere in the window must cancel the hold")
	}
	if rc.longPressTriggered(longPressCtx(start.Add(2 * longPressDuration))) {
		t.Fatal("cancelled hold must not fire")
	}
}

func TestShowRequestBumpsGenerationInvalidatingHides(t *testing.T) {
	kbd := new(touchKeyboardState)

	// Simulate a hide captured at the CURRENT hide generation — hides are
	// versioned by hideGen, not showGen.
	kbd.shownByUs.Store(true)
	hideEpoch := kbd.hideGen.Load()
	showEpoch := kbd.showGen.Load()

	// A new show request must supersede both the pending hide (hideGen)
	// and any in-flight show (showGen).
	requestTouchKeyboard(kbd)
	if kbd.hideGen.Load() == hideEpoch {
		t.Fatal("show request must bump hideGen so a stale hide is invalidated")
	}
	if kbd.showGen.Load() == showEpoch {
		t.Fatal("dispatched show request must bump showGen")
	}
}

func TestCancelLongPressSameFrameSecondFinger(t *testing.T) {
	start := time.Unix(1000, 0)
	w := &Window{touchPressPos: map[pointer.ID]image.Point{}}
	rc := new(rightClickState)

	// Finger 1 holds a card.
	w.touchPressPos[1] = image.Pt(10, 10)
	rc.handleTouchLongPress(touchPress(1, f32.Pt(10, 10)), start, 20, image.Pt(10, 10))

	// Finger 2 presses AND releases within one frame: by guard time the
	// map is back to len==1, but the tracker recorded the overlap moment.
	w.multiTouchAt = start.Add(100 * time.Millisecond) // overlap after the hold started

	w.cancelLongPressOnMultiTouch(rc)
	if rc.touchDown {
		t.Fatal("overlap recorded during the hold must cancel it even after the second finger lifted")
	}

	// A fresh hold started AFTER the overlap moment survives.
	rc2 := new(rightClickState)
	rc2.handleTouchLongPress(touchPress(3, f32.Pt(10, 10)), start.Add(200*time.Millisecond), 20, image.Pt(10, 10))
	w.touchPressPos[3] = image.Pt(10, 10)
	delete(w.touchPressPos, 1)
	w.cancelLongPressOnMultiTouch(rc2)
	if !rc2.touchDown {
		t.Fatal("hold started after the overlap moment must survive")
	}
}

func TestTapCancelsPendingHideEvenWhenDebounced(t *testing.T) {
	kbd := new(touchKeyboardState)
	kbd.shownByUs.Store(true)

	requestTouchKeyboard(kbd) // dispatched (or at least accepted) tap
	hideEpoch := kbd.hideGen.Load()

	// Second tap lands inside the debounce window: the show dispatch is
	// suppressed, but a pending blur-hide captured at hideEpoch must
	// still be invalidated — otherwise it would close the keyboard right
	// after the user returned to the editor.
	requestTouchKeyboard(kbd)
	if kbd.hideGen.Load() == hideEpoch {
		t.Fatal("debounced tap must still invalidate pending hides")
	}
}

func TestTouchDrivenInputRecency(t *testing.T) {
	w := &Window{lastInputTouch: true, lastPressAt: time.Unix(1000, 0)}
	if !w.touchDrivenInput(longPressCtx(time.Unix(1000, 1))) {
		t.Fatal("action right after a touch press must count as touch-driven")
	}
	// A keyboard-driven action long after the last touch press must not.
	if w.touchDrivenInput(longPressCtx(time.Unix(1000, 0).Add(time.Minute))) {
		t.Fatal("stale touch flag must not mark later actions as touch-driven")
	}
	w.lastInputTouch = false
	if w.touchDrivenInput(longPressCtx(time.Unix(1000, 1))) {
		t.Fatal("mouse-driven input must never count as touch-driven")
	}
}

func TestFocusRegainCancelsEnqueuedHide(t *testing.T) {
	s := new(touchKeyboardState)
	start := time.Unix(1000, 0)

	// Focus, blur, and let the hide mature (it is now enqueued with the
	// hideGen captured at requestTouchKeyboardHide time).
	s.trackEditorFocus(longPressCtx(start), true)
	blur := start.Add(50 * time.Millisecond)
	s.trackEditorFocus(longPressCtx(blur), false)
	s.trackEditorFocus(longPressCtx(blur.Add(keyboardHideDelay)), false)
	hideEpoch := s.hideGen.Load()

	// Programmatic focus return (FocusCmd, keyboard navigation) — no new
	// touch event — must still invalidate the enqueued hide.
	s.trackEditorFocus(longPressCtx(blur.Add(keyboardHideDelay+time.Millisecond)), true)
	if s.hideGen.Load() == hideEpoch {
		t.Fatal("focus regain must invalidate an already-enqueued hide")
	}
}

func TestRegisterKeyboardStateDropsReleased(t *testing.T) {
	known := map[*touchKeyboardState]bool{}
	live, dead := new(touchKeyboardState), new(touchKeyboardState)

	registerKeyboardState(known, live)
	registerKeyboardState(known, dead)
	if len(known) != 2 {
		t.Fatalf("known = %d entries, want 2", len(known))
	}

	// Release: the entry must go away and a late command (Publish, retry)
	// must NOT resurrect it — each entry pins the whole window.
	dead.released.Store(true)
	registerKeyboardState(known, dead)
	if known[dead] {
		t.Fatal("released state must be removed and never re-added")
	}
	if !known[live] {
		t.Fatal("live state must survive")
	}
}

func TestTransferKeyboardOwnershipCarriesLegacy(t *testing.T) {
	known := map[*touchKeyboardState]bool{}
	main, console := new(touchKeyboardState), new(touchKeyboardState)
	registerKeyboardState(known, main)
	registerKeyboardState(known, console)

	// Main window owns a LEGACY-shown keyboard; user moves to the console.
	main.shownByUs.Store(true)
	main.legacyShow.Store(true)
	// Session type (will this session get a Hiding event?) is a SEPARATE flag
	// from ownership and hide-method. main's is a legacy session (no Hiding).
	main.expectHiding.Store(false)

	if !transferKeyboardOwnership(known, console) {
		t.Fatal("ownership must transfer from the other window")
	}
	// The old owner loses ownership AND the hide-method flag (legacyShow is
	// purely "how to hide" and follows ownership). Its monitor's session type
	// lives in the separate expectHiding flag, which transfer does not touch,
	// so clearing legacyShow here no longer strands the old monitor.
	if main.shownByUs.Load() || main.legacyShow.Load() {
		t.Fatal("old owner must be fully cleared of ownership and hide-method")
	}
	if main.expectHiding.Load() {
		t.Fatal("transfer must NOT alter the old owner's session-type flag (expectHiding)")
	}
	if !console.shownByUs.Load() {
		t.Fatal("new window must own the session")
	}
	if !console.legacyShow.Load() {
		t.Fatal("legacy flag must carry over — hide must still go through Toggle")
	}
	if console.expectHiding.Load() {
		t.Fatal("transfer must NOT set session type on the new owner — that is decided at show/Showing time")
	}

	// No other owner → nothing to transfer, self is never a donor.
	if transferKeyboardOwnership(known, console) {
		t.Fatal("transfer must report false when no OTHER window owns the keyboard")
	}
}

func TestKeyboardShowRetryDelayLadder(t *testing.T) {
	// A show whose COM init failed is re-enqueued on a growing ladder —
	// dropping it would reproduce the original first-tap-no-keyboard bug.
	for i := int8(0); i < keyboardShowRetryMax; i++ {
		delay, again := keyboardShowRetryDelay(i)
		if !again {
			t.Fatalf("retry %d: must still requeue", i)
		}
		if want := time.Duration(i+1) * time.Second; delay != want {
			t.Fatalf("retry %d: delay = %v, want %v", i, delay, want)
		}
	}
	if _, again := keyboardShowRetryDelay(keyboardShowRetryMax); again {
		t.Fatal("ladder exhausted: the stale command must be dropped (next tap makes a fresh one)")
	}
}

func TestKeyboardMonitorInitDelayGrowsAndCaps(t *testing.T) {
	if d := keyboardMonitorInitDelay(0); d != time.Second {
		t.Fatalf("first delay = %v, want 1s", d)
	}
	if d := keyboardMonitorInitDelay(4); d != 5*time.Second {
		t.Fatalf("fifth delay = %v, want 5s", d)
	}
	// UNLIMITED retries at a capped pace: a dead monitor is never restarted
	// by anything if the Showing already happened.
	for _, attempt := range []int{29, 30, 100, 1 << 20} {
		if d := keyboardMonitorInitDelay(attempt); d != 30*time.Second {
			t.Fatalf("attempt %d: delay = %v, want capped 30s", attempt, d)
		}
	}
}

func TestAdvisableKeyboardStatesFilters(t *testing.T) {
	known := map[*touchKeyboardState]bool{}
	needy := new(touchKeyboardState)
	needy.hwnd.Store(101)
	released := new(touchKeyboardState)
	released.hwnd.Store(102)
	released.released.Store(true)
	unbound := new(touchKeyboardState) // hwnd == 0: never shown yet
	advised := new(touchKeyboardState)
	advised.hwnd.Store(103)
	advised.eventsBound.Store(true)
	for _, s := range []*touchKeyboardState{needy, released, unbound, advised} {
		known[s] = true
	}

	got := advisableKeyboardStates(known)
	if len(got) != 1 || got[0] != needy {
		t.Fatalf("advisable = %v, want exactly the live unadvised bound state", got)
	}
}

func TestEndKeyboardSessionClearsEverything(t *testing.T) {
	kbd := new(touchKeyboardState)
	invalidated := false
	kbd.setInvalidate(func() { invalidated = true })
	kbd.shownByUs.Store(true)
	kbd.legacyShow.Store(true)
	kbd.publishOccludedDp(300)
	invalidated = false
	epoch := kbd.occlusionEpoch.Load()

	endKeyboardSession(kbd)
	if kbd.shownByUs.Load() || kbd.legacyShow.Load() {
		t.Fatal("session end must clear ownership and the legacy flag")
	}
	if kbd.occludedHeightDp() != 0 {
		t.Fatal("session end must remove the bottom padding")
	}
	if !invalidated {
		t.Fatal("padding removal must wake the frame loop")
	}
	if kbd.occlusionEpoch.Load() == epoch {
		t.Fatal("session end must bump the occlusion epoch so in-flight samples are voided")
	}
}

func TestRequestTouchKeyboardHideReportsDispatchGeneration(t *testing.T) {
	s := new(touchKeyboardState)

	// A keyboard we did not open cannot be hidden, and the caller has to be
	// told that nothing was dispatched: a caller that recorded a generation
	// here would throttle away every later ask, including asks made after the
	// keyboard becomes ours.
	if gen, sent := requestTouchKeyboardHide(s); sent || gen != 0 {
		t.Fatalf("hide of a keyboard we did not open must dispatch nothing, got (%d, %v)", gen, sent)
	}

	s.shownByUs.Store(true)
	s.hideGen.Add(1) // any earlier tap/release; the marker must not assume 0
	want := s.hideGen.Load()
	gen, sent := requestTouchKeyboardHide(s)
	if !sent {
		t.Fatal("hide of a keyboard we opened must dispatch")
	}
	if gen != want {
		t.Fatalf("reported generation must be the one the command carries: got %d want %d", gen, want)
	}

	// doHide drops a command whose generation moved on and its retry ladder
	// re-enqueues that same generation, so this is cancellation, not delay: a
	// caller throttling repeat asks must be able to see it by comparing the
	// reported generation against hideGen.
	s.hideGen.Add(1) // an editor tap; see requestTouchKeyboard
	if s.hideGen.Load() == gen {
		t.Fatal("a later editor tap must move hideGen past the dispatched hide")
	}
}

// editorAreaHarness drives editorTouchKeyboardArea around a REAL widget.Editor
// through a real input.Router. Nothing here calls requestTouchKeyboard: the
// point of these tests is the path from a finger on the glass to the show, and
// that path is exactly what the direct-call tests above cannot see.
type editorAreaHarness struct {
	router *input.Router
	kbd    *touchKeyboardState
	editor widget.Editor
	shaper *text.Shaper
	tag    *int
	// now is the frame clock the area stamps its held presses with. It is
	// driven by hand so a test can put real time between a press and the
	// cancel that ends it without spending any.
	now time.Time
}

// newEditorAreaHarness returns a harness whose first frame is already behind
// it, because Gio hands a brand-new handler a bare pointer.Cancel the very
// first time it asks for events — before any pointer has touched anything.
// Absorbing it here is not tidying: it is the assertion that an unprovoked
// cancel raises no keyboard, made against the router's real behaviour rather
// than a synthesized event, and it is the reason the cancel path insists on a
// press of its own before it treats a cancel as a tap.
func newEditorAreaHarness(t *testing.T) *editorAreaHarness {
	t.Helper()
	h := &editorAreaHarness{
		router: new(input.Router),
		kbd:    new(touchKeyboardState),
		shaper: text.NewShaper(text.WithCollection(gofont.Collection())),
		tag:    new(int),
		now:    time.Unix(1_700_000_000, 0),
	}
	h.frame()
	if got := h.kbd.showGen.Load(); got != 0 {
		t.Fatalf("the router's startup cancel dispatched %d shows — "+
			"a keyboard must not appear before the user has touched anything", got)
	}
	return h
}

// frame lays one frame out and hands it to the router. The editor really lays
// out — its gesture.Drag is registered by Editor.Layout, and it is that gesture
// that grabs the pointer and cancels ours.
func (h *editorAreaHarness) frame() {
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      h.router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 200)},
		Now:         h.now,
	}
	editorTouchKeyboardArea(gtx, h.tag, h.kbd, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min = image.Pt(400, 60)
		return h.editor.Layout(gtx, h.shaper, font.Font{}, unit.Sp(14), op.CallOp{}, op.CallOp{})
	})
	h.router.Frame(ops)
}

// advance moves the frame clock forward. Nothing else in the harness reads
// wall time, so this is the whole of "later" as these tests mean it.
func (h *editorAreaHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

// touch injects one pointer event and then runs a frame, so whatever the
// editor's gesture does in response (a grab, and the cancels it sends the rest
// of us) is on the record before the next assertion.
func (h *editorAreaHarness) touch(e pointer.Event) {
	h.router.Queue(e)
	h.frame()
}

// showsAfter reports the number of shows dispatched while fn ran. It is the
// generation counter because that is the only observable a headless test has:
// showPlatformTouchKeyboard is a no-op off Windows, but showGen is bumped by
// requestTouchKeyboard itself, before the platform is reached.
func (h *editorAreaHarness) showsAfter(fn func()) int64 {
	// The show debounce is cleared first. It collapses duplicate dispatches
	// from ONE physical tap over 300ms, which is an eternity for a gesture and
	// no time at all for a test: without this, a measurement would silently
	// score the second gesture in a test as "no show" and pass for the wrong
	// reason. Each measurement here stands for a separate touch.
	h.kbd.lastShow.Store(0)
	before := h.kbd.showGen.Load()
	fn()
	return h.kbd.showGen.Load() - before
}

// The regression this whole round is about. A finger that lands on a text
// field and rolls even slightly is grabbed by the editor's own selection
// gesture, and gesture.Drag grabs at 3dp — which an ordinary tap covers. From
// that moment Gio's router owns the pointer: every other handler under the
// finger gets a bare pointer.Cancel and is dropped, and the Release this area
// used to wait for never arrives. On the device that read as three text fields
// that did nothing at all.
func TestEditorAreaRaisesKeyboardWhenTheEditorGrabsTheTouch(t *testing.T) {
	h := newEditorAreaHarness(t)

	shows := h.showsAfter(func() {
		h.touch(pointer.Event{
			Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
			PointerID: 1, Position: f32.Pt(20, 20),
		})
		// Past gesture.touchSlop (3dp), which is what makes the editor grab.
		h.touch(pointer.Event{
			Kind: pointer.Move, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
			PointerID: 1, Position: f32.Pt(20, 32),
		})
		// The cancel the grab produced reaches us on the frame after it.
		h.frame()
	})
	if shows != 1 {
		t.Fatalf("a touch the editor grabbed dispatched %d shows, want 1 — "+
			"the release never comes for a grabbed pointer, so a cancelled press IS the tap", shows)
	}
}

// The ordinary path still works: a finger that does not move enough to be
// grabbed releases normally, and that release is still the tap.
func TestEditorAreaRaisesKeyboardOnAnUngrabbedRelease(t *testing.T) {
	h := newEditorAreaHarness(t)

	shows := h.showsAfter(func() {
		h.touch(pointer.Event{
			Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
			PointerID: 1, Position: f32.Pt(20, 20),
		})
		h.touch(pointer.Event{
			Kind: pointer.Release, Source: pointer.Touch,
			PointerID: 1, Position: f32.Pt(20, 20),
		})
	})
	if shows != 1 {
		t.Fatalf("a plain tap dispatched %d shows, want 1", shows)
	}
}

// A cancel with no press of ours behind it is not a tap. Gio's Windows backend
// sends a blanket cancel on WM_CANCELMODE — a menu opening, a window losing
// activation — and that must not summon a keyboard the user never asked for.
func TestEditorAreaIgnoresACancelWithNoPressBehindIt(t *testing.T) {
	h := newEditorAreaHarness(t)

	shows := h.showsAfter(func() {
		h.router.Queue(pointer.Event{Kind: pointer.Cancel})
		h.frame()
	})
	if shows != 0 {
		t.Fatalf("a bare cancel dispatched %d shows, want 0 — nothing was ever pressed here", shows)
	}
}

// And a cancel spends the press: the same finger cannot be cashed in twice,
// so a second cancel behind it is silent.
func TestEditorAreaCancelSpendsThePress(t *testing.T) {
	h := newEditorAreaHarness(t)
	h.touch(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(20, 20),
	})

	if shows := h.showsAfter(func() {
		h.router.Queue(pointer.Event{Kind: pointer.Cancel})
		h.frame()
	}); shows != 1 {
		t.Fatalf("the cancel that ends a real press dispatched %d shows, want 1", shows)
	}
	if shows := h.showsAfter(func() {
		h.router.Queue(pointer.Event{Kind: pointer.Cancel})
		h.frame()
	}); shows != 0 {
		t.Fatalf("a second cancel dispatched %d shows, want 0 — the press behind it was already spent", shows)
	}
}

// Two fingers cancelled together are one gesture, not two taps. The state
// clears wholesale for exactly this reason: a per-finger accounting would
// dispatch a show per finger, and the debounce that would usually hide the
// duplicate is bypassed while the keyboard is closing.
func TestEditorAreaMultiFingerCancelRaisesTheKeyboardOnce(t *testing.T) {
	h := newEditorAreaHarness(t)
	h.kbd.holdEditorTouch(h.tag, 1, h.now)
	h.kbd.holdEditorTouch(h.tag, 2, h.now)

	if !h.kbd.takeEditorTouches(h.tag, h.now) {
		t.Fatal("two held fingers must report as held")
	}
	if h.kbd.takeEditorTouches(h.tag, h.now) {
		t.Fatal("one cancel resolves every held finger — a second ask must find nothing")
	}
}

// The held-press list is bounded. Gio ends every press with a release or a
// cancel so the bound is unreachable in practice, but a backend that ever drops
// a terminator must leak a fixed handful of ints, not grow for the life of the
// process.
func TestEditorHeldTouchesAreBounded(t *testing.T) {
	s := new(touchKeyboardState)
	area := new(int)
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < tkMaxHeldEditorTouches*3; i++ {
		s.holdEditorTouch(area, pointer.ID(i), now)
	}
	if got := len(s.editorHeld); got != tkMaxHeldEditorTouches {
		t.Fatalf("held presses = %d, want the bound %d", got, tkMaxHeldEditorTouches)
	}
	// The oldest are the ones evicted: the newest finger is the one whose
	// cancel is still to come.
	if newest := s.editorHeld[len(s.editorHeld)-1].id; newest != pointer.ID(tkMaxHeldEditorTouches*3-1) {
		t.Fatalf("newest held press = %d, want %d", newest, tkMaxHeldEditorTouches*3-1)
	}
	// A repeat of a held id is not a second entry.
	before := len(s.editorHeld)
	s.holdEditorTouch(area, s.editorHeld[0].id, now)
	if len(s.editorHeld) != before {
		t.Fatalf("re-holding a held id grew the list to %d, want %d", len(s.editorHeld), before)
	}
}

// A release resolves ONLY its own finger. One finger lifting off a two-finger
// hold must not disarm the cancel path for the finger still down.
func TestEditorReleaseResolvesOnlyItsOwnTouch(t *testing.T) {
	s := new(touchKeyboardState)
	area := new(int)
	now := time.Unix(1_700_000_000, 0)
	s.holdEditorTouch(area, 1, now)
	s.holdEditorTouch(area, 2, now)
	s.dropEditorTouch(area, 1)
	if len(s.editorHeld) != 1 || s.editorHeld[0].id != 2 {
		t.Fatalf("held after releasing finger 1 = %v, want [2]", s.editorHeld)
	}
	s.dropEditorTouch(area, 7) // never held: no-op, and above all not a panic
	if len(s.editorHeld) != 1 {
		t.Fatalf("releasing an unheld id changed the list to %v", s.editorHeld)
	}
	if !s.takeEditorTouches(area, now) {
		t.Fatal("the finger still down must still count as held")
	}
}

// A press abandoned by one editor area is not a tap on another. Every editor in
// a window shares one touchKeyboardState, Gio hands a brand-new handler a bare
// Cancel the first time it asks for events, and a press whose widget vanished
// before its release never gets a terminator of its own — so without per-area
// scoping, opening a screen that has a text field on it would raise a keyboard
// the user never asked for, on the strength of a stale press somewhere else.
func TestEditorCancelOnlySpendsItsOwnAreasTouches(t *testing.T) {
	s := new(touchKeyboardState)
	message, search := new(int), new(int)
	now := time.Unix(1_700_000_000, 0)
	s.holdEditorTouch(message, 1, now)

	if s.takeEditorTouches(search, now) {
		t.Fatal("the search field's cancel must not be able to spend a press held on the message field")
	}
	if len(s.editorHeld) != 1 {
		t.Fatalf("the other area's press must survive that cancel, held = %v", s.editorHeld)
	}
	if !s.takeEditorTouches(message, now) {
		t.Fatal("the message field's own cancel must still find its press")
	}

	// And a release is scoped the same way: same finger id, different area.
	s.holdEditorTouch(message, 3, now)
	s.dropEditorTouch(search, 3)
	if len(s.editorHeld) != 1 {
		t.Fatalf("a release in another area resolved this one's press, held = %v", s.editorHeld)
	}
}

// A press the field never got to finish is not a tap on the field when it
// comes BACK. Tags here are stable pointers, so a console or alias editor that
// disappears mid-touch and reappears later reappears as the same tag — and the
// first thing Gio hands a re-registered handler is a bare Cancel. Per-area
// scoping cannot tell that apart from the real thing; only the age of the press
// can, which is what the stamp is for.
func TestEditorStalePressIsNotCashedInByALaterCancel(t *testing.T) {
	h := newEditorAreaHarness(t)

	// A finger goes down on the field, and the field goes away before it
	// comes back up: no release, no cancel, the record just sits there.
	h.touch(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(20, 20),
	})
	if len(h.kbd.editorHeld) != 1 {
		t.Fatalf("harness bug: the press was supposed to be held, held = %v", h.kbd.editorHeld)
	}

	// The user reads another tab and comes back. The field is laid out again,
	// and its brand-new handler is greeted by the router's bare startup cancel.
	// Ten seconds is stated outright rather than derived from tkHeldTouchTTL:
	// a bound expressed in terms of the constant it is bounding would move
	// with it and pin nothing.
	h.advance(10 * time.Second)
	if shows := h.showsAfter(func() {
		h.router.Queue(pointer.Event{Kind: pointer.Cancel})
		h.frame()
	}); shows != 0 {
		t.Fatalf("a cancel arriving long after the press dispatched %d shows, want 0 — "+
			"the finger that made that record was lifted on another screen", shows)
	}
	if len(h.kbd.editorHeld) != 0 {
		t.Fatalf("the stale record must be gone either way, held = %v — "+
			"leaving it means the NEXT cancel gets to try again", h.kbd.editorHeld)
	}
}

// And the press-then-grab it must not break: a real gesture takes a moment to
// cross the 3dp drag slop, and a window too tight to cover that would put the
// original "the field does nothing at all" defect straight back.
func TestEditorPressStillCountsWithinTheHoldWindow(t *testing.T) {
	h := newEditorAreaHarness(t)
	h.touch(pointer.Event{
		Kind: pointer.Press, Source: pointer.Touch, Buttons: pointer.ButtonPrimary,
		PointerID: 1, Position: f32.Pt(20, 20),
	})

	// Also a plain number, and for the same reason: this is how long a real
	// finger can take to cross the 3dp drag slop that triggers the grab, so it
	// is the floor the window has to clear no matter what the constant says.
	h.advance(400 * time.Millisecond)
	if shows := h.showsAfter(func() {
		h.router.Queue(pointer.Event{Kind: pointer.Cancel})
		h.frame()
	}); shows != 1 {
		t.Fatalf("a cancel 400ms after the press dispatched %d shows, want 1 — "+
			"a deliberate, slow tap is still a tap, and a window too tight to hold one "+
			"puts the original \"the field does nothing at all\" defect straight back", shows)
	}
}

// Expiry is per record, not wholesale: an old press and a fresh one can be
// held at once (two fingers, one of them left over), and the cancel must
// answer for the fresh one.
func TestEditorStaleAndFreshPressesExpireIndependently(t *testing.T) {
	s := new(touchKeyboardState)
	area := new(int)
	t0 := time.Unix(1_700_000_000, 0)
	s.holdEditorTouch(area, 1, t0)
	late := t0.Add(tkHeldTouchTTL + time.Second)
	s.holdEditorTouch(area, 2, late)

	if len(s.editorHeld) != 1 || s.editorHeld[0].id != 2 {
		t.Fatalf("the stale record must be dropped when a new press is recorded, held = %v", s.editorHeld)
	}
	if !s.takeEditorTouches(area, late) {
		t.Fatal("the press made moments ago must still count")
	}

	// A re-press of a still-held finger re-stamps rather than keeping the
	// original alive, or a record could be refreshed indefinitely.
	s.holdEditorTouch(area, 3, t0)
	s.holdEditorTouch(area, 3, late)
	if got := len(s.editorHeld); got != 1 {
		t.Fatalf("re-pressing a held finger made %d records, want 1", got)
	}
	if !s.takeEditorTouches(area, late) {
		t.Fatal("the re-stamped press must be read as fresh")
	}
}

// The trace switch has to reach the diagnostics that say WHY a show failed,
// and those were the ones it did not reach: crashlog leaves zerolog at warn
// while every stage/HRESULT line was log.Debug(), so a device run produced
// "status=error" and nothing else. This pins the promotion, at the level a
// device actually logs at.
func TestTouchKeyboardTracePromotesTheDiagnosticsThatExplainAFailure(t *testing.T) {
	prevLogger, prevTrace := log.Logger, touchKbdTraceOn
	defer func() { log.Logger, touchKbdTraceOn = prevLogger, prevTrace }()

	// A device's logger: warn and above, exactly what crashlog.Setup leaves.
	emit := func(trace bool) string {
		var buf bytes.Buffer
		log.Logger = zerolog.New(&buf).Level(zerolog.WarnLevel)
		touchKbdTraceOn = trace
		tkDiagEvent("pane").Uint64("hr", 0x80004005).Str("stage", "TryShow").Msg("touch keyboard: pane show failed")
		return buf.String()
	}

	if got := emit(false); got != "" {
		t.Fatalf("without the trace switch the diagnostic must stay at debug, got %q", got)
	}
	got := emit(true)
	if got == "" {
		t.Fatal("CORSA_TOUCHKBD_TRACE=1 must lift the diagnostic above the default warn filter — " +
			"a trace that cannot show the failing stage and HRESULT is the P2 finding all over again")
	}
	for _, want := range []string{`"level":"warn"`, `"tk":"pane"`, `"stage":"TryShow"`, `"hr":2147500037`} {
		if !strings.Contains(got, want) {
			t.Fatalf("traced diagnostic %q is missing %s", got, want)
		}
	}

	// And the plain trace helper stays nil when off: every call site chains
	// fields onto it unconditionally, which is only safe because of that.
	touchKbdTraceOn = false
	if tkTraceEvent("pane") != nil {
		t.Fatal("tkTraceEvent must be nil with tracing off; the call sites depend on zerolog's nil no-op")
	}
}

// tkAwaitVisible is the answer to "the call was accepted, but did a keyboard
// actually appear?" — the question the legacy fallback used to skip entirely,
// committing paneVisible=true on a Toggle HRESULT alone. Its whole value is in
// telling four outcomes apart, so each one is pinned separately here. The
// sleep is injected, so the table is checked without spending the 1.2s the
// real polling schedule describes.
func TestAwaitVisibleTellsTheFourOutcomesApart(t *testing.T) {
	const polls, delay = 4, 150 * time.Millisecond

	// A probe that answers "nothing there" conclusively, forever: the ONLY
	// case that may escalate, and only after every poll has been spent.
	t.Run("absent after every poll", func(t *testing.T) {
		var slept []time.Duration
		probes := 0
		got := tkAwaitVisible(polls, delay, func(d time.Duration) { slept = append(slept, d) },
			func() bool { return true },
			func() (bool, bool) { probes++; return false, true })
		if got != tkVisibleAbsent {
			t.Fatalf("a keyboard that never appeared must read as absent, got %v", got)
		}
		if len(slept) != polls || probes != polls {
			t.Fatalf("the wait must spend all %d polls before giving up, slept %d probed %d", polls, len(slept), probes)
		}
		for _, d := range slept {
			if d != delay {
				t.Fatalf("each poll waits %v, got %v", delay, d)
			}
		}
	})

	// A keyboard that comes up on the first poll costs one delay, not all of
	// them: the fallback path is on the critical path of a user's tap.
	t.Run("seen stops immediately", func(t *testing.T) {
		probes := 0
		slept := 0
		got := tkAwaitVisible(polls, delay, func(time.Duration) { slept++ },
			func() bool { return true },
			func() (bool, bool) { probes++; return true, true })
		if got != tkVisibleSeen {
			t.Fatalf("a visible keyboard must read as seen, got %v", got)
		}
		if probes != 1 || slept != 1 {
			t.Fatalf("a keyboard seen on the first poll must end the wait there, slept %d probed %d", slept, probes)
		}
	})

	// Validity is read BEFORE the probe on purpose. A show that no longer
	// belongs to this window must not be committed on the evidence of a
	// keyboard that now belongs to someone else's.
	t.Run("cancellation beats a visible keyboard", func(t *testing.T) {
		probed := false
		got := tkAwaitVisible(polls, delay, func(time.Duration) {},
			func() bool { return false },
			func() (bool, bool) { probed = true; return true, true })
		if got != tkVisibleCancelled {
			t.Fatalf("a superseded show must read as cancelled, got %v", got)
		}
		if probed {
			t.Fatal("validity is checked before the probe: a cancelled show must not be able to commit on someone else's keyboard")
		}
	})

	// No probe could answer. That is not evidence of absence — reporting
	// absence would fire the global, non-idempotent Toggle at a keyboard that
	// may well be up — and it is not evidence of a keyboard either.
	t.Run("inconclusive is not absence", func(t *testing.T) {
		probes := 0
		got := tkAwaitVisible(polls, delay, func(time.Duration) {},
			func() bool { return true },
			func() (bool, bool) { probes++; return false, false })
		if got != tkVisibleUnknown {
			t.Fatalf("an unanswerable probe must read as unknown, not absent, got %v", got)
		}
		if probes != polls {
			t.Fatalf("a probe that failed once usually answers the next time, so the wait must keep asking: probed %d of %d", probes, polls)
		}
	})

	// The reason the wait no longer stops at the first unanswered probe: it
	// used to, and the caller read that early stop as success, committed
	// paneVisible for a keyboard nobody had seen and returned — so one
	// transient COM hiccup on the very first poll ended the whole attempt and
	// the user got no keyboard and no retry.
	t.Run("one unanswered poll does not end the wait", func(t *testing.T) {
		probes := 0
		got := tkAwaitVisible(polls, delay, func(time.Duration) {},
			func() bool { return true },
			func() (bool, bool) { probes++; return false, probes != 1 })
		if got != tkVisibleUnknown {
			t.Fatalf("a wait with an unanswered poll in it cannot claim absence, got %v", got)
		}
		if probes != polls {
			t.Fatalf("the remaining polls must still be spent, probed %d of %d", probes, polls)
		}
	})

	// And the keyboard those extra polls exist to catch.
	t.Run("a keyboard after an unanswered poll still counts", func(t *testing.T) {
		probes := 0
		got := tkAwaitVisible(polls, delay, func(time.Duration) {},
			func() bool { return true },
			func() (bool, bool) { probes++; return probes == 3, probes != 1 })
		if got != tkVisibleSeen {
			t.Fatalf("a keyboard seen after a failed probe must read as seen, got %v", got)
		}
	})

	// The realistic shape: the host takes a moment, then the keyboard is up.
	t.Run("a late keyboard still counts", func(t *testing.T) {
		probes := 0
		got := tkAwaitVisible(polls, delay, func(time.Duration) {},
			func() bool { return true },
			func() (bool, bool) { probes++; return probes >= 3, true })
		if got != tkVisibleSeen {
			t.Fatalf("a keyboard that appears on the third poll must still read as seen, got %v", got)
		}
		if probes != 3 {
			t.Fatalf("the wait must stop at the poll that saw it, probed %d", probes)
		}
	})
}

// The debounce exists to collapse the several dispatches one physical tap
// produces, and it is safe only while a dropped show leaves a keyboard
// standing. A hide in flight inverts that: the pane is on its way out, nothing
// else will bring it back, and the dropped show was the only thing that would
// have. This is the "user closed the keyboard, then immediately tapped the
// field again" flow — a re-tap that fast lands well inside the 300ms window.
func TestRequestTouchKeyboardBypassesDebounceWhileClosing(t *testing.T) {
	prev := keyboardClosingHook
	defer func() { keyboardClosingHook = prev }()
	closing := false
	keyboardClosingHook = func() bool { return closing }

	a := new(touchKeyboardState)
	requestTouchKeyboard(a)
	first := a.lastShow.Load()
	if first == 0 {
		t.Fatal("first request must pass the debounce")
	}
	// Step the stamp back one nanosecond. On a coarse wall clock (Windows
	// serves time.Now from the shared-page _SYSTEM_TIME, advanced once per
	// clock interrupt, so UnixNano moves in 0.5-15.6ms ticks) every tap in
	// this test can draw the SAME stamp; an equal stamp is deliberately a
	// fresh tap (the backward-clock guard in keyboardDebounceBlocks), and the
	// closing bypass below would be indistinguishable from a plain dispatch
	// whose CAS rewrites the same number. With now > last guaranteed, the
	// debounce is real and the bypassed dispatch visibly moves the stamp.
	first--
	a.lastShow.Store(first)
	// Nothing is closing: a re-tap this fast is a duplicate of one physical tap.
	requestTouchKeyboard(a)
	if a.lastShow.Load() != first {
		t.Fatal("repeat request must be debounced while no hide is in flight")
	}

	closing = true
	gen := a.showGen.Load()
	requestTouchKeyboard(a)
	if a.lastShow.Load() == first {
		t.Fatal("a tap made while the keyboard is closing must not be debounced away — " +
			"the pane is going out and no other command will bring it back")
	}
	if a.showGen.Load() != gen+1 {
		t.Fatal("the bypassing tap must dispatch a show (showGen bump)")
	}
}

// tkWindowsAST parses touch_keyboard_windows.go. The file is behind
// //go:build windows and so is never COMPILED by this suite, but a build tag
// is a constraint on building, not on parsing: go/parser reads it on any
// platform. That gap is the whole reason the guards below exist. Everything in
// that file — the apartment the input pane is activated from, the message pump
// that apartment needs — is invisible to a test suite that only ever runs on
// linux, which is exactly the kind of code that gets quietly reverted.
func tkWindowsAST(t *testing.T) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "touch_keyboard_windows.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing touch_keyboard_windows.go: %v", err)
	}
	return f
}

// The bug that cost the most on the device was not in any branch this suite
// can reach. RoInitialize was called with the multi-threaded apartment, so
// every activation of Windows.UI.ViewManagement.InputPane — a single-threaded
// WinRT class — answered RO_E_UNSUPPORTED_FROM_MTA before any of the show
// path's careful retries and fallbacks had anything to work with. Fifteen
// seconds of logging, no keyboard.
//
// TestPermanentPaneHRESULTsAreTheOnesThatCannotChange covers what to do when
// that HRESULT arrives, and it stays green whether or not the HRESULT can
// still arrive — it is a test about the fallback, not about the cause. Nothing
// in the suite fails if someone changes the apartment back, because nothing in
// the suite compiles the file it lives in.
//
// So this reads the source. It is a blunter instrument than a unit test and it
// is chosen deliberately: the alternative on offer is no coverage at all of
// the single change this entire round of work was about.
func TestInputPaneIsActivatedFromASingleThreadedApartment(t *testing.T) {
	f := tkWindowsAST(t)

	// 1. The apartment constant exists and is STA. A test that only checks the
	// name would pass on `roInitSingleThreaded = 1`, which is the same bug
	// wearing the right label.
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if name.Name != "roInitSingleThreaded" || i >= len(vs.Values) {
				continue
			}
			found = true
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				t.Errorf("roInitSingleThreaded is not an integer literal, so this test cannot tell which apartment is being asked for")
				continue
			}
			v, err := strconv.ParseInt(lit.Value, 0, 64)
			if err != nil || v != 0 {
				t.Errorf("roInitSingleThreaded = %s, want 0. RO_INIT_SINGLETHREADED is 0 and RO_INIT_MULTITHREADED is 1, so any other value puts the service thread back in the apartment the input pane cannot be activated from", lit.Value)
			}
		}
		return true
	})
	if !found {
		t.Error("no roInitSingleThreaded constant in touch_keyboard_windows.go — the apartment the input pane requires is no longer named anywhere")
	}

	// 2. Nothing is called roInitMultithreaded any more. This is an AST walk,
	// not a text search, so the prose above tkRoInit is free to keep
	// explaining what the MTA was and why it was wrong.
	ast.Inspect(f, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "roInitMultithreaded" {
			t.Error("roInitMultithreaded is back in touch_keyboard_windows.go; InputPane is a single-threaded class and cannot be activated from the MTA")
		}
		return true
	})

	// 3. The constant is what RoInitialize is actually PASSED. Declaring the
	// right value and then calling with a literal 1 would satisfy both checks
	// above and reproduce the failure exactly.
	calls := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Call" {
			return true
		}
		recv, ok := sel.X.(*ast.Ident)
		if !ok || recv.Name != "tkProcRoInitialize" {
			return true
		}
		calls++
		if len(call.Args) != 1 {
			t.Errorf("tkProcRoInitialize.Call takes %d arguments; RoInitialize takes exactly one, the apartment type", len(call.Args))
			return true
		}
		arg, ok := call.Args[0].(*ast.Ident)
		if !ok || arg.Name != "roInitSingleThreaded" {
			t.Errorf("RoInitialize is called with something other than roInitSingleThreaded; the apartment the input pane needs must be the one actually requested")
		}
		return true
	})
	if calls != 1 {
		t.Errorf("found %d calls to tkProcRoInitialize.Call, want exactly 1 — a second call site is a second apartment decision, and this test only guards the one it can see", calls)
	}
}

// An STA is only half a fix. COM delivers incoming calls to a single-threaded
// apartment through a hidden window on that thread's message queue, so a
// thread that asks for an STA and then parks on a Go channel receives none of
// the Showing/Hiding callbacks it registered, and the shell thread raising
// them blocks until it gives up. The keyboard opens and this process is never
// told — the same end state as the original bug, reached from the other side.
// The pump is therefore part of the apartment change, not an addition to it,
// and reverting either half alone is enough to break the device again.
func TestTheSingleThreadedApartmentIsPumped(t *testing.T) {
	f := tkWindowsAST(t)

	// The service loop must not be a bare channel receive again.
	ast.Inspect(f, func(n ast.Node) bool {
		rng, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}
		if id, ok := rng.X.(*ast.Ident); ok && id.Name == "tkCmdKick" {
			t.Error("the service loop ranges over tkCmdKick again; a goroutine parked on a channel dispatches no window messages, so this apartment would never receive its Showing/Hiding callbacks")
		}
		return true
	})

	callsIn := func(fn string) map[string]int {
		out := map[string]int{}
		ast.Inspect(f, func(n ast.Node) bool {
			d, ok := n.(*ast.FuncDecl)
			if !ok || d.Name.Name != fn {
				return true
			}
			ast.Inspect(d.Body, func(m ast.Node) bool {
				call, ok := m.(*ast.CallExpr)
				if !ok {
					return true
				}
				if id, ok := call.Fun.(*ast.Ident); ok {
					out[id.Name]++
				}
				return true
			})
			return false
		})
		return out
	}

	if callsIn("tkKeyboardService")["tkAwaitCommands"] == 0 {
		t.Error("tkKeyboardService no longer calls tkAwaitCommands — whatever it waits on instead is not pumping this thread's message queue")
	}
	if callsIn("tkAwaitCommands")["tkStaPump"] == 0 {
		t.Error("tkAwaitCommands no longer calls tkStaPump; the wait would then be the only thing servicing the apartment, and a wait services nothing")
	}

	// The occlusion monitors are apartments of their own — tkRoInit is called
	// per goroutine — and hold pane proxies of their own, so they need the
	// pump for the same reason. Counting call sites file-wide catches the
	// monitor's without asserting the shape of the goroutine it sits in.
	total := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "tkStaPump" {
			total++
		}
		return true
	})
	if total < 2 {
		t.Errorf("tkStaPump is called from %d place(s); the service thread and the occlusion monitor are separate apartments and both have to drain their own queue", total)
	}
}

// The window between 300ms and 3.5s is where a tap used to do the most damage
// while looking like it did nothing at all: past the debounce, so it was
// admitted; on top of a show that had not finished, so it cancelled that show
// and started a fresh one with an empty retry budget. Tap at that rate and the
// attempt restarts forever and never reaches the fallback that would have
// worked. The whole window is walked here at its exact boundaries, because
// both edges are the ones that decide whether a real user gesture survives.
func TestKeyboardTapVerdictAcrossTheShowWindow(t *testing.T) {
	const last = 1 << 40 // an arbitrary but realistic stamp, far from zero
	deb := int64(keyboardShowDebounce)
	coa := int64(keyboardShowCoalesce)

	if deb >= coa {
		t.Fatalf("keyboardShowDebounce (%v) must be shorter than keyboardShowCoalesce (%v), or the coalesce window is unreachable and the re-tap it exists to absorb goes straight through",
			keyboardShowDebounce, keyboardShowCoalesce)
	}

	cases := []struct {
		name    string
		now     int64
		visible int64 // visibleGen NOW; the dispatch snapshot is always 7 below
		active  bool  // the dispatched show is still in flight
		closing bool
		want    tkTapVerdict
	}{
		{"the same physical tap, one nanosecond later", last + 1, 7, true, false, tkTapDebounced},
		{"still one physical tap at the last instant of the debounce", last + deb - 1, 7, true, false, tkTapDebounced},
		{"the debounce boundary itself belongs to the coalesce", last + deb, 7, true, false, tkTapCoalesced},
		{"a deliberate second tap, half a second in, show still silent", last + 500_000_000, 7, true, false, tkTapCoalesced},
		{"two seconds in, mid legacy-host recovery", last + 2_000_000_000, 7, true, false, tkTapCoalesced},
		{"the last instant a show can still be called in flight", last + coa - 1, 7, true, false, tkTapCoalesced},
		{"past the coalesce: the show had its chance, let the tap try again", last + coa, 7, true, false, tkTapDispatch},
		{"long past it", last + 60*coa, 7, true, false, tkTapDispatch},

		// A moved visibleGen means a keyboard was SEEN on screen, so the show
		// is finished and every one of these is a genuine re-open.
		{"keyboard appeared, user closed it by hand, taps again at 400ms", last + 400_000_000, 8, true, false, tkTapDispatch},
		{"keyboard appeared, taps again at 2s", last + 2_000_000_000, 8, true, false, tkTapDispatch},
		{"keyboard appeared, taps again just under the coalesce edge", last + coa - 1, 8, true, false, tkTapDispatch},
		// ...but not so genuine that it outranks the debounce, which is still
		// collapsing duplicate dispatches of ONE tap.
		{"a successful show does not unlock the debounce", last + 1, 8, true, false, tkTapDebounced},

		// A show that ENDED without a keyboard. visibleGen never moved, the
		// stamp is untouched, and by the old reading of those two the field
		// stayed held for the rest of the window — told, every time, that an
		// earlier show was still working on it. Nothing was working on it: the
		// platform ran out of things to try and said so.
		{"the host would not start; user taps again 400ms later", last + 400_000_000, 7, false, false, tkTapDispatch},
		{"a failed show two seconds ago holds nothing", last + 2_000_000_000, 7, false, false, tkTapDispatch},
		{"a failed show at the last instant of the window", last + coa - 1, 7, false, false, tkTapDispatch},
		// The debounce is NOT part of the same claim and does not move with it.
		// It collapses duplicate dispatches of one physical tap, which stay
		// duplicates whether or not the show they duplicate went on to fail —
		// and a fast failure lands well inside 300ms, so this is a real case
		// and not a theoretical one.
		{"a failed show still does not license a duplicate of its own tap", last + 1, 7, false, false, tkTapDebounced},

		// A hide already animating inverts the premise behind both refusals:
		// the keyboard a dropped tap would have relied on is leaving.
		{"mid-hide re-tap inside the debounce", last + 1, 7, true, true, tkTapDispatch},
		{"mid-hide re-tap inside the coalesce", last + 2_000_000_000, 7, true, true, tkTapDispatch},

		// Clock movement must not be able to invent a window that is not there.
		{"a backward clock step is a fresh request, not a late one", last - 1_000_000_000, 7, true, false, tkTapDispatch},
		{"a coarse clock reading the same nanosecond twice", last, 7, true, false, tkTapDispatch},
		{"no show has ever been dispatched", 1 << 50, 7, true, false, tkTapDispatch},
	}

	for _, c := range cases {
		l := int64(last)
		if c.name == "no show has ever been dispatched" {
			l = 0
		}
		got := keyboardTapVerdict(c.now, l, 7, c.visible, c.active, c.closing)
		if got != c.want {
			t.Errorf("%s: verdict %s, want %s", c.name, tapVerdictName(got), tapVerdictName(c.want))
		}
	}
}

// keyboardShowActive is read at every tap, so its two ways of saying "no" are
// worth pinning: a superseded generation, and a generation the platform layer
// declared over. The zero case is the one with a trap in it — showGen and
// showActiveGen are both zero on a window that has never been tapped, and
// treating that as an active show would be a lockout waiting for a stale stamp
// to line up with it.
func TestKeyboardShowActive(t *testing.T) {
	cases := []struct {
		name        string
		active, cur int64
		want        bool
	}{
		{"a dispatched show is in flight", 4, 4, true},
		{"the platform reported it over", 0, 4, false},
		{"a newer show superseded it", 3, 4, false},
		{"the gap between showGen.Add and the store beside it", 3, 4, false},
		{"a window that has never dispatched anything", 0, 0, false},
	}
	for _, c := range cases {
		if got := keyboardShowActive(c.active, c.cur); got != c.want {
			t.Errorf("%s: keyboardShowActive(%d, %d) = %v, want %v", c.name, c.active, c.cur, got, c.want)
		}
	}
}

// The gap the coalesce still had after it was taught to wait for a SEEN
// keyboard: it could wait for one that was never coming. visibleGen answers
// "did a keyboard appear" and cannot answer "did this show end", and a show
// that ends in failure moves neither it nor the stamp. So the field went on
// being held, and the trace went on saying an earlier show was still working
// on it, for the remainder of five seconds after the platform had given up —
// after a TabTip.exe that would not start, that can be a few hundred
// milliseconds in, leaving four and a half seconds of a dead field.
func TestAShowThatEndedStopsHoldingTheField(t *testing.T) {
	kbd := new(touchKeyboardState)

	requestTouchKeyboard(kbd)
	gen := kbd.showGen.Load()
	if got := kbd.showActiveGen.Load(); got != gen {
		t.Fatalf("a dispatched show must mark its own generation in flight: showActiveGen=%d, showGen=%d", got, gen)
	}

	// One second in, nothing on screen, the show still working: held, and
	// rightly — this is the case the window exists for.
	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen {
		t.Fatalf("a re-tap into a LIVE show dispatched anyway (showGen=%d, want %d) — releasing the coalesce while the fallback is still running is the starvation it was built to stop", got, gen)
	}

	// The platform layer reaches a terminal exit: no keyboard, no retry, no
	// poll, nothing scheduled. On Windows this is a finishShow call at the end
	// of legacyShow or one of doVerifyShow's unreadable probes.
	kbd.finishShow(gen)

	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen+1 {
		t.Fatalf("a tap after the show had ENDED was still coalesced into it: showGen=%d, want %d. Nothing was running; the user was tapping a dead field and being told to wait for it", got, gen+1)
	}
	if got := kbd.showActiveGen.Load(); got != gen+1 {
		t.Fatalf("the replacement show did not mark itself in flight: showActiveGen=%d, want %d — the very next tap would then restart it", got, gen+1)
	}
}

// The objection that kept this design out of the previous round, answered. An
// explicit "a show is running" flag is only safe if a show that has been
// superseded cannot clear it: every stage of a show outlives the tap that
// started it — the retry ladder by fifteen seconds, the legacy verification by
// three and a half — so a dead stage finishing after the user has tapped again
// is the ORDINARY case, not the exotic one. Clearing unconditionally there
// would hand the newer show's coalesce away and let the next tap cancel the
// fallback that was still running.
func TestAnEndedShowCannotFreeANewerOne(t *testing.T) {
	kbd := new(touchKeyboardState)

	requestTouchKeyboard(kbd)
	first := kbd.showGen.Load()

	// A keyboard appeared and the user closed it by hand; the next tap is a
	// genuine new request and dispatches a second show.
	kbd.visibleGen.Add(1)
	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	second := kbd.showGen.Load()
	if second != first+1 {
		t.Fatalf("the tap after a seen keyboard must dispatch: showGen=%d, want %d", second, first+1)
	}

	// Only NOW does the first show's legacy fallback — still polling on its own
	// goroutine, three seconds behind — run out of hosts and report it.
	kbd.finishShow(first)
	if got := kbd.showActiveGen.Load(); got != second {
		t.Fatalf("a stale show cleared the in-flight mark of a newer one: showActiveGen=%d, want %d", got, second)
	}

	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != second {
		t.Fatalf("a re-tap into the SECOND show dispatched (showGen=%d, want %d) because the first show's ending freed it — that cancels a live show and restarts its retry budget, which is the whole defect the coalesce exists to prevent", got, second)
	}
}

// The mark must never move backwards, and the reason is not hypothetical
// tidiness. Dispatch bumps showGen and then marks the result in flight, two
// separate atomics; two taps that arrive together — which the stamp CAS in
// requestTouchKeyboard already treats as a real possibility — can interleave so
// that the OLDER generation performs its mark last. A plain store would then
// leave showActiveGen one behind showGen, keyboardShowActive would read false
// for a show dispatched microseconds earlier, and the very next tap would cancel
// it. That is the starvation this round is fixing, reached by a route with no
// failure of the platform in it at all.
func TestAnOlderShowCannotTakeOverTheMark(t *testing.T) {
	kbd := new(touchKeyboardState)

	kbd.beginShow(7)
	kbd.beginShow(5)
	if got := kbd.showActiveGen.Load(); got != 7 {
		t.Fatalf("an older generation dragged the in-flight mark backwards: showActiveGen=%d, want 7", got)
	}
	kbd.beginShow(8)
	if got := kbd.showActiveGen.Load(); got != 8 {
		t.Fatalf("a newer generation did not take the mark: showActiveGen=%d, want 8", got)
	}

	// The same thing where it actually happens: a dispatch has just marked
	// itself, and the loser of the race gets to its own mark afterwards.
	kbd = new(touchKeyboardState)
	requestTouchKeyboard(kbd)
	gen := kbd.showGen.Load()
	kbd.beginShow(gen - 1)
	if got := kbd.showActiveGen.Load(); got != gen {
		t.Fatalf("a late mark from the losing tap unset the live show: showActiveGen=%d, showGen=%d", got, gen)
	}
	if !keyboardShowActive(kbd.showActiveGen.Load(), kbd.showGen.Load()) {
		t.Fatal("the show read as inactive immediately after being dispatched — the next tap would cancel and restart it, which is the starvation itself")
	}

	// Concurrently, in the order the scheduler happens to pick: the highest
	// generation must win regardless of who ran last. Worth running under -race,
	// which is where a non-atomic read-modify-write of the mark would show up.
	kbd = new(touchKeyboardState)
	var wg sync.WaitGroup
	for i := int64(1); i <= 16; i++ {
		wg.Add(1)
		go func(gen int64) {
			defer wg.Done()
			kbd.beginShow(gen)
		}(i)
	}
	wg.Wait()
	if got := kbd.showActiveGen.Load(); got != 16 {
		t.Fatalf("the newest generation did not survive a concurrent scramble: showActiveGen=%d, want 16", got)
	}
}

// A cancellation leaves no residue. This one is an invariant test and not a
// behaviour test, and the distinction is worth stating rather than blurring:
// removing the store this pins does NOT reopen any defect, because
// keyboardShowActive compares the mark against showGen and the bump inside
// cancelPendingShow has already left every marked generation behind. What it
// pins is that the struct does not go on claiming a cancelled generation is in
// flight — a lie that is harmless today and would stop being harmless the first
// time someone reads showActiveGen without also reading showGen.
func TestACancelledShowLeavesNothingMarkedInFlight(t *testing.T) {
	kbd := new(touchKeyboardState)

	requestTouchKeyboard(kbd)
	if kbd.showActiveGen.Load() == 0 {
		t.Fatal("a dispatched show did not mark itself in flight")
	}

	kbd.cancelPendingShow()
	if got := kbd.showActiveGen.Load(); got != 0 {
		t.Fatalf("a cancelled show is still marked in flight: showActiveGen=%d", got)
	}
	if keyboardShowActive(kbd.showActiveGen.Load(), kbd.showGen.Load()) {
		t.Fatal("a cancelled show still reads as active")
	}

	// And the field is free at once: the point of the cancel is that the tap
	// which follows it — editor, button, editor — dispatches rather than being
	// swallowed by the window of the show it just cancelled.
	gen := kbd.showGen.Load()
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen+1 {
		t.Fatalf("the tap after a cancel did not dispatch: showGen=%d, want %d", got, gen+1)
	}
}

// The rule is only worth as much as its coverage of the exits, and the exits
// live in a file the linux suite never compiles. Read them.
//
// The direction of risk decides the shape of this guard. A terminal exit that
// forgets to release costs the user the rest of a five-second window; a release
// somewhere the show is still working cancels a live fallback and brings the
// starvation back. So the release is written explicitly at each ending rather
// than deferred-unless-claimed, and this test asserts both halves: that the
// endings say it, and that the retry ladders do not.
func TestEveryTerminalShowExitReleasesTheCoalesce(t *testing.T) {
	f := tkWindowsAST(t)

	// Minimums per closure. Each is a way for a show to stop without a
	// keyboard: a cancellation, a spent ladder, a host that would not start, a
	// probe that could not be read.
	//
	//	doShow        6  COM cancel + COM ladder spent, the settle-sleep and
	//	                 advise cancels, and the two post-TryShow cancels
	//	legacyShow    7  entry cancel, mid-Toggle cancel, the compensate verdict,
	//	                 TabTip refusing to start, two waiting cancels, and the
	//	                 poll loop running out
	//	doVerifyShow  5  cancel, not-ours, and the three probes that FAILED (the
	//	                 three that answer YES mark a keyboard seen instead)
	want := map[string]int{"doShow": 6, "legacyShow": 7, "doVerifyShow": 5}
	got := map[string]int{}
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		id, ok := as.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		if _, wanted := want[id.Name]; !wanted {
			return true
		}
		got[id.Name] = tkASTCalls(as.Rhs[0])["finishShow"]
		return false
	})
	for name, n := range want {
		have, found := got[name]
		if !found {
			t.Errorf("%s no longer exists in touch_keyboard_windows.go; this guard can no longer find the exits it checks and is silently passing", name)
			continue
		}
		if have < n {
			t.Errorf("%s ends a show without a keyboard %d time(s) that release the tap coalesce, want at least %d. An ending that stays silent leaves every tap for the next five seconds answered with \"an earlier show is still working\" about a show that has stopped", name, have, n)
		}
	}

	// The one the device report was actually about: TabTip.exe fails to launch
	// and legacyShow returns. It is the fastest terminal exit in the file, so it
	// is the one that wastes the most of the window.
	tabTipGuards := 0
	ast.Inspect(f, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || tkASTCalls(is.Cond)["tkStartTabTip"] == 0 {
			return true
		}
		tabTipGuards++
		if tkASTCalls(is.Body)["finishShow"] == 0 {
			t.Error("the branch taken when the legacy host will not start does not release the tap coalesce. That return can happen within a few hundred milliseconds of the tap, and every tap for the rest of the window is then declined in favour of a show that has already run out of places to try")
		}
		return true
	})
	if tabTipGuards == 0 {
		t.Fatal("no branch on tkStartTabTip found; the exit this test was written for can no longer be located")
	}

	// ...and the other half: a requeue is not an ending. The retry branch keeps
	// the SAME generation, so the mark must stay and the coalesce must go on
	// covering it, or the ladder is cancellable by exactly the impatient tapping
	// the coalesce was added to survive. The else-branch of these ifs is a spent
	// ladder and DOES release; only the taken branch is checked.
	ladders := 0
	ast.Inspect(f, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || is.Init == nil || tkASTCalls(is.Init)["keyboardShowRetryDelay"] == 0 {
			return true
		}
		ladders++
		if tkASTCalls(is.Body)["finishShow"] > 0 {
			t.Error("a retry branch releases the tap coalesce. The requeued command carries the same generation and is still going to run, so the show has not ended — releasing here lets the next tap bump showGen and kill the retry ladder mid-climb")
		}
		return true
	})
	if ladders < 3 {
		t.Errorf("found %d retry ladder(s) keyed on keyboardShowRetryDelay, want at least 3 (COM init, re-show poll exhausted, transient pane error)", ladders)
	}

	// Success ends a show too, and ends it in the one place that knows a
	// keyboard is really there.
	marked := false
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "tkMarkKeyboardSeen" {
			return true
		}
		ast.Inspect(fd.Body, func(m ast.Node) bool {
			if sel, ok := m.(*ast.SelectorExpr); ok && sel.Sel.Name == "showActiveGen" {
				marked = true
			}
			return true
		})
		return false
	})
	if !marked {
		t.Error("tkMarkKeyboardSeen no longer clears the in-flight mark. A keyboard on screen is a show that is over; leaving the mark set lets the two pieces of state disagree about whether anything is still working")
	}

	if total := tkASTCalls(f)["finishShow"]; total < 18 {
		t.Errorf("the show path releases the coalesce from %d place(s), want at least 18. A new way for a show to stop is a new way for the field to stay held after it has", total)
	}
}

func tapVerdictName(v tkTapVerdict) string {
	switch v {
	case tkTapDispatch:
		return "dispatch"
	case tkTapDebounced:
		return "debounced"
	case tkTapCoalesced:
		return "coalesced"
	}
	return "unknown"
}

// The same rule seen from where the user stands, through requestTouchKeyboard
// rather than the predicate: an impatient second tap on a field that is still
// blank must not cancel the show that is working on it, and the moment a
// keyboard has actually been produced the field must become tappable again.
func TestRetapDuringAnUnfinishedShowIsCoalesced(t *testing.T) {
	kbd := new(touchKeyboardState)

	requestTouchKeyboard(kbd)
	gen := kbd.showGen.Load()
	if gen == 0 {
		t.Fatal("the first tap must dispatch a show")
	}
	if kbd.showVisibleGen.Load() != 0 {
		t.Fatal("the first show dispatches with no keyboard seen and must record that")
	}

	// Backdate the stamp to one second ago: past the debounce, inside the
	// coalesce, with visibleGen still where the dispatch left it — i.e. a show
	// that was accepted and has so far put nothing on screen.
	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen {
		t.Fatalf("a re-tap one second into an unfinished show bumped showGen to %d (was %d) — that cancels the in-flight show and restarts its retry budget from zero, which is how a repeatedly tapped field never gets a keyboard at all", got, gen)
	}

	// The step that made the first version of this fix useless on the device.
	// The Windows layer bumps sessionGen the instant TryShow returns true —
	// roughly 150ms in, long before anything is on screen, and on the same code
	// path that then schedules a 700ms verification precisely BECAUSE nothing
	// may be on screen. While the coalesce was released by sessionGen, this
	// single line was enough to let the next tap through, cancel the pending
	// verification with a new showGen, and take the legacy fallback down with
	// it. The show is still exactly as unfinished as it was a line ago, and the
	// verdict must be exactly as unmoved.
	kbd.sessionGen.Add(1)
	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen {
		t.Fatalf("an ACCEPTED show (sessionGen bumped, nothing on screen yet) released the coalesce: showGen=%d, want %d. TryShow returning true is a statement about the request, not about the screen — releasing on it reopens the starvation this window exists to close, in the one case it exists for", got, gen)
	}

	// A coalesced tap must still invalidate a queued blur-hide. It is a real
	// tap in a real field; the only thing being declined is a second show.
	if kbd.hideGen.Load() == 0 {
		t.Fatal("a coalesced tap must still bump hideGen, or a pending blur-hide closes the keyboard the show is about to open")
	}

	// The keyboard finally APPEARS — a pane Showing event, or the 700ms
	// verification finding it on screen. From here the field is the user's
	// again.
	kbd.visibleGen.Add(1)
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen+1 {
		t.Fatalf("once a keyboard session has started, a tap must dispatch again: showGen=%d, want %d. Holding the field after a SUCCESSFUL show would swallow the ordinary flow of closing the keyboard by hand and tapping the field to bring it back", got, gen+1)
	}

	// That dispatch has to re-anchor the snapshot to the value it started
	// from. If it does not, every later tap sees a visibleGen that differs
	// from a stale snapshot, the coalesce silently stops applying, and the
	// window is protected on the very first show of a process and never again
	// — a defect that no first-show test can see.
	if got := kbd.showVisibleGen.Load(); got != 1 {
		t.Fatalf("the second show dispatched after 1 seen keyboard but recorded %d; the snapshot must be re-anchored at every dispatch or the coalesce lapses after the first keyboard of the process", got)
	}
	kbd.lastShow.Store(time.Now().UnixNano() - int64(time.Second))
	requestTouchKeyboard(kbd)
	if got := kbd.showGen.Load(); got != gen+1 {
		t.Fatalf("a re-tap one second into the SECOND unfinished show bumped showGen to %d (was %d) — the coalesce must protect every show, not just the first", got, gen+1)
	}

	// And a show that simply ran out of time releases the field too, so a
	// failure can never lock a field permanently.
	stuck := new(touchKeyboardState)
	requestTouchKeyboard(stuck)
	g := stuck.showGen.Load()
	stuck.lastShow.Store(time.Now().UnixNano() - int64(keyboardShowCoalesce) - int64(time.Second))
	requestTouchKeyboard(stuck)
	if got := stuck.showGen.Load(); got != g+1 {
		t.Fatalf("a tap past the coalesce window must dispatch even though no keyboard ever appeared: showGen=%d, want %d", got, g+1)
	}
}

// tkASTCalls counts the calls under n by the name being called, taking both a
// bare identifier (helper()) and a selector (kbd.method()) by their last
// component. Shared by the guards below, which all ask the same shape of
// question about a file the linux suite cannot compile.
func tkASTCalls(n ast.Node) map[string]int {
	out := map[string]int{}
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			out[fn.Name]++
		case *ast.SelectorExpr:
			out[fn.Sel.Name]++
		}
		return true
	})
	return out
}

// The coalesce is only as good as the event that releases it, and the first
// version of it released on the wrong one. This reads the Windows source for
// the same reason the apartment tests do — none of it compiles on linux — and
// guards the distinction the two counters exist to keep apart:
//
//	sessionGen  a show was ACCEPTED. TryShow returned true. OwnerExpire binds
//	            to this, so it must move on acceptance and cannot be delayed.
//	visibleGen  a keyboard was SEEN. Only a Showing event, a pane that
//	            occludes or reports a location, or the legacy host's window.
//
// Bumping the second where only the first is known is not a subtle error: it
// hands the field back ~150ms into a show that will put nothing on screen for
// another 700ms, and the tap that then arrives cancels the verification that
// would have started the fallback. That is the whole defect, restored.
func TestOnlyASeenKeyboardReleasesTheCoalesce(t *testing.T) {
	f := tkWindowsAST(t)

	calls := tkASTCalls

	// Every case clause that handles tkPaneShown is the "TryShow said yes"
	// branch, in whichever switch it appears. None of them has seen a keyboard.
	shownClauses := 0
	sawSessionBump := false
	ast.Inspect(f, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, e := range cc.List {
			id, ok := e.(*ast.Ident)
			if !ok || id.Name != "tkPaneShown" {
				continue
			}
			shownClauses++
			c := calls(cc)
			if c["tkMarkKeyboardSeen"] > 0 {
				t.Error("the tkPaneShown branch marks a keyboard as seen. TryShow returning true is the REQUEST being accepted; the code in that same branch schedules a verification 700ms later precisely because nothing may be on screen. Releasing the tap coalesce there is the starvation bug the coalesce was added to fix")
			}
			if c["Add"] > 0 {
				sawSessionBump = true
			}
		}
		return true
	})
	if shownClauses == 0 {
		t.Fatal("no tkPaneShown case clause found in touch_keyboard_windows.go; this test can no longer see the branch it guards")
	}
	if !sawSessionBump {
		t.Error("no generation counter is bumped in any tkPaneShown branch — either the show path was restructured out from under this test, or an accepted session no longer records itself for OwnerExpire to bind to")
	}

	// A total count is not enough, and the first version of this test used one:
	// with seven call sites and a floor of four, any single path could quietly
	// drop its mark and stay green. Two injected regressions did exactly that.
	// So each closure that can conclude "a keyboard is there" is named and
	// required to say so on its own — losing any one of them is a keyboard the
	// user is already typing on while the coalesce still holds the field.
	//
	// The counts are minimums per closure, not totals:
	//
	//	commitLegacyShow      1  every call site has already seen the legacy host
	//	legacyKeyboardUp      0  it only REPORTS; its callers do the marking
	//	doVerifyShow          3  three independent probes, each able to answer yes
	//	reconcilePaneVisible  1  its found branch means the pane was OBSERVED
	//
	// The last two are the load-bearing ones, and they are load-bearing for the
	// same reason: both run on the device where the Showing events never arrive.
	// doVerifyShow is then the only confirmation a show of ours ever gets, and
	// its three probes are independent — whichever one answers is the only one
	// that will, so a probe returning "visible" without marking throws that
	// answer away. reconcilePaneVisible is the only place a keyboard opened
	// outside our own show is ever noticed at all.
	want := map[string]int{"commitLegacyShow": 1, "doVerifyShow": 3, "reconcilePaneVisible": 1}
	got := map[string]int{}
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		id, ok := as.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		if _, wanted := want[id.Name]; !wanted {
			return true
		}
		got[id.Name] = calls(as.Rhs[0])["tkMarkKeyboardSeen"]
		return false
	})
	for name, n := range want {
		have, found := got[name]
		if !found {
			t.Errorf("%s no longer exists in touch_keyboard_windows.go; the seen-marks this test guards can no longer be located, so the guard is silently doing nothing", name)
			continue
		}
		if have < n {
			t.Errorf("%s marks a keyboard as seen %d time(s), want %d. Each of those is a distinct way of learning a keyboard is on screen, and whichever one answers first is usually the only one that answers at all — an unmarked confirmation is a keyboard the user is typing on while the tap coalesce still holds the field against them", name, have, n)
		}
	}

	// The floor on the file as a whole stays, for the paths that live in case
	// clauses rather than named closures (the Showing event, the already-visible
	// pane, the legacy already-up branch).
	if total := calls(f)["tkMarkKeyboardSeen"]; total < 8 {
		t.Errorf("tkMarkKeyboardSeen is called from %d place(s), want at least 8 (legacy commit, legacy already-up, already-visible pane, Showing event, reconciliation, and doVerifyShow's three probes). A path that produces a keyboard without marking it holds the field for the full coalesce window", total)
	}

	// In the Showing handler the mark must be unconditional. The sessionGen
	// bump beside it is gated on !shownByUs — correctly, since our own shows
	// bumped it at TryShow — and putting the mark under the same guard would
	// leave our own shows with no early evidence at all.
	guarded := false
	seenInShowing := false
	ast.Inspect(f, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, e := range cc.List {
			id, ok := e.(*ast.Ident)
			if !ok || id.Name != "tkCmdShowingEvent" {
				continue
			}
			if calls(cc)["tkMarkKeyboardSeen"] > 0 {
				seenInShowing = true
			}
			ast.Inspect(cc, func(m ast.Node) bool {
				ifs, ok := m.(*ast.IfStmt)
				if !ok {
					return true
				}
				if calls(ifs)["tkMarkKeyboardSeen"] > 0 {
					guarded = true
				}
				return true
			})
		}
		return true
	})
	if !seenInShowing {
		t.Error("the Showing handler does not mark a keyboard as seen, and a Showing event IS the pane appearing — the hardest evidence available anywhere in that file")
	}
	if guarded {
		t.Error("the seen-mark in the Showing handler sits inside a conditional. A Showing arrives for our own shows too, where it is the earliest proof the show worked; gating it (on shownByUs, as the sessionGen bump beside it correctly is) holds the field while the user is already typing")
	}
}

// keyboardShowCoalesce is not a free parameter: it has to outlast the work it
// is protecting, and that work is timed by four constants in the Windows file
// that nothing here compiles or imports. Nobody editing tkLegacyHostPolls has
// any reason to look at a constant in touch_input.go, so the coupling is read
// out of the source and checked.
//
// The previous value was 3.5s, derived from the legacy budget alone. The
// derivation left out tkShowVerifyDelay — the 700ms that must elapse before
// the legacy path is even entered — so the window expired with the fallback
// still running, in its final and most productive second.
func TestTheCoalesceWindowCoversTheFallbackItProtects(t *testing.T) {
	f := tkWindowsAST(t)

	// lead returns the leading integer literal of a constant's value, which
	// covers both spellings used over there: a plain count (8) and a duration
	// (150 * time.Millisecond).
	lead := func(name string) (int64, bool) {
		var out int64
		ok := false
		ast.Inspect(f, func(n ast.Node) bool {
			vs, is := n.(*ast.ValueSpec)
			if !is {
				return true
			}
			for i, id := range vs.Names {
				if id.Name != name || i >= len(vs.Values) {
					continue
				}
				e := vs.Values[i]
				if b, is := e.(*ast.BinaryExpr); is {
					e = b.X
				}
				lit, is := e.(*ast.BasicLit)
				if !is || lit.Kind != token.INT {
					continue
				}
				v, err := strconv.ParseInt(lit.Value, 0, 64)
				if err != nil {
					continue
				}
				out, ok = v, true
			}
			return true
		})
		return out, ok
	}

	need := func(name string) int64 {
		v, ok := lead(name)
		if !ok {
			t.Fatalf("cannot read %s out of touch_keyboard_windows.go; the coalesce window is derived from it and can no longer be checked against it", name)
		}
		return v
	}

	verify := time.Duration(need("tkShowVerifyDelay")) * time.Millisecond
	poll := time.Duration(need("tkLegacyVerifyDelay")) * time.Millisecond
	chain := verify + time.Duration(need("tkLegacyVerifyPolls"))*poll + time.Duration(need("tkLegacyHostPolls"))*poll

	// Room for starting TabTip.exe between those two polling stretches, and for
	// COM activation on a tablet that has just woken up. A second is a guess,
	// but it is a guess on the safe side: being too generous costs a field that
	// stays coalesced slightly longer during a show that is failing anyway,
	// while being too tight costs the keyboard itself.
	const slack = time.Second

	if keyboardShowCoalesce < chain+slack {
		t.Errorf("keyboardShowCoalesce is %v, but a show that has to fall back spends %v (%v verifying the accepted show, then the legacy host's own polling) plus process start before it can succeed. The window has to outlast that or a re-tap lands inside the fallback and cancels it — which is the exact failure the window was added to prevent",
			keyboardShowCoalesce, chain+slack, verify)
	}

	// And the other direction: it must stay well under the retry ladder, or it
	// stops being a coalesce and becomes a lockout on a dead field. The ladder
	// is summed from the function rather than approximated as max × 1s — the
	// first draft of this test did approximate it, got 5s instead of 15s, and
	// failed against a correct constant. keyboardShowRetryDelay is ordinary Go
	// that compiles here, so there is no excuse for guessing at it.
	var ladder time.Duration
	for i := int8(0); ; i++ {
		d, ok := keyboardShowRetryDelay(i)
		if !ok {
			break
		}
		ladder += d
	}
	if keyboardShowCoalesce >= ladder {
		t.Errorf("keyboardShowCoalesce (%v) has grown to the scale of the whole retry ladder (%v). Past that it is no longer absorbing an impatient re-tap, it is refusing the user's input for as long as the subsystem feels like trying",
			keyboardShowCoalesce, ladder)
	}
}

// The one number that mattered most in this whole subsystem turned out to be a
// classification, not a call: RO_E_UNSUPPORTED_FROM_MTA was filed as transient,
// so a tap spent fifteen seconds re-issuing an activation whose answer could
// not change, with the legacy host — the one path that could still have
// produced a keyboard — withheld for the entire time, and then gave up. The
// device trace is unambiguous about the cost: the user tapped a text field and
// nothing ever appeared.
//
// So the values are written out in full here rather than referenced. A test
// that says tkPaneHRPermanent(tkROEUnsupportedFromMTA) is true passes just as
// happily when the constant holds the wrong number, and the wrong number is
// precisely the failure this is guarding: the classifier would be correct and
// the device would still see nothing.
func TestPermanentPaneHRESULTsAreTheOnesThatCannotChange(t *testing.T) {
	permanent := []struct {
		hr   uintptr
		name string
		why  string
	}{
		{0x80040154, "REGDB_E_CLASSNOTREG", "the class is not registered; no amount of waiting registers it"},
		{0x80004002, "E_NOINTERFACE", "IInputPaneInterop arrived in Win10 1607; an older build will not grow it"},
		{0x8000001D, "RO_E_UNSUPPORTED_FROM_MTA", "a running thread's apartment cannot be changed, so this answer is fixed for its lifetime"},
	}
	for _, c := range permanent {
		if !tkPaneHRPermanent(c.hr) {
			t.Fatalf("%s (%#x) classified transient — %s. A transient verdict puts the show on a "+
				"~15s ladder that withholds the legacy host, so this is a tap that raises no keyboard at all", c.name, c.hr, c.why)
		}
	}

	// Named against the constants too, so a renumbered constant fails here
	// rather than silently detaching the classifier from the call sites.
	for name, got := range map[string]uintptr{
		"tkRegdbEClassNotReg":     tkRegdbEClassNotReg,
		"tkENoInterface":          tkENoInterface,
		"tkROEUnsupportedFromMTA": tkROEUnsupportedFromMTA,
	} {
		found := false
		for _, c := range permanent {
			if c.hr == got {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s = %#x, which is not one of the three HRESULTs this rule is written about", name, got)
		}
	}

	// The other direction is load-bearing in exactly the opposite way. These
	// are the failures a retry can genuinely clear — a shell still starting, a
	// server that died mid-call, a momentarily denied activation. Calling any
	// of them permanent skips the ladder and fires a global, non-idempotent
	// Toggle straight away, which on Windows 11 can CLOSE the keyboard the user
	// is typing on. RPC_E_CHANGED_MODE is in the list for a subtler reason: it
	// comes back from RoInitialize, never from an activation, and misfiling it
	// here would be a plausible-looking way to conflate "wrong apartment" with
	// "cannot initialize".
	transient := []struct {
		hr   uintptr
		name string
	}{
		{0x80004001, "E_NOTIMPL"},
		{0x80004005, "E_FAIL"},
		{0x80070005, "E_ACCESSDENIED"},
		{0x8007000E, "E_OUTOFMEMORY"},
		{0x80010106, "RPC_E_CHANGED_MODE"},
		{0x80010108, "RPC_E_DISCONNECTED"},
		{0x80080005, "CO_E_SERVER_EXEC_FAILURE"},
		{0x800706BA, "RPC_S_SERVER_UNAVAILABLE"},
		{0, "S_OK"},
	}
	for _, c := range transient {
		if tkPaneHRPermanent(c.hr) {
			t.Fatalf("%s (%#x) classified permanent — the show would skip its retries and hand a "+
				"non-idempotent Toggle to the legacy host, which can close a keyboard the user is using", c.name, c.hr)
		}
	}
}

// The same HRESULT, asked a different question, gives the opposite answer, and
// for four rounds it only ever got asked one of them. "Should the show stop
// retrying and go to the legacy host" is true for all three permanent codes.
// "Does the screen have no keyboard on it" is true for two of them and unknown
// for the third: REGDB_E_CLASSNOTREG and E_NOINTERFACE say there is no WinRT
// pane on this build, so the legacy IPTip host IS the keyboard and not finding
// one is real evidence; RO_E_UNSUPPORTED_FROM_MTA says the pane is there and
// this thread may not look at it, which is evidence about nothing.
//
// The literal is written out for the reason the permanent test gives: a
// classifier that reads the wrong constant is correct and useless at once.
func TestApartmentRefusalIsNotEvidenceAboutTheScreen(t *testing.T) {
	if !tkPaneHRApartment(0x8000001D) {
		t.Fatalf("RO_E_UNSUPPORTED_FROM_MTA (0x8000001D) not recognized as the apartment refusal. " +
			"Every reader then treats an unreadable pane as an absent one, which is the whole defect")
	}
	if tkROEUnsupportedFromMTA != 0x8000001D {
		t.Fatalf("tkROEUnsupportedFromMTA = %#x, not RO_E_UNSUPPORTED_FROM_MTA", tkROEUnsupportedFromMTA)
	}

	// The two that really do mean "there is no WinRT pane here". Classifying
	// either of them as the apartment refusal would take away the legacy
	// path's only teardown signal on the builds that depend on it.
	for _, c := range []struct {
		hr   uintptr
		name string
	}{
		{0x80040154, "REGDB_E_CLASSNOTREG"},
		{0x80004002, "E_NOINTERFACE"},
		{0x80004005, "E_FAIL"},
		{0x80010106, "RPC_E_CHANGED_MODE"},
		{0, "S_OK (the combase-procs branch, which reports hr 0)"},
	} {
		if tkPaneHRApartment(c.hr) {
			t.Fatalf("%s (%#x) read as the apartment refusal — a genuinely absent pane would then "+
				"never resolve, and the legacy session would keep its clearance for good", c.name, c.hr)
		}
	}

	// The apartment refusal must stay a SUBSET of the permanent set. Splitting
	// the visibility question off is not a licence to re-open the show ladder
	// that the device trace closed: fifteen seconds of re-activation with the
	// legacy host withheld, and no keyboard at the end of it.
	if !tkPaneHRPermanent(tkROEUnsupportedFromMTA) {
		t.Fatalf("the apartment refusal fell out of the permanent set. Show routing would put it " +
			"back on the ~15s retry ladder, which is the round-77 defect returning")
	}
}

// tkLegacyPaneSample is the occlusion monitor's half of the same split. The
// monitor polls four times a second and twelve consecutive all-zero samples
// expire the session's ownership and its padding. Under the apartment refusal
// the IPTip probe answers "nothing there" for a keyboard the user is typing
// on — Windows 11 rarely hosts one in an IPTip_Main_Window — so counting that
// as a sample tears the session down about three seconds after it opened.
func TestLegacyPaneSampleKeepsTheApartmentUnknown(t *testing.T) {
	for _, c := range []struct {
		name     string
		hr       uintptr
		legacyUp bool
		sampled  bool
		up       bool
		why      string
	}{
		{
			"absent class, host seen", tkRegdbEClassNotReg, true, true, true,
			"a sighting is a sighting: the legacy host is on screen",
		},
		{
			"absent class, no host", tkRegdbEClassNotReg, false, true, false,
			"there is no WinRT pane on this build, so the empty screen is an answer and the only teardown signal there is",
		},
		{
			"apartment refusal, host seen", tkROEUnsupportedFromMTA, true, true, true,
			"the refusal blinds the WinRT probe, not the window-class one; a keyboard seen is still a keyboard",
		},
		{
			"apartment refusal, no host", tkROEUnsupportedFromMTA, false, false, false,
			"the pane is there and unreadable from this thread, so this is not an observation at all",
		},
		{
			"combase procs missing", 0, false, true, false,
			"tkInputPaneFactory reports hr 0 for a build with no WinRT exports; that is genuine absence",
		},
	} {
		sampled, up := tkLegacyPaneSample(c.hr, c.legacyUp)
		if sampled != c.sampled || up != c.up {
			t.Errorf("%s: tkLegacyPaneSample(%#x, %v) = (%v, %v), want (%v, %v) — %s",
				c.name, c.hr, c.legacyUp, sampled, up, c.sampled, c.up, c.why)
		}
	}
}

// tkASTBoolPair reads a `return <bool>, <bool>` statement. Anything else — a
// named variable, a call, a different arity — is reported as unreadable rather
// than guessed at, so a rewritten exit fails the guard loudly instead of
// passing it by accident.
func tkASTBoolPair(n ast.Node) (first, second, ok bool) {
	ret, isReturn := n.(*ast.ReturnStmt)
	if !isReturn || len(ret.Results) != 2 {
		return false, false, false
	}
	var vals [2]bool
	for i, r := range ret.Results {
		id, isIdent := r.(*ast.Ident)
		if !isIdent || (id.Name != "true" && id.Name != "false") {
			return false, false, false
		}
		vals[i] = id.Name == "true"
	}
	return vals[0], vals[1], true
}

// The two pure functions above are unit-tested because they compile on linux.
// Their CALLERS do not: both live in touch_keyboard_windows.go behind
// //go:build windows, and both are the whole point of the split — a classifier
// nobody consults is a comment. So this reads the source, for the reason
// tkWindowsAST gives.
//
// The callers are the two places a tkPaneUnavailable factory is read as
// evidence about the SCREEN rather than about routing. legacyShow's
// legacyKeyboardUp decides whether a second, non-idempotent Toggle may fire —
// answering "no keyboard" from the apartment refusal is what closes the
// keyboard the first Toggle raised. The occlusion monitor decides whether a
// poll counted at all — counting the refusal as a zero sample expires the
// session's ownership and its padding about three seconds in.
func TestTheApartmentRefusalIsNotReadAsAnAbsentKeyboardByItsCallers(t *testing.T) {
	f := tkWindowsAST(t)

	// 1. legacyShow. The tkPaneUnavailable arm has to answer the two facts
	// differently, so both the branch and its polarity are checked: a guard
	// that only looked for the call would pass on the two returns swapped,
	// which is the same defect with the classifier wired in backwards.
	var clause *ast.CaseClause
	ast.Inspect(f, func(n ast.Node) bool {
		as, isAssign := n.(*ast.AssignStmt)
		if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		id, isIdent := as.Lhs[0].(*ast.Ident)
		if !isIdent || id.Name != "legacyKeyboardUp" {
			return true
		}
		ast.Inspect(as.Rhs[0], func(m ast.Node) bool {
			cc, isCase := m.(*ast.CaseClause)
			if !isCase {
				return true
			}
			for _, e := range cc.List {
				if lit, isIdent := e.(*ast.Ident); isIdent && lit.Name == "tkPaneUnavailable" {
					clause = cc
				}
			}
			return true
		})
		return false
	})
	if clause == nil {
		t.Fatal("no tkPaneUnavailable case inside legacyKeyboardUp; this guard can no longer find the branch it exists for and would pass in silence")
	}

	var split *ast.IfStmt
	for _, st := range clause.Body {
		is, isIf := st.(*ast.IfStmt)
		if isIf && tkASTCalls(is.Cond)["tkPaneHRApartment"] > 0 {
			split = is
		}
	}
	if split == nil {
		t.Fatal("legacyKeyboardUp reads tkPaneUnavailable without asking tkPaneHRApartment which of the two facts it is. Every Windows 11 session where IPTip_Main_Window is absent then reports a conclusive \"no keyboard\" after a successful first Toggle, and tkMayRetoggle fires the second one that closes it")
	}
	if len(split.Body.List) != 1 {
		t.Fatalf("the apartment branch of legacyKeyboardUp has %d statements, want 1 return; this guard reads that return and cannot judge anything else", len(split.Body.List))
	}
	up, conclusive, readable := tkASTBoolPair(split.Body.List[0])
	if !readable {
		t.Fatal("the apartment branch of legacyKeyboardUp no longer ends in a literal (up, conclusive) return, so its polarity cannot be checked here")
	}
	if up || conclusive {
		t.Errorf("the apartment branch returns (up %v, conclusive %v), want (false, false). RO_E_UNSUPPORTED_FROM_MTA means this thread cannot see a pane that is there; reporting it as conclusive hands tkMayRetoggle permission to Toggle a keyboard that is already up", up, conclusive)
	}

	last := clause.Body[len(clause.Body)-1]
	up, conclusive, readable = tkASTBoolPair(last)
	if !readable {
		t.Fatal("the genuinely-absent arm of legacyKeyboardUp no longer ends in a literal (up, conclusive) return")
	}
	if up || !conclusive {
		t.Errorf("the absent-class arm returns (up %v, conclusive %v), want (false, true). On a build with no WinRT pane the legacy host IS the keyboard, so not finding one is a real answer — losing it strands legacyShow with no verdict at all", up, conclusive)
	}

	// 2. The occlusion monitor. Same status, same two facts, and the only thing
	// this can check from linux is that the branch still routes the question
	// through the classifier rather than reading the IPTip probe raw.
	branches := 0
	ast.Inspect(f, func(n ast.Node) bool {
		is, isIf := n.(*ast.IfStmt)
		if !isIf {
			return true
		}
		bin, isBinary := is.Cond.(*ast.BinaryExpr)
		if !isBinary || bin.Op != token.EQL {
			return true
		}
		id, isIdent := bin.Y.(*ast.Ident)
		if !isIdent || id.Name != "tkPaneUnavailable" {
			return true
		}
		branches++
		if tkASTCalls(is.Body)["tkLegacyPaneSample"] == 0 {
			t.Error("the occlusion monitor's tkPaneUnavailable branch does not go through tkLegacyPaneSample. Reading touchKeyboardVisible directly counts the apartment refusal as an observed empty screen, and twelve of those in three seconds expire the padding under a keyboard the user is typing on")
		}
		return true
	})
	if branches == 0 {
		t.Fatal("no branch on status == tkPaneUnavailable found; the monitor path this guard covers can no longer be located")
	}
}

// The end-to-end consequence, composed from the two pure pieces the way
// legacyShow composes them: legacyKeyboardUp turns the factory status into
// (up, conclusive), and tkMayRetoggle turns conclusive into permission to fire
// a second, non-idempotent Toggle.
//
// The sequence that made this a P1: the first Toggle is accepted and raises a
// keyboard, the poll loop asks whether it is up, the pane factory is refused
// for the apartment, the IPTip host does not exist on this Windows 11 machine,
// and the old code answered "conclusively not up". With an accepted Toggle
// outstanding, that is precisely the input tkMayRetoggle exists to refuse —
// and it was the input that made it say yes.
func TestApartmentRefusalCannotAuthorizeTheSecondToggle(t *testing.T) {
	// conclusive mirrors the tkPaneUnavailable arm of legacyKeyboardUp.
	conclusive := func(hr uintptr) bool { return !tkPaneHRApartment(hr) }

	const (
		poll   = 5 // past tkLegacyHostSettlePolls (3); windows-only constant
		settle = 3
	)
	if tkMayRetoggle(poll, settle, false, true, conclusive(tkROEUnsupportedFromMTA)) {
		t.Fatalf("a second Toggle authorized while the first is outstanding and the only " +
			"negative came from a pane this thread may not read. Toggle is not idempotent: " +
			"this closes the keyboard the first Toggle raised")
	}

	// The two directions that must keep working, so the fix is not a blanket
	// "never retoggle".
	if !tkMayRetoggle(poll, settle, false, true, conclusive(tkRegdbEClassNotReg)) {
		t.Fatalf("no second Toggle on a build with no WinRT pane at all, where the empty legacy " +
			"host really is proof the first Toggle produced nothing. That tap raises no keyboard")
	}
	if !tkMayRetoggle(poll, settle, false, false, conclusive(tkROEUnsupportedFromMTA)) {
		t.Fatalf("no Toggle with none outstanding — there is no keyboard to lose, and the " +
			"caller's precondition already says the screen was empty")
	}
}

// The legacy show has committed a keyboard nobody had seen twice now: first on
// the Toggle HRESULT alone, then on a verdict that only meant "the probes could
// not tell". The cost is the same both times and it is not subtle — paneVisible
// makes the layout reserve room for a keyboard that is not on screen, the
// session it opens debounces the user's next tap away, and the function returns
// without escalating, so nothing tries again. Every step reports success and
// the user gets no keyboard at all. So the mapping is pinned, exhaustively.
func TestOnlyASeenKeyboardCommitsTheLegacyShow(t *testing.T) {
	all := []struct {
		verdict tkVisibilityVerdict
		name    string
	}{
		{tkVisibleSeen, "seen"},
		{tkVisibleCancelled, "cancelled"},
		{tkVisibleUnknown, "unknown"},
		{tkVisibleAbsent, "absent"},
	}
	for _, c := range all {
		plan := tkLegacyVerdictPlan(c.verdict)
		if got, want := plan.commit, c.verdict == tkVisibleSeen; got != want {
			t.Fatalf("%s: commit = %v, want %v — only a keyboard somebody actually saw may be recorded as shown", c.name, got, want)
		}
		if got, want := plan.compensate, c.verdict == tkVisibleCancelled; got != want {
			t.Fatalf("%s: compensate = %v, want %v — a superseded show may still be raising a keyboard, and only that case is retryably undone", c.name, got, want)
		}
		escalates := plan.escalateNote != ""
		if want := !plan.commit && !plan.compensate; escalates != want {
			t.Fatalf("%s: escalates = %v, want %v — the three outcomes are meant to be mutually exclusive", c.name, escalates, want)
		}
	}

	// The two escalating verdicts do not mean the same thing, and the log is
	// the only place that distinction ever reaches a human: "every probe said
	// the screen was empty" and "no probe could answer" have different fixes.
	if a, b := tkLegacyVerdictPlan(tkVisibleUnknown).escalateNote, tkLegacyVerdictPlan(tkVisibleAbsent).escalateNote; a == b {
		t.Fatalf("an unverifiable show and a confirmed-empty screen log the same line (%q); a device trace cannot tell them apart", a)
	}
}

// The recovery pass after (re)starting TabTip.exe gets exactly one Toggle, and
// for a while it got none: the flag saying "a Toggle is outstanding" was set
// against the PREVIOUS host and never cleared, so the pass watched a freshly
// started process that had never been asked to show anything. Unless the
// launch alone raised a keyboard, the tap could not succeed. These pin both
// halves of the ration — that it fires, and that it does not fire twice or
// blind.
func TestLegacyRetoggleIsRationedNotForbidden(t *testing.T) {
	const settle = 3

	// The case the field hit: an earlier Toggle went to a host that answered
	// and showed nothing, the host has been restarted, and the probes agree
	// the screen is empty. This MUST toggle, or the show cannot happen.
	if !tkMayRetoggle(settle, settle, false, true, true) {
		t.Fatal("a restarted host with a conclusively empty screen must be toggled — " +
			"a recovery pass that only watches is the reason a tap produces no keyboard at all")
	}

	// One, though.
	if tkMayRetoggle(settle+1, settle, true, true, true) {
		t.Fatal("Toggle is not idempotent: a second one would close the keyboard the first is raising")
	}

	// Not before the host can answer. A Toggle fired at a process that is
	// still starting spends the single attempt on nobody.
	for i := 0; i < settle; i++ {
		if tkMayRetoggle(i, settle, false, true, true) {
			t.Fatalf("poll %d is inside the settle window; the host has no COM server to accept a Toggle yet", i)
		}
	}

	// Not blind, either: an outstanding Toggle plus a probe that cannot answer
	// is exactly the state where a second Toggle would undo the first.
	if tkMayRetoggle(settle, settle, false, true, false) {
		t.Fatal("with a Toggle outstanding and no answer from the probes, another Toggle could close a keyboard that is coming up")
	}

	// But with nothing outstanding there is no such keyboard to lose, and
	// every caller of legacyShow got here by establishing an empty screen. A
	// probe that cannot answer must not strand the tap.
	if !tkMayRetoggle(settle, settle, false, false, false) {
		t.Fatal("no Toggle outstanding means nothing to cancel; refusing to toggle here strands a tap on a broken probe")
	}
}
