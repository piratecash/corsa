package desktop

import (
	"image"
	"math"
	"time"

	"gioui.org/f32"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/unit"
	"gioui.org/widget"
)

// image_viewer_input.go is everything the viewer reads: the keys that step
// and close it, and the finger and mouse gestures over the picture.
//
// The gestures are recognised by a state machine that takes one pointer
// event and returns one thing to do, rather than by reaching into the
// viewer's fields from inside the pointer loop. Pinch and double-tap have no
// equivalent in gioui.org/gesture and are the kind of thing that is only
// ever right by accident unless it can be tested without a window — see
// image_viewer_test.go.

const (
	// viewerTapSlopDp is how far a finger may travel and still be a tap. The
	// same order as the long-press slop the chat bubbles use: a finger held
	// still on a screen still moves a little.
	viewerTapSlopDp = 10
	// viewerSwipeDp is how far a finger has to travel across the picture
	// before it means "the next image". Deliberately larger than the tap
	// slop and than a stray sideways wobble during a vertical drag.
	viewerSwipeDp = 48
	// viewerDoubleTapWindow is how long the second tap of a double tap may
	// take to arrive. The platform values sit between 300ms and 500ms;
	// shorter than this and a deliberate double tap is read as two taps.
	viewerDoubleTapWindow = 350 * time.Millisecond
	// viewerTapMaxDuration keeps a long press from becoming half of a double
	// tap when the finger happens not to move.
	viewerTapMaxDuration = 500 * time.Millisecond
	// viewerDoubleTapZoom is where a double tap magnifies to, and back from.
	viewerDoubleTapZoom = 200
)

// viewerGestureKind is what one pointer event asked the viewer to do.
type viewerGestureKind uint8

const (
	viewerGestureNone viewerGestureKind = iota
	// viewerGesturePan drags a magnified picture under the pointer.
	viewerGesturePan
	// viewerGestureZoom is a pinch: an absolute zoom around a point, not a
	// step, because a pinch is continuous.
	viewerGestureZoom
	// viewerGestureToggleZoom is a double tap: fitted ↔ viewerDoubleTapZoom
	// around the point touched.
	viewerGestureToggleZoom
	// viewerGestureStep is a swipe to the neighbouring image.
	viewerGestureStep
)

// viewerGesture is one recognised gesture. Only the fields its Kind names
// are meaningful.
type viewerGesture struct {
	Kind   viewerGestureKind
	Pan    image.Point
	Zoom   float32
	Anchor f32.Point
	Step   int
}

// viewerGestureEnv is what the picture on screen allows right now. The
// recognizer holds no viewer state of its own: what a drag MEANS depends on
// whether the picture is magnified and whether it has already been dragged
// as far as it goes, and both are measured during layout.
type viewerGestureEnv struct {
	// Zoom is the current zoom, so the recognizer can tell a pan from a
	// swipe without asking the viewer.
	Zoom float32
	// AtStartEdge and AtEndEdge report that the picture cannot be panned any
	// further towards the previous / next image. At fitted zoom both are
	// true: there is nothing to pan, so every swipe steps.
	AtStartEdge bool
	AtEndEdge   bool
	// SlopPx and SwipePx are the dp constants above in this frame's pixels.
	SlopPx  float32
	SwipePx float32
}

// viewerGestures recognises the viewer's touch and mouse gestures.
type viewerGestures struct {
	// touches are the fingers currently down on the picture, by pointer id.
	// A map rather than a pair of slots because a third finger must not
	// silently replace one of the two the pinch is following.
	touches map[pointer.ID]f32.Point

	dragID     pointer.ID
	dragging   bool
	dragTouch  bool // the drag came from a finger, so it may become a swipe
	dragLast   f32.Point
	dragTravel float32 // total distance moved, for the tap test
	// swipeDX accumulates only the travel that did NOT pan the picture:
	// while a magnified picture still has room to move, dragging it is
	// dragging it, and only once its edge is against the viewport does the
	// same finger start asking for the next image.
	swipeDX float32
	dragAt  time.Duration
	// panRest carries the sub-pixel part of a drag into the next event. The
	// pan is applied in whole pixels, and a touchpad on a high-DPI screen
	// reports fractions of one — truncating each of them separately makes a
	// slow drag move nothing at all.
	panRest f32.Point

	// pinch follows exactly two fingers. baseDistance is their distance when
	// the second one landed and baseZoom the zoom it started from (taken on
	// the first move, which is the first point where the recognizer is told
	// what the zoom is), so the zoom follows the fingers absolutely instead
	// of drifting by accumulated deltas.
	pinching     bool
	pinchIDs     [2]pointer.ID
	baseDistance float32
	baseZoom     float32

	// A finger lifted out of a pinch leaves the other one down. That finger
	// must not become a swipe or a tap: the user is finishing a zoom, not
	// starting a gesture.
	suppressDrag bool

	hasTap     bool
	lastTapAt  time.Duration
	lastTapPos f32.Point
}

func (g *viewerGestures) reset() {
	*g = viewerGestures{}
}

// handle folds one pointer event into the gesture state and reports what it
// means.
func (g *viewerGestures) handle(event pointer.Event, env viewerGestureEnv) viewerGesture {
	switch event.Kind {
	case pointer.Press:
		return g.press(event)
	case pointer.Drag:
		return g.drag(event, env)
	case pointer.Release:
		return g.release(event, env)
	case pointer.Cancel:
		g.reset()
	}
	return viewerGesture{}
}

func (g *viewerGestures) press(event pointer.Event) viewerGesture {
	if event.Source != pointer.Touch {
		// A mouse or pen drags the picture and nothing else: there is no
		// mouse swipe between images (the arrows and the keys are that), and
		// a click that pans by a pixel must not read as a tap.
		g.beginDrag(event, false)
		return viewerGesture{}
	}
	if g.touches == nil {
		g.touches = make(map[pointer.ID]f32.Point)
	}
	g.touches[event.PointerID] = event.Position
	if len(g.touches) == 2 {
		g.beginPinch()
		return viewerGesture{}
	}
	if len(g.touches) > 2 {
		// More fingers than any gesture here uses: stop interpreting until
		// the screen is clear again.
		g.dragging = false
		g.pinching = false
		return viewerGesture{}
	}
	g.beginDrag(event, true)
	return viewerGesture{}
}

func (g *viewerGestures) beginDrag(event pointer.Event, touch bool) {
	g.dragID = event.PointerID
	g.dragging = true
	g.dragTouch = touch
	g.dragLast = event.Position
	g.dragTravel = 0
	g.swipeDX = 0
	g.panRest = f32.Point{}
	g.dragAt = event.Time
	g.suppressDrag = false
}

// wholePixels turns a fractional drag into whole pixels, keeping what is left
// over for the next one.
func (g *viewerGestures) wholePixels(delta f32.Point) image.Point {
	total := g.panRest.Add(delta)
	whole := image.Pt(int(total.X), int(total.Y))
	g.panRest = total.Sub(f32.Pt(float32(whole.X), float32(whole.Y)))
	return whole
}

func (g *viewerGestures) beginPinch() {
	ids := make([]pointer.ID, 0, 2)
	for id := range g.touches {
		ids = append(ids, id)
	}
	// Two fingers, so the order only has to be stable within this pinch.
	if ids[0] > ids[1] {
		ids[0], ids[1] = ids[1], ids[0]
	}
	g.pinchIDs = [2]pointer.ID{ids[0], ids[1]}
	g.baseDistance = distance(g.touches[ids[0]], g.touches[ids[1]])
	g.pinching = g.baseDistance > 0
	g.dragging = false
}

func (g *viewerGestures) drag(event pointer.Event, env viewerGestureEnv) viewerGesture {
	if event.Source == pointer.Touch {
		if g.touches == nil {
			g.touches = make(map[pointer.ID]f32.Point)
		}
		g.touches[event.PointerID] = event.Position
	}
	if g.pinching {
		return g.pinch(env)
	}
	if !g.dragging || event.PointerID != g.dragID {
		return viewerGesture{}
	}
	delta := event.Position.Sub(g.dragLast)
	g.dragLast = event.Position
	g.dragTravel += absFloat(delta.X) + absFloat(delta.Y)

	if env.Zoom > viewerMinZoom() {
		if g.dragTouch && edgeReached(delta.X, env) {
			g.swipeDX += delta.X
		}
		return viewerGesture{Kind: viewerGesturePan, Pan: g.wholePixels(delta)}
	}
	if g.dragTouch {
		g.swipeDX += delta.X
	}
	return viewerGesture{}
}

// edgeReached reports whether a drag in this direction has nothing left to
// pan, which is when it starts counting towards a swipe.
func edgeReached(dx float32, env viewerGestureEnv) bool {
	if dx > 0 {
		return env.AtStartEdge
	}
	if dx < 0 {
		return env.AtEndEdge
	}
	return false
}

func (g *viewerGestures) pinch(env viewerGestureEnv) viewerGesture {
	first, firstDown := g.touches[g.pinchIDs[0]]
	second, secondDown := g.touches[g.pinchIDs[1]]
	if !firstDown || !secondDown || g.baseDistance <= 0 {
		return viewerGesture{}
	}
	if g.baseZoom == 0 {
		g.baseZoom = env.Zoom
	}
	current := distance(first, second)
	if current <= 0 {
		return viewerGesture{}
	}
	return viewerGesture{
		Kind:   viewerGestureZoom,
		Zoom:   g.baseZoom * current / g.baseDistance,
		Anchor: midpoint(first, second),
	}
}

func (g *viewerGestures) release(event pointer.Event, env viewerGestureEnv) viewerGesture {
	if event.Source == pointer.Touch {
		delete(g.touches, event.PointerID)
	}
	if g.pinching {
		// The zoom is finished when either finger leaves. A finger still on
		// the screen carries on dragging the picture — the user is moving
		// around what they just magnified — but it can no longer become a
		// swipe or half of a double tap: it belongs to the pinch.
		g.pinching = false
		g.baseZoom = 0
		g.dragging = false
		g.hasTap = false
		g.resumeDragAfterPinch(event.Time)
		return viewerGesture{}
	}
	if !g.dragging || event.PointerID != g.dragID {
		if len(g.touches) == 0 {
			g.suppressDrag = false
		}
		return viewerGesture{}
	}
	g.dragging = false
	suppressed := g.suppressDrag
	if len(g.touches) == 0 {
		g.suppressDrag = false
	}
	if !g.dragTouch || suppressed {
		return viewerGesture{}
	}
	if g.dragTravel <= env.SlopPx && event.Time-g.dragAt <= viewerTapMaxDuration {
		return g.tap(event, env)
	}
	g.hasTap = false
	if absFloat(g.swipeDX) < env.SwipePx {
		return viewerGesture{}
	}
	// A finger travelling left pulls the next image in from the right.
	step := 1
	if g.swipeDX > 0 {
		step = -1
	}
	return viewerGesture{Kind: viewerGestureStep, Step: step}
}

// resumeDragAfterPinch hands the finger left on the screen back to the drag
// path, so a pinch followed by a one-finger move keeps panning instead of
// waiting for that finger to be lifted and put down again.
func (g *viewerGestures) resumeDragAfterPinch(now time.Duration) {
	if len(g.touches) != 1 {
		return
	}
	for id, position := range g.touches {
		g.dragID = id
		g.dragging = true
		g.dragTouch = true
		g.dragLast = position
		g.dragTravel = 0
		g.swipeDX = 0
		g.panRest = f32.Point{}
		g.dragAt = now
	}
	g.suppressDrag = true
}

func (g *viewerGestures) tap(event pointer.Event, env viewerGestureEnv) viewerGesture {
	if g.hasTap &&
		event.Time-g.lastTapAt <= viewerDoubleTapWindow &&
		distance(g.lastTapPos, event.Position) <= 2*env.SlopPx {
		g.hasTap = false
		return viewerGesture{Kind: viewerGestureToggleZoom, Anchor: event.Position}
	}
	g.hasTap = true
	g.lastTapAt = event.Time
	g.lastTapPos = event.Position
	return viewerGesture{}
}

func distance(a, b f32.Point) float32 {
	d := a.Sub(b)
	return float32(math.Hypot(float64(d.X), float64(d.Y)))
}

func midpoint(a, b f32.Point) f32.Point {
	return f32.Pt((a.X+b.X)/2, (a.Y+b.Y)/2)
}

func absFloat(v float32) float32 {
	if v < 0 {
		return -v
	}
	return v
}

// drainClicks throws away every click still queued on these widgets.
//
// A Clickable hands out its clicks one at a time, so a control that acts on
// the first one and then stops being drawn keeps the rest — and delivers
// them, out of context, the next time it appears. For a confirmation that is
// a destructive action nobody asked for.
func drainClicks(gtx layout.Context, buttons ...*widget.Clickable) {
	for _, button := range buttons {
		for button.Clicked(gtx) {
		}
	}
}

// apply performs one recognised gesture on the viewer.
func (v *imageViewer) apply(gesture viewerGesture) {
	switch gesture.Kind {
	case viewerGesturePan:
		v.panBy(gesture.Pan)
	case viewerGestureZoom:
		v.setZoom(gesture.Zoom, gesture.Anchor)
	case viewerGestureToggleZoom:
		if v.zoom > viewerMinZoom() {
			v.setZoom(viewerMinZoom(), gesture.Anchor)
			return
		}
		v.setZoom(viewerDoubleTapZoom, gesture.Anchor)
	case viewerGestureStep:
		v.step(gesture.Step)
	}
}

// gestureEnv describes the picture as this frame drew it.
func (v *imageViewer) gestureEnv(gtx layout.Context) viewerGestureEnv {
	display := v.displaySize()
	return viewerGestureEnv{
		Zoom:        v.zoom,
		AtStartEdge: viewerAtHorizontalEdge(v.offset, display, v.viewport, 1),
		AtEndEdge:   viewerAtHorizontalEdge(v.offset, display, v.viewport, -1),
		SlopPx:      float32(gtx.Dp(unit.Dp(viewerTapSlopDp))),
		SwipePx:     float32(gtx.Dp(unit.Dp(viewerSwipeDp))),
	}
}

// readImageGestures drains the picture area's pointer events. The area is
// registered by the caller, which is also what clips it — a drag that leaves
// the picture keeps panning it, and Gio delivers those to the tag the press
// landed on.
func (v *imageViewer) readImageGestures(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(
			pointer.Filter{
				Target: &v.imageTag,
				Kinds:  pointer.Press | pointer.Drag | pointer.Release | pointer.Cancel,
			},
			pointer.Filter{
				Target:  &v.imageTag,
				Kinds:   pointer.Scroll,
				ScrollY: pointer.ScrollRange{Min: -1, Max: 1},
			},
		)
		if !ok {
			return
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		if v.confirmDelete {
			// The question is about one image; nothing on the picture under
			// it may move while it is up.
			continue
		}
		if pe.Kind == pointer.Scroll {
			v.zoomWheel(pe)
			continue
		}
		v.apply(v.gestures.handle(pe, v.gestureEnv(gtx)))
	}
}

// zoomWheel is Ctrl + mouse wheel. Without the modifier the wheel does
// nothing: the viewer has nothing to scroll, and a bare wheel that zoomed
// would fire on every trackpad brush.
func (v *imageViewer) zoomWheel(pe pointer.Event) {
	if !pe.Modifiers.Contain(key.ModCtrl) || pe.Scroll.Y == 0 {
		return
	}
	step := -1
	if pe.Scroll.Y < 0 {
		step = 1
	}
	v.setZoom(viewerZoomStep(v.zoom, step), pe.Position)
}

// readKeys applies the viewer's keyboard contract: step with the arrows and
// PageUp/PageDown, close with Escape.
//
// The filters name no focus target, like the console's Escape does: the keys
// have to work wherever focus happens to sit, and while the viewer is up
// nothing underneath it is focusable at all.
func (v *imageViewer) readKeys(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(
			key.Filter{Name: key.NameEscape},
			key.Filter{Name: key.NameLeftArrow},
			key.Filter{Name: key.NameRightArrow},
			key.Filter{Name: key.NamePageUp},
			key.Filter{Name: key.NamePageDown},
		)
		if !ok {
			return
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		switch ke.Name {
		case key.NameEscape:
			v.parent.escapeImageViewer()
		case key.NameLeftArrow, key.NamePageUp:
			v.step(-1)
		case key.NameRightArrow, key.NamePageDown:
			v.step(1)
		}
	}
}
