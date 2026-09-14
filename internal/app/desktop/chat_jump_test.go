package desktop

import (
	"image"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
)

// chat_jump_test.go drives the real layout.List, because the defect this
// replaces was invisible to anything less.
//
// The heights below are deliberately unequal. The version that mixed pixels
// with child counts lands on the right message whenever every message is the
// same height — which is what a hand-written fixture tends to be, and what a
// real conversation never is. Give the list one long message and the arithmetic
// misses by however tall that message was, in whichever direction it sat.
//
// The assertion is a pixel: where the target's top edge ends up relative to the
// viewport. It is derived the way the list itself lays children out — the first
// visible child starts at -Position.Offset, everything after it follows in
// order — so it reads the same geometry the user sees rather than the fields
// the code happened to write.

const jumpViewport = 400

// unevenHeights is a conversation whose messages are as unlike each other as
// real ones: a one-line reply next to a paragraph next to a picture.
var unevenHeights = []int{60, 340, 48, 52, 210, 44, 300, 56, 50, 180, 46, 64}

type jumpHarness struct {
	w      *Window
	router *input.Router
	ops    *op.Ops
	// heights is the conversation, and the tests change it mid-run: that is
	// what a thumbnail landing looks like from the list's side.
	heights []int
	view    int
	now     time.Time
	// arriving is what the bubbles report this frame — "still waiting for a
	// picture", i.e. these heights are not final.
	arriving bool
	// scrollbar is a drag of the scrollbar, in items, applied on the next
	// frame at the point material.List applies one. Consumed when used.
	scrollbar float32
}

func newJumpHarness(t *testing.T, heights []int) *jumpHarness {
	t.Helper()
	h := &jumpHarness{
		router:  new(input.Router),
		ops:     new(op.Ops),
		heights: append([]int(nil), heights...),
		view:    jumpViewport,
		now:     time.Unix(1_800_000_000, 0),
	}
	h.w = &Window{}
	// The same list the window builds. Axis and ScrollToEnd are not
	// decoration here: a list left on its zero Axis is HORIZONTAL, and one
	// without ScrollToEnd never has to be talked out of the bottom of the
	// conversation, which is the state every one of these jumps starts from.
	h.w.chatList.List = layout.List{Axis: layout.Vertical, ScrollToEnd: true}
	h.w.msgCacheByID = make(map[string]cachedMsg, len(heights))
	for i := range heights {
		h.w.msgCacheByID[msgIDAt(i)] = cachedMsg{Index: i}
	}
	// A user who can click a quote has seen the chat, so the list has been
	// laid out at least once and its viewport height is known.
	h.w.chatViewportH = h.view
	return h
}

func msgIDAt(index int) string { return "m" + itoa(index) }

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var digits []byte
	for v > 0 {
		digits = append([]byte{byte('0' + v%10)}, digits...)
		v /= 10
	}
	return string(digits)
}

// frame runs one frame in the order layout() runs it, which is the order that
// matters here: the jump is applied first, the router's pending actions are
// drained after it, and only then does the list lay out. A message arriving
// mid-jump therefore gets its say BETWEEN the jump and the drawing of what the
// jump decided, which is exactly the window the guard exists for.
func (h *jumpHarness) frame(messageArrived ...bool) {
	h.ops.Reset()
	h.now = h.now.Add(16 * time.Millisecond)
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Now:         h.now,
		Constraints: layout.Constraints{Max: image.Pt(300, h.view)},
	}
	h.w.chatImagesWerePending, h.w.chatImagesArriving = h.w.chatImagesArriving, false
	h.w.applyChatJump(gtx)
	if len(messageArrived) > 0 && messageArrived[0] {
		h.w.applyScrollToEnd()
	}
	h.w.chatList.Layout(gtx, len(h.heights), func(gtx layout.Context, index int) layout.Dimensions {
		dims := layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, h.heights[index])}
		h.w.chatJump.measure(index, dims.Size.Y)
		if h.arriving {
			h.w.chatImagesArriving = true
		}
		return dims
	})
	// Where material.List moves the list for the scrollbar: AFTER the messages
	// have been measured, and by a number of ITEMS rather than pixels
	// (widget/material/list.go, ScrollBy). That ordering is the whole point of
	// this case — the frame's measurements describe where the list WAS.
	if h.scrollbar != 0 {
		h.w.chatList.ScrollBy(h.scrollbar)
		h.scrollbar = 0
	}
	h.w.noteChatJumpDrawn(len(h.heights))
	h.router.Frame(h.ops)
}

// wheel queues a real scroll event over the list, the way a mouse wheel
// delivers one. Positive dy scrolls the conversation towards its end.
//
// Through the router rather than by writing Position: the list consumes the
// delta inside its own Layout, AFTER the jump has written where it wants the
// target, and that ordering is the whole question here.
func (h *jumpHarness) wheel(dy float32) {
	h.router.Queue(pointer.Event{
		Kind:     pointer.Scroll,
		Source:   pointer.Mouse,
		Position: f32.Pt(150, float32(h.view)/2),
		Scroll:   f32.Pt(0, dy),
	})
}

// wokeUp reports whether the last frame asked to be drawn again.
func (h *jumpHarness) wokeUp() bool {
	_, ok := h.router.WakeupTime()
	return ok
}

// settle runs frames until the jump reports itself finished, with a cap so a
// jump that never finishes fails as a hang rather than looping forever.
func (h *jumpHarness) settle(t *testing.T) {
	t.Helper()
	for i := 0; i < 8; i++ {
		h.frame()
		if !h.w.chatJump.pending() {
			return
		}
	}
	t.Fatal("the jump never finished: eight frames and still pending")
}

// topOf is where message index's top edge sits relative to the viewport's top,
// as the CURRENT list position draws it. Negative means above the fold.
func (h *jumpHarness) topOf(index int) int {
	pos := h.w.chatList.Position
	top := -pos.Offset
	for i := pos.First; i < index; i++ {
		top += h.heights[i]
	}
	for i := index; i < pos.First; i++ {
		top -= h.heights[i]
	}
	return top
}

func (h *jumpHarness) jumpTo(t *testing.T, index int) {
	t.Helper()
	h.w.chatJump = chatJump{msgID: msgIDAt(index)}
	h.settle(t)
}

func TestAJumpCentresItsMessageWhateverTheOthersMeasure(t *testing.T) {
	// Index 6 is the 300px message, with a 210px one four above it: heights
	// the child-counting version could not have seen.
	const target = 6

	h := newJumpHarness(t, unevenHeights)
	h.frame() // the conversation as the user was reading it, at the end
	h.jumpTo(t, target)

	want := (jumpViewport - unevenHeights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: centring a message needs ITS height and "+
			"the heights above it, and only the list knows either", got, want)
	}
}

func TestEveryMessageLandsCentred(t *testing.T) {
	// One target is a special case; a conversation of them is the contract.
	// The first and last few cannot be centred — there is nothing beyond the
	// ends to show — so they are checked against the list's own clamp instead.
	h := newJumpHarness(t, unevenHeights)
	h.frame()

	total := 0
	for _, v := range unevenHeights {
		total += v
	}

	for target := range unevenHeights {
		h.jumpTo(t, target)

		before := 0
		for i := 0; i < target; i++ {
			before += unevenHeights[i]
		}
		after := total - before - unevenHeights[target]
		want := (jumpViewport - unevenHeights[target]) / 2
		switch {
		case before < want:
			// Not enough conversation above it: the list is at the top.
			want = before
		case after < jumpViewport-unevenHeights[target]-want:
			// Not enough below: the list is at the end.
			want = jumpViewport - unevenHeights[target] - after
		}
		if want < 0 {
			want = 0
		}
		if got := h.topOf(target); got != want {
			t.Fatalf("message %d top at %d, want %d", target, got, want)
		}
	}
}

func TestATallMessageIsShownFromItsBeginning(t *testing.T) {
	// Taller than the viewport: there is no centre to put it at, and a
	// midpoint would push its first line off the top — the half the reader
	// came for.
	heights := []int{80, 90, jumpViewport + 260, 70, 85, 75, 95, 88}
	const target = 2

	h := newJumpHarness(t, heights)
	h.frame()
	h.jumpTo(t, target)

	if got := h.topOf(target); got != 0 {
		t.Fatalf("tall message top at %d, want 0", got)
	}
}

func TestAJumpToAMessageThatIsGoneChangesNothing(t *testing.T) {
	h := newJumpHarness(t, unevenHeights)
	h.frame()
	before := h.w.chatList.Position

	h.w.chatJump = chatJump{msgID: "no-such-message"}
	h.frame()

	if h.w.chatJump.pending() {
		t.Fatal("a jump with no target stayed armed")
	}
	if h.w.chatList.Position.First != before.First || h.w.chatList.Position.Offset != before.Offset {
		t.Fatalf("position moved to %+v from %+v", h.w.chatList.Position, before)
	}
}

func TestAJumpFollowsItsMessageWhenTheConversationShifts(t *testing.T) {
	// A message deleted while the jump is in flight moves every index after
	// it. The settling frame therefore asks the cache where the target is NOW
	// rather than reusing the index the rough frame wrote down — the id is
	// what the user clicked, the index is only where it happened to sit.
	h := newJumpHarness(t, unevenHeights)
	h.frame()

	target := msgIDAt(6)
	h.w.chatJump = chatJump{msgID: target}
	h.frame() // rough placement; the list measures the target

	// Drop the first message: everything shifts up by one.
	h.heights = h.heights[1:]
	shifted := make(map[string]cachedMsg, len(h.heights))
	for i := range h.heights {
		shifted[msgIDAt(i+1)] = cachedMsg{Index: i}
	}
	h.w.msgCacheByID = shifted

	h.settle(t)

	index := h.w.msgCacheByID[target].Index
	want := (jumpViewport - h.heights[index]) / 2
	if got := h.topOf(index); got != want {
		t.Fatalf("after the shift the target sits at %d, want %d", got, want)
	}
}

// A thumbnail decodes on a background goroutine and appears in a later frame,
// and the bubble it lands in grows by up to 200dp when it does. The two tests
// below are the two places that can happen relative to the jump's target.

func TestAPictureLandingInTheTargetLeavesItCentred(t *testing.T) {
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true // the target is still waiting for its picture
	h.heights[target] = 70
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame()
	h.frame()

	// The picture lands: the bubble grows and stops waiting.
	h.heights[target] = 70 + 210
	h.arriving = false
	h.settle(t)

	want := (jumpViewport - h.heights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: the jump centred a height the message "+
			"no longer has", got, want)
	}
}

func TestPicturesLandingAboveTheTargetDoNotPushItOffScreen(t *testing.T) {
	// This is the one that hurts. The list anchors on Position.First, and by
	// the time a jump has been drawn First is a message ABOVE the target — so
	// anything that grows in between moves the target down by its whole
	// height, with nothing to stop it leaving the viewport entirely.
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.heights[target-1] = 50
	h.heights[target-2] = 50
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame()
	h.frame()

	// Two pictures above the target land at once.
	h.heights[target-1] = 50 + 200
	h.heights[target-2] = 50 + 200
	h.arriving = false
	h.settle(t)

	want := (jumpViewport - h.heights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d (viewport %d): the pictures above it "+
			"pushed it out of the screen it had just been put on",
			got, want, jumpViewport)
	}
}

func TestAShiftDuringTheHoldStillMeasuresTheTarget(t *testing.T) {
	// The two halves of this file's bookkeeping meet here: the hold keeps
	// measuring the target while pictures arrive, and the conversation can
	// move under it at the same time — a message deleted, or one arriving
	// above. The height is recorded by INDEX, because that is all the list
	// callback knows, so an index left over from an earlier frame measures
	// whichever message inherited the position and the jump then centres
	// THAT height.
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.frame()

	id := msgIDAt(target)
	h.w.chatJump = chatJump{msgID: id}
	h.frame() // rough placement, still waiting for pictures

	// Drop the first message: the target moves from index 6 to index 5, and
	// index 6 now holds a message of an entirely different height.
	h.heights = h.heights[1:]
	shifted := make(map[string]cachedMsg, len(h.heights))
	for i := range h.heights {
		shifted[msgIDAt(i+1)] = cachedMsg{Index: i}
	}
	h.w.msgCacheByID = shifted

	h.frame() // one more frame of waiting, now with the shifted conversation
	h.arriving = false
	h.settle(t)

	index := h.w.msgCacheByID[id].Index
	want := (jumpViewport - h.heights[index]) / 2
	if got := h.topOf(index); got != want {
		t.Fatalf("target top at %d, want %d: the hold measured the wrong message",
			got, want)
	}
}

func TestScrollingDuringTheHoldTakesItOver(t *testing.T) {
	// The hold writes the position at the top of every frame, so without this
	// the reader's wheel was applied and then undone on the next frame, over
	// and over for as long as a picture kept decoding — the chat shifting and
	// snapping back for up to jumpHoldCap.
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true // pictures still decoding, so the hold stays up
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame() // rough
	h.frame() // holding, target centred

	centred := h.topOf(target)
	if want := (jumpViewport - unevenHeights[target]) / 2; centred != want {
		t.Fatalf("the hold did not centre the target first: %d, want %d", centred, want)
	}

	h.wheel(50)
	h.frame()
	moved := h.topOf(target)
	if moved == centred {
		t.Fatalf("the wheel moved nothing — target still at %d", moved)
	}

	h.frame()
	if got := h.topOf(target); got != moved {
		t.Fatalf("target snapped back from %d to %d: the hold has to let go when "+
			"the reader takes over", moved, got)
	}
	// One more frame releases the guard — the jump holds it for the frame that
	// draws its last decision, and this one drew nothing.
	h.frame()
	if h.w.chatJump.pending() {
		t.Fatal("the jump is still holding the list after the reader scrolled")
	}
	if got := h.topOf(target); got != moved {
		t.Fatalf("target moved again, to %d", got)
	}
	// The mark stays: what the reader lost was the anchor, not the answer to
	// which message they were sent to.
	if h.w.msgHighlight.msgID != "" && h.w.msgHighlight.msgID != msgIDAt(target) {
		t.Fatalf("the highlight moved to %q", h.w.msgHighlight.msgID)
	}
}

func TestAWheelOnTheFirstHeldFrameIsNotUndone(t *testing.T) {
	// The frame-to-frame test cannot speak yet here: the rough frame asked for
	// a different offset than the held one, so there is no earlier answer to
	// the same question to compare against. What settles it instead is that a
	// list anchored away from both ends MUST land exactly where it was put, so
	// any difference at all is the reader's doing.
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame() // rough

	placed := h.topOf(target)
	// Upwards, not down. A queued event is routed against the filters of the
	// frame BEFORE it, and layout.List derives the scroll range it will accept
	// from a Position.Count that is one frame old — right after a jump has
	// moved First, that stale count reads as "already at the end" and a
	// downward wheel is refused before it reaches the gesture. Nothing to do
	// with this file; it just decides which direction a test can scroll in.
	h.wheel(-50)
	h.frame() // the first held frame, and the reader scrolls on it
	moved := h.topOf(target)
	if moved == placed {
		t.Fatalf("the wheel moved nothing — target still at %d", moved)
	}

	h.frame()
	if got := h.topOf(target); got != moved {
		t.Fatalf("target snapped back from %d to %d on the very next frame", moved, got)
	}
}

func TestAClampedTargetIsNotMistakenForTheReader(t *testing.T) {
	// A message near the START of a conversation cannot be centred at first —
	// there is not enough above it to fill half a viewport — so the list
	// clamps and the target does NOT land where it was asked to. Reading that
	// as "the reader scrolled" drops the hold, and then a picture landing
	// above pushes the target down with nothing holding it.
	//
	// The heights are chosen so the clamp STOPS applying: 60px of conversation
	// above the target is less than the 176 it asks for, and the picture that
	// lands takes it to 280 — past it. That is what makes this case tell the
	// two behaviours apart, since while the clamp holds it pins the answer by
	// itself and the hold has nothing to add.
	const target = 2
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.heights[0] = 30
	h.heights[1] = 30
	h.heights[target] = 48
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame()
	h.frame()

	// A picture lands above the target while the hold is up.
	h.heights[1] = 30 + 220
	h.arriving = false
	h.settle(t)

	want := (jumpViewport - h.heights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: the hold let go of a target it had "+
			"merely clamped, and the picture above then pushed it down", got, want)
	}
}

func TestRoomAppearingAboveIsNotMistakenForTheReader(t *testing.T) {
	// The shape that broke the previous version. Near the start of a
	// conversation the target cannot be centred — there is too little above it
	// — so the list clamps. Then a picture lands above and MAKES room, and the
	// same request correctly produces a different answer. Read as a scroll,
	// that ended the hold on the one frame it was needed, and the picture that
	// landed in the target afterwards was never centred: its top stayed where
	// the clamp had left it and its bottom ran off the screen.
	const target = 2
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.heights[0] = 30
	h.heights[1] = 30 // 60px above the target: less than it will ask for
	h.heights[target] = 60
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame() // rough
	h.frame() // held, and clamped at the start: the target sits at 60

	if got := h.topOf(target); got != 60 {
		t.Fatalf("target top at %d, want 60 while the start still clamps it", got)
	}

	// A picture lands ABOVE the target. Room appears, the clamp stops binding,
	// and the target moves — through no doing of the reader's.
	h.heights[1] = 30 + 220
	h.frame()

	// And now one lands in the target itself, which is what has to be centred.
	h.heights[target] = 60 + 206
	h.arriving = false
	h.settle(t)

	want := (jumpViewport - h.heights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: the hold let go when the room above it "+
			"grew, so the picture that landed in it was never centred", got, want)
	}
}

func TestRoomAppearingBelowIsNotMistakenForTheReader(t *testing.T) {
	// The same thing at the other end. A target near the last message cannot
	// be centred while there is too little BELOW it, so the list clamps there
	// instead; a picture landing below makes room and the target moves, again
	// through no doing of the reader's.
	const target = 9
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true
	h.heights[target] = 60
	h.heights[10] = 20
	h.heights[11] = 20 // 100px from the target to the end: too little
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame()
	h.frame()

	if got, want := h.topOf(target), jumpViewport-100; got != want {
		t.Fatalf("target top at %d, want %d while the end still clamps it", got, want)
	}

	h.heights[10] = 20 + 250
	h.arriving = false
	h.settle(t)

	want := (jumpViewport - h.heights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: the hold let go when the room below it "+
			"grew", got, want)
	}
}

func TestDraggingTheScrollbarTakesTheHoldOver(t *testing.T) {
	// The scrollbar moves the list after the messages have been measured and by
	// whole items, so a long drag lands somewhere none of this frame's children
	// were measured and the target's position cannot be worked out at all.
	// Read as "cannot tell", the drag went unnoticed and the next frame pulled
	// the conversation back to the quote.
	const target = 6
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true // pictures still decoding, so the hold stays up
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame() // rough
	h.frame() // holding

	centred := h.topOf(target)
	if want := (jumpViewport - unevenHeights[target]) / 2; centred != want {
		t.Fatalf("the hold did not centre the target first: %d, want %d", centred, want)
	}

	// Three messages back up the conversation, in one movement of the bar —
	// far enough that none of the children between the new position and the
	// target were measured on the frame that moved.
	h.scrollbar = -3
	h.frame()
	moved := h.topOf(target)
	if moved == centred {
		t.Fatalf("the scrollbar moved nothing — target still at %d", centred)
	}

	h.frame()
	if got := h.topOf(target); got == centred {
		t.Fatalf("the list was dragged back to the quote at %d: a scrollbar drag has "+
			"to end the hold like any other scroll", got)
	}
	h.frame()
	if h.w.chatJump.pending() {
		t.Fatal("the jump is still holding the list after the scrollbar was dragged")
	}
}

func TestTheHoldCannotOutlastItsCap(t *testing.T) {
	// A decode that never finishes must not hold the list for ever: while the
	// hold is up the position is written at the top of every frame, so the
	// reader cannot scroll past it.
	h := newJumpHarness(t, unevenHeights)
	h.arriving = true // and stays that way
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(6)}
	deadline := h.now.Add(jumpHoldCap + 2*16*time.Millisecond)
	for i := 0; i < 200 && h.w.chatJump.pending(); i++ {
		if h.now.After(deadline) {
			t.Fatal("the hold is still up well past its cap, and the reader cannot " +
				"scroll while it is")
		}
		h.frame()
	}
	if h.w.chatJump.pending() {
		t.Fatal("the hold never ended")
	}
}

func TestAMessageArrivingMidJumpDoesNotStealTheScroll(t *testing.T) {
	// The pending-actions drain sits BETWEEN the jump and the layout, so the
	// frame that writes the final position is also a frame on which a new
	// message can ask for the end of the conversation. It must not get it:
	// being taken somewhere and then dumped at the bottom is worse than not
	// being taken at all, and the reader has no idea what happened.
	const target = 6

	h := newJumpHarness(t, unevenHeights)
	h.frame()

	h.w.chatJump = chatJump{msgID: msgIDAt(target)}
	h.frame(true) // rough placement, and a message arrives
	h.frame(true) // the settling frame, and another one arrives

	want := (jumpViewport - unevenHeights[target]) / 2
	if got := h.topOf(target); got != want {
		t.Fatalf("target top at %d, want %d: the arrival took the list back to the end "+
			"of the conversation while the jump was still being drawn", got, want)
	}

	// And once the jump has been drawn it lets go: the next arrival scrolls.
	h.frame(true)
	if h.w.chatJump.pending() {
		t.Fatal("the jump still holds the list a frame after it settled")
	}
	if h.w.chatList.Position.BeforeEnd {
		t.Fatal("a message arriving after the jump was drawn did not scroll to the end")
	}
}

func TestArmingAJumpAsksForTheFrameThatRunsIt(t *testing.T) {
	// Everything a jump does happens at the top of a LATER frame, and nothing
	// schedules that frame on its own. A press is normally followed by a
	// release and a release draws a frame, which is why this was invisible —
	// until the two arrive together and the click sits there unanswered.
	h := newJumpHarness(t, unevenHeights)
	h.frame()
	h.wokeUp() // drain whatever the first frame asked for

	h.ops.Reset()
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Now:         time.Now(),
		Constraints: layout.Constraints{Max: image.Pt(300, h.view)},
	}
	h.w.beginChatJump(gtx, msgIDAt(6))
	h.router.Frame(h.ops)

	if !h.wokeUp() {
		t.Fatal("arming a jump asked for no frame, so nothing will run it")
	}
	if h.w.msgHighlight.msgID != msgIDAt(6) {
		t.Fatalf("the highlight was armed for %q", h.w.msgHighlight.msgID)
	}
}

func TestAJumpNothingCanMeasureStopsAskingForFrames(t *testing.T) {
	// The chat is not laid out at all — a modal over it, or a view with no
	// list on it — so no height ever arrives. The jump has to give up anyway:
	// one that waits for a measurement that is not coming asks for a new
	// frame every frame, and the window then redraws at full rate for as long
	// as that lasts.
	h := newJumpHarness(t, unevenHeights)
	h.frame()
	h.wokeUp() // drain

	blind := func() {
		h.ops.Reset()
		gtx := layout.Context{
			Ops:         h.ops,
			Source:      h.router.Source(),
			Now:         time.Now(),
			Constraints: layout.Constraints{Max: image.Pt(300, h.view)},
		}
		h.w.applyChatJump(gtx) // and nothing lays the list out
		h.router.Frame(h.ops)
	}

	h.w.chatJump = chatJump{msgID: msgIDAt(6)}
	blind()
	if !h.wokeUp() {
		t.Fatal("the rough frame did not ask for the one that settles it")
	}
	blind()
	if h.wokeUp() {
		t.Fatal("a jump with no measurement asked for yet another frame, and every " +
			"frame after it would do the same")
	}
}

func TestTheHighlightRisesHoldsAndGoesOut(t *testing.T) {
	var h msgHighlight
	start := time.Unix(0, 0)
	h.start("m1", start)

	at := func(d time.Duration) (float32, bool) {
		running := h.tick(start.Add(d))
		return h.levelFor("m1"), running
	}

	if level, running := at(0); !running || level != 0 {
		t.Fatalf("at the click: level %v running %v, want 0/true", level, running)
	}
	if level, running := at(msgHighlightRise / 2); !running || level <= 0 || level >= 1 {
		t.Fatalf("mid-rise: level %v running %v, want a value between 0 and 1", level, running)
	}
	if level, running := at(msgHighlightRise + msgHighlightHold/2); !running || level != 1 {
		t.Fatalf("mid-hold: level %v running %v, want 1/true", level, running)
	}
	mid := msgHighlightRise + msgHighlightHold + msgHighlightFall/2
	if level, running := at(mid); !running || level <= 0 || level >= 1 {
		t.Fatalf("mid-fall: level %v running %v, want a value between 0 and 1", level, running)
	}
	if _, running := at(msgHighlightRise + msgHighlightHold + msgHighlightFall); running {
		t.Fatal("the highlight is still running after its last step — every frame from " +
			"here on would ask for another one")
	}
	if level := h.levelFor("m1"); level != 0 {
		t.Fatalf("a finished highlight still reports %v", level)
	}
}

func TestOnlyTheJumpedMessageIsLit(t *testing.T) {
	var h msgHighlight
	start := time.Unix(0, 0)
	h.start("m1", start)
	h.tick(start.Add(msgHighlightRise + msgHighlightHold/2))

	if got := h.levelFor("m1"); got != 1 {
		t.Fatalf("the target is lit at %v, want 1", got)
	}
	if got := h.levelFor("m2"); got != 0 {
		t.Fatalf("a neighbour is lit at %v, want 0", got)
	}
	if got := h.levelFor(""); got != 0 {
		t.Fatalf("a message with no id is lit at %v, want 0", got)
	}
}
