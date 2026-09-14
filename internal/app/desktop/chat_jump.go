package desktop

import (
	"time"

	"gioui.org/layout"
	"gioui.org/op"
)

// chat_jump.go is what a click on a reply quote does: the conversation moves so
// the quoted message is centred, and that message lights up for long enough to
// be found by eye.
//
// WHY THE POSITION IS COMPUTED IN PIXELS. layout.List offers two ways to say
// where a list sits, and they are not interchangeable. Position.First is an
// INDEX; Position.Offset is PIXELS, signed, measured from the viewport's
// leading edge to the leading edge of the child at First. The version this
// replaces mixed them: it took the click's height within the viewport as a
// fraction of PIXELS, multiplied that fraction by Position.Count — a count of
// CHILDREN — and used the product as "how many messages to show above the
// target". That product is only a position if every message is the same height.
// One long message or one image in the way, and the jump missed by as much as
// that message was tall, over or under depending on which side of the target it
// sat. Position.Count made it worse by coming from the PREVIOUS frame: it
// described the neighbourhood being left, not the one being entered.
//
// Offset needs none of that. A negative Offset says "leave this many pixels
// above the target", and layout.List fills them by walking backwards through
// the preceding messages itself (layout/list.go, nextDir → iterateBackward),
// measuring each as it goes. Whether that takes one message or nine is the
// list's business, and it is the only party that can answer it: the heights of
// children above First are not known anywhere else, not even approximately.
//
// WHY IT TAKES MORE THAN ONE FRAME. Centring needs the target's own height,
// and nothing knows a message's height until it has been laid out. So the
// first frame puts the target's top edge at the middle of the viewport —
// already close, and correct for the common case of a short message — and
// records what the list measured for it. The next frame moves it up by half
// that height, which is the exact centre. At sixty frames a second the
// correction lands 16ms after the jump; what would be visible is the wrong
// position it replaces, and that one is off by at most half a bubble.
//
// AND WHY IT SOMETIMES TAKES SEVERAL MORE. The heights are not final while a
// thumbnail is still decoding, so the jump keeps re-anchoring on the target
// until a whole frame goes by with nothing waiting for a picture. See the hold
// in applyChatJump and jumpHoldCap.
//
// The list's own clamps finish the job at the two ends: a message among the
// first cannot be centred because there is nothing above it to show, and one
// among the last cannot because there is nothing below. layout.List pins the
// offset in both cases (list.go, init and nextDir), so "centre it" degrades to
// "as close as the conversation allows" without this file testing for it.
type chatJump struct {
	// msgID is the message the user asked for; "" when nothing is pending.
	msgID string
	// index is where the target sits, refreshed every frame, and it exists for
	// one reason: measure is called for every child and has to recognise this
	// one. It is NOT how a later frame FINDS the target — that re-reads the
	// id, because a message added or deleted in between moves every index
	// after it while leaving the message itself alone.
	index int
	// placed says the rough frame has been drawn, so later ones settle.
	placed bool
	// settled says the final position has been written. The jump is over as
	// far as the list is concerned, and the ONLY thing this state still does
	// is hold the guard below up for the rest of that frame — see pending.
	settled bool
	// height is the main-axis size the list measured for the target's child,
	// insets included. Zero means "not measured", never "empty".
	height int
	// deadline ends the hold whatever the pictures are doing. See jumpHoldCap.
	deadline time.Time
	// reader says the person scrolled while the hold was up, so the hold is
	// over: what they asked for outranks what the jump was still tidying.
	reader bool
	// askedNow is the offset this frame's placement asked for, after the
	// clamping placeChatList does, so the frame can be closed against the
	// number that was actually written.
	askedNow int
	// drawn is what the list measured for every child of the frame being laid
	// out. It is the only source for two things neither the jump nor the list
	// can otherwise answer: where the target ACTUALLY ended up — Position
	// reports a distance to whichever child the list normalised onto, not to
	// the one asked for — and how much conversation lies beyond the target,
	// which is what decides where a clamp puts it. Truncated rather than
	// reallocated: it lives for the handful of frames a hold lasts.
	drawn []drawnChild
}

// drawnChild is one child of the frame being laid out, as the list measured it.
type drawnChild struct {
	index  int
	height int
}

// pending reports whether a jump still owns the list position, and it is what
// keeps a message arriving mid-jump from pulling the conversation back to its
// end underneath the reader.
//
// It stays true for the whole of the settling frame, which is the part that
// took a second attempt to get right. The guard is read AFTER applyChatJump
// within one frame — the pending-actions drain runs between the jump and the
// layout — so a jump that cleared itself the moment it wrote its final
// position left the guard down while that position had not been drawn yet.
// One message arriving in that window scrolled the conversation to the end and
// the jump was simply lost: measured at 602px past the target, which is to say
// off the screen it had just been put on.
func (j *chatJump) pending() bool { return j.msgID != "" }

// measure records what the list gave one child, and is called for every child
// of the frame. Two different things are kept: the target's own height, which
// the next placement centres on, and every height, which is what says where
// the target ended up and where a clamp would have put it.
//
// The guards are the point for the first of those: a jump that is not placed
// yet has no index to compare against, one already settled has nothing left to
// measure for, and an index that no longer belongs to the target would hand
// the next placement a stranger's height.
func (j *chatJump) measure(index, height int) {
	if !j.pending() || !j.placed || j.settled {
		return
	}
	j.drawn = append(j.drawn, drawnChild{index: index, height: height})
	if index == j.index {
		j.height = height
	}
}

// topOfTarget is where the target's leading edge sits relative to the top of
// the viewport, read off the frame the list has just laid out, and false when
// the children between the list's own anchor and the target were not all
// measured.
//
// It has to be computed rather than read because Position says where the child
// at Position.First is, and by the end of a frame First is whichever child the
// list normalised onto — for a centred target, one above it.
func (j *chatJump) topOfTarget(first, offset int) (int, bool) {
	top := -offset
	if j.index >= first {
		span, ok := j.spanOf(first, j.index)
		if !ok {
			return 0, false
		}
		return top + span, true
	}
	span, ok := j.spanOf(j.index, first)
	if !ok {
		return 0, false
	}
	return top - span, true
}

func (j *chatJump) heightOf(index int) (int, bool) {
	for _, child := range j.drawn {
		if child.index == index {
			return child.height, true
		}
	}
	return 0, false
}

// noteDrawn closes a frame: it works out where the target HAD to land if
// nobody but the jump moved the list, and a difference from that is the reader
// scrolling.
//
// Predicting rather than remembering is the part that took three rounds. The
// first version compared the position with the offset asked for, which is
// wrong at either end of a conversation, where the list clamps and the target
// legitimately lands somewhere else. The second compared this frame's answer
// with the previous frame's answer to the same request — which is wrong for
// exactly the case the hold exists for: at the start of a conversation the
// target cannot be centred while there is too little above it, and a picture
// landing above MAKES room, so the same request correctly produces a different
// answer. The hold ended on the one frame it was needed.
//
// So the clamps are predicted instead of guessed around. Which of them binds is
// not inferred from the numbers but read off the list: First back at zero means
// the start held it back, not being before the end means the end did. And the
// heights each case needs are exactly the ones the list measured — when the
// start binds, every child from the first to the target was laid out; when the
// end binds, every child from the target to the last was.
//
// Neither span can actually be missing by the time this is called, and the
// checks are here because saying so is cheaper than proving it at every call.
// The caller has already worked out where the target IS, which needs the
// children between the list's anchor and the target; the start case adds only
// the ones between the first child and the anchor, and it applies exactly when
// the anchor IS the first child, so it adds nothing. The end case adds the
// children from the anchor to the last, and it applies exactly when the list
// has reached the last — which means they were all laid out. Whether a
// genuinely missing height should read as the reader is therefore not a
// question this answers, and it declines to invent one.
func (j *chatJump) noteDrawn(asked, got, viewport, length int, atStart, atEnd bool) {
	want := asked
	if atStart {
		above, ok := j.spanOf(0, j.index)
		if !ok {
			return
		}
		if want > above {
			want = above
		}
	}
	if atEnd {
		below, ok := j.spanOf(j.index, length)
		if !ok {
			return
		}
		if bottom := viewport - below; want < bottom {
			want = bottom
		}
	}
	if want < 0 {
		want = 0
	}
	if got != want {
		j.reader = true
	}
}

// spanOf adds up the heights of the children in [from, to), and says so when
// one of them was not measured this frame rather than returning a total
// assembled from gaps. What the callers make of that is their business.
func (j *chatJump) spanOf(from, to int) (int, bool) {
	span := 0
	for i := from; i < to; i++ {
		height, ok := j.heightOf(i)
		if !ok {
			return 0, false
		}
		span += height
	}
	return span, true
}

// beginChatJump arms a jump to msgID, lights that message, and asks for the
// frame that carries both out.
//
// The invalidation is not optional. Everything here is state read at the TOP
// of a later frame, and nothing schedules that frame by itself. It appeared to
// work without asking because a press is normally followed by a release and a
// release draws a frame — but when the two are delivered together, which a
// synthetic tap, a stylus or a dropped frame all do, the click was recorded
// and then sat there until the user happened to move the mouse.
func (w *Window) beginChatJump(gtx layout.Context, msgID string) {
	w.chatJump = chatJump{msgID: msgID}
	w.msgHighlight.start(msgID, gtx.Now)
	gtx.Execute(op.InvalidateCmd{})
}

// applyScrollToEnd obeys the router's request to show the newest message,
// unless a jump owns the list position right now. See chatJump.pending.
func (w *Window) applyScrollToEnd() {
	if w.chatJump.pending() {
		return
	}
	w.chatList.Position.BeforeEnd = false
}

// applyChatJump moves the chat list towards a pending jump, one frame's worth.
//
// It runs at the top of layout(), before the list is laid out, and never from
// inside it: layout.List recomputes Position while it lays out, so a write
// made during that pass is overwritten by the end of it.
func (w *Window) applyChatJump(gtx layout.Context) {
	if !w.chatJump.pending() {
		return
	}
	// Released at the START of the first frame after the one that drew the
	// final position, which is the earliest moment the guard is no longer
	// protecting anything. Nothing is placed on this frame: the list is
	// already where the jump left it, and whatever wants it elsewhere — a new
	// message pulling to the end — is now free to say so.
	if w.chatJump.settled {
		w.chatJump = chatJump{}
		return
	}
	// The cache is rebuilt earlier in this same frame, so an id it does not
	// hold is a message that is gone — nothing to jump to, and nothing to
	// retry.
	target, ok := w.findCachedMsg(w.chatJump.msgID)
	if !ok {
		w.chatJump = chatJump{}
		return
	}

	// Refreshed every frame, not written once: measure has to recognise the
	// target's child among all of them, and a message added or deleted since
	// the last frame moved it. A stale index measures whichever message
	// inherited the position, and the next placement centres THAT height.
	w.chatJump.index = target.Index
	// This frame's measurements start empty; the previous frame's were read by
	// noteChatJumpDrawn before it ended.
	w.chatJump.drawn = w.chatJump.drawn[:0]

	// Every frame after the rough one re-anchors on the target, using the
	// height the list last measured for it. Re-anchoring rather than
	// correcting once is what survives the pictures: a thumbnail decodes on a
	// background goroutine and appears in a LATER frame, and the bubble it
	// lands in grows by up to 200dp the moment it does. One that grows above
	// the target pushes the target down by its whole height — measured at
	// 582px in a 400px viewport, which is to say off the screen it had just
	// been put on — because by then the list is anchored on a message above
	// the target, not on the target. Writing First back to the target every
	// frame makes everything above it irrelevant by construction.
	//
	// Reserving the space instead would be better, and cannot be done today:
	// what a picture's proportions are is known only once it is decoded, and
	// file_announce does not carry them (domain.FileAnnouncePayload). A
	// placeholder of a guessed size would trade a large displacement for a
	// smaller one under every undecoded picture in the app, for ever.
	if w.chatJump.placed {
		// The reader moved the list themselves. Their scroll outranks the
		// tidying the hold was still doing: re-anchoring now would drag them
		// back to the target, and the next frame would do it again, for as
		// long as a picture kept decoding. The highlight is left alone — the
		// message stays marked, it just stops being chased.
		//
		if w.chatJump.reader {
			w.chatJump.settled = true
			return
		}
		height := w.chatJump.height
		if height <= 0 || w.chatViewportH <= 0 {
			// Never measured — a modal over the chat, or a conversation
			// closed by the same click. The rough placement already has the
			// target on screen and there is nothing better to be had, so
			// stop rather than wait for a measurement that is not coming:
			// an earlier version asked for another frame instead, and a
			// window that never lays the list out redrew at full rate for as
			// long as it stayed that way.
			w.chatJump.settled = true
			return
		}
		w.placeChatList(target.Index, (w.chatViewportH-height)/2)

		// Done as soon as a frame goes by with nothing waiting for a picture.
		// That frame's measurements are final by definition, and this
		// placement is the one computed from them — including the frame a
		// picture landed in, which is the first frame that is not waiting and
		// the one whose new height this placement has just used.
		if !w.chatImagesWerePending || !gtx.Now.Before(w.chatJump.deadline) {
			w.chatJump.settled = true
			return
		}
		gtx.Execute(op.InvalidateCmd{})
		return
	}

	// Rough frame: the target's TOP edge at the middle of the viewport. For a
	// short message that is already within half a bubble of the answer, so
	// what the next frame replaces is never far from what it draws.
	w.placeChatList(target.Index, w.chatViewportH/2)
	w.chatJump.placed = true
	w.chatJump.deadline = gtx.Now.Add(jumpHoldCap)
	// The settling frame has to be asked for. A click is followed by a
	// release, which draws one more frame, so this appeared to work without
	// asking — but a jump that depends on the user's finger coming back up is
	// a jump a stylus, a synthetic tap or a dropped frame leaves half-applied.
	gtx.Execute(op.InvalidateCmd{})
}

// noteChatJumpDrawn closes a jump's frame. It must be called right after the
// chat list has laid out and not before: Position is rewritten during that
// pass, and what it says halfway through is not where anything ended up.
//
// "After" includes the scrollbar, which is why a position that cannot be worked
// out at all counts as the reader rather than as no answer. material.List moves
// the list for the scrollbar AFTER measuring the messages, by a number of ITEMS
// (widget/material/list.go, ScrollBy), so a click far down the bar lands the
// list somewhere no child of this frame was measured — the report that found
// this had First go 47 → 83 and then be dragged back to 47. Treating that as
// "cannot tell" left the drag unnoticed; it is the one thing the jump provably
// did not do.
func (w *Window) noteChatJumpDrawn(length int) {
	if !w.chatJump.pending() || !w.chatJump.placed || w.chatJump.settled {
		return
	}
	position := w.chatList.Position
	got, ok := w.chatJump.topOfTarget(position.First, position.Offset)
	if !ok {
		w.chatJump.reader = true
		return
	}
	w.chatJump.noteDrawn(w.chatJump.askedNow, got, w.chatViewportH, length,
		position.First == 0, !position.BeforeEnd)
}

// jumpHoldCap is how long the target may keep the list anchored while the
// pictures around it are still decoding.
//
// It is a cap and not the mechanism: the hold normally ends on the frame after
// the last picture lands, which for a conversation whose thumbnails are
// already decoded is the frame right after the jump. The cap is what a decode
// that never finishes runs into — the cache serialises large images behind a
// byte budget, and one of them must not be able to hold the list for ever.
//
// What it costs is stated rather than hidden: while the hold is up, the list
// is written at the top of every frame, so a wheel scroll during it is undone
// on the next one. That is the reason this is 600ms and not the length of the
// highlight — the reader who has just been taken somewhere is looking, not
// scrolling, and six hundred milliseconds is about as long as that stays true.
const jumpHoldCap = 600 * time.Millisecond

// placeChatList puts the child at index that many pixels below the top of the
// viewport. A negative Offset is not a special case in layout.List: it is how
// the list is told to fill the space above First from the children that come
// before it, measuring each one as it goes.
//
// above is clamped here and only here, which is what makes a message TALLER
// than the viewport come out right: half of a negative remainder would push
// its first line off the top, and the first line is the half the reader came
// for. Zero puts the message's beginning at the top and lets the rest run on.
func (w *Window) placeChatList(index, above int) {
	if above < 0 {
		above = 0
	}
	// Recorded here rather than by the callers so the number the frame is
	// judged against cannot drift from the number that was written.
	w.chatJump.askedNow = above
	w.chatList.Position.First = index
	w.chatList.Position.Offset = -above
	// Without this the list stays pinned to the end and ignores both fields
	// above — ScrollToEnd outranks them (see layout.Position.BeforeEnd).
	w.chatList.Position.BeforeEnd = true
}

// The shape of the highlight, in the order it happens. Held long enough for the
// eye to arrive after the scroll, then given a fall slow enough to read as
// fading rather than as a second jump.
const (
	msgHighlightRise = 140 * time.Millisecond
	msgHighlightHold = 900 * time.Millisecond
	msgHighlightFall = 520 * time.Millisecond
)

// msgHighlight is the fading fill behind the message a jump landed on.
//
// It is separate from chatJump on purpose: the jump is over in two frames and
// the highlight outlives it by a second and a half, and the one thing the two
// must NOT share is the moment they finish. A single flag that meant both
// would have taken the light away the instant the scroll settled, which is
// before the reader has looked.
type msgHighlight struct {
	// msgID is the lit message; "" when nothing is lit.
	msgID string
	// since is when it was lit, on the frame clock (gtx.Now) rather than
	// time.Now: the frames are what draws the fade, and Gio hands each one
	// the time it was scheduled for.
	since time.Time
	// level is this frame's strength, 0…1.
	level float32
}

func (h *msgHighlight) start(msgID string, now time.Time) {
	*h = msgHighlight{msgID: msgID, since: now}
}

// level tells a bubble how lit it is. Every bubble asks; only one is ever
// answered with more than zero.
func (h *msgHighlight) levelFor(msgID string) float32 {
	if msgID == "" || msgID != h.msgID {
		return 0
	}
	return h.level
}

// tick recomputes this frame's level and reports whether the highlight is
// still running — that is, whether another frame is owed.
func (h *msgHighlight) tick(now time.Time) bool {
	if h.msgID == "" {
		return false
	}
	elapsed := now.Sub(h.since)
	switch {
	case elapsed <= 0:
		// The frame clock can hand out a time at or before the one the click
		// carried. Not started yet, rather than finished.
		h.level = 0
	case elapsed < msgHighlightRise:
		h.level = float32(elapsed) / float32(msgHighlightRise)
	case elapsed < msgHighlightRise+msgHighlightHold:
		h.level = 1
	case elapsed < msgHighlightRise+msgHighlightHold+msgHighlightFall:
		h.level = 1 - float32(elapsed-msgHighlightRise-msgHighlightHold)/float32(msgHighlightFall)
	default:
		*h = msgHighlight{}
		return false
	}
	return true
}

// tickMessageHighlight advances the fade and asks for the frame that draws its
// next step. Unconditional invalidation, not a deadline: a fade is every frame
// until it is over, and it is over within two seconds.
func (w *Window) tickMessageHighlight(gtx layout.Context) {
	if w.msgHighlight.tick(gtx.Now) {
		gtx.Execute(op.InvalidateCmd{})
	}
}
