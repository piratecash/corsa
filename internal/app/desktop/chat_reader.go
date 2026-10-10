package desktop

import (
	"gioui.org/layout"
	"gioui.org/unit"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// chat_reader.go is the window's half of "a message is read when the reader
// has seen it": after the conversation is laid out, it works out which message
// is the newest one on screen and whether the end of the conversation is, and
// hands that to the router (service.DMRouter.ReportReaderPosition). The router
// decides what that makes read; this file only says what is on screen.
//
// The answer has to come from the list's own layout pass. A message's height
// is its text, its quote, its picture and its reactions, and none of those are
// known anywhere else — so the heights are taken where the list measured them,
// exactly as the reply jump takes them (see chat_jump.go).

// readerMinVisibleDp is how much of the message at the bottom edge has to be
// on screen before it counts as seen, when half of it would be more. Half a
// one-line message is enough to read it; half a picture taller than the window
// is a screen the reader has not scrolled through yet.
const readerMinVisibleDp = unit.Dp(48)

// chatReader is what the window knows about the reader of the open
// conversation. It belongs to that conversation and is replaced when it
// changes (resetConversationStateOnPeerChange), so a report made for one chat
// is never taken as already made for the next.
type chatReader struct {
	// drawn is what the list measured for every child of the frame being laid
	// out. Truncated, not reallocated, at the start of each frame.
	drawn []drawnChild
	// reported is the last position handed to the router; nil until the
	// first one. Reporting takes the router's lock, and the window draws at
	// frame rate — only a change is worth that.
	reported *readerReport
}

// readerReport is one position as it was reported, and for which
// conversation.
type readerReport struct {
	peer     domain.PeerIdentity
	position service.ReaderPosition
}

// beginFrame forgets the previous frame's measurements: they describe where
// the list was, and the frame about to be laid out may be somewhere else.
func (c *chatReader) beginFrame() {
	c.drawn = c.drawn[:0]
}

// measure records what the list gave one child of this frame.
func (c *chatReader) measure(index, height int) {
	c.drawn = append(c.drawn, drawnChild{index: index, height: height})
}

func (c *chatReader) heightOf(index int) (int, bool) {
	for _, child := range c.drawn {
		if child.index == index {
			return child.height, true
		}
	}
	return 0, false
}

// newestReadIndex is the newest message the reader has on screen, read off the
// position the list has just laid out, and false when the frame cannot say.
//
// The last visible child is counted only if enough of it shows: more than half
// of it, or minVisible pixels of a message so tall that half of it is more than
// a screen. One that only peeks over the bottom edge is not read yet, and the
// one before it is — the reader can see where it ends.
//
// "Cannot say" is a last child nobody measured this frame. The frame the
// scrollbar moved is turned away before this is asked (readerScreen); the
// check here is what keeps any other mismatch from being answered with a
// guess.
func newestReadIndex(position layout.Position, heightOf func(int) (int, bool), minVisible int) (int, bool) {
	if position.Count <= 0 {
		return 0, false
	}
	last := position.First + position.Count - 1
	// OffsetLast is the room left under the last child: zero or more means
	// it ends on screen.
	if position.OffsetLast >= 0 {
		return last, true
	}
	height, measured := heightOf(last)
	if !measured {
		return 0, false
	}
	visible := height + position.OffsetLast
	if visible >= min(height/2, minVisible) {
		return last, true
	}
	if position.Count > 1 {
		return last - 1, true
	}
	return 0, false
}

// readerFrame is what one laid-out frame says about the reader's screen.
type readerFrame struct {
	// position is the list position after the frame.
	position layout.Position
	// scrollbarMoved says material.List moved the list for its scrollbar
	// after the children were measured (widget/material/list.go, ScrollBy):
	// position then names children laid out somewhere else, or not at all,
	// and Count and OffsetLast still describe where the list was.
	scrollbarMoved bool
	// minVisible is readerMinVisibleDp in pixels.
	minVisible int
}

// positionToReport is the position to hand the router for peer's
// conversation after this frame, and false when there is nothing new to say —
// the frame cannot tell, or it tells what was already reported.
//
// A frame the scrollbar moved cannot tell: what it would report is a guess
// about the next frame, which lays those children out and reports for itself.
//
// The position is recorded as reported here, before the router has it: the
// router drops a report for a conversation that is no longer open, and the
// window's next snapshot then shows the other conversation, which resets this
// state anyway.
func (c *chatReader) positionToReport(peer domain.PeerIdentity, conversation []service.DirectMessage, frame readerFrame) (service.ReaderPosition, bool) {
	index, atEnd, ok := readerScreen(len(conversation), frame, c.heightOf)
	if !ok || index < 0 || index >= len(conversation) {
		return service.ReaderPosition{}, false
	}
	current := readerReport{
		peer: peer,
		position: service.ReaderPosition{
			NewestSeen: domain.MessageID(conversation[index].ID),
			AtEnd:      atEnd,
		},
	}
	if c.reported != nil && *c.reported == current {
		return service.ReaderPosition{}, false
	}
	c.reported = &current
	return current.position, true
}

// readerScreen is the newest message on screen and whether the end is, read
// off one frame, and false when the frame cannot say.
//
// A conversation that fits on screen whole is answered first, from Count and
// OffsetLast alone: layout.List writes both and the scrollbar moves neither,
// and a scrollbar that cannot scroll is never updated (ScrollbarStyle.Layout
// returns before Scrollbar.Update), so the drag delta it last held stays
// readable for as long as the conversation fits. Asked the other way round, a
// short conversation would report nothing, frame after frame.
//
// That is the only frame a stale delta can reach: a list that does not fit
// whole has laid out more than a screen of children, so its estimated Length
// exceeds the viewport, the scrollbar counts as scrollable, and Update — which
// zeroes the delta first — runs on every such frame.
func readerScreen(length int, frame readerFrame, heightOf func(int) (int, bool)) (int, bool, bool) {
	position := frame.position
	if length > 0 && position.Count == length && position.OffsetLast >= 0 {
		return length - 1, true, true
	}
	if frame.scrollbarMoved {
		return 0, false, false
	}
	index, ok := newestReadIndex(position, heightOf, frame.minVisible)
	return index, !position.BeforeEnd, ok
}

// reportReaderPosition tells the router what the reader of the open
// conversation has on screen, when that changed. Called right after the chat
// list has laid out — Position only describes the screen once that pass is
// over.
func (w *Window) reportReaderPosition(gtx layout.Context, peer domain.PeerIdentity, conversation []service.DirectMessage) {
	position, changed := w.chatReader.positionToReport(peer, conversation, readerFrame{
		position:       w.chatList.Position,
		scrollbarMoved: w.chatList.ScrollDistance() != 0,
		minVisible:     gtx.Dp(readerMinVisibleDp),
	})
	if !changed {
		return
	}
	w.router.ReportReaderPosition(peer, position)
}

// unreadDividerAbove says whether the "unread messages" divider is drawn above
// message: the router names the message (RouterSnapshot.UnreadMarker), and a
// message that is no longer in the conversation gets no divider anywhere.
func unreadDividerAbove(firstUnread domain.MessageID, placed bool, message service.DirectMessage) bool {
	return placed && domain.MessageID(message.ID) == firstUnread
}
