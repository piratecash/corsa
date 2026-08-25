package ui

import (
	"image"
	"testing"

	"gioui.org/layout"
	"gioui.org/unit"
)

// slotRecorder reports the order the bubble laid its slots out in, and where
// each one started. The gaps between them are the design's, and a slot inserted
// in the wrong place is exactly the mistake this component exists to prevent.
type slotRecorder struct {
	order []string
	top   map[string]int
}

func newSlotRecorder() *slotRecorder {
	return &slotRecorder{top: make(map[string]int)}
}

func (r *slotRecorder) slot(name string, height int) layout.Widget {
	return func(gtx layout.Context) layout.Dimensions {
		r.order = append(r.order, name)
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, height)}
	}
}

func TestMessageBubbleDrawsItsSlotsInDesignOrder(t *testing.T) {
	recorder := newSlotRecorder()
	gtx := testGtx(600, 400, 1)

	testKit(t).MessageBubble(gtx, MessageBubble{
		Mine:      true,
		Quote:     recorder.slot("quote", 20),
		Header:    recorder.slot("header", 16),
		Body:      recorder.slot("body", 30),
		Reactions: recorder.slot("reactions", 22),
		Status:    recorder.slot("status", 12),
	})

	want := []string{"quote", "header", "body", "reactions", "status"}
	if len(recorder.order) != len(want) {
		t.Fatalf("bubble drew %v, want %v", recorder.order, want)
	}
	for index, name := range want {
		if recorder.order[index] != name {
			t.Fatalf("bubble drew %v, want %v", recorder.order, want)
		}
	}
}

// A nil slot takes its spacer with it. A reaction row that drew nothing but
// still reported a size would leave 8dp of air under every message that has no
// reactions — which today is every message.
func TestMessageBubbleDropsTheSpacerOfAnAbsentSlot(t *testing.T) {
	kit := testKit(t)
	gtx := testGtx(600, 400, 1)

	bare := newSlotRecorder()
	without := kit.MessageBubble(gtx, MessageBubble{
		Header: bare.slot("header", 16),
		Body:   bare.slot("body", 30),
	})

	empty := newSlotRecorder()
	with := kit.MessageBubble(gtx, MessageBubble{
		Header:    empty.slot("header", 16),
		Body:      empty.slot("body", 30),
		Reactions: empty.slot("reactions", 0),
	})

	if with.Size.Y <= without.Size.Y {
		t.Fatalf("a zero-height reactions slot cost %d extra pixels, want the gap to be present when the slot is",
			with.Size.Y-without.Size.Y)
	}
	if got, want := with.Size.Y-without.Size.Y, gtx.Dp(messageBubbleReactionsGapDp); got != want {
		t.Fatalf("a present but empty reactions slot added %dpx, want exactly the %dpx gap", got, want)
	}
}

// The bubble caps its own width. Handed a desktop-wide chat area it must not
// stretch a two-word message across 1200px.
func TestMessageBubbleCapsItsWidth(t *testing.T) {
	gtx := testGtx(1200, 400, 1)
	recorder := newSlotRecorder()

	dims := testKit(t).MessageBubble(gtx, MessageBubble{
		Header: recorder.slot("header", 16),
		Body:   recorder.slot("body", 30),
	})

	if want := gtx.Dp(MessageBubbleMaxWidthDp); dims.Size.X > want {
		t.Fatalf("bubble width = %d, want at most %d", dims.Size.X, want)
	}
}

// Which side of the chat a bubble sits on is the list's business; what the
// bubble itself changes with the sender is the border and the author colour,
// and nothing else.
func TestMessageBubbleColoursDifferBySender(t *testing.T) {
	if MessageBubbleBorder(true) == MessageBubbleBorder(false) {
		t.Fatal("both senders draw the same border, so nothing tells them apart")
	}
	if MessageAuthorColor(true) == MessageAuthorColor(false) {
		t.Fatal("both senders draw the same author colour")
	}
}

func TestMessageBubbleGapsFollowTheDesign(t *testing.T) {
	for _, tt := range []struct {
		name string
		got  unit.Dp
		want unit.Dp
	}{
		{name: "after the quote", got: messageBubbleHeaderGapDp, want: 4},
		{name: "after the header", got: messageBubbleBodyGapDp, want: 4},
		{name: "before the reactions", got: messageBubbleReactionsGapDp, want: 8},
		{name: "before the status", got: messageBubbleStatusGapDp, want: 6},
	} {
		if tt.got != tt.want {
			t.Errorf("gap %s = %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}
