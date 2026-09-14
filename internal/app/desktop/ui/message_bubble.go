package ui

import (
	"image"
	"image/color"

	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
)

// message_bubble.go is the frame around one chat message: its border, its
// width, and the ORDER of the five things it can hold. See screen `7g` of
// docs/design/CHANGES-reactions.md.
//
// The order is the reason this is a component. It used to be a sequence of
// appends inside one 180-line function, where a fifth part — the reaction row —
// could be added in four different places and three of them would have looked
// almost right. Here the five slots are fields, the spacing between them is
// fixed, and a caller that has nothing for a slot passes nil.
//
// What goes IN the slots is not this package's business. The quote resolves a
// message by ID, the body may be a file card, the status line reads delivery
// receipts — all of them reach into application state, and a component that
// reached with them would be a screen.

const (
	// MessageBubbleMaxWidthDp caps how wide a bubble grows on a desktop window.
	// A message line longer than this is harder to read, not easier.
	MessageBubbleMaxWidthDp = unit.Dp(520)
	// messageBubbleRadiusDp rounds the border.
	messageBubbleRadiusDp = unit.Dp(8)
	// messageBubbleInsetDp is the air between the border and the content.
	messageBubbleInsetDp = unit.Dp(10)

	// The gaps between the five slots. Each is named for the slot it precedes,
	// so a slot added later cannot borrow a neighbour's spacing by accident.
	messageBubbleHeaderGapDp    = unit.Dp(4)
	messageBubbleBodyGapDp      = unit.Dp(4)
	messageBubbleReactionsGapDp = unit.Dp(8)
	messageBubbleStatusGapDp    = unit.Dp(6)
)

// MessageBubbleBorder is the only colour a bubble carries. The design leaves
// the inside unfilled: mine and theirs differ by this line and by which side of
// the chat they sit on.
//
// A light fill behind each is proposed in docs/design/CHANGES-reactions.md §4
// and deliberately not taken — it is the product owner's call, and taking it
// here would make a layout round decide a visual question.
func MessageBubbleBorder(mine bool) color.NRGBA {
	if mine {
		return color.NRGBA{R: 0x4a, G: 0x6d, B: 0xb0, A: 255}
	}
	return color.NRGBA{R: 0x37, G: 0x44, B: 0x56, A: 255}
}

// MessageBubbleHighlight is the fill behind a bubble the reader was SENT to —
// the message a reply quote points at — and the second return says whether
// there is anything to paint at all.
//
// It is the bubble's own border colour at a fraction of its opacity, so no new
// colour enters the palette: the message reads as itself, lit. This is not the
// general fill proposed in docs/design/CHANGES-reactions.md §4 and does not
// decide it — that one distinguishes mine from theirs on every bubble and is
// still the product owner's call; this one marks exactly one message and fades.
//
// The two peaks differ because the two borders are not equally bright: the blue
// of a sent message carries at a lower alpha than the grey of a received one,
// and matching the numbers rather than the appearance left one side looking
// washed out beside the other.
func MessageBubbleHighlight(mine bool, strength float32) (color.NRGBA, bool) {
	if strength <= 0 {
		return color.NRGBA{}, false
	}
	if strength > 1 {
		strength = 1
	}
	peak := float32(0x62)
	if mine {
		peak = float32(0x4e)
	}
	fill := MessageBubbleBorder(mine)
	fill.A = uint8(peak*strength + 0.5)
	if fill.A == 0 {
		return color.NRGBA{}, false
	}
	return fill, true
}

// MessageAuthorColor is the author name in the bubble's header. The timestamp
// and the "⋯" button beside it are the same on both sides; only the name
// changes.
func MessageAuthorColor(mine bool) color.NRGBA {
	if mine {
		return color.NRGBA{R: 0xad, G: 0xcd, B: 0xff, A: 255}
	}
	return color.NRGBA{R: 0xa2, G: 0xb0, B: 0xc4, A: 255}
}

// MessageStatusColor is the delivery line under the caller's own messages, and
// the "⋯" button in every bubble's header.
func MessageStatusColor() color.NRGBA {
	return color.NRGBA{R: 0x6e, G: 0x82, B: 0xa0, A: 180}
}

// MessageBubble is one message's frame and its five slots, top to bottom.
type MessageBubble struct {
	// Mine picks the border and the author colour. Which SIDE of the chat the
	// bubble sits on is the caller's — it is a property of the list, not of the
	// bubble.
	Mine bool
	// Quote is the reply block, drawn above the header. nil when the message
	// answers nothing.
	Quote layout.Widget
	// Header is the author, the timestamp and the menu button. Always present:
	// every message has all three.
	Header layout.Widget
	// Body is the message text or the file card. Always present.
	Body layout.Widget
	// Reactions is the chip row. nil when the message has none — which is not
	// the same as a widget that draws nothing, because a nil slot also drops
	// the spacer above it.
	Reactions layout.Widget
	// Status is the delivery line. nil on incoming messages and on outgoing
	// ones whose status is not known yet.
	Status layout.Widget
	// Highlight fills the bubble, 0 for none and 1 for the full strength of
	// MessageBubbleHighlight. It is a level rather than a flag because the
	// only thing that sets it is a fade, and a caller holding a flag would
	// have to own the fade's colours to animate it.
	Highlight float32
}

// MessageBubble draws one, no wider than MessageBubbleMaxWidthDp.
func (k Kit) MessageBubble(gtx layout.Context, bubble MessageBubble) layout.Dimensions {
	gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(MessageBubbleMaxWidthDp))

	border := widget.Border{
		Color:        MessageBubbleBorder(bubble.Mine),
		CornerRadius: messageBubbleRadiusDp,
		Width:        unit.Dp(1),
	}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		// The content is recorded before it is added so the fill can go
		// UNDER it: a bubble is sized by what it holds, and the fill needs
		// that size. A Stack.Expanded would take the size from the
		// constraints instead, which is the whole column, not the bubble.
		content := op.Record(gtx.Ops)
		dims := layout.UniformInset(messageBubbleInsetDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, messageBubbleSlots(bubble)...)
		})
		call := content.Stop()

		// Filling the whole rectangle, border included: widget.Border paints
		// its stroke AFTER this closure returns, so the line is drawn over
		// the fill's edge rather than beside it.
		if fill, ok := MessageBubbleHighlight(bubble.Mine, bubble.Highlight); ok {
			area := clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(messageBubbleRadiusDp)).Push(gtx.Ops)
			paint.Fill(gtx.Ops, fill)
			area.Pop()
		}
		call.Add(gtx.Ops)
		return dims
	})
}

// messageBubbleSlots turns the five slots into flex children, dropping the ones
// the caller left nil along with the spacer that would have preceded them.
func messageBubbleSlots(bubble MessageBubble) []layout.FlexChild {
	children := make([]layout.FlexChild, 0, 9)
	if bubble.Quote != nil {
		children = append(children,
			layout.Rigid(bubble.Quote),
			layout.Rigid(layout.Spacer{Height: messageBubbleHeaderGapDp}.Layout),
		)
	}
	children = append(children,
		layout.Rigid(bubble.Header),
		layout.Rigid(layout.Spacer{Height: messageBubbleBodyGapDp}.Layout),
		layout.Rigid(bubble.Body),
	)
	if bubble.Reactions != nil {
		children = append(children,
			layout.Rigid(layout.Spacer{Height: messageBubbleReactionsGapDp}.Layout),
			layout.Rigid(bubble.Reactions),
		)
	}
	if bubble.Status != nil {
		children = append(children,
			layout.Rigid(layout.Spacer{Height: messageBubbleStatusGapDp}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.E.Layout(gtx, bubble.Status)
			}),
		)
	}
	return children
}
