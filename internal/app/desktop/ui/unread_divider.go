package ui

import (
	"image/color"

	"gioui.org/layout"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget/material"
)

const (
	// unreadDividerGapDp is the air above and below the band, so it reads as
	// a break in the conversation rather than as part of either message.
	unreadDividerGapDp = unit.Dp(6)
	// unreadDividerPadDp is the band's own height around its caption.
	unreadDividerPadDp = unit.Dp(4)
)

// UnreadDividerFill is the band behind the caption: the idle chip fill, so the
// divider sits in the palette the rest of the window already uses and stays
// quieter than any message.
func UnreadDividerFill() color.NRGBA {
	return ChipFill(false)
}

// UnreadDividerLabel is the caption colour — the received-author grey, muted
// like everything else in the conversation that is not somebody's words.
func UnreadDividerLabel() color.NRGBA {
	return MessageAuthorColor(false)
}

// UnreadDivider is the "unread messages" band drawn above the first message of
// the newest unread run: across the whole width of the conversation, caption
// centred, one line.
//
// It spans the width because it marks a place in the timeline, not a message —
// the bubbles sit on either side of the chat, and a band as wide as one of them
// would read as belonging to it.
func (k Kit) UnreadDivider(gtx layout.Context, label string) layout.Dimensions {
	return layout.Inset{Top: unreadDividerGapDp, Bottom: unreadDividerGapDp}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.X = gtx.Constraints.Max.X
		return Filled(gtx, UnreadDividerFill(), 0, func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(unreadDividerPadDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				caption := material.Caption(k.Theme, label)
				caption.Color = UnreadDividerLabel()
				caption.Alignment = text.Middle
				caption.MaxLines = 1
				return caption.Layout(gtx)
			})
		})
	})
}
