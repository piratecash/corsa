package ui

import (
	"image"
	"image/color"

	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// checkbox.go is the application's on/off control.
//
// It is not material.CheckBox for the reason Chip is not material.Clickable:
// the Material ink is drawn against a square clip and shows as pale corner
// dots and a flashing ring that appear nowhere in this design. It shares the
// chip palette instead, so a ticked box reads as the same "chosen" state as a
// selected tab or a selected menu row.
//
// The whole row is the target, label included. A 18dp box is a hard thing to
// hit with a finger, and the label beside it is the only part of the control
// the user is actually looking at.

const (
	checkboxBoxDp    = 18
	checkboxRadiusDp = 4
	checkboxGlyphDp  = 14
	checkboxGapDp    = 10
	checkboxTextSp   = 13
	checkboxNoteSp   = 12
	// checkboxNoteGapDp separates the label from the note under it.
	checkboxNoteGapDp = 3
)

func checkboxBorderColor() color.NRGBA {
	return color.NRGBA{R: 0x3c, G: 0x4c, B: 0x62, A: 255}
}

func checkboxLabelColor() color.NRGBA {
	return color.NRGBA{R: 0xf5, G: 0xf7, B: 0xfa, A: 255}
}

func checkboxNoteColor() color.NRGBA {
	return color.NRGBA{R: 0x9a, G: 0xa8, B: 0xbe, A: 255}
}

// Checkbox describes one on/off control.
type Checkbox struct {
	// Label is the line beside the box.
	Label string
	// Note is a dimmer second line under the label, for the consequence of
	// ticking the box. Empty draws nothing — the row keeps the label's height.
	Note string
	// Checked is the current state. The caller owns it: this draws the state
	// and Button reports the press, so what a tick MEANS stays next to the
	// state it changes.
	Checked bool
	// Check is the glyph drawn inside a ticked box. Nil draws a filled box
	// with no mark, which still reads as on against the empty one.
	Check *widget.Icon
}

// Checkbox draws the control. button carries the press and is drained by the
// caller.
func (k Kit) Checkbox(gtx layout.Context, button *widget.Clickable, box Checkbox) layout.Dimensions {
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.CheckBox.Add(gtx.Ops)
		semantic.SelectedOp(box.Checked).Add(gtx.Ops)
		semantic.LabelOp(box.Label).Add(gtx.Ops)

		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Start}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return k.checkboxMark(gtx, box)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(checkboxGapDp)}.Layout),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return k.checkboxText(gtx, box)
			}),
		)
	})
}

// checkboxMark is the box itself, at exactly its own size: a Rigid child of a
// Flex is handed the row's cross-axis minimum, and a box that took it would be
// as tall as the note beside it.
func (k Kit) checkboxMark(gtx layout.Context, box Checkbox) layout.Dimensions {
	side := gtx.Dp(unit.Dp(checkboxBoxDp))
	gtx.Constraints = layout.Exact(image.Pt(side, side))

	if !box.Checked {
		border := widget.Border{
			Color:        checkboxBorderColor(),
			CornerRadius: unit.Dp(checkboxRadiusDp),
			Width:        unit.Dp(1),
		}
		return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: image.Pt(side, side)}
		})
	}

	FillRounded(gtx, ChipFill(true), unit.Dp(checkboxRadiusDp))
	layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return Icon(gtx, box.Check, unit.Dp(checkboxGlyphDp), ChipActiveLabel())
	})
	return layout.Dimensions{Size: image.Pt(side, side)}
}

func (k Kit) checkboxText(gtx layout.Context, box Checkbox) layout.Dimensions {
	label := material.Label(k.Theme, unit.Sp(checkboxTextSp), box.Label)
	label.Color = checkboxLabelColor()

	if box.Note == "" {
		return label.Layout(gtx)
	}

	note := material.Label(k.Theme, unit.Sp(checkboxNoteSp), box.Note)
	note.Color = checkboxNoteColor()

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(label.Layout),
		layout.Rigid(layout.Spacer{Height: unit.Dp(checkboxNoteGapDp)}.Layout),
		layout.Rigid(note.Layout),
	)
}
