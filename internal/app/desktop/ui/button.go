package ui

import (
	"image"
	"image/color"

	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// SquareIconButtonSideDp is the standard touch target for square icon actions.
const SquareIconButtonSideDp = 44

// SquareIconButton draws a compact action identified by its icon.
func (k Kit) SquareIconButton(gtx layout.Context, button *widget.Clickable, icon *widget.Icon, hint string) layout.Dimensions {
	side := min(gtx.Dp(unit.Dp(SquareIconButtonSideDp)), gtx.Constraints.Max.X, gtx.Constraints.Max.Y)
	gtx.Constraints = layout.Exact(image.Pt(side, side))
	return k.ButtonFrame(gtx, button, ButtonSecondary, unit.Dp(ModalButtonRadiusDp), func(gtx layout.Context) layout.Dimensions {
		semantic.DescriptionOp(hint).Add(gtx.Ops)
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Icon(gtx, icon, unit.Dp(20), ModalCloseButtonColors(ModalCloseButtonIdle).Icon)
		})
	})
}

type ButtonVariant uint8

const (
	ButtonSecondary ButtonVariant = iota
	ButtonPrimary
	ButtonSelected
	ButtonDanger
	ButtonNotice
)

type buttonPalette struct {
	fill, border, text color.NRGBA
}

func actionButtonPalette(variant ButtonVariant) buttonPalette {
	text := color.NRGBA{R: 240, G: 244, B: 249, A: 255}
	switch variant {
	case ButtonPrimary:
		return buttonPalette{color.NRGBA{R: 34, G: 59, B: 85, A: 255}, color.NRGBA{R: 69, G: 100, B: 132, A: 255}, text}
	case ButtonSelected:
		return buttonPalette{color.NRGBA{R: 44, G: 72, B: 100, A: 255}, color.NRGBA{R: 107, G: 149, B: 190, A: 255}, text}
	case ButtonDanger:
		return buttonPalette{color.NRGBA{R: 77, G: 40, B: 49, A: 255}, color.NRGBA{R: 142, G: 77, B: 91, A: 255}, text}
	case ButtonNotice:
		return buttonPalette{color.NRGBA{R: 230, G: 126, B: 34, A: 255}, color.NRGBA{R: 244, G: 163, B: 81, A: 255}, modalCardFillColor()}
	default:
		return buttonPalette{color.NRGBA{R: 27, G: 42, B: 58, A: 255}, color.NRGBA{R: 52, G: 70, B: 90, A: 255}, text}
	}
}

// ButtonFrame applies the action palette while retaining the control's geometry.
func (k Kit) ButtonFrame(gtx layout.Context, button *widget.Clickable, variant ButtonVariant, radius unit.Dp, body layout.Widget) layout.Dimensions {
	palette := actionButtonPalette(variant)
	border := widget.Border{Color: palette.border, CornerRadius: radius, Width: unit.Dp(1)}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		style := material.ButtonLayout(k.Theme, button)
		style.Background = palette.fill
		style.CornerRadius = radius
		return style.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			pointer.CursorPointer.Add(gtx.Ops)
			return body(gtx)
		})
	})
}

type TextButtonStyle struct {
	kit          Kit
	button       *widget.Clickable
	label        string
	variant      ButtonVariant
	Inset        layout.Inset
	TextSize     unit.Sp
	CornerRadius unit.Dp
}

func (k Kit) TextButton(button *widget.Clickable, label string, variant ButtonVariant) TextButtonStyle {
	return TextButtonStyle{
		kit: k, button: button, label: label, variant: variant,
		Inset:        layout.Inset{Top: 10, Bottom: 10, Left: 12, Right: 12},
		TextSize:     k.Theme.TextSize * 14 / 16,
		CornerRadius: 4,
	}
}

func (b TextButtonStyle) Layout(gtx layout.Context) layout.Dimensions {
	return b.kit.ButtonFrame(gtx, b.button, b.variant, b.CornerRadius, func(gtx layout.Context) layout.Dimensions {
		semantic.LabelOp(b.label).Add(gtx.Ops)
		return b.Inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Label(b.kit.Theme, b.TextSize, b.label)
			label.Color = actionButtonPalette(b.variant).text
			label.MaxLines = 1
			return layout.Center.Layout(gtx, label.Layout)
		})
	})
}
