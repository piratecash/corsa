package ui

import (
	"image/color"

	"gioui.org/layout"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// toolbar_button.go is the small labelled button that sits in the window
// chrome: the language selector in the header and the Console button in the
// footer. See §3 and screen `7f` of docs/design/CHANGES1.md.
//
// It exists because those two were assembled separately and drifted: the
// language button was pinned to 76dp with centred text and a literal "v" for a
// chevron, the console button sized itself to its content with a real icon and
// different insets. Two neighbouring buttons of the same kind looked like two
// different controls.

const (
	toolbarButtonRadiusDp   = 4
	toolbarButtonPaddingXDp = 12
	toolbarButtonPaddingYDp = 10
	toolbarButtonGapDp      = 8
	toolbarButtonIconDp     = 18
	toolbarButtonTextSp     = 13
)

func toolbarButtonLabelColor() color.NRGBA {
	return color.NRGBA{R: 0xf5, G: 0xf7, B: 0xfa, A: 255}
}

// IconSide says which side of the label an icon sits on. The console button
// leads with its chart glyph; the language button trails with a chevron,
// because the chevron is about the button rather than about what the button
// does.
type IconSide uint8

const (
	IconLeading IconSide = iota
	IconTrailing
)

// ToolbarButtonOpts describes one button to ToolbarButton.
type ToolbarButtonOpts struct {
	// Label is the text. Never empty: these buttons are read, not guessed at.
	Label string
	// Icon accompanies it.
	Icon *widget.Icon
	// IconSide places that icon.
	IconSide IconSide
	// Active is true while the surface the button owns is open, which paints
	// it like a selected tab.
	Active bool
}

// ToolbarButton draws one. It sizes itself to its content: a fixed width is
// what cut "中文" off the language button and left "EN" floating in the middle
// of it.
func (k Kit) ToolbarButton(gtx layout.Context, button *widget.Clickable, opts ToolbarButtonOpts) layout.Dimensions {
	label := toolbarButtonLabelColor()
	if opts.Active {
		label = ChipActiveLabel()
	}

	icon := layout.Rigid(func(gtx layout.Context) layout.Dimensions {
		return Icon(gtx, opts.Icon, unit.Dp(toolbarButtonIconDp), label)
	})
	text := layout.Rigid(func(gtx layout.Context) layout.Dimensions {
		style := material.Label(k.Theme, unit.Sp(toolbarButtonTextSp), opts.Label)
		style.Color = label
		style.MaxLines = 1
		return style.Layout(gtx)
	})
	gap := layout.Rigid(layout.Spacer{Width: unit.Dp(toolbarButtonGapDp)}.Layout)

	children := []layout.FlexChild{icon, gap, text}
	if opts.IconSide == IconTrailing {
		children = []layout.FlexChild{text, gap, icon}
	}

	return k.Chip(gtx, button, ChipFill(opts.Active), unit.Dp(toolbarButtonRadiusDp), func(gtx layout.Context) layout.Dimensions {
		return layout.Inset{
			Top: unit.Dp(toolbarButtonPaddingYDp), Bottom: unit.Dp(toolbarButtonPaddingYDp),
			Left: unit.Dp(toolbarButtonPaddingXDp), Right: unit.Dp(toolbarButtonPaddingXDp),
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
		})
	})
}
