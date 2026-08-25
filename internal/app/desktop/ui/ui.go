// Package ui holds the interface elements the desktop application shares
// between its screens: the modal shell, the toolbar button, the dropdown menu
// and the painting helpers they are built from.
//
// It is a package rather than a naming convention because the boundary is the
// point. Every one of these used to be a method on *Window, which meant a
// component could reach any state the window had, and the ones that did are
// exactly the ones that were hard to move: a "component" that reads the peer
// list is a screen. Here the compiler decides — a component gets a Kit and its
// arguments, and nothing else.
//
// The values come from docs/design/CHANGES.md and CHANGES1.md; the reasoning
// behind them is in docs/ui.md.
package ui

import (
	"image"
	"image/color"

	"gioui.org/f32"
	"gioui.org/font"
	"gioui.org/io/event"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// Kit is what every component needs from the application it is drawn in. One
// per window, built once and passed by value.
type Kit struct {
	// Theme shapes text.
	Theme *material.Theme
	// CloseIcon is the glyph in a modal's close button.
	CloseIcon *widget.Icon
	// EmojiFace is the family emoji are drawn in. It is given rather than
	// assumed: the family is a bundled font the application registers with its
	// own shaper, and which font that is — and why it cannot be the host's — is
	// the application's business, not a component's.
	EmojiFace font.Typeface
}

// VerticallyCentered centres one widget in the height it was given without
// stretching it.
func VerticallyCentered(gtx layout.Context, content layout.Widget) layout.Dimensions {
	return layout.Flex{
		Axis:      layout.Vertical,
		Alignment: layout.Start,
		Spacing:   layout.SpaceSides,
	}.Layout(gtx, layout.Rigid(content))
}

// Fill paints the whole area the caller was given.
func Fill(gtx layout.Context, fill color.NRGBA) {
	defer clip.Rect{Max: gtx.Constraints.Max}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: fill}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

// FillRounded paints the whole area the caller was given, with rounded
// corners.
//
// "The whole area" means Constraints.Max, which is a trap worth naming: inside
// a widget that has not been told its size — the background half of a
// layout.Background, anything under a Clickable — the maximum is the room
// OFFERED, not the room taken. Use Filled instead of reaching for this
// directly; it is what pins the two together.
func FillRounded(gtx layout.Context, fill color.NRGBA, radius unit.Dp) {
	bounds := image.Rectangle{Max: gtx.Constraints.Max}
	paint.FillShape(gtx.Ops, fill, clip.UniformRRect(bounds, gtx.Dp(radius)).Op(gtx.Ops))
}

// Filled draws w on a rounded fill of exactly w's own size.
//
// This is the shape every pill, row and card in this package wants, and doing
// it by hand is how a fill ends up covering the room its widget was offered
// instead of the room it took — which put an opaque column down the console
// window under the tab menu.
func Filled(gtx layout.Context, fill color.NRGBA, radius unit.Dp, w layout.Widget) layout.Dimensions {
	return layout.Background{}.Layout(gtx,
		func(gtx layout.Context) layout.Dimensions {
			// layout.Background hands the background the content size as its
			// MINIMUM and leaves the maximum alone.
			gtx.Constraints.Max = gtx.Constraints.Min
			FillRounded(gtx, fill, radius)
			return layout.Dimensions{Size: gtx.Constraints.Min}
		},
		w,
	)
}

// Chip is a rounded, clickable, filled surface: the shape of a console tab
// pill, a toolbar button and a popup menu row.
//
// It goes through widget.Clickable rather than material.Clickable, which means
// no Material ink. That decoration is drawn against a SQUARE clip of the
// button's size, and over a rounded fill it showed as four pale dots at the
// corners on hover and as a white ring that flashed and faded on every click.
// Neither appears anywhere in the design: these chips say what they are by
// their fill, and the one control that does react to the pointer — the modal
// close button — does it by changing colour.
//
// The semantic tag stays: a screen reader still finds a button here.
func (k Kit) Chip(gtx layout.Context, button *widget.Clickable, fill color.NRGBA, radius unit.Dp, w layout.Widget) layout.Dimensions {
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		return Filled(gtx, fill, radius, w)
	})
}

// SwallowPresses makes an area consume every pointer press that lands on it, so
// the press reaches nothing behind it.
//
// A floating surface needs this or it is not really a surface. Only its
// interactive widgets register for input, so a press on its padding, its header,
// or the gap between two of its blocks falls straight through to whatever is
// underneath — which for a popup is the backdrop, and the backdrop's whole job
// is to dismiss. The user presses the middle of an open panel and the panel
// vanishes.
//
// The modal shell answers the same question by asking whether the press landed
// inside the card (see ModalCardBounds); a surface placed by an offset rather
// than centred does not know its own window coordinates and cannot ask that, so
// it answers by being in the way instead.
//
// Call it with the surface's own size and BEFORE replaying the surface's
// recorded content: Gio hands a press to the LAST area registered over that
// point, so the widgets drawn afterwards still win it.
func SwallowPresses(gtx layout.Context, tag event.Tag, size image.Point) {
	area := clip.Rect{Max: size}.Push(gtx.Ops)
	event.Op(gtx.Ops, tag)
	area.Pop()

	// Drained and discarded. Nothing acts on these — being consumed IS the
	// behaviour — but an undrained filter stops matching after Gio's queue for
	// it fills, and the surface would start leaking presses again.
	for {
		if _, ok := gtx.Event(pointer.Filter{Target: tag, Kinds: pointer.Press | pointer.Release}); !ok {
			return
		}
	}
}

// Icon draws one vector icon at a fixed size.
//
// A nil icon leaves the space blank instead of panicking, which Gio's own
// Icon.Layout does. The space is what matters to everything around it: the
// control keeps its size, its hit area and its accessibility label, so a window
// assembled without one glyph — every test in the application builds one by
// literal — is a window with a blank button rather than a crash.
func Icon(gtx layout.Context, icon *widget.Icon, size unit.Dp, tint color.NRGBA) layout.Dimensions {
	side := gtx.Dp(size)
	gtx.Constraints = layout.Exact(image.Pt(side, side))
	if icon == nil {
		return layout.Dimensions{Size: image.Pt(side, side)}
	}
	return icon.Layout(gtx, tint)
}

// PointInside reports whether a pointer position falls inside bounds.
func PointInside(point f32.Point, bounds image.Rectangle) bool {
	return point.X >= float32(bounds.Min.X) && point.X < float32(bounds.Max.X) &&
		point.Y >= float32(bounds.Min.Y) && point.Y < float32(bounds.Max.Y)
}

// ChipFill is the background of every small selectable control: console tab
// pill, toolbar button, popup menu row. The design describes each of them by
// pointing at the others ("as the selected tab"), so there is one pair of
// values and one place to change it.
//
// Only the fill is shared. The idle label colour is not: the tab strip uses a
// dimmer #dce4f0 than the #f5f7fa of the other two, and pretending otherwise
// would quietly restyle the strip.
func ChipFill(active bool) color.NRGBA {
	if active {
		return color.NRGBA{R: 0x39, G: 0x62, B: 0xaa, A: 255}
	}
	return color.NRGBA{R: 0x22, G: 0x2e, B: 0x3e, A: 255}
}

// ChipActiveLabel is the label colour of a selected chip, shared for the same
// reason as the fill.
func ChipActiveLabel() color.NRGBA {
	return color.NRGBA{R: 255, G: 255, B: 255, A: 255}
}
