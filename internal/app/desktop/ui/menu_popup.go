package ui

import (
	"image"
	"image/color"

	"gioui.org/io/event"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// menu_popup.go is the one dropdown in the application: the language selector
// and the console tabs that do not fit on a narrow strip. See §1, §2, §4 and
// screen `7e` of docs/design/CHANGES1.md.
//
// Before it, the two were different things. The language menu was a card with
// no border whose rows were material.Buttons 8dp apart; the folded tabs were
// bare pills drawn straight over the tab content, with no card at all — they
// read as part of the content rather than as an open menu.
//
// The component is the CARD and its backdrop. Where the card goes is the
// caller's business, because the two anchors have nothing in common: the
// language menu hangs under a header button in window coordinates, the tab
// menu under a strip inside the console modal.

const (
	menuPopupRadiusDp    = 8
	menuPopupPaddingDp   = 8
	menuPopupGapDp       = 4
	menuPopupRowRadiusDp = 5
	menuPopupRowPadXDp   = 12
	menuPopupRowPadYDp   = 9
	menuPopupTextSp      = 13
	// MenuPopupAnchorGapDp is the air between the button that opens a popup
	// and the popup itself. Small on purpose: a dropdown that floats away from
	// its button stops looking like it belongs to it, which is what the first
	// cut of the language menu did with a hard-coded 58dp offset that no
	// longer matched the header it was measured against.
	MenuPopupAnchorGapDp = 4
	// MenuPopupLanguageWidthDp is the fixed width of the language menu. The
	// tab menu takes its width from its content instead — its labels are one
	// word each, and a 220dp card over a phone-width tab strip would cover
	// most of it.
	MenuPopupLanguageWidthDp = 220
	// menuPopupChromeDp is everything the card puts around its rows on one
	// side: the padding and the border.
	menuPopupChromeDp = menuPopupPaddingDp + 1
)

func menuPopupCardColor() color.NRGBA {
	return color.NRGBA{R: 0x15, G: 0x1a, B: 0x22, A: 255}
}

func menuPopupBorderColor() color.NRGBA {
	return color.NRGBA{R: 0x34, G: 0x44, B: 0x57, A: 255}
}

func menuPopupBackdropColor() color.NRGBA {
	return color.NRGBA{R: 0x06, G: 0x08, B: 0x0c, A: 102}
}

func menuPopupRowLabelColor() color.NRGBA {
	return color.NRGBA{R: 0xf5, G: 0xf7, B: 0xfa, A: 255}
}

// MenuPopupItem is one row.
type MenuPopupItem struct {
	// Label is the row's text.
	Label string
	// Button carries the row's clicks. The caller owns it and drains it — the
	// popup only lays it out, so that what a row DOES stays next to the state
	// it changes.
	Button *widget.Clickable
	// Selected paints the row as the current choice.
	Selected bool
}

// MenuPopupWidth says how the card is sized across.
type MenuPopupWidth uint8

const (
	// MenuPopupWidthGiven takes the width from the constraints the caller has
	// already applied. The language menu is a fixed 220dp.
	MenuPopupWidthGiven MenuPopupWidth = iota
	// MenuPopupWidthFit measures the widest row and takes that.
	MenuPopupWidthFit
)

// MenuPopup is one open dropdown.
type MenuPopup struct {
	// Items are the rows, top to bottom. Never empty: a menu with nothing in
	// it is a menu that should not have opened.
	Items []MenuPopupItem
	// Scroll owns the list position. The card scrolls when the height it is
	// given cannot hold every row — which is the normal case for the language
	// menu on a phone in landscape.
	Scroll *widget.List
	// Width selects how wide the card is.
	Width MenuPopupWidth
}

// MenuPopupScrim says whether a backdrop tints what it covers.
//
// Catching the press and tinting the background are two different jobs, and
// the design asks for them separately: the console's tab menu sits over the
// console card and dims it (screen 6m), while the language menu over the whole
// application does not (screen 7b) — a 40% wash over every contact and message
// for the sake of a six-row dropdown reads as a modal dialogue, which it is
// not. Both still swallow the press.
type MenuPopupScrim uint8

const (
	// MenuPopupScrimNone is an invisible click-catcher.
	MenuPopupScrimNone MenuPopupScrim = iota
	// MenuPopupScrimDim tints what it covers.
	MenuPopupScrimDim
)

// MenuPopupAnchorX clamps a popup's preferred left edge into the area it is
// drawn in. want is where the anchor puts it, width the card's width and
// available the width of that area.
//
// Both menus anchor to the RIGHT edge of their button, and both buttons sit at
// the right end of their row, so without the clamp the card hangs off the edge
// exactly when it is wider than the button — the normal case.
func MenuPopupAnchorX(want, width, available int) int {
	if limit := available - width; want > limit {
		want = limit
	}
	if want < 0 {
		return 0
	}
	return want
}

// MenuPopupBackdrop covers the area behind an open popup and closes it on a
// press.
//
// It consumes every press it receives, whether or not the press is what closed
// the menu. That is the half the language menu never had: it used to be a
// Stacked layer that let input through, so the click a user aims at empty
// space to dismiss the menu also selected whatever contact was underneath.
func (k Kit) MenuPopupBackdrop(gtx layout.Context, tag event.Tag, scrim MenuPopupScrim, dismiss func()) layout.Dimensions {
	if scrim == MenuPopupScrimDim {
		Fill(gtx, menuPopupBackdropColor())
	}

	area := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, tag)
	area.Pop()

	for {
		ev, ok := gtx.Event(pointer.Filter{Target: tag, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			dismiss()
		}
	}
	return layout.Dimensions{Size: gtx.Constraints.Max}
}

// MenuPopupFitWidth measures the widest row and returns the card width that
// holds it.
//
// Only the LABELS are measured, never the rows. A row is a widget.Clickable,
// and laying one out drains its click queue — measuring rows would swallow
// every second click on the menu. A label has no state to disturb, and the row
// is the label plus a fixed padding either side.
func (k Kit) MenuPopupFitWidth(gtx layout.Context, popup MenuPopup) int {
	measure := gtx
	measure.Ops = new(op.Ops)
	measure.Constraints.Min = image.Point{}

	widest := 0
	for _, item := range popup.Items {
		label := material.Label(k.Theme, unit.Sp(menuPopupTextSp), item.Label)
		label.MaxLines = 1
		widest = max(widest, label.Layout(measure).Size.X)
	}
	return widest + 2*gtx.Dp(unit.Dp(menuPopupRowPadXDp)) + 2*gtx.Dp(unit.Dp(menuPopupChromeDp))
}

// MenuPopupCard draws the card itself, at the position the caller has already
// decided. The card hugs its rows: it is as tall as they are, up to the height
// it is given, and as wide as its Width says.
func (k Kit) MenuPopupCard(gtx layout.Context, popup MenuPopup) layout.Dimensions {
	if popup.Width == MenuPopupWidthFit {
		width := min(k.MenuPopupFitWidth(gtx, popup), gtx.Constraints.Max.X)
		gtx.Constraints.Min.X = width
		gtx.Constraints.Max.X = width
	}
	// Never a fixed height. The caller passes the room available under the
	// anchor as the MAXIMUM; a card stretched to it would end in a slab of
	// empty background under the last row.
	gtx.Constraints.Min.Y = 0

	border := widget.Border{
		Color:        menuPopupBorderColor(),
		CornerRadius: unit.Dp(menuPopupRadiusDp),
		Width:        unit.Dp(1),
	}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return Filled(gtx, menuPopupCardColor(), unit.Dp(menuPopupRadiusDp), func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(unit.Dp(menuPopupPaddingDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return k.menuPopupRows(gtx, popup)
			})
		})
	})
}

func (k Kit) menuPopupRows(gtx layout.Context, popup MenuPopup) layout.Dimensions {
	popup.Scroll.Axis = layout.Vertical
	// Every row is as wide as the card. Left to themselves they size to their
	// own text, which in the tab menu gave "Info" and "Donate" visibly
	// different widths inside one card.
	gtx.Constraints.Min.X = gtx.Constraints.Max.X

	list := material.List(k.Theme, popup.Scroll)
	// The scrollbar floats over the rows instead of reserving a gutter. Gio's
	// default is Occupy, which takes its width out of the content — so the
	// rows sat with the card's 8dp padding on the left and 8dp PLUS the bar on
	// the right, and the card looked lopsided even with nothing to scroll.
	list.AnchorStrategy = material.Overlay
	return list.Layout(gtx, len(popup.Items), func(gtx layout.Context, index int) layout.Dimensions {
		item := popup.Items[index]
		row := func(gtx layout.Context) layout.Dimensions {
			return k.menuPopupRow(gtx, item)
		}
		if index == len(popup.Items)-1 {
			return row(gtx)
		}
		return layout.Inset{Bottom: unit.Dp(menuPopupGapDp)}.Layout(gtx, row)
	})
}

func (k Kit) menuPopupRow(gtx layout.Context, item MenuPopupItem) layout.Dimensions {
	label := menuPopupRowLabelColor()
	if item.Selected {
		label = ChipActiveLabel()
	}
	return k.Chip(gtx, item.Button, ChipFill(item.Selected), unit.Dp(menuPopupRowRadiusDp), func(gtx layout.Context) layout.Dimensions {
		return layout.Inset{
			Top: unit.Dp(menuPopupRowPadYDp), Bottom: unit.Dp(menuPopupRowPadYDp),
			Left: unit.Dp(menuPopupRowPadXDp), Right: unit.Dp(menuPopupRowPadXDp),
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			text := material.Label(k.Theme, unit.Sp(menuPopupTextSp), item.Label)
			text.Color = label
			text.MaxLines = 1
			return text.Layout(gtx)
		})
	})
}
