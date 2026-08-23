package ui

import (
	"image"
	"image/color"

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

// modal.go is the chrome every modal window shares: the dimmed backdrop that
// swallows input meant for the screen underneath, the card, the header with
// its title and close button, and the sizing rules that decide how big the
// card is on a desktop window and on a phone.
//
// It exists because there were about to be three of them — identity details,
// the console and the traffic graph — and each copy would have owned its own
// answer to the same questions: which colours, how far from the window edge,
// whether a click beside the card closes it, whether a blank patch of card
// leaks the click through to the application below. See docs/design/CHANGES.md
// §1—§2 for the design these values come from.

const (
	// ModalCardInsetDp is how far a full-window (console) card stays from the
	// window edges on the desktop layout.
	ModalCardInsetDp = 6
	// modalCardPaddingDp is the card's inner padding, between its border and
	// the header or the content.
	modalCardPaddingDp = 16
	// ModalCardRadiusDp is the corner radius of a desktop card. Identity
	// details keeps its own, rounder, radius — see ModalIdentityRadiusDp.
	ModalCardRadiusDp = 12
	// ModalIdentityRadiusDp is the identity panel's corner radius, which the
	// design keeps larger than the other modals'.
	ModalIdentityRadiusDp = 16
	// modalHeaderGapDp separates the header row from the content below it.
	modalHeaderGapDp = 14
	// modalTitleSizeSp is the header title size.
	modalTitleSizeSp = 22

	// modalCloseButtonSideDp is the diameter of the close button. 44dp is the
	// smallest comfortable touch target, and the button is the only way out of
	// a modal on a phone.
	modalCloseButtonSideDp = 44
	// modalCloseButtonIconDp is the size of the glyph inside that circle.
	modalCloseButtonIconDp = 22
	// modalCloseButtonBorderDp is the width of the ring around it.
	modalCloseButtonBorderDp = 1

	// modalCenteredMaxWidthDp and modalCenteredMaxHeightDp bound a centred
	// card, and modalCenteredEdgeDp keeps it off the window edges when the
	// window is smaller than those bounds.
	modalCenteredMaxWidthDp  = 384
	modalCenteredMaxHeightDp = 520
	modalCenteredEdgeDp      = 16
)

// modalBackdropColor dims whatever the modal covers. Semi-transparent rather
// than opaque so the user keeps their bearings in the application underneath.
func modalBackdropColor() color.NRGBA {
	return color.NRGBA{R: 0x06, G: 0x08, B: 0x0c, A: 140}
}

func modalCardFillColor() color.NRGBA {
	return color.NRGBA{R: 0x12, G: 0x1b, B: 0x25, A: 255}
}

func modalCardBorderColor() color.NRGBA {
	return color.NRGBA{R: 0x34, G: 0x44, B: 0x57, A: 255}
}

func modalTitleColor() color.NRGBA {
	return color.NRGBA{R: 0xf6, G: 0xf8, B: 0xfb, A: 255}
}

// ModalCloseButtonState is the visual state of the close button. The button
// has exactly two looks; which one is showing follows from the pointer, never
// from what the modal itself is doing.
type ModalCloseButtonState uint8

const (
	ModalCloseButtonIdle ModalCloseButtonState = iota
	ModalCloseButtonHighlighted
)

// ModalCloseButtonPalette is the full colour set of one close-button state.
// Grouped rather than passed as three arguments so a state can never be drawn
// half in one look and half in the other.
type ModalCloseButtonPalette struct {
	Fill   color.NRGBA
	Border color.NRGBA
	Icon   color.NRGBA
}

var modalCloseButtonPalettes = map[ModalCloseButtonState]ModalCloseButtonPalette{
	ModalCloseButtonIdle: {
		Fill:   color.NRGBA{R: 0x1b, G: 0x27, B: 0x35, A: 255},
		Border: color.NRGBA{R: 0x33, G: 0x44, B: 0x5a, A: 255},
		Icon:   color.NRGBA{R: 0x9d, G: 0xad, B: 0xc2, A: 255},
	},
	ModalCloseButtonHighlighted: {
		Fill:   color.NRGBA{R: 0x33, G: 0x45, B: 0x5c, A: 255},
		Border: color.NRGBA{R: 0x4a, G: 0x5f, B: 0x7a, A: 255},
		Icon:   color.NRGBA{R: 0xea, G: 0xf1, B: 0xf8, A: 255},
	},
}

func ModalCloseButtonColors(state ModalCloseButtonState) ModalCloseButtonPalette {
	return modalCloseButtonPalettes[state]
}

// ModalCloseButtonStateFor maps the pointer onto the one visible state.
//
// Keyboard focus deliberately does NOT highlight. It looks like an
// accessibility win — this button draws no focus ring — but the identity panel
// hands focus to its close button the moment it opens (the first item of its
// focus ring), so a focus-driven highlight left that button stuck in the hover
// look for the whole life of the panel while the console's, which runs no
// focus ring, reacted to the mouse. One component with two behaviours is worse
// than no focus affordance, and the design names exactly two states.
func ModalCloseButtonStateFor(hovered bool) ModalCloseButtonState {
	if hovered {
		return ModalCloseButtonHighlighted
	}
	return ModalCloseButtonIdle
}

// ModalCloseButton draws the shared close button: a 44dp circle with a 1dp
// ring, filled per its current state. hint is the accessibility description
// screen readers announce, since the button carries no text.
//
// The ring is an outer disc with the fill laid over it inset by its width,
// rather than a stroked outline. A stroke is centred ON the path, so half of
// it falls outside the button's bounds — and widget.Clickable clips to exactly
// those bounds, which cut the ring on all four sides.
func (k Kit) ModalCloseButton(gtx layout.Context, button *widget.Clickable, hint string) layout.Dimensions {
	side := gtx.Dp(unit.Dp(modalCloseButtonSideDp))
	gtx.Constraints = layout.Exact(image.Pt(side, side))
	palette := ModalCloseButtonColors(ModalCloseButtonStateFor(button.Hovered()))

	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(hint).Add(gtx.Ops)

		border := gtx.Dp(unit.Dp(modalCloseButtonBorderDp))
		outer := image.Rectangle{Max: image.Pt(side, side)}
		inner := image.Rect(border, border, side-border, side-border)
		paint.FillShape(gtx.Ops, palette.Border, clip.Ellipse(outer).Op(gtx.Ops))
		paint.FillShape(gtx.Ops, palette.Fill, clip.Ellipse(inner).Op(gtx.Ops))

		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Icon(gtx, k.CloseIcon, unit.Dp(modalCloseButtonIconDp), palette.Icon)
		})
	})
}

// ModalSizing selects how a modal card is measured on the desktop layout. The
// compact layout ignores it — a phone screen has room for exactly one card, so
// every modal fills it.
type ModalSizing uint8

const (
	// ModalSizingCentered bounds the card at 384×520dp and centres it. Used by
	// identity details, whose content is a QR code and two buttons: stretched
	// to a desktop window it would be mostly empty.
	ModalSizingCentered ModalSizing = iota
	// ModalSizingInset spreads the card to the window edges less
	// ModalCardInsetDp. Used by the console, which wants every pixel of width
	// it can get.
	ModalSizingInset
)

// ModalCardSize measures the card. window is the client area in pixels.
func ModalCardSize(window image.Point, pxPerDp float32, sizing ModalSizing, compact bool) image.Point {
	if compact {
		return window
	}
	switch sizing {
	case ModalSizingInset:
		return modalInsetCardSize(window, pxPerDp)
	default:
		return modalCenteredCardSize(window, pxPerDp)
	}
}

func modalCenteredCardSize(window image.Point, pxPerDp float32) image.Point {
	edge := scaleDp(modalCenteredEdgeDp, pxPerDp)
	return image.Pt(
		clampNonNegative(min(scaleDp(modalCenteredMaxWidthDp, pxPerDp), window.X-2*edge)),
		clampNonNegative(min(scaleDp(modalCenteredMaxHeightDp, pxPerDp), window.Y-2*edge)),
	)
}

func modalInsetCardSize(window image.Point, pxPerDp float32) image.Point {
	inset := scaleDp(ModalCardInsetDp, pxPerDp)
	return image.Pt(
		clampNonNegative(window.X-2*inset),
		clampNonNegative(window.Y-2*inset),
	)
}

// scaleDp converts dp to pixels without a layout.Context. The sizing helpers
// are pure so they can be tested against window geometry directly, and gtx.Dp
// is not available to them.
func scaleDp(value int, pxPerDp float32) int {
	return int(float32(value)*pxPerDp + 0.5)
}

func clampNonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// ModalCardBounds places a measured card in the centre of the window. The
// backdrop needs the rectangle in window coordinates to tell a click beside
// the card from one on it.
func ModalCardBounds(window, card image.Point) image.Rectangle {
	origin := image.Pt((window.X-card.X)/2, (window.Y-card.Y)/2)
	return image.Rectangle{Min: origin, Max: origin.Add(card)}
}

// Modal describes one modal window to Kit.Modal. Every field is required: a
// modal with no title, no close button or no way to dismiss it is not a state
// this application has, and defaulting any of them would hide the omission at
// the call site instead of at the compiler.
type Modal struct {
	// Title is the header text.
	Title string
	// CloseHint is what a screen reader announces for the close button.
	CloseHint string
	// Close is the header's close button.
	Close *widget.Clickable
	// DismissTag is the backdrop's pointer target. It must be a pointer unique
	// to this modal and stable across frames.
	DismissTag event.Tag
	// Dismiss closes the modal. Called when the user clicks the desktop
	// backdrop beside the card.
	Dismiss func()
	// CornerRadius rounds the desktop card.
	CornerRadius unit.Dp
	// Sizing measures the desktop card.
	Sizing ModalSizing
	// Compact is the phone layout, where the card fills the screen.
	Compact bool
	// Content fills the card below the header.
	Content layout.Widget
}

// Modal draws one: backdrop, card, header, content.
//
// The backdrop covers the whole window and consumes every press on it, whether
// or not that press dismisses anything. That is deliberate and is two separate
// guarantees: a click beside the card closes the modal, and a click on a blank
// patch of the card — its padding, the gap under short content, the whole
// screen in the compact layout — reaches neither the modal nor the application
// underneath. Without the second one the contact list stays clickable through
// a modal that is visibly covering it.
func (k Kit) Modal(gtx layout.Context, modal Modal) layout.Dimensions {
	cardSize := ModalCardSize(gtx.Constraints.Max, gtx.Metric.PxPerDp, modal.Sizing, modal.Compact)
	cardBounds := ModalCardBounds(gtx.Constraints.Max, cardSize)

	return layout.Stack{Alignment: layout.Center}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return k.modalBackdrop(gtx, modal, cardBounds)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if cardSize.X == 0 || cardSize.Y == 0 {
				return layout.Dimensions{}
			}
			gtx.Constraints = layout.Exact(cardSize)
			return k.modalCard(gtx, modal)
		}),
	)
}

func (k Kit) modalBackdrop(gtx layout.Context, modal Modal, cardBounds image.Rectangle) layout.Dimensions {
	Fill(gtx, modalBackdropColor())

	area := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, modal.DismissTag)
	area.Pop()

	for {
		ev, ok := gtx.Event(pointer.Filter{Target: modal.DismissTag, Kinds: pointer.Press})
		if !ok {
			break
		}
		press, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		// Only the desktop area OUTSIDE the card dismisses. In the compact
		// layout the card is the whole window, so there is no outside.
		if !modal.Compact && !PointInside(press.Position, cardBounds) {
			modal.Dismiss()
		}
	}
	return layout.Dimensions{Size: gtx.Constraints.Max}
}

// modalCard draws the card itself. The compact layout drops the border and the
// radius: a card that fills the screen has no edge to round, and a 1dp line
// hugging the display edge reads as a rendering artefact.
func (k Kit) modalCard(gtx layout.Context, modal Modal) layout.Dimensions {
	body := func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(modalCardPaddingDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return k.modalHeader(gtx, modal)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(modalHeaderGapDp)}.Layout),
				layout.Flexed(1, modal.Content),
			)
		})
	}

	if modal.Compact {
		Fill(gtx, modalCardFillColor())
		return body(gtx)
	}

	border := widget.Border{
		Color:        modalCardBorderColor(),
		CornerRadius: modal.CornerRadius,
		Width:        unit.Dp(1),
	}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		FillRounded(gtx, modalCardFillColor(), modal.CornerRadius)
		return body(gtx)
	})
}

func (k Kit) modalHeader(gtx layout.Context, modal Modal) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			label := material.Label(k.Theme, unit.Sp(modalTitleSizeSp), modal.Title)
			label.Color = modalTitleColor()
			label.MaxLines = 1
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.ModalCloseButton(gtx, modal.Close, modal.CloseHint)
		}),
	)
}
