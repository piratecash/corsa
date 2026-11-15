package ui

import (
	"image"
	"image/color"

	"gioui.org/io/event"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
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

	// ModalButtonRadiusDp is the corner radius for labelled modal actions.
	ModalButtonRadiusDp = 8
	// modalCloseButtonSideDp is the standard icon control's touch target.
	modalCloseButtonSideDp = 44
	// modalCloseButtonIconDp is the size of the glyph inside the control.
	modalCloseButtonIconDp = 22
	// modalCloseButtonBorderDp is the width of the frame around it.
	modalCloseButtonBorderDp = 1

	// modalCenteredMaxWidthDp and modalCenteredMaxHeightDp bound a centred
	// card, and modalCenteredEdgeDp keeps it off the window edges when the
	// window is smaller than those bounds.
	modalCenteredMaxWidthDp  = 384
	modalContentMaxWidthDp   = 410
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

// RoundIconButton describes a circular icon control.
//
// The zero value is not usable: a button with no icon and no palette is not a
// state this application has, and defaulting either would hide the omission
// at the call site instead of at the compiler. Sizes are the exception —
// zero means the standard 44/22.
type RoundIconButton struct {
	// Icon is the glyph in the middle.
	Icon *widget.Icon
	// Hint is what a screen reader announces, since the button carries no
	// text.
	Hint string
	// SideDp is the circle's diameter and IconDp the glyph's size; zero for
	// each means the standard 44dp / 22dp.
	SideDp int
	IconDp int
	// Idle and Hovered are the two looks. Which one is showing follows from
	// the pointer, never from what the surface is doing — see
	// ModalCloseButtonStateFor.
	Idle    ModalCloseButtonPalette
	Hovered ModalCloseButtonPalette
	// Enabled false draws the control at DimmedAlpha and wraps NO
	// Clickable: no hover, no hit area, no click. It is how a control says
	// "not now" without disappearing — a control that vanishes moves
	// everything laid out beside it.
	Enabled bool
}

// DimmedAlpha is what an unavailable control keeps of itself.
const DimmedAlpha = 0.4

func dim(c color.NRGBA) color.NRGBA {
	c.A = uint8(float32(c.A) * DimmedAlpha)
	return c
}

// RoundIconButton draws a filled control with a 1dp frame inside its hit area.
func (k Kit) RoundIconButton(gtx layout.Context, button *widget.Clickable, opts RoundIconButton) layout.Dimensions {
	sideDp := opts.SideDp
	if sideDp == 0 {
		sideDp = modalCloseButtonSideDp
	}
	iconDp := opts.IconDp
	if iconDp == 0 {
		iconDp = modalCloseButtonIconDp
	}
	side := min(gtx.Dp(unit.Dp(sideDp)), gtx.Constraints.Max.X, gtx.Constraints.Max.Y)
	gtx.Constraints = layout.Exact(image.Pt(side, side))

	palette := opts.Idle
	switch {
	case !opts.Enabled:
		palette = ModalCloseButtonPalette{
			Fill:   dim(palette.Fill),
			Border: dim(palette.Border),
			Icon:   dim(palette.Icon),
		}
	case button.Hovered():
		palette = opts.Hovered
	}

	draw := func(gtx layout.Context) layout.Dimensions {
		semantic.DescriptionOp(opts.Hint).Add(gtx.Ops)

		border := gtx.Dp(unit.Dp(modalCloseButtonBorderDp))
		outer := image.Rectangle{Max: image.Pt(side, side)}
		inner := image.Rect(border, border, side-border, side-border)
		paint.FillShape(gtx.Ops, palette.Border, clip.Ellipse(outer).Op(gtx.Ops))
		paint.FillShape(gtx.Ops, palette.Fill, clip.Ellipse(inner).Op(gtx.Ops))

		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Icon(gtx, opts.Icon, unit.Dp(iconDp), palette.Icon)
		})
	}

	if !opts.Enabled {
		// No Clickable at all: a disabled control that still reported hovers
		// would light up under the pointer and do nothing when pressed.
		return draw(gtx)
	}
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		pointer.CursorPointer.Add(gtx.Ops)
		return draw(gtx)
	})
}

// ModalCloseButton draws the shared close button in the standard palette.
// hint is the accessibility description screen readers announce.
func (k Kit) ModalCloseButton(gtx layout.Context, button *widget.Clickable, hint string) layout.Dimensions {
	return k.ModalIconButton(gtx, button, k.CloseIcon, hint)
}

// ModalIconButton draws a secondary icon action in the shared modal style.
func (k Kit) ModalIconButton(gtx layout.Context, button *widget.Clickable, icon *widget.Icon, hint string) layout.Dimensions {
	return k.RoundIconButton(gtx, button, RoundIconButton{
		Icon:    icon,
		Hint:    hint,
		Idle:    ModalCloseButtonColors(ModalCloseButtonIdle),
		Hovered: ModalCloseButtonColors(ModalCloseButtonHighlighted),
		Enabled: true,
	})
}

// ModalActionButton draws a full-width labelled action in a modal.
func (k Kit) ModalActionButton(gtx layout.Context, button *widget.Clickable, icon *widget.Icon, label string, primary bool) layout.Dimensions {
	gtx.Constraints.Min.X = gtx.Constraints.Max.X
	gtx.Constraints.Min.Y = max(gtx.Constraints.Min.Y, min(gtx.Dp(unit.Dp(modalCloseButtonSideDp)), gtx.Constraints.Max.Y))
	variant := ButtonSecondary
	if primary {
		variant = ButtonPrimary
	}
	return k.ButtonFrame(gtx, button, variant, unit.Dp(ModalButtonRadiusDp), func(gtx layout.Context) layout.Dimensions {
		semantic.LabelOp(label).Add(gtx.Ops)
		return layout.Inset{Left: 10, Right: 10, Top: 6, Bottom: 6}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			labelStyle := material.Body1(k.Theme, label)
			labelStyle.Color = actionButtonPalette(variant).text
			labelStyle.MaxLines = 1
			if icon == nil {
				return layout.Center.Layout(gtx, labelStyle.Layout)
			}
			return layout.Flex{Alignment: layout.Middle, Spacing: layout.SpaceSides, Gap: gtx.Dp(unit.Dp(8))}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return Icon(gtx, icon, unit.Dp(19), ModalCloseButtonColors(ModalCloseButtonIdle).Icon)
				}),
				layout.Rigid(labelStyle.Layout),
			)
		})
	})
}

// ModalActionFooter separates a fitted modal's actions from its body.
func (k Kit) ModalActionFooter(gtx layout.Context, content layout.Widget) layout.Dimensions {
	return Filled(gtx, color.NRGBA{R: 23, G: 35, B: 48, A: 255}, 0, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				size := image.Pt(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(1)))
				gtx.Constraints = layout.Exact(size)
				Fill(gtx, modalCardBorderColor())
				return layout.Dimensions{Size: size}
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{Top: 12, Bottom: 12, Left: modalCardPaddingDp, Right: modalCardPaddingDp}.Layout(gtx, content)
			}),
		)
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
	// ModalSizingContent centers a card that fits its content up to the
	// available height. A scrollable Content keeps a supplied Footer visible.
	ModalSizingContent
)

// ModalCardSize measures the card. window is the client area in pixels.
func ModalCardSize(window image.Point, pxPerDp float32, sizing ModalSizing, compact bool) image.Point {
	if compact {
		return window
	}
	switch sizing {
	case ModalSizingInset:
		return modalInsetCardSize(window, pxPerDp)
	case ModalSizingContent:
		size := modalCenteredCardSize(window, pxPerDp)
		size.X = clampNonNegative(min(scaleDp(modalContentMaxWidthDp, pxPerDp), window.X-2*scaleDp(modalCenteredEdgeDp, pxPerDp)))
		return size
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

// Modal describes one modal window to Kit.Modal. Callers supply the title,
// close button and dismissal handler. The backdrop can use the shared shade
// or an explicit alpha.
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
	// BackdropAlpha sets the shade behind the card. Zero uses the default.
	BackdropAlpha uint8
	// Content fills the card below the header.
	Content layout.Widget
	// Footer, if set for ModalSizingContent, is pinned below the content.
	Footer layout.Widget
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
	windowSize := gtx.Constraints.Max
	cardSize := ModalCardSize(windowSize, gtx.Metric.PxPerDp, modal.Sizing, modal.Compact)
	var cardBounds image.Rectangle

	return layout.Stack{Alignment: layout.Center}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return k.modalBackdrop(gtx, modal, cardBounds)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if cardSize.X == 0 || cardSize.Y == 0 {
				return layout.Dimensions{}
			}
			gtx.Constraints = layout.Exact(cardSize)
			if modal.Sizing == ModalSizingContent && !modal.Compact {
				gtx.Constraints.Min.Y = 0
			}
			dims := k.modalCard(gtx, modal)
			// Stack measures this card before laying out the backdrop.
			cardBounds = ModalCardBounds(windowSize, dims.Size)
			return dims
		}),
	)
}

func (k Kit) modalBackdrop(gtx layout.Context, modal Modal, cardBounds image.Rectangle) layout.Dimensions {
	backdrop := modalBackdropColor()
	if modal.BackdropAlpha != 0 {
		backdrop.A = modal.BackdropAlpha
	}
	Fill(gtx, backdrop)

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
		// Only the area OUTSIDE the card dismisses. In the compact
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
		if modal.Sizing == ModalSizingContent && !modal.Compact {
			return k.layoutFittedModal(gtx, modal)
		}
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
	record := op.Record(gtx.Ops)
	dims := body(gtx)
	content := record.Stop()
	gtx.Constraints = layout.Exact(dims.Size)
	defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(modal.CornerRadius)).Push(gtx.Ops).Pop()
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		Fill(gtx, modalCardFillColor())
		content.Add(gtx.Ops)
		return dims
	})
}

func (k Kit) layoutFittedModal(gtx layout.Context, modal Modal) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Top: modalCardPaddingDp, Left: modalCardPaddingDp, Right: modalCardPaddingDp, Bottom: modalHeaderGapDp}.
				Layout(gtx, func(gtx layout.Context) layout.Dimensions { return k.modalHeader(gtx, modal) })
		}),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			// Give a short body its natural height. A scrolling body still
			// gets only the space left between the header and the footer.
			gtx.Constraints.Min.Y = 0
			return layout.Inset{Left: modalCardPaddingDp, Right: modalCardPaddingDp, Bottom: 12}.Layout(gtx, modal.Content)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			if modal.Footer == nil {
				return layout.Dimensions{}
			}
			return modal.Footer(gtx)
		}),
	)
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
