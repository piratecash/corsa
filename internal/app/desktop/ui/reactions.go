package ui

import (
	"image"
	"image/color"
	"strconv"

	"gioui.org/io/event"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

// reactions.go is the two surfaces a reaction is seen on: the pill of quick
// choices that opens with a message's context menu (screens `3e` and `3f`), and
// the row of chips under a message that already has some (screen `3g`). See
// docs/design/CHANGES-reactions.md.
//
// Neither knows what a reaction IS. They are handed emoji and counts and hand
// back which one was pressed; whether that becomes a frame on the wire, a row
// in the database or nothing at all is decided by the caller — which is why the
// layout could be built before the protocol behind it exists.

const (
	// reactionPickerRadiusDp rounds the pill. Half its height, so the ends are
	// semicircles.
	reactionPickerRadiusDp = unit.Dp(22)
	// reactionPickerPaddingDp is the air between the pill's edge and its slots.
	reactionPickerPaddingDp = unit.Dp(5)
	// reactionPickerGapDp separates two slots.
	reactionPickerGapDp = unit.Dp(5)
	// reactionSlotRingDp is the outline drawn around the slot the pointer or the
	// keyboard is on. Inset by its own width so the stroke stays inside the
	// slot rather than overlapping its neighbour's gap.
	reactionSlotRingDp = unit.Dp(2)
	// ReactionSlotSideDp is the diameter of one slot. 40dp is the smallest
	// comfortable touch target, and this row is aimed at a thumb.
	ReactionSlotSideDp = unit.Dp(40)
	// reactionSlotGlyphSp is the emoji size inside a slot.
	reactionSlotGlyphSp = unit.Sp(22)
	// reactionMoreIconDp is the "more" chevron inside its slot.
	reactionMoreIconDp = unit.Dp(20)

	// reactionChipRadiusDp rounds one chip under a message.
	reactionChipRadiusDp = unit.Dp(12)
	// reactionChipPadXDp and reactionChipPadYDp are the chip's inner padding.
	reactionChipPadXDp = unit.Dp(8)
	reactionChipPadYDp = unit.Dp(3)
	// reactionChipGapDp separates the emoji from its counter inside a chip.
	reactionChipGapDp = unit.Dp(5)
	// reactionChipRowGapDp separates two chips, across and down.
	reactionChipRowGapDp = unit.Dp(4)
	// reactionChipGlyphSp and reactionChipCountSp are the chip's two type sizes.
	reactionChipGlyphSp = unit.Sp(14)
	reactionChipCountSp = unit.Sp(12)
)

func reactionPickerFill() color.NRGBA {
	return color.NRGBA{R: 0x1c, G: 0x22, B: 0x2c, A: 255}
}

func reactionPickerBorder() color.NRGBA {
	return color.NRGBA{R: 0x48, G: 0x55, B: 0x6a, A: 255}
}

// reactionSelectedFill is the blue behind a slot or chip the local user has
// already picked. It is the same value in both places on purpose: one reaction
// looks the same wherever it is shown.
func reactionSelectedFill() color.NRGBA {
	return color.NRGBA{R: 0x24, G: 0x43, B: 0x7e, A: 255}
}

func reactionSelectedBorder() color.NRGBA {
	return color.NRGBA{R: 0x4a, G: 0x6d, B: 0xb0, A: 255}
}

func reactionMoreFill() color.NRGBA {
	return color.NRGBA{R: 0x22, G: 0x2e, B: 0x3e, A: 255}
}

func reactionMoreIconColor() color.NRGBA {
	return color.NRGBA{R: 0x9d, G: 0xad, B: 0xc2, A: 255}
}

func reactionChipFill() color.NRGBA {
	return color.NRGBA{R: 0x22, G: 0x2e, B: 0x3e, A: 255}
}

func reactionChipText() color.NRGBA {
	return color.NRGBA{R: 0xc4, G: 0xcd, B: 0xda, A: 255}
}

func reactionChipMineText() color.NRGBA {
	return color.NRGBA{R: 0xff, G: 0xff, B: 0xff, A: 255}
}

func reactionGlyphColor() color.NRGBA {
	return color.NRGBA{R: 0xf7, G: 0xf9, B: 0xfc, A: 255}
}

// ReactionPickerRowState carries the pill's clicks between frames. The slot
// buttons are keyed by emoji and created on demand, so the state needs no list
// of them up front, survives the default set changing, and is usable from its
// ZERO value — see NewEmojiPickerState for why that matters here.
type ReactionPickerRowState struct {
	// More is the trailing slot that opens the full emoji panel.
	More widget.Clickable
	// surface is the catch-all pointer target that keeps a press on the pill's
	// own padding from reaching the backdrop behind it. See ui.SwallowPresses.
	surface int
	buttons map[string]*widget.Clickable
}

func (s *ReactionPickerRowState) button(value string) *widget.Clickable {
	button := s.buttons[value]
	if button == nil {
		if s.buttons == nil {
			s.buttons = make(map[string]*widget.Clickable)
		}
		button = new(widget.Clickable)
		s.buttons[value] = button
	}
	return button
}

// Clicked reports the quick reaction pressed since the last call. Draining is
// the caller's, for the reason given on EmojiPickerState.Clicked.
func (s *ReactionPickerRowState) Clicked(gtx layout.Context, emojis []string) (string, bool) {
	for _, value := range emojis {
		button := s.buttons[value]
		if button == nil {
			continue
		}
		if button.Clicked(gtx) {
			return value, true
		}
	}
	return "", false
}

// Tags lists the pill's focusable widgets in the order they are drawn, for a
// caller running a focus ring over the whole surface.
//
// A slot is created here if this is the first the state has heard of it. That
// is deliberate: the ring is built BEFORE the pill is laid out, so on the frame
// a menu opens no slot exists yet, and returning a short list would leave the
// row unreachable by keyboard until the second frame.
func (s *ReactionPickerRowState) Tags(emojis []string) []event.Tag {
	tags := make([]event.Tag, 0, len(emojis)+1)
	for _, value := range emojis {
		tags = append(tags, s.button(value))
	}
	return append(tags, &s.More)
}

// MoreClicked reports a press on the trailing "more" slot.
func (s *ReactionPickerRowState) MoreClicked(gtx layout.Context) bool {
	clicked := false
	for s.More.Clicked(gtx) {
		clicked = true
	}
	return clicked
}

// DropClicks discards every click queued for this frame on the pill. A key that
// already dismissed the row has answered the gesture; the tap behind it must
// not answer it a second time.
func (s *ReactionPickerRowState) DropClicks(gtx layout.Context) {
	for _, button := range s.buttons {
		for button.Clicked(gtx) {
		}
	}
	for s.More.Clicked(gtx) {
	}
}

// ReactionPickerRow describes one drawn pill.
type ReactionPickerRow struct {
	// Emojis are the quick choices, left to right.
	Emojis []string
	// Selected answers, for one quick choice, whether the local user already
	// holds it. Never nil — see the constructor comment on Describe.
	//
	// A predicate and not a single value, because a user holds as many
	// reactions as they like: marking only one of five leaves the other four
	// looking un-chosen, which is worse than marking none.
	Selected func(value string) bool
	// MoreIcon is the chevron in the trailing slot.
	MoreIcon *widget.Icon
	// MoreHint is what a screen reader announces for that slot.
	MoreHint string
	// Describe announces one quick-choice slot, and is told whether the user
	// already holds it: a tap on a held reaction CLEARS it, so one wording for
	// both would tell a screen reader the opposite of what the button does.
	// Never nil, for the reason given on EmojiPickerLabels.Describe.
	Describe func(value string, selected bool) string
}

// ReactionPickerRowSize is how big the pill comes out for a given number of
// quick choices, in pixels. The trailing "more" slot is counted; the caller
// passes only the choices.
//
// The caller needs this BEFORE the pill is drawn, because the pill is placed
// with the same anchor rules as the menu under it and an anchor needs a size.
// Measuring by laying the pill out is not an option: its slots are Clickables,
// and laying one out drains the click queue it was about to answer.
func ReactionPickerRowSize(gtx layout.Context, choices int) image.Point {
	slots := choices + 1
	side := gtx.Dp(ReactionSlotSideDp)
	width := slots*side + max(0, slots-1)*gtx.Dp(reactionPickerGapDp) + 2*gtx.Dp(reactionPickerPaddingDp)
	return image.Pt(width, side+2*gtx.Dp(reactionPickerPaddingDp))
}

// ReactionPickerRowCapacity is how many quick choices a pill of the given width
// can hold, once the trailing "more" slot has had its share.
//
// The pill is drawn at its OWN size and placed by an anchor, so nothing clips it
// to the window: a seven-slot pill on a 320dp phone had its "more" button
// entirely past the right edge, and the focus ring went on offering it. Rather
// than dropping the whole row on a narrow screen, the caller drops slots — the
// "more" button is what reaches every other emoji, so it is the one that must
// survive.
//
// Zero means not even one quick choice fits beside that button, and the caller
// should draw no pill at all: a lone round button is not the surface the design
// describes, and a window that narrow has no room for the panel behind it
// either.
func ReactionPickerRowCapacity(gtx layout.Context, available int) int {
	side, gap := gtx.Dp(ReactionSlotSideDp), gtx.Dp(reactionPickerGapDp)
	room := available - side - 2*gtx.Dp(reactionPickerPaddingDp)
	if room < 0 {
		return 0
	}
	return room / (side + gap)
}

// ReactionPickerRow draws the pill at its own size, ignoring any minimum the
// caller's constraints carry.
func (k Kit) ReactionPickerRow(gtx layout.Context, state *ReactionPickerRowState, row ReactionPickerRow) layout.Dimensions {
	size := ReactionPickerRowSize(gtx, len(row.Emojis))
	macro := op.Record(gtx.Ops)
	dims := k.reactionPickerPill(gtx, state, row, size)
	pill := macro.Stop()

	// Registered under the pill's own slots, which are added after it and so
	// still win every press aimed at them.
	SwallowPresses(gtx, &state.surface, dims.Size)
	pill.Add(gtx.Ops)
	return dims
}

func (k Kit) reactionPickerPill(gtx layout.Context, state *ReactionPickerRowState, row ReactionPickerRow, size image.Point) layout.Dimensions {
	gtx.Constraints = layout.Exact(size)

	// The design asks for a drop shadow under the pill and it is not drawn,
	// for the reason the modal card and the popup already have none: Gio has no
	// blur, and stacking translucent rectangles to fake one paints over the
	// backdrop and reads worse than no shadow at all. The 1dp border is what
	// lifts the pill off the chat behind it.
	bounds := image.Rectangle{Max: size}
	radius := gtx.Dp(reactionPickerRadiusDp)
	border := gtx.Dp(unit.Dp(1))
	paint.FillShape(gtx.Ops, reactionPickerBorder(), clip.UniformRRect(bounds, radius).Op(gtx.Ops))
	paint.FillShape(gtx.Ops, reactionPickerFill(),
		clip.UniformRRect(image.Rect(border, border, size.X-border, size.Y-border), max(0, radius-border)).Op(gtx.Ops))

	return layout.UniformInset(reactionPickerPaddingDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := make([]layout.FlexChild, 0, 2*len(row.Emojis)+1)
		for index, value := range row.Emojis {
			if index > 0 {
				children = append(children, layout.Rigid(layout.Spacer{Width: reactionPickerGapDp}.Layout))
			}
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				held := row.Selected(value)
				return k.reactionSlot(gtx, state.button(value), held, row.Describe(value, held), func(gtx layout.Context) layout.Dimensions {
					return k.EmojiGlyph(gtx, reactionSlotGlyphSp, value, reactionGlyphColor())
				})
			}))
		}
		if len(row.Emojis) > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Width: reactionPickerGapDp}.Layout))
		}
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.reactionMoreSlot(gtx, state, row)
		}))
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
	})
}

func (k Kit) reactionSlot(gtx layout.Context, button *widget.Clickable, selected bool, hint string, content layout.Widget) layout.Dimensions {
	side := gtx.Dp(ReactionSlotSideDp)
	gtx.Constraints = layout.Exact(image.Pt(side, side))
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(hint).Add(gtx.Ops)
		// Announced as well as drawn. The fill is what a sighted user reads
		// "you already reacted with this" from; without this a screen reader
		// has no way to learn it at all.
		semantic.SelectedOp(selected).Add(gtx.Ops)
		// Chosen and merely pointed-at are drawn with DIFFERENT SHAPES, not one
		// shape at two weights.
		//
		// They used to share a filled circle, differing only in alpha, and the
		// result read as two chosen slots: the keyboard lands on the first slot
		// when the menu opens, so a user holding one reaction saw two circles
		// and no way to tell which meant what. A fill says "you reacted with
		// this"; a ring says "this is where the pointer or the keyboard is".
		bounds := image.Rectangle{Max: image.Pt(side, side)}
		if selected {
			paint.FillShape(gtx.Ops, reactionSelectedFill(),
				clip.Ellipse(bounds).Op(gtx.Ops))
		}
		if button.Hovered() || gtx.Focused(button) {
			ring := clip.Stroke{
				Path:  clip.Ellipse(bounds.Inset(gtx.Dp(reactionSlotRingDp))).Path(gtx.Ops),
				Width: float32(gtx.Dp(reactionSlotRingDp)),
			}.Op()
			paint.FillShape(gtx.Ops, reactionSelectedBorder(), ring)
		}
		return layout.Center.Layout(gtx, content)
	})
}

func (k Kit) reactionMoreSlot(gtx layout.Context, state *ReactionPickerRowState, row ReactionPickerRow) layout.Dimensions {
	side := gtx.Dp(ReactionSlotSideDp)
	gtx.Constraints = layout.Exact(image.Pt(side, side))
	return state.More.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(row.MoreHint).Add(gtx.Ops)
		fill := reactionMoreFill()
		if state.More.Hovered() || gtx.Focused(&state.More) {
			fill = reactionSelectedFill()
		}
		paint.FillShape(gtx.Ops, fill, clip.Ellipse(image.Rectangle{Max: image.Pt(side, side)}).Op(gtx.Ops))
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Icon(gtx, row.MoreIcon, reactionMoreIconDp, reactionMoreIconColor())
		})
	})
}

// Reaction is one emoji under a message and how many peers picked it.
type Reaction struct {
	// Emoji is the character shown on the chip.
	Emoji string
	// Count is how many peers picked it. Always drawn, including 1: a chip that
	// showed its count only from two upwards would change width the moment a
	// second peer joined, and the row would reflow under the reader's eyes.
	Count int
	// Mine says the local user is one of them, which fills the chip instead of
	// leaving it in the neutral grey.
	Mine bool
}

// ReactionChipsState carries the chips' clicks between frames. Zero value
// usable, as ReactionPickerRowState.
type ReactionChipsState struct {
	buttons map[string]*widget.Clickable
	// Describe says what a press on one chip will do, for a screen reader. It
	// lives on the state rather than in the call because the caller creates one
	// of these per message and can set it once; a nil one draws the chips with
	// their held state announced but unexplained.
	Describe func(Reaction) string
}

func (s *ReactionChipsState) button(value string) *widget.Clickable {
	button := s.buttons[value]
	if button == nil {
		if s.buttons == nil {
			s.buttons = make(map[string]*widget.Clickable)
		}
		button = new(widget.Clickable)
		s.buttons[value] = button
	}
	return button
}

// Clicked reports the chip pressed since the last call. reactions is the set
// that was DRAWN, so a chip that has since left the message cannot answer for a
// tap aimed at whatever took its place.
func (s *ReactionChipsState) Clicked(gtx layout.Context, reactions []Reaction) (string, bool) {
	for _, reaction := range reactions {
		button := s.buttons[reaction.Emoji]
		if button == nil {
			continue
		}
		if button.Clicked(gtx) {
			return reaction.Emoji, true
		}
	}
	return "", false
}

// ReactionChips draws the chips under a message, wrapping onto as many rows as
// the width allows. An empty set draws nothing at all — not an empty row of
// zero height, which would still cost the bubble the spacer above it.
func (k Kit) ReactionChips(gtx layout.Context, state *ReactionChipsState, reactions []Reaction) layout.Dimensions {
	if len(reactions) == 0 {
		return layout.Dimensions{}
	}

	widths := make([]int, len(reactions))
	for index, reaction := range reactions {
		widths[index] = k.reactionChipWidth(gtx, reaction)
	}
	rows := packReactionChips(widths, gtx.Constraints.Max.X, gtx.Dp(reactionChipRowGapDp))

	children := make([]layout.FlexChild, 0, 2*len(rows))
	for rowIndex, row := range rows {
		if rowIndex > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Height: reactionChipRowGapDp}.Layout))
		}
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.reactionChipRow(gtx, state, reactions[row.First:row.First+row.Count])
		}))
	}
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

func (k Kit) reactionChipRow(gtx layout.Context, state *ReactionChipsState, reactions []Reaction) layout.Dimensions {
	children := make([]layout.FlexChild, 0, 2*len(reactions))
	for index, reaction := range reactions {
		if index > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Width: reactionChipRowGapDp}.Layout))
		}
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.reactionChip(gtx, state.button(reaction.Emoji), reaction, state.Describe)
		}))
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
}

// ReactionChipRow is one wrapped line of chips: where it starts in the caller's
// slice and how many chips it holds.
type ReactionChipRow struct {
	First int
	Count int
}

// packReactionChips wraps chip widths onto lines no wider than available. A
// chip too wide for a line of its own still gets one — dropping it would lose a
// reaction, and a clipped chip at least says one is there.
func packReactionChips(widths []int, available, gap int) []ReactionChipRow {
	rows := make([]ReactionChipRow, 0, len(widths))
	current := ReactionChipRow{}
	used := 0
	for index, width := range widths {
		next := width
		if current.Count > 0 {
			next += gap
		}
		if current.Count > 0 && used+next > available {
			rows = append(rows, current)
			current = ReactionChipRow{First: index}
			used = 0
			next = width
		}
		current.Count++
		used += next
	}
	if current.Count > 0 {
		rows = append(rows, current)
	}
	return rows
}

// reactionChipWidth measures one chip without laying it out.
//
// Only the two LABELS are measured, never the chip. A chip is a
// widget.Clickable, and laying one out drains its click queue — measuring chips
// would swallow every second tap on the row. See MenuPopupFitWidth, which is
// the same trap.
func (k Kit) reactionChipWidth(gtx layout.Context, reaction Reaction) int {
	measure := gtx
	measure.Ops = new(op.Ops)
	measure.Constraints.Min = image.Point{}

	glyph := k.EmojiGlyph(measure, reactionChipGlyphSp, reaction.Emoji, reactionGlyphColor()).Size.X
	count := material.Label(k.Theme, reactionChipCountSp, reactionCountText(reaction.Count))
	count.MaxLines = 1
	return glyph + gtx.Dp(reactionChipGapDp) + count.Layout(measure).Size.X + 2*gtx.Dp(reactionChipPadXDp)
}

func (k Kit) reactionChip(
	gtx layout.Context,
	button *widget.Clickable,
	reaction Reaction,
	describe func(Reaction) string,
) layout.Dimensions {
	fill, text := reactionChipFill(), reactionChipText()
	if reaction.Mine {
		fill, text = reactionSelectedFill(), reactionChipMineText()
	}
	body := func(gtx layout.Context) layout.Dimensions {
		// The chip says two things visually — which reaction it is, and whether
		// this user is one of the people who made it — and a screen reader gets
		// neither from a colour and a border. Held is announced as SELECTED, and
		// the description says what a press will DO, because on a chip the user
		// already holds a press takes it back.
		semantic.SelectedOp(reaction.Mine).Add(gtx.Ops)
		if describe != nil {
			if hint := describe(reaction); hint != "" {
				semantic.DescriptionOp(hint).Add(gtx.Ops)
			}
		}
		return layout.Inset{
			Top: reactionChipPadYDp, Bottom: reactionChipPadYDp,
			Left: reactionChipPadXDp, Right: reactionChipPadXDp,
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return k.EmojiGlyph(gtx, reactionChipGlyphSp, reaction.Emoji, reactionGlyphColor())
				}),
				layout.Rigid(layout.Spacer{Width: reactionChipGapDp}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(k.Theme, reactionChipCountSp, reactionCountText(reaction.Count))
					label.Color = text
					label.MaxLines = 1
					return label.Layout(gtx)
				}),
			)
		})
	}
	if !reaction.Mine {
		return k.Chip(gtx, button, fill, reactionChipRadiusDp, body)
	}
	// The local user's own chip is the only one with a border, so it is the
	// only one that goes through widget.Border. Adding the border to every chip
	// and painting it transparent would shift the neutral ones by a pixel.
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		border := widget.Border{Color: reactionSelectedBorder(), CornerRadius: reactionChipRadiusDp, Width: unit.Dp(1)}
		return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Filled(gtx, fill, reactionChipRadiusDp, body)
		})
	})
}

// reactionCountText renders a chip's counter. A reaction that exists was picked
// by at least one peer, so a count below one is a caller's bookkeeping error
// rather than a state to render: it is floored instead of drawn as "0", which
// would read as a reaction nobody made.
func reactionCountText(count int) string {
	return strconv.Itoa(max(1, count))
}
