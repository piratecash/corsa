package ui

import (
	"image"
	"image/color"
	"slices"

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

// emoji_picker.go is the emoji panel, drawn twice from one place: under the
// composer, where a choice is inserted into the message being written, and over
// a message, where a choice becomes a reaction. See screen `7h` of
// docs/design/CHANGES-reactions.md.
//
// The panel is the same in both: same frame, same nine category chips, same
// search field, same grid. The mode changes exactly two things — a header with
// a title and a close button appears, and the caller closes the panel after a
// choice instead of leaving it open. Everything else is shared, which is the
// whole reason this is one component and not two files.
//
// The CATALOG is not here. What emoji exist, what they are called in six
// languages and which of them match a query are application data — they carry
// translated keywords and a per-user "recently used" list persisted in
// preferences — so the caller filters and hands the result over as Choices.
// This package draws what it is given.

// EmojiPickerMode selects which of the two panels is drawn.
type EmojiPickerMode uint8

const (
	// EmojiPickerModeCompose is the composer's panel: no header, and the caller
	// leaves it open after a choice.
	EmojiPickerModeCompose EmojiPickerMode = iota
	// EmojiPickerModeReaction is the panel opened from the reaction row: a
	// header with a title and a close button, and the caller closes it on the
	// first choice.
	EmojiPickerModeReaction
)

// The panel's vertical budget, in one place. Every constant is used at the
// single site that draws the thing it measures, so the sums below cannot drift
// away from the surface they describe.
const (
	emojiPickerBorderDp  = unit.Dp(1)  // frame around the whole surface
	emojiPickerRadiusDp  = unit.Dp(12) // corner radius of that frame
	emojiPickerPaddingDp = unit.Dp(8)  // inset between frame and content
	emojiPickerSpacingDp = unit.Dp(8)  // header → categories → search → grid

	emojiCategoryIconDp      = unit.Dp(17)
	emojiCategoryIconInsetDp = unit.Dp(5)
	emojiCategoryChipRadius  = unit.Dp(7)

	// The scrollbar beside the grid: a 4dp indicator with 2dp of track padding
	// either side, which is the 8dp the design reserves for it. material.List
	// takes that width OUT of the row it lays out (its default Occupy
	// strategy), so it has to be budgeted wherever the grid's width is decided.
	emojiScrollbarWidthDp = unit.Dp(4)
	emojiScrollbarPadDp   = unit.Dp(2)

	emojiSearchHeightDp   = unit.Dp(34)
	emojiSearchRadiusDp   = unit.Dp(7)
	emojiSearchIconDp     = unit.Dp(16)
	emojiSearchTextSp     = unit.Sp(13)
	emojiGridCellHeightDp = unit.Dp(38)

	// EmojiGridCellWidthDp is the width one grid cell aims for; the grid fits as
	// many columns as that allows, never fewer than four.
	EmojiGridCellWidthDp = unit.Dp(52)

	// EmojiPickerDesiredHeightDp is how tall the panel opens when whoever hosts
	// it can afford that much.
	EmojiPickerDesiredHeightDp = unit.Dp(250)

	// emojiPickerTitleSp is the reaction header's title. Smaller than a modal's
	// (modalTitleSizeSp) on purpose: this header sits on a 250dp panel, not on a
	// card that fills the window.
	emojiPickerTitleSp = unit.Sp(15)
	// emojiPickerHeaderGapDp separates that title from the close button.
	emojiPickerHeaderGapDp = unit.Dp(8)
	// emojiPickerTitleMinDp is the narrowest the title may be squeezed to before
	// the header stops reading as one. The close button beside it cannot shrink
	// at all, so without this floor the header simply loses its text.
	emojiPickerTitleMinDp = unit.Dp(48)
)

// emojiGlyphSizeSp is the type size one emoji is drawn at in the grid. At 22sp
// the bundled font gives a 27px line box straddling the baseline, which is what
// has to fit inside emojiGridCellHeightDp with room for the hover highlight
// around it. See EmojiGlyph for where those numbers come from.
const emojiGlyphSizeSp = unit.Sp(22)

func emojiPickerSurfaceColor() color.NRGBA {
	return color.NRGBA{R: 0x12, G: 0x19, B: 0x22, A: 255}
}

func emojiPickerBorderColor() color.NRGBA {
	return color.NRGBA{R: 0x2e, G: 0x3a, B: 0x4b, A: 255}
}

func emojiPickerTitleColor() color.NRGBA {
	return color.NRGBA{R: 0xf6, G: 0xf8, B: 0xfb, A: 255}
}

func emojiCategoryIdleColor() color.NRGBA {
	return color.NRGBA{R: 0x96, G: 0xa6, B: 0xbc, A: 255}
}

func emojiCategoryActiveFill() color.NRGBA {
	return color.NRGBA{R: 0x20, G: 0x4c, B: 0x87, A: 255}
}

func emojiCategoryActiveIcon() color.NRGBA {
	return color.NRGBA{R: 0xde, G: 0xee, B: 0xff, A: 255}
}

func emojiSearchFillColor() color.NRGBA {
	return color.NRGBA{R: 0x0d, G: 0x13, B: 0x1b, A: 255}
}

func emojiSearchIconColor() color.NRGBA {
	return color.NRGBA{R: 0x73, G: 0x86, B: 0xa0, A: 255}
}

func emojiSearchTextColor() color.NRGBA {
	return color.NRGBA{R: 0xe7, G: 0xed, B: 0xf5, A: 255}
}

func emojiSearchHintColor() color.NRGBA {
	return color.NRGBA{R: 0x69, G: 0x79, B: 0x8f, A: 255}
}

func emojiGridEmptyColor() color.NRGBA {
	return color.NRGBA{R: 0x7e, G: 0x8f, B: 0xa6, A: 255}
}

func emojiGridHoverFill() color.NRGBA {
	return color.NRGBA{R: 0x24, G: 0x36, B: 0x4c, A: 255}
}

func emojiGlyphColor() color.NRGBA {
	return color.NRGBA{R: 0xf7, G: 0xf9, B: 0xfc, A: 255}
}

func emojiScrollbarColor() color.NRGBA {
	return color.NRGBA{R: 0x9d, G: 0xad, B: 0xc2, A: 128}
}

// emojiCategoryChipPx is how wide and tall a full-size category chip draws,
// summed from its PARTS the way the icon button draws them: at 1.5 px/dp the
// 17dp icon rounds to 26px and each 5dp inset to 8px, one pixel more than
// rounding the 27dp total would claim.
func emojiCategoryChipPx(gtx layout.Context) int {
	return gtx.Dp(emojiCategoryIconDp) + 2*gtx.Dp(emojiCategoryIconInsetDp)
}

// emojiPickerHeaderPx is the reaction header plus the gap under it, and 0 in
// compose mode where there is no header at all.
func emojiPickerHeaderPx(gtx layout.Context, mode EmojiPickerMode) int {
	if mode != EmojiPickerModeReaction {
		return 0
	}
	return gtx.Dp(unit.Dp(modalCloseButtonSideDp)) + gtx.Dp(emojiPickerSpacingDp)
}

// EmojiPickerChromeHeight is everything the panel draws around its grid: the
// frame, the content insets, the mode's header, the category row, the search
// field and the spacers between them. Every term carries its own draw site's
// rounding and they are summed in PIXELS, rather than taking gtx.Dp of a total
// in dp — the two differ by up to a pixel per component at fractional
// densities, and this number is compared against real pixels.
//
// The category row is counted at FULL chip height and always comes out that
// tall: a row too narrow for nine chips scrolls them rather than shrinking them
// (see emojiCategories), so this is the row's real height at every width, not
// an upper bound on it.
func EmojiPickerChromeHeight(gtx layout.Context, mode EmojiPickerMode) int {
	return 2*gtx.Dp(emojiPickerBorderDp) + 2*gtx.Dp(emojiPickerPaddingDp) +
		emojiPickerHeaderPx(gtx, mode) +
		emojiCategoryChipPx(gtx) +
		2*gtx.Dp(emojiPickerSpacingDp) + gtx.Dp(emojiSearchHeightDp)
}

// EmojiPickerMinHeight is the smallest height at which the panel can show a
// single emoji. Below it the panel is not worth drawing: a clipped strip with
// no reachable cell is worse than no panel, and an empty one is worse still —
// it is invisible, yet it is the top Escape/Back target and consumes the key
// that was meant for the surface underneath.
func EmojiPickerMinHeight(gtx layout.Context, mode EmojiPickerMode) int {
	return EmojiPickerChromeHeight(gtx, mode) + gtx.Dp(emojiGridCellHeightDp)
}

// emojiGridGutterPx is what the scrollbar takes out of the grid's width.
func emojiGridGutterPx(gtx layout.Context) int {
	return gtx.Dp(emojiScrollbarWidthDp) + 2*gtx.Dp(emojiScrollbarPadDp)
}

// EmojiPickerMinWidth is the narrowest the panel can be drawn at and still be
// the panel. Below it the caller must not draw it at all.
//
// The floor differs by mode, and the reaction one is much higher, which is the
// whole reason this exists: its header carries a close button laid out at an
// EXACT square (ModalCloseButton), so it cannot shrink with the surface. Given
// less width it simply hangs outside, clipped — a control the user can see and
// cannot press. A panel narrower than its own way out is worse than no panel.
//
// Summed in pixels from the terms the panel draws, never by converting a total
// in dp: the two differ at fractional densities, and this number is compared
// against real pixels. EmojiPickerChromeHeight states the same rule for the
// other axis.
func EmojiPickerMinWidth(gtx layout.Context, mode EmojiPickerMode) int {
	chrome := 2*gtx.Dp(emojiPickerBorderDp) + 2*gtx.Dp(emojiPickerPaddingDp)
	// One grid cell plus the scrollbar's gutter, mirroring EmojiPickerMinHeight:
	// a panel that cannot show one emoji is not worth drawing. It is a floor on
	// the PANEL, and EmojiGridColumns is what keeps it honest: the columns are
	// as many as fit at their target width, down to one, so a narrow panel loses
	// columns rather than shrinking the cells inside them.
	width := chrome + emojiGridGutterPx(gtx) + gtx.Dp(EmojiGridCellWidthDp)
	if mode == EmojiPickerModeReaction {
		width = max(width, chrome+
			gtx.Dp(unit.Dp(modalCloseButtonSideDp))+
			gtx.Dp(emojiPickerHeaderGapDp)+
			gtx.Dp(emojiPickerTitleMinDp))
	}
	return width
}

// EmojiGridColumns is how many cells fit across a grid of the given pixel width,
// at the cells' TARGET width and never fewer than one. The width is what the
// cells get — the scrollbar's gutter has already been taken out of it.
//
// It used to claim four columns whatever the width, and that is the bug this
// wording replaces: the grid stretches its columns to fill the width it is
// given, so four forced columns in a 160dp panel are four cells of a little over
// 20dp — tap targets a finger cannot hit, and glyphs wide enough to be clipped.
// Losing columns is the graceful way to be narrow; shrinking the cells is not.
func EmojiGridColumns(width, targetCellWidth int) int {
	if targetCellWidth <= 0 {
		return 1
	}
	return max(1, width/targetCellWidth)
}

// EmojiPickerGridColumns is the grid the design draws on a phone (screens `7h`
// and `3h`). The number is not enforced — the grid always fits as many columns
// as its width allows — but a panel the CALLER sizes, rather than one stretched
// to a composer, has to be told how wide "wide enough" is.
const EmojiPickerGridColumns = 7

// EmojiPickerWidthForColumns is the panel width whose grid comes out exactly
// this many columns across, chrome included.
//
// It exists because the reaction panel is placed by hand rather than filling a
// row. Sizing it to the reaction pill above it looked tidy and cost a column:
// the pill is 365dp, of which the panel's frame and padding take 18, leaving
// 347 — six cells of 52dp, not the seven the design draws.
//
// The scrollbar's gutter is part of the sum. Leaving it out did not change the
// column COUNT at 382dp, which is why it survived a first reading — it made the
// seven cells share 356px instead of 364 and drew each of them a pixel narrow,
// with the count one rounding step away from dropping to six.
func EmojiPickerWidthForColumns(gtx layout.Context, columns int) int {
	return columns*gtx.Dp(EmojiGridCellWidthDp) + emojiGridGutterPx(gtx) +
		2*gtx.Dp(emojiPickerBorderDp) + 2*gtx.Dp(emojiPickerPaddingDp)
}

// EmojiPickerCategory is one chip in the category row.
type EmojiPickerCategory struct {
	// ID identifies the category to the caller. It is the value handed back by
	// CategoryClicked and compared against EmojiPicker.Selected.
	ID string
	// Icon is the chip's glyph.
	Icon *widget.Icon
	// Hint is what a screen reader announces for the chip.
	Hint string
}

// EmojiPickerLabels is the panel's text, supplied by the caller because this
// package holds no message catalogue.
type EmojiPickerLabels struct {
	// SearchPlaceholder is the search field's hint.
	SearchPlaceholder string
	// Empty is shown in place of the grid when nothing matches.
	Empty string
	// Title heads the panel in reaction mode. Unused in compose mode.
	Title string
	// CloseHint is what a screen reader announces for the header's close
	// button. Unused in compose mode.
	CloseHint string
	// Describe announces one grid cell. Never nil: a grid of unlabelled
	// buttons is unusable with a screen reader, and a default here would hide
	// the omission at the call site.
	Describe func(value string) string
}

// EmojiPicker describes one drawn panel. The state it needs between frames is
// separate (EmojiPickerState) because this value is rebuilt every frame.
type EmojiPicker struct {
	// Mode selects the header and, for the caller, what a choice does.
	Mode EmojiPickerMode
	// Categories are the chips, left to right.
	Categories []EmojiPickerCategory
	// Selected is the highlighted category's ID, or "" for none — which is what
	// a caller passes while a search query is active, since the grid then shows
	// matches from every category and no chip describes it.
	Selected string
	// Choices are the emoji in the grid, already filtered by the caller.
	Choices []string
	// Labels is the panel's text.
	Labels EmojiPickerLabels
	// SearchWrap wraps the search editor in whatever the host platform needs
	// around a text field — on touch, the area that raises the on-screen
	// keyboard. nil draws the editor bare.
	SearchWrap func(gtx layout.Context, editor layout.Widget) layout.Dimensions
	// SearchIcon is the magnifier drawn at the head of the search field.
	SearchIcon *widget.Icon
}

// EmojiPickerState is what the panel remembers between frames: which category
// is selected, the search text, where the two lists are scrolled and the
// widgets that carry the clicks.
//
// The widgets are created on demand and keyed by the caller's own IDs, so the
// state needs no catalogue either. They are never dropped: the catalogue is
// finite and a button dropped between frames loses the click it was about to
// deliver.
type EmojiPickerState struct {
	// Search is the query field. Exported because the caller both reads it (to
	// filter) and focuses it.
	Search widget.Editor
	// Grid scrolls the emoji.
	Grid widget.List
	// Row scrolls the category chips on a panel too narrow to spread nine of
	// them. Plain layout.List, not material's: a scrollbar's gutter would eat
	// into a 27dp row.
	Row layout.List
	// Close is the reaction header's close button.
	Close widget.Clickable

	category        string
	categoryButtons map[string]*widget.Clickable
	emojiButtons    map[string]*widget.Clickable

	// reveal is a request to bring the selected category chip on screen, set
	// wherever the selection changes and consumed by emojiCategories on the
	// next enabled frame. It has to cross into layout because only the row
	// knows how wide it came out, and therefore which first index shows the
	// chip without leaving a gap after the last one. A panel wide enough to
	// spread every chip consumes nothing: there is nothing to reveal, and the
	// request keeps until the window is narrow enough for it to mean something.
	reveal bool

	// surface is the catch-all pointer target that keeps a press on the panel's
	// own padding from reaching the backdrop behind it. See SwallowPresses.
	surface int

	// cursor is the grid cell the keyboard is on, as an index into choices.
	//
	// The grid holds several hundred cells, so it cannot be walked with Tab —
	// a ring that listed them would take a minute to step past. Instead the
	// ring gets ONE stop for the whole grid (CursorTag), and the arrows move
	// within it. That is how a grid is navigated everywhere else, and it is the
	// only way a keyboard reaches an emoji that is not one of the seven quick
	// reactions.
	cursor int
	// columns is what the last layout divided the grid into. The arrows need it
	// to know what "one row down" means, and only the layout knows how wide the
	// panel came out.
	columns int
	// revealTag is a widget the caller's focus ring has just sent focus to, and
	// which the next layout must bring on screen. See RevealTag.
	revealTag event.Tag
	// cursorReveal asks the next layout to scroll the cursor's row into view.
	// Not optional: Gio drops the focus of any tag the frame does not draw, so a
	// cursor that walked off the bottom of the viewport would lose its focus and
	// the ring would pull it back to the search field.
	cursorReveal bool

	// choices is the set this frame's grid was built from, written by Choices
	// during layout and read by Clicked at the top of the NEXT frame. Crossing a
	// frame is the point, not an oversight: the clicks that frame delivers were
	// aimed at the buttons the previous layout drew, so the set those buttons
	// came from is the set that may answer for them. Reading a freshly filtered
	// list instead would let a query typed in between decide which taps count —
	// a tap on an emoji the search has since filtered out would be dropped, and
	// one on the cell that took its place would answer for a character nobody
	// touched.
	choices []string
}

// NewEmojiPickerState returns a panel state showing the given category.
//
// The ZERO value is usable too, and deliberately so: this state is a field of
// the window, every test in the application builds a window by literal, and a
// field somebody has to remember to initialise is a class of nil panics rather
// than a design. What this constructor adds is the opening category — the only
// thing a zero value cannot guess. Everything else is settled where it is
// needed (see emojiCategories, emojiSearch and emojiGrid).
func NewEmojiPickerState(category string) EmojiPickerState {
	return EmojiPickerState{category: category}
}

// Category is the selected category's ID.
func (s *EmojiPickerState) Category() string { return s.category }

// SelectCategory switches the grid to a category, puts its list back at the top
// and asks the chip row to show the chip that is now highlighted. The three
// belong together: a highlighted chip nobody can see reads as nothing being
// selected at all.
func (s *EmojiPickerState) SelectCategory(id string) {
	s.category = id
	s.Grid.Position = layout.Position{}
	s.reveal = true
	s.cursor = 0
}

// RevealCategory asks the chip row to scroll the selected chip into view on the
// next frame that has one to scroll. Reopening a panel on the flags category
// used to show a row that highlighted nothing at all.
func (s *EmojiPickerState) RevealCategory() { s.reveal = true }

// ResetSearch clears the query and the grid's scroll offset, and reports
// whether the query had anything in it.
//
// The two go together on close. A query left behind reopens the panel on one
// cell with no category highlighted — a state that reads as broken, and whose
// only explanation is small text in a field the user is not looking at. The
// grid's offset indexes a result list that no longer exists. The chip ROW's
// offset is deliberately untouched: whoever reopens the panel owns where that
// row sits, and zeroing it here would be a second owner putting the selected
// chip out of sight.
func (s *EmojiPickerState) ResetSearch() bool {
	had := s.Search.Text() != ""
	s.Search.SetText("")
	s.Grid.Position = layout.Position{}
	s.ResetCursor()
	return had
}

// Choices refreshes the grid's contents and hands them to the caller, keeping
// the copy Clicked answers next frame's clicks from (see the field).
//
// filter is called with the selected category and the current query. It is not
// called at all when the source is disabled: a host measuring the panel before
// drawing it for real gets a fixed height either way, so the previous result is
// sufficient for that inert pass and avoids doing the same catalogue search
// twice in one frame.
func (s *EmojiPickerState) Choices(sourceEnabled bool, filter func(category, query string) []string) []string {
	if !sourceEnabled {
		return s.choices
	}
	s.choices = filter(s.category, s.Search.Text())
	// A narrower result cannot leave the cursor pointing past its end: the tag
	// it names is what the focus ring lists, and a ring item that indexes
	// nothing is a ring with a hole in it.
	s.cursor = min(s.cursor, max(0, len(s.choices)-1))
	return s.choices
}

// Cursor is the index of the grid cell the keyboard is on.
func (s *EmojiPickerState) Cursor() int { return s.cursor }

// CursorTag is the widget under that cursor, and nil when the grid is empty.
// It is the grid's single stop in a caller's focus ring.
func (s *EmojiPickerState) CursorTag() event.Tag {
	if s.cursor < 0 || s.cursor >= len(s.choices) {
		return nil
	}
	return s.emojiButton(s.choices[s.cursor])
}

// MoveCursor steps the cursor by dx cells and dy rows, and reports whether it
// moved. It does NOT wrap: false at an edge is what lets the caller decide what
// leaving the grid means — upwards, that is a return to the search field.
//
// The horizontal step is bounded by the ROW, not just by the list. Stepping the
// index by one looks like the same thing and is not: at the end of a row it
// lands on the first cell of the next one, which is nowhere near where the user
// was pointing, and half a screen away as soon as the grid scrolls.
func (s *EmojiPickerState) MoveCursor(dx, dy int) bool {
	if len(s.choices) == 0 {
		return false
	}
	columns := max(1, s.columns)
	if column := s.cursor%columns + dx; column < 0 || column >= columns {
		return false
	}
	next := s.cursor + dx + dy*columns
	if next < 0 || next >= len(s.choices) || next == s.cursor {
		return false
	}
	s.cursor = next
	s.cursorReveal = true
	return true
}

// ResetCursor puts the keyboard back on the first cell and asks for it to be
// scrolled into view.
func (s *EmojiPickerState) ResetCursor() {
	s.cursor = 0
	s.cursorReveal = true
}

// CategoryTags lists the category chips in draw order, for a caller running a
// focus ring over the panel.
//
// Nine chips are few enough to Tab through, unlike the grid, so they go in the
// ring whole. Without them a keyboard user is stuck in whichever category the
// panel opened on: search finds an emoji by name, but nothing reaches animals,
// food or flags by browsing.
//
// A chip is created here if this is the first the state has heard of it, for
// the reason given on ReactionPickerRowState.Tags.
func (s *EmojiPickerState) CategoryTags(categories []EmojiPickerCategory) []event.Tag {
	tags := make([]event.Tag, 0, len(categories))
	for _, category := range categories {
		tags = append(tags, s.categoryButton(category.ID))
	}
	return tags
}

// RevealTag tells the panel that a focus ring has just sent keyboard focus to
// one of its widgets, so that the next layout can bring it on screen. Tags the
// panel does not own are ignored, and nil clears the request.
//
// Whatever the keyboard walks to has to be DRAWN: Gio drops the focus of any tag
// the frame did not draw, and a ring that then finds focus missing pulls it back
// to its first item — every frame, for as long as the surface is open. Both the
// category row and the grid can scroll a target out of sight.
//
// The caller SAYS where it sent focus rather than the panel asking gtx.Focused,
// which is the same contract menuScroll works under. Asking looks equivalent and
// is not quite: Gio applies a FocusCmd immediately only while its queue is not
// deferring, and it starts deferring as soon as some handler has processed a
// matching filter this frame. Reading the answer back would work on a quiet
// frame and silently stop working on a busy one.
func (s *EmojiPickerState) RevealTag(tag event.Tag) {
	s.revealTag = tag
}

// takeRevealTag consumes the request if it names one of the given widgets, and
// reports which.
func (s *EmojiPickerState) takeRevealCategory(categories []EmojiPickerCategory) (int, bool) {
	if s.revealTag == nil {
		return 0, false
	}
	for index, category := range categories {
		if button := s.categoryButtons[category.ID]; button != nil && button == s.revealTag {
			s.revealTag = nil
			return index, true
		}
	}
	return 0, false
}

// ChoiceFocused reports whether the keyboard is on a grid cell rather than on
// the search field or the close button. The arrows belong to the caret unless
// this is true.
func (s *EmojiPickerState) ChoiceFocused(gtx layout.Context) bool {
	tag := s.CursorTag()
	return tag != nil && gtx.Focused(tag)
}

// Clicked reports the emoji chosen since the last call.
//
// Draining is the caller's job — what a choice DOES differs between the two
// modes, and keeping the decision at the call site is what lets one panel serve
// both.
func (s *EmojiPickerState) Clicked(gtx layout.Context) (string, bool) {
	for _, value := range s.choices {
		button := s.emojiButtons[value]
		if button == nil {
			continue
		}
		if button.Clicked(gtx) {
			return value, true
		}
	}
	return "", false
}

// CategoryClicked reports the category chip pressed since the last call.
func (s *EmojiPickerState) CategoryClicked(gtx layout.Context) (string, bool) {
	for id, button := range s.categoryButtons {
		if button.Clicked(gtx) {
			return id, true
		}
	}
	return "", false
}

// CloseClicked reports a press on the reaction header's close button.
func (s *EmojiPickerState) CloseClicked(gtx layout.Context) bool {
	clicked := false
	for s.Close.Clicked(gtx) {
		clicked = true
	}
	return clicked
}

// DropClicks discards every click queued for this frame on the panel's own
// widgets. A key that already dismissed the panel has answered the gesture; the
// tap behind it must not answer it a second time.
func (s *EmojiPickerState) DropClicks(gtx layout.Context) {
	for _, button := range s.emojiButtons {
		for button.Clicked(gtx) {
		}
	}
	for _, button := range s.categoryButtons {
		for button.Clicked(gtx) {
		}
	}
	for s.Close.Clicked(gtx) {
	}
}

func (s *EmojiPickerState) categoryButton(id string) *widget.Clickable {
	button := s.categoryButtons[id]
	if button == nil {
		if s.categoryButtons == nil {
			s.categoryButtons = make(map[string]*widget.Clickable)
		}
		button = new(widget.Clickable)
		s.categoryButtons[id] = button
	}
	return button
}

func (s *EmojiPickerState) emojiButton(value string) *widget.Clickable {
	button := s.emojiButtons[value]
	if button == nil {
		if s.emojiButtons == nil {
			s.emojiButtons = make(map[string]*widget.Clickable)
		}
		button = new(widget.Clickable)
		s.emojiButtons[value] = button
	}
	return button
}

// EmojiPicker draws the panel at the size it is given.
func (k Kit) EmojiPicker(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	macro := op.Record(gtx.Ops)
	dims := k.emojiPickerSurface(gtx, state, picker)
	surface := macro.Stop()

	// Registered under the panel's own widgets, which are added after it and so
	// still win every press aimed at them.
	SwallowPresses(gtx, &state.surface, dims.Size)
	surface.Add(gtx.Ops)
	return dims
}

func (k Kit) emojiPickerSurface(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			paint.FillShape(gtx.Ops, emojiPickerBorderColor(),
				clip.UniformRRect(image.Rectangle{Max: gtx.Constraints.Min}, gtx.Dp(emojiPickerRadiusDp)).Op(gtx.Ops))
			return layout.Dimensions{Size: gtx.Constraints.Min}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(emojiPickerBorderDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				FillRounded(gtx, emojiPickerSurfaceColor(), emojiPickerRadiusDp-emojiPickerBorderDp)
				return layout.UniformInset(emojiPickerPaddingDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return k.emojiPickerBody(gtx, state, picker)
				})
			})
		}),
	)
}

func (k Kit) emojiPickerBody(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	children := make([]layout.FlexChild, 0, 7)
	if picker.Mode == EmojiPickerModeReaction {
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return k.emojiPickerHeader(gtx, state, picker)
			}),
			layout.Rigid(layout.Spacer{Height: emojiPickerSpacingDp}.Layout),
		)
	}
	children = append(children,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.emojiCategories(gtx, state, picker)
		}),
		layout.Rigid(layout.Spacer{Height: emojiPickerSpacingDp}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.emojiSearch(gtx, state, picker)
		}),
		layout.Rigid(layout.Spacer{Height: emojiPickerSpacingDp}.Layout),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return k.emojiGrid(gtx, state, picker)
		}),
	)
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

// emojiPickerHeader is the reaction mode's title row. It reuses the modal close
// button rather than growing a second one: it is the application's one "get me
// out of this surface" affordance, and a panel opened over a message is exactly
// the case where a user needs to find it without hunting.
func (k Kit) emojiPickerHeader(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			label := material.Label(k.Theme, emojiPickerTitleSp, picker.Labels.Title)
			label.Color = emojiPickerTitleColor()
			label.MaxLines = 1
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: emojiPickerHeaderGapDp}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return k.ModalCloseButton(gtx, &state.Close, picker.Labels.CloseHint)
		}),
	)
}

// emojiCategories spreads the chips across the row when they all fit at full
// size, and scrolls them at full size when they do not.
//
// Shrinking them to fit was the other option and it is the worse one: nine
// chips need 243dp, and a panel narrow enough to matter drives the icon down to
// 15dp at 140dp of row, 10dp at 90dp, 4dp at 40dp — no overlap, but nothing
// left to hit either. Nobody can be asked for horizontal room the way the
// keyboard can be asked for vertical room, so the row does what the grid below
// it already does: keeps its cells the size a finger needs and lets the ones
// that do not fit be scrolled to.
//
// Full size at every width is also what keeps EmojiPickerChromeHeight exact — a
// row that shrank was a row shorter than the height budget reserved for it.
func (k Kit) emojiCategories(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	state.Row.Axis = layout.Horizontal
	state.Row.Alignment = layout.Middle
	chip := emojiCategoryChipPx(gtx)
	if len(picker.Categories)*chip > gtx.Constraints.Max.X {
		if state.reveal && gtx.Enabled() {
			// Not in a measuring pass: it would spend the request on a row that
			// is never shown.
			state.reveal = false
			selected := slices.IndexFunc(picker.Categories, func(c EmojiPickerCategory) bool {
				return c.ID == state.category
			})
			state.Row.ScrollTo(EmojiCategoryRowFirst(selected, len(picker.Categories), gtx.Constraints.Max.X/chip))
		}
		// Only scrolled when the chip is actually out of view, so a row somebody
		// is dragging with a finger is not snapped back under them.
		if index, ok := state.takeRevealCategory(picker.Categories); ok {
			if position := state.Row.Position; index < position.First || index >= position.First+position.Count {
				state.Row.ScrollTo(EmojiCategoryRowFirst(index, len(picker.Categories), gtx.Constraints.Max.X/chip))
			}
		}
		return state.Row.Layout(gtx, len(picker.Categories),
			func(gtx layout.Context, index int) layout.Dimensions {
				return k.emojiCategoryChip(gtx, state, picker.Categories[index], picker.Selected)
			})
	}

	children := make([]layout.FlexChild, 0, len(picker.Categories))
	for _, category := range picker.Categories {
		children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return k.emojiCategoryChip(gtx, state, category, picker.Selected)
			})
		}))
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
}

// EmojiCategoryRowFirst is the chip the scrolling row should start on to show
// the selected one. It is the selected chip itself, pulled back far enough that
// the row still ends on the last chip: layout.List renders from First onwards
// and does not backfill, so ScrollTo(8) of nine chips would leave one chip
// beside an empty row.
func EmojiCategoryRowFirst(selected, count, visible int) int {
	return max(0, min(selected, count-visible))
}

// emojiCategoryChip is a rounded SQUARE, not a circle.
//
// It is drawn by hand rather than through material.IconButton, which was the
// first cut: that style paints its background as an ellipse, so the selected
// category read as a blue dot where the design (screen `7h`) asks for a 7dp
// rounded slot — the same shape as every other small selectable control in the
// application.
//
// The idle chip paints no background at all. A fill in the surface colour would
// be invisible anyway, and leaving it out keeps the one painted rectangle in
// this function the one the user is meant to see.
func (k Kit) emojiCategoryChip(gtx layout.Context, state *EmojiPickerState, category EmojiPickerCategory, selected string) layout.Dimensions {
	side := emojiCategoryChipPx(gtx)
	gtx.Constraints = layout.Exact(image.Pt(side, side))

	iconColor := emojiCategoryIdleColor()
	active := selected != "" && category.ID == selected
	if active {
		iconColor = emojiCategoryActiveIcon()
	}

	button := state.categoryButton(category.ID)
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(category.Hint).Add(gtx.Ops)
		switch {
		case active:
			FillRounded(gtx, emojiCategoryActiveFill(), emojiCategoryChipRadius)
		case button.Hovered() || gtx.Focused(button):
			FillRounded(gtx, emojiGridHoverFill(), emojiCategoryChipRadius)
		}
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return Icon(gtx, category.Icon, emojiCategoryIconDp, iconColor)
		})
	})
}

func (k Kit) emojiSearch(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	state.Search.SingleLine = true
	height := gtx.Dp(emojiSearchHeightDp)
	gtx.Constraints.Min.Y = height
	gtx.Constraints.Max.Y = height
	FillRounded(gtx, emojiSearchFillColor(), emojiSearchRadiusDp)
	return layout.Inset{Left: unit.Dp(9), Right: unit.Dp(9)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return Icon(gtx, picker.SearchIcon, emojiSearchIconDp, emojiSearchIconColor())
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(7)}.Layout),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				editor := material.Editor(k.Theme, &state.Search, picker.Labels.SearchPlaceholder)
				editor.Color = emojiSearchTextColor()
				editor.HintColor = emojiSearchHintColor()
				editor.TextSize = emojiSearchTextSp
				field := func(gtx layout.Context) layout.Dimensions {
					return VerticallyCentered(gtx, editor.Layout)
				}
				if picker.SearchWrap == nil {
					return field(gtx)
				}
				return picker.SearchWrap(gtx, field)
			}),
		)
	})
}

func (k Kit) emojiGrid(gtx layout.Context, state *EmojiPickerState, picker EmojiPicker) layout.Dimensions {
	state.Grid.Axis = layout.Vertical
	gtx.Constraints.Min.X = gtx.Constraints.Max.X
	if len(picker.Choices) == 0 {
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(k.Theme, picker.Labels.Empty)
			label.Color = emojiGridEmptyColor()
			return label.Layout(gtx)
		})
	}

	// Counted against the width the CELLS get, not the width the panel has:
	// material.List takes its gutter out of the row before the cells are laid
	// out, so counting first and reserving second hands every cell less room
	// than the count assumed.
	columns := EmojiGridColumns(gtx.Constraints.Max.X-emojiGridGutterPx(gtx), max(1, gtx.Dp(EmojiGridCellWidthDp)))
	rows := (len(picker.Choices) + columns - 1) / columns
	state.columns = columns
	if cursor := state.CursorTag(); cursor != nil && state.revealTag == cursor {
		// The ring Tabbed into the grid; the cursor may be scrolled away.
		state.revealTag = nil
		state.cursorReveal = true
	}
	if state.cursorReveal && gtx.Enabled() {
		// Not in a measuring pass, for the reason given in emojiCategories.
		state.cursorReveal = false
		state.Grid.ScrollTo(min(state.cursor/columns, max(0, rows-1)))
	}
	list := material.List(k.Theme, &state.Grid)
	list.Indicator.MinorWidth = emojiScrollbarWidthDp
	list.Indicator.CornerRadius = unit.Dp(2)
	list.Indicator.Color = emojiScrollbarColor()
	list.Indicator.HoverColor = emojiScrollbarColor()
	list.Track.MinorPadding = emojiScrollbarPadDp
	return list.Layout(gtx, rows, func(gtx layout.Context, row int) layout.Dimensions {
		gtx.Constraints.Min.X = gtx.Constraints.Max.X
		children := make([]layout.FlexChild, 0, columns)
		for column := range columns {
			index := row*columns + column
			if index >= len(picker.Choices) {
				children = append(children, layout.Flexed(1, func(layout.Context) layout.Dimensions { return layout.Dimensions{} }))
				continue
			}
			value := picker.Choices[index]
			children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return k.emojiChoice(gtx, state, value, picker.Labels.Describe(value))
			}))
		}
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
	})
}

func (k Kit) emojiChoice(gtx layout.Context, state *EmojiPickerState, value, description string) layout.Dimensions {
	button := state.emojiButton(value)
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(description).Add(gtx.Ops)
		side := gtx.Dp(emojiGridCellHeightDp)
		gtx.Constraints.Min.X = max(gtx.Constraints.Min.X, min(side, gtx.Constraints.Max.X))
		gtx.Constraints.Min.Y = side
		gtx.Constraints.Max.Y = side
		if button.Hovered() || gtx.Focused(button) {
			FillRounded(gtx, emojiGridHoverFill(), emojiCategoryChipRadius)
		}
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return k.EmojiGlyph(gtx, emojiGlyphSizeSp, value, emojiGlyphColor())
		})
	})
}

// EmojiGlyph draws one emoji in the bundled emoji family and reports its whole
// LINE box, so a centring parent centres the glyph.
//
// Reporting the ascent alone — the line box less its baseline — is the obvious
// thing to do for text and the wrong thing to do here, because an emoji does
// not sit on the baseline the way a letter does. Measured from the bundled
// font at 22sp: the line box is 27px with the baseline 6px off the bottom, and
// the glyph's ink runs from 20.4px ABOVE that baseline to 5.4px BELOW it — 26
// of the 27px, straddling the baseline almost symmetrically. Centring the
// 21px ascent instead therefore centred a box the ink overflows downwards, and
// pushed every glyph ~2.5px below the middle of its cell, measurably off the
// hover highlight drawn around it.
//
// So the line box is the honest answer for this font: it is the ink, to within
// half a pixel at each end. TestEmojiGlyphInkCentresOnItsLineBox re-derives
// those numbers from the shaper rather than trusting this comment.
func (k Kit) EmojiGlyph(gtx layout.Context, size unit.Sp, value string, tint color.NRGBA) layout.Dimensions {
	label := material.Label(k.Theme, size, value)
	label.Font.Typeface = k.EmojiFace
	label.Color = tint
	return label.Layout(gtx)
}
