package ui

import (
	"image"
	"slices"
	"testing"

	"gioui.org/io/input"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"golang.org/x/exp/shiny/materialdesign/icons"
)

// testCategoryIDs mirrors the nine the application draws. The component takes
// whatever list it is handed, so the exact identifiers do not matter — the
// COUNT does, because nine full-size chips are what a narrow row cannot hold.
var testCategoryIDs = []string{
	"recent", "smileys", "gestures", "animals", "food",
	"travel", "activities", "symbols", "flags",
}

func testCategories(t *testing.T) []EmojiPickerCategory {
	t.Helper()
	icon, err := widget.NewIcon(icons.SocialMood)
	if err != nil {
		t.Fatalf("category icon: %v", err)
	}
	categories := make([]EmojiPickerCategory, 0, len(testCategoryIDs))
	for _, id := range testCategoryIDs {
		categories = append(categories, EmojiPickerCategory{ID: id, Icon: icon, Hint: id})
	}
	return categories
}

func testPicker(t *testing.T, mode EmojiPickerMode, choices []string) EmojiPicker {
	t.Helper()
	searchIcon, err := widget.NewIcon(icons.ActionSearch)
	if err != nil {
		t.Fatalf("search icon: %v", err)
	}
	return EmojiPicker{
		Mode:       mode,
		Categories: testCategories(t),
		Selected:   testCategoryIDs[1],
		Choices:    choices,
		Labels: EmojiPickerLabels{
			SearchPlaceholder: "Search",
			Empty:             "Nothing",
			Title:             "Pick a reaction",
			CloseHint:         "Close",
			Describe:          func(value string) string { return "Insert " + value },
		},
		SearchIcon: searchIcon,
	}
}

// testGtx is one frame of layout. It carries a Source of its own because a
// zero one reports the frame DISABLED, and everything the panel defers to an
// enabled frame — the category reveal, the grid's refilter — would silently
// never happen.
func testGtx(width, height int, pxPerDp float32) layout.Context {
	return layout.Context{
		Ops:         new(op.Ops),
		Source:      new(input.Router).Source(),
		Metric:      unit.Metric{PxPerDp: pxPerDp, PxPerSp: pxPerDp},
		Constraints: layout.Constraints{Max: image.Pt(width, height)},
	}
}

func TestEmojiGridColumnsUseAvailableWidth(t *testing.T) {
	tests := []struct {
		width int
		want  int
	}{
		// Narrow panels lose COLUMNS, they do not squeeze the cells: four forced
		// columns in a 160dp panel are cells of a little over 20dp, which is
		// neither a tap target nor wide enough for the glyph.
		{width: 100, want: 1},
		{width: 160, want: 3},
		{width: 240, want: 4},
		{width: 360, want: 6},
		{width: 760, want: 14},
		{width: 1200, want: 23},
	}

	for _, tt := range tests {
		if got := EmojiGridColumns(tt.width, 52); got != tt.want {
			t.Errorf("EmojiGridColumns(%d, 52) = %d, want %d", tt.width, got, tt.want)
		}
	}
}

func TestEmojiGridOccupiesFullAvailableWidth(t *testing.T) {
	kit := testKit(t)
	for _, width := range []int{360, 1200} {
		var router input.Router
		gtx := testGtx(width, 240, 1)
		gtx.Source = router.Source()

		state := NewEmojiPickerState(testCategoryIDs[1])
		dims := kit.emojiGrid(gtx, &state, testPicker(t, EmojiPickerModeCompose, []string{"a", "b", "c"}))
		if dims.Size.X != width {
			t.Errorf("emoji grid width = %d, want available width %d", dims.Size.X, width)
		}
	}
}

func TestEmojiChoiceOccupiesAllocatedCellWidth(t *testing.T) {
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(104, 52)),
	}

	state := NewEmojiPickerState(testCategoryIDs[1])
	dims := testKit(t).emojiChoice(gtx, &state, "a", "Insert a")
	if dims.Size.X != gtx.Constraints.Max.X {
		t.Fatalf("emoji cell width = %d, want allocated width %d", dims.Size.X, gtx.Constraints.Max.X)
	}
}

// Nine chips need 243dp. Narrower than that the row scrolls them rather than
// shrinking them: a 15dp chip at 140dp of row, or a 4dp one at 40dp, overlaps
// nothing and can be hit by nobody. Full size at every width is also what keeps
// EmojiPickerChromeHeight exact.
func TestEmojiCategoryRowKeepsChipsFullSizeAtEveryWidth(t *testing.T) {
	kit := testKit(t)
	picker := testPicker(t, EmojiPickerModeCompose, nil)

	for _, pxPerDp := range []float32{1, 1.5, 2} {
		for _, width := range []int{40, 90, 140, 243, 261, 400} {
			gtx := testGtx(width, 200, pxPerDp)
			state := NewEmojiPickerState(testCategoryIDs[1])
			dims := kit.emojiCategories(gtx, &state, picker)
			if chip := emojiCategoryChipPx(gtx); dims.Size.Y != chip {
				t.Fatalf("at %dpx wide (PxPerDp %v) the row is %dpx tall, want a full chip of %dpx",
					width, pxPerDp, dims.Size.Y, chip)
			}
			if dims.Size.X > width {
				t.Fatalf("at %dpx wide (PxPerDp %v) the row reports %dpx and spills out of the panel",
					width, pxPerDp, dims.Size.X)
			}
		}
	}
}

// A panel reopened on a category keeps the selection but not where the row was
// scrolled to, so on a narrow panel the grid showed one category while the row
// highlighted none of the chips it had room for.
func TestNarrowEmojiCategoryRowOpensOnTheSelectedChip(t *testing.T) {
	kit := testKit(t)
	picker := testPicker(t, EmojiPickerModeCompose, nil)

	for _, selected := range testCategoryIDs {
		t.Run(selected, func(t *testing.T) {
			state := NewEmojiPickerState(testCategoryIDs[1])
			state.SelectCategory(selected)

			// Wide enough to spread every chip: the row scrolls nowhere, so the
			// reveal has to survive to a frame that can act on it.
			kit.emojiCategories(testGtx(400, 200, 1), &state, picker)
			state.RevealCategory()

			gtx := testGtx(140, 200, 1)
			kit.emojiCategories(gtx, &state, picker)

			index := slices.Index(testCategoryIDs, selected)
			position := state.Row.Position
			if index < position.First || index >= position.First+position.Count {
				t.Fatalf("%s is chip %d, but the row shows %d..%d", selected, index,
					position.First, position.First+position.Count-1)
			}
			if chip := emojiCategoryChipPx(gtx); position.OffsetLast >= chip {
				t.Fatalf("%s left %dpx empty after the last chip, more than the %dpx chip that could fill it",
					selected, position.OffsetLast, chip)
			}
		})
	}
}

func TestEmojiCategoryRowFirstKeepsTheRowFull(t *testing.T) {
	const count, visible = 9, 7
	tests := []struct{ selected, want int }{
		{selected: 0, want: 0},
		{selected: 1, want: 1},
		{selected: 2, want: 2},
		{selected: 5, want: 2},
		{selected: 8, want: 2},
		{selected: -1, want: 0}, // an unknown category resolves to no index
	}
	for _, tt := range tests {
		if got := EmojiCategoryRowFirst(tt.selected, count, visible); got != tt.want {
			t.Fatalf("first chip for selection %d = %d, want %d", tt.selected, got, tt.want)
		}
	}
	if got := EmojiCategoryRowFirst(8, count, 0); got != 8 {
		t.Fatalf("first chip in a row narrower than one chip = %d, want the selected 8", got)
	}
}

func TestNarrowEmojiCategoryRowScrolls(t *testing.T) {
	kit := testKit(t)
	picker := testPicker(t, EmojiPickerModeCompose, nil)
	state := NewEmojiPickerState(testCategoryIDs[1])

	kit.emojiCategories(testGtx(400, 200, 1), &state, picker)
	if count := state.Row.Position.Count; count != 0 {
		t.Fatalf("a row with room for every chip used the scrolling list (%d items)", count)
	}

	kit.emojiCategories(testGtx(140, 200, 1), &state, picker)
	count := state.Row.Position.Count
	if count == 0 {
		t.Fatal("a row too narrow for nine chips did not scroll them")
	}
	if count >= len(testCategoryIDs) {
		t.Fatalf("the scrolling row laid out %d of %d chips: they cannot all fit 140px at full size",
			count, len(testCategoryIDs))
	}
}

// The design draws seven columns on a phone, and a panel the caller sizes by
// hand has to be told how wide that is — sizing the reaction panel to the 365dp
// pill above it left 347dp of grid.
//
// The count is taken from a REAL layout rather than from the same arithmetic
// the production code uses. Repeating the arithmetic is how the scrollbar's
// gutter went unnoticed: material.List takes 8dp out of the row it lays out,
// which no by-hand sum of border and padding can see. Seven choices come out as
// one row when there are seven columns and two when there are six, and the
// list's own Position is where that shows.
func TestEmojiPickerWidthHoldsTheDesignsColumns(t *testing.T) {
	kit := testKit(t)
	gtx := testGtx(412, 400, 1)
	width := EmojiPickerWidthForColumns(gtx, EmojiPickerGridColumns)

	if want := 412 - 2*8; width > want {
		t.Fatalf("a %d-column panel is %dpx wide and does not fit a 412dp phone with 8dp insets (%dpx)",
			EmojiPickerGridColumns, width, want)
	}

	content := width - 2*gtx.Dp(emojiPickerBorderDp) - 2*gtx.Dp(emojiPickerPaddingDp)
	choices := make([]string, EmojiPickerGridColumns)
	for index := range choices {
		choices[index] = string(rune('a' + index))
	}

	state := NewEmojiPickerState(testCategoryIDs[1])
	grid := gtx
	grid.Constraints.Min.X, grid.Constraints.Max.X = content, content
	kit.emojiGrid(grid, &state, testPicker(t, EmojiPickerModeCompose, choices))

	if rows := state.Grid.Position.Count; rows != 1 {
		t.Fatalf("a %dpx panel wrapped %d choices onto %d rows, so its grid is narrower than %d columns",
			width, len(choices), rows, EmojiPickerGridColumns)
	}
}

// Every cell gets the width the count was computed against. Counting before the
// scrollbar's gutter is reserved and laying out after it hands each cell less
// room than the count assumed — invisible at one width, a lost column at the
// next.
func TestEmojiGridCountsColumnsAgainstTheRoomTheCellsGet(t *testing.T) {
	gtx := testGtx(412, 400, 1)
	width := EmojiPickerWidthForColumns(gtx, EmojiPickerGridColumns)
	content := width - 2*gtx.Dp(emojiPickerBorderDp) - 2*gtx.Dp(emojiPickerPaddingDp)

	cells := content - emojiGridGutterPx(gtx)
	if got := cells / EmojiPickerGridColumns; got < gtx.Dp(EmojiGridCellWidthDp) {
		t.Fatalf("each of %d cells gets %dpx of the %dpx left after the gutter, want the full %dpx",
			EmojiPickerGridColumns, got, cells, gtx.Dp(EmojiGridCellWidthDp))
	}
}

// The category chip is a rounded square, the shape every other small selectable
// control in the application has. material.IconButton — the first cut — paints
// its background as an ellipse, so a selected category read as a blue dot.
func TestEmojiCategoryChipIsASquareNotACircle(t *testing.T) {
	gtx := testGtx(400, 200, 1)
	state := NewEmojiPickerState(testCategoryIDs[1])
	picker := testPicker(t, EmojiPickerModeCompose, nil)

	dims := testKit(t).emojiCategoryChip(gtx, &state, picker.Categories[1], picker.Selected)
	side := emojiCategoryChipPx(gtx)
	if dims.Size != (image.Pt(side, side)) {
		t.Fatalf("chip is %v, want the square %dx%d the height budget reserves", dims.Size, side, side)
	}
	if emojiCategoryChipRadius <= 0 || emojiCategoryChipRadius >= unit.Dp(side)/2 {
		t.Fatalf("chip radius %v is not a rounded square in a %dpx slot", emojiCategoryChipRadius, side)
	}
}

// Left and Right stay on their row. Stepping by one index looks like the same
// thing and is not: at the end of a row it lands on the first cell of the next
// one, which is nowhere near where the user was pointing — and half a screen
// away once the grid scrolls.
func TestGridCursorDoesNotWrapBetweenRows(t *testing.T) {
	const columns = 7
	kit := testKit(t)
	state := NewEmojiPickerState(testCategoryIDs[1])

	choices := make([]string, 3*columns+2) // two full rows, then a partial one
	for index := range choices {
		choices[index] = string(rune('a' + index))
	}
	state.Choices(true, func(string, string) []string { return choices })

	// One layout, to fix the column count the arrows step by.
	gtx := testGtx(EmojiPickerWidthForColumns(testGtx(400, 400, 1), columns), 400, 1)
	kit.emojiGrid(gtx, &state, testPicker(t, EmojiPickerModeCompose, choices))

	tests := []struct {
		name    string
		from    int
		dx, dy  int
		want    int
		wantMov bool
	}{
		{name: "right within a row", from: 0, dx: 1, want: 1, wantMov: true},
		{name: "right off the last column", from: columns - 1, dx: 1, want: columns - 1},
		{name: "left off the first column", from: columns, dx: -1, want: columns},
		{name: "left within a row", from: columns + 1, dx: -1, want: columns, wantMov: true},
		{name: "down a whole row", from: 0, dy: 1, want: columns, wantMov: true},
		{name: "up off the first row", from: 2, dy: -1, want: 2},
		{name: "down off the last cell", from: 3 * columns, dy: 1, want: 3 * columns},
		{name: "right off the end of a partial row", from: 3*columns + 1, dx: 1, want: 3*columns + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state.cursor = tt.from
			if moved := state.MoveCursor(tt.dx, tt.dy); moved != tt.wantMov {
				t.Fatalf("moved = %v, want %v", moved, tt.wantMov)
			}
			if got := state.Cursor(); got != tt.want {
				t.Fatalf("cursor %d + (%d,%d) = %d, want %d", tt.from, tt.dx, tt.dy, got, tt.want)
			}
		})
	}
}

// The reaction panel grows a header the composer's does not have, and the
// height budget has to grow with it — a budget that did not would open the
// panel one row short and clip the last line of emoji.
func TestReactionModeReservesRoomForItsHeader(t *testing.T) {
	gtx := testGtx(360, 1000, 1)
	compose := EmojiPickerChromeHeight(gtx, EmojiPickerModeCompose)
	reaction := EmojiPickerChromeHeight(gtx, EmojiPickerModeReaction)

	want := gtx.Dp(unit.Dp(modalCloseButtonSideDp)) + gtx.Dp(emojiPickerSpacingDp)
	if got := reaction - compose; got != want {
		t.Fatalf("reaction chrome exceeds compose chrome by %dpx, want the %dpx header and its gap", got, want)
	}
	if EmojiPickerMinHeight(gtx, EmojiPickerModeReaction) <= EmojiPickerMinHeight(gtx, EmojiPickerModeCompose) {
		t.Fatal("the reaction panel's minimum height ignores its header")
	}
}

// A choice is answered from the set the PREVIOUS frame drew. A query typed in
// between must not decide which taps count.
func TestEmojiPickerAnswersClicksFromTheDrawnSet(t *testing.T) {
	state := NewEmojiPickerState(testCategoryIDs[1])

	if got := state.Choices(true, func(string, string) []string { return []string{"a", "b"} }); len(got) != 2 {
		t.Fatalf("enabled layout resolved %d choices, want 2", len(got))
	}
	if got := state.Choices(false, func(string, string) []string { return []string{"z"} }); len(got) != 2 || got[0] != "a" {
		t.Fatalf("a disabled measuring pass re-filtered the grid: %#v", got)
	}
}

func TestVerticallyCenteredKeepsNaturalChildHeight(t *testing.T) {
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(200, 34)),
	}
	childMinHeight := -1

	dims := VerticallyCentered(gtx, func(gtx layout.Context) layout.Dimensions {
		childMinHeight = gtx.Constraints.Min.Y
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, 14)}
	})

	if childMinHeight != 0 {
		t.Fatalf("centered child minimum height = %d, want natural height constraint 0", childMinHeight)
	}
	if dims.Size != gtx.Constraints.Max {
		t.Fatalf("centered area size = %v, want full search area %v", dims.Size, gtx.Constraints.Max)
	}
}

// The grid stretches its columns to fill the width it is given, so the number of
// columns is what has to give on a narrow panel. Four forced columns in a 160dp
// panel are cells of a little over 20dp: too small to hit and too small for the
// glyph.
func TestANarrowGridKeepsItsCellsAtTheirTargetWidth(t *testing.T) {
	const target = 52
	for _, width := range []int{60, 100, 160, 240, 400} {
		columns := EmojiGridColumns(width, target)
		if columns < 1 {
			t.Fatalf("a %dpx grid laid out %d columns", width, columns)
		}
		if cell := width / columns; cell < target && columns > 1 {
			t.Fatalf("a %dpx grid split into %d columns of %dpx, under the %dpx a cell needs",
				width, columns, cell, target)
		}
	}
}
