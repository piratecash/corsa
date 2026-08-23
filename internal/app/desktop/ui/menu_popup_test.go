package ui

import (
	"image"
	"testing"

	"gioui.org/io/input"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
	"golang.org/x/exp/shiny/materialdesign/icons"
)

// testKit builds a Kit with no application behind it. The components take
// their resources as arguments, which is the point of the package: a test
// needs a theme and an icon, not a window.
func testKit(t *testing.T) Kit {
	t.Helper()
	icon, err := widget.NewIcon(icons.NavigationClose)
	if err != nil {
		t.Fatalf("load close icon: %v", err)
	}
	return Kit{Theme: material.NewTheme(), CloseIcon: icon}
}

func testMenuPopup(labels ...string) MenuPopup {
	items := make([]MenuPopupItem, 0, len(labels))
	for _, label := range labels {
		items = append(items, MenuPopupItem{Label: label, Button: new(widget.Clickable)})
	}
	return MenuPopup{Items: items, Scroll: new(widget.List)}
}

// menuPopupRowBounds lays a card out THE WAY ITS CALLERS DO — offered room
// rather than told a size — and returns the rectangle of every row plus the
// card's own size.
func menuPopupRowBounds(t *testing.T, popup MenuPopup, offered image.Point) ([]image.Rectangle, image.Point) {
	t.Helper()
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: offered},
	}
	if popup.Width == MenuPopupWidthGiven {
		// A fixed-width menu's caller pins the width and offers the rest.
		gtx.Constraints.Min.X = offered.X
	}
	dims := testKit(t).MenuPopupCard(gtx, popup)
	router.Frame(gtx.Ops)

	var rows []image.Rectangle
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button {
			rows = append(rows, node.Desc.Bounds)
		}
	}
	if len(rows) == 0 {
		t.Fatalf("card of %v laid out no rows", dims.Size)
	}
	return rows, dims.Size
}

// Each row is a button the keyboard and a screen reader can reach.
func TestMenuPopupCardDrawsOneButtonPerRow(t *testing.T) {
	popup := testMenuPopup("EN — English", "RU — Русский", "ES — Español")
	rows, _ := menuPopupRowBounds(t, popup, image.Pt(MenuPopupLanguageWidthDp, 400))

	if len(rows) != len(popup.Items) {
		t.Fatalf("popup drew %d buttons for %d rows", len(rows), len(popup.Items))
	}
}

// Every row is as wide as the card. Left to themselves they size to their own
// text, which gave "Info" and "Donate" visibly different widths inside one
// content-sized card.
func TestMenuPopupRowsAreAllTheSameWidth(t *testing.T) {
	popup := testMenuPopup("Info", "Donate")
	popup.Width = MenuPopupWidthFit

	const offered = 390
	rows, card := menuPopupRowBounds(t, popup, image.Pt(offered, 400))

	for _, row := range rows[1:] {
		if row.Dx() != rows[0].Dx() {
			t.Fatalf("rows have different widths: %d and %d", rows[0].Dx(), row.Dx())
		}
	}
	if card.X >= offered {
		t.Fatalf("content-sized card took the whole %ddp offered", offered)
	}
}

// The card's padding is the same on both sides. Gio's material.List reserves a
// scrollbar gutter by default, which took its width out of the content and left
// the rows 8dp from the left edge and 8dp plus a bar from the right.
func TestMenuPopupPaddingIsSymmetric(t *testing.T) {
	const width = MenuPopupLanguageWidthDp
	rows, _ := menuPopupRowBounds(t, testMenuPopup("EN — English", "RU — Русский"), image.Pt(width, 400))

	left := rows[0].Min.X
	right := width - rows[0].Max.X
	if left != right {
		t.Fatalf("row insets are %ddp on the left and %ddp on the right", left, right)
	}
}

// The card is as tall as its rows, not as tall as the room it was offered: a
// stretched card ends in a slab of empty background under the last row.
func TestMenuPopupCardHugsItsRows(t *testing.T) {
	const offered = 600
	_, card := menuPopupRowBounds(t, testMenuPopup("EN", "RU", "ES"), image.Pt(MenuPopupLanguageWidthDp, offered))

	if card.Y >= offered {
		t.Fatalf("card took the whole %ddp it was offered instead of hugging its rows", offered)
	}
}

// Filled paints exactly its widget's size. Reading Constraints.Max instead —
// which is what a bare FillRounded does — ran the menu's fill down the window
// as an opaque column, and squared off every pill's corners, because the
// rounded rect's corners ended up outside the clip.
func TestFilledPaintsTheContentSizeOnly(t *testing.T) {
	content := image.Pt(80, 40)
	offered := image.Pt(400, 600)

	var painted layout.Constraints
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: offered},
	}
	dims := layout.Background{}.Layout(gtx,
		func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Max = gtx.Constraints.Min
			painted = gtx.Constraints
			return layout.Dimensions{Size: gtx.Constraints.Min}
		},
		func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: content}
		},
	)

	if painted.Max != content {
		t.Fatalf("fill covered %v, want the content size %v", painted.Max, content)
	}
	if dims.Size != content {
		t.Fatalf("Filled reported %v, want %v", dims.Size, content)
	}
}

// MenuPopupAnchorX keeps a card inside the area it is drawn in. Both menus
// anchor to the RIGHT edge of their button and both buttons sit at the right
// end of their row, so the card hangs off the edge exactly when it is wider
// than the button — the normal case.
func TestMenuPopupAnchorXKeepsTheCardInside(t *testing.T) {
	tests := []struct {
		name                   string
		want, width, available int
		expected               int
	}{
		{name: "fits", want: 100, width: 80, available: 390, expected: 100},
		{name: "overflows right", want: 340, width: 80, available: 390, expected: 310},
		{name: "wider than the area", want: 40, width: 500, available: 390, expected: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MenuPopupAnchorX(tt.want, tt.width, tt.available); got != tt.expected {
				t.Fatalf("MenuPopupAnchorX(%d, %d, %d) = %d, want %d",
					tt.want, tt.width, tt.available, got, tt.expected)
			}
		})
	}
}
