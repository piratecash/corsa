package ui

import (
	"image"
	"image/color"
	"testing"

	"gioui.org/io/input"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
)

// The fill is shared with the console tab pill: the design describes the open
// toolbar button as looking exactly like the selected tab.
func TestChipFillMatchesTheTabPalette(t *testing.T) {
	tests := []struct {
		name   string
		active bool
		want   color.NRGBA
	}{
		{name: "idle", want: color.NRGBA{R: 0x22, G: 0x2e, B: 0x3e, A: 255}},
		{name: "active", active: true, want: color.NRGBA{R: 0x39, G: 0x62, B: 0xaa, A: 255}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ChipFill(tt.active); got != tt.want {
				t.Fatalf("ChipFill(%v) = %+v, want %+v", tt.active, got, tt.want)
			}
		})
	}
}

func toolbarFrame(t *testing.T, button *widget.Clickable, opts ToolbarButtonOpts) layout.Dimensions {
	t.Helper()
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 100)},
	}
	dims := testKit(t).ToolbarButton(gtx, button, opts)
	router.Frame(gtx.Ops)
	return dims
}

// The language button used to be pinned at 76dp, which cut "中文" off and left
// the short codes swimming. Toolbar buttons size to their content.
func TestToolbarButtonSizesToItsContent(t *testing.T) {
	kit := testKit(t)
	var short, long widget.Clickable

	shortDims := toolbarFrame(t, &short, ToolbarButtonOpts{
		Label: "EN", Icon: kit.CloseIcon, IconSide: IconTrailing,
	})
	longDims := toolbarFrame(t, &long, ToolbarButtonOpts{
		Label: "Console", Icon: kit.CloseIcon, IconSide: IconLeading,
	})

	if shortDims.Size.X >= longDims.Size.X {
		t.Fatalf("short label is %ddp wide and long one %ddp: the button is not content-sized",
			shortDims.Size.X, longDims.Size.X)
	}
	// 10dp of padding above and below an 18dp icon.
	if want := 38; longDims.Size.Y != want {
		t.Fatalf("toolbar button height = %ddp, want %ddp", longDims.Size.Y, want)
	}
}

// Which side the icon sits on must not change the button's size — only the
// order of its two parts.
func TestToolbarButtonIconSideKeepsTheSize(t *testing.T) {
	kit := testKit(t)
	var leading, trailing widget.Clickable

	leadingDims := toolbarFrame(t, &leading, ToolbarButtonOpts{
		Label: "Console", Icon: kit.CloseIcon, IconSide: IconLeading,
	})
	trailingDims := toolbarFrame(t, &trailing, ToolbarButtonOpts{
		Label: "Console", Icon: kit.CloseIcon, IconSide: IconTrailing,
	})

	if leadingDims.Size != trailingDims.Size {
		t.Fatalf("leading %v vs trailing %v", leadingDims.Size, trailingDims.Size)
	}
}
