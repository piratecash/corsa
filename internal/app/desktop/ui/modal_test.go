package ui

import (
	"image"
	"image/color"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

func TestModalCloseButtonPaletteMatchesDesign(t *testing.T) {
	tests := []struct {
		name  string
		state ModalCloseButtonState
		want  ModalCloseButtonPalette
	}{
		{
			name:  "idle",
			state: ModalCloseButtonIdle,
			want: ModalCloseButtonPalette{
				Fill:   color.NRGBA{R: 0x1b, G: 0x27, B: 0x35, A: 255},
				Border: color.NRGBA{R: 0x33, G: 0x44, B: 0x5a, A: 255},
				Icon:   color.NRGBA{R: 0x9d, G: 0xad, B: 0xc2, A: 255},
			},
		},
		{
			name:  "highlighted",
			state: ModalCloseButtonHighlighted,
			want: ModalCloseButtonPalette{
				Fill:   color.NRGBA{R: 0x33, G: 0x45, B: 0x5c, A: 255},
				Border: color.NRGBA{R: 0x4a, G: 0x5f, B: 0x7a, A: 255},
				Icon:   color.NRGBA{R: 0xea, G: 0xf1, B: 0xf8, A: 255},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ModalCloseButtonColors(tt.state); got != tt.want {
				t.Fatalf("ModalCloseButtonColors(%v) = %+v, want %+v", tt.state, got, tt.want)
			}
		})
	}
}

// The pointer is the only thing that highlights. Keyboard focus must not:
// the identity panel focuses its close button on open, and a focus-driven
// highlight would leave that one button permanently in the hover look while
// every other modal's reacted to the mouse.
func TestModalCloseButtonHighlightsOnHoverOnly(t *testing.T) {
	if got := ModalCloseButtonStateFor(false); got != ModalCloseButtonIdle {
		t.Fatalf("resting close button = %v, want idle", got)
	}
	if got := ModalCloseButtonStateFor(true); got != ModalCloseButtonHighlighted {
		t.Fatalf("hovered close button = %v, want highlighted", got)
	}
}

// TestRoundIconButtonDisabledTakesNoInput: the disabled look is not a look,
// it is the absence of a control. A dimmed circle that still reported hovers
// would light up under the pointer and do nothing when pressed.
func TestRoundIconButtonDisabledTakesNoInput(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		router := new(input.Router)
		kit := Kit{Theme: material.NewTheme()}
		button := new(widget.Clickable)

		frame := func() {
			ops := new(op.Ops)
			gtx := layout.Context{
				Ops:         ops,
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(44, 44)),
			}
			kit.RoundIconButton(gtx, button, RoundIconButton{
				Hint:    "close",
				Idle:    ModalCloseButtonColors(ModalCloseButtonIdle),
				Hovered: ModalCloseButtonColors(ModalCloseButtonHighlighted),
				Enabled: enabled,
			})
			router.Frame(ops)
		}

		frame()
		at := f32.Pt(22, 22)
		router.Queue(
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
		)
		ops := new(op.Ops)
		gtx := layout.Context{
			Ops:         ops,
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(44, 44)),
		}
		clicked := button.Clicked(gtx)
		kit.RoundIconButton(gtx, button, RoundIconButton{
			Hint:    "close",
			Idle:    ModalCloseButtonColors(ModalCloseButtonIdle),
			Hovered: ModalCloseButtonColors(ModalCloseButtonHighlighted),
			Enabled: enabled,
		})
		router.Frame(ops)

		if clicked != enabled {
			t.Fatalf("enabled=%v: clicked=%v", enabled, clicked)
		}
	}
}

// TestRoundIconButtonSizesDefaultToTheCloseButton keeps the "same component"
// claim honest: a caller that names no size gets the modal close button's.
func TestRoundIconButtonSizesDefaultToTheCloseButton(t *testing.T) {
	kit := Kit{Theme: material.NewTheme()}
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 2, PxPerSp: 2},
		Constraints: layout.Constraints{Max: image.Pt(500, 500)},
	}
	dims := kit.RoundIconButton(gtx, new(widget.Clickable), RoundIconButton{
		Idle:    ModalCloseButtonColors(ModalCloseButtonIdle),
		Hovered: ModalCloseButtonColors(ModalCloseButtonHighlighted),
		Enabled: true,
	})
	if want := image.Pt(88, 88); dims.Size != want {
		t.Fatalf("default size = %v at 2px/dp, want %v (44dp)", dims.Size, want)
	}
	dims = kit.RoundIconButton(gtx, new(widget.Clickable), RoundIconButton{
		SideDp:  36,
		IconDp:  20,
		Idle:    ModalCloseButtonColors(ModalCloseButtonIdle),
		Hovered: ModalCloseButtonColors(ModalCloseButtonHighlighted),
		Enabled: true,
	})
	if want := image.Pt(72, 72); dims.Size != want {
		t.Fatalf("36dp size = %v at 2px/dp, want %v", dims.Size, want)
	}
}

func TestModalCardSizeCentered(t *testing.T) {
	tests := []struct {
		name   string
		window image.Point
		want   image.Point
	}{
		{name: "desktop", window: image.Pt(1000, 700), want: image.Pt(384, 520)},
		{name: "phone", window: image.Pt(360, 640), want: image.Pt(328, 520)},
		{name: "landscape phone", window: image.Pt(640, 320), want: image.Pt(384, 288)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ModalCardSize(tt.window, 1, ModalSizingCentered, false)
			if got != tt.want {
				t.Fatalf("ModalCardSize(%v, centered) = %v, want %v", tt.window, got, tt.want)
			}
		})
	}
}

// The console and traffic modals do not centre a fixed card: they take the
// whole window less a 6dp margin, so the tab strip and the graph get the
// width they need.
func TestModalCardSizeInsetLeavesTheDesignMargin(t *testing.T) {
	window := image.Pt(1000, 700)
	want := image.Pt(1000-2*ModalCardInsetDp, 700-2*ModalCardInsetDp)
	if got := ModalCardSize(window, 1, ModalSizingInset, false); got != want {
		t.Fatalf("ModalCardSize(%v, inset) = %v, want %v", window, got, want)
	}
}

// A window narrower than twice the margin must not produce a negative card.
func TestModalCardSizeNeverGoesNegative(t *testing.T) {
	for _, sizing := range []ModalSizing{ModalSizingCentered, ModalSizingInset} {
		got := ModalCardSize(image.Pt(4, 4), 1, sizing, false)
		if got.X < 0 || got.Y < 0 {
			t.Fatalf("ModalCardSize(4x4, %v) = %v, want non-negative", sizing, got)
		}
	}
}

func TestModalCardFillsTheCompactWindow(t *testing.T) {
	window := image.Pt(390, 720)
	for _, sizing := range []ModalSizing{ModalSizingCentered, ModalSizingInset} {
		if got := ModalCardSize(window, 1, sizing, true); got != window {
			t.Fatalf("compact ModalCardSize(%v) = %v, want the full client area", sizing, got)
		}
		if got := ModalCardSize(window, 1, sizing, false); got == window {
			t.Fatalf("desktop ModalCardSize(%v) unexpectedly fills the window", sizing)
		}
	}
}

func TestModalCardBoundsCentersTheCard(t *testing.T) {
	got := ModalCardBounds(image.Pt(1000, 700), image.Pt(384, 520))
	want := image.Rect(308, 90, 692, 610)
	if got != want {
		t.Fatalf("modalCardBounds = %v, want %v", got, want)
	}
}

func TestFittedModalMeasuresItsBodyAndReservesItsFooter(t *testing.T) {
	for _, tc := range []struct {
		name       string
		windowY    int
		wantHeight int
	}{
		{"natural height", 700, 191},
		{"short window", 200, 168},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var router input.Router
			var modalTag struct{}
			dismissed := false
			kit := Kit{Theme: material.NewTheme()}
			closeButton := new(widget.Clickable)
			window := image.Pt(500, tc.windowY)
			card := ModalCardBounds(window, image.Pt(410, tc.wantHeight))
			frame := func() {
				gtx := layout.Context{
					Ops:         new(op.Ops),
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Exact(window),
				}
				kit.Modal(gtx, Modal{
					Title: "Contact", Close: closeButton, CloseHint: "Close",
					DismissTag: &modalTag, Dismiss: func() { dismissed = true },
					Sizing: ModalSizingContent, CornerRadius: 16,
					Content: func(gtx layout.Context) layout.Dimensions {
						return layout.Dimensions{Size: gtx.Constraints.Constrain(image.Pt(0, 55))}
					},
					Footer: func(gtx layout.Context) layout.Dimensions {
						if gtx.Constraints.Max.Y < 50 {
							t.Fatal("the header and body consumed the footer's space")
						}
						return layout.Dimensions{Size: gtx.Constraints.Constrain(image.Pt(0, 50))}
					},
				})
				router.Frame(gtx.Ops)
			}
			press := func(y int) {
				router.Queue(
					pointer.Event{Kind: pointer.Press, Source: pointer.Mouse, Buttons: pointer.ButtonPrimary, Position: f32.Pt(250, float32(y))},
					pointer.Event{Kind: pointer.Release, Source: pointer.Mouse, Position: f32.Pt(250, float32(y))},
				)
				frame()
			}
			frame()
			press(card.Max.Y - 3)
			if dismissed {
				t.Fatal("press inside the fitted footer dismissed the modal")
			}
			press(card.Min.Y - 2)
			if !dismissed {
				t.Fatal("the area above the fitted card did not dismiss the modal")
			}
		})
	}
}
