package desktop

import (
	"image"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
)

// console_tabs_test.go covers the tab strip: all six tabs side by side on a
// desktop window, four plus a "More" menu on a phone.

func TestConsoleTabStripShowsEveryTabOnDesktop(t *testing.T) {
	strip := consoleTabStripFor(false, consoleTabConsole)

	if got, want := len(strip.Visible), len(consoleTabOrder()); got != want {
		t.Fatalf("desktop strip shows %d tabs, want all %d", got, want)
	}
	if len(strip.Menu) != 0 {
		t.Fatalf("desktop strip folded %v into a menu", strip.Menu)
	}
}

func TestConsoleTabStripFoldsTheTailOnCompact(t *testing.T) {
	strip := consoleTabStripFor(true, consoleTabConsole)

	want := []consoleTab{consoleTabConsole, consoleTabPeers, consoleTabTraffic, consoleTabFile}
	if len(strip.Visible) != len(want) {
		t.Fatalf("compact strip shows %v, want %v", strip.Visible, want)
	}
	for i, tab := range want {
		if strip.Visible[i] != tab {
			t.Fatalf("compact strip position %d = %v, want %v", i, strip.Visible[i], tab)
		}
	}

	wantMenu := []consoleTab{consoleTabInfo, consoleTabDonate}
	if len(strip.Menu) != len(wantMenu) {
		t.Fatalf("compact menu = %v, want %v", strip.Menu, wantMenu)
	}
	for i, tab := range wantMenu {
		if strip.Menu[i] != tab {
			t.Fatalf("compact menu position %d = %v, want %v", i, strip.Menu[i], tab)
		}
	}
}

// The button that opens the menu is labelled "More" — unless the selected tab
// is inside it, in which case it carries that tab's name so the strip still
// says where the user is.
func TestConsoleTabStripNamesTheMoreButtonAfterTheActiveTab(t *testing.T) {
	tests := []struct {
		name      string
		active    consoleTab
		hasActive bool
		want      consoleTab
	}{
		{name: "active tab is on the strip", active: consoleTabPeers},
		{name: "active tab is folded away", active: consoleTabDonate, hasActive: true, want: consoleTabDonate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strip := consoleTabStripFor(true, tt.active)
			if strip.MenuHasActive != tt.hasActive {
				t.Fatalf("MenuHasActive = %v, want %v", strip.MenuHasActive, tt.hasActive)
			}
			if tt.hasActive && strip.MenuActive != tt.want {
				t.Fatalf("MenuActive = %v, want %v", strip.MenuActive, tt.want)
			}
		})
	}
}

// Picking a folded tab must both select it and put the menu away.
func TestConsoleTabMenuClosesWhenAFoldedTabIsPicked(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	console := w.consoleModal
	console.tabMenuOpen = true

	console.selectTab(consoleTabDonate)

	if console.currentTab() != consoleTabDonate {
		t.Fatalf("selected tab = %v, want donate", console.currentTab())
	}
	if console.tabMenuOpen {
		t.Fatal("the More menu stayed open after a tab was picked from it")
	}
}

// On a phone the strip must fit: six tabs did not, which is the whole reason
// the menu exists.
func TestConsoleTabStripFitsTheCompactWidth(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)

	const width = 390
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(width, 720)},
	}
	dims := w.consoleModal.layoutTabs(gtx)
	router.Frame(gtx.Ops)

	if dims.Size.X > width {
		t.Fatalf("compact tab strip is %ddp wide, want no more than %ddp", dims.Size.X, width)
	}
}

// The dropdown hangs under the More button. Its first cut aligned to the right
// edge of the CARD instead, which put it well past the strip and over the tab
// content with nothing connecting the two.
func TestConsoleTabMenuHangsUnderTheMoreButton(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	console := w.consoleModal
	console.tabMenuOpen = true

	const width = 390
	frame := func(w layout.Widget) {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(width, 720)),
		}
		w(gtx)
		router.Frame(gtx.Ops)
	}

	// The strip first, because that is what records the anchor. Then the
	// dropdown in a frame of its own, so every button the semantics report
	// belongs to it rather than to the strip.
	frame(func(gtx layout.Context) layout.Dimensions { return console.layoutTabs(gtx) })
	anchor := console.tabMenuAnchor
	frame(func(gtx layout.Context) layout.Dimensions {
		return console.layoutTabMenu(gtx, consoleTabStripFor(true, console.currentTab()))
	})

	items := 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class != semantic.Button {
			continue
		}
		items++
		// The card is RIGHT-aligned with the slot that opened it, so its rows
		// end where the slot ends, less the card's padding and border.
		if delta := anchor.Max.X - node.Desc.Bounds.Max.X; delta < 0 || delta > 12 {
			t.Fatalf("dropdown item ends at x=%d, the More slot ends at %d (delta %d)",
				node.Desc.Bounds.Max.X, anchor.Max.X, delta)
		}
	}
	if items != len(consoleTabStripFor(true, console.currentTab()).Menu) {
		t.Fatalf("the open dropdown laid out %d items, want the folded tabs", items)
	}
}

// A press outside the open dropdown puts it away instead of reaching the tab
// underneath it.
func TestConsoleTabMenuClosesOnAPressOutside(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	console := w.consoleModal
	console.tabMenuOpen = true

	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(390, 720)),
		}
		console.layoutContent(gtx)
		router.Frame(gtx.Ops)
	}

	frame()
	router.Queue(pointer.Event{
		Source:   pointer.Mouse,
		Kind:     pointer.Press,
		Buttons:  pointer.ButtonPrimary,
		Position: f32.Pt(20, 600),
	})
	frame()

	if console.tabMenuOpen {
		t.Fatal("a press away from the dropdown left it open")
	}
}

// The More button opens the menu; a second click puts it away.
func TestConsoleMoreButtonTogglesTheMenu(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	console := w.consoleModal

	var stripHeight int
	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(390, 720)},
		}
		console.handleTabActions(gtx, true)
		stripHeight = console.layoutTabs(gtx).Size.Y
		router.Frame(gtx.Ops)
	}
	clickMore := func() {
		// layoutTabs records the More slot's rectangle, which beats guessing
		// at a coordinate that moves with the font and the locale.
		at := f32.Pt(float32(console.tabMenuAnchor.Min.X)+4, float32(stripHeight)/2)
		router.Queue(
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
		)
	}

	frame()
	clickMore()
	frame()
	if !console.tabMenuOpen {
		t.Fatal("clicking More did not open the menu")
	}

	clickMore()
	frame()
	if console.tabMenuOpen {
		t.Fatal("clicking More again did not close the menu")
	}
}
