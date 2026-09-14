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

// console_tabs_test.go covers the tab strip: every tab side by side on a
// desktop window, four plus a "More" menu on a phone.

func TestConsoleTabStripShowsEveryTabOnDesktop(t *testing.T) {
	strip := consoleTabStripFor(len(consoleTabOrder()), consoleTabConsole)

	if got, want := len(strip.Visible), len(consoleTabOrder()); got != want {
		t.Fatalf("desktop strip shows %d tabs, want all %d", got, want)
	}
	if len(strip.Menu) != 0 {
		t.Fatalf("desktop strip folded %v into a menu", strip.Menu)
	}
}

// Settings leads the strip. Below the compact breakpoint the tail folds behind
// a "More" button, and Settings is the one tab a user goes LOOKING for rather
// than stumbles into — folded away, it is a setting nobody suspects exists
// behind a button nobody presses.
func TestSettingsIsTheFirstTab(t *testing.T) {
	order := consoleTabOrder()
	if order[0] != consoleTabSettings {
		t.Fatalf("the strip starts with %v, want Settings", order[0])
	}

	// It stays on the strip at the narrowest width the application supports,
	// which is the whole reason it is first.
	strip := consoleTabStripFor(1, consoleTabConsole)
	if len(strip.Visible) != 1 || strip.Visible[0] != consoleTabSettings {
		t.Fatalf("the last tab standing is %v, want Settings", strip.Visible)
	}
}

// Leading the strip must not make Settings the tab a console OPENS on: the
// console is a console, and the presentation order and the default are
// different questions.
func TestTheConsoleStillOpensOnTheConsoleTab(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	if got := w.consoleModal.currentTab(); got != consoleTabConsole {
		t.Fatalf("a fresh console opens on %v, want the Console tab", got)
	}
}

func TestConsoleTabStripFoldsTheTailOnCompact(t *testing.T) {
	strip := consoleTabStripFor(consoleVisibleTabs, consoleTabConsole)

	want := []consoleTab{consoleTabSettings, consoleTabConsole, consoleTabPeers, consoleTabTraffic}
	if len(strip.Visible) != len(want) {
		t.Fatalf("compact strip shows %v, want %v", strip.Visible, want)
	}
	for i, tab := range want {
		if strip.Visible[i] != tab {
			t.Fatalf("compact strip position %d = %v, want %v", i, strip.Visible[i], tab)
		}
	}

	wantMenu := []consoleTab{consoleTabFile, consoleTabInfo, consoleTabDonate}
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
			strip := consoleTabStripFor(consoleVisibleTabs, tt.active)
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

// On a phone the strip must fit: the full set does not, which is the whole
// reason the menu exists.
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

// And it must fit at EVERY width, not only on a phone.
//
// The fold used to be decided by Window.isCompactLayout — the single-pane PANE
// breakpoint at 600dp — which answers a different question from "do the labels
// fit". Between that breakpoint and the width the labels actually want, the
// strip was drawn unfolded and ran straight off the card. Adding a seventh tab
// widened that band by a whole pill, and nothing measured the desktop case.
func TestConsoleTabStripFitsEveryWidth(t *testing.T) {
	// Every locale: the labels are translated, and the widest set is what
	// decides where the fold has to happen.
	for _, language := range supportedLanguages {
		for _, width := range []int{360, 390, 480, 599, 600, 601, 700, 760, 800, 1000, 1400} {
			var router input.Router
			w := newConsoleModalTestWindow(t)
			w.language = language.Code
			openConsoleForTest(w)

			gtx := layout.Context{
				Ops:         new(op.Ops),
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Constraints{Max: image.Pt(width, 720)},
			}
			dims := w.consoleModal.layoutTabs(gtx)
			router.Frame(gtx.Ops)

			if dims.Size.X > width {
				t.Fatalf("%s at %ddp: the strip is %ddp wide", language.Code, width, dims.Size.X)
			}
		}
	}
}

// A window wide enough for every tab puts the menu away, wherever the fold
// happens to be. Left open it would hang under a "More" slot that is no longer
// drawn.
func TestWideningTheWindowClosesTheTabMenu(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	console := w.consoleModal
	console.tabMenuOpen = true

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(2000, 720)},
	}
	console.layoutTabs(gtx)
	router.Frame(gtx.Ops)

	if console.tabMenuOpen {
		t.Fatal("the More menu stayed open on a window wide enough for every tab")
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

	var folded int
	frame(func(gtx layout.Context) layout.Dimensions {
		strip := console.tabStripFor(gtx)
		folded = len(strip.Menu)
		return console.layoutTabMenu(gtx, strip)
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
	if items != folded {
		t.Fatalf("the open dropdown laid out %d items, want the %d folded tabs", items, folded)
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
		console.handleTabActions(gtx)
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
