package desktop

import (
	"image"
	"strings"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
)

// The popup component itself is covered in internal/app/desktop/ui. What
// belongs here is how this application drives it: what the rows say, where the
// card hangs, and what the backdrop blocks.

// The rows say which one is current, so the open menu shows where the user is.
func TestLanguageMenuMarksTheCurrentLanguage(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	w.language = "ru"

	items := w.languageMenuItems()
	if len(items) != len(supportedLanguages) {
		t.Fatalf("language menu has %d rows, want %d", len(items), len(supportedLanguages))
	}

	selected := 0
	for i, item := range items {
		if !item.Selected {
			continue
		}
		selected++
		if supportedLanguages[i].Code != "ru" {
			t.Fatalf("row %d (%s) is marked selected, want ru", i, supportedLanguages[i].Code)
		}
	}
	if selected != 1 {
		t.Fatalf("%d rows marked selected, want exactly one", selected)
	}
}

// The design writes the language rows with an em dash: "EN — English".
func TestLanguageMenuLabelsUseAnEmDash(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)

	for _, item := range w.languageMenuItems() {
		if !strings.Contains(item.Label, " — ") {
			t.Fatalf("language row %q does not separate code and name with an em dash", item.Label)
		}
	}
}

// The card hangs just under the button it belongs to. The offset used to be a
// constant 58dp below a 24dp window inset, which stopped matching the header
// the day its padding changed — the menu opened a finger's width below the
// button it belongs to.
func TestLanguageMenuHangsUnderItsButton(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	w.showLanguageMenu = true
	w.headerHeight = 46
	w.languageButtonSize = image.Pt(70, 38)

	const height = 900
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, height)),
	}
	w.layoutLanguageOverlay(gtx)
	router.Frame(gtx.Ops)

	anchor := w.languageMenuAnchor(gtx)
	top, bottom := 0, 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class != semantic.Button {
			continue
		}
		if top == 0 || node.Desc.Bounds.Min.Y < top {
			top = node.Desc.Bounds.Min.Y
		}
		bottom = max(bottom, node.Desc.Bounds.Max.Y)
	}

	// The anchor gap plus the card's border and padding sit between the button
	// and the first row.
	if gap := top - anchor.Max.Y; gap < 0 || gap > 24 {
		t.Fatalf("first row starts %ddp under the button, want it hugging the anchor", gap)
	}
	// Six rows of ~35dp plus the card's chrome come to roughly 260dp. Anything
	// near the bottom of a 900dp window means the card was stretched.
	if bottom > 400 {
		t.Fatalf("last row ends at y=%d in a %ddp window: the card was stretched to the room offered", bottom, height)
	}
}

// A press on the backdrop closes the menu. The language menu had no backdrop
// at all before this: it did not block the application underneath, so a click
// meant to dismiss it landed on a contact row instead.
func TestLanguageMenuBackdropClosesIt(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	w.showLanguageMenu = true

	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.layoutLanguageOverlay(gtx)
		router.Frame(gtx.Ops)
	}

	frame()
	router.Queue(pointer.Event{
		Source:   pointer.Mouse,
		Kind:     pointer.Press,
		Buttons:  pointer.ButtonPrimary,
		Position: f32.Pt(40, 600),
	})
	frame()

	if w.showLanguageMenu {
		t.Fatal("a press on the backdrop left the language menu open")
	}
}

// And the backdrop must swallow that press rather than let it through to the
// application it is covering — even though it draws nothing.
func TestLanguageMenuBackdropBlocksTheApplicationUnderneath(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	w.showLanguageMenu = true

	var underneath widget.Clickable
	reached := false
	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		for underneath.Clicked(gtx) {
			reached = true
		}
		underneath.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: gtx.Constraints.Max}
		})
		w.layoutLanguageOverlay(gtx)
		router.Frame(gtx.Ops)
	}

	frame()
	at := f32.Pt(40, 600)
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)
	frame()

	if reached {
		t.Fatal("the press went through the language menu backdrop")
	}
}
