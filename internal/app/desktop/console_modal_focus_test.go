package desktop

import (
	"image"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget/material"
)

// console_modal_focus_test.go covers what the console modal does to the
// keyboard and to the Back key — the two things a modal owes the surface it
// covers.

// Opening the console must move focus into it. Without this the caret stays in
// the message composer hidden underneath, so typing goes to a contact and
// Enter SENDS that text instead of running a command.
func TestConsoleModalTakesKeyboardFocus(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)

	focusComposer := false
	frame := func() layout.Context {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		if focusComposer {
			gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
			focusComposer = false
		}
		// The composer underneath is a focus target on every frame — an editor
		// only becomes one by being laid out.
		material.Editor(w.theme, &w.messageEditor, "").Layout(gtx)
		if w.consoleModalVisible() {
			w.layoutConsoleOverlay(gtx)
		}
		router.Frame(gtx.Ops)
		return gtx
	}

	focusComposer = true
	frame()
	gtx := frame()
	if !gtx.Focused(&w.messageEditor) {
		t.Fatal("test setup: the composer never took focus")
	}

	openConsoleForTest(w)
	frame()
	gtx = frame()

	if gtx.Focused(&w.messageEditor) {
		t.Fatal("the composer under the console modal still holds keyboard focus")
	}
	if !gtx.Focused(&w.consoleModal.consoleEditor) {
		t.Fatal("the console command line did not take focus when the modal opened")
	}
}

// The selected tab survives a close, so the console can reopen on Peers or
// Donate — where the command line is not laid out at all. Focusing it there
// hands the keyboard to a widget that is not in the frame, and Gio drops it:
// the user is left with no focus anywhere.
func TestConsoleFocusTargetExistsOnEveryTab(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	console := w.consoleModal

	for _, tab := range consoleTabOrder() {
		console.selectTab(tab)
		target := console.focusTarget()

		if tab == consoleTabConsole {
			if target != &console.consoleEditor {
				t.Fatal("the Console tab should focus its command line")
			}
			continue
		}
		// Every other tab has no command line in the frame at all, so the
		// header's close button — which every tab has — is the target.
		if target != &console.closeButton {
			t.Fatalf("the %v tab focuses a widget it does not lay out", tab)
		}
	}
}

// And end to end on a tab that is not Console: the keyboard lands on
// something the frame actually drew.
func TestConsoleModalFocusesSomethingOnANonConsoleTab(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	w.consoleModal.selectTab(consoleTabPeers)
	openConsoleForTest(w)

	frame := func() layout.Context {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.layoutConsoleOverlay(gtx)
		router.Frame(gtx.Ops)
		return gtx
	}

	frame()
	gtx := frame()
	if gtx.Focused(nil) {
		t.Fatal("console opened on the Peers tab with the keyboard focused on nothing")
	}
	if !gtx.Focused(&w.consoleModal.closeButton) {
		t.Fatal("the close button did not take focus on a tab with no command line")
	}
}

// Closing the console must hand the keyboard back to the button that opened
// it. Gio drops focus from the command line the frame after it leaves, so
// without this the user is left with no focus at all.
func TestClosingTheConsoleReturnsFocusToItsButton(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)

	frame := func() layout.Context {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		if !w.consoleModalVisible() {
			w.consoleModal.focusRing.restoreOnClose(gtx, &w.messageEditor)
		}
		w.layoutConsoleButton(gtx)
		if w.consoleModalVisible() {
			w.layoutConsoleOverlay(gtx)
		}
		router.Frame(gtx.Ops)
		return gtx
	}

	openConsoleForTest(w)
	frame()
	frame()

	w.closeConsoleModal()
	frame()
	gtx := frame()

	if !gtx.Focused(&w.consoleButton) {
		t.Fatal("closing the console left the keyboard focused on nothing instead of its button")
	}
}

// A pending "focus the composer" request must not fire while the console
// covers it — same rule the context menus already have.
func TestConsoleModalVoidsAPendingComposerFocus(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)

	if !w.contextMenuOpen() {
		t.Fatal("the console modal does not count as an overlay over the composer")
	}
	if focus, _ := consumeComposerFocus(true, false, w.contextMenuOpen()); focus {
		t.Fatal("a pending composer focus survived the console modal")
	}
}

// The system Back key backs out one layer at a time, exactly like Escape. It
// used to close the whole console from inside an open More menu, so a user on
// Android had no way to dismiss just the menu.
//
// Driven through handleBackNavigation with a real key event: calling the
// ladder directly would prove it exists without proving Back is wired to it,
// which is precisely what was wrong.
func TestConsoleBackClosesTheInnerSurfaceFirst(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.tabMenuOpen = true

	pressBack := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(390, 800)),
		}
		w.handleBackNavigation(gtx)
		router.Frame(gtx.Ops)
		router.Queue(key.Event{Name: key.NameBack, State: key.Press})
	}

	// One frame registers the filter, the next one receives the press.
	pressBack()
	pressBack()
	if !w.consoleModalVisible() {
		t.Fatal("Back closed the console instead of its open menu")
	}
	if w.consoleModal.tabMenuOpen {
		t.Fatal("Back left the More menu open")
	}

	pressBack()
	if w.consoleModalVisible() {
		t.Fatal("Back did not close the console once nothing was open inside it")
	}
}

// Closing the console must not leave the More menu armed for the next open.
func TestClosingTheConsoleForgetsTheOpenMenu(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.tabMenuOpen = true

	w.closeConsoleModal()
	openConsoleForTest(w)

	if w.consoleModal.tabMenuOpen {
		t.Fatal("the More menu came back with the reopened console")
	}
}

// The compact layout gives a modal the whole screen. The flag was added and
// then never passed, so a phone got a bordered, rounded, inset card.
func TestModalsAskForTheCompactLayoutOnAPhone(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)

	const phone = 390
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(phone, 800)),
	}
	if !w.isCompactLayout(gtx) {
		t.Fatalf("%ddp is not the compact layout; pick a narrower window", phone)
	}

	w.layoutConsoleOverlay(gtx)
	router.Frame(gtx.Ops)

	// The modal's own size is the backdrop's, which is the whole window either
	// way — the CARD is what changes. Its right edge shows through the close
	// button, the rightmost thing in the header: a compact card is flush with
	// the window and spends only its 16dp padding, while a desktop one loses a
	// further 6dp of inset and 1dp of border.
	edge := 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button {
			edge = max(edge, node.Desc.Bounds.Max.X)
		}
	}
	if want := phone - 16; edge != want {
		t.Fatalf("close button ends at x=%d, want %d: the card is not full-screen", edge, want)
	}
}

// The header close button must actually close. It sits in the shell's header,
// which the shell lays out BEFORE the content — and Clickable.Layout drains
// its own click queue, so a drain that runs after it (from layoutContent, say)
// finds nothing and the button does nothing at all.
func TestConsoleCloseButtonCloses(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)

	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.handleActions(gtx)
		if w.consoleModalVisible() {
			w.layoutConsoleOverlay(gtx)
		}
		router.Frame(gtx.Ops)
	}

	frame()
	// The close button is the top-right control of the card: the window less
	// its 6dp inset, 1dp border and 16dp padding, and half of the 44dp circle.
	at := f32.Pt(1000-6-1-16-22, 6+1+16+22)
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)
	frame()

	if w.consoleModalVisible() {
		t.Fatal("clicking the header close button did not close the console")
	}
}

// The frame that OPENS the modal must stop at the guard too. Checking only at
// the top of handleActions let that one frame run on and register Send, Attach
// and the rest — leaving them in Gio's focus order for the walk to find.
//
// What is observable is the registration itself: Gio keeps focus on a tag only
// while something declares a key.FocusFilter for it, so a FocusCmd aimed at
// Send sticks exactly when handleActions read its clicks, and is dropped when
// it did not.
func TestTheFrameThatOpensTheConsoleReadsNoBackgroundControls(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	w.layoutConsoleButton(gtx)
	router.Frame(gtx.Ops)

	at := f32.Pt(40, 20)
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)

	// The frame that turns the click into an open modal.
	opening := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	w.handleActions(opening)
	under := w.disableUnderConsoleModal(opening)
	w.sendButton.Layout(under, func(gtx layout.Context) layout.Dimensions {
		return layout.Dimensions{Size: image.Pt(40, 20)}
	})
	w.layoutConsoleOverlay(opening)
	// After the overlay, so the modal's own claim on the focus does not simply
	// overwrite this one — the last command of a frame wins.
	opening.Execute(key.FocusCmd{Tag: &w.sendButton})
	router.Frame(opening.Ops)

	if !w.consoleModalVisible() {
		t.Fatal("test setup: the click did not open the console")
	}

	next := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	if next.Focused(&w.sendButton) {
		t.Fatal("Send took focus on the frame that opened the modal: handleActions read it and left it in the focus order")
	}
}

// Opening the console closes the emoji picker through the picker's own close,
// which also drops its search query and grid offset. Clearing the flag by hand
// left the query behind, so the picker reopened filtered by something the user
// had forgotten typing.
func TestOpeningTheConsoleClosesTheEmojiPickerProperly(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	w.emojiPicker.visible = true
	w.emojiPicker.panel.Search.SetText("pizza")
	w.emojiPicker.panel.Grid.Position.First = 3

	openConsoleForTest(w)

	if w.emojiPicker.visible {
		t.Fatal("the emoji picker stayed open under the console modal")
	}
	if got := w.emojiPicker.panel.Search.Text(); got != "" {
		t.Fatalf("the picker kept the search query %q, so it reopens filtered by it", got)
	}
	if got := w.emojiPicker.panel.Grid.Position.First; got != 0 {
		t.Fatalf("the picker kept its grid offset at row %d, which indexes a result list that no longer exists", got)
	}
}
