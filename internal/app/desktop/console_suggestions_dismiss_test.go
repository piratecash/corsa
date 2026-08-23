package desktop

import (
	"image"
	"testing"

	"gioui.org/io/input"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
)

// console_suggestions_dismiss_test.go covers the completion popup's place in
// the Escape/Back ladder. The predicate that decided "is the popup showing"
// used to be inside out: a visible list answered no and an already-dismissed
// one answered yes.

// newConsoleWithCommands builds a console whose command table has entries, so
// typing a prefix actually produces suggestions.
func newConsoleWithCommands(t *testing.T) *Window {
	t.Helper()
	w := newIdentityLayoutTestWindow(t)
	w.cmdTable = testTable()
	w.consoleModal = newConsoleModal(w)
	return w
}

func consoleLadderContext(router *input.Router) layout.Context {
	return layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
}

// A showing popup is the first thing Escape takes away, and the modal the
// second. Before this the visible list reported itself as absent, so the first
// Escape closed the whole console.
func TestEscapeClosesTheSuggestionPopupBeforeTheModal(t *testing.T) {
	var router input.Router
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)

	console := w.consoleModal
	console.consoleEditor.SetText("pi")
	console.syncSuggestionVisibility()
	if len(console.consoleSuggestions()) == 0 {
		t.Fatal("test setup: typing a known prefix produced no suggestions")
	}

	w.escapeConsoleModal(consoleLadderContext(&router))
	if !w.consoleModalVisible() {
		t.Fatal("Escape closed the console instead of its suggestion popup")
	}
	if got := len(console.consoleSuggestions()); got != 0 {
		t.Fatalf("%d suggestions still showing after Escape", got)
	}

	w.escapeConsoleModal(consoleLadderContext(&router))
	if w.consoleModalVisible() {
		t.Fatal("Escape did not close the console once the popup was gone")
	}
}

// With no popup showing, Escape closes the modal and leaves the typed command
// alone. It used to read the dismissed state as "a popup is open", eat the
// key, and reset the editor to an empty base query — wiping what was typed.
func TestEscapeWithNoPopupKeepsTheTypedCommand(t *testing.T) {
	var router input.Router
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)

	console := w.consoleModal
	console.consoleEditor.SetText("ping")
	console.syncSuggestionVisibility()
	// Dismiss the popup the way picking a suggestion does.
	console.hideSuggestions = true

	w.escapeConsoleModal(consoleLadderContext(&router))

	if w.consoleModalVisible() {
		t.Fatal("Escape was swallowed by a popup that was not showing")
	}
	if got := console.consoleEditor.Text(); got != "ping" {
		t.Fatalf("the typed command became %q, want it untouched", got)
	}
}

// Nothing open INSIDE the console survives its close — the popup included.
// Only the tab menu was being reset, so a console closed with suggestions up
// reopened with them up, over a command the user had stopped typing.
func TestClosingTheConsoleForgetsTheSuggestionPopup(t *testing.T) {
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)

	console := w.consoleModal
	console.consoleEditor.SetText("pi")
	console.syncSuggestionVisibility()
	if len(console.consoleSuggestions()) == 0 {
		t.Fatal("test setup: typing a known prefix produced no suggestions")
	}

	w.closeConsoleModal()
	openConsoleForTest(w)
	console.syncSuggestionVisibility()

	if got := len(console.consoleSuggestions()); got != 0 {
		t.Fatalf("%d suggestions came back with the reopened console", got)
	}
	// The command itself is state the user typed, and it survives like the
	// history does.
	if got := console.consoleEditor.Text(); got != "pi" {
		t.Fatalf("the typed command became %q across a close, want it kept", got)
	}
}
