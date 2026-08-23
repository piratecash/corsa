package desktop

import (
	"image"
	"testing"

	"gioui.org/io/input"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
)

// console_suggestions_scope_test.go covers where the completion popup counts
// as showing, and what closing the console owes the text underneath it.

// The popup is drawn on the Console tab and nowhere else, so its state must
// not answer for the others. It used to: switching to Peers left the
// suggestions "open", and the first Back or Escape there closed an invisible
// popup while nothing on screen moved.
func TestSuggestionsCountAsShowingOnlyOnTheConsoleTab(t *testing.T) {
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)
	console := w.consoleModal

	console.consoleEditor.SetText("pi")
	console.syncSuggestionVisibility()
	if len(console.consoleSuggestions()) == 0 {
		t.Fatal("test setup: typing a known prefix produced no suggestions")
	}

	console.selectTab(consoleTabPeers)
	if got := len(console.consoleSuggestions()); got != 0 {
		t.Fatalf("%d suggestions reported on the Peers tab, which never draws them", got)
	}
}

// And the key that lands there closes the modal, rather than being eaten by a
// popup the user cannot see.
func TestEscapeOnAnotherTabClosesTheModal(t *testing.T) {
	var router input.Router
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)
	console := w.consoleModal

	console.consoleEditor.SetText("pi")
	console.syncSuggestionVisibility()
	console.selectTab(consoleTabPeers)

	w.escapeConsoleModal(consoleLadderContext(&router))

	if w.consoleModalVisible() {
		t.Fatal("Escape on the Peers tab was swallowed by the Console tab's popup")
	}
}

// Every suggestion the popup counts must be one the popup can show. Laid out
// as plain rows they hug their content when there is room and get zero height
// when there is not — while arrow navigation went on counting them, so the
// user could select, and run, a command they could not see.
func TestSuggestionRowsStayVisibleInAShortWindow(t *testing.T) {
	var router input.Router
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)
	console := w.consoleModal

	console.consoleEditor.SetText("fetch")
	console.syncSuggestionVisibility()
	suggestions := console.consoleSuggestions()
	if len(suggestions) < 2 {
		t.Fatalf("test setup: %d suggestions, need at least 2 to run out of room", len(suggestions))
	}

	// Far less room than the rows need.
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(560, 90)},
	}
	console.layoutConsoleSuggestions(gtx, suggestions)
	router.Frame(gtx.Ops)

	drawn := 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button && node.Desc.Bounds.Dy() > 0 {
			drawn++
		}
	}
	if drawn == 0 {
		t.Fatal("no suggestion row survived the short window at all")
	}
	// The panel scrolls, so the rows past the fold are reachable rather than
	// laid out flat at zero height.
	if console.suggestList.Position.Length <= 0 {
		t.Fatal("the suggestion panel does not scroll: rows past the fold are unreachable")
	}
}

// Stepping the highlight must not scroll a list that already shows the row.
// Scrolling unconditionally made the selection the FIRST element, and
// layout.List draws nothing before First — so Down to the second suggestion
// hid the first, and Up from nothing left a single row on screen.
func TestSuggestionScrollOnlyMovesForAnOffScreenRow(t *testing.T) {
	w := newConsoleWithCommands(t)
	console := w.consoleModal

	// Four rows on screen starting at the top.
	console.suggestList.Position.First = 0
	console.suggestList.Position.Count = 4

	console.selectedSuggest = 1
	console.scrollSuggestionIntoView(6)
	if got := console.suggestList.Position.First; got != 0 {
		t.Fatalf("selecting a row already on screen scrolled to %d, hiding the rows above it", got)
	}

	// Past the bottom: the row comes into view at the END of the span, so what
	// is above it stays where the user last saw it.
	console.selectedSuggest = 5
	console.scrollSuggestionIntoView(6)
	if got, want := console.suggestList.Position.First, 2; got != want {
		t.Fatalf("scrolled to %d for a row past the bottom, want %d", got, want)
	}

	// Above the top: that one does become first, because there is nothing to
	// preserve above it.
	console.selectedSuggest = 1
	console.scrollSuggestionIntoView(6)
	if got := console.suggestList.Position.First; got != 1 {
		t.Fatalf("scrolled to %d for a row above the top, want 1", got)
	}
}

// And the next query opens at its own first row rather than partway down the
// last one.
func TestSuggestionScrollResetsWhenThePopupCloses(t *testing.T) {
	w := newConsoleWithCommands(t)
	console := w.consoleModal
	console.suggestList.Position.First = 3

	console.hideSuggestionsUntilRetyped()

	if got := console.suggestList.Position.First; got != 0 {
		t.Fatalf("the popup kept its scroll position at row %d for the next query", got)
	}
}

// Arrow navigation parks what the user typed in suggestBaseQuery and puts the
// highlighted command in the editor. Closing mid-walk owes them their own text
// back — it used to drop the base query and reopen showing a command they had
// never written.
func TestClosingMidSuggestionWalkKeepsWhatWasTyped(t *testing.T) {
	w := newConsoleWithCommands(t)
	openConsoleForTest(w)
	console := w.consoleModal

	console.consoleEditor.SetText("pi")
	console.syncSuggestionVisibility()
	suggestions := console.consoleSuggestions()
	if len(suggestions) == 0 {
		t.Fatal("test setup: typing a known prefix produced no suggestions")
	}

	// Walking onto the first suggestion freezes the snapshot and rewrites the
	// editor with the highlighted command.
	console.moveSuggestionSelection(1, suggestions)
	if got := console.consoleEditor.Text(); got == "pi" {
		t.Fatal("test setup: arrow navigation did not rewrite the command line")
	}

	w.closeConsoleModal()
	openConsoleForTest(w)

	if got := console.consoleEditor.Text(); got != "pi" {
		t.Fatalf("the command line came back as %q, want the %q the user typed", got, "pi")
	}
}

// EVERY way of putting the popup away resets its scroll, not just Escape and
// closing the modal. Accepting a suggestion — by Tab, click, Enter or Right
// Arrow — used to clear the same five fields by hand, which is how the reset
// reached only some of the paths and the next query opened partway down.
func TestEveryDismissalResetsTheSuggestionScroll(t *testing.T) {
	dismissals := map[string]func(gtx layout.Context, c *consoleModal, suggestions []consoleSuggestion){
		"accept the highlighted one": func(gtx layout.Context, c *consoleModal, s []consoleSuggestion) {
			c.applySelectedSuggestion(gtx, s, true)
		},
		"accept and keep typing arguments": func(gtx layout.Context, c *consoleModal, s []consoleSuggestion) {
			c.commitSuggestionForArguments(gtx, s)
		},
		"click a row": func(gtx layout.Context, c *consoleModal, s []consoleSuggestion) {
			c.applySuggestion(gtx, s[0].Insert)
		},
		"walk the command history": func(gtx layout.Context, c *consoleModal, s []consoleSuggestion) {
			c.commandHistory = []string{"ping"}
			c.historyCursor = 1
			c.navigateHistory(-1)
		},
	}

	for name, dismiss := range dismissals {
		t.Run(name, func(t *testing.T) {
			var router input.Router
			w := newConsoleWithCommands(t)
			openConsoleForTest(w)
			console := w.consoleModal

			console.consoleEditor.SetText("fetch")
			console.syncSuggestionVisibility()
			suggestions := console.consoleSuggestions()
			if len(suggestions) == 0 {
				t.Fatal("test setup: typing a known prefix produced no suggestions")
			}
			// As if the user had scrolled down to a later row.
			console.suggestList.Position.First = 2

			dismiss(consoleLadderContext(&router), console, suggestions)

			if got := console.suggestList.Position.First; got != 0 {
				t.Fatalf("the popup kept its scroll position at row %d, so the next query opens partway down", got)
			}
		})
	}
}
