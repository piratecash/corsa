package desktop

import (
	"encoding/json"
	"image"
	"os"
	"path/filepath"
	"sort"
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

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

// The language choice used to be a dropdown hanging off a header button; it is
// now a block of rows on the console's Settings tab. What is covered here is
// what the application puts in those rows and what picking one does — the row
// COMPONENT is covered in internal/app/desktop/ui.

// The rows say which one is current, so the block shows where the user is.
func TestLanguageRowsMarkTheCurrentLanguage(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	w.language = "ru"

	items := w.languageOptionRows()
	if len(items) != len(supportedLanguages) {
		t.Fatalf("language block has %d rows, want %d", len(items), len(supportedLanguages))
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
func TestLanguageRowLabelsUseAnEmDash(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)

	for _, item := range w.languageOptionRows() {
		if !strings.Contains(item.Label, " — ") {
			t.Fatalf("language row %q does not separate code and name with an em dash", item.Label)
		}
	}
}

// The closed lookup names the language in use, in the same form as the row it
// stands for.
func TestTheClosedLookupNamesTheCurrentLanguage(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)

	for _, option := range supportedLanguages {
		w.language = option.Code
		label := w.currentLanguageRowLabel()
		if !strings.Contains(label, option.Label) {
			t.Fatalf("with %s selected the lookup reads %q", option.Code, label)
		}
		if !strings.Contains(label, localizedLanguageName(option.Code)) {
			t.Fatalf("the lookup %q does not name the language", label)
		}
	}

	// An unknown code cannot leave the button blank.
	w.language = "qq"
	if w.currentLanguageRowLabel() == "" {
		t.Fatal("an unrecognised language left the lookup with no label")
	}
}

// Picking a row switches the language AND writes it down. The header button
// that used to do this drained its clicks from Window.handleActions, which the
// console modal returns from early — the rows now live inside that modal, so
// the drain had to move with them or the choice would do nothing.
func TestPickingALanguageRowSwitchesAndPersistsIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json.desktop.json")

	w := newIdentityLayoutTestWindow(t)
	w.prefs = &Preferences{path: path}

	var router input.Router
	scroll := widget.List{List: layout.List{Axis: layout.Vertical}}
	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(400, 600)),
		}
		// Drained before the rows are laid out, which is the order the console
		// itself uses: handleActions runs at the top of layoutContent, and a
		// Clickable answers Clicked from what the PREVIOUS frame's area
		// collected.
		w.handleLanguageSelection(gtx)
		w.kit().MenuPopupCard(gtx, ui.MenuPopup{Items: w.languageOptionRows(), Scroll: &scroll})
		router.Frame(gtx.Ops)
	}

	frame()

	// Click the row the layout actually produced rather than a guessed
	// coordinate: the row height moves with the font and the locale.
	rows := languageRowBounds(router)
	if len(rows) != len(supportedLanguages) {
		t.Fatalf("%d rows laid out, want %d", len(rows), len(supportedLanguages))
	}
	second := rows[1]
	at := f32.Pt(float32(second.Min.X+second.Max.X)/2, float32(second.Min.Y+second.Max.Y)/2)

	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)
	frame()

	if w.language != "ru" {
		t.Fatalf("language = %q after picking the second row, want ru", w.language)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("preferences were not written: %v", err)
	}
	var saved struct {
		Language string `json:"language"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("decode preferences: %v", err)
	}
	if saved.Language != "ru" {
		t.Fatalf("saved language = %q, want ru", saved.Language)
	}
}

// languageRowBounds returns the laid-out rows top to bottom.
func languageRowBounds(router input.Router) []image.Rectangle {
	var rows []image.Rectangle
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class != semantic.Button {
			continue
		}
		rows = append(rows, node.Desc.Bounds)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Min.Y < rows[j].Min.Y })
	return rows
}
