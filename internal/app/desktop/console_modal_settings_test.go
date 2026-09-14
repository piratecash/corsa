package desktop

import (
	"image"
	"path/filepath"
	"reflect"
	"sort"
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

// console_modal_settings_test.go covers the Settings tab: that it is reachable,
// that the consent box is wired to the preference, and that the controls the
// consent gates appear and disappear with it.

func TestSettingsTabIsOneOfTheConsoleTabs(t *testing.T) {
	found := false
	for _, tab := range consoleTabOrder() {
		if tab == consoleTabSettings {
			found = true
		}
	}
	if !found {
		t.Fatal("the Settings tab is not in consoleTabOrder, so nothing can draw or click it")
	}
	if consoleTabLabelKeys[consoleTabSettings] == "" {
		t.Fatal("the Settings tab has no label key")
	}
}

// settingsTabFrame lays the tab out once and returns the buttons it produced,
// top to bottom.
//
// ONE router across every frame of a test: a fresh one would drop the events
// queued against the previous frame, which is the whole mechanism a click test
// runs on.
func settingsTabFrame(t *testing.T, w *Window, router *input.Router, size image.Point) []image.Rectangle {
	t.Helper()
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(size),
	}
	w.consoleModal.handleSettingsActions(gtx)
	dims := w.consoleModal.layoutSettingsTab(gtx)
	router.Frame(gtx.Ops)

	if dims.Size.X == 0 || dims.Size.Y == 0 {
		t.Fatalf("the Settings tab laid out %v", dims.Size)
	}

	var buttons []image.Rectangle
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button {
			buttons = append(buttons, node.Desc.Bounds)
		}
	}
	sort.Slice(buttons, func(i, j int) bool { return buttons[i].Min.Y < buttons[j].Min.Y })
	return buttons
}

// settingsCheckboxCount is what the tab announced as a checkbox this frame.
func settingsCheckboxCount(router *input.Router) int {
	boxes := 0
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.CheckBox {
			boxes++
		}
	}
	return boxes
}

// The tab draws, and it draws the things the user came for. A layout that
// panics or silently produces nothing is the failure this guards.
func TestSettingsTabDrawsTheLanguageLookupAndTheConsentBox(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)

	router := new(input.Router)
	buttons := settingsTabFrame(t, w, router, image.Pt(390, 900))

	// Closed, the language choice is ONE button — not six rows. That is the
	// whole point of a lookup: six full-width options stretched across the tab
	// is what it replaced.
	if len(buttons) != 1 {
		t.Fatalf("the closed tab laid out %d buttons, want just the language lookup", len(buttons))
	}
	if boxes := settingsCheckboxCount(router); boxes != 1 {
		t.Fatalf("the tab laid out %d checkboxes, want exactly the consent box", boxes)
	}
}

// Opening the lookup puts the options on screen; picking one closes it again.
func TestTheLanguageLookupOpensAndClosesOnAPick(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	w.prefs = &Preferences{path: filepath.Join(t.TempDir(), "prefs.json")}
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)

	const size = 390
	router := new(input.Router)
	buttons := settingsTabFrame(t, w, router, image.Pt(size, 900))
	press(router, centreOf(buttons[0]))

	buttons = settingsTabFrame(t, w, router, image.Pt(size, 900))
	if !w.consoleModal.languageMenuOpen {
		t.Fatal("clicking the lookup did not open it")
	}
	// The lookup itself plus one row per language.
	if want := 1 + len(supportedLanguages); len(buttons) != want {
		t.Fatalf("the open lookup laid out %d buttons, want %d", len(buttons), want)
	}

	// The rows hang UNDER the button and HUG it. Both halves matter: a card
	// that floats away from its button stops looking like it belongs to it,
	// and that is exactly what a mis-reconstructed anchor produces — the
	// failure the old header menu had with a hard-coded offset.
	lookup := buttons[0]
	first := buttons[1]
	if first.Min.Y < lookup.Max.Y {
		t.Fatalf("the first row starts at y=%d, above the lookup's bottom edge at %d", first.Min.Y, lookup.Max.Y)
	}
	// The anchor gap plus the card's border and padding sit between the two.
	if gap := first.Min.Y - lookup.Max.Y; gap > 24 {
		t.Fatalf("the first row starts %ddp under the lookup, want it hugging the button", gap)
	}
	// And it hangs from the button's LEFT edge, not the tab's right.
	if delta := first.Min.X - lookup.Min.X; delta < -12 || delta > 12 {
		t.Fatalf("the card starts at x=%d, the lookup at x=%d", first.Min.X, lookup.Min.X)
	}

	// Second row: RU, in supportedLanguages order.
	press(router, centreOf(buttons[2]))
	buttons = settingsTabFrame(t, w, router, image.Pt(size, 900))

	if w.language != "ru" {
		t.Fatalf("language = %q after picking the second row, want ru", w.language)
	}
	if w.consoleModal.languageMenuOpen {
		t.Fatal("picking a language left the lookup open")
	}
	if len(buttons) != 1 {
		t.Fatalf("%d buttons after the pick, want the closed lookup alone", len(buttons))
	}
}

// The card never leaves the tab, whatever the window is doing.
//
// Under the button is the natural side, and the first cut always used it: when
// less than a usable row was left below, it forced the height up to a floor
// WITHOUT moving the card, so the last options sat past the bottom edge where
// scrolling INSIDE the card could not reach them. A menu that does not fit has
// to move, not grow.
func TestTheLanguageLookupStaysInsideTheTab(t *testing.T) {
	// 130dp tall is the reported case: the button ends near the bottom edge and
	// there is no room under it at all. The rest bracket it, down to a tab too
	// short to hold either the card or the section it belongs to.
	for _, size := range []image.Point{{X: 390, Y: 100}, {X: 390, Y: 130}, {X: 390, Y: 220}, {X: 390, Y: 400}, {X: 390, Y: 900}} {
		w := newConsoleModalTestWindow(t)
		openConsoleForTest(w)
		w.consoleModal.selectTab(consoleTabSettings)

		router := new(input.Router)
		// One frame closed, so the button has a measured size for the anchor.
		settingsTabFrame(t, w, router, size)
		w.consoleModal.languageMenuOpen = true
		settingsTabFrame(t, w, router, size)

		// The card's own rectangle, not the frame's semantics: a list reports
		// the UNCLIPPED bounds of rows it has scrolled out of view, so a button
		// below the fold reads as an overflowing one and the assertion would
		// fire on ordinary scrolling.
		card := w.consoleModal.languageMenuRect
		if card.Dx() == 0 || card.Dy() == 0 {
			t.Fatalf("%v: the open lookup drew nothing", size)
		}
		// The card is placed inside the tab's 8dp padding, so the area it must
		// stay within is the tab less that padding on each side.
		area := image.Rect(0, 0, size.X-16, size.Y-16)
		if !card.In(area) {
			t.Fatalf("%v: the card occupies %v, outside the %v it was drawn in", size, card, area)
		}
	}
}

// The placement rule on its own, where the arithmetic is visible.
func TestLanguageMenuPlacementPicksASideAndStaysInside(t *testing.T) {
	const gap = 4

	cases := []struct {
		name   string
		area   int
		anchor image.Rectangle
		wanted int
		// wantAbove is whether the card is expected to end at or before the
		// button's top edge; wantWholeArea that neither side had room and it
		// took the tab.
		wantAbove     bool
		wantWholeArea bool
	}{
		{name: "room below", area: 900, anchor: image.Rect(0, 100, 200, 130), wanted: 250},
		{name: "no room below", area: 130, anchor: image.Rect(0, 90, 200, 120), wanted: 250, wantAbove: true},
		{name: "more room above", area: 400, anchor: image.Rect(0, 300, 200, 330), wanted: 250, wantAbove: true},
		{name: "tighter above than below", area: 400, anchor: image.Rect(0, 40, 200, 70), wanted: 250},
		// Neither side has room: the card takes the whole tab and scrolls,
		// which is the only placement in which every option is reachable.
		{name: "nothing fits anywhere", area: 40, anchor: image.Rect(0, 10, 200, 38), wanted: 250, wantWholeArea: true},
		{name: "anchor scrolled above the tab", area: 200, anchor: image.Rect(0, -60, 200, -30), wanted: 250},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gtx := layout.Context{
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(390, tc.area)),
			}
			y, height := languageMenuPlacement(gtx, tc.anchor, tc.wanted, gap)
			card := min(tc.wanted, height)

			if y < 0 {
				t.Fatalf("y = %d, above the top of the tab", y)
			}
			if y+card > tc.area {
				t.Fatalf("the card occupies y=%d..%d in a %ddp tab", y, y+card, tc.area)
			}
			if height <= 0 {
				t.Fatalf("height = %d: the card was given no room at all", height)
			}
			switch {
			case tc.wantWholeArea:
				if height != tc.area {
					t.Fatalf("height = %d, want the whole %ddp tab", height, tc.area)
				}
			case tc.wantAbove:
				if y+card > tc.anchor.Min.Y {
					t.Fatalf("the card ends at y=%d, want it above the button starting at %d", y+card, tc.anchor.Min.Y)
				}
			case tc.anchor.Max.Y >= 0:
				if y < tc.anchor.Max.Y {
					t.Fatalf("the card starts at y=%d, want it below the button ending at %d", y, tc.anchor.Max.Y)
				}
			}
		})
	}
}

// Escape backs out of the lookup before it backs out of the console — the same
// ladder the tab menu is on.
func TestEscapeClosesTheLanguageLookupBeforeTheConsole(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)
	w.consoleModal.languageMenuOpen = true

	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(390, 900)),
	}
	w.escapeConsoleModal(gtx)

	if w.consoleModal.languageMenuOpen {
		t.Fatal("Escape did not close the lookup")
	}
	if !w.consoleModalVisible() {
		t.Fatal("Escape closed the whole console instead of just the lookup")
	}

	w.escapeConsoleModal(gtx)
	if w.consoleModalVisible() {
		t.Fatal("a second Escape did not close the console")
	}
}

// Leaving the tab takes the lookup with it: a card that came back over a tab
// the user had not chosen is the bug closeInnerSurfaces exists to prevent.
func TestSwitchingTabsClosesTheLanguageLookup(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)
	w.consoleModal.languageMenuOpen = true

	w.closeConsoleModal()

	if w.consoleModal.languageMenuOpen {
		t.Fatal("the lookup survived the console closing")
	}
}

func centreOf(r image.Rectangle) f32.Point {
	return f32.Pt(float32(r.Min.X+r.Max.X)/2, float32(r.Min.Y+r.Max.Y)/2)
}

func press(router *input.Router, at f32.Point) {
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)
}

// The box is a view of the preference, not a second copy of it: a checkbox
// with its own bool drifts from what the checker is actually doing the first
// time either is set anywhere else.
func TestConsentBoxReflectsThePreference(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	w.prefs = &Preferences{path: filepath.Join(t.TempDir(), "prefs.json")}

	if w.releaseCheckEnabled() {
		t.Fatal("the release check is on by default; it must be opt-in")
	}

	w.prefs.CheckGitHubReleases = true
	if !w.releaseCheckEnabled() {
		t.Fatal("the box does not follow the preference")
	}
}

// Clicking the box records consent. The console modal returns early from
// Window.handleActions, so this is also what proves the Settings controls are
// drained from the console's own handler rather than the window's.
func TestClickingTheConsentBoxRecordsConsent(t *testing.T) {
	var router input.Router
	w := newConsoleModalTestWindow(t)
	w.prefs = &Preferences{path: filepath.Join(t.TempDir(), "prefs.json")}
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)

	// The WHOLE console, not layoutSettingsTab on its own: what this has to
	// prove is that the Settings controls are drained by the console's own
	// handler. Window.handleActions returns early while the modal is up, so a
	// control left there would draw and never fire.
	frame := func() {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(390, 900)),
		}
		w.consoleModal.layoutContent(gtx)
		router.Frame(gtx.Ops)
	}

	frame()

	box := checkboxBounds(t, &router)
	at := f32.Pt(float32(box.Min.X+box.Max.X)/2, float32(box.Min.Y+box.Max.Y)/2)
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
	)
	frame()

	if !w.releaseCheckEnabled() {
		t.Fatal("clicking the consent box did not turn the release check on")
	}
}

// The manual button and the status line belong to a check that may happen.
// With consent withdrawn there is nothing to run and nothing to report, and
// leaving a "Check now" button behind would invite exactly the request the
// user just declined.
func TestTheCheckNowButtonAppearsOnlyWithConsent(t *testing.T) {
	w := newConsoleModalTestWindow(t)
	w.prefs = &Preferences{path: filepath.Join(t.TempDir(), "prefs.json")}
	openConsoleForTest(w)
	w.consoleModal.selectTab(consoleTabSettings)

	countButtons := func() int {
		var router input.Router
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(390, 900)),
		}
		w.consoleModal.layoutSettingsTab(gtx)
		router.Frame(gtx.Ops)

		buttons := 0
		for _, node := range router.AppendSemantics(nil) {
			if node.Desc.Class == semantic.Button {
				buttons++
			}
		}
		return buttons
	}

	attachReleaseChecker(t, w, testNewerVersion, nil)

	withoutConsent := countButtons()
	w.prefs.CheckGitHubReleases = true
	withConsent := countButtons()

	if withConsent != withoutConsent+1 {
		t.Fatalf("consent changed the button count from %d to %d, want exactly one more (Check now)",
			withoutConsent, withConsent)
	}

	// And with no checker at all — an unparsable running version, see
	// newReleaseChecker — the button goes too. It would do nothing, and there
	// would be no status line to say why.
	w.releaseChecker = nil
	if got := countButtons(); got != withoutConsent {
		t.Fatalf("with no checker the tab draws %d buttons, want the %d it draws without consent",
			got, withoutConsent)
	}
}

// layout.Axis's zero value is HORIZONTAL and every scrolling surface in the
// console is vertical, so the axis is set at construction, once per list.
// Forgetting it is silent: the list lays its content out sideways with an
// unbounded width, which reads as "the tab drew, and every control in it is a
// million pixels wide and cannot be clicked". That is exactly how the Settings
// tab first behaved.
func TestEveryConsoleListScrollsVertically(t *testing.T) {
	console := newConsoleModal(newIdentityLayoutTestWindow(t))

	value := reflect.ValueOf(console).Elem()
	listType := reflect.TypeOf(widget.List{})
	checked := 0
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if field.Type() != listType {
			continue
		}
		checked++
		// Read through reflect rather than Interface(): most of these fields
		// are unexported, and Interface() refuses those.
		axis := field.FieldByName("Axis")
		if !axis.IsValid() {
			t.Fatalf("field %s has no Axis", value.Type().Field(i).Name)
		}
		if got := layout.Axis(axis.Uint()); got != layout.Vertical {
			t.Errorf("%s.Axis is %v, want Vertical", value.Type().Field(i).Name, got)
		}
	}
	if checked == 0 {
		t.Fatal("no widget.List fields found; this guard is inspecting the wrong type")
	}
}

// checkboxBounds returns the one checkbox the Settings tab draws.
func checkboxBounds(t *testing.T, router *input.Router) image.Rectangle {
	t.Helper()
	var found []image.Rectangle
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.CheckBox {
			found = append(found, node.Desc.Bounds)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d checkboxes laid out, want exactly one", len(found))
	}
	return found[0]
}
