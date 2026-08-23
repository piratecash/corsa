package desktop

import (
	"image"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/f32"
	"gioui.org/gpu/headless"
	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
)

func TestFilterEmojiChoices(t *testing.T) {
	tests := []struct {
		name     string
		category emojiCategoryID
		query    string
		recents  []string
		want     []string
	}{
		{
			name:     "selected category",
			category: emojiCategorySmileys,
			want:     emojiValues(emojiCategorySmileys),
		},
		{
			name:     "search ignores selected category",
			category: emojiCategorySmileys,
			query:    "огонь",
			want:     []string{"🔥"},
		},
		{
			name:     "search is case insensitive",
			category: emojiCategoryAnimals,
			query:    "HEART",
			want:     []string{"😍", "🥰", "😘", "❤️", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔", "💕", "💖", "💗", "❣️", "💞", "💓", "💘", "💝", "💟"},
		},
		{
			name:     "recent category preserves order",
			category: emojiCategoryRecent,
			recents:  []string{"🔥", "😊", "👍"},
			want:     []string{"🔥", "😊", "👍"},
		},
		{name: "unicorn english", query: "unicorn", want: []string{"🦄"}},
		{name: "unicorn russian", query: "единорог", want: []string{"🦄"}},
		{name: "star russian", query: "звезда", want: []string{"⭐"}},
		{name: "money russian", query: "деньги", want: []string{"💰"}},
		{name: "beer russian", query: "пиво", want: []string{"🍺"}},
		{name: "check russian", query: "галочка", want: []string{"✅"}},
		{name: "sun russian", query: "солнце", want: []string{"☀️"}},
		{name: "pizza incremental prefix", query: "piz", want: []string{"🍕"}},
		{name: "heart incremental prefix", query: "сердц", want: []string{"😍", "🥰", "😘", "❤️", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔", "💕", "💖", "💗", "❣️", "💞", "💓", "💘", "💝", "💟"}},
		{name: "unicorn incremental prefix", query: "unic", want: []string{"🦄"}},
		{name: "apple english", query: "apple", want: []string{"🍏", "🍎"}},
		{name: "apple russian", query: "яблоко", want: []string{"🍏", "🍎"}},
		{name: "infix does not match keyword tokens", query: "lass", want: nil},
		{name: "sun does not match sunglasses", query: "sun", want: []string{"☀️"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterEmojiChoices(tt.category, tt.query, tt.recents); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("filterEmojiChoices(%q, %q) = %#v, want %#v", tt.category, tt.query, got, tt.want)
			}
		})
	}
}

func TestEveryEmojiHasSpecificSearchKeywords(t *testing.T) {
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			keywords := strings.TrimSpace(emojiSpecificKeywords[entry.value])
			if keywords == "" {
				t.Errorf("emoji %q in category %q has no specific search keywords", entry.value, category.id)
			}
			var hasEnglish, hasRussian bool
			for _, r := range keywords {
				hasEnglish = hasEnglish || unicode.Is(unicode.Latin, r)
				hasRussian = hasRussian || unicode.Is(unicode.Cyrillic, r)
			}
			if !hasEnglish || !hasRussian {
				t.Errorf("emoji %q keywords must include English and Russian names: %q", entry.value, keywords)
			}
		}
	}
}

func TestEmojiCatalogPrecompilesCombinedSearchTokens(t *testing.T) {
	var apple *emojiEntry
	for categoryIndex := range emojiCategories {
		for entryIndex := range emojiCategories[categoryIndex].entries {
			entry := &emojiCategories[categoryIndex].entries[entryIndex]
			if entry.value == "🍎" {
				apple = entry
				break
			}
		}
	}
	if apple == nil {
		t.Fatal("red apple is missing from the catalog")
	}
	for _, want := range []string{"food", "еда", "apple", "яблоко"} {
		if !slices.Contains(apple.searchTokens, want) {
			t.Errorf("compiled search tokens %q do not contain %q", apple.searchTokens, want)
		}
	}
}

var emojiFilterSink []string

func TestFilterEmojiChoicesKeepsHotPathAllocationsBounded(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		emojiFilterSink = filterEmojiChoices(emojiCategorySmileys, "piz", nil)
	})
	if allocs > 12 {
		t.Fatalf("filterEmojiChoices allocations = %.0f/run, want at most 12", allocs)
	}
}

func TestDisabledComposerMeasurementReusesEmojiChoices(t *testing.T) {
	state := newEmojiPickerState()
	state.visibleChoices = []string{"😀"}
	state.searchEditor.SetText("pizza")

	if got := state.resolveVisibleChoices(false); !reflect.DeepEqual(got, []string{"😀"}) {
		t.Fatalf("disabled measurement recalculated choices: %#v", got)
	}
	if got := state.resolveVisibleChoices(true); !reflect.DeepEqual(got, []string{"🍕"}) {
		t.Fatalf("enabled layout choices = %#v, want pizza", got)
	}
}

func BenchmarkFilterEmojiChoices(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		emojiFilterSink = filterEmojiChoices(emojiCategorySmileys, "piz", nil)
	}
}

func TestRememberRecentEmojiDeduplicatesAndBounds(t *testing.T) {
	recents := []string{"😊", "🔥", "👍"}
	recents = rememberRecentEmoji(recents, "🔥")
	if want := []string{"🔥", "😊", "👍"}; !reflect.DeepEqual(recents, want) {
		t.Fatalf("repeat = %#v, want %#v", recents, want)
	}

	for _, emoji := range []string{"😂", "🥰", "😎", "🤔", "👏", "🙏", "🎉", "🚀", "❤️", "🐶", "🍕", "⚽", "💡", "🏁"} {
		recents = rememberRecentEmoji(recents, emoji)
	}
	if len(recents) != maxRecentEmojis {
		t.Fatalf("recent count = %d, want %d", len(recents), maxRecentEmojis)
	}
	if recents[0] != "🏁" {
		t.Fatalf("most recent = %q, want flag", recents[0])
	}
}

func TestInsertEmojiReplacesSelection(t *testing.T) {
	var editor widget.Editor
	editor.SetText("hello world")
	editor.SetCaret(6, 11)

	insertEmoji(&editor, "😊")

	if got := editor.Text(); got != "hello 😊" {
		t.Fatalf("editor text = %q, want %q", got, "hello 😊")
	}
	start, end := editor.Selection()
	if start != 7 || end != 7 {
		t.Fatalf("caret = (%d, %d), want (7, 7)", start, end)
	}
}

func TestAppThemeIncludesEmojiFallback(t *testing.T) {
	theme := newAppTheme()
	// The BUNDLED family by name, not the word "emoji": asking for the generic
	// family handed the request to the host's own font — see emoji_font.go.
	if !strings.Contains(string(theme.Face), string(emojiTypeface)) {
		t.Fatalf("theme face = %q, want the bundled %q family as a fallback", theme.Face, emojiTypeface)
	}
}

func TestLoadWindowIconsIncludesComposerIcons(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	if icons.attach == nil || icons.emoji == nil || icons.send == nil || icons.shield == nil || icons.console == nil {
		t.Fatal("composer icon set is incomplete")
	}
	for _, category := range []emojiCategoryID{
		emojiCategoryRecent,
		emojiCategorySmileys,
		emojiCategoryGestures,
		emojiCategoryAnimals,
		emojiCategoryFood,
		emojiCategoryTravel,
		emojiCategoryActivities,
		emojiCategorySymbols,
		emojiCategoryFlags,
	} {
		if icons.emojiCategories[category] == nil {
			t.Fatalf("category %q has no icon", category)
		}
	}
}

func TestComposerFooterResponsiveRows(t *testing.T) {
	loadedIcons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}

	tests := []struct {
		name      string
		width     int
		maxHeight int
		minHeight int
	}{
		{name: "medium width stays on one row", width: 450, maxHeight: 60},
		{name: "narrow width stacks", width: 320, minHeight: 70},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var router input.Router
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Constraints{Max: image.Pt(tt.width, 200)},
			}
			w := &Window{
				theme:       newAppTheme(),
				language:    "en",
				shieldIcon:  loadedIcons.shield,
				consoleIcon: loadedIcons.console,
			}

			dims := w.layoutComposerFooter(gtx, service.NodeStatus{})
			if tt.maxHeight > 0 && dims.Size.Y > tt.maxHeight {
				t.Fatalf("footer height at width %d = %d, want at most %d", tt.width, dims.Size.Y, tt.maxHeight)
			}
			if tt.minHeight > 0 && dims.Size.Y < tt.minHeight {
				t.Fatalf("footer height at width %d = %d, want at least %d", tt.width, dims.Size.Y, tt.minHeight)
			}
		})
	}
}

func TestOpenEmojiPickerFitsComposerHeight(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(360, 360)},
	}
	w := &Window{
		theme:              newAppTheme(),
		language:           "en",
		attachIcon:         icons.attach,
		emojiIcon:          icons.emoji,
		sendIcon:           icons.send,
		searchIcon:         icons.search,
		emojiCategoryIcons: icons.emojiCategories,
		emojiPicker:        newEmojiPickerState(),
	}
	w.emojiPicker.visible = true

	dims := w.messageInputCard(gtx, domain.PeerIdentity{}, 120, 44)
	if dims.Size.Y > gtx.Constraints.Max.Y {
		t.Fatalf("open picker height = %d, exceeds composer constraint %d", dims.Size.Y, gtx.Constraints.Max.Y)
	}
	if dims.Size.Y < 200 {
		t.Fatalf("open picker height = %d, want a usable picker", dims.Size.Y)
	}
}

func TestEmojiGridColumnsUseAvailableWidth(t *testing.T) {
	tests := []struct {
		width int
		want  int
	}{
		{width: 240, want: 4},
		{width: 360, want: 6},
		{width: 760, want: 14},
		{width: 1200, want: 23},
	}

	for _, tt := range tests {
		if got := emojiGridColumns(tt.width, 52); got != tt.want {
			t.Errorf("emojiGridColumns(%d, 52) = %d, want %d", tt.width, got, tt.want)
		}
	}
}

func TestEmojiGridOccupiesFullAvailableWidth(t *testing.T) {
	for _, width := range []int{360, 1200} {
		var router input.Router
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(width, 240)},
		}
		w := &Window{
			theme:       newAppTheme(),
			language:    "en",
			emojiPicker: newEmojiPickerState(),
		}

		dims := w.layoutEmojiGrid(gtx, emojiValues(emojiCategorySmileys))
		if dims.Size.X != width {
			t.Errorf("emoji grid width = %d, want available width %d", dims.Size.X, width)
		}
	}
}

func TestEmojiChoiceOccupiesAllocatedCellWidth(t *testing.T) {
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(104, 52)),
	}
	w := &Window{
		theme:       newAppTheme(),
		language:    "en",
		emojiPicker: newEmojiPickerState(),
	}

	dims := w.layoutEmojiChoice(gtx, "😀")
	if dims.Size.X != gtx.Constraints.Max.X {
		t.Fatalf("emoji cell width = %d, want allocated width %d", dims.Size.X, gtx.Constraints.Max.X)
	}
}

func TestLayoutVerticallyCenteredKeepsNaturalChildHeight(t *testing.T) {
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(200, 34)),
	}
	childMinHeight := -1

	dims := layoutVerticallyCentered(gtx, func(gtx layout.Context) layout.Dimensions {
		childMinHeight = gtx.Constraints.Min.Y
		return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, 14)}
	})

	if childMinHeight != 0 {
		t.Fatalf("centered child minimum height = %d, want natural height constraint 0", childMinHeight)
	}
	if dims.Size != gtx.Constraints.Max {
		t.Fatalf("centered area size = %v, want full search area %v", dims.Size, gtx.Constraints.Max)
	}
}

func TestLayoutComposerEditorCentersOnlySingleLine(t *testing.T) {
	tests := []struct {
		name          string
		lines         int
		wantMinHeight int
	}{
		{name: "placeholder and one line", lines: 1, wantMinHeight: 0},
		{name: "multiline text", lines: 2, wantMinHeight: 36},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(300, 36)),
			}
			childMinHeight := -1

			dims := layoutComposerEditorContent(gtx, tt.lines, func(gtx layout.Context) layout.Dimensions {
				childMinHeight = gtx.Constraints.Min.Y
				return layout.Dimensions{Size: gtx.Constraints.Constrain(image.Pt(gtx.Constraints.Max.X, 18))}
			})

			if childMinHeight != tt.wantMinHeight {
				t.Fatalf("editor minimum height = %d, want %d", childMinHeight, tt.wantMinHeight)
			}
			if dims.Size != gtx.Constraints.Max {
				t.Fatalf("editor area size = %v, want %v", dims.Size, gtx.Constraints.Max)
			}
		})
	}
}

func TestEmojiPickerDismissesOnEscapeAndBack(t *testing.T) {
	for _, keyName := range []key.Name{key.NameEscape, key.NameBack} {
		t.Run(string(keyName), func(t *testing.T) {
			w := &Window{emojiPicker: newEmojiPickerState()}
			w.emojiPicker.visible = true
			router := new(input.Router)
			ops := new(op.Ops)

			runEmojiNavigationFrame(w, router, ops)
			router.Queue(key.Event{Name: keyName, State: key.Press})
			runEmojiNavigationFrame(w, router, ops)

			if w.emojiPicker.visible {
				t.Fatal("emoji picker stayed visible after dismiss key")
			}
			if !router.Source().Focused(&w.messageEditor) {
				t.Fatal("composer did not regain focus after dismissing emoji picker")
			}
		})
	}
}

// Escape (and Back) are handled before the toggle's click is read, so a tap
// delivered in the same frame would re-open what the key just closed. Swapping
// the two handlers is not the answer: the tap would then close the picker and
// the key would fall through to the surface underneath.
func TestEmojiDismissKeyBeatsASameFrameToggleTap(t *testing.T) {
	for _, keyName := range []key.Name{key.NameEscape, key.NameBack} {
		t.Run(string(keyName), func(t *testing.T) {
			w := &Window{emojiPicker: newEmojiPickerState()}
			w.emojiPicker.visible = true
			router := new(input.Router)
			ops := new(op.Ops)
			toggle := f32.Pt(20, 20)

			frame := func() {
				ops.Reset()
				gtx := layout.Context{
					Ops:         ops,
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Constraints{Max: image.Pt(800, 600)},
				}
				w.handleBackNavigation(gtx)
				w.handleEmojiEscapeNavigation(gtx)
				w.handleEmojiActions(gtx)
				w.emojiPicker.toggleButton.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Dimensions{Size: image.Pt(40, 40)}
				})
				router.Frame(ops)
			}

			frame()
			router.Queue(
				pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: toggle},
				pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: toggle},
				key.Event{Name: keyName, State: key.Press},
			)
			frame()

			if w.emojiPicker.visible {
				t.Fatal("a toggle tap delivered with the dismissal key re-opened the picker")
			}
		})
	}
}

func TestEmojiPickerOpeningAndSelectionKeepComposerFocused(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	router := new(input.Router)
	ops := new(op.Ops)

	runEmojiNavigationFrame(w, router, ops, func(gtx layout.Context) {
		gtx.Execute(key.FocusCmd{Tag: &w.emojiPicker.toggleButton})
	})
	if !router.Source().Focused(&w.emojiPicker.toggleButton) {
		t.Fatal("precondition: toggle did not receive focus")
	}

	runEmojiNavigationFrame(w, router, ops, func(gtx layout.Context) {
		w.openEmojiPicker(gtx)
	})
	if !router.Source().Focused(&w.messageEditor) {
		t.Fatal("opening picker stole focus from composer")
	}

	w.messageEditor.SetText("draft")
	w.messageEditor.SetCaret(w.messageEditor.Len(), w.messageEditor.Len())
	runEmojiNavigationFrame(w, router, ops, func(gtx layout.Context) {
		w.selectEmoji(gtx, "😀")
	})
	if got := w.messageEditor.Text(); got != "draft😀" {
		t.Fatalf("draft after emoji selection = %q, want %q", got, "draft😀")
	}
	if !router.Source().Focused(&w.messageEditor) {
		t.Fatal("emoji selection moved focus away from composer")
	}
}

func TestEmojiPickerRestoresGenericSoftKeyboardWithoutRequestingTabTip(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	router := new(input.Router)
	ops := new(op.Ops)
	now := time.Unix(1_000, 0)
	w.touchKbd.softKeyboardExpected.Store(true)

	runEmojiNavigationFrameAt(w, router, ops, now, func(gtx layout.Context) {
		w.openEmojiPicker(gtx)
	})
	if got := router.TextInputState(); got != input.TextInputClose {
		t.Fatalf("opening picker soft-keyboard state = %v, want close", got)
	}
	if !router.Source().Focused(&w.messageEditor) {
		t.Fatal("opening picker blurred the composer")
	}
	if w.emojiPicker.takeSoftKeyboardSuppression(false) {
		t.Fatal("a disabled measurement pass consumed keyboard suppression")
	}
	if !w.emojiPicker.takeSoftKeyboardSuppression(true) {
		t.Fatal("the first enabled editor layout did not suppress its focus-triggered keyboard show")
	}
	if w.emojiPicker.takeSoftKeyboardSuppression(true) {
		t.Fatal("keyboard suppression remained active after the opening layout")
	}

	runEmojiNavigationFrameAt(w, router, ops, now.Add(time.Second), func(gtx layout.Context) {
		w.closeEmojiPicker(gtx)
	})
	if got := router.TextInputState(); got != input.TextInputOpen {
		t.Fatalf("closing picker soft-keyboard state = %v, want open", got)
	}
	if w.touchKbd.platformTouchKeyboardExpected.Load() {
		t.Fatal("generic mouse-style restore incorrectly requested the Windows touch keyboard")
	}
	if !router.Source().Focused(&w.messageEditor) {
		t.Fatal("closing picker did not preserve composer focus")
	}
}

// A query that outlives its picker reopens it on one cell with no category
// highlighted. Nothing on screen explains that except small text in a field
// the user is not looking at.
func TestClosingEmojiPickerClearsTheSearchQuery(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	gtx := layout.Context{Ops: new(op.Ops)}

	w.openEmojiPicker(gtx)
	w.emojiPicker.searchEditor.SetText("пиц")
	w.emojiPicker.list.Position.First = 3
	if got := w.emojiPicker.resolveVisibleChoices(true); len(got) != 1 {
		t.Fatalf("precondition: query matched %d emoji, want the single pizza", len(got))
	}

	w.closeEmojiPicker(gtx)
	w.openEmojiPicker(gtx)

	if query := w.emojiPicker.searchEditor.Text(); query != "" {
		t.Fatalf("reopened picker still filters on %q", query)
	}
	if first := w.emojiPicker.list.Position.First; first != 0 {
		t.Fatalf("reopened picker scrolled to item %d of a result list that no longer exists", first)
	}
	if got, want := w.emojiPicker.resolveVisibleChoices(true), emojiValues(emojiCategorySmileys); len(got) != len(want) {
		t.Fatalf("reopened picker shows %d emoji, want the whole %d of its category", len(got), len(want))
	}
	if !emojiCategoryIsActive(w.emojiPicker.category, emojiCategorySmileys, w.emojiPicker.searchEditor.Text()) {
		t.Fatal("reopened picker highlights no category")
	}
}

func TestEmojiPickerRestoresExpectedPlatformTouchKeyboard(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	w.touchKbd.softKeyboardExpected.Store(true)
	w.touchKbd.platformTouchKeyboardExpected.Store(true)
	gtx := layout.Context{Ops: new(op.Ops)}

	w.openEmojiPicker(gtx)
	if !w.emojiPicker.restorePlatformTouchKeyboard {
		t.Fatal("opening picker did not remember the expected platform touch keyboard")
	}
	w.touchKbd.platformTouchKeyboardExpected.Store(false) // hide completion while picker is open
	w.closeEmojiPicker(gtx)
	if !w.touchKbd.platformTouchKeyboardExpected.Load() {
		t.Fatal("closing picker did not restore the platform touch-keyboard expectation")
	}
}

func runEmojiNavigationFrame(w *Window, router *input.Router, ops *op.Ops, actions ...func(layout.Context)) {
	runEmojiNavigationFrameAt(w, router, ops, time.Time{}, actions...)
}

func runEmojiNavigationFrameAt(w *Window, router *input.Router, ops *op.Ops, now time.Time, actions ...func(layout.Context)) {
	ops.Reset()
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Now:         now,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(800, 600)},
	}
	w.handleBackNavigation(gtx)
	w.handleEmojiEscapeNavigation(gtx)
	for _, action := range actions {
		action(gtx)
	}
	for _, tag := range []event.Tag{&w.messageEditor, &w.emojiPicker.toggleButton, &w.emojiPicker.searchEditor} {
		event.Op(gtx.Ops, tag)
		gtx.Event(key.FocusFilter{Target: tag})
	}
	router.Frame(ops)
}

func TestNavigationDismissTargetUsesOneOverlayPriority(t *testing.T) {
	w := &Window{emojiPicker: newEmojiPickerState()}
	w.emojiPicker.visible = true
	w.showLanguageMenu = true
	w.identityPanelVisible = true
	gtx := layout.Context{
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(800, 600)},
	}

	if got := w.topNavigationDismissTarget(gtx); got != dismissIdentityPanel {
		t.Fatalf("top target = %v, want identity panel", got)
	}
	w.identityPanelVisible = false
	if got := w.topNavigationDismissTarget(gtx); got != dismissLanguageMenu {
		t.Fatalf("target after identity = %v, want language menu", got)
	}
	w.showLanguageMenu = false
	if got := w.topNavigationDismissTarget(gtx); got != dismissEmojiPicker {
		t.Fatalf("target after language = %v, want emoji picker", got)
	}
}

func TestEmojiCategoryHighlightClearsDuringGlobalSearch(t *testing.T) {
	if !emojiCategoryIsActive(emojiCategorySmileys, emojiCategorySmileys, "") {
		t.Fatal("selected category must be active with no search")
	}
	if emojiCategoryIsActive(emojiCategorySmileys, emojiCategorySmileys, "fire") {
		t.Fatal("category must not stay highlighted while global search ignores it")
	}
}

func TestRecentEmojisPersistInPreferences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "desktop.json")
	prefs := &Preferences{path: path, RecentEmojis: []string{"🔥", "😊", "🔥", "unknown"}}
	if err := prefs.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadPreferences(path)
	if err != nil {
		t.Fatalf("LoadPreferences: %v", err)
	}
	if want := []string{"🔥", "😊"}; !reflect.DeepEqual(loaded.RecentEmojis, want) {
		t.Fatalf("recent emojis = %#v, want %#v", loaded.RecentEmojis, want)
	}
}

func TestEmojiPickerStateRestoresPersistedRecents(t *testing.T) {
	state := newEmojiPickerStateWithRecents([]string{"🔥", "😊", "🔥", "unknown"})
	if want := []string{"🔥", "😊"}; !reflect.DeepEqual(state.recents, want) {
		t.Fatalf("restored recents = %#v, want %#v", state.recents, want)
	}
}

func TestComposerEditorMetricsShowsScrollbarWhenBudgetFitsOnlyThreeLines(t *testing.T) {
	// A 72px budget buys three 21px lines and 9px of a fourth: the editor
	// takes the three it can show, not the strip as well.
	height, visible, scrollbar := composerEditorMetrics(4, 72, 42, 21)
	if height != 63 || visible != 3 || !scrollbar {
		t.Fatalf("metrics = height %d, visible %d, scrollbar %v; want 63, 3, true", height, visible, scrollbar)
	}

	height, visible, scrollbar = composerEditorMetrics(4, 84, 42, 21)
	if height != 84 || visible != 4 || scrollbar {
		t.Fatalf("fitting metrics = height %d, visible %d, scrollbar %v; want 84, 4, false", height, visible, scrollbar)
	}
}

// The cap is a third of the window minus the composer's chrome, so it lands on
// a whole line only by accident. A capped editor that keeps the remainder
// draws the top slice of a line nobody can read — visible on every window size
// the app ships against.
func TestComposerEditorMetricsCapsOnWholeLines(t *testing.T) {
	const (
		lineStep = 21
		base     = 2 * lineStep
		chrome   = 26
	)
	tests := []struct {
		windowDp   int
		wantHeight int
		wantLines  int
	}{
		{windowDp: 480, wantHeight: 42, wantLines: 2},
		{windowDp: 640, wantHeight: 105, wantLines: 5},
		{windowDp: 720, wantHeight: 126, wantLines: 6},
		{windowDp: 1080, wantHeight: 252, wantLines: 12},
	}

	for _, tt := range tests {
		// The same budget layoutComposerCard computes, at PxPerDp 1.
		maxInputHeight := max(tt.windowDp/3-76, 62)
		height, visible, scrollbar := composerEditorMetrics(99, maxInputHeight-chrome, base, lineStep)
		if height != tt.wantHeight || visible != tt.wantLines {
			t.Fatalf("%ddp window: editor height %d over %d lines, want %d over %d",
				tt.windowDp, height, visible, tt.wantHeight, tt.wantLines)
		}
		if height%lineStep != 0 {
			t.Fatalf("%ddp window: editor height %d leaves a %dpx slice of a line", tt.windowDp, height, height%lineStep)
		}
		if !scrollbar {
			t.Fatalf("%ddp window: 99 lines in %d want a scrollbar", tt.windowDp, height)
		}
	}
}

func TestComposerEditorStyleUsesFixedLineHeightAndFitsBudget(t *testing.T) {
	var editor widget.Editor
	editor.SetText("one\ntwo\nthree\nfour")
	style := composerEditorStyle(newAppTheme(), &editor, "")
	if style.LineHeightScale != 1 {
		t.Fatalf("LineHeightScale = %v, want 1 for a fixed 21sp line step", style.LineHeightScale)
	}
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 1_000)},
	}
	got := style.Layout(gtx).Size.Y
	wantMax := 4 * gtx.Sp(composerEditorLineHeight)
	if got > wantMax {
		t.Fatalf("four rendered lines use %dpx, line-height budget is %dpx", got, wantMax)
	}
}

func TestRecentEmojiPreferencesAreCoalesced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "desktop.json")
	w := &Window{
		prefs:       &Preferences{path: path},
		emojiPicker: newEmojiPickerState(),
	}
	now := time.Unix(1_000, 0)
	gtx := layout.Context{Ops: new(op.Ops), Now: now}
	w.selectEmoji(gtx, "😀")
	w.selectEmoji(gtx, "🔥")

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("preferences were written synchronously on emoji tap: %v", err)
	}
	w.flushRecentEmojiPreferences(now.Add(emojiRecentSaveDelay), false)
	loaded, err := LoadPreferences(path)
	if err != nil {
		t.Fatalf("LoadPreferences: %v", err)
	}
	if want := []string{"🔥", "😀"}; !reflect.DeepEqual(loaded.RecentEmojis, want) {
		t.Fatalf("persisted recents = %#v, want %#v", loaded.RecentEmojis, want)
	}
}

// A token that appears in both the category blob and the emoji's own keywords
// cannot widen what the search accepts, so carrying it twice only costs the
// scan that finds it a second time.
func TestEmojiSearchTokensCarryNoRepeats(t *testing.T) {
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			seen := make(map[string]struct{}, len(entry.searchTokens))
			for _, token := range entry.searchTokens {
				if _, duplicate := seen[token]; duplicate {
					t.Fatalf("%s in %s repeats search token %q", entry.value, category.id, token)
				}
				seen[token] = struct{}{}
			}
		}
	}
}

func TestEmojiValuesReuseTheCatalogSlice(t *testing.T) {
	first := emojiValues(emojiCategorySmileys)
	second := emojiValues(emojiCategorySmileys)
	if len(first) == 0 {
		t.Fatal("smileys category resolved to nothing")
	}
	if &first[0] != &second[0] {
		t.Fatal("emojiValues rebuilt the category list instead of handing out the catalog's")
	}
	if cap(first) != len(first) {
		t.Fatalf("category slice len %d, cap %d: an append by a caller would write into the catalog", len(first), cap(first))
	}
	if emojiValues(emojiCategoryRecent) != nil {
		t.Fatal("recent is not a catalog category and must not resolve to one")
	}
}

func TestComposerPickerHeightUsesMeasuredFooter(t *testing.T) {
	if got := composerPickerHeight(340, 26, 42, 44, 125, 250); got != 228 {
		t.Fatalf("one-row footer picker height = %d, want 228", got)
	}
	if got := composerPickerHeight(340, 26, 42, 82, 125, 250); got != 190 {
		t.Fatalf("stacked footer picker height = %d, want 190", got)
	}
}

func TestComposerPickerHeightRefusesUnusableStrip(t *testing.T) {
	const (
		chrome   = 26
		editor   = 42
		footer   = 44
		minimum  = 125
		reserved = chrome + editor + footer
	)

	if got := composerPickerHeight(reserved+minimum, chrome, editor, footer, minimum, 250); got != minimum {
		t.Fatalf("picker height at exactly the minimum = %d, want %d", got, minimum)
	}
	if got := composerPickerHeight(reserved+minimum-1, chrome, editor, footer, minimum, 250); got != 0 {
		t.Fatalf("picker height one pixel below the minimum = %d, want 0 (deferred)", got)
	}
	if got := composerPickerHeight(reserved+1, chrome, editor, footer, minimum, 250); got != 0 {
		t.Fatalf("picker height on a sliver = %d, want 0 (deferred)", got)
	}
}

// The hover highlight covers the whole cell, so the glyph has to sit in the
// middle of it. An emoji's ink ends on the baseline and the font's descent
// below it is empty, so centring the line box used to lift every glyph 4px in
// a 38px cell — off the highlight drawn around it.
func TestEmojiChoiceCentresTheGlyphInItsCell(t *testing.T) {
	const width, height = 57, 38
	window, err := headless.NewWindow(width, height)
	if err != nil {
		t.Skipf("no headless GPU context here: %v", err)
	}
	defer window.Release()

	w := &Window{theme: newAppTheme(), language: "en", emojiPicker: newEmojiPickerState()}
	ops := new(op.Ops)
	w.layoutEmojiChoice(layout.Context{
		Ops:         ops,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(width, height)),
	}, "\U0001F600")
	if err := window.Frame(ops); err != nil {
		t.Fatalf("render frame: %v", err)
	}
	shot := image.NewRGBA(image.Rect(0, 0, width, height))
	if err := window.Screenshot(shot); err != nil {
		t.Fatalf("screenshot: %v", err)
	}

	ink := drawnBounds(shot)
	if ink.Empty() {
		t.Fatal("nothing was drawn in the cell")
	}
	// Doubled so an odd cell height needs no rounding rule of its own.
	if got, want := ink.Min.Y+ink.Max.Y, height; got < want-1 || got > want+1 {
		t.Fatalf("glyph ink spans y %d..%d in a %dpx cell: its centre is %.1f, want %.1f",
			ink.Min.Y, ink.Max.Y, height, float32(got)/2, float32(want)/2)
	}
	if got, want := ink.Min.X+ink.Max.X, width; got < want-1 || got > want+1 {
		t.Fatalf("glyph ink spans x %d..%d in a %dpx cell: its centre is %.1f, want %.1f",
			ink.Min.X, ink.Max.X, width, float32(got)/2, float32(want)/2)
	}
}

// drawnBounds is the bounding box of every pixel the frame painted over the
// transparent background it started from.
func drawnBounds(img *image.RGBA) image.Rectangle {
	bounds := img.Bounds()
	ink := image.Rectangle{Min: bounds.Max, Max: bounds.Min}
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			if _, _, _, alpha := img.At(x, y).RGBA(); alpha < 0x2000 {
				continue
			}
			ink.Min.X = min(ink.Min.X, x)
			ink.Min.Y = min(ink.Min.Y, y)
			ink.Max.X = max(ink.Max.X, x+1)
			ink.Max.Y = max(ink.Max.Y, y+1)
		}
	}
	if ink.Min.X >= ink.Max.X || ink.Min.Y >= ink.Max.Y {
		return image.Rectangle{}
	}
	return ink
}

// Nine chips need 243dp. Narrower than that the row scrolls them rather than
// shrinking them: a 15dp chip at 140dp of row, or a 4dp one at 40dp, overlaps
// nothing and can be hit by nobody. Full size at every width is also what
// keeps emojiPickerChromeHeight exact.
func TestEmojiCategoryRowKeepsChipsFullSizeAtEveryWidth(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	w := &Window{
		theme:              newAppTheme(),
		language:           "en",
		emojiCategoryIcons: icons.emojiCategories,
		emojiPicker:        newEmojiPickerState(),
	}

	for _, pxPerDp := range []float32{1, 1.5, 2} {
		for _, width := range []int{40, 90, 140, 243, 261, 400} {
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Metric:      unit.Metric{PxPerDp: pxPerDp, PxPerSp: pxPerDp},
				Constraints: layout.Constraints{Max: image.Pt(width, 200)},
			}
			dims := w.layoutEmojiCategories(gtx)
			if chip := emojiCategoryChipPx(gtx); dims.Size.Y != chip {
				t.Fatalf("at %dpx wide (PxPerDp %v) the row is %dpx tall, want a full chip of %dpx",
					width, pxPerDp, dims.Size.Y, chip)
			}
			if dims.Size.X > width {
				t.Fatalf("at %dpx wide (PxPerDp %v) the row reports %dpx and spills out of the picker",
					width, pxPerDp, dims.Size.X)
			}
		}
	}
}

// Reopening the picker keeps the selected category but not where the row was
// scrolled to, so on a narrow picker the grid showed one category while the
// row highlighted none of the chips it had room for.
func TestNarrowEmojiCategoryRowOpensOnTheSelectedChip(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}

	for _, selected := range emojiCategoryOrder {
		t.Run(string(selected), func(t *testing.T) {
			w := &Window{
				theme:              newAppTheme(),
				language:           "en",
				emojiCategoryIcons: icons.emojiCategories,
				emojiPicker:        newEmojiPickerState(),
			}
			w.emojiPicker.selectCategory(selected)
			router := new(input.Router)
			row := func(width int) layout.Context {
				return layout.Context{
					Ops:         new(op.Ops),
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Constraints{Max: image.Pt(width, 200)},
				}
			}
			// Wide enough to spread every chip: the row scrolls nowhere, so
			// the reveal has to survive to a frame that can act on it.
			w.layoutEmojiCategories(row(400))
			w.closeEmojiPicker(layout.Context{Ops: new(op.Ops)})
			w.openEmojiPicker(layout.Context{Ops: new(op.Ops)})

			gtx := row(140)
			w.layoutEmojiCategories(gtx)

			index := slices.Index(emojiCategoryOrder, selected)
			position := w.emojiPicker.categoryList.Position
			if index < position.First || index >= position.First+position.Count {
				t.Fatalf("%s is chip %d, but the row shows %d..%d", selected, index,
					position.First, position.First+position.Count-1)
			}
			if chip := emojiCategoryChipPx(gtx); position.OffsetLast >= chip {
				t.Fatalf("%s left %dpx empty after the last chip, more than the %dpx chip that could fill it",
					selected, position.OffsetLast, chip)
			}
		})
	}
}

func TestEmojiCategoryRowFirstKeepsTheRowFull(t *testing.T) {
	const count, visible = 9, 7
	tests := []struct{ selected, want int }{
		{selected: 0, want: 0},
		{selected: 1, want: 1},
		{selected: 2, want: 2},
		{selected: 5, want: 2},
		{selected: 8, want: 2},
		{selected: -1, want: 0}, // an unknown category resolves to no index
	}
	for _, tt := range tests {
		if got := emojiCategoryRowFirst(tt.selected, count, visible); got != tt.want {
			t.Fatalf("first chip for selection %d = %d, want %d", tt.selected, got, tt.want)
		}
	}
	if got := emojiCategoryRowFirst(8, count, 0); got != 8 {
		t.Fatalf("first chip in a row narrower than one chip = %d, want the selected 8", got)
	}
}

func TestNarrowEmojiCategoryRowScrolls(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	w := &Window{
		theme:              newAppTheme(),
		language:           "en",
		emojiCategoryIcons: icons.emojiCategories,
		emojiPicker:        newEmojiPickerState(),
	}
	row := func(width int) layout.Dimensions {
		return w.layoutEmojiCategories(layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(width, 200)},
		})
	}

	row(400)
	if count := w.emojiPicker.categoryList.Position.Count; count != 0 {
		t.Fatalf("a row with room for every chip used the scrolling list (%d items)", count)
	}

	row(140)
	count := w.emojiPicker.categoryList.Position.Count
	if count == 0 {
		t.Fatal("a row too narrow for nine chips did not scroll them")
	}
	if count >= len(emojiCategoryOrder) {
		t.Fatalf("the scrolling row laid out %d of %d chips: they cannot all fit 140px at full size",
			count, len(emojiCategoryOrder))
	}
}

// emojiPickerMinHeightDp is a budget for a surface drawn elsewhere, so it is
// worth proving rather than trusting: at exactly that height an emoji must
// still be reachable by a tap. If the chrome grows a row without the budget
// growing with it, the press below lands on nothing.
func TestEmojiPickerMinHeightKeepsAnEmojiTappable(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	w := &Window{
		theme:              newAppTheme(),
		language:           "en",
		searchIcon:         icons.search,
		emojiCategoryIcons: icons.emojiCategories,
		emojiPicker:        newEmojiPickerState(),
	}
	w.emojiPicker.visible = true
	router := new(input.Router)
	ops := new(op.Ops)
	metric := unit.Metric{PxPerDp: 1, PxPerSp: 1}
	measure := layout.Context{Metric: metric, Constraints: layout.Constraints{Max: image.Pt(360, 1000)}}
	height := emojiPickerMinHeight(measure)

	frame := func() {
		ops.Reset()
		gtx := layout.Context{
			Ops:         ops,
			Source:      router.Source(),
			Metric:      metric,
			Constraints: layout.Exact(image.Pt(360, height)),
		}
		w.handleEmojiActions(gtx)
		w.layoutEmojiPicker(gtx)
		router.Frame(ops)
	}

	// The only row of cells, and the first of them: the grid starts below the
	// chrome, and the row is one cell tall by construction.
	cell := int(emojiGridCellHeightDp)
	tap := f32.Pt(float32(measure.Dp(emojiPickerBorderDp)+8+cell/2), float32(emojiPickerChromeHeight(measure)+cell/2))

	frame()
	for _, kind := range []pointer.Kind{pointer.Press, pointer.Release} {
		router.Queue(pointer.Event{Kind: kind, Source: pointer.Touch, Position: tap})
	}
	frame()

	if got := w.messageEditor.Text(); got == "" {
		t.Fatalf("a tap at %v inserted nothing: the picker's minimum height no longer holds a reachable cell", tap)
	}
}

// The picker draws 87dp of its own chrome — frame, insets, category row,
// search field, spacers — before a single emoji appears. A composer that can
// spare less than that plus one 38dp row must not open it: the surface would
// be a clipped strip with nothing tappable in it, or, at the bottom end, an
// invisible one that is still the top Escape/Back target.
func TestShortComposerDefersEmojiPickerInsteadOfDrawingAStrip(t *testing.T) {
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	w := &Window{
		theme:              newAppTheme(),
		language:           "en",
		attachIcon:         icons.attach,
		emojiIcon:          icons.emoji,
		sendIcon:           icons.send,
		searchIcon:         icons.search,
		emojiCategoryIcons: icons.emojiCategories,
		emojiPicker:        newEmojiPickerState(),
	}
	const (
		maxInputHeight = 120
		footerReserve  = 44
	)
	card := func(height int) layout.Dimensions {
		var router input.Router
		return w.messageInputCard(layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(360, height)},
		}, domain.PeerIdentity{}, maxInputHeight, footerReserve)
	}

	// What the composer is without a picker; neither part depends on the
	// window height, so it stays the baseline for every case below.
	closed := card(600).Size.Y
	w.emojiPicker.visible = true

	minimum := emojiPickerMinHeight(layout.Context{
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(360, 1000)},
	})
	if got := card(closed + footerReserve + minimum); got.Size.Y != closed+minimum {
		t.Fatalf("card height with exactly enough room = %d, want %d", got.Size.Y, closed+minimum)
	}
	if got := card(closed + footerReserve + minimum - 1); got.Size.Y != closed {
		t.Fatalf("card height one pixel short = %d, want %d (composer only)", got.Size.Y, closed)
	}
	if !w.emojiPicker.visible {
		t.Fatal("a deferred picker was closed; the draw is deferred, not cancelled")
	}
}

func TestComposerSendActionExplainsBlockingStates(t *testing.T) {
	tests := []struct {
		name       string
		recipient  bool
		pending    bool
		hasContent bool
		wantEnable bool
		wantReason string
	}{
		{name: "no chat", hasContent: true, wantReason: "compose.select_first"},
		{name: "wipe pending", recipient: true, pending: true, hasContent: true, wantReason: "compose.send_blocked_during_wipe"},
		{name: "empty draft", recipient: true},
		{name: "ready", recipient: true, hasContent: true, wantEnable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enabled, reason := composerSendActionState(tt.recipient, tt.pending, tt.hasContent)
			if enabled != tt.wantEnable || reason != tt.wantReason {
				t.Fatalf("state = (%v, %q), want (%v, %q)", enabled, reason, tt.wantEnable, tt.wantReason)
			}
		})
	}
}

func TestBlockedComposerSendButtonRendersVisibleReason(t *testing.T) {
	loadedIcons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("loadWindowIcons: %v", err)
	}
	w := &Window{theme: newAppTheme(), sendIcon: loadedIcons.send}
	layoutButton := func(showReason bool) layout.Dimensions {
		return w.layoutComposerSendButton(layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(300, 100)},
		}, false, "Select Client First", showReason)
	}

	iconOnly := layoutButton(false)
	withReason := layoutButton(true)
	if withReason.Size.X <= iconOnly.Size.X {
		t.Fatalf("blocked send button width = %d, icon-only width = %d; reason text is not visibly rendered", withReason.Size.X, iconOnly.Size.X)
	}
}
