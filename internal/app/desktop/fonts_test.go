package desktop

import (
	"encoding/binary"
	"slices"
	"strings"
	"testing"
	"unicode"

	"gioui.org/font"

	"github.com/go-text/typesetting/fontscan"
	"github.com/go-text/typesetting/harfbuzz"
)

// TestBundledEmojiFontIsABitmapBuild is the guard on the emoji file itself.
//
// Gio draws outline, SVG and bitmap glyphs and silently skips everything else,
// so a COLR build — which is what Google Fonts serves for Noto Color Emoji,
// and what Windows ships as Segoe UI Emoji — renders as blank space. That is
// the bug this font was bundled to fix, and swapping the file for a COLR one
// would bring it straight back with nothing failing to say so.
func TestBundledEmojiFontIsABitmapBuild(t *testing.T) {
	t.Parallel()

	tables := sfntTableTags(t, emojiFontTTF)

	bitmapTables := []string{"CBDT", "sbix", "EBDT"}
	if !slices.ContainsFunc(bitmapTables, func(tag string) bool { return slices.Contains(tables, tag) }) {
		t.Fatalf("the bundled emoji font has none of %v; its tables are %v.\n"+
			"Gio renders only outline, SVG and bitmap glyphs — a COLR build shows nothing at all.",
			bitmapTables, tables)
	}
}

// TestBundledEmojiFontCoversThePicker shapes every emoji the picker offers and
// fails on the ones the font cannot draw.
//
// Shaping, not a cmap lookup: flags are two regional indicators joined by a
// ligature, and the rainbow flag is a ZWJ sequence, so "every rune is in the
// font" is not the same question as "this emoji has a glyph". Glyph 0 is
// .notdef — the answer a font gives for something it does not have.
func TestBundledEmojiFontCoversThePicker(t *testing.T) {
	t.Parallel()

	shaper := shaperFor(t, emojiFont)

	var missing []string
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			if !drawable(shaper, entry.value) {
				missing = append(missing, entry.value)
			}
		}
	}

	if len(missing) > 0 {
		t.Fatalf("the bundled emoji font cannot draw %d of the picker's emoji: %s.\n"+
			"Either the font is older than the emoji, or the picker offers something it never had.",
			len(missing), strings.Join(missing, " "))
	}
}

// TestBundledFontsDrawEveryTranslatedRune is the guard the Arabic interface was
// missing: it shipped translated, and every glyph of it came out as a hollow
// box because no font in the binary — nor, on that device, on the host — had a
// single Arabic letter.
//
// The question is asked of the strings that actually reach the screen rather
// than of a sample alphabet, so a translator reaching for a letter outside the
// bundled font's coverage fails here instead of in front of the user. Only the
// runes belonging to the script the font is bundled for are its business; the
// Latin and the format verbs mixed into those strings are the Go faces' job.
func TestBundledFontsDrawEveryTranslatedRune(t *testing.T) {
	t.Parallel()

	coverage := []struct {
		bundled  bundledFont
		language string
		scripts  []*unicode.RangeTable
	}{
		{bundled: arabicFont, language: "ar", scripts: []*unicode.RangeTable{unicode.Arabic}},
		{
			bundled:  cjkFont,
			language: "zh",
			scripts: []*unicode.RangeTable{
				unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul,
			},
		},
	}

	for _, want := range coverage {
		t.Run(want.language, func(t *testing.T) {
			t.Parallel()

			shaper := shaperFor(t, want.bundled)

			var missing []string
			for _, message := range messages[want.language] {
				for _, r := range message {
					if !unicode.IsOneOf(want.scripts, r) {
						continue
					}
					if drawable(shaper, string(r)) {
						continue
					}
					if !slices.Contains(missing, string(r)) {
						missing = append(missing, string(r))
					}
				}
			}

			if len(missing) > 0 {
				t.Fatalf("the bundled %s font cannot draw %d rune(s) used by the %q translation: %s.\n"+
					"Every one of them reaches the user as a hollow .notdef box.",
					want.bundled.typeface, len(missing), want.language, strings.Join(missing, " "))
			}
		})
	}
}

// TestBundledFamiliesAreNotGenericFamilies is the guard on the NAMES.
//
// The font matcher treats a handful of family names as generic — "serif",
// "monospace", "emoji" and friends — and its substitution table indexes the
// host's own fonts under them. Registering the bundled emoji font as "emoji"
// was therefore not registering a family at all but joining a queue behind
// whatever the system calls its emoji font: on Windows the request went to
// Segoe UI Emoji, whose COLR glyphs Gio does not draw, and the emoji stayed
// blank with the bundled font sitting unused in the binary.
func TestBundledFamiliesAreNotGenericFamilies(t *testing.T) {
	t.Parallel()

	generic := []string{
		fontscan.Serif,
		fontscan.SansSerif,
		fontscan.Monospace,
		fontscan.Cursive,
		fontscan.Fantasy,
		fontscan.Math,
		fontscan.Emoji,
	}
	for _, bundled := range bundledFonts {
		for _, family := range generic {
			if strings.EqualFold(string(bundled.typeface), family) {
				t.Fatalf("a bundled font is registered as %q, which is a generic family: "+
					"the host's own font is indexed under that name and wins the request", family)
			}
		}
	}
}

// TestAppFontCollectionCarriesEveryBundledFamily pins what the window's shaper
// is built from. The theme asks for the families by name, and a family the
// shaper was never given is resolved against the host's fonts instead — which
// on Windows is the COLR font that started all this, and on Android is nothing
// at all.
func TestAppFontCollectionCarriesEveryBundledFamily(t *testing.T) {
	t.Parallel()

	collection := appFontCollection()

	faces := make(map[font.Typeface]int, len(collection))
	for _, face := range collection {
		faces[face.Font.Typeface]++
	}

	for _, bundled := range bundledFonts {
		if got := faces[bundled.typeface]; got != 1 {
			t.Errorf("faces of %q in the collection = %d, want exactly 1", bundled.typeface, got)
		}
	}
	if faces[textTypeface]+faces[font.Typeface("Go Mono")] == 0 {
		t.Fatal("the Go text faces are gone from the collection")
	}
}

// TestAppThemeFaceNamesEveryBundledFamily covers the half of the wiring the
// collection cannot: a face the shaper holds but the theme never asks for is
// reached only by the matcher's own fallback, behind whatever the host offers
// for that script. The text family has to come first, or the Go faces stop
// being what plain text is drawn in.
func TestAppThemeFaceNamesEveryBundledFamily(t *testing.T) {
	t.Parallel()

	families := strings.Split(string(appThemeFace()), ",")
	for i := range families {
		families[i] = strings.TrimSpace(families[i])
	}

	if len(families) == 0 || families[0] != string(textTypeface) {
		t.Fatalf("theme face = %q, want %q first", appThemeFace(), textTypeface)
	}
	for _, bundled := range bundledFonts {
		if !slices.Contains(families, string(bundled.typeface)) {
			t.Errorf("theme face = %q, want it to name %q", appThemeFace(), bundled.typeface)
		}
	}
}

// TestAppFontCollectionDoesNotMutateGofont covers the sharing hazard behind
// the clone in appFontCollection: gofont.Collection hands out its own
// package-level slice, so appending to it would leave our faces in the state
// every other caller of that package sees.
func TestAppFontCollectionDoesNotMutateGofont(t *testing.T) {
	t.Parallel()

	first := len(appFontCollection())
	second := len(appFontCollection())
	if first != second {
		t.Fatalf("collection length grew between calls: %d then %d", first, second)
	}
}

// shaperFor parses a bundled font and returns a shaper over it.
func shaperFor(t *testing.T, bundled bundledFont) *harfbuzz.Font {
	t.Helper()

	face, err := bundled.parse()
	if err != nil {
		t.Fatalf("parse the bundled %s font: %v", bundled.typeface, err)
	}
	return harfbuzz.NewFont(face.Face.Face())
}

// drawable reports whether a font has a glyph for every part of text. Glyph 0
// is .notdef — the answer a font gives for something it does not have, and the
// hollow box the user sees.
func drawable(shaper *harfbuzz.Font, text string) bool {
	buffer := harfbuzz.NewBuffer()
	buffer.AddRunes([]rune(text), 0, -1)
	buffer.GuessSegmentProperties()
	buffer.Shape(shaper, nil)

	for _, glyph := range buffer.Info {
		if glyph.Glyph == 0 {
			return false
		}
	}
	return true
}

// sfntTableTags returns the four-character tags of an sfnt file's tables. The
// directory is read by hand so that the check above depends on the bytes that
// ship, not on what a font library chose to expose.
func sfntTableTags(t *testing.T, data []byte) []string {
	t.Helper()

	const (
		headerSize = 12
		recordSize = 16
	)
	if len(data) < headerSize {
		t.Fatalf("the bundled font is %d bytes, too short to be an sfnt file", len(data))
	}

	count := int(binary.BigEndian.Uint16(data[4:6]))
	if len(data) < headerSize+count*recordSize {
		t.Fatalf("the bundled font claims %d tables but is only %d bytes", count, len(data))
	}

	tags := make([]string, 0, count)
	for i := range count {
		record := headerSize + i*recordSize
		tags = append(tags, string(data[record:record+4]))
	}
	return tags
}
