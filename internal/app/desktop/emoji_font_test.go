package desktop

import (
	"encoding/binary"
	"slices"
	"strings"
	"testing"

	"gioui.org/font"

	"github.com/go-text/typesetting/fontscan"
	"github.com/go-text/typesetting/harfbuzz"
)

// TestBundledEmojiFontIsABitmapBuild is the guard on the file itself.
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

	face, err := emojiFontFace()
	if err != nil {
		t.Fatalf("parse the bundled emoji font: %v", err)
	}
	shaper := harfbuzz.NewFont(face.Face.Face())

	var missing []string
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			buffer := harfbuzz.NewBuffer()
			buffer.AddRunes([]rune(entry.value), 0, -1)
			buffer.GuessSegmentProperties()
			buffer.Shape(shaper, nil)

			for _, glyph := range buffer.Info {
				if glyph.Glyph == 0 {
					missing = append(missing, entry.value)
					break
				}
			}
		}
	}

	if len(missing) > 0 {
		t.Fatalf("the bundled emoji font cannot draw %d of the picker's emoji: %s.\n"+
			"Either the font is older than the emoji, or the picker offers something it never had.",
			len(missing), strings.Join(missing, " "))
	}
}

// TestEmojiFamilyIsNotAGenericFamily is the guard on the NAME.
//
// The font matcher treats a handful of family names as generic — "serif",
// "monospace", "emoji" and friends — and its substitution table indexes the
// host's own fonts under them. Registering the bundled font as "emoji" was
// therefore not registering a family at all but joining a queue behind
// whatever the system calls its emoji font: on Windows the request went to
// Segoe UI Emoji, whose COLR glyphs Gio does not draw, and the emoji stayed
// blank with the bundled font sitting unused in the binary.
func TestEmojiFamilyIsNotAGenericFamily(t *testing.T) {
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
	for _, family := range generic {
		if strings.EqualFold(string(emojiTypeface), family) {
			t.Fatalf("the bundled font is registered as %q, which is a generic family: "+
				"the host's own font is indexed under that name and wins the request", family)
		}
	}
}

// TestAppFontCollectionCarriesBothFamilies pins what the window's shaper is
// built from. The theme asks for the two families by name, and a family the
// shaper was never given is resolved against the host's fonts instead — which
// on Windows is the COLR font that started all this.
func TestAppFontCollectionCarriesBothFamilies(t *testing.T) {
	t.Parallel()

	collection := appFontCollection()

	var goFaces, emojiFaces int
	for _, face := range collection {
		switch face.Font.Typeface {
		case emojiTypeface:
			emojiFaces++
		case font.Typeface("Go"), font.Typeface("Go Mono"):
			goFaces++
		}
	}
	if emojiFaces != 1 {
		t.Fatalf("emoji faces in the collection = %d, want exactly 1", emojiFaces)
	}
	if goFaces == 0 {
		t.Fatal("the Go text faces are gone from the collection")
	}
}

// TestAppFontCollectionDoesNotMutateGofont covers the sharing hazard behind
// the clone in appFontCollection: gofont.Collection hands out its own
// package-level slice, so appending to it would leave our emoji face in the
// state every other caller of that package sees.
func TestAppFontCollectionDoesNotMutateGofont(t *testing.T) {
	t.Parallel()

	first := len(appFontCollection())
	second := len(appFontCollection())
	if first != second {
		t.Fatalf("collection length grew between calls: %d then %d", first, second)
	}
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
