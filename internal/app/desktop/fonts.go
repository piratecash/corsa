package desktop

import (
	_ "embed"
	"fmt"
	"slices"
	"strings"
	"sync"

	"gioui.org/font"
	"gioui.org/font/gofont"
	"gioui.org/font/opentype"

	"github.com/rs/zerolog/log"
)

// Every script the Go faces do not cover travels inside the binary instead of
// being taken from the host, because the host's font set is not something this
// application can rely on.
//
// The rule was learnt on emoji and then again on Arabic. Gio draws three kinds
// of glyph — outlines, SVG and BITMAPS — and skips everything else without a
// word, so the Windows emoji font, whose colour glyphs live in a COLR table,
// rendered as blank space and the regional-indicator pairs behind flags fell
// back to their plain letter outlines: "UA", "DE". Windows ships no flag
// glyphs at all, so no fix on the host side could have produced them.
//
// Arabic failed the other way. Nothing was wrong with the renderer: the font
// simply was not there. go-text scans /system/fonts, /system/font and
// /data/fonts on Android and nothing else, so on a device that keeps its
// Arabic face anywhere else — /product/fonts, a vendor overlay, a mounted font
// module — the shaper found no glyph for a single Arabic letter and drew
// .notdef, one hollow box per code point, for the whole interface.
//
// Hence three bundled files, and hence the licence that travels with them as
// assets/fonts/OFL.txt. All three are SIL Open Font License 1.1.

// The emoji font MUST stay a bitmap (CBDT/sbix) build, for the reason above:
// the Noto Color Emoji served by Google Fonts is COLRv1 and would reintroduce
// exactly the bug this file exists to close, which is why
// TestBundledEmojiFontIsABitmapBuild refuses anything else.
//
//go:embed assets/fonts/NotoColorEmoji.ttf
var emojiFontTTF []byte

//go:embed assets/fonts/NotoSansArabic-Regular.ttf
var arabicFontTTF []byte

// The SC cut of Noto Sans CJK carries the whole shared CJK glyph set — the
// language-specific cuts differ in which variant they select by default, not
// in what they cover — so one file answers for Chinese, Japanese and Korean
// alike. It is a single face on purpose: the .ttc that ships the ten cuts
// together would have every one of them parsed at startup to use one.
//
//go:embed assets/fonts/NotoSansCJKsc-Regular.otf
var cjkFontOTF []byte

// The family names below are deliberately neither generic nor the vendor's.
//
// "emoji", like "serif" and "monospace", is a GENERIC family in the font
// matcher, and the platform's own emoji font is indexed under it by the
// fontconfig substitution table — so asking for "emoji" handed the request to
// Segoe UI Emoji on Windows and the bundled font was never reached.
// Registering under a name nothing else can claim makes the exact-family match
// land here every time. Not the vendor's name either, so replacing a file — a
// newer Noto, or another font entirely — changes nothing but the file.
const (
	emojiTypeface  = font.Typeface("Corsa Emoji")
	arabicTypeface = font.Typeface("Corsa Arabic")
	cjkTypeface    = font.Typeface("Corsa CJK")

	// textTypeface is the family that carries Latin, Cyrillic and Greek. It
	// comes from gofont rather than from assets/fonts.
	textTypeface = font.Typeface("Go")
)

// bundledFont is one font file that ships inside the binary, together with the
// family name it is registered under and the parse that hands it to a shaper.
type bundledFont struct {
	// parse yields the face once for the whole process. opentype.Face is
	// documented as thread-safe and meant to be shared between shapers, and
	// every window builds a shaper of its own (see newAppTheme), so parsing
	// megabytes per window would buy nothing.
	parse    func() (font.FontFace, error)
	source   []byte
	typeface font.Typeface
}

func newBundledFont(typeface font.Typeface, source []byte) bundledFont {
	return bundledFont{
		typeface: typeface,
		source:   source,
		parse: sync.OnceValues(func() (font.FontFace, error) {
			face, err := opentype.Parse(source)
			if err != nil {
				return font.FontFace{}, fmt.Errorf("parse the bundled %s font: %w", typeface, err)
			}
			return font.FontFace{Font: font.Font{Typeface: typeface}, Face: face}, nil
		}),
	}
}

var (
	emojiFont  = newBundledFont(emojiTypeface, emojiFontTTF)
	arabicFont = newBundledFont(arabicTypeface, arabicFontTTF)
	cjkFont    = newBundledFont(cjkTypeface, cjkFontOTF)

	// bundledFonts is the fallback chain behind the text face, in the order
	// the shaper is asked to try them. Emoji first because its coverage is the
	// narrowest and the least likely to collide; the two script fonts follow.
	bundledFonts = []bundledFont{emojiFont, arabicFont, cjkFont}
)

// appFontCollection is the font set every window's shaper is built from: the
// bundled Go faces for text, and the bundled script faces behind them.
//
// A font the shaper is given by hand wins over the host's fonts for an exact
// family match, so a request for one of the families above reaches this file
// rather than whatever the operating system has under that name — or nothing
// at all, on a host that has no such font.
//
// A font that fails to parse is logged and dropped rather than fatal: it is
// embedded, so a failure here is a broken build rather than a broken host, and
// losing one script is not a reason to leave the user without any text at all.
func appFontCollection() []font.FontFace {
	// gofont.Collection returns its own package-level slice; appending to it
	// directly would write our faces into state shared with every other caller.
	collection := slices.Clone(gofont.Collection())

	for _, bundled := range bundledFonts {
		face, err := bundled.parse()
		if err != nil {
			log.Error().Err(err).
				Str("family", string(bundled.typeface)).
				Msg("bundled font unusable, the script it carries will not render")
			continue
		}

		// Logged because the failure mode this file exists for is invisible: a
		// build that draws blank glyphs looks exactly like one that draws
		// none, and the operator needs to be able to tell from the log whether
		// the font in use is the one shipped with the binary.
		log.Info().
			Str("family", string(bundled.typeface)).
			Int("bytes", len(bundled.source)).
			Msg("font loaded from the binary")

		collection = append(collection, face)
	}

	return collection
}

// appThemeFace is the family list the theme asks for by name: the Go faces for
// text, then every bundled family as a fallback.
//
// Built from bundledFonts rather than written out, so that a font added to the
// collection cannot be left out of the request that reaches it — a family the
// theme never names is resolved against the host's fonts instead.
func appThemeFace() font.Typeface {
	families := make([]string, 0, len(bundledFonts)+1)
	families = append(families, string(textTypeface))
	for _, bundled := range bundledFonts {
		families = append(families, string(bundled.typeface))
	}
	return font.Typeface(strings.Join(families, ", "))
}
