package desktop

import (
	_ "embed"
	"fmt"
	"slices"
	"sync"

	"gioui.org/font"
	"gioui.org/font/gofont"
	"gioui.org/font/opentype"

	"github.com/rs/zerolog/log"
)

// The emoji font travels inside the binary instead of being taken from the
// host, because the platform fonts are not interchangeable for this renderer.
//
// Gio draws three kinds of glyph — outlines, SVG and BITMAPS — and skips
// everything else without a word. The Windows emoji font, Segoe UI Emoji,
// keeps its colour glyphs in a COLR table, which is none of those three: on
// Windows every emoji came out as blank space, while the regional-indicator
// pairs behind flags fell back to their plain letter outlines and rendered as
// "UA", "DE" and so on. Windows ships no flag glyphs at all, so no fix on the
// host side could have produced them.
//
// Hence a bundled font, and hence a CBDT one: the file MUST stay a bitmap
// (CBDT/sbix) build. The Noto Color Emoji served by Google Fonts is COLRv1 and
// would reintroduce exactly the bug this file exists to close, which is why
// TestBundledEmojiFontIsABitmapBuild refuses anything else.
//
// Noto Color Emoji, SIL Open Font License 1.1 — the licence travels with it as
// assets/fonts/OFL.txt.
//
//go:embed assets/fonts/NotoColorEmoji.ttf
var emojiFontTTF []byte

// emojiTypeface is the family name the bundled font is registered under.
//
// The name is deliberately not "emoji". That one is a GENERIC family in the
// font matcher, like "serif" or "monospace", and the platform's own emoji font
// is indexed under it by the fontconfig substitution table — so asking for
// "emoji" handed the request to Segoe UI Emoji on Windows and the bundled font
// was never reached. Registering under a name nothing else can claim makes the
// exact-family match land here every time.
//
// The name is also not the vendor's, so replacing the file — a newer Noto, or
// another font entirely — changes nothing but the file.
const emojiTypeface = font.Typeface("Corsa Emoji")

// emojiFontFace parses the bundled font once for the whole process.
//
// opentype.Face is documented as thread-safe and meant to be shared between
// shapers, and every window builds a shaper of its own (see newAppTheme), so
// parsing ten megabytes per window would buy nothing.
var emojiFontFace = sync.OnceValues(func() (font.FontFace, error) {
	face, err := opentype.Parse(emojiFontTTF)
	if err != nil {
		return font.FontFace{}, fmt.Errorf("parse the bundled emoji font: %w", err)
	}
	return font.FontFace{Font: font.Font{Typeface: emojiTypeface}, Face: face}, nil
})

// appFontCollection is the font set every window's shaper is built from: the
// bundled Go faces for text, and the emoji family behind them.
//
// A font the shaper is given by hand wins over the host's fonts for an exact
// family match, so asking for "emoji" reaches this file rather than whatever
// the operating system calls its emoji font.
//
// A font that fails to parse is logged and dropped rather than fatal: it is
// embedded, so a failure here is a broken build rather than a broken host, and
// losing emoji is not a reason to leave the user without any text at all.
func appFontCollection() []font.FontFace {
	// gofont.Collection returns its own package-level slice; appending to it
	// directly would write our face into state shared with every other caller.
	collection := slices.Clone(gofont.Collection())

	face, err := emojiFontFace()
	if err != nil {
		log.Error().Err(err).Msg("bundled emoji font unusable, emoji will not render")
		return collection
	}

	// Logged because the failure mode this file exists for is invisible: a
	// build that draws blank emoji looks exactly like one that draws none, and
	// the operator needs to be able to tell from the log whether the font in
	// use is the one shipped with the binary.
	log.Info().
		Str("family", string(emojiTypeface)).
		Int("bytes", len(emojiFontTTF)).
		Msg("emoji font loaded from the binary")

	return append(collection, face)
}
