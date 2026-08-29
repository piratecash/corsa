package desktop

import (
	"gioui.org/io/system"
	"gioui.org/layout"

	"golang.org/x/text/unicode/bidi"
)

// Three kinds of text share one window, and the interface language answers for
// only one of them.
//
// TRANSLATED INTERFACE text runs in the direction of the language on screen.
// That is what the frame's locale carries (textLocale, i18n.go) and it is the
// default every subtree inherits.
//
// TECHNICAL text is not language at all: identities, `corsa:` addresses, file
// names, byte counts, timestamps, and everything the node says about itself in
// the console. It is written left to right in every locale. Laying it out
// right to left does more than move it across the card: inside an editor it
// reverses which way the caret and the arrow keys travel, so correcting a
// fingerprint in the Arabic interface would move the caret backwards through a
// string that has no right-to-left character in it.
//
// FREE USER TEXT — a message, a draft, a contact's alias — belongs to whoever
// wrote it rather than to whoever is reading it. An Arabic message has to read
// right to left inside an English interface, and an English one left to right
// inside an Arabic one, so its direction comes from the text itself.
//
// layout.Context is a value, so an override reaches the subtree it is passed
// to and nothing else. Both helpers below therefore return a context rather
// than changing one.

// leftToRight lays a subtree out left to right whatever the interface language
// is. It is for technical text — see above for what that means and why.
func leftToRight(gtx layout.Context) layout.Context {
	gtx.Locale.Direction = system.LTR
	return gtx
}

// directedByContent lays a subtree out in the direction of the text it is
// about to draw, and leaves the interface language's direction in place for
// text that says nothing about direction — an empty draft, a row of digits, a
// file name.
//
// ONE direction for the whole text, which is coarser than UAX#9: the algorithm
// resolves a base direction per PARAGRAPH, so in "hello\nمرحبا" the second
// line should stand on its own and be laid out right to left. It is not,
// because a Gio text widget carries a single Locale — widget.Label and
// widget.Editor build one text.Parameters for everything they hold, and the
// wrapper takes one base direction with it. What the text does keep is its
// internal ordering: go-text still resolves bidi runs inside that base, so the
// Arabic line reads correctly and loses its alignment and the side its
// trailing punctuation settles on.
//
// Buying the paragraph would mean one widget per paragraph. For a label that
// is layout noise; for the message body and the composer it is worse than the
// defect — a Selectable per paragraph cannot be selected or copied across a
// line break, and an Editor per paragraph is not an editor. So the first
// paragraph with a direction of its own decides for the rest, and a document
// that mixes directions is laid out under whichever of them speaks first.
func directedByContent(gtx layout.Context, text string) layout.Context {
	if direction, ok := contentDirection(text); ok {
		gtx.Locale.Direction = direction
	}
	return gtx
}

// contentDirection is the first-strong-character rule of UAX#9 (P2/P3): text
// takes the direction of its first strong character, and text with no strong
// character has no direction of its own — which is what the second return
// value reports.
//
// It is asked of a whole string rather than of one paragraph, and the scan
// crosses line breaks: in "123\nمرحبا" the digits decide nothing and the
// Arabic below them does. See directedByContent for why the answer is one per
// widget and what that costs.
//
// "Strong" comes from the Unicode bidi property tables rather than from a list
// of scripts written out here. A hand-kept list is wrong the moment Unicode
// adds a right-to-left script — Yezidi and Garay were already missing from one
// — and it has no place at all for the invisible marks that exist purely to
// answer this question: U+200E is L, U+200F is R, and U+061C, the Arabic letter
// mark, is AL. The tables know all of them, so the classes are read and nothing
// is enumerated here.
//
// Digits stay out of it in both alphabets: European digits are EN and
// Arabic-Indic ones AN, and neither class is strong, so a line of numbers keeps
// the interface language's direction instead of dragging a technical string
// right to left.
//
// P2 also says to skip whatever sits between an isolate initiator and its
// matching PDI, which is the whole point of an isolate: text wrapped in one is
// declared not to speak for the paragraph around it. Without the depth counter
// below, "\u2066hello\u2069مرحبا" would answer LTR on a word its author
// explicitly isolated, when the first character that speaks for the paragraph
// is Arabic. An unmatched PDI closes nothing, and an isolate left open swallows
// the rest of the string — both are what the algorithm prescribes.
func contentDirection(text string) (system.TextDirection, bool) {
	isolated := 0

	for _, r := range text {
		properties, _ := bidi.LookupRune(r)
		switch properties.Class() {
		case bidi.LRI, bidi.RLI, bidi.FSI:
			isolated++
		case bidi.PDI:
			if isolated > 0 {
				isolated--
			}
		case bidi.L:
			if isolated == 0 {
				return system.LTR, true
			}
		case bidi.R, bidi.AL:
			if isolated == 0 {
				return system.RTL, true
			}
		}
	}

	return system.LTR, false
}

// inReadingOrder returns a horizontal row's children in the order the locale
// reads them: unchanged left to right, reversed right to left.
//
// layout.Flex places children in the order it is given them and knows nothing
// about direction, so a row written as "label, name, ID, fingerprint" keeps
// exactly that arrangement in Arabic — the label stranded on the left, the
// value it introduces stranded on the right, and the flexed child's own text
// pushed to the far side of its box because right-to-left text starts there.
// The row does not overflow anything; it simply reads backwards, which looks
// like the field has collided with the card around it.
//
// Reversal is enough for a row whose children are symmetric — labels, values
// and even spacers, which have no side of their own. A row whose children are
// NOT symmetric (an icon that must stay on the leading edge, an inset that is
// larger on one side) needs its own decision and must not be handed here.
//
// It returns a new slice: the caller's own is left alone, which matters
// because rows are built once and laid out every frame.
func inReadingOrder(gtx layout.Context, children ...layout.FlexChild) []layout.FlexChild {
	if gtx.Locale.Direction.Progression() != system.TowardOrigin {
		return children
	}

	reversed := make([]layout.FlexChild, len(children))
	for i, child := range children {
		reversed[len(children)-1-i] = child
	}
	return reversed
}
