package desktop

import (
	"testing"

	"gioui.org/io/system"
)

// TestTextLocaleDirection pins the direction every supported language is laid
// out in. Arabic is the reason the function exists; the rest are here so that
// adding a language without a direction cannot pass unnoticed.
func TestTextLocaleDirection(t *testing.T) {
	t.Parallel()

	want := map[string]system.TextDirection{
		"en": system.LTR,
		"ru": system.LTR,
		"es": system.LTR,
		"fr": system.LTR,
		"ar": system.RTL,
		"zh": system.LTR,
	}

	for _, option := range supportedLanguages {
		expected, known := want[option.Code]
		if !known {
			t.Errorf("language %q has no expected direction: decide which way it reads "+
				"and add it here before it reaches the user", option.Code)
			continue
		}
		if got := textLocale(option.Code).Direction; got != expected {
			t.Errorf("direction of %q = %v, want %v", option.Code, got, expected)
		}
	}
}

// TestTextLocaleLanguageIsTheNormalizedCode keeps the locale reporting the
// language actually on screen. An unknown code falls back to the English
// interface, so it has to report English rather than itself.
func TestTextLocaleLanguageIsTheNormalizedCode(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"ar":      "ar",
		"AR":      "ar",
		"  ar  ":  "ar",
		"":        "en",
		"klingon": "en",
	}

	for code, want := range cases {
		if got := textLocale(code).Language; got != want {
			t.Errorf("language of %q = %q, want %q", code, got, want)
		}
	}
}

// TestTextDirectionsOnlyNamesSupportedLanguages guards the exceptions table
// against a code that no longer exists: an entry nothing normalizes to is dead
// weight that reads as coverage.
func TestTextDirectionsOnlyNamesSupportedLanguages(t *testing.T) {
	t.Parallel()

	supported := make(map[string]struct{}, len(supportedLanguages))
	for _, option := range supportedLanguages {
		supported[option.Code] = struct{}{}
	}

	for code := range textDirections {
		if _, ok := supported[code]; !ok {
			t.Errorf("textDirections names %q, which is not a supported language", code)
		}
	}
}
