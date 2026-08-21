package desktop

import (
	"strings"
	"testing"
)

// TestPluralFormFor pins the rules that make a counted phrase readable.
// Russian is the reason this exists — "2 ждут" and "1 сообщений" are both
// wrong — and its boundaries (11–14, and every hundred after) are exactly
// where a naive n==1 check breaks.
func TestPluralFormFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lang  string
		count int
		want  pluralForm
	}{
		{"ru", 1, pluralOne},
		{"ru", 2, pluralFew},
		{"ru", 4, pluralFew},
		{"ru", 5, pluralMany},
		{"ru", 11, pluralMany},
		{"ru", 12, pluralMany},
		{"ru", 14, pluralMany},
		{"ru", 21, pluralOne},
		{"ru", 22, pluralFew},
		{"ru", 25, pluralMany},
		{"ru", 111, pluralMany},
		{"ru", 121, pluralOne},

		{"en", 0, pluralOther},
		{"en", 1, pluralOne},
		{"en", 2, pluralOther},
		{"es", 1, pluralOne},
		{"es", 2, pluralOther},

		// French counts zero with one.
		{"fr", 0, pluralOne},
		{"fr", 1, pluralOne},
		{"fr", 2, pluralOther},

		{"ar", 0, pluralZero},
		{"ar", 1, pluralOne},
		{"ar", 2, pluralTwo},
		{"ar", 3, pluralFew},
		{"ar", 10, pluralFew},
		{"ar", 11, pluralMany},
		{"ar", 99, pluralMany},
		{"ar", 100, pluralOther},

		// No grammatical number.
		{"zh", 1, pluralOther},
		{"zh", 5, pluralOther},

		// An unknown language falls back to the English rule.
		{"pt", 1, pluralOne},
		{"pt", 3, pluralOther},

		// The grammar of a negative count follows its magnitude.
		{"ru", -2, pluralFew},
	}

	for _, tc := range tests {
		if got := pluralFormFor(tc.lang, tc.count); got != tc.want {
			t.Errorf("pluralFormFor(%q, %d) = %q, want %q", tc.lang, tc.count, got, tc.want)
		}
	}
}

// TestTranslateCountAgreesWithTheNumber is the user-visible half: the
// words around the count must agree with it in every shipped language.
func TestTranslateCountAgreesWithTheNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lang     string
		count    int
		contains string
	}{
		{"ru", 1, "1 сообщение ждёт"},
		{"ru", 2, "2 сообщения ждут"},
		{"ru", 5, "5 сообщений ждут"},
		{"ru", 21, "21 сообщение ждёт"},
		{"en", 1, "1 message waiting"},
		{"en", 3, "3 messages waiting"},
	}

	for _, tc := range tests {
		got := translateCount(tc.lang, "chat.deletes_pending", tc.count)
		if !strings.Contains(got, tc.contains) {
			t.Errorf("translateCount(%q, %d) = %q, want it to contain %q", tc.lang, tc.count, got, tc.contains)
		}
	}
}

// TestTranslateCountFallsBackToOther pins the catalogue contract: a
// language missing a form renders an awkward phrase, never a raw key.
func TestTranslateCountFallsBackToOther(t *testing.T) {
	t.Parallel()

	// Chinese declares only `other`; asking for a form it does not have
	// must still produce the Chinese sentence.
	got := translateCount("zh", "chat.deletes_pending", 1)
	if !strings.Contains(got, "条消息") {
		t.Errorf("translateCount(zh, 1) = %q, want the Chinese phrase", got)
	}

	// A key with no plural entries at all falls through to the plain
	// catalogue rather than rendering "key.other".
	if got := translateCount("en", "chat.peers", 4); got != "Known peers: 4" {
		t.Errorf("translateCount on a non-plural key = %q, want the plain entry", got)
	}
}
