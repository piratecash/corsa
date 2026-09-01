package desktop

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// TestEveryLanguageCoversEveryKey fails when a catalogue is missing
// something it will be asked for.
//
// Two kinds of gap, and the first version of this test caught only one.
// A key present in English and absent elsewhere is the obvious kind —
// translate falls back to English, so the gap ships invisibly, which is
// how several of them reached review instead of the build. The other kind
// is a plural FORM: Arabic asks for `zero`, `two`, `few` and `many` that
// English never has, so walking English keys alone can never notice one
// missing. The required set is therefore taken per language, from the
// language's own pluraliser.
func TestEveryLanguageCoversEveryKey(t *testing.T) {
	english, ok := messages["en"]
	if !ok {
		t.Fatal("the English catalogue is missing")
	}

	// Every key that exists anywhere, so a key added to one catalogue and
	// forgotten in the rest is caught even if English is the one missing
	// it.
	plain := map[string]struct{}{}
	plurals := map[string]struct{}{}
	for _, catalogue := range messages {
		for key := range catalogue {
			if base, form, ok := splitPluralKey(key); ok {
				_ = form
				plurals[base] = struct{}{}
				continue
			}
			plain[key] = struct{}{}
		}
	}
	// A key can only be plural or plain, never both.
	for base := range plurals {
		delete(plain, base)
	}
	if len(english) == 0 {
		t.Fatal("the English catalogue is empty")
	}

	for _, option := range supportedLanguages {
		lang := option.Code
		catalogue, ok := messages[lang]
		if !ok {
			t.Errorf("%s has no catalogue at all", lang)
			continue
		}

		var missing []string
		for key := range plain {
			if _, ok := catalogue[key]; !ok {
				missing = append(missing, key)
			}
		}
		// Every form this language's own rule can select, for every key
		// that is counted anywhere.
		for base := range plurals {
			for form := range formsUsedBy(lang) {
				key := base + "." + string(form)
				if _, ok := catalogue[key]; !ok {
					missing = append(missing, key)
				}
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s is missing %d key(s): %s", lang, len(missing), strings.Join(missing, ", "))
		}
	}
}

// formsUsedBy asks the pluraliser which forms it can return, so the test
// cannot drift from the rule it is checking.
func formsUsedBy(lang string) map[pluralForm]struct{} {
	forms := map[pluralForm]struct{}{}
	for count := 0; count <= 200; count++ {
		forms[pluralFormFor(lang, count)] = struct{}{}
	}
	return forms
}

// splitPluralKey reports whether the last dotted segment is a plural form,
// and if so returns the key without it.
func splitPluralKey(key string) (base string, form pluralForm, ok bool) {
	idx := strings.LastIndex(key, ".")
	if idx < 0 {
		return key, "", false
	}
	candidate := pluralForm(key[idx+1:])
	switch candidate {
	case pluralZero, pluralOne, pluralTwo, pluralFew, pluralMany, pluralOther:
		return key[:idx], candidate, true
	default:
		return key, "", false
	}
}

// TestEveryTranslationTakesTheSameArguments fails when a catalogue entry
// does not accept the arguments its call site passes.
//
// The English entry defines the call: `%d` for a count, `%s` for a name.
// A translation that drops one renders the extra as `%!(EXTRA int=0)` in
// the user's face, and one that adds one renders `%!d(MISSING)`. This is
// not hypothetical — an Arabic zero form was written as a plain phrase,
// which reads naturally in Arabic and would have shipped that tail the
// first time a conversation had nothing outstanding, because
// translateCount passes the count whatever the form says.
//
// Explicit argument indexes (`%[1]s`) are left alone: reordering is a
// legitimate translation need and this check is about the ARGUMENT LIST,
// not the order.
func TestEveryTranslationTakesTheSameArguments(t *testing.T) {
	english, ok := messages["en"]
	if !ok {
		t.Fatal("the English catalogue is missing")
	}

	reference := func(key string) (string, bool) {
		if value, ok := english[key]; ok {
			return value, true
		}
		// A plural form: any English form of the same key defines the
		// argument list, since they are all rendered from one call.
		base, _, isPlural := splitPluralKey(key)
		if !isPlural {
			return "", false
		}
		for _, form := range []pluralForm{pluralOther, pluralOne, pluralZero, pluralTwo, pluralFew, pluralMany} {
			if value, ok := english[base+"."+string(form)]; ok {
				return value, true
			}
		}
		return "", false
	}

	for _, option := range supportedLanguages {
		lang := option.Code
		if lang == "en" {
			continue
		}
		var offenders []string
		for key, value := range messages[lang] {
			want, ok := reference(key)
			if !ok {
				continue
			}
			if strings.Contains(value, "%[") || strings.Contains(want, "%[") {
				continue
			}
			if got, expected := formatVerbs(value), formatVerbs(want); got != expected {
				offenders = append(offenders, fmt.Sprintf("%s: takes %q, English takes %q", key, got, expected))
			}
		}
		sort.Strings(offenders)
		for _, offender := range offenders {
			t.Errorf("%s %s", lang, offender)
		}
	}
}

// formatVerbs is the argument list a format string accepts, as a
// comparable string. `%%` is an escaped percent and takes nothing.
func formatVerbs(format string) string {
	var verbs []string
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			continue
		}
		next := format[i+1]
		if next == '%' {
			i++
			continue
		}
		// Skip flags and width to reach the verb.
		j := i + 1
		for j < len(format) && strings.ContainsRune("+-# 0123456789.", rune(format[j])) {
			j++
		}
		if j < len(format) {
			verbs = append(verbs, "%"+string(format[j]))
			i = j
		}
	}
	return strings.Join(verbs, ",")
}
