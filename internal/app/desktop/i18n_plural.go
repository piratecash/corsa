package desktop

import "fmt"

// i18n_plural.go picks the grammatical form a counted phrase needs.
//
// "2 ждут удаления" is not a sentence in Russian, and neither is
// "1 messages". A count only reads correctly when the words around it
// agree with it, and which words those are is a property of the language,
// not of the caller: Russian needs three forms, Arabic six, Chinese one.
// So the caller passes the number and the message catalogue carries one
// entry per form.
//
// The rules follow the CLDR plural categories for the languages this app
// ships. Adding a language means adding its rule here and its forms to the
// catalogue; a missing form falls back to `other`, so a partially
// translated catalogue renders an awkward phrase rather than a key.

// pluralForm is a CLDR plural category. The zero value is deliberately not
// one of them: a form is always chosen explicitly.
type pluralForm string

const (
	pluralZero  pluralForm = "zero"
	pluralOne   pluralForm = "one"
	pluralTwo   pluralForm = "two"
	pluralFew   pluralForm = "few"
	pluralMany  pluralForm = "many"
	pluralOther pluralForm = "other"
)

// pluralFormFor reports which form `count` takes in the given language.
// Negative counts are read by their magnitude — the grammar of "-2 files"
// follows the 2, and no caller has a use for a negative count anyway.
func pluralFormFor(lang string, count int) pluralForm {
	if count < 0 {
		count = -count
	}
	switch normalizeLanguage(lang) {
	case "ru":
		return russianPluralForm(count)
	case "ar":
		return arabicPluralForm(count)
	case "fr":
		// French counts 0 with 1: "0 message", not "0 messages".
		if count <= 1 {
			return pluralOne
		}
		return pluralOther
	case "zh":
		// No grammatical number.
		return pluralOther
	default:
		// en, es: one for exactly 1.
		if count == 1 {
			return pluralOne
		}
		return pluralOther
	}
}

func russianPluralForm(count int) pluralForm {
	tens := count % 100
	if tens >= 11 && tens <= 14 {
		return pluralMany
	}
	switch count % 10 {
	case 1:
		return pluralOne
	case 2, 3, 4:
		return pluralFew
	default:
		return pluralMany
	}
}

func arabicPluralForm(count int) pluralForm {
	switch count {
	case 0:
		return pluralZero
	case 1:
		return pluralOne
	case 2:
		return pluralTwo
	}
	hundreds := count % 100
	switch {
	case hundreds >= 3 && hundreds <= 10:
		return pluralFew
	case hundreds >= 11 && hundreds <= 99:
		return pluralMany
	default:
		return pluralOther
	}
}

// translateCount renders a counted phrase: it resolves `key` to the entry
// for the count's plural form (`key.one`, `key.few`, …) and formats it
// with the count. Extra args follow the count, in the order the format
// string expects.
//
// Falls back to `key.other` when the exact form is missing, so a catalogue
// that has not been filled in for a language still renders a sentence.
func translateCount(lang, key string, count int, args ...any) string {
	form := pluralFormFor(lang, count)
	formatArgs := append([]any{count}, args...)

	if value, ok := lookupPluralForm(lang, key, form); ok {
		return fmt.Sprintf(value, formatArgs...)
	}
	return translate(lang, key, formatArgs...)
}

// lookupPluralForm resolves one form in the requested language, then in
// English, then the `other` form of either. Reports false when the key has
// no plural entries at all, which is the caller's signal to fall back to
// the plain catalogue.
func lookupPluralForm(lang, key string, form pluralForm) (string, bool) {
	lang = normalizeLanguage(lang)
	for _, candidate := range []struct {
		lang string
		form pluralForm
	}{
		{lang, form},
		{lang, pluralOther},
		{"en", form},
		{"en", pluralOther},
	} {
		if value, ok := messages[candidate.lang][key+"."+string(candidate.form)]; ok {
			return value, true
		}
	}
	return "", false
}
