package desktop

import (
	"image"
	"slices"
	"testing"

	"gioui.org/io/system"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
)

// TestContentDirection covers the rule that decides which way a message, a
// draft or an alias is laid out. The cases that matter are the ones where the
// answer is NOT the first character: a technical string that opens with digits
// or punctuation, and text whose direction only shows up in the middle.
func TestContentDirection(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		text      string
		direction system.TextDirection
		strong    bool
	}{
		{name: "empty", text: "", direction: system.LTR, strong: false},
		{name: "latin", text: "hello", direction: system.LTR, strong: true},
		{name: "cyrillic", text: "привет", direction: system.LTR, strong: true},
		{name: "han", text: "中文", direction: system.LTR, strong: true},
		{name: "arabic", text: "مرحبا", direction: system.RTL, strong: true},
		{name: "hebrew", text: "שלום", direction: system.RTL, strong: true},
		{
			name:      "arabic after neutrals",
			text:      "  «— مرحبا",
			direction: system.RTL,
			strong:    true,
		},
		{
			name:      "latin wins when it comes first",
			text:      "OK مرحبا",
			direction: system.LTR,
			strong:    true,
		},
		{
			// A fingerprint: no strong character decides it before the hex
			// letters do, and those are Latin.
			name:      "fingerprint",
			text:      "ae47201c...5fdcdd",
			direction: system.LTR,
			strong:    true,
		},
		{
			// Digits and punctuation alone say nothing, so the interface
			// language keeps the decision.
			name:      "byte counter",
			text:      "4/500 | 996",
			direction: system.LTR,
			strong:    false,
		},
		{
			// Arabic-Indic digits are numbers, not letters: they must not drag
			// a line right to left on their own.
			name:      "arabic indic digits alone",
			text:      "٢٠٢٦",
			direction: system.LTR,
			strong:    false,
		},
		{
			// The known limit, pinned so it cannot change unnoticed: UAX#9
			// resolves a base direction per PARAGRAPH, and this answers once
			// for the whole text, because a Gio text widget carries a single
			// Locale. The Arabic line still reads correctly — go-text resolves
			// bidi runs inside the base — but it is aligned as part of a
			// left-to-right document. See directedByContent.
			name:      "paragraphs that disagree take the first one's direction",
			text:      "hello\nمرحبا",
			direction: system.LTR,
			strong:    true,
		},
		{
			// The other half of the same rule: a first paragraph that decides
			// nothing hands the answer to the one below it.
			name:      "first paragraph with no strong character defers",
			text:      "123\nمرحبا",
			direction: system.RTL,
			strong:    true,
		},
		{
			name:      "right to left mark",
			text:      "\u200f42",
			direction: system.RTL,
			strong:    true,
		},
		{
			name:      "left to right mark",
			text:      "\u200eمرحبا",
			direction: system.LTR,
			strong:    true,
		},
		{
			// U+061C, the Arabic letter mark. Invisible, class AL, and exactly
			// the character somebody pastes to make a bare number read as part
			// of Arabic text. A list of scripts could never have caught it.
			name:      "arabic letter mark",
			text:      "\u061c123",
			direction: system.RTL,
			strong:    true,
		},
		{
			// A right-to-left script that a hand-kept list did not have. The
			// classes come from the Unicode tables, so the next script to be
			// encoded needs nothing added here either.
			name:      "yezidi",
			text:      "\U00010E80\U00010E81",
			direction: system.RTL,
			strong:    true,
		},
		{
			// UAX#9 P2 skips whatever sits between an isolate initiator and its
			// matching PDI: an isolate exists precisely to declare that its
			// contents do not speak for the paragraph. The Arabic after the PDI
			// is the first character that does.
			name:      "strong character inside an isolate does not decide",
			text:      "\u2066hello\u2069مرحبا",
			direction: system.RTL,
			strong:    true,
		},
		{
			// Nested isolates unwind one PDI at a time.
			name:      "nested isolates",
			text:      "\u2067\u2066hello\u2069مرحبا\u2069OK",
			direction: system.LTR,
			strong:    true,
		},
		{
			// An isolate nobody closed swallows the rest of the string, which
			// then has nothing left that speaks for the paragraph.
			name:      "unclosed isolate leaves no decider",
			text:      "\u2068مرحبا hello",
			direction: system.LTR,
			strong:    false,
		},
		{
			// A PDI with no initiator closes nothing and decides nothing.
			name:      "stray PDI",
			text:      "\u2069مرحبا",
			direction: system.RTL,
			strong:    true,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			direction, strong := contentDirection(test.text)
			if strong != test.strong {
				t.Errorf("strong = %v, want %v", strong, test.strong)
			}
			if direction != test.direction {
				t.Errorf("direction = %v, want %v", direction, test.direction)
			}
		})
	}
}

// TestLeftToRightOverridesTheInterfaceLanguage is the guard on the technical
// half of the rule: whatever the frame's locale says, a subtree handed to
// leftToRight is laid out left to right. That is what keeps the caret in the
// console and in the identity search from walking backwards through a string
// with no right-to-left character in it.
func TestLeftToRightOverridesTheInterfaceLanguage(t *testing.T) {
	t.Parallel()

	gtx := layout.Context{Locale: textLocale("ar")}
	if gtx.Locale.Direction != system.RTL {
		t.Fatalf("the Arabic frame locale is %v, want RTL — the rest of this test proves nothing", gtx.Locale.Direction)
	}

	if got := leftToRight(gtx).Locale.Direction; got != system.LTR {
		t.Errorf("direction inside leftToRight = %v, want LTR", got)
	}
	if gtx.Locale.Direction != system.RTL {
		t.Error("leftToRight changed the caller's context: layout.Context must be overridden by value, per subtree")
	}
}

// TestDirectedByContentFallsBackToTheInterfaceLanguage pins the other half:
// text that says nothing about direction leaves the frame's locale alone, so
// an empty composer in the Arabic interface still puts its caret and its hint
// on the right.
func TestDirectedByContentFallsBackToTheInterfaceLanguage(t *testing.T) {
	t.Parallel()

	arabic := layout.Context{Locale: textLocale("ar")}
	english := layout.Context{Locale: textLocale("en")}

	cases := []struct {
		name string
		gtx  layout.Context
		text string
		want system.TextDirection
	}{
		{name: "empty draft in the Arabic interface", gtx: arabic, text: "", want: system.RTL},
		{name: "empty draft in the English interface", gtx: english, text: "", want: system.LTR},
		{name: "English message in the Arabic interface", gtx: arabic, text: "hello", want: system.LTR},
		{name: "Arabic message in the English interface", gtx: english, text: "مرحبا", want: system.RTL},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := directedByContent(test.gtx, test.text).Locale.Direction; got != test.want {
				t.Errorf("direction = %v, want %v", got, test.want)
			}
		})
	}
}

// TestDataFallbackComposition covers the idiom the console's mixed paths use:
// leftToRight first, directedByContent over it. Those paths carry data AND
// translated text through the same helper, so the string decides — and a
// string that decides nothing is data, which reads left to right rather than
// following the interface.
func TestDataFallbackComposition(t *testing.T) {
	t.Parallel()

	arabic := layout.Context{Locale: textLocale("ar")}

	cases := []struct {
		name string
		text string
		want system.TextDirection
	}{
		{name: "peer address has no strong character", text: "1.2.3.4:9000", want: system.LTR},
		{name: "byte counter has no strong character", text: "4/500 | 996", want: system.LTR},
		{name: "hex fingerprint decides on its own Latin", text: "ae47201c5fdcdd", want: system.LTR},
		{name: "translated line still wins", text: "الأقران المتصلون: 4", want: system.RTL},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := directedByContent(leftToRight(arabic), test.text).Locale.Direction
			if got != test.want {
				t.Errorf("direction = %v, want %v", got, test.want)
			}
		})
	}
}

// TestInReadingOrder covers the row arranger: a horizontal row of labels and
// values has to be placed in the direction the interface is read in, because
// layout.Flex places children in the order it is handed them and knows nothing
// about direction.
//
// The order is observed through the order Flex CALLS the children in, which is
// the placement order and the only thing about a layout.FlexChild visible from
// outside Gio.
func TestInReadingOrder(t *testing.T) {
	t.Parallel()

	laidOut := func(t *testing.T, locale string) []int {
		t.Helper()

		var order []int
		child := func(id int) layout.FlexChild {
			return layout.Rigid(func(layout.Context) layout.Dimensions {
				order = append(order, id)
				return layout.Dimensions{Size: image.Pt(10, 10)}
			})
		}

		gtx := layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(300, 40)),
			Locale:      textLocale(locale),
		}
		children := []layout.FlexChild{child(1), child(2), child(3)}
		layout.Flex{Axis: layout.Horizontal}.Layout(gtx, inReadingOrder(gtx, children...)...)
		return order
	}

	if got := laidOut(t, "en"); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("left-to-right order = %v, want unchanged [1 2 3]", got)
	}
	if got := laidOut(t, "ar"); !slices.Equal(got, []int{3, 2, 1}) {
		t.Errorf("right-to-left order = %v, want reversed [3 2 1]", got)
	}
}

// TestInReadingOrderLeavesTheCallersSliceAlone guards the allocation contract:
// rows are built once per frame and handed here every frame, so reversing in
// place would flip the row back and forth as the frames go by.
func TestInReadingOrderLeavesTheCallersSliceAlone(t *testing.T) {
	t.Parallel()

	var order []int
	child := func(id int) layout.FlexChild {
		return layout.Rigid(func(layout.Context) layout.Dimensions {
			order = append(order, id)
			return layout.Dimensions{}
		})
	}
	children := []layout.FlexChild{child(1), child(2), child(3)}

	arabic := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(300, 40)),
		Locale:      textLocale("ar"),
	}
	inReadingOrder(arabic, children...)

	order = nil
	layout.Flex{Axis: layout.Horizontal}.Layout(arabic, children...)
	if !slices.Equal(order, []int{1, 2, 3}) {
		t.Errorf("the caller's own slice came back as %v, want [1 2 3]", order)
	}
}
