package ui

import (
	"image"
	"reflect"
	"testing"

	"gioui.org/f32"
	"gioui.org/io/event"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"
	"golang.org/x/exp/shiny/materialdesign/icons"
)

var testQuickReactions = []string{"👍", "❤️", "🔥", "😂", "😮", "😢", "🙏"}

func testReactionRow(t *testing.T) ReactionPickerRow {
	t.Helper()
	more, err := widget.NewIcon(icons.NavigationExpandMore)
	if err != nil {
		t.Fatalf("more icon: %v", err)
	}
	return ReactionPickerRow{
		Emojis:   testQuickReactions,
		Selected: func(string) bool { return false },
		MoreIcon: more,
		MoreHint: "More",
		Describe: func(value string, selected bool) string {
			if selected {
				return "Clear " + value
			}
			return "React with " + value
		},
	}
}

// Seven slots and the "more" button come to 365dp, which is what has to fit a
// 412dp phone with 8dp of inset either side. The design picked seven for
// exactly this reason, so the arithmetic is worth pinning down.
func TestReactionPickerRowFitsAPhoneWidth(t *testing.T) {
	gtx := testGtx(412, 200, 1)
	size := ReactionPickerRowSize(gtx, len(testQuickReactions))

	if want := 365; size.X != want {
		t.Fatalf("pill width = %dpx, want %dpx", size.X, want)
	}
	if size.X > 412-2*8 {
		t.Fatalf("pill width %dpx does not fit a 412dp phone with 8dp insets", size.X)
	}
	if want := gtx.Dp(ReactionSlotSideDp) + 2*gtx.Dp(reactionPickerPaddingDp); size.Y != want {
		t.Fatalf("pill height = %dpx, want a %dpx slot in %dpx of padding", size.Y, gtx.Dp(ReactionSlotSideDp), want)
	}
}

// The pill draws at its own size whatever it is handed. It is placed by an
// anchor that was computed from ReactionPickerRowSize, so a pill that stretched
// to its constraints would land somewhere other than where the caller measured
// it.
func TestReactionPickerRowIgnoresOversizedConstraints(t *testing.T) {
	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(900, 400)),
	}
	state := new(ReactionPickerRowState)

	dims := testKit(t).ReactionPickerRow(gtx, state, testReactionRow(t))
	if want := ReactionPickerRowSize(gtx, len(testQuickReactions)); dims.Size != want {
		t.Fatalf("pill drew %v inside %v of room, want its own %v", dims.Size, gtx.Constraints.Max, want)
	}
}

// The focus ring is built before the pill is laid out, so on the frame a menu
// opens no slot widget exists yet. A short list would leave the row unreachable
// by keyboard until the second frame.
func TestReactionPickerRowTagsCoverEverySlotBeforeTheFirstDraw(t *testing.T) {
	state := new(ReactionPickerRowState)
	tags := state.Tags(testQuickReactions)

	if got, want := len(tags), len(testQuickReactions)+1; got != want {
		t.Fatalf("ring has %d items, want %d slots plus the more button", got, want)
	}
	if tags[len(tags)-1] != &state.More {
		t.Fatal("the more button is not last in the ring, so Tab leaves the pill before reaching it")
	}
	if !reflect.DeepEqual(tags, state.Tags(testQuickReactions)) {
		t.Fatal("the ring changed between frames: a re-created slot loses the focus and the click it held")
	}
}

func TestReactionChipsWrapOntoRowsThatFit(t *testing.T) {
	tests := []struct {
		name      string
		widths    []int
		available int
		want      []ReactionChipRow
	}{
		{
			name:      "one row",
			widths:    []int{40, 40, 40},
			available: 200,
			want:      []ReactionChipRow{{First: 0, Count: 3}},
		},
		{
			name:      "wraps on the gap, not just the chips",
			widths:    []int{40, 40, 40},
			available: 126,
			want:      []ReactionChipRow{{First: 0, Count: 2}, {First: 2, Count: 1}},
		},
		{
			name:      "a chip wider than the row still gets one",
			widths:    []int{300, 40},
			available: 100,
			want:      []ReactionChipRow{{First: 0, Count: 1}, {First: 1, Count: 1}},
		},
		{
			name:      "nothing to wrap",
			widths:    nil,
			available: 100,
			want:      []ReactionChipRow{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := packReactionChips(tt.widths, tt.available, 4)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("rows = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// An empty set draws nothing at all rather than a row of zero height: the
// bubble drops the spacer above a nil slot, and a chip row that reported a size
// would keep it.
func TestReactionChipsDrawNothingWhenThereAreNone(t *testing.T) {
	gtx := testGtx(400, 200, 1)
	if dims := testKit(t).ReactionChips(gtx, new(ReactionChipsState), nil); dims.Size != (image.Point{}) {
		t.Fatalf("an empty chip row took %v", dims.Size)
	}
}

// The counter is shown from one upwards. A chip that started counting at two
// would change width the moment a second peer reacted, and the row would reflow
// under the reader's eyes.
func TestReactionChipCounterIsAlwaysDrawn(t *testing.T) {
	for _, tt := range []struct {
		count int
		want  string
	}{{count: 1, want: "1"}, {count: 12, want: "12"}, {count: 0, want: "1"}} {
		if got := reactionCountText(tt.count); got != tt.want {
			t.Fatalf("counter for %d = %q, want %q", tt.count, got, tt.want)
		}
	}
}

// A chip answers for the reaction it was drawn for. Keying the widgets by emoji
// is what makes that hold across a row that gains and loses chips.
func TestReactionChipsAnswerForTheirOwnEmoji(t *testing.T) {
	state := new(ReactionChipsState)
	reactions := []Reaction{{Emoji: "🔥", Count: 2}, {Emoji: "👍", Count: 1, Mine: true}}

	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(400, 200)},
	}
	testKit(t).ReactionChips(gtx, state, reactions)

	if _, ok := state.Clicked(gtx, reactions); ok {
		t.Fatal("a chip reported a click nobody made")
	}
}

// A chip answers a press, and answers it as the emoji it was drawn for. The
// row under a message is the only surface where a reaction can be toggled
// without opening anything, so a press it does not report is an announced
// button that does nothing.
func TestAPressOnAChipIsReportedAsThatEmoji(t *testing.T) {
	for _, mine := range []bool{false, true} {
		state := new(ReactionChipsState)
		drawn := []Reaction{{Emoji: "👍", Count: 1, Mine: mine}}
		var router input.Router
		var got string
		frame := func(events ...event.Event) {
			for _, ev := range events {
				router.Queue(ev)
			}
			ops := new(op.Ops)
			gtx := layout.Context{
				Ops:         ops,
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Constraints{Max: image.Pt(400, 200)},
			}
			// Drained before the row lays out, which is the order the caller
			// uses: a Clickable can only be asked about its events while a
			// frame is open.
			for {
				value, ok := state.Clicked(gtx, drawn)
				if !ok {
					break
				}
				got = value
			}
			testKit(t).ReactionChips(gtx, state, drawn)
			router.Frame(ops)
		}

		frame()
		at := f32.Pt(23, 11)
		frame(pointer.Event{Kind: pointer.Press, Source: pointer.Touch, Position: at})
		frame(pointer.Event{Kind: pointer.Release, Source: pointer.Touch, Position: at})
		frame()

		if got != "👍" {
			t.Fatalf("mine=%v: a press on the chip was reported as %q", mine, got)
		}
	}
}

// The pill asks about EVERY slot, so a user holding several reactions sees
// several marked. Marking one of five is what made the row unreadable: the
// keyboard also lands on a slot when the menu opens, and with only one slot
// ever marked the two states were indistinguishable.
func TestThePillAsksAboutEverySlot(t *testing.T) {
	asked := map[string]int{}
	row := testReactionRow(t)
	row.Selected = func(value string) bool {
		asked[value]++
		return value == "🔥" || value == "🙏"
	}

	var router input.Router
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(900, 400)},
	}
	testKit(t).ReactionPickerRow(gtx, new(ReactionPickerRowState), row)
	router.Frame(ops)

	for _, value := range testQuickReactions {
		if asked[value] == 0 {
			t.Fatalf("the pill never asked whether %q is held: its slot can never be marked", value)
		}
	}
}

// A slot the user holds is announced as held, and its description says what a
// press will do — take the reaction back, not add it again. The fill alone is
// visual: a screen reader reads neither the colour nor the ring.
func TestAHeldSlotIsAnnouncedAsHeld(t *testing.T) {
	held := "🔥"
	row := testReactionRow(t)
	row.Selected = func(value string) bool { return value == held }

	var router input.Router
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(900, 400)},
	}
	testKit(t).ReactionPickerRow(gtx, new(ReactionPickerRowState), row)
	router.Frame(ops)

	var selected, described int
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Selected {
			selected++
		}
		if node.Desc.Description == "Clear "+held {
			described++
		}
	}
	if selected != 1 {
		t.Fatalf("%d slots were announced as held, want exactly 1", selected)
	}
	if described != 1 {
		t.Fatalf("%d slots said a press would clear the reaction, want exactly 1", described)
	}
}

// A chip says two things visually — which reaction it is, and whether this user
// is one of the people who made it — and a screen reader gets neither from a
// colour and a border. The held one is announced as selected, and both say what
// a press will do, because on a chip the user already holds a press takes it
// back.
func TestReactionChipsAnnounceWhetherTheyAreYours(t *testing.T) {
	state := &ReactionChipsState{Describe: func(reaction Reaction) string {
		if reaction.Mine {
			return "Clear " + reaction.Emoji
		}
		return "React with " + reaction.Emoji
	}}
	reactions := []Reaction{
		{Emoji: "👍", Count: 2, Mine: true},
		{Emoji: "🔥", Count: 1},
	}

	var router input.Router
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(900, 400)},
	}
	testKit(t).ReactionChips(gtx, state, reactions)
	router.Frame(ops)

	selected, described := 0, map[string]bool{}
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Selected {
			selected++
		}
		if node.Desc.Description != "" {
			described[node.Desc.Description] = true
		}
	}
	if selected != 1 {
		t.Fatalf("%d chips were announced as held, want exactly the user's own", selected)
	}
	if !described["Clear 👍"] {
		t.Fatal("the user's own chip did not say that a press takes the reaction back")
	}
	if !described["React with 🔥"] {
		t.Fatal("somebody else's chip did not say that a press adds the user's reaction")
	}
}
