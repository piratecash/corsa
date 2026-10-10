package desktop

import (
	"image"
	"testing"
	"time"

	"gioui.org/io/input"
	"gioui.org/layout"
	"gioui.org/op"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

// chat_reader_test.go pins down what the window tells the router about the
// reader: which message is the newest one on screen, and whether the end of
// the conversation is. The router marks messages read from this alone, so a
// message counted as seen that only peeks over the bottom edge is a receipt
// for something nobody read.

const readerViewport = 400

// readerMinVisible is the floor the window passes in pixels at 1px/dp.
const readerMinVisible = 48

func knownHeights(heights map[int]int) func(int) (int, bool) {
	return func(index int) (int, bool) {
		height, ok := heights[index]
		return height, ok
	}
}

func TestNewestReadIndex(t *testing.T) {
	cases := []struct {
		name     string
		position layout.Position
		heights  map[int]int
		want     int
		wantOK   bool
	}{
		{
			name:     "everything fits: the last message is on screen whole",
			position: layout.Position{First: 0, Count: 3, OffsetLast: 120},
			heights:  map[int]int{0: 60, 1: 60, 2: 60},
			want:     2, wantOK: true,
		},
		{
			name:     "the last message ends exactly at the bottom edge",
			position: layout.Position{First: 4, Count: 3, OffsetLast: 0},
			heights:  map[int]int{4: 100, 5: 100, 6: 100},
			want:     6, wantOK: true,
		},
		{
			name:     "the last message shows more than half of itself",
			position: layout.Position{First: 4, Count: 3, OffsetLast: -30},
			heights:  map[int]int{4: 100, 5: 100, 6: 80},
			want:     6, wantOK: true,
		},
		{
			name:     "the last message only peeks over the bottom edge",
			position: layout.Position{First: 4, Count: 3, OffsetLast: -70},
			heights:  map[int]int{4: 100, 5: 100, 6: 80},
			want:     5, wantOK: true,
		},
		{
			name:     "a tall message counts once 48dp of it shows, short of half",
			position: layout.Position{First: 4, Count: 2, OffsetLast: -500},
			heights:  map[int]int{4: 100, 5: 600},
			want:     5, wantOK: true,
		},
		{
			name:     "a tall message whose top barely shows is not read",
			position: layout.Position{First: 4, Count: 2, OffsetLast: -580},
			heights:  map[int]int{4: 100, 5: 600},
			want:     4, wantOK: true,
		},
		{
			name:     "one message taller than the screen, its top just visible",
			position: layout.Position{First: 7, Count: 1, OffsetLast: -590},
			heights:  map[int]int{7: 600},
			wantOK:   false,
		},
		{
			name: "the scrollbar moved the list after the frame was measured",
			// First was rewritten by material.List's ScrollBy; the children
			// it now names were never laid out, so nothing says how much of
			// the last one shows.
			position: layout.Position{First: 9, Count: 3, OffsetLast: -20},
			heights:  map[int]int{2: 100, 3: 100, 4: 100},
			wantOK:   false,
		},
		{
			name:     "nothing laid out",
			position: layout.Position{},
			heights:  map[int]int{},
			wantOK:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := newestReadIndex(tc.position, knownHeights(tc.heights), readerMinVisible)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Fatalf("newestReadIndex = (%d, %v), want (%d, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// readerHarness lays the conversation out with the real layout.List, the way
// layoutConversation does, and asks the reader what to report.
type readerHarness struct {
	w            *Window
	router       *input.Router
	ops          *op.Ops
	heights      []int
	conversation []service.DirectMessage
	now          time.Time
	// scrollbar is a drag of the scrollbar, in items, applied on the next
	// frame where material.List applies one: after the children were
	// measured. Consumed when used.
	scrollbar float32
}

func newReaderHarness(heights []int) *readerHarness {
	h := &readerHarness{
		router:  new(input.Router),
		ops:     new(op.Ops),
		heights: heights,
		now:     time.Unix(1_800_000_000, 0),
	}
	h.w = &Window{}
	h.w.chatList.List = layout.List{Axis: layout.Vertical, ScrollToEnd: true}
	for i := range heights {
		h.conversation = append(h.conversation, service.DirectMessage{ID: msgIDAt(i)})
	}
	return h
}

// frame lays the list out and returns what the reader would report for peer.
func (h *readerHarness) frame(peer domain.PeerIdentity) (service.ReaderPosition, bool) {
	h.ops.Reset()
	h.now = h.now.Add(16 * time.Millisecond)
	gtx := layout.Context{
		Ops:         h.ops,
		Source:      h.router.Source(),
		Now:         h.now,
		Constraints: layout.Constraints{Max: image.Pt(300, readerViewport)},
	}
	h.w.chatReader.beginFrame()
	h.w.chatList.Layout(gtx, len(h.heights), func(gtx layout.Context, index int) layout.Dimensions {
		dims := layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, h.heights[index])}
		h.w.chatReader.measure(index, dims.Size.Y)
		return dims
	})
	h.router.Frame(h.ops)
	scrollbarMoved := h.scrollbar != 0
	if scrollbarMoved {
		h.w.chatList.ScrollBy(h.scrollbar)
		h.scrollbar = 0
	}
	return h.w.chatReader.positionToReport(peer, h.conversation, readerFrame{
		position:       h.w.chatList.Position,
		scrollbarMoved: scrollbarMoved,
		minVisible:     readerMinVisible,
	})
}

// TestAReaderAtTheEndReportsTheNewestMessage: an opened conversation sits at
// its end, and that is what the first report says.
func TestAReaderAtTheEndReportsTheNewestMessage(t *testing.T) {
	peer := domaintest.ID("reader-ui-end")
	h := newReaderHarness(unevenHeights)

	got, report := h.frame(peer)
	if !report {
		t.Fatal("the first frame of a conversation reported nothing")
	}
	want := service.ReaderPosition{NewestSeen: domain.MessageID(msgIDAt(len(unevenHeights) - 1)), AtEnd: true}
	if got != want {
		t.Fatalf("reported %+v, want %+v", got, want)
	}
}

// TestAReaderScrolledUpReportsWhatIsOnScreen: scrolled to the top, the newest
// message on screen is the last one the viewport reaches far enough into, and
// the end is not on screen.
func TestAReaderScrolledUpReportsWhatIsOnScreen(t *testing.T) {
	peer := domaintest.ID("reader-ui-up")
	h := newReaderHarness(unevenHeights)
	h.frame(peer)

	h.w.chatList.Position = layout.Position{First: 0, Offset: 0, BeforeEnd: true}
	got, report := h.frame(peer)
	if !report {
		t.Fatal("scrolling up reported nothing")
	}
	// 60 + 340 = 400: messages 0 and 1 fill the viewport exactly, so 1 ends
	// on the bottom edge and nothing after it shows.
	want := service.ReaderPosition{NewestSeen: domain.MessageID(msgIDAt(1)), AtEnd: false}
	if got != want {
		t.Fatalf("reported %+v, want %+v", got, want)
	}
}

// TestTheReaderReportsOnlyChanges: the report takes the router's lock, and the
// window draws at frame rate — an unchanged position is not reported again.
func TestTheReaderReportsOnlyChanges(t *testing.T) {
	peer := domaintest.ID("reader-ui-dedup")
	h := newReaderHarness(unevenHeights)
	if _, report := h.frame(peer); !report {
		t.Fatal("the first frame reported nothing")
	}
	if got, report := h.frame(peer); report {
		t.Fatalf("an unchanged frame reported %+v again", got)
	}

	h.w.chatList.Position = layout.Position{First: 0, Offset: 0, BeforeEnd: true}
	if _, report := h.frame(peer); !report {
		t.Fatal("a scroll was not reported")
	}
	if got, report := h.frame(peer); report {
		t.Fatalf("the frame after the scroll reported %+v again", got)
	}
}

// TestAnotherConversationIsReportedAfresh: the same position in a different
// conversation is a different fact, and the switch forgets the last report.
func TestAnotherConversationIsReportedAfresh(t *testing.T) {
	first := domaintest.ID("reader-ui-first")
	second := domaintest.ID("reader-ui-second")
	h := newReaderHarness(unevenHeights)
	h.frame(first)

	if _, report := h.frame(second); !report {
		t.Fatal("the same position in another conversation was not reported")
	}
	h.w.chatReader = chatReader{}
	if _, report := h.frame(second); !report {
		t.Fatal("a forgotten report was not made again")
	}
}

// TestAPartlyVisibleLastMessageIsNotReported: on the real list, the message at
// the bottom edge that shows less than the threshold is not counted — the one
// above it is the newest read.
func TestAPartlyVisibleLastMessageIsNotReported(t *testing.T) {
	peer := domaintest.ID("reader-ui-partly")
	h := newReaderHarness(unevenHeights)
	h.frame(peer)

	// From message 2 down: 48+52+210+44 = 354px, so 46px of the 300px
	// message 6 shows — under min(150, 48).
	h.w.chatList.Position = layout.Position{First: 2, Offset: 0, BeforeEnd: true}
	got, report := h.frame(peer)
	if !report {
		t.Fatal("the scroll was not reported")
	}
	want := service.ReaderPosition{NewestSeen: domain.MessageID(msgIDAt(5)), AtEnd: false}
	if got != want {
		t.Fatalf("reported %+v, want %+v — message 6 only peeks over the edge", got, want)
	}
}

// TestTheScrollbarFrameReportsNothing: material.List moves the list for its
// scrollbar AFTER the children were measured, so on that frame Position names
// children laid out somewhere else — or not at all. Whatever it would report
// is a guess about the next frame; that frame reports for itself.
func TestTheScrollbarFrameReportsNothing(t *testing.T) {
	peer := domaintest.ID("reader-ui-scrollbar")
	h := newReaderHarness(unevenHeights)
	h.frame(peer)
	h.w.chatList.Position = layout.Position{First: 0, Offset: 0, BeforeEnd: true}
	h.frame(peer)

	h.scrollbar = 1
	if got, report := h.frame(peer); report {
		t.Fatalf("the scrollbar frame reported %+v — messages 0 and 1 were the ones on screen", got)
	}
	if _, report := h.frame(peer); !report {
		t.Fatal("the frame after the scrollbar moved the list reported nothing")
	}
}

func TestUnreadDividerGoesAboveTheMarkedMessageOnly(t *testing.T) {
	conversation := []service.DirectMessage{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	cases := []struct {
		name        string
		firstUnread domain.MessageID
		placed      bool
		want        []bool
	}{
		{name: "marked message in the middle", firstUnread: "c", placed: true, want: []bool{false, false, true, false}},
		{name: "no divider", firstUnread: "", placed: false, want: []bool{false, false, false, false}},
		{name: "marked message no longer in the conversation", firstUnread: "gone", placed: true, want: []bool{false, false, false, false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for index, message := range conversation {
				if got := unreadDividerAbove(tc.firstUnread, tc.placed, message); got != tc.want[index] {
					t.Fatalf("message %d (%s): divider = %v, want %v", index, message.ID, got, tc.want[index])
				}
			}
		})
	}
}

// TestAStuckScrollbarDoesNotSilenceAShortConversation: the scrollbar of a
// conversation that fits on screen is never updated (ScrollbarStyle.Layout
// returns before Scrollbar.Update), so a drag delta left from before it fit
// stays readable frame after frame. The whole conversation is on screen, and
// that is what is reported.
func TestAStuckScrollbarDoesNotSilenceAShortConversation(t *testing.T) {
	peer := domaintest.ID("reader-ui-stuck-scrollbar")
	h := newReaderHarness([]int{60, 60, 60})

	h.scrollbar = 0.3
	got, report := h.frame(peer)
	if !report {
		t.Fatal("a conversation that fits on screen reported nothing")
	}
	want := service.ReaderPosition{NewestSeen: domain.MessageID(msgIDAt(2)), AtEnd: true}
	if got != want {
		t.Fatalf("reported %+v, want %+v", got, want)
	}
}

// TestAPositionBeyondTheConversationIsNotReported: the list can describe more
// children than the conversation the frame was asked about holds — a snapshot
// that shrank — and a position past its end names no message.
func TestAPositionBeyondTheConversationIsNotReported(t *testing.T) {
	peer := domaintest.ID("reader-ui-beyond")
	h := newReaderHarness(unevenHeights)
	h.conversation = h.conversation[:3]

	if got, report := h.frame(peer); report {
		t.Fatalf("reported %+v for a position past the conversation's end", got)
	}
}
