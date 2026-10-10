package ui

import "testing"

// The divider is a band across the conversation, not a chip beside a message:
// it marks a place in the whole timeline, and a narrower one reads as
// belonging to the bubble next to it.
func TestUnreadDividerSpansTheWholeWidth(t *testing.T) {
	for _, width := range []int{320, 900} {
		gtx := testGtx(width, 400, 1)
		dims := testKit(t).UnreadDivider(gtx, "Unread messages")
		if dims.Size.X != width {
			t.Fatalf("divider in %dpx is %dpx wide, want the whole width", width, dims.Size.X)
		}
		if dims.Size.Y <= 0 || dims.Size.Y > 80 {
			t.Fatalf("divider is %dpx tall, want one line of caption with its air", dims.Size.Y)
		}
	}
}

// A long translation is cut to one line rather than growing the band: the
// divider sits between two messages, and a second line pushes both apart.
func TestUnreadDividerKeepsToOneLine(t *testing.T) {
	gtx := testGtx(120, 400, 1)
	short := testKit(t).UnreadDivider(testGtx(120, 400, 1), "Unread")
	long := testKit(t).UnreadDivider(gtx, "Unread messages unread messages unread messages unread messages")
	if long.Size.Y != short.Size.Y {
		t.Fatalf("a long label made the band %dpx tall, a short one %dpx", long.Size.Y, short.Size.Y)
	}
}
