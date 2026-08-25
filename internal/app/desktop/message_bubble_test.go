package desktop

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// The delivery line reads the receipt status and the timestamp TOGETHER:
// "delivered" with a time and "delivered" without are different lines, and a
// message whose only evidence of arrival is the timestamp still earns its tick.
// Everything else must produce no line at all — the bubble drops the slot and
// the 6dp gap above it with it.
func TestMessageStatusLine(t *testing.T) {
	at := time.Date(2026, 8, 23, 14, 5, 0, 0, time.Local)
	stamp := at.Format(chatTimestampLayout)
	tr := func(key string, _ ...any) string { return "<" + key + ">" }

	tests := []struct {
		name      string
		status    string
		delivered domain.OptionalTime
		want      string
		wantShown bool
	}{
		{name: "seen with a time", status: "seen", delivered: domain.TimeOf(at), want: "✓✓ " + stamp, wantShown: true},
		{name: "seen without one", status: "seen", want: "✓✓", wantShown: true},
		{name: "delivered with a time", status: "delivered", delivered: domain.TimeOf(at), want: "✓ " + stamp, wantShown: true},
		{name: "delivered without one", status: "delivered", want: "✓", wantShown: true},
		{name: "a timestamp alone still ticks", delivered: domain.TimeOf(at), want: "✓ " + stamp, wantShown: true},
		{name: "queued", status: "queued", want: "<chat.status.queued>", wantShown: true},
		{name: "retrying", status: "retrying", want: "<chat.status.retrying>", wantShown: true},
		{name: "failed", status: "failed", want: "<chat.status.failed>", wantShown: true},
		{name: "expired", status: "expired", want: "<chat.status.expired>", wantShown: true},
		{name: "sent", status: "sent", want: "<chat.status.sent>", wantShown: true},
		{name: "nothing known yet", want: "", wantShown: false},
		{name: "a status the UI does not show", status: "pending", want: "", wantShown: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := service.DirectMessage{ReceiptStatus: tt.status, DeliveredAt: tt.delivered}
			got, shown := messageStatusText(message, tr)
			if shown != tt.wantShown {
				t.Fatalf("shown = %v, want %v", shown, tt.wantShown)
			}
			if got != tt.want {
				t.Fatalf("status line = %q, want %q", got, tt.want)
			}
		})
	}
}

// Only the caller's own messages carry a delivery line, and only when there is
// something to say. Both halves drop the whole slot rather than drawing an
// empty label, which is what keeps the gap above it from appearing.
func TestOnlyOutgoingMessagesGetAStatusSlot(t *testing.T) {
	w := &Window{language: "en"}
	delivered := service.DirectMessage{ReceiptStatus: "delivered"}

	if w.bubbleStatus(delivered, false) != nil {
		t.Fatal("an incoming message was given a delivery line")
	}
	if w.bubbleStatus(delivered, true) == nil {
		t.Fatal("a delivered outgoing message has no delivery line")
	}
	if w.bubbleStatus(service.DirectMessage{}, true) != nil {
		t.Fatal("an outgoing message with no known status was given an empty line")
	}
}

// The reaction slot is wired and empty, and the bubble must not pay for it.
// A widget that drew nothing would still cost the 8dp gap above it on every
// message in every conversation.
func TestTheReactionSlotIsAbsentRatherThanEmpty(t *testing.T) {
	w := &Window{language: "en"}
	if w.bubbleReactions(service.DirectMessage{ID: "m1"}) != nil {
		t.Fatal("a message with no reactions was given a chip row, and the gap above it")
	}
}
