package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// The local copy is gone by the time SendMessageDelete returns, so the
// status line only ever describes what is still owed to the peer. An
// empty caption means "say nothing": the terminal outcome has already
// been published and must not be overwritten by a progress line.
func TestMessageDeleteStatusFor(t *testing.T) {
	tests := []struct {
		name   string
		route  domain.MessageDeleteRoute
		online bool
		want   string
	}{
		{
			name:  "recalled route leaves its own terminal outcome standing",
			route: domain.MessageDeleteRouteRecalled,
			want:  "",
		},
		{
			name:   "withdrawn message with the peer online is being asked now",
			route:  domain.MessageDeleteRouteWithdraw,
			online: true,
			want:   "status.message_delete_dispatched",
		},
		{
			name:  "withdrawn message with the peer offline is scheduled",
			route: domain.MessageDeleteRouteWithdraw,
			want:  "status.message_delete_scheduled",
		},
		{
			name:  "confirmed message with the peer offline is scheduled",
			route: domain.MessageDeleteRouteScheduled,
			want:  "status.message_delete_scheduled",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &Window{language: "en"}

			got := w.messageDeleteStatusFor(tc.route, tc.online)
			want := ""
			if tc.want != "" {
				want = w.t(tc.want)
			}
			if got != want {
				t.Fatalf("caption = %q, want %q", got, want)
			}
		})
	}
}

// The outcome subscriber is where a finished deletion gets its wording,
// and a recall is not the same news as an ordinary delete: the message
// never left this node, so there is nothing pending and nothing the peer
// ever saw. Reading the same caption in both cases is what leaves a user
// unable to tell why two chats answered differently.
func TestMessageDeleteOutcomeCaptions(t *testing.T) {
	peer := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")

	tests := []struct {
		name    string
		outcome ebus.MessageDeleteOutcome
		wantKey string
	}{
		{
			name:    "recalled says the message never went out",
			outcome: ebus.MessageDeleteOutcome{Peer: peer, Status: domain.MessageDeleteStatusDeleted, Route: domain.MessageDeleteRouteRecalled},
			wantKey: "status.message_delete_recalled",
		},
		{
			name:    "a peer ack carries no route and reads by status",
			outcome: ebus.MessageDeleteOutcome{Peer: peer, Status: domain.MessageDeleteStatusDeleted},
			wantKey: "status.message_deleted",
		},
		{
			name:    "a refusal is reported as such",
			outcome: ebus.MessageDeleteOutcome{Peer: peer, Status: domain.MessageDeleteStatusDenied},
			wantKey: "status.message_delete_denied",
		},
		{
			name:    "an expired intent is abandonment",
			outcome: ebus.MessageDeleteOutcome{Peer: peer, Abandoned: true},
			wantKey: "status.message_delete_abandoned",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &Window{language: "en"}
			if got, want := w.messageDeleteOutcomeCaption(tc.outcome), w.t(tc.wantKey); got != want {
				t.Fatalf("caption = %q, want %q", got, want)
			}
		})
	}
}

// The wipe's captions. It has two moments — the click, when this side is
// finished, and the peer's answer, when the other side is — and the wording
// has to tell them apart: "scheduled" is not the same news as "deleted".
//
// There is no third caption for giving up, because the request is never given
// up on: "erased here, still there, nobody will ask again" is the state this
// must not produce, so the pending line stands until the peer confirms.
func TestConversationDeleteOutcomeCaptions(t *testing.T) {
	peer := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")

	w := &Window{language: "en"}

	tests := []struct {
		name    string
		outcome ebus.ConversationDeleteOutcome
		want    string
	}{
		{
			name:    "a wipe that could not run is reported as such",
			outcome: ebus.ConversationDeleteOutcome{Peer: peer, LocalCleanupFailed: true},
			want:    w.t("status.clear_chat_local_cleanup_failed"),
		},
		{
			name:    "a dispatched wipe says the peer was asked, not how many messages",
			outcome: ebus.ConversationDeleteOutcome{Peer: peer, Deleted: 3, Requested: true},
			want:    w.t("status.clear_chat_scheduled"),
		},
		{
			name:    "an already empty thread is still a request, because the peer may hold the rest",
			outcome: ebus.ConversationDeleteOutcome{Peer: peer, Deleted: 0, Requested: true},
			want:    w.t("status.clear_chat_scheduled"),
		},
		{
			name:    "the peer's answer is its own news",
			outcome: ebus.ConversationDeleteOutcome{Peer: peer, Settled: true, Status: domain.ConversationDeleteStatusApplied},
			want:    w.t("status.clear_chat_confirmed"),
		},
		{
			name:    "nothing local and nothing asked says so",
			outcome: ebus.ConversationDeleteOutcome{Peer: peer},
			want:    w.t("status.clear_chat_empty"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.conversationDeleteOutcomeCaption(tc.outcome); got != tc.want {
				t.Fatalf("caption = %q, want %q", got, tc.want)
			}
		})
	}
}
