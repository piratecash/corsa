package desktop

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// TestHeaderPendingCaption covers what the conversation header says about
// work still outstanding — and, above all, that it says WHICH work.
//
// Two counts share one line and they mean opposite things: messages this
// node has not got to the recipient yet, and messages the recipient has
// not confirmed deleting. A bare number would be ambiguous exactly when
// both are present, which is the case that made this caption necessary.
func TestHeaderPendingCaption(t *testing.T) {
	me := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	peer := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	now := time.Now()

	mine := func(status string, age time.Duration) service.DirectMessage {
		return service.DirectMessage{
			Sender: me, Recipient: peer, ReceiptStatus: status, Timestamp: now.Add(-age),
		}
	}

	for _, tc := range []struct {
		name     string
		messages []service.DirectMessage
		state    service.RouterPeerState
		wantHas  []string
		wantNone []string
	}{
		{
			name:     "nothing outstanding says nothing",
			messages: []service.DirectMessage{mine("delivered", time.Hour), mine("sent", time.Second)},
			wantNone: []string{"·"},
		},
		{
			name:     "a queued message is counted",
			messages: []service.DirectMessage{mine("queued", time.Minute)},
			wantHas:  []string{"1", "доставлен"},
		},
		{
			name: "a fresh sent message is not counted, a stale one is",
			messages: []service.DirectMessage{
				mine("sent", time.Second),
				mine("sent", staleSendThreshold+time.Minute),
			},
			wantHas: []string{"1", "доставлен"},
		},
		{
			name:     "an incoming message is never counted",
			messages: []service.DirectMessage{{Sender: peer, Recipient: me, ReceiptStatus: "queued", Timestamp: now}},
			wantNone: []string{"·"},
		},
		{
			name:     "both halves name themselves",
			messages: []service.DirectMessage{mine("queued", time.Minute), mine("queued", time.Minute)},
			state:    service.RouterPeerState{PendingDeletes: 3},
			wantHas:  []string{"2", "доставлен", "3", "удаления"},
		},
		{
			name:     "a pending wipe still wins over the delete count, and keeps the queue",
			messages: []service.DirectMessage{mine("queued", time.Minute)},
			state:    service.RouterPeerState{PendingDeletes: 3, PendingConversationDelete: true},
			wantHas:  []string{"1", "доставлен", "удаление"},
			wantNone: []string{"3"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			w := &Window{language: "ru"}
			w.snap = service.RouterSnapshot{
				ActivePeer:     peer,
				MyAddress:      me,
				ActiveMessages: tc.messages,
				Peers:          map[domain.PeerIdentity]*service.RouterPeerState{peer: &state},
			}

			caption := w.headerPendingCaption(peer)
			for _, want := range tc.wantHas {
				if !strings.Contains(caption, want) {
					t.Errorf("caption %q does not contain %q", caption, want)
				}
			}
			for _, unwanted := range tc.wantNone {
				if strings.Contains(caption, unwanted) {
					t.Errorf("caption %q contains %q", caption, unwanted)
				}
			}
		})
	}
}
