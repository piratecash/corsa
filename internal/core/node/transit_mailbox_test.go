package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestFetchSurfacesExcludeTransitDM pins the forwarding-only transit
// contract on every fetch surface: a transit DM (neither sender nor
// recipient is this node) lives in s.topics solely as the in-flight buffer
// of its own forwarding operation (transit_retention.go) and must never be
// served as a mailbox — not through fetch_messages / fetch_message_ids /
// fetch_message, and not through fetch_inbox (which also feeds the
// auth-time backlog replay in pushBacklogToSubscriber). Local DMs — where
// this node is sender or recipient — keep flowing through all surfaces.
func TestFetchSurfacesExcludeTransitDM(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	const otherSender = "third-party-sender-aaa"
	const otherRecipient = "third-party-recipient-bbb"

	transit := protocol.Envelope{
		ID: "transit-dm-1", Topic: "dm",
		Sender: otherSender, Recipient: otherRecipient,
		Payload: []byte("transit-payload"), CreatedAt: now, StoredAt: now,
	}
	inboundLocal := protocol.Envelope{
		ID: "local-in-1", Topic: "dm",
		Sender: otherSender, Recipient: svc.Address(),
		Payload: []byte("inbound-payload"), CreatedAt: now, StoredAt: now,
	}
	outboundLocal := protocol.Envelope{
		ID: "local-out-1", Topic: "dm",
		Sender: svc.Address(), Recipient: otherRecipient,
		Payload: []byte("outbound-payload"), CreatedAt: now, StoredAt: now,
	}

	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], transit, inboundLocal, outboundLocal)
	svc.gossipMu.Unlock()

	wantIDs := map[string]bool{"local-in-1": true, "local-out-1": true}

	messages := svc.fetchMessagesFrame("dm")
	if messages.Count != 2 {
		t.Fatalf("fetch_messages: expected 2 local messages, got %d: %#v", messages.Count, messages.Messages)
	}
	for _, item := range messages.Messages {
		if !wantIDs[item.ID] {
			t.Fatalf("fetch_messages leaked non-local message %q", item.ID)
		}
	}

	ids := svc.fetchMessageIDsFrame("dm")
	if ids.Count != 2 {
		t.Fatalf("fetch_message_ids: expected 2 local ids, got %d: %v", ids.Count, ids.IDs)
	}
	for _, id := range ids.IDs {
		if !wantIDs[id] {
			t.Fatalf("fetch_message_ids leaked non-local id %q", id)
		}
	}

	if f := svc.fetchMessageFrame("dm", "transit-dm-1"); f.Type != "error" || f.Code != protocol.ErrCodeUnknownMessageID {
		t.Fatalf("fetch_message must not serve a transit DM, got %#v", f)
	}
	if f := svc.fetchMessageFrame("dm", "local-in-1"); f.Type != "message" {
		t.Fatalf("fetch_message must keep serving local DMs, got %#v", f)
	}

	// fetch_inbox for the third-party recipient: only the DM this node
	// itself SENT to them may be replayed (sender-owned retry); the transit
	// envelope addressed to them must be invisible.
	inbox := svc.fetchInboxFrame("dm", otherRecipient)
	if inbox.Count != 1 || len(inbox.Messages) != 1 || inbox.Messages[0].ID != "local-out-1" {
		t.Fatalf("fetch_inbox must serve only locally-sent DMs, got %#v", inbox.Messages)
	}

	// fetch_inbox for our own identity keeps serving inbound local DMs.
	ownInbox := svc.fetchInboxFrame("dm", svc.Address())
	if ownInbox.Count != 1 || len(ownInbox.Messages) != 1 || ownInbox.Messages[0].ID != "local-in-1" {
		t.Fatalf("fetch_inbox for own identity must serve inbound local DMs, got %#v", ownInbox.Messages)
	}
}
