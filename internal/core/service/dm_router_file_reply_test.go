package service

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// newFileReplyTestRouter builds a router whose file_announce send is captured
// instead of performed (prepareAndSend override), backed by an in-memory
// chatlog. The returned channel receives the exact OutgoingDM the send path
// would transmit — so a test can assert what happened to ReplyTo after the
// stale-reply degrade runs. The peer "bob" is seeded as a live conversation at
// generation 7.
func newFileReplyTestRouter(t *testing.T) (*DMRouter, domain.PeerIdentity, chan domain.OutgoingDM) {
	t.Helper()
	r := newTestRouter()
	cl := newTestChatLog(t)
	r.client.setChatLogForTest(cl)

	sent := make(chan domain.OutgoingDM, 1)
	r.prepareAndSend = func(ctx context.Context, to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload) (*AnnounceResult, error) {
		sent <- msg
		return &AnnounceResult{
			Sent: &DirectMessage{
				ID:        "sent-file-1",
				Sender:    domaintest.ID("me"),
				Recipient: to,
				Body:      msg.Body,
				Timestamp: time.Now().UTC(),
			},
			FileID: domain.FileID("sent-file-1"),
		}, nil
	}

	peer := domaintest.ID("bob")
	r.mu.Lock()
	r.peers[peer] = &RouterPeerState{}
	r.peerOrder = []domain.PeerIdentity{peer}
	r.peerGen[peer] = 7
	r.mu.Unlock()

	return r, peer, sent
}

var fileReplyMeta = domain.FileAnnouncePayload{
	FileHash: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	FileName: "test.txt",
	FileSize: 1024,
}

// TestSendFileAnnounceFromComposerDropsStaleReplyTo is the regression guard for
// the file-reply degrade: when the quoted message no longer exists in the
// conversation (deleted while composing), the file_announce DM must still be
// sent, with ReplyTo cleared, and the async-failure callback must NOT fire.
// Without the pre-send degrade, this send would fail on every retry forever.
func TestSendFileAnnounceFromComposerDropsStaleReplyTo(t *testing.T) {
	t.Parallel()
	r, peer, sent := newFileReplyTestRouter(t)

	failed := make(chan struct{}, 1)
	onFailure := func() { failed <- struct{}{} }

	// ReplyTo references a message that is NOT in the chatlog.
	err := r.SendFileAnnounceFromComposer(peer, domain.OutgoingDM{
		Body:    "with a stale quote",
		ReplyTo: domain.MessageID("11111111-1111-4111-8111-111111111111"),
	}, fileReplyMeta, onFailure, 7)
	if err != nil {
		t.Fatalf("SendFileAnnounceFromComposer returned a sync error: %v", err)
	}

	select {
	case msg := <-sent:
		if msg.ReplyTo != "" {
			t.Fatalf("file announce ReplyTo = %q, want empty (stale quote must be dropped)", msg.ReplyTo)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prepareAndSend was never called — the file announce did not go out")
	}

	// The send succeeded, so the failure callback must stay silent — otherwise
	// the user gets an endless Retry on a file that actually sent.
	select {
	case <-failed:
		t.Fatal("onAsyncFailure fired although the file announce succeeded without the quote")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestSendFileAnnounceFromComposerKeepsLiveReplyTo is the counter-case: a quote
// that still exists must be preserved, so the degrade does not strip valid
// reply references.
func TestSendFileAnnounceFromComposerKeepsLiveReplyTo(t *testing.T) {
	t.Parallel()
	r, peer, sent := newFileReplyTestRouter(t)

	const replyID = "22222222-2222-4222-8222-222222222222"
	// Seed the quoted message so the reference is live in this conversation.
	if err := r.client.chatLog.Append(context.Background(), "dm", domaintest.ID("me"), chatlog.Entry{
		ID:        replyID,
		Sender:    domaintest.ID("me").String(),
		Recipient: peer.String(),
		Body:      "quoted",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed chatlog: %v", err)
	}

	err := r.SendFileAnnounceFromComposer(peer, domain.OutgoingDM{
		Body:    "with a live quote",
		ReplyTo: domain.MessageID(replyID),
	}, fileReplyMeta, func() {}, 7)
	if err != nil {
		t.Fatalf("SendFileAnnounceFromComposer returned a sync error: %v", err)
	}

	select {
	case msg := <-sent:
		if string(msg.ReplyTo) != replyID {
			t.Fatalf("file announce ReplyTo = %q, want %q (live quote must be preserved)", msg.ReplyTo, replyID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prepareAndSend was never called")
	}
}
