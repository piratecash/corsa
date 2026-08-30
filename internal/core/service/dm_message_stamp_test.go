package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_message_stamp_test.go pins ONE rule: a message has one timestamp.
//
// The sidebar decides which message is the newest one in a conversation, and
// it decides it from timestamps that reach it by two different roads — the
// object the send path hands back, and the row the database holds. When the
// same message carries two different instants on those two roads, the newer
// of the two becomes a watermark no honest later message can beat, and the
// sidebar keeps showing the message before it.

// TestSentMessageShowsTheTimeItWasStoredWith is the regression for exactly
// that divergence on the send path: the wire (and therefore the row) carried
// second-resolution RFC3339 while the returned DirectMessage carried the same
// instant WITH its nanoseconds, up to a second ahead of everything durable.
//
// The assertion is deliberately equality rather than "close enough": the two
// values describe one event, and any difference between them is a difference
// between what the user sees and what the database can prove.
func TestSentMessageShowsTheTimeItWasStoredWith(t *testing.T) {
	ctx := context.Background()

	client, _ := newTestDesktopClientWithNode(t)
	// The production wiring registers the client as the node's message store;
	// without it the send never reaches the chatlog and there is no row to
	// compare against.
	client.localNode.RegisterMessageStore(client)

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate the recipient identity: %v", err)
	}
	trust(t, client, recipient)
	peer := domain.PeerIdentityFromWire(recipient.Address)

	sent, err := client.SendDirectMessage(ctx, peer, domain.OutgoingDM{Body: "hello"})
	if err != nil {
		t.Fatalf("send a direct message: %v", err)
	}

	entry, err := client.chatLog.ReadLastEntry(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("read the stored row back: %v", err)
	}
	if entry == nil {
		t.Fatal("the message the send path reported as stored is not in the chatlog")
	}

	stored, parseErr := parseTimestamp(entry.CreatedAt)
	if parseErr != nil {
		t.Fatalf("the stored created_at %q does not parse: %v", entry.CreatedAt, parseErr)
	}
	if !stored.Equal(sent.Timestamp) {
		t.Fatalf("one message, two timestamps: the object handed to the UI says %s, the row on disk says %s (difference %s)",
			sent.Timestamp.Format("15:04:05.000000000"),
			stored.Format("15:04:05.000000000"),
			sent.Timestamp.Sub(stored))
	}
}

// TestLiveMessageReachesTheSidebarWhateverThePeersClockSays is the pair of
// symptoms the user sees: the badge counts a message, the conversation jumps
// to the top of the sidebar, and the text next to it is still the previous
// message.
//
// Both halves come from the same rule — a preview used to be accepted only
// when its timestamp beat the one on screen — and both stamps below are ones
// the node accepts and stores: it tolerates ten minutes of drift into the
// future and puts no bound on the past.
func TestLiveMessageReachesTheSidebarWhateverThePeersClockSays(t *testing.T) {
	peer := domaintest.ID("clock-drift-peer")

	for _, tc := range []struct {
		name  string
		skew  time.Duration
		since string
	}{
		{"their clock runs behind ours", -90 * time.Second, "older than the reply we sent before it"},
		{"their clock runs ahead of ours", 5 * time.Minute, "dated after our own now"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRouter()
			me := r.client.Address()
			now := time.Now().UTC()

			// Our reply, stamped by our own clock.
			r.applyDecryptedMessageToSidebar(&DirectMessage{
				ID: "ours", Sender: me, Recipient: peer, Body: "our reply", Timestamp: now,
			}, peer, peerStamp{})

			// Their answer to it, stamped by theirs.
			r.applyDecryptedMessageToSidebar(&DirectMessage{
				ID: "theirs", Sender: peer, Recipient: me, Body: "their answer", Timestamp: now.Add(tc.skew),
			}, peer, peerStamp{})

			r.mu.RLock()
			body := r.peers[peer].Preview.Body
			unread := r.peers[peer].Unread
			r.mu.RUnlock()

			if body != "their answer" {
				t.Fatalf("preview = %q, want %q: the message arrived and was stored, and it is %s",
					body, "their answer", tc.since)
			}
			if unread != 1 {
				t.Fatalf("unread = %d, want 1: the badge and the preview must describe the same message", unread)
			}
		})
	}
}

// TestDecryptedMessageWithAnUnreadableStampStaysStorable is the same rule on
// the receiving side. When a row's created_at cannot be parsed the decrypt
// path invents one, and an invented moment carrying nanoseconds is a moment
// the chatlog can never hold: it outranks every honest row of that second and
// pins the sidebar exactly as the send path used to.
func TestDecryptedMessageWithAnUnreadableStampStaysStorable(t *testing.T) {
	t.Parallel()

	receiver, receiverID := newTestDesktopClientWithNode(t)
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate the sender identity: %v", err)
	}
	trust(t, receiver, senderID)

	ciphertext, err := directmsg.EncryptForParticipants(
		senderID,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(receiverID.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(receiverID.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello"},
	)
	if err != nil {
		t.Fatalf("seal a message for the receiver: %v", err)
	}

	msg := receiver.dm.DecryptIncomingMessage(context.Background(), protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		Topic:     "dm",
		MessageID: "0b7d81f2-9c48-4a6e-9d10-00000000ab01",
		Sender:    senderID.Address,
		Recipient: receiverID.Address,
		Body:      ciphertext,
		// Whatever wrote this row, we cannot read its time.
		CreatedAt: "not a timestamp",
	})
	if msg == nil {
		t.Fatal("the message did not decrypt; the test cannot say anything about its timestamp")
	}
	if !msg.Timestamp.Equal(msg.Timestamp.Truncate(time.Second)) {
		t.Fatalf("the invented timestamp %s carries a fraction the chatlog cannot store, so no stored row can ever match or beat it",
			msg.Timestamp.Format("15:04:05.000000000"))
	}
}

// TestSendEchoWithNoSequenceCannotOverwriteAnOrderedPreview covers the send
// whose own row could not be located: the message went out, the node stored
// it, and only the follow-up lookup of its place in the arrival order failed
// — a context spent on the RPC, a database hiccup, or a row already deleted.
//
// Such an echo cannot claim to be the newest message, because nothing about
// it can be compared with what is on the row. Letting it write anyway is the
// original race with an extra step: A is stored, B is stored and shown, and
// A's late echo puts the older text back.
func TestSendEchoWithNoSequenceCannotOverwriteAnOrderedPreview(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("unordered-echo-peer")
	me := r.client.Address()
	now := time.Now().UTC()

	// B arrived and is on the row, with its place in the order known.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "B", Sender: peer, Recipient: me, Body: "their message",
		Timestamp: now.Add(-time.Minute), Seq: 2,
	}, peer, peerStamp{})

	// A was stored first; its echo comes back late and unplaceable.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "A", Sender: me, Recipient: peer, Body: "our message",
		Timestamp: now, Seq: 0,
	}, peer, peerStamp{})

	r.mu.RLock()
	body := r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if body != "their message" {
		t.Fatalf("preview = %q, want %q: an echo that cannot be placed in the arrival order must not displace one that can",
			body, "their message")
	}
}

// TestUnorderedPreviewsStillApplyToEachOther keeps the refusal narrow. With no
// store to ask — a headless runtime, a client whose chatlog is not wired —
// NOTHING carries a sequence, and a rule that refused every unplaceable
// preview would freeze the sidebar on its first message forever.
func TestUnorderedPreviewsStillApplyToEachOther(t *testing.T) {
	r := newTestRouter()
	peer := domaintest.ID("no-store-peer")
	me := r.client.Address()
	now := time.Now().UTC()

	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "first", Sender: peer, Recipient: me, Body: "first", Timestamp: now.Add(-time.Minute),
	}, peer, peerStamp{})
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "second", Sender: peer, Recipient: me, Body: "second", Timestamp: now,
	}, peer, peerStamp{})

	r.mu.RLock()
	body := r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if body != "second" {
		t.Fatalf("preview = %q, want %q: with no sequences anywhere the later writer must still win", body, "second")
	}
}

// TestUnplaceableLiveMessageIsRepairedFromTheStore closes the hole the refusal
// above opens. Refusing keeps the sidebar from going backwards, but the
// message is still the newest one in the conversation: its id is already
// through the dedup gate, the badge counts it, the conversation has jumped to
// the top — and nothing on this path reads the database again. A single
// MessageSeq timeout on the genuinely last message would otherwise leave the
// old text on the row for good.
//
// The store is the one place that knows both which row is last and where it
// landed, so a preview that could not be placed asks it.
func TestUnplaceableLiveMessageIsRepairedFromTheStore(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("unplaceable-live-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	// Two rows: an older one, and the one that just arrived.
	for _, row := range []struct {
		id, body string
		at       time.Time
	}{
		{"row-old", "older", now.Add(-time.Hour)},
		{"row-new", "the newest row", now},
	} {
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: row.id, Sender: peer.String(), Recipient: id.Address,
			Body: "sealed", CreatedAt: row.at.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", row.id, err)
		}
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	// The sidebar already shows something placed: the older row.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "older", Timestamp: now.Add(-time.Hour), Seq: 1,
	})
	r.mu.Unlock()

	// The newest message arrives, but the lookup of its place failed.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "row-new", Sender: peer, Recipient: me, Body: "the newest row",
		Timestamp: now, Seq: 0,
	}, peer, r.peerStampOf(peer))

	// The store answers what the message could not: row-new is the last row.
	// The repair is asynchronous, like every other store read on this path.
	// row-new is the second row written, so the store places it at 2. The
	// bodies are opaque in this fixture — what is being proved is that the row
	// stopped describing the older message.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.peers[peer].Preview.Seq == 2
	}) {
		r.mu.RLock()
		preview := r.peers[peer].Preview
		r.mu.RUnlock()
		t.Fatalf("preview still sits at sequence %d (%q): a message that could not be ordered left the row stale with nothing to correct it",
			preview.Seq, preview.Body)
	}
}

// TestOwnSentMessageIsGuardedLikeEveryOtherApply covers the send path's echo.
// It used to check only whether the CONTACT still existed, and nothing about
// whether the conversation had moved underneath it — so a wipe, or a deletion
// of the very message being sent, landing while the send was in flight would
// be undone by the echo arriving afterwards.
//
// That guard matters more now that arrival sequences decide the order:
// SQLite hands out max(rowid)+1, so the row deleted from the end of the table
// gives its number to the next insert, and an echo of the deleted row can
// carry the same sequence as the message that replaced it. What separates
// them is not the number but the epoch: a deletion bumps it, and an apply
// holding the older one is stale by construction.
func TestOwnSentMessageIsGuardedLikeEveryOtherApply(t *testing.T) {
	peer := domaintest.ID("own-send-guard-peer")
	now := time.Now().UTC()

	sent := func(seq int64) DirectMessage {
		return DirectMessage{
			ID: "ours", Sender: domaintest.ID("me"), Recipient: peer,
			Body: "our message", Timestamp: now, Seq: seq,
		}
	}

	t.Run("applies while the conversation stands", func(t *testing.T) {
		r := newTestRouter()
		r.mu.Lock()
		r.tryEnsurePeerLocked(peer)
		r.mu.Unlock()

		r.applyOwnSentMessage(peer, sent(5), r.peerStampOf(peer))

		r.mu.RLock()
		body := r.peers[peer].Preview.Body
		r.mu.RUnlock()
		if body != "our message" {
			t.Fatalf("preview = %q, want our own message on the row", body)
		}
	})

	t.Run("stands aside when the conversation moved", func(t *testing.T) {
		r := newTestRouter()
		r.mu.Lock()
		r.tryEnsurePeerLocked(peer)
		// What the sidebar shows after the deletion settled: the row that
		// survived, which reused the sequence of the one that was removed.
		r.applyPreviewLocked(peer, ConversationPreview{
			PeerAddress: peer, Sender: peer, Body: "what survived the wipe",
			Timestamp: now, Seq: 5,
		})
		r.mu.Unlock()

		// The stamp the send captured BEFORE the deletion bumped the epoch.
		stale := r.peerStampOf(peer)
		r.mu.Lock()
		r.moveHistoryBackwardsLocked(peer)
		r.mu.Unlock()

		r.applyOwnSentMessage(peer, sent(5), stale)

		r.mu.RLock()
		body := r.peers[peer].Preview.Body
		r.mu.RUnlock()
		if body != "what survived the wipe" {
			t.Fatalf("preview = %q: an echo from before the deletion put its message back", body)
		}
	})
}

// TestAnUnplacedRowIsAlwaysRepaired covers the other side of the same
// question. A row whose preview carries no sequence is not ordered against
// anything: the next message to arrive wins by nothing more than the moment it
// was applied, and a slow answer holding a real sequence can walk in later and
// overwrite it. The row is a defect wherever it comes from, so it is repaired
// whichever side of the comparison is missing.
func TestAnUnplacedRowIsAlwaysRepaired(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("unplaced-row-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	for _, row := range []struct {
		id string
		at time.Time
	}{
		{"row-1", now.Add(-time.Hour)},
		{"row-2", now},
	} {
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: row.id, Sender: peer.String(), Recipient: id.Address,
			Body: "sealed", CreatedAt: row.at.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", row.id, err)
		}
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	// The row already carries an unplaced preview — an earlier message whose
	// own lookup failed.
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "unplaced", Timestamp: now.Add(-time.Hour),
	})
	r.mu.Unlock()

	// A second message arrives, its lookup fails as well. It is written —
	// there is nothing to order it against and the row must show something —
	// but the row stays unplaced, and that has to be settled by the store.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "row-2", Sender: peer, Recipient: me, Body: "also unplaced", Timestamp: now,
	}, peer, r.peerStampOf(peer))

	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.peers[peer].Preview.Seq == 2
	}) {
		r.mu.RLock()
		preview := r.peers[peer].Preview
		r.mu.RUnlock()
		t.Fatalf("preview sits at sequence %d (%q): an unordered row was left for the next writer to win by timing",
			preview.Seq, preview.Body)
	}
}

// flakyHistoryReader fails a fixed number of reads and then behaves normally,
// which is what a database busy for a moment looks like from up here. The
// counter is atomic because the reads happen on the repair goroutine while
// the test watches from its own.
type flakyHistoryReader struct {
	chatHistoryReader
	failures atomic.Int32
}

func (f *flakyHistoryReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	if f.failures.Add(-1) >= 0 {
		return time.Time{}, errors.New("database is busy")
	}
	return f.chatHistoryReader.LastIncomingAtFor(ctx, peer, now)
}

// TestPreviewRepairSurvivesAFailedRead is the other half of the repair. The
// sequence lookup and the read that settles it fail for the same reasons — a
// database busy longer than either budget — so "ask the store once" is asking
// exactly when the store is least likely to answer.
//
// What happens after that read is the point. Putting the message id back
// through the dedup gate is NOT a way back: the header pass that would
// rediscover it runs once, deferred from initializeFromDB, and the network
// cannot re-announce the message either — a re-delivery is a duplicate the
// node stores and publishes nothing for. The row is queued for the sweep the
// delete path already runs, which is the only thing that comes back for it.
func TestPreviewRepairSurvivesAFailedRead(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("repair-retry-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	for _, row := range []struct {
		id string
		at time.Time
	}{
		{"retry-row-1", now.Add(-time.Hour)},
		{"retry-row-2", now},
	} {
		if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
			ID: row.id, Sender: peer.String(), Recipient: id.Address,
			Body: "sealed", CreatedAt: row.at.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", row.id, err)
		}
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	// The read that follows the message fails; the sweep's read succeeds.
	flaky := &flakyHistoryReader{chatHistoryReader: client.chatLog}
	flaky.failures.Store(1)
	r.history = flaky

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "older", Timestamp: now.Add(-time.Hour), Seq: 1,
	})
	r.mu.Unlock()

	// The message arrives without a sequence — the same busy database that is
	// about to fail the immediate re-read.
	r.applyDecryptedMessageToSidebar(&DirectMessage{
		ID: "retry-row-2", Sender: peer, Recipient: me, Body: "the newest row",
		Timestamp: now, Seq: 0,
	}, peer, r.peerStampOf(peer))

	// The row is owed a repair, and nothing on the message's own path will
	// deliver it.
	if !pollCondition(2*time.Second, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		_, queued := r.pendingPreviewRepair[peer]
		return queued
	}) {
		t.Fatal("the failed read left nothing behind: the row is unplaced and no sweep will come back for it")
	}

	// One tick of the scheduler — the whole tick, so the wiring is covered
	// and not just the sweep it should contain.
	r.runRetrySweep(ctx, now)

	r.mu.RLock()
	preview := r.peers[peer].Preview
	_, stillQueued := r.pendingPreviewRepair[peer]
	r.mu.RUnlock()
	if preview.Seq != 2 {
		t.Fatalf("preview sits at sequence %d (%q) after the sweep, want the last row", preview.Seq, preview.Body)
	}
	if stillQueued {
		t.Fatal("the row was placed but stays queued; the sweep would keep asking about it forever")
	}
}

// hangingHistoryReader never answers: it waits for the caller's context, which
// is what a database busy past every budget looks like from up here. The
// distinction matters — a read that spends the whole tick is the case a
// deadline check can silently forgive.
type hangingHistoryReader struct {
	chatHistoryReader
}

func (h *hangingHistoryReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}

// TestPreviewRepairGivesUpEventually keeps the queue from becoming a
// treadmill: a store that never answers must cost a bounded number of sweeps,
// not one per tick for the life of the process.
//
// The read here does not fail immediately — it waits until the sweep's own
// budget cancels it, which is exactly the shape that used to escape the
// budget: the tick's deadline was set, so the failure was read as "the tick
// ran out, this peer never had its turn" and cost nothing.
func TestPreviewRepairGivesUpEventually(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("repair-giveup-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "giveup-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	r.history = &hangingHistoryReader{chatHistoryReader: client.chatLog}

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	r.queuePreviewRepair(peer)

	// One attempt left, so one sweep decides it. The budget is the package's
	// own, and a test that waited out twelve of them would wait half a minute
	// to prove the same thing.
	r.mu.Lock()
	entry := r.pendingPreviewRepair[peer]
	entry.attemptsLeft = 1
	r.pendingPreviewRepair[peer] = entry
	r.mu.Unlock()

	r.retryPendingPreviewRepair(ctx)

	r.mu.RLock()
	_, stillQueued := r.pendingPreviewRepair[peer]
	r.mu.RUnlock()
	if stillQueued {
		t.Fatal("a read that spent the whole tick cost no attempt; a hung store would be polled every tick for the life of the process")
	}
}

// requeueingHistoryReader asks for a repair from INSIDE the sweep's read,
// which is the window the token exists for: the per-peer lock is released
// while the store is being read, so a message can arrive, fail to be placed,
// and queue a repair of its own before this read comes back.
type requeueingHistoryReader struct {
	chatHistoryReader
	once sync.Once
	hook func()
}

func (q *requeueingHistoryReader) LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error) {
	q.once.Do(q.hook)
	return q.chatHistoryReader.LastIncomingAtFor(ctx, peer, now)
}

// TestSweepKeepsARepairQueuedWhileItRan covers the answer that is older than
// the question. The sweep's read succeeds and it deletes the entry — but the
// entry it deletes may no longer be the one it was answering, and the request
// that replaced it would then be owed to nobody: the sidebar would keep a
// snapshot taken before the newest message, with nothing scheduled to correct
// it.
func TestSweepKeepsARepairQueuedWhileItRan(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("requeue-during-sweep-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "requeue-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	r.queuePreviewRepair(peer)

	// A message lands mid-read and asks for a repair the sweep cannot have
	// included in its answer.
	r.history = &requeueingHistoryReader{
		chatHistoryReader: client.chatLog,
		hook:              func() { r.queuePreviewRepair(peer) },
	}

	r.retryPendingPreviewRepair(ctx)

	r.mu.RLock()
	_, stillQueued := r.pendingPreviewRepair[peer]
	r.mu.RUnlock()
	if !stillQueued {
		t.Fatal("the sweep deleted a request that arrived after its own read began; nothing will come back for the message that asked")
	}
}

// TestReloadFailureStillQueuesTheRepair covers the path that deliberately
// skips the immediate repair: a message arriving into a conversation that is
// selected but still loading is followed by a full reload, so asking the store
// twice would be waste. That reasoning holds only while the reload SUCCEEDS.
//
// When its preview read fails, all that runs is the cache fallback — and the
// cache is the thread, ordered by the senders' clocks, carrying no sequence,
// so it cannot place the row. The message is then visible inside the open
// conversation while the sidebar shows the previous one, with nothing
// scheduled to correct it: reopening the dedup gate leads nowhere, because
// there is no recurring header pass.
func TestReloadFailureStillQueuesTheRepair(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("reload-failure-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "reload-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }
	// The conversation loads; the read that would place the row does not.
	r.history = &hangingHistoryReader{chatHistoryReader: client.chatLog}

	r.mu.Lock()
	// Selected, which is what the reload applies its answer to.
	r.activePeer = peer
	r.tryEnsurePeerLocked(peer)
	r.applyPreviewLocked(peer, ConversationPreview{
		PeerAddress: peer, Sender: peer, Body: "the previous message",
		Timestamp: now.Add(-time.Hour), Seq: 1,
	})
	r.mu.Unlock()

	if !r.reloadAndRefreshPreview(peer, "reload-1") {
		t.Fatal("the conversation did not load; this half is about the reload that succeeds and the preview read that does not")
	}

	r.mu.RLock()
	_, queued := r.pendingPreviewRepair[peer]
	body := r.peers[peer].Preview.Body
	r.mu.RUnlock()
	if body != "the previous message" {
		t.Fatalf("preview = %q: the cache fallback overruled a placed row", body)
	}
	if !queued {
		t.Fatal("the failed preview read left nothing owed: the sidebar keeps the previous message and no sweep will come back for it")
	}
}

// TestFailedConversationLoadStillQueuesTheRepair is the other failure of the
// same helper: not the preview read but the whole conversation load. The
// caller for an already-loaded chat simply ends its goroutine there, so
// without a queued repair the row keeps its old last message for good.
func TestFailedConversationLoadStillQueuesTheRepair(t *testing.T) {
	client, id := newTestDesktopClientWithNode(t)
	me := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("load-failure-peer")
	ctx := context.Background()
	now := time.Now().UTC()

	if err := client.chatLog.Append(ctx, "dm", me, chatlog.Entry{
		ID: "load-1", Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	r := newTestRouter()
	r.client = client
	r.presenceClock = func() time.Time { return now }

	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	// The user has moved on: loadConversation refuses to apply an answer to a
	// conversation that is no longer the one on screen, which is the failure
	// this covers.
	r.activePeer = domaintest.ID("somebody-else")
	r.mu.Unlock()

	if r.reloadAndRefreshPreview(peer, "load-1") {
		t.Fatal("the load succeeded; this test is about the one that does not")
	}

	r.mu.RLock()
	_, queued := r.pendingPreviewRepair[peer]
	r.mu.RUnlock()
	if !queued {
		t.Fatal("a failed conversation load left nothing owed: the row keeps its old last message and no sweep will come back for it")
	}
}
