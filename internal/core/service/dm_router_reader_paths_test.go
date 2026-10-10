package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_router_reader_paths_test.go walks every path by which a message reaches
// the open conversation, and checks each one asks the reader the same
// question: was the end on screen when it landed?

// readerPlacement is where the reader is when the message lands.
type readerPlacement struct {
	name  string
	atEnd bool
}

var readerPlacements = []readerPlacement{
	{name: "reader at the end", atEnd: true},
	{name: "reader scrolled up", atEnd: false},
}

// checkArrival asserts what an arrival of id means for a reader placed so.
func checkArrival(t *testing.T, c *storedConversation, placement readerPlacement, id domain.MessageID) {
	t.Helper()
	// Some of these paths settle the arrival on a goroutine of their own,
	// and a shutdown started before it gets there refuses the receipt and
	// puts the badge back — which would be the shutdown's answer, not the
	// path's. So the outcome is awaited before the router is drained.
	pollCondition(3*time.Second, func() bool {
		if placement.atEnd {
			return len(c.recorder.sent()) > 0
		}
		return unreadOf(c.r, c.peer) > 0
	})
	pending := c.r.ConsumePendingActions()
	marker, placed := markerOf(c.r)
	settle(t, c.r)
	unread := unreadOf(c.r, c.peer)
	sent := c.recorder.sent()
	if pending.ScrollToEnd {
		t.Fatal("the arrival asked to scroll")
	}
	if placement.atEnd {
		if unread != 0 || len(sent) != 1 || !idsEqual(sent[0], id) {
			t.Fatalf("unread = %d, receipts = %v; want 0 and exactly [%s]", unread, sent, id)
		}
		return
	}
	if unread != 1 || len(sent) != 0 {
		t.Fatalf("unread = %d, receipts = %v; want 1 and none", unread, sent)
	}
	if !placed || marker != id {
		t.Fatalf("divider = %q (placed %v), want above %s", marker, placed, id)
	}
}

// TestHeaderRepairArrivalMeetsTheReader: a message the event path missed is
// found by the header repair, reloaded into the open conversation, and then
// read or badged by where the reader is.
func TestHeaderRepairArrivalMeetsTheReader(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "h0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "h0", AtEnd: false})
			}
			c.store(t, "h1", 1, chatlog.StatusDelivered)
			c.r.mu.Lock()
			c.r.initialSynced = true
			c.r.mu.Unlock()

			c.r.repairUnreadFromHeaders(NodeStatus{DMHeaders: []DMHeader{
				{ID: "h1", Sender: c.peer, Recipient: domain.PeerIdentityFromWire(c.me.Address)},
			}})

			checkArrival(t, c, placement, "h1")
		})
	}
}

// TestDecryptFailArrivalRespectsTheReader: an event whose body does not
// decrypt reaches the conversation through a reload from the store, and is
// then read or badged by where the reader is.
func TestDecryptFailArrivalRespectsTheReader(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "d0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "d0", AtEnd: false})
			}
			c.store(t, "d1", 1, chatlog.StatusDelivered)

			c.r.onNewMessage(protocol.LocalChangeEvent{
				Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "d1",
				Sender: c.sender.Address, Recipient: c.me.Address,
				Body: "not a ciphertext", CreatedAt: c.at(1).Format(time.RFC3339Nano),
			})

			checkArrival(t, c, placement, "d1")
		})
	}
}

// TestDeselectPeerForgetsTheReader: leaving the conversation takes its divider
// with it, and a report the UI still makes for it is about nothing on screen.
func TestDeselectPeerForgetsTheReader(t *testing.T) {
	peer := domaintest.ID("reader-deselected")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	r.DeselectPeer()
	if _, placed := r.Snapshot().UnreadMarker.FirstUnread(); placed {
		t.Fatal("the divider outlived the conversation it was in")
	}
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})
	settle(t, r)

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1 — a late report read a conversation that is closed", got)
	}
	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestRemovePeerForgetsTheReader: the same for a contact removed while open.
func TestRemovePeerForgetsTheReader(t *testing.T) {
	peer := domaintest.ID("reader-removed")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	if _, err := r.RemovePeer(peer); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if _, placed := r.Snapshot().UnreadMarker.FirstUnread(); placed {
		t.Fatal("the divider outlived the contact")
	}
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})
	settle(t, r)

	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none for a removed contact", sent)
	}
}

// TestOwnSendLandsWithoutAScrollRequest: the user's own message lands when
// the send RPC answers, which is not when the user acted — the composer showed
// the end at the press (TestPressingSendShowsTheEndAndEndsAJump in the
// desktop package), and a user who has scrolled up since did that later. So
// the landing asks for no scroll; and its echo is not something to badge.
func TestOwnSendLandsWithoutAScrollRequest(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "s0", 0, chatlog.StatusSeen)
	c.open(t)
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "s0", AtEnd: false})

	if err := c.r.SendMessage(c.peer, domain.OutgoingDM{Body: "mine"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if !pollCondition(5*time.Second, func() bool { return activeCount(c.r) == 2 }) {
		c.r.mu.RLock()
		status := c.r.sendStatus
		c.r.mu.RUnlock()
		t.Fatalf("the sent message never reached the conversation (status %q)", status)
	}
	if c.r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("the send landing asked for a scroll — the press already showed the end")
	}

	c.r.mu.RLock()
	echo := c.r.activeMessages[len(c.r.activeMessages)-1]
	c.r.mu.RUnlock()
	c.r.deliverDecryptedMessage(&echo, c.peer, c.r.peerStampOf(c.peer))
	settle(t, c.r)
	if got := unreadOf(c.r, c.peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — the echo of the user's own message was badged", got)
	}
}

// TestArrivalReadAtTheEndWhoseReceiptFailsIsBadged: read as it landed, but
// the database never heard so — the badge says what the database says.
func TestArrivalReadAtTheEndWhoseReceiptFailsIsBadged(t *testing.T) {
	peer := domaintest.ID("reader-end-receipt-fails")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	recorder.fail = true

	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	settle(t, r)

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1 — the receipt for m2 failed", got)
	}
}

// TestReloadedArrivalAfterTheReaderLeftIsIgnored: the reader left while the
// reload ran; the reload is refused, the message is not on any screen and
// nothing is sent for it.
func TestReloadedArrivalAfterTheReaderLeftIsIgnored(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "l0", 0, chatlog.StatusSeen)
	c.open(t)
	c.store(t, "l1", 1, chatlog.StatusDelivered)
	c.r.DeselectPeer()

	if c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("a reload for a conversation the reader left was applied")
	}
	settle(t, c.r)

	if sent := c.recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
	if _, placed := markerOf(c.r); placed {
		t.Fatal("a divider was placed for a conversation that is not open")
	}
}

// TestASendLandingInALeftConversationDoesNotMoveTheOpenOne: A's cache stays
// warm after the user leaves it for B, so a send to A that lands now goes into
// A's cache — and asks nothing of B's screen: neither A's messages under B's
// header nor a scroll of B's conversation to its end.
func TestASendLandingInALeftConversationDoesNotMoveTheOpenOne(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "s0", 0, chatlog.StatusSeen)
	c.open(t)
	opened := domaintest.ID("reader-open-while-send-lands")
	c.r.DeselectPeer()
	c.r.mu.Lock()
	c.r.tryEnsurePeerLocked(opened)
	c.r.activePeer = opened
	c.r.reader = openedReader()
	c.r.mu.Unlock()

	if err := c.r.SendMessage(c.peer, domain.OutgoingDM{Body: "to the left conversation"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if !pollCondition(5*time.Second, func() bool { return c.r.cache.Len() == 2 }) {
		t.Fatal("the sent message never reached the left conversation's cache")
	}
	settle(t, c.r)

	if c.r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("a send to the left conversation asked the open one to scroll")
	}
	if got := activeCount(c.r); got != 0 {
		t.Fatalf("B's screen shows %d of A's messages", got)
	}
}
