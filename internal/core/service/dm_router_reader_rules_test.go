package service

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_router_reader_rules_test.go holds the reader rules that need the store:
// opening, reloads, rebuilds from the database — the paths where what the
// router knows about the reader meets what the database says is unread.

// storedConversation is a conversation with a contact whose keys the node
// holds, so what is written to the store reads back decrypted.
type storedConversation struct {
	r        *DMRouter
	client   *DesktopClient
	me       *identity.Identity
	sender   *identity.Identity
	peer     domain.PeerIdentity
	recorder *seenRecorder
	start    time.Time
}

func newStoredConversation(t *testing.T) *storedConversation {
	t.Helper()
	client, me := newTestDesktopClientWithNode(t)
	sender := knownContact(t, client)
	r := newTestRouter()
	r.client = client
	recorder := &seenRecorder{}
	r.markConversationSeenFn = recorder.record
	peer := domain.PeerIdentityFromWire(sender.Address)
	r.mu.Lock()
	r.tryEnsurePeerLocked(peer)
	r.mu.Unlock()
	return &storedConversation{
		r: r, client: client, me: me, sender: sender, peer: peer,
		recorder: recorder, start: time.Now().UTC().Add(-time.Hour),
	}
}

// store writes an incoming message the way the node does before announcing it.
func (c *storedConversation) store(t *testing.T, id string, minute int, status string) {
	t.Helper()
	appendIncomingSealed(t, c.client, c.sender, c.me, id, c.at(minute), status)
}

// message is id as the event path hands it over, decrypted.
func (c *storedConversation) message(id string, minute int) DirectMessage {
	return DirectMessage{
		ID: id, Sender: c.peer, Recipient: domain.PeerIdentityFromWire(c.me.Address),
		Body: id, Timestamp: c.at(minute),
	}
}

func (c *storedConversation) at(minute int) time.Time {
	return c.start.Add(time.Duration(minute) * time.Minute)
}

// open is a completed open: the conversation loaded and on screen from its
// end, its scroll request spent, and the receipts it sent for what was there
// forgotten — the tests are about what comes after.
func (c *storedConversation) open(t *testing.T) {
	t.Helper()
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(nil)
	c.r.mu.Unlock()
	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the conversation did not load")
	}
	c.r.ConsumePendingActions()
	// The open reads in the background; what it sent is forgotten only once
	// it has been sent.
	if !pollCondition(3*time.Second, func() bool { return len(c.recorder.sent()) > 0 }) {
		t.Fatal("the open never read the conversation")
	}
	c.recorder.forget()
}

// openWith is open for a conversation whose cache holds exactly messages —
// what was on screen before something new was written to the store.
func (c *storedConversation) openWith(messages ...DirectMessage) {
	c.r.cache.Load(c.peer, messages, 0)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openedReader()
	c.r.activeMessages = c.r.cache.Messages()
	c.r.mu.Unlock()
}

func (c *storedConversation) markUnread(ids ...domain.MessageID) {
	c.r.mu.Lock()
	for _, id := range ids {
		c.r.markUnreadLocked(c.peer, id)
	}
	c.r.mu.Unlock()
}

func activeCount(r *DMRouter) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.activeMessages)
}

func waitForActive(t *testing.T, r *DMRouter, want int) {
	t.Helper()
	if !pollCondition(3*time.Second, func() bool { return activeCount(r) == want }) {
		t.Fatalf("the conversation never showed %d messages (has %d)", want, activeCount(r))
	}
}

// TestReopeningAConversationAfterLeavingItShowsTheEndAndTheDivider: leaving
// a conversation (the back button of the single-pane layout) keeps its cache,
// and coming back to it is an open all the same — shown from its end, with
// the divider above what arrived meanwhile.
func TestReopeningAConversationAfterLeavingItShowsTheEndAndTheDivider(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusSeen)
	c.r.SelectPeer(c.peer)
	waitForActive(t, c.r, 2)
	c.r.ConsumePendingActions()

	c.r.DeselectPeer()
	c.store(t, "o3", 2, chatlog.StatusDelivered)
	c.store(t, "o4", 3, chatlog.StatusDelivered)
	c.markUnread("o3", "o4")

	c.r.SelectPeer(c.peer)
	waitForActive(t, c.r, 4)
	pending := c.r.ConsumePendingActions()
	first, placed := markerOf(c.r)
	settle(t, c.r)

	if !pending.ScrollToEnd {
		t.Fatal("coming back to a conversation did not show its end")
	}
	if !placed || first != "o3" {
		t.Fatalf("divider = %q (placed %v), want above o3, the first unread at the reopen", first, placed)
	}
}

// TestReturningFromAConversationThatFailedToLoadShowsTheEnd: A was open, B's
// load failed so the cache still holds A, and the user goes back to A. That
// is an open of A, not a reload of what is on screen.
func TestReturningFromAConversationThatFailedToLoadShowsTheEnd(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusSeen)
	c.r.SelectPeer(c.peer)
	waitForActive(t, c.r, 2)
	c.r.ConsumePendingActions()

	// What a selection of B leaves behind when its load fails: B selected,
	// nothing on screen, the cache still A's.
	c.r.mu.Lock()
	c.r.activePeer = domaintest.ID("failed-to-load")
	c.r.reader = openReaderFor(nil)
	c.r.activeMessages = nil
	c.r.mu.Unlock()
	c.store(t, "o3", 2, chatlog.StatusDelivered)
	c.markUnread("o3")

	c.r.SelectPeer(c.peer)
	waitForActive(t, c.r, 3)
	pending := c.r.ConsumePendingActions()
	first, placed := markerOf(c.r)
	settle(t, c.r)

	if !pending.ScrollToEnd {
		t.Fatal("going back to A after B failed to load did not show A's end")
	}
	if !placed || first != "o3" {
		t.Fatalf("divider = %q (placed %v), want above o3", first, placed)
	}
}

// TestASeededOpenLeavesTheDividerToTheLoadThatFollows: when the opening load
// fails, the one message the event carried is shown so the screen is not
// blank. That is not the conversation, and the load that does bring it is
// still the open: it shows the end and places the divider.
func TestASeededOpenLeavesTheDividerToTheLoadThatFollows(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.store(t, "o3", 2, chatlog.StatusDelivered)
	// selectPeerCore has run for a conversation whose badge held o2; its load
	// has not landed.
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(map[domain.MessageID]struct{}{"o2": {}})
	c.r.mu.Unlock()

	if !c.r.seedOpeningConversation(c.peer, c.r.peerStampOf(c.peer), c.message("o3", 2)) {
		t.Fatal("the seed was refused for the conversation being opened")
	}
	if !c.r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("the seeded message was not shown at the end")
	}

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the full load failed")
	}
	if !c.r.ConsumePendingActions().ScrollToEnd {
		t.Fatal("the load that brought the whole conversation did not show its end")
	}
	if first, placed := markerOf(c.r); !placed || first != "o2" {
		t.Fatalf("divider = %q (placed %v), want above o2, the first unread at open", first, placed)
	}
}

// TestAReportOutsideTheConversationIsDroppedWhole: a snapshot can carry the
// new selection with the previous conversation's messages, and a report made
// from it names a message this conversation does not have. Its AtEnd is about
// that other screen too.
func TestAReportOutsideTheConversationIsDroppedWhole(t *testing.T) {
	peer := domaintest.ID("reader-foreign-report")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})

	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "from-another-conversation", AtEnd: false})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	settle(t, r)

	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — a report about another screen moved this reader", got)
	}
	if sent := recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m2") {
		t.Fatalf("receipts = %v, want [m2]", sent)
	}
}

// TestStaleArrivalAtTheEndLeavesNoBadge: a message whose apply went stale is
// rebuilt from the store, which badges it — and the reader at the end saw it.
// One path settles it: one receipt, no badge left behind.
func TestStaleArrivalAtTheEndLeavesNoBadge(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "m1", 0, chatlog.StatusSeen)
	c.store(t, "m2", 1, chatlog.StatusDelivered)
	c.openWith(c.message("m1", 0))

	staleStamp := c.r.peerStampOf(c.peer)
	c.r.mu.Lock()
	c.r.moveHistoryBackwardsLocked(c.peer)
	c.r.mu.Unlock()
	arrival := c.message("m2", 1)
	c.r.deliverDecryptedMessage(&arrival, c.peer, staleStamp)
	settle(t, c.r)

	if got := unreadOf(c.r, c.peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — the reader at the end saw it", got)
	}
	if sent := c.recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m2") {
		t.Fatalf("receipts = %v, want exactly [m2]", sent)
	}
}

// TestStaleArrivalBelowTheReaderPlacesTheDivider: the same rebuild with the
// reader further up — badged, and the start of a new run.
func TestStaleArrivalBelowTheReaderPlacesTheDivider(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "m1", 0, chatlog.StatusSeen)
	c.store(t, "m2", 1, chatlog.StatusDelivered)
	c.openWith(c.message("m1", 0))
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})

	staleStamp := c.r.peerStampOf(c.peer)
	c.r.mu.Lock()
	c.r.moveHistoryBackwardsLocked(c.peer)
	c.r.mu.Unlock()
	arrival := c.message("m2", 1)
	c.r.deliverDecryptedMessage(&arrival, c.peer, staleStamp)
	settle(t, c.r)

	if got := unreadOf(c.r, c.peer); got != 1 {
		t.Fatalf("unread = %d, want 1", got)
	}
	if first, placed := markerOf(c.r); !placed || first != "m2" {
		t.Fatalf("divider = %q (placed %v), want above m2", first, placed)
	}
	if sent := c.recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestAnArrivalAlreadyPlacedIsNotAdmittedAgain: a message the cache already
// holds met the reader when it was placed — by its first delivery here, or by
// the reload that brought it in (TestAMessageAReloadBroughtInForAnotherMeetsTheReader).
// Admitting it again badges a message the reader has already read.
func TestAnArrivalAlreadyPlacedIsNotAdmittedAgain(t *testing.T) {
	peer := domaintest.ID("reader-duplicate-arrival")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})

	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	settle(t, r)

	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — m2 was read when it first landed", got)
	}
	if sent := recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m2") {
		t.Fatalf("receipts = %v, want exactly one [m2]", sent)
	}
}

// TestANewRunBelowOlderUnreadMovesTheDivider: the badge can hold messages
// above the reader — a receipt that failed puts them back. A message arriving
// below the reader after read ones is still the start of a new run.
func TestANewRunBelowOlderUnreadMovesTheDivider(t *testing.T) {
	peer := domaintest.ID("reader-run-below-old-badge")
	start := time.Now().Add(-time.Hour)
	r, _ := newReaderTestRouter(t, peer, []DirectMessage{
		incomingFrom(peer, "m1", start),
		incomingFrom(peer, "m2", start.Add(time.Minute)),
		incomingFrom(peer, "m3", start.Add(2*time.Minute)),
	})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: false})
	r.mu.Lock()
	r.markUnreadLocked(peer, "m1")
	r.mu.Unlock()

	deliverIncoming(t, r, peer, "m4", start.Add(3*time.Minute))
	if first, placed := markerOf(r); !placed || first != "m4" {
		t.Fatalf("divider = %q (placed %v), want above m4, the start of the new run", first, placed)
	}

	deliverIncoming(t, r, peer, "m5", start.Add(4*time.Minute))
	settle(t, r)
	if first, _ := markerOf(r); first != "m4" {
		t.Fatalf("divider = %q, want it kept above m4 — m5 continues the run", first)
	}
}

// TestRebuildFromTheStoreReappliesWhatTheReaderHasSeen: a rebuild replaces
// the badge with what the database calls unread, which can include messages
// the reader has on screen right now. Their position has not changed, so no
// report will come for them — the router reads them with what it already
// knows.
func TestRebuildFromTheStoreReappliesWhatTheReaderHasSeen(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "m1", 0, chatlog.StatusDelivered)
	c.store(t, "m2", 1, chatlog.StatusDelivered)
	c.open(t)
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})

	if !c.r.repairBadgeFromStore(c.peer) {
		t.Fatal("the rebuild failed")
	}
	settle(t, c.r)

	if got := unreadOf(c.r, c.peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — both messages are on screen", got)
	}
	if sent := c.recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m1", "m2") {
		t.Fatalf("receipts = %v, want [m1 m2]", sent)
	}
}

// TestAReportAfterShutdownMovesNothing: the report runs on the UI goroutine,
// and a router that can no longer send receipts must not take the badge down
// and put it back from there — that is a snapshot rebuild on the UI thread
// for nothing.
func TestAReportAfterShutdownMovesNothing(t *testing.T) {
	peer := domaintest.ID("reader-report-after-shutdown")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))
	settle(t, r)
	for len(r.uiEvents) > 0 {
		<-r.uiEvents
	}

	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m2", AtEnd: true})

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1", got)
	}
	if pending := len(r.uiEvents); pending != 0 {
		t.Fatalf("the report emitted %d UI events on the UI goroutine", pending)
	}
	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestEmptyingTheConversationPutsTheReaderBackAtTheEnd: a conversation with
// nothing left in it has no list to scroll and no position to report, so the
// next message lands on a screen showing nothing but it.
func TestEmptyingTheConversationPutsTheReaderBackAtTheEnd(t *testing.T) {
	peer := domaintest.ID("reader-emptied")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.ReportReaderPosition(peer, ReaderPosition{NewestSeen: "m1", AtEnd: false})
	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	r.evictDeletedMessageFromUI(peer, "m1")
	r.evictDeletedMessageFromUI(peer, "m2")
	if _, placed := markerOf(r); placed {
		t.Fatal("an empty conversation still carries an unread divider")
	}

	deliverIncoming(t, r, peer, "m3", start.Add(2*time.Minute))
	settle(t, r)
	if got := unreadOf(r, peer); got != 0 {
		t.Fatalf("unread = %d, want 0 — m3 is all there is on screen", got)
	}
	if sent := recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "m3") {
		t.Fatalf("receipts = %v, want [m3]", sent)
	}
}

// flatten is every id the receipts carried, in the order they were sent.
func flatten(batches [][]domain.MessageID) []domain.MessageID {
	var ids []domain.MessageID
	for _, batch := range batches {
		ids = append(ids, batch...)
	}
	return ids
}

func countID(ids []domain.MessageID, want domain.MessageID) int {
	n := 0
	for _, id := range ids {
		if id == want {
			n++
		}
	}
	return n
}

// TestAMessageAReloadBroughtInForAnotherMeetsTheReader: a reload run for one
// message (here, a decrypt failure on x1) brings in every message written by
// then — m2 among them, whose own delivery then finds it already in the cache.
// The reload is what put m2 in front of the reader, so the reload is what has
// to ask the reader about it.
func TestAMessageAReloadBroughtInForAnotherMeetsTheReader(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "x0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "x0", AtEnd: false})
			}
			c.store(t, "x1", 1, chatlog.StatusDelivered)
			c.store(t, "m2", 2, chatlog.StatusDelivered)

			if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
				t.Fatal("the reload failed")
			}
			arrival := c.message("m2", 2)
			c.r.deliverDecryptedMessage(&arrival, c.peer, c.r.peerStampOf(c.peer))
			pending := c.r.ConsumePendingActions()
			marker, placed := markerOf(c.r)
			settle(t, c.r)

			if pending.ScrollToEnd {
				t.Fatal("a reload of the conversation on screen asked to scroll")
			}
			sent := flatten(c.recorder.sent())
			unread := unreadOf(c.r, c.peer)
			if placement.atEnd {
				if unread != 0 || countID(sent, "m2") != 1 || countID(sent, "x1") != 1 {
					t.Fatalf("unread = %d, receipts = %v; want 0, and x1 and m2 once each", unread, c.recorder.sent())
				}
				return
			}
			if unread != 2 || len(sent) != 0 {
				t.Fatalf("unread = %d, receipts = %v; want x1 and m2 badged, none sent", unread, c.recorder.sent())
			}
			if !placed || marker != "x1" {
				t.Fatalf("divider = %q (placed %v), want above x1, where the run starts", marker, placed)
			}
		})
	}
}

// TestAnArrivalAlreadyInTheCacheBeforeItsEventIsAdmitted: the event for m2
// arrives after a reload has already placed it, and the event path stops at
// "the cache has it". The message still meets the reader — through the
// reload that placed it.
func TestAnArrivalAlreadyInTheCacheBeforeItsEventIsAdmitted(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "a0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "a0", AtEnd: false})
			}
			c.store(t, "m2", 1, chatlog.StatusDelivered)
			if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
				t.Fatal("the reload failed")
			}

			c.r.onNewMessage(protocol.LocalChangeEvent{
				Type: protocol.LocalChangeNewMessage, Topic: "dm", MessageID: "m2",
				Sender: c.sender.Address, Recipient: c.me.Address,
				Body: "not needed: the cache has it", CreatedAt: c.at(1).Format(time.RFC3339Nano),
			})

			checkArrival(t, c, placement, "m2")
		})
	}
}

// TestAReceiptReloadMeetsTheReader: a receipt for a message the cache does
// not hold reloads the open conversation, and whatever that brings in that
// nobody has seen meets the reader like any other arrival.
func TestAReceiptReloadMeetsTheReader(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "r0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "r0", AtEnd: false})
			}
			c.store(t, "r1", 1, chatlog.StatusDelivered)

			c.r.onReceiptUpdate(protocol.LocalChangeEvent{
				MessageID: "an-outgoing-message-not-in-the-cache",
				Sender:    c.me.Address, Recipient: c.sender.Address,
				Status: "delivered",
			})

			checkArrival(t, c, placement, "r1")
		})
	}
}

// TestADeletionInALeftConversationDoesNotReachTheScreen: A's cache stays warm
// after the user leaves it for B, and a deletion in A republishes A's cache —
// which must not become what B's screen shows, nor something a report about
// B's screen is checked against.
func TestADeletionInALeftConversationDoesNotReachTheScreen(t *testing.T) {
	left := domaintest.ID("reader-left-warm-cache")
	opened := domaintest.ID("reader-opened-instead")
	start := time.Now().Add(-time.Hour)
	r, _ := newReaderTestRouter(t, left, []DirectMessage{
		incomingFrom(left, "m1", start),
		incomingFrom(left, "m2", start.Add(time.Minute)),
	})
	r.DeselectPeer()
	r.mu.Lock()
	r.tryEnsurePeerLocked(opened)
	r.activePeer = opened
	r.reader = openReaderFor(nil)
	r.activeMessages = nil
	r.mu.Unlock()

	r.evictDeletedMessageFromUI(left, "m1")
	settle(t, r)

	if got := activeCount(r); got != 0 {
		t.Fatalf("B's screen shows %d of A's messages", got)
	}
}

// TestAReaderAlreadyScrollingIsNotTakenToTheEndByALateLoad: the opening load
// failed, but the warm cache put the conversation on screen anyway, and the
// reader has started scrolling it. A load that succeeds later is not the open
// any more — it must not pull the reader to the end. The divider it can still
// place: a divider moves no one.
func TestAReaderAlreadyScrollingIsNotTakenToTheEndByALateLoad(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o1", 0), c.message("o2", 1)}, 0)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(map[domain.MessageID]struct{}{"o2": {}})
	c.r.refreshActiveMessagesLocked()
	c.r.mu.Unlock()
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "o1", AtEnd: false})
	c.r.ConsumePendingActions()

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the late load failed")
	}
	pending := c.r.ConsumePendingActions()
	marker, placed := markerOf(c.r)
	settle(t, c.r)

	if pending.ScrollToEnd {
		t.Fatal("a load after the reader had started scrolling took them to the end")
	}
	if !placed || marker != "o2" {
		t.Fatalf("divider = %q (placed %v), want above o2, the first unread at open", marker, placed)
	}
}

// TestAnArrivalReadAtShutdownStaysBadged: read as it landed, but the router
// was already shutting down and could send no receipt — so it stays unread.
func TestAnArrivalReadAtShutdownStaysBadged(t *testing.T) {
	peer := domaintest.ID("reader-arrival-at-shutdown")
	start := time.Now().Add(-time.Hour)
	r, recorder := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	settle(t, r)

	deliverIncoming(t, r, peer, "m2", start.Add(time.Minute))

	if got := unreadOf(r, peer); got != 1 {
		t.Fatalf("unread = %d, want 1 — no receipt could be sent", got)
	}
	if sent := recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestARunStartForAMessageNotInTheConversation: a message the conversation
// does not hold has no neighbours, so it starts its own run.
func TestARunStartForAMessageNotInTheConversation(t *testing.T) {
	peer := domaintest.ID("reader-run-absent")
	start := time.Now().Add(-time.Hour)
	r, _ := newReaderTestRouter(t, peer, []DirectMessage{incomingFrom(peer, "m1", start)})
	r.mu.Lock()
	first, newRun := r.unreadRunStartLocked(peer, "absent")
	r.mu.Unlock()
	if first != "absent" || !newRun {
		t.Fatalf("run start = (%q, %v), want (absent, true)", first, newRun)
	}
}

// TestAStartupReloadMeetsTheReader: startup that could not keep every event
// re-reads the open conversation, and what that brings in that nobody has seen
// meets the reader like any other arrival — the dropped event that would have
// carried it is gone for good.
func TestAStartupReloadMeetsTheReader(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "s0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "s0", AtEnd: false})
			}
			c.store(t, "s1", 1, chatlog.StatusDelivered)

			c.r.reloadAfterStartupDrops(1)

			checkArrival(t, c, placement, "s1")
		})
	}
}

// TestAnOpenCarriedOutByAnotherReloadReadsTheConversation: the opening load
// failed, so the open is still waiting, and the next load of the conversation
// is run for something else — a receipt, here. That load is the one that puts
// the conversation on screen from its end, so the open it carries out reads
// the conversation: the selection whose load failed is not coming back to do
// it, and what this load brought in has met no one else.
func TestAnOpenCarriedOutByAnotherReloadReadsTheConversation(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	// What a selection whose load failed leaves behind: the conversation
	// selected, its open waiting, the badge it had put back, the cache
	// nobody's.
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(map[domain.MessageID]struct{}{"o1": {}})
	c.r.markUnreadLocked(c.peer, "o1")
	c.r.mu.Unlock()

	c.r.onReceiptUpdate(protocol.LocalChangeEvent{
		MessageID: "an-outgoing-message-not-in-the-cache",
		Sender:    c.me.Address, Recipient: c.sender.Address,
		Status: "delivered",
	})
	pollCondition(3*time.Second, func() bool { return len(c.recorder.sent()) > 0 })
	pending := c.r.ConsumePendingActions()
	marker, placed := markerOf(c.r)
	settle(t, c.r)

	if !pending.ScrollToEnd {
		t.Fatal("the open did not take the reader to the end")
	}
	if !placed || marker != "o1" {
		t.Fatalf("divider = %q (placed %v), want above o1, the first unread at open", marker, placed)
	}
	sent := c.recorder.sent()
	if unread := unreadOf(c.r, c.peer); unread != 0 || len(sent) != 1 || !containsID(sent[0], "o1") || !containsID(sent[0], "o2") {
		t.Fatalf("unread = %d, receipts = %v; want 0 and one batch carrying o1 and o2", unread, sent)
	}
}

// TestAReportBeforeTheOpeningLoadGivesTheBadgeBack: the selection cleared the
// badge because the open reads the whole conversation. A reader already
// scrolling the conversation the warm cache put on screen cancels that open —
// so the badge comes back, and is read the way this reader reads: by what
// they scroll past. The load that lands later reads nothing on its own.
func TestAReportBeforeTheOpeningLoadGivesTheBadgeBack(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.store(t, "o3", 2, chatlog.StatusDelivered)
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o1", 0), c.message("o2", 1), c.message("o3", 2)}, 0)
	c.r.mu.Lock()
	c.r.markUnreadLocked(c.peer, "o2")
	c.r.markUnreadLocked(c.peer, "o3")
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(c.r.unreadSnapshotLocked(c.peer))
	c.r.clearUnreadLocked(c.peer)
	c.r.refreshActiveMessagesLocked()
	c.r.mu.Unlock()

	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "o2", AtEnd: false})
	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the opening load failed")
	}
	settle(t, c.r)

	if unread := unreadOf(c.r, c.peer); unread != 1 {
		t.Fatalf("unread = %d, want 1 — o3 is below the reader", unread)
	}
	if sent := c.recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "o2") {
		t.Fatalf("receipts = %v, want exactly [o2], the one the reader scrolled past", sent)
	}
}

// TestAnOpenAsksForTheEndAtOnce: the selection itself asks for the end, not
// only the load that follows. The UI keeps the list's position across a
// conversation switch, so without it a warm cache published before the load —
// by a receipt, an arrival, a deletion — is laid out wherever the previous
// conversation was scrolled to, and its first report says "not at the end"
// for a reader who never moved. The router is shut down so that no load runs:
// what is pending is the selection's own request.
func TestAnOpenAsksForTheEndAtOnce(t *testing.T) {
	t.Run("another conversation", func(t *testing.T) {
		r := newTestRouter()
		target := domaintest.ID("reader-open-asks-for-the-end")
		r.mu.Lock()
		r.tryEnsurePeerLocked(target)
		r.mu.Unlock()
		settle(t, r)

		r.SelectPeer(target)

		if !r.ConsumePendingActions().ScrollToEnd {
			t.Fatal("opening a conversation did not ask for its end")
		}
	})
	t.Run("a retry after a failed load", func(t *testing.T) {
		peer := domaintest.ID("reader-retry-asks-for-the-end")
		r, _ := newReaderTestRouter(t, peer, nil)
		r.cache.Load(domaintest.ID("someone-else"), nil, 0)
		settle(t, r)

		r.SelectPeer(peer)

		if !r.ConsumePendingActions().ScrollToEnd {
			t.Fatal("retrying a conversation whose load failed did not ask for its end")
		}
	})
}

// TestAWarmCacheShownBeforeItsLoadIsStillOpened: the user opens A, whose cache
// is still warm, and a receipt publishes that cache before A's load lands. The
// UI lays it out from the end — the selection asked for it — and reports so.
// That report is not a reader who has gone elsewhere: the load still carries
// out the open, showing the end and reading the whole conversation.
func TestAWarmCacheShownBeforeItsLoadIsStillOpened(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusSeen)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.store(t, "o3", 2, chatlog.StatusDelivered)
	mine := DirectMessage{
		ID: "mine", Sender: domain.PeerIdentityFromWire(c.me.Address), Recipient: c.peer,
		Body: "mine", Timestamp: c.at(3), ReceiptStatus: MessageStatusSent,
	}
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o1", 0), c.message("o2", 1), c.message("o3", 2), mine}, 0)
	c.r.mu.Lock()
	c.r.markUnreadLocked(c.peer, "o2")
	c.r.markUnreadLocked(c.peer, "o3")
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(c.r.unreadSnapshotLocked(c.peer))
	c.r.clearUnreadLocked(c.peer)
	c.r.mu.Unlock()

	c.r.onReceiptUpdate(protocol.LocalChangeEvent{
		MessageID: "mine", Sender: c.me.Address, Recipient: c.sender.Address, Status: "delivered",
	})
	if got := activeCount(c.r); got != 4 {
		t.Fatalf("the receipt did not put the warm cache on screen (%d messages)", got)
	}
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "mine", AtEnd: true})
	c.r.ConsumePendingActions()

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the opening load failed")
	}
	pending := c.r.ConsumePendingActions()
	settle(t, c.r)

	if !pending.ScrollToEnd {
		t.Fatal("the load did not carry out the open")
	}
	sent := c.recorder.sent()
	if len(sent) != 1 || !containsID(sent[0], "o1") || !containsID(sent[0], "o2") || !containsID(sent[0], "o3") {
		t.Fatalf("receipts = %v, want the open's one batch over the whole conversation", sent)
	}
	if got := unreadOf(c.r, c.peer); got != 0 {
		t.Fatalf("unread = %d after the open, want 0", got)
	}
}

// TestASeededMessageIsReadOnce: when the opening load fails, the one message
// the event carried is put on screen at the end — so it is read there and
// then, like any arrival the reader sees land. The load that later carries
// out the open reads the rest of the conversation, not that message again.
func TestASeededMessageIsReadOnce(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "e2", 1, chatlog.StatusDelivered)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(nil)
	c.r.mu.Unlock()

	if !c.r.seedOpeningConversation(c.peer, c.r.peerStampOf(c.peer), c.message("e2", 1)) {
		t.Fatal("the seed was refused for the conversation being opened")
	}
	pollCondition(time.Second, func() bool { return len(c.recorder.sent()) > 0 })
	if sent := c.recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "e2") {
		t.Fatalf("receipts after the seed = %v, want [e2] — it is on screen at the end", sent)
	}

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the load that follows failed")
	}
	settle(t, c.r)

	sent := flatten(c.recorder.sent())
	if countID(sent, "e2") != 1 || countID(sent, "o1") != 1 {
		t.Fatalf("receipts = %v, want e2 and o1 once each", c.recorder.sent())
	}
}

// TestAnArrivalReadWhileTheOpenWaitsIsNotReadAgain: the warm cache is on
// screen, the open's load has not landed, and a message arrives with the
// reader at the end — read as it lands. The open then reads the
// conversation, and that message already has its receipt.
func TestAnArrivalReadWhileTheOpenWaitsIsNotReadAgain(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "m2", 1, chatlog.StatusDelivered)
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o1", 0)}, 0)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(nil)
	c.r.refreshActiveMessagesLocked()
	c.r.mu.Unlock()

	arrival := c.message("m2", 1)
	if !c.r.deliverDecryptedMessage(&arrival, c.peer, c.r.peerStampOf(c.peer)) {
		t.Fatal("m2 was not delivered to the open conversation")
	}
	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the opening load failed")
	}
	settle(t, c.r)

	sent := flatten(c.recorder.sent())
	if countID(sent, "m2") != 1 || countID(sent, "o1") != 1 {
		t.Fatalf("receipts = %v, want m2 and o1 once each", c.recorder.sent())
	}
}

// TestTheOpenReadsInTheBackground: reading the opened conversation waits on
// the receipt RPC, and the load that carries the open out is run by paths
// that must not wait with it — the selection, which publishes the
// conversation only after the load returns, and an ebus subscriber or the
// startup goroutine for the reloads. The load returns first; the read
// follows.
func TestTheOpenReadsInTheBackground(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	release := make(chan struct{})
	c.r.markConversationSeenFn = func(ctx context.Context, peer domain.PeerIdentity, batch []DirectMessage) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return c.recorder.record(ctx, peer, batch)
	}
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(nil)
	c.r.mu.Unlock()

	returned := make(chan bool, 1)
	go func() { returned <- c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) }()
	select {
	case ok := <-returned:
		if !ok {
			t.Fatal("the opening load failed")
		}
	case <-time.After(time.Second):
		close(release)
		<-returned
		t.Fatal("the opening load waited for the receipt RPC")
	}
	close(release)
	settle(t, c.r)
	if sent := c.recorder.sent(); len(sent) != 1 || !idsEqual(sent[0], "o1") {
		t.Fatalf("receipts = %v, want the open's [o1]", sent)
	}
}

// TestAFailedOpenReadPutsTheBadgeBack: the selection cleared the badge because
// the open reads the conversation; when that read fails, the database still
// calls those messages unread, and the badge is what says so.
func TestAFailedOpenReadPutsTheBadgeBack(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.markUnread("o1", "o2")
	c.recorder.fail = true

	c.r.SelectPeer(c.peer)
	waitForActive(t, c.r, 2)
	pollCondition(3*time.Second, func() bool { return len(c.recorder.sent()) > 0 })
	settle(t, c.r)

	if got := unreadOf(c.r, c.peer); got != 2 {
		t.Fatalf("unread = %d after the open's read failed, want 2", got)
	}
}

// TestADividerFromANewerRunSurvivesTheLoadThatFindsTheOpenUnread: the reader
// scrolled the warm cache before the open's load landed — down past o1, not
// to the end — so the open is off; a message then arrives below them and
// starts a new run. The load that lands later finds o1, the message that was
// unread at the open — but the divider above the newer run is the answer to
// "where did I stop reading" now, and it stays.
func TestADividerFromANewerRunSurvivesTheLoadThatFindsTheOpenUnread(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o0", 0, chatlog.StatusSeen)
	c.store(t, "o1", 1, chatlog.StatusDelivered)
	c.store(t, "n3", 2, chatlog.StatusDelivered)
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o0", 0), c.message("o1", 1)}, 0)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(map[domain.MessageID]struct{}{"o1": {}})
	c.r.refreshActiveMessagesLocked()
	c.r.mu.Unlock()
	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "o1", AtEnd: false})
	arrival := c.message("n3", 2)
	if !c.r.deliverDecryptedMessage(&arrival, c.peer, c.r.peerStampOf(c.peer)) {
		t.Fatal("n3 was not delivered to the open conversation")
	}

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the late load failed")
	}
	settle(t, c.r)

	if first, placed := markerOf(c.r); !placed || first != "n3" {
		t.Fatalf("divider = %q (placed %v), want it kept above n3", first, placed)
	}
}

// TestAReloadedMessageTheStoreCallsSeenIsLeftAlone: a reload can bring in a
// message the cache never held that the database already calls seen — read
// before, wherever. It is neither badged for a reader scrolled up nor sent a
// second receipt for a reader at the end.
func TestAReloadedMessageTheStoreCallsSeenIsLeftAlone(t *testing.T) {
	for _, placement := range readerPlacements {
		t.Run(placement.name, func(t *testing.T) {
			c := newStoredConversation(t)
			c.store(t, "k0", 0, chatlog.StatusSeen)
			c.open(t)
			if !placement.atEnd {
				c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "k0", AtEnd: false})
			}
			c.store(t, "k1", 1, chatlog.StatusSeen)

			if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
				t.Fatal("the reload failed")
			}
			settle(t, c.r)

			if got := unreadOf(c.r, c.peer); got != 0 {
				t.Fatalf("unread = %d, want 0 — the database calls k1 seen", got)
			}
			if sent := c.recorder.sent(); len(sent) != 0 {
				t.Fatalf("receipts = %v, want none", sent)
			}
		})
	}
}

// TestAnOpenLoadAfterShutdownGivesTheBadgeBack: the selection cleared the
// badge because the open reads the conversation. A router shutting down when
// the load lands can send no receipt, so nothing is read — and the badge it
// cleared goes back.
func TestAnOpenLoadAfterShutdownGivesTheBadgeBack(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.markUnread("o1", "o2")
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(c.r.unreadSnapshotLocked(c.peer))
	c.r.clearUnreadLocked(c.peer)
	c.r.mu.Unlock()
	settle(t, c.r)

	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the opening load failed")
	}

	if got := unreadOf(c.r, c.peer); got != 2 {
		t.Fatalf("unread = %d after an open the shutdown could not read, want 2", got)
	}
	if sent := c.recorder.sent(); len(sent) != 0 {
		t.Fatalf("receipts = %v, want none", sent)
	}
}

// TestWhatAReportReadWhileTheOpenWaitsIsNotReadAgain: with the open still
// waiting for its load, a report at the end reads what is badged on screen —
// put back by a failed receipt or a rebuild. The open then reads the
// conversation, and those messages have their receipts already.
func TestWhatAReportReadWhileTheOpenWaitsIsNotReadAgain(t *testing.T) {
	c := newStoredConversation(t)
	c.store(t, "o1", 0, chatlog.StatusDelivered)
	c.store(t, "o2", 1, chatlog.StatusDelivered)
	c.r.cache.Load(c.peer, []DirectMessage{c.message("o1", 0), c.message("o2", 1)}, 0)
	c.r.mu.Lock()
	c.r.activePeer = c.peer
	c.r.reader = openReaderFor(nil)
	c.r.refreshActiveMessagesLocked()
	c.r.markUnreadLocked(c.peer, "o1")
	c.r.mu.Unlock()

	c.r.ReportReaderPosition(c.peer, ReaderPosition{NewestSeen: "o2", AtEnd: true})
	pollCondition(3*time.Second, func() bool { return len(c.recorder.sent()) > 0 })
	if !c.r.loadConversation(c.peer, c.r.peerEpochsOf(c.peer)) {
		t.Fatal("the opening load failed")
	}
	settle(t, c.r)

	sent := flatten(c.recorder.sent())
	if countID(sent, "o1") != 1 || countID(sent, "o2") != 1 {
		t.Fatalf("receipts = %v, want o1 and o2 once each", c.recorder.sent())
	}
}
