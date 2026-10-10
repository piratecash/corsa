package service

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// The open conversation is read where the READER is, not where the router is.
//
// "The chat is selected" and "the user has seen this message" stopped being the
// same claim the moment a conversation could be scrolled: a reader who has gone
// up to look at last week is not looking at what arrives at the bottom, and
// marking it read sends the peer a receipt for a message nobody saw — and pulls
// the reader down to it, which is how the arrival reached the screen at all.
//
// So the UI tells the router what is on screen (ReportReaderPosition), and the
// router keeps the two rules that follow from it:
//
//   - a message arriving while the end of the conversation is on screen is read
//     the moment it lands, exactly as before;
//   - one arriving while the reader is elsewhere is badged like a message in any
//     other conversation, and stays unread until it is scrolled into view.
//
// Opening a conversation is the third rule. An open is every selection that
// puts a conversation on screen — the first one, a return to it after leaving
// (its cache may still be warm), a retry after a failed load — and it is
// carried out, whole and once, by the load that brings the messages: that
// load shows the end, places the divider and reads the conversation
// (showOpenedConversationLocked, readOpenedConversationInBackground),
// whichever path ran it. Whether the cache already held the peer says nothing about whether
// the reader is looking at it.

// ReaderPosition is what the reader of the open conversation has on screen, as
// the UI reports it after laying the conversation out.
type ReaderPosition struct {
	// NewestSeen is the newest message that is on screen. Everything at or
	// before it in the conversation counts as read — the reader came down
	// past it to get here.
	NewestSeen domain.MessageID
	// AtEnd says the conversation is scrolled to its end, so a message that
	// arrives now lands on screen.
	AtEnd bool
}

// UnreadMarker is where the open conversation's "unread messages" divider
// goes: above the first message of the newest unread run.
//
// It outlives the run it marks on purpose. The divider answers "where did I
// stop reading", and it has to stay where it is while the reader scrolls down
// through what it marks — a divider that followed the first still-unread
// message would crawl down the screen under the reader's eyes. It moves only
// when a NEW run starts, which is when the old answer stops being the useful
// one.
type UnreadMarker struct {
	first  domain.MessageID
	placed bool
}

// unreadMarkerAt puts the divider above messageID.
func unreadMarkerAt(messageID domain.MessageID) UnreadMarker {
	return UnreadMarker{first: messageID, placed: true}
}

// FirstUnread is the message the divider sits above, and false when the open
// conversation has none.
func (m UnreadMarker) FirstUnread() (domain.MessageID, bool) {
	return m.first, m.placed
}

// openReader is the router's view of the reader of the open conversation.
// Guarded by DMRouter.mu. It belongs to activePeer and is replaced whenever
// that changes (see openReaderFor), so nothing in it can be carried from one
// conversation into another.
type openReader struct {
	// atEnd says what lands now lands on screen. An opened conversation is
	// shown from its end, so that is where a reader starts.
	atEnd bool
	// marker is the divider the UI draws.
	marker UnreadMarker
	// newestSeen is the newest message the UI last reported on screen, and
	// hasNewestSeen says whether it has reported at all since the open. It is
	// kept for the one case the UI cannot know about: a rebuild of the badge
	// from the database, which can put back messages the reader is looking
	// at right now — their position has not changed, so no report is coming
	// for them (rereadThroughReaderPosition).
	newestSeen    domain.MessageID
	hasNewestSeen bool
	// unreadAtOpen is the badge the conversation had when it was opened. The
	// open clears the badge before the messages are loaded, so this is the
	// only record of where the divider goes.
	unreadAtOpen map[domain.MessageID]struct{}
	// awaitingOpenLoad says the open has not been carried out yet: the next
	// successful load of this conversation is the one that brings it onto
	// the screen, and it shows the end, places the divider and reads the
	// conversation. A report away from the end cancels it instead.
	awaitingOpenLoad bool
	// readWhileOpening is what met the reader and was read while the open
	// waited for its load — the seeded message, an arrival into the warm
	// cache with the end on screen, a badged message a report took. Those
	// have their receipts already, and the open's read leaves them out.
	readWhileOpening map[domain.MessageID]struct{}
}

// openReaderFor is the reader of a conversation that is being opened, with the
// badge it had at that moment.
func openReaderFor(unreadAtOpen map[domain.MessageID]struct{}) openReader {
	return openReader{atEnd: true, unreadAtOpen: unreadAtOpen, awaitingOpenLoad: true}
}

// noReader is the reader when no conversation is open. Nothing arrives into a
// conversation that is not open, so none of it is consulted; it exists so the
// field is never left describing a conversation that is gone.
func noReader() openReader {
	return openReader{}
}

// ReportReaderPosition records what the reader of peer's conversation has on
// screen, and marks read every unread message they have now scrolled past.
//
// The report is dropped whole when it does not describe the open
// conversation: another peer, or a NewestSeen this conversation does not hold.
// The UI lays out the snapshot it has, and a snapshot can carry the new
// selection over the previous conversation's messages — a position read off
// that screen, AtEnd included, says nothing about this one.
//
// Called on the UI goroutine whenever the position changes, so only the state
// change happens here: the snapshot rebuild and the receipts run in the
// background, and the badge comes back if the receipts fail. A router that is
// shutting down can send no receipts, so it takes nothing off the badge.
func (r *DMRouter) ReportReaderPosition(peer domain.PeerIdentity, position ReaderPosition) {
	peer = normalizePeer(peer)
	if !r.beginOp() {
		return
	}
	r.mu.Lock()
	if !r.positionDescribesOpenLocked(peer, position) {
		r.mu.Unlock()
		r.endOp()
		return
	}
	r.reader.atEnd = position.AtEnd
	r.reader.newestSeen = position.NewestSeen
	r.reader.hasNewestSeen = true
	badgeBack := false
	if !position.AtEnd {
		badgeBack = r.cancelPendingOpenLocked(peer)
	}
	batch := r.takeUnreadThroughLocked(peer, position.NewestSeen)
	r.mu.Unlock()

	if !badgeBack && len(batch) == 0 {
		r.endOp()
		return
	}
	go func() {
		defer r.endOp()
		defer recoverLog("ReportReaderPosition")
		r.notify(UIEventSidebarUpdated)
		if len(batch) > 0 {
			r.confirmSeen(peer, batch)
		}
	}()
}

// cancelPendingOpenLocked calls off an open that has not been carried out
// yet, for a reader who has already scrolled up the conversation the warm
// cache put on screen before its load landed, and reports whether that gave
// the badge back.
//
// Only a report away from the end does that. The first report comes from the
// first layout, not from the reader moving, and the open asked for the end the
// moment it was selected — a report AT the end is the open working, and leaves
// it to its load.
//
// Whatever load lands later is not the one that shows the conversation to
// this reader, so it must neither take them to the end nor read the
// conversation for them. The selection cleared the badge because the open was
// going to read all of it; with the open off, that badge goes back, to be read
// the way this reader reads — by what they scroll past. Callers hold r.mu.
func (r *DMRouter) cancelPendingOpenLocked(peer domain.PeerIdentity) bool {
	if !r.reader.awaitingOpenLoad {
		return false
	}
	r.reader.awaitingOpenLoad = false
	r.reader.readWhileOpening = nil
	r.restoreUnreadLocked(peer, r.reader.unreadAtOpen)
	return len(r.reader.unreadAtOpen) > 0
}

// positionDescribesOpenLocked says whether a report is about the conversation
// that is open now. Callers hold r.mu.
func (r *DMRouter) positionDescribesOpenLocked(peer domain.PeerIdentity, position ReaderPosition) bool {
	if peer.IsZero() || r.activePeer != peer {
		return false
	}
	return indexOfMessage(r.activeMessages, position.NewestSeen) >= 0
}

// rereadThroughReaderPosition reads, with what the reader last reported, the
// messages a rebuild of the badge has just put back. Without it they would
// stay badged on the conversation in front of the user until the reader
// happened to scroll.
//
// Only a rebuild asks, never the receipt this sends: one that fails puts its
// messages back because the database still calls them unread, and asking
// again from there would be a retry loop with nothing to stop it. A failed
// read of the WHOLE conversation (putBadgeBack) does end in a
// rebuild, and so in one more pass through here — once, because the receipt
// that pass sends is a reader-position receipt, and its failure rebuilds
// nothing.
func (r *DMRouter) rereadThroughReaderPosition(peer domain.PeerIdentity) {
	r.mu.Lock()
	if peer.IsZero() || r.activePeer != peer || !r.reader.hasNewestSeen {
		r.mu.Unlock()
		return
	}
	batch := r.takeUnreadThroughLocked(peer, r.reader.newestSeen)
	r.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	r.notify(UIEventSidebarUpdated)
	r.sendSeenReceipts(peer, batch)
}

// takeUnreadThroughLocked removes from the badge every unread message of the
// open conversation at or before newest, and returns them for the receipt.
// Messages after newest are below what the reader has on screen and stay
// unread. Callers hold r.mu.
func (r *DMRouter) takeUnreadThroughLocked(peer domain.PeerIdentity, newest domain.MessageID) []DirectMessage {
	unread := r.unreadIDs[peer]
	if len(unread) == 0 {
		return nil
	}
	through := indexOfMessage(r.activeMessages, newest)
	if through < 0 {
		// Not a message of the conversation as the router has it — a
		// reload or a deletion has replaced it since it was reported.
		return nil
	}
	var batch []DirectMessage
	for _, msg := range r.activeMessages[:through+1] {
		if _, waiting := unread[domain.MessageID(msg.ID)]; waiting {
			batch = append(batch, msg)
			// Read now, so not again by an open still waiting for its load.
			r.noteReadWhileOpeningLocked(domain.MessageID(msg.ID))
		}
	}
	r.dropUnreadLocked(peer, messageIDsOf(batch)...)
	return batch
}

// admitArrivalLocked decides what an incoming message that has just landed in
// the open conversation is to its reader, and reports whether it was seen.
//
// With the end of the conversation on screen it is seen as it lands, and the
// caller sends its receipt. It is taken off the badge too: a rebuild from the
// database can have put it there first, and a badge left behind would be read
// again — a second receipt — by the next report. Otherwise it lands below what
// the reader is looking at: it is badged like a message in any other
// conversation, and the divider goes where its run starts.
// Callers hold r.mu.
func (r *DMRouter) admitArrivalLocked(peer domain.PeerIdentity, messageID domain.MessageID) bool {
	if r.reader.atEnd {
		r.dropUnreadLocked(peer, messageID)
		r.noteReadWhileOpeningLocked(messageID)
		return true
	}
	r.markUnreadLocked(peer, messageID)
	start, newRun := r.unreadRunStartLocked(peer, messageID)
	if newRun || !r.reader.marker.placed {
		r.reader.marker = unreadMarkerAt(start)
	}
	return false
}

// unreadRunStartLocked is where the unread run that messageID belongs to
// begins, and whether messageID is that beginning — a new run.
//
// A run is the stretch of unread incoming messages that ends at messageID;
// the user's own messages in between neither start nor break one. Deciding by
// the neighbour rather than by "is the badge empty" matters because the badge
// can hold messages far above the reader — a receipt that failed puts them
// back, a rebuild from the database can too — and a message arriving below
// read ones starts a new run whatever is badged up there.
// Callers hold r.mu.
func (r *DMRouter) unreadRunStartLocked(peer domain.PeerIdentity, messageID domain.MessageID) (domain.MessageID, bool) {
	at := indexOfMessage(r.activeMessages, messageID)
	if at < 0 {
		return messageID, true
	}
	me := r.client.Address()
	unread := r.unreadIDs[peer]
	start := messageID
	for i := at - 1; i >= 0; i-- {
		earlier := r.activeMessages[i]
		if earlier.Sender == me {
			continue
		}
		if _, waiting := unread[domain.MessageID(earlier.ID)]; !waiting {
			break
		}
		start = domain.MessageID(earlier.ID)
	}
	return start, start == messageID
}

// heldMessageIDsLocked is what the cache holds for peer before a load
// replaces it: the messages that have already met the reader. Empty when the
// cache belongs to someone else. Callers hold r.mu.
func (r *DMRouter) heldMessageIDsLocked(peer domain.PeerIdentity) map[domain.MessageID]struct{} {
	if !r.cache.MatchesPeer(peer) {
		return nil
	}
	cached := r.cache.Messages()
	held := make(map[domain.MessageID]struct{}, len(cached))
	for i := range cached {
		held[domain.MessageID(cached[i].ID)] = struct{}{}
	}
	return held
}

// admitReloadedArrivalsLocked meets the reader with every incoming message a
// reload of the open conversation brought in, and returns the ones the reader
// saw land, for their receipts.
//
// It is one path for every reload, whatever it was run for — a decrypt
// failure, a receipt for a message the cache did not hold, the header repair,
// a stale apply, the startup re-read and its retries — because a reload brings
// in EVERYTHING written by then, not only the message it was run for.
// Admitting per path, by the id each one knew about, left the others in front
// of the reader with neither a receipt nor a badge. And because this is where
// a reloaded message meets the reader, a delivery or an event that later finds
// it already in the cache has nothing left to do (cacheAppendAlreadyHeld, the
// HasMessage early return in onNewMessage).
//
// New means not in the cache before the load; a message the database already
// calls seen was read before, wherever, and is left alone. Walked oldest
// first, so the first new unread one is where a run starts and the rest
// continue it. Callers hold r.mu.
func (r *DMRouter) admitReloadedArrivalsLocked(peer domain.PeerIdentity, held map[domain.MessageID]struct{}) []DirectMessage {
	me := r.client.Address()
	var seen []DirectMessage
	for _, msg := range r.activeMessages {
		id := domain.MessageID(msg.ID)
		if _, alreadyMet := held[id]; alreadyMet {
			continue
		}
		if msg.Sender == me || msg.ReceiptStatus == protocol.ReceiptStatusSeen {
			continue
		}
		if r.admitArrivalLocked(peer, id) {
			seen = append(seen, msg)
		}
	}
	return seen
}

// noteReadWhileOpeningLocked records a message read as it landed while the
// open still waits for its load, so that the open's read leaves it out: it
// has its receipt, and the open would send it a second one. Callers hold r.mu.
func (r *DMRouter) noteReadWhileOpeningLocked(messageID domain.MessageID) {
	if !r.reader.awaitingOpenLoad {
		return
	}
	if r.reader.readWhileOpening == nil {
		r.reader.readWhileOpening = make(map[domain.MessageID]struct{})
	}
	r.reader.readWhileOpening[messageID] = struct{}{}
}

// openedRead is what carrying out an open leaves to do once r.mu is released:
// read the conversation the load has just put on screen.
type openedRead struct {
	// batch is the conversation as the load brought it, less what was read
	// while the open waited (openReader.readWhileOpening). Taken under the
	// r.mu hold that spends the open, so a message landing after it is read
	// by its own arrival, not by the open as well.
	batch []DirectMessage
	// shown says the load put anything on screen at all. An open that showed
	// nothing has read nothing, and its badge goes back.
	shown bool
	// badgeBefore is the badge the conversation had at the open, which the
	// selection cleared: what goes back if the read fails.
	badgeBefore map[domain.MessageID]struct{}
}

// showOpenedConversationLocked carries out the open, once its load has brought
// the messages, and reports whether this load was the one doing it: the reader
// is shown the end, and the caller reads the conversation after releasing r.mu
// (readOpenedConversationInBackground). Spent once: a later reload of the same
// open conversation runs because something arrived or left, not because the
// reader asked to move. Called off by a report away from the end
// (cancelPendingOpenLocked).
//
// The divider is placed either way — it moves no one. Callers hold r.mu.
func (r *DMRouter) showOpenedConversationLocked() (openedRead, bool) {
	unreadAtOpen := r.reader.unreadAtOpen
	r.placeMarkerFromOpenLocked()
	if !r.reader.awaitingOpenLoad {
		return openedRead{}, false
	}
	r.reader.awaitingOpenLoad = false
	r.requestScrollToEndLocked()
	read := openedRead{shown: len(r.activeMessages) > 0, badgeBefore: unreadAtOpen}
	for _, msg := range r.activeMessages {
		if _, alreadyRead := r.reader.readWhileOpening[domain.MessageID(msg.ID)]; !alreadyRead {
			read.batch = append(read.batch, msg)
		}
	}
	r.reader.readWhileOpening = nil
	return read, true
}

// readOpenedConversationInBackground reads what the open put on screen, off
// the goroutine that ran the load: the receipt RPC can take seconds, and that
// goroutine may be the selection's (which publishes the conversation only
// after the load returns), an ebus subscriber's, or startup's. A router that
// is shutting down sends nothing, and gives the badge back.
func (r *DMRouter) readOpenedConversationInBackground(peer domain.PeerIdentity, read openedRead) {
	if !r.beginOp() {
		r.restorePeerUnread(peer, read.badgeBefore)
		return
	}
	go func() {
		defer r.endOp()
		defer recoverLog("readOpenedConversation")
		r.readOpenedConversation(peer, read)
	}()
}

// readOpenedConversation reads the conversation the open put on screen from
// its end: everything in it is on screen or above it.
func (r *DMRouter) readOpenedConversation(peer domain.PeerIdentity, read openedRead) {
	switch {
	case !read.shown:
		r.putBadgeBack(peer, read.badgeBefore)
	case len(read.batch) == 0:
		// Everything on screen was read as it landed.
	case !r.markBatchSeen(peer, read.batch):
		r.putBadgeBack(peer, read.badgeBefore)
	}
}

// putBadgeBack undoes the optimistic clear of a read of the whole
// conversation that did not happen — the open's, or a click's on the open
// conversation — by putting back what the badge held then, and has the
// database say what is still unread (repairBadgeFromStore): the snapshot can
// miss what the startup seed had not applied yet, and a receipt that failed
// halfway still marked some messages seen.
func (r *DMRouter) putBadgeBack(peer domain.PeerIdentity, badgeBefore map[domain.MessageID]struct{}) {
	r.restorePeerUnread(peer, badgeBefore)
	_ = r.repairBadgeFromStore(peer)
}

// placeMarkerFromOpenLocked puts the divider above the first message that was
// unread when the conversation was opened, once a load has brought it.
// Spent when it finds it — a seed of the one message an event carried does
// not hold it, and the load that follows does — or when a run that started
// since has placed the divider: that one is newer. Callers hold r.mu.
func (r *DMRouter) placeMarkerFromOpenLocked() {
	if len(r.reader.unreadAtOpen) == 0 {
		return
	}
	if r.reader.marker.placed {
		r.reader.unreadAtOpen = nil
		return
	}
	for _, msg := range r.activeMessages {
		if _, wasUnread := r.reader.unreadAtOpen[domain.MessageID(msg.ID)]; wasUnread {
			r.reader.marker = unreadMarkerAt(domain.MessageID(msg.ID))
			r.reader.unreadAtOpen = nil
			return
		}
	}
}

// refreshActiveMessagesLocked republishes the open conversation from the cache
// — the one place every path that changes it goes through.
//
// Only a cache that belongs to the open conversation is published. The cache
// outlives the selection: leaving a conversation keeps it warm, and a deletion
// or a send landing in it then refreshes it while another conversation is
// open — publishing it would put one conversation's messages under the
// other's header, and a report read off that screen would be checked against
// them.
//
// A conversation left with nothing in it has no list to scroll and no
// position for the UI to report, so its reader is put back at the end and its
// divider taken away: the next message lands on a screen showing nothing but
// it. Callers hold r.mu.
func (r *DMRouter) refreshActiveMessagesLocked() {
	if r.activePeer.IsZero() || !r.cache.MatchesPeer(r.activePeer) {
		return
	}
	r.activeMessages = r.cache.Messages()
	if len(r.activeMessages) > 0 {
		return
	}
	r.reader.atEnd = true
	r.reader.marker = UnreadMarker{}
}

// placeOwnSentLocked puts the user's own message, which the send RPC has just
// answered for, into its conversation's cache.
//
// It asks nothing of the screen. The answer comes when the RPC does, not when
// the user acted: the composer already showed the end the moment send was
// pressed, and a list at the end stays there as the message lands. A user who
// has since jumped to a quote or scrolled up did that later, and keeps it.
// And the cache is not the selection: leaving a conversation keeps its cache
// warm, so a send to it that lands while another one is open belongs in that
// cache and nowhere on screen (refreshActiveMessagesLocked). Callers hold r.mu.
func (r *DMRouter) placeOwnSentLocked(to domain.PeerIdentity, sent DirectMessage) {
	if !r.cache.MatchesPeer(to) {
		return
	}
	r.cache.AppendMessage(sent)
	r.refreshActiveMessagesLocked()
}

// requestScrollToEndLocked asks the UI for the end of the open conversation:
// the router has taken the reader there. Callers hold r.mu.
func (r *DMRouter) requestScrollToEndLocked() {
	r.pendingScrollToEnd = true
}

// seenReceiptTimeout bounds one batch of seen receipts.
const seenReceiptTimeout = 2 * time.Second

// sendSeenReceipts tells the node the reader has seen batch, in the
// background. The caller has already taken the batch off the badge — or never
// put it there, for a message read as it landed — so a failure puts it back:
// the database still calls those messages unread, and a badge is the only
// thing that says so on screen.
func (r *DMRouter) sendSeenReceipts(peer domain.PeerIdentity, batch []DirectMessage) {
	if len(batch) == 0 {
		return
	}
	if !r.beginOp() {
		r.restoreUnseen(peer, batch)
		return
	}
	go func() {
		defer r.endOp()
		defer recoverLog("sendSeenReceipts")
		r.confirmSeen(peer, batch)
	}()
}

// confirmSeen sends the receipts for batch and waits for the answer, putting
// the batch back on the badge if it fails. Runs off the UI goroutine.
func (r *DMRouter) confirmSeen(peer domain.PeerIdentity, batch []DirectMessage) {
	ctx, cancel := context.WithTimeout(r.opContext(), seenReceiptTimeout)
	err := r.markConversationSeen(ctx, peer, batch)
	cancel()
	if err == nil {
		return
	}
	log.Warn().Err(err).
		Str("peer", peer.String()).
		Int("messages", len(batch)).
		Str("first_message_id", batch[0].ID).
		Msg("dm_router: seen receipts for messages the reader saw failed, badge restored")
	r.restoreUnseen(peer, batch)
}

// restoreUnseen puts batch back on peer's badge.
func (r *DMRouter) restoreUnseen(peer domain.PeerIdentity, batch []DirectMessage) {
	ids := make(map[domain.MessageID]struct{}, len(batch))
	for _, id := range messageIDsOf(batch) {
		ids[id] = struct{}{}
	}
	r.restorePeerUnread(peer, ids)
}

// markConversationSeen is the receipt RPC, behind the test seam.
func (r *DMRouter) markConversationSeen(ctx context.Context, peer domain.PeerIdentity, batch []DirectMessage) error {
	if r.markConversationSeenFn != nil {
		return r.markConversationSeenFn(ctx, peer, batch)
	}
	return r.client.MarkConversationSeen(ctx, peer, batch)
}

// indexOfMessage is where messageID sits in messages, or -1.
func indexOfMessage(messages []DirectMessage, messageID domain.MessageID) int {
	if messageID == "" {
		return -1
	}
	for i := range messages {
		if domain.MessageID(messages[i].ID) == messageID {
			return i
		}
	}
	return -1
}

func messageIDsOf(messages []DirectMessage) []domain.MessageID {
	ids := make([]domain.MessageID, 0, len(messages))
	for i := range messages {
		ids = append(ids, domain.MessageID(messages[i].ID))
	}
	return ids
}
