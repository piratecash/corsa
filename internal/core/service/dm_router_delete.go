package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// message_delete / message_delete_ack — durable delete scheduler
// ---------------------------------------------------------------------------
//
// This file implements the application-level reliability layer for
// control DMs that ask the recipient to delete a previously delivered
// data DM. The wire-level plumbing lives in service/dm_crypto_control.go
// (DMCrypto.SendControlMessage / DecryptIncomingControlMessage) and
// node/service.go (send_control_message Frame.Type, dispatchNetworkFrame
// topic-aware divergence).
//
// Design contract: docs/dm-commands.md §"Scheduled deletion".
//
// The local copy is removed the moment the user asks, reachable peer or
// not: a message the user deleted must not stay on this disk waiting for
// somebody else to come online. What outlives the click is the INTENT —
// a row in message_delete_intents naming the peer and the message id, and
// nothing else — which the sweep below drives until the peer acknowledges
// it. The intent is the single source of truth for pending deletions:
// there is no parallel in-memory map to diverge from it, and a restart
// resumes exactly where it left off.

// Retry policy for the intent sweep. An attempt is one dispatch this node
// actually made — successful or not, since a send that failed is exactly
// what the backoff is for — and nothing else is ever charged: an intent
// whose peer cannot answer is parked without an attempt, so a contact
// offline for a week costs nothing and loses nothing. The TTL is the only
// way an intent dies unacknowledged.
const (
	deleteIntentRetryInitial = 30 * time.Second
	deleteIntentRetryCap     = time.Hour
	deleteRetryTickPeriod    = 5 * time.Second

	// deleteIntentHoldInterval is how long an intent is parked when its
	// peer cannot answer. It is a fairness device, not a backoff: the
	// sweep reads due intents oldest-first under a limit, so intents to
	// one absent contact left at the head of that queue would starve
	// every other peer's deletions indefinitely. Parking rotates the
	// head; the peer-connected kick pulls them back immediately, and this
	// interval is only the ceiling for a peer that becomes routable
	// without a fresh handshake.
	//
	// Five minutes rather than thirty seconds because the parked set no
	// longer drains on its own: a request to a contact who never returns
	// is kept indefinitely, so this interval sets a permanent floor of
	// writes on a device that has to sleep. The user never waits it out
	// — the peer-connected kick is what actually un-parks — so the only
	// thing a shorter interval buys is a faster reaction to a peer that
	// became routable without ever connecting to us.
	deleteIntentHoldInterval = 5 * time.Minute

	// deleteIntentPerPeerPerSweep bounds how many requests one peer
	// receives per sweep. A bulk deletion can leave hundreds of intents
	// due at once, and firing them at a single peer as fast as the sweep
	// can read them is what the receiver's control-DM rate limiter would
	// answer — turning a burst into rejected requests and burnt backoff.
	// The overflow is parked to the next tick, not charged.
	deleteIntentPerPeerPerSweep = 4

	// deleteIntentGiveUpAttempts bounds how many times a peer is ASKED
	// before the request is written off. Counted in attempts rather than
	// on the calendar because only an attempt is evidence of anything:
	// a month of silence from a contact who was never once reachable says
	// nothing about the request, while 720 unanswered dispatches say the
	// peer is not going to answer this one.
	//
	// The backoff caps at an hour, so the budget is roughly a month of a
	// peer that is reachable and keeps not answering — the same order as
	// the calendar month it replaces, but measured in the thing that
	// actually happened.
	deleteIntentGiveUpAttempts = 720

	// deleteIntentSweepLimit caps one sweep. The sweep dispatches control
	// DMs, so an unbounded batch would let a large backlog monopolise the
	// scheduler goroutine and the control path behind it.
	deleteIntentSweepLimit = 64
)

// deleteIntentBackoff is the wait before the attempt with the given
// one-based number, doubling from the initial interval up to the cap.
func deleteIntentBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	backoff := deleteIntentRetryInitial
	for i := 1; i < attempts; i++ {
		backoff *= 2
		if backoff >= deleteIntentRetryCap {
			return deleteIntentRetryCap
		}
	}
	return backoff
}

// ---------------------------------------------------------------------------
// Sender-side API
// ---------------------------------------------------------------------------

// SendMessageDelete is the canonical sender-side entry point for
// "delete this message". It removes the local copy and returns the route
// it took (domain.MessageDeleteRoute), so callers can tell a deletion
// that is finished from one the peer still owes us.
//
// The local row, its file-transfer state and its place in the UI are gone
// by the time this returns, in every route and regardless of peer
// reachability — and regardless of whether stopping the delivery
// succeeded. That is the point: a message the user asked to destroy does
// not stay on this disk because some other subsystem is unavailable.
//
// thawAfterClassification releases a single-message freeze that no
// withdrawal is going to release for us.
//
// On a DETACHED context, never the caller's. The likeliest way to reach
// here is an exit the caller's context caused — a lookup or a transaction
// that ended in Canceled or DeadlineExceeded — and passing that same
// context on would refuse the thaw at exactly the moment it is needed.
// The freeze has no TTL, so a refused thaw means the message is not sent
// again until the process restarts.
func (r *DMRouter) thawAfterClassification(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) {
	thawCtx, cancel := context.WithTimeout(r.detachedCtx(ctx), conversationCompensationBudget)
	defer cancel()
	if err := r.client.ThawConversationDelivery(thawCtx, peer, []domain.MessageID{target}); err != nil {
		log.Debug().Err(err).
			Str("target", string(target)).
			Msg("dm_router: SendMessageDelete: releasing the freeze bookkeeping failed")
	}
}

// The route is classified by domain.MessageDeleteContext and this
// function is the only place that executes it:
//
//   - local (incoming row) — nothing is asked of the author. Under the
//     default sender-delete policy they would reply denied, and their
//     outgoing record is not ours to touch. The terminal outcome is
//     published immediately.
//   - recalled (outgoing, unconfirmed, and the node proved the envelope
//     never reached the wire) — nobody has ever seen it, so nothing is
//     scheduled: asking a peer to delete a message they never received
//     would tell them one existed. Terminal outcome published here too.
//   - withdraw (outgoing, unconfirmed, emission not ruled out) — the
//     delivery is cancelled and the peer-side deletion scheduled anyway,
//     because a copy may have escaped before the cancellation landed;
//     the peer answers not_found if it did not.
//   - scheduled (outgoing row the recipient confirmed) — nothing left to
//     cancel, so the row goes and the peer-side deletion is scheduled.
//
// The row and its intent are removed and recorded in ONE transaction
// (chatlog.DeleteWithIntent): a crash between them would destroy the
// user's copy while leaving nobody to ask the peer, which is the exact
// state the intent table exists to make impossible.
//
// Scheduling means a durable intent plus, when the peer is reachable
// right now, an immediate dispatch. An unreachable peer costs nothing:
// the sweep in deleteRetryLoop picks the intent up when they are back,
// across restarts, and drops it on their ack.
//
// For a found row the conversation peer is DERIVED from the row and the
// caller-supplied peer is ignored as untrustworthy: a buggy or malicious
// caller could otherwise dispatch the deletion into the wrong
// conversation. The !found case is the recovery path — the row is
// already absent (deleted earlier, expired by TTL), so there is nothing
// local to remove; the caller-supplied peer is trusted and the peer-side
// deletion is scheduled so the two sides still converge.
//
// Authorization: an immutable target row triggers an early error and
// no state mutation. Other flags only affect the peer-side decision;
// the local side is permissive (the user can always remove their own
// view of a message).
func (r *DMRouter) SendMessageDelete(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) (domain.MessageDeleteRoute, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.client == nil {
		return "", fmt.Errorf("DMRouter has no client")
	}
	peer = normalizePeer(peer)
	if peer.IsZero() {
		return "", fmt.Errorf("peer is required")
	}
	if !target.IsValid() {
		return "", fmt.Errorf("target message id %q is not a valid UUID v4", target)
	}

	store := r.client.chatlog.Store()
	if store == nil {
		return "", fmt.Errorf("chatlog store is not available")
	}

	// Stop the node from sending this message BEFORE the row is read. The
	// classification below rests on the row's durable never-emitted mark,
	// and that mark only means something while nothing can emit behind
	// the reader's back: an emission landing between the read and the
	// cancellation clears the mark, reports "already emitted", and the
	// stale read would still route the delete as `recalled` — deleting a
	// message the peer now holds without ever asking them for it.
	//
	// The freeze has NO TTL, so every exit from here on has to end it —
	// including the early ones. Left standing over an immutable message,
	// or over one whose row could not be read because the database
	// blinked, it would stop that message being sent for the life of the
	// process with nothing able to release it.
	//
	// The one exit that must NOT thaw is a withdrawal that failed after
	// the row was already deleted: those messages are not the user's any
	// more. That path clears the flag and hands the id to the withdrawal
	// backlog instead.
	frozenNeverEmitted, freezeErr := r.client.FreezeMessageDelivery(ctx, target)
	if freezeErr != nil {
		log.Warn().Err(freezeErr).
			Str("target", string(target)).
			Msg("dm_router: SendMessageDelete: could not stop the delivery; the row's proof will not be trusted")
	}
	thawOnExit := true
	defer func() {
		if thawOnExit {
			r.thawAfterClassification(ctx, peer, target)
		}
	}()

	entry, found, err := store.EntryByID(ctx, target)
	if err != nil {
		return "", fmt.Errorf("lookup target %s: %w", target, err)
	}

	myAddr := r.client.Address()

	// The recovery path has no row to classify: nothing local to remove
	// and nothing of ours left to cancel, so it is a plain schedule. The
	// flag went with the row, so whether the peer will honour it is
	// unknowable here — their ack answers that, and a denied one retires
	// the intent.
	route := domain.MessageDeleteRouteScheduled

	if found {
		flag := protocol.MessageFlag(entry.Flag)
		if flag == protocol.MessageFlagImmutable {
			return "", fmt.Errorf("target %s is immutable and cannot be deleted", target)
		}

		// Derive the actual conversation peer from the row, NOT from
		// the caller. The caller-supplied peer is only trusted on
		// !found (recovery), where there is no row to derive from.
		// A caller that passes a wrong peer for a found row would
		// otherwise leak the deletion to the wrong conversation.
		entrySender := domain.PeerIdentityFromWire(entry.Sender)
		entryRecipient := domain.PeerIdentityFromWire(entry.Recipient)
		isIncoming := entrySender != myAddr
		var derivedPeer domain.PeerIdentity
		if isIncoming {
			derivedPeer = entrySender
		} else {
			derivedPeer = entryRecipient
		}
		if derivedPeer != peer {
			log.Warn().
				Str("target", string(target)).
				Str("caller_peer", peer.String()).
				Str("derived_peer", derivedPeer.String()).
				Msg("dm_router: SendMessageDelete: caller peer did not match the row; using derived peer")
		}
		peer = derivedPeer

		classification := domain.MessageDeleteContext{
			Outgoing:        !isIncoming,
			ConfirmedByPeer: protocol.ReceiptStatusConfirmsDelivery(entry.DeliveryStatus),
			// Two witnesses, both taken under the freeze above. The row
			// outlives the node's memory — a retry that ran out of
			// attempts is dropped from the delivery domain, so the
			// cancellation reports "no entry", indistinguishable from
			// "already emitted" — and the node covers a message withheld
			// so recently that its mark had not landed. Neither is
			// trusted without the freeze: without it the row's answer can
			// be stale by the time it is read.
			NeverEmitted: freezeErr == nil &&
				(frozenNeverEmitted || chatlog.NeverEmitted(entry.Metadata)),
		}
		route = classification.Route()

		// The withdrawal itself comes AFTER the local deletion, below.
		// It is irreversible, and running it first means a transaction
		// that then fails leaves the row on screen in "sending" with
		// every delivery hook already destroyed — nothing sends it and
		// nothing recalls it. The freeze is what makes the order free:
		// the message cannot go out while the transaction runs, so
		// deleting first costs nothing and keeps the failure recoverable.

	} else {
		log.Debug().
			Str("target", string(target)).
			Str("peer", peer.String()).
			Msg("dm_router: SendMessageDelete: target already absent locally; scheduling the peer-side deletion only")
	}

	// The envelope may still exist in some relay's in-flight buffer, and
	// an incoming message can be re-delivered by a peer that never got
	// our receipt, so the id has to be refused from here on — otherwise
	// such an echo resurfaces as a bubble the user already deleted.
	//
	// Volatile mark only. The durable half rides the deletion's own
	// transaction below, so the row and its refusal land together and a
	// rollback leaves neither. Writing it here instead would put an
	// hour's refusal of a LIVE message on disk whenever that transaction
	// fails: metadata about a message that still exists, and a trap that
	// swallows its next legitimate re-delivery.
	r.wipeTombstones.Mark([]domain.MessageID{target}, time.Now().UTC())

	now := time.Now().UTC()
	intent := chatlog.DeleteIntent{
		MessageID: target,
		Peer:      peer,
		CreatedAt: now,
		// Due immediately: the dispatch below handles a reachable peer,
		// and for an unreachable one the sweep must be free to pick the
		// intent up as soon as that changes.
		NextAttemptAt: now,
	}

	if found {
		if err := r.removeLocalMessage(ctx, store, peer, target, route, intent); err != nil {
			// Nothing was committed, so the row is still here and the
			// mark above names a live message. Drop it: a refusal for a
			// row the user can still see is both a record of it and a
			// trap for its next legitimate re-delivery.
			r.wipeTombstones.Forget(ctx, []domain.MessageID{target})
			return route, err
		}
		if route.CancelsDelivery() {
			// The row is gone, so the node is holding the payload of a
			// message that no longer exists here, kept off the wire only
			// by the freeze. A failure does NOT thaw — this message is
			// not the user's any more — and is not dropped either: it is
			// owed, and the delete sweep retries it until it succeeds.
			if err := r.withdrawDeletedDeliveries(ctx, peer, []domain.MessageID{target}); err != nil {
				thawOnExit = false
			}
		}
	} else {
		// The recovery path: no row to delete, so no transaction for the
		// refusal to ride. It is written on its own — the id is exactly
		// the one a late echo would re-insert.
		r.wipeTombstones.Note(ctx, []domain.MessageID{target}, time.Now().UTC())
		if route.SchedulesPeerDeletion() {
			if err := store.NoteDeleteIntent(ctx, intent); err != nil {
				return route, fmt.Errorf("schedule peer-side delete of %s: %w", target, err)
			}
		}
	}

	if !route.SchedulesPeerDeletion() {
		// Nobody is owed anything: the deletion is final right here.
		r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
			Target:    target,
			Peer:      peer,
			Status:    domain.MessageDeleteStatusDeleted,
			Abandoned: false,
			Attempts:  0,
			Route:     route,
		})
		return route, nil
	}

	r.refreshPendingDeleteCounts()

	if !r.peerReachable(peer) {
		log.Info().
			Str("target", string(target)).
			Str("peer", peer.String()).
			Str("route", string(route)).
			Msg("dm_router: message deleted locally; peer-side deletion scheduled until the peer is reachable")
		return route, nil
	}

	r.dispatchScheduledDelete(ctx, store, intent, now)
	log.Info().
		Str("target", string(target)).
		Str("peer", peer.String()).
		Str("route", string(route)).
		Msg("dm_router: message deleted locally; message_delete dispatched, awaiting peer ack")
	return route, nil
}

// dispatchScheduledDelete sends one message_delete for the intent and
// charges the attempt. Shared by the immediate send in
// SendMessageDelete and the sweep, so both count attempts the same way:
// an attempt is one dispatch this node actually made, successful or not.
// A failed send is exactly the case the backoff exists for.
func (r *DMRouter) dispatchScheduledDelete(ctx context.Context, store *chatlog.Store, intent chatlog.DeleteIntent, now time.Time) {
	if err := r.dispatchMessageDelete(ctx, intent.Peer, intent.MessageID); err != nil {
		log.Debug().Err(err).
			Str("target", string(intent.MessageID)).
			Str("peer", intent.Peer.String()).
			Int("attempt", intent.Attempts+1).
			Msg("dm_router: message_delete send failed; charging the attempt and backing off")
	}
	attempts := intent.Attempts + 1
	if err := store.RecordDeleteIntentAttempt(ctx, intent.MessageID, now.Add(deleteIntentBackoff(attempts))); err != nil {
		log.Warn().Err(err).
			Str("target", string(intent.MessageID)).
			Msg("dm_router: charging a delete-intent attempt failed; the sweep may re-send early")
	}
}

// peerReachable reports whether the conversation peer has at least one
// usable next-hop right now. It decides only WHEN a scheduled deletion
// is dispatched — never whether the local copy goes.
//
// Unknown answers TRUE. "No reachability source" is not evidence of an
// unreachable peer, and the cost of guessing wrong in this direction is
// one control DM that the transport drops; guessing the other way would
// hold a request back for no reason.
func (r *DMRouter) peerReachable(peer domain.PeerIdentity) bool {
	if r.peerReachableFn != nil {
		return r.peerReachableFn(peer)
	}
	if r.statusMonitor == nil {
		return true
	}
	reachable := r.statusMonitor.ReachableIDsSnapshot()
	if reachable == nil {
		return true
	}
	return reachable[peer]
}

// removeLocalMessage destroys the local copy: the chatlog row and every
// per-message trace under its id, any backing file-transfer state, and
// its place in the live UI. The refusal of the id — and, when the route
// owes the peer a deletion, the intent — is written in the SAME
// transaction as the row. They are one invariant seen from three sides,
// and a crash between separate commits leaves either a destroyed message
// nobody will ever ask the peer about, or one whose next replay is
// welcomed straight back in.
//
// It deliberately publishes no outcome: that belongs to whoever knows
// the request is finished. For a local or recalled route that is the
// caller, immediately; for the scheduled ones it is the ack handler or
// the intent's expiry, possibly days later.
func (r *DMRouter) removeLocalMessage(ctx context.Context, store *chatlog.Store, peer domain.PeerIdentity, target domain.MessageID, route domain.MessageDeleteRoute, intent chatlog.DeleteIntent) error {
	// The volatile mark is already in place (SendMessageDelete plants it
	// before deciding anything); Mark is idempotent and hands back the
	// expiry the durable half commits with, inside the same transaction
	// as the row.
	expiry := r.wipeTombstones.Mark([]domain.MessageID{target}, time.Now().UTC())

	if route.SchedulesPeerDeletion() {
		if _, err := store.DeleteWithIntent(ctx, intent, expiry); err != nil {
			return fmt.Errorf("delete %s and schedule the peer-side removal: %w", target, err)
		}
	} else if _, err := store.DeleteMessageWithTombstone(ctx, target, expiry); err != nil {
		return fmt.Errorf("delete chatlog entry %s: %w", target, err)
	}

	if r.fileBridge != nil {
		r.fileBridge.OnMessageDeleted(target)
	}
	r.evictDeletedMessageFromUI(peer, target)
	r.checkpointAfterDelete(ctx, store)

	log.Info().
		Str("target", string(target)).
		Str("peer", peer.String()).
		Str("route", string(route)).
		Msg("dm_router: local copy of the message removed")
	return nil
}

// checkpointAfterDelete retires the write-ahead log so the pages that
// held the removed message stop existing in the file, not just in the
// database's logical view. secure_delete zeroes the freed page, but in
// WAL mode that zeroing is itself a log frame — the original bytes sit
// in the -wal until a checkpoint folds it back.
//
// Best-effort: a busy checkpoint is not a failed deletion, and the
// automatic one still comes.
func (r *DMRouter) checkpointAfterDelete(ctx context.Context, store *chatlog.Store) {
	if err := store.CheckpointWAL(ctx); err != nil {
		log.Debug().Err(err).Msg("dm_router: wal checkpoint after delete did not complete; the automatic one will retire the pages")
	}
}

// checkpointSoonAfterDelete asks for a checkpoint without taking one per
// row.
//
// A thread wipe reaches the receiver as N separate message_delete
// commands, so a checkpoint per deletion is N truncations of the
// write-ahead log — every one of them waiting on readers and rewriting
// the file — for hours on a long thread, on a device that has to sleep.
// The sender's side of the same wipe pays exactly one, after its single
// transaction.
//
// Coalescing keeps the guarantee that matters: the pages holding a
// deleted message leave the log promptly, rather than whenever the
// automatic checkpoint happens to run. It is best-effort either way, so
// "promptly" is allowed to mean "within a second of the last deletion in
// a burst" instead of "before this one returns".
func (r *DMRouter) checkpointSoonAfterDelete() {
	if r.deleteCheckpoint == nil {
		return
	}
	r.deleteCheckpoint.request()
}

// evictDeletedMessageFromUI removes a deleted message from the live
// chat cache, refreshes the conversation preview, and notifies the UI.
// Called after both local-side (SendMessageDelete) and remote-side
// (applyInboundDelete on the recipient) chatlog deletions so the
// deleted bubble disappears immediately, without waiting for a manual
// conversation reload.
//
// peer is the conversation counterparty (the "other" party of the
// thread the deleted message belonged to). For an outgoing message
// this is the original recipient; for an incoming message it is the
// original sender.
func (r *DMRouter) evictDeletedMessageFromUI(peer domain.PeerIdentity, target domain.MessageID) {
	if r.cache == nil {
		return
	}

	r.mu.Lock()
	cacheRemoved := false
	if r.cache.MatchesPeer(peer) {
		cacheRemoved = r.cache.RemoveMessage(string(target))
		if cacheRemoved {
			r.activeMessages = r.cache.Messages()
		}
	}
	// seenMessageIDs is the dedup gate for inbound new-message events;
	// drop the entry so a future re-delivery of the same ID (e.g. peer
	// resends the message after we re-add a contact) is not silently
	// ignored.
	delete(r.seenMessageIDs, string(target))
	r.mu.Unlock()

	// Refresh the sidebar preview from chatlog. Done outside the lock
	// because FetchSinglePreview hits SQLite. We use a delete-aware
	// path here (instead of the stock updatePreviewFromStore): on the
	// "no rows left" case (FetchSinglePreview returns nil), the
	// peer's preview MUST be cleared explicitly. updatePreviewFromStore
	// deliberately leaves the preview untouched on nil to avoid
	// erasing a sidebar entry whose unread badge was repaired from
	// DMHeaders before the chatlog row landed — that contract is for
	// the new-message path, not for delete. Reusing it here would
	// keep the just-deleted message visible in the sidebar/file-tab
	// preview row even though the bubble has been removed from the
	// active conversation.
	r.refreshPreviewAfterDelete(peer)

	if cacheRemoved {
		r.notify(UIEventMessagesUpdated)
	}
	r.notify(UIEventSidebarUpdated)
}

// refreshPreviewAfterDelete is the delete-aware counterpart of
// updatePreviewFromStore. The two differ in how they treat the
// "no rows in chatlog for this peer" case AND in their treatment of
// the per-peer unread badge:
//
//   - updatePreviewFromStore (new-message / receipt path) leaves the
//     preview untouched, because a peer entry can be created by
//     repairUnreadFromHeaders before the corresponding chatlog row is
//     persisted; clearing it would erase the unread badge.
//   - refreshPreviewAfterDelete (delete path) explicitly clears the
//     preview to a zero ConversationPreview when chatlog is empty,
//     because the only way a peer's chatlog becomes empty during
//     delete is that the user just removed the last row. Leaving the
//     stale preview behind keeps the deleted message visible in the
//     sidebar even after the active-conversation bubble disappears.
//
// Unread badge: deleting an unread incoming message decrements the
// per-peer unread count in SQLite (the row is gone — no longer
// counted). The in-memory RouterPeerState.Unread is event-driven via
// Unread++ / Unread=0 transitions and never spontaneously
// recalculates from SQL outside of seedPreviews. Without an explicit
// refresh here the sidebar badge stays at the pre-delete value, even
// down to "5 unread" after the only 5 unread messages have been
// deleted. We pull the authoritative count from chatlog and overwrite.
//
// The peer entry itself stays in the sidebar — only the preview body
// goes blank and the unread badge resets to whatever chatlog reports.
// Removing the peer outright is a separate user action
// (DeletePeerHistory).
func (r *DMRouter) refreshPreviewAfterDelete(peer domain.PeerIdentity) {
	if r.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	preview, err := r.client.FetchSinglePreview(ctx, peer)
	cancel()
	if err != nil {
		// Transient chatlog error — fall back to leaving the preview
		// as-is rather than wiping it on a flaky read. The next
		// successful sidebar refresh will pick up the correct state.
		return
	}

	// Fetch the authoritative unread count for this peer from chatlog.
	// Done outside the router lock because it is SQL I/O. UnreadCountFor
	// is a no-op when chatlog is unavailable, so an unset count means
	// "leave Unread untouched".
	var (
		unreadCount    int
		unreadResolved bool
	)
	if r.client.chatlog != nil {
		if store := r.client.chatlog.Store(); store != nil {
			n, err := store.UnreadCountFor(r.opContext(), peer)
			if err == nil {
				unreadCount = n
				unreadResolved = true
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensurePeerLocked(peer)
	if preview != nil {
		r.peers[peer].Preview = *preview
	} else {
		// nil preview during delete = "no chatlog rows left for this
		// peer". Clear preview explicitly + force unread to 0; an
		// empty conversation cannot have unread messages.
		r.peers[peer].Preview = ConversationPreview{PeerAddress: peer}
		r.peers[peer].Unread = 0
		return
	}
	if unreadResolved {
		r.peers[peer].Unread = unreadCount
	}
}

// dispatchMessageDelete encodes the MessageDeletePayload and submits it
// through DMCrypto.SendControlMessage. Used by both the initial send in
// SendMessageDelete and the retry loop for subsequent attempts.
//
// Tests that need to count dispatches or avoid the rpc/identity stack
// can install r.dispatchControlDeleteFn before exercising the public
// entry points; when set, this method delegates to that function and
// skips payload encoding + SendControlMessage entirely. Production
// code leaves r.dispatchControlDeleteFn nil and runs the real path.
func (r *DMRouter) dispatchMessageDelete(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) error {
	if r.dispatchControlDeleteFn != nil {
		return r.dispatchControlDeleteFn(ctx, peer, target)
	}
	payload, err := domain.MarshalMessageDeletePayload(domain.MessageDeletePayload{TargetID: target})
	if err != nil {
		return fmt.Errorf("marshal message_delete payload: %w", err)
	}
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandMessageDelete, payload); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Inbound handlers
// ---------------------------------------------------------------------------

// onControlMessage is the receive-side dispatcher for inbound control DM
// events. It decrypts the envelope through DMCrypto and routes by inner
// DMCommand. Replaces the slice-A stub that only logged the command.
func (r *DMRouter) onControlMessage(event protocol.LocalChangeEvent) {
	if event.Topic != protocol.TopicControlDM {
		return
	}
	if r.client == nil {
		return
	}

	cmd, payload, sender, ok := r.client.DecryptIncomingControlMessage(event)
	if !ok {
		log.Debug().
			Str("message_id", event.MessageID).
			Str("envelope_sender", event.Sender).
			Msg("dm_router: control DM decrypt failed or non-control inner command")
		return
	}

	switch cmd {
	case domain.DMCommandDecryptFailed:
		r.handleInboundDecryptFailed(sender.String(), payload)
	case domain.DMCommandMessageDelete:
		r.handleInboundMessageDelete(sender, payload)
	case domain.DMCommandMessageDeleteAck:
		r.handleInboundMessageDeleteAck(sender, payload)
	default:
		log.Debug().
			Str("command", string(cmd)).
			Str("sender", sender.String()).
			Msg("dm_router: control DM with unknown inner command")
	}
}

// handleInboundMessageDelete processes a remote request to delete a
// previously delivered DM. Authorization is keyed on the target
// message's MessageFlag — see docs/dm-commands.md §"Authorization":
//
//   - Immutable          → reject (status: immutable)
//   - SenderDelete (default) → require envelopeSender == M.Sender
//   - AnyDelete          → allow envelopeSender ∈ {M.Sender, M.Recipient}
//   - AutoDeleteTTL      → same as SenderDelete; the TTL itself is enforced
//     independently by chatlog's expiry sweeper
//
// Idempotency: a duplicate request after the row has already been
// deleted produces the same MessageDeleteStatusNotFound ack as the
// first; the sender clears the pending entry on either deleted or
// not_found.
func (r *DMRouter) handleInboundMessageDelete(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var payload domain.MessageDeletePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", envelopeSender.String()).
			Msg("dm_router: message_delete payload malformed; dropping")
		return
	}
	if !payload.Valid() {
		log.Debug().
			Str("envelope_sender", envelopeSender.String()).
			Str("target_id", string(payload.TargetID)).
			Msg("dm_router: message_delete payload invalid; dropping")
		return
	}

	status := r.applyInboundDelete(envelopeSender, payload.TargetID)
	r.replyMessageDeleteAck(envelopeSender, payload.TargetID, status)
}

// applyInboundDelete is the authorization core. Pure decision logic on
// the chatlog state — no I/O outside of the Store reads/writes. Returns
// the terminal status to ack back to the requester.
func (r *DMRouter) applyInboundDelete(envelopeSender domain.PeerIdentity, target domain.MessageID) domain.MessageDeleteStatus {
	store := r.client.chatlog.Store()
	if store == nil {
		// We cannot even look: answering not_found here would tell the
		// sender their message is gone from this side while it may be
		// sitting in a chatlog that is merely unreachable right now.
		// `error` keeps their intent alive so the next sweep asks again.
		log.Warn().
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: chatlog store unavailable")
		return domain.MessageDeleteStatusError
	}

	entry, found, err := store.EntryByID(r.opContext(), target)
	if err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: lookup failed")
		return domain.MessageDeleteStatusError
	}
	if !found {
		// Idempotent success — but not necessarily "already deleted".
		// A delete can overtake the message it is about: the DM may
		// still be sitting in a relay's buffer and land here minutes
		// later. Answering not_found without refusing the id would let
		// that copy settle in permanently, while the sender treats
		// not_found as success and retires the request, leaving nobody
		// to ask again.
		//
		// Refusing it costs nothing when the row really was deleted
		// earlier: the tombstone expires on its own hour.
		if err := store.NoteWipeTombstones(r.opContext(), []domain.MessageID{target},
			r.wipeTombstones.Mark([]domain.MessageID{target}, time.Now().UTC())); err != nil {
			// not_found is terminal — it retires the sender's request —
			// and it is only true if the message stays away. Without a
			// durable refusal a restart forgets it, the in-flight copy
			// lands, and nobody is left to ask. `error` costs the sender
			// one retry instead.
			log.Warn().Err(err).
				Str("target", string(target)).
				Msg("dm_router: applyInboundDelete: refusing an unseen target did not persist; answering error so the sender asks again")
			return domain.MessageDeleteStatusError
		}
		return domain.MessageDeleteStatusNotFound
	}

	flag := protocol.MessageFlag(entry.Flag)
	if flag == protocol.MessageFlagImmutable {
		log.Warn().
			Str("target", string(target)).
			Str("envelope_sender", envelopeSender.String()).
			Msg("dm_router: applyInboundDelete: target is immutable")
		return domain.MessageDeleteStatusImmutable
	}

	if !authorizedToDelete(flag, envelopeSender, domain.PeerIdentityFromWire(entry.Sender), domain.PeerIdentityFromWire(entry.Recipient)) {
		log.Warn().
			Str("target", string(target)).
			Str("envelope_sender", envelopeSender.String()).
			Str("target_sender", entry.Sender).
			Str("flag", string(flag)).
			Msg("dm_router: applyInboundDelete: envelope sender not authorized for this flag")
		return domain.MessageDeleteStatusDenied
	}

	if _, err := store.DeleteMessageWithTombstone(r.opContext(), target,
		r.wipeTombstones.Mark([]domain.MessageID{target}, time.Now().UTC())); err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: chatlog DeleteByID failed")
		// The row is authorized for deletion and is still here. Saying
		// not_found would retire the sender's intent over a database
		// fault and strand the message on this side forever; `error`
		// asks them to come back.
		return domain.MessageDeleteStatusError
	}

	if r.fileBridge != nil {
		r.fileBridge.OnMessageDeleted(target)
	}
	// The peer asked us to destroy this message — the deletion the
	// protocol exists to deliver, and it gets the same on-disk treatment
	// as our own. Coalesced rather than immediate: a thread wipe arrives
	// as N of these commands, and a truncation per row would run for
	// hours on a long thread.
	r.checkpointSoonAfterDelete()

	// Drop the deleted bubble from the live conversation cache and
	// refresh the sidebar preview. The conversation peer (relative to
	// us) is the *other* party of the original message — for an
	// inbound message we are receiving from the sender, so the
	// thread peer is entry.Sender; for outbound it would be
	// entry.Recipient. Compute it generically via myAddr to be
	// resilient to either direction (in practice on this side the
	// deleted message is incoming, so peer == entry.Sender).
	myAddr := r.client.Address()
	threadPeer := domain.PeerIdentityFromWire(entry.Sender)
	if threadPeer == myAddr {
		threadPeer = domain.PeerIdentityFromWire(entry.Recipient)
	}
	r.evictDeletedMessageFromUI(threadPeer, target)

	log.Info().
		Str("target", string(target)).
		Str("envelope_sender", envelopeSender.String()).
		Msg("dm_router: applied inbound message_delete")

	return domain.MessageDeleteStatusDeleted
}

// authorizedToDelete is the pure-function authorization predicate. The
// envelope sender (cryptographically verified upstream in
// storeIncomingMessage) is compared against the target message's
// participants under the rules of the target's flag.
//
// The AUTHOR decides who may reach into the other side's copy, and says
// so in the flag they stamped: `sender-delete` (the default every DM
// carries) keeps that to themselves, `any-delete` extends it to the
// recipient, `immutable` denies it to everyone.
//
// Deleting a message somebody sent US is therefore a LOCAL act: our own
// view goes, their copy is not ours to touch. The bulk wipe is the one
// place with stronger authority, and it earns it differently — an
// explicit two-click "delete this chat for everyone" is mutual consent
// to forget the thread, not a standing right over the peer's history.
//
// Empty / unknown flag is treated as `sender-delete` to match the
// documented default policy (docs/dm-commands.md §"Authorization").
func authorizedToDelete(flag protocol.MessageFlag, envelopeSender, targetSender, targetRecipient domain.PeerIdentity) bool {
	switch flag {
	case protocol.MessageFlagImmutable:
		return false
	case protocol.MessageFlagAnyDelete:
		return envelopeSender == targetSender || envelopeSender == targetRecipient
	default:
		// sender-delete, auto-delete-ttl, empty and anything a future
		// version invents: only the author.
		return envelopeSender == targetSender
	}
}

// replyMessageDeleteAck encodes a MessageDeleteAckPayload and ships it
// back over the control wire. Best-effort — if the ack send itself
// fails we log and move on; the requester will retry message_delete
// and we will reply again idempotently (every status the recipient
// reports is reproducible).
func (r *DMRouter) replyMessageDeleteAck(peer domain.PeerIdentity, target domain.MessageID, status domain.MessageDeleteStatus) {
	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   status,
	})
	if err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Msg("dm_router: marshal message_delete_ack failed")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandMessageDeleteAck, payload); err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Str("peer", peer.String()).
			Msg("dm_router: send message_delete_ack failed; requester will retry")
	}
}

// handleInboundMessageDeleteAck settles one scheduled deletion against
// the peer's answer.
//
// The local row is long gone (SendMessageDelete removed it at click
// time), so the ack decides only the fate of the intent:
//
//   - deleted / not_found — the peer is consistent with us. Drop the
//     intent; the deletion is finished on both sides.
//   - denied / immutable — the peer refuses and re-asking will not
//     change that. Drop the intent too, and publish the status so the
//     user learns their copy is gone but the peer's is not.
//
// A defensive DeleteByID runs on success: a late relay echo can
// re-insert the row after the wipe tombstone has expired, and the ack is
// the last moment we are guaranteed to be looking at this id.
func (r *DMRouter) handleInboundMessageDeleteAck(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var ack domain.MessageDeleteAckPayload
	if err := json.Unmarshal([]byte(payloadJSON), &ack); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", envelopeSender.String()).
			Msg("dm_router: message_delete_ack payload malformed; dropping")
		return
	}
	if !ack.Valid() {
		log.Debug().
			Str("envelope_sender", envelopeSender.String()).
			Str("target_id", string(ack.TargetID)).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack payload invalid; dropping")
		return
	}

	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("target", string(ack.TargetID)).
			Msg("dm_router: message_delete_ack: chatlog store unavailable; intent left scheduled")
		return
	}
	ctx := r.opContext()

	intent, found, err := store.DeleteIntentByID(ctx, ack.TargetID)
	if err != nil {
		log.Warn().Err(err).
			Str("target", string(ack.TargetID)).
			Msg("dm_router: message_delete_ack: intent lookup failed; the sweep will re-issue")
		return
	}
	if !found {
		log.Debug().
			Str("target", string(ack.TargetID)).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack for an unknown intent; dropping")
		return
	}
	// Cross-check: the ack must come from the peer we addressed. A
	// peer cannot ack on behalf of someone else, and the intent stays
	// scheduled so the real peer's ack can still settle it.
	if intent.Peer != envelopeSender {
		log.Warn().
			Str("target", string(ack.TargetID)).
			Str("expected_peer", intent.Peer.String()).
			Str("actual_envelope_sender", envelopeSender.String()).
			Msg("dm_router: message_delete_ack from unexpected peer; intent kept")
		return
	}

	if ack.Status.IsTransient() {
		// The peer could not decide. Keep the intent and say nothing to
		// the UI — the deletion is still outstanding.
		//
		// The schedule is deliberately left alone. The dispatch that
		// provoked this answer already charged its attempt and set the
		// next due time; charging again here would count one exchange
		// twice, pushing the backoff up a step per round-trip and
		// burning the give-up budget at double rate. An ack is the
		// answer to an attempt, not another attempt.
		log.Info().
			Str("target", string(ack.TargetID)).
			Str("peer", envelopeSender.String()).
			Int("attempts", intent.Attempts).
			Msg("dm_router: message_delete_ack: peer reported a transient failure; intent kept")
		return
	}

	if ack.Status.IsTerminalSuccess() {
		if _, err := store.DeleteByID(ctx, ack.TargetID); err != nil {
			log.Warn().Err(err).
				Str("target", string(ack.TargetID)).
				Msg("dm_router: message_delete_ack: defensive DeleteByID failed")
		}
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(ack.TargetID)
		}
		r.evictDeletedMessageFromUI(envelopeSender, ack.TargetID)
	}

	if _, err := store.DropDeleteIntent(ctx, ack.TargetID); err != nil {
		// The peer has answered; leaving the row means the sweep
		// re-asks, which the peer answers idempotently. Log and move
		// on rather than withholding the outcome from the user.
		log.Warn().Err(err).
			Str("target", string(ack.TargetID)).
			Msg("dm_router: message_delete_ack: dropping the intent failed; the sweep may re-ask")
	}

	r.refreshPendingDeleteCounts()

	log.Info().
		Str("target", string(ack.TargetID)).
		Str("peer", envelopeSender.String()).
		Str("status", string(ack.Status)).
		Int("attempts", intent.Attempts).
		Msg("dm_router: message_delete completed")

	r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
		Target:    ack.TargetID,
		Peer:      envelopeSender,
		Status:    ack.Status,
		Abandoned: false,
		Attempts:  intent.Attempts,
	})
}

// publishMessageDeleteOutcome forwards the terminal outcome onto the
// ebus so UI / RPC subscribers can differentiate the four statuses
// instead of treating the synchronous SendMessageDelete return as a
// completion signal. Safe when the bus is nil — the publish step is
// skipped silently.
func (r *DMRouter) publishMessageDeleteOutcome(outcome ebus.MessageDeleteOutcome) {
	if r.eventBus == nil {
		return
	}
	r.eventBus.Publish(ebus.TopicMessageDeleteCompleted, outcome)
}

// ---------------------------------------------------------------------------
// Delete scheduler
// ---------------------------------------------------------------------------

// deleteRetryLoop runs in a dedicated goroutine launched from Start().
// Every tick it sweeps the durable delete intents: whatever the peer
// still owes us, whether it was scheduled a second ago or by a process
// that has since been restarted.
//
// The loop terminates when ctx is cancelled; with context.Background()
// it runs for the process lifetime.
func (r *DMRouter) deleteRetryLoop(ctx context.Context) {
	defer recoverLog("deleteRetryLoop")
	ticker := time.NewTicker(deleteRetryTickPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.processDeleteRetryDue(ctx, now.UTC())
			// Same tick, same reason: something a deletion set in motion
			// is still outstanding. A withdrawal the node refused earlier
			// keeps the payload of a deleted message in this process.
			r.retryOwedWithdrawals(ctx)
		}
	}
}

// processDeleteRetryDue is one sweep, factored out for testability.
//
// Four outcomes per due intent:
//
//   - past its TTL — the peer has had a month and never answered. Drop
//     it and publish Abandoned, so the UI can tell the user their copy
//     is gone but the peer's may not be.
//   - peer unreachable — parked for deleteIntentHoldInterval, charging
//     NOTHING. No attempt, no backoff. The park is what keeps the sweep
//     fair; the peer-connected kick undoes it the moment they are back.
//   - peer already served its quota this sweep — parked to the next
//     tick, also uncharged, so one peer's backlog cannot be fired at it
//     faster than its rate limiter accepts.
//   - otherwise — dispatched, then the attempt is charged and the next
//     due time set by deleteIntentBackoff.
func (r *DMRouter) processDeleteRetryDue(ctx context.Context, now time.Time) {
	if r.client == nil || r.client.chatlog == nil {
		return
	}
	store := r.client.chatlog.Store()
	if store == nil {
		return
	}

	due, err := store.DueDeleteIntents(ctx, now, deleteIntentSweepLimit)
	if err != nil {
		log.Warn().Err(err).Msg("dm_router: delete intent sweep failed; retrying on the next tick")
		return
	}

	var (
		dispatchedPerPeer = make(map[domain.PeerIdentity]int, len(due))
		absent            []domain.MessageID
		throttled         []domain.MessageID
	)
	for _, intent := range due {
		if r.expireDeleteIntent(ctx, store, intent, now) {
			continue
		}
		if !r.peerReachable(intent.Peer) {
			absent = append(absent, intent.MessageID)
			continue
		}
		if dispatchedPerPeer[intent.Peer] >= deleteIntentPerPeerPerSweep {
			throttled = append(throttled, intent.MessageID)
			continue
		}

		dispatchedPerPeer[intent.Peer]++
		r.dispatchScheduledDelete(ctx, store, intent, now)
	}

	// Parked in two writes, not one per row. A request to a contact who
	// never comes back is kept indefinitely, so the parked set is not a
	// backlog that drains — it is a floor the sweep pays every tick,
	// forever. One UPDATE per row would make that floor proportional to
	// how many messages the user ever deleted while a peer was away, on
	// a device that has to sleep.
	r.holdDeleteIntents(ctx, store, absent, now.Add(deleteIntentHoldInterval))
	r.holdDeleteIntents(ctx, store, throttled, now.Add(deleteRetryTickPeriod))

	// The wipe barrier is latched synchronously and released by the
	// goroutine that runs the wipe; if that goroutine never gets there —
	// a panic between Begin and the launch, a scheduling stall past the
	// TTL — the latch would pin the conversation shut for good. Swept
	// here rather than from a loop of its own: it is the same subsystem
	// on a fitting cadence, and a wipe is now N of these intents anyway.
	r.reapStaleWipeReservations(now)
}

// reapStaleWipeReservations drops wipe reservations whose owner never
// came back to finish them, and tells the UI about each one so the user
// is not left looking at a conversation they cannot write to.
func (r *DMRouter) reapStaleWipeReservations(now time.Time) {
	if r.convDeleteRetry == nil {
		return
	}
	for _, stranded := range r.convDeleteRetry.pruneStaleReservations(now, convDeleteReservationTTL) {
		log.Warn().
			Str("peer", stranded.peer.String()).
			Str("request_id", string(stranded.requestID)).
			Msg("dm_router: wipe reservation stranded past its TTL; releasing the barrier")
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:               stranded.peer,
			LocalCleanupFailed: true,
		})
	}
}

// holdDeleteIntents parks a batch without charging any of it, and says so
// in the log at debug: a held intent looks like an idle one from the
// outside, and the reason it is idle is the interesting part.
func (r *DMRouter) holdDeleteIntents(ctx context.Context, store *chatlog.Store, ids []domain.MessageID, until time.Time) {
	if len(ids) == 0 {
		return
	}
	if err := store.HoldDeleteIntents(ctx, ids, until); err != nil {
		log.Warn().Err(err).
			Int("intents", len(ids)).
			Msg("dm_router: parking delete intents failed; they may hold the head of the sweep queue")
		return
	}
	log.Debug().
		Int("intents", len(ids)).
		Time("until", until).
		Msg("dm_router: delete intents parked; no attempt charged")
}

// refreshPendingDeleteCounts re-reads how many deletions each peer still
// owes and publishes the numbers into the peer states the UI renders.
//
// Read outside the router lock (it is SQL) and applied inside it, like
// every other chatlog-derived counter here. Called after any change to
// the intent table: a count that only refreshed on restart would tell
// the user their deletion is still pending long after it completed.
//
// Peers with no entry in the map are skipped rather than created: an
// intent can outlive its conversation, and inventing a sidebar row for a
// contact the user has removed would undo that removal.
func (r *DMRouter) refreshPendingDeleteCounts() {
	if r.client == nil || r.client.chatlog == nil {
		return
	}
	store := r.client.chatlog.Store()
	if store == nil {
		return
	}
	ctx := r.opContext()
	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("dm_router: reading pending delete counts failed; the badge keeps its previous value")
		return
	}
	changed := false
	r.mu.Lock()
	for peer, state := range r.peers {
		if state.PendingDeletes != counts[peer] {
			state.PendingDeletes = counts[peer]
			changed = true
		}
	}
	r.mu.Unlock()

	if changed {
		r.notify(UIEventSidebarUpdated)
	}
}

// reviveDeleteIntentsForPeer un-parks everything this peer owes us when
// they come back — single deletions and a whole-thread wipe alike — so a
// request that has been waiting for days goes out on the next tick
// instead of at the end of the parking interval. Called from the
// peer-connected subscription.
func (r *DMRouter) reviveDeleteIntentsForPeer(peer domain.PeerIdentity) {
	if r.client == nil || r.client.chatlog == nil {
		return
	}
	store := r.client.chatlog.Store()
	if store == nil {
		return
	}
	ctx := r.opContext()
	now := time.Now().UTC()

	revived, err := store.ReviveDeleteIntentsForPeer(ctx, peer, now)
	if err != nil {
		log.Warn().Err(err).Str("peer", peer.String()).Msg("dm_router: reviving delete intents failed; they wake on their own schedule")
	}
	if revived > 0 {
		log.Info().
			Str("peer", peer.String()).
			Int64("message_intents", revived).
			Msg("dm_router: peer is back; pending deletions re-armed")
	}
}

// expireDeleteIntent writes off an intent the peer has refused to settle
// and reports whether it did.
//
// The budget is spent in ATTEMPTS, not in days. A calendar deadline
// measures the wrong thing: it runs while the peer is unreachable, which
// is exactly the stretch the durable intent exists to survive, so it
// gives up on the case the feature was built for and reports "abandoned"
// about a peer nobody managed to ask. Attempts only accrue when the peer
// was there to be asked, so the budget is spent only on being ignored.
//
// The price is that a request to a contact who never returns is kept
// indefinitely: one row per deletion addressed to an absent identity,
// dropped with the rest of their history when the identity is removed
// (chatlog.DeleteByPeer).
func (r *DMRouter) expireDeleteIntent(ctx context.Context, store *chatlog.Store, intent chatlog.DeleteIntent, now time.Time) bool {
	if intent.Attempts < deleteIntentGiveUpAttempts {
		return false
	}
	if _, err := store.DropDeleteIntent(ctx, intent.MessageID); err != nil {
		log.Warn().Err(err).
			Str("target", string(intent.MessageID)).
			Msg("dm_router: dropping an expired delete intent failed; will retry on the next sweep")
		return true
	}

	log.Warn().
		Str("target", string(intent.MessageID)).
		Str("peer", intent.Peer.String()).
		Int("attempts", intent.Attempts).
		Time("created_at", intent.CreatedAt).
		Msg("dm_router: delete intent unanswered after the full attempt budget; giving up on the peer-side deletion")

	r.refreshPendingDeleteCounts()
	r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
		Target:    intent.MessageID,
		Peer:      intent.Peer,
		Status:    "",
		Abandoned: true,
		Attempts:  intent.Attempts,
	})
	return true
}
