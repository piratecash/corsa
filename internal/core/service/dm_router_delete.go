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
			Str("target", logID(string(target))).
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
			Str("target", logID(string(target))).
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
				Str("target", logID(string(target))).
				Str("caller_peer", logID(peer.String())).
				Str("derived_peer", logID(derivedPeer.String())).
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
			Str("target", logID(string(target))).
			Str("peer", logID(peer.String())).
			Msg("dm_router: SendMessageDelete: target already absent locally; scheduling the peer-side deletion only")
	}

	// The envelope may still exist in some relay's in-flight buffer, and
	// an incoming message can be re-delivered by a peer that never got
	// our receipt, so the id has to be refused from here on — otherwise
	// such an echo resurfaces as a bubble the user already deleted.
	//
	// In memory, before anything is decided, and covering the window until the
	// transaction below commits. What carries the refusal across a restart is
	// the REQUEST that transaction writes — while the peer still owes us their
	// copy, the id is one this node is openly asking about, so refusing it
	// records nothing extra. See wipe_tombstone_set.go.
	r.wipeTombstones.Note([]domain.MessageID{target}, time.Now().UTC())

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
			// refusal above names a live message. Lift it: a refusal for a
			// row the user can still see is a trap for its next legitimate
			// re-delivery.
			r.wipeTombstones.Forget([]domain.MessageID{target})
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
		// The recovery path: no row to delete. The refusal planted above
		// stands on its own — the id is exactly the one a late echo would
		// insert for the first time.
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
		deletionLog().Info().
			Str("target", logID(string(target))).
			Str("peer", logID(peer.String())).
			Str("route", string(route)).
			Msg("dm_router: message deleted locally; peer-side deletion scheduled until the peer is reachable")
		return route, nil
	}

	r.dispatchScheduledDelete(ctx, store, intent, now)
	deletionLog().Info().
		Str("target", logID(string(target))).
		Str("peer", logID(peer.String())).
		Str("route", string(route)).
		Msg("dm_router: message deleted locally; message_delete dispatched, awaiting peer ack")
	return route, nil
}

// dispatchScheduledDelete sends one request for the intent and charges the
// attempt. Shared by the immediate send in SendMessageDelete and the sweep, so
// both count attempts the same way: an attempt is one dispatch this node
// actually made, successful or not. A failed send is exactly the case the
// backoff exists for.
//
// The two kinds differ only in what goes on the wire and in what the charge is
// written against — one is keyed by message id, the other by peer.
func (r *DMRouter) dispatchScheduledDelete(ctx context.Context, store *chatlog.Store, intent chatlog.DeleteIntent, now time.Time) {
	attempts := intent.Attempts + 1
	nextAttemptAt := now.Add(deleteIntentBackoff(attempts))

	if intent.Kind == chatlog.DeleteIntentConversation {
		if err := r.dispatchConversationDelete(ctx, intent.Peer, intent.RequestID); err != nil {
			log.Debug().Err(err).
				Str("peer", logID(intent.Peer.String())).
				Str("request_id", logID(string(intent.RequestID))).
				Int("attempt", attempts).
				Msg("dm_router: conversation_delete send failed; charging the attempt and backing off")
		}
		if err := store.RecordConversationDeleteAttempt(ctx, intent.Peer, intent.RequestID, nextAttemptAt); err != nil {
			log.Warn().Err(err).
				Str("peer", logID(intent.Peer.String())).
				Msg("dm_router: charging a conversation-delete attempt failed; the sweep may re-send early")
		}
		return
	}

	if err := r.dispatchMessageDelete(ctx, intent.Peer, intent.MessageID); err != nil {
		log.Debug().Err(err).
			Str("target", logID(string(intent.MessageID))).
			Str("peer", logID(intent.Peer.String())).
			Int("attempt", attempts).
			Msg("dm_router: message_delete send failed; charging the attempt and backing off")
	}
	if err := store.RecordDeleteIntentAttempt(ctx, intent.MessageID, nextAttemptAt); err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(intent.MessageID))).
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
// its place in the live UI. When the route owes the peer a deletion, the
// request is written in the SAME transaction as the row: a crash between
// separate commits would leave a destroyed message nobody will ever ask
// the peer about.
//
// It deliberately publishes no outcome: that belongs to whoever knows
// the request is finished. For a local or recalled route that is the
// caller, immediately; for the scheduled ones it is the ack handler or
// the intent's expiry, possibly days later.
func (r *DMRouter) removeLocalMessage(ctx context.Context, store *chatlog.Store, peer domain.PeerIdentity, target domain.MessageID, route domain.MessageDeleteRoute, intent chatlog.DeleteIntent) error {
	// The reaction queue is bracketed around the delete ITSELF, and around
	// nothing else: it names reactions rather than messages, so it cannot see
	// this coming, and a frame built from the record a moment ago would
	// otherwise go out about a message that is no longer here.
	//
	// Released the moment the row is gone, before the file cleanup, the UI
	// eviction and the checkpoint. Those can take a while — a synchronous
	// TRUNCATE checkpoint waits for readers up to busy_timeout — and every
	// moment the gate is up costs the whole conversation: a pass that meets it
	// puts its ENTIRE batch back for dmControlRetryDelay, including reactions on
	// other messages and the answers this node owes the peer. The defer is a
	// safety net for the error returns above; the release is idempotent.
	resumeReactions := r.client.HoldReactionSends(peer)
	defer resumeReactions()

	// removed says whether a row actually went. A re-issued delete finds
	// nothing to remove, and moving the version for it would mark every
	// load and decrypt in flight as stale for nothing.
	var removed bool
	if route.SchedulesPeerDeletion() {
		var err error
		if removed, err = store.DeleteWithIntent(ctx, intent); err != nil {
			return fmt.Errorf("delete %s and schedule the peer-side removal: %w", target, err)
		}
	} else {
		var err error
		if removed, err = store.DeleteByID(ctx, target); err != nil {
			return fmt.Errorf("delete chatlog entry %s: %w", target, err)
		}
	}

	// The row is gone: everything below is cleanup, and none of it can put a
	// reaction on the wire, so the gate comes down here rather than at return.
	resumeReactions()

	// Under the file barrier, which moves the version first: a registration
	// in flight either finishes before this begins or finds the deletion
	// and stands down.
	r.withFileOps(peer, removed, func() {
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(target)
		}
	})
	if !removed {
		// The row was already gone — a re-issued request, a race with the
		// peer's own delete. There is nothing to evict from the UI and
		// nothing to recompute; doing it anyway would move the version for
		// a deletion that did not happen and mark every read in flight as
		// stale.
		log.Debug().
			Str("target", logID(string(target))).
			Str("peer", logID(peer.String())).
			Msg("dm_router: local copy of the message was already gone")
		return nil
	}

	r.evictDeletedMessageFromUI(peer, target)
	r.checkpointAfterDelete(ctx, store)

	deletionLog().Info().
		Str("target", logID(string(target))).
		Str("peer", logID(peer.String())).
		Str("route", string(route)).
		Msg("dm_router: local copy of the message removed")
	return nil
}

// deleteCheckpointWait is the deadline this path puts on the truncation.
//
// It is an UPPER bound and not a promise: `wal_checkpoint(TRUNCATE)` waits for
// the readers holding the log inside SQLite, up to the connection's
// busy_timeout (five seconds), and the driver in use does not interrupt a
// running pragma when the context expires. So a deletion that meets a live
// reader can hold this path for that timeout, once, and then answer.
//
// That is accepted rather than tuned around. The readers here are single
// queries — a conversation load, a preview refresh — so the obstacle clears in
// milliseconds in every ordinary case, and the alternative to waiting is
// telling somebody a message is gone while its bytes are still in a file.
const deleteCheckpointWait = 500 * time.Millisecond

// checkpointAfterDelete retires the write-ahead log so the pages that held the
// removed message stop existing in the file, not just in the database's logical
// view. secure_delete zeroes the freed page, but in WAL mode that zeroing is
// itself a log frame — the original bytes sit in the -wal until a checkpoint
// folds it back.
//
// It REPORTS whether that happened, and the callers that tell somebody a
// deletion is finished — the ack this node sends, the outcome it publishes —
// wait for a true answer. That is the whole point of the contract: "deleted"
// said over a log that still holds the message is a promise the file does not
// keep, and after it is said the request is retired and nothing looks at that
// id again.
//
// A failure still hands the work to the retrying checkpointer, so the pages do
// leave; what the caller loses is the right to call it done yet.
func (r *DMRouter) checkpointAfterDelete(ctx context.Context, store *chatlog.Store) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, deleteCheckpointWait)
	defer cancel()

	if err := store.CheckpointWAL(attemptCtx); err != nil {
		log.Debug().Err(err).
			Msg("dm_router: the write-ahead log still holds a deletion; not reporting it finished yet")
		r.checkpointSoonAfterDelete()
		return false
	}
	return true
}

// checkpointSoonAfterDelete asks for a checkpoint without taking one per
// row.
//
// A burst of separate message_delete commands — a peer working through a
// backlog of single deletions — would otherwise mean one truncation of the
// write-ahead log per row, every one of them waiting on readers and
// rewriting the file, on a device that has to sleep. A thread wipe is not
// among them any more: it arrives as ONE command and pays exactly one
// truncation, like the sender's side of it.
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
	r.forgetMessageLocked(string(target))
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

	// The badge is a set of ids, and this id is gone. The history move was
	// already recorded by the file barrier when the row was removed — one
	// deletion is one move, and a second bump would make every read that
	// started in between look stale for nothing.
	r.mu.Lock()
	r.dropUnreadLocked(peer, target)
	r.mu.Unlock()

	r.refreshPreviewAfterDelete(peer)

	// The reactions of that message went with it, and the chips are drawn from a
	// per-conversation cache that ONLY this event reloads. Without it the facts
	// of a deleted message stay in the window's memory until the user leaves the
	// chat — and if the same id is delivered again after its wipe tombstone
	// expires, the new bubble is drawn with chips no row backs any more.
	r.publishReactionsChanged(peer)

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
// Unread badge: deleting an unread incoming message removes its row, and
// with it the only record of its delivery status. The event stream carries
// no status, so nothing else can ever take an id OUT of the badge set —
// which is why this path re-derives the set from the database instead of
// subtracting from it. Without that the sidebar would keep reading "5
// unread" after the only five unread messages were deleted.
//
// The peer entry itself stays in the sidebar — only the preview body
// goes blank and the unread badge resets to whatever chatlog reports.
// Removing the peer outright is a separate user action
// (DeletePeerHistory).
func (r *DMRouter) refreshPreviewAfterDelete(peer domain.PeerIdentity) {
	r.refreshPreviewAfterDeleteCtx(r.opContext(), peer)
}

// refreshPreviewAfterDeleteCtx is the same work on a caller-supplied context,
// so the retry sweep can bound itself by the loop it runs in rather than by
// the router's process-lifetime context.
func (r *DMRouter) refreshPreviewAfterDeleteCtx(ctx context.Context, peer domain.PeerIdentity) {
	// afterDelete=true: an empty conversation here means the user removed the
	// last row, so the preview is cleared and the badge forced to zero. The
	// ordering rules — one reconciliation per peer at a time, and a revision
	// that rejects an answer the event path has already overtaken — live in
	// reconcilePeerFromStore, because every SQL recomputation needs them and
	// three copies of them would be three chances to get them wrong.
	// TryLock, never Lock: this runs on the ebus subscriber goroutine that
	// carries control DMs, and that goroutine has a 64-slot inbox which
	// DROPS its overflow. Waiting behind another reconciliation's two query
	// timeouts would cost incoming control messages, so a contended peer is
	// handed to the sweep instead.
	switch r.reconcilePeerFromStore(ctx, peer, true, false) {
	case reconcileApplied, reconcilePeerGone, reconcileNoHistory:
		r.clearPendingDeleteReconcile(peer)
	case reconcileRetry, reconcileBusy:
		// The one recomputation nobody repeats. A new message reconciles its
		// own peer, and the startup scan runs once; a deletion whose
		// reconciliation failed has no second chance, so the sidebar would go
		// on quoting the message the user destroyed. The sweep below owes it
		// one.
		r.queueDeleteReconcile(peer)
	}
}

// deleteReconcileSweepBudget is the wall-clock share of one tick the owed
// reconciliations may take. The tick period is five seconds and the
// scheduler behind it has its own deadlines to keep.
const deleteReconcileSweepBudget = 2 * time.Second

// deleteReconcileSweepLimit bounds how many owed reconciliations one tick
// takes on. They share a goroutine with the delete-intent scheduler, and a
// long queue of peers whose chatlog is slow would otherwise hold up the
// deletions themselves.
const deleteReconcileSweepLimit = 8

// deleteReconcileRetries bounds the retries of one deletion's reconciliation.
// The sweep runs every few seconds, so this is a minute of a chatlog that
// cannot answer — long past the transient failures this exists for, and short
// of retrying a broken database until the process ends.
const deleteReconcileRetries = 12

// queueDeleteReconcile records that a peer still owes a post-deletion
// reconciliation.
func (r *DMRouter) queueDeleteReconcile(peer domain.PeerIdentity) {
	if peer.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingDeleteReconcile == nil {
		r.pendingDeleteReconcile = make(map[domain.PeerIdentity]int)
	}
	if _, queued := r.pendingDeleteReconcile[peer]; !queued {
		r.pendingDeleteReconcile[peer] = deleteReconcileRetries
	}
}

// clearPendingDeleteReconcile forgets an owed reconciliation.
func (r *DMRouter) clearPendingDeleteReconcile(peer domain.PeerIdentity) {
	r.mu.Lock()
	delete(r.pendingDeleteReconcile, peer)
	r.mu.Unlock()
}

// retryPendingDeleteReconcile is one sweep of the owed reconciliations. A peer
// that has run out of attempts is dropped with a warning: the sidebar is
// wrong for that conversation until something else touches it, and saying so
// beats a queue that retries a broken database forever.
func (r *DMRouter) retryPendingDeleteReconcile(ctx context.Context) {
	r.mu.RLock()
	owed := make([]domain.PeerIdentity, 0, len(r.pendingDeleteReconcile))
	for peer := range r.pendingDeleteReconcile {
		owed = append(owed, peer)
		if len(owed) >= deleteReconcileSweepLimit {
			break
		}
	}
	r.mu.RUnlock()

	// A budget in TIME, not only in peers: one reconciliation can wait out
	// two query timeouts, so eight of them would hold the shared scheduler
	// goroutine for half a minute while delete intents and withdrawals wait
	// behind it. Whatever does not fit stays queued for the next tick.
	sweepCtx, cancelSweep := context.WithTimeout(ctx, deleteReconcileSweepBudget)
	defer cancelSweep()

	for _, peer := range owed {
		if sweepCtx.Err() != nil {
			return
		}
		// TryLock, not Lock: the sweep shares its goroutine with the
		// delete-intent scheduler, and a peer whose reconciliation is
		// already running would hold it past any budget.
		outcome := r.reconcilePeerFromStore(sweepCtx, peer, true, false)
		if outcome == reconcileRetry && sweepCtx.Err() != nil {
			// The tick's own budget expired under it, not the database. The
			// peer stays queued with its attempts intact: charging it here
			// would write off a healthy conversation after twelve busy
			// ticks.
			continue
		}
		switch outcome {
		case reconcileApplied:
			// Publish it. The first attempt was published by its caller,
			// this one has no caller waiting: Snapshot() serves a cache only
			// notify rebuilds, so a retry that lands silently leaves the
			// deleted message quoted on screen — the exact outcome the queue
			// exists to prevent.
			r.clearPendingDeleteReconcile(peer)
			r.notify(UIEventSidebarUpdated)
			continue
		case reconcilePeerGone, reconcileNoHistory:
			r.clearPendingDeleteReconcile(peer)
			continue
		case reconcileBusy:
			// Contention is not failure: nothing was read, so nothing went
			// wrong. Spending an attempt here would write off a busy
			// conversation after twelve ticks of perfectly healthy work.
			continue
		}
		r.mu.Lock()
		left := r.pendingDeleteReconcile[peer] - 1
		if left <= 0 {
			delete(r.pendingDeleteReconcile, peer)
		} else {
			r.pendingDeleteReconcile[peer] = left
		}
		r.mu.Unlock()
		if left <= 0 {
			log.Warn().Str("peer", logID(peer.String())).Msg("dm_router: giving up on the post-deletion sidebar refresh; the row keeps the deleted message until something else touches this conversation")
		}
	}
}

// optionalTimeOrUnset keeps the zero time out of an OptionalTime: "no
// incoming message survived" has to read as absent, not as an observation
// made in year 1.
func optionalTimeOrUnset(at time.Time) domain.OptionalTime {
	if at.IsZero() {
		return domain.OptionalTime{}
	}
	return domain.TimeOf(at)
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
			Str("message_id", logID(event.MessageID)).
			Str("envelope_sender", logID(event.Sender)).
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
	case domain.DMCommandConversationDelete:
		r.handleInboundConversationDelete(sender, payload)
	case domain.DMCommandConversationDeleteAck:
		r.handleInboundConversationDeleteAck(sender, payload)
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
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: message_delete payload malformed; dropping")
		return
	}
	if !payload.Valid() {
		log.Debug().
			Str("envelope_sender", logID(envelopeSender.String())).
			Str("target_id", logID(string(payload.TargetID))).
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
			Str("target", logID(string(target))).
			Msg("dm_router: applyInboundDelete: chatlog store unavailable")
		return domain.MessageDeleteStatusError
	}

	entry, found, err := store.EntryByID(r.opContext(), target)
	if err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(target))).
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
		// earlier: the refusal expires on its own.
		//
		// In memory only, and this is the one place that leaves a real
		// window: `not_found` is terminal, so the sender retires the
		// request, and a copy that lands after THIS process restarts has
		// nothing left to turn it away. Writing the id down instead would
		// mean keeping a list of messages this node was asked to delete and
		// never had — a record of the peer's deletions, on our disk, past
		// the moment either of us needed it.
		r.wipeTombstones.Note([]domain.MessageID{target}, r.now())

		// `not_found` is TERMINAL — the sender retires the request on it — so
		// it may not be said over a log that still holds the message. This is
		// the path a busy first attempt comes back to: the row went, the
		// truncation was refused, we answered `error`, and the retry arrives
		// here with nothing left to delete. Without this the second attempt
		// would close the request while the bytes were still in the sidecar,
		// which is exactly the hole the first attempt refused to leave.
		//
		// It also costs nothing in the ordinary case: the log is clean, the
		// checkpoint is a no-op, and the answer goes out.
		if !r.checkpointAfterDelete(r.opContext(), store) {
			return domain.MessageDeleteStatusError
		}
		return domain.MessageDeleteStatusNotFound
	}

	flag := protocol.MessageFlag(entry.Flag)
	if flag == protocol.MessageFlagImmutable {
		log.Warn().
			Str("target", logID(string(target))).
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: applyInboundDelete: target is immutable")
		return domain.MessageDeleteStatusImmutable
	}

	if !authorizedToDelete(flag, envelopeSender, domain.PeerIdentityFromWire(entry.Sender), domain.PeerIdentityFromWire(entry.Recipient)) {
		log.Warn().
			Str("target", logID(string(target))).
			Str("envelope_sender", logID(envelopeSender.String())).
			Str("target_sender", logID(entry.Sender)).
			Str("flag", string(flag)).
			Msg("dm_router: applyInboundDelete: envelope sender not authorized for this flag")
		return domain.MessageDeleteStatusDenied
	}

	// Bracketed like the local delete: a reaction frame built a moment ago must
	// not reach the peer about a message this node is erasing right now.
	r.wipeTombstones.Note([]domain.MessageID{target}, time.Now().UTC())
	resumeReactions := r.client.HoldReactionSends(envelopeSender)
	inboundRemoved, err := store.DeleteByID(r.opContext(), target)
	resumeReactions()
	if err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(target))).
			Msg("dm_router: applyInboundDelete: chatlog DeleteByID failed")
		// The row is authorized for deletion and is still here. Saying
		// not_found would retire the sender's intent over a database
		// fault and strand the message on this side forever; `error`
		// asks them to come back.
		return domain.MessageDeleteStatusError
	}

	r.withFileOps(envelopeSender, inboundRemoved, func() {
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(target)
		}
	})
	// The peer asked us to destroy this message — the deletion the protocol
	// exists to deliver — and it gets the same on-disk treatment as our own,
	// BEFORE we answer them.
	//
	// Order matters here, and so does the ANSWER. `deleted` is what makes the
	// requester retire their request: after it, nothing anywhere will look at
	// this message again. Saying it while the pages that held it are still
	// legible in the -wal file would make that the final state.
	//
	// So a log that will not truncate is answered `error` instead. The row is
	// already gone from the database — this costs the requester one more
	// round-trip of an idempotent request, and the next attempt finds nothing
	// to delete and a log it can retire.
	truncated := r.checkpointAfterDelete(r.opContext(), store)

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

	if !truncated {
		// The row is gone from the database and the screen, but the pages that
		// held it are still in the log. `deleted` is terminal for the
		// requester, so answering it here would make "still readable in a
		// sidecar" the final state. `error` costs one more round-trip of an
		// idempotent request; the next attempt finds nothing to delete and a
		// log it can retire.
		return domain.MessageDeleteStatusError
	}

	deletionLog().Info().
		Str("target", logID(string(target))).
		Str("envelope_sender", logID(envelopeSender.String())).
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
			Str("target", logID(string(target))).
			Msg("dm_router: marshal message_delete_ack failed")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandMessageDeleteAck, payload); err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(target))).
			Str("peer", logID(peer.String())).
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
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: message_delete_ack payload malformed; dropping")
		return
	}
	if !ack.Valid() {
		log.Debug().
			Str("envelope_sender", logID(envelopeSender.String())).
			Str("target_id", logID(string(ack.TargetID))).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack payload invalid; dropping")
		return
	}

	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("target", logID(string(ack.TargetID))).
			Msg("dm_router: message_delete_ack: chatlog store unavailable; intent left scheduled")
		return
	}
	ctx := r.opContext()

	intent, found, err := store.DeleteIntentByID(ctx, ack.TargetID)
	if err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(ack.TargetID))).
			Msg("dm_router: message_delete_ack: intent lookup failed; the sweep will re-issue")
		return
	}
	if !found {
		log.Debug().
			Str("target", logID(string(ack.TargetID))).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack for an unknown intent; dropping")
		return
	}
	// Cross-check: the ack must come from the peer we addressed. A
	// peer cannot ack on behalf of someone else, and the intent stays
	// scheduled so the real peer's ack can still settle it.
	if intent.Peer != envelopeSender {
		log.Warn().
			Str("target", logID(string(ack.TargetID))).
			Str("expected_peer", logID(intent.Peer.String())).
			Str("actual_envelope_sender", logID(envelopeSender.String())).
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
			Str("target", logID(string(ack.TargetID))).
			Str("peer", logID(envelopeSender.String())).
			Int("attempts", intent.Attempts).
			Msg("dm_router: message_delete_ack: peer reported a transient failure; intent kept")
		return
	}

	if ack.Status.IsTerminalSuccess() {
		// Whether this defensive delete actually removed anything decides
		// the version move: an ack usually names a row deleted when the
		// intent was written, and moving the counter for it would mark
		// every current read stale for nothing.
		// Bracketed like every other per-message delete. This one is for a row
		// that came BACK after its tombstone expired, which means the user may
		// well have seen it and reacted to it — so the window this closes is not
		// theoretical here, it is the case the path exists for.
		resumeReactions := r.client.HoldReactionSends(envelopeSender)
		removed, err := store.DeleteByID(ctx, ack.TargetID)
		resumeReactions()
		if err != nil {
			// The local copy may still be here, so NOTHING below may run: the
			// request stays, the UI is not told the deletion finished, and the
			// sweep asks again. Reporting success on a failed delete is how a
			// message the user destroyed comes back after a restart with
			// nobody left to remove it — the peer has answered, so if the
			// request went too, no path would ever look at this id again.
			log.Warn().Err(err).
				Str("target", logID(string(ack.TargetID))).
				Msg("dm_router: message_delete_ack: the local copy could not be removed; keeping the request so the sweep retries")
			return
		}
		if removed {
			// A row that came BACK after its refusal expired and is being
			// removed again: its pages are in the log like any other
			// deletion's, and nothing else on this path would retire them.
			r.checkpointSoonAfterDelete()
		}
		r.withFileOps(envelopeSender, removed, func() {
			if r.fileBridge != nil {
				r.fileBridge.OnMessageDeleted(ack.TargetID)
			}
		})
		r.evictDeletedMessageFromUI(envelopeSender, ack.TargetID)
	}

	if r.beforeDropDeleteIntentForTest != nil {
		r.beforeDropDeleteIntentForTest()
	}
	settled, carried, err := store.DropDeleteIntentUnlessCarried(ctx, ack.TargetID)
	if carried {
		// The answer arrived for a request a wipe has since taken over. The
		// request was already on the wire when the user cleared the chat, so
		// the peer answers it on its own terms — and `denied` here would be
		// "the peer would not delete it" about a conversation the user has been
		// told is gone. The wipe asks for this message too and is answered in
		// its own right; nothing is published, and the row stays until that
		// answer comes.
		deletionLog().Debug().
			Str("target", logID(string(ack.TargetID))).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack for a request the wipe carries; the wipe answers for it")
		r.refreshPendingDeleteCounts()
		return
	}
	if err != nil {
		// The request is still on disk, so the sweep will ask again and the
		// "waiting for the peer" indicator is still true. Publishing a settled
		// outcome here would put the UI in two states at once: "the messages
		// are gone" and a pending marker that keeps re-dispatching.
		log.Warn().Err(err).
			Str("target", logID(string(ack.TargetID))).
			Msg("dm_router: message_delete_ack: dropping the request failed; it stays scheduled and the outcome is not published")
		r.refreshPendingDeleteCounts()
		return
	}
	if !settled {
		// The row went while this answer was in flight — the wipe that carried
		// it was answered first, or an earlier copy of this same ack settled
		// it. Either way the request is retired, and an answer to a retired
		// request is dropped: publishing here would report a refusal for a
		// deletion nothing is waiting for any more.
		deletionLog().Debug().
			Str("target", logID(string(ack.TargetID))).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack for a request that was already retired; dropping")
		r.refreshPendingDeleteCounts()
		return
	}
	// The truncation is attempted here, and its FAILURE does not stop the
	// outcome from being published.
	//
	// The distinction is exact. An answer that will be re-asked — the ack this
	// node sends a peer — can be withheld until the log is clean, because the
	// peer asks again and nothing is lost. A report to our OWN user cannot: the request has just been
	// retired, so no sweep will come back, a repeat of the peer's ack is
	// dropped as unknown, and withholding it means the pending indicator
	// disappears while "the messages are deleted" is never said. The
	// information would be gone for good.
	//
	// The physical erasure is still guaranteed — by the retrying
	// checkpointer, which is what checkpointAfterDelete hands the work to
	// — just not by this line.
	//
	// The whole split, and what it costs, is in docs/dm-commands.md §"Why
	// an outcome is reported before the log is truncated".
	r.checkpointAfterDelete(ctx, store)
	// The refusal of this id STAYS. An ack says the peer removed the row from their
	// database; it says nothing about the copies of the envelope that may
	// still be sitting in a relay's buffer or an inbox queue, and those are
	// exactly what the refusal exists to turn away. Dropping it on the ack
	// dropping it on the ack re-opens the window inside the same process — no
	// restart needed.
	//
	// It expires on its own, at the sender's reseed horizon
	// (wipeTombstoneTTL), which is the moment a replay stops being
	// possible. Memory is not the disk: the contract this design keeps is
	// that nothing is WRITTEN DOWN, and a bounded in-memory set that
	// expires by itself is not a record of anything.

	r.refreshPendingDeleteCounts()

	deletionLog().Info().
		Str("target", logID(string(ack.TargetID))).
		Str("peer", logID(envelopeSender.String())).
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
			r.runRetrySweep(ctx, now.UTC())
		}
	}
}

// runRetrySweep is one tick, factored out for the same reason
// processDeleteRetryDue is: the loop around it is a ticker and a select, and
// everything worth testing is in here.
//
// The four sweeps share a tick because they share a cause — work this process
// started and could not finish — and sharing one goroutine is what keeps them
// from competing for the database. Each carries its own budget.
func (r *DMRouter) runRetrySweep(ctx context.Context, now time.Time) {
	r.processDeleteRetryDue(ctx, now)
	// Same tick, same cause: a deletion whose sidebar refresh did not land is
	// still showing the message it removed.
	r.retryPendingDeleteReconcile(ctx)
	// And the rows that never got a place in the arrival order: a read that
	// failed has nothing else to bring it back, since the header pass runs
	// once per process and a re-delivery of the message is a duplicate the
	// node does not publish.
	r.retryPendingPreviewRepair(ctx)
	// Same tick, same reason: something a deletion set in motion is still
	// outstanding. A withdrawal the node refused earlier keeps the payload of
	// a deleted message in this process.
	r.retryOwedWithdrawals(ctx)
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
		absent            parkedIntents
		throttled         parkedIntents
	)
	for _, intent := range due {
		if r.expireDeleteIntent(ctx, store, intent, now) {
			continue
		}
		if !r.peerReachable(intent.Peer) {
			absent.add(intent)
			continue
		}
		if dispatchedPerPeer[intent.Peer] >= deleteIntentPerPeerPerSweep {
			throttled.add(intent)
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
	// here rather than from a loop of its own: it is the same subsystem on
	// a fitting cadence, and the wipe's own request is swept here too.
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
			Str("peer", logID(stranded.peer.String())).
			Str("request_id", logID(string(stranded.requestID))).
			Msg("dm_router: wipe reservation stranded past its TTL; releasing the barrier")
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:               stranded.peer,
			LocalCleanupFailed: true,
		})
	}
}

// parkedIntents is one sweep's set of requests to park, kept apart by what
// they are keyed on: a message request is parked by id, a conversation request
// by peer. Collected rather than parked one by one because a request to a
// contact who never comes back is kept indefinitely — the parked set is not a
// backlog that drains but a floor the sweep pays every tick, forever.
type parkedIntents struct {
	messages      []domain.MessageID
	conversations []domain.PeerIdentity
}

func (p *parkedIntents) add(intent chatlog.DeleteIntent) {
	if intent.Kind == chatlog.DeleteIntentConversation {
		p.conversations = append(p.conversations, intent.Peer)
		return
	}
	p.messages = append(p.messages, intent.MessageID)
}

func (p parkedIntents) len() int {
	return len(p.messages) + len(p.conversations)
}

// holdDeleteIntents parks a batch without charging any of it, and says so
// in the log at debug: a held intent looks like an idle one from the
// outside, and the reason it is idle is the interesting part.
func (r *DMRouter) holdDeleteIntents(ctx context.Context, store *chatlog.Store, parked parkedIntents, until time.Time) {
	if parked.len() == 0 {
		return
	}
	if len(parked.messages) > 0 {
		if err := store.HoldDeleteIntents(ctx, parked.messages, until); err != nil {
			log.Warn().Err(err).
				Int("intents", len(parked.messages)).
				Msg("dm_router: parking delete intents failed; they may hold the head of the sweep queue")
			return
		}
	}
	if len(parked.conversations) > 0 {
		if err := store.HoldConversationDeleteIntents(ctx, parked.conversations, until); err != nil {
			log.Warn().Err(err).
				Int("intents", len(parked.conversations)).
				Msg("dm_router: parking conversation-delete intents failed; they may hold the head of the sweep queue")
			return
		}
	}
	log.Debug().
		Int("message_intents", len(parked.messages)).
		Int("conversation_intents", len(parked.conversations)).
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
		pending := counts[peer]
		if state.PendingDeletes != pending.Messages || state.PendingConversationDelete != pending.Conversation {
			state.PendingDeletes = pending.Messages
			state.PendingConversationDelete = pending.Conversation
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
		log.Warn().Err(err).Str("peer", logID(peer.String())).Msg("dm_router: reviving delete intents failed; they wake on their own schedule")
	}
	if revived > 0 {
		log.Info().
			Str("peer", logID(peer.String())).
			Int64("intents", revived).
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
	if intent.Kind == chatlog.DeleteIntentConversation {
		// A wipe is NEVER written off. Giving up on it would leave the one
		// state this product may not produce: erased here, still there at the
		// peer, and nothing left that will ever ask again. The row is tiny, the
		// backoff caps at an hour, and it goes when the contact goes — that is
		// a cheaper price than a conversation the user believes is gone.
		return false
	}
	if intent.Attempts < deleteIntentGiveUpAttempts {
		return false
	}
	// The sweep works from a snapshot, and a wipe can be written between the
	// read and this line. Dropping the row then would take a request the wipe
	// is carrying — with it the durable refusal of that id — and announce
	// Abandoned for a deletion that is still going to be delivered.
	settled, carried, err := store.DropDeleteIntentUnlessCarried(ctx, intent.MessageID)
	if err != nil {
		log.Warn().Err(err).
			Str("target", logID(string(intent.MessageID))).
			Msg("dm_router: dropping an expired delete intent failed; will retry on the next sweep")
		return true
	}
	if carried || !settled {
		// Carried: the wipe answers for it, and its budget is not this one's to
		// spend. Already gone: somebody else retired it, and the outcome that
		// belonged to it has been published by whoever did.
		return true
	}
	// Same reason as on the ack path: the row that just went was the last
	// mention of this message here, and its page stays legible in the
	// write-ahead log until a checkpoint retires it.
	r.checkpointSoonAfterDelete()

	// Behind the diagnostics gate, although it reports a FAILURE — the one
	// place that exception does not apply.
	//
	// The exception exists because a support case must be able to see what went
	// wrong. Here it can: the user is told directly, on their own screen, by the
	// Abandoned outcome published two lines down. The log line is not the only
	// channel, and what it would leave behind is a permanent note that a
	// deletion was wanted and never delivered — after the request that made it
	// legitimate has just been dropped. Unfinished work may be written down;
	// this is no longer unfinished work.
	deletionLog().Warn().
		Str("target", logID(string(intent.MessageID))).
		Str("peer", logID(intent.Peer.String())).
		Int("attempts", intent.Attempts).
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
