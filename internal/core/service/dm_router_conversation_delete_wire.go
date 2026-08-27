package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// dm_router_conversation_delete_wire.go is the conversation wipe ON THE WIRE:
// the request going out, the same request arriving, and the answer that
// settles it.
//
// Why the wipe has a command of its own, when a deletion already has one:
// message_delete names an id and is answered per id, under the flag that id
// carries. A thread has no id, and answering it per message is what used to
// leave each side holding the half the other wrote — the requester's own
// messages came back refused, their screen showed an empty conversation, and
// nothing could ask again because the ids had gone with the rows.
//
// So this request names nothing at all. It cannot tell the peer about messages
// they never received, it does not grow with the thread, and it can be re-made
// long after the local conversation is empty — which is exactly what repairing
// an already-split thread needs.

// dispatchConversationDelete encodes the request and submits it through
// DMCrypto.SendControlMessage.
//
// Tests that need to count dispatches or avoid the rpc/identity stack install
// r.dispatchControlConversationDeleteFn; production leaves it nil.
func (r *DMRouter) dispatchConversationDelete(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error {
	if r.dispatchControlConversationDeleteFn != nil {
		return r.dispatchControlConversationDeleteFn(ctx, peer, requestID)
	}
	payload, err := domain.MarshalConversationDeletePayload(domain.ConversationDeletePayload{
		RequestID: requestID,
	})
	if err != nil {
		return fmt.Errorf("marshal conversation_delete payload: %w", err)
	}
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandConversationDelete, payload); err != nil {
		return err
	}
	return nil
}

// handleInboundConversationDelete applies a peer's request to clear the
// conversation the two share.
//
// Authorization is the conversation itself: the envelope is cryptographically
// bound to its sender, and every row in scope is one of the two identities'
// own. Authorship is NOT consulted — that is the whole difference from
// message_delete, and the reason this command exists. A thread wipe is a
// mutual forgetting the user confirms twice before it is sent; refusing the
// requester's own messages inside it is not a stricter rule but a broken one,
// since it leaves the conversation half-alive on each side with no way left to
// finish it.
//
// Immutable rows survive, as they survive every other deletion.
//
// Applying it is NOT idempotent, and that is the whole shape of the command,
// written out in five lines in docs/dm-commands.md §"The rule": a command
// arrives, the conversation is erased, the answer says so; it arrives again,
// the conversation is erased again and answered again; an answer for a request
// already retired is dropped; nothing is written down about having applied one;
// our own request is never written off until the peer answers.
//
// Nothing is remembered about having applied one — a deletion that left a
// record of itself on disk would be the one trace it exists to remove — and
// nothing needs to be, because the answer does not depend on what happened
// before.
//
// What that costs is a message written between two arrivals: the second one
// takes it. In its sharpest form — the ack is lost, the requester writes a new
// message, the sweep re-dispatches — the repeat erases that message here while
// the requester keeps it.
//
// Accepted, not overlooked: it follows from two rules that outrank it — the
// request is never written off, and nothing on disk records that a wipe was
// applied. Together those leave the receiver no way to tell "existed at the
// click" from "written since", so a repeat takes what arrived meanwhile. The
// full argument, the three boundary designs that were tried and why each is
// wrong, and what would have to be given up to close the case are in
// docs/dm-commands.md §"Why a repeat can take a new message". Removing it means
// giving up one of the five rules above, so a change that removes it has to
// name WHICH rule it drops.
func (r *DMRouter) handleInboundConversationDelete(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var payload domain.ConversationDeletePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete payload malformed; dropping")
		return
	}
	if !payload.Valid() {
		log.Debug().
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete payload invalid; dropping")
		return
	}

	status := r.applyInboundConversationDelete(envelopeSender, payload.RequestID)
	r.replyConversationDeleteAck(envelopeSender, payload.RequestID, status)
}

// applyInboundConversationDelete erases this side of the thread and reports
// what to answer. How MANY rows went is deliberately not reported anywhere: a
// count of them is a count of the messages the requester never had.
func (r *DMRouter) applyInboundConversationDelete(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) domain.ConversationDeleteStatus {
	store := r.client.chatlog.Store()
	if store == nil {
		// Answering "applied" here would tell the requester their
		// conversation is gone from this side while it sits in a chatlog
		// that is merely unreachable right now. `error` keeps their request
		// alive so the next sweep asks again.
		log.Warn().
			Str("peer", logID(peer.String())).
			Msg("dm_router: applyInboundConversationDelete: chatlog store unavailable")
		return domain.ConversationDeleteStatusError
	}
	ctx := r.opContext()

	// The gates go up BEFORE the thread is read, not after. Between a read and
	// a barrier there is room for an arriving message to land in the database
	// and miss the list — and then the requester is told `applied`, the request
	// is retired, and that message stays for good. The window is small and the
	// consequence is permanent, which is the combination that has to be closed
	// by ordering rather than by hoping.
	releaseRemoval := r.removals.begin(peer)
	resumeReactions := r.client.HoldReactionSends(peer)
	defer func() {
		resumeReactions()
		releaseRemoval()
	}()

	// Everything this node holds of the conversation, read at the moment the
	// request is applied. No bound of any kind: the request says "erase this
	// conversation", and the receiver answers with what it has.
	//
	// A repeat therefore erases whatever is there the second time as well, and
	// answers again. That is the deliberate shape of it — five arrivals, five
	// wipes, five answers — and it is what makes the receiving side completely
	// stateless: nothing is remembered about having applied a request, because
	// nothing needs to be. A record of having applied it would be the one trace
	// this gesture exists to remove.
	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		log.Warn().Err(err).
			Msg("dm_router: applyInboundConversationDelete: chatlog read failed")
		return domain.ConversationDeleteStatusError
	}

	// Stop OUR OWN deliveries of those messages before the transaction takes
	// them, exactly as the local wipe does. The rows leaving this database is
	// only half of it: a message of this thread still sitting in the delivery
	// queue would be handed over after the wipe and re-open, on the requester's
	// side, the conversation they just cleared.
	//
	// A freeze that FAILS stops the wipe. Erasing the rows anyway and answering
	// `applied` would retire the request while a copy of one of those messages
	// is still queued to go out — the exact outcome the freeze exists to
	// prevent, made permanent by the ack. `error` costs a round-trip and keeps
	// the request with the requester, which is the only durable retry either
	// side has.
	if len(scope.IDs) > 0 {
		if _, err := r.client.FreezeConversationDelivery(ctx, peer, scope.IDs); err != nil {
			log.Warn().Err(err).
				Msg("dm_router: applyInboundConversationDelete: could not stop our deliveries; refusing to erase what we might still send")
			return domain.ConversationDeleteStatusError
		}
	}

	// Refused before the transaction opens: a copy of one of these messages
	// arriving while it runs must be turned away, not stored behind the wipe.
	r.wipeTombstones.Note(scope.IDs, r.now())
	wiped, err := store.DeleteConversationForPeerRequest(ctx, peer, scope)
	if err == nil {
		r.client.ForgetConversationState(peer)
	}

	if err != nil {
		log.Warn().Err(err).
			Str("peer", logID(peer.String())).
			Msg("dm_router: applyInboundConversationDelete: transactional wipe failed; the thread is untouched")
		// The rows are alive, so the ids refused above name messages that still
		// exist: leaving those refusals would swallow a legitimate re-delivery
		// of any of them. The freeze goes the same way — those messages are
		// still ours to send.
		r.wipeTombstones.Forget(scope.IDs)
		if len(scope.IDs) > 0 {
			thawCtx, cancelThaw := context.WithTimeout(r.detachedCtx(ctx), conversationCompensationBudget)
			thawErr := r.client.ThawConversationDelivery(thawCtx, peer, scope.IDs)
			cancelThaw()
			if thawErr != nil {
				log.Error().Err(thawErr).
					Str("peer", logID(peer.String())).
					Msg("dm_router: applyInboundConversationDelete: the wipe failed AND our deliveries stayed frozen; they resume after a restart")
			}
		}
		return domain.ConversationDeleteStatusError
	}

	// The rows are gone, so this node is holding the payload of a conversation
	// that no longer exists on either side. Withdrawing does not thaw — that
	// would hand the requester what they asked us to erase.
	//
	// A withdrawal that fails is NOT acknowledged as applied. The in-memory
	// backlog retries it, but memory does not survive a crash, and `applied`
	// is terminal: the requester would drop the only durable record that
	// anything is still owed here. Answering `error` keeps their request alive,
	// and the repeat — bounded by the same moment, so it takes nothing new —
	// runs the withdrawal again.
	if len(scope.IDs) > 0 {
		_ = r.withdrawDeletedDeliveries(ctx, peer, scope.IDs)
	}

	r.withFileOps(peer, len(wiped.Removed) > 0, func() {
		if r.fileBridge == nil {
			return
		}
		for _, id := range wiped.Removed {
			r.fileBridge.OnMessageDeleted(id)
		}
	})

	r.evictWipedConversationFromUI(peer, wiped.Removed)
	r.publishReactionsChanged(peer)
	// The peer asked us to destroy this conversation — the deletion the
	// protocol exists to deliver — so it gets the same on-disk treatment as
	// our own: the pages leave the write-ahead log now rather than at the next
	// automatic checkpoint. One truncation for the whole thread, which is what
	// the per-message path could not afford and had to coalesce.
	//
	// Unconditional, for the reason given on the local path: a wipe that
	// removed no messages can still have cleared the conversation's orphaned
	// reactions, and those rows name the messages they were for.
	r.checkpointAfterDelete(ctx, store)

	// Anything this node still owes the peer keeps the request open — whether
	// this pass failed to withdraw it or an EARLIER one did.
	//
	// The check is on the backlog rather than on this call's result, because a
	// repeat is the case that matters: the rows are already gone, so its scope
	// is empty and it withdraws nothing, and an `applied` then would retire the
	// requester's request while a message of the erased conversation is still
	// queued here. The sweep retries the withdrawal; until it succeeds, every
	// repeat answers `error` and the request — the only durable record that
	// something is outstanding — stays with the requester.
	if r.withdrawals.owes(peer) {
		log.Warn().
			Msg("dm_router: conversation cleared at the peer's request, but our own deliveries of it are not withdrawn yet; answering error so they ask again")
		return domain.ConversationDeleteStatusError
	}

	deletionLog().Info().Msg("dm_router: conversation cleared at the peer's request")
	return domain.ConversationDeleteStatusApplied
}

// replyConversationDeleteAck encodes the answer and ships it back over the
// control wire. Best-effort — if the ack itself fails the requester will ask
// again, and applying the request twice removes nothing the second time.
func (r *DMRouter) replyConversationDeleteAck(
	peer domain.PeerIdentity,
	requestID domain.ConversationDeleteRequestID,
	status domain.ConversationDeleteStatus,
) {
	answer := domain.ConversationDeleteAckPayload{
		RequestID: requestID,
		Status:    status,
	}
	if r.dispatchControlConversationDeleteAckFn != nil {
		if err := r.dispatchControlConversationDeleteAckFn(context.Background(), peer, answer); err != nil {
			log.Warn().Err(err).Msg("dm_router: send conversation_delete_ack failed; requester will retry")
		}
		return
	}
	payload, err := domain.MarshalConversationDeleteAckPayload(answer)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", logID(peer.String())).
			Msg("dm_router: marshal conversation_delete_ack failed")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), conversationAckSendTimeout)
	defer cancel()
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandConversationDeleteAck, payload); err != nil {
		log.Warn().Err(err).
			Str("peer", logID(peer.String())).
			Msg("dm_router: send conversation_delete_ack failed; requester will retry")
	}
}

// conversationAckSendTimeout bounds one ack send. Same order as the
// per-message ack: the answer is small, and a peer that cannot take it in ten
// seconds will be asked again anyway.
const conversationAckSendTimeout = 10 * time.Second

// handleInboundConversationDeleteAck settles the wipe this node asked for.
//
// The local thread is long gone — it was erased at click time — so the ack
// decides only the fate of the request:
//
//   - applied — the peer is consistent with us. Drop the request; the wipe is
//     finished on both sides.
//   - error — the peer could not decide. Keep the request and say nothing to
//     the user; the sweep asks again on the backoff already charged.
func (r *DMRouter) handleInboundConversationDeleteAck(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var ack domain.ConversationDeleteAckPayload
	if err := json.Unmarshal([]byte(payloadJSON), &ack); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete_ack payload malformed; dropping")
		return
	}
	if !ack.Valid() {
		log.Debug().
			Str("envelope_sender", logID(envelopeSender.String())).
			Str("status", string(ack.Status)).
			Msg("dm_router: conversation_delete_ack payload invalid; dropping")
		return
	}

	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("peer", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete_ack: chatlog store unavailable; request left scheduled")
		return
	}
	ctx := r.opContext()

	intent, found, err := store.ConversationDeleteIntentForPeer(ctx, envelopeSender)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete_ack: request lookup failed; the sweep will re-issue")
		return
	}
	if !found {
		log.Debug().
			Str("peer", logID(envelopeSender.String())).
			Str("request_id", logID(string(ack.RequestID))).
			Msg("dm_router: conversation_delete_ack for a request nobody is waiting on; dropping")
		return
	}
	// An ack for a wipe the user has already replaced settles nothing: the
	// current request was made about a conversation as it is NOW, and the
	// answer to the previous one says nothing about it.
	if intent.RequestID != ack.RequestID {
		log.Warn().
			Str("peer", logID(envelopeSender.String())).
			Str("expected_request_id", logID(string(intent.RequestID))).
			Str("actual_request_id", logID(string(ack.RequestID))).
			Msg("dm_router: conversation_delete_ack echoes a superseded request; keeping the current one")
		return
	}

	if ack.Status.IsTransient() {
		// The peer could not decide. Keep the request and say nothing to the
		// UI — the wipe is still outstanding. The schedule is left alone: the
		// dispatch that provoked this answer already charged its attempt, and
		// charging again here would burn the give-up budget at double rate.
		log.Info().
			Str("peer", logID(envelopeSender.String())).
			Int("attempts", intent.Attempts).
			Msg("dm_router: conversation_delete_ack: peer reported a transient failure; request kept")
		return
	}

	// By (peer, request id), in ONE statement. The comparison above is a
	// cheap early exit; it is not the guard. Between reading the row and
	// deleting it the user can reserve a fresh wipe, and a delete keyed on
	// the peer alone would retire THAT request on the strength of the
	// previous one's answer — the UI would report both sides cleared while
	// nothing had been asked of the peer at all.
	settled, err := store.DropConversationDeleteIntent(ctx, envelopeSender, ack.RequestID)
	if err != nil {
		// The request is still on disk, so the sweep will ask again and the
		// "waiting for the peer" indicator is still true. Publishing a settled
		// outcome here would put the UI in two states at once: "the messages
		// are deleted at the peer" and a pending marker that keeps
		// re-dispatching the same wipe.
		log.Warn().Err(err).
			Str("peer", logID(envelopeSender.String())).
			Msg("dm_router: conversation_delete_ack: dropping the request failed; it stays scheduled and the outcome is not published")
		r.refreshPendingDeleteCounts()
		return
	}
	if settled {
		// The refusals of this conversation's ids STAY until they expire on
		// their own; see the same decision on the per-message path. An ack is
		// the peer's database, not the queues that may still hold copies of
		// those envelopes.
		//
		// The request was the last row on this disk that belonged to the wipe,
		// and a deleted row is still readable in the write-ahead log until a
		// checkpoint retires its page. Attempted here; a failure does NOT
		// withhold the outcome, for the reason spelled out on the per-message
		// ack path: the request is already retired, so nothing would ever
		// publish it afterwards and the user would be left with a pending
		// indicator that vanished without an answer. The retrying checkpointer
		// still finishes the truncation. See docs/dm-commands.md §"Why an
		// outcome is reported before the log is truncated".
		r.checkpointAfterDelete(ctx, store)
	}
	if !settled {
		// The row moved under us: a newer wipe replaced it between the read
		// and the delete. Nothing to report — the current request has not
		// been answered.
		log.Warn().
			Str("peer", logID(envelopeSender.String())).
			Str("request_id", logID(string(ack.RequestID))).
			Msg("dm_router: conversation_delete_ack: the request was replaced before the answer landed; the new one stands")
		return
	}

	r.refreshPendingDeleteCounts()

	deletionLog().Info().
		Str("peer", logID(envelopeSender.String())).
		Str("status", string(ack.Status)).
		Msg("dm_router: conversation wipe settled by the peer")

	r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
		Peer:     envelopeSender,
		Settled:  true,
		Status:   ack.Status,
		Attempts: intent.Attempts,
	})
}
