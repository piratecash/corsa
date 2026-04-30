package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// message_delete / message_delete_ack — sender retry + receiver auth
// ---------------------------------------------------------------------------
//
// This file implements the application-level reliability layer for
// control DMs that ask the recipient to delete a previously delivered
// data DM. The wire-level plumbing lives in service/dm_crypto_control.go
// (DMCrypto.SendControlMessage / DecryptIncomingControlMessage) and
// node/service.go (send_control_message Frame.Type, dispatchNetworkFrame
// topic-aware divergence).
//
// Design contract: docs/dm-commands.md.

// Retry policy mirrors filetransfer chunk-request retry (see
// docs/dm-commands.md §"Acknowledgement and retry"). Initial 30 s,
// exponential backoff x2, cap 300 s, **max 6 dispatches total** —
// one initial dispatch in SendMessageDelete plus up to five retries
// in processDeleteRetryDue. After the budget is spent the pending
// entry is dropped with a warn log and TopicMessageDeleteCompleted
// fires with Abandoned=true; the OUTGOING LOCAL ROW STAYS ALIVE
// (pessimistic ordering — DELETE only happens on success ack inside
// handleInboundMessageDeleteAck), so the user can re-issue the
// delete from the UI.
const (
	deleteRetryInitial    = 30 * time.Second
	deleteRetryCap        = 300 * time.Second
	deleteRetryMultiplier = 2
	deleteRetryMaxAttempt = 6
	deleteRetryTickPeriod = 5 * time.Second
)

// pendingDelete tracks a single in-flight message_delete on the sender
// side. Created by SendMessageDelete BEFORE the wire dispatch; cleared
// by handleInboundMessageDeleteAck (any terminal status) or by the
// retry loop when the dispatch budget is exhausted.
//
// Pessimistic ordering: for an outgoing message the local chatlog row
// is NOT deleted at the time the pending entry is created. The actual
// chatlog mutation runs inside handleInboundMessageDeleteAck only when
// the peer replies with a success status (deleted / not_found). On
// denied / immutable / abandoned the local row stays alive so the
// user sees the rejection instead of a silently diverging chat
// thread.
//
// Two sender paths bypass this gate intentionally and ARE synchronous
// with the user click:
//   - incoming local-only delete (the user removes their view of a
//     message the peer sent them) — under the default sender-delete
//     policy the peer would refuse our request anyway, so the wire
//     round-trip is skipped and chatlog DELETE + cleanup run inline
//     in SendMessageDelete;
//   - recovery !found delete — there is no local row to wait on; we
//     dispatch through the wire path so the peer can converge with us.
//
// The sender treats both MessageDeleteStatusDeleted and
// MessageDeleteStatusNotFound as success — the recipient is
// consistent with our intent in both cases. The post-ack DELETE is
// idempotent: the recovery path's no-op DeleteByID is harmless.
//
// IN-MEMORY ONLY: this map is not persisted across process restart.
// On crash during in-flight retry the local row is INTACT (because
// pessimistic ordering never deleted it), so the user can simply
// re-issue the delete from the UI. The UI surfaces in-process
// budget-exhaustion via TopicMessageDeleteCompleted with
// Abandoned=true; there is no equivalent signal across a restart.
// Adding JSON persistence rendezvous-ed at a path alongside
// transfers-*.json is a tracked follow-up — see
// docs/dm-commands.md §"Acknowledgement and retry".
type pendingDelete struct {
	target      domain.MessageID
	peer        domain.PeerIdentity
	sentAt      time.Time
	nextRetryAt time.Time
	attempt     int
}

// deleteRetryState holds the sender-side pending-delete map and its
// dedicated mutex. Kept as a separate struct (rather than fields on
// DMRouter directly) so the lock surface stays narrow and so DMRouter
// does not need a second mutex on its hot path. The retry loop
// goroutine drives sweeps; SendMessageDelete and the inbound ack
// handler mutate entries.
type deleteRetryState struct {
	mu      sync.Mutex
	entries map[domain.MessageID]*pendingDelete
}

func newDeleteRetryState() *deleteRetryState {
	return &deleteRetryState{
		entries: make(map[domain.MessageID]*pendingDelete),
	}
}

func (s *deleteRetryState) add(p *pendingDelete) {
	s.mu.Lock()
	s.entries[p.target] = p
	s.mu.Unlock()
}

func (s *deleteRetryState) remove(target domain.MessageID) (*pendingDelete, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.entries[target]
	if !ok {
		return nil, false
	}
	delete(s.entries, target)
	return p, true
}

// dueEntries returns a snapshot of pending entries whose nextRetryAt has
// passed at the given time. Returned slice is independent of the map
// so callers can release the mutex while doing network I/O.
func (s *deleteRetryState) dueEntries(now time.Time) []pendingDelete {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []pendingDelete
	for _, p := range s.entries {
		if !p.nextRetryAt.After(now) {
			due = append(due, *p)
		}
	}
	return due
}

// recordAttempt updates an existing entry after a dispatch: bumps
// attempt, computes next backoff, records sentAt. Returns terminal=true
// when the just-completed dispatch was the deleteRetryMaxAttempt-th
// — the entry is dropped from the map so no further retries occur.
//
// Counting model (attempt == "number of dispatches that have actually
// gone out"):
//
//   - SendMessageDelete sets attempt=1 right after the initial dispatch.
//   - The retry loop dispatches FIRST and THEN calls recordAttempt,
//     which bumps attempt to reflect the just-completed dispatch and
//     decides whether the budget is now spent.
//
// With deleteRetryMaxAttempt=6 the trace is:
//
//	initial: attempt set to 1 (1 dispatch made)
//	tick:    dispatch #2 → bump to 2 → 2 < 6 → keep
//	tick:    dispatch #3 → bump to 3 → keep
//	...
//	tick:    dispatch #6 → bump to 6 → 6 >= 6 → terminal
//
// Without the >= here the loop would do a 7th dispatch and only retire
// after that.
func (s *deleteRetryState) recordAttempt(target domain.MessageID, now time.Time) (entry pendingDelete, terminal bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.entries[target]
	if !ok {
		return pendingDelete{}, false
	}
	p.attempt++
	p.sentAt = now
	if p.attempt >= deleteRetryMaxAttempt {
		// The dispatch that preceded this recordAttempt was the
		// budget's last one. Retire the entry. Caller surfaces the
		// warn log and any UI state changes outside the lock.
		out := *p
		delete(s.entries, target)
		return out, true
	}
	backoff := time.Duration(deleteRetryInitial) * (1 << (p.attempt - 1))
	if backoff > deleteRetryCap {
		backoff = deleteRetryCap
	}
	p.nextRetryAt = now.Add(backoff)
	return *p, false
}

// ---------------------------------------------------------------------------
// Sender-side API
// ---------------------------------------------------------------------------

// SendMessageDelete is the canonical sender-side entry point that asks
// the peer to mirror a DM deletion. The local chatlog row and any
// attached file-transfer state are removed ONLY after the recipient's
// message_delete_ack arrives with a success status (deleted /
// not_found) — see handleInboundMessageDeleteAck for the actual
// mutation. This pessimistic ordering means a peer rejection
// (denied / immutable / abandoned) leaves the local row untouched, so
// the user sees the failure instead of a silently-diverging copy.
//
// Three direction cases:
//
//   - found && entry.Sender == myAddr (outgoing) — wire path. The
//     peer is the row's Recipient; the caller-supplied peer is
//     ignored as untrustworthy (a buggy or malicious caller could
//     otherwise dispatch the deletion to the wrong conversation).
//     Local row stays until ack.
//   - found && entry.Sender != myAddr (incoming) — local-only.
//     Under default sender-delete the peer would reply denied, so we
//     do not waste a wire round-trip. The local row + file-transfer
//     state ARE removed synchronously here; the user's intent is
//     local-side recall, not peer-side.
//   - !found — recovery path. The row is already absent, so there is
//     nothing local to gate on the ack. We still go through the wire
//     path with the caller-supplied peer so a re-issued delete after
//     a crash (which dropped the in-memory pendingDelete queue) can
//     converge the peer. The ack handler's idempotent DeleteByID is
//     a no-op when the row is gone.
//
// Authorization: an immutable target row triggers an early error and
// no state mutation. Other flags only affect the peer-side decision;
// the local side is permissive (the user can always remove their own
// view of a message).
func (r *DMRouter) SendMessageDelete(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.client == nil {
		return fmt.Errorf("DMRouter has no client")
	}
	peer = normalizePeer(peer)
	if peer == "" {
		return fmt.Errorf("peer is required")
	}
	if !target.IsValid() {
		return fmt.Errorf("target message id %q is not a valid UUID v4", target)
	}

	store := r.client.chatlog.Store()
	if store == nil {
		return fmt.Errorf("chatlog store is not available")
	}

	entry, found, err := store.EntryByID(target)
	if err != nil {
		return fmt.Errorf("lookup target %s: %w", target, err)
	}

	myAddr := r.client.Address()

	if found {
		flag := protocol.MessageFlag(entry.Flag)
		if flag == protocol.MessageFlagImmutable {
			return fmt.Errorf("target %s is immutable and cannot be deleted", target)
		}

		// Derive the actual conversation peer from the row, NOT from
		// the caller. The caller-supplied peer is only trusted on
		// !found (recovery), where there is no row to derive from.
		// A caller that passes a wrong peer for a found row would
		// otherwise leak the deletion to the wrong conversation.
		entrySender := domain.PeerIdentity(entry.Sender)
		entryRecipient := domain.PeerIdentity(entry.Recipient)
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
				Str("caller_peer", string(peer)).
				Str("derived_peer", string(derivedPeer)).
				Msg("dm_router: SendMessageDelete: caller peer did not match the row; using derived peer")
		}
		peer = derivedPeer

		if isIncoming {
			// Local-only path: the user removes their view of an
			// incoming message. Default sender-delete would have the
			// peer reject our request anyway, so we skip the wire
			// round-trip entirely. Local row + file-transfer state
			// are removed synchronously, the live UI cache is
			// evicted, and a terminal "deleted" outcome is published
			// so the UI settles immediately.
			if _, err := store.DeleteByID(target); err != nil {
				return fmt.Errorf("delete chatlog entry %s: %w", target, err)
			}
			if r.fileBridge != nil {
				r.fileBridge.OnMessageDeleted(target)
			}
			r.evictDeletedMessageFromUI(peer, target)

			log.Info().
				Str("target", string(target)).
				Str("peer", string(peer)).
				Msg("dm_router: message_delete handled locally (incoming message)")

			r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
				Target:    target,
				Peer:      peer,
				Status:    domain.MessageDeleteStatusDeleted,
				Abandoned: false,
				Attempts:  0,
			})
			return nil
		}
		// Outgoing message: fall through to the wire path. Local row
		// stays alive until handleInboundMessageDeleteAck confirms
		// the peer has acted on our request.
	} else {
		// !found: keep caller-supplied peer for the recovery path.
		log.Debug().
			Str("target", string(target)).
			Str("peer", string(peer)).
			Msg("dm_router: SendMessageDelete: target already absent locally; running recovery wire path")
	}

	// Wire path. Pend a retry entry BEFORE the first dispatch so a
	// synchronous SendControlMessage failure (e.g. rpc unavailable)
	// does not lose the in-flight request. attempt=1 reflects the
	// dispatch we are about to make; the retry loop bumps from there.
	now := time.Now().UTC()
	r.deleteRetry.add(&pendingDelete{
		target:      target,
		peer:        peer,
		sentAt:      now,
		nextRetryAt: now.Add(deleteRetryInitial),
		attempt:     1,
	})

	if err := r.dispatchMessageDelete(ctx, peer, target); err != nil {
		// Wire submission failed; pending entry stays so the retry
		// loop can attempt again. Local row is intact — the user
		// will see either a success (chatlog DELETE on ack) or an
		// abandonment (TopicMessageDeleteCompleted with
		// Abandoned=true) once the budget is exhausted.
		log.Warn().Err(err).
			Str("target", string(target)).
			Str("peer", string(peer)).
			Msg("dm_router: SendMessageDelete: initial wire send failed; retry pending")
		return nil
	}

	log.Info().
		Str("target", string(target)).
		Str("peer", string(peer)).
		Msg("dm_router: message_delete dispatched; awaiting peer ack before local DELETE")
	return nil
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
			n, err := store.UnreadCountFor(peer)
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
			Str("sender", string(sender)).
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
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: message_delete payload malformed; dropping")
		return
	}
	if !payload.Valid() {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
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
		// Chatlog unavailable. Treat as not_found so the sender can
		// stop retrying — there is no recoverable state on this side.
		log.Warn().
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: chatlog store unavailable")
		return domain.MessageDeleteStatusNotFound
	}

	entry, found, err := store.EntryByID(target)
	if err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: lookup failed")
		return domain.MessageDeleteStatusNotFound
	}
	if !found {
		// Idempotent success: the row is already absent on this side.
		return domain.MessageDeleteStatusNotFound
	}

	flag := protocol.MessageFlag(entry.Flag)
	if flag == protocol.MessageFlagImmutable {
		log.Warn().
			Str("target", string(target)).
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: applyInboundDelete: target is immutable")
		return domain.MessageDeleteStatusImmutable
	}

	if !authorizedToDelete(flag, envelopeSender, domain.PeerIdentity(entry.Sender), domain.PeerIdentity(entry.Recipient)) {
		log.Warn().
			Str("target", string(target)).
			Str("envelope_sender", string(envelopeSender)).
			Str("target_sender", entry.Sender).
			Str("flag", string(flag)).
			Msg("dm_router: applyInboundDelete: envelope sender not authorized for this flag")
		return domain.MessageDeleteStatusDenied
	}

	if _, err := store.DeleteByID(target); err != nil {
		log.Warn().Err(err).
			Str("target", string(target)).
			Msg("dm_router: applyInboundDelete: chatlog DeleteByID failed")
		// Treat as not_found so the sender stops retrying. Local state
		// is whatever the database left us with; we cannot do better
		// without operator intervention.
		return domain.MessageDeleteStatusNotFound
	}

	if r.fileBridge != nil {
		r.fileBridge.OnMessageDeleted(target)
	}

	// Drop the deleted bubble from the live conversation cache and
	// refresh the sidebar preview. The conversation peer (relative to
	// us) is the *other* party of the original message — for an
	// inbound message we are receiving from the sender, so the
	// thread peer is entry.Sender; for outbound it would be
	// entry.Recipient. Compute it generically via myAddr to be
	// resilient to either direction (in practice on this side the
	// deleted message is incoming, so peer == entry.Sender).
	myAddr := r.client.Address()
	threadPeer := domain.PeerIdentity(entry.Sender)
	if threadPeer == myAddr {
		threadPeer = domain.PeerIdentity(entry.Recipient)
	}
	r.evictDeletedMessageFromUI(threadPeer, target)

	log.Info().
		Str("target", string(target)).
		Str("envelope_sender", string(envelopeSender)).
		Msg("dm_router: applied inbound message_delete")

	return domain.MessageDeleteStatusDeleted
}

// authorizedToDelete is the pure-function authorization predicate. The
// envelope sender (cryptographically verified upstream in
// storeIncomingMessage) is compared against the target message's
// participants under the rules of the target's flag.
//
// Empty / unknown flag is treated as MessageFlagSenderDelete to match
// the documented default policy (see docs/dm-commands.md §"Authorization"
// table row "empty / unknown").
func authorizedToDelete(flag protocol.MessageFlag, envelopeSender, targetSender, targetRecipient domain.PeerIdentity) bool {
	switch flag {
	case protocol.MessageFlagAnyDelete:
		return envelopeSender == targetSender || envelopeSender == targetRecipient
	case protocol.MessageFlagSenderDelete,
		protocol.MessageFlagAutoDeleteTTL,
		"":
		return envelopeSender == targetSender
	default:
		// Unknown flag: be conservative, treat as sender-delete.
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
			Str("peer", string(peer)).
			Msg("dm_router: send message_delete_ack failed; requester will retry")
	}
}

// handleInboundMessageDeleteAck removes the matching pendingDelete
// entry on any terminal status. Stale acks (target_id not present in
// pendingDelete) are dropped silently — this matches the contract that
// "every authenticated message_delete is idempotent and therefore so
// is its ack from the sender's perspective" (docs/dm-commands.md
// §"Idempotency").
func (r *DMRouter) handleInboundMessageDeleteAck(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var ack domain.MessageDeleteAckPayload
	if err := json.Unmarshal([]byte(payloadJSON), &ack); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: message_delete_ack payload malformed; dropping")
		return
	}
	if !ack.Valid() {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("target_id", string(ack.TargetID)).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack payload invalid; dropping")
		return
	}

	pending, ok := r.deleteRetry.remove(ack.TargetID)
	if !ok {
		log.Debug().
			Str("target", string(ack.TargetID)).
			Str("status", string(ack.Status)).
			Msg("dm_router: message_delete_ack for unknown pending; dropping")
		return
	}
	// Cross-check: the ack must come from the peer we addressed. A
	// peer cannot ack on behalf of someone else.
	if pending.peer != envelopeSender {
		log.Warn().
			Str("target", string(ack.TargetID)).
			Str("expected_peer", string(pending.peer)).
			Str("actual_envelope_sender", string(envelopeSender)).
			Msg("dm_router: message_delete_ack from unexpected peer; pending entry restored")
		// Restore the entry so the retry loop continues — the real
		// peer's ack may still arrive.
		r.deleteRetry.add(pending)
		return
	}
	// Pessimistic delete: the local row is dropped here, only after
	// the peer confirmed the action. A success status (deleted /
	// not_found) means the peer is consistent with our intent — we
	// can now mutate local state. A failure status (denied /
	// immutable) means the peer kept the row; we leave our copy in
	// place so the user can see the rejection reason and the chat
	// thread does not silently diverge.
	if ack.Status.IsTerminalSuccess() {
		store := r.client.chatlog.Store()
		if store != nil {
			if _, err := store.DeleteByID(ack.TargetID); err != nil {
				// Best-effort. If the row cannot be removed for some
				// non-recoverable reason (corrupted database, etc.) we
				// still publish the outcome so the UI doesn't hang —
				// the divergence with the peer is now in the opposite
				// direction (we have the row, peer does not), but the
				// retry loop has already retired this entry and
				// surfacing it via the failure log is the best we can
				// do without operator intervention.
				log.Warn().Err(err).
					Str("target", string(ack.TargetID)).
					Msg("dm_router: message_delete_ack: chatlog DeleteByID failed; UI may be inconsistent")
			}
		}
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(ack.TargetID)
		}
		r.evictDeletedMessageFromUI(envelopeSender, ack.TargetID)
	}

	log.Info().
		Str("target", string(ack.TargetID)).
		Str("peer", string(envelopeSender)).
		Str("status", string(ack.Status)).
		Int("attempts", pending.attempt).
		Bool("local_row_removed", ack.Status.IsTerminalSuccess()).
		Msg("dm_router: message_delete completed")

	r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
		Target:    ack.TargetID,
		Peer:      envelopeSender,
		Status:    ack.Status,
		Abandoned: false,
		Attempts:  pending.attempt,
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
// Retry loop
// ---------------------------------------------------------------------------

// deleteRetryLoop runs in a dedicated goroutine launched from Start().
// On each tick it scans pendingDelete for entries whose nextRetryAt has
// passed and re-dispatches the message_delete control DM. Entries that
// hit the dispatch budget are dropped with a warn log and a final
// TopicMessageDeleteCompleted publication with Abandoned=true; they
// are never retried again. The OUTGOING LOCAL ROW STAYS ALIVE on
// abandonment (pessimistic ordering keeps it until success ack), so
// the user simply sees that the deletion never converged with the
// peer and can re-issue from the UI — see docs/dm-commands.md.
//
// The loop terminates when ctx is cancelled. If ctx is
// context.Background() (current behaviour), the loop runs for the
// process lifetime. pendingDelete is in-memory only — a process
// restart starts from a clean slate, dropping all in-flight retry.
// This is the documented contract; JSON persistence is a tracked
// follow-up.
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
		}
	}
}

// processDeleteRetryDue is one sweep of the retry loop, factored out
// for testability. It collects due entries under the deleteRetry mutex,
// then dispatches them outside the mutex so a slow rpc call cannot
// stall future sweeps.
//
// Order of operations is significant: the dispatch happens BEFORE
// recordAttempt. recordAttempt is the "this attempt is now done"
// bookkeeping step — it bumps the counter and decides whether the
// just-completed attempt was the last one. Doing the dispatch first
// is what makes deleteRetryMaxAttempt the actual number of dispatches
// rather than the number of dispatches plus one (the previous order
// retired the entry on attempt N before dispatch N could run).
func (r *DMRouter) processDeleteRetryDue(ctx context.Context, now time.Time) {
	due := r.deleteRetry.dueEntries(now)
	for _, entry := range due {
		// Dispatch the retry first. Failures are logged but do not
		// short-circuit the bookkeeping — the entry must still be
		// charged for this attempt slot so the retry budget converges.
		if err := r.dispatchMessageDelete(ctx, entry.peer, entry.target); err != nil {
			log.Debug().Err(err).
				Str("target", string(entry.target)).
				Str("peer", string(entry.peer)).
				Int("attempt", entry.attempt+1).
				Msg("dm_router: message_delete retry send failed; will count toward retry budget")
		}

		updated, terminal := r.deleteRetry.recordAttempt(entry.target, now)
		if terminal {
			log.Warn().
				Str("target", string(updated.target)).
				Str("peer", string(updated.peer)).
				Int("attempts", updated.attempt).
				Msg("dm_router: message_delete retry budget exhausted after final dispatch; giving up")
			r.publishMessageDeleteOutcome(ebus.MessageDeleteOutcome{
				Target:    updated.target,
				Peer:      updated.peer,
				Status:    "",
				Abandoned: true,
				Attempts:  updated.attempt,
			})
			continue
		}
		// recordAttempt also handles the unknown-target case
		// (concurrent ack removed the entry just before we
		// incremented): it returns terminal=false with a zero
		// pendingDelete. There is nothing else to do — the dispatch
		// above is harmless if the recipient already acked, and the
		// entry is gone from the pending map.
		_ = updated
	}
}
