package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ErrConversationDeleteInflight is returned by SendMessage and
// SendFileAnnounce when the caller tries to send to a peer that
// has a wipe in progress. The block
// is the outgoing-side barrier that closes the race where a new
// user-authored message would land in chatlog just after the local
// wipe read it, surviving a wipe the user believes erased the
// thread. Callers (UI) should match via errors.Is and render a
// localised "wipe in progress" hint instead of a generic send
// failure. The block lifts as soon as the wipe and its intent have
// committed — it covers the click-to-wipe window only, so the
// barrier stays raised until the next definitive outcome.
var ErrConversationDeleteInflight = errors.New("conversation wipe: already pending for peer")

// ErrConversationDeleteReservationLost is returned by
// CompleteConversationDelete when the pending entry that
// BeginConversationDelete reserved is no longer in the map at the
// time Complete tries to claim or attach to it. The reservation can
// be lost for two reasons in the current model:
//
//   - The TTL reaper (pruneStaleReservations) dropped the entry
//     because it sat unprepared past convDeleteReservationTTL —
//     typically because the goroutine launching Complete was
//     scheduled too late after Begin.
//   - A concurrent control path retired the entry (e.g. a stray
//     applied ack matching this requestID, which is effectively
//     impossible before the first dispatch but covered for
//     defensive symmetry).
//
// Callers (UI) MUST match via errors.Is and refrain from rendering
// "wipe request sent / dispatched" status in this case — no wire
// command went out under this requestID. The reaper has already
// published an Abandoned outcome on TopicConversationDeleteCompleted
// so the UI status string transitions through that subscriber
// rather than from this return value.
var ErrConversationDeleteReservationLost = errors.New("conversation wipe: reservation lost before complete")

// ---------------------------------------------------------------------------
// The bulk wipe-the-thread
// control DM. Sibling of dm_router_delete.go, which handles the
// per-message variant. The two share the same wire transport
// (DMCrypto.SendControlMessage / DecryptIncomingControlMessage), the
// same durable-intent scheduler, AND the same ordering on the sender
// side: the local thread is erased when the user confirms, and the
// peer's half becomes a request that outlives the click.
//
// Why not wait for the ack: a conversation the user asked to destroy
// must not sit on this disk while a peer who may be offline for days
// decides. That wait is what used to make "Delete chat and ask the peer"
// unavailable offline at all. The failure the old ordering protected
// against — the network half never completing — is now expressed
// instead as a request the scheduler keeps carrying and, at the very
// end of its TTL, an Abandoned outcome that says the PEER's copy may
// remain. Nothing in that path can leave the user's own history
// standing against their instruction.
//
// Authorization deliberately diverges from message_delete:
//
//   - message_delete (single row): runs authorizedToDelete per row
//     so a per-flag matrix decides whether the requesting peer may
//     touch THIS message. The sender of the row owns its lifecycle
//     under the default sender-delete policy.
//   - the bulk wipe (whole thread): erases every non-immutable
//     row of the conversation regardless of authorship. Reusing
//     authorizedToDelete would refuse all rows the requester did
//     not author — under the default sender-delete that means HALF
//     the thread survives on each side after a "wipe everything"
//     gesture, which directly contradicts the user-visible promise
//     "Delete chat and ask the peer". The bulk gesture is a mutual
//     consent to forget the conversation and is initiated by an
//     explicit two-click confirmation in the UI, so it carries
//     stronger authority over peer-authored rows than a single
//     message_delete would. Immutable rows are the only carve-out
//     and stay on both sides.
//
// Design contract: docs/dm-commands.md.

// The reservation is all this file schedules on its own now: the
// peer's half is N ordinary delete intents, which the same sweep
// drives under the same policy as any single deletion — one set of
// rules, because there is only one kind of request.
const (
	// convDeleteReservationTTL bounds how long a reservation may stay
	// installed by BeginConversationDelete without
	// CompleteConversationDelete finishing it. The reservation is a
	// synchronous barrier latch and the work behind it (drain, wipe,
	// intent) is local, so the healthy gap is seconds. A value
	// comfortably above that catches pathological scheduling delays
	// without leaving an orphan reservation pinned to the peer forever
	// (e.g. if the calling code panics between Begin and the goroutine
	// launch). On TTL expiry the periodic reaper drops the entry; the
	// user can then re-issue the wipe.
	convDeleteReservationTTL = 60 * time.Second
)

// pendingConversationDelete tracks a single in-flight
// wipe on the sender side. Keyed by peer (one wipe
// per peer at a time — a second click on the same identity while a
// wipe is being prepared is a no-op: SendConversationDelete
// short-circuits via has(peer) + atomic tryAdd, and the original
// request continues).
//
// The entry lives only from BeginConversationDelete to the end of
// CompleteConversationDelete — the window in which the local wipe
// must not race a send. Everything the peer still owes us after that
// lives in message_delete_intents, not here.
//
// requestID binds the entry to the wire request that produced it.
// The sweep re-dispatches with the SAME requestID so genuine
// duplicates of the same request are matched on ack; a fresh wipe
// (e.g. after the previous one was abandoned) generates a NEW
// requestID and the ack handler refuses any inbound ack whose
// echoed requestID does not match the current pending entry. Without
// this guard a late ack from an abandoned earlier wipe could
// silently retire the new pending and trigger the local sweep before
// the new wipe was applied on the recipient.
//
// The reservation itself is in-memory and short-lived — it covers the
// click-to-wipe window and nothing more. What survives a restart is
// the requests in message_delete_intents, which the sweep resumes
// with its original requestID, so a peer's ack from before the restart
// still matches.
// The fresh requestID drives a real first-contact gather on the
// peer over whatever scope exists when the new request lands;
// it is NOT guaranteed to find zero rows or return
// Applied/Deleted=0.
type pendingConversationDelete struct {
	peer      domain.PeerIdentity
	requestID domain.ConversationDeleteRequestID
	// reservedAt records when BeginConversationDelete installed the
	// reservation, so the reaper can drop one whose
	// CompleteConversationDelete never ran rather than leave the
	// outgoing barrier raised on that peer until process restart.
	reservedAt time.Time
}

// conversationDeleteRetryState holds the sender-side
// pendingConversationDelete map and its dedicated mutex. Same shape
// as deleteRetryState — kept separate so the lock surfaces stay
// narrow and the two retry loops do not contend on a single mutex.
//
// inflight + drained close the outgoing send race: SendMessage /
// SendFileAnnounce do an ATOMIC "barrier check + inflight increment"
// via acquireSendIfNoPending under the same mutex that guards entries,
// so a send that wins the race holds an inflight slot the snapshot
// step (CompleteConversationDelete) must wait out via
// inflightDrainedChan before reading chatlog. Without this two-step
// gate a send goroutine that observed an empty barrier could land in
// chatlog AFTER BeginConversationDelete raised the barrier but
// BEFORE CompleteConversationDelete wiped — the row would survive a
// wipe the user believes erased the thread, and the conversation
// would look empty while it is not.
type conversationDeleteRetryState struct {
	mu       sync.Mutex
	entries  map[domain.PeerIdentity]*pendingConversationDelete
	inflight map[domain.PeerIdentity]int
	drained  map[domain.PeerIdentity]chan struct{}
}

func newConversationDeleteRetryState() *conversationDeleteRetryState {
	return &conversationDeleteRetryState{
		entries:  make(map[domain.PeerIdentity]*pendingConversationDelete),
		inflight: make(map[domain.PeerIdentity]int),
		drained:  make(map[domain.PeerIdentity]chan struct{}),
	}
}

// acquireSendIfNoPending is the atomic "barrier check + inflight
// increment" primitive every outgoing user-authored send must use.
// Returns true (and bumps the inflight counter) when no pending
// wipe is in progress for peer; returns false when the
// barrier is up. The caller MUST pair every true return with exactly
// one releaseSend(peer) — typically via defer in the send goroutine.
//
// Combining check + increment under the SAME mutex hold is
// load-bearing: a separate has(peer) followed by a separate
// inflight++ would let BeginConversationDelete slip in between the
// two steps, raising the barrier and missing the slot the send is
// about to occupy. CompleteConversationDelete's drain wait would
// then see inflight=0 and snapshot before the actual send finished
// landing.
//
// nil receiver returns true without recording anything so test
// fixtures that omit convDeleteRetry can still exercise SendMessage.
func (s *conversationDeleteRetryState) acquireSendIfNoPending(peer domain.PeerIdentity) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[peer]; exists {
		return false
	}
	s.inflight[peer]++
	return true
}

// releaseSend decrements the in-flight counter for peer and, when the
// counter reaches zero, closes any channel returned by a prior
// inflightDrainedChan(peer) call so a waiting CompleteConversationDelete
// can proceed. nil receiver / unknown peer are no-ops.
func (s *conversationDeleteRetryState) releaseSend(peer domain.PeerIdentity) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := s.inflight[peer]
	if count <= 0 {
		// Defensive: a release without a matching acquire would
		// underflow. Drop the entry rather than carry a negative
		// count that would forever block future drains.
		delete(s.inflight, peer)
		return
	}
	count--
	if count == 0 {
		delete(s.inflight, peer)
		if ch, ok := s.drained[peer]; ok {
			close(ch)
			delete(s.drained, peer)
		}
		return
	}
	s.inflight[peer] = count
}

// inflightDrainedChan returns a channel that closes when the
// in-flight counter for peer reaches zero. If the counter is already
// zero when this is called, returns a pre-closed channel so the
// caller never blocks. If a drain channel already exists for peer
// (a concurrent drain waiter), the same channel is returned so all
// waiters wake on the single close. nil receiver returns a closed
// channel.
//
// MUST be called AFTER the pending entry has been installed by
// BeginConversationDelete: otherwise a fresh send could acquire a
// new inflight slot AFTER the channel was returned, the close would
// fire too early (the count went 0 → 1 → 0 over the wait window),
// and the snapshot would observe a chatlog state inconsistent with
// the eventual peer-side wipe. With the barrier raised first, no
// new acquire can succeed and the counter only moves downward.
func (s *conversationDeleteRetryState) inflightDrainedChan(peer domain.PeerIdentity) <-chan struct{} {
	closed := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if s == nil {
		return closed()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] == 0 {
		return closed()
	}
	if ch, ok := s.drained[peer]; ok {
		return ch
	}
	ch := make(chan struct{})
	s.drained[peer] = ch
	return ch
}

// tryAdd atomically inserts the entry only when no pending wipe
// exists for the peer. Returns false when an in-flight entry is
// already present so SendConversationDelete can short-circuit a
// duplicate click without overwriting the live request. A
// has-then-add split (has under one lock, add under another)
// would be racy: two concurrent SendConversationDelete calls
// could both observe has==false and both add, with the second
// silently replacing the first and stranding the first ack as a
// requestID mismatch. tryAdd closes that window.
//
// SendConversationDelete uses tryAdd to RESERVE the slot BEFORE the
// local wipe runs, and drops it via removeReservedIfMatch once the
// wipe and its intent have landed. Reserving first is load-bearing
// for the outgoing barrier: SendMessage and SendFileAnnounce go
// through acquireSendIfNoPending, which checks the entries map and
// increments the inflight counter under the SAME mutex hold. Once
// tryAdd installs the entry, every subsequent acquireSendIfNoPending
// observes the barrier and returns false; an already-acquired send
// keeps its inflight slot until releaseSend runs, and
// CompleteConversationDelete waits for the counter to drop to 0 (via
// inflightDrainedChan) BEFORE it wipes. Without the atomic acquire a
// user-authored send could observe an empty barrier and land in
// chatlog just after the wipe read it, surviving a wipe the user
// believes erased the thread.
func (s *conversationDeleteRetryState) tryAdd(p *pendingConversationDelete) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[p.peer]; exists {
		return false
	}
	s.entries[p.peer] = p
	return true
}

// removeReservedIfMatch drops the reservation made by tryAdd when
// the click-time chatlog snapshot fails. The requestID guard
// prevents a late cleanup from clobbering a freshly-installed
// pending entry that may have been added by a concurrent
// SendConversationDelete after our reservation was already retired
// by some other path. Returns true when the reservation was
// removed; false when the slot was already gone or the requestID
// no longer matches.
func (s *conversationDeleteRetryState) removeReservedIfMatch(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.entries[peer]
	if !exists {
		return false
	}
	if p.requestID != requestID {
		return false
	}
	delete(s.entries, peer)
	return true
}

// claimForCompletion is the first step of CompleteConversationDelete:
// it confirms the reservation made by BeginConversationDelete is
// still alive and refreshes its TTL anchor (reservedAt = now) so
// pruneStaleReservations cannot race the snapshot. After a
// successful claim the snapshot has the full convDeleteReservationTTL
// window before the reaper would consider the entry stranded — far
// more headroom than the 10s snapshot timeout, so the snapshot
// completes (or fails cleanly) without colliding with the reaper.
//
// Returns false when the reservation is no longer present or its
// requestID no longer matches — for example, the reaper already
// dropped a reservation whose Begin → Complete goroutine startup
// gap exceeded the TTL, or some other control path retired the
// entry. The caller MUST surface this as
// ErrConversationDeleteReservationLost and skip the wire dispatch;
// without the typed signal the UI would advertise "dispatched"
// while no wire command actually went out.
func (s *conversationDeleteRetryState) claimForCompletion(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.entries[peer]
	if !exists {
		return false
	}
	if p.requestID != requestID {
		return false
	}
	p.reservedAt = now
	return true
}

// has reports whether a wipe for peer is currently in-flight. Used by
// SendConversationDelete to short-circuit a duplicate click without
// re-running the local wipe (which has nothing left to do anyway).
// has reports whether a wipe for peer is currently in-flight.
// nil-safe: a nil receiver is treated as "no wipes pending" so
// production call sites (SendMessage, SendFileAnnounce,
// IsConversationDeletePending) can be exercised by lightweight
// router fixtures that skip convDeleteRetry initialisation
// without panicking.
func (s *conversationDeleteRetryState) has(peer domain.PeerIdentity) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[peer]
	return ok
}

// IsConversationDeletePending exposes the pending-wipe flag to the
// UI so the composer can disable input + render a "wipe in progress"
// hint while a wipe is in progress for the given peer.
// Equivalent to checking errors.Is(err, ErrConversationDeleteInflight)
// after a SendMessage/SendFileAnnounce attempt, but lets the UI
// gate the input synchronously without spending a send call.
func (r *DMRouter) IsConversationDeletePending(peer domain.PeerIdentity) bool {
	if r == nil || r.convDeleteRetry == nil {
		return false
	}
	return r.convDeleteRetry.has(normalizePeer(peer))
}

// pruneStaleReservations drops every entry that is still UNPREPARED
// (no CompleteConversationDelete has attached the snapshot) and was
// installed more than ttl ago. Returns the dropped entries so the
// caller can log / publish abandonment for each. The retry loop
// invokes this every tick so a stranded reservation (e.g. if the
// calling code crashes between Begin and the goroutine that runs
// Complete) does not pin the outgoing barrier on the peer
// indefinitely.
func (s *conversationDeleteRetryState) pruneStaleReservations(now time.Time, ttl time.Duration) []pendingConversationDelete {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pruned []pendingConversationDelete
	for peer, p := range s.entries {
		if !p.reservedAt.IsZero() && now.Sub(p.reservedAt) <= ttl {
			continue
		}
		pruned = append(pruned, *p)
		delete(s.entries, peer)
	}
	return pruned
}

// ---------------------------------------------------------------------------
// Sender-side API
// ---------------------------------------------------------------------------

// BeginConversationDelete is the SYNCHRONOUS reservation step of the
// bulk wipe. It raises the outgoing barrier
// (IsConversationDeletePending = true; SendMessage and
// SendFileAnnounce return ErrConversationDeleteInflight for this
// peer) without doing any I/O, so callers can install the barrier
// BEFORE returning control to the UI event loop. The async work
// (chatlog snapshot + initial wire dispatch) runs in
// CompleteConversationDelete; UI callers typically run that step on
// a background goroutine so the snapshot's 10s timeout does not
// block the event loop, while the click→reservation transition
// stays atomic on the calling thread.
//
// Why split: dispatching the whole flow on a goroutine leaves a
// scheduling gap between "user confirmed wipe" and "tryAdd
// reserves the slot". A fast Enter / click during that gap can
// pass through SendMessage's barrier check and land in chatlog
// just after the wipe read it — surviving a wipe the user believes
// erased the thread. Pulling the reservation onto the synchronous
// path closes that window.
//
// Returns:
//
//   - (requestID, nil) on a fresh reservation. The caller MUST
//     either run CompleteConversationDelete with the same requestID
//     (which releases or completes the reservation as the snapshot
//     and dispatch resolve) or, if dropping the work, call
//     convDeleteRetry.removeReservedIfMatch to lift the barrier.
//   - ("", nil) on duplicate click — a wipe is already pending for
//     this peer. The existing in-flight request continues; the UI
//     short-circuits the duplicate without surfacing an error.
//   - ("", err) on a synchronous validation / mint failure. The
//     reservation was NOT installed.
//
// The minted id never leaves this process. It exists to bind the two
// halves of the local operation: Complete presents it to claim the same
// latch this call installed, so a goroutine that arrives late cannot
// finish a wipe a newer click has already replaced.
//
// Idempotency on duplicate clicks: tryAdd is atomic, so concurrent
// BeginConversationDelete calls cannot both install a fresh
// reservation. The second caller sees ("", nil) and the first
// request continues unmolested.
func (r *DMRouter) BeginConversationDelete(peer domain.PeerIdentity) (domain.ConversationDeleteRequestID, error) {
	if r.client == nil {
		return "", fmt.Errorf("DMRouter has no client")
	}
	peer = normalizePeer(peer)
	if peer.IsZero() {
		return "", fmt.Errorf("peer is required")
	}
	if r.client.chatlog.Store() == nil {
		return "", fmt.Errorf("chatlog store is not available")
	}

	// Cheap pending check before the atomic tryAdd below. has() is
	// best-effort — a wipe landing between this check and tryAdd
	// is still rejected safely by the latter. Without the cheap
	// pre-check we would still mint a fresh requestID we'd then
	// throw away on a duplicate click.
	if r.convDeleteRetry.has(peer) {
		log.Debug().
			Str("peer", peer.String()).
			Msg("dm_router: BeginConversationDelete: wipe already in-flight (cheap pre-check); ignoring duplicate request")
		return "", nil
	}

	// Mint a fresh request id so the eventual ack can be matched
	// to THIS wipe rather than to any stale earlier wipe for the
	// same peer. The id is stored in pending and travels with
	// every dispatch (initial and every retry) so genuine
	// duplicates of the same request still match on ack.
	rawID, err := protocol.NewMessageID()
	if err != nil {
		return "", fmt.Errorf("generate wipe request id: %w", err)
	}
	requestID := domain.ConversationDeleteRequestID(rawID)

	now := time.Now().UTC()

	// Atomic short-circuit on duplicate clicks: tryAdd installs
	// the pending entry only when no in-flight wipe exists for
	// this peer. A two-step has+add would race with a concurrent
	// BeginConversationDelete and let the second click silently
	// overwrite the first request — the first ack would then be
	// dropped as a requestID-mismatch and the local mirror would
	// never run for the wipe the peer actually applied. Returning
	// ("", nil) on the duplicate is the correct UX: the in-flight
	// request continues, the user sees the eventual outcome via
	// TopicConversationDeleteCompleted, and re-clicking is
	// harmless until the first round-trip terminates.
	added := r.convDeleteRetry.tryAdd(&pendingConversationDelete{
		peer:       peer,
		requestID:  requestID,
		reservedAt: now,
	})
	if !added {
		log.Debug().
			Str("peer", peer.String()).
			Msg("dm_router: BeginConversationDelete: wipe already in-flight for this peer; ignoring duplicate request")
		return "", nil
	}
	return requestID, nil
}

// CompleteConversationDelete runs the chatlog snapshot and the
// initial wire dispatch for a wipe previously reserved by
// BeginConversationDelete. The reservation MUST already exist with
// the supplied requestID — pass the value returned by
// BeginConversationDelete unchanged.
//
// Behaviour on each failure mode:
//
//   - Reservation already gone at claim time (TTL reaper dropped
//     the entry before this goroutine reached us, or some other
//     control path retired it): returns
//     ErrConversationDeleteReservationLost. UI must NOT render
//     "dispatched" — no wire command went out under this
//     requestID. The reaper has already published an Abandoned
//     outcome on TopicConversationDeleteCompleted so the UI
//     status string transitions through that subscriber.
//   - Snapshot read failure: the reservation is dropped (barrier
//     lifts, user can re-issue) and the error is returned. UI
//     should render a "wipe failed" status.
//   - Reservation gone between snapshot and attach (extremely
//     unlikely after a successful claim, since the TTL was just
//     refreshed): returns ErrConversationDeleteReservationLost
//     for the same reason as the claim case.
//   - Wipe failure: nothing ran at all — the wipe is one transaction —
//     so the reservation is dropped, the error is returned and the UI
//     renders "wipe failed". There is no partially erased state to
//     describe.
//
// Scope: every non-immutable row the thread holds at click time. Rows
// that arrive later are outside it on both sides — late deliveries from
// the OUTGOING side are blocked by the barrier this call runs under and
// by the drain below, and a message the peer sent before receiving the
// deletions but still in flight is the documented asymmetry in
// docs/dm-commands.md. The refusal recorded per id suppresses only the
// re-replay class: the same envelope arriving again after we deleted it.
//
// What each peer-side request may do is the peer's own answer, carried
// per message by their ack, exactly as for a single deletion.
// Context contract, in full, because two earlier readings of it were
// wrong in opposite directions:
//
//   - each of the three steps below (drain, transaction, compensation)
//     has its OWN budget. They are sequential, so one shared clock would
//     let a slow drain spend the transaction's;
//   - the caller's CANCELLATION applies to the first two: shutdown must
//     be able to abort a wipe in progress;
//   - the caller's DEADLINE, if it sets one, still caps the whole
//     operation — the budgets below are maxima, not guarantees, and a
//     ctx with a 10s deadline will cut a 30s transaction short. Pass a
//     deadline only when that is what you mean; the desktop caller
//     passes none, exactly so the steps keep their own clocks;
//   - the compensation is the one exception to both: it is detached,
//     because it exists to run after a step failed and would be useless
//     if it inherited the cancellation or the expiry that caused the
//     failure.
func (r *DMRouter) CompleteConversationDelete(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.client == nil {
		return fmt.Errorf("DMRouter has no client")
	}
	peer = normalizePeer(peer)
	if peer.IsZero() {
		return fmt.Errorf("peer is required")
	}
	if requestID == "" {
		return fmt.Errorf("requestID is required")
	}

	// Claim the reservation BEFORE the snapshot. claimForCompletion
	// confirms our entry is still in the map (TTL reaper has not
	// dropped it) and refreshes reservedAt to anchor the TTL on
	// this active completion attempt. After the claim the snapshot
	// has the full convDeleteReservationTTL window before the
	// reaper would prune the entry — comfortably more than the 10s
	// snapshot timeout, so the reaper cannot race us into the
	// "attach to a removed entry" hole described by
	// ErrConversationDeleteReservationLost.
	//
	// If the claim fails, the goroutine took longer to reach us
	// than the TTL allows (the reaper already dropped the entry
	// and published Abandoned). Returning the typed error lets the
	// UI suppress its "dispatched" status — without this, the UI
	// would advertise that a wire command went out while in fact
	// nothing was sent under this requestID.
	if !r.convDeleteRetry.claimForCompletion(peer, requestID, time.Now().UTC()) {
		log.Warn().
			Str("peer", peer.String()).
			Str("request_id", string(requestID)).
			Msg("dm_router: CompleteConversationDelete: reservation gone at claim (TTL reaper raced the goroutine startup); abandoning dispatch")
		return ErrConversationDeleteReservationLost
	}

	// Drain any sends that won the race against BeginConversationDelete:
	// SendMessage / SendFileAnnounce that observed an empty barrier and
	// already incremented the in-flight counter must finish — landing in
	// chatlog or failing — BEFORE the wipe reads it. Without the drain a
	// row committed a moment later would survive a wipe the user
	// believes erased the thread, which is the one outcome they cannot
	// see: the conversation looks empty and is not.
	//
	// The drain waits for LOCAL handoff only (the goroutine returns once
	// the node's send_message reply lands), not for peer receipt. A row
	// accepted locally but not yet delivered is wiped here like any
	// other, and its delivery is cancelled with the rest
	// (wipeConversationLocally) so the peer is not handed a message from
	// a thread the user erased.
	//
	// Bounded by its own timeout, on top of the caller's context: its
	// cancellation aborts this, and a deadline it may carry caps it. See
	// the contract on CompleteConversationDelete.
	drainCtx, cancelDrain := context.WithTimeout(ctx, conversationDrainBudget)
	drainCh := r.convDeleteRetry.inflightDrainedChan(peer)
	select {
	case <-drainCh:
		// All in-flight sends settled; safe to snapshot.
	case <-drainCtx.Done():
		cancelDrain()
		log.Warn().
			Str("peer", peer.String()).
			Str("request_id", string(requestID)).
			Err(drainCtx.Err()).
			Msg("dm_router: CompleteConversationDelete: inflight send drain timed out; dropping the reservation rather than wiping around an unfinished send")
		r.convDeleteRetry.removeReservedIfMatch(peer, requestID)
		return fmt.Errorf("conversation wipe: inflight send drain timeout: %w", drainCtx.Err())
	}
	cancelDrain()

	store := r.client.chatlog.Store()
	if store == nil {
		r.convDeleteRetry.removeReservedIfMatch(peer, requestID)
		return fmt.Errorf("conversation wipe: chatlog store is not available")
	}

	// Wipe the local thread NOW, with the outgoing barrier up and the
	// in-flight drain just observed. This is the whole point of the
	// action: a conversation the user asked to erase does not stay on
	// this disk waiting for a peer who may be offline for days.
	//
	// The rows, the per-message requests the peer now owes us and the
	// refusal of every id go in ONE transaction
	// (chatlog.DeleteConversationWithIntents), so there is no crash
	// window in which the thread is destroyed here and nobody will ever
	// ask the peer.
	//
	// Rows that arrive after this point are outside the wipe on both
	// sides — the documented asymmetry — because the deletion happens
	// while the barrier still holds.
	// Its own budget, so a slow drain cannot spend it: the transaction of
	// a long thread is tens of thousands of statements. A deadline on the
	// caller's context still caps it — see the contract above — which is
	// why the desktop caller sets none.
	wipeCtx, cancelWipe := context.WithTimeout(ctx, conversationWipeBudget)
	deleted, owed, localOK := r.wipeConversationLocally(wipeCtx, peer)
	cancelWipe()

	// The barrier comes down here. It exists to keep a send from racing
	// the local wipe, and the wipe is done; holding it until the peer
	// answers would leave the user unable to write to that conversation
	// for as long as the peer stays offline.
	r.convDeleteRetry.removeReservedIfMatch(peer, requestID)
	r.refreshPendingDeleteCounts()

	if !localOK {
		// The transaction rolled back: the thread is untouched and
		// nothing was recorded, so there is nothing outstanding and
		// nothing to retry on the user's behalf. Say so at once — the
		// conversation they asked to erase is still on this disk, and
		// clicking again is the only thing that changes that.
		log.Warn().
			Str("peer", peer.String()).
			Str("request_id", string(requestID)).
			Msg("dm_router: conversation wipe failed; nothing was changed on either side")
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:               peer,
			LocalCleanupFailed: true,
		})
		return fmt.Errorf("wipe the conversation with %s: the local thread is unchanged", peer)
	}

	log.Info().
		Str("peer", peer.String()).
		Str("request_id", string(requestID)).
		Int("deleted_local", deleted).
		Int("owed_by_peer", owed).
		Msg("dm_router: conversation wiped locally; the peer owes one deletion per message")

	// Nothing is dispatched from here. Every message of the thread is now
	// an ordinary delete intent, and the delete scheduler owns them: it
	// paces them per peer, parks them while the peer is away, wakes them
	// the moment the peer connects, and settles each one on its own ack.
	// A wipe is not a separate kind of request, so it does not need a
	// separate dispatcher, retry loop or acknowledgement.
	r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
		Peer:    peer,
		Deleted: deleted,
		Owed:    owed,
	})
	return nil
}

// SendConversationDelete runs the reservation step and the wipe in one
// call. UI code that wants the barrier up BEFORE returning to the event
// loop must NOT use it — that path calls BeginConversationDelete
// synchronously and runs CompleteConversationDelete on a goroutine. Test
// code that does not need that discipline uses this.
//
// Returns nil on the duplicate-click short-circuit (no reservation was
// installed, the wipe already under way continues).
func (r *DMRouter) SendConversationDelete(ctx context.Context, peer domain.PeerIdentity) error {
	requestID, err := r.BeginConversationDelete(peer)
	if err != nil {
		return err
	}
	if requestID == "" {
		return nil
	}
	return r.CompleteConversationDelete(ctx, peer, requestID)
}

// wipeConversationLocally erases the thread with the peer from this
// side: every non-immutable row, its per-message journal traces, any
// backing file-transfer state, and the deliveries this node still owed
// the peer for messages it is about to destroy. What the peer owes us in
// return is ONE DELETE INTENT PER MESSAGE — the same rows the scheduler
// already drives for a single deletion, because a conversation wipe is N
// message deletions and nothing else.
//
// It runs at CLICK time, under the outgoing barrier, not after any peer
// answers. Waiting for a peer who may be offline for days is exactly the
// exposure the wipe exists to end, and it is what made "Delete chat for
// everyone" unavailable offline.
//
// Immutable rows survive, as they do everywhere else. Authorship is NOT
// consulted for the LOCAL removal: the user is erasing their own view of
// a conversation, which is theirs to do for either side's messages. What
// each peer-side request may do is their own answer, carried by their ack
// per message.
//
// Returns how many rows went and ok==false when nothing happened at all
// (chatlog unavailable, the candidate read failed, or the transaction
// rolled back), so the caller can say so rather than report a wipe that
// did not run.
func (r *DMRouter) wipeConversationLocally(ctx context.Context, peer domain.PeerIdentity) (deleted int, owed int, ok bool) {
	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("peer", peer.String()).
			Msg("dm_router: wipeConversationLocally: chatlog store unavailable")
		return 0, 0, false
	}

	// One reading of the thread defines the wipe: these are the ids
	// marked against replays, the ids the transaction deletes, and the
	// ids the peer is asked about. A second read inside the transaction
	// would destroy rows this one never saw — rows nobody marked and
	// nobody will ask the peer for.
	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", peer.String()).
			Msg("dm_router: wipeConversationLocally: chatlog read failed; nothing wiped")
		return 0, 0, false
	}
	if len(scope.IDs) == 0 {
		return 0, 0, true
	}

	// Stop the node from sending anything in scope BEFORE reading what
	// the rows say about it. Without this the classification races the
	// delivery engine in both directions: a message can go out between
	// the read and the delete (so a row read as "never emitted" is
	// already at the peer, and no request is written for it), or its mark
	// can be cleared by an emission the transaction never saw.
	//
	// A freeze rather than the cancellation, because the cancellation
	// cannot be undone: if the transaction below then failed, the user
	// would be left with messages still on screen that nothing will ever
	// send. The freeze is ended either way — by the cancellation on
	// success, by a thaw on failure.
	frozen, err := r.client.FreezeConversationDelivery(ctx, peer, scope.IDs)
	classification := chatlog.ConversationWipeClassification{
		Trusted: err == nil,
		Proven:  frozen.NeverEmitted,
	}
	if err != nil {
		// Without the freeze nothing can be classified: a row's mark only
		// means something while nothing may emit the message behind the
		// transaction's back. Every message in scope becomes a request —
		// the peer is asked about ids they may not resolve, rather than a
		// message being deleted here while a copy escapes to them with
		// nothing left to recall it.
		log.Warn().Err(err).
			Str("peer", peer.String()).
			Msg("dm_router: wipeConversationLocally: could not stop deliveries; asking the peer about every message in scope")
	}

	// Volatile marks only: the durable ones ride the transaction below,
	// so a row and its refusal land together. These cover the window
	// before that commit, which no transaction can reach into.
	now := time.Now().UTC()
	expiry := r.wipeTombstones.Mark(scope.IDs, now)

	// One transaction for the rows, the requests the peer now owes us and
	// the refusal of every id. Either the conversation is gone AND
	// somebody is bound to ask the peer for each message, or nothing
	// happened and the user can click again — a half-applied wipe is the
	// one outcome they cannot see.
	//
	// The classification is part of it. Whether a message ever reached
	// the wire is written on the row the transaction is about to destroy
	// (chatlog's never_emitted mark), so it is read there, and a request
	// is written only for the messages the peer may actually hold. That
	// is what removes the parked-then-released dance and the grace it
	// needed: there is no window in which an unclassified request exists,
	// and no timeout that could expire into sending one.
	wiped, err := store.DeleteConversationWithIntents(ctx, peer, scope, classification, now, expiry)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", peer.String()).
			Msg("dm_router: wipeConversationLocally: transactional wipe failed; the thread is untouched")
		// The rows are still here, so the messages are still the user's:
		// the freeze has to end or they would sit unsent forever.
		//
		// DETACHED, unlike the two steps above: this is the compensation
		// for one of them failing, and the most likely reason to be here
		// is that the wipe's context was cancelled or timed out. Handing
		// it that same context would make it fail exactly when it is
		// needed. detachedCtx keeps the values and drops both the
		// deadline and the cancellation — see its own comment.
		thawCtx, cancelThaw := context.WithTimeout(r.detachedCtx(ctx), conversationCompensationBudget)
		thawErr := r.client.ThawConversationDelivery(thawCtx, peer, scope.IDs)
		cancelThaw()
		if thawErr != nil {
			log.Error().Err(thawErr).
				Str("peer", peer.String()).
				Msg("dm_router: wipeConversationLocally: the wipe failed AND its deliveries stayed frozen; they will resume after a restart")
		}
		// The thread is still here, so the ids we pre-marked name rows
		// that are alive. Leaving those marks would keep a day's worth
		// of "id of a message that exists" on disk — the metadata class
		// the wipe exists to remove, only about the wrong messages — and
		// would swallow a legitimate re-delivery of any of them.
		r.wipeTombstones.Forget(ctx, scope.IDs)
		return 0, 0, false
	}

	// Now stop what we still owed the peer, scoped to what the wipe took:
	// immutable rows survive it, and cancelling their delivery would
	// strand a message the user can still see in "sending" with nothing
	// left to send it.
	//
	// A failure here does not undo the wipe, for the same reason the
	// single-message withdraw does not stop for one: the outage is what
	// the user is deleting around.
	owed = wiped.Owed
	// The rows are gone, so the node is holding the payload of a deleted
	// conversation, kept off the wire only by the freeze. A failure here
	// does not thaw — that would hand the peer what the user just erased —
	// and it is not dropped either: it is owed, and the delete sweep
	// retries it until it succeeds.
	_ = r.withdrawDeletedDeliveries(ctx, peer, scope.IDs)

	// Under the file barrier: it moves the peer's history counter first, so
	// a registration in flight stands down instead of re-creating a mapping
	// this is about to remove. Taken even with no bridge, because the
	// version move is what every chatlog read in flight checks.
	r.withFileOps(peer, len(wiped.Removed) > 0, func() {
		if r.fileBridge == nil {
			return
		}
		for _, id := range wiped.Removed {
			r.fileBridge.OnMessageDeleted(id)
		}
	})

	r.evictWipedConversationFromUI(peer, wiped.Removed)
	if len(wiped.Removed) > 0 {
		r.checkpointAfterDelete(ctx, store)
	}
	return len(wiped.Removed), owed, true
}

// evictWipedConversationFromUI removes the given message IDs from the
// active-conversation cache (when peer is the active conversation),
// drops the same IDs from seenMessageIDs, and refreshes the sidebar
// preview / unread badge from chatlog. Called once at the end of the
// wipe so the deleted bubbles disappear immediately, without waiting
// for an unrelated redraw.
//
// removedIDs MUST be the list of IDs that were ACTUALLY removed from
// chatlog (not the list considered for removal). Rows that survived —
// immutable ones — must NOT be listed; they stay in chatlog and the
// active cache must keep rendering them, otherwise the user sees a
// blank chat thread that would resurrect on the next conversation
// reload. A blanket cache.Load(peer, nil) is wrong for the same
// reason.
//
// We deliberately do NOT remove the peer entry from r.peers — the
// identity stays in the sidebar with whatever preview / unread the
// post-sweep chatlog state implies (the helper
// refreshPreviewAfterDelete computes both). The peer-removal path
// is a separate user action via RemovePeer / "Delete identity".
func (r *DMRouter) evictWipedConversationFromUI(peer domain.PeerIdentity, removedIDs []domain.MessageID) {
	if r.cache == nil {
		return
	}
	if len(removedIDs) == 0 {
		// Nothing was removed (immutable-only conversation, or the
		// sweep found zero rows). The sidebar preview can still
		// have shifted (e.g. unread badge transitions during a
		// concurrent receive), so refresh that, but skip the cache
		// touch and the messages-updated notification.
		r.refreshPreviewAfterDelete(peer)
		r.notify(UIEventSidebarUpdated)
		return
	}

	r.mu.Lock()
	cacheChanged := false
	if r.cache.MatchesPeer(peer) {
		for _, id := range removedIDs {
			if r.cache.RemoveMessage(string(id)) {
				cacheChanged = true
			}
		}
		if cacheChanged {
			r.activeMessages = r.cache.Messages()
		}
	}
	// Drop the matching seenMessageIDs entries so a future
	// re-delivery of one of these IDs (e.g. peer resends after we
	// re-add a contact) is not silently ignored. seenMessageIDs is
	// keyed by message ID alone — only the IDs we just removed need
	// to be cleared; entries for surviving rows must stay so the
	// dedup gate keeps working for them.
	for _, id := range removedIDs {
		delete(r.seenMessageIDs, string(id))
	}
	// A deleted message is not an unread message. The set is authoritative
	// for the badge, and the ids are right here — no query needed. The
	// history move was recorded by the file barrier when the rows went;
	// one deletion is one move.
	r.dropUnreadLocked(peer, removedIDs...)
	r.mu.Unlock()

	r.refreshPreviewAfterDelete(peer)

	if cacheChanged {
		r.notify(UIEventMessagesUpdated)
	}
	r.notify(UIEventSidebarUpdated)
}

// The three budgets of one wipe. They are SEQUENTIAL and each is bounded
// on its own so that a slow drain cannot spend the transaction's time.
// They are maxima: a caller that sets a deadline caps all of them, which
// is why the desktop caller does not. See the contract on
// CompleteConversationDelete.
const (
	conversationDrainBudget = 10 * time.Second
	// The transaction is tens of thousands of statements on a long thread
	// (per message: the emission mark read, the row, three journal rows,
	// the request upsert, the refusal), and it runs on phones.
	conversationWipeBudget = 30 * time.Second
	// Compensation is a single RPC and must not inherit anybody's
	// exhausted clock.
	conversationCompensationBudget = 5 * time.Second
)

// wipeTombstoneTTL bounds how long a deleted id is refused before a
// re-delivery of it is allowed to re-create the row.
//
// It has to outlast the window in which that re-delivery can happen, and
// that window belongs to the SENDER: their node keeps retrying a message
// until its delivery receipt arrives — 20 attempts on a schedule that
// caps at 11 minutes, about 3.5 hours by default, and configurable
// higher (CORSA_DELIVERY_RETRY_MAX_ATTEMPTS). A refusal shorter than
// that expires while copies of the message are still being sent, which
// is the resurrection it exists to prevent.
//
// The bound is not the retry budget but the SENDER'S RESEED HORIZON. A
// restart resets their attempt counter, and their outbox re-injects
// anything undelivered from the last week, so a message can be sent again
// long after its original 3.5-hour budget was spent. Past that horizon
// nobody re-sends it, which makes the horizon — not the budget — the
// point at which a refusal has nothing left to refuse.
//
// A little over the week, so the two do not expire in the same instant.
// The cost is one small row per deleted message for that long, which the
// reaper removes as soon as it owes nothing and refuses nothing.
const wipeTombstoneTTL = 8 * 24 * time.Hour

// wipeTombstoneReapPeriod is how often the reaper goroutine prunes
// stale entries. Independent of TTL — must be small enough that the
// peak set size stays bounded under heavy wipe activity.
const wipeTombstoneReapPeriod = 5 * time.Minute

// wipeTombstoneSet records ids of rows the wipe path just removed,
// with a TTL eviction model. The handler for new inbound messages
// (DMRouter.onNewMessage) consults this set on every event so a
// late-replayed envelope cannot resurrect a wiped row by silently
// re-inserting it through storeIncomingMessage.
type wipeTombstoneSet struct {
	mu      sync.Mutex
	entries map[domain.MessageID]time.Time // id → expiresAt
	// journal resolves the durable half. The replay window and a
	// restart overlap: the process that wiped the thread can be gone by
	// the time the echo lands, and a tombstone lost with it resurrects
	// exactly the rows the user destroyed.
	journal func() wipeTombstoneJournal
	// unloaded is set when the startup load FAILED. The memory half is
	// then not a complete answer, so Has consults the database instead
	// of reporting "not refused" for everything the load never saw.
	unloaded bool
	// nextReload throttles that fallback. Has runs on the INBOUND path —
	// the node calls StoreMessage synchronously for every arriving
	// message — and a database that is wedged rather than merely late
	// answers each retry only after busy_timeout. Without a floor between
	// attempts every message pays that timeout, and a slow disk becomes a
	// stalled receive path. Retrying at a bounded rate keeps the recovery
	// (the reaper retries on its own tick regardless) without putting the
	// disk's health on the critical path of every message.
	nextReload time.Time
}

// wipeTombstoneReloadFloor is the least time between two fallback loads
// from the inbound path. Short enough that a database that recovers is
// picked up long before the reaper's own tick, long enough that a wedged
// one is asked once per interval instead of once per message.
const wipeTombstoneReloadFloor = 5 * time.Second

// wipeTombstoneJournal is the durable half of the tombstone set.
type wipeTombstoneJournal interface {
	NoteWipeTombstones(ctx context.Context, ids []domain.MessageID, expiresAt time.Time) error
	DropWipeTombstones(ctx context.Context, ids []domain.MessageID) error
	LiveWipeTombstones(ctx context.Context, now time.Time) (map[domain.MessageID]time.Time, error)
	ReapWipeTombstones(ctx context.Context, now time.Time) (int64, error)
}

// newWipeTombstoneSet builds the set over a journal RESOLVER, for the
// reason given on newInboundConversationDeleteSeenSet: the store is opened
// after the router is built.
func newWipeTombstoneSet(journal func() wipeTombstoneJournal) *wipeTombstoneSet {
	return &wipeTombstoneSet{
		entries: make(map[domain.MessageID]time.Time),
		journal: journal,
	}
}

func (s *wipeTombstoneSet) durableJournal() wipeTombstoneJournal {
	if s == nil || s.journal == nil {
		return nil
	}
	return s.journal()
}

// Hydrate loads the tombstones that outlived the last process. Called
// once at startup, before any inbound message is handled: Has stays a
// pure memory lookup afterwards, because putting a query on the arrival
// path of every message to catch a rare replay is the wrong trade.
func (s *wipeTombstoneSet) Hydrate(ctx context.Context, now time.Time) {
	journal := s.durableJournal()
	if journal == nil {
		return
	}
	live, err := journal.LiveWipeTombstones(ctx, now)
	if err != nil {
		// Leaves the set UNLOADED, not merely empty: an empty set that
		// claims to be loaded would let every replay through until the
		// next restart. Has retries the load while this is set — at a
		// bounded rate, see nextReload — and the reaper retries it on
		// its own tick regardless, which is what makes the throttle safe
		// to apply to the inbound path.
		s.mu.Lock()
		s.unloaded = true
		s.mu.Unlock()
		log.Warn().Err(err).
			Msg("dm_router: loading refusals failed; falling back to a per-message lookup until the next attempt")
		return
	}
	s.mu.Lock()
	for id, expiry := range live {
		if current, ok := s.entries[id]; !ok || expiry.After(current) {
			s.entries[id] = expiry
		}
	}
	s.unloaded = false
	s.mu.Unlock()
	if len(live) > 0 {
		log.Info().Int("refusals", len(live)).Msg("dm_router: refusals restored")
	}
}

// Forget drops the given ids from both halves of the set. Called when a
// wipe that pre-marked them rolled back: the rows are alive after all, and
// a mark for a live row is both a record of a message that still exists
// and a trap that would swallow its next legitimate re-delivery.
func (s *wipeTombstoneSet) Forget(ctx context.Context, ids []domain.MessageID) {
	if s == nil || len(ids) == 0 {
		return
	}
	s.mu.Lock()
	for _, id := range ids {
		delete(s.entries, id)
	}
	s.mu.Unlock()

	journal := s.durableJournal()
	if journal == nil {
		return
	}
	if err := journal.DropWipeTombstones(ctx, ids); err != nil {
		log.Warn().Err(err).
			Int("ids", len(ids)).
			Msg("dm_router: dropping the tombstones of a rolled-back wipe failed; they expire on their own TTL")
	}
}

// Mark inserts every id into the VOLATILE half only, and returns the
// expiry it used. For a deletion that runs in a transaction: the durable
// half rides that commit, so the row and its refusal land together, while
// this covers the window before it — the moment between deciding to delete
// an id and committing, in which a replay can still arrive.
func (s *wipeTombstoneSet) Mark(ids []domain.MessageID, now time.Time) time.Time {
	expiry := now.Add(wipeTombstoneTTL)
	if s == nil || len(ids) == 0 {
		return expiry
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.entries[id] = expiry
	}
	return expiry
}

// Note inserts every id with an expiry of now+TTL, in both halves. For
// deletions with no transaction of their own to ride. Called after a
// successful wipe (the sender's click-time wipe AND receiver-side sweep)
// so both sides are protected against late re-delivery. A nil
// receiver is a no-op so test fixtures that do not need tombstone
// behaviour can leave the field unset without nil-panicking
// production call sites.
func (s *wipeTombstoneSet) Note(ctx context.Context, ids []domain.MessageID, now time.Time) {
	if s == nil || len(ids) == 0 {
		return
	}
	expiry := s.Mark(ids, now)

	journal := s.durableJournal()
	if journal == nil {
		return
	}
	if err := journal.NoteWipeTombstones(ctx, ids, expiry); err != nil {
		// This process still refuses the replay; what is lost is the
		// refusal surviving a restart inside the replay window.
		log.Warn().Err(err).
			Int("ids", len(ids)).
			Msg("dm_router: persisting wipe tombstones failed; a replay after a restart could resurrect the rows")
	}
}

// Refuses reports whether id is currently tombstoned (present and not
// expired), and whether it could answer at all.
//
// known=false is not "no". It means the durable half has not been read and
// a memory miss therefore proves nothing — the caller must treat the
// message as undecidable rather than as allowed. A nil receiver is a
// deployment without the feature and answers "not refused, known".
func (s *wipeTombstoneSet) Refuses(id domain.MessageID, now time.Time) (refused, known bool) {
	if s == nil {
		return false, true
	}
	// Memory ALWAYS first, whatever the load did. It holds the marks a
	// deletion in progress has just made — the ones covering the window
	// before its transaction commits — and those exist nowhere else yet.
	if s.hasInMemory(id, now) {
		return true, true
	}
	if !s.isUnloaded() {
		return false, true
	}

	// The startup load failed, so a memory miss proves nothing. Retry the
	// load rather than querying for this one id: it answers the same
	// question, costs the same read, and — unlike a lookup — ends the
	// fallback for every message after this one. But not on every
	// message: see nextReload. A caller inside the throttle window is
	// told "unknown" rather than "not refused" — the throttle bounds what
	// the inbound path PAYS, and must not turn into permission to
	// re-create a row the user deleted.
	if !s.claimReload(now) {
		return false, false
	}
	s.Hydrate(context.Background(), now)
	if s.isUnloaded() {
		return false, false
	}
	return s.hasInMemory(id, now), true
}

func (s *wipeTombstoneSet) isUnloaded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unloaded
}

// claimReload reports whether this caller should pay for a fallback load,
// and books the next slot if so.
func (s *wipeTombstoneSet) claimReload(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.unloaded || now.Before(s.nextReload) {
		return false
	}
	s.nextReload = now.Add(wipeTombstoneReloadFloor)
	return true
}

func (s *wipeTombstoneSet) hasInMemory(id domain.MessageID, now time.Time) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expiry, ok := s.entries[id]
	if !ok {
		return false
	}
	if !expiry.After(now) {
		// Expired — drop opportunistically and report miss.
		delete(s.entries, id)
		return false
	}
	return true
}

// reloadIfUnloaded retries a startup load that failed. A no-op once the
// set is loaded, which is the normal case.
func (s *wipeTombstoneSet) reloadIfUnloaded(ctx context.Context, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	unloaded := s.unloaded
	s.mu.Unlock()
	if unloaded {
		s.Hydrate(ctx, now)
	}
}

// reap drops every entry whose expiry has passed, in memory and on disk.
func (s *wipeTombstoneSet) reap(ctx context.Context, now time.Time) {
	s.mu.Lock()
	for id, expiry := range s.entries {
		if !expiry.After(now) {
			delete(s.entries, id)
		}
	}
	s.mu.Unlock()

	journal := s.durableJournal()
	if journal == nil {
		return
	}
	if _, err := journal.ReapWipeTombstones(ctx, now); err != nil {
		log.Warn().Err(err).
			Msg("dm_router: reaping wipe tombstones failed; retrying on the next tick")
	}
}

// suppressIfWipeTombstoned is the inbound-event guard used by
// onNewMessage. When the freshly-stored message id matches a
// recently-wiped tombstone, the row is removed again from chatlog,
// the active-conversation cache evicted, and the function returns
// true so the caller skips the rest of the new-message UI path
// (notifications, beep, sidebar nudge — none of those should fire
// for a row the user has already deleted).
//
// Returns false ONLY in two cases:
//
//   - The id is not tombstoned at all — the regular new-message
//     flow handles it.
//   - The tombstone is present but the chatlog re-DELETE returned
//     a real error (not "row already gone"). In that case the row
//     stays on disk; suppressing the UI path on top of that would
//     silently hide the row from the active conversation while it
//     sits in chatlog and reappears on the next reload, so we let
//     the regular new-message flow surface it. The warn log
//     records the underlying chatlog error so the user has at
//     least visibility to manually delete it.
//
// A tombstoned id whose row was already gone (DeleteByID returned
// removed==false — concurrent cleanup, double-fire) STILL returns
// true: the inbound event carries the encrypted body and would
// otherwise be decrypted and surfaced in the active chat or
// sidebar even though chatlog no longer has the row. The
// suppression path stays the same in that case (fileBridge and
// cache eviction are idempotent, seenMessageIDs pinning prevents
// future re-entries).
func (r *DMRouter) suppressIfWipeTombstoned(event protocol.LocalChangeEvent) bool {
	if r.wipeTombstones == nil || event.MessageID == "" {
		return false
	}
	id := domain.MessageID(event.MessageID)
	// An undecidable answer does not suppress: the storage gate refused
	// the insert in that case, so there is no row here to hide.
	if refused, _ := r.wipeTombstones.Refuses(id, time.Now().UTC()); !refused {
		return false
	}

	// Resurrection attempt — the envelope was re-delivered after
	// the wipe completed. storeIncomingMessage already inserted
	// the row by the time this event fires; try to remove it
	// again and only suppress the UI path if the re-DELETE
	// actually succeeded.
	store := r.client.chatlog.Store()
	if store == nil {
		return false
	}
	removed, err := store.DeleteByID(r.opContext(), id)
	if err != nil {
		log.Warn().Err(err).
			Str("message_id", event.MessageID).
			Msg("dm_router: tombstone re-DELETE failed; falling back to regular new-message UI so the row is at least visible")
		return false
	}
	// removed==false means DeleteByID idempotently reported the row
	// was already gone (concurrent cleanup, double-fire of the same
	// envelope). The TOMBSTONE still says we wiped this id, and the
	// inbound event carries the encrypted body — without
	// suppression the active-conversation cache or sidebar code
	// would happily decrypt and surface a fresh bubble for a row
	// the user has already deleted. Suppression still runs:
	// fileBridge cleanup is idempotent, evictWipedConversationFromUI
	// is a no-op when the cache lacks the id, and pinning
	// seenMessageIDs prevents future re-deliveries from re-entering
	// the new-message path. Only a real DeleteByID error (above)
	// falls through, since in that case the row is genuinely on
	// disk and the user needs the manual-delete UI.

	wipedPeer := r.peerForMessage(event)
	r.withFileOps(wipedPeer, removed, func() {
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(id)
		}
	})
	r.evictWipedConversationFromUI(wipedPeer, []domain.MessageID{id})

	// Nothing to credit anywhere: a wipe is N ordinary deletions, each
	// settled by its own ack, so a re-delete here is just this guard
	// doing its job.
	_ = removed

	// Pin this id in seenMessageIDs so any subsequent
	// TopicMessageNew event for the same id (a second re-delivery
	// path, the repair-path fallback, a second relay retry) is
	// dropped at the dedup gate before reaching this suppression
	// point again — we already paid the chatlog DELETE cost
	// once, no reason to repeat it. This does NOT cover
	// node-level paths that bypass onNewMessage entirely
	// (fetch_dm_headers, gossip surfacing the envelope through
	// node.s.topics["dm"]); pushing the tombstone gate down into
	// node admission is a tracked follow-up.
	r.mu.Lock()
	r.seenMessageIDs[event.MessageID] = struct{}{}
	r.mu.Unlock()

	log.Debug().
		Str("message_id", event.MessageID).
		Msg("dm_router: suppressed re-delivery of wiped message")
	return true
}

// publishConversationDeleteOutcome forwards the terminal outcome
// onto the ebus so UI / RPC subscribers can differentiate "wipe
// confirmed" (applied) from "transport abandoned" (Abandoned=true).
// Safe when the bus is nil — the publish step is skipped silently.
func (r *DMRouter) publishConversationDeleteOutcome(outcome ebus.ConversationDeleteOutcome) {
	if r.eventBus == nil {
		return
	}
	r.eventBus.Publish(ebus.TopicConversationDeleteCompleted, outcome)
}

// ---------------------------------------------------------------------------
// Retry loop
// ---------------------------------------------------------------------------

// wipeTombstoneReaperLoop runs in a dedicated goroutine launched from
// Start(). It restores the refusals the previous process left behind, then
// prunes expired ones so a long-running install stays bounded.
func (r *DMRouter) wipeTombstoneReaperLoop(ctx context.Context) {
	defer recoverLog("wipeTombstoneReaperLoop")
	if r.wipeTombstones == nil {
		return
	}
	ticker := time.NewTicker(wipeTombstoneReapPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// Retry a load that failed at startup before reaping: while
			// it is outstanding every arrival costs a database lookup,
			// and the reaper is the only thing that comes back around.
			r.wipeTombstones.reloadIfUnloaded(ctx, now.UTC())
			r.wipeTombstones.reap(ctx, now.UTC())
		}
	}
}
