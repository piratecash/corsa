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
// decides. That wait is what used to make "Delete chat for both sides"
// unavailable offline at all. The failure the old ordering protected
// against — the network half never completing — is now expressed
// instead as a request the scheduler keeps carrying and, at the very
// end of its TTL, an Abandoned outcome that says the PEER's copy may
// remain. Nothing in that path can leave the user's own history
// standing against their instruction.
//
// Authorization deliberately diverges from message_delete, and the wipe has
// a command of its own (dm_router_conversation_delete_wire.go) because it
// must:
//
//   - message_delete (single row) runs authorizedToDelete per row, so the
//     message's own flag decides whether the requesting peer may touch it.
//     An author-only flag reserves it to whoever wrote it.
//   - conversation_delete (whole thread) erases every non-immutable row
//     regardless of authorship. Per-row authorization here refuses exactly
//     the rows the requester WROTE, so half the thread survives on each
//     side after a "clear everything" gesture — the user's screen empties
//     while their own messages stand on the other side. That is not a
//     stricter rule but a broken one, and it is what shipped for one
//     release. The gesture is a mutual forgetting, confirmed twice in the
//     UI before it is sent; immutable rows are the only carve-out and stay
//     on both sides.
//
// Design contract: docs/dm-commands.md §"Clearing a chat".

// The reservation is all this file schedules on its own: the peer's half is
// ONE conversation request, which the same sweep drives under the same
// pacing, parking and give-up budget as any single deletion.
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
			Str("peer", logID(peer.String())).
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
			Str("peer", logID(peer.String())).
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
			Str("peer", logID(peer.String())).
			Str("request_id", logID(string(requestID))).
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
			Str("peer", logID(peer.String())).
			Str("request_id", logID(string(requestID))).
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
	//
	// The REMOVAL gate goes up for the wipe itself. convDeleteRetry's barrier
	// stops this node's own sends; it says nothing to the paths that write the
	// conversation from the side — the reaction re-offer reads a page of facts
	// and queues it as a copy, and a wipe landing between those two steps
	// deletes rows that are already on their way out again. begin() waits for
	// the leases already handed out, so a re-offer mid-flight finishes before
	// the transaction, and refuses new ones until the queue has been dropped
	// too. Incoming messages are DEFERRED for that window, not lost: the sender
	// re-delivers, and a message that arrives after the wipe is outside it on
	// both sides either way.
	releaseRemoval := r.removals.begin(peer)
	// The gate stops writes and re-offers; it does not stop the send queue,
	// which by then may already hold RESOLVED facts of this thread. Without the
	// pause a pass can read them before the transaction, clear the frame gate
	// during it, and hand the frame over after the commit — and the forgetting
	// below would then only wait for a frame that has already gone. So the
	// outbox is stopped for the length of the wipe, and released once there is
	// nothing left in it to send.
	resumeReactions := r.client.HoldReactionSends(peer)
	wipeCtx, cancelWipe := context.WithTimeout(ctx, conversationWipeBudget)
	deleted, localOK := r.wipeConversationLocally(wipeCtx, peer, requestID)
	cancelWipe()
	if localOK {
		// Inside the gate: see ForgetConversationState. Only after a wipe that
		// actually happened — a rolled-back transaction leaves the thread
		// untouched, and its queue with it.
		r.client.ForgetConversationState(peer)
	}
	resumeReactions()
	releaseRemoval()

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
			Str("peer", logID(peer.String())).
			Str("request_id", logID(string(requestID))).
			Msg("dm_router: conversation wipe failed; nothing was changed on either side")
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:               peer,
			LocalCleanupFailed: true,
		})
		return fmt.Errorf("wipe the conversation with %s: the local thread is unchanged", peer)
	}

	deletionLog().Info().
		Str("peer", logID(peer.String())).
		Str("request_id", logID(string(requestID))).
		Int("deleted_local", deleted).
		Msg("dm_router: conversation wiped locally; the peer owes one wipe of their side")

	// Nothing is dispatched from here. The request the transaction just
	// wrote is an ordinary row of the delete scheduler, which owns it
	// exactly as it owns a single deletion: it paces it per peer, parks it
	// while the peer is away, wakes it the moment they connect, and settles
	// it on their ack. The wipe needs no dispatcher, retry loop or
	// acknowledgement of its own — only its own COMMAND, because what it
	// asks for cannot be said in message ids.
	r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
		Peer:      peer,
		Deleted:   deleted,
		Requested: true,
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

// wipeConversationLocally erases the thread with the peer from this side:
// every non-immutable row, its per-message journal traces, any backing
// file-transfer state, and the deliveries this node still owed the peer for
// messages it is about to destroy. What the peer owes us in return is ONE
// REQUEST — that they clear their side of the conversation — written in the
// same transaction as the deletions.
//
// It runs at CLICK time, under the outgoing barrier, not after any peer
// answers. Waiting for a peer who may be offline for days is exactly the
// exposure the wipe exists to end, and it is what made "Delete chat for
// everyone" unavailable offline.
//
// Immutable rows survive, here and there. Authorship is consulted on NEITHER
// side: the user is erasing a conversation, which is not a pile of individual
// messages with individual owners but the thing the two of them made together,
// and asking per message is what used to leave each side holding the half the
// other wrote.
//
// An EMPTY thread is still wiped, and that is the repair path rather than a
// no-op: a conversation an older build erased here while the peer refused the
// user's own messages has nothing left to name, and the request carries no ids
// precisely so it can still be made.
//
// Returns how many rows went and ok==false when nothing happened at all
// (chatlog unavailable, the candidate read failed, or the transaction rolled
// back), so the caller can say so rather than report a wipe that did not run.
func (r *DMRouter) wipeConversationLocally(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) (deleted int, ok bool) {
	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("peer", logID(peer.String())).
			Msg("dm_router: wipeConversationLocally: chatlog store unavailable")
		return 0, false
	}

	// One reading of the thread defines the wipe: these are the ids marked
	// against replays and the ids the transaction deletes. A second read
	// inside the transaction would destroy rows this one never saw — rows
	// nobody marked and nobody refuses afterwards.
	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", logID(peer.String())).
			Msg("dm_router: wipeConversationLocally: chatlog read failed; nothing wiped")
		return 0, false
	}

	// Stop the node from sending anything in scope BEFORE the transaction
	// takes it. A message that goes out between the read and the delete is one
	// the user destroyed here and handed over anyway.
	//
	// A freeze rather than the cancellation, because the cancellation cannot
	// be undone: if the transaction below then failed, the user would be left
	// with messages still on screen that nothing will ever send. The freeze is
	// ended either way — by the cancellation on success, by a thaw on failure.
	if len(scope.IDs) > 0 {
		if _, err := r.client.FreezeConversationDelivery(ctx, peer, scope.IDs); err != nil {
			// The wipe STOPS. A message still queued would go out after the
			// erasure, and — because the request carries no ids — it would land
			// on the peer either before their wipe (deleted, fine) or AFTER it,
			// where nothing can name it: their request is already answered,
			// their refusals cover the ids they erased, and this one is not
			// among them. It would sit in a conversation both users believe is
			// gone, permanently.
			//
			// Nothing has been destroyed at this point, so refusing costs the
			// user a retry and no history.
			log.Warn().Err(err).
				Msg("dm_router: wipeConversationLocally: could not stop deliveries; refusing to erase what we might still send")
			return 0, false
		}
	}

	// Volatile marks only: the durable ones ride the transaction below, so a
	// row and its refusal land together. These cover the window before that
	// commit, which no transaction can reach into.
	now := r.now()

	// Refused BEFORE the transaction opens, so a copy of one of these messages
	// arriving while it runs is turned away rather than stored behind the wipe.
	// They expire on their own at the sender's reseed horizon. Not lifted when
	// the peer acknowledges the wipe: an ack is their database, not the relay
	// buffers and inbox queues that may still hold copies of these envelopes.
	r.wipeTombstones.Note(scope.IDs, now)

	// One transaction for the rows and the request the peer now owes us. Either
	// the conversation is gone AND somebody is bound to ask the peer, or
	// nothing happened and the user can click again — a half-applied wipe is
	// the one outcome they cannot see.
	wiped, err := store.DeleteConversationWithIntent(ctx, peer, scope, chatlog.DeleteIntent{
		Kind:      chatlog.DeleteIntentConversation,
		Peer:      peer,
		RequestID: requestID,
		// When the user asked. Diagnostics only — the request carries no
		// moment on the wire and the peer compares nothing against it.
		CreatedAt:     now,
		NextAttemptAt: now,
	})
	if err != nil {
		log.Warn().Err(err).
			Str("peer", logID(peer.String())).
			Msg("dm_router: wipeConversationLocally: transactional wipe failed; the thread is untouched")
		// The rows are still here, so the messages are still the user's: the
		// freeze has to end or they would sit unsent forever.
		//
		// DETACHED, unlike the steps above: this is the compensation for one
		// of them failing, and the most likely reason to be here is that the
		// wipe's context was cancelled or timed out. Handing it that same
		// context would make it fail exactly when it is needed. detachedCtx
		// keeps the values and drops both the deadline and the cancellation.
		if len(scope.IDs) > 0 {
			thawCtx, cancelThaw := context.WithTimeout(r.detachedCtx(ctx), conversationCompensationBudget)
			thawErr := r.client.ThawConversationDelivery(thawCtx, peer, scope.IDs)
			cancelThaw()
			if thawErr != nil {
				log.Error().Err(thawErr).
					Str("peer", logID(peer.String())).
					Msg("dm_router: wipeConversationLocally: the wipe failed AND its deliveries stayed frozen; they will resume after a restart")
			}
		}
		// The thread is still here, so the ids refused above name rows that are
		// alive, and a refusal for a live row would swallow its next legitimate
		// re-delivery.
		r.wipeTombstones.Forget(scope.IDs)
		return 0, false
	}

	// Now stop what we still owed the peer, scoped to what the wipe took:
	// immutable rows survive it, and cancelling their delivery would strand a
	// message the user can still see in "sending" with nothing left to send it.
	//
	// The rows are gone, so the node is holding the payload of a deleted
	// conversation, kept off the wire only by the freeze. A failure here does
	// not thaw — that would hand the peer what the user just erased — and it
	// is not dropped either: it is owed, and the delete sweep retries it until
	// it succeeds.
	if len(scope.IDs) > 0 {
		_ = r.withdrawDeletedDeliveries(ctx, peer, scope.IDs)
	}

	// Under the file barrier: it moves the peer's history counter first, so a
	// registration in flight stands down instead of re-creating a mapping this
	// is about to remove. Taken even with no bridge, because the version move
	// is what every chatlog read in flight checks.
	r.withFileOps(peer, len(wiped.Removed) > 0, func() {
		if r.fileBridge == nil {
			return
		}
		for _, id := range wiped.Removed {
			r.fileBridge.OnMessageDeleted(id)
		}
	})

	r.evictWipedConversationFromUI(peer, wiped.Removed)
	// The chips are drawn from a per-conversation cache the window holds, and
	// the ONLY thing that reloads it is this event. Evicting the messages is
	// not enough: a message that survived the wipe — an immutable one — is
	// still on screen with whatever chips were cached for it, and the facts of
	// the erased messages stay in the window's memory until the user leaves
	// the chat.
	r.publishReactionsChanged(peer)
	// What the transaction took leaves the write-ahead log now rather than at
	// the next automatic checkpoint: a truncation per THREAD, which is what
	// makes it affordable where a truncation per row was not.
	//
	// Unconditional, including the repair click on an already-empty thread.
	// "Nothing was removed" is a statement about MESSAGES, and the same
	// transaction also clears the conversation's orphaned reactions — rows that
	// name the message each fact was for. Deciding by the message count left
	// exactly those ids legible in the log.
	r.checkpointAfterDelete(ctx, store)
	return len(wiped.Removed), true
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
	// Bracketed like every other per-message delete: the row exists for as long
	// as this takes, and a reaction frame built from it a moment ago must not
	// leave after it is gone.
	//
	// The conversation is peerForMessage, not the event's sender: this event
	// fires for messages in BOTH directions, and on one this node sent, the
	// sender is this node — pausing that would leave the real recipient
	// unguarded, which is the only one a frame could go to.
	resumeReactions := r.client.HoldReactionSends(r.peerForMessage(event))
	removed, err := store.DeleteByID(r.opContext(), id)
	resumeReactions()
	if err != nil {
		log.Warn().Err(err).
			Msg("dm_router: tombstone re-DELETE failed; falling back to regular new-message UI so the row is at least visible")
		return false
	}
	if removed {
		// A wiped message that was re-delivered and inserted before the
		// refusal caught it. Its body reached the log on the way in and
		// again on the way out; nothing else on this path retires those
		// pages.
		r.checkpointSoonAfterDelete()
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

	// Behind the diagnostics gate: "this id was refused because it had been
	// deleted here" is a statement that the user deleted that message, with the
	// time attached, in a file nothing ever truncates.
	deletionLog().Debug().
		Str("message_id", logID(event.MessageID)).
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
//
// It also sweeps reactions that waited for a message that never arrived. The
// two share a loop rather than each having one because they are the same job on
// the same schedule — bounding a set a REMOTE peer can grow — and a second
// ticker of the same period would only be a second thing to keep in step.
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
			// Re-read the deletions still owed before reaping. It both
			// retries a load that failed — while one is outstanding every
			// arrival costs a database lookup, and the reaper is the only
			// thing that comes back around — and RETIRES the refusals whose
			// deletions the peers have since confirmed, which is the only
			// way that set ever shrinks.
			r.wipeTombstones.Hydrate(ctx, now.UTC())
			r.wipeTombstones.reap(now.UTC())
			// RELEASE BEFORE SWEEP, and the order is load-bearing. Both look at
			// pending rows; the sweep destroys the ones past their TTL. If a
			// release failed and stayed failed for longer than the TTL, sweeping
			// first would destroy a fact whose message is sitting right there —
			// and the release would then have nothing left to find.
			r.releaseArrivedReactions(ctx, now.UTC())
			r.sweepHeldReactions(ctx, now.UTC())
			r.reofferReactions(ctx)
		}
	}
}

// sweepHeldReactions drops facts that waited past their TTL for a message that
// never came.
//
// A held row is the one reaction row nothing else can reach: every other
// deletion joins through a row in `messages`, and a held fact names an id that
// has none. See chatlog.HeldReactionTTL.
// releaseArrivedReactions un-hides held facts whose message turned out to be
// here after all.
//
// The per-message release is best-effort — its error is logged and the message
// is still stored — so a fact can be left pending on a node that HAS the
// message, and nothing else comes back for it: the sender will not repeat what
// it believes delivered, and even a repeat may never bring another copy of the
// MESSAGE, which is what the per-message path keys on. This is the local half of
// the recovery, owing nothing to the peer.
func (r *DMRouter) releaseArrivedReactions(ctx context.Context, now time.Time) {
	store := r.reactions()
	if store == nil {
		return
	}
	scopes, err := store.ReleaseArrivedReactions(ctx, now)
	if err != nil {
		log.Warn().Err(err).Msg("arrived reactions could not be released")
		return
	}
	for _, scope := range scopes {
		// Told, not merely logged: the chips are drawn from a per-conversation
		// cache that only this event reloads, so a release nobody announces is a
		// reaction the user does not see until they switch chats.
		log.Debug().Str("scope", string(scope)).
			Msg("reactions whose message had already arrived were released")
		r.publishReactionsChanged(domain.PeerIdentityFromWire(string(scope)))
	}
}

// publishReactionsChanged tells the UI to reload one conversation's chips.
func (r *DMRouter) publishReactionsChanged(peer domain.PeerIdentity) {
	if r.eventBus == nil || peer.IsZero() {
		return
	}
	r.eventBus.Publish(ebus.TopicReactionsChanged, peer)
}

// reofferReactions offers every conversation's own facts again, one page each.
//
// This is the half that reaches a peer this node has no SESSION with. A reaction
// can travel three hops, and the two ends of that path may never be neighbours;
// re-offering only when a session comes up would then never retry for them, and
// nothing else would, because no outcome on this transport reports arrival.
//
// A page per conversation per pass, and only for the conversations whose backoff
// says it is time (reofferDue). The gap starts short and widens to
// ReofferMaxInterval, where it stays: the retries do not stop, because a peer
// reached only through transit may never open a session and "until their build
// can take it" has no deadline. What the backoff removes is the fixed cadence,
// not the retrying.
func (r *DMRouter) reofferReactions(ctx context.Context) {
	store := r.reactions()
	control := r.reactionControl()
	if store == nil || control == nil {
		return
	}
	self := r.MyAddress()
	if self.IsZero() {
		return
	}
	scopes, err := store.ConversationsWithReactionsBy(ctx, self)
	if err != nil {
		log.Warn().Err(err).Msg("conversations with our reactions could not be listed")
		return
	}
	for _, scope := range scopes {
		peer := domain.PeerIdentityFromWire(string(scope))
		if peer.IsZero() {
			// A group id rather than a peer identity: groups have no fan-out
			// yet (§8), so there is nobody to offer it to.
			continue
		}
		r.reofferConversation(ctx, control, peer)
	}
}

// reofferConversation offers one conversation's due page.
//
// The read and the send are one step under the store's removal lease — see
// offerReoffer — so a removal that starts between them cannot leave a queue full
// of facts about messages it has just erased.
func (r *DMRouter) reofferConversation(
	ctx context.Context,
	control *ReactionControlAdapter,
	peer domain.PeerIdentity,
) {
	err := control.reofferDue(ctx, peer, func(facts []domain.ReactionFact) error {
		return r.client.SendReactionFacts(peer, facts)
	})
	if err != nil {
		log.Debug().Err(err).Str("peer", logID(peer.String())).Msg("re-offer page not queued")
	}
}

func (r *DMRouter) sweepHeldReactions(ctx context.Context, now time.Time) {
	store := r.reactions()
	if store == nil {
		return
	}
	swept, err := store.SweepHeldReactions(ctx, now)
	if err != nil {
		log.Warn().Err(err).Msg("held reactions could not be swept")
		return
	}
	if swept > 0 {
		log.Debug().Int("reactions", swept).
			Msg("reactions waiting for a message that never arrived were dropped")
	}

}
