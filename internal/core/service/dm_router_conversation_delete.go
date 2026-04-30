package service

import (
	"context"
	"encoding/json"
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
// has an in-flight conversation_delete (wipe pending). The block
// is the outgoing-side barrier that closes the race where a new
// user-authored message would otherwise reach the peer's chatlog
// after the peer's wipe ran but before the sender's post-ack
// sweep — leaving a row gone on the receiver and present on the
// sender. Callers (UI) should match via errors.Is and render a
// localised "wipe in progress" hint instead of a generic send
// failure. The block lifts only when the pending entry is removed —
// that is, on a success ack or on abandonment. A transient error ack
// keeps the pending entry alive and the retry loop running, so the
// barrier stays raised until the next definitive outcome.
var ErrConversationDeleteInflight = errors.New("conversation_delete: wipe pending for peer")

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
var ErrConversationDeleteReservationLost = errors.New("conversation_delete: reservation lost before complete")

// ---------------------------------------------------------------------------
// conversation_delete / conversation_delete_ack — bulk wipe-the-thread
// control DM. Sibling of dm_router_delete.go, which handles the
// per-message variant. The two share the same wire transport
// (DMCrypto.SendControlMessage / DecryptIncomingControlMessage),
// the same retry-loop shape, AND the same pessimistic ordering on
// the sender side: the sender's chatlog rows are removed only after
// the peer's ack confirms it has wiped its own copy. A
// denied / abandoned / error response leaves the local rows alive so
// the user can see the divergence and re-issue the request rather
// than discover later that one side still holds the history.
//
// Why pessimistic for the bulk variant too: the spec calls for
// "delete the messages on the recipient ... and only THEN we mirror
// the action locally" — the sender must observe a successful peer
// wipe before mutating local state. An eager local wipe would leave
// the user with no way to recover history if the network half of the
// round-trip fails (offline peer, transport abandonment, partial
// receiver failure), which silently violates the protocol contract.
//
// Authorization deliberately diverges from message_delete:
//
//   - message_delete (single row): runs authorizedToDelete per row
//     so a per-flag matrix decides whether the requesting peer may
//     touch THIS message. The sender of the row owns its lifecycle
//     under the default sender-delete policy.
//   - conversation_delete (whole thread): wipes every non-immutable
//     row of the conversation regardless of authorship. Reusing
//     authorizedToDelete would refuse all rows the requester did
//     not author — under the default sender-delete that means HALF
//     the thread survives on each side after a "wipe everything"
//     gesture, which directly contradicts the user-visible promise
//     "Delete chat for everyone". The bulk gesture is a mutual
//     consent to forget the conversation and is initiated by an
//     explicit two-click confirmation in the UI, so it carries
//     stronger authority over peer-authored rows than a single
//     message_delete would. Immutable rows are the only carve-out
//     and stay on both sides.
//
// Design contract: docs/dm-commands.md.

// Retry policy mirrors message_delete (initial 30 s, exponential
// backoff x2, cap 300 s, max 6 dispatches). Constants are duplicated
// here rather than reused from dm_router_delete.go so each control DM
// can evolve its own policy without affecting the other; if the
// values diverge the documentation must be updated alongside the
// constant.
const (
	convDeleteRetryInitial    = 30 * time.Second
	convDeleteRetryCap        = 300 * time.Second
	convDeleteRetryMultiplier = 2
	convDeleteRetryMaxAttempt = 6
	convDeleteRetryTickPeriod = 5 * time.Second
	// convDeleteReservationTTL bounds how long a pending entry may
	// stay UNPREPARED — i.e. installed by BeginConversationDelete
	// but not yet promoted to prepared by CompleteConversationDelete.
	// The reservation is a synchronous barrier latch; the snapshot
	// + first wire dispatch run in CompleteConversationDelete with
	// a 10s snapshot timeout, so the worst-case healthy gap is on
	// the order of 10s. A value comfortably above that catches
	// pathological scheduling delays without leaving an orphan
	// reservation pinned to the peer forever (e.g. if the calling
	// code panics between Begin and the goroutine launch). On TTL
	// expiry the periodic reaper drops the entry; the user can
	// then re-issue the wipe.
	convDeleteReservationTTL = 60 * time.Second
	// convDeleteTerminalAckGrace bounds how long a pending entry stays
	// alive AFTER the final dispatch (attempt == convDeleteRetryMaxAttempt)
	// before the retry loop reaps it and publishes Abandoned. Without
	// this grace the entry would be retired the moment recordAttemptIfMatch
	// hits the budget, dropping the ack for the LAST dispatch — the
	// peer's reply would then race against an empty pending map and
	// the wipe would be reported Abandoned even though it was actually
	// applied on both sides. The grace gives the final ack a window
	// to land via removeIfMatch / noteErrorIfMatch and resolve the
	// outcome correctly. Sized comfortably above the largest backoff
	// and a typical RTT so a slow ack still wins over the reaper.
	convDeleteTerminalAckGrace = 60 * time.Second
)

// pendingConversationDelete tracks a single in-flight
// conversation_delete on the sender side. Keyed by peer (one wipe
// per peer at a time — a second click on the same identity while a
// wipe is pending is a no-op: SendConversationDelete short-circuits
// via has(peer) + atomic tryAdd, the original requestID and
// localKnownIDs both survive, and only the first request continues
// to retry until ack/abandonment).
//
// requestID binds the entry to the wire request that produced it.
// The retry loop re-dispatches with the SAME requestID so genuine
// duplicates of the same request are matched on ack; a fresh wipe
// (e.g. after the previous one was abandoned) generates a NEW
// requestID and the ack handler refuses any inbound ack whose
// echoed requestID does not match the current pending entry. Without
// this guard a late ack from an abandoned earlier wipe could
// silently retire the new pending and trigger the local sweep before
// the new wipe was applied on the recipient.
//
// IN-MEMORY ONLY: identical persistence story to pendingDelete in
// dm_router_delete.go. A process restart drops the in-flight
// retry. Pessimistic ordering means the local rows are still
// intact (the local sweep would only have run on a successful
// applied ack the sender process never observed), so the user
// can re-issue the wipe without losing local history. The
// re-issue mints a FRESH requestID — both because a stale
// pre-restart ack must not silently retire the new pending entry
// AND because the peer's outcome for the pre-restart requestID
// is unknown (it may have applied, partially applied, or never
// received the wipe; the sender process simply has no record).
// The fresh requestID drives a real first-contact gather on the
// peer over whatever scope exists when the new request lands;
// it is NOT guaranteed to find zero rows or return
// Applied/Deleted=0.
type pendingConversationDelete struct {
	peer        domain.PeerIdentity
	requestID   domain.ConversationDeleteRequestID
	sentAt      time.Time
	nextRetryAt time.Time
	attempt     int
	// localKnownIDs is the in-memory snapshot of chatlog row ids
	// this side considers peer-confirmed at click time, captured
	// by snapshotLocalKnownConversationIDs. NOT every non-immutable
	// row currently in chatlog: the snapshot deliberately excludes
	// self-authored outbound rows whose DeliveryStatus is "sent"
	// (locally accepted but peer-node has not confirmed receipt),
	// and skips immutable rows. Inbound rows are always included.
	// See the function comment on snapshotLocalKnownConversationIDs
	// for the full inclusion contract.
	//
	// NOT transmitted on the wire (the payload stays intent-only);
	// consulted only inside applyLocalConversationWipe to bound
	// the post-ack local sweep to this snapshot's row set. A
	// row OUTSIDE the snapshot is OUTSIDE the sender cleanup
	// scope and stays on this side: late-arriving inbound rows
	// (peer authored, landed here after click), late-arriving
	// late-delivery outbound rows (locally enqueued but not yet
	// peer-receipted at click), and immutable rows all survive
	// the local sweep regardless of what the peer ultimately
	// did with them.
	//
	// Receiver behaviour is independently scoped: the receiver
	// freezes a per-(peer, requestID) candidate set on
	// first-contact gather and re-attempts only that frozen
	// scope on retries — it does NOT re-iterate current chatlog
	// (see processInboundConversationDeleteFreshGather and the
	// inboundConversationDeleteCacheEntry comment). The wire
	// contract carries no scope; both sides build their scopes
	// locally and accept the documented asymmetric trade-offs.
	localKnownIDs map[domain.MessageID]struct{}
	// prepared marks an entry whose CompleteConversationDelete has
	// successfully attached localKnownIDs and is therefore ready to
	// be retried by the conversationDeleteRetryLoop. Entries
	// installed by BeginConversationDelete start with prepared=false
	// (synchronous barrier latch only); dueEntries skips them so
	// the retry loop cannot dispatch a request for which we have
	// no click-time snapshot. Without this gate, an applied ack
	// for a retry-driven dispatch would call applyLocalConversationWipe
	// with a nil localKnownIDs and silently report "wiped on both
	// sides" while the local rows survived intact.
	//
	// reservedAt records when BeginConversationDelete installed the
	// reservation. The retry loop reaps entries that stay
	// unprepared past convDeleteReservationTTL so a stranded
	// reservation (e.g. if the calling code crashes between Begin
	// and the goroutine launching CompleteConversationDelete) does
	// not pin the outgoing barrier on the peer until process
	// restart.
	prepared   bool
	reservedAt time.Time
	// terminalAt is non-zero once recordAttemptIfMatch has consumed
	// the final dispatch slot (attempt == convDeleteRetryMaxAttempt).
	// While terminalAt is set, dueEntries skips the entry — no further
	// dispatch will go out — but the entry is KEPT in the map so the
	// peer's ack for the final dispatch can still match via
	// removeIfMatch / noteErrorIfMatch within
	// convDeleteTerminalAckGrace. The retry loop's pruneTerminalAckExpired
	// reaps entries whose terminalAt is older than the grace and
	// publishes the abandoned outcome at that point. Without this
	// two-step terminal handling the final ack would arrive after the
	// pending entry was already retired, the requestID match would
	// fail, and a wipe that the peer actually applied would be
	// reported Abandoned.
	terminalAt time.Time
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
// BEFORE CompleteConversationDelete snapshotted — the row would
// reach the peer ahead of conversation_delete (so the peer's wipe
// removes it) but the requester's localKnownIDs snapshot would not
// include it, leaving it alive on the requester side.
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
// conversation_delete exists for peer; returns false when the
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
// SendConversationDelete uses tryAdd to RESERVE the slot BEFORE
// taking the click-time chatlog snapshot, with localKnownIDs left
// nil; the snapshot is then attached via attachLocalKnownIDs once
// it completes successfully, or the reservation is dropped via
// removeReservedIfMatch on snapshot failure. Reserving first is
// load-bearing for the outgoing barrier: SendMessage and
// SendFileAnnounce go through acquireSendIfNoPending, which checks
// the entries map and increments the inflight counter under the
// SAME mutex hold. Once tryAdd installs the entry, every subsequent
// acquireSendIfNoPending observes the barrier and returns false; an
// already-acquired send keeps its inflight slot until releaseSend
// runs, and CompleteConversationDelete waits for the inflight
// counter to drop to 0 (via inflightDrainedChan) BEFORE it
// snapshots — so the snapshot reads a chatlog state that is
// consistent with what the peer will see when conversation_delete
// arrives. Without the atomic acquire, a user-authored send could
// observe an empty barrier and land in chatlog after Begin reserved
// but before the snapshot ran, expanding the wipe scope past the
// localKnownIDs set.
func (s *conversationDeleteRetryState) tryAdd(p *pendingConversationDelete) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[p.peer]; exists {
		return false
	}
	s.entries[p.peer] = p
	return true
}

// attachLocalKnownIDs installs the click-time chatlog snapshot into
// an already-reserved pending entry, promotes it from reservation
// to prepared, AND re-anchors the retry-timer at `now`. Used by
// CompleteConversationDelete after tryAdd has reserved the barrier
// slot and the snapshot has completed. Mutates in place under the
// same mutex that guards every other access to entries; the
// requestID guard is defensive — if the slot was retired between
// reserve and attach (only possible via removeIfMatch on a stale
// ack racing with our own pending, which the retry / dispatch
// ordering rules out under normal operation), the caller treats
// the false return as "the reservation no longer belongs to us"
// and abandons the dispatch without restoring it.
//
// Re-anchoring `nextRetryAt = now + convDeleteRetryInitial` (and
// sentAt = now) is load-bearing: BeginConversationDelete set the
// initial retry timer relative to its OWN now (click-time on the
// UI thread), but the actual first wire dispatch happens from
// CompleteConversationDelete on a goroutine that may start much
// later — anywhere up to convDeleteReservationTTL behind. If the
// timer stayed anchored at click-time, prepared=true would flip
// with a nextRetryAt already in the past, the retry-loop tick
// could fire BEFORE this goroutine's own dispatchConversationDelete
// call, and the peer would see two duplicate dispatches inside
// one round-trip while the retry budget burned a slot for nothing.
// Re-anchoring at attach-time gives the goroutine the full
// convDeleteRetryInitial window before the retry loop is allowed
// to step in.
//
// Promoting prepared=true here is what allows the retry loop to
// pick the entry up via dueEntries; until this flip, the entry
// only exists to hold the outgoing barrier. Without that gate, a
// retry-loop dispatch could fire between Begin and Complete and
// the eventual applied ack would call applyLocalConversationWipe
// with a nil snapshot, silently reporting "wiped on both sides"
// while the local rows stayed intact.
func (s *conversationDeleteRetryState) attachLocalKnownIDs(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, ids map[domain.MessageID]struct{}, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.entries[peer]
	if !exists {
		return false
	}
	if p.requestID != requestID {
		return false
	}
	p.localKnownIDs = ids
	p.prepared = true
	p.sentAt = now
	p.nextRetryAt = now.Add(convDeleteRetryInitial)
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
// hint while a conversation_delete is in flight for the given peer.
// Equivalent to checking errors.Is(err, ErrConversationDeleteInflight)
// after a SendMessage/SendFileAnnounce attempt, but lets the UI
// gate the input synchronously without spending a send call.
func (r *DMRouter) IsConversationDeletePending(peer domain.PeerIdentity) bool {
	if r == nil || r.convDeleteRetry == nil {
		return false
	}
	return r.convDeleteRetry.has(normalizePeer(peer))
}

// removeIfMatch atomically retires and returns the pending entry for
// peer if and only if the entry's requestID matches the supplied
// requestID. On mismatch the map is left untouched and ok=false is
// returned — the inbound ack is dropped, the current pending entry
// continues unmolested, and the retry loop keeps chasing it.
//
// Use this ONLY on the terminal success path of the ack handler: a
// retired entry cannot be restored without a remove+restore window
// during which a concurrent SendConversationDelete could replace
// the pending with a fresh requestID and then be silently
// overwritten. For the transient-error path use noteErrorIfMatch,
// which does not remove the entry.
//
// The lookup AND the deletion happen under one mutex hold, which is
// load-bearing: a non-atomic peek+remove would let a concurrent
// SendConversationDelete (replacing the pending with a fresh
// requestID) or a concurrent retry-loop sweep slip between the two
// steps and either silently retire the wrong pending entry or
// observe a nil after the entry was already retired. The handler
// then has no way to recover the correct state.
func (s *conversationDeleteRetryState) removeIfMatch(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) (entry pendingConversationDelete, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.entries[peer]
	if !exists {
		return pendingConversationDelete{}, false
	}
	if p.requestID != requestID {
		return pendingConversationDelete{}, false
	}
	out := *p
	delete(s.entries, peer)
	return out, true
}

// noteErrorIfMatch returns a snapshot of the pending entry for peer
// when its requestID matches the supplied one — WITHOUT removing
// it. Used by the ack handler's transient-error path so the entry
// stays in the map for the retry loop to keep chasing, with no
// remove+restore window in which a concurrent
// SendConversationDelete could replace the pending with a fresh
// requestID and then be silently overwritten.
//
// On mismatch (late error ack from an abandoned earlier wipe, or
// no pending at all) ok=false is returned and the map stays
// untouched.
func (s *conversationDeleteRetryState) noteErrorIfMatch(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) (entry pendingConversationDelete, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.entries[peer]
	if !exists {
		return pendingConversationDelete{}, false
	}
	if p.requestID != requestID {
		return pendingConversationDelete{}, false
	}
	return *p, true
}

// dueEntries returns a snapshot of pending entries that are PREPARED
// (CompleteConversationDelete has attached the click-time snapshot)
// AND whose nextRetryAt has passed at the given time. The returned
// slice is independent of the map so callers can release the mutex
// while doing network I/O.
//
// Unprepared entries — installed by BeginConversationDelete but not
// yet promoted by CompleteConversationDelete — are deliberately
// skipped. Dispatching one would send a wipe whose eventual applied
// ack would call applyLocalConversationWipe with a nil
// localKnownIDs and silently report "wiped on both sides" while the
// local rows stayed intact. The reservation expires via
// pruneStaleReservations once it sits unprepared past
// convDeleteReservationTTL.
func (s *conversationDeleteRetryState) dueEntries(now time.Time) []pendingConversationDelete {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []pendingConversationDelete
	for _, p := range s.entries {
		if !p.prepared {
			continue
		}
		// Terminal entries are kept alive only so the ack for the
		// final dispatch can still land. They must NEVER be
		// re-dispatched: doing so would consume budget that does
		// not exist (recordAttemptIfMatch already capped attempt at
		// convDeleteRetryMaxAttempt), and the second "terminal"
		// signal would corrupt the abandonment flow.
		if !p.terminalAt.IsZero() {
			continue
		}
		if !p.nextRetryAt.After(now) {
			due = append(due, *p)
		}
	}
	return due
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
		if p.prepared {
			continue
		}
		if !p.reservedAt.IsZero() && now.Sub(p.reservedAt) <= ttl {
			continue
		}
		pruned = append(pruned, *p)
		delete(s.entries, peer)
	}
	return pruned
}

// recordAttemptIfMatch updates the pending entry for peer ONLY when
// its current requestID still matches the supplied one. The retry
// loop snapshots due entries first and dispatches outside the
// mutex, so by the time it comes back to bookkeeping the pending
// entry may have been replaced by a fresh SendConversationDelete
// (different requestID, attempt counter reset to 1). Updating that
// fresh entry as if it were a continuation of the old one would
// corrupt its retry budget — the old dispatch might even abandon
// the new request on the terminal path.
//
// Returns:
//
//   - terminal=true when the matched entry's attempt counter just
//     hit convDeleteRetryMaxAttempt; the entry is KEPT in the map
//     with terminalAt = now so the ack for the final dispatch can
//     still match within convDeleteTerminalAckGrace. The caller
//     does NOT publish abandonment from this signal — the retry
//     loop's pruneTerminalAckExpired publishes it once the grace
//     window elapses without an ack. matched is also true in this
//     case so the caller can log the budget-exhaustion event.
//   - matched=false when no entry exists for peer OR the entry's
//     requestID differs from the supplied one. The caller treats
//     this as "snapshot was stale, do not publish abandonment for
//     this dispatch". Both fields entry and terminal are zero in
//     this case.
//
// Counting model matches deleteRetryState.recordAttempt (see that
// rationale): dispatch happens BEFORE this call, so the bumped
// attempt counter reflects "number of dispatches that have actually
// gone out". With convDeleteRetryMaxAttempt=6 the trace is:
//
//	initial: attempt set to 1 by SendConversationDelete (1 dispatch)
//	tick:    dispatch #2 → bump to 2 → keep
//	...
//	tick:    dispatch #6 → bump to 6 → 6 >= 6 → terminal (entry
//	         retained until ack lands or grace elapses)
func (s *conversationDeleteRetryState) recordAttemptIfMatch(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, now time.Time) (entry pendingConversationDelete, terminal, matched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.entries[peer]
	if !ok {
		return pendingConversationDelete{}, false, false
	}
	if p.requestID != requestID {
		return pendingConversationDelete{}, false, false
	}
	p.attempt++
	p.sentAt = now
	if p.attempt >= convDeleteRetryMaxAttempt {
		// Mark the entry terminal but KEEP it in the map. dueEntries
		// will skip it (no further dispatch), and the ack handler
		// can still match via removeIfMatch / noteErrorIfMatch
		// within convDeleteTerminalAckGrace. pruneTerminalAckExpired
		// reaps it and publishes Abandoned once the grace elapses.
		p.terminalAt = now
		return *p, true, true
	}
	backoff := time.Duration(convDeleteRetryInitial) * (1 << (p.attempt - 1))
	if backoff > convDeleteRetryCap {
		backoff = convDeleteRetryCap
	}
	p.nextRetryAt = now.Add(backoff)
	return *p, false, true
}

// pruneTerminalAckExpired drops every entry whose terminalAt was set
// more than grace ago without an ack landing. Returns the dropped
// entries so the caller can publish Abandoned for each. Called once
// per retry-loop tick alongside pruneStaleReservations.
//
// Without this reaper the terminal entry would sit forever holding
// the outgoing barrier even though no further dispatches would go
// out (dueEntries skips terminal entries). With it, the barrier is
// released and the user gets a definitive Abandoned outcome at most
// grace after the final dispatch.
func (s *conversationDeleteRetryState) pruneTerminalAckExpired(now time.Time, grace time.Duration) []pendingConversationDelete {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pruned []pendingConversationDelete
	for peer, p := range s.entries {
		if p.terminalAt.IsZero() {
			continue
		}
		if now.Sub(p.terminalAt) <= grace {
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
// pass through SendMessage's barrier check and reach the peer
// ahead of conversation_delete — the receiver would sweep the
// row, the requester's snapshot would not contain it (it had not
// yet committed when the snapshot ran), and the post-ack local
// sweep would leave it alive on the requester side. Pulling the
// reservation onto the synchronous path closes that window.
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
// Wire-side semantics (carried by CompleteConversationDelete):
//
//   - The minted ConversationDeleteRequestID travels in
//     ConversationDeletePayload.RequestID and the recipient echoes
//     it in ConversationDeleteAckPayload; the ack handler uses
//     (peer, requestID) — not envelope sender alone — to match the
//     pending entry. This blocks late acks from abandoned earlier
//     wipes from silently retiring a fresh pending wipe.
//   - The conversation peer is NOT in the payload — the recipient
//     derives it from the verified envelope sender, so a forged
//     payload cannot redirect the wipe to a different conversation.
//   - The retry loop re-dispatches with the SAME requestID until
//     the peer's conversation_delete_ack arrives or the budget is
//     exhausted.
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
	if peer == "" {
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
			Str("peer", string(peer)).
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
		return "", fmt.Errorf("generate conversation_delete request id: %w", err)
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
		peer:        peer,
		requestID:   requestID,
		sentAt:      now,
		nextRetryAt: now.Add(convDeleteRetryInitial),
		attempt:     1,
		reservedAt:  now,
		// prepared stays false: dueEntries skips this entry until
		// CompleteConversationDelete attaches the click-time
		// localKnownIDs snapshot via attachLocalKnownIDs.
	})
	if !added {
		log.Debug().
			Str("peer", string(peer)).
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
//   - Wire dispatch failure: the reservation is LEFT IN PLACE so
//     the retry loop can pick it up; nil is returned and the UI
//     can render "dispatched" — the request is now under the retry
//     loop's care and will resolve through the same terminal-event
//     paths as a successful initial dispatch.
//
// The bulk-delete predicate is NOT applied here, and the two sides
// run subtly different scopes:
//
//   - Receiver (handleInboundConversationDelete → frozen
//     first-contact gather): on the first inbound for a given
//     (peer, requestID) the receiver gathers every non-immutable
//     row currently in chatlog with the peer and FREEZES that set
//     in inboundConvDeleteCache. Every subsequent retry of the
//     same requestID — lost-ack replay or partial-failure
//     survivor sweep — operates ONLY on the cached frozen scope;
//     current chatlog is never re-iterated for the same
//     requestID. Rows the peer authored AFTER first contact stay
//     on the receiver side.
//   - Sender (applyLocalConversationWipe, called from
//     handleInboundConversationDeleteAck on the success ack) only
//     removes rows present in pending.localKnownIDs — the
//     in-memory snapshot captured here at click time. The
//     snapshot deliberately excludes self-authored outbound rows
//     in DeliveryStatus="sent" (locally accepted, peer-receipt
//     unconfirmed) so a row still in our outbound queue at click
//     time stays on this side regardless of when the peer
//     actually receives it. Immutable rows are skipped on both
//     sides.
//
// This asymmetry is the snapshot/freeze gate: the receiver freezes
// its scope on first contact, the sender freezes its scope at
// click. Both sides intentionally stop their wipe at the rows they
// already knew about; rows authored later by either party fall
// outside both scopes. Late deliveries from the OUTGOING side (a
// user-authored send that reaches the peer ahead of
// conversation_delete) are blocked separately by the outgoing
// barrier in SendMessage and SendFileAnnounce, which
// BeginConversationDelete installs BEFORE this method runs, plus
// the localKnownIDs DeliveryStatus filter that keeps unconfirmed
// outbound rows on this side. Late deliveries that we cannot
// block (a message the peer sent before receiving the wipe but
// still in flight to us at command-arrival on its side) are
// documented as a known asymmetry in docs/dm-commands.md;
// tombstones suppress only the re-replay class (the same envelope
// arriving again after we deleted it).
func (r *DMRouter) CompleteConversationDelete(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error {
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
			Str("peer", string(peer)).
			Str("request_id", string(requestID)).
			Msg("dm_router: CompleteConversationDelete: reservation gone at claim (TTL reaper raced the goroutine startup); abandoning dispatch")
		return ErrConversationDeleteReservationLost
	}

	// Drain any sends that won the race against BeginConversationDelete:
	// SendMessage / SendFileAnnounce that observed an empty barrier
	// and already incremented the in-flight counter must finish (and
	// either land in chatlog or fail) BEFORE we read chatlog for the
	// snapshot. Without the drain, the snapshot can observe a chatlog
	// state that's missing a row the goroutine is about to commit;
	// once that row lands AFTER the snapshot, it is OUTSIDE
	// localKnownIDs and applyLocalConversationWipe will skip it on
	// this side (the local sweep is gated on snapshot membership,
	// NOT a full current-chatlog scan). The remaining hole the drain
	// closes is purely a SEEN/DELIVERY one: if the row reaches the
	// peer ahead of conversation_delete, the peer wipes its copy
	// while we keep ours — a one-sided originator-keeps outcome
	// that's only "wrong" because we deliberately omitted the row
	// from a snapshot we could have included it in.
	//
	// Note: drain only waits for LOCAL handoff (the goroutine returns
	// once the local node's send_message reply lands), not for peer
	// receipt. A row that's local-accepted-but-not-yet-delivered at
	// snapshot time is still in chatlog, BUT
	// snapshotLocalKnownConversationIDs deliberately filters such
	// outbound rows out (DeliveryStatus="sent"). The
	// receiver-only-row hole — peer eventually receives the data DM
	// after its first-contact gather, peer keeps it, we wiped it —
	// is closed by that snapshot rule, not by the drain. See
	// snapshotLocalKnownConversationIDs for the full inclusion
	// contract and docs/dm-commands.md for the asymmetric cases the
	// design accepts.
	//
	// Bounded by the same context as the snapshot below — a stuck
	// send (or a ctx that's already cancelled) will not pin the
	// reservation forever; on timeout we drop the reservation and
	// let the user re-issue.
	drainCtx, cancelDrain := context.WithTimeout(ctx, 10*time.Second)
	drainCh := r.convDeleteRetry.inflightDrainedChan(peer)
	select {
	case <-drainCh:
		// All in-flight sends settled; safe to snapshot.
	case <-drainCtx.Done():
		cancelDrain()
		log.Warn().
			Str("peer", string(peer)).
			Str("request_id", string(requestID)).
			Err(drainCtx.Err()).
			Msg("dm_router: CompleteConversationDelete: inflight send drain timed out; dropping reservation to avoid snapshot inconsistency")
		r.convDeleteRetry.removeReservedIfMatch(peer, requestID)
		return fmt.Errorf("conversation_delete: inflight send drain timeout: %w", drainCtx.Err())
	}
	cancelDrain()

	// Snapshot of local row IDs at click time. This set is
	// in-memory only and constrains the post-ack local sweep to
	// the rows snapshotLocalKnownConversationIDs decided are
	// peer-confirmed — NOT every non-immutable row currently in
	// chatlog, and NOT every row the user saw at click. The
	// inclusion contract (full detail at
	// snapshotLocalKnownConversationIDs): inbound rows always
	// included, outbound rows included only when DeliveryStatus
	// is "delivered" or "seen" (outbound "sent" rows are
	// deliberately excluded — they may still be visible in our
	// own UI but the peer-node has not confirmed receipt, so
	// including them would race the receiver-only-row hole).
	// Without this snapshot a late-arriving inbound between
	// click and ack would be deleted on our side while the
	// receiver kept it, leaving a sender-only row.
	//
	// Wire payload still carries no row list — the receiver
	// freezes its OWN scope on first contact via
	// inboundConvDeleteCache and re-attempts only that frozen
	// scope on retries (current chatlog is never re-iterated for
	// the same requestID). Both sides build their scopes
	// locally; the documented late-delivery asymmetry is the
	// accepted trade-off (see docs/dm-commands.md §"Snapshot
	// inclusion rule"). The snapshot here runs WITH the
	// outgoing barrier already up (see BeginConversationDelete),
	// the inflight drain just observed, and the reservation
	// TTL freshly refreshed by the claim above — so neither a
	// user-authored send nor the reaper can slip through this
	// window.
	snapshotCtx, cancelSnap := context.WithTimeout(ctx, 10*time.Second)
	localKnownIDs, err := r.snapshotLocalKnownConversationIDs(snapshotCtx, peer)
	cancelSnap()
	if err != nil {
		// Snapshot failed — drop the reservation so the barrier
		// lifts and the user can re-issue. removeReservedIfMatch
		// guards on requestID so a concurrent BeginConversationDelete
		// that already raced in (extremely unlikely under the
		// duplicate-click short-circuit, but defensive) keeps its
		// own reservation intact.
		r.convDeleteRetry.removeReservedIfMatch(peer, requestID)
		return err
	}

	// Attach the snapshot to our reserved entry so the post-ack
	// applyLocalConversationWipe can consult it. Mismatch is
	// effectively impossible here (no dispatch has happened, no
	// ack can have arrived, the retry loop won't fire for
	// convDeleteRetryInitial which exceeds the snapshot timeout,
	// and the claim above refreshed the TTL), but the requestID
	// guard documents the contract: only the reservation we just
	// made may be mutated by this call. If we still see a miss,
	// surface it as the same typed error as the claim case so the
	// UI does not advertise "dispatched" — by the time we attach,
	// no wire command has gone out under this requestID.
	if !r.convDeleteRetry.attachLocalKnownIDs(peer, requestID, localKnownIDs, time.Now().UTC()) {
		log.Warn().
			Str("peer", string(peer)).
			Str("request_id", string(requestID)).
			Msg("dm_router: CompleteConversationDelete: reservation gone before snapshot attach; abandoning dispatch")
		return ErrConversationDeleteReservationLost
	}

	if err := r.dispatchConversationDelete(ctx, peer, requestID); err != nil {
		// Wire submission failed; pending entry stays so the retry
		// loop can attempt again. Local rows are intact — the user
		// will see either a peer-side success (chatlog wipe in the
		// ack handler + TopicConversationDeleteCompleted with
		// status applied) or an abandonment once the budget is
		// exhausted.
		log.Warn().Err(err).
			Str("peer", string(peer)).
			Msg("dm_router: CompleteConversationDelete: initial wire send failed; retry pending")
		return nil
	}

	log.Info().
		Str("peer", string(peer)).
		Msg("dm_router: conversation_delete dispatched; awaiting peer ack before local wipe")
	return nil
}

// SendConversationDelete is a convenience wrapper that runs the
// reservation step (BeginConversationDelete) and the snapshot +
// dispatch step (CompleteConversationDelete) in one call. UI code
// that wants the barrier to be up BEFORE returning to the event
// loop must NOT use this wrapper — it MUST call
// BeginConversationDelete synchronously and then run
// CompleteConversationDelete on a background goroutine. Test code
// that does not need that synchronisation discipline can use this
// wrapper for brevity.
//
// Returns nil on the duplicate-click short-circuit (no reservation
// was installed, the existing in-flight request continues). Returns
// the validation / snapshot error on synchronous failures of
// either phase.
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

// applyLocalConversationWipe runs the sender-side chatlog sweep that
// mirrors the peer's confirmed wipe. Called by
// handleInboundConversationDeleteAck once the peer reports applied;
// never called from SendConversationDelete (pessimistic ordering —
// no local mutation before the peer confirms).
//
// Walks the conversation's current chatlog and removes every
// non-immutable row whose ID is also present in localKnownIDs
// (the click-time snapshot captured in
// CompleteConversationDelete). The snapshot is the SENDER's
// peer-confirmed local cleanup scope at click time, NOT every
// visible row: snapshotLocalKnownConversationIDs deliberately
// excludes self-authored outbound rows in DeliveryStatus="sent"
// (locally accepted but peer-receipt unconfirmed) even though
// those rows ARE visible in the user's own UI. Rows that landed
// locally AFTER the click and rows excluded by the
// DeliveryStatus filter are both OUTSIDE the SENDER's cleanup
// scope and stay on this side; symmetric survival on the
// receiver side is NOT guaranteed and depends on delivery
// ordering — a row may have been wiped on the peer
// (originator-only outcome) or kept on the peer (both-sides
// hold), see the dm_command.go payload comment and
// docs/dm-commands.md "Snapshot inclusion rule" for the full
// trade-off. The contract this method enforces is local-only:
// "delete the rows in our peer-confirmed snapshot once the peer
// confirms its own wipe". Authorship is NOT consulted (see the
// file header for why bulk wipe diverges from the per-row
// authorizedToDelete matrix). DeleteByID is paired with
// file-transfer cleanup for each removed ID.
//
// Returns the number of rows actually removed and ok==false when
// the wipe could not run completely (chatlog unavailable, ReadCtx
// failed before the first row, or DeleteByID failed for at least
// one row). The ack handler uses ok to decide whether to publish
// the outcome with LocalCleanupFailed=true so the UI can warn the
// user that the peer is consistent but the local side still holds
// some rows.
func (r *DMRouter) applyLocalConversationWipe(peer domain.PeerIdentity, localKnownIDs map[domain.MessageID]struct{}) (deleted int, ok bool) {
	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("peer", string(peer)).
			Msg("dm_router: applyLocalConversationWipe: chatlog store unavailable; cannot mirror peer wipe locally")
		return 0, false
	}

	// Empty local snapshot ⇒ chatlog had no non-immutable rows
	// at click time, nothing to mirror. Skip the read entirely
	// and report clean success.
	if len(localKnownIDs) == 0 {
		return 0, true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := store.ReadCtx(ctx, "dm", peer)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", string(peer)).
			Msg("dm_router: applyLocalConversationWipe: chatlog read failed; cannot mirror peer wipe locally")
		return 0, false
	}

	var removedIDs []domain.MessageID
	failures := 0
	for _, e := range entries {
		flag := protocol.MessageFlag(e.Flag)
		if flag == protocol.MessageFlagImmutable {
			continue
		}
		id := domain.MessageID(e.ID)
		// Local-snapshot gate: only delete rows whose id is in
		// localKnownIDs. The snapshot is the SENDER's local
		// cleanup scope at click time — see
		// snapshotLocalKnownConversationIDs for the inclusion
		// rule. It is NOT "every row the user saw": visible
		// self-authored outbound rows still in DeliveryStatus="sent"
		// are deliberately excluded so a row in our outbound
		// queue that the peer-node has not yet ACK'd does not
		// get mirror-wiped here while the data DM still
		// reaches the peer. It also makes NO promise about
		// peer-side survival of out-of-snapshot rows: a row
		// that lands locally after the click (late-inbound the
		// peer authored before its own first-contact gather)
		// may have been wiped on the peer side, while a
		// self-authored "sent" row may be wiped or kept on the
		// peer depending on delivery ordering. The
		// out-of-snapshot row is left ON THIS SIDE regardless;
		// the asymmetric trade-off is documented in
		// docs/dm-commands.md.
		if _, ok := localKnownIDs[id]; !ok {
			continue
		}
		// Tombstone BEFORE DeleteByID so a replay arriving in
		// the window between DELETE and the post-loop tombstone
		// update is suppressed by onNewMessage. A pre-mark for a
		// row whose DeleteByID later fails is harmless: a future
		// replay simply re-DELETEs.
		r.wipeTombstones.Note([]domain.MessageID{id}, time.Now().UTC())
		removed, err := store.DeleteByID(id)
		if err != nil {
			failures++
			log.Warn().Err(err).
				Str("target", string(id)).
				Str("peer", string(peer)).
				Msg("dm_router: applyLocalConversationWipe: chatlog DeleteByID failed for row; continuing but will report local cleanup failure")
			continue
		}
		if !removed {
			continue
		}
		removedIDs = append(removedIDs, id)
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(id)
		}
	}
	deleted = len(removedIDs)

	// Refresh the UI with whatever we did remove, even on partial
	// failure — those rows ARE gone and the active-conversation
	// cache must reflect that. Surviving rows (immutable, or rows
	// DeleteByID failed for) stay in the cache because their IDs
	// are not in removedIDs. Tombstones for removed IDs were
	// pre-marked inside the loop; no post-loop Note is needed.
	r.evictWipedConversationFromUI(peer, removedIDs)

	if failures > 0 {
		log.Warn().
			Str("peer", string(peer)).
			Int("deleted", deleted).
			Int("failures", failures).
			Msg("dm_router: local conversation wipe partial failure after peer ack; outcome will carry LocalCleanupFailed=true")
		return deleted, false
	}

	log.Info().
		Str("peer", string(peer)).
		Int("deleted", deleted).
		Msg("dm_router: local conversation wipe mirrored after peer ack")

	return deleted, true
}

// evictWipedConversationFromUI removes the given message IDs from the
// active-conversation cache (when peer is the active conversation),
// drops the same IDs from seenMessageIDs, and refreshes the sidebar
// preview / unread badge from chatlog. Called once at the end of
// each conversation-wipe path (sender-side post-ack
// applyLocalConversationWipe, receiver-side sweepInboundDeleteScope)
// so the deleted bubbles disappear immediately, without waiting for
// an unrelated redraw.
//
// removedIDs MUST be the list of IDs that were ACTUALLY removed
// from chatlog (not the list considered for removal). Rows that
// survived the sweep — immutable rows always, plus any rows whose
// per-row DeleteByID failed during partial failure — must NOT be
// listed; they stay in chatlog and the active cache must keep
// rendering them, otherwise the user sees a blank chat thread that
// would resurrect on the next conversation reload. A blanket
// cache.Load(peer, nil) is wrong for the same reason.
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
	r.mu.Unlock()

	r.refreshPreviewAfterDelete(peer)

	if cacheChanged {
		r.notify(UIEventMessagesUpdated)
	}
	r.notify(UIEventSidebarUpdated)
}

// dispatchConversationDelete encodes the ConversationDeletePayload
// (carrying the requestID that binds the wire request to its
// pending entry) and submits it through DMCrypto.SendControlMessage.
// Used by both the initial send in SendConversationDelete and the
// retry loop for subsequent attempts; the same requestID is reused
// across all dispatches of one request.
//
// Tests that need to count dispatches or avoid the rpc/identity
// stack can install r.dispatchControlConversationDeleteFn before
// exercising the public entry points; when set, this method
// delegates to that function and skips payload encoding +
// SendControlMessage entirely. Production code leaves
// r.dispatchControlConversationDeleteFn nil and runs the real path.
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

// ---------------------------------------------------------------------------
// Inbound handlers
// ---------------------------------------------------------------------------

// handleInboundConversationDelete processes a remote request to wipe
// the conversation with envelopeSender. The conversation peer is
// derived exclusively from the verified envelope sender — the
// payload carries only RequestID, so a malicious sender cannot
// redirect the wipe to a different conversation. RequestID binds
// the eventual ack to the pending entry on the requester side.
//
// Per-row predicate diverges from handleInboundMessageDelete: the
// authorizedToDelete matrix is NOT consulted. The receiver removes
// every non-immutable row of the conversation regardless of
// authorship (see the file header for the bulk authorisation
// rationale). Per-row chatlog DeleteByID failures downgrade the
// entire ack to error so the requester retries instead of
// mirroring the wipe against an inconsistent peer state.
//
// Receiver-side FROZEN SCOPE per (peer, requestID). The first
// apply gathers a candidate set from CURRENT chatlog ONCE and
// caches it in inboundConvDeleteCache. Every subsequent retry —
// whether a lost-ack replay (survivors empty) or a partial-failure
// retry (survivors non-empty) — operates ONLY on the cached
// candidate set; the receiver's CURRENT chatlog is NEVER
// re-iterated for the same requestID. This is load-bearing:
// without the freeze a row the peer added between attempts would
// be swept on the retry, expanding scope past the sender's
// localKnownIDs snapshot and leaving the two histories divergent
// on the eventual applied ack.
//
// Cache shape: candidates (frozen on first apply, never extended),
// survivors (subset of candidates whose DeleteByID failed in the
// most recent attempt — empty after a clean apply), lastStatus,
// cumulativeDeleted (running total across all attempts of this
// requestID, NOT per-attempt), gatheredScope (false on a
// gather-failed tombstone). Both success AND error outcomes are
// cached so partial-failure retries can re-attempt only the
// surviving subset; gather-failed first-contact also caches a
// tombstone so future retries refuse a second cold gather.
//
// Decision tree (load-bearing ORDER — live cache always wins
// over the seen-set, otherwise an Error seen entry left by a
// partial-failure first apply would short-circuit the very
// retry that is supposed to sweep the survivors):
//
//  1. atomic claimColdOrReplay (under cache mutex). Three
//     outcomes:
//     - Replay: live cached entry exists (incl. one with
//     non-empty survivors) → process against frozen scope
//     (tombstone short-circuit / lost-ack replay /
//     partial-failure survivors retry). DO NOT touch the
//     seen-set on this path; the live entry is more
//     authoritative.
//     - Contended: cache miss + another handler is gathering
//     → reply Error/0 conservatively. The sender retries; by
//     then the winner has populated the cache and the next
//     call resolves to Replay.
//     - Cold: cache miss + no in-progress mark → claim the slot
//     (defer releaseClaim), then proceed to step 2.
//  2. ONLY on Cold: long-lived seen-set lookup. Hit means the
//     active scope cache TTL elapsed but we still owe a stable
//     replay for this requestID — replay
//     replyInboundConversationDeleteFromSeen WITHOUT a fresh
//     cold gather (a fresh gather would silently expand scope
//     past the sender's localKnownIDs). Miss means true first
//     contact for this requestID — proceed to step 3.
//  3. Fresh gather + sweep + commit (cache + seen-set).
func (r *DMRouter) handleInboundConversationDelete(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var payload domain.ConversationDeletePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: conversation_delete payload malformed; dropping")
		return
	}
	if !payload.Valid() {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(payload.RequestID)).
			Msg("dm_router: conversation_delete payload invalid; dropping")
		return
	}

	now := time.Now().UTC()

	// Step 1: atomic claim against the LIVE scope cache. Under
	// one mutex hold the cache returns one of three results:
	// replay against a live cached entry, claim a cold-gather
	// slot, or back off because another handler is already
	// gathering for this requestID. The live cache MUST be
	// consulted before the long-lived seen-set: a partial-failure
	// first apply commits BOTH a live cache entry (with
	// survivors!=empty, status=Error) AND a seen-set entry
	// (status=Error, cumulativeDeleted=N). Checking seen-set
	// first would reply Error/N immediately and skip
	// processInboundConversationDeleteReplay's survivor sweep,
	// turning a recoverable partial failure into guaranteed
	// retry-budget abandonment. The seen-set is the FALLBACK
	// for retries that arrive after the active scope cache has
	// been reaped, not a fast path over it.
	cachedEntry, claim := r.inboundConvDeleteCache.claimColdOrReplay(envelopeSender, payload.RequestID, now, inboundConvDeleteCacheTTL)

	if claim == inboundClaimContended {
		// Another handler is mid-process for the SAME
		// (peer, requestID) — either Cold first-apply OR a
		// live-cache Replay (partial-failure retry sweep or
		// lost-ack replay). Responding Error/0 is conservative
		// but correct: the sender retries on the normal
		// schedule, and by the time that retry lands the winner
		// has either committed a converged entry (next call
		// resolves to inboundClaimReplay against it) or planted
		// a gather-failed tombstone (also Replay,
		// short-circuited). Letting both handlers run would
		// either race two cold scopes against possibly-different
		// chatlog snapshots OR have two replays iterate the
		// same shared survivors map (a Go data race) and
		// overwrite each other's cumulative-progress on commit.
		log.Warn().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(payload.RequestID)).
			Msg("dm_router: inbound conversation_delete — concurrent processor contention; replying Error/0 conservatively, sender will retry")
		r.replyConversationDeleteAck(envelopeSender, payload.RequestID, domain.ConversationDeleteStatusError, 0)
		return
	}

	// Both Replay and Cold took the in-progress slot — defer
	// release so a panic anywhere in the processing path cannot
	// strand the slot and forever block retries on this
	// (peer, requestID).
	defer r.inboundConvDeleteCache.releaseClaim(envelopeSender, payload.RequestID)

	if claim == inboundClaimReplay {
		r.processInboundConversationDeleteReplay(envelopeSender, payload.RequestID, cachedEntry, now)
		return
	}

	// inboundClaimCold:
	// Step 2: seen-set fallback. The live cache had no entry for
	// this requestID — either we've never seen it, OR the
	// active scope cache TTL has expired and the heavy scope
	// was reaped. The seen-set distinguishes the two: a hit
	// means "we processed this requestID before, the scope is
	// gone, but we still owe a replay". Replaying the seen
	// outcome is safe; running a FRESH cold-gather against
	// current chatlog is not, because chatlog may have grown
	// since first contact and a fresh sweep would silently
	// expand scope past the sender's localKnownIDs.
	seen, present, fresh := r.inboundConvDeleteSeen.lookup(envelopeSender, payload.RequestID, now, inboundConvDeleteSeenTTL)
	if present {
		if fresh {
			r.replyInboundConversationDeleteFromSeen(envelopeSender, payload.RequestID, seen, now)
			return
		}
		// Present but not fresh — either an expired-but-not-
		// yet-reaped entry (full payload, seen.seenAt populated)
		// or a tombstone hit (full payload zero, seen.seenAt
		// zero — reaper already dropped the payload but kept
		// the key as a known-requestID marker). Both cases mean
		// "we have definitely seen this requestID before, don't
		// cold-gather". Refuse with conservative Error/0; the
		// sender's retry-budget / terminal-grace logic
		// resolves to Abandoned on its side, and the user can
		// re-issue with a FRESH requestID for a real wipe.
		ev := log.Warn().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(payload.RequestID))
		if !seen.seenAt.IsZero() {
			ev = ev.Dur("entry_age", now.Sub(seen.seenAt))
		} else {
			ev = ev.Bool("tombstone", true)
		}
		ev.Msg("dm_router: inbound conversation_delete — known requestID past TTL or tombstoned; replying Error/0 conservatively to refuse late cold-gather")
		r.replyConversationDeleteAck(envelopeSender, payload.RequestID, domain.ConversationDeleteStatusError, 0)
		return
	}

	// True first contact for this requestID: gather, sweep,
	// commit.
	r.processInboundConversationDeleteFreshGather(envelopeSender, payload.RequestID, now)
}

// replyInboundConversationDeleteFromSeen answers an inbound
// conversation_delete from the long-lived minimal-outcome seen-set.
// Wire contract: Applied replays echo the cumulativeDeleted count
// recorded for the requestID; non-Applied (Error from a
// gather-failed tombstone or from an abandoned partial failure
// whose scope cache was reaped) replays echo Deleted=0 to match
// the documented "non-Applied seen replays carry zero count" rule.
// cumulativeDeleted is preserved in the seen-set for diagnostics
// and for any future transition back to Applied (it stays
// monotonic — a later attempt only adds rows).
//
// The seen-set anchor is also refreshed (seenAt = now) so a
// sustained burst of retries against the same already-known
// requestID does not let the seen entry expire mid-stream and
// reopen a cold gather.
func (r *DMRouter) replyInboundConversationDeleteFromSeen(envelopeSender domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, seen inboundConversationDeleteSeenEntry, now time.Time) {
	wireDeleted := 0
	if seen.status == domain.ConversationDeleteStatusApplied {
		wireDeleted = seen.cumulativeDeleted
	}
	r.inboundConvDeleteSeen.record(envelopeSender, requestID, seen.status, seen.cumulativeDeleted, seen.gatheredScope, now)
	log.Debug().
		Str("envelope_sender", string(envelopeSender)).
		Str("request_id", string(requestID)).
		Str("seen_status", string(seen.status)).
		Int("seen_cumulative", seen.cumulativeDeleted).
		Int("wire_deleted", wireDeleted).
		Bool("seen_gathered_scope", seen.gatheredScope).
		Msg("dm_router: inbound conversation_delete — seen-set replay (live scope cache miss)")
	r.replyConversationDeleteAck(envelopeSender, requestID, seen.status, wireDeleted)
}

// processInboundConversationDeleteReplay handles the
// inboundClaimReplay branch: a cached entry exists for
// (peer, requestID), and the receiver MUST process against the
// frozen scope rather than against current chatlog. Three
// sub-cases:
//
//   - gatheredScope=false (gather-failed tombstone): short-circuit
//     to Error/0. We refuse a second cold gather because chatlog
//     may have grown since first contact; gathering now would
//     expand scope past the sender's localKnownIDs.
//   - survivors empty: lost-ack replay. Echo the cached outcome
//     and cumulative count; do not touch chatlog at all.
//   - survivors non-empty: partial-failure retry. Sweep ONLY the
//     cached survivors set (not current chatlog). Each
//     successful DeleteByID inside sweep credits
//     cumulativeDeleted INLINE via recordDeletion under
//     cache.mu — there is no deferred bump. commitReplayResult
//     only intersects newSurvivors with the live survivor set
//     (defending against a tombstone-cleared id being
//     resurrected by an errored DeleteByID) and settles
//     status; cumulativeDeleted is read back from the live
//     entry, not added to here. The seen-set is mirrored
//     inline by every recordDeletion / commitReplayResult
//     call.
func (r *DMRouter) processInboundConversationDeleteReplay(envelopeSender domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, entry inboundConversationDeleteCacheEntry, now time.Time) {
	if !entry.gatheredScope {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(requestID)).
			Msg("dm_router: inbound conversation_delete retry — gather-failed tombstone hit; replying Error/0 without re-attempting gather")
		// Refresh the seen-set anchor so the tombstone keeps
		// gating future retries even if the scope cache reap
		// happens before this requestID finishes draining the
		// sender's retry budget.
		r.inboundConvDeleteSeen.record(envelopeSender, requestID, entry.lastStatus, entry.cumulativeDeleted, false, now)
		r.replyConversationDeleteAck(envelopeSender, requestID, entry.lastStatus, entry.cumulativeDeleted)
		return
	}
	if len(entry.survivors) == 0 {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(requestID)).
			Str("cached_status", string(entry.lastStatus)).
			Int("cumulative_deleted", entry.cumulativeDeleted).
			Msg("dm_router: inbound conversation_delete retry — replaying cached ack without re-sweeping receiver chatlog")
		// Same anchor refresh rationale as the tombstone branch.
		r.inboundConvDeleteSeen.record(envelopeSender, requestID, entry.lastStatus, entry.cumulativeDeleted, true, now)
		r.replyConversationDeleteAck(envelopeSender, requestID, entry.lastStatus, entry.cumulativeDeleted)
		return
	}

	// Partial-failure retry: re-attempt only the cached survivors.
	// entry.survivors here is a DEEP COPY (claimColdOrReplay
	// cloned it before returning), so iterating it inside
	// sweepInboundDeleteScope cannot race a concurrent
	// recordDeletionAnyScope (fired by suppressIfWipeTombstoned
	// for a replayed envelope landing during our sweep) mutating
	// the cache's real survivors set. Rows the peer authored
	// after first contact are NOT in candidates and are NEVER
	// touched here.
	//
	// cumulativeDeleted is updated INLINE by sweep — each
	// successful DeleteByID inside sweepInboundDeleteScope calls
	// recordDeletion under cache.mu, which atomically drops the
	// id from survivors AND bumps cumulative. The same primitive
	// is invoked by the tombstone path (recordDeletionAnyScope).
	// Idempotency lives at the SURVIVOR-SET level under cache.mu,
	// NOT at the database level — a tombstoned replay can
	// re-insert the row after the original delete and a second
	// DeleteByID can then succeed on the same id. The credit
	// guarantee is: whichever recordDeletion(/AnyScope) call
	// takes the cache mutex first while id is still in survivors
	// wins ownership and credits exactly once; every subsequent
	// recordDeletion call for that id finds the survivor already
	// cleared and returns without bumping cumulative.
	// commitReplayResult therefore does NOT add deletedThisAttempt
	// — it only intersects newSurvivors with the live survivor
	// set (defending against resurrecting a tombstone-cleared id
	// when our DeleteByID errored on it) and settles status. The
	// older deferred "commit-side bump" model double-counted any
	// id whose replay envelope landed during sweep; the inline
	// model is the load-bearing fix.
	deletedThisAttempt, newSurvivors := r.sweepInboundDeleteScope(envelopeSender, requestID, entry.survivors)
	status, finalCumulative := r.inboundConvDeleteCache.commitReplayResult(envelopeSender, requestID, newSurvivors, r.inboundConvDeleteSeen, now)
	log.Info().
		Str("envelope_sender", string(envelopeSender)).
		Str("request_id", string(requestID)).
		Str("status", string(status)).
		Int("deleted_this_attempt", deletedThisAttempt).
		Int("cumulative_deleted", finalCumulative).
		Int("remaining_survivors", len(newSurvivors)).
		Msg("dm_router: inbound conversation_delete partial-failure retry — re-attempted cached survivors only")
	r.replyConversationDeleteAck(envelopeSender, requestID, status, finalCumulative)
}

// processInboundConversationDeleteFreshGather handles the
// inboundClaimCold branch: no cached entry exists, no other
// handler is gathering, and we hold the in-progress slot. We
// gather candidates ONCE, sweep them, and commit either:
//
//   - real entry (gather + sweep both ran) with frozen
//     candidates / latest survivors / cumulative=this-attempt;
//   - gather-failed tombstone (chatlog read errored): empty
//     candidates / empty survivors / status=Error /
//     gatheredScope=false. Tombstone short-circuits all future
//     retries on this requestID — even after chatlog recovers,
//     because a recovered cold gather would pick up rows
//     authored after first contact.
//
// Both outcomes also record into the long-lived seen-set so the
// minimal outcome survives scope-cache reaping.
func (r *DMRouter) processInboundConversationDeleteFreshGather(envelopeSender domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, now time.Time) {
	candidates, ok := r.gatherInboundDeleteCandidates(envelopeSender)
	if !ok {
		// Plant the gather-failed tombstone: gatheredScope=false
		// freezes the entry and refuses every subsequent
		// retry's cold-gather attempt. lastStatus=Error and
		// cumulativeDeleted=0 echo to the sender; the sender
		// burns its retry budget on these Error replies and
		// abandons. The user can re-issue with a FRESH
		// requestID once the chatlog backend stabilises (a
		// fresh requestID is a different cache key, so the
		// tombstone for the failed requestID does not block
		// it).
		r.inboundConvDeleteCache.put(envelopeSender, requestID, nil, nil, domain.ConversationDeleteStatusError, 0, false, now)
		r.inboundConvDeleteSeen.record(envelopeSender, requestID, domain.ConversationDeleteStatusError, 0, false, now)
		log.Warn().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(requestID)).
			Msg("dm_router: inbound conversation_delete first apply — chatlog gather failed; planting gather-failed tombstone (gatheredScope=false)")
		r.replyConversationDeleteAck(envelopeSender, requestID, domain.ConversationDeleteStatusError, 0)
		return
	}
	if len(candidates) == 0 {
		// Empty conversation — applied/0 cached so subsequent
		// retries replay without re-iterating CURRENT chatlog.
		// A row that lands AFTER the first-apply gather is
		// outside the receiver's frozen scope (it was not in
		// candidates) AND outside the sender's localKnownIDs
		// snapshot (it had not committed locally before the
		// click drain). It therefore stays on this side; the
		// sender side's behaviour depends on local delivery
		// timing, with the same originator-only / both-keep
		// asymmetries documented in docs/dm-commands.md. No
		// symmetric "both sides converge" promise is made.
		r.inboundConvDeleteCache.put(envelopeSender, requestID, candidates, nil, domain.ConversationDeleteStatusApplied, 0, true, now)
		r.inboundConvDeleteSeen.record(envelopeSender, requestID, domain.ConversationDeleteStatusApplied, 0, true, now)
		log.Info().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(requestID)).
			Msg("dm_router: applied inbound conversation_delete (empty conversation); cached for retry replay")
		r.replyConversationDeleteAck(envelopeSender, requestID, domain.ConversationDeleteStatusApplied, 0)
		return
	}
	// Seed a placeholder cache entry BEFORE sweep so the inline
	// credit primitives (recordDeletion called from sweep,
	// recordDeletionAnyScope called from suppressIfWipeTombstoned)
	// can find a survivor set to drop the id from and a
	// cumulativeDeleted to bump. Without the seed there is no
	// entry yet, both primitives become no-ops, and the cumulative
	// bump for any id deleted during this first sweep is lost —
	// the eventual Applied ack would underreport rows the
	// receiver actually deleted.
	//
	// Placeholder shape: candidates frozen, survivors=clone of
	// candidates (everything is "yet to do" before sweep),
	// lastStatus=Error (will be settled by commit),
	// cumulativeDeleted=0, gatheredScope=true. The clone is
	// required — sweep iterates the candidates deep copy
	// independently, while recordDeletionAnyScope (fired by a
	// replay envelope landing during sweep) mutates the cache's
	// real survivors set; a shared map would race their
	// iterations.
	r.inboundConvDeleteCache.put(envelopeSender, requestID, candidates, cloneMessageIDSet(candidates), domain.ConversationDeleteStatusError, 0, true, now)

	deleted, survivors := r.sweepInboundDeleteScope(envelopeSender, requestID, candidates)

	// cumulativeDeleted has already been bumped INLINE — every
	// successful DeleteByID inside sweepInboundDeleteScope called
	// recordDeletion under cache.mu (single-source-of-truth).
	// commitReplayResult therefore only intersects newSurvivors
	// with the live survivor set (which may have shrunk further
	// via tombstone-path recordDeletionAnyScope calls during
	// sweep) and settles lastStatus to Applied iff the merged
	// survivor set is empty under gatheredScope=true. The
	// returned finalCumulative is read back from the live entry,
	// not computed from deletedThisAttempt.
	status, finalCumulative := r.inboundConvDeleteCache.commitReplayResult(envelopeSender, requestID, survivors, r.inboundConvDeleteSeen, now)
	log.Info().
		Str("envelope_sender", string(envelopeSender)).
		Str("request_id", string(requestID)).
		Str("status", string(status)).
		Int("candidates", len(candidates)).
		Int("deleted_this_attempt", deleted).
		Int("cumulative_deleted", finalCumulative).
		Int("survivors", len(survivors)).
		Msg("dm_router: applied inbound conversation_delete (first apply); scope frozen in cache + seen-set")
	r.replyConversationDeleteAck(envelopeSender, requestID, status, finalCumulative)
}

// wipeTombstoneTTL bounds how long a tombstone (the id of a row
// just removed by a conversation wipe) suppresses re-insertion of a
// re-delivered envelope for that id. The relay/inbox replay window
// is bounded by message_delete-style retries (~minutes); 1 hour is
// comfortably larger than any expected replay window without
// growing the set unboundedly.
const wipeTombstoneTTL = 1 * time.Hour

// wipeTombstoneReapPeriod is how often the reaper goroutine prunes
// stale entries. Independent of TTL — must be small enough that the
// peak set size stays bounded under heavy wipe activity.
const wipeTombstoneReapPeriod = 5 * time.Minute

// inboundConvDeleteCacheTTL bounds how long the receiver remembers
// a successful conversation_delete outcome for replay-without-resweep.
// Sender retry backoff caps at convDeleteRetryCap=300s with at most
// convDeleteRetryMaxAttempt=6 attempts; the worst-case window from
// the first dispatch to the final retry is well under 15 minutes.
// 30 minutes is comfortably larger and still keeps the cache bounded.
const inboundConvDeleteCacheTTL = 30 * time.Minute

// inboundConvDeleteCacheReapPeriod is how often the reaper goroutine
// prunes stale entries; sized like wipeTombstoneReapPeriod for
// symmetry.
const inboundConvDeleteCacheReapPeriod = 5 * time.Minute

// wipeTombstoneSet records ids of rows the wipe path just removed,
// with a TTL eviction model. The handler for new inbound messages
// (DMRouter.onNewMessage) consults this set on every event so a
// late-replayed envelope cannot resurrect a wiped row by silently
// re-inserting it through storeIncomingMessage.
type wipeTombstoneSet struct {
	mu      sync.Mutex
	entries map[domain.MessageID]time.Time // id → expiresAt
}

func newWipeTombstoneSet() *wipeTombstoneSet {
	return &wipeTombstoneSet{
		entries: make(map[domain.MessageID]time.Time),
	}
}

// Note inserts every id with an expiry of now+TTL. Called after a
// successful wipe (sender post-ack sweep AND receiver-side sweep)
// so both sides are protected against late re-delivery. A nil
// receiver is a no-op so test fixtures that do not need tombstone
// behaviour can leave the field unset without nil-panicking
// production call sites.
func (s *wipeTombstoneSet) Note(ids []domain.MessageID, now time.Time) {
	if s == nil || len(ids) == 0 {
		return
	}
	expiry := now.Add(wipeTombstoneTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.entries[id] = expiry
	}
}

// Has reports whether id is currently tombstoned (present and not
// expired). A nil receiver returns false so production code can
// guard with `if r.wipeTombstones != nil` paranoia.
func (s *wipeTombstoneSet) Has(id domain.MessageID, now time.Time) bool {
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

// reap drops every entry whose expiry has passed.
func (s *wipeTombstoneSet) reap(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, expiry := range s.entries {
		if !expiry.After(now) {
			delete(s.entries, id)
		}
	}
}

// inboundConversationDeleteKey identifies a single inbound
// conversation_delete request on the receiver side. The (peer,
// requestID) tuple is the load-bearing key: peer alone would
// collide with a fresh wipe-the-thread request from the same
// peer, and requestID alone would collide if two different peers
// happened to mint the same UUID.
type inboundConversationDeleteKey struct {
	peer      domain.PeerIdentity
	requestID domain.ConversationDeleteRequestID
}

// inboundConversationDeleteCacheEntry is the per-(peer, requestID)
// state the receiver keeps so that EVERY retry of the same wipe
// sweeps only the FROZEN scope captured on first apply — never the
// receiver's CURRENT chatlog. Load-bearing for three retry paths:
//
//   - Success replay (lost-ack retry): the original outcome and
//     cumulative count are echoed without touching chatlog. Without
//     this gate the retry would re-iterate current chatlog and
//     silently delete rows the peer added between the first apply
//     and the retry, expanding scope past the sender's
//     localKnownIDs snapshot.
//   - Partial-failure retry (some rows DeleteByID-failed in the
//     prior attempt): the receiver retries ONLY the cached
//     survivors. Rows added after the first apply are not in
//     candidates and are NOT touched, even though they would be
//     candidates by current-chatlog rules. cumulativeDeleted
//     accumulates each attempt's removed count so the final ack
//     reports total rows wiped on this side, not just the rows the
//     last attempt happened to clean up.
//   - Gather-failed tombstone (chatlog read errored on first
//     contact): gatheredScope=false freezes the entry as a
//     terminal Error/0 reply. Future retries SHORT-CIRCUIT on the
//     tombstone and refuse to gather again, even if chatlog has
//     since recovered — a recovered cold gather would pick up rows
//     authored between first contact and the retry, expanding
//     scope past the sender's localKnownIDs.
//
// Fields:
//
//   - candidates: the non-immutable IDs the FIRST apply identified
//     for deletion. Frozen on first apply, never extended. Empty
//     on a gather-failed tombstone.
//   - survivors: the subset of candidates whose DeleteByID failed
//     on the most recent attempt. Empty after a clean apply or on
//     a gather-failed tombstone (nothing to retry there). On
//     partial-failure retries success drops the ID from survivors,
//     idempotent already-gone (removed=false) also drops it,
//     real DeleteByID error keeps it for the next attempt.
//   - lastStatus: the outcome to echo back to the sender. Settles
//     to Applied once survivors is empty AND gatheredScope=true,
//     stays Error otherwise.
//   - cumulativeDeleted: total non-immutable rows this side has
//     wiped for this requestID across ALL attempts (NOT
//     per-attempt). The wire ack echoes this so a partial-failure
//     scenario like "first attempt removed 10 + retry removed 1"
//     reports 11 in the final applied ack instead of the
//     last-attempt 1.
//   - gatheredScope: true once gatherInboundDeleteCandidates
//     successfully ran for this requestID; false on a tombstone
//     planted by a gather failure. Replay paths consult this to
//     refuse a second cold-gather attempt — a recovered cold
//     gather would silently expand scope to rows that landed
//     after first contact, violating the localKnownIDs symmetry.
//   - cachedAt: TTL anchor for the reaper.
type inboundConversationDeleteCacheEntry struct {
	candidates        map[domain.MessageID]struct{}
	survivors         map[domain.MessageID]struct{}
	lastStatus        domain.ConversationDeleteStatus
	cumulativeDeleted int
	gatheredScope     bool
	cachedAt          time.Time
}

// inboundConversationDeleteCache pins the receiver-side scope of a
// conversation_delete by (peer, requestID). Used by
// handleInboundConversationDelete to (a) freeze the candidate set
// captured on the first apply and (b) replay the cached outcome on
// every subsequent retry against ONLY that frozen scope. The wipe
// of the receiver's chatlog is therefore at-most-once-per-id-per-
// requestID — rows the peer adds between attempts cannot be
// silently swept up because they are not in the cached candidates
// set.
//
// TTL eviction is the simplest fit: the receiver has no observable
// signal for "sender abandoned" and no incentive to keep the
// outcome forever. inboundConvDeleteCacheTTL (30 min) comfortably
// exceeds the worst-case sender retry window
// (~12 minutes at convDeleteRetryMaxAttempt=6 with cap=300s).
// inProgress closes the concurrent-cold-gather race: two inbound
// handlers for the SAME (peer, requestID) that both observe a cache
// miss must NOT both run gather + sweep — the second one would
// gather a chatlog state that already includes rows the first sweep
// removed (or rows the peer authored between first-contact-1 and
// first-contact-2), and either commit a stale outcome over the
// first one's freshly-cached entry or expand scope past
// localKnownIDs. The claim primitive (claimColdOrReplay) atomically
// peeks the entry map AND the in-progress map under one mutex hold:
// only one handler wins the cold-gather slot; subsequent handlers
// either replay the now-cached entry (if the winner already
// finished) or respond conservatively Error/0 (if the winner is
// still running). The conservative reply makes the sender retry,
// and by the time that retry lands the winner has either populated
// the cache (replay path) or planted a gather-failed tombstone
// (also replay path, with status=Error).
type inboundConversationDeleteCache struct {
	mu         sync.Mutex
	entries    map[inboundConversationDeleteKey]inboundConversationDeleteCacheEntry
	inProgress map[inboundConversationDeleteKey]struct{}
}

func newInboundConversationDeleteCache() *inboundConversationDeleteCache {
	return &inboundConversationDeleteCache{
		entries:    make(map[inboundConversationDeleteKey]inboundConversationDeleteCacheEntry),
		inProgress: make(map[inboundConversationDeleteKey]struct{}),
	}
}

// inboundClaimResult tells handleInboundConversationDelete which
// path to follow after a single atomic peek of cache state. See
// claimColdOrReplay for the decision matrix.
type inboundClaimResult int

const (
	// inboundClaimReplay: a live cached entry exists for this
	// (peer, requestID) AND we successfully took the in-progress
	// slot for it. Caller MUST process against the returned
	// entry (tombstone short-circuit / lost-ack replay /
	// partial-failure survivors retry) and MUST releaseClaim
	// exactly once when done. The returned entry's `survivors`
	// map is a DEEP COPY — safe to iterate outside the cache
	// mutex even while the inline credit primitives
	// (recordDeletion called from sweepInboundDeleteScope,
	// recordDeletionAnyScope called from
	// suppressIfWipeTombstoned) mutate the cache's real
	// survivors set in parallel.
	inboundClaimReplay inboundClaimResult = iota
	// inboundClaimCold: cache miss AND no concurrent handler is
	// processing this (peer, requestID). Caller now owns the
	// in-progress slot and MUST run cold-gather + put + then
	// releaseClaim exactly once. The eventual put records the
	// outcome (real entry on success, tombstone on gather
	// failure). releaseClaim drops the in-progress mark so a
	// future retry on this requestID can hit the put'd entry via
	// inboundClaimReplay.
	inboundClaimCold
	// inboundClaimContended: another handler is already
	// processing this (peer, requestID) — either Cold first
	// apply OR a live-cache Replay (partial-failure retry sweep
	// or lost-ack replay). Caller MUST NOT process, MUST NOT
	// releaseClaim, and MUST respond conservatively (Error/0).
	// The sender retries on the normal schedule; by the time
	// that retry lands the winner has either committed a
	// converged entry (next call resolves to inboundClaimReplay)
	// or planted a tombstone. Serialising every concurrent
	// processor through the in-progress slot is what closes
	// the data-race / lost-cumulativeDeleted hole that
	// existed when only Cold path was claimed: with two
	// concurrent Replay handlers iterating the same shared
	// survivors map and both committing back, one's
	// cumulativeDeleted overwrote the other's, AND the
	// concurrent map iteration was a Go data race waiting to
	// panic.
	inboundClaimContended
)

// claimColdOrReplay is the atomic gatekeeper for every path that
// touches an inboundConvDeleteCache entry. Under one mutex hold it
// consults the entry map and the in-progress map and returns one
// of three outcomes:
//
//   - live cached entry, no in-progress conflict → inboundClaimReplay.
//     The returned entry has a DEEP-COPIED survivors map so the
//     caller can sweep it without holding the cache mutex even
//     while the inline credit primitives (recordDeletion,
//     recordDeletionAnyScope) are mutating the cache's real
//     survivors set in parallel. The in-progress slot is taken
//     so concurrent retries serialise into Contended.
//   - cache miss / TTL-expired AND not in-progress →
//     inboundClaimCold. Caller now owns the in-progress slot;
//     candidates is nil (cold path will gather it).
//   - in-progress by another handler (whether Cold or Replay) →
//     inboundClaimContended. Caller backs off.
//
// Without this serialisation, two concurrent Replay handlers each
// (a) read the same shared cache entry (including the same
// survivors map reference), (b) iterate that map outside the
// cache mutex while sweepInboundDeleteScope runs, and (c) commit
// independently — any of which is a data race or a
// cumulative-progress overwrite. The Cold path needed the same
// serialisation for a different reason (preventing two cold
// gathers against possibly-different chatlog snapshots from
// silently expanding scope).
//
// nil receiver returns inboundClaimCold without recording
// anything so test fixtures that omit the cache still exercise
// the first-apply code path. Callers MUST guard nil-receiver
// releaseClaim accordingly.
func (c *inboundConversationDeleteCache) claimColdOrReplay(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, now time.Time, ttl time.Duration) (entry inboundConversationDeleteCacheEntry, result inboundClaimResult) {
	if c == nil {
		return inboundConversationDeleteCacheEntry{}, inboundClaimCold
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := inboundConversationDeleteKey{peer: peer, requestID: requestID}
	if _, busy := c.inProgress[key]; busy {
		return inboundConversationDeleteCacheEntry{}, inboundClaimContended
	}
	if existing, ok := c.entries[key]; ok {
		if now.Sub(existing.cachedAt) <= ttl {
			c.inProgress[key] = struct{}{}
			// Deep-copy the survivors map: replay sweeps it
			// outside the cache mutex, while the inline credit
			// primitives (recordDeletion called from sweep,
			// recordDeletionAnyScope called from
			// suppressIfWipeTombstoned) mutate the cache's real
			// survivors set. Without the copy the iteration is
			// a Go data race. candidates is read-only after
			// first apply, so a shared reference is fine
			// (replay does not iterate it).
			snapshot := existing
			snapshot.survivors = cloneMessageIDSet(existing.survivors)
			return snapshot, inboundClaimReplay
		}
		// TTL miss: drop the stale entry and fall through to
		// the cold-gather claim below. The handler's seen-set
		// check runs INSIDE the inboundClaimCold branch, NOT
		// before this call — load-bearing ordering: a live
		// cache entry must always win over the seen-set, which
		// would otherwise short-circuit a partial-failure
		// retry with cached Error/N. Reaching this branch
		// means the scope-cache TTL elapsed; the seen-set
		// then short-circuits the cold-gather attempt with
		// its MINIMAL outcome and refuses a fresh chatlog
		// read, preserving localKnownIDs symmetry across
		// long wall-clock pauses.
		delete(c.entries, key)
	}
	c.inProgress[key] = struct{}{}
	return inboundConversationDeleteCacheEntry{}, inboundClaimCold
}

// cloneMessageIDSet returns an independent copy of the given map.
// nil → nil so callers can detect "no scope" without an extra
// branch. Used to insulate inboundClaimReplay's caller from
// concurrent mutations of the cache's real survivors set by the
// inline credit primitives (recordDeletion called from sweep,
// recordDeletionAnyScope called from suppressIfWipeTombstoned).
func cloneMessageIDSet(src map[domain.MessageID]struct{}) map[domain.MessageID]struct{} {
	if src == nil {
		return nil
	}
	out := make(map[domain.MessageID]struct{}, len(src))
	for k := range src {
		out[k] = struct{}{}
	}
	return out
}

// commitReplayResult is the survivor-merge hook used by BOTH the
// partial-failure replay path AND the cold first-apply path (the
// latter seeds a placeholder entry via put before sweep, then
// commits via this method). Under one mutex hold it:
//
//   - intersects the caller's newSurvivors with the LIVE
//     existing.survivors so any id recordDeletion(/AnyScope)
//     already cleared (and credited) cannot be resurrected by
//     writing newSurvivors back wholesale;
//   - settles lastStatus to Applied iff the intersected
//     survivor set is empty AND gatheredScope=true (a
//     gather-failed tombstone keeps its permanent Error);
//   - refreshes cachedAt;
//   - mirrors the (status, cumulativeDeleted) outcome to the
//     seen-set.
//
// cumulativeDeleted is NOT touched here — recordDeletion (called
// inline by sweep on each successful DeleteByID) and
// recordDeletionAnyScope (called inline by suppressIfWipeTombstoned
// on each successful re-DELETE) are the SOLE writers of
// cumulativeDeleted. The "credit each id at most once" invariant
// is NOT a database property — a tombstoned replay can re-insert
// the row after the original delete and a second DeleteByID can
// then return removed=true on the same id. The actual guarantee
// is at the SURVIVOR-SET level under cache.mu: every successful
// DeleteByID routes through recordDeletion(/AnyScope), which
// performs an atomic "if id in survivors → delete from survivors,
// bump cumulative; else no-op". Whichever path takes the cache
// mutex first while id is still a survivor wins ownership and
// credits exactly once; every subsequent winning DeleteByID for
// that same id finds the survivor already cleared and bumps
// nothing. Without that single-source-of-truth invariant, the
// older "deferred bump in commit + concurrent inline tombstone
// bump" design double-counted any id whose replay envelope
// landed during sweep.
//
// Returns the (status, finalCumulative) pair the caller echoes on
// the wire ack. Caller MUST hold the corresponding inProgress
// slot (claimed via claimColdOrReplay); commit does not release
// the slot — releaseClaim is paired with claim by the caller via
// defer.
func (c *inboundConversationDeleteCache) commitReplayResult(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, newSurvivors map[domain.MessageID]struct{}, seen *inboundConversationDeleteSeenSet, now time.Time) (status domain.ConversationDeleteStatus, finalCumulative int) {
	if c == nil {
		// Defensive: without a cache there is nothing to merge;
		// derive a conservative status purely from the survivor
		// set the caller computed. cumulative=0 is also
		// conservative — without the cache there is no
		// per-attempt tracking to read from.
		if len(newSurvivors) == 0 {
			return domain.ConversationDeleteStatusApplied, 0
		}
		return domain.ConversationDeleteStatusError, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := inboundConversationDeleteKey{peer: peer, requestID: requestID}
	existing, ok := c.entries[key]
	if !ok {
		// Should not happen under normal flow — claimColdOrReplay
		// returned a non-Contended outcome and the inProgress
		// slot prevents reaping. If we still see a miss, fall
		// back to a fresh entry that at least records what we
		// know about the survivors.
		gatheredScope := true
		s := domain.ConversationDeleteStatusApplied
		if len(newSurvivors) > 0 {
			s = domain.ConversationDeleteStatusError
		}
		c.entries[key] = inboundConversationDeleteCacheEntry{
			survivors:         newSurvivors,
			lastStatus:        s,
			cumulativeDeleted: 0,
			gatheredScope:     gatheredScope,
			cachedAt:          now,
		}
		seen.record(peer, requestID, s, 0, gatheredScope, now)
		return s, 0
	}
	// Intersect newSurvivors with existing.survivors so an id
	// that recordDeletion / recordDeletionAnyScope already
	// cleared cannot be resurrected by writing newSurvivors
	// back. existing.survivors is the authoritative live set
	// under our mutex; both existing.survivors and newSurvivors
	// are subsets of the original survivor scope frozen at
	// first apply, so the intersection is well-defined.
	mergedSurvivors := make(map[domain.MessageID]struct{}, len(newSurvivors))
	for id := range newSurvivors {
		if _, ok := existing.survivors[id]; ok {
			mergedSurvivors[id] = struct{}{}
		}
	}
	settled := domain.ConversationDeleteStatusApplied
	if !existing.gatheredScope {
		// Tombstone (gather-failed) is a permanent Error;
		// partial-failure replay should not transition it.
		settled = existing.lastStatus
	} else if len(mergedSurvivors) > 0 {
		settled = domain.ConversationDeleteStatusError
	}
	c.entries[key] = inboundConversationDeleteCacheEntry{
		candidates:        existing.candidates,
		survivors:         mergedSurvivors,
		lastStatus:        settled,
		cumulativeDeleted: existing.cumulativeDeleted,
		gatheredScope:     existing.gatheredScope,
		cachedAt:          now,
	}
	seen.record(peer, requestID, settled, existing.cumulativeDeleted, existing.gatheredScope, now)
	return settled, existing.cumulativeDeleted
}

// recordDeletion is the SINGLE atomic credit primitive for any
// path that successfully deletes a row of an in-flight
// conversation_delete scope. Both call sites:
//
//   - sweepInboundDeleteScope, immediately after its own
//     DeleteByID(id) returned removed=true (it knows the
//     (peer, requestID) of the scope it's running on);
//   - suppressIfWipeTombstoned, immediately after its own
//     DeleteByID(id) returned removed=true (it does NOT know
//     the requestID, so it goes through the recordDeletionAnyScope
//     variant below — a linear scan of cache entries that
//     applies the same drop-and-credit semantics to whichever
//     entry holds the id in its survivors set).
//
// Under one mutex hold the method drops id from the entry's
// survivors set, bumps cumulativeDeleted by 1, settles
// lastStatus to Applied if survivors emptied AND
// gatheredScope=true, refreshes cachedAt, and mirrors the new
// outcome to the seen-set. Idempotent at the (peer, requestID, id)
// granularity: the second call (whichever path ran second) finds
// id no longer in survivors and returns false WITHOUT bumping
// cumulative again. This is what closes the double-count window
// — both paths can legitimately observe DeleteByID returning
// removed=true on the same id (sweep deletes the original, then
// a relay-replayed envelope re-inserts the row, and
// suppressIfWipeTombstoned's DeleteByID also returns true), but
// only the first call into recordDeletion under cache.mu finds
// id still in survivors and credits; every subsequent call for
// that id is a no-op at the survivor-set level. The
// single-credit invariant lives in this function, NOT in the
// chatlog store.
//
// Returns true when this call took ownership and credited; false
// when id was already missing from survivors (someone else
// credited or the entry has been reaped). nil receiver returns
// false.
func (c *inboundConversationDeleteCache) recordDeletion(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, id domain.MessageID, seen *inboundConversationDeleteSeenSet, now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := inboundConversationDeleteKey{peer: peer, requestID: requestID}
	entry, ok := c.entries[key]
	if !ok {
		return false
	}
	if _, present := entry.survivors[id]; !present {
		return false
	}
	delete(entry.survivors, id)
	entry.cumulativeDeleted++
	if entry.gatheredScope && len(entry.survivors) == 0 {
		entry.lastStatus = domain.ConversationDeleteStatusApplied
	}
	entry.cachedAt = now
	c.entries[key] = entry
	seen.record(key.peer, key.requestID, entry.lastStatus, entry.cumulativeDeleted, entry.gatheredScope, now)
	return true
}

// recordDeletionAnyScope is the suppressIfWipeTombstoned variant
// of recordDeletion: tombstone-path callers do NOT have a
// (peer, requestID) key (wipeTombstones is keyed by id alone), so
// the method does a linear scan of cache entries for any whose
// survivors set contains id. Each matching entry is updated under
// the same one-shot semantics as recordDeletion. Linear scan is
// acceptable: the cache size is bounded by
// inboundConvDeleteCacheTTL × wipe rate, and tombstone re-DELETEs
// are rare.
//
// The scan deliberately updates EVERY matching entry, not just
// one. Multiple entries can legitimately hold the same id in
// their survivors sets at the same time: an abandoned (or
// retry-budget-exhausted) older requestID can leave a cache
// entry with id still in survivors until inboundConvDeleteCacheTTL
// expires, while a fresh user-issued wipe under a NEW requestID
// freezes a candidate set that overlaps the old one. A
// tombstone re-DELETE that successfully removes id should
// credit BOTH cache entries — the freshly-active wipe converges
// (cumulativeDeleted bumps, survivors shrinks toward Applied),
// and the abandoned-but-still-cached entry stays internally
// consistent (the row is genuinely gone from chatlog, so the
// abandoned entry's survivors must reflect that even though no
// retry will read it again before it's reaped). Iterating all
// matches also removes any need to plumb a (peer, requestID)
// hint through the suppression path.
func (c *inboundConversationDeleteCache) recordDeletionAnyScope(id domain.MessageID, seen *inboundConversationDeleteSeenSet, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if _, ok := entry.survivors[id]; !ok {
			continue
		}
		delete(entry.survivors, id)
		entry.cumulativeDeleted++
		if entry.gatheredScope && len(entry.survivors) == 0 {
			entry.lastStatus = domain.ConversationDeleteStatusApplied
		}
		entry.cachedAt = now
		c.entries[key] = entry
		seen.record(key.peer, key.requestID, entry.lastStatus, entry.cumulativeDeleted, entry.gatheredScope, now)
	}
}

// releaseClaim drops the in-progress mark planted by a successful
// inboundClaimCold from claimColdOrReplay. MUST be called exactly
// once per claim, typically via defer right after the claim wins.
// nil receiver / unknown key are no-ops so the call site can
// release unconditionally without checking the result.
func (c *inboundConversationDeleteCache) releaseClaim(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inProgress, inboundConversationDeleteKey{peer: peer, requestID: requestID})
}

// put writes a fresh entry for (peer, requestID), replacing any
// existing one. Used in two narrow situations:
//
//   - cold first-apply seed: caller plants a placeholder before
//     sweep with candidates frozen, survivors=clone(candidates),
//     status=Error (placeholder, settled by commitReplayResult),
//     cumulativeDeleted=0, gatheredScope=true. The seed gives
//     the inline credit primitives (recordDeletion called from
//     sweep, recordDeletionAnyScope called from
//     suppressIfWipeTombstoned) an entry whose survivor set
//     they can drop from and whose cumulativeDeleted they can
//     bump.
//   - gather-failed tombstone: caller plants empty
//     candidates/survivors, status=Error, cumulativeDeleted=0,
//     gatheredScope=false. Future retries hit Replay, the
//     tombstone short-circuit refuses a second cold gather.
//   - empty-conversation first apply: caller plants
//     candidates=nil/empty survivors, status=Applied,
//     cumulativeDeleted=0, gatheredScope=true. Future retries
//     replay the cached Applied/0.
//
// Production callers ALWAYS pass cumulativeDeleted=0 here.
// cumulativeDeleted is the running total across attempts and is
// owned EXCLUSIVELY by recordDeletion / recordDeletionAnyScope
// (single-source-of-truth, bumped inline under cache.mu); put
// would clobber that running total if used for anything other
// than the initial seed of a fresh entry. nil receiver is a
// no-op.
func (c *inboundConversationDeleteCache) put(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, candidates, survivors map[domain.MessageID]struct{}, status domain.ConversationDeleteStatus, cumulativeDeleted int, gatheredScope bool, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[inboundConversationDeleteKey{peer: peer, requestID: requestID}] = inboundConversationDeleteCacheEntry{
		candidates:        candidates,
		survivors:         survivors,
		lastStatus:        status,
		cumulativeDeleted: cumulativeDeleted,
		gatheredScope:     gatheredScope,
		cachedAt:          now,
	}
}

// reap drops every entry older than ttl that is NOT currently
// claimed by an in-flight handler. Skipping in-progress keys is
// load-bearing: claimColdOrReplay marks (peer, requestID) as
// in-progress around sweepInboundDeleteScope and the subsequent
// commitReplayResult, but a slow chatlog (or simply an entry
// that was claimed near its TTL boundary) could let the reaper
// drop the entry mid-sweep. recordDeletion's
// drop-from-survivors-and-bump would then miss the entry and
// silently lose credit; commitReplayResult would fall back to
// its missing-entry branch and write a fresh entry with
// cumulativeDeleted=0, replying Applied/0 to the sender even
// though rows were actually deleted. Pinning in-progress keys
// against reaping closes that hole.
//
// Reaping resumes for a stranded key once releaseClaim drops the
// inProgress mark on the next tick (handlers always defer
// release). Stranded inProgress is bounded by handler runtime
// (sweep + commit), which is on the order of seconds.
func (c *inboundConversationDeleteCache) reap(now time.Time, ttl time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if _, busy := c.inProgress[key]; busy {
			continue
		}
		if now.Sub(entry.cachedAt) > ttl {
			delete(c.entries, key)
		}
	}
}

// inboundConvDeleteCacheReaperLoop runs in a dedicated goroutine
// launched from Start(). Mirrors wipeTombstoneReaperLoop — both
// caches are bounded by the same TTL-based eviction model.
func (r *DMRouter) inboundConvDeleteCacheReaperLoop(ctx context.Context) {
	defer recoverLog("inboundConvDeleteCacheReaperLoop")
	if r.inboundConvDeleteCache == nil {
		return
	}
	ticker := time.NewTicker(inboundConvDeleteCacheReapPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.inboundConvDeleteCache.reap(now.UTC(), inboundConvDeleteCacheTTL)
		}
	}
}

// inboundConvDeleteSeenTTL bounds how long a seen-set entry remains
// FRESH (replay returns the recorded outcome — Applied/cumulative
// or Error/0 per the wire contract). Beyond this TTL the entry is
// EXPIRED but still present until the reaper purges it; an
// expired-but-present entry is replayed CONSERVATIVELY as Error/0
// so the receiver still refuses a fresh cold-gather (which would
// silently widen scope past the sender's localKnownIDs).
//
// Sized at 1 year to make the "expired-but-present" window cover
// any realistic laptop-sleep / process-suspend duration plus a
// large safety margin. Sender pending state is in-memory only and
// attempt-bounded (~30 minutes worth of dispatches), but
// wall-clock time advances during sleep, so a process that
// suspends mid-retry-window can resume long after the original
// first-contact and fire one final retry. The seen-set stays
// available to recognise that requestID and reply Error/0 instead
// of falling into the cold-gather scope-widening hole.
//
// Memory cost is negligible — one (key + 3 small fields +
// timestamp) per unique applied/failed wipe per peer, bounded by
// user-initiated wipe rate.
const inboundConvDeleteSeenTTL = 365 * 24 * time.Hour

// inboundConvDeleteSeenReapPeriod is how often the reaper goroutine
// purges entries beyond TTL. Reap cadence is independent of the
// per-lookup expiration check — lookup never lazy-deletes; the
// reaper alone bounds memory.
const inboundConvDeleteSeenReapPeriod = 24 * time.Hour

// inboundConversationDeleteSeenEntry is the MINIMAL outcome the
// receiver remembers for a (peer, requestID) long after the heavy
// scope cache has been reaped. Carries just enough to refuse a
// fresh cold-gather AND replay a consistent outcome:
//
//   - status: the LATEST minimal-replay status the receiver
//     would echo for this requestID right now (applied or
//     error). NOT terminal at the cache level — while the
//     live scope cache still holds the entry, a partial-failure
//     first apply records seen.status=Error and a follow-up
//     survivor retry can promote it to Applied via
//     recordDeletion's Error→Applied transition (mirrored to
//     the seen-set in lockstep). Once the scope cache is
//     reaped, the seen-set freezes whatever status was last
//     recorded — typically Applied for a converged wipe or
//     Error for a gather-failed tombstone or an abandoned
//     partial failure.
//   - cumulativeDeleted: the total count this side wiped for this
//     requestID across all attempts. Echoed on the wire ONLY for
//     Applied seen replays (replyInboundConversationDeleteFromSeen
//     sends Deleted=cumulativeDeleted only when status=Applied; a
//     non-Applied / Error seen replay always echoes Deleted=0 to
//     match the documented wire contract). The field is kept on
//     the seen entry regardless — recordDeletion mirrors every
//     bump and Error→Applied transition into the seen entry
//     IMMEDIATELY (during the live-cache phase, while there is
//     still a frozen scope to sweep), so the seen entry stays
//     monotonic with the cache. Once the cache is reaped the
//     value freezes at whatever was last mirrored; an Error
//     seen entry can no longer transition to Applied at the
//     fallback layer (no scope, no retry to credit), but its
//     cumulativeDeleted survives for diagnostics.
//   - gatheredScope: false on a gather-failed tombstone (replay
//     path also short-circuits at the seen-set level — no
//     attempt to re-gather even after scope cache reap). True
//     otherwise.
//   - seenAt: TTL anchor.
type inboundConversationDeleteSeenEntry struct {
	status            domain.ConversationDeleteStatus
	cumulativeDeleted int
	gatheredScope     bool
	seenAt            time.Time
}

// inboundConversationDeleteSeenSet is the long-lived companion to
// inboundConversationDeleteCache. Same key, much longer TTL, much
// smaller value. Consulted as a FALLBACK by
// handleInboundConversationDelete — only after the live scope cache
// reports a miss (claimColdOrReplay returns inboundClaimCold). A
// hit replays the minimal outcome without touching chatlog or
// running a fresh gather. The live cache MUST be checked first:
// after a partial-failure first apply both the cache (with
// non-empty survivors, status=Error) and the seen-set
// (status=Error, cumulativeDeleted=N) carry an entry, and a
// seen-first lookup would short-circuit the very retry that is
// supposed to sweep the survivors. Every successful first-apply
// / gather-failed tombstone / converged partial-failure retry
// records here in addition to the scope cache; that way once
// the scope cache TTL expires the seen-set keeps the contract
// intact.
//
// Without the seen-set, a sender retry that arrives more than
// inboundConvDeleteCacheTTL after the original first-contact would
// hit a clean cache miss AND a clean seen miss, and the cold-gather
// path would re-read CURRENT chatlog — silently sweeping rows the
// peer authored in the intervening period and breaking the
// localKnownIDs symmetry.
type inboundConversationDeleteSeenSet struct {
	mu sync.Mutex
	// entries holds the full minimal-outcome data (status,
	// cumulative, gathered, seenAt). Reaped after
	// inboundConvDeleteSeenTTL.
	entries map[inboundConversationDeleteKey]inboundConversationDeleteSeenEntry
	// tombstones holds key-only markers for (peer, requestID) pairs
	// the receiver has DEFINITELY seen at some point but whose
	// full entry was reaped past TTL. Lookup-against-tombstone is
	// distinguishable from never-seen and triggers the same
	// conservative Error/0 reply as an expired-but-present entry.
	// This closes the residual "sender pending state outlives our
	// seen TTL" hole: even years after first contact, a late
	// retry's requestID is still recognisable and refused a fresh
	// cold-gather (which would silently widen scope past the
	// sender's localKnownIDs). Memory cost is one map key per
	// unique requestID ever observed by this process —
	// in-memory only, lost on restart, bounded by user-driven
	// wipe rate.
	tombstones map[inboundConversationDeleteKey]struct{}
}

func newInboundConversationDeleteSeenSet() *inboundConversationDeleteSeenSet {
	return &inboundConversationDeleteSeenSet{
		entries:    make(map[inboundConversationDeleteKey]inboundConversationDeleteSeenEntry),
		tombstones: make(map[inboundConversationDeleteKey]struct{}),
	}
}

// lookup returns the cached minimal outcome for (peer, requestID).
// Three outcomes:
//
//   - present=false: nothing has EVER been recorded for this
//     (peer, requestID) — neither in the full-payload entries
//     map nor in the key-only tombstones map. Caller treats
//     this as a true fresh first contact. (A previously known
//     requestID does NOT fall back into this branch when the
//     reaper purges its full payload past TTL — the reaper
//     transfers the key to tombstones, which the next branch
//     reports as present-but-not-fresh.)
//   - present=true, fresh=true: entry exists and is within TTL.
//     Caller replays per the wire contract — Applied/cumulative
//     for status=Applied, Error/0 otherwise.
//   - present=true, fresh=false: the (peer, requestID) is
//     known but its full payload is no longer authoritative.
//     Two underlying cases, indistinguishable to the caller:
//     (a) the entry still has its full payload but has aged
//     past TTL and the reaper has not yet moved it to
//     tombstones; entry.seenAt carries the original timestamp,
//     entry.status / cumulativeDeleted are populated.
//     (b) the reaper has dropped the full payload and only the
//     key remains in tombstones; the returned entry is the
//     zero value (seenAt is the zero time, status empty).
//     Either way the caller MUST reply CONSERVATIVELY (Error/0)
//     and MUST NOT cold-gather. This is the load-bearing
//     guarantee: a sender whose pending entry survives a
//     wall-clock pause longer than the seen TTL (laptop sleep)
//     and fires a final retry on resume still finds its
//     requestID known and is refused a fresh gather (which
//     would silently widen scope past the sender's
//     localKnownIDs). Tombstones are NEVER reaped on their own
//     — they live until process restart, matching the
//     in-memory-only restart semantics of the rest of the
//     seen-set. The lookup deliberately does NOT lazy-delete
//     expired entries either; the reaper alone moves them into
//     tombstones.
//
// nil receiver returns (_, false, false). The returned entry is
// a value copy — safe to read after the call returns.
func (s *inboundConversationDeleteSeenSet) lookup(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, now time.Time, ttl time.Duration) (entry inboundConversationDeleteSeenEntry, present bool, fresh bool) {
	if s == nil {
		return inboundConversationDeleteSeenEntry{}, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := inboundConversationDeleteKey{peer: peer, requestID: requestID}
	if raw, ok := s.entries[key]; ok {
		if now.Sub(raw.seenAt) > ttl {
			return raw, true, false
		}
		return raw, true, true
	}
	if _, tombstoned := s.tombstones[key]; tombstoned {
		// Reaper has dropped the full entry but kept the
		// key-only tombstone. Treat as expired-but-present so
		// the caller refuses a fresh cold-gather.
		return inboundConversationDeleteSeenEntry{}, true, false
	}
	return inboundConversationDeleteSeenEntry{}, false, false
}

// record inserts or updates the minimal outcome for (peer,
// requestID). Called every time handleInboundConversationDelete
// commits a status: the seen-set tracks the LATEST observed
// outcome so a partial-failure retry that converges to Applied
// updates the seen-set from Error to Applied alongside the
// cumulativeDeleted bump. nil receiver is a no-op.
func (s *inboundConversationDeleteSeenSet) record(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, status domain.ConversationDeleteStatus, cumulativeDeleted int, gatheredScope bool, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := inboundConversationDeleteKey{peer: peer, requestID: requestID}
	// Defensive: clear any tombstone for this key so a fresh
	// record after a hypothetical re-issue (would require a
	// requestID collision under in-memory tombstones, so the
	// branch is essentially dead, but kept consistent so
	// lookup never sees both an entry AND a tombstone for the
	// same key).
	delete(s.tombstones, key)
	s.entries[key] = inboundConversationDeleteSeenEntry{
		status:            status,
		cumulativeDeleted: cumulativeDeleted,
		gatheredScope:     gatheredScope,
		seenAt:            now,
	}
}

// reap moves every entry whose age exceeds ttl from the full
// entries map to the key-only tombstones map. The full payload
// (status, cumulative, gathered, seenAt) is discarded — only the
// key remains as a "we've seen this requestID" marker. Tombstones
// are NOT reaped on their own cadence; they live until process
// restart, matching the in-memory-only retention model of the
// rest of the seen-set. Dropping the payload but keeping the key
// is what closes the residual hole flagged by reviewer: a sender
// whose pending state survives a wall-clock pause longer than
// the seen TTL (laptop sleep) and fires a final retry still
// finds its requestID known on the receiver and is refused a
// fresh cold-gather (which would silently widen scope past its
// localKnownIDs). Memory cost of a tombstone is just the key
// (~50 bytes); bounded by the user-driven wipe rate over process
// lifetime.
func (s *inboundConversationDeleteSeenSet) reap(now time.Time, ttl time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.entries {
		if now.Sub(entry.seenAt) > ttl {
			delete(s.entries, key)
			s.tombstones[key] = struct{}{}
		}
	}
}

// inboundConvDeleteSeenReaperLoop is the long-lived counterpart to
// inboundConvDeleteCacheReaperLoop. Runs at a coarser cadence
// because the seen-set is small and TTL is long.
func (r *DMRouter) inboundConvDeleteSeenReaperLoop(ctx context.Context) {
	defer recoverLog("inboundConvDeleteSeenReaperLoop")
	if r.inboundConvDeleteSeen == nil {
		return
	}
	ticker := time.NewTicker(inboundConvDeleteSeenReapPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.inboundConvDeleteSeen.reap(now.UTC(), inboundConvDeleteSeenTTL)
		}
	}
}

// wipeTombstoneReaperLoop runs in a dedicated goroutine launched
// from Start(). It periodically prunes expired tombstones so the
// set stays bounded during long-running processes.
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
			r.wipeTombstones.reap(now.UTC())
		}
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
	if !r.wipeTombstones.Has(id, time.Now().UTC()) {
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
	removed, err := store.DeleteByID(id)
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

	if r.fileBridge != nil {
		r.fileBridge.OnMessageDeleted(id)
	}
	r.evictWipedConversationFromUI(r.peerForMessage(event), []domain.MessageID{id})

	// If we actually removed the row (not the idempotent
	// already-gone case), credit the deletion to the
	// inbound-conv-delete cache entry that owns this id via
	// recordDeletionAnyScope: that primitive atomically drops
	// the id from the entry's survivors set AND bumps
	// cumulativeDeleted under cache.mu. cumulativeDeleted is
	// the SINGLE source of truth — sweepInboundDeleteScope
	// uses the same primitive (recordDeletion) inline for its
	// own DeleteByID(success) ids, and recordDeletion's
	// drop-from-survivors is idempotent at the id level so a
	// concurrent sweep of the same id finds nothing to credit
	// and skips. removed==false means another path's
	// DeleteByID won the database race and that path's own
	// recordDeletion call already credited the row, so we
	// skip — there is no "split tally" any more.
	if removed {
		r.inboundConvDeleteCache.recordDeletionAnyScope(id, r.inboundConvDeleteSeen, time.Now().UTC())
	}

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

// snapshotLocalKnownConversationIDs reads the conversation with peer
// and returns the IDs of every non-immutable row whose state is
// confirmed-known to the peer side. Used by
// CompleteConversationDelete to capture the local click-time view
// of the thread; the resulting set lives on the pending entry only
// and never leaves this process.
//
// Inclusion rule:
//   - Inbound rows (Sender == peer): always included. The peer
//     authored them, so the peer obviously has them — they are
//     in the peer's first-contact gather scope and will be wiped
//     on the receiver side.
//   - Outbound rows (Sender == self): included ONLY when
//     DeliveryStatus is "delivered" or "seen". A "sent" outbound
//     row is one the local node accepted but the peer node has
//     not yet confirmed receipt for. The CompleteConversationDelete
//     drain step waits for the SendMessage / SendFileAnnounce
//     goroutine to finish, but those goroutines return on
//     LOCAL handoff (chatlog.send_message reply) — not on peer
//     receipt. A "sent" row may therefore reach the peer AFTER
//     conversation_delete arrives there: the peer's gather
//     ran without it (so the peer side won't wipe it), and if
//     we put it in localKnownIDs the post-ack mirror sweep
//     would delete it on our side, leaving a receiver-only row.
//     Excluding it from the snapshot keeps it on our side; if
//     the peer eventually receives the data DM after its wipe
//     gather, both sides will hold the row symmetrically. If
//     the peer happens to receive it BEFORE the gather (the
//     queue happens to deliver out of order under the same
//     wire), the peer's gather wipes it and we keep it — the
//     same originator-only asymmetry the design already
//     accepts for inbound rows that land late on this side
//     (see the dm_command.go payload comment for the full
//     trade-off).
//   - Immutable rows: skipped regardless (matches the wipe
//     scope rule on both sides — immutable rows survive
//     conversation_delete).
//
// Returns (nil, nil) when the conversation has no rows or only
// immutable rows. Read errors propagate so the caller can refuse
// the send and let the user retry once the underlying store is
// healthy.
func (r *DMRouter) snapshotLocalKnownConversationIDs(ctx context.Context, peer domain.PeerIdentity) (map[domain.MessageID]struct{}, error) {
	store := r.client.chatlog.Store()
	if store == nil {
		return nil, fmt.Errorf("chatlog store is not available")
	}
	entries, err := store.ReadCtx(ctx, "dm", peer)
	if err != nil {
		return nil, fmt.Errorf("snapshot local known ids %s: %w", peer, err)
	}
	if len(entries) == 0 {
		return nil, nil
	}
	myAddr := r.client.Address()
	out := make(map[domain.MessageID]struct{}, len(entries))
	for _, e := range entries {
		if protocol.MessageFlag(e.Flag) == protocol.MessageFlagImmutable {
			continue
		}
		// Outbound rows whose peer-receipt is not yet confirmed
		// are excluded from the snapshot — see the function
		// comment for the receiver-only-row hole this closes.
		// "sent" is the locally-accepted-but-undelivered state;
		// "delivered" / "seen" both confirm peer-node receipt
		// (delivered = peer node ACK'd, seen = peer user
		// opened) so either is sufficient for snapshot
		// inclusion. An outbound row whose DeliveryStatus is
		// empty is treated as "sent" (legacy rows that
		// pre-date the receipt mechanism — safer to exclude).
		if domain.PeerIdentity(e.Sender) == myAddr {
			if e.DeliveryStatus != chatlog.StatusDelivered && e.DeliveryStatus != chatlog.StatusSeen {
				continue
			}
		}
		out[domain.MessageID(e.ID)] = struct{}{}
	}
	return out, nil
}

// gatherInboundDeleteCandidates reads the receiver's CURRENT chatlog
// for envelopeSender and returns the non-immutable IDs as a frozen
// candidate set. Called ONCE per (peer, requestID) on the first
// apply; subsequent retries do NOT re-read chatlog — they sweep only
// the cached candidate set so rows added between attempts cannot
// silently expand the wipe scope past the sender's localKnownIDs
// snapshot.
//
// Returns ok=false when the chatlog read failed.
// processInboundConversationDeleteFreshGather translates that into
// a gatheredScope=false TOMBSTONE entry in the cache + seen-set
// rather than a no-cache outcome: every subsequent retry of the
// same (peer, requestID) hits the tombstone via inboundClaimReplay
// (or via the seen-set fallback after scope-cache reap) and replies
// Error/0 WITHOUT re-attempting gather. Re-attempting gather after
// the chatlog backend recovers would silently expand scope to rows
// that landed between first contact and the recovered gather,
// breaking the sender's localKnownIDs symmetry. The user can
// re-issue the wipe with a FRESH requestID once the backend is
// stable; that requestID is a different cache key, so the
// tombstone for the failed requestID does not block it.
func (r *DMRouter) gatherInboundDeleteCandidates(envelopeSender domain.PeerIdentity) (map[domain.MessageID]struct{}, bool) {
	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: gatherInboundDeleteCandidates: chatlog store unavailable")
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entries, err := store.ReadCtx(ctx, "dm", envelopeSender)
	if err != nil {
		log.Warn().Err(err).
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: gatherInboundDeleteCandidates: chatlog read failed")
		return nil, false
	}
	out := make(map[domain.MessageID]struct{}, len(entries))
	for _, e := range entries {
		if protocol.MessageFlag(e.Flag) == protocol.MessageFlagImmutable {
			continue
		}
		out[domain.MessageID(e.ID)] = struct{}{}
	}
	return out, true
}

// sweepInboundDeleteScope iterates a frozen scope set (NOT current
// chatlog) and tries DeleteByID for each id. Returns:
//
//   - removedThisAttempt: how many IDs were successfully removed
//     from chatlog by this call (excludes idempotent already-gone
//     rows where DeleteByID returned removed=false).
//   - survivors: the subset of scope whose DeleteByID returned an
//     error and which the caller should retry on the next attempt.
//     Empty when every scope id either succeeded or was already
//     gone — the request is terminally complete from this
//     receiver's standpoint.
//
// Important: rows whose DeleteByID returns removed=false (already
// gone — concurrent cleanup, prior attempt's progress) are NOT
// added to survivors. The sender's intent for that id is satisfied;
// counting it as a survivor would force endless retries against a
// row that no longer exists.
//
// chatlog-store unavailable is treated like "every scope id is a
// survivor": the caller will see error/0 and the next retry will
// try again.
//
// requestID is the (peer, requestID) cache key the sweep is
// running on behalf of and is required to be the same key the
// caller already seeded in inboundConvDeleteCache. Sweep calls
// inboundConvDeleteCache.recordDeletion(id) immediately after
// each successful DeleteByID. recordDeletion is the SINGLE
// atomic credit primitive: under cache.mu it drops id from the
// live survivor set AND bumps cumulativeDeleted by 1 (and
// settles lastStatus to Applied if survivors empties under
// gatheredScope=true). The same primitive — exposed as
// recordDeletionAnyScope, the linear-scan variant for callers
// that don't know requestID — is invoked by
// suppressIfWipeTombstoned when its own DeleteByID for a
// replayed envelope returns removed=true. The
// "credit each id at most once" invariant lives at the
// SURVIVOR-SET level under cache.mu, NOT at the database
// level — a tombstoned replay can re-insert the row after
// the original delete and a second DeleteByID can then return
// removed=true on the same id. Whichever recordDeletion call
// takes the cache mutex first while id is still in survivors
// wins ownership and credits exactly once; every later
// recordDeletion call for that id finds id already cleared
// and bumps nothing.
func (r *DMRouter) sweepInboundDeleteScope(envelopeSender domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, scope map[domain.MessageID]struct{}) (removedThisAttempt int, survivors map[domain.MessageID]struct{}) {
	survivors = make(map[domain.MessageID]struct{})
	store := r.client.chatlog.Store()
	if store == nil {
		log.Warn().
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: sweepInboundDeleteScope: chatlog store unavailable; every candidate becomes a survivor for retry")
		for id := range scope {
			survivors[id] = struct{}{}
		}
		return 0, survivors
	}
	var removedIDs []domain.MessageID
	for id := range scope {
		// Tombstone BEFORE DeleteByID so a replay of the same
		// envelope arriving in the window between DELETE and
		// the post-loop tombstone update is suppressed by
		// onNewMessage. A pre-mark for a row whose DeleteByID
		// later fails is harmless: the row stays in chatlog,
		// the tombstone simply makes a future replay re-DELETE
		// it (which is the right behaviour — the user wanted
		// it gone).
		r.wipeTombstones.Note([]domain.MessageID{id}, time.Now().UTC())
		removed, err := store.DeleteByID(id)
		if err != nil {
			survivors[id] = struct{}{}
			log.Warn().Err(err).
				Str("target", string(id)).
				Str("envelope_sender", string(envelopeSender)).
				Msg("dm_router: sweepInboundDeleteScope: chatlog DeleteByID failed for row; staying in survivors for next retry")
			continue
		}
		if !removed {
			// Already gone — sender's intent for this id is
			// satisfied. Do NOT add to survivors. Whoever
			// actually deleted the row (typically a parallel
			// suppressIfWipeTombstoned re-delete that won the
			// chatlog lock first) is responsible for crediting
			// cumulativeDeleted via their own
			// recordDeletion(/AnyScope) call. We could safely
			// call recordDeletion here too — the survivor-set
			// idempotency would correctly no-op if the other
			// path already cleared the id — but we skip out of
			// laziness: there is no scenario where THIS sweep
			// owes the credit when its own DeleteByID returned
			// removed=false.
			continue
		}
		// Atomic "drop survivor + bump cumulativeDeleted +
		// settle status" under cache.mu. Idempotent at the
		// SURVIVOR-SET level: if a concurrent
		// recordDeletionAnyScope from suppressIfWipeTombstoned
		// already credited this id (its own DeleteByID also
		// returned true for the row — possible when a
		// tombstoned replay re-inserted the row after our
		// first delete; the database does NOT guarantee
		// only-one-true), our call observes id no longer in
		// survivors and returns false WITHOUT bumping
		// cumulative again. The single-credit guarantee is
		// produced by recordDeletion itself, not by the
		// chatlog store.
		r.inboundConvDeleteCache.recordDeletion(envelopeSender, requestID, id, r.inboundConvDeleteSeen, time.Now().UTC())
		removedIDs = append(removedIDs, id)
		if r.fileBridge != nil {
			r.fileBridge.OnMessageDeleted(id)
		}
	}
	r.evictWipedConversationFromUI(envelopeSender, removedIDs)
	return len(removedIDs), survivors
}

// replyConversationDeleteAck encodes a ConversationDeleteAckPayload
// (echoing the request id from the inbound conversation_delete) and
// ships it back over the control wire. Best-effort — if the ack
// send itself fails we log and move on; the requester will retry
// conversation_delete with the SAME requestID, and our handler
// resolves that retry against the live scope cache (or its
// seen-set fallback after cache TTL) rather than re-sweeping
// chatlog. A successful first-apply replay therefore echoes the
// original `(Applied, cumulativeDeleted)` outcome — NOT
// applied/Deleted=0 — so the sender's UI sees a stable count
// across retries; a tombstoned-or-Error replay echoes Error/0.
// The echoed request id lets the requester drop late acks from
// abandoned earlier wipes instead of mis-attributing them to a
// fresh in-flight wipe.
func (r *DMRouter) replyConversationDeleteAck(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, status domain.ConversationDeleteStatus, deleted int) {
	payload, err := domain.MarshalConversationDeleteAckPayload(domain.ConversationDeleteAckPayload{
		RequestID: requestID,
		Status:    status,
		Deleted:   deleted,
	})
	if err != nil {
		log.Warn().Err(err).
			Str("peer", string(peer)).
			Str("request_id", string(requestID)).
			Msg("dm_router: marshal conversation_delete_ack failed")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := r.client.SendControlMessage(ctx, peer, domain.DMCommandConversationDeleteAck, payload); err != nil {
		log.Warn().Err(err).
			Str("peer", string(peer)).
			Str("request_id", string(requestID)).
			Msg("dm_router: send conversation_delete_ack failed; requester will retry")
	}
}

// handleInboundConversationDeleteAck branches on the recipient-
// reported wire status and only retires the matching pending entry
// on the SENDER-SIDE TERMINAL path:
//
//   - applied (IsTerminalSuccess): retire the entry via
//     removeIfMatch, run the local chatlog wipe via
//     applyLocalConversationWipe, and publish a
//     TopicConversationDeleteCompleted outcome (with
//     LocalCleanupFailed reflecting whether the local sweep ran
//     cleanly).
//   - error: NOT terminal for the sender. The handler uses
//     noteErrorIfMatch (read-only — no removal) so the pending
//     entry stays in the map and the retry loop keeps chasing the
//     peer on the normal schedule. No outcome event is published;
//     the only remaining sender-side terminal paths are an applied
//     ack or retry-budget abandonment from the retry loop.
//
// Stale acks (envelope sender does not match a pending entry, or
// requestID guard rejects a late ack from an abandoned earlier
// wipe) are dropped silently — this matches the per-message
// contract that every authenticated control DM is idempotent and
// so are its acks from the sender's perspective.
//
// Pessimistic ordering: the local wipe runs HERE, only after the
// peer confirmed it has wiped its own copy on the applied path. An
// error status (and abandonment) leaves the local rows in place so
// the user can re-issue once the peer is healthy.
func (r *DMRouter) handleInboundConversationDeleteAck(envelopeSender domain.PeerIdentity, payloadJSON string) {
	var ack domain.ConversationDeleteAckPayload
	if err := json.Unmarshal([]byte(payloadJSON), &ack); err != nil {
		log.Debug().Err(err).
			Str("envelope_sender", string(envelopeSender)).
			Msg("dm_router: conversation_delete_ack payload malformed; dropping")
		return
	}
	if !ack.Valid() {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("request_id", string(ack.RequestID)).
			Str("status", string(ack.Status)).
			Int("deleted", ack.Deleted).
			Msg("dm_router: conversation_delete_ack payload invalid; dropping")
		return
	}

	// Branch by ack class BEFORE touching the pending map. The two
	// paths use different state-mutation primitives so a transient
	// error path never has to remove-and-restore the entry — that
	// window would let a concurrent fresh SendConversationDelete
	// for the same peer be silently overwritten by the restored
	// stale entry.

	if !ack.Status.IsTerminalSuccess() {
		// Transient error: the peer signalled it could not run the
		// wipe. Leave the pending entry in place so the retry loop
		// keeps trying on the normal schedule. noteErrorIfMatch
		// validates the requestID without mutating the map; on
		// mismatch (late error ack from an abandoned earlier wipe)
		// or absence (already retired) we drop the ack silently and
		// the current pending — if any — continues uninterrupted.
		// Local rows stay intact (pessimistic ordering — we only
		// wipe after a success ack).
		pending, ok := r.convDeleteRetry.noteErrorIfMatch(envelopeSender, ack.RequestID)
		if !ok {
			log.Debug().
				Str("envelope_sender", string(envelopeSender)).
				Str("ack_request_id", string(ack.RequestID)).
				Str("status", string(ack.Status)).
				Msg("dm_router: conversation_delete_ack dropped (error path) — no matching pending entry (unknown peer or requestID mismatch from an abandoned earlier wipe)")
			return
		}
		log.Info().
			Str("peer", string(envelopeSender)).
			Str("request_id", string(ack.RequestID)).
			Str("status", string(ack.Status)).
			Int("attempts", pending.attempt).
			Msg("dm_router: conversation_delete_ack transient error; pending entry left in place for retry")
		return
	}

	// Terminal success: atomic check-and-retire under one mutex hold
	// so a concurrent SendConversationDelete or retry-loop sweep
	// cannot slip between an unguarded peek and a later remove.
	pending, ok := r.convDeleteRetry.removeIfMatch(envelopeSender, ack.RequestID)
	if !ok {
		log.Debug().
			Str("envelope_sender", string(envelopeSender)).
			Str("ack_request_id", string(ack.RequestID)).
			Str("status", string(ack.Status)).
			Msg("dm_router: conversation_delete_ack dropped (success path) — no matching pending entry (unknown peer or requestID mismatch from an abandoned earlier wipe)")
		return
	}

	// Pessimistic delete: the local sweep runs here, only after
	// the peer confirmed the action. applyLocalConversationWipe
	// removes the intersection between the chatlog and the
	// click-time local snapshot (pending.localKnownIDs); rows
	// that landed locally after the click are OUTSIDE the local
	// sweep scope and are deliberately SKIPPED on this side.
	// Whether such a row also survives on the receiver side is
	// NOT guaranteed. Two known asymmetries:
	//   - in-flight inbound: an originator-authored message that
	//     was in-flight at click time and reached the receiver
	//     before its first-contact gather was wiped by the
	//     receiver — that row lands on the originator AFTER
	//     the click, falls outside localKnownIDs, and survives
	//     only on the originator;
	//   - sent outbound: self-authored rows still in
	//     DeliveryStatus="sent" at click time are deliberately
	//     excluded from localKnownIDs by the snapshot's
	//     delivery-aware filter even though they are visible in
	//     the user's own UI. They stay on the originator;
	//     whether they survive on the receiver depends on
	//     delivery ordering (peer receives BEFORE its
	//     first-contact gather → peer wipes → originator-only
	//     outcome; peer receives AFTER → both keep).
	// The protocol's contract is "wipe the sender's
	// peer-confirmed local snapshot, with documented
	// out-of-scope asymmetries", NOT "wipe everything the user
	// saw at click" or "guarantee both-side parity". See the
	// dm_command.go payload comment for the full trade-off. The
	// wire payload is still intent-only; the snapshot only ever
	// lives in this process.
	// ok=false signals that at least one in-scope row could not
	// be removed, so the outcome carries LocalCleanupFailed=true
	// and the UI surfaces a "peer wiped, local cleanup failed"
	// message instead of a clean "wiped as of click".
	deletedLocal, localOK := r.applyLocalConversationWipe(envelopeSender, pending.localKnownIDs)

	log.Info().
		Str("peer", string(envelopeSender)).
		Str("status", string(ack.Status)).
		Int("deleted_remote", ack.Deleted).
		Int("deleted_local", deletedLocal).
		Bool("local_cleanup_failed", !localOK).
		Int("attempts", pending.attempt).
		Msg("dm_router: conversation_delete completed")

	r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
		Peer:               envelopeSender,
		Status:             ack.Status,
		DeletedRemote:      ack.Deleted,
		Abandoned:          false,
		Attempts:           pending.attempt,
		LocalCleanupFailed: !localOK,
	})
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

// conversationDeleteRetryLoop runs in a dedicated goroutine launched
// from Start(). On each tick it scans pendingConversationDelete for
// entries whose nextRetryAt has passed and re-dispatches the
// conversation_delete control DM with the SAME requestID as the
// original send (the wire payload carries no scope; the only thing
// that needs to stay stable across retries is the ack matching id).
//
// Budget-exhaustion handling has TWO steps, not one:
//
//  1. recordAttemptIfMatch returns terminal=true when the attempt
//     counter just hit convDeleteRetryMaxAttempt. The entry is
//     KEPT in the map with terminalAt=now and the loop only logs
//     the budget-exhaustion event — it does NOT publish
//     Abandoned. dueEntries skips terminal entries, so no
//     further dispatches go out.
//  2. pruneTerminalAckExpired runs at the head of every tick and
//     drops entries whose terminalAt is older than
//     convDeleteTerminalAckGrace. Only THEN does
//     processConversationDeleteRetryDue publish
//     TopicConversationDeleteCompleted with Abandoned=true.
//
// The grace window gives the ack for the FINAL dispatch room to
// arrive: dropping the entry the moment the budget is hit would
// race the peer's reply against an empty pending map and report
// a successfully-applied wipe as Abandoned. Pessimistic ordering
// still means the local chatlog wipe NEVER ran for an abandoned
// request, so the sender's chat history stays intact and the user
// can re-issue the wipe (which mints a fresh requestID) once the
// peer is reachable.
//
// The loop terminates when ctx is cancelled. If ctx is
// context.Background() (current behaviour), the loop runs for the
// process lifetime. pendingConversationDelete is in-memory only — a
// process restart starts from a clean slate, dropping all in-flight
// retry. JSON persistence rendezvous-ed alongside transfers-*.json
// is a tracked follow-up shared with message_delete.
func (r *DMRouter) conversationDeleteRetryLoop(ctx context.Context) {
	defer recoverLog("conversationDeleteRetryLoop")
	ticker := time.NewTicker(convDeleteRetryTickPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.processConversationDeleteRetryDue(ctx, now.UTC())
		}
	}
}

// processConversationDeleteRetryDue is one sweep of the retry loop,
// factored out for testability. It collects due entries under the
// convDeleteRetry mutex, then dispatches them outside the mutex so a
// slow rpc call cannot stall future sweeps.
//
// Order of operations matches processDeleteRetryDue: dispatch first,
// then recordAttempt. The dispatch counter therefore reflects "number
// of dispatches that have actually gone out" rather than "number of
// dispatches plus one".
//
// The sweep also reaps two classes of stranded entries BEFORE
// dispatching the due set:
//
//   - pruneStaleReservations drops pending entries installed by
//     BeginConversationDelete whose CompleteConversationDelete
//     never ran (e.g. caller crashed between Begin and the
//     goroutine launching Complete). Without reaping, the
//     outgoing barrier would stay raised on the peer until
//     process restart. Each pruned reservation publishes
//     Abandoned with attempts=0.
//   - pruneTerminalAckExpired drops entries that hit the
//     dispatch budget more than convDeleteTerminalAckGrace ago
//     without an ack landing. Each pruned terminal publishes
//     Abandoned with attempts=convDeleteRetryMaxAttempt. This
//     is where retry-budget abandonment is actually announced —
//     recordAttemptIfMatch only marks terminalAt; the publish
//     waits out the grace so a slow-but-successful final ack
//     still wins.
func (r *DMRouter) processConversationDeleteRetryDue(ctx context.Context, now time.Time) {
	for _, stranded := range r.convDeleteRetry.pruneStaleReservations(now, convDeleteReservationTTL) {
		log.Warn().
			Str("peer", string(stranded.peer)).
			Str("request_id", string(stranded.requestID)).
			Dur("age", now.Sub(stranded.reservedAt)).
			Msg("dm_router: conversation_delete reservation stranded past TTL (CompleteConversationDelete never ran); dropping to lift the outgoing barrier")
		// Publish Abandoned so the UI's clear_chat status string
		// transitions out of "dispatching…" instead of staying
		// pinned forever. attempt is 0 for a stranded reservation
		// — no dispatch ever ran — and Status is empty (no ack).
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:      stranded.peer,
			Status:    "",
			Abandoned: true,
			Attempts:  0,
		})
	}

	// Reap entries whose final dispatch was sent more than the grace
	// window ago without any ack landing. Publishing here (instead of
	// at the moment recordAttemptIfMatch hits the budget) gives the
	// final ack a real chance to arrive: the peer may be slow but
	// alive, and dropping the entry the instant the last dispatch
	// went out would discard a successful round-trip.
	for _, abandoned := range r.convDeleteRetry.pruneTerminalAckExpired(now, convDeleteTerminalAckGrace) {
		log.Warn().
			Str("peer", string(abandoned.peer)).
			Str("request_id", string(abandoned.requestID)).
			Int("attempts", abandoned.attempt).
			Dur("ack_grace", now.Sub(abandoned.terminalAt)).
			Msg("dm_router: conversation_delete final dispatch ack grace expired; publishing Abandoned and lifting barrier")
		r.publishConversationDeleteOutcome(ebus.ConversationDeleteOutcome{
			Peer:      abandoned.peer,
			Status:    "",
			Abandoned: true,
			Attempts:  abandoned.attempt,
		})
	}

	due := r.convDeleteRetry.dueEntries(now)
	for _, entry := range due {
		// Reuse the same requestID across retries so a duplicate
		// ack for an earlier dispatch of THIS request still
		// matches the pending entry. The wire payload is
		// intent-only; no scope to repeat.
		if err := r.dispatchConversationDelete(ctx, entry.peer, entry.requestID); err != nil {
			log.Debug().Err(err).
				Str("peer", string(entry.peer)).
				Str("request_id", string(entry.requestID)).
				Int("attempt", entry.attempt+1).
				Msg("dm_router: conversation_delete retry send failed; will count toward retry budget")
		}

		// recordAttemptIfMatch is request-bound: if the snapshot is
		// stale (the user issued a fresh wipe between dueEntries
		// and now, replacing the pending entry with a different
		// requestID), the bookkeeping for the old request is
		// dropped silently. The fresh entry's retry budget stays
		// intact instead of being polluted by an old dispatch's
		// attempt slot.
		//
		// On terminal=true the entry is retained with terminalAt
		// set so the ack for THIS dispatch can still land within
		// convDeleteTerminalAckGrace. The actual Abandoned outcome
		// is published by pruneTerminalAckExpired above on a later
		// tick if no ack arrives; here we only log the
		// budget-exhaustion event.
		updated, terminal, matched := r.convDeleteRetry.recordAttemptIfMatch(entry.peer, entry.requestID, now)
		if !matched {
			log.Debug().
				Str("peer", string(entry.peer)).
				Str("request_id", string(entry.requestID)).
				Msg("dm_router: conversation_delete retry bookkeeping skipped — pending entry was replaced or removed by a concurrent operation")
			continue
		}
		if terminal {
			log.Warn().
				Str("peer", string(updated.peer)).
				Str("request_id", string(updated.requestID)).
				Int("attempts", updated.attempt).
				Dur("ack_grace", convDeleteTerminalAckGrace).
				Msg("dm_router: conversation_delete retry budget exhausted after final dispatch; awaiting ack within grace window before publishing Abandoned")
			continue
		}
		_ = updated
	}
}
