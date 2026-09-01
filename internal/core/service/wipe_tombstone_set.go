package service

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// wipe_tombstone_set.go answers one question: this envelope names a message
// this node deleted — do we take it back?
//
// A deletion removes the chatlog row AND clears the router's dedup gate for its
// id, which is precisely what lets a relay echo or an inbox replay re-insert
// it. The answer used to be a durable refusal, one row per deleted id, kept for
// the sender's whole reseed horizon. That row was the last thing on this disk
// that knew a message had been deleted here, and it outlived the deletion by
// design — so it was also the one trace a wipe could not remove. It is gone.
//
// What replaces it is the transport, in three parts:
//
//   - THE TASK. While this node still owes a peer "delete your copy of this
//     id", the request row names that id, and refusing it costs nothing extra:
//     the id is one we are openly carrying, not one we are recording. Hydrate
//     loads those ids at startup, so this half survives a restart — for exactly
//     as long as the job does, and not one moment longer.
//   - THE ANSWER. A refused replay is reported as a DUPLICATE, and the node's
//     duplicate branch answers the frame with `ack_delete` — the
//     backlog-release signal (node.shouldAckOnStoreResult). The hop that
//     pushed it drops the message, and the original sender's retry loop ends
//     on the delivery receipt it already has. Answering stops a replay at its
//     source; remembering would only stop it here.
//   - THE PEER'S OWN DELETION. When the task settles, the peer has erased their
//     copy. There is then nothing left to replay from, which is why the refusal
//     can end with the task.
//
// This map is the fourth part and the shortest-lived: it holds what THIS
// PROCESS deleted, so an echo arriving minutes later is refused even after the
// task settled. It is memory only — nothing here is ever written down.
//
// THE WINDOW THAT IS LEFT OPEN, and why it stays open.
//
// A relay may hold a copy that it never managed to deliver to us — so it never
// received our ack for it — of a message we got by some other path and then
// deleted. If it delivers that copy after this process has restarted, nothing
// here recognises the id and the message comes back.
//
// It is narrower than it sounds: with CORSA_TRANSIT_FORWARD_ONCE a relay is a
// pure forwarder and holds no copies at all, and every copy that WAS delivered
// released its hop on arrival. But it is real, and there are only two ways to
// close it. One is a durable list of the ids this node has deleted — the trace
// the whole design exists to remove. The other is telling relays which ids to
// drop, which hands a third party the very fact we refuse to write down about
// ourselves: that this user deleted this message. Between a rare resurrection
// the user can delete again and a deletion notice broadcast to strangers, this
// takes the resurrection.

// wipeTombstoneTTL bounds how long a deleted id is refused before a
// re-delivery of it is allowed to re-create the row.
//
// WHAT STOPS THE SENDER is the answer, not this TTL — see part two of the
// file header. A refused arrival is reported as StoreDuplicate, the
// duplicate path re-sends the delivery receipt, and the sender's retry
// ends on it. This map covers something narrower: an echo arriving at THIS
// process minutes or days after the deletion, once the task has settled
// and stopped covering the id.
//
// A week and a little is what that costs, and the number is no longer
// derived from the sender at all. It used to be: the sender's outbox
// re-injected anything undelivered from the last week, so past that nobody
// re-sent it. That horizon is gone — a delivery now ends only when the
// recipient confirms it, when its author withdraws it, or when its own TTL
// expires (node/delivery_retry.go) — so no finite TTL here can be derived
// from how long a sender might keep trying.
//
// That widens the window the file header declines to close, and the answer
// to it is unchanged. This is a designed behaviour with a stated cost, not
// an unexamined leftover, so the cost is written down here rather than
// discovered later:
//
// A message deleted here can be re-created if a relay is still holding an
// undelivered copy AND our delivery receipt for it never reached the
// sender AND either more than wipeTombstoneTTL has passed or this process
// has restarted since the deletion. The user deletes it again. Nothing
// about it is silent — the message reappears in the thread rather than
// leaking anywhere, and every step that could have prevented it costs
// more than it saves:
//
//   - A durable list of deleted ids closes it completely, and that row
//     would be the last thing on this disk that knows a message was
//     deleted here, outliving the deletion by design — the one trace a
//     wipe could not remove. This is the trade the whole deletion design
//     exists to refuse; see part two of the file header.
//   - A terminal "stop sending this id" on the wire closes it at the
//     source and writes nothing down, but hands a third party the fact we
//     refuse to record about ourselves, and needs a protocol version.
//   - Re-bounding the sender's retry by a clock closes it symmetrically
//     and reintroduces the reported bug: a recipient offline overnight
//     losing messages nobody was ever told about.
//
// The cost is a map entry per deleted id for that long, and only while the
// process lives; maxWipeTombstones is what actually bounds the size.
const wipeTombstoneTTL = 8 * 24 * time.Hour

// wipeTombstoneReapPeriod is how often the reaper goroutine prunes stale
// entries. Independent of TTL — must be small enough that the peak set size
// stays bounded under heavy wipe activity.
const wipeTombstoneReapPeriod = 5 * time.Minute

// maxWipeTombstones caps what this process ACCUMULATES, because a TTL alone
// does not.
//
// The horizon is eight days and a wipe can name a whole conversation, so
// "everything deleted in the last eight days" is a number the user chooses, not
// one this code does. A cap turns an unbounded structure into a bounded one,
// which is the property that matters for something that lives as long as the
// process — roughly 110 bytes an entry, so a full set is around 20 MB.
//
// What eviction costs, stated honestly: past the cap the OLDEST refusals go,
// and a refusal dropped before its horizon is one a late copy could get past.
// The order is chosen so that the entries most likely to still matter are the
// last to go — a refusal made minutes ago belongs to a message whose sender may
// still be re-seeding it, while the oldest ones belong to deletions from days
// ago. Inside ONE enormous wipe the entries are queued in the order they were
// refused, so the eviction is still oldest-first and deterministic, but their
// horizons differ by milliseconds: the protection that goes is as arbitrary as
// the position in that one wipe. That case is logged rather than pretended
// away.
//
// The refusals hydrated from OUTSTANDING DELETIONS are not in this set at all
// (see the owed field on wipeTombstoneSet), so the cap never reaches them. They
// are not this process's accumulation: they name work that exists on disk, they
// are read from there, and their number falls as the peers confirm. Evicting
// them would drop the protection of exactly the deletions whose messages are
// still being re-sent — the ones the cap's own ordering rule says to keep last.
const maxWipeTombstones = 200_000

// wipeTombstoneQueueSlack is how many superseded positions the queue carries
// before it is rebuilt. Small enough that the queue stays proportional to the
// set, large enough that a burst of renewals does not rebuild on every call.
const wipeTombstoneQueueSlack = 1024

// wipeTombstoneReloadFloor is the least time between two fallback loads from
// the inbound path. Short enough that a database that recovers is picked up
// long before the reaper's own tick, long enough that a wedged one is asked
// once per interval instead of once per message.
const wipeTombstoneReloadFloor = 5 * time.Second

// wipeTombstone is one refusal: when it runs out, and which queue position
// speaks for it.
type wipeTombstone struct {
	expiresAt time.Time
	// seq identifies the queue position this refusal owns. A queued position
	// whose id is gone from the map, or whose seq no longer matches, was
	// SUPERSEDED — by a renewal that queued the id again, or by a Forget — and
	// the queue skips it.
	//
	// Without it a renewed id would keep its old position, and that position
	// would speak for the new refusal: the reaper would stop at an id that
	// expires later than the queue implies, and the cap would evict as "oldest"
	// an id refused a moment ago.
	seq uint64
}

// queuedTombstone is one position in the expiry queue.
type queuedTombstone struct {
	id  domain.MessageID
	seq uint64
}

// wipeTombstoneSet is the in-memory half described above: ids this process
// deleted, plus the ids of the deletions it has still not had confirmed.
type wipeTombstoneSet struct {
	mu sync.Mutex
	// entries maps a refused id to its refusal. Keyed by the PLAIN id, which it
	// can be because none of this is persisted: what made the durable half
	// dangerous was that the list survived the messages.
	entries map[domain.MessageID]wipeTombstone
	// order is the refusals in the sequence they were made, which is also the
	// sequence they expire in: every refusal gets the same TTL, and a renewal
	// takes a NEW position at the tail rather than keeping its old one, so the
	// head of this queue is always the next to go. It is what makes both the
	// reaper and the cap O(1) per removal instead of a scan of the whole map
	// under the lock — a scan that would run on the deletion path, once per
	// deleted message, exactly when the set is at its largest.
	//
	// Superseded positions are skipped when the queue is drained and removed
	// wholesale by compactQueueLocked; nothing removes them one at a time,
	// because finding one would be the scan this queue exists to avoid.
	order []queuedTombstone
	// head is the next position to look at. Consuming from the front by moving
	// an index — instead of re-slicing — is what keeps eviction amortised O(1)
	// once the set is full: a re-slice copies everything that is left, so a set
	// at the cap would copy 200 000 positions on EVERY deletion, under this
	// lock, on the inbound path. The prefix behind it is reclaimed in one go,
	// rarely, by reclaimConsumedLocked.
	head int
	// nextSeq hands out queue positions. Monotonic per process; it identifies a
	// position, it is not a clock.
	nextSeq uint64
	// owed is the ids of the deletions this node still owes a peer, as of the
	// last load. Refused for as long as the work exists, and kept OUT of the
	// capped set above deliberately: those ids are not this process's
	// accumulation but a mirror of a work queue on disk, so they are reconciled
	// against it — an id whose peer has confirmed is gone at the next load —
	// rather than expired on a timer or evicted by a cap. See maxWipeTombstones.
	owed map[domain.MessageID]struct{}
	// tasks resolves the outstanding deletion requests. It is not a journal of
	// this set — nothing writes to it here — it is the work queue, read for the
	// ids it happens to name.
	tasks func() deleteTaskList
	// loaded is set once the outstanding deletions have been read successfully,
	// ever. It separates "we have never managed to read them" from "we read them
	// and this refresh failed": the first must fall back per message, the second
	// keeps the set it already has.
	loaded bool
	// unloaded is set when a load failed and none had succeeded before it. A
	// memory miss then proves nothing about the deletions in flight, so Refuses
	// answers "cannot tell" rather than "not refused".
	unloaded bool
	// nextReload throttles the retry of that load. Refuses runs on the INBOUND
	// path — the node calls StoreMessage synchronously for every arriving
	// message — and a database that is wedged rather than merely late answers
	// each retry only after busy_timeout. Without a floor between attempts
	// every message pays that timeout, and a slow disk becomes a stalled
	// receive path.
	nextReload time.Time
}

// deleteTaskList is the outstanding-deletion half of the chatlog store.
type deleteTaskList interface {
	OwedDeleteIntentMessageIDs(ctx context.Context) ([]domain.MessageID, error)
}

// newWipeTombstoneSet builds the set over a RESOLVER, for the reason given on
// newInboundConversationDeleteSeenSet: the store is opened after the router is
// built.
func newWipeTombstoneSet(tasks func() deleteTaskList) *wipeTombstoneSet {
	return &wipeTombstoneSet{
		entries: make(map[domain.MessageID]wipeTombstone),
		owed:    make(map[domain.MessageID]struct{}),
		tasks:   tasks,
	}
}

func (s *wipeTombstoneSet) taskList() deleteTaskList {
	if s == nil || s.tasks == nil {
		return nil
	}
	return s.tasks()
}

// Hydrate refuses the ids of every deletion still owed to a peer. Called once
// at startup before any inbound message is handled, on the reaper's tick, and
// again whenever a load has failed.
//
// These are the deletions still in flight — the ones whose messages the sender
// may still be retrying, because nothing has yet told them to stop. That is
// exactly the set worth carrying across a restart, and it is carried without a
// record of anything: the rows are requests this node is making, not notes
// about messages it destroyed.
//
// It RECONCILES rather than adds. The set of outstanding deletions only ever
// shrinks by a peer confirming one, and this side has no event for that worth
// reacting to — the confirmation is already handled elsewhere, and reacting to
// it here would drop a refusal on the strength of an ack, which is the mistake
// this file's history is made of. Reading the list again and keeping exactly
// what it says means an id whose deletion has settled stops being exempt from
// the cap at the next tick, without anything having to be told.
//
// A refusal that leaves this set does NOT leave the process: if this node
// deleted the message itself, Note put it in the capped set for its own
// horizon. What ends is only the exemption.
func (s *wipeTombstoneSet) Hydrate(ctx context.Context, now time.Time) {
	tasks := s.taskList()
	if tasks == nil {
		return
	}
	owed, err := tasks.OwedDeleteIntentMessageIDs(ctx)
	if err != nil {
		s.mu.Lock()
		// A set that was read once keeps what it has: the list on disk did not
		// change because we failed to read it, and answering "cannot tell" for
		// every message over a transient busy would stall the receive path for
		// as long as the database is slow. A set that was NEVER read has nothing
		// to keep, and an empty set that claimed to be loaded would let every
		// replay through until the next restart — so that one goes UNLOADED.
		// Refuses retries the load while it is, at a bounded rate (nextReload),
		// and the reaper retries on its own tick regardless.
		s.unloaded = !s.loaded
		s.mu.Unlock()
		log.Warn().Err(err).
			Msg("dm_router: reading the deletions still owed failed; the refusals from the last successful read stand")
		return
	}
	refreshed := make(map[domain.MessageID]struct{}, len(owed))
	for _, id := range owed {
		if id == "" {
			continue
		}
		refreshed[id] = struct{}{}
	}
	s.mu.Lock()
	s.owed = refreshed
	s.loaded = true
	s.unloaded = false
	// Same housekeeping as the deletion path, because this runs on a timer and
	// nothing else is guaranteed to: a node that receives nothing for a week
	// would otherwise hold every refusal it ever made.
	s.dropExpiredLocked(now)
	evicted := s.enforceCapLocked()
	s.compactQueueIfSlackLocked()
	s.reclaimConsumedLocked()
	s.mu.Unlock()
	if evicted > 0 {
		deletionLog().Warn().Int("dropped", evicted).Int("cap", maxWipeTombstones).
			Msg("dm_router: the refusal set is full; the oldest deletions are no longer protected from a late re-delivery")
	}
	if len(refreshed) > 0 {
		// Behind the diagnostics gate, and with no count of what SETTLED.
		//
		// This runs on a timer, so a count of the deletions that finished since
		// the last tick would put both the number and the five-minute window
		// they finished in into an ordinary log file — the fact and the time of
		// a deletion, which is what this design refuses to write down. The
		// number still in flight is work this node is openly carrying and is
		// diagnostic, so it stays, but it stays behind the same gate.
		deletionLog().Info().
			Int("deletions_in_flight", len(refreshed)).
			Msg("dm_router: the messages of the deletions still owed are refused again")
	}
}

// Note refuses every id until now+TTL.
//
// Called from the deletion paths BEFORE their transaction commits, which is the
// window that matters: between deciding to delete an id and the row actually
// going, a replay can arrive and be stored by a path that has no idea a
// deletion is in progress. There is nothing to undo if the transaction then
// fails — see Forget.
//
// A nil receiver is a no-op, so fixtures that do not need refusal behaviour can
// leave the field unset without nil-panicking production call sites.
func (s *wipeTombstoneSet) Note(ids []domain.MessageID, now time.Time) {
	if s == nil || len(ids) == 0 {
		return
	}
	expiry := now.Add(wipeTombstoneTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Room first: a wipe arriving after a week of uptime should spend the space
	// of refusals that have run out before it spends the cap.
	s.dropExpiredLocked(now)
	dropped := 0
	for _, id := range ids {
		s.refuseLocked(id, expiry)
		// Per id, not per batch. One wipe names a whole conversation, and a set
		// that took the batch whole before trimming it would peak at the size of
		// the batch — the cap would bound what is KEPT while the allocation it
		// exists to prevent had already happened. The reclaim is inside the loop
		// for the same reason: it is what stops the QUEUE growing to the size of
		// the batch behind a head that keeps moving. Both are threshold-guarded
		// and cost nothing on a batch that does not reach the cap.
		dropped += s.enforceCapLocked()
		s.reclaimConsumedLocked()
	}
	s.compactQueueIfSlackLocked()
	s.reclaimConsumedLocked()
	if dropped > 0 {
		// Worth saying out loud: from here, a late copy of one of those messages
		// can be stored again. Nothing else in the system reports that.
		//
		// Behind the diagnostics gate all the same, because the number IS a
		// count of deletions this user performed and the line carries the
		// moment they stopped being protected. It is not a failure of anything
		// promised to the user — those messages are deleted, and stay deleted
		// unless a relay still holds a copy — so it does not qualify for the
		// exception that keeps failure lines visible.
		deletionLog().Warn().
			Int("dropped", dropped).
			Int("held", len(s.entries)).
			Int("cap", maxWipeTombstones).
			Msg("dm_router: the refusal set is full; the oldest deletions are no longer protected from a late re-delivery")
	}
}

// refuseLocked records one refusal and gives it a place in the queue. Caller
// MUST hold s.mu.
//
// A refusal that would end EARLIER than the one already held is dropped, not
// applied: shortening a refusal is never what a caller means, and it would also
// put the id out of expiry order in the queue, which is the one property the
// head of that queue relies on.
func (s *wipeTombstoneSet) refuseLocked(id domain.MessageID, expiry time.Time) {
	if id == "" {
		return
	}
	if current, known := s.entries[id]; known && !expiry.After(current.expiresAt) {
		return
	}
	s.nextSeq++
	s.entries[id] = wipeTombstone{expiresAt: expiry, seq: s.nextSeq}
	s.order = append(s.order, queuedTombstone{id: id, seq: s.nextSeq})
}

// dropExpiredLocked removes the entries whose refusal has run out. Caller MUST
// hold s.mu.
//
// The queue is in expiry order, so this stops at the first entry that is still
// refusing rather than walking the map.
func (s *wipeTombstoneSet) dropExpiredLocked(now time.Time) {
	for s.head < len(s.order) {
		queued := s.order[s.head]
		entry, live := s.liveAtLocked(queued)
		if live && entry.expiresAt.After(now) {
			return
		}
		if live {
			delete(s.entries, queued.id)
		}
		s.consumeHeadLocked()
	}
}

// consumeHeadLocked advances past the position at the head and ERASES it.
// Caller MUST hold s.mu.
//
// The erasure is the point. A position holds a plain message id, and the head
// moving past it does not release the string it points at: the slot keeps the
// reference until something overwrites it, so an id whose refusal ran out days
// ago stays readable in this process's memory — and the prefix behind the head
// is only reclaimed once it is long enough to be worth copying, which for a
// quiet node is never. One assignment here bounds that to the positions still
// ahead of the head, which are the refusals that are still doing something.
func (s *wipeTombstoneSet) consumeHeadLocked() {
	s.order[s.head] = queuedTombstone{}
	s.head++
}

// liveAtLocked reports whether a queued position still speaks for its entry.
// Caller MUST hold s.mu.
func (s *wipeTombstoneSet) liveAtLocked(queued queuedTombstone) (wipeTombstone, bool) {
	entry, known := s.entries[queued.id]
	if !known || entry.seq != queued.seq {
		return wipeTombstone{}, false
	}
	return entry, true
}

// enforceCapLocked drops the oldest refusal if the set is over the cap, and
// reports how many went. Caller MUST hold s.mu.
//
// What eviction costs — a late copy of an evicted id can be stored again — is
// the declared price of bounding a set that lives as long as the process. It
// The argument for why no third option exists, and the one number that trades
// memory for protection if the trade is wanted, are in docs/dm-commands.md
// §"Why a refusal can be evicted before its horizon". An unbounded alternative is a leak this
// codebase has already shipped twice; a durable one is the trace the whole
// design exists to remove.
//
// Called once per added id, so it takes the front of the queue and stops; the
// deletions still owed to a peer are not in this queue at all (see the owed
// field), so there is nothing here to skip over and no scan to pay for. Any
// work proportional to the whole queue belongs in the two reclaim paths below,
// which run rarely — this one runs on the inbound path with the lock held.
func (s *wipeTombstoneSet) enforceCapLocked() int {
	dropped := 0
	for len(s.entries) > maxWipeTombstones && s.head < len(s.order) {
		queued := s.order[s.head]
		s.consumeHeadLocked()
		if _, live := s.liveAtLocked(queued); !live {
			continue
		}
		delete(s.entries, queued.id)
		dropped++
	}
	return dropped
}

// compactQueueIfSlackLocked rebuilds the queue once it carries more superseded
// positions than it is worth keeping. Caller MUST hold s.mu.
//
// The threshold is what makes this amortised: every rebuild needs at least
// wipeTombstoneQueueSlack superseded positions to have been made since the last
// one, and each of those cost O(1) to make.
func (s *wipeTombstoneSet) compactQueueIfSlackLocked() {
	if len(s.order)-s.head <= 2*len(s.entries)+wipeTombstoneQueueSlack {
		return
	}
	live := s.order[:0]
	for _, queued := range s.order[s.head:] {
		if _, ok := s.liveAtLocked(queued); !ok {
			continue
		}
		live = append(live, queued)
	}
	s.order = live
	s.head = 0
	s.eraseTailLocked()
}

// eraseTailLocked clears the slots past the end of the queue. Caller MUST hold
// s.mu.
//
// Shortening a slice does not drop what the slots beyond it point at — the
// backing array keeps every message id it was ever given until something
// overwrites that slot. For a set whose whole purpose is that a deleted id
// stops being held anywhere, "still in the array, just past len" is exactly the
// wrong answer.
func (s *wipeTombstoneSet) eraseTailLocked() {
	clear(s.order[len(s.order):cap(s.order)])
}

// reclaimConsumedLocked releases the positions already walked past. Caller MUST
// hold s.mu.
//
// The head only moves forward, so without this the slice keeps every position
// the set has ever held — and on one enormous wipe that is a slice the size of
// the wipe, which is the allocation the cap exists to prevent. It is a copy of
// what is LEFT, so it must not run on every deletion either: that is the
// per-deletion full scan this queue exists to avoid.
//
// The threshold is what makes it amortised — a quarter of the queue must have
// been consumed before the rest is copied, so each position pays a small
// constant — and it is a QUARTER rather than a half to keep the peak near the
// cap rather than at twice it.
func (s *wipeTombstoneSet) reclaimConsumedLocked() {
	if s.head <= wipeTombstoneQueueSlack || s.head*4 <= len(s.order) {
		return
	}
	s.order = append(s.order[:0], s.order[s.head:]...)
	s.head = 0
	s.eraseTailLocked()
}

// Forget lifts the refusal on the given ids. Called when a deletion that
// pre-refused them rolled back: the rows are alive after all, and a refusal for
// a live row is a trap that would swallow its next legitimate re-delivery.
func (s *wipeTombstoneSet) Forget(ids []domain.MessageID) {
	if s == nil || len(ids) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.entries, id)
	}
	// The queue keeps each position until it is drained or compacted away:
	// removing one here means finding it, which is the scan the queue exists to
	// avoid. What it must NOT do is let those positions accumulate — a
	// forgotten id that is refused again takes a new position and leaves the
	// old one behind.
	s.compactQueueIfSlackLocked()
	// The owed set is not touched. It mirrors the work queue on disk, and a
	// deletion that rolled back did not remove its request from there — the next
	// load is what decides, not this.
}

// Refuses reports whether id is currently refused, and whether it could answer
// at all.
//
// known=false is not "no". It means the deletions in flight have not been read
// and a memory miss therefore proves nothing — the caller must treat the
// message as undecidable rather than as allowed. A nil receiver is a deployment
// without the feature and answers "not refused, known".
func (s *wipeTombstoneSet) Refuses(id domain.MessageID, now time.Time) (refused, known bool) {
	if s == nil {
		return false, true
	}
	// Memory ALWAYS first, whatever the load did. It holds what a deletion in
	// progress has just refused — the window before its transaction commits —
	// and that exists nowhere else at all.
	if s.hasInMemory(id, now) {
		return true, true
	}
	if !s.isUnloaded() {
		return false, true
	}

	// The startup load failed, so a memory miss proves nothing. Retry the load
	// rather than querying for this one id: it answers the same question, costs
	// the same read, and — unlike a lookup — ends the fallback for every
	// message after this one. But not on every message: see nextReload. A
	// caller inside the throttle window is told "unknown" rather than "not
	// refused" — the throttle bounds what the inbound path PAYS, and must not
	// turn into permission to re-create a row the user deleted.
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

// claimReload reports whether this caller should pay for a fallback load, and
// books the next slot if so.
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
	if s == nil || id == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A deletion this node still owes a peer refuses its message for as long as
	// the work stands, with no horizon of its own: the request is the reason the
	// sender may still be re-sending, so the two end together.
	if _, owed := s.owed[id]; owed {
		return true
	}
	entry, ok := s.entries[id]
	if !ok {
		return false
	}
	if !entry.expiresAt.After(now) {
		// Expired — drop opportunistically and report miss. The queue keeps the
		// position until it is drained or compacted away; see Forget.
		delete(s.entries, id)
		return false
	}
	return true
}

// reap drops every entry whose refusal has run out.
func (s *wipeTombstoneSet) reap(now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropExpiredLocked(now)
	s.compactQueueIfSlackLocked()
	s.reclaimConsumedLocked()
}
