package node

// ---------------------------------------------------------------------------
// Sender-side end-to-end delivery retry.
//
// Relays are forwarding-only (transit_retention.go): nothing in the mesh
// stores user messages durably for an offline recipient. The party that owns
// delivery is therefore the SENDER, and this scheduler is its retry engine:
//
//   - awaitingDelivered — every locally-sent DM stays here until the
//     recipient's delivered/seen receipt arrives. On each due tick the
//     envelope is re-sent with the SAME MessageID: the receiver dedupes
//     silently (no beep, no unread) and re-sends the delivered receipt
//     (see the duplicate paths in storeIncomingMessage).
//   - awaitingSeenAck — every locally-sent "seen" receipt stays here until
//     the original message sender confirms it with a "seen_ack" receipt
//     (ReceiptStatusSeenAck, additive in ProtocolVersion 23). The original
//     sender answers seen and seen-duplicates with seen_ack symmetrically
//     to the message/delivered pair.
//
// Both maps are delivery-domain state under s.deliveryMu (docs/locking.md).
// The tick runs from bootstrapLoop: due entries are snapshotted (and their
// schedule bumped) under the mutex, the actual sends happen after release —
// network I/O under a domain mutex is forbidden.
//
// Retry intervals are exponential: 30s → 1m → 2m → 5m → 11m (capped). Routing
// per attempt mirrors the primary send: live subscriber push, then
// table-directed relay. Blind-gossip behaviour depends on the reachability
// gate (CORSA_HOLD_DM_UNTIL_REACHABLE, dispatchEnvelopeRetry):
//   - default (ON): an attempt EMITS only when the recipient is reachable —
//     a directed route or a directly connected subscriber. Otherwise the
//     message is HELD (no blind gossip into the void) and re-armed the moment
//     a route/connection appears (kickDeliveryRetriesForReachable). This is
//     the cure for the blind-gossip storm to offline recipients.
//   - kill-switch (OFF): legacy behaviour — an attempt also blind-gossips when
//     no route is known ("the route may be missing only on OUR side").
// The early intervals only help when a direct route already exists; a
// re-emission through transit peers is absorbed by their dedup layers (relay
// exact-dedup TTL 3 min, bloom rotation window 5-10 min).
//
// Durability: the desktop layer registers a chatlog-backed DeliveryOutbox;
// RegisterDeliveryOutbox reseeds awaitingDelivered from the still-'sent'
// rows, and — when the outbox also implements SeenAckJournal — reseeds
// awaitingSeenAck from the seen rows that lack a journaled seen_ack, so
// both retry sets survive a sender restart. Relay-only nodes (no outbox)
// run memory-only.
// ---------------------------------------------------------------------------

import (
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// DeliveryOutbox is the narrow read-only view of the durable message store
// used to reseed the sender-side delivery retry scheduler after a restart.
// Implemented by the desktop chatlog adapter; nil on relay-only nodes.
type DeliveryOutbox interface {
	// UndeliveredOutgoing returns the sealed envelopes of locally-sent DMs
	// whose delivery status is still "sent".
	UndeliveredOutgoing() ([]OutboxEntry, error)
	// SentMessageIDs returns the ids of DMs this node authored, whatever
	// their status, so the solicited-receipt gate survives a restart. A
	// message that reached `delivered` before the restart is not reseeded
	// for retry — correctly — but its `seen` still has to be accepted when
	// the recipient finally opens the conversation, and that can be at any
	// remove: there is no age at which a genuine receipt stops being
	// genuine, so this query has no time horizon.
	//
	// NEWEST FIRST, at most limit of them. The set they refill is a
	// bounded LRU, so the caller decides how many it can hold and gets
	// the ones most likely to still earn a receipt.
	SentMessageIDs(limit int) ([]protocol.MessageID, error)
}

// OutboxEntry is one reseeded row: the envelope, plus the durable answer
// to the only thing about its past the scheduler cannot re-derive.
type OutboxEntry struct {
	Envelope protocol.Envelope
	// Emitted carries deliveryRetryEntry.Emitted across the restart: "may
	// a writer have taken this frame?". The outbox reports FALSE only when
	// it can prove the envelope never reached the wire; anything it does
	// not know is true, because the claim a deletion makes with false is
	// "the peer cannot have this", and an unprovable one there leaves a
	// delivered message with them.
	Emitted bool
	// OnWire carries the OTHER question across the restart: "did a sink
	// confirm it?". It reads FALSE unless the row says otherwise, because
	// its reader is the sender's badge and the honest answer there when
	// unsure is "queued".
	//
	// The two are separate because their safe answers point in opposite
	// directions. Deriving one from the other is what made the flag
	// non-monotone; see chatlog/emission.go.
	OnWire bool
}

// DeliveryEmissionJournal is the durable record behind the two questions
// this node asks about its own outgoing messages. Implemented by the
// desktop chatlog adapter; without it both answers are memory-only.
//
// Each call moves ONE bit in ONE direction, and that is the whole design —
// see chatlog/emission.go for why a single non-monotone flag needed a
// queue, a per-attempt stamp and a correction, and still had a race left.
//
//   - ClearNeverEmitted must be durable BEFORE the frame goes out: a crash
//     in between has to read as "the peer may have it", or a deletion would
//     skip a peer who is holding the message.
//   - MarkOnWire may land whenever: a crash before it reads as "not yet
//     sent", so the badge says queued and the engine sends again.
type DeliveryEmissionJournal interface {
	// ClearNeverEmitted records that a writer may have taken the frame.
	// Written once; nothing ever puts the claim back.
	ClearNeverEmitted(ids []protocol.MessageID) error
	// MarkOnWire records that a sink confirmed the frame on the wire.
	// Written once; nothing ever clears it.
	MarkOnWire(ids []protocol.MessageID) error
}

// DeliveryFailureJournal is the optional durable journal for messages the
// retry engine gave up on, which now has exactly one cause: the message
// outlived its own TTL. A journaled id is excluded from
// UndeliveredOutgoing, so RegisterDeliveryOutbox does not resurrect an
// expired message after a restart. Implemented by the desktop chatlog
// adapter, whose query honours the journal only for rows that HAVE
// expired — anything else in that table was put there by the attempt cap
// this engine used to have.
type DeliveryFailureJournal interface {
	// MarkDeliveryFailed durably records that automatic retries for the
	// message have been abandoned.
	MarkDeliveryFailed(id protocol.MessageID) error
}

// SeenAckJournal is the optional durable journal for outgoing seen
// receipts: which of the locally-seen inbound DMs still lack the original
// sender's seen_ack. Implemented by the desktop chatlog adapter (an outbox
// that also implements this interface gets the seen retry reseeded after a
// restart and confirmations persisted on arrival).
type SeenAckJournal interface {
	// UnconfirmedSeen returns the outgoing seen receipts that have not been
	// confirmed with a seen_ack yet.
	UnconfirmedSeen() ([]protocol.DeliveryReceipt, error)
	// MarkSeenConfirmed durably records that the seen receipt for the
	// message was confirmed by the original sender.
	MarkSeenConfirmed(id protocol.MessageID) error
}

// deliveryRetrySchedule are the exponential retry intervals; the last value
// repeats. See the package comment for why the tail exceeds the transit
// dedup windows.
var deliveryRetrySchedule = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	11 * time.Minute,
}

// defaultDeliveryRetryMaxAttempts bounds how many times a locally-sent
// SEEN RECEIPT is re-sent before the node stops asking for its seen_ack:
// 20 attempts ≈ 3.5 hours on the capped schedule. Overridable via
// config.Node.DeliveryRetryMaxAttempts (CORSA_DELIVERY_RETRY_MAX_ATTEMPTS),
// whose name still says "delivery" for compatibility although the only
// thing it now bounds is that receipt retry.
//
// It does NOT bound a message. A message ends in exactly three ways —
// the recipient confirms it, its author withdraws it, or its own TTL
// expires — and running out of patience is not one of them. Any cap here
// would be a silent "we gave up" the sender is never shown and cannot
// undo, which is the failure this engine was built out of: a night of the
// recipient being offline used to exhaust a budget without a single byte
// going out, after which the id was journalled and the message could never
// leave the machine, not even when they came back.
//
// What paces the re-sends instead is the backoff, capped at eleven
// minutes, and deliveryRetryEntry.Attempts is now only its index — reset
// whenever the recipient returns, so a message waiting for someone who
// just came back is tried in thirty seconds rather than eleven minutes.
// A receipt keeps its cap because the other side re-triggers it: every
// arrival of the seen receipt makes the original sender answer again.
const defaultDeliveryRetryMaxAttempts = 20

// deliveryHoldPollInterval is how often a HELD message re-checks whether
// its recipient became reachable. It is a safety net, not the mechanism:
// delivery normally resumes the instant kickDeliveryRetriesForReachable
// fires from the announce/connect drain. The check itself is a local
// routing-table lookup, so the interval only has to be short enough that a
// missed event costs a minute rather than an hour.
//
// It is deliberately NOT the exponential schedule: that schedule paces
// re-emissions of a message the peer may already hold, and stretching a
// hold to eleven minutes would make a returning recipient wait for a
// timer that was measuring something else entirely.
const deliveryHoldPollInterval = 60 * time.Second

// deliveryQueueWindow is how long an emitted message keeps the queue slot
// of its recipient — see planDueDeliveries for what the slot is for. Once
// it passes, the next message goes out even though the first one is still
// unconfirmed: a receipt that never arrives must cost that one message its
// place in line, not freeze the whole conversation behind it.
//
// It has to stay BELOW deliveryRetrySchedule[0], and that is not a matter
// of taste. A message that is due again re-takes the slot it just gave up,
// so if the window were as long as the shortest retry interval the head of
// a queue whose receipts are being lost would win every turn and nothing
// behind it would ever leave. A test pins the inequality.
//
// The window is not what paces an ordinary drain: a recipient who is
// actually there answers in milliseconds, and storeDeliveryReceipt pulls
// the next message forward the moment they do, so the backlog leaves at
// one message per tick. The window is the answer to silence.
const deliveryQueueWindow = 20 * time.Second

// A message whose recipient never comes back has NO age limit, and that
// is a decision rather than an omission. The alternative was a horizon
// after which the node quietly stopped trying — and quietly is the whole
// problem with it: the sender is not told, cannot undo it, and a person
// who returns from three weeks away would find the messages waiting for
// them silently dead. Waiting costs nothing on the wire, because the
// reachability gate emits nothing while they are unreachable.
//
// The three ways a delivery ends are therefore: the recipient confirms
// it, the author withdraws it (CancelOutgoingDelivery), or its own TTL
// expires — and ordinary DMs carry TTLSeconds=0, so the last one applies
// only to messages explicitly sent to auto-delete.
//
// The cost is real and accepted: awaitingDelivered holds one entry per
// undelivered message for as long as the process runs, and the startup
// reseed scans the chatlog with no horizon (message_store_adapter.go).

// deliveryRetryBackoff returns the wait before the attempt with the given
// ZERO-BASED number: backoff(0) is the wait before the first retry.
//
// Use deliveryRetryBackoffAfter when scheduling from a COUNT of attempts
// already made — the two differ by one, and reading a count into this
// function is what silently dropped the first step of the schedule.
func deliveryRetryBackoff(attempt int) time.Duration {
	if attempt >= len(deliveryRetrySchedule) {
		return deliveryRetrySchedule[len(deliveryRetrySchedule)-1]
	}
	return deliveryRetrySchedule[attempt]
}

// deliveryRetryBackoffAfter returns the wait that follows the given NUMBER
// of attempts already made. After one attempt the wait is the first step of
// the schedule, not the second.
//
// It exists because the obvious spelling was wrong for two years' worth of
// reading: `backoff(entry.Attempts)` looks right and is off by one, so the
// 30-second step never ran and the first retry of a lost message — or of a
// lost receipt from a recipient who is right there — waited a minute.
func deliveryRetryBackoffAfter(attempts int) time.Duration {
	if attempts < 1 {
		return deliveryRetryBackoff(0)
	}
	return deliveryRetryBackoff(attempts - 1)
}

// deliveryHoldReason says why an envelope is not known to be on the wire.
//
// The distinction that matters is between "we did not try" and "we tried
// and nobody has said they took it". Both leave the message ours to
// deliver; only the first is evidence about the recipient.
type deliveryHoldReason uint8

const (
	// holdNone: a sink confirmed it accepted the frame. This is as far as
	// certainty goes — an accepted frame can still be lost on the wire,
	// which is what the delivery receipt is for.
	holdNone deliveryHoldReason = iota
	// holdUnreachable: no route and no connected subscriber, so nothing
	// was attempted. This is the reachability gate, and it is the only
	// reason that says anything about the recipient.
	holdUnreachable
	// holdUnconfirmed: the envelope was handed to sinks and none of them
	// has reported accepting it. Some sinks answer immediately (a relay
	// enqueue) and some answer from a background goroutine (a subscriber
	// push), so this is the state between the attempt and its answer —
	// and the state it stays in forever if the answer is "no".
	holdUnconfirmed
)

// deliveryRetryEntry tracks one locally-sent DM awaiting its delivered/seen
// receipt. Owned by s.deliveryMu.
type deliveryRetryEntry struct {
	Envelope      protocol.Envelope
	Attempts      int
	NextAttemptAt time.Time
	// Hold says why the last pass did not put this envelope on the wire,
	// or holdNone when a sink confirmed that it did.
	//
	// The two hold reasons are not interchangeable, which is why this is
	// not a bool. "Nobody to send to" and "sent, nobody has confirmed
	// taking it" both mean the message is still ours to deliver — so both
	// are woken by kickDeliveryRetriesForReachable — but only the first is
	// a recipient being away, and only a recipient coming BACK from being
	// away may reset the backoff (resetBackoffOnReturn). Conflating them
	// made every unconfirmed dispatch look like a returning peer.
	Hold deliveryHoldReason
	// Emitted is the CONSERVATIVE answer to "might the peer have this?".
	// It turns true when the durable never-emitted claim is withdrawn —
	// BEFORE the frame is written, because a crash in that gap has to read
	// as "maybe they got it". A cancellation reads it, and an
	// over-cautious yes there costs one control DM the peer answers
	// not_found, while a wrong no leaves their copy in place with nothing
	// to recall it.
	Emitted bool
	// LastEmittedAt is when a sink CONFIRMED taking this envelope. It is
	// monotone — only confirmEnvelopeOnWire writes it, and only forward.
	// It is a different question from Emitted, and the two must not be
	// conflated in either direction:
	//
	//   - zero means no sink has ever taken it, which is what makes the
	//     sender's badge say queued;
	//   - non-zero and recent holds the recipient's queue slot while the
	//     delivered receipt has had no time to come back yet.
	//
	// The durable never-emitted mark is keyed on this one AND on Emitted
	// together — see syncEmissionMarks. Keying it on the current hold
	// instead wrote the mark for messages that had gone out and whose
	// recipient later left, which tells a deletion to skip a peer who is
	// holding the message.
	LastEmittedAt time.Time
	// Stamped memoises that the DURABLE on-wire bit has landed for this
	// message. Separate from Announced because the two debts are settled
	// by different means and fail for different reasons: the stamp is one
	// journal write that never has to be redone once it succeeds, while
	// the announcement can be shed by a full subscriber inbox again and
	// again. Keying the repair pass on Announced alone re-ran SQLite for
	// every shed event, forever.
	Stamped bool
	// AnnounceAfter paces the RE-announcement of an event the bus shed.
	// Without it a repair pass under sustained backpressure republished
	// the same ids every two seconds into an inbox that was still full.
	AnnounceAfter time.Time
	// Announced records that the sender's own client has been told this
	// message stopped being queued.
	//
	// It is not the same fact as LastEmittedAt, and the difference is the
	// whole reason it exists: several sinks can carry one envelope, each
	// confirming when it succeeds, and the sender is owed exactly one
	// event. Announced is claimed under the delivery mutex so only one of
	// them publishes — and given back when the bus sheds the event.
	//
	// Memory-only. After a restart the chatlog's never-emitted mark is the
	// authority and the conversation is read fresh, so there is nothing an
	// announcement could add.
	Announced bool
}

// seenAckRetryEntry tracks one locally-sent seen receipt awaiting the
// original sender's seen_ack. Owned by s.deliveryMu.
type seenAckRetryEntry struct {
	Receipt       protocol.DeliveryReceipt
	Attempts      int
	NextAttemptAt time.Time
}

// noteOwnEnvelopeEmitted records that a frame carrying one of THIS node's
// own DMs has been handed to the wire. It is the single accounting point
// for emission, and every path that can put an origin envelope in front
// of a peer has to go through it:
//
//   - the live push at store time and at every retry tick;
//   - the auth-time backlog replay (pushBacklogToSubscriber), which
//     serves s.topics["dm"] to a recipient that dials US. That path is
//     independent of the retry engine: the backlog append is not gated
//     on the reachability hold, so a HELD message — one the sender-side
//     scheduler never dispatched — is handed over in full the moment the
//     recipient connects.
//
// Gossip and relay emissions need no call of their own: they only run
// from the origin send and the retry tick, both of which record the
// attempt themselves.
//
// Marked BEFORE the write, not after: the question this answers is "can
// the peer possibly have it?", and an over-cautious yes costs one control
// DM that the peer answers not_found, while a wrong no silently leaves
// their copy in place with nothing left to retry it.
//
// Returns false when the message must NOT go out after all — it was
// withdrawn while this ran, or the durable claim that it never went out
// could not be withdrawn. Both make the caller drop the frame; the retry
// engine still owns the message and tries again.
// now is the instant the emission is being recorded at. Callers that are
// already working from a scheduling clock (the retry tick) pass theirs, so
// one emission is not stamped twice from two different sources of time.
//
// Returns whether the caller may write the frame. The queued → sent
// announcement is NOT reported here: it belongs to whoever writes the
// frame, AFTER writing it, and publishMessagesEmitted is idempotent so
// that caller needs no permission slip from this one.
func (s *Service) noteOwnEnvelopeEmitted(sender string, messageID protocol.MessageID, now time.Time) bool {
	if s.identity == nil || sender != s.identity.Address {
		return true
	}
	outcome := s.noteOwnEnvelopesEmitted([]protocol.MessageID{messageID}, now)
	_, blocked := outcome.Withheld[messageID]
	return !blocked
}

// noteOwnEnvelopesEmitted is the batch form, for a caller that is about
// to put many of our envelopes on the wire at once (the backlog replay).
// One section instead of one per message: the writer mutex is the
// delivery domain's, and a large backlog would otherwise take it once
// per row from a background goroutine.
//
// The ids must be ours; the batch caller filters by sender before
// calling, since it already walks the list.
//
// The outcome names the ids the caller must NOT write. What it must
// ANNOUNCE is not reported here — see emissionOutcome.
func (s *Service) noteOwnEnvelopesEmitted(ids []protocol.MessageID, now time.Time) emissionOutcome {
	if len(ids) == 0 {
		return emissionOutcome{}
	}
	claim := s.claimEmissionLocked(ids, now)
	if len(claim.Unproven) > 0 {
		// Outside the delivery mutex (SQLite) and BEFORE the caller writes
		// a frame: an id whose claim is still on disk has not been cleared
		// to go out. Ids the clear could not reach join the withheld set —
		// the caller drops them, their entry keeps its schedule, and the
		// next retry tick tries the whole thing again.
		claim = s.confirmEmission(claim, s.clearEmissionMarks(claim.Unproven), now)
	}
	return claim
}

// emissionOutcome is what one pass of noteOwnEnvelopesEmitted worked out.
//
// It deliberately does NOT carry the queued → sent announcement, although
// this is where the transition is detected. The two facts move in opposite
// directions in time: the MARK has to land before the frame is written — a
// crash in between must read as "the peer may have it", or a deletion
// would skip them — while the ANNOUNCEMENT must follow it, because saying
// "sent" for a frame that then failed to go out leaves the badge ahead of
// the wire. So the announcement is left to whoever writes the frame, and
// publishMessagesEmitted is idempotent so several sinks carrying the same
// envelope produce one event.
type emissionOutcome struct {
	// Withheld are the ids the caller must NOT write: withdrawn while this
	// ran, frozen by a wipe, or still carrying a durable never-emitted
	// claim the journal refused to withdraw.
	Withheld map[protocol.MessageID]struct{}
	// Unproven are the ids whose durable never-emitted claim still has to
	// come off the disk before their frame may go out.
	Unproven []protocol.MessageID
}

// claimEmissionLocked is the first half of noteOwnEnvelopesEmitted: it
// takes the delivery domain once, reports the ids a withdrawal has
// shadowed, and separates the ids that still carry a durable never-emitted
// claim from the ones already accounted for.
//
// An id with no standing claim — transit traffic, or a message whose claim
// was withdrawn by an earlier attempt — takes this one lock hold and no
// disk write at all, which is what the two-phase form exists to preserve.
// An outgoing message's FIRST emission does pay the withdrawal, because
// its row was born carrying the claim.
func (s *Service) claimEmissionLocked(ids []protocol.MessageID, now time.Time) emissionOutcome {
	var claim emissionOutcome
	s.deliveryMu.Lock()
	for _, id := range ids {
		if _, withdrawn := s.cancelledDeliveries[id]; withdrawn {
			claim.Withheld = withhold(claim.Withheld, id)
			continue
		}
		if s.deliveryFrozenLocked(id) {
			// A wipe is deciding whether the peer may ever hold this
			// message. Emitting now would answer the question behind its
			// back — and it is deciding from a row it is about to delete.
			claim.Withheld = withhold(claim.Withheld, id)
			continue
		}
		entry, awaiting := s.awaitingDelivered[id]
		if !awaiting {
			// No entry does NOT mean nothing to withdraw. The backlog
			// replay reaches past the retry engine entirely, so it can
			// emit a message whose entry was dropped when it was
			// withdrawn — while the durable claim that it never went out
			// still stands on the row. s.markedNeverEmitted remembers
			// exactly those ids, so the ordinary case still costs no
			// lookup and no write.
			if _, marked := s.markedNeverEmitted[id]; marked {
				claim.Unproven = append(claim.Unproven, id)
			}
			continue
		}
		if _, standing := s.markedNeverEmitted[id]; !standing || s.emissionJournal == nil {
			// Nothing on disk to withdraw: this id carries no standing
			// claim, or the flag has no durable half on this node. Asking
			// the claim set rather than a per-entry flag is what keeps an
			// ordinary send off the disk entirely — and keeps one id's
			// write failure from stranding every other id in the batch.
			markEntryEmitted(entry, now)
			continue
		}
		claim.Unproven = append(claim.Unproven, id)
	}
	s.deliveryMu.Unlock()
	return claim
}

// confirmEmission is the second half: the ids whose claim is now off the
// disk become emitted, and the ones that failed join the withheld set.
//
// The withdrawal shadow is re-read here, not carried over from the first
// hold: the durable write happened between the two, and a cancellation
// landing in that gap has already told the user the message was recalled.
func (s *Service) confirmEmission(claim emissionOutcome, stranded map[protocol.MessageID]struct{}, now time.Time) emissionOutcome {
	s.deliveryMu.Lock()
	for _, id := range claim.Unproven {
		if _, failed := stranded[id]; failed {
			claim.Withheld = withhold(claim.Withheld, id)
			continue
		}
		if _, withdrawn := s.cancelledDeliveries[id]; withdrawn {
			claim.Withheld = withhold(claim.Withheld, id)
			continue
		}
		if s.deliveryFrozenLocked(id) {
			claim.Withheld = withhold(claim.Withheld, id)
			continue
		}
		if entry, awaiting := s.awaitingDelivered[id]; awaiting {
			markEntryEmitted(entry, now)
		}
	}
	s.deliveryMu.Unlock()
	return claim
}

// markEntryEmitted records that the envelope has reached the wire: it is no
// longer held, its recipient may have it from now on, and it holds their
// queue slot until the receipt arrives or deliveryQueueWindow passes.
// Caller MUST hold s.deliveryMu.Lock.
//
// LastEmittedAt only ever moves FORWARD. Emissions are recorded from paths
// that do not share a clock — the retry tick works from the instant its
// pass began, the live push at store time and the auth-time backlog replay
// from wall time when they get there — so the guard is what keeps a stamp
// that was taken earlier from landing second and handing the queue slot
// back while a frame is still on its way out.
//
// It says nothing about the queued → sent announcement: that is
// deliberately NOT derived from Emitted, which turns true before the frame
// is written and never goes back. See deliveryRetryEntry.Announced.
//
// Nor does it clear the hold. Emitted answers "might the peer have this?",
// where an over-cautious yes is harmless; the hold answers "is this still
// ours to deliver?", where an over-cautious no would strand the message.
// Only confirmEnvelopeOnWire clears it.
// It also leaves LastEmittedAt alone. That stamp is the CONFIRMATION —
// the instant a sink said it took the frame — and it is what holds the
// recipient's queue slot; writing it here, before the wire, would make
// confirmEnvelopeOnWire see its own attempt as already confirmed and skip
// it, so a message that really did go out would never be charged, never
// leave the hold and never be announced.
func markEntryEmitted(entry *deliveryRetryEntry, _ time.Time) {
	entry.Emitted = true
}

// confirmed reports whether any sink has ever taken this envelope. Caller
// MUST hold s.deliveryMu.
func (e *deliveryRetryEntry) confirmed() bool { return !e.LastEmittedAt.IsZero() }

func withhold(set map[protocol.MessageID]struct{}, id protocol.MessageID) map[protocol.MessageID]struct{} {
	if set == nil {
		set = make(map[protocol.MessageID]struct{}, 1)
	}
	set[id] = struct{}{}
	return set
}

// markDeliveryOnWire stamps the durable "a sink confirmed this" bit.
//
// Monotone and one-directional: nothing ever clears it, so this needs no
// ordering against anything, no queue and no correction. A crash before it
// lands reads as "not yet sent" — the badge says queued and the engine
// sends again, which the recipient dedupes silently.
//
// It BLOCKS on the journal, and its callers are the ones that decide where
// to run it. confirmEnvelopeOnWire hands it to a background goroutine —
// one of its own callers is the session's writer loop, immediately after
// NetCore accepted a frame, and the journal writes through SQLite, where
// contention can park a statement for the whole busy timeout. Holding that
// loop would stop the peer's other outbound frames for seconds, to land a
// fact that needs no ordering with anything. The sole reason a write ever
// had to be synchronous on this path was the flag that could be reversed,
// and that flag is gone.
//
// It stays synchronous HERE so a caller can sequence something after it —
// the queued → sent event does exactly that, because an event that
// overtakes the disk lets a reload put the badge back.
//
// It goes through the emission lane as BOOKKEEPING, which means two things
// a caller has to respect. It never runs ahead of a pre-wire withdrawal,
// because the withdrawal is what a message the user is waiting on needs and
// this is a record of one already gone. And the lane may turn it away when
// enough stamps are already waiting — a reconnect confirming a whole
// conversation is when that happens — in which case NOTHING is written and
// the answer is the same false as a failed write.
//
// Returns whether the row now says so. A caller that is about to TELL the
// sender "sent" must not do it on a false: the badge would move while the
// disk still reads queued, and the next reload would put it back with no
// further event coming, because the announcement is claimed once. Every
// false here is owed to repairLocalDeliveryRecord, which re-derives the
// debt from state and needs no hand-off.
func (s *Service) markDeliveryOnWire(ids ...protocol.MessageID) bool {
	if len(ids) == 0 {
		return false
	}
	if s.emissionJournal == nil {
		// No durable half on this node: memory is the whole truth, so
		// there is nothing that could disagree with the badge later.
		return true
	}
	admitted, err := s.emissionLane.runBookkeeping(ids, func(chunk []protocol.MessageID) error {
		return s.emissionJournal.MarkOnWire(chunk)
	})
	if !admitted {
		// Not a failure and not a loss: the lane is full of stamps and
		// this one is still owed by state, which the repair pass reads.
		log.Debug().Int("count", len(ids)).
			Msg("emission_journal_stamp_deferred_lane_full")
		return false
	}
	if err != nil {
		// The row keeps reading as not-yet-sent, so the badge stays on
		// queued until the next confirmation or the recipient's receipt
		// moves it past sent anyway. Staying at queued is the honest
		// answer here — the alternative is saying "sent" and taking it
		// back on the next reload.
		log.Warn().Err(err).Int("count", len(ids)).
			Msg("emission_journal_mark_on_wire_failed")
		return false
	}
	return true
}

func (s *Service) forgetMarkedNeverEmitted(ids []protocol.MessageID) {
	s.deliveryMu.Lock()
	for _, id := range ids {
		delete(s.markedNeverEmitted, id)
	}
	s.deliveryMu.Unlock()
}

// clearClaimsLocked drops the standing claims for ids whose ROW is being
// destroyed, so nothing is left in the set that no later emission could
// ever consume. Memory only — the mark goes with the row.
// Caller MUST hold s.deliveryMu.Lock.
func (s *Service) clearClaimsLocked(ids map[protocol.MessageID]struct{}) {
	for id := range ids {
		delete(s.markedNeverEmitted, id)
	}
}

// clearEmissionMarks withdraws the durable claim for the ids and reports
// the ones it could NOT reach. A stranded id must not go on the wire: the
// disk would keep saying the message never went out while the peer holds
// it, and after a restart the deletion would skip them.
//
// The journal call is all-or-nothing, so a failure strands the whole
// batch. Nothing is retried here — the caller withholds the frames, the
// entries keep their schedule, and the next retry tick repeats both the
// clear and the send.
func (s *Service) clearEmissionMarks(ids []protocol.MessageID) map[protocol.MessageID]struct{} {
	if len(ids) == 0 || s.emissionJournal == nil {
		return nil
	}
	// PRE-WIRE, so it is on the critical path of a message the user is
	// waiting to send — including the first one to a recipient who has
	// just come back after a night offline, which is also when the lane
	// is busiest with stamps for the conversation being replayed. It goes
	// in as URGENT: it waits for at most the statement already running
	// and never for the bookkeeping queued behind it. See emission_lane.go.
	if err := s.emissionLane.runPreWire(ids, func(batched []protocol.MessageID) error {
		return s.emissionJournal.ClearNeverEmitted(batched)
	}); err == nil {
		s.forgetMarkedNeverEmitted(ids)
		return nil
	} else {
		log.Error().Err(err).Int("count", len(ids)).
			Msg("emission_journal_clear_failed_send_withheld")
	}
	stranded := make(map[protocol.MessageID]struct{}, len(ids))
	for _, id := range ids {
		stranded[id] = struct{}{}
	}
	return stranded
}

// noteDeliveryCancelledLocked remembers a withdrawn delivery for as long
// as a backlog push built just before it could still be writing frames.
// Caller MUST hold s.deliveryMu.Lock.
func (s *Service) noteDeliveryCancelledLocked(id protocol.MessageID, now time.Time) {
	if s.cancelledDeliveries == nil {
		s.cancelledDeliveries = make(map[protocol.MessageID]time.Time, 1)
	}
	for old, at := range s.cancelledDeliveries {
		if now.Sub(at) > cancelledDeliveryMemory {
			delete(s.cancelledDeliveries, old)
		}
	}
	s.cancelledDeliveries[id] = now
}

// cancelledDeliveryMemory is how long a withdrawal shadows a backlog
// push. It only has to outlast one push of one subscriber's inbox, which
// is bounded by the write path, not by the peer's schedule.
const cancelledDeliveryMemory = 5 * time.Minute

func (s *Service) deliveryRetryMaxAttempts() int {
	if s.cfg.DeliveryRetryMaxAttempts > 0 {
		return s.cfg.DeliveryRetryMaxAttempts
	}
	return defaultDeliveryRetryMaxAttempts
}

// registerAwaitingDeliveredLocked schedules the locally-sent envelope for
// end-to-end retry until its delivered/seen receipt arrives. Caller MUST
// hold s.deliveryMu.Lock. Idempotent per MessageID — a re-send of the same
// id keeps the original schedule. held records whether the first send was
// withheld (recipient unreachable) so kickDeliveryRetriesForReachable can
// wake it the moment a route/connection appears; an emitted send registers
// held=false (it is merely awaiting its receipt on the normal schedule).
func (s *Service) registerAwaitingDeliveredLocked(envelope protocol.Envelope, now time.Time, held bool) {
	// Record that THIS node originated this DM so storeDeliveryReceipt can tell
	// a solicited delivered/seen receipt from an unsolicited one. Done before
	// the idempotency check so a re-send promotes the id in the LRU; the entry
	// survives the delivered→seen transition because it is evicted only by LRU
	// capacity, never on receipt arrival.
	s.sentDMIDs.Add(string(envelope.ID))
	if _, exists := s.awaitingDelivered[envelope.ID]; exists {
		return
	}
	// A HELD entry is due IMMEDIATELY. The backoff schedule times a wait
	// for a RECEIPT, and a message that never reached the wire has no
	// receipt coming — starting it at thirty seconds only meant that a
	// reachability kick landing between the caller's route check and this
	// registration found nothing to wake, and the message then sat out a
	// timer that was measuring the wrong thing.
	nextAttempt := now
	if !held {
		nextAttempt = now.Add(deliveryRetryBackoff(0))
	}
	entry := &deliveryRetryEntry{
		Envelope:      envelope,
		NextAttemptAt: nextAttempt,
		Hold:          holdReasonFor(held),
		// The registration itself never means "on the wire": a send that
		// was not withheld has been HANDED to the sinks, and they confirm
		// for themselves.
		// EVERY message this node authors is shown to its author as
		// queued until a sink confirms it — the reply, the message.new
		// event and the row's own durable mark all say so — so every one
		// of them is owed the transition.
		Announced: false,
	}
	// The row this entry describes is BORN carrying the durable
	// never-emitted claim (message_store_adapter writes it in the same
	// insert), so this process has a standing claim to withdraw the first
	// time a sink takes the frame. Recording it here is what lets
	// claimEmissionLocked find it: without this the withdrawal was skipped
	// for every ordinary message, and a delivered message kept reading as
	// queued for good.
	//
	// The one caller for which "born marked" is NOT true is the startup
	// reseed, whose rows may have been emitted before the restart. It reads
	// the answer off the outbox and removes the id again — this function
	// cannot know, and guessing conservatively here is what keeps the
	// ordinary path honest.
	if s.emissionJournal != nil {
		if s.markedNeverEmitted == nil {
			s.markedNeverEmitted = make(map[protocol.MessageID]struct{}, 1)
		}
		s.markedNeverEmitted[envelope.ID] = struct{}{}
	}
	// LastEmittedAt is deliberately left ZERO even for a send that is
	// going out right now. It is the CONFIRMATION stamp — the instant a
	// sink said it took the frame — and the caller has only just handed
	// the envelope over. Stamping it here would claim the recipient's
	// queue slot for a frame no writer may ever accept, and would make
	// confirmEnvelopeOnWire mistake this attempt for one already answered.
	s.awaitingDelivered[envelope.ID] = entry
}

// holdReasonFor turns the caller's "was this send withheld?" into the
// reason a new entry starts with. A withheld send is the reachability
// gate; anything else has just been handed to the sinks and is waiting to
// hear from them.
func holdReasonFor(withheld bool) deliveryHoldReason {
	if withheld {
		return holdUnreachable
	}
	return holdUnconfirmed
}

// emissionStampNotInTheFuture repairs an emission stamp this node cannot
// have made yet.
//
// It clamps to one whole window BEFORE now, not to now. Clamping to now
// would say "emitted this instant", which holds the recipient's queue slot
// for the next twenty seconds — turning a clock correction into a stall on
// every restart and breaking the invariant that a reseed claims no slot it
// did not have. The truthful reading is the opposite: the send happened at
// some unknown moment in the past, and any moment far enough back is
// right for every reader of this field.
func emissionStampNotInTheFuture(stamp, now time.Time) time.Time {
	if stamp.After(now) {
		return now.Add(-deliveryQueueWindow)
	}
	return stamp
}

// deliveryQueueOrder reports whether a sorts before b in a recipient's
// delivery queue. Messages leave in the order they were written; the id
// breaks ties, because two messages of the same second must still have ONE
// order and Go map iteration does not provide one.
func deliveryQueueOrder(a, b *deliveryRetryEntry) bool {
	if !a.Envelope.CreatedAt.Equal(b.Envelope.CreatedAt) {
		return a.Envelope.CreatedAt.Before(b.Envelope.CreatedAt)
	}
	return a.Envelope.ID < b.Envelope.ID
}

// registerAwaitingSeenAckLocked schedules the locally-sent seen receipt for
// retry until the original sender's seen_ack arrives. Caller MUST hold
// s.deliveryMu.Lock. Idempotent per MessageID.
func (s *Service) registerAwaitingSeenAckLocked(receipt protocol.DeliveryReceipt, now time.Time) {
	if _, exists := s.awaitingSeenAck[receipt.MessageID]; exists {
		return
	}
	s.awaitingSeenAck[receipt.MessageID] = &seenAckRetryEntry{
		Receipt: receipt,
		// Attempts counts RE-SENDS, and starts at zero: the send this
		// registration accompanies has not happened yet — distributeReceipt
		// runs after the lock stack unwinds — and the budget in
		// CORSA_DELIVERY_RETRY_MAX_ATTEMPTS is a number of re-sends, so
		// charging the original would spend one of them on a send that
		// may not have reached any transport at all.
		//
		// This is why the two retry machines schedule differently, and
		// why the message path's deliveryRetryBackoffAfter does NOT
		// belong here. A message is SENT at registration, so its first
		// wait follows an attempt already made. A seen receipt is
		// registered with its first wait ahead of it: the wait set here
		// IS the first step, and each retry then takes the next.
		NextAttemptAt: now.Add(deliveryRetryBackoff(0)),
	}
}

// RegisterDeliveryOutbox reseeds the delivery retry scheduler from the
// durable outbox (chatlog rows still in "sent"). Called once by the desktop
// layer right after RegisterMessageStore, before Run.
func (s *Service) RegisterDeliveryOutbox(outbox DeliveryOutbox) {
	if outbox == nil {
		return
	}

	// Lightweight durable-journal refs are wired up ALWAYS — even under DM
	// opt-out. A DisableDirectMessages node still ORIGINATES DMs (the opt-out
	// only gates INBOUND via dropsInboundDM), so an abandoned outbound
	// delivery must persist to the failure journal (failDelivery →
	// MarkDeliveryFailed) and an arriving seen_ack must persist back — so a
	// restart neither resurrects an abandoned retry nor re-arms a confirmed
	// seen receipt. These are pointer stores, not scans.
	if failureJournal, ok := outbox.(DeliveryFailureJournal); ok {
		s.deliveryFailureJournal = failureJournal
	}
	if emissionJournal, ok := outbox.(DeliveryEmissionJournal); ok {
		s.emissionJournal = emissionJournal
	}
	seenJournal, hasSeenJournal := outbox.(SeenAckJournal)
	if hasSeenJournal {
		s.seenAckJournal = seenJournal
	}

	// The startup scan/reseed (UndeliveredOutgoing + UnconfirmedSeen) runs
	// for every node that registers an outbox, DM opt-out included. Opting
	// out refuses INCOMING direct messages; it says nothing about the
	// node's own outgoing ones, and the rows waiting in the outbox after a
	// restart are exactly those. Skipping the scan there stranded them:
	// nothing else re-arms a retry for a message whose process died.
	now := time.Now().UTC()

	// The solicited-receipt gate first: a `delivered` message is NOT
	// reseeded for retry, and after a restart nothing else remembers that
	// we sent it — so its `seen`, which arrives whenever the recipient
	// opens the conversation, would be dropped as unsolicited and their
	// seen retry would never be acked.
	if ids, err := outbox.SentMessageIDs(maxSentDMIDs); err != nil {
		log.Error().Err(err).Msg("delivery_retry_sent_ids_reseed_failed")
	} else {
		// OLDEST FIRST into the LRU, from a list that arrives newest
		// first. Asking for exactly the LRU's capacity already means the
		// reseed alone cannot overflow it, so this is defence for the
		// case where the set is not empty: inserting in arrival order
		// would then make the NEWEST ids the first evicted, leaving
		// exactly the messages whose receipts are still coming outside
		// the gate. There is no test for it — through the real interface
		// the limit makes the overflow unreachable, and a test that
		// cannot fail is worse than none.
		for i := len(ids) - 1; i >= 0; i-- {
			s.sentDMIDs.Add(string(ids[i]))
		}
		if len(ids) > 0 {
			log.Debug().Int("count", len(ids)).Msg("delivery_retry_sent_ids_reseeded")
		}
	}

	// No opt-out shortcut here, and that is deliberate. DM opt-out refuses
	// INCOMING messages; the node still authors its own, and those are
	// exactly what this scan restores. Skipping it left such a node
	// retrying an outgoing DM until the process stopped and then losing
	// the retry entirely at the next start — the message stayed in the
	// chatlog as `sent` and nothing would ever put it on the wire again,
	// online trigger included. A node that really has no outgoing backlog
	// pays one query that returns no rows.
	entries, err := outbox.UndeliveredOutgoing()
	if err != nil {
		log.Error().Err(err).Msg("delivery_retry_outbox_reseed_failed")
	} else if len(entries) > 0 {
		s.deliveryMu.Lock()
		for _, row := range entries {
			// Reseeded from chatlog on restart: held=false, and DUE
			// IMMEDIATELY (NextAttemptAt=now) rather than now+first-backoff,
			// so the first retry tick evaluates reachability and either
			// sends or flips Held, instead of every reseeded undelivered DM
			// idling a full backoff interval before its first attempt.
			s.registerAwaitingDeliveredLocked(row.Envelope, now, false)
			e, ok := s.awaitingDelivered[row.Envelope.ID]
			if !ok {
				continue
			}
			e.NextAttemptAt = now
			e.Emitted = row.Emitted
			// The two bits are read SEPARATELY, because they answer
			// different questions with opposite safe defaults.
			//
			// Emitted ("may a writer have taken this?") comes from the
			// never-emitted claim and decides what a deletion may skip.
			// OnWire ("did a sink confirm it?") comes from its own stamp
			// and decides what the sender sees. Deriving the second from
			// the first is what used to make the whole flag non-monotone.
			if row.OnWire {
				// A confirmed row is stamped with the message's own
				// creation time: the real instant is unknowable after a
				// restart, and any past one is right for every reader —
				// far outside the queue window, so it claims no slot.
				//
				// PAST is the load-bearing word. CreatedAt is a wall
				// clock reading kept across restarts, and a clock moved
				// backwards leaves rows stamped in the future; the queue
				// window then reads a negative age as "emitted moments
				// ago" and holds the recipient's slot until that date
				// arrives — hours or days, not twenty seconds.
				e.LastEmittedAt = emissionStampNotInTheFuture(row.Envelope.CreatedAt, now)
			} else {
				e.LastEmittedAt = time.Time{}
			}
			if row.Emitted {
				// Registration assumes the row was BORN carrying the
				// claim, which is true of every message this node
				// authors — but this row's claim has since been
				// withdrawn, and the outbox is the witness for that.
				// Leaving the id in the set would send the first retry to
				// SQLite to withdraw a claim that is not there, and —
				// worse — would let a database that is briefly
				// unavailable withhold the frame, because a failed
				// withdrawal withholds the send by contract. A returning
				// recipient would then wait on a write with nothing to
				// write.
				delete(s.markedNeverEmitted, row.Envelope.ID)
			}
			// The conversation is read fresh after a restart, so what the
			// sender is looking at comes from the row — and from the bit
			// that answers THEIR question. A row with no on-wire stamp
			// reads as queued and is still owed the transition; a stamped
			// one already reads as sent.
			e.Announced = row.OnWire
			// The row already carries the stamp, so the repair pass has
			// no durable debt for it — only, at most, an announcement.
			e.Stamped = row.OnWire
		}
		count := len(s.awaitingDelivered)
		standing := len(s.markedNeverEmitted)
		s.deliveryMu.Unlock()
		log.Info().Int("reseeded", len(entries)).Int("awaiting_delivered", count).
			Int("standing_claims", standing).Msg("delivery_retry_outbox_reseeded")
	}

	if !hasSeenJournal {
		return
	}
	receipts, err := seenJournal.UnconfirmedSeen()
	if err != nil {
		log.Error().Err(err).Msg("seen_ack_journal_reseed_failed")
		return
	}
	if len(receipts) == 0 {
		return
	}
	s.deliveryMu.Lock()
	for _, receipt := range receipts {
		s.registerAwaitingSeenAckLocked(receipt, now)
	}
	seenCount := len(s.awaitingSeenAck)
	s.deliveryMu.Unlock()
	log.Info().Int("reseeded", len(receipts)).Int("awaiting_seen_ack", seenCount).Msg("seen_ack_journal_reseeded")
}

// abandonedDelivery is a message the tick gave up on, carried out of the
// mutex so failDelivery can run with every domain lock released.
type abandonedDelivery struct {
	envelope protocol.Envelope
	status   string
	reason   string
}

// dueCandidate is the one message of a recipient's queue that this tick may
// act on. The Held decision is taken after the lock-free reachability check
// and written atomically with the schedule, so no kick observes a
// half-updated entry.
type dueCandidate struct {
	id  protocol.MessageID
	env protocol.Envelope
}

// dueDispatch is one envelope the tick may put on the wire, plus the
// schedule it had before the pass parked it. Arming moves the schedule to
// the poll interval so an unanswered dispatch does not re-fire every two
// seconds — but a message a WIPE froze in the meantime never reached any
// sink, and must come back from the thaw due exactly when it was, not a
// poll interval later for a decision its recipient had no part in.
type dueDispatch struct {
	env      protocol.Envelope
	parkedAt time.Time
}

// retryDueDeliveries advances the sender-owned delivery queues by one step.
// Called from bootstrapLoop on its 2s tick; the schedule inside the entries
// provides the real pacing. Snapshots and schedule writes happen under
// s.deliveryMu; the sends run after release.
func (s *Service) retryDueDeliveries(now time.Time) {
	abandoned, candidates := s.planDueDeliveries(now)
	dueReceipts := s.planDueSeenAcks(now)

	// Reachability runs LOCK-FREE: router.Route reads routing/peer state
	// under its own locks, and taking those under deliveryMu would invert
	// the canonical peerMu → deliveryMu order. With the gate off every
	// candidate is "reachable" — the legacy unconditional send.
	reachable := make(map[protocol.MessageID]bool, len(candidates))
	for _, c := range candidates {
		if !s.cfg.HoldDMUntilReachable {
			reachable[c.id] = true
			continue
		}
		d := s.router.Route(c.env)
		reachable[c.id] = d.RelayNextHop != nil || len(d.PushSubscribers) > 0
	}

	dueMessages := s.armDueDeliveries(candidates, reachable, now)

	for _, entry := range abandoned {
		s.failDelivery(entry.envelope, entry.status, entry.reason)
	}
	// TEST-ONLY interleaving point: everything above ran under the
	// delivery mutex, everything below decides at the last boundary
	// whether the frame may go out. The window between them is where a
	// deletion's freeze lands, and it lives entirely between two
	// statements of this function — a test that approximated it with a
	// sleep would pin the scheduler instead. No mutex is held here.
	s.runRetryDispatchBarrier()
	for _, due := range dueMessages {
		s.emitDueDelivery(due, now)
	}
	// Local bookkeeping the wire already earned: a stamp the journal
	// refused, an announcement or a receipt update the bus shed. Redone
	// as BOOKKEEPING rather than by re-sending the frame.
	//
	// AFTER the dispatch above, not before. The emission lane already
	// keeps this pass from getting ahead of a pre-wire clear, so this is
	// no longer the mechanism — but starting it second still costs
	// nothing and states the intent where a reader meets it.
	s.startDeliveryRecordRepair(now)
	for _, receipt := range dueReceipts {
		log.Info().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("seen_receipt_retry_resend")
		s.distributeReceipt(receipt)
	}
}

// planDueDeliveries sweeps the awaiting set under s.deliveryMu and returns
// what this tick may act on: the deliveries to abandon, and AT MOST ONE
// candidate per recipient.
//
// One per recipient is the queue discipline the whole file exists to serve.
// A recipient who was away holds a backlog, and that backlog is a
// conversation: it has to arrive in the order it was written. Emitting the
// whole set at once cannot do that — the sends are handed to background
// goroutines, so the order they reach the wire is the scheduler's, not
// ours — and neither can iterating a Go map, whose order is randomised per
// process. So the queue advances by one message, oldest first, and the next
// one leaves when the recipient confirms the current one (storeDeliveryReceipt
// promotes it) or when deliveryQueueWindow says the confirmation is not
// coming.
func (s *Service) planDueDeliveries(now time.Time) ([]abandonedDelivery, []dueCandidate) {
	var abandoned []abandonedDelivery
	queues := make(map[string][]*deliveryRetryEntry)

	log.Trace().Str("site", "planDueDeliveries").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "planDueDeliveries").Str("phase", "lock_held").Msg("delivery_mu_writer")
	for id, entry := range s.awaitingDelivered {
		if terminal, ok := s.classifyAbandonedLocked(id, entry, now); ok {
			delete(s.awaitingDelivered, id)
			abandoned = append(abandoned, terminal)
			continue
		}
		queues[entry.Envelope.Recipient] = append(queues[entry.Envelope.Recipient], entry)
	}
	candidates := s.selectQueueHeadsLocked(queues, now)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "planDueDeliveries").Str("phase", "lock_released").Msg("delivery_mu_writer")
	return abandoned, candidates
}

// classifyAbandonedLocked reports the terminal verdict for one awaiting
// entry, if it has one. There is exactly one — its own TTL. Caller MUST
// hold s.deliveryMu.Lock.
func (s *Service) classifyAbandonedLocked(id protocol.MessageID, entry *deliveryRetryEntry, now time.Time) (abandonedDelivery, bool) {
	if s.deliveryFrozenLocked(id) {
		// A wipe is deciding about this message. Nothing about its
		// delivery moves while that lasts — terminalizing it here would
		// answer, behind the wipe's back, a question the wipe is in the
		// middle of asking, and would journal an abandonment for a row
		// that may be about to be deleted anyway.
		return abandonedDelivery{}, false
	}
	// TTL bounds the delivery lifetime (docs/protocol/messaging.md) — the
	// retry engine honours it the same way the relay retry loop does,
	// instead of re-emitting an envelope receivers would reject as expired.
	// Ordinary DMs carry TTLSeconds=0, so this is the auto-delete case and
	// nothing else: the message was sent to expire, and it has.
	if s.messageDeliveryExpiredAt(now, entry.Envelope.CreatedAt, entry.Envelope.TTLSeconds) {
		log.Warn().Str("message_id", string(id)).Str("recipient", entry.Envelope.Recipient).Msg("delivery_retry_expired_ttl")
		return abandonedDelivery{entry.Envelope, "expired", "message delivery expired"}, true
	}
	return abandonedDelivery{}, false
}

// selectQueueHeadsLocked picks the one message per recipient this tick may
// send. Caller MUST hold s.deliveryMu.Lock (it reads the freeze set).
//
// Recipients are walked in sorted order so a tick is reproducible; within
// a recipient the queue is oldest-first (deliveryQueueOrder).
func (s *Service) selectQueueHeadsLocked(queues map[string][]*deliveryRetryEntry, now time.Time) []dueCandidate {
	recipients := make([]string, 0, len(queues))
	for recipient := range queues {
		recipients = append(recipients, recipient)
	}
	sort.Strings(recipients)

	var candidates []dueCandidate
	for _, recipient := range recipients {
		queue := queues[recipient]
		sort.Slice(queue, func(i, j int) bool { return deliveryQueueOrder(queue[i], queue[j]) })
		if head, ok := s.pickQueueHeadLocked(queue, now); ok {
			candidates = append(candidates, head)
		}
	}
	return candidates
}

// pickQueueHeadLocked walks one recipient's queue oldest-first and returns
// the message that may go out now, if any. Caller MUST hold
// s.deliveryMu.Lock.
//
// The rule it implements, stated as the guarantee it buys: A MESSAGE THAT
// HAS NEVER BEEN EMITTED IS NEVER OVERTAKEN BY A NEWER ONE. That is the
// ordering the reader actually perceives — once a message has gone out,
// the recipient either has it or it is in transit, and a newer one passing
// it on the sender's side reorders nothing they will see.
//
// Which is why the three cases below are not symmetric:
//   - a message still in its queue window owns the slot outright;
//   - a NEVER-EMITTED message that is not due yet still holds its place,
//     because letting the next one past would be a real reordering;
//   - an already-emitted message that is merely waiting out its backoff
//     steps aside, because otherwise one lost receipt would stall the
//     whole conversation behind it for up to eleven minutes.
func (s *Service) pickQueueHeadLocked(queue []*deliveryRetryEntry, now time.Time) (dueCandidate, bool) {
	for _, entry := range queue {
		id := entry.Envelope.ID
		if s.deliveryFrozenLocked(id) {
			// A wipe is deciding whether the peer may ever hold this
			// message, and a freeze has no expiry of its own — it ends
			// when the deletion commits or aborts. So it is the one thing
			// allowed past the rule above: a stuck freeze must cost its
			// own message, never the whole conversation.
			continue
		}
		if !entry.LastEmittedAt.IsZero() {
			// Repaired here as well as at the reseed that invents these,
			// because the window is a DURATION and this is the place a
			// bad stamp does its damage: left alone it reads as "sent a
			// moment ago" for as long as the clock takes to catch up, and
			// the whole conversation waits behind it.
			entry.LastEmittedAt = emissionStampNotInTheFuture(entry.LastEmittedAt, now)
			if now.Sub(entry.LastEmittedAt) < deliveryQueueWindow {
				return dueCandidate{}, false
			}
		}
		if entry.NextAttemptAt.After(now) {
			if entry.Hold == holdNone {
				// Confirmed on the wire: the recipient has it or it is in
				// transit, so a newer message passing it here reorders
				// nothing they will see, and letting it pass is what keeps
				// one lost receipt from stalling the conversation.
				continue
			}
			// Never confirmed — the sinks may have refused it, and the
			// place to find that out is here. It keeps its slot, because
			// letting the next message overtake would be a real
			// reordering. Emitted must NOT be consulted: it is the
			// conservative "might the peer have it" flag and is already
			// true for an attempt no writer took.
			return dueCandidate{}, false
		}
		return dueCandidate{id, entry.Envelope}, true
	}
	return dueCandidate{}, false
}

// planDueSeenAcks bumps and snapshots the seen receipts due for a re-send.
// Unlike messages these are not queued per recipient: a seen receipt names
// one message and carries no order relative to the others.
func (s *Service) planDueSeenAcks(now time.Time) []protocol.DeliveryReceipt {
	maxAttempts := s.deliveryRetryMaxAttempts()
	var due []protocol.DeliveryReceipt
	s.deliveryMu.Lock()
	for id, entry := range s.awaitingSeenAck {
		if entry.NextAttemptAt.After(now) {
			continue
		}
		if entry.Attempts >= maxAttempts {
			delete(s.awaitingSeenAck, id)
			log.Warn().Str("message_id", string(id)).Str("recipient", entry.Receipt.Recipient).Int("attempts", entry.Attempts).Msg("seen_ack_retry_exhausted")
			continue
		}
		// backoff, not backoffAfter: Attempts counts re-sends and the
		// schedule is indexed by the wait ahead, so after the first
		// re-send (Attempts == 1) the next wait is the schedule's second
		// step — the first step was served between registration and
		// this re-send. See registerAwaitingSeenAckLocked.
		entry.Attempts++
		entry.NextAttemptAt = now.Add(deliveryRetryBackoff(entry.Attempts))
		due = append(due, entry.Receipt)
	}
	s.deliveryMu.Unlock()
	return due
}

// armDueDeliveries writes the Held decision atomically with the schedule
// and returns the envelopes the dispatch loop may try. An unreachable
// candidate is HELD — re-armed by kickDeliveryRetriesForReachable the
// moment a route or connection appears, and re-checked locally every
// deliveryHoldPollInterval until then. Writing Held under the same lock
// that moves the schedule closes the false→true lost-wakeup window a
// post-dispatch write left open.
//
// Nothing is charged here. The attempt is spent by emitDueDelivery, after
// the wire has actually taken the frame.
func (s *Service) armDueDeliveries(candidates []dueCandidate, reachable map[protocol.MessageID]bool, now time.Time) []dueDispatch {
	if len(candidates) == 0 {
		return nil
	}
	var due []dueDispatch
	s.deliveryMu.Lock()
	for _, c := range candidates {
		entry, ok := s.awaitingDelivered[c.id]
		if !ok {
			continue // delivered/removed in the unlocked gap
		}
		if reachable[c.id] {
			s.resetBackoffOnReturn(entry, c.id)
			// Attempted, not confirmed. The hold is only cleared by a sink
			// reporting that it took the frame (confirmEnvelopeOnWire), so
			// a dispatch nobody accepts leaves the entry exactly where a
			// reachability kick can find it. The schedule is parked on the
			// poll interval meanwhile; a confirmation overwrites it with
			// the real backoff, and a freeze restores what it displaced.
			parkedAt := entry.NextAttemptAt
			entry.Hold = holdUnconfirmed
			entry.NextAttemptAt = now.Add(deliveryHoldPollInterval)
			due = append(due, dueDispatch{env: entry.Envelope, parkedAt: parkedAt})
			continue
		}
		entry.Hold = holdUnreachable
		entry.NextAttemptAt = now.Add(deliveryHoldPollInterval)
		if !s.lastReachabilityKickAt.Before(now) {
			// A reachability kick landed while this pass was deciding, so
			// the "unreachable" answer it is acting on may already be out
			// of date — and the entry was not yet held when the kick
			// looked, so the kick could not wake it either. Leaving it due
			// costs one local routing lookup on the next tick; parking it
			// would cost the recipient a poll interval of silence right
			// after they came back.
			entry.NextAttemptAt = now
		}
	}
	s.deliveryMu.Unlock()
	return due
}

// resetBackoffOnReturn puts an entry back on the fast end of the schedule
// because its recipient has just become reachable again.
//
// Held is the transition marker and the only one needed: it says the last
// decision about this message was "they are not there". So Held plus
// reachable is exactly the moment they came back, and it cannot fire for
// a peer that never left — those entries are not Held, and every caller
// filters on it.
//
// The reset matters because Attempts indexes the backoff. Without it, a
// message that spent an evening climbing to the eleven-minute step would
// keep that step after its recipient walks back in, and the person who
// just came online would wait a quarter of an hour for a message that has
// been ready since yesterday. Caller MUST hold s.deliveryMu.Lock.
func (s *Service) resetBackoffOnReturn(entry *deliveryRetryEntry, id protocol.MessageID) {
	// With the reachability gate off nothing is ever genuinely held, so
	// there is no such thing as a return to observe — and leaving the
	// schedule alone keeps flag-off behaviour identical to the legacy
	// baseline, the same contract kickDeliveryRetriesForReachable keeps.
	if !s.cfg.HoldDMUntilReachable {
		return
	}
	if entry.Hold != holdUnreachable || entry.Attempts == 0 {
		return
	}
	log.Debug().Str("message_id", string(id)).Str("recipient", entry.Envelope.Recipient).
		Int("attempts", entry.Attempts).Msg("delivery_retry_backoff_reset_on_return")
	entry.Attempts = 0
}

// emitDueDelivery puts one envelope on the wire and records what that cost.
//
// The candidate was chosen phases ago, and a deletion can have frozen it
// since — the wipe classifies against the delivery domain and then destroys
// the row, so an envelope that goes out after that classification stays
// with the peer with nothing left to recall it.
//
// So the claim is taken HERE, immediately before the wire, in the same lock
// hold that reads the freeze and the withdrawal shadow. It also withdraws
// the durable never-emitted claim, and refuses the attempt when that write
// fails: sending anyway would leave the disk saying the message never left
// while the peer holds it.
//
// Claiming BEFORE the attempt rather than after costs one thing,
// deliberately: a dispatch that then fails to emit (its route vanished in
// the microsecond gap) leaves the entry counted as possibly-out, so a later
// deletion asks the peer about an id they may not have. That is the
// harmless direction, and it is the price of the freeze meaning anything
// at all.
func (s *Service) emitDueDelivery(due dueDispatch, now time.Time) {
	envelope := due.env
	log.Info().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).Msg("delivery_retry_resend")
	if !s.noteOwnEnvelopeEmitted(envelope.Sender, envelope.ID, now) {
		// Frozen by a deletion, withdrawn, or its durable claim could not
		// be withdrawn. Nothing reached the wire, so nothing is charged —
		// and this is not the recipient's doing, so it is not a statement
		// about their reachability. A freeze also gives the schedule back:
		// this node paused itself, and a thawed message must go out when
		// it was due rather than pay for the pause.
		s.holdDeliveryRetry(envelope.ID, due.parkedAt, holdUnconfirmed)
		return
	}
	if !s.dispatchEnvelopeRetry(envelope, now) {
		// The route vanished between the reachability check and the wire:
		// that IS a statement about the recipient, so it holds as
		// unreachable and a returning peer resets the backoff.
		s.holdDeliveryRetry(envelope.ID, now, holdUnreachable)
		return
	}
	// Nothing is charged, cleared or announced here. The sinks answer on
	// their own schedules and each confirms for itself; the entry stays
	// holdUnconfirmed until one of them does.
}

// confirmEnvelopeOnWire is the ONE place a delivery becomes "sent", and it
// is called by the sink that accepted the frame — never by the code that
// merely decided to try.
//
// That inversion is the point. A dispatch hands the envelope to sinks that
// answer on different schedules: a relay enqueue answers immediately, a
// subscriber push answers from a background goroutine. Charging the
// attempt and clearing the hold at dispatch time meant a message that no
// writer ever took was recorded as sent, shown as sent, and left with
// Hold cleared so no reconnect could wake it. Now nothing moves until a
// sink says it took the frame; if none ever does, the entry simply stays
// holdUnconfirmed and the next pass tries again.
//
// dispatchedAt identifies the attempt, so several sinks confirming the
// same dispatch charge it once. A later dispatch carries a later stamp and
// is charged separately.
//
// A message with no entry — transit traffic, or one whose receipt already
// arrived — is not ours to account for, and this is a no-op for it.
func (s *Service) confirmEnvelopeOnWire(envelope protocol.Envelope, dispatchedAt time.Time) {
	if !s.confirmEnvelopeInMemory(envelope, dispatchedAt) {
		return
	}
	// The durable stamp FIRST, the announcement only if it lands, both on
	// one background goroutine — off the writer loop, because the journal
	// writes through SQLite where contention can park a statement for the
	// whole busy timeout.
	//
	// The order and the condition are the same rule twice. The event moves
	// the conversation cache to "sent" while a full reload reads the row,
	// so an event that runs before the write — or without it — lets the
	// next reload put the badge back to "queued", and no further event
	// comes because the announcement is claimed once. Staying at queued
	// until the disk agrees is the direction that cannot go backwards.
	s.goBackground(func() {
		s.confirmEnvelopesDurably([]protocol.Envelope{envelope}, s.owesAnnouncement(envelope.ID))
	})
}

// confirmEnvelopeInMemory is the delivery-domain half, split out so a
// caller with a WHOLE BATCH to confirm — the reconnect backlog — can move
// every entry first and pay for the durable half once.
//
// Reports whether this call is the one that confirmed the dispatch.
func (s *Service) confirmEnvelopeInMemory(envelope protocol.Envelope, dispatchedAt time.Time) bool {
	var confirmedNow bool
	s.deliveryMu.Lock()
	entry, awaiting := s.awaitingDelivered[envelope.ID]
	if awaiting && entry.LastEmittedAt.Before(dispatchedAt) {
		entry.Attempts++
		entry.NextAttemptAt = dispatchedAt.Add(deliveryRetryBackoffAfter(entry.Attempts))
		entry.Hold = holdNone
		// A frame NetCore has taken is a frame the peer may hold, so the
		// conservative deletion flag is true from here whatever path got
		// it there. Setting it only in the pre-write claim left the paths
		// that reach a writer without one — the origin send's directed
		// relay, the pending ring's flush — reporting a delivered message
		// as never emitted, which tells a deletion to skip a peer holding
		// it.
		entry.Emitted = true
		entry.LastEmittedAt = dispatchedAt
		// The durable half is stamped outside this mutex, by the caller.
		// There is no handshake with anything: the bit is monotone, so a
		// refusal racing this confirmation cannot unset it, and nothing
		// has to be corrected afterwards.
		confirmedNow = true
	}
	s.deliveryMu.Unlock()

	return confirmedNow
}

// owesAnnouncement reports whether the sender still has to be told this
// message left the machine. Read separately from the confirmation because
// the two are answered at different moments: the entry moves under the
// delivery mutex, the announcement waits for the disk.
func (s *Service) owesAnnouncement(id protocol.MessageID) bool {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	entry, awaiting := s.awaitingDelivered[id]
	return awaiting && !entry.Announced
}

// repairLocalDeliveryRecord finishes the local bookkeeping for deliveries
// the WIRE has already accepted.
//
// Three local failures leave a confirmed message half-recorded, and none
// is the network's fault: the journal can refuse the on-wire stamp
// ("database is locked"), and the event bus can shed either the queued →
// sent announcement or the receipt update that supersedes it — which a
// reconnect replaying a whole conversation makes likely, because they
// arrive as a burst. Retrying any of them by SENDING THE FRAME AGAIN is
// the wrong repair for a local problem: it puts a message the peer already
// took back on the wire.
//
// This pass is a PURE FUNCTION OF STATE, and that shape is the whole
// design rather than an implementation detail. Its first version was a
// second retry machine standing beside the delivery tick — a dirty-hint
// flag, a take-and-delete queue, an early return, a priority counter —
// and every one of those grew its own defect: a hint cleared while work
// was still owed, an event deleted before it was published and lost when
// the pass returned early, a priority check that was not atomic with the
// acquisition it was meant to order.
//
// So it holds NOTHING of its own. It reads what is owed, tries, and
// removes each debt only when the write that settles it has landed. A
// pass that is skipped, that returns early, or that dies loses nothing:
// the next one reads the same state and tries again. There is no flag to
// get stale and no queue to drop an item on the floor.
//
// Two rules stay because they are about pacing, not correctness. The
// halves are taken OLDEST FIRST so a backlog drains in the order the user
// wrote it rather than re-rolling a random sample of the map; and a shed
// announcement waits out deliveryAnnounceRetry, because an inbox that is
// already full sheds every new event whatever the batch size — the cap
// bounds one pass, the backoff is what makes the passes converge.
func (s *Service) repairLocalDeliveryRecord(now time.Time) {
	for _, pending := range s.dueShedEvents(now) {
		// Removed by the publish itself, on success, and only if the map
		// still holds THIS snapshot — a newer status may have arrived
		// while the copy was in flight.
		s.publishRetryableSnapshot(pending.topic, pending.event, pending.seq)
	}
	unstamped, unannounced := s.localDeliveryDebt(now)
	if len(unstamped) > 0 {
		log.Debug().Int("count", len(unstamped)).Msg("delivery_repairing_durable_stamp")
		if ids := idsOf(unstamped); s.markDeliveryOnWire(ids...) {
			s.noteStamped(ids)
			// Announced in the SAME pass, and only now: the stamp had to
			// land first, and making these wait for the next tick would
			// add two seconds to a badge for no reason.
			s.publishMessagesEmitted(unstamped)
		}
	}
	if len(unannounced) > 0 {
		log.Debug().Int("count", len(unannounced)).Msg("delivery_repairing_announcement")
		s.publishMessagesEmitted(unannounced)
	}
}

// startDeliveryRecordRepair runs one repair pass off the tick, and only
// one at a time: the pass can block on the journal, and a contended
// database would otherwise accumulate a goroutine every two seconds.
//
// Called AFTER the tick has put its due messages on the wire. What keeps
// the pass out of their way is the emission lane, which admits a pre-wire
// clear ahead of any bookkeeping queued behind it; going second is the
// same intent stated where a reader meets it first.
func (s *Service) startDeliveryRecordRepair(now time.Time) {
	select {
	case s.repairSlot <- struct{}{}:
	default:
		// A pass is still working. The next tick offers again.
		return
	}
	// goRunLoop, not goBackground: this pass touches the journal, so a
	// shutdown has to WAIT for it rather than merely ask — Run returning
	// underneath an in-flight SQLite statement is exactly what the
	// lifecycle group exists to prevent.
	s.goRunLoop(func() {
		defer func() { <-s.repairSlot }()
		s.repairLocalDeliveryRecord(now)
	})
}

// localDeliveryDebt reads what still owes local work, oldest first and
// capped. It REMOVES nothing: the debts live on the entries, and each is
// settled by the write that clears it. Caller must hold no domain mutex.
func (s *Service) localDeliveryDebt(now time.Time) (unstamped, unannounced []protocol.Envelope) {
	var stampDue, announceDue []*deliveryRetryEntry
	s.deliveryMu.RLock()
	for _, entry := range s.awaitingDelivered {
		if !entry.confirmed() {
			continue
		}
		switch {
		case !entry.Stamped:
			stampDue = append(stampDue, entry)
		case !entry.Announced && !entry.AnnounceAfter.After(now):
			announceDue = append(announceDue, entry)
		}
	}
	s.deliveryMu.RUnlock()
	return envelopesOldestFirst(stampDue), envelopesOldestFirst(announceDue)
}

// envelopesOldestFirst sorts by the same rule the delivery queue uses and
// takes the head, so repeated passes make progress on the same messages
// instead of sampling the map afresh.
func envelopesOldestFirst(entries []*deliveryRetryEntry) []protocol.Envelope {
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return deliveryQueueOrder(entries[i], entries[j]) })
	if len(entries) > deliveryRepairBatch {
		entries = entries[:deliveryRepairBatch]
	}
	out := make([]protocol.Envelope, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Envelope)
	}
	return out
}

func idsOf(envelopes []protocol.Envelope) []protocol.MessageID {
	ids := make([]protocol.MessageID, 0, len(envelopes))
	for _, envelope := range envelopes {
		ids = append(ids, envelope.ID)
	}
	return ids
}

// deliveryRepairBatch bounds one repair pass, so a backlog is redone in
// steady slices rather than all at once.
const deliveryRepairBatch = 16

// deliveryAnnounceRetry is how long a shed announcement waits before the
// repair pass offers it again. It has to outlast the burst that filled the
// inbox; a subscriber that drains at all clears 64 slots well inside it.
const deliveryAnnounceRetry = 10 * time.Second

// pendingUIEvent is one local-change event the bus shed, and when it may
// be offered again.
type pendingUIEvent struct {
	topic string
	event protocol.LocalChangeEvent
	after time.Time
	// seq identifies THIS snapshot of the debt.
	//
	// The pass publishes a copy while the map can move on: a `seen` for
	// the same message can arrive while an older `delivered` is in flight.
	// Without it the successful publish of the stale copy deleted the
	// newer one, and a re-shed of the stale copy overwrote it — either way
	// the badge settled on a status the peer had already moved past.
	seq uint64
}

// localChangeRank orders the delivery statuses these events carry. A kept
// event is only ever replaced by a LATER one; the client applies statuses
// monotonically, so re-offering an older one would be noise at best and a
// regression at worst.
func localChangeRank(status string) int {
	switch status {
	case protocol.MessageStatusSent:
		return 1
	case protocol.ReceiptStatusDelivered:
		return 2
	case protocol.ReceiptStatusSeen:
		return 3
	default:
		return 0
	}
}

// publishRetryableLocalChange offers an event to the local client and, if
// the bus sheds it, keeps it for the repair pass.
//
// The bus is deliberately lossy — a publisher never blocks on a wedged
// subscriber — which is right for a stream of notifications and wrong for
// the LAST event that can correct a badge. This is the narrow bridge: the
// event is kept, not the whole stream, and only until it lands.
func (s *Service) publishRetryableLocalChange(topic string, event protocol.LocalChangeEvent) {
	s.publishRetryableSnapshot(topic, event, 0)
}

// publishRetryableSnapshot is the form the repair pass uses: seq names the
// kept snapshot it is republishing, so a success removes THAT one and not
// whatever the map holds by then. A fresh publish passes 0, which matches
// nothing and therefore removes nothing it did not put there.
func (s *Service) publishRetryableSnapshot(topic string, event protocol.LocalChangeEvent, seq uint64) {
	s.emitLocalChange(event)
	if _, dropped := s.eventBus.PublishReporting(topic, event); dropped == 0 {
		// Settled — and this is the ONLY place a kept event is removed.
		// Taking it out before the publish let a pass that then returned
		// early drop it for good; removing it unconditionally let a stale
		// copy's success delete a newer status that had arrived since.
		s.forgetShedEvent(protocol.MessageID(event.MessageID), seq, event.Status)
		return
	}
	s.keepShedEvent(topic, event)
}

// receiptUpdateEvent is the one shape of the receipt-update event, so the
// handler and the repair pass cannot describe the same receipt differently.
func receiptUpdateEvent(receipt protocol.DeliveryReceipt) protocol.LocalChangeEvent {
	return protocol.LocalChangeEvent{
		Type:        protocol.LocalChangeReceiptUpdate,
		Topic:       "dm",
		MessageID:   string(receipt.MessageID),
		Sender:      receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt,
	}
}

// keepShedEvent stores a shed event for a later pass. Keyed by message id,
// so a newer status for the same message replaces an older one rather than
// queueing behind it — the client applies statuses monotonically, and the
// latest is the only one worth repeating.
func (s *Service) keepShedEvent(topic string, event protocol.LocalChangeEvent) {
	next := time.Now().UTC().Add(deliveryAnnounceRetry)
	s.deliveryMu.Lock()
	if s.pendingUIEvents == nil {
		s.pendingUIEvents = make(map[protocol.MessageID]pendingUIEvent, 1)
	}
	id := protocol.MessageID(event.MessageID)
	existing, known := s.pendingUIEvents[id]
	switch {
	case known && localChangeRank(event.Status) < localChangeRank(existing.event.Status):
		// A re-shed of a stale in-flight copy. Keeping it would move the
		// badge backwards from a status that has already arrived.
	case known || len(s.pendingUIEvents) < maxPendingUIEvents:
		// An id already in the set is UPDATED whatever the capacity: the
		// cap bounds how many distinct MESSAGES are kept, and refusing a
		// newer status for one already there would pin the badge to the
		// older one — the opposite of what keying by id is for.
		s.pendingEventSeq++
		s.pendingUIEvents[id] = pendingUIEvent{topic, event, next, s.pendingEventSeq}
	}
	s.deliveryMu.Unlock()
}

// forgetShedEvent removes a settled debt, but only if the map still holds
// the snapshot that was published — or one this publish supersedes.
func (s *Service) forgetShedEvent(id protocol.MessageID, seq uint64, status string) {
	s.deliveryMu.Lock()
	if existing, known := s.pendingUIEvents[id]; known {
		superseded := existing.seq == seq ||
			localChangeRank(existing.event.Status) <= localChangeRank(status)
		if superseded {
			delete(s.pendingUIEvents, id)
		}
	}
	s.deliveryMu.Unlock()
}

// dueShedEvents READS the events whose backoff has expired, capped like
// every other half of the pass.
//
// It does not remove them. The first version did, and a pass that then
// returned early — or died — dropped the event for good, which for the
// receipt update is the last thing that could correct the badge. Removal
// belongs to the publish that succeeds.
func (s *Service) dueShedEvents(now time.Time) []pendingUIEvent {
	var due []pendingUIEvent
	s.deliveryMu.RLock()
	for _, pending := range s.pendingUIEvents {
		if len(due) >= deliveryRepairBatch {
			break
		}
		if pending.after.After(now) {
			continue
		}
		due = append(due, pending)
	}
	s.deliveryMu.RUnlock()
	return due
}

// maxPendingUIEvents bounds the kept set. A client wedged past this many
// distinct messages is not one more event away from being correct, and the
// chatlog row is the durable authority a reload reads.
const maxPendingUIEvents = 512

// deferAnnouncement pushes a shed event's next attempt out, so the repair
// pass does not republish into an inbox that is still full.
func (s *Service) deferAnnouncement(id protocol.MessageID) {
	next := time.Now().UTC().Add(deliveryAnnounceRetry)
	s.deliveryMu.Lock()
	if entry, awaiting := s.awaitingDelivered[id]; awaiting {
		entry.AnnounceAfter = next
	}
	s.deliveryMu.Unlock()
}

// confirmEnvelopesDurably stamps the rows and, only if that lands, tells
// the sender.
//
// ONE journal write for the whole batch. The reconnect backlog replays a
// whole conversation at once, and confirming per message took a goroutine
// and a separate UPDATE for each of them, all contending for the same
// database as the pre-wire withdrawals of freshly-typed messages.
func (s *Service) confirmEnvelopesDurably(envelopes []protocol.Envelope, announce bool) {
	if len(envelopes) == 0 {
		return
	}
	ids := make([]protocol.MessageID, 0, len(envelopes))
	for _, envelope := range envelopes {
		ids = append(ids, envelope.ID)
	}
	if !s.markDeliveryOnWire(ids...) {
		return
	}
	s.noteStamped(ids)
	if announce {
		s.publishMessagesEmitted(envelopes)
	}
}

// noteStamped memoises a landed journal write, so the repair pass stops
// asking for it. The durable bit is monotone, so this flag only ever moves
// one way too.
func (s *Service) noteStamped(ids []protocol.MessageID) {
	s.deliveryMu.Lock()
	for _, id := range ids {
		if entry, awaiting := s.awaitingDelivered[id]; awaiting {
			entry.Stamped = true
		}
	}
	s.deliveryMu.Unlock()
}

// publishMessagesEmitted tells the local client that messages it was
// showing as queued have left the machine.
//
// A held message shows as "queued", and the moment it goes out that stops
// being true — but nothing else would ever say so, because the next thing
// to happen is the recipient's receipt, and a lost receipt would leave the
// sender believing a message was never sent while their counterpart is
// reading it.
//
// It is IDEMPOTENT and it is the only place Announced is set: several
// sinks can carry the same envelope on one dispatch, and each announces
// when it succeeds, so the flag is claimed under the delivery mutex and
// only the claimant publishes. An id whose entry is already gone is
// skipped — a receipt beat us to it, and the badge is past "sent" anyway.
//
// Callers must hold NO domain mutex: publishing runs subscriber callbacks
// that may come back into the Service.
func (s *Service) publishMessagesEmitted(envelopes []protocol.Envelope) {
	if len(envelopes) == 0 {
		return
	}
	claimed := make([]protocol.Envelope, 0, len(envelopes))
	s.deliveryMu.Lock()
	for _, envelope := range envelopes {
		entry, awaiting := s.awaitingDelivered[envelope.ID]
		if !awaiting || entry.Announced {
			continue
		}
		entry.Announced = true
		claimed = append(claimed, envelope)
	}
	s.deliveryMu.Unlock()

	for _, envelope := range claimed {
		log.Info().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).
			Msg("delivery_queued_message_emitted")
		_, dropped := s.eventBus.PublishReporting(ebus.TopicMessageEmitted, protocol.LocalChangeEvent{
			Type:      protocol.LocalChangeMessageEmitted,
			Topic:     envelope.Topic,
			MessageID: string(envelope.ID),
			Sender:    envelope.Sender,
			Recipient: envelope.Recipient,
			Status:    protocol.MessageStatusSent,
		})
		if dropped > 0 {
			// The bus sheds events when a subscriber's inbox is full, and
			// this is the one fact nothing else would ever restate: the
			// receipt is the next thing to happen, and a lost receipt
			// would leave the badge on "queued" for good. So the claim is
			// GIVEN BACK and the next confirmed emission says it again.
			// The cost of the other direction — announcing twice — is
			// nothing, because the subscriber applies it monotonically.
			s.returnEmissionAnnouncement(envelope.ID)
			s.deferAnnouncement(envelope.ID)
		}
	}
}

// returnEmissionAnnouncement un-claims an announcement the bus could not
// deliver, so a later emission of the same message re-announces it.
func (s *Service) returnEmissionAnnouncement(id protocol.MessageID) {
	log.Warn().Str("message_id", string(id)).
		Msg("delivery_queued_message_emitted_event_dropped_will_retry")
	s.deliveryMu.Lock()
	if entry, awaiting := s.awaitingDelivered[id]; awaiting {
		entry.Announced = false
	}
	s.deliveryMu.Unlock()
}

// runRetryDispatchBarrier fires the test-only seam between the retry
// tick's planning and its dispatch. Nil in production.
func (s *Service) runRetryDispatchBarrier() {
	if s.retryDispatchBarrier != nil {
		s.retryDispatchBarrier()
	}
}

// holdDeliveryRetry records why the entry did not go out, so a later
// reachability kick can wake it instead of leaving it to wait out the full
// backoff. The entry may already be gone — a receipt that landed in the
// gap — in which case there is nothing to hold.
//
// The schedule moves to a hold poll past `from`, EXCEPT for a message a
// wipe has frozen: that is this node pausing itself, and a thawed message
// must go out when it was due rather than pay a poll interval for a
// decision its recipient had no part in. The freeze caller passes the
// schedule the pass displaced, so the restore is exact.
func (s *Service) holdDeliveryRetry(id protocol.MessageID, from time.Time, reason deliveryHoldReason) {
	s.deliveryMu.Lock()
	if entry, awaiting := s.awaitingDelivered[id]; awaiting {
		entry.Hold = reason
		if s.deliveryFrozenLocked(id) {
			entry.NextAttemptAt = from
		} else {
			entry.NextAttemptAt = from.Add(deliveryHoldPollInterval)
		}
	}
	s.deliveryMu.Unlock()
}

// promoteQueueHeadLocked pulls the recipient's next queued message forward
// so it leaves as soon as the tick comes round, instead of idling out a
// backoff that was measuring the message just confirmed. Called when a
// delivered/seen receipt frees the recipient's queue slot. Caller MUST hold
// s.deliveryMu.Lock.
// It promotes the oldest message that has NOT been on the wire, which is
// not the same as the oldest message left. An already-emitted one is not
// waiting on the freed slot: its schedule measures its OWN dispatch, and
// pickQueueHeadLocked lets it step aside for the tail precisely so a lost
// receipt cannot stall the conversation. Promoting it anyway re-sent it
// early — a duplicate the recipient already had — and, because it then
// owned the slot again for a queue window, delayed the message that had
// never gone out at all. The rule promotion must not break is the same one
// the pick implements: a message that has never been emitted is never
// overtaken by a newer one.
func (s *Service) promoteQueueHeadLocked(recipient string, now time.Time) {
	var head *deliveryRetryEntry
	for id, entry := range s.awaitingDelivered {
		if entry.Envelope.Recipient != recipient || !entry.LastEmittedAt.IsZero() {
			continue
		}
		if s.deliveryFrozenLocked(id) {
			// A wipe is deciding about this one; its schedule is not
			// ours to move while that lasts.
			continue
		}
		if head == nil || deliveryQueueOrder(entry, head) {
			head = entry
		}
	}
	if head == nil || head.NextAttemptAt.Before(now) {
		return
	}
	head.NextAttemptAt = now
}

// noteRecipientWentOffline puts this recipient's unconfirmed deliveries back
// into holdUnreachable, so the kick that fires when they return re-arms them
// and resets their backoff.
//
// Without it a message confirmed onto the wire sits at holdNone waiting for
// a receipt, and the kick filters holdNone out — deliberately, so an
// unrelated route refresh cannot pull an in-flight message forward. But a
// peer that actually went away and came back is not a route refresh: their
// receipt is not coming, and the message can sit out the rest of an
// eleven-minute backoff while they are sitting there online. Observing the
// departure is what tells the two apart, and this is where it is observed.
//
// It reopens holdUnconfirmed too, and that is the second half of the same
// argument. An unconfirmed entry was dispatched while the recipient LOOKED
// reachable and no writer took the frame; it is parked on the poll
// interval, and the kick — which now only moves holdUnreachable, so a route
// merely being re-confirmed cannot overrule the pacing — would leave it
// there. But the peer actually leaving and coming back is the one event
// that says the earlier answer is stale, so it belongs in the state the
// kick acts on. Without this the returning recipient waited out the rest of
// a minute for a message that had been ready the whole time.
//
// A message the recipient already confirmed is gone from the set, so this
// only ever touches deliveries still owed. Caller must hold no domain mutex.
//
// The presence check is re-made HERE, and peerMu is held across the park.
// The close path counts the session down under peerMu and calls this after
// releasing it, so between those two the peer can already have come back:
// their registration completes, their online kick runs, finds the entry in
// holdNone or holdUnconfirmed and correctly leaves it alone — and then this
// stale departure parks it on holdUnreachable with the old backoff intact.
// Nothing fires a second kick, so a message ready to go waits out up to
// eleven minutes with the recipient sitting there online: the original
// symptom of this whole task, re-entered through the back door.
//
// The presence check does NOT hold peerMu across the park, and must not.
// The canonical order permits it, but this runs on the session-close path
// while other goroutines hold deliveryMu and reach for peerMu: with a
// peerMu writer queued behind us, Go's RWMutex stops handing out read
// locks, and the three of us wait on each other. It showed up as an
// integration test timing out rather than as anything reading like a
// deadlock, which is the usual way this class arrives.
//
// Not holding it costs nothing, because the check is an optimisation and
// not the guarantee. A recipient is also reachable through a TRANSIT
// ROUTE, which no session count shows, so the answer can be stale whatever
// we hold. The park is made HARMLESS instead of made atomic: reopening
// also pulls the entry DUE. If the departure was real the next tick finds them
// still unreachable and parks it on the poll interval, which costs
// nothing — no attempt is charged for a message that is held. If the
// departure was already stale, the tick sends within two seconds instead
// of after the rest of an eleven-minute backoff. The backoff being
// meaningless is the whole claim this function makes: it was measuring a
// peer who is no longer there.
func (s *Service) noteRecipientWentOffline(identity domain.PeerIdentity) {
	if identity.IsZero() {
		return
	}
	wire := identity.String()
	now := time.Now().UTC()
	var reopened int
	s.peerMu.RLock()
	present := s.identitySessions[identity] > 0
	s.peerMu.RUnlock()
	if present {
		log.Debug().Str("recipient", wire).
			Msg("stale offline ignored: the recipient is connected again")
		return
	}
	s.deliveryMu.Lock()
	for id, entry := range s.awaitingDelivered {
		if entry.Hold == holdUnreachable || entry.Envelope.Recipient != wire {
			continue
		}
		if s.deliveryFrozenLocked(id) {
			// A wipe is deciding about this one; its hold is not ours to
			// move while that lasts.
			continue
		}
		entry.Hold = holdUnreachable
		if entry.NextAttemptAt.After(now) {
			entry.NextAttemptAt = now
		}
		reopened++
	}
	s.deliveryMu.Unlock()
	if reopened > 0 {
		log.Debug().Str("recipient", wire).Int("deliveries", reopened).
			Msg("delivery_reopened_on_recipient_offline")
	}
}

// kickDeliveryRetriesForReachable re-arms held sender-owned delivery retries
// whose recipient just became reachable — a route appeared (announce drain) or
// the peer connected (session-established drain). Held messages (dispatchEnvelopeRetry
// returned early because the recipient was unreachable) sit in awaitingDelivered
// with NextAttemptAt on the exponential backoff schedule; pulling NextAttemptAt
// forward to now lets the next 2s retry tick deliver immediately instead of
// waiting out the backoff, and resetBackoffOnReturn puts the schedule back on
// its fast end — a message that has been waiting since yesterday must not make
// the person who just came online wait another eleven minutes for it. Takes
// deliveryMu alone; callers must hold no other domain mutex.
func (s *Service) kickDeliveryRetriesForReachable(identities map[domain.PeerIdentity]struct{}) {
	// Only meaningful when sends are held on reachability; with the flag off
	// nothing is held, so leaving the retry schedule untouched keeps flag-off
	// behaviour byte-for-byte identical to the legacy baseline.
	if !s.cfg.HoldDMUntilReachable || len(identities) == 0 {
		return
	}

	// Phase 1 (deliveryMu): collect held entries whose recipient is in the
	// set. No mutation yet — reachability is decided lock-free below.
	type heldEnvelope struct {
		id  protocol.MessageID
		env protocol.Envelope
	}
	var held []heldEnvelope
	s.deliveryMu.Lock()
	// Stamped BEFORE the scan and regardless of what it finds, because the
	// case this exists for is the one where it finds NOTHING: a tick that
	// sampled reachability a moment ago is about to mark its entries held
	// from an answer this kick has just made stale, and those entries are
	// not held yet, so they are invisible here. armDueDeliveries reads the
	// stamp and keeps such an entry due instead of parking it for a poll
	// interval. See docs/locking.md.
	s.lastReachabilityKickAt = time.Now().UTC()
	for id, entry := range s.awaitingDelivered {
		// Only entries whose last send decision was "they are not there" are
		// eligible. Both other states have already been handed to the sinks
		// on the current reachability, and pulling them forward would let a
		// routing event overrule the pacing rather than the reachability:
		//
		//   - holdNone is confirmed and waiting for a receipt on its backoff;
		//   - holdUnconfirmed was dispatched and nothing took the frame, so
		//     it is parked on the poll interval — and a route that keeps
		//     being RE-CONFIRMED (routing.RouteUnchanged, which announce
		//     ingest and route-query answers both feed into this kick) would
		//     otherwise reset that park on every announcement. On a route
		//     that resolves but is dead in practice that is a re-send, and a
		//     journal write, at announcement frequency.
		//
		// This is the state the kick is named after: it re-arms a message
		// whose recipient was ABSENT, the moment they are not.
		if entry.Hold != holdUnreachable {
			continue
		}
		if _, ok := identities[domain.PeerIdentityFromWire(entry.Envelope.Recipient)]; ok {
			held = append(held, heldEnvelope{id, entry.Envelope})
		}
	}
	s.deliveryMu.Unlock()
	if len(held) == 0 {
		return
	}

	// Phase 2 (no deliveryMu — router.Route reads routing/peer state under
	// its own locks): SELF-CHECK reachability per candidate. The kick is
	// precise regardless of the call site — a caller that reports an
	// identity which is not actually a usable delivery target (e.g. a
	// non-relay-capable peer connect, or a route that did not resolve to a
	// next hop) produces no re-arm, so no scheduled retry wastes an attempt
	// holding again.
	reachable := make(map[protocol.MessageID]bool, len(held))
	for _, h := range held {
		d := s.router.Route(h.env)
		reachable[h.id] = d.RelayNextHop != nil || len(d.PushSubscribers) > 0
	}

	// Phase 3 (deliveryMu): re-arm only the genuinely reachable held
	// entries that still exist AND are still held on reachability.
	// Nothing is spent here — an emission is charged by emitDueDelivery,
	// after the wire has taken the frame.
	//
	// The hold is re-read rather than remembered from phase 1, because
	// phase 2 runs with the lock RELEASED: a tick can dispatch the entry
	// meanwhile (holdUnconfirmed), or a sink can confirm it (holdNone),
	// and re-arming either of those is exactly what the selection above
	// exists to prevent — the same route re-confirmation that must not
	// overrule pacing would do it through this window instead.
	now := time.Now().UTC()
	s.deliveryMu.Lock()
	for _, h := range held {
		if !reachable[h.id] {
			continue
		}
		entry, ok := s.awaitingDelivered[h.id]
		if !ok || entry.Hold != holdUnreachable {
			continue
		}
		s.resetBackoffOnReturn(entry, h.id)
		if entry.NextAttemptAt.After(now) {
			entry.NextAttemptAt = now
		}
	}
	s.deliveryMu.Unlock()
}

// peerSupportsProtocol reports whether the peer behind the address
// advertises a negotiated wire protocol version >= min. Checks the outbound
// session first, then the inbound conn registry. Unknown peers (no live
// session/conn) report false — for additive features the caller's retry
// re-attempts once the peer is back with a known version.
func (s *Service) peerSupportsProtocol(address domain.PeerAddress, min int) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if session := s.resolveSessionLocked(address); session != nil && session.version >= min {
		return true
	}
	for _, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		if entry.core.Address() == address && int(entry.core.ProtocolVersion()) >= min {
			return true
		}
	}
	return false
}

// connSupportsProtocol reports whether the connection advertises a
// negotiated wire protocol version >= min.
func (s *Service) connSupportsProtocol(connID domain.ConnID, min int) bool {
	core := s.netCoreForID(connID)
	return core != nil && int(core.ProtocolVersion()) >= min
}

// failDelivery finalises a locally-sent DM that outlived its own TTL —
// the only way the engine gives up on one: outbound goes terminal — the
// same "expired"/"failed" statuses the pending-ring paths use, visible through
// fetch_pending_messages — the pending rings drop every queued frame of the
// message (the send_message AND any relay_message queued by the relay
// fallback), relayRetry drops its entry, the aggregate pending count
// refreshes, and the durable failure journal (written synchronously — it is
// the durable boundary of the abandonment) keeps RegisterDeliveryOutbox
// from reseeding the same chatlog row after a restart. A late-receipt
// re-check under the locks makes the abandon decision lose against a
// delivered/seen receipt that landed in the unlocked gap since the retry
// tick. The chatlog row itself intentionally stays at "sent": sent→failed
// is not a chatlog lifecycle transition, and "sent without delivered" is
// the truthful terminal state the UI shows — only the automatic retries
// stop.
//
// Canonical order peerMu → deliveryMu → statusMu (refreshAggregatePendingLocked
// reads peer-domain health and writes status-domain state); side effects and
// the synchronous journal write run after every mutex is released.
func (s *Service) failDelivery(envelope protocol.Envelope, status, reason string) {
	frame := protocol.Frame{Type: "send_message", Topic: envelope.Topic, ID: string(envelope.ID), Recipient: envelope.Recipient}

	log.Trace().Str("site", "failDelivery").Str("phase", "lock_wait").Str("msg_id", string(envelope.ID)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "failDelivery").Str("phase", "lock_held").Str("msg_id", string(envelope.ID)).Msg("peer_mu_writer")
	log.Trace().Str("site", "failDelivery").Str("phase", "lock_wait").Str("msg_id", string(envelope.ID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "failDelivery").Str("phase", "lock_held").Str("msg_id", string(envelope.ID)).Msg("delivery_mu_writer")
	// Late-receipt re-check: the abandon decision was taken under a
	// previous deliveryMu window; a delivered/seen receipt may have landed
	// in the unlocked gap (clearing awaitingDelivered and updating chatlog).
	// Terminalizing or journaling on top of that would overwrite a
	// confirmed delivery with failed/expired — skip entirely.
	if s.hasReceiptForMessageLocked(envelope.Sender, envelope.ID) {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "failDelivery").Str("phase", "lock_released_receipt_won").Str("msg_id", string(envelope.ID)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "failDelivery").Str("phase", "lock_released_receipt_won").Str("msg_id", string(envelope.ID)).Msg("peer_mu_writer")
		log.Debug().Str("message_id", string(envelope.ID)).Msg("delivery_retry_abandon_skipped: receipt arrived first")
		return
	}
	s.markOutboundTerminalLocked(frame, status, reason)
	pendingDeltas := s.clearPendingMessageLocked(envelope.ID)
	delete(s.relayRetry, relayMessageKey(envelope.ID))
	s.statusMu.Lock()
	if len(pendingDeltas) > 0 {
		s.refreshAggregatePendingLocked()
	}
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "failDelivery").Str("phase", "lock_released").Str("msg_id", string(envelope.ID)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "failDelivery").Str("phase", "lock_released").Str("msg_id", string(envelope.ID)).Msg("peer_mu_writer")

	for _, d := range pendingDeltas {
		s.emitPeerPendingChanged(d.Address, d.Count)
	}
	if len(pendingDeltas) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	}

	// The journal write is the durable boundary of the abandonment ("do not
	// reseed after restart"), so it runs SYNCHRONOUSLY here — every domain
	// mutex is already released, and failDelivery executes on the
	// bootstrapLoop tick goroutine, not on a hot wire path. A background
	// hop would race production shutdown: Run does not wait for
	// backgroundWg, and the desktop/SDK runtime closes the chatlog right
	// after Run returns, silently dropping the write and resurrecting the
	// abandoned retry on the next start.
	if s.deliveryFailureJournal != nil {
		if err := s.deliveryFailureJournal.MarkDeliveryFailed(envelope.ID); err != nil {
			log.Warn().Str("message_id", string(envelope.ID)).Err(err).Msg("delivery_failure_journal_write_failed")
		}
	}
}

// sendSeenAck answers a received "seen" receipt with the end-to-end
// seen_ack confirmation (ReceiptStatusSeenAck) so the seen-sender's retry
// loop stops. No local retry state is kept for the ack itself: every
// (re)arrival of the seen receipt re-triggers it, mirroring the
// duplicate-DM → delivered re-send contract.
func (s *Service) sendSeenAck(seen protocol.DeliveryReceipt) {
	defer crashlog.DeferRecover()
	ack := protocol.DeliveryReceipt{
		MessageID:   seen.MessageID,
		Sender:      s.identity.Address,
		Recipient:   seen.Sender,
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	}
	log.Info().Str("message_id", string(ack.MessageID)).Str("recipient", ack.Recipient).Msg("seen_ack_send")
	s.distributeReceipt(ack)
}

// dispatchEnvelopeRetry re-sends one locally-sent envelope, mirroring the
// primary send paths of storeIncomingMessage: live subscriber push, then
// table-directed relay when a route exists. Whether it ALSO blind-gossips
// when no route is known depends on the reachability gate
// (CORSA_HOLD_DM_UNTIL_REACHABLE): default-ON HOLDS such a message (returns
// false, no blind gossip); the kill-switch OFF restores the legacy blind
// gossip. The re-emission reuses the stored hop budget (a retry is the same
// hop, not a new one) and is deduped by receivers via the duplicate paths
// that also re-send the delivered receipt.
// dispatchEnvelopeRetry returns whether the envelope was ATTEMPTED — not
// whether it went out. false means the reachability gate refused to try at
// all; true means the sinks were called, and each of them confirms for
// itself through confirmEnvelopeOnWire if it accepted the frame. Until one
// does, the entry stays holdUnconfirmed.
//
// dispatchedAt identifies this attempt to the confirmations, which arrive
// on their own schedules.
func (s *Service) dispatchEnvelopeRetry(envelope protocol.Envelope, dispatchedAt time.Time) (attempted bool) {
	decision := s.router.Route(envelope)

	// Sender-owned delivery emits ONLY when the recipient is reachable: a
	// directed route exists (RelayNextHop) or the recipient is a directly
	// connected subscriber (PushSubscribers). An unreachable recipient is
	// HELD — no blind gossip into the void. This is the root cure for the
	// churn/storm: an offline or long-gone recipient no longer triggers a
	// blind-gossip fan-out on every retry tick. Delivery resumes when a
	// route or connection appears — kickDeliveryRetriesForReachable (fired
	// from the announce/connect drain) re-arms held entries immediately;
	// until then the message waits in awaitingDelivered, bounded by its own
	// TTL and nothing else. See docs/protocol/relay.md INV-3.
	if s.cfg.HoldDMUntilReachable && decision.RelayNextHop == nil && len(decision.PushSubscribers) == 0 {
		log.Debug().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).Msg("delivery_retry_held_unreachable")
		return false
	}

	// gossipTargetsForRelay drops the table next-hop from the fan-out (it gets
	// the directed relay_message below) before the gates/K-of-N — so the
	// sender-owned retry tick does not duplicate push_message+relay_message to
	// the same peer.
	gossipTargets := s.gossipTargetsForRelay(envelope, decision)

	// Every sink below confirms for itself, and none of them is trusted to
	// have worked just because it was called.
	if len(decision.PushSubscribers) > 0 {
		s.goBackground(func() { s.pushOwnEnvelopeToSubscribers(envelope, decision.PushSubscribers, dispatchedAt) })
	}

	// Gossip carries this dispatch too. It is not the sink the queued → sent
	// transition waits on — it hands jobs to a bounded pool that sheds under
	// load — but it is a real path to the wire, and a path to the wire that
	// skips the pre-wire gate is how a message the author has just recalled
	// still reaches the recipient, with the deletion recording it as never
	// emitted and scheduling nothing.
	s.executeGossipTargets(envelope, gossipTargets, deliveryDispatchRef{Envelope: envelope, DispatchedAt: dispatchedAt})

	relayed := relaySendRefused
	if decision.RelayNextHop != nil {
		relayed = s.sendTableDirectedRelay(s.runCtx, envelope, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops, dispatchedAt)
	} else {
		relayed = s.tryRelayToCapableFullNodes(envelope, gossipTargets, dispatchedAt)
	}
	switch {
	case relayed.leftTheNode():
		s.confirmEnvelopeOnWire(envelope, dispatchedAt)
	case len(decision.PushSubscribers) > 0:
		// The push answers for this dispatch later.
	default:
		// Nobody took it. NOTHING is written down, and that is the whole
		// point of the two-bit model: the row already reads as not-on-wire
		// (no on_wire stamp) and as may-have-been-handed-to-a-writer (its
		// never-emitted claim came off before the attempt). Both answers
		// are already correct for their reader, so a refusal has nothing
		// to record — and cannot get it wrong.
		log.Debug().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).
			Str("outcome", "unconfirmed").Msg("delivery_dispatch_unconfirmed")
	}
	return true
}

// pushOwnEnvelopeToSubscribers is the retry tick's push, wrapped so the
// outcome is not thrown away. It runs on a background goroutine: the tick
// must not block on a subscriber's writer.
//
// A push that no subscriber accepted confirms nothing, and confirming
// nothing is the whole mechanism — the entry stays holdUnconfirmed, so a
// reachability kick can wake it and the next pass tries again.
//
// "Nobody accepted" and "nobody can say" are different answers, though, and
// only the first is worth writing down. A subscriber whose writer refused
// ambiguously may be holding the message, and recording "never emitted" for
// it would tell a later deletion to skip them.
func (s *Service) pushOwnEnvelopeToSubscribers(envelope protocol.Envelope, subs []*subscriber, dispatchedAt time.Time) {
	accepted, unresolved := s.pushToSubscriberSnapshot(envelope, subs)
	if accepted > 0 {
		s.confirmEnvelopeOnWire(envelope, dispatchedAt)
		return
	}
	log.Warn().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).
		Int("unresolved", unresolved).Msg("delivery_push_refused_by_every_subscriber")
}

// clearedToWrite is the PRE-WIRE gate every path that hands a frame to a
// writer has to pass, and the one the confirmation cannot stand in for.
//
// It answers two questions that are only answerable BEFORE the write. Has
// the message been frozen by a wipe or withdrawn since the frame was
// queued? A frame sitting in a session's queue can outlive the moment its
// author recalls it, and the recall classifies it as never-emitted — so
// writing it afterwards puts a message the sender was told was recalled
// in front of the recipient. And has the durable never-emitted claim been
// withdrawn? That write has to land first, or the disk says the message
// never left while the peer holds it.
//
// A frame that carries no delivery of ours passes untouched.
func (s *Service) clearedToWrite(ref deliveryDispatchRef, now time.Time) bool {
	if ref.Envelope.ID == "" {
		return true
	}
	return s.noteOwnEnvelopeEmitted(ref.Envelope.Sender, ref.Envelope.ID, now)
}
