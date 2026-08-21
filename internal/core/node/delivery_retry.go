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
}

// OutboxEntry is one reseeded row: the envelope, plus the durable answer
// to the only thing about its past the scheduler cannot re-derive.
type OutboxEntry struct {
	Envelope protocol.Envelope
	// Emitted carries deliveryRetryEntry.Emitted across the restart. The
	// outbox reports FALSE only when it can prove the envelope never
	// reached the wire; anything it does not know is true, because the
	// claim a deletion makes with false is "the peer cannot have this",
	// and an unprovable one there leaves a delivered message with them.
	Emitted bool
}

// DeliveryEmissionJournal is the optional durable record behind
// deliveryRetryEntry.Emitted. Implemented by the desktop chatlog adapter;
// without it the flag is memory-only and every restart answers "emitted".
//
// The two calls are not symmetric in urgency. A lost mark costs the peer
// one control DM naming an id they cannot resolve; a stale mark costs the
// user a deletion that is never asked for. So ClearNeverEmitted has to be
// durable BEFORE the frame goes out, while MarkNeverEmitted may land after
// the hold it describes.
type DeliveryEmissionJournal interface {
	// MarkNeverEmitted records that the messages have not reached the wire.
	MarkNeverEmitted(ids []protocol.MessageID) error
	// ClearNeverEmitted withdraws that claim.
	ClearNeverEmitted(ids []protocol.MessageID) error
}

// DeliveryFailureJournal is the optional durable journal for messages the
// retry engine gave up on (TTL expiry / attempts cap). A journaled id is
// excluded from UndeliveredOutgoing, so RegisterDeliveryOutbox does not
// reseed it after a restart — the attempts cap stays durable. Implemented
// by the desktop chatlog adapter.
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

// defaultDeliveryRetryMaxAttempts bounds how long a sender keeps retrying a
// single message/receipt: 20 attempts ≈ 3.5 hours on the capped schedule.
// Overridable via config.Node.DeliveryRetryMaxAttempts
// (CORSA_DELIVERY_RETRY_MAX_ATTEMPTS).
const defaultDeliveryRetryMaxAttempts = 20

// deliveryRetryBackoff returns the wait before the attempt with the given
// zero-based number.
func deliveryRetryBackoff(attempt int) time.Duration {
	if attempt >= len(deliveryRetrySchedule) {
		return deliveryRetrySchedule[len(deliveryRetrySchedule)-1]
	}
	return deliveryRetrySchedule[attempt]
}

// deliveryRetryEntry tracks one locally-sent DM awaiting its delivered/seen
// receipt. Owned by s.deliveryMu.
type deliveryRetryEntry struct {
	Envelope      protocol.Envelope
	Attempts      int
	NextAttemptAt time.Time
	// Held is true when the last send decision did NOT emit because the
	// recipient was unreachable (reachability gate, CORSA_HOLD_DM_UNTIL_REACHABLE).
	// Only Held entries are eligible for kickDeliveryRetriesForReachable —
	// a message that was already emitted and is merely awaiting its receipt
	// on the exponential schedule must NOT be pulled forward by an unrelated
	// route refresh (that would burn attempts and produce early duplicates).
	Held bool
	// Emitted is monotone: it turns true the first time an attempt actually
	// puts the envelope on the wire and never goes back. Held cannot answer
	// "did the recipient ever have a chance to see this?" — it flips back to
	// true whenever a later tick finds the peer unreachable, long after the
	// message went out.
	//
	// A cancellation reads it to tell "this never left the node" (the peer
	// has nothing, so asking them to delete it would announce a message
	// they never received) from "we cannot know" (the default, which keeps
	// the peer-side deletion scheduled).
	Emitted bool
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
func (s *Service) noteOwnEnvelopeEmitted(sender string, messageID protocol.MessageID) bool {
	if s.identity == nil || sender != s.identity.Address {
		return true
	}
	withheld := s.noteOwnEnvelopesEmitted([]protocol.MessageID{messageID})
	_, blocked := withheld[messageID]
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
// Returns the ids the caller must NOT write: withdrawn while this ran, or
// still carrying a durable never-emitted claim the journal refused to
// withdraw.
func (s *Service) noteOwnEnvelopesEmitted(ids []protocol.MessageID) map[protocol.MessageID]struct{} {
	if len(ids) == 0 {
		return nil
	}
	blocked, unproven := s.claimEmissionLocked(ids)
	if len(unproven) == 0 {
		return blocked
	}
	// Outside the delivery mutex (SQLite) and BEFORE the caller writes a
	// frame: an id whose claim is still on disk has not been cleared to go
	// out. Ids the clear could not reach join the withheld set — the caller
	// drops them, their entry keeps its schedule, and the next retry tick
	// tries the whole thing again.
	stranded := s.clearEmissionMarks(unproven)
	return s.confirmEmission(unproven, stranded, blocked)
}

// claimEmissionLocked is the first half of noteOwnEnvelopesEmitted: it
// takes the delivery domain once, reports the ids a withdrawal has
// shadowed, and separates the ids that still carry a durable never-emitted
// claim from the ones already accounted for.
//
// The ordinary send has nothing unproven — the entry was born emitted — so
// it takes this one lock hold and no disk write at all, which is what the
// two-phase form exists to preserve.
func (s *Service) claimEmissionLocked(ids []protocol.MessageID) (map[protocol.MessageID]struct{}, []protocol.MessageID) {
	var blocked map[protocol.MessageID]struct{}
	var unproven []protocol.MessageID
	s.deliveryMu.Lock()
	for _, id := range ids {
		if _, withdrawn := s.cancelledDeliveries[id]; withdrawn {
			blocked = withhold(blocked, id)
			continue
		}
		if s.deliveryFrozenLocked(id) {
			// A wipe is deciding whether the peer may ever hold this
			// message. Emitting now would answer the question behind its
			// back — and it is deciding from a row it is about to delete.
			blocked = withhold(blocked, id)
			continue
		}
		entry, awaiting := s.awaitingDelivered[id]
		if !awaiting {
			// No entry does NOT mean nothing to withdraw. The backlog
			// replay reaches past the retry engine entirely, so it can
			// emit a message whose entry was dropped when its attempts
			// ran out — while the durable claim that it never went out
			// still stands on the row. s.markedNeverEmitted remembers
			// exactly those ids, so the ordinary case still costs no
			// lookup and no write.
			if _, marked := s.markedNeverEmitted[id]; marked {
				unproven = append(unproven, id)
			}
			continue
		}
		if entry.Emitted || s.emissionJournal == nil {
			// Nothing on disk to withdraw: either this message has gone
			// out before, or the flag has no durable half on this node.
			entry.Emitted = true
			entry.Held = false
			continue
		}
		unproven = append(unproven, id)
	}
	s.deliveryMu.Unlock()
	return blocked, unproven
}

// confirmEmission is the second half: the ids whose claim is now off the
// disk become emitted, and the ones that failed join the withheld set.
//
// The withdrawal shadow is re-read here, not carried over from the first
// hold: the durable write happened between the two, and a cancellation
// landing in that gap has already told the user the message was recalled.
func (s *Service) confirmEmission(unproven []protocol.MessageID, stranded, blocked map[protocol.MessageID]struct{}) map[protocol.MessageID]struct{} {
	s.deliveryMu.Lock()
	for _, id := range unproven {
		if _, failed := stranded[id]; failed {
			blocked = withhold(blocked, id)
			continue
		}
		if _, withdrawn := s.cancelledDeliveries[id]; withdrawn {
			blocked = withhold(blocked, id)
			continue
		}
		if s.deliveryFrozenLocked(id) {
			blocked = withhold(blocked, id)
			continue
		}
		if entry, awaiting := s.awaitingDelivered[id]; awaiting {
			entry.Emitted = true
			entry.Held = false
		}
	}
	s.deliveryMu.Unlock()
	return blocked
}

func withhold(set map[protocol.MessageID]struct{}, id protocol.MessageID) map[protocol.MessageID]struct{} {
	if set == nil {
		set = make(map[protocol.MessageID]struct{}, 1)
	}
	set[id] = struct{}{}
	return set
}

// syncEmissionMarks records the durable claim for whichever of the ids the
// delivery domain still says has never reached the wire. It writes in ONE
// direction only — the claim is never withdrawn here, that belongs to
// clearEmissionMarks, which reports its failures instead of swallowing
// them.
//
// The ordering that keeps a mark from landing on top of a clear is the
// re-read: s.emissionMu is held across decide-and-write, and the decision
// reads the delivery domain, so an id another goroutine has just emitted
// is seen as emitted here and skipped rather than re-marked.
//
// s.emissionMu is NOT one of the seven domain mutexes: it is taken OUTSIDE
// all of them, never while one is held, and is the only lock held across
// the journal's disk I/O. See docs/locking.md.
//
// An id with no entry left counts as emitted: the entry is deleted when
// the receipt arrives, and a row that has been delivered is never reseeded.
func (s *Service) syncEmissionMarks(ids []protocol.MessageID) {
	if len(ids) == 0 || s.emissionJournal == nil {
		return
	}
	s.emissionMu.Lock()
	defer s.emissionMu.Unlock()

	never := make([]protocol.MessageID, 0, len(ids))
	s.deliveryMu.RLock()
	for _, id := range ids {
		if entry, awaiting := s.awaitingDelivered[id]; awaiting && !entry.Emitted {
			never = append(never, id)
		}
	}
	s.deliveryMu.RUnlock()
	if len(never) == 0 {
		return
	}
	if err := s.emissionJournal.MarkNeverEmitted(never); err != nil {
		// The harmless direction: without the mark the message reads as
		// possibly-sent, so a later deletion asks the peer rather than
		// skipping them.
		log.Warn().Err(err).Int("count", len(never)).
			Msg("emission_journal_mark_failed")
		return
	}
	s.rememberMarkedNeverEmitted(never)
}

// rememberMarkedNeverEmitted / forgetMarkedNeverEmitted track which ids
// this process has a durable claim standing for, so an emission can
// withdraw it even after the retry entry is gone. Memory-only and bounded
// by the number of withheld messages: after a restart the reseed rebuilds
// the entries themselves from the same marks.
func (s *Service) rememberMarkedNeverEmitted(ids []protocol.MessageID) {
	s.deliveryMu.Lock()
	if s.markedNeverEmitted == nil {
		s.markedNeverEmitted = make(map[protocol.MessageID]struct{}, len(ids))
	}
	for _, id := range ids {
		s.markedNeverEmitted[id] = struct{}{}
	}
	s.deliveryMu.Unlock()
}

func (s *Service) forgetMarkedNeverEmitted(ids []protocol.MessageID) {
	s.deliveryMu.Lock()
	for _, id := range ids {
		delete(s.markedNeverEmitted, id)
	}
	s.deliveryMu.Unlock()
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
	s.emissionMu.Lock()
	defer s.emissionMu.Unlock()

	if err := s.emissionJournal.ClearNeverEmitted(ids); err == nil {
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
	s.awaitingDelivered[envelope.ID] = &deliveryRetryEntry{
		Envelope:      envelope,
		NextAttemptAt: now.Add(deliveryRetryBackoff(0)),
		Held:          held,
		// A send that was not withheld went out on the caller's own
		// gossip/relay pass. Anything else counts as emitted too: the
		// conservative answer to "might the peer have it?" is yes, and
		// only an explicit hold proves otherwise. A held entry can still
		// be marked later by noteOwnEnvelopeEmitted — the backlog replay
		// reaches past this scheduler entirely. The startup reseed, which
		// has a durable answer, overwrites this one right after the call.
		Emitted: !held,
	}
}

// registerAwaitingSeenAckLocked schedules the locally-sent seen receipt for
// retry until the original sender's seen_ack arrives. Caller MUST hold
// s.deliveryMu.Lock. Idempotent per MessageID.
func (s *Service) registerAwaitingSeenAckLocked(receipt protocol.DeliveryReceipt, now time.Time) {
	if _, exists := s.awaitingSeenAck[receipt.MessageID]; exists {
		return
	}
	s.awaitingSeenAck[receipt.MessageID] = &seenAckRetryEntry{
		Receipt:       receipt,
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

	// The HEAVY startup scan/reseed (UndeliveredOutgoing + UnconfirmedSeen) is
	// skipped for DM opt-out (relay-only) node mode: it holds no reseedable DM
	// backlog, so spinning up the wake/reseed machinery is pure waste. Reached
	// via the common DesktopClient/SDK composition, not only the standalone
	// corsa-node that never registers an outbox. Ongoing in-session sends, if
	// any, still populate awaitingDelivered directly via storeIncomingMessage
	// and terminalize through the durable journals registered above.
	if s.cfg.DisableDirectMessages {
		log.Debug().Msg("delivery_retry_outbox_scan_skipped_dm_optout")
		return
	}

	now := time.Now().UTC()

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
			// Emitted comes from the OUTBOX, not from held: held answers
			// "should the reachability kick wake this", which after a
			// restart is no, while Emitted answers "can the peer possibly
			// have it", which the outbox is the only surviving witness of.
			// Deriving one from the other is what made every restart
			// announce ids for messages that never left the machine.
			e.Emitted = row.Emitted
		}
		count := len(s.awaitingDelivered)
		s.deliveryMu.Unlock()
		log.Info().Int("reseeded", len(entries)).Int("awaiting_delivered", count).Msg("delivery_retry_outbox_reseeded")
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

// retryDueDeliveries re-sends every awaiting entry whose NextAttemptAt has
// passed. Called from bootstrapLoop on its 2s tick; the schedule inside the
// entries provides the real pacing. The due snapshot (and the schedule
// bump) happens under s.deliveryMu; the sends run after release.
func (s *Service) retryDueDeliveries(now time.Time) {
	maxAttempts := s.deliveryRetryMaxAttempts()

	// dueDispatch is one envelope the tick may put on the wire. Whether it
	// actually goes out is decided at the last boundary, not here: see the
	// claim in the dispatch loop — which is why the schedule this pass
	// SPENT travels with it, so an attempt the wire never saw can be
	// given back.
	type dueDispatch struct {
		env   protocol.Envelope
		spent deliveryAttemptCharge
	}
	var dueMessages []dueDispatch
	var dueReceipts []protocol.DeliveryReceipt
	type abandonedDelivery struct {
		envelope protocol.Envelope
		status   string
		reason   string
	}
	var abandoned []abandonedDelivery
	// dueCandidate is a message whose NextAttemptAt has passed and which is
	// neither expired nor over the cap. The attempt bump and the Held
	// decision are deferred to Phase 3 (after the lock-free reachability
	// check) so Held is written atomically with the schedule — no kick can
	// observe a half-updated entry.
	type dueCandidate struct {
		id  protocol.MessageID
		env protocol.Envelope
	}
	var candidates []dueCandidate

	log.Trace().Str("site", "retryDueDeliveries").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "retryDueDeliveries").Str("phase", "lock_held").Msg("delivery_mu_writer")
	for id, entry := range s.awaitingDelivered {
		if entry.NextAttemptAt.After(now) {
			continue
		}
		if s.deliveryFrozenLocked(id) {
			// Held by a wipe. Not even the attempt counter moves: the
			// freeze is a pause, and charging it would abandon the
			// delivery for a reason that has nothing to do with the peer.
			continue
		}
		// TTL bounds the delivery lifetime (docs/protocol/messaging.md) —
		// the retry engine honours it the same way the relay retry loop
		// does, instead of re-emitting an envelope receivers would reject
		// as expired.
		if s.messageDeliveryExpired(entry.Envelope.CreatedAt, entry.Envelope.TTLSeconds) {
			delete(s.awaitingDelivered, id)
			abandoned = append(abandoned, abandonedDelivery{entry.Envelope, "expired", "message delivery expired"})
			log.Warn().Str("message_id", string(id)).Str("recipient", entry.Envelope.Recipient).Msg("delivery_retry_expired_ttl")
			continue
		}
		if entry.Attempts >= maxAttempts {
			delete(s.awaitingDelivered, id)
			abandoned = append(abandoned, abandonedDelivery{entry.Envelope, "failed", "delivery retries exhausted"})
			log.Warn().Str("message_id", string(id)).Str("recipient", entry.Envelope.Recipient).Int("attempts", entry.Attempts).Msg("delivery_retry_exhausted")
			continue
		}
		// Defer the bump + Held decision to Phase 3 (after reachability).
		candidates = append(candidates, dueCandidate{id, entry.Envelope})
	}
	for id, entry := range s.awaitingSeenAck {
		if entry.NextAttemptAt.After(now) {
			continue
		}
		if entry.Attempts >= maxAttempts {
			delete(s.awaitingSeenAck, id)
			log.Warn().Str("message_id", string(id)).Str("recipient", entry.Receipt.Recipient).Int("attempts", entry.Attempts).Msg("seen_ack_retry_exhausted")
			continue
		}
		entry.Attempts++
		entry.NextAttemptAt = now.Add(deliveryRetryBackoff(entry.Attempts))
		dueReceipts = append(dueReceipts, entry.Receipt)
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "retryDueDeliveries").Str("phase", "lock_released").Msg("delivery_mu_writer")

	// Phase 2 (lock-free): reachability per candidate. router.Route reads
	// routing/peer state under its own locks, so it must NOT run under
	// deliveryMu (canonical peerMu → deliveryMu order). With the gate off
	// every candidate is "reachable" — the legacy unconditional send.
	reachable := make(map[protocol.MessageID]bool, len(candidates))
	for _, c := range candidates {
		if !s.cfg.HoldDMUntilReachable {
			reachable[c.id] = true
			continue
		}
		d := s.router.Route(c.env)
		reachable[c.id] = d.RelayNextHop != nil || len(d.PushSubscribers) > 0
	}

	// Phase 3 (deliveryMu): apply the attempt bump, reschedule, and set Held
	// ATOMICALLY with the schedule. Reachable → emit on this tick (collected
	// into dueMessages); unreachable → Held=true (re-armed later by
	// kickDeliveryRetriesForReachable the moment a route/connection appears).
	// Writing Held under the same lock that bumps the schedule closes the
	// false→true lost-wakeup window a post-dispatch write left open.
	if len(candidates) > 0 {
		s.deliveryMu.Lock()
		for _, c := range candidates {
			entry, ok := s.awaitingDelivered[c.id]
			if !ok {
				continue // delivered/removed in the unlocked gap
			}
			spent := deliveryAttemptCharge{Attempts: entry.Attempts, NextAttemptAt: entry.NextAttemptAt}
			entry.Attempts++
			entry.NextAttemptAt = now.Add(deliveryRetryBackoff(entry.Attempts))
			if reachable[c.id] {
				entry.Held = false
				dueMessages = append(dueMessages, dueDispatch{env: entry.Envelope, spent: spent})
			} else {
				entry.Held = true
			}
		}
		s.deliveryMu.Unlock()
	}

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
		envelope := due.env
		// The candidate was chosen three phases ago, and a deletion can
		// have frozen it since — the wipe classifies against the delivery
		// domain and then destroys the row, so an envelope that goes out
		// after that classification stays with the peer with nothing left
		// to recall it.
		//
		// So the claim is taken HERE, immediately before the wire, in the
		// same lock hold that reads the freeze and the withdrawal shadow.
		// It also withdraws the durable never-emitted claim, and refuses
		// the attempt when that write fails: sending anyway would leave
		// the disk saying the message never left while the peer holds it.
		//
		// Claiming BEFORE the attempt rather than after costs one thing,
		// deliberately: a dispatch that then fails to emit (its route
		// vanished in the microsecond gap) leaves the entry counted as
		// possibly-out, so a later deletion asks the peer about an id
		// they may not have. That is the harmless direction, and it is
		// the price of the freeze meaning anything at all.
		log.Info().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).Msg("delivery_retry_resend")
		if !s.noteOwnEnvelopeEmitted(envelope.Sender, envelope.ID) {
			// Frozen by a deletion, withdrawn, or its durable claim could
			// not be withdrawn. Nothing reached the wire, so the attempt
			// this tick charged is GIVEN BACK: a freeze is this node
			// pausing itself, and a message that comes back from a thaw
			// carrying a spent attempt and an 11-minute backoff can be
			// abandoned by the very next tick without the network having
			// been used once.
			//
			// A dispatch that runs and finds no route is the opposite
			// case and keeps its charge: an unreachable recipient is
			// exactly what the attempt budget measures, which is why
			// Phase 3 charges the unreachable candidates too.
			s.refundDeliveryAttempt(envelope.ID, due.spent)
			continue
		}
		if s.dispatchEnvelopeRetry(envelope) {
			continue
		}
		// The attempt did not go out after all. Held so a reachability
		// kick can wake it; Emitted stays true, per the trade above.
		s.holdDeliveryRetry(envelope.ID)
	}
	for _, receipt := range dueReceipts {
		log.Info().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("seen_receipt_retry_resend")
		s.distributeReceipt(receipt)
	}
}

// deliveryAttemptCharge is what one retry pass spent on an entry before
// the wire had its say.
type deliveryAttemptCharge struct {
	Attempts      int
	NextAttemptAt time.Time
}

// refundDeliveryAttempt gives back a charge whose frame never went out and
// holds the entry so a reachability kick can wake it.
//
// The refund is conditional on the entry still carrying exactly what this
// pass charged: anything else means another path has moved the schedule
// since, and restoring a stale snapshot over it would undo a decision this
// pass knows nothing about.
func (s *Service) refundDeliveryAttempt(id protocol.MessageID, spent deliveryAttemptCharge) {
	s.deliveryMu.Lock()
	if entry, awaiting := s.awaitingDelivered[id]; awaiting {
		if entry.Attempts == spent.Attempts+1 {
			entry.Attempts = spent.Attempts
			entry.NextAttemptAt = spent.NextAttemptAt
		}
		entry.Held = true
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

// holdDeliveryRetry marks the entry held so a later reachability kick can
// wake it, instead of leaving it Held=false (which the kick filters out) to
// wait out the full backoff. The entry may already be gone — a receipt that
// landed in the gap — in which case there is nothing to hold.
func (s *Service) holdDeliveryRetry(id protocol.MessageID) {
	s.deliveryMu.Lock()
	if entry, awaiting := s.awaitingDelivered[id]; awaiting {
		entry.Held = true
	}
	s.deliveryMu.Unlock()
}

// kickDeliveryRetriesForReachable re-arms held sender-owned delivery retries
// whose recipient just became reachable — a route appeared (announce drain) or
// the peer connected (session-established drain). Held messages (dispatchEnvelopeRetry
// returned early because the recipient was unreachable) sit in awaitingDelivered
// with NextAttemptAt on the exponential backoff schedule; pulling NextAttemptAt
// forward to now lets the next 2s retry tick deliver immediately instead of
// waiting out the backoff. Only advances the schedule — never touches the
// attempt counter — so it cannot, by itself, exhaust the cap. Takes deliveryMu
// alone; callers must hold no other domain mutex.
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
	for id, entry := range s.awaitingDelivered {
		// Only entries whose last send decision was a HOLD (unreachable) are
		// eligible. An already-emitted send awaiting its receipt must not be
		// pulled forward by an unrelated route refresh/reconfirm.
		if !entry.Held {
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

	// Phase 3 (deliveryMu): pull NextAttemptAt forward only for genuinely
	// reachable held entries that still exist. Only advances the schedule —
	// never spends an attempt.
	now := time.Now().UTC()
	s.deliveryMu.Lock()
	for _, h := range held {
		if !reachable[h.id] {
			continue
		}
		entry, ok := s.awaitingDelivered[h.id]
		if !ok {
			continue
		}
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

// failDelivery finalises a locally-sent DM the retry engine gave up on
// (TTL expiry or attempts cap): outbound goes terminal — the same
// "expired"/"failed" statuses the pending-ring paths use, visible through
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
// dispatchEnvelopeRetry returns whether the envelope was actually EMITTED.
// false means it was HELD (recipient unreachable under the reachability gate),
// which the caller records as deliveryRetryEntry.Held so only genuinely-held
// messages are woken by kickDeliveryRetriesForReachable.
func (s *Service) dispatchEnvelopeRetry(envelope protocol.Envelope) (emitted bool) {
	decision := s.router.Route(envelope)

	// Sender-owned delivery emits ONLY when the recipient is reachable: a
	// directed route exists (RelayNextHop) or the recipient is a directly
	// connected subscriber (PushSubscribers). An unreachable recipient is
	// HELD — no blind gossip into the void. This is the root cure for the
	// churn/storm: an offline or long-gone recipient no longer triggers a
	// blind-gossip fan-out on every retry tick. Delivery resumes when a
	// route or connection appears — kickDeliveryRetriesForReachable (fired
	// from the announce/connect drain) re-arms held entries immediately;
	// until then the message waits in awaitingDelivered, bounded by TTL and
	// the attempts cap. See docs/protocol/relay.md INV-3.
	if s.cfg.HoldDMUntilReachable && decision.RelayNextHop == nil && len(decision.PushSubscribers) == 0 {
		log.Debug().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).Msg("delivery_retry_held_unreachable")
		return false
	}

	if len(decision.PushSubscribers) > 0 {
		s.goBackground(func() { s.pushToSubscriberSnapshot(envelope, decision.PushSubscribers) })
	}

	// gossipTargetsForRelay drops the table next-hop from the fan-out (it gets
	// the directed relay_message below) before the gates/K-of-N — so the
	// sender-owned retry tick does not duplicate push_message+relay_message to
	// the same peer.
	gossipTargets := s.gossipTargetsForRelay(envelope, decision)
	s.executeGossipTargets(envelope, gossipTargets)

	if decision.RelayNextHop != nil {
		s.sendTableDirectedRelay(s.runCtx, envelope, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops)
	} else {
		s.tryRelayToCapableFullNodes(envelope, gossipTargets)
	}
	return true
}
