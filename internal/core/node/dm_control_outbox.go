package node

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/dmcontrol"
	"github.com/piratecash/corsa/internal/core/domain"
)

// dm_control_outbox.go is the QUEUE half of the conversation-control sender:
// what is waiting to be said to a peer, and what happens to it when the
// conversation it belongs to is removed.
//
// It holds KEYS, never facts. What a queued reaction currently says is read from
// the durable record when the frame is built (dm_control_send.go), so nothing
// here is a copy of state something else can delete — which is what every "tell
// the queue it is stale" path used to be, and each of those was a race.
//
// All of the state below lives behind the sender's single mutex, together with
// the belief state in dm_control_policy.go. One mutex and not two: a pass reads
// both halves (what is queued, and whether the peer can take it) and the two
// must not be able to disagree with each other mid-pass.

// dmControlOutbox is one peer's undelivered batch.
//
// It holds KEYS, not facts. A key is "which reaction of ours this is about";
// what that reaction currently SAYS is read from the durable record at the
// moment of sending (ReactionFactsFor). The queue therefore never carries a copy
// of state that something else can delete, which is what every "tell the queue
// it is stale" path existed to paper over: a key whose row is gone resolves to
// nothing and no frame is built for it.
type dmControlOutbox struct {
	entries  []dmControlEntry
	refusals []dmControlAnswer
	dueAt    time.Time
	// debounced says the deadline above was set by the DEBOUNCE and not by a
	// retry. It is what makes "one deadline per batch" true: later taps join a
	// batch that is already armed without moving it, while a batch carrying a
	// retry deadline half a minute out is re-armed by the first fresh tap,
	// because that tap did not earn somebody else's failure.
	debounced bool
	// abandoned is raised by ForgetPeerReactions on the batch that is out of the
	// map being sent. The batch is the identity here — a pointer, not a counter
	// — because any counter that can be cleared and start again is an ABA: a
	// stale batch holding the value 0 passes the moment the peer's entry is
	// dropped, and both a new fact and the sweep drop it.
	//
	// Guarded by d.mu, like the maps.
	abandoned bool
}

// dmControlEntry is one queued reaction and when it started waiting.
//
// The stamp is PER KEY and not per batch, because the batch is a moving thing: a
// retry puts old keys back into the same outbox that fresh taps join, and a
// batch-wide stamp made a reaction decided a second ago inherit the age of one
// that has been failing for half an hour — aged out on somebody else's clock,
// before it had been tried even once.
type dmControlEntry struct {
	key      domain.ReactionKey
	queuedAt time.Time
}

// dmControlAnswer is one `unsupported` this node owes a peer, and when it
// started waiting.
//
// Stamped for the same reason a reaction is: an answer that cannot be delivered
// would otherwise be retried every thirty seconds for as long as the process
// lives. It used to be bounded by the batch's own age; when that became
// per-reaction, the answers were left with nothing.
type dmControlAnswer struct {
	command  domain.DMControlCommand
	queuedAt time.Time
}

// keysOf is what the send path works with: the entries' keys, in order.
func keysOf(entries []dmControlEntry) []domain.ReactionKey {
	keys := make([]domain.ReactionKey, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.key)
	}
	return keys
}

// dmControlForget is when a conversation was thrown away.
//
// It exists for what the PEER says back. An inner `unsupported`, or the
// transport's dtype gate, answers a frame sent before the removal and arrives
// after it — and recording it would refill the belief the removal just cleared.
// The batch that was in flight at that moment is handled by the batch itself
// (dmControlOutbox.abandoned), not from here.
type dmControlForget struct {
	at time.Time
}

// frameVerdict is what the gate before one frame says about the rest of the
// batch.
type frameVerdict uint8

const (
	// frameAllowed: build it and send it.
	frameAllowed frameVerdict = iota
	// frameAbandoned: the conversation is gone. The rest is dropped, not
	// retried — there is nothing left for it to be about.
	frameAbandoned
	// framePaused: something outside this subsystem is changing what these
	// frames would say, or changed it while they were being built. The rest goes
	// BACK, to be sent when it is over — rebuilt from the record as it stands by
	// then.
	framePaused
)

// beginFrame reports whether one frame of this batch may still be built and
// sent, and counts it while it is on its way.
//
// The permission and the count are ONE critical section on purpose. Checking and
// then sealing would leave the gap this exists to close: sealing, building and
// the hand-over to the plane all run unlocked, and a removal completing in that
// gap would still see its frame reach the transport.
func (d *dmControlSender) beginFrame(
	peer domain.PeerIdentity,
	batch *dmControlOutbox,
	builtAt uint64,
) frameVerdict {
	d.mu.Lock()
	defer d.mu.Unlock()
	if batch.abandoned {
		return frameAbandoned
	}
	if d.paused[peer] > 0 {
		return framePaused
	}
	if d.pauseGen[peer] != builtAt {
		// A delete came and went while these frames were being built. Waiting
		// for the frames in flight does not cover this pass: it had not reached
		// the gate yet, so it was not in flight, and what it built was read from
		// the record BEFORE the row was removed. The frames go back and the next
		// pass reads the record again.
		return framePaused
	}
	d.framesOut[peer]++
	return frameAllowed
}

// pauseGeneration is how many pause boundaries this peer has crossed, read by a
// pass before it reads the record.
func (d *dmControlSender) pauseGeneration(peer domain.PeerIdentity) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pauseGen[peer]
}

// ReactionSendsHeldFor reports whether something is holding this peer's frames
// back right now.
//
// Exported for the layer above to assert on: a delete's own correctness depends
// on the queue being shut WHILE it runs, and that is not visible from there in
// any other way.
func (s *Service) ReactionSendsHeldFor(peer domain.PeerIdentity) bool {
	if s == nil || s.dmControl == nil {
		return false
	}
	d := s.dmControl
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.paused[peer] > 0
}

// HoldReactionSends stops new frames from going to a peer and waits for the ones
// already being sealed. Everything queued for that peer is offered again once
// the returned release runs.
//
// It exists for the deletion of a SINGLE message, which this subsystem cannot
// see: the queue names reactions, and the frames are built from what the record
// says a moment earlier. Between that read and the hand-over to the plane the
// message can be deleted, and the frame goes out anyway — telling the peer about
// a reaction on something this node has just erased.
//
// So the deletion brackets itself: pause, wait for the frames already past the
// gate, delete, release. A frame that was already handed over went out while the
// message still existed, which is not a window but simply the past; anything
// still to come is built from the record after the delete, where the reaction is
// gone with it.
//
// Call it BEFORE the delete and release it after — see removeLocalMessage. It
// blocks for at most one hand-over to the local send queue; no network round
// trip happens under it, and it takes no lock the send path needs.
func (s *Service) HoldReactionSends(peer domain.PeerIdentity) func() {
	if s == nil || s.dmControl == nil || peer.IsZero() {
		return func() {}
	}
	d := s.dmControl
	d.mu.Lock()
	d.paused[peer]++
	// Every pause boundary moves this peer's generation, so a pass that read the
	// record on either side of it is refused at the gate even if the whole
	// delete comes and goes before it gets there.
	d.pauseGen[peer]++
	for d.framesOut[peer] > 0 {
		d.quiet.Wait()
	}
	d.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			if left := d.paused[peer] - 1; left > 0 {
				d.paused[peer] = left
			} else {
				delete(d.paused, peer)
			}
			// The RELEASE is a boundary too: a pass that started while the pause
			// was up read the record before the delete committed, and by the
			// time it reaches the gate it would find nothing in its way.
			d.pauseGen[peer]++
			d.mu.Unlock()
		})
	}
}

// endFrame releases what beginFrame took.
func (d *dmControlSender) endFrame(peer domain.PeerIdentity) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if left := d.framesOut[peer] - 1; left > 0 {
		d.framesOut[peer] = left
	} else {
		delete(d.framesOut, peer)
	}
	d.quiet.Broadcast()
}

// finishBatch takes a batch out of the in-flight registry once its pass is over.
func (d *dmControlSender) finishBatch(peer domain.PeerIdentity, batch *dmControlOutbox) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight[peer] == batch {
		delete(d.inflight, peer)
	}
}

// QueuedReactionsFor is how many of a peer's reactions are waiting to be sent.
//
// Exported for the layer above to assert on after it forgets a conversation:
// the queue is this node's alone, and "nothing of that conversation is left
// outside the database" is not a property the service layer can otherwise see.
func (s *Service) QueuedReactionsFor(peer domain.PeerIdentity) int {
	if s == nil || s.dmControl == nil {
		return 0
	}
	d := s.dmControl
	d.mu.Lock()
	defer d.mu.Unlock()
	outbox := d.pending[peer]
	if outbox == nil {
		return 0
	}
	return len(outbox.entries)
}

// dedupeEntries keeps one entry per reaction and moves a repeat to the BACK,
// with the newer stamp.
//
// One entry per key because the queue names reactions rather than carrying their
// values: twenty taps on one emoji are one thing to send, and what it says is
// read when the frame is built.
//
// Moving a repeat to the back is what the cap depends on: it drops from the
// front, so the order has to be "least recently decided first" rather than
// "first ever seen first" — otherwise the cap could throw away the reaction the
// user just changed while keeping ones nobody has touched in an hour. The stamp
// moves with it for the same reason: a key the user has just decided again has
// not been waiting since the first time they touched it.
func dedupeEntries(entries []dmControlEntry) []dmControlEntry {
	at := make(map[domain.ReactionKey]time.Time, len(entries))
	order := make([]domain.ReactionKey, 0, len(entries))
	for _, entry := range entries {
		if _, already := at[entry.key]; already {
			order = slices.DeleteFunc(order, func(other domain.ReactionKey) bool { return other == entry.key })
		}
		at[entry.key] = entry.queuedAt
		order = append(order, entry.key)
	}
	out := make([]dmControlEntry, 0, len(order))
	for _, key := range order {
		out = append(out, dmControlEntry{key: key, queuedAt: at[key]})
	}
	return out
}

func (d *dmControlSender) queueReactions(peer domain.PeerIdentity, facts []domain.ReactionFact) error {
	if peer.IsZero() {
		return fmt.Errorf("dm_control: reactions need a peer")
	}
	if len(facts) == 0 {
		return nil
	}

	// The caller hands over facts because that is what it has just decided or
	// just read; only their KEYS are kept. The values are read back from the
	// durable record when the frame is built, so a decision that changes — or a
	// message that is deleted — needs no message to this queue at all.
	keys := make([]domain.ReactionKey, 0, len(facts))
	for _, fact := range facts {
		// Checked here, at the door, rather than at flush time: a caller that
		// hands over an unusable key learns it synchronously, whereas a flush
		// hours later can only log it. The clock and the op are not checked —
		// they are not kept, and whatever the record says at send time is what
		// goes on the wire.
		candidate := dmcontrol.Fact{
			MessageID: fact.Key.MessageID,
			Emoji:     fact.Key.Emoji,
			Op:        domain.ReactionSet,
			Clock:     1,
		}
		if err := candidate.Validate(); err != nil {
			return err
		}
		if fact.Key.Actor.IsZero() {
			return fmt.Errorf("dm_control: reaction on %s names no actor", fact.Key.MessageID)
		}
		keys = append(keys, fact.Key)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	// Under the mutex, not before it: the shutdown flush lowers the flag and
	// then drains, both while holding it, so a check taken outside could let an
	// append land after the last drain and sit in the outbox forever.
	if !d.canSendLocked() {
		return fmt.Errorf("dm_control: this node has nothing running to send on")
	}
	now := d.clock()
	fresh := make([]dmControlEntry, 0, len(keys))
	for _, key := range keys {
		fresh = append(fresh, dmControlEntry{key: key, queuedAt: now})
	}
	// The deadline is armed by outboxLocked, once per batch: see there.
	outbox := d.outboxLocked(peer)
	outbox.entries = dedupeEntries(append(outbox.entries, fresh...))
	d.trimLocked(peer, outbox)
	return nil
}

// forgottenRecentlyLocked reports whether an answer from this peer is about a
// conversation that has since been forgotten. Caller must hold d.mu.
func (d *dmControlSender) forgottenRecentlyLocked(peer domain.PeerIdentity) bool {
	forgot, ok := d.forgot[peer]
	if !ok {
		return false
	}
	return d.clock().Sub(forgot.at) < dmControlForgetGrace
}

// trimLocked enforces the outbox caps.
//
// Applied on BOTH paths into the outbox — a local burst of taps and a requeue
// after a failed pass — because either can be the one that overflows it, and a
// cap on one of them is not a cap.
//
// Drops from the FRONT, which after coalescing is the least recently DECIDED
// key rather than the one queued earliest: coalesce moves a refreshed key to the
// back, so what goes is what the peer is least likely to be missing. Whatever it
// drops, the periodic re-offer will present again from the durable record.
//
// Caller must hold d.mu.
func (d *dmControlSender) trimLocked(peer domain.PeerIdentity, outbox *dmControlOutbox) {
	if over := len(outbox.entries) - dmControlOutboxMaxKeys; over > 0 {
		log.Warn().Str("peer", peer.String()).Int("dropped", over).
			Msg("dm_control_outbox_full_stalest_keys_dropped")
		outbox.entries = outbox.entries[over:]
	}
	if over := len(outbox.refusals) - maxQueuedRefusalsPerPeer; over > 0 {
		outbox.refusals = outbox.refusals[over:]
	}
}

// ForgetPeerReactions drops everything this node was going to say to a peer.
//
// Called when the user removes the contact or wipes the conversation. The
// outbox is a COPY: the facts were read out of the chatlog when they were
// queued, so deleting the rows does not empty it, and a queue left behind would
// go on offering reactions about messages the user has erased — to a contact
// they have erased — until it aged out.
//
// The refusals go too. They are beliefs about a peer we are no longer talking
// to, and if the user adds them back the answer has to be found again rather
// than remembered from before.
func (s *Service) ForgetPeerReactions(peer domain.PeerIdentity) {
	s.forgetPeerReactions(peer, true)
}

// DropQueuedReactions throws away what is queued for a peer WITHOUT forgetting
// what we know about their build.
//
// This is the conversation wipe. The contact stays, and whether their build can
// receive reactions is a property of THEM, not of the thread: clearing it would
// make the next reaction to that contact look delivered for as long as it takes
// to learn the refusal again — and starting the answer-refusing window on top
// would make even the fresh answer be ignored, for over half an hour.
//
// A contact REMOVAL is the other case and takes everything: see
// ForgetPeerReactions.
func (s *Service) DropQueuedReactions(peer domain.PeerIdentity) {
	s.forgetPeerReactions(peer, false)
}

// forgetPeerReactions empties a peer's queue and, when the peer itself is going
// away, everything this node believes about them.
func (s *Service) forgetPeerReactions(peer domain.PeerIdentity, beliefs bool) {
	if s == nil || s.dmControl == nil || peer.IsZero() {
		return
	}
	d := s.dmControl
	d.mu.Lock()
	defer d.mu.Unlock()
	if outbox := d.pending[peer]; outbox != nil {
		log.Debug().Str("peer", peer.String()).Bool("beliefs", beliefs).
			Int("reactions", len(outbox.entries)).Int("refusals", len(outbox.refusals)).
			Msg("dm_control_outbox_dropped_with_the_conversation")
		delete(d.pending, peer)
	}
	if beliefs {
		for key := range d.refusedAt {
			if key.peer == peer {
				delete(d.refusedAt, key)
			}
		}
		delete(d.refusedTypeAt, peer)
		// And the record of having spoken to them, which is an admission in its
		// own right: left behind, it lets a REMOVED contact keep sending — an
		// answer of theirs would still be believed, and what it writes would
		// meet them again if the user ever adds them back. A wipe keeps it: the
		// contact is still there and their answer to the reaction we sent a
		// moment ago is on its way.
		delete(d.sentAt, peer)
		// What the PEER says back outlives the conversation: an answer to a
		// frame sent a moment ago arrives after this, and recording it would
		// refill the belief just cleared. The stamp refuses those for
		// dmControlForgetGrace.
		//
		// Only on this path. A wipe keeps the belief, so there is nothing to
		// refill and nothing to refuse — and refusing would silence the answers
		// about a contact the user still has.
		d.forgot[peer] = dmControlForget{at: d.clock()}
	}

	// The batch that is out of the map being sent cannot be reached by clearing
	// the map — takeDue handed it out and the pass owns it. Marking it stops the
	// frames that have not started and keeps requeue from putting the rest back.
	if batch := d.inflight[peer]; batch != nil {
		batch.abandoned = true
	}

	// And then WAIT for the frames already past that mark. Sealing, building and
	// the hand-over to the plane run unlocked, so a mark alone would leave a
	// frame in that gap free to reach the transport after the conversation is
	// gone. This returns only when nothing of it can.
	//
	// The wait is bounded by one hand-over to the local send queue, which is
	// what the plane's accept is; no network round trip happens under it.
	for d.framesOut[peer] > 0 {
		d.quiet.Wait()
	}
}

// outboxLocked returns the peer's batch, arming its deadline the first time
// anything is put in it.
//
// The deadline is set once per batch and not extended by later additions: an
// extended deadline is a debounce that never fires while the user keeps
// tapping, which is precisely when they most expect the reaction to have gone.
//
// Caller must hold d.mu.
func (d *dmControlSender) outboxLocked(peer domain.PeerIdentity) *dmControlOutbox {
	outbox := d.pending[peer]
	if outbox == nil {
		outbox = &dmControlOutbox{}
		d.pending[peer] = outbox
	}
	if outbox.debounced {
		// Already armed. Later taps join the batch they find; moving its
		// deadline with every tap would make the delay depend on how many times
		// the user pressed, and the jitter is there so that a frame's timing
		// says nothing about when the tap was.
		return outbox
	}
	// Not armed: a new batch, or one carrying a RETRY deadline half a minute out
	// that this tap did not earn. One draw, here.
	outbox.dueAt = d.clock().Add(dmControlDebounceFloor + d.jitter())
	outbox.debounced = true
	return outbox
}

// takeDue removes and returns every batch whose deadline has passed, and sweeps
// expired refusals in the same pass.
//
// Removing is what lets send run without the mutex; whatever it fails to hand
// over comes back through requeue.
func (d *dmControlSender) takeDue(now time.Time) (
	map[domain.PeerIdentity]*dmControlOutbox,
	[]domain.PeerIdentity,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var due map[domain.PeerIdentity]*dmControlOutbox
	for peer, outbox := range d.pending {
		if outbox.dueAt.After(now) {
			continue
		}
		if due == nil {
			due = map[domain.PeerIdentity]*dmControlOutbox{}
		}
		outbox.abandoned = false
		d.inflight[peer] = outbox
		due[peer] = outbox
		delete(d.pending, peer)
	}

	// A belief that expires is a peer that BECOMES able to receive reactions
	// again, as far as anything here can tell, and the UI drew a notice saying
	// the opposite. Nothing else will say so: a later session clears an entry
	// that is already gone and therefore announces nothing.
	expired := map[domain.PeerIdentity]struct{}{}
	for key, at := range d.refusedAt {
		if now.Sub(at) < dmControlUnsupportedTTL {
			continue
		}
		delete(d.refusedAt, key)
		if key.command == domain.DMControlReactions {
			expired[key.peer] = struct{}{}
		}
	}
	for peer, at := range d.refusedTypeAt {
		if now.Sub(at) >= dmControlUnsupportedTTL {
			delete(d.refusedTypeAt, peer)
			expired[peer] = struct{}{}
		}
	}
	// Only the peers left with NO reason to be held back: one belief can expire
	// while the other still stands, and the answer the UI asks for is the union.
	var cleared []domain.PeerIdentity
	for peer := range expired {
		if _, held := d.refusedTypeAt[peer]; held {
			continue
		}
		if _, held := d.refusedAt[refusalKey{peer: peer, command: domain.DMControlReactions}]; held {
			continue
		}
		cleared = append(cleared, peer)
	}

	// Swept in the same pass, for the same reason and against the same window:
	// past it an answer can no longer be about anything we sent.
	for peer, at := range d.sentAt {
		if now.Sub(at) >= dmControlForgetGrace {
			delete(d.sentAt, peer)
		}
	}

	// Swept in the same pass, and independent of any batch: what an entry decides
	// is whether to believe an ANSWER from the peer, and the batch that was in
	// flight when the conversation was thrown away carries its own mark.
	for peer, forgot := range d.forgot {
		if now.Sub(forgot.at) >= dmControlForgetGrace {
			delete(d.forgot, peer)
		}
	}
	return due, cleared
}

// requeue puts back what a pass could not hand to the plane, to be tried again
// after dmControlRetryDelay.
//
// Age is measured from when the facts FIRST waited, so a batch that is retried
// forever still ends; and the cap drops the OLDEST, because the newest facts
// are the ones whose state the peer is missing. Both are the honest residual of
// having no reconciliation yet: a queue in memory has to end somewhere, and
// saying where beats a silent drop on the first flap.
func (d *dmControlSender) requeue(
	peer domain.PeerIdentity,
	entries []dmControlEntry,
	refusals []dmControlAnswer,
	batch *dmControlOutbox,
	retryIn time.Duration,
) {
	if len(entries) == 0 && len(refusals) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.canSendLocked() {
		// The loop is gone; there is nothing to retry onto.
		return
	}
	if batch.abandoned {
		// The contact was removed, or the thread wiped, while this batch was out
		// of the map being sent. Putting it back would rebuild the queue that
		// removal just emptied, with facts about messages that no longer exist.
		log.Debug().Str("peer", peer.String()).
			Int("reactions", len(entries)).Int("refusals", len(refusals)).
			Msg("dm_control_batch_dropped_conversation_forgotten_mid_send")
		return
	}
	now := d.clock()

	// Aged out ONE BY ONE, on each key's own clock. A batch-wide age made a
	// reaction decided a second ago share the fate of one that had been failing
	// for half an hour. The answers are bounded on the same terms: an
	// undeliverable `unsupported` with no age would be retried every thirty
	// seconds for as long as the process lives.
	kept := make([]dmControlEntry, 0, len(entries))
	for _, entry := range entries {
		if now.Sub(entry.queuedAt) >= dmControlOutboxMaxAge {
			continue
		}
		kept = append(kept, entry)
	}
	keptAnswers := make([]dmControlAnswer, 0, len(refusals))
	for _, answer := range refusals {
		if now.Sub(answer.queuedAt) >= dmControlOutboxMaxAge {
			continue
		}
		keptAnswers = append(keptAnswers, answer)
	}
	if dropped := len(entries) - len(kept) + len(refusals) - len(keptAnswers); dropped > 0 {
		log.Warn().Str("peer", peer.String()).Int("dropped", dropped).
			Msg("dm_control_outbox_aged_out")
	}
	if len(kept) == 0 && len(keptAnswers) == 0 {
		return
	}

	outbox := d.pending[peer]
	if outbox == nil {
		outbox = &dmControlOutbox{dueAt: now.Add(retryIn)}
		d.pending[peer] = outbox
	}
	// Retried keys go in FRONT of anything queued since: they are older, and the
	// cap drops from the front. dedupeEntries then moves any key that was queued
	// again in the meantime to the back with its newer stamp, so "least recently
	// decided first" holds across a requeue too.
	outbox.entries = dedupeEntries(append(append([]dmControlEntry{}, kept...), outbox.entries...))
	outbox.refusals = append(append([]dmControlAnswer{}, keptAnswers...), outbox.refusals...)
	d.trimLocked(peer, outbox)
	// The retry waits its delay, and NEVER pushes a deadline back — not one
	// something fresher has set, and not one that has already come due. A tap
	// made while this batch was in flight is not the failure's to punish, and a
	// deadline already past means "send it now", which a retry delay would turn
	// into another thirty seconds of silence.
	if retry := now.Add(retryIn); retry.Before(outbox.dueAt) {
		outbox.dueAt = retry
	}
}
