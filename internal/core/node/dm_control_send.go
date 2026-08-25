package node

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/dmcontrol"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_control_send.go is the PASS: turning what is waiting into frames, sealing
// them and handing them to the plane.
//
// The subsystem is three files, one responsibility each, sharing one mutex:
//
//   - dm_control_outbox.go — what is waiting to be said to each peer (keys, not
//     facts), and what happens to it when the conversation is removed;
//   - dm_control_policy.go — what this node believes about a peer's build, and
//     the answers it owes a peer whose commands it cannot read;
//   - this file — the pass itself, plus the timing constants everything else
//     measures against.
//
// One mutex across all three and not one per file: a pass reads both halves —
// what is queued, and whether the peer can take it — and the two must not be
// able to disagree with each other mid-pass.
//
// # Nothing here knows a fact ARRIVED, and the design follows from that
//
// SendQueued means the local write queue accepted the frame. The writer may
// still drop it on its send deadline; a transit hop with no route drops it
// SILENTLY by contract ("recovery belongs to the originator",
// datagram/pipeline_routed.go); and a last hop that does not declare the dtype
// drops it without telling the originator, because that gate answers the node
// that HANDED IT OVER, which on a relayed frame is not us.
//
// So "delivered" is not a state this file can hold, and a retry keyed on local
// outcomes covers only faults before the first enqueue. Building a bigger
// in-memory retry on top of that would be a second, weaker answer to the
// question §6.3 answers, and it would still not deliver anything.
//
// The delivery guarantee therefore does NOT live here. It lives in
// reofferReactions: every time a session with a peer comes up, this node offers
// that conversation's own facts again, read from the durable record. Re-offering
// is free of consequence — the merge is one clock comparison, so a fact the peer
// already has changes nothing — and it is the §6.3 shape minus the digest that
// would let us skip what is already agreed.
//
// What the outbox IS: a debounce buffer that batches a burst and blurs the
// timing of a tap. It retries within a session, because a session can stay up
// for hours and a route that flapped for a second should not wait for the next
// one; and it is bounded in both directions, because it is memory and the
// durable record is what recovery reads.

const (
	// dmControlDebounceFloor and dmControlDebounceJitter set how long facts wait
	// before they leave: 1.2s to 2.0s.
	//
	// The wait buys two things at once. It batches — a user setting and
	// unsetting a few reactions produces one frame instead of four — and it
	// breaks the tie between an action and a frame, so a relay watching the
	// link cannot read a keystroke's timing off it. The refusal answer waits in
	// the same queue for the same reason: an immediate reply to an incoming
	// frame is a correlation whatever it contains.
	dmControlDebounceFloor  = 1200 * time.Millisecond
	dmControlDebounceJitter = 800 * time.Millisecond

	// dmControlTick is how often the outbox is examined. Well under the floor,
	// so the jitter drawn per batch is what decides the delay rather than the
	// tick quantising it into visible steps.
	dmControlTick = 200 * time.Millisecond

	// dmControlUnsupportedTTL bounds how long "this peer cannot do it" is
	// believed without re-testing.
	//
	// It has to expire for two independent reasons. The peer may update, and
	// nothing else would ever tell us. And the map is keyed by peer identity,
	// so without expiry a node that meets many peers grows it forever — the
	// same unbounded-map shape that has already cost this project memory once.
	dmControlUnsupportedTTL = time.Hour

	// dmControlRetryDelay is how long an undelivered batch waits before it is
	// tried again. Well above the debounce, because the causes it retries past
	// — no route, a box key we have not learned, a peer that refuses the type —
	// are all measured in seconds at best.
	dmControlRetryDelay = 30 * time.Second

	// dmControlOutboxMaxAge is how long a batch may keep being retried WITHIN a
	// session.
	//
	// It ends there because the queue is memory and the recovery is elsewhere:
	// past this, the facts are still in the chatlog and the next session with
	// the peer offers them again. Nothing is lost by giving up here; what would
	// be lost is the pretence that an in-memory queue is the delivery path.
	dmControlOutboxMaxAge = 30 * time.Minute

	// dmControlPausedRetryDelay is how long frames wait when a message delete
	// sent them back. The delete holds the queue for one transaction, so this is
	// the shortest wait that is still a wait rather than a spin.
	dmControlPausedRetryDelay = dmControlDebounceFloor

	// dmControlForgetGrace is how long after forgetting a conversation this node
	// still refuses what the peer says about it.
	//
	// It covers the answers to frames sent BEFORE the removal, which are the
	// only ones that can still be on their way: an inner `unsupported` or the
	// transport's dtype gate arriving afterwards would refill exactly the belief
	// the removal just cleared, and that belief then silences reactions for an
	// hour if the user adds the contact back.
	//
	// Derived from how long the whole exchange can LIVE, not from a round trip.
	// The chain is three legs, and only counting the middle one left the window
	// shorter than the answer it exists to refuse:
	//
	//   1. our frame is already on the plane when the removal happens, and it
	//      stays valid there for up to DatagramBaseReplayWindow;
	//   2. the peer queues its `unsupported` in an outbox of this same design,
	//      which retries it for up to dmControlOutboxMaxAge, plus one
	//      dmControlRetryDelay for the attempt that was on its way when their
	//      outbox gave up;
	//   3. that answer then travels back under the same validity ceiling.
	//
	// A window shorter than the sum lets the answer to a REMOVED conversation
	// land against the one that replaced it and mark it "cannot receive
	// reactions" for an hour.
	//
	// A new fact for the peer does NOT end it early. During the window this node
	// keeps offering reactions to a peer that may not take them, which costs
	// padded frames the peer drops; ending it early costs a false "they cannot
	// receive reactions" against a conversation that has just been created,
	// which is a lie to the user for an hour.
	dmControlForgetGrace = 2*domain.DatagramBaseReplayWindow + dmControlOutboxMaxAge + dmControlRetryDelay

	// How many facts one re-offer carries is the STORE's decision, not this
	// file's: it is the one that knows the conversation. See
	// service.ReofferLimit.
)

// dmControlOutboxMaxKeys bounds how many of a peer's reactions wait at once.
//
// It is reached far less often than it looks, because the outbox holds each KEY
// once: a user toggling one reaction twenty times contributes one entry, not
// twenty, and what that entry is worth is read at send time. That is also why
// the cap can be honest about what it drops — every entry is the current state
// of a distinct reaction, never a superseded step of one.
const dmControlOutboxMaxKeys = 512

// maxQueuedRefusalsPerPeer bounds how many distinct commands one peer can make
// this node queue answers for inside one debounce window.
//
// Each queued refusal becomes a padded frame at flush, so without a cap a peer
// sending N frames with N invented command names makes this node emit N frames
// back at it. Nothing legitimate produces more than a couple: a build only
// answers for commands it does not know.
const maxQueuedRefusalsPerPeer = 8

// dmControlSender batches, delays and sends conversation-control commands.
//
// It carries its own mutex and is reached through an immutable Service field,
// so it stays outside the seven domain mutexes of docs/locking.md — the state
// here is this subsystem's alone and no path crosses into it holding one of
// them. See docs/locking.md, "fields outside the scheme".
type dmControlSender struct {
	svc *Service

	mu sync.Mutex
	// pending is keyed by peer and drained on every flush, so it is bounded by
	// the number of peers written to inside one debounce window — PROVIDED
	// something drains it, which is why queueReactions refuses to accept
	// anything while nothing is draining it (see canSendLocked).
	pending map[domain.PeerIdentity]*dmControlOutbox
	// refusedAt remembers which COMMAND a peer's build does not know, learned
	// from an inner `unsupported` answer that names it. Swept against
	// dmControlUnsupportedTTL.
	refusedAt map[refusalKey]time.Time
	// refusedTypeAt remembers that a peer does not declare the dm_control dtype
	// at all, learned from the transport's own `unsupported_dtype` gate.
	//
	// Separate from refusedAt, and the separation is the point: the gate answers
	// about the TYPE and cannot see inside the sealed payload, so reading a
	// command out of it is an inference the signal does not support. It would
	// also be the wrong shape — a peer that declares no dm_control refuses every
	// command in it, not the one that happened to be in the frame.
	refusedTypeAt map[domain.PeerIdentity]time.Time
	// forgot names the conversations whose queue was thrown away, and when.
	// Entries are swept against dmControlForgetGrace and by nothing else: a new
	// fact for the peer deliberately does NOT clear one, because the answer to
	// the conversation that was removed can still be in flight and would then be
	// believed against the conversation that replaced it.
	forgot map[domain.PeerIdentity]dmControlForget
	// inflight is the batch each peer currently has out of `pending` — takeDue
	// put it there and the pass has not finished with it yet. It is how
	// ForgetPeerReactions reaches a batch the map no longer holds.
	inflight map[domain.PeerIdentity]*dmControlOutbox
	// framesOut counts the frames of a peer that are between "allowed to go" and
	// "handed to the plane". Sealing and building happen unlocked, so a check
	// alone would leave exactly that gap open: ForgetPeerReactions waits on this
	// instead, and when it returns nothing of that conversation can still reach
	// the transport.
	framesOut map[domain.PeerIdentity]int
	// paused counts the reasons no frame of a peer may leave right now. Raised
	// by HoldReactionSends while something outside this subsystem changes what
	// the frames would have said — a single message being deleted, which the
	// queue cannot see because it names reactions, not messages.
	paused map[domain.PeerIdentity]int
	// pauseGen counts the pause BOUNDARIES of each peer — every raise and every
	// release. A pass reads its peer's value before it reads the record and
	// hands it back at the gate: if it has moved, a delete began or ended while
	// these frames were being built, and what they say may already be false.
	//
	// Both edges are counted, not just the raise. A pass that starts while a
	// pause is up reads the record BEFORE the delete commits and would find the
	// gate open again by the time it got there — the raise is behind it and the
	// release is what it must notice.
	//
	// Per peer, because a delete in one conversation says nothing about another:
	// a shared counter made every pass everywhere start again, and a series of
	// deletes in one thread could hold up reactions in all the others. Never
	// pruned, for the same reason the counter is not a flag: an entry removed
	// and recreated compares equal to a value read before it existed.
	pauseGen map[domain.PeerIdentity]uint64
	// sentAt is when this node last handed a dm_control frame to the plane for
	// a peer. It is what makes an ANSWER from them believable: `unsupported`
	// answers something we sent, and a conversation with no messages left in it
	// — a thread the user has just wiped — is still one we may have sent a
	// reaction into a second ago.
	//
	// Written only by our own send path, so nothing remote can grow it, and
	// swept against dmControlForgetGrace, which is the same chain this window
	// has to cover: our frame's validity, their retries, the trip back.
	sentAt map[domain.PeerIdentity]time.Time
	// quiet is broadcast when a peer's frame count reaches zero.
	quiet *sync.Cond

	// draining is raised by dmControlSendLoop while it is running and lowered
	// when it leaves. It is the real predicate behind canSendLocked: the layer
	// handle is stored once in NewService and never cleared, so "a plane exists"
	// remains true both before Run and forever after it returns, while the only
	// thing that empties the outbox exists strictly in between.
	//
	// Under d.mu with the rest of the state, not an atomic beside it: the
	// shutdown flush has to lower it and drain as ONE step, or an append taken
	// between the two joins a batch nothing will ever send.
	draining bool

	clock  func() time.Time
	jitter func() time.Duration
	// dispatch hands one signed frame to the datagram plane; nil in production —
	// sendOne falls back to the pipeline. Overridable ONLY in tests, and it is
	// the network boundary of this file: everything sendOne decides is decided
	// FROM the outcome this returns, so without a seam here the branch that
	// records "this peer's build cannot take the command" — the one the whole
	// transport was chosen for — cannot be exercised without a live mesh.
	dispatch func(ctx context.Context, frame protocol.DatagramFrame) dmControlDispatch

	builderOnce sync.Once
	builder     *datagram.RoutedFrameBuilder
	builderErr  error
}

// dmControlFrame is one sealed payload together with the command it carries and
// the facts it would have delivered.
//
// The command travels with it so a log line can say what was in a frame that
// did not leave. The KEYS travel with it because a frame that is not handed to
// the plane has to put them back, and this is what makes "not sent" a retry
// rather than a loss — of the key, not of the fact, which was never here to
// lose: it lives in the chatlog and is read again on the next attempt.
type dmControlFrame struct {
	command domain.DMControlCommand
	plain   []byte
	// entries is what this frame would have delivered, so an undelivered frame
	// can put exactly those back — with the stamps the queue ages them by.
	entries []dmControlEntry
	// refused is the answer this frame carries, kept with its stamp so an
	// undelivered one can be queued again and still age out.
	refused dmControlAnswer
}

func newDMControlSender(svc *Service) *dmControlSender {
	sender := &dmControlSender{
		svc:           svc,
		pending:       map[domain.PeerIdentity]*dmControlOutbox{},
		refusedAt:     map[refusalKey]time.Time{},
		refusedTypeAt: map[domain.PeerIdentity]time.Time{},
		forgot:        map[domain.PeerIdentity]dmControlForget{},
		inflight:      map[domain.PeerIdentity]*dmControlOutbox{},
		framesOut:     map[domain.PeerIdentity]int{},
		paused:        map[domain.PeerIdentity]int{},
		sentAt:        map[domain.PeerIdentity]time.Time{},
		pauseGen:      map[domain.PeerIdentity]uint64{},
		clock:         func() time.Time { return time.Now().UTC() },
		jitter: func() time.Duration {
			return time.Duration(rand.Int64N(int64(dmControlDebounceJitter)))
		},
	}
	sender.quiet = sync.NewCond(&sender.mu)
	return sender
}

// canSendLocked reports whether anything will drain the outbox.
//
// It asks the loop, not the layer. The layer handle is stored once during
// NewService and never cleared, so testing it would answer true before Run and
// forever after Run returns — in both windows queueing is not "delayed sending"
// but a map that grows for the life of the process. Refusing at the door is
// what keeps the field comment on `pending` true.
//
// Caller must hold d.mu, so that the shutdown flush — which lowers the flag and
// drains under the same mutex — cannot slip between a caller's check and its
// append.
func (d *dmControlSender) canSendLocked() bool {
	return d != nil && d.draining
}

// QueueReactionFacts hands this node's own reaction decisions to the peer they
// concern.
//
// It returns as soon as the facts are queued. The send happens a second or so
// later and its outcome is not reported back: there is nothing the caller could
// do with it that the state model does not already do better, and blocking a
// tap on a network round trip would make the chip appear late for no gain.
func (s *Service) QueueReactionFacts(peer domain.PeerIdentity, facts []domain.ReactionFact) error {
	if s == nil || s.dmControl == nil {
		return fmt.Errorf("dm_control: this node has no control sender")
	}
	return s.dmControl.queueReactions(peer, facts)
}

// reofferReactions offers this conversation's own facts to the peer again.
//
// Called when a session with the peer comes up, and it is where the delivery
// guarantee lives — see the file comment. Nothing here knows what arrived, so
// nothing here decides what to skip: the whole bounded set goes, and the
// receiver's merge makes the ones it already has free.
//
// Runs on the caller's goroutine, so the caller puts it on the background pool.
func (s *Service) reofferReactions(ctx context.Context, peer domain.PeerIdentity) {
	if s == nil || s.dmControl == nil || peer.IsZero() {
		return
	}
	store := s.conversationControl
	if store == nil {
		return
	}
	// The queueing happens INSIDE the read, under the store's removal lease: a
	// removal of this conversation must not be able to land between the two and
	// leave a queue full of facts about messages it has just erased.
	err := store.ReactionsToReoffer(ctx, peer, func(facts []domain.ReactionFact) error {
		if len(facts) == 0 {
			return nil
		}
		if err := s.dmControl.queueReactions(peer, facts); err != nil {
			return err
		}
		log.Debug().Str("peer", peer.String()).Int("facts", len(facts)).
			Msg("dm_control_reactions_reoffered_on_session")
		return nil
	})
	if err != nil {
		// Either the read failed or the loop is not running (before Run, or
		// after it returned). The next session does this again, so there is
		// nothing to recover here.
		log.Debug().Err(err).Str("peer", peer.String()).
			Msg("dm_control_reoffer_not_queued")
	}
}

// flushDue sends everything that is due. Separate from the loop so a test can
// drive one pass without a clock to wait on.
func (d *dmControlSender) flushDue(ctx context.Context, now time.Time) {
	due, cleared := d.takeDue(now)
	// Published OUTSIDE the sweep's lock, like every other effect here. These
	// are the peers whose refusal expired on this pass: to the UI that is the
	// same news as a peer upgrading, and nothing else reports it.
	for _, peer := range cleared {
		d.announceRefusalChanged(peer, true)
	}
	for peer, outbox := range due {
		entries, refusals, retryIn := d.send(ctx, peer, outbox)
		d.requeue(peer, entries, refusals, outbox, retryIn)
		d.finishBatch(peer, outbox)
	}
}

// sendVerdict is what one frame's attempt says about the NEXT frame to the same
// peer.
//
// Three values and not a bool, because the first version collapsed them and got
// the common case wrong: a momentary "no route" on the first of three frames
// silently dropped the other two, and there is no queue behind them to notice.
type sendVerdict uint8

const (
	// sendDelivered: the frame is on the outbound queue.
	sendDelivered sendVerdict = iota
	// sendPeerCannot: this peer's build does not take the type. Every further
	// frame to them fails identically, so the rest of the batch is pointless.
	sendPeerCannot
	// sendThisFrameOnly: something about this attempt failed — no route, a
	// missing box key, a local fault. It says nothing about the next frame, so
	// the batch continues.
	sendThisFrameOnly
)

// send turns one peer's batch into frames, puts them on the wire, and reports
// back everything that did NOT get there.
//
// Returning the leftovers rather than swallowing them is the whole contract: the
// outbox is the only copy, so a frame that was not handed to the plane has to
// come back or the user's reaction is gone. The caller requeues them.
//
// The refusals are built even when the facts could not be packed, and the facts
// are sent even when a refusal could not be encoded: the two are independent
// statements that happen to share a deadline, and letting one failure take the
// other is how a peer stops hearing about a feature because of an unrelated bug.
func (d *dmControlSender) send(
	ctx context.Context,
	peer domain.PeerIdentity,
	outbox *dmControlOutbox,
) (entries []dmControlEntry, refusals []dmControlAnswer, retryIn time.Duration) {
	// Taken BEFORE the record is read. Everything below is built from what the
	// record said at this moment, and a delete that lands while it is being
	// built makes it stale — see beginFrame.
	// What is handed back waits the ordinary retry delay, unless a pause sends it
	// back — a pause is over in milliseconds, and half a minute of silence for a
	// message delete elsewhere in the same conversation is the user's reaction
	// held hostage to it.
	retryIn = dmControlRetryDelay
	builtAt := d.pauseGeneration(peer)
	frames, held := d.framesFor(ctx, peer, outbox)
	// held is what was not even offered — a peer we already know cannot take
	// the type. It waits exactly like an undelivered frame does.
	entries = append(entries, held...)

	delivered := 0
	for i, frame := range frames {
		switch d.beginFrame(peer, outbox, builtAt) {
		case frameAbandoned:
			// The user removed this contact, or wiped the thread, and this batch
			// was marked while the pass was working on it. What is left is about
			// a conversation that no longer exists, so it is dropped rather than
			// sent or requeued — requeue refuses it on the same mark.
			log.Debug().Str("peer", peer.String()).
				Int("delivered", delivered).Int("frames", len(frames)).
				Msg("dm_control_batch_abandoned_conversation_forgotten_mid_send")
			return nil, nil, retryIn
		case framePaused:
			// A message is being deleted, or was deleted while these frames were
			// being built, and they were built from what the record said before
			// that. They go BACK rather than out: the next pass builds them
			// again from the record as it stands after the delete, where a
			// reaction on the deleted message no longer exists.
			for _, rest := range frames[i:] {
				entries = append(entries, rest.entries...)
				if rest.command == domain.DMControlUnsupported {
					refusals = append(refusals, rest.refused)
				}
			}
			log.Debug().Str("peer", peer.String()).
				Int("delivered", delivered).Int("frames", len(frames)).
				Msg("dm_control_batch_paused_while_a_message_is_deleted")
			return entries, refusals, dmControlPausedRetryDelay
		}
		verdict := d.sendOne(ctx, peer, frame)
		if verdict == sendDelivered {
			// Recorded on the ACCEPTED frame only: an attempt that never
			// reached the plane cannot be answered. noteSpokeTo itself refuses
			// to write for a conversation that was forgotten while this frame
			// was on its way.
			d.noteSpokeTo(peer)
		}
		d.endFrame(peer)
		if verdict == sendDelivered {
			delivered++
			continue
		}
		entries = append(entries, frame.entries...)
		if frame.command == domain.DMControlUnsupported {
			refusals = append(refusals, frame.refused)
		}
		if verdict == sendPeerCannot {
			// Every further frame to this peer fails identically right now, so
			// the rest of the batch is not attempted — but it is not lost
			// either: what has not been tried goes back with the rest.
			for _, rest := range frames[i+1:] {
				entries = append(entries, rest.entries...)
				if rest.command == domain.DMControlUnsupported {
					refusals = append(refusals, rest.refused)
				}
			}
			log.Debug().Str("peer", peer.String()).
				Int("delivered", delivered).Int("frames", len(frames)).
				Msg("dm_control_batch_paused_peer_cannot_receive")
			return entries, refusals, retryIn
		}
	}
	if delivered < len(frames) || len(held) > 0 {
		log.Debug().Str("peer", peer.String()).
			Int("delivered", delivered).Int("frames", len(frames)).
			Int("requeued_reactions", len(entries)).
			Msg("dm_control_batch_partly_sent")
	}
	return entries, refusals, retryIn
}

// framesFor turns a batch into the frames it becomes, and reports the keys it
// deliberately did not build a frame for.
//
// This is where a KEY becomes something to say: the conversation store is asked
// what each queued reaction is worth right now. A key whose row has been deleted
// — with its message, with the conversation, or because the user cleared the
// reaction away — comes back with nothing and is neither sent nor requeued. That
// is the property the queue is built around: nothing has to TELL it about a
// deletion, because it never held a copy to be told about.
//
// Split out of send so what this node DECIDES to put on the wire — which
// commands, and whether the facts are offered at all — can be checked without a
// network behind it.
//
// "Not offered" is NOT "discarded". A peer we already know cannot receive them
// gets no frame, because sealing and signing one for a certainty costs the
// network for nothing — but the keys come straight back to the caller and wait
// for the peer's next session to say otherwise.
func (d *dmControlSender) framesFor(
	ctx context.Context,
	peer domain.PeerIdentity,
	outbox *dmControlOutbox,
) (frames []dmControlFrame, held []dmControlEntry) {
	switch {
	case len(outbox.entries) == 0:
	case d.cannotTakeReactions(peer):
		log.Debug().Str("peer", peer.String()).Int("reactions", len(outbox.entries)).
			Msg("dm_control_reactions_held_for_a_peer_that_cannot_receive_them")
		held = append(held, outbox.entries...)
	default:
		facts, err := d.resolve(ctx, peer, keysOf(outbox.entries))
		switch {
		case err != nil:
			// The record could not be read. The keys are not stale, only
			// unreadable right now, so they go back and are tried again.
			log.Warn().Err(err).Str("peer", peer.String()).Int("reactions", len(outbox.entries)).
				Msg("dm_control_reactions_unreadable")
			held = append(held, outbox.entries...)
		case len(facts) == 0:
			// Everything queued has since been deleted. Nothing to send and
			// nothing to retry.
			log.Debug().Str("peer", peer.String()).Int("reactions", len(outbox.entries)).
				Msg("dm_control_reactions_gone_before_they_were_sent")
		default:
			frames = append(frames, d.reactionFrames(peer, outbox.entries, facts)...)
		}
	}

	// Refusals go out even to a peer that refused reactions: they are a
	// different command, and their whole job is to answer something the peer
	// sent US. A failure to encode one does not stop the facts, and a failure
	// to pack the facts does not stop the refusals — the two are independent
	// statements that happen to share a deadline.
	for _, answer := range outbox.refusals {
		plain, err := dmcontrol.Encode(dmcontrol.UnsupportedPayload(domain.ConversationDirect, answer.command))
		if err != nil {
			log.Warn().Err(err).Str("peer", peer.String()).Str("command", answer.command.String()).
				Msg("dm_control_unsupported_encode_failed")
			continue
		}
		frames = append(frames, dmControlFrame{
			command: domain.DMControlUnsupported,
			plain:   plain,
			refused: answer,
		})
	}
	return frames, held
}

// resolve asks the conversation store what the queued keys are worth now.
//
// Called with no lock of the sender's held: it is a database read, and the rule
// for this file is that d.mu never spans anything external.
func (d *dmControlSender) resolve(
	ctx context.Context,
	peer domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	if d.svc == nil {
		return nil, fmt.Errorf("dm_control: no node to read the conversation from")
	}
	store := d.svc.conversationControl
	if store == nil {
		return nil, fmt.Errorf("dm_control: this node keeps no conversation state")
	}
	return store.ReactionFactsFor(ctx, peer, keys)
}

// reactionFrames packs resolved facts into frames, each carrying the keys it
// would deliver.
func (d *dmControlSender) reactionFrames(
	peer domain.PeerIdentity,
	queued []dmControlEntry,
	facts []domain.ReactionFact,
) []dmControlFrame {
	// Keyed by what travels on the wire, because that is the only thing a frame
	// can be read back as. POSITION would be the obvious index and is wrong:
	// PackReactions skips an unusable fact and packs the ones behind it, so the
	// n-th packed fact is not the n-th offered one, and slicing by position
	// would put somebody else's keys back on a failure.
	entryOf := make(map[dmControlKey]dmControlEntry, len(queued))
	for _, entry := range queued {
		entryOf[dmControlKey{message: entry.key.MessageID, emoji: entry.key.Emoji}] = entry
	}

	wire := make([]dmcontrol.Fact, 0, len(facts))
	for _, fact := range facts {
		wire = append(wire, dmcontrol.Fact{
			MessageID: fact.Key.MessageID,
			Emoji:     fact.Key.Emoji,
			Op:        fact.Op,
			Clock:     fact.Clock,
		})
	}

	packed, err := dmcontrol.PackReactions(domain.ConversationDirect, wire)
	if err != nil {
		// What could not be packed is unusable, not undelivered — putting those
		// keys back would retry them forever — and the facts behind it are in
		// `packed` all the same.
		log.Warn().Err(err).Str("peer", peer.String()).Int("reactions", len(wire)).
			Msg("dm_control_facts_dropped_as_unpackable")
	}
	var frames []dmControlFrame
	for _, plain := range packed {
		decoded, decodeErr := dmcontrol.Decode(plain)
		if decodeErr != nil {
			// Unreachable: we encoded it a line ago. Refusing to guess which
			// reactions it held is better than requeueing the wrong ones.
			log.Warn().Err(decodeErr).Str("peer", peer.String()).
				Msg("dm_control_own_frame_unreadable")
			continue
		}
		carried := make([]dmControlEntry, 0, len(decoded.Facts))
		for _, fact := range decoded.Facts {
			entry, known := entryOf[dmControlKey{message: fact.MessageID, emoji: fact.Emoji}]
			if !known {
				// Unreachable for the same reason: this frame was built from
				// the queue above a moment ago.
				continue
			}
			carried = append(carried, entry)
		}
		frames = append(frames, dmControlFrame{
			command: domain.DMControlReactions,
			plain:   plain,
			entries: carried,
		})
	}
	return frames
}

// dmControlKey names one reaction the way a frame carries it: the message and
// the emoji. The actor is not part of it — every fact in a frame this node
// builds is its own.
type dmControlKey struct {
	message domain.MessageID
	emoji   string
}

// sendOne seals and sends one frame.
func (d *dmControlSender) sendOne(ctx context.Context, peer domain.PeerIdentity, frame dmControlFrame) sendVerdict {
	plain := frame.plain
	layer := d.svc.datagramLayer()
	if layer == nil {
		// The plane was torn down between the queue and this flush. Nothing
		// about the peer, and nothing to remember.
		return sendThisFrameOnly
	}
	boxKey, ok := d.svc.knownBoxKey(peer.String())
	if !ok {
		boxKey, ok = d.svc.peerBoxKeyBase64(peer)
	}
	if !ok {
		// Without the peer's box key there is nothing to seal to. This is not a
		// refusal by the peer — it is a gap in what we know about them — so it
		// is not remembered as one.
		log.Debug().Str("peer", peer.String()).Msg("dm_control_skipped_no_box_key")
		return sendThisFrameOnly
	}
	self := domain.PeerIdentityFromWire(d.svc.identity.Address)
	sealed, err := dmcontrol.Seal(self, peer, boxKey, plain)
	if err != nil {
		log.Warn().Err(err).Str("peer", peer.String()).Msg("dm_control_seal_failed")
		return sendThisFrameOnly
	}
	builder, err := d.frameBuilder(layer)
	if err != nil {
		log.Warn().Err(err).Msg("dm_control_frame_builder_unavailable")
		return sendThisFrameOnly
	}
	built, err := builder.Build(datagram.RoutedFrameOpts{
		Dst:         peer,
		DType:       domain.DTypeDMControl,
		Class:       domain.DatagramClassControl,
		RoutePolicy: domain.RoutePolicyBest,
		Payload:     sealed,
	})
	if err != nil {
		log.Warn().Err(err).Str("peer", peer.String()).Msg("dm_control_build_failed")
		return sendThisFrameOnly
	}

	outcome := d.dispatchFrame(ctx, layer, built)
	switch outcome.kind {
	case datagram.SendQueued:
		return sendDelivered
	case datagram.SendRejected:
		if outcome.rejection == datagram.RejectionUnsupportedDType {
			// The destination declared its dtypes at handshake and dm_control
			// was not among them. That is a property of their build, and it is
			// what lets the UI stop claiming the reaction was seen.
			//
			// Recorded against the TYPE and not against frame.command. The gate
			// answers before anything is opened, so it cannot know which command
			// was inside — reading one out of it would be an inference the
			// signal does not support, and the wrong shape besides: a peer with
			// no dm_control refuses every command in it, not the one that
			// happened to be in this frame.
			d.markTypeRefused(peer)
			log.Debug().Str("peer", peer.String()).Str("command", frame.command.String()).
				Msg("dm_control_dtype_unsupported_by_peer")
			return sendPeerCannot
		}
		// Another gate — a missing capability, a policy — which is about this
		// route rather than about the peer's build.
		log.Debug().Str("peer", peer.String()).Str("reason", outcome.summary).
			Msg("dm_control_rejected")
		return sendThisFrameOnly
	default:
		// No route, or a local fault. Neither says anything about the peer's
		// build, and neither is retried here: the fact is stored and the next
		// reconciliation carries it.
		log.Debug().Str("peer", peer.String()).Str("outcome", outcome.summary).
			Err(outcome.err).Msg("dm_control_not_sent")
		return sendThisFrameOnly
	}
}

// dmControlDispatch is what one hand-over to the plane tells this file, and
// nothing more.
//
// A narrowed projection of datagram.SendOutcome rather than the outcome itself:
// the outcome's constructors are unexported, so a seam typed on it could not be
// driven from here, and the three fields below are all sendOne reads.
type dmControlDispatch struct {
	kind      datagram.SendOutcomeKind
	rejection datagram.RejectionReason
	// summary is what the log line carries; the outcome's own String() when
	// the plane produced it.
	summary string
	err     error
}

// dispatchFrame is the one call into the plane.
func (d *dmControlSender) dispatchFrame(
	ctx context.Context,
	layer *datagramLayer,
	frame protocol.DatagramFrame,
) dmControlDispatch {
	if d.dispatch != nil {
		return d.dispatch(ctx, frame)
	}
	outcome := layer.pipeline.SendLocal(ctx, datagram.LocalSendOpts{
		Frame: frame,
		Avoid: datagram.NoAvoidedNextHop(),
	})
	rejection, _ := outcome.Rejection()
	return dmControlDispatch{
		kind:      outcome.Kind(),
		rejection: rejection,
		summary:   outcome.String(),
		err:       outcome.Err(),
	}
}

// frameBuilder builds the signer once. It cannot be built in NewService because
// the layer carries the network id and is assembled later.
func (d *dmControlSender) frameBuilder(layer *datagramLayer) (*datagram.RoutedFrameBuilder, error) {
	d.builderOnce.Do(func() {
		d.builder, d.builderErr = datagram.NewRoutedFrameBuilder(datagram.RoutedFrameBuilderConfig{
			Network:    layer.network,
			LocalID:    domain.PeerIdentityFromWire(d.svc.identity.Address),
			PrivateKey: d.svc.identity.PrivateKey,
			Clock:      d.clock,
		})
	})
	return d.builder, d.builderErr
}

// dmControlSendLoop drains the outbox for as long as the node runs.
//
// # There is no last pass on shutdown, and that is not an omission
//
// One was written and removed, because it could not work. This loop and the
// outbound pump are started on the SAME context and joined by the same wait
// group (startDatagramSchedules), and stopRunLifecycle is cancel-then-wait with
// no ordering between them. ClassQueueEmitter.Run parks in a select and returns
// on ctx.Done without a final drain, so by the time a shutdown flush here could
// seal and sign a frame, the queue it hands that frame to has no reader left.
// The frames would be built, paid for in CPU, and dropped — which is worse than
// not building them, because the code would claim a guarantee it does not have.
//
// What IS true: the door closes when this loop stops, so nothing queues behind
// a pump that is gone, and the facts are already stored. Their trip to the peer
// is the thing that waits, and this transport has no retry by design — the last
// debounce window is one more divergence for reconciliation to find, exactly
// like a datagram lost in flight. Making it survive shutdown means giving the
// node an ordered teardown between the two loops, which is a change to the
// node's lifecycle rather than to this feature.
func (s *Service) dmControlSendLoop(ctx context.Context) {
	ticker := time.NewTicker(dmControlTick)
	defer ticker.Stop()
	// The door was opened by startDatagramSchedules BEFORE this goroutine was
	// scheduled — see there for why. This loop only closes it, on its way out.
	defer s.dmControl.stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dmControl.flushDue(ctx, s.dmControl.clock())
		}
	}
}

// setDraining opens or closes the door, under the mutex the door is checked
// behind.
func (d *dmControlSender) setDraining(open bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.draining = open
}

// stop closes the door and drops what was still waiting behind it.
//
// Both halves under one lock, so nothing can be queued into a batch that is
// about to be discarded and then believed sent. Dropping rather than sending is
// the subject of the loop's own comment: there is no reader left for a frame
// built here.
//
// What makes the drop survivable is that the facts are DURABLE — they are rows
// in the chatlog, and the next start re-announces the recent ones
// (DMRouter.reannounceRecentReactions). The outbox is a send queue, not the
// record.
func (d *dmControlSender) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.draining = false
	if len(d.pending) == 0 {
		return
	}
	log.Debug().Int("peers", len(d.pending)).
		Msg("dm_control_outbox_dropped_at_shutdown")
	clear(d.pending)
}
