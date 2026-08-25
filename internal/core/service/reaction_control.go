package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// reaction_control.go is the receiving end of the conversation-control plane:
// the node hands over facts a peer stated, and this decides where they go.
//
// It is the mirror of MessageStoreAdapter — the node owns no conversation
// state, so everything it accepts arrives here — and it is a separate type for
// the same reason that one is: one door, one responsibility.

// errDeletionUnreadable marks the one per-fact outcome the batch keeps going
// past: whether the message was deleted here cannot be answered right now.
var errDeletionUnreadable = errors.New("service: the refusals of deleted ids are unreadable")

// ReactionControlAdapter satisfies node.ConversationControlStore.
type ReactionControlAdapter struct {
	chatlog *ChatlogGateway
	events  *ebus.Bus
	// refusals names ids deleted here recently enough that copies can still be
	// in flight. It is the SAME set MessageStoreAdapter guards the message door
	// with, and for the same reason: a reaction naming a deleted id is metadata
	// about a message the user destroyed, and storing it rebuilds exactly what
	// the deletion existed to remove. nil on a runtime with no deletion
	// subsystem (SDK consumers).
	refusals *wipeTombstoneSet
	// removals names the conversations being removed right now, and this door
	// takes a lease on one exactly as MessageStoreAdapter does.
	//
	// Checking would not be enough: between deciding the message is here and
	// writing the fact, a whole wipe can start and finish, and the fact lands
	// behind it as an APPLIED row naming a message that no longer exists. That
	// row is then unreachable by every deletion path — the per-message delete
	// needs a message, the wipe runs by scope but has already run, and the sweep
	// only takes held rows. It is permanent metadata about a conversation the
	// user erased.
	removals *removalGate
	clock    func() time.Time

	// reoffer is the per-conversation re-offer state: which page comes next and
	// when the periodic pass may take it. Its own mutex, because it is touched
	// from the datagram ingress goroutine (a session's re-offer) and from the
	// router's reaper loop (the periodic one), and it belongs to neither.
	reofferMu sync.Mutex
	reoffer   map[domain.ReactionScope]*reofferState
	// jitter spreads one gap so the cadence is not a clock. Injected for the
	// same reason the sender's is: a test that has to guess a random gap can
	// only assert something weaker than the rule.
	jitter func(time.Duration) time.Duration
}

// reofferJitter is the default spread: the gap lands anywhere in ±25% of the
// interval, so neither an observer nor a bunch of conversations waking together
// sees a fixed rhythm.
func reofferJitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}
	spread := int64(interval / 2)
	return interval - interval/4 + time.Duration(rand.Int64N(spread))
}

// reofferState paces one conversation's re-offers.
//
// Backed off rather than periodic at a fixed rate, and the reason is that
// nothing here can learn "the peer has it". Without that knowledge the honest
// shape is an ordinary retry: often at first, rarer as it goes, and back to
// often whenever there is new reason to try — a local decision, or the peer
// turning up on a session.
//
// It does NOT stop. A bounded round was the first cut and it was wrong: a peer
// reached only through transit may never open a session with this node, so for
// that pair the end of the round is simply the end of delivery.
type reofferState struct {
	// page is where the next read starts, wrapping at the end.
	page int
	// dueAt is when the periodic pass may take this conversation again.
	dueAt time.Time
	// interval is the current gap, doubling each pass up to ReofferMaxInterval.
	interval time.Duration
}

// NewReactionControlAdapter binds the chatlog, the two deletion gates and the
// event bus to the surface the node calls. The clock is injected because
// whether a fact is applied, held or dropped is a decision, and its timestamp
// is part of the row it writes.
func NewReactionControlAdapter(
	gateway *ChatlogGateway,
	refusals *wipeTombstoneSet,
	removals *removalGate,
	events *ebus.Bus,
	clock func() time.Time,
) *ReactionControlAdapter {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &ReactionControlAdapter{
		chatlog:  gateway,
		refusals: refusals,
		removals: removals,
		events:   events,
		clock:    clock,
		reoffer:  map[domain.ReactionScope]*reofferState{},
		jitter:   reofferJitter,
	}
}

// ApplyReactionFacts merges what a peer said about a conversation.
//
// Each fact's fate is decided on its own, not the batch's: a batch names
// several messages, and a peer may know about one this node has, one it has
// not, and one it deleted.
//
//   - the id was deleted here → DROPPED. A reaction is metadata about a message
//     ("who responded to what"), and storing it rebuilds exactly what the
//     deletion existed to destroy;
//   - the message is here → applied, and shows immediately;
//   - neither, and we HAVE a conversation with the sender → HELD. A reaction can
//     overtake the message it is about, nothing orders the two, and the sender
//     has no reason to repeat a fact it believes delivered;
//   - neither, and we have never exchanged a message with the sender → DROPPED.
//     Holding is the only unbounded thing here, and the ceilings that bound it
//     are per identity, which costs nothing to mint (§9.5).
//
// Partial success is its own path and not a variant of failure. Facts written
// before an error are committed — there is no transaction spanning the batch —
// so the UI is told about them and the error is returned afterwards. Reporting
// the failure alone would leave chips in the database that nothing draws until
// the user leaves the conversation and comes back.
//
// Idempotent, as the node's contract requires: the merge underneath is one
// clock comparison, so the same batch twice is the same state.
func (a *ReactionControlAdapter) ApplyReactionFacts(
	ctx context.Context,
	sender domain.PeerIdentity,
	facts []domain.ReactionFact,
) error {
	store := a.store()
	if store == nil {
		return fmt.Errorf("service: reactions arrived on a node with no chatlog")
	}
	if sender.IsZero() {
		// Unreachable through the node, which resolves the conversation from a
		// signed source before calling. Checked because this is the door, and
		// because a zero identity does not merely fail downstream — it changes
		// the QUESTION: chatlog.peerQuery falls back to the global topic for
		// one, so "do we have a conversation" would be answered about something
		// else entirely.
		return fmt.Errorf("service: reactions arrived with no sender to attribute them to")
	}
	// One lease for the whole batch, held until the last row is written, so a
	// wipe of this conversation either sees these writes and waits for them, or
	// starts after them and sweeps what they left.
	releaseWrite, admitted := a.admitWrite(sender)
	if !admitted {
		// DROPPED, not deferred: the conversation is being erased, so these
		// facts are about to have nothing to be about. There is no sender-side
		// retry to hand them back to, and holding them would put them back into
		// the conversation the wipe is removing.
		log.Debug().Str("peer", sender.String()).Int("facts", len(facts)).
			Msg("reactions dropped: their conversation is being removed")
		return nil
	}
	defer releaseWrite()

	// Asked once for the batch, not per fact: it is a property of the sender,
	// and the answer cannot change under a lease we already hold.
	holdable, err := store.HasConversationWith(ctx, sender)
	if err != nil {
		return err
	}
	if !holdable {
		// And the whole batch is over here, not fact by fact. No conversation
		// means no message of theirs is in this node at all, so every fact would
		// be looked up and then dropped — one SQLite read per fact, which is a
		// stranger multiplying our work by the size of a batch they choose. The
		// lease is held while we say so, which is what makes "no conversation"
		// still true when we act on it.
		log.Debug().Str("peer", sender.String()).Int("facts", len(facts)).
			Msg("reactions from a peer with no conversation here are dropped")
		return nil
	}

	now := a.clock()
	changed := false
	unreadable := 0
	var failure error
	for _, fact := range facts {
		if fact.Key.Actor != sender {
			// Unreachable through the node, which builds every fact from the
			// signed source. Checked anyway because this is the door: a fact
			// stored under the wrong actor is a reaction attributed to someone
			// who never made it, and nothing downstream would notice.
			failure = fmt.Errorf("service: fact for %s attributed to %s, not to its sender %s",
				fact.Key.MessageID, fact.Key.Actor, sender)
			break
		}
		applied, err := a.writeOne(ctx, store, sender, fact, now, holdable)
		changed = changed || applied
		if errors.Is(err, errDeletionUnreadable) {
			// Counted and reported once for the batch: the condition is
			// store-wide, so one line per fact is up to a frame's worth of
			// identical warnings for a single cause.
			unreadable++
			continue
		}
		if err != nil {
			failure = err
			break
		}
	}
	if unreadable > 0 {
		log.Warn().Str("peer", sender.String()).Int("reactions", unreadable).
			Msg("reactions were dropped: the refusals of deleted ids are unreadable")
	}
	if changed {
		a.publishChange(sender)
	}
	return failure
}

// writeOne stores one fact and reports whether anything visible changed.
func (a *ReactionControlAdapter) writeOne(
	ctx context.Context,
	store *chatlog.Store,
	sender domain.PeerIdentity,
	fact domain.ReactionFact,
	now time.Time,
	holdable bool,
) (bool, error) {
	refused, known := a.refusesDeletedID(fact.Key.MessageID, now)
	switch {
	case refused:
		// The tombstone that answered here expires on the MESSAGE clock — a
		// week, the horizon past which no copy of the envelope is re-sent —
		// while the peer offering this fact has no horizon at all. Recording
		// the id durably is what keeps the answer after the tombstone goes;
		// without it the offer after that is taken as a fact waiting for its
		// message, swept an hour later, and offered again, for ever.
		//
		// This is the only moment the two facts are in one place: a reaction
		// made AFTER the deletion left nothing at delete time to notice.
		if err := store.RefuseReactionsFor(ctx, domain.ReactionScopeForPeer(sender), fact.Key.MessageID, now); err != nil {
			// Logged rather than returned, and the fact is still dropped. The
			// user's deletion holds either way; what a failure costs is the
			// refusal surviving this tombstone, and failing the batch would
			// not buy that back.
			log.Warn().Err(err).
				Str("peer", sender.String()).
				Str("id", string(fact.Key.MessageID)).
				Msg("a deleted id could not be recorded as refusing reactions; its offers can return once the tombstone expires")
		}
		log.Debug().
			Str("peer", sender.String()).
			Str("id", string(fact.Key.MessageID)).
			Msg("a reaction naming a deleted message was dropped")
		return false, nil
	case !known:
		// The refusals are unreadable, so whether this id was deleted here
		// cannot be answered. DROPPED, and the batch continues — the caller
		// counts these and reports them once.
		//
		// Dropped rather than deferred because there is nothing to defer TO:
		// this subsystem has no retry and no queue by design, and the sender
		// will not repeat a fact it believes delivered. So the honest choice is
		// between losing one reaction and risking the return of metadata the
		// user destroyed — a later reload of the refusals does not re-delete a
		// row that came back — and the reaction is much the cheaper of the two.
		return false, errDeletionUnreadable
	}

	// LookupEntryInConversation, not HasEntryInConversation: the latter answers
	// `err == nil && found`, so a transient database error becomes "the message
	// is not here" — and the fact is then HELD, occupies the actor's quota, and
	// is swept an hour later having never been shown. This is a decision point,
	// and a decision made on a swallowed error is the wrong decision silently.
	present, err := store.LookupEntryInConversation(ctx, sender, fact.Key.MessageID)
	if err != nil {
		return false, fmt.Errorf(
			"service: cannot tell whether %s is in this conversation: %w", fact.Key.MessageID, err)
	}
	if !present {
		if !holdable {
			// A fact about a message we do not have, from someone we have never
			// exchanged one with.
			// Holding it is what the per-actor ceilings
			// bound — and an identity costs nothing to mint, so on their own
			// they bound "as many identities as the attacker cares to make,
			// times the ceiling". Requiring a conversation makes that number
			// the people the user actually talks to.
			return false, nil
		}
		// The message is not here, so the question "was it deleted here" is
		// worth a read — and this is the read the tombstone's expiry used to
		// leave unanswered. Asked here rather than at the top of the function
		// so a conversation whose messages are all present pays nothing for it.
		forGood, err := store.ReactionsRefusedFor(ctx, domain.ReactionScopeForPeer(sender), fact.Key.MessageID, now)
		if err != nil {
			return false, fmt.Errorf(
				"service: cannot tell whether %s was deleted here: %w", fact.Key.MessageID, err)
		}
		if forGood {
			log.Debug().
				Str("peer", sender.String()).
				Str("id", string(fact.Key.MessageID)).
				Msg("a reaction naming a message deleted here long ago was dropped")
			return false, nil
		}
		// Held facts change nothing the user can see, so this deliberately does
		// not count as a change: waking the UI for a row it cannot draw would
		// be a redraw per arriving reaction on a message that may never come.
		held, err := store.HoldReactionFact(ctx, fact, now)
		if err != nil {
			return false, err
		}
		if !held {
			// Refused by a ceiling, or superseded. Nothing to release, and
			// nothing to look again for.
			return false, nil
		}
		// Then look again. The message can land — and be released — in the
		// window between the lookup above and this write, on the other
		// goroutine; the release names rows that exist, so it would not see
		// this one, and the fact would stay pending until the sweep took it an
		// hour later. Releasing here closes that window: the second lookup is
		// after the row exists, so either it saw the message or the release did.
		landed, lookupErr := store.LookupEntryInConversation(ctx, sender, fact.Key.MessageID)
		if lookupErr != nil || !landed {
			return false, lookupErr
		}
		released, releaseErr := store.ReleaseHeldReactions(
			ctx, domain.ReactionScopeForPeer(sender), fact.Key.MessageID, now)
		return released > 0, releaseErr
	}
	return store.ApplyReactionFact(ctx, fact, now)
}

// admitWrite takes a lease on writing this conversation, held until the batch
// is committed. It reports false when a removal of that conversation is running.
func (a *ReactionControlAdapter) admitWrite(peer domain.PeerIdentity) (func(), bool) {
	if a.removals == nil || peer.IsZero() {
		return func() {}, true
	}
	return a.removals.admitWrite(peer)
}

// refusesDeletedID asks the deletion gate about one message id.
//
// A runtime with no deletion subsystem answers "not refused, and that is
// known": there is nothing that could have deleted a message, so there is
// nothing to protect.
func (a *ReactionControlAdapter) refusesDeletedID(id domain.MessageID, now time.Time) (refused, known bool) {
	if a.refusals == nil {
		return false, true
	}
	return a.refusals.Refuses(id, now)
}

// publishChange tells the UI to reload one conversation.
func (a *ReactionControlAdapter) publishChange(peer domain.PeerIdentity) {
	// Published after the write, never before it: a subscriber reloads the
	// conversation from the database, so an event ahead of the commit would
	// send it to read the state it is being told about and find the old one.
	a.events.Publish(ebus.TopicReactionsChanged, peer)
}

func (a *ReactionControlAdapter) store() *chatlog.Store {
	if a == nil || a.chatlog == nil {
		return nil
	}
	return a.chatlog.Store()
}
