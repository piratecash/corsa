package service

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

// reactions.go is the service half of message reactions: the decisions, and
// nothing about how they are drawn or how they are stored. The model is in
// docs/refactoring/reactions-protocol.md.
//
// The only decision this layer makes today is what a tap MEANS — set or clear,
// and under which counter value. Getting that fact to the peer attaches to
// ToggleReaction's return value, without changing what a tap means.

// reactionStore is the chatlog surface reactions go through. Named as an
// interface for the reason chatHistoryReader is: a test drives the router
// without a database, and the router must not reach past it.
type reactionStore interface {
	ApplyReactionFact(ctx context.Context, fact domain.ReactionFact, now time.Time) (bool, error)
	ReactionFacts(ctx context.Context, messageID domain.MessageID) ([]domain.ReactionFact, error)
	ReactionsForScope(ctx context.Context, scope domain.ReactionScope, self domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error)
	NextReactionClock(ctx context.Context, self domain.PeerIdentity) (domain.ReactionClock, error)
	ReleaseHeldReactions(ctx context.Context, scope domain.ReactionScope, messageID domain.MessageID, now time.Time) (int, error)
	SweepHeldReactions(ctx context.Context, now time.Time) (int, error)
	ReleaseArrivedReactions(ctx context.Context, now time.Time) ([]domain.ReactionScope, error)
	ConversationsWithReactionsBy(ctx context.Context, actor domain.PeerIdentity) ([]domain.ReactionScope, error)
	ReactionFactsByKey(ctx context.Context, scope domain.ReactionScope, actor domain.PeerIdentity, keys []domain.ReactionKey) ([]domain.ReactionFact, error)
}

// ReofferPage is how many facts one re-offer carries.
//
// A PAGE, not the whole set, and successive re-offers walk the rest — see
// ReactionsToReoffer. One page is a frame or two on the wire; a cap with no
// paging behind it would be the same "old facts are never retried" the
// time-boxed version had, wearing a different number.
const ReofferPage = 64

// The re-offer cadence. A conversation is offered at ReofferMinInterval, and
// each pass doubles the gap up to ReofferMaxInterval, where it stays.
//
// It never stops, because the requirement has no deadline: the facts are
// retried until the peer's build can take them, and a peer reached only through
// transit may never open a session with this node at all — for that pair a
// bounded round would simply end, with the facts undelivered and nothing left
// that would ever try again. Nothing on this transport reports arrival, so
// "already in sync" is not a state this side can reach; what it can do is get
// rare, which is what the doubling is for.
//
// The gap is jittered so the result is not a clock. A padded frame hides its
// contents but not its addressee, and a fixed cadence to one peer is a pattern
// an observer can lock onto; at the far end of the backoff it also spreads the
// wake-ups of many conversations instead of bunching them.
//
// PR-2's digest is what ends the traffic properly: it answers "do we already
// agree", which is the question this cannot ask. Until then the honest shape is
// a slow retry, and the slowest gap here is the price of having no answer.
const (
	ReofferMinInterval = 5 * time.Minute
	ReofferMaxInterval = 2 * time.Hour
)

// ReactionsToReoffer offers the next page of this user's own facts in one
// conversation, because a SESSION with the peer just came up.
//
// A session is new reason to try, so it takes the conversation back to the
// shortest gap: the peer may have been away, may have upgraded, may have missed
// everything sent while it was gone. The periodic pass uses reofferDue instead,
// which respects the backoff this resets.
func (a *ReactionControlAdapter) ReactionsToReoffer(
	ctx context.Context,
	peer domain.PeerIdentity,
	offer func([]domain.ReactionFact) error,
) error {
	return a.offerReoffer(ctx, peer, offer, func(scope domain.ReactionScope) bool {
		// Reset the backoff AND take the slot: the pass that follows a session
		// is a pass like any other, and it books the next one. Returning true
		// without booking left the conversation due immediately, so the very
		// next reaper tick sent the same page again — for a conversation with
		// one page, a straight duplicate a second later.
		a.RestartReoffer(scope)
		return a.claimReofferSlot(scope)
	})
}

// reofferDue is the periodic pass's entry: the next page, but only if this
// conversation's backoff says it is time.
func (a *ReactionControlAdapter) reofferDue(
	ctx context.Context,
	peer domain.PeerIdentity,
	offer func([]domain.ReactionFact) error,
) error {
	return a.offerReoffer(ctx, peer, offer, a.claimReofferSlot)
}

// offerReoffer reads the page the cursor points at and hands it to offer,
// holding the removal lease across BOTH.
//
// One step as far as a removal is concerned, and that is the whole point of the
// callback. Reading the facts and queueing them are two moments; a removal that
// starts between them finds nothing to clean, because the rows are gone from the
// database while a COPY of them is already in the node's outbox, addressed to a
// contact the user has erased.
//
// The read itself is deliberately blind: nothing on this transport reports that
// a fact ARRIVED, so nothing here knows what to skip. The page is offered again
// and the receiver's merge — one clock comparison — makes everything it already
// has free.
//
// It WRAPS, so a conversation with more facts than a page holds still offers all
// of them across successive passes. Without that, "retried until the peer
// updates" would be false for everything past the first page.
//
// Building "what has this peer seen" instead is the delivery cursor §5.2
// rejected: it answers only "what I sent you", it is a second source of truth to
// keep correct, and the digest of §6.3 answers the real question — "do we agree"
// — without it.
func (a *ReactionControlAdapter) offerReoffer(
	ctx context.Context,
	peer domain.PeerIdentity,
	offer func([]domain.ReactionFact) error,
	due func(domain.ReactionScope) bool,
) error {
	store := a.store()
	if store == nil || peer.IsZero() || offer == nil {
		return nil
	}
	releaseWrite, admitted := a.admitWrite(peer)
	if !admitted {
		// Being removed right now. Its facts are about to stop existing, so
		// there is nothing to offer and nothing to retry.
		return nil
	}
	defer releaseWrite()

	scope := domain.ReactionScopeForPeer(peer)
	self := a.chatlog.SelfAddress()

	// COUNT FIRST, and pace afterwards. Pacing creates an entry per
	// conversation, and this runs on every session — including with transit
	// peers this node has never had a conversation with, and whose identity
	// costs nothing to mint. An entry made before knowing there is anything to
	// offer is one nothing ever removes: the peer has no conversation, so no
	// deletion path names it.
	//
	// A failure from here on is a LOCAL one and it is known synchronously: the
	// database would not read, or the node had nothing running to queue onto.
	// Neither is evidence that this conversation has been offered, so neither
	// may cost it its page or widen its gap — that would skip a page until the
	// cursor came all the way round, and wait longer before trying again, on
	// the strength of an error the caller was told about.
	//
	// The transport's outcome is the opposite case and stays out of this: what
	// happens to a frame after the plane accepts it is unobservable, so progress
	// cannot be conditioned on it.
	total, err := store.CountReactionsAuthoredBy(ctx, self, scope)
	if err != nil {
		a.resetPacedReoffer(scope)
		return err
	}
	if total == 0 {
		// Nothing of ours in this conversation — and if it once had something,
		// this is where the pacing entry goes: the facts are gone and nothing
		// else would take it.
		a.ForgetConversation(scope)
		return nil
	}
	if !due(scope) {
		return nil
	}
	page := a.reofferPageAt(scope, total)
	facts, err := store.ReactionsAuthoredBy(ctx, self, scope, ReofferPage, page)
	if err != nil {
		a.resetPacedReoffer(scope)
		return err
	}
	if len(facts) == 0 {
		return nil
	}
	if err := offer(facts); err != nil {
		a.resetPacedReoffer(scope)
		return err
	}
	a.advanceReofferPage(scope, page, total)
	return nil
}

// HasConversationWith answers the node's admission question about a sender.
//
// The same question the incoming door asks before letting a fact WAIT, and for
// the same reason: a signature says who signed, not that there is anything
// between us, and identities are free. See node.ConversationControlStore.
func (a *ReactionControlAdapter) HasConversationWith(
	ctx context.Context,
	peer domain.PeerIdentity,
) (bool, error) {
	store := a.store()
	if store == nil || peer.IsZero() {
		return false, nil
	}
	return store.HasConversationWith(ctx, peer)
}

// ReactionFactsFor answers the node's send queue: what are these keys worth
// RIGHT NOW, in this conversation.
//
// The queue holds keys rather than facts, so this is where a key becomes
// something to say. A key whose row has been deleted — with its message, with
// the conversation, or by the actor clearing it away — resolves to nothing and
// is simply not sent, without anything having to tell the queue about the
// deletion. That is the whole point: every "tell the queue it is stale" path was
// a race waiting to be found, and there is now nothing to tell.
//
// Only this node's OWN facts: what a peer stated is theirs to re-offer, not
// ours, and the actor is what the receiving end verifies against the signature.
func (a *ReactionControlAdapter) ReactionFactsFor(
	ctx context.Context,
	peer domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	store := a.store()
	if store == nil || peer.IsZero() || len(keys) == 0 {
		return nil, nil
	}
	return store.ReactionFactsByKey(ctx, domain.ReactionScopeForPeer(peer), a.chatlog.SelfAddress(), keys)
}

// RestartReoffer takes one conversation back to the start of the backoff: due
// now, and at the shortest gap after that.
//
// Called when there is fresh reason to try — the local user decided something,
// or a session with the peer came up.
func (a *ReactionControlAdapter) RestartReoffer(scope domain.ReactionScope) {
	if a == nil {
		return
	}
	now := a.clock()
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	state := a.stateLocked(scope)
	state.dueAt = now
	state.interval = ReofferMinInterval
}

// resetPacedReoffer takes a conversation back to the start of the backoff, but
// only if it is already being paced.
//
// The difference from RestartReoffer is the whole point: this runs on a local
// failure, which can happen for a conversation that has nothing to offer and
// therefore no entry — and creating one there would be a row nothing ever
// removes.
func (a *ReactionControlAdapter) resetPacedReoffer(scope domain.ReactionScope) {
	if a == nil {
		return
	}
	now := a.clock()
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	state := a.reoffer[scope]
	if state == nil {
		return
	}
	state.dueAt = now
	state.interval = ReofferMinInterval
}

// ForgetConversation drops one conversation's re-offer state.
//
// Called when the contact or the conversation is removed. The map is keyed by
// scope and nothing else prunes it: the database stops returning the
// conversation, but the entry would sit here until the process ended.
func (a *ReactionControlAdapter) ForgetConversation(scope domain.ReactionScope) {
	if a == nil {
		return
	}
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	delete(a.reoffer, scope)
}

// claimReofferSlot reports whether the periodic pass may offer this conversation
// now, and books the next slot if so.
func (a *ReactionControlAdapter) claimReofferSlot(scope domain.ReactionScope) bool {
	now := a.clock()
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	state := a.stateLocked(scope)
	if state.dueAt.After(now) {
		return false
	}
	// The gap WAITED is the current interval, and the doubling is for the next
	// one — the other order would make the first gap ten minutes and never use
	// the shortest interval at all, which is the one a fresh decision needs.
	state.dueAt = now.Add(a.jitter(state.interval))
	state.interval = min(state.interval*2, ReofferMaxInterval)
	return true
}

// reofferPageAt returns where this conversation's next page starts, without
// moving the cursor. Past the end it starts over.
//
// In memory and per process: a cursor that restarts at the newest facts after a
// restart is not a defect, because every page is offered again on the next lap
// anyway.
func (a *ReactionControlAdapter) reofferPageAt(scope domain.ReactionScope, total int) int {
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	state := a.stateLocked(scope)
	if state.page >= total {
		state.page = 0
	}
	return state.page
}

// advanceReofferPage moves the cursor past a page that was offered, wrapping at
// the end.
//
// Called AFTER the offer rather than before the read, so a local failure —
// unreadable database, nothing running to queue onto — does not cost the
// conversation a page it never offered. It takes the page it is advancing from,
// so a concurrent restart that already moved the cursor is not overwritten by a
// pass that started earlier.
func (a *ReactionControlAdapter) advanceReofferPage(scope domain.ReactionScope, from, total int) {
	a.reofferMu.Lock()
	defer a.reofferMu.Unlock()
	state := a.stateLocked(scope)
	if state.page != from {
		return
	}
	if next := from + ReofferPage; next < total {
		state.page = next
		return
	}
	state.page = 0
}

// stateLocked returns one conversation's re-offer state, creating it due now and
// at the shortest gap. Caller must hold a.reofferMu.
func (a *ReactionControlAdapter) stateLocked(scope domain.ReactionScope) *reofferState {
	if a.reoffer == nil {
		a.reoffer = map[domain.ReactionScope]*reofferState{}
	}
	state := a.reoffer[scope]
	if state == nil {
		state = &reofferState{
			dueAt:    a.clock(),
			interval: ReofferMinInterval,
		}
		a.reoffer[scope] = state
	}
	return state
}

// ErrNoReactionStore is returned when this node keeps no chat history: there is
// nowhere for a reaction to live, and pretending otherwise would show the user
// a chip that vanishes on the next redraw.
var ErrNoReactionStore = fmt.Errorf("service: reactions need a chatlog")

// reactions resolves the store, or nil on a node without persistence.
func (r *DMRouter) reactions() reactionStore {
	if r == nil || r.client == nil || r.client.chatlog == nil {
		return nil
	}
	store := r.client.chatlog.Store()
	if store == nil {
		return nil
	}
	return store
}

// reactionControl is the pager the periodic re-offer reads, or nil on a runtime
// that never wired the conversation-control door.
func (r *DMRouter) reactionControl() *ReactionControlAdapter {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.reactionControl
}

// MessageReactions is every reaction in one conversation, keyed by message.
//
// One call per conversation rather than one per message: the caller draws
// dozens of bubbles per frame and must not read a database inside a frame.
func (r *DMRouter) MessageReactions(ctx context.Context, peer domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error) {
	store := r.reactions()
	if store == nil {
		return nil, ErrNoReactionStore
	}
	return store.ReactionsForScope(ctx, domain.ReactionScopeForPeer(peer), r.MyAddress())
}

// ToggleReaction flips the local user's reaction on one message, stores the
// decision and hands it to the peer.
//
// The order matters and is not an implementation detail: the fact is written
// first and only then queued for sending. A fact that went out but was not
// stored is one this node can neither show nor reconcile, while one that was
// stored but not sent is simply a divergence — which is the case the whole
// model is built to survive.
//
// Queueing a fact the peer cannot use is not an error the caller sees. Whether
// the peer's build understands reactions is answered by ReactionsUnsupportedBy,
// separately and later, because the send is deliberately asynchronous.
func (r *DMRouter) ToggleReaction(ctx context.Context, peer domain.PeerIdentity, messageID domain.MessageID, emoji string, now time.Time) (domain.ReactionFact, error) {
	store := r.reactions()
	if store == nil {
		return domain.ReactionFact{}, ErrNoReactionStore
	}
	// The same lease the INCOMING door takes, and for the same reason: a fact
	// written while this conversation is being wiped lands behind the wipe as a
	// row no deletion path can reach afterwards. The local user's own tap is not
	// exempt from that — it writes the same table.
	if r.removals != nil {
		releaseWrite, admitted := r.removals.admitWrite(peer)
		if !admitted {
			return domain.ReactionFact{}, ErrConversationDeleteInflight
		}
		defer releaseWrite()
	}
	fact, err := toggleReactionWith(ctx, store, r.MyAddress(), peer, messageID, emoji, now)
	if err != nil {
		return domain.ReactionFact{}, err
	}
	// A local decision is fresh reason to keep trying, so the conversation's
	// backoff starts over: the peer has something it demonstrably does not have
	// yet, which is the one thing this side can know without an acknowledgement.
	if control := r.reactionControl(); control != nil {
		control.RestartReoffer(domain.ReactionScopeForPeer(peer))
	}
	if r.client != nil {
		if err := r.client.SendReactionFacts(peer, []domain.ReactionFact{fact}); err != nil {
			// The decision is stored and on screen; only its trip to the peer
			// failed to start. Reported as a log line rather than to the caller,
			// who would have nothing to do with it — the fact is not lost and
			// reconciliation carries it.
			log.Warn().Err(err).
				Str("peer", peer.String()).
				Str("message", string(messageID)).
				Msg("reaction stored but not queued for the peer")
		}
	}
	return fact, nil
}

// ReactionsUnsupportedBy reports whether the peer is known to run a build that
// cannot receive reactions.
//
// "Known to be", not "not known to be": a peer nothing has been sent to yet
// answers false, and the honest state then is that the reaction is on its way.
func (r *DMRouter) ReactionsUnsupportedBy(peer domain.PeerIdentity) bool {
	if r == nil || r.client == nil {
		return false
	}
	return r.client.ReactionsUnsupportedBy(peer)
}

// toggleReactionWith is the decision itself, apart from the router that carries
// it. What a tap MEANS is the only thing this layer decides, and it is worth
// being able to check that without a database or a node behind it.
//
// Flipping rather than setting is what the surface offers — one tap on a chip
// or a slot means "the opposite of what I have now" — and reading the current
// value here rather than trusting a caller's copy is what keeps two taps in
// quick succession from both deciding "set": the caller's copy is a frame old.
//
// Only this actor's own facts are consulted. Somebody else's reaction with the
// same emoji is not ours to clear, and their counter is not ours to continue.
func toggleReactionWith(
	ctx context.Context,
	store reactionStore,
	self, peer domain.PeerIdentity,
	messageID domain.MessageID,
	emoji string,
	now time.Time,
) (domain.ReactionFact, error) {
	if self.IsZero() {
		return domain.ReactionFact{}, fmt.Errorf("service: reaction needs a local identity")
	}
	if emoji == "" {
		return domain.ReactionFact{}, fmt.Errorf("service: reaction needs an emoji")
	}

	held, err := store.ReactionFacts(ctx, messageID)
	if err != nil {
		return domain.ReactionFact{}, err
	}
	op := domain.ReactionSet
	var highest domain.ReactionClock
	for _, fact := range held {
		if fact.Key.Actor != self || fact.Key.Emoji != emoji {
			continue
		}
		// The store returns one row per key, but a fake or a future join could
		// return several; the newest is the one that says what we hold now.
		if fact.Clock >= highest {
			highest = fact.Clock
			op = domain.ReactionCleared
			if fact.Op == domain.ReactionCleared {
				op = domain.ReactionSet
			}
		}
	}

	clock, err := store.NextReactionClock(ctx, self)
	if err != nil {
		return domain.ReactionFact{}, err
	}
	fact := domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(peer),
		Key:   domain.ReactionKey{MessageID: messageID, Actor: self, Emoji: emoji},
		Op:    op,
		Clock: clock,
	}
	applied, err := store.ApplyReactionFact(ctx, fact, now)
	if err != nil {
		return domain.ReactionFact{}, err
	}
	if !applied {
		// The write did not take. Two causes, and the store deliberately does
		// not tell them apart: the decision was superseded by one made
		// concurrently on the same key (reading, counting and writing are three
		// statements with no transaction around them), or it would have crossed
		// one of the storage ceilings.
		//
		// Reported rather than swallowed, because the caller would otherwise be
		// handed a fact that describes state this node does not hold and would
		// send it to the peer, leaving the two sides disagreeing with nothing
		// to notice it. The user's tap is lost either way; saying so is what
		// lets the surface offer it again.
		return domain.ReactionFact{}, fmt.Errorf(
			"service: the reaction on %s was not stored", messageID)
	}
	return fact, nil
}

// compile-time proof that the real store satisfies the surface above. Without
// it the interface and the store drift apart until a caller wires them.
var _ reactionStore = (*chatlog.Store)(nil)
