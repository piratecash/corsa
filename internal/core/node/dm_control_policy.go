package node

import (
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// dm_control_policy.go is what this node BELIEVES about a peer's build, and the
// answers it owes a peer whose commands it cannot read.
//
// Separate from the queue (dm_control_outbox.go) because they answer different
// questions and age differently: the queue is "what is left to say", these are
// "what is the point of saying it". They share the sender's one mutex — a pass
// reads both, and the two must not be able to disagree mid-pass.

// refusalKey names one peer's inability to handle ONE command.
//
// Keyed by both, not by peer alone. dm_control carries several commands by
// design, so a peer that refuses `message_delete` says nothing about whether it
// handles reactions — and a peer-only key would make the UI tell the user their
// reaction went nowhere on the strength of an unrelated refusal.
type refusalKey struct {
	peer    domain.PeerIdentity
	command domain.DMControlCommand
}

// ReactionsUnsupportedBy reports whether this peer is known to run a build that
// cannot receive conversation-control commands.
//
// This is the answer the UI needs to stop promising that a reaction was seen.
// It is deliberately a "known to be", not a "not known to be": a peer we have
// never sent to answers false, and the honest state then is simply "sent".
func (s *Service) ReactionsUnsupportedBy(peer domain.PeerIdentity) bool {
	if s == nil || s.dmControl == nil {
		return false
	}
	return s.dmControl.cannotTakeReactions(peer)
}

// answerCommandUnsupported queues the refusal of a command this build does not
// know. It goes through the same outbox as a reaction so it inherits the same
// delay and the same padded size.
func (s *Service) answerCommandUnsupported(peer domain.PeerIdentity, refused domain.DMControlCommand) {
	if s == nil || s.dmControl == nil || peer.IsZero() || refused == "" {
		return
	}
	s.dmControl.mu.Lock()
	defer s.dmControl.mu.Unlock()
	if !s.dmControl.canSendLocked() {
		return
	}
	if s.dmControl.forgottenRecentlyLocked(peer) {
		// The conversation was thrown away while this command was on its way in.
		// Queueing the answer now would rebuild the very queue the removal
		// emptied, from a frame that was already in flight when it ran — the
		// same window markRefused closes on the other kind of answer.
		log.Debug().Str("peer", peer.String()).Str("command", refused.String()).
			Msg("dm_control_answer_not_queued_for_a_forgotten_conversation")
		return
	}
	// The duplicate and the cap are decided BEFORE the outbox is armed: a repeat
	// of a command already answered adds nothing, and moving a batch's deadline
	// for it would let a peer nudge the queue by resending.
	if outbox := s.dmControl.pending[peer]; outbox != nil {
		for _, already := range outbox.refusals {
			if already.command == refused {
				return
			}
		}
		if len(outbox.refusals) >= maxQueuedRefusalsPerPeer {
			// A peer inventing command names is not owed an answer for each of
			// them: past the cap the answers stop, and the ones already queued
			// still go out.
			log.Debug().Str("peer", peer.String()).Str("command", refused.String()).
				Msg("dm_control_refusal_not_queued_over_cap")
			return
		}
	}
	now := s.dmControl.clock()
	outbox := s.dmControl.outboxLocked(peer)
	outbox.refusals = append(outbox.refusals, dmControlAnswer{command: refused, queuedAt: now})
}

// noteSpokeTo records that a frame of ours went to this peer.
//
// It is the other half of the admission question for what a peer may make this
// node keep: an `unsupported` ANSWERS something we sent, and after a thread wipe
// the conversation has no messages while the contact — and the reaction we sent
// them a moment before — are both still real.
func (d *dmControlSender) noteSpokeTo(peer domain.PeerIdentity) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.forgottenRecentlyLocked(peer) {
		// The conversation was thrown away while this frame was being sealed and
		// handed over. Recording the admission now would write it back into a
		// map the removal has just cleared — and waiting for the frame does not
		// prevent that: the removal waits on a condition variable, which
		// releases the mutex, so a write between its clearing and its return
		// lands anyway. The guard is what closes it, not the ordering.
		return
	}
	d.sentAt[peer] = d.clock()
}

// spokeToRecently reports whether an answer from this peer can be about
// something we sent, within the window their retries can span.
func (d *dmControlSender) spokeToRecently(peer domain.PeerIdentity) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	at, ok := d.sentAt[peer]
	return ok && d.clock().Sub(at) < dmControlForgetGrace
}

// commandsWeSend is the closed set of commands this build ever puts on the
// wire, and therefore the only ones a refusal can be worth remembering about.
//
// It is what bounds `refusedAt`. The command name in an incoming refusal is
// written by the PEER, checked only for length and alphabet, so remembering
// every name it invents would be an hour-long map with a remote pen in it —
// the ban-map shape again. A refusal of something we never send changes no
// decision here, so it is dropped rather than stored.
var commandsWeSend = map[domain.DMControlCommand]struct{}{
	domain.DMControlReactions:   {},
	domain.DMControlUnsupported: {},
}

// noteCommandRefused records that the peer could not read something we sent.
//
// An empty command name is ignored rather than recorded against everything: a
// refusal that does not say what it refused proves nothing, and treating it as
// a blanket "cannot do dm_control" would silence a peer over a malformed frame.
func (s *Service) noteCommandRefused(peer domain.PeerIdentity, refused domain.DMControlCommand) {
	if s == nil || s.dmControl == nil || peer.IsZero() || refused == "" {
		return
	}
	if _, ours := commandsWeSend[refused]; !ours {
		log.Debug().Str("peer", peer.String()).Str("command", refused.String()).
			Msg("dm_control_refusal_ignored_for_a_command_we_never_send")
		return
	}
	log.Debug().Str("peer", peer.String()).Str("command", refused.String()).
		Msg("dm_control_command_refused_by_peer")
	s.dmControl.markRefused(peer, refused)
}

// forgetDMControlRefusal clears what we believe about a peer's build.
//
// Called when a session with the peer is established: the declared dtype set is
// fixed for a session's lifetime, so a new session is the only moment at which
// the answer can have changed, and it is exactly when a peer that updated
// becomes reachable again. Every command is cleared, because a build that
// changed may have changed about any of them.
func (s *Service) forgetDMControlRefusal(peer domain.PeerIdentity) {
	if s == nil || s.dmControl == nil || peer.IsZero() {
		return
	}
	d := s.dmControl
	d.mu.Lock()
	// What matters is whether an ENTRY was here, not whether it was still
	// fresh. An entry past its TTL that the sweep has not reached yet is one the
	// UI has not been told about either — it is still drawing the notice — so
	// removing it here is the same news, and asking blockedLocked instead would
	// answer "not blocked" and say nothing at all.
	had := false
	for key := range d.refusedAt {
		if key.peer == peer {
			delete(d.refusedAt, key)
			if key.command == domain.DMControlReactions {
				had = true
			}
		}
	}
	if _, held := d.refusedTypeAt[peer]; held {
		delete(d.refusedTypeAt, peer)
		had = true
	}
	// And what was waiting ON THAT BELIEF becomes due now. This is the half the
	// clearing exists for: a peer that upgrades has to receive what was made
	// while it was old, and a batch sitting on a 30-second retry deadline would
	// otherwise keep waiting on an answer that has already changed.
	//
	// Only when a belief was actually cleared, and only for a batch the belief
	// was holding back. This runs on EVERY session, incoming and outgoing, and
	// pulling an ordinary debounced batch forward would send a reaction the
	// moment a peer reconnects — which is exactly the timing the debounce and
	// its jitter exist to hide.
	if outbox := d.pending[peer]; had && outbox != nil && !outbox.debounced {
		outbox.dueAt = d.clock()
	}
	d.mu.Unlock()

	// The UI is told, on the same topic and for the same reason the LEARNING is
	// announced: it drew a notice saying this peer cannot receive reactions, and
	// that notice has just become false. Without this it stands until something
	// else happens to reload the conversation.
	d.announceRefusalChanged(peer, had)
}

// refuses reports an INNER refusal: the peer answered `unsupported` naming this
// command.
func (d *dmControlSender) refuses(peer domain.PeerIdentity, command domain.DMControlCommand) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.refusesLocked(peer, command)
}

func (d *dmControlSender) refusesLocked(peer domain.PeerIdentity, command domain.DMControlCommand) bool {
	at, ok := d.refusedAt[refusalKey{peer: peer, command: command}]
	// Freshness is CHECKED here and the entry is removed only by the sweep
	// (takeDue). A read that deletes is what made the expiry silent: the sweep
	// is what tells the UI a peer can receive reactions again, and an entry a
	// read had already thrown away is one the sweep never sees.
	return ok && d.clock().Sub(at) < dmControlUnsupportedTTL
}

// refusesType reports an OUTER refusal: the transport's gate said the peer does
// not declare the dm_control dtype, so no command in it can arrive.
func (d *dmControlSender) refusesTypeLocked(peer domain.PeerIdentity) bool {
	at, ok := d.refusedTypeAt[peer]
	// Checked, not swept — see refusesLocked.
	return ok && d.clock().Sub(at) < dmControlUnsupportedTTL
}

// cannotTakeReactions is the union the UI asks about: either the peer's build
// has no dm_control at all, or it has one that does not know reactions.
func (d *dmControlSender) cannotTakeReactions(peer domain.PeerIdentity) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.refusesTypeLocked(peer) || d.refusesLocked(peer, domain.DMControlReactions)
}

func (d *dmControlSender) markRefused(peer domain.PeerIdentity, command domain.DMControlCommand) {
	d.mu.Lock()
	if d.forgottenRecentlyLocked(peer) {
		// The answer is to a frame from before the user removed this
		// conversation. Recording it would put the belief back that the removal
		// cleared, and then silence reactions for an hour if they add the
		// contact again.
		log.Debug().Str("peer", peer.String()).Str("command", command.String()).
			Msg("dm_control_refusal_ignored_for_a_forgotten_conversation")
		d.mu.Unlock()
		return
	}
	before := d.blockedLocked(peer)
	d.refusedAt[refusalKey{peer: peer, command: command}] = d.clock()
	learned := !before && d.blockedLocked(peer)
	d.mu.Unlock()

	d.announceRefusal(peer, learned)
}

// blockedLocked is the answer ReactionsUnsupportedBy gives, read under a lock
// the caller already holds. Caller must hold d.mu.
func (d *dmControlSender) blockedLocked(peer domain.PeerIdentity) bool {
	return d.refusesTypeLocked(peer) || d.refusesLocked(peer, domain.DMControlReactions)
}

// announceRefusal tells the UI that this peer has just turned out to be unable
// to receive reactions.
//
// Without it the news arrives one tap late. A tap queues the fact and asks
// ReactionsUnsupportedBy immediately, but the refusal is learned a second or so
// later, when the debounced frame is actually sent — so the FIRST reaction to an
// old client always looks delivered, and the user is only told when they react
// again.
//
// It reuses TopicReactionsChanged, which means "reload that conversation": what
// the reader recomputes from it includes this answer, and a second topic saying
// nearly the same thing is a second thing to keep in step.
//
// Published OUTSIDE d.mu, like every other effect here — a subscriber runs on
// the bus's goroutine and may come back into this node.
func (d *dmControlSender) announceRefusal(peer domain.PeerIdentity, learned bool) {
	if !learned || d.svc == nil || d.svc.eventBus == nil {
		return
	}
	log.Debug().Str("peer", peer.String()).
		Msg("dm_control_peer_cannot_receive_reactions_announced")
	d.svc.eventBus.Publish(ebus.TopicReactionsChanged, peer)
}

// announceRefusalChanged tells the UI when a peer stops being one that cannot
// receive reactions.
//
// Called with whether anything was actually removed, and publishes only then:
// the clearing runs on every session and on every reaction received, and a peer
// nothing was believed about is not news.
//
// Published OUTSIDE d.mu, like every other effect here.
func (d *dmControlSender) announceRefusalChanged(peer domain.PeerIdentity, changed bool) {
	if !changed || d.svc == nil || d.svc.eventBus == nil {
		return
	}
	log.Debug().Str("peer", peer.String()).
		Msg("dm_control_peer_can_receive_reactions_again_announced")
	d.svc.eventBus.Publish(ebus.TopicReactionsChanged, peer)
}

// markTypeRefused records the transport gate's answer, which is about the dtype
// and says nothing about which command was inside.
func (d *dmControlSender) markTypeRefused(peer domain.PeerIdentity) {
	d.mu.Lock()
	if d.forgottenRecentlyLocked(peer) {
		// About a frame sent before the removal — see markRefused.
		log.Debug().Str("peer", peer.String()).
			Msg("dm_control_type_refusal_ignored_for_a_forgotten_conversation")
		d.mu.Unlock()
		return
	}
	before := d.blockedLocked(peer)
	d.refusedTypeAt[peer] = d.clock()
	learned := !before
	d.mu.Unlock()

	d.announceRefusal(peer, learned)
}
