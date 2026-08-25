package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/dmcontrol"
	"github.com/piratecash/corsa/internal/core/domain"
)

// dm_control.go is the receiving half of the conversation-control datagram
// type: registration, and what happens when one arrives. Sending is in
// dm_control_send.go, the payload is internal/core/dmcontrol, and the model is
// docs/refactoring/reactions-protocol.md.

// ConversationControlStore is where an accepted control command lands. The node
// does not own conversation state — the desktop layer does — so this is the
// same shape of seam as MessageStore: registered before Run, absent on a
// relay-only node, and the only way the plane can reach a database.
type ConversationControlStore interface {
	// HasConversationWith reports whether this node exchanges messages with the
	// peer at all.
	//
	// It is the admission question for everything a STRANGER can make this node
	// keep or send. A signature proves who signed; it does not prove there is
	// anything between us, and an identity costs nothing to mint — so without
	// this, one relayed stream mints identities and each one gets a queue entry
	// (an answer this node then retries for half an hour) and an hour-long note
	// about its build. Bounded by the number of people the user actually talks
	// to, which is the same bound §9.5 puts on held reactions and for the same
	// reason.
	HasConversationWith(ctx context.Context, peer domain.PeerIdentity) (bool, error)

	// ApplyReactionFacts merges facts stated by sender. It must be idempotent:
	// the datagram layer promises zero or more deliveries, so the same batch
	// can arrive twice and must leave the state where it already was.
	ApplyReactionFacts(ctx context.Context, sender domain.PeerIdentity, facts []domain.ReactionFact) error

	// ReactionsToReoffer reads this node's OWN reaction facts in one
	// conversation — newest first and bounded — and hands them to offer.
	//
	// A CALLBACK rather than a return value, because reading them and queueing
	// them must be one step as far as a conversation removal is concerned. The
	// store holds its removal lease across the call, so a wipe either waits for
	// the offer or starts after it; with two steps, a removal landing between
	// them cleans a queue the second step then refills with facts about
	// messages it has just erased.
	//
	// offer runs on the caller's goroutine. It must not block on anything slow:
	// a removal of that conversation is waiting behind it.
	//
	// It exists because nothing on this transport can tell us a fact ARRIVED.
	// SendQueued means the local write queue accepted the frame; the writer may
	// still drop it, a transit hop with no route drops it silently by contract,
	// and a last hop that does not declare the dtype drops it without telling
	// the originator. So "delivered" is not a state this node can hold, and a
	// retry keyed on local outcomes only ever covers faults before the first
	// enqueue.
	//
	// What CAN be done is re-offering from the durable record whenever a session
	// with the peer comes up. Re-offering is free of consequence — the merge is
	// one clock comparison, so a fact the peer already has changes nothing — and
	// it is the same shape as the digest reconciliation of §6.3, minus the
	// digest that would let us skip the ones already agreed on.
	// ReactionFactsFor resolves queued KEYS into the facts they are worth right
	// now, in one conversation, dropping the ones that no longer exist.
	//
	// The outbox holds keys and not facts, and this is what makes that work. A
	// queue of copies has to be told about every deletion — of the message, of
	// the reaction, of the conversation — and each of those notifications is a
	// race between "the copy was taken" and "the original was destroyed". With
	// keys there is nothing to notify: a key whose row is gone resolves to
	// nothing and no frame is built for it.
	//
	// Called on the send pass, under no lock of the sender's.
	ReactionFactsFor(
		ctx context.Context,
		peer domain.PeerIdentity,
		keys []domain.ReactionKey,
	) ([]domain.ReactionFact, error)

	ReactionsToReoffer(
		ctx context.Context,
		peer domain.PeerIdentity,
		offer func([]domain.ReactionFact) error,
	) error
}

// RegisterConversationControlStore sets the handler for accepted dm_control
// commands. Must be called before Run(). A node without one accepts nothing:
// there is nowhere to put the state, and answering "understood" for a command
// that went nowhere would let the sender stop retrying a fact we never kept.
func (s *Service) RegisterConversationControlStore(store ConversationControlStore) {
	s.conversationControl = store
}

// errNoConversationControlStore is a RETRYABLE refusal: this node has no place
// to keep conversation state, which is a property of how it was assembled and
// not of the frame. Failing rather than rejecting releases the replay slot, so
// a later delivery of the same frame is still considered.
var errNoConversationControlStore = errors.New("dm_control: this node keeps no conversation state")

// registerDMControlTypes adds dm_control to the type registry.
//
// Called from newDatagramPlaneParts alongside the identity-discovery types, and
// for the same reason: §6.1 fixes the declared dtype set for a session's whole
// lifetime, so every type this node can receive has to exist before any
// handshake can run.
func registerDMControlTypes(types *datagram.TypeRegistry, svc *Service) error {
	return types.Register(datagram.TypeRegistration{
		DType:   domain.DTypeDMControl,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Payload: datagram.PayloadSchema{
			Name:    "dm_control",
			Version: domain.DMControlSchemaVersion,
		},
		// A control command reaches its recipient through relays, so the peer
		// that hands us the frame is usually not its author. Authorship comes
		// from the frame signature instead — SignedSrc is checked by the layer
		// before the handler sees anything — and requiring a proven NEIGHBOUR
		// would refuse every command that took more than one hop.
		SenderProof: datagram.SenderProvenInPayload,
		Handler:     &dmControlHandler{svc: svc},
	})
}

// dmControlHandler opens an arriving command and applies it.
//
// It holds the Service rather than the pieces it needs, because the store is
// registered after the registry is built; reaching through svc at delivery time
// is what keeps construction order inside NewService free.
type dmControlHandler struct {
	svc *Service
}

func (h *dmControlHandler) Handle(
	ctx context.Context,
	delivery datagram.DeliveryContext,
	payload []byte,
) datagram.HandlerResult {
	sender, ok := delivery.Header().SignedSrc()
	if !ok {
		// Unreachable for a routed frame — the layer sets this only after the
		// signature verified against src — and rejected rather than assumed,
		// because everything below attributes state to this identity.
		return datagram.RejectDelivery(errors.New("dm_control: no signed source"))
	}

	// Opened for the PAIR: the payload's key is derived from the signer and us,
	// so a relay that lifted somebody else's ciphertext into a frame it signed
	// itself cannot get it open here. See dmcontrol/seal.go.
	plain, err := dmcontrol.Open(h.svc.identity, sender, delivery.LocalIdentity(), payload)
	if err != nil {
		// Not ours to read. The commonest cause is a peer holding a box key we
		// have since rotated, and there is nothing a retry of the same bytes
		// would change, so the replay slot is committed.
		return datagram.RejectDelivery(err)
	}
	if len(plain) != dmcontrol.PayloadBucketBytes {
		// Every frame this design puts on the wire is padded to ONE size, and
		// the size is the contract: a sealed payload of any other length was not
		// built by a build that follows it. Checked before decoding, because the
		// plane's own limit leaves room for about four times the bucket — four
		// times the facts to walk and to write, on the caller's word alone.
		return datagram.RejectDelivery(fmt.Errorf(
			"dm_control: a %d-byte payload, not the %d-byte bucket",
			len(plain), dmcontrol.PayloadBucketBytes))
	}
	command, err := dmcontrol.Decode(plain)
	if err != nil {
		return datagram.RejectDelivery(err)
	}

	return h.dispatch(ctx, sender, command)
}

// dispatch is what an opened, decoded command MEANS to this node.
//
// Split from Handle so the decision can be checked without sealing a frame for
// it: everything above dispatch is the envelope — the signature, the pair-bound
// open, the decode — and none of it is what this switch decides.
func (h *dmControlHandler) dispatch(
	ctx context.Context,
	sender domain.PeerIdentity,
	command dmcontrol.Payload,
) datagram.HandlerResult {
	// Everything below this line either keeps state about the sender or sends
	// them a frame, so a stranger gets neither. The reactions branch is exempt
	// from asking HERE only because it asks the same question further in, where
	// it also decides whether a fact may WAIT for its message.
	//
	if command.Command != domain.DMControlReactions && !h.admits(ctx, sender, command.Command) {
		// Accepted rather than rejected: the frame was well formed and there is
		// nothing for the sender to retry differently. Silence is the answer.
		return datagram.AcceptDelivery()
	}

	switch command.Command {
	case domain.DMControlReactions:
		return h.applyReactions(ctx, sender, command)
	case domain.DMControlUnsupported:
		h.svc.noteCommandRefused(sender, command.Refused)
		return datagram.AcceptDelivery()
	default:
		// A command from a newer build. Answering names it back, which is the
		// only way the sender can tell "this peer cannot do it" from "this peer
		// is offline" — and telling those apart is why this feature is on
		// datagrams at all.
		h.svc.answerCommandUnsupported(sender, command.Command)
		return datagram.AcceptDelivery()
	}
}

// admits decides whether this sender may make the node keep state or send a
// frame back.
//
// A conversation with messages is the ordinary admission. Being the ANSWER to
// something we sent is the other, and it is deliberately narrow: only
// `unsupported`, and only within the window that answer can take, because that
// is the one case the first rule gets wrong — a thread the user has just wiped
// has no messages left while the contact, and the reaction that went to them a
// second before, are both still real.
//
// Narrow on purpose. Admitting ANY command on the strength of us having spoken
// to the peer would let a removed contact send an unknown command, be answered,
// and have that answer refresh the window that admitted them — a state they
// could hold open indefinitely from outside.
func (h *dmControlHandler) admits(
	ctx context.Context,
	sender domain.PeerIdentity,
	command domain.DMControlCommand,
) bool {
	if command == domain.DMControlUnsupported && h.svc.dmControl.spokeToRecently(sender) {
		return true
	}
	return h.knowsTheSender(ctx, sender)
}

// knowsTheSender answers whether this node has a conversation with the peer, and
// treats a store that cannot answer as "no".
//
// A database that will not read is not a reason to start keeping state for a
// stranger: the failure mode of guessing "yes" is exactly the growth this gate
// exists to prevent, and the cost of guessing "no" is one unanswered command
// from somebody we do talk to.
func (h *dmControlHandler) knowsTheSender(ctx context.Context, sender domain.PeerIdentity) bool {
	store := h.svc.conversationControl
	if store == nil {
		return false
	}
	known, err := store.HasConversationWith(ctx, sender)
	if err != nil {
		log.Warn().Err(err).Str("peer", sender.String()).
			Msg("dm_control_sender_unknown_after_a_failed_read")
		return false
	}
	if !known {
		log.Debug().Str("peer", sender.String()).
			Msg("dm_control_command_from_a_stranger_ignored")
	}
	return known
}

// applyReactions turns a decoded batch into facts attributed to the signer.
func (h *dmControlHandler) applyReactions(
	ctx context.Context,
	sender domain.PeerIdentity,
	command dmcontrol.Payload,
) datagram.HandlerResult {
	scope, err := domain.ResolveConversation(command.Conversation, sender)
	if err != nil {
		return datagram.RejectDelivery(err)
	}
	store := h.svc.conversationControl
	if store == nil {
		return datagram.FailDelivery(errNoConversationControlStore)
	}

	facts := make([]domain.ReactionFact, 0, len(command.Facts))
	for _, fact := range command.Facts {
		if err := fact.Validate(); err != nil {
			// One malformed fact voids the batch rather than being skipped:
			// a partially applied batch is a state neither side can name, and
			// the sender would go on believing all of it landed.
			return datagram.RejectDelivery(err)
		}
		facts = append(facts, domain.ReactionFact{
			Scope: scope,
			Key: domain.ReactionKey{
				MessageID: fact.MessageID,
				// The actor is the signer, never a field of the payload: there
				// is no way to state a fact in somebody else's name.
				Actor: sender,
				Emoji: fact.Emoji,
			},
			Op:    fact.Op,
			Clock: fact.Clock,
		})
	}
	if len(facts) == 0 {
		return datagram.RejectDelivery(fmt.Errorf("dm_control: a reactions command with no facts"))
	}

	// The command reads, so their build SENDS reactions — both directions ship
	// together — and whatever this node believed about it is now known to be
	// stale. Cleared here rather than only on a direct session, because a peer
	// reached through transit may never open one, and until something clears it
	// our own reactions to them are held back for the hour the belief lives.
	//
	// AFTER the structural checks and not before: a payload naming an unknown
	// conversation, carrying no facts or carrying a malformed one is rejected,
	// and a rejected frame must not be able to change what this node believes,
	// wake the outgoing queue and redraw the UI on its way out.
	h.svc.forgetDMControlRefusal(sender)

	if err := store.ApplyReactionFacts(ctx, sender, facts); err != nil {
		// A write that did not happen is retryable: releasing the replay slot
		// lets a re-delivery of the same batch be considered again.
		log.Warn().Err(err).Str("peer", sender.String()).Int("facts", len(facts)).
			Msg("dm_control_reactions_apply_failed")
		return datagram.FailDelivery(err)
	}
	log.Debug().Str("peer", sender.String()).Int("facts", len(facts)).
		Msg("dm_control_reactions_applied")
	return datagram.AcceptDelivery()
}
