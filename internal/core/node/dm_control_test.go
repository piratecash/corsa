package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/dmcontrol"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// futureCommandFrame is a frame from a build newer than this one: a command this
// code does not know, padded to the same bucket every real frame is.
//
// Written by hand because Encode refuses to SEND an unknown command, which is
// the point of it — and padded by hand for the same reason the receiver insists
// on the size: a short payload is not what a build following the contract emits,
// and a fixture that skipped the padding would be testing a frame nothing sends.
func futureCommandFrame(t *testing.T, command string) []byte {
	t.Helper()
	return commandFrameOfSize(t, command, dmcontrol.PayloadBucketBytes)
}

// commandFrameOfSize is the same, at whatever length the caller asks for, so a
// test can offer a payload that is valid JSON and the WRONG size.
func commandFrameOfSize(t *testing.T, command string, size int) []byte {
	t.Helper()
	frame := func(pad string) []byte {
		return []byte(fmt.Sprintf(`{"v":1,"cmd":%q,"conv":"direct","pad":%q}`, command, pad))
	}
	room := size - len(frame(""))
	if room < 0 {
		t.Fatalf("a %d-byte frame cannot hold the command %q", size, command)
	}
	plain := frame(strings.Repeat("a", room))
	if len(plain) != size {
		t.Fatalf("the fixture built a %d-byte frame, want %d", len(plain), size)
	}
	return plain
}

// recordingControlStore is node.ConversationControlStore without a database, so
// what the handler DECIDES can be checked apart from how it is persisted.
type recordingControlStore struct {
	senders []domain.PeerIdentity
	batches [][]domain.ReactionFact
	err     error
	// reoffer is what this node would offer a peer again on a session.
	reoffer []domain.ReactionFact
	// stranger is a peer this node has no conversation with, and
	// hasConversationErr makes the question itself fail.
	stranger           domain.PeerIdentity
	hasConversationErr error
}

func (r *recordingControlStore) ReactionsToReoffer(
	_ context.Context,
	_ domain.PeerIdentity,
	offer func([]domain.ReactionFact) error,
) error {
	if len(r.reoffer) == 0 {
		return nil
	}
	return offer(r.reoffer)
}

// ReactionFactsFor resolves queued keys the way the real store does: what the
// record says now, with a key it no longer holds simply missing.
func (r *recordingControlStore) ReactionFactsFor(
	_ context.Context,
	peer domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	facts := make([]domain.ReactionFact, 0, len(keys))
	for _, key := range keys {
		facts = append(facts, domain.ReactionFact{
			Scope: domain.ReactionScopeForPeer(peer),
			Key:   key,
			Op:    domain.ReactionSet,
			Clock: 1,
		})
	}
	return facts, nil
}

// HasConversationWith answers for the fixture's peers. `stranger`, when set,
// is the one identity this node has never exchanged a message with.
func (r *recordingControlStore) HasConversationWith(_ context.Context, peer domain.PeerIdentity) (bool, error) {
	if r.hasConversationErr != nil {
		return false, r.hasConversationErr
	}
	return peer != r.stranger, nil
}

func (r *recordingControlStore) ApplyReactionFacts(
	_ context.Context,
	sender domain.PeerIdentity,
	facts []domain.ReactionFact,
) error {
	if r.err != nil {
		return r.err
	}
	r.senders = append(r.senders, sender)
	r.batches = append(r.batches, facts)
	return nil
}

// sealedControlDelivery builds a signed dm_control frame from signer to svc,
// sealed to svc's box key, and the delivery context the pipeline would hand the
// handler for it.
func sealedControlDelivery(
	t *testing.T,
	svc *Service,
	signer *identity.Identity,
	plain []byte,
) (datagram.DeliveryContext, []byte) {
	t.Helper()
	sealed, err := dmcontrol.Seal(
		domain.PeerIdentityFromWire(signer.Address),
		domain.PeerIdentityFromWire(svc.identity.Address),
		identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey), plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("entropy: %v", err)
	}
	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         domain.PeerIdentityFromWire(signer.Address),
		Dst:         domain.PeerIdentityFromWire(svc.identity.Address),
		TTL:         1,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DTypeDMControl,
		Payload:     sealed,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			PubKey:      append([]byte(nil), signer.PublicKey...),
			Salt:        salt,
			MaxTTL:      1,
			Time:        time.Now().UTC().Unix(),
		},
	}
	signed, err := protocol.SignDatagram(frame, testRecordStoreNetwork, signer.PrivateKey)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	header, err := datagram.NewDeliveryHeader(signed)
	if err != nil {
		t.Fatalf("delivery header: %v", err)
	}
	delivery, err := datagram.NewDeliveryContext(datagram.DeliveryContextOpts{
		Header: header,
		// A control command normally reaches its destination through relays, so
		// the neighbour that handed it over is deliberately somebody else: the
		// handler must not be reading authorship from the session.
		IncomingPeer:  datagram.ProvenIngress(datagram.NetworkChannel(domain.ConnID(11)), domain.PeerIdentityFromWire(svc.identity.Address)),
		LocalIdentity: domain.PeerIdentityFromWire(svc.identity.Address),
	})
	if err != nil {
		t.Fatalf("delivery context: %v", err)
	}
	return delivery, sealed
}

// A peer only learns this node can take conversation-control commands from the
// dtype set declared at handshake. If dm_control were missing from it, every
// peer would refuse our reactions at the last hop and the feature would be
// silently one-way.
func TestDMControlIsDeclaredToPeers(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	if !slices.Contains(svc.localDatagramDTypes(), domain.DTypeDMControl) {
		t.Fatalf("dm_control is not declared; this node advertises %v", svc.localDatagramDTypes())
	}
}

// The actor is the frame's signer and nothing else. The payload has no field to
// claim one in, and the handler must not fall back to the neighbour that handed
// the frame over — on a relayed command that neighbour is a third party.
func TestReactionsAreAttributedToTheSigner(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	store := &recordingControlStore{}
	svc.RegisterConversationControlStore(store)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	plain, err := dmcontrol.Encode(dmcontrol.ReactionsPayload(domain.ConversationDirect, []dmcontrol.Fact{
		{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: 3},
	}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	delivery, sealed := sealedControlDelivery(t, svc, peer, plain)

	handler := &dmControlHandler{svc: svc}
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("a well-formed command was not accepted: %v", result.Err())
	}
	if len(store.batches) != 1 {
		t.Fatalf("the store received %d batches, want 1", len(store.batches))
	}
	fact := store.batches[0][0]
	signer := domain.PeerIdentityFromWire(peer.Address)
	if fact.Key.Actor != signer {
		t.Fatalf("the fact is attributed to %s, want the signer %s", fact.Key.Actor, signer)
	}
	if want := domain.ReactionScopeForPeer(signer); fact.Scope != want {
		t.Fatalf("scope is %q, want the conversation with the signer (%q)", fact.Scope, want)
	}

	// And again: the layer promises zero or more deliveries, so the same frame
	// twice must reach the store twice without the handler deciding anything
	// different about it.
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("a repeat delivery was refused: %v", result.Err())
	}
	if len(store.batches) != 2 || store.batches[1][0] != fact {
		t.Fatalf("the repeat delivery produced %#v", store.batches)
	}
}

// A command from a newer build is answered, not dropped. Without the answer the
// sender cannot tell "this peer will never understand it" from "this peer is
// offline" — and telling those apart is why this type is on datagrams at all.
func TestAnUnknownCommandIsAnsweredNotDropped(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	// The answer is queued only where something will send it, so the fixture
	// stands in for the send loop that would normally be running.
	svc.dmControl.setDraining(true)
	// And only for a sender this node has a conversation with: see
	// ConversationControlStore.HasConversationWith.
	svc.RegisterConversationControlStore(&recordingControlStore{})
	delivery, sealed := sealedControlDelivery(t, svc, peer, futureCommandFrame(t, "message_edit"))

	handler := &dmControlHandler{svc: svc}
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("an unknown command was refused instead of answered: %v", result.Err())
	}
	outbox := queuedFor(svc.dmControl, domain.PeerIdentityFromWire(peer.Address))
	if outbox == nil || len(outbox.refusals) != 1 || outbox.refusals[0].command != "message_edit" {
		t.Fatalf("no refusal was queued for the sender: %#v", outbox)
	}

	// A second copy of the same frame must not queue a second answer: the
	// layer delivers zero or more times, and one refusal per delivery would
	// turn a duplicate into a burst.
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("the repeat was refused: %v", result.Err())
	}
	if got := len(queuedFor(svc.dmControl, domain.PeerIdentityFromWire(peer.Address)).refusals); got != 1 {
		t.Fatalf("a duplicate delivery queued %d refusals", got)
	}
}

// A refusal arriving from the peer is what stops us offering the feature to
// them, and it is the inner counterpart of the transport's own
// unsupported_dtype.
func TestAPeersRefusalIsRemembered(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	svc.RegisterConversationControlStore(&recordingControlStore{})
	plain, err := dmcontrol.Encode(dmcontrol.UnsupportedPayload(domain.ConversationDirect, domain.DMControlReactions))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	delivery, sealed := sealedControlDelivery(t, svc, peer, plain)

	signer := domain.PeerIdentityFromWire(peer.Address)
	if svc.ReactionsUnsupportedBy(signer) {
		t.Fatal("a peer nothing was sent to already counts as unable")
	}
	if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("a refusal was not accepted: %v", result.Err())
	}
	if !svc.ReactionsUnsupportedBy(signer) {
		t.Fatal("the peer's refusal was not remembered")
	}

	// A new session re-declares what the peer can receive, so what we believe
	// about their build is stale from that moment.
	svc.forgetDMControlRefusal(signer)
	if svc.ReactionsUnsupportedBy(signer) {
		t.Fatal("a fresh session did not clear the refusal")
	}
}

// A node with nowhere to keep conversation state FAILS the delivery rather than
// rejecting it. The two differ in the replay slot: a rejection commits the key
// and the same frame is never considered again, which would lose the facts of
// exactly the window in which the store was being wired up.
func TestReactionsAreRetryableWhenThereIsNowhereToPutThem(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	plain, err := dmcontrol.Encode(dmcontrol.ReactionsPayload(domain.ConversationDirect, []dmcontrol.Fact{
		{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: 1},
	}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	delivery, sealed := sealedControlDelivery(t, svc, peer, plain)

	result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed)
	if result.Outcome() == datagram.HandlerAccepted {
		t.Fatal("facts were accepted by a node that has nowhere to keep them")
	}
	if result.Outcome() != datagram.HandlerFailed {
		t.Fatalf("the delivery was refused permanently rather than as retryable: %v", result.Err())
	}
}

// A payload this node cannot open is refused permanently: the commonest cause
// is a peer holding a box key we have since rotated, and no retry of the same
// bytes changes that.
func TestAPayloadWeCannotOpenIsRejected(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	svc.RegisterConversationControlStore(&recordingControlStore{})
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	stranger, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	plain, err := dmcontrol.Encode(dmcontrol.ReactionsPayload(domain.ConversationDirect, []dmcontrol.Fact{
		{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: 1},
	}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	delivery, _ := sealedControlDelivery(t, svc, peer, plain)
	// Sealed to somebody else entirely.
	elsewhere, err := dmcontrol.Seal(
		domain.PeerIdentityFromWire(peer.Address),
		domain.PeerIdentityFromWire(stranger.Address),
		identity.BoxPublicKeyBase64(stranger.BoxPublicKey), plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, elsewhere)
	if result.Outcome() != datagram.HandlerRejected {
		t.Fatalf("an unreadable payload was not rejected: %v", result.Err())
	}
}

// A relay carrying A's command to us can lift the ciphertext out and put it in
// a frame it signs ITSELF. It cannot read what it is asserting, but without the
// pair binding it would not need to: the facts would land under the relay's own
// key carrying A's clock values, and since the merge keeps the highest clock,
// one such frame silences those keys for good.
func TestACiphertextCannotBeReSignedByARelay(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	store := &recordingControlStore{}
	svc.RegisterConversationControlStore(store)

	author, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	relay, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	plain, err := dmcontrol.Encode(dmcontrol.ReactionsPayload(domain.ConversationDirect, []dmcontrol.Fact{
		{MessageID: "m1", Emoji: "👍", Op: domain.ReactionSet, Clock: 1 << 40},
	}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// The author's ciphertext, verbatim, inside a frame the relay signed.
	_, authored := sealedControlDelivery(t, svc, author, plain)
	relayDelivery, _ := sealedControlDelivery(t, svc, relay, plain)

	result := (&dmControlHandler{svc: svc}).Handle(context.Background(), relayDelivery, authored)
	if result.Outcome() != datagram.HandlerRejected {
		t.Fatalf("a re-signed ciphertext was accepted: %v", result.Err())
	}
	if len(store.batches) != 0 {
		t.Fatalf("the transplanted facts reached the store: %#v", store.batches)
	}
}

// A peer whose build SENDS reactions has the feature — both directions ship
// together — so whatever this node believed about them is now known to be
// stale. Cleared on the reaction itself and not only on a direct session,
// because a peer reached through transit may never open one, and until
// something clears it our reactions to them are held back for the hour the
// belief lives.
func TestAReceivedReactionClearsWhatWeBelievedAboutTheirBuild(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	store := &recordingControlStore{}
	svc.RegisterConversationControlStore(store)
	sender := controlTestPeer("e7")

	svc.noteCommandRefused(sender, domain.DMControlReactions)
	if !svc.ReactionsUnsupportedBy(sender) {
		t.Fatal("the fixture did not record the refusal it is about to clear")
	}

	handler := &dmControlHandler{svc: svc}
	handler.dispatch(context.Background(), sender, dmcontrol.Payload{
		Command:      domain.DMControlReactions,
		Conversation: domain.ConversationDirect,
		Facts: []dmcontrol.Fact{{
			MessageID: "m1", Emoji: "\U0001F44D", Op: domain.ReactionSet, Clock: 1,
		}},
	})

	if svc.ReactionsUnsupportedBy(sender) {
		t.Fatal("a reaction from the peer left the belief that they cannot receive one")
	}
}

// A signature says who signed, not that there is anything between us — and
// identities are free to mint. So a stranger's command must leave nothing
// behind: no queued answer (which this node would then retry for half an hour,
// amplifying one relayed frame into many) and no hour-long note about their
// build.
func TestAStrangersCommandLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	svc.dmControl.setDraining(true)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	stranger := domain.PeerIdentityFromWire(peer.Address)
	svc.RegisterConversationControlStore(&recordingControlStore{stranger: stranger})

	for _, tc := range []struct {
		name  string
		plain []byte
	}{
		{
			name:  "a command from a newer build",
			plain: futureCommandFrame(t, "message_edit"),
		},
		{
			name:  "an unsolicited refusal",
			plain: mustEncodeUnsupported(t, domain.DMControlReactions),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivery, sealed := sealedControlDelivery(t, svc, peer, tc.plain)
			handler := &dmControlHandler{svc: svc}
			if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
				t.Fatalf("the frame was refused rather than ignored: %v", result.Err())
			}
			if outbox := queuedFor(svc.dmControl, stranger); outbox != nil {
				t.Fatalf("a stranger made this node queue %#v", outbox)
			}
			if svc.ReactionsUnsupportedBy(stranger) {
				t.Fatal("a stranger made this node keep a note about their build")
			}
		})
	}

	// A database that will not answer is not a reason to start keeping state for
	// somebody: guessing "we know them" is exactly the growth the gate exists to
	// prevent, and guessing wrong the other way costs one unanswered command.
	unreadable := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	unreadable.dmControl.setDraining(true)
	unreadable.RegisterConversationControlStore(&recordingControlStore{
		hasConversationErr: errors.New("the conversation store is unreadable"),
	})
	delivery, sealed := sealedControlDelivery(t, unreadable, peer, mustEncodeUnsupported(t, domain.DMControlReactions))
	if result := (&dmControlHandler{svc: unreadable}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("the frame was refused rather than ignored: %v", result.Err())
	}
	if unreadable.ReactionsUnsupportedBy(stranger) {
		t.Fatal("a failed read let a sender leave a note about their build")
	}

	// A peer we DO talk to is answered and believed, so the gate is what stopped
	// the stranger and not something else about the fixture.
	known, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	delivery, sealed = sealedControlDelivery(t, svc, known, mustEncodeUnsupported(t, domain.DMControlReactions))
	if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("a refusal from a known peer was not accepted: %v", result.Err())
	}
	if !svc.ReactionsUnsupportedBy(domain.PeerIdentityFromWire(known.Address)) {
		t.Fatal("a refusal from a peer we talk to was ignored")
	}
}

func mustEncodeUnsupported(t *testing.T, refused domain.DMControlCommand) []byte {
	t.Helper()
	plain, err := dmcontrol.Encode(dmcontrol.UnsupportedPayload(domain.ConversationDirect, refused))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return plain
}

// A thread the user has just wiped keeps its contact and has no messages, so
// "do we have a conversation" answers no — while the reaction that went to them
// a second earlier is real and their answer to it is on its way. Refusing that
// answer would leave the next reaction looking delivered until the peer refuses
// again, which is what the wipe path takes care to avoid.
func TestALateAnswerIsBelievedAfterTheThreadIsWiped(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc
	// The store says what a wiped thread says: this contact has no messages.
	svc.RegisterConversationControlStore(&recordingControlStore{stranger: peer})
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
	}

	// We react, the frame goes out, and only then is the thread wiped.
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
	svc.DropQueuedReactions(peer)

	// Their answer arrives after all that.
	delivery, sealed := sealedControlDelivery(t, svc, peerID, mustEncodeUnsupported(t, domain.DMControlReactions))
	if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("the answer was refused: %v", result.Err())
	}
	if !svc.ReactionsUnsupportedBy(peer) {
		t.Fatal("the answer to a reaction we had just sent was ignored because the thread was wiped")
	}

	// And the window closes: past it, an answer can no longer be about anything
	// we sent, so a peer with no conversation is a stranger again.
	svc.forgetDMControlRefusal(peer)
	now = now.Add(dmControlForgetGrace)
	sender.flushDue(context.Background(), now)
	delivery, sealed = sealedControlDelivery(t, svc, peerID, mustEncodeUnsupported(t, domain.DMControlReactions))
	if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("the late answer was refused rather than ignored: %v", result.Err())
	}
	if svc.ReactionsUnsupportedBy(peer) {
		t.Fatal("an answer arriving long after anything we sent was still believed")
	}
	// And the record of having spoken to them is swept rather than kept: it is a
	// map keyed by peer identity, which is the growth shape this project has
	// paid for before.
	sender.mu.Lock()
	remembered := len(sender.sentAt)
	sender.mu.Unlock()
	if remembered != 0 {
		t.Fatalf("%d peers are still remembered as recently spoken to", remembered)
	}
}

// The admission granted by "we spoke to them" is deliberately narrow, and each
// of these is a way it was too wide before: an unknown command must not ride it
// (a removed contact could then be answered, and the answer would refresh the
// window that admitted them, holding it open from outside); removing the contact
// must revoke it; and an attempt that never reached the plane must not grant it,
// because nothing was sent for an answer to be about.
func TestSpeakingToAPeerAdmitsOnlyTheirAnswer(t *testing.T) {
	t.Parallel()

	// setup gives a peer with NO conversation that this node has just sent a
	// frame to, which is the only state that grants the narrow admission.
	setup := func(t *testing.T, delivered bool) (*Service, *identity.Identity, domain.PeerIdentity, *time.Time) {
		t.Helper()
		now := time.Now().UTC()
		peerID, err := identity.Generate()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		peer := domain.PeerIdentityFromWire(peerID.Address)
		sender := controlSenderWithKey(t, &now, peerID)
		sender.svc.RegisterConversationControlStore(&recordingControlStore{stranger: peer})
		sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
			if delivered {
				return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
			}
			return dmControlDispatch{kind: datagram.SendNoRoute, summary: "no_route"}
		}
		if err := sender.queueReactions(peer, []domain.ReactionFact{
			reactionFactFor(peer, "m1", "\U0001F44D", 1),
		}); err != nil {
			t.Fatalf("queue: %v", err)
		}
		sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
		return sender.svc, peerID, peer, &now
	}

	t.Run("an unknown command does not ride it", func(t *testing.T) {
		svc, peerID, peer, _ := setup(t, true)
		delivery, sealed := sealedControlDelivery(t, svc, peerID, futureCommandFrame(t, "message_edit"))
		if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
			t.Fatalf("the frame was refused rather than ignored: %v", result.Err())
		}
		if outbox := queuedFor(svc.dmControl, peer); outbox != nil {
			t.Fatalf("an unknown command was answered on the strength of what we sent: %#v", outbox)
		}
	})

	t.Run("removing the contact revokes it", func(t *testing.T) {
		svc, peerID, peer, _ := setup(t, true)
		svc.ForgetPeerReactions(peer)

		delivery, sealed := sealedControlDelivery(t, svc, peerID, mustEncodeUnsupported(t, domain.DMControlReactions))
		if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
			t.Fatalf("the frame was refused rather than ignored: %v", result.Err())
		}
		if svc.ReactionsUnsupportedBy(peer) {
			t.Fatal("a removed contact's answer was still believed")
		}

		// Asserted on the ADMISSION and not only on the belief: the answer above
		// is also refused by the forget window, which covers the same span, so
		// the behaviour alone cannot tell whether the admission itself was
		// revoked. It has to be, or a removed contact keeps a foothold that
		// outlives everything else the removal cleared.
		svc.dmControl.mu.Lock()
		_, admitted := svc.dmControl.sentAt[peer]
		svc.dmControl.mu.Unlock()
		if admitted {
			t.Fatal("the removal left the peer admitted as somebody we had spoken to")
		}
	})

	t.Run("a wipe keeps it", func(t *testing.T) {
		svc, peerID, peer, _ := setup(t, true)
		svc.DropQueuedReactions(peer)
		delivery, sealed := sealedControlDelivery(t, svc, peerID, mustEncodeUnsupported(t, domain.DMControlReactions))
		if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
			t.Fatalf("the frame was refused: %v", result.Err())
		}
		if !svc.ReactionsUnsupportedBy(peer) {
			t.Fatal("a wipe threw away the admission its own contract depends on")
		}
	})

	t.Run("an attempt that never left does not grant it", func(t *testing.T) {
		svc, peerID, peer, _ := setup(t, false)
		delivery, sealed := sealedControlDelivery(t, svc, peerID, mustEncodeUnsupported(t, domain.DMControlReactions))
		if result := (&dmControlHandler{svc: svc}).Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
			t.Fatalf("the frame was refused rather than ignored: %v", result.Err())
		}
		if svc.ReactionsUnsupportedBy(peer) {
			t.Fatal("an answer was believed although nothing of ours ever reached the plane")
		}
	})
}

// A rejected frame must not change what this node believes on its way out. The
// belief was cleared before the structural checks once, so a payload naming an
// unknown conversation — or carrying no facts, or a malformed one — still marked
// the peer as able to receive reactions, woke the outgoing queue and redrew the
// UI, on the strength of a frame that was refused.
func TestARejectedReactionsFrameLeavesTheBeliefAlone(t *testing.T) {
	t.Parallel()
	sender := controlTestPeer("d9")

	for _, tc := range []struct {
		name    string
		payload dmcontrol.Payload
	}{
		{
			name: "a conversation this build cannot resolve",
			payload: dmcontrol.Payload{
				Command:      domain.DMControlReactions,
				Conversation: domain.ConversationKind("group"),
				Facts: []dmcontrol.Fact{{
					MessageID: "m1", Emoji: "\U0001F44D", Op: domain.ReactionSet, Clock: 1,
				}},
			},
		},
		{
			name: "no facts at all",
			payload: dmcontrol.Payload{
				Command:      domain.DMControlReactions,
				Conversation: domain.ConversationDirect,
			},
		},
		{
			name: "a malformed fact",
			payload: dmcontrol.Payload{
				Command:      domain.DMControlReactions,
				Conversation: domain.ConversationDirect,
				Facts:        []dmcontrol.Fact{{MessageID: "m1", Emoji: "", Op: domain.ReactionSet, Clock: 1}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
			svc.RegisterConversationControlStore(&recordingControlStore{})
			svc.dmControl.setDraining(true)
			svc.noteCommandRefused(sender, domain.DMControlReactions)
			if !svc.ReactionsUnsupportedBy(sender) {
				t.Fatal("the fixture did not record the belief the frame must not clear")
			}

			result := (&dmControlHandler{svc: svc}).dispatch(context.Background(), sender, tc.payload)
			if result.Outcome() == datagram.HandlerAccepted {
				t.Fatal("the frame was accepted; this test is about a rejected one")
			}
			if !svc.ReactionsUnsupportedBy(sender) {
				t.Fatal("a rejected frame cleared what this node believed about the peer's build")
			}
		})
	}
}

// A frame carrying a clock no store can hold is REFUSED, not failed.
//
// Failing releases the replay slot, which is the right answer to a database that
// might work next time — and the wrong one here: the frame is unstorable for as
// long as it exists, so it would come back, and the usable facts ahead of it in
// the batch would be applied again on every pass.
func TestAFrameWithAnUnstorableClockIsRefusedNotFailed(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	store := &recordingControlStore{}
	svc.RegisterConversationControlStore(store)
	sender := controlTestPeer("da")

	result := (&dmControlHandler{svc: svc}).dispatch(context.Background(), sender, dmcontrol.Payload{
		Command:      domain.DMControlReactions,
		Conversation: domain.ConversationDirect,
		Facts: []dmcontrol.Fact{
			{MessageID: "m1", Emoji: "\U0001F44D", Op: domain.ReactionSet, Clock: 1},
			{MessageID: "m2", Emoji: "\U0001F525", Op: domain.ReactionSet, Clock: math.MaxInt64 + 1},
		},
	})
	if result.Outcome() != datagram.HandlerRejected {
		t.Fatalf("the frame was answered with %v, want a refusal that keeps the replay slot", result.Outcome())
	}
	// And nothing of it was applied: one unusable fact voids the batch rather
	// than leaving a state neither side can name.
	if len(store.batches) != 0 {
		t.Fatalf("%d batches were applied from a refused frame", len(store.batches))
	}
}

// Every frame this design puts on the wire is padded to ONE size, and the size
// is part of the contract. The plane's own limit leaves room for about four
// buckets, so a payload of any other length is either not built by a build that
// follows the contract — or is four times the facts to walk and to write, on the
// sender's word alone.
func TestAPayloadThatIsNotTheBucketIsRefused(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	svc.RegisterConversationControlStore(&recordingControlStore{})
	svc.dmControl.setDraining(true)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	// The same command, once at the bucket and once over it — both valid JSON,
	// so it is the LENGTH under test and not the parse.
	fitting := futureCommandFrame(t, "message_edit")
	oversized := commandFrameOfSize(t, "message_edit", dmcontrol.PayloadBucketBytes+64)

	handler := &dmControlHandler{svc: svc}
	delivery, sealed := sealedControlDelivery(t, svc, peer, oversized)
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerRejected {
		t.Fatalf("a payload over the bucket was answered with %v", result.Outcome())
	}
	if outbox := queuedFor(svc.dmControl, domain.PeerIdentityFromWire(peer.Address)); outbox != nil {
		t.Fatalf("a refused payload still made this node queue %#v", outbox)
	}

	// And the bucket-sized one goes through, so it is the LENGTH that was
	// refused and not the command.
	delivery, sealed = sealedControlDelivery(t, svc, peer, fitting)
	if result := handler.Handle(context.Background(), delivery, sealed); result.Outcome() != datagram.HandlerAccepted {
		t.Fatalf("a bucket-sized frame was refused: %v", result.Err())
	}
}
