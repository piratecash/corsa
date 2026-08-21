package node

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// storeOwnOutgoingDM stores a DM authored by svc and addressed to
// recipient, exactly as a local send would, and returns its id.
func storeOwnOutgoingDM(t *testing.T, svc *Service, recipient *identity.Identity, id protocol.MessageID) {
	t.Helper()
	body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        id,
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("own outgoing DM must be stored, got stored=%v errCode=%q", stored, errCode)
	}
}

func backlogHas(svc *Service, id protocol.MessageID) bool {
	svc.gossipMu.RLock()
	defer svc.gossipMu.RUnlock()
	for _, envelope := range svc.topics["dm"] {
		if envelope.ID == id {
			return true
		}
	}
	return false
}

// TestCancelOutgoingDeliveryClearsEveryDeliveryHook is the pin on the
// withdrawal guarantee: after the call nothing in the node can put the
// message on the wire again. Before the fix there was no way to stop the
// sender-owned retry, so "deleting" an unsent message locally would have
// left it being re-sent for hours.
func TestCancelOutgoingDeliveryClearsEveryDeliveryHook(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("cancel-hooks-1")
	storeOwnOutgoingDM(t, svc, recipientID, target)

	// Give the message the full set of delivery hooks a queued send owns:
	// the backlog envelope and retry entry come from the store above, the
	// pending frame and outbound entry from the queue-on-unreachable path.
	frame := protocol.Frame{Type: "send_message", ID: string(target), Topic: "dm", Recipient: recipientID.Address}
	svc.deliveryMu.Lock()
	svc.pending[domain.PeerAddress("127.0.0.1:64646")] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey(domain.PeerAddress("127.0.0.1:64646"), frame)] = struct{}{}
	svc.noteOutboundQueuedLocked(frame, "")
	_, registered := svc.awaitingDelivered[target]
	svc.deliveryMu.Unlock()
	if !registered {
		t.Fatal("precondition: own outgoing DM must be registered in awaitingDelivered")
	}
	if !backlogHas(svc, target) {
		t.Fatal("precondition: own outgoing DM must be in the backlog")
	}

	result, err := svc.CancelOutgoingDelivery(target, domain.PeerIdentityFromWire(recipientID.Address))
	if err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}

	if !result.BacklogRemoved || !result.RetryCancelled || !result.OutboundCleared {
		t.Errorf("result = %+v, want every hook reported removed", result)
	}
	if result.PendingFrames != 1 {
		t.Errorf("result.PendingFrames = %d, want 1", result.PendingFrames)
	}
	if got := result.Total(); got != 4 {
		t.Errorf("result.Total() = %d, want 4", got)
	}

	if backlogHas(svc, target) {
		t.Error("envelope still in the backlog after cancellation")
	}
	svc.deliveryMu.RLock()
	_, stillAwaiting := svc.awaitingDelivered[target]
	_, stillOutbound := svc.outbound[string(target)]
	_, stillRelayRetry := svc.relayRetry[relayMessageKey(target)]
	pendingLeft := svc.countPendingFramesLocked(target)
	svc.deliveryMu.RUnlock()

	if stillAwaiting {
		t.Error("sender-owned retry entry survived cancellation; the message would be re-sent")
	}
	if stillOutbound {
		t.Error("outbound status entry survived cancellation")
	}
	if stillRelayRetry {
		t.Error("relay retry shadow survived cancellation")
	}
	if pendingLeft != 0 {
		t.Errorf("pending frames left = %d, want 0", pendingLeft)
	}
}

// TestCancelOutgoingDeliveryRejectsForeignEnvelope pins the authorship
// gate: a transit envelope belongs to its sender's delivery contract, so
// a local caller must not be able to purge it by guessing an id.
func TestCancelOutgoingDeliveryRejectsForeignEnvelope(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Seeded straight into the backlog: what matters here is the
	// authorship of the stored envelope, not the admission path that
	// put it there.
	const target = protocol.MessageID("cancel-foreign-1")
	svc.gossipMu.Lock()
	svc.topics["dm"] = []protocol.Envelope{{
		ID:        target,
		Topic:     "dm",
		Sender:    senderID.Address,
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Payload:   []byte("transit-ciphertext"),
	}}
	svc.gossipMu.Unlock()

	_, err = svc.CancelOutgoingDelivery(target, domain.PeerIdentityFromWire(recipientID.Address))
	if !errors.Is(err, protocol.ErrInvalidCancelDelivery) {
		t.Fatalf("CancelOutgoingDelivery on a foreign envelope: err = %v, want %v", err, protocol.ErrInvalidCancelDelivery)
	}
	if !backlogHas(svc, target) {
		t.Error("transit envelope was dropped by a rejected cancellation")
	}
}

// TestCancelOutgoingDeliveryOnAbandonedMessageSucceeds pins that a
// message with nothing left to cancel — retry budget already spent,
// queues already swept — is a success with an empty result. The caller
// deletes its row on that answer, and refusing here would strand the
// user with an undeletable message.
func TestCancelOutgoingDeliveryOnAbandonedMessageSucceeds(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	result, err := svc.CancelOutgoingDelivery("cancel-unknown-1", domain.PeerIdentityFromWire(recipientID.Address))
	if err != nil {
		t.Fatalf("CancelOutgoingDelivery on an unknown message: %v", err)
	}
	if result.Total() != 0 {
		t.Errorf("result.Total() = %d, want 0", result.Total())
	}
}

// TestCancelOutgoingDeliveryRejectsIncompleteRequest pins the argument
// validation: both the message id and the recipient identify what to
// cancel, and a missing one would widen the removal instead of failing.
func TestCancelOutgoingDeliveryRejectsIncompleteRequest(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipient := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	if _, err := svc.CancelOutgoingDelivery("  ", recipient); !errors.Is(err, protocol.ErrInvalidCancelDelivery) {
		t.Errorf("empty message id: err = %v, want %v", err, protocol.ErrInvalidCancelDelivery)
	}
	if _, err := svc.CancelOutgoingDelivery("cancel-validate-1", domain.PeerIdentity{}); !errors.Is(err, protocol.ErrInvalidCancelDelivery) {
		t.Errorf("zero recipient: err = %v, want %v", err, protocol.ErrInvalidCancelDelivery)
	}
}

// TestCancelMessageDeliveryFrameReportsRemovedHooks covers the local
// command surface the application actually calls.
func TestCancelMessageDeliveryFrameReportsRemovedHooks(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("cancel-frame-1")
	storeOwnOutgoingDM(t, svc, recipientID, target)

	reply := svc.HandleLocalFrame(protocol.Frame{
		Type:      "cancel_message_delivery",
		ID:        string(target),
		Recipient: recipientID.Address,
	})
	if reply.Type != "delivery_cancelled" {
		t.Fatalf("reply.Type = %q (%s), want delivery_cancelled", reply.Type, reply.Error)
	}
	// Backlog envelope + sender-owned retry entry.
	if reply.Count != 2 {
		t.Errorf("reply.Count = %d, want 2", reply.Count)
	}
	if backlogHas(svc, target) {
		t.Error("envelope still in the backlog after the command")
	}

	bad := svc.HandleLocalFrame(protocol.Frame{Type: "cancel_message_delivery", ID: string(target)})
	if bad.Type != "error" || bad.Code != protocol.ErrCodeInvalidCancelDelivery {
		t.Errorf("missing recipient: reply = %+v, want an %s error", bad, protocol.ErrCodeInvalidCancelDelivery)
	}
}

// newHoldingTestService is newTestService with the reachability gate on,
// so an outgoing DM to a peer with no route is HELD instead of
// blind-gossiped — the state in which "this never reached the wire" is
// something the node can actually prove.
func newHoldingTestService(t *testing.T) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:        "127.0.0.1:64646",
		TrustStorePath:       filepath.Join(tempDir, "trust.json"),
		Type:                 config.NodeTypeFull,
		AllowPrivatePeers:    true,
		HoldDMUntilReachable: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	return svc
}

// TestCancelOutgoingDeliveryReportsNeverEmitted pins the claim the
// application acts on: a message held because the recipient was
// unreachable has never been on the wire, so nobody can be asked to
// delete it — and asking would tell them a message existed.
func TestCancelOutgoingDeliveryReportsNeverEmitted(t *testing.T) {
	t.Parallel()
	svc := newHoldingTestService(t)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("cancel-never-emitted-1")
	storeOwnOutgoingDM(t, svc, recipientID, target)

	svc.deliveryMu.RLock()
	entry, registered := svc.awaitingDelivered[target]
	emitted := registered && entry.Emitted
	svc.deliveryMu.RUnlock()
	if !registered {
		t.Fatal("precondition: the DM must be registered for retry")
	}
	if emitted {
		t.Fatal("precondition: an unreachable recipient means the first send was held, not emitted")
	}

	result, err := svc.CancelOutgoingDelivery(target, domain.PeerIdentityFromWire(recipientID.Address))
	if err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}
	if !result.NeverEmitted {
		t.Error("NeverEmitted = false for a message that was never dispatched")
	}
}

// TestCancelOutgoingDeliveryDoesNotClaimNeverEmittedWithoutEvidence — an
// id with no retry entry left (delivered, abandoned, evicted) is exactly
// the case where the node knows nothing, and the claim must not be made.
func TestCancelOutgoingDeliveryDoesNotClaimNeverEmittedWithoutEvidence(t *testing.T) {
	t.Parallel()
	svc := newHoldingTestService(t)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	result, err := svc.CancelOutgoingDelivery("cancel-no-evidence-1", domain.PeerIdentityFromWire(recipientID.Address))
	if err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}
	if result.NeverEmitted {
		t.Error("NeverEmitted = true with no retry entry to base it on")
	}
}

// TestBacklogReplayClearsTheNeverEmittedClaim is the regression test for
// the delivery path the retry engine cannot see: the recipient dials US,
// and the auth-time backlog replay hands them everything in
// s.topics["dm"] — held messages included, because the backlog append is
// not gated on the reachability hold.
//
// Before this was accounted for, such a message still reported
// "never emitted": the deletion took the recalled route, no intent was
// recorded, and the copy the peer had just collected stayed with them
// forever with nothing left to ask again.
func TestBacklogReplayClearsTheNeverEmittedClaim(t *testing.T) {
	t.Parallel()
	svc := newHoldingTestService(t)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("backlog-replay-emitted-1")
	storeOwnOutgoingDM(t, svc, recipientID, target)

	svc.deliveryMu.RLock()
	entry, registered := svc.awaitingDelivered[target]
	heldBefore := registered && !entry.Emitted
	svc.deliveryMu.RUnlock()
	if !heldBefore {
		t.Fatal("precondition: an unreachable recipient means the message is held, not emitted")
	}
	if !backlogHas(svc, target) {
		t.Fatal("precondition: a held message must still be in the backlog — that is what the replay serves")
	}

	// The recipient connects and the backlog is replayed to it. The
	// write itself goes nowhere (no live connection in this test), which
	// is exactly the boundary the accounting uses: handed to the wire.
	svc.pushBacklogToSubscriber(&subscriber{id: "sub-1", recipient: recipientID.Address})

	result, err := svc.CancelOutgoingDelivery(target, domain.PeerIdentityFromWire(recipientID.Address))
	if err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}
	if result.NeverEmitted {
		t.Fatal("NeverEmitted = true after the backlog replay handed the message to the recipient; " +
			"the deletion would be recalled and the peer's copy left with nothing to retry it")
	}
}
