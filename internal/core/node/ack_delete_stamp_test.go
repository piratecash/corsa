package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// sessionAtVersion is a peer session that only has to answer "which wire
// version does this connection speak" and hold what was written to it.
func sessionAtVersion(address domain.PeerAddress, version int) *peerSession {
	return &peerSession{
		address: address,
		version: version,
		sendCh:  make(chan peerSendItem, 4),
		authOK:  true,
	}
}

// A frame somebody else signed is not ours to re-sign: re-stamping it
// would replace their signature with one over our identity and turn a
// relayed ack into a claim of our own.
func TestStampLeavesForeignFramesAlone(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	foreign := protocol.Frame{
		Type:          "ack_delete",
		Address:       "somebody-else",
		AckType:       "receipt",
		ID:            "not-ours",
		Status:        protocol.ReceiptStatusDelivered,
		ReceiptSender: "their-receipt-author",
		Signature:     "their-signature",
	}
	stamped := svc.gateAckDeleteForSession(sessionAtVersion("older-peer", config.ProtocolVersion), legacyPeerSendItem(foreign))
	if stamped.Signature != foreign.Signature || stamped.ReceiptSender != foreign.ReceiptSender ||
		stamped.Address != foreign.Address {
		t.Errorf("a frame this node did not author was rewritten: %+v", stamped.Frame)
	}
}

// Committing one receipt used to drop the queued frames of every receipt
// that shared its message and status — the forged one and the genuine one
// have separate dedup keys and separate retry entries now, and this was
// the last place where confirming one still discarded the other.
func TestCommitOnlyClearsTheQueuedFramesOfItsOwnReceipt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	const target = protocol.MessageID("two-receipts-in-flight")
	const holder = domain.PeerAddress("peer-holding-both-frames")
	genuine := receiptFrom("the-actual-recipient", "the-waiting-sender", target)
	forged := receiptFrom("someone-who-knew-the-id", "the-waiting-sender", target)

	queueFrame := func(receipt protocol.DeliveryReceipt) protocol.Frame {
		return protocol.Frame{
			Type:        "relay_delivery_receipt",
			ID:          string(receipt.MessageID),
			Address:     receipt.Sender,
			Recipient:   receipt.Recipient,
			Status:      receipt.Status,
			DeliveredAt: receipt.DeliveredAt.Format(time.RFC3339),
		}
	}
	svc.deliveryMu.Lock()
	for _, receipt := range []protocol.DeliveryReceipt{genuine, forged} {
		frame := queueFrame(receipt)
		svc.pending[holder] = append(svc.pending[holder], pendingFrame{Frame: frame, QueuedAt: time.Now().UTC()})
		svc.pendingKeys[pendingFrameKey(holder, frame)] = struct{}{}
	}
	svc.clearPendingReceiptLocked(identityOf(forged), false)
	left := append([]pendingFrame(nil), svc.pending[holder]...)
	svc.deliveryMu.Unlock()

	if len(left) != 1 {
		t.Fatalf("queued frames after clearing one receipt = %d, want 1", len(left))
	}
	if left[0].Frame.Address != genuine.Sender {
		t.Errorf("the surviving frame is %q's, want the genuine recipient's", left[0].Frame.Address)
	}
}

// setPeerSession installs one session and a healthy peer entry for it, so
// the send door and the pending drain both accept the address.
func setPeerSession(svc *Service, session *peerSession) {
	now := time.Now().UTC()
	svc.peerMu.Lock()
	svc.health[session.address] = &peerHealth{
		Address: session.address, Connected: true, Direction: peerDirectionOutbound,
		State: peerStateHealthy, LastConnectedAt: now, LastUsefulReceiveAt: now,
	}
	svc.sessions[session.address] = session
	svc.peerMu.Unlock()
}

// A full session can park a receipt acknowledgment until a later connection.
func TestAckParkedByAFullSessionStillNamesItsReceiptAfterReconnect(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("peer-whose-queue-was-full")
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "parked-then-reconnected")

	// The first session cannot take the frame.
	older := sessionAtVersion(address, config.ProtocolVersion)
	older.sendCh = make(chan peerSendItem)
	setPeerSession(svc, older)
	svc.enqueueAckDeleteOnSession(older, address, ackDeleteForReceipt(receipt))

	svc.deliveryMu.RLock()
	parked := append([]pendingFrame(nil), svc.pending[address]...)
	svc.deliveryMu.RUnlock()
	if len(parked) != 1 {
		t.Fatalf("pending frames = %d, want the ack that did not fit", len(parked))
	}
	if parked[0].Frame.ReceiptSender != receipt.Sender {
		t.Fatalf("the parked ack lost the receipt's author: %+v", parked[0].Frame)
	}

	// The peer reconnects, and the drain hands it the full ack.
	current := sessionAtVersion(address, config.ProtocolVersion)
	setPeerSession(svc, current)
	svc.flushPendingPeerFrames(address)

	select {
	case item := <-current.sendCh:
		if item.ReceiptSender != receipt.Sender {
			t.Errorf("the drained ack does not name its receipt: %+v", item.Frame)
		}
		if err := identity.VerifyPayload(svc.identity.Address, identity.PublicKeyBase64(svc.identity.PublicKey),
			ackDeletePayloadForFrame(item.Frame), item.Signature); err != nil {
			t.Errorf("the drained ack does not verify: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the parked ack never reached the reconnected session")
	}
}

// The same rule at the other end of the queue: a drain that fails puts
// the frame BACK, and what goes back must still be the full one. The
// stamp is a view of the frame for one session, never a rewrite of what
// is stored.
func TestADrainThatFailsPutsTheFullAckBack(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("peer-that-cannot-take-it-yet")
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "queued-and-requeued")

	full := svc.buildAckDeleteFrame(ackDeleteForReceipt(receipt))
	svc.deliveryMu.Lock()
	svc.pending[address] = []pendingFrame{{Frame: full, QueuedAt: time.Now().UTC()}}
	svc.pendingKeys[pendingFrameKey(address, full)] = struct{}{}
	svc.deliveryMu.Unlock()

	older := sessionAtVersion(address, config.ProtocolVersion)
	older.sendCh = make(chan peerSendItem) // nothing fits
	setPeerSession(svc, older)
	svc.flushPendingPeerFrames(address)

	svc.deliveryMu.RLock()
	back := append([]pendingFrame(nil), svc.pending[address]...)
	svc.deliveryMu.RUnlock()
	if len(back) != 1 {
		t.Fatalf("pending frames after a failed drain = %d, want 1", len(back))
	}
	if back[0].Frame.ReceiptSender != receipt.Sender {
		t.Errorf("the re-queued ack came back downgraded: %+v", back[0].Frame)
	}
}
