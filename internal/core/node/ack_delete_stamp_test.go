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

// A queued ack outlives the session it was built for: the peer goes away,
// the frame waits, and the peer that comes back may be an older binary.
// The author is inside the signed payload, so a v2 frame handed to such a
// peer is not "a field it ignores" — it is a signature it cannot
// reproduce, which is scored as forgery.
func TestQueuedAckIsSignedForTheSessionThatFinallySendsIt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "queued-across-a-reconnect")

	// Built when the peer was current: the full form, author included.
	queued := svc.buildAckDeleteFrame(ackDeleteForReceipt(receipt))
	if queued.ReceiptSender != receipt.Sender {
		t.Fatalf("the queued frame lost the receipt's author: %+v", queued)
	}

	// The peer reconnects on an older binary.
	older := sessionAtVersion("peer-that-came-back-older", config.ProtocolVersionReceiptSenderAck-1)
	stamped := svc.stampAckDeleteForSession(older, legacyPeerSendItem(queued))
	if stamped.ReceiptSender != "" {
		t.Fatal("a v2 ack was handed to a peer that cannot verify it")
	}
	if err := identity.VerifyPayload(svc.identity.Address, identity.PublicKeyBase64(svc.identity.PublicKey),
		ackDeletePayload(stamped.Address, stamped.AckType, stamped.ID, stamped.Status),
		stamped.Signature); err != nil {
		t.Errorf("the older peer cannot verify the stamped ack: %v", err)
	}

	// And one that comes back current still gets the author.
	current := sessionAtVersion("peer-that-came-back-current", config.ProtocolVersionReceiptSenderAck)
	stamped = svc.stampAckDeleteForSession(current, legacyPeerSendItem(queued))
	if stamped.ReceiptSender != receipt.Sender {
		t.Errorf("a current peer was not told which receipt the ack names: %+v", stamped.Frame)
	}
	if err := identity.VerifyPayload(svc.identity.Address, identity.PublicKeyBase64(svc.identity.PublicKey),
		ackDeletePayloadForFrame(stamped.Frame), stamped.Signature); err != nil {
		t.Errorf("the current peer cannot verify the stamped ack: %v", err)
	}
}

// The stamp belongs to the door every session write passes, not to the
// build site: a producer that queues a frame cannot know which session
// will write it.
func TestEverySessionWriteStampsTheAckItCarries(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "through-the-door")
	queued := svc.buildAckDeleteFrame(ackDeleteForReceipt(receipt))

	older := sessionAtVersion("older-peer", config.ProtocolVersionReceiptSenderAck-1)
	now := time.Now().UTC()
	svc.peerMu.Lock()
	svc.health[older.address] = &peerHealth{
		Address: older.address, Connected: true, Direction: peerDirectionOutbound,
		State: peerStateHealthy, LastConnectedAt: now, LastUsefulReceiveAt: now,
	}
	svc.sessions[older.address] = older
	svc.peerMu.Unlock()
	if !svc.enqueueSessionSendItem(older, legacyPeerSendItem(queued)) {
		t.Fatal("the frame was not admitted to the session queue")
	}
	select {
	case item := <-older.sendCh:
		if item.ReceiptSender != "" {
			t.Error("the send door let a v2 ack through to a pre-floor peer")
		}
	case <-time.After(time.Second):
		t.Fatal("nothing reached the session queue")
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
	stamped := svc.stampAckDeleteForSession(sessionAtVersion("older-peer", config.ProtocolVersionReceiptSenderAck-1), legacyPeerSendItem(foreign))
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
	svc.clearPendingReceiptLocked(identityOf(forged))
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

// The bring-up ack path stamps for the session in front of it and parks
// the frame when that session's queue is full. Parking the STAMPED copy
// threw the receipt's author away permanently: the peer that drains the
// queue can be a current one, and it would get an ack that cannot say
// which receipt it holds — so with two contested receipts it deletes
// neither and keeps re-pushing both.
func TestAckParkedByAnOlderSessionStillNamesItsReceiptAfterTheUpgrade(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("peer-whose-queue-was-full")
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "parked-then-upgraded")

	// An older session whose queue cannot take the frame.
	older := sessionAtVersion(address, config.ProtocolVersionReceiptSenderAck-1)
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

	// The peer comes back current, and the drain hands it the full ack.
	current := sessionAtVersion(address, config.ProtocolVersionReceiptSenderAck)
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

	older := sessionAtVersion(address, config.ProtocolVersionReceiptSenderAck-1)
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
