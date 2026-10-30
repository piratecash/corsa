package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestDeliveryRetryBackoffSchedule pins the exponential schedule. Early
// attempts only make sense over a direct route; from the 5th attempt on the
// interval exceeds every transit dedup window (relay exact-dedup TTL 3 min,
// bloom rotation 5-10 min), so blind/relayed retries stop being absorbed.
func TestDeliveryRetryBackoffSchedule(t *testing.T) {
	t.Parallel()
	want := []time.Duration{
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		11 * time.Minute,
		11 * time.Minute, // capped
		11 * time.Minute,
	}
	for attempt, expected := range want {
		if got := deliveryRetryBackoff(attempt); got != expected {
			t.Fatalf("backoff(%d) = %v, want %v", attempt, got, expected)
		}
	}
}

// TestOwnOutgoingDMRegistersForRetryAndDeliveredStops verifies the
// scheduler bookkeeping: storing our own outgoing DM registers it in
// awaitingDelivered, and the delivered receipt from the recipient clears it.
func TestOwnOutgoingDMRegistersForRetryAndDeliveredStops(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))

	msg := incomingMessage{
		ID:        "retry-own-1",
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}
	stored, _, errCode := svc.storeIncomingMessage(msg, true)
	if !stored || errCode != "" {
		t.Fatalf("own outgoing DM must be stored, got stored=%v errCode=%q", stored, errCode)
	}

	svc.deliveryMu.RLock()
	entry, registered := svc.awaitingDelivered[svc.ownMessageIdentity("retry-own-1", recipientID.Address)]
	svc.deliveryMu.RUnlock()
	if !registered {
		t.Fatal("own outgoing DM must be registered in awaitingDelivered")
	}
	if entry.Envelope.Recipient != recipientID.Address {
		t.Fatalf("retry entry recipient = %q, want %q", entry.Envelope.Recipient, recipientID.Address)
	}

	// Delivered receipt from the recipient stops the retry.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "retry-own-1",
		Sender:      recipientID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	})

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingDelivered[svc.ownMessageIdentity("retry-own-1", recipientID.Address)]
	svc.deliveryMu.RUnlock()
	if stillThere {
		t.Fatal("delivered receipt must clear awaitingDelivered")
	}
}

// TestRetryDueDeliveriesResendsViaLiveRoute verifies that a due entry is
// re-sent: with the recipient registered as a live subscriber the retry
// arrives as a push_message, attempts grow and the next attempt moves out.
func TestRetryDueDeliveriesResendsViaLiveRoute(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	reader := attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7401))

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "retry-due-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[svc.ownMessageIdentity("retry-due-1", recipientID.Address)] = &deliveryRetryEntry{
		Envelope:      envelope,
		NextAttemptAt: now.Add(-time.Second), // already due
	}
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)

	pushed := readPushedFrame(t, reader, "push_message", 3*time.Second)
	if pushed.Item == nil || pushed.Item.ID != "retry-due-1" {
		t.Fatalf("unexpected push frame: %#v", pushed)
	}

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered[svc.ownMessageIdentity("retry-due-1", recipientID.Address)]
	svc.deliveryMu.RUnlock()
	if entry == nil {
		t.Fatal("entry must stay registered until a receipt arrives")
	}
	if entry.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", entry.Attempts)
	}
	if !entry.NextAttemptAt.After(now) {
		t.Fatalf("NextAttemptAt must move into the future, got %v (now %v)", entry.NextAttemptAt, now)
	}
}

// TestRetryDueDeliveriesDropsExpiredMessage verifies the ONE terminal
// verdict a message can reach on its own: its own TTL. Nothing else ends a
// delivery — no attempt cap, no age horizon — so this path carries the
// whole cleanup contract: the entry leaves the retry set, outbound goes
// terminal ("expired", visible through fetch_pending_messages), the
// pending rings drop every queued frame of the message, and the
// abandonment is journaled durably so a restart does not reseed the same
// chatlog row. (relayRetry is not part of it: an own message never owns a
// msg| entry.)
func TestRetryDueDeliveriesDropsExpiredMessage(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	journal := &stubFailureJournal{}
	svc.deliveryFailureJournal = journal

	now := time.Now().UTC()
	// Sent an hour ago to auto-delete after a minute: the delivery window
	// this message was given has closed.
	createdAt := now.Add(-time.Hour)
	// Both frames name their author, as every message frame the node queues
	// does: abandonment clears the frames of THIS message, and the id alone
	// does not say whose message a frame carries.
	frame := protocol.Frame{Type: "send_message", Topic: "dm", ID: "retry-cap-1", Address: svc.Address(), Recipient: "someone", Body: "x", CreatedAt: createdAt.Format(time.RFC3339)}
	// The relay fallback (sendRelayMessage → queuePeerFrame) can queue a
	// relay_message for the SAME message — abandonment must drop it too, or
	// a later flush would re-emit a finished delivery.
	relayFrame := protocol.Frame{Type: "relay_message", Topic: "dm", ID: "retry-cap-1", Address: svc.Address(), Recipient: "someone", Body: "x", CreatedAt: createdAt.Format(time.RFC3339)}
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[svc.ownMessageIdentity("retry-cap-1", "someone")] = &deliveryRetryEntry{
		Envelope: protocol.Envelope{
			ID: "retry-cap-1", Topic: "dm", Sender: svc.Address(), Recipient: "someone",
			CreatedAt: createdAt, TTLSeconds: 60,
		},
		Attempts:      3,
		NextAttemptAt: now.Add(-time.Second),
	}
	svc.pending["peer-x:64646"] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey("peer-x:64646", frame)] = struct{}{}
	svc.pending["peer-y:64646"] = []pendingFrame{{Frame: relayFrame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey("peer-y:64646", relayFrame)] = struct{}{}
	// No relayRetry entry: an own message never owns one, and abandoning it
	// must not touch an entry under its id, which would be some transit
	// DM's (TestOwnDeliveryWithdrawalLeavesTransitRelayRetryAlone).
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingDelivered[svc.ownMessageIdentity("retry-cap-1", "someone")]
	outboundState := svc.outbound[svc.ownMessageIdentity("retry-cap-1", "someone")]
	pendingLeft := len(svc.pending["peer-x:64646"])
	relayPendingLeft := len(svc.pending["peer-y:64646"])
	svc.deliveryMu.RUnlock()
	if stillThere {
		t.Fatal("an expired message must leave the retry set")
	}
	if outboundState.Status != "expired" {
		t.Fatalf("outbound must go terminal expired, got %q", outboundState.Status)
	}
	if pendingLeft != 0 {
		t.Fatalf("pending ring must drop the abandoned send_message, %d left", pendingLeft)
	}
	if relayPendingLeft != 0 {
		t.Fatalf("pending ring must drop the queued relay_message of the abandoned id, %d left", relayPendingLeft)
	}
	journal.mu.Lock()
	failed := append([]domain.MessageRef(nil), journal.failed...)
	journal.mu.Unlock()
	if len(failed) != 1 || failed[0] != deliveryMessageRef(svc.ownMessageIdentity("retry-cap-1", "someone")) {
		t.Fatalf("abandonment must be journaled, got %v", failed)
	}
}

// TestNoAttemptCapEndsAMessage is the guard on the decision itself: a
// message that has been re-sent far past the old cap of twenty is still
// being delivered. The cap that remains applies to seen receipts, which
// the far side re-triggers; a message has nobody to re-trigger it.
func TestNoAttemptCapEndsAMessage(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[svc.ownMessageIdentity("uncapped-1", "someone")] = &deliveryRetryEntry{
		Envelope:      protocol.Envelope{ID: "uncapped-1", Topic: "dm", Sender: svc.Address(), Recipient: "someone", CreatedAt: now},
		Attempts:      svc.deliveryRetryMaxAttempts() * 10,
		NextAttemptAt: now.Add(-time.Second),
	}
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingDelivered[svc.ownMessageIdentity("uncapped-1", "someone")]
	svc.deliveryMu.RUnlock()
	if !stillThere {
		t.Fatal("the engine gave up on a message for no reason but the number of tries")
	}
}

// TestSeenReceiptRegistersAwaitingAckAndSeenAckClears covers the
// seen-sender (recipient-of-the-DM) side: sending a seen receipt registers
// it in awaitingSeenAck and retries until the original sender's seen_ack
// arrives.
func TestSeenReceiptRegistersAwaitingAckAndSeenAckClears(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	counterpartyID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Our node sends the seen receipt (the local-RPC mark-seen path lands
	// here): Sender = us, Recipient = the original message sender.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "seen-ack-1",
		Sender:      svc.Address(),
		Recipient:   counterpartyID.Address,
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	})

	svc.deliveryMu.RLock()
	_, registered := svc.awaitingSeenAck[messageIdentity{MessageID: "seen-ack-1", Sender: counterpartyID.Address, Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if !registered {
		t.Fatal("outgoing seen receipt must be registered in awaitingSeenAck")
	}

	// The retry tick re-distributes the seen receipt to the live route.
	reader := attachPushObserver(t, svc, counterpartyID.Address, netcore.ConnID(7402))
	now := time.Now().UTC().Add(deliveryRetryBackoff(0) + time.Second)
	svc.retryDueDeliveries(now)
	frame := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if frame.Receipt == nil || frame.Receipt.MessageID != "seen-ack-1" || frame.Receipt.Status != protocol.ReceiptStatusSeen {
		t.Fatalf("unexpected retried seen receipt: %#v", frame)
	}

	// The seen_ack from the original sender stops the retry.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "seen-ack-1",
		Sender:      counterpartyID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	})

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingSeenAck[messageIdentity{MessageID: "seen-ack-1", Sender: counterpartyID.Address, Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if stillThere {
		t.Fatal("seen_ack must clear awaitingSeenAck")
	}
}

// TestSeenReceiptTriggersSeenAckFromOriginalSender covers the original
// sender's side: every arrival of a seen receipt addressed to us — first or
// duplicate — must answer with a seen_ack so the seen-sender's retries stop.
func TestSeenReceiptTriggersSeenAckFromOriginalSender(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	seenSenderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	reader := attachPushObserver(t, svc, seenSenderID.Address, netcore.ConnID(7403))

	// This node originated the message, so the seen receipt is solicited.
	svc.sentDMIDs.Add(deliveryMessageRef(svc.ownMessageIdentity("seen-ack-2", seenSenderID.Address)).Key())
	seen := protocol.DeliveryReceipt{
		MessageID:   "seen-ack-2",
		Sender:      seenSenderID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	}

	svc.storeDeliveryReceipt(seen)
	first := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if first.Receipt == nil || first.Receipt.MessageID != "seen-ack-2" || first.Receipt.Status != protocol.ReceiptStatusSeenAck {
		t.Fatalf("expected seen_ack push, got %#v", first)
	}

	// Duplicate seen (the seen-sender retried because the first ack got
	// lost) — must re-send the ack even though seenReceipts dedupes it.
	svc.storeDeliveryReceipt(seen)
	second := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if second.Receipt == nil || second.Receipt.MessageID != "seen-ack-2" || second.Receipt.Status != protocol.ReceiptStatusSeenAck {
		t.Fatalf("expected re-sent seen_ack push, got %#v", second)
	}
}

// TestRegisterDeliveryOutboxReseedsUndelivered verifies the durable arm:
// after a restart the scheduler is reseeded from the chatlog-backed outbox.
func TestRegisterDeliveryOutboxReseedsUndelivered(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	outbox := stubDeliveryOutbox{envelopes: []protocol.Envelope{
		{ID: "reseed-1", Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", Payload: []byte("x"), CreatedAt: now},
		{ID: "reseed-2", Topic: "dm", Sender: svc.Address(), Recipient: "peer-b", Payload: []byte("y"), CreatedAt: now},
	}}
	svc.RegisterDeliveryOutbox(outbox)
	// Captured AFTER reseed: the reseed uses its own time.Now() internally, so
	// a "due immediately" entry has NextAttemptAt <= this mark, whereas the
	// old +first-backoff behaviour would push it ~30s past it.
	afterReseed := time.Now().UTC()

	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	for _, envelope := range outbox.envelopes {
		id := envelope.ID
		entry, ok := svc.awaitingDelivered[messageIdentityOf(envelope)]
		if !ok {
			t.Fatalf("outbox envelope %s must be reseeded into awaitingDelivered", id)
		}
		// Reseeded entries must be DUE IMMEDIATELY, so the first retry tick
		// after restart evaluates reachability and sends right away instead
		// of idling a full backoff interval.
		if entry.NextAttemptAt.After(afterReseed) {
			t.Fatalf("reseeded %s must be due immediately; NextAttemptAt=%v > %v", id, entry.NextAttemptAt, afterReseed)
		}
		// And they must not claim anything about the RECIPIENT: a restart
		// has looked at nobody. holdUnconfirmed is right — this process has
		// put nothing on the wire — while holdUnreachable would be a
		// verdict the reseed is in no position to reach, and would let a
		// route refresh reset the backoff as if the peer had just returned.
		if entry.Hold != holdUnconfirmed {
			t.Fatalf("reseeded %s must start unconfirmed, got hold reason %d", id, entry.Hold)
		}
	}
}

// TestDMOptOutStillReseedsItsOwnOutgoing: opt-out refuses INCOMING
// messages; it does not stop this node from sending.
//
// Skipping the outbox scan for such a node looked like saving work on a
// relay that holds no DM backlog, and it was in fact dropping ITS OWN
// backlog: an outgoing DM was retried until the process stopped and then
// forgotten at the next start, with the row left at `sent` in the chatlog
// and nothing — not even the recipient coming online — able to put it on
// the wire again.
func TestDMOptOutStillReseedsItsOwnOutgoing(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.DisableDirectMessages = true

	const owed = protocol.MessageID("outgoing-from-an-optout-node")
	outbox := newEmissionOutbox(OutboxEntry{Envelope: protocol.Envelope{
		ID: owed, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		Payload: []byte("x"), CreatedAt: time.Now().UTC(),
	}})
	svc.RegisterDeliveryOutbox(optOutOutbox{outbox})

	svc.deliveryMu.RLock()
	_, reseeded := svc.awaitingDelivered[svc.ownMessageIdentity(owed, "peer-a")]
	svc.deliveryMu.RUnlock()
	if !reseeded {
		t.Error("an opt-out node lost its own undelivered message across the restart; nothing will ever send it again")
	}
	// The durable failure journal is still wired, so an abandoned outbound
	// DM terminalizes instead of resurrecting.
	if svc.deliveryFailureJournal == nil {
		t.Error("the failure journal was not registered")
	}
}

// optOutOutbox is an emissionOutbox that also carries the failure journal,
// which an opt-out node needs for its own outbound sends.
type optOutOutbox struct{ *emissionOutbox }

func (optOutOutbox) MarkDeliveryFailedRef(domain.MessageRef) error { return nil }

// TestSeenAckPushedToSubscriberAtMinimumProtocolVersion pins that seen_ack
// reaches every subscriber the node serves: the oldest connection it accepts
// advertises MinimumProtocolVersion, and that build already understands the
// status, so there is no version below which the ack could be withheld.
func TestSeenAckPushedToSubscriberAtMinimumProtocolVersion(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	seenSenderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	reader := attachPushObserverWithVersion(t, svc, seenSenderID.Address, netcore.ConnID(7404), config.MinimumProtocolVersion)

	svc.sentDMIDs.Add(deliveryMessageRef(svc.ownMessageIdentity("seen-ack-3", seenSenderID.Address)).Key()) // we sent this message; the seen receipt is solicited
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "seen-ack-3",
		Sender:      seenSenderID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	})

	frame := readPushedFrame(t, reader, "push_delivery_receipt", 3*time.Second)
	if frame.Receipt == nil || frame.Receipt.MessageID != "seen-ack-3" || frame.Receipt.Status != protocol.ReceiptStatusSeenAck {
		t.Fatalf("expected seen_ack push to a v%d subscriber, got %#v", config.MinimumProtocolVersion, frame)
	}
}

// TestReceiptPushToSubscriberWithoutConnRemovesSubscriber pins that every
// receipt status treats a subscriber whose connection is already gone the
// same way: the write finds no conn and drops the subscription. seen_ack is
// not special — skipping it would keep a dead subscription alive.
//
// Nothing is kept for a later replay. Removing the recipient's last
// subscription drops its receipt backlog (dropReceiptBacklogForOfflineRecipients),
// and a seen_ack never enters that backlog at all. Recovery is end-to-end
// retry: the message sender re-sends until delivered/seen comes back, and
// the seen-sender re-sends seen until its seen_ack comes back.
func TestReceiptPushToSubscriberWithoutConnRemovesSubscriber(t *testing.T) {
	t.Parallel()
	statuses := []string{
		protocol.ReceiptStatusDelivered,
		protocol.ReceiptStatusSeen,
		protocol.ReceiptStatusSeenAck,
	}
	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			const recipient = "recipient-whose-conn-is-gone"

			// No net core is registered under this ConnID: the conn was
			// unregistered while the subscription still pointed at it.
			svc.gossipMu.Lock()
			svc.subs[recipient] = map[string]*subscriber{
				"ghost": {id: "ghost", recipient: recipient, connID: domain.ConnID(7405)},
			}
			svc.gossipMu.Unlock()

			svc.pushReceiptToSubscribers(protocol.DeliveryReceipt{
				MessageID:   "receipt-to-gone-conn",
				Sender:      svc.Address(),
				Recipient:   recipient,
				Status:      status,
				DeliveredAt: time.Now().UTC(),
			})
			svc.WaitBackground()

			if svc.hasSubscriber(recipient) {
				t.Fatalf("a %s push to a subscriber without a conn left the subscription in place", status)
			}
		})
	}
}

// TestRelayHopForReceiptDirection pins the status-aware relay hop choice:
// delivered/seen travel BACK toward the original message sender (the
// reverse path, ReceiptForwardTo), while seen_ack travels in the ORIGINAL
// message direction (ForwardedTo) — routing it through the reverse path
// would bounce it back to the original sender instead of the seen-sender.
func TestRelayHopForReceiptDirection(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.relayStates.store(&relayForwardState{
		Message:          domain.MessageRef{Topic: "dm", ID: "dir-1", Sender: "original-author", Recipient: "original-reader"},
		ReceiptForwardTo: "hop-back",
		ForwardedTo:      "hop-forward",
		RemainingTTL:     100,
	})

	if got := svc.relayHopForReceipt(protocol.DeliveryReceipt{MessageID: "dir-1", Sender: "original-reader", Recipient: "original-author", Status: protocol.ReceiptStatusDelivered}); got != "hop-back" {
		t.Fatalf("delivered hop = %q, want hop-back", got)
	}
	if got := svc.relayHopForReceipt(protocol.DeliveryReceipt{MessageID: "dir-1", Sender: "original-reader", Recipient: "original-author", Status: protocol.ReceiptStatusSeen}); got != "hop-back" {
		t.Fatalf("seen hop = %q, want hop-back", got)
	}
	if got := svc.relayHopForReceipt(protocol.DeliveryReceipt{MessageID: "dir-1", Sender: "original-author", Recipient: "original-reader", Status: protocol.ReceiptStatusSeenAck}); got != "hop-forward" {
		t.Fatalf("seen_ack hop = %q, want hop-forward", got)
	}
}

// TestSeenAckNotStoredInReceipts pins the wire-only contract: seen_ack is
// deduped via seenReceipts but never appended to s.receipts, so it cannot
// leak through fetch_delivery_receipts or the auth-time backlog replay
// (which has no version gate).
func TestSeenAckNotStoredInReceipts(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	transitAck := protocol.DeliveryReceipt{
		MessageID:   "leak-1",
		Sender:      "third-party-a",
		Recipient:   "third-party-b",
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	}
	svc.storeDeliveryReceipt(transitAck)

	localAck := protocol.DeliveryReceipt{
		MessageID:   "leak-2",
		Sender:      "third-party-a",
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	}
	svc.storeDeliveryReceipt(localAck)

	for _, recipient := range []string{"third-party-b", svc.Address()} {
		reply := svc.fetchDeliveryReceiptsFrame(recipient)
		if reply.Count != 0 || len(reply.Receipts) != 0 {
			t.Fatalf("seen_ack leaked into fetch_delivery_receipts for %s: %#v", recipient, reply.Receipts)
		}
	}
}

// TestRetryDueDeliveriesDropsExpiredTTL pins the TTL contract on the retry
// engine: a DM whose ttl_seconds lifetime has elapsed is dropped instead of
// being re-sent (docs/protocol/messaging.md — TTL also bounds delivery).
func TestRetryDueDeliveriesDropsExpiredTTL(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[svc.ownMessageIdentity("retry-ttl-1", "someone")] = &deliveryRetryEntry{
		Envelope: protocol.Envelope{
			ID: "retry-ttl-1", Topic: "dm",
			Sender: svc.Address(), Recipient: "someone",
			CreatedAt:  now.Add(-2 * time.Minute),
			TTLSeconds: 60, // expired a minute ago
		},
		NextAttemptAt: now.Add(-time.Second),
	}
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingDelivered[svc.ownMessageIdentity("retry-ttl-1", "someone")]
	outboundState := svc.outbound[svc.ownMessageIdentity("retry-ttl-1", "someone")]
	svc.deliveryMu.RUnlock()
	if stillThere {
		t.Fatal("expired-TTL entry must be dropped from the retry set")
	}
	if outboundState.Status != "expired" {
		t.Fatalf("outbound must go terminal expired, got %q", outboundState.Status)
	}
}

// TestSeenAckJournalReseedAndConfirm pins the durable arm of the seen
// retry: unconfirmed seen receipts from the journal are reseeded into
// awaitingSeenAck at registration, and an arriving seen_ack is persisted
// back through MarkSeenConfirmed.
func TestSeenAckJournalReseedAndConfirm(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	journal := &stubSeenAckJournal{unconfirmed: []protocol.DeliveryReceipt{{
		MessageID:   "journal-1",
		Sender:      svc.Address(),
		Recipient:   "remote-sender",
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	}}}
	svc.RegisterDeliveryOutbox(journal)

	svc.deliveryMu.RLock()
	_, reseeded := svc.awaitingSeenAck[messageIdentity{MessageID: "journal-1", Sender: "remote-sender", Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if !reseeded {
		t.Fatal("unconfirmed seen receipt must be reseeded into awaitingSeenAck")
	}

	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "journal-1",
		Sender:      "remote-sender",
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	})
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, stillThere := svc.awaitingSeenAck[messageIdentity{MessageID: "journal-1", Sender: "remote-sender", Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if stillThere {
		t.Fatal("seen_ack must clear the reseeded awaitingSeenAck entry")
	}
	journal.mu.Lock()
	confirmed := append([]domain.MessageRef(nil), journal.confirmed...)
	journal.mu.Unlock()
	if len(confirmed) != 1 || confirmed[0] != (domain.MessageRef{Topic: "dm", ID: "journal-1", Sender: "remote-sender", Recipient: svc.Address()}) {
		t.Fatalf("seen_ack must be journaled via MarkSeenConfirmed, got %v", confirmed)
	}
}

// TestSeenAckRejectedFromUnexpectedSender pins the sender binding: a
// seen_ack only counts when it comes from the identity the seen receipt was
// addressed to. A spoofed ack from any other identity must neither stop the
// retry nor poison the seenReceipts dedupe key — the genuine ack arriving
// later must still be accepted.
func TestSeenAckRejectedFromUnexpectedSender(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	journal := &stubSeenAckJournal{}
	svc.RegisterDeliveryOutbox(journal)

	originalSenderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Our seen receipt toward the original sender registers the retry.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "bind-1",
		Sender:      svc.Address(),
		Recipient:   originalSenderID.Address,
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	})

	// Spoofed ack from a different identity: dropped entirely.
	stored := svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "bind-1",
		Sender:      "attacker-identity",
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	})
	if stored.stored {
		t.Fatal("spoofed seen_ack must be rejected")
	}
	svc.WaitBackground()
	svc.deliveryMu.RLock()
	_, stillAwaiting := svc.awaitingSeenAck[messageIdentity{MessageID: "bind-1", Sender: originalSenderID.Address, Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if !stillAwaiting {
		t.Fatal("spoofed seen_ack must not stop the seen retry")
	}
	journal.mu.Lock()
	confirmed := len(journal.confirmed)
	journal.mu.Unlock()
	if confirmed != 0 {
		t.Fatalf("spoofed seen_ack must not be journaled, got %d confirmations", confirmed)
	}

	// The genuine ack must still be accepted (the dedupe key was not
	// poisoned by the rejected spoof).
	stored = svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "bind-1",
		Sender:      originalSenderID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC(),
	})
	if !stored.stored {
		t.Fatal("genuine seen_ack must be accepted after a rejected spoof")
	}
	svc.WaitBackground()
	svc.deliveryMu.RLock()
	_, stillAwaiting = svc.awaitingSeenAck[messageIdentity{MessageID: "bind-1", Sender: originalSenderID.Address, Recipient: svc.Address()}]
	svc.deliveryMu.RUnlock()
	if stillAwaiting {
		t.Fatal("genuine seen_ack must clear the seen retry")
	}
	journal.mu.Lock()
	confirmedIDs := append([]domain.MessageRef(nil), journal.confirmed...)
	journal.mu.Unlock()
	if len(confirmedIDs) != 1 || confirmedIDs[0] != (domain.MessageRef{Topic: "dm", ID: "bind-1", Sender: originalSenderID.Address, Recipient: svc.Address()}) {
		t.Fatalf("genuine seen_ack must be journaled exactly once, got %v", confirmedIDs)
	}
}

// TestLocalSendDeliveryReceiptRejectsSeenAck pins the wire-only contract on
// the local command surface: send_delivery_receipt accepts only the
// user-level statuses (delivered/seen) — injecting seen_ack via local RPC
// would let a local console fake the remote confirmation and silence the
// seen retry without the original sender ever acking.
func TestLocalSendDeliveryReceiptRejectsSeenAck(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	reply := svc.HandleLocalFrame(protocol.Frame{
		Type:        "send_delivery_receipt",
		ID:          "local-ack-1",
		Address:     svc.Address(),
		Recipient:   "remote-identity",
		Status:      protocol.ReceiptStatusSeenAck,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})
	if reply.Type != "error" || reply.Code != protocol.ErrCodeInvalidSendDeliveryReceipt {
		t.Fatalf("local seen_ack must be rejected, got %#v", reply)
	}
}

// stubDeliveryOutbox is a DeliveryOutbox stub serving a fixed slice. The
// rows report themselves emitted, matching a store that carries no mark.
type stubDeliveryOutbox struct{ envelopes []protocol.Envelope }

func (s stubDeliveryOutbox) SentMessageRefs(int) ([]domain.MessageRef, error) { return nil, nil }

func (s stubDeliveryOutbox) UndeliveredOutgoing() ([]OutboxEntry, error) {
	rows := make([]OutboxEntry, 0, len(s.envelopes))
	for _, envelope := range s.envelopes {
		rows = append(rows, OutboxEntry{Envelope: envelope, Emitted: true})
	}
	return rows, nil
}

// TestFailDeliverySkipsWhenReceiptAlreadyArrived pins the late-receipt
// re-check: between the abandon decision (one deliveryMu window) and
// failDelivery (a later window) a delivered receipt can land. failDelivery
// must then do nothing — no terminal outbound overwrite, no failure
// journal entry — the confirmed delivery wins.
func TestFailDeliverySkipsWhenReceiptAlreadyArrived(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	journal := &stubFailureJournal{}
	svc.deliveryFailureJournal = journal

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	now := time.Now().UTC()
	envelope := protocol.Envelope{ID: "race-1", Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address, CreatedAt: now}
	svc.sentDMIDs.Add(envelopeRef(envelope).Key()) // this node originated the message; its delivered receipt is solicited

	// The receipt lands in the unlocked window before failDelivery runs.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   "race-1",
		Sender:      recipientID.Address,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: now,
	})

	svc.failDelivery(envelope, "failed", "delivery retries exhausted")
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	outboundState, outboundExists := svc.outbound[messageIdentityOf(envelope)]
	svc.deliveryMu.RUnlock()
	if outboundExists && (outboundState.Status == "failed" || outboundState.Status == "expired") {
		t.Fatalf("failDelivery must not overwrite a confirmed delivery, got %#v", outboundState)
	}
	journal.mu.Lock()
	failed := len(journal.failed)
	journal.mu.Unlock()
	if failed != 0 {
		t.Fatalf("failDelivery must not journal abandonment after a receipt, got %d entries", failed)
	}
}

// stubFailureJournal records MarkDeliveryFailed calls.
type stubFailureJournal struct {
	mu     sync.Mutex
	failed []domain.MessageRef
}

func (s *stubFailureJournal) MarkDeliveryFailedRef(id domain.MessageRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = append(s.failed, id)
	return nil
}

// stubSeenAckJournal implements DeliveryOutbox + SeenAckJournal.
type stubSeenAckJournal struct {
	mu          sync.Mutex
	unconfirmed []protocol.DeliveryReceipt
	confirmed   []domain.MessageRef
}

func (s *stubSeenAckJournal) UndeliveredOutgoing() ([]OutboxEntry, error) { return nil, nil }

func (s *stubSeenAckJournal) SentMessageRefs(int) ([]domain.MessageRef, error) { return nil, nil }

func (s *stubSeenAckJournal) UnconfirmedSeen() ([]protocol.DeliveryReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.DeliveryReceipt(nil), s.unconfirmed...), nil
}

func (s *stubSeenAckJournal) MarkSeenConfirmedRef(id domain.MessageRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confirmed = append(s.confirmed, id)
	return nil
}
