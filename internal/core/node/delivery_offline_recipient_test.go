package node

import (
	"bufio"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestHeldDeliverySurvivesLongOfflineRecipient is the regression for the
// reported "message stays 'sent' forever after the recipient comes back".
//
// The recipient is unreachable for longer than the attempt budget would
// cover. Every tick in that window is a HOLD: the reachability gate refuses
// to emit, so nothing reaches the wire. A tick that sent nothing must not
// spend an attempt — otherwise a night of being offline exhausts the budget
// without the network having been used once, failDelivery journals the id,
// and the message can never go out again, not even when the recipient
// returns.
func TestHeldDeliverySurvivesLongOfflineRecipient(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "offline-hold-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: start, StoredAt: start,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, start, true)
	svc.deliveryMu.Unlock()

	// Twice the attempt budget worth of ticks, all with the recipient
	// unreachable: no route, no subscriber.
	now := start
	for i := 0; i < 2*svc.deliveryRetryMaxAttempts(); i++ {
		now = now.Add(11 * time.Minute)
		svc.retryDueDeliveries(now)
	}

	svc.deliveryMu.RLock()
	entry, stillAwaiting := svc.awaitingDelivered["offline-hold-1"]
	svc.deliveryMu.RUnlock()
	if !stillAwaiting {
		t.Fatal("a message that never reached the wire must stay in awaitingDelivered: " +
			"the attempt budget measures emissions, not ticks spent waiting for the recipient")
	}
	if entry.Attempts != 0 {
		t.Fatalf("held ticks must not spend attempts; Attempts = %d, want 0", entry.Attempts)
	}
	if entry.Hold == holdNone {
		t.Fatal("an unreachable recipient must leave the entry Held so the reachability kick can wake it")
	}

	// The recipient comes back: a live subscriber appears, the kick re-arms
	// the held entry, and the very next tick puts it on the wire.
	reader := attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7411))
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipientID.Address): {},
	})
	svc.retryDueDeliveries(now.Add(time.Second))

	pushed := readPushedFrame(t, reader, "push_message", 3*time.Second)
	if pushed.Item == nil || pushed.Item.ID != "offline-hold-1" {
		t.Fatalf("the returning recipient must receive the held message, got %#v", pushed)
	}
}

// TestHeldDeliveryFlushIsOrderedOldestFirst pins the queue discipline of
// the flush. When a recipient returns after an offline stretch, the
// messages they missed must arrive in the order they were written — one at
// a time, the next one leaving only once the previous is confirmed. The
// sends are handed to background goroutines, so a tick that emitted the
// whole backlog at once would deliver it in whatever order the scheduler
// happened to run them.
func TestHeldDeliveryFlushIsOrderedOldestFirst(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	ids := []protocol.MessageID{"ordered-1", "ordered-2", "ordered-3", "ordered-4"}
	svc.deliveryMu.Lock()
	for i, id := range ids {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
	}
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7412)))
	now := start.Add(time.Minute)

	for _, want := range ids {
		svc.retryDueDeliveries(now)
		stream.expect(t, want)

		// Nothing else may leave while this one is unconfirmed: a second
		// tick inside the queue window must find the slot taken.
		svc.retryDueDeliveries(now.Add(time.Second))
		stream.expectQuiet(t, 150*time.Millisecond)

		// The recipient confirms it, which frees the slot and pulls the
		// next message forward.
		svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
			MessageID:   want,
			Sender:      recipientID.Address,
			Recipient:   svc.Address(),
			Status:      protocol.ReceiptStatusDelivered,
			DeliveredAt: now,
		})
		now = now.Add(2 * time.Second)
	}
}

// TestInboundConnectDoesNotWakeDeliveriesBeforeAuthOK pins WHERE the
// inbound kick may fire. trackInboundConnect runs inside handleAuthSession,
// before auth_ok has been written, so a retry tick woken there could put a
// push_message on the connection ahead of the handshake reply the peer is
// still waiting for. The kick therefore lives at the same boundary as the
// backlog replay — after sendHandshakeReplyViaNetwork returns.
func TestInboundConnectDoesNotWakeDeliveriesBeforeAuthOK(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const connID = netcore.ConnID(7419)
	attachPushObserver(t, svc, recipientID.Address, connID)
	// Without a live connection trackInboundConnect returns before doing
	// anything, and this test would prove nothing at all.
	if svc.Network().RemoteAddr(connID) == "" {
		t.Fatal("the test connection is not registered, so trackInboundConnect would exit early and assert nothing")
	}

	future := time.Now().UTC().Add(10 * time.Minute)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["pre-auth-hold"] = &deliveryRetryEntry{
		Envelope:      protocol.Envelope{ID: "pre-auth-hold", Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address},
		Attempts:      5,
		NextAttemptAt: future,
		Hold:          holdUnreachable,
		LastEmittedAt: time.Now().UTC().Add(-time.Hour),
	}
	svc.deliveryMu.Unlock()

	svc.trackInboundConnect(connID, "10.9.9.9:64646", domain.PeerIdentityFromWire(recipientID.Address))

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["pre-auth-hold"]
	attempts, next := entry.Attempts, entry.NextAttemptAt
	svc.deliveryMu.RUnlock()
	if attempts != 5 || !next.Equal(future) {
		t.Fatalf("trackInboundConnect woke a held delivery before auth_ok was written: Attempts=%d next=%v", attempts, next)
	}

	// And the kick itself still works — this test must not be passing
	// because the wake-up is broken everywhere.
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipientID.Address): {},
	})
	svc.deliveryMu.RLock()
	woken := svc.awaitingDelivered["pre-auth-hold"].NextAttemptAt
	svc.deliveryMu.RUnlock()
	if woken.Equal(future) {
		t.Fatal("the kick did not wake the held delivery, so the assertion above proves nothing")
	}
}

// TestDroppedAnnouncementIsRetried covers the bus shedding the event. The
// inbox is bounded and a full one drops silently, and this is the one fact
// nothing else restates: the receipt is the next thing to happen, and a
// lost receipt would leave the badge on "queued" for good. So a dropped
// announcement must not consume the claim.
func TestDroppedAnnouncementIsRetried(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	// A subscriber that never drains: its inbox fills and every further
	// event for it is shed.
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	bus.Subscribe(ebus.TopicMessageEmitted, func(protocol.LocalChangeEvent) {
		<-block
	})

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "dropped-announcement", Topic: "dm",
		Sender: svc.Address(), Recipient: "peer-a",
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	// Publish until the blocked subscriber's inbox overflows. The claim is
	// released by hand between attempts because a DELIVERED announcement
	// is correctly one-shot — what is under test is the other branch.
	for i := 0; i < 4096; i++ {
		svc.deliveryMu.Lock()
		svc.awaitingDelivered["dropped-announcement"].Announced = false
		svc.deliveryMu.Unlock()

		svc.publishMessagesEmitted([]protocol.Envelope{envelope})

		svc.deliveryMu.RLock()
		announced := svc.awaitingDelivered["dropped-announcement"].Announced
		svc.deliveryMu.RUnlock()
		if !announced {
			return // the drop gave the claim back, which is the contract
		}
	}
	t.Fatal("the bus never shed an event, so this test proved nothing about the drop path")
}

// TestOriginSendToADeadSubscriberIsNotSent covers the FIRST send, not the
// retry. Having a subscriber was treated as proof of delivery, but the push
// runs on a background goroutine and its answer was thrown away: a
// connection already gone left the message recorded as sent, shown as sent,
// and with the hold cleared so no reconnect could wake it.
func TestOriginSendToADeadSubscriberIsNotSent(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// A subscriber whose connection is NOT in the registry: reachable
	// enough to pass the gate, dead by the time the writer is asked.
	svc.gossipMu.Lock()
	svc.subs[recipientID.Address] = map[string]*subscriber{
		"ghost": {id: "ghost", recipient: recipientID.Address, connID: netcore.ConnID(999999)},
	}
	svc.gossipMu.Unlock()

	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        "ghost-subscriber-1",
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("own outgoing DM must be stored, got stored=%v errCode=%q", stored, errCode)
	}
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered["ghost-subscriber-1"]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("the message left the retry engine, so nothing will send it later")
	}
	if entry.Hold == holdNone {
		t.Error("a message no writer accepted is recorded as on the wire, so no reconnect will wake it")
	}
	if entry.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0: nothing was handed to a writer", entry.Attempts)
	}
	if !entry.LastEmittedAt.IsZero() {
		t.Error("a message no writer accepted is holding its recipient's queue slot")
	}
}

// TestDeadSubscriberIsReportedQueuedToTheSender is the user-visible half of
// TestOriginSendToADeadSubscriberIsNotSent. Getting the internal hold right
// is worth nothing if the person still reads "sent": the status must be
// answered by the CONFIRMATION, not by the conservative Emitted flag, which
// is already true for an attempt no writer took.
func TestDeadSubscriberIsReportedQueuedToTheSender(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	events := make(chan protocol.LocalChangeEvent, 4)
	bus.Subscribe(ebus.TopicMessageNew, func(event protocol.LocalChangeEvent) {
		events <- event
	})

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// Reachable enough to pass the gate, dead by the time the writer is
	// asked: the connection is not in the registry.
	svc.gossipMu.Lock()
	svc.subs[recipientID.Address] = map[string]*subscriber{
		"ghost": {id: "ghost", recipient: recipientID.Address, connID: netcore.ConnID(999998)},
	}
	svc.gossipMu.Unlock()

	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))
	frame := protocol.Frame{
		Type: "send_message", Topic: "dm",
		ID: "ghost-status-1", Address: svc.Address(), Recipient: recipientID.Address,
		Flag: string(protocol.MessageFlagImmutable), Body: body,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	reply := svc.storeMessageFrame(frame)
	svc.WaitBackground()

	if reply.Type != "message_stored" {
		t.Fatalf("send = %q, want message_stored", reply.Type)
	}
	if reply.Status != protocol.MessageStatusQueued {
		t.Errorf("reply status = %q, want %q: no writer took this frame", reply.Status, protocol.MessageStatusQueued)
	}
	select {
	case event := <-events:
		if event.Status != protocol.MessageStatusQueued {
			t.Errorf("message.new status = %q, want %q", event.Status, protocol.MessageStatusQueued)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("storing our own DM published no message.new event")
	}

	// The status stays queued on a later read too — the chatlog row is
	// the authority once the echo is gone.
	if got := svc.storedMessageStatus("ghost-status-1"); got != protocol.MessageStatusQueued {
		t.Errorf("storedMessageStatus = %q, want %q", got, protocol.MessageStatusQueued)
	}
}

// TestUnconfirmedHeadIsNotOvertaken is the ordering counterpart. The queue
// head keeps its place until a sink CONFIRMS it, not merely until the
// conservative Emitted flag is set — that flag turns true before the frame
// is written, so keying the rule on it let a newer message pass one whose
// writer had refused it.
func TestUnconfirmedHeadIsNotOvertaken(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	for i, id := range []protocol.MessageID{"unconfirmed-head", "younger-behind-it"} {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
	}
	// The head was attempted — Emitted is already true, as it is for every
	// attempt — but no sink ever confirmed, and its next try is not due.
	head := svc.awaitingDelivered["unconfirmed-head"]
	head.LastEmittedAt = start.Add(-time.Hour)
	head.Hold = holdUnconfirmed
	head.NextAttemptAt = start.Add(2 * time.Minute)
	svc.awaitingDelivered["younger-behind-it"].NextAttemptAt = start
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7422)))
	// Attaching the observer changed the mesh, which is a reason to re-try
	// overdue entries. Let the tick see that, then restate the schedule this
	// test is about: the subject here is the QUEUE ORDER, not the wake-up.
	svc.retryDueDeliveries(start)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["unconfirmed-head"].NextAttemptAt = start.Add(2 * time.Minute)
	svc.awaitingDelivered["younger-behind-it"].NextAttemptAt = start
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(start.Add(time.Minute))
	stream.expectQuiet(t, 150*time.Millisecond)

	// Once the head is due again it goes first, as it always should have.
	svc.retryDueDeliveries(start.Add(3 * time.Minute))
	stream.expect(t, "unconfirmed-head")
}

// TestUnconfirmedDeliveryIsWrittenDown covers the RESTART. The in-memory
// hold says "queued" and dies with the process; the chatlog row says "sent"
// for every outgoing message. Without a durable mark, a dispatch every
// writer refused reopened as sent — the reported symptom in a narrower
// form.
func TestUnconfirmedDeliveryIsWrittenDown(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// Reachable enough to pass the gate, dead at the writer.
	svc.gossipMu.Lock()
	svc.subs[recipientID.Address] = map[string]*subscriber{
		"ghost": {id: "ghost", recipient: recipientID.Address, connID: netcore.ConnID(999997)},
	}
	svc.gossipMu.Unlock()

	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        "unconfirmed-durable-1",
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("own outgoing DM must be stored, got stored=%v errCode=%q", stored, errCode)
	}
	svc.WaitBackground()

	// NOTHING was written by the refusal, and nothing had to be: the row
	// carries no on-wire stamp, which is the bit the sender's badge reads.
	if outbox.onWireStamped("unconfirmed-durable-1") {
		t.Fatal("a dispatch no writer accepted was stamped as on the wire, so a restart reads it as sent")
	}
	rows, err := outbox.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	for _, row := range rows {
		if row.Envelope.ID == "unconfirmed-durable-1" && row.OnWire {
			t.Fatal("the reseed would reopen the message as sent")
		}
	}
}

// TestPendingRingFlushConfirmsDelivery is the other half of "queued
// locally is not sent". The relay path refuses to call a frame parked in
// our OWN ring delivered — so when the park finally pays off and the frame
// goes out, something has to say so, or the message stays unconfirmed
// forever although the peer has it.
func TestPendingRingFlushConfirmsDelivery(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const connID = netcore.ConnID(7423)
	attachPushObserver(t, svc, recipientID.Address, connID)
	address := domain.PeerAddress("10.7.7.7:64646")

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "parked-then-flushed", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	frame := protocol.Frame{
		Type: "push_message", Topic: "dm", ID: string(envelope.ID),
		Address: envelope.Sender, Recipient: envelope.Recipient,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.pending[address] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(address, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	svc.deliveryMu.RLock()
	before := svc.awaitingDelivered["parked-then-flushed"].Hold
	svc.deliveryMu.RUnlock()
	if before == holdNone {
		t.Fatal("a parked frame must not start out confirmed, or this test proves nothing")
	}

	svc.flushPendingFireAndForget(connID, address)
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["parked-then-flushed"]
	svc.deliveryMu.RUnlock()
	if entry.Hold != holdNone {
		t.Error("a flushed frame did leave the node, but the delivery was never confirmed")
	}
	if entry.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1: the flush is the emission", entry.Attempts)
	}
}

// TestRecipientGoingOfflineReopensTheDelivery covers the presence
// transition the kick alone cannot see. A message confirmed onto the wire
// sits at holdNone waiting for a receipt, and the kick filters holdNone
// out — deliberately, so a route refresh cannot pull an in-flight message
// forward. But a peer that went away and came back is not a route refresh,
// and their receipt is not coming.
func TestRecipientGoingOfflineReopensTheDelivery(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["awaiting-receipt"] = &deliveryRetryEntry{
		Envelope: protocol.Envelope{
			ID: "awaiting-receipt", Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
		},
		Attempts:      6,
		NextAttemptAt: now.Add(11 * time.Minute),
		Hold:          holdNone, // confirmed on the wire, receipt still owed
		LastEmittedAt: now,
	}
	svc.deliveryMu.Unlock()

	svc.noteRecipientWentOffline(recipient)

	svc.deliveryMu.RLock()
	reopened := svc.awaitingDelivered["awaiting-receipt"].Hold
	svc.deliveryMu.RUnlock()
	if reopened != holdUnreachable {
		t.Fatalf("hold = %d after the recipient left, want holdUnreachable so their return can wake it", reopened)
	}

	// And their return now does what it should: schedule pulled forward,
	// backoff back to its first step.
	attachCapableRelayPeer(t, svc, "back-again:64646", recipient)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: recipient, Origin: recipient, NextHop: recipient,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{recipient: {}})

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["awaiting-receipt"]
	svc.deliveryMu.RUnlock()
	if entry.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0: the recipient came back", entry.Attempts)
	}
	if entry.NextAttemptAt.After(time.Now().UTC()) {
		t.Errorf("next attempt still %v away", time.Until(entry.NextAttemptAt))
	}
}

// TestRouteReconfirmationDoesNotOverrulePacing: the kick answers a change
// in REACHABILITY, and only an entry that was waiting on reachability may
// be pulled forward by it.
//
// Announce ingest and route-query answers both feed routing.RouteUnchanged
// into this kick — deliberately, because a route that was already in the
// table can become usable again the moment its peer answers. But an entry
// that has already been dispatched on the current reachability is parked
// on the poll interval waiting for a sink, and a route being RE-confirmed
// says nothing about that. Waking it turned every periodic announcement
// into a re-send and a journal write, at announcement frequency, for a
// route that resolves but is dead in practice.
func TestRouteReconfirmationDoesNotOverrulePacing(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	// A route that resolves, so the kick's own self-check passes and this
	// test cannot pass merely because nothing was reachable.
	attachCapableRelayPeer(t, svc, "reconfirmed:64647", recipient)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: recipient, Origin: recipient, NextHop: recipient,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	now := time.Now().UTC()
	parked := now.Add(deliveryHoldPollInterval)
	envelopeOf := func(id protocol.MessageID) protocol.Envelope {
		return protocol.Envelope{ID: id, Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address}
	}
	svc.deliveryMu.Lock()
	// Dispatched on the current reachability; no sink has answered yet.
	svc.awaitingDelivered["dispatched-awaiting-sink"] = &deliveryRetryEntry{
		Envelope: envelopeOf("dispatched-awaiting-sink"), Attempts: 3,
		NextAttemptAt: parked, Hold: holdUnconfirmed,
	}
	// Never dispatched: the recipient was not there.
	svc.awaitingDelivered["waiting-for-them"] = &deliveryRetryEntry{
		Envelope: envelopeOf("waiting-for-them"), Attempts: 3,
		NextAttemptAt: parked, Hold: holdUnreachable,
	}
	svc.deliveryMu.Unlock()

	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{recipient: {}})

	svc.deliveryMu.RLock()
	dispatched := *svc.awaitingDelivered["dispatched-awaiting-sink"]
	held := *svc.awaitingDelivered["waiting-for-them"]
	svc.deliveryMu.RUnlock()

	if !dispatched.NextAttemptAt.Equal(parked) {
		t.Errorf("a route reconfirmation pulled a dispatched entry forward: %v → %v", parked, dispatched.NextAttemptAt)
	}
	if dispatched.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3: nothing about this entry's recipient changed", dispatched.Attempts)
	}
	// The kick still does its job, or the assertions above prove nothing.
	if !held.NextAttemptAt.Before(parked) {
		t.Fatalf("the kick did not wake the entry that WAS waiting on reachability")
	}
}

// TestQueuedFrameIsNotWrittenAfterItsAuthorRecallsIt pins the PRE-WIRE
// gate on the session writer. A frame can sit in the session's queue long
// enough for its author to withdraw the message, and the withdrawal
// classifies a message nobody has taken as never-emitted — so it creates
// no peer-side deletion. Writing the frame afterwards would put a message
// the sender was told was recalled in front of the recipient, with nothing
// left to recall it.
func TestQueuedFrameIsNotWrittenAfterItsAuthorRecallsIt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "recalled-in-the-queue", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	svc.deliveryMu.Unlock()

	ref := deliveryDispatchRef{Envelope: envelope, DispatchedAt: now}
	if !svc.clearedToWrite(ref, now) {
		t.Fatal("the frame was refused before anything withdrew it, so this test proves nothing")
	}

	// The author recalls it while the next frame waits in the queue.
	if _, err := svc.CancelOutgoingDelivery(envelope.ID, recipient); err != nil {
		t.Fatalf("CancelOutgoingDelivery: %v", err)
	}
	if svc.clearedToWrite(ref, time.Now().UTC()) {
		t.Fatal("a withdrawn message was cleared to go out, so the recipient gets a message the sender recalled")
	}

	// A frame carrying nobody's delivery passes untouched — the gate must
	// not stall transit traffic or announce-plane frames.
	if !svc.clearedToWrite(deliveryDispatchRef{}, time.Now().UTC()) {
		t.Error("the gate refused a frame that carries no delivery of ours")
	}
}

// TestConfirmationOutlivesTheDispatchThatOrderedIt pins the ordering the
// old code got wrong: the tick charged the attempt and cleared the hold as
// soon as it had CALLED the sinks, while the background push wrote the
// opposite answer whenever it happened to finish. Nothing is written by the
// dispatch at all now — the sink that accepted the frame is the only writer
// — so the two can no longer race.
func TestConfirmationOutlivesTheDispatchThatOrderedIt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "confirm-after-dispatch", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7421))
	svc.emitDueDelivery(dueDispatch{env: envelope, parkedAt: now}, now)

	// Before the push goroutine reports, nothing may claim the message
	// went anywhere. After it does, everything moves at once.
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["confirm-after-dispatch"]
	svc.deliveryMu.RUnlock()
	if entry.Hold != holdNone {
		t.Error("the accepting sink did not clear the hold")
	}
	if entry.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 — charged once, by the sink that took the frame", entry.Attempts)
	}
	if !entry.LastEmittedAt.Equal(now) {
		t.Errorf("LastEmittedAt = %v, want the dispatch instant %v", entry.LastEmittedAt, now)
	}
	// After ONE attempt the wait is the FIRST step of the schedule. The
	// old spelling here was backoff(1) — the second step — which is how
	// the 30-second step came to be skipped in the code this test was
	// meant to hold.
	if got, want := entry.NextAttemptAt, now.Add(deliveryRetryBackoffAfter(1)); !got.Equal(want) {
		t.Errorf("next attempt = %v, want %v: confirmation must replace the parking interval", got, want)
	}
}

// TestNewMessageEventCarriesTheQueuedStatus closes the race that produced
// the original report. Storing a locally-authored DM announces it to this
// node's own client, and that announcement runs against the synchronous
// reply to send_message: whichever reaches the conversation cache first
// decides the badge, and the other is dropped as a duplicate. So the event
// has to carry the answer too, which means the reachability decision has
// to be taken before it is published.
func TestNewMessageEventCarriesTheQueuedStatus(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	events := make(chan protocol.LocalChangeEvent, 4)
	bus.Subscribe(ebus.TopicMessageNew, func(event protocol.LocalChangeEvent) {
		events <- event
	})

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))

	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        "queued-event-1",
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipientID.Address,
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("own outgoing DM must be stored, got stored=%v errCode=%q", stored, errCode)
	}

	select {
	case event := <-events:
		if event.MessageID != "queued-event-1" {
			t.Fatalf("announced %q, want queued-event-1", event.MessageID)
		}
		if event.Status != protocol.MessageStatusQueued {
			t.Fatalf("event status = %q, want %q — the client cannot work this out for itself",
				event.Status, protocol.MessageStatusQueued)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("storing our own DM published no message.new event")
	}
}

// TestResendOfAHeldMessageStillAnswersQueued covers the idempotent retry.
// A client that re-sends an id the node already holds gets message_known,
// treats it as success, and reads the status off it — so an empty status
// there told the retry that a message still waiting for an unreachable
// recipient had gone out.
func TestResendOfAHeldMessageStillAnswersQueued(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	body := sealDMBody(t, svc.identity, recipientID.Address, identity.BoxPublicKeyBase64(recipientID.BoxPublicKey))
	frame := protocol.Frame{
		Type: "send_message", Topic: "dm",
		ID: "resend-held-1", Address: svc.Address(), Recipient: recipientID.Address,
		Flag: string(protocol.MessageFlagImmutable), Body: body,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	first := svc.storeMessageFrame(frame)
	if first.Type != "message_stored" {
		t.Fatalf("first send = %q, want message_stored", first.Type)
	}
	if first.Status != protocol.MessageStatusQueued {
		t.Fatalf("first send status = %q, want %q", first.Status, protocol.MessageStatusQueued)
	}

	again := svc.storeMessageFrame(frame)
	if again.Type != "message_known" {
		t.Fatalf("re-send = %q, want message_known", again.Type)
	}
	if again.Status != protocol.MessageStatusQueued {
		t.Fatalf("re-send status = %q, want %q — an idempotent retry must get the same answer as the first send",
			again.Status, protocol.MessageStatusQueued)
	}
}

// TestHeldRegistrationIsDueImmediately closes the window between the
// origin send's reachability check and the registration that follows it.
// A kick landing in that gap finds nothing to wake — the entry does not
// exist yet — so the entry must arrive already due rather than starting on
// a backoff step. The schedule times a wait for a RECEIPT, and a message
// that never reached the wire has no receipt coming.
func TestHeldRegistrationIsDueImmediately(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: "held-due-now", Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", CreatedAt: now,
	}, now, true)
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: "emitted-waits", Topic: "dm", Sender: svc.Address(), Recipient: "peer-b", CreatedAt: now,
	}, now, false)
	held := svc.awaitingDelivered["held-due-now"]
	emitted := svc.awaitingDelivered["emitted-waits"]
	svc.deliveryMu.Unlock()

	if held.NextAttemptAt.After(now) {
		t.Fatalf("a held message must be due at once, next attempt is %v after now", held.NextAttemptAt.Sub(now))
	}
	// The emitted one is the opposite case: it IS waiting for a receipt,
	// so it keeps the first backoff step.
	if got := emitted.NextAttemptAt.Sub(now); got != deliveryRetryBackoff(0) {
		t.Fatalf("an emitted message must wait %v for its receipt, got %v", deliveryRetryBackoff(0), got)
	}
}

// TestKickDuringATickKeepsTheEntryDue closes the other window: the tick
// samples reachability without a mutex, and a kick landing in that gap
// cannot see the entry (not yet Held) while the tick goes on to park it
// for a poll interval on an answer the kick has just invalidated.
func TestKickDuringATickKeepsTheEntryDue(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: "raced-kick", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: start, StoredAt: start,
	}, start, true)
	svc.deliveryMu.Unlock()

	// The kick lands after this pass began. The recipient is not actually
	// reachable, so the kick itself re-arms nothing — which is exactly the
	// case that used to leave the entry parked for a minute.
	tickStart := time.Now().UTC()
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipientID.Address): {},
	})
	svc.retryDueDeliveries(tickStart)

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["raced-kick"]
	svc.deliveryMu.RUnlock()
	if entry.NextAttemptAt.After(tickStart) {
		t.Fatalf("a hold decided across a kick must stay due, parked for %v instead", entry.NextAttemptAt.Sub(tickStart))
	}
}

// TestFailedDispatchDoesNotAnnounceSent pins the order between the badge
// and the wire. The emission mark has to land BEFORE the frame is written
// — a crash in between must read as "the peer may have it" — but the
// announcement is the opposite: telling the sender their message is on its
// way, for a frame that then failed to go out, leaves the badge claiming
// sent for something still sitting on the machine.
func TestFailedDispatchDoesNotAnnounceSent(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	announced := make(chan protocol.LocalChangeEvent, 4)
	bus.Subscribe(ebus.TopicMessageEmitted, func(event protocol.LocalChangeEvent) {
		announced <- event
	})

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "no-route-at-the-wire", Topic: "dm",
		Sender: svc.Address(), Recipient: "peer-a",
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	// Straight to the emission path with no route and no subscriber:
	// dispatchEnvelopeRetry holds the message and reports no emission.
	svc.emitDueDelivery(dueDispatch{env: envelope, parkedAt: now}, now)

	select {
	case event := <-announced:
		t.Fatalf("the sender was told %q went out, but no frame was written", event.MessageID)
	case <-time.After(200 * time.Millisecond):
	}

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["no-route-at-the-wire"]
	svc.deliveryMu.RUnlock()
	if entry.Hold == holdNone {
		t.Error("a message the wire refused must be held so a reachability kick can wake it")
	}
}

// TestAnnouncementSurvivesAFailedDispatch is the other half of
// TestFailedDispatchDoesNotAnnounceSent. Suppressing the announcement for
// an attempt that went nowhere is only right if a LATER, successful
// attempt still makes it: the emission mark is conservative and turns true
// on the first try, so an announcement derived from it would be spent on
// the failure and the badge would sit on "queued" forever.
func TestAnnouncementSurvivesAFailedDispatch(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	announced := make(chan protocol.LocalChangeEvent, 4)
	bus.Subscribe(ebus.TopicMessageEmitted, func(event protocol.LocalChangeEvent) {
		announced <- event
	})

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "announce-after-failure", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	// First attempt: no route, no subscriber. Emitted flips to true (the
	// conservative direction) but nothing goes out and nothing is said.
	svc.emitDueDelivery(dueDispatch{env: envelope, parkedAt: now}, now)
	select {
	case event := <-announced:
		t.Fatalf("a failed dispatch announced %q", event.MessageID)
	case <-time.After(150 * time.Millisecond):
	}

	// The recipient shows up and the next attempt succeeds.
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7420))
	retryAt := now.Add(time.Second)
	svc.emitDueDelivery(dueDispatch{env: envelope, parkedAt: retryAt}, retryAt)

	select {
	case event := <-announced:
		if event.MessageID != "announce-after-failure" {
			t.Fatalf("announced %q, want announce-after-failure", event.MessageID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the successful attempt announced nothing: the news was spent on the attempt that failed")
	}
}

// TestDeliveryOutlivesAnyAmountOfWaiting pins the decision that a message
// ends only when the recipient confirms it, its author withdraws it, or
// its own TTL expires. Running out of patience is not one of them: a
// horizon here would be the node giving up silently, and the sender is
// never shown that and cannot undo it.
func TestDeliveryOutlivesAnyAmountOfWaiting(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "patient-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: start, StoredAt: start,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, start, true)
	svc.deliveryMu.Unlock()

	// A year of the recipient being away, walked one hold poll at a time.
	now := start
	for i := 0; i < 400; i++ {
		now = now.Add(24 * time.Hour)
		svc.retryDueDeliveries(now)
	}

	svc.deliveryMu.RLock()
	entry, stillAwaiting := svc.awaitingDelivered["patient-1"]
	svc.deliveryMu.RUnlock()
	if !stillAwaiting {
		t.Fatal("the message was abandoned for nothing but the passage of time")
	}
	if entry.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0: not one of those days put a byte on the wire", entry.Attempts)
	}

	// And it still goes out the moment they come back.
	reader := attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7416))
	svc.retryDueDeliveries(now.Add(time.Minute))
	if pushed := readPushedFrame(t, reader, "push_message", 3*time.Second); pushed.Item.ID != "patient-1" {
		t.Fatalf("expected the year-old message, got %q", pushed.Item.ID)
	}
}

// TestReturningRecipientResetsTheBackoff covers the other half of "no
// limit": without the reset, a message that spent an evening climbing to
// the eleven-minute step would keep it, and the person who just came back
// online would wait a quarter of an hour for a message that has been ready
// since yesterday.
func TestReturningRecipientResetsTheBackoff(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "backoff-reset-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: start, StoredAt: start,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, start, true)
	// It went out several times before its recipient disappeared, so the
	// schedule is deep into the exponential tail.
	entry := svc.awaitingDelivered["backoff-reset-1"]
	entry.Attempts = 8
	entry.LastEmittedAt = time.Now().UTC().Add(-time.Hour)
	entry.Hold = holdUnreachable
	svc.deliveryMu.Unlock()

	reader := attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7417))
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipientID.Address): {},
	})

	svc.deliveryMu.RLock()
	attempts := svc.awaitingDelivered["backoff-reset-1"].Attempts
	svc.deliveryMu.RUnlock()
	if attempts != 0 {
		t.Fatalf("Attempts = %d after the recipient returned, want 0 — the backoff is still on its slow end", attempts)
	}

	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	if pushed := readPushedFrame(t, reader, "push_message", 3*time.Second); pushed.Item.ID != "backoff-reset-1" {
		t.Fatalf("expected the waiting message, got %q", pushed.Item.ID)
	}

	// The next attempt is the FIRST backoff step, not the ninth — and
	// literally the first: deliveryRetryBackoffAfter(1) is 30s, where the
	// old backoff(1) was a minute, which is what "reset to the first
	// step" was silently returning.
	svc.deliveryMu.RLock()
	next := svc.awaitingDelivered["backoff-reset-1"].NextAttemptAt
	svc.deliveryMu.RUnlock()
	if got, want := next.Sub(now), deliveryRetryBackoffAfter(1); got != want {
		t.Fatalf("next attempt in %v, want %v", got, want)
	}
	if want := deliveryRetrySchedule[0]; deliveryRetryBackoffAfter(1) != want {
		t.Fatalf("the first wait after one attempt is %v, want the schedule's first step %v",
			deliveryRetryBackoffAfter(1), want)
	}
}

// TestEmittingAQueuedMessageAnnouncesIt covers the sender's own view. A
// held message reads as "queued", and when it finally goes out nothing
// else would ever say otherwise — the next event is the recipient's
// receipt, which may be lost.
func TestEmittingAQueuedMessageAnnouncesIt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// newTestService passes a nil bus; install a real one so the emission
	// announcement is observable.
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus
	emitted := make(chan protocol.LocalChangeEvent, 4)
	bus.Subscribe(ebus.TopicMessageEmitted, func(event protocol.LocalChangeEvent) {
		emitted <- event
	})

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: "announce-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: start, StoredAt: start,
	}, start, true)
	svc.deliveryMu.Unlock()

	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7418))
	svc.retryDueDeliveries(start.Add(time.Minute))

	select {
	case event := <-emitted:
		if event.MessageID != "announce-1" {
			t.Fatalf("announced %q, want announce-1", event.MessageID)
		}
		if event.Status != protocol.MessageStatusSent {
			t.Fatalf("announced status %q, want %q", event.Status, protocol.MessageStatusSent)
		}
		if event.Type != protocol.LocalChangeMessageEmitted {
			t.Fatalf("announced type %q, want %q", event.Type, protocol.LocalChangeMessageEmitted)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a queued message reached the wire and the sender was never told")
	}

	// A re-send of the same message is not news: it stopped being queued
	// the first time.
	svc.retryDueDeliveries(start.Add(time.Minute).Add(deliveryRetryBackoff(1)).Add(time.Second))
	select {
	case event := <-emitted:
		t.Fatalf("a re-send announced %q again", event.MessageID)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestReseedClaimsNoQueueSlotItDidNotHave covers the restart. The outbox
// is the only surviving witness of whether a message was ever on the wire,
// and its answer becomes the confirmation stamp — but a stamp taken at
// RESTART time would make every reseeded message look like a frame still
// in flight, and the first deliveryQueueWindow after every restart would
// send nothing at all, to anyone.
//
// So the stamp is the message's own creation time, which cannot claim a
// slot the message did not have; and a row the outbox proves never went
// out carries no stamp at all.
func TestReseedClaimsNoQueueSlotItDidNotHave(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	const (
		sent   = protocol.MessageID("reseed-slot-sent")
		unsent = protocol.MessageID("reseed-slot-unsent")
	)
	written := time.Now().UTC().Add(-time.Hour)
	outbox := newEmissionOutbox(
		OutboxEntry{Envelope: protocol.Envelope{
			ID: sent, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
			Payload: []byte("x"), CreatedAt: written,
		}},
		OutboxEntry{Envelope: protocol.Envelope{
			ID: unsent, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
			Payload: []byte("y"), CreatedAt: written,
		}},
	)
	outbox.markNeverEmitted(unsent)
	svc.RegisterDeliveryOutbox(outbox)

	restartedAt := time.Now().UTC()
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()

	sentEntry, ok := svc.awaitingDelivered[sent]
	if !ok {
		t.Fatal("the emitted row was not reseeded")
	}
	if sentEntry.LastEmittedAt.After(written) {
		t.Errorf("stamped %v, later than the message itself (%v): a restart must not invent a fresher emission",
			sentEntry.LastEmittedAt, written)
	}
	if restartedAt.Sub(sentEntry.LastEmittedAt) < deliveryQueueWindow {
		t.Error("a reseeded message is holding its recipient's queue slot, so nothing goes out for a whole window after every restart")
	}

	unsentEntry, ok := svc.awaitingDelivered[unsent]
	if !ok {
		t.Fatal("the marked row was not reseeded")
	}
	if !unsentEntry.LastEmittedAt.IsZero() {
		t.Errorf("a row the outbox proves never went out is stamped %v", unsentEntry.LastEmittedAt)
	}
	// And the claim it still carries is known to this process, so a later
	// emission can withdraw it instead of leaving the row saying "never
	// emitted" while the peer holds the message.
	if _, standing := svc.markedNeverEmitted[unsent]; !standing {
		t.Error("the reseed did not record the standing durable claim, so nothing will ever withdraw it")
	}
}

// TestClockMovedBackDoesNotStallTheQueue: CreatedAt is a wall clock
// reading kept across restarts, and wall clocks move backwards.
//
// After a correction, every row written before it carries a creation time
// in the FUTURE, and the reseed stamps the confirmed ones with it. The
// queue window is a duration — "emitted less than twenty seconds ago owns
// the recipient's slot" — so a future stamp makes that difference
// negative and the message reads as "sent a moment ago" until the clock
// catches up. The pick then returns nothing for that recipient at all:
// the whole backlog written before the correction waits hours or days,
// and neither their coming online nor the entries falling due changes it,
// because it is the SLOT that is held.
func TestClockMovedBackDoesNotStallTheQueue(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)
	attachCapableRelayPeer(t, svc, "clock-back:64647", recipient)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: recipient, Origin: recipient, NextHop: recipient,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	// The clock was moved back a day, so rows written before that carry
	// creation times a day ahead of "now". Two of them: the older one is
	// the queue head, and the other is what waits behind it.
	const (
		head   = protocol.MessageID("written-before-the-clock-moved")
		behind = protocol.MessageID("written-before-the-clock-moved-2")
	)
	future := time.Now().UTC().Add(24 * time.Hour)
	outbox := newEmissionOutbox(
		OutboxEntry{Envelope: protocol.Envelope{
			ID: head, Topic: "dm", Sender: svc.Address(),
			Recipient: recipientID.Address, Payload: []byte("x"), CreatedAt: future,
		}},
		OutboxEntry{Envelope: protocol.Envelope{
			ID: behind, Topic: "dm", Sender: svc.Address(),
			Recipient: recipientID.Address, Payload: []byte("y"), CreatedAt: future.Add(time.Second),
		}},
	)
	// The fake reports the on-wire bit from its own journal, not from the
	// literal above — the same way the real adapter reads the row.
	if err := outbox.MarkOnWire([]protocol.MessageID{head, behind}); err != nil {
		t.Fatalf("MarkOnWire: %v", err)
	}
	svc.RegisterDeliveryOutbox(outbox)

	now := time.Now().UTC()
	svc.deliveryMu.RLock()
	stamped := svc.awaitingDelivered[head]
	svc.deliveryMu.RUnlock()
	if stamped == nil {
		t.Fatal("the confirmed row was not reseeded")
	}
	if stamped.LastEmittedAt.After(now) {
		t.Errorf("reseeded with an emission stamp at %v, ahead of now (%v): this node cannot have sent it yet",
			stamped.LastEmittedAt, now)
	}
	// And outside the window, not merely in the past. A restart claims no
	// queue slot it did not have — clamping to "this instant" would hold
	// the recipient's slot for a whole window after every restart, which
	// is the same stall in smaller units.
	if now.Sub(stamped.LastEmittedAt) < deliveryQueueWindow {
		t.Errorf("reseeded stamp is %v old, inside the %v window: the restart is holding a slot it never had",
			now.Sub(stamped.LastEmittedAt), deliveryQueueWindow)
	}

	// So the queue moves AT ONCE, not one window later.
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7419)))
	svc.retryDueDeliveries(now.Add(time.Second))
	stream.expect(t, head)
}

// TestFrozenMessageDoesNotStallTheQueue pins the one exception to the
// ordering rule. A freeze has no expiry of its own: it ends when the wipe
// commits or aborts. If a frozen message held its recipient's queue, a
// deletion that never settled would take the whole conversation with it.
func TestFrozenMessageDoesNotStallTheQueue(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	for i, id := range []protocol.MessageID{"frozen-head", "behind-the-freeze"} {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
	}
	svc.deliveryMu.Unlock()

	if _, err := svc.FreezeOutgoingDeliveriesTo(
		domain.PeerIdentityFromWire(recipientID.Address),
		[]protocol.MessageID{"frozen-head"},
	); err != nil {
		t.Fatalf("FreezeOutgoingDeliveriesTo: %v", err)
	}

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7414)))
	svc.retryDueDeliveries(start.Add(time.Minute))
	stream.expect(t, "behind-the-freeze")

	svc.deliveryMu.RLock()
	frozen, stillAwaiting := svc.awaitingDelivered["frozen-head"]
	svc.deliveryMu.RUnlock()
	if !stillAwaiting {
		t.Fatal("the frozen message was dropped by the tick")
	}
	if frozen.confirmed() {
		t.Error("a frozen message reached the wire")
	}
}

// TestUnsentMessageIsNotOvertaken pins the ordering guarantee at its
// sharpest point: an older message that has NEVER been emitted holds its
// place even when a newer one is due first. Only an already-emitted head
// steps aside — the recipient has that one already, so nothing they see is
// reordered.
func TestUnsentMessageIsNotOvertaken(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	for i, id := range []protocol.MessageID{"older-unsent", "newer-due-first"} {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
	}
	// The older message is not due yet; the newer one is. Scheduling skew
	// like this is ordinary — a held entry re-checks on the hold poll while
	// a message written later starts on the first backoff step.
	svc.awaitingDelivered["older-unsent"].NextAttemptAt = start.Add(2 * time.Minute)
	svc.awaitingDelivered["newer-due-first"].NextAttemptAt = start
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7415)))
	// Attaching the observer changed the mesh, which is a reason to re-try
	// overdue entries. Let the tick see that, then restate the skew this
	// test is about — its subject is the queue order, not the wake-up.
	svc.retryDueDeliveries(start)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["older-unsent"].NextAttemptAt = start.Add(2 * time.Minute)
	svc.awaitingDelivered["newer-due-first"].NextAttemptAt = start
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(start.Add(time.Minute))
	stream.expectQuiet(t, 150*time.Millisecond)

	// Once the older one is due, it goes first.
	svc.retryDueDeliveries(start.Add(3 * time.Minute))
	stream.expect(t, "older-unsent")
}

// TestQueueWindowCannotStarveTheTail pins the inequality the queue
// discipline rests on: a message that is due again re-takes the slot it
// just released, so a window as long as the shortest retry interval would
// let the head of a queue whose receipts are being lost win every turn.
func TestQueueWindowCannotStarveTheTail(t *testing.T) {
	t.Parallel()
	if deliveryQueueWindow >= deliveryRetrySchedule[0] {
		t.Fatalf("deliveryQueueWindow (%v) must stay below the first retry interval (%v), "+
			"or an unconfirmed head starves everything behind it",
			deliveryQueueWindow, deliveryRetrySchedule[0])
	}
}

// pushMessageStream turns the observer connection into a single ordered
// channel of push_message ids. One reader goroutine for the whole test:
// readPushedFrame starts a fresh one per call, and several of those racing
// over one buffered reader would make "which message arrived first" a
// property of the test harness rather than of the scheduler.
type pushMessageStream struct {
	ids chan protocol.MessageID
}

func newPushMessageStream(t *testing.T, reader *bufio.Reader) *pushMessageStream {
	t.Helper()
	stream := &pushMessageStream{ids: make(chan protocol.MessageID, 256)}
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				close(stream.ids)
				return
			}
			frame, err := protocol.ParseFrameLine(line[:len(line)-1])
			if err != nil || frame.Type != "push_message" || frame.Item == nil {
				continue
			}
			select {
			case stream.ids <- protocol.MessageID(frame.Item.ID):
			default:
				// The buffer is the test's own budget for unread pushes.
				// Blocking here would hang the reader goroutine past the
				// end of the test instead of failing it, so an overflow
				// is reported and the stream closed.
				t.Errorf("push stream overflowed: more than %d unread push_message frames", cap(stream.ids))
				close(stream.ids)
				return
			}
		}
	}()
	return stream
}

func (p *pushMessageStream) expect(t *testing.T, want protocol.MessageID) {
	t.Helper()
	select {
	case got, ok := <-p.ids:
		if !ok {
			t.Fatalf("observer connection closed while waiting for %s", want)
		}
		if got != want {
			t.Fatalf("flush order broken: expected %s next, got %s", want, got)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}

func (p *pushMessageStream) expectQuiet(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case got, ok := <-p.ids:
		if ok {
			t.Fatalf("the queue slot was taken, yet %s went out behind it", got)
		}
	case <-time.After(d):
	}
}

// TestQueueWindowReleasesUnconfirmedHead pins the timeout half of the
// queue: a message whose receipt never comes back must lose its place in
// line, not freeze the conversation behind it for the rest of its backoff.
func TestQueueWindowReleasesUnconfirmedHead(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	svc.deliveryMu.Lock()
	for i, id := range []protocol.MessageID{"window-1", "window-2"} {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
	}
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7413)))
	now := start.Add(time.Minute)
	svc.retryDueDeliveries(now)
	stream.expect(t, "window-1")

	// No receipt ever arrives. Once the window passes, the second message
	// goes out even though the first is still unconfirmed.
	svc.retryDueDeliveries(now.Add(deliveryQueueWindow + time.Second))
	stream.expect(t, "window-2")
}

// TestLateReceiptPromotesTheMessageThatNeverWent is the three-message
// version of the queue rule, and the one that catches promotion breaking
// it.
//
// A receipt frees the recipient's slot, and the promotion that follows is
// meant for the message that was WAITING on that slot. Taking the oldest
// entry left instead takes one that has already gone out and is merely on
// its own backoff — so the recipient gets an early duplicate of a message
// they already have, and that duplicate owns the slot for another queue
// window, delaying the message that has never been sent at all.
func TestLateReceiptPromotesTheMessageThatNeverWent(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	svc.RegisterMessageStore(&receiptStatusStore{accept: true})

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	start := time.Now().UTC()
	ids := []protocol.MessageID{"queue-1", "queue-2", "queue-3"}
	svc.deliveryMu.Lock()
	for i, id := range ids {
		createdAt := start.Add(time.Duration(i) * time.Second)
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipientID.Address,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, start, true)
		svc.sentDMIDs.Add(string(id))
	}
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7418)))

	// The first goes out, and its receipt is late.
	now := start.Add(time.Minute)
	svc.retryDueDeliveries(now)
	stream.expect(t, "queue-1")

	// The window passes without a receipt, so the second goes out too.
	now = now.Add(deliveryQueueWindow + time.Second)
	svc.retryDueDeliveries(now)
	stream.expect(t, "queue-2")

	// Only now does the first message's receipt arrive.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID: ids[0], Sender: recipientID.Address, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: now,
	})
	svc.WaitBackground()

	// The freed slot belongs to the message that has never been sent.
	svc.deliveryMu.RLock()
	second := svc.awaitingDelivered["queue-2"]
	third := svc.awaitingDelivered["queue-3"]
	secondDue, thirdDue := second.NextAttemptAt, third.NextAttemptAt
	svc.deliveryMu.RUnlock()
	if !secondDue.After(now) {
		t.Errorf("queue-2 was pulled forward to %v by a receipt for another message; it has already gone out and is on its own backoff", secondDue)
	}
	if thirdDue.After(now) {
		t.Errorf("queue-3 is still parked at %v; the freed slot was the one it was waiting for", thirdDue)
	}

	// And the wire agrees: once the second message's window expires the
	// next frame is the one that has never gone out, not a duplicate.
	svc.retryDueDeliveries(now.Add(deliveryQueueWindow + time.Second))
	stream.expect(t, "queue-3")
}

// routeThenChange runs a hook on every Route call, so a test can make a
// concurrent change happen in the window where the kick has released the
// lock — the only place that window is observable.
type routeThenChange struct {
	inner Router
	hook  func()
}

func (r routeThenChange) Route(msg protocol.Envelope) RoutingDecision {
	decision := r.inner.Route(msg)
	if r.hook != nil {
		r.hook()
	}
	return decision
}

// TestKickDoesNotRearmAnEntryDispatchedWhileItLookedUpTheRoute is the
// window version of TestRouteReconfirmationDoesNotOverrulePacing, and it is
// about a ROUTE event for the same reason that one is: a reconnect
// deliberately DOES wake a dispatched-but-unconfirmed entry, so the window
// only matters for the cause whose selection is narrow.
//
// The kick decides who is eligible under the lock, then RELEASES it to ask
// the router — Route reads routing and peer state under its own locks, so
// holding deliveryMu across it would invert the canonical order. In that
// window a tick can dispatch the entry, or a sink can confirm it, and the
// state the decision was made on is gone. Re-arming then does precisely
// what the selection exists to prevent: an extra re-send, and the next
// message in that recipient's queue waiting behind it.
func TestKickDoesNotRearmAnEntryDispatchedWhileItLookedUpTheRoute(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)
	// A route that RESOLVES, or phase 3 never runs and this test passes
	// while proving nothing.
	attachCapableRelayPeer(t, svc, "dispatched-mid-kick:64647", recipient)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: recipient, Origin: recipient, NextHop: recipient,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	now := time.Now().UTC()
	parked := now.Add(deliveryHoldPollInterval)
	envelopeOf := func(id protocol.MessageID) protocol.Envelope {
		return protocol.Envelope{ID: id, Topic: "dm", Sender: svc.Address(), Recipient: recipientID.Address}
	}
	const (
		racing  = protocol.MessageID("dispatched-mid-kick")
		control = protocol.MessageID("still-waiting-for-them")
	)
	svc.deliveryMu.Lock()
	// Held on reachability when the kick looks — and dispatched by a tick
	// while the kick is asking the router.
	svc.awaitingDelivered[racing] = &deliveryRetryEntry{
		Envelope: envelopeOf(racing), Attempts: 3,
		NextAttemptAt: parked, Hold: holdUnreachable,
	}
	// Held for the whole call: the kick must still do its job, or the
	// assertion above proves nothing.
	svc.awaitingDelivered[control] = &deliveryRetryEntry{
		Envelope: envelopeOf(control), Attempts: 3,
		NextAttemptAt: parked, Hold: holdUnreachable,
	}
	svc.deliveryMu.Unlock()

	// The tick gets there first, inside the window the kick opens.
	svc.router = routeThenChange{inner: svc.router, hook: func() {
		svc.deliveryMu.Lock()
		if live, ok := svc.awaitingDelivered[racing]; ok {
			live.Hold = holdUnconfirmed
			live.NextAttemptAt = parked
		}
		svc.deliveryMu.Unlock()
	}}

	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{recipient: {}})

	svc.deliveryMu.RLock()
	raced := *svc.awaitingDelivered[racing]
	held := *svc.awaitingDelivered[control]
	svc.deliveryMu.RUnlock()

	if raced.Hold != holdUnconfirmed {
		t.Fatalf("hold is %v; the test did not reach the window it is about", raced.Hold)
	}
	if !held.NextAttemptAt.Before(parked) {
		t.Fatal("the kick did not wake the entry that WAS waiting on reachability; the assertion below would prove nothing")
	}
	if !raced.NextAttemptAt.Equal(parked) {
		t.Errorf("the kick pulled a dispatched entry forward to %v from %v; it re-armed on a hold that was gone by the time it acted",
			raced.NextAttemptAt, parked)
	}
	if raced.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3: the racing entry's backoff was reset too", raced.Attempts)
	}
}

// TestStaleOfflineDoesNotFreezeAReconnectedRecipient closes the mirror of
// the kick's window, and the one that re-enters this task's own symptom.
//
// The close path counts the session down under peerMu and reports the
// departure after releasing it. In between, the peer can be back: their
// registration completes, their online kick runs and correctly leaves an
// in-flight entry alone — and then the stale departure parks it on
// holdUnreachable with its old backoff. Nothing fires a second kick, so a
// message that was ready waits out up to eleven minutes with the recipient
// online.
func TestStaleOfflineDoesNotFreezeAReconnectedRecipient(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		reconnect bool
		wantHold  deliveryHoldReason
	}{
		{"they really are gone — reopen", false, holdUnreachable},
		{"they are already back — leave it alone", true, holdNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)

			recipientID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			recipient := domain.PeerIdentityFromWire(recipientID.Address)

			now := time.Now().UTC()
			const target = protocol.MessageID("stale-offline")
			svc.deliveryMu.Lock()
			svc.awaitingDelivered[target] = &deliveryRetryEntry{
				Envelope: protocol.Envelope{
					ID: target, Topic: "dm", Sender: svc.Address(),
					Recipient: recipientID.Address, CreatedAt: now,
				},
				Attempts: 3, NextAttemptAt: now.Add(11 * time.Minute), Hold: holdNone,
			}
			svc.deliveryMu.Unlock()

			if tc.reconnect {
				// The new session registered while the old close was on
				// its way to reporting the departure.
				svc.peerMu.Lock()
				svc.identitySessions[recipient] = 1
				svc.peerMu.Unlock()
			}

			svc.noteRecipientWentOffline(recipient)
			settled := time.Now().UTC()

			svc.deliveryMu.RLock()
			hold := svc.awaitingDelivered[target].Hold
			due := svc.awaitingDelivered[target].NextAttemptAt
			svc.deliveryMu.RUnlock()
			if hold != tc.wantHold {
				t.Errorf("hold = %v, want %v: a departure that was already undone must not park a live delivery", hold, tc.wantHold)
			}
			if tc.wantHold == holdUnreachable && due.After(settled) {
				// A recipient is also reachable through a transit route,
				// which no session count shows, so a stale departure can
				// still park a live delivery. Pulling the entry due makes
				// that harmless: the next tick either sends or re-parks
				// on the poll interval, instead of the message sitting
				// out the rest of an eleven-minute backoff.
				t.Errorf("the reopened entry is due at %v, still in the future at %v; it keeps a backoff that was measuring a peer who has left", due, settled)
			}
		})
	}
}

// TestSeenAckRetryFollowsTheSchedule pins the seen-receipt retry to the
// same chain the documentation states: 30s, then a minute, then two — and
// to the budget contract, where the configured maximum is a number of
// RE-SENDS.
//
// The two retry machines index the schedule differently and a shared
// helper made that invisible: a message is sent AT registration, so its
// first wait follows an attempt already made; a seen receipt is
// registered with its first wait ahead of it. Using the message rule here
// spent the first step twice (30s, 30s, 1m), and paying for that by
// starting the counter at one spent a re-send on a send that had not
// happened yet.
func TestSeenAckRetryFollowsTheSchedule(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	receipt := protocol.DeliveryReceipt{
		MessageID:   "seen-schedule-1",
		Sender:      svc.Address(),
		Recipient:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingSeenAckLocked(receipt, now)
	first := svc.awaitingSeenAck[receipt.MessageID].NextAttemptAt
	svc.deliveryMu.Unlock()

	if got, want := first.Sub(now), deliveryRetrySchedule[0]; got != want {
		t.Fatalf("first retry in %v, want the schedule's first step %v", got, want)
	}

	// The first retry fires; the next wait is the SECOND step, not the
	// first one again.
	svc.planDueSeenAcks(first)
	svc.deliveryMu.RLock()
	second := svc.awaitingSeenAck[receipt.MessageID].NextAttemptAt
	svc.deliveryMu.RUnlock()
	if got, want := second.Sub(first), deliveryRetrySchedule[1]; got != want {
		t.Errorf("second wait %v, want %v: the schedule must advance, not repeat", got, want)
	}

	// And the budget counts RE-SENDS: one has happened, so with a
	// configured maximum of one there is exactly none left.
	svc.deliveryMu.RLock()
	attempts := svc.awaitingSeenAck[receipt.MessageID].Attempts
	svc.deliveryMu.RUnlock()
	if attempts != 1 {
		t.Errorf("Attempts = %d after one re-send, want 1 — registration must not charge for the original", attempts)
	}
}

// TestSeenAckRetryBudgetCountsResends: the configured maximum is a number
// of re-sends, and it must buy exactly that many.
func TestSeenAckRetryBudgetCountsResends(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.DeliveryRetryMaxAttempts = 2

	now := time.Now().UTC()
	receipt := protocol.DeliveryReceipt{
		MessageID:   "seen-budget-1",
		Sender:      svc.Address(),
		Recipient:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingSeenAckLocked(receipt, now)
	svc.deliveryMu.Unlock()

	resends := 0
	for range 10 {
		svc.deliveryMu.RLock()
		entry, live := svc.awaitingSeenAck[receipt.MessageID]
		var due time.Time
		if live {
			due = entry.NextAttemptAt
		}
		svc.deliveryMu.RUnlock()
		if !live {
			break
		}
		resends += len(svc.planDueSeenAcks(due))
	}
	if resends != 2 {
		t.Errorf("the receipt was re-sent %d time(s) with a maximum of 2; the configured number is re-sends, not sends", resends)
	}
}
