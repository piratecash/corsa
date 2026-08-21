package node

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// emissionOutbox is a DeliveryOutbox that also keeps the durable
// never-emitted marks, so a test can restart a node against it.
type emissionOutbox struct {
	mu       sync.Mutex
	rows     []OutboxEntry
	never    map[protocol.MessageID]struct{}
	clearErr error
}

func newEmissionOutbox(rows ...OutboxEntry) *emissionOutbox {
	return &emissionOutbox{rows: rows, never: map[protocol.MessageID]struct{}{}}
}

func (o *emissionOutbox) UndeliveredOutgoing() ([]OutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rows := make([]OutboxEntry, 0, len(o.rows))
	for _, row := range o.rows {
		_, marked := o.never[row.Envelope.ID]
		row.Emitted = !marked
		rows = append(rows, row)
	}
	return rows, nil
}

func (o *emissionOutbox) MarkNeverEmitted(ids []protocol.MessageID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range ids {
		o.never[id] = struct{}{}
	}
	return nil
}

func (o *emissionOutbox) ClearNeverEmitted(ids []protocol.MessageID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.clearErr != nil {
		return o.clearErr
	}
	for _, id := range ids {
		delete(o.never, id)
	}
	return nil
}

func (o *emissionOutbox) failClears(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.clearErr = err
}

func (o *emissionOutbox) marked(id protocol.MessageID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.never[id]
	return ok
}

// TestWithheldSendIsRecordedNeverEmitted: withholding a message because the
// recipient is unreachable is the one thing about an envelope's past that
// cannot be re-derived after a restart, so it has to reach the journal.
func TestWithheldSendIsRecordedNeverEmitted(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const held = protocol.MessageID("held-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: held, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()
	svc.syncEmissionMarks([]protocol.MessageID{held})

	if !outbox.marked(held) {
		t.Fatal("a withheld send left no durable record that it never went out")
	}

	// And the claim is withdrawn the moment it does go out — before the
	// frame is written, which is what noteOwnEnvelopesEmitted guarantees.
	svc.noteOwnEnvelopeEmitted(svc.Address(), held)
	if outbox.marked(held) {
		t.Fatal("the claim survived the emission that disproved it")
	}
}

// TestRestartKeepsNeverEmittedProof is the fix itself. Before it, a
// reseeded entry was Emitted=true unconditionally, so after a restart a
// wipe announced the id of a message that had never left the machine.
func TestRestartKeepsNeverEmittedProof(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	now := time.Now().UTC()
	const (
		unsent = protocol.MessageID("restart-unsent")
		sent   = protocol.MessageID("restart-sent")
	)
	outbox := newEmissionOutbox(
		OutboxEntry{Envelope: protocol.Envelope{
			ID: unsent, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
			Payload: []byte("x"), CreatedAt: now,
		}},
		OutboxEntry{Envelope: protocol.Envelope{
			ID: sent, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
			Payload: []byte("y"), CreatedAt: now,
		}},
	)
	if err := outbox.MarkNeverEmitted([]protocol.MessageID{unsent}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	svc.RegisterDeliveryOutbox(outbox)

	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	unsentEntry, ok := svc.awaitingDelivered[unsent]
	if !ok {
		t.Fatal("the marked envelope was not reseeded")
	}
	if unsentEntry.Emitted {
		t.Error("a message the outbox proves never went out reads as emitted after restart")
	}
	sentEntry, ok := svc.awaitingDelivered[sent]
	if !ok {
		t.Fatal("the unmarked envelope was not reseeded")
	}
	if !sentEntry.Emitted {
		t.Error("an unmarked message must read as emitted — the outbox proves nothing about it")
	}
}

// TestUnmarkedRestartStillReadsAsEmitted pins the direction of the default
// for every row written before the journal existed: no mark is not proof of
// anything, and the cautious answer keeps the deletion scheduled.
func TestUnmarkedRestartStillReadsAsEmitted(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.RegisterDeliveryOutbox(stubDeliveryOutbox{envelopes: []protocol.Envelope{
		{ID: "legacy-1", Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", CreatedAt: time.Now().UTC()},
	}})

	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	entry, ok := svc.awaitingDelivered["legacy-1"]
	if !ok {
		t.Fatal("legacy envelope was not reseeded")
	}
	if !entry.Emitted {
		t.Error("an outbox row with no mark must read as emitted")
	}
}

// TestEmissionMarkIsNotWrittenForAnOrdinarySend: the mark exists for the
// withheld case, and the ordinary path — stored and emitted at once — must
// not pay a disk write for it.
func TestEmissionMarkIsNotWrittenForAnOrdinarySend(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const plain = protocol.MessageID("plain-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: plain, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()

	// Every later emission of the same message is a no-op too.
	svc.noteOwnEnvelopeEmitted(svc.Address(), plain)
	svc.noteOwnEnvelopeEmitted(svc.Address(), plain)

	if outbox.marked(plain) {
		t.Fatal("an ordinary send was recorded as never emitted")
	}
}

// TestFrameIsWithheldWhenTheClaimCannotBeWithdrawn is the rule the whole
// journal rests on: the disk claim and the wire must never disagree. If
// the clear cannot be written, sending anyway would put the message on the
// peer while the disk still says it never left — and after a restart that
// claim is what makes the deletion skip them.
func TestFrameIsWithheldWhenTheClaimCannotBeWithdrawn(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const held = protocol.MessageID("withheld-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: held, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()
	svc.syncEmissionMarks([]protocol.MessageID{held})

	outbox.failClears(errors.New("database is locked"))
	if svc.noteOwnEnvelopeEmitted(svc.Address(), held) {
		t.Fatal("the caller was cleared to send a message whose claim is still on disk")
	}

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[held]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("the message left the retry engine, so nothing will send it later")
	}
	if entry.Emitted {
		t.Error("the entry counts as emitted although nothing went out")
	}
	if !outbox.marked(held) {
		t.Error("the durable claim was dropped even though the write failed")
	}

	// The database recovers and the next attempt goes through.
	outbox.failClears(nil)
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), held) {
		t.Fatal("the retry was still withheld after the journal recovered")
	}
	if outbox.marked(held) {
		t.Error("the claim survived the emission that disproved it")
	}
}

// TestBacklogReplaySkipsOnlyTheStrandedIds: one message whose claim could
// not be withdrawn must not take the rest of the backlog down with it —
// but it must not go out either.
func TestBacklogReplaySkipsOnlyTheStrandedIds(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const (
		stuck = protocol.MessageID("backlog-held")
		plain = protocol.MessageID("backlog-plain")
	)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: stuck, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), true)
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: plain, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()
	svc.syncEmissionMarks([]protocol.MessageID{stuck})
	outbox.failClears(errors.New("database is locked"))

	withheld := svc.noteOwnEnvelopesEmitted([]protocol.MessageID{stuck, plain})
	if _, blocked := withheld[stuck]; !blocked {
		t.Error("the stranded message was cleared to go out")
	}
	if _, blocked := withheld[plain]; blocked {
		t.Error("a message with no claim on disk was held back by another one's failure")
	}
}

// TestFrozenDeliveryIsNotEmitted is the guarantee the wipe's freeze buys.
// While a wipe is deciding whether the peer may ever hold a message,
// nothing may put it on the wire — the decision is made from a row that is
// about to be destroyed, so an emission behind its back is unrecoverable.
func TestFrozenDeliveryIsNotEmitted(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	const target = protocol.MessageID("frozen-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient,
	}, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()

	frozen, err := svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(recipient), []protocol.MessageID{target})
	if err != nil {
		t.Fatalf("FreezeOutgoingDeliveriesTo: %v", err)
	}
	if _, never := frozen.NeverEmitted[target]; !never {
		t.Error("the freeze did not report a withheld message as never emitted")
	}

	if svc.noteOwnEnvelopeEmitted(svc.Address(), target) {
		t.Fatal("a frozen message was cleared to go out")
	}
	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if !awaiting || entry.Emitted {
		t.Fatalf("the frozen entry was consumed anyway: awaiting=%v emitted=%v", awaiting, entry != nil && entry.Emitted)
	}

	// A wipe that could not commit puts it back.
	svc.ThawOutgoingDeliveries([]protocol.MessageID{target})
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target) {
		t.Fatal("the message is still held back after the thaw")
	}
}

// TestFrozenDeliveryIsNotRetried: the retry tick must not dispatch a
// frozen message, and must not spend one of its attempts on it either —
// the freeze is a pause, and charging it would abandon the delivery for a
// reason that has nothing to do with the peer.
func TestFrozenDeliveryIsNotRetried(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	const target = protocol.MessageID("frozen-2")
	past := time.Now().UTC().Add(-time.Hour)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient,
		CreatedAt: time.Now().UTC(),
	}, past, false)
	svc.awaitingDelivered[target].NextAttemptAt = past
	svc.deliveryMu.Unlock()

	if _, err := svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(recipient), []protocol.MessageID{target}); err != nil {
		t.Fatalf("FreezeOutgoingDeliveriesTo: %v", err)
	}

	svc.retryDueDeliveries(time.Now().UTC())

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("the frozen entry was dropped by the retry tick")
	}
	if entry.Attempts != 0 {
		t.Errorf("attempts = %d, want 0: a freeze must not burn the retry budget", entry.Attempts)
	}
}

// deferringStore answers "cannot decide" for one id and stores the rest.
type deferringStore struct {
	mu     sync.Mutex
	defer_ protocol.MessageID
	stored []protocol.MessageID
}

func (d *deferringStore) StoreMessage(envelope protocol.Envelope, _ bool) StoreResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	if envelope.ID == d.defer_ {
		return StoreDeferred
	}
	d.stored = append(d.stored, envelope.ID)
	return StoreInserted
}

func (d *deferringStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

func (d *deferringStore) has(id protocol.MessageID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, stored := range d.stored {
		if stored == id {
			return true
		}
	}
	return false
}

// TestDeferredMessageIsLeftWithTheSender: a store that cannot decide has
// not received the message. Keeping it in the runtime backlog and
// acknowledging it would stop the sender retrying a message that is on no
// disk anywhere — and a restart would lose it for good.
func TestDeferredMessageIsLeftWithTheSender(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	const target = protocol.MessageID("deferred-1")
	store := &deferringStore{defer_: target}
	svc.RegisterMessageStore(store)

	body := sealDMBody(t, sender, svc.identity.Address, identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        target,
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: svc.Address(),
		Flag:      protocol.MessageFlagAnyDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)

	if stored {
		t.Fatal("a deferred message was reported as stored")
	}
	if errCode == "" {
		t.Fatal("a deferred message was reported as an ordinary duplicate; the frame would be acked and the sender would stop")
	}
	if shouldAckOnStoreResult(stored, errCode) {
		t.Error("the frame was acked although nothing kept the message")
	}
	if backlogHas(svc, target) {
		t.Error("a deferred message entered the runtime backlog; a restart would lose it silently")
	}
	if store.has(target) {
		t.Fatal("the store contradicted itself")
	}

	// The dedup mark must not be set either, or the sender's next attempt
	// is dropped in silence.
	svc.gossipMu.RLock()
	marked := svc.seen.Has(string(target))
	svc.gossipMu.RUnlock()
	if marked {
		t.Error("the deferred id was marked as seen; the retry would be deduped away")
	}
}

// TestBacklogReplayWithdrawsAClaimWithoutARetryEntry closes the path that
// reaches past the retry engine. A message whose attempts ran out is
// dropped from awaitingDelivered while its durable "never emitted" claim
// still stands — and the backlog replay can still hand it to the peer the
// moment they connect. Without withdrawing the claim there, deleting that
// message would skip the peer and leave their copy for good.
func TestBacklogReplayWithdrawsAClaimWithoutARetryEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const target = protocol.MessageID("orphaned-claim-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), true)
	svc.deliveryMu.Unlock()
	svc.syncEmissionMarks([]protocol.MessageID{target})
	if !outbox.marked(target) {
		t.Fatal("the withheld message was not recorded")
	}

	// The retry engine gives up: the entry is gone, the claim is not.
	svc.deliveryMu.Lock()
	delete(svc.awaitingDelivered, target)
	svc.deliveryMu.Unlock()

	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target) {
		t.Fatal("the backlog replay was blocked for a message with no entry")
	}
	if outbox.marked(target) {
		t.Error("the claim survived an emission that disproved it")
	}
}

// TestDeferredOutgoingMessageIsNotRouted: the exit has to come BEFORE the
// routing, not after it. An outgoing DM that reaches the peer while the
// local RPC reports an error is the worst of both — the recipient has a
// message whose row is on no disk here, and nothing will ever recall it.
func TestDeferredOutgoingMessageIsNotRouted(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("deferred-outgoing-1")
	svc.RegisterMessageStore(&deferringStore{defer_: target})

	body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        target,
		Topic:     "dm",
		Sender:    svc.Address(),
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagAnyDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)

	if stored || errCode == "" {
		t.Fatalf("a deferred outgoing message was accepted: stored=%v errCode=%q", stored, errCode)
	}
	if backlogHas(svc, target) {
		t.Error("the message entered the backlog, from which gossip and relay serve it")
	}
	svc.deliveryMu.RLock()
	_, awaiting := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if awaiting {
		t.Error("a sender-side retry was registered for a message the store never kept")
	}
}

// TestRetryTickHonoursAFreezeTakenAfterPlanning is the window the freeze
// existed to close and did not. The tick picks its candidates, checks
// routes without the lock and dispatches later; a deletion landing in
// between froze the message, classified it as never-emitted from the row
// it was about to destroy, and wrote no request — and the old dispatch
// then handed the payload to the peer, for good.
func TestRetryTickHonoursAFreezeTakenAfterPlanning(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	const target = protocol.MessageID("frozen-after-planning")
	past := time.Now().UTC().Add(-time.Hour)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient,
		CreatedAt: time.Now().UTC(),
	}, past, true)
	svc.awaitingDelivered[target].NextAttemptAt = past
	svc.deliveryMu.Unlock()

	// The freeze lands after the entry is due but before the tick runs —
	// the same relative order as a freeze taken mid-tick, which no test
	// can schedule deterministically.
	if _, err := svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(recipient), []protocol.MessageID{target}); err != nil {
		t.Fatalf("FreezeOutgoingDeliveriesTo: %v", err)
	}

	// The claim is what the dispatch consults, and it must refuse.
	if svc.noteOwnEnvelopeEmitted(svc.Address(), target) {
		t.Fatal("the last boundary before the wire cleared a frozen message to go out")
	}

	svc.retryDueDeliveries(time.Now().UTC())

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("the frozen entry was dropped by the tick")
	}
	if entry.Emitted {
		t.Error("a frozen message was counted as emitted")
	}
}

// TestFreezeDuringATickCostsNoAttempt pins the interleaving the previous
// test could not reach: the freeze lands AFTER the tick charged the
// attempt and pushed the backoff, and before the dispatch consults it.
//
// Blocking the send there is only half the answer. The message comes back
// from a thaw carrying a spent attempt and up to eleven minutes of
// backoff, and on the last attempt the very next tick abandons the
// delivery outright — all without the network having been used once.
func TestFreezeDuringATickCostsNoAttempt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	const target = protocol.MessageID("frozen-mid-tick")
	due := time.Now().UTC().Add(-time.Hour)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient,
		CreatedAt: time.Now().UTC(),
	}, due, true)
	entry := svc.awaitingDelivered[target]
	entry.NextAttemptAt = due
	entry.Attempts = 3
	svc.deliveryMu.Unlock()

	// The freeze is taken from inside the tick, between the charge and
	// the dispatch — the window a freeze taken before the tick cannot
	// exercise.
	svc.retryDispatchBarrier = func() {
		if _, err := svc.FreezeOutgoingDeliveriesTo(
			domain.PeerIdentityFromWire(recipient), []protocol.MessageID{target}); err != nil {
			t.Errorf("FreezeOutgoingDeliveriesTo: %v", err)
		}
	}

	svc.retryDueDeliveries(time.Now().UTC())

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[target]
	attempts := 0
	var nextAttempt time.Time
	emitted, held := false, false
	if awaiting {
		attempts, nextAttempt, emitted, held = entry.Attempts, entry.NextAttemptAt, entry.Emitted, entry.Held
	}
	svc.deliveryMu.RUnlock()

	if !awaiting {
		t.Fatal("the frozen entry was dropped by the tick")
	}
	if emitted {
		t.Error("a frozen message was counted as emitted")
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3: the wire was never used, so nothing was spent", attempts)
	}
	if !nextAttempt.Equal(due) {
		t.Errorf("next attempt = %s, want the schedule restored to %s", nextAttempt, due)
	}
	if !held {
		t.Error("the entry is not held, so a reachability kick would filter it out")
	}

	// After the thaw the very next tick sends it, without having lost
	// anything to the freeze.
	svc.retryDispatchBarrier = nil
	svc.ThawOutgoingDeliveries([]protocol.MessageID{target})
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target) {
		t.Fatal("the message is still held back after the thaw")
	}
}
