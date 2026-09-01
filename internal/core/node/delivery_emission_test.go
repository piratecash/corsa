package node

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
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
	// marks and clears count DISK WRITES, not state. The contract is not
	// only "the row ends up right" but "an ordinary send pays for one
	// withdrawal and nothing after it", and a set alone cannot tell a
	// second identical write from no write at all.
	marks  int
	clears int
	// The second bit, and its own failure switch.
	onWire        map[protocol.MessageID]struct{}
	onWireErr     error
	onWireGate    chan struct{}
	onWireEntered chan struct{}
	stamps        int
	// alsoSent are ids the chatlog holds as ALREADY delivered — not
	// reseeded for retry, but still ours.
	alsoSent []protocol.MessageID
	// order is the sequence of journal entries, which is the only way to
	// state the lane's contract: not "the clear was fast" but "the clear
	// went in before the bookkeeping that was already queued".
	order []string
}

func newEmissionOutbox(rows ...OutboxEntry) *emissionOutbox {
	return &emissionOutbox{rows: rows, never: map[protocol.MessageID]struct{}{}, onWire: map[protocol.MessageID]struct{}{}}
}

// SentMessageIDs reports every row this fake knows about, delivered ones
// included: that is the point of the set it fills.
func (o *emissionOutbox) SentMessageIDs(limit int) ([]protocol.MessageID, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ids := make([]protocol.MessageID, 0, len(o.rows)+len(o.alsoSent))
	for _, row := range o.rows {
		ids = append(ids, row.Envelope.ID)
	}
	ids = append(ids, o.alsoSent...)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}

func (o *emissionOutbox) UndeliveredOutgoing() ([]OutboxEntry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rows := make([]OutboxEntry, 0, len(o.rows))
	for _, row := range o.rows {
		_, marked := o.never[row.Envelope.ID]
		_, stamped := o.onWire[row.Envelope.ID]
		// Read SEPARATELY, as the adapter does: two bits, two questions.
		row.Emitted = !marked
		row.OnWire = stamped
		rows = append(rows, row)
	}
	return rows, nil
}

// markNeverEmitted stands in for the INSERT that gives an outgoing row its
// claim. It is not on the node's journal interface on purpose: the delivery
// path must not be able to re-set that bit.
func (o *emissionOutbox) markNeverEmitted(ids ...protocol.MessageID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.marks++
	for _, id := range ids {
		o.never[id] = struct{}{}
	}
}

// failOnWire makes the stamp fail, so a test can check what the sender is
// told when the row cannot be made to agree.
func (o *emissionOutbox) failOnWire(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onWireErr = err
}

// stampWrites counts journal round trips, not ids: the contract for a
// backlog replay is ONE write for the batch.
func (o *emissionOutbox) stampWrites() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stamps
}

// blockOnWire makes MarkOnWire hang until the returned release is called —
// the stand-in for a contended SQLite parked on its busy timeout, which an
// instantly-failing fake could never reproduce.
//
// entered closes when a caller is actually INSIDE the journal. Without it
// a test racing something against "a parked repair" can win before the
// repair gets there and prove nothing.
func (o *emissionOutbox) blockOnWire() (entered <-chan struct{}, release func()) {
	gate := make(chan struct{})
	arrived := make(chan struct{})
	o.mu.Lock()
	o.onWireGate = gate
	o.onWireEntered = arrived
	o.mu.Unlock()
	return arrived, func() { close(gate) }
}

func (o *emissionOutbox) MarkOnWire(ids []protocol.MessageID) error {
	o.mu.Lock()
	gate, arrived := o.onWireGate, o.onWireEntered
	o.onWireEntered = nil
	o.mu.Unlock()
	if arrived != nil {
		close(arrived)
	}
	if gate != nil {
		<-gate
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.order = append(o.order, "stamp")
	if o.onWireErr != nil {
		return o.onWireErr
	}
	o.stamps++
	for _, id := range ids {
		o.onWire[id] = struct{}{}
	}
	return nil
}

// journalOrder is the sequence of entries into the journal so far.
func (o *emissionOutbox) journalOrder() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.order...)
}

// onWireStamped reports whether the row carries the confirmation bit.
func (o *emissionOutbox) onWireStamped(id protocol.MessageID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.onWire[id]
	return ok
}

func (o *emissionOutbox) ClearNeverEmitted(ids []protocol.MessageID) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.order = append(o.order, "clear")
	if o.clearErr != nil {
		return o.clearErr
	}
	o.clears++
	for _, id := range ids {
		delete(o.never, id)
	}
	return nil
}

// markOnDisk gives the row the claim its INSERT would have written.
func markOnDisk(t *testing.T, o *emissionOutbox, ids ...protocol.MessageID) {
	t.Helper()
	o.markNeverEmitted(ids...)
}

// writes reports how many disk writes of each kind the journal has taken.
func (o *emissionOutbox) writes() (marks, clears int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.marks, o.clears
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
	markOnDisk(t, outbox, held)

	if !outbox.marked(held) {
		t.Fatal("a withheld send left no durable record that it never went out")
	}

	// And the claim is withdrawn the moment it does go out — before the
	// frame is written, which is what noteOwnEnvelopesEmitted guarantees.
	svc.noteOwnEnvelopeEmitted(svc.Address(), held, time.Now().UTC())
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
	outbox.markNeverEmitted(unsent)

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

// TestReseedClaimsOnlyTheRowsThatCarryOne: registration assumes the row was
// BORN carrying the claim, which is true of a message this node has just
// authored and false of one the outbox says already went out.
//
// Leaving an emitted row in the claim set sent the first retry to SQLite to
// withdraw a claim that is not there — and a failed withdrawal WITHHOLDS
// the frame by contract, so a database that was briefly unavailable made a
// returning recipient wait on a write with nothing to write.
func TestReseedClaimsOnlyTheRowsThatCarryOne(t *testing.T) {
	t.Parallel()
	const (
		gone = protocol.MessageID("reseed-emitted-1")
		held = protocol.MessageID("reseed-withheld-1")
	)
	now := time.Now().UTC()
	outbox := newEmissionOutbox(
		OutboxEntry{Envelope: protocol.Envelope{ID: gone, Topic: "dm", Recipient: "peer-a", CreatedAt: now}},
		OutboxEntry{Envelope: protocol.Envelope{ID: held, Topic: "dm", Recipient: "peer-b", CreatedAt: now}},
	)
	// Only the withheld one still carries a claim on disk; the outbox
	// reports Emitted = !marked.
	markOnDisk(t, outbox, held)

	svc := newTestService(t, config.NodeTypeFull)
	svc.RegisterDeliveryOutbox(outbox)

	svc.deliveryMu.RLock()
	_, claimsEmitted := svc.markedNeverEmitted[gone]
	_, claimsHeld := svc.markedNeverEmitted[held]
	svc.deliveryMu.RUnlock()

	if claimsEmitted {
		t.Error("an already-emitted row was given a standing never-emitted claim")
	}
	if !claimsHeld {
		t.Error("the standing claim of a withheld row was not tracked, so no emission can withdraw it")
	}
}

// TestOrdinarySendWithdrawsItsBirthClaimExactlyOnce is the shape of the
// contract after the claim moved into the row's own insert.
//
// Every outgoing row is BORN carrying the claim, so an ordinary send does
// not have to write one — it has to WITHDRAW one, once, before its first
// frame. What must not happen is the withdrawal being repaid on every
// subsequent attempt: that would put a SQLite write on the retry path,
// which is the one place the two-phase design exists to keep clean.
func TestOrdinarySendWithdrawsItsBirthClaimExactlyOnce(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const plain = protocol.MessageID("plain-1")
	// The row is born marked; the node records the standing claim when it
	// registers the delivery.
	markOnDisk(t, outbox, plain)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: plain, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()
	marksBefore, _ := outbox.writes()

	svc.noteOwnEnvelopeEmitted(svc.Address(), plain, time.Now().UTC())
	if outbox.marked(plain) {
		t.Fatal("the first emission did not withdraw the birth claim")
	}
	_, clearsAfterFirst := outbox.writes()
	if clearsAfterFirst != 1 {
		t.Fatalf("first emission wrote %d clears, want exactly 1", clearsAfterFirst)
	}

	// Every later emission of the same message is free.
	svc.noteOwnEnvelopeEmitted(svc.Address(), plain, time.Now().UTC())
	svc.noteOwnEnvelopeEmitted(svc.Address(), plain, time.Now().UTC())

	marksAfter, clearsAfter := outbox.writes()
	if clearsAfter != clearsAfterFirst {
		t.Errorf("a repeat emission paid %d extra clears", clearsAfter-clearsAfterFirst)
	}
	if marksAfter != marksBefore {
		t.Errorf("an ordinary send wrote %d marks of its own", marksAfter-marksBefore)
	}
}

// TestReceiptIsNotHandledUntilItsStatusIsRecorded covers the two ends of a
// message's life in the two-bit model, and what happens when the middle
// fails.
//
// A receipt writes NO journal. `delivered` is the stronger durable fact
// and the same handler writes it: the badge ranks it above `sent`, so the
// queued override never consults the on-wire stamp, and the reseed query
// selects `delivery_status = 'sent'`, so the row cannot come back.
//
// When that status write fails, the receipt is NOT handled: its dedup key
// is withdrawn and the peer is not acked, so their copy — which outlives
// every cap and both nodes' restarts — comes back and is processed again.
// A withdrawal, at the other end, destroys the row, so its in-memory claim
// has to go with it or the set grows for the life of the process.
func TestReceiptIsNotHandledUntilItsStatusIsRecorded(t *testing.T) {
	t.Parallel()

	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const (
		received  = protocol.MessageID("settled-by-receipt")
		withdrawn = protocol.MessageID("settled-by-withdrawal")
	)

	for _, tc := range []struct {
		name        string
		statusLands bool
	}{
		{"status lands — handled, ackable, deduped", true},
		{"status refused — un-handled, not ackable, not deduped", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			outbox := newEmissionOutbox()
			svc.RegisterDeliveryOutbox(outbox)
			svc.RegisterMessageStore(&receiptStatusStore{accept: tc.statusLands})

			now := time.Now().UTC()
			for _, id := range []protocol.MessageID{received, withdrawn} {
				markOnDisk(t, outbox, id)
				svc.deliveryMu.Lock()
				svc.registerAwaitingDeliveredLocked(protocol.Envelope{
					ID: id, Topic: "dm", Sender: svc.Address(), Recipient: recipient, CreatedAt: now,
				}, now, true)
				svc.deliveryMu.Unlock()
			}

			receipt := protocol.DeliveryReceipt{
				MessageID:   received,
				Sender:      recipient,
				Recipient:   svc.Address(),
				Status:      protocol.ReceiptStatusDelivered,
				DeliveredAt: now,
			}
			outcome := svc.storeDeliveryReceipt(receipt)
			if _, err := svc.CancelOutgoingDelivery(withdrawn, domain.PeerIdentityFromWire(recipient)); err != nil {
				t.Fatalf("CancelOutgoingDelivery: %v", err)
			}
			svc.WaitBackground()

			if outbox.stampWrites() != 0 {
				t.Error("handling a receipt wrote to the emission journal; the delivered status already outranks it")
			}
			if outcome.ackable != tc.statusLands {
				t.Errorf("ackable=%v for a status write that landed=%v; acking destroys the peer's only copy of the fact",
					outcome.ackable, tc.statusLands)
			}
			if deduped := svc.receiptAlreadySeen(receipt); deduped != tc.statusLands {
				t.Errorf("dedup key present=%v after a status write that landed=%v; a suppressed re-send is a fact lost for good",
					deduped, tc.statusLands)
			}
			svc.deliveryMu.RLock()
			_, withdrawalLeak := svc.markedNeverEmitted[withdrawn]
			svc.deliveryMu.RUnlock()
			if withdrawalLeak {
				t.Error("a withdrawn message left its claim in the set; the row it belongs to is gone")
			}
		})
	}
}

// TestReSentReceiptIsAcceptedAfterTheStoreRecovers is the other half of the
// contract above: withdrawing the dedup key is only worth doing if the
// peer's next copy actually gets through.
func TestReSentReceiptIsAcceptedAfterTheStoreRecovers(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	store := &receiptStatusStore{accept: false}
	svc.RegisterMessageStore(store)

	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const target = protocol.MessageID("re-sent-after-failure")
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient, CreatedAt: now,
	}, now, true)
	svc.deliveryMu.Unlock()

	receipt := protocol.DeliveryReceipt{
		MessageID:   target,
		Sender:      recipient,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: now,
	}
	if outcome := svc.storeDeliveryReceipt(receipt); outcome.ackable {
		t.Fatal("a receipt whose status write failed was declared ackable")
	}

	// The database recovers and the peer, never acked, sends it again.
	store.mu.Lock()
	store.accept = true
	store.mu.Unlock()
	outcome := svc.storeDeliveryReceipt(receipt)
	svc.WaitBackground()

	if !outcome.ackable {
		t.Error("the re-sent receipt was not accepted, so the peer keeps it forever")
	}
	if got := store.updates(); got != 2 {
		t.Errorf("the store saw %d attempts; the re-send was suppressed by dedup instead of retried", got)
	}
}

// storedReceipt is the old two-value shorthand the older receipt tests
// use: did this node accept the receipt as new?
func storedReceipt(svc *Service, receipt protocol.DeliveryReceipt) bool {
	return svc.storeDeliveryReceipt(receipt).stored
}

// receiptStatusStore is a MessageStore that answers the one question these
// tests ask: did the delivery status reach the chatlog?
type receiptStatusStore struct {
	mu      sync.Mutex
	accept  bool
	updated int
	// park lets a test hold a write open, which is the only way to reach
	// the window between deciding a receipt is storable and committing it.
	hold    chan struct{}
	entered chan struct{}
}

// park makes writes hang until release is called, and signals ONCE PER
// CALLER that has arrived inside.
//
// Per caller, not once: a test that arranges two copies racing has to wait
// for both to be in there. Waiting on a duration instead would let a slow
// machine release the first copy before the second arrived, and the test
// would then exercise the ordinary duplicate fast-path and pass without
// the thing it is checking.
//
// Both channels are returned as locals on purpose: the previous version of
// this fixture had the test read the struct field while the store wrote
// it, which is a data race and, on a bad schedule, a nil channel.
func (r *receiptStatusStore) park() (entered <-chan struct{}, release func()) {
	gate, arrived := make(chan struct{}), make(chan struct{}, 8)
	r.mu.Lock()
	r.hold, r.entered = gate, arrived
	r.mu.Unlock()
	return arrived, func() { close(gate) }
}

func (r *receiptStatusStore) StoreMessage(protocol.Envelope, bool) StoreResult { return StoreInserted }

func (r *receiptStatusStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool {
	r.mu.Lock()
	r.updated++
	hold, arrived, accept := r.hold, r.entered, r.accept
	r.mu.Unlock()
	if arrived != nil {
		select {
		case arrived <- struct{}{}:
		default:
		}
	}
	if hold != nil {
		<-hold
	}
	return accept
}

func (r *receiptStatusStore) updates() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.updated
}

// TestReceiptHandlerDoesNotWaitOnTheJournal: a receipt carries three things
// the user and the peer are waiting for — the delivery status, the UI event
// and the ack_delete that stops the peer re-sending — and none of them may
// queue behind bookkeeping.
//
// The stamp used to run first and synchronously, so a receipt arriving
// during a reconnect burst waited for every stamp already in the lane, each
// of which can sit on SQLite's busy timeout.
func TestReceiptHandlerDoesNotWaitOnTheJournal(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)
	// Refusing the status update is what makes the stamp live at all — the
	// test would be inert against a store that accepts it.
	svc.RegisterMessageStore(&receiptStatusStore{accept: false})

	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const target = protocol.MessageID("receipt-during-a-backlog")
	now := time.Now().UTC()
	markOnDisk(t, outbox, target)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient, CreatedAt: now,
	}, now, true)
	svc.deliveryMu.Unlock()

	// A stamp parks inside the journal, as a contended database makes it.
	entered, release := outbox.blockOnWire()
	go func() { svc.markDeliveryOnWire("unrelated-backlog-stamp") }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("nothing reached the journal, so nothing was parked")
	}

	handled := make(chan struct{})
	go func() {
		defer close(handled)
		svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
			MessageID:   target,
			Sender:      recipient,
			Recipient:   svc.Address(),
			Status:      protocol.ReceiptStatusDelivered,
			DeliveredAt: now,
		})
	}()
	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("the receipt handler waited for a stamp parked in the journal")
	}
	release()
	svc.WaitBackground()
}

// TestGossipOfOurOwnMessageTakesThePreWireGate: gossip is a real path to
// the wire, and a path to the wire that skips the gate is how a message
// its author has just recalled still reaches the recipient.
//
// The fan-out hands jobs to a bounded pool, so a job can be queued before
// the deletion and run after it. The deletion classifies the message as
// never-emitted — correctly, at the moment it looks — writes no peer-side
// delete, and destroys the row. Then the job sends. Nothing is left that
// could ever recall that copy.
func TestGossipOfOurOwnMessageTakesThePreWireGate(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const addr = domain.PeerAddress("gossip-target:64650")
	sendCh := attachCapableRelayPeer(t, svc, string(addr), domain.PeerIdentityFromWire(peerID.Address))

	const target = protocol.MessageID("gossip-tracked-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: peerID.Address,
		CreatedAt: time.Now().UTC(),
	}
	svc.executeGossipTargets(envelope, []domain.PeerAddress{addr},
		deliveryDispatchRef{Envelope: envelope, DispatchedAt: time.Now().UTC()})

	item := awaitQueuedItem(t, sendCh, string(target))
	if !item.carriesDelivery() {
		t.Fatal("a gossip frame carrying one of our own messages reached the queue with no delivery to answer for; the session writer will neither gate it nor confirm it")
	}
	if item.delivery.Envelope.ID != target {
		t.Errorf("the frame names delivery %q, want %q", item.delivery.Envelope.ID, target)
	}
}

// TestTransitGossipCarriesNoDelivery keeps the change above off other
// people's traffic: a transit envelope has no delivery of ours to answer
// for, and putting one on it would make this node confirm, charge and
// announce a message it does not own.
func TestTransitGossipCarriesNoDelivery(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const addr = domain.PeerAddress("transit-target:64651")
	sendCh := attachCapableRelayPeer(t, svc, string(addr), domain.PeerIdentityFromWire(peerID.Address))

	const target = protocol.MessageID("gossip-transit-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: senderID.Address, Recipient: peerID.Address,
		CreatedAt: time.Now().UTC(),
	}
	svc.executeGossipTargets(envelope, []domain.PeerAddress{addr}, deliveryDispatchRef{})

	if awaitQueuedItem(t, sendCh, string(target)).carriesDelivery() {
		t.Fatal("a transit gossip frame reported a delivery for a message this node does not own")
	}
}

// awaitQueuedItem waits for the gossip pool to hand the frame for id to a
// session queue. The fan-out is asynchronous by design, so a bare receive
// would be a flake and a bare poll would pass on an empty queue.
func awaitQueuedItem(t *testing.T, sendCh chan peerSendItem, id string) peerSendItem {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case item := <-sendCh:
			if item.Item != nil && item.Item.ID == id {
				return item
			}
		case <-deadline:
			t.Fatalf("no gossip frame for %s reached the peer queue", id)
		}
	}
}

// TestDiscardedSessionQueueLeavesTheMessageQueued: the third place a frame
// can die without reaching the wire.
//
// The retry tick withdraws the durable claim BEFORE it hands the envelope
// to the sinks, so a frame still sitting in a session's queue when that
// session is torn down has a row saying the message left this machine and
// no writer that ever saw it. Unlike a writer refusal there is no status
// to classify: a discarded queue element provably never reached NetCore.
func TestDiscardedSessionQueueLeavesTheMessageQueued(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const stranded = protocol.MessageID("stranded-in-queue-1")
	envelope := protocol.Envelope{
		ID: stranded, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		CreatedAt: time.Now().UTC(),
	}
	markOnDisk(t, outbox, stranded)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()

	session := &peerSession{
		address:    domain.PeerAddress("peer-a:1234"),
		sendCh:     make(chan peerSendItem, 4),
		onStranded: svc.recordStrandedDeliveries,
	}
	dispatchedAt := time.Now().UTC()
	if !svc.clearedToWrite(deliveryDispatchRef{Envelope: envelope, DispatchedAt: dispatchedAt}, dispatchedAt) {
		t.Fatal("the gate withheld a message nothing has objected to")
	}
	if !session.enqueueSend(deliveryPeerSendItem(protocol.Frame{Type: "push_message"}, envelope, dispatchedAt)) {
		t.Fatal("the queue refused the frame")
	}

	// The session dies with the frame still in it.
	session.discardSendQueue()

	// NOTHING is written, and nothing needs to be: no sink confirmed, so
	// the row carries no on-wire stamp and the sender still reads queued.
	// The never-emitted claim stays withdrawn, which is the right answer
	// for the OTHER reader — the frame was handed to a queue, and a
	// deletion asking the peer about it costs one unresolved id.
	if outbox.onWireStamped(stranded) {
		t.Error("a frame discarded with its queue was stamped as on the wire")
	}
	if status := svc.storedMessageStatus(stranded); status != protocol.MessageStatusQueued {
		t.Errorf("the stranded message reads as %q, want %q", status, protocol.MessageStatusQueued)
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
	markOnDisk(t, outbox, held)

	outbox.failClears(errors.New("database is locked"))
	if svc.noteOwnEnvelopeEmitted(svc.Address(), held, time.Now().UTC()) {
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
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), held, time.Now().UTC()) {
		t.Fatal("the retry was still withheld after the journal recovered")
	}
	if outbox.marked(held) {
		t.Error("the claim survived the emission that disproved it")
	}
}

// TestOrdinarySendWithdrawsItsDurableClaim is the counterpart to the row
// being born marked. The withdrawal is gated on this process knowing a
// claim stands for the id, so a registration that did not record one left
// the mark on disk forever: the message read as queued on every reopen,
// came back unsent after a restart, and a deletion decided the recipient
// had never seen it.
func TestOrdinarySendWithdrawsItsDurableClaim(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const plain = protocol.MessageID("ordinary-1")
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: plain, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()
	// The row was written with the claim by the store adapter; this is the
	// same starting state.
	markOnDisk(t, outbox, plain)
	if !outbox.marked(plain) {
		t.Fatal("the row does not carry the claim, so this test proves nothing about withdrawing it")
	}

	if !svc.noteOwnEnvelopeEmitted(svc.Address(), plain, time.Now().UTC()) {
		t.Fatal("the send was withheld")
	}
	if outbox.marked(plain) {
		t.Fatal("an ordinary send left the durable never-emitted claim on the row")
	}
}

// TestBacklogReplayWithholdsEveryStrandedId: a claim that cannot be
// withdrawn withholds its frame, and since the withdrawal is one
// all-or-nothing statement, a failure withholds every id it named.
//
// This used to assert that an unmarked message went out anyway. There are
// no unmarked outgoing messages any more — a row is BORN carrying the
// claim (message_store_adapter) so the durable answer is true from the
// insert — and the property that survives is the one that matters: while
// the disk still says a message never left, its frame does not go out, and
// nothing is recorded as emitted.
func TestBacklogReplayWithholdsEveryStrandedId(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const (
		held  = protocol.MessageID("backlog-held")
		plain = protocol.MessageID("backlog-plain")
	)
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: held, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), true)
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: plain, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
	}, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()
	markOnDisk(t, outbox, held, plain)
	outbox.failClears(errors.New("database is locked"))

	outcome := svc.noteOwnEnvelopesEmitted([]protocol.MessageID{held, plain}, time.Now().UTC())
	for _, id := range []protocol.MessageID{held, plain} {
		if _, blocked := outcome.Withheld[id]; !blocked {
			t.Errorf("%s was cleared to go out while the disk still claims it never left", id)
		}
		svc.deliveryMu.RLock()
		entry, awaiting := svc.awaitingDelivered[id]
		svc.deliveryMu.RUnlock()
		if !awaiting {
			t.Fatalf("%s left the retry engine, so nothing will send it later", id)
		}
		if entry.Emitted {
			t.Errorf("%s counts as emitted although nothing went out", id)
		}
		if !outbox.marked(id) {
			t.Errorf("the durable claim on %s was dropped even though the write failed", id)
		}
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

	if svc.noteOwnEnvelopeEmitted(svc.Address(), target, time.Now().UTC()) {
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
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target, time.Now().UTC()) {
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
// reaches past the retry engine. A message can be dropped from
// awaitingDelivered while its durable "never emitted" claim still stands —
// its own TTL expired, or the process is between the two — and the backlog
// replay can still hand it to the peer the moment they connect. Without
// withdrawing the claim there, deleting that message would skip the peer
// and leave their copy for good.
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
	markOnDisk(t, outbox, target)
	if !outbox.marked(target) {
		t.Fatal("the withheld message was not recorded")
	}

	// The retry engine gives up: the entry is gone, the claim is not.
	svc.deliveryMu.Lock()
	delete(svc.awaitingDelivered, target)
	svc.deliveryMu.Unlock()

	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target, time.Now().UTC()) {
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

// failingStore is a message store whose write fails — the disk is full,
// the database is locked, the file is gone.
type failingStore struct {
	mu     sync.Mutex
	fail   protocol.MessageID
	stored []protocol.MessageID
}

func (f *failingStore) StoreMessage(envelope protocol.Envelope, _ bool) StoreResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if envelope.ID == f.fail {
		return StoreFailed
	}
	f.stored = append(f.stored, envelope.ID)
	return StoreInserted
}

func (f *failingStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

// TestOwnMessageIsNotSentWhenItsRowCannotBeWritten: the local database
// comes FIRST for a message this node authors.
//
// Routing it anyway put the message on the wire while its own author had
// no record of it — the RPC answered message_stored, the UI drew a bubble
// no reload would bring back, and nothing reseeded it after a restart. The
// recipient could end up holding a message the sender cannot see, quote,
// resend or recall.
func TestOwnMessageIsNotSentWhenItsRowCannotBeWritten(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const target = protocol.MessageID("unwritable-outgoing-1")
	svc.RegisterMessageStore(&failingStore{fail: target})

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

	if stored {
		t.Fatal("a message that was never written was reported as stored")
	}
	if errCode != protocol.ErrCodeStoreFailed {
		t.Fatalf("the author was told %q, want %q", errCode, protocol.ErrCodeStoreFailed)
	}
	if backlogHas(svc, target) {
		t.Error("the message entered the backlog, from which gossip and relay serve it")
	}
	svc.deliveryMu.RLock()
	_, awaiting := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if awaiting {
		t.Error("a sender-side retry was registered for a message with no row to reseed from")
	}
	svc.gossipMu.RLock()
	marked := svc.seen.Has(string(target))
	svc.gossipMu.RUnlock()
	if marked {
		t.Error("the id was marked seen, so a retry of the same send would be deduped away")
	}
}

// TestIncomingMessageSurvivesAFailedWrite is the other half of the rule
// above, and the reason it is scoped to our OWN messages. A write failure
// on someone else's message costs us the local copy; dropping the message
// from the runtime as well would lose it from the network too, for a peer
// who has already done everything right.
func TestIncomingMessageSurvivesAFailedWrite(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	const target = protocol.MessageID("unwritable-incoming-1")
	svc.RegisterMessageStore(&failingStore{fail: target})

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

	if errCode != "" {
		t.Fatalf("an inbound message was refused with %q; the rule is scoped to our own", errCode)
	}
	if !stored {
		t.Error("an inbound message was dropped from the runtime on a local write failure")
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
	if svc.noteOwnEnvelopeEmitted(svc.Address(), target, time.Now().UTC()) {
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
// test could not reach: the freeze lands after the tick has planned the
// send and before the dispatch consults it.
//
// Blocking the send there is only half the answer. A message that comes
// back from a thaw carrying a spent attempt and up to eleven minutes of
// backoff can be abandoned by the very next tick — all without the network
// having been used once. The tick charges nothing until the wire takes the
// frame, so there is nothing to give back; this test is what keeps that
// true, including for the schedule.
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
		attempts, nextAttempt, emitted, held = entry.Attempts, entry.NextAttemptAt, entry.Emitted, entry.Hold != holdNone
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
	if !svc.noteOwnEnvelopeEmitted(svc.Address(), target, time.Now().UTC()) {
		t.Fatal("the message is still held back after the thaw")
	}
}

// TestBadgeStaysQueuedWhenTheStampCannotBeWritten: the sender is never
// told "sent" over a row that still reads queued.
//
// The announcement is claimed ONCE. So announcing on a failed stamp is not
// a cosmetic slip that the next attempt repairs — the next full reload of
// the conversation reads the row, puts the badge back to queued, and no
// further event is ever published for it. Staying at queued until the disk
// agrees is the only direction that cannot go backwards.
func TestBadgeStaysQueuedWhenTheStampCannotBeWritten(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)
	outbox.failOnWire(errors.New("database is locked"))

	const target = protocol.MessageID("stamp-failed-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		CreatedAt: time.Now().UTC(),
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, time.Now().UTC(), false)
	svc.deliveryMu.Unlock()

	svc.confirmEnvelopeOnWire(envelope, time.Now().UTC())
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	announced := svc.awaitingDelivered[target].Announced
	svc.deliveryMu.RUnlock()
	if announced {
		t.Error("the sender was told sent over a row that still reads queued; the next reload takes it back and nothing says it again")
	}
	if status := svc.storedMessageStatus(target); status != protocol.MessageStatusQueued {
		t.Errorf("the reply says %q, want %q", status, protocol.MessageStatusQueued)
	}

	// The database recovers and the next confirmation says it properly.
	outbox.failOnWire(nil)
	svc.confirmEnvelopeOnWire(envelope, time.Now().UTC().Add(time.Second))
	svc.WaitBackground()

	if !outbox.onWireStamped(target) {
		t.Fatal("the retry did not stamp the row")
	}
	svc.deliveryMu.RLock()
	announced = svc.awaitingDelivered[target].Announced
	svc.deliveryMu.RUnlock()
	if !announced {
		t.Error("the sender was never told, although the row now says sent")
	}
}

// TestBacklogReplayStampsTheWholeBatchAtOnce: a reconnect replays a whole
// conversation, and confirming per message cost a goroutine and a separate
// UPDATE for each of them — all queueing on the one journal mutex, behind
// which the withdrawals of freshly-typed messages were waiting too.
func TestBacklogReplayStampsTheWholeBatchAtOnce(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	now := time.Now().UTC()
	var batch []protocol.Envelope
	for i := range 25 {
		id := protocol.MessageID(fmt.Sprintf("backlog-%02d", i))
		envelope := protocol.Envelope{
			ID: id, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", CreatedAt: now,
		}
		svc.deliveryMu.Lock()
		svc.registerAwaitingDeliveredLocked(envelope, now, false)
		svc.deliveryMu.Unlock()
		svc.confirmEnvelopeInMemory(envelope, now)
		batch = append(batch, envelope)
	}

	before := outbox.stampWrites()
	svc.confirmEnvelopesDurably(batch, true)
	if wrote := outbox.stampWrites() - before; wrote != 1 {
		t.Errorf("the replay took %d journal writes for %d messages, want 1", wrote, len(batch))
	}
	for _, envelope := range batch {
		if !outbox.onWireStamped(envelope.ID) {
			t.Fatalf("%s was left unstamped by the batch", envelope.ID)
		}
	}
}

// TestLocalBookkeepingIsRepairedWithoutResending: a local failure is
// repaired locally.
//
// Two things can leave a confirmed message half-recorded, and neither is
// the network's fault — the journal refusing the on-wire stamp, and the
// event bus shedding the announcement when a subscriber's inbox is full,
// which a reconnect replaying a whole conversation makes likely. Before
// this pass the only thing that retried either was the next NETWORK send:
// a "database is locked" put a message the peer had already taken back on
// the wire, and for a batch it did so for the whole batch.
func TestLocalBookkeepingIsRepairedWithoutResending(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)
	outbox.failOnWire(errors.New("database is locked"))

	const target = protocol.MessageID("repair-local-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		CreatedAt: time.Now().UTC(),
	}
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	svc.deliveryMu.Unlock()

	// The wire took it; the journal would not record it.
	svc.confirmEnvelopeOnWire(envelope, now)
	svc.WaitBackground()
	if outbox.onWireStamped(target) {
		t.Fatal("the stamp was supposed to fail, so this test proves nothing")
	}

	// The database recovers. THE TICK repairs the record — driven through
	// retryDueDeliveries rather than by calling the repair directly, so
	// the test also proves the tick runs it, and that the message does not
	// go near the network again: a confirmed entry sits on its backoff and
	// the tick's own dispatch skips it. The pass runs off the tick, so
	// the test waits for it rather than assuming it finished inline.
	outbox.failOnWire(nil)
	svc.retryDueDeliveries(now.Add(time.Second))
	svc.WaitBackground()
	svc.runLoopsWg.Wait()

	if !outbox.onWireStamped(target) {
		t.Error("the row was never stamped; only another network send would have repaired it")
	}
	svc.deliveryMu.RLock()
	entry := *svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if !entry.Announced {
		t.Error("the sender was never told, although the row now says sent")
	}
	// The repair is bookkeeping only: it must not disturb the schedule the
	// confirmation set, or it would pull the message forward for a re-send.
	if entry.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1: the repair charged another attempt", entry.Attempts)
	}
	if !entry.LastEmittedAt.Equal(now) {
		t.Error("the repair moved the confirmation stamp")
	}
}

// TestRepairPassIsCappedPerTick: republishing a whole backlog into a
// 64-slot inbox would shed most of it again and re-queue the same work
// forever. The pass drains steadily instead.
func TestRepairPassIsCappedPerTick(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	now := time.Now().UTC()
	const total = deliveryRepairBatch * 3
	for i := range total {
		id := protocol.MessageID(fmt.Sprintf("repair-%03d", i))
		envelope := protocol.Envelope{
			ID: id, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", CreatedAt: now,
		}
		svc.deliveryMu.Lock()
		svc.registerAwaitingDeliveredLocked(envelope, now, false)
		svc.deliveryMu.Unlock()
		svc.confirmEnvelopeInMemory(envelope, now)
	}

	svc.repairLocalDeliveryRecord(time.Now().UTC())

	stamped := 0
	for i := range total {
		if outbox.onWireStamped(protocol.MessageID(fmt.Sprintf("repair-%03d", i))) {
			stamped++
		}
	}
	if stamped != deliveryRepairBatch {
		t.Errorf("one pass repaired %d of %d, want exactly %d", stamped, total, deliveryRepairBatch)
	}
	if writes := outbox.stampWrites(); writes != 1 {
		t.Errorf("one pass took %d journal writes, want 1", writes)
	}
}

// TestShedAnnouncementBacksOffAndCostsNoJournalWrite is the correction to
// the first version of the repair pass, whose whole argument was "16 is
// less than the inbox's 64". That arithmetic does not hold: an inbox that
// is ALREADY full sheds every new event whatever the batch size, so under
// sustained backpressure the pass re-picked the same work every two
// seconds — and dragged a SQLite write along with each shed event, because
// both debts were keyed on "not announced".
//
// Two things fix it, and this pins both: a landed stamp is remembered, so
// it is never redone; and a shed announcement waits.
func TestShedAnnouncementBacksOffAndCostsNoJournalWrite(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	const target = protocol.MessageID("shed-announcement-1")
	envelope := protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a",
		CreatedAt: time.Now().UTC(),
	}
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	svc.deliveryMu.Unlock()
	svc.confirmEnvelopeInMemory(envelope, now)

	// A wedged subscriber: every event is shed.
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[target].Stamped = false
	svc.deliveryMu.Unlock()
	svc.repairLocalDeliveryRecord(now)
	writesAfterFirst := outbox.stampWrites()
	if writesAfterFirst == 0 {
		t.Fatal("the first pass did not stamp the row, so this test proves nothing")
	}

	svc.deliveryMu.Lock()
	entry := svc.awaitingDelivered[target]
	stamped := entry.Stamped
	// Simulate the bus having shed the announcement.
	entry.Announced = false
	entry.AnnounceAfter = now.Add(deliveryAnnounceRetry)
	svc.deliveryMu.Unlock()
	if !stamped {
		t.Fatal("the landed stamp was not remembered; the next pass will rewrite it")
	}

	// The next tick must find NOTHING to do for this id: the stamp is
	// remembered and the announcement is still backing off.
	svc.repairLocalDeliveryRecord(now.Add(2 * time.Second))
	if writes := outbox.stampWrites(); writes != writesAfterFirst {
		t.Errorf("a shed announcement dragged %d extra journal write(s) with it", writes-writesAfterFirst)
	}
	svc.deliveryMu.RLock()
	announced := svc.awaitingDelivered[target].Announced
	svc.deliveryMu.RUnlock()
	if announced {
		t.Error("the announcement was republished while its backoff was still running")
	}

	// Once the backoff expires it is offered again — and still without
	// touching the journal.
	svc.repairLocalDeliveryRecord(now.Add(deliveryAnnounceRetry + time.Second))
	svc.deliveryMu.RLock()
	announced = svc.awaitingDelivered[target].Announced
	svc.deliveryMu.RUnlock()
	if !announced {
		t.Error("the announcement was never retried after its backoff expired")
	}
	if writes := outbox.stampWrites(); writes != writesAfterFirst {
		t.Errorf("the announcement retry took %d extra journal write(s)", writes-writesAfterFirst)
	}
}

// TestPreWireClearGoesAheadOfQueuedBookkeeping states what the emission
// lane actually promises, which is an ORDER and not a speed.
//
// A clear cannot preempt a statement already running — no lock and no
// queue can make SQLite abandon an UPDATE mid-flight. What it must never
// do is wait behind the OTHER stamps: a reconnect confirms a whole
// conversation while the same reconnect is trying to send the first
// message of the night, and with the ordering left to SQLite's busy
// handler the clear can lose that race repeatedly and, on running out of
// busy_timeout, withhold the user's message.
//
// So the assertion is on the sequence of journal entries: after the
// in-flight stamp, the clear goes next, ahead of everything queued before
// it arrived.
func TestPreWireClearGoesAheadOfQueuedBookkeeping(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	now := time.Now().UTC()
	const (
		repairing = protocol.MessageID("repair-in-flight")
		urgent    = protocol.MessageID("urgent-first-send")
	)
	repaired := protocol.Envelope{
		ID: repairing, Topic: "dm", Sender: svc.Address(), Recipient: "peer-a", CreatedAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(repaired, now, false)
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: urgent, Topic: "dm", Sender: svc.Address(), Recipient: "peer-b", CreatedAt: now,
	}, now, true)
	svc.deliveryMu.Unlock()
	svc.confirmEnvelopeInMemory(repaired, now)
	markOnDisk(t, outbox, urgent)

	// A repair pass parks inside the journal, as a contended database
	// makes it.
	entered, release := outbox.blockOnWire()
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		svc.repairLocalDeliveryRecord(now)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("the repair never reached the journal, so nothing was parked")
	}

	// The rest of the reconnect backlog piles up behind it.
	const backlog = 3
	stamped := make(chan struct{}, backlog)
	for i := range backlog {
		id := protocol.MessageID(fmt.Sprintf("backlog-stamp-%d", i))
		go func() {
			svc.markDeliveryOnWire(id)
			stamped <- struct{}{}
		}()
	}
	waitForLane(t, svc, 0, backlog+1)

	// Only NOW does the recipient of the other message come online. The
	// backlog was queued first; the clear still goes first.
	cleared := make(chan bool, 1)
	go func() {
		cleared <- svc.noteOwnEnvelopeEmitted(svc.Address(), urgent, time.Now().UTC())
	}()
	waitForLane(t, svc, 1, backlog+1)

	release()
	select {
	case ok := <-cleared:
		if !ok {
			t.Error("the urgent send was withheld")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pre-wire clear never completed")
	}
	<-parked
	for range backlog {
		<-stamped
	}

	order := outbox.journalOrder()
	if len(order) != backlog+2 {
		t.Fatalf("journal saw %v, want the parked stamp, the clear and %d more stamps", order, backlog)
	}
	if order[0] != "stamp" || order[1] != "clear" {
		t.Errorf("journal order was %v; the clear waited behind bookkeeping queued before it", order)
	}
	if outbox.marked(urgent) {
		t.Error("the claim was not withdrawn, so the frame cannot go out")
	}
}

// waitForLane blocks until the lane holds the expected number of waiting
// writers, so a test can arrange a queue without sleeping for one.
func waitForLane(t *testing.T, svc *Service, urgent, bookkeeping int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		gotUrgent, gotBookkeeping := svc.emissionLane.waiting()
		if gotUrgent == urgent && gotBookkeeping == bookkeeping {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lane holds %d urgent and %d bookkeeping writers, want %d and %d",
				gotUrgent, gotBookkeeping, urgent, bookkeeping)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestConcurrentPreWireClearsCostOneJournalCall is the other half of the
// lane's bound, and the half the ordering test cannot reach.
//
// The priority held only against BOOKKEEPING while withdrawals queued
// behind each other: several subscribers reconnecting at once each clear
// their own first frame, and the last of them would wait for all the rest.
// Refusing one is not an option — a refused withdrawal is a message
// withheld from the user — so the wait is bounded by COALESCING instead:
// everything waiting when the lane frees goes out as one journal call.
//
// It counts CALLS, which is what the lane controls. A call is not always
// one statement: chatlog chunks it at 128 ids per UPDATE, so the honest
// claim is that a withdrawal waits for its own batch and never another's,
// not that some fixed number of statements runs.
func TestConcurrentPreWireClearsCostOneJournalCall(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	outbox := newEmissionOutbox()
	svc.RegisterDeliveryOutbox(outbox)

	now := time.Now().UTC()
	const senders = 4
	ids := make([]protocol.MessageID, 0, senders)
	for i := range senders {
		id := protocol.MessageID(fmt.Sprintf("reconnect-%d", i))
		ids = append(ids, id)
		markOnDisk(t, outbox, id)
		svc.deliveryMu.Lock()
		svc.registerAwaitingDeliveredLocked(protocol.Envelope{
			ID: id, Topic: "dm", Sender: svc.Address(),
			Recipient: fmt.Sprintf("peer-%d", i), CreatedAt: now,
		}, now, true)
		svc.deliveryMu.Unlock()
	}

	// Hold the lane with a statement nothing can preempt.
	entered, release := outbox.blockOnWire()
	go func() { svc.markDeliveryOnWire("holds-the-lane") }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("nothing reached the journal, so the lane was never held")
	}

	// Every subscriber's first frame arrives at once.
	cleared := make(chan bool, senders)
	for _, id := range ids {
		go func() { cleared <- svc.noteOwnEnvelopeEmitted(svc.Address(), id, time.Now().UTC()) }()
	}
	waitForLane(t, svc, senders, 1)

	release()
	for range senders {
		select {
		case ok := <-cleared:
			if !ok {
				t.Error("a send was withheld")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a pre-wire clear never completed")
		}
	}
	svc.WaitBackground()

	order := outbox.journalOrder()
	clears := 0
	for _, entry := range order {
		if entry == "clear" {
			clears++
		}
	}
	if clears != 1 {
		t.Errorf("%d concurrent withdrawals cost %d journal calls (%v); they must coalesce into one",
			senders, clears, order)
	}
	for _, id := range ids {
		if outbox.marked(id) {
			t.Errorf("%s was left claimed, so its frame cannot go out", id)
		}
	}
}

// TestLaneYieldsBetweenStatementsOfABigWrite is the bound the coalescing
// test cannot state: not how many TURNS are taken, but how long one is.
//
// A reconnect confirms a whole conversation in one journal call, and the
// journal writes that as one UPDATE per 128 ids. While the lane was held
// for the whole call, a message typed a moment later waited for every one
// of those statements — dozens of them, none of them its own work. The
// lane now releases between chunks, so a waiting withdrawal takes its turn
// after ONE.
func TestLaneYieldsBetweenStatementsOfABigWrite(t *testing.T) {
	t.Parallel()
	lane := newEmissionLane()

	backlog := make([]protocol.MessageID, 0, 4*maxIdsPerLaneWrite)
	for i := range 4 * maxIdsPerLaneWrite {
		backlog = append(backlog, protocol.MessageID(fmt.Sprintf("backlog-%d", i)))
	}

	var mu sync.Mutex
	var order []string
	firstChunk := make(chan struct{})
	releaseFirst := make(chan struct{})

	stamped := make(chan struct{})
	go func() {
		defer close(stamped)
		chunk := 0
		_, _ = lane.runBookkeeping(backlog, func(ids []protocol.MessageID) error {
			mu.Lock()
			order = append(order, "stamp")
			mu.Unlock()
			if chunk == 0 {
				close(firstChunk)
				<-releaseFirst
			}
			chunk++
			return nil
		})
	}()

	// The backlog is inside its FIRST statement when the fresh send arrives.
	<-firstChunk
	cleared := make(chan struct{})
	go func() {
		defer close(cleared)
		_ = lane.runPreWire([]protocol.MessageID{"typed-just-now"}, func([]protocol.MessageID) error {
			mu.Lock()
			order = append(order, "clear")
			mu.Unlock()
			return nil
		})
	}()
	for {
		if urgent, _ := lane.waiting(); urgent == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	close(releaseFirst)
	<-cleared
	<-stamped

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 3 {
		t.Fatalf("journal saw %v, want the backlog's chunks and the clear", order)
	}
	if order[1] != "clear" {
		t.Errorf("journal order was %v; the fresh send waited for %d foreign statements",
			order, len(order)-1)
	}
}

// TestFreshWithdrawalDoesNotInheritABigBatchesWork: coalescing must not
// make a newcomer wait for work that is not its own.
//
// While the lane was busy, a reconnect's whole-conversation clear formed a
// batch, and a single message typed a moment later JOINED it — waiting on
// the same completion, which came only after every chunk of that
// conversation. Absorption is capped at one statement's worth precisely so
// that cannot happen: a batch bigger than a statement takes its turns like
// anyone else, and the newcomer's own turn comes in between.
func TestFreshWithdrawalDoesNotInheritABigBatchesWork(t *testing.T) {
	t.Parallel()
	lane := newEmissionLane()

	conversation := make([]protocol.MessageID, 0, 4*maxIdsPerLaneWrite)
	for i := range 4 * maxIdsPerLaneWrite {
		conversation = append(conversation, protocol.MessageID(fmt.Sprintf("backlog-%d", i)))
	}

	// Something else is inside the journal, so the big clear has to queue.
	blocking := make(chan struct{})
	blocked := make(chan struct{})
	go func() {
		_, _ = lane.runBookkeeping([]protocol.MessageID{"holds-the-lane"}, func([]protocol.MessageID) error {
			close(blocked)
			<-blocking
			return nil
		})
	}()
	<-blocked

	var mu sync.Mutex
	var sizes []int
	record := func(ids []protocol.MessageID) error {
		mu.Lock()
		sizes = append(sizes, len(ids))
		mu.Unlock()
		return nil
	}

	bigDone := make(chan struct{})
	go func() {
		defer close(bigDone)
		_ = lane.runPreWire(conversation, record)
	}()
	for {
		if urgent, _ := lane.waiting(); urgent == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// The message the user just typed, arriving while that batch waits.
	freshDone := make(chan struct{})
	freshAt := -1
	go func() {
		defer close(freshDone)
		_ = lane.runPreWire([]protocol.MessageID{"typed-just-now"}, func(ids []protocol.MessageID) error {
			mu.Lock()
			// Its own position, taken as it is written: reading the
			// length afterwards races the backlog's remaining turns.
			freshAt = len(sizes)
			mu.Unlock()
			return record(ids)
		})
	}()
	for {
		if urgent, _ := lane.waiting(); urgent == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	close(blocking)
	select {
	case <-freshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the fresh withdrawal never completed")
	}

	mu.Lock()
	at, sizesSeen := freshAt, append([]int(nil), sizes...)
	mu.Unlock()
	// It rides along in the batch's first statement (absorption) or takes
	// the very next turn — never after the conversation's four.
	if at > 1 {
		t.Errorf("the fresh withdrawal was written as statement %d (%v); it waited for another batch's work",
			at+1, sizesSeen)
	}
	// And the statement it rode in is ONE statement. Position alone does
	// not prove the bound: absorbing without a cap puts the newcomer in
	// the first write too, and that write is then the whole conversation
	// — which the journal splits into as many UPDATEs as it takes, with
	// the lane held for all of them.
	for i, size := range sizesSeen {
		if size > maxIdsPerLaneWrite {
			t.Errorf("statement %d carried %d ids (%v); a turn may carry %d, or it is several statements holding the lane",
				i+1, size, sizesSeen, maxIdsPerLaneWrite)
		}
	}
	<-bigDone
}

// TestBigWithdrawalYieldsItsTurnToAWaitingWithdrawal: FIFO, not luck.
//
// Between chunks the writer used to re-take the lane by competing for the
// same mutex as whoever was already waiting. A condition variable makes no
// promise about who wins that, so "the next turn goes to the waiter" was
// undetermined — and reliably wrong under load, because the running
// goroutine is the one already scheduled. Going to the BACK of the queue
// is what makes it a rule.
func TestBigWithdrawalYieldsItsTurnToAWaitingWithdrawal(t *testing.T) {
	t.Parallel()
	lane := newEmissionLane()

	conversation := make([]protocol.MessageID, 0, 3*maxIdsPerLaneWrite)
	for i := range 3 * maxIdsPerLaneWrite {
		conversation = append(conversation, protocol.MessageID(fmt.Sprintf("backlog-%d", i)))
	}

	var mu sync.Mutex
	var order []string
	firstChunk := make(chan struct{})
	releaseFirst := make(chan struct{})
	var chunks int

	bigDone := make(chan struct{})
	go func() {
		defer close(bigDone)
		_ = lane.runPreWire(conversation, func(ids []protocol.MessageID) error {
			mu.Lock()
			order = append(order, "backlog")
			chunks++
			first := chunks == 1
			mu.Unlock()
			if first {
				close(firstChunk)
				<-releaseFirst
			}
			return nil
		})
	}()

	// The fresh send arrives while the backlog is inside its first
	// statement, so it cannot have been absorbed into it.
	<-firstChunk
	freshDone := make(chan struct{})
	go func() {
		defer close(freshDone)
		_ = lane.runPreWire([]protocol.MessageID{"typed-just-now"}, func([]protocol.MessageID) error {
			mu.Lock()
			order = append(order, "fresh")
			mu.Unlock()
			return nil
		})
	}()
	for {
		if urgent, _ := lane.waiting(); urgent == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	close(releaseFirst)
	<-freshDone
	<-bigDone

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 || order[1] != "fresh" {
		t.Errorf("journal order was %v; the backlog kept re-taking the lane instead of yielding after one statement", order)
	}
}

// TestLaneTurnsAwayAStampItCannotQueue: the bookkeeping queue is bounded,
// and overflow must be a REFUSAL rather than a write nobody waits for.
//
// A stamp the lane declines is not lost: markDeliveryOnWire answers false,
// so no "sent" is announced over a row that still reads queued, and the
// debt stays derivable from state for the repair pass to find.
func TestLaneTurnsAwayAStampItCannotQueue(t *testing.T) {
	t.Parallel()
	lane := newEmissionLane()

	held := make(chan struct{})
	admittedFirst := make(chan struct{})
	go func() {
		_, _ = lane.runBookkeeping([]protocol.MessageID{"a"}, func([]protocol.MessageID) error {
			close(admittedFirst)
			<-held
			return nil
		})
	}()
	<-admittedFirst

	// Fill the queue behind it.
	for len(lane.bookkeeping) < maxWaitingStampWrites {
		go func() {
			_, _ = lane.runBookkeeping([]protocol.MessageID{"b"}, func([]protocol.MessageID) error { <-held; return nil })
		}()
		time.Sleep(time.Millisecond)
	}

	written := false
	admitted, err := lane.runBookkeeping([]protocol.MessageID{"c"}, func([]protocol.MessageID) error {
		written = true
		return nil
	})
	if admitted {
		t.Error("the lane admitted a stamp past its bound")
	}
	if written {
		t.Error("a refused stamp still wrote to the journal")
	}
	if err != nil {
		t.Errorf("a refusal is not an error: %v", err)
	}
	close(held)
}

// TestStaleSnapshotDoesNotOverwriteANewerStatus: the pass publishes a COPY
// while the map can move on.
//
// A `seen` can arrive while an older `delivered` is in flight. Removing
// the debt unconditionally on success deleted the newer one; re-keeping a
// re-shed stale copy overwrote it. Either way the badge settled on a
// status the peer had already moved past.
func TestStaleSnapshotDoesNotOverwriteANewerStatus(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	const target = protocol.MessageID("stale-snapshot-1")
	older := protocol.LocalChangeEvent{
		Type: protocol.LocalChangeReceiptUpdate, Topic: "dm",
		MessageID: string(target), Status: protocol.ReceiptStatusDelivered,
	}
	newer := older
	newer.Status = protocol.ReceiptStatusSeen

	svc.keepShedEvent(ebus.TopicReceiptUpdated, older)
	svc.deliveryMu.RLock()
	stale := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()

	// The newer status arrives while the older copy is in flight.
	svc.keepShedEvent(ebus.TopicReceiptUpdated, newer)

	// The in-flight OLDER copy now succeeds. It must not take the newer
	// one with it.
	svc.forgetShedEvent(target, stale.seq, older.Status)
	svc.deliveryMu.RLock()
	kept, present := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	if !present {
		t.Fatal("a stale publish deleted the newer status; the badge stops at delivered")
	}
	if kept.event.Status != protocol.ReceiptStatusSeen {
		t.Fatalf("kept status = %q, want %q", kept.event.Status, protocol.ReceiptStatusSeen)
	}

	// And a re-shed of the stale copy must not overwrite it either.
	svc.keepShedEvent(ebus.TopicReceiptUpdated, older)
	svc.deliveryMu.RLock()
	kept = svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	if kept.event.Status != protocol.ReceiptStatusSeen {
		t.Errorf("a re-shed stale copy moved the badge back to %q", kept.event.Status)
	}

	// The newer one settling clears the debt.
	svc.deliveryMu.RLock()
	current := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	svc.forgetShedEvent(target, current.seq, newer.Status)
	svc.deliveryMu.RLock()
	_, stillOwed := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	if stillOwed {
		t.Error("the settled debt was not removed")
	}
}

// TestShedReceiptEventOutlivesTheRetryEntry: the debt cannot live only on
// the retry entry, because the receipt DELETES it.
//
// If the emitted event and the receipt update are shed in the same burst —
// which is what a reconnect replaying a conversation produces — the entry
// is gone and nothing is left to repeat. The conversation then shows
// "queued" for a message the peer has read until the user reopens it.
//
// Driven through repairLocalDeliveryRecord, not through the store by hand:
// the first version of the pass DELETED a due event before publishing it
// and then returned early, losing it for good, and only the real pass can
// show that.
func TestShedReceiptEventOutlivesTheRetryEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	const target = protocol.MessageID("shed-receipt-1")
	event := protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeReceiptUpdate,
		Topic:     "dm",
		MessageID: string(target),
		Status:    protocol.ReceiptStatusDelivered,
	}

	// A wedged subscriber on a REAL bus: the handler never returns, so the
	// inbox fills and the bus sheds. (newTestService leaves eventBus nil,
	// and a nil bus never sheds, so the bare fixture proves nothing.)
	svc.eventBus = ebus.New()
	wedge := make(chan struct{})
	svc.eventBus.Subscribe(ebus.TopicReceiptUpdated, func(protocol.LocalChangeEvent) { <-wedge })
	for range 200 {
		svc.publishRetryableLocalChange(ebus.TopicReceiptUpdated, event)
	}

	svc.deliveryMu.RLock()
	_, kept := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	if !kept {
		close(wedge)
		t.Fatal("a shed receipt event was dropped; only a full reload would correct the badge")
	}

	// A pass while the subscriber is still wedged must LEAVE the debt in
	// place: the publish fails, so nothing is settled.
	now := time.Now().UTC()
	svc.repairLocalDeliveryRecord(now.Add(deliveryAnnounceRetry + time.Second))
	svc.deliveryMu.RLock()
	_, stillKept := svc.pendingUIEvents[target]
	svc.deliveryMu.RUnlock()
	if !stillKept {
		close(wedge)
		t.Fatal("the pass consumed the event without delivering it; nothing is left to retry")
	}

	// The subscriber drains. The next pass settles the debt and removes it.
	close(wedge)
	deadline := time.Now().Add(3 * time.Second)
	for {
		svc.repairLocalDeliveryRecord(time.Now().UTC().Add(deliveryAnnounceRetry + time.Second))
		svc.deliveryMu.RLock()
		_, owed := svc.pendingUIEvents[target]
		svc.deliveryMu.RUnlock()
		if !owed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the event was never delivered once the subscriber drained")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
