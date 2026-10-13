package node

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// msg_relay_retry_commit_test.go pins the invariant that ties a transit DM's
// relay retry entry to its backlog envelope: a `msg|` key in s.relayRetry
// exists only while the envelope it names is in s.topics["dm"].
// retryableRelayMessages, the only walk that visits those keys, iterates
// s.topics["dm"], so a key whose envelope is gone is never retried, never
// reaped and never expired. It holds a maxRelayRetryEntries slot, and it keeps
// the ack_delete suppression gate (recordRelayDeliveredWhileRelayed) open,
// which admits suppression records for a message this node no longer
// carries. The same file pins the companion invariant of that gate: a
// suppression set exists only while its envelope is backlogged.
//
// The tests run a removal path from inside Route. storeIncomingMessage asks
// the router only after the admission has released every domain mutex, so the
// hook lands exactly where a concurrent ack_delete or a concurrent admission
// that evicts the envelope would land in production.

// transitDMFixture is a full node that forwards transit DMs between two
// foreign identities whose keys it already knows.
type transitDMFixture struct {
	svc       *Service
	sender    *identity.Identity
	recipient *identity.Identity
}

func newTransitDMFixture(t *testing.T) transitDMFixture {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, sender)
	return transitDMFixture{svc: svc, sender: sender, recipient: recipient}
}

func (f transitDMFixture) message(t *testing.T, id protocol.MessageID) incomingMessage {
	t.Helper()
	return incomingMessage{
		ID:        id,
		Topic:     "dm",
		Sender:    f.sender.Address,
		Recipient: f.recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      sealDMBody(t, f.sender, f.recipient.Address, identity.BoxPublicKeyBase64(f.recipient.BoxPublicKey)),
	}
}

// store admits a transit DM and fails the test unless it was stored.
func (f transitDMFixture) store(t *testing.T, msg incomingMessage) {
	t.Helper()
	stored, _, errCode := f.svc.storeIncomingMessage(msg, false)
	if !stored || errCode != "" {
		t.Fatalf("transit DM %s must be admitted: stored=%v errCode=%q", msg.ID, stored, errCode)
	}
}

// onFirstRoute installs a router that runs hook on the first Route call only,
// so a hook that itself admits messages (and therefore routes them) is not
// re-entered.
func (f transitDMFixture) onFirstRoute(hook func()) *atomic.Bool {
	var fired atomic.Bool
	f.svc.router = routeThenChange{inner: f.svc.router, hook: func() {
		if fired.CompareAndSwap(false, true) {
			hook()
		}
	}}
	return &fired
}

// hasRelayRetryEntry reports whether s.relayRetry holds a msg| entry for id.
func (s *Service) hasRelayRetryEntry(id protocol.MessageID) bool {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	_, ok := s.relayRetry[relayMessageKey(id)]
	return ok
}

// backlogHasMessage reports whether s.topics["dm"] holds an envelope with id.
func backlogHasMessage(svc *Service, id protocol.MessageID) bool {
	svc.gossipMu.RLock()
	defer svc.gossipMu.RUnlock()
	for _, envelope := range svc.topics["dm"] {
		if envelope.ID == id {
			return true
		}
	}
	return false
}

// assertNoOrphanRelayMessageEntries checks the invariant over the whole map:
// every `msg|` key has its envelope in s.topics["dm"].
func assertNoOrphanRelayMessageEntries(t *testing.T, svc *Service) {
	t.Helper()
	svc.deliveryMu.RLock()
	svc.gossipMu.RLock()
	present := make(map[string]struct{}, len(svc.topics["dm"]))
	for _, envelope := range svc.topics["dm"] {
		present[relayMessageKey(envelope.ID)] = struct{}{}
	}
	var orphans []string
	for key := range svc.relayRetry {
		if !strings.HasPrefix(key, relayMessageKey("")) {
			continue
		}
		if _, ok := present[key]; !ok {
			orphans = append(orphans, key)
		}
	}
	svc.gossipMu.RUnlock()
	svc.deliveryMu.RUnlock()
	if len(orphans) > 0 {
		t.Fatalf("relayRetry holds %d msg| entries whose envelope is not in the dm backlog: %v", len(orphans), orphans)
	}
}

// The admission that puts a transit DM in the backlog arms its relay retry in
// the same commit, so a positive control comes first: without any removal the
// two halves are both there.
func TestStoreIncomingMessage_TransitCommitArmsRelayRetryWithTheBacklog(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("transit-commit-arms-1")

	f.store(t, f.message(t, id))
	f.svc.WaitBackground()

	if !backlogHasMessage(f.svc, id) {
		t.Fatal("the transit DM must be in the dm backlog after admission")
	}
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the admission that backlogged a transit DM must arm its relay retry")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// The recipient's subscriber received the push and acked it (ack_delete)
// before the storing goroutine got to the relay retry. ack_delete removes the
// envelope and its msg| key together; an arm that comes after it creates a key
// for an envelope no walk will ever visit.
func TestStoreIncomingMessage_AckDeleteAfterAdmissionLeavesNoOrphanRelayRetry(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("transit-acked-in-window-1")

	removed := 0
	fired := f.onFirstRoute(func() {
		removed = f.svc.deleteBacklogMessageForRecipient(f.recipient.Address, id)
	})

	f.store(t, f.message(t, id))
	f.svc.WaitBackground()

	if !fired.Load() || removed != 1 {
		t.Fatalf("ack_delete in the window removed %d envelopes (hook fired=%v), want 1: the test did not reach the window it is about", removed, fired.Load())
	}
	if backlogHasMessage(f.svc, id) {
		t.Fatal("the acked envelope must be gone from the dm backlog")
	}
	if f.svc.hasRelayRetryEntry(id) {
		t.Fatal("relayRetry holds msg| for an envelope ack_delete already removed: it is never retried, reaped or expired")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// A concurrent admission for the same recipient filled the per-recipient cap
// and evicted this envelope (the oldest) before the storing goroutine got to
// the relay retry. The eviction drops the evicted ids' msg| keys; an arm that
// comes after it re-creates one for an envelope that is gone.
func TestStoreIncomingMessage_TransitCapEvictionAfterAdmissionLeavesNoOrphanRelayRetry(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const victim = protocol.MessageID("transit-evicted-in-window-1")

	// Sealed up front so the hook does only the admissions.
	burst := make([]incomingMessage, 0, maxTransitPerRecipient)
	for i := 0; i < maxTransitPerRecipient; i++ {
		burst = append(burst, f.message(t, protocol.MessageID(fmt.Sprintf("transit-burst-%d", i))))
	}
	fired := f.onFirstRoute(func() {
		for _, msg := range burst {
			f.store(t, msg)
		}
	})

	f.store(t, f.message(t, victim))
	f.svc.WaitBackground()

	if !fired.Load() {
		t.Fatal("the hook never fired: the test did not reach the window it is about")
	}
	if backlogHasMessage(f.svc, victim) {
		t.Fatal("the burst must have evicted the oldest transit envelope for the recipient; the assertion below would prove nothing")
	}
	if f.svc.hasRelayRetryEntry(victim) {
		t.Fatal("relayRetry holds msg| for an envelope the transit cap already evicted: it is never retried, reaped or expired")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// The in-flight window closes on a transit envelope: the expiry sweep removes
// the envelope and its msg| entry in one deliveryMu → gossipMu section, so the
// retry walk, which no longer sees the envelope, has nothing left to reap.
func TestCleanupExpiredMessages_TransitExpiryDropsRelayRetryWithTheEnvelope(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("transit-window-closed-1")

	f.store(t, f.message(t, id))
	f.svc.WaitBackground()
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("fixture: the admission must arm the relay retry, or the assertion below proves nothing")
	}

	f.svc.gossipMu.Lock()
	for i := range f.svc.topics["dm"] {
		if f.svc.topics["dm"][i].ID == id {
			f.svc.topics["dm"][i].StoredAt = time.Now().UTC().Add(-transitInFlightWindow - time.Minute)
		}
	}
	f.svc.gossipMu.Unlock()

	f.svc.cleanupExpiredMessagesForce()

	if backlogHasMessage(f.svc, id) {
		t.Fatal("the transit envelope outlived its in-flight window")
	}
	if f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the expiry sweep removed the envelope but left its msg| relay retry entry")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// An origin-authored DM is the sender engine's (delivery_retry.go), never
// the relay retry contour's: the admission backlogs it but arms nothing.
func TestStoreIncomingMessage_OriginAuthoredAdmissionArmsNoRelayRetry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, recipient)
	const id = protocol.MessageID("origin-authored-admission-1")

	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:        id,
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey)),
	}, false)
	svc.WaitBackground()

	if !stored || errCode != "" {
		t.Fatalf("origin-authored DM must be admitted: stored=%v errCode=%q", stored, errCode)
	}
	if !backlogHasMessage(svc, id) {
		t.Fatal("origin-authored DM must be in the dm backlog; the assertion below would prove nothing")
	}
	if svc.hasRelayRetryEntry(id) {
		t.Fatal("an origin-authored DM must not be armed for relay retry")
	}
}

// A node that does not forward (a client node) still admits a transit DM —
// it is backlogged for a recipient that may fetch it here — but does not
// route it, so it must not own a relay retry entry either.
func TestStoreIncomingMessage_UnroutedTransitAdmissionArmsNoRelayRetry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeClient)
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, sender)
	f := transitDMFixture{svc: svc, sender: sender, recipient: recipient}
	const id = protocol.MessageID("unrouted-transit-admission-1")

	f.store(t, f.message(t, id))
	svc.WaitBackground()

	if svc.CanForward() {
		t.Fatal("fixture: a client node must not forward, or the test is about nothing")
	}
	if !backlogHasMessage(svc, id) {
		t.Fatal("the transit DM must be in the dm backlog; the assertion below would prove nothing")
	}
	if svc.hasRelayRetryEntry(id) {
		t.Fatal("a transit DM this node does not route must not be armed for relay retry")
	}
}

// A full relayRetry map leaves the new transit DM unarmed rather than
// evicting a live entry; the envelope itself is still backlogged.
func TestStoreIncomingMessage_FullRelayRetryMapLeavesTransitUnarmed(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("transit-full-map-1")

	// Receipt keys, so the fill itself cannot look like msg| orphans.
	f.svc.deliveryMu.Lock()
	for i := 0; i < maxRelayRetryEntries; i++ {
		f.svc.relayRetry[fmt.Sprintf("receipt|fill-%d", i)] = relayAttempt{FirstSeen: time.Now().UTC()}
	}
	f.svc.deliveryMu.Unlock()

	f.store(t, f.message(t, id))
	f.svc.WaitBackground()

	if !backlogHasMessage(f.svc, id) {
		t.Fatal("a full relayRetry map must not keep the transit DM out of the backlog")
	}
	if f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the arm went past maxRelayRetryEntries")
	}
	f.svc.deliveryMu.RLock()
	size := len(f.svc.relayRetry)
	f.svc.deliveryMu.RUnlock()
	if size != maxRelayRetryEntries {
		t.Fatalf("relayRetry holds %d entries, want exactly the cap %d: a live entry was evicted or the cap exceeded", size, maxRelayRetryEntries)
	}
}

// whileDeliveryMuHeld runs fn while the test holds deliveryMu exclusively and
// reports whether fn finished within the deadline. fn is always let finish
// before this returns, so a blocked fn cannot outlive the test.
func whileDeliveryMuHeld(t *testing.T, svc *Service, fn func()) bool {
	t.Helper()
	svc.deliveryMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		svc.deliveryMu.Unlock()
		return true
	case <-time.After(5 * time.Second):
		svc.deliveryMu.Unlock()
		<-done
		return false
	}
}

// admissionOutcome is storeIncomingMessage's return triple.
type admissionOutcome struct {
	stored  bool
	count   int
	errCode string
}

func admit(svc *Service, msg incomingMessage) admissionOutcome {
	stored, count, errCode := svc.storeIncomingMessage(msg, false)
	return admissionOutcome{stored: stored, count: count, errCode: errCode}
}

// A message that is not a transit DM entering the backlog — a broadcast and
// its bloom duplicate, a DM addressed to this node — cannot touch relayRetry,
// so its admission must not take deliveryMu. Taking it anyway queues a writer
// on the delivery domain for every gossip frame and, through Go's writer
// priority, stalls every deliveryMu reader behind any long gossipMu reader.
func TestStoreIncomingMessage_NonTransitAdmissionLeavesDeliveryMuAlone(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, sender)

	broadcast := incomingMessage{
		ID: "broadcast-admission-1", Topic: "global", Sender: sender.Address, Recipient: "*",
		CreatedAt: time.Now().UTC(), Body: "hello",
	}
	toSelf := incomingMessage{
		ID: "dm-to-self-admission-1", Topic: "dm", Sender: sender.Address, Recipient: svc.identity.Address,
		Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(),
		Body: sealDMBody(t, sender, svc.identity.Address, identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey)),
	}

	var first, duplicate, dm admissionOutcome
	if !whileDeliveryMuHeld(t, svc, func() {
		first = admit(svc, broadcast)
		duplicate = admit(svc, broadcast)
		dm = admit(svc, toSelf)
	}) {
		t.Fatal("a non-transit admission waited for deliveryMu")
	}
	// The outcomes prove each admission reached the commit; a refusal
	// before it would pass the timing check without testing anything.
	if !first.stored || first.errCode != "" {
		t.Fatalf("broadcast admission = %+v, want stored", first)
	}
	if duplicate.stored || duplicate.errCode != "" || duplicate.count != 1 {
		t.Fatalf("broadcast duplicate = %+v, want the dedup outcome (false, 1, \"\")", duplicate)
	}
	if !dm.stored || dm.errCode != "" {
		t.Fatalf("DM to this node = %+v, want stored", dm)
	}
}

// A transit DM's admission takes deliveryMu, but a bloom duplicate of one —
// the most frequent admission on a relay, every mesh echo — is answered
// under gossipMu alone, before the commit.
func TestStoreIncomingMessage_TransitDuplicateLeavesDeliveryMuAlone(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	msg := f.message(t, "transit-duplicate-1")
	f.store(t, msg)

	var duplicate admissionOutcome
	if !whileDeliveryMuHeld(t, f.svc, func() { duplicate = admit(f.svc, msg) }) {
		t.Fatal("a duplicate transit admission waited for deliveryMu")
	}
	if duplicate.stored || duplicate.errCode != "" || duplicate.count != 1 {
		t.Fatalf("transit duplicate = %+v, want the dedup outcome (false, 1, \"\")", duplicate)
	}
}

// An expiry sweep that removes no envelope able to own a `msg|` entry — here
// an auto-delete broadcast — does not take deliveryMu. storeIncomingMessage
// runs the sweep inline for every kind of admission, so taking it would put
// the delivery-domain writer back on the non-transit path.
func TestCleanupExpiredMessages_NonTransitExpiryLeavesDeliveryMuAlone(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.gossipMu.Lock()
	svc.topics["global"] = []protocol.Envelope{expiredBroadcast("expired-broadcast-1")}
	svc.gossipMu.Unlock()

	if !whileDeliveryMuHeld(t, svc, svc.cleanupExpiredMessagesForce) {
		t.Fatal("an expiry sweep with no transit envelope waited for deliveryMu")
	}
	svc.gossipMu.RLock()
	left := len(svc.topics["global"])
	svc.gossipMu.RUnlock()
	if left != 0 {
		t.Fatalf("the expired broadcast is still in its topic (%d left); the sweep did nothing", left)
	}
}

// orphanRelayMessageEntries counts the msg| keys whose envelope is not in
// s.topics["dm"], reading both domains in one canonical-order hold — the
// view any reader of relayRetry gets.
func orphanRelayMessageEntries(svc *Service) int {
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	svc.gossipMu.RLock()
	defer svc.gossipMu.RUnlock()
	present := make(map[string]struct{}, len(svc.topics["dm"]))
	for _, envelope := range svc.topics["dm"] {
		present[relayMessageKey(envelope.ID)] = struct{}{}
	}
	orphans := 0
	for key := range svc.relayRetry {
		if !strings.HasPrefix(key, relayMessageKey("")) {
			continue
		}
		if _, ok := present[key]; !ok {
			orphans++
		}
	}
	return orphans
}

// watchForOrphanRelayMessageEntries samples the invariant from a separate
// goroutine until the returned stop function is called, which reports how
// many samples saw an orphan. This is what catches a removal path that drops
// the envelope and its msg| entry in two sections: no single-goroutine
// assertion after the fact can, because the second section always runs.
func watchForOrphanRelayMessageEntries(svc *Service) (stop func() int) {
	var done atomic.Bool
	var sightings atomic.Int64
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for !done.Load() {
			if orphanRelayMessageEntries(svc) > 0 {
				sightings.Add(1)
			}
			// Without the yield, on one P the watcher would hold every time
			// slice it gets and starve the code it is watching.
			runtime.Gosched()
		}
	}()
	return func() int {
		done.Store(true)
		<-finished
		return int(sightings.Load())
	}
}

// The transit cap evicts inside the admission commit, so at no instant does
// an evicted envelope's msg| entry outlive it.
func TestStoreIncomingMessage_TransitCapEvictionNeverExposesOrphanRelayRetry(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const admissions = 4 * maxTransitPerRecipient
	burst := make([]incomingMessage, 0, admissions)
	for i := 0; i < admissions; i++ {
		burst = append(burst, f.message(t, protocol.MessageID(fmt.Sprintf("cap-stress-%d", i))))
	}

	stop := watchForOrphanRelayMessageEntries(f.svc)
	for _, msg := range burst {
		f.store(t, msg)
	}
	sightings := stop()
	f.svc.WaitBackground()

	if sightings > 0 {
		t.Fatalf("a reader saw msg| entries without their envelope %d times while the transit cap evicted", sightings)
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// The expiry sweep removes an envelope and its msg| entry in one section, so
// at no instant does the entry outlive the envelope. Two readers watch it:
// the orphan watcher reads both domains, and a reader holding deliveryMu
// alone — as the ack_delete gate and the retry bookkeeping do — is what lets
// -race see a sweep that writes relayRetry without deliveryMu (the watcher
// is ordered with the sweep by gossipMu and cannot). Every round also checks
// the envelope did expire, so a sweep that stops removing transit envelopes
// cannot pass by doing nothing.
func TestCleanupExpiredMessages_ExpiryNeverExposesOrphanRelayRetry(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const rounds = 300
	batch := make([]incomingMessage, 0, rounds)
	for i := 0; i < rounds; i++ {
		batch = append(batch, f.message(t, protocol.MessageID(fmt.Sprintf("expiry-stress-%d", i))))
	}

	// The reader touches nothing but deliveryMu and relayRetry — no atomic
	// handoff, no relayDeliveredMu — so the only happens-before edges
	// between it and a sweep are the ones deliveryMu gives. Any key will do:
	// the race detector sees the map as one object.
	var stopReader atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for !stopReader.Load() {
			f.svc.hasRelayRetryEntry(batch[0].ID)
			runtime.Gosched()
		}
	}()

	stop := watchForOrphanRelayMessageEntries(f.svc)
	notExpired := protocol.MessageID("")
	for _, msg := range batch {
		f.store(t, msg)
		backdateDMBacklog(f.svc)
		f.svc.cleanupExpiredMessagesForce()
		if backlogHasMessage(f.svc, msg.ID) {
			notExpired = msg.ID
			break
		}
		// Give the reader a turn between this sweep and the next
		// admission's deliveryMu section, the one stretch in which an
		// unguarded relayRetry write by the sweep is unordered with it.
		runtime.Gosched()
	}
	sightings := stop()
	stopReader.Store(true)
	<-readerDone
	f.svc.WaitBackground()

	if notExpired != "" {
		t.Fatalf("the sweep left %s in the backlog past its in-flight window; the test exercised no removal", notExpired)
	}
	if sightings > 0 {
		t.Fatalf("a reader saw msg| entries without their envelope %d times while the expiry sweep ran", sightings)
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// relayDeliveredTracked reports whether s.relayDeliveredTo holds a set for id.
func relayDeliveredTracked(svc *Service, id protocol.MessageID) bool {
	svc.relayDeliveredMu.Lock()
	defer svc.relayDeliveredMu.Unlock()
	_, ok := svc.relayDeliveredTo[id]
	return ok
}

// A relay hop's ack_delete records "this peer already holds id" only while
// the id is relayed, and the recipient's ack_delete removing the envelope
// forgets the record after its commit. If the gate checked the msg| entry
// and recorded in two steps, a removal and its forget could land between
// them and the record would be left with no envelope and no msg| entry —
// nothing forgets it again and it holds one of the maxRelayDeliveredMessages
// slots for the life of the process.
//
// A stress test: there is no seam between the check and the record. Every
// hop hammers the gate for the same current id while the recipient's
// ack_delete removes that id, then the hops move on to the next one. With
// several Ps, relayDeliveredMu is contended, hops that already passed the
// check sit waiting for it, and the removal's forget, arriving as a running
// goroutine, can take it ahead of them. With one P, the remover only runs
// when the scheduler preempts a hop, and some hops are preempted inside the
// window. Either way each id is one short chance, so the test runs ids for
// a fixed time budget — cheap ids, since the sealed body does not bind the
// message id — and stops at the first leak.
func TestAckDeleteSuppressionRecordNeverOutlivesItsEnvelope(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	template := f.message(t, "ack-race-template")
	hops := ackRaceHops(runtime.GOMAXPROCS(0))
	// The budget is soft: a run that has not checked minChecked ids by
	// then — a loaded machine, other tests sharing the CPU — keeps going
	// until it has, up to the hard cap.
	const minChecked = 10
	budget, hardCap := time.Now().Add(2*time.Second), time.Now().Add(15*time.Second)

	checked := 0
	leaked := 0
	var recorded int64
	// Rounds of fewer ids than the per-recipient transit cap, so none is
	// evicted before its turn.
	for round := 0; leaked == 0 && time.Now().Before(hardCap); round++ {
		batch := make([]incomingMessage, 0, maxTransitPerRecipient-8)
		for i := 0; i < cap(batch); i++ {
			msg := template
			msg.ID = protocol.MessageID(fmt.Sprintf("ack-race-%d-%d", round, i))
			// A bloom false positive answers an id as a duplicate (the
			// rotating filter does give them); such an id is simply not raced.
			if outcome := admit(f.svc, msg); !outcome.stored {
				continue
			}
			batch = append(batch, msg)
		}
		removed, recordedInRound := raceAckDeleteSuppressionAgainstRemoval(t, f, batch, hops, budget, minChecked-checked)
		recorded += recordedInRound
		// Every raced id is checked before the run may stop, the last
		// round's included: a round that ends the run is a round like any
		// other.
		for _, id := range removed {
			if relayDeliveredTracked(f.svc, id) {
				leaked++
			}
		}
		checked += len(removed)
		if checked >= minChecked && time.Now().After(budget) {
			break
		}
	}
	f.svc.WaitBackground()

	t.Logf("checked=%d raced ids, recorded=%d hop acks, hops=%d", checked, recorded, hops)
	// A gate that never records would pass the leak check below for free.
	if recorded == 0 {
		t.Fatal("no hop ack was ever recorded; the race had nothing to leak")
	}
	// A floor against a run that checked almost nothing, not a statement of
	// the test's power. One P is the slow case — the remover runs only when
	// the scheduler preempts a hop — and even there a run checks a dozen ids
	// or more.
	if checked < minChecked {
		t.Fatalf("only %d raced ids were checked within the budget; the test proved little", checked)
	}
	if leaked > 0 {
		t.Fatalf("%d of %d ack_delete suppression sets outlived their envelope and msg| entry", leaked, checked)
	}
}

// ackRaceHops sizes the hop pool for the scheduler the race runs on. With
// one P the remover runs only when the scheduler preempts a hop, and every
// hop costs a time slice per id, so a few hops give the most ids per budget.
// With several Ps what holds a hop inside the window is contention on
// relayDeliveredMu, so the pool is several hops per P — capped, because past
// that the hops starve the remover and fewer ids fit in the budget.
func ackRaceHops(procs int) int {
	if procs == 1 {
		return 4
	}
	return min(10*procs, 40)
}

// raceAckDeleteSuppressionAgainstRemoval has every hop ack the same current
// id of batch while the recipient's ack_delete removes the ids one by one,
// moving the hops on to the next id after each removal, until the batch runs
// out or the deadline passes with at least atLeast ids removed. It returns
// the ids it removed and how many hop acks the gate recorded. It runs on the
// test goroutine, so a removal that goes wrong stops the whole run at once.
func raceAckDeleteSuppressionAgainstRemoval(t *testing.T, f transitDMFixture, batch []incomingMessage, hops int, deadline time.Time, atLeast int) ([]protocol.MessageID, int64) {
	t.Helper()
	var target atomic.Int64
	var recorded atomic.Int64
	end := int64(len(batch))
	var wg sync.WaitGroup
	for h := 0; h < hops; h++ {
		wg.Add(1)
		go func(hop domain.PeerIdentity) {
			defer wg.Done()
			for {
				i := target.Load()
				if i >= end {
					return
				}
				if f.svc.recordRelayDeliveredWhileRelayed(batch[i].ID, hop) {
					recorded.Add(1)
				}
			}
		}(testHop(h + 1))
	}
	defer wg.Wait()
	defer target.Store(end)

	removed := make([]protocol.MessageID, 0, len(batch))
	for i, msg := range batch {
		if len(removed) >= atLeast && time.Now().After(deadline) {
			break
		}
		// Let the hops pile up on the current id before it goes.
		runtime.Gosched()
		if n := f.svc.deleteBacklogMessageForRecipient(f.recipient.Address, msg.ID); n != 1 {
			// The deferred Store and Wait stop the hops before the test ends.
			t.Fatalf("recipient ack_delete removed %d envelopes for %s, want 1", n, msg.ID)
		}
		removed = append(removed, msg.ID)
		target.Store(int64(i + 1))
	}
	return removed, recorded.Load()
}

// testHop is a relay hop identity for ack_delete suppression records.
func testHop(n int) domain.PeerIdentity {
	return domain.PeerIdentityFromWire(fmt.Sprintf("%040x", n))
}

// backdateDMBacklog moves every dm envelope's admission past the transit
// in-flight window, so the next expiry sweep removes the transit ones.
func backdateDMBacklog(svc *Service) {
	svc.gossipMu.Lock()
	defer svc.gossipMu.Unlock()
	for i := range svc.topics["dm"] {
		svc.topics["dm"][i].StoredAt = time.Now().UTC().Add(-transitInFlightWindow - time.Minute)
	}
}

// The suppression set of an envelope the expiry sweep removes is forgotten
// with it: recordRelayDeliveredWhileRelayed's guarantee that no set outlives
// its envelope rests on every removal path forgetting.
func TestCleanupExpiredMessages_TransitExpiryForgetsSuppressionSet(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("expiry-forgets-1")
	f.store(t, f.message(t, id))
	if !f.svc.recordRelayDeliveredWhileRelayed(id, testHop(1)) {
		t.Fatal("fixture: an ack_delete for a relayed id must be recorded")
	}

	backdateDMBacklog(f.svc)
	f.svc.cleanupExpiredMessagesForce()

	if backlogHasMessage(f.svc, id) {
		t.Fatal("fixture: the transit envelope must have expired")
	}
	if relayDeliveredTracked(f.svc, id) {
		t.Fatal("the expiry sweep removed the envelope but left its ack_delete suppression set")
	}
}

// Same guarantee for the transit cap: the evicted envelope's suppression set
// is forgotten by the admission that evicted it.
func TestStoreIncomingMessage_TransitCapEvictionForgetsSuppressionSet(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const victim = protocol.MessageID("eviction-forgets-1")
	f.store(t, f.message(t, victim))
	if !f.svc.recordRelayDeliveredWhileRelayed(victim, testHop(1)) {
		t.Fatal("fixture: an ack_delete for a relayed id must be recorded")
	}

	for i := 0; i < maxTransitPerRecipient; i++ {
		f.store(t, f.message(t, protocol.MessageID(fmt.Sprintf("eviction-forgets-burst-%d", i))))
	}
	f.svc.WaitBackground()

	if backlogHasMessage(f.svc, victim) {
		t.Fatal("fixture: the burst must have evicted the oldest transit envelope")
	}
	if relayDeliveredTracked(f.svc, victim) {
		t.Fatal("the transit cap evicted the envelope but left its ack_delete suppression set")
	}
}

// The recipient's ack_delete races the admission of the same transit DM.
// Whatever the interleaving, the settled state may hold the envelope with
// its entry or neither — and while it runs, a deliveryMu-only reader of
// relayRetry must never race a write to it. This catches what the
// Route-hook window tests cannot: an arm in a section of its own placed
// anywhere after the commit (an orphan whenever the ack lands first), and a
// commit that arms or drops without holding deliveryMu (a data race under
// -race). It needs a second P: with one, nothing runs between the commit's
// release and an arm placed right after it unless the scheduler preempts
// the admission exactly there. TestStoreIncomingMessage_FirstReaderAfterTransitCommitSeesItsArm
// pins the arm's placement deterministically on any scheduler; this test
// stays as the cheap end-to-end race of a real ack_delete against the
// admission it was written for.
func TestStoreIncomingMessage_ConcurrentAckDeleteLeavesNoOrphanRelayRetry(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	template := f.message(t, "ack-vs-admission-template")
	const rounds = 200

	// current is the round's id; the reader probes it under deliveryMu alone.
	var current atomic.Pointer[protocol.MessageID]
	first := protocol.MessageID("ack-vs-admission-0")
	current.Store(&first)
	var stopReader atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for !stopReader.Load() {
			f.svc.hasRelayRetryEntry(*current.Load())
			runtime.Gosched()
		}
	}()
	defer func() {
		stopReader.Store(true)
		<-readerDone
	}()

	acks := 0
	for i := 0; i < rounds; i++ {
		msg := template
		msg.ID = protocol.MessageID(fmt.Sprintf("ack-vs-admission-%d", i))
		current.Store(&msg.ID)
		var acked, giveUp atomic.Bool
		ackerDone := make(chan struct{})
		go func(id protocol.MessageID) {
			defer close(ackerDone)
			for !giveUp.Load() {
				if f.svc.deleteBacklogMessageForRecipient(f.recipient.Address, id) == 1 {
					acked.Store(true)
					return
				}
				runtime.Gosched()
			}
		}(msg.ID)
		outcome := admit(f.svc, msg)
		// The acker removes the envelope once it is there. A round whose
		// envelope never arrives fails here, at once, instead of letting
		// every later round run out a deadline and pass having raced
		// nothing.
		deadline := time.Now().Add(time.Second)
		for outcome.stored && !acked.Load() && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		giveUp.Store(true)
		<-ackerDone
		if outcome.stored && !acked.Load() {
			t.Fatalf("round %d: %s was admitted but the recipient's ack_delete never found it", i, msg.ID)
		}
		if acked.Load() {
			acks++
		}
	}
	f.svc.WaitBackground()

	// A bloom false positive can answer an id as a duplicate; nearly every
	// round must still have been raced.
	if acks < rounds*9/10 {
		t.Fatalf("only %d of %d rounds had an admitted envelope acked away; the race was barely exercised", acks, rounds)
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// The zero topicsCommitMode is no mode at all: a value nobody decided must
// not read as "gossipMu alone". An unknown mode is refused by the admission
// and, wherever it is locked anyway, takes deliveryMu too — the superset is
// never wrong, too little would leave relayRetry unguarded.
func TestTopicsCommitMode_ZeroValueIsUnknown(t *testing.T) {
	t.Parallel()
	var undecided topicsCommitMode
	if undecided.known() {
		t.Fatal("the zero topicsCommitMode must not be a known mode")
	}
	if !undecided.holdsDeliveryMu() {
		t.Fatal("an unknown topicsCommitMode must lock deliveryMu as well")
	}
	if !topicsCommitGossipOnly.known() || topicsCommitGossipOnly.holdsDeliveryMu() {
		t.Fatal("topicsCommitGossipOnly must be known and hold gossipMu alone")
	}
	if !topicsCommitWithRelayRetry.known() || !topicsCommitWithRelayRetry.holdsDeliveryMu() {
		t.Fatal("topicsCommitWithRelayRetry must be known and hold deliveryMu")
	}
}

// A commit that holds gossipMu alone never removes an envelope that can own a
// `msg|` entry: it could not drop the entry, so the entry would outlive its
// envelope. The expiry sweep decides its mode from the envelopes phase 1 saw,
// but phase 2 removes by id, and the outer id is not authenticated — the
// envelope under an id in phase 2 need not be the one phase 1 classified.
// Here phase 1 classified id as a local auto-delete DM (gossipMu alone), and
// by phase 2 a peer's transit DM sits under the same id, armed.
func TestCleanupExpiredMessages_GossipOnlyRemovalKeepsRelayRetryEnvelope(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("reused-outer-id-1")
	f.store(t, f.message(t, id))
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("fixture: the transit DM must be armed, or the assertion below proves nothing")
	}

	expiredIDs := map[string]map[string]struct{}{"dm": {string(id): {}}}
	f.svc.lockTopicsCommit(topicsCommitGossipOnly)
	removed := f.svc.removeExpiredEnvelopesLocked(expiredIDs, topicsCommitGossipOnly)
	f.svc.unlockTopicsCommit(topicsCommitGossipOnly)

	if len(removed) != 0 {
		t.Fatalf("a gossipMu-only removal removed %v, want nothing", removed)
	}
	if !backlogHasMessage(f.svc, id) {
		t.Fatal("a gossipMu-only removal took a transit envelope it cannot drop the relay retry of")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)

	// The relay-retry mode may remove it, entry and all.
	f.svc.lockTopicsCommit(topicsCommitWithRelayRetry)
	removed = f.svc.removeExpiredEnvelopesLocked(expiredIDs, topicsCommitWithRelayRetry)
	f.svc.unlockTopicsCommit(topicsCommitWithRelayRetry)
	if len(removed) != 1 || backlogHasMessage(f.svc, id) || f.svc.hasRelayRetryEntry(id) {
		t.Fatalf("the relay-retry removal: removed=%v, envelope left=%v, entry left=%v; want the envelope and its entry gone",
			removed, backlogHasMessage(f.svc, id), f.svc.hasRelayRetryEntry(id))
	}
}

// The admission commit checks the bloom again after the gossipMu-only
// pre-check. A transit DM that passed the pre-check while its id was not yet
// seen, then waited for deliveryMu while another admission of the same id
// committed and the recipient's ack_delete removed it, must be answered as a
// duplicate — not re-backlogged, re-armed and re-gossiped to a recipient that
// already has it.
//
// The test holds deliveryMu for reading, not writing, because that makes the
// wait observable: once a writer is queued on a sync.RWMutex, TryRLock fails
// even though only readers hold it. This test service runs no loops, so the
// only deliveryMu writer is the admission, and a failing TryRLock proves it
// got past the pre-check and is waiting for the commit.
func TestStoreIncomingMessage_TransitCommitRechecksTheBloom(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	msg := f.message(t, "transit-recheck-1")

	f.svc.deliveryMu.RLock()
	result := make(chan admissionOutcome, 1)
	go func() { result <- admit(f.svc, msg) }()
	queued := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if !f.svc.deliveryMu.TryRLock() {
			queued = true
			break
		}
		f.svc.deliveryMu.RUnlock()
		runtime.Gosched()
	}
	// Meanwhile the same id was admitted and acked away: seen, not backlogged.
	f.svc.seen.Add(string(msg.ID))
	f.svc.deliveryMu.RUnlock()
	outcome := <-result
	f.svc.WaitBackground()

	if !queued {
		t.Fatalf("the admission never queued on deliveryMu (outcome %+v); the test did not reach the window it is about", outcome)
	}
	if outcome.stored || outcome.errCode != "" {
		t.Fatalf("a duplicate seen while it waited for the commit = %+v, want the dedup outcome", outcome)
	}
	if backlogHasMessage(f.svc, msg.ID) {
		t.Fatal("the duplicate re-entered the dm backlog")
	}
	if f.svc.hasRelayRetryEntry(msg.ID) {
		t.Fatal("the duplicate re-armed a relay retry")
	}
}

// expireDMEnvelope moves one dm envelope's admission past the transit
// in-flight window, leaving the rest of the backlog alone.
func expireDMEnvelope(svc *Service, id protocol.MessageID) {
	svc.gossipMu.Lock()
	defer svc.gossipMu.Unlock()
	for i := range svc.topics["dm"] {
		if svc.topics["dm"][i].ID == id {
			svc.topics["dm"][i].StoredAt = time.Now().UTC().Add(-transitInFlightWindow - time.Minute)
		}
	}
}

// expiredBroadcast is an auto-delete broadcast already past its TTL.
func expiredBroadcast(id protocol.MessageID) protocol.Envelope {
	return protocol.Envelope{
		ID: id, Topic: "global", Sender: "alice", Recipient: "*",
		Flag: protocol.MessageFlagAutoDeleteTTL, TTLSeconds: 1,
		CreatedAt: time.Now().UTC().Add(-time.Hour), StoredAt: time.Now().UTC().Add(-time.Hour),
	}
}

// The relay retry entry and the ack_delete suppression set belong to the
// transit DM, not to its id: outer ids are unsigned, so after a bloom
// rotation a broadcast can carry the id of a live transit DM. A sweep that
// expires that broadcast must leave the live DM's msg| entry and suppression
// set alone — in either mode: here the sweep also expires another transit
// DM, so it holds deliveryMu and drops entries.
func TestCleanupExpiredMessages_ExpiredBroadcastLeavesLiveTransitWithTheSameID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		expireAnother bool
	}{
		{"gossipMu-only sweep", false},
		{"relay-retry sweep", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTransitDMFixture(t)
			const shared = protocol.MessageID("cross-topic-id-1")
			f.store(t, f.message(t, shared))
			if !f.svc.recordRelayDeliveredWhileRelayed(shared, testHop(1)) {
				t.Fatal("fixture: an ack_delete for a relayed id must be recorded")
			}
			if tc.expireAnother {
				const other = protocol.MessageID("cross-topic-other-1")
				f.store(t, f.message(t, other))
				expireDMEnvelope(f.svc, other)
			}
			f.svc.gossipMu.Lock()
			f.svc.topics["global"] = append(f.svc.topics["global"], expiredBroadcast(shared))
			f.svc.gossipMu.Unlock()

			f.svc.cleanupExpiredMessagesForce()

			f.svc.gossipMu.RLock()
			broadcastsLeft := len(f.svc.topics["global"])
			f.svc.gossipMu.RUnlock()
			if broadcastsLeft != 0 {
				t.Fatal("fixture: the expired broadcast must be gone, or the assertions below prove nothing")
			}
			if !backlogHasMessage(f.svc, shared) {
				t.Fatal("the live transit DM left the backlog with a broadcast that shares its id")
			}
			if !f.svc.hasRelayRetryEntry(shared) {
				t.Fatal("the sweep dropped the live transit DM's msg| entry for a broadcast that shares its id")
			}
			if !relayDeliveredTracked(f.svc, shared) {
				t.Fatal("the sweep forgot the live transit DM's suppression set for a broadcast that shares its id")
			}
			assertNoOrphanRelayMessageEntries(t, f.svc)
		})
	}
}

// A transit envelope a gossipMu-only removal kept is kept only until the
// next sweep that finds it expired: that sweep takes the relay-retry mode
// and removes it with its msg| entry, and forgets its ack_delete
// suppression set. Keeping is a deferral, never a leak.
func TestCleanupExpiredMessages_KeptTransitEnvelopeLeavesWithTheNextSweep(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	const id = protocol.MessageID("kept-then-swept-1")
	f.store(t, f.message(t, id))
	if !f.svc.recordRelayDeliveredWhileRelayed(id, testHop(1)) {
		t.Fatal("fixture: an ack_delete for a relayed id must be recorded")
	}

	// A gossipMu-only removal phase that was handed this id keeps it.
	f.svc.lockTopicsCommit(topicsCommitGossipOnly)
	kept := f.svc.removeExpiredEnvelopesLocked(map[string]map[string]struct{}{"dm": {string(id): {}}}, topicsCommitGossipOnly)
	f.svc.unlockTopicsCommit(topicsCommitGossipOnly)
	if len(kept) != 0 || !backlogHasMessage(f.svc, id) {
		t.Fatalf("fixture: the gossipMu-only removal must keep the transit envelope (it reported %v)", kept)
	}

	expireDMEnvelope(f.svc, id)
	f.svc.cleanupExpiredMessagesForce()

	if backlogHasMessage(f.svc, id) {
		t.Fatal("the next sweep left the kept transit envelope in the backlog past its in-flight window")
	}
	if f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the next sweep removed the kept envelope but left its msg| entry")
	}
	if relayDeliveredTracked(f.svc, id) {
		t.Fatal("the next sweep removed the kept envelope but left its ack_delete suppression set")
	}
	assertNoOrphanRelayMessageEntries(t, f.svc)
}

// Withdrawing or failing one of this node's own DMs removes no relay retry
// entry: an envelope this node authored never owns one, and the entry under
// its id — outer ids are unsigned — belongs to whatever transit DM carries
// that id. Each path below acts on an own message X→R1 that is no longer in
// the backlog while a peer's transit DM X→R2 is live and armed; afterwards
// the transit DM must still own its msg| entry.
func TestOwnDeliveryWithdrawalLeavesTransitRelayRetryAlone(t *testing.T) {
	t.Parallel()
	const shared = protocol.MessageID("own-and-transit-id-1")
	ownRecipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	for _, tc := range []struct {
		name     string
		withdraw func(t *testing.T, svc *Service)
	}{
		{"CancelOutgoingDelivery", func(t *testing.T, svc *Service) {
			if _, err := svc.CancelOutgoingDelivery(shared, domain.PeerIdentityFromWire(ownRecipient.Address)); err != nil {
				t.Fatalf("CancelOutgoingDelivery: %v", err)
			}
		}},
		{"CancelOutgoingDeliveriesTo", func(t *testing.T, svc *Service) {
			// A terminal outbound status entry is what still names the own
			// message for this recipient.
			svc.deliveryMu.Lock()
			svc.outbound[string(shared)] = outboundDelivery{MessageID: string(shared), Recipient: ownRecipient.Address, Status: "failed"}
			svc.deliveryMu.Unlock()
			if _, err := svc.CancelOutgoingDeliveriesTo(domain.PeerIdentityFromWire(ownRecipient.Address), nil); err != nil {
				t.Fatalf("CancelOutgoingDeliveriesTo: %v", err)
			}
		}},
		{"failDelivery", func(t *testing.T, svc *Service) {
			svc.failDelivery(protocol.Envelope{ID: shared, Topic: "dm", Sender: svc.identity.Address, Recipient: ownRecipient.Address}, "expired", "ttl")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTransitDMFixture(t)
			f.store(t, f.message(t, shared))
			if !f.svc.hasRelayRetryEntry(shared) {
				t.Fatal("fixture: the transit DM must be armed, or the assertion below proves nothing")
			}

			tc.withdraw(t, f.svc)
			f.svc.WaitBackground()

			if !backlogHasMessage(f.svc, shared) {
				t.Fatal("fixture: the transit DM must still be backlogged")
			}
			if !f.svc.hasRelayRetryEntry(shared) {
				t.Fatal("withdrawing an own DM deleted the msg| entry of a live transit DM that shares its id")
			}
		})
	}
}

// goroutineParkedIn reports whether some goroutine is parked on a
// sync.RWMutex read lock with fn on its stack. There is no API that reports
// a mutex's waiters, so the goroutine dump is the only witness; the check
// requires both the runtime's wait reason in the goroutine header — parked,
// not merely about to call RLock — and fn, a function only the waiting
// goroutine runs.
func goroutineParkedIn(fn string) bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	for n == len(buf) {
		buf = make([]byte, 2*len(buf))
		n = runtime.Stack(buf, true)
	}
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		header, _, _ := strings.Cut(g, "\n")
		if strings.Contains(header, "[sync.RWMutex.RLock") && strings.Contains(g, fn) {
			return true
		}
	}
	return false
}

// relayRetrySighting is what one deliveryMu reader saw of a message.
type relayRetrySighting struct {
	envelope bool
	entry    bool
}

// readRelayRetryAfterCommit is the reader queued behind a transit
// admission's commit. Its name is what goroutineParkedIn looks for.
func readRelayRetryAfterCommit(svc *Service, id protocol.MessageID, out chan<- relayRetrySighting) {
	svc.deliveryMu.RLock()
	_, entry := svc.relayRetry[relayMessageKey(id)]
	envelope := backlogHasMessage(svc, id)
	svc.deliveryMu.RUnlock()
	out <- relayRetrySighting{envelope: envelope, entry: entry}
}

// The first deliveryMu reader after a transit admission's commit already
// sees the msg| entry of the envelope that commit backlogged. A sync.RWMutex
// writer's Unlock hands the lock to the readers queued behind it before any
// later writer can take it, so a reader queued behind the commit runs before
// any deliveryMu section the admission could take afterwards: an arm moved
// out of the commit — even to directly after it — is seen as missing, on any
// scheduler, one P included.
//
// The test holds deliveryMu for reading so the commit has to queue, sees it
// queued through TryRLock (which fails once a writer is waiting), then
// starts the reader and waits until the reader is parked behind the commit
// before letting both go.
func TestStoreIncomingMessage_FirstReaderAfterTransitCommitSeesItsArm(t *testing.T) {
	t.Parallel()
	f := newTransitDMFixture(t)
	// Run the throttled sweep now, so the admission's own inline sweep is a
	// no-op and the only deliveryMu writer is the commit.
	f.svc.cleanupExpiredMessagesForce()
	msg := f.message(t, "first-reader-after-commit-1")

	f.svc.deliveryMu.RLock()
	result := make(chan admissionOutcome, 1)
	go func() { result <- admit(f.svc, msg) }()
	commitQueued := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); runtime.Gosched() {
		if !f.svc.deliveryMu.TryRLock() {
			commitQueued = true
			break
		}
		f.svc.deliveryMu.RUnlock()
	}
	if !commitQueued {
		f.svc.deliveryMu.RUnlock()
		t.Fatalf("the admission never queued its commit on deliveryMu (outcome %+v)", <-result)
	}

	sighting := make(chan relayRetrySighting, 1)
	go readRelayRetryAfterCommit(f.svc, msg.ID, sighting)
	readerQueued := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); runtime.Gosched() {
		if goroutineParkedIn("readRelayRetryAfterCommit") {
			readerQueued = true
			break
		}
	}
	f.svc.deliveryMu.RUnlock()
	seen := <-sighting
	outcome := <-result
	f.svc.WaitBackground()

	if !readerQueued {
		t.Fatal("the reader never parked behind the commit; the test did not reach the window it is about")
	}
	if !outcome.stored {
		t.Fatalf("fixture: the transit DM must be admitted, got %+v", outcome)
	}
	if !seen.envelope {
		t.Fatal("fixture: the first reader after the commit must see the envelope")
	}
	if !seen.entry {
		t.Fatal("the first deliveryMu reader after the commit saw the envelope without its msg| entry: the arm is not in the commit")
	}
}
