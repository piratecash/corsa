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
	"github.com/piratecash/corsa/internal/core/routing"
)

// This file covers ONE rule, and it is a fact about the RECIPIENT that this
// node measured itself: a message whose receipt is overdue is re-sent as
// soon as a recipient an earlier pass could not reach can be reached again
// (Service.measureRecipientReachability, Service.recipientPassState,
// applyReachabilityReturnLocked).
//
// Everything finer than that was tried and removed: a hold reason, an
// UpdateRoute status, snapshots compared around an event, a per-peer change
// counter bumped from eight call sites, the connection a sink took the
// frame on, the queue that died holding it. Each needed a fact owned by
// another component — routing, the peer domain, the transport — copied into
// the delivery entry by whichever sink got there first, and every copy went
// stale in its own way: a decision that names a next hop before the entry
// can record it, a replay that sends outside the engine, a batch timestamp,
// a lower queue that discards silently.
//
// What the person reported is exactly what the rule now says: their peer was
// away, came back, and a message still sat. A DIRECT peer whose connection
// changes without any pass ever measuring them away — a reconnect inside one
// tick, or a new session opening before the old one is torn down — is
// bounded instead of recognised: wakeOverdueForReturningPeer clamps the wait
// to the first schedule step, on every session event, which is why nothing
// has to count sessions — and the accelerated attempt is spent once given,
// so a flapping session cannot ride emit-rebuild-overdue-clamp for ever. A MULTI-HOP next hop flapping in that window is
// deliberately not covered at all — the recipient measures reachable at both
// ends of it, and through transit a faster re-send is absorbed by the relays'
// own dedup.
//
// The tests drive REAL reachability — subscribers, sessions, routes — and
// never poke the mechanism.

// detachPushObserver takes the recipient's push subscription away, which is
// what going offline looks like to the router, and returns it so the test
// can give it back. The connection stays registered: what changes is whether
// anything is subscribed for this recipient.
func detachPushObserver(t *testing.T, svc *Service, recipient string) *subscriber {
	t.Helper()
	svc.gossipMu.Lock()
	defer svc.gossipMu.Unlock()
	sub, ok := svc.subs[recipient]["observer"]
	if !ok {
		t.Fatalf("no observer subscribed for %s", recipient)
	}
	delete(svc.subs[recipient], "observer")
	return sub
}

// reattachPushObserver puts back what detachPushObserver took.
func reattachPushObserver(t *testing.T, svc *Service, recipient string, sub *subscriber) {
	t.Helper()
	svc.gossipMu.Lock()
	defer svc.gossipMu.Unlock()
	if svc.subs[recipient] == nil {
		svc.subs[recipient] = make(map[string]*subscriber)
	}
	svc.subs[recipient]["observer"] = sub
}

// stalledOnWireEntry builds the state a message reaches when every sink
// accepted its frame and the recipient never answered: confirmed on the
// wire, climbed to the tail of the backoff schedule, no receipt.
//
// It is driven through confirmEnvelopeInMemory rather than assembled by
// hand, so the entry the tests reason about is the one the delivery path
// actually produces.
//
// sinceLastEmission places the LAST confirmation that far before the wall
// clock, because staleness is judged against time.Now: a schedule built in
// the distant past would be stale by construction and the threshold tests
// would prove nothing.
func stalledOnWireEntry(t *testing.T, svc *Service, envelope protocol.Envelope, sinceLastEmission time.Duration) time.Time {
	t.Helper()
	span := time.Duration(0)
	for i := range deliveryRetrySchedule {
		span += deliveryRetryBackoff(i) + time.Second
	}
	start := time.Now().UTC().Add(-sinceLastEmission - span)

	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, start, false)
	svc.deliveryMu.Unlock()

	dispatched := start
	for i := range deliveryRetrySchedule {
		dispatched = dispatched.Add(deliveryRetryBackoff(i) + time.Second)
		if !svc.confirmEnvelopeInMemory(envelope, dispatched) {
			t.Fatalf("sink confirmation %d was not recorded", i)
		}
	}

	svc.deliveryMu.Lock()
	entry, ok := svc.awaitingDelivered[envelope.ID]
	if !ok {
		svc.deliveryMu.Unlock()
		t.Fatal("the entry must still be awaiting its receipt")
	}
	hold, attempts := entry.Hold, entry.Attempts
	svc.deliveryMu.Unlock()
	if hold != holdNone {
		t.Fatalf("a confirmed frame must leave the entry unheld; hold = %v", hold)
	}
	if attempts != len(deliveryRetrySchedule) {
		t.Fatalf("backoff index = %d, want %d", attempts, len(deliveryRetrySchedule))
	}
	return dispatched
}

// scheduleOf reads an entry's next attempt time. The schedule is what a
// wake-up actually writes, so asserting on it is what tells a test that did
// nothing from a test that proved something.
func scheduleOf(t *testing.T, svc *Service, id protocol.MessageID) time.Time {
	t.Helper()
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	entry, ok := svc.awaitingDelivered[id]
	if !ok {
		t.Fatalf("%s is no longer awaiting delivery", id)
	}
	return entry.NextAttemptAt
}

// attachRoutingCapablePeer registers a connected peer advertising both mesh
// capabilities, which is what a TRANSIT next hop must have before the router
// will pick it (resolveRoutableAddress). attachCapableRelayPeer is the
// destination-peer form and carries relay only.
func attachRoutingCapablePeer(t *testing.T, svc *Service, addr string, peerIdentity domain.PeerIdentity) chan peerSendItem {
	t.Helper()
	sendCh := make(chan peerSendItem, 32)
	setRoutingPeerConnected(t, svc, addr, peerIdentity, true, sendCh)
	return sendCh
}

// setRoutingPeerConnected drops the session with a next hop and brings it
// back, leaving its ROUTES in the table untouched. That is the withdrawal
// grace window: nothing is withdrawn on the way down and nothing is
// re-accepted on the way up, so no route event names the recipient behind
// it — the case every event-derived design missed.
func setRoutingPeerConnected(t *testing.T, svc *Service, addr string, peerIdentity domain.PeerIdentity, connected bool, sendCh chan peerSendItem) domain.ConnID {
	t.Helper()
	address := domain.PeerAddress(addr)
	svc.peerMu.Lock()
	defer svc.peerMu.Unlock()
	if !connected {
		if old := svc.sessions[address]; old != nil {
			delete(svc.conns, old.connID)
		}
		delete(svc.sessions, address)
		delete(svc.health, address)
		delete(svc.peerIDs, address)
		return 0
	}
	// A RECONNECT is a new connection id, never the old one back — which
	// is the whole reason the engine records connections and not peers.
	svc.connIDCounter++
	connID := domain.ConnID(svc.connIDCounter)
	svc.sessions[address] = &peerSession{
		address:      address,
		peerIdentity: peerIdentity,
		connID:       connID,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health[address] = &peerHealth{Connected: true}
	svc.peerIDs[address] = peerIdentity
	svc.conns[connID] = &connEntry{}
	return connID
}

// TestReturningPeerWakesMessageStuckWithoutReceipt is the regression for
// "the peer came back online and two messages still say not delivered".
//
// A frame a sink accepted is not a message the recipient has: the sink is
// our own writer or the next relay hop, and everything past it can still
// lose the envelope. So an entry waiting for a receipt is evidence about US,
// not about them — and once the path comes back it must be re-sent at once,
// exactly like one that never left. The original rule woke only entries that
// had never reached the wire, which left a message that DID reach it sitting
// out the eleven-minute tail while newer messages went out in seconds.
func TestReturningPeerWakesMessageStuckWithoutReceipt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Older than any transit or dedup window — the shape of the reported bug.
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "stale-wire-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7501)))

	// They go away. The entry is mid-backoff, so this tick sends nothing —
	// it only observes that there is no path.
	sub := detachPushObserver(t, svc, recipientID.Address)
	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	stream.expectQuiet(t, 200*time.Millisecond)

	// They come back.
	reattachPushObserver(t, svc, recipientID.Address, sub)
	svc.retryDueDeliveries(now.Add(time.Second))

	stream.expect(t, "stale-wire-1")
}

// TestPathThatNeverWentAwayLeavesTheBackoffAlone is the other half: a
// receipt that has not arrived while the path was up the whole time is a
// loss on the way, not a returning peer, and it is the backoff's job.
// Re-sending on every tick would be the storm the queue discipline exists to
// prevent.
func TestPathThatNeverWentAwayLeavesTheBackoffAlone(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "stale-wire-2", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7502)))
	parked := scheduleOf(t, svc, envelope.ID)

	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	svc.retryDueDeliveries(now.Add(time.Second))

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("an unbroken path pulled the entry forward: %v → %v", parked, got)
	}
	stream.expectQuiet(t, 200*time.Millisecond)
}

// TestRecentlyEmittedMessageIsDeferredNotDropped pins the threshold, and
// what happens at it.
//
// A receipt from a recipient who is there comes back in well under a
// second, so a message emitted moments ago is in flight, not stuck: the
// return must not put a second copy on the wire now. But "not now" is not
// "never" — the transition is not stored anywhere, so an entry that is
// merely SKIPPED goes back to waiting out its backoff, which for the tail
// means eleven minutes after the person came back. It is scheduled at the
// end of the queue window instead.
func TestRecentlyEmittedMessageIsDeferredNotDropped(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "stale-wire-3", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, time.Second)

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7503)))
	parked := scheduleOf(t, svc, envelope.ID)

	now := time.Now().UTC()
	sub := detachPushObserver(t, svc, recipientID.Address)
	svc.retryDueDeliveries(now)
	reattachPushObserver(t, svc, recipientID.Address, sub)
	svc.retryDueDeliveries(now.Add(time.Second))

	svc.deliveryMu.RLock()
	emitted := svc.awaitingDelivered[envelope.ID].LastEmittedAt
	svc.deliveryMu.RUnlock()
	got := scheduleOf(t, svc, envelope.ID)
	if want := emitted.Add(deliveryQueueWindow); !got.Equal(want) {
		t.Errorf("a recent message must be deferred to the end of the queue window: got %v, want %v (was %v)", got, want, parked)
	}
	if !got.Before(parked) {
		t.Errorf("the return must still shorten the wait: %v → %v", parked, got)
	}
	// And nothing goes out while a receipt could still be in flight.
	stream.expectQuiet(t, 200*time.Millisecond)
}

// TestReceiptOverdueThresholdIsTheQueueWindow states what the wake-up
// measures. The only question it asks is "could a receipt still be in
// flight", and the queue window is already this file's answer to that: past
// it the queue has given the slot away.
func TestReceiptOverdueThresholdIsTheQueueWindow(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	justEmitted := &deliveryRetryEntry{LastEmittedAt: now.Add(-deliveryQueueWindow / 2)}
	if receiptOverdue(justEmitted, now) {
		t.Error("a message emitted inside the queue window may still be in flight")
	}
	stale := &deliveryRetryEntry{LastEmittedAt: now.Add(-deliveryQueueWindow - time.Second)}
	if !receiptOverdue(stale, now) {
		t.Error("past the queue window the emission is no longer evidence of anything")
	}
	if !receiptOverdue(&deliveryRetryEntry{}, now) {
		t.Error("an entry no sink ever confirmed is overdue by definition")
	}
}

// TestMessageNoSinkEverTookIsWokenByAReturn covers holdUnconfirmed: nobody
// took that frame, so it is parked on a poll rather than a backoff, and it
// is the state a message written while the peer was away ends up in.
// Leaving it out made the promise false in the case a person notices — a
// fresh message goes out at once while the one written a minute earlier
// waits.
func TestMessageNoSinkEverTookIsWokenByAReturn(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "unconfirmed-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now.Add(-2 * time.Minute), StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now.Add(-2*time.Minute), false)
	entry := svc.awaitingDelivered[envelope.ID]
	// What armDueDeliveries writes for a dispatch no sink confirmed.
	entry.Hold = holdUnconfirmed
	entry.NextAttemptAt = now.Add(deliveryHoldPollInterval)
	svc.deliveryMu.Unlock()

	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7504)))

	sub := detachPushObserver(t, svc, recipientID.Address)
	svc.retryDueDeliveries(now)
	reattachPushObserver(t, svc, recipientID.Address, sub)
	svc.retryDueDeliveries(now.Add(time.Second))

	stream.expect(t, "unconfirmed-1")
}

// TestTheSameReturnDoesNotFireTwice keeps the rule from firing for ever. A
// return is a TRANSITION — unreachable on one pass, reachable on the next
// — so once the pass that saw it has recorded the new reading, a recipient
// who simply stays reachable produces nothing further.
func TestTheSameReturnDoesNotFireTwice(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7505))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "records-loss-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	now := time.Now().UTC()
	sub := detachPushObserver(t, svc, recipientID.Address)
	svc.retryDueDeliveries(now)
	reattachPushObserver(t, svc, recipientID.Address, sub)
	svc.retryDueDeliveries(now.Add(time.Second))

	// The dispatch the return produced is handed to background sinks, and
	// their confirmation legitimately rewrites the schedule. Wait for it to
	// land BEFORE reading the baseline, or the test races the sink it just
	// asked for and reads its answer as a second wake-up.
	svc.WaitBackground()

	// The recipient has not gone away since: a further tick must not re-arm it.
	parked := scheduleOf(t, svc, envelope.ID)
	svc.retryDueDeliveries(time.Now().UTC().Add(2 * time.Second))
	svc.WaitBackground()
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("the same return woke the entry twice: %v → %v", parked, got)
	}
}

// TestMultiHopNextHopFlapWakesTheMessage is the reported symptom end to end,
// through the real session lifecycle rather than the mechanism.
//
// The recipient is several hops away, so there is no session with them to
// observe: their next hop is what comes and goes, and its routes are never
// withdrawn (the grace window), so no route event names the recipient at
// all. Measuring reachability sees it anyway, because the router resolves
// the same next hop the send would use.
func TestMultiHopNextHopFlapWakesTheMessage(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient := domain.PeerIdentityFromWire(recipientID.Address)
	uplink, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	uplinkID := domain.PeerIdentityFromWire(uplink.Address)

	sendCh := attachRoutingCapablePeer(t, svc, "uplink:64646", uplinkID)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: recipient, Origin: recipient, NextHop: uplinkID,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "multi-hop-flap-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	parked := scheduleOf(t, svc, envelope.ID)

	// The next hop drops; the route through it stays in the table.
	_ = setRoutingPeerConnected(t, svc, "uplink:64646", uplinkID, false, sendCh)
	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Fatalf("losing the next hop must not by itself re-arm anything: %v → %v", parked, got)
	}

	// And returns.
	_ = setRoutingPeerConnected(t, svc, "uplink:64646", uplinkID, true, sendCh)
	svc.retryDueDeliveries(now.Add(time.Second))

	if got := scheduleOf(t, svc, envelope.ID); got.After(parked) {
		t.Fatalf("the next hop returned and the message was not woken: still %v", got)
	}
	select {
	case item := <-sendCh:
		id := item.ID
		if id == "" && item.Item != nil {
			id = item.Item.ID
		}
		if id != "multi-hop-flap-1" {
			t.Fatalf("unexpected frame on the wire: %s", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the woken message never reached the uplink")
	}
}

// TestOneConversationReturningLeavesTheOthersAlone is the scoping rule, and
// it needs no mechanism of its own: reachability is measured per recipient,
// so a peer coming back says nothing about anybody else. The counter design
// this replaced could not manage that — a global count re-armed every
// overdue message on the node, and even a per-peer count still fanned out
// across everyone a flapping BACKUP uplink happened to carry.
func TestOneConversationReturningLeavesTheOthersAlone(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	returning, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	other, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	for id, recipient := range map[protocol.MessageID]string{
		"scoped-returning": returning.Address,
		"scoped-other":     other.Address,
	} {
		stalledOnWireEntry(t, svc, protocol.Envelope{
			ID: id, Topic: "dm",
			Sender: svc.Address(), Recipient: recipient,
			Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
		}, 5*time.Minute)
	}

	returningStream := newPushMessageStream(t, attachPushObserver(t, svc, returning.Address, netcore.ConnID(7507)))
	otherStream := newPushMessageStream(t, attachPushObserver(t, svc, other.Address, netcore.ConnID(7508)))
	otherParked := scheduleOf(t, svc, "scoped-other")

	// Only one of them goes away and comes back. The other was reachable
	// throughout and must not be touched.
	sub := detachPushObserver(t, svc, returning.Address)
	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	reattachPushObserver(t, svc, returning.Address, sub)
	svc.retryDueDeliveries(now.Add(time.Second))

	returningStream.expect(t, "scoped-returning")
	otherStream.expectQuiet(t, 200*time.Millisecond)
	if got := scheduleOf(t, svc, "scoped-other"); !got.Equal(otherParked) {
		t.Errorf("an unrelated conversation was re-armed: %v → %v", otherParked, got)
	}
}

// TestUnreachableRecipientKeepsThePollInterval is the cost guard.
//
// A return wakes an entry, the send finds nobody to send to, and the entry
// goes back on the sixty-second hold poll. What may wake it again is the
// next RETURN, not the next tick — otherwise the poll would collapse to two
// seconds and put a routing lookup, and its rate-limited route queries,
// behind every offline recipient forever.
func TestUnreachableRecipientKeepsThePollInterval(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Nobody to send to: no session, no route, no subscriber.
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "offline-poll-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// Due now, so the tick actually tries it and finds nothing.
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now
	svc.deliveryMu.Unlock()
	svc.retryDueDeliveries(now)

	parked := scheduleOf(t, svc, envelope.ID)
	if parked.Before(now.Add(deliveryHoldPollInterval - time.Second)) {
		t.Fatalf("an unreachable recipient must be parked on the hold poll, got %v", parked)
	}

	// Still nobody. The next tick must leave the poll alone.
	svc.retryDueDeliveries(now.Add(2 * time.Second))
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Fatalf("the hold was re-armed without a return: %v → %v", parked, got)
	}
}

// TestFreshMessageIsNotWokenBeforeItsFirstBackoff covers the state a new
// entry is born in: nothing has been measured for it, so it has lost no path
// and there is nothing to return from. A rule that read "no record yet" as a
// change would re-send every message on the first tick after it was written.
func TestFreshMessageIsNotWokenBeforeItsFirstBackoff(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7509)))

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "fresh-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	svc.deliveryMu.Unlock()
	parked := scheduleOf(t, svc, envelope.ID)

	svc.retryDueDeliveries(now)
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("a message written a moment ago was re-armed: %v → %v", parked, got)
	}
	stream.expectQuiet(t, 200*time.Millisecond)
}

// TestGateOffLeavesTheScheduleUntouched keeps the kill switch honest. With
// HoldDMUntilReachable off nothing is measured, every recipient answers
// reachable, no loss is ever recorded and the schedule is the legacy
// unconditional one — the same contract resetBackoffOnReturn and
// kickDeliveryRetriesForReachable keep.
func TestGateOffLeavesTheScheduleUntouched(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = false

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "gate-off-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	parked := scheduleOf(t, svc, envelope.ID)

	// No subscriber and no route: with the gate ON this would be a loss.
	now := time.Now().UTC()
	svc.retryDueDeliveries(now)
	svc.retryDueDeliveries(now.Add(time.Second))

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("the wake-up ran with the reachability gate off: %v → %v", parked, got)
	}
	svc.deliveryMu.RLock()
	measured := len(svc.recipientPassState)
	svc.deliveryMu.RUnlock()
	if measured != 0 {
		t.Error("with the gate off nothing is measured, so no transition can ever be seen")
	}
}

// TestDeliveryRegisteredMidTickIsNotReArmedByIt covers the gap between the
// measurement snapshot and the plan that uses it. A message registered in
// that window was never measured, and "not asked" is not an answer: acting
// on it would re-send a brand-new message to a perfectly reachable
// recipient with nobody having gone anywhere.
func TestDeliveryRegisteredMidTickIsNotReArmedByIt(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7510))

	// The snapshot is taken while the awaiting set is empty, which is
	// exactly what a tick sees when the message is written a moment later.
	reachable := svc.measureRecipientReachability()

	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "mid-tick-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	entry := svc.awaitingDelivered[envelope.ID]
	parked := entry.NextAttemptAt
	svc.applyReachabilityReturnLocked(entry, reachable, now)
	rearmed := !entry.NextAttemptAt.Equal(parked)
	svc.deliveryMu.Unlock()

	if rearmed {
		t.Error("a delivery this pass never measured must not be re-armed by it")
	}
}

// TestSessionArrivingBoundsTheWaitToTheFirstStep is the sub-tick case, in
// every shape it comes in.
//
// A client that drops and returns inside one two-second tick — or that
// opens a new session before this node has torn the old one down, so no
// session count ever reaches zero — leaves the readings saying "reachable"
// at both ends. Nothing this node measured changed, so the tick's rule
// cannot fire; what did change is that their connection is not the one it
// was, and the message still has no receipt. The wait is bounded to the
// schedule's first step rather than the eleven-minute tail.
func TestSessionArrivingBoundsTheWaitToTheFirstStep(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7512))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "returning-peer-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// The reading says reachable — the peer never appeared to go away.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()
	if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || !reachable {
		t.Fatal("the tick must have recorded them as reachable, or this is not the case under test")
	}

	now := time.Now().UTC()
	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now)

	got := scheduleOf(t, svc, envelope.ID)
	if want := now.Add(deliveryRetryBackoff(0)); !got.Equal(want) {
		t.Fatalf("the wait after a session arrives must be the first schedule step: got %v, want %v", got, want)
	}
}

// TestRepeatedSessionsCannotBeatTheFirstStep is the storm guard, and the
// reason this clamps instead of firing.
//
// A peer may hold several sessions at once, and a backup one can flap
// beside a healthy primary. Every one of those events reaches the same
// helper, so the rule has to be one that repetition cannot exploit: a
// clamp only ever moves a schedule EARLIER and never past the floor, so a
// hundred events in a second buy no more than one ordinary retry step.
func TestRepeatedSessionsCannotBeatTheFirstStep(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// Reachable throughout — that is the case under test: a backup session
	// flapping beside a primary that is carrying the traffic.
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7524))
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "flapping-backup-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	peer := domain.PeerIdentityFromWire(recipientID.Address)
	start := time.Now().UTC()
	svc.wakeOverdueForReturningPeer(peer, start)
	first := scheduleOf(t, svc, envelope.ID)
	if floor := start.Add(deliveryRetryBackoff(0)); first.Before(floor) {
		t.Fatalf("the floor is the first schedule step: got %v, want no earlier than %v", first, floor)
	}

	// The accelerated attempt goes out — armed by a tick, as in
	// production, because the arm is what marks the dispatch as the answer
	// to the wake-up — and a sink confirms it, which rebuilds the backoff
	// and, one queue window later, makes the receipt overdue again. THIS
	// is the cycle a per-event clamp would ride for ever: emit, rebuild,
	// overdue, clamp, emit...
	emitted := start.Add(deliveryRetryBackoff(0))
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = emitted
	svc.deliveryMu.Unlock()
	_, candidates := svc.planDueDeliveries(emitted, svc.measureRecipientReachability())
	svc.armDueDeliveries(candidates, recipientReachability{gateOff: true}, emitted)
	if !svc.confirmEnvelopeInMemory(envelope, emitted) {
		t.Fatal("the accelerated attempt was not confirmed")
	}
	rebuilt := scheduleOf(t, svc, envelope.ID)
	// Later than the hold park the arm leaves behind (60 s), or the test
	// would pass on an entry that was never rebuilt at all.
	if !rebuilt.After(emitted.Add(deliveryHoldPollInterval)) {
		t.Fatalf("the emission must put the entry back on its own backoff, got %v (arm parks at %v)", rebuilt, emitted.Add(deliveryHoldPollInterval))
	}

	// A hundred more sessions arrive, well past the queue window, with the
	// peer reachable throughout — no pass has ever measured them away.
	for i := 1; i <= 100; i++ {
		svc.wakeOverdueForReturningPeer(peer, emitted.Add(deliveryQueueWindow+time.Duration(i)*time.Second))
	}

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(rebuilt) {
		t.Fatalf("repeated sessions kept pulling the message off its backoff: %v → %v", rebuilt, got)
	}
}

// TestArrivingSessionKeepsTheWindowAndOtherConversations keeps the clamp
// as narrow as its justification: only messages to the peer whose session
// arrived, and never inside the window where a receipt could still be in
// flight — though a message in that window is deferred to the floor rather
// than left on the tail, because a skipped observation is a lost one.
func TestArrivingSessionKeepsTheWindowAndOtherConversations(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	returning, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	other, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	stalledOnWireEntry(t, svc, protocol.Envelope{
		ID: "returning-recent", Topic: "dm",
		Sender: svc.Address(), Recipient: returning.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}, time.Second)
	stalledOnWireEntry(t, svc, protocol.Envelope{
		ID: "somebody-else", Topic: "dm",
		Sender: svc.Address(), Recipient: other.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}, 5*time.Minute)

	recentParked := scheduleOf(t, svc, "returning-recent")
	otherParked := scheduleOf(t, svc, "somebody-else")

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(returning.Address), time.Now().UTC())

	svc.deliveryMu.RLock()
	emitted := svc.awaitingDelivered["returning-recent"].LastEmittedAt
	svc.deliveryMu.RUnlock()
	got := scheduleOf(t, svc, "returning-recent")
	if !got.Before(recentParked) {
		t.Errorf("an arriving session must shorten the wait even for a recent message: %v → %v", recentParked, got)
	}
	if window := emitted.Add(deliveryQueueWindow); got.Before(window) {
		t.Errorf("nothing may be scheduled while a receipt could still be in flight: got %v, window ends %v", got, window)
	}
	if got := scheduleOf(t, svc, "somebody-else"); !got.Equal(otherParked) {
		t.Errorf("another conversation was clamped by a stranger arriving: %v → %v", otherParked, got)
	}
}

// TestArrivingSessionStandsDownWithTheGateOff keeps the kill switch honest
// on this path too.
func TestArrivingSessionStandsDownWithTheGateOff(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = false

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "gate-off-2", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	parked := scheduleOf(t, svc, envelope.ID)

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), time.Now().UTC())

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("the arriving-session clamp ran with the reachability gate off: %v → %v", parked, got)
	}
}

// lastReadingFor exposes what the previous pass measured for one recipient,
// so a test can state the precondition it is actually testing instead of
// assuming it.
func lastReadingFor(svc *Service, recipient string) (reachable, measured bool) {
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	record, measured := svc.recipientPassState[recipient]
	return record.reachable, measured
}

// TestPresenceObservedAbsenceEarnsTheAcceleration covers the recipient the
// pass cannot count: one reachable through a transit hop throughout. No pass
// ever measures them away, so their visit never ends, and their return earned
// nothing — the message stayed on a backoff that reaches eleven minutes while
// the person was demonstrably back.
//
// What ends the visit is presence OBSERVING THE ABSENCE, not presence
// reporting the return. The distinction is the whole of the fix: see
// TestOnePhysicalReturnIsOneAccelerationHoweverManySawIt for what the other
// choice cost.
func TestPresenceObservedAbsenceEarnsTheAcceleration(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// Attached for the whole test: the pass can always reach them, which is
	// exactly the case the visit counter is blind to.
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7531))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "presence-return-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	peer := domain.PeerIdentityFromWire(recipientID.Address)

	svc.wakeOverdueForReturningPeer(peer, time.Now().UTC())
	parked := parkDeliveryOnTheTail(t, svc, envelope.ID)

	// A second return with nothing in between is a repeat, and must do
	// nothing.
	svc.wakeOverdueForReturningPeer(peer, time.Now().UTC())
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Fatalf("a repeated return must not step over the backoff: %v → %v", parked, got)
	}

	// Presence sees them go: a probe went unanswered while the path stayed
	// visible. That is the absence, and it earns the next acceleration.
	svc.presenceProjector.noteProbeUnanswered(peer, svc.presenceNow())
	now := time.Now().UTC()
	svc.wakeOverdueForReturningPeer(peer, now)
	if want, got := now.Add(deliveryRetryBackoff(0)), scheduleOf(t, svc, envelope.ID); !got.Equal(want) {
		t.Fatalf("the return after a presence-observed absence earned nothing: parked %v, want %v, got %v", parked, want, got)
	}
}

// TestOnePhysicalReturnIsOneAccelerationHoweverManySawIt pins the contract
// that a second occasion counter broke: one departure and one return earn ONE
// accelerated attempt, however many observers report that return.
//
// The counter lives on the ABSENCE side for this reason and no other. Counting
// returns instead — a separate presenceReturn counter beside visit — meant the
// pass measuring "reachable again" and the proof arriving moments later were
// each fresh to the other's spend, so one physical return pulled the message
// off its backoff twice: once, then again after the backoff had been rebuilt.
// Extra bumps for one absence are harmless (a return spends against the current
// value, once); extra bumps for one return are the duplicate.
func TestOnePhysicalReturnIsOneAccelerationHoweverManySawIt(t *testing.T) {
	t.Parallel()

	// Each case is the SAME physical return seen twice, in both orders. The
	// second observer must find the occasion already spent.
	for _, order := range []struct {
		name   string
		first  func(*Service, domain.PeerIdentity, time.Time)
		second func(*Service, domain.PeerIdentity, time.Time)
	}{
		{
			name:   "the pass sees it, then the proof arrives",
			first:  func(s *Service, p domain.PeerIdentity, at time.Time) { s.retryDueDeliveries(at) },
			second: (*Service).wakeOverdueForReturningPeer,
		},
		{
			name:   "the session event lands, then the proof arrives",
			first:  (*Service).wakeOverdueForReturningPeer,
			second: (*Service).wakeOverdueForReturningPeer,
		},
		{
			name:   "the proof arrives, then the pass sees it",
			first:  (*Service).wakeOverdueForReturningPeer,
			second: func(s *Service, p domain.PeerIdentity, at time.Time) { s.retryDueDeliveries(at) },
		},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			svc.cfg.HoldDMUntilReachable = true

			recipientID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7532))

			createdAt := time.Now().UTC().Add(-72 * time.Hour)
			envelope := protocol.Envelope{
				ID: "one-return-1", Topic: "dm",
				Sender: svc.Address(), Recipient: recipientID.Address,
				Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
			}
			stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
			peer := domain.PeerIdentityFromWire(recipientID.Address)

			// One departure, measured by the pass — the ordinary case, and the
			// one that earns exactly one accelerated attempt.
			gone := detachPushObserver(t, svc, recipientID.Address)
			svc.retryDueDeliveries(time.Now().UTC())
			if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || reachable {
				t.Fatal("the pass must have measured them unreachable, or this is not the case under test")
			}
			reattachPushObserver(t, svc, recipientID.Address, gone)
			parkDeliveryOnTheTail(t, svc, envelope.ID)

			// The return, seen by the first observer: it spends the occasion.
			order.first(svc, peer, time.Now().UTC())
			svc.WaitBackground()
			parked := parkDeliveryOnTheTail(t, svc, envelope.ID)

			// And by the second, which is the SAME return: nothing more is
			// owed, so the schedule must not move.
			order.second(svc, peer, time.Now().UTC())
			svc.WaitBackground()
			if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
				t.Fatalf("one return earned two accelerations: %v → %v", parked, got)
			}
		})
	}
}

// parkDeliveryOnTheTail puts one delivery back on the far end of its backoff
// with its receipt window long expired, so the next acceleration — or its
// absence — is unambiguous.
func parkDeliveryOnTheTail(t *testing.T, svc *Service, id protocol.MessageID) time.Time {
	t.Helper()
	svc.deliveryMu.Lock()
	defer svc.deliveryMu.Unlock()
	svc.awaitingDelivered[id].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	svc.awaitingDelivered[id].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	return svc.awaitingDelivered[id].NextAttemptAt
}

// TestAccelerationIsEarnedAgainByAnAbsence is the other half of the same
// rule. Spending the accelerated attempt must not disarm the feature for
// good: a recipient this node genuinely cannot reach, and then can, is the
// transition the acceleration exists for, and a pass that measures them
// away gives it back.
func TestAccelerationIsEarnedAgainByAnAbsence(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	sub := attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7514))
	_ = sub

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "earned-again-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	peer := domain.PeerIdentityFromWire(recipientID.Address)
	svc.wakeOverdueForReturningPeer(peer, time.Now().UTC())
	spent := scheduleOf(t, svc, envelope.ID)

	// Park it on the tail again and confirm the acceleration is spent.
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	parked := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	svc.deliveryMu.Unlock()
	svc.wakeOverdueForReturningPeer(peer, time.Now().UTC())
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Fatalf("the accelerated attempt was already spent: %v → %v", parked, got)
	}
	_ = spent

	// They go away, and a pass measures it. That is what earns it back.
	detachPushObserver(t, svc, recipientID.Address)
	svc.retryDueDeliveries(time.Now().UTC())
	if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || reachable {
		t.Fatal("the pass must have measured them unreachable, or this is not the case under test")
	}
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()

	now := time.Now().UTC()
	svc.wakeOverdueForReturningPeer(peer, now)
	if want, got := now.Add(deliveryRetryBackoff(0)), scheduleOf(t, svc, envelope.ID); !got.Equal(want) {
		t.Fatalf("an absence must earn the acceleration back: got %v, want %v", got, want)
	}
}

// TestReadingCommittedAtTheEndKeepsWhatThePassLearnedLate is the ordering
// rule on the stored reading.
//
// The pass measures, plans, and only then tops the reading up for
// deliveries registered after its snapshot. A commit that ran in the middle
// stored a reading that was still being written, so those recipients never
// reached the state at all — and the next pass, having no previous answer
// for them, could not see a transition.
func TestReadingCommittedAtTheEndKeepsWhatThePassLearnedLate(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	early, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	late, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, early.Address, netcore.ConnID(7515))
	attachPushObserver(t, svc, late.Address, netcore.ConnID(7516))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	stalledOnWireEntry(t, svc, protocol.Envelope{
		ID: "measured-early", Topic: "dm",
		Sender: svc.Address(), Recipient: early.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}, 5*time.Minute)

	// The pass takes its reading, and only THEN a second conversation is
	// registered and becomes a candidate — the window measureMissing
	// exists for.
	now := time.Now().UTC()
	reachable := svc.measureRecipientReachability()
	stalledOnWireEntry(t, svc, protocol.Envelope{
		ID: "measured-late", Topic: "dm",
		Sender: svc.Address(), Recipient: late.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}, 5*time.Minute)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["measured-late"].NextAttemptAt = now
	svc.deliveryMu.Unlock()

	_, candidates := svc.planDueDeliveries(now, reachable)
	svc.finishRecipientReading(reachable, candidates)

	// The stored answer for the late one must be the MEASURED answer —
	// they are reachable. A commit that ran before the top-up stored the
	// zero value instead, which reads as "unreachable" and would hand the
	// next pass a transition that never happened.
	if reachable, measured := lastReadingFor(svc, late.Address); !measured || !reachable {
		t.Errorf("a reading taken late in the pass must reach the stored state: measured=%v reachable=%v", measured, reachable)
	}
	if reachable, measured := lastReadingFor(svc, early.Address); !measured || !reachable {
		t.Errorf("the reading taken at the start of the pass is stored too: measured=%v reachable=%v", measured, reachable)
	}
}

// TestSessionEventDuringAPassIsNotOverwritten is the other half: the pass
// is not the only writer of that state.
//
// A session event can clamp — and spend the recipient's accelerated
// attempt — at any moment, including while a pass is running. A commit that
// replaced the state wholesale from its own snapshot handed the same
// recipient another acceleration, which is exactly the re-send loop the
// spending exists to prevent.
func TestSessionEventDuringAPassIsNotOverwritten(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7517))

	now := time.Now().UTC()
	peer := domain.PeerIdentityFromWire(recipientID.Address)

	// The pass takes its reading BEFORE this conversation exists, so the
	// recipient is not in its snapshot at all — the case a wholesale
	// replace loses outright, because the key is simply not there to copy.
	reachable := svc.measureRecipientReachability()

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "not-overwritten-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// A session arrives and spends the acceleration, and only then does
	// the pass finish and store its reading.
	svc.wakeOverdueForReturningPeer(peer, now)
	_, candidates := svc.planDueDeliveries(now, reachable)
	svc.finishRecipientReading(reachable, candidates)

	// Park it back on the tail: a second session event must now find the
	// acceleration already spent.
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(11 * time.Minute)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-5 * time.Minute)
	parked := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	svc.deliveryMu.Unlock()

	svc.wakeOverdueForReturningPeer(peer, now.Add(time.Second))

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("the pass overwrote a session event's spent acceleration: %v → %v", parked, got)
	}
}

// TestUnmeasuredNewRecipientIsNotWrittenDownAsUnreachable closes the last
// way a fabricated reading could get into the state.
//
// A message registered after the pass took its reading, and not due for
// another thirty seconds, is measured by nobody: not by the snapshot, which
// predates it, and not by the top-up, which only measures candidates. The
// honest record for it is NO record. Storing the zero value instead wrote
// down "unreachable" — a fact nobody observed — and the very next pass read
// that as a return and re-sent the message to a recipient who had never
// gone anywhere.
func TestUnmeasuredNewRecipientIsNotWrittenDownAsUnreachable(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7520)))

	// The pass takes its reading while nothing is awaiting.
	now := time.Now().UTC()
	reachable := svc.measureRecipientReachability()

	// Only then is the message written: not due for thirty seconds, so it
	// is not a candidate either and the top-up never looks at it.
	envelope := protocol.Envelope{
		ID: "unmeasured-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, false)
	svc.deliveryMu.Unlock()
	parked := scheduleOf(t, svc, envelope.ID)

	_, candidates := svc.planDueDeliveries(now, reachable)
	svc.finishRecipientReading(reachable, candidates)

	if _, measured := lastReadingFor(svc, recipientID.Address); measured {
		t.Error("a recipient nobody measured must have no stored reading, not a fabricated one")
	}

	// The next pass, two seconds later, must not read a transition.
	svc.retryDueDeliveries(now.Add(2 * time.Second))
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(parked) {
		t.Errorf("a fabricated reading re-armed a message whose recipient never left: %v → %v", parked, got)
	}
	stream.expectQuiet(t, 200*time.Millisecond)
}

// TestReconnectInsideTheWindowIsNotLost is the reported gap, end to end.
//
// The client reconnects ten seconds after the last emission — inside the
// window where a receipt could still be coming. Sending a copy now would
// be wrong, but so is forgetting: nothing stores "they came back", so an
// entry that is merely skipped goes back to the eleven-minute tail. The
// wait is shortened to the end of the window instead, and the message goes
// out on the first tick after it.
func TestReconnectInsideTheWindowIsNotLost(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7521)))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "inside-window-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// On the eleven-minute step, emitted ten seconds ago.
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-10 * time.Second)
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(11 * time.Minute)
	tail := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	emitted := svc.awaitingDelivered[envelope.ID].LastEmittedAt
	svc.deliveryMu.Unlock()

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now)

	got := scheduleOf(t, svc, envelope.ID)
	if !got.Before(tail) {
		t.Fatalf("a reconnect inside the queue window was lost: still %v", got)
	}
	if window := emitted.Add(deliveryQueueWindow); got.Before(window) {
		t.Fatalf("nothing may be scheduled inside the window: got %v, window ends %v", got, window)
	}

	// The first tick after the scheduled moment sends it.
	svc.retryDueDeliveries(got.Add(time.Second))
	stream.expect(t, "inside-window-1")
}

// TestTransitionInsideTheWindowIsDeferredToItsEnd is the same rule on the
// measured path: a recipient that was unreachable and is reachable again,
// whose last emission is still young, is scheduled at the end of the
// window rather than skipped.
func TestTransitionInsideTheWindowIsDeferredToItsEnd(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "transition-window-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// A pass measures them unreachable; then they come back and the next
	// pass sees the transition — with the last emission ten seconds old.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()
	if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || reachable {
		t.Fatal("the pass must have measured them unreachable, or this is not the case under test")
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7522))

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-10 * time.Second)
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(11 * time.Minute)
	tail := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	emitted := svc.awaitingDelivered[envelope.ID].LastEmittedAt
	svc.deliveryMu.Unlock()

	reachable := svc.measureRecipientReachability()
	svc.deliveryMu.Lock()
	svc.applyReachabilityReturnLocked(svc.awaitingDelivered[envelope.ID], reachable, now)
	got := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	svc.deliveryMu.Unlock()

	if want := emitted.Add(deliveryQueueWindow); !got.Equal(want) {
		t.Fatalf("the transition must be deferred to the end of the window: got %v, want %v (tail was %v)", got, want, tail)
	}
}

// TestLateConfirmationDoesNotUndoAReconnect is the ordering rule on the
// schedule, and the mirror of the one on the reading.
//
// A retry is handed to a sink; its confirmation is slow. The client
// reconnects in the meantime and the schedule is shortened. Then the late
// confirmation arrives and rebuilds the backoff from ITS dispatch — the
// long tail. That answer is about the attempt made BEFORE the reconnect and
// says nothing about the recipient being back, and letting it win threw the
// whole wake-up away: the acceleration is already spent and the recipient
// already reads as reachable, so nothing would shorten the wait again.
func TestLateConfirmationDoesNotUndoAReconnect(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "late-confirm-2", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// An attempt is handed to a sink at t1 and its confirmation is slow.
	now := time.Now().UTC()
	dispatchedAt := now

	// The client reconnects at t2 and the schedule is shortened.
	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now.Add(time.Second))
	shortened := scheduleOf(t, svc, envelope.ID)

	// The t1 confirmation lands at t3, after the reconnect.
	if !svc.confirmEnvelopeInMemory(envelope, dispatchedAt) {
		t.Fatal("the late confirmation was not recorded")
	}

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(shortened) {
		t.Fatalf("a confirmation from before the reconnect undid it: %v → %v", shortened, got)
	}

	// And the accelerated attempt — armed by the tick that the wake-up
	// asked for — does rebuild the backoff once IT is confirmed,
	// otherwise the entry would never leave the fast end.
	afterWake := shortened.Add(time.Second)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = afterWake
	svc.deliveryMu.Unlock()
	_, candidates := svc.planDueDeliveries(afterWake, svc.measureRecipientReachability())
	svc.armDueDeliveries(candidates, recipientReachability{gateOff: true}, afterWake)
	if !svc.confirmEnvelopeInMemory(envelope, afterWake) {
		t.Fatal("the accelerated attempt's confirmation was not recorded")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.After(afterWake.Add(deliveryHoldPollInterval)) {
		t.Errorf("a confirmation of the accelerated attempt must rebuild the backoff, got %v (arm parks at %v)", got, afterWake.Add(deliveryHoldPollInterval))
	}
}

// TestOrderSurvivesAFrozenClock is the coarse-clock edge of the same rule,
// and the reason it is an order and not a timestamp.
//
// On Windows the wall clock ticks in 0.5-15.6 ms steps, so a dispatch, the
// reconnect that follows it and the late confirmation can all carry the
// same instant — and the two cases a timestamp would have to tell apart
// legitimately share it: a confirmation from BEFORE the wake-up, and the
// confirmation of the accelerated attempt the same tick then makes. Every
// moment in this test is the same instant; only the ORDER of the calls
// differs, and that is all the rule uses.
func TestOrderSurvivesAFrozenClock(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "frozen-clock-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	frozen := time.Now().UTC()

	// An attempt is dispatched, the client reconnects, and the dispatch's
	// confirmation lands — all at the same instant, in that order.
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = frozen
	svc.deliveryMu.Unlock()
	_, candidates := svc.planDueDeliveries(frozen, svc.measureRecipientReachability())
	svc.armDueDeliveries(candidates, recipientReachability{gateOff: true}, frozen)

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), frozen)
	shortened := scheduleOf(t, svc, envelope.ID)

	if !svc.confirmEnvelopeInMemory(envelope, frozen) {
		t.Fatal("the late confirmation was not recorded")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(shortened) {
		t.Fatalf("with a frozen clock the older confirmation undid the reconnect: %v → %v", shortened, got)
	}

	// The accelerated attempt is armed — still the same instant — and its
	// confirmation does rebuild the backoff.
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = frozen
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = frozen.Add(-time.Minute)
	svc.deliveryMu.Unlock()
	_, candidates = svc.planDueDeliveries(frozen, svc.measureRecipientReachability())
	svc.armDueDeliveries(candidates, recipientReachability{gateOff: true}, frozen)
	if !svc.confirmEnvelopeInMemory(envelope, frozen) {
		t.Fatal("the accelerated attempt's confirmation was not recorded")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.After(frozen.Add(deliveryHoldPollInterval)) {
		t.Errorf("the accelerated attempt's own confirmation must rebuild the backoff, got %v (arm parks at %v)", got, frozen.Add(deliveryHoldPollInterval))
	}
}

// TestBacklogReplayAnswersTheWakeUpInEitherOrder covers the OTHER sender.
//
// A reconnect fires two goroutines: the backlog replay, which sends down
// the connection the peer has just opened, and the wake-up. Their order is
// nobody's to choose, and the replay never goes through the retry engine's
// arm — so if only the arm answered a wake-up, the replay's copy went out
// and its confirmation was still mistaken for one from before the
// reconnect, leaving the entry on the first schedule step and sending a
// second copy thirty seconds later.
//
// The replay runs for real here (pushBacklogToSubscriber), not through a
// stand-in: what is under test is whether that path answers the wake-up,
// and a test that marked the dispatch itself would answer its own
// question.
func TestBacklogReplayAnswersTheWakeUpInEitherOrder(t *testing.T) {
	t.Parallel()

	for _, order := range []struct {
		name      string
		wakeFirst bool
	}{
		{"wake first, then the replay", true},
		{"replay first, then the wake", false},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			svc.cfg.HoldDMUntilReachable = true

			recipientID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7523)))
			svc.gossipMu.RLock()
			sub := svc.subs[recipientID.Address]["observer"]
			svc.gossipMu.RUnlock()
			if sub == nil {
				t.Fatal("the observer must be subscribed for the replay to find it")
			}

			createdAt := time.Now().UTC().Add(-72 * time.Hour)
			envelope := protocol.Envelope{
				ID: "backlog-order-1", Topic: "dm",
				Sender: svc.Address(), Recipient: recipientID.Address,
				Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
			}
			stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
			// The backlog the replay reads is the node's own dm topic.
			svc.gossipMu.Lock()
			svc.topics["dm"] = append(svc.topics["dm"], envelope)
			svc.gossipMu.Unlock()

			peer := domain.PeerIdentityFromWire(recipientID.Address)
			now := time.Now().UTC()
			if order.wakeFirst {
				svc.wakeOverdueForReturningPeer(peer, now)
				svc.pushBacklogToSubscriber(sub)
			} else {
				svc.pushBacklogToSubscriber(sub)
				svc.wakeOverdueForReturningPeer(peer, now)
			}
			svc.WaitBackground()

			got := scheduleOf(t, svc, envelope.ID)
			if order.wakeFirst {
				// The replay IS the accelerated attempt, so what follows
				// it is the ordinary backoff — later than the hold park
				// an arm would leave, and far later than the first step.
				if !got.After(now.Add(deliveryHoldPollInterval)) {
					t.Fatalf("the replay answered the wake-up, so the backoff must rebuild: got %v", got)
				}
				return
			}
			// Replay first: it already IS this reconnect's accelerated
			// re-send, so the wake-up that follows must leave the rebuilt
			// backoff alone. One reconnect, one re-send, either order.
			if !got.After(now.Add(deliveryHoldPollInterval)) {
				t.Fatalf("the replay's own backoff must stand after the wake-up: got %v", got)
			}
			svc.wakeOverdueForReturningPeer(peer, now.Add(time.Second))
			if again := scheduleOf(t, svc, envelope.ID); !again.Equal(got) {
				t.Fatalf("a further session event shortened the wait again: %v → %v", got, again)
			}
		})
	}
}

// TestPartialReplayLeavesTheRestAccelerable is the granularity rule.
//
// A backlog replay sends what its in-memory topic snapshot holds and what
// the writer accepts, which is not always every unconfirmed message a
// recipient has: after a sender restart the reseeded entries are awaiting a
// receipt without being in that snapshot at all. Spending ONE acceleration
// for the whole conversation left exactly those messages on the tail —
// eleven minutes after the peer came back — because the wake-up that
// followed found the recipient already "accelerated". The mark belongs to
// the message the send actually covered.
func TestPartialReplayLeavesTheRestAccelerable(t *testing.T) {
	t.Parallel()

	for _, order := range []struct {
		name      string
		wakeFirst bool
	}{
		{"replay first, then the wake", false},
		{"wake first, then the replay", true},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			svc.cfg.HoldDMUntilReachable = true

			recipientID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7525)))
			svc.gossipMu.RLock()
			sub := svc.subs[recipientID.Address]["observer"]
			svc.gossipMu.RUnlock()

			createdAt := time.Now().UTC().Add(-72 * time.Hour)
			inBacklog := protocol.Envelope{
				ID: "replayed", Topic: "dm",
				Sender: svc.Address(), Recipient: recipientID.Address,
				Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
			}
			missed := protocol.Envelope{
				ID: "not-replayed", Topic: "dm",
				Sender: svc.Address(), Recipient: recipientID.Address,
				Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
			}
			stalledOnWireEntry(t, svc, inBacklog, 5*time.Minute)
			stalledOnWireEntry(t, svc, missed, 5*time.Minute)
			// Only ONE of them is in the topic snapshot the replay reads.
			svc.gossipMu.Lock()
			svc.topics["dm"] = append(svc.topics["dm"], inBacklog)
			svc.gossipMu.Unlock()

			missedParked := scheduleOf(t, svc, missed.ID)
			peer := domain.PeerIdentityFromWire(recipientID.Address)
			now := time.Now().UTC()
			if order.wakeFirst {
				svc.wakeOverdueForReturningPeer(peer, now)
				svc.pushBacklogToSubscriber(sub)
			} else {
				svc.pushBacklogToSubscriber(sub)
				svc.wakeOverdueForReturningPeer(peer, now)
			}
			svc.WaitBackground()

			// The message the replay could not cover must be accelerated:
			// it is exactly the "sent, unconfirmed, peer is back" case.
			got := scheduleOf(t, svc, missed.ID)
			if !got.Before(missedParked) {
				t.Fatalf("the message the replay did not send was left on the tail: %v (was %v)", got, missedParked)
			}
			// And the one the replay DID send is on its ordinary backoff,
			// not on the first step: it has had its accelerated attempt.
			if replayed := scheduleOf(t, svc, inBacklog.ID); !replayed.After(now.Add(deliveryHoldPollInterval)) {
				t.Fatalf("the replayed message must be back on its own backoff, got %v", replayed)
			}
		})
	}
}

// TestAbsenceAndReturnCommute is what the refactor is for.
//
// The pass measures an absence and applies it several steps later; a
// session event can land anywhere in that window. Under the flags this
// replaced, each writer had to interpret the other's half-written state,
// and the interleavings lost updates in both directions: a stale absence
// handed back an acceleration that had just been spent, and a session
// event that arrived first made the pass discard a real absence, so a
// genuine offline→online cycle granted nothing.
//
// A monotone visit counter and a compare-and-set commute: whichever order
// the two run in, the absence earns the next acceleration and the return
// spends exactly one.
func TestAbsenceAndReturnCommute(t *testing.T) {
	t.Parallel()

	for _, order := range []struct {
		name       string
		eventFirst bool
	}{
		{"the pass applies its absence first", false},
		{"the session event lands first", true},
	} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			svc.cfg.HoldDMUntilReachable = true

			recipientID, err := identity.Generate()
			if err != nil {
				t.Fatalf("identity.Generate: %v", err)
			}
			stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7526)))

			createdAt := time.Now().UTC().Add(-72 * time.Hour)
			envelope := protocol.Envelope{
				ID: "commute-1", Topic: "dm",
				Sender: svc.Address(), Recipient: recipientID.Address,
				Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
			}
			stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
			peer := domain.PeerIdentityFromWire(recipientID.Address)

			// A first return, spent: this is the state in which the two
			// findings bite — the acceleration is gone and only a real
			// absence may earn another.
			svc.wakeOverdueForReturningPeer(peer, time.Now().UTC())
			svc.deliveryMu.Lock()
			svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
			svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
			tail := svc.awaitingDelivered[envelope.ID].NextAttemptAt
			svc.deliveryMu.Unlock()

			// They go away: the pass measures it, but does not apply the
			// reading yet.
			sub := detachPushObserver(t, svc, recipientID.Address)
			absence := svc.measureRecipientReachability()
			if canReach, measured := absence.canReach(recipientID.Address); !measured || canReach {
				t.Fatalf("the pass must have measured an absence: measured=%v reachable=%v", measured, canReach)
			}

			// And come back. The session event and the pass's commit race.
			reattachPushObserver(t, svc, recipientID.Address, sub)
			now := time.Now().UTC()
			if order.eventFirst {
				svc.wakeOverdueForReturningPeer(peer, now)
				_, candidates := svc.planDueDeliveries(now, absence)
				svc.finishRecipientReading(absence, candidates)
			} else {
				_, candidates := svc.planDueDeliveries(now, absence)
				svc.finishRecipientReading(absence, candidates)
				svc.wakeOverdueForReturningPeer(peer, now)
			}

			// However they interleaved, the absence earned an acceleration
			// and the return spends it — at worst on the pass that
			// follows, which is where a return the session event could not
			// grant (its visit had not been bumped yet) gets measured. The
			// message goes out instead of waiting out the tail.
			// One pass later everything has been observed by somebody: the
			// session event granted it, or this pass measures the return
			// and grants it. Either way the message is off the tail.
			svc.retryDueDeliveries(now.Add(2 * time.Second))
			due := scheduleOf(t, svc, envelope.ID)
			if !due.Before(tail) {
				t.Fatalf("a real absence-then-return granted nothing: still %v", due)
			}
			// And it goes out at the moment that schedule names.
			svc.retryDueDeliveries(due.Add(time.Second))
			stream.expect(t, "commute-1")
			svc.WaitBackground()

			// And exactly one acceleration: with the visit spent, a
			// further session event leaves the rebuilt schedule alone.
			svc.deliveryMu.Lock()
			svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(11 * time.Minute)
			svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-5 * time.Minute)
			parked := svc.awaitingDelivered[envelope.ID].NextAttemptAt
			svc.deliveryMu.Unlock()
			svc.wakeOverdueForReturningPeer(peer, now.Add(3*time.Second))
			if again := scheduleOf(t, svc, envelope.ID); !again.Equal(parked) {
				t.Fatalf("a second return in the same visit granted another acceleration: %v → %v", parked, again)
			}
		})
	}
}

// TestMeasuredReturnSpendsTheVisitToo covers the second half: the pass's
// own transition must spend the visit, or the session event of the same
// reconnect — which can arrive after the tick has already sent — grants a
// second accelerated attempt for one return.
func TestMeasuredReturnSpendsTheVisitToo(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "measured-spend-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// Measured away, then measured back: the pass grants the acceleration.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7527))
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()

	// Whatever the pass did with it, the schedule now belongs to this
	// visit's one accelerated attempt — a session event from the same
	// reconnect must not grant another.
	parked := scheduleOf(t, svc, envelope.ID)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = time.Now().UTC().Add(11 * time.Minute)
	tail := svc.awaitingDelivered[envelope.ID].NextAttemptAt
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = time.Now().UTC().Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()
	_ = parked

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), time.Now().UTC())

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(tail) {
		t.Fatalf("the measured return did not spend the visit: a session event granted a second acceleration %v → %v", tail, got)
	}
}

// TestReturnOnAnAlreadyDueMessageIsStillOneSend covers the case where the
// return needs no schedule change at all.
//
// The message is already due when its recipient is measured back: the tick
// sends it there and then, which is exactly what the return would have
// asked for. Skipping the entry because "it is due anyway" left the visit
// unspent, so the session event of the SAME reconnect — arriving after
// that send had rebuilt the backoff — shortened the schedule again and put
// a second copy on the wire. One return, one send.
func TestReturnOnAnAlreadyDueMessageIsStillOneSend(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "already-due-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// Measured away first, so the pass that follows sees a real return.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()
	if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || reachable {
		t.Fatal("the pass must have measured them unreachable, or this is not the case under test")
	}

	// They come back, and the message is ALREADY DUE when the pass runs:
	// the tick dispatches it, and the sink confirms.
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7528)))
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)
	stream.expect(t, "already-due-1")
	svc.WaitBackground()

	// That send rebuilt the backoff. The session event of the same
	// reconnect arrives late — and must find the visit already spent.
	rebuilt := scheduleOf(t, svc, envelope.ID)
	if !rebuilt.After(now.Add(deliveryHoldPollInterval)) {
		t.Fatalf("the send must have rebuilt the backoff, got %v", rebuilt)
	}
	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now.Add(time.Second))

	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(rebuilt) {
		t.Fatalf("a late session event sent the same message a second time for one return: %v → %v", rebuilt, got)
	}
	stream.expectQuiet(t, 200*time.Millisecond)
}

// TestReturnThatNeededNoChangeStillGuardsTheSchedule is the last shape of
// the same rule: a return that asks for nothing new still has to be
// defended.
//
// The message is already scheduled sooner than the return's floor, so the
// schedule does not move — but the visit IS spent, and the send it is
// waiting for has not happened yet. A confirmation of an EARLIER dispatch
// arriving now would rebuild the long backoff over that near-term attempt,
// and nothing could shorten it again: the visit is gone. So the message is
// marked as owed a send, exactly as it is when the schedule does move.
func TestReturnThatNeededNoChangeStillGuardsTheSchedule(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "no-change-guard-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	// Scheduled sooner than the floor an arriving session would ask for,
	// with its receipt long overdue.
	now := time.Now().UTC()
	soon := now.Add(5 * time.Second)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = soon
	svc.awaitingDelivered[envelope.ID].LastEmittedAt = now.Add(-5 * time.Minute)
	svc.deliveryMu.Unlock()

	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now)
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(soon) {
		t.Fatalf("a return must not push a nearer schedule out: %v → %v", soon, got)
	}

	// A confirmation of the dispatch made BEFORE the return lands now.
	if !svc.confirmEnvelopeInMemory(envelope, now.Add(-time.Minute)) {
		t.Fatal("the late confirmation was not recorded")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(soon) {
		t.Fatalf("a confirmation from before the return replaced the attempt it was owed: %v → %v", soon, got)
	}

	// A second session event cannot help — the visit is spent — which is
	// exactly why the guard above has to hold.
	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), now.Add(time.Second))
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(soon) {
		t.Fatalf("the visit must stay spent: %v → %v", soon, got)
	}
}

// TestMidPassMessageDoesNotInheritTheConversationsReturn is the case the
// per-recipient reading cannot answer on its own.
//
// The recipient already has an unconfirmed message, so the pass measures
// them and sees a return. A NEW message to the same recipient is written
// before the pass applies that answer — and the answer is about a moment
// before it existed. Applying it pulled the new message's first retry in,
// re-sending to a peer that was already back by the time it was written.
func TestMidPassMessageDoesNotInheritTheConversationsReturn(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	old := protocol.Envelope{
		ID: "old-unconfirmed", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, old, 5*time.Minute)

	// A pass measures them away, so the next one sees a real return.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()
	if reachable, measured := lastReadingFor(svc, recipientID.Address); !measured || reachable {
		t.Fatal("the pass must have measured them unreachable, or this is not the case under test")
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7529))

	// The next pass takes its reading — the return — and only THEN is a
	// new message to the same recipient written.
	now := time.Now().UTC()
	reachable := svc.measureRecipientReachability()
	fresh := protocol.Envelope{
		ID: "written-mid-pass", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(fresh, now, false)
	freshParked := svc.awaitingDelivered[fresh.ID].NextAttemptAt
	svc.deliveryMu.Unlock()

	_, candidates := svc.planDueDeliveries(now, reachable)
	svc.finishRecipientReading(reachable, candidates)

	if got := scheduleOf(t, svc, fresh.ID); !got.Equal(freshParked) {
		t.Errorf("a message written after the reading inherited its return: %v → %v", freshParked, got)
	}
	// And the message the reading WAS about is accelerated.
	if got := scheduleOf(t, svc, old.ID); got.After(now) {
		t.Errorf("the message the pass measured must be accelerated by the return, got %v", got)
	}
}

// TestFrozenDispatchGivesTheWakeUpBack pins the interleaving where the arm's
// promise is broken between the plan and the wire.
//
// The arm clears the wake-up mark because it is about to send. A deletion
// freeze taken in that gap cancels the send: nothing reaches the wire, the
// schedule is given back — and the mark has to come back with it. Otherwise
// the return is lost outright: after the thaw a late confirmation of the
// attempt made BEFORE the recipient came back rebuilds the eleven-minute
// tail over the shortened schedule, and with the visit already spent no
// further session event can shorten it again. This is a defect of the
// wake-up rule, not of the freeze, which restored the schedule correctly.
func TestFrozenDispatchGivesTheWakeUpBack(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "frozen-wake-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7531))

	// The peer's session comes back and the schedule is clamped to the
	// first step. The attempt still on the wire was dispatched before that.
	wokeAt := time.Now().UTC()
	preReturnDispatch := wokeAt.Add(-time.Second)
	svc.wakeOverdueForReturningPeer(domain.PeerIdentityFromWire(recipientID.Address), wokeAt)
	shortened := scheduleOf(t, svc, envelope.ID)
	if !shortened.After(wokeAt) || shortened.After(wokeAt.Add(deliveryRetryBackoff(0))) {
		t.Fatalf("the return did not clamp the schedule to the first step: %v", shortened)
	}

	// The tick the wake-up asked for arrives, and a deletion freezes the
	// message after it has planned the send.
	tick := shortened.Add(time.Second)
	svc.retryDispatchBarrier = func() {
		if _, err := svc.FreezeOutgoingDeliveriesTo(
			domain.PeerIdentityFromWire(recipientID.Address), []protocol.MessageID{envelope.ID}); err != nil {
			t.Errorf("FreezeOutgoingDeliveriesTo: %v", err)
		}
	}
	svc.retryDueDeliveries(tick)
	svc.WaitBackground()
	svc.retryDispatchBarrier = nil

	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered[envelope.ID]
	parked, held := time.Time{}, holdNone
	if awaiting {
		parked, held = entry.NextAttemptAt, entry.Hold
	}
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("the frozen entry was dropped by the tick")
	}
	if held == holdNone {
		t.Fatal("the tick did not arm this entry, so the freeze window was never entered")
	}
	// Exactly the displaced schedule, not the poll park a dispatch that
	// reached the wire would leave: this is what says the send was refused.
	if !parked.Equal(shortened) {
		t.Fatalf("the freeze must give the shortened schedule back, got %v want %v (a wired dispatch would park at %v)",
			parked, shortened, tick.Add(deliveryHoldPollInterval))
	}

	// The deletion is called off, and the pre-return attempt's confirmation
	// finally lands. It measures an attempt older than the return and may
	// not spend the schedule the return earned.
	svc.ThawOutgoingDeliveries([]protocol.MessageID{envelope.ID})
	if !svc.confirmEnvelopeInMemory(envelope, preReturnDispatch) {
		t.Fatal("the late confirmation was not recorded")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(shortened) {
		t.Fatalf("a confirmation from before the return undid it across the freeze: %v → %v", shortened, got)
	}
}

// TestReplayDuringAPassDoesNotOpenAVisit is the third and last shape of
// "a writer may only replace what it read".
//
// The pass measures the recipient away, and the reconnect lands before it
// commits: the backlog replay puts the message on the wire and counts that
// as this return's accelerated attempt. If the commit then writes its older
// answer down anyway, the visit ends, the message's stamp is left behind in
// the previous one, and the very next pass — which measures the peer back —
// grants a second acceleration and sends a duplicate one queue window after
// the replay. One reconnect, one send, whatever order the writers run in.
func TestReplayDuringAPassDoesNotOpenAVisit(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7533)))
	svc.gossipMu.RLock()
	sub := svc.subs[recipientID.Address]["observer"]
	svc.gossipMu.RUnlock()
	if sub == nil {
		t.Fatal("the observer must be subscribed for the replay to find it")
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "replay-mid-pass-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.gossipMu.Unlock()

	// A pass with the peer here, so the state says reachable.
	svc.retryDueDeliveries(time.Now().UTC())
	svc.WaitBackground()

	// The peer drops and THIS pass measures the absence.
	observer := detachPushObserver(t, svc, recipientID.Address)
	reading := svc.measureRecipientReachability()
	if canReach, known := reading.canReach(recipientID.Address); !known || canReach {
		t.Fatal("the pass must have measured them away, or this is not the case under test")
	}

	// They are back before the pass commits, and the replay sends down the
	// new connection — this reconnect's one accelerated attempt.
	reattachPushObserver(t, svc, recipientID.Address, observer)
	svc.pushBacklogToSubscriber(sub)
	svc.WaitBackground()
	afterReplay := scheduleOf(t, svc, envelope.ID)

	svc.finishRecipientReading(reading, nil)

	// The next pass sees them reachable. With the absence written down it
	// would read a return here and pull the schedule back to the end of the
	// queue window; there is nothing left to return from.
	next := svc.measureRecipientReachability()
	if canReach, known := next.canReach(recipientID.Address); !known || !canReach {
		t.Fatal("the recipient must measure reachable again for this to prove anything")
	}
	svc.planDueDeliveries(time.Now().UTC(), next)
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(afterReplay) {
		t.Fatalf("a stale absence bought the reconnect a second send: %v → %v", afterReplay, got)
	}
}

// TestReplayBetweenThePlanAndTheArmStopsTheRetry closes the last gap in the
// pass: the one between choosing what to send and arming it.
//
// The reconnect backlog replay sends outside the engine, down the connection
// the peer has only now opened, and confirms as it goes. Landing in that
// window it left the tick holding a candidate that had just gone out — and
// the arm only checked that the entry still existed, so it overwrote the
// confirmation's hold and schedule and put a second copy of the very same
// message on the wire. The plan already refuses a message emitted this
// recently; the arm has to ask again, because the send it must notice
// happened after the plan looked.
func TestReplayBetweenThePlanAndTheArmStopsTheRetry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7535)))
	svc.gossipMu.RLock()
	sub := svc.subs[recipientID.Address]["observer"]
	svc.gossipMu.RUnlock()
	if sub == nil {
		t.Fatal("the observer must be subscribed for the replay to find it")
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "plan-arm-replay-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.gossipMu.Unlock()

	// Due, and the tick picks it.
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()
	reading := svc.measureRecipientReachability()
	_, candidates := svc.planDueDeliveries(now, reading)
	if len(candidates) != 1 || candidates[0].id != envelope.ID {
		t.Fatalf("the pass must have chosen this message to re-send, got %d candidates", len(candidates))
	}

	// The reconnect's replay sends it in the gap the plan cannot see past.
	svc.pushBacklogToSubscriber(sub)
	svc.WaitBackground()
	afterReplay := scheduleOf(t, svc, envelope.ID)
	svc.deliveryMu.RLock()
	heldAfterReplay := svc.awaitingDelivered[envelope.ID].Hold
	svc.deliveryMu.RUnlock()
	if !afterReplay.After(now) {
		t.Fatalf("the replay must have confirmed and rebuilt the backoff, got %v", afterReplay)
	}

	if due := svc.armDueDeliveries(candidates, reading, now); len(due) != 0 {
		t.Fatalf("the tick armed a second copy of a message the replay had just sent: %d dispatches", len(due))
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.Equal(afterReplay) {
		t.Errorf("the arm overwrote the confirmation's schedule: %v → %v", afterReplay, got)
	}
	svc.deliveryMu.RLock()
	held := svc.awaitingDelivered[envelope.ID].Hold
	svc.deliveryMu.RUnlock()
	if held != heldAfterReplay {
		t.Errorf("the arm overwrote the confirmation's hold: %v → %v", heldAfterReplay, held)
	}
}

// TestReplayAfterTheArmStopsTheDispatch closes the last window of the pass:
// arm → wire.
//
// The arm decides, the mutex is released, and the frame is written a few
// statements later. A reconnect landing in there sends the same message down
// the connection the peer has just opened and confirms it, and the dispatch
// already in flight used to write the second copy anyway. The last boundary
// before the wire re-reads the entry for the freeze and the withdrawal, so
// it asks this too — in the same lock hold, and as an order (the emission
// count the arm saw), not against a clock.
func TestReplayAfterTheArmStopsTheDispatch(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7537)))
	svc.gossipMu.RLock()
	sub := svc.subs[recipientID.Address]["observer"]
	svc.gossipMu.RUnlock()
	if sub == nil {
		t.Fatal("the observer must be subscribed for the replay to find it")
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "arm-wire-replay-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.gossipMu.Unlock()

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()

	// The reconnect replays the backlog after the tick has armed the
	// dispatch and before it writes it — the window this seam exists for.
	svc.retryDispatchBarrier = func() {
		svc.pushBacklogToSubscriber(sub)
		svc.WaitBackground()
	}
	svc.retryDueDeliveries(now)
	svc.retryDispatchBarrier = nil
	svc.WaitBackground()

	// The replay's copy, and no other.
	stream.expect(t, envelope.ID)
	stream.expectQuiet(t, 300*time.Millisecond)

	// And the entry belongs to the replay's confirmation: the rebuilt
	// backoff, not the park an arm leaves behind.
	if got := scheduleOf(t, svc, envelope.ID); !got.After(now.Add(deliveryHoldPollInterval)) {
		t.Errorf("the abandoned dispatch wrote its park over the confirmation: %v", got)
	}
}

// TestRetryStandsDownWhileAReplayIsStillWriting is the same window as
// TestReplayAfterTheArmStopsTheDispatch, stopped in the middle.
//
// The replay does not confirm as it writes: it claims its WHOLE batch at the
// last boundary, writes the frames one by one, and confirms afterwards
// (pushBacklogToSubscriber). A big backlog spends real time in that state,
// and a guard that waited for the confirmation would wave through a
// duplicate for every message still being written. So the guard reads the
// CLAIM.
//
// The two phases straddle a writer this test cannot pause without adding a
// seam to production code, so it drives the same two production entry
// points in the production order — the batch claim, then the confirmation —
// and runs the whole retry pass in between, where the writer would be.
func TestRetryStandsDownWhileAReplayIsStillWriting(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7539)))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "replay-in-flight-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()

	// Phase one of the replay: the batch is claimed for the wire. Nothing
	// is confirmed yet — the frames are still being written.
	claim := svc.noteOwnEnvelopesEmitted([]protocol.MessageID{envelope.ID}, now, nil)
	if _, withheld := claim.Withheld[envelope.ID]; withheld {
		t.Fatal("the replay's claim was refused, so this is not the case under test")
	}
	svc.deliveryMu.RLock()
	attemptsMidReplay := svc.awaitingDelivered[envelope.ID].Attempts
	svc.deliveryMu.RUnlock()

	// A whole retry pass runs while that write is in progress.
	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	// Nothing of ours reached the wire: the copy in flight is the replay's.
	stream.expectQuiet(t, 300*time.Millisecond)
	svc.deliveryMu.RLock()
	attemptsAfterPass := svc.awaitingDelivered[envelope.ID].Attempts
	svc.deliveryMu.RUnlock()
	if attemptsAfterPass != attemptsMidReplay {
		t.Fatalf("the tick sent behind the replay: attempts %d → %d", attemptsMidReplay, attemptsAfterPass)
	}

	// Phase two: the replay's frame is confirmed, and the entry is the
	// confirmation's — one send for one reconnect.
	if !svc.confirmEnvelopeInMemory(envelope, now.Add(time.Millisecond)) {
		t.Fatal("the replay's own confirmation was refused")
	}
	if got := scheduleOf(t, svc, envelope.ID); !got.After(now.Add(deliveryHoldPollInterval)) {
		t.Errorf("the confirmation must own the schedule after the pass stood down, got %v", got)
	}
}

// TestAClaimTakenDuringThePassStandsEvenWithASkewedStamp separates the two
// halves of emissionInFlight.
//
// The window half reads the standing claim's age, and a stamp further ahead
// than the window is deliberately not evidence — otherwise one corrected
// clock would silence a message for as long as the correction lasts. The
// order half is what still covers the case: a claim taken WHILE this pass
// was working is a send in progress whatever its stamp says, and the pass
// stands down for one tick. The next pass reads the stamp afresh and, if it
// is nonsense, sends.
func TestAClaimTakenDuringThePassStandsEvenWithASkewedStamp(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7541))

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "skewed-claim-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()

	reading := svc.measureRecipientReachability()
	_, candidates := svc.planDueDeliveries(now, reading)
	if len(candidates) != 1 {
		t.Fatalf("the pass must have chosen this message, got %d candidates", len(candidates))
	}

	// Another sender claims it while the pass is working, stamping with a
	// clock that is an hour ahead of ours.
	svc.deliveryMu.Lock()
	markEntryEmitted(svc.awaitingDelivered[envelope.ID], now.Add(time.Hour))
	svc.deliveryMu.Unlock()

	if due := svc.armDueDeliveries(candidates, reading, now); len(due) != 0 {
		t.Fatalf("a claim taken during the pass was ignored because its stamp looked wrong: %d dispatches", len(due))
	}

	// And the stand-down lasts one pass, not until the clock agrees: the
	// next pass reads the same stamp, finds it is not evidence of anything,
	// and sends.
	_, next := svc.planDueDeliveries(now.Add(2*time.Second), reading)
	if len(next) != 1 {
		t.Fatalf("the next pass must consider the message again, got %d candidates", len(next))
	}
	if due := svc.armDueDeliveries(next, reading, now.Add(2*time.Second)); len(due) != 1 {
		t.Fatalf("a stamp this node cannot have made must not hold the message back for good: %d dispatches", len(due))
	}
}

// TestAFailedDispatchDoesNotStandTheNextPassDown is the other half of the
// in-flight rule: a claim that produced no frame is not a send.
//
// The claim is taken at the last boundary, before the wire, so a dispatch
// that then finds no route has claimed the message and sent nothing. Left
// standing, that claim tells the next pass somebody is sending this right
// now — and the next pass is exactly the one the peer's return has just
// kicked. The message this feature exists for would sit out a queue window
// on the strength of its own failed attempt.
func TestAFailedDispatchDoesNotStandTheNextPassDown(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	now := time.Now().UTC()
	envelope := protocol.Envelope{
		ID: "failed-then-kicked-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: now, StoredAt: now,
	}
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now.Add(-time.Minute), true)
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()

	// Nobody to send to: the pass claims the message at the last boundary
	// and the dispatch finds no route.
	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	// The peer arrives and the kick pulls the schedule to now, which is what
	// happens when a session or a route appears.
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7543)))
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipientID.Address): {},
	})

	// The very next pass sends it — no waiting out a window because of an
	// attempt that never reached the wire.
	svc.retryDueDeliveries(now.Add(time.Second))
	svc.WaitBackground()
	stream.expect(t, envelope.ID)
}

// TestReplayStandsDownWhileTheRetryIsStillWriting is the other order of the
// same race, and the reason the rule belongs to the CLAIM and not to the
// retry engine's own guard.
//
// One reconnect starts both senders independently: the engine's tick and
// the backlog replay. Either can be first. A rule only the tick obeyed left
// the replay writing the second copy — the same duplicate, from the other
// side. So the last boundary refuses any sender whose message another one
// is already putting on the wire, and the replay's own skip path
// (backlog_push_withheld) is what it turns into.
func TestReplayStandsDownWhileTheRetryIsStillWriting(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7545)))
	svc.gossipMu.RLock()
	sub := svc.subs[recipientID.Address]["observer"]
	svc.gossipMu.RUnlock()
	if sub == nil {
		t.Fatal("the observer must be subscribed for the replay to find it")
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "retry-first-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.gossipMu.Unlock()

	// The tick takes the last boundary and is writing its frame: claimed,
	// nothing confirmed yet.
	now := time.Now().UTC()
	svc.deliveryMu.RLock()
	claimedAtArm := svc.awaitingDelivered[envelope.ID].ClaimedAt
	svc.deliveryMu.RUnlock()
	cleared, superseded, _ := svc.claimEnvelopeForDispatch(envelope, claimedAtArm, now)
	if !cleared || superseded {
		t.Fatalf("the tick's own claim was refused (cleared=%v superseded=%v), so this is not the case under test", cleared, superseded)
	}

	// The reconnect replays the backlog while that write is in progress.
	svc.pushBacklogToSubscriber(sub)
	svc.WaitBackground()

	stream.expectQuiet(t, 300*time.Millisecond)
	svc.deliveryMu.RLock()
	confirmed := svc.awaitingDelivered[envelope.ID].LastEmittedAt
	svc.deliveryMu.RUnlock()
	if confirmed.After(now) {
		t.Fatal("the replay wrote and confirmed a copy of a message the tick was already sending")
	}
}

// TestALongPassDoesNotAgeItsOwnClaim pins where the claim's instant comes
// from.
//
// A pass carries ONE `now`, taken when it started, through every recipient
// it serves. That value is right for the schedule and wrong for the claim: a
// pass that spends longer than the queue window — many recipients, a slow
// journal — would stamp a claim that is already older than the window at the
// moment it is taken, and the next sender would read "nothing in flight" and
// write a second copy of the frame going out right then.
//
// The decision point is driven directly, because the writers of that same
// send cross the boundary again a moment later and re-stamp the claim with
// the real clock, which hides the defect from a test that runs a whole pass.
func TestALongPassDoesNotAgeItsOwnClaim(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stream := newPushMessageStream(t, attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7547)))
	svc.gossipMu.RLock()
	sub := svc.subs[recipientID.Address]["observer"]
	svc.gossipMu.RUnlock()
	if sub == nil {
		t.Fatal("the observer must be subscribed for the replay to find it")
	}

	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "long-pass-claim-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)
	svc.gossipMu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.gossipMu.Unlock()

	// The tick began more than a queue window ago and only now reaches this
	// message's last boundary. The claim it takes is being taken NOW.
	startedAt := time.Now().UTC().Add(-30 * time.Second)
	svc.deliveryMu.RLock()
	claimedAtArm := svc.awaitingDelivered[envelope.ID].ClaimedAt
	svc.deliveryMu.RUnlock()
	cleared, superseded, wrote := svc.claimEnvelopeForDispatch(envelope, claimedAtArm, startedAt)
	if !cleared || superseded {
		t.Fatalf("the tick's own claim was refused (cleared=%v superseded=%v)", cleared, superseded)
	}
	if age := time.Since(wrote); age > deliveryQueueWindow {
		t.Fatalf("the claim was stamped %s ago at the moment it was taken: it is born expired", age)
	}

	// Its frame is being written when the reconnect replays the backlog.
	svc.pushBacklogToSubscriber(sub)
	svc.WaitBackground()
	stream.expectQuiet(t, 300*time.Millisecond)
	svc.deliveryMu.RLock()
	confirmed := svc.awaitingDelivered[envelope.ID].LastEmittedAt
	svc.deliveryMu.RUnlock()
	if confirmed.After(startedAt) {
		t.Fatal("the replay wrote a second copy of a message the tick had just claimed")
	}
}

// TestTheClaimIsStampedInsideTheSection pins WHERE the claim's clock is
// read, not just which clock it is.
//
// Waiting for the delivery mutex is itself time. On a busy node the wait can
// outlast the queue window, so a stamp read on the way INTO the section is
// already stale when it is finally written, and the next sender reads
// "nothing in flight" for a frame that is going out right then — the same
// duplicate, arrived by a different route.
//
// The test does not sleep. It asks the clock itself where it is being
// called from: if the delivery mutex can be taken at that moment, the caller
// does not hold it, and the read is outside the section.
func TestTheClaimIsStampedInsideTheSection(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	var mu sync.Mutex
	var readsOutside, reads int
	svc.deliveryClock = func() time.Time {
		if svc.deliveryMu.TryLock() {
			// Nobody holds the delivery mutex, so neither does the caller.
			svc.deliveryMu.Unlock()
			mu.Lock()
			readsOutside++
			mu.Unlock()
		}
		mu.Lock()
		reads++
		mu.Unlock()
		return time.Now()
	}

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	attachPushObserver(t, svc, recipientID.Address, netcore.ConnID(7549))
	createdAt := time.Now().UTC().Add(-72 * time.Hour)
	envelope := protocol.Envelope{
		ID: "claim-inside-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipientID.Address,
		Payload: []byte("sealed"), CreatedAt: createdAt, StoredAt: createdAt,
	}
	stalledOnWireEntry(t, svc, envelope, 5*time.Minute)

	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.awaitingDelivered[envelope.ID].NextAttemptAt = now.Add(-time.Second)
	svc.deliveryMu.Unlock()

	// A whole pass: the arm reads the claim, the last boundary takes it.
	svc.retryDueDeliveries(now)
	svc.WaitBackground()

	mu.Lock()
	outside, total := readsOutside, reads
	mu.Unlock()
	if total == 0 {
		t.Fatal("the claim clock was never read, so this test proves nothing")
	}
	if outside != 0 {
		t.Fatalf("%d of %d claim-clock reads happened outside the delivery section: the wait for the mutex ages the stamp", outside, total)
	}
}
