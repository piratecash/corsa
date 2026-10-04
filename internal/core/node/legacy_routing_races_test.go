package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_routing_races_test.go pins three orderings a review found between a
// legacy connection's routing input and the v2 proof of the identity it names
// (docs/refactoring/n1-legacy-residual.md §8).

// unpinnedRoutingNode is a v2-capable node on which X has not proved itself
// yet, with an impostor's legacy session naming X.
func unpinnedRoutingNode(t *testing.T) (*Service, sessionv2.Peer, *peerSession) {
	t.Helper()
	svc := newLegacyPenaltyService(t)
	svc.secureSessions = &secureSessions{
		mode:  sessionv2.ModeTransition,
		store: loadSecureSessionStore("", time.Now),
		marks: newSessionAddressMarks(time.Now),
	}
	peer, _ := sessionv2test.NewProvenPeer(t)
	svc.announceLoop.StateRegistry().MarkReconnected(peer.Identity, routingTwinsCaps)
	impostor := legacyOutboundSession(legacyAttackerAddress, peer.Identity)
	impostor.capabilities = routingTwinsCaps
	svc.peerMu.Lock()
	svc.sessions[impostor.address] = impostor
	svc.peerMu.Unlock()
	return svc, peer, impostor
}

// proveV2 is what openInboundTransport / provenTransport do once X proved
// itself: the pin is stored, then the hook runs.
func proveV2(t *testing.T, svc *Service, peer sessionv2.Peer) {
	t.Helper()
	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := svc.onIdentityProvenV2(context.Background(), sessionv2test.Proof(t, peer)); err != nil {
		t.Fatalf("v2 proof: %v", err)
	}
}

// P1: a legacy announce admitted BEFORE the proof and stopped between its
// admission and its write. The proof must not complete — and must not purge —
// until that write is done, so the purge covers it.
func TestAV2ProofWaitsForLegacyRoutingInputAlreadyAdmitted(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	admitted := make(chan struct{})
	resume := make(chan struct{})
	held := false
	svc.routingInputAdmittedHook = func(sender routingSender, frameType string) {
		if held || frameType != "announce_routes" {
			return
		}
		held = true
		close(admitted)
		<-resume
	}

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(idTargetX))
	}()
	<-admitted

	proofDone := make(chan struct{})
	go func() {
		defer close(proofDone)
		proveV2(t, svc, peer)
	}()
	select {
	case <-proofDone:
		t.Fatal("the v2 proof completed while a legacy write it must cover was still pending")
	case <-time.After(200 * time.Millisecond):
	}

	close(resume)
	<-handlerDone
	select {
	case <-proofDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the v2 proof never completed after the legacy write finished")
	}
	if routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Fatal("a legacy write that raced the v2 proof survived the purge")
	}
}

// P1, cancellation: the proof's context is cancelled — a dial's timeout, not
// only shutdown — while a legacy write it must cover is stopped between its
// admission and its write. Cancelling the connection attempt does not lift
// the obligation to purge: the purge runs once that write is done.
func TestACancelledV2ProofStillPurgesAfterTheLegacyWriteItWaitedFor(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	admitted := make(chan struct{})
	resume := make(chan struct{})
	held := false
	svc.routingInputAdmittedHook = func(sender routingSender, frameType string) {
		if held || frameType != "announce_routes" {
			return
		}
		held = true
		close(admitted)
		<-resume
	}
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(idTargetX))
	}()
	<-admitted

	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatalf("pin: %v", err)
	}
	proof := sessionv2test.Proof(t, peer)
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() { cancelled <- svc.onIdentityProvenV2(ctx, proof) }()
	cancel()
	select {
	case err := <-cancelled:
		// The attempt must not be established: a v2 connection registered
		// before the owed purge would make that purge stand down.
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled v2 proof with the purge still owed returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled v2 proof did not return while the legacy write stayed stopped")
	}

	// X tries again while the write is still stopped: this proof waits, and
	// wakes only once the owed purge is done.
	retried := make(chan error, 1)
	go func() { retried <- svc.onIdentityProvenV2(context.Background(), proof) }()
	select {
	case err := <-retried:
		t.Fatalf("the retried proof completed (%v) while a legacy write it must cover was still pending", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(resume)
	<-handlerDone
	if routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Fatal("a cancelled v2 proof gave up the purge: the legacy write it waited for landed after it and survived")
	}
	select {
	case err := <-retried:
		if err != nil {
			t.Fatalf("the retried proof failed after the legacy write finished: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retried proof never completed after the legacy write finished")
	}
}

// The writers' bookkeeping behind a cancelled proof: a purge handed to the
// in-flight writers is run by the last of them, and while it runs it still
// counts as in flight — a proof waiting meanwhile is woken only after it.
func TestAnOwedPurgeIsRunByTheLastWriterBeforeWaitersWake(t *testing.T) {
	var m unprovenRoutingWriters
	x := domain.PeerIdentity{0xc1}

	if !m.owePurge(x) {
		t.Fatal("a purge was owed with nothing in flight: nobody would ever run it")
	}

	m.enter(x)
	m.enter(x)
	if m.owePurge(x) {
		t.Fatal("writers in flight, yet the proof was told to purge now")
	}
	woke := make(chan error, 1)
	go func() { woke <- m.waitDrained(context.Background(), x) }()

	if m.leave(x) {
		t.Fatal("a writer that was not the last was handed the purge")
	}
	if !m.leave(x) {
		t.Fatal("the last writer was not handed the owed purge")
	}
	select {
	case <-woke:
		t.Fatal("a waiting proof woke before the owed purge ran")
	case <-time.After(50 * time.Millisecond):
	}
	if m.leave(x) {
		t.Fatal("the purge was owed twice")
	}
	select {
	case err := <-woke:
		if err != nil {
			t.Fatalf("waitDrained: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting proof never woke after the owed purge")
	}
}

// P2: X's mark must survive any number of other legacy writers before X
// proves itself; and once the marks are full, a writer whose mark could not be
// recorded must still be purged at its proof.
func TestAMarkIsNotLostToOtherWritersBeforeTheProof(t *testing.T) {
	t.Run("marked before the store filled", func(t *testing.T) {
		svc, peer, impostor := unpinnedRoutingNode(t)
		svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(idTargetX))
		if !routeLearnedVia(svc, idTargetX, peer.Identity) {
			t.Fatal("precondition: the legacy session wrote a route via X")
		}
		now := time.Now()
		for i := 0; i < maxUnprovenRoutingMarks; i++ {
			svc.unprovenRouting.note(domain.PeerIdentity{0xb0, byte(i), byte(i >> 8)}, now)
		}
		proveV2(t, svc, peer)
		if routeLearnedVia(svc, idTargetX, peer.Identity) {
			t.Fatal("X's legacy residue survived its proof: its mark was evicted by other writers")
		}
	})

	t.Run("written after the store filled", func(t *testing.T) {
		svc, peer, impostor := unpinnedRoutingNode(t)
		now := time.Now()
		for i := 0; i < maxUnprovenRoutingMarks; i++ {
			svc.unprovenRouting.note(domain.PeerIdentity{0xb1, byte(i), byte(i >> 8)}, now)
		}
		svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(idTargetX))
		if !routeLearnedVia(svc, idTargetX, peer.Identity) {
			t.Fatal("precondition: the legacy session wrote a route via X")
		}
		proveV2(t, svc, peer)
		if routeLearnedVia(svc, idTargetX, peer.Identity) {
			t.Fatal("X's legacy residue survived its proof: a full mark store dropped the obligation to purge")
		}
	})
}

// P3: a relay sent over a legacy session at address A, which a v2 session of
// X then replaces at the same address. The old attempt's timeout belongs to
// the legacy session it went over, not to the v2 session that happens to be at
// A when the timer fires.
func TestAHopTimeoutIsChargedToTheConnectionTheAttemptUsed(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	svc.relayStates = newRelayStateStore()
	x := peer.Identity
	impostor.sendCh = make(chan peerSendItem, 16)
	svc.peerMu.Lock()
	svc.health[impostor.address] = &peerHealth{Connected: true, LastConnectedAt: time.Now()}
	svc.peerMu.Unlock()
	upsertRouteViaForTest(t, svc, idTargetX, x)

	envelope := protocol.Envelope{
		ID: "via-legacy", Topic: "dm", Sender: svc.identity.Address, Recipient: idTargetX.String(),
		Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(), TTLSeconds: 300, Payload: []byte("sealed"),
	}
	if outcome := svc.sendRelayMessage(impostor.address, envelope, time.Now()); !outcome.handled() {
		t.Fatalf("precondition: the relay was handed to the legacy session (%v)", outcome)
	}

	// X proves itself and its v2 session takes the same address.
	genuine := provenOutboundSession(impostor.address, peer)
	genuine.capabilities = routingTwinsCaps
	svc.peerMu.Lock()
	svc.sessions[impostor.address] = genuine
	svc.peerMu.Unlock()
	if err := svc.secureSessions.store.noteProvenInbound(x); err != nil {
		t.Fatalf("pin: %v", err)
	}

	state, ok := svc.relayStates.snapshotForTest("via-legacy")
	if !ok {
		t.Fatal("precondition: the relay state was stored")
	}
	for i := 0; i < routing.BlackHoleThreshold+1; i++ {
		svc.onRelayHopAckTimeout(state)
	}
	if len(svc.routingTable.Lookup(idTargetX)) == 0 {
		t.Fatal("a timeout of an attempt made over the legacy session was charged to X's v2 session at the same address")
	}
}

// P3, the other side: the attempt's own connection IS charged. A timeout of
// an attempt that went over X's v2 session, or over a legacy session of an
// identity nobody proved (legacy rights are kept), cools the route via X
// down. An attempt no connection carried — the frame was only queued locally —
// is charged to nobody: there is no connection it could be attributed to.
func TestAHopTimeoutChargesTheConnectionTheAttemptUsed(t *testing.T) {
	relayOver := func(t *testing.T, svc *Service, address domain.PeerAddress, id string) relayForwardState {
		t.Helper()
		envelope := protocol.Envelope{
			ID: protocol.MessageID(id), Topic: "dm", Sender: svc.identity.Address, Recipient: idTargetX.String(),
			Flag: protocol.MessageFlagImmutable, CreatedAt: time.Now().UTC(), TTLSeconds: 300, Payload: []byte("sealed"),
		}
		if outcome := svc.sendRelayMessage(address, envelope, time.Now()); !outcome.handled() {
			t.Fatalf("precondition: the relay was handed over (%v)", outcome)
		}
		state, ok := svc.relayStates.snapshotForTest(id)
		if !ok {
			t.Fatal("precondition: the relay state was stored")
		}
		return state
	}
	timeOut := func(svc *Service, state relayForwardState) {
		for i := 0; i < routing.BlackHoleThreshold+1; i++ {
			svc.onRelayHopAckTimeout(state)
		}
	}
	install := func(svc *Service, session *peerSession) {
		session.sendCh = make(chan peerSendItem, 16)
		svc.peerMu.Lock()
		svc.sessions[session.address] = session
		svc.health[session.address] = &peerHealth{Connected: true, LastConnectedAt: time.Now()}
		svc.peerMu.Unlock()
	}

	t.Run("over X's v2 session", func(t *testing.T) {
		svc, peer, impostor := unpinnedRoutingNode(t)
		svc.relayStates = newRelayStateStore()
		genuine := provenOutboundSession(impostor.address, peer)
		genuine.capabilities = routingTwinsCaps
		install(svc, genuine)
		if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
			t.Fatalf("pin: %v", err)
		}
		upsertRouteViaForTest(t, svc, idTargetX, peer.Identity)
		timeOut(svc, relayOver(t, svc, genuine.address, "via-v2"))
		if len(svc.routingTable.Lookup(idTargetX)) != 0 {
			t.Fatal("hop failures over X's own v2 session were not charged to the route via X")
		}
	})

	t.Run("over a legacy session of an identity nobody proved", func(t *testing.T) {
		svc, peer, impostor := unpinnedRoutingNode(t)
		svc.relayStates = newRelayStateStore()
		install(svc, impostor)
		upsertRouteViaForTest(t, svc, idTargetX, peer.Identity)
		timeOut(svc, relayOver(t, svc, impostor.address, "via-legacy-only"))
		if len(svc.routingTable.Lookup(idTargetX)) != 0 {
			t.Fatal("hop failures over a legacy-only identity's session were not charged: legacy routing rights were cut")
		}
	})

	t.Run("queued locally, carried by no connection", func(t *testing.T) {
		svc, peer, impostor := unpinnedRoutingNode(t)
		svc.relayStates = newRelayStateStore()
		svc.peerMu.Lock()
		delete(svc.sessions, impostor.address)
		svc.peerMu.Unlock()
		svc.deliveryMu.Lock()
		if svc.pending == nil {
			svc.pending = make(map[domain.PeerAddress][]pendingFrame)
		}
		if svc.pendingKeys == nil {
			svc.pendingKeys = make(map[pendingKey]struct{})
		}
		svc.deliveryMu.Unlock()
		upsertRouteViaForTest(t, svc, idTargetX, peer.Identity)
		state := relayOver(t, svc, impostor.address, "queued-locally")
		if !state.ForwardedVia.identity.IsZero() {
			t.Fatalf("an attempt queued locally was stamped with a connection: %+v", state.ForwardedVia)
		}
		// By the time the timer fires, X's v2 session holds the address.
		genuine := provenOutboundSession(impostor.address, peer)
		genuine.capabilities = routingTwinsCaps
		install(svc, genuine)
		if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
			t.Fatalf("pin: %v", err)
		}
		timeOut(svc, state)
		if len(svc.routingTable.Lookup(idTargetX)) == 0 {
			t.Fatal("an attempt no connection carried was charged to X's v2 session, which holds the address now")
		}
	})
}

// P3 for the ack: an ack is the connection's it ARRIVED on. A relay_hop_ack
// that came over a legacy connection naming X — or over a connection naming
// nobody — does not suppress the timer of an attempt now held by X's v2
// session at the same address; the same ack over that v2 session does.
func TestAHopAckIsAttributedToTheConnectionItArrivedOn(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	svc.relayStates = newRelayStateStore()
	genuine := provenOutboundSession(impostor.address, peer)
	genuine.capabilities = routingTwinsCaps
	svc.peerMu.Lock()
	svc.sessions[genuine.address] = genuine
	svc.peerMu.Unlock()
	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatalf("pin: %v", err)
	}
	svc.relayStates.store(&relayForwardState{
		MessageID:            "ack-attribution",
		ForwardedTo:          genuine.address,
		ForwardedVia:         sessionRoutingSender(genuine),
		Recipient:            idTargetX,
		HopAckRemainingTicks: 5,
	})
	ack := protocol.Frame{Type: "relay_hop_ack", ID: "ack-attribution", Status: "forwarded"}

	svc.handleRelayHopAck(genuine.address, sessionRoutingSender(impostor), ack)
	if observedHopAck(t, svc, "ack-attribution") {
		t.Fatal("an ack that arrived over a legacy connection naming X was taken as X's because X's v2 session holds the address now")
	}
	svc.handleRelayHopAck(genuine.address, routingSender{}, ack)
	if observedHopAck(t, svc, "ack-attribution") {
		t.Fatal("an ack over a connection that names nobody was credited to X's v2 session holding the address")
	}
	svc.handleRelayHopAck(genuine.address, sessionRoutingSender(genuine), ack)
	if !observedHopAck(t, svc, "ack-attribution") {
		t.Fatal("an ack over X's own v2 session did not suppress the timer")
	}
}

// The close of a legacy session naming X writes routing state in X's name —
// the withdrawal of what was learned through it, with its tombstones, and the
// flap history — so it leaves the same mark a legacy routing frame does: X's
// first proof purges that residue, and X's own announcement is accepted.
func TestALegacySessionCloseObligesThePurgeAtTheProof(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	x := peer.Identity
	upsertRouteViaForTest(t, svc, idTargetX, x)
	svc.peerMu.Lock()
	svc.identitySessions[x] = 1
	svc.identityRelaySessions[x] = 1
	delete(svc.sessions, impostor.address)
	svc.peerMu.Unlock()

	svc.onPeerSessionClosedWithAttribution(x, sessionRoutingSender(impostor).penalty, impostor.capabilities, sessionClosePeerInitiated, nil)
	proveV2(t, svc, peer)

	genuine := provenOutboundSession(legacyVictimAddress, peer)
	genuine.capabilities = routingTwinsCaps
	svc.dispatchPeerSessionFrame(genuine.address, genuine, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: idTargetX.String(), Hops: 1, SeqNo: 1},
		},
	})
	if !routeLearnedVia(svc, idTargetX, x) {
		t.Fatal("X's own announcement was refused by what the legacy session's close left in X's name: the close left no mark")
	}
}

// snapshotForTest returns a copy of one relay state, as the TTL ticker hands
// it to onRelayHopAckTimeout.
func (rs *relayStateStore) snapshotForTest(messageID string) (relayForwardState, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return relayForwardState{}, false
	}
	return *state, true
}
