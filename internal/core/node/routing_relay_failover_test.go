package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_relay_failover_test.go covers Phase 3 PR 12.3 — the
// Service.tryFailoverRelay path that fires from
// onRelayHopAckTimeout when a forwarded relay_message's hop-ack
// budget elapses without a matching ack.
//
// Test ownership: helper-level coverage of recordFailoverRetry /
// retryAttemptCountFor lives in relay_hop_ack_budget_test.go; this
// file owns the Service-level integration scenarios (alternative-
// uplink selection, MaxFailoverRetries gate, split-horizon, abandoned
// skip, FrameLine-disabled skip).
//
// Fixed-clock convention: tests use newTestServiceWithRoutingAndHealth
// which leaves routing.Table.clock=time.Now. upsertClaim is NOT used
// here (it's a routing-package helper); we go through Table.UpdateRoute
// directly with ExpiresAt anchored on time.Now to stay aligned with
// the table clock.

// makeRelayFrameLine builds the serialised wire line for a synthetic
// relay_message. Tests use it to populate relayForwardState.FrameLine
// the same way the production sites (sendRelayMessage, etc.) do.
func makeRelayFrameLine(t *testing.T, messageID string, recipient domain.PeerIdentity) string {
	t.Helper()
	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        messageID,
		Recipient: recipient.String(),
		HopCount:  1,
		MaxHops:   defaultMaxHops,
	}
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalFrameLine: %v", err)
	}
	return line
}

// installRelayCapableSession registers a session whose peerIdentity
// matches, with mesh_relay_v1 + mesh_routing_v1 capabilities and a
// buffered sendCh so sendFrameToAddress can enqueue without blocking.
// A peerHealth entry with Connected=true is required for the address
// resolver to count the session as reachable.
func installRelayCapableSession(t *testing.T, svc *Service, address domain.PeerAddress, peerID domain.PeerIdentity) chan protocol.Frame {
	t.Helper()
	sendCh := make(chan protocol.Frame, 16)
	svc.sessions[address] = &peerSession{
		peerIdentity: peerID,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
		sendCh:       sendCh,
	}
	svc.peerMu.Lock()
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	svc.health[address] = &peerHealth{Address: address, Connected: true}
	svc.peerMu.Unlock()
	return sendCh
}

// TestFailoverRelay_FailoverOnHopAckTimeout — overview §9.5 release-
// blocking scenario. Two uplinks (A, B) reach the target; the
// original forward went through A and timed out. tryFailoverRelay
// must re-send the same MessageID through B (the second-best
// uplink), advance RetryAttempt to 1, re-arm the hop-ack budget on
// the relayForwardState entry, and surface the retry via the
// sendCh on B's session.
func TestFailoverRelay_FailoverOnHopAckTimeout(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Two uplinks for idTargetX — A (1 hop, will fail) and B (2 hops).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(A): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(B): %v", err)
	}

	// Capable sessions for both addresses. A is the one that
	// originally received the forward and timed out; B receives the
	// retry.
	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChB := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)

	// Seed a relayForwardState as if sendRelayMessage already
	// forwarded through A.
	line := makeRelayFrameLine(t, "msg-failover", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-failover",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopCount:             1,
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            line,
	})

	// Snapshot the state the ticker would have captured at fire time
	// (HopAckObserved=true is set by tickHopAckBudgets before the
	// callback; we mirror that here to stay faithful to the
	// production call chain).
	snap := relayForwardState{
		MessageID:            "msg-failover",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopCount:             1,
		RemainingTTL:         60,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		FrameLine:            line,
	}

	svc.onRelayHopAckTimeout(snap)

	// The retry frame should have landed in B's send channel.
	select {
	case frame := <-sendChB:
		if frame.ID != "msg-failover" {
			t.Fatalf("retried frame ID = %q, want msg-failover", frame.ID)
		}
		if frame.Recipient != idTargetX.String() {
			t.Fatalf("retried frame Recipient = %q, want %q", frame.Recipient, idTargetX)
		}
	default:
		t.Fatal("no retry frame landed on B's send channel")
	}

	// State should now point at B with RetryAttempt=1, fresh budget,
	// observed=false, and A in AbandonedForwardedTo.
	svc.relayStates.mu.Lock()
	st := svc.relayStates.states["msg-failover"]
	svc.relayStates.mu.Unlock()
	if st.ForwardedTo != "addr-C" {
		t.Fatalf("ForwardedTo = %q, want addr-C", st.ForwardedTo)
	}
	if st.RetryAttempt != 1 {
		t.Fatalf("RetryAttempt = %d, want 1", st.RetryAttempt)
	}
	if st.HopAckRemainingTicks != defaultHopAckBudgetSeconds {
		t.Fatalf("HopAckRemainingTicks = %d, want %d (re-armed)", st.HopAckRemainingTicks, defaultHopAckBudgetSeconds)
	}
	if st.HopAckObserved {
		t.Fatal("HopAckObserved must be false after failover")
	}
	if len(st.AbandonedForwardedTo) != 1 || st.AbandonedForwardedTo[0] != "addr-A" {
		t.Fatalf("AbandonedForwardedTo = %v, want [addr-A]", st.AbandonedForwardedTo)
	}
}

// TestFailoverRelay_SkipsQuarantinedTransitNextHop pins the
// quarantine gate inside tryFailoverRelay. Without the gate the
// failover path would pick the highest-ranked alternative (D, 2
// hops) even though D is route-quarantined, undoing the "transit
// through P is skipped" contract that the normal forwarding and
// TableRouter paths uphold.
//
// Setup: A (1 hop, just failed), D (2 hops, quarantined),
// C (3 hops, clean). Without the gate D would be the chosen
// retry; with the gate D is skipped and C receives the retry.
func TestFailoverRelay_SkipsQuarantinedTransitNextHop(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(A): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerD,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(D): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(C): %v", err)
	}

	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChD := installRelayCapableSession(t, svc, domain.PeerAddress("addr-D"), idPeerD)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)

	// Arm route quarantine on D for a transit-blocking reason —
	// failover MUST skip it as a transit next-hop (target identity is
	// X, not D, so hops > 1 and the gate engages). disconnect_storm is
	// what blocks transit; chatty_routes deliberately would not (see
	// isPeerTransitQuarantinedLocked).
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(idPeerD, quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	line := makeRelayFrameLine(t, "msg-failover-q", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-failover-q",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopCount:             1,
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		FrameLine:            line,
	})

	snap := relayForwardState{
		MessageID:            "msg-failover-q",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopCount:             1,
		RemainingTTL:         60,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		FrameLine:            line,
	}

	svc.onRelayHopAckTimeout(snap)

	// D must NOT receive the retry (quarantined transit).
	if len(sendChD) != 0 {
		t.Fatal("quarantined transit peer D received the failover retry; gate is missing or broken")
	}
	// C MUST receive the retry (next clean candidate).
	select {
	case frame := <-sendChC:
		if frame.ID != "msg-failover-q" {
			t.Fatalf("retried frame ID = %q, want msg-failover-q", frame.ID)
		}
	default:
		t.Fatal("no retry frame landed on C's send channel — failover did not consider the clean alternative")
	}

	// State must point at C.
	svc.relayStates.mu.Lock()
	st := svc.relayStates.states["msg-failover-q"]
	svc.relayStates.mu.Unlock()
	if st.ForwardedTo != "addr-C" {
		t.Fatalf("ForwardedTo = %q, want addr-C (D should have been skipped by quarantine gate)", st.ForwardedTo)
	}
}

// TestFailoverRelay_RespectsMaxRetries — once RetryAttempt reaches
// MaxFailoverRetries the retry path bails out without firing
// another send, even if alternative uplinks exist. Phase 3 §4.9
// decision #7 sets MaxFailoverRetries=1.
func TestFailoverRelay_RespectsMaxRetries(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Three uplinks — but we have already retried once so the third
	// attempt must NOT fire.
	for i, id := range []string{idPeerA.String(), idPeerC.String(), idPeerD.String()} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: idTargetX, Origin: idTargetX, NextHop: domain.PeerIdentityFromWire(id),
			Hops: i + 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("UpdateRoute(%s): %v", id, err)
		}
	}

	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)
	sendChD := installRelayCapableSession(t, svc, domain.PeerAddress("addr-D"), idPeerD)

	line := makeRelayFrameLine(t, "msg-maxed", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-maxed",
		ForwardedTo:          domain.PeerAddress("addr-C"),
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		Recipient:            idTargetX,
		HopCount:             1,
		RemainingTTL:         60,
		RetryAttempt:         MaxFailoverRetries, // already at the cap
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		FrameLine:            line,
	})

	snap := relayForwardState{
		MessageID:            "msg-maxed",
		ForwardedTo:          domain.PeerAddress("addr-C"),
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		Recipient:            idTargetX,
		HopCount:             1,
		RetryAttempt:         MaxFailoverRetries,
		HopAckObserved:       true,
		FrameLine:            line,
	}

	svc.onRelayHopAckTimeout(snap)

	// The MaxFailoverRetries gate stops another FAILOVER RETRY —
	// i.e. recordFailoverRetry must NOT run, so the state's
	// ForwardedTo and RetryAttempt stay put. The P1.2 review fix
	// added a gossip fallback on this terminal branch, so a
	// gossip copy CAN legitimately land on D (idPeerD) now; that
	// is expected and is NOT a failover retry. The discriminator
	// between "failover retry" and "gossip send" is the state
	// bookkeeping below, not the send-channel content.
	//
	// addr-C is the failed uplink and is excluded from the gossip
	// fan-out (relayViaGossip's exclude arg), so its channel must
	// stay empty.
	if len(sendChC) != 0 {
		t.Fatalf("failed uplink C received a frame; it must be excluded from both failover retry and gossip fallback")
	}
	_ = sendChD // gossip fallback may legitimately enqueue here post-P1.2

	svc.relayStates.mu.Lock()
	st := svc.relayStates.states["msg-maxed"]
	svc.relayStates.mu.Unlock()
	if st.RetryAttempt != MaxFailoverRetries {
		t.Fatalf("RetryAttempt = %d, want %d (failover retry must not run at the cap)", st.RetryAttempt, MaxFailoverRetries)
	}
	if st.ForwardedTo != "addr-C" {
		t.Fatalf("ForwardedTo = %q, want addr-C (no failover rotation at the cap; gossip does not mutate state)", st.ForwardedTo)
	}
}

// TestFailoverRelay_NoAlternativeUplink_SkipsRetry — single-uplink
// case. The only route is via the just-failed peer; tryFailoverRelay
// finds nothing to retry through and exits cleanly without bumping
// RetryAttempt or sending anything.
func TestFailoverRelay_NoAlternativeUplink_SkipsRetry(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	sendChA := installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)

	line := makeRelayFrameLine(t, "msg-solo", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-solo",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		RemainingTTL:         60,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		FrameLine:            line,
	})

	snap := relayForwardState{
		MessageID:      "msg-solo",
		ForwardedTo:    domain.PeerAddress("addr-A"),
		Recipient:      idTargetX,
		HopAckObserved: true,
		FrameLine:      line,
	}
	svc.onRelayHopAckTimeout(snap)

	if len(sendChA) != 0 {
		t.Fatalf("retry resent through the failed uplink A; sendChA len = %d", len(sendChA))
	}
	svc.relayStates.mu.Lock()
	st := svc.relayStates.states["msg-solo"]
	svc.relayStates.mu.Unlock()
	if st.RetryAttempt != 0 {
		t.Fatalf("RetryAttempt bumped to %d on no-alternative path", st.RetryAttempt)
	}
}

// TestFailoverRelay_SkipsPreviousHop_SplitHorizon — even if the
// only alternative happens to be the same peer that handed us the
// original relay (PreviousHop), the retry path must respect split
// horizon and bail out.
func TestFailoverRelay_SkipsPreviousHop_SplitHorizon(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Two uplinks: peer-A (failed) and peer-C. peer-C is also the
	// previous-hop incoming sender, so split-horizon must filter it.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(A): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(C): %v", err)
	}
	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)

	line := makeRelayFrameLine(t, "msg-splitH", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-splitH",
		PreviousHop:          domain.PeerAddress("addr-C"), // C handed us the relay
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		FrameLine:            line,
		RemainingTTL:         60,
	})

	snap := relayForwardState{
		MessageID:      "msg-splitH",
		PreviousHop:    domain.PeerAddress("addr-C"),
		ForwardedTo:    domain.PeerAddress("addr-A"),
		Recipient:      idTargetX,
		HopAckObserved: true,
		FrameLine:      line,
	}
	svc.onRelayHopAckTimeout(snap)

	if len(sendChC) != 0 {
		t.Fatal("retry fired through previous-hop sender C; split-horizon broken")
	}
}

// TestFailoverRelay_SkipsAbandonedUplinks — a future failover round
// with MaxFailoverRetries>1 must not pick the same peer twice. The
// abandoned-uplink list captures every prior retry's target so the
// loop skips them by identity (peer address may have multiple
// transport forms but the identity dedup'd via resolvePeerIdentity
// keeps the skip consistent).
func TestFailoverRelay_SkipsAbandonedUplinks(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Three uplinks. The state already abandoned A (initial) and B
	// (previous retry); the current retry attempt is "what's left
	// after the abandoned list".
	for i, id := range []string{idPeerA.String(), idPeerC.String(), idPeerD.String()} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: idTargetX, Origin: idTargetX, NextHop: domain.PeerIdentityFromWire(id),
			Hops: i + 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("UpdateRoute(%s): %v", id, err)
		}
	}
	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)
	sendChD := installRelayCapableSession(t, svc, domain.PeerAddress("addr-D"), idPeerD)

	// Pretend a single retry already happened (the cap is 1 by
	// default but this test wants to assert the abandoned-skip
	// regardless, so we drop the cap to 2 by patching the snapshot
	// to RetryAttempt=0 — see the test on MaxRetries above for the
	// cap test). Here the abandoned list contains A AND C; only D
	// should be selectable.
	line := makeRelayFrameLine(t, "msg-aban", idTargetX)
	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-aban",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		AbandonedForwardedTo: []domain.PeerAddress{"addr-C"},
		Recipient:            idTargetX,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		RemainingTTL:         60,
		FrameLine:            line,
	})

	snap := relayForwardState{
		MessageID:            "msg-aban",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		AbandonedForwardedTo: []domain.PeerAddress{"addr-C"},
		Recipient:            idTargetX,
		HopAckObserved:       true,
		FrameLine:            line,
	}
	svc.onRelayHopAckTimeout(snap)

	// The retry must land on D, skipping the already-tried C and
	// the just-failed A.
	if len(sendChC) != 0 {
		t.Fatal("retry fired through abandoned uplink C")
	}
	select {
	case frame := <-sendChD:
		if frame.ID != "msg-aban" {
			t.Fatalf("retried frame ID = %q, want msg-aban", frame.ID)
		}
	default:
		t.Fatal("no retry frame landed on D's send channel")
	}
}

// TestFailoverRelay_EmptyFrameLine_SkipsRetry — defensive guard:
// a state without a serialised frame (origin-marshal failure path,
// or restored-from-disk entry whose FrameLine was empty) must skip
// the retry quietly. The MarkHopFailure that fired earlier still
// records reputation; the failover path is just disabled for this
// entry.
func TestFailoverRelay_EmptyFrameLine_SkipsRetry(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(A): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(C): %v", err)
	}
	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)

	svc.relayStates.store(&relayForwardState{
		MessageID:            "msg-nopayload",
		ForwardedTo:          domain.PeerAddress("addr-A"),
		Recipient:            idTargetX,
		HopAckRemainingTicks: 0,
		HopAckObserved:       true,
		RemainingTTL:         60,
		FrameLine:            "", // explicit empty
	})

	snap := relayForwardState{
		MessageID:      "msg-nopayload",
		ForwardedTo:    domain.PeerAddress("addr-A"),
		Recipient:      idTargetX,
		HopAckObserved: true,
		FrameLine:      "",
	}
	svc.onRelayHopAckTimeout(snap)

	if len(sendChC) != 0 {
		t.Fatal("retry fired with empty FrameLine; should have been skipped")
	}
	svc.relayStates.mu.Lock()
	st := svc.relayStates.states["msg-nopayload"]
	svc.relayStates.mu.Unlock()
	if st.RetryAttempt != 0 {
		t.Fatalf("RetryAttempt = %d, want 0 (no-FrameLine path)", st.RetryAttempt)
	}
}

// TestFailoverRelay_TTLEvictedBetweenFireAndRetry — race-window
// guard: if TTL eviction (or any other deletion) drops the
// relayForwardState between the ticker capturing the snapshot and
// tryFailoverRelay running, the helper must bail out without
// sending. retryAttemptCountFor returns present=false and the
// short-circuit hits.
func TestFailoverRelay_TTLEvictedBetweenFireAndRetry(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerA,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(A): %v", err)
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(C): %v", err)
	}
	_ = installRelayCapableSession(t, svc, domain.PeerAddress("addr-A"), idPeerA)
	sendChC := installRelayCapableSession(t, svc, domain.PeerAddress("addr-C"), idPeerC)

	// State exists at snapshot time but is gone by the time the
	// callback runs. We do NOT call svc.relayStates.store here.
	snap := relayForwardState{
		MessageID:      "msg-evicted",
		ForwardedTo:    domain.PeerAddress("addr-A"),
		Recipient:      idTargetX,
		HopAckObserved: true,
		FrameLine:      makeRelayFrameLine(t, "msg-evicted", idTargetX),
	}
	svc.onRelayHopAckTimeout(snap)

	if len(sendChC) != 0 {
		t.Fatal("retry fired despite the state being evicted")
	}
}
