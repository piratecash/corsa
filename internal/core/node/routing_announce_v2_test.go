package node

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestHandleRequestResync_MarksInvalidAndTriggersUpdate verifies the v2
// receive path's "force me to start over" contract: a request_resync frame
// from the peer must (a) flip the per-peer announce state to NeedsFullResync
// so the next cycle takes the legacy first-sync path, and (b) wake the
// AnnounceLoop via TriggerUpdate so the peer does not have to wait for the
// next periodic tick. Both effects are observable without running the loop —
// the registry surfaces NeedsFullResync via View(), and PendingTrigger()
// inspects the trigger channel without consuming it.
func TestHandleRequestResync_MarksInvalidAndTriggersUpdate(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Establish per-peer state for idPeerB. MarkInvalid is a no-op when
	// the state record does not exist — the production hook MarkReconnected
	// always creates the record before request_resync can race in.
	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})

	// MarkReconnected leaves NeedsFullResync==true. Clear it via a recorded
	// success so the assertion below proves handleRequestResync re-set it,
	// not that the flag was already left over from MarkReconnected.
	state := registry.Get(domain.PeerIdentity(idPeerB))
	if state == nil {
		t.Fatalf("per-peer state must exist after MarkReconnected")
	}
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, time.Now())
	if state.View().NeedsFullResync {
		t.Fatalf("precondition: NeedsFullResync must be cleared before handler call")
	}
	if svc.announceLoop.PendingTrigger() {
		t.Fatalf("precondition: trigger channel must be empty before handler call")
	}

	svc.handleRequestResync(domain.PeerIdentity(idPeerB))

	if !state.View().NeedsFullResync {
		t.Fatalf("handleRequestResync must MarkInvalid (NeedsFullResync=true)")
	}
	if !svc.announceLoop.PendingTrigger() {
		t.Fatalf("handleRequestResync must call TriggerUpdate (PendingTrigger=true)")
	}
}

// TestHandleRequestResync_MalformedSenderIsRejected pins the input-validation
// guard: an invalid identity (anything that fails identity.IsValidAddress)
// must NOT touch state or trigger an announce cycle. Without this guard a
// hostile peer could send request_resync frames with arbitrary senderIdentity
// values to force unrelated peers' state into NeedsFullResync.
func TestHandleRequestResync_MalformedSenderIsRejected(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB), nil)
	state := registry.Get(domain.PeerIdentity(idPeerB))
	if state == nil {
		t.Fatalf("per-peer state must exist")
	}
	// MarkReconnected leaves NeedsFullResync==true; record a successful full
	// sync so the post-condition can prove the malformed handler did not
	// flip it.
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, time.Now())
	if state.View().NeedsFullResync {
		t.Fatalf("precondition: NeedsFullResync must be false after RecordFullSyncSuccess")
	}

	// Drain any pending trigger from MarkReconnected hooks (none expected at
	// the package level, but the loop's trigger channel is shared state).
	svc.announceLoop.PendingTrigger()

	svc.handleRequestResync(domain.PeerIdentity("not-a-valid-hex-identity"))

	if state.View().NeedsFullResync {
		t.Fatalf("malformed sender must NOT mutate per-peer state")
	}
	if svc.announceLoop.PendingTrigger() {
		t.Fatalf("malformed sender must NOT call TriggerUpdate")
	}
}

// TestHandleRoutesUpdate_BeforeBaseline_EmitsRequestResync covers the v2
// receive gate: a routes_update arriving before any legacy announce_routes
// in the current session means the local table is desynced and the receiver
// must reply with a request_resync wire frame asking the peer to re-deliver
// a full baseline. The delta MUST NOT be applied — applying it would
// silently corrupt the per-origin SeqNo space whose state belongs to a
// different (or never-existed) session.
func TestHandleRoutesUpdate_BeforeBaseline_EmitsRequestResync(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	// Establish state without baseline: MarkReconnected leaves
	// HasReceivedBaseline==false on a fresh record.
	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})

	// Wire a session keyed by the sender address so SendRequestResync can
	// route the reply through enqueuePeerFrame. health.Connected==true is
	// required by activePeerSession.
	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		senderAddr: {Connected: true},
	}
	svc.peerMu.Unlock()

	frame := protocol.Frame{
		Type: "routes_update",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}

	svc.handleRoutesUpdate(domain.PeerIdentity(idPeerB), senderAddr, frame)

	// Delta must NOT have been applied — table stays empty.
	if got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)); len(got) > 0 {
		t.Fatalf("baseline gate failed: routes_update applied without baseline (entries=%d)", len(got))
	}

	// request_resync wire frame must be on the sender's send channel.
	select {
	case got := <-sendCh:
		if got.Type != "request_resync" {
			t.Fatalf("expected request_resync wire frame, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("baseline gate did not emit request_resync within 100ms")
	}
}

// TestHandleRoutesUpdate_AfterBaseline_AppliesEntries pins the post-baseline
// path: once handleAnnounceRoutes has flipped HasReceivedBaseline, a
// subsequent routes_update is applied through the same per-entry rules as
// the legacy frame. Entries land in the routing table with hops += 1
// (receiver convention), and no request_resync is emitted.
func TestHandleRoutesUpdate_AfterBaseline_AppliesEntries(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
	// Simulate the baseline arrival explicitly — production sets this via
	// handleAnnounceRoutes → applyAnnounceEntries(legacy) → MarkBaselineReceived.
	registry.GetOrCreate(domain.PeerIdentity(idPeerB)).MarkBaselineReceived()

	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		senderAddr: {Connected: true},
	}
	svc.peerMu.Unlock()

	frame := protocol.Frame{
		Type: "routes_update",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}

	svc.handleRoutesUpdate(domain.PeerIdentity(idPeerB), senderAddr, frame)

	// Route must be in the table at hops=2 (wire 1 + receiver +1).
	got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	if len(got) == 0 {
		t.Fatalf("post-baseline routes_update must apply: target not learned")
	}
	if got[0].Hops != 2 {
		t.Fatalf("receiver convention violated: expected hops=2, got %d", got[0].Hops)
	}

	// Send channel must be empty — no request_resync reply.
	select {
	case f := <-sendCh:
		t.Fatalf("post-baseline path must not emit any wire frame, got %q", f.Type)
	default:
	}
}

// TestHandleAnnounceRoutes_LegacyEmptyFrame_StillFlipsBaseline pins an
// edge case in the empty-baseline semantics: the protocol is additive, so a
// legacy first-sync frame with zero entries is a valid baseline (peer has
// nothing visible to us through the split horizon). The receiver must still
// flip HasReceivedBaseline so subsequent routes_update deltas are accepted.
// Without this, an empty-table peer would forever be stuck behind the v2
// receive gate.
func TestHandleAnnounceRoutes_LegacyEmptyFrame_StillFlipsBaseline(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB), nil)

	state := registry.Get(domain.PeerIdentity(idPeerB))
	if state == nil {
		t.Fatalf("per-peer state must exist after MarkReconnected")
	}
	if state.HasReceivedBaseline() {
		t.Fatalf("precondition: baseline must be false before legacy frame")
	}

	frame := protocol.Frame{
		Type:           "announce_routes",
		AnnounceRoutes: nil,
	}
	svc.handleAnnounceRoutes(domain.PeerIdentity(idPeerB), frame)

	if !state.HasReceivedBaseline() {
		t.Fatalf("legacy announce_routes (even empty) must flip baseline to true")
	}
}

// TestHandleRoutesUpdate_BeforeBaseline_NoSenderAddress_DropsSilently pins
// the unit-test escape hatch: when senderAddress is empty (no return path
// available — e.g. when the caller plumbs the handler from a test fixture
// without a session), the handler must NOT try to send request_resync.
// Production callers always pass a non-empty address, but the receive path
// must remain safe under the absence — the alternative is a nil-deref or
// a write to a non-existent session, both worse than a silent drop.
func TestHandleRoutesUpdate_BeforeBaseline_NoSenderAddress_DropsSilently(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(domain.PeerIdentity(idPeerB), nil)

	frame := protocol.Frame{
		Type: "routes_update",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}

	// Empty senderAddress — the handler must short-circuit without crashing.
	svc.handleRoutesUpdate(domain.PeerIdentity(idPeerB), domain.PeerAddress(""), frame)

	// Delta still must not have been applied — the gate is the dominant
	// decision, not the address.
	if got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)); len(got) > 0 {
		t.Fatalf("baseline gate must drop the delta even when senderAddress is empty")
	}
}

// TestOnPeerSessionEstablished_OverlappingReconnect_RelayOnlyAdditionalSessionDoesNotPoisonCaps
// pins one half of the contract that the firstRelay=false branch in
// onPeerSessionEstablished does NOT mutate AnnouncePeerState.capabilities.
// Persistent caps are reconciled at cycle start in
// AnnounceLoop.announceToAllPeers (synced to the AnnounceTarget caps
// routingCapablePeers actually picked); a lifecycle-hook variant that
// updated caps from session-establishment events alone could not reliably
// match that selection rule and was the source of the relay-only
// poisoning bug:
//
//   - Session 1 establishes v1+v2+relay → MarkReconnected stores caps.
//   - Session 2 (relay-only, no mesh_routing_v1) arrives and triggers a
//     hook-only refresh. routingCapablePeers cannot pick session 2 (lacks
//     v1), still picks session 1 as AnnounceTarget. classifyDeltaMode sees
//     state caps without v2, target caps with v2 → permanent divergence.
//   - Closing session 2 does not restore the snapshot — no hook recomputes
//     caps from active sessions, so the peer is stuck in legacy/forced-full.
//
// This test makes the negative invariant explicit: a relay-only additional
// session for the same identity must not change persistent caps. The
// fact that no caps refresh happens here is exactly what we want — the
// next announce cycle will sync persistent caps to whichever session
// routingCapablePeers actually selects.
func TestOnPeerSessionEstablished_OverlappingReconnect_RelayOnlyAdditionalSessionDoesNotPoisonCaps(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Session 1: full routing-capable v1+v2 — sets the persistent caps.
	caps1 := []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}
	svc.onPeerSessionEstablished(domain.PeerIdentity(idPeerB), caps1)

	registry := svc.announceLoop.StateRegistry()
	state := registry.Get(domain.PeerIdentity(idPeerB))
	if state == nil {
		t.Fatalf("first session must materialise per-peer announce state")
	}

	// Session 2: relay-capable but NOT routing-capable (no mesh_routing_v1).
	// firstRelay=false (relay session count is 2), so we hit the
	// non-firstRelay branch, but the UpdateCapabilities gate must skip
	// because session 2 cannot serve as an announce target.
	caps2 := []domain.Capability{domain.CapMeshRelayV1}
	svc.onPeerSessionEstablished(domain.PeerIdentity(idPeerB), caps2)

	view := state.View()
	wantCaps := map[routing.PeerCapability]bool{
		domain.CapMeshRelayV1:   true,
		domain.CapMeshRoutingV1: true,
		domain.CapMeshRoutingV2: true,
	}
	for _, c := range view.CapabilitiesSnapshot {
		if !wantCaps[c] {
			t.Fatalf("relay-only additional session leaked into snapshot: unexpected cap %s", c)
		}
		delete(wantCaps, c)
	}
	if len(wantCaps) > 0 {
		t.Fatalf("relay-only additional session must NOT overwrite routing-capable caps; missing %v", wantCaps)
	}
}

// TestOnPeerSessionEstablished_OverlappingReconnect_SkipsRefreshWithoutRelayCap
// pins the symmetric negative: an additional session without mesh_relay_v1
// returns from the early "no_relay_cap_skip_direct_route" branch before the
// firstRelay gate, so persistent caps stay at the prior routing-capable
// session's snapshot. This is intentional and matches the relay-only
// negative test above: caps are reconciled at cycle start, not in
// lifecycle hooks. A non-relay session cannot carry data-plane traffic
// and so its capability set is not authoritative for the announce-plane
// classifier in any path.
func TestOnPeerSessionEstablished_OverlappingReconnect_SkipsRefreshWithoutRelayCap(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Session 1: relay-capable v1+v2.
	caps1 := []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}
	svc.onPeerSessionEstablished(domain.PeerIdentity(idPeerB), caps1)

	registry := svc.announceLoop.StateRegistry()
	state := registry.Get(domain.PeerIdentity(idPeerB))
	if state == nil {
		t.Fatalf("first session must materialise per-peer announce state")
	}

	// Session 2: legacy peer (no relay cap) — must not overwrite caps.
	svc.onPeerSessionEstablished(domain.PeerIdentity(idPeerB), nil)

	view := state.View()
	wantCaps := map[routing.PeerCapability]bool{
		domain.CapMeshRelayV1:   true,
		domain.CapMeshRoutingV1: true,
		domain.CapMeshRoutingV2: true,
	}
	for _, c := range view.CapabilitiesSnapshot {
		if !wantCaps[c] {
			t.Fatalf("unexpected cap in snapshot: %s", c)
		}
		delete(wantCaps, c)
	}
	if len(wantCaps) > 0 {
		t.Fatalf("non-relay second session must NOT clear caps; missing %v", wantCaps)
	}
}

// TestSendRoutesUpdate_NoV2OnLiveSession_SkipsWire pins the send-time
// capability re-check that closes the race between the cycle-start
// AnnounceTarget snapshot and the actual write. The mode classifier in
// AnnounceLoop runs against caps captured at cycle start; by the time
// the per-peer goroutine reaches SendRoutesUpdate the underlying session
// can have closed and been replaced by a session at the same address
// that did NOT negotiate mesh_routing_v2. Without the send-time check,
// routes_update bytes would land on a non-v2 session — the receiver
// rejects the unknown frame type and may ban the sender. SendRoutesUpdate
// MUST detect the missing v2 cap on the live session and return false
// without emitting any wire frame; the caller's per-peer cache then
// stays untouched so the next cycle re-classifies on the current session.
func TestSendRoutesUpdate_NoV2OnLiveSession_SkipsWire(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Live session at the announce address has only v1+relay — no v2.
	// This simulates the race outcome: a v2-capable session was selected
	// at cycle start, then closed and replaced by a v1-only session at
	// the same address before the per-peer goroutine called SendRoutesUpdate.
	addr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		addr: {Connected: true},
	}
	svc.peerMu.Unlock()

	delta := []routing.AnnounceEntry{
		{Identity: domain.PeerIdentity(idTargetX), Origin: domain.PeerIdentity(idOriginC), Hops: 1, SeqNo: 1},
	}

	if got := svc.SendRoutesUpdate(context.Background(), addr, delta); got {
		t.Fatalf("SendRoutesUpdate must return false when live session lacks mesh_routing_v2")
	}

	select {
	case got := <-sendCh:
		t.Fatalf("SendRoutesUpdate must NOT enqueue a wire frame on a non-v2 session, got %q", got.Type)
	case <-time.After(50 * time.Millisecond):
		// success — nothing on the wire.
	}
}

// TestSendRoutesUpdate_V2OnLiveSession_SendsFrame is the positive
// counterpart: when the live session has mesh_routing_v2, the send-time
// check passes and the routes_update frame is enqueued through the
// normal dispatch path. Without this assertion, a regression that
// over-tightens the gate (e.g. requires v2 + something else, or fails
// the inbound branch) would silently break the v2 happy path.
func TestSendRoutesUpdate_V2OnLiveSession_SendsFrame(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	addr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		addr: {Connected: true},
	}
	svc.peerMu.Unlock()

	delta := []routing.AnnounceEntry{
		{Identity: domain.PeerIdentity(idTargetX), Origin: domain.PeerIdentity(idOriginC), Hops: 1, SeqNo: 1},
	}

	if got := svc.SendRoutesUpdate(context.Background(), addr, delta); !got {
		t.Fatalf("SendRoutesUpdate must return true when live session has mesh_routing_v2")
	}

	select {
	case got := <-sendCh:
		if got.Type != "routes_update" {
			t.Fatalf("expected routes_update frame, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("v2 wire frame did not arrive on send channel")
	}
}

// TestSendRoutesUpdate_SessionGone_ReturnsFalse pins the session-vanished
// branch: if the address resolves to no session at all (closed, never
// existed, or torn down between snapshot and call), SendRoutesUpdate
// returns false without panicking and without writing anything.
// dispatchAnnouncePlaneFrameWithCaps captures the transport handle under
// peerMu RLock and reports ok=false when no live session matches the
// address — the same outcome as missing any required cap. Both are
// protocol-safe: the v2 wire frame is never enqueued.
func TestSendRoutesUpdate_SessionGone_ReturnsFalse(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	delta := []routing.AnnounceEntry{
		{Identity: domain.PeerIdentity(idTargetX), Origin: domain.PeerIdentity(idOriginC), Hops: 1, SeqNo: 1},
	}

	// No session at this address — the check resolves to "no v2 cap" and
	// returns false rather than dispatching to enqueuePeerFrame.
	if got := svc.SendRoutesUpdate(context.Background(), domain.PeerAddress("ghost-addr"), delta); got {
		t.Fatalf("SendRoutesUpdate must return false when no session exists at the address")
	}
}

// TestSendRoutesUpdate_NarrowReplacementSession_SkipsWire pins the wider
// send-time gate: routingCapablePeers selects targets requiring
// mesh_routing_v1 AND mesh_relay_v1; the v2 path adds mesh_routing_v2.
// A churned replacement session that holds v2 but is missing v1 or relay
// would never have been picked as an AnnounceTarget, and the receive-side
// dispatcher rejects the frame against the same predicate. Re-checking
// only mesh_routing_v2 at send time would let routes_update bytes land
// on such a narrow session anyway. The send-time check therefore mirrors
// the entire target/receive gate (v1+v2+relay), not just the v2 cap.
func TestSendRoutesUpdate_NarrowReplacementSession_SkipsWire(t *testing.T) {
	cases := []struct {
		name string
		caps []domain.Capability
	}{
		{name: "v2_only_no_v1_no_relay", caps: []domain.Capability{domain.CapMeshRoutingV2}},
		{name: "v1_v2_no_relay", caps: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}},
		{name: "v2_relay_no_v1", caps: []domain.Capability{domain.CapMeshRoutingV2, domain.CapMeshRelayV1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestServiceWithRouting(t, idNodeA)
			addr := domain.PeerAddress("addr-peerB")
			sendCh := make(chan protocol.Frame, 4)
			svc.peerMu.Lock()
			svc.sessions[addr] = &peerSession{
				address:      addr,
				peerIdentity: domain.PeerIdentity(idPeerB),
				capabilities: tc.caps,
				sendCh:       sendCh,
			}
			svc.health = map[domain.PeerAddress]*peerHealth{
				addr: {Connected: true},
			}
			svc.peerMu.Unlock()

			delta := []routing.AnnounceEntry{
				{Identity: domain.PeerIdentity(idTargetX), Origin: domain.PeerIdentity(idOriginC), Hops: 1, SeqNo: 1},
			}

			if got := svc.SendRoutesUpdate(context.Background(), addr, delta); got {
				t.Fatalf("SendRoutesUpdate must return false for narrow replacement session caps=%v", tc.caps)
			}
			select {
			case got := <-sendCh:
				t.Fatalf("SendRoutesUpdate must NOT enqueue a wire frame for caps=%v, got %q", tc.caps, got.Type)
			case <-time.After(50 * time.Millisecond):
				// success — nothing on the wire.
			}
		})
	}
}

// TestSendRequestResync_V2OnlyOnLiveSession_SendsFrame pins the
// alignment between the send-time gate and the receive dispatcher.
// Both inbound and session receive dispatchers gate request_resync on
// mesh_routing_v2 alone — it is a v2-only control frame with no payload,
// signalling "clear my announce state" to the peer. The send-time gate
// must therefore accept any session that holds v2, even if it lacks
// mesh_routing_v1 and mesh_relay_v1 (e.g. a churn replacement that
// dropped one of the routing-target caps). Widening the send gate to
// the full v1+v2+relay routing-target predicate would have the sender
// skip a recovery control frame that the receiver would still accept,
// leaving v2-only sessions permanently desynced after a transient cap
// churn.
func TestSendRequestResync_V2OnlyOnLiveSession_SendsFrame(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	addr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		// Only v2 — no v1, no relay. The receive dispatcher accepts
		// request_resync against this gate, so the send gate must too.
		capabilities: []domain.Capability{domain.CapMeshRoutingV2},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		addr: {Connected: true},
	}
	svc.peerMu.Unlock()

	if got := svc.SendRequestResync(context.Background(), addr); !got {
		t.Fatalf("SendRequestResync must return true on a v2-only session — its receive-dispatcher gate is v2-only")
	}

	select {
	case got := <-sendCh:
		if got.Type != "request_resync" {
			t.Fatalf("expected request_resync wire frame, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("v2 control frame did not arrive on send channel")
	}
}

// TestSendRequestResync_NoV2OnLiveSession_SkipsWire mirrors the
// TestSendRoutesUpdate_NoV2OnLiveSession_SkipsWire contract for the
// other v2 control frame. handleRoutesUpdate calls SendRequestResync on
// the receive-baseline-gate path; if the session that just delivered the
// v2 routes_update churned to a non-v2 transport before the reply lands,
// the reply must be dropped rather than placed on a session that the
// receive dispatcher would reject.
func TestSendRequestResync_NoV2OnLiveSession_SkipsWire(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	addr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		addr: {Connected: true},
	}
	svc.peerMu.Unlock()

	if got := svc.SendRequestResync(context.Background(), addr); got {
		t.Fatalf("SendRequestResync must return false when live session lacks mesh_routing_v2")
	}

	select {
	case got := <-sendCh:
		t.Fatalf("SendRequestResync must NOT enqueue a wire frame on a non-v2 session, got %q", got.Type)
	case <-time.After(50 * time.Millisecond):
		// success — nothing on the wire.
	}
}
