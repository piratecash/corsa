package node

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_review_fixes_v4_test.go pins behaviour fixed in PR 11.11
// in response to a fourth Phase 2 review pass.

// --- P2 #1: sendProbe cancels registration when send fails ---

// TestReview_v4_P2_1_SendProbe_CancelsOnSendFail — without a
// usable send path (capability check passes, conn is non-nil for
// the peerSendableConnectionsLocked health gate, but the session's
// sendCh cannot accept the frame), sendFrameToIdentity returns
// false and the probe registry must NOT keep the entry: a later
// timeout would call MarkProbeFailure for a probe the peer never
// received.
//
// Fixture is built inline rather than reusing
// installCapableUplinkForTest because that helper deliberately
// wires a buffered sendCh so the non-blocking enqueue succeeds
// (needed by the TestProbeTick_* tests). Here we want the
// opposite: a session with the capability and a non-nil conn so
// the health/conn-presence gates pass, but an UNBUFFERED sendCh
// with no consumer goroutine so tryEnqueuePeerSessionFrame hits
// the select default and returns false. That drives
// sendFrameToIdentity to the inbound fall-back, which is empty,
// and the call returns false — exactly the post-Register send
// failure the Cancel-on-fail path was added to handle.
func TestReview_v4_P2_1_SendProbe_CancelsOnSendFail(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	addr := domain.PeerAddress("addr-" + string(idPeerB))
	// net.Pipe gives us a non-nil net.Conn without spinning up a
	// real listener; both halves are closed by the test cleanup.
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRouteProbeV1},
		conn:         clientConn,
		// Unbuffered channel with NO consumer — non-blocking send
		// inside tryEnqueuePeerSessionFrame falls through to the
		// default arm and returns false.
		sendCh: make(chan protocol.Frame),
	}
	svc.health = map[domain.PeerAddress]*peerHealth{
		addr: {
			Address:         addr,
			Connected:       true,
			Direction:       peerDirectionOutbound,
			LastConnectedAt: time.Now().UTC(),
			LastPongAt:      time.Now().UTC(),
		},
	}

	// Production path: peer is capable, but the send path cannot
	// enqueue, so sendFrameToIdentity returns false. The fix under
	// test: probeRegistry must be empty afterwards because Cancel
	// was invoked on the post-Register send failure.
	svc.sendProbe(idTargetX, idPeerB)

	if got := svc.probeRegistry.Len(); got != 0 {
		t.Fatalf("probeRegistry.Len = %d after sendProbe with failing send path, want 0 (Cancel must remove the pending entry)", got)
	}
}

// TestReview_v4_P2_1_Cancel_RemovesPendingWithoutFiringTimeout —
// direct unit test for the new Cancel method on probeRegistry:
// removes the entry, stops the timeout, and onTimeout is never
// invoked.
func TestReview_v4_P2_1_Cancel_RemovesPendingWithoutFiringTimeout(t *testing.T) {
	timeoutCalls := 0
	reg := newProbeRegistry(20*time.Millisecond, nil, func(domain.PeerIdentity, domain.PeerIdentity) {
		timeoutCalls++
	})

	id := reg.Register(idTargetX, idPeerB)
	if reg.Len() != 1 {
		t.Fatalf("setup: Len = %d, want 1", reg.Len())
	}

	reg.Cancel(id)
	if reg.Len() != 0 {
		t.Fatalf("after Cancel: Len = %d, want 0", reg.Len())
	}

	// Wait past the would-have-fired deadline to confirm the
	// timer was actually stopped.
	time.Sleep(80 * time.Millisecond)
	if timeoutCalls != 0 {
		t.Fatalf("onTimeout fired %d times after Cancel; want 0", timeoutCalls)
	}
}

// --- P2 #2: negative paths skip absent pairs ---

// TestReview_v4_P2_2_MarkProbeFailure_DropsAbsentPair — a late
// timeout fire after TickTTL evicted the underlying claim must
// NOT resurrect the pair as a fresh entry. Without this fix the
// late timeout would create a new HealthGood with current
// LastHopAck, hiding the eviction from operators.
func TestReview_v4_P2_2_MarkProbeFailure_DropsAbsentPair(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// Sanity: empty health store.
	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("setup: HealthSnapshot = %v, want nil", snap)
	}

	tbl.MarkProbeFailure(idTargetX, idPeerB)

	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("MarkProbeFailure created health state for absent pair: %v", snap)
	}
}

// TestReview_v4_P2_2_MarkProbeAck_NegativeDropsAbsentPair — same
// invariant for the reachable=false ack path.
func TestReview_v4_P2_2_MarkProbeAck_NegativeDropsAbsentPair(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("setup: HealthSnapshot = %v, want nil", snap)
	}

	tbl.MarkProbeAck(idTargetX, idPeerB, false, 0)

	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("MarkProbeAck(reachable=false) created health state for absent pair: %v", snap)
	}
}

// TestReview_v4_P2_2_MarkProbeAck_ReachableDropsAbsentPair — PR
// 11.24 P2#2 inverts the prior contract. The old behaviour
// (reachable=true ensures even for an absent storage claim)
// allowed a late ack arriving after TickTTL / cap-eviction to
// leave an orphan HealthGood entry; a fresh UpdateRoute for the
// same pair would then preserve it (eviction-on-accept only
// clears Bad/Dead, not Good), letting the new unverified claim
// inherit Good/RTT from the stale ack — a verify-before-trust
// bypass.
//
// The fix verifies the live storage claim atomically inside
// MarkProbeAck (same pattern as Table.ConfirmHopAck): no claim
// → drop the ack without touching health. The negative path
// already had this guard from PR 11.11 P2#2; this test pins the
// symmetric guard on the positive path.
func TestReview_v4_P2_2_MarkProbeAck_ReachableDropsAbsentPair(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// No upsertClaim → no storage claim for (idTargetX, idPeerB).
	tbl.MarkProbeAck(idTargetX, idPeerB, true, 30*time.Millisecond)

	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("MarkProbeAck(reachable=true) on absent claim created health: %v (PR 11.24 P2#2 — orphan ensure must be blocked)", snap)
	}
}

// --- P2 #3: Dead is sticky on negative ack ---

// TestReview_v4_P2_3_Dead_StaysDeadOnReachableFalse — a probe ack
// with reachable=false against a Dead pair must NOT transition
// the pair back to Questionable, because Lookup filters only
// Dead, and a "I can't reach X" answer should not silently
// re-enable a route that was previously excluded from selection.
func TestReview_v4_P2_3_Dead_StaysDeadOnReachableFalse(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// Seed a live storage claim — PR 11.24 P2#2 requires
	// MarkProbeAck(reachable=true) to verify the backing claim
	// exists before ensuring health.
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("setup: UpdateRoute: %v", err)
	}

	// Establish health, then force Dead.
	tbl.MarkProbeAck(idTargetX, idPeerB, true, 0) // ensures Good
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	// Negative ack from the same peer.
	tbl.MarkProbeAck(idTargetX, idPeerB, false, 0)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthDead {
		t.Fatalf("Health = %s, want dead (negative ack must NOT lift Dead back to Questionable)", snap[0].Health)
	}
}

// --- P3 #4: LastProbe bump in applyProbeFailure ---

// TestReview_v4_P3_4_MarkProbeFailure_BumpsLastProbe — a probe
// timeout must update LastProbe so observability can distinguish
// "no probe activity has ever been observed for this pair" from
// "probes are being fired but none ack back".
func TestReview_v4_P3_4_MarkProbeFailure_BumpsLastProbe(t *testing.T) {
	t0 := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := t0
	tbl := routing.NewTable(
		routing.WithLocalOrigin(idNodeA),
		routing.WithClock(func() time.Time { return clk }),
	)

	// PR 11.24 P2#2: MarkProbeAck(reachable=true) now verifies the
	// backing storage claim before ensuring health. Seed a live
	// claim so the ack creates the pair as before.
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: t0.Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("setup: UpdateRoute: %v", err)
	}

	// Establish a pair (Good with LastHopAck at t0; LastProbe is
	// the zero time because no probe has been issued yet).
	tbl.MarkProbeAck(idTargetX, idPeerB, true, 0)

	// Advance clock and fire a timeout.
	clk = t0.Add(20 * time.Second)
	tbl.MarkProbeFailure(idTargetX, idPeerB)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if !snap[0].LastProbe.Equal(clk) {
		t.Fatalf("LastProbe = %v, want %v (applyProbeFailure must bump LastProbe)", snap[0].LastProbe, clk)
	}
}

// --- P3 #5: triggerRouteQueryAsync spawn throttle ---

// TestReview_v4_P3_5_TriggerRouteQueryAsync_BoundedByBudget —
// concurrent triggers for the same target must NOT exhaust the
// emission budget through spawn count. The PR 11.12 P1#1 redesign
// throttles goroutine spawns through a per-target in-flight flag
// (TryReserveInFlight) that is disjoint from the emission budget,
// so a burst of triggers produces exactly one goroutine whose
// SendRouteQuery emits per the normal HasCapacity / Record
// contract. PendingCount stays bounded by the actual emission
// fan-out (≤ candidate peers × 1 spawn), well within the budget.
func TestReview_v4_P3_5_TriggerRouteQueryAsync_BoundedByBudget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	// Tight per-target budget so the assertion is observable
	// without timing fragility.
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, 2)

	// Add a triplet-capable peer so SendRouteQuery has a fan-out
	// target. The spawned goroutine will iterate this peer in the
	// fan-out loop; without a real send path the wire enqueue
	// fails but the Record stamp still lands — which is what we
	// observe through PendingCount. PR 11.21 P2#2: candidate
	// discovery now also requires a live peerHealth entry.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	// Burst of concurrent triggers for the same target.
	const burst = 20
	var wg sync.WaitGroup
	wg.Add(burst)
	for i := 0; i < burst; i++ {
		go func() {
			defer wg.Done()
			svc.triggerRouteQueryAsync(idTargetX)
		}()
	}
	wg.Wait()

	// Allow the spawned goroutine to run.
	time.Sleep(50 * time.Millisecond)

	// Budget is 2: PendingCount must stay ≤ 2 regardless of how
	// many callers raced. With the in-flight spawn throttle only
	// one goroutine runs, the fan-out has one capable peer, so
	// PendingCount lands at exactly 1 — the assertion's upper
	// bound is the production contract, not the expected value.
	if got := svc.queryRateLimit.PendingCount(idTargetX); got > 2 {
		t.Fatalf("rate-limit pending count = %d after burst of %d triggers, want ≤ 2 (in-flight spawn throttle must cap concurrent goroutines)", got, burst)
	}
}

// TestReview_v4_P1_1_TriggerRouteQueryAsync_DoesNotStarveBudget —
// the PR 11.12 P1#1 regression test: a burst of triggers must
// still produce at least one actual emission stamp. The previous
// design used Record as both a spawn reservation and an emission
// stamp, so a burst of (limit) triggers could consume the entire
// budget on reservations alone and leave SendRouteQuery's
// HasCapacity check returning false — net zero emissions for the
// window despite traffic begging for route discovery. The fix
// decouples spawn throttling from emission accounting; this test
// pins the contract that an emission DOES land after a burst.
func TestReview_v4_P1_1_TriggerRouteQueryAsync_DoesNotStarveBudget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	// Production-equivalent budget: 3 emissions per window.
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// Single triplet-capable peer — the goroutine will Record one
	// stamp per peer it tries, and we want to assert "at least
	// one" lands, not "exactly N". PR 11.21 P2#2: also seed
	// peerHealth so candidate discovery accepts the session.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	// Burst sized to the budget — under the old design every
	// reservation would consume one budget slot and leave the
	// spawned SendRouteQuery with HasCapacity=false.
	const burst = queryFanOutLimit
	var wg sync.WaitGroup
	wg.Add(burst)
	for i := 0; i < burst; i++ {
		go func() {
			defer wg.Done()
			svc.triggerRouteQueryAsync(idTargetX)
		}()
	}
	wg.Wait()

	// Wait for the spawned goroutine to land its emission stamp.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if svc.queryRateLimit.PendingCount(idTargetX) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got == 0 {
		t.Fatal("emission budget was fully consumed by spawn reservations — burst of triggers produced zero actual emissions (P1#1 regression)")
	}
}

// TestReview_v4_P1_1_TryReserveInFlight_IsAtomic — direct unit
// test for the new spawn-throttle primitive. Two consecutive
// reservations for the same target must yield true, false (the
// second caller is rejected); after releaseInFlight the slot
// reopens.
func TestReview_v4_P1_1_TryReserveInFlight_IsAtomic(t *testing.T) {
	r := newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	if !r.TryReserveInFlight(idTargetX) {
		t.Fatal("first TryReserveInFlight returned false on cold state")
	}
	if r.TryReserveInFlight(idTargetX) {
		t.Fatal("second TryReserveInFlight returned true while first still in flight")
	}
	if !r.IsInFlight(idTargetX) {
		t.Fatal("IsInFlight returned false despite a successful reservation")
	}

	// Different target is independent.
	if !r.TryReserveInFlight(idTargetY) {
		t.Fatal("TryReserveInFlight on a different target returned false — flag must be per-target")
	}

	r.releaseInFlight(idTargetX)
	if r.IsInFlight(idTargetX) {
		t.Fatal("IsInFlight returned true after releaseInFlight")
	}
	if !r.TryReserveInFlight(idTargetX) {
		t.Fatal("TryReserveInFlight returned false after the slot was released")
	}
}

// --- P2 #2: confirmRouteViaHopAck verifies route before marking health ---

// TestReview_v4_P2_2_ConfirmRouteViaHopAck_RevivesDeadRoute — the
// PR 11.12 P2#2 contract: when a route exists in storage but its
// health was driven to Dead (passive timeline or probe failures),
// a hop_ack from the matching uplink must still revive it. The
// pre-fix code called Lookup, which filters Dead, so the ack was
// silently dropped despite MarkHopAck's documented "Good on ack
// regardless of prior health" contract.
//
// We force the pair to Dead via ForceHealthForTest, then issue a
// hop_ack. After the fix the health snapshot shows Good (proof
// MarkHopAck fired) and the route source is promoted to HopAck
// (proof UpdateRoute also fired — the lookup path used InspectTriple
// which is unfiltered).
func TestReview_v4_P2_2_ConfirmRouteViaHopAck_RevivesDeadRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Seed a transit announcement route.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}

	// Drive health for this pair to Dead — Lookup-based recovery
	// would not see it past this point.
	svc.routingTable.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	snap := svc.routingTable.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthGood {
		t.Fatalf("Dead pair was not revived: Health = %s, want good (MarkHopAck must fire even when prior state was Dead)", snap[0].Health)
	}

	// Lookup now succeeds (Dead filter no longer applies); the
	// route should have been promoted to source=HopAck through the
	// UpdateRoute call.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup len = %d after Dead→Good revive, want 1", len(routes))
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("Source = %s after hop_ack on Dead pair, want hop_ack", routes[0].Source)
	}
}

// TestReview_v4_P2_2_ConfirmRouteViaHopAck_NoOrphanHealthOnUnmatchedAck —
// the other half of P2#2: when a hop_ack arrives with a
// (recipient, uplink) pair that has no storage claim, the
// function must NOT create a fresh Good health entry for that
// unmatched pair, AND must not touch the existing health of any
// unrelated pair. The pre-fix code called MarkHopAck before the
// storage-match loop, so an ack from a peer with no claim forged
// an orphan health entry for an arbitrary (Identity, that-peer)
// pair.
//
// Setup-side baseline. UpdateRoute on a fresh (Identity, Uplink)
// pair creates a Questionable health entry as a side effect
// (PR 11.9 P1#1: every accepted/unchanged route ensures a
// matching health state). So the snapshot is NOT empty after
// setup — it contains one Questionable entry for (idTargetX,
// idPeerB). The orphan-health concern is about the UNMATCHED
// pair (idTargetX, idPeerC), so we assert:
//
//   - no entry appears for (idTargetX, idPeerC) — the orphan
//     pair the unmatched ack would forge;
//   - the existing (idTargetX, idPeerB) entry stays Questionable
//     (a hop_ack from peer-C must not stamp Good on peer-B's
//     pair either, even by accident);
//   - the route source for peer-B stays Announcement (no
//     promotion).
func TestReview_v4_P2_2_ConfirmRouteViaHopAck_NoOrphanHealthOnUnmatchedAck(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Seed a route via peer-B so resolvePeerIdentity has context,
	// but the ack will arrive from peer-C — no matching claim.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}

	// Baseline snapshot — one Questionable entry for (idTargetX,
	// idPeerB) created by UpdateRoute's PR 11.9 P1#1 health
	// ensure path.
	baseline := svc.routingTable.HealthSnapshot()
	if len(baseline) != 1 {
		t.Fatalf("setup: HealthSnapshot len = %d, want 1", len(baseline))
	}
	if baseline[0].Uplink != idPeerB {
		t.Fatalf("setup: baseline entry Uplink = %q, want %q", baseline[0].Uplink, idPeerB)
	}
	if baseline[0].Health != routing.HealthQuestionable {
		t.Fatalf("setup: baseline entry Health = %s, want questionable", baseline[0].Health)
	}

	// Ack arrives from peer-C — there is no (idTargetX, idPeerC)
	// claim in storage. The pre-fix code would have called
	// MarkHopAck(idTargetX, idPeerC, 0) before realising the loop
	// has no match, leaving an orphan Good entry for that pair.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerC), "")

	after := svc.routingTable.HealthSnapshot()
	if len(after) != 1 {
		t.Fatalf("unmatched ack changed HealthSnapshot len = %d, want 1 (orphan entry must not be created)", len(after))
	}
	if after[0].Uplink == idPeerC {
		t.Fatalf("unmatched ack created orphan health for (idTargetX, idPeerC): %+v (MarkHopAck must run only after InspectTriple confirms a matching claim)", after[0])
	}
	if after[0].Uplink != idPeerB {
		t.Fatalf("post-ack entry Uplink = %q, want %q", after[0].Uplink, idPeerB)
	}
	if after[0].Health != routing.HealthQuestionable {
		t.Fatalf("peer-B health = %s after peer-C ack, want questionable (an unmatched ack must not promote any pair)", after[0].Health)
	}
	// The peer-B route must remain at its original Announcement
	// source — the ack from peer-C carries no authority over it.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup len = %d, want 1", len(routes))
	}
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("peer-B route source = %s, want announcement (peer-C ack must not promote)", routes[0].Source)
	}
}

// TestReview_v4_P2_2_ConfirmRouteViaHopAck_SkipsWithdrawnRoute —
// the inactive-route guard: a hop_ack arriving for a (recipient,
// uplink) pair whose route was explicitly withdrawn (hops=16)
// must not promote it via UpdateRoute and must not flip its
// existing health to Good. Withdrawal is a definitive "this
// route is gone" signal — even a verified peer ack carries no
// admission authority once the upstream retracted the
// announcement.
//
// Setup-side baseline. The announcement-then-withdraw sequence
// leaves the (idTargetX, idPeerB) health entry as Questionable
// from the original UpdateRoute (PR 11.9 P1#1 ensure on accept).
// WithdrawRoute does NOT evict health on its own — eviction
// runs through TickTTL on the storage-side cleanup pass. The
// orphan-health concern here is whether the ack would flip that
// existing Questionable to Good behind the withdrawal; the fix
// short-circuits before MarkHopAck.
func TestReview_v4_P2_2_ConfirmRouteViaHopAck_SkipsWithdrawnRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Insert as announcement first so the bucket exists.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute(announcement) status=%v err=%v", status, err)
	}

	// Withdraw it.
	if ok := svc.routingTable.WithdrawRoute(idTargetX, idTargetX, idPeerB, 2); !ok {
		t.Fatal("setup: WithdrawRoute returned false")
	}

	// Confirm Lookup sees no live route now.
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("setup: Lookup returned %d live routes after Withdraw, want 0", len(routes))
	}

	// Baseline: the original UpdateRoute created a Questionable
	// health entry; the withdrawal does not evict it inline.
	baseline := svc.routingTable.HealthSnapshot()
	if len(baseline) != 1 {
		t.Fatalf("setup: HealthSnapshot len = %d, want 1", len(baseline))
	}
	baselineHealth := baseline[0].Health

	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	after := svc.routingTable.HealthSnapshot()
	if len(after) != 1 {
		t.Fatalf("hop_ack on withdrawn route changed HealthSnapshot len = %d, want 1 (no new entry should appear)", len(after))
	}
	if after[0].Health == routing.HealthGood && baselineHealth != routing.HealthGood {
		t.Fatalf("hop_ack on withdrawn route promoted health to Good (was %s) — MarkHopAck must not fire when InspectTriple shows IsWithdrawn=true", baselineHealth)
	}
	// The withdrawal must remain — UpdateRoute must NOT have
	// resurrected the route at source=HopAck.
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("hop_ack on withdrawn route resurrected it: Lookup returned %d entries, want 0", len(routes))
	}
}

// --- P3 #2 (PR 11.13): triggerRouteQueryAsync HasCapacity pre-check ---

// TestReview_v4_P3_TriggerRouteQueryAsync_PreCheckRejectsExhaustedBudget —
// sequential-churn regression test. When the per-target emission
// budget is full, a stream of relay misses would otherwise spawn
// a fresh no-op goroutine on every miss (Reserve → SendRouteQuery
// → HasCapacity=false → return 0 → release). The PR 11.13 P3a
// fix adds a cheap HasCapacity pre-check before TryReserveInFlight,
// so a trigger with no available budget returns BEFORE touching
// the in-flight flag.
//
// We pre-fill the emission budget to `limit`, then call
// triggerRouteQueryAsync and immediately observe IsInFlight. With
// the pre-check, IsInFlight stays false synchronously (the
// function returned before TryReserveInFlight). Without the
// pre-check, TryReserveInFlight would succeed and set the flag,
// then `go func` would schedule but not yet run the goroutine —
// the immediate IsInFlight read would observe true. The race is
// inherent in `go`-scheduling, but a deterministic false here
// proves the pre-check rejected first.
func TestReview_v4_P3_TriggerRouteQueryAsync_PreCheckRejectsExhaustedBudget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	// Tight per-target budget to make exhaustion trivial.
	const limit = 2
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, limit)

	// Pre-fill the budget so HasCapacity returns false for
	// idTargetX. Record `limit` stamps directly.
	for i := 0; i < limit; i++ {
		if !svc.queryRateLimit.Record(idTargetX) {
			t.Fatalf("setup: Record %d returned false", i)
		}
	}
	if svc.queryRateLimit.HasCapacity(idTargetX) {
		t.Fatal("setup: HasCapacity returned true after pre-fill — budget is not actually full")
	}

	// Add a capable peer so peersWithRouteQueryCap would otherwise
	// produce a candidate (we want the test to verify the
	// pre-check, not skip-on-no-peers). PR 11.21 P2#2: candidate
	// discovery also requires a live peerHealth entry.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	// Pre-condition: no in-flight goroutine for idTargetX.
	if svc.queryRateLimit.IsInFlight(idTargetX) {
		t.Fatal("setup: IsInFlight returned true before any trigger")
	}

	// Trigger. With the pre-check, this must return without
	// touching the in-flight flag — the rejection happens before
	// TryReserveInFlight.
	svc.triggerRouteQueryAsync(idTargetX)

	if svc.queryRateLimit.IsInFlight(idTargetX) {
		t.Fatal("triggerRouteQueryAsync set IsInFlight even though budget is exhausted — HasCapacity pre-check did not short-circuit (sequential-churn pathology)")
	}

	// Sanity: budget count is unchanged. The pre-check itself
	// does a side-effect-free read (HasCapacity trims expired
	// stamps but does not record). With limit=2 and 30 s
	// window, no stamps expire during the test.
	if got := svc.queryRateLimit.PendingCount(idTargetX); got != limit {
		t.Fatalf("PendingCount = %d after exhausted-budget trigger, want %d (pre-check must not consume budget)", got, limit)
	}
}

// TestReview_v4_P3_TriggerRouteQueryAsync_PreCheckAllowsAvailableBudget —
// the positive-control companion to the rejection test. When
// budget IS available, the pre-check must not block the spawn;
// the goroutine must run SendRouteQuery as before. We observe
// either IsInFlight=true (spawn in progress) or PendingCount > 0
// (spawn completed and emitted at least one stamp) within a
// short polling window.
func TestReview_v4_P3_TriggerRouteQueryAsync_PreCheckAllowsAvailableBudget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// Capable peer so SendRouteQuery's fan-out has a candidate.
	// PR 11.21 P2#2: + peerHealth.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	svc.triggerRouteQueryAsync(idTargetX)

	// Poll for spawn evidence: either the in-flight flag is set
	// (goroutine is mid-flight) or PendingCount advanced (goroutine
	// completed and recorded at least one emission stamp).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if svc.queryRateLimit.IsInFlight(idTargetX) || svc.queryRateLimit.PendingCount(idTargetX) > 0 {
			return // pass
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("triggerRouteQueryAsync did not spawn a goroutine despite available budget — HasCapacity pre-check is over-rejecting")
}

// --- P2 #1.2 (PR 11.13): Table.ConfirmHopAck atomicity ---

// TestReview_v4_P2_AtomicConfirmHopAck_NoLiveClaim — direct unit
// test for Table.ConfirmHopAck. When the (identity, uplink) pair
// has no storage claim, the method returns applied=false and
// does NOT create a health entry. This is the cap-eviction edge
// case folded under one Lock so a concurrent withdrawal cannot
// race past the check.
func TestReview_v4_P2_AtomicConfirmHopAck_NoLiveClaim(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	status, applied := tbl.ConfirmHopAck(idTargetX, idPeerB, 0)
	if applied {
		t.Fatalf("ConfirmHopAck returned applied=true for absent claim, status=%v", status)
	}
	if snap := tbl.HealthSnapshot(); snap != nil {
		t.Fatalf("ConfirmHopAck on absent claim created health: %v", snap)
	}
}

// TestReview_v4_P2_AtomicConfirmHopAck_WithdrawnClaim — withdrawn
// claims must not be revived. ConfirmHopAck returns applied=false
// when InspectTriple sees IsWithdrawn=true and does not touch
// health.
func TestReview_v4_P2_AtomicConfirmHopAck_WithdrawnClaim(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// Seed announcement then withdraw.
	status, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}
	if ok := tbl.WithdrawRoute(idTargetX, idTargetX, idPeerB, 2); !ok {
		t.Fatal("setup: WithdrawRoute returned false")
	}

	// Baseline: UpdateRoute seeded Questionable health on accept.
	baseline := tbl.HealthSnapshot()
	if len(baseline) != 1 {
		t.Fatalf("setup: HealthSnapshot len = %d, want 1", len(baseline))
	}
	if baseline[0].Health != routing.HealthQuestionable {
		t.Fatalf("setup: baseline Health = %s, want questionable", baseline[0].Health)
	}

	_, applied := tbl.ConfirmHopAck(idTargetX, idPeerB, 0)
	if applied {
		t.Fatal("ConfirmHopAck returned applied=true for withdrawn claim — atomic guard failed")
	}

	after := tbl.HealthSnapshot()
	if len(after) != 1 {
		t.Fatalf("post-confirm HealthSnapshot len = %d, want 1", len(after))
	}
	if after[0].Health != routing.HealthQuestionable {
		t.Fatalf("withdrawn claim's health flipped to %s — ConfirmHopAck must short-circuit before applyHopAck", after[0].Health)
	}
}

// TestReview_v4_P2_AtomicConfirmHopAck_LiveAnnouncementPromotesToHopAck —
// happy path: a live Announcement claim atomically refreshes
// health to Good AND gets promoted to RouteSourceHopAck. Health
// mutation and source promotion landed under the same Lock.
func TestReview_v4_P2_AtomicConfirmHopAck_LiveAnnouncementPromotesToHopAck(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	status, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}

	confirmStatus, applied := tbl.ConfirmHopAck(idTargetX, idPeerB, 25*time.Millisecond)
	if !applied {
		t.Fatal("ConfirmHopAck returned applied=false on live Announcement claim")
	}
	if confirmStatus != routing.RouteAccepted {
		t.Fatalf("ConfirmHopAck status = %v, want RouteAccepted (promotion should have run)", confirmStatus)
	}

	// Health must be Good and RTT must reflect the sample.
	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthGood {
		t.Fatalf("Health = %s, want good (applyHopAck must have fired)", snap[0].Health)
	}
	if snap[0].RTT == 0 {
		t.Fatal("RTT EWMA did not absorb the 25ms sample — UpdateRTT path missed")
	}

	// Source must be promoted.
	routes := tbl.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup len = %d, want 1", len(routes))
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("Source = %s after ConfirmHopAck, want hop_ack", routes[0].Source)
	}
}

// TestReview_v4_P3_TriggerRouteQueryAsync_NoCapablePeersPreCheck —
// the PR 11.14 P3 regression: in a mixed-version network with no
// triplet-capable neighbours, every relay miss would otherwise
// spawn a fresh no-op goroutine (HasCapacity true → Reserve →
// SendRouteQuery → len(candidates)==0 → return 0 → release).
// The hasAnyPeerWithRouteQueryCap pre-check stops the spawn
// before TryReserveInFlight.
//
// Setup has zero capable peers — sessions are intentionally
// empty, so the pre-check returns false and triggerRouteQueryAsync
// must return without touching the in-flight flag. Synchronous
// IsInFlight check after the trigger proves the rejection
// happened before TryReserveInFlight.
func TestReview_v4_P3_TriggerRouteQueryAsync_NoCapablePeersPreCheck(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// No capable peers — svc.sessions is empty after
	// newTestServiceWithRouting, and we add nothing here.
	if svc.hasAnyPeerWithRouteQueryCap() {
		t.Fatal("setup: hasAnyPeerWithRouteQueryCap returned true on empty session set")
	}

	svc.triggerRouteQueryAsync(idTargetX)

	if svc.queryRateLimit.IsInFlight(idTargetX) {
		t.Fatal("triggerRouteQueryAsync set IsInFlight despite no capable peers — pre-check did not short-circuit (mixed-version churn pathology)")
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got != 0 {
		t.Fatalf("PendingCount = %d after no-capable-peers trigger, want 0 (no emission must happen, no churn)", got)
	}
}

// TestReview_v4_P3_HasAnyPeerWithRouteQueryCap_Behaviour — direct
// unit test for the early-exit helper. Three cases:
//
//   - empty session set → false;
//   - session with FULL triplet → true;
//   - session with PARTIAL caps (missing routing or relay) → false
//     (mirrors the receive-side gate; the helper must apply the
//     SAME admission rule peersWithRouteQueryCap uses, or the
//     pre-check would diverge from SendRouteQuery's real fan-out
//     and either over-spawn or over-reject).
func TestReview_v4_P3_HasAnyPeerWithRouteQueryCap_Behaviour(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		if svc.hasAnyPeerWithRouteQueryCap() {
			t.Fatal("returned true on empty session set")
		}
	})

	t.Run("full_triplet", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
			domain.CapMeshRouteQueryV1,
			domain.CapMeshRelayV1,
			domain.CapMeshRoutingV1,
		})
		if !svc.hasAnyPeerWithRouteQueryCap() {
			t.Fatal("returned false with a triplet-capable session present")
		}
	})

	t.Run("partial_missing_routing", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		// Seed health so the cap check is the only thing that
		// can reject. CapMeshRoutingV1 deliberately absent.
		seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
			domain.CapMeshRouteQueryV1,
			domain.CapMeshRelayV1,
		})
		if svc.hasAnyPeerWithRouteQueryCap() {
			t.Fatal("returned true for a session missing CapMeshRoutingV1 — pre-check must match peersWithRouteQueryCap's triplet rule")
		}
	})

	t.Run("partial_missing_relay", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		// Seed health so the cap check is the only thing that
		// can reject. CapMeshRelayV1 deliberately absent.
		seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
			domain.CapMeshRouteQueryV1,
			domain.CapMeshRoutingV1,
		})
		if svc.hasAnyPeerWithRouteQueryCap() {
			t.Fatal("returned true for a session missing CapMeshRelayV1 — pre-check must match peersWithRouteQueryCap's triplet rule")
		}
	})
}

// TestReview_v4_P2_AtomicConfirmHopAck_AlreadyHopAck — when the
// claim is already at HopAck (or better), ConfirmHopAck refreshes
// health (status=RouteUnchanged, applied=true) without re-running
// UpdateRoute. This preserves the existing-source-tier short-
// circuit from the pre-refactor confirmRouteViaHopAck behaviour.
func TestReview_v4_P2_AtomicConfirmHopAck_AlreadyHopAck(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	status, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceHopAck,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}

	confirmStatus, applied := tbl.ConfirmHopAck(idTargetX, idPeerB, 0)
	if !applied {
		t.Fatal("ConfirmHopAck returned applied=false on live HopAck claim")
	}
	if confirmStatus != routing.RouteUnchanged {
		t.Fatalf("ConfirmHopAck status = %v on already-HopAck claim, want RouteUnchanged (no re-promotion needed)", confirmStatus)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 || snap[0].Health != routing.HealthGood {
		t.Fatalf("health not refreshed to Good: %v", snap)
	}
}
