package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// ---------------------------------------------------------------------------
// Route quarantine — integration tests at the API call-sites where the
// gate must actually fire. State-machine unit tests live in
// routing_route_quarantine_test.go; these tests are explicitly about the
// integration with applyAnnounceEntries (receive-path drop),
// TableRouter.Route + tryForwardViaRoutingTable (relay gate), and the
// setup_failure_cycle trigger. Earlier passes added quarantine state
// without integration coverage and the table_router.go gate was missed
// — these tests pin every call site explicitly.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TableRouter integration (relay-forward gate)
// ---------------------------------------------------------------------------

// TestQuarantine_TableRouterSkipsTransitViaQuarantinedNextHop verifies
// that TableRouter.Route does NOT pick a quarantined peer as transit
// next-hop. When the route table holds target-X via quarantined peer-B
// AND a non-quarantined alternative peer-C, peer-C wins.
func TestQuarantine_TableRouterSkipsTransitViaQuarantinedNextHop(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer(domaintest.ID("peer-C")); err != nil {
		t.Fatal(err)
	}
	// target-X via peer-B (rank 1) AND via peer-C (rank 2).
	for _, hop := range []struct {
		next routing.PeerIdentity
		seq  uint64
	}{
		{domaintest.ID("peer-B"), 1},
		{domaintest.ID("peer-C"), 2},
	} {
		status, err := table.UpdateRoute(routing.RouteEntry{
			Identity: domaintest.ID("target-X"),
			Origin:   hop.next,
			NextHop:  hop.next,
			Hops:     2,
			SeqNo:    hop.seq,
			Source:   routing.RouteSourceAnnouncement,
		})
		if err != nil || status != routing.RouteAccepted {
			t.Fatalf("UpdateRoute via %s failed: status=%v err=%v", hop.next, status, err)
		}
	}

	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	// Put peer-B in quarantine.
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domaintest.ID("peer-B"), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	tr := &TableRouter{
		svc:   svc,
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return domain.PeerAddress("addr-" + peerIdentity.String())
		},
	}

	decision := tr.Route(protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    domaintest.ID("node-A").String(),
		Recipient: domaintest.ID("target-X").String(),
	})

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set (peer-C alternative exists)")
	}
	if *decision.RelayNextHop != domaintest.ID("peer-C") {
		t.Fatalf("expected RelayNextHop=peer-C (peer-B is quarantined), got %s", *decision.RelayNextHop)
	}
}

// TestQuarantine_TableRouterDirectRouteToQuarantinedPeerStillReachable
// pins the inverse: when recipient IS the quarantined peer (hops=1),
// the direct route MUST be used. Quarantine suppresses TRANSIT trust,
// not data-plane delivery to the peer itself.
func TestQuarantine_TableRouterDirectRouteToQuarantinedPeerStillReachable(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-Q")); err != nil {
		t.Fatal(err)
	}

	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domaintest.ID("peer-Q"), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	tr := &TableRouter{
		svc:   svc,
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return domain.PeerAddress("addr-" + peerIdentity.String())
		},
	}

	decision := tr.Route(protocol.Envelope{
		ID:        "msg-direct",
		Topic:     "dm",
		Sender:    domaintest.ID("node-A").String(),
		Recipient: domaintest.ID("peer-Q").String(),
	})

	if decision.RelayNextHop == nil {
		t.Fatal("direct route to quarantined peer must still be reachable (quarantine suppresses transit only)")
	}
	if *decision.RelayNextHop != domaintest.ID("peer-Q") {
		t.Fatalf("expected RelayNextHop=peer-Q (direct), got %s", *decision.RelayNextHop)
	}
}

// TestQuarantine_TableRouterFallsBackWhenOnlyTransitIsQuarantined
// covers the case where the ONLY route to target is through a
// quarantined transit peer: we must report no-route (gossip
// fallback), not silently pick the quarantined hop.
func TestQuarantine_TableRouterFallsBackWhenOnlyTransitIsQuarantined(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("target-Y"),
		Origin:   domaintest.ID("peer-B"),
		NextHop:  domaintest.ID("peer-B"),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("UpdateRoute failed: status=%v err=%v", status, err)
	}

	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domaintest.ID("peer-B"), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	tr := &TableRouter{
		svc:   svc,
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return domain.PeerAddress("addr-" + peerIdentity.String())
		},
	}

	decision := tr.Route(protocol.Envelope{
		ID:        "msg-only-via-quarantined",
		Topic:     "dm",
		Sender:    domaintest.ID("node-A").String(),
		Recipient: domaintest.ID("target-Y").String(),
	})

	if decision.RelayNextHop != nil {
		t.Fatalf("expected gossip fallback (RelayNextHop=nil) when only transit is quarantined; got %s",
			*decision.RelayNextHop)
	}
}

// ---------------------------------------------------------------------------
// applyAnnounceEntries integration (receive-path drop)
// ---------------------------------------------------------------------------

// TestQuarantine_ApplyAnnounceEntriesDropsQuarantinedSender verifies
// that inbound routing snapshots from a quarantined peer are silently
// dropped: the routing table is NOT mutated, no UpdateRoute calls
// happen.
func TestQuarantine_ApplyAnnounceEntriesDropsQuarantinedSender(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))

	svc := &Service{
		routingTable:          table,
		identity:              &identity.Identity{Address: "ff00000000000000000000000000000000000001"},
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	sender := domain.PeerIdentityFromWire("bb00000000000000000000000000000000000099")
	target := "aa00000000000000000000000000000000000088"
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(sender, "test", time.Now())
	svc.peerMu.Unlock()

	wireRoutes := []protocol.AnnounceRouteFrame{
		{Identity: target, Origin: sender.String(), Hops: 2, SeqNo: 1},
	}

	// Quarantine gate is at the very top of applyAnnounceEntries —
	// the routing-table mutation path is never reached, so we do
	// not need the full Service fixture (announceLoop, etc).
	svc.applyAnnounceEntries(sender, wireRoutes, nil, nil, announceReceiveLegacy)

	// Verify nothing was added to the table.
	got := table.Lookup(domain.PeerIdentityFromWire(target))
	if len(got) != 0 {
		t.Fatalf("expected zero routes after quarantine-dropped announce; got %d entries", len(got))
	}
}

// (Positive-path "non-quarantined sender's announce is processed"
// is NOT included here as a Service-level test — running
// applyAnnounceEntries through to UpdateRoute requires the full
// fixture surface (announceLoop, statePersister, etc) which lives
// in routing_integration_test.go. The drop-side test above is
// sufficient evidence that the quarantine gate at the top of
// applyAnnounceEntries fires: it cannot drop anything without
// IsPeerInRouteQuarantine being consulted, and IsPeerInRouteQuarantine
// is itself covered by the unit tests in
// routing_route_quarantine_test.go.)

// ---------------------------------------------------------------------------
// Shared gate (routeIsBlockedByQuarantine) — every relay-selection
// call-site (tryForwardViaRoutingTable and TableRouter.Route) routes
// through this single helper. Testing it directly covers BOTH gates
// regardless of the call-site fixture surface, which would otherwise
// require full Network()/sendFrameToAddress wiring to drive
// tryForwardViaRoutingTable end-to-end.
// ---------------------------------------------------------------------------

func TestRouteIsBlockedByQuarantine_DirectPasses(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerQuarantine: map[domain.PeerIdentity]routeQuarantineEntry{
			domaintest.ID("peer-Q"): {Until: time.Now().Add(time.Hour), Reason: quarantineReasonDisconnectStorm, LastArmed: time.Now(), Strikes: 1},
		},
	}
	if svc.routeIsBlockedByQuarantine(domaintest.ID("peer-Q"), 1) {
		t.Fatal("direct destination (hops=1) to quarantined peer must NOT be blocked — quarantine suppresses transit only")
	}
}

func TestRouteIsBlockedByQuarantine_TransitBlocked(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerQuarantine: map[domain.PeerIdentity]routeQuarantineEntry{
			domaintest.ID("peer-Q"): {Until: time.Now().Add(time.Hour), Reason: quarantineReasonDisconnectStorm, LastArmed: time.Now(), Strikes: 1},
		},
	}
	if !svc.routeIsBlockedByQuarantine(domaintest.ID("peer-Q"), 2) {
		t.Fatal("transit (hops>1) through quarantined peer must be blocked")
	}
	if !svc.routeIsBlockedByQuarantine(domaintest.ID("peer-Q"), 5) {
		t.Fatal("transit through quarantined peer must be blocked at any hops>1")
	}
}

func TestRouteIsBlockedByQuarantine_NonQuarantinedPasses(t *testing.T) {
	t.Parallel()

	svc := &Service{
		peerQuarantine: map[domain.PeerIdentity]routeQuarantineEntry{},
	}
	if svc.routeIsBlockedByQuarantine(domaintest.ID("peer-clean"), 1) {
		t.Fatal("non-quarantined peer (direct) must pass")
	}
	if svc.routeIsBlockedByQuarantine(domaintest.ID("peer-clean"), 5) {
		t.Fatal("non-quarantined peer (transit) must pass")
	}
}

func TestRouteIsBlockedByQuarantine_ExpiredQuarantinePasses(t *testing.T) {
	t.Parallel()

	// Quarantine entry exists but Until is in the past.
	svc := &Service{
		peerQuarantine: map[domain.PeerIdentity]routeQuarantineEntry{
			domaintest.ID("peer-was-bad"): {Until: time.Now().Add(-time.Hour), Reason: "test", LastArmed: time.Now().Add(-2 * time.Hour), Strikes: 1},
		},
	}
	if svc.routeIsBlockedByQuarantine(domaintest.ID("peer-was-bad"), 2) {
		t.Fatal("expired quarantine must not block traffic")
	}
}

// ---------------------------------------------------------------------------
// gossip / relay fanout gate — quarantined peer must be excluded from
// gossip targets when used as transit, but must remain a valid target
// when they ARE the recipient themselves.
// ---------------------------------------------------------------------------

// TestQuarantine_RoutingTargetsForMessageDropsQuarantinedTransit
// verifies that gossip fallback for DM does not pick a quarantined
// peer as a transit hop. Without this gate the table-route gate
// closes one path but the executor falls back to gossip and sends
// the relay_message to the same quarantined full node as a "gossip
// target" — still using P as transit.
func TestQuarantine_RoutingTargetsForMessageDropsQuarantinedTransit(t *testing.T) {
	t.Parallel()

	quarantinedID := domain.PeerIdentityFromWire("aa00000000000000000000000000000000000011")
	cleanID := domain.PeerIdentityFromWire("bb00000000000000000000000000000000000022")
	recipientID := domain.PeerIdentityFromWire("cc00000000000000000000000000000000000033")
	quarantinedAddr := domain.PeerAddress("addr-quarantined")
	cleanAddr := domain.PeerAddress("addr-clean")

	svc := &Service{
		sessions:              map[domain.PeerAddress]*peerSession{},
		peerIDs:               map[domain.PeerAddress]domain.PeerIdentity{},
		health:                map[domain.PeerAddress]*peerHealth{},
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	svc.sessions[quarantinedAddr] = &peerSession{
		peerIdentity: quarantinedID,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.sessions[cleanAddr] = &peerSession{
		peerIdentity: cleanID,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.peerIDs[quarantinedAddr] = quarantinedID
	svc.peerIDs[cleanAddr] = cleanID
	svc.health[quarantinedAddr] = &peerHealth{Address: quarantinedAddr, Connected: true}
	svc.health[cleanAddr] = &peerHealth{Address: cleanAddr, Connected: true}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(quarantinedID, quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	targets := svc.routingTargetsForMessage(protocol.Envelope{
		Topic:     "dm",
		Recipient: recipientID.String(),
	})

	// Quarantined peer MUST NOT appear as transit.
	for _, addr := range targets {
		if addr == quarantinedAddr {
			t.Fatal("quarantined peer appeared in gossip targets as transit — quarantine gate missing")
		}
	}
	// Clean peer MUST still be present.
	foundClean := false
	for _, addr := range targets {
		if addr == cleanAddr {
			foundClean = true
			break
		}
	}
	if !foundClean {
		t.Fatal("clean peer missing from gossip targets — fanout filter too aggressive")
	}
}

// TestQuarantine_RoutingTargetsForMessageAllowsRecipientEvenIfQuarantined
// guards the inverse direction: when the message recipient IS the
// quarantined peer, that peer MUST still be included as a target.
// Otherwise direct DM delivery to the peer would silently drop.
func TestQuarantine_RoutingTargetsForMessageAllowsRecipientEvenIfQuarantined(t *testing.T) {
	t.Parallel()

	quarantinedID := domain.PeerIdentityFromWire("aa00000000000000000000000000000000000044")
	quarantinedAddr := domain.PeerAddress("addr-recipient-quarantined")

	svc := &Service{
		sessions:              map[domain.PeerAddress]*peerSession{},
		peerIDs:               map[domain.PeerAddress]domain.PeerIdentity{},
		health:                map[domain.PeerAddress]*peerHealth{},
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	svc.sessions[quarantinedAddr] = &peerSession{
		peerIdentity: quarantinedID,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.peerIDs[quarantinedAddr] = quarantinedID
	svc.health[quarantinedAddr] = &peerHealth{Address: quarantinedAddr, Connected: true}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(quarantinedID, quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	// Message recipient is the quarantined peer itself.
	targets := svc.routingTargetsForMessage(protocol.Envelope{
		Topic:     "dm",
		Recipient: quarantinedID.String(),
	})

	found := false
	for _, addr := range targets {
		if addr == quarantinedAddr {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("quarantined peer dropped as gossip target despite being the recipient — direct delivery broken")
	}
}

// ---------------------------------------------------------------------------
// setup_failure_cycle trigger integration
// ---------------------------------------------------------------------------

// TestQuarantine_SetupFailureCycleArmsQuarantine simulates the
// setup-failure path: N consecutive setupFailures for the same
// address must arm route quarantine on the corresponding identity
// (the same trigger documented in route-withdrawal-grace-period.md
// and wired in peer_sessions.go::onCMSessionEstablished setup-failed
// branch).
func TestQuarantine_SetupFailureCycleArmsQuarantine(t *testing.T) {
	t.Parallel()

	svc := &Service{
		setupFailures:         map[domain.PeerAddress]*setupFailureEntry{},
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}
	addr := mustAddr("10.0.0.42:64646")
	identity := domain.PeerIdentityFromWire("ee00000000000000000000000000000000000055")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	// First (threshold-1) failures: not yet exceeding.
	for i := 0; i < setupFailureBanThreshold-1; i++ {
		svc.recordSetupFailureLocked(addr, now)
	}
	if svc.setupFailureExceedsThresholdLocked(addr) {
		svc.peerMu.Unlock()
		t.Fatalf("threshold reported as exceeded after %d failures; want %d",
			setupFailureBanThreshold-1, setupFailureBanThreshold)
	}

	// The threshold-th failure crosses the line and would arm
	// quarantine in the real call-site (peer_sessions.go).
	svc.recordSetupFailureLocked(addr, now)
	if !svc.setupFailureExceedsThresholdLocked(addr) {
		svc.peerMu.Unlock()
		t.Fatal("threshold should be reported as exceeded after Nth failure")
	}
	svc.armRouteQuarantineLocked(identity, quarantineReasonSetupFailureCycle, now)
	// Same controlled `now` for the check — see
	// TestQuarantine_NoTriggerBelowThreshold for why the public
	// IsPeerInRouteQuarantine helper would silently start to fail
	// here once wall-clock time crossed armTime + quarantineBaseDuration.
	banned := svc.isPeerInRouteQuarantineLocked(identity, now.Add(time.Second))
	entry := svc.peerQuarantine[identity]
	svc.peerMu.Unlock()

	if !banned {
		t.Fatal("identity should be route-quarantined after setup_failure_cycle trigger")
	}
	if entry.Reason != quarantineReasonSetupFailureCycle {
		t.Fatalf("reason = %q, want %q", entry.Reason, quarantineReasonSetupFailureCycle)
	}
}

// ---------------------------------------------------------------------------
// Transit invalidation on arm — stale claims must not outlive the
// quarantine via TTL.
// ---------------------------------------------------------------------------

// TestQuarantine_ArmInvalidatesTransitRoutes pins the fix for the
// TTL-vs-cooldown gap: route TTL (120s) exceeds the base quarantine
// duration (60s), so without local invalidation at arm time the
// peer's pre-quarantine transit claims — claims the peer may have
// withdrawn or corrected DURING the quarantine while we were
// dropping its frames — would become selectable again the moment
// the quarantine expires. Arming for a session-instability reason
// (disconnect_storm / setup_failure_cycle) must tombstone transit
// claims via the peer (so post-expiry transit resumes only from fresh
// announcements) while leaving the DIRECT route to the peer intact
// (quarantine suppresses transit trust, not data-plane delivery).
// chatty_routes does NOT invalidate transit — see
// quarantineReasonInvalidatesTransit and the dedicated
// routing_route_quarantine_transit_test.go.
func TestQuarantine_ArmInvalidatesTransitRoutes(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("target-Z"),
		Origin:   domaintest.ID("peer-B"),
		NextHop:  domaintest.ID("peer-B"),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("UpdateRoute failed: status=%v err=%v", status, err)
	}

	svc := &Service{
		routingTable:          table,
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	// disconnect_storm (a session-instability reason) is what arms
	// transit invalidation — see quarantineReasonInvalidatesTransit.
	// A chatty_routes arm deliberately leaves transit in place (covered
	// by routing_route_quarantine_transit_test.go).
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domaintest.ID("peer-B"), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	// Transit claim via the quarantined peer must be gone from the
	// live lookup — this is what guarantees it cannot resurface
	// after the quarantine expires (Lookup does not consult
	// quarantine state; the tombstone is the protection).
	if got := table.Lookup(domaintest.ID("target-Z")); len(got) != 0 {
		t.Fatalf("transit route via quarantined peer still live after arm: %d entries", len(got))
	}
	// Direct route to the peer itself must survive.
	if got := table.Lookup(domaintest.ID("peer-B")); len(got) == 0 {
		t.Fatal("direct route to quarantined peer was removed — arm must invalidate transit only")
	}

	// Re-arm while already active must be a no-op for the table
	// (no panic, no further mutation) — inbound announcements are
	// dropped during quarantine, so there is nothing new to
	// invalidate.
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domaintest.ID("peer-B"), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()
	if got := table.Lookup(domaintest.ID("peer-B")); len(got) == 0 {
		t.Fatal("re-arm during active quarantine must not touch the direct route")
	}

	// Wait for the background drain/event goroutine (if any) so the
	// race detector sees a clean shutdown.
	svc.backgroundWg.Wait()
}
