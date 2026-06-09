package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// ---------------------------------------------------------------------------
// Transit-invalidation-by-reason tests.
//
// Regression guard for the fix that decoupled "mute the peer's
// announce-plane opinions" from "tombstone every transit route behind
// the peer". Only session-instability reasons (disconnect_storm,
// setup_failure_cycle) invalidate transit; chatty_routes must NOT —
// counting a still-connected, often high-degree hub as chatty and then
// tearing down all transit behind it is what collapsed normal
// multi-hop delivery mesh-wide. See quarantineReasonInvalidatesTransit
// and invalidateTransitOnQuarantineLocked.
// ---------------------------------------------------------------------------

// TestQuarantineReasonInvalidatesTransit pins the predicate: only the
// session-instability reasons invalidate transit; everything else
// (chatty_routes, unknown reasons, empty) does not.
func TestQuarantineReasonInvalidatesTransit(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		quarantineReasonDisconnectStorm:   true,
		quarantineReasonSetupFailureCycle: true,
		quarantineReasonChattyRoutes:      false,
		"unknown_reason":                  false,
		"":                                false,
	}
	for reason, want := range cases {
		if got := quarantineReasonInvalidatesTransit(reason); got != want {
			t.Errorf("quarantineReasonInvalidatesTransit(%q) = %v, want %v", reason, got, want)
		}
	}
}

// seedTransitClaim builds a Table with a single transit route
// target-X via the given direct peer and returns the table. The
// caller arms quarantine on the peer and asserts whether the claim
// survived.
func seedTransitClaim(t *testing.T, peer routing.PeerIdentity) *routing.Table {
	t.Helper()
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	if _, err := table.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer(%q): %v", peer, err)
	}
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   peer,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("seed transit claim: status=%v err=%v", status, err)
	}
	// Sanity: the transit claim is live before any quarantine.
	if len(table.Lookup("target-X")) != 1 {
		t.Fatalf("precondition: seeded transit claim must be live, got %d routes",
			len(table.Lookup("target-X")))
	}
	return table
}

// transitLive reports whether target-X still resolves to a usable
// (non-withdrawn) transit route via peer.
func transitLive(table *routing.Table, peer routing.PeerIdentity) bool {
	for _, r := range table.Lookup("target-X") {
		if r.NextHop == peer {
			return true
		}
	}
	return false
}

// TestChattyQuarantineDoesNotInvalidateTransit is the core regression
// test: arming a peer for chatty_routes leaves the transit routes
// behind it intact. The peer's future announcements are still muted by
// the IsPeerInRouteQuarantine gate (covered elsewhere); here we only
// assert the existing transit survives.
func TestChattyQuarantineDoesNotInvalidateTransit(t *testing.T) {
	t.Parallel()

	peer := routing.PeerIdentity("peer-B")
	table := seedTransitClaim(t, peer)

	svc := &Service{
		routingTable:          table,
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domain.PeerIdentity(peer), quarantineReasonChattyRoutes, time.Now())
	svc.peerMu.Unlock()

	if !svc.isPeerInRouteQuarantineLocked(domain.PeerIdentity(peer), time.Now().Add(time.Second)) {
		t.Fatal("peer should be quarantined after chatty arm")
	}
	if !transitLive(table, peer) {
		t.Fatal("chatty_routes quarantine invalidated transit — it must NOT; only the announce gate should apply")
	}
}

// TestDisconnectStormQuarantineInvalidatesTransit is the paired
// positive test: a disconnect_storm arm DOES tombstone the transit
// behind the unstable peer, because a flapping session cannot be
// trusted to carry relay traffic.
func TestDisconnectStormQuarantineInvalidatesTransit(t *testing.T) {
	t.Parallel()

	peer := routing.PeerIdentity("peer-B")
	table := seedTransitClaim(t, peer)

	svc := &Service{
		routingTable:          table,
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domain.PeerIdentity(peer), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	if transitLive(table, peer) {
		t.Fatal("disconnect_storm quarantine must invalidate transit behind the unstable peer")
	}
}

// TestChattyQuarantineKeepsPeerUsableAsTransit is the data-plane half
// of the chatty fix: a chatty_routes peer's routing OPINIONS are muted
// (IsPeerInRouteQuarantine true → inbound announcements dropped), but
// the peer stays usable as a TRANSIT next-hop (IsPeerTransitQuarantined
// false, routeIsBlockedByQuarantine false). Blocking a still-connected
// hub from transit on a chatty signal is what collapsed multi-hop
// delivery mesh-wide.
func TestChattyQuarantineKeepsPeerUsableAsTransit(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentity("peer-B")
	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(peer, quarantineReasonChattyRoutes, time.Now())
	svc.peerMu.Unlock()

	// Announce-plane: opinions still muted.
	if !svc.IsPeerInRouteQuarantine(peer) {
		t.Fatal("chatty peer must still be route-quarantined (inbound announcements dropped)")
	}
	// Data-plane: NOT transit-blocked.
	if svc.IsPeerTransitQuarantined(peer) {
		t.Fatal("chatty peer must remain usable as transit — data-plane delivery must not be blocked")
	}
	if svc.routeIsBlockedByQuarantine(peer, 2) {
		t.Fatal("routeIsBlockedByQuarantine must not skip a chatty transit hop")
	}
}

// TestDisconnectStormQuarantineBlocksTransitSelection is the paired
// positive: an instability quarantine DOES remove the peer from
// transit selection, while a direct destination (hops==1) always
// passes.
func TestDisconnectStormQuarantineBlocksTransitSelection(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentity("peer-B")
	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(peer, quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()

	if !svc.IsPeerTransitQuarantined(peer) {
		t.Fatal("disconnect_storm peer must be transit-blocked")
	}
	if !svc.routeIsBlockedByQuarantine(peer, 2) {
		t.Fatal("routeIsBlockedByQuarantine must skip a disconnect_storm transit hop")
	}
	if svc.routeIsBlockedByQuarantine(peer, 1) {
		t.Fatal("direct destination (hops==1) must never be blocked by quarantine")
	}
}

// TestChattyToInstabilityEscalationInvalidatesTransit pins the
// escalation transition: a peer first quarantined for chatty_routes
// (transit kept) and then re-armed for disconnect_storm must have its
// stale transit tombstoned at the escalation, not left live until the
// longer cooldown expires. Without the prevReason-aware transition
// check, the second arm saw "already active" and skipped invalidation.
func TestChattyToInstabilityEscalationInvalidatesTransit(t *testing.T) {
	t.Parallel()

	peer := routing.PeerIdentity("peer-B")
	table := seedTransitClaim(t, peer)

	svc := &Service{
		routingTable:          table,
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	// 1) chatty arm: transit must survive.
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domain.PeerIdentity(peer), quarantineReasonChattyRoutes, time.Now())
	svc.peerMu.Unlock()
	if !transitLive(table, peer) {
		t.Fatal("precondition: chatty arm must leave transit live")
	}

	// 2) escalate to disconnect_storm: transit must now be invalidated.
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domain.PeerIdentity(peer), quarantineReasonDisconnectStorm, time.Now())
	svc.peerMu.Unlock()
	if transitLive(table, peer) {
		t.Fatal("escalation chatty_routes→disconnect_storm must tombstone stale transit")
	}

	svc.backgroundWg.Wait()
}

// TestInstabilityReasonNotDowngradedByChattyRearm guards the inverse:
// an active disconnect_storm quarantine re-armed for chatty_routes must
// KEEP its transit-blocking reason — otherwise a chatty re-arm would
// silently un-block transit through a still-flapping next-hop.
func TestInstabilityReasonNotDowngradedByChattyRearm(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentity("peer-B")
	svc := &Service{
		peerQuarantine:        map[domain.PeerIdentity]routeQuarantineEntry{},
		peerDisconnectHistory: map[domain.PeerIdentity][]time.Time{},
	}

	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(peer, quarantineReasonDisconnectStorm, time.Now())
	// Re-arm with the weaker chatty reason while still active.
	svc.armRouteQuarantineLocked(peer, quarantineReasonChattyRoutes, time.Now())
	entry := svc.peerQuarantine[peer]
	svc.peerMu.Unlock()

	if entry.Reason != quarantineReasonDisconnectStorm {
		t.Fatalf("reason downgraded to %q; transit-blocking reason must stick while active", entry.Reason)
	}
	if !svc.IsPeerTransitQuarantined(peer) {
		t.Fatal("peer must stay transit-blocked after a chatty re-arm of a disconnect_storm quarantine")
	}
}
