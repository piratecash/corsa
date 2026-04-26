package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// TestAnnounceLoop_ForcedFull_UsesLegacySender_NonEmptySnapshot is the
// routing-package guard for the first-sync wire-frame invariant
// documented in the "First-sync wire-frame invariant" section of
// docs/routing.md: the forced-full / periodic-forced-full path in
// AnnounceLoop MUST send the legacy announce_routes wire frame via
// SendAnnounceRoutes and MUST NOT route through SendRoutesUpdate,
// regardless of the peer's Capabilities snapshot.
//
// The test differs from TestAnnounceLoop_FullSync_UsesLegacyFrame in two
// ways:
//
//  1. it exercises the *forced-resync-after-baseline* branch of
//     sendFullAnnounce — the peer already has a recorded baseline, is
//     explicitly marked NeedsFullResync (simulating reconnect or state
//     invalidation), and has its rate-limit timer cleared so the cycle
//     reaches the wire-send call instead of being coalesced;
//  2. the mock SendRoutesUpdate hook calls t.Fatalf on any invocation.
//     The wire-frame assertion happens the moment a regression routes
//     forced-full through the live v2 delta sender (in production:
//     PeerSender.SendRoutesUpdate → node.Service implementation that
//     emits a routes_update frame), not at the end of the test when a
//     counter is compared to zero. Both checks are kept because the
//     Fatalf path surfaces call-site context in goroutine stacks while
//     the counter check catches the case where nothing at all was sent.
//
// The routing table is seeded with a direct peer so BuildAnnounceSnapshot
// returns a non-empty payload; the empty-baseline short-circuit at
// announce.go's len(snapshot.Entries) == 0 branch bypasses the wire
// sender entirely and therefore cannot exercise this guard (see the
// dedicated empty-baseline regression test in the node package).
func TestAnnounceLoop_ForcedFull_UsesLegacySender_NonEmptySnapshot(t *testing.T) {
	// Fixed clock so the rate-limit logic inside announceToAllPeers is
	// deterministic: we clear LastFullSyncAttemptAt below, so "now - 0"
	// greatly exceeds MinForcedFullSyncInterval and the cycle proceeds.
	now := time.Now()
	clock := func() time.Time { return now }

	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	sender := routingmocks.NewMockPeerSender(t)
	// Record SendAnnounceRoutes payloads so we can assert shape.
	var announceCalls int
	var lastRoutes []routing.AnnounceEntry
	sender.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			announceCalls++
			// Copy the slice so the assertion does not depend on whether
			// the caller reuses the underlying array between cycles.
			lastRoutes = append(lastRoutes[:0], routes...)
			return true
		},
	).Maybe()
	// SendRoutesUpdate is the real v2 delta wire path — calling it
	// from the forced-full branch is the regression this test guards
	// against. The first-sync invariant requires forced-full to ride
	// the legacy frame regardless of capabilities; routing it through
	// the v2 sender would either (a) put a delta on a peer with no
	// baseline, or (b) trigger the v2 send-time gate to drop the frame
	// silently when the test fixture lacks the v1+v2+relay caps.
	sender.EXPECT().SendRoutesUpdate(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, delta []routing.AnnounceEntry) bool {
			t.Fatalf("forced-full must not route through SendRoutesUpdate "+
				"(first-sync wire-frame invariant, docs/routing.md): "+
				"peer_address=%s delta_entries=%d", addr, len(delta))
			return false
		},
	).Maybe()

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:  "addr-C",
				Identity: "peer-C",
				// A speculative v2 capability on the target MUST NOT change
				// the wire-frame choice for forced-full. The whole point
				// of the first-sync wire-frame invariant (docs/routing.md)
				// is that this field is not consulted on the legacy path.
				Capabilities: []routing.PeerCapability{
					routing.PeerCapability("mesh_routing_v2"),
				},
			},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	// Prime per-peer state: establish a baseline and then explicitly mark
	// NeedsFullResync so the cycle takes the forced-full branch inside
	// announceToAllPeers (needsFull = NeedsFullResync || LastSentSnapshot == nil).
	// The rate-limit timer must be zero so the cycle is not coalesced
	// under "too soon since last forced full sync attempt".
	state := registry.GetOrCreate("peer-C")
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now.Add(-1*time.Minute))
	state.SetNeedsFullResyncForTest()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if announceCalls != 1 {
		t.Fatalf("expected exactly 1 SendAnnounceRoutes call on forced full, got %d", announceCalls)
	}
	if len(lastRoutes) == 0 {
		t.Fatalf("forced full must carry the non-empty snapshot payload, got 0 entries")
	}
	// Sanity check on the payload shape — the direct peer we added must be
	// represented in the forced-full snapshot the peer receives.
	foundB := false
	for _, e := range lastRoutes {
		if e.Identity == "peer-B" {
			foundB = true
			break
		}
	}
	if !foundB {
		t.Fatalf("expected peer-B in the forced-full snapshot, got entries=%+v", lastRoutes)
	}
}

// TestRoutingConstants_RefreshIntervalInvariant is a compile-time-style guard
// for the refresh-interval invariant. AnnounceLoop suppresses the wire send
// when the per-peer delta is empty (announce.go: skippedNoop branch), so
// learned routes on neighbors are renewed only by the forced-full sync
// cadence ForcedFullSyncMultiplier*DefaultAnnounceInterval. If that cadence
// is not strictly tighter than half the route TTL, a single dropped or
// late-arriving full sync lets the neighbor's copy expire before the next
// refresh — the route silently disappears for up to one full cycle even
// though the origin keeps confirming it. The "<= TTL/2" bound is the safety
// margin: at the boundary case the next refresh arrives exactly when the
// previous lifetime ends, and one missed cycle still keeps the route alive
// until the cycle after that.
//
// Production receive-side fix is in Table.UpdateRoute on the RouteUnchanged
// branch: an alive same-SeqNo same-source same-hops re-application extends
// ExpiresAt to now+defaultTTL. Without that fix this invariant alone is not
// sufficient; with it, this invariant is the second half of the contract.
func TestRoutingConstants_RefreshIntervalInvariant(t *testing.T) {
	refresh := time.Duration(routing.ForcedFullSyncMultiplier) * routing.DefaultAnnounceInterval
	half := routing.DefaultTTL / 2
	if refresh > half {
		t.Fatalf("refresh-interval invariant violated: ForcedFullSyncMultiplier*DefaultAnnounceInterval=%s must be <= DefaultTTL/2=%s (TTL=%s); learned routes will silently expire on neighbors between forced full syncs",
			refresh, half, routing.DefaultTTL)
	}
}
