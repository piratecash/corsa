package routing_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// wireFrameCounter tracks how many times each PeerSender method was
// called. It is the guard that protects the first-sync / forced-full
// wire-frame invariant: the announce loop must NOT route the v1
// connect-time / forced-full / divergence-fallback paths through the
// live v2 delta sender (PeerSender.SendRoutesUpdate). The legacy
// announce_routes frame carries both full sync and v1 delta; any silent
// regression that flipped one of those paths to the v2 wire frame would
// break mixed-version compatibility (peers without v2 reject
// routes_update) and violate the baseline contract (a fresh session has
// no baseline to diff a delta against). SendRoutesUpdate IS the real v2
// delta path now — this guard does not protect against a missing
// implementation, it pins the routing of v1 paths AWAY from the live v2
// implementation regardless of what capabilities the peer advertised.
type wireFrameCounter struct {
	announceCalls atomic.Int32
	updateCalls   atomic.Int32

	mu    sync.Mutex
	calls []sendCall
}

func (w *wireFrameCounter) recordAnnounce(addr routing.PeerAddress, routes []routing.AnnounceEntry) {
	w.announceCalls.Add(1)
	w.mu.Lock()
	w.calls = append(w.calls, sendCall{PeerAddress: addr, Routes: routes})
	w.mu.Unlock()
}

func (w *wireFrameCounter) recordUpdate() {
	w.updateCalls.Add(1)
}

func (w *wireFrameCounter) snapshotCalls() []sendCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := make([]sendCall, len(w.calls))
	copy(cp, w.calls)
	return cp
}

// newWireFramePeerSender wires MockPeerSender so that:
//   - every SendAnnounceRoutes call increments announceCalls and records
//     the payload for the test to inspect;
//   - every SendRoutesUpdate call increments updateCalls. v1 tests assert
//     updateCalls stays at zero; any non-zero value means the loop picked
//     the v2 wire frame, which violates the "initial sync is always
//     legacy announce_routes" invariant encoded in the PeerSender
//     interface docstring in announce.go.
func newWireFramePeerSender(t *testing.T) (*routingmocks.MockPeerSender, *wireFrameCounter) {
	t.Helper()
	c := &wireFrameCounter{}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			c.recordAnnounce(addr, routes)
			return true
		},
	).Maybe()
	m.EXPECT().SendRoutesUpdate(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, _ []routing.AnnounceEntry) bool {
			c.recordUpdate()
			return false
		},
	).Maybe()
	// Digest-as-heartbeat: periodic deadline emits a digest before the fallback
	// full; not an announce wire frame, so it is unrecorded.
	m.EXPECT().SendRouteSyncDigest(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()
	return m, c
}

// TestAnnounceLoop_FullSync_UsesLegacyFrame verifies that the first send
// to a fresh peer — which is always a full sync — goes through a
// self-contained baseline frame and never through a delta frame. For
// a peer that has NOT negotiated the v3 triplet (the fixture here)
// that frame is the legacy SendAnnounceRoutes; SendRouteAnnounceV3
// with kind="full" is the symmetric baseline path for v3-triplet
// peers (exercised in routing_integration_connect_sync_test.go). The
// negative assertion is the load-bearing piece: SendRoutesUpdate must
// NEVER fire on this path, regardless of any negotiated v2 state,
// because the peer has no baseline a delta could diff against.
func TestAnnounceLoop_FullSync_UsesLegacyFrame(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, counter := newWireFramePeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

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

	if got := counter.announceCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 SendAnnounceRoutes call for full sync, got %d", got)
	}
	if got := counter.updateCalls.Load(); got != 0 {
		t.Fatalf("v1 full sync must not call SendRoutesUpdate, got %d calls", got)
	}

	calls := counter.snapshotCalls()
	if len(calls[0].Routes) == 0 {
		t.Fatalf("full sync must carry route entries, got empty payload")
	}
}

// TestAnnounceLoop_Delta_UsesLegacyFrame verifies that the incremental
// delta path in v1 still uses the legacy SendAnnounceRoutes method. The
// two-method PeerSender interface exists to make the wire-frame choice
// explicit at the call site; v1 never flips the delta path to
// routes_update because that frame is reserved for capability-gated v2.
func TestAnnounceLoop_Delta_UsesLegacyFrame(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, counter := newWireFramePeerSender(t)

	// Two direct peers so the initial full sync has at least 2 entries —
	// the subsequent delta (1 new route) must be strictly smaller.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}
	if _, err := table.AddDirectPeer(domaintest.ID("peer-E")); err != nil {
		t.Fatalf("AddDirectPeer peer-E: %v", err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync baseline.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if got := counter.announceCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 SendAnnounceRoutes call after first trigger, got %d", got)
	}

	// Add a new route so the next cycle has a non-empty delta.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-D")); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}

	// Second trigger: delta path.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if got := counter.announceCalls.Load(); got != 2 {
		t.Fatalf("expected 2 SendAnnounceRoutes calls (full + delta), got %d", got)
	}
	if got := counter.updateCalls.Load(); got != 0 {
		t.Fatalf("v1 delta must not call SendRoutesUpdate, got %d calls", got)
	}

	calls := counter.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 recorded calls, got %d", len(calls))
	}
	if len(calls[1].Routes) >= len(calls[0].Routes) {
		t.Fatalf("delta payload must be smaller than full: full=%d, delta=%d",
			len(calls[0].Routes), len(calls[1].Routes))
	}
}

// TestAnnounceLoop_DoesNotReadCapabilities_Yet is a regression guard
// for the v1-only fallback floor of the cap-driven dispatch matrix.
// Wire-format selection in announceToAllPeers DOES branch on caps post-
// Phase 4 (legacy / v2 routes_update / v3 route_announce_v3 — see
// classifyDeltaMode and the HasSentWireBaseline* gates), but two
// degenerate cap snapshots must still route to legacy SendAnnounceRoutes:
//
//   - empty caps (no v1, no v2, no v3): the peer can't accept any
//     upgraded frame — only v1 fits;
//   - caps with a SHIPPED upgrade cap (mesh_routing_v2) but WITHOUT
//     v1 alongside it: same outcome — the classifier requires v1 as
//     the floor for any upgrade (see hasCapV2Triplet /
//     hasCapV3Triplet), so v2 / v3 stay off the wire even though the
//     upgrade cap is real and recognised.
//
// Both peers must therefore see the legacy frame here. The test
// name predates the Phase 4 dispatch — it is kept stable to preserve
// CI history and the negative-assertion shape (zero SendRoutesUpdate
// calls); the contract it pins today is "no-upgrade-cap fallback to
// legacy", not the original "loop doesn't read caps yet".
func TestAnnounceLoop_DoesNotReadCapabilities_Yet(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, counter := newWireFramePeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	// Two peers with different capability snapshots. mesh_routing_v2
	// IS a real shipped cap, but the second peer here intentionally
	// declares ONLY mesh_routing_v2 without mesh_routing_v1. The
	// classifier requires v1 alongside any upgrade (v2 or v3) — see
	// hasCapV2Triplet / hasCapV3Triplet — so a v2-without-v1 peer falls
	// through to the v1-only fallback and gets legacy SendAnnounceRoutes,
	// same as the first peer with nil caps. The test pins this floor:
	// no upgraded delta frame leaves on the wire for either peer.
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "addr-noCaps",
				Identity:     domaintest.ID("peer-noCaps"),
				Capabilities: nil,
			},
			{
				Address:  "addr-withCaps",
				Identity: domaintest.ID("peer-withCaps"),
				Capabilities: []routing.PeerCapability{
					routing.PeerCapability("mesh_routing_v2"),
				},
			},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

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

	if got := counter.updateCalls.Load(); got != 0 {
		t.Fatalf("v1 announce loop must NOT call SendRoutesUpdate regardless of Capabilities, got %d calls", got)
	}
	if got := counter.announceCalls.Load(); got != 2 {
		t.Fatalf("expected exactly 2 SendAnnounceRoutes calls (one per peer), got %d", got)
	}

	calls := counter.snapshotCalls()
	byAddr := make(map[routing.PeerAddress]sendCall, len(calls))
	for _, c := range calls {
		byAddr[c.PeerAddress] = c
	}
	noCapsCall, ok := byAddr["addr-noCaps"]
	if !ok {
		t.Fatalf("missing call for peer without capabilities")
	}
	withCapsCall, ok := byAddr["addr-withCaps"]
	if !ok {
		t.Fatalf("missing call for peer with capabilities")
	}
	// Both peers see the same full-sync snapshot — the loop builds payload
	// from the routing table, not from AnnounceTarget.Capabilities.
	if len(noCapsCall.Routes) != len(withCapsCall.Routes) {
		t.Fatalf("payload must not depend on Capabilities: noCaps=%d routes, withCaps=%d routes",
			len(noCapsCall.Routes), len(withCapsCall.Routes))
	}
}
