package routing_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// wireFrameCounter tracks how many times each PeerSender method was
// called. It is the v1-level guard that the announce loop never routes a
// send through the v2 routes_update scaffold method (SendRoutesUpdate):
// the legacy announce_routes frame carries both full sync and delta in v1,
// and any silent regression that flipped the delta path to
// SendRoutesUpdate would break mixed-version compatibility on the wire.
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
	return m, c
}

// TestAnnounceLoop_FullSync_UsesLegacyFrame verifies that the first send
// to a fresh peer — which is always a full sync — goes through the legacy
// SendAnnounceRoutes method and never through SendRoutesUpdate. This is
// the connect-time invariant encoded in the PeerSender interface:
// initial sync is always legacy, regardless of any future v2 negotiated
// state.
func TestAnnounceLoop_FullSync_UsesLegacyFrame(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, counter := newWireFramePeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
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
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, counter := newWireFramePeerSender(t)

	// Two direct peers so the initial full sync has at least 2 entries —
	// the subsequent delta (1 new route) must be strictly smaller.
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}
	if _, err := table.AddDirectPeer("peer-E"); err != nil {
		t.Fatalf("AddDirectPeer peer-E: %v", err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
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
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
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

// TestAnnounceLoop_DoesNotReadCapabilities_Yet is a regression guard:
// AnnounceTarget.Capabilities is plumbed into the announce cycle for a
// future routing-announce v2 mode-selection path, but v1 must NOT branch on
// it. Two peers — one with no capabilities, one with a speculative v2
// capability — must both receive the same wire frame (SendAnnounceRoutes)
// with the same payload shape. If a well-meaning change starts picking
// SendRoutesUpdate based on Capabilities before the v2 wire frame actually
// lands, this test fails and points directly at the commit that broke the
// "initial sync is always legacy announce_routes" invariant documented on
// the PeerSender interface.
func TestAnnounceLoop_DoesNotReadCapabilities_Yet(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, counter := newWireFramePeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	// Two peers with different capability snapshots. "mesh_routing_v2" is a
	// deliberately speculative name — this test MUST stay v1-only, so the
	// capability here is a sentinel that no v1 code path should recognise.
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "addr-noCaps",
				Identity:     "peer-noCaps",
				Capabilities: nil,
			},
			{
				Address:  "addr-withCaps",
				Identity: "peer-withCaps",
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
