package node

import (
	"fmt"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_receive_count_cap_test.go pins the Phase 4 13.5 RECEIVE-side
// enforcement of maxRoutesPerAnnounceFrame. The send-side chunker
// (announce_chunk_count_cap_test.go) guarantees this node never EMITS a
// frame above the cap; this test file proves the receiver also refuses
// to INGEST a frame above the cap regardless of what a remote peer
// (buggy or hostile) chose to put on the wire. The cap therefore holds
// on both ends, not just as a polite convention of our own chunker.

// buildAnnounceRouteFrames materialises n distinct AnnounceRouteFrame
// entries with the wire shape used by both announce_routes and
// routes_update frames. Identities/origins vary across entries so a
// (silently) truncated apply path would still leave a detectable
// post-condition on the routing table.
func buildAnnounceRouteFrames(n int) []protocol.AnnounceRouteFrame {
	out := make([]protocol.AnnounceRouteFrame, n)
	for i := 0; i < n; i++ {
		out[i] = protocol.AnnounceRouteFrame{
			Identity: fmt.Sprintf("dd%038x", i+1),
			Origin:   fmt.Sprintf("cc%038x", i+1),
			Hops:     1,
			SeqNo:    uint64(i + 1),
		}
	}
	return out
}

// buildRouteAnnounceV3Entries is the v3-shape counterpart of
// buildAnnounceRouteFrames. The v3 frame omits Origin (synthesised
// from sender at the receiver) so the entry shape is slightly
// different on the wire.
func buildRouteAnnounceV3Entries(n int) []protocol.RouteAnnounceV3Entry {
	out := make([]protocol.RouteAnnounceV3Entry, n)
	for i := 0; i < n; i++ {
		out[i] = protocol.RouteAnnounceV3Entry{
			Identity: fmt.Sprintf("dd%038x", i+1),
			Hops:     1,
			SeqNo:    uint64(i + 1),
		}
	}
	return out
}

// TestHandleAnnounceRoutes_OverCapFrameDroppedWhole verifies that
// handleAnnounceRoutes rejects an oversized frame outright: no entry
// is applied, the baseline flag is NOT flipped (the frame is treated
// as malformed at the wire layer, not as an empty-but-valid baseline).
func TestHandleAnnounceRoutes_OverCapFrameDroppedWhole(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})

	state := registry.Get(idPeerB)
	if state == nil {
		t.Fatalf("per-peer state must exist after MarkReconnected")
	}
	if state.HasReceivedBaseline() {
		t.Fatalf("precondition: baseline must be false before frame")
	}

	frame := protocol.Frame{
		Type:           "announce_routes",
		AnnounceRoutes: buildAnnounceRouteFrames(maxRoutesPerAnnounceFrame + 1),
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	// None of the over-cap entries must have been applied: probing the
	// first identity is enough — the cap drops the WHOLE frame, so a
	// partial apply would have stored at least the first one.
	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(frame.AnnounceRoutes[0].Identity)); len(got) > 0 {
		t.Fatalf("over-cap legacy frame must not be applied: %d entries stored for first identity", len(got))
	}
	// An over-cap frame is rejected at the wire layer; it does NOT count
	// as the first-sync baseline (a malformed baseline would let a hostile
	// peer cheaply unblock v2 deltas without ever delivering a real
	// snapshot).
	if state.HasReceivedBaseline() {
		t.Fatalf("over-cap legacy frame must NOT flip baseline (treated as malformed)")
	}
}

// TestHandleAnnounceRoutes_AtCapFrameAccepted pins the boundary: a frame
// with EXACTLY maxRoutesPerAnnounceFrame entries is still valid and must
// apply normally. The cap is a hard >, not >=.
func TestHandleAnnounceRoutes_AtCapFrameAccepted(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})

	entries := buildAnnounceRouteFrames(maxRoutesPerAnnounceFrame)
	frame := protocol.Frame{
		Type:           "announce_routes",
		AnnounceRoutes: entries,
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	// First and last entries are both present — a partial apply would
	// have shown up here.
	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(entries[0].Identity)); len(got) == 0 {
		t.Fatalf("at-cap legacy frame: first entry missing")
	}
	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(entries[len(entries)-1].Identity)); len(got) == 0 {
		t.Fatalf("at-cap legacy frame: last entry missing")
	}
}

// TestHandleRoutesUpdate_OverCapFrameDroppedWhole verifies the v2 delta
// receive path enforces the same cap as the legacy baseline path. The
// pre-baseline gate is not what protects us here — the cap fires before
// the baseline gate (the rate-limit + cap checks run first), so an
// over-cap delta is rejected without even reaching the SendRequestResync
// fallback.
func TestHandleRoutesUpdate_OverCapFrameDroppedWhole(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
	// Flip baseline so the cap is the only guard in play.
	registry.GetOrCreate(idPeerB).MarkBaselineReceived()

	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	frame := protocol.Frame{
		Type:           "routes_update",
		AnnounceRoutes: buildAnnounceRouteFrames(maxRoutesPerAnnounceFrame + 50),
	}

	svc.handleRoutesUpdate(idPeerB, senderAddr, frame)

	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(frame.AnnounceRoutes[0].Identity)); len(got) > 0 {
		t.Fatalf("over-cap v2 delta must not apply: %d entries stored for first identity", len(got))
	}
	// No wire frame on the send channel: the cap drops the input BEFORE
	// the baseline gate, so request_resync is not emitted (would only fire
	// from the baseline gate, which we never reached).
	select {
	case f := <-sendCh:
		t.Fatalf("over-cap drop must NOT emit a wire frame, got %q", f.Type)
	default:
	}
}

// TestHandleRouteAnnounceV3_OverCapFrameDroppedWhole verifies the v3
// compact-wire receive path enforces the same cap. The cap check runs
// before epoch / baseline gating so the heavy-weight verify path never
// inspects an oversized frame.
func TestHandleRouteAnnounceV3_OverCapFrameDroppedWhole(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	state := registry.Get(idPeerB)
	if state == nil {
		t.Fatalf("per-peer state must exist after MarkReconnected")
	}

	entries := buildRouteAnnounceV3Entries(maxRoutesPerAnnounceFrame + 1)
	frame := protocol.RouteAnnounceV3Frame{
		Kind:    protocol.RouteAnnounceV3KindFull,
		Epoch:   1,
		Entries: entries,
	}

	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"), frame)

	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(entries[0].Identity)); len(got) > 0 {
		t.Fatalf("over-cap v3 frame must not apply: %d entries stored for first identity", len(got))
	}
	if state.HasReceivedBaseline() {
		t.Fatalf("over-cap v3 full must NOT flip baseline (treated as malformed)")
	}
}

// TestHandleRouteAnnounceV3_AtCapFrameAccepted pins the boundary on
// the v3 path: exactly maxRoutesPerAnnounceFrame entries still apply
// and establish the baseline.
func TestHandleRouteAnnounceV3_AtCapFrameAccepted(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	entries := buildRouteAnnounceV3Entries(maxRoutesPerAnnounceFrame)
	frame := protocol.RouteAnnounceV3Frame{
		Kind:    protocol.RouteAnnounceV3KindFull,
		Epoch:   1,
		Entries: entries,
	}

	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"), frame)

	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(entries[0].Identity)); len(got) == 0 {
		t.Fatalf("at-cap v3 frame: first entry missing")
	}
	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(entries[len(entries)-1].Identity)); len(got) == 0 {
		t.Fatalf("at-cap v3 frame: last entry missing")
	}
	if state := registry.Get(idPeerB); state == nil || !state.HasReceivedBaseline() {
		t.Fatalf("at-cap v3 full must establish baseline")
	}
}
