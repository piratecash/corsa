package node

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestConnectTimeFullSync_UsesLegacySender_NonEmptySnapshot is the
// node-package guard for the first-sync wire-frame invariant documented in
// the "First-sync wire-frame invariant" section of docs/routing.md, for
// the v1/v2-peer half of the dispatch.
//
// sendConnectTimeFullSync — the shared core invoked from both
// sendFullTableSyncToInbound and sendOutboundFullTableSync — picks a
// self-contained baseline frame based on the peer's negotiated caps:
// legacy announce_routes for v1/v2 peers (this test) or
// route_announce_v3 with kind="full" for v3-triplet peers (covered by
// TestConnectTimeFullSync_UsesV3FullSender_NonEmptySnapshot below).
// In either branch the dispatch MUST NOT route through a delta path
// (Service.SendRoutesUpdate or SendRouteAnnounceV3 with kind="delta")
// — a fresh session has no baseline to diff an incremental against;
// sending one would either be rejected by the receiver dispatcher
// (which gates deltas on v1+v2+relay / v1+v3+relay AND the per-peer
// baseline flag) or desynchronise mixed-version networks where the
// peer expects an initial baseline.
//
// This particular guard exercises the v1/v2-peer path by setting up
// the inbound conn without the mesh_routing_v3 cap, so
// peerSupportsRoutingV3 returns false and the dispatch falls into
// SendAnnounceRoutes. The negative assertion (frame.Type ==
// "announce_routes", NOT "routes_update", NOT "route_announce_v3")
// fails loudly in any direction.
//
// The guard exercises the non-empty-snapshot branch: the routing table is
// seeded with a direct peer so BuildAnnounceSnapshot produces at least one
// entry. The empty-baseline short-circuit at routing_announce.go's
// len(snapshot.Entries) == 0 branch bypasses the wire sender entirely and
// is covered separately by TestConnectTimeFullSync_EmptySnapshot_NoWireFrame
// below — that contract is "neither legacy nor v2 frame on the wire", not
// the first-sync wire-frame invariant.
//
// Observation method: the inbound path synchronously writes the marshalled
// frame to the connection, so we pipe it through a net.Pipe and parse the
// JSON line. Asserting frame.Type explicitly against both "announce_routes"
// and "routes_update" makes the test fail loudly in two directions — a
// missing frame or the wrong frame — without depending on substring search
// that might coincidentally match either name.
func TestConnectTimeFullSync_UsesLegacySender_NonEmptySnapshot(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Non-empty peer-specific snapshot: one direct route to peer-C that
	// the connecting inbound peer (peer-B) learns about on connect.
	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 22000},
	}
	// NetCore must be created BEFORE setTestConnEntryLocked because
	// enqueueFrameSync routes through the writer goroutine; without one
	// the send would fall back to a blocking direct write with a deadline.
	pc := netcore.New(netcore.ConnID(100), conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Reader is started before the function under test; net.Pipe is
	// unbuffered, so the writer would block past syncFlushTimeout
	// otherwise and the send would look like a transport drop.
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
	}()

	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(context.Background(), id, idPeerB)

	select {
	case data := <-received:
		var frame protocol.Frame
		if err := json.Unmarshal(data, &frame); err != nil {
			t.Fatalf("unmarshal wire frame: %v (raw=%q)", err, string(data))
		}

		// First-sync wire-frame invariant guard (see docs/routing.md):
		// for a peer WITHOUT the v3 triplet (this fixture's case —
		// no caps wired on the conn so peerSupportsRoutingV3 returns
		// false), the first sync after session establishment must
		// land as legacy announce_routes. A regression that routed
		// this through the live v2 delta path (Service.SendRoutesUpdate
		// + dispatchAnnouncePlaneFrameWithCaps) would either produce
		// frame.Type == "routes_update" or — if the captured-handle
		// gate dropped the send because the test fixture lacks the
		// v2 cap on the conn — fail to emit any wire frame at all.
		// Both regressions are caught here: the first by the type
		// check, the second by the timeout arm below. The v3-side
		// guard is TestConnectTimeFullSync_UsesV3FullSender_NonEmptySnapshot.
		if frame.Type == "routes_update" {
			t.Fatalf("connect-time sync must not emit routes_update " +
				"(first-sync wire-frame invariant, docs/routing.md): got v2 wire frame")
		}
		if frame.Type != "announce_routes" {
			t.Fatalf("expected announce_routes on the wire, got %q", frame.Type)
		}

		if len(frame.AnnounceRoutes) == 0 {
			t.Fatalf("connect-time full sync must carry the non-empty snapshot, got 0 routes")
		}
		foundC := false
		for _, r := range frame.AnnounceRoutes {
			if r.Identity == idPeerC {
				foundC = true
				break
			}
		}
		if !foundC {
			t.Fatalf("expected peer-C in the full-sync payload, got routes=%+v", frame.AnnounceRoutes)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for connect-time full sync wire frame — " +
			"no announce_routes frame reached the inbound pipe, which " +
			"suggests sendConnectTimeFullSync stopped emitting the legacy " +
			"baseline (e.g. a regression that routed first sync through " +
			"the live v2 routes_update delta path whose send-time gate " +
			"requires v1+v2+relay caps not present on this fixture)")
	}

	// Peer-state side-effect: RecordFullSyncSuccess must have fired so
	// subsequent cycles can compute deltas against this baseline.
	state := svc.announceLoop.StateRegistry().Get(idPeerB)
	if state == nil {
		t.Fatal("announce peer state not created for inbound peer")
	}
	view := state.View()
	if view.LastSentSnapshot == nil {
		t.Fatal("expected LastSentSnapshot to be recorded after successful connect-time sync")
	}
	if view.NeedsFullResync {
		t.Fatal("NeedsFullResync should clear after RecordFullSyncSuccess")
	}
}

// TestConnectTimeFullSync_UsesV3FullSender_NonEmptySnapshot is the
// node-package guard for the v3-peer half of the first-sync wire-frame
// invariant: when the inbound peer's negotiated cap set includes the
// v3 triplet (mesh_routing_v1 + mesh_routing_v3 + mesh_relay_v1),
// sendConnectTimeFullSync MUST dispatch through SendRouteAnnounceV3
// with kind="full" — NOT legacy SendAnnounceRoutes, NOT a delta of
// any flavour (routes_update or kind="delta"). Mirrors the v1/v2 guard
// above; together the two tests pin both halves of the dispatch.
func TestConnectTimeFullSync_UsesV3FullSender_NonEmptySnapshot(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 22001},
	}
	pc := netcore.New(netcore.ConnID(101), conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()
	// v3-triplet caps on the inbound conn so peerSupportsRoutingV3
	// returns true and sendConnectTimeFullSync picks the v3 baseline
	// branch. Without these caps the dispatch falls back to legacy
	// (covered by the v1/v2 test above).
	pc.SetCapabilities([]domain.Capability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV3,
		domain.CapMeshRelayV1,
	})

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
	}()

	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(context.Background(), id, idPeerB)

	select {
	case data := <-received:
		var frame protocol.Frame
		if err := json.Unmarshal(data, &frame); err != nil {
			t.Fatalf("unmarshal wire frame: %v (raw=%q)", err, string(data))
		}
		// Triple negative pin: v3 connect-time MUST emit
		// route_announce_v3, NOT legacy announce_routes (would mean
		// the v3 cap gate didn't fire), NOT routes_update (would
		// mean a v2 delta path took over), NOT a v3 kind="delta"
		// (would mean the kind discriminator slipped).
		if frame.Type == "announce_routes" {
			t.Fatalf("v3-triplet peer must NOT receive legacy announce_routes on connect-time; got legacy frame")
		}
		if frame.Type == "routes_update" {
			t.Fatalf("v3-triplet peer must NOT receive routes_update on connect-time; got v2 delta")
		}
		if frame.Type != protocol.RouteAnnounceV3FrameType {
			t.Fatalf("expected %q on the wire, got %q", protocol.RouteAnnounceV3FrameType, frame.Type)
		}
		// Parse the RawLine for kind / entries — Frame.RawLine holds
		// the pre-marshalled JSON for the v3 frame (see
		// buildRouteAnnounceV3Frame).
		var v3 protocol.RouteAnnounceV3Frame
		if err := json.Unmarshal(data, &v3); err != nil {
			t.Fatalf("unmarshal v3 frame: %v", err)
		}
		if v3.Kind != protocol.RouteAnnounceV3KindFull {
			t.Fatalf("connect-time v3 sync MUST be kind=%q, got %q (delta on a fresh session has no baseline to diff)", protocol.RouteAnnounceV3KindFull, v3.Kind)
		}
		if len(v3.Entries) == 0 {
			t.Fatal("connect-time v3 sync must carry the non-empty snapshot, got 0 entries")
		}
		foundC := false
		for _, e := range v3.Entries {
			if e.Identity == string(idPeerC) {
				foundC = true
				break
			}
		}
		if !foundC {
			t.Fatalf("expected peer-C in the v3 full-sync payload, got entries=%+v", v3.Entries)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for v3 connect-time full sync wire frame")
	}

	// Same peer-state side-effect contract as the legacy guard:
	// RecordFullSyncSuccess must have fired so subsequent cycles can
	// compute deltas against this baseline.
	state := svc.announceLoop.StateRegistry().Get(idPeerB)
	if state == nil {
		t.Fatal("announce peer state not created for inbound peer")
	}
	view := state.View()
	if view.LastSentSnapshot == nil {
		t.Fatal("expected LastSentSnapshot to be recorded after successful v3 connect-time sync")
	}
	if !view.HasSentWireBaselineV3 {
		t.Fatal("HasSentWireBaselineV3 must flip after a successful v3 kind=full send so subsequent cycles can pick v3 delta")
	}
}

// TestConnectTimeFullSync_EmptySnapshot_NoWireFrame is the regression
// guard for the empty-baseline short-circuit at routing_announce.go's
// len(snapshot.Entries) == 0 branch.
//
// When the peer-specific snapshot is empty (empty table OR everything
// split-horizoned out), the call site records a full-sync baseline and
// MUST NOT write any wire frame — not the legacy announce_routes, not
// the v2 routes_update. This contract is orthogonal to the first-sync
// wire-frame invariant in docs/routing.md (which is about which frame
// to use when one IS sent) but it anchors the reasoning behind it:
// guard tests for the first-sync invariant deliberately seed a non-empty
// snapshot because this branch produces nothing observable on the wire.
//
// A future refactor that accidentally made empty-baseline emit either
// frame type would break two things at once: subscribers would see a
// spurious event on every connect, and peers would count an empty legacy
// announce as a destructive snapshot (the "announce_routes is not a
// destructive snapshot" invariant in docs/routing.md). Both are enough
// reason to keep this regression guard.
func TestConnectTimeFullSync_EmptySnapshot_NoWireFrame(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Routing table is intentionally left empty — no direct peers, no
	// transit routes. BuildAnnounceSnapshot therefore returns an empty
	// slice and sendConnectTimeFullSync must take the empty-baseline
	// branch.

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 22001},
	}
	pc := netcore.New(netcore.ConnID(101), conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(context.Background(), id, idPeerB)

	// Give the inbound write path a chance to run; absence of data on
	// the pipe within this window is the positive signal.
	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		readDone <- n
	}()

	select {
	case n := <-readDone:
		t.Fatalf("empty-baseline branch must not emit any wire frame "+
			"(neither legacy announce_routes nor v2 routes_update), "+
			"but got %d bytes on the wire", n)
	case <-time.After(200 * time.Millisecond):
		// Expected path: nothing sent.
	}

	// Even without a wire frame, the baseline is recorded so the next
	// announce cycle computes a meaningful delta.
	state := svc.announceLoop.StateRegistry().Get(idPeerB)
	if state == nil {
		t.Fatal("announce peer state not created on empty-baseline sync")
	}
	view := state.View()
	if view.LastSentSnapshot == nil {
		t.Fatal("empty-baseline must still record LastSentSnapshot so " +
			"later cycles can diff against it")
	}
	if view.NeedsFullResync {
		t.Fatal("empty-baseline RecordFullSyncSuccess must clear NeedsFullResync")
	}
}
