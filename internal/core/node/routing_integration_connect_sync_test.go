package node

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestConnectTimeFullSync_UsesLegacySender_NonEmptySnapshot is the
// node-package guard for the first-sync wire-frame invariant documented in
// the "First-sync wire-frame invariant" section of docs/routing.md.
//
// sendConnectTimeFullSync — the shared core invoked from both
// sendFullTableSyncToInbound and sendOutboundFullTableSync — MUST emit the
// legacy announce_routes wire frame and MUST NOT route through the v2
// routes_update scaffold, regardless of any capability the peer may have
// advertised. A fresh session has no baseline to diff an incremental
// routes_update against; sending one there would either be rejected by
// the receiver or desynchronise mixed-version networks.
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
		// the first sync after session establishment is always legacy.
		// A regression that routed this through the v2 scaffold would
		// either produce frame.Type == "routes_update" or — because
		// Service.SendRoutesUpdate is still a warn-only stub — drop the
		// frame entirely. Both regressions are caught here: the first by
		// the type check, the second by the timeout arm below.
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
			"suggests sendConnectTimeFullSync silently dropped the send " +
			"(e.g. routed through the SendRoutesUpdate warn-only stub)")
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
