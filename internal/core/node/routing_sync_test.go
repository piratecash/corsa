package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

func TestRouteSyncUsesOriginalTransportGate(t *testing.T) {
	for _, direction := range []string{"inbound", "outbound"} {
		t.Run(direction, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			svc.runCtx = t.Context()
			newTransport := func(id domain.ConnID, address domain.PeerAddress) (*netcore.NetCore, func(protocol.Frame)) {
				if direction == "inbound" {
					id, core := newTrackedInboundHealthConn(t, svc, address)
					core.ApplyOpts(netcore.Options{Address: address, Identity: idPeerB, Caps: []domain.Capability{domain.CapMeshRouteSyncV1}})
					core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: idPeerB.String()}})
					return core, func(frame protocol.Frame) { svc.dispatchNetworkFrame(id, frame.RawLine+"\n") }
				}
				session := newReceiveOwnerSession(t, svc, id, address)
				session.capabilities = []domain.Capability{domain.CapMeshRouteSyncV1}
				return session.netCore, func(frame protocol.Frame) { svc.dispatchPeerSessionFrame(address, session, frame) }
			}
			oldCore, dispatchOld := newTransport(5920, "192.0.2.96:64646")
			_, dispatchLive := newTransport(5921, "192.0.2.97:64646")
			expires := time.Now().Add(90 * time.Second)
			if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
				Identity: idTargetX, Origin: idPeerB, NextHop: idPeerB, Hops: 2, SeqNo: 1,
				Source: routing.RouteSourceAnnouncement, ExpiresAt: expires,
			}); err != nil {
				t.Fatal(err)
			}
			digest, count := svc.routingTable.SyncDigestFor(idPeerB)
			if count != 1 {
				t.Fatalf("expected one route in the fixture, got %d", count)
			}
			digestWire, err := protocol.MarshalRouteSyncDigestFrame(protocol.RouteSyncDigestFrame{Digest: digest, KnownIdentitiesCount: count})
			if err != nil {
				t.Fatal(err)
			}
			digestFrame := protocol.Frame{Type: protocol.RouteSyncDigestFrameType, RawLine: string(digestWire)}
			summary := func(match bool) protocol.Frame {
				wire, err := protocol.MarshalRouteSyncSummaryFrame(protocol.RouteSyncSummaryFrame{Digest: digest, Match: match, ExpectFullSync: !match})
				if err != nil {
					t.Fatal(err)
				}
				return protocol.Frame{Type: protocol.RouteSyncSummaryFrameType, RawLine: string(wire)}
			}
			oldCore.ShutSendQueue()
			if svc.netCoreForID(oldCore.ConnID()) != oldCore {
				t.Fatal("test must exercise the interval before unregister")
			}
			svc.announceLoop.MarkPeerDigestPending(idPeerB, time.Now(), digest)
			dispatchOld(digestFrame)
			dispatchOld(summary(true))
			if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 1 || !routes[0].ExpiresAt.Equal(expires) || svc.digestStats.digestsCompared.Load() != 0 {
				t.Fatalf("closed transport changed the digest comparison or route expiry: %v", routes)
			}
			if svc.digestStats.summaryMatch.Load() != 0 {
				t.Fatal("closed transport confirmed a pending digest")
			}
			dispatchLive(digestFrame)
			dispatchLive(summary(true))
			if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 1 || !routes[0].ExpiresAt.After(expires) || svc.digestStats.digestsCompared.Load() != 1 {
				t.Fatalf("open transport did not refresh the matching route: %v", routes)
			}
			if svc.digestStats.summaryMatch.Load() != 1 || !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerB, time.Now()) {
				t.Fatal("open transport did not confirm digest suppression")
			}
			dispatchOld(summary(false))
			if !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerB, time.Now()) {
				t.Fatal("closed transport cleared active digest suppression")
			}
			dispatchLive(summary(false))
			if svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerB, time.Now()) {
				t.Fatal("open transport did not clear digest suppression")
			}
		})
	}
}

// routing_sync_test.go covers Phase 3 PR 12.5 — the
// route_sync_digest_v1 / route_sync_summary_v1 handler pair.
// Unit-level coverage of the digest engine itself lives in
// routing/sync_digest_test.go; this file owns the Service-level
// integration: handler does the right thing on match/mismatch,
// AnnounceLoop suppression is armed correctly, lifecycle hooks
// record/consume the cache.

// TestHandleRouteSyncSummary_MatchArmsAnnounceSuppression — the
// release-blocking integration check: a Match=true summary from
// a peer must arm AnnounceLoop.MarkPeerDigestMatched so the
// next periodic forced-full-sync is suppressed.
func TestHandleRouteSyncSummary_MatchArmsAnnounceSuppression(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	// Wire a minimal AnnounceLoop so the handler does not bail
	// out on the "no announce loop" defensive guard. NewAnnounceLoop
	// requires a sender; the noop sender from the routing test
	// fixtures suffices.
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)

	// Correlation anchor: the handler only arms suppression for a
	// summary that echoes a digest we actually emitted. Simulate the
	// reconnect emit by arming a pending window for that digest.
	svc.announceLoop.MarkPeerDigestPending(idPeerA, time.Now().UTC(), "abcd")

	summary := protocol.RouteSyncSummaryFrame{
		Type:           protocol.RouteSyncSummaryFrameType,
		Digest:         "abcd",
		Match:          true,
		ExpectFullSync: false,
	}
	svc.handleRouteSyncSummary(svc.provenRoutingSender(idPeerA), summary)

	if !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("correlated Match=true summary did not arm suppression")
	}
}

// TestHandleRouteSyncSummary_UncorrelatedMatchIgnored — a Match=true
// summary whose echo does NOT match any digest we emitted must not arm
// suppression. Pins the correlation contract: only a reply to a digest
// we actually sent may suppress a forced full sync.
func TestHandleRouteSyncSummary_UncorrelatedMatchIgnored(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)

	// No pending digest armed for idPeerA — the summary is unsolicited.
	summary := protocol.RouteSyncSummaryFrame{
		Type:   protocol.RouteSyncSummaryFrameType,
		Digest: "abcd",
		Match:  true,
	}
	svc.handleRouteSyncSummary(svc.provenRoutingSender(idPeerA), summary)

	if svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("uncorrelated Match=true summary armed suppression")
	}
}

// TestHandleRouteSyncSummary_EmptyDigestIgnored — a summary with an
// empty echo carries no correlation and must be dropped, matching the
// UnmarshalRouteSyncSummaryFrame contract.
func TestHandleRouteSyncSummary_EmptyDigestIgnored(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)
	// Even with a pending window armed, an empty-echo summary must not
	// touch suppression state in either direction.
	svc.announceLoop.MarkPeerDigestPending(idPeerA, time.Now().UTC(), "abcd")

	summary := protocol.RouteSyncSummaryFrame{
		Type:   protocol.RouteSyncSummaryFrameType,
		Digest: "",
		Match:  true,
	}
	svc.handleRouteSyncSummary(svc.provenRoutingSender(idPeerA), summary)

	// The original pending window is untouched (still active now), proving
	// the empty-echo summary was a no-op rather than a re-arm or clear.
	if !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("empty-digest summary disturbed the existing pending window")
	}
}

// TestHandleRouteSyncSummary_MismatchIsNoop — a Match=false
// summary must NOT arm suppression. Mismatch is the
// "regenerate from scratch" signal; suppressing would defeat
// the purpose of the digest exchange.
func TestHandleRouteSyncSummary_MismatchIsNoop(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)

	summary := protocol.RouteSyncSummaryFrame{
		Type:           protocol.RouteSyncSummaryFrameType,
		Digest:         "abcd",
		Match:          false,
		ExpectFullSync: true,
	}
	svc.handleRouteSyncSummary(svc.provenRoutingSender(idPeerA), summary)

	if svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("Match=false summary armed suppression; gate should stay inactive")
	}
}

// TestHandleRouteSyncSummary_EmptySenderIsNoop — defensive
// guard mirrors the rest of the handler family. An empty sender
// identity means the auth resolution race lost; we must not
// arm suppression for the empty-string peer key.
func TestHandleRouteSyncSummary_EmptySenderIsNoop(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)

	summary := protocol.RouteSyncSummaryFrame{
		Type:   protocol.RouteSyncSummaryFrameType,
		Digest: "abcd",
		Match:  true,
	}
	svc.handleRouteSyncSummary(svc.provenRoutingSender(domain.PeerIdentity{}), summary)

	if svc.announceLoop.IsDigestSuppressionActiveForTest(domain.PeerIdentity{}, time.Now().UTC()) {
		t.Fatal("empty sender armed suppression for empty key")
	}
}

// TestHandleRouteSyncSummary_NoAnnounceLoopIsNoop — defensive
// guard: tests / fixtures that build a partial Service without
// AnnounceLoop must not panic on the handler dereference. The
// handler logs and returns instead.
func TestHandleRouteSyncSummary_NoAnnounceLoopIsNoop(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	// Leave svc.announceLoop nil.
	summary := protocol.RouteSyncSummaryFrame{
		Type:   protocol.RouteSyncSummaryFrameType,
		Digest: "abcd",
		Match:  true,
	}
	// Must not panic.
	svc.handleRouteSyncSummary(svc.provenRoutingSender(idPeerA), summary)
}

// TestRecordPeerDigestOnSessionClose_CachedAndConsumable —
// integration with the table-level cache: the on-close helper
// stores the OUTBOUND announce digest to the peer (AnnounceDigestFor),
// which the on-open ConsumePeerDigestSnapshot returns ok=true with
// non-empty content.
//
// Post-review-P1 semantics: the recorded digest is what we would
// announce TO idPeerA, NOT the routes we learned through idPeerA. So
// the seeded route must reach idTargetX via a DIFFERENT uplink
// (idPeerC) — split horizon excludes routes via the peer itself — and
// we must actually announce to idPeerA first so the per-peer wire
// SeqNo is recorded (outboundPeerMax), the value AnnounceDigestFor
// reproduces.
func TestRecordPeerDigestOnSessionClose_CachedAndConsumable(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerC,
		Hops: 2, SeqNo: 7, Source: routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	// Announce to idPeerA so outboundPeerMax[(idTargetX, idPeerA)] is
	// populated — AnnounceDigestFor digests exactly that pair.
	svc.routingTable.AnnounceTo(idPeerA)

	svc.recordPeerDigestOnSessionClose(idPeerA)

	digest, _, count, _, ok := svc.routingTable.ConsumePeerDigestSnapshot(idPeerA, time.Now().UTC())
	if !ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=false after record")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	// The digest content is verified at the routing-package unit
	// test; here we just pin "not the empty-input constant" so a
	// future refactor that mistakenly hashes nil is loud.
	if digest == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatal("recorded digest equals the empty-input sha256; AnnounceDigestFor returned no entries")
	}
}
