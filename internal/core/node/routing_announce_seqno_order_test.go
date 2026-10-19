package node

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// A peer can deliver the same route over route_announce_v3 (where the
// receiver synthesises Origin = sender) and over legacy announce_routes
// (where the sender writes Origin = its own identity on the wire). Both
// land in one (Identity, sender) slot with the same Origin, so the
// legacy frame at the same SeqNo and hops is a reconfirmation that
// refreshes the TTL, and a lower SeqNo is stale whichever frame carries
// it.
func TestAnnounceIngest_V3ThenLegacySameSlotIsSeqNoOrdered(t *testing.T) {
	svc, now := newSeqOrderFixture(t)
	const ttl = seqOrderFixtureTTL

	sendV3Full(svc, idTargetX, 2, 5)
	assertStoredRoute(t, svc, idTargetX, 5, 3, now.Add(ttl))

	*now = now.Add(30 * time.Second)
	svc.handleAnnounceRoutes(svc.provenRoutingSender(idPeerB), legacyAnnounce(idTargetX, idPeerB, 2, 5))
	assertStoredRoute(t, svc, idTargetX, 5, 3, now.Add(ttl))

	refreshedAt := *now
	*now = now.Add(30 * time.Second)
	svc.handleAnnounceRoutes(svc.provenRoutingSender(idPeerB), legacyAnnounce(idTargetX, idPeerB, 1, 4))
	assertStoredRoute(t, svc, idTargetX, 5, 3, refreshedAt.Add(ttl))
}

// Every supported sender writes its own identity as the Origin of each
// legacy entry, so a live entry naming another Origin is refused on
// receive exactly like a withdrawal with a foreign Origin — whatever its
// SeqNo and hops, and whether or not the slot already exists.
func TestAnnounceIngest_LegacyLiveEntryWithForeignOriginRejected(t *testing.T) {
	svc, now := newSeqOrderFixture(t)
	const ttl = seqOrderFixtureTTL

	sendV3Full(svc, idTargetX, 2, 5)
	assertStoredRoute(t, svc, idTargetX, 5, 3, now.Add(ttl))
	storedAt := *now

	*now = now.Add(30 * time.Second)
	svc.handleAnnounceRoutes(svc.provenRoutingSender(idPeerB), protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: idOriginC.String(), Hops: 0, SeqNo: 4},
			{Identity: idTargetX.String(), Origin: idOriginC.String(), Hops: 0, SeqNo: 6},
			{Identity: idTargetY.String(), Origin: idOriginC.String(), Hops: 1, SeqNo: 1},
		},
	})

	assertStoredRoute(t, svc, idTargetX, 5, 3, storedAt.Add(ttl))
	if routes := svc.routingTable.Lookup(idTargetY); len(routes) != 0 {
		t.Fatalf("live entry with a foreign Origin must not create a route, got %+v", routes)
	}
}

// A sender can fill one frame with entries the receiver refuses — foreign
// Origins, transit withdrawals, malformed addresses, entries the table
// refuses as invalid (negative hops) — and repeat it at will.
// Each reason produces ONE Warn per frame carrying the count and the first
// refused entry, not one Warn per entry; every entry is still counted as
// rejected and refused.
func TestAnnounceIngest_RefusedEntriesLogOneWarnPerReasonPerFrame(t *testing.T) {
	// NOT parallel: it inspects the global zerolog output.
	var buf syncWriter
	origLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Logger()
	defer func() { log.Logger = origLogger }()

	svc, _ := newSeqOrderFixture(t)
	foreign := idOriginC.String()
	svc.handleAnnounceRoutes(svc.provenRoutingSender(idPeerB), protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: foreign, Hops: 0, SeqNo: 1},
			{Identity: idTargetY.String(), Origin: foreign, Hops: 1, SeqNo: 1},
			{Identity: idPeerD.String(), Origin: foreign, Hops: 2, SeqNo: 1},
			{Identity: idTargetX.String(), Origin: foreign, Hops: routing.HopsInfinity, SeqNo: 2},
			{Identity: idTargetY.String(), Origin: foreign, Hops: routing.HopsInfinity, SeqNo: 2},
			{Identity: strings.Repeat("not-a-fingerprint", 64), Origin: idPeerB.String(), Hops: 0, SeqNo: 1},
			{Identity: idTargetX.String(), Origin: idPeerB.String(), Hops: -1, SeqNo: 3},
			{Identity: idTargetY.String(), Origin: idPeerB.String(), Hops: -1, SeqNo: 3},
		},
	})

	warns := warnSummaries(t, buf.String())
	want := map[string]int{
		"announce_rejected_foreign_origin":     3,
		"announce_rejected_transit_withdrawal": 2,
		"announce_rejected_malformed_address":  1,
		"route_update_rejected_invalid":        2,
	}
	for event, count := range want {
		got := warns[event]
		if len(got) != 1 {
			t.Fatalf("%s: %d Warn lines for one frame, want exactly 1", event, len(got))
		}
		if got[0].Count != count {
			t.Fatalf("%s: Warn reports count=%d, want %d", event, got[0].Count, count)
		}
		if len(got[0].ExampleIdentity) > maxRefusedFieldLogBytes {
			t.Fatalf("%s: example_identity is %d bytes, want at most %d", event, len(got[0].ExampleIdentity), maxRefusedFieldLogBytes)
		}
	}
	for _, target := range []domain.PeerIdentity{idTargetX, idTargetY, idPeerD} {
		if routes := svc.routingTable.Lookup(target); len(routes) != 0 {
			t.Fatalf("a refused entry created a route to %s: %+v", target, routes)
		}
	}
}

// refusalWarn is the part of a refusal summary line the test checks.
type refusalWarn struct {
	Count           int    `json:"count"`
	ExampleIdentity string `json:"example_identity"`
}

// warnSummaries returns, per Warn event in the captured zerolog output, the
// summary fields of each line (zero when the line has none).
func warnSummaries(t *testing.T, captured string) map[string][]refusalWarn {
	t.Helper()
	summaries := make(map[string][]refusalWarn)
	scanner := bufio.NewScanner(strings.NewReader(captured))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		var line struct {
			refusalWarn
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("unparseable log line %q: %v", scanner.Text(), err)
		}
		if line.Level == zerolog.WarnLevel.String() {
			summaries[line.Message] = append(summaries[line.Message], line.refusalWarn)
		}
	}
	return summaries
}

const seqOrderFixtureTTL = 120 * time.Second

// newSeqOrderFixture builds a routing Service whose table runs on the
// returned mutable clock, with idPeerB registered as a v3-capable peer.
func newSeqOrderFixture(t *testing.T) (*Service, *time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(idNodeA),
		routing.WithClock(func() time.Time { return now }),
		routing.WithDefaultTTL(seqOrderFixtureTTL),
	)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)
	svc.eventBus = newStormBus(t)
	svc.announceLoop.StateRegistry().MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
	return svc, &now
}

func sendV3Full(svc *Service, target domain.PeerIdentity, wireHops uint8, seqNo uint64) {
	svc.handleRouteAnnounceV3(svc.provenRoutingSender(idPeerB), domain.PeerAddress("addr-peerB"), protocol.RouteAnnounceV3Frame{
		Kind:    protocol.RouteAnnounceV3KindFull,
		Epoch:   1,
		Entries: []protocol.RouteAnnounceV3Entry{{Identity: target.String(), Hops: wireHops, SeqNo: seqNo}},
	})
}

func legacyAnnounce(target, sender domain.PeerIdentity, wireHops int, seqNo uint64) protocol.Frame {
	return protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: target.String(), Origin: sender.String(), Hops: wireHops, SeqNo: seqNo},
		},
	}
}

// assertStoredRoute checks the single route to target via idPeerB:
// hops include the receiver's own +1.
func assertStoredRoute(t *testing.T, svc *Service, target domain.PeerIdentity, seqNo uint64, hops int, expiresAt time.Time) {
	t.Helper()
	routes := svc.routingTable.Lookup(target)
	if len(routes) != 1 {
		t.Fatalf("expected one route to %s, got %+v", target, routes)
	}
	got := routes[0]
	if got.NextHop != idPeerB || got.SeqNo != seqNo || got.Hops != hops || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("stored route = {NextHop:%s SeqNo:%d Hops:%d ExpiresAt:%v}, want {NextHop:%s SeqNo:%d Hops:%d ExpiresAt:%v}",
			got.NextHop, got.SeqNo, got.Hops, got.ExpiresAt, idPeerB, seqNo, hops, expiresAt)
	}
}
