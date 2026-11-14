package node

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

type loadStandFullProjection struct {
	sender       domain.PeerIdentity
	epoch        uint64
	expected     map[domain.PeerIdentity]loadStandProjectedRoute
	wireReceived map[domain.PeerIdentity]bool
	invalid      bool
}

type loadStandProjectedRoute struct {
	hops int
	seq  uint64
}

type loadStandFullProjectionReceipt struct {
	Destinations int
	IdentitySHA  loadstand.LaunchFingerprint
}

// This verifier takes the exact per-peer projection from one full sync.
// Calling the projection builder again could restamp sequence numbers.
func newLoadStandFullProjection(sender, receiver domain.PeerIdentity, epoch uint64, entries []routing.AnnounceEntry) (*loadStandFullProjection, error) {
	if sender.IsZero() || receiver.IsZero() || sender == receiver || epoch == 0 || len(entries) == 0 || len(entries) > announceBurstRoutesPerPeer {
		return nil, loadstand.ErrInvalidConfiguration
	}
	p := &loadStandFullProjection{
		sender: sender, epoch: epoch,
		expected: make(map[domain.PeerIdentity]loadStandProjectedRoute, len(entries)), wireReceived: make(map[domain.PeerIdentity]bool, len(entries)),
	}
	for _, entry := range entries {
		if _, duplicate := p.expected[entry.Identity]; duplicate || entry.Identity.IsZero() ||
			entry.Identity == sender || entry.Identity == receiver || entry.Hops < 1 || entry.Hops >= routing.HopsInfinity-1 {
			return nil, loadstand.ErrInvalidConfiguration
		}
		p.expected[entry.Identity] = loadStandProjectedRoute{hops: entry.Hops, seq: entry.SeqNo}
	}
	return p, nil
}

func (p *loadStandFullProjection) accept(frame protocol.RouteAnnounceV3Frame) error {
	if p.invalid || frame.Type != protocol.RouteAnnounceV3FrameType || frame.Kind != protocol.RouteAnnounceV3KindFull ||
		frame.Epoch != p.epoch || len(frame.Entries) == 0 {
		p.invalid = true
		return loadstand.ErrFinalReportInvalid
	}
	for _, entry := range frame.Entries {
		id := domain.PeerIdentityFromWire(entry.Identity)
		want, found := p.expected[id]
		if !found || p.wireReceived[id] || int(entry.Hops) != want.hops || entry.SeqNo != want.seq {
			p.invalid = true
			return fmt.Errorf("%w: unexpected full entry for %s", loadstand.ErrFinalReportInvalid, id)
		}
		p.wireReceived[id] = true
	}
	return nil
}

// receipt checks the original uplink claim in a single receiver snapshot.
// Other paths to the same destinations do not satisfy this check.
func (p *loadStandFullProjection) receipt(snapshot routing.Snapshot) (loadStandFullProjectionReceipt, error) {
	if p.invalid {
		return loadStandFullProjectionReceipt{}, loadstand.ErrFinalReportInvalid
	}
	if snapshot.TakenAt.IsZero() || len(p.wireReceived) != len(p.expected) {
		return loadStandFullProjectionReceipt{}, loadstand.ErrFinalAccountingUnavailable
	}
	ids := slices.SortedFunc(maps.Keys(p.expected), func(a, b domain.PeerIdentity) int { return bytes.Compare(a[:], b[:]) })
	digest := sha256.New()
	for _, id := range ids {
		want := p.expected[id]
		found := false
		for _, row := range snapshot.Routes[id] {
			if row.NextHop != p.sender {
				continue
			}
			if found || row.Identity != id || row.Source != routing.RouteSourceAnnouncement || row.Hops != want.hops+1 ||
				row.SeqNo != want.seq || row.IsWithdrawn() || row.ExpiresAt.IsZero() || row.IsExpired(snapshot.TakenAt) {
				return loadStandFullProjectionReceipt{}, fmt.Errorf("%w: wrong original claim for %s", loadstand.ErrFinalReportInvalid, id)
			}
			found = true
		}
		if !found {
			return loadStandFullProjectionReceipt{}, fmt.Errorf("%w: missing original claim for %s", loadstand.ErrFinalAccountingUnavailable, id)
		}
		_, _ = digest.Write(id[:])
	}
	return loadStandFullProjectionReceipt{Destinations: len(ids), IdentitySHA: loadstand.LaunchFingerprint(digest.Sum(nil))}, nil
}

func TestLoadStandRouteWireLargeProjectionAndReceiverReceipt(t *testing.T) {
	const count = 10_000
	sender, receiver, transit := idPeerB, domain.PeerIdentity{0xcc, 1}, loadstandRouteWireSeedIdentity(0)
	table := routing.NewTable(routing.WithLocalOrigin(sender))
	for _, n := range []int{-1, 0, count + 1} {
		if err := seedLoadstandRouteWireTable(table, sender, n); !errors.Is(err, errRouteWireIncomplete) {
			t.Fatalf("invalid seed size %d: %v", n, err)
		}
	}
	if err := seedLoadstandRouteWireTable(table, transit, count); !errors.Is(err, errRouteWireIncomplete) {
		t.Fatalf("fixture identity accepted as local origin: %v", err)
	}
	if err := seedLoadstandRouteWireTable(table, sender, count); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer(receiver); err != nil {
		t.Fatal(err)
	}
	excluded := domain.PeerIdentity{0xcc, 3}
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: excluded, Origin: receiver, NextHop: receiver, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("seed route via receiver: %v, %v", status, err)
	}
	raw, _ := table.AnnounceToWithChangeHead(receiver)
	defer table.ReleaseAnnounceEntries(raw)
	entries := routing.BuildAnnounceSnapshot(raw).Entries
	if len(entries) != count {
		t.Fatalf("per-peer projection has %d rows, want %d", len(entries), count)
	}
	epoch := table.Epoch()
	proof, err := newLoadStandFullProjection(sender, receiver, epoch, entries)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := proof.expected[excluded]; exists {
		t.Fatal("split horizon retained the claim through the receiver")
	}

	service := newTestServiceWithRouting(t, receiver)
	service.eventBus = newStormBus(t)
	fixed := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	service.announceLimiter = newAnnounceRateLimiter(func() time.Time { return fixed })
	service.announceLoop.StateRegistry().MarkReconnected(sender, []routing.PeerCapability{
		domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1,
	})
	receive := service.provenRoutingSender(sender)
	now := time.Duration(1)
	window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	obs := &LoadstandRouteWireObservation{window: window, fullBudget: count, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
	const connID domain.ConnID = 1
	sessionID := [32]byte{1}
	obs.begin(connID)
	obs.bind(connID, sender, domain.PeerDirectionInbound, sessionID)
	now = 15
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, epoch, protocol.MaxFrameLine)
	if len(chunks) != 100 || len(skipped) != 0 {
		t.Fatalf("full projection: %d chunks, %d skips", len(chunks), len(skipped))
	}
	for i, chunk := range chunks {
		wire, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, epoch, chunk)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(wire.RawLine))
		if err != nil {
			t.Fatal(err)
		}
		if err := proof.accept(frame); err != nil {
			t.Fatal(err)
		}
		obs.observe(connID, routeWireRead, wire.RawLine, true, 11)
		if i == len(chunks)-1 {
			if got, err := proof.receipt(service.routingTable.Snapshot()); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) || got != (loadStandFullProjectionReceipt{}) {
				t.Fatalf("wire receipt without applying the last chunk counted as complete: %+v, %v", got, err)
			}
		}
		service.handleRouteAnnounceV3(receive, "addr-peerB", frame)
	}
	snapshot := service.routingTable.Snapshot()
	receipt, err := proof.receipt(snapshot)
	const wantDigest = "168c3502e786c934c14c464e8ef003d331a273ff9ca636d9109ee516340d42d8"
	if err != nil || receipt.Destinations != count || receipt.IdentitySHA.String() != wantDigest || loadstandRouteWireSeedDigest(count) != wantDigest {
		t.Fatalf("receiver receipt: %+v, %v", receipt, err)
	}
	if err := obs.captureFullReceipts(service.routingTable.Snapshot); !errors.Is(err, errRouteWireIncomplete) {
		t.Fatalf("captured before end of window: %v", err)
	}
	now = 21
	if err := obs.captureFullReceipts(service.routingTable.Snapshot); err != nil {
		t.Fatal(err)
	}
	obs.finish(connID, routeWireRead)
	obs.finish(connID, routeWireWrite)
	rows, err := obs.SealWindow([]RouteWireCensusEndpoint{{
		ConnID: connID, Peer: sender, Direction: domain.PeerDirectionInbound, SessionID: sessionID,
		Interval: domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20},
	}})
	if err != nil || len(rows) != 1 || rows[0].FullReceive == nil ||
		*rows[0].FullReceive != (domain.RouteWireFullReceipt{Destinations: count, IdentitySHA256: receipt.IdentitySHA.String(), Epoch: epoch, SnapshotAfterNS: 21}) ||
		rows[0].Received.FullV3.Frames != uint64(len(chunks)) {
		t.Fatalf("sealed route receipt: %+v, %v", rows, err)
	}
	if !receive.receive.HasReceivedBaseline() || announceLimiterTokens(t, service.announceLimiter, receive.penalty) != 0 {
		t.Fatal("receiver did not establish baseline under the normal route budget")
	}
	target := loadstandRouteWireSeedIdentity(1)
	row := &snapshot.Routes[target][0]
	row.SeqNo++
	if _, err := proof.receipt(snapshot); !errors.Is(err, loadstand.ErrFinalReportInvalid) {
		t.Fatalf("different sequence satisfied receiver receipt: %v", err)
	}
	row.SeqNo--
	row.NextHop = transit
	if _, err := proof.receipt(snapshot); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
		t.Fatalf("another uplink satisfied receiver receipt: %v", err)
	}
	row.NextHop = sender
	repeated := protocol.RouteAnnounceV3Frame{
		Type: protocol.RouteAnnounceV3FrameType, Kind: protocol.RouteAnnounceV3KindFull, Epoch: epoch,
		Entries: []protocol.RouteAnnounceV3Entry{{Identity: target.String(), Hops: uint8(proof.expected[target].hops), SeqNo: proof.expected[target].seq}},
	}
	if err := proof.accept(repeated); !errors.Is(err, loadstand.ErrFinalReportInvalid) {
		t.Fatalf("duplicate full entry accepted: %v", err)
	}
	if _, err := proof.receipt(snapshot); !errors.Is(err, loadstand.ErrFinalReportInvalid) {
		t.Fatalf("invalid transfer produced a receipt: %v", err)
	}
	t.Logf("projection_destinations=%d full_frames=%d receiver_identity_sha256=%x", receipt.Destinations, len(chunks), receipt.IdentitySHA)
}

func TestLoadStandRouteWireFullReceiptRecordsOnlyStableClaimsWithinBudget(t *testing.T) {
	const a = "aa00000000000000000000000000000000000001"
	const b = "aa00000000000000000000000000000000000002"
	frame := func(id string, epoch, seq uint64) string {
		return fmt.Sprintf("{\"type\":\"route_announce_v3\",\"kind\":\"full\",\"epoch\":%d,\"entries\":[{\"identity\":%q,\"hops\":2,\"seq_no\":%d}]}\n", epoch, id, seq)
	}
	for _, tc := range []struct{ name, line string }{
		{"second unique claim over budget", frame(b, 1, 1)},
		{"changed sequence", frame(a, 1, 2)},
		{"changed epoch", frame(a, 2, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			const connID domain.ConnID = 1
			o := &LoadstandRouteWireObservation{window: window, fullBudget: 1, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			o.begin(connID)
			o.bind(connID, idPeerB, domain.PeerDirectionInbound, [32]byte{1})
			now = 15
			stable := frame(a, 1, 1)
			o.observe(connID, routeWireRead, stable, true, 11)
			o.observe(connID, routeWireRead, stable, true, 13)
			if o.incomplete || o.fullRows != 1 || o.connections[connID].counts[routeWireRead][routeWireFullV3].frames != 2 {
				t.Fatal("stable repeat lost the frame count or consumed another route slot")
			}
			o.observe(connID, routeWireRead, tc.line, true, 14)
			if !o.incomplete || o.fullRows != 1 {
				t.Fatal("invalid claim did not invalidate the report")
			}
		})
	}
}
