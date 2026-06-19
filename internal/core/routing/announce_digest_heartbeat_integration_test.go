package routing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// heartbeatClock is a mutex-guarded test clock so the announce loop's
// background goroutine can read `now` while the test advances it.
type heartbeatClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *heartbeatClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *heartbeatClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestAnnounceLoop_DigestHeartbeat_NoFullOnIntermediateTick is the review-fix
// P2 integration check: after a confirmed heartbeat, the next deadline is
// digest-only; a new (still-unconfirmed) heartbeat must NOT escalate to a full
// on the intermediate delta tick before the next cadence. The fallback full
// fires only at the NEXT freshness deadline. Without the `due` gate, the
// frozen LastSuccessfulFullSyncAt keeps the outer deadline true on every tick
// and an in-flight heartbeat re-escalates one tick later.
func TestAnnounceLoop_DigestHeartbeat_NoFullOnIntermediateTick(t *testing.T) {
	clk := &heartbeatClock{t: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)}
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clk.now))

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	var (
		mu          sync.Mutex
		fullCalls   int
		digestCalls int
		lastDigest  string
	)
	read := func() (int, int, string) {
		mu.Lock()
		defer mu.Unlock()
		return fullCalls, digestCalls, lastDigest
	}

	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, _ []routing.AnnounceEntry) bool {
			mu.Lock()
			fullCalls++
			mu.Unlock()
			return true
		}).Maybe()
	m.EXPECT().SendRouteSyncDigest(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, digest string, _ uint32, _ time.Time) bool {
			mu.Lock()
			digestCalls++
			lastDigest = digest
			mu.Unlock()
			return true
		}).Maybe()

	peerC := domaintest.ID("peer-C")
	caps := []routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRouteSyncV1}
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{{Address: "addr-C", Identity: peerC, Capabilities: caps}}
	}

	loop := routing.NewAnnounceLoop(table, m, peers,
		routing.WithAnnounceInterval(10*time.Second), // cadence = min(10*10, TTL/2) = 100s
		routing.WithStateRegistry(registry),
	)
	cadence := routing.EffectiveForcedFullSyncInterval(10 * time.Second)

	// Seed a baseline equal to the current projection + cursor at head, so the
	// cursor delta is empty and only the freshness path can produce a send.
	raw, head := table.AnnounceToWithChangeHead(peerC)
	baseline := routing.BuildAnnounceSnapshot(raw)
	table.ReleaseAnnounceEntries(raw)
	registry.GetOrCreate(peerC).RecordFullSyncSuccess(baseline, head, clk.now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor := func(cond func() bool, msg string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		f, d, _ := read()
		t.Fatalf("%s (fullCalls=%d digestCalls=%d)", msg, f, d)
	}

	// Cycle 1 — first periodic deadline. No entry yet → bootstrap full + heartbeat.
	clk.advance(cadence)
	loop.TriggerUpdate()
	waitFor(func() bool { f, d, _ := read(); return f == 1 && d == 1 }, "cycle 1 must emit one bootstrap full + one heartbeat")

	// Confirm the heartbeat so the next deadline is digest-only.
	_, _, d1 := read()
	if !loop.ConfirmPeerDigestMatch(peerC, d1, clk.now()) {
		t.Fatal("ConfirmPeerDigestMatch should accept the correlated heartbeat echo")
	}

	// Cycle 2 — confirmed match covers freshness → digest-only, NO full.
	clk.advance(cadence)
	loop.TriggerUpdate()
	waitFor(func() bool { _, d, _ := read(); return d == 2 }, "cycle 2 must emit a second heartbeat")
	if f, _, _ := read(); f != 1 {
		t.Fatalf("a confirmed-match deadline must be digest-only, got fullCalls=%d", f)
	}

	// Do NOT confirm this second heartbeat. Intermediate delta tick BEFORE the
	// next cadence: the outer deadline stays true (LastSuccessfulFullSyncAt
	// frozen), but `due` is false, so no full and no new heartbeat.
	clk.advance(cadence / 2)
	loop.TriggerUpdate()
	time.Sleep(100 * time.Millisecond) // let the cycle fully run
	if f, d, _ := read(); f != 1 || d != 2 {
		t.Fatalf("intermediate tick before cadence must not escalate (P2): fullCalls=%d digestCalls=%d, want 1,2", f, d)
	}

	// Next cadence deadline: the unconfirmed heartbeat now escalates to a full.
	clk.advance(cadence)
	loop.TriggerUpdate()
	waitFor(func() bool { f, _, _ := read(); return f == 2 }, "unconfirmed heartbeat must escalate to a full at the next cadence deadline")
}

// TestAnnounceLoop_DigestHeartbeat_NoDigestForPeerWithoutRouteSyncCap is the
// review-fix P3 check: a peer that did not negotiate mesh_route_sync_v1 must
// never receive a digest (and never pay the AnnounceDigestFor walk) — the
// periodic deadline falls straight through to the unconditional full.
func TestAnnounceLoop_DigestHeartbeat_NoDigestForPeerWithoutRouteSyncCap(t *testing.T) {
	clk := &heartbeatClock{t: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)}
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clk.now))

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	var (
		mu          sync.Mutex
		fullCalls   int
		digestCalls int
	)
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, _ []routing.AnnounceEntry) bool {
			mu.Lock()
			fullCalls++
			mu.Unlock()
			return true
		}).Maybe()
	m.EXPECT().SendRouteSyncDigest(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, _ string, _ uint32, _ time.Time) bool {
			mu.Lock()
			digestCalls++
			mu.Unlock()
			return true
		}).Maybe()

	peerC := domaintest.ID("peer-C")
	// v1 only — NO mesh_route_sync_v1.
	caps := []routing.PeerCapability{domain.CapMeshRoutingV1}
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{{Address: "addr-C", Identity: peerC, Capabilities: caps}}
	}

	loop := routing.NewAnnounceLoop(table, m, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)
	cadence := routing.EffectiveForcedFullSyncInterval(10 * time.Second)

	raw, head := table.AnnounceToWithChangeHead(peerC)
	baseline := routing.BuildAnnounceSnapshot(raw)
	table.ReleaseAnnounceEntries(raw)
	registry.GetOrCreate(peerC).RecordFullSyncSuccess(baseline, head, clk.now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	clk.advance(cadence)
	loop.TriggerUpdate()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		f := fullCalls
		mu.Unlock()
		if f >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	f, d := fullCalls, digestCalls
	mu.Unlock()
	if f != 1 {
		t.Fatalf("a route_sync-incapable peer must still get the unconditional full, got fullCalls=%d", f)
	}
	if d != 0 {
		t.Fatalf("a route_sync-incapable peer must never receive a digest, got digestCalls=%d", d)
	}
}
