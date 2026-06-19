package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_resync_suppression_test.go is the release-blocking
// integration check for Phase 3 review fix P1: the reconnect forced
// full sync — the PRIMARY path the digest exchange targets — must be
// suppressible by an active digest-match window, while an explicit
// request_resync / consistency-loss (hard) resync must NOT be.
//
// Before the fix the suppression gate exempted every NeedsFullResync,
// which a reconnect always sets, so the feature was inert on its
// intended path. The gate now keys off ResyncIsHard instead.

// runReconnectSuppressionCycle builds a loop whose only announce target
// (peer-C) already has a baseline equal to the current table projection,
// marks it for a resync (soft via MarkReconnected, or hard via
// MarkInvalid), arms a digest-match suppression window, runs one
// triggered cycle and returns the number of wire sends.
//
// The table holds a single direct-peer route. RefreshDirectPeers is a
// no-op, so the per-cycle projection is byte-identical to the seeded
// baseline and the delta is empty — isolating the forced-full decision
// as the only thing that can produce a send. LastFullSyncAttemptAt is
// left zero (RecordFullSyncSuccess does not set it), so the forced-full
// rate limiter never interferes.
func runReconnectSuppressionCycle(t *testing.T, hardResync bool) int {
	t.Helper()

	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	sender, rec := newControllableMockPeerSender(t)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{{Address: "addr-C", Identity: domaintest.ID("peer-C")}}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	// Seed a baseline equal to the current projection so the delta is
	// empty; the only thing that can produce a send is a forced full.
	baseline := routing.BuildAnnounceSnapshot(table.AnnounceTo(domaintest.ID("peer-C")))
	state := registry.GetOrCreate(domaintest.ID("peer-C"))
	state.RecordFullSyncSuccess(baseline, 0, now)

	if hardResync {
		registry.MarkInvalid(domaintest.ID("peer-C")) // request_resync / consistency loss
	} else {
		registry.MarkReconnected(domaintest.ID("peer-C"), nil) // session boundary
	}

	// Arm an active suppression window at the same clock the cycle
	// reads, so isDigestSuppressionActive(peer, now) is true. The gate
	// only consults the window's liveness, not its origin, so a pending
	// window suffices to exercise it.
	loop.MarkPeerDigestPending(domaintest.ID("peer-C"), now, "dig")

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

	return rec.callCount()
}

// TestAnnounceLoop_SoftReconnectResyncIsSuppressed — a reconnect
// (soft) resync with an active digest-match window must NOT emit the
// forced full sync. This is the whole point of Phase 3 incremental
// sync: the pair already agreed nothing changed.
func TestAnnounceLoop_SoftReconnectResyncIsSuppressed(t *testing.T) {
	if got := runReconnectSuppressionCycle(t, false); got != 0 {
		t.Fatalf("soft reconnect resync with active suppression sent %d frames, want 0 (forced full must be suppressed)", got)
	}
}

// TestAnnounceLoop_HardResyncBypassesSuppression — an explicit
// request_resync / consistency-loss (hard) resync must full-sync even
// with an active digest-match window. The peer is demanding a fresh
// table; a digest hint must never suppress it.
func TestAnnounceLoop_HardResyncBypassesSuppression(t *testing.T) {
	if got := runReconnectSuppressionCycle(t, true); got != 1 {
		t.Fatalf("hard request_resync with active suppression sent %d frames, want 1 (suppression must not apply)", got)
	}
}
