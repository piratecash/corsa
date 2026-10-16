package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// pastEveryLimiterHorizon is far enough past every limiter's idle horizon
// that a single sweep must leave nothing behind. The horizon check in the
// wiring test keeps it honest when a horizon grows.
const pastEveryLimiterHorizon = time.Hour

// Every per-key limiter the Service owns is swept by the very method
// bootstrapLoop's tick runs, so no key a peer (or a spoofed source address)
// once touched outlives its horizon. The limiters come from their production
// constructors; only the clock is the test's.
func TestSweepIdleRateLimiters_ReclaimsEveryLimiterPastItsHorizon(t *testing.T) {
	t.Parallel()
	clock := newManualTestClock()
	svc := &Service{
		relayLimiter:    newRelayRateLimiter(clock.now),
		announceLimiter: newAnnounceRateLimiter(clock.now),
		connLimiter:     newConnRateLimiter(clock.now),
		cmdLimiter:      newCommandRateLimiter(clock.now),
		loopbackInbound: newLoopbackInboundClass(productionLoopbackInboundBounds(0), productionIdentityPenaltyPolicy(), clock.now),
	}
	for name, horizon := range map[string]time.Duration{
		"relay":    svc.relayLimiter.buckets.sweepAfter,
		"announce": svc.announceLimiter.buckets.sweepAfter,
		"conn":     svc.connLimiter.windowSize,
		"command":  svc.cmdLimiter.buckets.sweepAfter,
	} {
		if horizon >= pastEveryLimiterHorizon {
			t.Fatalf("%s horizon %v is not below the %v this test advances", name, horizon, pastEveryLimiterHorizon)
		}
	}

	const (
		peer    = domain.PeerAddress("10.0.0.1:64646")
		ip      = "198.51.100.7"
		connKey = "198.51.100.7:50000"
	)
	subject := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	svc.relayLimiter.allow(peer)
	svc.announceLimiter.allow(subject, 1)
	svc.connLimiter.allowConnect(ip)
	svc.cmdLimiter.allowCommand(connKey)

	clock.advance(pastEveryLimiterHorizon)
	svc.sweepIdleRateLimiters()

	if relayLimiterHasBucket(svc.relayLimiter, peer) {
		t.Error("relay bucket survived a sweep past its horizon")
	}
	if announceLimiterHasBucket(svc.announceLimiter, subject) {
		t.Error("announce bucket survived a sweep past its horizon")
	}
	if connLimiterHasWindow(svc.connLimiter, ip) {
		t.Error("connection window survived a sweep past its horizon: every source IP ever seen stays in memory")
	}
	if commandLimiterHasBucket(svc.cmdLimiter, connKey) {
		t.Error("command bucket survived a sweep past its horizon: a bucket no close path removed stays in memory")
	}
}

// The sweep is reached from bootstrapLoop itself, not only from a method a
// test calls by name: one tick of the real loop, on a Service from
// NewService, reclaims every limiter entry past its horizon. Without this a
// tick that stopped calling the sweep would leave every other test green and
// every source IP in memory for the life of the process.
func TestBootstrapLoopTick_SweepsEveryRateLimiter(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	// The limiters are swapped for ones on the test's clock before the loop
	// starts, so nothing reads them concurrently with the swap.
	clock := newManualTestClock()
	svc.relayLimiter = newRelayRateLimiter(clock.now)
	svc.announceLimiter = newAnnounceRateLimiter(clock.now)
	svc.connLimiter = newConnRateLimiter(clock.now)
	svc.cmdLimiter = newCommandRateLimiter(clock.now)
	ticks := make(chan time.Time)
	svc.bootstrapTickOverride = ticks

	const (
		peer    = domain.PeerAddress("10.0.0.1:64646")
		ip      = "198.51.100.7"
		connKey = "198.51.100.7:50000"
	)
	subject := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	svc.relayLimiter.allow(peer)
	svc.announceLimiter.allow(subject, 1)
	svc.connLimiter.allowConnect(ip)
	svc.cmdLimiter.allowCommand(connKey)
	clock.advance(pastEveryLimiterHorizon)

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		svc.bootstrapLoop(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-loopDone
	})
	sendTick := func() {
		select {
		case ticks <- clock.now():
		case <-time.After(10 * time.Second):
			t.Fatal("bootstrapLoop did not take a tick")
		}
	}
	// The second send is accepted only once the loop has finished the first
	// tick's work.
	sendTick()
	sendTick()
	cancel()
	<-loopDone

	if relayLimiterHasBucket(svc.relayLimiter, peer) {
		t.Error("a bootstrapLoop tick left a relay bucket past its horizon")
	}
	if announceLimiterHasBucket(svc.announceLimiter, subject) {
		t.Error("a bootstrapLoop tick left an announce bucket past its horizon")
	}
	if connLimiterHasWindow(svc.connLimiter, ip) {
		t.Error("a bootstrapLoop tick left a connection window past its horizon")
	}
	if commandLimiterHasBucket(svc.cmdLimiter, connKey) {
		t.Error("a bootstrapLoop tick left a command bucket past its horizon")
	}
}
