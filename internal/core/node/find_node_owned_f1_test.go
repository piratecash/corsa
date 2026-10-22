package node

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Tagged fixture domain, immutable before builder/FIRST literal. No Ready/Seal.
func ownedF1TestService(t *testing.T) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	return ownedF1TestServiceConfigured(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
}
func ownedF1TestServiceConfigured(t *testing.T, cfg config.Node) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	clock := new(atomic.Int64)
	d, err := findnodebridge.NewFixtureDomain(ctx.Done(), clock)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), degreeProfile{MaxActive: 2})
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done()}}
	cfg = configtest.WithTempDataDirs(t, cfg)
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b})
	// These no-Run fixtures own transport cleanup synchronously; do not
	// leave route-withdrawal AfterFunc callbacks outside the owned joins.
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { cancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if s.findNode == nil || s.findNode.owned.terminal != s.bridgeTerminal || s.findNode.owned.done != s.runCtx.Done() {
		t.Fatal("early immutable F1 adapter lost")
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("F1 root became Ready")
	}
	return s, clock, d
}

func ownedF1ContactTestService(t *testing.T) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull}
	cfg.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.SecureSessionStorePath = filepath.Join(dir, "pins.json")
	return ownedF1TestServiceConfigured(t, cfg)
}

// Test-only inspection of an already genuine Session. This callback grants no
// production class/capture authority and never runs inside publication W.
func ownedF1Opened(t *testing.T, s findnode.Session, target domain.PeerIdentity) findnode.Instant {
	t.Helper()
	called := false
	var opened findnode.Instant
	_, err := findnode.NewPrioritySnapshot(target, []findnode.Session{s}, findnode.Instant(math.MaxInt64), func(info findnode.SessionInfo) (findnode.SessionClass, error) {
		called = true
		opened = info.Opened
		return findnode.ClassOrdinary, nil
	})
	if err != nil || !called {
		t.Fatal("genuine F1 inspection failed", err)
	}
	return opened
}

func TestOwnedF1GenuineListenerSamplesAfterOwnerAndClockWaits(t *testing.T) {
	s, clock, d := ownedF1TestService(t)
	dialer, _, _ := ownedF1TestService(t)
	f := f1PairOn(t, true, nil, s, dialer)
	l := f.inbound.life
	if l.degreeWitness == nil || l.degreeWitness.origin != l || l.degreeWitness.owner != s.degree || l.bridgeTerminal != s.bridgeTerminal {
		t.Fatal("positive F1 bypassed sole genuine degree mint")
	}
	clockGuard, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			clockGuard.Release()
		}
	})
	l.mu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			l.mu.Unlock()
		}
	})
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	go func() { defer close(done); s.publishAcceptedProofTransport(f.inbound) }()
	waitFor(t, 2*time.Second, "real accepted publisher peer wait", func() bool {
		if s.peerMu.TryRLock() {
			s.peerMu.RUnlock()
			return false
		}
		return true
	})
	clock.Store(int64(5 * time.Second))
	l.mu.Unlock()
	locked = false
	waitFor(t, 2*time.Second, "accepted publisher clock wait under original common", func() bool {
		if l.mu.TryLock() {
			l.mu.Unlock()
			return false
		}
		return true
	})
	clock.Store(int64(7 * time.Second))
	clockGuard.Release()
	released = true
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("owned listener publication did not finish")
	}
	members := f1Members(t, s)
	if len(members.sessions) != 1 {
		t.Fatal("registered/authenticated owned listener absent")
	}
	target := domain.PeerIdentityFromWire(s.identity.Address)
	if got := ownedF1Opened(t, members.sessions[0], target); got != findnode.Instant(7*time.Second) {
		t.Fatal("F1 opened was sampled before final clock wait", got)
	}
	clock.Store(-1)
	s.publishAcceptedProofTransport(f.inbound)
	again := f1Members(t, s)
	if len(again.sessions) != 1 || again.sessions[0] != members.sessions[0] {
		t.Fatal("duplicate publication resampled or reminted")
	}
	if _, _, err := s.findNodePrioritySnapshot(t.Context(), target, findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4}); !errors.Is(err, findnode.ErrNotReady) {
		t.Fatal("owned F1 entered legacy priority clock", err)
	}
	w := l.degreeWitness
	l.end()
	if !l.ended || !w.closed {
		t.Fatal("clock refusal skipped mandatory genuine F1/degree End")
	}
	if _, err := findnode.NewPrioritySnapshot(target, members.sessions, findnode.Instant(math.MaxInt64), func(findnode.SessionInfo) (findnode.SessionClass, error) { return findnode.ClassOrdinary, nil }); !errors.Is(err, findnode.ErrUnauthenticated) {
		t.Fatal("End did not close original F1 handle", err)
	}
}

func TestOwnedF1ActualCMAcceptanceAndOriginalChecks(t *testing.T) {
	s, _, _ := ownedF1TestService(t)
	d, clock, domainClock := ownedF1TestService(t)
	f := f1PairOn(t, true, nil, s, d)
	p := f.outbound
	l := p.common
	if l.degreeWitness == nil || l.degreeWitness.origin != l {
		t.Fatal("CM positive has no genuine production mint")
	}
	if d.publishFindNodeCM(p, p.address, 9) {
		t.Fatal("mirrored generation fabricated accepted receipt")
	}
	start := f1PrepareActualCM(t, f, 9)
	dispatched := make(chan struct{})
	f.fixtureOwner.caseDone = dispatched
	publication := domainClock.Publisher().BeginPublication()
	released := false
	t.Cleanup(func() {
		if !released {
			publication.Complete()
		}
	})
	go func() { defer close(dispatched); start() }()
	waitFor(t, 2*time.Second, "actual CM callback peer held before registry W", func() bool {
		if d.peerMu.TryRLock() {
			d.peerMu.RUnlock()
			return false
		}
		return true
	})
	clock.Store(int64(11 * time.Second))
	publication.Complete()
	released = true
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("actual CM dispatch did not join")
	}
	waitFor(t, 2*time.Second, "actual owned CM postsetup", func() bool { return len(f1Members(t, d).sessions) == 1 })
	members := f1Members(t, d)
	target := domain.PeerIdentityFromWire(d.identity.Address)
	if got := ownedF1Opened(t, members.sessions[0], target); got != findnode.Instant(11*time.Second) {
		t.Fatal("CM used legacy independent origin", got)
	}
	d.peerMu.RLock()
	l.mu.Lock()
	generation := p.selectedGeneration
	receipt := p.findNodeReceipt
	entry := l.entry
	l.mu.Unlock()
	d.peerMu.RUnlock()
	if receipt == nil || generation == 9 || receipt.generation != generation {
		t.Fatal("actual CM receipt missing")
	}
	// Exact-original negative controls are SOURCE-only below; no live reader
	// pointer is reassigned while the actual CM fixture is serving.
	if entry == nil || !d.publishFindNodeCM(p, p.address, generation) {
		t.Fatal("restored exact CM receipt refused")
	}
	clock.Store(-1)
	if !d.publishFindNodeCM(p, p.address, generation) {
		t.Fatal("duplicate CM resampled clock")
	}
	w := l.degreeWitness
	l.end()
	if !w.closed || !l.ended || len(f1Members(t, d).sessions) != 0 {
		t.Fatal("CM End retained original F1/degree")
	}
}

func TestOwnedF1ClockRefusalStillEndsOriginalWithoutOpening(t *testing.T) {
	for _, kind := range []string{"cancel", "negative", "regression", "maximum"} {
		t.Run(kind, func(t *testing.T) {
			s, clock, d := ownedF1TestService(t)
			peer, _, _ := ownedF1TestService(t)
			f := f1PairOn(t, true, nil, s, peer)
			l := f.inbound.life
			w := l.degreeWitness
			if w == nil || w.origin != l {
				t.Fatal("refusal fixture bypassed genuine mint")
			}
			switch kind {
			case "cancel":
				s.runCancel()
			case "negative":
				clock.Store(-1)
			case "maximum":
				clock.Store(math.MaxInt64)
			case "regression":
				clock.Store(5)
				guard := d.Publisher().BeginClockedPublication()
				_, err := guard.Sample()
				guard.Complete()
				if err != nil {
					t.Fatal(err)
				}
				clock.Store(4)
			}
			s.publishAcceptedProofTransport(f.inbound)
			if len(f1Members(t, s).sessions) != 0 {
				t.Fatal("invalid clock opened new owned F1", kind)
			}
			l.end()
			if !l.ended || !w.closed || l.degreeWitness != nil || s.degree.active[w.slot].life != nil {
				t.Fatal("refused open prevented mandatory retirement", kind)
			}
		})
	}
}

// Inspect the real builder escape, not merely the returned Service.
type ownedF1InspectConstructor struct {
	t      *testing.T
	called bool
}

func (c *ownedF1InspectConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	c.called = true
	if s.findNode == nil || s.findNode.owned == nil || s.findNode.owned.terminal != s.bridgeTerminal || s.findNode.owned.done != s.runCtx.Done() || s.bridgeTerminal.owner != s.degree {
		c.t.Fatal("constructor callback escaped before owned F1 binding")
	}
	return nil, nil
}
func TestOwnedF1BindingPrecedesBuilderAndDefaultCapabilitiesStayOff(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	b, err := prepareFindNodeTerminalConstruction(degreeProfile{MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: b.ctx.Done()}}
	constructor := &ownedF1InspectConstructor{t: t}
	s := newServiceWithConstruction(cfg, id, nil, nil, constructor, serviceConstruction{bridge: b})
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if !constructor.called {
		t.Fatal("constructor escape not exercised")
	}
	accepted, _, _ := ownedF1TestService(t)
	dialer, _, _ := ownedF1TestService(t)
	f := f1PairOn(t, false, nil, accepted, dialer)
	accepted.publishAcceptedProofTransport(f.inbound)
	if f.inbound.life.findNode != nil || f.outbound.common.findNode != nil || len(f1Members(t, accepted).sessions) != 0 {
		t.Fatal("owned root advertised capability or manufactured seed")
	}
}

func TestOwnedF1CancellationAfterClockWaitDoesNotBind(t *testing.T) {
	s, _, d := ownedF1TestService(t)
	peer, _, _ := ownedF1TestService(t)
	f := f1PairOn(t, true, nil, s, peer)
	l := f.inbound.life
	w := l.degreeWitness
	if w == nil || w.origin != l {
		t.Fatal("cancel fixture lacks genuine P1")
	}
	clock, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			clock.Release()
		}
	})
	done := make(chan struct{})
	f.fixtureOwner.caseDone = done
	go func() { defer close(done); s.publishAcceptedProofTransport(f.inbound) }()
	waitFor(t, 2*time.Second, "new open blocked on original clock", func() bool {
		if l.mu.TryLock() {
			l.mu.Unlock()
			return false
		}
		return true
	})
	s.runCancel()
	clock.Release()
	released = true
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled publication did not finish")
	}
	l.mu.Lock()
	published := l.findNode.published
	l.mu.Unlock()
	if published {
		t.Fatal("post-wait cancellation consumed and published F1")
	}
	l.end()
	if !w.closed || !l.ended {
		t.Fatal("post-wait refusal blocked mandatory End")
	}
}
