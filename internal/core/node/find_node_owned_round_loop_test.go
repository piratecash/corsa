package node

import (
	"bufio"
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func ownedExecutingTestService(t *testing.T) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	return ownedExecutingTestServiceWorker(t, true)
}
func ownedExecutingTestServiceWorker(t *testing.T, worker bool) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := new(atomic.Int64)
	var service *Service
	t.Cleanup(func() {
		cancel()
		if service != nil {
			service.closeBackgroundAdmission()
			service.WaitBackground()
		}
	})
	d, err := findnodebridge.NewFixtureDomain(ctx.Done(), clock)
	if err != nil {
		t.Fatal(err)
	}
	profile := findNodeBridgeProfile{Capture: findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 8}, Priority: findNodePriorityProfile{MaxMembers: 8, MaxRegistryVisits: 16}}, Intake: findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Capture.Degree)
	intake, sink, err := findnode.NewExecutingFixtureIntake(d.Consumer(), d.Observer(), profile.Intake, 8)
	if err != nil {
		t.Fatal(err)
	}
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Capture.Priority, intake: intake, sink: sink}
	if worker {
		b.capture.roundLoop = newOwnedFindNodeRoundLoop(ctx.Done(), profile.Intake.Round)
	}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done(), retirement: intake}}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	dir := t.TempDir()
	cfg.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.SecureSessionStorePath = filepath.Join(dir, "pins.json")
	service = newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b})
	service.routeWithdrawalGracePeriodTest = -1
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	return service, clock, d
}
func ownedExecutingIngress(t *testing.T, s *Service, owner *proofTransportOwner, target findnode.Session) error {
	t.Helper()
	wire, _ := ownedReceiveWire(t, target.Identity())
	receipt, err := s.recordOwnedFindNodeReceive(owner, wire)
	if err != nil {
		t.Fatal(err)
	}
	// Same post-admission dispatcher used by both actual bounded read loops.
	return s.dispatchOwnedFindNodeReceive(receipt)
}
func ownedExecutingReadForward(t *testing.T, f f1ProofFixture, target domain.PeerIdentity) protocol.FindNodeRequest {
	t.Helper()
	if err := f.outbound.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(f.outbound.conn)
	for i := 0; i < 32; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal("original target did not receive scheduled write", err)
		}
		request, err := protocol.ParseFindNodeRequest([]byte(line))
		if err == nil {
			if request.Target != target {
				t.Fatal("writer selected different target")
			}
			return request
		}
	}
	t.Fatal("no strict FIND_NODE in bounded original target frames")
	return protocol.FindNodeRequest{}
}

func TestOwnedRoundLoopIdleLastRequestReachesOriginalWriter(t *testing.T) {
	s, clock, _ := ownedExecutingTestService(t)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	remote, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	source, _ := ownedRawAccepted(t, s, from)
	targetWire, target := ownedRawAccepted(t, s, remote)
	clock.Store(int64(3 * time.Second))
	if err := ownedExecutingIngress(t, s, source.inbound, target); err != nil {
		t.Fatal(err)
	}
	if !s.bridgeCapture.intake.OwnedRoundPending() {
		t.Fatal("actual offer did not open original round")
	}
	// No second request wakes the owner. The fixed-cut timer alone must drive it.
	clock.Store(int64(3*time.Second + time.Millisecond))
	got := ownedExecutingReadForward(t, targetWire, target.Identity())
	if got.ID != 0 {
		t.Fatal("actual C3a original forwarded ID absent", got.ID)
	}
	waitFor(t, 2*time.Second, "original round closed after idle request", func() bool { return !s.bridgeCapture.intake.OwnedRoundPending() })
	loop := s.bridgeCapture.roundLoop
	for i := 0; i < 100; i++ {
		s.signalOwnedFindNodeRound()
	}
	if !loop.started.Load() || cap(loop.wake) != 1 {
		t.Fatal("original bounded one-worker wake contract lost")
	}
}
func TestOwnedRoundLoopRefusedProtectedNewcomerClosesEmptyInterval(t *testing.T) {
	s, clock, _ := ownedExecutingTestService(t)
	a, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	b, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	remote, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	if result := addLocalContact(t, s, a.identity); result.Fact != domain.LocalContactFactDurableConfirmed {
		t.Fatal(result)
	}
	first, _ := ownedRawAccepted(t, s, a)
	_, target := ownedRawAccepted(t, s, remote)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(3 * time.Second))
	if err := ownedExecutingIngress(t, s, first.inbound, target); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(3*time.Second + time.Millisecond))
	waitFor(t, 2*time.Second, "first protected sweep collection closed", func() bool { return !s.bridgeCapture.intake.OwnedRoundPending() })
	if result := addLocalContact(t, s, b.identity); result.Fact != domain.LocalContactFactDurableConfirmed {
		t.Fatal(result)
	}
	newcomer, member := ownedRawAccepted(t, s, b)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if member.ConnID() <= target.ConnID() {
		t.Fatal("fixture did not create protected newcomer beyond old watermark")
	}
	if err := ownedExecutingIngress(t, s, newcomer.inbound, target); !errors.Is(err, findnode.ErrNotReady) {
		t.Fatal("fixture did not exercise lawful empty protected refusal", err)
	}
	if !s.bridgeCapture.intake.OwnedRoundPending() {
		t.Fatal("protected refusal did not open fixed empty interval")
	}
	clock.Store(int64(3*time.Second + 2*time.Millisecond))
	waitFor(t, 2*time.Second, "idle refused newcomer empty interval closed", func() bool { return !s.bridgeCapture.intake.OwnedRoundPending() })
	if err := ownedExecutingIngress(t, s, newcomer.inbound, target); err != nil {
		t.Fatal("empty sweep failed to renew original watermark", err)
	}
}
func TestOwnedRoundCandidateRuntimeStillConstructing(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s, err := newFindNodeOwnedRoundCandidateService(cfg, id, nil, ownedCutProfile())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	spy := &ownedCaptureErrSpy{Context: context.Background()}
	cut, err := s.captureOwnedFindNodeCut(spy)
	if err == nil || cut != (findnode.OwnedCut{}) || spy.calls.Load() != 0 || s.bridgeCapture.roundLoop.started.Load() {
		t.Fatal("scheduler candidate activated or reached partial Node state", err)
	}
}
