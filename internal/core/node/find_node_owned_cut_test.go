package node

import (
	"context"
	"errors"
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

func ownedCutProfile() findNodeBridgeProfile {
	return findNodeBridgeProfile{Capture: findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 4}, Priority: findNodePriorityProfile{MaxMembers: 4, MaxRegistryVisits: 8}}, Intake: findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}}
}

// The original genuine fixture roles and exact constructor tuple exist before
// the common builder. Only the original FixtureDomain publisher seals it.
func ownedCutBuild(t *testing.T, profile findNodeBridgeProfile, constructor datagramConstructor, seal bool) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := new(atomic.Int64)
	var s *Service
	t.Cleanup(func() {
		cancel()
		if s != nil {
			s.closeBackgroundAdmission()
			s.WaitBackground()
		}
	})
	d, err := findnodebridge.NewFixtureDomain(ctx.Done(), clock)
	if err != nil {
		t.Fatal(err)
	}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Capture.Degree)
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Capture.Priority}
	intake, sink, err := findnode.NewDormantFixtureIntake(d.Consumer(), profile.Intake, profile.Capture.Priority.MaxMembers)
	if err != nil {
		t.Fatal(err)
	}
	b.capture.intake, b.capture.sink = intake, sink
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done()}}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s = newServiceWithConstruction(cfg, id, nil, nil, constructor, serviceConstruction{bridge: b})
	// These no-Run fixtures own transport cleanup synchronously; do not
	// leave route-withdrawal AfterFunc callbacks outside the owned joins.
	s.routeWithdrawalGracePeriodTest = -1
	if s.bridgeCapture != b.capture || s.bridgeCapture.terminal != s.bridgeTerminal || s.ownedDegreeStatisticProducer != nil {
		t.Fatal("capture escaped original FIRST-literal roles or installed statistics")
	}
	if _, err := s.captureOwnedFindNodeCut(t.Context()); err == nil {
		t.Fatal("fixture made Ready before explicit original publisher seal")
	}
	if seal {
		if err := d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
	}
	return s, clock, d
}
func ownedCutAssertRefused(t *testing.T, got findnode.OwnedCut, err error) {
	t.Helper()
	if err == nil || got != (findnode.OwnedCut{}) {
		t.Fatal("cut refusal returned partial authority", got, err)
	}
}

type ownedCutInspectConstructor struct {
	t      *testing.T
	called bool
}

func (c *ownedCutInspectConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	c.called = true
	if s.bridgeCapture == nil || s.bridgeCapture.terminal != s.bridgeTerminal || s.bridgeCapture.intake == (findnode.DormantIntake{}) || s.bridgeCapture.sink == (findnode.ObservationSink{}) || s.secureSessions != nil {
		c.t.Fatal("original owner/sink missing before FIRST builder state")
	}
	spy := &ownedCaptureErrSpy{Context: context.Background()}
	got, err := s.captureOwnedFindNodeCut(spy)
	ownedCutAssertRefused(c.t, got, err)
	if spy.calls.Load() != 0 {
		c.t.Fatal("Constructing cut reached context callback")
	}
	return nil, nil
}
func TestOwnedNodeCutOriginalTupleBeforeBuilderAndRuntimeOff(t *testing.T) {
	inspect := &ownedCutInspectConstructor{t: t}
	ownedCutBuild(t, ownedCutProfile(), inspect, false)
	if !inspect.called {
		t.Fatal("actual early builder not exercised")
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", Type: domain.NodeTypeFull})
	s, err := newFindNodeBridgeCandidateService(cfg, id, nil, ownedCutProfile())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	spy := &ownedCaptureErrSpy{Context: context.Background()}
	cut, err := s.captureOwnedFindNodeCut(spy)
	ownedCutAssertRefused(t, cut, err)
	if spy.calls.Load() != 0 || !s.bridgeCapture.consumer.IsRuntime() || s.bridgeCapture.intake == (findnode.DormantIntake{}) || s.bridgeCapture.sink == (findnode.ObservationSink{}) || s.ownedDegreeStatisticProducer != nil {
		t.Fatal("runtime cut activated or lost original pair")
	}
	old, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	if old.bridgeCapture.intake != (findnode.DormantIntake{}) || old.bridgeCapture.sink != (findnode.ObservationSink{}) {
		t.Fatal("observation-only root acquired intake")
	}
	cut, err = old.captureOwnedFindNodeCut(t.Context())
	ownedCutAssertRefused(t, cut, err)
	var absent *Service
	cut, err = absent.captureOwnedFindNodeCut(nil) //nolint:staticcheck // SA1012: intentional nil-context refusal/order negative; TODO would remove the tested input.
	ownedCutAssertRefused(t, cut, err)
}

func TestOwnedNodeCutGenuineListenerAndActualCM(t *testing.T) {
	s, _, sd := ownedCutBuild(t, ownedCutProfile(), productionDatagramConstructor{}, true)
	dialer, _, dd := ownedCutBuild(t, ownedCutProfile(), productionDatagramConstructor{}, true)
	f, original := ownedRawAccepted(t, s, dialer)
	observation, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || len(observation.facts.allOriginal) != 1 || observation.facts.allOriginal[0] != original || observation.facts.classes[0] != findnode.ClassOrdinary || observation.degree.window != 1 {
		t.Fatal("listener observation lost exact originals/class/degree", err)
	}
	ownedCaptureAssertPoint(t, s, sd, observation)
	cut, err := s.captureOwnedFindNodeCut(t.Context())
	if err != nil || cut == (findnode.OwnedCut{}) {
		t.Fatal("actual accepted observation-to-cut failed", err)
	}
	slotAddress := domain.PeerAddress("127.0.0.1:64003")
	if slotAddress == f.outbound.address {
		slotAddress = domain.PeerAddress("127.0.0.1:64004")
	}
	start := ownedRawPrepareCMAtSlot(t, f, slotAddress)
	joined := make(chan struct{})
	f.fixtureOwner.caseDone = joined
	go func() { defer close(joined); start() }()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("actual CM job did not join")
	}
	waitFor(t, 2*time.Second, "owned cut actual CM publication", func() bool { return len(f1Members(t, dialer).sessions) == 1 })
	dialer.peerMu.RLock()
	l := f.outbound.common
	l.mu.Lock()
	bound := l.findNode.session
	receipt := f.outbound.findNodeReceipt
	genuine := receipt != nil && receipt.peer == f.outbound && receipt.common == l && receipt.address == slotAddress && receipt.address != f.outbound.address
	l.mu.Unlock()
	dialer.peerMu.RUnlock()
	observation, err = dialer.captureOwnedFindNodeObservation(t.Context())
	if err != nil || !genuine || len(observation.facts.allOriginal) != 1 || observation.facts.allOriginal[0] != bound || observation.degree.window != 1 {
		t.Fatal("actual CM original receipt/vector mismatch", err)
	}
	ownedCaptureAssertPoint(t, dialer, dd, observation)
	cut, err = dialer.captureOwnedFindNodeCut(t.Context())
	if err != nil || cut == (findnode.OwnedCut{}) {
		t.Fatal("actual CM observation-to-cut failed", err)
	}
	// Opaque success supplies no getter or Node black-box completeness oracle.
	// Exact conversion closure and findnode private-copy tests cover that claim.
}

func TestOwnedNodeCutCancellationAndRealEnd(t *testing.T) {
	s, clock, d := ownedCutBuild(t, ownedCutProfile(), productionDatagramConstructor{}, true)
	peer, _, _ := ownedCutBuild(t, ownedCutProfile(), productionDatagramConstructor{}, true)
	f, _ := ownedRawAccepted(t, s, peer)
	observation, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cut, err := s.captureOwnedFindNodeCut(t.Context())
	if err != nil || cut == (findnode.OwnedCut{}) {
		t.Fatal("genuine positive cut absent", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rejected, err := s.captureOwnedFindNodeCut(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cut lost request cancellation", err)
	}
	ownedCutAssertRefused(t, rejected, err)
	clock.Store(int64(time.Second))
	f.inbound.life.end()
	if !pinPublicationRefused(d, observation.stamp) || observation.degree.at.Elapsed() != 0 || cut == (findnode.OwnedCut{}) {
		t.Fatal("real End failed original stamp/historical result separation")
	}
}
