package node

import (
	"context"
	"errors"
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

// Positive fixture: original concrete FixtureDomain and ALL three original
// roles precede the common builder/FIRST Service literal. Only its original
// publisher is sealed after construction. No RuntimePair/Ready bool is forged.
func ownedCaptureBuild(t *testing.T, profile findNodeOwnedCaptureProfile, constructor datagramConstructor, seal bool) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	return ownedCaptureBuildConfigured(t, profile, constructor, seal, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
}
func ownedCaptureBuildConfigured(t *testing.T, profile findNodeOwnedCaptureProfile, constructor datagramConstructor, seal bool, cfg config.Node) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
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
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Degree)
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Priority}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done()}}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg = configtest.WithTempDataDirs(t, cfg)
	s = newServiceWithConstruction(cfg, id, nil, nil, constructor, serviceConstruction{bridge: b})
	// These no-Run fixtures own transport cleanup synchronously; do not
	// leave route-withdrawal AfterFunc callbacks outside the owned joins.
	s.routeWithdrawalGracePeriodTest = -1
	if s.bridgeCapture != b.capture || s.bridgeCapture.terminal != s.bridgeTerminal || s.ownedDegreeStatisticProducer != nil {
		t.Fatal("capture escaped original FIRST-literal roles or installed statistics")
	}
	if _, err := s.captureOwnedFindNodeObservation(t.Context()); err == nil {
		t.Fatal("fixture made Ready before explicit original publisher seal")
	}
	if seal {
		if err := d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
	}
	return s, clock, d
}

func ownedCaptureContactTestService(t *testing.T, maxActive, m, v int) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull}
	cfg.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.SecureSessionStorePath = filepath.Join(dir, "pins.json")
	return ownedCaptureBuildConfigured(t, findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: maxActive}, Priority: findNodePriorityProfile{MaxMembers: m, MaxRegistryVisits: v}}, productionDatagramConstructor{}, true, cfg)
}
func ownedCaptureTestService(t *testing.T, maxActive, m, v int) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
	t.Helper()
	return ownedCaptureBuild(t, findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: maxActive}, Priority: findNodePriorityProfile{MaxMembers: m, MaxRegistryVisits: v}}, productionDatagramConstructor{}, true)
}
func ownedCaptureAssertEmpty(t *testing.T, got findNodeOwnedObservation, err error) {
	t.Helper()
	if err == nil || got.owner != nil || got.facts.allOriginal != nil || got.facts.classes != nil || got.degree.at != (findnodebridge.OwnedInstant{}) {
		t.Fatal("capture refusal returned partial authority", got, err)
	}
}
func ownedCaptureAssertPoint(t *testing.T, s *Service, d findnodebridge.FixtureDomain, got findNodeOwnedObservation) {
	t.Helper()
	if got.owner != s.bridgeCapture || !d.Consumer().Matches(got.stamp, got.degree.at) || !got.degree.degreeValidBefore.Contains(got.degree.at) {
		t.Fatal("observation lost original sampled provenance or exclusive frontier", got)
	}
	lease, err := d.Consumer().TryReadClock(got.stamp)
	if err != nil {
		t.Fatal("unchanged fixture generation refused", err)
	}
	released := false
	release := func() {
		if !released {
			lease.Release()
			released = true
		}
	}
	t.Cleanup(release)
	final, err := lease.Sample()
	if err != nil || !final.NotBefore(got.degree.at) {
		t.Fatal("fixture final original lease/point mismatch", err)
	}
	release()
	// This fixture check is not a production commit/source CAS.
}

type ownedCaptureErrSpy struct {
	context.Context
	calls atomic.Int64
}

func (s *ownedCaptureErrSpy) Err() error { s.calls.Add(1); return s.Context.Err() }

type ownedCaptureInspectConstructor struct {
	t      *testing.T
	called bool
}

func (c *ownedCaptureInspectConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	c.called = true
	if s.bridgeCapture == nil || s.bridgeCapture.terminal != s.bridgeTerminal || s.secureSessions != nil {
		c.t.Fatal("capture dependency absent at actual incomplete builder escape")
	}
	spy := &ownedCaptureErrSpy{Context: context.Background()}
	got, err := s.captureOwnedFindNodeObservation(spy)
	ownedCaptureAssertEmpty(c.t, got, err)
	if spy.calls.Load() != 0 {
		c.t.Fatal("Constructing gate reached caller callback before phase refusal")
	}
	return nil, nil
}
func TestOwnedNodeCaptureConstructingBeforeBuilderState(t *testing.T) {
	inspect := &ownedCaptureInspectConstructor{t: t}
	ownedCaptureBuild(t, findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 1}, Priority: findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2}}, inspect, false)
	if !inspect.called {
		t.Fatal("actual builder callback not exercised")
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", Type: domain.NodeTypeFull})
	s, err := newFindNodeOwnedCaptureCandidateService(cfg, id, nil, findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 1}, Priority: findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	ownedCaptureAssertEmpty(t, got, err)
	if !s.bridgeCapture.consumer.IsRuntime() || s.ownedDegreeStatisticProducer != nil {
		t.Fatal("runtime root changed kind or statistics installation")
	}
	var absent *Service
	got, err = absent.captureOwnedFindNodeObservation(nil) //nolint:staticcheck // SA1012: intentional nil-context refusal/order negative; TODO would remove the tested input.
	ownedCaptureAssertEmpty(t, got, err)
}
func TestOwnedNodeCaptureGenuineListenerAndActualCM(t *testing.T) {
	s, _, sd := ownedCaptureTestService(t, 4, 4, 8)
	dialer, _, dd := ownedCaptureTestService(t, 4, 4, 8)
	f, original := ownedRawAccepted(t, s, dialer)
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || len(got.facts.allOriginal) != 1 || got.facts.allOriginal[0] != original || got.facts.classes[0] != findnode.ClassOrdinary || got.degree.window != 1 {
		t.Fatal("Node listener capture lost genuine original/class/degree", got, err)
	}
	ownedCaptureAssertPoint(t, s, sd, got)
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
		t.Fatal("actual CM fixture dispatch did not join")
	}
	waitFor(t, 2*time.Second, "owned capture actual CM publication", func() bool { return len(f1Members(t, dialer).sessions) == 1 })
	dialer.peerMu.RLock()
	l := f.outbound.common
	l.mu.Lock()
	bound := l.findNode.session
	receipt := f.outbound.findNodeReceipt
	genuine := receipt != nil && receipt.peer == f.outbound && receipt.common == l && receipt.address == slotAddress && receipt.address != f.outbound.address
	l.mu.Unlock()
	dialer.peerMu.RUnlock()
	got, err = dialer.captureOwnedFindNodeObservation(t.Context())
	if err != nil || !genuine || len(got.facts.allOriginal) != 1 || got.facts.allOriginal[0] != bound || got.degree.window != 1 {
		t.Fatal("Node CM capture replaced original/fallback receipt", got, err)
	}
	ownedCaptureAssertPoint(t, dialer, dd, got)
}
func TestOwnedNodeCaptureFullClassesBoundsAndRequestCancellation(t *testing.T) {
	s, _, d := ownedCaptureContactTestService(t, 4, 2, 4)
	contact, _, _ := ownedCaptureTestService(t, 4, 2, 4)
	ordinary, _, _ := ownedCaptureTestService(t, 4, 2, 4)
	contactResult := addLocalContact(t, s, contact.identity)
	if contactResult.Fact != domain.LocalContactFactDurableConfirmed {
		t.Fatal("positive K fact was not durably confirmed", contactResult)
	}
	_, k := ownedRawAccepted(t, s, contact)
	_, other := ownedRawAccepted(t, s, ordinary)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if contactResult.Identity != k.Identity() {
		t.Fatal("durable fact and genuine TLS identity differ")
	}
	store := s.localContactPinStore()
	if store == nil {
		t.Fatal("positive K has no original pin store")
	}
	store.mu.Lock()
	entry, present := store.identities[k.Identity()]
	confirmed := present && entry.metadata != nil && entry.classMetadata().Contact.State == domain.IdentityPinAllocationEligible && entry.metadata.confirmedRevision != 0 && entry.metadata.confirmedRevision == entry.protectionRevision
	store.mu.Unlock()
	if !confirmed {
		t.Fatal("positive K lacks current strict pin metadata ACK")
	}
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || len(got.facts.allOriginal) != 2 {
		t.Fatal("Node full strict class fixture", err)
	}
	classes := map[findnode.Session]findnode.SessionClass{}
	for i, member := range got.facts.allOriginal {
		classes[member] = got.facts.classes[i]
	}
	if classes[k] != findnode.ClassContact || classes[other] != findnode.ClassOrdinary {
		t.Fatal("Node classes lost exact original index", classes)
	}
	ownedCaptureAssertPoint(t, s, d, got)
	// Negative authority fixture only, never a manufactured positive class.
	s.trust.mu.Lock()
	prior := s.trust.authorityReadErr
	s.trust.authorityReadErr = errors.New("owned capture unreadable authority")
	s.trust.mu.Unlock()
	got, err = s.captureOwnedFindNodeObservation(t.Context())
	s.trust.mu.Lock()
	s.trust.authorityReadErr = prior
	s.trust.mu.Unlock()
	ownedCaptureAssertEmpty(t, got, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err = s.captureOwnedFindNodeObservation(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("request cancellation lost", err)
	}
	ownedCaptureAssertEmpty(t, got, err)
	// Existing accepted M/V guard is exercised through the real Node entry.
	small, _, _ := ownedCaptureTestService(t, 4, 1, 4)
	a, _, _ := ownedCaptureTestService(t, 4, 2, 4)
	b, _, _ := ownedCaptureTestService(t, 4, 2, 4)
	ownedRawAccepted(t, small, a)
	ownedRawAccepted(t, small, b)
	got, err = small.captureOwnedFindNodeObservation(t.Context())
	if !errors.Is(err, findnode.ErrCapacity) {
		t.Fatal("Node M overflow returned partial vector", err)
	}
	ownedCaptureAssertEmpty(t, got, err)
}
func TestOwnedNodeObservationOriginalStampVersionsRealEnd(t *testing.T) {
	s, clock, d := ownedCaptureTestService(t, 2, 2, 4)
	peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	f, _ := ownedRawAccepted(t, s, peer)
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ownedCaptureAssertPoint(t, s, d, got)
	clock.Store(int64(time.Second))
	f.inbound.life.end()
	if !pinPublicationRefused(d, got.stamp) {
		t.Fatal("actual common End did not invalidate original observation stamp")
	}
	// Historical result is not silently relabeled with a fresh stamp/q.
	if got.degree.at.Elapsed() != 0 {
		t.Fatal("original historical q changed after publication")
	}
}

func TestOwnedNodeCaptureForeignConsumerRefusesAfterActualPoint(t *testing.T) {
	s, _, d := ownedCaptureTestService(t, 2, 2, 4)
	peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	_, _, foreign := ownedCaptureTestService(t, 2, 2, 4)
	ownedRawAccepted(t, s, peer)
	good, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ownedCaptureAssertPoint(t, s, d, good)
	// Negative private role corruption AFTER a genuine same-domain positive;
	// never a substitute positive constructor or manufactured observation.
	original := s.bridgeCapture.consumer
	s.bridgeCapture.consumer = foreign.Consumer()
	t.Cleanup(func() { s.bridgeCapture.consumer = original })
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	if !errors.Is(err, findnode.ErrUnauthenticated) {
		t.Fatal("foreign consumer was accepted by actual Node point validation", got, err)
	}
	ownedCaptureAssertEmpty(t, got, err)
}
