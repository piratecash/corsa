package node

import (
	"bufio"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func ownedReceiveWire(t *testing.T, target domain.PeerIdentity) (string, protocol.FindNodeRequest) {
	t.Helper()
	request := protocol.FindNodeRequest{ID: 17, Target: target}
	request.Challenge[0] = 29
	raw, err := protocol.MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), request
}

func TestOwnedReceiveGenuineOriginalPointAndSingleUse(t *testing.T) {
	s, clock, d := ownedCaptureTestService(t, 4, 4, 8)
	dialer, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	f, original := ownedRawAccepted(t, s, dialer)
	wire, request := ownedReceiveWire(t, original.Identity())
	clock.Store(int64(3 * time.Second))
	receipt, err := s.recordOwnedFindNodeReceive(f.inbound, wire)
	if err != nil || receipt.cell == nil {
		t.Fatal("genuine ingress mint", err)
	}
	copied := receipt
	clock.Store(int64(4 * time.Second))
	got, err := s.takeOwnedFindNodeReceive(t.Context(), copied)
	if err != nil || got.source != original || got.transport != f.inbound || got.capture != s.bridgeCapture || got.request != request || got.received.Elapsed() != 3*time.Second || !d.Consumer().Matches(got.stamp, got.received) {
		t.Fatal("receive replaced original or refreshed point", got, err)
	}
	if _, err := s.takeOwnedFindNodeReceive(t.Context(), receipt); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("copied ingress replay", err)
	}
}

func TestOwnedReceiveStrictParserReturnsNoPartialIngress(t *testing.T) {
	s, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	dialer, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	f, original := ownedRawAccepted(t, s, dialer)
	wire, _ := ownedReceiveWire(t, original.Identity())
	for _, bad := range []string{strings.Replace(wire, "{", "{\"type\":\"dm\",", 1), strings.Replace(wire, "\"v\":1", "\"v\":2", 1), strings.Repeat("x", protocol.MaxFindNodeFrameBytes+1)} {
		receipt, err := s.recordOwnedFindNodeReceive(f.inbound, bad)
		if err != nil {
			if receipt.cell != nil {
				t.Fatal("raw refusal returned partial point", receipt, err)
			}
			continue
		}
		if receipt.cell == nil {
			t.Fatal("raw point absent before postadmission strict dispatch")
		}
		ingress, err := s.takeOwnedFindNodeReceive(t.Context(), receipt)
		if err == nil || ingress.source != (findnode.Session{}) || ingress.transport != nil {
			t.Fatal("strict parser returned partial ingress", ingress, err)
		}
	}
}

func TestOwnedReceiveForeignOwnerAndEndedOriginalRefuse(t *testing.T) {
	s, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	other, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	f, original := ownedRawAccepted(t, s, other)
	wire, _ := ownedReceiveWire(t, original.Identity())
	if got, err := other.recordOwnedFindNodeReceive(f.inbound, wire); !errors.Is(err, findnode.ErrUnauthenticated) || got.cell != nil {
		t.Fatal("foreign transport minted receipt", got, err)
	}
	receipt, err := s.recordOwnedFindNodeReceive(f.inbound, wire)
	if err != nil {
		t.Fatal(err)
	}
	f.inbound.end()
	if got, err := s.takeOwnedFindNodeReceive(t.Context(), receipt); !errors.Is(err, findnode.ErrUnauthenticated) || got.source != (findnode.Session{}) {
		t.Fatal("ended original was replaced", got, err)
	}
	if _, err := s.takeOwnedFindNodeReceive(t.Context(), receipt); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("refused envelope replay", err)
	}
}

func TestOwnedReceiveCancellationRetiresOriginalEnvelope(t *testing.T) {
	s, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	other, _, _ := ownedCaptureTestService(t, 4, 4, 8)
	f, original := ownedRawAccepted(t, s, other)
	wire, _ := ownedReceiveWire(t, original.Identity())
	receipt, err := s.recordOwnedFindNodeReceive(f.inbound, wire)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := s.takeOwnedFindNodeReceive(ctx, receipt); !errors.Is(err, context.Canceled) || got.source != (findnode.Session{}) {
		t.Fatal("cancel returned partial ingress", got, err)
	}
	if _, err := s.takeOwnedFindNodeReceive(t.Context(), receipt); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("cancelled ingress replay", err)
	}
}

// Tagged FixtureDomain + SAME original roles/intake precede the common builder.
// No runtime Ready switch, manual positive Session or transport receipt exists.
func ownedReceiveService(t *testing.T) (*Service, *atomic.Int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clock := new(atomic.Int64)
	d, err := findnodebridge.NewFixtureDomain(ctx.Done(), clock)
	if err != nil {
		t.Fatal(err)
	}
	profile := findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 8}, Priority: findNodePriorityProfile{MaxMembers: 8, MaxRegistryVisits: 16}}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Degree)
	intake, sink, err := findnode.NewDormantFixtureIntake(d.Consumer(), findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}, 8)
	if err != nil {
		t.Fatal(err)
	}
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Priority, intake: intake, sink: sink}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done()}}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	dir := t.TempDir()
	cfg.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.SecureSessionStorePath = filepath.Join(dir, "pins.json")
	var service *Service
	t.Cleanup(func() {
		cancel()
		if service != nil {
			service.closeBackgroundAdmission()
			service.WaitBackground()
		}
	})
	service = newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b})
	service.routeWithdrawalGracePeriodTest = -1
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	return service, clock
}

func TestOwnedInboundReadToOfferRetainsOriginalRow(t *testing.T) {
	s, clock := ownedReceiveService(t)
	sender, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	targetNode, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	from, _ := ownedRawAccepted(t, s, sender)
	_, target := ownedRawAccepted(t, s, targetNode)
	wire, _ := ownedReceiveWire(t, target.Identity())
	clock.Store(int64(3 * time.Second))
	for index := 0; index < 2; index++ {
		got, receipt, claimed, receiveErr, readErr := s.readInboundOwnedFindNodeLine(bufio.NewReader(strings.NewReader(wire+"\n")), from.inbound.id, from.inbound)
		if readErr != nil || receiveErr != nil || !claimed || strings.TrimSpace(got) != wire {
			t.Fatal("actual bounded inbound read did not seal original ingress", index, claimed, receiveErr, readErr)
		}
		err := s.dispatchOwnedFindNodeReceive(receipt)
		if index == 0 && err != nil {
			t.Fatal("vertical request did not reach original owned offer", err)
		}
		if index == 1 && !errors.Is(err, findnode.ErrDuplicateID) {
			t.Fatal("first vertical offer was discarded or rebound", err)
		}
	}
}

func TestOwnedOutboundReadToOfferUsesActualCMAcceptedOriginal(t *testing.T) {
	s, clock := ownedReceiveService(t)
	remote, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	targetNode, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	f := f1PairOn(t, true, nil, remote, s)
	start := ownedRawPrepareCMAtSlot(t, f, domain.PeerAddress("127.0.0.1:64005"))
	dispatched := make(chan struct{})
	f.fixtureOwner.caseDone = dispatched
	go func() { defer close(dispatched); start() }()
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("actual CM dispatch did not join")
	}
	waitFor(t, 2*time.Second, "owned receive actual outbound F1", func() bool { return len(f1Members(t, s).sessions) == 1 })
	_, target := ownedRawAccepted(t, s, targetNode)
	wire, _ := ownedReceiveWire(t, target.Identity())
	clock.Store(int64(3 * time.Second))
	f.outbound.common.mu.Lock()
	owner := f.outbound.common.ownerLocked()
	f.outbound.common.mu.Unlock()
	if owner == nil {
		t.Fatal("actual CM original owner missing")
	}
	for index := 0; index < 2; index++ {
		_, receipt, claimed, receiveErr, readErr := s.readOutboundOwnedFindNodeLine(bufio.NewReader(strings.NewReader(wire+"\n")), f.outbound, owner)
		if readErr != nil || receiveErr != nil || !claimed {
			t.Fatal("actual outbound bounded read lost original receipt", index, claimed, receiveErr, readErr)
		}
		err := s.dispatchOwnedFindNodeReceive(receipt)
		if index == 0 && err != nil {
			t.Fatal("outbound request did not reach owned offer", err)
		}
		if index == 1 && !errors.Is(err, findnode.ErrDuplicateID) {
			t.Fatal("outbound offer not retained", err)
		}
	}
}

func TestOwnedOutboundReceivePointPrecedesOriginalAdmissionMutexWait(t *testing.T) {
	s, clock := ownedReceiveService(t)
	remote, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	f := f1PairOn(t, true, nil, remote, s)
	start := ownedRawPrepareCMAtSlot(t, f, domain.PeerAddress("127.0.0.1:64006"))
	dispatched := make(chan struct{})
	f.fixtureOwner.caseDone = dispatched
	go func() { defer close(dispatched); start() }()
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("CM dispatch did not join")
	}
	waitFor(t, 2*time.Second, "original outbound published", func() bool { return len(f1Members(t, s).sessions) == 1 })
	f.outbound.common.mu.Lock()
	owner := f.outbound.common.ownerLocked()
	f.outbound.common.mu.Unlock()
	wire, _ := ownedReceiveWire(t, f1Members(t, s).sessions[0].Identity())
	clock.Store(int64(3 * time.Second))
	raw, receipt, claimed, receiveErr, readErr := s.readOutboundOwnedFindNodeBeforeCharge(bufio.NewReader(strings.NewReader(wire+"\n")), f.outbound, owner)
	if readErr != nil || receiveErr != nil || !claimed || receipt.cell == nil || receipt.cell.received.Elapsed() != 3*time.Second {
		t.Fatal("original post-read point missing", receiveErr, readErr)
	}
	f.outbound.admission.mu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(started); _, err := s.admitReadSessionLine(f.outbound, raw); done <- err }()
	<-started
	select {
	case err := <-done:
		f.outbound.admission.mu.Unlock()
		t.Fatal("charge bypassed actual admission mutex", err)
	default:
	}
	clock.Store(int64(4 * time.Second))
	f.outbound.admission.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not join")
	}
	if receipt.cell.received.Elapsed() != 3*time.Second {
		t.Fatal("admission wait refreshed original received")
	}
	ingress, err := s.takeOwnedFindNodeReceive(t.Context(), receipt)
	if err != nil || ingress.received.Elapsed() != 3*time.Second || ingress.request.ID != 17 {
		t.Fatal("postadmission parser replaced raw point", ingress, err)
	}
	// Actual full wrapper discards its locally minted receipt on raw admission refusal.
	f.outbound.admission.mu.Lock()
	f.outbound.admission.bytes = -1e9
	f.outbound.admission.frames = -1e9
	f.outbound.admission.mu.Unlock()
	_, rejected, wasClaimed, receiveErr, readErr := s.readOutboundOwnedFindNodeLine(bufio.NewReader(strings.NewReader(wire+"\n")), f.outbound, owner)
	if readErr == nil || rejected.cell != nil || wasClaimed || receiveErr != nil {
		t.Fatal("raw admission refusal leaked original receipt", wasClaimed, receiveErr, readErr)
	}
}
