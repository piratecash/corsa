package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// All positive work below comes through genuine original TLS/F1, strict receive
// evidence and the same original local core. No public fixture work factory.
func ownedLocalNodeService(t *testing.T, limit int) (*Service, *atomic.Int64, findnodebridge.FixtureDomain) {
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
	profile := ownedCutProfile()
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Capture.Degree)
	intake, sink, err := findnode.NewExecutingFixtureIntake(d.Consumer(), d.Observer(), profile.Intake, profile.Capture.Priority.MaxMembers)
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	key, err := identity.ParsePublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	local, err := intake.NewLocalTarget(key, domain.NetworkID(networkName), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: limit})
	if err != nil {
		t.Fatal(err)
	}
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Capture.Priority, intake: intake, sink: sink, localTarget: local, localLoop: newOwnedFindNodeLocalLoop(ctx.Done(), limit, profile.Intake.Round)}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: ctx.Done(), retirement: intake, localRetirement: local}}
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
func ownedLocalNodeReadRequest(t *testing.T, s *Service, f f1ProofFixture, request protocol.FindNodeRequest) ownedFindNodeReceive {
	t.Helper()
	raw, err := protocol.MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if status := f.outbound.netCore.SendRaw(append(raw, '\n')); status != netcore.SendOK {
		t.Fatal(status)
	}
	core := f.fixtureOwner.inboundCore
	if err := core.Conn().SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire, receipt, claimed, receiveErr, readErr := s.readInboundOwnedFindNodeLine(bufio.NewReader(core.Conn()), f.inbound.id, f.inbound)
	if readErr != nil || receiveErr != nil || !claimed {
		t.Fatal("genuine original local receive", claimed, receiveErr, readErr)
	}
	if !s.admitInboundCommandLine(f.inbound.id, core.Conn().RemoteAddr().String(), wire) {
		t.Fatal("original local command admission refused")
	}
	return receipt
}
func ownedLocalNodeReadAnswer(t *testing.T, f f1ProofFixture) protocol.FindNodeAnswer {
	t.Helper()
	if err := f.outbound.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(f.outbound.conn)
	for n := 0; n < 32; n++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal("original local source did not receive exact answer", err)
		}
		answer, err := protocol.ParseFindNodeAnswer([]byte(line))
		if err == nil {
			return answer
		}
	}
	t.Fatal("strict local answer missing from bounded original frames")
	return protocol.FindNodeAnswer{}
}
func ownedLocalNodeRequest(t *testing.T, s *Service) protocol.FindNodeRequest {
	t.Helper()
	_, request := ownedReceiveWire(t, domain.PeerIdentityFromWire(s.identity.Address))
	return request
}
func ownedLocalNodeReserve(t *testing.T, s *Service, receipt ownedFindNodeReceive) findnode.OwnedLocalResult {
	t.Helper()
	ingress, err := s.takeOwnedFindNodeReceive(t.Context(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := s.captureOwnedFindNodeCut(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	input, err := s.bridgeCapture.sink.SealReceived(ingress.stamp, ingress.received, ingress.source, []byte(ingress.wire))
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.bridgeCapture.localTarget.ReserveReceived(input, cut)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestOwnedLocalNodeRuntimeConstructionDoesNotEnableReady(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s, err := newFindNodeOwnedLocalTargetCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8})
	if err != nil {
		t.Fatal("local constructor must succeed before Ready", err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	spy := &ownedCaptureErrSpy{Context: context.Background()}
	cut, err := s.captureOwnedFindNodeCut(spy)
	if err == nil || cut != (findnode.OwnedCut{}) || spy.calls.Load() != 0 {
		t.Fatal("local construction prematurely enabled capture/admission", err)
	}
	if s.bridgeCapture.localTarget == (findnode.OwnedLocalTarget{}) || s.bridgeCapture.localLoop.started.Load() || s.findNode.owned.localRetirement != s.bridgeCapture.localTarget {
		t.Fatal("constructor lost original local role/retirement custody")
	}
}
func TestOwnedLocalNodeSelfRequestUsesOriginalSignedAnswerWriter(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request)); err != nil {
		t.Fatal("self branch fell through missing remote target", err)
	}
	answer := ownedLocalNodeReadAnswer(t, original)
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil {
		t.Fatal("actual local signature/original request binding", err)
	}
	if answer.ID != request.ID || s.bridgeCapture.intake.OwnedRoundPending() {
		t.Fatal("self request entered remote forwarding or changed original envelope")
	}
	waitFor(t, 2*time.Second, "local signer original slot released", func() bool { return len(s.bridgeCapture.localLoop.slots) == 0 })
	if !s.bridgeCapture.localLoop.started.Load() || cap(s.bridgeCapture.localLoop.jobs) != 8 {
		t.Fatal("bounded single local worker missing")
	}
}
func TestOwnedLocalNodeCachedCopyRestoresThisOriginalRequestID(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	secondFrom, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	secondOriginal, _ := ownedRawAccepted(t, s, secondFrom)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request)); err != nil {
		t.Fatal(err)
	}
	first := ownedLocalNodeReadAnswer(t, original)
	clock.Store(int64(3100 * time.Millisecond))
	again := request
	again.ID++
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, secondOriginal, again)); err != nil {
		t.Fatal(err)
	}
	second := ownedLocalNodeReadAnswer(t, secondOriginal)
	if second.ID != again.ID || second.Signature != first.Signature || second.Target != first.Target || !bytes.Equal(second.PublicKey.Bytes(), first.PublicKey.Bytes()) {
		t.Fatal("cached local answer lost THIS original recipient ID or reminted signed body")
	}
	if err := protocol.VerifyFindNodeAnswer(second, again, domain.NetworkID(networkName)); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedLocalNodeQueueCapacityRefusesBeforeCoreAdmission(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 1)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	loop := s.bridgeCapture.localLoop
	loop.slots <- struct{}{} // negative capacity condition only, never positive work authority
	err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request))
	<-loop.slots
	if !errors.Is(err, findnode.ErrCapacity) || len(loop.jobs) != 0 || loop.started.Load() {
		t.Fatal("full local Node queue reached core admission/worker", err)
	}
	// Same challenge MUST reserve/sign now. A Reserve-before-capacity mutant
	// leaves a Waiting row without its worker, so this exact response never comes.
	request.ID++
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request)); err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyFindNodeAnswer(ownedLocalNodeReadAnswer(t, original), request, domain.NetworkID(networkName)); err != nil {
		t.Fatal("capacity refusal orphaned accepted local row", err)
	}
}
func TestOwnedLocalNodeWorkerStartRefusalHasNoAcceptedOrphan(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	s.closeBackgroundAdmission()
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, request)); !errors.Is(err, findnode.ErrNotReady) {
		t.Fatal("closed original background gate admitted local job", err)
	}
	loop := s.bridgeCapture.localLoop
	if len(loop.slots) != 0 || len(loop.jobs) != 0 || loop.started.Load() {
		t.Fatal("worker-start refusal leaked queue slot")
	}
	request.ID++
	result := ownedLocalNodeReserve(t, s, ownedLocalNodeReadRequest(t, s, original, request))
	if result.Kind != findnode.LocalReserved {
		t.Fatal("worker-start refusal left accepted orphan", result.Kind)
	}
	_ = result.Work.Cancel()
}
func TestOwnedLocalNodeSigningPanicCompletesOriginalWithoutReplay(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	result := ownedLocalNodeReserve(t, s, ownedLocalNodeReadRequest(t, s, original, request))
	if result.Kind != findnode.LocalReserved {
		t.Fatal(result.Kind)
	}
	permit, input, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	// Genuine consumed permit; only the private signing endpoint is faulty.
	// Nil identity provokes the existing signer panic, never a fake success.
	if answers := (*Service)(nil).completeOwnedFindNodeLocalSignature(permit, input); len(answers) != 0 {
		t.Fatal("failed signing minted answer")
	}
	if _, err := permit.Complete(&domain.FindNodeSignature{}); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("panic stranded or replayed mandatory original completion", err)
	}
	if _, _, err := result.Work.Take(); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("panic refunded/replayed actual T", err)
	}
}
func TestOwnedLocalNodeEndAfterTKeepsMandatoryCompletion(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, source := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	request := ownedLocalNodeRequest(t, s)
	result := ownedLocalNodeReserve(t, s, ownedLocalNodeReadRequest(t, s, original, request))
	if result.Kind != findnode.LocalReserved {
		t.Fatal(result.Kind)
	}
	permit, input, err := result.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	original.inbound.end()
	if answers := s.completeOwnedFindNodeLocalSignature(permit, input); len(answers) != 0 {
		t.Fatal("closed original recipient received answer work")
	}
	if _, err := permit.Complete(nil); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("End stranded mandatory completion", err)
	}
	if s.findNode.owned.localRetirement != s.bridgeCapture.localTarget || source.ConnID() == 0 {
		t.Fatal("fixture lost original local retirement binding")
	}
}
func TestOwnedLocalNodeAcceptedFutureSlotRetainsOriginalUntilFreshT(t *testing.T) {
	s, clock, _ := ownedLocalNodeService(t, 8)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	original, _ := ownedRawAccepted(t, s, from)
	clock.Store(int64(3 * time.Second))
	first := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, first)); err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyFindNodeAnswer(ownedLocalNodeReadAnswer(t, original), first, domain.NetworkID(networkName)); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID++
	second.Challenge[0] ^= 1
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, second)); err != nil {
		t.Fatal(err)
	}
	loop := s.bridgeCapture.localLoop
	waitFor(t, 2*time.Second, "accepted future local work retained by one worker", func() bool { return len(loop.slots) == 1 && len(loop.jobs) == 0 })
	// Calendar's second alias is strictly later than f1. Advancing only this
	// genuine fixture clock, rather than reusing f1/once-per-worker Sample, makes
	// the second exact original T lawful and its answer signature verifiable.
	clock.Store(int64(3250 * time.Millisecond))
	if err := protocol.VerifyFindNodeAnswer(ownedLocalNodeReadAnswer(t, original), second, domain.NetworkID(networkName)); err != nil {
		t.Fatal("future accepted original did not receive fresh T", err)
	}
	waitFor(t, 2*time.Second, "both original local worker slots released", func() bool { return len(loop.slots) == 0 })
}
func TestOwnedLocalNodeShutdownDrainsOriginalPendingSlots(t *testing.T) {
	for _, mode := range []string{"shutdown", "clock-poison"} {
		t.Run(mode, func(t *testing.T) {
			s, clock, _ := ownedLocalNodeService(t, 8)
			from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
			original, _ := ownedRawAccepted(t, s, from)
			clock.Store(int64(3 * time.Second))
			first := ownedLocalNodeRequest(t, s)
			if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, first)); err != nil {
				t.Fatal(err)
			}
			_ = ownedLocalNodeReadAnswer(t, original)
			second := first
			second.ID++
			second.Challenge[0] ^= 1
			if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, second)); err != nil {
				t.Fatal(err)
			}
			third := second
			third.ID++
			third.Challenge[0] ^= 2
			if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, original, third)); err != nil {
				t.Fatal(err)
			}
			loop := s.bridgeCapture.localLoop
			if len(loop.slots) != 2 {
				t.Fatal("genuine accepted future jobs absent before shutdown")
			}
			if mode == "clock-poison" {
				clock.Store(int64(2 * time.Second)) // actual regression after genuine f=3s
				waitFor(t, 2*time.Second, "poisoned original clock retires pending local work", func() bool { return len(loop.slots) == 0 && len(loop.jobs) == 0 })
				if _, err := s.bridgeCapture.observer.BeginCapture(); err == nil {
					t.Fatal("fixture did not poison the actual original clock domain")
				}
			}
			s.runCancel()
			s.closeBackgroundAdmission()
			joined := make(chan struct{})
			go func() { s.WaitBackground(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown stranded original local worker/timer")
			}
			if len(loop.slots) != 0 || len(loop.jobs) != 0 {
				t.Fatal("shutdown retained accepted queue slot or orphan job")
			}
		})
	}
}
