package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Closed two-service test composition. Actual Run owns every operational
// session/receive/round/reservation/worker/writer; no manual R or T handoff.
// This is the existing two-member small2 profile, not population512 or G2.
func newOwnedAutomaticTraceService(t *testing.T, bootstrap string, target bool) (*Service, string) {
	t.Helper()
	if (bootstrap != "") != target {
		t.Fatal("automatic trace requires the fixed R-listener/T-dialer composition")
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true,
		Type: domain.NodeTypeFull, BootstrapPeers: []string{}, AllowPrivatePeers: true,
	})
	cfg = deriveTestAdvertisePort(cfg)
	if target {
		cfg.BootstrapPeers = []string{normalizeAddress(bootstrap)}
	}
	profile := ownedCutProfile()
	profile.Capture = findNodeOwnedCaptureProfile{
		Degree:   degreeProfile{MaxActive: 2},
		Priority: findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4},
	}
	profile.Intake = findnode.DormantIntakeProfile{
		Timing: findnode.TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second},
		Round:  100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4,
	}
	signing := findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4}
	var timing []findnode.OwnedTargetTimingProfile
	if target {
		// Existing explicit r/c timing tuple, with the selected up-to8 capacity.
		// No numeric default, injected clock, sleep or latency assertion.
		signing.MaxReservations = 8
		timing = []findnode.OwnedTargetTimingProfile{{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond}}
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, signing, timing...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil {
		t.Fatal(err)
	}
	if !r.finalized || !r.checked || !r.bindings(s) || !s.bridgeCapture.consumer.IsRuntime() || s.bridgeCapture.localLoop.signingGate != nil || s.ownedFindNodeEndpointTest != nil {
		t.Fatal("original Ready/default tuple changed before test-only selection")
	}
	s.ownedFindNodeAdvertiseTest = true
	s.ownedFindNodeEndpointTest = new(ownedFindNodeEndpointTestSlot)
	running := runServiceForTest(t.Context(), s)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.Stop(ctx); err != nil {
			t.Error("original trace Run did not stop", err)
		}
		if err := running.DrainBackground(ctx); err != nil {
			t.Error("original trace background did not join", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := running.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	s.peerMu.RLock()
	address := s.listener.Addr().String()
	s.peerMu.RUnlock()
	if s.disableRateLimiting || s.loopbackInbound.bounds.maxActive > 64 {
		t.Fatal("trace bypassed original loopback/listener budgets")
	}
	return s, address
}

func TestOwnedAutomaticEndpointTraceDefaultsUnchanged(t *testing.T) {
	cfg, id := readyTestInputs(t)
	ordinary := NewService(cfg, id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	rcfg, rid := readyTestInputs(t)
	ready, err := newFindNodeReadyCandidateService(rcfg, rid, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ready.runCancel(); ready.closeBackgroundAdmission(); ready.WaitBackground() })
	for _, s := range []*Service{ordinary, ready} {
		if s.ownedFindNodeEndpointTest != nil {
			t.Fatal("ordinary/Ready enabled a test trace or advertisement")
		}
	}
}

func TestOwnedAutomaticEndpointTraceOriginalSixPoints(t *testing.T) {
	relay, relayAddress := newOwnedAutomaticTraceService(t, "", false)
	target, _ := newOwnedAutomaticTraceService(t, relayAddress, true)
	source := dialOwnedBareV2PilotClient(t, relay, relayAddress, true)
	rid := domain.PeerIdentityFromWire(relay.identity.Address)
	tid := domain.PeerIdentityFromWire(target.identity.Address)
	if rid == tid || rid == source.id || tid == source.id {
		t.Fatal("trace identities not distinct")
	}
	// R listener and T outbound CM independently publish their actual F1 rows.
	// Returned session values are inspected only; none is installed or minted.
	bareV2PilotOriginalMembers(t, relay, source.id, tid)
	waitFor(t, 2*time.Second, "original T outbound CM did not publish authenticated R", func() bool {
		facts, err := target.findNodePriorityMembers(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4})
		if err != nil || len(facts.sessions) != 1 || facts.sessions[0].Identity() != rid {
			return false
		}
		target.peerMu.RLock()
		defer target.peerMu.RUnlock()
		// The original outbound CM owns its TLS proof on peer.proven; the
		// NetCore Auth store is the inbound connauth route, not this owner.
		original, found, err := target.collectOwnedFindNodeRawMemberLocked(t.Context(), facts.sessions[0].ConnID())
		if err != nil || !found || original != facts.sessions[0] {
			return false
		}
		entry := target.connEntryByIDLocked(original.ConnID())
		if entry == nil || entry.common == nil || entry.core == nil {
			return false
		}
		life := entry.common
		life.mu.Lock()
		defer life.mu.Unlock()
		outbound := life.peer
		if life.ended || life.token != target.proofToken || life.entry != entry || !life.outbound || life.role != sessionv2.RoleDialer || life.findNode == nil || !life.findNode.published || life.findNode.session != original || outbound == nil || outbound.common != life || outbound.connID != original.ConnID() || outbound.netCore != entry.core || target.sessions[outbound.address] != outbound || !outbound.authOK || outbound.proven == nil {
			return false
		}
		proof, proven := outbound.proven.Proof()
		peer, known := proof.Identity()
		return proven && known && peer == rid && outbound.proven.Identity == rid
	})
	rslot, tslot := relay.ownedFindNodeEndpointTest, target.ownedFindNodeEndpointTest
	if rslot == nil || tslot == nil || rslot == tslot || rslot.relayTraceReady.Load() || rslot.targetTraceReady.Load() || tslot.relayTraceReady.Load() || tslot.targetTraceReady.Load() {
		t.Fatal("missing/nonempty original trace slots")
	}
	beforeR := relay.bridgeCapture.intake.ResourceSnapshot()
	beforeT := target.bridgeCapture.intake.ResourceSnapshot()
	raw, request := ownedReceiveWire(t, tid)
	if err := source.write([]byte(raw + "\n")); err != nil {
		t.Fatal(err)
	}
	var answer protocol.FindNodeAnswer
	select {
	case answer = <-source.answers:
	case err := <-source.errors:
		t.Fatal("actual upstream answer reader", err)
	case <-time.After(2 * time.Second):
		t.Fatal("original R/T round/sign/writer did not return answer")
	}
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
		t.Fatal("original target signature/upstream rewrite mismatch", err)
	}
	waitFor(t, 2*time.Second, "original six-point handoffs/writer outcome unavailable", func() bool {
		if !rslot.relayTraceReady.Load() || !tslot.targetTraceReady.Load() {
			return false
		}
		r := rslot.relayTraceWork.RelayEndpointObservation()
		x := tslot.targetTraceWork.TargetEndpointObservation()
		return r.ChainKnown && x.PairKnown && r.WriteOutcome == findnode.OwnedRelayWriteReportedOK
	})
	r := rslot.relayTraceWork.RelayEndpointObservation()
	x := tslot.targetTraceWork.TargetEndpointObservation()
	if r.Clock != findnode.OwnedRelayEndpointClockRuntime || !r.RelayKnown || !r.ReceivedKnown || !r.ForwardKnown || !r.AnswerReceivedKnown || !r.AnswerStartKnown || !r.ChainKnown || r.Generation == 0 || r.ReceivedNS < 0 || r.ReceivedNS > r.ForwardNS || r.ForwardNS > r.AnswerReceivedNS || r.AnswerReceivedNS > r.AnswerStartNS || r.SpanNS != r.AnswerStartNS-r.ReceivedNS {
		t.Fatal("original R a/f/h/u provenance/order absent", r)
	}
	if x.Clock != findnode.OwnedTargetEndpointClockRuntime || !x.ReceivedKnown || !x.TakeKnown || !x.CompleteKnown || !x.PairKnown || x.ReceivedNS < 0 || x.TakeNS < x.ReceivedNS || x.CompleteNS < x.TakeNS || x.SpanNS != x.CompleteNS-x.ReceivedNS {
		t.Fatal("original T r/Take/c provenance/order absent", x)
	}
	// Separate same-T-owner spans: service r->c includes waiting before Take.
	// The reserve span is observed only, never a new 500ms success condition.
	targetReserveNS := x.CompleteNS - x.TakeNS
	if targetReserveNS > x.SpanNS || x.SpanNS-targetReserveNS != x.TakeNS-x.ReceivedNS {
		t.Fatal("target reserve span was conflated with r-to-c service span", x)
	}
	if r.SourceIdentity != source.id || r.TargetIdentity != tid || r.RequestID != uint64(request.ID) || r.RequestTarget != request.Target || r.RequestChallenge != request.Challenge || r.ForwardedTarget != request.Target || r.ForwardedChallenge != request.Challenge || x.SourceIdentity != rid || x.TargetIdentity != tid || x.RequestID != r.ForwardedID || x.Challenge != r.ForwardedChallenge || r.SourceConnID == 0 || r.TargetConnID == 0 || x.SourceConnID == 0 {
		t.Fatal("automatic exact request/forwarded/authenticated-R join failed", r, x, request)
	}
	if r.SourceOpenedNS < 0 || r.SourceOpenedNS > r.ReceivedNS || r.TargetOpenedNS < 0 || r.TargetOpenedNS > r.ReceivedNS || x.SourceOpenedNS < 0 || x.SourceOpenedNS > x.ReceivedNS {
		t.Fatal("original process-local session tuple missing", r, x)
	}
	afterR := relay.bridgeCapture.intake.ResourceSnapshot()
	afterT := target.bridgeCapture.intake.ResourceSnapshot()
	if afterR.Intake.Committed != beforeR.Intake.Committed+1 || afterR.Intake.Taken != beforeR.Intake.Taken+1 || afterT.Local.Local.Consumed != beforeT.Local.Local.Consumed+1 || afterT.Local.Local.Completed != beforeT.Local.Local.Completed+1 || afterT.Local.CalendarPending != 0 || rslot.targetTraceReady.Load() || tslot.relayTraceReady.Load() {
		t.Fatal("original automatic debit/signing path or exact retention role changed", beforeR, afterR, beforeT, afterT)
	}
	copyR, copyT := r, x
	copyR.RequestChallenge[0]++
	copyT.Challenge[0]++
	copyT.TakeNS = -1
	if rslot.relayTraceWork.RelayEndpointObservation() != r || tslot.targetTraceWork.TargetEndpointObservation() != x {
		t.Fatal("readout aliased mutable caller copies", copyR, copyT)
	}
	// ConnID/OpenedNS are not compared across nodes/processes. R ordering and
	// T ordering use their own original domains; wire+signature supplies only
	// causal correlation. u/write report and received answer remain separate.
	// No cross-clock subtraction, physical calibration or six-second/G2 claim.

	// Emit one value record only after all original assertions. Complete records
	// still require the parent PASS and original closed-child qualification;
	// cleanup runs after this function returns and can invalidate the attempt.
	record, err := json.Marshal(struct {
		Version  string                                  `json:"version"`
		Case     string                                  `json:"case"`
		Profile  string                                  `json:"profile"`
		Complete bool                                    `json:"complete"`
		Request  protocol.FindNodeRequest                `json:"request"`
		Relay    findnode.OwnedRelayEndpointObservation  `json:"relay"`
		Target   findnode.OwnedTargetEndpointObservation `json:"target"`
	}{
		Version:  "corsa.d1.p0.R4-T3.v1",
		Case:     "TestOwnedAutomaticEndpointTraceOriginalSixPoints",
		Profile:  "small2-active2-members2-visits4",
		Complete: true, Request: request, Relay: r, Target: x,
	})
	if err != nil || len(record)+len("D1_P0_R4_T3_V1 ") >= 8*1024 {
		t.Fatal("original endpoint value record is not bounded", len(record), err)
	}
	t.Logf("D1_P0_R4_T3_V1 %s", record)
}
