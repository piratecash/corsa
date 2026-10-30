package node

import (
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/protocol"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func readyTimedTargetForTest(t *testing.T, arrived chan struct{}, release <-chan struct{}) (*findNodeReadyConstruction, *Service, *ninFixtureClock) {
	t.Helper()
	cfg, id, clock := readyInboundInputs(t)
	profile := ownedCutProfile()
	profile.Intake.Round = 100 * time.Millisecond
	profile.Intake.Slack = 100 * time.Millisecond
	// Existing spacing h; these are ONLY this explicit test's service inputs.
	h := 250 * time.Millisecond
	r, options, err := prepareFindNodeReadyTimedTestConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: h, MaxReservations: 8}, findnode.OwnedTargetTimingProfile{ServiceBound: 3 * h / 2, RuntimeReserve: h / 4}, arrived, release)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, options, inboundPinClock{elapsed: clock.read})
	if err != nil {
		t.Fatal(err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	if !r.bindings(s) || !r.finalized || !r.checked {
		t.Fatal("timed target did not retain coherent original Ready")
	}
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, s, clock
}
func TestOwnedReadyTargetTimingFourGenuineSources(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	targetReady, target, ninClock := readyTimedTargetForTest(t, arrived, release)
	targetID := domain.PeerIdentityFromWire(target.identity.Address)
	var chains [4]*readyThreeNodeChain
	// Four real reads and relay rounds remain concurrent. Only their target
	// receipt clock sections are serialized; no receipt or timestamp is supplied.
	var targetReceiptMu sync.Mutex
	for i := range chains {
		x := &readyThreeNodeChain{target: target, targetReady: targetReady, targetID: targetID, answers: make(chan string, 4), forwarded: make(chan protocol.FindNodeRequest, 4), errors: make(chan error, 4), receivedTimes: make(chan int64, 1), targetDispatchDone: make(chan struct{}), targetReceiptMu: &targetReceiptMu, gate: &readyChainTargetGate{arrived: make(chan struct{}), release: make(chan struct{})}, clock: new(atomic.Int64)}
		x.initiator, _, _ = ownedCaptureTestService(t, 8, 8, 16)
		x.relayReady, x.relay, _ = readyChainBuild(t, true)
		x.sourceWire = d1InitiatorPair(t, x.initiator, x.relay, true, true)
		x.targetWire = d1InitiatorPair(t, target, x.relay, true, true)
		x.clock.Store(int64(3 * time.Second))
		x.driver = newD1TestInitiator(x.initiator, []domain.PeerIdentity{targetID}, func() time.Duration { return time.Duration(x.clock.Load()) })
		chains[i] = x
	}
	// Register before any reader: cancel/release and close ALL raw pairs before
	// either original owner waits. Existing per-pair cleanup remains intact.
	t.Cleanup(func() {
		unblock()
		// Every retained real-read gate is released BEFORE cancellation/owner joins.
		for _, x := range chains {
			x.gate.unblock()
		}
		target.runCancel()
		target.closeBackgroundAdmission()
		for _, x := range chains {
			x.relay.runCancel()
			x.relay.closeBackgroundAdmission()
			x.initiator.runCancel()
			x.initiator.closeBackgroundAdmission()
		}
		for _, x := range chains {
			for _, p := range []f1ProofFixture{x.sourceWire, x.targetWire} {
				_ = p.fixtureOwner.rawServer.Close()
				_ = p.fixtureOwner.rawClient.Close()
			}
		}
		for _, x := range chains {
			x.sourceWire.fixtureOwner.stopAndJoin(t)
			x.targetWire.fixtureOwner.stopAndJoin(t)
			x.relay.WaitBackground()
			x.initiator.WaitBackground()
		}
		target.WaitBackground()
	})
	// Only this real-CM fixture uses the same finite inbox bound as both
	// production session constructors. The tiny f1 proof-only fixture queue
	// has no reader/CM consumer yet; never replace a live or populated queue.
	for i, x := range chains {
		for _, pair := range []f1ProofFixture{x.sourceWire, x.targetWire} {
			p := pair.outbound
			if p == nil || p.common == nil || pair.fixtureOwner.readerDone != nil || pair.fixtureOwner.serverDone != nil || cap(p.inboxCh) != 1 || len(p.inboxCh) != 0 {
				t.Fatalf("source %d outbound inbox was not an unused pre-reader fixture queue", i)
			}
			p.inboxCh = make(chan protocol.Frame, peerSessionInboxBuffer)
		}
	}
	for _, x := range chains {
		readyChainStartCM(t, x, x.sourceWire, false)
		readyChainStartCM(t, x, x.targetWire, true)
	}
	waitFor(t, 2*time.Second, "four original relay CM roots", func() bool {
		for i, x := range chains {
			for _, pair := range []f1ProofFixture{x.sourceWire, x.targetWire} {
				select {
				case <-pair.fixtureOwner.readerDone:
					p := pair.outbound
					x.relay.peerMu.RLock()
					p.common.mu.Lock()
					ended := p.common.ended
					published := p.common.findNode != nil && p.common.findNode.published
					auth, generation := p.authOK, p.selectedGeneration
					p.common.mu.Unlock()
					x.relay.peerMu.RUnlock()
					t.Fatalf("source %d original relay reader ended before CM roots: conn=%d ended=%v auth=%v published=%v generation=%d inbox-cap=%d", i, p.connID, ended, auth, published, generation, cap(p.inboxCh))
				default:
				}
			}
			if len(f1Members(t, x.relay).sessions) != 2 {
				return false
			}
		}
		return true
	})
	// H1 publication, not merely the relay CM roots, starts each ORIGINAL
	// target leaf's continuous interval. Observe all four before advancing only
	// the existing N-in fixture clock; no proof or class is installed here.
	waitFor(t, 2*time.Second, "four original target published N-in lifetimes", func() bool {
		target.peerMu.RLock()
		defer target.peerMu.RUnlock()
		if len(target.conns) != len(chains) {
			return false
		}
		for _, x := range chains {
			owner := x.targetWire.inbound
			life := owner.life
			life.mu.Lock()
			entry := target.conns[owner.id]
			leaf := life.nin
			exact := entry != nil && entry == life.entry && entry.common == life && entry.core == x.targetWire.fixtureOwner.inboundCore && life.id == owner.id && !life.ended && leaf != nil && leaf.transport == life && leaf.producer == target.inboundPins && leaf.phase == inboundPublished && leaf.ready == time.Duration(ninClock.tick.Load()) && life.findNode != nil && life.findNode.published
			if exact {
				auth := entry.core.Auth()
				exact = auth != nil
				if exact {
					proof, proven := auth.ProvenIdentity()
					identity, identityOK := proof.Identity()
					exact = proven && identityOK && identity == domain.PeerIdentityFromWire(x.relay.identity.Address) && leaf.binding.identity == identity
				}
			}
			life.mu.Unlock()
			if !exact {
				return false
			}
		}
		return true
	})
	ninClock.advance(10 * time.Minute)
	// One original pass selects at most ONE witness and confirms its content.
	// Walk the original cursor four times; the existing class oracle below
	// still requires all four actual originals to earn strict N-in.
	for range chains {
		if err := target.inboundPins.pass(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	facts, err := target.collectOwnedFindNodeRawFacts(t.Context(), target.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 4 {
		t.Fatal("missing four genuine target source originals", err)
	}
	for i := range facts.allOriginal {
		if facts.classes[i] != findnode.ClassIncomingPinned {
			t.Fatal("target source lacks real strict N-in", facts.classes)
		}
	}
	before := target.bridgeCapture.intake.ResourceSnapshot()
	if before.Local.Local.Reserved != 0 || before.Local.Local.Consumed != 0 || before.Local.Cache.Entries != 0 || before.Local.CalendarPending != 0 {
		t.Fatal("target did not start empty", before)
	}
	var lookups [4]*d1TestLookup
	var cancels [4]context.CancelFunc
	var failures [4]error
	for i, x := range chains {
		// Start all four genuine originals ONCE without waiting for a relay round.
		// Each existing target read gate retains its original receipt BEFORE
		// dispatch; the four real relay workers may collect/forward concurrently.
		lookups[i], cancels[i] = d1StartOriginal(t, x.driver, targetID)
		for j := 0; j < i; j++ {
			if lookups[i].request.Challenge == lookups[j].request.Challenge {
				t.Fatal("original challenges unexpectedly collided")
			}
		}
	}
	for i, x := range chains {
		select {
		case <-x.gate.arrived:
		case e := <-x.errors:
			t.Fatalf("source %d failed before genuine target read gate: type=%T error=%v", i, e, e)
		case <-time.After(2 * time.Second):
			t.Fatalf("source %d original target read did not reach predispatch gate", i)
		}
	}
	// A successful target read can precede the original relay writer Finish.
	// Finish all four before starting local T; no synthetic calendar slot,
	// receipt refresh or admission result is installed by these observations.
	waitFor(t, 2*time.Second, "four original relay writer Finish before target dispatch", func() bool {
		for _, x := range chains {
			r := x.relay.bridgeCapture.intake.ResourceSnapshot()
			if r.Pending != 1 || r.Intake.Committed != 1 || r.Intake.Taken != 1 || r.Writer.ForwardItems != 0 {
				return false
			}
		}
		return true
	})
	for i, x := range chains {
		// Dispatch originals serially AFTER relay collection has completed. The
		// previous call returns/releases its Try* leases before the next starts.
		// The first actual signer reaches its existing post-T gate before #2.
		x.gate.unblock()
		select {
		case <-x.targetDispatchDone:
		case e := <-x.errors:
			var detail *findnode.OwnedTargetTimingRefusal
			if !errors.Is(e, findnode.ErrBudget) || !errors.As(e, &detail) {
				t.Fatalf("source %d failed before typed target timing refusal (transport/receive/admission/dispatch): type=%T error=%v", i, e, e)
			}
			failures[i] = e
		case <-time.After(2 * time.Second):
			t.Fatalf("source %d original target dispatch did not return", i)
		}
		if i == 0 {
			// Take returned and released its original clock before the worker
			// entered this existing gate under mandatory Complete cleanup.
			select {
			case <-arrived:
			case e := <-x.errors:
				t.Fatalf("first original signer did not reach gate: type=%T error=%v", e, e)
			case <-time.After(2 * time.Second):
				t.Fatal("actual T never reached precrypto close-only gate")
			}
		}
	}
	waitFor(t, 2*time.Second, "four actual target admissions or finite service refusals", func() bool {
		count := 0
		for i, x := range chains {
			if failures[i] == nil {
				select {
				case e := <-x.errors:
					var detail *findnode.OwnedTargetTimingRefusal
					if !errors.Is(e, findnode.ErrBudget) || !errors.As(e, &detail) {
						t.Fatalf("source %d failed before typed target timing refusal (transport/receive/admission/dispatch): type=%T error=%v", i, e, e)
					}
					failures[i] = e
				default:
				}
			}
			if failures[i] != nil {
				count++
			}
		}
		return int(target.bridgeCapture.intake.ResourceSnapshot().Local.Local.Reserved)+count == 4
	})
	held := target.bridgeCapture.intake.ResourceSnapshot()
	accepted := int(held.Local.Local.Reserved)
	if accepted < 1 || accepted > 3 || held.Local.Local.Consumed != 1 || held.Local.Local.Completed != 0 || held.Local.CalendarPending != accepted-1 || held.Local.AliasPending != accepted-1 || len(target.bridgeCapture.localLoop.slots) != accepted {
		t.Fatal("actual pre/post projected/actual debit custody", before, held)
	}
	var receivedTimes [4]int64
	for i, x := range chains {
		select {
		case receivedTimes[i] = <-x.receivedTimes:
		case <-time.After(2 * time.Second):
			t.Fatal("missing actual original receive-time observation")
		}
	}
	lo, hi := receivedTimes[0], receivedTimes[0]
	for _, at := range receivedTimes {
		lo = min(lo, at)
		hi = max(hi, at)
	}
	// Report the actual ingress span if the causal case is not reached; no
	// unrelated wall-time SLA or synthetic Runtime control is asserted.
	observedIngressSpan := hi - lo
	refused, rangeWitness := 0, false
	for i, x := range chains {
		var forwarded protocol.FindNodeRequest
		select {
		case forwarded = <-x.forwarded:
		case <-time.After(2 * time.Second):
			t.Fatal("original R writer did not deliver retained challenge")
		}
		if forwarded.Target != targetID || forwarded.Challenge != lookups[i].request.Challenge || !x.driver.sameOriginal(lookups[i].peers[0]) {
			t.Fatal("forwarded request lost original pending challenge/transport")
		}
		if failures[i] != nil {
			var detail *findnode.OwnedTargetTimingRefusal
			if !errors.Is(failures[i], findnode.ErrBudget) || !errors.As(failures[i], &detail) {
				t.Fatal("not original typed calendar refusal", failures[i])
			}
			if detail.ProjectedNS < detail.LatestStartNS || detail.ProjectedNS >= detail.AnswerDeadlineNS || detail.SignatureReadyNS-detail.ReceivedNS != int64(375*time.Millisecond) || detail.LatestStartNS != detail.SignatureReadyNS-int64(62500*time.Microsecond) {
				t.Fatal("actual projected slot not between service and original6s bounds", detail)
			}
			if detail.ReceivedNS != receivedTimes[i] || detail.SampledNS < detail.ReceivedNS {
				t.Fatal("actual ingress/trial span invalidates bounded fixture premise", detail)
			}
			rangeWitness = rangeWitness || detail.ProjectedNS > detail.SignatureReadyNS
			refused++
			cancels[i]()
			if got := x.driver.Poll(lookups[i]); !errors.Is(got.err, context.Canceled) || d1PendingCount(x.driver) != 0 {
				t.Fatal("refused source pending lifetime was forged or leaked", got)
			}
		}
	}
	if refused != 4-accepted || !rangeWitness {
		t.Fatal("actual original calendar counterexample not reached", receivedTimes, observedIngressSpan, failures)
	}
	unblock()
	for i, x := range chains {
		if failures[i] != nil {
			continue
		}
		var line string
		select {
		case line = <-x.answers:
		case e := <-x.errors:
			t.Fatal(e)
		case <-time.After(2 * time.Second):
			t.Fatal("accepted original signing/writer did not finish")
		}
		answer, e := protocol.ParseFindNodeAnswer([]byte(line))
		if e != nil || protocol.VerifyFindNodeAnswer(answer, lookups[i].request, domain.NetworkID(networkName)) != nil {
			t.Fatal("not actual signature on retained challenge", e)
		}
		if got := x.driver.Receive(lookups[i], lookups[i].peers[0].common, []byte(line)); got.status != d1TestFound || got.err != nil || d1PendingCount(x.driver) != 0 {
			t.Fatal("original initiator writer/reader did not complete", got)
		}
	}
	waitFor(t, 2*time.Second, "all accepted actual signatures/original writers and target slots released", func() bool {
		r := target.bridgeCapture.intake.ResourceSnapshot()
		return r.Local.Local.Consumed == uint64(accepted) && r.Local.Local.Completed == uint64(accepted) && r.Local.CalendarPending == 0 && r.Writer.AnswerItems == 0 && len(target.bridgeCapture.localLoop.slots) == 0
	})
	for _, x := range chains {
		r := x.relay.bridgeCapture.intake.ResourceSnapshot()
		if r.Intake.Committed != 1 || r.Intake.Taken != 1 || r.GroupRecords == 0 || !x.relayReady.bindings(x.relay) {
			t.Fatal("local refusal/cancellation refunded R admission or replaced Ready", r)
		}
	}
	if !targetReady.bindings(target) {
		t.Fatal("target timing/gate binding changed after Seal")
	}
}

// This uses the normal timed factory, not the close-only test construction.
// The actual format3 journal is independently loaded and uses production time.
func TestOwnedReadyNormalTimedFactoryLoadsOriginalAndSignsWithoutGate(t *testing.T) {
	cfg, id, fixtureClock := readyInboundInputs(t)
	h := 250 * time.Millisecond
	s, err := newFindNodeReadyTimedLoadedInboundCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: h, MaxReservations: 4}, findnode.OwnedTargetTimingProfile{ServiceBound: 3 * h / 2, RuntimeReserve: h / 4})
	if err != nil || s == nil {
		t.Fatal("normal explicit timed factory refused original loaded input", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if s.bridgeCapture == nil || s.bridgeCapture.localLoop == nil || s.bridgeCapture.localLoop.signingGate != nil || !s.bridgeCapture.consumer.IsRuntime() || s.inboundPins == nil || s.inboundPins.service != s || s.ownedDegreeStatisticProducer == nil || !s.ownedDegreeStatisticProducer.installed {
		t.Fatal("normal timed factory lost original Ready or installed test gate")
	}
	store := s.localContactPinStore()
	if store == nil || s.inboundPins.store != store || store.ninElapsed == nil {
		t.Fatal("normal timed factory replaced original loaded store/clock")
	}
	store.mu.Lock()
	loaded := store.ninFormat3 && store.ninJournal != nil
	store.mu.Unlock()
	if !loaded {
		t.Fatal("normal timed factory did not load actual format3")
	}
	reads := fixtureClock.reads.Load()
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if fixtureClock.reads.Load() != reads {
		t.Fatal("normal timed factory retained fixture clock")
	}
	if _, err := s.captureOwnedFindNodeCut(t.Context()); err != nil {
		t.Fatal("normal timed factory has no original Ready cut", err)
	}

	remoteCfg, remoteID := readyTestInputs(t)
	remote, err := newFindNodeReadyCandidateService(remoteCfg, remoteID, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: h, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	remote.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { remote.runCancel(); remote.closeBackgroundAdmission(); remote.WaitBackground() })
	wire, original := ownedRawAccepted(t, s, remote)
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original {
		t.Fatal("normal timed original TLS source was replaced", err)
	}
	before := s.bridgeCapture.intake.ResourceSnapshot()
	if before.Local.Local.Reserved != 0 || before.Local.Local.Consumed != 0 || before.Local.Local.Completed != 0 {
		t.Fatal("normal timed target did not start empty", before)
	}
	request := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, wire, request)); err != nil {
		t.Fatal("normal timed actual local admission refused", err)
	}
	answer := ownedLocalNodeReadAnswer(t, wire)
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
		t.Fatal("normal timed original signature/writer lost retained challenge", err)
	}
	waitFor(t, 2*time.Second, "normal timed actual signing/writer and original slot released", func() bool {
		r := s.bridgeCapture.intake.ResourceSnapshot()
		return r.Local.Local.Reserved == 1 && r.Local.Local.Consumed == 1 && r.Local.Local.Completed == 1 && r.Local.CalendarPending == 0 && r.Writer.AnswerItems == 0 && len(s.bridgeCapture.localLoop.slots) == 0
	})
	if s.bridgeCapture.localLoop.signingGate != nil {
		t.Fatal("normal timed path acquired a test gate")
	}
}
