package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// This gate belongs only to the bounded test reader. It holds the ORIGINAL
// successful-read receipt before dispatch, without changing its Runtime point.
type readyChainTargetGate struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *readyChainTargetGate) unblock() { g.once.Do(func() { close(g.release) }) }

type readyThreeNodeChain struct {
	initiator, relay, target *Service
	relayReady, targetReady  *findNodeReadyConstruction
	sourceWire, targetWire   f1ProofFixture
	driver                   *d1TestInitiator
	clock                    *atomic.Int64 // existing test-initiator clock only
	targetID                 domain.PeerIdentity
	answers                  chan string
	forwarded                chan protocol.FindNodeRequest
	errors                   chan error
	gate                     *readyChainTargetGate
	// Numeric observation only for the new contention testcase; no receipt escapes.
	receivedTimes chan int64
	// Optional close-only testcase observation AFTER original target dispatch
	// returns; no receipt, clock, result or admission is supplied by this channel.
	targetDispatchDone chan struct{}
	// Optional scheduling only for concurrent target receipt tests. The original
	// bounded read stays outside this lock; the real receive point is sampled once.
	targetReceiptMu *sync.Mutex
}

// Like f1PrepareActualCM, this fixture supplies a real accepted socket reader
// and the original dialler readPeerSession before the actual CM event. Unlike
// the older observation-only helper, the target reader uses the exact owned
// read/admission/dispatch suffix of handleConn. No receipt, Session, CM result,
// proof, reservation or signed answer is manufactured here.
func readyChainStartCM(t *testing.T, chain *readyThreeNodeChain, pair f1ProofFixture, target bool) {
	t.Helper()
	owner, accepted, dialer := pair.fixtureOwner, pair.accepted, pair.dialer
	serverDone, readerDone := make(chan struct{}), make(chan struct{})
	owner.serverDone, owner.readerDone = serverDone, readerDone
	owner.serviceJobs = [2]*Service{accepted, dialer}
	// The already-registered whole-chain cleanup closes BOTH socket pairs
	// before either original owner joins; per-pair LIFO cleanup comes later.
	core := owner.inboundCore
	if core == nil || owner.outboundCore != pair.outbound.netCore {
		t.Fatal("original chain core absent before readers")
	}
	report := func(err error) {
		select {
		case chain.errors <- err:
		default:
		}
	}
	go func() {
		defer close(serverDone)
		reader := bufio.NewReader(core.Conn())
		for {
			var line string
			var receipt ownedFindNodeReceive
			var claimed bool
			var receiveErr, readErr error
			if target && chain.targetReceiptMu != nil {
				// The same two production read/record operations as the wrapper below,
				// with only the competing receive-point clock sections ordered.
				line, readErr = accepted.readInboundCommandLineOwned(reader, pair.inbound.id, pair.inbound)
				if readErr == nil {
					chain.targetReceiptMu.Lock()
					receipt, claimed, receiveErr = accepted.recordOwnedFindNodeLine(pair.inbound, line)
					chain.targetReceiptMu.Unlock()
				}
			} else {
				line, receipt, claimed, receiveErr, readErr = accepted.readInboundOwnedFindNodeLine(reader, pair.inbound.id, pair.inbound)
			}
			if readErr != nil {
				if accepted.runCtx.Err() == nil {
					report(fmt.Errorf("original accepted chain read: %w", readErr))
				}
				return
			}
			if !accepted.admitInboundCommandLine(pair.inbound.id, core.Conn().RemoteAddr().String(), line) {
				report(errors.New("original chain command admission refused"))
				return
			}
			kind, class := classifyFrameLine(line)
			if target && claimed {
				if receiveErr != nil {
					report(fmt.Errorf("original target receipt: %w", receiveErr))
					return
				}
				if class != frameLineNamed || kind != protocol.FindNodeFrameType {
					report(errors.New("unexpected owned target frame"))
					return
				}
				request, err := protocol.ParseFindNodeRequest([]byte(line))
				if err != nil {
					report(err)
					return
				}
				if chain.receivedTimes != nil {
					select {
					case chain.receivedTimes <- int64(receipt.cell.received.Elapsed()):
					default:
						report(errors.New("bounded target receive-time observation full"))
						return
					}
				}
				select {
				case chain.forwarded <- request:
				default:
					report(errors.New("bounded target observation channel full"))
					return
				}
				if chain.gate != nil {
					close(chain.gate.arrived) // these cases submit exactly one lookup
					select {
					case <-chain.gate.release:
					case <-accepted.runCtx.Done():
						return
					}
				}
				dispatchErr := accepted.dispatchOwnedFindNodeReceive(receipt)
				if chain.targetDispatchDone != nil {
					close(chain.targetDispatchDone) // this testcase sends one original lookup per chain
				}
				if dispatchErr != nil {
					report(fmt.Errorf("original target dispatch: %w", dispatchErr))
					return
				}
				continue
			}
			if !target && class == frameLineNamed && kind == protocol.FindNodeAnswerFrameType {
				if claimed {
					report(errors.New("test-only initiator unexpectedly owns executing intake"))
					return
				}
				select {
				case chain.answers <- line:
				default:
					report(errors.New("bounded initiator observation channel full"))
					return
				}
				continue
			}
			if !accepted.handleCommandOwned(pair.inbound.id, line, pair.inbound) {
				if accepted.runCtx.Err() == nil {
					report(errors.New("original chain setup command refused"))
				}
				return
			}
		}
	}()
	go func() {
		defer close(readerDone)
		dialer.readPeerSession(bufio.NewReader(pair.outbound.conn), pair.outbound)
	}()
	// The controlled fixture supplies two real dial slots. The original CM
	// event still owns acceptance/generation, application setup and publication.
	// Never reset the slot slice when adding the second original neighbour.
	cm := dialer.connManager
	cm.mu.Lock()
	cm.generation++
	generation := cm.generation
	cm.slots = append(cm.slots, &slot{Address: pair.outbound.address, State: domain.SlotStateDialing, Generation: generation})
	cm.mu.Unlock()
	cm.handleDialSucceeded(dialer.runCtx, DialSucceeded{Address: pair.outbound.address, ConnectedAddress: pair.outbound.address, SlotGeneration: generation, Session: pair.outbound})
}

func readyChainBuild(t *testing.T, own bool) (*findNodeReadyConstruction, *Service, *ninFixtureClock) {
	t.Helper()
	cfg, id, clock := readyInboundInputs(t)
	// These are explicit scheduling inputs for this instrumented fixture only.
	// The genuine Runtime, all Timing bounds (including Runtime=1s), limits,
	// tokens and signing profile stay original. This is neither a production
	// default nor a measured wire/G2 profile; a missed real window still fails.
	profile := ownedCutProfile()
	profile.Intake.Round = 100 * time.Millisecond
	profile.Intake.Slack = 100 * time.Millisecond
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	options := findNodeReadyOptions{loadedInbound: true}
	if own {
		options.ownDM = &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: time.Minute}}
	}
	s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, options, inboundPinClock{elapsed: clock.read})
	if err != nil || s == nil {
		t.Fatal("loaded original journal did not compose Ready", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	if !r.bindings(s) || !r.finalized || !r.checked || !r.bridge.statisticInstalled || s.inboundPins == nil || s.inboundPins != r.originalInbound || s.inboundPins.service != s || s.inboundPins.store != r.store || s.inboundPins.wake != r.originalInboundWake {
		t.Fatal("composed producer/Service/store/wake differs from original")
	}
	store := s.localContactPinStore()
	store.mu.Lock()
	loaded, pending := store.ninFormat3 && store.ninJournal != nil, !store.ninJournal.strictConfirmed()
	store.mu.Unlock()
	if !loaded || !pending {
		t.Fatal("load was not actual format3 or falsely acknowledged startup obligation")
	}
	if own && (!options.ownDM.port.ValidFor(s, id) || options.ownDM.owner != s.r2OwnDMOwner || options.ownDM.resolver != s.identityResolver) {
		t.Fatal("own-DM did not retain same original Service/resolver")
	}
	// The original loaded-only worker first confirms the pending strict state.
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, s, clock
}

func newReadyThreeNodeChain(t *testing.T, held bool) *readyThreeNodeChain {
	t.Helper()
	x := &readyThreeNodeChain{answers: make(chan string, 4), forwarded: make(chan protocol.FindNodeRequest, 4), errors: make(chan error, 4), clock: new(atomic.Int64)}
	// A is the existing observation-only test initiator, not a product lookup
	// API or executing FixtureDomain. R and T both use genuine Runtime Ready.
	x.initiator, _, _ = ownedCaptureTestService(t, 8, 8, 16)
	x.relayReady, x.relay, _ = readyChainBuild(t, true)
	var ninClock *ninFixtureClock
	x.targetReady, x.target, ninClock = readyChainBuild(t, false)
	if held {
		x.gate = &readyChainTargetGate{arrived: make(chan struct{}), release: make(chan struct{})}
		t.Cleanup(x.gate.unblock)
	}
	// Existing explicit test capabilities enter BOTH original signed TLS
	// introductions. Public Service capabilities/defaults are unchanged.
	x.sourceWire = d1InitiatorPair(t, x.initiator, x.relay, true, true)
	x.targetWire = d1InitiatorPair(t, x.target, x.relay, true, true)
	// Register whole-chain cleanup before launching any new readers. Both raw
	// pairs are closed before either original owner's bounded join can wait.
	t.Cleanup(func() {
		if x.gate != nil {
			x.gate.unblock()
		}
		for _, service := range []*Service{x.initiator, x.relay, x.target} {
			service.runCancel()
			service.closeBackgroundAdmission()
		}
		for _, pair := range []f1ProofFixture{x.sourceWire, x.targetWire} {
			_ = pair.fixtureOwner.rawServer.Close()
			_ = pair.fixtureOwner.rawClient.Close()
		}
		x.sourceWire.fixtureOwner.stopAndJoin(t)
		x.targetWire.fixtureOwner.stopAndJoin(t)
		for _, service := range []*Service{x.initiator, x.relay, x.target} {
			service.WaitBackground()
		}
	})
	readyChainStartCM(t, x, x.sourceWire, false)
	readyChainStartCM(t, x, x.targetWire, true)
	waitFor(t, 2*time.Second, "two original Ready relay CM publications", func() bool { return len(f1Members(t, x.relay).sessions) == 2 })
	for _, pair := range []f1ProofFixture{x.sourceWire, x.targetWire} {
		x.relay.peerMu.RLock()
		p := pair.outbound
		p.common.mu.Lock()
		r := p.findNodeReceipt
		exact := r != nil && r.peer == p && r.common == p.common && r.address == p.address && r.generation == p.selectedGeneration && p.common.findNode != nil && p.common.findNode.published
		p.common.mu.Unlock()
		x.relay.peerMu.RUnlock()
		if !exact {
			t.Fatal("actual CM did not bind the same original relay neighbour")
		}
	}
	// Only the existing N-in fixture clock advances; both R/T Runtime clocks,
	// F1 proof and receive/admission points remain original production values.
	ninClock.advance(10 * time.Minute)
	if err := x.target.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	facts, err := x.target.collectOwnedFindNodeRawFacts(t.Context(), x.target.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 1 || facts.classes[0] != findnode.ClassIncomingPinned || facts.allOriginal[0].Identity() != domain.PeerIdentityFromWire(x.relay.identity.Address) {
		t.Fatal("actual relay original did not earn target's strict N-in class", facts, err)
	}
	x.targetID = domain.PeerIdentityFromWire(x.target.identity.Address)
	x.clock.Store(int64(3 * time.Second))
	relayID := domain.PeerIdentityFromWire(x.relay.identity.Address)
	x.driver = newD1TestInitiator(x.initiator, []domain.PeerIdentity{relayID, x.targetID}, func() time.Duration { return time.Duration(x.clock.Load()) })
	if !x.relayReady.bindings(x.relay) || !x.targetReady.bindings(x.target) || !x.relay.bridgeCapture.consumer.IsRuntime() || !x.target.bridgeCapture.consumer.IsRuntime() {
		t.Fatal("same original Ready R/T bindings changed during setup")
	}
	// Consume ONE source envelope through a genuine local request to R. Its
	// original signing worker answers, but no forwarding alias is allocated.
	// T lookups consequently exercise non-identity R envelope translation;
	// no allocator state or signed answer is injected by the fixture.
	local, _ := d1StartOriginal(t, x.driver, relayID)
	var localLine string
	select {
	case localLine = <-x.answers:
	case err := <-x.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("original Ready R did not complete local envelope prelude")
	}
	localAnswer, err := protocol.ParseFindNodeAnswer([]byte(localLine))
	if err != nil || localAnswer.ID != local.request.ID || protocol.VerifyFindNodeAnswer(localAnswer, local.request, domain.NetworkID(networkName)) != nil {
		t.Fatal("original Ready R local prelude lost source envelope/signature", err)
	}
	if got := x.driver.Receive(local, local.peers[0].common, []byte(localLine)); got.status != d1TestFound || got.err != nil || d1PendingCount(x.driver) != 0 {
		t.Fatal("original initiator did not finish genuine R-local prelude", got)
	}
	waitFor(t, 2*time.Second, "original R-local prelude writer/signing completion", func() bool {
		r := x.relay.bridgeCapture.intake.ResourceSnapshot()
		return r.Local.Local.Completed == 1 && r.Local.CalendarPending == 0 && r.Writer.AnswerItems == 0 && len(x.relay.bridgeCapture.localLoop.slots) == 0
	})
	if r := x.relay.bridgeCapture.intake.ResourceSnapshot(); r.Intake.Committed != 0 || r.Pending != 0 || len(x.forwarded) != 0 {
		t.Fatal("local prelude unexpectedly used R forwarding state", r)
	}
	return x
}

func (x *readyThreeNodeChain) answer(t *testing.T, lookup *d1TestLookup) (string, protocol.FindNodeAnswer) {
	t.Helper()
	var forwarded protocol.FindNodeRequest
	select {
	case forwarded = <-x.forwarded:
	case err := <-x.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatalf("original Ready relay never wrote to original target: sourceID=%d R=%+v T=%+v roundStarted=%v signStarted=%v", lookup.request.ID, x.relay.bridgeCapture.intake.ResourceSnapshot(), x.target.bridgeCapture.intake.ResourceSnapshot(), x.relay.bridgeCapture.roundLoop.started.Load(), x.target.bridgeCapture.localLoop.started.Load())
	}
	if forwarded.Target != lookup.request.Target || forwarded.Challenge != lookup.request.Challenge || forwarded.ID == lookup.request.ID {
		t.Fatal("original R changed retained request or bypassed required envelope translation", forwarded, lookup.request)
	}
	// The genuine local prelude makes this fixture's namespaces numerically
	// different. The real return path must restore the retained source ID
	// while preserving the target's original challenge signature.
	var line string
	select {
	case line = <-x.answers:
	case err := <-x.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("original Ready T/sign/R writer did not return answer")
	}
	answer, err := protocol.ParseFindNodeAnswer([]byte(line))
	if err != nil || answer.ID != lookup.request.ID || protocol.VerifyFindNodeAnswer(answer, lookup.request, domain.NetworkID(networkName)) != nil {
		t.Fatal("actual T signature or R envelope restore failed", answer, err)
	}
	if !x.target.bridgeCapture.localLoop.started.Load() || !x.relay.bridgeCapture.roundLoop.started.Load() {
		t.Fatal("answer bypassed original Ready round/signing workers")
	}
	return line, answer
}

func (x *readyThreeNodeChain) settled(t *testing.T, count uint64) {
	t.Helper()
	waitFor(t, 2*time.Second, "original R/T writers and pending slots complete", func() bool {
		r := x.relay.bridgeCapture.intake.ResourceSnapshot()
		target := x.target.bridgeCapture.intake.ResourceSnapshot()
		return r.Pending == 0 && r.Writer.ForwardItems == 0 && r.Writer.AnswerItems == 0 && target.Local.CalendarPending == 0 && target.Writer.AnswerItems == 0 && len(x.target.bridgeCapture.localLoop.slots) == 0
	})
	r := x.relay.bridgeCapture.intake.ResourceSnapshot()
	target := x.target.bridgeCapture.intake.ResourceSnapshot()
	if r.Intake.Committed != count || r.Intake.Taken != count || r.Intake.Answered != count || r.GroupRecords == 0 || target.Local.Local.Consumed != count || target.Local.Local.Completed != count {
		t.Fatal("actual original commit/forward/correlation/signature counts or retained debt differ", r, target)
	}
	if !x.relayReady.bindings(x.relay) || !x.targetReady.bindings(x.target) {
		t.Fatal("successful original chain replaced Ready/N-in/own-DM bindings")
	}
}

func TestD1OriginalInitiatorReadyRelayReadyTargetChain(t *testing.T) {
	t.Run("Found-three-and-fourth-start-limited", func(t *testing.T) {
		x := newReadyThreeNodeChain(t, false)
		for i := uint64(1); i <= 3; i++ {
			lookup, _ := d1StartOriginal(t, x.driver, x.targetID)
			line, answer := x.answer(t, lookup)
			original := lookup.peers[0]
			if original.core != x.sourceWire.fixtureOwner.inboundCore || original.common != x.sourceWire.inbound.life {
				t.Fatal("initiator lost exact protected reader/writer/common")
			}
			if got := x.driver.Receive(lookup, x.targetWire.inbound.life, []byte(line)); !errors.Is(got.err, findnode.ErrUnauthenticated) || d1PendingCount(x.driver) != 1 {
				t.Fatal("foreign reader completed or consumed original pending lookup", got)
			}
			got := x.driver.Receive(lookup, original.common, []byte(line))
			if got.status != d1TestFound || got.err != nil || got.answer.ID != answer.ID || got.answer.Target != answer.Target || got.answer.Signature != answer.Signature || !bytes.Equal(got.answer.PublicKey.Bytes(), answer.PublicKey.Bytes()) || d1PendingCount(x.driver) != 0 {
				t.Fatal("original complete three-node chain did not finish Found", got)
			}
			if replay := x.driver.Receive(lookup, original.common, []byte(line)); !errors.Is(replay.err, findnode.ErrStaleWork) {
				t.Fatal("original answer completed twice", replay)
			}
			x.settled(t, i)
		}
		if lookup, got := x.driver.Start(t.Context(), x.targetID); lookup != nil || got.status != d1TestLimited || got.err != nil || d1PendingCount(x.driver) != 0 {
			t.Fatal("fourth start bypassed existing three-per-thirty-second initiator gate", got)
		}
		x.settled(t, 3)
	})
	for _, mode := range []string{"cancel-while-target-held", "timeout-equality-while-target-held"} {
		t.Run(mode, func(t *testing.T) {
			x := newReadyThreeNodeChain(t, true)
			lookup, cancel := d1StartOriginal(t, x.driver, x.targetID)
			select {
			case <-x.gate.arrived:
			case err := <-x.errors:
				t.Fatal(err)
			case <-time.After(2 * time.Second):
				t.Fatal("actual target read never reached held original receipt")
			}
			// The target can read before the original relay writer publishes its
			// post-write Finish diagnostics. Observe that completion before Poll.
			waitFor(t, 2*time.Second, "original relay writer Finish before local terminal", func() bool {
				r := x.relay.bridgeCapture.intake.ResourceSnapshot()
				return r.Pending == 1 && r.Intake.Taken == 1 && r.Writer.ForwardItems == 0
			})
			r := x.relay.bridgeCapture.intake.ResourceSnapshot()
			if r.Pending != 1 || r.Intake.Committed != 1 || r.Intake.Taken != 1 {
				t.Fatal("held real target lacks original admitted relay obligation", r)
			}
			if mode == "cancel-while-target-held" {
				cancel()
				if got := x.driver.Poll(lookup); !errors.Is(got.err, context.Canceled) || got.status != "" {
					t.Fatal("original initiator cancellation changed", got)
				}
			} else {
				// Existing test-workload clock only: not a twelve-second delay
				// or refresh of R/T's genuine receive/deadline/sample authority.
				x.clock.Store(int64(lookup.started + peerRequestTimeout))
				if got := x.driver.Poll(lookup); got.status != d1TestTimeout || got.err != nil {
					t.Fatal("original initiator deadline equality changed", got)
				}
			}
			if d1PendingCount(x.driver) != 0 || x.relay.bridgeCapture.intake.ResourceSnapshot().Pending != 1 {
				t.Fatal("local terminal outcome refunded or erased remote accepted work")
			}
			x.gate.unblock()
			line, _ := x.answer(t, lookup)
			if got := x.driver.Receive(lookup, lookup.peers[0].common, []byte(line)); !errors.Is(got.err, findnode.ErrStaleWork) {
				t.Fatal("late original answer revived a released initiator attempt", got)
			}
			x.settled(t, 1)
		})
	}
	t.Run("forged-return-copy-not-original-wire", func(t *testing.T) {
		x := newReadyThreeNodeChain(t, false)
		lookup, _ := d1StartOriginal(t, x.driver, x.targetID)
		line, answer := x.answer(t, lookup)
		answer.Signature[0] ^= 1 // local negative only; original socket bytes unchanged
		forged, err := protocol.MarshalFindNodeAnswer(answer)
		if err != nil {
			t.Fatal(err)
		}
		if got := x.driver.Receive(lookup, lookup.peers[0].common, forged); got.status != d1TestInvalid || got.err != nil || d1PendingCount(x.driver) != 0 {
			t.Fatal("invalid copy completed the original initiator lookup", got)
		}
		if got := x.driver.Receive(lookup, lookup.peers[0].common, []byte(line)); !errors.Is(got.err, findnode.ErrStaleWork) {
			t.Fatal("late valid original reused an already-terminal attempt", got)
		}
		x.settled(t, 1)
	})
}
