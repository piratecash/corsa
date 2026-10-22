package node

import (
	"bufio"
	"crypto/ed25519"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type ownedAnswerCapture struct{ replies chan bool }

func (c ownedAnswerCapture) OnSendAttempt(data []byte, ok bool) {
	kind, class := classifyFrameLine(string(data))
	if class == frameLineNamed && (kind == protocol.FindNodeFrameType || kind == protocol.FindNodeAnswerFrameType) {
		select {
		case c.replies <- ok:
		default:
		}
	}
}

func ownedAnswerSignedBody(t *testing.T, target *Service, request protocol.FindNodeRequest) protocol.FindNodeAnswer {
	t.Helper()
	key, err := identity.ParsePublicKey(target.identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := protocol.BuildFindNodeAnswerTranscript(domain.NetworkID(networkName), request.Target, request.Challenge)
	if err != nil {
		t.Fatal(err)
	}
	var signature domain.FindNodeSignature
	copy(signature[:], ed25519.Sign(target.identity.PrivateKey, transcript))
	return protocol.FindNodeAnswer{ID: request.ID, Target: request.Target, PublicKey: key, Signature: signature}
}

// Real bytes cross the SAME genuine protected socket. These are precisely the
// bounded read + command admission + owned dispatch calls of handleConn; this
// fixture owns its already-authenticated original proof/core and no listener.
func ownedAnswerReadOriginalInbound(t *testing.T, s *Service, f f1ProofFixture) ownedFindNodeReceive {
	t.Helper()
	core := f.fixtureOwner.inboundCore
	if err := core.Conn().SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, receipt, claimed, receiveErr, readErr := s.readInboundOwnedFindNodeLine(bufio.NewReader(core.Conn()), f.inbound.id, f.inbound)
	if readErr != nil || receiveErr != nil || !claimed {
		t.Fatal("original bounded owned ingress", claimed, receiveErr, readErr)
	}
	if !s.admitInboundCommandLine(f.inbound.id, core.Conn().RemoteAddr().String(), line) {
		t.Fatal("fixture original inbound command budget exhausted")
	}
	return receipt
}
func ownedAnswerReadBody(t *testing.T, f f1ProofFixture) protocol.FindNodeAnswer {
	t.Helper()
	if err := f.outbound.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(f.outbound.conn)
	for n := 0; n < 32; n++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal("original source did not receive answer", err)
		}
		answer, err := protocol.ParseFindNodeAnswer([]byte(line))
		if err == nil {
			return answer
		}
	}
	t.Fatal("strict answer absent from bounded original frames")
	return protocol.FindNodeAnswer{}
}
func TestOwnedAnswerThreeNodeOriginalRequestRelayAndSignedReturn(t *testing.T) {
	relay, clock, _ := ownedExecutingTestService(t)
	initiator, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	target, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	sourceWire, _ := ownedRawAccepted(t, relay, initiator)
	targetWire, targetSession := ownedRawAccepted(t, relay, target)
	clock.Store(int64(3 * time.Second))
	wire, request := ownedReceiveWire(t, targetSession.Identity())
	if status := sourceWire.outbound.netCore.SendRaw([]byte(wire + "\n")); status != netcore.SendOK {
		t.Fatal(status)
	}
	if err := relay.dispatchOwnedFindNodeReceive(ownedAnswerReadOriginalInbound(t, relay, sourceWire)); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(3*time.Second + time.Millisecond))
	forwarded := ownedExecutingReadForward(t, targetWire, targetSession.Identity())
	if forwarded.ID != 0 || forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
		t.Fatal("R did not retain exact request with only envelope ID rewritten")
	}
	answer := ownedAnswerSignedBody(t, target, forwarded)
	raw, err := protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	if status := targetWire.outbound.netCore.SendRaw(append(raw, '\n')); status != netcore.SendOK {
		t.Fatal(status)
	}
	if err := relay.dispatchOwnedFindNodeReceive(ownedAnswerReadOriginalInbound(t, relay, targetWire)); err != nil {
		t.Fatal(err)
	}
	returned := ownedAnswerReadBody(t, sourceWire)
	expected := answer
	expected.ID = request.ID
	if !reflect.DeepEqual(returned, expected) {
		t.Fatal("R altered signed answer body or failed original envelope restore")
	}
	if err := protocol.VerifyFindNodeAnswer(returned, request, domain.NetworkID(networkName)); err != nil {
		t.Fatal("retained original challenge verification failed", err)
	}
	wrong := request
	wrong.Challenge[0] ^= 1
	if err := protocol.VerifyFindNodeAnswer(returned, wrong, domain.NetworkID(networkName)); err == nil {
		t.Fatal("initiator accepted answer to another retained challenge")
	}
}
func TestOwnedOutboundCommandGateRefusesBeforeRequestOrAnswerParse(t *testing.T) {
	for _, kind := range []string{protocol.FindNodeFrameType, protocol.FindNodeAnswerFrameType} {
		t.Run(kind, func(t *testing.T) {
			relay, clock, _ := ownedExecutingTestServiceWorker(t, false)
			limiterClock := new(atomic.Int64)
			limiterClock.Store(time.Unix(1, 0).UnixNano())
			relay.cmdLimiter = newCommandRateLimiter(func() time.Time { return time.Unix(0, limiterClock.Load()) }) // before any fixture socket/reader
			remote, _, _ := ownedCaptureTestService(t, 8, 8, 16)
			target, _, _ := ownedCaptureTestService(t, 8, 8, 16)
			f := f1PairOn(t, true, nil, remote, relay)
			start := ownedRawPrepareCMAtSlot(t, f, domain.PeerAddress("127.0.0.1:64007"))
			dispatched := make(chan struct{})
			f.fixtureOwner.caseDone = dispatched
			go func() { defer close(dispatched); start() }()
			select {
			case <-dispatched:
			case <-time.After(2 * time.Second):
				t.Fatal("genuine outbound CM fixture did not join")
			}
			waitFor(t, 2*time.Second, "genuine original outbound member", func() bool { return len(f1Members(t, relay).sessions) == 1 })
			ownedRawAccepted(t, relay, target)
			clock.Store(int64(3 * time.Second))
			f.outbound.common.mu.Lock()
			owner := f.outbound.common.ownerLocked()
			f.outbound.common.mu.Unlock()
			if owner == nil {
				t.Fatal("original CM owner absent")
			}
			// Deliberately malformed after its sole named type. Denial must precede
			// strict decoding and any row/reply; the raw historical point is genuine.
			raw := "{\"type\":\"" + kind + "\",\"v\":1,\"id\":17}"
			receipt, claimed, err := relay.recordOwnedFindNodeLine(owner, raw)
			if err != nil || !claimed {
				t.Fatal(claimed, err)
			}
			key := outboundControlFrameLimitKey(f.outbound.connID)
			denied := false
			for n := 0; n < 1024; n++ {
				if !relay.cmdLimiter.allowCommand(key) {
					denied = true
					break
				}
			}
			if !denied {
				t.Fatal("fixture did not genuinely exhaust original command bucket")
			}
			replies := make(chan bool, 4)
			f.outbound.netCore.SetCaptureSink(ownedAnswerCapture{replies})
			if err := relay.dispatchOutboundOwnedFindNode(f.outbound, receipt); !errors.Is(err, findnode.ErrBudget) {
				t.Fatal("owned branch bypassed original command gate", err)
			}
			if receipt.cell.consumed.Load() || relay.bridgeCapture.intake.OwnedRoundPending() {
				t.Fatal("exhausted command paid parser/offer cost")
			}
			select {
			case <-replies:
				t.Fatal("exhausted original command generated reply")
			default:
			}
			// Same raw cell becomes a strict-parser error after honest bucket refill;
			// it never turns into a valid request or invented response.
			limiterClock.Add(int64(time.Second))
			if err := relay.dispatchOutboundOwnedFindNode(f.outbound, receipt); err == nil || errors.Is(err, findnode.ErrBudget) || !receipt.cell.consumed.Load() {
				t.Fatal("fixture did not reach strict parser only after admission", err)
			}
			if relay.bridgeCapture.intake.OwnedRoundPending() {
				t.Fatal("malformed admitted frame created row")
			}
		})
	}
}

// Fault writer boundary: genuine original Sessions and real Offer/C3b/C3c/
// Take/Match produce the opaque answer. The separate fault endpoint is NOT
// claimed as the original registered core; the three-node test covers that.
func ownedAnswerActualWork(t *testing.T) (*Service, *atomic.Int64, f1ProofFixture, findnode.Session, findnode.OwnedAnswerWork) {
	t.Helper()
	relay, clock, _ := ownedExecutingTestServiceWorker(t, false)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	target, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	sourceWire, source := ownedRawAccepted(t, relay, from)
	targetWire, targetSession := ownedRawAccepted(t, relay, target)
	clock.Store(int64(3 * time.Second))
	if err := ownedExecutingIngress(t, relay, sourceWire.inbound, targetSession); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(3*time.Second + time.Millisecond))
	cut, err := relay.captureOwnedFindNodeCut(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := relay.bridgeCapture.intake.CloseOwnedRound(cut)
	if err != nil {
		t.Fatal(err)
	}
	forward, ok := batch.Next()
	if !ok {
		t.Fatal("original actual forward absent")
	}
	queued, err := forward.QueueForOriginal(targetSession)
	if err != nil {
		t.Fatal(err)
	}
	data, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.ParseFindNodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	answer := ownedAnswerSignedBody(t, target, request)
	raw, err := protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := relay.recordOwnedFindNodeReceive(targetWire.inbound, string(raw))
	if err != nil {
		t.Fatal(err)
	}
	input, err := relay.bridgeCapture.sink.SealAnswer(receipt.cell.stamp, receipt.cell.received, targetSession, raw)
	if err != nil {
		t.Fatal(err)
	}
	work, err := relay.bridgeCapture.intake.MatchOwnedAnswer(input)
	if err != nil {
		t.Fatal(err)
	}
	return relay, clock, sourceWire, source, work
}
func TestOwnedAnswerWriterDeadlinePartialAndErrorRetireOnce(t *testing.T) {
	for _, mode := range []string{"deadline", "partial", "write-error"} {
		t.Run(mode, func(t *testing.T) {
			_, clock, _, source, work := ownedAnswerActualWork(t)
			core, c := ownedWriterFaultCore(t, mode, clock)
			events := make(chan bool, 1)
			core.SetCaptureSink(ownedWriterCapture{func(ok bool) { events <- ok }})
			if status := core.SendOwnedFindNodeAnswer(work, source); status != netcore.SendOK {
				t.Fatal(status)
			}
			ownedWriterWaitDone(t, core, c)
			if _, err := work.QueueAnswerForOriginal(source); !errors.Is(err, findnode.ErrStaleWork) {
				t.Fatal("failed answer cell retained/replayed", err)
			}
			if mode == "deadline" {
				if c.writes.Load() != 0 {
					t.Fatal("deadline failure reached Write")
				}
				select {
				case <-events:
					t.Fatal("deadline failure invented write event")
				default:
				}
			} else {
				if c.writes.Load() != 1 {
					t.Fatal("partial/error writer retried")
				}
				select {
				case ok := <-events:
					if ok {
						t.Fatal("partial/error answer reported full success")
					}
				default:
					t.Fatal("write outcome event absent")
				}
			}
		})
	}
}
func TestOwnedAnswerWriterEndDuringRawWriteKeepsMandatoryFinish(t *testing.T) {
	_, clock, sourceWire, source, work := ownedAnswerActualWork(t)
	core, c := ownedWriterFaultCore(t, "success", clock)
	c.beforeWrite = func() { sourceWire.inbound.end() }
	events := make(chan bool, 1)
	core.SetCaptureSink(ownedWriterCapture{func(ok bool) { events <- ok }})
	if status := core.SendOwnedFindNodeAnswer(work, source); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case ok := <-events:
		if !ok {
			t.Fatal("original End invented byte failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("original End stranded admitted answer")
	}
	select {
	case <-core.WriterDone():
		t.Fatal("post-End answer Finish failed")
	default:
	}
	if _, err := work.QueueAnswerForOriginal(source); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("completed original answer replayed", err)
	}
}
func TestOwnedTypedUnionFailureDrainsBothOriginalVariants(t *testing.T) {
	for _, first := range []string{"forward", "answer"} {
		t.Run(first, func(t *testing.T) {
			_, forwardClock, _, target, forward := ownedWriterActualWork(t)
			_, answerClock, _, source, answer := ownedAnswerActualWork(t)
			clock := forwardClock
			if first == "answer" {
				clock = answerClock
			}
			core, c := ownedWriterFaultCore(t, "write-error", clock)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce atomic.Bool
			unblock := func() {
				if releaseOnce.CompareAndSwap(false, true) {
					close(release)
				}
			}
			t.Cleanup(unblock)
			c.beforeWrite = func() { close(entered); <-release }
			if first == "forward" {
				if status := core.SendOwnedFindNode(forward, target); status != netcore.SendOK {
					t.Fatal(status)
				}
			} else {
				if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
					t.Fatal(status)
				}
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("first actual typed writer did not start")
			}
			if first == "forward" {
				if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
					t.Fatal(status)
				}
			} else {
				if status := core.SendOwnedFindNode(forward, target); status != netcore.SendOK {
					t.Fatal(status)
				}
			}
			unblock()
			ownedWriterWaitDone(t, core, c)
			if _, err := forward.QueueForOriginal(target); !errors.Is(err, findnode.ErrStaleWork) {
				t.Fatal("union drain retained original forward", err)
			}
			if _, err := answer.QueueAnswerForOriginal(source); !errors.Is(err, findnode.ErrStaleWork) {
				t.Fatal("union drain retained original answer", err)
			}
			if c.writes.Load() != 1 {
				t.Fatal("drained typed work reached second raw Write")
			}
		})
	}
}
