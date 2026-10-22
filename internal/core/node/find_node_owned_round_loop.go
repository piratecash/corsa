package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"sync/atomic"
	"time"
)

// Exactly one original bounded worker and timer. Timer/wake only schedule an
// attempt; every actual round uses a fresh complete original Node cut.
type ownedFindNodeRoundLoop struct {
	batch   atomic.Pointer[findnode.BatchResourceShape] // diagnostic current scheduler custody
	wake    chan struct{}
	done    <-chan struct{}
	delay   time.Duration
	started atomic.Bool
}

func newOwnedFindNodeRoundLoop(done <-chan struct{}, delay time.Duration) *ownedFindNodeRoundLoop {
	return &ownedFindNodeRoundLoop{wake: make(chan struct{}, 1), done: done, delay: delay}
}
func newFindNodeOwnedRoundCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile) (*Service, error) {
	b, err := prepareFindNodeOwnedRoundConstruction(profile)
	if err != nil {
		return nil, err
	}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}
func prepareFindNodeOwnedRoundConstruction(profile findNodeBridgeProfile) (*findNodeTerminalConstruction, error) {
	b, err := prepareFindNodeOwnedCaptureConstruction(profile.Capture)
	if err != nil {
		return nil, err
	}
	intake, sink, err := findnode.NewExecutingIntake(b.capture.consumer, b.capture.observer, profile.Intake, profile.Capture.Priority.MaxMembers)
	if err != nil {
		b.cancel()
		return nil, err
	}
	b.capture.intake, b.capture.sink = intake, sink
	b.capture.roundLoop = newOwnedFindNodeRoundLoop(b.ctx.Done(), profile.Intake.Round)
	b.findNode.owned.retirement = intake
	return b, nil // Runtime remains Constructing; no SealConstruction here.
}
func (s *Service) signalOwnedFindNodeRound() {
	if s == nil || s.bridgeCapture == nil || s.bridgeCapture.roundLoop == nil {
		return
	}
	loop := s.bridgeCapture.roundLoop
	select {
	case loop.wake <- struct{}{}:
	default:
	}
	if loop.started.CompareAndSwap(false, true) {
		if !s.goOwnedFindNodeLoop(loop.done, func() { s.runOwnedFindNodeRoundLoop(loop) }) {
			loop.started.Store(false)
		}
	}
}
func (s *Service) runOwnedFindNodeRoundLoop(loop *ownedFindNodeRoundLoop) {
	// A single timer exists for this worker. Later requests never reset an
	// already armed collection timer or extend the owner's already fixed cut.
	timer := time.NewTimer(loop.delay)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var tick <-chan time.Time
	for {
		select {
		case <-loop.done:
			return
		case <-loop.wake:
			if tick == nil && s.bridgeCapture.intake.OwnedRoundPending() {
				timer.Reset(loop.delay)
				tick = timer.C
			}
		case <-tick:
			tick = nil
			s.driveOwnedFindNodeRound()
			if s.bridgeCapture.intake.OwnedRoundPending() {
				timer.Reset(loop.delay)
				tick = timer.C
			}
		}
	}
}
func (s *Service) driveOwnedFindNodeRound() {
	cut, err := s.captureOwnedFindNodeCut(s.runCtx)
	if err != nil {
		return
	}
	batch, _ := s.bridgeCapture.intake.CloseOwnedRound(cut)
	if loop := s.bridgeCapture.roundLoop; loop != nil {
		shape := batch.ResourceShape()
		loop.batch.Store(&shape)
		defer loop.batch.Store(nil) // operational scope ends at this function return, not a GC claim
	}
	// Earlier accepted exact receipts survive any later refusal/error. Every
	// successful original work reaches its selected core or explicit cancellation.
	for {
		work, ok := batch.Next()
		if !ok {
			break
		}
		_ = s.queueOwnedFindNodeForward(work)
	}
}
