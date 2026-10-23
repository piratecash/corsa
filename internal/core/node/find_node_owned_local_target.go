package node

import (
	"encoding/base64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

// One original local signer, one worker/timer and a fixed constructor bound.
// Slot custody covers queue residence AND signing; it is acquired BEFORE core
// ReserveReceived. A full Node queue therefore never revokes accepted local T.
// Admission serialization preserves the calendar's projected FIFO order.
type ownedFindNodeLocalLoop struct {
	admission   sync.Mutex
	jobs        chan findnode.OwnedLocalSignWork
	slots       chan struct{}
	done        <-chan struct{}
	retry       time.Duration
	started     atomic.Bool
	signingGate *ownedFindNodeSigningTestGate
}

func newOwnedFindNodeLocalLoop(done <-chan struct{}, limit int, retry time.Duration) *ownedFindNodeLocalLoop {
	return &ownedFindNodeLocalLoop{jobs: make(chan findnode.OwnedLocalSignWork, limit), slots: make(chan struct{}, limit), done: done, retry: retry}
}

// Separate unused constructor: old forwarding/observation constructors retain
// their behavior. Roles, local calendar, retirement and queues precede the first
// Service escape. It deliberately does not SealConstruction or enable Ready.
func newFindNodeOwnedLocalTargetCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile, signing findnode.SigningProfile) (*Service, error) {
	b, err := prepareFindNodeOwnedLocalTargetConstruction(id, profile, signing)
	if err != nil {
		return nil, err
	}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}
func prepareFindNodeOwnedLocalTargetConstruction(id *identity.Identity, profile findNodeBridgeProfile, signing findnode.SigningProfile) (*findNodeTerminalConstruction, error) {
	if id == nil {
		return nil, findnode.ErrInvalid
	}
	key, err := identity.ParsePublicKey(id.PublicKey)
	if err != nil {
		return nil, err
	}
	if identity.Fingerprint(id.PublicKey) != id.Address {
		return nil, findnode.ErrInvalid
	}
	b, err := prepareFindNodeOwnedRoundConstruction(profile)
	if err != nil {
		return nil, err
	}
	local, err := b.capture.intake.NewLocalTarget(key, domain.NetworkID(networkName), signing)
	if err != nil {
		b.cancel()
		return nil, err
	}
	b.capture.localTarget = local
	b.capture.localLoop = newOwnedFindNodeLocalLoop(b.ctx.Done(), signing.MaxReservations, profile.Intake.Round)
	b.findNode.owned.localRetirement = local
	return b, nil
}

// The already admitted original request and complete observation are retained.
// This branch is BEFORE remote-neighbor selection: self has no remote Session.
func (s *Service) dispatchOwnedFindNodeLocalTarget(ingress ownedFindNodeIngress, observation findNodeOwnedObservation) error {
	c := ingress.capture
	if s == nil || c == nil || c != s.bridgeCapture || observation.owner != c || c.localTarget == (findnode.OwnedLocalTarget{}) || c.localLoop == nil {
		return findnode.ErrNotReady
	}
	loop := c.localLoop
	loop.admission.Lock()
	select {
	case <-loop.done:
		loop.admission.Unlock()
		return findnode.ErrNotReady
	default:
	}
	select {
	case loop.slots <- struct{}{}:
	default:
		loop.admission.Unlock()
		return findnode.ErrCapacity
	}
	release := true
	defer func() {
		if release {
			<-loop.slots
		}
		loop.admission.Unlock()
	}()
	if loop.started.CompareAndSwap(false, true) {
		if !s.goOwnedFindNodeLoop(loop.done, func() { s.runOwnedFindNodeLocalLoop(loop) }) {
			loop.started.Store(false)
			return findnode.ErrNotReady
		}
	}
	cut, err := sealOwnedFindNodeObservation(observation)
	if err != nil {
		return err
	}
	input, err := c.sink.SealReceived(ingress.stamp, ingress.received, ingress.source, []byte(ingress.wire))
	if err != nil {
		return err
	}
	result, err := c.localTarget.ReserveReceived(input, cut)
	if err != nil {
		return err
	}
	switch result.Kind {
	case findnode.LocalWaitingExisting:
		return nil
	case findnode.LocalReadyCopy:
		// Queue publication can wait on registry/common; do not hold admission.
		<-loop.slots
		release = false
		loop.admission.Unlock()
		err = s.queueOwnedFindNodeAnswer(result.Copy)
		loop.admission.Lock()
		return err
	case findnode.LocalReserved:
		select {
		case <-loop.done:
			_ = result.Work.Cancel()
			return findnode.ErrNotReady
		case loop.jobs <- result.Work:
			release = false
		}
	default:
		return findnode.ErrInvalid
	}
	return nil
}
func (s *Service) runOwnedFindNodeLocalLoop(loop *ownedFindNodeLocalLoop) {
	timer := time.NewTimer(loop.retry)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	defer func() {
		loop.admission.Lock()
		defer loop.admission.Unlock()
		for {
			select {
			case work := <-loop.jobs:
				_ = work.Cancel()
				<-loop.slots
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-loop.done:
			return
		case work := <-loop.jobs:
			s.runOwnedFindNodeLocalWork(loop, timer, work)
			<-loop.slots
		}
	}
}
func (s *Service) runOwnedFindNodeLocalWork(loop *ownedFindNodeLocalLoop, timer *time.Timer, work findnode.OwnedLocalSignWork) {
	// Before T this retires the exact work; after mandatory Complete it is stale.
	defer func() { _ = work.Cancel() }()
	for {
		delay, err := work.WaitDelay()
		// Clock-only hints have no Busy path. Invalid/ended/poisoned original
		// provenance is terminal; retire exact pending work without refund/retry.
		if err != nil {
			return
		}
		if delay > 0 {
			timer.Reset(delay)
			select {
			case <-loop.done:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-timer.C:
			}
			continue // every timer wake obtains a NEW genuine hint, never an authority
		}
		select {
		case <-loop.done:
			return
		default:
		}
		permit, input, err := work.Take()
		if err != nil {
			// A transient accepted-clock/actual-budget refusal retains the same work.
			// Wait again instead of spinning or replacing its original reservation.
			timer.Reset(loop.retry)
			select {
			case <-loop.done:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-timer.C:
			}
			continue
		}
		answers := s.completeOwnedFindNodeLocalSignature(permit, input)
		for _, answer := range answers {
			_ = s.queueOwnedFindNodeAnswer(answer)
		}
		return
	}
}

// No core/Node/common lock is held across transcript construction or crypto.
// Complete(nil) is mandatory on EVERY signing error and recovered panic. It
// uses the original completion clock even when this exact Session has ended.
func (s *Service) completeOwnedFindNodeLocalSignature(permit findnode.OwnedLocalSignPermit, input findnode.PublicSigningInput) (answers []findnode.OwnedAnswerWork) {
	completed := false
	defer func() {
		if !completed {
			_, _ = permit.Complete(nil)
		}
		if recover() != nil {
			answers = nil
		}
	}()
	// The original mandatory completion defer is installed BEFORE this wait.
	// Only the dedicated test construction can install this close-only gate.
	if loop := s.bridgeCapture.localLoop; loop.signingGate != nil && !loop.signingGate.wait(loop.done) {
		return nil
	}
	transcript, err := input.Transcript()
	if err != nil {
		return nil
	}
	encoded := identity.SignPayload(s.identity, transcript)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) != len(domain.FindNodeSignature{}) {
		return nil
	}
	var signature domain.FindNodeSignature
	copy(signature[:], raw)
	answers, err = permit.Complete(&signature)
	completed = true
	if err != nil {
		return nil
	}
	return answers
}
