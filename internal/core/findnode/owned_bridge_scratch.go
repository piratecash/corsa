package findnode

import (
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
)

// A single private lane belongs to the original dormant owner. Fixed scratch
// bounds active preparation, not aggregate retained handles or live admission.
type ownedAttemptScratch struct {
	borrowed     atomic.Bool
	targets      [MaxPendingTotal]ownedProjectionTarget
	targetCount  int
	offered      [MaxPendingTotal]ownedProjectionSource
	offeredCount int
	maximum      domain.ConnID
}
type ownedAttemptClaim struct {
	owner     *dormantIntakeState
	cut       *ownedCutState
	phase     atomic.Uint32
	projected bool
	roundPlan *ownedRoundPlanState
	execution *ownedRoundExecution
}
type ownedAttempt struct{ claim *ownedAttemptClaim }
type ownedProjectionTarget struct {
	identity domain.PeerIdentity
	oldest   [4]Session
}
type ownedProjectionSource struct {
	session Session
	class   SessionClass
}

const (
	ownedAttemptLive    uint32 = 1
	ownedAttemptBusy    uint32 = 2
	ownedAttemptClosing uint32 = 3
	ownedAttemptClosed  uint32 = 4
)

func (intake DormantIntake) tryOwnedAttempt(cut OwnedCut) (ownedAttempt, error) {
	owner, sealed := intake.dormantOwner, cut.sealedCut
	if owner == nil || sealed == nil {
		return ownedAttempt{}, ErrInvalid
	}
	if sealed.owner != owner {
		return ownedAttempt{}, ErrUnauthenticated
	}
	if owner.maxMembers <= 0 || sealed.originals.members == nil || len(sealed.originals.members) > owner.maxMembers || len(sealed.classes) != len(sealed.originals.members) {
		return ownedAttempt{}, ErrInvalid
	}
	if !owner.scratch.borrowed.CompareAndSwap(false, true) {
		return ownedAttempt{}, ErrNotReady
	}
	// No claim allocation exists before the sole successful owner-lane CAS.
	claim := &ownedAttemptClaim{owner: owner, cut: sealed}
	claim.phase.Store(ownedAttemptLive)
	return ownedAttempt{claim: claim}, nil
}

func (attempt ownedAttempt) projectOwnedMembership(targets []domain.PeerIdentity, offered []Session) error {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner {
		return ErrInvalid
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return ErrStaleWork
		}
		if phase == ownedAttemptLive || phase == ownedAttemptBusy {
			return ErrNotReady
		}
		return ErrInvalid
	}
	defer c.phase.Store(ownedAttemptLive)
	if c.projected {
		return ErrInvalid
	}
	s := &c.owner.scratch
	clear(s.targets[:])
	clear(s.offered[:])
	s.targetCount, s.offeredCount, s.maximum = 0, 0, 0
	var projectionErr error
projection:
	for {
		// Input cardinalities are checked BEFORE deduplication, including N=0.
		if len(targets) > MaxPendingTotal || len(offered) > MaxPendingTotal || len(c.cut.originals.members) > c.owner.maxMembers || len(c.cut.classes) != len(c.cut.originals.members) {
			projectionErr = ErrInvalid
			break projection
		}
		for _, identity := range targets {
			if identity.IsZero() {
				projectionErr = ErrInvalid
				break projection
			}
			duplicate := false
			for index := 0; index < s.targetCount; index++ {
				if s.targets[index].identity == identity {
					duplicate = true
					break
				}
			}
			if !duplicate {
				s.targets[s.targetCount].identity = identity
				s.targetCount++
			}
		}
		for _, session := range offered {
			duplicate := false
			for index := 0; index < s.offeredCount; index++ {
				if s.offered[index].session == session {
					duplicate = true
					break
				}
			}
			if !duplicate {
				s.offered[s.offeredCount].session = session
				s.offeredCount++
			}
		}
		q := Instant(c.cut.at.Elapsed())
		// The entire immutable original vector is scanned even when D=P=0.
		for originalIndex, source := range c.cut.originals.members {
			class := c.cut.classes[originalIndex]
			switch class {
			case ClassOrdinary, ClassContact, ClassSelected, ClassIncomingPinned:
			default:
				projectionErr = ErrInvalid
				break projection
			}
			if source.state == nil || source.state.connID == 0 || source.state.identity.IsZero() || source.state.group == "" || source.state.opened < 0 || source.state.opened > q {
				projectionErr = ErrInvalid
				break projection
			}
			s.maximum = max(s.maximum, source.ConnID())
			for index := 0; index < s.offeredCount; index++ {
				if s.offered[index].session == source {
					s.offered[index].class = class
				}
			}
			if class == ClassContact || class == ClassSelected || (class != ClassIncomingPinned && q-source.state.opened < candidateGroupWindow) {
				continue
			}
			for targetIndex := 0; targetIndex < s.targetCount; targetIndex++ {
				target := &s.targets[targetIndex]
				if source.Identity() == target.identity {
					continue
				}
				for index, prior := range target.oldest {
					if prior.state != nil && !candidateOlder(source, prior) {
						continue
					}
					copy(target.oldest[index+1:], target.oldest[index:len(target.oldest)-1])
					target.oldest[index] = source
					break
				}
			}
		}
		for index := 0; index < s.offeredCount; index++ {
			if s.offered[index].class == 0 {
				projectionErr = ErrPrioritySnapshot
				break projection
			}
		}
		c.projected = true
		return nil
	}
	// Failure retains the SAME LIVE borrow, but no partial projection or refs.
	clear(s.targets[:])
	clear(s.offered[:])
	s.targetCount, s.offeredCount, s.maximum = 0, 0, 0
	c.projected = false
	return projectionErr
}

func (attempt ownedAttempt) ownedHistoricalPriority(source Session, target domain.PeerIdentity) (Priority, error) {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner {
		return 0, ErrInvalid
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return 0, ErrStaleWork
		}
		if phase == ownedAttemptLive || phase == ownedAttemptBusy {
			return 0, ErrNotReady
		}
		return 0, ErrInvalid
	}
	defer c.phase.Store(ownedAttemptLive)
	if !c.projected {
		return 0, ErrNotReady
	}
	s := &c.owner.scratch
	class := SessionClass(0)
	for index := 0; index < s.offeredCount; index++ {
		if s.offered[index].session == source {
			class = s.offered[index].class
			break
		}
	}
	targetIndex := -1
	for index := 0; index < s.targetCount; index++ {
		if s.targets[index].identity == target {
			targetIndex = index
			break
		}
	}
	if class == 0 || targetIndex < 0 || source.state == nil || source.Identity() == target {
		return 0, ErrPrioritySnapshot
	}
	if class == ClassContact || class == ClassSelected {
		return PriorityProtected, nil
	}
	for _, member := range s.targets[targetIndex].oldest {
		if member == source {
			return PriorityAged, nil
		}
	}
	return PriorityNew, nil
}

func (attempt ownedAttempt) releaseOwnedAttempt() error {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner {
		return ErrInvalid
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptClosing) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return ErrStaleWork
		}
		if phase == ownedAttemptLive || phase == ownedAttemptBusy {
			return ErrNotReady
		}
		return ErrInvalid
	}
	s := &c.owner.scratch
	clear(s.targets[:])
	clear(s.offered[:])
	s.targetCount, s.offeredCount, s.maximum = 0, 0, 0
	c.projected = false
	c.roundPlan = nil
	c.execution = nil
	c.phase.Store(ownedAttemptClosed)
	s.borrowed.Store(false)
	return nil
}
