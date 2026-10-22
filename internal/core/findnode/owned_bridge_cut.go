package findnode

import (
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

// Historical immutable evidence only. This is not a current-generation lease,
// source charge, target-relative selection, frozen live capacity or admission.
type ownedCutState struct {
	owner       *dormantIntakeState
	stamp       findnodebridge.CaptureStamp
	at          findnodebridge.OwnedInstant
	originals   ownedOriginalSet
	classes     []SessionClass
	degree      ObservedDegree
	validBefore findnodebridge.OwnedBoundary
}

func (sink ObservationSink) Seal(stamp findnodebridge.CaptureStamp, observedAt findnodebridge.OwnedInstant, degreeBefore findnodebridge.OwnedBoundary, members []ObservedMember, degree ObservedDegree) (OwnedCut, error) {
	owner := sink.observationOwner
	if owner == nil {
		return OwnedCut{}, ErrInvalid
	}
	if !owner.consumer.Matches(stamp, observedAt) || !degreeBefore.Contains(observedAt) {
		return OwnedCut{}, ErrUnauthenticated
	}
	if len(members) > owner.maxMembers {
		return OwnedCut{}, ErrCapacity
	}
	// Individual bounds precede addition, even for hostile maximum int inputs.
	if degree.Outgoing < 0 || degree.Outgoing > 64 || degree.Incoming < 0 || degree.Incoming > 64 || degree.Window < 0 || degree.Window > 64 || degree.Outgoing+degree.Incoming != degree.Window {
		return OwnedCut{}, ErrInvalid
	}
	q := observedAt.Elapsed()
	originals := make([]Session, len(members))
	classes := make([]SessionClass, len(members))
	for index, member := range members {
		switch member.Class {
		case ClassOrdinary, ClassContact, ClassSelected, ClassIncomingPinned:
		default:
			return OwnedCut{}, ErrInvalid
		}
		if member.Session.state == nil || member.Session.state.opened < 0 || time.Duration(member.Session.state.opened) > q {
			return OwnedCut{}, ErrUnauthenticated
		}
		originals[index], classes[index] = member.Session, member.Class
	}
	prepared, err := prepareOwnedOriginalSet(originals, owner.maxMembers)
	if err != nil {
		return OwnedCut{}, err
	}
	remaining := degreeBefore.Elapsed() - q
	window := time.Duration(candidateCapacityWindow)
	for index, member := range prepared.members {
		if classes[index] != ClassOrdinary {
			continue
		}
		age := q - time.Duration(member.state.opened)
		if age >= window {
			continue
		}
		youngRemaining := window - age
		if _, err := observedAt.BoundaryAfter(youngRemaining); err != nil {
			return OwnedCut{}, err
		}
		remaining = min(remaining, youngRemaining)
	}
	// Origin zero and exact multiples belong to the NEXT capacity epoch.
	capacityRemaining := window - q%window
	if _, err := observedAt.BoundaryAfter(capacityRemaining); err != nil {
		return OwnedCut{}, err
	}
	remaining = min(remaining, capacityRemaining)
	validBefore, err := observedAt.BoundaryAfter(remaining)
	if err != nil {
		return OwnedCut{}, err
	}
	if !validBefore.Contains(observedAt) {
		return OwnedCut{}, ErrInvalid
	}
	return OwnedCut{sealedCut: &ownedCutState{owner: owner, stamp: stamp, at: observedAt, originals: prepared, classes: classes, degree: degree, validBefore: validBefore}}, nil
}
