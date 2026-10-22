package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

type findNodeOwnedCaptureProfile struct {
	Degree   degreeProfile
	Priority findNodePriorityProfile
}

// Original roles are minted together before Service or any callback escapes.
// The terminal holds the same publisher, immutable expected owner and token.
type findNodeBridgeCaptureContext struct {
	terminal    *findNodeBridgeTerminal
	observer    findnodebridge.Observer
	consumer    findnodebridge.Consumer
	priority    findNodePriorityProfile
	intake      findnode.DormantIntake
	sink        findnode.ObservationSink
	roundLoop   *ownedFindNodeRoundLoop
	localTarget findnode.OwnedLocalTarget
	localLoop   *ownedFindNodeLocalLoop
}

// Sole complete conversion. Observation retains BeginCapture first and q after
// every Node wait. There is no clock sample, lock or context callback after q.
func (s *Service) captureOwnedFindNodeCut(ctx context.Context) (findnode.OwnedCut, error) {
	observation, err := s.captureOwnedFindNodeObservation(ctx)
	if err != nil {
		return findnode.OwnedCut{}, err
	}
	return sealOwnedFindNodeObservation(observation)
}

func sealOwnedFindNodeObservation(observation findNodeOwnedObservation) (findnode.OwnedCut, error) {
	c := observation.owner
	if c == nil || c.sink == (findnode.ObservationSink{}) {
		return findnode.OwnedCut{}, findnode.ErrNotReady
	}
	if len(observation.facts.allOriginal) != len(observation.facts.classes) {
		return findnode.OwnedCut{}, findnode.ErrInvalid
	}
	members := make([]findnode.ObservedMember, len(observation.facts.allOriginal))
	for index, original := range observation.facts.allOriginal {
		members[index] = findnode.ObservedMember{Session: original, Class: observation.facts.classes[index]}
	}
	return c.sink.Seal(observation.stamp, observation.degree.at, observation.degree.degreeValidBefore, members, findnode.ObservedDegree{Outgoing: observation.degree.outgoing, Incoming: observation.degree.incoming, Window: observation.degree.window})
}

// Historical candidate only, not OwnedCut, completeness authority or admission.
// degree.at is the ONE genuine q. No source/target filtering or authority bool.
type findNodeOwnedObservation struct {
	owner  *findNodeBridgeCaptureContext
	stamp  findnodebridge.CaptureStamp
	facts  ownedFindNodeRawFacts
	degree bridgeDegreeProjection
}

// Candidate integration entry. A production Constructing domain refuses before
// ctx callback, allocation or incomplete raw/store/trust state can be touched.
func (s *Service) captureOwnedFindNodeObservation(ctx context.Context) (findNodeOwnedObservation, error) {
	if s == nil || s.bridgeCapture == nil {
		return findNodeOwnedObservation{}, findnode.ErrNotReady
	}
	c := s.bridgeCapture
	stamp, err := c.observer.BeginCapture()
	if err != nil {
		return findNodeOwnedObservation{}, err
	}
	if ctx == nil {
		return findNodeOwnedObservation{}, findnode.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return findNodeOwnedObservation{}, err
	}
	t := c.terminal
	if t == nil || t != s.bridgeTerminal || t.owner == nil || t.owner != s.degree || t.token == nil || t.token != s.proofToken || t.owner.token != t.token || t.owner.bridgePublisher != t.publisher || s.findNode == nil || s.findNode.owned == nil || s.findNode.owned.terminal != t {
		return findNodeOwnedObservation{}, findnode.ErrUnauthenticated
	}
	facts, err := s.collectOwnedFindNodeRawFacts(ctx, c.priority)
	if err != nil {
		return findNodeOwnedObservation{}, err
	}
	// All raw common/peer/store/trust waits are over. Degree is the LAST Node wait.
	projection, err := s.captureBridgeDegreeProjection()
	if err != nil {
		return findNodeOwnedObservation{}, err
	}
	if !c.consumer.Matches(stamp, projection.at) {
		return findNodeOwnedObservation{}, findnode.ErrUnauthenticated
	}
	return findNodeOwnedObservation{owner: c, stamp: stamp, facts: facts, degree: projection}, nil
}
