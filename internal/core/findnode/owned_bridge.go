package findnode

import (
	"strconv"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

// DormantIntakeProfile is an explicit proposal input, never a deployment default.
type DormantIntakeProfile struct {
	Timing        TimingProfile
	Round, Slack  time.Duration
	InitialTokens uint32
}

type ObservedMember struct {
	Session Session
	Class   SessionClass
}
type ObservedDegree struct{ Outgoing, Incoming, Window int }

// Distinct private fields make these capabilities nonconvertible in safe Go.
// Copies retain one allocated owner; no wrapper contains a copied authority.
type DormantIntake struct{ dormantOwner *dormantIntakeState }
type ObservationSink struct{ observationOwner *dormantIntakeState }
type OwnedCut struct{ sealedCut *ownedCutState }

type dormantIntakeState struct {
	resources      ownedWriterResourceCounters // actual typed queue-item custody, diagnostic only
	consumer       findnodebridge.Consumer
	finishObserver findnodebridge.Observer
	localTarget    *ownedLocalTargetState
	profile        DormantIntakeProfile
	maxMembers     int
	coordinator    *intakeCoordinator
	scratch        ownedAttemptScratch
}

func NewDormantIntake(consumer findnodebridge.Consumer, profile DormantIntakeProfile, maxMembers int) (DormantIntake, ObservationSink, error) {
	if !consumer.IsRuntime() {
		return DormantIntake{}, ObservationSink{}, ErrUnauthenticated
	}
	return newDormantIntake(consumer, profile, maxMembers)
}

func NewDormantFixtureIntake(consumer findnodebridge.Consumer, profile DormantIntakeProfile, maxMembers int) (DormantIntake, ObservationSink, error) {
	if !consumer.IsFixture() {
		return DormantIntake{}, ObservationSink{}, ErrUnauthenticated
	}
	return newDormantIntake(consumer, profile, maxMembers)
}

func newDormantIntake(consumer findnodebridge.Consumer, profile DormantIntakeProfile, maxMembers int) (DormantIntake, ObservationSink, error) {
	return newDormantIntakeAt(consumer, profile, maxMembers, 0, candidateCapacityHint{})
}

// Shared sole-constructor path. Bootstrap never resets an escaped owner.
func newDormantIntakeAt(consumer findnodebridge.Consumer, profile DormantIntakeProfile, maxMembers int, start Instant, hint candidateCapacityHint) (DormantIntake, ObservationSink, error) {
	// Two words cover the closed ObservedMember row. Each individual temporary
	// and retained slice is bounded before allocating the one owner or arrays.
	if maxMembers <= 0 || maxMembers > int(^uint(0)>>1)/(2*(strconv.IntSize/8)) || profile.Round <= 0 || profile.Slack < 0 || profile.InitialTokens > 4 {
		return DormantIntake{}, ObservationSink{}, ErrInvalid
	}
	if _, err := profile.Timing.LatestForward(0); err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	if profile.Slack > profile.Timing.Runtime || profile.Round > profile.Timing.Runtime-profile.Slack {
		return DormantIntake{}, ObservationSink{}, ErrInvalid
	}
	coordinator, err := newCapacityIntakeCoordinator(candidateProfile{timing: profile.Timing, round: Instant(profile.Round), slack: Instant(profile.Slack), nodeCap: 512, initialTokens: profile.InitialTokens}, start, hint)
	if err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	owner := &dormantIntakeState{consumer: consumer, profile: profile, maxMembers: maxMembers, coordinator: coordinator}
	return DormantIntake{dormantOwner: owner}, ObservationSink{observationOwner: owner}, nil
}
