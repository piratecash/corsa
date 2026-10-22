package findnode

import (
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"time"
)

// NewExecutingBootstrapIntake is a constructor-only companion. The start point
// is genuine original Runtime provenance, not a caller duration. Hint metadata
// only selects the first 600s capacity epoch and grants no membership/T right.
// The observer must independently prove the SAME domain before either handle
// escapes. The coordinator and capacity owner are allocated together exactly once.
func NewExecutingBootstrapIntake(consumer findnodebridge.Consumer, observer findnodebridge.Observer, profile DormantIntakeProfile, maxMembers int, start findnodebridge.OwnedInstant, value uint8, age time.Duration, present bool) (DormantIntake, ObservationSink, error) {
	if !consumer.IsRuntime() || !consumer.MatchesPoint(start) {
		return DormantIntake{}, ObservationSink{}, ErrUnauthenticated
	}
	clock, err := observer.ReserveClock()
	if err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	observed, err := clock.Sample()
	clock.Release()
	if err != nil || !consumer.MatchesPoint(observed) || !observed.NotBefore(start) {
		return DormantIntake{}, ObservationSink{}, ErrUnauthenticated
	}
	hint := candidateCapacityHint{value: int(value), age: Instant(age), present: present}
	intake, sink, err := newDormantIntakeAt(consumer, profile, maxMembers, Instant(start.Elapsed()), hint)
	if err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	intake.dormantOwner.finishObserver = observer
	return intake, sink, nil
}
