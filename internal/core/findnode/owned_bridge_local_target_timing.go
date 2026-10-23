package findnode

import "time"

// Explicit inactive owned-only service input. No deployment default is chosen.
// Complete(signature) success is signature-ready; reserve covers Take through it.
type OwnedTargetTimingProfile struct {
	ServiceBound   time.Duration
	RuntimeReserve time.Duration
}

func (p OwnedTargetTimingProfile) valid() bool {
	return p.RuntimeReserve > 0 && p.ServiceBound > p.RuntimeReserve && p.ServiceBound < time.Duration(responderTimeout)
}

type ownedTargetServiceWindow struct {
	enabled                           bool
	latestSignStart, signatureReadyBy Instant
}

func (c *coordinatedSigning) startBoundary() Instant {
	if c.serviceWindow.enabled {
		return c.serviceWindow.latestSignStart
	}
	return c.signBy
}
func (c *coordinatedSigning) readyBoundary() Instant {
	if c.serviceWindow.enabled {
		return c.serviceWindow.signatureReadyBy
	}
	return c.signBy
}

// Non-authoritative diagnostic from the original timed admission trial.
// Raw monotonic nanoseconds are data, not an exported Instant/owner capability.
// No job, receipt, calendar, callback or shared state pointer escapes.
type OwnedTargetTimingRefusal struct {
	ReceivedNS, SampledNS, ProjectedNS                int64
	LatestStartNS, SignatureReadyNS, AnswerDeadlineNS int64
}

func (*OwnedTargetTimingRefusal) Error() string        { return ErrBudget.Error() }
func (*OwnedTargetTimingRefusal) Is(target error) bool { return target == ErrBudget }
