package trustedclock

// GuardDurationSnapshot is the original by-value epoch summary. COMPLETE is
// conditional source coverage, not verified attribution or physical wall history.
type GuardDurationSnapshot = guardDurationSnapshot

// NewWithGuardObservation attaches the existing source before the Clock can be
// published. Sources remain the same security authority; no fallback rebuild or
// late installation is allowed. New and NewSystem remain ordinary clocks.
func NewWithGuardObservation(sources Sources, instance [16]byte) *Clock {
	return newObservedClock(sources, instance)
}

// GuardDurationSnapshot reads the original fresh cut without normalizing any
// unavailable status or filling its placeholders from Stats. Nil is inactive.
func (c *Clock) GuardDurationSnapshot() GuardDurationSnapshot {
	if c == nil {
		return GuardDurationSnapshot{Status: "INACTIVE"}
	}
	return c.guardDuration()
}
