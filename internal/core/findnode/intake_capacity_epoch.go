package findnode

import "time"

// LOGICAL-ONLY companion. These private values are deterministic model inputs,
// not authenticated Node observations or a post-mutex real clock port.
const candidateCapacityWindow = Instant(600 * time.Second)
const candidateCapacityHintAge = Instant(24 * time.Hour)

type candidateCapacityDomain struct{ _ byte }
type candidateCapacityHint struct {
	value   int
	age     Instant
	present bool
}
type candidateCapacityCut struct {
	owner    *intakeCoordinator
	domain   *candidateCapacityDomain
	at       Instant
	degree   int
	complete bool
}
type candidateCapacityEpoch struct {
	owner        *intakeCoordinator
	domain       *candidateCapacityDomain
	origin, last Instant
	epoch        uint64
	cap          int
}

// Unused private construction root; the ordinary fixed-cap constructor is
// unchanged. One owner/domain is retained for the lifetime of all old debt.
func newCapacityIntakeCoordinator(profile candidateProfile, start Instant, hint candidateCapacityHint) (*intakeCoordinator, error) {
	o, err := newIntakeCoordinator(profile, start)
	if err != nil {
		return nil, err
	}
	o.capacity = &candidateCapacityEpoch{owner: o, domain: &candidateCapacityDomain{}, origin: start, last: start, cap: candidateBootstrapCap(hint)}
	return o, nil
}
func candidateBootstrapCap(hint candidateCapacityHint) int {
	if !hint.present || hint.value < 0 || hint.age < 0 || hint.age >= candidateCapacityHintAge {
		return 512
	}
	return max(512, 60*min(hint.value, 64))
}
func candidateCapacityEpochAt(origin, now Instant) (uint64, error) {
	if origin < 0 || now < origin {
		return 0, ErrClock
	}
	// Ordered nonnegative operands cannot overflow the subtraction. Skipped
	// epochs use one division, never an attacker-sized replay loop.
	return uint64((now - origin) / candidateCapacityWindow), nil
}

// Requires owner.mu and pending.mu in the coordinator path. now and cuts are
// trusted PRIVATE LOGICAL inputs only. No callback or Node lock is acquired.
func (o *intakeCoordinator) selectCapacityLocked(now Instant, cuts []candidateCapacityCut) (int, error) {
	e := o.capacity
	if e == nil {
		if len(cuts) != 0 {
			return 0, ErrInvalid
		}
		return o.profile.nodeCap, nil
	}
	if e.owner != o || e.domain == nil || len(cuts) > 1 {
		return 0, ErrInvalid
	}
	if now < e.last {
		return 0, ErrClock
	}
	epoch, err := candidateCapacityEpochAt(e.origin, now)
	if err != nil {
		return 0, err
	}
	e.last = now
	// Includes the distinct bootstrap window. A later observation never
	// overwrites a complete cap already frozen in this same epoch.
	if epoch == e.epoch {
		return e.cap, nil
	}
	if len(cuts) != 1 {
		return 0, ErrNotReady
	}
	cut := cuts[0]
	if !cut.complete || cut.owner != o || cut.domain != e.domain || cut.at > now || cut.degree < 0 {
		return 0, ErrNotReady
	}
	cutEpoch, err := candidateCapacityEpochAt(e.origin, cut.at)
	if err != nil || cutEpoch != epoch {
		return 0, ErrNotReady
	}
	e.epoch = epoch
	e.cap = max(512, 60*min(cut.degree, 64))
	return e.cap, nil
}

// Rechecks the frozen state at the same logical instant after the target lock.
// This is NOT a real elapsed-time sample after mutex waits. The future online
// bridge must provide owned clock and membership revalidation separately.
func (o *intakeCoordinator) frozenCapacityLocked(now Instant) (int, error) {
	e := o.capacity
	if e == nil {
		return o.profile.nodeCap, nil
	}
	if e.owner != o || e.domain == nil || e.cap < 512 || e.cap > 3840 {
		return 0, ErrInvalid
	}
	if now < e.last {
		return 0, ErrClock
	}
	epoch, err := candidateCapacityEpochAt(e.origin, now)
	if err != nil {
		return 0, err
	}
	if epoch != e.epoch {
		return 0, ErrNotReady
	}
	return e.cap, nil
}
