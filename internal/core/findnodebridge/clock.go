package findnodebridge

import (
	"math"
	"sync/atomic"
	"time"
)

type leaseRole uint8

const (
	observationRole leaseRole = iota + 1
	commitRole
	publicationRole
	clockPublicationRole
)

type resourceMask uint8

const (
	clockResource resourceMask = 1 << iota
	readResource
	writeResource
)
const (
	leaseLive uint32 = iota + 1
	leaseBusy
	leaseClosing
	leaseClosed
)

// Aliased exported values share this ONE cell; no exported copy owns an atomic.
type leaseCell struct {
	owner     *domain
	kind      domainKind
	role      leaseRole
	resources resourceMask
	noop      bool
	state     atomic.Uint32
}
type ObservationClock struct{ observationLease *leaseCell }
type CommitReservation struct{ commitLease *leaseCell }
type Publication struct{ publicationLease *leaseCell }
type ClockPublication struct{ clockPublicationLease *leaseCell }

func resourcesFor(role leaseRole) resourceMask {
	switch role {
	case observationRole:
		return clockResource
	case commitRole:
		return readResource | clockResource
	case publicationRole:
		return writeResource
	case clockPublicationRole:
		return clockResource | writeResource
	default:
		return 0
	}
}
func newLease(d *domain, role leaseRole, noop bool) *leaseCell {
	c := &leaseCell{owner: d, role: role, noop: noop}
	if !noop {
		c.kind, c.resources = d.kind, resourcesFor(role)
	}
	c.state.Store(leaseLive)
	return c
}
func (c *leaseCell) valid(role leaseRole) bool {
	if c == nil || c.role != role || resourcesFor(role) == 0 {
		return false
	}
	if c.noop {
		return (role == publicationRole || role == clockPublicationRole) && c.owner == nil && c.kind == 0 && c.resources == 0
	}
	return c.owner.identityValid() && c.kind == c.owner.kind && c.resources == resourcesFor(role)
}
func (c *leaseCell) beginSample(role leaseRole) error {
	if role == publicationRole || !c.valid(role) {
		return errLease
	}
	if !c.state.CompareAndSwap(leaseLive, leaseBusy) {
		if c.state.Load() == leaseBusy {
			return errBusy
		}
		return errLease
	}
	return nil
}
func (c *leaseCell) finishSample() { c.state.Store(leaseLive) }
func (c *leaseCell) terminate(role leaseRole) {
	if !c.valid(role) || !c.state.CompareAndSwap(leaseLive, leaseClosing) {
		panic(errLease)
	}
	if !c.noop {
		d := c.owner
		switch role {
		case observationRole:
			d.clock.Unlock()
		case commitRole:
			d.clock.Unlock()
			d.auth.RUnlock()
		case publicationRole:
			d.advanceGenerationLocked()
			d.auth.Unlock()
		case clockPublicationRole:
			d.advanceGenerationLocked()
			d.auth.Unlock()
			d.clock.Unlock()
		}
	}
	c.state.Store(leaseClosed)
}
func (d *domain) advanceGenerationLocked() {
	if d.generation < math.MaxUint64 {
		d.generation++
	}
	if d.generation == math.MaxUint64 {
		d.poisoned.Store(true)
	}
}

// Only a held clock plus BUSY cell reaches here. No auth/phase read, callback,
// mutex acquisition or caller-supplied timestamp exists in this uniform helper.
func (d *domain) sampleLocked() (OwnedInstant, time.Time, error) {
	if d.poisoned.Load() {
		return OwnedInstant{}, time.Time{}, errUnavailable
	}
	var q time.Duration
	var wall time.Time
	if d.kind == runtimeKind {
		sample := time.Now()
		q = sample.Sub(d.origin)
		wall = sample.UTC()
	} else {
		q = time.Duration(d.fixture.Load())
	}
	if q < 0 || q == time.Duration(math.MaxInt64) || q < d.last {
		d.poisoned.Store(true)
		return OwnedInstant{}, time.Time{}, errUnavailable
	}
	// Required POST-raw validity poison check, before HWM update OR mint.
	if d.poisoned.Load() {
		return OwnedInstant{}, time.Time{}, errUnavailable
	}
	d.last = q
	return OwnedInstant{instantDomain: d, instantKind: d.kind, instantElapsed: q}, wall, nil
}
func (c *ObservationClock) Sample() (OwnedInstant, error) {
	if c == nil {
		return OwnedInstant{}, errLease
	}
	cell := c.observationLease
	if err := cell.beginSample(observationRole); err != nil {
		return OwnedInstant{}, err
	}
	defer cell.finishSample()
	at, _, err := cell.owner.sampleLocked()
	return at, err
}
func (c *CommitReservation) Sample() (OwnedInstant, error) {
	if c == nil {
		return OwnedInstant{}, errLease
	}
	cell := c.commitLease
	if err := cell.beginSample(commitRole); err != nil {
		return OwnedInstant{}, err
	}
	defer cell.finishSample()
	at, _, err := cell.owner.sampleLocked()
	if err != nil {
		return OwnedInstant{}, err
	}
	// Done is tested AFTER actual raw q/HWM/mint, never Context callback.
	select {
	case <-cell.owner.done:
		return OwnedInstant{}, errLifecycle
	default:
		return at, nil
	}
}
func (c *ClockPublication) Sample() (OwnedInstant, error) {
	if c == nil {
		return OwnedInstant{}, errLease
	}
	cell := c.clockPublicationLease
	if err := cell.beginSample(clockPublicationRole); err != nil {
		return OwnedInstant{}, err
	}
	defer cell.finishSample()
	if cell.noop {
		return OwnedInstant{}, errNoClock
	}
	at, _, err := cell.owner.sampleLocked()
	return at, err
}

// SampleRuntimePair returns metadata from the same original runtime reading as
// the owned point. Wall time grants no admission authority. Fixture and noop
// guards refuse the pair but retain their mandatory Complete obligation.
func (c *ClockPublication) SampleRuntimePair() (OwnedInstant, time.Time, error) {
	if c == nil {
		return OwnedInstant{}, time.Time{}, errLease
	}
	cell := c.clockPublicationLease
	if err := cell.beginSample(clockPublicationRole); err != nil {
		return OwnedInstant{}, time.Time{}, err
	}
	defer cell.finishSample()
	if cell.noop {
		return OwnedInstant{}, time.Time{}, errNoClock
	}
	if cell.kind != runtimeKind {
		return OwnedInstant{}, time.Time{}, errInvalid
	}
	return cell.owner.sampleLocked()
}
func (c *ObservationClock) Release() {
	if c == nil {
		panic(errLease)
	}
	c.observationLease.terminate(observationRole)
}
func (c *CommitReservation) Release() {
	if c == nil {
		panic(errLease)
	}
	c.commitLease.terminate(commitRole)
}
func (p *Publication) Complete() {
	if p == nil {
		panic(errLease)
	}
	p.publicationLease.terminate(publicationRole)
}
func (p *ClockPublication) Complete() {
	if p == nil {
		panic(errLease)
	}
	p.clockPublicationLease.terminate(clockPublicationRole)
}

type OwnedInstant struct {
	instantDomain  *domain
	instantKind    domainKind
	instantElapsed time.Duration
}
type OwnedBoundary struct {
	boundaryDomain  *domain
	boundaryKind    domainKind
	boundaryElapsed time.Duration
}

func (at OwnedInstant) valid() bool {
	return at.instantDomain.identityValid() && at.instantKind == at.instantDomain.kind && at.instantElapsed >= 0 && at.instantElapsed < time.Duration(math.MaxInt64)
}
func (at OwnedInstant) Elapsed() time.Duration { return at.instantElapsed }
func (at OwnedInstant) NotBefore(other OwnedInstant) bool {
	return at.valid() && other.valid() && at.instantDomain == other.instantDomain && at.instantKind == other.instantKind && at.instantElapsed >= other.instantElapsed
}
func (at OwnedInstant) BoundaryAfter(delta time.Duration) (OwnedBoundary, error) {
	if !at.valid() || delta < 0 || delta > time.Duration(math.MaxInt64)-at.instantElapsed {
		return OwnedBoundary{}, errInvalid
	}
	return OwnedBoundary{boundaryDomain: at.instantDomain, boundaryKind: at.instantKind, boundaryElapsed: at.instantElapsed + delta}, nil
}
func (b OwnedBoundary) Elapsed() time.Duration { return b.boundaryElapsed }
func (b OwnedBoundary) Contains(at OwnedInstant) bool {
	return b.boundaryDomain.identityValid() && b.boundaryKind == b.boundaryDomain.kind && b.boundaryElapsed >= 0 && at.valid() && b.boundaryDomain == at.instantDomain && b.boundaryKind == at.instantKind && at.instantElapsed < b.boundaryElapsed
}
