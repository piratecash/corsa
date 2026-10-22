// Package findnodebridge contains unused ownership and clock primitives.
// It installs no Node, transport, intake, configuration or activation path.
package findnodebridge

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errInvalid       = errors.New("bridge domain: invalid handle")
	errUnavailable   = errors.New("bridge domain: unavailable")
	errBusy          = errors.New("bridge domain: busy")
	errStale         = errors.New("bridge domain: stale capture")
	errLease         = errors.New("bridge domain: consumed or invalid lease")
	errAlreadySealed = errors.New("bridge domain: already sealed")
	errNoClock       = errors.New("bridge domain: noop has no clock")
	errLifecycle     = errors.New("bridge domain: lifecycle closed")
)

type domainKind uint8

const (
	runtimeKind domainKind = iota + 1
	fixtureKind
)

type domainPhase uint8

const (
	constructing domainPhase = iota + 1
	ready
)

type handleRole uint8

const (
	publisherRole handleRole = iota + 1
	observerRole
	consumerRole
)

// Immutable construction identity and clock origin; auth owns phase/generation,
// clock owns last. Only sticky poison crosses these two serialization domains.
type domain struct {
	kind       domainKind
	done       <-chan struct{}
	origin     time.Time
	fixture    *atomic.Int64
	auth       sync.RWMutex
	phase      domainPhase
	generation uint64
	clock      sync.Mutex
	last       time.Duration
	poisoned   atomic.Bool
}

// Different private field names prevent safe named-struct role/kind conversion.
type RuntimeDomain struct{ runtimeDomain *domain }
type FixtureDomain struct{ fixtureDomain *domain }
type roleBinding struct {
	owner *domain
	kind  domainKind
	role  handleRole
}
type Publisher struct{ publisherBinding *roleBinding }
type Observer struct{ observerBinding *roleBinding }
type Consumer struct{ consumerBinding *roleBinding }
type CaptureStamp struct {
	captureDomain *domain
	captureKind   domainKind
	generation    uint64
}

func NewRuntimeDomain(done <-chan struct{}) (RuntimeDomain, error) {
	if done == nil {
		return RuntimeDomain{}, errInvalid
	}
	// The original monotonic component is minted here, never supplied by caller.
	d := &domain{kind: runtimeKind, done: done, origin: time.Now(), phase: constructing, generation: 1}
	return RuntimeDomain{runtimeDomain: d}, nil
}
func NewFixtureDomain(done <-chan struct{}, clock *atomic.Int64) (FixtureDomain, error) {
	if done == nil || clock == nil {
		return FixtureDomain{}, errInvalid
	}
	d := &domain{kind: fixtureKind, done: done, fixture: clock, phase: constructing, generation: 1}
	return FixtureDomain{fixtureDomain: d}, nil
}
func (d *domain) identityValid() bool {
	if d == nil || d.done == nil {
		return false
	}
	switch d.kind {
	case runtimeKind:
		return !d.origin.IsZero() && d.fixture == nil
	case fixtureKind:
		return d.fixture != nil && d.origin.IsZero()
	default:
		return false
	}
}
func bindRole(d *domain, kind domainKind, role handleRole) *roleBinding {
	if !d.identityValid() || d.kind != kind {
		return nil
	}
	return &roleBinding{owner: d, kind: kind, role: role}
}
func (b *roleBinding) valid(role handleRole) bool {
	return b != nil && b.role == role && b.owner.identityValid() && b.kind == b.owner.kind
}
func (d RuntimeDomain) Publisher() Publisher {
	return Publisher{publisherBinding: bindRole(d.runtimeDomain, runtimeKind, publisherRole)}
}
func (d RuntimeDomain) Observer() Observer {
	return Observer{observerBinding: bindRole(d.runtimeDomain, runtimeKind, observerRole)}
}
func (d RuntimeDomain) Consumer() Consumer {
	return Consumer{consumerBinding: bindRole(d.runtimeDomain, runtimeKind, consumerRole)}
}
func (d FixtureDomain) Publisher() Publisher {
	return Publisher{publisherBinding: bindRole(d.fixtureDomain, fixtureKind, publisherRole)}
}
func (d FixtureDomain) Observer() Observer {
	return Observer{observerBinding: bindRole(d.fixtureDomain, fixtureKind, observerRole)}
}
func (d FixtureDomain) Consumer() Consumer {
	return Consumer{consumerBinding: bindRole(d.fixtureDomain, fixtureKind, consumerRole)}
}

func (c Consumer) IsRuntime() bool {
	return c.consumerBinding.valid(consumerRole) && c.consumerBinding.kind == runtimeKind
}
func (c Consumer) IsFixture() bool {
	return c.consumerBinding.valid(consumerRole) && c.consumerBinding.kind == fixtureKind
}
func (s CaptureStamp) identityValid() bool {
	return s.captureDomain.identityValid() && s.captureKind == s.captureDomain.kind && s.generation != 0
}

// Matches compares immutable provenance, never current generation or liveness.
func (c Consumer) Matches(stamp CaptureStamp, at OwnedInstant) bool {
	b := c.consumerBinding
	return b.valid(consumerRole) && stamp.identityValid() && at.valid() && b.owner == stamp.captureDomain && b.owner == at.instantDomain && b.kind == stamp.captureKind && b.kind == at.instantKind
}
func (d *domain) readableLocked() bool {
	return d.phase == ready && d.generation != 0 && d.generation < math.MaxUint64 && !d.poisoned.Load()
}
func (o Observer) BeginCapture() (CaptureStamp, error) {
	b := o.observerBinding
	if !b.valid(observerRole) {
		return CaptureStamp{}, errInvalid
	}
	d := b.owner
	if !d.auth.TryRLock() {
		return CaptureStamp{}, errBusy
	}
	defer d.auth.RUnlock()
	if !d.readableLocked() {
		return CaptureStamp{}, errUnavailable
	}
	return CaptureStamp{captureDomain: d, captureKind: b.kind, generation: d.generation}, nil
}
func (o Observer) ReserveClock() (*ObservationClock, error) {
	b := o.observerBinding
	if !b.valid(observerRole) {
		return nil, errInvalid
	}
	d := b.owner
	if d.poisoned.Load() {
		return nil, errUnavailable
	}
	d.clock.Lock()
	if d.poisoned.Load() {
		d.clock.Unlock()
		return nil, errUnavailable
	}
	return &ObservationClock{observationLease: newLease(d, observationRole, false)}, nil
}
func (c Consumer) TryReadClock(stamp CaptureStamp) (*CommitReservation, error) {
	b := c.consumerBinding
	if !b.valid(consumerRole) || !stamp.identityValid() || b.owner != stamp.captureDomain || b.kind != stamp.captureKind {
		return nil, errInvalid
	}
	d := b.owner
	guard := &CommitReservation{commitLease: newLease(d, commitRole, false)}
	if !d.auth.TryRLock() {
		return nil, errBusy
	}
	if !d.readableLocked() {
		d.auth.RUnlock()
		return nil, errUnavailable
	}
	if d.generation != stamp.generation {
		d.auth.RUnlock()
		return nil, errStale
	}
	if !d.clock.TryLock() {
		d.auth.RUnlock()
		return nil, errBusy
	}
	if d.poisoned.Load() {
		d.clock.Unlock()
		d.auth.RUnlock()
		return nil, errUnavailable
	}
	return guard, nil
}

// ReserveAcceptedClock is only for work ALREADY admitted with exact original
// Session/debt/deadline custody. Historical class/membership cuts do not revoke
// it. All application waits precede this port. Preallocate, then clock->authR:
// clocked publishers use clock->W; ordinary W holders never acquire clock.
// The existing commit Sample performs genuine raw/HWM then Done validation.
func (c Consumer) ReserveAcceptedClock() (CaptureStamp, *CommitReservation, error) {
	b := c.consumerBinding
	if !b.valid(consumerRole) {
		return CaptureStamp{}, nil, errInvalid
	}
	d := b.owner
	guard := &CommitReservation{commitLease: newLease(d, commitRole, false)}
	d.clock.Lock()
	d.auth.RLock()
	if !d.readableLocked() {
		d.auth.RUnlock()
		d.clock.Unlock()
		return CaptureStamp{}, nil, errUnavailable
	}
	stamp := CaptureStamp{captureDomain: d, captureKind: b.kind, generation: d.generation}
	return stamp, guard, nil
}

func (p Publisher) BeginPublication() *Publication {
	b := p.publisherBinding
	if b == nil {
		return &Publication{publicationLease: newLease(nil, publicationRole, true)}
	}
	if !b.valid(publisherRole) {
		panic(errInvalid)
	}
	guard := &Publication{publicationLease: newLease(b.owner, publicationRole, false)}
	b.owner.auth.Lock()
	return guard
}
func (p Publisher) BeginClockedPublication() *ClockPublication {
	b := p.publisherBinding
	if b == nil {
		return &ClockPublication{clockPublicationLease: newLease(nil, clockPublicationRole, true)}
	}
	if !b.valid(publisherRole) {
		panic(errInvalid)
	}
	guard := &ClockPublication{clockPublicationLease: newLease(b.owner, clockPublicationRole, false)}
	b.owner.clock.Lock()
	b.owner.auth.Lock()
	return guard
}
func (p Publisher) SealConstruction() error {
	b := p.publisherBinding
	if !b.valid(publisherRole) {
		return errInvalid
	}
	d := b.owner
	d.auth.Lock()
	defer d.auth.Unlock()
	// Repeat priority is fixed even when the already Ready domain is poisoned.
	if d.phase == ready {
		return errAlreadySealed
	}
	if d.phase != constructing {
		return errInvalid
	}
	if d.generation >= math.MaxUint64-1 {
		d.poisoned.Store(true)
		return errUnavailable
	}
	if d.poisoned.Load() {
		return errUnavailable
	}
	// LP is the preceding false poison load; clock-only later poison is allowed.
	d.generation++
	d.phase = ready
	return nil
}
