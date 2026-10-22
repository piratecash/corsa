package findnodebridge

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func domainFixture(t *testing.T) (FixtureDomain, *atomic.Int64, chan struct{}) {
	t.Helper()
	done, clock := make(chan struct{}), new(atomic.Int64)
	d, err := NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	return d, clock, done
}
func domainReady(t *testing.T) (FixtureDomain, *atomic.Int64, chan struct{}, CaptureStamp) {
	t.Helper()
	d, clock, done := domainFixture(t)
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	return d, clock, done, stamp
}
func domainPanics(body func()) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	body()
	return false
}

// Five seconds is deadlock detection only, not a scheduling/performance bound.
func domainAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("domain handshake did not finish")
	}
}

func TestDomainConstructorsAndRoleIdentity(t *testing.T) {
	if _, err := NewRuntimeDomain(nil); err == nil {
		t.Fatal("nil runtime lifecycle accepted")
	}
	if _, err := NewFixtureDomain(nil, new(atomic.Int64)); err == nil {
		t.Fatal("nil fixture lifecycle accepted")
	}
	if _, err := NewFixtureDomain(make(chan struct{}), nil); err == nil {
		t.Fatal("nil fixture clock accepted")
	}
	runtime, err := NewRuntimeDomain(make(chan struct{}))
	if err != nil {
		t.Fatal(err)
	}
	fixture, _, _ := domainFixture(t)
	if !runtime.Consumer().IsRuntime() || runtime.Consumer().IsFixture() || !fixture.Consumer().IsFixture() || fixture.Consumer().IsRuntime() {
		t.Fatal("immutable runtime/fixture roles confused")
	}
	if (Consumer{}).IsRuntime() || (Consumer{}).IsFixture() {
		t.Fatal("zero consumer has kind")
	}
	forged := RuntimeDomain{runtimeDomain: fixture.fixtureDomain}
	if forged.Consumer().IsRuntime() || forged.Consumer().IsFixture() {
		t.Fatal("wrong actual domain kind escaped wrapper")
	}
	bad := Publisher{publisherBinding: &roleBinding{owner: fixture.fixtureDomain, kind: fixtureKind, role: observerRole}}
	if !domainPanics(func() { bad.BeginPublication() }) {
		t.Fatal("wrong publisher role reached acquisition")
	}
	if fixture.fixtureDomain.generation != 1 {
		t.Fatal("wrong role mutated domain")
	}
}

func TestDomainConstructionSealAndRepeatedPriority(t *testing.T) {
	d, clock, _ := domainFixture(t)
	if _, err := d.Observer().BeginCapture(); !errors.Is(err, errUnavailable) {
		t.Fatal("constructing capture accepted", err)
	}
	fake := CaptureStamp{captureDomain: d.fixtureDomain, captureKind: fixtureKind, generation: 1}
	if r, err := d.Consumer().TryReadClock(fake); r != nil || !errors.Is(err, errUnavailable) {
		t.Fatal("constructing read accepted", err)
	}
	clock.Store(4)
	observation, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	at, err := observation.Sample()
	observation.Release()
	if err != nil || !at.valid() {
		t.Fatal("constructing clock-only mint refused", err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal("first seal refused", err)
	}
	if d.fixtureDomain.phase != ready || d.fixtureDomain.generation != 2 {
		t.Fatal("first seal did not advance exactly once")
	}
	d.fixtureDomain.poisoned.Store(true)
	if err := d.Publisher().SealConstruction(); !errors.Is(err, errAlreadySealed) {
		t.Fatal("Ready repeat lost already-sealed priority", err)
	}
	if d.fixtureDomain.phase != ready || d.fixtureDomain.generation != 2 || !d.fixtureDomain.poisoned.Load() {
		t.Fatal("repeated seal mutated domain")
	}
}

func TestDomainSealExhaustionDoesNotConsumeReady(t *testing.T) {
	for _, generation := range []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64} {
		t.Run(map[uint64]string{math.MaxUint64 - 2: "last-valid", math.MaxUint64 - 1: "reserved-sentinel", math.MaxUint64: "exhausted"}[generation], func(t *testing.T) {
			d, _, _ := domainFixture(t)
			d.fixtureDomain.generation = generation
			err := d.Publisher().SealConstruction()
			if generation == math.MaxUint64-2 {
				if err != nil || d.fixtureDomain.phase != ready || d.fixtureDomain.generation != generation+1 || d.fixtureDomain.poisoned.Load() {
					t.Fatal("last nonexhausted first seal refused", err)
				}
				cleanup := d.Publisher().BeginPublication()
				cleanup.Complete()
				if d.fixtureDomain.generation != math.MaxUint64 || !d.fixtureDomain.poisoned.Load() {
					t.Fatal("completion did not poison exhausted generation")
				}
			} else if !errors.Is(err, errUnavailable) || d.fixtureDomain.phase != constructing || d.fixtureDomain.generation != generation || !d.fixtureDomain.poisoned.Load() {
				t.Fatal("first seal consumed exhausted generation", err)
			}
		})
	}
	t.Run("poisoned", func(t *testing.T) {
		d, _, _ := domainFixture(t)
		d.fixtureDomain.poisoned.Store(true)
		if err := d.Publisher().SealConstruction(); !errors.Is(err, errUnavailable) || d.fixtureDomain.phase != constructing || d.fixtureDomain.generation != 1 {
			t.Fatal("poisoned construction became Ready", err)
		}
	})
}

func TestDomainOriginalGenerationAndPureMatches(t *testing.T) {
	d, q, _, stamp := domainReady(t)
	other, _, _, foreign := domainReady(t)
	if r, err := d.Consumer().TryReadClock(foreign); err == nil || r != nil {
		t.Fatal("foreign capture domain accepted")
	}
	pub := d.Publisher().BeginPublication()
	pub.Complete()
	if r, err := d.Consumer().TryReadClock(stamp); r != nil || !errors.Is(err, errStale) {
		t.Fatal("stale original generation accepted", err)
	}
	q.Store(7)
	obs, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	point, err := obs.Sample()
	obs.Release()
	if err != nil {
		t.Fatal(err)
	}
	d.fixtureDomain.poisoned.Store(true)
	if !d.Consumer().Matches(stamp, point) || other.Consumer().Matches(stamp, point) {
		t.Fatal("pure Matches confused authority or domain")
	}
	if (Consumer{}).Matches(stamp, point) || d.Consumer().Matches(CaptureStamp{}, point) || d.Consumer().Matches(stamp, OwnedInstant{}) {
		t.Fatal("zero provenance matched")
	}
}

func TestDomainNoopAndInvalidLeaseTerminals(t *testing.T) {
	var publisher Publisher
	pub := publisher.BeginPublication()
	if pub == nil || !pub.publicationLease.noop {
		t.Fatal("zero publisher refused cleanup guard")
	}
	alias := *pub
	pub.Complete()
	if !domainPanics(alias.Complete) {
		t.Fatal("noop copy acquired second terminal right")
	}
	clocked := publisher.BeginClockedPublication()
	point, err := clocked.Sample()
	if !errors.Is(err, errNoClock) || point != (OwnedInstant{}) {
		t.Fatal("noop sample invented observation", err)
	}
	clocked.Complete()
	if _, err := clocked.Sample(); !errors.Is(err, errLease) {
		t.Fatal("closed noop sampled", err)
	}
	if !domainPanics(clocked.Complete) {
		t.Fatal("noop clock publication completed twice")
	}
	if err := publisher.SealConstruction(); !errors.Is(err, errInvalid) {
		t.Fatal("zero publisher sealed", err)
	}
	if _, err := (Observer{}).BeginCapture(); err == nil {
		t.Fatal("zero observer captured")
	}
	if _, err := (Consumer{}).TryReadClock(CaptureStamp{}); err == nil {
		t.Fatal("zero consumer acquired")
	}
	var obs *ObservationClock
	if _, err := obs.Sample(); !errors.Is(err, errLease) {
		t.Fatal("nil observation sampled")
	}
	if !domainPanics(obs.Release) || !domainPanics((&CommitReservation{}).Release) || !domainPanics((&Publication{}).Complete) || !domainPanics((&ClockPublication{}).Complete) {
		t.Fatal("nil/zero terminal did not refuse")
	}
}

func TestDomainWrongLeaseRoleHasNoNativeEffects(t *testing.T) {
	d, _, _ := domainFixture(t)
	pub := d.Publisher().BeginPublication()
	wrong := &ObservationClock{observationLease: pub.publicationLease}
	if _, err := wrong.Sample(); !errors.Is(err, errLease) {
		t.Fatal("wrong-role Sample claimed cell", err)
	}
	if !domainPanics(wrong.Release) {
		t.Fatal("wrong-role terminal claimed cell")
	}
	if pub.publicationLease.state.Load() != leaseLive || d.fixtureDomain.generation != 1 {
		t.Fatal("wrong-role view changed ownership")
	}
	badCell := &leaseCell{owner: d.fixtureDomain, kind: runtimeKind, role: observationRole, resources: clockResource}
	badCell.state.Store(leaseLive)
	badKind := &ObservationClock{observationLease: badCell}
	if _, err := badKind.Sample(); !errors.Is(err, errLease) || badCell.state.Load() != leaseLive {
		t.Fatal("foreign actual lease kind reached clock")
	}
	if !domainPanics(badKind.Release) || badCell.state.Load() != leaseLive {
		t.Fatal("foreign actual lease kind unlocked")
	}
	pub.Complete()
	if !d.fixtureDomain.auth.TryLock() {
		t.Fatal("valid publisher did not release W")
	}
	d.fixtureDomain.auth.Unlock()
}

func TestDomainTwoTerminalAliasesOnlyOneWins(t *testing.T) {
	d, _, _ := domainFixture(t)
	pub := d.Publisher().BeginPublication()
	alias := *pub
	start := make(chan struct{})
	results := make(chan bool, 2)
	finished := make(chan struct{})
	go func() { <-start; results <- domainPanics(pub.Complete) }()
	go func() { <-start; results <- domainPanics(alias.Complete) }()
	close(start)
	go func() {
		a, b := <-results, <-results
		if a == b {
			results <- false
		} else {
			results <- true
		}
		close(finished)
	}()
	domainAwait(t, finished)
	if !<-results || d.fixtureDomain.generation != 2 || pub.publicationLease.state.Load() != leaseClosed {
		t.Fatal("terminal aliases did not arbitrate once")
	}
	if !d.fixtureDomain.auth.TryLock() {
		t.Fatal("terminal alias race left W held")
	}
	d.fixtureDomain.auth.Unlock()
}

func TestDomainFailedClockTryDropsReadBeforeWriter(t *testing.T) {
	d, _, _, stamp := domainReady(t)
	obs, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	if r, err := d.Consumer().TryReadClock(stamp); r != nil || !errors.Is(err, errBusy) {
		t.Fatal("held clock did not refuse combined try", err)
	}
	completed := make(chan struct{})
	go func() { pub := d.Publisher().BeginPublication(); pub.Complete(); close(completed) }()
	// Writer must complete while observation still owns clock: no leaked R.
	domainAwait(t, completed)
	obs.Release()
}

func TestDomainHeldWriterAndCombinedReaderDoNotNestWaits(t *testing.T) {
	d, _, _, stamp := domainReady(t)
	pub := d.Publisher().BeginPublication()
	if r, err := d.Consumer().TryReadClock(stamp); r != nil || !errors.Is(err, errBusy) {
		t.Fatal("held W did not refuse read try", err)
	}
	if _, err := d.Observer().BeginCapture(); !errors.Is(err, errBusy) {
		t.Fatal("capture parked or accepted W", err)
	}
	pub.Complete()
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	started, finished := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		writer := d.Publisher().BeginClockedPublication()
		writer.Complete()
		close(finished)
	}()
	domainAwait(t, started)
	reader.Release()
	domainAwait(t, finished)
}
