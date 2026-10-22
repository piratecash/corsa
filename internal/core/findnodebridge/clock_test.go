package findnodebridge

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestClockRuntimeOriginAndDistinctFixturePoints(t *testing.T) {
	runtime, err := NewRuntimeDomain(make(chan struct{}))
	if err != nil {
		t.Fatal(err)
	}
	obs, err := runtime.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	first, err := obs.Sample()
	if err != nil {
		t.Fatal(err)
	}
	second, err := obs.Sample()
	obs.Release()
	if err != nil || !first.valid() || !second.NotBefore(first) || first.instantKind != runtimeKind {
		t.Fatal("runtime owned origin failed", err)
	}
	fixture, q, _, stamp := domainReady(t)
	q.Store(int64(first.Elapsed()))
	fobs, err := fixture.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fobs.Sample()
	fobs.Release()
	if err != nil {
		t.Fatal(err)
	}
	if fp.NotBefore(first) || first.NotBefore(fp) || fixture.Consumer().Matches(stamp, first) {
		t.Fatal("same numeric time forged cross-kind provenance")
	}
	bound, err := first.BoundaryAfter(time.Second)
	if err != nil || bound.Contains(fp) {
		t.Fatal("boundary accepted fixture point", err)
	}
}

func TestClockInvalidSamplesPoisonWithoutProgress(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  int64
	}{{"negative", -1}, {"saturated", math.MaxInt64}, {"regressed", 9}} {
		t.Run(tc.name, func(t *testing.T) {
			d, q, _ := domainFixture(t)
			obs, err := d.Observer().ReserveClock()
			if err != nil {
				t.Fatal(err)
			}
			q.Store(10)
			first, err := obs.Sample()
			if err != nil || first.Elapsed() != 10 {
				t.Fatal("valid initial sample failed", err)
			}
			q.Store(tc.bad)
			at, err := obs.Sample()
			if !errors.Is(err, errUnavailable) || at != (OwnedInstant{}) || d.fixtureDomain.last != 10 || !d.fixtureDomain.poisoned.Load() {
				t.Fatal("invalid sample advanced or minted", err)
			}
			q.Store(11)
			if at, err := obs.Sample(); err == nil || at != (OwnedInstant{}) || d.fixtureDomain.last != 10 {
				t.Fatal("poison recovered on later clock value")
			}
			obs.Release()
			if _, err := d.Observer().ReserveClock(); !errors.Is(err, errUnavailable) {
				t.Fatal("poisoned observation acquired", err)
			}
			if err := d.Publisher().SealConstruction(); !errors.Is(err, errUnavailable) || d.fixtureDomain.phase != constructing {
				t.Fatal("poison was silently sealed")
			}
		})
	}
}

func TestClockMandatoryCleanupSurvivesPoisonAndGenerationMax(t *testing.T) {
	d, q, done := domainFixture(t)
	d.fixtureDomain.poisoned.Store(true)
	d.fixtureDomain.generation = math.MaxUint64 - 1
	close(done)
	marker := 0
	pub := d.Publisher().BeginPublication()
	if pub == nil {
		t.Fatal("poison vetoed mandatory publication acquisition")
	}
	marker++ // End-like mandatory local memory body, not a Node integration.
	pub.Complete()
	if marker != 1 || d.fixtureDomain.generation != math.MaxUint64 {
		t.Fatal("poison vetoed mandatory cleanup")
	}
	q.Store(-1)
	clocked := d.Publisher().BeginClockedPublication()
	if clocked == nil {
		t.Fatal("poison vetoed mandatory clock publication acquisition")
	}
	if at, err := clocked.Sample(); !errors.Is(err, errUnavailable) || at != (OwnedInstant{}) {
		t.Fatal("poisoned publication minted time", err)
	}
	marker++
	clocked.Complete()
	if marker != 2 || d.fixtureDomain.generation != math.MaxUint64 || !d.fixtureDomain.poisoned.Load() {
		t.Fatal("cleanup generation wrapped or recovered")
	}
	if !d.fixtureDomain.clock.TryLock() {
		t.Fatal("cleanup did not release clock")
	}
	d.fixtureDomain.clock.Unlock()
	if !d.fixtureDomain.auth.TryLock() {
		t.Fatal("cleanup did not release W")
	}
	d.fixtureDomain.auth.Unlock()
}

func TestClockBusyAliasesHaveNoSampleOrTerminalEffects(t *testing.T) {
	for _, role := range []string{"observation", "commit", "clock-publication"} {
		t.Run(role, func(t *testing.T) {
			d, q, _, stamp := domainReady(t)
			var cell *leaseCell
			var sample func() (OwnedInstant, error)
			var terminal func()
			var kind leaseRole
			switch role {
			case "observation":
				lease, err := d.Observer().ReserveClock()
				if err != nil {
					t.Fatal(err)
				}
				alias := *lease
				cell, sample, terminal, kind = lease.observationLease, alias.Sample, lease.Release, observationRole
			case "commit":
				lease, err := d.Consumer().TryReadClock(stamp)
				if err != nil {
					t.Fatal(err)
				}
				alias := *lease
				cell, sample, terminal, kind = lease.commitLease, alias.Sample, lease.Release, commitRole
			case "clock-publication":
				lease := d.Publisher().BeginClockedPublication()
				alias := *lease
				cell, sample, terminal, kind = lease.clockPublicationLease, alias.Sample, lease.Complete, clockPublicationRole
			}
			// Lawful whitebox claim, no injected production callback/hook.
			if err := cell.beginSample(kind); err != nil {
				t.Fatal(err)
			}
			q.Store(-1) // A forbidden raw read would poison this exact domain.
			at, err := sample()
			if !errors.Is(err, errBusy) || at != (OwnedInstant{}) {
				t.Fatal("BUSY alias sampled instead of refusing", err)
			}
			if !domainPanics(terminal) {
				t.Fatal("terminal stole BUSY ownership")
			}
			if cell.state.Load() != leaseBusy || d.fixtureDomain.poisoned.Load() || d.fixtureDomain.last != 0 || d.fixtureDomain.generation != 2 {
				t.Fatal("BUSY method touched clock or generation")
			}
			if d.fixtureDomain.clock.TryLock() {
				d.fixtureDomain.clock.Unlock()
				t.Fatal("BUSY terminal unlocked native clock")
			}
			cell.finishSample()
			q.Store(5)
			if at, err := sample(); err != nil || at.Elapsed() != 5 {
				t.Fatal("released primitive claim did not resume", err)
			}
			terminal()
		})
	}
}

func TestClockReleasedAliasesDoNotReadOrUnlock(t *testing.T) {
	d, q, _ := domainFixture(t)
	obs, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	alias := *obs
	q.Store(4)
	if _, err := obs.Sample(); err != nil {
		t.Fatal(err)
	}
	obs.Release()
	q.Store(-1)
	if at, err := alias.Sample(); !errors.Is(err, errLease) || at != (OwnedInstant{}) {
		t.Fatal("released alias read raw clock", err)
	}
	if d.fixtureDomain.last != 4 || d.fixtureDomain.poisoned.Load() {
		t.Fatal("released alias poisoned or advanced domain")
	}
	if !domainPanics(alias.Release) {
		t.Fatal("released alias had another unlock right")
	}
	next, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal("released alias damaged clock availability", err)
	}
	next.Release()
}

func TestClockCommitDoneAfterRawKeepsOwnedResources(t *testing.T) {
	d, q, done, stamp := domainReady(t)
	first, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	q.Store(1)
	point, err := first.Sample()
	first.Release()
	if err != nil || point.Elapsed() != 1 {
		t.Fatal("open lifecycle commit sample refused", err)
	}
	held, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	close(done)
	q.Store(2)
	at, err := held.Sample()
	if !errors.Is(err, errLifecycle) || at != (OwnedInstant{}) || d.fixtureDomain.last != 2 || d.fixtureDomain.poisoned.Load() {
		t.Fatal("Done was checked before raw or poisoned clock", err)
	}
	if held.commitLease.state.Load() != leaseLive {
		t.Fatal("cancel error consumed lease")
	}
	held.Release()
	// Clocked mandatory publication is not lifecycle admission.
	writer := d.Publisher().BeginClockedPublication()
	q.Store(3)
	if at, err := writer.Sample(); err != nil || at.Elapsed() != 3 {
		t.Fatal("closed Done vetoed cleanup clock", err)
	}
	writer.Complete()
	if !d.Consumer().Matches(stamp, point) {
		t.Fatal("historical point lost pure provenance after Done")
	}
}

func TestClockBoundaryMaxIsArithmeticNotObservation(t *testing.T) {
	d, q, _, stamp := domainReady(t)
	obs, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	q.Store(math.MaxInt64 - 2)
	at, err := obs.Sample()
	if err != nil {
		t.Fatal(err)
	}
	edge, err := at.BoundaryAfter(2)
	if err != nil || edge.Elapsed() != time.Duration(math.MaxInt64) || !edge.Contains(at) {
		t.Fatal("representable exclusive MAX boundary rejected", err)
	}
	empty, err := at.BoundaryAfter(0)
	if err != nil || empty.Contains(at) {
		t.Fatal("zero-width boundary contained original point", err)
	}
	if _, err := at.BoundaryAfter(3); !errors.Is(err, errInvalid) {
		t.Fatal("overflow boundary accepted", err)
	}
	if _, err := at.BoundaryAfter(-1); !errors.Is(err, errInvalid) {
		t.Fatal("negative boundary accepted", err)
	}
	if d.fixtureDomain.last != at.Elapsed() || d.fixtureDomain.poisoned.Load() {
		t.Fatal("pure boundary changed domain")
	}
	q.Store(math.MaxInt64 - 1)
	later, err := obs.Sample()
	if err != nil || !edge.Contains(later) || !later.NotBefore(at) {
		t.Fatal("valid point below MAX lost ordering", err)
	}
	q.Store(math.MaxInt64)
	if maxPoint, err := obs.Sample(); err == nil || maxPoint != (OwnedInstant{}) {
		t.Fatal("raw MAX became observation")
	}
	obs.Release()
	if !edge.Contains(later) || !d.Consumer().Matches(stamp, at) {
		t.Fatal("pure arithmetic changed after poison/release")
	}
	if (OwnedBoundary{}).Contains(at) || at.NotBefore(OwnedInstant{}) {
		t.Fatal("zero pure provenance accepted")
	}
}

func TestClockEqualFixtureSamplesRemainValid(t *testing.T) {
	d, q, _ := domainFixture(t)
	obs, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	q.Store(0)
	first, err := obs.Sample()
	if err != nil || !first.valid() {
		t.Fatal("zero elapsed valid point refused", err)
	}
	second, err := obs.Sample()
	obs.Release()
	if err != nil || !second.NotBefore(first) || !first.NotBefore(second) {
		t.Fatal("equal samples treated as regression", err)
	}
}
