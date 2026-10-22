package findnodebridge

import (
	"bytes"
	"errors"
	"go/format"
	"math"
	"strings"
	"testing"
	"time"
)

func TestRuntimePairOriginalReadingAndSharedHighWater(t *testing.T) {
	d, err := NewRuntimeDomain(make(chan struct{}))
	if err != nil {
		t.Fatal(err)
	}
	origin := d.runtimeDomain.origin
	publication := d.Publisher().BeginClockedPublication()
	completed := false
	defer func() {
		if !completed {
			publication.Complete()
		}
	}()
	first, err := publication.Sample()
	if err != nil {
		t.Fatal(err)
	}
	at, wall, err := publication.SampleRuntimePair()
	if err != nil || !at.valid() || at.instantDomain != d.runtimeDomain || at.instantKind != runtimeKind || !at.NotBefore(first) {
		t.Fatal("runtime pair lost shared domain or sample order", err)
	}
	if wall.IsZero() || wall.Location() != time.UTC {
		t.Fatal("runtime pair wall is not nonzero UTC metadata", wall)
	}
	last, err := publication.Sample()
	if err != nil || !last.NotBefore(at) || d.runtimeDomain.last != last.Elapsed() || d.runtimeDomain.origin != origin {
		t.Fatal("old Sample and pair do not share original clock HWM", err)
	}
	publication.Complete()
	completed = true
	if d.runtimeDomain.generation != 2 || d.runtimeDomain.phase != constructing {
		t.Fatal("pair changed construction phase or completion count")
	}
}

func TestRuntimePairRejectsFixtureBeforeRead(t *testing.T) {
	d, q, _ := domainFixture(t)
	publication := d.Publisher().BeginClockedPublication()
	completed := false
	defer func() {
		if !completed {
			publication.Complete()
		}
	}()
	d.fixtureDomain.last = 9
	q.Store(-1) // A forbidden fixture read would poison before returning.
	at, wall, err := publication.SampleRuntimePair()
	if !errors.Is(err, errInvalid) || at != (OwnedInstant{}) || !wall.IsZero() || d.fixtureDomain.last != 9 || d.fixtureDomain.poisoned.Load() {
		t.Fatal("fixture pair read clock or manufactured runtime metadata", err)
	}
	if publication.clockPublicationLease.state.Load() != leaseLive {
		t.Fatal("fixture refusal consumed cleanup lease")
	}
	q.Store(10)
	old, err := publication.Sample()
	if err != nil || old.Elapsed() != 10 || d.fixtureDomain.last != 10 {
		t.Fatal("fixture refusal changed existing Sample", err)
	}
	publication.Complete()
	completed = true
	if d.fixtureDomain.generation != 2 {
		t.Fatal("fixture refusal vetoed mandatory completion")
	}
}

func TestRuntimePairNoopAndInvalidViews(t *testing.T) {
	noop := (Publisher{}).BeginClockedPublication()
	alias := *noop
	at, wall, err := noop.SampleRuntimePair()
	if !errors.Is(err, errNoClock) || at != (OwnedInstant{}) || !wall.IsZero() || noop.clockPublicationLease.state.Load() != leaseLive {
		t.Fatal("noop pair minted values or consumed cleanup", err)
	}
	noop.Complete()
	if at, wall, err := alias.SampleRuntimePair(); !errors.Is(err, errLease) || at != (OwnedInstant{}) || !wall.IsZero() {
		t.Fatal("released noop alias sampled", err)
	}
	if !domainPanics(alias.Complete) {
		t.Fatal("noop alias completed twice")
	}
	d, _, _ := domainFixture(t)
	held := d.Publisher().BeginPublication()
	defer held.Complete()
	foreign := newLease(d.fixtureDomain, clockPublicationRole, false)
	foreign.kind = runtimeKind // Invalid immutable view, never runtime evidence.
	for _, tc := range []struct {
		name string
		view *ClockPublication
	}{
		{"nil", nil},
		{"zero", &ClockPublication{}},
		{"wrong-role", &ClockPublication{clockPublicationLease: held.publicationLease}},
		{"foreign-kind", &ClockPublication{clockPublicationLease: foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at, wall, err := tc.view.SampleRuntimePair()
			if !errors.Is(err, errLease) || at != (OwnedInstant{}) || !wall.IsZero() {
				t.Fatal("invalid pair view reached raw sample", err)
			}
			if d.fixtureDomain.last != 0 || d.fixtureDomain.poisoned.Load() || d.fixtureDomain.generation != 1 || held.publicationLease.state.Load() != leaseLive || foreign.state.Load() != leaseLive {
				t.Fatal("invalid pair view changed owner or lease")
			}
		})
	}
}

func TestRuntimePairBusyAndReleasedAliases(t *testing.T) {
	d, err := NewRuntimeDomain(make(chan struct{}))
	if err != nil {
		t.Fatal(err)
	}
	publication := d.Publisher().BeginClockedPublication()
	completed := false
	defer func() {
		if !completed {
			publication.Complete()
		}
	}()
	alias := *publication
	cell := publication.clockPublicationLease
	if err := cell.beginSample(clockPublicationRole); err != nil {
		t.Fatal(err)
	}
	// Existing private BUSY claim is a lawful whitebox ownership fixture, not a
	// production hook or a claimed schedule between raw and postcheck.
	at, wall, err := alias.SampleRuntimePair()
	if !errors.Is(err, errBusy) || at != (OwnedInstant{}) || !wall.IsZero() || !domainPanics(alias.Complete) {
		cell.finishSample()
		t.Fatal("pair alias stole BUSY claim or cleanup", err)
	}
	if cell.state.Load() != leaseBusy || d.runtimeDomain.last != 0 || d.runtimeDomain.generation != 1 || d.runtimeDomain.poisoned.Load() {
		cell.finishSample()
		t.Fatal("BUSY pair changed time or generation")
	}
	if d.runtimeDomain.clock.TryLock() {
		d.runtimeDomain.clock.Unlock()
		cell.finishSample()
		t.Fatal("BUSY terminal released clock")
	}
	if d.runtimeDomain.auth.TryLock() {
		d.runtimeDomain.auth.Unlock()
		cell.finishSample()
		t.Fatal("BUSY terminal released W")
	}
	cell.finishSample()
	point, _, err := publication.SampleRuntimePair()
	if err != nil {
		t.Fatal(err)
	}
	publication.Complete()
	completed = true
	last := d.runtimeDomain.last
	if at, wall, err := alias.SampleRuntimePair(); !errors.Is(err, errLease) || at != (OwnedInstant{}) || !wall.IsZero() || last != point.Elapsed() || d.runtimeDomain.last != last {
		t.Fatal("released pair alias read raw clock", err)
	}
	if !domainPanics(alias.Complete) {
		t.Fatal("released pair alias unlocked twice")
	}
}

func TestRuntimePairErrorsRetainMandatoryCleanup(t *testing.T) {
	for _, name := range []string{"poisoned", "regressed", "closed-done", "exhausted"} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			d, err := NewRuntimeDomain(done)
			if err != nil {
				t.Fatal(err)
			}
			publication := d.Publisher().BeginClockedPublication()
			completed := false
			defer func() {
				if !completed {
					publication.Complete()
				}
			}()
			switch name {
			case "poisoned":
				d.runtimeDomain.poisoned.Store(true)
			case "regressed":
				d.runtimeDomain.last = time.Duration(math.MaxInt64 - 1)
			case "closed-done":
				close(done)
			case "exhausted":
				d.runtimeDomain.generation = math.MaxUint64
				d.runtimeDomain.poisoned.Store(true)
			}
			priorLast, priorGeneration := d.runtimeDomain.last, d.runtimeDomain.generation
			at, wall, err := publication.SampleRuntimePair()
			if name == "closed-done" {
				if err != nil || !at.valid() || wall.IsZero() {
					t.Fatal("Done vetoed cleanup pair", err)
				}
			} else if !errors.Is(err, errUnavailable) || at != (OwnedInstant{}) || !wall.IsZero() || d.runtimeDomain.last != priorLast {
				t.Fatal("failed pair minted values or advanced HWM", err)
			}
			if publication.clockPublicationLease.state.Load() != leaseLive || d.runtimeDomain.generation != priorGeneration {
				t.Fatal("sampling result consumed cleanup or published early")
			}
			publication.Complete()
			completed = true
			want := priorGeneration
			if want < math.MaxUint64 {
				want++
			}
			if d.runtimeDomain.generation != want {
				t.Fatal("mandatory pair cleanup skipped or wrapped generation")
			}
			if !d.runtimeDomain.clock.TryLock() {
				t.Fatal("pair cleanup retained clock")
			}
			d.runtimeDomain.clock.Unlock()
			if !d.runtimeDomain.auth.TryLock() {
				t.Fatal("pair cleanup retained W")
			}
			d.runtimeDomain.auth.Unlock()
		})
	}
}

func TestRuntimePairPreservesCommitDoneAndObservationRoles(t *testing.T) {
	done := make(chan struct{})
	d, err := NewRuntimeDomain(done)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	close(done)
	publication := d.Publisher().BeginClockedPublication()
	point, wall, err := publication.SampleRuntimePair()
	publication.Complete()
	if err != nil || !point.valid() || wall.IsZero() {
		t.Fatal("closed Done blocked historical pair", err)
	}
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	at, err := consumer.Sample()
	last := d.runtimeDomain.last
	consumer.Release()
	if !errors.Is(err, errLifecycle) || at != (OwnedInstant{}) || last < point.Elapsed() || d.runtimeDomain.poisoned.Load() {
		t.Fatal("shared raw pair changed post-sample commit Done behavior", err)
	}
	observation, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	later, err := observation.Sample()
	observation.Release()
	if err != nil || !later.NotBefore(point) {
		t.Fatal("clock-only observation gained Done veto", err)
	}
}

func TestRuntimePairGuardRejectsRawAndOwnershipEscapes(t *testing.T) {
	original := domainSourceFiles(t)
	for name, raw := range original {
		normalized, err := format.Source(raw)
		if err != nil {
			t.Fatal(err)
		}
		original[name] = normalized
	}
	if failures := domainSourceErrors(original); len(failures) != 0 {
		t.Fatal("runtime pair guard baseline", failures)
	}
	for _, tc := range []struct {
		name, path, before, after string
		statement                 bool
	}{
		{"second-read", "clock.go", "wall = sample.UTC()", "wall = time.Now().UTC()", false},
		{"fixture-pair", "clock.go", "if cell.kind != runtimeKind { return OwnedInstant{}, time.Time{}, errInvalid }", "", true},
		{"pair-rename", "clock.go", "func (c *ClockPublication) SampleRuntimePair()", "func (c *ClockPublication) SampleOtherPair()", false},
		{"observer-pair", "clock.go", "type OwnedInstant struct", "func (c *ObservationClock) SampleRuntimePair() (OwnedInstant, time.Time, error) { return OwnedInstant{}, time.Time{}, errInvalid }\ntype OwnedInstant struct", false},
		{"late-clocked-allocation", "domain.go", "guard := &ClockPublication{clockPublicationLease: newLease(b.owner, clockPublicationRole, false)}\nb.owner.clock.Lock()\nb.owner.auth.Lock()", "b.owner.clock.Lock()\nb.owner.auth.Lock()\nguard := &ClockPublication{clockPublicationLease: newLease(b.owner, clockPublicationRole, false)}", true},
		{"late-publication-allocation", "domain.go", "guard := &Publication{publicationLease: newLease(b.owner, publicationRole, false)}\nb.owner.auth.Lock()", "b.owner.auth.Lock()\nguard := &Publication{publicationLease: newLease(b.owner, publicationRole, false)}", true},
		{"raw-alias", "clock.go", "var q time.Duration", "_ = d.sampleLocked\nvar q time.Duration", true},
		{"pair-done-veto", "clock.go", "if cell.kind != runtimeKind { return OwnedInstant{}, time.Time{}, errInvalid }", "if cell.kind != runtimeKind { return OwnedInstant{}, time.Time{}, errInvalid }\nselect { case <-cell.owner.done: return OwnedInstant{}, time.Time{}, errLifecycle; default: }", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := map[string][]byte{}
			for name, raw := range original {
				altered[name] = bytes.Clone(raw)
			}
			before, after := tc.before, tc.after
			if tc.statement {
				before, after = domainStatementText(t, before), domainStatementText(t, after)
			}
			if strings.Count(string(altered[tc.path]), before) != 1 {
				t.Fatal("runtime pair source anchor not unique", tc.name)
			}
			altered[tc.path] = []byte(strings.Replace(string(altered[tc.path]), before, after, 1))
			failures := domainSourceErrors(altered)
			if len(failures) == 0 {
				t.Fatal("runtime pair source escape accepted", tc.name)
			}
			want := map[string]string{
				"second-read":                 "runtime pair must use one original read",
				"fixture-pair":                "fixture pair must refuse before raw read",
				"pair-rename":                 "missing or duplicate public API: ClockPublication.SampleRuntimePair",
				"observer-pair":               "closed public API shape: ObservationClock.SampleRuntimePair",
				"late-clocked-allocation":     "clocked guard must be allocated before clock and W",
				"late-publication-allocation": "publication guard must be allocated before W",
				"raw-alias":                   "raw sample method-value escape",
				"pair-done-veto":              "runtime pair role, lifecycle or wait escape",
			}[tc.name]
			found := false
			for _, failure := range failures {
				if failure == want {
					found = true
				}
			}
			if !found {
				t.Fatal("runtime pair self-control refused for unrelated reason", tc.name, failures)
			}
		})
	}
}
