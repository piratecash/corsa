package findnode

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"reflect"
	"testing"
	"time"
)

// This unit fixture is explicitly FixtureDomain. The independent Node case
// below uses real TLS/F1 and production Runtime points, not these test Instants.
func ownedTimedFixture(t *testing.T, n int) ownedLocalFixtureState {
	t.Helper()
	d, clock, _ := ownedOriginalDomain(t)
	intake, sink, err := NewExecutingFixtureIntake(d.Consumer(), d.Observer(), ownedBridgeProfile(), n)
	if err != nil {
		t.Fatal(err)
	}
	key, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	public, err := identity.ParsePublicKey(key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	local, err := intake.NewLocalTargetWithTiming(public, domain.NetworkID("gazeta-devnet"), SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}, OwnedTargetTimingProfile{ServiceBound: 375 * time.Millisecond, RuntimeReserve: 62500 * time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]Session, n)
	for i := range n {
		sources[i] = ownedOriginalSession(t, domain.ConnID(i+1), 0)
	}
	return ownedLocalFixtureState{intake, sink, local, d, clock, key, sources}
}
func TestOwnedTargetTimingExplicitProfileAndLegacyUnchanged(t *testing.T) {
	for _, p := range []OwnedTargetTimingProfile{{}, {ServiceBound: time.Second}, {ServiceBound: time.Second, RuntimeReserve: time.Second}, {ServiceBound: 6 * time.Second, RuntimeReserve: time.Millisecond}, {ServiceBound: -1, RuntimeReserve: time.Millisecond}} {
		if p.valid() {
			t.Fatal("invalid explicit owned timing profile", p)
		}
	}
	f := ownedLocalFixture(t, 1)
	if f.local.local.timed || f.local.local.timing != (OwnedTargetTimingProfile{}) {
		t.Fatal("legacy local target acquired implicit service policy")
	}
	r := f.reserve(t, f.sources[0], 11, 1)
	if r.Work.work.entry.coordinated.serviceWindow.enabled || r.Work.work.entry.coordinated.startBoundary() != Instant(6*time.Second) || r.Work.work.entry.coordinated.readyBoundary() != Instant(6*time.Second) {
		t.Fatal("legacy boundaries changed")
	}
	if err := r.Work.Cancel(); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedTargetTimingTrialRefusesWithoutNewDebt(t *testing.T) {
	f := ownedTimedFixture(t, 4)
	one := f.reserve(t, f.sources[0], 11, 1)
	two := f.reserve(t, f.sources[1], 12, 2)
	defer func() {
		if err := one.Work.Cancel(); err != nil {
			t.Errorf("cancel original accepted work one: %v", err)
		}
	}()
	defer func() {
		if err := two.Work.Cancel(); err != nil {
			t.Errorf("cancel original accepted work two: %v", err)
		}
	}()
	o := f.local.local.target
	before := o.Snapshot()
	beforeNext, beforeActual := o.calendar.nextProjected, o.calendar.nextActual
	sourceDebt := func() any {
		s := f.sources[2].state
		return struct {
			signer            *SigningCalendar
			projected, actual forwardBucket
		}{s.signer, s.signingProjected, s.signingActual}
	}
	sourceBefore := sourceDebt()
	input := f.received(t, f.sources[2], 13, 3)
	result, err := f.local.ReserveReceived(input, f.cut(t))
	var detail *OwnedTargetTimingRefusal
	if !errors.Is(err, ErrBudget) || !errors.As(err, &detail) || result.Work.work != nil || detail.ProjectedNS != int64(500*time.Millisecond) || detail.SignatureReadyNS != int64(375*time.Millisecond) || detail.LatestStartNS != int64(312500*time.Microsecond) || detail.AnswerDeadlineNS != int64(6*time.Second) {
		t.Fatal("actual calendar service refusal witness", result, detail, err)
	}
	if o.calendar.nextProjected != beforeNext || o.calendar.nextActual != beforeActual || len(o.cache.entries) != before.Cache.Entries || len(o.calendar.slots) != before.CalendarPending || o.counters != before.Local || !reflect.DeepEqual(sourceBefore, sourceDebt()) {
		t.Fatal("refused offer created slot/entry/source or actual debt")
	}
	if detail.Error() != ErrBudget.Error() {
		t.Fatal("diagnostic changed sanitized error")
	}
}
func TestOwnedTargetTimingStrictStartAndReadyKeepAnswerDeadline(t *testing.T) {
	t.Run("strict-start", func(t *testing.T) {
		f := ownedTimedFixture(t, 1)
		r := f.reserve(t, f.sources[0], 11, 1)
		f.clock.Store(int64(312500 * time.Microsecond))
		if _, _, err := r.Work.Take(); !errors.Is(err, ErrStaleWork) {
			t.Fatal("start equality admitted", err)
		}
		if f.local.local.target.counters.Consumed != 0 || f.local.local.target.calendar.nextActual != 0 {
			t.Fatal("late start spent actual debt")
		}
	})
	t.Run("strict-ready", func(t *testing.T) {
		f := ownedTimedFixture(t, 1)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, in, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		sig := targetOwnerSignature(t, f.key, in)
		f.clock.Store(int64(375 * time.Millisecond))
		if answers, err := permit.Complete(&sig); !errors.Is(err, ErrStaleWork) || len(answers) != 0 {
			t.Fatal("ready equality published", answers, err)
		}
		if f.local.local.target.counters.Consumed != 1 || f.local.local.target.counters.Completed != 0 || f.sources[0].state.signingActual.tokens != forwardBurst-forwardToken {
			t.Fatal("failed signature refunded actual debit")
		}
	})
	t.Run("early-ready-original-expiry", func(t *testing.T) {
		f := ownedTimedFixture(t, 2)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, in, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		sig := targetOwnerSignature(t, f.key, in)
		answers, err := permit.Complete(&sig)
		if err != nil || len(answers) != 1 {
			t.Fatal(err)
		}
		if answers[0].answer.expires != Instant(6*time.Second) {
			t.Fatal("service cutoff shortened original response expiry")
		}
		f.clock.Store(int64(500 * time.Millisecond))
		actualBefore, projectedBefore := f.sources[1].state.signingActual, f.sources[1].state.signingProjected
		copyResult, err := f.local.ReserveReceived(f.received(t, f.sources[1], 12, 1), f.cut(t))
		if err != nil || copyResult.Kind != LocalReadyCopy || copyResult.Work.work != nil || copyResult.Copy.answer == nil || copyResult.Copy.answer.source != f.sources[1] || copyResult.Copy.answer.body.ID != 12 || copyResult.Copy.answer.expires != Instant(6500*time.Millisecond) || f.local.local.target.counters.Consumed != 1 || f.sources[1].state.signer != nil || f.sources[1].state.signingActual != actualBefore || f.sources[1].state.signingProjected != projectedBefore {
			t.Fatal("cached original response acquired new signing budget or wrong expiry", err)
		}
	})
}
func TestOwnedTargetTimingOriginalDuplicateAndFailedSigner(t *testing.T) {
	f := ownedTimedFixture(t, 2)
	first := f.reserve(t, f.sources[0], 11, 1)
	original := first.Work.work.entry.coordinated
	f.clock.Store(int64(50 * time.Millisecond))
	dup, err := f.local.ReserveReceived(f.received(t, f.sources[1], 12, 1), f.cut(t))
	if err != nil || dup.Kind != LocalWaitingExisting || first.Work.work.entry.coordinated != original || original.readyBoundary() != Instant(375*time.Millisecond) || original.signBy != Instant(6*time.Second) {
		t.Fatal("duplicate retimed original signing promise", err)
	}
	permit, _, err := first.Work.Take()
	if err != nil {
		t.Fatal(err)
	}
	if answers, err := permit.Complete(nil); !errors.Is(err, ErrStaleWork) || len(answers) != 0 {
		t.Fatal("failed signer published reply", err)
	}
	if f.local.local.target.counters.Consumed != 1 || f.sources[0].state.signingActual.tokens != forwardBurst-forwardToken || f.local.local.target.calendar.Pending() != 0 {
		t.Fatal("failed signer refunded or orphaned actual work")
	}
}
