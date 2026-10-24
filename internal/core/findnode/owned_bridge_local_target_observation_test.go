package findnode

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func p0TargetObservationFixture(t *testing.T) ownedLocalFixtureState {
	t.Helper()
	f := p0ParameterFixture(t, 1)
	key, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	public, err := identity.ParsePublicKey(key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	local, err := f.intake.NewLocalTargetWithTiming(public, domain.NetworkID("gazeta-devnet"), SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}, OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return ownedLocalFixtureState{f.intake, f.sink, local, f.domain, f.clock, key, f.members}
}

func TestOwnedTargetTimingOriginalSpanObservation(t *testing.T) {
	t.Run("actual-take-over-reserve-and-replay", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		if got := r.Work.timingObservation(); got.pairKnown || got.takeKnown || got.clock != ownedLocalTimingClockFixture {
			t.Fatal("reservation fabricated original timing endpoints", got)
		}
		f.clock.Store(int64(100 * time.Millisecond))
		permit, input, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		if got := r.Work.timingObservation(); !got.takeKnown || got.takeNS != int64(100*time.Millisecond) || got.completeKnown || got.pairKnown {
			t.Fatal("observation used projected slot instead of actual Take", got)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(850 * time.Millisecond))
		answers, err := permit.Complete(&signature)
		if err != nil || len(answers) != 1 {
			t.Fatal("observed reserve excess changed legal original completion", err)
		}
		got := r.Work.timingObservation()
		if !got.pairKnown || got.clock != ownedLocalTimingClockFixture || got.takeNS != int64(100*time.Millisecond) || got.completeNS != int64(850*time.Millisecond) || got.spanNS != int64(750*time.Millisecond) || !got.observedReserveExceeded {
			t.Fatal("original Take/Complete span or observed excess missing", got)
		}
		before := f.local.local.target.Snapshot()
		copy := got
		copy.spanNS = -1
		if again := r.Work.timingObservation(); again != got || !reflect.DeepEqual(before, f.local.local.target.Snapshot()) {
			t.Fatal("value diagnostic mutated owner or retained endpoints", again, copy)
		}
		f.clock.Store(int64(time.Second))
		if _, err := permit.Complete(&signature); !errors.Is(err, ErrStaleWork) || r.Work.timingObservation() != got {
			t.Fatal("replay changed retained successful pair", err)
		}
	})
	t.Run("equal-reserve-is-not-excess", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, input, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(500 * time.Millisecond))
		if _, err = permit.Complete(&signature); err != nil {
			t.Fatal(err)
		}
		if got := r.Work.timingObservation(); !got.pairKnown || got.spanNS != int64(500*time.Millisecond) || got.observedReserveExceeded {
			t.Fatal("reserve equality became observed excess", got)
		}
	})
	t.Run("cancelled-has-no-pair", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		if err := r.Work.Cancel(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Work.Take(); !errors.Is(err, ErrStaleWork) {
			t.Fatal("cancelled work regained actual Take", err)
		}
		if got := r.Work.timingObservation(); got.pairKnown || got.takeKnown || got.completeKnown {
			t.Fatal("cancel fabricated completed pair", got)
		}
	})
	t.Run("failed-signature-has-no-pair", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, _, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = permit.Complete(nil); !errors.Is(err, ErrStaleWork) {
			t.Fatal("nil signature changed original refusal", err)
		}
		if got := r.Work.timingObservation(); !got.takeKnown || got.completeKnown || got.pairKnown {
			t.Fatal("failed completion minted timing pair", got)
		}
	})
	t.Run("clock-refusal-has-no-pair", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		f.clock.Store(int64(100 * time.Millisecond))
		permit, input, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(99 * time.Millisecond))
		if _, err = permit.Complete(&signature); err == nil {
			t.Fatal("regressed original clock unexpectedly completed")
		}
		if got := r.Work.timingObservation(); !got.takeKnown || got.completeKnown || got.pairKnown {
			t.Fatal("clock refusal minted timing pair", got)
		}
	})
	t.Run("end-after-take-preserves-lawful-completion", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, input, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		publication := f.domain.Publisher().BeginPublication()
		f.sources[0].End()
		publication.Complete()
		if err = f.local.EndOriginalSession(f.sources[0]); err != nil {
			t.Fatal(err)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(750 * time.Millisecond))
		if answers, err := permit.Complete(&signature); err != nil || len(answers) != 0 {
			t.Fatal("diagnostic added source-liveness completion gate", err)
		}
		if got := r.Work.timingObservation(); !got.pairKnown || got.spanNS != int64(750*time.Millisecond) {
			t.Fatal("lawful consumed completion lost observation after End", got)
		}
	})
	t.Run("untimed-remains-unknown", func(t *testing.T) {
		f := ownedLocalFixture(t, 1)
		r := f.reserve(t, f.sources[0], 11, 1)
		permit, input, err := r.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		signature := targetOwnerSignature(t, f.key, input)
		if _, err = permit.Complete(&signature); err != nil {
			t.Fatal(err)
		}
		if got := r.Work.timingObservation(); got != (ownedLocalTimingObservation{}) {
			t.Fatal("untimed original acquired timing evidence", got)
		}
	})
	t.Run("runtime-original-successful-span", func(t *testing.T) {
		done := make(chan struct{})
		t.Cleanup(func() { close(done) })
		d, err := findnodebridge.NewRuntimeDomain(done)
		if err != nil {
			t.Fatal(err)
		}
		intake, sink, err := NewExecutingIntake(d.Consumer(), d.Observer(), ownedBridgeProfile(), 8)
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
		local, err := intake.NewLocalTargetWithTiming(public, domain.NetworkID("gazeta-devnet"), SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}, OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		source := ownedOriginalSession(t, 1, 0)
		if err = d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
		stamp, err := d.Observer().BeginCapture()
		if err != nil {
			t.Fatal(err)
		}
		clock, err := d.Observer().ReserveClock()
		if err != nil {
			t.Fatal(err)
		}
		at, err := clock.Sample()
		clock.Release()
		if err != nil {
			t.Fatal(err)
		}
		boundary, err := at.BoundaryAfter(10 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		cut, err := sink.Seal(stamp, at, boundary, []ObservedMember{{Session: source, Class: ClassOrdinary}}, ObservedDegree{Incoming: 1, Window: 1})
		if err != nil {
			t.Fatal(err)
		}
		var challenge domain.FindNodeChallenge
		challenge[0] = 1
		raw, err := protocol.MarshalFindNodeRequest(protocol.FindNodeRequest{ID: 11, Target: local.local.target.cache.self, Challenge: challenge})
		if err != nil {
			t.Fatal(err)
		}
		input, err := sink.SealReceived(stamp, at, source, raw)
		if err != nil {
			t.Fatal(err)
		}
		result, err := local.ReserveReceived(input, cut)
		if err != nil || result.Kind != LocalReserved {
			t.Fatal("original Runtime reservation", err)
		}
		t.Cleanup(func() { _ = result.Work.Cancel() })
		if got := result.Work.timingObservation(); got.clock != ownedLocalTimingClockRuntime || got.pairKnown || got.takeKnown || got.completeKnown {
			t.Fatal("Runtime provenance became fabricated latency pair", got)
		}

		permit, signingInput, err := result.Work.Take()
		if err != nil {
			t.Fatal("original Runtime Take", err)
		}
		// Mandatory retirement also runs if signing or an assertion stops this test.
		// After a completed permit the same cleanup is an exact stale no-op.
		defer func() {
			if _, cleanupErr := permit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
				t.Error("original Runtime permit cleanup", cleanupErr)
			}
		}()
		taken := result.Work.timingObservation()
		if taken.clock != ownedLocalTimingClockRuntime || !taken.takeKnown || taken.completeKnown || taken.pairKnown {
			t.Fatal("original Runtime Take did not retain its own sample", taken)
		}
		signature := targetOwnerSignature(t, key, signingInput)
		answers, completeErr := permit.Complete(&signature)
		defer func() {
			for _, answer := range answers {
				if cleanupErr := answer.CancelAnswer(); cleanupErr != nil {
					t.Error("original Runtime answer cleanup", cleanupErr)
				}
			}
		}()
		if completeErr != nil || len(answers) != 1 || answers[0].SourceSession() != source {
			t.Fatal("original Runtime signature completion", completeErr, len(answers))
		}
		got := result.Work.timingObservation()
		if got.clock != ownedLocalTimingClockRuntime || !got.takeKnown || !got.completeKnown || !got.pairKnown || got.takeNS != taken.takeNS || got.takeNS < 0 || got.completeNS < got.takeNS || got.spanNS != got.completeNS-got.takeNS || got.observedReserveExceeded != (got.spanNS > int64(500*time.Millisecond)) {
			t.Fatal("original Runtime successful sample pair missing or inconsistent", taken, got)
		}
		// One owner-clock observation, not physical calibration or a latency SLA.
		t.Logf("original Runtime T span: take_ns=%d complete_ns=%d span_ns=%d reserve_ns=%d reserve_exceeded=%t", got.takeNS, got.completeNS, got.spanNS, int64(500*time.Millisecond), got.observedReserveExceeded)
	})
}
