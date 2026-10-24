package findnode

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Explicit private logical profile, never a production/default selection.
// The existing owner harness uses real TLS/session proofs and FixtureDomain;
// elapsed, ConnID, classes and complete membership are declared fixture inputs.
func p0ParameterFixture(t *testing.T, count int) executingFixture {
	t.Helper()
	d, clock, stop := ownedOriginalDomain(t)
	profile := DormantIntakeProfile{
		Timing: TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second},
		Round:  100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4,
	}
	intake, sink, err := NewExecutingFixtureIntake(d.Consumer(), d.Observer(), profile, 8)
	if err != nil {
		t.Fatal("explicit P0 fixture construction", err)
	}
	f := executingFixture{intake: intake, sink: sink, domain: d, clock: clock, stop: stop}
	for id := 1; id <= count+1; id++ {
		f.members = append(f.members, ownedOriginalSession(t, domain.ConnID(id), 0))
	}
	return f
}

func TestInactiveP0ParameterBinding(t *testing.T) {
	t.Run("R-common-receive-five-originals", func(t *testing.T) {
		f := p0ParameterFixture(t, 5)
		target := f.members[5]
		// Seal the complete original fixture sessions through the original sink.
		// ClassSelected is logical input here, not fabricated Node/CM evidence.
		cut := func() OwnedCut {
			stamp, at := ownedBridgePoint(t, f.domain)
			members := make([]ObservedMember, len(f.members))
			for i, source := range f.members {
				members[i] = ObservedMember{Session: source, Class: ClassSelected}
			}
			value, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, time.Second), members, ObservedDegree{Incoming: len(members), Window: len(members)})
			if err != nil {
				t.Fatal("original P0 complete fixture cut", err)
			}
			return value
		}
		for i, source := range f.members[:5] {
			if err := f.intake.OfferReceived(receivedRequest(t, f.sink, f.domain, source, target, domain.FindNodeRequestID(17+i)), target, cut()); err != nil {
				t.Fatal("original P0 received offer", i, err)
			}
		}
		o := f.intake.dormantOwner.coordinator
		if s := o.snapshot(); s.provisional != 5 || s.committed != 0 || target.state.nextID != 0 {
			t.Fatal("P0 offers charged before actual joint selection", s)
		}
		f.clock.Store(int64(100 * time.Millisecond))
		batch, err := f.intake.CloseOwnedRound(cut())
		if err != nil {
			t.Fatal("original P0 round close", err)
		}
		accepted := make(map[Session]bool)
		var calendar *ForwardCalendar
		for {
			work, ok := batch.Next()
			if !ok {
				break
			}
			t.Cleanup(func() { _ = work.Cancel() })
			if !ownedForwardOriginal(work.work) || work.RouteTarget() != target || accepted[work.work.source] {
				t.Fatal("P0 batch lost distinct original source/target custody")
			}
			accepted[work.work.source] = true
			r := work.work.receipt
			if r.latest != Instant(time.Second) || r.scheduled < Instant(100*time.Millisecond) || r.scheduled > r.latest {
				t.Fatal("P0 original receive/deadline or actual slot changed", r.scheduled, r.latest)
			}
			current := r.reservation.entry.intake.calendar
			if calendar != nil && current != calendar {
				t.Fatal("P0 target links acquired different calendars")
			}
			calendar = current
		}
		if s := o.snapshot(); len(accepted) != 4 || s.committed != 4 || s.refused != 1 || s.pending != 4 || s.reserved != 4 || s.provisional != 0 || s.aliases != 4 || s.names != 1 || s.records != 1 || target.state.nextID != 4 {
			t.Fatal("P0 common receive burst or refusal accounting", len(accepted), s, target.state.nextID)
		}
		if calendar == nil || calendar.actual.tokens != forwardBurst || calendar.actual.at != Instant(100*time.Millisecond) {
			t.Fatal("P0 reservation spent actual writer tokens")
		}
		refused := 0
		for _, source := range f.members[:5] {
			windows := source.state.sourceWindows.Load()
			if accepted[source] {
				if windows == nil {
					t.Fatal("P0 accepted source lost original pair history")
				}
				continue
			}
			refused++
			o.mu.Lock()
			o.pending.mu.Lock()
			retained := o.pending.sourceCountLocked(source)
			o.pending.mu.Unlock()
			o.mu.Unlock()
			if windows != nil || retained != 0 || o.snapshot().cursor == source.ConnID() {
				t.Fatal("P0 fifth refusal bound history, pending row or successful cursor")
			}
		}
		if refused != 1 {
			t.Fatal("P0 did not account for exactly one original refusal", refused)
		}
	})

	t.Run("T-six-projected-before-strict-start", func(t *testing.T) {
		f := p0ParameterFixture(t, 6) // Seven distinct original source sessions.
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
			t.Fatal("explicit P0 timed target", err)
		}
		lf := ownedLocalFixtureState{intake: f.intake, sink: f.sink, local: local, domain: f.domain, clock: f.clock, key: key, sources: f.members}
		var works [6]OwnedLocalSignWork
		for i := range works {
			result := lf.reserve(t, lf.sources[i], domain.FindNodeRequestID(100+i), byte(i+1))
			work := result.Work
			t.Cleanup(func() { _ = work.Cancel() })
			if result.Kind != LocalReserved || !ownedLocalOriginal(work.work) {
				t.Fatal("P0 original timed reservation refused before sixth", i)
			}
			works[i] = work
			entry := work.work.entry.coordinated
			if entry.alias.At() != Instant(time.Duration(i)*250*time.Millisecond) || entry.startBoundary() != Instant(1500*time.Millisecond) || entry.readyBoundary() != Instant(2*time.Second) || entry.signBy != Instant(6*time.Second) {
				t.Fatal("P0 target projection or original service/deadline changed", i, entry.alias.At(), entry.startBoundary(), entry.readyBoundary(), entry.signBy)
			}
		}
		o := local.local.target
		before := o.Snapshot()
		nextProjected, nextActual := o.calendar.nextProjected, o.calendar.nextActual
		source := lf.sources[6].state
		signer, projected, actual := source.signer, source.signingProjected, source.signingActual
		result, err := local.ReserveReceived(lf.received(t, lf.sources[6], 106, 7), lf.cut(t))
		var refusal *OwnedTargetTimingRefusal
		if !errors.Is(err, ErrBudget) || !errors.As(err, &refusal) || result.Work.work != nil || refusal.ReceivedNS != 0 || refusal.ProjectedNS != int64(1500*time.Millisecond) || refusal.LatestStartNS != int64(1500*time.Millisecond) || refusal.SignatureReadyNS != int64(2*time.Second) || refusal.AnswerDeadlineNS != int64(6*time.Second) {
			t.Fatal("P0 seventh original did not refuse exact strict start boundary", result.Kind, refusal, err)
		}
		// A refused Reserve still publishes exactly one diagnostic snapshot.
		// Publication sequence is not admission/debt; preserve every other field.
		expected := before
		expected.Sequence++
		if !reflect.DeepEqual(expected, o.Snapshot()) || o.calendar.nextProjected != nextProjected || o.calendar.nextActual != nextActual || source.signer != signer || source.signingProjected != projected || source.signingActual != actual || o.calendar.Pending() != 6 {
			t.Fatal("P0 seventh refusal changed original cache/calendar/source debt")
		}
		// This is not six successful signatures or a physical500ms claim.
		// The original sixth job also refuses actual Take at start equality.
		f.clock.Store(int64(1500 * time.Millisecond))
		permit, _, err := works[5].Take()
		if permit.work != nil {
			t.Cleanup(func() { _, _ = permit.Complete(nil) })
		}
		if !errors.Is(err, ErrStaleWork) || permit.work != nil || o.calendar.nextActual != nextActual || o.counters.Consumed != 0 {
			t.Fatal("P0 actual T start equality spent authority or actual debt", err)
		}
	})
}
