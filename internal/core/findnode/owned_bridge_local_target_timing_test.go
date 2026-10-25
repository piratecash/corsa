package findnode

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
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

// These are original FixtureDomain coordinates, not measured physical time.
// The existing P0 factory supplies two genuine TLS-backed Session values and
// the selected2/2/1s intake,2s service/500ms reserve and250ms/max8 signer.
func TestOwnedTargetTimingReadyCacheOriginalAvailability(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		aReceive, complete, bReceive, dispatch time.Duration
		untimed, duplicate                     bool
		want                                   error
	}{
		{"P0-earlier-cached-too-late", 3 * time.Second, 4 * time.Second, 0, 4100 * time.Millisecond, false, false, ErrBudget},
		{"P0-ready-boundary-minus-one", time.Second, 2*time.Second - time.Nanosecond, 0, 2100 * time.Millisecond, false, false, nil},
		{"P0-ready-boundary-equality", time.Second, 2 * time.Second, 0, 2100 * time.Millisecond, false, false, ErrBudget},
		{"P0-ready-boundary-plus-one", time.Second, 2*time.Second + time.Nanosecond, 0, 2100 * time.Millisecond, false, false, ErrBudget},
		{"P0-fast-earlier-cached-allowed", 100 * time.Millisecond, 200 * time.Millisecond, 0, 500 * time.Millisecond, false, false, nil},
		{"P0-already-ready-later-ingress", 0, 100 * time.Millisecond, 500 * time.Millisecond, 700 * time.Millisecond, false, false, nil},
		{"P0-known-zero-completion", 0, 0, 0, 100 * time.Millisecond, false, false, nil},
		{"untimed-earlier-cache-unchanged", 3 * time.Second, 4 * time.Second, 0, 4100 * time.Millisecond, true, false, nil},
		{"duplicate-precedes-timing-refusal", 3 * time.Second, 4 * time.Second, 0, 4100 * time.Millisecond, false, true, ErrDuplicateLink},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f ownedLocalFixtureState
			if tc.untimed {
				f = ownedLocalFixture(t, 2)
			} else {
				f = p0TargetObservationFixture(t)
			}
			if len(f.sources) != 2 || f.local.local.timed == tc.untimed {
				t.Fatal("original two-source timing factory changed")
			}
			sourceB := f.sources[1]
			if tc.duplicate {
				sourceB = f.sources[0]
			}
			var earlier OwnedReceivedRequest
			if tc.bReceive <= tc.aReceive {
				f.clock.Store(int64(tc.bReceive))
				earlier = f.received(t, sourceB, 22, 1)
			}
			f.clock.Store(int64(tc.aReceive))
			first := f.reserve(t, f.sources[0], 11, 1)
			targetEndpointReadoutCleanup(t, first.Work)
			permit, input, err := first.Work.Take()
			if err != nil {
				t.Fatal("original fresh Take", err)
			}
			t.Cleanup(func() {
				if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
					t.Error("mandatory original completion cleanup", err)
				}
			})
			signature := targetOwnerSignature(t, f.key, input)
			f.clock.Store(int64(tc.complete))
			answers, err := permit.Complete(&signature)
			if err != nil || len(answers) != 1 || answers[0].answer == nil || answers[0].answer.expires != Instant(tc.aReceive)+responderTimeout {
				t.Fatal("genuine first completion/original answer expiry", len(answers), err)
			}
			t.Cleanup(func() {
				if err := answers[0].CancelAnswer(); err != nil {
					t.Error("original A answer cleanup", err)
				}
			})
			observed := first.Work.TargetEndpointObservation()
			if !tc.untimed && (observed.Clock != OwnedTargetEndpointClockFixture || !observed.PairKnown || observed.ReceivedNS != int64(tc.aReceive) || observed.TakeNS != int64(tc.aReceive) || observed.CompleteNS != int64(tc.complete)) {
				t.Fatal("first work lost original fixture endpoints", observed)
			}
			entry := first.Work.work.entry
			if entry.phase != targetReady || entry.received != Instant(tc.aReceive) || entry.expires != Instant(tc.complete)+cacheLifetime || entry.coordinated != nil {
				t.Fatal("ready row lost authentic original completion/retention")
			}
			if tc.bReceive > tc.aReceive {
				f.clock.Store(int64(tc.bReceive))
				earlier = f.received(t, sourceB, 22, 1)
			}
			f.clock.Store(int64(tc.dispatch))
			o := f.local.local.target
			before := *entry
			before.recipients = make(map[Session]domain.FindNodeRequestID, len(entry.recipients))
			for source, requestID := range entry.recipients {
				before.recipients[source] = requestID
			}
			beforeSessions := make(map[Session]int, len(o.cache.sessions))
			for source, count := range o.cache.sessions {
				beforeSessions[source] = count
			}
			localCounters, cacheCounters := o.counters, o.cache.counters
			nextProjected, nextActual, slots := o.calendar.nextProjected, o.calendar.nextActual, len(o.calendar.slots)
			debt := func() [2]struct {
				signer            *SigningCalendar
				projected, actual forwardBucket
			} {
				var value [2]struct {
					signer            *SigningCalendar
					projected, actual forwardBucket
				}
				for i, source := range f.sources {
					value[i].signer, value[i].projected, value[i].actual = source.state.signer, source.state.signingProjected, source.state.signingActual
				}
				return value
			}
			beforeDebt := debt()
			result, err := f.local.ReserveReceived(earlier, f.cut(t))
			if tc.want != nil {
				if !errors.Is(err, tc.want) || result != (OwnedLocalResult{}) {
					t.Fatal("expected original refusal with no result authority", tc.want, result.Kind, err)
				}
				if !reflect.DeepEqual(*entry, before) || !reflect.DeepEqual(o.cache.sessions, beforeSessions) || o.cache.counters != cacheCounters || o.counters != localCounters {
					t.Fatal("refused detached copy changed live row/recipient/history/counters")
				}
			} else {
				if err != nil || result.Kind != LocalReadyCopy || result.Work.work != nil || result.Copy.answer == nil {
					t.Fatal("valid cached recipient refused or acquired fresh signing work", result.Kind, err)
				}
				t.Cleanup(func() {
					if err := result.Copy.CancelAnswer(); err != nil {
						t.Error("cached B answer cleanup", err)
					}
				})
				answer := result.Copy.answer
				request := earlier.receivedRequest.identity.request
				if answer.source != sourceB || answer.body.ID != 22 || answer.body.Target != input.Target() || answer.body.Signature != signature || answer.expires != Instant(tc.bReceive)+responderTimeout || protocol.VerifyFindNodeAnswer(answer.body, request, o.network) != nil {
					t.Fatal("cached copy changed original source/ID/signed body/reply expiry")
				}
				if o.counters.Reserved != localCounters.Reserved || o.counters.Consumed != localCounters.Consumed || o.counters.Completed != localCounters.Completed || o.counters.ReadyCopy != localCounters.ReadyCopy+1 || entry.historyCount != before.historyCount+1 || entry.recipients[sourceB] != 22 || o.cache.sessions[sourceB] != 1 {
					t.Fatal("cached recipient created fresh work or lost genuine history")
				}
			}
			if o.cache.entries[entry.challenge] != entry || entry.received != before.received || entry.expires != before.expires || !reflect.DeepEqual(entry.body, before.body) || o.calendar.nextProjected != nextProjected || o.calendar.nextActual != nextActual || len(o.calendar.slots) != slots || debt() != beforeDebt || first.Work.TargetEndpointObservation() != observed {
				t.Fatal("cached result refreshed completion/cache expiry or first work/debt")
			}
			// Refusal still consumes this input and advances original cleanup/sample
			// metadata. No claim of an entirely side-effect-free Reserve is made.
		})
	}
}

func TestOwnedTargetTimingPendingRecipientInheritance(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		name := "later-pending-inherits-original-signature"
		if earlier {
			name = "earlier-pending-still-refuses"
		}
		t.Run(name, func(t *testing.T) {
			f := p0TargetObservationFixture(t)
			var received OwnedReceivedRequest
			if earlier {
				received = f.received(t, f.sources[1], 22, 1)
				f.clock.Store(int64(3 * time.Second))
			}
			first := f.reserve(t, f.sources[0], 11, 1)
			targetEndpointReadoutCleanup(t, first.Work)
			entry := first.Work.work.entry
			original, retained := entry.coordinated, *entry.coordinated
			actual, projected := f.sources[1].state.signingActual, f.sources[1].state.signingProjected
			if earlier {
				f.clock.Store(int64(4100 * time.Millisecond))
			} else {
				f.clock.Store(int64(500 * time.Millisecond))
				received = f.received(t, f.sources[1], 22, 1)
			}
			result, err := f.local.ReserveReceived(received, f.cut(t))
			if earlier {
				if !errors.Is(err, ErrBudget) || result != (OwnedLocalResult{}) || len(entry.recipients) != 1 || entry.historyCount != 1 || original.signBy != Instant(9*time.Second) || original.readyBoundary() != Instant(5*time.Second) {
					t.Fatal("earlier pending refusal changed original promise", result.Kind, err)
				}
			} else if err != nil || result.Kind != LocalWaitingExisting || result.Work.work != nil || result.Copy.answer != nil || len(entry.recipients) != 2 || original.signBy != responderTimeout || original.readyBoundary() != Instant(2*time.Second) {
				t.Fatal("later pending recipient retimed or duplicated first work", result.Kind, err)
			}
			if entry.coordinated != original || *entry.coordinated != retained || f.sources[1].state.signer != nil || f.sources[1].state.signingActual != actual || f.sources[1].state.signingProjected != projected || f.local.local.target.calendar.Pending() != 1 {
				t.Fatal("pending recipient allocated or changed historical signing debt")
			}
			if earlier {
				return // existing cleanup cancels only the original retained A
			}
			permit, input, err := first.Work.Take()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
					t.Error("mandatory original pending completion cleanup", err)
				}
			})
			signature := targetOwnerSignature(t, f.key, input)
			f.clock.Store(int64(1900 * time.Millisecond))
			answers, err := permit.Complete(&signature)
			if err != nil || len(answers) != 2 {
				t.Fatal("one actual signature did not complete both original recipients", len(answers), err)
			}
			for i, answer := range answers {
				t.Cleanup(func() {
					if err := answer.CancelAnswer(); err != nil {
						t.Error("pending original answer cleanup", err)
					}
				})
				if answer.answer == nil || answer.answer.source != f.sources[i] || answer.answer.body.ID != domain.FindNodeRequestID(11+11*i) || answer.answer.body.Signature != signature || answer.answer.expires != responderTimeout {
					t.Fatal("coalesced original body/first response expiry changed")
				}
			}
			o := f.local.local.target
			if o.counters.Reserved != 1 || o.counters.Waiting != 1 || o.counters.Consumed != 1 || o.counters.Completed != 1 || o.counters.ReadyCopy != 0 || o.calendar.Pending() != 0 || f.sources[1].state.signer != nil || f.sources[1].state.signingActual != actual || f.sources[1].state.signingProjected != projected || !first.Work.TargetEndpointObservation().PairKnown {
				t.Fatal("pending inheritance changed single-signature accounting")
			}
		})
	}
}
