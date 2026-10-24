package findnode

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOwnedRelayEndpointPublicOriginalTuple(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "awaiting-answer", true: "during-dispatch"}[during], func(t *testing.T) {
			f, forward, forwardPermit, answer := ownedRelayAnswerFixture(t)
			forwardObservationCleanup(t, forward)
			defer forwardObservationFinish(t, forwardPermit)
			entry := forward.work.receipt.reservation.entry
			expected := OwnedRelayEndpointObservation{
				Clock: OwnedRelayEndpointClockFixture, RelayKnown: true,
				ReceivedKnown: true, ForwardKnown: true, AnswerReceivedKnown: true,
				ReceivedNS:   int64(entry.intake.ownedIdentity.received.Elapsed()),
				ForwardNS:    entry.intake.forwardEndpointNS,
				Generation:   forward.work.receipt.reservation.generation,
				SourceConnID: uint64(entry.source.ConnID()), TargetConnID: uint64(entry.target.ConnID()),
				SourceOpenedNS: int64(entry.source.state.opened), TargetOpenedNS: int64(entry.target.state.opened),
				SourceIdentity: entry.source.Identity(), TargetIdentity: entry.target.Identity(),
				RequestID: uint64(entry.request.ID), ForwardedID: uint64(entry.forwarded.ID),
				RequestTarget: entry.request.Target, ForwardedTarget: entry.forwarded.Target,
				RequestChallenge: entry.request.Challenge, ForwardedChallenge: entry.forwarded.Challenge,
				ExpiresNS: int64(entry.expires),
			}
			if !during {
				if err := forwardPermit.Finish(true); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.Add(17)
			input := ownedRelaySeal(t, f, f.members[1], answer)
			expected.AnswerReceivedNS = int64(input.answer.received.Elapsed())
			f.clock.Add(23)
			work, err := f.intake.MatchOwnedAnswer(input)
			if err != nil {
				t.Fatal(err)
			}
			relayObservationCleanup(t, work)
			if got := work.RelayEndpointObservation(); got != expected || !got.ReceivedKnown || got.ReceivedNS != 0 || got.AnswerReceivedNS == f.clock.Load() {
				t.Fatal("public reader lost original frozen tuple/known-zero or refreshed h", got, expected)
			}
			copy := work.RelayEndpointObservation()
			copy.RequestChallenge[0]++
			copy.SourceIdentity[0]++
			copy.ForwardedID++
			before := f.intake.ResourceSnapshot()
			clockBefore := f.clock.Load()
			if work.RelayEndpointObservation() != expected || !reflect.DeepEqual(f.intake.ResourceSnapshot(), before) || f.clock.Load() != clockBefore {
				t.Fatal("value read changed retained evidence/resources/clock", copy)
			}
			replacement := ownedOriginalSession(t, f.members[0].ConnID(), 0)
			if _, err := work.QueueAnswerForOriginal(replacement); !errors.Is(err, ErrUnauthenticated) || work.RelayEndpointObservation() != expected {
				t.Fatal("numeric-ID replacement rebound public history", err)
			}
			if _, err := f.intake.MatchOwnedAnswer(input); !errors.Is(err, ErrStaleWork) || work.RelayEndpointObservation() != expected {
				t.Fatal("receive replay changed public history", err)
			}
			queued, err := work.QueueAnswerForOriginal(f.members[0])
			if err != nil {
				t.Fatal(err)
			}
			f.clock.Add(31)
			expected.AnswerStartKnown, expected.ChainKnown = true, true
			expected.AnswerStartNS = f.clock.Load()
			expected.SpanNS = expected.AnswerStartNS - expected.ReceivedNS
			data, writer, err := queued.Take()
			if err != nil {
				t.Fatal(err)
			}
			defer relayObservationFinish(t, writer)
			body, err := protocol.ParseFindNodeAnswer(data)
			expectedBody := answer
			expectedBody.ID = entry.request.ID
			if err != nil || !reflect.DeepEqual(body, expectedBody) {
				t.Fatal("original writer/answer binding changed", body, err)
			}
			if work.RelayEndpointObservation() != expected || expected.WriteOutcome != OwnedRelayWriteUnreported {
				t.Fatal("public u is not original Take sample")
			}
			publication := f.domain.Publisher().BeginPublication()
			f.members[0].End()
			publication.Complete()
			if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
				t.Fatal(err)
			}
			if err := forwardPermit.Finish(true); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Fatal(err)
			}
			f.stop()
			if work.RelayEndpointObservation() != expected {
				t.Fatal("later End/shutdown erased public history")
			}
			if err := writer.Finish(false); err != nil {
				t.Fatal(err)
			}
			expected.WriteOutcome = OwnedRelayWriteReportedFailed
			if work.RelayEndpointObservation() != expected {
				t.Fatal("reported failure erased endpoints or became unreported")
			}
			if err := writer.Finish(true); !errors.Is(err, ErrStaleWork) || work.RelayEndpointObservation() != expected {
				t.Fatal("Finish replay changed original report", err)
			}
		})
	}
}

func TestOwnedRelayEndpointPublicRefusals(t *testing.T) {
	for _, kind := range []string{"cancel", "expiry", "End", "clock"} {
		t.Run(kind, func(t *testing.T) {
			f, forward, permit, answer := ownedRelayAnswerFixture(t)
			forwardObservationCleanup(t, forward)
			defer forwardObservationFinish(t, permit)
			if err := permit.Finish(true); err != nil {
				t.Fatal(err)
			}
			work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
			if err != nil {
				t.Fatal(err)
			}
			relayObservationCleanup(t, work)
			queued, err := work.QueueAnswerForOriginal(f.members[0])
			if err != nil {
				t.Fatal(err)
			}
			before := work.RelayEndpointObservation()
			if !before.RelayKnown || before.AnswerStartKnown || before.ChainKnown || before.WriteOutcome != OwnedRelayWriteUnreported {
				t.Fatal("pre-refusal observation fabricated u", before)
			}
			switch kind {
			case "cancel":
				if err := queued.Cancel(); err != nil {
					t.Fatal(err)
				}
			case "expiry":
				f.clock.Store(int64(work.answer.expires))
			case "End":
				publication := f.domain.Publisher().BeginPublication()
				f.members[0].End()
				publication.Complete()
				if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
					t.Fatal(err)
				}
			case "clock":
				f.clock.Store(-1)
			}
			_, _, err = queued.Take()
			if err == nil || kind == "cancel" && !errors.Is(err, ErrStaleWork) || kind == "expiry" && !errors.Is(err, ErrNotReady) || kind == "End" && !errors.Is(err, ErrUnauthenticated) {
				t.Fatal("original refusal changed", kind, err)
			}
			if work.RelayEndpointObservation() != before {
				t.Fatal("refusal minted u/report or erased original tuple")
			}
			if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || work.RelayEndpointObservation() != before {
				t.Fatal("refusal replay changed public history", err)
			}
		})
	}
}

func TestOwnedRelayEndpointPublicLocalAndUnknown(t *testing.T) {
	t.Run("zero-and-missing-owner", func(t *testing.T) {
		if (OwnedAnswerWork{}).RelayEndpointObservation() != (OwnedRelayEndpointObservation{}) || (OwnedAnswerWork{answer: &ownedAnswerState{}}).RelayEndpointObservation() != (OwnedRelayEndpointObservation{}) {
			t.Fatal("zero/ownerless observation became evidence")
		}
	})
	t.Run("diagnostic-mismatch-stays-unknown", func(t *testing.T) {
		f, forward, permit, answer := ownedRelayAnswerFixture(t)
		forwardObservationCleanup(t, forward)
		defer forwardObservationFinish(t, permit)
		if err := permit.Finish(true); err != nil {
			t.Fatal(err)
		}
		// Negative diagnostic qualification only; functional Match is unchanged.
		forward.work.receipt.reservation.entry.intake.ownedIdentity.request.ID++
		work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
		if err != nil {
			t.Fatal("diagnostic mismatch became an admission gate", err)
		}
		relayObservationCleanup(t, work)
		if got := work.RelayEndpointObservation(); got != (OwnedRelayEndpointObservation{Clock: OwnedRelayEndpointClockFixture}) {
			t.Fatal("inconsistent original tuple fabricated relayed values", got)
		}
	})
	t.Run("local-coalesced-cache-own-u-without-relay", func(t *testing.T) {
		f := ownedLocalFixture(t, 3)
		first := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, first.Work)
		second := f.reserve(t, f.sources[1], 22, 1)
		if second.Kind != LocalWaitingExisting || second.Work != (OwnedLocalSignWork{}) {
			t.Fatal("real coalesced work absent")
		}
		permit, input, err := first.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error(err)
			}
		}()
		signature := targetOwnerSignature(t, f.key, input)
		answers, err := permit.Complete(&signature)
		if err != nil || len(answers) != 2 {
			t.Fatal("original local/coalesced signature completion", len(answers), err)
		}
		f.clock.Store(int64(time.Second))
		// Third original source has never used this challenge; do not bypass the
		// duplicate-session guard by repeating the coalesced source.
		cached := f.reserve(t, f.sources[2], 33, 1)
		if cached.Kind != LocalReadyCopy || cached.Copy == (OwnedAnswerWork{}) {
			t.Fatal("original cached copy absent")
		}
		for _, work := range append(answers, cached.Copy) {
			relayObservationCleanup(t, work)
			expected := OwnedRelayEndpointObservation{Clock: OwnedRelayEndpointClockFixture}
			if work.RelayEndpointObservation() != expected {
				t.Fatal("local recipient fabricated R relay tuple")
			}
			q, err := work.QueueAnswerForOriginal(work.SourceSession())
			if err != nil {
				t.Fatal(err)
			}
			_, writer, err := q.Take()
			if err != nil {
				t.Fatal(err)
			}
			defer relayObservationFinish(t, writer)
			expected.AnswerStartKnown, expected.AnswerStartNS = true, f.clock.Load()
			if work.RelayEndpointObservation() != expected {
				t.Fatal("own local u became relayed a/f/h or known chain")
			}
			if err := writer.Finish(true); err != nil {
				t.Fatal(err)
			}
			expected.WriteOutcome = OwnedRelayWriteReportedOK
			if work.RelayEndpointObservation() != expected {
				t.Fatal("local reported outcome lost classification")
			}
		}
		if f.local.local.target.counters.Consumed != 1 || f.local.local.target.counters.Completed != 1 {
			t.Fatal("reader minted local signing debt")
		}
	})
}

func TestOwnedRelayEndpointPublicSparseReuse(t *testing.T) {
	f, forward, permit, answer := ownedRelayAnswerFixture(t)
	forwardObservationCleanup(t, forward)
	defer forwardObservationFinish(t, permit)
	o := f.intake.dormantOwner.coordinator
	old := forward.work.receipt.reservation.entry
	oldIndex := o.pending.sourceIndexLocked(pendingKey{f.members[0], 17})
	if oldIndex < 0 {
		t.Fatal("original pending row missing")
	}
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
	if err != nil {
		t.Fatal(err)
	}
	relayObservationCleanup(t, work)
	historical := work.RelayEndpointObservation()
	f.clock.Add(1)
	if err := f.intake.OfferReceived(receivedRequest(t, f.sink, f.domain, f.members[0], f.members[1], 18), f.members[1], f.cut(t)); err != nil {
		t.Fatal(err)
	}
	index := o.pending.sourceIndexLocked(pendingKey{f.members[0], 18})
	if index != oldIndex {
		t.Fatal("actual sparse slot not reused", oldIndex, index)
	}
	fresh := o.pending.entries[index]
	if fresh == old || fresh.intake == old.intake || fresh.intake.generation == historical.Generation || fresh.intake.forwardEndpointKnown || fresh.intake.forwardEndpointNS != 0 || work.RelayEndpointObservation() != historical {
		t.Fatal("fresh generation inherited or rebound old public evidence")
	}
	publication := f.domain.Publisher().BeginPublication()
	f.members[0].End()
	publication.Complete()
	if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
		t.Fatal(err)
	}
	if work.RelayEndpointObservation() != historical {
		t.Fatal("later End erased frozen generation")
	}
}

func TestOwnedRelayEndpointPublicConcurrentPublication(t *testing.T) {
	f, forward, permit, answer := ownedRelayAnswerFixture(t)
	forwardObservationCleanup(t, forward)
	defer forwardObservationFinish(t, permit)
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
	if err != nil {
		t.Fatal(err)
	}
	relayObservationCleanup(t, work)
	queued, err := work.QueueAnswerForOriginal(f.members[0])
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Add(1)
	u := f.clock.Load()
	done := make(chan error, 1)
	go func() {
		_, writer, err := queued.Take()
		if err == nil {
			err = writer.Finish(true)
		}
		done <- err
	}()
	invalid := false
	for range 512 {
		got := work.RelayEndpointObservation()
		if got.AnswerStartKnown && (!got.ChainKnown || got.AnswerStartNS != u) || !got.AnswerStartKnown && (got.ChainKnown || got.WriteOutcome != OwnedRelayWriteUnreported) {
			invalid = true
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		f.stop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("original writer did not join after shutdown")
		}
		t.Fatal("original writer did not join")
	}
	got := work.RelayEndpointObservation()
	if invalid || !got.ChainKnown || got.AnswerStartNS != u || got.WriteOutcome != OwnedRelayWriteReportedOK {
		t.Fatal("atomic publication exposed partial public u/report", got)
	}
}

// Closed Source controls exercise the new finite value surface at the existing
// source checker, not a functional mutation campaign or a runtime hook.
func TestOwnedRelayEndpointPublicClosedSourceControls(t *testing.T) {
	for _, escape := range []struct{ name, source string }{
		{"value-alias", "package findnode\ntype relayPublicEscape = OwnedRelayEndpointObservation\n"},
		{"clock-value", "package findnode\nvar relayPublicEscape = OwnedRelayEndpointClockRuntime\n"},
		{"writer-value", "package findnode\nvar relayPublicEscape = OwnedRelayWriteReportedFailed\n"},
	} {
		t.Run(escape.name, func(t *testing.T) {
			sources := preparedSessionSources(t)
			if failures := ownedLocalTargetSourceErrors(sources); len(failures) != 0 {
				t.Fatal("source-control baseline", failures)
			}
			sources["relay_public_escape.go"] = []byte(escape.source)
			if failures := ownedLocalTargetSourceErrors(sources); len(failures) == 0 {
				t.Fatal("new public scalar surface escaped exact Source", escape.name)
			}
		})
	}
}
