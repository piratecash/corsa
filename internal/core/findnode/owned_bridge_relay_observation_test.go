package findnode

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func relayObservationCleanup(t *testing.T, work OwnedAnswerWork) {
	t.Helper()
	t.Cleanup(func() {
		if err := work.CancelAnswer(); err != nil && !errors.Is(err, ErrStaleWork) {
			t.Error("original answer cleanup", err)
		}
	})
}
func relayObservationFinish(t *testing.T, permit OwnedAnswerPermit) {
	t.Helper()
	if err := permit.Finish(false); err != nil && !errors.Is(err, ErrStaleWork) {
		t.Error("mandatory original answer Finish", err)
	}
}

func TestOwnedRelayedOriginalEndpointObservation(t *testing.T) {
	t.Run("zero-is-unknown", func(t *testing.T) {
		if got := (OwnedAnswerWork{}).relayEndpointObservation(); got != (ownedRelayEndpointObservation{}) {
			t.Fatal("zero answer fabricated relay testimony", got)
		}
	})
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "awaiting-answer", true: "during-dispatch"}[during], func(t *testing.T) {
			f, forward, permit, answer := ownedRelayAnswerFixture(t)
			forwardObservationCleanup(t, forward)
			defer forwardObservationFinish(t, permit)
			entry := forward.work.receipt.reservation.entry
			originalState := entry.intake
			if !originalState.forwardEndpointKnown || originalState.forwardEndpointNS != forward.endpointObservation().forwardNS {
				t.Fatal("actual Forward.Take did not retain scalar f")
			}
			if !during {
				if err := permit.Finish(true); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.Add(17)
			input := ownedRelaySeal(t, f, f.members[1], answer)
			h := int64(input.answer.received.Elapsed())
			f.clock.Add(23) // Match's validation sample is later than original h.
			result, err := f.intake.MatchOwnedAnswer(input)
			if err != nil {
				t.Fatal(err)
			}
			relayObservationCleanup(t, result)
			got := result.relayEndpointObservation()
			if !got.relayKnown || got.clock != ownedForwardObservationClockFixture || !got.receivedKnown || !got.forwardKnown || !got.answerReceivedKnown || got.answerStartKnown || got.chainKnown || got.receivedNS != 0 || got.forwardNS != originalState.forwardEndpointNS || got.answerReceivedNS != h || got.answerReceivedNS == f.clock.Load() {
				t.Fatal("relay a/f/h lost or refreshed at Match", got)
			}
			if got.generation != forward.work.receipt.reservation.generation || got.sourceConnID != uint64(entry.source.state.connID) || got.targetConnID != uint64(entry.target.state.connID) || got.sourceOpenedNS != int64(entry.source.state.opened) || got.targetOpenedNS != int64(entry.target.state.opened) || got.sourceIdentity != entry.source.state.identity || got.targetIdentity != entry.target.state.identity || got.request != entry.request || got.forwarded != entry.forwarded || got.expiresNS != int64(entry.expires) {
				t.Fatal("frozen original identity tuple changed", got)
			}
			expected := answer
			expected.ID = entry.request.ID
			if !reflect.DeepEqual(result.answer.body, expected) {
				t.Fatal("diagnostic changed original signed-body delivery")
			}
			copy := got
			copy.receivedNS = -1
			copy.forwarded.ID++
			if result.relayEndpointObservation() != got {
				t.Fatal("returned value aliased the owner", copy)
			}
			replacement := ownedOriginalSession(t, f.members[0].state.connID, 0)
			if _, err := result.QueueAnswerForOriginal(replacement); !errors.Is(err, ErrUnauthenticated) || result.relayEndpointObservation() != got {
				t.Fatal("numeric-ID replacement changed identity testimony", err)
			}
			if _, err := f.intake.MatchOwnedAnswer(input); !errors.Is(err, ErrStaleWork) || result.relayEndpointObservation() != got {
				t.Fatal("receive replay rewrote history", err)
			}
			queued, err := result.QueueAnswerForOriginal(f.members[0])
			if err != nil {
				t.Fatal(err)
			}
			f.clock.Add(31)
			u := f.clock.Load()
			data, done, err := queued.Take()
			if err != nil {
				t.Fatal(err)
			}
			defer relayObservationFinish(t, done)
			parsed, err := protocol.ParseFindNodeAnswer(data)
			if err != nil || !reflect.DeepEqual(parsed, expected) {
				t.Fatal("actual writer bytes changed", parsed, err)
			}
			started := result.relayEndpointObservation()
			if !started.chainKnown || !started.answerStartKnown || started.answerStartNS != u || started.spanNS != u-started.receivedNS || started.writeOutcome != ownedAnswerWriteUnreported {
				t.Fatal("u not the successful original writer start", started)
			}
			if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || result.relayEndpointObservation() != started {
				t.Fatal("writer replay overwrote u", err)
			}
			publication := f.domain.Publisher().BeginPublication()
			f.members[0].End()
			publication.Complete()
			if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
				t.Fatal(err)
			}
			if err := permit.Finish(true); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Fatal(err)
			}
			f.stop()
			if err := done.Finish(false); err != nil {
				t.Fatal("End/done stranded original writer", err)
			}
			finished := result.relayEndpointObservation()
			started.writeOutcome = ownedAnswerWriteReportedFailed
			if finished != started {
				t.Fatal("writer outcome changed historical identity/endpoints", finished, started)
			}
			if err := done.Finish(true); !errors.Is(err, ErrStaleWork) || result.relayEndpointObservation() != finished {
				t.Fatal("Finish replay changed writer-reported outcome", err)
			}
			if originalState.forwardEndpointNS != got.forwardNS || !originalState.forwardEndpointKnown || f.intake.dormantOwner.coordinator.snapshot().pending != 0 {
				t.Fatal("retirement erased f or retained pending work")
			}
		})
	}
	t.Run("fresh-sparse-slot-does-not-inherit", func(t *testing.T) {
		f, forward, permit, answer := ownedRelayAnswerFixture(t)
		forwardObservationCleanup(t, forward)
		defer forwardObservationFinish(t, permit)
		o := f.intake.dormantOwner.coordinator
		old := forward.work.receipt.reservation.entry
		oldIndex := o.pending.sourceIndexLocked(pendingKey{f.members[0], 17})
		if oldIndex < 0 {
			t.Fatal("original row absent")
		}
		if err := permit.Finish(true); err != nil {
			t.Fatal(err)
		}
		result, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
		if err != nil {
			t.Fatal(err)
		}
		relayObservationCleanup(t, result)
		historical := result.relayEndpointObservation()
		f.clock.Add(1)
		if err := f.intake.OfferReceived(receivedRequest(t, f.sink, f.domain, f.members[0], f.members[1], 18), f.members[1], f.cut(t)); err != nil {
			t.Fatal(err)
		}
		index := o.pending.sourceIndexLocked(pendingKey{f.members[0], 18})
		if index != oldIndex {
			t.Fatal("did not exercise actual sparse slot reuse", oldIndex, index)
		}
		fresh := o.pending.entries[index]
		if fresh == old || fresh.intake == old.intake || fresh.intake.forwardEndpointKnown || fresh.intake.forwardEndpointNS != 0 || !old.intake.forwardEndpointKnown || result.relayEndpointObservation() != historical {
			t.Fatal("new sparse original inherited or rewrote historical scalar")
		}
		publication := f.domain.Publisher().BeginPublication()
		f.members[0].End()
		publication.Complete()
		if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("diagnostic-identity-mismatch-does-not-change-functional-match", func(t *testing.T) {
		f, forward, permit, answer := ownedRelayAnswerFixture(t)
		forwardObservationCleanup(t, forward)
		defer forwardObservationFinish(t, permit)
		if err := permit.Finish(true); err != nil {
			t.Fatal(err)
		}
		// Private causal fault in diagnostic identity, never a new admission source.
		forward.work.receipt.reservation.entry.intake.ownedIdentity.request.ID++
		work, err := f.intake.MatchOwnedAnswer(ownedRelaySeal(t, f, f.members[1], answer))
		if err != nil {
			t.Fatal("diagnostic qualification changed old functional success", err)
		}
		relayObservationCleanup(t, work)
		got := work.relayEndpointObservation()
		if got.relayKnown || got.receivedKnown || got.forwardKnown || got.answerReceivedKnown || got.chainKnown {
			t.Fatal("inconsistent original identity fabricated relay history", got)
		}
	})
}

func TestOwnedRelayedAnswerRefusalsDoNotPublishU(t *testing.T) {
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
			before := work.relayEndpointObservation()
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
			if err == nil {
				t.Fatal("refused original unexpectedly gained writer start")
			}
			if kind == "cancel" && !errors.Is(err, ErrStaleWork) || kind == "expiry" && !errors.Is(err, ErrNotReady) || kind == "End" && !errors.Is(err, ErrUnauthenticated) {
				t.Fatal("old refusal reason changed", kind, err)
			}
			if got := work.relayEndpointObservation(); got != before || got.answerStartKnown || got.chainKnown || got.writeOutcome != ownedAnswerWriteUnreported {
				t.Fatal("refused Take fabricated/erased endpoints", before, got)
			}
			if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || work.relayEndpointObservation() != before {
				t.Fatal("refused writer replay changed history", err)
			}
		})
	}
}

func TestOwnedRelayedObservationConcurrentPublication(t *testing.T) {
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
	expectedU := f.clock.Load()
	start := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-start
		_, writer, takeErr := queued.Take()
		if takeErr == nil {
			takeErr = writer.Finish(true)
		}
		done <- takeErr
	}()
	close(start)
	invalid := false
	for range 512 {
		got := work.relayEndpointObservation()
		if got.answerStartKnown && (got.answerStartNS != expectedU || !got.chainKnown) || !got.answerStartKnown && (got.chainKnown || got.writeOutcome != ownedAnswerWriteUnreported) {
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
			t.Fatal("original answer child did not join after shutdown")
		}
		t.Fatal("original answer child did not join")
	}
	if invalid {
		t.Fatal("atomic-known publication exposed partial endpoint or outcome")
	}
	got := work.relayEndpointObservation()
	if !got.chainKnown || got.answerStartNS != expectedU || got.writeOutcome != ownedAnswerWriteReportedOK {
		t.Fatal("original successful writer report missing", got)
	}
}

func TestOwnedLocalAndLegacyDoNotFabricateRelayHistory(t *testing.T) {
	t.Run("legacy-take-has-no-owned-f", func(t *testing.T) {
		o := candidateTestOwner(t)
		source, _ := provenFixture(t, 10, "source", 0)
		target, _ := provenFixture(t, 20, "target", 0)
		reservation, err := o.offer(source, target, request(7, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
		if err != nil {
			t.Fatal(err)
		}
		at := Instant(100 * time.Millisecond)
		if out, err := o.sealRound(candidateView(t, o, at, []Session{source}, []Session{source}, target), at); err != nil || out.reserved != 1 {
			t.Fatal("legacy original reservation absent", out, err)
		}
		permit, _, err := o.takeForward(reservation, at)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := o.finishForward(permit, at, candidateWriteFailed); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error(err)
			}
		}()
		if reservation.entry.intake.forwardEndpointKnown || reservation.entry.intake.forwardEndpointNS != 0 {
			t.Fatal("legacy caller time masqueraded as original owned f")
		}
	})
	t.Run("actual-local-coalesced-and-cache", func(t *testing.T) {
		f := ownedLocalFixture(t, 3)
		first := f.reserve(t, f.sources[0], 11, 1)
		second := f.reserve(t, f.sources[1], 22, 1)
		if second.Kind != LocalWaitingExisting || second.Work.work != nil {
			t.Fatal("actual coalesced original absent")
		}
		permit, in, err := first.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error("mandatory local Complete", err)
			}
		}()
		sig := targetOwnerSignature(t, f.key, in)
		answers, err := permit.Complete(&sig)
		if err != nil || len(answers) != 2 {
			t.Fatal("real signing/coalesced completion", len(answers), err)
		}
		f.clock.Store(int64(time.Second))
		cached := f.reserve(t, f.sources[2], 33, 1)
		if cached.Kind != LocalReadyCopy || cached.Copy.answer == nil {
			t.Fatal("actual cached original absent")
		}
		answers = append(answers, cached.Copy)
		for _, work := range answers {
			relayObservationCleanup(t, work)
			before := work.relayEndpointObservation()
			if before.relayKnown || before.receivedKnown || before.forwardKnown || before.answerReceivedKnown || before.chainKnown {
				t.Fatal("local source masqueraded as relayed ingress", before)
			}
			queued, err := work.QueueAnswerForOriginal(work.SourceSession())
			if err != nil {
				t.Fatal(err)
			}
			_, done, err := queued.Take()
			if err != nil {
				t.Fatal(err)
			}
			defer relayObservationFinish(t, done)
			got := work.relayEndpointObservation()
			if !got.answerStartKnown || got.clock != ownedForwardObservationClockFixture || got.relayKnown || got.chainKnown || got.receivedKnown || got.forwardKnown || got.answerReceivedKnown {
				t.Fatal("local u fabricated a relayed chain", got)
			}
			if err := done.Finish(true); err != nil {
				t.Fatal(err)
			}
		}
		if f.local.local.target.counters.Consumed != 1 {
			t.Fatal("observation minted another signing debit")
		}
	})
}

func TestOwnedRelayedRuntimeOriginalEndpoints(t *testing.T) {
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
	source := ownedOriginalSession(t, 1, 0)
	target, key := provenFixture(t, 2, "owned-originals", 0)
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	point := func() (findnodebridge.CaptureStamp, findnodebridge.OwnedInstant) {
		stamp, err := d.Observer().BeginCapture()
		if err != nil {
			t.Fatal(err)
		}
		lease, err := d.Observer().ReserveClock()
		if err != nil {
			t.Fatal(err)
		}
		at, err := lease.Sample()
		lease.Release()
		if err != nil {
			t.Fatal(err)
		}
		return stamp, at
	}
	cut := func() OwnedCut {
		stamp, at := point()
		boundary, err := at.BoundaryAfter(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		result, err := sink.Seal(stamp, at, boundary, []ObservedMember{{Session: source, Class: ClassOrdinary}, {Session: target, Class: ClassOrdinary}}, ObservedDegree{Incoming: 2, Window: 2})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	stamp, a := point()
	raw, err := protocol.MarshalFindNodeRequest(request(17, target.Identity(), 9))
	if err != nil {
		t.Fatal(err)
	}
	input, err := sink.SealReceived(stamp, a, source, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := intake.OfferReceived(input, target, cut()); err != nil {
		t.Fatal(err)
	}
	// Existing Round wait only; no injected Runtime clock or latency SLA.
	timer := time.NewTimer(ownedBridgeProfile().Round)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal("original Runtime round cancelled")
	}
	batch, err := intake.CloseOwnedRound(cut())
	if err != nil {
		t.Fatal(err)
	}
	forward, ok := batch.Next()
	if !ok {
		t.Fatal("original Runtime work absent")
	}
	forwardObservationCleanup(t, forward)
	queued, err := forward.QueueForOriginal(target)
	if err != nil {
		t.Fatal(err)
	}
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	defer forwardObservationFinish(t, permit)
	if err := permit.Finish(true); err != nil {
		t.Fatal(err)
	}
	stamp, h := point()
	answer := bodyFor(t, key).Answer(forward.work.forwarded.ID)
	raw, err = protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	received, err := sink.SealAnswer(stamp, h, target, raw)
	if err != nil {
		t.Fatal(err)
	}
	work, err := intake.MatchOwnedAnswer(received)
	if err != nil {
		t.Fatal(err)
	}
	relayObservationCleanup(t, work)
	answerQueue, err := work.QueueAnswerForOriginal(source)
	if err != nil {
		t.Fatal(err)
	}
	_, writer, err := answerQueue.Take()
	if err != nil {
		t.Fatal(err)
	}
	defer relayObservationFinish(t, writer)
	got := work.relayEndpointObservation()
	if got.clock != ownedForwardObservationClockRuntime || !got.chainKnown || got.receivedNS != int64(a.Elapsed()) || got.forwardNS != forward.endpointObservation().forwardNS || got.answerReceivedNS != int64(h.Elapsed()) || got.spanNS != got.answerStartNS-got.receivedNS || got.writeOutcome != ownedAnswerWriteUnreported {
		t.Fatal("genuine original Runtime a/f/h/u absent", got)
	}
	public := work.RelayEndpointObservation()
	if public.Clock != OwnedRelayEndpointClockRuntime || !public.RelayKnown || !public.ChainKnown || !public.ReceivedKnown || !public.ForwardKnown || !public.AnswerReceivedKnown || !public.AnswerStartKnown || public.ReceivedNS != int64(a.Elapsed()) || public.ForwardNS != forward.endpointObservation().forwardNS || public.AnswerReceivedNS != int64(h.Elapsed()) || public.AnswerStartNS != got.answerStartNS || public.SpanNS != public.AnswerStartNS-public.ReceivedNS || public.RequestID != uint64(forward.work.request.ID) || public.ForwardedID != uint64(forward.work.forwarded.ID) || public.RequestTarget != forward.work.request.Target || public.RequestChallenge != forward.work.request.Challenge || public.SourceConnID != uint64(source.ConnID()) || public.TargetConnID != uint64(target.ConnID()) || public.WriteOutcome != OwnedRelayWriteUnreported {
		t.Fatal("public reader lost genuine Runtime original endpoints/tuple", public)
	}
	if err := writer.Finish(true); err != nil {
		t.Fatal(err)
	}
	public.WriteOutcome = OwnedRelayWriteReportedOK
	if work.RelayEndpointObservation() != public {
		t.Fatal("Runtime report changed public original history")
	}
	got.writeOutcome = ownedAnswerWriteReportedOK
	if work.relayEndpointObservation() != got {
		t.Fatal("Runtime Finish changed original endpoints")
	}
}
