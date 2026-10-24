package findnode

import (
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func forwardObservationCleanup(t *testing.T, work OwnedForwardWork) {
	t.Helper()
	t.Cleanup(func() {
		if err := work.Cancel(); err != nil && !errors.Is(err, ErrStaleWork) {
			t.Error("original forward cleanup", err)
		}
	})
}

func forwardObservationFinish(t *testing.T, permit OwnedWritePermit) {
	t.Helper()
	if err := permit.Finish(false); err != nil && !errors.Is(err, ErrStaleWork) {
		t.Error("mandatory original writer Finish", err)
	}
}

func TestOwnedForwardOriginalEndpointObservation(t *testing.T) {
	t.Run("zero-is-unknown", func(t *testing.T) {
		if got := (OwnedForwardWork{}).endpointObservation(); got != (ownedForwardEndpointObservation{}) {
			t.Fatal("zero work fabricated an original endpoint", got)
		}
	})
	t.Run("initial-and-value-copy", func(t *testing.T) {
		f, work, _ := executingOne(t, false)
		forwardObservationCleanup(t, work)
		before := f.intake.dormantOwner.coordinator.snapshot()
		got := work.endpointObservation()
		if got.clock != ownedForwardObservationClockFixture || !got.receivedKnown || got.receivedNS != 0 || got.forwardKnown || got.pairKnown {
			t.Fatal("original ingress lost or reservation fabricated Take", got)
		}
		copy := got
		copy.receivedNS, copy.forwardNS, copy.pairKnown = -1, 99, true
		if work.endpointObservation() != got || !reflect.DeepEqual(before, f.intake.dormantOwner.coordinator.snapshot()) {
			t.Fatal("value diagnostic mutated owner or original endpoints", copy)
		}
	})
	t.Run("successful-post-owner-wait-and-replay", func(t *testing.T) {
		f, work, queued := executingOne(t, false)
		forwardObservationCleanup(t, work)
		before := work.endpointObservation()
		want := int64(work.work.receipt.scheduled) + int64(17*time.Microsecond)
		o := f.intake.dormantOwner.coordinator
		o.mu.Lock()
		type result struct {
			permit OwnedWritePermit
			err    error
		}
		done := make(chan result, 1)
		go func() { _, permit, err := queued.Take(); done <- result{permit, err} }()
		deadline := time.Now().Add(2 * time.Second)
		for work.work.phase.Load() != ownedForwardTaking {
			if time.Now().After(deadline) {
				o.mu.Unlock()
				select {
				case got := <-done:
					if got.err == nil {
						forwardObservationFinish(t, got.permit)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("original Take did not join after releasing owner")
				}
				t.Fatal("original Take never reached its owner wait")
			}
			runtime.Gosched()
		}
		// Advance only the existing genuine FixtureDomain while actual Take is
		// blocked at its original R lock. No production hook or slot is changed.
		f.clock.Store(want)
		o.mu.Unlock()
		var got result
		select {
		case got = <-done:
		case <-time.After(2 * time.Second):
			f.stop()
			select {
			case got = <-done:
				if got.err == nil {
					forwardObservationFinish(t, got.permit)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("original Take did not join after fixture shutdown")
			}
			t.Fatal("original Take did not join")
		}
		if got.err != nil {
			t.Fatal("original post-wait Take", got.err)
		}
		defer forwardObservationFinish(t, got.permit)
		observed := work.endpointObservation()
		if !observed.pairKnown || !observed.forwardKnown || observed.receivedNS != before.receivedNS || observed.forwardNS != want || observed.spanNS != want-before.receivedNS || observed.forwardNS <= int64(work.work.receipt.scheduled) {
			t.Fatal("a refreshed or f came from the projected slot/prewait", before, observed)
		}
		f.clock.Add(31)
		if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || work.endpointObservation() != observed {
			t.Fatal("Take replay fabricated or overwrote endpoint", err)
		}
		if err := got.permit.Finish(true); err != nil || work.endpointObservation() != observed {
			t.Fatal("Finish replaced a/f with completion time", err)
		}
	})
	t.Run("future-early-refusal-never-publishes-f", func(t *testing.T) {
		f := executingNew(t, 2, 1, false)
		batch := f.admit(t, 2)
		var future OwnedForwardWork
		for {
			work, ok := batch.Next()
			if !ok {
				break
			}
			forwardObservationCleanup(t, work)
			if work.work.receipt.scheduled > Instant(f.clock.Load()) {
				future = work
			}
		}
		if future.work == nil {
			t.Fatal("original calendars did not produce a future slot")
		}
		queued, err := future.QueueForOriginal(f.members[2])
		if err != nil {
			t.Fatal(err)
		}
		before := future.endpointObservation()
		if _, _, err := queued.Take(); !errors.Is(err, ErrNotReady) {
			t.Fatal("early original writer did not refuse", err)
		}
		if after := future.endpointObservation(); after != before || !after.receivedKnown || after.forwardKnown || after.pairKnown {
			t.Fatal("refused Take published a successful endpoint", before, after)
		}
		f.clock.Store(int64(future.work.receipt.scheduled))
		if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || future.endpointObservation() != before {
			t.Fatal("refused original became successful after retry", err)
		}
	})
	t.Run("wrong-owner-target-and-cancel", func(t *testing.T) {
		f, work, queued := executingOne(t, false)
		forwardObservationCleanup(t, work)
		before := work.endpointObservation()
		if _, err := work.QueueForOriginal(f.members[0]); !errors.Is(err, ErrUnauthenticated) || work.endpointObservation() != before {
			t.Fatal("wrong target changed original endpoint", err)
		}
		other := executingNew(t, 1, 4, false)
		forged := &ownedForwardState{owner: other.intake.dormantOwner, cut: work.work.cut, receipt: work.work.receipt, source: work.work.source, target: work.work.target, request: work.work.request, forwarded: work.work.forwarded}
		if got := (OwnedForwardWork{work: forged}).endpointObservation(); got != (ownedForwardEndpointObservation{}) {
			t.Fatal("foreign owner fabricated original observation", got)
		}
		if _, _, err := (OwnedQueuedForward{work: forged}).Take(); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("foreign owner acquired writer authority", err)
		}
		if err := queued.Cancel(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) || work.endpointObservation() != before {
			t.Fatal("cancelled original fabricated or erased endpoints", err)
		}
	})
	t.Run("end-before-take-retains-only-a", func(t *testing.T) {
		f, work, queued := executingOne(t, false)
		forwardObservationCleanup(t, work)
		before := work.endpointObservation()
		publication := f.domain.Publisher().BeginPublication()
		f.members[0].End()
		publication.Complete()
		if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
			t.Fatal(err)
		}
		if _, _, err := queued.Take(); err == nil {
			t.Fatal("ended source acquired original writer start")
		}
		if got := work.endpointObservation(); got != before || !got.receivedKnown || got.forwardKnown {
			t.Fatal("End fabricated f or discarded historical a", before, got)
		}
	})
	t.Run("end-after-start-finish-and-shutdown-retain-pair", func(t *testing.T) {
		f, work, queued := executingOne(t, false)
		forwardObservationCleanup(t, work)
		_, permit, err := queued.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer forwardObservationFinish(t, permit)
		before := work.endpointObservation()
		if !before.pairKnown {
			t.Fatal("successful original start missing endpoints", before)
		}
		publication := f.domain.Publisher().BeginPublication()
		f.members[0].End()
		publication.Complete()
		if err = f.intake.EndOriginalSession(f.members[0]); err != nil {
			t.Fatal(err)
		}
		f.clock.Add(17)
		f.stop()
		if err = permit.Finish(true); err != nil || work.endpointObservation() != before {
			t.Fatal("lawful Finish/End/shutdown changed historical a/f", err)
		}
	})
	t.Run("failed-finish-is-not-bytecompletion", func(t *testing.T) {
		f, work, queued := executingOne(t, false)
		forwardObservationCleanup(t, work)
		_, permit, err := queued.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer forwardObservationFinish(t, permit)
		before := work.endpointObservation()
		f.clock.Store(-1)
		if err = permit.Finish(false); err == nil || work.endpointObservation() != before || !before.pairKnown {
			t.Fatal("failed Finish rewrote start endpoints or invented completion", err)
		}
	})
	t.Run("runtime-original-pair", func(t *testing.T) {
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
		source, target := ownedOriginalSession(t, 1, 0), ownedOriginalSession(t, 2, 0)
		if err = d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
		point := func() (findnodebridge.CaptureStamp, findnodebridge.OwnedInstant) {
			stamp, sampleErr := d.Observer().BeginCapture()
			if sampleErr != nil {
				t.Fatal(sampleErr)
			}
			clock, sampleErr := d.Observer().ReserveClock()
			if sampleErr != nil {
				t.Fatal(sampleErr)
			}
			at, sampleErr := clock.Sample()
			clock.Release()
			if sampleErr != nil {
				t.Fatal(sampleErr)
			}
			return stamp, at
		}
		cut := func() OwnedCut {
			stamp, at := point()
			boundary, boundaryErr := at.BoundaryAfter(time.Second)
			if boundaryErr != nil {
				t.Fatal(boundaryErr)
			}
			value, sealErr := sink.Seal(stamp, at, boundary, []ObservedMember{{Session: source, Class: ClassOrdinary}, {Session: target, Class: ClassOrdinary}}, ObservedDegree{Incoming: 2, Window: 2})
			if sealErr != nil {
				t.Fatal(sealErr)
			}
			return value
		}
		stamp, at := point()
		raw, err := protocol.MarshalFindNodeRequest(request(17, target.Identity(), 9))
		if err != nil {
			t.Fatal(err)
		}
		input, err := sink.SealReceived(stamp, at, source, raw)
		if err != nil {
			t.Fatal(err)
		}
		if err = intake.OfferReceived(input, target, cut()); err != nil {
			t.Fatal(err)
		}
		// Existing fixture profile Round only; this does not assert a latency SLA
		// or inject/control the Runtime clock. CloseOwnedRound samples genuinely.
		timer := time.NewTimer(ownedBridgeProfile().Round)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-t.Context().Done():
			t.Fatal("Runtime original collection cancelled")
		}
		batch, err := intake.CloseOwnedRound(cut())
		if err != nil {
			t.Fatal(err)
		}
		work, ok := batch.Next()
		if !ok {
			t.Fatal("Runtime original round produced no accepted work")
		}
		forwardObservationCleanup(t, work)
		before := work.endpointObservation()
		if before.clock != ownedForwardObservationClockRuntime || !before.receivedKnown || before.receivedNS != int64(at.Elapsed()) || before.forwardKnown {
			t.Fatal("Runtime original a lost or f fabricated", before)
		}
		queued, err := work.QueueForOriginal(target)
		if err != nil {
			t.Fatal(err)
		}
		_, permit, err := queued.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer forwardObservationFinish(t, permit)
		got := work.endpointObservation()
		if got.clock != ownedForwardObservationClockRuntime || !got.pairKnown || got.receivedNS != before.receivedNS || got.forwardNS < got.receivedNS || got.spanNS != got.forwardNS-got.receivedNS {
			t.Fatal("genuine Runtime original pair absent", before, got)
		}
		if err = permit.Finish(false); err != nil || work.endpointObservation() != got {
			t.Fatal("Runtime Finish changed the original start pair", err)
		}
	})
}
