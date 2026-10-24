package findnode

import (
	"errors"
	"testing"
	"time"
)

func targetEndpointReadoutCleanup(t *testing.T, work OwnedLocalSignWork) {
	t.Helper()
	t.Cleanup(func() {
		if err := work.Cancel(); err != nil && !errors.Is(err, ErrStaleWork) {
			t.Error("original target work cleanup", err)
		}
	})
}

func TestOwnedTargetOriginalReceiveCompleteReadout(t *testing.T) {
	t.Run("zero-and-known-zero", func(t *testing.T) {
		if got := (OwnedLocalSignWork{}).TargetEndpointObservation(); got != (OwnedTargetEndpointObservation{}) {
			t.Fatal("zero work fabricated target endpoints", got)
		}
		f := p0TargetObservationFixture(t)
		result := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, result.Work)
		got := result.Work.TargetEndpointObservation()
		if got.Clock != OwnedTargetEndpointClockFixture || !got.ReceivedKnown || got.ReceivedNS != 0 || got.TakeKnown || got.TakeNS != 0 || got.CompleteKnown || got.PairKnown {
			t.Fatal("original known-zero r lost or c fabricated before Take", got)
		}
	})
	t.Run("original-receive-before-later-reserve-take-and-complete", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		f.clock.Store(int64(37 * time.Millisecond))
		received := f.received(t, f.sources[0], 11, 9)
		original := received.receivedRequest.identity
		f.clock.Store(int64(53 * time.Millisecond))
		result, err := f.local.ReserveReceived(received, f.cut(t))
		if err != nil || result.Kind != LocalReserved {
			t.Fatal("original timed local reservation", err)
		}
		targetEndpointReadoutCleanup(t, result.Work)
		before := result.Work.TargetEndpointObservation()
		if !before.ReceivedKnown || before.ReceivedNS != int64(original.received.Elapsed()) || before.ReceivedNS == f.clock.Load() || before.RequestID != uint64(original.request.ID) || before.TargetIdentity != original.request.Target || before.Challenge != original.request.Challenge || before.SourceConnID != uint64(original.source.state.connID) || before.SourceOpenedNS != int64(original.source.state.opened) || before.SourceIdentity != original.source.state.identity || before.TakeKnown || before.TakeNS != 0 || before.CompleteKnown || before.PairKnown {
			t.Fatal("target r/tuple refreshed or rebound at admission", before)
		}
		f.clock.Store(int64(100 * time.Millisecond))
		permit, input, err := result.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error("mandatory original Complete", err)
			}
		}()
		taken := result.Work.TargetEndpointObservation()
		if !taken.TakeKnown || taken.TakeNS != int64(100*time.Millisecond) || taken.ReceivedNS != before.ReceivedNS || taken.CompleteKnown || taken.PairKnown {
			t.Fatal("successful original Take lost its own Sample", before, taken)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(850 * time.Millisecond))
		answers, err := permit.Complete(&signature)
		if err != nil || len(answers) != 1 {
			t.Fatal("original actual signature completion", len(answers), err)
		}
		got := result.Work.TargetEndpointObservation()
		old := result.Work.timingObservation()
		if got.Clock != OwnedTargetEndpointClockFixture || !got.PairKnown || !got.CompleteKnown || !got.TakeKnown || got.TakeNS != taken.TakeNS || got.TakeNS < got.ReceivedNS || got.CompleteNS < got.TakeNS || got.ReceivedNS != before.ReceivedNS || got.CompleteNS != old.completeNS || got.SpanNS != got.CompleteNS-got.ReceivedNS || got.SpanNS == got.CompleteNS-got.TakeNS || got.CompleteNS-got.TakeNS != old.spanNS || old.takeNS != int64(100*time.Millisecond) || old.spanNS != int64(750*time.Millisecond) || !old.observedReserveExceeded {
			t.Fatal("r replaced by Take or c moved from original Sample", got, old)
		}
		copy := got
		copy.ReceivedNS = -1
		copy.TakeNS = -1
		copy.Challenge[0]++
		if result.Work.TargetEndpointObservation() != got {
			t.Fatal("value-copy readout aliased original state", copy)
		}
		f.clock.Store(int64(time.Second))
		if _, err := permit.Complete(&signature); !errors.Is(err, ErrStaleWork) || result.Work.TargetEndpointObservation() != got {
			t.Fatal("Complete replay refreshed original r/c", err)
		}
	})
	t.Run("original-end-keeps-lawful-completion-and-history", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		result := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, result.Work)
		permit, input, err := result.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error(err)
			}
		}()
		before := result.Work.TargetEndpointObservation()
		if !before.TakeKnown || before.TakeNS != 0 || before.CompleteKnown || before.PairKnown {
			t.Fatal("genuine known-zero Take was treated as missing", before)
		}
		publication := f.domain.Publisher().BeginPublication()
		f.sources[0].End()
		publication.Complete()
		if err := f.local.EndOriginalSession(f.sources[0]); err != nil {
			t.Fatal(err)
		}
		signature := targetOwnerSignature(t, f.key, input)
		f.clock.Store(int64(750 * time.Millisecond))
		answers, err := permit.Complete(&signature)
		if err != nil || len(answers) != 0 {
			t.Fatal("historical readout added live-source completion gate", err)
		}
		got := result.Work.TargetEndpointObservation()
		if !got.PairKnown || !got.TakeKnown || got.TakeNS != before.TakeNS || got.ReceivedNS != before.ReceivedNS || got.RequestID != before.RequestID || got.SourceIdentity != before.SourceIdentity || got.SourceConnID != before.SourceConnID || got.CompleteNS != int64(750*time.Millisecond) {
			t.Fatal("End lost original target r/c", got)
		}
	})
	t.Run("coalesced-and-cache-recipient-is-not-a-new-signing-work", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		first := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, first.Work)
		original := first.Work.TargetEndpointObservation()
		f.clock.Store(int64(17 * time.Millisecond))
		second := f.reserve(t, f.sources[1], 22, 1)
		if second.Kind != LocalWaitingExisting || second.Work.TargetEndpointObservation() != (OwnedTargetEndpointObservation{}) || first.Work.TargetEndpointObservation() != original {
			t.Fatal("coalesced recipient acquired or replaced first job r")
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
		f.clock.Store(int64(100 * time.Millisecond))
		answers, err := permit.Complete(&signature)
		if err != nil || len(answers) != 2 {
			t.Fatal("genuine coalesced signature completion", len(answers), err)
		}
		completed := first.Work.TargetEndpointObservation()
		if completed.RequestID != 11 || completed.ReceivedNS != original.ReceivedNS || !completed.PairKnown || f.local.local.target.counters.Consumed != 1 || f.local.local.target.counters.Completed != 1 {
			t.Fatal("coalesced recipient changed original history or signing debit")
		}
		// A coalesced source already used this challenge. Exercise the cache with
		// a separate original fixture whose second source has never requested it.
		cacheFixture := p0TargetObservationFixture(t)
		prime := cacheFixture.reserve(t, cacheFixture.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, prime.Work)
		primePermit, primeInput, err := prime.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, cleanupErr := primePermit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
				t.Error(cleanupErr)
			}
		}()
		primeSignature := targetOwnerSignature(t, cacheFixture.key, primeInput)
		cacheFixture.clock.Store(int64(100 * time.Millisecond))
		if primeAnswers, err := primePermit.Complete(&primeSignature); err != nil || len(primeAnswers) != 1 {
			t.Fatal("original cache priming signature completion", len(primeAnswers), err)
		}
		cacheHistory := prime.Work.TargetEndpointObservation()
		cacheFixture.clock.Store(int64(time.Second))
		cached := cacheFixture.reserve(t, cacheFixture.sources[1], 23, 1)
		if cached.Kind != LocalReadyCopy || cached.Copy.answer == nil || cached.Work.TargetEndpointObservation() != (OwnedTargetEndpointObservation{}) || prime.Work.TargetEndpointObservation() != cacheHistory || cacheHistory.RequestID != 11 || !cacheHistory.PairKnown {
			t.Fatal("cached recipient inherited another fresh signing chain")
		}
		if cacheFixture.local.local.target.counters.Consumed != 1 || cacheFixture.local.local.target.counters.Completed != 1 {
			t.Fatal("cache readout minted another T/Complete")
		}
	})
	t.Run("foreign-and-missing-original-provenance-is-unknown", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		result := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, result.Work)
		other := p0TargetObservationFixture(t)
		foreignStamp, foreignPoint := ownedBridgePoint(t, other.domain)
		// Explicit private negative observations only, never Take/admission on them.
		bad := &ownedLocalSignState{local: result.Work.work.local, identity: result.Work.work.identity, entry: result.Work.work.entry}
		bad.identity.stamp, bad.identity.received = foreignStamp, foreignPoint
		if (OwnedLocalSignWork{work: bad}).TargetEndpointObservation() != (OwnedTargetEndpointObservation{}) {
			t.Fatal("foreign original domain fabricated target readout")
		}
		missing := &ownedLocalSignState{local: result.Work.work.local, identity: result.Work.work.identity, entry: result.Work.work.entry}
		missing.identity.source = Session{}
		if (OwnedLocalSignWork{work: missing}).TargetEndpointObservation() != (OwnedTargetEndpointObservation{}) {
			t.Fatal("missing original source fabricated target readout")
		}
	})
	for _, kind := range []string{"cancelled-retains-r-without-c", "failed-signature-retains-r-without-c", "regressed-complete-retains-r-without-c"} {
		t.Run(kind, func(t *testing.T) {
			f := p0TargetObservationFixture(t)
			f.clock.Store(int64(37 * time.Millisecond))
			result := f.reserve(t, f.sources[0], 11, 1)
			targetEndpointReadoutCleanup(t, result.Work)
			before := result.Work.TargetEndpointObservation()
			if !before.ReceivedKnown || before.ReceivedNS != int64(37*time.Millisecond) || before.TakeKnown || before.TakeNS != 0 || before.CompleteKnown || before.PairKnown {
				t.Fatal("original receive evidence missing before refusal", before)
			}
			if kind == "cancelled-retains-r-without-c" {
				if err := result.Work.Cancel(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := result.Work.Take(); !errors.Is(err, ErrStaleWork) {
					t.Fatal("cancelled original regained Take", err)
				}
			} else {
				f.clock.Store(int64(100 * time.Millisecond))
				permit, input, err := result.Work.Take()
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, cleanupErr := permit.Complete(nil); cleanupErr != nil && !errors.Is(cleanupErr, ErrStaleWork) {
						t.Error("mandatory original refusal cleanup", cleanupErr)
					}
				}()
				if kind == "failed-signature-retains-r-without-c" {
					// nil is the original failed-signing contract, not a crypto verifier.
					if answers, err := permit.Complete(nil); !errors.Is(err, ErrStaleWork) || len(answers) != 0 {
						t.Fatal("failed original signer changed refusal", len(answers), err)
					}
				} else {
					signature := targetOwnerSignature(t, f.key, input)
					f.clock.Store(int64(99 * time.Millisecond))
					if answers, err := permit.Complete(&signature); err == nil || len(answers) != 0 {
						t.Fatal("regressed original clock completed", len(answers), err)
					}
				}
			}
			got := result.Work.TargetEndpointObservation()
			want := before
			if kind != "cancelled-retains-r-without-c" {
				want.TakeKnown, want.TakeNS = true, int64(100*time.Millisecond)
			}
			if !got.ReceivedKnown || got.CompleteKnown || got.PairKnown || got != want {
				t.Fatal("original refusal lost a genuine Take or fabricated completion/tuple", want, got)
			}
		})
	}
	t.Run("refused-take-does-not-fabricate-endpoint", func(t *testing.T) {
		f := p0TargetObservationFixture(t)
		result := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, result.Work)
		before := result.Work.TargetEndpointObservation()
		// Existing strict start boundary: received+2s-500ms, not a new limit.
		f.clock.Store(int64(1500 * time.Millisecond))
		if _, _, err := result.Work.Take(); !errors.Is(err, ErrStaleWork) {
			t.Fatal("original strict start refusal changed", err)
		}
		if got := result.Work.TargetEndpointObservation(); got != before || got.TakeKnown || got.CompleteKnown || got.PairKnown {
			t.Fatal("refused original Take fabricated a Sample", before, got)
		}
	})
	t.Run("untimed-original-remains-unknown", func(t *testing.T) {
		f := ownedLocalFixture(t, 1)
		result := f.reserve(t, f.sources[0], 11, 1)
		targetEndpointReadoutCleanup(t, result.Work)
		permit, input, err := result.Work.Take()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := permit.Complete(nil); err != nil && !errors.Is(err, ErrStaleWork) {
				t.Error(err)
			}
		}()
		signature := targetOwnerSignature(t, f.key, input)
		if _, err := permit.Complete(&signature); err != nil {
			t.Fatal(err)
		}
		if result.Work.TargetEndpointObservation() != (OwnedTargetEndpointObservation{}) {
			t.Fatal("ordinary untimed work relabelled as timed target evidence")
		}
	})
}
