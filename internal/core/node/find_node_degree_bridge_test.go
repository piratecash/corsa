package node

import (
	"math"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

func TestOwnedNodeDegreeLastWaitAndPureHistory(t *testing.T) {
	s, clock, d := ownedCaptureTestService(t, 2, 2, 4)
	peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	ownedRawAccepted(t, s, peer)
	o := s.degree
	o.mu.Lock()
	history, used, unknown := o.history, o.historyN, o.unknown
	active := append([]degreeActiveRow(nil), o.active...)
	held := true
	t.Cleanup(func() {
		if held {
			o.mu.Unlock()
		}
	})
	clockGuard, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	clockHeld := true
	t.Cleanup(func() {
		if clockHeld {
			clockGuard.Release()
		}
	})
	type outcome struct {
		got findNodeOwnedObservation
		err error
	}
	result := make(chan outcome, 1)
	joined := make(chan struct{})
	// Cleanup is registered BEFORE dispatch and releases both owned barriers
	// before joining the exact job; no leaked worker survives assertion failure.
	t.Cleanup(func() {
		if held {
			o.mu.Unlock()
			held = false
		}
		if clockHeld {
			clockGuard.Release()
			clockHeld = false
		}
		<-joined
	})
	go func() {
		defer close(joined)
		got, err := s.captureOwnedFindNodeObservation(t.Context())
		result <- outcome{got, err}
	}()
	clock.Store(int64(5 * time.Second))
	o.mu.Unlock()
	held = false
	waitFor(t, 2*time.Second, "Node reader owns degree while waiting original clock", func() bool {
		if o.mu.TryLock() {
			o.mu.Unlock()
			return false
		}
		return true
	})
	clock.Store(int64(7 * time.Second))
	clockGuard.Release()
	clockHeld = false
	var out outcome
	select {
	case out = <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("Node observation job did not finish")
	}
	<-joined
	if out.err != nil || out.got.degree.at.Elapsed() != 7*time.Second {
		t.Fatal("Node q preceded last degree/clock wait", out.got, out.err)
	}
	o.mu.Lock()
	pure := o.history == history && o.historyN == used && o.unknown == unknown && len(o.active) == len(active)
	for i, row := range active {
		pure = pure && o.active[i] == row
	}
	o.mu.Unlock()
	if !pure {
		t.Fatal("Node read changed shared history or active witnesses")
	}
	ownedCaptureAssertPoint(t, s, d, out.got)
	// Positive barrier evidence only; a mutant timeout is NOT F qualification.
}
func TestOwnedNodeDegreeGenuineInactiveFrontier(t *testing.T) {
	s, clock, d := ownedCaptureTestService(t, 2, 2, 4)
	peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	f, _ := ownedRawAccepted(t, s, peer)
	clock.Store(int64(2 * time.Second))
	f.inbound.life.end()
	clock.Store(int64(602*time.Second - time.Nanosecond))
	before, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || before.degree.window != 1 || before.degree.degreeValidBefore.Elapsed() != 602*time.Second {
		t.Fatal("genuine inactive frontier lost exact last+600", before, err)
	}
	ownedCaptureAssertPoint(t, s, d, before)
	clock.Store(int64(602 * time.Second))
	at, err := s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || at.degree.window != 0 || !at.degree.degreeValidBefore.Contains(at.degree.at) {
		t.Fatal("expired genuine degree contribution survived exclusive boundary", at, err)
	}
	ownedCaptureAssertPoint(t, s, d, at)
}
func TestOwnedNodeDegreeUnknownIsUnavailableAndRecoversCoverage(t *testing.T) {
	s, _, d := ownedCaptureTestService(t, 1, 4, 8)
	a, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	b, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	ownedRawAccepted(t, s, a)
	second, _ := ownedRawAccepted(t, s, b)
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	ownedCaptureAssertEmpty(t, got, err)
	s.degree.mu.Lock()
	unknown, poisoned := s.degree.unknown, s.degree.poisoned
	s.degree.mu.Unlock()
	if unknown != 1 || poisoned {
		t.Fatal("unknown coverage became zero or new poison", unknown, poisoned)
	}
	if _, err := d.Observer().BeginCapture(); err != nil {
		t.Fatal("unknown coverage poisoned leaf", err)
	}
	second.inbound.life.end()
	got, err = s.captureOwnedFindNodeObservation(t.Context())
	if err != nil || got.degree.window < 1 {
		t.Fatal("mandatory genuine overflow retirement failed to restore coverage", got, err)
	}
}
func TestOwnedNodeDegreeFaultPoisonsExpectedOwnerAfterClockRelease(t *testing.T) {
	s, _, d := ownedCaptureTestService(t, 2, 2, 4)
	peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	foreign, _, _ := ownedCaptureTestService(t, 2, 2, 4)
	f, _ := ownedRawAccepted(t, s, peer)
	o := s.degree
	w := f.inbound.life.degreeWitness
	// Negative corruption of an actually minted witness, never a positive mint.
	o.mu.Lock()
	w.owner = foreign.degree
	o.mu.Unlock()
	t.Cleanup(func() { o.mu.Lock(); w.owner = o; o.mu.Unlock() })
	stamp := pinPublicationCapture(t, d)
	got, err := s.captureOwnedFindNodeObservation(t.Context())
	ownedCaptureAssertEmpty(t, got, err)
	o.mu.Lock()
	expectedPoisoned := o.poisoned
	o.mu.Unlock()
	foreign.degree.mu.Lock()
	otherPoisoned := foreign.degree.poisoned
	foreign.degree.mu.Unlock()
	if !expectedPoisoned || otherPoisoned || !pinPublicationRefused(d, stamp) {
		t.Fatal("read fault did not publish ONLY expected-owner poison")
	}
	clock, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal("read fault left primitive clock poisoned unexpectedly", err)
	}
	clock.Release()
	// No timeout/deadlock of a lock-order mutant counts as a causal F result.
}
func TestOwnedNodeDegreeInvalidClockStillAllowsMandatoryEnd(t *testing.T) {
	for _, kind := range []string{"negative", "regression", "maximum"} {
		t.Run(kind, func(t *testing.T) {
			s, clock, d := ownedCaptureTestService(t, 2, 2, 4)
			peer, _, _ := ownedCaptureTestService(t, 2, 2, 4)
			f, _ := ownedRawAccepted(t, s, peer)
			if kind == "regression" {
				clock.Store(10)
				if _, err := s.captureOwnedFindNodeObservation(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "negative":
				clock.Store(-1)
			case "regression":
				clock.Store(5)
			case "maximum":
				clock.Store(math.MaxInt64)
			}
			got, err := s.captureOwnedFindNodeObservation(t.Context())
			ownedCaptureAssertEmpty(t, got, err)
			f.inbound.life.end()
			f.inbound.life.mu.Lock()
			ended := f.inbound.life.ended && f.inbound.life.degreeWitness == nil && f.inbound.life.evidence == nil
			f.inbound.life.mu.Unlock()
			if !ended {
				t.Fatal("read clock refusal skipped original mandatory End")
			}
			if _, err := d.Observer().BeginCapture(); err == nil {
				t.Fatal("invalid clock did not poison leaf")
			}
		})
	}
}
func TestOwnedNodeDegreeFrontierZeroAndForeignPointRefuse(t *testing.T) {
	if _, err := ownedDegreeProjectionFrontier(findnodebridge.OwnedInstant{}, [degreeHistoryLimit]degreeHistoryRow{}, 0); err == nil {
		t.Fatal("zero point became a frontier")
	}
	s, _, _ := ownedCaptureTestService(t, 1, 1, 2)
	other, _, otherDomain := ownedCaptureTestService(t, 1, 1, 2)
	got, err := other.captureOwnedFindNodeObservation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := s.bridgeCapture.observer.BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	if s.bridgeCapture.consumer.Matches(stamp, got.degree.at) || !otherDomain.Consumer().Matches(got.stamp, got.degree.at) {
		t.Fatal("foreign original role/point identity accepted")
	}
}
