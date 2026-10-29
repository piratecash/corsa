package node

import (
	"testing"
	"time"
)

// Deterministic interleavings of the real marks API. The operations model two
// simultaneously open dials; no scheduler timing or remote TLS peer is needed
// to reproduce the reset between stages of the older attempt.
func TestLateDialStagePreservesConcurrentV2Success(t *testing.T) {
	for _, event := range []string{"unanswered", "probe-v2-hint", "listener-refused"} {
		t.Run(event, func(t *testing.T) {
			clock := newStoreClock()
			marks := newSessionAddressMarks(clock.read)
			marks.jitter = func(time.Duration) time.Duration { return 0 }
			func() {
				closeA := marks.openDial(dialChoiceAddress)
				defer closeA()
				closeB := marks.openDial(dialChoiceAddress)
				defer closeB()
				if wait := marks.noteListenerRefused(dialChoiceAddress); wait != v2RetryBase {
					t.Fatalf("initial refusal wait = %v, want %v", wait, v2RetryBase)
				}
				// B completes after A's first failure, before A's probe ends.
				marks.noteOutboundV2(dialChoiceAddress)
				if !marks.v2Strong(dialChoiceAddress) || marks.v2RetryHeld(dialChoiceAddress) {
					t.Fatal("successful outbound proof did not clear the retry")
				}
				if event == "probe-v2-hint" {
					marks.noteV2Hint(dialChoiceAddress)
				}
				var wait time.Duration
				if event == "listener-refused" {
					wait = marks.noteListenerRefused(dialChoiceAddress)
				} else {
					wait = marks.noteV2Unanswered(dialChoiceAddress)
				}
				if wait != 0 || !marks.v2Strong(dialChoiceAddress) || marks.v2RetryHeld(dialChoiceAddress) {
					t.Fatalf("late stage replaced a successful reset: wait=%v", wait)
				}
				marks.mu.Lock()
				retry := marks.marks[dialChoiceAddress].v2Retry
				stepped := marks.dials[dialChoiceAddress].stepped
				marks.mu.Unlock()
				if retry != nil || !stepped {
					t.Fatal("late stage invented a retry or returned the group's step")
				}
			}()
			// The exhausted group does not suppress the first step of a new dial.
			closeNext := marks.openDial(dialChoiceAddress)
			defer closeNext()
			if wait := marks.noteV2Unanswered(dialChoiceAddress); wait != v2RetryBase {
				t.Fatalf("fresh dial wait = %v, want base %v", wait, v2RetryBase)
			}
		})
	}
}

// Mark eviction is allowed while the independently owned dial group remains.
// This removes the mark directly to cover that valid state, not to claim an
// exercised full-map eviction or a physically concurrent network scenario.
func TestLateDialStageToleratesRemovedRetryMark(t *testing.T) {
	for _, event := range []string{"unanswered", "listener-refused"} {
		t.Run(event, func(t *testing.T) {
			clock := newStoreClock()
			marks := newSessionAddressMarks(clock.read)
			marks.jitter = func(time.Duration) time.Duration { return 0 }
			closeDial := marks.openDial(dialChoiceAddress)
			defer closeDial()
			if wait := marks.noteListenerRefused(dialChoiceAddress); wait != v2RetryBase {
				t.Fatal("initial retry step missing")
			}
			marks.mu.Lock()
			delete(marks.marks, dialChoiceAddress)
			marks.mu.Unlock()
			var wait time.Duration
			if event == "listener-refused" {
				wait = marks.noteListenerRefused(dialChoiceAddress)
			} else {
				wait = marks.noteV2Unanswered(dialChoiceAddress)
			}
			if wait != 0 || marks.v2RetryHeld(dialChoiceAddress) {
				t.Fatalf("removed mark created a second retry: wait=%v", wait)
			}
		})
	}
}
