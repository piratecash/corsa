package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// receipt_dedup_usage_test.go pins how the resource breakdown is allowed to ask
// the receipt-dedup set for its size.
//
// The distinction is not academic. Len walks the whole previous generation —
// up to maxReceiptDedupEntries keys — to subtract the overlap, and it does that
// under the dedup mutex. finishReceipt takes the SAME mutex while holding
// deliveryMu, so a diagnostic that called Len could park the entire delivery
// domain behind a 50 000-key scan. A measuring instrument that stalls the thing
// it measures is not a measurement.
//
// Reference: docs/refactoring/dht/13-measurements.md §4, §8.1.

// TestStoredLenDoesNotRotate pins the second half of the problem: Len rotates
// when a rotation is due, and rotation is a WRITE. An accounting read must not
// advance the generation it is counting, or two consecutive diagnostics change
// the state between them.
func TestStoredLenDoesNotRotate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	clock := func() time.Time { return now }
	dedup := newRotatingHashDedup(time.Minute, 1_000, clock)

	dedup.Add("first-generation-key")
	// Move past the rotation deadline WITHOUT touching the set through a
	// mutating method.
	now = now.Add(2 * time.Minute)

	if got := dedup.StoredLen(); got != 1 {
		t.Fatalf("StoredLen() = %d before any rotation, want 1", got)
	}
	// Still one, and still in the CURRENT generation: had StoredLen rotated,
	// the key would have moved and a following Add would sit in a fresh
	// generation beside it, giving two.
	dedup.mu.Lock()
	current := len(dedup.current)
	previous := len(dedup.previous)
	dedup.mu.Unlock()
	if current != 1 || previous != 0 {
		t.Fatalf("StoredLen rotated the set: current=%d previous=%d, want 1/0", current, previous)
	}
}

// TestStoredLenCountsBothGenerations pins that the accounting figure does NOT
// subtract the overlap — which is the correct question for memory and the
// cheap one to answer.
//
// A key recorded before and after a rotation occupies a slot in each
// generation, and both slots are resident until the older one is dropped. Len
// answers "how many receipts are still recognised", a different and smaller
// number, and pays a full scan for it.
func TestStoredLenCountsBothGenerations(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	clock := func() time.Time { return now }
	dedup := newRotatingHashDedup(time.Minute, 1_000, clock)

	dedup.Add("shared-key")
	// Past the deadline but LESS than a full interval past it, so the current
	// generation is promoted rather than both being discarded — a clock jump of
	// a whole interval clears everything, which would leave nothing to overlap.
	now = now.Add(90 * time.Second)
	// Rotates (the key moves to previous), then records the same key again in
	// the fresh generation.
	dedup.Add("shared-key")

	if got := dedup.StoredLen(); got != 2 {
		t.Fatalf("StoredLen() = %d, want 2: the key holds a slot in each generation and both are resident", got)
	}
	if got := dedup.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1: the deduplicated count is a different question", got)
	}
}

// TestBreakdownReadsTheDedupSetWithoutScanningIt is the guard on the CALL SITE,
// and it is the one that would catch a future edit swapping the two methods
// back: it fills both generations with an overlap and asserts the breakdown
// reports what is stored rather than what is distinct.
//
// Without it, the two methods differ only in a number nobody compares, and the
// stall would return silently.
func TestBreakdownReadsTheDedupSetWithoutScanningIt(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)

	// Two generations with a deliberate overlap: stored 4, distinct 3.
	svc.seenReceipts = newRotatingHashDedup(time.Minute, 1_000, nil)
	svc.seenReceipts.Add("overlapping")
	svc.seenReceipts.Add("only-in-previous")
	svc.seenReceipts.mu.Lock()
	svc.seenReceipts.previous = svc.seenReceipts.current
	svc.seenReceipts.current = map[dedupKey]struct{}{}
	svc.seenReceipts.mu.Unlock()
	svc.seenReceipts.Add("overlapping")
	svc.seenReceipts.Add("only-in-current")

	stored := svc.seenReceipts.StoredLen()
	distinct := svc.seenReceipts.Len()
	if stored == distinct {
		t.Fatalf("the fixture built no overlap (stored %d, distinct %d): the assertion below would prove nothing",
			stored, distinct)
	}

	gauge := deliveryGauge(t, svc.ResourceBreakdown(), "receipt_dedup")
	if gauge.Count() != uint64(stored) {
		t.Fatalf("receipt_dedup reported %d, want the STORED %d (distinct is %d): reading the distinct count means scanning a generation under the mutex finishReceipt takes while holding deliveryMu",
			gauge.Count(), stored, distinct)
	}
}

// deliveryGauge extracts one named gauge from the delivery subsystem, failing
// loudly rather than returning a zero a comparison would pass on.
func deliveryGauge(t *testing.T, breakdown domain.ResourceBreakdown, name string) domain.ResourceGauge {
	t.Helper()
	for _, usage := range breakdown.Subsystems() {
		if usage.Subsystem() != domain.ResourceSubsystemDelivery {
			continue
		}
		for _, gauge := range usage.Gauges() {
			if gauge.Name() == name {
				return gauge
			}
		}
	}
	t.Fatalf("the delivery subsystem reports no %q gauge", name)
	return domain.ResourceGauge{}
}
