package node

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_summary_keys_test.go guards the join between what the layer
// COLLECTS and what the node RENDERS.
//
// The two drifted apart once already: CollectDiagnostics gained the reverse
// block — the only place the shared request quota says whose requests it
// turned away — and the summary's key list was not extended, so the field was
// gathered on every call and dropped on the last line. The documentation
// promised it meanwhile, which is the worst version of the failure: an
// operator reads a contract, asks for the number, and concludes the quota
// never refuses anybody.
//
// Reference: docs/rpc/datagram.md, docs/refactoring/dht/13-measurements.md §8.5.

// TestDatagramSummaryRendersEveryCollectedBlock is the structural guard, and it
// is written against the TYPE rather than against a list of names on purpose:
// a list would have to be updated by the same person who forgot to update the
// summary, which is no guard at all. Adding a field to datagram.Diagnostics and
// not rendering it fails here.
func TestDatagramSummaryRendersEveryCollectedBlock(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	raw, err := svc.FetchDatagramSummary()
	if err != nil {
		t.Fatalf("FetchDatagramSummary: %v", err)
	}

	var rendered map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	diagnostics := reflect.TypeOf(datagram.Diagnostics{})
	for i := range diagnostics.NumField() {
		key := strings.ToLower(diagnostics.Field(i).Name)
		if _, present := rendered[key]; !present {
			t.Fatalf("datagram.Diagnostics has field %s but the summary has no %q key: the block is collected on every call and thrown away, while the docs promise it",
				diagnostics.Field(i).Name, key)
		}
	}
}

// TestDatagramSummaryCarriesTheQuotaAttribution pins the specific number the
// structural guard exists for, in the shape an operator parses.
//
// A shared quota that cannot name whom it refused answers nothing, so this
// asserts the map is present and typed — not merely that some `reverse` key
// exists.
func TestDatagramSummaryCarriesTheQuotaAttribution(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	raw, err := svc.FetchDatagramSummary()
	if err != nil {
		t.Fatalf("FetchDatagramSummary: %v", err)
	}

	var summary struct {
		Reverse struct {
			LocalRefusals map[string]uint64 `json:"LocalRefusals"`
			Held          int               `json:"Held"`
			LocalSlots    int               `json:"LocalSlots"`
		} `json:"reverse"`
	}
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	// Empty, not nil: "no local request has ever been refused" is a real state
	// and must be distinguishable from "this build cannot answer".
	if summary.Reverse.LocalRefusals == nil {
		t.Fatalf("the summary carries no local_refusals map: %s", raw)
	}
	if summary.Reverse.Held != 0 || summary.Reverse.LocalSlots != 0 {
		t.Fatalf("a freshly built node reports %d records and %d occupied slots",
			summary.Reverse.Held, summary.Reverse.LocalSlots)
	}
}

// TestDatagramSummaryBoundsTheCounterPeriod pins that the SERIALIZED datagram
// summary says when its cumulative counters started and when they were read.
//
// Without the start stamp the pair-of-readings rule that guards the routing
// counters cannot be applied here at all: 100 refusals before a restart and 120
// after look like a difference of 20 while describing two unrelated runs, and
// no comparison of the counter to itself detects that — the second run may well
// have passed the first.
//
// Asserted on the JSON rather than on MetricsSnapshot, because the claim of
// step 05 ("every cumulative counter carries its period start") is a claim
// about what an operator can read, not about a Go field.
func TestDatagramSummaryBoundsTheCounterPeriod(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	raw, err := svc.FetchDatagramSummary()
	if err != nil {
		t.Fatalf("FetchDatagramSummary: %v", err)
	}

	var rendered struct {
		Metrics struct {
			StartedAt *time.Time
			ReadAt    *time.Time
		}
	}
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	if rendered.Metrics.StartedAt == nil {
		t.Fatal("metrics.StartedAt missing: the cumulative counters cannot be compared across two reads without it")
	}
	if rendered.Metrics.ReadAt == nil {
		t.Fatal("metrics.ReadAt missing: a rate has no denominator without it")
	}
	if rendered.Metrics.ReadAt.Before(*rendered.Metrics.StartedAt) {
		t.Fatalf("read_at %v precedes started_at %v", *rendered.Metrics.ReadAt, *rendered.Metrics.StartedAt)
	}

	// Sub-second precision, for the same reason the routing stamps carry it:
	// two reads inside one second are exactly the case somebody watching a
	// rollout produces, and a truncated stamp hands them a zero denominator.
	// time.Time marshals as RFC3339Nano, so this pins that nobody replaces it
	// with a second-precision string later.
	var precision struct {
		Metrics struct{ ReadAt string }
	}
	if err := json.Unmarshal(raw, &precision); err != nil {
		t.Fatalf("unmarshal summary as strings: %v", err)
	}
	if parsed, err := time.Parse(time.RFC3339Nano, precision.Metrics.ReadAt); err != nil {
		t.Fatalf("metrics.ReadAt does not parse as RFC3339Nano: %v", err)
	} else if parsed.Nanosecond() == 0 && !strings.Contains(precision.Metrics.ReadAt, ".") {
		t.Fatalf("metrics.ReadAt has no sub-second part: %q", precision.Metrics.ReadAt)
	}
}
