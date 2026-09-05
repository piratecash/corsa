package node

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

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
