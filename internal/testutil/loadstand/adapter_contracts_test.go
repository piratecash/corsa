package loadstand

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

func datagramFixture() map[string]any {
	start, read := adapterAt.Add(-time.Second), adapterAt
	sample := datagram.MetricsSnapshot{Observed: 1, Accepted: 1, Delivered: 1, ByMode: map[string]datagram.ModeCounts{"routed": {Observed: 1, Delivered: 1}}, DropsByReason: map[string]uint64{}, ReverseEvents: map[string]uint64{}, SendRefusals: map[string]uint64{}, StartedAt: &start, ReadAt: &read}
	raw, _ := json.Marshal(sample)
	var metrics map[string]any
	_ = json.Unmarshal(raw, &metrics)
	return map[string]any{"enabled": true, "metrics": metrics, "reverse": map[string]any{"LocalRefusals": map[string]any{}, "Held": 0, "LocalSlots": 0}}
}
func TestDatagramSourceRejectsPartialNestedCounters(t *testing.T) {
	baseline := datagramFixture()
	reading, err := typedSource(t, baseline).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sample, err := reading.Get()
	if err != nil || sample.Metrics.Delivered != 1 {
		t.Fatal(err)
	}
	for _, field := range []string{"Observed", "Delivered", "Forwarded", "Answered", "Dropped"} {
		t.Run(field, func(t *testing.T) {
			fixture := datagramFixture()
			row := fixture["metrics"].(map[string]any)["ByMode"].(map[string]any)["routed"].(map[string]any)
			delete(row, field)
			if _, err := typedSource(t, fixture).DatagramSummary(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatalf("missing nested counter became zero: %v", err)
			}
		})
	}
	for _, field := range []string{"DropsByReason", "ReverseEvents", "SendRefusals", "LocalRefusals"} {
		t.Run("null_"+field, func(t *testing.T) {
			fixture := datagramFixture()
			owner := fixture["metrics"].(map[string]any)
			if field == "LocalRefusals" {
				owner = fixture["reverse"].(map[string]any)
			}
			owner[field] = map[string]any{"label": nil}
			if _, err := typedSource(t, fixture).DatagramSummary(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatalf("null nested counter became zero: %v", err)
			}
		})
	}
	for _, mutant := range []string{"unknown_mode", "forged_total", "future_read", "overflow"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := datagramFixture()
			metrics := fixture["metrics"].(map[string]any)
			switch mutant {
			case "unknown_mode":
				metrics["ByMode"] = map[string]any{"future": metrics["ByMode"].(map[string]any)["routed"]}
			case "forged_total":
				metrics["Accepted"] = 2
			case "future_read":
				metrics["ReadAt"] = adapterAt.Add(time.Second)
			case "overflow":
				row := metrics["ByMode"].(map[string]any)["routed"].(map[string]any)
				row["Forwarded"] = uint64(math.MaxUint64)
				metrics["Forwarded"] = uint64(math.MaxUint64)
			}
			if _, err := typedSource(t, fixture).DatagramSummary(t.Context()); !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("invalid metrics accepted: %v", err)
			}
		})
	}
	off := datagramStateFixture()
	off.State = domain.DatagramStateConfiguredOff
	off.ConfiguredEnabled = false
	disabled, err := stateAndSummarySource(t, off, nil).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disabled.Get(); !errors.Is(err, ErrDatagramDisabled) {
		t.Fatal(err)
	}
}
func TestTypedNestedFamiliesCannotSilentlyLoseFields(t *testing.T) {
	for _, field := range []string{"operation", "mode", "reason", "count"} {
		t.Run("decision_"+field, func(t *testing.T) {
			fixture := modeFixture()
			delete(fixture["decisions"].([]any)[0].(map[string]any), field)
			if _, err := typedSource(t, map[string]any{"mode_selection": fixture}).ModeSelection(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatal(err)
			}
		})
	}
	for _, field := range []string{"capability", "connections", "peers"} {
		t.Run("neighbour_"+field, func(t *testing.T) {
			fixture := neighbourFixture()
			delete(fixture["capabilities"].([]any)[0].(map[string]any), field)
			if _, err := typedSource(t, map[string]any{"neighbours": fixture}).Neighbours(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatal(err)
			}
		})
	}
	for _, field := range []string{"name", "kind", "count", "entry_bytes", "floor_bytes"} {
		t.Run("gauge_"+field, func(t *testing.T) {
			fixture := resourceFixture()
			row := fixture["subsystems"].([]any)[0].(map[string]any)
			gauge := map[string]any{"name": "entries", "kind": "memory", "count": 0, "entry_bytes": 8, "floor_bytes": 0}
			delete(gauge, field)
			row["gauges"] = []any{gauge}
			if _, err := typedSource(t, fixture).Resources(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatal(err)
			}
		})
	}
	fixture := resourceFixture()
	fixture["subsystems"] = fixture["subsystems"].([]any)[1:]
	if _, err := typedSource(t, fixture).Resources(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
		t.Fatalf("missing owner became zero: %v", err)
	}
}
func TestAdapterDuplicateKeysAreRejectedBeforeZeroDecode(t *testing.T) {
	for _, raw := range []string{`{"count":1,"count":2}`, `{"nested":{"x":1,"\u0078":2}}`, `{"x":1} {"x":2}`} {
		if _, err := object(json.RawMessage(raw)); !errors.Is(err, ErrInvalidSample) {
			t.Fatalf("duplicate/trailing accepted: %v", err)
		}
	}
}

func TestProcessTypedAdaptersRejectInconsistentCensusAndFloors(t *testing.T) {
	for _, mutant := range []string{"unknown_reason", "duplicate_decision", "backward_period", "future_period"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := modeFixture()
			switch mutant {
			case "unknown_reason":
				fixture["decisions"].([]any)[0].(map[string]any)["reason"] = "new_reason"
			case "duplicate_decision":
				row := fixture["decisions"].([]any)[0]
				fixture["decisions"] = []any{row, row}
			case "backward_period":
				fixture["read_at"] = adapterAt.Add(-2 * time.Second)
			case "future_period":
				fixture["read_at"] = adapterAt.Add(time.Second)
			}
			if _, err := typedSource(t, map[string]any{"mode_selection": fixture}).ModeSelection(t.Context()); !errors.Is(err, ErrInvalidSample) {
				t.Fatal(err)
			}
		})
	}
	for _, mutant := range []string{"missing_capability", "duplicate_capability", "unknown_capability", "negative_population", "peers_exceed_connections", "future_census"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := neighbourFixture()
			rows := fixture["capabilities"].([]any)
			switch mutant {
			case "missing_capability":
				fixture["capabilities"] = rows[1:]
			case "duplicate_capability":
				fixture["capabilities"] = append(rows, rows[0])
			case "unknown_capability":
				rows[0].(map[string]any)["capability"] = "future_capability"
			case "negative_population":
				fixture["connections"] = -1
			case "peers_exceed_connections":
				fixture["peers"] = 1
			case "future_census":
				fixture["updated_at"] = adapterAt.Add(time.Second)
			}
			if _, err := typedSource(t, map[string]any{"neighbours": fixture}).Neighbours(t.Context()); err == nil {
				t.Fatal("invalid population accepted")
			}
		})
	}
	for _, mutant := range []string{"unknown_owner", "duplicate_owner", "unknown_kind", "overflow", "wrong_product", "saturation_bytes", "future_sample"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := resourceFixture()
			rows := fixture["subsystems"].([]any)
			row := rows[0].(map[string]any)
			gauge := map[string]any{"name": "entries", "kind": "memory", "count": 0, "entry_bytes": 8, "floor_bytes": 0}
			row["gauges"] = []any{gauge}
			switch mutant {
			case "unknown_owner":
				row["subsystem"] = "future_owner"
			case "duplicate_owner":
				fixture["subsystems"] = append(rows, rows[0])
			case "unknown_kind":
				gauge["kind"] = "future_kind"
			case "overflow":
				gauge["count"] = uint64(math.MaxUint64)
			case "wrong_product":
				gauge["count"] = 1
			case "saturation_bytes":
				gauge["kind"] = "saturation"
			case "future_sample":
				fixture["sampled_at"] = adapterAt.Add(time.Second)
			}
			if _, err := typedSource(t, fixture).Resources(t.Context()); !errors.Is(err, ErrInvalidSample) {
				t.Fatal(err)
			}
		})
	}
}

func TestProcessDatagramRejectsCaseFoldFieldAliases(t *testing.T) {
	for _, mutant := range []struct {
		name, alias string
		value       any
		nested      bool
	}{
		{"ASCII_null_period", "readat", nil, false},
		{"ASCII_forged_period", "readat", adapterAt.Add(-time.Hour), false},
		{"ASCII_null_map", "bymode", nil, false},
		{"ASCII_forged_counter", "unknowndtype", 42, false},
		{"secret_alias_value", "readat", "REMOTE_ALIAS_SECRET", false},
		{"Unicode_long_s", "ſtartedat", nil, false},
		{"Unicode_Kelvin", "unKnowndtype", 42, false},
		{"nested_mode_counter", "observed", 0, true},
	} {
		t.Run(mutant.name, func(t *testing.T) {
			fixture := datagramFixture()
			fields := fixture["metrics"].(map[string]any)
			if mutant.nested {
				fields = fields["ByMode"].(map[string]any)["routed"].(map[string]any)
			}
			fields[mutant.alias] = mutant.value
			defer func() {
				if recover() != nil {
					t.Error("field alias panicked instead of refusing malformed reading")
				}
			}()
			_, err := typedSource(t, fixture).DatagramSummary(t.Context())
			if !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("field alias accepted: %v", err)
			}
			if strings.Contains(err.Error(), "REMOTE_ALIAS_SECRET") {
				t.Fatal("remote alias value escaped into error")
			}
		})
	}
}
func TestProcessBanAliasesCannotEraseValidatedProtection(t *testing.T) {
	for _, mutant := range []string{"ASCII_top_null", "Unicode_top_counter", "nested_until", "optional_blacklist_present", "optional_blacklist_absent", "Unicode_nested_score"} {
		t.Run(mutant, func(t *testing.T) {
			fixture := banFixture()
			switch mutant {
			case "ASCII_top_null":
				fixture["local_bans"] = []any{}
				fixture["LOCAL_BANS"] = nil
			case "Unicode_top_counter":
				fixture["loopback_identitieſ_scored"] = 0
			case "optional_blacklist_present":
				fixture["local_bans"].([]any)[1].(map[string]any)["BLACKLISTED_UNTIL"] = nil
			case "optional_blacklist_absent":
				fixture["local_bans"].([]any)[0].(map[string]any)["BLACKLISTED_UNTIL"] = adapterAt.Add(time.Hour)
			case "Unicode_nested_score":
				fixture["local_bans"].([]any)[0].(map[string]any)["ſcore"] = 0
			}
			raw, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if mutant == "nested_until" {
				// Preserve the attack's order: encoding/json sees the alias after the
				// canonical expiry even though required-field validation reads the latter.
				until, _ := json.Marshal(adapterAt.Add(time.Minute))
				past, _ := json.Marshal(adapterAt.Add(-time.Hour))
				raw = bytes.Replace(raw, append([]byte(`"until":`), until...), append(append(append([]byte(`"until":`), until...), []byte(`,"Until":`)...), past...), 1)
			}
			source, err := NewProcessSourceWithClock(protocolClient(t, 200, string(raw)), adapterClock{adapterAt})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if recover() != nil {
					t.Error("ban alias panicked instead of refusing malformed reading")
				}
			}()
			if _, err = source.Bans(t.Context(), adapterAt, BanPolicy{}); !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("ban alias changed validated protection: %v", err)
			}
		})
	}
}
func TestCanonicalFieldsDoNotCaseFoldArbitraryCounterLabels(t *testing.T) {
	fixture := datagramFixture()
	labels := map[string]any{"label": 1, "LABEL": 2, "s": 3, "ſ": 4, "k": 5, "K": 6}
	fixture["metrics"].(map[string]any)["DropsByReason"] = labels
	fixture["reverse"].(map[string]any)["LocalRefusals"] = labels
	reading, err := typedSource(t, fixture).DatagramSummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sample, err := reading.Get()
	if err != nil {
		t.Fatal(err)
	}
	if len(sample.Metrics.DropsByReason) != len(labels) || len(sample.Reverse.LocalRefusals) != len(labels) || sample.Metrics.DropsByReason["ſ"] != 4 || sample.Reverse.LocalRefusals["K"] != 6 {
		t.Fatal("arbitrary labels were case folded")
	}
}

func TestProcessExistingDTOPathsUseSameCanonicalNames(t *testing.T) {
	zeroCount, zeroCPU := uint64(0), float64(0)
	runtimeReading := domain.RuntimeMetrics{Version: 1, StartedAt: adapterAt.Add(-time.Second), ReadAt: adapterAt, HeapLiveBytes: &zeroCount, HeapObjectsBytes: &zeroCount, HeapAllocsBytes: &zeroCount, GoMemoryBytes: &zeroCount, GCCycles: &zeroCount, Goroutines: &zeroCount, GOMAXPROCS: &zeroCount, CPUTotalSeconds: &zeroCPU, CPUUserSeconds: &zeroCPU, CPUGCSeconds: &zeroCPU, CPUIdleSeconds: &zeroCPU, Unsupported: []domain.RuntimeMetricName{}}
	cases := []struct {
		name             string
		payload          any
		canonical, alias string
		read             func(*ProcessSource) error
	}{
		{"status", domain.NodeStatus{Identity: "node", Address: "node"}, "identity", "IDENTITY", func(s *ProcessSource) error { _, err := s.Status(t.Context()); return err }},
		{"runtime", runtimeReading, "cpu_user_seconds", "cpu_uſer_ſeconds", func(s *ProcessSource) error { _, err := s.Runtime(t.Context()); return err }},
		{"traffic", map[string]any{"started_at": adapterAt.Add(-time.Second), "read_at": adapterAt, "bytes_sent": 1, "bytes_received": 1}, "bytes_sent", "BYTES_SENT", func(s *ProcessSource) error { _, err := s.TransportTraffic(t.Context()); return err }},
		{"sessions", map[string]any{"started_at": adapterAt.Add(-time.Second), "read_at": adapterAt, "attempts": 1, "succeeded": 1, "errors_connect": 0, "errors_compat": 0, "errors_other": 0}, "succeeded", "SUCCEEDED", func(s *ProcessSource) error { _, err := s.SessionOutcomes(t.Context()); return err }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.payload)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err = json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			wrap := func() any {
				if test.name == "traffic" {
					return map[string]any{"transport_traffic": fields}
				}
				if test.name == "sessions" {
					return map[string]any{"session_outcomes": fields}
				}
				return fields
			}
			if err = test.read(typedSource(t, wrap())); err != nil {
				t.Fatalf("canonical valid DTO refused: %v", err)
			}
			fields[test.alias] = fields[test.canonical]
			if err = test.read(typedSource(t, wrap())); !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("conflicting alias accepted: %v", err)
			}
			delete(fields, test.canonical)
			if err = test.read(typedSource(t, wrap())); !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("alias substituted for canonical field: %v", err)
			}
		})
	}
}

func TestProcessPeerReadinessUsesCanonicalNestedDTOFields(t *testing.T) {
	for _, alias := range []bool{false, true} {
		row := map[string]any{"address": "192.0.2.1:64650", "connected": true}
		if alias {
			row["Connected"] = false
		}
		source := typedSource(t, map[string]any{"type": "peer_health", "peer_health": []any{row}})
		peers, err := source.ConnectedPeers(t.Context())
		if alias {
			if !errors.Is(err, ErrInvalidSample) {
				t.Fatalf("nested readiness alias accepted: %v", err)
			}
		} else if err != nil || len(peers) != 1 {
			t.Fatalf("canonical peer health refused: %v", err)
		}
	}
}
