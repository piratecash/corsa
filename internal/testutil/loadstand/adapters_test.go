package loadstand

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

var adapterAt = time.Date(2026, 10, 18, 0, 0, 1, 0, time.UTC)

type adapterClock struct{ at time.Time }

func (c adapterClock) Now() time.Time { return c.at }
func typedSource(t *testing.T, payload any) *ProcessSource {
	t.Helper()
	return stateAndSummarySource(t, datagramStateFixture(), payload)
}
func datagramStateFixture() domain.DatagramRuntimeState {
	return domain.DatagramRuntimeState{Version: 1, Identity: domain.PeerIdentity{1}, StartedAt: adapterAt.Add(-time.Second), ReadAt: adapterAt, ConfiguredEnabled: true, State: domain.DatagramStateInitialized}
}
func stateAndSummarySource(t *testing.T, state, summary any) *ProcessSource {
	t.Helper()
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Command string `json:"command"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		payload := summary
		if req.Command == "getDatagramRuntimeState" {
			payload = state
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	source, err := NewBoundProcessSource(client, adapterClock{adapterAt}, RuntimeBinding{Identity: domain.PeerIdentity{1}, StartedAt: adapterAt.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func modeFixture() map[string]any {
	return map[string]any{"started_at": adapterAt.Add(-time.Second), "read_at": adapterAt, "decisions": []any{map[string]any{"operation": "delta", "mode": "v3", "reason": "negotiated", "count": 3}}}
}
func neighbourFixture() map[string]any {
	rows := make([]any, 0, len(neighbourCapabilities))
	for _, capability := range neighbourCapabilities {
		rows = append(rows, map[string]any{"capability": capability.String(), "connections": 0, "peers": 0})
	}
	return map[string]any{"ready": true, "updated_at": adapterAt, "connections": 0, "peers": 0, "identity_unproven": 0, "identity_unknown": 0, "capabilities": rows, "routing_v3_triplet": map[string]any{"capability": domain.CapMeshRoutingV3.String(), "connections": 0, "peers": 0}}
}
func resourceFixture() map[string]any {
	rows := make([]any, 0)
	for _, owner := range domain.ResourceSubsystems() {
		rows = append(rows, map[string]any{"subsystem": owner.String(), "floor_bytes": uint64(0), "gauges": []any{}})
	}
	return map[string]any{"sampled_at": adapterAt, "floor_bytes": uint64(0), "subsystems": rows}
}
func TestProcessTypedFamiliesRequireEveryMeasuredField(t *testing.T) {
	for _, family := range []string{"mode_selection", "neighbours"} {
		fixture := modeFixture()
		if family == "neighbours" {
			fixture = neighbourFixture()
		}
		for missing := range fixture {
			t.Run(family+"_"+missing, func(t *testing.T) {
				copy := map[string]any{}
				for k, v := range fixture {
					copy[k] = v
				}
				delete(copy, missing)
				s := typedSource(t, map[string]any{family: copy})
				var e error
				if family == "mode_selection" {
					_, e = s.ModeSelection(t.Context())
				} else {
					_, e = s.Neighbours(t.Context())
				}
				if !errors.Is(e, ErrFamilyUnavailable) {
					t.Fatalf("missing family field silently zero: %v", e)
				}
			})
		}
	}
}
func TestProcessTypedModeNamesAndGaugeReadiness(t *testing.T) {
	s := typedSource(t, map[string]any{"mode_selection": modeFixture(), "neighbours": neighbourFixture()})
	modes, e := s.ModeSelection(t.Context())
	if e != nil || len(modes.Decisions) != 1 || modes.Decisions[0].Operation != routing.AnnounceOperationDelta || modes.Decisions[0].Mode != routing.AnnounceModeV3 || modes.Decisions[0].Reason != routing.ModeReasonNegotiated {
		t.Fatal(e)
	}
	neighbours, e := s.Neighbours(t.Context())
	if e != nil || !neighbours.Ready || neighbours.Connections != 0 {
		t.Fatal(e)
	}
	bad := modeFixture()
	bad["decisions"] = []any{map[string]any{"operation": "future_operation", "mode": "v3", "reason": "negotiated", "count": 3}}
	if _, e := typedSource(t, map[string]any{"mode_selection": bad}).ModeSelection(t.Context()); !errors.Is(e, ErrInvalidSample) {
		t.Fatalf("unknown enum folded to zero: %v", e)
	}
	unready := neighbourFixture()
	unready["ready"] = false
	if _, e := typedSource(t, map[string]any{"neighbours": unready}).Neighbours(t.Context()); !errors.Is(e, ErrFamilyUnavailable) {
		t.Fatal(e)
	}
}
func TestResourceSourceReconstructsFloorsAndRefusesArithmeticForgery(t *testing.T) {
	fixture := resourceFixture()
	owners := fixture["subsystems"].([]any)
	route := owners[0].(map[string]any)
	route["gauges"] = []any{map[string]any{"name": "claims", "kind": "memory", "count": 2, "entry_bytes": 8, "floor_bytes": 16}, map[string]any{"name": "occupied", "kind": "saturation", "count": 2, "entry_bytes": 0, "floor_bytes": 0}}
	route["floor_bytes"] = 16
	fixture["floor_bytes"] = 16
	sample, e := typedSource(t, fixture).Resources(t.Context())
	if e != nil || sample.FloorBytes() != 16 || !sample.SampledAt().Equal(adapterAt) {
		t.Fatal(e)
	}
	fixture["floor_bytes"] = 17
	if _, e := typedSource(t, fixture).Resources(t.Context()); !errors.Is(e, ErrInvalidSample) {
		t.Fatalf("forged total accepted: %v", e)
	}
}
func TestUnavailableDatagramIsNotInventedDisabledZero(t *testing.T) {
	for _, status := range []int{404, 503} {
		s := NewProcessSource(protocolClient(t, status, `{}`))
		if _, e := s.DatagramSummary(t.Context()); !errors.Is(e, ErrFamilyUnavailable) {
			t.Fatalf("status%d: %v", status, e)
		}
	}
}
