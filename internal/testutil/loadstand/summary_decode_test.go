package loadstand

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// The real authenticated HTTP client is retained; only the local server reply
// varies. No ProcessSource field, transport global or production hook is added.
func summaryTestSource(t *testing.T, respond func(string, http.ResponseWriter)) *ProcessSource {
	t.Helper()
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != credentials.username || password != credentials.passwordText() || r.Method != http.MethodPost || r.URL.Path != "/rpc/v1/exec" {
			t.Error("changed authenticated RPC request")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Command string `json:"command"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		respond(request.Command, w)
	}))
	t.Cleanup(server.Close)
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	source, err := NewProcessSourceWithClock(client, adapterClock{adapterAt})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func summaryTestPayload(count uint64, started, at time.Time) map[string]any {
	modes := modeFixture()
	modes["started_at"], modes["read_at"] = started, at
	modes["decisions"] = []any{map[string]any{"operation": "delta", "mode": "v3", "reason": "negotiated", "count": count}}
	neighbours := neighbourFixture()
	neighbours["updated_at"], neighbours["connections"] = at, count
	return map[string]any{
		"transport_traffic": map[string]any{"started_at": started, "read_at": at, "bytes_sent": count, "bytes_received": count},
		"session_outcomes":  map[string]any{"started_at": started, "read_at": at, "attempts": count, "succeeded": count, "errors_connect": 0, "errors_compat": 0, "errors_other": 0},
		"mode_selection":    modes, "neighbours": neighbours, "total_entries": count,
	}
}

func TestProcessSummaryPublicMethodsStillFetchPerCall(t *testing.T) {
	readers := []struct {
		name string
		read func(*ProcessSource) (uint64, error)
	}{
		{"traffic", func(s *ProcessSource) (uint64, error) {
			v, err := s.TransportTraffic(t.Context())
			return v.BytesSent, err
		}},
		{"sessions", func(s *ProcessSource) (uint64, error) {
			v, err := s.SessionOutcomes(t.Context())
			return v.Attempts, err
		}},
		{"modes", func(s *ProcessSource) (uint64, error) {
			v, err := s.ModeSelection(t.Context())
			if err != nil || len(v.Decisions) != 1 {
				return 0, errors.Join(err, ErrInvalidSample)
			}
			return v.Decisions[0].Count, nil
		}},
		{"neighbours", func(s *ProcessSource) (uint64, error) {
			v, err := s.Neighbours(t.Context())
			return uint64(v.Connections), err
		}},
		{"routes", func(s *ProcessSource) (uint64, error) { v, err := s.RouteEntries(t.Context()); return uint64(v), err }},
	}
	for _, test := range readers {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Uint64
			source := summaryTestSource(t, func(command string, w http.ResponseWriter) {
				if command != "fetchRouteSummary" {
					t.Error("unexpected command", command)
				}
				if err := json.NewEncoder(w).Encode(summaryTestPayload(calls.Add(1), adapterAt.Add(-time.Second), adapterAt)); err != nil {
					t.Error(err)
				}
			})
			for want := uint64(1); want <= 2; want++ {
				got, err := test.read(source)
				if err != nil || got != want || calls.Load() != want {
					t.Fatalf("public accessor reused an earlier reply: got=%d calls=%d err=%v", got, calls.Load(), err)
				}
			}
		})
	}
}

// The public and sample-local paths enter the same original strict decoders.
// These cases reuse the existing DTO fixtures and refusal categories, including
// overflow/count/period laws rather than asserting implementation-shaped text.
func TestProcessSummaryPrivateDecodersKeepStrictRefusals(t *testing.T) {
	valid := summaryTestPayload(1, adapterAt.Add(-time.Second), adapterAt)
	type familyReader struct {
		name   string
		public func(*ProcessSource) error
		local  func(*ProcessSource, map[string]json.RawMessage, error) error
	}
	readers := []familyReader{
		{"transport_traffic", func(s *ProcessSource) error { _, err := s.TransportTraffic(t.Context()); return err }, func(_ *ProcessSource, v map[string]json.RawMessage, e error) error {
			_, err := transportTrafficFromSummary(v, e)
			return err
		}},
		{"session_outcomes", func(s *ProcessSource) error { _, err := s.SessionOutcomes(t.Context()); return err }, func(_ *ProcessSource, v map[string]json.RawMessage, e error) error {
			_, err := sessionOutcomesFromSummary(v, e)
			return err
		}},
		{"mode_selection", func(s *ProcessSource) error { _, err := s.ModeSelection(t.Context()); return err }, func(s *ProcessSource, v map[string]json.RawMessage, e error) error {
			_, err := s.modeSelectionFromSummary(v, e)
			return err
		}},
		{"neighbours", func(s *ProcessSource) error { _, err := s.Neighbours(t.Context()); return err }, func(s *ProcessSource, v map[string]json.RawMessage, e error) error {
			_, err := s.neighboursFromSummary(v, e)
			return err
		}},
		{"total_entries", func(s *ProcessSource) error { _, err := s.RouteEntries(t.Context()); return err }, func(_ *ProcessSource, v map[string]json.RawMessage, e error) error {
			_, err := routeEntriesFromSummary(v, e)
			return err
		}},
	}
	for _, reader := range readers {
		raw, err := json.Marshal(valid[reader.name])
		if err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			name, body string
			refusal    error
		}{
			{"missing", `{}`, ErrFamilyUnavailable},
			{"null", fmt.Sprintf(`{%q:null}`, reader.name), ErrFamilyUnavailable},
			{"duplicate", fmt.Sprintf(`{%q:%s,%q:%s}`, reader.name, raw, reader.name, raw), ErrInvalidSample},
			{"malformed", fmt.Sprintf(`{%q:"not-an-observation"}`, reader.name), ErrInvalidSample},
			{"wrong-family-case", fmt.Sprintf(`{%q:%s}`, strings.ToUpper(reader.name), raw), ErrFamilyUnavailable},
		}
		switch reader.name {
		case "transport_traffic":
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"alias", `{"transport_traffic":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","bytes_sent":1,"BYTES_SENT":1,"bytes_received":1}}`, ErrInvalidSample})
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"period", `{"transport_traffic":{"started_at":"2026-10-18T00:00:02Z","read_at":"2026-10-18T00:00:01Z","bytes_sent":1,"bytes_received":1}}`, ErrInvalidSample})
		case "session_outcomes":
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"overflow", `{"session_outcomes":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","attempts":18446744073709551615,"succeeded":18446744073709551615,"errors_connect":1,"errors_compat":0,"errors_other":0}}`, ErrInvalidSample})
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"count", `{"session_outcomes":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","attempts":1,"succeeded":0,"errors_connect":0,"errors_compat":0,"errors_other":0}}`, ErrInvalidSample})
		case "mode_selection":
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"future", `{"mode_selection":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:02Z","decisions":[]}}`, ErrInvalidSample})
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"alias", `{"mode_selection":{"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z","DECISIONS":[]}}`, ErrInvalidSample})
		case "neighbours":
			bad := neighbourFixture()
			bad["updated_at"] = adapterAt.Add(time.Second)
			future, marshalErr := json.Marshal(map[string]any{reader.name: bad})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"future", string(future), ErrInvalidSample})
			bad["updated_at"], bad["peers"] = adapterAt, 1
			count, marshalErr := json.Marshal(map[string]any{reader.name: bad})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"count", string(count), ErrInvalidSample})
		case "total_entries":
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"negative", `{"total_entries":-1}`, ErrInvalidSample})
			cases = append(cases, struct {
				name, body string
				refusal    error
			}{"overflow", `{"total_entries":9223372036854775808}`, ErrInvalidSample})
		}
		for _, test := range cases {
			t.Run(reader.name+"/"+test.name, func(t *testing.T) {
				var calls atomic.Uint64
				source := summaryTestSource(t, func(command string, w http.ResponseWriter) {
					if command != "fetchRouteSummary" {
						t.Error("unexpected command", command)
					}
					calls.Add(1)
					_, _ = fmt.Fprint(w, test.body)
				})
				if err := reader.public(source); !errors.Is(err, test.refusal) {
					t.Fatal("public decoder changed refusal", err)
				}
				summary, fetchErr := source.RouteSummary(t.Context())
				if err := reader.local(source, summary, fetchErr); !errors.Is(err, test.refusal) || calls.Load() != 2 {
					t.Fatal("local decoder changed refusal or fetched again", err, calls.Load())
				}
			})
		}
		fetchErr := fmt.Errorf("original authenticated fetch: %w", ErrRPCAccess)
		if err := reader.local(&ProcessSource{clock: adapterClock{adapterAt}}, nil, fetchErr); err != fetchErr {
			t.Fatal("shared fetch cause replaced", reader.name, err)
		}
	}
}

func TestProcessSummaryMalformedFamilyDoesNotFabricateOtherReadings(t *testing.T) {
	payload := summaryTestPayload(3, adapterAt.Add(-time.Second), adapterAt)
	payload["mode_selection"] = map[string]any{"started_at": adapterAt.Add(-time.Second), "read_at": adapterAt, "decisions": nil}
	var calls atomic.Uint64
	source := summaryTestSource(t, func(_ string, w http.ResponseWriter) { calls.Add(1); _ = json.NewEncoder(w).Encode(payload) })
	summary, fetchErr := source.RouteSummary(t.Context())
	traffic, trafficErr := transportTrafficFromSummary(summary, fetchErr)
	sessions, sessionErr := sessionOutcomesFromSummary(summary, fetchErr)
	_, modeErr := source.modeSelectionFromSummary(summary, fetchErr)
	neighbours, neighbourErr := source.neighboursFromSummary(summary, fetchErr)
	routes, routeErr := routeEntriesFromSummary(summary, fetchErr)
	if trafficErr != nil || sessionErr != nil || neighbourErr != nil || routeErr != nil || !errors.Is(modeErr, ErrFamilyUnavailable) || calls.Load() != 1 || traffic.BytesSent != 3 || sessions.Attempts != 3 || neighbours.Connections != 3 || routes != 3 {
		t.Fatal("one unavailable family contaminated or fabricated the others", trafficErr, sessionErr, modeErr, neighbourErr, routeErr)
	}
}
