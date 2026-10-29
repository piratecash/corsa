package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/piratecash/corsa/internal/core/domain"
)

// These are transport fixtures, not endpoint mutation activation. The handler
// returns synthetic outcomes; no Service, endpoint engine or listener is run.
func endpointActionTransportDetails(outcome domain.EndpointBindingActionOutcome) EndpointBindingActionErrorDTO {
	d := EndpointBindingActionErrorDTO{
		OperationID: "operator-confirmed-action-01",
		Endpoints:   []domain.PeerAddress{"Example.Invalid:64646", "[2001:db8::1]:64646"},
		Outcome:     outcome, Restoration: domain.EndpointBindingRestorationNotAttempted,
		CauseCode:            domain.EndpointBindingCauseValidationRefused,
		RestorationCauseCode: domain.EndpointBindingCauseNone,
	}
	if outcome == domain.EndpointBindingActionIndeterminate {
		d.CauseCode = domain.EndpointBindingCauseWriteFailed
		d.Restoration = domain.EndpointBindingRestorationConfirmed
	}
	if outcome == domain.EndpointBindingActionAppliedCleanupError {
		d.CauseCode = domain.EndpointBindingCauseCleanupFailed
	}
	return d
}

func endpointActionTransportDomain(d EndpointBindingActionErrorDTO, cause error) *domain.EndpointBindingActionError {
	return &domain.EndpointBindingActionError{OperationID: d.OperationID,
		Endpoints: append([]domain.PeerAddress(nil), d.Endpoints...), Outcome: d.Outcome,
		Restoration: d.Restoration, CauseCode: d.CauseCode,
		RestorationCauseCode: d.RestorationCauseCode, Cause: cause}
}

func endpointActionTransportJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func endpointActionTransportEnvelope(t *testing.T, d EndpointBindingActionErrorDTO) []byte {
	t.Helper()
	return endpointActionTransportJSON(t, map[string]any{
		"error": "unsafe outer text /private/transport-wrapper/secret-key", "endpoint_action": d,
	})
}

func TestEndpointActionServerPreservesTypedErrorsOnEveryDispatch(t *testing.T) {
	paths := []struct {
		name, body string
		handler    func(*Server) fiber.Handler
	}{
		{"exec", `{"command":"ping","args":{"marker":"owned"}}`, func(s *Server) fiber.Handler { return s.handleExec }},
		{"frame", `{"type":"ping","marker":"owned"}`, func(s *Server) fiber.Handler { return s.handleFrame }},
		{"legacy", `{}`, func(s *Server) fiber.Handler { return s.legacyHandler("ping") }},
		{"legacy-args", `{"marker":"owned"}`, func(s *Server) fiber.Handler { return s.legacyArgHandler("ping") }},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			table := NewCommandTable()
			var answer CommandResponse
			calls := 0
			table.Register(CommandInfo{Name: "ping", Transport: TransportAnyAuthenticated}, func(req CommandRequest) CommandResponse {
				calls++
				if req.Ctx == nil {
					t.Error("dispatch lost request context")
				}
				if path.name != "legacy" && req.Args["marker"] != "owned" {
					t.Errorf("args changed: %v", req.Args)
				}
				return answer
			})
			app := fiber.New()
			s := &Server{app: app, table: table}
			app.Post("/owned", path.handler(s))
			request := func(wantStatus int) map[string]json.RawMessage {
				t.Helper()
				req := httptest.NewRequest(http.MethodPost, "http://owned.invalid/owned", strings.NewReader(path.body))
				req.Header.Set("Content-Type", "application/json")
				response, err := app.Test(req)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := response.Body.Close(); err != nil {
						t.Errorf("close response body: %v", err)
					}
				}()
				raw, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != wantStatus {
					t.Fatalf("status %d: %s", response.StatusCode, raw)
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Fatal(err)
				}
				return body
			}
			outcomes := []domain.EndpointBindingActionOutcome{
				domain.EndpointBindingActionNotApplied, domain.EndpointBindingActionIndeterminate, domain.EndpointBindingActionAppliedCleanupError,
			}
			for _, outcome := range outcomes {
				d := endpointActionTransportDetails(outcome)
				cause := errors.New("local secret /private/storage/pin-file.json")
				action := endpointActionTransportDomain(d, cause)
				wrapped := fmt.Errorf("wrapper /private/outer/private-key: %w", action)
				kind := ErrInternal
				if outcome == domain.EndpointBindingActionNotApplied {
					kind = ErrValidation
				}
				answer = CommandResponse{Error: wrapped, ErrorKind: kind}
				body := request(kind.HTTPStatus())
				if len(body) != 2 {
					t.Fatalf("typed error shape: %v", body)
				}
				var got EndpointBindingActionErrorDTO
				if err := json.Unmarshal(body["endpoint_action"], &got); err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(body["endpoint_action"], &fields); err != nil || len(fields) != 6 {
					t.Fatalf("six mandatory fields: %s", body["endpoint_action"])
				}
				if !reflect.DeepEqual(got, d) {
					t.Fatalf("receipt changed: %#v", got)
				}
				var message string
				if err := json.Unmarshal(body["error"], &message); err != nil {
					t.Fatal(err)
				}
				if message != (&EndpointBindingRPCError{Details: d}).Error() || strings.Contains(message, "/private/") {
					t.Fatalf("unsafe/noncanonical error: %q", message)
				}
				if !errors.Is(wrapped, cause) {
					t.Fatal("local cause no longer unwraps")
				}
			}
			for _, legacy := range []struct {
				kind    ErrorKind
				message string
			}{{ErrInternal, "ordinary failure"}, {ErrUnavailable, "mutation unavailable"}} {
				answer = CommandResponse{Error: errors.New(legacy.message), ErrorKind: legacy.kind}
				body := request(legacy.kind.HTTPStatus())
				if len(body) != 1 || string(body["error"]) != string(endpointActionTransportJSON(t, legacy.message)) {
					t.Fatalf("legacy error changed: %v", body)
				}
			}
			answer = CommandResponse{Data: json.RawMessage(`{"ok":true}`)}
			body := request(http.StatusOK)
			if len(body) != 1 || string(body["ok"]) != "true" {
				t.Fatalf("success changed: %v", body)
			}
			if calls != 6 {
				t.Fatalf("handler calls %d, want 6", calls)
			}
		})
	}
}

type endpointActionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f endpointActionRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestEndpointActionClientsKeepTypedDetailsAndLegacyBehavior(t *testing.T) {
	variants := []struct {
		name, path, key string
		call            func(*Client) (any, error)
	}{
		{"execute-named", "/rpc/v1/exec", "command", func(c *Client) (any, error) { return c.ExecuteCommand("ping") }},
		{"execute-frame", "/rpc/v1/frame", "type", func(c *Client) (any, error) { return c.ExecuteCommand(`{"type":"ping"}`) }},
		{"exec", "/rpc/v1/exec", "command", func(c *Client) (any, error) { return c.exec("ping", nil) }},
		{"execInto", "/rpc/v1/exec", "command", func(c *Client) (any, error) {
			var out map[string]any
			err := c.execInto("ping", nil, &out)
			return out, err
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			var raw []byte
			status, calls := http.StatusInternalServerError, 0
			client := &Client{baseURL: "http://owned.invalid", httpClient: &http.Client{Transport: endpointActionRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.Method != http.MethodPost || req.URL.Path != variant.path {
					t.Fatalf("request changed: %s %s", req.Method, req.URL.Path)
				}
				defer func() {
					if err := req.Body.Close(); err != nil {
						t.Errorf("close request body: %v", err)
					}
				}()
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body[variant.key] != "ping" {
					t.Fatalf("request body: %v (%v)", body, err)
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
			})}}
			for _, outcome := range []domain.EndpointBindingActionOutcome{domain.EndpointBindingActionNotApplied, domain.EndpointBindingActionIndeterminate, domain.EndpointBindingActionAppliedCleanupError} {
				d := endpointActionTransportDetails(outcome)
				raw = endpointActionTransportEnvelope(t, d)
				_, err := variant.call(client)
				var typed *EndpointBindingRPCError
				if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Details, d) {
					t.Fatalf("typed receipt lost: %#v (%v)", typed, err)
				}
				if strings.Contains(err.Error(), "/private/") {
					t.Fatalf("unsafe outer text: %v", err)
				}
			}
			// A bounded, unknown JSON extension need not fit float64. Decoding
			// the generic map first used to lose this otherwise valid receipt.
			d := endpointActionTransportDetails(domain.EndpointBindingActionIndeterminate)
			raw = append(endpointActionTransportEnvelope(t, d)[:len(endpointActionTransportEnvelope(t, d))-1], []byte(`,"extension":1e400}`)...)
			_, err := variant.call(client)
			var typed *EndpointBindingRPCError
			if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Details, d) {
				t.Fatalf("numeric extension lost receipt: %v", err)
			}
			for _, invalid := range [][]byte{[]byte(`{"error":"/private/untrusted","endpoint_action":null}`), []byte(`{"error":"/private/untrusted","endpoint_action":{"outcome":"indeterminate"}}`)} {
				raw = invalid
				_, err := variant.call(client)
				typed = nil
				if err == nil || errors.As(err, &typed) || strings.Contains(err.Error(), "/private/") {
					t.Fatalf("malformed receipt flattened: %v", err)
				}
			}
			// Raw invalid UTF-8 must not be silently changed into a different
			// operation/selection by encoding/json's replacement behaviour.
			raw = bytes.Replace(endpointActionTransportEnvelope(t, d), []byte("Example"), []byte{'E', 0xff}, 1)
			_, err = variant.call(client)
			typed = nil
			if err == nil || errors.As(err, &typed) || err.Error() != "invalid endpoint action error payload" {
				t.Fatalf("invalid-byte receipt accepted: %v", err)
			}
			raw = bytes.Replace(endpointActionTransportEnvelope(t, d), []byte("Example.Invalid:64646"), []byte(`\ud800`), 1)
			_, err = variant.call(client)
			typed = nil
			if err == nil || errors.As(err, &typed) || err.Error() != "invalid endpoint action error payload" {
				t.Fatalf("lone-surrogate receipt normalized into a selection: %v", err)
			}
			raw = []byte(`{"error":"legacy failure"}`)
			if _, err := variant.call(client); err == nil || err.Error() != "rpc error: legacy failure" {
				t.Fatalf("legacy message changed: %v", err)
			}
			raw = []byte(`{"other":true}`)
			if _, err := variant.call(client); err == nil || err.Error() != "rpc error: status 500" {
				t.Fatalf("legacy status fallback changed: %v", err)
			}
			// An absent receipt with the same extension keeps each method's
			// original generic behaviour, including exec's parse refusal.
			raw = []byte(`{"error":"legacy failure","extension":1e400}`)
			_, err = variant.call(client)
			if variant.name == "exec" {
				if err == nil || !strings.HasPrefix(err.Error(), "parse response:") {
					t.Fatalf("legacy generic parse changed: %v", err)
				}
			} else if err == nil || err.Error() != "rpc error: status 500" {
				t.Fatalf("legacy numeric extension changed: %v", err)
			}
			raw = []byte("{\"error\":\"legacy\xfffailure\"}")
			if _, err := variant.call(client); err == nil || err.Error() != "rpc error: legacy\ufffdfailure" {
				t.Fatalf("absent-receipt UTF-8 legacy behaviour changed: %v", err)
			}
			status = http.StatusOK
			raw = []byte(`{"ok":true,"endpoint_action":null}`)
			value, err := variant.call(client)
			if err != nil {
				t.Fatalf("success became an action error: %v", err)
			}
			if text, ok := value.(string); ok {
				var parsed map[string]any
				if err := json.Unmarshal([]byte(text), &parsed); err != nil {
					t.Fatal(err)
				}
				value = parsed
			}
			if !reflect.DeepEqual(value, map[string]any{"ok": true, "endpoint_action": nil}) {
				t.Fatalf("success changed: %#v", value)
			}
			if calls != 13 {
				t.Fatalf("request count %d, want 13", calls)
			}
		})
	}
}

func TestEndpointActionMalformedDetailsCannotBecomeLegacyError(t *testing.T) {
	d := endpointActionTransportDetails(domain.EndpointBindingActionIndeterminate)
	var base map[string]any
	if err := json.Unmarshal(endpointActionTransportJSON(t, d), &base); err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		name string
		raw  []byte
	}
	for _, key := range []string{"operation_id", "endpoints", "outcome", "restoration", "cause_code", "restoration_cause_code"} {
		fields := make(map[string]any, len(base))
		for k, v := range base {
			if k != key {
				fields[k] = v
			}
		}
		cases = append(cases, struct {
			name string
			raw  []byte
		}{"missing-" + key, endpointActionTransportJSON(t, map[string]any{"error": "/private/hidden", "endpoint_action": fields})})
	}
	changes := []struct {
		name, key string
		value     any
	}{
		{"committed-error", "outcome", "committed"}, {"unknown-outcome", "outcome", "unknown"},
		{"unknown-restoration", "restoration", "unknown"}, {"unknown-cause", "cause_code", "unknown"},
		{"unknown-restoration-cause", "restoration_cause_code", "unknown"}, {"duplicate-selection", "endpoints", []string{"raw:01", "raw:01"}},
		{"empty-selection", "endpoints", []string{}}, {"empty-endpoint", "endpoints", []string{""}},
		{"control-endpoint", "endpoints", []string{"peer\nsecret"}}, {"oversize-endpoint", "endpoints", []string{strings.Repeat("e", 513)}},
		{"control-operation", "operation_id", "operation\nsecret"}, {"oversize-operation", "operation_id", strings.Repeat("o", 129)},
		{"wrong-type", "operation_id", 7}, {"null-field", "restoration_cause_code", nil},
	}
	for _, change := range changes {
		fields := make(map[string]any, len(base))
		for k, v := range base {
			fields[k] = v
		}
		fields[change.key] = change.value
		cases = append(cases, struct {
			name string
			raw  []byte
		}{change.name, endpointActionTransportJSON(t, map[string]any{"error": "/private/hidden", "endpoint_action": fields})})
	}
	valid := endpointActionTransportJSON(t, d)
	duplicate := append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"operation_id":"duplicate"}`)...)
	cases = append(cases, struct {
		name string
		raw  []byte
	}{"duplicate-field", append(append([]byte(`{"error":"/private/hidden","endpoint_action":`), duplicate...), '}')})
	cases = append(cases, struct {
		name string
		raw  []byte
	}{"invalid-raw-utf8", bytes.Replace(endpointActionTransportEnvelope(t, d), []byte("Example"), []byte{'E', 0xff}, 1)})
	for _, spelling := range []struct{ name, original, replacement string }{
		{"lone-surrogate-operation", "operator-confirmed-action-01", `op\ud800`},
		{"lone-surrogate-endpoint", "Example.Invalid:64646", `\ud800`},
		{"alternate-ascii-escape", "Example.Invalid:64646", `\u0045xample.Invalid:64646`},
	} {
		cases = append(cases, struct {
			name string
			raw  []byte
		}{spelling.name,
			bytes.Replace(endpointActionTransportEnvelope(t, d), []byte(spelling.original), []byte(spelling.replacement), 1)})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			action, err := decodeEndpointBindingActionError(test.raw)
			if action != nil || err == nil || err.Error() != "invalid endpoint action error payload" {
				t.Fatalf("malformed receipt accepted/echoed: %#v (%v)", action, err)
			}
		})
	}
	// Boundary values are accepted as exact owned strings, without endpoint
	// canonicalization or silent truncation by the transport.
	d.OperationID = domain.EndpointBindingOperationID(strings.Repeat("o", 128))
	d.Endpoints = []domain.PeerAddress{domain.PeerAddress(strings.Repeat("e", 512))}
	action, err := decodeEndpointBindingActionError(endpointActionTransportEnvelope(t, d))
	if err != nil || action == nil || !reflect.DeepEqual(action.Details, d) {
		t.Fatalf("valid boundary rejected: %#v %v", action, err)
	}
	d.OperationID = "operation-\ufffd"
	d.Endpoints = []domain.PeerAddress{"retained-\ufffd:64646"}
	action, err = decodeEndpointBindingActionError(endpointActionTransportEnvelope(t, d))
	if err != nil || action == nil || !reflect.DeepEqual(action.Details, d) {
		t.Fatalf("genuine canonical replacement character changed: %#v %v", action, err)
	}
	if action, err := decodeEndpointBindingActionError([]byte(`{"error":"legacy"}`)); action != nil || err != nil {
		t.Fatalf("absent receipt changed: %#v %v", action, err)
	}
}

func TestEndpointActionDetailsOwnTheirSelection(t *testing.T) {
	d := endpointActionTransportDetails(domain.EndpointBindingActionIndeterminate)
	cause := errors.New("/private/local/cause-token")
	action := endpointActionTransportDomain(d, cause)
	wrapped := fmt.Errorf("/private/local/outer-wrapper: %w", action)
	got := endpointBindingActionDetails(wrapped)
	if got == nil || !reflect.DeepEqual(*got, d) || !errors.Is(wrapped, cause) {
		t.Fatalf("wrapped details/cause lost: %#v", got)
	}
	encodedDomain := endpointActionTransportJSON(t, action)
	if bytes.Contains(encodedDomain, []byte("Cause\"")) || bytes.Contains(encodedDomain, []byte("/private/")) {
		t.Fatalf("local domain cause was serialized: %s", encodedDomain)
	}
	action.Endpoints[0] = "changed.invalid:1"
	if !reflect.DeepEqual(*got, d) {
		t.Fatal("DTO aliases source selection")
	}
	decoded, err := decodeEndpointBindingActionError(endpointActionTransportEnvelope(t, *got))
	if err != nil || decoded == nil {
		t.Fatal(err)
	}
	got.Endpoints[0] = "changed.invalid:2"
	if !reflect.DeepEqual(decoded.Details, d) || strings.Contains(decoded.Error(), "/private/") {
		t.Fatal("decoded selection changed or exposed local cause")
	}
	// An invalid local typed provider must still take the safe typed branch,
	// rather than falling back to an outer filesystem error string.
	action.OperationID = ""
	invalid := endpointBindingActionDetails(wrapped)
	if invalid == nil || (&EndpointBindingRPCError{Details: *invalid}).Error() != "invalid endpoint action error payload" {
		t.Fatal("invalid local provider escaped safe sentinel")
	}
	if endpointBindingActionDetails(errors.New("ordinary")) != nil {
		t.Fatal("ordinary error became typed")
	}
}
