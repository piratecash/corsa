package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/piratecash/corsa/internal/core/domain"
)

// EndpointBindingActionErrorDTO is a bounded, pathless operation receipt on an
// error response. The underlying local error is deliberately not serialized.
type EndpointBindingActionErrorDTO struct {
	OperationID          domain.EndpointBindingOperationID   `json:"operation_id"`
	Endpoints            []domain.PeerAddress                `json:"endpoints"`
	Outcome              domain.EndpointBindingActionOutcome `json:"outcome"`
	Restoration          domain.EndpointBindingRestoration   `json:"restoration"`
	CauseCode            domain.EndpointBindingActionCause   `json:"cause_code"`
	RestorationCauseCode domain.EndpointBindingActionCause   `json:"restoration_cause_code"`
}

// EndpointBindingRPCError survives typed client and console error paths. It
// prints the exact safe operation facts, never an untrusted outer message.
type EndpointBindingRPCError struct{ Details EndpointBindingActionErrorDTO }

func (e *EndpointBindingRPCError) Error() string {
	if e == nil || !validEndpointBindingActionDetails(e.Details) {
		return "invalid endpoint action error payload"
	}
	d := e.Details
	return fmt.Sprintf("endpoint action %s: operation %q; endpoints %q; cause %s; restoration %s (%s)", d.Outcome, d.OperationID, d.Endpoints, d.CauseCode, d.Restoration, d.RestorationCauseCode)
}

func endpointBindingActionDetails(err error) *EndpointBindingActionErrorDTO {
	var action *domain.EndpointBindingActionError
	if !errors.As(err, &action) {
		return nil
	}
	// A malformed local provider must not fall back to printing its outer
	// error (which might contain a local path). The empty sentinel is invalid
	// to a client, carries no leaked fields, and has a fixed safe message.
	if action == nil {
		return &EndpointBindingActionErrorDTO{}
	}
	d := &EndpointBindingActionErrorDTO{OperationID: action.OperationID,
		Endpoints: append([]domain.PeerAddress(nil), action.Endpoints...), Outcome: action.Outcome,
		Restoration: action.Restoration, CauseCode: action.CauseCode, RestorationCauseCode: action.RestorationCauseCode}
	if !validEndpointBindingActionDetails(*d) {
		return &EndpointBindingActionErrorDTO{}
	}
	return d
}

func validEndpointActionText(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, b := range []byte(value) {
		if b < 0x20 || b == 0x7f {
			return false
		}
	}
	return true
}

func validEndpointBindingActionDetails(d EndpointBindingActionErrorDTO) bool {
	if !validEndpointActionText(string(d.OperationID), 128) || len(d.Endpoints) == 0 || len(d.Endpoints) > domain.MaxEndpointBindingSelection {
		return false
	}
	seen := make(map[domain.PeerAddress]bool, len(d.Endpoints))
	for _, endpoint := range d.Endpoints {
		if !validEndpointActionText(string(endpoint), maxEndpointArgLength) || seen[endpoint] {
			return false
		}
		seen[endpoint] = true
	}
	cause := func(c domain.EndpointBindingActionCause) bool {
		switch c {
		case domain.EndpointBindingCauseNone, domain.EndpointBindingCauseValidationRefused,
			domain.EndpointBindingCauseSelectionChanged, domain.EndpointBindingCauseWriteFailed, domain.EndpointBindingCauseCleanupFailed:
			return true
		}
		return false
	}
	if !cause(d.CauseCode) || !cause(d.RestorationCauseCode) || d.CauseCode == domain.EndpointBindingCauseNone {
		return false
	}
	switch d.Restoration {
	case domain.EndpointBindingRestorationNotAttempted:
		if d.RestorationCauseCode != domain.EndpointBindingCauseNone {
			return false
		}
	case domain.EndpointBindingRestorationConfirmed:
		if d.RestorationCauseCode != domain.EndpointBindingCauseNone && d.RestorationCauseCode != domain.EndpointBindingCauseCleanupFailed {
			return false
		}
	case domain.EndpointBindingRestorationUnconfirmed:
		if d.RestorationCauseCode != domain.EndpointBindingCauseWriteFailed && d.RestorationCauseCode != domain.EndpointBindingCauseCleanupFailed {
			return false
		}
	default:
		return false
	}
	switch d.Outcome {
	case domain.EndpointBindingActionNotApplied:
		return d.Restoration == domain.EndpointBindingRestorationNotAttempted
	case domain.EndpointBindingActionIndeterminate:
		return true
	case domain.EndpointBindingActionAppliedCleanupError:
		return d.CauseCode == domain.EndpointBindingCauseCleanupFailed && d.Restoration == domain.EndpointBindingRestorationNotAttempted
	}
	return false // A committed success is never represented as an error.
}

// endpointActionObject parses one small closed object without duplicate keys.
// This is only the two fixed error-envelope shapes, not a JSON framework.
func endpointActionObject(raw []byte, limit int) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > limit {
		return nil, errors.New("invalid endpoint action error payload")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid endpoint action error payload")
	}
	out := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || out[name] != nil || len(out) >= 8 {
			return nil, errors.New("invalid endpoint action error payload")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, errors.New("invalid endpoint action error payload")
		}
		out[name] = value
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("invalid endpoint action error payload")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("invalid endpoint action error payload")
	}
	return out, nil
}

// nil,nil means an ordinary legacy error response with no endpoint_action;
// a present but malformed structured receipt is a protocol error, never a
// default-zero outcome or a silently flattened actionable error.
func decodeEndpointBindingActionError(raw []byte) (*EndpointBindingRPCError, error) {
	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) != nil {
		return nil, nil
	}
	if _, present := probe["endpoint_action"]; !present {
		return nil, nil
	}
	// Go's JSON decoder otherwise replaces invalid UTF-8 with U+FFFD,
	// which would change the exact operation/endpoint facts in a receipt.
	if !utf8.Valid(raw) {
		return nil, errors.New("invalid endpoint action error payload")
	}
	outer, err := endpointActionObject(raw, 1024*1024)
	if err != nil {
		return nil, err
	}
	var message string
	if json.Unmarshal(outer["error"], &message) != nil || message == "" {
		return nil, errors.New("invalid endpoint action error payload")
	}
	fields, err := endpointActionObject(outer["endpoint_action"], 1024*1024)
	if err != nil || len(fields) != 6 {
		return nil, errors.New("invalid endpoint action error payload")
	}
	for _, key := range []string{"operation_id", "endpoints", "outcome", "restoration", "cause_code", "restoration_cause_code"} {
		if _, present := fields[key]; !present {
			return nil, errors.New("invalid endpoint action error payload")
		}
	}
	var details EndpointBindingActionErrorDTO
	if json.Unmarshal(outer["endpoint_action"], &details) != nil || !validEndpointBindingActionDetails(details) {
		return nil, errors.New("invalid endpoint action error payload")
	}
	// These two exact fact fields are emitted canonically by the fixed
	// standard-JSON server. Alternate escapes are refused as well as lone
	// UTF-16 surrogates that encoding/json would normalize to U+FFFD.
	for name, value := range map[string]any{"operation_id": details.OperationID, "endpoints": details.Endpoints} {
		canonical, err := json.Marshal(value)
		var compact bytes.Buffer
		if err != nil || json.Compact(&compact, fields[name]) != nil || !bytes.Equal(compact.Bytes(), canonical) {
			return nil, errors.New("invalid endpoint action error payload")
		}
	}
	return &EndpointBindingRPCError{Details: details}, nil
}
