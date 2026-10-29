package rpc

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestEndpointActionDispositionPrecedesLegacyValidationMapping(t *testing.T) {
	action := &domain.EndpointBindingActionError{
		OperationID:          "exact-confirmed-operation",
		Endpoints:            []domain.PeerAddress{"192.0.2.221:64646"},
		Outcome:              domain.EndpointBindingActionIndeterminate,
		Restoration:          domain.EndpointBindingRestorationConfirmed,
		CauseCode:            domain.EndpointBindingCauseSelectionChanged,
		RestorationCauseCode: domain.EndpointBindingCauseNone,
		Cause:                domain.ErrEndpointBindingSelectionChanged,
	}
	for _, test := range []struct {
		name   string
		err    error
		status int
		typed  bool
	}{
		{"typed-postwrite-selection-changed", fmt.Errorf("private wrapper: %w", action), http.StatusInternalServerError, true},
		{"plain-precheck-selection-changed", domain.ErrEndpointBindingSelectionChanged, http.StatusBadRequest, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := endpointBindingFailure("demote endpoint bindings", test.err)
			if response.ErrorKind.HTTPStatus() != test.status || !errors.Is(response.Error, domain.ErrEndpointBindingSelectionChanged) || len(response.Data) != 0 {
				t.Fatalf("wrong mapped error: %+v", response)
			}
			details := endpointBindingActionDetails(response.Error)
			if test.typed {
				if details == nil || details.OperationID != action.OperationID || len(details.Endpoints) != 1 || details.Endpoints[0] != action.Endpoints[0] || details.Outcome != action.Outcome || details.Restoration != action.Restoration || details.CauseCode != action.CauseCode {
					t.Fatalf("typed disposition was flattened to validation: %+v", details)
				}
			} else if details != nil {
				t.Fatal("precheck refusal fabricated a postwrite disposition")
			}
		})
	}
}
