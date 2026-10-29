package domain

import "fmt"

// These values describe one exact operator-confirmed endpoint action, never
// permission to enable an entrypoint or to remove an identity pin.
type EndpointBindingActionOutcome string

const (
	EndpointBindingActionNotApplied          EndpointBindingActionOutcome = "not_applied"
	EndpointBindingActionIndeterminate       EndpointBindingActionOutcome = "indeterminate"
	EndpointBindingActionAppliedCleanupError EndpointBindingActionOutcome = "applied_cleanup_error"
	EndpointBindingActionCommitted           EndpointBindingActionOutcome = "committed"
)

type EndpointBindingRestoration string

const (
	EndpointBindingRestorationNotAttempted EndpointBindingRestoration = "not_attempted"
	EndpointBindingRestorationConfirmed    EndpointBindingRestoration = "confirmed"
	EndpointBindingRestorationUnconfirmed  EndpointBindingRestoration = "unconfirmed"
)

type EndpointBindingActionCause string

const (
	EndpointBindingCauseNone              EndpointBindingActionCause = "none"
	EndpointBindingCauseValidationRefused EndpointBindingActionCause = "validation_refused"
	EndpointBindingCauseSelectionChanged  EndpointBindingActionCause = "selection_changed"
	EndpointBindingCauseWriteFailed       EndpointBindingActionCause = "write_failed"
	EndpointBindingCauseCleanupFailed     EndpointBindingActionCause = "cleanup_failed"
)

// EndpointBindingActionError retains safe operation facts despite a nonnil
// error. Cause is local-only: Error deliberately does not print it; Unwrap
// preserves errors.Is/As without exposing local filesystem paths to RPC.
type EndpointBindingActionError struct {
	OperationID          EndpointBindingOperationID
	Endpoints            []PeerAddress
	Outcome              EndpointBindingActionOutcome
	Restoration          EndpointBindingRestoration
	CauseCode            EndpointBindingActionCause
	RestorationCauseCode EndpointBindingActionCause
	Cause                error `json:"-"`
}

func (e *EndpointBindingActionError) Error() string {
	if e == nil {
		return "invalid endpoint action error"
	}
	return fmt.Sprintf("endpoint action %s: operation %q; endpoints %q; cause %s; restoration %s (%s)", e.Outcome, e.OperationID, e.Endpoints, e.CauseCode, e.Restoration, e.RestorationCauseCode)
}

func (e *EndpointBindingActionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
