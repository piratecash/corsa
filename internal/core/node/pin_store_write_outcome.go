package node

import (
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// pinFileWriteError preserves the existing visibility/confirmation boundary.
// Untyped errors (including private test writers) are never asserted to have
// preceded visibility. Windows retains its explicitly unverified legacy path.
type pinFileWriteError struct {
	commit secretfile.DurableCommit
	cause  error
}

func (e *pinFileWriteError) Error() string {
	return fmt.Sprintf("pin file confirmation %d: %v", e.commit, e.cause)
}
func (e *pinFileWriteError) Unwrap() error { return e.cause }

func pinFileCommit(err error) secretfile.DurableCommit {
	if err == nil {
		return secretfile.DurableCommitted
	}
	var failure *pinFileWriteError
	if errors.As(err, &failure) && failure != nil {
		switch failure.commit {
		case secretfile.DurableNotCommitted, secretfile.DurableCommitted, secretfile.DurableIndeterminate:
			return failure.commit
		}
	}
	return secretfile.DurableIndeterminate
}

// pinSnapshotWriteError describes the whole required main/marker sequence,
// not merely whichever low-level call last returned. Ordinary persist never
// acknowledges an error, even when its namespace is confirmed.
type pinSnapshotWriteError struct {
	outcome          domain.EndpointBindingActionOutcome
	causeCode        domain.EndpointBindingActionCause
	restoration      domain.EndpointBindingRestoration
	restorationCause domain.EndpointBindingActionCause
	cause            error
}

func (e *pinSnapshotWriteError) Error() string {
	return fmt.Sprintf("pin snapshot %s: %v", e.outcome, e.cause)
}
func (e *pinSnapshotWriteError) Unwrap() error { return e.cause }

func snapshotWriteFailure(outcome domain.EndpointBindingActionOutcome, cause error) *pinSnapshotWriteError {
	code := domain.EndpointBindingCauseWriteFailed
	if outcome == domain.EndpointBindingActionAppliedCleanupError {
		code = domain.EndpointBindingCauseCleanupFailed
	}
	return &pinSnapshotWriteError{outcome: outcome, causeCode: code,
		restoration: domain.EndpointBindingRestorationNotAttempted, restorationCause: domain.EndpointBindingCauseNone, cause: cause}
}

func endpointActionError(op domain.EndpointBindingOperationID, edits []bindingEdit, err error) error {
	var failure *pinSnapshotWriteError
	if len(edits) == 0 || !errors.As(err, &failure) || failure == nil {
		return err
	}
	endpoints := make([]domain.PeerAddress, len(edits))
	for i, edit := range edits {
		endpoints[i] = edit.key.address
	}
	return &domain.EndpointBindingActionError{OperationID: op, Endpoints: endpoints,
		Outcome: failure.outcome, Restoration: failure.restoration, CauseCode: failure.causeCode,
		RestorationCauseCode: failure.restorationCause, Cause: err}
}
