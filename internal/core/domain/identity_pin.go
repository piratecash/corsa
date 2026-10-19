package domain

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// IdentityPinConfirmationToken confirms one preview in one running store.
// It is a credential: callers must not log it or reuse it after a new proof.
type IdentityPinConfirmationToken string

// Format prevents incidental diagnostic formatting from logging the credential.
// Explicit conversion and the intended JSON confirmation field retain its value.
func (token IdentityPinConfirmationToken) Format(state fmt.State, verb rune) {
	// Formatting cannot report a destination error to its caller.
	_, _ = io.WriteString(state, "[redacted identity pin token]")
}

// IdentityPinConsequenceVersion changes when the operator's warning changes.
type IdentityPinConsequenceVersion uint32

const CurrentIdentityPinConsequenceVersion IdentityPinConsequenceVersion = 2

// IdentityPinOperationID names one attempted operator removal in its audit.
type IdentityPinOperationID string

// IdentityPinRemovalOutcome describes the confirmed persistence boundary.
type IdentityPinRemovalOutcome string

const (
	IdentityPinRemovalNotCommitted     IdentityPinRemovalOutcome = "not_committed"
	IdentityPinRemovalCommitted        IdentityPinRemovalOutcome = "committed"
	IdentityPinRemovalIndeterminate    IdentityPinRemovalOutcome = "indeterminate_recovery_required"
	IdentityPinRemovalRecoveryRequired IdentityPinRemovalOutcome = "recovery_required"
)

// IdentityPinRemovalConsequence is shown before the operator confirms.
const IdentityPinRemovalConsequence = "Remove this identity pin, one of the grounds for refusing downgrade. Endpoint bindings and all other requirements to use v2 remain. A later successful v2 proof pins this identity again. The inactive candidate workflow requires a node stop and permits only one removal before external operator finalize. A receipt alone never authorizes network restart."

// IdentityPinRemovalPreview identifies exactly one protection, without mutation.
type IdentityPinRemovalPreview struct {
	Identity           PeerIdentity
	PinnedAt           time.Time
	Consequence        string
	ConsequenceVersion IdentityPinConsequenceVersion
	Token              IdentityPinConfirmationToken
	RemovalAvailable   bool
}

// IdentityPinRemovalResult is the actual outcome of one confirmed operation.
// Committed confirms local pin persistence only. It does not finish the
// candidate stop/seal/external-finalize workflow or authorize network restart.
type IdentityPinRemovalResult struct {
	OperationID IdentityPinOperationID
	Identity    PeerIdentity
	Outcome     IdentityPinRemovalOutcome
}

var (
	ErrIdentityPinIdentityRequired    = errors.New("identity_pin_identity_required")
	ErrIdentityPinNotFound            = errors.New("identity_pin_not_found")
	ErrIdentityPinStoreUnavailable    = errors.New("identity_pin_store_unavailable")
	ErrIdentityPinConfirmationStale   = errors.New("identity_pin_confirmation_stale: obtain a fresh preview")
	ErrIdentityPinRemovalUnavailable  = errors.New("identity_pin_removal_unavailable: verified durable storage and external restart fence required")
	ErrIdentityPinRecoveryRequired    = errors.New("identity_pin_recovery_required: the pin remains active in this process; on-disk state is unconfirmed; trusted recovery is required before restarting networking")
	ErrIdentityPinRecoveryOfflineOnly = errors.New("identity_pin_recovery_offline_only: recovery processes never start networking")
	ErrIdentityPinSupervisionRequired = errors.New("identity_pin_supervision_required: enrolled store requires verified supervised startup")
	ErrIdentityPinGuardInvalid        = errors.New("identity_pin_guard_invalid")
	ErrIdentityPinFenceMismatch       = errors.New("identity_pin_fence_mismatch")
	ErrIdentityPinRevisionExhausted   = errors.New("identity_pin_revision_exhausted")
)
