package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// IdentityPinConfirmationToken is an opaque credential from one preview.
// Formatting redacts it; only pass it after displaying the exact warning.
type IdentityPinConfirmationToken = domain.IdentityPinConfirmationToken

// IdentityPinConsequenceVersion identifies the warning the operator confirmed.
type IdentityPinConsequenceVersion = domain.IdentityPinConsequenceVersion

// IdentityPinOperationID names one operator action in its audit.
type IdentityPinOperationID = domain.IdentityPinOperationID

// IdentityPinRemovalOutcome reports the actual durable transaction outcome.
type IdentityPinRemovalOutcome = domain.IdentityPinRemovalOutcome

const (
	IdentityPinRemovalNotCommitted     = domain.IdentityPinRemovalNotCommitted
	IdentityPinRemovalCommitted        = domain.IdentityPinRemovalCommitted
	IdentityPinRemovalIndeterminate    = domain.IdentityPinRemovalIndeterminate
	IdentityPinRemovalRecoveryRequired = domain.IdentityPinRemovalRecoveryRequired
)

// IdentityPinRemovalPreview is a read-only view for an operator confirmation.
// RemovalAvailable is false without verified durable storage and a restart fence.
type IdentityPinRemovalPreview struct {
	Identity           string                        `json:"identity"`
	PinnedAt           time.Time                     `json:"pinned_at"`
	Consequence        string                        `json:"consequence"`
	ConsequenceVersion IdentityPinConsequenceVersion `json:"consequence_version"`
	ConfirmToken       IdentityPinConfirmationToken  `json:"confirm_token"`
	RemovalAvailable   bool                          `json:"removal_available"`
}

// IdentityPinRemovalResult reports only the named pin; endpoint bindings remain.
type IdentityPinRemovalResult struct {
	OperationID IdentityPinOperationID    `json:"operation_id"`
	Identity    string                    `json:"identity"`
	Outcome     IdentityPinRemovalOutcome `json:"outcome"`
}

// PreviewIdentityPinRemoval uses the in-process UI entrypoint. Show Identity
// and Consequence before requesting a separate explicit operator confirmation.
func (r *Runtime) PreviewIdentityPinRemoval(ctx context.Context, identity string) (IdentityPinRemovalPreview, error) {
	raw, err := r.ExecuteContext(ctx, "previewIdentityPinRemovalUI", map[string]interface{}{"identity": identity})
	if err != nil {
		return IdentityPinRemovalPreview{}, err
	}
	var preview IdentityPinRemovalPreview
	if err := json.Unmarshal(raw, &preview); err != nil {
		return IdentityPinRemovalPreview{}, fmt.Errorf("decode identity pin preview: %w", err)
	}
	return preview, nil
}

// ConfirmIdentityPinRemoval is a distinct operator action after a preview.
// It stays unavailable until the node can enforce durable recovery across restart.
func (r *Runtime) ConfirmIdentityPinRemoval(ctx context.Context, identity string, token IdentityPinConfirmationToken) (IdentityPinRemovalResult, error) {
	raw, err := r.ExecuteContext(ctx, "confirmIdentityPinRemovalUI", map[string]interface{}{"identity": identity, "confirm": string(token)})
	if err != nil {
		return IdentityPinRemovalResult{}, err
	}
	var result IdentityPinRemovalResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return IdentityPinRemovalResult{}, fmt.Errorf("decode identity pin removal: %w", err)
	}
	return result, nil
}

// These errors retain their identity across the SDK's in-process boundary.
var (
	ErrIdentityPinRemovalUnavailable = domain.ErrIdentityPinRemovalUnavailable
	ErrIdentityPinConfirmationStale  = domain.ErrIdentityPinConfirmationStale
	ErrIdentityPinNotFound           = domain.ErrIdentityPinNotFound
	ErrIdentityPinStoreUnavailable   = domain.ErrIdentityPinStoreUnavailable
)
