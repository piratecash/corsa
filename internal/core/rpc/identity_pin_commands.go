package rpc

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// IdentityPinRemovalPreviewDTO is the warning for one exact protection.
// ConfirmToken is a credential; UI must display the warning before using it.
type IdentityPinRemovalPreviewDTO struct {
	Identity           domain.PeerIdentity                  `json:"identity"`
	PinnedAt           time.Time                            `json:"pinned_at"`
	Consequence        string                               `json:"consequence"`
	ConsequenceVersion domain.IdentityPinConsequenceVersion `json:"consequence_version"`
	ConfirmToken       domain.IdentityPinConfirmationToken  `json:"confirm_token"`
	RemovalAvailable   bool                                 `json:"removal_available"`
}

// IdentityPinRemovalResultDTO reports persistence, never a speculative removal.
type IdentityPinRemovalResultDTO struct {
	OperationID domain.IdentityPinOperationID    `json:"operation_id"`
	Identity    domain.PeerIdentity              `json:"identity"`
	Outcome     domain.IdentityPinRemovalOutcome `json:"outcome"`
}

// RegisterIdentityPinCommands keeps distinct UI and HTTP entrypoints. Their
// handlers share domain behaviour; the transport gates run before dispatch.
func RegisterIdentityPinCommands(table *CommandTable, provider IdentityPinProvider) {
	for _, surface := range []struct {
		suffix string
		policy TransportPolicy
	}{
		{"", TransportLoopbackOnly}, {"UI", TransportInProcessOnly},
	} {
		preview := CommandInfo{Name: "previewIdentityPinRemoval" + surface.suffix, Description: "Preview one identity pin and its removal warning; never changes protection", Category: "secure_session", Usage: "identity=<40 lowercase hex>", Transport: surface.policy}
		confirm := CommandInfo{Name: "confirmIdentityPinRemoval" + surface.suffix, Description: "Confirm one preview; unavailable without verified durable storage and external restart fence", Category: "secure_session", Usage: "identity=<40 lowercase hex> confirm=<preview token>", Transport: surface.policy}
		if provider == nil {
			table.RegisterUnavailable(preview)
			table.RegisterUnavailable(confirm)
			continue
		}
		table.Register(preview, previewIdentityPinHandler(provider))
		table.Register(confirm, confirmIdentityPinHandler(provider))
	}
}

func previewIdentityPinHandler(provider IdentityPinProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		identity, err := identityPinIdentityArg(req.Args)
		if err != nil {
			return validationError(err)
		}
		preview, err := provider.PreviewIdentityPinRemoval(requestContext(req), identity)
		if err != nil {
			return identityPinFailure(err)
		}
		return jsonResponse(IdentityPinRemovalPreviewDTO{Identity: preview.Identity, PinnedAt: preview.PinnedAt, Consequence: preview.Consequence, ConsequenceVersion: preview.ConsequenceVersion, ConfirmToken: preview.Token, RemovalAvailable: preview.RemovalAvailable})
	}
}

func confirmIdentityPinHandler(provider IdentityPinProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		identity, err := identityPinIdentityArg(req.Args)
		if err != nil {
			return validationError(err)
		}
		token, err := identityPinConfirmationArg(req.Args)
		if err != nil {
			return validationError(err)
		}
		result, err := provider.ConfirmIdentityPinRemoval(requestContext(req), identity, token)
		if err != nil {
			return identityPinFailure(err)
		}
		return jsonResponse(IdentityPinRemovalResultDTO{OperationID: result.OperationID, Identity: result.Identity, Outcome: result.Outcome})
	}
}

func identityPinIdentityArg(args map[string]interface{}) (domain.PeerIdentity, error) {
	text, err := optionalStringArg(args, "identity")
	if err != nil {
		return domain.PeerIdentity{}, err
	}
	if text == nil {
		return domain.PeerIdentity{}, domain.ErrIdentityPinIdentityRequired
	}
	identity, err := domain.ParsePeerIdentity(*text)
	if err != nil {
		return domain.PeerIdentity{}, fmt.Errorf("identity: %w", err)
	}
	if identity.IsZero() {
		return domain.PeerIdentity{}, domain.ErrIdentityPinIdentityRequired
	}
	return identity, nil
}

func identityPinConfirmationArg(args map[string]interface{}) (domain.IdentityPinConfirmationToken, error) {
	token, err := optionalStringArg(args, "confirm")
	if err != nil {
		return "", err
	}
	if token == nil || len(*token) != 64 {
		return "", domain.ErrIdentityPinConfirmationStale
	}
	if _, err := hex.DecodeString(*token); err != nil {
		return "", domain.ErrIdentityPinConfirmationStale
	}
	return domain.IdentityPinConfirmationToken(*token), nil
}

func identityPinFailure(err error) CommandResponse {
	switch {
	case errors.Is(err, domain.ErrIdentityPinRemovalUnavailable), errors.Is(err, domain.ErrIdentityPinStoreUnavailable):
		return unavailableError(err)
	case errors.Is(err, domain.ErrIdentityPinIdentityRequired), errors.Is(err, domain.ErrIdentityPinNotFound), errors.Is(err, domain.ErrIdentityPinConfirmationStale):
		return validationError(err)
	default:
		return internalError(err)
	}
}
