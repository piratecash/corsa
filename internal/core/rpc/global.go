package rpc

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

type GlobalPublisherProvider interface {
	PublishGlobalMessage(context.Context, domain.OutgoingGlobalMessage) (domain.GlobalPublication, error)
}

// RegisterGlobalPublisherCommand exposes a business publisher, never raw frames.
func RegisterGlobalPublisherCommand(t *CommandTable, publisher GlobalPublisherProvider) {
	info := CommandInfo{Name: "publishGlobal", Description: "Store an own unsigned global publication and attempt propagation", Category: "message", Usage: "<body>"}
	if publisher == nil {
		t.RegisterUnavailable(info)
		return
	}
	t.Register(info, func(req CommandRequest) CommandResponse {
		if response, done := ctxDone(req); done {
			return response
		}
		if len(req.Args) != 1 {
			return validationError(domain.ErrInvalidGlobalMessage)
		}
		text, ok := req.Args["body"].(string)
		if !ok {
			return validationError(domain.ErrInvalidGlobalMessage)
		}
		body, err := domain.CanonicalGlobalMessageBody(text)
		if err != nil {
			return validationError(err)
		}
		result, err := publisher.PublishGlobalMessage(commandContext(req), domain.OutgoingGlobalMessage{Body: body})
		if errors.Is(err, domain.ErrInvalidGlobalMessage) {
			return validationError(err)
		}
		if errors.Is(err, domain.ErrGlobalPublisherUnavailable) {
			return unavailableError(err)
		}
		if err != nil {
			return internalError(err)
		}
		return jsonResponse(result)
	})
}
