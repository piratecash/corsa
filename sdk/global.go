package sdk

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// GlobalPublication acknowledges this runtime's committed local row only.
// ClaimedSender is unsigned when replicated; Outcome never promises delivery.
type GlobalPublication struct {
	ID            string    `json:"id"`
	Topic         string    `json:"topic"`
	ClaimedSender string    `json:"claimed_sender"`
	Body          string    `json:"body"`
	CreatedAt     time.Time `json:"created_at"`
	Outcome       string    `json:"outcome"`
}

func (r *Runtime) publishGlobalMessage(ctx context.Context, command domain.OutgoingGlobalMessage) (domain.GlobalPublication, error) {
	if !r.beginOperation() {
		return domain.GlobalPublication{}, errClosed
	}
	defer r.endOperation()
	publishCtx, release := r.commandContext(ctx)
	defer release()
	return r.client.PublishGlobalMessage(publishCtx, command)
}

// PublishGlobalMessage canonicalizes the body and uses immutable/TTL0 global
// messages through the ordinary node store and bounded gossip attempt.
func (r *Runtime) PublishGlobalMessage(ctx context.Context, body string) (GlobalPublication, error) {
	result, err := r.publishGlobalMessage(ctx, domain.OutgoingGlobalMessage{Body: domain.GlobalMessageBody(body)})
	if err != nil {
		return GlobalPublication{}, err
	}
	return GlobalPublication{ID: string(result.Message.ID), Topic: string(result.Message.Topic), ClaimedSender: result.Message.ClaimedSender.String(), Body: string(result.Message.Body), CreatedAt: result.Message.CreatedAt, Outcome: string(result.Outcome)}, nil
}

type globalRPCPublisher struct{ runtime *Runtime }

func (p globalRPCPublisher) PublishGlobalMessage(ctx context.Context, command domain.OutgoingGlobalMessage) (domain.GlobalPublication, error) {
	return p.runtime.publishGlobalMessage(ctx, command)
}
