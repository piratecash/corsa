package sdk

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// DirectMessage is the SDK-safe view of a decrypted direct message.
type DirectMessage struct {
	ID            string     `json:"id"`
	Sender        string     `json:"sender"`
	Recipient     string     `json:"recipient"`
	Body          string     `json:"body"`
	ReplyTo       string     `json:"reply_to,omitempty"`
	Command       string     `json:"command,omitempty"`
	CommandData   string     `json:"command_data,omitempty"`
	Timestamp     time.Time  `json:"timestamp"`
	ReceiptStatus string     `json:"receipt_status,omitempty"`
	DeliveredAt   *time.Time `json:"delivered_at,omitempty"`
}

func fromInternalMessage(msg *service.DirectMessage) DirectMessage {
	return DirectMessage{
		ID:            msg.ID,
		Sender:        string(msg.Sender),
		Recipient:     string(msg.Recipient),
		Body:          msg.Body,
		ReplyTo:       string(msg.ReplyTo),
		Command:       string(msg.Command),
		CommandData:   msg.CommandData,
		Timestamp:     msg.Timestamp,
		ReceiptStatus: msg.ReceiptStatus,
		DeliveredAt:   msg.DeliveredAt.Ptr(),
	}
}

// SubscribeDirectMessages streams decrypted incoming direct messages.
func (r *Runtime) SubscribeDirectMessages(ctx context.Context) <-chan DirectMessage {
	events, cancel := r.client.SubscribeLocalChanges()
	out := make(chan DirectMessage, 16)

	go func() {
		defer cancel()
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				msg := r.client.DecryptIncomingMessage(event)
				if msg == nil || string(msg.Sender) == r.Address() {
					continue
				}

				select {
				case out <- fromInternalMessage(msg):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

// SendDirectMessage sends a direct message using the same delivery stack as the desktop client.
func (r *Runtime) SendDirectMessage(ctx context.Context, to, body string) (*DirectMessage, error) {
	msg, err := r.client.SendDirectMessage(ctx, domain.PeerIdentity(to), domain.OutgoingDM{
		Body: body,
	})
	if err != nil {
		return nil, err
	}
	result := fromInternalMessage(msg)
	return &result, nil
}
