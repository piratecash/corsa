package service

import (
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func (r *DMRouter) messageRefPeer(ref domain.MessageRef) (domain.PeerIdentity, error) {
	if r == nil || r.client == nil || ref.Topic != "dm" || ref.ID == "" || ref.Sender == "" || ref.Recipient == "" {
		return domain.PeerIdentity{}, fmt.Errorf("complete local DM reference is required")
	}
	self := r.client.Address().String()
	var other string
	switch {
	case ref.Sender == self:
		other = ref.Recipient
	case ref.Recipient == self:
		other = ref.Sender
	default:
		return domain.PeerIdentity{}, fmt.Errorf("message reference is outside this identity's conversation")
	}
	peer := domain.PeerIdentityFromWire(other)
	if peer.IsZero() || peer.String() != other {
		return domain.PeerIdentity{}, fmt.Errorf("message reference has an invalid counterparty")
	}
	return peer, nil
}

func directMessageRef(message DirectMessage) domain.MessageRef {
	return domain.MessageRef{Topic: "dm", ID: message.ID, Sender: message.Sender.String(), Recipient: message.Recipient.String()}
}

func localMessageRef(event protocol.LocalChangeEvent) domain.MessageRef {
	return domain.MessageRef{Topic: event.Topic, ID: event.MessageID, Sender: event.Sender, Recipient: event.Recipient}
}

func headerMessageRef(header DMHeader) domain.MessageRef {
	return domain.MessageRef{Topic: "dm", ID: header.ID, Sender: header.Sender.String(), Recipient: header.Recipient.String()}
}

func statusEventRef(event protocol.LocalChangeEvent) domain.MessageRef {
	ref := localMessageRef(event)
	if ref.Topic == "" {
		ref.Topic = "dm"
	}
	if event.Type != protocol.LocalChangeMessageEmitted && event.Status != protocol.ReceiptStatusSeenAck {
		ref.Sender, ref.Recipient = ref.Recipient, ref.Sender
	}
	return ref
}
