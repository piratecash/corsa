package service

import (
	"context"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Unused private composition. Public NewDMCrypto remains zero-origin. The
// observer is synchronous and receives only a genuine opaque receipt after
// node Submit releases every owner lock; it installs no asynchronous consumer.
func newDMCryptoWithOwnDMProvenance(rpc *LocalRPCClient, chatlog *ChatlogGateway, id *identity.Identity, port node.OwnDMProvenancePort, observer func(node.OwnDMSubmission)) (*DMCrypto, error) {
	if rpc == nil || !port.ValidFor(rpc.LocalNode(), id) {
		return nil, fmt.Errorf("own DM provenance owner mismatch")
	}
	d := NewDMCrypto(rpc, chatlog, id)
	d.ownDMPort = port
	d.ownDMObserver = observer
	return d, nil
}

func (d *DMCrypto) submitOwnDM(ctx, residence context.Context, frame protocol.Frame, createdAt time.Time) (protocol.Frame, error) {
	if !d.ownDMPort.ValidFor(d.rpc.LocalNode(), d.id) {
		return d.rpc.LocalRequestFrameCtx(ctx, frame)
	}
	input := protocol.Envelope{ID: protocol.MessageID(frame.ID), Sender: frame.Address, Recipient: frame.Recipient, Topic: frame.Topic, Flag: protocol.MessageFlag(frame.Flag), CreatedAt: createdAt, TTLSeconds: frame.TTLSeconds, Payload: []byte(frame.Body)}
	reply, receipt, err := d.ownDMPort.SubmitWithResidence(ctx, residence, input)
	if err != nil {
		return protocol.Frame{}, err
	}
	if d.ownDMObserver != nil {
		d.ownDMObserver(receipt)
	}
	if reply.Type == "error" {
		if reply.Code != "" {
			return protocol.Frame{}, protocol.ErrorFromCode(reply.Code)
		}
		if reply.Error != "" {
			return protocol.Frame{}, fmt.Errorf("%s", reply.Error)
		}
		return protocol.Frame{}, protocol.ErrProtocol
	}
	return reply, nil
}
