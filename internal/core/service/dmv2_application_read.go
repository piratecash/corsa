package service

import (
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// A live event still has the author's flag. The stored flag represents local
// deletion policy and must not be substituted for the signed wire value.
func openApplicationDM(id *identity.Identity, network string, ref domain.MessageRef, authorKey, body string, createdAt time.Time, liveFlag *string) (*directmsg.PlainMessage, error) {
	if network == "" || !strings.HasPrefix(body, directmsg.V2EnvelopePrefix) {
		return directmsg.DecryptForIdentity(id, ref.Sender, authorKey, ref.Recipient, body)
	}
	if createdAt.IsZero() {
		return nil, directmsg.ErrEnvelopeAuth
	}
	binding := directmsg.V2Binding{
		Network: network, Topic: ref.Topic, ID: ref.ID,
		Sender: ref.Sender, Recipient: ref.Recipient,
	}
	var plain *directmsg.PlainMessage
	var err error
	if liveFlag != nil {
		binding.Flag = *liveFlag
		plain, err = directmsg.OpenV2(id, binding, authorKey, body)
	} else {
		plain, err = directmsg.OpenStoredV2(id, binding, authorKey, body)
	}
	if err != nil {
		return nil, err
	}
	if !plain.CreatedAt.Equal(createdAt) {
		return nil, directmsg.ErrEnvelopeAuth
	}
	return plain, nil
}

func (d *DMCrypto) openPreviewDM(entry *chatlog.Entry, authorKey string) (*directmsg.PlainMessage, error) {
	var createdAt time.Time
	if d.dmV2ReadNetwork != "" && strings.HasPrefix(entry.Body, directmsg.V2EnvelopePrefix) {
		var err error
		createdAt, err = parseTimestamp(entry.CreatedAt)
		if err != nil {
			return nil, directmsg.ErrEnvelopeAuth
		}
	}
	ref := domain.MessageRef{Topic: "dm", ID: entry.ID, Sender: entry.Sender, Recipient: entry.Recipient}
	return openApplicationDM(d.id, d.dmV2ReadNetwork, ref, authorKey, entry.Body, createdAt, nil)
}
