package desktop

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

func messageRef(message service.DirectMessage) domain.MessageRef {
	return domain.MessageRef{Topic: "dm", ID: message.ID, Sender: message.Sender.String(), Recipient: message.Recipient.String()}
}

func (w *Window) findCachedMsgRef(ref domain.MessageRef) (cachedMsg, bool) {
	cm, found := w.msgCacheByID[ref]
	return cm, found
}
