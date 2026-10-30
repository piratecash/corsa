package desktop

import (
	"testing"
	"time"

	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestMessageRefCacheSeparatesOppositeDirections(t *testing.T) {
	self, peer := domaintest.ID("ui-ref-self"), domaintest.ID("ui-ref-peer")
	a := service.DirectMessage{ID: "shared", Sender: self, Recipient: peer, Body: "outgoing"}
	b := service.DirectMessage{ID: "shared", Sender: peer, Recipient: self, Body: "incoming"}
	w := &Window{snap: service.RouterSnapshot{ActivePeer: peer, MyAddress: self, CacheReady: true, DMGeneration: 1, ActiveMessages: []service.DirectMessage{a, b}}}
	w.rebuildMsgCache()
	if len(w.msgCacheByID) != 2 {
		t.Fatal("same-ID rows share cache metadata")
	}
	for i, msg := range []service.DirectMessage{a, b} {
		cm, ok := w.findCachedMsgRef(messageRef(msg))
		if !ok || cm.Body != msg.Body || cm.Index != i {
			t.Fatalf("wrong cached row: %+v", cm)
		}
	}
	if _, ok := w.findCachedMsg("shared"); ok {
		t.Fatal("wire-only quote lookup chose a direction")
	}
	w.messageSelectables = map[domain.MessageRef]*widget.Selectable{}
	if w.messageSelectable(messageRef(a)) == w.messageSelectable(messageRef(b)) {
		t.Fatal("same-ID rows share text selection widget")
	}
	w.replyToMsg = &a
	w.msgContextMsg = &a
	w.snap.ActiveMessages = []service.DirectMessage{b}
	w.snap.DMGeneration++
	w.rebuildMsgCache()
	w.dropStaleReply()
	w.dropStaleMsgMenu()
	if w.replyToMsg != nil || w.msgContextMsg != nil {
		t.Fatal("surviving opposite direction kept deleted row actions alive")
	}
	if cm, ok := w.findCachedMsg("shared"); !ok || cm.Body != b.Body {
		t.Fatal("remaining unambiguous wire reference was not restored")
	}
}

func TestViewerAndHighlightRetainExactMessageRef(t *testing.T) {
	a := domain.MessageRef{Topic: "dm", ID: "shared", Sender: "a", Recipient: "b"}
	b := domain.MessageRef{Topic: "dm", ID: "shared", Sender: "b", Recipient: "a"}
	items := []viewerItem{{messageID: b}, {messageID: a}}
	if got := viewerIndexAfterRebuild(a, items, 0); got != 1 {
		t.Fatalf("viewer jumped to same-ID other direction: %d", got)
	}
	var h msgHighlight
	now := time.Unix(123, 0)
	h.start(a, now)
	h.tick(now.Add(msgHighlightRise))
	if h.levelFor(a) == 0 || h.levelFor(b) != 0 {
		t.Fatal("highlight leaked to same-ID other direction")
	}
	buttons := map[domain.MessageRef]*widget.Clickable{}
	if fileRowButton(buttons, a) == fileRowButton(buttons, b) {
		t.Fatal("Files tab shares action widget between directions")
	}
}
