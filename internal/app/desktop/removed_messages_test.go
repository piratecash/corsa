package desktop

import (
	"image"
	"image/color"
	"path/filepath"
	"testing"

	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

// imageMessage is a file_announce bubble whose content hash is its own id,
// so two of them are two different pictures.
func imageMessage(id string) service.DirectMessage {
	return service.DirectMessage{
		ID:          id,
		Body:        domain.FileDMBodySentinel,
		Command:     domain.DMCommandFileAnnounce,
		CommandData: `{"file_name":"photo.png","file_size":1,"content_type":"image/png","file_hash":"` + id + `"}`,
	}
}

// drawMessage fills every piece of per-message state a drawn bubble leaves
// behind: the widgets of the bubble and the file card, the ⋯ rectangle, and
// the decoded picture together with the memory of where it was read from.
func drawMessage(t *testing.T, w *Window, id, path string) {
	t.Helper()
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})
	if got := decodedBounds(t, &w.thumbCache, imageSource{Path: path, ContentID: id}); got != image.Pt(8, 8) {
		t.Fatalf("%s decoded as %v, want 8x8", id, got)
	}
	w.rememberImagePath(id, path)

	menuBtn := new(widget.Clickable)
	w.messageSelectables[id] = new(widget.Selectable)
	w.msgRightClick[id] = new(rightClickState)
	w.replyQuoteTags[id] = new(widget.Clickable)
	w.msgMenuBtns[id] = menuBtn
	w.menuBtnRects[menuBtn] = image.Rect(0, 0, 10, 10)
	w.msgReactionChips[domain.MessageID(id)] = new(ui.ReactionChipsState)
	w.thumbClickBtns[id] = new(widget.Clickable)
	w.fileDownloadBtns[id] = new(widget.Clickable)
	w.fileCancelDownloadBtns[id] = new(widget.Clickable)
	w.fileRestartBtns[id] = new(widget.Clickable)
	w.fileRevealBtns[id] = new(widget.Clickable)
	w.fileOpenBtns[id] = new(widget.Clickable)
	w.fileRowDeleteBtns[id] = new(widget.Clickable)
}

// windowShowingTwoPictures is a conversation of two image messages, both
// already drawn, with the peer it belongs to recorded as the open one — so
// nothing that follows can be credited to a conversation switch.
func windowShowingTwoPictures(t *testing.T) (*Window, map[string]string) {
	t.Helper()
	peer := domaintest.ID("peer-a")
	w := &Window{
		lastChatPeer:           peer,
		messageSelectables:     map[string]*widget.Selectable{},
		msgRightClick:          map[string]*rightClickState{},
		replyQuoteTags:         map[string]*widget.Clickable{},
		msgMenuBtns:            map[string]*widget.Clickable{},
		menuBtnRects:           map[*widget.Clickable]image.Rectangle{},
		msgReactionChips:       map[domain.MessageID]*ui.ReactionChipsState{},
		thumbClickBtns:         map[string]*widget.Clickable{},
		fileDownloadBtns:       map[string]*widget.Clickable{},
		fileCancelDownloadBtns: map[string]*widget.Clickable{},
		fileRestartBtns:        map[string]*widget.Clickable{},
		fileRevealBtns:         map[string]*widget.Clickable{},
		fileOpenBtns:           map[string]*widget.Clickable{},
		fileRowDeleteBtns:      map[string]*widget.Clickable{},
	}
	w.snap = service.RouterSnapshot{
		ActivePeer:     peer,
		CacheReady:     true,
		DMGeneration:   1,
		ActiveMessages: []service.DirectMessage{imageMessage("msg-1"), imageMessage("msg-2")},
	}
	w.rebuildMsgCache()

	dir := t.TempDir()
	paths := map[string]string{}
	for _, id := range []string{"msg-1", "msg-2"} {
		paths[id] = filepath.Join(dir, id+".png")
		drawMessage(t, w, id, paths[id])
	}
	return w, paths
}

// applyConversation replays the part of one layout pass that answers "which
// messages does the open conversation have now", in layout()'s own order.
func applyConversation(w *Window, messages []service.DirectMessage, cacheReady bool) {
	w.snap.ActiveMessages = messages
	w.snap.CacheReady = cacheReady
	w.snap.DMGeneration++
	w.rebuildMsgCache()
	w.resetConversationStateOnPeerChange()
	w.dropStateOfRemovedMessages()
}

// mapsHolding names every per-message map that still has an entry for id.
// Naming them one by one is the point: "something still holds msg-1" is not
// a usable failure.
func mapsHolding(w *Window, id string) []string {
	var held []string
	note := func(name string, ok bool) {
		if ok {
			held = append(held, name)
		}
	}
	_, ok := w.messageSelectables[id]
	note("messageSelectables", ok)
	_, ok = w.msgRightClick[id]
	note("msgRightClick", ok)
	_, ok = w.replyQuoteTags[id]
	note("replyQuoteTags", ok)
	menuBtn, ok := w.msgMenuBtns[id]
	note("msgMenuBtns", ok)
	if menuBtn != nil {
		_, ok = w.menuBtnRects[menuBtn]
		note("menuBtnRects", ok)
	}
	_, ok = w.msgReactionChips[domain.MessageID(id)]
	note("msgReactionChips", ok)
	_, ok = w.thumbClickBtns[id]
	note("thumbClickBtns", ok)
	_, ok = w.fileDownloadBtns[id]
	note("fileDownloadBtns", ok)
	_, ok = w.fileCancelDownloadBtns[id]
	note("fileCancelDownloadBtns", ok)
	_, ok = w.fileRestartBtns[id]
	note("fileRestartBtns", ok)
	_, ok = w.fileRevealBtns[id]
	note("fileRevealBtns", ok)
	_, ok = w.fileOpenBtns[id]
	note("fileOpenBtns", ok)
	_, ok = w.fileRowDeleteBtns[id]
	note("fileRowDeleteBtns", ok)
	_, ok = w.msgImagePaths[id]
	note("msgImagePaths", ok)
	return held
}

// cachedPaths is what the bitmap cache is holding, by file.
func cachedPaths(w *Window) map[string]bool {
	w.thumbCache.mu.Lock()
	defer w.thumbCache.mu.Unlock()
	held := make(map[string]bool, len(w.thumbCache.entries))
	for path := range w.thumbCache.entries {
		held[path] = true
	}
	return held
}

// TestDeletingOneMessageGivesBackItsState — a message deleted while its
// conversation stays open. The peer does not change, so the reset that runs
// on a conversation switch never sees this, and before this pass the erased
// picture's bitmap sat in the cache until the user walked away from the
// chat.
//
// The message that SURVIVED is asserted too: a deletion must not cost the
// rest of the conversation its decoded pictures.
func TestDeletingOneMessageGivesBackItsState(t *testing.T) {
	t.Parallel()

	w, paths := windowShowingTwoPictures(t)
	drawn := mapsHolding(w, "msg-2")

	applyConversation(w, []service.DirectMessage{imageMessage("msg-2")}, true)

	if held := mapsHolding(w, "msg-1"); len(held) != 0 {
		t.Errorf("the deleted message is still held by %v", held)
	}
	if held := mapsHolding(w, "msg-2"); len(held) != len(drawn) {
		t.Errorf("the surviving message lost state: %v remain of %v", held, drawn)
	}
	cached := cachedPaths(w)
	if cached[paths["msg-1"]] {
		t.Error("the bitmap of the deleted picture is still cached")
	}
	if !cached[paths["msg-2"]] {
		t.Error("the surviving picture was decoded again for nothing")
	}
}

// TestPeerWipingTheThreadGivesBackEverything — the same erasure asked for by
// the OTHER side (applyInboundConversationDelete), which reaches this window
// exactly as the local wipe does: the conversation empties, the peer stays.
func TestPeerWipingTheThreadGivesBackEverything(t *testing.T) {
	t.Parallel()

	w, _ := windowShowingTwoPictures(t)

	applyConversation(w, nil, true)

	for _, id := range []string{"msg-1", "msg-2"} {
		if held := mapsHolding(w, id); len(held) != 0 {
			t.Errorf("%s survived the wipe in %v", id, held)
		}
	}
	if cached := cachedPaths(w); len(cached) != 0 {
		t.Errorf("the wiped conversation's bitmaps are still cached: %v", cached)
	}
	w.thumbCache.mu.Lock()
	defer w.thumbCache.mu.Unlock()
	if w.thumbCache.totalBytes != 0 {
		t.Errorf("bytes held after the wipe = %d, want 0", w.thumbCache.totalBytes)
	}
}

// TestLoadingConversationIsNotAnErasedOne: an empty ActiveMessages with
// CacheReady false is a conversation still being read from the database, not
// one whose messages are gone. Freeing on that would throw away the pictures
// of a chat that is about to draw them again — the same reason dropStaleReply
// takes the same gate.
func TestLoadingConversationIsNotAnErasedOne(t *testing.T) {
	t.Parallel()

	w, paths := windowShowingTwoPictures(t)
	drawn := mapsHolding(w, "msg-1")

	applyConversation(w, nil, false)

	if !cachedPaths(w)[paths["msg-1"]] {
		t.Error("a conversation that is merely loading lost its bitmaps")
	}
	if held := mapsHolding(w, "msg-1"); len(held) != len(drawn) {
		t.Errorf("a conversation that is merely loading lost state: %v remain of %v", held, drawn)
	}
}

// TestOpenConsoleKeepsTheButtonsItDraws: the Files tab lists every peer's
// transfers through three of these maps, so while it is open the open
// conversation is not authority over their keys. Everything else still goes.
func TestOpenConsoleKeepsTheButtonsItDraws(t *testing.T) {
	t.Parallel()

	w, _ := windowShowingTwoPictures(t)
	w.consoleModal = &consoleModal{}
	w.consoleModal.visible.Store(true)

	applyConversation(w, []service.DirectMessage{imageMessage("msg-2")}, true)

	held := mapsHolding(w, "msg-1")
	want := map[string]bool{
		"fileCancelDownloadBtns": true,
		"fileRevealBtns":         true,
		"fileOpenBtns":           true,
	}
	if len(held) != len(want) {
		t.Fatalf("held by %v, want only the three the console draws", held)
	}
	for _, name := range held {
		if !want[name] {
			t.Errorf("%s should have been given back with the console open", name)
		}
	}
}

// TestClosingTheConsolePaysWhatItDeferred: closing the console moves no DM
// generation, so the debt the pass left behind has to be remembered rather
// than inferred from the counter. Gated on the generation alone, the pass
// had already advanced past this deletion and the three maps kept the
// erased message until the next one — or until the user switched chats.
func TestClosingTheConsolePaysWhatItDeferred(t *testing.T) {
	t.Parallel()

	w, _ := windowShowingTwoPictures(t)
	w.consoleModal = &consoleModal{}
	w.consoleModal.visible.Store(true)

	applyConversation(w, []service.DirectMessage{imageMessage("msg-2")}, true)
	if held := mapsHolding(w, "msg-1"); len(held) == 0 {
		t.Fatal("the console's buttons were taken while it was drawing them")
	}

	// The console closes. Nothing else about the conversation changes — no
	// new message, no new generation, the same peer — and the next frame
	// lays out.
	w.consoleModal.visible.Store(false)
	w.dropStateOfRemovedMessages()

	if held := mapsHolding(w, "msg-1"); len(held) != 0 {
		t.Errorf("after the console closed the deleted message is still held by %v", held)
	}
	// And the surviving message keeps its buttons: the debt is paid against
	// the live set, not by emptying the maps.
	if _, ok := w.fileRevealBtns["msg-2"]; !ok {
		t.Error("the surviving message lost the buttons the console had drawn")
	}
}

// TestConsoleLeavesNothingOfItsOwnBehind: the Files tab writes into three of
// the chat's button maps for transfers of OTHER conversations. Browsing it
// and closing it changes nothing about the open chat — same peer, same
// messages, same DM generation — so a debt raised only by a deletion is
// never raised at all, and those rows stay behind with nobody drawing them.
func TestConsoleLeavesNothingOfItsOwnBehind(t *testing.T) {
	t.Parallel()

	w, _ := windowShowingTwoPictures(t)
	// The open conversation is settled: whatever this pass owes, it has paid.
	w.dropStateOfRemovedMessages()

	// The Files tab opens and lists a transfer belonging to another chat.
	w.consoleModal = &consoleModal{}
	w.consoleModal.visible.Store(true)
	w.dropStateOfRemovedMessages()
	const elsewhere = "msg-of-another-chat"
	w.fileCancelDownloadBtns[elsewhere] = new(widget.Clickable)
	w.fileRevealBtns[elsewhere] = new(widget.Clickable)
	w.fileOpenBtns[elsewhere] = new(widget.Clickable)

	// It closes. Nothing about the conversation moved.
	w.consoleModal.visible.Store(false)
	w.dropStateOfRemovedMessages()

	if held := mapsHolding(w, elsewhere); len(held) != 0 {
		t.Errorf("the console's own rows outlived it in %v", held)
	}
	if _, ok := w.fileOpenBtns["msg-1"]; !ok {
		t.Error("the open conversation lost buttons it is still drawing")
	}
}
