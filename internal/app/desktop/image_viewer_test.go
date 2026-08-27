package desktop

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"image"
	"testing"
	"time"

	"gioui.org/f32"
	"gioui.org/io/input"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/unit"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

func imagePayload(t *testing.T, name string, size uint64, contentType string) string {
	t.Helper()
	data, err := json.Marshal(domain.FileAnnouncePayload{
		FileName:    name,
		FileSize:    size,
		ContentType: contentType,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(data)
}

func peerFromByte(b byte) domain.PeerIdentity {
	var id domain.PeerIdentity
	raw := id[:]
	raw[0] = b
	return id
}

// TestCollectViewerItemsKeepsChatOrder is the viewer's list rule: image
// attachments whose file is actually on disk, in the order the conversation
// shows them, and nothing else.
func TestCollectViewerItemsKeepsChatOrder(t *testing.T) {
	me := peerFromByte(1)
	peer := peerFromByte(2)

	messages := []service.DirectMessage{
		{ID: "text", Sender: peer, Recipient: me, Body: "hello"},
		{
			ID: "first", Sender: peer, Recipient: me,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: imagePayload(t, "one.png", 100, "image/png"),
		},
		{
			ID: "document", Sender: peer, Recipient: me,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: imagePayload(t, "notes.pdf", 200, "application/pdf"),
		},
		{
			ID: "missing", Sender: peer, Recipient: me,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: imagePayload(t, "gone.png", 300, "image/png"),
		},
		{
			ID: "broken", Sender: peer, Recipient: me,
			Command: domain.DMCommandFileAnnounce, CommandData: "{",
		},
		{
			ID: "arriving", Sender: peer, Recipient: me,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: imagePayload(t, "coming.png", 500, "image/png"),
		},
		{
			ID: "mine", Sender: me, Recipient: peer,
			Command:     domain.DMCommandFileAnnounce,
			CommandData: imagePayload(t, "two.jpg", 400, "image/jpeg"),
		},
	}

	var sawSender []bool
	resolve := func(id domain.MessageID, isMine bool) (string, bool) {
		sawSender = append(sawSender, isMine)
		switch id {
		case "missing":
			return "", false
		case "arriving":
			// No file yet, but one is on its way: the strip has to hold a
			// place for it, or a download that finishes while the viewer is
			// open has nowhere to appear.
			return "", true
		}
		return "/files/" + string(id), true
	}

	items := collectViewerItems(messages, me, peer, resolve)
	if len(items) != 3 {
		t.Fatalf("items = %d, want 3 (%+v)", len(items), items)
	}
	if items[0].messageID != "first" || items[1].messageID != "arriving" || items[2].messageID != "mine" {
		t.Fatalf("order = %s, %s, %s; want first, arriving, mine",
			items[0].messageID, items[1].messageID, items[2].messageID)
	}
	if items[0].name != "one.png" || items[0].size != 100 || items[0].path != "/files/first" {
		t.Fatalf("first item = %+v", items[0])
	}
	if items[1].path != "" {
		t.Fatalf("a file still arriving has no path yet: %+v", items[1])
	}
	if items[2].peer != peer || !items[2].mine {
		t.Fatalf("outgoing item = %+v, want this node's side of the peer's conversation", items[2])
	}
	// The resolver has to be told which side of the transfer this node is on:
	// the sender's copy and the receiver's copy are different files.
	if len(sawSender) != 4 || sawSender[0] || sawSender[3] != true {
		t.Fatalf("isMine flags = %v, want [false false false true]", sawSender)
	}
}

func TestTransferIsArriving(t *testing.T) {
	for _, state := range []string{"downloading", "verifying"} {
		if !transferIsArriving(state, false) {
			t.Fatalf("%s must count as arriving", state)
		}
		if transferIsArriving(state, true) {
			t.Fatalf("%s on our own side is not a file arriving here", state)
		}
	}
	// A download nobody is driving must not hold a place in the strip.
	for _, state := range []string{"available", "waiting_route", "failed", "completed", "tombstone", ""} {
		if transferIsArriving(state, false) {
			t.Fatalf("%s must not count as arriving", state)
		}
	}
}

func TestCollectViewerItemsEmpty(t *testing.T) {
	me := peerFromByte(1)
	resolve := func(domain.MessageID, bool) (string, bool) { return "/x", false }
	if items := collectViewerItems(nil, me, peerFromByte(2), resolve); items != nil {
		t.Fatalf("items = %+v, want nil", items)
	}
	if items := collectViewerItems([]service.DirectMessage{{ID: "a"}}, me, peerFromByte(2), nil); items != nil {
		t.Fatalf("items with no resolver = %+v, want nil", items)
	}
}

// TestViewerIndexAfterRebuild covers what the viewer shows after the list
// under it changed: the same image wherever it moved to, and — when it is
// gone — the next one, or the previous one if it was last.
func TestViewerIndexAfterRebuild(t *testing.T) {
	items := []viewerItem{
		{messageID: "a"}, {messageID: "b"}, {messageID: "c"},
	}
	tests := []struct {
		name     string
		current  domain.MessageID
		items    []viewerItem
		previous int
		want     int
	}{
		{name: "unmoved", current: "b", items: items, previous: 1, want: 1},
		{name: "shifted by an insert", current: "c", items: []viewerItem{
			{messageID: "new"}, {messageID: "a"}, {messageID: "b"}, {messageID: "c"},
		}, previous: 2, want: 3},
		{name: "deleted in the middle", current: "b", items: []viewerItem{
			{messageID: "a"}, {messageID: "c"},
		}, previous: 1, want: 1},
		{name: "deleted last", current: "c", items: []viewerItem{
			{messageID: "a"}, {messageID: "b"},
		}, previous: 2, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := viewerIndexAfterRebuild(test.current, test.items, test.previous); got != test.want {
				t.Fatalf("index = %d, want %d", got, test.want)
			}
		})
	}
}

func TestStepViewerIndexDoesNotWrap(t *testing.T) {
	if got := stepViewerIndex(0, -1, 3); got != 0 {
		t.Fatalf("step back from the first = %d, want 0", got)
	}
	if got := stepViewerIndex(2, 1, 3); got != 2 {
		t.Fatalf("step past the last = %d, want 2", got)
	}
	if got := stepViewerIndex(1, 1, 3); got != 2 {
		t.Fatalf("step forward = %d, want 2", got)
	}
	if got := stepViewerIndex(0, 1, 0); got != 0 {
		t.Fatalf("step in an empty list = %d, want 0", got)
	}
}

func TestViewerZoomStep(t *testing.T) {
	tests := []struct {
		zoom  float32
		delta int
		want  float32
	}{
		{zoom: 100, delta: 1, want: 200},
		{zoom: 200, delta: 1, want: 400},
		{zoom: 400, delta: 1, want: 400},
		{zoom: 400, delta: -1, want: 200},
		{zoom: 100, delta: -1, want: 100},
		// A pinch leaves the zoom between the stops; a step then moves to
		// the stop BEYOND it, not back to the one it passed.
		{zoom: 137, delta: 1, want: 200},
		{zoom: 137, delta: -1, want: 100},
		{zoom: 250, delta: 1, want: 400},
		{zoom: 250, delta: -1, want: 200},
	}
	for _, test := range tests {
		if got := viewerZoomStep(test.zoom, test.delta); got != test.want {
			t.Fatalf("viewerZoomStep(%v, %d) = %v, want %v", test.zoom, test.delta, got, test.want)
		}
	}
}

func TestScaledSizeContainsWithoutUpscaling(t *testing.T) {
	tests := []struct {
		name    string
		natural image.Point
		box     image.Point
		want    image.Point
	}{
		{name: "wider than the box", natural: image.Pt(2000, 1000), box: image.Pt(500, 500), want: image.Pt(500, 250)},
		{name: "taller than the box", natural: image.Pt(1000, 2000), box: image.Pt(500, 500), want: image.Pt(250, 500)},
		{name: "smaller than the box", natural: image.Pt(120, 80), box: image.Pt(500, 500), want: image.Pt(120, 80)},
		{name: "no box", natural: image.Pt(120, 80), box: image.Point{}, want: image.Point{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := scaledSize(test.natural, test.box); got != test.want {
				t.Fatalf("scaledSize = %v, want %v", got, test.want)
			}
		})
	}
}

func TestClampViewerOffset(t *testing.T) {
	viewport := image.Pt(400, 300)
	// Nothing to pan: a picture no larger than the viewport stays centred
	// however far the finger travelled.
	if got := clampViewerOffset(image.Pt(120, -80), image.Pt(400, 200), viewport); got != (image.Point{}) {
		t.Fatalf("offset of a contained picture = %v, want zero", got)
	}
	// Magnified: the edges stop at the viewport's.
	display := image.Pt(800, 900)
	if got := clampViewerOffset(image.Pt(1000, -1000), display, viewport); got != image.Pt(200, -300) {
		t.Fatalf("clamped offset = %v, want (200,-300)", got)
	}
	if got := clampViewerOffset(image.Pt(50, -20), display, viewport); got != image.Pt(50, -20) {
		t.Fatalf("offset inside the limits = %v, want unchanged", got)
	}
}

func TestViewerAtHorizontalEdge(t *testing.T) {
	viewport := image.Pt(400, 300)
	// Fitted: both edges, so a swipe in either direction steps at once.
	if !viewerAtHorizontalEdge(image.Point{}, image.Pt(400, 300), viewport, -1) ||
		!viewerAtHorizontalEdge(image.Point{}, image.Pt(400, 300), viewport, 1) {
		t.Fatal("a contained picture must be at both edges")
	}
	display := image.Pt(800, 300)
	if viewerAtHorizontalEdge(image.Point{}, display, viewport, -1) {
		t.Fatal("a centred magnified picture still has room to pan")
	}
	if !viewerAtHorizontalEdge(image.Pt(-200, 0), display, viewport, -1) {
		t.Fatal("panned to the end, the next-image edge is reached")
	}
	if !viewerAtHorizontalEdge(image.Pt(200, 0), display, viewport, 1) {
		t.Fatal("panned to the start, the previous-image edge is reached")
	}
}

// TestViewerOffsetAfterZoomHoldsTheAnchor is the rule that makes zooming
// usable: the picture point under the pointer stays under it.
func TestViewerOffsetAfterZoomHoldsTheAnchor(t *testing.T) {
	viewport := image.Pt(400, 400)
	anchor := f32.Pt(100, 100)
	// Centred at 100%, the anchor sits 100px left and above the centre; at
	// 200% that point is 200px out, so the picture has to move 100px right
	// and down to keep it under the pointer.
	got := viewerOffsetAfterZoom(image.Point{}, viewport, 100, 200, anchor)
	if got != image.Pt(100, 100) {
		t.Fatalf("offset = %v, want (100,100)", got)
	}
	// Zooming around the centre moves nothing.
	if got := viewerOffsetAfterZoom(image.Point{}, viewport, 100, 400, viewerCenterAnchor(viewport)); got != (image.Point{}) {
		t.Fatalf("offset around the centre = %v, want zero", got)
	}
}

func TestViewerNeighbourPaths(t *testing.T) {
	items := []viewerItem{
		{path: "a"}, {path: "b"}, {path: "c"}, {path: ""},
	}
	if got := viewerNeighbourPaths(items, 0); len(got) != 1 || got[0] != "b" {
		t.Fatalf("at the first image = %v, want [b]", got)
	}
	if got := viewerNeighbourPaths(items, 1); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("in the middle = %v, want [a c]", got)
	}
	// The neighbour whose file has not arrived yet has nothing to decode.
	if got := viewerNeighbourPaths(items, 2); len(got) != 1 || got[0] != "b" {
		t.Fatalf("beside a file still arriving = %v, want [b]", got)
	}
	if got := viewerNeighbourPaths(nil, 0); len(got) != 0 {
		t.Fatalf("empty list = %v, want nothing", got)
	}
}

// TestViewerCacheKeepsTheCurrentPictureUnderBudget: the byte budget bounds
// the PRELOAD, so it evicts neighbours and never the picture on screen.
func TestViewerCacheKeepsTheCurrentPictureUnderBudget(t *testing.T) {
	var cache viewerImageCache
	cache.entries = map[string]*thumbnailEntry{}
	add := func(path string, bytes int64) {
		entry := &thumbnailEntry{state: thumbReady, byteSize: bytes}
		cache.putLocked(path, entry)
		cache.totalBytes += bytes
	}
	cache.primary = "current"
	add("old", viewerCacheMaxBytes/2)
	add("current", viewerCacheMaxBytes)
	add("new", viewerCacheMaxBytes/2)

	cache.evictBeyondBudgetLocked()

	if _, ok := cache.entries["current"]; !ok {
		t.Fatal("the picture on screen was evicted by the budget")
	}
	if _, ok := cache.entries["old"]; ok {
		t.Fatal("the oldest neighbour should go first")
	}
	if cache.totalBytes != viewerCacheMaxBytes {
		t.Fatalf("held bytes = %d, want the current picture alone (%d)", cache.totalBytes, viewerCacheMaxBytes)
	}
}

func TestViewerCacheRetainDropsWhatIsNotNamed(t *testing.T) {
	var cache viewerImageCache
	cache.entries = map[string]*thumbnailEntry{}
	for _, path := range []string{"a", "b", "c"} {
		cache.putLocked(path, &thumbnailEntry{state: thumbReady, byteSize: 10})
		cache.totalBytes += 10
	}
	cache.retain("b", "c")
	if len(cache.entries) != 2 || cache.entries["a"] != nil {
		t.Fatalf("entries = %v, want b and c", cache.entries)
	}
	if cache.totalBytes != 20 {
		t.Fatalf("held bytes = %d, want 20", cache.totalBytes)
	}
	cache.retain("")
	if len(cache.entries) != 0 || cache.totalBytes != 0 {
		t.Fatalf("closing the viewer must give everything back: %v / %d bytes", cache.entries, cache.totalBytes)
	}
}

// newViewerForTest builds a viewer over a bare Window: everything under test
// here is state, and none of it reaches the router, the file bridge or a
// window handle.
func newViewerForTest(items ...viewerItem) *imageViewer {
	viewer := newImageViewer(&Window{})
	viewer.visible = true
	viewer.standalone = true
	viewer.items = items
	return viewer
}

func TestViewerStepResetsZoomAndPan(t *testing.T) {
	viewer := newViewerForTest(viewerItem{messageID: "a"}, viewerItem{messageID: "b"})
	viewer.viewport = image.Pt(400, 300)
	viewer.fit = image.Pt(400, 300)
	viewer.zoom = 400
	viewer.offset = image.Pt(120, 40)

	viewer.step(1)
	if viewer.index != 1 {
		t.Fatalf("index = %d, want 1", viewer.index)
	}
	if viewer.zoom != viewerMinZoom() || viewer.offset != (image.Point{}) {
		t.Fatalf("zoom/offset after a step = %v/%v, want fitted and centred", viewer.zoom, viewer.offset)
	}
	// The end of the list is the end.
	viewer.step(1)
	if viewer.index != 1 {
		t.Fatalf("index past the end = %d, want 1", viewer.index)
	}
}

func TestViewerStepIsInertUnderTheConfirmation(t *testing.T) {
	viewer := newViewerForTest(viewerItem{messageID: "a"}, viewerItem{messageID: "b"})
	viewer.confirmDelete = true
	viewer.step(1)
	if viewer.index != 0 {
		t.Fatalf("index = %d, want 0: the confirmation freezes the strip", viewer.index)
	}
	if viewer.canStep(1) {
		t.Fatal("the arrows must read as unavailable while the confirmation is up")
	}
}

// TestViewerDropCurrentItem is what a confirmed delete leaves behind: the
// next image, the previous one when the last goes, and a closed viewer on an
// empty list.
func TestViewerDropCurrentItem(t *testing.T) {
	viewer := newViewerForTest(
		viewerItem{messageID: "a", path: "/a"},
		viewerItem{messageID: "b", path: "/b"},
		viewerItem{messageID: "c", path: "/c"},
	)
	viewer.parent.imageViewer = viewer

	viewer.show(1)
	viewer.dropCurrentItem()
	if len(viewer.items) != 2 || viewer.items[1].messageID != "c" {
		t.Fatalf("items = %+v, want a and c", viewer.items)
	}
	if viewer.index != 1 || viewer.items[viewer.index].messageID != "c" {
		t.Fatalf("index = %d, want the next image", viewer.index)
	}

	// The last image: the viewer falls back to the previous one.
	viewer.dropCurrentItem()
	if viewer.index != 0 || viewer.items[0].messageID != "a" {
		t.Fatalf("after deleting the last = index %d, items %+v", viewer.index, viewer.items)
	}

	viewer.dropCurrentItem()
	if viewer.visible {
		t.Fatal("the viewer must close once the last image is deleted")
	}
}

// TestViewerItemsPollOnlyWhileAFileIsMissing: the list re-resolves itself on
// a timer exactly while something in it has no file yet — a finished download
// changes the disk, which no router generation counts — and never otherwise.
func TestViewerItemsPollOnlyWhileAFileIsMissing(t *testing.T) {
	viewer := newViewerForTest(
		viewerItem{messageID: "a", path: "/a"},
		viewerItem{messageID: "b"},
	)
	built := time.Unix(1000, 0)
	viewer.noteItemsBuilt(built)
	if !viewer.awaitingFile {
		t.Fatal("an item with no file must arm the poll")
	}
	if viewer.itemsExpired(built.Add(viewerItemsPollInterval - time.Millisecond)) {
		t.Fatal("the list is re-resolved on the interval, not on every frame")
	}
	if !viewer.itemsExpired(built.Add(viewerItemsPollInterval)) {
		t.Fatal("the interval passed and the list was not re-resolved")
	}

	viewer.items[1].path = "/b"
	viewer.noteItemsBuilt(built)
	if viewer.awaitingFile {
		t.Fatal("every file is on disk: nothing left to wait for")
	}
	if viewer.itemsExpired(built.Add(time.Hour)) {
		t.Fatal("a settled list must never rebuild on a timer")
	}
}

// --- gestures -------------------------------------------------------------

func touchEvent(kind pointer.Kind, id pointer.ID, x, y float32, at time.Duration) pointer.Event {
	return pointer.Event{
		Kind:      kind,
		Source:    pointer.Touch,
		PointerID: id,
		Position:  f32.Pt(x, y),
		Time:      at,
	}
}

func fittedEnv() viewerGestureEnv {
	return viewerGestureEnv{
		Zoom:        100,
		AtStartEdge: true,
		AtEndEdge:   true,
		SlopPx:      10,
		SwipePx:     48,
	}
}

func TestViewerSwipeStepsToTheNextImage(t *testing.T) {
	var gestures viewerGestures
	env := fittedEnv()

	gestures.handle(touchEvent(pointer.Press, 1, 300, 200, 0), env)
	gestures.handle(touchEvent(pointer.Drag, 1, 200, 205, 20*time.Millisecond), env)
	gestures.handle(touchEvent(pointer.Drag, 1, 120, 210, 40*time.Millisecond), env)
	got := gestures.handle(touchEvent(pointer.Release, 1, 120, 210, 60*time.Millisecond), env)

	if got.Kind != viewerGestureStep || got.Step != 1 {
		t.Fatalf("gesture = %+v, want a step to the next image", got)
	}
}

func TestViewerShortSwipeDoesNothing(t *testing.T) {
	var gestures viewerGestures
	env := fittedEnv()

	gestures.handle(touchEvent(pointer.Press, 1, 300, 200, 0), env)
	gestures.handle(touchEvent(pointer.Drag, 1, 280, 200, 20*time.Millisecond), env)
	got := gestures.handle(touchEvent(pointer.Release, 1, 280, 200, 40*time.Millisecond), env)

	if got.Kind != viewerGestureNone {
		t.Fatalf("gesture = %+v, want nothing: 20px is under the swipe threshold", got)
	}
}

func TestViewerMouseDragNeverSteps(t *testing.T) {
	var gestures viewerGestures
	env := fittedEnv()
	mouse := func(kind pointer.Kind, x float32) pointer.Event {
		return pointer.Event{Kind: kind, Source: pointer.Mouse, Position: f32.Pt(x, 100)}
	}

	gestures.handle(mouse(pointer.Press, 300), env)
	gestures.handle(mouse(pointer.Drag, 100), env)
	got := gestures.handle(mouse(pointer.Release, 100), env)

	if got.Kind != viewerGestureNone {
		t.Fatalf("gesture = %+v: there is no mouse swipe between images", got)
	}
}

// TestViewerDragPansBeforeItSteps is the rule that keeps a magnified picture
// usable: one finger moves the picture while it still has room, and only
// asks for the neighbouring image once the edge is against the viewport.
func TestViewerDragPansBeforeItSteps(t *testing.T) {
	var gestures viewerGestures
	panning := viewerGestureEnv{Zoom: 200, SlopPx: 10, SwipePx: 48}

	gestures.handle(touchEvent(pointer.Press, 1, 300, 200, 0), panning)
	got := gestures.handle(touchEvent(pointer.Drag, 1, 200, 200, 20*time.Millisecond), panning)
	if got.Kind != viewerGesturePan || got.Pan.X != -100 {
		t.Fatalf("gesture = %+v, want a pan of -100", got)
	}
	release := gestures.handle(touchEvent(pointer.Release, 1, 200, 200, 40*time.Millisecond), panning)
	if release.Kind != viewerGestureNone {
		t.Fatalf("gesture = %+v: a pan that still had room must not step", release)
	}

	// Same drag once the picture cannot move any further.
	atEdge := viewerGestureEnv{Zoom: 200, AtEndEdge: true, SlopPx: 10, SwipePx: 48}
	gestures.reset()
	gestures.handle(touchEvent(pointer.Press, 1, 300, 200, 0), atEdge)
	gestures.handle(touchEvent(pointer.Drag, 1, 200, 200, 20*time.Millisecond), atEdge)
	gestures.handle(touchEvent(pointer.Drag, 1, 240, 200, 30*time.Millisecond), atEdge)
	gestures.handle(touchEvent(pointer.Drag, 1, 180, 200, 40*time.Millisecond), atEdge)
	stepped := gestures.handle(touchEvent(pointer.Release, 1, 180, 200, 60*time.Millisecond), atEdge)
	if stepped.Kind != viewerGestureStep || stepped.Step != 1 {
		t.Fatalf("gesture = %+v, want a step once the edge is reached", stepped)
	}
}

// TestViewerPanKeepsSubPixelTravel: a trackpad reports fractions of a pixel,
// and truncating each drag on its own would make a slow pan move nothing.
func TestViewerPanKeepsSubPixelTravel(t *testing.T) {
	var gestures viewerGestures
	env := viewerGestureEnv{Zoom: 200, SlopPx: 10, SwipePx: 48}
	mouse := func(kind pointer.Kind, x, y float32) pointer.Event {
		return pointer.Event{Kind: kind, Source: pointer.Mouse, Position: f32.Pt(x, y)}
	}

	gestures.handle(mouse(pointer.Press, 100, 100), env)
	moved := 0
	for i := 1; i <= 3; i++ {
		got := gestures.handle(mouse(pointer.Drag, 100+0.4*float32(i), 100), env)
		if got.Kind != viewerGesturePan {
			t.Fatalf("drag %d = %+v, want a pan", i, got)
		}
		moved += got.Pan.X
	}
	if moved != 1 {
		t.Fatalf("three 0.4px drags moved %dpx, want 1", moved)
	}
}

func TestViewerDoubleTapToggles(t *testing.T) {
	var gestures viewerGestures
	env := fittedEnv()

	gestures.handle(touchEvent(pointer.Press, 1, 150, 120, 0), env)
	first := gestures.handle(touchEvent(pointer.Release, 1, 150, 120, 30*time.Millisecond), env)
	if first.Kind != viewerGestureNone {
		t.Fatalf("first tap = %+v, want nothing yet", first)
	}

	gestures.handle(touchEvent(pointer.Press, 1, 152, 121, 200*time.Millisecond), env)
	second := gestures.handle(touchEvent(pointer.Release, 1, 152, 121, 220*time.Millisecond), env)
	if second.Kind != viewerGestureToggleZoom {
		t.Fatalf("second tap = %+v, want a zoom toggle", second)
	}
	if second.Anchor != f32.Pt(152, 121) {
		t.Fatalf("anchor = %v, want the point touched", second.Anchor)
	}

	// A second tap that arrives too late starts over instead of toggling.
	gestures.handle(touchEvent(pointer.Press, 1, 150, 120, 1*time.Second), env)
	gestures.handle(touchEvent(pointer.Release, 1, 150, 120, 1010*time.Millisecond), env)
	gestures.handle(touchEvent(pointer.Press, 1, 150, 120, 3*time.Second), env)
	late := gestures.handle(touchEvent(pointer.Release, 1, 150, 120, 3010*time.Millisecond), env)
	if late.Kind != viewerGestureNone {
		t.Fatalf("late tap = %+v, want nothing", late)
	}
}

func TestViewerPinchZoomsByTheFingerRatio(t *testing.T) {
	var gestures viewerGestures
	env := fittedEnv()

	gestures.handle(touchEvent(pointer.Press, 1, 100, 200, 0), env)
	gestures.handle(touchEvent(pointer.Press, 2, 200, 200, 10*time.Millisecond), env)
	// Fingers twice as far apart: twice the zoom, around their midpoint.
	got := gestures.handle(touchEvent(pointer.Drag, 2, 300, 200, 30*time.Millisecond), env)
	if got.Kind != viewerGestureZoom {
		t.Fatalf("gesture = %+v, want a pinch zoom", got)
	}
	if got.Zoom != 200 {
		t.Fatalf("zoom = %v, want 200", got.Zoom)
	}
	if got.Anchor != f32.Pt(200, 200) {
		t.Fatalf("anchor = %v, want the midpoint between the fingers", got.Anchor)
	}

	// Lifting one finger ends the zoom; the one left behind keeps panning
	// but must not be read as a swipe.
	gestures.handle(touchEvent(pointer.Release, 2, 300, 200, 40*time.Millisecond), env)
	swiped := viewerGestureEnv{Zoom: 200, AtEndEdge: true, SlopPx: 10, SwipePx: 48}
	gestures.handle(touchEvent(pointer.Drag, 1, 20, 200, 60*time.Millisecond), swiped)
	after := gestures.handle(touchEvent(pointer.Release, 1, 20, 200, 80*time.Millisecond), swiped)
	if after.Kind != viewerGestureNone {
		t.Fatalf("gesture = %+v: the finger left over from a pinch must not step", after)
	}
}

func TestViewerApplyToggleZoom(t *testing.T) {
	viewer := newViewerForTest(viewerItem{messageID: "a"})
	viewer.viewport = image.Pt(400, 400)
	viewer.fit = image.Pt(400, 400)

	viewer.apply(viewerGesture{Kind: viewerGestureToggleZoom, Anchor: f32.Pt(200, 200)})
	if viewer.zoom != viewerDoubleTapZoom {
		t.Fatalf("zoom = %v, want %v", viewer.zoom, float32(viewerDoubleTapZoom))
	}
	viewer.apply(viewerGesture{Kind: viewerGestureToggleZoom, Anchor: f32.Pt(200, 200)})
	if viewer.zoom != viewerMinZoom() {
		t.Fatalf("zoom = %v, want back to fitted", viewer.zoom)
	}
}

func TestViewerZoomClampsToTheStops(t *testing.T) {
	viewer := newViewerForTest(viewerItem{messageID: "a"})
	viewer.viewport = image.Pt(400, 400)
	viewer.fit = image.Pt(400, 400)

	viewer.apply(viewerGesture{Kind: viewerGestureZoom, Zoom: 900, Anchor: f32.Pt(200, 200)})
	if viewer.zoom != viewerMaxZoom() {
		t.Fatalf("zoom = %v, want the maximum", viewer.zoom)
	}
	viewer.apply(viewerGesture{Kind: viewerGestureZoom, Zoom: 12, Anchor: f32.Pt(200, 200)})
	if viewer.zoom != viewerMinZoom() {
		t.Fatalf("zoom = %v, want the minimum", viewer.zoom)
	}
}

// TestViewerConfirmDeletesOnlyOnce drives two real clicks into one frame,
// which is what a double click is.
//
// The first answer deletes and moves the viewer to the next image. The second
// must not exist: it was aimed at a question about a picture that is already
// gone, and acting on it deletes an image nobody was asked about. Left in the
// widget's queue it is worse — it fires the next time the confirmation opens.
func TestViewerConfirmDeletesOnlyOnce(t *testing.T) {
	router := new(input.Router)
	viewer := newViewerForTest(
		viewerItem{messageID: "a", path: "/a"},
		viewerItem{messageID: "b", path: "/b"},
		viewerItem{messageID: "c", path: "/c"},
	)
	viewer.parent.imageViewer = viewer
	viewer.confirmDelete = true

	// The real handler, on a Window with no router: deleteLocalFileCopy
	// stops at that and the rest of the answer — the strip, the caches, the
	// confirmation — runs exactly as it does in the application.
	frame := func(handle bool) {
		ops := new(op.Ops)
		gtx := layout.Context{
			Ops:         ops,
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(200, 60)),
		}
		if handle {
			viewer.handleActions(gtx)
		}
		viewer.deleteYesBtn.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Dimensions{Size: gtx.Constraints.Max}
		})
		router.Frame(ops)
	}

	frame(false) // the button is on screen and can be pressed
	at := f32.Pt(100, 30)
	for range 2 {
		router.Queue(
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: at},
			pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: at},
		)
	}
	frame(true)

	if len(viewer.items) != 2 || viewer.items[0].messageID != "b" {
		t.Fatalf("items = %+v, want b and c: a double click deleted more than one image", viewer.items)
	}
	if viewer.confirmDelete {
		t.Fatal("the confirmation is still up after it was answered")
	}

	// And nothing is left in the queue for the next time the question is
	// asked: a click kept there deletes without asking.
	viewer.confirmDelete = true
	frame(true)
	if len(viewer.items) != 2 {
		t.Fatalf("items = %+v: a click left over from the previous question deleted another image", viewer.items)
	}
}

// TestViewerDeleteIsRefusedForOutgoingImages: what this node holds for a
// picture it sent is the transmit blob the recipient is served from, shared
// by content between messages and impossible to restore.
func TestViewerDeleteIsRefusedForOutgoingImages(t *testing.T) {
	viewer := newViewerForTest(
		viewerItem{messageID: "mine", path: "/mine", mine: true},
		viewerItem{messageID: "theirs", path: "/theirs"},
	)
	if viewer.canDelete(viewer.items[0]) {
		t.Fatal("an outgoing image must not be deletable on its own")
	}
	if !viewer.canDelete(viewer.items[1]) {
		t.Fatal("a received image must be deletable")
	}
	// A file that has not arrived has nothing to delete either.
	if viewer.canDelete(viewerItem{messageID: "arriving"}) {
		t.Fatal("a file still arriving must not be deletable")
	}

	viewer.requestDelete()
	if viewer.confirmDelete {
		t.Fatal("the confirmation opened for an image that cannot be deleted")
	}
	// Even reached directly, the answer changes nothing.
	viewer.confirmDelete = true
	viewer.confirmDeleteCurrent()
	if len(viewer.items) != 2 {
		t.Fatalf("items = %+v: an outgoing image was dropped from the strip", viewer.items)
	}
}

// TestViewerDeleteRemovesTheFileNotTheMessage reads the source, because the
// difference between the two is one call and both compile.
//
// Deleting the image deletes THIS node's copy of the file
// (FileBridge.DeleteLocalCopy) and leaves the message where it is. Deleting
// the message (dispatchMessageDeleteAsync) is the message menu's action: it
// takes the row out of the chat and asks the peer to delete their copy too —
// a different, louder thing to do, and it is what this button used to do.
func TestViewerDeleteRemovesTheFileNotTheMessage(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "image_viewer.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing image_viewer.go: %v", err)
	}
	var confirm *ast.FuncDecl
	ast.Inspect(file, func(node ast.Node) bool {
		decl, ok := node.(*ast.FuncDecl)
		if ok && decl.Recv != nil && decl.Name.Name == "confirmDeleteCurrent" {
			confirm = decl
		}
		return true
	})
	if confirm == nil {
		t.Fatal("no confirmDeleteCurrent in image_viewer.go — this guard can no longer see the code it protects")
	}

	called := map[string]bool{}
	ast.Inspect(confirm.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			called[selector.Sel.Name] = true
		}
		return true
	})
	if !called["deleteLocalFileCopy"] {
		t.Error("the delete button no longer deletes the local file copy")
	}
	if called["dispatchMessageDeleteAsync"] {
		t.Error("the delete button deletes the whole message: the row leaves the chat and the peer is asked " +
			"to delete their copy, which is the message menu's action, not this one's")
	}
}

// TestViewerCirclesComeFromTheSharedComponent reads the source, because what
// went wrong is invisible from the outside until somebody moves a mouse: the
// viewer drew its own circles and, drawing them itself, drew only one state.
// Its buttons had no hover at all while every other modal's did.
func TestViewerCirclesComeFromTheSharedComponent(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "image_viewer_layout.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing image_viewer_layout.go: %v", err)
	}

	shared := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch selector.Sel.Name {
		case "RoundIconButton", "ModalCloseButton":
			shared = true
		case "Ellipse":
			// clip.Ellipse in here is a circle drawn by hand, which is the
			// component's job and the component's states.
			t.Error("the viewer draws a circle of its own instead of using ui.RoundIconButton")
		}
		return true
	})
	if !shared {
		t.Error("the viewer's buttons no longer go through the shared round-button component")
	}
}

// TestViewerLocalesCarryEveryViewerString guards the six catalogues against
// a key added to one of them: a missing translation silently falls back to
// English, which reads as a bug in the wrong place.
func TestViewerLocalesCarryEveryViewerString(t *testing.T) {
	var keys []string
	for key := range messages["en"] {
		if len(key) > 7 && key[:7] == "viewer." {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		t.Fatal("no viewer strings in the English catalogue")
	}
	for _, option := range supportedLanguages {
		for _, key := range keys {
			if value, ok := messages[option.Code][key]; !ok || value == "" {
				t.Errorf("%s is missing %s", option.Code, key)
			}
		}
	}
}

// TestViewerFrameLaysOutEveryState runs real frames through the viewer — both
// layouts, with and without the delete confirmation — because everything
// above this line tests state and none of it would notice a layout that
// panics on a picture that has not been decoded, an empty strip or a missing
// icon.
func TestViewerFrameLaysOutEveryState(t *testing.T) {
	sizes := map[string]image.Point{
		"desktop": {X: 1000, Y: 700},
		"phone":   {X: 380, Y: 780},
	}
	for name, size := range sizes {
		for _, confirm := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				window := &Window{theme: newAppTheme()}
				window.openImageViewer(viewerItem{
					messageID: "a", path: "/does/not/exist/a.png", name: "a.png", size: 1234,
				}, time.Unix(1000, 0))
				viewer := window.viewer()
				// A second image, so the arrows and the thumbnail strip are
				// drawn rather than skipped.
				viewer.items = append(viewer.items, viewerItem{
					messageID: "b", path: "/does/not/exist/b.png", name: "b.png", size: 99,
				})
				viewer.thumbBtns = make([]widget.Clickable, len(viewer.items))
				viewer.confirmDelete = confirm

				router := new(input.Router)
				ops := new(op.Ops)
				gtx := layout.Context{
					Ops:         ops,
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Constraints{Max: size},
				}
				dims := viewer.layout(gtx)
				router.Frame(ops)

				if dims.Size != size {
					t.Fatalf("dims = %v, want the whole window %v", dims.Size, size)
				}
			})
		}
	}
}
