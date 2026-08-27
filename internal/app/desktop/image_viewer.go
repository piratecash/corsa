package desktop

import (
	"encoding/json"
	"errors"
	"image"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gioui.org/f32"
	"gioui.org/layout"
	"gioui.org/widget"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// image_viewer.go is the in-app image viewer: one picture over the whole
// window, stepping through every image of the open conversation, with save
// and delete under it. See docs/design/CHANGES-image-viewer.md, screens
// 8a–8d.
//
// It replaces handing the file to the desktop's own image application, which
// was never a portable answer: openFile is a stub on Android (no
// FileProvider — see open_android.go), so a tapped preview did nothing at
// all there. The external application is still the fallback for a file this
// viewer cannot draw.

// viewerItem is one image the viewer can show: the file on disk, plus what
// the header, the carousel and the delete path need to name it.
//
// It carries the MESSAGE, not just the path, because deleting an image is
// deleting the message that brought it — the same action as the message
// menu's Delete — and that needs both the conversation peer and the message
// id.
type viewerItem struct {
	messageID domain.MessageID
	peer      domain.PeerIdentity
	// path is the file on disk, EMPTY while it is still arriving. An item
	// with no path is drawn as the loading state and re-resolved on the poll
	// below — that is how a picture appears in an open viewer the moment its
	// download finishes.
	path string
	name string
	size uint64
	// mine says which side of the transfer this node is on, which is what
	// decides where the file lives (the transmit blob or the download).
	mine bool
}

// viewerZoomSteps are the stops the desktop zoom buttons and Ctrl+wheel walk.
// Touch zoom is continuous between the first and the last of them.
var viewerZoomSteps = []float32{100, 200, 400}

func viewerMinZoom() float32 { return viewerZoomSteps[0] }
func viewerMaxZoom() float32 { return viewerZoomSteps[len(viewerZoomSteps)-1] }

// imageViewer is the viewer's whole state. It lives on the UI goroutine
// alone; the only thing crossing goroutines is the decode cache, which
// carries its own mutex.
type imageViewer struct {
	parent *Window

	visible bool
	items   []viewerItem
	index   int

	// itemsPeer and itemsGen are what the item list was built from. The list
	// is rebuilt when either moves — a message arriving in this chat, or a
	// deletion taking one away — and not on every frame: walking a
	// conversation and stat-ing every attachment is per-open work, not
	// per-frame work.
	itemsPeer domain.PeerIdentity
	itemsGen  uint64
	// itemsAt and awaitingFile drive the second reason to rebuild: a
	// download finishing changes what is on disk without changing anything
	// the router snapshot counts, so a list holding an item with no file yet
	// re-resolves itself on a timer until it has one.
	itemsAt      time.Time
	awaitingFile bool
	// itemsDirty is raised by the background goroutines that change what is
	// on disk (a delete that failed) and lowered by the layout goroutine.
	// It is the only viewer state those goroutines touch, and it is atomic
	// for that reason — the same shape as Window.reactionsStale.
	itemsDirty atomic.Bool
	// standalone marks a viewer opened on a file that is not part of the
	// open conversation — the console's Files tab lists every peer's
	// transfers. The list is then exactly that one image and is never
	// rebuilt, because the snapshot it would be rebuilt from describes a
	// different chat.
	standalone bool

	// zoom is a percentage of the fitted size, never of the file's own
	// pixels: 100 is "contained in the viewport", which for a small image is
	// already its natural size (viewerDisplaySize never upscales at 100).
	zoom float32
	// offset pans the image away from centred, in pixels. Only meaningful
	// while the image is larger than the viewport; clamped so its edges can
	// never be dragged inside the viewport.
	offset image.Point

	// confirmDelete is the confirmation over the picture. While it is up the
	// viewer does not step, zoom or scroll — a destructive question must not
	// be answered for a different image than the one it was asked about.
	confirmDelete bool

	closeBtn    widget.Clickable
	prevBtn     widget.Clickable
	nextBtn     widget.Clickable
	zoomInBtn   widget.Clickable
	zoomOutBtn  widget.Clickable
	downloadBtn widget.Clickable
	deleteBtn   widget.Clickable
	// carouselPrevBtn and carouselNextBtn are the compact layout's stepping
	// buttons, which sit at the ends of the thumbnail strip rather than
	// beside the picture. Separate widgets from prevBtn/nextBtn: one
	// Clickable laid out in two places would take a click meant for the
	// other, and only one of the two layouts is ever on screen.
	carouselPrevBtn widget.Clickable
	carouselNextBtn widget.Clickable
	deleteYesBtn    widget.Clickable
	deleteNoBtn     widget.Clickable
	// externalBtn is the fallback state's "Open" link, the one place the
	// external application is still reachable from.
	externalBtn widget.Clickable
	// thumbBtns is one Clickable per item, grown with the list. A slice
	// rather than a map keyed by message id because the strip is the item
	// list — same order, same length — and an index is what a click on it
	// means.
	thumbBtns []widget.Clickable
	// carousel is a plain layout.List: the strip carries no scrollbar (it is
	// dragged, and the arrows beside it step through it), so the widget.List
	// wrapper would only add scrollbar state nothing draws.
	carousel layout.List

	// dismissTag catches presses on the backdrop, confirmTag on the
	// confirmation's own backdrop, imageTag the drags and pinches on the
	// picture itself.
	dismissTag struct{}
	confirmTag struct{}
	// confirmCardTag is the confirmation card itself, which swallows the
	// presses that land on it — its backdrop dismisses, and the card is not
	// its backdrop.
	confirmCardTag struct{}
	imageTag       struct{}

	gestures viewerGestures

	// focusPending asks the next laid-out frame to move the keyboard into
	// the viewer, and focusRing hands it back when the viewer closes. Same
	// pair, and the same reasons, as the console modal's — see claimFocus.
	focusPending bool
	focusRing    menuFocusState

	cache viewerImageCache

	// viewport and fit are what the last drawn frame measured: the box the
	// picture is drawn in, and the size the picture is at zoom 100 —
	// contained in that box and never larger than the file's own pixels.
	// Every size the pan and the gestures work in comes from these two, and
	// neither is knowable outside layout.
	viewport image.Point
	fit      image.Point
	// compact is the layout this frame drew, recorded because the picture
	// area is laid out with its own box's constraints and cannot ask.
	compact bool
	// carouselTarget is an index the strip still has to be scrolled to,
	// or -1 when it is already showing the active thumbnail. The scroll is
	// applied at the top of the next layout because layout.List overwrites
	// its own position while it lays out.
	carouselTarget int
}

func newImageViewer(parent *Window) *imageViewer {
	return &imageViewer{
		parent:         parent,
		zoom:           viewerMinZoom(),
		carousel:       layout.List{Axis: layout.Horizontal},
		carouselTarget: -1,
	}
}

// imageViewer returns the one viewer instance, creating it on first use —
// the same shape as the console, and for the same reason: most sessions
// never open it, and the one that does keeps its decode cache for as long as
// the picture is on screen.
func (w *Window) viewer() *imageViewer {
	if w.imageViewer == nil {
		w.imageViewer = newImageViewer(w)
	}
	return w.imageViewer
}

// imageViewerVisible reports whether the viewer covers the window. It
// tolerates a viewer that was never opened, which is the normal case.
func (w *Window) imageViewerVisible() bool {
	return w.imageViewer != nil && w.imageViewer.visible
}

// openImageViewer shows item, with the open conversation's other images
// around it.
//
// item is passed in rather than looked up, because the two surfaces that
// open the viewer know different things: a chat bubble has the message it
// draws, the console's Files tab has a transfer snapshot for a conversation
// that may not even be the open one.
func (w *Window) openImageViewer(item viewerItem, now time.Time) {
	viewer := w.viewer()
	viewer.visible = true
	viewer.confirmDelete = false
	viewer.focusPending = true
	// No trigger: the thumbnail that opened the viewer belongs to a message
	// row or a console file row, either of which can be gone by the time the
	// viewer closes. restoreOnClose falls back to the composer, which is the
	// one focus target every frame of this window draws.
	viewer.focusRing.open(nil)
	viewer.focusRing.held = true
	viewer.gestures.reset()
	viewer.rebuildItems(now)

	viewer.standalone = true
	for i, candidate := range viewer.items {
		if candidate.messageID == item.messageID {
			viewer.standalone = false
			viewer.show(i)
			break
		}
	}
	if viewer.standalone {
		// A file from another conversation, or one the conversation walk did
		// not resolve: the viewer shows exactly it, and nothing rebuilds a
		// list the open chat does not own.
		viewer.items = []viewerItem{item}
		viewer.noteItemsBuilt(now)
		viewer.show(0)
	}
	w.invalidate()
}

// closeImageViewer hides the viewer and gives its bitmaps back. Tens of
// megabytes are held for the picture on screen and its neighbours, and
// nothing off screen needs any of it.
func (w *Window) closeImageViewer() {
	if !w.imageViewerVisible() {
		return
	}
	viewer := w.imageViewer
	viewer.visible = false
	viewer.confirmDelete = false
	viewer.items = nil
	viewer.index = 0
	viewer.standalone = false
	viewer.gestures.reset()
	viewer.cache.retain("")
	// The viewer can be opened from the console's Files tab, and closing it
	// puts the console back in front. The keyboard has to go back INTO the
	// console then: the fallback the focus ring restores to is the composer,
	// which the console covers and lays out disabled, so Gio would drop that
	// focus at Frame time and leave the window with none at all.
	if w.consoleModalVisible() {
		w.consoleModal.focusPending = true
	}
	w.invalidate()
}

// escapeImageViewer backs out one layer: the delete confirmation first, the
// viewer itself once it is not up. Escape and the system Back key share it,
// for the same reason the console's ladder is shared — a user who cannot
// dismiss the question with the key that dismisses everything else is stuck
// in front of a destructive choice.
func (w *Window) escapeImageViewer() {
	if !w.imageViewerVisible() {
		return
	}
	if w.imageViewer.confirmDelete {
		w.imageViewer.confirmDelete = false
		w.invalidate()
		return
	}
	w.closeImageViewer()
}

// current is the item on screen, if there is one.
func (v *imageViewer) current() (viewerItem, bool) {
	if v.index < 0 || v.index >= len(v.items) {
		return viewerItem{}, false
	}
	return v.items[v.index], true
}

// show moves to index: the zoom goes back to fitted, the pan with it, the
// strip scrolls to the new thumbnail and the decode cache is re-aimed at the
// new neighbourhood.
func (v *imageViewer) show(index int) {
	if index < 0 || index >= len(v.items) {
		return
	}
	v.index = index
	v.zoom = viewerMinZoom()
	v.offset = image.Point{}
	v.gestures.reset()
	v.carouselTarget = index
	v.retainAndPreload()
}

// retainAndPreload aims the decode cache at the current neighbourhood: what
// is outside it is dropped, and what is inside it is asked for.
//
// The ask is the preload. lookup starts a background decode for a path that
// has none and returns whatever it has otherwise, so calling it for the
// neighbours is what makes the next step appear immediately instead of
// through the loading state. Its result is deliberately discarded here — the
// frame that draws a picture asks for it again.
func (v *imageViewer) retainAndPreload() {
	item, ok := v.current()
	if !ok {
		v.cache.retain("")
		return
	}
	neighbours := viewerNeighbourPaths(v.items, v.index)
	v.cache.retain(item.path, neighbours...)
	// The picture on screen first: it is the one decode the user is waiting
	// on, and the admission budget is shared, so asking for it before its
	// neighbours is what keeps them from taking the slot.
	v.cache.lookup(item.path, v.parent.window)
	for _, path := range neighbours {
		v.cache.lookup(path, v.parent.window)
	}
}

// step moves by delta without wrapping: the ends of a conversation are ends,
// and an arrow that jumps from the last image to the first loses the user's
// place in a strip they are reading left to right.
func (v *imageViewer) step(delta int) {
	if v.confirmDelete {
		return
	}
	next := stepViewerIndex(v.index, delta, len(v.items))
	if next == v.index {
		return
	}
	v.show(next)
}

// canStep reports whether an arrow in that direction does anything. It is
// what dims the arrow at the ends of the list rather than removing it: a
// control that disappears moves everything beside it.
func (v *imageViewer) canStep(delta int) bool {
	return !v.confirmDelete && stepViewerIndex(v.index, delta, len(v.items)) != v.index
}

// syncItems refreshes the item list from the open conversation when the
// snapshot it was built from has moved on, keeping the picture on screen
// selected by its message id.
//
// Everything that changes the list — a message arriving, a delete here or at
// the peer — comes back through here, so there is one place that decides
// what the viewer is showing and one rule for what happens when it is gone.
//
// A rebuild that leaves the same image on screen leaves the ZOOM alone too.
// DMGeneration moves for anything the conversation does, a delivery receipt
// included, and resetting the zoom on each of those would drop a user out of
// a magnified picture while they were reading it.
func (v *imageViewer) syncItems(now time.Time) {
	if !v.visible {
		return
	}
	if v.standalone {
		if v.itemsExpired(now) {
			v.refreshStandalone(now)
		}
		return
	}
	snap := v.parent.snap
	fresh := snap.ActivePeer == v.itemsPeer && snap.DMGeneration == v.itemsGen
	if fresh && !v.itemsExpired(now) && !v.itemsDirty.CompareAndSwap(true, false) {
		return
	}
	previous, had := v.current()
	v.rebuildItems(now)
	if len(v.items) == 0 {
		v.parent.closeImageViewer()
		return
	}
	next := min(v.index, len(v.items)-1)
	if had {
		next = viewerIndexAfterRebuild(previous.messageID, v.items, v.index)
	}
	if had && v.items[next].messageID == previous.messageID {
		v.index = next
		v.retainAndPreload()
		return
	}
	v.show(next)
}

// itemsExpired reports whether the list is due a re-resolve because
// something in it is still waiting for its file.
//
// This is the whole of the polling: a list with every file on disk is never
// rebuilt on a timer, and one that is waiting re-reads the transfer state a
// couple of times a second until it is not.
func (v *imageViewer) itemsExpired(now time.Time) bool {
	return v.awaitingFile && !v.itemsAt.IsZero() && !now.Before(v.itemsAt.Add(viewerItemsPollInterval))
}

// viewerItemsPollInterval is how often a list with a file still arriving
// re-resolves itself. The same cadence the file cards poll their progress at.
const viewerItemsPollInterval = 500 * time.Millisecond

// rebuildItems collects the open conversation's images. It does NOT touch
// the index — callers decide what the new list means for what is on screen.
func (v *imageViewer) rebuildItems(now time.Time) {
	snap := v.parent.snap
	v.items = collectViewerItems(snap.ActiveMessages, snap.MyAddress, snap.ActivePeer, v.parent.resolveViewerFile)
	v.itemsPeer = snap.ActivePeer
	v.itemsGen = snap.DMGeneration
	v.noteItemsBuilt(now)
	if len(v.thumbBtns) < len(v.items) {
		v.thumbBtns = make([]widget.Clickable, len(v.items))
	}
}

// noteItemsBuilt records when the list was resolved and whether anything in
// it is still waiting for a file.
func (v *imageViewer) noteItemsBuilt(now time.Time) {
	v.itemsAt = now
	v.awaitingFile = false
	for _, item := range v.items {
		if item.path == "" {
			v.awaitingFile = true
			return
		}
	}
}

// refreshStandalone re-resolves the one file a console-opened viewer is
// showing. There is no conversation to rebuild from — the row it came from
// may belong to another chat — but a download that finishes while it is open
// still has to appear.
func (v *imageViewer) refreshStandalone(now time.Time) {
	item, ok := v.current()
	if !ok || item.path != "" {
		return
	}
	v.itemsAt = now
	path, arriving := v.parent.resolveViewerFile(item.messageID, item.mine)
	if path == "" {
		v.awaitingFile = arriving
		return
	}
	v.items[v.index].path = path
	v.awaitingFile = false
	v.retainAndPreload()
}

// resolveViewerFile answers where a file_announce's file is, and — when it
// is not anywhere yet — whether it is on its way.
//
// Both halves matter. The receiver's mapping keeps a path after the download
// completed and the file behind it can be deleted from under the message, so
// the path is checked against the disk rather than trusted. And a file still
// downloading has no path at all, which is not the same as "no picture here":
// it is the picture the viewer draws as loading and picks up when it lands.
func (w *Window) resolveViewerFile(id domain.MessageID, isMine bool) (string, bool) {
	if w.router == nil {
		return "", false
	}
	bridge := w.router.FileBridge()
	if bridge == nil {
		return "", false
	}
	fileID := domain.FileID(id)
	if path := bridge.FilePath(fileID, isMine); path != "" {
		if _, err := os.Stat(path); err == nil {
			return path, false
		}
	}
	_, _, state, found := bridge.Progress(fileID, isMine)
	return "", found && transferIsArriving(state, isMine)
}

// transferIsArriving reports whether a file with nothing on disk yet is
// expected to have some.
//
// Only the receiving side and only while chunks are actually moving: a
// download paused because the sender is offline (waiting_route), a failed
// one and an untouched announce may all sit there for days, and listing them
// would fill the strip with pictures that never appear. The sending side has
// its blob from the start — no path there means it is gone, not coming.
func transferIsArriving(state string, isMine bool) bool {
	if isMine {
		return false
	}
	return state == "downloading" || state == "verifying"
}

// viewerPathResolver reports the on-disk file of a message's attachment, or
// — with no path — whether one is arriving. Injected so the collection rule
// can be tested without a file-transfer subsystem behind it.
type viewerPathResolver func(id domain.MessageID, isMine bool) (path string, arriving bool)

// collectViewerItems walks a conversation in message order and keeps the
// image attachments whose file is on disk — the same test the chat bubble
// applies before it draws a preview, so what the viewer steps through is
// what the conversation shows.
func collectViewerItems(
	messages []service.DirectMessage,
	me, peer domain.PeerIdentity,
	resolve viewerPathResolver,
) []viewerItem {
	if resolve == nil {
		return nil
	}
	items := make([]viewerItem, 0, len(messages))
	for i := range messages {
		message := messages[i]
		if message.Command != domain.DMCommandFileAnnounce || message.CommandData == "" {
			continue
		}
		var payload domain.FileAnnouncePayload
		if err := json.Unmarshal([]byte(message.CommandData), &payload); err != nil {
			continue
		}
		if !isImageContentType(payload.ContentType) {
			continue
		}
		mine := message.Sender == me
		path, arriving := resolve(domain.MessageID(message.ID), mine)
		if path == "" && !arriving {
			continue
		}
		items = append(items, viewerItem{
			messageID: domain.MessageID(message.ID),
			peer:      peer,
			path:      path,
			name:      payload.FileName,
			size:      payload.FileSize,
			mine:      mine,
		})
	}
	if len(items) == 0 {
		return nil
	}
	return items
}

// viewerIndexAfterRebuild keeps the viewer on the image it was showing.
//
// When that image is still in the list, its new position is the answer
// however the list moved around it. When it is gone — deleted here or at the
// peer — the viewer lands on what took its place, which is the NEXT image
// and, for a deleted last image, the previous one.
func viewerIndexAfterRebuild(current domain.MessageID, items []viewerItem, previousIndex int) int {
	for i := range items {
		if items[i].messageID == current {
			return i
		}
	}
	if previousIndex >= len(items) {
		return max(0, len(items)-1)
	}
	return max(0, previousIndex)
}

// stepViewerIndex moves an index by delta inside count items, stopping at
// the ends instead of wrapping.
func stepViewerIndex(index, delta, count int) int {
	if count == 0 {
		return 0
	}
	next := index + delta
	if next < 0 {
		return 0
	}
	if next >= count {
		return count - 1
	}
	return next
}

// viewerNeighbourPaths is what to decode ahead: the files on either side of
// the one on screen, which are the two the next step can ask for.
//
// An item with no path yet — a file still arriving — is skipped rather than
// held as an empty key.
func viewerNeighbourPaths(items []viewerItem, index int) []string {
	paths := make([]string, 0, viewerCacheMaxEntries-1)
	for _, at := range []int{index - 1, index + 1} {
		if at < 0 || at >= len(items) || items[at].path == "" {
			continue
		}
		paths = append(paths, items[at].path)
	}
	return paths
}

// zoomBy walks the zoom stops. It is what the buttons and Ctrl+wheel do; a
// pinch sets a value between them instead (setZoom).
func (v *imageViewer) zoomBy(delta int) {
	v.setZoom(viewerZoomStep(v.zoom, delta), viewerCenterAnchor(v.viewport))
}

// setZoom changes the zoom, keeping the picture point under anchor where it
// is. Without the anchor a zoom-in walks away from whatever the user was
// looking at, which on a magnified image is the whole reason they zoomed.
func (v *imageViewer) setZoom(zoom float32, anchor f32.Point) {
	zoom = clampViewerZoom(zoom)
	if zoom == v.zoom {
		return
	}
	v.offset = viewerOffsetAfterZoom(v.offset, v.viewport, v.zoom, zoom, anchor)
	v.zoom = zoom
	v.offset = clampViewerOffset(v.offset, v.displaySize(), v.viewport)
}

// displaySize is the size the picture is drawn at right now: the fitted size
// this frame measured, times the zoom.
func (v *imageViewer) displaySize() image.Point {
	return viewerDisplaySize(v.fit, v.zoom/100)
}

// panBy drags the picture, and reports whether it moved. "Did not move" is
// what turns a one-finger drag on a magnified image into a step to the
// neighbouring one: the edge is reached, so the gesture means something else.
func (v *imageViewer) panBy(delta image.Point) bool {
	before := v.offset
	v.offset = clampViewerOffset(v.offset.Add(delta), v.displaySize(), v.viewport)
	return v.offset != before
}

// zoomLabel is the header's "100%".
func (v *imageViewer) zoomLabel() string {
	return strconv.Itoa(int(v.zoom+0.5)) + "%"
}

// clampViewerZoom holds the zoom between the first and last stop.
func clampViewerZoom(zoom float32) float32 {
	if zoom < viewerMinZoom() {
		return viewerMinZoom()
	}
	if zoom > viewerMaxZoom() {
		return viewerMaxZoom()
	}
	return zoom
}

// viewerZoomStep is the next stop up (delta > 0) or down (delta < 0) from
// zoom. A zoom left between stops by a pinch steps to the nearest stop
// BEYOND it rather than snapping back to where it came from.
func viewerZoomStep(zoom float32, delta int) float32 {
	const epsilon = 0.5
	if delta > 0 {
		for _, step := range viewerZoomSteps {
			if step > zoom+epsilon {
				return step
			}
		}
		return viewerMaxZoom()
	}
	if delta < 0 {
		for i := len(viewerZoomSteps) - 1; i >= 0; i-- {
			if viewerZoomSteps[i] < zoom-epsilon {
				return viewerZoomSteps[i]
			}
		}
		return viewerMinZoom()
	}
	return clampViewerZoom(zoom)
}

// viewerDisplaySize is the size a fitted picture is drawn at for a zoom
// factor.
func viewerDisplaySize(fit image.Point, factor float32) image.Point {
	if fit.X <= 0 || fit.Y <= 0 {
		return image.Point{}
	}
	return image.Pt(
		max(1, int(float32(fit.X)*factor+0.5)),
		max(1, int(float32(fit.Y)*factor+0.5)),
	)
}

// clampViewerOffset keeps the picture's edges outside the viewport: an axis
// with nothing to pan is pinned to centred, and one that has been panned
// stops at the edge rather than sliding the picture off screen.
func clampViewerOffset(offset, display, viewport image.Point) image.Point {
	limitX := max(0, (display.X-viewport.X)/2)
	limitY := max(0, (display.Y-viewport.Y)/2)
	return image.Pt(
		clampInt(offset.X, -limitX, limitX),
		clampInt(offset.Y, -limitY, limitY),
	)
}

func clampInt(value, low, high int) int {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

// viewerCenterAnchor is the anchor a keyboard or button zoom uses: the
// middle of the viewport, because those inputs name no point of their own.
func viewerCenterAnchor(viewport image.Point) f32.Point {
	return f32.Pt(float32(viewport.X)/2, float32(viewport.Y)/2)
}

// viewerOffsetAfterZoom returns the pan that keeps the picture point under
// anchor in place across a zoom change.
//
// anchor is in viewport coordinates. The point of the picture under it is
// found relative to the picture's centre, scaled by the zoom ratio, and put
// back under the anchor.
func viewerOffsetAfterZoom(offset, viewport image.Point, from, to float32, anchor f32.Point) image.Point {
	if from <= 0 || to <= 0 {
		return offset
	}
	ratio := to / from
	centreX := float32(viewport.X)/2 + float32(offset.X)
	centreY := float32(viewport.Y)/2 + float32(offset.Y)
	// Where the anchor sits relative to the picture's centre, before and
	// after: the same picture point is ratio times further out.
	nextX := anchor.X - (anchor.X-centreX)*ratio - float32(viewport.X)/2
	nextY := anchor.Y - (anchor.Y-centreY)*ratio - float32(viewport.Y)/2
	return image.Pt(int(nextX+0.5), int(nextY+0.5))
}

// viewerAtHorizontalEdge reports whether the picture cannot be panned
// further in that direction — the condition a swipe to the neighbouring
// image waits for while the picture is magnified.
func viewerAtHorizontalEdge(offset, display, viewport image.Point, direction int) bool {
	limit := max(0, (display.X-viewport.X)/2)
	if limit == 0 {
		return true
	}
	if direction < 0 {
		return offset.X <= -limit
	}
	return offset.X >= limit
}

// requestDelete opens the confirmation. Nothing is deleted here, and there
// is no keyboard shortcut that reaches this: the button sits in the bottom
// row away from close and save, and the only way past it is the explicit
// answer below.
func (v *imageViewer) requestDelete() {
	item, ok := v.current()
	if !ok || !v.canDelete(item) {
		return
	}
	v.confirmDelete = true
	v.parent.invalidate()
}

// canDelete reports whether this image's file may be deleted on its own.
//
// Only files that arrived here. The copy of a picture this node SENT is the
// transmit blob the recipient is still served from — one file per content,
// shared by every message carrying it, and nothing can bring it back — so
// deleting it "for one message" would either take the attachment out of
// another message or, because the store keeps a file something still
// references, delete nothing and put the picture back on the next rebuild.
// Taking back an outgoing image is the message menu's Delete.
func (v *imageViewer) canDelete(item viewerItem) bool {
	return item.path != "" && !item.mine
}

// confirmDeleteCurrent deletes the FILE, and only the file. The message
// stays in the conversation and shows its attachment without a preview; on
// the receiving side it offers to download it from the peer again.
//
// Deleting the message is a different action with a different button (the
// message menu's Delete), because it asks the peer to delete their copy too.
//
// The item leaves the strip here rather than on a later rebuild: the file is
// gone the moment the erasure runs, and the viewer must not stay on a picture
// that is not there. A delete that fails says so on the status line, and the
// image comes back the next time the list is built.
func (v *imageViewer) confirmDeleteCurrent() {
	item, ok := v.current()
	v.confirmDelete = false
	if !ok || !v.canDelete(item) {
		return
	}
	v.parent.deleteLocalFileCopy(item)
	v.dropCurrentItem()
	v.parent.invalidate()
}

// dropCurrentItem takes the image on screen out of the strip and moves to
// whatever takes its place — the next one, or the previous one when the last
// image goes, and the viewer closes on an empty list.
//
// It also drops the file from BOTH bitmap caches. The chat bubble behind the
// viewer reads the same thumbnail entry, so leaving it there would keep the
// preview of a deleted file on screen until an unrelated eviction reached
// it.
func (v *imageViewer) dropCurrentItem() {
	item, ok := v.current()
	if !ok {
		return
	}
	v.cache.forget(item.path)
	v.parent.thumbCache.forget(item.path)

	v.items = append(v.items[:v.index], v.items[v.index+1:]...)
	if len(v.items) == 0 {
		v.parent.closeImageViewer()
		return
	}
	v.show(min(v.index, len(v.items)-1))
}

// deleteLocalFileCopy erases this node's copy of the file behind item and
// keeps the message.
//
// The bitmaps go first and synchronously: every surface that draws a preview
// reads those two caches, and the erasure itself runs on a background
// goroutine — leaving them filled would repaint the picture of a file that is
// being deleted for as long as that takes.
func (w *Window) deleteLocalFileCopy(item viewerItem) {
	w.thumbCache.forget(item.path)
	if w.imageViewer != nil {
		w.imageViewer.cache.forget(item.path)
	}
	if w.router == nil {
		return
	}
	bridge := w.router.FileBridge()
	if bridge == nil {
		return
	}
	if !w.beginUIOp() {
		return
	}
	go func() {
		defer w.endUIOp()
		// The erasure is disk work — an unlink, a directory flush and a
		// rewrite of the mappings file — so it does not belong on the frame.
		err := bridge.DeleteLocalCopy(domain.FileID(item.messageID))
		switch {
		case err == nil, errors.Is(err, filetransfer.ErrNoLocalCopy):
			// Nothing on disk to delete is the state the user asked for.
			w.router.SetSendStatus(w.t("status.image_deleted"))
		default:
			w.router.SetSendStatus(w.t("status.image_delete_failed", err.Error()))
		}
		// Whichever way it went, the list has to be built again, and this is
		// the first moment at which building it gives the right answer. The
		// strip dropped the image when the user confirmed, before this
		// goroutine had touched anything: a rebuild racing it — a message
		// arriving, a receipt moving the DM generation — would find the file
		// still on disk and put the image back. A failed delete needs the
		// rebuild for the opposite reason: the picture really is still there.
		if w.imageViewer != nil {
			w.imageViewer.itemsDirty.Store(true)
		}
		w.invalidate()
	}()
}

// saveToDownloads puts a copy of the file in the user's downloads folder,
// under its own name, without asking anything.
//
// Android has no such folder reachable by an application — its files live in
// app-private storage — so there the save goes through the system document
// picker instead, which is the only way out of it. The picker is also the
// fallback anywhere the folder cannot be found, since refusing to save at all
// would be worse than asking where.
func (w *Window) saveToDownloads(path, displayName string) {
	if path == "" {
		return
	}
	if runtime.GOOS == "android" {
		w.exportReceivedFile(path, displayName)
		return
	}
	directory, err := userDownloadsDir()
	if err != nil {
		w.exportReceivedFile(path, displayName)
		return
	}
	name := strings.TrimSpace(displayName)
	if base := filepath.Base(name); name == "" || base == "." || base == string(filepath.Separator) {
		name = filepath.Base(path)
	} else {
		// A peer-supplied name may not steer the copy out of the folder.
		name = base
	}
	if !w.beginUIOp() {
		return
	}
	go func() {
		defer w.endUIOp()
		saved, err := copyIntoDirectory(path, directory, name, w.uiStop())
		if err != nil {
			w.router.SetSendStatus(w.t("file.export_failed", err.Error()))
			w.invalidate()
			return
		}
		log.Info().Str("path", saved).Msg("desktop: file saved to the downloads folder")
		w.router.SetSendStatus(w.t("file.export_done"))
		w.invalidate()
	}()
}

// downloadCurrent saves the picture to the downloads folder.
func (v *imageViewer) downloadCurrent() {
	item, ok := v.current()
	if !ok || item.path == "" {
		return
	}
	v.parent.saveToDownloads(item.path, item.name)
}
