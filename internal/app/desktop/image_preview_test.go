package desktop

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gioui.org/app"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"
)

// bitmapCache is what the two decode caches have in common: an entry is
// asked for by the picture it is FOR, not by the file name it happens to
// live under. Both are exercised through it because the reused-name failure
// is a property of the identity, not of either cache's eviction policy.
type bitmapCache interface {
	lookup(src imageSource, window *app.Window) thumbnailLookup
}

// writePNG writes a solid square of the given edge, so the decoded bounds
// alone say WHICH picture came back.
func writePNG(t *testing.T, path string, edge int, fill color.NRGBA) {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, edge, edge))
	for y := range edge {
		for x := range edge {
			img.SetNRGBA(x, y, fill)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode %dx%d png: %v", edge, edge, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// decodedBounds asks cache for src until the background decode settles.
//
// A zero-value app.Window is a safe Invalidate target — it has no driver
// yet, so the call is a no-op — which is what lets a decode run here
// without a GPU.
func decodedBounds(t *testing.T, cache bitmapCache, src imageSource) image.Point {
	t.Helper()
	window := &app.Window{}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res := cache.lookup(src, window)
		if res.Entry != nil {
			return res.Entry.bounds
		}
		if !res.Pending {
			t.Fatalf("decode of %q failed permanently", src.Path)
		}
		if time.Now().After(deadline) {
			t.Fatalf("decode of %q did not finish", src.Path)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestThumbnailCacheRedecodesWhenTheFileNameIsReused is the bug this
// identity exists for.
//
// A received file is stored under the sender's file name, and that name is
// handed out again as soon as nothing occupies it — erasing a conversation
// unlinks the file, so the next picture called photo.png lands on exactly
// the path whose bitmap is still cached. Keyed by path alone, the chat
// bubble repainted the ERASED picture while the viewer, which keeps nothing
// once it closes, decoded the file and showed the new one: the same message
// showing two different images depending on where you looked.
func TestThumbnailCacheRedecodesWhenTheFileNameIsReused(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "photo.png")
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})

	var cache thumbnailCache
	if got := decodedBounds(t, &cache, imageSource{Path: path, ContentID: "hash-a"}); got != image.Pt(8, 8) {
		t.Fatalf("first picture decoded as %v, want 8x8", got)
	}

	// The conversation is wiped and a new image arrives under the same
	// name — the file is different, the path is not.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	writePNG(t, path, 16, color.NRGBA{B: 255, A: 255})

	if got := decodedBounds(t, &cache, imageSource{Path: path, ContentID: "hash-b"}); got != image.Pt(16, 16) {
		t.Fatalf("second picture drawn as %v, want 16x16 — the erased picture is still on screen", got)
	}
	// One location, one entry: the replaced bitmap is given back rather
	// than left for an unrelated eviction to reach.
	cache.mu.Lock()
	entries, bytesHeld := len(cache.entries), cache.totalBytes
	cache.mu.Unlock()
	if entries != 1 || bytesHeld != 16*16*4 {
		t.Fatalf("cache holds %d entries / %d bytes, want exactly the new picture", entries, bytesHeld)
	}
}

// TestViewerCacheRedecodesWhenTheFileNameIsReused: the viewer keeps far
// less than the thumbnail cache, which is why it showed the right picture —
// but a neighbour preloaded before the wipe is the same stale bitmap under
// the same reused name.
func TestViewerCacheRedecodesWhenTheFileNameIsReused(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "photo.png")
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})

	var cache viewerImageCache
	if got := decodedBounds(t, &cache, imageSource{Path: path, ContentID: "hash-a"}); got != image.Pt(8, 8) {
		t.Fatalf("first picture decoded as %v, want 8x8", got)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	writePNG(t, path, 16, color.NRGBA{B: 255, A: 255})

	if got := decodedBounds(t, &cache, imageSource{Path: path, ContentID: "hash-b"}); got != image.Pt(16, 16) {
		t.Fatalf("second picture drawn as %v, want 16x16", got)
	}
}

// TestThumbnailCacheServesTheSamePictureFromCache guards the other half of
// the rule: identity is what decides, so the same content under the same
// path is answered from memory instead of being decoded on every frame.
func TestThumbnailCacheServesTheSamePictureFromCache(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "photo.png")
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})

	var cache thumbnailCache
	src := imageSource{Path: path, ContentID: "hash-a"}
	decodedBounds(t, &cache, src)

	// With the file gone, only a cache hit can still answer.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	entry := cache.get(src, &app.Window{})
	if entry == nil || entry.bounds != image.Pt(8, 8) {
		t.Fatalf("entry = %v, want the cached 8x8 picture: the same content must not be decoded twice", entry)
	}
}

// TestPeerChangeGivesBackTheConversationsPictures: the decoded bitmaps and
// the per-message click targets of a chat belong to that chat. The bitmaps
// are the megabytes; the Clickable map was the one per-message map nothing
// ever emptied, so it grew for the life of the process.
func TestPeerChangeGivesBackTheConversationsPictures(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "photo.png")
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})

	w := &Window{
		snap:         service.RouterSnapshot{ActivePeer: domaintest.ID("peer-b")},
		lastChatPeer: domaintest.ID("peer-a"),
	}
	w.thumbClickBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileDownloadBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileCancelDownloadBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileRestartBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileRevealBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileOpenBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	w.fileRowDeleteBtns = map[string]*widget.Clickable{"msg-1": new(widget.Clickable)}
	decodedBounds(t, &w.thumbCache, imageSource{Path: path, ContentID: "hash-a"})

	w.resetConversationStateOnPeerChange()

	w.thumbCache.mu.Lock()
	held := len(w.thumbCache.entries)
	bytesHeld := w.thumbCache.totalBytes
	w.thumbCache.mu.Unlock()
	if held != 0 || bytesHeld != 0 {
		t.Errorf("leaving the chat kept %d bitmaps / %d bytes", held, bytesHeld)
	}
	// Every per-message button map, not just the preview's: they are one
	// fact — state belonging to the chat being left — and a map that misses
	// the switch is memory nothing ever frees.
	for name, held := range map[string]int{
		"thumbClickBtns":         len(w.thumbClickBtns),
		"fileDownloadBtns":       len(w.fileDownloadBtns),
		"fileCancelDownloadBtns": len(w.fileCancelDownloadBtns),
		"fileRestartBtns":        len(w.fileRestartBtns),
		"fileRevealBtns":         len(w.fileRevealBtns),
		"fileOpenBtns":           len(w.fileOpenBtns),
		"fileRowDeleteBtns":      len(w.fileRowDeleteBtns),
	} {
		if held != 0 {
			t.Errorf("%s kept %d entries of a conversation that is not on screen", name, held)
		}
	}
}

// TestThumbnailCacheClearGivesEverythingBack: leaving a conversation hands
// back the megabytes decoded for it, accounting included.
func TestThumbnailCacheClearGivesEverythingBack(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "photo.png")
	writePNG(t, path, 8, color.NRGBA{R: 255, A: 255})

	var cache thumbnailCache
	decodedBounds(t, &cache, imageSource{Path: path, ContentID: "hash-a"})

	cache.clear()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.entries) != 0 || len(cache.lru) != 0 || cache.totalBytes != 0 {
		t.Fatalf("after clear: %d entries, %d in the order, %d bytes held",
			len(cache.entries), len(cache.lru), cache.totalBytes)
	}
}
