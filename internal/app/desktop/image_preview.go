package desktop

import (
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"strings"
	"sync"

	_ "golang.org/x/image/webp"

	"gioui.org/app"
	"gioui.org/op/paint"
)

// thumbnailMaxWidth and thumbnailMaxHeight define the maximum display size
// (in logical pixels) for image thumbnails inside file cards. The actual
// image is decoded at full resolution; Gio scales it during rendering.
const (
	thumbnailMaxWidth  = 260
	thumbnailMaxHeight = 200
)

// thumbnailState describes the lifecycle of a single cache entry.
type thumbnailState uint8

const (
	// thumbPending means a background goroutine is decoding the image.
	thumbPending thumbnailState = iota
	// thumbReady means the image was decoded successfully and op/bounds are usable.
	thumbReady
	// thumbFailed means decoding was attempted and failed; the entry is
	// cached permanently so we never retry on every frame.
	thumbFailed
)

// thumbnailEntry holds a decoded image ready for Gio rendering.
type thumbnailEntry struct {
	state  thumbnailState
	op     paint.ImageOp
	bounds image.Point // original image dimensions (before scaling)
}

// thumbnailCache is a concurrency-safe cache of decoded image thumbnails.
// Keyed by the on-disk file path (content-addressed hash for sender,
// CompletedPath for receiver).
//
// Decoding happens in a background goroutine. The first call to get() for
// an unknown path spawns the goroutine and returns nil (no thumbnail yet).
// When decoding finishes, the goroutine updates the entry under lock and
// calls window.Invalidate() to schedule a redraw. The next layout pass
// finds the ready entry and renders the thumbnail with zero decode latency.
//
// The cache is intentionally unbounded for the lifetime of the Window:
// each file card has at most one entry, and the number of file cards
// in a conversation is limited by chat history.
type thumbnailCache struct {
	mu      sync.Mutex
	entries map[string]*thumbnailEntry
}

// thumbnailLookup is the atomic result of resolving a cache entry
// (or kicking off a fresh decode). Combining the get/isPending pair
// into a single struct returned under one cache lock closes the
// reviewer race: with two separate calls, a thumbnail that
// transitions Pending→Ready BETWEEN them returns (nil, !pending)
// — i.e. "not ready and won't poll", which can leave the file tab
// stuck on a stale placeholder until an unrelated repaint.
//
// Exactly one of Entry / Pending is meaningful at a time:
//   - Entry != nil → ready, render it.
//   - Pending      → decode in flight; caller should schedule a
//     fallback redraw because the decode goroutine's
//     Invalidate is pinned to the FIRST requesting
//     window and may miss this window.
//   - both zero    → permanent failure (decode failed earlier; the
//     cache will not retry). Caller must not poll.
type thumbnailLookup struct {
	Entry   *thumbnailEntry
	Pending bool
}

// lookup atomically fetches the cache entry for path under a single
// lock acquisition, kicking off a background decode on first
// request. Use this instead of get()+isPending() when both pieces
// of state matter — the two-call form has a nil→ready race window
// that drops the polling gate prematurely.
func (tc *thumbnailCache) lookup(path string, window *app.Window) thumbnailLookup {
	if path == "" || window == nil {
		return thumbnailLookup{}
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.entries == nil {
		tc.entries = make(map[string]*thumbnailEntry)
	}
	if entry, ok := tc.entries[path]; ok {
		switch entry.state {
		case thumbReady:
			return thumbnailLookup{Entry: entry}
		case thumbPending:
			return thumbnailLookup{Pending: true}
		default: // thumbFailed
			return thumbnailLookup{}
		}
	}
	entry := &thumbnailEntry{state: thumbPending}
	tc.entries[path] = entry
	go tc.decodeInBackground(path, window)
	return thumbnailLookup{Pending: true}
}

// get returns the cached thumbnail for the given path.
//
// Three possible outcomes:
//   - entry is ready (thumbReady): returns the entry — caller renders it.
//   - entry is pending (thumbPending): returns nil — decode in progress,
//     a redraw will be triggered when it finishes.
//   - path not seen before: spawns a background decode goroutine and
//     returns nil. The goroutine calls window.Invalidate() on completion.
//   - entry failed (thumbFailed): returns nil — will not retry.
//
// get() collapses pending and failed into the same nil return,
// which is fine for the chat thread because each chat-bubble
// repaint re-runs get() and picks up the eventual ready state.
// Callers that need to DISTINGUISH pending from failed (e.g. the
// file tab's polled redraw gate, which must stop polling once a
// decode permanently fails) MUST use lookup() instead — it
// resolves both pieces of state under a single lock and avoids
// the get() + isPending() race that lookup() was introduced to
// fix.
//
// The window parameter is used solely to call Invalidate() from the
// background goroutine; it is safe to call from any goroutine.
func (tc *thumbnailCache) get(path string, window *app.Window) *thumbnailEntry {
	if path == "" || window == nil {
		return nil
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.entries == nil {
		tc.entries = make(map[string]*thumbnailEntry)
	}

	if entry, ok := tc.entries[path]; ok {
		if entry.state == thumbReady {
			return entry
		}
		// pending or failed — nothing to render yet (or ever).
		return nil
	}

	// First access for this path — create a pending entry and spawn
	// background decode.
	entry := &thumbnailEntry{state: thumbPending}
	tc.entries[path] = entry

	go tc.decodeInBackground(path, window)

	return nil
}

// decodeInBackground decodes the image at path and updates the cache
// entry. Calls window.Invalidate() to trigger a redraw regardless of
// success or failure (the next layout frame will pick up the new state).
func (tc *thumbnailCache) decodeInBackground(path string, window *app.Window) {
	img, err := decodeImageFile(path)

	tc.mu.Lock()
	entry := tc.entries[path]
	if entry == nil {
		// Entry was invalidated while we were decoding — discard result.
		tc.mu.Unlock()
		return
	}
	if err != nil {
		entry.state = thumbFailed
	} else {
		entry.op = paint.NewImageOp(img)
		entry.bounds = img.Bounds().Size()
		entry.state = thumbReady
	}
	tc.mu.Unlock()

	window.Invalidate()
}

// isImageContentType returns true if the MIME content type represents an
// image format that Go's standard library (plus x/image/webp) can decode.
func isImageContentType(contentType string) bool {
	ct := strings.ToLower(contentType)
	switch {
	case strings.HasPrefix(ct, "image/png"):
		return true
	case strings.HasPrefix(ct, "image/jpeg"):
		return true
	case strings.HasPrefix(ct, "image/gif"):
		return true
	case strings.HasPrefix(ct, "image/webp"):
		return true
	default:
		return false
	}
}

// maxImageDecodeBytes is the maximum file size we attempt to decode as an
// image thumbnail. Files larger than this are skipped to avoid excessive
// memory usage.
const maxImageDecodeBytes = 20 * 1024 * 1024 // 20 MB

// decodeImageFile opens a file and decodes it as an image. Returns an
// error if the file cannot be read, the format is unrecognized, or the
// file exceeds maxImageDecodeBytes.
func decodeImageFile(path string) (image.Image, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > maxImageDecodeBytes {
		return nil, fmt.Errorf("file too large for thumbnail: %d bytes", info.Size())
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	img, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}
	return img, nil
}

// thumbnailDisplaySize computes the display dimensions that fit the
// original image within the maxWidth × maxHeight box while preserving
// the aspect ratio.
func thumbnailDisplaySize(origW, origH, maxW, maxH int) (w, h int) {
	if origW <= 0 || origH <= 0 {
		return maxW, maxH
	}

	w, h = origW, origH

	// Scale down to fit maxWidth.
	if w > maxW {
		h = h * maxW / w
		w = maxW
	}
	// Scale down to fit maxHeight.
	if h > maxH {
		w = w * maxH / h
		h = maxH
	}

	if w <= 0 {
		w = 1
	}
	if h <= 0 {
		h = 1
	}
	return w, h
}
