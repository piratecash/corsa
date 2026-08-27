package desktop

import (
	"image"
	"sync"

	"gioui.org/app"
	"gioui.org/op/paint"
)

// image_viewer_cache.go holds the bitmaps the in-app image viewer paints.
//
// It is a second cache on purpose. thumbnailCache stores bitmaps downscaled
// to thumbnailStoreMaxPx (1024px long side) because its consumers draw them
// into a 260dp box; the viewer fills the window and magnifies up to 400%, so
// the same bitmap would be visibly soft the moment it is opened. Widening the
// thumbnail cache instead was the alternative and is worse in both
// directions: a 48-entry LRU of viewer-sized bitmaps is over a gigabyte, and
// a scrolled conversation would decode every card at viewer size to show it
// at 260dp.
//
// The two caches share what actually bounds memory — estimateDecodeBytes and
// the byte-weighted thumbDecodeAdmit budget — so a viewer decode and a screen
// of thumbnails cannot both claim the peak at once.
type viewerImageCache struct {
	mu      sync.Mutex
	entries map[string]*thumbnailEntry
	// order is insertion order, so the budget evicts the neighbour that has
	// been sitting there longest rather than an arbitrary map key.
	order []string
	// primary is the path of the picture on screen. It is never evicted by
	// the budget: the budget exists to bound the PRELOAD, and dropping what
	// the user is looking at to stay under it would be a blank screen.
	primary    string
	totalBytes int64
}

// viewerStoreMaxPx bounds the long side of a bitmap the viewer keeps. It is
// full resolution for every picture that fits under it, which is what the
// viewer is for — the thumbnail cache is the place where pictures are made
// small.
//
// The number is not a taste decision, it is the hardware's. Gio gives a
// painted image a texture of its own (renderer.texHandle) and PANICS if the
// driver refuses to create it, so a bitmap wider than the device's
// GL_MAX_TEXTURE_SIZE does not render badly — it takes the window down. 4096
// is the floor OpenGL ES 3.0 guarantees, which is the baseline for the
// Android minimum this application targets, and every desktop driver is at or
// above it; Gio caps its own atlases at 8192 for a related reason. A picture
// larger than this is the one case that is downscaled, and at 4096 the 400%
// zoom still has four times the pixels of a 1024-wide viewport.
const viewerStoreMaxPx = 4096

// viewerCacheMaxEntries is the current image plus one neighbour on each
// side. The neighbours are what makes stepping through a conversation feel
// immediate: a decode takes long enough to see, and the next image is the
// one the user is about to ask for.
const viewerCacheMaxEntries = 3

// viewerCacheMaxBytes bounds what the preload may add. Full-resolution
// bitmaps are large — a 12MP photo is 48MB as NRGBA — so "three of them" is
// not a memory bound by itself, and on Android it is an out-of-memory kill.
// The picture on screen is always kept; the neighbours are kept while they
// fit under this.
const viewerCacheMaxBytes = 96 << 20

// lookup resolves the bitmap for path, starting a background decode the
// first time it is asked for. The result carries the same three states as
// thumbnailCache.lookup — ready, decoding, permanently failed — and for the
// same reason: the viewer must tell "not yet" (keep the placeholder, expect
// a redraw) from "never" (draw the fallback), and reading those as two
// separate calls has a Pending→Ready window between them.
func (c *viewerImageCache) lookup(path string, window *app.Window) thumbnailLookup {
	if path == "" {
		return thumbnailLookup{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]*thumbnailEntry)
	}
	if entry, ok := c.entries[path]; ok {
		switch entry.state {
		case thumbReady:
			return thumbnailLookup{Entry: entry}
		case thumbPending:
			return thumbnailLookup{Pending: true}
		default: // thumbFailed
			return thumbnailLookup{}
		}
	}
	if window == nil {
		// Nothing to wake when the decode finishes, so there is no point
		// starting one — what is already decoded is still returned above.
		return thumbnailLookup{}
	}
	entry := &thumbnailEntry{state: thumbPending}
	c.putLocked(path, entry)
	go c.decodeInBackground(path, entry, window)
	return thumbnailLookup{Pending: true}
}

func (c *viewerImageCache) putLocked(path string, entry *thumbnailEntry) {
	c.entries[path] = entry
	c.order = append(c.order, path)
}

// dropLocked removes one entry and everything that accounts for it.
func (c *viewerImageCache) dropLocked(path string) {
	entry, ok := c.entries[path]
	if !ok {
		return
	}
	c.totalBytes -= entry.byteSize
	delete(c.entries, path)
	for i, candidate := range c.order {
		if candidate == path {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
}

// evictBeyondBudgetLocked drops preloaded neighbours, oldest first, until
// what is held fits the budget. The picture on screen is never a candidate —
// it is the one entry that must be there.
func (c *viewerImageCache) evictBeyondBudgetLocked() {
	for c.totalBytes > viewerCacheMaxBytes {
		victim := ""
		for _, path := range c.order {
			if path != c.primary {
				victim = path
				break
			}
		}
		if victim == "" {
			return
		}
		c.dropLocked(victim)
	}
}

// retain names the picture on screen and the neighbours worth keeping, and
// drops everything else.
//
// This cache has no LRU: what it may hold is not "the last few images looked
// at" but exactly the current one and its neighbours, and that set is known
// to the caller on every move. retain("") is the close path — the viewer
// holds tens of megabytes and gives them all back the moment it is
// dismissed.
//
// Dropping a PENDING entry is safe: its decode goroutine re-checks the
// entry it was spawned for by pointer identity and discards a result whose
// entry is gone.
func (c *viewerImageCache) retain(primary string, neighbours ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.primary = primary
	for _, path := range append([]string(nil), c.order...) {
		if path != primary && !containsPath(neighbours, path) {
			c.dropLocked(path)
		}
	}
}

// forget drops one path so the next lookup decodes it again. Used after the
// file behind it is deleted, where a cached bitmap would outlive its file.
func (c *viewerImageCache) forget(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropLocked(path)
}

func containsPath(paths []string, want string) bool {
	for _, path := range paths {
		if path == want {
			return true
		}
	}
	return false
}

// decodeInBackground decodes path at viewer size and publishes the result on
// the entry it was spawned for, then asks for a redraw whichever way it
// went — a failure is a state the viewer draws (the fallback), not silence.
func (c *viewerImageCache) decodeInBackground(path string, entry *thumbnailEntry, window *app.Window) {
	// alive is pointer identity, not key presence: after a retain() dropped
	// this path and a later lookup re-added it, the key exists again but
	// belongs to another decoder's entry.
	alive := func() bool { return c.entries[path] == entry }

	est, err := estimateDecodeBytes(path, viewerStoreMaxPx)
	if err != nil {
		c.mu.Lock()
		if alive() {
			entry.state = thumbFailed
		}
		c.mu.Unlock()
		window.Invalidate()
		return
	}

	c.mu.Lock()
	dropped := !alive()
	c.mu.Unlock()
	if dropped {
		return
	}

	thumbDecodeAdmit(est.PeakBytes)
	defer thumbDecodeRelease(est.PeakBytes)

	// Re-check after the admission wait, which can be long enough for the
	// user to have closed the viewer.
	c.mu.Lock()
	dropped = !alive()
	c.mu.Unlock()
	if dropped {
		return
	}

	img, err := decodeImageFile(path, viewerStoreMaxPx)

	c.mu.Lock()
	if !alive() {
		c.mu.Unlock()
		return
	}
	if err != nil {
		entry.state = thumbFailed
	} else {
		entry.op = paint.NewImageOp(img)
		size := img.Bounds().Size()
		entry.bounds = size
		entry.natural = est.Natural
		// decodeImageFile always returns *image.NRGBA, so 4 bytes per pixel
		// is exact rather than an assumption.
		entry.byteSize = int64(size.X) * int64(size.Y) * 4
		entry.state = thumbReady
		c.totalBytes += entry.byteSize
		c.evictBeyondBudgetLocked()
	}
	c.mu.Unlock()

	window.Invalidate()
}

// scaledSize returns the size natural fits into box without growing past
// its own pixels — the "contain, never upscale" rule the viewer draws at
// 100% zoom, and the same rule thumbnailDisplaySize applies to file cards.
func scaledSize(natural, box image.Point) image.Point {
	if natural.X <= 0 || natural.Y <= 0 || box.X <= 0 || box.Y <= 0 {
		return image.Point{}
	}
	scale := min(float64(box.X)/float64(natural.X), float64(box.Y)/float64(natural.Y))
	if scale > 1 {
		scale = 1
	}
	return image.Pt(
		max(1, int(float64(natural.X)*scale)),
		max(1, int(float64(natural.Y)*scale)),
	)
}
