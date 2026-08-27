package desktop

import (
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"strings"
	"sync"

	xdraw "golang.org/x/image/draw"
	_ "golang.org/x/image/webp"

	"gioui.org/app"
	"gioui.org/op/paint"

	"github.com/piratecash/corsa/internal/core/domain"
)

// thumbnailMaxWidth and thumbnailMaxHeight define the maximum display size
// (in logical pixels) for image thumbnails inside file cards. The cached
// bitmap is pre-downscaled to thumbnailStoreMaxPx; Gio scales it to the
// display box during rendering.
const (
	thumbnailMaxWidth  = 260
	thumbnailMaxHeight = 200
)

// replyQuoteThumbDp and composerReplyThumbDp are the square edge sizes
// (in dp) of the mini image preview inside reply quotes: the quote block
// rendered in a chat bubble and the "Replying to" banner above the
// composer respectively. The banner uses a smaller square so it stays
// compact while the user types.
const (
	replyQuoteThumbDp    = 40
	composerReplyThumbDp = 32
)

// isImageFileAnnounce reports whether a DM is a file_announce whose
// payload describes an image the thumbnail pipeline can decode. Reply
// quotes use this to decide if a mini preview should be rendered next
// to the quoted text.
//
// A payload that fails to parse yields false: the quote then degrades
// to plain text, mirroring layoutFileCard's "invalid file data" path.
func isImageFileAnnounce(command domain.DMCommand, commandData string) bool {
	if command != domain.DMCommandFileAnnounce || commandData == "" {
		return false
	}
	var payload domain.FileAnnouncePayload
	if err := json.Unmarshal([]byte(commandData), &payload); err != nil {
		return false
	}
	return isImageContentType(payload.ContentType)
}

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
	bounds image.Point // cached bitmap dimensions
	// natural is the source image's own dimensions, before the downscale
	// that produced bounds. The viewer's header names them, and no consumer
	// can recover them from the bitmap once it has been shrunk — re-reading
	// the header off disk to print a caption would be file I/O on the layout
	// path.
	natural  image.Point
	byteSize int64 // decoded bytes held (0 until ready)
}

// decodeEstimate is what reading an image header tells the decode pipeline:
// what the decode will cost at its peak, and how big the picture actually
// is. They come from the same DecodeConfig, so returning them separately
// would mean reading the header twice.
type decodeEstimate struct {
	PeakBytes int64
	Natural   image.Point
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
// The cache is a bounded LRU (thumbnailCacheMaxEntries): before it was
// unbounded AND held full-resolution decodes, so a long image-heavy
// conversation pinned hundreds of MB — enough to OOM-kill the Android
// process. Entries now hold downscaled bitmaps (thumbnailStoreMaxPx)
// and the least-recently-rendered ones are evicted; a re-scrolled card
// simply decodes again.
type thumbnailCache struct {
	mu         sync.Mutex
	entries    map[string]*thumbnailEntry
	lru        []string // least-recently used first; keys mirror entries
	totalBytes int64    // decoded bytes held by ready entries
}

// thumbnailCacheMaxEntries and thumbnailCacheMaxBytes bound the cache:
// by count AND by the decoded bytes actually held (a downscaled entry
// is up to ~4MB, so a count cap alone would still allow ~200MB).
// Eviction runs until both limits hold.
const (
	thumbnailCacheMaxEntries = 48
	thumbnailCacheMaxBytes   = 64 << 20
)

// Decode admission is BYTE-weighted, not count-based: the real memory
// peak is the transient full-resolution bitmap, and a fixed count cap
// mis-sizes it — two 32MP 16-bit decodes are ~512MB, not the ~260MB a
// "2 at a time" cap would suggest. thumbDecodeBudgetBytes caps the total
// estimated in-flight decode bytes; small previews still fill
// concurrently, large ones serialize. A single decode larger than the
// whole budget is still admitted when nothing else is running (the
// inFlight>0 guard), so one huge image cannot deadlock the pipeline —
// it is instead rejected earlier by maxImageDecodeMemBytes.
const thumbDecodeBudgetBytes = 128 << 20

var (
	thumbDecodeMu       sync.Mutex
	thumbDecodeCond     = sync.NewCond(&thumbDecodeMu)
	thumbDecodeInFlight int64
)

func thumbDecodeAdmit(est int64) {
	thumbDecodeMu.Lock()
	for thumbDecodeInFlight > 0 && thumbDecodeInFlight+est > thumbDecodeBudgetBytes {
		thumbDecodeCond.Wait()
	}
	thumbDecodeInFlight += est
	thumbDecodeMu.Unlock()
}

func thumbDecodeRelease(est int64) {
	thumbDecodeMu.Lock()
	thumbDecodeInFlight -= est
	thumbDecodeCond.Broadcast()
	thumbDecodeMu.Unlock()
}

// touchLocked moves path to the most-recent end of the LRU order.
// Caller holds tc.mu.
func (tc *thumbnailCache) touchLocked(path string) {
	for i, p := range tc.lru {
		if p == path {
			tc.lru = append(tc.lru[:i], tc.lru[i+1:]...)
			break
		}
	}
	tc.lru = append(tc.lru, path)
}

// insertLocked adds a fresh entry and evicts beyond the caps. Evicting a
// PENDING entry is safe: its decode goroutine re-checks entries[path]
// and discards the result when the entry is gone. Caller holds tc.mu.
func (tc *thumbnailCache) insertLocked(path string, e *thumbnailEntry) {
	tc.entries[path] = e
	tc.touchLocked(path)
	tc.evictLocked()
}

// evictLocked drops least-recently-used entries until both the count cap
// and the byte budget hold. Caller holds tc.mu.
func (tc *thumbnailCache) evictLocked() {
	for (len(tc.entries) > thumbnailCacheMaxEntries || tc.totalBytes > thumbnailCacheMaxBytes) && len(tc.lru) > 0 {
		victim := tc.lru[0]
		tc.lru = tc.lru[1:]
		if e := tc.entries[victim]; e != nil {
			tc.totalBytes -= e.byteSize
		}
		delete(tc.entries, victim)
	}
}

// forget drops one path from the cache, so what is drawn for it next is
// decoded again.
//
// It exists for deletion: the file behind an entry can be destroyed while
// the entry is still cached, and every surface that draws a preview reads
// this cache — a file card would keep showing the picture of a message that
// no longer has one until an unrelated eviction happened to reach it.
func (tc *thumbnailCache) forget(path string) {
	if path == "" {
		return
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	entry, ok := tc.entries[path]
	if !ok {
		return
	}
	tc.totalBytes -= entry.byteSize
	delete(tc.entries, path)
	for i, candidate := range tc.lru {
		if candidate == path {
			tc.lru = append(tc.lru[:i], tc.lru[i+1:]...)
			break
		}
	}
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
		tc.touchLocked(path)
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
	tc.insertLocked(path, entry)
	go tc.decodeInBackground(path, entry, window)
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
		tc.touchLocked(path)
		if entry.state == thumbReady {
			return entry
		}
		// pending or failed — nothing to render yet (or ever).
		return nil
	}

	// First access for this path — create a pending entry and spawn
	// background decode.
	entry := &thumbnailEntry{state: thumbPending}
	tc.insertLocked(path, entry)

	go tc.decodeInBackground(path, entry, window)

	return nil
}

// decodeInBackground decodes the image at path and updates the cache
// entry. Calls window.Invalidate() to trigger a redraw regardless of
// success or failure (the next layout frame will pick up the new state).
func (tc *thumbnailCache) decodeInBackground(path string, entry *thumbnailEntry, window *app.Window) {
	// alive reports whether the SAME entry this goroutine was spawned
	// for is still the one cached at path. Pointer identity, not key
	// presence: after an evict+re-add the key exists again but points to
	// a DIFFERENT entry owned by another decoder. Filling it here would
	// double-count totalBytes and race two decoders onto one entry.
	alive := func() bool { return tc.entries[path] == entry }

	// Read the decode cost (DecodeConfig only — no pixel allocation) and
	// reject bombs before admission.
	est, err := estimateDecodeBytes(path, thumbnailStoreMaxPx)
	if err != nil {
		tc.mu.Lock()
		if alive() {
			entry.state = thumbFailed
		}
		tc.mu.Unlock()
		window.Invalidate()
		return
	}
	natural := est.Natural

	// Skip everything if the entry was evicted before we even got here.
	tc.mu.Lock()
	if !alive() {
		tc.mu.Unlock()
		return
	}
	tc.mu.Unlock()

	// Byte-weighted admission bounds the transient full-res peak.
	thumbDecodeAdmit(est.PeakBytes)
	defer thumbDecodeRelease(est.PeakBytes)

	// Re-check after the (possibly long) admission wait.
	tc.mu.Lock()
	if !alive() {
		tc.mu.Unlock()
		return
	}
	tc.mu.Unlock()

	img, err := decodeImageFile(path, thumbnailStoreMaxPx)

	tc.mu.Lock()
	if !alive() {
		// Evicted (or replaced) while decoding — discard the result and
		// do not touch totalBytes for a foreign entry.
		tc.mu.Unlock()
		return
	}
	if err != nil {
		entry.state = thumbFailed
	} else {
		entry.op = paint.NewImageOp(img)
		sz := img.Bounds().Size()
		entry.bounds = sz
		entry.natural = natural
		// The stored bitmap is always *image.NRGBA (downscaleToMaxPx
		// converts even when it does not shrink), so 4 bytes/pixel is
		// exact, not an assumption.
		entry.byteSize = int64(sz.X) * int64(sz.Y) * 4
		tc.totalBytes += entry.byteSize
		entry.state = thumbReady
		tc.evictLocked()
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

// maxImageDecodeMemBytes caps the DECODED in-memory size, computed from
// DecodeConfig BEFORE any pixel allocation. A count/pixel limit alone is
// wrong: a 32MP image is ~128MB at 8bpp (16-bit PNG) but ~128MB is the
// worst case that must not be exceeded on Android, and a kilobyte-scale
// decompression bomb can declare enormous dimensions. Estimating bytes
// (dimensions × the decoded color model's bytes-per-pixel) catches both.
// Anything larger renders as a plain file card without a preview.
const maxImageDecodeMemBytes = 96 << 20

// thumbnailStoreMaxPx bounds the long side of the bitmap kept in the
// cache. The display box is ≤260dp, so 1024px stays crisp on high-DPI
// while costing exactly 1024×h×4 ≈ ≤4MB per entry.
const thumbnailStoreMaxPx = 1024

// modelBytesPerPixel is the worst-case bytes/pixel the decoder allocates
// for cfg.ColorModel. The JPEG and GIF models need care: a JPEG decodes
// into *image.YCbCr (three planes, 3 bytes/px at 4:4:4 and less when
// subsampled) and a GIF into *image.Paletted (1 byte/px) — lumping them
// into the unknown-model fallback made ordinary photos look like 8 bpp
// and get rejected as "too large" (a 20MP JPEG estimated at 160MB
// instead of ~60MB). color.Palette is a slice type implementing
// color.Model, so it needs a type switch rather than an equality case.
// Genuinely unknown models still fall through to 8 so admission never
// UNDER-counts the peak.
func modelBytesPerPixel(m color.Model) int64 {
	if _, ok := m.(color.Palette); ok {
		return 1
	}
	switch m {
	case color.GrayModel, color.AlphaModel:
		return 1
	case color.Gray16Model, color.Alpha16Model:
		return 2
	case color.YCbCrModel:
		return 3
	case color.NRGBAModel, color.RGBAModel, color.CMYKModel, color.NYCbCrAModel:
		return 4
	case color.NRGBA64Model, color.RGBA64Model:
		return 8
	default:
		return 8
	}
}

// scaledBitmapBytes returns the size of the NRGBA bitmap that
// downscaleToMaxPx will produce for a WxH source bounded to maxPx. It is
// allocated WHILE the full-resolution decode is still alive, so admission
// has to reserve both — otherwise many cheap sources (e.g. 1 byte/px
// grayscale) are admitted en masse and then each allocates its output on
// top of the reservation.
//
// maxPx is a parameter rather than the thumbnail constant because the two
// consumers keep bitmaps of very different sizes: a file-card thumbnail is
// capped at thumbnailStoreMaxPx, the image viewer at viewerStoreMaxPx, and
// charging the viewer's decode the thumbnail's output would under-count its
// peak by an order of magnitude.
func scaledBitmapBytes(w, h, maxPx int) int64 {
	long := max(w, h)
	if long > maxPx {
		scale := float64(maxPx) / float64(long)
		w = max(1, int(float64(w)*scale))
		h = max(1, int(float64(h)*scale))
	}
	return int64(w) * int64(h) * 4
}

// estimateDecodeBytes reads only the header (DecodeConfig) and returns
// the estimated PEAK byte cost of producing a bitmap whose long side is
// bounded by storeMaxPx (full-resolution decode + the NRGBA output that
// coexists with it), rejecting files over the size or memory limits
// without allocating any pixels.
func estimateDecodeBytes(path string, storeMaxPx int) (decodeEstimate, error) {
	info, err := os.Stat(path)
	if err != nil {
		return decodeEstimate{}, err
	}
	if info.Size() > maxImageDecodeBytes {
		return decodeEstimate{}, fmt.Errorf("file too large to decode: %d bytes", info.Size())
	}
	f, err := os.Open(path)
	if err != nil {
		return decodeEstimate{}, err
	}
	defer func() { _ = f.Close() }()

	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return decodeEstimate{}, err
	}
	if cfg.Width <= 0 || cfg.Height <= 0 {
		return decodeEstimate{}, fmt.Errorf("invalid image dimensions %dx%d", cfg.Width, cfg.Height)
	}
	decoded := int64(cfg.Width) * int64(cfg.Height) * modelBytesPerPixel(cfg.ColorModel)
	if decoded > maxImageDecodeMemBytes {
		return decodeEstimate{}, fmt.Errorf("image too large to decode: %dx%d (~%dMB decoded)", cfg.Width, cfg.Height, decoded>>20)
	}
	// Peak = full-resolution decode + the NRGBA bitmap built from it while
	// the former is still referenced.
	return decodeEstimate{
		PeakBytes: decoded + scaledBitmapBytes(cfg.Width, cfg.Height, storeMaxPx),
		Natural:   image.Pt(cfg.Width, cfg.Height),
	}, nil
}

// decodeImageFile decodes the image at path into an *image.NRGBA whose long
// side is at most storeMaxPx. Callers gate the cost first with
// estimateDecodeBytes; this re-checks the file size but assumes admission
// already bounded the peak.
func decodeImageFile(path string, storeMaxPx int) (image.Image, error) {
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
	return downscaleToMaxPx(img, storeMaxPx), nil
}

// downscaleToMaxPx returns an *image.NRGBA whose long side is at most
// maxPx. It ALWAYS produces NRGBA (4bpp) — even when the source is already
// small — so the cached byte size is exactly w×h×4 and a 16-bit (8bpp)
// source never sneaks into a cache at double the accounted cost. The
// full-resolution decode is released to the GC.
func downscaleToMaxPx(img image.Image, maxPx int) *image.NRGBA {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	long := max(w, h)
	if long <= maxPx {
		if nrgba, ok := img.(*image.NRGBA); ok {
			return nrgba
		}
		dst := image.NewNRGBA(image.Rect(0, 0, w, h))
		draw.Draw(dst, dst.Bounds(), img, b.Min, draw.Src)
		return dst
	}
	scale := float64(maxPx) / float64(long)
	nw := max(1, int(float64(w)*scale))
	nh := max(1, int(float64(h)*scale))
	dst := image.NewNRGBA(image.Rect(0, 0, nw, nh))
	xdraw.ApproxBiLinear.Scale(dst, dst.Bounds(), img, b, xdraw.Src, nil)
	return dst
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
