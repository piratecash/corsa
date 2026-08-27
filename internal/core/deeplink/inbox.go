package deeplink

import (
	"sync"

	"github.com/rs/zerolog/log"
)

// maxPendingLinks bounds the queue. A user opens links one at a time;
// anything past this is a misbehaving sender, and dropping the arrivals
// keeps the ones the user is actually waiting for.
const maxPendingLinks = 8

// Inbox is the handover between the thread an operating system delivers
// a URI on — an Apple Event callback, a Win32 message pump, the Android
// UI thread, a local socket accept loop — and the single goroutine that
// owns the interface state. Producers Push, the owner Drains, and
// nothing else crosses.
//
// Waking the consumer is deliberately NOT its job: the queue would then
// call back into the windowing system while holding a lock the same
// callback can re-enter. The pusher owns that half.
//
// The zero value is ready to use.
type Inbox struct {
	mu      sync.Mutex
	pending []string
}

// Push queues a URI delivered by the operating system.
func (i *Inbox) Push(raw string) {
	if raw == "" {
		return
	}
	i.mu.Lock()
	if len(i.pending) >= maxPendingLinks {
		i.mu.Unlock()
		log.Warn().Int("pending", maxPendingLinks).Msg("deep link inbox full; dropping link")
		return
	}
	i.pending = append(i.pending, raw)
	i.mu.Unlock()
}

// Drain takes everything queued so far. Returns nil when empty, so the
// caller's hot path (a frame) costs one lock and no allocation.
func (i *Inbox) Drain() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.pending) == 0 {
		return nil
	}
	drained := i.pending
	i.pending = nil
	return drained
}
