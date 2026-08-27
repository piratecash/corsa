package service

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
)

// deleteCheckpointDelay is how long a request waits for company before the
// checkpoint runs. Long enough that a burst of deletions — a thread wipe
// arriving as N separate commands, paced by the sender's sweep — collapses
// into a handful of truncations; short enough that a single deletion still
// leaves the write-ahead log within a moment of the row going.
const deleteCheckpointDelay = time.Second

// deleteCheckpointer coalesces the WAL truncations that follow deletions.
//
// `wal_checkpoint(TRUNCATE)` waits on readers and rewrites the log file. As
// long as a deletion was a single row that was the right granularity, and
// the guarantee it buys is real: `secure_delete` overwrites the freed page,
// but in WAL mode that overwrite is itself a log frame, so the original
// bytes live in the -wal file until a checkpoint retires them.
//
// A conversation wipe changed the arithmetic. It reaches the receiver as
// one message_delete per message, so the same work that costs the sender a
// single checkpoint after a single transaction would cost the receiver one
// per row, for as long as the sweep takes to walk the thread. Coalescing
// keeps the guarantee — the pages leave the log promptly — without paying
// for it once per message.
type deleteCheckpointer struct {
	store func() *chatlog.Store
	ctx   func() context.Context
	// delay and retryCap are the two constants above, as fields, so a
	// test can exercise the retry without waiting out a real backoff.
	delay    time.Duration
	retryCap time.Duration

	mu       sync.Mutex
	timer    *time.Timer
	stopped  bool
	running  bool
	pending  bool
	failures int
	inFlight chan struct{}
}

// deleteCheckpointRetryCap bounds the backoff between failed attempts.
// `wal_checkpoint(TRUNCATE)` fails on a BUSY database — a reader still
// holds the log — which is a transient condition, so the answer is to come
// back, not to give up: the pages of a deleted message stay in the -wal
// file until some checkpoint retires them, and "the automatic one might"
// is not the guarantee the deletion promised.
const deleteCheckpointRetryCap = 2 * time.Minute

func newDeleteCheckpointer(store func() *chatlog.Store, ctx func() context.Context) *deleteCheckpointer {
	return &deleteCheckpointer{
		store:    store,
		ctx:      ctx,
		delay:    deleteCheckpointDelay,
		retryCap: deleteCheckpointRetryCap,
	}
}

// request asks for a checkpoint soon. Repeated calls inside the window
// collapse into one run: the timer is armed by the first and left alone by
// the rest, so a long burst is truncated at a steady cadence rather than
// once per deletion.
func (c *deleteCheckpointer) request() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	// One at a time — two concurrent truncations race each other, and
	// stop() would have only one of them to wait for. A request that
	// arrives while one is running is REMEMBERED rather than dropped:
	// the deletion behind it added log frames the running checkpoint has
	// already passed, and without the note nothing would retire them
	// until some later, unrelated deletion happened to ask.
	if c.running {
		c.pending = true
		return
	}
	if c.timer != nil {
		return
	}
	c.timer = time.AfterFunc(c.delay, c.run)
}

func (c *deleteCheckpointer) run() {
	c.mu.Lock()
	c.timer = nil
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.running = true
	// Claimed under the same mutex that stop() takes, so a stop racing
	// this line either wins (and this returns above) or waits below. The
	// checkpoint is a WRITE, and the storage contract is that background
	// writers join before the database closes.
	done := make(chan struct{})
	c.inFlight = done
	c.mu.Unlock()

	failed := false
	defer func() {
		c.mu.Lock()
		c.inFlight = nil
		c.running = false
		// A request that arrived mid-run is armed now, from inside the
		// same lock hold that cleared `running` — so the frames it was
		// asking about are retired by the next pass rather than waiting
		// for an unrelated deletion. A run that FAILED re-arms itself the
		// same way, on a backoff, because nothing else will: the request
		// that asked for it is long gone.
		switch {
		case c.stopped:
			c.pending = false
		case failed:
			c.failures++
			c.pending = false
			c.timer = time.AfterFunc(c.retryDelayLocked(), c.run)
		case c.pending:
			c.failures = 0
			c.pending = false
			c.timer = time.AfterFunc(c.delay, c.run)
		default:
			c.failures = 0
		}
		c.mu.Unlock()
		close(done)
	}()

	store := c.store()
	if store == nil {
		// NOT a success. The router asks for a checkpoint at startup on
		// purpose, before the composition root is guaranteed to have finished
		// opening the database — the point of that request is to retire a
		// write-ahead log that a busy reader kept alive through storage.Open.
		// Returning here as if the work were done cancelled exactly that
		// request, and nothing else would ever make it: the deletions of the
		// previous run stay legible in the sidecar until some unrelated
		// deletion happens to ask again.
		failed = true
		log.Debug().Msg("dm_router: wal checkpoint deferred: the chatlog is not open yet")
		return
	}
	if err := store.CheckpointWAL(c.ctx()); err != nil {
		failed = true
		log.Debug().Err(err).Msg("dm_router: coalesced wal checkpoint did not complete; retrying on a backoff")
	}
}

// retryDelayLocked doubles from the ordinary delay up to the cap. Caller
// MUST hold c.mu.
func (c *deleteCheckpointer) retryDelayLocked() time.Duration {
	delay := c.delay
	for range c.failures {
		delay *= 2
		if delay >= c.retryCap {
			return c.retryCap
		}
	}
	return delay
}

// stop refuses further requests, waits for a run already under way, and — if a
// checkpoint was still owed — RUNS it rather than dropping it.
//
// Dropping it was the hole. The coalescing window is a second, and a user who
// deletes a message and closes the application lands inside it every time: the
// timer is cancelled, the process exits, and the pages holding what they just
// erased stay in the write-ahead log until some later run happens to fill it.
// The deletion was reported as done.
//
// Called on shutdown, so the checkpoint has to finish before the database is
// closed — which is also why it is done here, synchronously, rather than handed
// to a goroutine nothing joins.
func (c *deleteCheckpointer) stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopped = true
	// `running` counts as owed. A pass under way when the process starts
	// shutting down cannot report its own failure any more — its deferred
	// handler sees stopped=true and stands down without re-arming or recording
	// anything — so a BUSY database at that instant would leave the deletion in
	// the log with nobody left to retire it. It also cannot cover frames written
	// after it began. One extra truncation at shutdown is the cheaper side of
	// this trade by a wide margin.
	owed := c.pending || c.timer != nil || c.failures > 0 || c.running
	c.pending = false
	c.failures = 0
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	done := c.inFlight
	c.mu.Unlock()

	// WAIT FIRST. SQLite refuses a checkpoint that overlaps another one
	// outright — busy, without consulting the busy handler — so running the
	// final pass while one is under way would return BUSY, and the pass we
	// interrupted has already seen stopped=true and will not re-arm. The
	// deletion would stay in the log with nothing left to retire it.
	if done != nil {
		<-done
	}

	if owed {
		if store := c.store(); store != nil {
			if err := store.CheckpointWAL(c.ctx()); err != nil {
				// Nothing left to retry with — the process is going. Said
				// out loud because what stays behind is the content of a
				// deleted message, in a file the user believes is clean.
				log.Warn().Err(err).
					Msg("dm_router: the write-ahead log still holds a deletion at shutdown; the next start truncates it")
			}
		}
	}
}
