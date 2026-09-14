package updatecheck

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	// DefaultInterval is how often an enabled checker asks again. An hour is
	// far below the endpoint's unauthenticated rate limit and far above how
	// often releases appear; it exists so a machine left running for a week
	// learns about a release without being restarted.
	DefaultInterval = time.Hour
	// DefaultStartDelay keeps the first check away from process start. Not a
	// warm-up: a request that leaves the moment the application opens
	// correlates the operator's IP with "this node just came up", which is
	// exactly the sort of timing fact the rest of the program avoids
	// publishing.
	DefaultStartDelay = 90 * time.Second
)

// Outcome is a closed enum for how one check ended. Every consumer must handle
// all three — "no result yet" is NOT one of them, because it is the absence of
// a Result rather than a value of one (see Checker.Latest).
type Outcome string

const (
	// OutcomeUpToDate — the endpoint answered and this build is current or
	// ahead of the newest published release.
	OutcomeUpToDate Outcome = "up_to_date"
	// OutcomeUpdateAvailable — the endpoint answered and a newer release is
	// published.
	OutcomeUpdateAvailable Outcome = "update_available"
	// OutcomeFailed — the question was not answered. Says nothing about
	// whether an update exists.
	OutcomeFailed Outcome = "failed"
)

// Result is one completed check.
type Result struct {
	// Outcome says which of the fields below carry meaning.
	Outcome Outcome
	// Latest is the newest published release. Meaningful when Outcome is
	// OutcomeUpToDate or OutcomeUpdateAvailable; zero when OutcomeFailed.
	Latest domain.ReleaseVersion
	// Err is why the check failed. Non-nil exactly when Outcome is
	// OutcomeFailed.
	Err error
	// CheckedAt is when the check finished, on every outcome.
	CheckedAt time.Time
}

// UpdateAvailable is the one question the header badge asks. It is a method
// rather than a comparison at the call site so a failed check can never be
// mistaken for "no update": OutcomeFailed answers false here, and the settings
// tab is where the failure is explained.
func (r Result) UpdateAvailable() bool {
	return r.Outcome == OutcomeUpdateAvailable
}

// CheckerOpts carries the parts of a Checker that have a working default.
type CheckerOpts struct {
	// OnResult is called on the checker's own goroutine after each completed
	// check, so a UI can repaint. Nil means nobody is listening.
	OnResult func(Result)
	// Interval between automatic checks. Zero means DefaultInterval.
	Interval time.Duration
	// StartDelay before the first automatic check. Zero means
	// DefaultStartDelay.
	StartDelay time.Duration
	// After and Now are the loop's only dependencies on the passage of time,
	// so a test steps the schedule instead of waiting for it. Nil means
	// time.After and time.Now.
	//
	// Two function fields rather than a Clock interface: the loop selects on
	// the delay channel together with the manual-check kick, so a provider
	// that only slept could not be interrupted, and a generated interface
	// mock cannot hand back a channel the test controls.
	After func(time.Duration) <-chan time.Time
	Now   func() time.Time
}

// Checker owns the schedule: it is the single path on which a release check
// runs. The periodic tick and the user's "check now" button both wake THIS
// goroutine rather than starting a request of their own, so two checks can
// never be in flight against the same endpoint at once.
//
// It starts disabled. Nothing leaves the machine until Enable(true) is called,
// which is what the desktop preference does.
type Checker struct {
	source ReleaseSource
	local  domain.ReleaseVersion

	onResult   func(Result)
	interval   time.Duration
	startDelay time.Duration
	after      func(time.Duration) <-chan time.Time
	now        func() time.Time

	// enabled is read by the loop and written by the UI goroutine.
	enabled atomic.Bool
	// latest is the last completed Result, or nil when no check has completed
	// under the current setting. An atomic pointer rather than a mutex
	// because the layout path reads it on every frame to decide whether to
	// draw the header badge — readers never take publishMu.
	latest atomic.Pointer[Result]
	// publishMu orders "check the consent, then store the result" against
	// "withdraw consent, then clear the result". Two atomics cannot express
	// that: a check finishing at the moment the box is unticked would read
	// enabled as true, be preempted, and store its answer AFTER Enable(false)
	// had already emptied the slot — leaving the header badge lit by a source
	// the user just switched off, and stuck there, since a second
	// Enable(false) has nothing left to change.
	//
	// It guards the same invariant at the START of a check (cancelCheck), so
	// consent cannot be withdrawn between the gate and the request either.
	//
	// It is a leaf: only bookkeeping runs inside it, never I/O — the request is
	// issued after the lock is dropped and has already returned by the time
	// publish takes it. Calling a context.CancelFunc under it is safe: cancel
	// does not block and cannot re-enter here.
	publishMu sync.Mutex
	// cancelCheck aborts the request in flight, or nil when none is. Cancelling
	// is the other half of withdrawing consent: a check whose socket is already
	// open — or still waiting on a connect that the client gives fifteen
	// seconds — would otherwise reach the endpoint after the user unticked the
	// box. Dropping the ANSWER is not enough; the request itself is the thing
	// that shows a third party this node's address.
	cancelCheck context.CancelFunc
	// kick wakes the loop out of its wait. Buffered by one: a second press
	// while a check is already queued asks for nothing new.
	kick chan struct{}
}

// NewChecker builds the scheduler. source and local are positional because
// there is no useful checker without them.
func NewChecker(source ReleaseSource, local domain.ReleaseVersion, opts CheckerOpts) *Checker {
	checker := &Checker{
		source:     source,
		local:      local,
		onResult:   opts.OnResult,
		interval:   opts.Interval,
		startDelay: opts.StartDelay,
		after:      opts.After,
		now:        opts.Now,
		kick:       make(chan struct{}, 1),
	}
	if checker.interval <= 0 {
		checker.interval = DefaultInterval
	}
	if checker.startDelay <= 0 {
		checker.startDelay = DefaultStartDelay
	}
	if checker.after == nil {
		checker.after = time.After
	}
	if checker.now == nil {
		checker.now = time.Now
	}
	return checker
}

// Enabled reports whether checks are allowed to leave the machine.
func (c *Checker) Enabled() bool {
	return c.enabled.Load()
}

// Enable turns the checks on or off. It does NOT ask for one: restoring a
// stored setting at start-up and a user ticking the box are the same change to
// this flag but different requests, and only the second wants an immediate
// check. The caller that knows which it is calls CheckNow.
//
// Turning them OFF discards the last result. The header badge is fed by that
// result, and leaving it standing would keep showing a conclusion drawn from a
// source the user has just withdrawn consent from. The peer-based signal is
// untouched — it is a different source and the badge reads both.
func (c *Checker) Enable(enabled bool) {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	c.enabled.Store(enabled)
	if !enabled {
		// Unconditional, not "only when the flag actually changed": an
		// in-flight check can slip a result in behind a flag that is already
		// false, and the shortcut made the second withdrawal a no-op — which
		// is the one the user would reach for after seeing the badge survive.
		c.latest.Store(nil)
		if c.cancelCheck != nil {
			c.cancelCheck()
			c.cancelCheck = nil
		}
	}
}

// CheckNow asks the loop to check at the next opportunity. It never blocks and
// never starts a request itself — the loop owns the request.
func (c *Checker) CheckNow() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Latest returns the last completed check, and whether there is one.
//
// The bool is the "not checked yet" state: Result has no value that means it,
// and a zero Result would read as a successful check that found version 0.0.0.
func (c *Checker) Latest() (Result, bool) {
	result := c.latest.Load()
	if result == nil {
		return Result{}, false
	}
	return *result, true
}

// UpdateAvailable is what the header badge reads: true only when a completed
// check under the current setting found a newer release.
func (c *Checker) UpdateAvailable() bool {
	result, ok := c.Latest()
	return ok && result.UpdateAvailable()
}

// Run drives the schedule until ctx is cancelled. It is the only goroutine
// that calls the source.
func (c *Checker) Run(ctx context.Context) {
	delay := c.startDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.after(delay):
		case <-c.kick:
		}

		// Checked here rather than around the wait so a disabled checker
		// still parks on its timer instead of spinning: the wait is the same
		// either way, only the request is withheld.
		if c.enabled.Load() {
			c.check(ctx)
		}
		delay = c.interval
	}
}

// check performs one request and publishes its outcome.
func (c *Checker) check(parent context.Context) {
	// The gate and the request start under the same lock that withdrawal
	// takes, so consent cannot be withdrawn in between — and the context this
	// returns is the one Enable(false) cancels.
	ctx, cancel := c.beginCheck(parent)
	if ctx == nil {
		return
	}
	defer c.endCheck(cancel)

	latest, err := c.source.LatestRelease(ctx)
	result := Result{CheckedAt: c.now()}
	switch {
	case err != nil:
		result.Outcome = OutcomeFailed
		result.Err = err
	case latest.Newer(c.local):
		result.Outcome = OutcomeUpdateAvailable
		result.Latest = latest
	default:
		result.Outcome = OutcomeUpToDate
		result.Latest = latest
	}

	// A cancelled check has no outcome to report: shutdown would otherwise
	// light a "check failed" line on the way out, and a withdrawal would
	// explain itself as a network error. Consent is checked again inside
	// publish, where it is atomic with the store.
	if ctx.Err() != nil {
		return
	}
	c.publish(result)
}

// beginCheck opens the request's context if consent still holds, and records
// its cancel so a withdrawal can abort the request rather than only discard its
// answer. A nil context means consent is gone and nothing should be sent.
func (c *Checker) beginCheck(parent context.Context) (context.Context, context.CancelFunc) {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	if !c.enabled.Load() {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancelCheck = cancel
	return ctx, cancel
}

// endCheck retires the finished request's cancel. It clears the field first and
// cancels after, so a withdrawal arriving in between cancels a context this is
// about to release anyway rather than one belonging to the NEXT check.
func (c *Checker) endCheck(cancel context.CancelFunc) {
	c.publishMu.Lock()
	c.cancelCheck = nil
	c.publishMu.Unlock()
	cancel()
}

// publish records the result, unless consent was withdrawn while the request
// was in flight.
func (c *Checker) publish(result Result) {
	stored := func() bool {
		c.publishMu.Lock()
		defer c.publishMu.Unlock()
		if !c.enabled.Load() {
			return false
		}
		c.latest.Store(&result)
		return true
	}()
	if !stored {
		return
	}

	// The zerolog event is built INSIDE the branch that sends it: an event
	// that is created and never given a Msg keeps the buffer it took from the
	// pool.
	switch result.Outcome {
	case OutcomeFailed:
		log.Warn().
			Err(result.Err).
			Str("source", "github_tags").
			Str("local_version", c.local.String()).
			Msg("release check failed")
	default:
		log.Info().
			Str("source", "github_tags").
			Str("outcome", string(result.Outcome)).
			Str("local_version", c.local.String()).
			Str("latest_version", result.Latest.String()).
			Msg("release check completed")
	}

	if c.onResult != nil {
		c.onResult(result)
	}
}
