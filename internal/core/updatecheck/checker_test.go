package updatecheck

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

var (
	localVersion   = domain.ReleaseVersion{Major: 2, Minor: 3, Build: 69}
	newerVersion   = domain.ReleaseVersion{Major: 2, Minor: 3, Build: 70}
	olderVersion   = domain.ReleaseVersion{Major: 2, Minor: 3, Build: 68}
	errUnreachable = errors.New("endpoint unreachable")
)

// scriptedDelays stands in for time.After so the schedule is stepped rather
// than waited out. Every call hands back the SAME unbuffered channel, so
// fire() blocks until the loop is actually parked on its wait — which is the
// synchronisation these tests need.
type scriptedDelays struct {
	ch chan time.Time

	mu       sync.Mutex
	observed []time.Duration
}

func newScriptedDelays() *scriptedDelays {
	return &scriptedDelays{ch: make(chan time.Time)}
}

func (s *scriptedDelays) After(d time.Duration) <-chan time.Time {
	s.mu.Lock()
	s.observed = append(s.observed, d)
	s.mu.Unlock()
	return s.ch
}

func (s *scriptedDelays) fire(t *testing.T) {
	t.Helper()
	select {
	case s.ch <- time.Time{}:
	case <-time.After(2 * time.Second):
		t.Fatal("checker never parked on its delay")
	}
}

func (s *scriptedDelays) delays() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.observed...)
}

// countingSource records how many times the checker asked, and releases a
// signal after each answer. ReleaseSourceFunc is the production adapter, so
// this is a plain function rather than a hand-written interface stand-in.
type countingSource struct {
	answered chan struct{}

	mu      sync.Mutex
	calls   int
	version domain.ReleaseVersion
	err     error
}

func newCountingSource(version domain.ReleaseVersion) *countingSource {
	return &countingSource{answered: make(chan struct{}, 8), version: version}
}

func (s *countingSource) source() ReleaseSource {
	return ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
		s.mu.Lock()
		s.calls++
		version, err := s.version, s.err
		s.mu.Unlock()
		s.answered <- struct{}{}
		return version, err
	})
}

func (s *countingSource) setFailure(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *countingSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *countingSource) awaitAnswer(t *testing.T) {
	t.Helper()
	select {
	case <-s.answered:
	case <-time.After(2 * time.Second):
		t.Fatal("the source was never asked")
	}
}

// runChecker starts the loop and stops it when the test ends.
func runChecker(t *testing.T, checker *Checker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		checker.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("checker did not stop on context cancellation")
		}
	})
}

// awaitResult waits for the checker to publish, since publication happens on
// the checker's goroutine after the source answers.
func awaitResult(t *testing.T, checker *Checker) Result {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if result, ok := checker.Latest(); ok {
			return result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no result was published")
	return Result{}
}

func newTestChecker(source ReleaseSource, delays *scriptedDelays, onResult func(Result)) *Checker {
	return NewChecker(source, localVersion, CheckerOpts{
		OnResult:   onResult,
		Interval:   time.Hour,
		StartDelay: time.Minute,
		After:      delays.After,
		Now:        func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
}

func TestCheckerStaysSilentUntilEnabled(t *testing.T) {
	// The whole privacy contract: an anonymous messenger must not contact a
	// third party because the process started. Consent is the gate.
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)

	if checker.Enabled() {
		t.Fatal("a new checker must start disabled")
	}

	runChecker(t, checker)
	delays.fire(t)
	// Give the loop a chance to do the wrong thing before concluding it did
	// not: fire() returns as soon as the loop takes the tick.
	delays.fire(t)

	if got := source.callCount(); got != 0 {
		t.Fatalf("the source was asked %d times while disabled", got)
	}
	if _, ok := checker.Latest(); ok {
		t.Fatal("a disabled checker published a result")
	}
	if checker.UpdateAvailable() {
		t.Fatal("a disabled checker reported an update")
	}
}

func TestEnableAloneDoesNotSendAnything(t *testing.T) {
	// Restoring a stored setting at start-up and a user ticking the box are
	// the same change to the flag but different requests. Only the second
	// wants a check now, and only the caller knows which it is — so Enable
	// must not decide for it, or every launch would reach out at t=0, which is
	// exactly what the start delay exists to prevent.
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	runChecker(t, checker)

	checker.Enable(true)

	select {
	case <-source.answered:
		t.Fatal("Enable(true) sent a request of its own")
	case <-time.After(100 * time.Millisecond):
	}

	// The scheduled tick still runs it.
	delays.fire(t)
	source.awaitAnswer(t)
}

func TestCheckNowChecksImmediately(t *testing.T) {
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()

	notified := make(chan Result, 1)
	checker := newTestChecker(source.source(), delays, func(r Result) { notified <- r })
	runChecker(t, checker)

	checker.Enable(true)
	checker.CheckNow()
	source.awaitAnswer(t)

	select {
	case result := <-notified:
		if result.Outcome != OutcomeUpdateAvailable {
			t.Fatalf("Outcome = %q, want %q", result.Outcome, OutcomeUpdateAvailable)
		}
		if result.Latest != newerVersion {
			t.Fatalf("Latest = %s, want %s", result.Latest, newerVersion)
		}
		if result.CheckedAt.IsZero() {
			t.Fatal("CheckedAt was not stamped")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnResult was never called")
	}

	if !checker.UpdateAvailable() {
		t.Fatal("UpdateAvailable is false after a newer release was found")
	}
}

func TestUpToDateIsNotAnUpdate(t *testing.T) {
	for _, published := range []domain.ReleaseVersion{localVersion, olderVersion} {
		source := newCountingSource(published)
		delays := newScriptedDelays()
		checker := newTestChecker(source.source(), delays, nil)
		runChecker(t, checker)

		checker.Enable(true)
		checker.CheckNow()
		source.awaitAnswer(t)
		result := awaitResult(t, checker)

		if result.Outcome != OutcomeUpToDate {
			t.Fatalf("published %s: Outcome = %q, want %q", published, result.Outcome, OutcomeUpToDate)
		}
		if checker.UpdateAvailable() {
			t.Fatalf("published %s: UpdateAvailable is true", published)
		}
	}
}

func TestAFailedCheckIsNotAnUpdateSignal(t *testing.T) {
	// A failure says nothing about whether a release exists. Reporting one
	// would light the header badge on every flaky connection.
	source := newCountingSource(newerVersion)
	source.setFailure(errUnreachable)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	runChecker(t, checker)

	checker.Enable(true)
	checker.CheckNow()
	source.awaitAnswer(t)
	result := awaitResult(t, checker)

	if result.Outcome != OutcomeFailed {
		t.Fatalf("Outcome = %q, want %q", result.Outcome, OutcomeFailed)
	}
	if !errors.Is(result.Err, errUnreachable) {
		t.Fatalf("Err = %v, want %v", result.Err, errUnreachable)
	}
	if result.UpdateAvailable() || checker.UpdateAvailable() {
		t.Fatal("a failed check reported an update")
	}
}

func TestDisablingDiscardsTheResult(t *testing.T) {
	// The badge is fed by this result. Leaving it standing after the user
	// withdrew consent keeps showing a conclusion drawn from a source they
	// just switched off.
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	runChecker(t, checker)

	checker.Enable(true)
	checker.CheckNow()
	source.awaitAnswer(t)
	awaitResult(t, checker)

	checker.Enable(false)

	if _, ok := checker.Latest(); ok {
		t.Fatal("the result survived Enable(false)")
	}
	if checker.UpdateAvailable() {
		t.Fatal("UpdateAvailable is true after the source was switched off")
	}
}

// Withdrawing consent WHILE a check is in flight is the case that matters, and
// the one two bare atomics get wrong: the check reads the flag as true, is
// preempted, Enable(false) empties the slot, and the check then refills it. The
// badge stays lit from a source the user just switched off — and stays lit,
// because a second Enable(false) finds nothing to change.
func TestDisablingDuringAnInFlightCheckDiscardsIt(t *testing.T) {
	// Enough rounds to hit a window measured in nanoseconds. The race is real
	// but narrow, so a handful of iterations would pass either way; at this
	// count the unfixed code failed three runs in five when this was written.
	// A probabilistic test can only produce a false PASS here, never a false
	// failure, which is the right direction to be imperfect in — and the
	// ordering it samples is asserted directly by the test below.
	const rounds = 60000

	for round := range rounds {
		released := make(chan struct{})
		answered := make(chan struct{})
		// parked fires each time the loop asks for its next delay, which it
		// does only after publish has returned. Waiting on it is what makes
		// the assertion below observe a settled checker instead of polling.
		parked := make(chan struct{}, 4)
		never := make(chan time.Time)

		checker := NewChecker(
			ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
				close(answered)
				// Hold the answer until the test is ready to race it.
				<-released
				return newerVersion, nil
			}),
			localVersion,
			CheckerOpts{After: func(time.Duration) <-chan time.Time {
				select {
				case parked <- struct{}{}:
				default:
				}
				return never
			}},
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			checker.Run(ctx)
			close(done)
		}()

		checker.Enable(true)
		checker.CheckNow()
		<-answered
		<-parked // the wait that the kick interrupted

		// The withdrawal and the answer are released together, so the
		// scheduler decides which of them lands first.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			checker.Enable(false)
		}()
		close(released)
		wg.Wait()
		<-parked // publish has returned

		_, published := checker.Latest()
		cancel()
		<-done

		if published {
			t.Fatalf("round %d: a result landed after consent was withdrawn", round)
		}
		if checker.UpdateAvailable() {
			t.Fatalf("round %d: the update signal survived the withdrawal", round)
		}
	}
}

// Withdrawing consent must ABORT the request, not merely discard its answer.
//
// Dropping the result was the first cut and it is not enough: the request
// itself is the thing that shows a third party this node's address. A check
// still waiting on a connect — which the client gives fifteen seconds — would
// otherwise go on to reach the endpoint long after the box was unticked.
func TestDisablingCancelsTheRequestInFlight(t *testing.T) {
	started := make(chan struct{})
	observed := make(chan error, 1)
	never := make(chan time.Time)

	checker := NewChecker(
		ReleaseSourceFunc(func(ctx context.Context) (domain.ReleaseVersion, error) {
			close(started)
			// Stand in for a request that has left but not yet answered.
			select {
			case <-ctx.Done():
				observed <- ctx.Err()
				return domain.ReleaseVersion{}, ctx.Err()
			case <-time.After(2 * time.Second):
				observed <- nil
				return newerVersion, nil
			}
		}),
		localVersion,
		CheckerOpts{After: func(time.Duration) <-chan time.Time { return never }},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	checker.Enable(true)
	checker.CheckNow()
	<-started

	checker.Enable(false)

	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the request in flight ended with %v, want it cancelled by the withdrawal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request in flight was never cancelled")
	}
	if _, ok := checker.Latest(); ok {
		t.Fatal("the cancelled check published a result")
	}
}

// The gate and the request start under the same lock the withdrawal takes, so
// there is no window between "consent still holds" and "the request left".
func TestNoRequestStartsAfterConsentIsWithdrawn(t *testing.T) {
	const rounds = 20000

	for round := range rounds {
		asked := make(chan struct{}, 1)
		never := make(chan time.Time)

		checker := NewChecker(
			ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
				asked <- struct{}{}
				return newerVersion, nil
			}),
			localVersion,
			CheckerOpts{After: func(time.Duration) <-chan time.Time { return never }},
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			checker.Run(ctx)
			close(done)
		}()

		checker.Enable(true)
		checker.CheckNow()
		// Racing the loop's wake-up: the request must either have left before
		// the withdrawal, or not leave at all.
		checker.Enable(false)

		time.Sleep(time.Microsecond)
		cancel()
		<-done

		// Whether the request went out is timing; what must never happen is a
		// result standing after the withdrawal.
		if _, ok := checker.Latest(); ok {
			t.Fatalf("round %d: a result stands after consent was withdrawn", round)
		}
		select {
		case <-asked:
		default:
		}
	}
}

// The ordering itself, asserted rather than sampled: a publish in progress and
// a withdrawal must not interleave.
//
// This reaches for the mutex, which is normally implementation rather than
// behaviour — but "these two operations do not overlap" has no other
// deterministic observation from outside, and the race above can only sample
// it. Holding the lock stands in for "a check is in the middle of publishing".
func TestWithdrawalWaitsForAPublishInProgress(t *testing.T) {
	checker := NewChecker(
		ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
			return newerVersion, nil
		}),
		localVersion,
		CheckerOpts{},
	)

	checker.publishMu.Lock()

	done := make(chan struct{})
	go func() {
		checker.Enable(false)
		close(done)
	}()

	select {
	case <-done:
		checker.publishMu.Unlock()
		t.Fatal("Enable(false) ran while a publish held the lock: the two can interleave")
	case <-time.After(50 * time.Millisecond):
	}

	checker.publishMu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Enable(false) never completed after the publish finished")
	}
}

// The second half of the same bug, and the half that made it permanent.
//
// An in-flight check can leave the checker holding a result behind a flag that
// is already false. The user's response to a badge that will not go out is to
// untick the box again — and an Enable that returns early when the flag has not
// changed does nothing at all with it.
func TestDisablingAgainClearsAResultLeftBehind(t *testing.T) {
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	runChecker(t, checker)

	checker.Enable(true)
	checker.CheckNow()
	source.awaitAnswer(t)
	awaitResult(t, checker)

	// The state the race leaves behind: flag down, result still there. Set
	// directly because reproducing it through the loop is the other test's
	// job, and this one is about what the NEXT withdrawal does.
	checker.enabled.Store(false)
	if _, ok := checker.Latest(); !ok {
		t.Fatal("the test failed to set up the state it means to check")
	}

	checker.Enable(false)

	if _, ok := checker.Latest(); ok {
		t.Fatal("unticking the box a second time left the stale result standing")
	}
	if checker.UpdateAvailable() {
		t.Fatal("the update signal survived a second withdrawal")
	}
}

func TestTheScheduleIsDelayedFirstThenPeriodic(t *testing.T) {
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	runChecker(t, checker)
	checker.Enable(true)
	checker.CheckNow()
	source.awaitAnswer(t)

	// The manual/consent path took the first slot; the loop now parks on the
	// interval and each tick is one more request.
	delays.fire(t)
	source.awaitAnswer(t)
	delays.fire(t)
	source.awaitAnswer(t)

	if got := source.callCount(); got != 3 {
		t.Fatalf("the source was asked %d times, want 3", got)
	}

	observed := delays.delays()
	if len(observed) == 0 {
		t.Fatal("the loop never asked for a delay")
	}
	if observed[0] != time.Minute {
		t.Fatalf("first delay = %s, want the start delay", observed[0])
	}
	for _, d := range observed[1:] {
		if d != time.Hour {
			t.Fatalf("later delay = %s, want the interval", d)
		}
	}
}

func TestCheckNowDoesNotQueueASecondRequest(t *testing.T) {
	// One resource, one processing path: the button wakes the loop, it never
	// starts a request of its own.
	source := newCountingSource(newerVersion)
	delays := newScriptedDelays()
	checker := newTestChecker(source.source(), delays, nil)
	checker.Enable(true)
	// Three presses before the loop even starts must produce one request, not
	// three: the kick is a request for "check at the next opportunity", not a
	// queue.
	checker.CheckNow()
	checker.CheckNow()
	checker.CheckNow()

	runChecker(t, checker)
	source.awaitAnswer(t)

	// Nothing else may be pending: the next request must come from a tick.
	select {
	case <-source.answered:
		t.Fatal("a second request was queued by repeated CheckNow presses")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDefaultsAreAppliedForZeroOptions(t *testing.T) {
	checker := NewChecker(ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
		return localVersion, nil
	}), localVersion, CheckerOpts{})

	if checker.interval != DefaultInterval {
		t.Fatalf("interval = %s, want %s", checker.interval, DefaultInterval)
	}
	if checker.startDelay != DefaultStartDelay {
		t.Fatalf("startDelay = %s, want %s", checker.startDelay, DefaultStartDelay)
	}
	if checker.after == nil || checker.now == nil {
		t.Fatal("the time providers were left nil")
	}
}
