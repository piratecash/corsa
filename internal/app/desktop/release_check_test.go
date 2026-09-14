package desktop

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
	"github.com/piratecash/corsa/internal/core/updatecheck"
)

var (
	testLocalVersion  = domain.ReleaseVersion{Major: 2, Minor: 3, Build: 69}
	testNewerVersion  = domain.ReleaseVersion{Major: 2, Minor: 3, Build: 70}
	errCheckUnreached = errors.New("no route to host")
)

// attachReleaseChecker gives the window a checker whose only trigger is
// CheckNow — its timer never fires — and runs it for the test's lifetime. It
// returns once the first check has been published, so the caller can assert on
// a settled state rather than poll.
func attachReleaseChecker(t *testing.T, w *Window, published domain.ReleaseVersion, failure error) {
	t.Helper()

	never := make(chan time.Time)
	published2, failure2 := published, failure
	settled := make(chan struct{}, 1)

	w.releaseChecker = updatecheck.NewChecker(
		updatecheck.ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
			return published2, failure2
		}),
		testLocalVersion,
		updatecheck.CheckerOpts{
			OnResult: func(updatecheck.Result) {
				select {
				case settled <- struct{}{}:
				default:
				}
			},
			After: func(time.Duration) <-chan time.Time { return never },
			Now:   func() time.Time { return time.Unix(1700000000, 0).UTC() },
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.releaseChecker.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	w.releaseChecker.Enable(true)
	w.releaseChecker.CheckNow()
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		t.Fatal("the release check never completed")
	}
}

// The badge answers to EITHER source. Neither subsumes the other: the peer
// signal needs several peers on a newer build before it concludes anything and
// says nothing at all to a node with no peers, while the release check answers
// at once but only for a user who opted in.
func TestUpdateBadgeReadsBothSources(t *testing.T) {
	t.Run("neither source", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		if w.updateAvailable() {
			t.Fatal("the badge is lit with no signal at all")
		}
	})

	t.Run("peers only", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		w.snap.NodeStatus.AggregateStatus = &service.AggregateStatus{UpdateAvailable: true}
		if !w.updateAvailable() {
			t.Fatal("the peer signal did not light the badge")
		}
	})

	t.Run("release check only", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, testNewerVersion, nil)
		if w.nodeUpdateAvailable() {
			t.Fatal("the peer signal is set; this case is meant to isolate the release check")
		}
		if !w.updateAvailable() {
			t.Fatal("the release check did not light the badge")
		}
	})

	t.Run("release check finds nothing newer", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, testLocalVersion, nil)
		if w.updateAvailable() {
			t.Fatal("the badge is lit although the published release is the running one")
		}
	})

	// A failure says nothing about whether a release exists. Lighting the
	// badge on one would turn every flaky connection into an update prompt.
	t.Run("release check failed", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, domain.ReleaseVersion{}, errCheckUnreached)
		if w.updateAvailable() {
			t.Fatal("a failed check lit the badge")
		}
	})
}

// Consent is persisted before it is acted on, so a process that dies in
// between comes back with the setting the user last saw.
func TestTogglingTheReleaseCheckPersistsConsent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json.desktop.json")
	w := newIdentityLayoutTestWindow(t)
	w.prefs = &Preferences{path: path}
	attachReleaseChecker(t, w, testNewerVersion, nil)
	// attachReleaseChecker enabled the checker directly to settle a result;
	// the preference itself is still at its default.
	if w.releaseCheckEnabled() {
		t.Fatal("the preference defaults to on")
	}

	w.setReleaseCheckEnabled(true)

	if !w.releaseCheckEnabled() {
		t.Fatal("ticking the box did not record consent")
	}
	if !readSavedConsent(t, path) {
		t.Fatal("consent was not written to the preferences file")
	}

	w.setReleaseCheckEnabled(false)

	if w.releaseCheckEnabled() {
		t.Fatal("unticking the box did not withdraw consent")
	}
	if readSavedConsent(t, path) {
		t.Fatal("withdrawn consent was not written to the preferences file")
	}
	// Withdrawing consent takes the badge down with it: the conclusion came
	// from a source the user has just switched off.
	if w.releaseUpdateAvailable() {
		t.Fatal("the release-check signal survived the box being unticked")
	}
}

// Consent is what is written down, so a failed write is a failed consent.
// Applying it anyway would run checks on an agreement the machine does not
// hold, and the next start — reading the file that was never updated — would
// come up with the box unticked and no explanation for the requests that went
// out in between.
//
// Withdrawal is the other direction and applies regardless: it is the safe one,
// and refusing to act on it because a disk write failed would be the worst of
// both.
func TestAFailedWriteDoesNotGrantConsent(t *testing.T) {
	// A directory where the preferences file should be: Save cannot write it.
	dir := t.TempDir()
	path := filepath.Join(dir, "prefs.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("set up an unwritable preferences path: %v", err)
	}

	w := newIdentityLayoutTestWindow(t)
	w.prefs = &Preferences{path: path}
	attachReleaseChecker(t, w, testNewerVersion, nil)
	w.releaseChecker.Enable(false)

	w.setReleaseCheckEnabled(true)

	if w.releaseCheckEnabled() {
		t.Fatal("consent was granted although it could not be written down")
	}
	if w.releaseChecker.Enabled() {
		t.Fatal("the checker was switched on although consent could not be written down")
	}

	// Withdrawal still applies with the same unwritable path.
	w.prefs.CheckGitHubReleases = true
	w.releaseChecker.Enable(true)

	w.setReleaseCheckEnabled(false)

	if w.releaseCheckEnabled() {
		t.Fatal("withdrawal was refused because the write failed")
	}
	if w.releaseChecker.Enabled() {
		t.Fatal("the checker kept running after a withdrawal whose write failed")
	}
}

// The running version is a compile-time constant, so the branch that gives up
// on an unparsable one is unreachable in a built binary — as long as something
// holds the constant to it. This is that something: a release tagged
// "2.3.69-rc1" would otherwise ship with the release check quietly disabled.
func TestTheRunningVersionIsComparable(t *testing.T) {
	if _, err := domain.ParseReleaseVersion(config.CorsaVersion); err != nil {
		t.Fatalf("config.CorsaVersion %q does not parse as a release version: %v", config.CorsaVersion, err)
	}
	if newReleaseChecker(func() {}) == nil {
		t.Fatal("newReleaseChecker gave up on the running version")
	}
}

func readSavedConsent(t *testing.T, path string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("preferences were not written: %v", err)
	}
	var saved struct {
		CheckGitHubReleases bool `json:"check_github_releases"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("decode preferences: %v", err)
	}
	return saved.CheckGitHubReleases
}

// The manual button must not be a way past the consent gate. It is only drawn
// while the box is ticked, but it is reachable by keyboard.
//
// Two gates are asserted, because they fail independently. The checker's own
// flag is the authoritative one, and the preference gate in front of it is what
// holds when the two have come apart — which is the only state in which the
// button could do any harm.
func TestCheckNowDoesNothingWithoutConsent(t *testing.T) {
	for _, checkerEnabled := range []bool{false, true} {
		w := newIdentityLayoutTestWindow(t)
		w.prefs = &Preferences{} // consent withheld

		asked := make(chan struct{}, 1)
		never := make(chan time.Time)
		w.releaseChecker = updatecheck.NewChecker(
			updatecheck.ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
				asked <- struct{}{}
				return testNewerVersion, nil
			}),
			testLocalVersion,
			updatecheck.CheckerOpts{After: func(time.Duration) <-chan time.Time { return never }},
		)
		// checkerEnabled=true is the drifted state: the checker would run the
		// request, and only the preference gate stops it.
		w.releaseChecker.Enable(checkerEnabled)

		ctx, cancel := context.WithCancel(context.Background())
		go w.releaseChecker.Run(ctx)

		w.releaseCheckNow()

		select {
		case <-asked:
			cancel()
			t.Fatalf("checker enabled=%v: the manual check ran without consent", checkerEnabled)
		case <-time.After(100 * time.Millisecond):
		}
		cancel()
	}
}

// The status block is what distinguishes "up to date" from "could not tell".
// The badge is dark in both, and only one of them supports the conclusion the
// user would otherwise draw.
func TestReleaseStatusLinesReportEachOutcome(t *testing.T) {
	t.Run("never checked", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		w.releaseChecker = updatecheck.NewChecker(
			updatecheck.ReleaseSourceFunc(func(context.Context) (domain.ReleaseVersion, error) {
				return testLocalVersion, nil
			}),
			testLocalVersion,
			updatecheck.CheckerOpts{},
		)
		lines := w.releaseCheckStatusLines()
		if len(lines) != 1 || lines[0] != w.t("settings.release_pending") {
			t.Fatalf("lines = %v, want the not-checked-yet line alone", lines)
		}
	})

	t.Run("update available", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, testNewerVersion, nil)
		assertStatusMentions(t, w, testNewerVersion.String())
	})

	t.Run("up to date", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, testLocalVersion, nil)
		assertStatusMentions(t, w, testLocalVersion.String())
	})

	// The reason is shown, not swallowed: "rate limit exceeded" and "no route
	// to host" ask the user for different things.
	t.Run("failed", func(t *testing.T) {
		w := newIdentityLayoutTestWindow(t)
		attachReleaseChecker(t, w, domain.ReleaseVersion{}, errCheckUnreached)
		assertStatusMentions(t, w, errCheckUnreached.Error())
	})
}

func assertStatusMentions(t *testing.T, w *Window, want string) {
	t.Helper()
	lines := w.releaseCheckStatusLines()
	if len(lines) != 2 {
		t.Fatalf("lines = %v, want an outcome line and a checked-at line", lines)
	}
	if !strings.Contains(lines[0], want) {
		t.Fatalf("outcome line %q does not mention %q", lines[0], want)
	}
	if !strings.Contains(lines[1], "2023-11-14") {
		t.Fatalf("checked-at line %q does not carry the timestamp", lines[1])
	}
}

// The three outcomes must each have their own sentence. A missing case would
// fall through to "not checked yet", which is the one thing that is certainly
// untrue after a check has run.
func TestEveryOutcomeHasItsOwnSentence(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	pending := w.t("settings.release_pending")

	outcomes := []updatecheck.Outcome{
		updatecheck.OutcomeUpdateAvailable,
		updatecheck.OutcomeUpToDate,
		updatecheck.OutcomeFailed,
	}
	seen := make(map[string]updatecheck.Outcome, len(outcomes))
	for _, outcome := range outcomes {
		line := releaseOutcomeLine(w, updatecheck.Result{Outcome: outcome, Latest: testNewerVersion, Err: errCheckUnreached})
		if line == pending {
			t.Fatalf("outcome %q has no sentence of its own", outcome)
		}
		if other, clash := seen[line]; clash {
			t.Fatalf("outcomes %q and %q share the line %q", other, outcome, line)
		}
		seen[line] = outcome
	}
}
