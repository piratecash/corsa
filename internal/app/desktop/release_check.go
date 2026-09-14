package desktop

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/updatecheck"
)

// release_check.go is the desktop half of the opt-in GitHub release check: the
// checker's lifetime, the preference that gates it, and the controls the
// Settings tab drives it with.
//
// Only the UI has one. A headless node has nobody to ask for consent, so
// nothing under cmd/corsa-node reaches this package — the peer-based version
// policy remains its only update signal, exactly as before.

// releasesPageURL is where the update badge sends the user. The tag listing the
// check reads is a different endpoint (updatecheck.DefaultTagsURL); this one is
// for a human.
const releasesPageURL = "https://github.com/piratecash/corsa/releases"

// newReleaseChecker builds the checker, already carrying the running version to
// compare against. onResult is asked for a frame after each completed check.
//
// It takes the callback rather than the *Window because the callback is the
// only thing it needs: handing a component the whole window is how a component
// ends up reaching state nobody expected it to.
//
// The checker is constructed whether or not the preference is on: a checker
// that exists but is disabled sends nothing (see updatecheck.Checker), and
// building it lazily on the first tick would leave the Settings tab with no
// state to report.
func newReleaseChecker(onResult func()) *updatecheck.Checker {
	local, err := domain.ParseReleaseVersion(config.CorsaVersion)
	if err != nil {
		// The running version is a compile-time constant, and
		// TestTheRunningVersionIsComparable holds it to that — so this branch
		// is unreachable in a built binary. If it is ever reached there is
		// nothing to compare against and the check could only produce noise:
		// go without it, and leave the peer signal alone.
		log.Error().Err(err).Str("version", config.CorsaVersion).Msg("release check disabled: unparsable local version")
		return nil
	}

	return updatecheck.NewChecker(updatecheck.NewClient(updatecheck.ClientOpts{}), local, updatecheck.CheckerOpts{
		// A completed check changes the header badge and the Settings tab, and
		// it completes on the checker's own goroutine — nothing else would ask
		// for the frame that shows it.
		OnResult: func(updatecheck.Result) { onResult() },
	})
}

// RunReleaseChecker drives the release check for the application's lifetime. It
// is the caller's goroutine; ctx cancellation stops it.
//
// Nothing leaves the machine unless the preference is on: Run parks on its
// timer either way and withholds the request.
func (w *Window) RunReleaseChecker(ctx context.Context) {
	if w.releaseChecker == nil {
		return
	}
	w.releaseChecker.Run(ctx)
}

// releaseCheckEnabled is the persisted consent, and the single source of truth
// for it: the checkbox reads this rather than keeping a copy that could drift
// from what the checker is actually doing.
func (w *Window) releaseCheckEnabled() bool {
	return w.prefs != nil && w.prefs.CheckGitHubReleases
}

// setReleaseCheckEnabled records the consent and applies it.
//
// Consent is what is WRITTEN DOWN, which is what decides the two directions
// when the write fails:
//
//   - turning it ON is abandoned. The preference did not reach the disk, so the
//     next start would come up without it, and running checks in the meantime
//     would be checking on a consent the machine does not hold. The box springs
//     back unticked, which is the user's signal that it did not take.
//   - turning it OFF applies regardless. Withdrawal is the safe direction and
//     must never be blocked by a disk error; the file keeping the old value
//     means the next start re-enables it, which is the honest consequence of
//     the write that failed, not something to compound by leaving the checker
//     running now.
func (w *Window) setReleaseCheckEnabled(enabled bool) {
	defer w.invalidate()

	if w.prefs != nil {
		previous := w.prefs.CheckGitHubReleases
		w.prefs.CheckGitHubReleases = enabled
		if err := w.prefs.Save(); err != nil {
			log.Warn().Err(err).Bool("enabled", enabled).Msg("saving the release-check preference failed")
			if enabled {
				w.prefs.CheckGitHubReleases = previous
				return
			}
		}
	}

	if w.releaseChecker == nil {
		return
	}
	w.releaseChecker.Enable(enabled)
	if enabled {
		// Consent is also the request: a checkbox that reports nothing for the
		// length of the start delay reads as broken. The delay exists to keep
		// an UNASKED-for check away from process start, which this is not.
		w.releaseChecker.CheckNow()
	}
}

// releaseCheckNow runs the check ahead of its schedule. It does nothing while
// the preference is off — the button is only drawn then anyway, but a control
// that can be reached by keyboard should not be the one place consent is
// bypassed.
func (w *Window) releaseCheckNow() {
	if w.releaseChecker == nil || !w.releaseCheckEnabled() {
		return
	}
	w.releaseChecker.CheckNow()
}
