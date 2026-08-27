//go:build darwin

package desktop

import (
	"context"
	"errors"
	"os"
	"time"

	"gioui.org/app"
	"gioui.org/io/event"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// deepLinkCourierWait bounds a courier's whole life. It is waiting for
// an Apple Event the system has already queued, so this is a backstop
// for "no link was coming after all", not a delivery budget.
const deepLinkCourierWait = 5 * time.Second

// beginDeepLinkDelivery is the early half, and it runs before ANY state
// is opened or swept: a process that turns out to be a courier must not
// touch the data directory it is about to leave, and the process that
// stays must own the socket before it opens that directory.
//
// macOS delivers a clicked link to the bundle LaunchServices registered,
// which need not be the instance that is running — during development it
// usually is not (`go run` builds a binary in a temp directory, and
// LaunchServices cannot see it). Left alone, that launch would open a
// SECOND node on the same identity, chat log and port. So a launch that
// finds an owner does not start: it waits for the URL event, hands it
// over the socket and exits.
//
// Unless it was started from a terminal. There the second instance is a
// developer's deliberate act — two builds, one data directory — and this
// build has always allowed it; a courier would silently kill that
// workflow. LaunchServices gives its children no controlling terminal,
// which is exactly the launch that opens links.
//
// Returns the bound listener (nil when this process could not take the
// address) and whether this process is DONE. It may also never return —
// a courier exits the process from the event loop below.
func beginDeepLinkDelivery(ctx context.Context, dataDir, scope string) (*deeplink.LocalListener, bool) {
	socketPath := deeplink.SocketPath(dataDir, scope)
	// A developer may also start the binary by hand with the URI on its
	// command line; macOS itself never does.
	raw, hasLink := deeplink.FromArgs(os.Args[1:])
	if hasLink && forwardDeepLink(ctx, socketPath, raw) {
		return nil, true
	}

	listener, err := deeplink.Listen(ctx, socketPath)
	switch {
	case errors.Is(err, deeplink.ErrAlreadyServing):
		if hasLink && forwardDeepLink(ctx, socketPath, raw) {
			return nil, true
		}
		if hasControllingTerminal() {
			log.Info().Msg("another instance owns the deep link socket; starting anyway (launched from a terminal)")
			return nil, false
		}
		runDeepLinkCourier(ctx, socketPath)
		return nil, true
	case err != nil:
		log.Warn().Err(err).Msg("deep link socket unavailable; links will start a new instance instead")
		return nil, false
	default:
		log.Info().Str("socket", listener.Path()).Msg("deep link socket listening")
		return listener, false
	}
}

// runDeepLinkCourier waits for the link this process was launched to
// open, hands it to the instance that owns the data directory and exits.
// It never returns: app.Events does not, and every way out of the errand
// is an exit.
//
// The event loop is the whole reason a courier exists on macOS: the URI
// is not on the command line, it arrives as an Apple Event that only a
// running NSApplication receives.
func runDeepLinkCourier(ctx context.Context, socketPath string) {
	log.Info().Str("socket", socketPath).Msg("this data directory has an owner; running as a deep link courier")

	go func() {
		select {
		case <-time.After(deepLinkCourierWait):
			log.Info().Msg("no link arrived; leaving the data directory to its owner")
		case <-ctx.Done():
		}
		os.Exit(0)
	}()

	app.Events(func(e event.Event) bool {
		opened, ok := e.(app.URLEvent)
		if !ok {
			return true
		}
		if !forwardDeepLink(ctx, socketPath, opened.URL.String()) {
			// The owner did not take it. Saying so is all a process with
			// no window can do — and it beats opening a second node.
			log.Warn().Msg("the running instance did not take the link")
			os.Exit(1)
		}
		os.Exit(0)
		return false
	})
}
