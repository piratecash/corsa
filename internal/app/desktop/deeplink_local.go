//go:build (linux && !android) || darwin

package desktop

import (
	"context"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// deeplink_local.go is the half shared by every desktop where a clicked
// link can start a SECOND process against a data directory that already
// has an owner: X11 and Wayland do it because they cannot deliver into a
// running program at all, macOS does it whenever the running instance is
// not the bundle LaunchServices has registered (a `go run` build during
// development, or a copy started from another path). Either way the
// newcomer hands the link to the owner over the local socket instead of
// opening a second node on one identity.

// forwardDeepLink reports whether the link was taken by another instance.
func forwardDeepLink(ctx context.Context, socketPath, raw string) bool {
	delivered, err := deeplink.Forward(ctx, socketPath, raw)
	if err != nil {
		// Somebody answered but the exchange failed. The link's fate is
		// unknown, so the caller decides what to do with this process;
		// nothing here is lost silently.
		log.Warn().Err(err).Msg("deep link forward failed")
		return false
	}
	if delivered {
		log.Info().Msg("deep link handed to the running instance")
	}
	return delivered
}

// startDeepLinkDelivery is the late half: the window now exists, so the
// link this process was started with and everything the socket accepted
// meanwhile can be acted on.
func startDeepLinkDelivery(listener *deeplink.LocalListener, window *Window) {
	// A plain push: a command line is delivered once and never repeated,
	// unlike the Android launch intent.
	if raw, ok := deeplink.FromArgs(os.Args[1:]); ok {
		window.enqueueDeepLink(raw)
	}
	if listener == nil {
		return
	}
	listener.Deliver(func(link deeplink.Link) {
		window.enqueueDeepLink(link.URI)
	})
}

// hasControllingTerminal separates a launch by a person at a shell from
// one by the desktop environment — and the desktop environment is what
// launches the program to open a link.
//
// It decides one thing: whether a process that finds an owner for this
// data directory may start anyway. From a terminal it may — running two
// builds against one directory is a deliberate act this build has always
// allowed. From Finder, from a .desktop entry, from xdg-open it may not:
// nobody asked for a second node on one identity, and the link that
// caused the launch belongs to the instance already running.
//
// /dev/tty is the controlling terminal itself: opening it fails with
// ENXIO for a process that has none. Stdin's type cannot answer this —
// a desktop launcher hands its children /dev/null, which is a character
// device just like a terminal.
func hasControllingTerminal() bool {
	tty, err := os.OpenFile("/dev/tty", os.O_RDONLY, 0)
	if err != nil {
		return false
	}
	_ = tty.Close()
	return true
}
