//go:build linux && !android

package desktop

import (
	"context"
	"errors"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// beginDeepLinkDelivery is the early half, and it runs before ANY state
// is opened or swept: a process that turns out to be a courier must not
// touch the data directory it is about to leave, and the process that
// stays must own the socket before it opens that directory — otherwise a
// launch racing this one finds no listener and starts a second node.
//
// X11 and Wayland put the URI on the command line, so a courier here
// knows its errand immediately and needs no event loop for it (unlike
// the macOS twin).
//
// A launch that finds an owner does not open the shared state at all —
// not even when the hand-over fails — unless it came from a terminal,
// where a second instance is a developer's deliberate act.
//
// Returns the bound listener (nil when this process could not take the
// address) and whether this process is DONE: true means the link was
// handed to the instance already running.
func beginDeepLinkDelivery(ctx context.Context, dataDir, scope string) (*deeplink.LocalListener, bool) {
	socketPath := deeplink.SocketPath(dataDir, scope)
	raw, hasLink := deeplink.FromArgs(os.Args[1:])
	if hasLink && forwardDeepLink(ctx, socketPath, raw) {
		return nil, true
	}

	listener, err := deeplink.Listen(ctx, socketPath)
	switch {
	case errors.Is(err, deeplink.ErrAlreadyServing):
		// Somebody bound the address between the dial above and this
		// bind, or was already running and simply did not answer in
		// time. Either way there IS an instance now, so the link gets a
		// second chance to reach it.
		if hasLink && forwardDeepLink(ctx, socketPath, raw) {
			return nil, true
		}
		// The owner is a proven fact by now, and the decision below does
		// not depend on whether the hand-over worked: a malformed link
		// or a failed acknowledgement is no reason to open a second node
		// on this identity, chat log and port.
		if hasControllingTerminal() {
			log.Info().Msg("another instance owns the deep link socket; starting anyway (launched from a terminal)")
			return nil, false
		}
		if hasLink {
			log.Warn().Msg("the link was not handed over and this data directory has an owner; not starting a second node")
		} else {
			log.Info().Msg("this data directory has an owner; not starting a second node")
		}
		return nil, true
	case err != nil:
		log.Warn().Err(err).Msg("deep link socket unavailable; links will start a new instance instead")
		return nil, false
	default:
		log.Info().Str("socket", listener.Path()).Msg("deep link socket listening")
		return listener, false
	}
}
