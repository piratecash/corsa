package desktop

import (
	"context"
	"errors"

	"gioui.org/app"
	"gioui.org/io/system"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// deeplink.go is the application half of the corsa: URI family: the
// operating system hands the link over on a thread of its own (an Apple
// Event callback, the Win32 message pump, the Android UI thread, the
// local socket of a second launch), the inbox carries it to the frame
// goroutine, and the router decides WHICH member it is before anything
// acts on it. Contact links import; a member this build does not know
// says so instead of failing as "malformed".

// errContactLinkRefused marks an import the UI has already explained in
// the status line. The router only needs to know the handler failed.
var errContactLinkRefused = errors.New("contact link refused")

// deepLinkRouter is the kind → handler table. A new member is one entry
// here plus its handler — no change to the delivery paths, the inbox or
// the platform wiring.
//
// Built on first use rather than in NewWindow: the table binds to this
// window, and a window assembled field by field (every test in this
// package does) must route the links it is handed just the same.
// Layout-goroutine only, like every caller.
func (w *Window) deepLinkRouter() *deeplink.Router {
	if w.deepLinkRoutes != nil {
		return w.deepLinkRoutes
	}
	routes, err := deeplink.NewRouter(map[deeplink.Kind]deeplink.Handler{
		deeplink.KindContact: deeplink.HandlerFunc(w.handleContactDeepLink),
	})
	if err != nil {
		log.Error().Err(err).Msg("deep link routing table is unbuildable")
		return nil
	}
	w.deepLinkRoutes = routes
	return routes
}

// handleContactDeepLink runs the same verify-then-import path a pasted
// link takes — one authority for what a contact link means, whichever
// door it arrived through.
func (w *Window) handleContactDeepLink(_ context.Context, link deeplink.Link) error {
	if !w.importContactLink(link.URI) {
		return errContactLinkRefused
	}
	return nil
}

// enqueueDeepLink accepts a URI from ANY goroutine — including the
// platform's own callback thread. It is the only entry point platform
// code uses.
func (w *Window) enqueueDeepLink(raw string) {
	w.deepLinks.Push(raw)
	w.requestDeepLinkFrame()
}

// requestDeepLinkFrame asks for the frame that drains the inbox: the
// link usually arrives while the app sits idle with no frame scheduled.
//
// A signal, never an Invalidate — see the deepLinkWake field for the
// deadlock that buys. Non-blocking, so the caller (an Apple Event
// callback, a socket accept loop) is never held up, and safe on a window
// assembled without NewWindow: a send on a nil channel is never ready,
// and the UI heartbeat still picks the link up.
func (w *Window) requestDeepLinkFrame() {
	select {
	case w.deepLinkWake <- struct{}{}:
	default:
	}
}

// handleDeepLinks routes everything the inbox collected. Called from
// layout, on the goroutine that owns the UI state — the same contract
// reloadStaleReactions has.
func (w *Window) handleDeepLinks() {
	pending := w.deepLinks.Drain()
	if len(pending) == 0 {
		return
	}
	for _, raw := range pending {
		w.routeDeepLink(raw)
	}
	// A link is an explicit user action taken somewhere else — the
	// browser, another app — so the window comes forward to show what
	// happened to it. Gio's Windows driver already raises on its own
	// relay path; a second request there is a no-op.
	if w.window != nil {
		w.window.Perform(system.ActionRaise)
	}
}

func (w *Window) routeDeepLink(raw string) {
	routes := w.deepLinkRouter()
	if routes == nil {
		w.router.SetSendStatus(w.t("status.deeplink_unsupported"))
		return
	}
	// A frame is the deadline here: handlers run on the layout goroutine
	// and must not block, exactly as the paste path's import does not.
	// The context is what a member that DOES reach the network or the
	// disk will need, and it is created here because a frame has no
	// ambient one to inherit.
	link, err := routes.Handle(context.Background(), raw)
	switch {
	case err == nil:
		log.Info().Str("kind", link.Kind.String()).Msg("deep link handled")
	case errors.Is(err, errContactLinkRefused):
		// importContactLink already wrote the reason to the status line.
		log.Info().Str("kind", link.Kind.String()).Msg("deep link refused by its handler")
	case errors.Is(err, deeplink.ErrUnsupportedKind):
		w.router.SetSendStatus(w.t("status.deeplink_unsupported"))
		log.Warn().Str("kind", link.Kind.String()).Msg("deep link names a kind this build has no handler for")
	default:
		// Classification failed: the URI is not a member of the family
		// at all, so no member's wording fits it.
		w.router.SetSendStatus(w.t("status.deeplink_rejected", err.Error()))
		log.Warn().Err(err).Msg("deep link rejected")
	}
}

// noteLaunchDeepLink queues the URI the process was STARTED with, read
// from the platform itself, and filters out an ordinary command line
// that carries no link at all.
func (w *Window) noteLaunchDeepLink(raw string) {
	if !deeplink.IsDeepLink(raw) {
		// The ordinary case: a command line with no URI on it.
		return
	}
	log.Info().Msg("launched with a deep link")
	w.deepLinks.Push(raw)
	w.requestDeepLinkFrame()
}

// readLaunchDeepLinkOnce asks the platform for the URI this process was
// started with, at the first moment a native view exists. Only Android
// answers (deeplink_launch_android.go): everywhere else the launch link
// arrives either on the command line (desktop.Run) or through Gio's own
// URL event.
func (w *Window) readLaunchDeepLinkOnce(e app.ViewEvent) {
	if w.launchDeepLinkRead {
		return
	}
	raw, read := platformLaunchDeepLink(e)
	if !read {
		// No view to ask yet; the next attach gets another chance.
		return
	}
	w.launchDeepLinkRead = true
	w.noteLaunchDeepLink(raw)
}
