package desktop

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// TestDeepLinkRouterHandlesContactLinks pins what this build routes: a
// contact link has a handler, and a member from a newer version does
// not — which is the difference between importing a contact and telling
// the user their version is too old.
func TestDeepLinkRouterHandlesContactLinks(t *testing.T) {
	w := &Window{}

	routes := w.deepLinkRouter()
	if routes == nil {
		t.Fatal("the routing table did not build")
	}
	kinds := routes.Kinds()
	if len(kinds) != 1 || kinds[0] != deeplink.KindContact {
		t.Fatalf("kinds = %v, want [%s]", kinds, deeplink.KindContact)
	}
}

// TestDeepLinkRouterIsBuiltOnce: the table binds to this window, so a
// second call must hand back the same one rather than rebuild it every
// frame a link arrives.
func TestDeepLinkRouterIsBuiltOnce(t *testing.T) {
	w := &Window{}
	if first, second := w.deepLinkRouter(), w.deepLinkRouter(); first != second {
		t.Error("the routing table is rebuilt on every call")
	}
}

// TestNoteLaunchDeepLinkIgnoresOrdinaryArguments: the command line of a
// normal launch carries no URI, and nothing must be queued for it.
func TestNoteLaunchDeepLinkIgnoresOrdinaryArguments(t *testing.T) {
	w := &Window{}

	w.noteLaunchDeepLink("")
	w.noteLaunchDeepLink("--debug")
	w.noteLaunchDeepLink("/home/user/file.txt")

	if queued := w.deepLinks.Drain(); queued != nil {
		t.Fatalf("queued %v from a command line with no link", queued)
	}
}

// TestNoteLaunchDeepLinkQueuesTheLink: the URI a desktop entry appended
// to the command line reaches the frame goroutine.
func TestNoteLaunchDeepLinkQueuesTheLink(t *testing.T) {
	w := &Window{}

	w.noteLaunchDeepLink("corsa:group/abc?v=1")

	queued := w.deepLinks.Drain()
	if len(queued) != 1 || queued[0] != "corsa:group/abc?v=1" {
		t.Fatalf("queued %v", queued)
	}
}

// TestHandleDeepLinksIsFreeWhenIdle: it runs on every frame, so an empty
// inbox must touch nothing — no router call, no window action.
func TestHandleDeepLinksIsFreeWhenIdle(t *testing.T) {
	w := &Window{}

	w.handleDeepLinks()

	if w.deepLinkRoutes != nil {
		t.Error("an idle frame built the routing table")
	}
}

// TestReadLaunchDeepLinkOnce: a view can be recreated, and re-reading
// the launch intent on each one would import the same contact again.
func TestReadLaunchDeepLinkOnce(t *testing.T) {
	w := &Window{}

	w.readLaunchDeepLinkOnce(nil)
	if !w.launchDeepLinkRead {
		t.Fatal("the launch link was not marked as read")
	}
	w.readLaunchDeepLinkOnce(nil)

	if queued := w.deepLinks.Drain(); queued != nil {
		t.Fatalf("queued %v — outside Android there is no launch intent to read", queued)
	}
}

// TestEnqueueDeepLinkOnlySignals is the regression test for the freeze:
// a link is delivered on the platform's own thread, and the ONE thing
// that thread may not do is call Window.Invalidate — on macOS Gio then
// pumps the event loop inline, holding the window's invalidation lock,
// while the layout goroutine waits for that same lock. So the push must
// leave the window object alone and only signal.
func TestEnqueueDeepLinkOnlySignals(t *testing.T) {
	w := &Window{deepLinkWake: make(chan struct{}, 1)}

	// w.window is deliberately nil: if the push reached for it, this
	// would panic instead of signalling.
	w.enqueueDeepLink("corsa:group/abc?v=1")

	select {
	case <-w.deepLinkWake:
	default:
		t.Fatal("no frame was requested for a queued link")
	}
	if queued := w.deepLinks.Drain(); len(queued) != 1 {
		t.Fatalf("queued %v, want the link", queued)
	}
}

// TestEnqueueDeepLinkNeverBlocksThePlatformThread: the signal is a
// request for the next frame, and that frame drains every queued link —
// so a full channel is nothing to wait for.
func TestEnqueueDeepLinkNeverBlocksThePlatformThread(t *testing.T) {
	w := &Window{deepLinkWake: make(chan struct{}, 1)}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			w.enqueueDeepLink("corsa:group/abc?v=1")
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a push blocked on the wake signal")
	}
}

// TestEnqueueDeepLinkSurvivesAWindowWithoutAWakeChannel: windows built
// field by field (tests, and any future call site that forgets) must not
// panic — the heartbeat picks the link up a beat later.
func TestEnqueueDeepLinkSurvivesAWindowWithoutAWakeChannel(t *testing.T) {
	w := &Window{}

	w.enqueueDeepLink("corsa:group/abc?v=1")

	if queued := w.deepLinks.Drain(); len(queued) != 1 {
		t.Fatalf("queued %v, want the link", queued)
	}
}
