//go:build !((linux && !android) || darwin)

package desktop

import (
	"context"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// Windows and Android need no local socket. Android hands the intent to
// the activity, and on Windows Gio does this job itself: with a scheme
// declared it finds the running window by its class, relays the URI with
// WM_COPYDATA and exits the new process. See the Linux and macOS twins.

func beginDeepLinkDelivery(context.Context, string, string) (*deeplink.LocalListener, bool) {
	return nil, false
}

func startDeepLinkDelivery(*deeplink.LocalListener, *Window) {}
