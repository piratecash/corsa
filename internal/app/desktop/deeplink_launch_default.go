//go:build !android

package desktop

import "gioui.org/app"

// platformLaunchDeepLink has nothing to read outside Android.
//
// macOS delivers the launch URL as an Apple Event once the run loop is
// up, Windows relays it through Gio's own startup handling, and X11 /
// Wayland desktops pass it on the command line, which desktop.Run reads
// before any of this exists. See the Android twin for the case that
// needs a platform query.
func platformLaunchDeepLink(app.ViewEvent) (uri string, read bool) { return "", true }
