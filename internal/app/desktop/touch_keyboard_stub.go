//go:build !windows

package desktop

import "gioui.org/app"

// showPlatformTouchKeyboard is a no-op on platforms where the OS (or Gio's
// backend) shows the on-screen keyboard itself; the per-window occlusion
// state then permanently reports zero.
func showPlatformTouchKeyboard(*touchKeyboardState, uintptr, int64, bool) {}

// hidePlatformTouchKeyboard is a no-op on non-Windows platforms.
func hidePlatformTouchKeyboard(*touchKeyboardState, int64) {}

// platformKeyboardClosing reports no in-flight hide on non-Windows platforms
// (the OS drives the keyboard there), so the show debounce is never bypassed.
func platformKeyboardClosing() bool { return false }

// platformActiveWindowHandle reports no native handle on non-Windows
// platforms, which short-circuits the show request path.
func platformActiveWindowHandle() uintptr { return 0 }

// platformReleaseKeyboardEvents is a no-op on non-Windows platforms.
func platformReleaseKeyboardEvents(kbd *touchKeyboardState) { kbd.released.Store(true) }

// platformViewHWND reports no native handle on non-Windows platforms.
func platformViewHWND(app.ViewEvent) uintptr { return 0 }

// platformBindKeyboardWindow is a no-op on non-Windows platforms (the OS
// drives the keyboard and occlusion itself there).
func platformBindKeyboardWindow(*touchKeyboardState, uintptr) {}
