//go:build !android

package main

import "gioui.org/app"

// dataDirStrict — non-Android builds of this entry point (used for
// cross-compile type checking) keep the lenient behaviour: on failure
// anchorDataDir falls through to appdata's platform defaults, which are
// correct on desktops. See datadir_android.go for the fail-closed
// rationale.
const dataDirStrict = false

// platformDataDir returns the base directory for app data. Non-Android
// builds just defer to Gio's DataDir — the user config dir.
func platformDataDir() (string, error) {
	return app.DataDir()
}

// migrateLegacyDataDir is Android-only (see datadir_android.go); no-op
// elsewhere.
func migrateLegacyDataDir(string) error {
	return nil
}
