//go:build corsadebug

package crashlog

import "github.com/rs/zerolog"

// logcatDefaultLevel in corsadebug builds (make build-android-debug):
// info — the level a developer watching `adb logcat` wants. Release
// builds default to warn; see logcat_level_release.go.
const logcatDefaultLevel = zerolog.InfoLevel
