//go:build !corsadebug

package crashlog

import "github.com/rs/zerolog"

// logcatDefaultLevel is the steady-state logcat level for release
// builds. Warn, same as the desktop default: the network core logs
// identities, peer addresses and message/file IDs at info, which is
// metadata leakage into logcat (readable by adb and pre-API-30 apps
// with READ_LOGS) plus steady battery/IO cost. `make
// build-android-debug` (build tag corsadebug) flips this to info — see
// logcat_level_debug.go.
const logcatDefaultLevel = zerolog.WarnLevel
