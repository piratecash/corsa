package crashlog

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// setupLogcat configures logging for Android, where the debugging
// surface is `adb logcat`, not files on app-private storage.
//
// Gio's Android glue pipes the process stdout/stderr into logcat (the
// tag is the app ID, wired by gogio through
// gioui.org/app/internal/log.appID), so it is enough to keep zerolog on
// stderr and NOT touch the descriptors:
//
//   - no corsa.log / shrink / rotation — logcat has its own ring buffer;
//   - no fd-2 redirect (redirectStderr): stealing fd 2 would divert Go
//     runtime fatal errors AND panics into an invisible stderr.log,
//     which is exactly the opposite of what logcat debugging needs.
//
// Crash reports from recovered panics still go to crash-*.log files in
// the data dir (recoverAndLog) — they complement logcat rather than
// replacing it, and the re-panic output itself lands in logcat.
//
// The steady-state default level is build-selected (logcat_level_*.go):
// warn in release — the network core logs identities, addresses and
// message/file IDs at info, which is metadata leakage into logcat plus
// steady battery/IO cost — and info under the corsadebug build tag
// (`make build-android-debug`), since CORSA_LOG_LEVEL cannot practically
// reach an Android app process. NoColor — logcat stores raw bytes and
// ANSI escapes would only litter it.
func setupLogcat() func() {
	cw := zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true, TimeFormat: "15:04:05"}
	log.Logger = zerolog.New(cw).With().Timestamp().Caller().Logger()

	level := zerolog.Level(logcatDefaultLevel)
	if envLevel := os.Getenv("CORSA_LOG_LEVEL"); envLevel != "" {
		if parsed, err := zerolog.ParseLevel(envLevel); err == nil {
			level = parsed
		}
	}
	// Same banner contract as Setup: emit the startup line with the gate
	// forced open, then apply the steady-state level.
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Info().Str("level", level.String()).Msg("logging to logcat (android)")
	zerolog.SetGlobalLevel(level)

	dir := logDir()
	return func() { recoverAndLog(dir) }
}
