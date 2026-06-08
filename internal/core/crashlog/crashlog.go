// Package crashlog provides structured logging via zerolog, panic recovery,
// and persistent crash logging.
//
// On startup, call Setup() to configure zerolog (console + file output) and
// install a panic handler. When the application panics, the stack trace is
// written to a crash-<timestamp>.log file before the process terminates.
package crashlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	logFileName    = "corsa.log"
	stderrFileName = "stderr.log"
	crashPrefix    = "crash-"
	maxLogSize     = 10 * 1024 * 1024 // 10 MB — rotate when exceeded
	keepCrashLogs  = 10               // keep last N crash files
)

// Setup initialises structured logging via zerolog and installs a deferred
// panic handler. It configures dual output: human-friendly console (stdout)
// + JSON log file (.corsa/corsa.log).
//
// It returns a cleanup function that should be deferred by the caller
// (typically main). If the log directory cannot be created, Setup falls
// back to console-only logging and does not return an error.
//
// Usage:
//
//	func main() {
//	    cleanup := crashlog.Setup()
//	    defer cleanup()
//	    // ... rest of main
//	}
func Setup() func() {
	// Ensure all goroutine stacks are dumped on runtime fatal errors
	// (concurrent map writes, SIGSEGV, etc.). Without this, the default
	// GOTRACEBACK=single only prints the crashing goroutine.
	debug.SetTraceback("all")

	// On Windows GUI applications stdout may not exist (invalid handle).
	// Detect this early so we never pass a broken writer to zerolog.
	stdoutAvailable := isWriterUsable(os.Stdout)

	dir := logDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		if stdoutAvailable {
			cw := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}
			log.Logger = zerolog.New(cw).With().Timestamp().Caller().Logger()
		}
		log.Warn().Str("dir", dir).Err(err).Msg("cannot create log directory, falling back to console")
		return func() { recoverAndLog(dir) }
	}

	// Redirect stderr (fd 2) to a file BEFORE anything else so that Go
	// runtime fatal errors are captured even if the terminal is not visible.
	stderrFile := redirectStderr(dir)

	logPath := filepath.Join(dir, logFileName)
	rotateIfNeeded(logPath)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		if stdoutAvailable {
			cw := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}
			log.Logger = zerolog.New(cw).With().Timestamp().Caller().Logger()
		}
		log.Warn().Str("path", logPath).Err(err).Msg("cannot open log file, falling back to console")
		return func() { recoverAndLog(dir) }
	}

	var out io.Writer = f
	if stdoutAvailable {
		cw := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"}
		out = io.MultiWriter(cw, f)
	}
	log.Logger = zerolog.New(out).With().Timestamp().Caller().Logger()

	// Set the global zerolog level from CORSA_LOG_LEVEL env var.
	// Supported values: trace, debug, info, warn (default), error.
	// The default sits at warn so a freshly started daemon does not
	// flood the console with steady-state info chatter (peer-state
	// transitions, announce cycles, push-message delivery) — info
	// volume on a busy node easily reaches several lines per second
	// and drowns out the warn/error events operators actually need.
	// Set CORSA_LOG_LEVEL=info to restore the previous verbosity, or
	// CORSA_LOG_LEVEL=debug / trace to surface routing / delivery
	// tracing during incident diagnosis.
	level := zerolog.WarnLevel
	if envLevel := os.Getenv("CORSA_LOG_LEVEL"); envLevel != "" {
		if parsed, err := zerolog.ParseLevel(envLevel); err == nil {
			level = parsed
		}
	}

	// Emit the one-shot startup banner ("logging initialised", "stderr
	// redirected", "application started") with the gate forced open, then apply
	// the configured steady-state level. These lifecycle events are info-level
	// but must always be recorded — an operator needs the start time and
	// effective level in the log even under the default warn gate.
	//
	// zerolog's global level is process-wide mutable state: a prior Setup() in
	// the same process may have left it at warn, which would suppress the
	// banner on a second call. Rather than rely on the process-start default,
	// drop the gate to Info for the banner and then set the real level. Setup()
	// runs single-threaded during init (before any worker goroutine logs), so
	// the brief permissive window is not observable elsewhere. (Without this,
	// the warn default dropped the banner entirely — TestSetupLogsStartupEvent
	// read an empty corsa.log.)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Info().Str("path", logPath).Str("level", level.String()).Msg("logging initialised")
	if stderrFile != nil {
		log.Info().Str("stderr_path", filepath.Join(dir, stderrFileName)).Msg("stderr redirected to file")
	}
	log.Info().Str("started_at", time.Now().UTC().Format(time.RFC3339)).Msg("application started")

	zerolog.SetGlobalLevel(level)

	return func() {
		recoverAndLog(dir)
		_ = f.Close()
		if stderrFile != nil {
			_ = stderrFile.Close()
		}
	}
}

// DeferRecover returns a function suitable for use with defer in any
// goroutine to capture panics into crash-*.log files. Unlike the cleanup
// function returned by Setup() (which is tied to the main goroutine),
// DeferRecover can be deferred in arbitrary goroutines where panics would
// otherwise be lost.
//
// Usage:
//
//	go func() {
//	    defer crashlog.DeferRecover()
//	    // ... work ...
//	}()
func DeferRecover() {
	// recover() MUST be called directly by the deferred function to stop a
	// panicking sequence — calling it from a nested helper (the previous
	// `recoverAndLog(logDir())` form) made recover() return nil, so this
	// guard never actually caught goroutine panics. Calling it directly here
	// also keeps the no-panic hot path allocation-free: logDir() (which walks
	// os.UserHomeDir + filepath.Join on every call) is now only evaluated
	// when a panic is actually in flight, instead of on every deferred
	// return. drainPendingForIdentities and the other s.goBackground closures
	// defer DeferRecover on every fire-and-forget goroutine, so the old form
	// was a multi-million-object alloc_space source under sustained churn.
	r := recover()
	if r == nil {
		return
	}
	logCrashAndRepanic(logDir(), r)
}

// recoverAndLog catches a panic, writes a crash log file, then re-panics
// so the default Go behaviour (print + exit 2) still happens. It must be
// deferred directly (e.g. `defer recoverAndLog(dir)`) for recover() to fire.
func recoverAndLog(dir string) {
	r := recover()
	if r == nil {
		return
	}
	logCrashAndRepanic(dir, r)
}

// logCrashAndRepanic writes the crash report for an already-recovered panic
// value and then re-panics. Shared by DeferRecover and recoverAndLog so both
// entry points produce identical crash files; it does not call recover()
// itself — the caller has already done so directly from its deferred frame.
func logCrashAndRepanic(dir string, r any) {
	stack := debug.Stack()
	ts := time.Now().UTC().Format("20060102-150405")
	crashPath := filepath.Join(dir, crashPrefix+ts+".log")

	report := fmt.Sprintf(
		"CRASH at %s\nPanic: %v\n\nStack trace:\n%s\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		r,
		stack,
	)

	// Best-effort write — if this fails we still re-panic.
	if err := os.WriteFile(crashPath, []byte(report), 0o600); err != nil {
		log.Error().Err(err).Msg("failed to write crash file")
	} else {
		log.Info().Str("path", crashPath).Msg("crash report saved")
	}

	log.Error().
		Interface("panic", r).
		Str("stack", string(stack)).
		Msg("CRASH")

	cleanOldCrashLogs(dir)

	// Re-panic so the runtime prints the trace and exits with code 2.
	panic(r)
}

func rotateIfNeeded(path string) {
	info, err := os.Stat(path)
	if err != nil || info.Size() < maxLogSize {
		return
	}

	ts := time.Now().UTC().Format("20060102-150405")
	rotated := path + "." + ts
	if err := os.Rename(path, rotated); err != nil {
		// Use fmt here because zerolog may not be initialised yet.
		fmt.Fprintf(os.Stderr, "crashlog: rotate failed: %v\n", err)
	}
}

func cleanOldCrashLogs(dir string) {
	entries, err := filepath.Glob(filepath.Join(dir, crashPrefix+"*.log"))
	if err != nil || len(entries) <= keepCrashLogs {
		return
	}

	// filepath.Glob returns sorted results; oldest first because of the
	// timestamp naming scheme (crash-YYYYMMDD-HHMMSS.log).
	toRemove := entries[:len(entries)-keepCrashLogs]
	for _, path := range toRemove {
		_ = os.Remove(path)
	}
}

// isWriterUsable returns true when w is non-nil and can accept at least a
// zero-length write. On Windows GUI apps os.Stdout / os.Stderr are backed by
// invalid handles, so even a 0-byte Write returns an error.
func isWriterUsable(w io.Writer) bool {
	if w == nil {
		return false
	}
	_, err := w.Write(nil)
	return err == nil
}

func logDir() string {
	if d := os.Getenv("CORSA_CHATLOG_DIR"); d != "" {
		return d
	}
	return appdata.DefaultDir()
}
