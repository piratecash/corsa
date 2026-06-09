package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"github.com/piratecash/corsa/internal/app/node"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/crashlog"

	"github.com/rs/zerolog/log"
)

func main() {
	// --version is handled before crashlog / signal / app initialisation so a
	// version probe does not write a crashlog row, leave a half-started node,
	// or hold any OS resources. Scan argv directly so unknown process-manager
	// arguments keep the historic "ignored by corsa-node" behavior.
	if versionRequested(os.Args[1:]) {
		fmt.Println(config.CorsaVersion)
		return
	}

	cleanup := crashlog.Setup()
	defer cleanup()

	configureMemoryLimit()

	log.Info().Msg("corsa-node starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app := node.New()
	if err := app.Run(ctx); err != nil {
		log.Error().Err(err).Msg("corsa-node exited with error")
		cleanup()
		os.Exit(1)
	}
}

// configureMemoryLimit applies a soft heap limit (Go's GOMEMLIMIT)
// from CORSA_MEM_LIMIT_BYTES so the runtime keeps the live+idle heap
// bounded and returns memory to the OS under pressure, instead of
// letting RSS ratchet up after transient allocation spikes (large
// per-cycle routing snapshots/deltas) and never come back down.
//
// Opt-in by design. A hard-coded default would be dangerous on a P2P
// node that runs on wildly heterogeneous hosts: too low and the GC
// thrashes. So:
//   - If GOMEMLIMIT is already set in the environment, the runtime
//     honours it on its own — defer to the operator and do nothing.
//   - Otherwise, apply CORSA_MEM_LIMIT_BYTES when present and valid.
//
// Recommended starting point for the ~1000-node mesh seen in the
// field (heap_alloc ~130MB, but RSS climbing past 700MB): set
// CORSA_MEM_LIMIT_BYTES=536870912 (512 MiB) or GOMEMLIMIT=512MiB and
// tune from there.
func configureMemoryLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		// Runtime already reads GOMEMLIMIT directly; don't override.
		return
	}
	raw := strings.TrimSpace(os.Getenv("CORSA_MEM_LIMIT_BYTES"))
	if raw == "" {
		return
	}
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit <= 0 {
		log.Warn().
			Str("value", raw).
			Msg("ignoring invalid CORSA_MEM_LIMIT_BYTES (want a positive byte count)")
		return
	}
	debug.SetMemoryLimit(limit)
	log.Info().
		Int64("bytes", limit).
		Msg("soft memory limit configured via CORSA_MEM_LIMIT_BYTES (GOMEMLIMIT)")
}

func versionRequested(args []string) bool {
	for _, arg := range args {
		switch {
		case arg == "--":
			return false
		case arg == "-version" || arg == "--version":
			return true
		case strings.HasPrefix(arg, "-version="):
			return boolFlagValue(arg, "-version=")
		case strings.HasPrefix(arg, "--version="):
			return boolFlagValue(arg, "--version=")
		}
	}
	return false
}

func boolFlagValue(arg, prefix string) bool {
	value := strings.TrimSpace(strings.TrimPrefix(arg, prefix))
	if value == "" {
		return true
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}
