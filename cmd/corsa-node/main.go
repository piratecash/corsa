package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
