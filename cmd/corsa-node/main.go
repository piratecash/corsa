package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/piratecash/corsa/internal/app/node"
	"github.com/piratecash/corsa/internal/core/crashlog"

	"github.com/rs/zerolog/log"
)

func main() {
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
