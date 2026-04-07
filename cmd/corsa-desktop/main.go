package main

import (
	"os"

	"github.com/piratecash/corsa/internal/app/desktop"
	"github.com/piratecash/corsa/internal/core/crashlog"

	"github.com/rs/zerolog/log"
)

func main() {
	cleanup := crashlog.Setup()
	defer cleanup()

	log.Info().Msg("corsa-desktop starting")

	if err := desktop.Run(); err != nil {
		log.Error().Err(err).Msg("corsa-desktop exited with error")
		cleanup()
		os.Exit(1)
	}
}
