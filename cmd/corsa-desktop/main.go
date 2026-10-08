package main

import (
	"os"

	"github.com/piratecash/corsa/internal/app/desktop"
	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/crashlog"

	"github.com/rs/zerolog/log"
)

func main() {
	// Before crashlog.Setup: the crash log, like everything else, lives in
	// the data directory, and an unreachable one must stop the start
	// rather than be recreated on the plain disk.
	if err := appdata.Anchor(); err != nil {
		reportDataDirUnavailable(err)
		os.Exit(1)
	}

	cleanup := crashlog.Setup()
	defer cleanup()

	log.Info().Msg("corsa-desktop starting")

	if err := desktop.Run(); err != nil {
		log.Error().Err(err).Msg("corsa-desktop exited with error")
		cleanup()
		os.Exit(1)
	}
}
