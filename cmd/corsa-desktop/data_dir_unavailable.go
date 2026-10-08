package main

import (
	"github.com/rs/zerolog/log"
)

// dataDirUnavailableTitle / dataDirUnavailableHint are shown before any
// localisation is loaded — preferences live in the very directory that is
// missing — so the text is English.
const (
	dataDirUnavailableTitle = "Corsa cannot start"
	dataDirUnavailableHint  = "The data directory is not available. If it lives in an encrypted container, mount the container and start Corsa again."
)

// reportDataDirUnavailable tells the user why the app is not starting. The
// log line reaches a terminal; a GUI launch on Windows has no console at
// all, so the platform notice is the only thing the user ever sees there.
func reportDataDirUnavailable(cause error) {
	log.Error().Err(cause).Msg("corsa-desktop refusing to start: data directory unavailable")
	if err := showDataDirUnavailable(dataDirUnavailableHint + "\n\n" + cause.Error()); err != nil {
		log.Error().Err(err).Msg("data_dir_notice_show_failed")
	}
}
