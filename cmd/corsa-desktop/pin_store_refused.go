package main

import (
	"errors"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// pinStoreRefusedHint opens the notice of a node that refused to start over
// its v2 pin store. English for the same reason as the data directory's: the
// app stops before its localisation is in use.
const pinStoreRefusedHint = "The node's peer protection (pin store) cannot be kept or enforced safely, so Corsa did not start. " +
	"Nothing was changed or deleted. The details below name the file and how to fix it."

// reportPinStoreRefused shows a pin store refusal (domain.ErrPinStoreRefused)
// as the startup notice; any other error is left to the log line. The
// refusal itself is the operator's instruction — the file, what is wrong,
// what to do — so the notice carries it whole.
func reportPinStoreRefused(err error) {
	if !errors.Is(err, domain.ErrPinStoreRefused) {
		return
	}
	if noticeErr := showStartupNotice(pinStoreRefusedHint + "\n\n" + err.Error()); noticeErr != nil {
		log.Error().Err(noticeErr).Msg("pin_store_notice_show_failed")
	}
}
