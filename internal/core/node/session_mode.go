package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// configuredSessionMode is the mode this build's version constants select.
func configuredSessionMode() (sessionv2.Mode, error) {
	return sessionv2.ModeFor(config.ProtocolVersion, config.MinimumProtocolVersion, config.ProtocolVersionSecureSession)
}
