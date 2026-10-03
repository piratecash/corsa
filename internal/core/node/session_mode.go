package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_mode.go ties the v1 → v2 transition to this node's accept and dial
// paths so that it cannot be forgotten. The mode comes from the version
// constants (config.ProtocolVersionSecureSession); sessionModeWired says
// which modes the paths below actually implement. Raising ProtocolVersion or
// MinimumProtocolVersion to the activation version without the paths of the
// resulting mode fails TestTheConfiguredSessionModeIsWired.
//
// Steps that add a mode here (docs/protocol/session_v2.md):
//   - ModeTransition — the listener demultiplexer and every dial path (S3,
//     S4), then the activation bump of ProtocolVersion (S6);
//   - ModeV2Only — the legacy path removed from both sides, then the bump of
//     MinimumProtocolVersion (S7).
var sessionModeWired = map[sessionv2.Mode]bool{
	sessionv2.ModeLegacyOnly: true,
	// The listener chooses the kind by the first byte (openInboundTransport),
	// the CM dialler tries v2 first (dialPeerTransportForCM).
	sessionv2.ModeTransition: true,
}

// configuredSessionMode is the mode this build's version constants select.
func configuredSessionMode() (sessionv2.Mode, error) {
	return sessionv2.ModeFor(config.ProtocolVersion, config.MinimumProtocolVersion, config.ProtocolVersionSecureSession)
}
