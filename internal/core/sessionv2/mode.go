package sessionv2

import (
	"errors"
	"fmt"
)

// mode.go is the transition from v1 to v2 written as code rather than as a
// plan: a build's session mode follows from its version constants alone.
//
//	ProtocolVersion < activation             → ModeLegacyOnly (today)
//	ProtocolVersion ≥ activation > Minimum   → ModeTransition
//	MinimumProtocolVersion ≥ activation      → ModeV2Only
//
// The legacy path is a temporary allowance of the transition: raising the
// minimum to the activation version closes it, and that — not a check of a
// version number inside an unsigned hello — is what shuts a relayed v1
// proof out of the network.

// Mode is how a node establishes sessions.
type Mode int

const (
	// ModeLegacyOnly — v1 only; the v2 package is not used.
	ModeLegacyOnly Mode = iota + 1
	// ModeTransition — v2 first. Legacy only on a connection whose peer
	// showed no v2 and is not pinned to it; a v2 failure never falls back.
	ModeTransition
	// ModeV2Only — v2 only: the listener closes what is not TLS, the dialer
	// never probes and never goes legacy.
	ModeV2Only
)

var modeNames = map[Mode]string{
	ModeLegacyOnly: "legacy_only",
	ModeTransition: "transition",
	ModeV2Only:     "v2_only",
}

func (m Mode) String() string {
	if name, ok := modeNames[m]; ok {
		return name
	}
	return fmt.Sprintf("mode(%d)", int(m))
}

// ErrVersionOrder is a minimum above the protocol version.
var ErrVersionOrder = errors.New("sessionv2: minimum protocol version above the protocol version")

// ModeFor derives the session mode from a build's version constants.
func ModeFor(protocolVersion, minimumProtocolVersion, activationVersion int) (Mode, error) {
	switch {
	case minimumProtocolVersion > protocolVersion:
		return 0, fmt.Errorf("%w: %d > %d", ErrVersionOrder, minimumProtocolVersion, protocolVersion)
	case minimumProtocolVersion >= activationVersion:
		return ModeV2Only, nil
	case protocolVersion >= activationVersion:
		return ModeTransition, nil
	default:
		return ModeLegacyOnly, nil
	}
}
