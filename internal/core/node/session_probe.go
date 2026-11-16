package node

import (
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// legacyProbeLineForVersion emits the fixed identity-less probe layout.
// The listener matches the v31 shape used by v2.5.71 and never admits a
// plaintext peer session.
func legacyProbeLineForVersion(version int) ([]byte, error) {
	return protocol.MarshalFrameLineBytes(protocol.Frame{
		Type: "hello", Version: version, Client: "node", Network: networkName,
	})
}

// legacyWelcomeCorrect recognizes a self-consistent version claim for
// historical endpoint diagnostics. It does not grant session authority.
func legacyWelcomeCorrect(frame protocol.Frame) bool {
	return frame.Type == "welcome" && frame.Version > 0 && frame.Network == networkName &&
		connauth.HasIdentityFields(frame) &&
		identity.VerifyBoxKeyBinding(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig) == nil
}
