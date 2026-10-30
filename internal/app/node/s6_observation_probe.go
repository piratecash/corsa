//go:build s6_observation_probe && !d1_resource_probe

package node

import corenode "github.com/piratecash/corsa/internal/core/node"

// S6ObservationProbeRequest selects the eight existing in-memory sources for
// an isolated S6 diagnostic build. Every field is a value request; its labels
// and capacities remain caller assertions, not deployment or run qualification.
// No file source, clock, callback, configuration or resource owner is supplied.
type S6ObservationProbeRequest struct {
	RegisteredProvenV2    corenode.RegisteredProvenV2ObservationRequest
	AdvertisedConnections corenode.AdvertisedConnectionObservationRequest
	RawPinMembership      corenode.RawPinMembershipObservationRequest
	Guard                 corenode.GuardObservationKey
	Terminal              corenode.TerminalObservationKey
	Pending               corenode.PendingObservationKey
	Handshake             corenode.HandshakeObservationRequest
	ConnectionTotal       corenode.ConnectionTotalObservationRequest
}

// NewWithS6ObservationProbe uses the original App constructor once. It neither
// changes admission/configuration nor starts Run, readers or a collector.
// In particular a disabled connection budget stays disabled. Source-specific
// invalid/UNKNOWN states and the original App lifetime remain unchanged.
func NewWithS6ObservationProbe(request S6ObservationProbeRequest) (*App, error) {
	return newAppWithObservations(corenode.ObservationOptions{
		RegisteredProvenV2:    &request.RegisteredProvenV2,
		AdvertisedConnections: &request.AdvertisedConnections,
		RawPinMembership:      &request.RawPinMembership,
		Guard:                 &request.Guard,
		Terminal:              &request.Terminal,
		Pending:               &request.Pending,
		Handshake:             &request.Handshake,
		ConnectionTotal:       &request.ConnectionTotal,
	})
}
