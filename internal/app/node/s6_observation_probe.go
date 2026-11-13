//go:build s6_observation_probe && !d1_resource_probe

package node

import (
	"context"

	corenode "github.com/piratecash/corsa/internal/core/node"
)

// RunWithS6TerminalDrain joins Service background work after a successful
// diagnostic App.Run before the caller reads the terminal source.
func (a *App) RunWithS6TerminalDrain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.Run(ctx); err != nil {
		return err
	}
	a.service.WaitBackground()
	return nil
}

// S6ObservationProbeRequest selects the in-memory sources for an isolated S6
// diagnostic build. Queue observations are optional. Labels and capacities
// are caller assertions.
// No file source, clock, callback, configuration or resource owner is supplied.
type S6ObservationProbeRequest struct {
	RegisteredProvenV2          corenode.RegisteredProvenV2ObservationRequest
	AdvertisedConnections       corenode.AdvertisedConnectionObservationRequest
	RawPinMembership            corenode.RawPinMembershipObservationRequest
	Guard                       corenode.GuardObservationKey
	Terminal                    corenode.TerminalObservationKey
	Pending                     corenode.PendingObservationKey
	DatagramQueueGrowth         bool `json:",omitempty"`
	WriterQueueHistoryCapacity  int  `json:",omitempty"`
	GossipQueueDepth            bool `json:",omitempty"`
	SessionQueueHistoryCapacity int  `json:",omitempty"`
	Handshake                   corenode.HandshakeObservationRequest
	ConnectionTotal             corenode.ConnectionTotalObservationRequest
}

// NewWithS6ObservationProbe uses the original App constructor once. It neither
// changes admission/configuration nor starts Run, readers or a collector.
// In particular a disabled connection budget stays disabled. Source-specific
// invalid/UNKNOWN states and the original App lifetime remain unchanged.
func NewWithS6ObservationProbe(request S6ObservationProbeRequest) (*App, error) {
	return newAppWithObservations(corenode.ObservationOptions{
		RegisteredProvenV2:          &request.RegisteredProvenV2,
		AdvertisedConnections:       &request.AdvertisedConnections,
		RawPinMembership:            &request.RawPinMembership,
		Guard:                       &request.Guard,
		Terminal:                    &request.Terminal,
		Pending:                     &request.Pending,
		DatagramQueueGrowth:         request.DatagramQueueGrowth,
		WriterQueueHistoryCapacity:  request.WriterQueueHistoryCapacity,
		GossipQueueDepth:            request.GossipQueueDepth,
		SessionQueueHistoryCapacity: request.SessionQueueHistoryCapacity,
		Handshake:                   &request.Handshake,
		ConnectionTotal:             &request.ConnectionTotal,
	})
}
