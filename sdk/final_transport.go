package sdk

import (
	"errors"
	"time"
)

var ErrFinalTransportUnavailable = errors.New("SDK final transport observation unavailable")

type FinalTransportBoundary string

const TransportQuiescent FinalTransportBoundary = "transport_quiescent"

// FinalTransportSnapshot is copied after the SDK joins its managed transport
// owners. Availability does not mean that Run or the subsequent database close
// succeeded. No observation starts shutdown or admits a new operation.
type FinalTransportSnapshot struct {
	StartedAt     time.Time
	ReadAt        time.Time
	BytesSent     uint64
	BytesReceived uint64
	Boundary      FinalTransportBoundary
}

func (r *Runtime) FinalTransport() (FinalTransportSnapshot, error) {
	if r == nil {
		return FinalTransportSnapshot{}, ErrFinalTransportUnavailable
	}
	snapshot := r.finalTransport.Load()
	if snapshot == nil {
		return FinalTransportSnapshot{}, ErrFinalTransportUnavailable
	}
	return *snapshot, nil
}

// latchFinalTransport requires lifecycle and all required Close joins. The
// normal SDK constructor is the supported managed-CM origin; arbitrary Service
// fixtures and never-started/failed-before-node starts have no final period.
func (r *Runtime) latchFinalTransport() {
	if !r.managedTransportOrigin || !r.nodeStarted || r.startErr != nil || r.finalTransport.Load() != nil {
		return
	}
	stats := r.nodeService.TransportTrafficStats()
	r.finalTransport.Store(&FinalTransportSnapshot{StartedAt: stats.StartedAt, ReadAt: stats.ReadAt, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, Boundary: TransportQuiescent})
}

type runtimeDatabaseCloser interface{ Close() error }

func (r *Runtime) closeDatabase() error {
	if r.databaseCloser != nil {
		return r.databaseCloser.Close()
	}
	// Preserve old internal Runtime fixtures; normal constructors always bind it.
	return r.database.Close()
}
