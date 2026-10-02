package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Transport traffic: the node's monotonic byte count, measured at the socket.
//
// The point of measurement is netcore.MeteredConn.Read / Write and nowhere
// else, so a byte is counted once however many layers later look at it, and
// a retransmitted frame is counted every time it actually crosses the socket.
// Every peer socket the node opens or accepts is wrapped exactly once, right
// where it is born, with netcore.NewMeteredConn(raw, &s.transportTotals):
// handleConn (accepted), openPeerSession and openPeerSessionForCM (sessions),
// syncPeer and sendNoticeToPeer (one-shot dials). There is deliberately no
// net.Conn-accepting helper on Service for this — the set of such methods is
// frozen by scripts/enforce-netcore-boundary.sh. The reasoning and the list
// of deliberate exclusions live in docs/refactoring/dht/05-rollout-metrics.md
// §5.3.

// TransportTrafficStats returns the cumulative transport byte counts.
// Lock-free: the counters are atomics and startedAt is immutable. Reading
// resets nothing. Implements rpc.RoutingProvider, which serves the counts to
// other processes through fetchRouteSummary.
func (s *Service) TransportTrafficStats() domain.TransportTrafficStats {
	return domain.TransportTrafficStats{
		StartedAt: s.startedAt,
		// The stamp and the two loads are not one atomic step, and their
		// order only moves the period's closing edge by the nanoseconds the
		// loads take. What keeps a rate honest is the pairing rule: two
		// readings by one sequential poller with the same StartedAt and
		// ReadAt₂ > ReadAt₁ — the second reading's loads then follow the
		// first one's, so neither difference can go negative.
		ReadAt:        time.Now().UTC(),
		BytesSent:     s.transportTotals.BytesSent(),
		BytesReceived: s.transportTotals.BytesReceived(),
	}
}

// transportTrafficFrame projects the stats onto the wire block of the
// fetch_traffic_totals answer.
func transportTrafficFrame(stats domain.TransportTrafficStats) *protocol.TransportTrafficFrame {
	return &protocol.TransportTrafficFrame{
		StartedAt:     stats.StartedAt,
		ReadAt:        stats.ReadAt,
		BytesSent:     stats.BytesSent,
		BytesReceived: stats.BytesReceived,
	}
}
