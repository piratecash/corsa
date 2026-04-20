package metrics

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TrafficSource provides current traffic totals.
// Implemented by node.Service via HandleLocalFrame.
type TrafficSource interface {
	HandleLocalFrame(frame protocol.Frame) protocol.Frame
}

// Collector periodically samples node metrics and maintains
// rolling history buffers. It runs as an independent goroutine
// and exposes data through thread-safe snapshot methods.
type Collector struct {
	source  TrafficSource
	traffic *TrafficHistory
}

// NewCollector creates a metrics collector that reads traffic data
// from the given source.
func NewCollector(source TrafficSource) *Collector {
	return &Collector{
		source:  source,
		traffic: NewTrafficHistory(),
	}
}

// Seed captures the current cumulative byte counters from the source as the
// delta baseline, without recording a sample. Intended to be called once,
// synchronously, before Run, so that the very first ticker-driven Record
// produces the genuine per-second delta instead of either (a) reporting the
// entire pre-attach cumulative as a single-second spike or (b) silently
// dropping the first delta to mask that spike.
//
// The latter mode (drop-on-first) is what the previous implementation did,
// and it was the root cause of the desktop traffic chart appearing empty for
// many seconds after launch on an idle node: the only sample that would
// reliably contain non-zero traffic — the first one — was always zeroed,
// and subsequent idle ticks recorded zero deltas, leaving the chart blank
// until the next observable network exchange.
//
// If the source returns nil NetworkStats (collector not yet wired or
// source error), Seed leaves the baseline at zero, which is correct for the
// "started together" case.
func (c *Collector) Seed() {
	reply := c.source.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if reply.NetworkStats == nil {
		return
	}
	c.traffic.Seed(reply.NetworkStats.TotalBytesSent, reply.NetworkStats.TotalBytesReceived)
}

// Run starts the collection loop. It takes a snapshot every second
// until the context is cancelled. Call this in a goroutine.
//
// Run does not call Seed automatically: callers attaching to an
// already-running system must Seed once before Run so the baseline reflects
// the real pre-attach totals. Callers starting the source from scratch can
// skip Seed (the zero baseline matches reality).
func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Info().Msg("metrics collector started")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("metrics collector stopped")
			return
		case <-ticker.C:
			c.collectTrafficSample()
		}
	}
}

func (c *Collector) collectTrafficSample() {
	reply := c.source.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if reply.NetworkStats == nil {
		return
	}
	c.traffic.Record(reply.NetworkStats.TotalBytesSent, reply.NetworkStats.TotalBytesReceived)
}

// TrafficSnapshot returns the current traffic history as a protocol frame.
func (c *Collector) TrafficSnapshot() protocol.Frame {
	samples := c.traffic.Snapshot()

	items := make([]protocol.TrafficSampleFrame, len(samples))
	for i, s := range samples {
		items[i] = protocol.TrafficSampleFrame{
			Timestamp:     s.Timestamp.Format(time.RFC3339),
			BytesSentPS:   s.BytesSentPS,
			BytesRecvPS:   s.BytesRecvPS,
			TotalSent:     s.TotalSent,
			TotalReceived: s.TotalReceived,
		}
	}

	return protocol.Frame{
		Type: "traffic_history",
		TrafficHistory: &protocol.TrafficHistoryFrame{
			IntervalSeconds: 1,
			Capacity:        c.traffic.Capacity(),
			Count:           len(items),
			Samples:         items,
		},
	}
}
