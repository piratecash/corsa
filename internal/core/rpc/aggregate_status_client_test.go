package rpc_test

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// fetchAggregateStatus decodes back through rpc.Client with every field the
// node set, v2_tls_first included in both of its values: false is the
// operator's TLS-first switch turned off, and a decoder that dropped it would
// report TLS-first as on.
func TestTheAggregateStatusRoundTripsThroughTheClient(t *testing.T) {
	for _, tlsFirst := range []bool{true, false} {
		want := protocol.AggregateStatusFrame{
			Status:          "healthy",
			UsablePeers:     3,
			ConnectedPeers:  4,
			TotalPeers:      5,
			PendingMessages: 6,
			UpdateAvailable: true,
			UpdateReason:    "peer_build_newer",
			V2TLSFirst:      tlsFirst,
		}
		node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
			if frame.Type != "fetch_aggregate_status" {
				return protocol.Frame{Type: "error", Error: "unexpected " + frame.Type}
			}
			answer := want
			return protocol.Frame{Type: "aggregate_status", AggregateStatus: &answer}
		})
		table := rpc.NewCommandTable()
		rpc.RegisterNetworkCommands(table, node)
		cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
		startServer(t, cfg, table)

		got, err := rpc.NewClient(cfg).FetchAggregateStatus()
		if err != nil || got != want {
			t.Fatalf("v2_tls_first=%v: decoded %+v, %v; want %+v", tlsFirst, got, err, want)
		}
	}
}
