package netcore

import "sync/atomic"

// TransportTotals accumulates every byte that crossed a peer socket of one
// process, split by direction.
//
// It exists because the per-peer totals cannot answer "how much did this node
// send": they are assembled from live sessions plus persisted health rows, so a
// session that has been removed from the registry but not yet folded into
// health drops out of the sum, an inbound connection is briefly counted in both
// places, an evicted health row takes its bytes with it, and the persisted part
// survives a restart. A rate computed from two such readings can be negative.
// These counters only ever grow inside one process and are never persisted.
//
// The zero value is ready to use, so an owner can hold it by value and no
// construction site can forget to build it.
//
// The increment methods are unexported on purpose: the only code allowed to
// advance the totals is MeteredConn.Read / MeteredConn.Write, so a byte is
// counted at exactly one place regardless of how many layers above the socket
// later look at it.
type TransportTotals struct {
	sent     atomic.Uint64
	received atomic.Uint64
}

// BytesSent is the cumulative number of bytes written to peer sockets.
func (t *TransportTotals) BytesSent() uint64 {
	return t.sent.Load()
}

// BytesReceived is the cumulative number of bytes read from peer sockets.
func (t *TransportTotals) BytesReceived() uint64 {
	return t.received.Load()
}

func (t *TransportTotals) addSent(n int) {
	t.sent.Add(uint64(n))
}

func (t *TransportTotals) addReceived(n int) {
	t.received.Add(uint64(n))
}
