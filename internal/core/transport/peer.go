package transport

import "corsa/internal/core/domain"

// Peer is a dial candidate known to the local node.
// It stores the transport address together with the source tag that
// indicates how the address was discovered (bootstrap, persisted,
// peer exchange, manual, etc.).
type Peer struct {
	Address domain.PeerAddress
	Source  domain.PeerSource
}
