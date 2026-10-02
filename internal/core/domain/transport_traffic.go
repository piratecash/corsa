package domain

import "time"

// TransportTrafficStats is the node's cumulative transport byte count: every
// byte read from or written to a peer socket since the process started,
// retransmissions and handshakes included.
//
// The counters never decrease within one process and are never persisted.
// A rate is therefore the difference of two readings divided by the
// difference of their ReadAt — valid only when both readings carry the same
// StartedAt. A different StartedAt means the process restarted between them
// and the counters began again from zero; such a pair must be discarded, not
// subtracted.
type TransportTrafficStats struct {
	// StartedAt is when the process began accumulating — the node's start.
	StartedAt time.Time
	// ReadAt is when the counters were loaded, the closing edge of the period
	// [StartedAt, ReadAt] they describe.
	ReadAt time.Time
	// BytesSent is every byte written to a peer socket.
	BytesSent uint64
	// BytesReceived is every byte read from a peer socket.
	BytesReceived uint64
}
