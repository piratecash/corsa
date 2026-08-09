package netcore

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// write_ticket.go carries the TIMING contract of one outbound frame from the
// caller that created it down to the socket write.
//
// It used to carry a second thing — a terminal notification back up, so an
// attempt machine could learn whether its frame had been written. That machine
// is gone: the datagram layer keeps no per-send record for a terminal to reach,
// and no other send path ever attached an observer. What is left travels one way
// only, which is why nothing here needs a once-guard or a callback contract any
// more.
//
// Reference: docs/protocol/network_core.md, docs/locking.md (sendMu).

// OutboundWrite is the per-item outbound contract attached to a frame whose
// timing the connection default cannot express. Both fields are optional; the
// zero value describes a legacy frame — no send deadline of its own and the
// connection's default write deadline.
type OutboundWrite struct {
	// SendUntil is the instant after which the frame must not be written.
	// The writer re-checks it immediately BEFORE the socket write, not only
	// at enqueue time: the frame may have sat in two queues since, and a
	// frame written after its deadline is worse than a frame dropped — the
	// receiving side has already torn down the state that gave it meaning.
	// An expired frame is discarded by the writer without being written, and
	// the connection keeps serving the frames behind it: a worthless frame is
	// not a broken link.
	SendUntil domain.OptionalTime

	// WriteGrace bounds the duration of the socket write of THIS frame.
	// A write that has not finished within the grace is aborted and the
	// connection is torn down as dead: a frame cut in the middle desyncs the
	// line protocol, and there is no way to resynchronise it.
	// Zero means "use the connection's default write deadline".
	WriteGrace time.Duration
}

// WriteTicket binds one queued outbound frame to its outbound contract.
//
// The ticket is what travels with the queue element, and the SAME ticket
// pointer is handed from the session queue down to the connection queue. That
// is what makes the deadline survive the hop between the two: a side table
// keyed by queue element would have to be kept in sync with two channels and
// would drift the moment either of them is drained somewhere new.
//
// A nil *WriteTicket is a valid, fully inert ticket: no deadline and no grace,
// so every method below answers as if the frame were a legacy one. Legacy send
// paths pass nil.
//
// The ticket is IMMUTABLE once built and every method on it is a question, so
// one ticket may be attached to several queue elements at once — a caller that
// offers the same frame to a peer's several sockets in turn builds one and
// hands it to each. Any field added here must keep that property, or the
// senders that share a ticket become a data race.
type WriteTicket struct {
	write OutboundWrite
}

// NewWriteTicket creates a ticket for a single outbound frame. Returns nil
// when the contract is empty in every field, so callers can pass the result
// straight into the tracked send path without branching: an empty contract
// must not cost an allocation on the hot legacy path.
func NewWriteTicket(write OutboundWrite) *WriteTicket {
	if !write.SendUntil.Valid() && write.WriteGrace <= 0 {
		return nil
	}
	return &WriteTicket{write: write}
}

// expiredAt reports whether the frame's send deadline has already passed.
func (t *WriteTicket) expiredAt(now time.Time) bool {
	if t == nil || !t.write.SendUntil.Valid() {
		return false
	}
	return now.After(t.write.SendUntil.Time())
}

// writeDeadlineAt returns the socket write deadline for this item: the
// write grace when the caller set one, the connection default otherwise.
func (t *WriteTicket) writeDeadlineAt(now time.Time, connDefault time.Duration) time.Time {
	if t != nil && t.write.WriteGrace > 0 {
		return now.Add(t.write.WriteGrace)
	}
	return now.Add(connDefault)
}
