package capture

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Event is the internal unit of work for the capture writer goroutine.
// It carries everything needed to render one line in a capture file
// without referencing external mutable state.
type Event struct {
	// Timestamp of the capture moment (from the injected clock).
	Ts time.Time

	// WireDir: in (received from peer) or out (sent to peer).
	WireDir domain.WireDirection

	// Outcome for outbound events: sent or write_failed.
	// Zero value for inbound events.
	Outcome domain.SendOutcome

	// Kind classifies the content for diagnostics.
	Kind domain.PayloadKind

	// Raw is the original wire payload. Stored as a defensive copy
	// safe to read from the writer goroutine after the network
	// goroutine has moved on.
	Raw string
}
