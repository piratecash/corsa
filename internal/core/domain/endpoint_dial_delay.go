package domain

import (
	"errors"
	"time"
)

// EndpointDialDelayReason is why a node holds its dials to one endpoint
// (docs/protocol/session_v2.md, "An endpoint that claims an old version").
type EndpointDialDelayReason string

// EndpointDialDelayClaimedOldVersion: the endpoint's correct answer — the
// probe's, a v1 welcome — claimed a protocol version below this node's
// minimum. The claim proves nothing; the delay only paces the redials.
const EndpointDialDelayClaimedOldVersion EndpointDialDelayReason = "claimed_old_version"

// EndpointDialDelay is one endpoint the node holds its dials to, as the
// operator's diagnostics show it.
type EndpointDialDelay struct {
	Endpoint PeerAddress
	Reason   EndpointDialDelayReason
	// ClaimedVersion is the version the endpoint's answer announced.
	ClaimedVersion ProtocolVersion
	// Steps is how many times the delay has grown since it last started
	// over; the next one doubles on it.
	Steps int
	// NextAttemptAt is when the next dial may go; it may be past, while the
	// count still holds.
	NextAttemptAt time.Time
}

// ErrEndpointDialDelayNotFound is a retry by hand of an endpoint the node
// holds no delay for.
var ErrEndpointDialDelayNotFound = errors.New("no dial delay is held for this endpoint")
