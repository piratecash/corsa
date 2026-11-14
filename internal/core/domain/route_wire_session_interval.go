package domain

import "time"

const RouteWireFullMaxDestinations = 10_000

// RouteWireDialRole selects the direction of a two-node loadstand fixture.
type RouteWireDialRole string

const (
	RouteWireDialer   RouteWireDialRole = "dialer"
	RouteWireListener RouteWireDialRole = "listener"
)

type RouteWireFullSource string

const (
	RouteWireFullPeriodic  RouteWireFullSource = "periodic"
	RouteWireFullTriggered RouteWireFullSource = "triggered"
	RouteWireFullConnect   RouteWireFullSource = "connect"
)

// RouteWireFullReceipt binds live claims from one endpoint's full-v3
// receive stream to a snapshot sampled after the wire window closed.
type RouteWireFullReceipt struct {
	Destinations    int           `json:"destinations"`
	IdentitySHA256  string        `json:"identity_sha256"`
	Epoch           uint64        `json:"epoch"`
	SnapshotAfterNS time.Duration `json:"snapshot_after_ns"`
}

// RouteWireFullSend is the common projection of the complete full-v3 passes
// whose ordered bytes matched the original connection's writer.
type RouteWireFullSend struct {
	Destinations    int    `json:"destinations"`
	IdentitySHA256  string `json:"identity_sha256"`
	Epoch           uint64 `json:"epoch"`
	Passes          uint64 `json:"passes"`
	PeriodicPasses  uint64 `json:"periodic_passes"`
	TriggeredPasses uint64 `json:"triggered_passes"`
	ConnectPasses   uint64 `json:"connect_passes"`
}

func (s RouteWireFullSend) PassesFrom(source RouteWireFullSource) uint64 {
	switch source {
	case RouteWireFullPeriodic:
		return s.PeriodicPasses
	case RouteWireFullTriggered:
		return s.TriggeredPasses
	case RouteWireFullConnect:
		return s.ConnectPasses
	}
	return 0
}

func (s RouteWireFullSend) ValidPasses() bool {
	return s.Passes > 0 && s.PeriodicPasses <= s.Passes && s.TriggeredPasses <= s.Passes-s.PeriodicPasses &&
		s.ConnectPasses == s.Passes-s.PeriodicPasses-s.TriggeredPasses
}

// RouteWireSessionInterval brackets the registered lifetime of one endpoint
// clipped to a measurement window in its clock domain.
type RouteWireSessionInterval struct {
	BeginBefore time.Duration `json:"begin_before_ns"`
	BeginAfter  time.Duration `json:"begin_after_ns"`
	EndBefore   time.Duration `json:"end_before_ns"`
	EndAfter    time.Duration `json:"end_after_ns"`
}

func (i RouteWireSessionInterval) Within(begin, end time.Duration) bool {
	return begin >= 0 && i.BeginBefore >= begin && i.BeginAfter >= i.BeginBefore &&
		i.EndBefore > i.BeginAfter && i.EndAfter >= i.EndBefore && i.EndAfter <= end
}

// Overlap returns the guaranteed and possible durations of a common
// registered lifetime. Both intervals must use the same clock domain.
func (i RouteWireSessionInterval) Overlap(other RouteWireSessionInterval) (minimum, maximum time.Duration) {
	return min(i.EndBefore, other.EndBefore) - max(i.BeginAfter, other.BeginAfter),
		min(i.EndAfter, other.EndAfter) - max(i.BeginBefore, other.BeginBefore)
}

// RouteWireFrameSpan bounds all observed callbacks in one direction. Zero
// means no measured frames. The endpoints use the window's clock domain.
type RouteWireFrameSpan struct {
	First time.Duration `json:"first_ns"`
	Last  time.Duration `json:"last_ns"`
}

func (s RouteWireFrameSpan) Within(begin, end time.Duration) bool {
	return s.First > begin && s.Last >= s.First && s.Last < end
}
