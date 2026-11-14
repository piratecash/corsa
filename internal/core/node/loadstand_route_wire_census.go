package node

import (
	"cmp"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// RouteWireCensusEndpoint contains only registry evidence. It has no
// dependency on the frame recorder or its final report.
type RouteWireCensusEndpoint struct {
	ConnID    domain.ConnID
	Peer      domain.PeerIdentity
	Direction domain.PeerDirection
	SessionID [32]byte
	Interval  domain.RouteWireSessionInterval
}

// RouteWireCensus returns all proven registry endpoints that intersect the
// window, including endpoints that closed within it. The caller must supply
// boundaries in this observation's clock domain and establish their
// attribution before combining results across processes.
func (o *RegisteredProvenV2Observation) RouteWireCensus(begin, end time.Duration) ([]RouteWireCensusEndpoint, error) {
	return projectRouteWireCensus(o.Snapshot(), o.Identity(), begin, end)
}

type routeWireCensusOwner struct {
	ref  k3RegisteredOwnerRef
	peer domain.PeerIdentity
}

type routeWireCensusLifetime struct {
	owner    routeWireCensusOwner
	interval domain.RouteWireSessionInterval
}

func projectRouteWireCensus(snapshot k3RegisteredSnapshot, self domain.PeerIdentity, begin, end time.Duration) ([]RouteWireCensusEndpoint, error) {
	if snapshot.Population != "RegisteredProvenV2" || snapshot.Status != "PAST" || snapshot.Reason != "" ||
		!snapshot.PastOnly || snapshot.Instance == ([16]byte{}) || self.IsZero() || begin < 0 || end <= begin ||
		snapshot.Baseline.Before < 0 || snapshot.Baseline.After < snapshot.Baseline.Before || snapshot.Baseline.After > begin ||
		snapshot.Cut.Before < end || snapshot.Cut.After < snapshot.Cut.Before || snapshot.Sequence != uint64(len(snapshot.Transitions)) ||
		len(snapshot.Transitions) > k3RegisteredMaxHistory {
		return nil, errRouteWireIncomplete
	}
	active := make(map[domain.ConnID]routeWireCensusOwner)
	seen := make(map[domain.ConnID]struct{})
	covered := make(map[domain.ConnID]routeWireCensusLifetime)
	previous := snapshot.Baseline.After
	var generation uint64
	for i, row := range snapshot.Transitions {
		clock := row.Clock
		if row.Sequence != uint64(i)+1 || clock.Before < previous || clock.After < clock.Before || clock.After > snapshot.Cut.Before ||
			(clock.Before <= begin && begin <= clock.After) || (clock.Before <= end && end <= clock.After) {
			return nil, errRouteWireIncomplete
		}
		previous = clock.After
		removed := routeWireCensusOwner{ref: row.Removed, peer: row.RemovedIdentity}
		added := routeWireCensusOwner{ref: row.Added, peer: row.AddedIdentity}
		if removed == (routeWireCensusOwner{}) && added == (routeWireCensusOwner{}) {
			return nil, errRouteWireIncomplete
		}
		if removed != (routeWireCensusOwner{}) {
			id := removed.ref.ConnID
			if active[id] != removed {
				return nil, errRouteWireIncomplete
			}
			delete(active, id)
			if clock.Before < begin {
				delete(covered, id)
			} else if lifetime, ok := covered[id]; ok && clock.After < end {
				lifetime.interval.EndBefore, lifetime.interval.EndAfter = clock.Before, clock.After
				covered[id] = lifetime
			}
		}
		if added != (routeWireCensusOwner{}) {
			id := added.ref.ConnID
			_, reused := seen[id]
			if id == 0 || reused || added.peer.IsZero() || added.peer == self || added.ref.Generation != generation+1 ||
				(added.ref.Direction != "inbound" && added.ref.Direction != "outbound") {
				return nil, errRouteWireIncomplete
			}
			generation = added.ref.Generation
			seen[id] = struct{}{}
			active[id] = added
			if clock.After < end {
				covered[id] = routeWireCensusLifetime{owner: added, interval: domain.RouteWireSessionInterval{
					BeginBefore: max(begin, clock.Before), BeginAfter: max(begin, clock.After), EndBefore: end, EndAfter: end,
				}}
			}
		}
		if row.RegisteredOwners != len(active) || len(active) > k3RegisteredMaxOwners {
			return nil, errRouteWireIncomplete
		}
	}
	if snapshot.RegisteredOwners != len(active) {
		return nil, errRouteWireIncomplete
	}
	endpoints := make([]RouteWireCensusEndpoint, 0, len(covered))
	for id, lifetime := range covered {
		if !lifetime.interval.Within(begin, end) {
			return nil, errRouteWireIncomplete
		}
		owner := lifetime.owner
		peerDirection := domain.PeerDirectionInbound
		if owner.ref.Direction == "outbound" {
			peerDirection = domain.PeerDirectionOutbound
		}
		endpoints = append(endpoints, RouteWireCensusEndpoint{ConnID: id, Peer: owner.peer, Direction: peerDirection, SessionID: owner.ref.RouteWireSession, Interval: lifetime.interval})
	}
	slices.SortFunc(endpoints, func(a, b RouteWireCensusEndpoint) int { return cmp.Compare(a.ConnID, b.ConnID) })
	return endpoints, nil
}
