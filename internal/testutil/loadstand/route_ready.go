package loadstand

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrRouteNotReady = errors.New("planned recipient route is not yet observed")

type plannedRouteSnapshot struct {
	Routes     *[]plannedRouteRow `json:"routes"`
	Total      *int               `json:"total"`
	Active     *int               `json:"active"`
	SnapshotAt time.Time          `json:"snapshot_at"`
}
type plannedRouteRow struct {
	Identity   domain.PeerIdentity `json:"identity"`
	Origin     domain.PeerIdentity `json:"origin"`
	NextHop    plannedRouteHop     `json:"next_hop"`
	Hops       *int                `json:"hops"`
	SeqNo      *uint64             `json:"seq_no"`
	Source     string              `json:"source"`
	Withdrawn  *bool               `json:"withdrawn"`
	Expired    *bool               `json:"expired"`
	TTLSeconds *float64            `json:"ttl_seconds"`
}
type plannedRouteHop struct {
	Identity domain.PeerIdentity `json:"identity"`
	Address  domain.PeerAddress  `json:"address"`
	Network  string              `json:"network"`
}

// PlannedRoute observes the actual public table schema. It does not trigger
// probes, sync, key lookup, dial, or mutation. A route must use a planned
// gateway and retain positive TTL after conservatively ageing the cached
// snapshot to this response's read time. This observes cached historical flags;
// it does not prove that a route has not been withdrawn since the snapshot.
func (s *ProcessSource) PlannedRoute(ctx context.Context, recipient domain.PeerIdentity, gateways []domain.PeerIdentity) error {
	if recipient.IsZero() || len(gateways) == 0 {
		return ErrInvalidConfiguration
	}
	requestStart := s.clock.Now().UTC()
	var snapshot plannedRouteSnapshot
	if err := s.readDTO(ctx, "fetchRouteTable", &snapshot); err != nil {
		return err
	}
	readAt := s.clock.Now().UTC()
	if readAt.Before(requestStart) {
		return ErrInvalidSample
	}
	if snapshot.Routes == nil || snapshot.Total == nil || snapshot.Active == nil || *snapshot.Total < 0 || *snapshot.Active < 0 || *snapshot.Active > *snapshot.Total || snapshot.SnapshotAt.IsZero() || snapshot.SnapshotAt.After(readAt) {
		return ErrFamilyUnavailable
	}
	// The server formats snapshot_at with RFC3339 seconds, discarding its
	// subsecond fraction. Using the wire value deliberately over-ages by <1s.
	ageSeconds := readAt.Sub(snapshot.SnapshotAt).Seconds()
	for _, route := range *snapshot.Routes {
		if route.Identity != recipient {
			continue
		}
		if route.Hops == nil || route.SeqNo == nil || route.Withdrawn == nil || route.Expired == nil || route.TTLSeconds == nil || route.Origin.IsZero() || route.NextHop.Identity.IsZero() {
			return ErrFamilyUnavailable
		}
		if *route.Hops < 1 || math.IsNaN(*route.TTLSeconds) || math.IsInf(*route.TTLSeconds, 0) || *route.TTLSeconds < 0 {
			return ErrInvalidSample
		}
		if route.Source != domain.RouteSourceAnnouncement.String() && route.Source != domain.RouteSourceDirect.String() && route.Source != domain.RouteSourceHopAck.String() {
			return ErrInvalidSample
		}
		if !*route.Withdrawn && !*route.Expired && *route.TTLSeconds-ageSeconds > 0 && slices.Contains(gateways, route.NextHop.Identity) {
			return nil
		}
	}
	return ErrRouteNotReady
}
