package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestPlannedRouteActualSchemaAndMutants(t *testing.T) {
	target := domain.PeerIdentityFromWire(strings.Repeat("1", 40))
	gateway := domain.PeerIdentityFromWire(strings.Repeat("2", 40))
	for _, kind := range []string{"valid", "no-route", "expired", "withdrawn", "zero-ttl", "foreign-gateway", "missing-expired", "unknown-source", "case-alias", "missing-schema"} {
		t.Run(kind, func(t *testing.T) {
			row := map[string]any{"identity": target, "origin": gateway, "next_hop": map[string]any{"identity": gateway}, "hops": 2, "seq_no": 1, "source": domain.RouteSourceAnnouncement.String(), "withdrawn": false, "expired": false, "ttl_seconds": 100}
			rows := []any{row}
			switch kind {
			case "no-route":
				rows = nil
			case "expired":
				row["expired"] = true
			case "withdrawn":
				row["withdrawn"] = true
			case "zero-ttl":
				row["ttl_seconds"] = 0
			case "foreign-gateway":
				row["next_hop"] = map[string]any{"identity": target}
			case "missing-expired":
				delete(row, "expired")
			case "unknown-source":
				row["source"] = "invented"
			case "case-alias":
				row["Expired"] = nil
			}
			snapshot := map[string]any{"routes": rows, "total": 1, "active": 1, "snapshot_at": time.Now().Add(-time.Second)}
			if kind == "no-route" {
				snapshot["routes"] = []any{}
				snapshot["total"] = 0
				snapshot["active"] = 0
			}
			if kind == "missing-schema" {
				delete(snapshot, "active")
			}
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			source := NewProcessSource(protocolClient(t, 200, string(raw)))
			err = source.PlannedRoute(context.WithoutCancel(t.Context()), target, []domain.PeerIdentity{gateway})
			if kind == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid/unavailable route accepted")
			}
			if kind == "no-route" && !errors.Is(err, ErrRouteNotReady) {
				t.Fatal(err)
			}
		})
	}
}

type routeRequestClock struct {
	start, end time.Time
	reads      int
}

func (c *routeRequestClock) Now() time.Time {
	c.reads++
	if c.reads == 1 {
		return c.start
	}
	return c.end
}
func TestPlannedRouteConservativeCachedTTLAndClock(t *testing.T) {
	target := domain.PeerIdentityFromWire(strings.Repeat("1", 40))
	gateway := domain.PeerIdentityFromWire(strings.Repeat("2", 40))
	base := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"valid-cached", "old-expired", "old-still-positive", "ttl-equals-age", "expires-during-request", "backward-clock", "forward-wall-jump", "future-snapshot", "rounded-conservative"} {
		t.Run(kind, func(t *testing.T) {
			start, end, snapshot, ttl := base.Add(2*time.Second), base.Add(2*time.Second), base, 10.0
			valid := false
			switch kind {
			case "valid-cached":
				valid = true
			case "old-expired":
				start = base.Add(time.Hour)
				end = start
				ttl = 1
			case "old-still-positive":
				start = base.Add(time.Hour)
				end = start
				ttl = 3601
				valid = true
			case "ttl-equals-age":
				ttl = 2
			case "expires-during-request":
				end = start.Add(time.Second)
				ttl = 2.5
			case "backward-clock":
				end = start.Add(-time.Nanosecond)
			case "forward-wall-jump":
				end = start.Add(time.Hour)
			case "future-snapshot":
				snapshot = end.Add(time.Second)
			case "rounded-conservative":
				start = base.Add(time.Second)
				end = start
				ttl = 0.2
			}
			row := map[string]any{"identity": target, "origin": gateway, "next_hop": map[string]any{"identity": gateway}, "hops": 2, "seq_no": 1, "source": domain.RouteSourceAnnouncement.String(), "withdrawn": false, "expired": false, "ttl_seconds": ttl}
			raw, err := json.Marshal(map[string]any{"routes": []any{row}, "total": 1, "active": 1, "snapshot_at": snapshot.Format(time.RFC3339)})
			if err != nil {
				t.Fatal(err)
			}
			clock := &routeRequestClock{start: start, end: end}
			source, err := NewProcessSourceWithClock(protocolClient(t, 200, string(raw)), clock)
			if err != nil {
				t.Fatal(err)
			}
			err = source.PlannedRoute(t.Context(), target, []domain.PeerIdentity{gateway})
			if valid && err != nil {
				t.Fatal(err)
			}
			if !valid && err == nil {
				t.Fatal("expired, future, or clock-inconsistent cached observation accepted")
			}
			if kind == "backward-clock" && !errors.Is(err, ErrInvalidSample) {
				t.Fatal("backward wall time not classified invalid")
			}
		})
	}
}
