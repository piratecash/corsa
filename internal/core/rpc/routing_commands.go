package rpc

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"corsa/internal/core/identity"
	"corsa/internal/core/routing"
)

// RegisterRoutingCommands registers RPC commands for routing table observability.
// When the routing provider is nil (nodes without distance-vector routing),
// commands are registered as unavailable — hidden from help/autocomplete
// and returning 503 on execution, consistent with other mode-gated commands.
func RegisterRoutingCommands(t *CommandTable, rp RoutingProvider) {
	tableInfo := CommandInfo{
		Name:        "fetch_route_table",
		Description: "Get full routing table snapshot with all entries",
		Category:    "routing",
	}
	summaryInfo := CommandInfo{
		Name:        "fetch_route_summary",
		Description: "Get routing table summary: total/active entries, destinations, flap state",
		Category:    "routing",
	}
	lookupInfo := CommandInfo{
		Name:        "fetch_route_lookup",
		Description: "Lookup routes for a specific destination identity",
		Category:    "routing",
		Usage:       "<identity>",
	}

	if rp == nil {
		t.RegisterUnavailable(tableInfo)
		t.RegisterUnavailable(summaryInfo)
		t.RegisterUnavailable(lookupInfo)
		return
	}

	t.Register(tableInfo, routeTableHandler(rp))
	t.Register(summaryInfo, routeSummaryHandler(rp))
	t.Register(lookupInfo, routeLookupHandler(rp))
}

// routeTableHandler returns the full routing table as a structured JSON response.
// Each destination identity maps to its route entries with source, hops, seqno,
// next-hop, origin, and TTL remaining.
func routeTableHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		snap := rp.RoutingSnapshot()
		snapTime := snap.TakenAt

		type wireNextHop struct {
			Identity string `json:"identity"`
			Address  string `json:"address,omitempty"`
			Network  string `json:"network,omitempty"`
		}

		type wireRoute struct {
			Identity   string      `json:"identity"`
			Origin     string      `json:"origin"`
			NextHop    wireNextHop `json:"next_hop"`
			Hops       int         `json:"hops"`
			SeqNo      uint64      `json:"seq_no"`
			Source     string      `json:"source"`
			Withdrawn  bool        `json:"withdrawn"`
			Expired    bool        `json:"expired"`
			TTLSeconds float64     `json:"ttl_seconds"`
		}

		// Cache transport lookups — many routes share the same next_hop.
		nextHopCache := make(map[string]wireNextHop)

		var routes []wireRoute
		for _, entries := range snap.Routes {
			for _, e := range entries {
				ttl := e.ExpiresAt.Sub(snapTime).Seconds()
				if ttl < 0 {
					ttl = 0
				}

				nhKey := string(e.NextHop)
				nh, ok := nextHopCache[nhKey]
				if !ok {
					addr, net := rp.PeerTransport(e.NextHop)
					nh = wireNextHop{Identity: nhKey, Address: string(addr), Network: net.String()}
					nextHopCache[nhKey] = nh
				}

				routes = append(routes, wireRoute{
					Identity:   string(e.Identity),
					Origin:     string(e.Origin),
					NextHop:    nh,
					Hops:       e.Hops,
					SeqNo:      e.SeqNo,
					Source:     e.Source.String(),
					Withdrawn:  e.IsWithdrawn(),
					Expired:    e.IsExpired(snapTime),
					TTLSeconds: ttl,
				})
			}
		}

		// Stable sort: identity → origin → next_hop identity → source.
		sort.Slice(routes, func(i, j int) bool {
			if routes[i].Identity != routes[j].Identity {
				return routes[i].Identity < routes[j].Identity
			}
			if routes[i].Origin != routes[j].Origin {
				return routes[i].Origin < routes[j].Origin
			}
			if routes[i].NextHop.Identity != routes[j].NextHop.Identity {
				return routes[i].NextHop.Identity < routes[j].NextHop.Identity
			}
			return routes[i].Source < routes[j].Source
		})

		return jsonResponse(map[string]interface{}{
			"routes":      routes,
			"total":       snap.TotalEntries,
			"active":      snap.ActiveEntries,
			"snapshot_at": snapTime.UTC().Format(time.RFC3339),
		})
	}
}

// routeSummaryHandler returns a compact overview of routing table health:
// entry counts, reachable destinations, and flap detection state.
func routeSummaryHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		snap := rp.RoutingSnapshot()
		snapTime := snap.TakenAt

		// Count unique reachable destinations (active, non-withdrawn routes).
		destinations := make(map[string]struct{})
		directPeers := make(map[string]struct{})
		for ident, entries := range snap.Routes {
			for _, e := range entries {
				if e.IsWithdrawn() || e.IsExpired(snapTime) {
					continue
				}
				destinations[string(ident)] = struct{}{}
				if e.Source.String() == "direct" {
					directPeers[string(ident)] = struct{}{}
				}
			}
		}

		// Flap state from the same atomic snapshot.
		type wireFlapEntry struct {
			PeerIdentity      string `json:"peer_identity"`
			RecentWithdrawals int    `json:"recent_withdrawals"`
			InHoldDown        bool   `json:"in_hold_down"`
			HoldDownUntil     string `json:"hold_down_until,omitempty"`
		}

		var flapState []wireFlapEntry
		for _, fe := range snap.FlapState {
			wfe := wireFlapEntry{
				PeerIdentity:      string(fe.PeerIdentity),
				RecentWithdrawals: fe.RecentWithdrawals,
				InHoldDown:        fe.InHoldDown,
			}
			if fe.InHoldDown {
				wfe.HoldDownUntil = fe.HoldDownUntil.UTC().Format(time.RFC3339)
			}
			flapState = append(flapState, wfe)
		}

		// Stable sort by peer identity for deterministic output.
		sort.Slice(flapState, func(i, j int) bool {
			return flapState[i].PeerIdentity < flapState[j].PeerIdentity
		})

		return jsonResponse(map[string]interface{}{
			"snapshot_at":          snapTime.UTC().Format(time.RFC3339),
			"total_entries":        snap.TotalEntries,
			"active_entries":       snap.ActiveEntries,
			"withdrawn_entries":    snap.TotalEntries - snap.ActiveEntries,
			"reachable_identities": len(destinations),
			"direct_peers":         len(directPeers),
			"flap_state":           flapState,
		})
	}
}

// routeLookupHandler returns routes for a specific destination identity,
// sorted by preference (source priority, then hops ascending).
// Uses the atomic snapshot for TTL computation, consistent with routeTableHandler.
func routeLookupHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		identityArg, _ := req.Args["identity"].(string)
		identityArg = strings.TrimSpace(identityArg)
		if identityArg == "" {
			return validationError(fmt.Errorf("identity is required"))
		}
		if err := identity.ValidateAddress(identityArg); err != nil {
			return validationError(fmt.Errorf("invalid identity format: %w", err))
		}

		snap := rp.RoutingSnapshot()
		snapTime := snap.TakenAt
		entries := snap.Routes[routing.PeerIdentity(identityArg)]

		type wireRoute struct {
			Origin     string  `json:"origin"`
			NextHop    string  `json:"next_hop"`
			Hops       int     `json:"hops"`
			SeqNo      uint64  `json:"seq_no"`
			Source     string  `json:"source"`
			TTLSeconds float64 `json:"ttl_seconds"`
		}

		var result []wireRoute
		for _, e := range entries {
			if e.IsWithdrawn() || e.IsExpired(snapTime) {
				continue
			}
			ttl := e.ExpiresAt.Sub(snapTime).Seconds()
			if ttl < 0 {
				ttl = 0
			}
			result = append(result, wireRoute{
				Origin:     string(e.Origin),
				NextHop:    string(e.NextHop),
				Hops:       e.Hops,
				SeqNo:      e.SeqNo,
				Source:     e.Source.String(),
				TTLSeconds: ttl,
			})
		}

		// Stable sort: source trust (desc), hops (asc), origin, next_hop, seq_no (desc).
		sort.Slice(result, func(i, j int) bool {
			pi, pj := sourcePriority(result[i].Source), sourcePriority(result[j].Source)
			if pi != pj {
				return pi > pj
			}
			if result[i].Hops != result[j].Hops {
				return result[i].Hops < result[j].Hops
			}
			if result[i].Origin != result[j].Origin {
				return result[i].Origin < result[j].Origin
			}
			if result[i].NextHop != result[j].NextHop {
				return result[i].NextHop < result[j].NextHop
			}
			return result[i].SeqNo > result[j].SeqNo
		})

		return jsonResponse(map[string]interface{}{
			"identity":    identityArg,
			"routes":      result,
			"count":       len(result),
			"snapshot_at": snapTime.UTC().Format(time.RFC3339),
		})
	}
}

// sourcePriority maps wire source strings to numeric priority.
// Higher value = more trusted, matching RouteSource.TrustRank().
func sourcePriority(source string) int {
	switch source {
	case "direct":
		return 2
	case "hop_ack":
		return 1
	case "announcement":
		return 0
	default:
		return -1
	}
}
