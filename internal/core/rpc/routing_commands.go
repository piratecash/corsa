package rpc

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// RegisterRoutingCommands registers RPC commands for routing table observability.
// When the routing provider is nil (nodes without distance-vector routing),
// commands are registered as unavailable — hidden from help/autocomplete
// and returning 503 on execution, consistent with other mode-gated commands.
func RegisterRoutingCommands(t *CommandTable, rp RoutingProvider) {
	tableInfo := CommandInfo{
		Name:        "fetchRouteTable",
		Description: "Get full routing table snapshot with all entries",
		Category:    "routing",
	}
	summaryInfo := CommandInfo{
		Name:        "fetchRouteSummary",
		Description: "Get routing table summary: total/active entries, destinations, flap state",
		Category:    "routing",
	}
	lookupInfo := CommandInfo{
		Name:        "fetchRouteLookup",
		Description: "Lookup routes for a specific destination identity",
		Category:    "routing",
		Usage:       "<identity>",
	}
	// Phase 2 (cluster-mesh) — per-(Identity, Uplink) route
	// health, RTT EWMA, and transition timestamps. Read-only
	// observability surface; does NOT trigger probe sends. See
	// docs/protocol/route_health.md
	healthInfo := CommandInfo{
		Name:        "fetchRouteHealth",
		Description: "Get Phase 2 route health states per (Identity, Uplink) pair",
		Category:    "routing",
	}
	// Phase 3 PR 12.7 — per-(Identity, Uplink) hop-ack reputation
	// observability (HopAckAttempts / HopAckSuccesses /
	// ReliabilityScore / ConsecutiveFailures / CooldownUntil).
	// Read-only over RoutingProvider.ReputationSnapshot; never
	// triggers a probe, digest, or MarkHopFailure side effect
	// (Phase 3 §2.4 architectural-constraint contract).
	reputationInfo := CommandInfo{
		Name:        "fetchRouteReputation",
		Description: "Get Phase 3 per-(Identity, Uplink) reputation (hop-ack success ratio, cooldown state)",
		Category:    "routing",
	}

	if rp == nil {
		t.RegisterUnavailable(tableInfo)
		t.RegisterUnavailable(summaryInfo)
		t.RegisterUnavailable(lookupInfo)
		t.RegisterUnavailable(healthInfo)
		t.RegisterUnavailable(reputationInfo)
		return
	}

	t.Register(tableInfo, routeTableHandler(rp))
	t.Register(summaryInfo, routeSummaryHandler(rp))
	t.Register(lookupInfo, routeLookupHandler(rp))
	t.Register(healthInfo, routeHealthHandler(rp))
	t.Register(reputationInfo, routeReputationHandler(rp))
}

// routeTableHandler returns the full routing table as a structured JSON response.
// Each destination identity maps to its route entries with source, hops, seqno,
// next-hop, origin, and TTL remaining.
func routeTableHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
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

				nhKey := e.NextHop.String()
				nh, ok := nextHopCache[nhKey]
				if !ok {
					addr, net := rp.PeerTransport(e.NextHop)
					nh = wireNextHop{Identity: nhKey, Address: string(addr), Network: net.String()}
					nextHopCache[nhKey] = nh
				}

				routes = append(routes, wireRoute{
					Identity:   e.Identity.String(),
					Origin:     e.Origin.String(),
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
		if r, done := ctxDone(req); done {
			return r
		}
		snap := rp.RoutingSnapshot()
		snapTime := snap.TakenAt

		// PR 11.28 P2#2 — health-aware reachability. "Reachable"
		// means the relay path could actually select at least one
		// route to the identity, matching the contract on
		// Table.Lookup: skip withdrawn / expired AND skip
		// HealthDead pairs unless Source == Direct. Without the
		// Dead filter a target with one transit route stuck in
		// Dead would be reported as reachable even though Lookup
		// excludes it and the relay path falls back to gossip /
		// triggers a route query. Direct claims are exempt because
		// their session-bound lifecycle is the ground truth (PR
		// 11.25 / 11.26 alignment).
		//
		// Build the per-(Identity, Uplink) Dead lookup from
		// snap.Health (cached alongside Routes by the publisher's
		// Table.SnapshotIncremental, PR 11.27 P2#1) — no extra
		// RoutingProvider call.
		deadPair := make(map[routing.PeerIdentity]map[routing.PeerIdentity]struct{})
		for _, h := range snap.Health {
			if h.Health != routing.HealthDead {
				continue
			}
			byUplink, ok := deadPair[h.Identity]
			if !ok {
				byUplink = make(map[routing.PeerIdentity]struct{})
				deadPair[h.Identity] = byUplink
			}
			byUplink[h.Uplink] = struct{}{}
		}

		// Phase 3 PR 12.4 — black-hole cooldown filter, matching
		// Table.Lookup. A (Identity, Uplink) pair inside an armed
		// cooldown window is NOT selectable by the relay path, so
		// reachability must exclude it too — otherwise fetchRouteSummary
		// reports a route as reachable that the relay path would skip.
		// Unlike the Dead filter there is NO Direct exemption: the 5
		// consecutive hop-ack failures that armed the cooldown are
		// empirical "cannot deliver" evidence regardless of session
		// liveness (mirrors healthStore.isCooledDownLocked). snapTime is
		// the consistent "now" the rest of this handler already uses.
		cooledPair := make(map[routing.PeerIdentity]map[routing.PeerIdentity]struct{})
		for _, h := range snap.Health {
			if h.CooldownUntil.IsZero() || !snapTime.Before(h.CooldownUntil) {
				continue
			}
			byUplink, ok := cooledPair[h.Identity]
			if !ok {
				byUplink = make(map[routing.PeerIdentity]struct{})
				cooledPair[h.Identity] = byUplink
			}
			byUplink[h.Uplink] = struct{}{}
		}

		// Count unique reachable destinations (selectable routes).
		// RouteSourceLocal (synthetic self-route) is excluded —
		// reachability is about remote peers, not the node itself.
		destinations := make(map[string]struct{})
		directPeers := make(map[string]struct{})
		for ident, entries := range snap.Routes {
			for _, e := range entries {
				if e.IsWithdrawn() || e.IsExpired(snapTime) {
					continue
				}
				if e.Source == routing.RouteSourceLocal {
					continue
				}
				if e.Source != routing.RouteSourceDirect {
					if _, dead := deadPair[ident][e.NextHop]; dead {
						continue
					}
				}
				// Cooldown filter has no Direct exemption — applies to
				// every source.
				if _, cooled := cooledPair[ident][e.NextHop]; cooled {
					continue
				}
				destinations[ident.String()] = struct{}{}
				if e.Source == routing.RouteSourceDirect {
					directPeers[ident.String()] = struct{}{}
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
				PeerIdentity:      fe.PeerIdentity.String(),
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

		// Cap admission counters from the same atomic snapshot. The
		// JSON envelope carries FOUR independent policies with
		// SEPARATE kill switches:
		//   - K-cap (`accepted` / `accepted_replaced` / `rejected_full`
		//     / `rejected_all_protected`) — stays at zero only when
		//     `MaxNextHopsPerOrigin <= 0`.
		//   - SeqNo flap cap (`seqno_flap_holdowns`) — stays at zero
		//     only when `MaxSeqAdvancePerWindow <= 0` OR
		//     `SeqAdvanceWindow <= 0`.
		//   - Fast invalidation (`fast_invalidations`) — stays at
		//     zero only when `MaxSaneHops <= 0`.
		//   - Bad-hops hysteresis (`bad_hops_holdowns`) — stays at
		//     zero when `MaxSaneHops <= 0` (the hysteresis is only
		//     reachable from the fast-invalidation branch) OR when
		//     its own budget/window is non-positive
		//     (WithMaxBadHopsPerWindow / WithBadHopsWindow; both
		//     positive by default).
		// Disabling the K-cap does NOT silence the P2/P3 counters,
		// and disabling either P2/P3 knob does NOT silence the K-cap
		// counters. See routing.RouteCapStats for the per-field
		// semantics.
		// Per-policy kill-switch knobs are documented in the block
		// comment above; per-field semantics live on
		// routing.RouteCapStats. JSON key ordering is NOT a wire
		// contract — encoding/json sorts map keys alphabetically on
		// marshal, and dashboards key on field names. If a future
		// caller needs a stable ordered shape it should switch to a
		// struct with explicit json tags instead of editing this
		// map literal.
		capStats := map[string]uint64{
			"accepted":               snap.CapStats.Accepted,
			"accepted_replaced":      snap.CapStats.AcceptedReplaced,
			"rejected_full":          snap.CapStats.RejectedFull,
			"rejected_all_protected": snap.CapStats.RejectedAllProtected,
			"seqno_flap_holdowns":    snap.CapStats.SeqNoFlapHoldowns,
			"fast_invalidations":     snap.CapStats.FastInvalidations,
			"bad_hops_holdowns":      snap.CapStats.BadHopsHoldowns,
		}

		// Phase 0 overload-gate counters. Stay at zero when the gate
		// is not wired (CORSA_OVERLOAD_GOROUTINE_THRESHOLD unset) or
		// has never engaged. See routing.OverloadStats and
		// docs/routing.md "Operator tuning" section.
		overload := rp.OverloadStats()
		overloadStats := map[string]uint64{
			"engaged_cycles": overload.EngagedCycles,
		}

		// route_sync digest-as-heartbeat counters (docs/protocol/route_sync.md).
		// summary_match ≫ summary_mismatch means our periodic heartbeats are
		// confirmed and full syncs are suppressed; a high mismatch share means
		// digests diverge and the heartbeat is escalating to fulls.
		digest := rp.DigestHeartbeatStats()
		digestStats := map[string]uint64{
			"heartbeats_sent":  digest.HeartbeatsSent,
			"summary_match":    digest.SummaryMatch,
			"summary_mismatch": digest.SummaryMismatch,
			"digests_compared": digest.DigestsCompared,
			"compare_match":    digest.CompareMatch,
		}

		// Change-journal churn attribution (docs/routing.md). A dominant
		// health_aging share means quiet routes flap Dead↔Good on the passive
		// timeline between forced-fulls — control-plane churn with no real
		// topology change — rather than announce_upsert (a peer told us
		// something new). Sample this to see which cause leads before tuning.
		journalChurn := rp.JournalCauseStats()

		return jsonResponse(map[string]interface{}{
			"snapshot_at":          snapTime.UTC().Format(time.RFC3339),
			"total_entries":        snap.TotalEntries,
			"active_entries":       snap.ActiveEntries,
			"withdrawn_entries":    snap.TotalEntries - snap.ActiveEntries,
			"reachable_identities": len(destinations),
			"direct_peers":         len(directPeers),
			"flap_state":           flapState,
			"cap_admission":        capStats,
			"overload":             overloadStats,
			"digest":               digestStats,
			"journal_churn":        journalChurn,
		})
	}
}

// routeLookupHandler returns routes for a specific destination identity,
// ranked by the Phase 2 CompositeScore (the same ranking the relay
// path uses through routing.Table.Lookup). Operators reading this RPC
// see the same CompositeScore tiers the data plane would pick,
// including the HealthDead exclusion (with its Direct exemption) and
// the black-hole cooldown exclusion (NO Direct exemption) — see
// routing/table_lookup.go for the live-Lookup contract. NOTE: when
// multiple routes tie on CompositeScore this
// RPC applies a deterministic origin / next_hop / seq_no tiebreak
// for diff-friendly output, whereas Table.Lookup preserves storage
// iteration order on ties — within a tied set the first entry here
// is not guaranteed to be the one the relay path would pick, but
// both are equivalently selectable per the ranking model. Callers
// needing exact relay-path parity should treat same-score entries
// as equivalent.
//
// Read model: route entries come from the cached atomic RoutingSnapshot
// pointer (rp.RoutingSnapshot() — a single atomic.Pointer Load, no routing
// mutex). The per-(Identity, Uplink) Health TIERS do NOT come from that
// snapshot: Snapshot.Health was narrowed to the routing-relevant (Dead/cooled)
// subset to kill the periodic 2s full-health copy churn, and this
// CompositeScore diagnostic needs the complete Good/Questionable/Bad tiers, so
// it reads them from rp.HealthSnapshot() per request (a brief t.mu.RLock plus
// a copy of the live health set, skipped on a miss via the live-route guard
// below).
//
// Consequences for latency/alloc analysis: fetchRouteLookup is NOT a pure
// cached hot-read — on a hit it briefly takes t.mu.RLock and its route vs
// health view may be skewed by up to one snapshot interval (immaterial for a
// reported ranking). See docs/rpc/routing.md and docs/locking.md.
func routeLookupHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		identityArg, _ := req.Args["identity"].(string)
		identityArg = strings.TrimSpace(identityArg)
		if identityArg == "" {
			return validationError(fmt.Errorf("identity is required"))
		}
		// Parse at the boundary so a malformed or all-zero identity is
		// rejected with 400 here instead of being coerced to the zero
		// (absent) identity by PeerIdentityFromWire and silently looking
		// up no routes. ParsePeerIdentity accepts the all-zero 40-hex form
		// with nil error, so the explicit IsZero gate is required.
		target, err := domain.ParsePeerIdentity(identityArg)
		if err != nil {
			return validationError(fmt.Errorf("identity must be a valid peer identity: %w", err))
		}
		if target.IsZero() {
			return validationError(fmt.Errorf("identity must not be the zero peer identity"))
		}

		snap := rp.RoutingSnapshot()
		snapTime := snap.TakenAt
		entries := snap.Routes[target]

		// Build a (uplink) → *RouteHealthState index keyed by Uplink
		// only — we are looking at a single Identity, so all entries
		// in the index share that Identity.
		//
		// Source is Table.HealthSnapshot (full per-pair tiers), NOT
		// snap.Health: this diagnostic mirrors the data-plane CompositeScore
		// (line below), which needs every pair's Good/Questionable/Bad tier.
		// Snapshot.Health now carries only the routing-relevant subset
		// (Dead ∪ cooled — see routing.Snapshot.Health doc), so a Questionable
		// or Bad pair would be absent there and CompositeScore would mis-score
		// it as nil-health (by-hops). fetch_route_lookup is an on-demand
		// diagnostic, so the per-request full copy is acceptable; the small
		// route/health skew versus snap is immaterial for a reported ranking.
		//
		// Miss guard: HealthSnapshot is an RLock + deep copy of the entire
		// health set, so it is fetched ONLY when the target has at least one
		// live route. Without this, a burst of lookup-miss requests (unknown /
		// fully-withdrawn / expired target) on a dense node would reintroduce
		// the per-request copy churn this patch removes from the periodic
		// snapshot path. The scoring loop below re-applies the withdrawn/
		// expired skip, so a target with no live route yields an empty result
		// regardless.
		healthByUplink := map[routing.PeerIdentity]*routing.RouteHealthState{}
		hasLiveRoute := false
		for i := range entries {
			if !entries[i].IsWithdrawn() && !entries[i].IsExpired(snapTime) {
				hasLiveRoute = true
				break
			}
		}
		if hasLiveRoute {
			for _, state := range rp.HealthSnapshot() {
				if state.Identity != target {
					continue
				}
				s := state // capture by value, take address of the copy
				healthByUplink[state.Uplink] = &s
			}
		}

		type wireRoute struct {
			Origin     string  `json:"origin"`
			NextHop    string  `json:"next_hop"`
			Hops       int     `json:"hops"`
			SeqNo      uint64  `json:"seq_no"`
			Source     string  `json:"source"`
			TTLSeconds float64 `json:"ttl_seconds"`
		}

		type scoredEntry struct {
			wire  wireRoute
			score float64
		}

		// CompositeScore-based ranking on the same filter contract as
		// routing.Table.Lookup: drop HealthDead claims unless the
		// claim is RouteSourceDirect (Direct stays selectable; see
		// table_lookup.go "Direct exemption" branch). The synthetic
		// self-route (RouteSourceLocal, Hops=0) IS present in the
		// cached snapshot because the publisher's snapshot build injects
		// it for the node's own identity — both Table.Snapshot and the
		// hot path's Table.SnapshotIncremental inject the same synthetic
		// self-route (see internal/core/routing/table_lookup.go). It flows through
		// this filter+score path unchanged — RouteSourceLocal
		// carries the +20 source bonus and Hops=0, so it ranks
		// first when the caller queries the local identity. Matches
		// what Table.Lookup itself returns.
		var scored []scoredEntry
		for _, e := range entries {
			if e.IsWithdrawn() || e.IsExpired(snapTime) {
				continue
			}
			h := healthByUplink[e.NextHop]
			if h != nil && h.Health == routing.HealthDead && e.Source != routing.RouteSourceDirect {
				continue
			}
			// Phase 3 PR 12.4 — black-hole cooldown filter, matching
			// Table.Lookup. A pair inside an armed cooldown window is not
			// selectable by the relay path, so it must be dropped here
			// too. NO Direct exemption (mirrors
			// healthStore.isCooledDownLocked): the cooldown is empirical
			// "cannot deliver" evidence regardless of session liveness.
			if h != nil && !h.CooldownUntil.IsZero() && snapTime.Before(h.CooldownUntil) {
				continue
			}
			// Direct + Dead → substitute Bad for scoring (matches
			// the Lookup-side adjustment in
			// table_lookup.go::sortRoutesByCompositeScoreLocked) so
			// Direct stays selectable but deprioritised.
			if e.Source == routing.RouteSourceDirect && h != nil && h.Health == routing.HealthDead {
				adjusted := *h
				adjusted.Health = routing.HealthBad
				h = &adjusted
			}
			ttl := e.ExpiresAt.Sub(snapTime).Seconds()
			if ttl < 0 {
				ttl = 0
			}
			scored = append(scored, scoredEntry{
				wire: wireRoute{
					Origin:     e.Origin.String(),
					NextHop:    e.NextHop.String(),
					Hops:       e.Hops,
					SeqNo:      e.SeqNo,
					Source:     e.Source.String(),
					TTLSeconds: ttl,
				},
				score: routing.CompositeScore(uint8(e.Hops), e.Source, h, e.AttestedSigVerified),
			})
		}

		// Sort descending by score (higher = better). At equal
		// score this RPC applies a deterministic origin / next_hop /
		// seq_no tiebreak so operators eyeballing diffs across
		// requests see a stable order — useful for monitoring
		// dashboards. NOTE: Table.Lookup's relay-path winner uses
		// an insertion-sort that preserves storage iteration order
		// at equal score, so on a tied pair the relay path may
		// pick a different first entry than this RPC reports. The
		// divergence is bounded to ties (genuinely indistinguishable
		// by the ranking model — a Good 2-hop with the same RTT
		// and the same source as another Good 2-hop), so it does
		// not affect "which CompositeScore tier the relay would
		// pick"; only the within-tier ordering. PR 11.35 P3 —
		// callers needing exact relay-path parity should treat
		// any same-score entry as equivalently selectable.
		sort.SliceStable(scored, func(i, j int) bool {
			if scored[i].score != scored[j].score {
				return scored[i].score > scored[j].score
			}
			if scored[i].wire.Origin != scored[j].wire.Origin {
				return scored[i].wire.Origin < scored[j].wire.Origin
			}
			if scored[i].wire.NextHop != scored[j].wire.NextHop {
				return scored[i].wire.NextHop < scored[j].wire.NextHop
			}
			return scored[i].wire.SeqNo > scored[j].wire.SeqNo
		})

		result := make([]wireRoute, 0, len(scored))
		for _, s := range scored {
			result = append(result, s.wire)
		}

		return jsonResponse(map[string]interface{}{
			"identity":    identityArg,
			"routes":      result,
			"count":       len(result),
			"snapshot_at": snapTime.UTC().Format(time.RFC3339),
		})
	}
}

// sourcePriority was the pre-Phase-2 wire-source ranking helper
// used by fetchRouteLookup before PR 11.26 P2#1 aligned the
// handler with routing.Table.Lookup's CompositeScore ranking.
// Retained as a dead-code stub for one release to keep external
// debug snippets that imported it compiling; remove on next sweep.
//
//nolint:unused
func sourcePriority(source string) int {
	switch source {
	case "local":
		return 3
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

// routeHealthHandler exposes the Phase 2 RouteHealthState snapshot
// (docs/protocol/route_health.md). Output is a
// flat list keyed by (identity, uplink) so callers that need
// per-identity grouping can do so locally — keeping the wire shape
// simple avoids ambiguity about ordering.
//
// Health string labels match RouteHealth.String:
// "good" / "questionable" / "bad" / "dead". rtt_ms is the EWMA
// estimate in milliseconds (0 = no sample yet). last_hop_ack and
// last_probe are RFC3339 timestamps; the zero-time appears as the
// JSON-empty default for cold state. probe_failures is the
// consecutive-failure counter currently used by the state machine
// (reset on any confirmation). transition_at marks the most recent
// Health state change — useful for "transitioned N seconds ago"
// dashboard widgets.
//
// The handler is a pure read — it does NOT trigger probe sends or
// any other side effect. Probes run on their own ticker
// (HealthProbeInterval).
func routeHealthHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		snap := rp.HealthSnapshot()

		type wireState struct {
			Identity      string `json:"identity"`
			Uplink        string `json:"uplink"`
			Health        string `json:"health"`
			RTTMs         int64  `json:"rtt_ms"`
			LastHopAck    string `json:"last_hop_ack,omitempty"`
			LastProbe     string `json:"last_probe,omitempty"`
			ProbeFailures int    `json:"probe_failures"`
			TransitionAt  string `json:"transition_at,omitempty"`
		}

		states := make([]wireState, 0, len(snap))
		for _, s := range snap {
			ws := wireState{
				Identity:      s.Identity.String(),
				Uplink:        s.Uplink.String(),
				Health:        s.Health.String(),
				RTTMs:         s.RTT.Milliseconds(),
				ProbeFailures: s.ProbeFailures,
			}
			// PR 11.15 P3 / 11.16: emit last_hop_ack ONLY for pairs
			// that have actually received positive evidence
			// (relay_hop_ack or probe_ack with reachable=true).
			// The RouteHealthState.LastHopAck field is also used
			// internally as the applyIdleTick timeline reference
			// and gets stamped at creation (ensureLocked) for any
			// fresh entry — including announcement-only entries
			// that have never been confirmed. Gating emission on
			// the Confirmed flag preserves the protocol contract
			// in docs/protocol/route_health.md: last_hop_ack is
			// omitted when the pair has never been confirmed.
			if s.Confirmed && !s.LastHopAck.IsZero() {
				ws.LastHopAck = s.LastHopAck.UTC().Format(time.RFC3339)
			}
			if !s.LastProbe.IsZero() {
				ws.LastProbe = s.LastProbe.UTC().Format(time.RFC3339)
			}
			if !s.TransitionAt.IsZero() {
				ws.TransitionAt = s.TransitionAt.UTC().Format(time.RFC3339)
			}
			states = append(states, ws)
		}

		// Stable sort: identity asc, uplink asc. Test fixtures
		// rely on deterministic ordering; production consumers
		// can re-sort as needed.
		sort.Slice(states, func(i, j int) bool {
			if states[i].Identity != states[j].Identity {
				return states[i].Identity < states[j].Identity
			}
			return states[i].Uplink < states[j].Uplink
		})

		return jsonResponse(map[string]interface{}{
			"states":      states,
			"count":       len(states),
			"snapshot_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// routeReputationHandler exposes the Phase 3 PR 12.7 per-
// (Identity, Uplink) reputation snapshot
// (docs/cluster-mesh/phase-3-multipath-reputation.md §4.7).
// Output is a flat list keyed by (identity, uplink) — same
// shape convention as fetchRouteHealth so an operator UI can
// join the two surfaces by (identity, uplink) without
// reshaping. Each state carries the raw counter pair and the
// derived EMA / cooldown fields:
//
//   - hop_ack_attempts / hop_ack_successes — total observations
//     and successful subset. Operators read the ratio
//     (successes / attempts) as the long-run reliability;
//     reliability_score is the EWMA-smoothed instantaneous
//     view that CompositeScore actually consumes.
//   - reliability_score — float in [0, 1].
//   - consecutive_failures — current streak; resets on any
//     success.
//   - cooldown_until — RFC3339 deadline of an armed black-hole
//     cooldown. Emitted ONLY when non-zero so absent pairs and
//     cleared cooldowns produce a clean JSON payload.
//
// Wire-vs-RPC invariant (Phase 3 §2.4): the handler is a pure
// read; it does NOT trigger probe sends, digest emits,
// MarkHopFailure, or any other side effect. Reputation is
// local-only state — the snapshot is for internal observation,
// never gossiped or transferred between nodes. UI aggregation
// (e.g. ranked top-N reputable uplinks, success-rate
// histograms) lives on top of this raw snapshot, not under
// the fetchRouteReputation name — that contract carries the
// exact wire shape documented here on every transport path.
func routeReputationHandler(rp RoutingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		snap := rp.ReputationSnapshot()

		type wireState struct {
			Identity            string  `json:"identity"`
			Uplink              string  `json:"uplink"`
			HopAckAttempts      uint64  `json:"hop_ack_attempts"`
			HopAckSuccesses     uint64  `json:"hop_ack_successes"`
			ReliabilityScore    float64 `json:"reliability_score"`
			ConsecutiveFailures int     `json:"consecutive_failures"`
			CooldownUntil       string  `json:"cooldown_until,omitempty"`
		}

		states := make([]wireState, 0, len(snap))
		for _, s := range snap {
			ws := wireState{
				Identity:            s.Identity.String(),
				Uplink:              s.Uplink.String(),
				HopAckAttempts:      s.HopAckAttempts,
				HopAckSuccesses:     s.HopAckSuccesses,
				ReliabilityScore:    s.ReliabilityScore,
				ConsecutiveFailures: s.ConsecutiveFailures,
			}
			// Emit CooldownUntil only when armed — the zero
			// time would JSON-encode to a non-trivial RFC3339
			// string ("0001-01-01T00:00:00Z") that operators
			// would have to special-case downstream.
			if !s.CooldownUntil.IsZero() {
				ws.CooldownUntil = s.CooldownUntil.UTC().Format(time.RFC3339)
			}
			states = append(states, ws)
		}

		// Stable sort: identity asc, uplink asc. Matches the
		// fetchRouteHealth contract so UI consumers reading both
		// commands can join by (identity, uplink) without
		// re-sorting.
		sort.Slice(states, func(i, j int) bool {
			if states[i].Identity != states[j].Identity {
				return states[i].Identity < states[j].Identity
			}
			return states[i].Uplink < states[j].Uplink
		})

		return jsonResponse(map[string]interface{}{
			"states":      states,
			"count":       len(states),
			"snapshot_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}
