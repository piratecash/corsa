package node

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_diagnostics.go is the read-only surface of the datagram plane: the
// operator's summary and the two interfaces §4.3 makes a CONTRACT of the layer
// rather than a convenience — the reachability probe and the route plan.
//
// They are here, on the node, for the same reason isPeerReachable and
// ExplainFileRoute are: an artifact owner builds its retries on the probe and
// its diagnostics on the plan, and both need the node's identity parsing and
// its JSON shapes. Everything they decide is decided INSIDE the layer, over
// the same selection code a real send walks — this file only asks and renders.
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §5, §9, §10.

// errDatagramNotEnabled marks every diagnostic asked of a node whose plane is
// off. It is an error rather than an empty answer on purpose: "no candidates"
// and "this build does not speak the plane" are different facts, and an
// operator debugging a route needs to tell them apart.
var errDatagramNotEnabled = &datagramError{"datagram transport layer is not enabled"}

type datagramError struct {
	msg string
}

func (e *datagramError) Error() string { return "datagram: " + e.msg }

// FetchDatagramSummary returns the whole §5 / §10 picture of the local plane:
// what the conveyor decided, what the per-neighbour budgets did, what the class
// queue holds, what the anti-replay cache refused, and the numbers they all run
// on.
//
// It is the datagram counterpart of fetchRouteSummary and follows the same
// rule: a pure read that spends no lock on the receive path. The counters are
// atomics, the queue and the replay cache publish their own snapshots under
// their own mutexes, and no domain mutex of the Service is involved at all.
//
// The `replay` block is the only place the §5 fairness refusals surface. The
// cache evicts and refuses records under pressure — RejectedNoisyPeer,
// EvictedNoisyPeer, RejectedCapacity — and reclaims lost pipeline branches
// (AbandonedReservations); a rule that fires invisibly is a rule nobody can
// act on, so those counters are reported here beside the occupancy they must
// be read against.
//
// Wire schema:
//
//	{
//	  "enabled": true,
//	  "transit": true,                    // advertises mesh_datagram_transit_v1
//	  "dtypes": ["cached_identity", ...], // types this build handles as an endpoint
//	  "registered_dtypes": [],            // types actually in the registry
//	  "metrics":   { ... },               // observed/accepted/dropped by reason
//	  "admission": { ... },               // per-neighbour budget counters
//	  "queue":     { ... },               // weighted class queue depths
//	  "replay":    { "Counters": {...}, "Held": 0 }, // anti-replay cache
//	  "limits":    { ... }                // the §5 numbers in force
//	}
func (s *Service) FetchDatagramSummary() (json.RawMessage, error) {
	layer := s.datagramLayer()
	if layer == nil {
		return nil, errDatagramNotEnabled
	}

	diagnostics := datagram.CollectDiagnostics(
		layer.limits, layer.metrics, layer.admission, layer.queue, layer.replayCache,
	)

	summary := map[string]any{
		"enabled":  true,
		"endpoint": s.localDatagramAdvertise().Endpoint,
		"transit":  s.localDatagramAdvertise().Transit,
		// The declared set IS the registered set (§6.1): one field would be
		// enough, and the two are kept so a reader can see at a glance that
		// they agree — the pair that used to disagree is exactly the bug.
		"dtypes":            dtypeNames(s.localDatagramDTypes()),
		"registered_dtypes": dtypeNames(layer.types.DTypes()),
		"metrics":           diagnostics.Metrics,
		"admission":         diagnostics.Admission,
		"queue":             diagnostics.Queue,
		"replay":            diagnostics.Replay,
		"limits":            diagnostics.Limits,
	}
	data, err := json.Marshal(summary)
	if err != nil {
		return nil, fmt.Errorf("marshal datagram summary: %w", err)
	}
	return data, nil
}

// dtypeNames renders a dtype set for the wire. Always a non-nil slice: an
// empty registry is a real and expected state in PR-0, and `null` would read
// as "unknown".
func dtypeNames(dtypes []domain.DType) []string {
	names := make([]string, 0, len(dtypes))
	for _, dtype := range dtypes {
		names = append(names, dtype.String())
	}
	return names
}

// DatagramReachable answers "is there anybody to give the first hop to" for
// one exact datagram (§4.3).
//
// The guarantee is ONE-WAY and covers BOTH negative outcomes of a send: false
// means a send performed at the same moment over the same data would NOT have
// been queued — `no_route`, or a gate's `rejected`, the last-hop dtype gate
// included. A true answer promises nothing: the probe is TOCTOU by
// construction, the route may vanish between the two calls, and no read-only
// interface can fix that.
//
// dtype is a mandatory input, not decoration: the last-hop gate depends on the
// type, so a probe without it would answer about "some" datagram rather than
// the one about to be sent — and worse, it would answer `unsupported_dtype`,
// because the empty name is in no peer's declared set. It is REFUSED rather
// than defaulted.
//
// It reserves nothing, dials nothing and spends no cryptographic budget, and
// it reads the FRESH lookup — the same source a locally originated send reads
// — so an action taken right after a route appears is not answered with
// "unreachable" while the send would already work.
// The negative answer is REPORTED WITH ITS REASON, not as a bare false. §6.1
// makes "the peer declared no handler for this dtype" cancel a cached
// confirmation immediately, while a destination that is merely off the routing
// table must not cancel anything — one bool cannot carry that difference, and
// an operator reading "unreachable" needs the same distinction.
func (s *Service) DatagramReachable(
	ctx context.Context,
	dst domain.PeerIdentity,
	dtype domain.DType,
) (json.RawMessage, error) {
	layer := s.datagramLayer()
	if layer == nil {
		return nil, errDatagramNotEnabled
	}
	// The query is VALIDATED by its constructor, and the refusals are the
	// layer's rather than a second opinion built here. That matters beyond
	// tidiness: this method is reached by in-process consumers that never touch
	// the RPC surface, so a check living at the transport edge would leave them
	// able to ask about a datagram no send could ever build — and to be handed
	// a verdict about it. A missing dtype is the sharpest case: the empty name
	// is in no declared set, so every live neighbour would
	// answer `unsupported_dtype` and the destination would read as unreachable
	// when it is nothing of the sort.
	query, err := datagram.NewReachabilityQuery(datagram.ReachabilityQueryOpts{
		Dst:   dst,
		DType: dtype,
	})
	if err != nil {
		return nil, fmt.Errorf("datagram reachability query: %w", err)
	}
	result, err := layer.scheduler.Reachable(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("datagram reachability: %w", err)
	}

	answer := map[string]any{
		"identity":  dst.String(),
		"dtype":     dtype.String(),
		"reachable": result.Reachable(),
	}
	if reason, refused := result.Rejection(); refused {
		answer["reason"] = reason.String()
	} else if !result.Reachable() {
		answer["reason"] = "no_route"
	}
	if missing, named := result.MissingCapability(); named {
		answer["missing_capability"] = missing.String()
	}
	data, err := json.Marshal(answer)
	if err != nil {
		return nil, fmt.Errorf("marshal datagram reachability: %w", err)
	}
	return data, nil
}

// ExplainDatagramRoute returns the ranked next-hop plan a real send would
// build for one datagram — the datagram counterpart of ExplainFileRoute, with
// the same JSON shape wherever the two mean the same thing, so an operator
// reading both consoles does not have to learn two vocabularies.
//
// One field has no file-router equivalent and it exists to stop the plan from
// over-promising (§4.3):
//
//   - "first_candidate_guaranteed" is false under `explore`. The rotation
//     counter mutates on a SEND; a read-only plan neither moves nor reserves
//     it, and under concurrent sends of the same key "the next candidate" is
//     not defined in advance at all. Only `best` promises that element 0 is
//     what the send would try first.
//
// Wire schema (one entry per next-hop, in selection order):
//
//	{
//	  "route_policy": "best",
//	  "first_candidate_guaranteed": true,
//	  "candidates": [
//	    {
//	      "next_hop": "<peer identity>",
//	      "hops": 1,
//	      "protocol_version": 27,                 // NORMALIZED ranking key: min(reported, local)
//	      "connected_at": "2025-01-01T12:34:56Z", // omitted when unknown
//	      "uptime_seconds": 3600.5,               // 0 when connected_at omitted
//	      "route_source": "direct",               // TRUST axis; omitted when unattributed
//	      "discovery_plane": "mesh",              // PLANE axis; omitted when unattributed
//	      "best": true                            // true only for index 0
//	    }
//	  ]
//	}
//
// The two attribution fields are ORTHOGONAL and both are rendered: a hop found
// through the overlay that turned out to be a direct session reads
// "route_source": "direct" together with "discovery_plane": "overlay", and
// neither fact is derivable from the other. They are omitted together when the
// resolver attributed nothing, because an absent field is the honest rendering
// of "nobody said" — filling in "mesh" there would put a claim in the console
// that no plane made.
func (s *Service) ExplainDatagramRoute(
	ctx context.Context,
	dst domain.PeerIdentity,
	dtype domain.DType,
	policy domain.RoutePolicy,
) (json.RawMessage, error) {
	layer := s.datagramLayer()
	if layer == nil {
		return nil, errDatagramNotEnabled
	}
	// Same constructor, plus the policy the plan is ranked by — and the ZERO
	// policy is refused rather than defaulted, because "best" and "explore"
	// answer different questions and a plan that silently picked one would be
	// describing a send the caller did not ask about.
	query, err := datagram.NewRoutePlanQuery(datagram.RoutePlanQueryOpts{
		ReachabilityQueryOpts: datagram.ReachabilityQueryOpts{
			Dst:   dst,
			DType: dtype,
		},
		RoutePolicy: policy,
	})
	if err != nil {
		return nil, fmt.Errorf("datagram route plan query: %w", err)
	}
	plan, err := layer.scheduler.ExplainRoute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("datagram route plan: %w", err)
	}

	type wireCandidate struct {
		NextHop         string  `json:"next_hop"`
		Hops            int     `json:"hops"`
		ProtocolVersion int     `json:"protocol_version"`
		ConnectedAt     string  `json:"connected_at,omitempty"`
		UptimeSeconds   float64 `json:"uptime_seconds"`
		RouteSource     string  `json:"route_source,omitempty"`
		DiscoveryPlane  string  `json:"discovery_plane,omitempty"`
		Best            bool    `json:"best"`
	}

	now := time.Now().UTC()
	entries := plan.Entries()
	candidates := make([]wireCandidate, len(entries))
	for i, entry := range entries {
		candidate := wireCandidate{
			NextHop:         entry.NextHop.String(),
			Hops:            entry.Hops,
			ProtocolVersion: int(entry.ProtocolVersion),
			Best:            i == 0,
		}
		// Both axes or neither: they are filled by one constructor, and a
		// half-rendered attribution would suggest one of them was answered
		// and the other refused.
		if source, attributed := entry.Attribution.Source(); attributed {
			plane, _ := entry.Attribution.Plane()
			candidate.RouteSource = source.String()
			candidate.DiscoveryPlane = plane.String()
		}
		if !entry.ConnectedAt.IsZero() {
			candidate.ConnectedAt = entry.ConnectedAt.UTC().Format(time.RFC3339)
			// Clamped at zero so a future-dated connectedAt (clock skew on
			// the peer) never renders as negative uptime.
			if uptime := now.Sub(entry.ConnectedAt).Seconds(); uptime > 0 {
				candidate.UptimeSeconds = uptime
			}
		}
		candidates[i] = candidate
	}

	data, err := json.Marshal(map[string]any{
		"route_policy":               policy.String(),
		"first_candidate_guaranteed": plan.FirstCandidateGuaranteed(),
		"candidates":                 candidates,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal datagram route plan: %w", err)
	}
	return data, nil
}
