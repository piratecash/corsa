package rpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_commands.go registers the read-only RPC surface of the datagram
// transport layer: the local plane summary, the reachability probe and the
// route plan.
//
// The shape follows RegisterRoutingCommands exactly — one registration
// function, unavailable-when-nil, argument parsing at the boundary — because
// an operator switching between fetchRouteSummary and fetchDatagramSummary
// should not have to learn a second convention. Everything these handlers do
// is ask the node; every decision behind the answer belongs to the layer.
//
// The one rule this boundary adds to "parse and forward" is that it accepts
// EXACTLY what the wire accepts. Both diagnostics answer for one concrete
// datagram, so an argument set no legal datagram can be built from is refused
// here rather than answered about: a verdict on a frame a real send would drop
// before the queue is not a diagnostic, it is a wrong answer with a route in
// it.
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §5, §10.

// RegisterDatagramCommands registers the datagram observability commands.
// When the provider is nil (a node built without the plane) the commands are
// registered as unavailable: hidden from help and autocomplete, and 503 on
// execution, consistent with every other mode-gated group.
func RegisterDatagramCommands(t *CommandTable, dp DatagramProvider) {
	summaryInfo := CommandInfo{
		Name:        "fetchDatagramSummary",
		Description: "Get datagram transport layer diagnostics: decision counters, per-neighbour admission budget, class queue depths, anti-replay cache counters and the limits in force",
		Category:    "datagram",
	}
	reachableInfo := CommandInfo{
		Name:        "datagramReachable",
		Description: "Probe whether a datagram of the given type would find a first hop to the destination (dtype is required — it decides the last-hop gate)",
		Category:    "datagram",
		Usage:       "<identity> <dtype>",
	}
	explainInfo := CommandInfo{
		Name:        "explainDatagramRoute",
		Description: "Explain the ranked next-hop plan a datagram of the given type would use (best, hops, protocol_version, connected_at, uptime_seconds)",
		Category:    "datagram",
		Usage:       "<identity> <dtype> [route_policy]",
	}

	if dp == nil {
		t.RegisterUnavailable(summaryInfo)
		t.RegisterUnavailable(reachableInfo)
		t.RegisterUnavailable(explainInfo)
		return
	}

	t.Register(summaryInfo, datagramSummaryHandler(dp))
	t.Register(reachableInfo, datagramReachableHandler(dp))
	t.Register(explainInfo, datagramExplainRouteHandler(dp))
}

// datagramSummaryHandler serves the local plane diagnostic.
func datagramSummaryHandler(dp DatagramProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		data, err := dp.FetchDatagramSummary()
		if err != nil {
			// The layer being off is a service STATE, not a caller mistake
			// and not a system failure: 503 tells a client to stop asking
			// until the node is reconfigured, which is exactly right.
			return unavailableError(fmt.Errorf("fetch datagram summary: %w", err))
		}
		return CommandResponse{Data: data}
	}
}

// datagramReachableHandler serves the reachability probe.
func datagramReachableHandler(dp DatagramProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		query, response, ok := parseDatagramProbeQuery(req)
		if !ok {
			return response
		}
		data, err := dp.DatagramReachable(commandContext(req), query.dst, query.dtype)
		if err != nil {
			return unavailableError(fmt.Errorf("datagram reachability probe: %w", err))
		}
		return CommandResponse{Data: data}
	}
}

// datagramExplainRouteHandler serves the route plan.
func datagramExplainRouteHandler(dp DatagramProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		query, response, ok := parseDatagramPlanQuery(req)
		if !ok {
			return response
		}
		data, err := dp.ExplainDatagramRoute(commandContext(req), query.dst, query.dtype, query.policy)
		if err != nil {
			return unavailableError(fmt.Errorf("explain datagram route: %w", err))
		}
		return CommandResponse{Data: data}
	}
}

// commandContext is the request's context, or Background when the caller did
// not supply one. The layer's read-only surfaces take a context because every
// I/O-shaped method in this project does; a nil one here would panic the
// moment a resolver adapter started honouring cancellation.
func commandContext(req CommandRequest) context.Context {
	if req.Ctx == nil {
		return context.Background()
	}
	return req.Ctx
}

// datagramQuery is the datagram both read-only surfaces answer about: the
// fields the send gates depend on (§4.3) plus the route policy the frame must
// carry. Parsing them in one place is what stops the probe and the plan from
// disagreeing about what a missing dtype means — and what it means is now
// stated once: it is refused.
type datagramQuery struct {
	dst    domain.PeerIdentity
	dtype  domain.DType
	policy domain.RoutePolicy
}

// datagramSendShapeSrc stands in for the fields of a routed frame that the
// LAYER fixes rather than the operator: header src is RoutedFrameBuilder's
// LocalID, and the auth block is drawn from the node's key and clock. They are
// filled here with well-formed-by-construction values (a non-zero identity,
// correctly sized key/salt/signature), so every failure the shape check can
// report is attributable to an ARGUMENT — see TestDatagramQueryDescribesASend,
// which pins that the stand-ins alone never fail Validate.
var datagramSendShapeSrc = domain.PeerIdentity{0x01}

// parseDatagramProbeQuery parses and wire-checks the probe's arguments.
//
// The probe takes no route_policy — reachability does not depend on it — but a
// routed frame must carry one (§2.1), so the check runs against `best`, which
// is also this group's default. The two legal policies validate identically,
// so the choice cannot change the verdict.
func parseDatagramProbeQuery(req CommandRequest) (datagramQuery, CommandResponse, bool) {
	query, response, ok := parseDatagramQuery(req)
	if !ok {
		return datagramQuery{}, response, false
	}
	query.policy = domain.RoutePolicyBest
	return checkDatagramSendShape(query)
}

// parseDatagramPlanQuery parses and wire-checks the plan's arguments, the
// route policy included: the plan renders an order, and the order is the
// policy's.
func parseDatagramPlanQuery(req CommandRequest) (datagramQuery, CommandResponse, bool) {
	query, response, ok := parseDatagramQuery(req)
	if !ok {
		return datagramQuery{}, response, false
	}
	policy, response, ok := parseDatagramRoutePolicy(req)
	if !ok {
		return datagramQuery{}, response, false
	}
	query.policy = policy
	return checkDatagramSendShape(query)
}

// checkDatagramSendShape refuses a query no legal datagram could be built from.
//
// The diagnostic's whole value is that it predicts a real send, so the
// arguments are assembled into the frame RoutedFrameBuilder.Build would produce
// and handed to the SAME Validate that build runs. Without it the boundary can
// only be as strict as whoever last edited it remembered to be, and a probe
// that answers "reachable" for a frame the builder refuses is worse than no
// probe: the operator acts on a route that no send of that frame can ever use.
//
// The wire's refusal is SURFACED rather than swallowed or restated: it already
// names the clause and the offending value.
func checkDatagramSendShape(query datagramQuery) (datagramQuery, CommandResponse, bool) {
	if err := query.sendFrame().Validate(); err != nil {
		return datagramQuery{}, validationError(fmt.Errorf(
			"these arguments describe no legal datagram, so no send of them could be routed: %w", err)), false
	}
	return query, CommandResponse{}, true
}

// sendFrame renders the query as the routed frame a local send of it would put
// on the wire (§2.1, §3.1). Every field the operator does not supply takes the
// value the layer fixes for it, so the frame is the one Validate is asked
// about, not a lookalike:
//
//   - class is `control`: it is not an argument of either surface — the gates
//     never read it — and of the two ceilings it is the strict one;
//   - ttl equals auth.max_ttl at the origin, which is what makes `ttl >
//     max_ttl` impossible for a frame this node produced (§4.1 rule 2);
//   - payload is empty, because neither surface accepts one.
func (q datagramQuery) sendFrame() protocol.DatagramFrame {
	return protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         datagramSendShapeSrc,
		Dst:         q.dst,
		TTL:         domain.DatagramDefaultMaxHops,
		RoutePolicy: q.policy,
		DType:       q.dtype,
		Auth: &protocol.DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			PubKey:      make([]byte, domain.DatagramPubKeyBytes),
			Salt:        make([]byte, domain.DatagramSaltBytes),
			MaxTTL:      domain.DatagramDefaultMaxHops,
			Sig:         make([]byte, domain.DatagramSigBytes),
		},
	}
}

// parseDatagramQuery validates the shared arguments at the transport boundary.
//
// The destination is mandatory and is parsed here so a console typo never
// reaches the layer as a garbled identity — the same gate fetchRouteLookup and
// explainFileRoute apply. ParsePeerIdentity accepts the all-zero 40-hex form
// with a nil error, so the explicit IsZero check is required to reject the
// absent sentinel.
//
// dtype is MANDATORY, and that is a correction of what this file used to
// promise. The description said an absent dtype "exercises no last-hop gate",
// but nothing in the layer implements that reading: the query carries a
// domain.DType, an absent one is the empty string, and the last-hop gate asks
// DeclaredDTypes.Supports("") — which is false for every peer, because the
// empty name is in no declared set. The operator was
// therefore told `unsupported_dtype` about a destination that is perfectly
// reachable, which is the worst answer a reachability probe can give.
//
// Of the two honest fixes — teach the probe an explicit "no type" that really
// skips the gate, or require the type — only the second is available from here:
// the first needs the LAYER to represent an unset dtype (see
// docs/rpc/datagram.md), and the gate is the layer's decision to make. Requiring
// it also matches what the node's own DatagramReachable has always documented:
// "dtype is a mandatory input, not decoration".
func parseDatagramQuery(req CommandRequest) (datagramQuery, CommandResponse, bool) {
	identityArg, _ := req.Args["identity"].(string)
	identityArg = strings.TrimSpace(identityArg)
	if identityArg == "" {
		return datagramQuery{}, validationError(fmt.Errorf("identity is required")), false
	}
	dst, err := domain.ParsePeerIdentity(identityArg)
	if err != nil {
		return datagramQuery{}, validationError(fmt.Errorf("identity must be a valid peer identity: %w", err)), false
	}
	if dst.IsZero() {
		return datagramQuery{}, validationError(fmt.Errorf("identity must not be the zero peer identity")), false
	}

	raw, _ := req.Args["dtype"].(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return datagramQuery{}, validationError(fmt.Errorf(
			"dtype is required: the last-hop gate is decided by the type, and an absent one is refused by every peer")), false
	}
	dtype, err := domain.ParseDType(raw)
	if err != nil {
		return datagramQuery{}, validationError(fmt.Errorf("dtype must be a valid datagram type: %w", err)), false
	}
	return datagramQuery{dst: dst, dtype: dtype}, CommandResponse{}, true
}

// parseDatagramRoutePolicy reads the optional route_policy argument. An absent
// value means `best`, which is both the common case and the only policy whose
// plan promises that element 0 is what a send would really try first (§4.3).
// Anything else goes through the wire's own parser, so the RPC admits exactly
// the two values the mode matrix admits for a routed frame (§2.1).
func parseDatagramRoutePolicy(req CommandRequest) (domain.RoutePolicy, CommandResponse, bool) {
	raw, _ := req.Args["route_policy"].(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return domain.RoutePolicyBest, CommandResponse{}, true
	}
	policy, err := domain.ParseRoutePolicy(raw)
	if err != nil {
		return "", validationError(fmt.Errorf("route_policy must be best or explore: %w", err)), false
	}
	return policy, CommandResponse{}, true
}
