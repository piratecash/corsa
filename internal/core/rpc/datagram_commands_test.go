package rpc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// stubDatagramProvider is a provider that only has to EXIST: these tests read
// the registration surface — usage strings, availability — and never execute a
// handler that would reach it.
type stubDatagramProvider struct{}

func (stubDatagramProvider) FetchDatagramSummary() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (stubDatagramProvider) DatagramReachable(
	context.Context, domain.PeerIdentity, domain.DType,
) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (stubDatagramProvider) ExplainDatagramRoute(
	context.Context, domain.PeerIdentity, domain.DType, domain.RoutePolicy,
) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

// datagram_commands_test.go covers the transport boundary of the datagram
// observability group: registration and argument parsing. Everything the
// handlers decide belongs to the node and to the layer, and is tested there.

// TestDatagramCommandsUnavailableWithoutProvider pins the mode gate: a build
// whose node does not implement DatagramProvider must answer 503 — the command
// exists, the plane does not — and must stay out of help and autocomplete.
func TestDatagramCommandsUnavailableWithoutProvider(t *testing.T) {
	t.Parallel()

	table := NewCommandTable()
	RegisterDatagramCommands(table, nil)

	for _, name := range []string{"fetchDatagramSummary", "datagramReachable", "explainDatagramRoute"} {
		response := table.Execute(CommandRequest{Name: name})
		if response.ErrorKind != ErrUnavailable {
			t.Fatalf("%s error kind = %v, want ErrUnavailable", name, response.ErrorKind)
		}
		for _, listed := range table.Commands() {
			if listed.Name == name {
				t.Fatalf("%s is listed in help although it is unavailable", name)
			}
		}
	}
}

// TestParseConsoleInputDatagramReachable pins the positional shape of the
// probe. The PARSER stays permissive — it hands whatever positions it read to
// the handler, which is where the required arguments are enforced — so the
// console can report a missing dtype as a validation error rather than as a
// parse failure the user cannot read.
func TestParseConsoleInputDatagramReachable(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	request, err := ParseConsoleInput("datagram_reachable " + identity + " push_identity")
	if err != nil {
		t.Fatalf("ParseConsoleInput: %v", err)
	}
	if request.Name != "datagramReachable" {
		t.Fatalf("command = %q, want the canonical camelCase name", request.Name)
	}
	for key, want := range map[string]string{
		"identity": identity,
		"dtype":    "push_identity",
	} {
		if got, _ := request.Args[key].(string); got != want {
			t.Fatalf("arg %q = %q, want %q", key, got, want)
		}
	}

	// Identity alone still PARSES: the console parser assigns positions and
	// does not know which of them the command requires. The refusal belongs to
	// parseDatagramQuery, which is where the last-hop gate's needs are known
	// (see TestDatagramQueryRequiresADType).
	bare, err := ParseConsoleInput("datagramReachable " + identity)
	if err != nil {
		t.Fatalf("ParseConsoleInput (identity only): %v", err)
	}
	if got, _ := bare.Args["identity"].(string); got != identity {
		t.Fatalf("identity = %q, want %q", got, identity)
	}
	if got, _ := bare.Args["dtype"].(string); got != "" {
		t.Fatalf("dtype = %q, want empty", got)
	}

	if _, err := ParseConsoleInput("datagramReachable"); err == nil {
		t.Fatal("a probe without a destination was accepted")
	}
}

// TestDatagramConsoleTakesNoPathRequirement is the CLI half of the envelope
// change. `req_caps` used to be the third positional argument of both
// commands; the envelope carries no such field any more, so a third word is a
// mistake the parser has to name rather than a filter it silently applies.
func TestDatagramConsoleTakesNoPathRequirement(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	if _, err := ParseConsoleInput(
		"datagram_reachable " + identity + " push_identity mesh_datagram_durable_v1",
	); err == nil {
		t.Fatal("datagramReachable still accepts a third positional argument")
	}
	if _, err := ParseConsoleInput(
		"explain_datagram_route " + identity + " push_identity mesh_datagram_durable_v1 explore",
	); err == nil {
		t.Fatal("explainDatagramRoute still accepts four positional arguments")
	}
}

// TestParseConsoleInputExplainDatagramRoute pins the plan's third positional
// argument: the route policy decides what element 0 of the plan may promise.
func TestParseConsoleInputExplainDatagramRoute(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	request, err := ParseConsoleInput("explain_datagram_route " + identity + " push_identity explore")
	if err != nil {
		t.Fatalf("ParseConsoleInput: %v", err)
	}
	if request.Name != "explainDatagramRoute" {
		t.Fatalf("command = %q, want the canonical camelCase name", request.Name)
	}
	if got, _ := request.Args["route_policy"].(string); got != "explore" {
		t.Fatalf("route_policy = %q, want %q", got, "explore")
	}

	// Absent policy is `best`, resolved by the handler rather than the parser,
	// so the parser must hand through an empty string instead of inventing one.
	bare, err := ParseConsoleInput("explainDatagramRoute " + identity)
	if err != nil {
		t.Fatalf("ParseConsoleInput (identity only): %v", err)
	}
	if got, _ := bare.Args["route_policy"].(string); got != "" {
		t.Fatalf("route_policy = %q, want empty so the handler applies the default", got)
	}
}

// TestDatagramQueryValidation pins the boundary checks: a destination is
// mandatory and the absent-identity sentinel is refused.
func TestDatagramQueryValidation(t *testing.T) {
	t.Parallel()

	if _, _, ok := parseDatagramQuery(CommandRequest{Args: map[string]interface{}{}}); ok {
		t.Fatal("a query without an identity was accepted")
	}
	zero := map[string]interface{}{"identity": "0000000000000000000000000000000000000000"}
	if _, _, ok := parseDatagramQuery(CommandRequest{Args: zero}); ok {
		t.Fatal("the zero peer identity was accepted as a destination")
	}

	args := map[string]interface{}{
		"identity": "00f39d89f345eb1613bb2fa02ee883a214a6a697",
		"dtype":    "push_identity",
	}
	query, _, ok := parseDatagramQuery(CommandRequest{Args: args})
	if !ok {
		t.Fatal("a well-formed query was rejected")
	}
	if query.dtype != "push_identity" {
		t.Fatalf("dtype = %q, want push_identity", query.dtype)
	}
}

// TestDatagramQueryRequiresADType is finding 4.
//
// The command described an absent dtype as "exercises no last-hop gate", and
// nothing implemented that reading: the query carries a domain.DType, an absent
// one is the empty string, and the gate asks the destination's declared set
// about that empty name — which no peer declares and no baseline contains. The
// operator got `unsupported_dtype` for a destination that was reachable, which
// is the one answer a reachability probe must never invent.
//
// The fix chosen here is the one available WITHOUT changing the layer: require
// the type. Skipping the gate honestly would need the layer to represent an
// unset dtype, and the gate is the layer's decision to make (see
// docs/rpc/datagram.md).
//
// The mutation this kills: restoring the "absent dtype is legal" branch — the
// query then carries a zero DType and the probe answers unsupported_dtype.
func TestDatagramQueryRequiresADType(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"

	if _, response, ok := parseDatagramQuery(CommandRequest{
		Args: map[string]interface{}{"identity": identity},
	}); ok {
		t.Fatal("a query without a dtype was accepted: every live neighbour would answer unsupported_dtype")
	} else if response.ErrorKind != ErrValidation {
		t.Fatalf("error kind = %v, want ErrValidation — a missing argument is a caller mistake", response.ErrorKind)
	}

	if _, _, ok := parseDatagramQuery(CommandRequest{
		Args: map[string]interface{}{"identity": identity, "dtype": "   "},
	}); ok {
		t.Fatal("a blank dtype was accepted")
	}

	query, _, ok := parseDatagramQuery(CommandRequest{
		Args: map[string]interface{}{"identity": identity, "dtype": "push_identity"},
	})
	if !ok {
		t.Fatal("a query naming a dtype was rejected")
	}
	if query.dtype != "push_identity" {
		t.Fatalf("dtype = %q, want push_identity", query.dtype)
	}

	// Every surface, not just the one the finding named: the two commands take
	// the same arguments, and a fix applied to one of them leaves the other
	// answering about a datagram no send could build.
	for surface, parse := range datagramQueryParsers() {
		if _, response, ok := parse(CommandRequest{
			Args: map[string]interface{}{"identity": identity},
		}); ok {
			t.Fatalf("%s accepted a query without a dtype", surface)
		} else if response.ErrorKind != ErrValidation {
			t.Fatalf("%s error kind = %v, want ErrValidation", surface, response.ErrorKind)
		}
	}
}

// datagramQueryParsers is every argument-parsing entry point of the group,
// keyed by the surface it serves. Tests iterate it instead of naming one, so a
// third command cannot be added without either joining this map or visibly
// leaving it.
func datagramQueryParsers() map[string]func(CommandRequest) (datagramQuery, CommandResponse, bool) {
	return map[string]func(CommandRequest) (datagramQuery, CommandResponse, bool){
		"probe": parseDatagramProbeQuery,
		"plan":  parseDatagramPlanQuery,
	}
}

// TestDatagramQueryDescribesASend is the second half of the finding: whatever
// the boundary accepts must describe a frame a real send could build.
//
// It also pins that the stand-ins for src and auth — the fields the layer fills
// from node state, which the RPC cannot know — never fail Validate on their
// own. Without that, checkDatagramSendShape could refuse every query and this
// suite would still be green.
func TestDatagramQueryDescribesASend(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	for _, tc := range []struct {
		name   string
		args   map[string]interface{}
		parse  func(CommandRequest) (datagramQuery, CommandResponse, bool)
		policy domain.RoutePolicy
	}{
		{
			name:   "probe",
			args:   map[string]interface{}{"identity": identity, "dtype": "push_identity"},
			parse:  parseDatagramProbeQuery,
			policy: domain.RoutePolicyBest,
		},
		{
			name: "plan under explore",
			args: map[string]interface{}{
				"identity": identity, "dtype": "push_identity", "route_policy": "explore",
			},
			parse:  parseDatagramPlanQuery,
			policy: domain.RoutePolicyExplore,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			query, response, ok := tc.parse(CommandRequest{Args: tc.args})
			if !ok {
				t.Fatalf("a legal query was refused: %v", response.Error)
			}
			if query.policy != tc.policy {
				t.Fatalf("policy = %q, want %q", query.policy, tc.policy)
			}
			frame := query.sendFrame()
			if err := frame.Validate(); err != nil {
				t.Fatalf("the probe describes a frame a real send would refuse: %v", err)
			}
			// The layer-fixed fields must be the layer's, not plausible-looking
			// stand-ins of their own: a frame validated with another version, mode
			// or ttl is not the frame the send builds.
			switch {
			case frame.Version != domain.DatagramHeaderVersion:
				t.Fatalf("version = %d, want %d", frame.Version, domain.DatagramHeaderVersion)
			case frame.Mode != domain.DatagramModeRouted:
				t.Fatalf("mode = %q, want routed", frame.Mode)
			case frame.TTL != domain.DatagramDefaultMaxHops || frame.Auth.MaxTTL != frame.TTL:
				t.Fatalf("ttl/max_ttl = %d/%d, want %d for a frame at its origin",
					frame.TTL, frame.Auth.MaxTTL, domain.DatagramDefaultMaxHops)
			case frame.RoutePolicy != tc.policy:
				t.Fatalf("frame route_policy = %q, want the one the query carries (%q)", frame.RoutePolicy, tc.policy)
			case frame.DType != "push_identity":
				t.Fatalf("dtype = %q, want the argument", frame.DType)
			}
		})
	}
}

// TestDatagramSendShapeIsCheckedByBothSurfaces stops the fix from being applied
// to one command and forgotten on its sibling: the probe and the plan take the
// same arguments and must refuse the same input, WITHOUT reaching the provider.
// A handler that asks the layer first has already spent the answer.
func TestDatagramSendShapeIsCheckedByBothSurfaces(t *testing.T) {
	t.Parallel()

	const identity = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	provider := &countingDatagramProvider{}
	table := NewCommandTable()
	RegisterDatagramCommands(table, provider)

	for _, name := range []string{"datagramReachable", "explainDatagramRoute"} {
		response := table.Execute(CommandRequest{
			Name: name,
			Args: map[string]interface{}{
				"identity": identity,
				"dtype":    "Push Identity",
			},
		})
		if response.ErrorKind != ErrValidation {
			t.Fatalf("%s error kind = %v, want ErrValidation", name, response.ErrorKind)
		}
	}
	if provider.calls != 0 {
		t.Fatalf("the layer was asked %d times about a datagram that cannot exist", provider.calls)
	}
}

// TestDatagramSendShapeRefusesWhatTheParsersMiss pins the backstop itself. The
// argument parsers reject these queries first, so the only way to exercise the
// check is to hand it a query built directly — which is also what a future
// third command would do if it skipped a parser.
func TestDatagramSendShapeRefusesWhatTheParsersMiss(t *testing.T) {
	t.Parallel()

	dst, err := domain.ParsePeerIdentity("00f39d89f345eb1613bb2fa02ee883a214a6a697")
	if err != nil {
		t.Fatalf("ParsePeerIdentity: %v", err)
	}
	for name, query := range map[string]datagramQuery{
		"no route policy": {dst: dst, dtype: "push_identity"},
		"no destination":  {dtype: "push_identity", policy: domain.RoutePolicyBest},
		"no dtype":        {dst: dst, policy: domain.RoutePolicyBest},
		"malformed dtype": {dst: dst, dtype: "Push Identity", policy: domain.RoutePolicyBest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, response, ok := checkDatagramSendShape(query); ok {
				t.Fatal("a query describing no legal datagram passed the send-shape check")
			} else if response.ErrorKind != ErrValidation {
				t.Fatalf("error kind = %v, want ErrValidation", response.ErrorKind)
			}
		})
	}
}

// countingDatagramProvider records whether a handler reached the layer at all.
type countingDatagramProvider struct {
	calls int
}

func (p *countingDatagramProvider) FetchDatagramSummary() (json.RawMessage, error) {
	p.calls++
	return json.RawMessage(`{}`), nil
}

func (p *countingDatagramProvider) DatagramReachable(
	context.Context, domain.PeerIdentity, domain.DType,
) (json.RawMessage, error) {
	p.calls++
	return json.RawMessage(`{}`), nil
}

func (p *countingDatagramProvider) ExplainDatagramRoute(
	context.Context, domain.PeerIdentity, domain.DType, domain.RoutePolicy,
) (json.RawMessage, error) {
	p.calls++
	return json.RawMessage(`{}`), nil
}

// TestDatagramUsageRequiresTheDType keeps the OPERATOR-FACING text in step with
// the check above. Usage strings are the only contract a console user reads,
// and a `[dtype]` there while the handler refuses an absent one is the same
// documentation-versus-code split this finding started as.
func TestDatagramUsageRequiresTheDType(t *testing.T) {
	t.Parallel()

	table := NewCommandTable()
	RegisterDatagramCommands(table, stubDatagramProvider{})
	for _, info := range table.Commands() {
		switch info.Name {
		case "datagramReachable", "explainDatagramRoute":
			if strings.Contains(info.Usage, "[dtype]") {
				t.Errorf("%s usage still calls dtype optional: %q", info.Name, info.Usage)
			}
			if !strings.Contains(info.Usage, "<dtype>") {
				t.Errorf("%s usage does not require a dtype: %q", info.Name, info.Usage)
			}
			if strings.Contains(info.Usage, "req_caps") {
				t.Errorf("%s usage still offers req_caps, which the envelope no longer carries: %q", info.Name, info.Usage)
			}
		}
	}
}
