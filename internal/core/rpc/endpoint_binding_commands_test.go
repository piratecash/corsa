package rpc_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// endpoint_binding_commands_test.go pins the RPC surface of the operator's
// actions on endpoint bindings (docs/rpc/secure_session.md): all three
// commands are loopback-only — a remote socket and a listener without
// authentication are refused before the node is asked anything — the
// handlers only parse and validate, and every answer decodes back through
// rpc.Client into the DTO it was encoded from.

// The node is what RegisterAllCommands finds the provider on: without this
// the commands would silently register as unavailable.
var _ rpc.EndpointBindingProvider = (*node.Service)(nil)

var endpointBindingCommands = []string{"listEndpointBindings", "removeEndpointBinding", "demoteEndpointBindings"}

// nodeWithBindings is a node provider that also serves the binding commands,
// as node.Service does.
type nodeWithBindings struct {
	*rpcmocks.MockNodeProvider
	*rpcmocks.MockEndpointBindingProvider
}

func TestEndpointBindingCommandsAreLoopbackOnly(t *testing.T) {
	t.Parallel()
	served := rpc.NewCommandTable()
	rpc.RegisterAllCommands(served, nodeWithBindings{newDefaultNodeProvider(t), rpcmocks.NewMockEndpointBindingProvider(t)}, nil, nil, nil)
	// Unavailable on a node without the provider — and still loopback-only,
	// so the gate answers a remote caller before the 503 says anything.
	unavailable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(unavailable, newDefaultNodeProvider(t), nil, nil, nil)
	for name, table := range map[string]*rpc.CommandTable{"served": served, "unavailable": unavailable} {
		for _, command := range endpointBindingCommands {
			policy, known := table.TransportPolicyFor(command)
			if !known || policy != rpc.TransportLoopbackOnly {
				t.Fatalf("%s: %s declares %s (registered %v), want loopback_only", name, command, policy, known)
			}
		}
	}
	listed := map[string]bool{}
	for _, info := range served.Commands() {
		listed[info.Name] = true
	}
	for _, command := range endpointBindingCommands {
		if !listed[command] {
			t.Fatalf("%s is missing from help on a node that serves it", command)
		}
	}
}

// A remote socket is refused for every command — a forged forwarding header
// buys nothing — and the provider is never asked.
func TestEndpointBindingCommandsRefuseARemoteSocket(t *testing.T) {
	t.Parallel()
	table := rpc.NewCommandTable()
	rpc.RegisterEndpointBindingCommands(table, rpcmocks.NewMockEndpointBindingProvider(t))
	server, err := rpc.NewServer(config.RPC{Host: "127.0.0.1", Port: "0", Username: "u", Password: "p"}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	for _, command := range endpointBindingCommands {
		req := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec",
			strings.NewReader(fmt.Sprintf(`{"command":%q,"args":{"endpoint":"192.0.2.1:64646","endpoints":["192.0.2.1:64646"]}}`, command)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", "127.0.0.1")
		req.SetBasicAuth("u", "p")
		resp, err := server.Test(req)
		if err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s over a non-loopback socket returned %d: %s", command, resp.StatusCode, body)
		}
	}
}

// A loopback caller on a listener without authentication is refused: any
// local process is on loopback.
func TestEndpointBindingCommandsRefuseALoopbackListenerWithoutAuthentication(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterEndpointBindingCommands(table, rpcmocks.NewMockEndpointBindingProvider(t))
	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t)}
	startServer(t, cfg, table)
	for _, command := range endpointBindingCommands {
		resp, err := http.Post("http://127.0.0.1:"+cfg.Port+"/rpc/v1/exec", "application/json",
			strings.NewReader(fmt.Sprintf(`{"command":%q,"args":{"endpoint":"192.0.2.1:64646"}}`, command)))
		if err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s on an unauthenticated listener returned %d: %s", command, resp.StatusCode, body)
		}
	}
}

func startServer(t *testing.T, cfg config.RPC, table *rpc.CommandTable) {
	t.Helper()
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := server.StartAsync(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.ShutdownWithTimeout(5 * time.Second) })
}

// Every answer travels the whole loop — the handler encodes the DTO, the
// authenticated loopback server ships it, rpc.Client decodes it — and comes
// back equal to what the node returned; the node gets the arguments parsed
// into domain types.
func TestEndpointBindingAnswersRoundTripThroughTheClient(t *testing.T) {
	at := time.Unix(1780000000, 0).UTC()
	giver := domain.PeerIdentity{0x11, 0x22}
	owner := domain.PeerIdentity{0x33, 0x44}
	protected, ordinary := domain.PeerAddress("192.0.2.1:64646"), domain.PeerAddress("192.0.2.2:64646")
	pool, tcp := domain.ProtectedBindingPoolContact, domain.EndpointTransportTCP
	expires, name := at.Add(domain.EndpointBindingGeneralTerm), domain.PeerAddress("node.example:64646")
	page := domain.EndpointBindingPage{
		Bindings: []domain.EndpointBindingView{
			{Endpoint: protected, KeyForm: domain.EndpointBindingKeyCanonical, Transport: &tcp, SourceGroup: "192.0.0.0/16",
				Tier: domain.EndpointBindingTierProtected, Pool: &pool, ProtectedBy: &giver, Identity: owner, LastV2: at, FirstAt: &at},
			{Endpoint: ordinary, KeyForm: domain.EndpointBindingKeyNonCanonical, SourceGroup: domain.SourceGroupUnresolvedName,
				Tier: domain.EndpointBindingTierGeneral, Identity: owner, LastV2: at, ExpiresAt: &expires, DerivedFrom: &name, DemotedAt: &at},
		},
		Total:     3,
		NextAfter: &ordinary,
	}
	item := domain.EndpointBindingDemotionItem{Endpoint: protected, KeyForm: domain.EndpointBindingKeyCanonical, Pool: pool, ExpiresAt: expires, IdentityPinRemains: true}
	token := domain.EndpointBindingSelectionToken("ab12")
	op := domain.EndpointBindingOperationID("op-1")

	provider := rpcmocks.NewMockEndpointBindingProvider(t)
	provider.EXPECT().ListEndpointBindings(mock.Anything, domain.EndpointBindingQuery{Identity: &owner, Endpoint: &protected, After: &protected, Limit: 2}).Return(page, nil).Once()
	provider.EXPECT().ListEndpointBindings(mock.Anything, domain.EndpointBindingQuery{Limit: rpc.DefaultEndpointBindingPage}).Return(domain.EndpointBindingPage{}, nil).Once()
	provider.EXPECT().RemoveEndpointBinding(mock.Anything, ordinary).Return(domain.EndpointBindingRemoval{OperationID: op, Endpoint: ordinary, KeyForm: domain.EndpointBindingKeyCanonical}, nil).Once()
	provider.EXPECT().PlanEndpointBindingDemotion(mock.Anything, []domain.PeerAddress{protected}).Return(domain.EndpointBindingDemotionPlan{Items: []domain.EndpointBindingDemotionItem{item}, Consequence: domain.EndpointBindingDemotionConsequence, Token: token}, nil).Once()
	provider.EXPECT().DemoteEndpointBindings(mock.Anything, []domain.PeerAddress{protected}, token).Return(domain.EndpointBindingDemotionResult{OperationID: op, Items: []domain.EndpointBindingDemotionItem{item}, Consequence: domain.EndpointBindingDemotionConsequence}, nil).Once()

	table := rpc.NewCommandTable()
	rpc.RegisterEndpointBindingCommands(table, provider)
	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
	startServer(t, cfg, table)
	client := rpc.NewClient(cfg)

	gotPage, err := client.ListEndpointBindings(rpc.EndpointBindingListRequest{Identity: owner.String(), Endpoint: string(protected), After: string(protected), Limit: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	wantPage := rpc.EndpointBindingPageDTO{
		Bindings: []rpc.EndpointBindingDTO{
			{Endpoint: protected, KeyForm: domain.EndpointBindingKeyCanonical, Transport: &tcp, SourceGroup: "192.0.0.0/16",
				Tier: domain.EndpointBindingTierProtected, Pool: &pool, ProtectedBy: &giver, Identity: owner, LastV2: at, FirstAt: &at},
			{Endpoint: ordinary, KeyForm: domain.EndpointBindingKeyNonCanonical, SourceGroup: domain.SourceGroupUnresolvedName,
				Tier: domain.EndpointBindingTierGeneral, Identity: owner, LastV2: at, ExpiresAt: &expires, DerivedFrom: &name, DemotedAt: &at},
		},
		Total:     3,
		NextAfter: &ordinary,
	}
	if !reflect.DeepEqual(gotPage, wantPage) {
		t.Fatalf("list decoded as %+v, want %+v", gotPage, wantPage)
	}
	if empty, err := client.ListEndpointBindings(rpc.EndpointBindingListRequest{}); err != nil || empty.Bindings == nil || len(empty.Bindings) != 0 {
		t.Fatalf("an empty list decoded as %+v, %v; want an empty array", empty, err)
	}

	removal, err := client.RemoveEndpointBinding(string(ordinary))
	if err != nil || removal != (rpc.EndpointBindingRemovalDTO{OperationID: op, Endpoint: ordinary, KeyForm: domain.EndpointBindingKeyCanonical}) {
		t.Fatalf("removal decoded as %+v, %v", removal, err)
	}

	wantItem := rpc.EndpointBindingDemotionItemDTO{Endpoint: protected, KeyForm: domain.EndpointBindingKeyCanonical, Pool: pool, ExpiresAt: expires, IdentityPinRemains: true}
	plan, err := client.DemoteEndpointBindings([]string{string(protected)}, "")
	wantPlan := rpc.EndpointBindingDemotionDTO{Confirmed: false, Endpoints: []rpc.EndpointBindingDemotionItemDTO{wantItem}, Consequence: domain.EndpointBindingDemotionConsequence, ConfirmToken: &token}
	if err != nil || !reflect.DeepEqual(plan, wantPlan) {
		t.Fatalf("dry run decoded as %+v, %v; want %+v", plan, err, wantPlan)
	}
	result, err := client.DemoteEndpointBindings([]string{string(protected)}, string(*plan.ConfirmToken))
	wantResult := rpc.EndpointBindingDemotionDTO{Confirmed: true, Endpoints: []rpc.EndpointBindingDemotionItemDTO{wantItem}, Consequence: domain.EndpointBindingDemotionConsequence, OperationID: &op}
	if err != nil || !reflect.DeepEqual(result, wantResult) {
		t.Fatalf("confirmation decoded as %+v, %v; want %+v", result, err, wantResult)
	}
}

// The handlers only parse and validate: a malformed argument never reaches
// the node, the node's refusals map to their kinds by errors.Is, and there is
// no way to name more than one binding in a removal.
func TestEndpointBindingHandlersValidateAndMapRefusals(t *testing.T) {
	t.Parallel()
	provider := rpcmocks.NewMockEndpointBindingProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterEndpointBindingCommands(table, provider)
	execute := func(command string, args map[string]interface{}) rpc.CommandResponse {
		return table.Execute(rpc.CommandRequest{Name: command, Args: args})
	}

	for name, tc := range map[string]struct {
		command string
		args    map[string]interface{}
	}{
		"list: identity not hex":       {command: "listEndpointBindings", args: map[string]interface{}{"identity": "not-an-identity"}},
		"list: limit zero":             {command: "listEndpointBindings", args: map[string]interface{}{"limit": "0"}},
		"list: limit over the page":    {command: "listEndpointBindings", args: map[string]interface{}{"limit": float64(domain.MaxEndpointBindingPage + 1)}},
		"list: limit not a number":     {command: "listEndpointBindings", args: map[string]interface{}{"limit": "ten"}},
		"list: endpoint not a string":  {command: "listEndpointBindings", args: map[string]interface{}{"endpoint": float64(1)}},
		"remove: no endpoint":          {command: "removeEndpointBinding", args: nil},
		"remove: a list of endpoints":  {command: "removeEndpointBinding", args: map[string]interface{}{"endpoint": []interface{}{"192.0.2.1:64646", "192.0.2.2:64646"}}},
		"remove: endpoint too long":    {command: "removeEndpointBinding", args: map[string]interface{}{"endpoint": strings.Repeat("a", 600)}},
		"demote: no endpoints":         {command: "demoteEndpointBindings", args: nil},
		"demote: an endpoint not text": {command: "demoteEndpointBindings", args: map[string]interface{}{"endpoints": []interface{}{"192.0.2.1:64646", float64(2)}}},
		"demote: confirm not text":     {command: "demoteEndpointBindings", args: map[string]interface{}{"endpoints": "192.0.2.1:64646", "confirm": float64(1)}},
	} {
		if resp := execute(tc.command, tc.args); resp.Error == nil || resp.ErrorKind != rpc.ErrValidation {
			t.Fatalf("%s: error %v, kind %v; want a validation error", name, resp.Error, resp.ErrorKind)
		}
	}

	refused := domain.PeerAddress("192.0.2.9:64646")
	provider.EXPECT().RemoveEndpointBinding(mock.Anything, refused).Return(domain.EndpointBindingRemoval{}, fmt.Errorf("%w: %s", domain.ErrEndpointBindingProtected, refused)).Once()
	provider.EXPECT().RemoveEndpointBinding(mock.Anything, domain.PeerAddress("192.0.2.10:64646")).Return(domain.EndpointBindingRemoval{}, domain.ErrEndpointBindingStoreUnavailable).Once()
	provider.EXPECT().RemoveEndpointBinding(mock.Anything, domain.PeerAddress("192.0.2.11:64646")).Return(domain.EndpointBindingRemoval{}, errors.New("secure session store: disk full")).Once()
	// The console's key=value form: a comma-separated list.
	provider.EXPECT().PlanEndpointBindingDemotion(mock.Anything, []domain.PeerAddress{"192.0.2.1:64646", "192.0.2.2:64646"}).Return(domain.EndpointBindingDemotionPlan{}, domain.ErrEndpointBindingNotProtected).Once()
	provider.EXPECT().DemoteEndpointBindings(mock.Anything, []domain.PeerAddress{"192.0.2.1:64646"}, domain.EndpointBindingSelectionToken("stale")).Return(domain.EndpointBindingDemotionResult{}, domain.ErrEndpointBindingSelectionChanged).Once()
	for name, tc := range map[string]struct {
		command string
		args    map[string]interface{}
		kind    rpc.ErrorKind
	}{
		"protected":         {command: "removeEndpointBinding", args: map[string]interface{}{"endpoint": string(refused)}, kind: rpc.ErrValidation},
		"store unavailable": {command: "removeEndpointBinding", args: map[string]interface{}{"endpoint": "192.0.2.10:64646"}, kind: rpc.ErrUnavailable},
		"write failed":      {command: "removeEndpointBinding", args: map[string]interface{}{"endpoint": "192.0.2.11:64646"}, kind: rpc.ErrInternal},
		"not protected":     {command: "demoteEndpointBindings", args: map[string]interface{}{"endpoints": "192.0.2.1:64646, 192.0.2.2:64646"}, kind: rpc.ErrValidation},
		"selection changed": {command: "demoteEndpointBindings", args: map[string]interface{}{"endpoints": []interface{}{"192.0.2.1:64646"}, "confirm": "stale"}, kind: rpc.ErrValidation},
	} {
		if resp := execute(tc.command, tc.args); resp.Error == nil || resp.ErrorKind != tc.kind {
			t.Fatalf("%s: error %v, kind %v; want kind %v", name, resp.Error, resp.ErrorKind, tc.kind)
		}
	}
}

// The console reaches the commands positionally where one shape is obvious:
// the endpoint of a removal, the endpoints of a dry run. A confirmation and
// the list's filters go by key=value.
func TestEndpointBindingConsoleInput(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]rpc.CommandRequest{
		"removeEndpointBinding 192.0.2.1:64646":                         {Name: "removeEndpointBinding", Args: map[string]interface{}{"endpoint": "192.0.2.1:64646"}},
		"demoteEndpointBindings 192.0.2.1:64646 192.0.2.2:64646":        {Name: "demoteEndpointBindings", Args: map[string]interface{}{"endpoints": []interface{}{"192.0.2.1:64646", "192.0.2.2:64646"}}},
		"demoteendpointbindings endpoints=192.0.2.1:64646 confirm=ab12": {Name: "demoteEndpointBindings", Args: map[string]interface{}{"endpoints": "192.0.2.1:64646", "confirm": "ab12"}},
		"listEndpointBindings":                                          {Name: "listEndpointBindings"},
		"listendpointbindings limit=5":                                  {Name: "listEndpointBindings", Args: map[string]interface{}{"limit": "5"}},
	} {
		got, err := rpc.ParseConsoleInput(input)
		if err != nil {
			t.Fatalf("%q: %v", input, err)
		}
		if got.Name != want.Name || !reflect.DeepEqual(got.Args, want.Args) {
			t.Fatalf("%q parsed as %+v, want %+v", input, got, want)
		}
	}
	for _, input := range []string{"removeEndpointBinding", "removeEndpointBinding 192.0.2.1:64646 192.0.2.2:64646", "demoteEndpointBindings", "listEndpointBindings 192.0.2.1:64646"} {
		if _, err := rpc.ParseConsoleInput(input); err == nil {
			t.Fatalf("%q parsed without an error", input)
		}
	}
}
