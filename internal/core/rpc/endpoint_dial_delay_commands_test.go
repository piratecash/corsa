package rpc_test

import (
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

// endpoint_dial_delay_commands_test.go pins the RPC surface of the per-endpoint
// dial delays (docs/rpc/secure_session.md): the list and
// the retry by hand are loopback-only with authentication, the handlers only
// parse and validate, and the answers decode back through rpc.Client.

var _ rpc.EndpointDialDelayProvider = (*node.Service)(nil)

var endpointDialDelayCommands = []string{"listEndpointDialDelays", "retryEndpointDial"}

type nodeWithDialDelays struct {
	*rpcmocks.MockNodeProvider
	*rpcmocks.MockEndpointDialDelayProvider
}

func TestEndpointDialDelayCommandsAreLoopbackOnly(t *testing.T) {
	t.Parallel()
	served := rpc.NewCommandTable()
	rpc.RegisterAllCommands(served, nodeWithDialDelays{newDefaultNodeProvider(t), rpcmocks.NewMockEndpointDialDelayProvider(t)}, nil, nil, nil)
	unavailable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(unavailable, newDefaultNodeProvider(t), nil, nil, nil)
	for name, table := range map[string]*rpc.CommandTable{"served": served, "unavailable": unavailable} {
		for _, command := range endpointDialDelayCommands {
			policy, known := table.TransportPolicyFor(command)
			if !known || policy != rpc.TransportLoopbackOnly {
				t.Fatalf("%s: %s declares %s (registered %v), want loopback_only", name, command, policy, known)
			}
		}
	}
}

func TestEndpointDialDelayCommandsRefuseARemoteSocket(t *testing.T) {
	t.Parallel()
	table := rpc.NewCommandTable()
	rpc.RegisterEndpointDialDelayCommands(table, rpcmocks.NewMockEndpointDialDelayProvider(t))
	server, err := rpc.NewServer(config.RPC{Host: "127.0.0.1", Port: "0", Username: "u", Password: "p"}, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	for _, command := range endpointDialDelayCommands {
		req := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec",
			strings.NewReader(fmt.Sprintf(`{"command":%q,"args":{"endpoint":"192.0.2.1:64646"}}`, command)))
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

func TestEndpointDialDelayAnswersRoundTripThroughTheClient(t *testing.T) {
	at := time.Unix(1780000000, 0).UTC()
	endpoint := domain.PeerAddress("192.0.2.1:64646")
	delay := domain.EndpointDialDelay{Endpoint: endpoint, Reason: domain.EndpointDialDelayClaimedOldVersion, ClaimedVersion: 27, Steps: 3, NextAttemptAt: at}
	provider := rpcmocks.NewMockEndpointDialDelayProvider(t)
	provider.EXPECT().ListEndpointDialDelays(mock.Anything).Return([]domain.EndpointDialDelay{delay}, nil).Once()
	provider.EXPECT().RetryEndpointDial(mock.Anything, endpoint).Return(nil).Once()
	provider.EXPECT().RetryEndpointDial(mock.Anything, domain.PeerAddress("192.0.2.2:64646")).Return(domain.ErrEndpointDialDelayNotFound).Once()

	table := rpc.NewCommandTable()
	rpc.RegisterEndpointDialDelayCommands(table, provider)
	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
	startServer(t, cfg, table)
	client := rpc.NewClient(cfg)

	got, err := client.ListEndpointDialDelays()
	want := []rpc.EndpointDialDelayDTO{{Endpoint: endpoint, Reason: domain.EndpointDialDelayClaimedOldVersion, ClaimedVersion: 27, Steps: 3, NextAttemptAt: at}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("list decoded as %+v, %v; want %+v", got, err, want)
	}
	if err := client.RetryEndpointDial(string(endpoint)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if err := client.RetryEndpointDial("192.0.2.2:64646"); err == nil {
		t.Fatal("a retry of an endpoint without a delay succeeded")
	}
	if resp := table.Execute(rpc.CommandRequest{Name: "retryEndpointDial"}); resp.Error == nil || resp.ErrorKind != rpc.ErrValidation {
		t.Fatalf("a retry without an endpoint: %v, kind %v; want a validation error", resp.Error, resp.ErrorKind)
	}
}
