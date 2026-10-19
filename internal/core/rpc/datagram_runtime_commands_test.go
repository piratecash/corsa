package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/stretchr/testify/mock"
)

func TestDatagramRuntimeProviderUnavailableAndErrorAreNeverOff(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterDatagramRuntimeStateCommand(table, nil)
	if response := table.Execute(rpc.CommandRequest{Name: "getDatagramRuntimeState"}); response.ErrorKind != rpc.ErrUnavailable {
		t.Fatal(response)
	}
	provider := rpcmocks.NewMockDatagramRuntimeStateProvider(t)
	provider.EXPECT().DatagramRuntimeState(mock.Anything).Return(domain.DatagramRuntimeState{}, errors.New("secret-path-do-not-render")).Once()
	rpc.RegisterDatagramRuntimeStateCommand(table, provider)
	response := table.Execute(rpc.CommandRequest{Name: "getDatagramRuntimeState"})
	if response.ErrorKind != rpc.ErrUnavailable || strings.Contains(response.Error.Error(), "secret-path") {
		t.Fatal("raw provider failure escaped", response)
	}
	var nilNode *node.Service
	rpc.RegisterDatagramRuntimeStateCommand(table, nilNode)
	if response := table.Execute(rpc.CommandRequest{Name: "getDatagramRuntimeState"}); response.ErrorKind != rpc.ErrUnavailable {
		t.Fatal("typed nil became off", response)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if response := table.Execute(rpc.CommandRequest{Name: "getDatagramRuntimeState", Ctx: ctx}); response.Error == nil {
		t.Fatal("cancelled command executed")
	}
}
func TestDatagramRuntimeStateActualSocketAuthAndAliases(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenerSet: true, ListenerEnabled: false, TrustStorePath: filepath.Join(t.TempDir(), "trust.json")})
	service := node.NewService(cfg, id, nil)
	t.Cleanup(service.WaitBackground)
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, service, nil, nil, nil)
	for _, command := range []string{"getDatagramRuntimeState", "get_datagram_runtime_state"} {
		policy, ok := table.TransportPolicyFor(command)
		if !ok || policy != rpc.TransportLoopbackOnly {
			t.Fatal("alias policy lost", command)
		}
	}
	for _, auth := range []bool{true, false} {
		t.Run(map[bool]string{true: "authenticated", false: "no_listener_auth"}[auth], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			_, port, err := net.SplitHostPort(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			rpcConfig := config.RPC{Host: "127.0.0.1", Port: port}
			if auth {
				rpcConfig.Username = "state"
				rpcConfig.Password = "password"
			}
			server, err := rpc.NewServer(rpcConfig, table)
			if err != nil {
				t.Fatal(err)
			}
			if err := server.StartAsync(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.ShutdownWithTimeout(5 * time.Second); err != nil {
					t.Error(err)
				}
			})
			for _, command := range []string{"getDatagramRuntimeState", "get_datagram_runtime_state"} {
				req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+port+"/rpc/v1/exec", strings.NewReader(`{"command":"`+command+`"}`))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Forwarded-For", "203.0.113.1")
				if auth {
					req.SetBasicAuth("state", "password")
				}
				response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				raw, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if err := errors.Join(readErr, closeErr); err != nil {
					t.Fatal(err)
				}
				if !auth {
					if response.StatusCode != http.StatusForbidden {
						t.Fatalf("unauth listener served state: %d %s", response.StatusCode, raw)
					}
					continue
				}
				if response.StatusCode != http.StatusOK {
					t.Fatalf("protected actual socket refused: %d %s", response.StatusCode, raw)
				}
				var state domain.DatagramRuntimeState
				if err := json.Unmarshal(raw, &state); err != nil {
					t.Fatal(err)
				}
				if state.Identity.String() != id.Address || state.State != domain.DatagramStateConfiguredOff || state.ConfiguredEnabled {
					t.Fatal("wrong constructor facts", state)
				}
				if strings.Contains(string(raw), "trust.json") || strings.Contains(string(raw), "password") || strings.Contains(string(raw), `"ready"`) {
					t.Fatal("private/general/health data escaped", string(raw))
				}
			}
		})
	}
}
