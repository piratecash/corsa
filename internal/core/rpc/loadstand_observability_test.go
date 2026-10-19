package rpc_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
)

func TestLoadStandObservabilityDeclaresLoopbackPolicy(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		table := rpc.NewCommandTable()
		if supplied {
			rpc.RegisterAllCommands(table, &nodeWithConnDiag{newDefaultNodeProvider(t), newMockConnDiagProvider(t)}, nil, nil, nil)
		} else {
			rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)
		}
		for _, name := range []string{"listBanned", "getRuntimeMetrics"} {
			policy, known := table.TransportPolicyFor(name)
			if !known || policy != rpc.TransportLoopbackOnly {
				t.Errorf("%s supplied=%v: known=%v policy=%s", name, supplied, known, policy)
			}
		}
	}
}

func TestRuntimeMetricsUsesTheRealNodeAndExecEnvelope(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &node.Service{}, nil, nil, nil)
	response := table.Execute(rpc.CommandRequest{Name: "getRuntimeMetrics"})
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	var snapshot domain.RuntimeMetrics
	if err := json.Unmarshal(response.Data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.Goroutines == nil || snapshot.GOMAXPROCS == nil || len(snapshot.Unsupported) != 0 {
		t.Fatalf("runtime command failed: %s", response.Data)
	}
}

func TestLoadStandObservabilityRefusesRemoteAndUnauthenticatedSockets(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &node.Service{}, nil, nil, nil)
	for _, credentials := range []bool{false, true} {
		cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
		if credentials {
			cfg.Username = "u"
			cfg.Password = "p"
		}
		server, err := rpc.NewServer(cfg, table)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"listBanned", "getRuntimeMetrics"} {
			req := httptest.NewRequest(http.MethodPost, "/rpc/v1/exec", strings.NewReader(fmt.Sprintf(`{"command":%q}`, name)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Forwarded-For", "127.0.0.1")
			if credentials {
				req.SetBasicAuth("u", "p")
			}
			response, err := server.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusForbidden {
				t.Errorf("%s auth=%v remote status=%d", name, credentials, response.StatusCode)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRuntimeMetricsAllowsAuthenticatedRealLoopbackHTTP(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &node.Service{}, nil, nil, nil)
	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
	startServer(t, cfg, table)
	raw, err := rpc.NewClient(cfg).ExecuteCommand("getRuntimeMetrics")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot domain.RuntimeMetrics
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.Goroutines == nil {
		t.Fatalf("loopback HTTP lost metrics: %s", raw)
	}
}
