package node_test

import (
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

func TestLoadStandProcessAdapterRealAuthenticatedRPC(t *testing.T) {
	svc := node.NewLoadStandProcessFixtureForTest(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.Repeat("a", 64)
	env := map[string]string{"LOADSTAND_DATA_DIR": filepath.Join(t.TempDir(), "child"), "LOADSTAND_LISTEN": "127.0.0.1:64650", "LOADSTAND_RPC": address, "LOADSTAND_BOOTSTRAP": "127.0.0.1:64650", "LOADSTAND_LISTENER": "true", "GOMAXPROCS": "1", "LOADSTAND_RPC_USER": "loadstand", "LOADSTAND_RPC_PASSWORD": secret}
	_, credentials, err := loadstand.ReadChildEnvironment(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, svc, nil, nil, nil)
	server, err := rpc.NewServer(config.RPC{Host: "127.0.0.1", Port: port, Username: "loadstand", Password: secret}, table)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.StartAsync(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	client, err := loadstand.NewRPCClient(loadstand.RPCOptions{Address: domain.ListenAddress(address), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	source, err := loadstand.NewBoundProcessSource(client, runjournal.SystemClock{}, loadstand.RuntimeBinding{Identity: domain.PeerIdentityFromWire(svc.NodeStatus().Identity), StartedAt: svc.NodeStatus().StartedAt})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.CheckLoadStandProcessSampleForTest(t.Context(), source); err != nil {
		t.Fatal(err)
	}
}
