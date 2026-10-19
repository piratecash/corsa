//go:build darwin

package loadstand

import (
	"context"
	"github.com/piratecash/corsa/internal/core/domain"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestFunctionalSandboxDeniesOwnNonloopbackService(t *testing.T) {
	// Both controls target our own listener. No external host or production seed
	// is contacted even if the sandbox rule regresses.
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var ownIP net.IP
	for _, address := range addresses {
		network, ok := address.(*net.IPNet)
		if ok && network.IP.To4() != nil && !network.IP.IsLoopback() {
			ownIP = network.IP
			break
		}
	}
	if ownIP == nil {
		t.Skip("no local nonloopback IPv4 interface for isolation control")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(ownIP.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	}()
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "/usr/bin/nc", "-v", "-z", "-w", "1", host, port).CombinedOutput(); err != nil {
		t.Fatalf("local positive control: %v: %s", err, output)
	}
	config := ChildConfig{DataDir: t.TempDir(), Listen: privateAddress(t), RPC: privateAddress(t), Bootstrap: []domain.PeerAddress{domain.PeerAddress(privateAddress(t))}, Procs: 1}
	profile, err := sandboxProfile(config)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, "/usr/bin/nc", "-v", "-z", "-w", "1", host, port).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Operation not permitted") {
		t.Fatalf("network deny must be actual sandbox refusal: %v: %s", err, output)
	}
	// Same allowed port, nonlocal documentation-range address: the kernel must
	// refuse before any network attempt; a timeout is not isolation evidence.
	_, plannedPort, err := net.SplitHostPort(string(config.Listen))
	if err != nil {
		t.Fatal(err)
	}
	output, err = exec.CommandContext(ctx, "/usr/bin/sandbox-exec", "-p", profile, "/usr/bin/nc", "-v", "-z", "-w", "1", "192.0.2.1", plannedPort).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Operation not permitted") {
		t.Fatalf("remote-host deny must be actual sandbox refusal: %v: %s", err, output)
	}

}
