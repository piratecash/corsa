package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
)

// network_override_test.go pins the invariant behind NewServiceWithNetwork:
// a Service whose Network is overridden owns no real connection.
//
// A NetCore that registerInboundConn or attachOutboundNetCore inserts into
// s.conns is closed through Service.Network(). With an override that Network
// is the caller's (a netcoretest.Backend), which has
// never heard of the registry's cores: the inbound teardown's Close was
// answered ErrUnknownConn and the entry was removed with its core still open,
// and the shutdown drain enumerated the override and missed every real core,
// leaving Run waiting on connWg for the read timeout. Rather than teach the
// override about cores it does not hold, the Service refuses to own any: the
// listener, the inbound registration and the outbound attach all refuse.

func newOverrideService(t *testing.T) *Service {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress:     "127.0.0.1:0",
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	})
	svc := NewServiceWithNetwork(cfg, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	return svc
}

func TestAServiceWithANetworkOverrideRefusesToRegisterAnInboundConn(t *testing.T) {
	t.Parallel()
	svc := newOverrideService(t)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	err := svc.registerInboundConn(server, nil)
	if err == nil {
		// Close what the registration built, so the failure does not leak a
		// writer the override can never reach.
		t.Cleanup(registeredInboundCore(t, svc, server).Close)
		t.Fatal("registerInboundConn accepted a real conn on a Service whose Network is overridden")
	}
	if !errors.Is(err, ErrNetworkOverrideOwnsNoConnections) {
		t.Fatalf("registerInboundConn = %v, want ErrNetworkOverrideOwnsNoConnections", err)
	}
	if _, registered := svc.connIDFor(server); registered {
		t.Fatal("the refused conn is in the registry")
	}
}

// TestHandleConnClosesTheConnItCannotRegisterUnderANetworkOverride drives the
// accepted-socket path end to end: the handler gets as far as registration,
// is refused, and closes the socket instead of serving it.
func TestHandleConnClosesTheConnItCannotRegisterUnderANetworkOverride(t *testing.T) {
	t.Parallel()
	svc := newOverrideService(t)
	svc.disableRateLimiting = true
	reachedRegistration := make(chan struct{})
	svc.inboundBeforeRegister = func() { close(reachedRegistration) }

	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		svc.handleConn(server, nil)
	}()

	// A non-TLS first byte is a v1 session, which needs no handshake before
	// registration.
	if _, err := client.Write([]byte("{")); err != nil {
		t.Fatalf("write the first byte: %v", err)
	}
	select {
	case <-reachedRegistration:
	case <-time.After(promptWaitBound):
		t.Fatal("fixture: the handler never reached registration, so a refusal proves nothing")
	}
	expectHangUp(t, client)
	select {
	case <-handlerDone:
	case <-time.After(promptWaitBound):
		t.Fatal("the handler is still running a conn it could not register")
	}
	svc.peerMu.RLock()
	n := svc.inboundConnCountLocked()
	svc.peerMu.RUnlock()
	if n != 0 {
		t.Fatalf("the registry holds %d inbound conns after the refusal", n)
	}
}

func TestRunRefusesToServeUnderANetworkOverride(t *testing.T) {
	t.Parallel()
	svc := newOverrideService(t)

	ctx, cancel := context.WithTimeout(context.Background(), promptWaitBound)
	defer cancel()
	err := svc.Run(ctx)
	if !errors.Is(err, ErrNetworkOverrideOwnsNoConnections) {
		t.Fatalf("Run = %v, want ErrNetworkOverrideOwnsNoConnections", err)
	}
	if ctx.Err() != nil {
		t.Fatal("Run served until its ctx ended instead of refusing at once")
	}
}

func TestAttachOutboundNetCoreRefusesUnderANetworkOverride(t *testing.T) {
	t.Parallel()
	svc := newOverrideService(t)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	svc.peerMu.Lock()
	connID := svc.nextConnIDLocked()
	svc.peerMu.Unlock()
	session := &peerSession{
		address: "127.0.0.1:64646",
		connID:  connID,
		conn:    local,
	}
	t.Cleanup(func() { _ = session.Close() })

	err := svc.attachOutboundNetCore(session)
	if !errors.Is(err, ErrNetworkOverrideOwnsNoConnections) {
		t.Fatalf("attachOutboundNetCore = %v, want ErrNetworkOverrideOwnsNoConnections", err)
	}
	if session.netCore != nil {
		t.Fatal("the refused session was given a NetCore")
	}
	if _, registered := svc.connIDFor(local); registered {
		t.Fatal("the refused outbound conn is in the registry")
	}
}
