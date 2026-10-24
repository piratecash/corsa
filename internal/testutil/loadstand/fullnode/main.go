//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/loadstand/fullwitness"
	"golang.org/x/sys/unix"
)

// A separate measured headless entry, stamped for BOTH baseline and candidate.
// It uses the normal core Service/metrics/headless RPC lifecycle; it never
// creates SDK/Desktop/chatlog/DMrouter or accepts user DMs as a finalrecipient.
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "loadstand-full: configuration or original lifecycle failed")
		os.Exit(1)
	}
}
func run(ctx context.Context) (result error) {
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return err
	}
	child, plan, credentials, err := loadstand.ReadPlannedChildEnvironment(os.Getenv)
	if err != nil || plan.Role != loadstand.MemberFull {
		return errors.Join(loadstand.ErrInvalidConfiguration, err)
	}
	if err = loadstand.ValidateAmbientEnvironment(os.Environ()); err != nil {
		return err
	}
	finalSlot, err := loadstand.ReadChildFinalSlot(os.Getenv)
	if err != nil {
		return err
	}
	if finalSlot == nil {
		return loadstand.ErrUnsupportedArtifact
	}
	defer func() { result = errors.Join(result, finalSlot.Close()) }()
	cfg := config.Default().Node
	cfg.ListenAddress = string(child.Listen)
	address, _ := netip.ParseAddrPort(string(child.Listen))
	advertise := domain.PeerPort(address.Port())
	cfg.AdvertisePort = &advertise
	cfg.Type = config.NodeTypeFull
	cfg.DisableDirectMessages = true
	cfg.ListenerEnabled = true
	cfg.ListenerSet = true
	cfg.AllowPrivatePeers = true
	cfg.IdentityPath = filepath.Join(child.DataDir, "identity.json")
	cfg.TrustStorePath = filepath.Join(child.DataDir, "trust.json")
	cfg.IdentityIntentsPath = filepath.Join(child.DataDir, "identity-intents.json")
	cfg.PeersStatePath = filepath.Join(child.DataDir, "peers.json")
	cfg.SecureSessionStorePath = filepath.Join(child.DataDir, "sessions.json")
	cfg.StateDBPath = filepath.Join(child.DataDir, "state.db")
	cfg.ChatLogDir = child.DataDir
	cfg.DownloadDir = filepath.Join(child.DataDir, "downloads")
	cfg.PprofAddr = ""
	cfg.ProxyAddress = ""
	cfg.ConnectOnly = ""
	cfg.BootstrapPeers = make([]string, len(child.Bootstrap))
	for i, peer := range child.Bootstrap {
		cfg.BootstrapPeers[i] = string(peer)
	}
	own, err := identity.LoadOrCreate(cfg.IdentityPath)
	if err != nil {
		return err
	}
	service, unstartedCleanup := node.NewServiceForRuntime(cfg, own, ebus.New())
	started := false
	defer func() {
		if !started {
			unstartedCleanup()
		}
		service.WaitBackground()
	}()
	if err = service.StartupRefusal(); err != nil {
		return err
	}
	collector := metrics.NewCollector(service)
	collector.Seed()
	metricCtx, cancelMetrics := context.WithCancel(ctx)
	metricDone := make(chan struct{})
	go func() { defer close(metricDone); collector.Run(metricCtx) }()
	defer func() { cancelMetrics(); <-metricDone }()
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, service, nil, nil, collector)
	// This exact identity was passed to the Service above. Keep relay-only
	// contact policy unchanged; the fixture-only local witness is public data.
	public := fullwitness.PublicIdentity{Identity: domain.PeerIdentityFromWire(own.Address), PublicKey: identity.PublicKeyBase64(own.PublicKey), BoxPublicKey: identity.BoxPublicKeyBase64(own.BoxPublicKey), BoxSignature: identity.SignBoxKeyBinding(own)}
	if err = fullwitness.RegisterPlannedFullPublicIdentity(table, public); err != nil {
		return err
	}
	rpcAddress, _ := netip.ParseAddrPort(string(child.RPC))
	rpcConfig := config.RPC{Host: rpcAddress.Addr().String(), Port: fmt.Sprint(rpcAddress.Port()), Username: os.Getenv("LOADSTAND_RPC_USER"), Password: os.Getenv("LOADSTAND_RPC_PASSWORD")}
	server, err := rpc.NewServer(rpcConfig, table, service)
	if err != nil {
		return err
	}
	if err = server.StartAsync(); err != nil {
		return errors.Join(err, server.Shutdown())
	}
	service.PrimeBootstrapPeers()
	// The signal is an ordered shutdown request: first stop/join RPC handlers,
	// then cancel the original Service Run and join its background continuations.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()
	runDone := make(chan error, 1)
	started = true
	go func() { runDone <- service.Run(runCtx) }()
	var runErr, rpcErr error
	select {
	case <-ctx.Done():
		rpcErr = server.Shutdown()
		cancelRun()
		runErr = <-runDone
	case runErr = <-runDone:
		rpcErr = server.Shutdown()
		cancelRun()
	}
	service.WaitBackground()
	cancelMetrics()
	<-metricDone
	if err = errors.Join(runErr, rpcErr); err != nil {
		return err
	}
	secrets := [][]byte{own.PrivateKey, own.PrivateKey.Seed(), own.BoxPrivateKey.Bytes()}
	cleanup := context.WithoutCancel(ctx)
	if err = errors.Join(loadstand.ScanPublicArtifacts(cleanup, secrets, []string{filepath.Join(child.DataDir, "child.log")}), credentials.ScanPublic(cleanup, []string{filepath.Join(child.DataDir, "child.log")})); err != nil {
		return err
	}
	return finalSlot.PublishHeadlessAndScan(cleanup, domain.PeerIdentityFromWire(own.Address), service.TransportTrafficStats(), secrets, credentials)
}
