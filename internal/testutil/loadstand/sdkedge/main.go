package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/sdk"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "sdkedge: configuration, runtime or public-artifact validation failed")
		os.Exit(1)
	}
}
func run(ctx context.Context) (runErr error) {
	finalSlot, err := loadstand.ReadChildFinalSlot(os.Getenv)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, finalSlot.Close()) }()
	if err := disableCoreDumps(); err != nil {
		return err
	}
	config, credentials, err := loadstand.ReadSDKChildEnvironment(os.Getenv)
	if err != nil {
		return err
	}
	if err := loadstand.ValidateAmbientEnvironment(os.Environ()); err != nil {
		return err
	}
	cfg := sdk.DefaultConfig()
	cfg.Node.ListenAddress = string(config.Listen)
	_, port, err := net.SplitHostPort(string(config.Listen))
	if err != nil {
		return err
	}
	numeric, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return err
	}
	advertised := uint16(numeric)
	cfg.Node.AdvertisePort = &advertised
	cfg.Node.Type = sdk.NodeTypeClient
	cfg.Node.ListenerEnabled = &config.Listener
	cfg.Node.AllowPrivatePeers = true
	cfg.Node.IdentityPath = filepath.Join(config.DataDir, "identity.json")
	intentsPath := filepath.Join(config.DataDir, "identity-intents.json")
	cfg.Node.IdentityIntentsPath = &intentsPath
	cfg.Node.TrustStorePath = filepath.Join(config.DataDir, "trust.json")
	cfg.Node.PeersStatePath = filepath.Join(config.DataDir, "peers.json")
	cfg.Node.ChatLogDir = config.DataDir
	cfg.Node.StateDBPath = filepath.Join(config.DataDir, "state.db")
	cfg.Node.DownloadDir = filepath.Join(config.DataDir, "downloads")
	cfg.Node.BootstrapPeers = nil
	for _, peer := range config.Bootstrap {
		cfg.Node.BootstrapPeers = append(cfg.Node.BootstrapPeers, string(peer))
	}
	host, rpcPort, err := net.SplitHostPort(string(config.RPC))
	if err != nil {
		return err
	}
	cfg.RPC = sdk.RPCConfig{Enabled: true, Host: host, Port: rpcPort, Username: os.Getenv("LOADSTAND_RPC_USER"), Password: os.Getenv("LOADSTAND_RPC_PASSWORD")}
	if err := sdk.EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		return err
	}
	stack, err := sdk.NewWithContext(ctx, cfg)
	if err != nil {
		return err
	}
	receiptFile, err := os.OpenFile(filepath.Join(config.DataDir, "receipts.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Join(err, stack.Close())
	}
	observed := make(chan error, 1)
	messages := stack.SubscribeDirectMessages(ctx)
	go func() { observed <- observeMessages(messages, receiptFile) }()
	if err := stack.Start(ctx); err != nil {
		return errors.Join(err, stack.Close(), <-observed)
	}
	runtimeErr := errors.Join(stack.Wait(), <-observed)
	own, err := identity.Load(cfg.Node.IdentityPath)
	if err != nil {
		return errors.Join(runtimeErr, err)
	}
	secrets := [][]byte{own.PrivateKey, own.PrivateKey.Seed(), own.BoxPrivateKey.Bytes()}
	// Only this child sees its private material. The parent receives only the
	// scanner's exit outcome, never the keys or matching bytes.
	paths := []string{filepath.Join(config.DataDir, "child.log"), filepath.Join(config.DataDir, "receipts.jsonl")}
	scanErr := errors.Join(loadstand.ScanPublicArtifacts(context.WithoutCancel(ctx), secrets, paths), credentials.ScanPublic(context.WithoutCancel(ctx), paths))
	var finalErr error
	if finalSlot != nil && runtimeErr == nil {
		final, err := stack.FinalTransport()
		if err != nil || final.Boundary != sdk.TransportQuiescent {
			finalErr = errors.Join(err, loadstand.ErrFinalAccountingUnavailable)
		} else {
			finalErr = finalSlot.PublishAndScan(context.WithoutCancel(ctx), domain.PeerIdentityFromWire(own.Address), domain.TransportTrafficStats{StartedAt: final.StartedAt, ReadAt: final.ReadAt, BytesSent: final.BytesSent, BytesReceived: final.BytesReceived}, secrets, credentials)
		}
	}
	return errors.Join(runtimeErr, scanErr, finalErr)
}

func observeMessages(messages <-chan sdk.DirectMessage, file *os.File) error {
	encoder := json.NewEncoder(file)
	var writeErr error
	for message := range messages {
		if writeErr != nil {
			continue
		}
		sender, err := domain.ParsePeerIdentity(message.Sender)
		if err != nil {
			writeErr = err
			continue
		}
		recipient, err := domain.ParsePeerIdentity(message.Recipient)
		if err != nil {
			writeErr = err
			continue
		}
		digest := sha256.Sum256([]byte(message.Body))
		writeErr = encoder.Encode(loadstand.Receipt{ID: domain.MessageID(message.ID), Sender: sender, Recipient: recipient, BodySHA256: hex.EncodeToString(digest[:])})
	}
	return errors.Join(writeErr, file.Close())
}
