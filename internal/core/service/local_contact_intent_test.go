package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
)

func TestK1DesktopManualLinkPersistsExplicitLocalFact(t *testing.T) {
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	remoteAddress := freeLoopbackAddress(t)
	cfg := config.Node{ListenAddress: freeLoopbackAddress(t), BootstrapPeers: []string{remoteAddress}, AllowPrivatePeers: true, TrustStorePath: filepath.Join(dir, "trust.json"), PeersStatePath: filepath.Join(dir, "peers.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json")}
	cfg = configtest.WithTempDataDirs(t, cfg)
	svc := node.NewService(cfg, owner, ebus.New())
	t.Cleanup(svc.WaitBackground)
	client := NewDesktopClient(config.App{Network: "gazeta-devnet"}, cfg, owner, svc, nil)
	link, err := contactlink.Build(peer, domain.NetworkID("gazeta-devnet"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ImportContactLink(context.Background(), link); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(cfg.TrustStorePath)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Contacts map[string]struct {
			Fact json.RawMessage `json:"local_contact_intent"`
		} `json:"contacts"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Contacts[peer.Address].Fact) == 0 {
		t.Fatal("actual manual contact link created no durable explicit local fact")
	}
}

func TestK1DesktopLinkThenActualNetworkProofConfirmsK(t *testing.T) {
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	remoteDir := t.TempDir()
	remoteAddress := freeLoopbackAddress(t)
	cfg := config.Node{ListenAddress: freeLoopbackAddress(t), BootstrapPeers: []string{remoteAddress}, AllowPrivatePeers: true, TrustStorePath: filepath.Join(dir, "trust.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json"), PeersStatePath: filepath.Join(dir, "peers.json")}
	remoteCfg := config.Node{ListenAddress: remoteAddress, AllowPrivatePeers: true, TrustStorePath: filepath.Join(remoteDir, "trust.json"), SecureSessionStorePath: filepath.Join(remoteDir, "pins.json"), PeersStatePath: filepath.Join(remoteDir, "peers.json")}
	cfg = configtest.WithTempDataDirs(t, cfg)
	remoteCfg = configtest.WithTempDataDirs(t, remoteCfg)
	local := node.NewService(cfg, owner, ebus.New())
	remote := node.NewService(remoteCfg, peer, ebus.New())
	client := NewDesktopClient(config.App{Network: "gazeta-devnet"}, cfg, owner, local, nil)
	link, err := contactlink.Build(peer, domain.NetworkID("gazeta-devnet"))
	if err != nil {
		t.Fatal(err)
	}
	initial, err := client.ImportContactLink(t.Context(), link)
	if err != nil || initial.Fact != domain.LocalContactFactDurableConfirmed || initial.K != domain.LocalContactKAwaitingFreshProof {
		t.Fatalf("Desktop manual result %+v %v", initial, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { done <- remote.Run(ctx) }()
	go func() { done <- local.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("node did not stop")
			}
		}
		local.WaitBackground()
		remote.WaitBackground()
	})
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("actual network proof did not confirm Desktop K")
		case <-ticker.C:
			result, err := client.ImportContactLink(t.Context(), link)
			if err != nil {
				t.Fatal(err)
			}
			if result.K == domain.LocalContactKConfirmed {
				return
			}
		}
	}
}
