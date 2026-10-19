//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/startupfence"
	"github.com/piratecash/corsa/internal/testutil/startupfencetest"
)

func TestOfflineRecoveryCLIChild(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "--offline-recovery-child" {
		return
	}
	// App.Run would try this invalid public profiling listener and fail. A
	// successful recovery therefore proves the CLI branch precedes App.Run.
	if err := os.Setenv("CORSA_PPROF_ADDR", "203.0.113.1:1"); err != nil {
		t.Fatal(err)
	}
	os.Exit(runNode())
}

func TestOfflineRecoveryCLICompletesBeforeAnyAppListener(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux supervisor and a separate child UID")
	}
	for _, mode := range []string{"complete-main", "missing-main"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			directory, err := os.MkdirTemp("", "pin-cli-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(directory) }()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			if err := identity.Save(filepath.Join(directory, "identity.json"), id); err != nil {
				t.Fatal(err)
			}
			node := domain.PeerIdentityFromWire(id.Address)
			target, other := domain.PeerIdentity{21}, domain.PeerIdentity{22}
			at := time.Now().UTC().Add(-time.Hour)
			original := struct {
				Format     int                               `json:"format"`
				Identities map[domain.PeerIdentity]time.Time `json:"identities"`
				Endpoints  map[string]struct {
					Identity domain.PeerIdentity `json:"identity"`
					LastV2   time.Time           `json:"last_v2"`
				} `json:"endpoints"`
			}{Format: 1, Identities: map[domain.PeerIdentity]time.Time{other: at}}
			original.Endpoints = map[string]struct {
				Identity domain.PeerIdentity `json:"identity"`
				LastV2   time.Time           `json:"last_v2"`
			}{"192.0.2.21:64646": {Identity: target, LastV2: at}}
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			mainPath := filepath.Join(directory, "pins.json")
			if mode == "complete-main" {
				if err := os.WriteFile(mainPath, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if err := os.Chown(filepath.Join(directory, entry.Name()), 65534, 65534); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chown(directory, 65534, 65534); err != nil {
				t.Fatal(err)
			}
			ledger, client, enrollment, closeBroker := startupfencetest.NewBroker(t, node)
			defer closeBroker()
			grant, err := ledger.PrepareStartup(ctx, node, "original-writer")
			if err != nil {
				t.Fatal(err)
			}
			lease, err := ledger.ConsumeStartup(ctx, grant)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ledger.BeginRemoval(ctx, lease, pinBindingBackup(t, startupfence.Binding{Node: node, Store: enrollment.Store, RuntimeEpoch: "original-epoch", Generation: lease.Generation, Identity: target, PinRevision: 1, PreviousPinnedAt: at}))
			if err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			environment := map[string]startupfence.EnvironmentValue{
				"CORSA_STARTUP_FENCE_CANDIDATE": "1", "CORSA_STARTUP_FENCE_LAUNCHER": startupfence.EnvironmentValue(exe), "CORSA_STARTUP_FENCE_STORE": startupfence.EnvironmentValue(enrollment.Store),
				"CORSA_DATA_DIR": startupfence.EnvironmentValue(directory), "CORSA_IDENTITY_PATH": startupfence.EnvironmentValue(filepath.Join(directory, "identity.json")), "CORSA_SECURE_SESSION_STORE_PATH": startupfence.EnvironmentValue(mainPath),
				"CORSA_PEERS_PATH": startupfence.EnvironmentValue(filepath.Join(directory, "peers.json")), "CORSA_TRUST_STORE_PATH": startupfence.EnvironmentValue(filepath.Join(directory, "trust.json")),
				"CORSA_IDENTITY_INTENTS_PATH": startupfence.EnvironmentValue(filepath.Join(directory, "intents.json")), "CORSA_LISTEN_ADDRESS": "203.0.113.1:64599",
				"CORSA_RPC_HOST": "203.0.113.1", "CORSA_RPC_PORT": "64598", "CORSA_RPC_USERNAME": "candidate-user", "CORSA_RPC_PASSWORD": "candidate-password",
				"CORSA_CONNECT_ONLY": "127.0.0.1:9",
			}
			controller, err := startupfence.NewController(client, node, enrollment.Store)
			if err != nil {
				t.Fatal(err)
			}
			command := startupfence.ChildCommand{Path: exe, Args: []string{"-test.run=^TestOfflineRecoveryCLIChild$", "--", "--offline-recovery-child"}, UID: 65534, GID: 65534, Environment: environment}
			if err := controller.StartRecovery(ctx, command); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = controller.Stop() }()
			childErr := controller.Wait()
			status, err := ledger.Status(ctx, node)
			if err != nil {
				t.Fatal(err)
			}
			if status.State != startupfence.StateFenced {
				t.Fatal("recovery CLI unfenced startup")
			}
			if mode == "missing-main" {
				if childErr == nil {
					t.Fatal("missing-main recovery exited successfully")
				}
				if !status.WriterActive || status.Receipt != nil {
					t.Fatal("missing-main recovery sealed a receipt")
				}
				if _, err := os.Stat(mainPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing-main recovery synthesized a store: %v", err)
				}
				return
			}
			if childErr != nil {
				t.Fatalf("offline CLI entered App.Run/listeners or failed recovery: %v", childErr)
			}
			if status.WriterActive || status.Receipt == nil || status.Receipt.Kind != startupfence.ReceiptProtectionRestored {
				t.Fatalf("offline CLI did not seal restored receipt: %+v", status)
			}
			saved, err := os.ReadFile(mainPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(saved, &original); err != nil {
				t.Fatal(err)
			}
			restored, found := original.Identities[target]
			if !found || !restored.Equal(at) || !original.Identities[other].Equal(at) || len(original.Endpoints) != 1 {
				t.Fatal("CLI restoration lost pin timestamps, sibling pin or endpoint")
			}
			if bytes.Contains(saved, []byte("candidate-password")) {
				t.Fatal("CLI wrote RPC credentials into pin store")
			}
			if _, err := ledger.PrepareStartup(ctx, node, "premature-restart"); !errors.Is(err, startupfence.ErrFenced) {
				t.Fatalf("restored receipt admitted restart without finalize: %v", err)
			}
		})
	}
}
