package node_test

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
)

func TestIdentityRestoreWithPriorRecord(t *testing.T) {
	t.Parallel()
	const networkName = "gazeta-devnet"
	dataDir := t.TempDir()
	cfg := config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(dataDir, "trust.json"),
		IdentityPath:      filepath.Join(dataDir, "identity.json"),
		ChatLogDir:        dataDir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}
	current, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc := node.NewService(cfg, current, nil)
	t.Cleanup(svc.WaitBackground)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	makeRecord := func(signer *identity.Identity, network domain.NetworkID, seq domain.IdentityRecordSeq) []byte {
		t.Helper()
		record, err := protocol.BuildSignedIdentityRecord(signer, protocol.IdentityRecordSpec{
			Network: network, IssuedAt: uint64(time.Now().Unix()), Seq: seq, DM: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	dir := cfg.EffectiveIdentityBackupDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	backup, err := identity.ExportBackup(owner, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "recovery.json"), backup, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := identity.Save(cfg.IdentityPath, current); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(cfg.IdentityPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "missing"},
		{name: "../outside"},
		{name: "wrong-signer", data: makeRecord(current, networkName, 12)},
		{name: "wrong-network", data: makeRecord(owner, "gazeta-othernet", 12)},
		{name: "exhausted", data: makeRecord(owner, networkName, math.MaxUint64)},
		{name: "oversize", data: bytes.Repeat([]byte(" "), domain.MaxIdentityRecordBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.data != nil {
				if err := os.WriteFile(filepath.Join(dir, tc.name), tc.data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			reply := svc.HandleLocalFrame(protocol.Frame{
				Type: "identity_restore", BackupName: "recovery.json", PriorRecordName: tc.name,
			})
			if reply.Type != "error" || reply.IdentityBackup != nil {
				t.Fatalf("restore with invalid evidence: %+v", reply)
			}
			after, err := os.ReadFile(cfg.IdentityPath)
			if err != nil || !bytes.Equal(original, after) {
				t.Fatalf("refused restore changed installed identity: %v", err)
			}
		})
	}
	if err := os.WriteFile(filepath.Join(dir, "prior.json"), makeRecord(owner, networkName, 12), 0o600); err != nil {
		t.Fatal(err)
	}
	table := rpc.NewCommandTable()
	rpc.RegisterNetworkCommands(table, svc)
	request, err := rpc.ParseConsoleInput("identityRestore recovery.json prior.json")
	if err != nil {
		t.Fatal(err)
	}
	response := table.Execute(request)
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	var reply protocol.Frame
	if err := json.Unmarshal(response.Data, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Type != "identity_restore" || reply.IdentityBackup == nil || reply.IdentityBackup.RecordSeq != 12 {
		t.Fatalf("restore with prior record: %+v", reply)
	}
	if reply.IdentityBackup.Warning != "" {
		t.Fatalf("restore with verified record warned of missing evidence: %q", reply.IdentityBackup.Warning)
	}
	saved, err := identity.Load(cfg.IdentityPath)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Address != owner.Address || saved.RecordSeqFloor != 12 || !bytes.Equal(saved.BoxPublicKey, owner.BoxPublicKey) {
		t.Fatal("restore changed the key material or lost the verified floor")
	}
	cfg.TrustStorePath = filepath.Join(t.TempDir(), "trust.json")
	restarted := node.NewService(cfg, saved, nil)
	t.Cleanup(restarted.WaitBackground)
	_, own := restarted.SelfIdentityRecord()
	if own.Seq != 13 {
		t.Fatalf("first published sequence after restore = %d, want 13", own.Seq)
	}
}
