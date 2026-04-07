package sdk

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRuntimeExecuteHelp(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.AdvertiseAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = t.TempDir()
	cfg.Node.IdentityPath = filepath.Join(cfg.Node.ChatLogDir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(cfg.Node.ChatLogDir, "trust.json")
	cfg.Node.QueueStatePath = filepath.Join(cfg.Node.ChatLogDir, "queue.json")
	cfg.Node.PeersStatePath = filepath.Join(cfg.Node.ChatLogDir, "peers.json")

	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	raw, err := runtime.Execute("help", nil)
	if err != nil {
		t.Fatalf("Execute(help) error = %v", err)
	}

	var result struct {
		Commands []CommandInfo `json:"commands"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal help result: %v", err)
	}

	if len(result.Commands) == 0 {
		t.Fatal("expected help to return commands")
	}

	hasSendDM := false
	for _, command := range result.Commands {
		if command.Name == "send_dm" {
			hasSendDM = true
			break
		}
	}
	if !hasSendDM {
		t.Fatal("expected SDK runtime to expose send_dm")
	}
}
