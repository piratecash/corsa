package desktop

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestWindowActualOpenUsesTypedLookupWithoutCloseCancellation(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	intents := filepath.Join(dir, "intents.json")
	cfg := config.Node{ListenAddress: "127.0.0.1:0", TrustStorePath: filepath.Join(dir, "trust.json"), PeersStatePath: filepath.Join(dir, "peers.json"), IdentityIntentsPath: intents}
	bus := ebus.New()
	svc := node.NewService(cfg, id, bus)
	t.Cleanup(svc.WaitBackground)
	client := service.NewDesktopClient(config.App{}, cfg, id, svc, nil)
	router := service.NewDMRouter(client, nil, bus, nil)
	window := &Window{router: router}
	target := domaintest.ID("actual-window-open")
	window.maybeResolveIdentityKeys(target)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		raw, readErr := os.ReadFile(intents)
		var file struct {
			Intents []struct {
				Target string `json:"target"`
				Reason struct {
					Type string `json:"reason_type"`
				} `json:"reason"`
			} `json:"intents"`
		}
		if readErr == nil && json.Unmarshal(raw, &file) == nil && len(file.Intents) == 1 {
			if file.Intents[0].Target != target.String() || file.Intents[0].Reason.Type != "ui_chat" {
				t.Fatal("Window used wrong ordinary intent")
			}
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("actual Window did not reach typed embedded-node lookup")
		case <-tick.C:
		}
	}
	close(window.uiStop())
	if !router.ShutdownDrain(time.Second) {
		t.Fatal("actual Window lookup did not release router operation lease")
	}
	raw, err := os.ReadFile(intents)
	if err != nil {
		t.Fatal(err)
	}
	var persisted struct {
		Intents []json.RawMessage `json:"intents"`
	}
	if err := json.Unmarshal(raw, &persisted); err != nil || len(persisted.Intents) != 1 {
		t.Fatal("Window close invented cancellation of durable seven-day intent")
	}
}
