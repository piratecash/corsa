package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// service_pin_store_refusal_test.go pins WHERE a refused pin store stops the
// node: NewService records the refusal, StartupRefusal reports it to the
// process that is about to start listeners of its own (RPC, pprof), and Run
// returns it before it opens the node's listener.

// newPinStoreService is a node whose identity file, marker and peers file sit
// in dir.
func newPinStoreService(t *testing.T, dir string) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	cfg := deriveTestAdvertisePort(config.Node{
		ListenAddress:     freeAddress(t),
		Type:              domain.NodeTypeFull,
		IdentityPath:      filepath.Join(dir, "identity-64646.json"),
		PeersStatePath:    filepath.Join(dir, "peers-64646.json"),
		ChatLogDir:        dir,
		AllowPrivatePeers: true,
	})
	return NewService(cfg, id, nil)
}

func TestARefusedPinStoreStopsRunBeforeTheListener(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A marker that says a store was kept, and no store: the node must not
	// start on an empty one.
	marker := filepath.Join(dir, "identity-64646.json"+pinStoreMarkerSuffix)
	if err := os.WriteFile(marker, []byte(`{"format":1,"store":"/elsewhere/secure-sessions-64646.json"}`), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	svc := newPinStoreService(t, dir)
	if err := svc.StartupRefusal(); !errors.Is(err, domain.ErrPinStoreRefused) {
		t.Fatalf("StartupRefusal = %v, want ErrPinStoreRefused", err)
	}
	// The node's address is taken: a Run that reached its listener would
	// fail with a bind error instead of the refusal.
	listenForeign(t, svc.cfg.ListenAddress)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := svc.Run(ctx)
	if !errors.Is(err, domain.ErrPinStoreRefused) {
		t.Fatalf("Run = %v, want ErrPinStoreRefused before any listener", err)
	}
	if fileExists(t, filepath.Join(dir, "secure-sessions-64646.json")) {
		t.Fatal("a refused node created an empty store")
	}
}

func TestAServiceWithAnIntactPinStoreHasNoStartupRefusal(t *testing.T) {
	t.Parallel()
	svc := newPinStoreService(t, t.TempDir())
	if err := svc.StartupRefusal(); err != nil {
		t.Fatalf("StartupRefusal = %v, want nil on a first start", err)
	}
}
