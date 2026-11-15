//go:build !windows

package node

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
)

func TestAppPinStoreRefusalPrecedesDebugListener(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   os.FileMode
		reason domain.PinStoreRefusalReason
	}{
		{"unsafe permissions", 0o666, domain.PinStoreRefusalLocationUnsafe},
		{"damaged store", 0o600, domain.PinStoreRefusalStoreUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatePendingApp(t)
			path := os.Getenv("CORSA_SECURE_SESSION_STORE_PATH")
			const body = "incomplete store"
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			// If Run reached pprof, the occupied address would return a bind
			// error before the node listener or RPC could start.
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			t.Setenv("CORSA_PPROF_ADDR", listener.Addr().String())
			app := New()
			t.Cleanup(app.service.WaitBackground)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var refusal *corenode.PinStoreRefusalError
			if err := app.Run(ctx); !errors.As(err, &refusal) || refusal.Reason != tc.reason || refusal.File != path {
				t.Fatalf("Run = %v, want %s refusal for %s before pprof", err, tc.reason, path)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != body {
				t.Fatalf("refused store changed: read error = %v", err)
			}
			if info, err := os.Stat(path); err != nil || info.Mode().Perm() != tc.mode {
				t.Fatalf("refused store mode changed: stat error = %v", err)
			}
		})
	}
}
