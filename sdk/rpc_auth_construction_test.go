package sdk

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestHTTPRPCConstructionRequiresCredentials(t *testing.T) {
	for _, tc := range []struct {
		name, username, password string
		enabled, refused         bool
	}{
		{name: "disabled-empty"},
		{name: "disabled-partial", username: "operator"},
		{name: "enabled-empty", enabled: true, refused: true},
		{name: "enabled-user-only", enabled: true, username: "operator", refused: true},
		{name: "enabled-password-only", enabled: true, password: "fixture-secret", refused: true},
		{name: "enabled-pair", enabled: true, username: "operator", password: "fixture-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testRuntimeConfig(t)
			cfg.RPC.Enabled, cfg.RPC.Username, cfg.RPC.Password = tc.enabled, tc.username, tc.password
			runtime, err := New(cfg)
			if tc.refused {
				if runtime != nil {
					_ = runtime.Close()
					t.Fatal("constructor returned a runtime for an unauthenticated HTTP listener")
				}
				if !errors.Is(err, ErrRPCAuthenticationRequired) {
					t.Fatalf("New error = %v, want ErrRPCAuthenticationRequired", err)
				}
				if tc.password != "" && strings.Contains(err.Error(), tc.password) {
					t.Fatal("constructor error exposed a credential")
				}
				if _, err := os.Stat(cfg.Node.StateDBPath); err != nil {
					t.Fatalf("expected to exercise failure after database construction: %v", err)
				}
				if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed constructor did not close its database: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := runtime.Close(); err != nil {
					t.Error(err)
				}
			})
			if (runtime.rpcServer != nil) != tc.enabled {
				t.Fatalf("HTTP server created = %v, requested = %v", runtime.rpcServer != nil, tc.enabled)
			}
			if _, err := runtime.Execute("help", nil); err != nil {
				t.Fatalf("in-process command dispatch unavailable: %v", err)
			}
		})
	}
}
