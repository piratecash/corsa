package node

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func TestAppRefusesExtendedPinStoreACLBeforeDebugListener(t *testing.T) {
	for _, tc := range []struct {
		name      string
		acl       string
		component secretfile.LocationComponent
	}{
		{"store", "everyone allow read", secretfile.LocationComponent{Kind: secretfile.ComponentFile}},
		{"marker", "everyone allow read", secretfile.LocationComponent{Kind: secretfile.ComponentFile}},
		{"directory", "everyone allow delete_child", secretfile.LocationComponent{Kind: secretfile.ComponentDirectory, LevelsAbove: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatePendingApp(t)
			dir := filepath.Join(filepath.Dir(os.Getenv("CORSA_SECURE_SESSION_STORE_PATH")), "pin-data")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			store := filepath.Join(dir, "secure.json")
			t.Setenv("CORSA_SECURE_SESSION_STORE_PATH", store)
			const pinFile = `{"format":1,"identities":{"1111111111111111111111111111111111111111":"2026-01-01T00:00:00Z"},"endpoints":{}}`
			if err := os.WriteFile(store, []byte(pinFile), 0o600); err != nil {
				t.Fatal(err)
			}
			healthy := New()
			t.Cleanup(healthy.service.WaitBackground)
			if err := healthy.service.StartupRefusal(); err != nil {
				t.Fatalf("unmodified pin store refused: %v", err)
			}
			marker := os.Getenv("CORSA_IDENTITY_PATH") + ".pin-store-marker.json"
			read := func(path string) []byte {
				t.Helper()
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			storeBefore, markerBefore := read(store), read(marker)

			target, refusedFile := store, store
			switch tc.name {
			case "marker":
				target, refusedFile = marker, marker
			case "directory":
				target = dir
			}
			if raw, err := exec.Command("/bin/chmod", "+a", tc.acl, target).CombinedOutput(); err != nil {
				t.Fatalf("set fixture ACL: %v (%s)", err, raw)
			}
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

			err = app.Run(ctx)
			var refusal *corenode.PinStoreRefusalError
			var unsafe *secretfile.UnsafeLocationError
			if !errors.As(err, &refusal) || refusal.Reason != domain.PinStoreRefusalLocationUnsafe ||
				refusal.File != refusedFile || !errors.As(err, &unsafe) ||
				unsafe.Violation != secretfile.ViolationExtendedACL || unsafe.Component != tc.component {
				t.Fatalf("Run = %v, want extended ACL refusal for %s at %v before pprof", err, refusedFile, tc.component)
			}
			if !bytes.Equal(read(store), storeBefore) || !bytes.Equal(read(marker), markerBefore) {
				t.Fatal("refused startup changed pin store or marker bytes")
			}
			if err := secretfile.AuditLocation(refusedFile); !errors.As(err, &unsafe) || unsafe.Violation != secretfile.ViolationExtendedACL {
				t.Fatalf("refused startup changed the fixture ACL: %v", err)
			}
		})
	}
}
