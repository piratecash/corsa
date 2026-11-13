package desktop

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/deeplink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func TestDesktopRefusesPinStoreACLAndReleasesStartupResources(t *testing.T) {
	// Keep the Unix socket path within Darwin's sockaddr_un limit.
	dir, err := os.MkdirTemp("/tmp", "corsa-acl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	appdata.SetDir(dir)
	t.Cleanup(func() { appdata.SetDir("") })
	t.Setenv("TMPDIR", dir) // Keep the legacy overflow sweep in this fixture.
	t.Setenv("CORSA_LISTEN_ADDRESS", "127.0.0.1:64646")
	t.Setenv("CORSA_BOOTSTRAP_PEERS", "127.0.0.1:64647")
	t.Setenv("CORSA_RPC_USERNAME", "")
	t.Setenv("CORSA_RPC_PASSWORD", "")
	for key, name := range map[string]string{
		"CORSA_IDENTITY_PATH": "identity.json", "CORSA_IDENTITY_INTENTS_PATH": "intents.json",
		"CORSA_TRUST_STORE_PATH": "trust.json", "CORSA_PEERS_PATH": "peers.json",
		"CORSA_SECURE_SESSION_STORE_PATH": "pins.json", "CORSA_CHATLOG_DIR": "chat",
		"CORSA_STATE_DB_PATH": "state.db", "CORSA_DOWNLOAD_DIR": "downloads",
	} {
		t.Setenv(key, filepath.Join(dir, name))
	}
	const pins = `{"format":1,"identities":{"1111111111111111111111111111111111111111":"2026-01-01T00:00:00Z"},"endpoints":{}}`
	store := filepath.Join(dir, "pins.json")
	if err := os.WriteFile(store, []byte(pins), 0o600); err != nil {
		t.Fatal(err)
	}
	if raw, err := exec.Command("/bin/chmod", "+a", "everyone allow read", store).CombinedOutput(); err != nil {
		t.Fatalf("set ACL on fixture: %v (%s)", err, raw)
	}
	pprof, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pprof.Close() })
	t.Setenv("CORSA_PPROF_ADDR", pprof.Addr().String())
	var output bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(zerolog.SyncWriter(&output)).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = previous })

	err = Run()
	var refusal *node.PinStoreRefusalError
	var unsafe *secretfile.UnsafeLocationError
	if !errors.As(err, &refusal) || refusal.Reason != domain.PinStoreRefusalLocationUnsafe ||
		refusal.File != store || !errors.As(err, &unsafe) ||
		unsafe.Violation != secretfile.ViolationExtendedACL || unsafe.Component.Kind != secretfile.ComponentFile {
		t.Fatalf("Run = %v, want the pin-store ACL refusal before pprof", err)
	}
	if !strings.Contains(err.Error(), "remove the extended ACL grants") {
		t.Fatalf("ACL refusal has no ACL remediation: %v", err)
	}
	events := output.String()
	if !strings.Contains(events, `"message":"deep link socket listening"`) ||
		!strings.Contains(events, `"message":"storage state database closed"`) {
		t.Fatalf("startup did not acquire IPC and confirm DB close: %s", events)
	}
	socket := deeplink.SocketPath(dir, "64646")
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused launch left its IPC socket: %v", err)
	}
	raw, err := os.ReadFile(store)
	if err != nil || string(raw) != pins {
		t.Fatalf("refused launch changed pin store: %v", err)
	}
	if err := secretfile.AuditLocation(store); !errors.As(err, &unsafe) || unsafe.Violation != secretfile.ViolationExtendedACL {
		t.Fatalf("refused launch changed ACL: %v", err)
	}
	marker := filepath.Join(dir, "identity.json.pin-store-marker.json")
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused launch created a pin marker: %v", err)
	}
}
