package loadstand

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestChildEnvironmentIsSealedAndRequiresExplicitLoopback(t *testing.T) {
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	config := ChildConfig{DataDir: t.TempDir(), Listen: "127.0.0.1:20001", RPC: "127.0.0.1:20002", Bootstrap: []domain.PeerAddress{"127.0.0.1:20003"}, Listener: true, Procs: 1}
	environment, err := config.environment(credentials)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatal("malformed environment")
		}
		values[key] = value
	}
	if len(values) != 12 {
		t.Fatalf("unexpected environment allowlist size %d", len(values))
	}
	roundTrip, loaded, err := ReadChildEnvironment(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.DataDir != config.DataDir || loaded.passwordText() != credentials.passwordText() {
		t.Fatal("lost sealed child settings")
	}
	for _, bad := range []string{"65.108.204.190:64646", "localhost:64646", "0.0.0.0:1234", "127.0.0.1:0"} {
		copy := config
		copy.Bootstrap = []domain.PeerAddress{domain.PeerAddress(bad)}
		if !errors.Is(copy.ValidateLoopback(), ErrInvalidConfiguration) {
			t.Fatalf("unsafe bootstrap accepted: %s", bad)
		}
		copy = config
		copy.RPC = domain.ListenAddress(bad)
		if !errors.Is(copy.ValidateLoopback(), ErrInvalidConfiguration) {
			t.Fatalf("unsafe RPC accepted: %s", bad)
		}
	}
	if err := ValidateAmbientEnvironment([]string{"CORSA_VERSION=test", "PATH=/bin"}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ValidateAmbientEnvironment([]string{"CORSA_BOOTSTRAP_PEERS=secret"}), ErrInvalidConfiguration) {
		t.Fatal("ambient production override accepted")
	}
}
func TestProcessProfileRefusesPublicDirectoryBeforeLaunch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := preparePrivateProfile(dir); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("public profile accepted: %v", err)
	}
	private := filepath.Join(t.TempDir(), "private")
	if err := preparePrivateProfile(private); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(private)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0700 {
		t.Fatal("private directory mode lost")
	}
}
