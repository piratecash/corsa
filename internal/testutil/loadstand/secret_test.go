package loadstand

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialsCannotEscapeThroughPublicFormatting(t *testing.T) {
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{credentials, *credentials, struct{ Credentials *Credentials }{credentials}, ProcessOptions{Credentials: credentials}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%b", "%o", "%O", "%c", "%U", "%e", "%E", "%f", "%F", "%g", "%G", "%t", "%T", "%p", "%w"} {
			text := fmt.Sprintf(format, value)
			for _, pattern := range secretPatterns([][]byte{credentials.password.raw}) {
				if strings.Contains(text, string(pattern)) {
					t.Fatal("formatter leaked secret")
				}
			}
		}
	}
	if _, err := json.Marshal(credentials); !errors.Is(err, ErrSecretSerialization) {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal([]byte(`{}`), credentials); !errors.Is(err, ErrSecretSerialization) {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestPublicArtifactScannerDetectsEverySecretEncoding(t *testing.T) {
	secret := []byte("private material-\xff\xee\xdd\xcc-0123456789")
	path := filepath.Join(t.TempDir(), "public.json")
	forms := [][]byte{secret, []byte(hex.EncodeToString(secret)), []byte(strings.ToUpper(hex.EncodeToString(secret))), []byte(base64.StdEncoding.EncodeToString(secret)), []byte(base64.RawStdEncoding.EncodeToString(secret)), []byte(base64.URLEncoding.EncodeToString(secret)), []byte(base64.RawURLEncoding.EncodeToString(secret)), []byte(fmt.Sprintf("%d", secret))}
	for i, pattern := range forms {
		if err := os.WriteFile(path, append([]byte("prefix "), pattern...), 0600); err != nil {
			t.Fatal(err)
		}
		if err := ScanPublicArtifacts(t.Context(), [][]byte{secret}, []string{path}); !errors.Is(err, ErrSecretLeak) {
			t.Fatalf("encoding %d: %v", i, err)
		}
	}
	if err := os.WriteFile(path, []byte(`{"pid":123,"status":"ok"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ScanPublicArtifacts(t.Context(), [][]byte{secret}, []string{path}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := ScanPublicArtifacts(ctx, [][]byte{secret}, []string{path}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, (8<<20)+1), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ScanPublicArtifacts(t.Context(), [][]byte{secret}, []string{path}); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatal(err)
	}
}

func TestPublicArtifactScannerRejectsUnsupportedLocations(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "regular")
	if err := os.WriteFile(path, []byte("public data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ScanPublicArtifacts(t.Context(), nil, []string{path}); err != nil {
		t.Fatal(err)
	}
	for _, unsupported := range []string{directory, filepath.Join(directory, "missing")} {
		if err := ScanPublicArtifacts(t.Context(), nil, []string{unsupported}); err == nil {
			t.Fatal("unsupported or missing artifact treated as clean")
		}
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if err := ScanPublicArtifacts(t.Context(), nil, []string{link}); err == nil {
		t.Fatal("symlink treated as clean artifact")
	}
}

func TestArtifactOpenRejectsRegularInodeSubstitution(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "public")
	if err := os.WriteFile(path, []byte("expected artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, err := root.Lstat("public")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, filepath.Join(directory, "original")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("substitute"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := openVerifiedArtifact(t.Context(), root, "public", before)
	if file != nil {
		t.Error("substituted descriptor returned")
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}
	if !errors.Is(err, ErrUnsupportedArtifact) {
		t.Fatalf("regular-file identity substitution not rejected: %v", err)
	}
}
