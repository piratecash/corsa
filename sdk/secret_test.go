package sdk

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
)

// secretFieldNameFragments marks a struct field as carrying a secret by its
// NAME: the guard's job is to catch the field somebody adds later, and a
// name is the only thing that field is guaranteed to have. Bare "key" is
// excluded — PubKey and BoxKey are public by construction.
var secretFieldNameFragments = []string{
	"privatekey", "secretkey", "password", "passphrase", "secret",
	"token", "seed", "credential", "apikey",
}

// secretFieldsWithoutJSONExclusion walks a struct type and returns every
// secret-looking field that is not excluded from JSON with `json:"-"`.
func secretFieldsWithoutJSONExclusion(root reflect.Type, pkgPath string) []string {
	offenders := []string{}
	seen := map[reflect.Type]bool{}

	var walk func(typ reflect.Type, path string)
	walk = func(typ reflect.Type, path string) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return
		}
		seen[typ] = true
		for i := range typ.NumField() {
			field := typ.Field(i)
			fieldPath := path + "." + field.Name
			lowered := strings.ToLower(field.Name)
			for _, fragment := range secretFieldNameFragments {
				if !strings.Contains(lowered, fragment) {
					continue
				}
				if field.Tag.Get("json") != "-" {
					offenders = append(offenders, fieldPath)
				}
				break
			}
			if elem := field.Type; elem.PkgPath() == pkgPath || elem.PkgPath() == "" {
				walk(elem, fieldPath)
			}
		}
	}
	walk(root, root.Name())
	return offenders
}

// Compile-time proof that the redaction covers every verb rather than the
// four Stringer and GoStringer happen to serve. Runtime is here because it
// holds a config in an UNEXPORTED field, which fmt reaches no method through.
var (
	_ fmt.Formatter  = NodeConfig{}
	_ fmt.Formatter  = RPCConfig{}
	_ fmt.Formatter  = &Runtime{}
	_ fmt.Stringer   = &Runtime{}
	_ fmt.GoStringer = &Runtime{}
)

// TestSDKConfigSecretFieldsAreExcludedFromJSON guards the public SDK surface:
// an embedder marshalling its own Config — into a support bundle, a crash
// report, a settings file — must not thereby publish the signing key.
func TestSDKConfigSecretFieldsAreExcludedFromJSON(t *testing.T) {
	t.Parallel()
	root := reflect.TypeOf(Config{})
	offenders := secretFieldsWithoutJSONExclusion(root, root.PkgPath())
	if len(offenders) != 0 {
		t.Fatalf("secret SDK config fields without json:\"-\": %v", offenders)
	}
}

// TestSDKSecretFieldGuardIsNotInert: the walker must actually flag a tagless
// secret field, or it clears the real config for the wrong reason.
func TestSDKSecretFieldGuardIsNotInert(t *testing.T) {
	t.Parallel()
	type inner struct {
		SigningSeed string // no json:"-" — must be reported
	}
	type fixture struct {
		Host       string
		PrivateKey string `json:"-"`
		Nested     inner
	}
	root := reflect.TypeOf(fixture{})
	offenders := secretFieldsWithoutJSONExclusion(root, root.PkgPath())
	if len(offenders) != 1 || !strings.HasSuffix(offenders[0], ".Nested.SigningSeed") {
		t.Fatalf("guard reported %v, want exactly the tagless nested secret", offenders)
	}
}

// newSecretTestConfig builds a runnable config carrying both secrets, with
// every file under t.TempDir().
func newSecretTestConfig(t *testing.T, privateKey, rpcPassword string) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = t.TempDir()
	cfg.Node.IdentityPath = filepath.Join(cfg.Node.ChatLogDir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(cfg.Node.ChatLogDir, "trust.json")
	cfg.Node.PeersStatePath = filepath.Join(cfg.Node.ChatLogDir, "peers.json")
	cfg.Node.PrivateKey = privateKey
	cfg.RPC.Username = "corsa"
	cfg.RPC.Password = rpcPassword
	return cfg
}

// TestRuntimeNeverRendersSecrets is the hole a Stringer on NodeConfig cannot
// close. fmt walks an UNEXPORTED field (Runtime.cfg) by reflection and calls
// no method on the way down, so the redaction that protects a config printed
// directly did nothing for a config printed as part of its owner — and
// numeric verbs skip Stringer even on exported paths.
//
// Two independent layers are asserted: the runtime keeps no secret to print,
// and it renders itself through fmt.Formatter for every verb.
func TestRuntimeNeverRendersSecrets(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	const rpcPassword = "s3cr3t-rpc-password"
	privateKey := base64.StdEncoding.EncodeToString(id.PrivateKey)
	cfg := newSecretTestConfig(t, privateKey, rpcPassword)

	rt, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	secrets := map[string]string{"private key": privateKey, "rpc password": rpcPassword}

	// Layer one: the secret is not in the struct at all.
	stored := rt.Config()
	if stored.Node.PrivateKey != "" || stored.RPC.Password != "" {
		t.Fatalf("Config() still carries secrets: %+v", stored)
	}

	// Layer two: every verb renders through Format. %d and %x are the ones a
	// Stringer would have missed.
	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		rendered := fmt.Sprintf(verb, rt)
		for label, secret := range secrets {
			if strings.Contains(rendered, secret) {
				t.Fatalf("fmt %s of the runtime leaked the %s: %s", verb, label, rendered)
			}
		}
		if !strings.Contains(rendered, "sdk.Runtime{") {
			t.Fatalf("fmt %s of the runtime did not go through Format: %s", verb, rendered)
		}
	}

	// A struct that merely HOLDS the runtime is the realistic accident.
	holder := struct {
		Runtime *Runtime
		Note    string
	}{Runtime: rt, Note: "support bundle"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		rendered := fmt.Sprintf(verb, holder)
		for label, secret := range secrets {
			if strings.Contains(rendered, secret) {
				t.Fatalf("fmt %s of a struct holding the runtime leaked the %s: %s", verb, label, rendered)
			}
		}
	}

	// The identity still resolved from the private key — proof the secret was
	// consumed before it was dropped, not merely thrown away.
	if rt.Address() != id.Address {
		t.Fatalf("runtime address = %s, want %s", rt.Address(), id.Address)
	}
}

// TestRPCAuthSurvivesConfigWipe: clearing the stored copy must not disarm the
// RPC server, which reads the password from its OWN config built earlier in
// construction. Without this, "the runtime forgot the password" would look
// exactly like "the runtime is safe to print".
func TestRPCAuthSurvivesConfigWipe(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	const rpcPassword = "s3cr3t-rpc-password"
	cfg := newSecretTestConfig(t, base64.StdEncoding.EncodeToString(id.PrivateKey), rpcPassword)
	cfg.RPC.Enabled = true
	cfg.RPC.Port = "0"

	rt, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	if rt.rpcServer == nil {
		t.Fatal("no rpc server was created")
	}
	// Driven through the server's in-memory connection rather than a socket:
	// the question is whether the auth middleware still holds the password,
	// and that is answered without starting the node.
	if status, body := execOnRPCServer(t, rt, "corsa", "wrong-password"); status != http.StatusUnauthorized {
		t.Fatalf("wrong credentials returned %d (%s), want 401 — the wipe disarmed authentication", status, body)
	}
	if status, body := execOnRPCServer(t, rt, "corsa", rpcPassword); status != http.StatusOK {
		t.Fatalf("correct credentials returned %d (%s), want 200", status, body)
	}
}

// execOnRPCServer issues one authenticated request against the runtime's RPC
// server and returns the status and body.
func execOnRPCServer(t *testing.T, rt *Runtime, username, password string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/rpc/v1/exec", strings.NewReader(`{"command":"version"}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)
	resp, err := rt.rpcServer.Test(req)
	if err != nil {
		t.Fatalf("rpc request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// TestNodeConfigPrivateKeyNeverRendered: the Base64 signing key survives
// neither marshalling nor any formatting verb, on either the value or the
// pointer form — and the surrounding non-secret fields still render, so the
// redaction has not quietly emptied every diagnostic.
func TestNodeConfigPrivateKeyNeverRendered(t *testing.T) {
	t.Parallel()
	const privateKey = "Yk9HVVNfUFJJVkFURV9LRVlfRk9SX1RIRV9MRUFLX0dVQVJE"
	const password = "s3cr3t-rpc-password"
	cfg := DefaultConfig()
	cfg.Node.PrivateKey = privateKey
	cfg.RPC.Username = "corsa"
	cfg.RPC.Password = password

	payload, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for label, secret := range map[string]string{"private key": privateKey, "rpc password": password} {
		if strings.Contains(string(payload), secret) {
			t.Fatalf("json.Marshal leaked the %s: %s", label, payload)
		}
	}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		for form, subject := range map[string]any{"value": cfg.Node, "pointer": &cfg.Node} {
			rendered := fmt.Sprintf(verb, subject)
			if strings.Contains(rendered, privateKey) {
				t.Fatalf("fmt %s of the %s NodeConfig leaked the private key: %s", verb, form, rendered)
			}
			if !strings.Contains(rendered, redactedSecret) {
				t.Fatalf("fmt %s of the %s NodeConfig dropped the redaction marker: %s", verb, form, rendered)
			}
			if !strings.Contains(rendered, cfg.Node.ListenAddress) {
				t.Fatalf("fmt %s of the %s NodeConfig dropped the listen address: %s", verb, form, rendered)
			}
		}
		for form, subject := range map[string]any{"value": cfg.RPC, "pointer": &cfg.RPC} {
			rendered := fmt.Sprintf(verb, subject)
			if strings.Contains(rendered, password) {
				t.Fatalf("fmt %s of the %s RPCConfig leaked the password: %s", verb, form, rendered)
			}
			if !strings.Contains(rendered, "corsa") {
				t.Fatalf("fmt %s of the %s RPCConfig dropped the username: %s", verb, form, rendered)
			}
		}
	}

	// An unset secret must not render as redacted: a "[redacted]" marker
	// where no secret exists sends the reader hunting for one.
	if rendered := fmt.Sprintf("%+v", NodeConfig{ListenAddress: ":64646"}); strings.Contains(rendered, redactedSecret) {
		t.Fatalf("an unset private key rendered as redacted: %s", rendered)
	}

	// Identity resolution must still read the real value.
	if cfg.Node.PrivateKey != privateKey {
		t.Fatal("redaction changed what the identity-resolution path reads")
	}
}
