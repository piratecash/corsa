package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// secretFieldNameFragments are the substrings that mark a struct field as
// carrying a secret. The list is deliberately about NAMES, not types: the
// guard's job is to catch the field somebody adds next year, and the only
// thing that field is guaranteed to have is a name that says what it holds.
//
// Bare "key" is excluded on purpose — PubKey, BoxKey and their friends are
// public by construction and appear all over the wire types.
var secretFieldNameFragments = []string{
	"privatekey", "secretkey", "password", "passphrase", "secret",
	"token", "seed", "credential", "apikey",
}

// assertSecretFieldsAreUnmarshalable walks a struct type and fails for every
// secret-looking field that is not excluded from JSON with `json:"-"`.
// Anonymous nested structs are walked too; named types outside this package
// are not, because their own package owns that guard.
func assertSecretFieldsAreUnmarshalable(t *testing.T, root reflect.Type, pkgPath string) []string {
	t.Helper()
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

// TestConfigSecretFieldsAreExcludedFromJSON: every secret-looking field in
// the config tree carries `json:"-"`. This is the guard against the accident
// that has no other check — a new secret field added by someone who has not
// read this file.
func TestConfigSecretFieldsAreExcludedFromJSON(t *testing.T) {
	t.Parallel()
	offenders := assertSecretFieldsAreUnmarshalable(t, reflect.TypeOf(Config{}), reflect.TypeOf(Config{}).PkgPath())
	if len(offenders) != 0 {
		t.Fatalf("secret config fields without json:\"-\": %v", offenders)
	}
}

// TestSecretFieldGuardIsNotInert: the walker above must actually flag a
// tagless secret field. Without this, the guard passes forever the moment a
// refactor breaks its traversal, and nobody notices.
func TestSecretFieldGuardIsNotInert(t *testing.T) {
	t.Parallel()
	type inner struct {
		APIToken string // no json:"-" — must be reported
	}
	type fixture struct {
		Host     string
		Password string `json:"-"`
		Nested   inner
	}
	offenders := assertSecretFieldsAreUnmarshalable(t, reflect.TypeOf(fixture{}), reflect.TypeOf(fixture{}).PkgPath())
	if len(offenders) != 1 || !strings.HasSuffix(offenders[0], ".Nested.APIToken") {
		t.Fatalf("guard reported %v, want exactly the tagless nested secret", offenders)
	}
}

// Compile-time proof that the redaction covers every verb rather than the
// four Stringer and GoStringer happen to serve.
var (
	_ fmt.Formatter  = RPC{}
	_ fmt.Stringer   = RPC{}
	_ fmt.GoStringer = RPC{}
)

// TestRPCPasswordNeverRendered: the password survives neither marshalling
// nor any formatting verb. %v/%+v/%s go through Stringer, %#v through
// GoStringer, and the numeric verbs through neither — each is its own leak,
// so each is asserted.
func TestRPCPasswordNeverRendered(t *testing.T) {
	t.Parallel()
	const password = "s3cr3t-rpc-password"
	cfg := RPC{Host: "127.0.0.1", Port: "46464", Username: "corsa", Password: password}

	payload, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(payload), password) {
		t.Fatalf("json.Marshal leaked the RPC password: %s", payload)
	}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		for form, subject := range map[string]any{"value": cfg, "pointer": &cfg} {
			rendered := fmt.Sprintf(verb, subject)
			if strings.Contains(rendered, password) {
				t.Fatalf("fmt %s of the %s form leaked the RPC password: %s", verb, form, rendered)
			}
			if !strings.Contains(rendered, redactedSecret) {
				t.Fatalf("fmt %s of the %s form dropped the redaction marker: %s", verb, form, rendered)
			}
			// The non-secret fields must survive, or the redaction has
			// silently turned every diagnostic into an empty line.
			if !strings.Contains(rendered, "corsa") {
				t.Fatalf("fmt %s of the %s form dropped the username: %s", verb, form, rendered)
			}
		}
	}

	// An empty password must not be replaced by the marker: "[redacted]" in
	// a log where no credential exists sends the reader hunting for one.
	if rendered := fmt.Sprintf("%+v", RPC{Host: "127.0.0.1"}); strings.Contains(rendered, redactedSecret) {
		t.Fatalf("an unset password rendered as redacted: %s", rendered)
	}

	// Authentication must still read the real value.
	if !cfg.AuthEnabled() || cfg.Password != password {
		t.Fatal("redaction changed what the auth path reads")
	}
}
