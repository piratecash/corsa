package identity

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// secretEncodings renders one secret blob in every encoding a leak could
// plausibly take: the raw bytes, all four Base64 alphabets, hex, and the
// decimal byte list fmt produces for a []byte under %d.
//
// A scanner that checks a single encoding proves only that the one encoding it
// knows about is absent — and the decimal form is the one that made that
// concrete here. %d is precisely the verb that bypasses Stringer, so it is the
// verb a leak survives, and "[85 86 162 …]" is a perfectly usable rendering of
// a private key that none of the other five entries would have matched.
func secretEncodings(label string, secret []byte) map[string]string {
	return map[string]string{
		label + "/raw":            string(secret),
		label + "/base64-std":     base64.StdEncoding.EncodeToString(secret),
		label + "/base64-rawstd":  base64.RawStdEncoding.EncodeToString(secret),
		label + "/base64-url":     base64.URLEncoding.EncodeToString(secret),
		label + "/base64-raw-url": base64.RawURLEncoding.EncodeToString(secret),
		label + "/hex":            hex.EncodeToString(secret),
		label + "/decimal":        fmt.Sprintf("%d", secret),
	}
}

// identitySecrets is every blob that must never leave the process: the full
// Ed25519 private key, the seed it is derived from (the seed alone is enough
// to reconstruct the identity AND, via deriveBoxKeyPair, an SDK box key), and
// the X25519 box private key.
func identitySecrets(t *testing.T, id *Identity) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, blob := range map[string][]byte{
		"ed25519-private": id.PrivateKey,
		"ed25519-seed":    id.PrivateKey.Seed(),
		"x25519-box":      id.BoxPrivateKey.Bytes(),
	} {
		for encoding, value := range secretEncodings(name, blob) {
			out[encoding] = value
		}
	}
	return out
}

// assertNoSecret fails when any encoding of any identity secret appears in
// the rendered artifact.
func assertNoSecret(t *testing.T, what string, rendered string, secrets map[string]string) {
	t.Helper()
	for encoding, value := range secrets {
		if strings.Contains(rendered, value) {
			t.Fatalf("%s leaked the identity secret (%s)", what, encoding)
		}
	}
}

// TestIdentityMarshalJSONRefuses: the generic serialisation door is closed
// for BOTH the value and the pointer form, and closed with a sentinel that
// errors.Is can recognise through encoding/json's own wrapper.
func TestIdentityMarshalJSONRefuses(t *testing.T) {
	t.Parallel()
	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	cases := map[string]any{
		"pointer": id,
		"value":   *id,
		// The nested case is the realistic accident: nobody marshals a bare
		// Identity, they marshal a struct that happens to carry one.
		"nested pointer field": struct {
			ID *Identity `json:"id"`
		}{ID: id},
		"nested value field": struct {
			ID Identity `json:"id"`
		}{ID: *id},
	}

	for name, subject := range cases {
		payload, err := json.Marshal(subject)
		if err == nil {
			t.Fatalf("%s: json.Marshal succeeded and produced %q", name, payload)
		}
		if !errors.Is(err, ErrSecretSerialization) {
			t.Fatalf("%s: error = %v, want ErrSecretSerialization", name, err)
		}
	}
}

// TestIdentityUnmarshalJSONRefuses: the inbound half is closed too. Without
// it json.Unmarshal half-fills the struct — the Ed25519 fields are []byte and
// decode from Base64, the X25519 pointer does not — leaving a value that
// looks restored and cannot sign.
func TestIdentityUnmarshalJSONRefuses(t *testing.T) {
	t.Parallel()
	var restored Identity
	err := json.Unmarshal([]byte(`{"Address":"deadbeef"}`), &restored)
	if err == nil {
		t.Fatal("json.Unmarshal into an Identity succeeded")
	}
	if !errors.Is(err, ErrSecretSerialization) {
		t.Fatalf("error = %v, want ErrSecretSerialization", err)
	}
	if restored.Address != "" {
		t.Fatalf("a refused unmarshal still wrote Address = %q", restored.Address)
	}
}

// Compile-time proof that the redaction covers every verb rather than the
// four Stringer and GoStringer happen to serve.
var (
	_ fmt.Formatter  = Identity{}
	_ fmt.Formatter  = &Identity{}
	_ fmt.Stringer   = Identity{}
	_ fmt.GoStringer = Identity{}
)

// TestIdentityFormattingRedactsEveryVerb: %v, %+v, %s, %q and %x route
// through Stringer, %#v through GoStringer — and %d through NEITHER. That
// last one is not a theoretical gap: without Format it prints the private key
// as the decimal byte list "[85 86 162 …]", which is exactly as usable as the
// Base64 form and matches none of the encodings a scanner would normally look
// for. Each verb is a separate way to forget, so each is asserted, on both the
// value and the pointer form.
func TestIdentityFormattingRedactsEveryVerb(t *testing.T) {
	t.Parallel()
	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	secrets := identitySecrets(t, id)

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		for form, subject := range map[string]any{"pointer": id, "value": *id} {
			rendered := fmt.Sprintf(verb, subject)
			assertNoSecret(t, fmt.Sprintf("fmt %s of the %s form", verb, form), rendered, secrets)
			if !strings.Contains(rendered, id.Address) {
				t.Fatalf("fmt %s of the %s form dropped the address: %q", verb, form, rendered)
			}
		}
	}
}

// TestExportBackupIsTheOneDoor pins BOTH halves of the contract in one test:
// the named export still produces the key material a backup is useless
// without, and — the part that matters — the leak scanner above actually
// fires when the secret IS present. A scanner that has never gone red proves
// nothing about the artifacts it clears.
func TestExportBackupIsTheOneDoor(t *testing.T) {
	t.Parallel()
	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	payload, err := ExportBackup(id, 7)
	if err != nil {
		t.Fatalf("export backup: %v", err)
	}

	secrets := identitySecrets(t, id)
	found := []string{}
	for encoding, value := range secrets {
		if strings.Contains(string(payload), value) {
			found = append(found, encoding)
		}
	}
	// ExportBackup writes Base64-std of both private keys, so both the
	// padded and the unpadded rendering of each are expected to match.
	for _, want := range []string{"ed25519-private/base64-std", "x25519-box/base64-std"} {
		if !strings.Contains(string(payload), secrets[want]) {
			t.Fatalf("ExportBackup did not emit %s — the backup is unusable", want)
		}
	}
	if len(found) == 0 {
		t.Fatal("the leak scanner found nothing in a backup that is made of secrets: the scanner is inert")
	}
}
