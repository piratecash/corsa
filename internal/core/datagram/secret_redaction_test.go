package datagram

import (
	"crypto/ed25519"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Compile-time proof that both halves answer for every verb. Stringer alone
// leaves %d, which renders a []byte key as a decimal list.
// The VALUE assertions are the load-bearing ones: a method set that covers
// only the pointer leaves fmt.Sprintf("%+v", *builder) printing the key.
var (
	_ fmt.Formatter  = RoutedFrameBuilder{}
	_ fmt.Stringer   = RoutedFrameBuilder{}
	_ fmt.GoStringer = RoutedFrameBuilder{}
	_ fmt.Formatter  = &RoutedFrameBuilder{}
	_ fmt.Formatter  = RoutedFrameBuilderConfig{}
	_ fmt.Stringer   = RoutedFrameBuilderConfig{}
	_ fmt.GoStringer = RoutedFrameBuilderConfig{}
	_ fmt.Formatter  = &RoutedFrameBuilderConfig{}
)

// keyEncodings renders one key in every form a leak could take, including the
// decimal byte list %d produces — the encoding a Stringer-only redaction
// leaves behind, and the one none of the others match.
func keyEncodings(key []byte) map[string]string {
	return map[string]string{
		"raw":            string(key),
		"base64-std":     base64.StdEncoding.EncodeToString(key),
		"base64-rawstd":  base64.RawStdEncoding.EncodeToString(key),
		"base64-url":     base64.URLEncoding.EncodeToString(key),
		"base64-raw-url": base64.RawURLEncoding.EncodeToString(key),
		"base32":         base32.StdEncoding.EncodeToString(key),
		"hex":            hex.EncodeToString(key),
		"decimal":        fmt.Sprintf("%d", key),
	}
}

// TestFrameBuilderNeverRendersPrivateKey: the builder is the one place that
// holds the node's signing key OUTSIDE identity.Identity, so the fail-closed
// methods on that type protect nothing here. Both the config (exported field)
// and the builder (unexported value field) have to answer for themselves.
func TestFrameBuilderNeverRendersPrivateKey(t *testing.T) {
	t.Parallel()
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	localID := domain.PeerIdentityFromWire(hex.EncodeToString(public[:20]))
	cfg := RoutedFrameBuilderConfig{
		Network:    domain.NetworkID("gazeta-devnet"),
		LocalID:    localID,
		PrivateKey: private,
	}
	builder, err := NewRoutedFrameBuilder(cfg)
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}

	secrets := keyEncodings(private)
	// Both the pointer and the DEREFERENCED value: one character at the call
	// site used to decide whether the signing key reached the log.
	for label, subject := range map[string]any{
		"config":          cfg,
		"config pointer":  &cfg,
		"builder pointer": builder,
		"builder value":   *builder,
	} {
		for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
			rendered := fmt.Sprintf(verb, subject)
			for encoding, value := range secrets {
				if strings.Contains(rendered, value) {
					t.Fatalf("fmt %s of the %s leaked the signing key (%s)", verb, label, encoding)
				}
			}
			if !strings.Contains(rendered, "redacted") {
				t.Fatalf("fmt %s of the %s dropped the redaction marker: %s", verb, label, rendered)
			}
		}
	}

	// A struct merely HOLDING either of them is the realistic accident — and
	// a struct holding one BY VALUE is the form a pointer-only method set
	// misses entirely.
	holder := struct {
		Builder      *RoutedFrameBuilder
		BuilderValue RoutedFrameBuilder
		Config       RoutedFrameBuilderConfig
		Note         string
	}{Builder: builder, BuilderValue: *builder, Config: cfg, Note: "support bundle"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		rendered := fmt.Sprintf(verb, holder)
		for encoding, value := range secrets {
			if strings.Contains(rendered, value) {
				t.Fatalf("fmt %s of a struct holding the builder leaked the key (%s)", verb, encoding)
			}
		}
	}

	// The builder must still sign with the real key — the redaction changes
	// what is printed, not what is used.
	if !builder.public.Equal(public) {
		t.Fatal("the builder no longer carries the matching public key")
	}

	// A nil pointer must not panic the caller's log line. fmt catches the
	// nil-receiver panic and prints "<nil>", which is the right answer — the
	// point is only that it is not a crash and not a key.
	var missing *RoutedFrameBuilder
	rendered := fmt.Sprintf("%+v", missing)
	for encoding, value := range secrets {
		if strings.Contains(rendered, value) {
			t.Fatalf("a nil builder leaked the key (%s)", encoding)
		}
	}
	if strings.Contains(rendered, "PANIC") && !strings.Contains(rendered, "nil") {
		t.Fatalf("a nil builder panicked the formatter: %s", rendered)
	}
}
