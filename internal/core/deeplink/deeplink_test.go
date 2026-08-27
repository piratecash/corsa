package deeplink_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/deeplink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const testNetwork = domain.NetworkID("gazeta-testnet")

// newContactLink mints a link from a FRESH identity: every fixture here
// is generated, so no test ever carries a real address or real keys.
func newContactLink(t *testing.T) string {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	link, err := contactlink.Build(owner, testNetwork)
	if err != nil {
		t.Fatalf("build contact link: %v", err)
	}
	return link
}

// TestSchemeIsOneString pins the family scheme against the contact
// link's own constant: two spellings of "corsa" would mean a URI the
// router accepts and the contact parser rejects.
func TestSchemeIsOneString(t *testing.T) {
	if deeplink.Scheme != contactlink.Scheme {
		t.Fatalf("scheme drift: deeplink %q, contactlink %q", deeplink.Scheme, contactlink.Scheme)
	}
}

// TestClassifyContactLink: a real, built contact link classifies as the
// contact member and reaches its parser byte-identical.
func TestClassifyContactLink(t *testing.T) {
	raw := newContactLink(t)

	link, err := deeplink.Classify(raw)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if link.Kind != deeplink.KindContact {
		t.Errorf("kind = %q, want %q", link.Kind, deeplink.KindContact)
	}
	if link.URI != raw {
		t.Errorf("URI was rewritten:\n got %s\nwant %s", link.URI, raw)
	}
	if _, err := contactlink.Parse(link.URI, testNetwork); err != nil {
		t.Fatalf("classified link no longer parses as a contact: %v", err)
	}
}

// TestClassifyAcceptsUppercaseScheme: URI schemes are
// case-insensitive, and a link that survived a mail client's
// capitalisation still routes.
func TestClassifyAcceptsUppercaseScheme(t *testing.T) {
	raw := newContactLink(t)
	shouted := "CORSA:" + strings.TrimPrefix(raw, deeplink.Scheme+":")

	link, err := deeplink.Classify(shouted)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if link.Kind != deeplink.KindContact {
		t.Errorf("kind = %q, want %q", link.Kind, deeplink.KindContact)
	}
}

// TestClassifyFutureKinds: the <kind>/<payload> form is what every later
// member takes, and the classifier names it without knowing it.
func TestClassifyFutureKinds(t *testing.T) {
	cases := map[string]deeplink.Kind{
		"corsa:group/abc?v=1":   "group",
		"corsa:GROUP/abc?v=1":   "group",
		"corsa:call-invite/x":   "call-invite",
		"corsa:room2/abc":       "room2",
		"corsa:group/abc/extra": "group",
	}
	for raw, want := range cases {
		link, err := deeplink.Classify(raw)
		if err != nil {
			t.Errorf("classify(%q): %v", raw, err)
			continue
		}
		if link.Kind != want {
			t.Errorf("classify(%q) kind = %q, want %q", raw, link.Kind, want)
		}
	}
}

// TestClassifyRejects covers every structural reject class.
func TestClassifyRejects(t *testing.T) {
	long := "corsa:" + strings.Repeat("a", deeplink.MaxURIBytes)

	cases := map[string]struct {
		raw  string
		want error
	}{
		"other scheme":  {"https://example.org", deeplink.ErrMalformed},
		"scheme only":   {"corsa:", deeplink.ErrMalformed},
		"empty target":  {"corsa:?v=1", deeplink.ErrMalformed},
		"empty string":  {"", deeplink.ErrMalformed},
		"kind charset":  {"corsa:Grüße/abc", deeplink.ErrMalformed},
		"kind spaces":   {"corsa:my kind/abc", deeplink.ErrMalformed},
		"over the cap":  {long, deeplink.ErrTooLarge},
		"short address": {"corsa:abc?v=1", deeplink.ErrMalformed},
		"kind no slash": {"corsa:group", deeplink.ErrMalformed},
	}
	for name, tc := range cases {
		if _, err := deeplink.Classify(tc.raw); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
}

// TestClassifyRejectsShortHexAsAddress guards the discrimination rule
// itself: only a full 40-hex fingerprint is the bare contact form, and a
// hex-looking string of another length must not be silently routed to
// the contact parser.
func TestClassifyRejectsShortHexAsAddress(t *testing.T) {
	if link, err := deeplink.Classify("corsa:ab12cd?v=1"); err == nil {
		t.Fatalf("39-hex target classified as %q, want a reject", link.Kind)
	}
}

// TestIsDeepLink is the cheap pre-check every argv scanner and paste
// handler runs before Classify.
func TestIsDeepLink(t *testing.T) {
	cases := map[string]bool{
		newContactLink(t):     true,
		"corsa:group/abc":     true,
		"  corsa:group/abc  ": true,
		"CoRsA:group/abc":     true,
		"corsa:":              false,
		"corsandom:abc":       false,
		"https://corsa.chat":  false,
		"":                    false,
	}
	for raw, want := range cases {
		if got := deeplink.IsDeepLink(raw); got != want {
			t.Errorf("IsDeepLink(%q) = %v, want %v", raw, got, want)
		}
	}
}

// TestFromArgs: the URI is found by its scheme, wherever the desktop
// entry put it, and a command line without one is left alone.
func TestFromArgs(t *testing.T) {
	raw := newContactLink(t)

	got, ok := deeplink.FromArgs([]string{"--debug", raw, "ignored"})
	if !ok || got != raw {
		t.Errorf("FromArgs = (%q, %v), want (%q, true)", got, ok, raw)
	}
	if _, ok := deeplink.FromArgs([]string{"--debug", "file.txt"}); ok {
		t.Error("FromArgs found a link in a command line that has none")
	}
	if _, ok := deeplink.FromArgs(nil); ok {
		t.Error("FromArgs found a link in an empty command line")
	}
}
