package contactlink

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const testNetwork = domain.NetworkID("gazeta-devnet")

func newLinkOwner(t *testing.T) *identity.Identity {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return owner
}

// TestContactLinkRoundtrip: Build → Parse yields the verified triple in the
// internal encodings, and the size stays inside the QR-friendly budget.
func TestContactLinkRoundtrip(t *testing.T) {
	owner := newLinkOwner(t)
	link, err := Build(owner, testNetwork)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(link) > 300 {
		t.Errorf("link is %d bytes — the ~230–260 B QR budget drifted", len(link))
	}
	if !IsContactLink(link) {
		t.Error("built link fails its own pre-check")
	}

	contact, err := Parse(link, testNetwork)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if contact.Address.String() != owner.Address {
		t.Errorf("address = %s, want %s", contact.Address, owner.Address)
	}
	if string(contact.PubKey) != identity.PublicKeyBase64(owner.PublicKey) {
		t.Error("pk did not roundtrip into the internal encoding")
	}
	if string(contact.BoxKey) != identity.BoxPublicKeyBase64(owner.BoxPublicKey) {
		t.Error("bk did not roundtrip into the internal encoding")
	}
	if string(contact.BoxSig) != identity.SignBoxKeyBinding(owner) {
		t.Error("bs did not roundtrip into the internal encoding")
	}
}

// TestContactLinkRejects covers every §4.8 reject class.
func TestContactLinkRejects(t *testing.T) {
	owner := newLinkOwner(t)
	stranger := newLinkOwner(t)
	link, err := Build(owner, testNetwork)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	t.Run("broken signature", func(t *testing.T) {
		forged := swapParam(t, link, "bs", base64.RawURLEncoding.EncodeToString(make([]byte, 64)))
		if _, err := Parse(forged, testNetwork); !errors.Is(err, ErrLinkInvalidKeys) {
			t.Errorf("err = %v, want ErrLinkInvalidKeys", err)
		}
	})

	t.Run("foreign signature", func(t *testing.T) {
		// The stranger's own valid bs under the owner's address: binding
		// verification must refuse the graft.
		strangerSig, _ := base64.RawURLEncoding.DecodeString(identity.SignBoxKeyBinding(stranger))
		forged := swapParam(t, link, "bs", base64.RawURLEncoding.EncodeToString(strangerSig))
		if _, err := Parse(forged, testNetwork); !errors.Is(err, ErrLinkInvalidKeys) {
			t.Errorf("err = %v, want ErrLinkInvalidKeys", err)
		}
	})

	t.Run("foreign pubkey", func(t *testing.T) {
		strangerPK, _ := base64.StdEncoding.DecodeString(identity.PublicKeyBase64(stranger.PublicKey))
		forged := swapParam(t, link, "pk", base64.RawURLEncoding.EncodeToString(strangerPK))
		if _, err := Parse(forged, testNetwork); !errors.Is(err, ErrLinkInvalidKeys) {
			t.Errorf("err = %v, want ErrLinkInvalidKeys", err)
		}
	})

	t.Run("unknown version", func(t *testing.T) {
		bumped := strings.Replace(link, "?v=1&", "?v=2&", 1)
		if _, err := Parse(bumped, testNetwork); !errors.Is(err, ErrLinkVersionUnsupported) {
			t.Errorf("err = %v, want ErrLinkVersionUnsupported", err)
		}
	})

	t.Run("network mismatch", func(t *testing.T) {
		if _, err := Parse(link, "other-net"); !errors.Is(err, ErrLinkNetworkMismatch) {
			t.Errorf("err = %v, want ErrLinkNetworkMismatch", err)
		}
	})

	t.Run("duplicate parameter", func(t *testing.T) {
		doubled := link + "&v=1"
		if _, err := Parse(doubled, testNetwork); !errors.Is(err, ErrLinkMalformed) {
			t.Errorf("err = %v, want ErrLinkMalformed", err)
		}
	})

	t.Run("size cap before decoding", func(t *testing.T) {
		padded := link + "&x=" + strings.Repeat("a", MaxLinkBytes)
		if _, err := Parse(padded, testNetwork); !errors.Is(err, ErrLinkTooLarge) {
			t.Errorf("err = %v, want ErrLinkTooLarge", err)
		}
	})

	t.Run("missing net", func(t *testing.T) {
		netless := strings.Replace(link, "&net=gazeta-devnet", "", 1)
		if _, err := Parse(netless, testNetwork); !errors.Is(err, ErrLinkMalformed) {
			t.Errorf("err = %v, want ErrLinkMalformed", err)
		}
	})
}

// TestContactLinkPercentEncoding: the raw query is split BEFORE decoding, so
// an encoded %26 inside a value never becomes a separator, and an encoded
// net still matches.
func TestContactLinkPercentEncoding(t *testing.T) {
	owner := newLinkOwner(t)
	link, err := Build(owner, "net&with=reserved")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if strings.Contains(link, "net&with") {
		t.Fatalf("reserved characters left unescaped: %s", link)
	}
	contact, err := Parse(link, "net&with=reserved")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if contact.Network != "net&with=reserved" {
		t.Errorf("net = %q", contact.Network)
	}

	// A raw %26 planted into a value must not split the pair.
	planted := swapParam(t, mustBuild(t, owner), "net", "a%26v%3D9")
	if _, err := Parse(planted, "a&v=9"); err != nil {
		t.Errorf("encoded separators broke the split: %v", err)
	}
}

// TestContactLinkUnknownParamsIgnored: additive growth.
func TestContactLinkUnknownParamsIgnored(t *testing.T) {
	owner := newLinkOwner(t)
	link := mustBuild(t, owner) + "&future=opaque"
	if _, err := Parse(link, testNetwork); err != nil {
		t.Fatalf("unknown parameter rejected: %v", err)
	}
}

func mustBuild(t *testing.T, owner *identity.Identity) string {
	t.Helper()
	link, err := Build(owner, testNetwork)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return link
}

// swapParam replaces one query parameter's raw value.
func swapParam(t *testing.T, link, name, value string) string {
	t.Helper()
	start := strings.Index(link, name+"=")
	if start < 0 {
		t.Fatalf("parameter %q not in link", name)
	}
	end := strings.IndexByte(link[start:], '&')
	if end < 0 {
		return link[:start] + name + "=" + value
	}
	return link[:start] + name + "=" + value + link[start+end:]
}
