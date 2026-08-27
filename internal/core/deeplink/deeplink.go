// Package deeplink is the entry door for corsa: URIs the operating system
// hands to the application — a link clicked in a browser, another
// messenger or a file manager, not text pasted into the app.
//
// The family has one scheme and several members, told apart by the URI
// target (everything between the scheme and the query):
//
//	corsa:<40-hex address>?…   → KindContact, the contact link of
//	                             docs/protocol/identity-lookup.md §9
//	corsa:<kind>/<payload>?…   → Kind(<kind>), the shape every later
//	                             member takes
//
// Classification is syntax only: it names the member and hands the
// UNMODIFIED URI to that member's own parser (contactlink.Parse for
// KindContact), which stays the single place that validates and verifies
// its format. Adding a member is a new Kind plus a handler in the
// Router's map — no change here and none at the call sites.
package deeplink

import (
	"errors"
	"fmt"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Scheme is the URI scheme of the whole family, compared
// case-insensitively as URI schemes are.
const Scheme = "corsa"

// MaxURIBytes bounds the whole URI BEFORE any decoding or dispatch: the
// classifier must never do work proportional to attacker-sized input,
// and the local socket (localsocket.go) reads no more than this.
const MaxURIBytes = 2048

// Failures are distinguishable sentinels so the UI can phrase each case.
var (
	// ErrMalformed covers structural violations: wrong scheme, empty
	// target, a kind name outside the permitted charset.
	ErrMalformed = errors.New("deep link: malformed")

	// ErrTooLarge marks a URI above the size cap.
	ErrTooLarge = errors.New("deep link: exceeds size cap")

	// ErrUnsupportedKind marks a well-formed member this build has no
	// handler for — a link minted by a newer version. The UI owes the
	// user a plain explanation for this one.
	ErrUnsupportedKind = errors.New("deep link: unsupported kind")
)

// Kind names one member of the family. It is the routing key, so it is
// always lowercase — URI targets are compared case-insensitively.
type Kind string

// KindContact is the contact link: the self-certifying triple that
// imports a peer with no network at all.
const KindContact Kind = "contact"

func (k Kind) String() string { return string(k) }

// Link is a classified URI: the member it belongs to and the original
// text, untouched, for that member's parser.
type Link struct {
	Kind Kind
	URI  string
}

// IsDeepLink reports whether raw looks like a corsa: URI — the cheap
// pre-check argv scanners and paste handlers use before Classify.
func IsDeepLink(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	return len(trimmed) > len(Scheme)+1 && strings.EqualFold(trimmed[:len(Scheme)+1], Scheme+":")
}

// Classify names the member a URI belongs to. It does not validate the
// member's own format — that is the member parser's job, and doing it
// here would put two authorities on one format.
func Classify(raw string) (Link, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) > MaxURIBytes {
		return Link{}, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(raw))
	}
	if !IsDeepLink(raw) {
		return Link{}, fmt.Errorf("%w: not a %s: URI", ErrMalformed, Scheme)
	}

	target, _, _ := strings.Cut(raw[len(Scheme)+1:], "?")
	if target == "" {
		return Link{}, fmt.Errorf("%w: empty target", ErrMalformed)
	}

	// The bare address form predates the family and stays the contact
	// link: a 40-hex fingerprint cannot collide with a kind name, which
	// always carries its payload behind a slash.
	if address, err := domain.ParsePeerIdentity(target); err == nil && !address.IsZero() {
		return Link{Kind: KindContact, URI: raw}, nil
	}

	// Every other member carries its payload behind a slash. The slash
	// is mandatory precisely so a TRUNCATED address ("corsa:ab12cd") is
	// rejected as malformed instead of quietly becoming a kind nobody
	// has ever defined.
	name, _, hasPayload := strings.Cut(target, "/")
	kind := Kind(strings.ToLower(name))
	if !hasPayload || !isKindName(kind) {
		return Link{}, fmt.Errorf("%w: %q is neither an address nor a kind", ErrMalformed, target)
	}
	return Link{Kind: kind, URI: raw}, nil
}

// isKindName pins the charset a kind name may use: lowercase ASCII
// letters, digits and '-'. Anything else is a malformed URI rather than
// an unsupported member, so a newer build's link and a broken one are
// told apart in the message the user sees.
func isKindName(kind Kind) bool {
	if kind == "" {
		return false
	}
	for _, r := range kind {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '-':
		default:
			return false
		}
	}
	return true
}
