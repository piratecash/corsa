// Package contactlink implements the corsa: contact link of
// docs/protocol/identity-lookup.md — the offline channel that closes the
// original discovery gap: a full address handed over outside the network
// (QR, another messenger, voice) carries no keys, a corsa: link carries the
// self-certifying triple, so the contact is importable with no network at
// all.
//
//	corsa:<address>?v=1&net=<network_id>&pk=<b64url ed25519>&bk=<b64url x25519>&bs=<b64url boxsig>
//
// The triple itself is network-neutral — unlike the identity record's
// signature, the box binding does not cover the network — so `net` is an
// explicit label, not a cryptographic binding, and it is mandatory: there
// is no legacy netless form.
package contactlink

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/piratecash/corsa/internal/core/deeplink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Scheme is the URI scheme, compared case-insensitively as URI schemes
// are. The contact link is one member of the corsa: family (see
// internal/core/deeplink), and the family owns the scheme: two spellings
// would be a URI the operating system routes to us and this parser
// rejects.
const Scheme = deeplink.Scheme

// MaxLinkBytes bounds the whole URI BEFORE any decoding: the parser must
// never do work proportional to attacker-sized input.
const MaxLinkBytes = 2048

// Failures are distinguishable sentinels so the UI can phrase each case.
var (
	// ErrLinkMalformed covers structural violations: wrong scheme, broken
	// query, duplicate parameters, missing mandatory ones, undecodable
	// values.
	ErrLinkMalformed = errors.New("contact link: malformed")

	// ErrLinkTooLarge marks a URI above the size cap.
	ErrLinkTooLarge = errors.New("contact link: exceeds size cap")

	// ErrLinkVersionUnsupported marks a v this build does not understand.
	// Interpreting an unknown version as v1 is forbidden.
	ErrLinkVersionUnsupported = errors.New("contact link: unsupported version")

	// ErrLinkNetworkMismatch marks a link minted for another protocol
	// network. The UI owes the user a plain explanation for this one.
	ErrLinkNetworkMismatch = errors.New("contact link: different network")

	// ErrLinkInvalidKeys marks a triple that does not verify: the address
	// is not the fingerprint of pk, or the box binding fails.
	ErrLinkInvalidKeys = errors.New("contact link: key material does not verify")
)

// Contact is a parsed and VERIFIED link: fingerprint and box binding have
// already been checked, so importing it is a pure store operation. Key
// material is re-encoded into the internal (std base64 / RawURL signature)
// forms every import chokepoint expects.
type Contact struct {
	PubKey  domain.PeerPublicKey
	BoxKey  domain.PeerBoxKey
	BoxSig  domain.PeerBoxSignature
	Network domain.NetworkID
	Address domain.PeerIdentity
}

// Build renders the owner's own link.
func Build(owner *identity.Identity, network domain.NetworkID) (string, error) {
	if owner == nil || len(owner.BoxPublicKey) == 0 {
		return "", fmt.Errorf("%w: identity has no box key", ErrLinkMalformed)
	}
	pubKey, err := base64.StdEncoding.DecodeString(identity.PublicKeyBase64(owner.PublicKey))
	if err != nil {
		return "", fmt.Errorf("encode link pk: %w", err)
	}
	boxSig, err := base64.RawURLEncoding.DecodeString(identity.SignBoxKeyBinding(owner))
	if err != nil {
		return "", fmt.Errorf("encode link bs: %w", err)
	}

	link := Scheme + ":" + owner.Address +
		"?v=1&net=" + escapeQueryComponent(network.String()) +
		"&pk=" + base64.RawURLEncoding.EncodeToString(pubKey) +
		"&bk=" + base64.RawURLEncoding.EncodeToString(owner.BoxPublicKey) +
		"&bs=" + base64.RawURLEncoding.EncodeToString(boxSig)
	if len(link) > MaxLinkBytes {
		return "", fmt.Errorf("%w: built link is %d bytes", ErrLinkTooLarge, len(link))
	}
	return link, nil
}

// IsContactLink reports whether raw looks like a corsa: link — the cheap
// pre-check UI paste handlers use before calling Parse.
func IsContactLink(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	return len(trimmed) > len(Scheme)+1 && strings.EqualFold(trimmed[:len(Scheme)+1], Scheme+":")
}

// Parse validates and verifies a link against the node's own network.
//
// The parse order is strict and load-bearing: the size cap first; the RAW
// query string is split on '&' and '=' BEFORE any percent-decoding (a
// decoded %26 would otherwise become a fresh separator), then names and
// values are decoded separately; canonical duplicate names are a reject;
// unknown parameters are ignored (the format grows additively).
func Parse(raw string, network domain.NetworkID) (Contact, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) > MaxLinkBytes {
		return Contact{}, fmt.Errorf("%w: %d bytes", ErrLinkTooLarge, len(raw))
	}
	if !IsContactLink(raw) {
		return Contact{}, fmt.Errorf("%w: not a %s: link", ErrLinkMalformed, Scheme)
	}
	rest := raw[len(Scheme)+1:]

	addressPart, queryPart, hasQuery := strings.Cut(rest, "?")
	if !hasQuery {
		return Contact{}, fmt.Errorf("%w: no query", ErrLinkMalformed)
	}
	address, err := domain.ParsePeerIdentity(addressPart)
	if err != nil || address.IsZero() {
		return Contact{}, fmt.Errorf("%w: address is not 40-hex", ErrLinkMalformed)
	}

	params, err := parseQuery(queryPart)
	if err != nil {
		return Contact{}, err
	}

	version, ok := params["v"]
	if !ok {
		return Contact{}, fmt.Errorf("%w: missing v", ErrLinkMalformed)
	}
	if version != "1" {
		return Contact{}, fmt.Errorf("%w: v=%s", ErrLinkVersionUnsupported, version)
	}
	net, ok := params["net"]
	if !ok || net == "" {
		return Contact{}, fmt.Errorf("%w: missing net", ErrLinkMalformed)
	}
	if net != network.String() {
		return Contact{}, fmt.Errorf("%w: link is for %q, this node runs %q", ErrLinkNetworkMismatch, net, network.String())
	}

	pubKey, err := requiredBinaryParam(params, "pk", 32)
	if err != nil {
		return Contact{}, err
	}
	boxKey, err := requiredBinaryParam(params, "bk", 32)
	if err != nil {
		return Contact{}, err
	}
	boxSig, err := requiredBinaryParam(params, "bs", 64)
	if err != nil {
		return Contact{}, err
	}

	// verify-then-import: the triple is checked here, once, so every caller
	// downstream deals only with a proven contact.
	pubKeyStd := base64.StdEncoding.EncodeToString(pubKey)
	boxKeyStd := base64.StdEncoding.EncodeToString(boxKey)
	boxSigURL := base64.RawURLEncoding.EncodeToString(boxSig)
	if err := identity.VerifyPublicKeyFingerprint(address.String(), pubKeyStd); err != nil {
		return Contact{}, fmt.Errorf("%w: %v", ErrLinkInvalidKeys, err)
	}
	if err := identity.VerifyBoxKeyBinding(address.String(), pubKeyStd, boxKeyStd, boxSigURL); err != nil {
		return Contact{}, fmt.Errorf("%w: %v", ErrLinkInvalidKeys, err)
	}

	return Contact{
		Address: address,
		Network: domain.NetworkID(net),
		PubKey:  domain.PeerPublicKey(pubKeyStd),
		BoxKey:  domain.PeerBoxKey(boxKeyStd),
		BoxSig:  domain.PeerBoxSignature(boxSigURL),
	}, nil
}

// parseQuery splits the RAW query and only then decodes each half.
func parseQuery(query string) (map[string]string, error) {
	params := map[string]string{}
	for _, pair := range strings.Split(query, "&") {
		if pair == "" {
			return nil, fmt.Errorf("%w: empty query pair", ErrLinkMalformed)
		}
		rawName, rawValue, _ := strings.Cut(pair, "=")
		name, err := url.PathUnescape(rawName)
		if err != nil {
			return nil, fmt.Errorf("%w: undecodable parameter name", ErrLinkMalformed)
		}
		value, err := url.PathUnescape(rawValue)
		if err != nil {
			return nil, fmt.Errorf("%w: undecodable value of %q", ErrLinkMalformed, name)
		}
		if _, duplicate := params[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate parameter %q", ErrLinkMalformed, name)
		}
		params[name] = value
	}
	return params, nil
}

func requiredBinaryParam(params map[string]string, name string, size int) ([]byte, error) {
	value, ok := params[name]
	if !ok {
		return nil, fmt.Errorf("%w: missing %s", ErrLinkMalformed, name)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not base64url", ErrLinkMalformed, name)
	}
	if len(decoded) != size {
		return nil, fmt.Errorf("%w: %s is %d bytes, want %d", ErrLinkMalformed, name, len(decoded), size)
	}
	return decoded, nil
}

// escapeQueryComponent percent-encodes everything outside the unreserved
// set. Deliberately NOT url.QueryEscape: its '+' means a space only to
// parsers that decode application/x-www-form-urlencoded, and this format
// decodes with PathUnescape, where '+' is a literal.
func escapeQueryComponent(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}
