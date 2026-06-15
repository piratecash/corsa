package domain

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"net/netip"
)

// peerIdentityHexLen is the canonical textual length of a PeerIdentity:
// 20 raw bytes rendered as lowercase hex.
const peerIdentityHexLen = 2 * len(PeerIdentity{})

// ParsePeerIdentity decodes the canonical 40-char lowercase hex
// fingerprint into a PeerIdentity. An empty string decodes to the zero
// value (the "absent" sentinel) so that wire/JSON producers that emit an
// empty string for a missing identity round-trip unchanged. Any other
// length, or non-hex input, is an error — callers must not silently
// accept a malformed fingerprint.
func ParsePeerIdentity(s string) (PeerIdentity, error) {
	var id PeerIdentity
	if s == "" {
		return id, nil
	}
	if len(s) != peerIdentityHexLen {
		return id, fmt.Errorf("peer identity must be %d hex chars, got %d", peerIdentityHexLen, len(s))
	}
	// Enforce lowercase hex to match the protocol address invariant
	// (identity.ValidateAddress: ^[0-9a-f]{40}$). encoding/hex itself
	// accepts uppercase, so an uppercase wire identity would otherwise be
	// silently canonicalised instead of rejected.
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return id, fmt.Errorf("peer identity must be lowercase hex, got %q", s)
		}
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return id, fmt.Errorf("decode peer identity hex: %w", err)
	}
	return id, nil
}

// PeerIdentityFromWire decodes a 40-char hex fingerprint that arrived on
// an already-authenticated / frame-validated wire field, where the
// pre-refactor code performed an unchecked string→PeerIdentity cast.
// It is best-effort: empty or malformed input yields the zero identity,
// which downstream map lookups treat as "absent / no match" — exactly
// the harmless outcome the old cast produced for a bogus value, without
// forcing error handling onto ~60 trusted-boundary call sites. Use
// ParsePeerIdentity instead at any boundary where a decode failure must
// be surfaced to the caller.
func PeerIdentityFromWire(s string) PeerIdentity {
	id, err := ParsePeerIdentity(s)
	if err != nil {
		return PeerIdentity{}
	}
	return id
}

// PeerIdentityFromBytes builds a PeerIdentity from exactly 20 raw
// fingerprint bytes. A slice of any other length is an error.
func PeerIdentityFromBytes(b []byte) (PeerIdentity, error) {
	var id PeerIdentity
	if len(b) != len(id) {
		return id, fmt.Errorf("peer identity must be %d bytes, got %d", len(id), len(b))
	}
	copy(id[:], b)
	return id, nil
}

// IsZero reports whether the identity is the zero value, i.e. absent.
// Replaces the pre-refactor `id == ""` empty-string sentinel check.
func (p PeerIdentity) IsZero() bool { return p == PeerIdentity{} }

// Compare orders identities by raw byte value (bytes.Compare semantics:
// -1, 0, +1). Because lowercase-hex encoding is order-preserving, this
// yields the same ordering as the pre-refactor string comparison — so
// any sort or digest that relied on `<` keeps its exact output.
func (p PeerIdentity) Compare(other PeerIdentity) int {
	return bytes.Compare(p[:], other[:])
}

// String returns the canonical 40-char lowercase hex form, or the empty
// string for the zero (absent) identity. Because fmt invokes String for
// %s/%v, existing log sites that formatted the old string value continue
// to print the same hex text unchanged.
func (p PeerIdentity) String() string {
	if p.IsZero() {
		return ""
	}
	return hex.EncodeToString(p[:])
}

// MarshalText renders the identity as lowercase hex (empty for the zero
// value), keeping the JSON/wire shape identical to the pre-refactor
// string form. encoding/json uses this for both values and map keys.
func (p PeerIdentity) MarshalText() ([]byte, error) {
	if p.IsZero() {
		return []byte{}, nil
	}
	out := make([]byte, hex.EncodedLen(len(p)))
	hex.Encode(out, p[:])
	return out, nil
}

// UnmarshalText decodes the canonical hex form; empty input yields the
// zero (absent) identity.
func (p *PeerIdentity) UnmarshalText(text []byte) error {
	parsed, err := ParsePeerIdentity(string(text))
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

// Value implements driver.Valuer so a PeerIdentity persists to SQL as
// its canonical hex text (empty string for the zero value) — the exact
// column shape the pre-refactor string type produced, so existing rows
// match without migration and identities can be passed directly as query
// parameters.
func (p PeerIdentity) Value() (driver.Value, error) {
	return p.String(), nil
}

// Scan implements sql.Scanner, decoding the hex text (or raw text bytes)
// stored in SQL back into the fixed-width identity. NULL yields the zero
// value.
func (p *PeerIdentity) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*p = PeerIdentity{}
		return nil
	case string:
		return p.UnmarshalText([]byte(v))
	case []byte:
		return p.UnmarshalText(v)
	default:
		return fmt.Errorf("cannot scan %T into PeerIdentity", src)
	}
}

// ParsePeerIP parses a bare IP (IPv4 or IPv6), collapsing IPv4-mapped
// IPv6 to the bare IPv4 form via Unmap. An empty string yields the zero
// (invalid) PeerIP, which is the canonical "no value" sentinel.
func ParsePeerIP(s string) (PeerIP, error) {
	if s == "" {
		return PeerIP(netip.Addr{}), nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return PeerIP(netip.Addr{}), fmt.Errorf("parse peer ip %q: %w", s, err)
	}
	return PeerIP(addr.Unmap()), nil
}

// PeerIPFromAddr wraps a netip.Addr, collapsing any IPv4-mapped IPv6 to
// the bare IPv4 form so equality and map-key behaviour match the
// canonical convergence-layer representation.
func PeerIPFromAddr(addr netip.Addr) PeerIP { return PeerIP(addr.Unmap()) }

// Addr returns the underlying netip.Addr for callers that need the
// netip API (comparison helpers, prefix checks, JoinHostPort).
func (p PeerIP) Addr() netip.Addr { return netip.Addr(p) }

// IsValid reports whether the PeerIP holds a real address. The zero
// value is invalid and is the "absent" sentinel — replaces the
// pre-refactor empty-string check.
func (p PeerIP) IsValid() bool { return netip.Addr(p).IsValid() }

// String returns the canonical textual IP, or the empty string for the
// zero (absent) value, preserving the pre-refactor wire shape.
func (p PeerIP) String() string {
	addr := netip.Addr(p)
	if !addr.IsValid() {
		return ""
	}
	return addr.String()
}

// MarshalText renders the IP as text (empty for the absent value),
// matching the pre-refactor string JSON/wire shape.
func (p PeerIP) MarshalText() ([]byte, error) {
	addr := netip.Addr(p)
	if !addr.IsValid() {
		return []byte{}, nil
	}
	return addr.MarshalText()
}

// UnmarshalText parses the textual IP; empty input yields the absent
// (invalid) value.
func (p *PeerIP) UnmarshalText(text []byte) error {
	parsed, err := ParsePeerIP(string(text))
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}
