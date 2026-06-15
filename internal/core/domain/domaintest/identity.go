// Package domaintest holds deterministic constructors for domain value
// types, used by tests across packages. It lives outside the domain
// package so no test-only helper leaks into the production API surface.
package domaintest

import "github.com/piratecash/corsa/internal/core/domain"

// ID maps an arbitrary short label to a stable, valid PeerIdentity by
// copying the label bytes into the 20-byte fingerprint (zero-padded,
// truncated past 20 bytes). It exists so fixtures that previously used
// short string literals — domain.PeerIdentity("a") — can switch to
// domaintest.ID("a") mechanically.
//
// Byte order tracks lexical order for short distinct labels, so
// ordering-sensitive fixtures keep their expected sequence. The empty
// label yields the zero identity, matching the absent sentinel. For
// real 40-hex fingerprints use domain.ParsePeerIdentity instead.
func ID(label string) domain.PeerIdentity {
	var id domain.PeerIdentity
	copy(id[:], label)
	return id
}
