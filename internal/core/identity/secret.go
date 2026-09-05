package identity

import (
	"errors"
	"fmt"
	"io"
)

// secret.go makes Identity fail-closed for every generic serialisation and
// formatting path. Identity OWNS both private keys; the exported fields make
// json.Marshal(identity) a one-line Base64 dump of the Ed25519 signing key,
// and fmt's %v walks the same fields. Neither call exists in the tree today —
// which is exactly the problem: nothing stops the next one from being written,
// and a leaked signing key is not a bug that can be rolled back.
//
// The block is deliberately loud rather than silent. Key material has exactly
// ONE named door out of this package — ExportBackup (backup.go) — and code
// that needs an identity on the wire builds the public projection it needs
// from PublicKeyBase64 / BoxPublicKeyBase64 / Address, which are public data
// by construction. Everything else gets an error it cannot ignore.

// ErrSecretSerialization is returned by every generic serialisation entry
// point on a secret-owning type. Distinguishable via errors.Is so callers and
// tests never match on message text (encoding/json wraps it in
// *json.MarshalerError, which unwraps).
var ErrSecretSerialization = errors.New("identity: refusing to serialise private key material")

// redacted is what every formatting verb sees instead of key material.
const redacted = "[redacted]"

// MarshalJSON refuses to render an Identity. The receiver is a VALUE on
// purpose: a value receiver puts the method in the method set of both
// Identity and *Identity, so json.Marshal(id) and json.Marshal(&id) both
// fail. A pointer receiver would leave the value form wide open — and the
// value form is the one a struct field embeds by default.
func (Identity) MarshalJSON() ([]byte, error) {
	return nil, ErrSecretSerialization
}

// UnmarshalJSON refuses to populate an Identity from JSON, for symmetry: the
// only supported inbound paths are decodeIdentity (the on-disk identity file)
// and ImportBackup (the versioned backup). Without this, json.Unmarshal would
// half-fill the struct — Ed25519 keys are []byte and decode from Base64, the
// X25519 key does not — leaving a value that looks restored and cannot sign.
func (*Identity) UnmarshalJSON([]byte) error {
	return ErrSecretSerialization
}

// String redacts the key material for %v, %s and %+v. The address is public
// (it IS the fingerprint of the signing key) and is the only field worth
// having in a log line.
func (id Identity) String() string {
	return fmt.Sprintf("identity.Identity{Address: %s, PrivateKey: %s, BoxPrivateKey: %s}",
		id.Address, redacted, redacted)
}

// GoString redacts the same material for %#v, which ignores Stringer and
// would otherwise print every field as a Go literal.
func (id Identity) GoString() string {
	return id.String()
}

// Format renders the identity for EVERY verb.
//
// String and GoString between them cover %v, %s, %+v and %#v — and nothing
// else. A numeric verb (%d, %x) falls through to fmt's reflective walk, which
// prints the private key's bytes without consulting either method; %x on a
// []byte field is a particularly convincing rendering of a key. Formatter is
// asked first for every verb, so there is no verb left over.
//
// Value receiver, like MarshalJSON: it covers Identity and *Identity, and the
// value form is what a struct field embeds by default.
func (id Identity) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, id.String())
}
