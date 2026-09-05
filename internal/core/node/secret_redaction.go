package node

import (
	"fmt"
	"io"
)

// secret_redaction.go answers a question the fail-closed methods on
// identity.Identity cannot.
//
// Identity refuses to serialise itself and redacts every format verb — but
// only when fmt can REACH one of its methods, and fmt reaches none through an
// UNEXPORTED field: it walks such a field by reflection, calling nothing.
//
// This redaction is PRECAUTIONARY rather than the fix for a live leak, and the
// distinction is worth writing down so nobody weakens it by accident. Service
// holds the identity behind a POINTER, and fmt does not follow pointers below
// the top level — %+v prints the address, not the keys. Change that field to a
// value, or add a value-typed field that carries a secret, and the reflective
// walk reaches the bytes immediately; sdk.Runtime, which held its config by
// value, printed the whole private key under %d for exactly that reason.
//
// Formatter rather than Stringer, for the same reason it is Formatter
// everywhere else in this tree: String and GoString cover %v, %s, %+v and
// %#v and nothing more, while %d and %x fall through to the reflective walk —
// and %x over a []byte private key is a perfectly usable rendering of it.
// Formatter is consulted first for every verb, so no verb is left over.
//
// It also makes the output useful: %+v on this struct was several thousand
// lines of mutex-guarded fields, which is its own reason nobody would notice
// a key in there.

// Format renders the node service for EVERY verb, redacted.
//
// The line is deliberately short. A full dump of this struct is thousands of
// mutex-guarded fields nobody reads; the address, the role and the listener
// are what a log line about "which node is this" actually wants.
func (s *Service) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, s.String())
}

// String is the single redacted rendering Format hands to every verb.
//
// It touches only fields that are immutable after NewService, so it takes no
// domain mutex. That is a requirement, not an optimisation: a String that
// locked would deadlock the moment someone logged the service from inside a
// section already holding that mutex — and logging under a lock is exactly
// the situation where a diagnostic print gets added.
func (s *Service) String() string {
	if s == nil {
		return "node.Service(nil)"
	}
	address := "unset"
	if s.identity != nil {
		address = s.identity.Address
	}
	return fmt.Sprintf("node.Service{Address: %s, Type: %s, Listen: %s, Secrets: redacted}",
		address, s.cfg.Type, s.cfg.ListenAddress)
}

// GoString covers %#v for callers that reach it without going through Format
// (fmt consults Formatter first, so this is belt-and-braces).
func (s *Service) GoString() string {
	return s.String()
}
