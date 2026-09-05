package datagram

import (
	"fmt"
	"io"
)

// secret_redaction.go keeps the node's Ed25519 signing key out of every
// formatted rendering of the frame builder and its config.
//
// This is the one place in the tree that holds the private key OUTSIDE
// identity.Identity: the builder is given the raw ed25519.PrivateKey so the
// signing path does not depend on the identity type. That is a deliberate
// design (see RoutedFrameBuilderConfig.PrivateKey), and the cost of it is
// that Identity's own fail-closed methods protect nothing here — the bytes
// live in a plain field of a different struct.
//
// Both halves need answering, for different reasons:
//
//   - RoutedFrameBuilderConfig.PrivateKey is EXPORTED, so %+v on the config
//     prints it directly and json.Marshal would too.
//   - RoutedFrameBuilder.private is unexported and held BY VALUE, and fmt
//     walks an unexported field by reflection without calling any method on
//     it — so %+v on the builder prints the bytes just as plainly.
//
// Formatter rather than Stringer in both cases: %v, %s, %q and %x consult
// Stringer and %#v consults GoStringer, but %d consults neither and renders a
// []byte key as the decimal list "[85 86 162 …]" — every bit as usable as the
// Base64 form.

// Format renders the builder for EVERY verb, redacted.
//
// VALUE receiver, and that is the whole point of this line. A method declared
// on *RoutedFrameBuilder is in the pointer's method set only, so
// fmt.Sprintf("%+v", builder) is redacted while fmt.Sprintf("%+v", *builder)
// walks the copy by reflection and prints the key — a one-character
// difference at the call site deciding whether a private key reaches the log.
// A value receiver is in BOTH method sets, so both forms are covered. It is
// available here because the builder holds no locks; where a type does hold
// them (rpc.Server, sdk.Runtime) a value receiver is a copylocks violation and
// the secret has to be kept out of the struct instead.
func (b RoutedFrameBuilder) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, b.String())
}

// String is the single redacted rendering Format hands to every verb. The
// builder holds no mutable state, so this takes no lock and cannot deadlock a
// caller logging from inside one.
func (b RoutedFrameBuilder) String() string {
	return fmt.Sprintf("datagram.RoutedFrameBuilder{Network: %s, LocalID: %s, PrivateKey: redacted}",
		b.network, b.localID)
}

// GoString covers %#v for callers that reach it without going through Format.
func (b RoutedFrameBuilder) GoString() string {
	return b.String()
}

// Format renders the config for EVERY verb, redacted. Value receiver, so it
// covers the config both as a value and behind a pointer — and the value form
// is the one callers actually build.
func (c RoutedFrameBuilderConfig) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, c.String())
}

// String is the redacted rendering. The non-secret fields survive it: a
// redaction that empties the whole diagnostic just moves the next person to
// printing the fields one by one.
func (c RoutedFrameBuilderConfig) String() string {
	return fmt.Sprintf("datagram.RoutedFrameBuilderConfig{Network: %s, LocalID: %s, PrivateKey: redacted}",
		c.Network, c.LocalID)
}

// GoString covers %#v for callers that reach it without going through Format.
func (c RoutedFrameBuilderConfig) GoString() string {
	return c.String()
}
