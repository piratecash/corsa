package domain

import "errors"

// ErrDialIntentWithoutIdentity is a dial "to X" built without an X: the zero
// identity names nobody, and a dial that names nobody is an address dial.
var ErrDialIntentWithoutIdentity = errors.New("dial intent: a dial to an identity needs a non-zero identity")

// ErrDialIntentUnset is a dial whose path stated no intent: the zero
// DialIntent. It is refused rather than taken for an address dial, so a path
// that forgot to decide does not get a meaning by default.
var ErrDialIntentUnset = errors.New("dial intent: no intent stated")

// DialIntent is whom a dial means to reach (docs/protocol/session_v2.md,
// "Dial intent"). An address dial means whoever answers on the address and
// accepts any identity proven there; a dial "to X" means X alone, and another
// identity proven on the address is not a session with X. The intent is a
// statement of the caller that knows X — sending to X, opening contact X —
// never something derived from the address: neither an endpoint binding, nor
// the address map, nor a pin says who stands on an address.
//
// The zero value is not a valid intent (Valid, ErrDialIntentUnset);
// DialToAddress and DialToIdentity are the only ways to make one, so a dial
// path states its intent explicitly.
type DialIntent struct {
	target PeerIdentity
	kind   dialIntentKind
}

type dialIntentKind int

const (
	dialIntentAddress dialIntentKind = iota + 1
	dialIntentIdentity
)

// DialToAddress is an address dial: it names nobody.
func DialToAddress() DialIntent {
	return DialIntent{kind: dialIntentAddress}
}

// DialToIdentity is a dial "to x". The zero identity is refused.
func DialToIdentity(x PeerIdentity) (DialIntent, error) {
	if x.IsZero() {
		return DialIntent{}, ErrDialIntentWithoutIdentity
	}
	return DialIntent{target: x, kind: dialIntentIdentity}, nil
}

// Valid reports whether i was made by DialToAddress or DialToIdentity.
func (i DialIntent) Valid() bool {
	return i.kind == dialIntentAddress || i.kind == dialIntentIdentity
}

// Target is the identity the dial means to reach; named is false for an
// address dial (and for the zero value, which names nobody either).
func (i DialIntent) Target() (target PeerIdentity, named bool) {
	if i.kind != dialIntentIdentity {
		return PeerIdentity{}, false
	}
	return i.target, true
}
