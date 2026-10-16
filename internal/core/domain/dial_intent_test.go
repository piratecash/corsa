package domain

import (
	"errors"
	"testing"
)

// An address dial names nobody: it accepts whoever proves itself on the
// address.
func TestAnAddressDialNamesNoIdentity(t *testing.T) {
	t.Parallel()
	if target, named := DialToAddress().Target(); named || !target.IsZero() {
		t.Fatalf("an address dial names %s (named %v), want nobody", target, named)
	}
}

// A dial "to X" names X, and only a real identity can be named: the zero
// identity would turn the intent back into an address dial that no type
// could tell apart from one.
func TestADialToAnIdentityNamesIt(t *testing.T) {
	t.Parallel()
	x := PeerIdentity{0x42, 0x01}
	intent, err := DialToIdentity(x)
	if err != nil {
		t.Fatalf("DialToIdentity(%s): %v", x, err)
	}
	if target, named := intent.Target(); !named || target != x {
		t.Fatalf("the intent names %s (named %v), want %s", target, named, x)
	}

	if _, err := DialToIdentity(PeerIdentity{}); !errors.Is(err, ErrDialIntentWithoutIdentity) {
		t.Fatalf("DialToIdentity(zero) err = %v, want ErrDialIntentWithoutIdentity", err)
	}
}

// The zero value is not an intent: a dial path that forgot to state one is
// refused (ErrDialIntentUnset), not taken for an address dial.
func TestTheZeroDialIntentIsNotAnIntent(t *testing.T) {
	t.Parallel()
	if (DialIntent{}).Valid() {
		t.Fatal("the zero DialIntent is valid")
	}
	if !DialToAddress().Valid() {
		t.Fatal("an address dial is not valid")
	}
	intent, err := DialToIdentity(PeerIdentity{0x42})
	if err != nil || !intent.Valid() {
		t.Fatalf("a dial to an identity is not valid: %v", err)
	}
}
