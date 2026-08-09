package datagram

import "reflect"

// nilseam.go holds the one rule every seam of this layer that accepts a
// nil-able implementation is read through: what "nothing was supplied" means
// and how it is recognised.

// isNilValue reports whether an interface holds nothing usable — including the
// TYPED nil, `SomeIface((*impl)(nil))`, which is not `== nil`.
//
// The distinction is invisible at the call site and decisive at run time: the
// constructor's `x == nil` was false, the build succeeded, and the first method
// call on it either panicked or returned a zero result that no caller reads as
// "there is nothing here". A nil-able dynamic value is checked by what it
// POINTS AT, not by what the interface header is.
//
// It is ONE function for every seam that accepts a nil-able implementation — the
// route resolver, the peer metadata source, the type registry, a type's handler
// and authorizer, the emitter and the writer — because the hole was found twice:
// closing it per call site is how the second one survived the first fix.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Chan, reflect.Map,
		reflect.Func, reflect.Slice, reflect.Interface:
		return value.IsNil()
	default:
		return false
	}
}

// normaliseOptional turns a TYPED NIL into a plain nil, so "not supplied" has
// exactly one representation at an optional seam.
//
// # It applies to INERT optional seams only
//
// The rule used to be "required is refused, optional is normalised", which is
// one rule and was the wrong one: it turned a broken Authorizer from fail-CLOSED
// into fail-OPEN. Absent authorizer means "every frame of this type is
// authorized", so normalising a typed nil to absent silently switched a safety
// property off — where leaving it present had made guardHook convert its crash
// into Reject.
//
// So optional splits in two, by what ABSENT MEANS:
//
//   - INERT — absent is defined and costs nothing anyone relies on: a metrics
//     sink, the reverse-state limits, the class queue. These are
//     normalised, and normalising is what makes every `if x == nil` downstream
//     correct again;
//   - SAFETY — absent switches a check off: an Authorizer. A typed nil here is
//     refused at construction, because the alternative is the exact outcome the
//     seam exists to prevent and it is invisible from outside. Choosing to have
//     none stays lawful; it just has to be said by leaving the field empty.
//
// Admission and Crypto sit between: absent means "admit everything" and "an
// unlimited verification budget", which are real properties — but both are
// documented as optional on the config fields an operator reads, so a build
// that omits them has said so. They are normalised, and the class is recorded
// here rather than left to be re-derived.
func normaliseOptional[T any](value T) T {
	if isNilValue(value) {
		var absent T
		return absent
	}
	return value
}
