package domain

import "time"

// OptionalTime is a value-typed optional time.Time for snapshot-visible
// domain state. It satisfies the rule that optional state must be visible
// from the type — callers always see Valid()/Time() rather than guessing
// from time.IsZero().
//
// Choosing a value type over *time.Time is deliberate: copy-by-value
// always yields an independent snapshot, so a consumer of NodeStatus()
// cannot accidentally mutate monitor-owned state by writing through a
// shared pointer. deepCopyNodeStatus and similar boundary copiers stay
// trivial — no per-field pointer cloning to remember when a new field
// is added.
//
// Aliasing is impossible by construction; any callers that still need a
// *time.Time at the ebus/wire boundary use Ptr(), which allocates a
// fresh copy every call.
type OptionalTime struct {
	t     time.Time
	valid bool
}

// TimeOf wraps t as a valid OptionalTime. Note: a zero t still produces
// a valid OptionalTime — callers that want "zero means absent" semantics
// must use TimeFromNonZero instead.
func TimeOf(t time.Time) OptionalTime {
	return OptionalTime{t: t, valid: true}
}

// TimeFromNonZero wraps t when t is non-zero, returns an invalid
// OptionalTime otherwise. Adapter for legacy inputs where zero-time
// conventionally meant "not set".
func TimeFromNonZero(t time.Time) OptionalTime {
	if t.IsZero() {
		return OptionalTime{}
	}
	return OptionalTime{t: t, valid: true}
}

// TimeFromPtr adapts a *time.Time at ebus/wire boundaries. nil produces
// an invalid OptionalTime; a non-nil pointer is dereferenced (a copy of
// the pointee is stored — the OptionalTime does not retain the pointer,
// so subsequent mutations through p cannot leak into the wrapper).
func TimeFromPtr(p *time.Time) OptionalTime {
	if p == nil {
		return OptionalTime{}
	}
	return OptionalTime{t: *p, valid: true}
}

// Valid reports whether the OptionalTime carries a time value.
func (o OptionalTime) Valid() bool {
	return o.valid
}

// Time returns the wrapped time. Undefined when Valid() is false —
// callers must check Valid() first. Returning time.Time (a value) rather
// than *time.Time preserves the no-aliasing property.
func (o OptionalTime) Time() time.Time {
	return o.t
}

// Ptr returns a fresh *time.Time allocation when Valid(), nil otherwise.
// Used to cross back to ebus/wire boundaries that still carry *time.Time.
// The returned pointer never aliases internal state — each call allocates.
func (o OptionalTime) Ptr() *time.Time {
	if !o.valid {
		return nil
	}
	t := o.t
	return &t
}

// Before reports whether o is Valid and wraps a time strictly before u.
// An invalid OptionalTime is never Before any time.
func (o OptionalTime) Before(u time.Time) bool {
	return o.valid && o.t.Before(u)
}

// After reports whether o is Valid and wraps a time strictly after u.
// An invalid OptionalTime is never After any time.
func (o OptionalTime) After(u time.Time) bool {
	return o.valid && o.t.After(u)
}

// Equal reports whether both sides are invalid or both wrap the same
// instant (time.Time.Equal semantics — monotonic readings stripped).
// Two invalid OptionalTimes compare equal regardless of the stored t.
func (o OptionalTime) Equal(other OptionalTime) bool {
	if o.valid != other.valid {
		return false
	}
	if !o.valid {
		return true
	}
	return o.t.Equal(other.t)
}

// Sub returns the duration o - u when o is Valid, zero otherwise.
// Callers that need to distinguish "absent" from "equal" must check
// Valid() before interpreting a zero result.
func (o OptionalTime) Sub(u time.Time) time.Duration {
	if !o.valid {
		return 0
	}
	return o.t.Sub(u)
}
