package domain

import (
	"testing"
	"time"
)

// fixedTime keeps tests deterministic across runs and timezones.
func fixedTime() time.Time {
	return time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
}

func TestTimeOfIsValid(t *testing.T) {
	o := TimeOf(fixedTime())
	if !o.Valid() {
		t.Fatal("TimeOf must produce a valid OptionalTime")
	}
	if !o.Time().Equal(fixedTime()) {
		t.Fatalf("Time() = %v, want %v", o.Time(), fixedTime())
	}
}

func TestTimeOfZeroIsStillValid(t *testing.T) {
	// TimeOf deliberately does NOT treat zero as "absent" — callers who
	// want that semantic must use TimeFromNonZero.
	o := TimeOf(time.Time{})
	if !o.Valid() {
		t.Fatal("TimeOf(zero) must be valid — only TimeFromNonZero treats zero as absent")
	}
}

func TestTimeFromNonZero(t *testing.T) {
	cases := []struct {
		name  string
		input time.Time
		valid bool
	}{
		{"zero becomes invalid", time.Time{}, false},
		{"non-zero becomes valid", fixedTime(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := TimeFromNonZero(tc.input)
			if o.Valid() != tc.valid {
				t.Fatalf("Valid() = %v, want %v", o.Valid(), tc.valid)
			}
		})
	}
}

func TestTimeFromPtrNil(t *testing.T) {
	o := TimeFromPtr(nil)
	if o.Valid() {
		t.Fatal("TimeFromPtr(nil) must be invalid")
	}
}

func TestTimeFromPtrCopiesPointee(t *testing.T) {
	// The OptionalTime must not alias the source pointer — mutating
	// *source after wrapping must not affect the wrapper.
	source := fixedTime()
	o := TimeFromPtr(&source)

	// Mutate source; wrapper must still hold the original value.
	source = source.Add(time.Hour)

	if !o.Time().Equal(fixedTime()) {
		t.Fatalf("OptionalTime aliased source pointer — got %v, want %v",
			o.Time(), fixedTime())
	}
}

func TestPtrReturnsFreshAllocation(t *testing.T) {
	// Ptr() on the same OptionalTime must return distinct pointers per
	// call so downstream consumers cannot alias each other's copies.
	o := TimeOf(fixedTime())
	p1 := o.Ptr()
	p2 := o.Ptr()
	if p1 == nil || p2 == nil {
		t.Fatal("Ptr() on valid OptionalTime must not return nil")
	}
	if p1 == p2 {
		t.Fatal("Ptr() must allocate a fresh *time.Time per call, not share one")
	}
	// Mutating one returned pointer must not affect the other.
	*p1 = p1.Add(time.Hour)
	if p2.Equal(*p1) {
		t.Fatal("Ptr() returned aliased pointers — mutation leaked")
	}
}

func TestPtrNilWhenInvalid(t *testing.T) {
	var o OptionalTime
	if got := o.Ptr(); got != nil {
		t.Fatalf("Ptr() on invalid must be nil, got %v", got)
	}
}

func TestBeforeAfter(t *testing.T) {
	early := fixedTime()
	late := early.Add(time.Hour)

	eo := TimeOf(early)
	if !eo.Before(late) {
		t.Fatal("early.Before(late) must be true")
	}
	if eo.After(late) {
		t.Fatal("early.After(late) must be false")
	}

	var invalid OptionalTime
	if invalid.Before(late) {
		t.Fatal("invalid.Before is always false")
	}
	if invalid.After(early) {
		t.Fatal("invalid.After is always false")
	}
}

func TestEqual(t *testing.T) {
	a := TimeOf(fixedTime())
	b := TimeOf(fixedTime())
	c := TimeOf(fixedTime().Add(time.Second))
	var inv1, inv2 OptionalTime

	if !a.Equal(b) {
		t.Fatal("equal valid times must compare equal")
	}
	if a.Equal(c) {
		t.Fatal("different valid times must not compare equal")
	}
	if !inv1.Equal(inv2) {
		t.Fatal("two invalid OptionalTimes must compare equal")
	}
	if a.Equal(inv1) {
		t.Fatal("valid and invalid must not compare equal")
	}
}

func TestSub(t *testing.T) {
	early := fixedTime()
	late := early.Add(42 * time.Minute)

	lateO := TimeOf(late)
	if got := lateO.Sub(early); got != 42*time.Minute {
		t.Fatalf("Sub() = %v, want %v", got, 42*time.Minute)
	}

	var invalid OptionalTime
	if got := invalid.Sub(early); got != 0 {
		t.Fatalf("invalid.Sub() must be 0, got %v", got)
	}
}

func TestCopyIsIndependent(t *testing.T) {
	// The core property: copying an OptionalTime yields an independent
	// snapshot. The copy path in NodeStatus relies on this — replacing
	// *time.Time with OptionalTime removes the need for per-field
	// pointer cloning in deepCopyNodeStatus.
	original := TimeOf(fixedTime())
	copy := original

	// Mutate the underlying pointer from the copy via Ptr() — original
	// must be unaffected because Ptr() allocates fresh memory.
	p := copy.Ptr()
	*p = p.Add(time.Hour)

	if !original.Time().Equal(fixedTime()) {
		t.Fatalf("original mutated via copy.Ptr() — got %v, want %v",
			original.Time(), fixedTime())
	}
}
