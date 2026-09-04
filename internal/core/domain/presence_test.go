package domain

import "testing"

// TestAbsentKeyIsUnknownNotOffline pins the rule the whole type exists for:
// having no record about a contact must never read as evidence that they are
// gone. A bare map would hand back the zero value of its value type, and when
// that type was bool the zero value meant "offline" — which is how one node's
// own outage turned into "all of your contacts left".
func TestAbsentKeyIsUnknownNotOffline(t *testing.T) {
	known := PeerIdentityFromWire("1111111111111111111111111111111111111111")
	absent := PeerIdentityFromWire("2222222222222222222222222222222222222222")

	set := PresenceSet{known: OnlinePresence(PresenceSourceProof)}

	if got := set.Get(absent); got.State != PresenceUnknown {
		t.Fatalf("absent key: got %s, want unknown", got)
	}
	if got := set.Get(absent).Reason; got != PresenceUnknownStale {
		t.Fatalf("absent key reason: got %s, want stale", got)
	}
	if got := PresenceSet(nil).Get(known); got.State != PresenceUnknown {
		t.Fatalf("nil set: got %s, want unknown", got)
	}
}

// TestZeroValueIsUnknown guards the same rule one level down: a Presence that
// nobody filled in must be unknown, so a struct that reaches a reader through
// any path other than the constructors is still safe.
func TestZeroValueIsUnknown(t *testing.T) {
	var zero Presence
	if zero.State != PresenceUnknown {
		t.Fatalf("zero Presence state: got %s, want unknown", zero.State)
	}
	if zero.IsProven() {
		t.Fatal("zero Presence must not read as proven")
	}
	if zero.IsInferred() {
		t.Fatal("zero Presence must not read as inferred")
	}
}

// TestProvenSeparatesEarnedFromInferred is the distinction the interface draws
// as filled versus outlined. A route-derived presence is online, but it is not
// proven, and the two must not answer the same predicate.
func TestProvenSeparatesEarnedFromInferred(t *testing.T) {
	cases := []struct {
		name     string
		presence Presence
		proven   bool
		inferred bool
	}{
		{"probe answered", OnlinePresence(PresenceSourceProof), true, false},
		{"their frame arrived", OnlinePresence(PresenceSourcePassive), true, false},
		{"route fallback", OnlinePresence(PresenceSourceRouteFallback), false, true},
		{"offline by session close", OfflinePresence(PresenceSourceSessionClosed), false, false},
		{"probing", ProbingPresence(), false, false},
		{"unknown", UnknownPresence(PresenceUnknownNotProbeable), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.presence.IsProven(); got != tc.proven {
				t.Fatalf("IsProven: got %v, want %v (%s)", got, tc.proven, tc.presence)
			}
			if got := tc.presence.IsInferred(); got != tc.inferred {
				t.Fatalf("IsInferred: got %v, want %v (%s)", got, tc.inferred, tc.presence)
			}
		})
	}
}

// TestEveryStateAndSourceIsNamed keeps a new enum member from reaching a log
// line or a diagnostic as a bare number. Adding one without a name fails here
// rather than in production output.
func TestEveryStateAndSourceIsNamed(t *testing.T) {
	for state := PresenceUnknown; state <= PresenceOnline; state++ {
		if !state.Valid() {
			t.Fatalf("state %d is not valid but is inside the defined range", state)
		}
		if got := state.String(); got == "" || got[0] == 'p' && len(got) > 14 && got[:14] == "presence_state" {
			t.Fatalf("state %d has no name: %q", state, got)
		}
	}
	for source := PresenceSourceNone; source <= PresenceSourceRouteFallback; source++ {
		if name, ok := presenceSourceNames[source]; !ok || name == "" {
			t.Fatalf("source %d has no name", source)
		}
	}
	for reason := PresenceUnknownNotApplicable; reason <= PresenceUnknownNotProbeable; reason++ {
		if name, ok := presenceUnknownReasonNames[reason]; !ok || name == "" {
			t.Fatalf("unknown-reason %d has no name", reason)
		}
	}
}

// TestCloneIsCallerOwned: the UI merges partial status updates into a cached
// copy, so handing out the live map would let a reader mutate a generation
// another reader is still walking.
func TestCloneIsCallerOwned(t *testing.T) {
	id := PeerIdentityFromWire("3333333333333333333333333333333333333333")
	original := PresenceSet{id: OnlinePresence(PresenceSourceProof)}

	clone := original.Clone()
	clone[id] = OfflinePresence(PresenceSourceProbeTimeout)

	if got := original.Get(id).State; got != PresenceOnline {
		t.Fatalf("mutating the clone changed the original: got %s", got)
	}
	if PresenceSet(nil).Clone() != nil {
		t.Fatal("cloning a nil set must stay nil")
	}
}
