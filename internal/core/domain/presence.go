package domain

import "fmt"

// presence.go answers ONE question: is the person behind this identity there
// right now, and how do we know?
//
// It is deliberately NOT the routing question. "Do I have a path to X" is
// answered by routing.Snapshot.ReachableIdentitiesWithTransit and stays there;
// it is the right answer to a delivery question and the wrong answer to a human
// one. The two were the same value for a long time, and that is what made a
// contact look present for up to ten minutes after they had gone: a route
// outlives its owner. The contract is docs/protocol/presence.md.
//
// Two independent facts are recorded, and they are separate types on purpose:
//
//   - PresenceState — what we believe about the contact;
//   - PresenceSource — what that belief RESTS ON.
//
// Collapsing them loses the distinction that the whole design exists to make.
// "Online because the contact signed a challenge for us a minute ago" and
// "online because a route to them is still in our table" are not the same
// claim, and the interface must be able to say which one it is showing.

// PresenceState is what this node believes about one contact's liveness.
//
// The zero value is PresenceUnknown, and that is the only zero value that is
// safe here: an absent map key means "we have nothing to say", never "offline".
// Reading "offline" out of an absent key is the failure mode that made a whole
// contact list go grey the moment this node lost its own connectivity.
type PresenceState uint8

const (
	// PresenceUnknown is the honest absence of an answer. It is not a
	// degraded "offline" — see PresenceUnknownReason for which flavour of
	// not-knowing this is.
	PresenceUnknown PresenceState = iota

	// PresenceOffline means the contact is believed absent, and the belief
	// rests on an observation ABOUT THEM: their session closed, or their
	// last route disappeared while our own connectivity was fine, or they
	// missed enough consecutive probes to exhaust the hysteresis.
	//
	// It is never set from a suppression of our own (quarantine, hold-down,
	// K-cap): that a route is gone because WE removed it says nothing about
	// the peer, and stating otherwise is the same lie with the sign flipped.
	PresenceOffline

	// PresenceProbing means a path exists but liveness is not established
	// yet — the window between "a route appeared" and "the target answered".
	// It is shown as not-green: a claim we have not yet earned.
	PresenceProbing

	// PresenceOnline means the contact is believed present. What that belief
	// rests on is PresenceSource, and the interface distinguishes them: a
	// proven presence and an inferred one are both useful and are not the
	// same thing.
	PresenceOnline
)

var presenceStateNames = map[PresenceState]string{
	PresenceUnknown: "unknown",
	PresenceOffline: "offline",
	PresenceProbing: "probing",
	PresenceOnline:  "online",
}

func (s PresenceState) String() string {
	if name, ok := presenceStateNames[s]; ok {
		return name
	}
	return fmt.Sprintf("presence_state(%d)", uint8(s))
}

// Valid reports whether s is one of the four defined states. It exists so a
// value crossing a process boundary is checked rather than assumed.
func (s PresenceState) Valid() bool {
	_, ok := presenceStateNames[s]
	return ok
}

// PresenceUnknownReason says WHY there is no answer. docs/protocol/presence.md §3 rule 8: "unknown"
// carries two genuinely different meanings for a reader — "we are the ones who
// are offline" and "nobody is watching this contact, so what we had went
// stale" — and a single zero cannot express both.
type PresenceUnknownReason uint8

const (
	// PresenceUnknownNotApplicable is the reason field of a state that is
	// not PresenceUnknown. Naming it explicitly keeps "no reason" from
	// being confused with "reason not recorded".
	PresenceUnknownNotApplicable PresenceUnknownReason = iota

	// PresenceUnknownNoLocalConnectivity: OUR network is down or
	// reconnecting, so every contact is unknown. Without this, our own
	// outage reads as "all of your contacts left".
	PresenceUnknownNoLocalConnectivity

	// PresenceUnknownRouteSuppressedLocally: a route existed and WE removed
	// it — quarantine, flap hold-down, seq hold-down, K-cap eviction. The
	// contact may be perfectly reachable; we merely stopped believing our
	// own path. Suppressions last up to 30 minutes, so calling this
	// "offline" would be a half-hour lie.
	PresenceUnknownRouteSuppressedLocally

	// PresenceUnknownStale: the last proof expired and this contact's
	// cadence class does not probe (the application is in the background).
	// The honest reading is "nobody has looked recently".
	PresenceUnknownStale

	// PresenceUnknownNotProbeable: the contact cannot answer a liveness
	// probe at all and no route-derived answer was available either. It is
	// derived from what the contact DECLARED, never from their silence —
	// silence from an old build is indistinguishable from silence from a
	// dead one, and treating it as offline is exactly the trap docs/protocol/presence.md §4 names.
	PresenceUnknownNotProbeable
)

var presenceUnknownReasonNames = map[PresenceUnknownReason]string{
	PresenceUnknownNotApplicable:          "n/a",
	PresenceUnknownNoLocalConnectivity:    "no_local_connectivity",
	PresenceUnknownRouteSuppressedLocally: "route_suppressed_locally",
	PresenceUnknownStale:                  "stale",
	PresenceUnknownNotProbeable:           "not_probeable",
}

func (r PresenceUnknownReason) String() string {
	if name, ok := presenceUnknownReasonNames[r]; ok {
		return name
	}
	return fmt.Sprintf("presence_unknown_reason(%d)", uint8(r))
}

// PresenceSource is what the state rests on. It is what lets the interface
// draw a proven presence differently from an assumed one, and it is what makes
// the route fallback removable: when the fallback goes, so does exactly one
// value of this type, and every site that must change fails to compile.
type PresenceSource uint8

const (
	// PresenceSourceNone belongs to a state that rests on nothing observed —
	// PresenceUnknown, and PresenceProbing before the first answer.
	PresenceSourceNone PresenceSource = iota

	// PresenceSourceProof: the contact signed something for us. A valid
	// target_proof is signed by the holder of the contact's secret key,
	// bound to one attempt and one question, so it cannot be replayed by a
	// relay or produced by a cache. This is the only source that proves the
	// OWNER is there, rather than that some machinery around them is.
	PresenceSourceProof

	// PresenceSourcePassive: a frame carrying the contact's verified
	// signature arrived OVER THEIR OWN authenticated session. Weaker than a
	// probe only in timing — nobody chose when it would come — and free,
	// which is why an active conversation costs no probes at all.
	//
	// The session requirement is not incidental. A signature proves who
	// WROTE something, not that they are awake: relays store and forward, so
	// a relayed copy can arrive long after its author left, and can arrive
	// repeatedly. Accepting one would keep a contact green for as long as the
	// network kept replaying them.
	PresenceSourcePassive

	// PresenceSourceSessionClosed: their session with us ended and the close
	// was attributable to the remote side. Evidence of absence, not
	// presence; it belongs to PresenceOffline.
	PresenceSourceSessionClosed

	// PresenceSourceRouteObservation: their last route disappeared while our
	// own connectivity was healthy and the disappearance was not one of our
	// own suppressions. Also evidence of absence.
	PresenceSourceRouteObservation

	// PresenceSourceProbeTimeout: consecutive probes went unanswered up to
	// the hysteresis limit. Evidence of absence.
	PresenceSourceProbeTimeout

	// PresenceSourceRouteFallback is the TEMPORARY bridge: presence inferred
	// from the routing table for a contact that cannot be probed at all.
	//
	// It is a weaker claim than every other source here and is marked as
	// such all the way to the interface, which draws it as an outline rather
	// than a filled dot. It exists because the alternative — showing a whole
	// un-upgraded network as "unknown" — replaces one wrong answer with
	// another and reads to a user as the feature having broken.
	//
	// This value disappears together with the full routing table. See
	// node/presence_route_fallback.go for the removal contract and the guard
	// test that fails once the floor makes the bridge unnecessary.
	PresenceSourceRouteFallback
)

var presenceSourceNames = map[PresenceSource]string{
	PresenceSourceNone:             "none",
	PresenceSourceProof:            "proof",
	PresenceSourcePassive:          "passive",
	PresenceSourceSessionClosed:    "session_closed",
	PresenceSourceRouteObservation: "route_observation",
	PresenceSourceProbeTimeout:     "probe_timeout",
	PresenceSourceRouteFallback:    "route_fallback",
}

func (s PresenceSource) String() string {
	if name, ok := presenceSourceNames[s]; ok {
		return name
	}
	return fmt.Sprintf("presence_source(%d)", uint8(s))
}

// Proven reports whether the source is a signature by the contact. It is the
// predicate the interface uses to decide between a filled and an outlined
// indicator, so that "we saw them" and "we assume them" never look alike.
func (s PresenceSource) Proven() bool {
	return s == PresenceSourceProof || s == PresenceSourcePassive
}

// Presence is one contact's presence as a whole. It travels as a value; there
// is no partially-filled form, because the three fields only make sense
// together — a state without its source is the ambiguity this file removes.
type Presence struct {
	State  PresenceState
	Source PresenceSource
	// Reason is meaningful only when State is PresenceUnknown.
	Reason PresenceUnknownReason
}

// UnknownPresence is the value for a contact we have nothing to say about. It
// is also the zero value of Presence, which is what makes an absent map key
// safe to read.
func UnknownPresence(reason PresenceUnknownReason) Presence {
	return Presence{State: PresenceUnknown, Source: PresenceSourceNone, Reason: reason}
}

// OnlinePresence builds a present state attributed to source.
func OnlinePresence(source PresenceSource) Presence {
	return Presence{State: PresenceOnline, Source: source}
}

// OfflinePresence builds an absent state attributed to source.
func OfflinePresence(source PresenceSource) Presence {
	return Presence{State: PresenceOffline, Source: source}
}

// ProbingPresence is "a path exists, liveness not established yet".
func ProbingPresence() Presence {
	return Presence{State: PresenceProbing, Source: PresenceSourceNone}
}

// IsProven reports whether this presence rests on the contact's own signature.
// Only a proven ONLINE is drawn as a filled indicator.
func (p Presence) IsProven() bool {
	return p.State == PresenceOnline && p.Source.Proven()
}

// IsInferred reports whether this presence is the route fallback — believed
// present, but on our own routing inference rather than on anything the
// contact did.
func (p Presence) IsInferred() bool {
	return p.State == PresenceOnline && p.Source == PresenceSourceRouteFallback
}

func (p Presence) String() string {
	if p.State == PresenceUnknown {
		return fmt.Sprintf("%s(%s)", p.State, p.Reason)
	}
	return fmt.Sprintf("%s(%s)", p.State, p.Source)
}

// ParsePresenceState turns a wire name back into a state. An unrecognised name
// is PresenceUnknown, which is the only safe default: a reader that does not
// know a state added later must say "no answer", never invent one.
func ParsePresenceState(name string) PresenceState {
	for state, candidate := range presenceStateNames {
		if candidate == name {
			return state
		}
	}
	return PresenceUnknown
}

// ParsePresenceSource turns a wire name back into a source. An unrecognised
// name is PresenceSourceNone — an unattributed belief, which downstream reads
// as not proven, so an unknown source can never be drawn as evidence.
func ParsePresenceSource(name string) PresenceSource {
	for source, candidate := range presenceSourceNames {
		if candidate == name {
			return source
		}
	}
	return PresenceSourceNone
}

// ParsePresenceUnknownReason turns a wire name back into a reason.
func ParsePresenceUnknownReason(name string) PresenceUnknownReason {
	for reason, candidate := range presenceUnknownReasonNames {
		if candidate == name {
			return reason
		}
	}
	return PresenceUnknownNotApplicable
}

// PresenceSet is a read-only view over the presence of many contacts.
//
// It is a type rather than a bare map because the lookup rule is part of the
// contract: an absent key is UNKNOWN, and every caller must get that answer
// without remembering to write it. A bare map hands out the zero value of its
// value type, which is how "offline" got read out of "no data".
type PresenceSet map[PeerIdentity]Presence

// Get returns the presence of one identity. An absent key is unknown, with the
// stale reason: we have no record, which is exactly "nobody looked".
func (s PresenceSet) Get(identity PeerIdentity) Presence {
	if s == nil {
		return UnknownPresence(PresenceUnknownStale)
	}
	presence, ok := s[identity]
	if !ok {
		return UnknownPresence(PresenceUnknownStale)
	}
	return presence
}

// Clone returns a caller-owned copy. Callers mutate their own view (the UI
// merges partial updates into a cached status), so handing out the live map
// would let a reader observe a half-applied generation.
func (s PresenceSet) Clone() PresenceSet {
	if s == nil {
		return nil
	}
	clone := make(PresenceSet, len(s))
	for identity, presence := range s {
		clone[identity] = presence
	}
	return clone
}
