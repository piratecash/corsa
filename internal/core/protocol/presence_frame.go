package protocol

// presence_frame.go carries one contact's presence across the LOCAL RPC
// boundary — the interface asking its own node what it believes.
//
// This is not a peer-to-peer wire type, which is why it needs no protocol
// version and no capability: the two ends of this hop ship together. It is
// spelled out as a frame type rather than smuggled through the existing
// Identities slice because presence is three facts, not a set membership, and
// the whole point of the design is that "online" alone is not a complete answer.
//
// Strings rather than the domain enums on purpose: a frame is JSON, and a
// number would make an older reader silently mis-decode a state added later. An
// unknown name decodes to the safest value there is — unknown.

// PresenceFrame is one contact's presence as it crosses local RPC.
type PresenceFrame struct {
	// Identity is the contact's 40-hex fingerprint.
	Identity string `json:"identity"`
	// State is one of unknown / offline / probing / online.
	State string `json:"state"`
	// Source says what the state rests on: proof, passive, session_closed,
	// route_observation, probe_timeout, route_fallback. It is what lets the
	// interface show a proven presence differently from an inferred one, so
	// it is not optional decoration.
	Source string `json:"source,omitempty"`
	// Reason is meaningful only when State is unknown, and says which kind of
	// not-knowing this is: our own outage reads differently to a person than
	// a contact who cannot be probed.
	Reason string `json:"reason,omitempty"`
}
