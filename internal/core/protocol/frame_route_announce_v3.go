package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// frame_route_announce_v3.go defines the wire format for the Phase 4
// compact announce frame route_announce_v3
// (docs/cluster-mesh/phase-4-compact-wire-signed.md §3.1, overview §7.1),
// gated by the mesh_routing_v3 capability.
//
// Like the Phase 3 route_sync frames and the Phase 2 route_probe /
// route_query frames, route_announce_v3 travels through the Frame
// dispatcher via the Frame.Type + Frame.RawLine bypass rather than as a
// new field on the universal Frame struct. This keeps each
// capability-gated wire shape compile-time isolated: when the Phase 6
// cleanup window drops legacy announce_routes / routes_update ingest,
// removal of a frame is a file delete rather than a sweep through the
// shared Frame struct (overview §5.2 Phase C).
//
// What v3 changes vs legacy announce_routes / routes_update:
//
//   - Origin is GONE. After the Phase 1 per-uplink storage refactor the
//     receiver derives the uplink from the sending session, so the wire
//     Origin field carries no routing-relevant information — it only
//     multiplied storage and bandwidth (overview §4.1). Dropping it saves
//     ~50% of full-sync wire volume on top of the Phase 0.x send-side
//     per-Identity collapse (overview §5.2 Phase B "Compact savings").
//   - A single frame carries both full sync and delta via the Kind
//     discriminator, instead of the announce_routes (full) /
//     routes_update (delta) frame-type split.
//   - Each entry may carry an optional ed25519 signature (Sig) over its
//     canonical bytes, signed by the destination identity's key. Signing
//     and verification land with the mesh_attested_links_v1 capability
//     (phase-4 §3.2); this file only defines the wire shape and the
//     canonical-bytes construction the signer and verifier share.
//   - Epoch is the sender's local table epoch (incremented on table
//     reset, e.g. process restart). It lets the receiver distinguish a
//     replay from an old process (epoch lower than known → ignore) from a
//     fresh table that invalidates the diff baseline (epoch higher than
//     known → drop baseline and request full sync).

// RouteAnnounceV3FrameType is the type identifier used in the unified
// Frame dispatcher for the compact Phase 4 announce frame.
const RouteAnnounceV3FrameType = "route_announce_v3"

// Kind discriminator values for RouteAnnounceV3Frame.Kind. A full frame
// carries the sender's complete view of the routes visible to the peer
// (split horizon applied); a delta carries only entries that changed
// since the last frame to that peer.
const (
	RouteAnnounceV3KindFull  = "full"
	RouteAnnounceV3KindDelta = "delta"
)

// RouteAnnounceV3Entry is a single route entry inside a route_announce_v3
// frame. It is the compact replacement for AnnounceRouteFrame: the Origin
// field is intentionally absent (see file header), and an optional Sig
// field carries the attested-links signature.
//
// Extra is an opaque JSON object forwarded unchanged through transit
// (the same forward-compat semantic legacy AnnounceRouteFrame.Extra
// provides). It is carried as a verbatim json.RawMessage so that a node
// re-announcing a learned route preserves the exact bytes the origin
// emitted — this is what keeps a Sig over the canonical bytes verifiable
// after the entry has passed through intermediate nodes that do not
// understand the Extra payload.
type RouteAnnounceV3Entry struct {
	Identity string          `json:"identity"`
	Hops     uint8           `json:"hops"`
	SeqNo    uint64          `json:"seq_no"`
	Extra    json.RawMessage `json:"extra,omitempty"`
	Sig      string          `json:"sig,omitempty"`
}

// RouteAnnounceV3Frame is the wire form of the Phase 4 compact announce.
//
// Wire layout (JSON):
//
//	{
//	  "type":      "route_announce_v3",
//	  "kind":      "full" | "delta",
//	  "epoch":     7,
//	  "issued_at": "2026-05-27T12:00:00Z",
//	  "entries": [
//	    { "identity": "<fp>", "hops": 2, "seq_no": 41, "extra": {…}, "sig": "<b64>" }
//	  ]
//	}
//
// IssuedAt is the sender-local RFC3339 timestamp; it is a diagnostic
// freshness hint, not consulted for acceptance decisions (per-(Identity,
// sender) SeqNo monotonicity governs that — overview §4.3).
type RouteAnnounceV3Frame struct {
	Type     string                 `json:"type"`
	Kind     string                 `json:"kind"`
	Epoch    uint64                 `json:"epoch"`
	IssuedAt string                 `json:"issued_at,omitempty"`
	Entries  []RouteAnnounceV3Entry `json:"entries"`
}

// validRouteAnnounceV3Kind reports whether kind is one of the two
// recognised discriminator values.
func validRouteAnnounceV3Kind(kind string) bool {
	switch kind {
	case RouteAnnounceV3KindFull, RouteAnnounceV3KindDelta:
		return true
	default:
		return false
	}
}

// MarshalRouteAnnounceV3Frame serialises a RouteAnnounceV3Frame to JSON
// bytes intended for Frame.RawLine. The Type field is filled from the
// constant when blank so call sites do not need to repeat it on every
// emit. Kind must be set to a recognised value — an unset or unknown Kind
// is a caller bug (the send path always classifies full vs delta), so it
// is rejected here rather than silently defaulted.
func MarshalRouteAnnounceV3Frame(f RouteAnnounceV3Frame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteAnnounceV3FrameType
	}
	if !validRouteAnnounceV3Kind(f.Kind) {
		return nil, fmt.Errorf("route announce v3 frame: invalid kind %q", f.Kind)
	}
	return json.Marshal(f)
}

// UnmarshalRouteAnnounceV3Frame deserialises a RouteAnnounceV3Frame from
// JSON bytes. Returns an error if Type does not match
// RouteAnnounceV3FrameType or if Kind is not a recognised discriminator
// value — both are required for the receive path to apply the frame
// correctly (the Kind decides full-replace vs delta-merge).
func UnmarshalRouteAnnounceV3Frame(data []byte) (RouteAnnounceV3Frame, error) {
	var f RouteAnnounceV3Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteAnnounceV3Frame{}, fmt.Errorf("unmarshal route announce v3 frame: %w", err)
	}
	if f.Type != RouteAnnounceV3FrameType {
		return RouteAnnounceV3Frame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteAnnounceV3FrameType)
	}
	if !validRouteAnnounceV3Kind(f.Kind) {
		return RouteAnnounceV3Frame{}, fmt.Errorf("route announce v3 frame: invalid kind %q", f.Kind)
	}
	return f, nil
}

// CanonicalSigningBytes returns the deterministic byte string that the
// attested-links signature (Sig) covers for this entry. The construction
// is the wire-protocol contract shared by the signer (phase-4 §3.2
// origin side) and the verifier (receiver side), so it must stay
// byte-stable across versions once mesh_attested_links_v1 ships.
//
// Layout — length-prefixed concatenation of (identity || extra). A
// length-prefixed binary encoding is used instead of JSON because JSON
// is not canonical (key order and whitespace vary by encoder), which
// would make signatures non-reproducible. Each variable-length field
// (identity, extra) is preceded by an 8-byte big-endian length so no
// field's bytes can be confused with another's (prevents (a,"") and
// ("",a) colliding).
//
// HOPS, EPOCH, AND SEQ_NO ARE ALL DELIBERATELY EXCLUDED. All three
// vary per emitter — every signing-related design iteration found a
// new field that transits restamp on re-emit, and each broke
// multi-hop verification when the receiver recomputed canonical bytes
// from the LAST emitter's wire value rather than the origin's:
//
//   - hops increments on every transit hop (receiver convention, +1
//     on ingest).
//   - epoch is the sending node's local table-generation counter; the
//     origin signs with their own epoch, but transit stamps its own.
//   - seq_no on the wire is synthesised by the EMITTER via
//     nextOutboundSeqLockedPerPeer / nextOutboundSeqLockedBroadcast
//     (see route_store.go). Transit re-emit can advance the wire
//     seq_no past the origin's native value (outboundMax+1
//     monotonicity guard) while forwarding AttestedSig verbatim, so
//     including seq_no in the signed payload makes the origin's
//     signature unverifiable after the first re-emit.
//
// Multi-hop transit verification — the very thing Phase 5 anchor
// publication needs to prove a DHT lookup result reaches an identity
// through the advertised anchor chain — requires the canonical bytes
// to be reconstructible from origin-stable fields only. Excluding all
// three per-emitter fields lets the signature attest to the stable
// properties of the identity's claim (identity + extra) while
// leaving every per-emitter wire detail unsigned. This mirrors the
// design of attestation protocols such as BGP RPKI ROAs.
//
// Replay-protection trade-off (acceptable for this design): a
// signed Extra payload can in principle be replayed at a different
// wire seq_no, because seq_no is no longer bound into the
// signature. Per-(Identity, sender) SeqNo monotonicity on the wire
// still rejects stale-seqno replays at the receiver, so the only
// surviving replay is "origin-signed Extra appears at a fresh
// wire seq_no through a different uplink". The Extra payload is
// forward-compat metadata (e.g. onion box keys); it does not carry
// time-sensitive secrets, so a replay reintroduces authentic-but-
// stale content rather than a forgery. Origins that need
// freshness binding in Extra should embed an explicit timestamp /
// nonce inside Extra itself (and re-sign on rotation).
//
// Extra is hashed verbatim as it appears on the wire. Because
// transit forwards Extra unchanged (see RouteAnnounceV3Entry doc),
// the verifier reconstructs identical bytes even after the entry
// has crossed intermediate nodes.
func (e RouteAnnounceV3Entry) CanonicalSigningBytes() []byte {
	return canonicalRouteAnnounceV3Bytes(e.Identity, e.Extra)
}

// canonicalRouteAnnounceV3Bytes is the field-level core of
// CanonicalSigningBytes, split out so tests and the future signer can
// build the canonical bytes from raw fields without constructing a full
// entry. See CanonicalSigningBytes for the layout contract and the
// rationale for excluding hops, epoch, and seq_no.
func canonicalRouteAnnounceV3Bytes(identity string, extra json.RawMessage) []byte {
	idBytes := []byte(identity)
	// 8 (id len) + len(id) + 8 (extra len) + len(extra).
	out := make([]byte, 0, 8+len(idBytes)+8+len(extra))
	out = appendLengthPrefixed(out, idBytes)
	out = appendLengthPrefixed(out, []byte(extra))
	return out
}

// appendLengthPrefixed appends an 8-byte big-endian length followed by
// the field bytes.
func appendLengthPrefixed(dst, field []byte) []byte {
	dst = appendUint64(dst, uint64(len(field)))
	return append(dst, field...)
}

// appendUint64 appends v as 8 big-endian bytes.
func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}
