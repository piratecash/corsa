// Package domain defines value types shared across layers.
// These types enforce compile-time distinction between semantically
// different string values, preventing the class of bugs where a
// transport address is passed where a routing identity is expected
// (and vice versa).
package domain

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Typed scalars
// ---------------------------------------------------------------------------

// PeerIdentity is an Ed25519 fingerprint (40-char lowercase hex).
// Used for routing, announce targets, split horizon filtering,
// and identity-level deduplication.
type PeerIdentity string

// PeerAddress is a remote peer's transport address (host:port).
// Used for health tracking, dial candidates, TCP connections,
// and session map keys.
type PeerAddress string

// PeerPublicKey is a peer's Ed25519 public key encoded as base64.
// Kept distinct from PeerIdentity: one is the routing fingerprint,
// the other is the verification material exchanged on the wire.
type PeerPublicKey string

// PeerBoxKey is a peer's Curve25519 box public key encoded as base64.
// Used for DM encryption and persisted trust/contact metadata.
type PeerBoxKey string

// PeerBoxSignature is the Ed25519 signature binding PeerBoxKey to
// PeerIdentity. Stored separately so call sites cannot accidentally swap
// the raw box key with its authentication proof.
type PeerBoxSignature string

// ListenAddress is the local node's bind address (e.g. ":64646").
// Kept separate from PeerAddress to prevent mixing local binding
// with remote transport addresses at compile time.
type ListenAddress string

// PeerIP is the canonical bare-IP form of a peer endpoint (IPv4 or
// IPv6 textual representation after IPv4-mapped IPv6 has been collapsed
// to the bare IPv4 form). Distinct from PeerAddress, which carries the
// host:port tuple used for dialling. Used by the advertise-address
// convergence layer for trusted-advertise triples, observed-IP history,
// consensus computation, and self-advertise overrides. An empty value
// is a legal "no value" sentinel — absence is encoded by the zero value
// rather than a separate optional wrapper because the canonical form of
// a missing IP is still an empty string on the wire.
type PeerIP string

// PeerPort is a peer's listening port. Kept as a distinct type so the
// compiler prevents silent mix-ups between a port and any other small
// integer flowing through the advertise-convergence layer (peer counters,
// build numbers, version levels). Value range is the TCP/UDP-legal
// inclusive 1..65535; zero is the explicit "no valid port on the wire"
// sentinel used by passive-learning fallbacks to collapse to
// config.DefaultPeerPort. Callers that persist or JoinHostPort the value
// perform the single textual conversion at their boundary — the runtime
// path never stores the port as a string.
type PeerPort int

// IsValid reports whether p falls inside the inclusive 1..65535 range
// that both the TCP stack and the hello.advertise_port wire contract
// require. Every other value — zero, negative, or >65535 — must be
// treated by the receiver as "no valid advertise port" and collapsed
// to config.DefaultPeerPort per docs/advertise-address-phase1-deprecation.md
// §7.2.
func (p PeerPort) IsValid() bool {
	return p >= 1 && p <= 65535
}

// UnmarshalJSON decodes a JSON value into PeerPort with a lenient contract:
// a JSON integer decodes as-is, a JSON string is accepted as a v10-style
// compatibility shape (some implementations historically used a string
// wire form for ports), and any other representation — null, float with
// a fractional part, boolean, object, array, unparseable text — collapses
// silently to zero. The zero value is the explicit "no valid port" sentinel
// that callers combine with IsValid to trigger the config.DefaultPeerPort
// fallback. This keeps a malformed advertise_port from failing the whole
// Frame decode and guarantees the advertise-address phase 1 deprecation
// contract: "any non-integer payload, missing field, or value outside
// 1..65535 collapses to DefaultPeerPort".
func (p *PeerPort) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*p = 0
		return nil
	}
	// JSON string: unwrap and try to parse as a decimal integer.
	if trimmed[0] == '"' {
		var raw string
		if err := json.Unmarshal(trimmed, &raw); err != nil {
			*p = 0
			return nil
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			*p = 0
			return nil
		}
		*p = PeerPort(value)
		return nil
	}
	// JSON number: decode into int. json.Number handling is not needed
	// because we do not accept floats as valid ports — fractional wire
	// values collapse to zero so IsValid rejects them downstream.
	var value int
	if err := json.Unmarshal(trimmed, &value); err != nil {
		*p = 0
		return nil
	}
	*p = PeerPort(value)
	return nil
}

// PeerSource describes how a peer address was discovered.
type PeerSource string

const (
	PeerSourceBootstrap    PeerSource = "bootstrap"
	PeerSourcePersisted    PeerSource = "persisted"
	PeerSourcePeerExchange PeerSource = "peer_exchange"
	PeerSourceManual       PeerSource = "manual"
	PeerSourceAnnounce     PeerSource = "announce"
)

// String returns the raw string value of the source tag.
func (s PeerSource) String() string { return string(s) }

// PeerDirection indicates how a session was established.
type PeerDirection string

const (
	PeerDirectionOutbound PeerDirection = "outbound"
	PeerDirectionInbound  PeerDirection = "inbound"
)

// String returns the raw direction label.
func (d PeerDirection) String() string { return string(d) }

// ---------------------------------------------------------------------------
// Role-specific aggregate types
// ---------------------------------------------------------------------------

// KnownPeer is a stable persisted/bootstrap record representing a dial
// candidate. It replaces the previous use of transport.Peer (which carried
// raw string fields) and peerEntry for in-memory representation.
type KnownPeer struct {
	Address  PeerAddress
	Source   PeerSource
	NodeType NodeType   // node role classification (full, client)
	Network  NetGroup   // network group classification for this address
	AddedAt  *time.Time // first time this address was seen
}

// PeerSessionRef describes a live outbound session with a remote peer.
// One KnownPeer may have zero or one active outbound session.
type PeerSessionRef struct {
	Address      PeerAddress
	Identity     PeerIdentity
	ConnID       ConnID
	Version      int
	Capabilities []Capability
	AuthOK       bool
}

// ---------------------------------------------------------------------------
// Aggregate network status
// ---------------------------------------------------------------------------

// NetworkStatus represents the aggregate health of the node's network
// connectivity. It is the single source of truth for policy decisions
// (e.g. whether to request peers during initial sync) and for Desktop
// UI rendering. Desktop must obtain this value from the node layer via
// the fetch_aggregate_status command rather than computing it locally.
//
// See docs/mesh.md § Aggregate Status and
// docs/peer-discovery-conditional-get-peers.ru.md § Шаг 2a.
type NetworkStatus string

const (
	// NetworkStatusOffline — no known peers exist.
	NetworkStatusOffline NetworkStatus = "offline"
	// NetworkStatusReconnecting — all peers are reconnecting, none connected.
	NetworkStatusReconnecting NetworkStatus = "reconnecting"
	// NetworkStatusLimited — zero or one usable peer (healthy+degraded).
	NetworkStatusLimited NetworkStatus = "limited"
	// NetworkStatusWarning — usable peers exist but less than half of connected are usable.
	NetworkStatusWarning NetworkStatus = "warning"
	// NetworkStatusHealthy — at least half of connected peers are usable (minimum 2).
	NetworkStatusHealthy NetworkStatus = "healthy"
)

// String returns the raw status label.
func (s NetworkStatus) String() string { return string(s) }

// IsHealthy reports whether the aggregate network status is in the steady-state
// healthy mode. Policy helpers use this to decide whether peer exchange
// (get_peers) can be skipped during initial sync.
func (s NetworkStatus) IsHealthy() bool { return s == NetworkStatusHealthy }

// AggregateStatusSnapshot is an immutable point-in-time snapshot of the
// node's aggregate network health. It is returned by the node layer to
// Desktop and consumed by internal policy helpers.
type AggregateStatusSnapshot struct {
	// Status is the computed aggregate network health label.
	Status NetworkStatus
	// UsablePeers is the count of healthy + degraded peers that can route messages.
	UsablePeers int
	// ConnectedPeers is usable + stalled (TCP-connected but not routing).
	ConnectedPeers int
	// TotalPeers is connected + reconnecting.
	TotalPeers int
	// PendingMessages is the total number of unsent pending messages across all peers.
	PendingMessages int
	// ComputedAt is the wall-clock time when the node layer last recomputed
	// this snapshot. Desktop uses it as the "last checked" indicator in the
	// Info tab — it proves the node layer is alive and responsive.
	ComputedAt time.Time
}

// EqualContent reports whether two aggregate snapshots carry the same
// semantic payload. ComputedAt is intentionally excluded: it is a liveness
// heartbeat, not a visible part of the aggregate state, and including it
// would make every recompute look "changed" even when nothing the UI cares
// about moved. Publishers use this to suppress no-op events that otherwise
// flood subscribers with identical snapshots during peer connection storms.
func (s AggregateStatusSnapshot) EqualContent(other AggregateStatusSnapshot) bool {
	return s.Status == other.Status &&
		s.UsablePeers == other.UsablePeers &&
		s.ConnectedPeers == other.ConnectedPeers &&
		s.TotalPeers == other.TotalPeers &&
		s.PendingMessages == other.PendingMessages
}

// RouteChangeReason describes why a routing table mutation occurred.
type RouteChangeReason string

const (
	RouteChangeDirectPeerAdded    RouteChangeReason = "direct_peer_added"
	RouteChangeDirectPeerRemoved  RouteChangeReason = "direct_peer_removed"
	RouteChangeAnnouncement       RouteChangeReason = "announcement"
	RouteChangeTransitInvalidated RouteChangeReason = "transit_invalidated"
	RouteChangeTTLExpired         RouteChangeReason = "ttl_expired"
)

// InboundPeerRef describes a live inbound connection that has completed
// (or is completing) the hello/auth handshake.
type InboundPeerRef struct {
	ListenAddress PeerAddress  // listen address declared in hello frame
	Identity      PeerIdentity // Ed25519 fingerprint from hello.Address
	ConnID        ConnID
	Capabilities  []Capability
	Networks      []NetGroup // network groups the peer declared
}
