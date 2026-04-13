// Package domain defines value types shared across layers.
// These types enforce compile-time distinction between semantically
// different string values, preventing the class of bugs where a
// transport address is passed where a routing identity is expected
// (and vice versa).
package domain

import "time"

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

// ListenAddress is the local node's bind address (e.g. ":64646").
// Kept separate from PeerAddress to prevent mixing local binding
// with remote transport addresses at compile time.
type ListenAddress string

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
	ConnID       uint64
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
}

// InboundPeerRef describes a live inbound connection that has completed
// (or is completing) the hello/auth handshake.
type InboundPeerRef struct {
	ListenAddress PeerAddress  // listen address declared in hello frame
	Identity      PeerIdentity // Ed25519 fingerprint from hello.Address
	ConnID        uint64
	Capabilities  []Capability
	Networks      []NetGroup // network groups the peer declared
}
