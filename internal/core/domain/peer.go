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

// InboundPeerRef describes a live inbound connection that has completed
// (or is completing) the hello/auth handshake.
type InboundPeerRef struct {
	ListenAddress PeerAddress  // listen address declared in hello frame
	Identity      PeerIdentity // Ed25519 fingerprint from hello.Address
	ConnID        uint64
	Capabilities  []Capability
	Networks      []NetGroup // network groups the peer declared
}
