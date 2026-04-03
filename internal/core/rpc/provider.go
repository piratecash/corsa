package rpc

import (
	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
	"corsa/internal/core/service"
)

// NodeProvider abstracts access to the node service layer.
// Both DesktopClient and standalone node can implement this.
type NodeProvider interface {
	HandleLocalFrame(frame protocol.Frame) protocol.Frame
	Address() string
	ClientVersion() string
}

// ChatlogProvider abstracts access to chatlog operations.
// Only available when desktop client is present.
type ChatlogProvider interface {
	FetchChatlog(topic, peerAddress string) (string, error)
	FetchChatlogPreviews() (string, error)
	FetchConversations() (string, error)
	// HasEntryInConversation checks whether a message with the given ID
	// exists in the conversation with peerAddress. Used by send_dm to
	// validate reply_to references synchronously before queueing.
	HasEntryInConversation(peerAddress, messageID string) bool
}

// DMRouterProvider abstracts access to dm_router.
type DMRouterProvider interface {
	Snapshot() service.RouterSnapshot
	SendMessage(to domain.PeerIdentity, msg domain.OutgoingDM)
}

// MetricsProvider abstracts access to the metrics collector.
// Returns traffic history snapshots for RPC consumption.
type MetricsProvider interface {
	TrafficSnapshot() protocol.Frame
}

// RoutingProvider abstracts access to the distance-vector routing table.
// Exposes read-only snapshot and lookup operations for RPC observability.
// When nil (routing not enabled), commands are registered as unavailable.
type RoutingProvider interface {
	// RoutingSnapshot returns an immutable point-in-time copy of the full
	// routing table, safe to read without locks. The snapshot includes
	// entry counts (TotalEntries, ActiveEntries) and FlapState, so
	// separate count/flap methods are not needed.
	RoutingSnapshot() routing.Snapshot

	// PeerTransport returns the transport address and network group
	// for a directly connected peer identified by its Ed25519 fingerprint.
	// Returns zero values if the peer is not currently connected.
	PeerTransport(peerIdentity domain.PeerIdentity) (address domain.PeerAddress, network domain.NetGroup)
}

// DiagnosticProvider abstracts access to desktop-level diagnostic commands.
// Only the desktop client implements this; standalone node uses the base
// handlers from RegisterSystemCommands.
//
// The desktop ping is a network diagnostic: it opens TCP sessions to every
// connected peer, sends actual ping frames, and reports per-peer status.
// The desktop get_peers merges the raw peer list with peer health data
// and categorizes peers into connected/pending/known_only groups.
//
// DesktopVersion returns the desktop application version string (e.g. "1.0.0").
// It is used by the hello override to correctly identify as Client: "desktop"
// instead of the generic "rpc" identity used by the standalone node.
type DiagnosticProvider interface {
	ConsolePingJSON() (string, error)
	ConsolePeersJSON() (string, error)
	DesktopVersion() string
}
