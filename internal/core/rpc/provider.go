package rpc

import (
	"corsa/internal/core/protocol"
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
}

// DMRouterProvider abstracts access to dm_router.
type DMRouterProvider interface {
	Snapshot() service.RouterSnapshot
	SendMessage(to, body string)
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
