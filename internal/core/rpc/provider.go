package rpc

import (
	"encoding/json"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service"
)

// NodeProvider abstracts access to the node service layer.
// Both DesktopClient and standalone node can implement this.
type NodeProvider interface {
	HandleLocalFrame(frame protocol.Frame) protocol.Frame
	Address() string
	ClientVersion() string
	// FetchFileTransfers returns a JSON-encoded list of active and pending
	// sender/receiver file transfers. Terminal states are excluded.
	FetchFileTransfers() (json.RawMessage, error)

	// FetchFileMappings returns a JSON-encoded list of active and pending
	// sender file mappings (TransmitPath is excluded from the output).
	FetchFileMappings() (json.RawMessage, error)

	// RetryFileChunk forces an immediate retry of a stalled chunk request
	// for the given file ID.
	RetryFileChunk(fileID domain.FileID) error

	// StartFileDownload begins downloading a file that was previously
	// announced via file_announce DM. Sends the first chunk_request.
	StartFileDownload(fileID domain.FileID) error

	// CancelFileDownload aborts an active download, deletes partial data,
	// and resets the receiver mapping to available state.
	CancelFileDownload(fileID domain.FileID) error

	// RestartFileDownload resets a failed download back to available state
	// so the user can re-initiate the download.
	RestartFileDownload(fileID domain.FileID) error
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
	// SendFileAnnounce validates the transmit file, then asynchronously
	// sends a file_announce DM and registers the sender-side mapping
	// using the real DM message ID. Returns an error synchronously if
	// pre-send validation fails (e.g. transmit file missing).
	// onAsyncFailure (may be nil) is called inside the send goroutine
	// when the async delivery fails, giving the caller a chance to
	// restore UI state (e.g. re-attach the file for retry).
	SendFileAnnounce(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure func()) error
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

// ConnectionDiagnosticProvider exposes ConnectionManager and PeerProvider
// data for RPC observability. When nil (CM/PP not wired), commands are
// registered as unavailable.
type ConnectionDiagnosticProvider interface {
	// ActivePeersJSON returns a JSON-encoded snapshot of CM slots:
	// {"slots": [...], "count": N, "max_slots": M}
	ActivePeersJSON() (json.RawMessage, error)

	// ListPeersJSON returns a JSON-encoded list of all known peers
	// from PeerProvider with ExcludeReasons:
	// {"peers": [...], "count": N}
	ListPeersJSON() (json.RawMessage, error)

	// ListBannedJSON returns a JSON-encoded list of banned IPs:
	// {"banned_ips": [...], "count": N}
	ListBannedJSON() (json.RawMessage, error)

	// ActiveConnectionsJSON returns a JSON-encoded snapshot of all
	// currently live peer connections (both inbound and outbound):
	// {"version": 1, "connections": [...], "count": N}
	ActiveConnectionsJSON() (json.RawMessage, error)
}

// CaptureProvider abstracts access to the traffic capture subsystem.
// When nil (capture not available), commands are registered as unavailable.
type CaptureProvider interface {
	// StartCaptureByConnIDs starts recording for the given conn_ids.
	StartCaptureByConnIDs(connIDs []uint64, format string) (json.RawMessage, error)
	// StartCaptureByIPs starts recording for the given remote IPs.
	StartCaptureByIPs(ips []string, format string) (json.RawMessage, error)
	// StartCaptureAll starts recording for all peer connections.
	StartCaptureAll(format string) (json.RawMessage, error)
	// StopCaptureByConnIDs stops recording for the given conn_ids.
	StopCaptureByConnIDs(connIDs []uint64) (json.RawMessage, error)
	// StopCaptureByIPs stops recording for the given remote IPs.
	StopCaptureByIPs(ips []string) (json.RawMessage, error)
	// StopCaptureAll stops all recording.
	StopCaptureAll() (json.RawMessage, error)
}

// DiagnosticProvider abstracts access to desktop-level identity metadata.
// Only the desktop client implements this; standalone node uses the base
// handlers from RegisterSystemCommands.
//
// DesktopVersion returns the desktop application version string (e.g. "1.0.0").
// It is used by the hello override to correctly identify as Client: "desktop"
// instead of the generic "rpc" identity used by the standalone node.
type DiagnosticProvider interface {
	DesktopVersion() string
}
