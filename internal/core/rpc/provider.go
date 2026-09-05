package rpc

import (
	"context"
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

	// NodeStatus returns the PIP-0001 integration snapshot
	// (identity, public-key material, protocol/version, peer count,
	// uptime). Returned by the getNodeStatus RPC command. The struct
	// is a typed point-in-time view — callers should not assume any
	// field is stable across calls.
	NodeStatus() domain.NodeStatus

	// ResourceUsage returns a point-in-time snapshot of the process
	// memory footprint (runtime.MemStats: sys, heap alloc/inuse/idle/
	// released, GC metadata), the cgroup memory limit + usage, the live
	// connection count, and uptime — each with both machine-readable
	// integers and human-formatted strings. Returned by the
	// getResourceUsage RPC command; the desktop console Info tab renders
	// the memory/uptime subset.
	ResourceUsage() domain.ResourceUsage

	// FetchResourceBreakdown returns a JSON-encoded per-subsystem view of the
	// long-lived state this node holds — who is holding memory, rather than
	// how much the process holds. Returned by the getResourceBreakdown RPC
	// command.
	//
	// It is separate from ResourceUsage because the desktop client samples
	// that one every second to draw the Info tab; a breakdown folded into it
	// would make every node with a UI attached pay for a dozen domain-lock
	// acquisitions per second to produce numbers nothing renders.
	FetchResourceBreakdown() (json.RawMessage, error)
	// FetchFileTransfers returns a JSON-encoded list of active and pending
	// sender/receiver file transfers. Terminal states are excluded.
	FetchFileTransfers() (json.RawMessage, error)

	// FetchAllFileTransfers returns a JSON-encoded list of ALL
	// sender/receiver file transfers, including terminal states
	// (completed, failed, tombstone). Used by the desktop UI's file
	// tab to display transfer history. Use FetchFileTransfers when
	// only active/pending entries are needed (existing observability).
	FetchAllFileTransfers() (json.RawMessage, error)

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

	// ExplainFileRoute returns a JSON-encoded ranked plan describing where
	// a file command for dst would actually be sent. The first entry is
	// marked best=true; subsequent entries are the fall-back order. Empty
	// array means no usable next-hop. Powers the diagnostic command of the
	// same name in console / CLI / SDK.
	ExplainFileRoute(dst domain.PeerIdentity) (json.RawMessage, error)
}

// ChatlogProvider abstracts access to chatlog operations.
// Only available when desktop client is present.
type ChatlogProvider interface {
	FetchChatlog(ctx context.Context, topic, peerAddress string) (string, error)
	FetchChatlogPreviews(ctx context.Context) (string, error)
	FetchConversations(ctx context.Context) (string, error)
	// LookupEntryInConversation reports whether a message with the given ID
	// exists in the conversation with peerAddress, and reports a lookup that
	// FAILED as an error rather than as absence. Used by send_dm to validate
	// reply_to references synchronously before queueing.
	//
	// The bool-only form it replaced folded every failure into "not found",
	// so a cancelled context or a database fault reached the client as
	// "reply_to references a message that does not exist" — a claim about the
	// data that had never actually been established.
	LookupEntryInConversation(ctx context.Context, peerAddress, messageID string) (bool, error)
}

// DMRouterProvider abstracts access to dm_router.
type DMRouterProvider interface {
	Snapshot() service.RouterSnapshot
	// SendMessage queues a text DM. Returns
	// service.ErrConversationDeleteInflight when an in-flight
	// wipe is in progress for the peer; the caller
	// should surface a localised "wipe in progress" hint and
	// refuse the attempt.
	SendMessage(to domain.PeerIdentity, msg domain.OutgoingDM) error
	// SendFileAnnounce validates the transmit file, then asynchronously
	// sends a file_announce DM and registers the sender-side mapping
	// using the real DM message ID. Returns an error synchronously if
	// pre-send validation fails (e.g. transmit file missing) or if a
	// wipe is in progress for this peer — the
	// outgoing barrier returns service.ErrConversationDeleteInflight
	// (mapped to RPC ErrUnavailable / 503 in command_table) so the
	// caller can render the same "wipe in progress" hint as for
	// SendMessage instead of a generic internal error.
	// onAsyncFailure (may be nil) is called inside the send goroutine
	// when the async delivery fails, giving the caller a chance to
	// restore UI state (e.g. re-attach the file for retry).
	SendFileAnnounce(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure func()) error
	// SendMessageDelete removes the local copy of the target message —
	// chatlog row and file-transfer state — and returns the route it
	// took. Outgoing routes also leave a durable intent behind: the peer
	// is asked to mirror the deletion now if reachable, and otherwise as
	// soon as they are, across restarts. See docs/dm-commands.md.
	SendMessageDelete(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) (domain.MessageDeleteRoute, error)
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

	// OverloadStats returns the cumulative engagement counters for the
	// announce-loop overload gate (Phase 0). Returned struct is
	// value-safe; caller may pass it through JSON without further
	// copies. When the gate is not wired or has never engaged, all
	// fields stay at zero. See docs/routing.md "Operator tuning"
	// section for the contract.
	OverloadStats() routing.OverloadStats

	// DigestHeartbeatStats returns the cumulative route_sync
	// digest-as-heartbeat counters (sent / summary match / summary mismatch /
	// digests compared / compare match). Value-safe for JSON. Lets operators
	// see, without debug logging, whether periodic heartbeats are confirmed
	// (suppressing full syncs) or diverging. See docs/protocol/route_sync.md.
	DigestHeartbeatStats() routing.DigestHeartbeatStats

	// JournalCauseStats returns the lifetime per-cause tally of change-journal
	// appends (announce_upsert / health_aging / health_evidence / ttl_expiry /
	// …), keyed by stable snake_case cause name. It attributes steady-state
	// announce churn: a dominant health_aging share means quiet routes are
	// flapping Dead↔Good on the passive timeline rather than the network
	// actually changing. Value-safe for JSON; nil for a journal-less table.
	JournalCauseStats() map[string]uint64

	// HealthSnapshot returns a deep copy of every tracked
	// RouteHealthState (Phase 2). Used by the fetchRouteHealth RPC and,
	// since Snapshot.Health was narrowed to the Dead∪cooled subset, by
	// fetchRouteLookup's CompositeScore ranking (which needs the full
	// per-pair tiers). The snapshot is built under
	// routing.Table.t.mu.RLock and is safe to publish lock-free.
	// Returns nil when no health entries are tracked yet so
	// callers can compare cheaply.
	//
	// HealthSnapshot is a pure read — it MUST NOT trigger probe
	// sends or any other side effect. Observers reading the
	// snapshot for diagnostics get a point-in-time view; the
	// probe sender ticker operates on its own schedule, not on
	// RPC pressure.
	HealthSnapshot() []routing.RouteHealthState

	// ReputationSnapshot returns a deep copy of every tracked
	// per-(Identity, Uplink) reputation state (Phase 3 PR 12.7).
	// Used by fetchRouteReputation RPC observability — the
	// snapshot is built under routing.Table.t.mu.RLock and is
	// safe to publish lock-free. Returns nil when no
	// reputation entries are tracked yet.
	//
	// Pure read: must NOT fire probes, digests, MarkHopFailure,
	// or any other side effect. Reputation is local-only state
	// (Phase 3 §2.3 trust-budget invariant); observers see a
	// point-in-time view, never a re-emission.
	ReputationSnapshot() []routing.RouteReputationState
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

// DatagramProvider exposes the read-only surfaces of the datagram transport
// layer (docs/refactoring/datagram-transport.md §4.3, §5, §10). When the node
// does not implement it the commands are registered as unavailable, exactly
// like the routing group.
//
// Every method is a PURE READ: none of them reserves a replay slot, rotates
// the explore counter, dials anything or spends a cryptographic budget. That
// is a contract of the layer, not an implementation detail — the probe is what
// an artifact owner puts on a periodic ticker, and a diagnostic with side
// effects on that path is a diagnostic that changes what it measures.
type DatagramProvider interface {
	// FetchDatagramSummary returns the JSON diagnostic of the local plane:
	// the conveyor's decision counters, the per-neighbour admission budget,
	// the weighted class queue and the §5 numbers all three run on. Returns
	// an error when the layer is not enabled — "off" and "idle" are
	// different facts and an operator needs to tell them apart.
	FetchDatagramSummary() (json.RawMessage, error)

	// DatagramReachable reports whether a datagram of this type would find a
	// first hop to dst right now.
	//
	// The guarantee is one-way: unreachable means a send performed at the
	// same moment would NOT have been queued — no_route, or a gate's
	// rejected, the last-hop dtype gate included. Reachable promises
	// nothing, because the probe is TOCTOU by construction.
	//
	// It answers with the layer's own JSON rather than a bool because the
	// two negatives are different facts: a destination that declared no
	// handler for the type is refused by a gate, while one that is off the
	// routing table is not, and §6.1 reacts to only one of them.
	DatagramReachable(
		ctx context.Context,
		dst domain.PeerIdentity,
		dtype domain.DType,
	) (json.RawMessage, error)

	// ExplainDatagramRoute returns the JSON ranked next-hop plan a real send
	// would build. Under route_policy=explore the plan reports the
	// comparator order and says so through first_candidate_guaranteed=false:
	// the rotation counter advances on a send, and a read-only plan must
	// neither move nor reserve it.
	ExplainDatagramRoute(
		ctx context.Context,
		dst domain.PeerIdentity,
		dtype domain.DType,
		policy domain.RoutePolicy,
	) (json.RawMessage, error)
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
