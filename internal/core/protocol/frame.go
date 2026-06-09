package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type Frame struct {
	Type                   string `json:"type"`
	Version                int    `json:"version,omitempty"`
	MinimumProtocolVersion int    `json:"minimum_protocol_version,omitempty"`
	Client                 string `json:"client,omitempty"`
	Node                   string `json:"node,omitempty"`
	Network                string `json:"network,omitempty"`
	Listen                 string `json:"listen,omitempty"`
	Listener               string `json:"listener,omitempty"`
	// AdvertisePort is the self-reported listening port introduced by the
	// advertise-address phase 1 deprecation rollout (ProtocolVersion=11).
	// It is the only authoritative wire source of the peer's listening
	// port from this rollout onward — Listen/host loses truth status, the
	// inbound TCP source port must NEVER be reused as a listening port.
	// Canonical wire shape is a JSON integer in the inclusive range
	// 1..65535. The parser side accepts a v10-style JSON string shape too
	// (see domain.PeerPort.UnmarshalJSON): an unparseable string, a
	// missing field, a null, or any value outside 1..65535 collapses to
	// the zero PeerPort sentinel, which callers combine with IsValid() to
	// fall back to config.DefaultPeerPort. omitempty on the writer side
	// keeps legacy v10 frames indistinguishable on the wire from a v11
	// frame with no port set — legacy parsers treat the absent field as
	// "no advertise_port", which is the same as the v11 fallback.
	AdvertisePort   domain.PeerPort       `json:"advertise_port,omitempty"`
	NodeType        string                `json:"node_type,omitempty"`
	ClientVersion   string                `json:"client_version,omitempty"`
	ClientBuild     int                   `json:"client_build,omitempty"`
	Services        []string              `json:"services,omitempty"`
	Networks        []string              `json:"networks,omitempty"` // self-declared reachable network groups (hello frame)
	Address         string                `json:"address,omitempty"`
	Recipient       string                `json:"recipient,omitempty"`
	PubKey          string                `json:"pubkey,omitempty"`
	BoxKey          string                `json:"boxkey,omitempty"`
	BoxSig          string                `json:"boxsig,omitempty"`
	Peers           []string              `json:"peers,omitempty"`
	Identities      []string              `json:"identities,omitempty"`
	Contacts        []ContactFrame        `json:"contacts,omitempty"`
	Topic           string                `json:"topic,omitempty"`
	ID              string                `json:"id,omitempty"`
	IDs             []string              `json:"ids,omitempty"`
	PendingIDs      []string              `json:"pending_ids,omitempty"`
	PendingMessages []PendingMessageFrame `json:"pending_messages,omitempty"`
	Item            *MessageFrame         `json:"item,omitempty"`
	Receipt         *ReceiptFrame         `json:"receipt,omitempty"`
	Messages        []MessageFrame        `json:"messages,omitempty"`
	Receipts        []ReceiptFrame        `json:"receipts,omitempty"`
	Notices         []NoticeFrame         `json:"notices,omitempty"`
	PeerHealth      []PeerHealthFrame     `json:"peer_health,omitempty"`
	Subscriber      string                `json:"subscriber,omitempty"`
	Flag            string                `json:"flag,omitempty"`
	CreatedAt       string                `json:"created_at,omitempty"`
	DeliveredAt     string                `json:"delivered_at,omitempty"`
	TTLSeconds      int                   `json:"ttl_seconds,omitempty"`
	Body            string                `json:"body,omitempty"`
	Ciphertext      string                `json:"ciphertext,omitempty"`
	ExpiresAt       int64                 `json:"expires_at,omitempty"`
	Count           int                   `json:"count,omitempty"`
	Limit           int                   `json:"limit,omitempty"`
	Status          string                `json:"status,omitempty"`
	AckType         string                `json:"ack_type,omitempty"`
	ObservedAddress string                `json:"observed_address,omitempty"`
	Challenge       string                `json:"challenge,omitempty"`
	Signature       string                `json:"signature,omitempty"`
	Code            string                `json:"code,omitempty"`
	Error           string                `json:"error,omitempty"`
	// Details carries machine-readable payload for connection_notice-style
	// control frames. Shape is a function of Code. Kept as json.RawMessage
	// so the wire layer stays agnostic to per-code schemas — decode it in
	// the handler that understands the Code. omitempty prevents historical
	// frames (hello, welcome, auth_*) from serialising "details": null.
	Details         json.RawMessage       `json:"details,omitempty"`
	DMHeaders       []DMHeaderFrame       `json:"dm_headers,omitempty"`
	ChatEntries     []ChatEntryFrame      `json:"chat_entries,omitempty"`
	ChatPreviews    []ChatPreviewFrame    `json:"chat_previews,omitempty"`
	Conversations   []ConversationFrame   `json:"conversations,omitempty"`
	NetworkStats    *NetworkStatsFrame    `json:"network_stats,omitempty"`
	AggregateStatus *AggregateStatusFrame `json:"aggregate_status,omitempty"`
	ResourceUsage   *ResourceUsageFrame   `json:"resource_usage,omitempty"`
	TrafficHistory  *TrafficHistoryFrame  `json:"traffic_history,omitempty"`
	Capabilities    []string              `json:"capabilities,omitempty"`

	// Relay fields (Iteration 1 — hop-by-hop relay)
	HopCount    int    `json:"hop_count,omitempty"`
	MaxHops     int    `json:"max_hops,omitempty"`
	PreviousHop string `json:"previous_hop,omitempty"`

	// Routing table fields (Iteration 1 — distance vector)
	// Used with type="announce_routes".
	AnnounceRoutes []AnnounceRouteFrame `json:"routes,omitempty"`

	// RawLine carries a pre-serialized JSON line (including trailing newline)
	// that bypasses MarshalFrameLine's json.Marshal. Used for frame types like
	// file_command that have their own wire format (FileCommandFrame) and must
	// be forwarded verbatim through the per-connection write queue.
	RawLine string `json:"-"`
}

type ContactFrame struct {
	Address string `json:"address"`
	PubKey  string `json:"pubkey"`
	BoxKey  string `json:"boxkey"`
	BoxSig  string `json:"boxsig"`
}

type MessageFrame struct {
	ID         string `json:"id"`
	Sender     string `json:"sender"`
	Recipient  string `json:"recipient"`
	Flag       string `json:"flag"`
	CreatedAt  string `json:"created_at"`
	TTLSeconds int    `json:"ttl_seconds"`
	Body       string `json:"body"`
}

type ReceiptFrame struct {
	MessageID   string `json:"message_id"`
	Sender      string `json:"sender"`
	Recipient   string `json:"recipient"`
	Status      string `json:"status"`
	DeliveredAt string `json:"delivered_at"`
}

type NoticeFrame struct {
	ID         string `json:"id"`
	ExpiresAt  int64  `json:"expires_at"`
	Ciphertext string `json:"ciphertext"`
}

type PeerHealthFrame struct {
	Address             string   `json:"address"`
	PeerID              string   `json:"peer_id,omitempty"`
	ConnID              uint64   `json:"conn_id,omitempty"`
	Network             string   `json:"network,omitempty"`
	Direction           string   `json:"direction,omitempty"`
	ClientVersion       string   `json:"client_version,omitempty"`
	ClientBuild         int      `json:"client_build,omitempty"`
	ProtocolVersion     int      `json:"protocol_version,omitempty"`
	State               string   `json:"state"`
	Connected           bool     `json:"connected"`
	PendingCount        int      `json:"pending_count,omitempty"`
	LastConnectedAt     string   `json:"last_connected_at,omitempty"`
	LastDisconnectedAt  string   `json:"last_disconnected_at,omitempty"`
	LastPingAt          string   `json:"last_ping_at,omitempty"`
	LastPongAt          string   `json:"last_pong_at,omitempty"`
	LastUsefulSendAt    string   `json:"last_useful_send_at,omitempty"`
	LastUsefulReceiveAt string   `json:"last_useful_receive_at,omitempty"`
	ConsecutiveFailures int      `json:"consecutive_failures,omitempty"`
	LastError           string   `json:"last_error,omitempty"`
	Score               int      `json:"score"`
	BannedUntil         string   `json:"banned_until,omitempty"`
	BytesSent           int64    `json:"bytes_sent"`
	BytesReceived       int64    `json:"bytes_received"`
	TotalTraffic        int64    `json:"total_traffic"`
	Capabilities        []string `json:"capabilities,omitempty"`
	SlotState           string   `json:"slot_state,omitempty"`             // CM slot lifecycle state (queued/dialing/active/reconnecting/retry_wait)
	SlotRetryCount      int      `json:"slot_retry_count,omitempty"`       // how many consecutive dial retries the slot has performed
	SlotGeneration      uint64   `json:"slot_generation,omitempty"`        // monotonic generation counter for stale-event suppression
	SlotConnectedAddr   string   `json:"slot_connected_address,omitempty"` // actual TCP address used for the active connection

	// Capture state — per-connection recording diagnostics (plan §8.1).
	Recording              bool   `json:"recording,omitempty"`
	RecordingFile          string `json:"recording_file,omitempty"`
	RecordingStartedAt     string `json:"recording_started_at,omitempty"`
	RecordingScope         string `json:"recording_scope,omitempty"`
	RecordingError         string `json:"recording_error,omitempty"`
	RecordingDroppedEvents int64  `json:"recording_dropped_events,omitempty"`

	// Machine-readable disconnect diagnostics.
	//
	// LastErrorCode: protocol.ErrorCode of the most recent pre-handshake
	// rejection (e.g. "incompatible-protocol-version"). Set by
	// penalizeOldProtocolPeer. Cleared on successful reconnect and by
	// operator add_peer override.
	//
	// LastDisconnectCode: protocol.ErrorCode that caused the most recent
	// post-handshake socket teardown (e.g. "frame-too-large",
	// "rate-limited"). Empty when the disconnect was clean, non-protocol,
	// or the error maps to the generic "protocol-error" sentinel. Cleared
	// on successful reconnect.
	LastErrorCode               string `json:"last_error_code,omitempty"`
	LastDisconnectCode          string `json:"last_disconnect_code,omitempty"`
	IncompatibleVersionAttempts int    `json:"incompatible_version_attempts,omitempty"`
	LastIncompatibleVersionAt   string `json:"last_incompatible_version_at,omitempty"`
	ObservedPeerVersion         int    `json:"observed_peer_version,omitempty"`
	ObservedPeerMinimumVersion  int    `json:"observed_peer_minimum_version,omitempty"`
	VersionLockoutActive        bool   `json:"version_lockout_active,omitempty"`
}

// NetworkStatsFrame provides aggregated traffic statistics for the entire node.
type NetworkStatsFrame struct {
	TotalBytesSent     int64              `json:"total_bytes_sent"`
	TotalBytesReceived int64              `json:"total_bytes_received"`
	TotalTraffic       int64              `json:"total_traffic"`
	ConnectedPeers     int                `json:"connected_peers"`
	KnownPeers         int                `json:"known_peers"`
	PeerTraffic        []PeerTrafficFrame `json:"peer_traffic"`
}

// PeerTrafficFrame holds per-peer traffic counters.
type PeerTrafficFrame struct {
	Address       string `json:"address"`
	BytesSent     int64  `json:"bytes_sent"`
	BytesReceived int64  `json:"bytes_received"`
	TotalTraffic  int64  `json:"total_traffic"`
	Connected     bool   `json:"connected"`
}

// AggregateStatusFrame is the wire representation of the node's aggregate
// network health returned by the fetch_aggregate_status local RPC command.
// Desktop consumes this frame instead of computing the status locally.
type AggregateStatusFrame struct {
	Status          string `json:"status"`          // offline | reconnecting | limited | warning | healthy
	UsablePeers     int    `json:"usable_peers"`    // healthy + degraded — can route messages
	ConnectedPeers  int    `json:"connected_peers"` // usable + stalled
	TotalPeers      int    `json:"total_peers"`     // connected + reconnecting
	PendingMessages int    `json:"pending_messages"`

	// Version policy snapshot — embedded directly in AggregateStatusFrame
	// (not a separate top-level block). All fields are omitempty for
	// backward compatibility with consumers that predate version detection.
	//
	// UpdateReason is a closed enum with exactly four values:
	//   ""                                        — no signal (update_available=false)
	//   "peer_build_newer"                        — build heuristic only
	//   "incompatible_version_reporters"          — version evidence or lockout
	//   "peer_build_and_incompatible_version"     — both active
	//
	// Precedence: both > incompatible_version > peer_build > none.
	UpdateAvailable              bool   `json:"update_available,omitempty"`
	UpdateReason                 string `json:"update_reason,omitempty"`
	IncompatibleVersionReporters int    `json:"incompatible_version_reporters,omitempty"`
	MaxObservedPeerBuild         int    `json:"max_observed_peer_build,omitempty"`
	// MaxObservedPeerVersion is the highest protocol version among incompatible
	// peers (runtime reporters + persisted lockouts). See domain.VersionPolicySnapshot.
	MaxObservedPeerVersion int `json:"max_observed_peer_version,omitempty"`
}

// ResourceUsageFrame is the wire representation of the node's process
// memory footprint, cgroup memory, connection count, and uptime,
// returned by the fetch_resource_usage local RPC command. Desktop
// consumes this frame (via the prober) the same way it consumes
// fetch_aggregate_status — through the local-frame dispatch, NOT a
// direct method call into the node. It carries the FULL field set
// (machine + human) so the embedded path is byte-for-byte equivalent to
// the public getResourceUsage RPC; the Info tab renders only the Memory
// / Uptime subset but the rest is available to any frame consumer. Field
// semantics mirror domain.ResourceUsage.
type ResourceUsageFrame struct {
	MemSysBytes       uint64 `json:"mem_sys_bytes"`
	MemSysHuman       string `json:"mem_sys_human"`
	MemHeapAllocBytes uint64 `json:"mem_heap_alloc_bytes"`
	MemHeapAllocHuman string `json:"mem_heap_alloc_human"`

	HeapInuseBytes    uint64 `json:"heap_inuse_bytes"`
	HeapInuseHuman    string `json:"heap_inuse_human"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapIdleHuman     string `json:"heap_idle_human"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapReleasedHuman string `json:"heap_released_human"`
	GCSysBytes        uint64 `json:"gc_sys_bytes"`
	GCSysHuman        string `json:"gc_sys_human"`

	CgroupMemLimitBytes uint64 `json:"cgroup_mem_limit_bytes"`
	CgroupMemLimitHuman string `json:"cgroup_mem_limit_human"`
	CgroupMemUsageBytes uint64 `json:"cgroup_mem_usage_bytes"`
	CgroupMemUsageHuman string `json:"cgroup_mem_usage_human"`

	ConnectionCount int `json:"connection_count"`

	UptimeSeconds int64  `json:"uptime_seconds"`
	UptimeHuman   string `json:"uptime_human"`
	SampledAt     string `json:"sampled_at"` // RFC3339Nano UTC
}

// TrafficHistoryFrame holds a rolling window of per-second traffic samples.
type TrafficHistoryFrame struct {
	IntervalSeconds int                  `json:"interval_seconds"`
	Capacity        int                  `json:"capacity"`
	Count           int                  `json:"count"`
	Samples         []TrafficSampleFrame `json:"samples"`
}

// TrafficSampleFrame is a single data point in the traffic history.
type TrafficSampleFrame struct {
	Timestamp     string `json:"timestamp"`
	BytesSentPS   int64  `json:"bytes_sent_ps"`
	BytesRecvPS   int64  `json:"bytes_recv_ps"`
	TotalSent     int64  `json:"total_sent"`
	TotalReceived int64  `json:"total_received"`
}

type PendingMessageFrame struct {
	ID            string `json:"id"`
	Recipient     string `json:"recipient,omitempty"`
	Status        string `json:"status"`
	QueuedAt      string `json:"queued_at,omitempty"`
	LastAttemptAt string `json:"last_attempt_at,omitempty"`
	Retries       int    `json:"retries,omitempty"`
	Error         string `json:"error,omitempty"`
}

// Connection-notice wire constants.
//
// type="connection_notice" is a universal transport/control frame that
// carries a machine-readable Code plus Details describing why the
// connection is about to be closed or why a handshake decision is being
// forwarded to the caller. Unlike type="error", connection_notice is
// not tied to a specific request/reply interaction and explicitly
// includes Status so callers know the expected transport action.
const (
	// FrameTypeConnectionNotice is the canonical type string for the
	// universal transport/control frame introduced by the advertise-address
	// convergence contract. Currently the only producer is the peer-banned
	// notice path — the v10/v11 observed-address-mismatch consumer was
	// removed in the v12 cleanup phase.
	FrameTypeConnectionNotice = "connection_notice"

	// ConnectionStatusClosing is the only Status value currently shipped
	// with connection_notice: the responder will close the TCP socket
	// immediately after the frame is flushed.
	ConnectionStatusClosing = "closing"
)

// PeerBannedReason is the closed enum of machine-readable reasons the
// responder can cite when sending a peer-banned notice. Kept as a
// distinct string type so stray untyped assignments are a compile error.
// The wire form is a plain JSON string; unknown values decode as-is and
// callers must treat them as opaque — the dialler only needs Until to
// honour the ban, the reason is advisory.
type PeerBannedReason string

const (
	// PeerBannedReasonBlacklisted is the IP-wide blacklist hit, the sole
	// producer today. Emission sits on the addBanScore zero→non-zero
	// transition and rides out on the still-open ConnID so the dialler
	// learns the ban window before teardown.
	PeerBannedReasonBlacklisted PeerBannedReason = "blacklisted"
	// PeerBannedReasonPeerBan is a per-peer timed ban (peerEntry.BannedUntil).
	// Reserved for a future emission path: an inbound connect-time reject
	// against an already-banned peer address, gated on auth since the
	// overlay address is attacker-controlled until then. Dialers must
	// accept and record the window on receipt today so a forthcoming
	// server rollout does not require a concurrent client rollout.
	PeerBannedReasonPeerBan PeerBannedReason = "peer-ban"
	// PeerBannedReasonSelfIdentity is emitted when the responder detects
	// that the inbound hello carries the responder's own Ed25519
	// identity — i.e. the sender is actually the local node reflected
	// back via NAT loopback, a peer-exchange mirror, a fallback-port
	// alias, or an onion-to-clearnet echo. The dialler should treat this
	// as an address-level dead-end and stop redialling the endpoint so
	// the node does not spin in a self-loop.
	PeerBannedReasonSelfIdentity PeerBannedReason = "self-identity"
)

// PeerBannedDetails is the schema for Frame.Details when
// Code == ErrCodePeerBanned. Until is the wall-clock time the ban lifts,
// encoded as UTC RFC3339 so the dialler can parse without ambiguity.
// Reason selects the dialler's persistence SCOPE: `peer-ban` /
// `self-identity` suppress only this PeerAddress, while `blacklisted`
// additionally counts toward an IP-wide escalation that, past a
// distinct-offender threshold, suppresses every sibling behind the
// egress (see node.handlePeerBannedNotice and docs/protocol/errors.md).
// A dialler MUST still honour the ban regardless of reason — the reason
// only widens or narrows the blast radius, it never makes the notice a
// no-op. Fields are omitempty so a minimal notice ({code:"peer-banned"})
// is valid — a dialler that receives no Until falls back to a
// conservative local default. A
// dialler-supplied remote Until is trusted verbatim: if the responder
// declares a ten-year ban, the dialler records ten years. The peer has
// authority over its own refusal window, and argument with that decision
// on the dialler side only wastes connect attempts.
type PeerBannedDetails struct {
	Until  string           `json:"until,omitempty"`
	Reason PeerBannedReason `json:"reason,omitempty"`
}

// MarshalPeerBannedDetails serialises the ban-notice payload into the
// opaque json.RawMessage carried by Frame.Details. Until is written as
// UTC RFC3339. Passing a zero time emits no "until" key, which the
// dialler will treat as "ban with unknown expiration" and apply its
// local default. Returns (nil, nil) when both fields are empty so the
// Details field is omitted entirely by omitempty.
func MarshalPeerBannedDetails(until time.Time, reason PeerBannedReason) (json.RawMessage, error) {
	details := PeerBannedDetails{Reason: reason}
	if !until.IsZero() {
		details.Until = until.UTC().Format(time.RFC3339)
	}
	if details.Until == "" && details.Reason == "" {
		return nil, nil
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// ParsePeerBannedDetails decodes Frame.Details for a connection_notice
// with Code == ErrCodePeerBanned. Returns the decoded struct and, when
// Until was present, the parsed time. A zero time.Time return means the
// notice did not carry an expiration and the caller must fall back to a
// local default. Empty raw input is not an error: the minimal notice
// shape is legal.
func ParsePeerBannedDetails(raw json.RawMessage) (PeerBannedDetails, time.Time, error) {
	if len(raw) == 0 {
		return PeerBannedDetails{}, time.Time{}, nil
	}
	var details PeerBannedDetails
	if err := json.Unmarshal(raw, &details); err != nil {
		return PeerBannedDetails{}, time.Time{}, err
	}
	if details.Until == "" {
		return details, time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, details.Until)
	if err != nil {
		return details, time.Time{}, fmt.Errorf("peer-banned details: invalid until: %w", err)
	}
	return details, parsed.UTC(), nil
}

func IsJSONLine(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}")
}

// maxJSONDepth is the maximum nesting depth allowed in inbound JSON frames.
// The Frame struct has a maximum depth of 3 (top-level → array element →
// nested struct). Anything deeper is either malformed or a deliberate attempt
// to amplify memory allocation in the JSON tokenizer.
const maxJSONDepth = 10

// frameLineBufPool recycles the scratch []byte that backs json.Unmarshal's
// input. Previously ParseFrameLine allocated a fresh []byte(line) copy on every
// inbound frame — on the route-announce churn path (one ParseFrameLine per
// frame line, scaling with route flap volume) that single copy was the largest
// line item in the alloc_space profile. encoding/json never retains a reference
// to its input buffer: every string field is freshly allocated and every
// json.RawMessage field (including AnnounceRouteFrame.Extra) is copied out via
// append, so the scratch buffer is safe to reuse the instant Unmarshal returns.
var frameLineBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

// maxPooledFrameLineCap bounds the capacity we are willing to retain in the
// pool. Command-plane frames are <128 KiB; response-plane lines can legally
// reach MaxResponseLine (8 MiB). Returning an 8 MiB buffer to the pool would
// pin that memory indefinitely, so oversized scratch buffers are dropped on the
// floor (GC reclaims them) instead of being recycled.
const maxPooledFrameLineCap = 128 * 1024

func ParseFrameLine(line string) (Frame, error) {
	if err := checkJSONDepth(line, maxJSONDepth); err != nil {
		return Frame{}, err
	}
	bufp := frameLineBufPool.Get().(*[]byte)
	buf := append((*bufp)[:0], line...)
	var frame Frame
	err := json.Unmarshal(buf, &frame)
	if cap(buf) <= maxPooledFrameLineCap {
		*bufp = buf
		frameLineBufPool.Put(bufp)
	}
	return frame, err
}

// checkJSONDepth scans the raw JSON string for nesting depth (counting { and
// [ as depth increments). Returns an error if the depth exceeds maxDepth.
// This runs in O(n) time with zero allocations — much cheaper than letting
// json.Decoder parse a deeply nested structure.
func checkJSONDepth(data string, maxDepth int) error {
	depth := 0
	inString := false
	escaped := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inString {
			escaped = true
			continue
		}
		if c == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch c {
		case '{', '[':
			depth++
			if depth > maxDepth {
				return fmt.Errorf("JSON nesting depth %d exceeds maximum %d", depth, maxDepth)
			}
		case '}', ']':
			depth--
		}
	}
	return nil
}

// MaxFrameLine is the writer-side wire size budget for a single
// serialized frame line on the COMMAND plane (inbound TCP via
// handleConn / writeFrameToInboundConn). The value is kept in lock-step
// with the receive-side maxCommandLineBytes guard inside admission.go:
// any frame larger than MaxFrameLine will be rejected by the remote
// command-plane reader with errCode=frame-too-large, so the sender MUST
// refuse to put oversize frames on the wire instead of burning a
// connection per offending frame. The budget includes the trailing
// newline byte — see MarshalFrameLineWithLimit for the off-by-one
// rationale. The constant is exported so tests in adjacent packages
// can pin its value without re-deriving the budget.
const MaxFrameLine = 128 * 1024

// MaxResponseLine is the writer-side wire size budget for a single
// serialized frame line on the RESPONSE plane (peer-session writes via
// peerSessionRequest, where the receiver dispatches through the
// readPeerSession line reader bound by maxResponseLineBytes). Response
// frames legally batch many DM bodies into a single line (contacts,
// messages, inbox), so the budget is wider than MaxFrameLine. As with
// MaxFrameLine, the budget includes the trailing newline byte — see
// MarshalFrameLineWithLimit for the off-by-one rationale.
const MaxResponseLine = 8 * 1024 * 1024

// MarshalFrameLine serializes a frame to its wire form without any
// size guard. Callers that need the writer-side wire-size budget — and
// most production callers do — MUST use MarshalFrameLineWithLimit
// instead, picking MaxFrameLine for command-plane writes and
// MaxResponseLine for peer-session writes. This unguarded entry point
// stays in the API for tests, raw-line fast-path callers, and shared
// infrastructure (NetCore.Send / SendSync) where the direction of the
// frame is unknown to the marshaller.
//
// The RawLine fast-path is left untrusted on purpose — its caller
// already owns the wire shape and chose to bypass json.Marshal.
func MarshalFrameLine(frame Frame) (string, error) {
	if frame.RawLine != "" {
		return frame.RawLine, nil
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return "", err
	}
	return string(data) + "\n", nil
}

// MarshalFrameLineWithLimit serializes a frame to its wire form,
// rejecting frames whose final wire line (JSON + trailing newline)
// exceeds maxBytes with ErrFrameTooLarge. Use MaxFrameLine (128 KiB)
// for command-plane writes (inbound TCP via writeFrameToInboundConn)
// and MaxResponseLine (8 MiB) for peer-session writes that legally
// carry batched responses (contacts, messages, inbox).
//
// The size budget is computed against the FULL wire line including
// the trailing newline so it matches the receive-side bufio.Reader
// line counter byte-for-byte: a JSON payload that would emit a wire
// line of exactly maxBytes+1 (JSON length == maxBytes, plus '\n')
// would be rejected by the remote, so the writer-side guard rejects
// it here too.
//
// The RawLine fast-path is also size-checked because callers using
// RawLine still produce bytes that hit the same wire limits — only
// the fast-path skips the JSON marshal step, not the size budget.
func MarshalFrameLineWithLimit(frame Frame, maxBytes int) (string, error) {
	if frame.RawLine != "" {
		if len(frame.RawLine) > maxBytes {
			return "", fmt.Errorf("MarshalFrameLineWithLimit: raw line size %d exceeds %d: %w", len(frame.RawLine), maxBytes, ErrFrameTooLarge)
		}
		return frame.RawLine, nil
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return "", err
	}
	// +1 accounts for the trailing newline appended below — the
	// receive-side line reader counts the newline as part of the
	// line, so a JSON payload with len(data)==maxBytes would produce
	// a maxBytes+1 wire line and be rejected by the remote.
	if len(data)+1 > maxBytes {
		return "", fmt.Errorf("MarshalFrameLineWithLimit: frame size %d (with newline %d) exceeds %d: %w", len(data), len(data)+1, maxBytes, ErrFrameTooLarge)
	}
	return string(data) + "\n", nil
}

type ChatEntryFrame struct {
	ID             string `json:"id"`
	Sender         string `json:"sender"`
	Recipient      string `json:"recipient"`
	Body           string `json:"body"`
	CreatedAt      string `json:"created_at"`
	Flag           string `json:"flag,omitempty"`
	DeliveryStatus string `json:"delivery_status,omitempty"`
	TTLSeconds     int    `json:"ttl_seconds,omitempty"`
	Metadata       string `json:"metadata,omitempty"`
}

type DMHeaderFrame struct {
	ID        string `json:"id"`
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
	CreatedAt string `json:"created_at"`
}

type ChatPreviewFrame struct {
	PeerAddress    string `json:"peer_address"`
	ID             string `json:"id"`
	Sender         string `json:"sender"`
	Recipient      string `json:"recipient"`
	Body           string `json:"body"`
	CreatedAt      string `json:"created_at"`
	Flag           string `json:"flag,omitempty"`
	DeliveryStatus string `json:"delivery_status,omitempty"`
	TTLSeconds     int    `json:"ttl_seconds,omitempty"`
	Metadata       string `json:"metadata,omitempty"`
}

// AnnounceRouteFrame is a single route entry inside an announce_routes frame.
// The wire format carries only the fields needed for distance-vector convergence;
// RemainingTTL and Source are derived locally by the receiver.
//
// Extra preserves any JSON fields beyond the known set (identity, origin,
// hops, seq). When a node re-announces a route learned from a neighbor,
// the Extra blob is forwarded unchanged — ensuring that future protocol
// extensions (e.g. onion box keys) survive transit through older nodes
// that do not yet understand them.
type AnnounceRouteFrame struct {
	Identity string `json:"identity"`
	Origin   string `json:"origin"`
	Hops     int    `json:"hops"`
	SeqNo    uint64 `json:"seq"`

	// Extra holds unknown JSON fields for forward-compatible relay.
	// Nil when no extra fields are present.
	Extra json.RawMessage `json:"-"`
}

// knownRouteFields lists JSON keys that AnnounceRouteFrame handles itself.
// Everything else is captured into Extra for forward-compatible relay.
var knownRouteFields = map[string]struct{}{
	"identity": {},
	"origin":   {},
	"hops":     {},
	"seq":      {},
}

// UnmarshalJSON deserializes an AnnounceRouteFrame, capturing any fields
// beyond the known set into Extra for forward-compatible relay.
func (f *AnnounceRouteFrame) UnmarshalJSON(data []byte) error {
	// Decode the known fields via an alias to avoid infinite recursion.
	type plain AnnounceRouteFrame
	if err := json.Unmarshal(data, (*plain)(f)); err != nil {
		return err
	}

	// Reset Extra before collecting unknown keys. Without this, a reused
	// struct instance would keep stale Extra from a previous Unmarshal call
	// when the new payload has no unknown fields.
	f.Extra = nil

	// Collect all top-level keys and keep the unknown ones.
	var allFields map[string]json.RawMessage
	if err := json.Unmarshal(data, &allFields); err != nil {
		return err
	}

	extra := make(map[string]json.RawMessage, len(allFields))
	for k, v := range allFields {
		if _, known := knownRouteFields[k]; !known {
			extra[k] = v
		}
	}

	if len(extra) > 0 {
		raw, err := json.Marshal(extra)
		if err != nil {
			return err
		}
		f.Extra = raw
	}

	return nil
}

// MarshalJSON serializes an AnnounceRouteFrame, merging any Extra fields
// back into the top-level JSON object alongside the known fields.
func (f AnnounceRouteFrame) MarshalJSON() ([]byte, error) {
	// Start with Extra if present, then overlay known fields on top.
	// Known fields always win if there's a key collision.
	var obj map[string]json.RawMessage
	if len(f.Extra) > 0 {
		if err := json.Unmarshal(f.Extra, &obj); err != nil {
			return nil, fmt.Errorf("AnnounceRouteFrame: malformed Extra: %w", err)
		}
		if obj == nil {
			// Extra was valid JSON but not an object (e.g. "null").
			// Only JSON objects can carry extension fields.
			return nil, fmt.Errorf("AnnounceRouteFrame: Extra is not a JSON object")
		}
	} else {
		obj = make(map[string]json.RawMessage, 4)
	}

	if raw, err := json.Marshal(f.Identity); err == nil {
		obj["identity"] = raw
	}
	if raw, err := json.Marshal(f.Origin); err == nil {
		obj["origin"] = raw
	}
	if raw, err := json.Marshal(f.Hops); err == nil {
		obj["hops"] = raw
	}
	if raw, err := json.Marshal(f.SeqNo); err == nil {
		obj["seq"] = raw
	}

	return json.Marshal(obj)
}

type ConversationFrame struct {
	PeerAddress string `json:"peer_address"`
	LastMessage string `json:"last_message,omitempty"`
	Count       int    `json:"count"`
	UnreadCount int    `json:"unread_count"`
}
