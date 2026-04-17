package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Frame struct {
	Type                   string                `json:"type"`
	Version                int                   `json:"version,omitempty"`
	MinimumProtocolVersion int                   `json:"minimum_protocol_version,omitempty"`
	Client                 string                `json:"client,omitempty"`
	Node                   string                `json:"node,omitempty"`
	Network                string                `json:"network,omitempty"`
	Listen                 string                `json:"listen,omitempty"`
	Listener               string                `json:"listener,omitempty"`
	NodeType               string                `json:"node_type,omitempty"`
	ClientVersion          string                `json:"client_version,omitempty"`
	ClientBuild            int                   `json:"client_build,omitempty"`
	Services               []string              `json:"services,omitempty"`
	Networks               []string              `json:"networks,omitempty"` // self-declared reachable network groups (hello frame)
	Address                string                `json:"address,omitempty"`
	Recipient              string                `json:"recipient,omitempty"`
	PubKey                 string                `json:"pubkey,omitempty"`
	BoxKey                 string                `json:"boxkey,omitempty"`
	BoxSig                 string                `json:"boxsig,omitempty"`
	Peers                  []string              `json:"peers,omitempty"`
	Identities             []string              `json:"identities,omitempty"`
	Contacts               []ContactFrame        `json:"contacts,omitempty"`
	Topic                  string                `json:"topic,omitempty"`
	ID                     string                `json:"id,omitempty"`
	IDs                    []string              `json:"ids,omitempty"`
	PendingIDs             []string              `json:"pending_ids,omitempty"`
	PendingMessages        []PendingMessageFrame `json:"pending_messages,omitempty"`
	Item                   *MessageFrame         `json:"item,omitempty"`
	Receipt                *ReceiptFrame         `json:"receipt,omitempty"`
	Messages               []MessageFrame        `json:"messages,omitempty"`
	Receipts               []ReceiptFrame        `json:"receipts,omitempty"`
	Notices                []NoticeFrame         `json:"notices,omitempty"`
	PeerHealth             []PeerHealthFrame     `json:"peer_health,omitempty"`
	Subscriber             string                `json:"subscriber,omitempty"`
	Flag                   string                `json:"flag,omitempty"`
	CreatedAt              string                `json:"created_at,omitempty"`
	DeliveredAt            string                `json:"delivered_at,omitempty"`
	TTLSeconds             int                   `json:"ttl_seconds,omitempty"`
	Body                   string                `json:"body,omitempty"`
	Ciphertext             string                `json:"ciphertext,omitempty"`
	ExpiresAt              int64                 `json:"expires_at,omitempty"`
	Count                  int                   `json:"count,omitempty"`
	Limit                  int                   `json:"limit,omitempty"`
	Status                 string                `json:"status,omitempty"`
	AckType                string                `json:"ack_type,omitempty"`
	ObservedAddress        string                `json:"observed_address,omitempty"`
	Challenge              string                `json:"challenge,omitempty"`
	Signature              string                `json:"signature,omitempty"`
	Code                   string                `json:"code,omitempty"`
	Error                  string                `json:"error,omitempty"`
	DMHeaders              []DMHeaderFrame       `json:"dm_headers,omitempty"`
	ChatEntries            []ChatEntryFrame      `json:"chat_entries,omitempty"`
	ChatPreviews           []ChatPreviewFrame    `json:"chat_previews,omitempty"`
	Conversations          []ConversationFrame   `json:"conversations,omitempty"`
	NetworkStats           *NetworkStatsFrame    `json:"network_stats,omitempty"`
	AggregateStatus        *AggregateStatusFrame `json:"aggregate_status,omitempty"`
	TrafficHistory         *TrafficHistoryFrame  `json:"traffic_history,omitempty"`
	Capabilities           []string              `json:"capabilities,omitempty"`

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
	LastErrorCode      string `json:"last_error_code,omitempty"`
	LastDisconnectCode string `json:"last_disconnect_code,omitempty"`
	IncompatibleVersionAttempts int   `json:"incompatible_version_attempts,omitempty"`
	LastIncompatibleVersionAt  string `json:"last_incompatible_version_at,omitempty"`
	ObservedPeerVersion        int    `json:"observed_peer_version,omitempty"`
	ObservedPeerMinimumVersion int    `json:"observed_peer_minimum_version,omitempty"`
	VersionLockoutActive       bool   `json:"version_lockout_active,omitempty"`
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
	Status          string `json:"status"`           // offline | reconnecting | limited | warning | healthy
	UsablePeers     int    `json:"usable_peers"`     // healthy + degraded — can route messages
	ConnectedPeers  int    `json:"connected_peers"`  // usable + stalled
	TotalPeers      int    `json:"total_peers"`      // connected + reconnecting
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
	MaxObservedPeerVersion       int    `json:"max_observed_peer_version,omitempty"`
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

func IsJSONLine(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}")
}

// maxJSONDepth is the maximum nesting depth allowed in inbound JSON frames.
// The Frame struct has a maximum depth of 3 (top-level → array element →
// nested struct). Anything deeper is either malformed or a deliberate attempt
// to amplify memory allocation in the JSON tokenizer.
const maxJSONDepth = 10

func ParseFrameLine(line string) (Frame, error) {
	if err := checkJSONDepth(line, maxJSONDepth); err != nil {
		return Frame{}, err
	}
	var frame Frame
	err := json.Unmarshal([]byte(line), &frame)
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
