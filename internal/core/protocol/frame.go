package protocol

import (
	"encoding/json"
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
	DMHeaders              []DMHeaderFrame        `json:"dm_headers,omitempty"`
	ChatEntries            []ChatEntryFrame      `json:"chat_entries,omitempty"`
	ChatPreviews           []ChatPreviewFrame    `json:"chat_previews,omitempty"`
	Conversations          []ConversationFrame   `json:"conversations,omitempty"`
	NetworkStats           *NetworkStatsFrame    `json:"network_stats,omitempty"`
	TrafficHistory         *TrafficHistoryFrame  `json:"traffic_history,omitempty"`
	Capabilities           []string              `json:"capabilities,omitempty"`

	// Relay fields (Iteration 1 — hop-by-hop relay)
	HopCount    int    `json:"hop_count,omitempty"`
	MaxHops     int    `json:"max_hops,omitempty"`
	PreviousHop string `json:"previous_hop,omitempty"`
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
	Network             string   `json:"network,omitempty"`
	Direction           string   `json:"direction,omitempty"`
	ClientVersion       string   `json:"client_version,omitempty"`
	ClientBuild         int      `json:"client_build,omitempty"`
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
	BytesSent           int64    `json:"bytes_sent"`
	BytesReceived       int64    `json:"bytes_received"`
	TotalTraffic        int64    `json:"total_traffic"`
	Capabilities        []string `json:"capabilities,omitempty"`
}

// NetworkStatsFrame provides aggregated traffic statistics for the entire node.
type NetworkStatsFrame struct {
	TotalBytesSent     int64            `json:"total_bytes_sent"`
	TotalBytesReceived int64            `json:"total_bytes_received"`
	TotalTraffic       int64            `json:"total_traffic"`
	ConnectedPeers     int              `json:"connected_peers"`
	KnownPeers         int              `json:"known_peers"`
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

func ParseFrameLine(line string) (Frame, error) {
	var frame Frame
	err := json.Unmarshal([]byte(line), &frame)
	return frame, err
}

func MarshalFrameLine(frame Frame) (string, error) {
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

type ConversationFrame struct {
	PeerAddress string `json:"peer_address"`
	LastMessage string `json:"last_message,omitempty"`
	Count       int    `json:"count"`
	UnreadCount int    `json:"unread_count"`
}
