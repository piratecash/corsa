package protocol

import (
	"encoding/json"
	"strings"
)

type Frame struct {
	Type                   string            `json:"type"`
	Version                int               `json:"version,omitempty"`
	MinimumProtocolVersion int               `json:"minimum_protocol_version,omitempty"`
	Client                 string            `json:"client,omitempty"`
	Node                   string            `json:"node,omitempty"`
	Network                string            `json:"network,omitempty"`
	Listen                 string            `json:"listen,omitempty"`
	Listener               string            `json:"listener,omitempty"`
	NodeType               string            `json:"node_type,omitempty"`
	ClientVersion          string            `json:"client_version,omitempty"`
	Services               []string          `json:"services,omitempty"`
	Address                string            `json:"address,omitempty"`
	Recipient              string            `json:"recipient,omitempty"`
	PubKey                 string            `json:"pubkey,omitempty"`
	BoxKey                 string            `json:"boxkey,omitempty"`
	BoxSig                 string            `json:"boxsig,omitempty"`
	Peers                  []string          `json:"peers,omitempty"`
	Identities             []string          `json:"identities,omitempty"`
	Contacts               []ContactFrame    `json:"contacts,omitempty"`
	Topic                  string            `json:"topic,omitempty"`
	ID                     string            `json:"id,omitempty"`
	IDs                    []string          `json:"ids,omitempty"`
	PendingIDs             []string          `json:"pending_ids,omitempty"`
	Item                   *MessageFrame     `json:"item,omitempty"`
	Receipt                *ReceiptFrame     `json:"receipt,omitempty"`
	Messages               []MessageFrame    `json:"messages,omitempty"`
	Receipts               []ReceiptFrame    `json:"receipts,omitempty"`
	Notices                []NoticeFrame     `json:"notices,omitempty"`
	PeerHealth             []PeerHealthFrame `json:"peer_health,omitempty"`
	Subscriber             string            `json:"subscriber,omitempty"`
	Flag                   string            `json:"flag,omitempty"`
	CreatedAt              string            `json:"created_at,omitempty"`
	DeliveredAt            string            `json:"delivered_at,omitempty"`
	TTLSeconds             int               `json:"ttl_seconds,omitempty"`
	Body                   string            `json:"body,omitempty"`
	Ciphertext             string            `json:"ciphertext,omitempty"`
	ExpiresAt              int64             `json:"expires_at,omitempty"`
	Count                  int               `json:"count,omitempty"`
	Status                 string            `json:"status,omitempty"`
	Code                   string            `json:"code,omitempty"`
	Error                  string            `json:"error,omitempty"`
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
	Address             string `json:"address"`
	State               string `json:"state"`
	Connected           bool   `json:"connected"`
	PendingCount        int    `json:"pending_count,omitempty"`
	LastConnectedAt     string `json:"last_connected_at,omitempty"`
	LastDisconnectedAt  string `json:"last_disconnected_at,omitempty"`
	LastPingAt          string `json:"last_ping_at,omitempty"`
	LastPongAt          string `json:"last_pong_at,omitempty"`
	LastUsefulSendAt    string `json:"last_useful_send_at,omitempty"`
	LastUsefulReceiveAt string `json:"last_useful_receive_at,omitempty"`
	ConsecutiveFailures int    `json:"consecutive_failures,omitempty"`
	LastError           string `json:"last_error,omitempty"`
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
