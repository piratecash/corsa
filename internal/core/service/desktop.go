package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
	"github.com/piratecash/corsa/internal/core/transport"
)

type DesktopClient struct {
	id        *identity.Identity
	appCfg    config.App
	nodeCfg   config.Node
	localNode *node.Service
	chatLog   *chatlog.Store // owned by DesktopClient, not the node
}

type Contact struct {
	BoxKey       string
	PubKey       string
	BoxSignature string
}

type MessageRecord struct {
	ID              string
	Flag            string
	Timestamp       time.Time
	TTLSeconds      int
	Sender          string
	Recipient       string
	Body            string
	PersistedStatus string // delivery_status from SQLite chatlog (may be empty for legacy data)
}

type DeliveryReceipt struct {
	MessageID   string
	Sender      domain.PeerIdentity
	Recipient   domain.PeerIdentity
	Status      string
	DeliveredAt time.Time
}

type PeerHealth struct {
	Address             string
	PeerID              string
	ConnID              uint64
	Direction           string
	ClientVersion       string
	ClientBuild         int
	ProtocolVersion     int
	State               string
	Connected           bool
	PendingCount        int
	LastConnectedAt     *time.Time
	LastDisconnectedAt  *time.Time
	LastPingAt          *time.Time
	LastPongAt          *time.Time
	LastUsefulSendAt    *time.Time
	LastUsefulReceiveAt *time.Time
	ConsecutiveFailures int
	LastError           string
	Score               int
	BannedUntil         *time.Time
	BytesSent           int64
	BytesReceived       int64
	TotalTraffic        int64
}

type DirectMessage struct {
	ID            string
	Sender        domain.PeerIdentity
	Recipient     domain.PeerIdentity
	Body          string
	ReplyTo       domain.MessageID
	Command       domain.FileAction // e.g. FileActionAnnounce for file transfers; empty for regular DMs
	CommandData   string            // JSON-encoded payload (e.g. FileAnnouncePayload); empty for regular DMs
	Timestamp     time.Time
	ReceiptStatus string
	DeliveredAt   *time.Time
}

type DMHeader struct {
	ID        string
	Sender    domain.PeerIdentity
	Recipient domain.PeerIdentity
	Timestamp time.Time
}

type ConversationPreview struct {
	PeerAddress domain.PeerIdentity
	Sender      domain.PeerIdentity
	Body        string
	Timestamp   time.Time
	UnreadCount int // number of incoming messages with delivery_status != 'seen'
}

type PendingMessage struct {
	ID            string
	Recipient     string
	Status        string
	QueuedAt      *time.Time
	LastAttemptAt *time.Time
	Retries       int
	Error         string
}

type ConsolePeerStatus struct {
	Address       string `json:"address"`
	Identity      string `json:"identity,omitempty"`
	ConnID        uint64 `json:"conn_id,omitempty"`
	Network       string `json:"network,omitempty"`
	Direction     string `json:"direction,omitempty"`
	ClientVersion string `json:"client_version"`
	ClientBuild   int    `json:"client_build"`
	State         string `json:"state"`
	Connected     bool   `json:"connected"`
	PendingCount  int    `json:"pending_count,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	BytesSent     int64  `json:"bytes_sent"`
	BytesReceived int64  `json:"bytes_received"`
	TotalTraffic  int64  `json:"total_traffic"`
}

type ConsolePingStatus struct {
	Address   string `json:"address"`
	OK        bool   `json:"ok"`
	Status    string `json:"status"`
	Connected bool   `json:"connected"`
	State     string `json:"state,omitempty"`
	Node      string `json:"node,omitempty"`
	Network   string `json:"network,omitempty"`
	Error     string `json:"error,omitempty"`
}

type NodeStatus struct {
	Address          string
	Connected        bool
	Welcome          string
	NodeID           string
	NodeType         string
	ListenerEnabled  bool
	ListenerAddress  string
	ClientVersion    string
	Services         []string
	Capabilities     []string
	KnownIDs         []string
	Contacts         map[string]Contact
	Peers            []string
	PeerHealth       []PeerHealth
	ReachableIDs     map[domain.PeerIdentity]bool // identity reachable via routing table (at least one live route exists)
	Stored           string
	Messages         []string
	MessageIDs       []string
	DirectMessages   []DirectMessage
	DirectMessageIDs []string
	DMHeaders        []DMHeader
	PendingMessages  []PendingMessage
	DeliveryReceipts []DeliveryReceipt
	Gazeta           []string
	Error            string
	CheckedAt        time.Time
}

func NewDesktopClient(appCfg config.App, nodeCfg config.Node, id *identity.Identity, localNode *node.Service) *DesktopClient {
	store := chatlog.NewStore(nodeCfg.EffectiveChatLogDir(), domain.PeerIdentity(id.Address), domain.ListenAddress(nodeCfg.ListenAddress))

	// Register DesktopClient as the node's MessageStore so the node
	// delegates message persistence to the desktop layer instead of
	// managing its own chatlog.
	c := &DesktopClient{
		id:        id,
		appCfg:    appCfg,
		nodeCfg:   nodeCfg,
		localNode: localNode,
		chatLog:   store,
	}
	if localNode != nil {
		localNode.RegisterMessageStore(c)
	}
	return c
}

// Close releases the chatlog SQLite database. Called at shutdown so WAL
// checkpoint and file handles are released cleanly.
func (c *DesktopClient) Close() error {
	if c.chatLog != nil {
		return c.chatLog.Close()
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
// node.MessageStore implementation — called by the node for
// messages that belong to the local identity.
// ──────────────────────────────────────────────────────────────

func (c *DesktopClient) StoreMessage(envelope protocol.Envelope, isOutgoing bool) node.StoreResult {
	if c.chatLog == nil {
		return node.StoreFailed
	}
	status := chatlog.StatusDelivered
	if isOutgoing {
		status = chatlog.StatusSent
	}
	entry := chatlog.Entry{
		ID:             string(envelope.ID),
		Sender:         envelope.Sender,
		Recipient:      envelope.Recipient,
		Body:           string(envelope.Payload),
		CreatedAt:      envelope.CreatedAt.Format(time.RFC3339Nano),
		Flag:           string(envelope.Flag),
		DeliveryStatus: status,
		TTLSeconds:     envelope.TTLSeconds,
	}
	inserted, err := c.chatLog.AppendReportNew(envelope.Topic, domain.PeerIdentity(c.id.Address), entry)
	if err != nil {
		log.Error().Str("topic", envelope.Topic).Str("id", string(envelope.ID)).Err(err).Msg("chatlog append failed")
		return node.StoreFailed
	}
	if !inserted {
		return node.StoreDuplicate
	}
	return node.StoreInserted
}

func (c *DesktopClient) UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool {
	if c.chatLog == nil {
		return false
	}
	// The receipt sender is the message recipient (confirming delivery/seen).
	// The receipt recipient is the message sender (being notified).
	// The peer in the chatlog is the other party.
	var chatlogPeer domain.PeerIdentity
	if receipt.Sender == c.id.Address {
		chatlogPeer = domain.PeerIdentity(receipt.Recipient)
	} else if receipt.Recipient == c.id.Address {
		chatlogPeer = domain.PeerIdentity(receipt.Sender)
	}
	if chatlogPeer == "" {
		return true // not our message, nothing to update
	}
	if _, err := c.chatLog.UpdateStatus("dm", chatlogPeer, domain.MessageID(receipt.MessageID), receipt.Status); err != nil {
		log.Error().Str("message_id", string(receipt.MessageID)).Str("status", receipt.Status).Err(err).Msg("chatlog update status failed")
		return false
	}
	return true
}

func (c *DesktopClient) NetworkName() string {
	return c.appCfg.Network
}

func (c *DesktopClient) ProfileName() string {
	return c.appCfg.Profile
}

func (c *DesktopClient) AppName() string {
	return c.appCfg.Name
}

func (c *DesktopClient) Language() string {
	return c.appCfg.Language
}

func (c *DesktopClient) Version() string {
	return c.appCfg.Version
}

func (c *DesktopClient) ListenAddress() string {
	return c.nodeCfg.ListenAddress
}

func (c *DesktopClient) Address() domain.PeerIdentity {
	return domain.PeerIdentity(c.id.Address)
}

// TransmitDir returns the absolute path to the directory where files awaiting
// transfer are stored. Derived from the node data directory.
func (c *DesktopClient) TransmitDir() string {
	return filepath.Join(c.nodeCfg.EffectiveDataDir(), domain.TransmitSubdir)
}

// DeletePeerHistory removes all chat messages for the given identity from the
// local chatlog database.
func (c *DesktopClient) DeletePeerHistory(identity domain.PeerIdentity) (int64, error) {
	if c.chatLog == nil {
		return 0, nil
	}
	return c.chatLog.DeleteByPeer(identity)
}

// DeleteContact removes a trusted contact from the node's trust store.
// The contact will no longer appear in Contacts on the next ProbeNode cycle.
func (c *DesktopClient) DeleteContact(identity domain.PeerIdentity) error {
	_, err := c.localRequestFrame(protocol.Frame{
		Type:    "delete_trusted_contact",
		Address: string(identity),
	})
	return err
}

func (c *DesktopClient) BootstrapPeers() []transport.Peer {
	peers := make([]transport.Peer, 0, len(c.nodeCfg.BootstrapPeers))
	for _, addr := range c.nodeCfg.BootstrapPeers {
		peers = append(peers, transport.Peer{
			Address: domain.PeerAddress(addr),
			Source:  domain.PeerSourceBootstrap,
		})
	}
	return peers
}

func (c *DesktopClient) SubscribeLocalChanges() (<-chan protocol.LocalChangeEvent, func()) {
	if c.localNode == nil {
		ch := make(chan protocol.LocalChangeEvent)
		close(ch)
		return ch, func() {}
	}
	return c.localNode.SubscribeLocalChanges()
}

func (c *DesktopClient) ProbeNode(ctx context.Context) NodeStatus {
	status := NodeStatus{
		Address: c.localAddress(),
	}

	if status.Address == "" {
		status.Error = "no target address configured"
		status.CheckedAt = time.Now()
		return status
	}

	welcome, err := c.localRequestFrame(protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "desktop",
		ClientVersion: strings.ReplaceAll(c.appCfg.Version, " ", "-"),
		ClientBuild:   config.ClientBuild,
	})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	status.Welcome = welcome.Type
	status.NodeID = welcome.Address
	status.NodeType = welcome.NodeType
	status.ListenerEnabled = strings.TrimSpace(welcome.Listener) == "1"
	status.ListenerAddress = strings.TrimSpace(welcome.Listen)
	status.ClientVersion = welcome.ClientVersion
	status.Services = welcome.Services
	status.Capabilities = welcome.Capabilities

	peersReply, err := c.localRequestFrame(protocol.Frame{Type: "get_peers"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	idsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_identities"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	contactsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	peerHealthReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_peer_health"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	pendingReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_pending_messages", Topic: "dm"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	messagesReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	dmHeadersReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_dm_headers"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	messageIDsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_message_ids", Topic: "global"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	directMessageIDsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_message_ids", Topic: "dm"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	receiptsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_delivery_receipts", Recipient: c.id.Address})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	noticesReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_notices"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	peers := peersReply.Peers
	ids := idsReply.Identities
	contacts := contactsFromFrame(contactsReply)
	messages := messageRecordsFromFrames(messagesReply.Messages)
	dmHeaders := dmHeadersFromFrame(dmHeadersReply)
	messageIDs := messageIDsReply.IDs
	directMessageIDs := directMessageIDsReply.IDs
	deliveryReceipts := receiptRecordsFromFrames(receiptsReply.Receipts)
	notices := decryptNoticeFrames(c.id, noticesReply.Notices)

	// Check for missing contacts from DM headers (lightweight, no decryption needed).
	if missing := missingDMHeaderContacts(c.id.Address, contacts, dmHeaders); len(missing) > 0 {
		refreshedContactsReply, refreshErr := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
		if refreshErr == nil {
			networkContacts := contactsFromFrame(refreshedContactsReply)
			// Auto-import new contacts from DM headers.
			if imported := c.importIncomingDMHeaderContacts(contacts, networkContacts, dmHeaders); imported > 0 {
				trustedContactsReply, trustedErr := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
				if trustedErr == nil {
					contacts = contactsFromFrame(trustedContactsReply)
				}
			}
		}
	}

	pendingMessages := pendingMessagesFromFrame(pendingReply)

	status.Connected = true
	status.KnownIDs = ids
	status.Contacts = contacts
	status.Peers = peers
	status.PeerHealth = peerHealthFromFrame(peerHealthReply)
	status.ReachableIDs = c.buildReachableIDs()
	status.Messages = stringifyMessages(messages)
	status.MessageIDs = messageIDs
	status.DirectMessages = nil // no longer loaded in ProbeNode; use FetchConversation on demand
	status.DirectMessageIDs = directMessageIDs
	status.DMHeaders = dmHeaders
	status.PendingMessages = pendingMessages
	status.DeliveryReceipts = deliveryReceipts
	status.Gazeta = notices
	status.CheckedAt = time.Now()
	return status
}

// buildReachableIDs returns a set of identities that have at least one live
// route in the routing table. The set is built from the snapshot itself (all
// routable destinations), not from a caller-supplied list — this ensures
// sidebar peers that came from chatlog or DM headers are covered too.
//
// Embedded mode: direct localNode.RoutingSnapshot() call.
// Remote TCP mode: falls back to fetch_reachable_ids RPC frame.
func (c *DesktopClient) buildReachableIDs() map[domain.PeerIdentity]bool {
	if c.localNode != nil {
		return reachableFromSnapshot(c.localNode.RoutingSnapshot())
	}
	reply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_reachable_ids"})
	if err != nil || reply.Type == "error" {
		return nil
	}
	reachable := make(map[domain.PeerIdentity]bool, len(reply.Identities))
	for _, id := range reply.Identities {
		reachable[domain.PeerIdentity(id)] = true
	}
	return reachable
}

// reachableFromSnapshot extracts identities with at least one live route.
// The synthetic local self-route (RouteSourceLocal) is excluded because
// reachability is about remote peers, not the node itself.
func reachableFromSnapshot(snap routing.Snapshot) map[domain.PeerIdentity]bool {
	reachable := make(map[domain.PeerIdentity]bool)
	for id := range snap.Routes {
		best := snap.BestRoute(id)
		if best != nil && best.Source != routing.RouteSourceLocal {
			reachable[id] = true
		}
	}
	return reachable
}

func peerHealthFromFrame(frame protocol.Frame) []PeerHealth {
	items := make([]PeerHealth, 0, len(frame.PeerHealth))
	for _, item := range frame.PeerHealth {
		items = append(items, PeerHealth{
			Address:             item.Address,
			PeerID:              item.PeerID,
			ConnID:              item.ConnID,
			Direction:           item.Direction,
			ClientVersion:       item.ClientVersion,
			ClientBuild:         item.ClientBuild,
			ProtocolVersion:     item.ProtocolVersion,
			State:               item.State,
			Connected:           item.Connected,
			PendingCount:        item.PendingCount,
			LastConnectedAt:     parseOptionalTime(item.LastConnectedAt),
			LastDisconnectedAt:  parseOptionalTime(item.LastDisconnectedAt),
			LastPingAt:          parseOptionalTime(item.LastPingAt),
			LastPongAt:          parseOptionalTime(item.LastPongAt),
			LastUsefulSendAt:    parseOptionalTime(item.LastUsefulSendAt),
			LastUsefulReceiveAt: parseOptionalTime(item.LastUsefulReceiveAt),
			ConsecutiveFailures: item.ConsecutiveFailures,
			LastError:           item.LastError,
			Score:               item.Score,
			BannedUntil:         parseOptionalTime(item.BannedUntil),
			BytesSent:           item.BytesSent,
			BytesReceived:       item.BytesReceived,
			TotalTraffic:        item.TotalTraffic,
		})
	}
	return items
}

func parseOptionalTime(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	return &ts
}

func incomingContactsToTrust(self string, trustedContacts, decryptContacts map[string]Contact, messages []DirectMessage) []protocol.ContactFrame {
	toImport := make(map[string]protocol.ContactFrame)

	for _, message := range messages {
		if message.Recipient != domain.PeerIdentity(self) || message.Sender == domain.PeerIdentity(self) {
			continue
		}
		if _, ok := trustedContacts[string(message.Sender)]; ok {
			continue
		}
		contact, ok := decryptContacts[string(message.Sender)]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[string(message.Sender)] = protocol.ContactFrame{
			Address: string(message.Sender),
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}
	}

	contacts := make([]protocol.ContactFrame, 0, len(toImport))
	addresses := make([]string, 0, len(toImport))
	for address := range toImport {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		contacts = append(contacts, toImport[address])
	}
	return contacts
}

func (c *DesktopClient) FetchMessageIDs(ctx context.Context, topic string) ([]string, error) {
	frame, err := c.localRequestFrame(protocol.Frame{Type: "fetch_message_ids", Topic: strings.TrimSpace(topic)})
	if err != nil {
		return nil, err
	}
	return frame.IDs, nil
}

func (c *DesktopClient) FetchMessage(ctx context.Context, topic, messageID string) (MessageRecord, error) {
	frame, err := c.localRequestFrame(protocol.Frame{Type: "fetch_message", Topic: strings.TrimSpace(topic), ID: strings.TrimSpace(messageID)})
	if err != nil {
		return MessageRecord{}, err
	}
	if frame.Item == nil {
		return MessageRecord{}, fmt.Errorf("message item is missing")
	}
	return messageRecordFromFrame(*frame.Item)
}

func (c *DesktopClient) ExecuteConsoleCommand(input string) (string, error) {
	frame, inlineOutput, err := parseConsoleCommand(input, c.id.Address, c.appCfg.Version)
	if err != nil {
		return "", err
	}
	if inlineOutput != "" {
		return inlineOutput, nil
	}
	if frame.Type == "ping" {
		return c.consolePingJSON()
	}
	if frame.Type == "get_peers" {
		return c.consolePeersJSON()
	}

	// Chatlog commands are handled directly by DesktopClient (owns chatlog.Store).
	// These frame types were removed from node.HandleLocalFrame after the chatlog
	// ownership refactor; routing them through localRequestFrame would return
	// "unknown command".
	switch frame.Type {
	case "fetch_chatlog":
		return c.consoleFetchChatlog(frame.Topic, frame.Address)
	case "fetch_chatlog_previews":
		return c.consoleFetchChatlogPreviews()
	case "fetch_conversations":
		return c.consoleFetchConversations()
	}

	reply, err := c.localRequestFrame(frame)
	if err != nil {
		return "", err
	}

	data, err := json.MarshalIndent(reply, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format console response: %w", err)
	}
	return string(data), nil
}

// consoleFetchChatlog reads chat entries for a peer directly from the local chatlog.
func (c *DesktopClient) consoleFetchChatlog(topic, peerAddress string) (string, error) {
	if c.chatLog == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	if topic == "" {
		topic = "dm"
	}
	entries, err := c.chatLog.Read(topic, domain.PeerIdentity(peerAddress))
	if err != nil {
		return "", fmt.Errorf("chatlog read: %w", err)
	}
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format chatlog entries: %w", err)
	}
	return string(data), nil
}

// consoleFetchChatlogPreviews reads the last entry per peer from the local chatlog.
func (c *DesktopClient) consoleFetchChatlogPreviews() (string, error) {
	if c.chatLog == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	previews, err := c.chatLog.ReadLastEntryPerPeer()
	if err != nil {
		return "", fmt.Errorf("chatlog previews: %w", err)
	}
	data, err := json.MarshalIndent(previews, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format chatlog previews: %w", err)
	}
	return string(data), nil
}

// consoleFetchConversations lists all conversations with message counts from the local chatlog.
func (c *DesktopClient) consoleFetchConversations() (string, error) {
	if c.chatLog == nil {
		return "", fmt.Errorf("chatlog not available")
	}
	conversations, err := c.chatLog.ListConversations()
	if err != nil {
		return "", fmt.Errorf("chatlog conversations: %w", err)
	}
	data, err := json.MarshalIndent(conversations, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format conversations: %w", err)
	}
	return string(data), nil
}

// FetchChatlog reads chat entries for a peer from the local chatlog.
func (c *DesktopClient) FetchChatlog(topic, peerAddress string) (string, error) {
	return c.consoleFetchChatlog(topic, peerAddress)
}

// FetchChatlogPreviews reads the last entry per peer from the local chatlog.
func (c *DesktopClient) FetchChatlogPreviews() (string, error) {
	return c.consoleFetchChatlogPreviews()
}

// FetchConversations lists all conversations with message counts.
func (c *DesktopClient) FetchConversations() (string, error) {
	return c.consoleFetchConversations()
}

// HasEntryInConversation checks whether a message with the given ID exists
// in the conversation with peerAddress. Returns false when chatlog is not
// available (standalone node mode).
func (c *DesktopClient) HasEntryInConversation(peerAddress, messageID string) bool {
	if c.chatLog == nil {
		return false
	}
	return c.chatLog.HasEntryInConversation(domain.PeerIdentity(peerAddress), domain.MessageID(messageID))
}

// ConsolePingJSON pings every connected peer over TCP and returns a
// structured JSON report. Implements rpc.DiagnosticProvider.
func (c *DesktopClient) ConsolePingJSON() (string, error) {
	return c.consolePingJSON()
}

// ConsolePeersJSON merges the raw peer list with peer health data and
// categorizes peers into connected/pending/known_only groups.
// Implements rpc.DiagnosticProvider.
func (c *DesktopClient) ConsolePeersJSON() (string, error) {
	return c.consolePeersJSON()
}

// DesktopVersion returns the desktop application version.
// Implements rpc.DiagnosticProvider.
func (c *DesktopClient) DesktopVersion() string {
	return c.appCfg.Version
}

func (c *DesktopClient) consolePeersJSON() (string, error) {
	peersReply, err := c.localRequestFrame(protocol.Frame{Type: "get_peers"})
	if err != nil {
		return "", err
	}
	peerHealthReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_peer_health"})
	if err != nil {
		return "", err
	}
	payload := buildConsolePeersPayload(peersReply.Peers, peerHealthFromFrame(peerHealthReply))

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format get_peers response: %w", err)
	}
	return string(data), nil
}

func (c *DesktopClient) consolePingJSON() (string, error) {
	peerHealthReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_peer_health"})
	if err != nil {
		return "", err
	}
	health := peerHealthFromFrame(peerHealthReply)

	connectedPeers := make([]PeerHealth, 0, len(health))
	for _, item := range health {
		if item.Connected {
			connectedPeers = append(connectedPeers, item)
		}
	}

	results := make([]ConsolePingStatus, 0, len(connectedPeers))
	okCount := 0
	for _, item := range connectedPeers {
		address := strings.TrimSpace(item.Address)
		if address == "" {
			continue
		}

		result := ConsolePingStatus{
			Address:   address,
			Status:    "not_ok",
			Connected: item.Connected,
			State:     item.State,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, reader, _, err := c.openSessionAt(ctx, address, "desktop")
		cancel()
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		reply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "ping"})
		_ = conn.Close()
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}

		result.OK = reply.Type == "pong"
		if result.OK {
			result.Status = "ok"
			result.Node = reply.Node
			result.Network = reply.Network
			okCount++
		} else {
			result.Error = "unexpected ping reply: " + reply.Type
		}
		results = append(results, result)
	}

	payload := struct {
		Type    string              `json:"type"`
		Count   int                 `json:"count"`
		Total   int                 `json:"total"`
		Results []ConsolePingStatus `json:"results"`
	}{
		Type:    "ping",
		Count:   okCount,
		Total:   len(connectedPeers),
		Results: results,
	}

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format ping response: %w", err)
	}
	return string(data), nil
}

func buildConsolePeersPayload(peers []string, health []PeerHealth) any {
	byAddress := make(map[string]PeerHealth, len(health))
	for _, item := range health {
		byAddress[strings.TrimSpace(item.Address)] = item
	}

	allPeers := make([]ConsolePeerStatus, 0, len(peers)+len(health))
	connected := make([]ConsolePeerStatus, 0, len(peers))
	knownWithState := make([]ConsolePeerStatus, 0, len(health))
	knownOnly := make([]string, 0, len(peers))

	// Track addresses we've already processed so we can pick up
	// health-only peers (e.g. inbound-only connections) in a second pass.
	seen := make(map[string]struct{}, len(peers))

	for _, address := range peers {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		seen[address] = struct{}{}
		item, ok := byAddress[address]
		if !ok {
			knownOnly = append(knownOnly, address)
			allPeers = append(allPeers, ConsolePeerStatus{Address: address, Network: node.ClassifyAddress(address).String(), State: "known"})
			continue
		}

		status := ConsolePeerStatus{
			Address:       address,
			Identity:      item.PeerID,
			ConnID:        item.ConnID,
			Network:       node.ClassifyAddress(address).String(),
			Direction:     item.Direction,
			ClientVersion: item.ClientVersion,
			ClientBuild:   item.ClientBuild,
			State:         "known",
			Connected:     item.Connected,
			PendingCount:  item.PendingCount,
			LastError:     item.LastError,
			BytesSent:     item.BytesSent,
			BytesReceived: item.BytesReceived,
			TotalTraffic:  item.TotalTraffic,
		}
		if strings.TrimSpace(item.State) != "" {
			status.State = item.State
		}
		allPeers = append(allPeers, status)
		if item.Connected {
			connected = append(connected, status)
		} else {
			knownWithState = append(knownWithState, status)
		}
	}

	// Second pass: include health-only peers whose address is not in the
	// peers[] list (e.g. inbound-only connections that appear in
	// fetch_peer_health but not in the configured peer list).
	// Collect and sort addresses for deterministic output order.
	healthOnlyAddrs := make([]string, 0, len(byAddress))
	for addr := range byAddress {
		if _, ok := seen[addr]; !ok {
			healthOnlyAddrs = append(healthOnlyAddrs, addr)
		}
	}
	sort.Strings(healthOnlyAddrs)

	for _, addr := range healthOnlyAddrs {
		item := byAddress[addr]
		status := ConsolePeerStatus{
			Address:       addr,
			Identity:      item.PeerID,
			ConnID:        item.ConnID,
			Network:       node.ClassifyAddress(addr).String(),
			Direction:     item.Direction,
			ClientVersion: item.ClientVersion,
			ClientBuild:   item.ClientBuild,
			State:         item.State,
			Connected:     item.Connected,
			PendingCount:  item.PendingCount,
			LastError:     item.LastError,
			BytesSent:     item.BytesSent,
			BytesReceived: item.BytesReceived,
			TotalTraffic:  item.TotalTraffic,
		}
		allPeers = append(allPeers, status)
		if item.Connected {
			connected = append(connected, status)
		} else {
			knownWithState = append(knownWithState, status)
		}
	}

	return struct {
		Type      string              `json:"type"`
		Count     int                 `json:"count"`
		Total     int                 `json:"total"`
		Connected []ConsolePeerStatus `json:"connected,omitempty"`
		Pending   []ConsolePeerStatus `json:"pending,omitempty"`
		KnownOnly []string            `json:"known_only,omitempty"`
		Peers     []ConsolePeerStatus `json:"peers"`
	}{
		Type:      "peers",
		Count:     len(connected),
		Total:     len(allPeers),
		Connected: connected,
		Pending:   knownWithState,
		KnownOnly: knownOnly,
		Peers:     allPeers,
	}
}

func parseConsoleCommand(input, selfAddress, clientVersion string) (protocol.Frame, string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return protocol.Frame{}, "", fmt.Errorf("console command is empty")
	}

	if protocol.IsJSONLine(trimmed) {
		frame, err := protocol.ParseFrameLine(trimmed)
		if err != nil {
			return protocol.Frame{}, "", err
		}
		if strings.TrimSpace(frame.Type) == "" {
			return protocol.Frame{}, "", fmt.Errorf("console command type is required")
		}
		return frame, "", nil
	}

	fields := strings.Fields(trimmed)
	command := strings.ToLower(fields[0])

	switch command {
	case "help":
		return protocol.Frame{}, consoleHelpText(selfAddress), nil
	case "ping":
		return protocol.Frame{Type: "ping"}, "", nil
	case "hello":
		return protocol.Frame{
			Type:          "hello",
			Version:       config.ProtocolVersion,
			Client:        "desktop",
			ClientVersion: strings.ReplaceAll(clientVersion, " ", "-"),
			ClientBuild:   config.ClientBuild,
		}, "", nil
	case "add_peer":
		if len(fields) < 2 {
			return protocol.Frame{}, "", fmt.Errorf("usage: add_peer <host:port>")
		}
		return protocol.Frame{Type: "add_peer", Peers: []string{fields[1]}}, "", nil
	case "fetch_chatlog":
		topic := commandArg(fields, 1, "dm")
		addr := commandArg(fields, 2, "")
		return protocol.Frame{Type: "fetch_chatlog", Topic: topic, Address: addr}, "", nil
	case "fetch_conversations":
		return protocol.Frame{Type: "fetch_conversations"}, "", nil
	case "fetch_chatlog_previews":
		return protocol.Frame{Type: "fetch_chatlog_previews"}, "", nil
	case "fetch_dm_headers":
		return protocol.Frame{Type: "fetch_dm_headers"}, "", nil
	case "get_peers", "fetch_identities", "fetch_contacts", "fetch_trusted_contacts", "fetch_peer_health", "fetch_notices":
		return protocol.Frame{Type: command}, "", nil
	case "fetch_pending_messages":
		return protocol.Frame{Type: command, Topic: commandArg(fields, 1, "dm")}, "", nil
	case "fetch_messages", "fetch_message_ids":
		return protocol.Frame{Type: command, Topic: commandArg(fields, 1, "global")}, "", nil
	case "fetch_message":
		if len(fields) < 3 {
			return protocol.Frame{}, "", fmt.Errorf("usage: fetch_message <topic> <id>")
		}
		return protocol.Frame{Type: command, Topic: fields[1], ID: strings.Join(fields[2:], " ")}, "", nil
	case "fetch_inbox":
		return protocol.Frame{
			Type:      command,
			Topic:     commandArg(fields, 1, "dm"),
			Recipient: commandArg(fields, 2, selfAddress),
		}, "", nil
	case "fetch_delivery_receipts":
		return protocol.Frame{Type: command, Recipient: commandArg(fields, 1, selfAddress)}, "", nil
	default:
		return protocol.Frame{}, "", fmt.Errorf("unknown console command: %s", fields[0])
	}
}

func commandArg(fields []string, index int, fallback string) string {
	if len(fields) <= index {
		return fallback
	}
	return strings.TrimSpace(fields[index])
}

func consoleHelpText(selfAddress string) string {
	return strings.Join([]string{
		"== Control ==",
		"help",
		"ping",
		"hello",
		"",
		"== Network ==",
		"add_peer <host:port>",
		"get_peers",
		"fetch_peer_health",
		"",
		"== Identity & Contacts ==",
		"fetch_identities",
		"fetch_contacts",
		"fetch_trusted_contacts",
		"",
		"== Messages ==",
		"fetch_pending_messages [topic]",
		"fetch_messages [topic]",
		"fetch_message_ids [topic]",
		"fetch_message <topic> <id>",
		"fetch_inbox <topic> [recipient]",
		"fetch_delivery_receipts [recipient]",
		"",
		"== Chat History ==",
		"fetch_chatlog [topic] <peer_address>",
		"fetch_chatlog_previews",
		"fetch_dm_headers",
		"fetch_conversations",
		"",
		"== Notices ==",
		"fetch_notices",
		"",
		"Defaults:",
		"  topic for fetch_messages/fetch_message_ids: global",
		"  topic for fetch_pending_messages/fetch_inbox: dm",
		"  recipient: " + selfAddress,
		"",
		"You can also paste a raw JSON frame for any registered command.",
	}, "\n")
}

func (c *DesktopClient) SyncDirectMessagesFromPeers(ctx context.Context, peerAddresses []string, counterparty string) (int, error) {
	counterparty = strings.TrimSpace(counterparty)
	if counterparty == "" {
		return 0, fmt.Errorf("counterparty is required")
	}

	imported := 0
	seenIDs := make(map[string]struct{})
	var firstErr error
	me := c.id.Address

	for _, peerAddress := range peerAddresses {
		peerAddress = strings.TrimSpace(peerAddress)
		if peerAddress == "" || peerAddress == c.localAddress() {
			continue
		}

		remoteConn, remoteReader, _, err := c.openSessionAt(ctx, peerAddress, "desktop")
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		idsFrame, err := c.requestFrame(remoteConn, remoteReader, protocol.Frame{
			Type:  "fetch_message_ids",
			Topic: "dm",
		})
		if err != nil {
			_ = remoteConn.Close()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		for _, id := range idsFrame.IDs {
			if _, ok := seenIDs[id]; ok {
				continue
			}
			seenIDs[id] = struct{}{}

			messageFrame, err := c.requestFrame(remoteConn, remoteReader, protocol.Frame{
				Type:  "fetch_message",
				Topic: "dm",
				ID:    id,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if messageFrame.Item == nil {
				continue
			}

			item := messageFrame.Item
			if !isConversationMessage(*item, me, counterparty) {
				continue
			}

			reply, err := c.localRequestFrame(protocol.Frame{
				Type:       "import_message",
				Topic:      "dm",
				ID:         item.ID,
				Address:    item.Sender,
				Recipient:  item.Recipient,
				Flag:       item.Flag,
				CreatedAt:  item.CreatedAt,
				TTLSeconds: item.TTLSeconds,
				Body:       item.Body,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if reply.Type == "message_stored" {
				imported++
			}
		}

		_ = remoteConn.Close()
	}

	if imported == 0 && firstErr != nil {
		return 0, firstErr
	}
	return imported, nil
}

func (c *DesktopClient) SendDirectMessage(ctx context.Context, to domain.PeerIdentity, msg domain.OutgoingDM) (*DirectMessage, error) {
	to = domain.PeerIdentity(strings.TrimSpace(string(to)))
	msg.Body = strings.TrimSpace(msg.Body)
	msg.ReplyTo = domain.MessageID(strings.TrimSpace(string(msg.ReplyTo)))
	if to == "" || msg.Body == "" {
		return nil, fmt.Errorf("recipient and message are required")
	}
	if !msg.ReplyTo.IsValidOrEmpty() {
		return nil, fmt.Errorf("reply_to must be a valid message ID (UUID v4)")
	}

	if msg.ReplyTo != "" && c.chatLog != nil {
		if !c.chatLog.HasEntryInConversation(to, domain.MessageID(msg.ReplyTo)) {
			return nil, fmt.Errorf("reply_to message %q not found in conversation with %s", msg.ReplyTo, to)
		}
	}

	contact, err := c.ensureRecipientContact(ctx, string(to))
	if err != nil {
		return nil, err
	}

	recipient := domain.DMRecipient{
		Address:      to,
		BoxKeyBase64: contact.BoxKey,
	}

	ciphertext, err := directmsg.EncryptForParticipants(c.id, recipient, msg)
	if err != nil {
		return nil, err
	}

	messageID, err := protocol.NewMessageID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339)
	reply, err := c.localRequestFrame(protocol.Frame{
		Type:       "send_message",
		Topic:      "dm",
		ID:         string(messageID),
		Address:    c.id.Address,
		Recipient:  string(to),
		Flag:       string(protocol.MessageFlagSenderDelete),
		CreatedAt:  createdAt,
		TTLSeconds: 0,
		Body:       ciphertext,
	})
	if err != nil {
		return nil, err
	}

	if reply.Type != "message_stored" && reply.Type != "message_known" {
		return nil, fmt.Errorf("unexpected send reply: %s", reply.Type)
	}

	result := &DirectMessage{
		ID:            string(messageID),
		Sender:        domain.PeerIdentity(c.id.Address),
		Recipient:     to,
		Body:          msg.Body, // plaintext — already known to us
		ReplyTo:       msg.ReplyTo,
		Command:       msg.Command,
		CommandData:   msg.CommandData,
		Timestamp:     now,
		ReceiptStatus: "sent",
	}

	return result, nil
}

// StoreFileForTransmit copies the source file into the transmit directory
// through FileStore, which handles content-addressed dedup and ref counting.
// Returns the SHA-256 hash of the file content.
func (c *DesktopClient) StoreFileForTransmit(sourcePath string) (string, error) {
	if c.localNode == nil {
		return "", fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.StoreFileForTransmit(sourcePath)
}

// TransmitFileSize returns the byte size of the stored transmit blob.
// The size comes from the persisted copy, not from the original source file.
func (c *DesktopClient) TransmitFileSize(fileHash string) (uint64, error) {
	if c.localNode == nil {
		return 0, fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.TransmitFileSize(fileHash)
}

// RemoveUnreferencedTransmitFile deletes the transmit blob for the given hash
// if no active sender mapping or pending reservation protects it. Used to
// clean up after EnsureStored when PrepareFileAnnounce fails before creating
// a token.
func (c *DesktopClient) RemoveUnreferencedTransmitFile(fileHash string) {
	if c.localNode == nil {
		return
	}
	c.localNode.RemoveUnreferencedTransmitFile(fileHash)
}

// PrepareFileAnnounce atomically validates transmit file availability and
// reserves a sender quota slot. Returns a token that the caller uses to
// either Commit (after the DM is sent successfully) or Rollback (on any
// failure). This encapsulates the entire transmit-file and sender-mapping
// lifecycle so callers (dm_router) only deal with sending the DM.
func (c *DesktopClient) PrepareFileAnnounce(
	fileHash, fileName, contentType string,
	fileSize uint64,
) (*filetransfer.SenderAnnounceToken, error) {
	if c.localNode == nil {
		return nil, fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.PrepareFileAnnounce(fileHash, fileName, contentType, fileSize)
}

// RegisterIncomingFileTransfer registers a receiver-side file mapping.
// Called when a file_announce DM is decrypted from a remote peer.
// Returns an error if the announce metadata is invalid.
func (c *DesktopClient) RegisterIncomingFileTransfer(
	fileID domain.FileID,
	fileHash, fileName, contentType string,
	fileSize uint64,
	sender domain.PeerIdentity,
) error {
	if c.localNode == nil {
		return fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.RegisterIncomingFileTransfer(fileID, fileHash, fileName, contentType, fileSize, sender)
}

// CancelFileDownload aborts an active download and resets the mapping.
func (c *DesktopClient) CancelFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.CancelFileDownload(fileID)
}

// StartFileDownload begins downloading a previously registered incoming file.
func (c *DesktopClient) StartFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.StartFileDownload(fileID)
}

// RestartFileDownload resets a failed download back to available state.
func (c *DesktopClient) RestartFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return fmt.Errorf("no local node (embedded mode required)")
	}
	return c.localNode.RestartFileDownload(fileID)
}

// FileTransferProgress returns the transfer progress for a given file.
// isSender=true queries the sender mapping; false queries the receiver mapping.
func (c *DesktopClient) FileTransferProgress(
	fileID domain.FileID,
	isSender bool,
) (bytesTransferred, totalSize uint64, state string, found bool) {
	if c.localNode == nil {
		return 0, 0, "", false
	}
	return c.localNode.FileTransferProgress(fileID, isSender)
}

// FileTransferFilePath returns the on-disk path for a transferred file.
// For the sender it resolves the transmit blob; for the receiver it returns
// the completed download path. Returns empty string if unavailable.
func (c *DesktopClient) FileTransferFilePath(fileID domain.FileID, isSender bool) string {
	if c.localNode == nil {
		return ""
	}
	return c.localNode.FileTransferFilePath(fileID, isSender)
}

// CleanupPeerTransfers removes all file transfer mappings and files associated
// with the given peer identity.
func (c *DesktopClient) CleanupPeerTransfers(peer domain.PeerIdentity) {
	if c.localNode == nil {
		return
	}
	c.localNode.CleanupPeerTransfers(peer)
}

// RemoveSenderMapping removes a single sender mapping by fileID, releasing
// the transmit file ref if the mapping is not in a terminal state. Returns
// true if the mapping existed and was removed.
func (c *DesktopClient) RemoveSenderMapping(fileID domain.FileID) bool {
	if c.localNode == nil {
		return false
	}
	return c.localNode.RemoveSenderMapping(fileID)
}

func (c *DesktopClient) ensureRecipientContact(ctx context.Context, recipient string) (Contact, error) {
	trustedReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts := contactsFromFrame(trustedReply)
	if contact, ok := trustedContacts[recipient]; ok && contact.BoxKey != "" {
		return contact, nil
	}

	networkReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		return Contact{}, err
	}
	networkContacts := contactsFromFrame(networkReply)
	contact, ok := networkContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, fmt.Errorf("recipient box key is unknown")
	}
	if contact.PubKey == "" || contact.BoxSignature == "" {
		return Contact{}, fmt.Errorf("recipient trust data is incomplete")
	}

	importReply, err := c.localRequestFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: recipient,
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}},
	})
	if err != nil {
		return Contact{}, err
	}
	if importReply.Type != "contacts_imported" {
		return Contact{}, fmt.Errorf("unexpected contacts import reply: %s", importReply.Type)
	}

	trustedReply, err = c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts = contactsFromFrame(trustedReply)
	contact, ok = trustedContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, fmt.Errorf("recipient box key is unknown")
	}
	return contact, nil
}

func (c *DesktopClient) MarkConversationSeen(ctx context.Context, counterparty domain.PeerIdentity, messages []DirectMessage) error {
	counterparty = domain.PeerIdentity(strings.TrimSpace(string(counterparty)))
	if counterparty == "" {
		return nil
	}

	var firstErr error
	seenAt := time.Now().UTC().Format(time.RFC3339)

	for _, message := range messages {
		if message.Sender != counterparty || message.Recipient != domain.PeerIdentity(c.id.Address) {
			continue
		}
		if message.ReceiptStatus == protocol.ReceiptStatusSeen {
			continue
		}

		reply, err := c.localRequestFrameCtx(ctx, protocol.Frame{
			Type:        "send_delivery_receipt",
			ID:          message.ID,
			Address:     c.id.Address,
			Recipient:   string(counterparty),
			Status:      protocol.ReceiptStatusSeen,
			DeliveredAt: seenAt,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if reply.Type != "receipt_stored" && reply.Type != "receipt_known" {
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected receipt reply: %s", reply.Type)
			}
		}
	}

	return firstErr
}

func (c *DesktopClient) localAddress() string {
	if strings.HasPrefix(c.nodeCfg.ListenAddress, ":") {
		return "127.0.0.1" + c.nodeCfg.ListenAddress
	}
	return c.nodeCfg.ListenAddress
}

// openLocalSession was removed: console passthrough must not fall back to
// the TCP data port. All local command dispatch goes through the embedded
// localNode (HandleLocalFrame). Remote-peer TCP sessions (openSessionAt)
// are unaffected — they serve legitimate P2P operations like ping and
// message sync.

func (c *DesktopClient) openSessionAt(ctx context.Context, address, clientKind string) (net.Conn, *bufio.Reader, protocol.Frame, error) {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, protocol.Frame{}, err
	}

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)

	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        clientKind,
		ClientVersion: strings.ReplaceAll(c.appCfg.Version, " ", "-"),
		ClientBuild:   config.ClientBuild,
		Address:       c.id.Address,
		PubKey:        identity.PublicKeyBase64(c.id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(c.id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(c.id),
	})
	if err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	if _, err := io.WriteString(conn, line); err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	welcome, err := readJSONFrame(reader)
	if err != nil {
		_ = conn.Close()
		return nil, nil, protocol.Frame{}, err
	}
	if welcome.Type == "error" {
		_ = conn.Close()
		if welcome.Code != "" {
			return nil, nil, protocol.Frame{}, protocol.ErrorFromCode(welcome.Code)
		}
		return nil, nil, protocol.Frame{}, protocol.ErrProtocol
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   c.id.Address,
			Signature: identity.SignPayload(c.id, []byte("corsa-session-auth-v1|"+welcome.Challenge+"|"+c.id.Address)),
		})
		if err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		authReply, err := readJSONFrame(reader)
		if err != nil {
			_ = conn.Close()
			return nil, nil, protocol.Frame{}, err
		}
		if authReply.Type == "error" {
			_ = conn.Close()
			if authReply.Code != "" {
				return nil, nil, protocol.Frame{}, protocol.ErrorFromCode(authReply.Code)
			}
			return nil, nil, protocol.Frame{}, protocol.ErrProtocol
		}
	}

	return conn, reader, welcome, nil
}

func (c *DesktopClient) requestFrame(conn net.Conn, reader *bufio.Reader, request protocol.Frame) (protocol.Frame, error) {
	line, err := protocol.MarshalFrameLine(request)
	if err != nil {
		return protocol.Frame{}, err
	}
	if _, err := io.WriteString(conn, line); err != nil {
		return protocol.Frame{}, err
	}
	return readJSONFrame(reader)
}

func (c *DesktopClient) localRequestFrame(request protocol.Frame) (protocol.Frame, error) {
	return c.localRequestFrameCtx(context.Background(), request)
}

// localRequestFrameCtx dispatches a command frame through the embedded node.
// ctx.Err() is checked before and after HandleLocalFrame as a best-effort
// cancellation check — HandleLocalFrame itself is synchronous and cannot be
// interrupted mid-execution, so a stuck handler will block until it returns
// regardless of ctx cancellation.
//
// TCP fallback to the data port was removed: local command dispatch must go
// through the in-process embedded node exclusively. Sending console commands
// over the TCP data port bypassed RPC CommandTable and RBAC enforcement.
func (c *DesktopClient) localRequestFrameCtx(ctx context.Context, request protocol.Frame) (protocol.Frame, error) {
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}

	if c.localNode == nil {
		return protocol.Frame{}, fmt.Errorf("localRequestFrame: embedded node is required, TCP fallback removed")
	}

	frame := c.localNode.HandleLocalFrame(request)
	if err := ctx.Err(); err != nil {
		return protocol.Frame{}, err
	}
	if frame.Type == "error" {
		if frame.Code != "" {
			return protocol.Frame{}, protocol.ErrorFromCode(frame.Code)
		}
		if frame.Error != "" {
			return protocol.Frame{}, fmt.Errorf("%s", frame.Error)
		}
		return protocol.Frame{}, protocol.ErrProtocol
	}
	return frame, nil
}

func readJSONFrame(reader *bufio.Reader) (protocol.Frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return protocol.Frame{}, err
	}

	frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
	if err != nil {
		return protocol.Frame{}, err
	}
	if frame.Type == "error" {
		if frame.Code != "" {
			return protocol.Frame{}, protocol.ErrorFromCode(frame.Code)
		}
		return protocol.Frame{}, protocol.ErrProtocol
	}
	return frame, nil
}

func contactsFromFrame(frame protocol.Frame) map[string]Contact {
	out := make(map[string]Contact, len(frame.Contacts))
	for _, contact := range frame.Contacts {
		out[contact.Address] = Contact{
			BoxKey:       contact.BoxKey,
			PubKey:       contact.PubKey,
			BoxSignature: contact.BoxSig,
		}
	}
	return out
}

func messageRecordsFromFrames(messages []protocol.MessageFrame) []MessageRecord {
	out := make([]MessageRecord, 0, len(messages))
	for _, message := range messages {
		record, err := messageRecordFromFrame(message)
		if err != nil {
			continue
		}
		out = append(out, record)
	}
	return out
}

func messageRecordFromFrame(message protocol.MessageFrame) (MessageRecord, error) {
	timestamp, err := time.Parse(time.RFC3339, message.CreatedAt)
	if err != nil {
		return MessageRecord{}, err
	}
	return MessageRecord{
		ID:         message.ID,
		Flag:       message.Flag,
		Timestamp:  timestamp.UTC(),
		TTLSeconds: message.TTLSeconds,
		Sender:     message.Sender,
		Recipient:  message.Recipient,
		Body:       message.Body,
	}, nil
}

func receiptRecordsFromFrames(receipts []protocol.ReceiptFrame) []DeliveryReceipt {
	out := make([]DeliveryReceipt, 0, len(receipts))
	for _, receipt := range receipts {
		deliveredAt, err := time.Parse(time.RFC3339, receipt.DeliveredAt)
		if err != nil {
			continue
		}
		out = append(out, DeliveryReceipt{
			MessageID:   receipt.MessageID,
			Sender:      domain.PeerIdentity(receipt.Sender),
			Recipient:   domain.PeerIdentity(receipt.Recipient),
			Status:      receipt.Status,
			DeliveredAt: deliveredAt.UTC(),
		})
	}
	return out
}

func decryptNoticeFrames(id *identity.Identity, notices []protocol.NoticeFrame) []string {
	out := make([]string, 0, len(notices))
	for _, item := range notices {
		notice, err := gazeta.DecryptForIdentity(id, item.Ciphertext)
		if err != nil {
			continue
		}
		out = append(out, notice.From+">"+notice.Body)
	}
	return out
}

// receiptStatusRank returns the monotonic rank of a delivery status.
// Higher rank = further along the lifecycle. Unknown/empty = 0.
func receiptStatusRank(status string) int {
	switch status {
	case "sent":
		return 1
	case "delivered":
		return 2
	case "seen":
		return 3
	default:
		return 0
	}
}

func decryptDirectMessages(id *identity.Identity, contacts map[string]Contact, messages []MessageRecord, receipts []DeliveryReceipt, pendingItems []PendingMessage) []DirectMessage {
	receiptsByMessageID := make(map[string]DeliveryReceipt, len(receipts))
	for _, receipt := range receipts {
		existing, ok := receiptsByMessageID[receipt.MessageID]
		if !ok || receipt.Status == protocol.ReceiptStatusSeen || existing.Status != protocol.ReceiptStatusSeen {
			receiptsByMessageID[receipt.MessageID] = receipt
		}
	}
	pending := make(map[string]PendingMessage, len(pendingItems))
	for _, item := range pendingItems {
		pending[item.ID] = item
	}

	out := make([]DirectMessage, 0, len(messages))
	for _, item := range messages {
		sender := item.Sender
		recipient := item.Recipient
		ciphertext := item.Body

		// Determine the sender's public key for decryption.
		// For outgoing messages (sender == self) use the identity key directly
		// instead of relying on the contacts map, which typically does not
		// contain the node's own address. Mirrors DecryptIncomingMessage().
		var senderPubKey string
		if sender == id.Address {
			senderPubKey = identity.PublicKeyBase64(id.PublicKey)
		} else {
			contact, ok := contacts[sender]
			if !ok || contact.PubKey == "" {
				continue
			}
			senderPubKey = contact.PubKey
		}

		message, err := directmsg.DecryptForIdentity(id, sender, senderPubKey, recipient, ciphertext)
		if err != nil {
			continue
		}

		var deliveredAt *time.Time

		// Start from the persisted status in SQLite (survives restart).
		receiptStatus := item.PersistedStatus

		// Layer in-memory receipt on top — but only if it advances the status.
		if receipt, ok := receiptsByMessageID[item.ID]; ok {
			deliveredCopy := receipt.DeliveredAt
			deliveredAt = &deliveredCopy
			if receiptStatusRank(receipt.Status) > receiptStatusRank(receiptStatus) {
				receiptStatus = receipt.Status
			}
		}

		// Synthesize DeliveredAt for persisted statuses that survive restart.
		// After a restart the in-memory receipt map is empty, so deliveredAt
		// would be nil even though SQLite has a valid "delivered" or "seen"
		// status. Use the message timestamp as a reasonable approximation so
		// the UI can render status badges (checkmarks) after restart.
		if deliveredAt == nil && (receiptStatus == "delivered" || receiptStatus == "seen") {
			t := item.Timestamp
			deliveredAt = &t
		}

		// For outgoing messages with no persisted or receipt status, check pending state.
		if receiptStatus == "" && item.Sender == id.Address {
			if pendingItem, ok := pending[item.ID]; ok {
				receiptStatus = pendingItem.Status
			} else {
				receiptStatus = "sent"
			}
		}

		replyTo := domain.MessageID(message.ReplyTo)
		if replyTo != "" && !replyTo.IsValid() {
			replyTo = ""
		}

		out = append(out, DirectMessage{
			ID:            item.ID,
			Sender:        domain.PeerIdentity(sender),
			Recipient:     domain.PeerIdentity(recipient),
			Body:          message.Body,
			ReplyTo:       replyTo,
			Command:       domain.FileAction(message.Command),
			CommandData:   message.CommandData,
			Timestamp:     item.Timestamp,
			ReceiptStatus: receiptStatus,
			DeliveredAt:   deliveredAt,
		})
	}

	return out
}

// sanitizeReplyReferences checks that each ReplyTo points to a message
// that actually exists in the same conversation. Requires the persistent
// chatlog store — malformed UUIDs are already cleared during decryption.
func sanitizeReplyReferences(messages []DirectMessage, store *chatlog.Store, selfAddress string) {
	if store == nil {
		return
	}
	for i := range messages {
		if messages[i].ReplyTo == "" {
			continue
		}
		peerAddr := messages[i].Sender
		if peerAddr == domain.PeerIdentity(selfAddress) {
			peerAddr = messages[i].Recipient
		}
		if !store.HasEntryInConversation(peerAddr, domain.MessageID(messages[i].ReplyTo)) {
			messages[i].ReplyTo = ""
		}
	}
}

func (c *DesktopClient) DecryptIncomingMessage(event protocol.LocalChangeEvent) *DirectMessage {
	if event.Type != protocol.LocalChangeNewMessage || event.Topic != "dm" {
		return nil
	}

	// Determine the sender's public key for decryption.
	// For outgoing messages (sender == self) use the identity key directly
	// instead of relying on the trust store, which is more robust.
	var senderPubKey string
	if event.Sender == c.id.Address {
		senderPubKey = identity.PublicKeyBase64(c.id.PublicKey)
	} else {
		// Try the trust store first (explicitly added contacts).
		contactsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
		if err != nil {
			return nil
		}
		contacts := contactsFromFrame(contactsReply)
		contact, ok := contacts[event.Sender]
		if ok && contact.PubKey != "" {
			senderPubKey = contact.PubKey
		} else {
			// Fall back to all known contacts (includes keys recovered
			// via on-demand sync for relayed messages from senders that
			// are not in the local trust store yet).
			allReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
			if err != nil {
				return nil
			}
			allContacts := contactsFromFrame(allReply)
			allContact, ok := allContacts[event.Sender]
			if !ok || allContact.PubKey == "" {
				return nil
			}
			senderPubKey = allContact.PubKey
		}
	}

	msg, err := directmsg.DecryptForIdentity(c.id, event.Sender, senderPubKey, event.Recipient, event.Body)
	if err != nil {
		return nil
	}

	ts, parseErr := parseTimestamp(event.CreatedAt)
	if parseErr != nil {
		ts = time.Now().UTC()
	}

	// Incoming messages from others start as "delivered" (we just received them).
	status := "delivered"
	if event.Sender == c.id.Address {
		status = "sent"
	}

	// Sanitize reply_to: drop references to messages that don't exist in
	// this conversation. Prevents a remote peer from injecting dangling or
	// cross-thread reply links.
	replyTo := domain.MessageID(msg.ReplyTo)
	if replyTo != "" {
		if !replyTo.IsValid() {
			replyTo = ""
		} else if c.chatLog != nil {
			peerAddress := event.Sender
			if event.Sender == c.id.Address {
				peerAddress = event.Recipient
			}
			if !c.chatLog.HasEntryInConversation(domain.PeerIdentity(peerAddress), domain.MessageID(replyTo)) {
				replyTo = ""
			}
		}
	}

	return &DirectMessage{
		ID:            event.MessageID,
		Sender:        domain.PeerIdentity(event.Sender),
		Recipient:     domain.PeerIdentity(event.Recipient),
		Body:          msg.Body,
		ReplyTo:       replyTo,
		Command:       domain.FileAction(msg.Command),
		CommandData:   msg.CommandData,
		Timestamp:     ts,
		ReceiptStatus: status,
	}
}

func pendingMessagesFromFrame(frame protocol.Frame) []PendingMessage {
	out := make([]PendingMessage, 0, len(frame.PendingMessages))
	for _, item := range frame.PendingMessages {
		queuedAt := parseOptionalTime(item.QueuedAt)
		lastAttemptAt := parseOptionalTime(item.LastAttemptAt)
		out = append(out, PendingMessage{
			ID:            item.ID,
			Recipient:     item.Recipient,
			Status:        item.Status,
			QueuedAt:      queuedAt,
			LastAttemptAt: lastAttemptAt,
			Retries:       item.Retries,
			Error:         item.Error,
		})
	}
	return out
}

func stringifyMessages(messages []MessageRecord) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		out = append(out, message.Sender+">"+message.Recipient+">"+message.Body)
	}
	return out
}

func isConversationMessage(message protocol.MessageFrame, self, counterparty string) bool {
	return (message.Sender == self && message.Recipient == counterparty) ||
		(message.Sender == counterparty && message.Recipient == self)
}

// parseTimestamp tries RFC3339Nano first, then falls back to RFC3339 for
// legacy/migrated records that were stored with second precision.
func parseTimestamp(s string) (time.Time, error) {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		ts, err = time.Parse(time.RFC3339, s)
	}
	return ts, err
}

func dmHeadersFromFrame(frame protocol.Frame) []DMHeader {
	out := make([]DMHeader, 0, len(frame.DMHeaders))
	for _, h := range frame.DMHeaders {
		ts, err := parseTimestamp(h.CreatedAt)
		if err != nil {
			continue
		}
		out = append(out, DMHeader{
			ID:        h.ID,
			Sender:    domain.PeerIdentity(h.Sender),
			Recipient: domain.PeerIdentity(h.Recipient),
			Timestamp: ts.UTC(),
		})
	}
	return out
}

func missingDMHeaderContacts(self string, contacts map[string]Contact, headers []DMHeader) []string {
	missing := make(map[string]struct{})
	for _, h := range headers {
		for _, address := range []string{string(h.Sender), string(h.Recipient)} {
			address = strings.TrimSpace(address)
			if address == "" || address == "*" || address == self {
				continue
			}
			if _, ok := contacts[address]; ok {
				continue
			}
			missing[address] = struct{}{}
		}
	}

	out := make([]string, 0, len(missing))
	for address := range missing {
		out = append(out, address)
	}
	return out
}

// importIncomingDMHeaderContacts imports contacts for DM senders that are
// not yet in the trusted store. This is the header-only variant that works
// without decrypted message bodies.
func (c *DesktopClient) importIncomingDMHeaderContacts(trustedContacts, networkContacts map[string]Contact, headers []DMHeader) int {
	toImport := make(map[string]protocol.ContactFrame)

	for _, h := range headers {
		if h.Recipient != domain.PeerIdentity(c.id.Address) || h.Sender == domain.PeerIdentity(c.id.Address) {
			continue
		}
		if _, ok := trustedContacts[string(h.Sender)]; ok {
			continue
		}
		contact, ok := networkContacts[string(h.Sender)]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[string(h.Sender)] = protocol.ContactFrame{
			Address: string(h.Sender),
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}
	}

	if len(toImport) == 0 {
		return 0
	}

	contacts := make([]protocol.ContactFrame, 0, len(toImport))
	addresses := make([]string, 0, len(toImport))
	for address := range toImport {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		contacts = append(contacts, toImport[address])
	}

	reply, err := c.localRequestFrame(protocol.Frame{
		Type:     "import_contacts",
		Contacts: contacts,
	})
	if err != nil {
		return 0
	}
	return reply.Count
}

// fetchContactsForDecrypt loads trusted contacts and, if any sender in the
// supplied list is missing, supplements with network contacts. Trusted
// contacts always take precedence over network ones. This helper
// deduplicates the contact-fetching pattern used by FetchConversation,
// FetchConversationPreviews, and FetchSinglePreview.
//
// The local identity address (c.id.Address) is excluded from the missing-
// sender check because all callers already handle sender==self via the
// identity key, without needing contacts. This avoids a spurious
// fetch_contacts roundtrip on every conversation that contains outgoing
// messages.
func (c *DesktopClient) fetchContactsForDecrypt(ctx context.Context, senders []string) (map[string]Contact, error) {
	contactsReply, err := c.localRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	contacts := contactsFromFrame(contactsReply)

	// Check if any non-self sender is missing from the trusted store.
	// Self (outgoing messages) is decrypted via the identity key by all
	// callers, so its absence in contacts is not a reason to fetch network
	// contacts.
	myAddr := c.id.Address
	needNetwork := false
	for _, sender := range senders {
		if sender == myAddr {
			continue
		}
		if _, ok := contacts[sender]; !ok {
			needNetwork = true
			break
		}
	}
	if !needNetwork {
		return contacts, nil
	}

	// Supplement with network contacts; trusted contacts take precedence.
	// If this fails, propagate the error so callers can retry — silently
	// returning trusted-only contacts would cause decryptDirectMessages to
	// skip messages from unknown senders, producing an incomplete history
	// or empty preview bodies without any error signal.
	networkReply, networkErr := c.localRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_contacts"})
	if networkErr != nil {
		return nil, fmt.Errorf("fetch network contacts: %w", networkErr)
	}
	networkContacts := contactsFromFrame(networkReply)
	merged := make(map[string]Contact, len(contacts)+len(networkContacts))
	for addr, nc := range networkContacts {
		merged[addr] = nc
	}
	for addr, tc := range contacts {
		merged[addr] = tc // trusted wins
	}
	return merged, nil
}

// FetchConversation loads the full chat history for a single peer from the
// chatlog on disk, decrypts it, and returns the messages. This is called
// on demand when the user switches to a conversation rather than keeping
// all conversations in memory.
func (c *DesktopClient) FetchConversation(ctx context.Context, peerAddress domain.PeerIdentity) ([]DirectMessage, error) {
	peerAddress = domain.PeerIdentity(strings.TrimSpace(string(peerAddress)))
	if peerAddress == "" {
		return nil, fmt.Errorf("peer address is required")
	}
	if c.chatLog == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	// Read directly from the local chatlog — no node frame roundtrip.
	// Use context-aware variant so caller deadlines bound SQLite I/O.
	entries, err := c.chatLog.ReadCtx(ctx, "dm", peerAddress)
	if err != nil {
		return nil, fmt.Errorf("chatlog read: %w", err)
	}

	// Collect unique senders for contact lookup.
	senderSet := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		senderSet[entry.Sender] = struct{}{}
	}
	senders := make([]string, 0, len(senderSet))
	for s := range senderSet {
		senders = append(senders, s)
	}
	decryptContacts, err := c.fetchContactsForDecrypt(ctx, senders)
	if err != nil {
		return nil, err
	}

	receiptsReply, err := c.localRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_delivery_receipts", Recipient: c.id.Address})
	if err != nil {
		return nil, err
	}
	deliveryReceipts := receiptRecordsFromFrames(receiptsReply.Receipts)
	pendingReply, err := c.localRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_pending_messages", Topic: "dm"})
	if err != nil {
		return nil, err
	}
	pendingMessages := pendingMessagesFromFrame(pendingReply)

	// Convert chatlog entries to MessageRecord for decryption.
	records := make([]MessageRecord, 0, len(entries))
	for _, entry := range entries {
		ts, parseErr := parseTimestamp(entry.CreatedAt)
		if parseErr != nil {
			continue
		}
		records = append(records, MessageRecord{
			ID:              entry.ID,
			Flag:            entry.Flag,
			Timestamp:       ts.UTC(),
			Sender:          entry.Sender,
			Recipient:       entry.Recipient,
			Body:            entry.Body,
			PersistedStatus: entry.DeliveryStatus,
		})
	}

	messages := decryptDirectMessages(c.id, decryptContacts, records, deliveryReceipts, pendingMessages)
	sanitizeReplyReferences(messages, c.chatLog, c.id.Address)
	return messages, nil
}

// FetchConversationPreviews loads the last message for each DM conversation
// from the chatlog, decrypts them, and returns preview data for the sidebar.
// It also fetches conversation summaries (including UnreadCount) from SQLite
// so the desktop UI can restore unread badges after a restart.
func (c *DesktopClient) FetchConversationPreviews(ctx context.Context) ([]ConversationPreview, error) {
	if c.chatLog == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	// Read directly from the local chatlog — no node frame roundtrip.
	// Use context-aware variants so caller deadlines bound SQLite I/O.
	lastEntries, err := c.chatLog.ReadLastEntryPerPeerCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("chatlog previews: %w", err)
	}

	// Fetch conversation summaries for unread counts.
	// This query is the source of truth for unread badges after restart.
	// If it fails, return an error so the caller can retry — silently
	// continuing with an empty map would show all chats as read.
	unreadByPeer := make(map[string]int)
	summaries, err := c.chatLog.ListConversationsCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("chatlog summaries: %w", err)
	}
	for _, conv := range summaries {
		if conv.UnreadCount > 0 {
			unreadByPeer[conv.PeerAddress] = conv.UnreadCount
		}
	}

	// Collect unique senders for contact lookup.
	senders := make([]string, 0, len(lastEntries))
	for _, entry := range lastEntries {
		senders = append(senders, entry.Sender)
	}
	decryptContacts, err := c.fetchContactsForDecrypt(ctx, senders)
	if err != nil {
		return nil, err
	}

	out := make([]ConversationPreview, 0, len(lastEntries))
	for peerAddr, entry := range lastEntries {
		peer := domain.PeerIdentity(peerAddr)
		senderRaw := entry.Sender
		sender := domain.PeerIdentity(senderRaw)

		// For outgoing messages (sender == self) use the identity key directly.
		var senderPubKey string
		if senderRaw == c.id.Address {
			senderPubKey = identity.PublicKeyBase64(c.id.PublicKey)
		} else {
			contact, ok := decryptContacts[senderRaw]
			if !ok || contact.PubKey == "" {
				ts, _ := parseTimestamp(entry.CreatedAt)
				out = append(out, ConversationPreview{
					PeerAddress: peer,
					Sender:      sender,
					Body:        "",
					Timestamp:   ts.UTC(),
					UnreadCount: unreadByPeer[peerAddr],
				})
				continue
			}
			senderPubKey = contact.PubKey
		}

		message, decryptErr := directmsg.DecryptForIdentity(c.id, senderRaw, senderPubKey, entry.Recipient, entry.Body)
		if decryptErr != nil {
			ts, _ := parseTimestamp(entry.CreatedAt)
			out = append(out, ConversationPreview{
				PeerAddress: peer,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
				UnreadCount: unreadByPeer[peerAddr],
			})
			continue
		}

		ts, _ := parseTimestamp(entry.CreatedAt)
		out = append(out, ConversationPreview{
			PeerAddress: peer,
			Sender:      sender,
			Body:        message.Body,
			Timestamp:   ts.UTC(),
			UnreadCount: unreadByPeer[peerAddr],
		})
	}

	return out, nil
}

// FetchSinglePreview loads and decrypts the last message for a single conversation.
func (c *DesktopClient) FetchSinglePreview(ctx context.Context, peerAddress domain.PeerIdentity) (*ConversationPreview, error) {
	peerAddress = domain.PeerIdentity(strings.TrimSpace(string(peerAddress)))
	if peerAddress == "" {
		return nil, fmt.Errorf("peer address is required")
	}
	if c.chatLog == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	// Read directly from the local chatlog — no node frame roundtrip.
	// Use context-aware variant so caller deadlines bound SQLite I/O.
	entry, err := c.chatLog.ReadLastEntryCtx(ctx, "dm", peerAddress)
	if err != nil {
		return nil, fmt.Errorf("chatlog read last: %w", err)
	}
	if entry == nil {
		return nil, nil
	}

	senderRaw := entry.Sender
	sender := domain.PeerIdentity(senderRaw)
	contacts, err := c.fetchContactsForDecrypt(ctx, []string{senderRaw})
	if err != nil {
		return nil, err
	}

	// For outgoing messages (sender == self) use the identity key directly.
	var senderPubKey string
	if senderRaw == c.id.Address {
		senderPubKey = identity.PublicKeyBase64(c.id.PublicKey)
	} else {
		contact, ok := contacts[senderRaw]
		if !ok || contact.PubKey == "" {
			ts, _ := parseTimestamp(entry.CreatedAt)
			return &ConversationPreview{
				PeerAddress: peerAddress,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
			}, nil
		}
		senderPubKey = contact.PubKey
	}

	ts, _ := parseTimestamp(entry.CreatedAt)

	message, decryptErr := directmsg.DecryptForIdentity(c.id, senderRaw, senderPubKey, entry.Recipient, entry.Body)
	if decryptErr != nil {
		return &ConversationPreview{
			PeerAddress: peerAddress,
			Sender:      sender,
			Body:        "",
			Timestamp:   ts.UTC(),
		}, nil
	}

	return &ConversationPreview{
		PeerAddress: peerAddress,
		Sender:      sender,
		Body:        message.Body,
		Timestamp:   ts.UTC(),
	}, nil
}
