package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
	"corsa/internal/core/node"
	"corsa/internal/core/protocol"
	"corsa/internal/core/transport"
)

type DesktopClient struct {
	id        *identity.Identity
	appCfg    config.App
	nodeCfg   config.Node
	localNode *node.Service
}

type Contact struct {
	BoxKey       string
	PubKey       string
	BoxSignature string
}

type MessageRecord struct {
	ID         string
	Flag       string
	Timestamp  time.Time
	TTLSeconds int
	Sender     string
	Recipient  string
	Body       string
}

type DeliveryReceipt struct {
	MessageID   string
	Sender      string
	Recipient   string
	Status      string
	DeliveredAt time.Time
}

type PeerHealth struct {
	Address             string
	ClientVersion       string
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
}

type DirectMessage struct {
	ID            string
	Sender        string
	Recipient     string
	Body          string
	Timestamp     time.Time
	ReceiptStatus string
	DeliveredAt   *time.Time
}

// DMHeader is a lightweight message header (no body) for new-message detection.
type DMHeader struct {
	ID        string
	Sender    string
	Recipient string
	Timestamp time.Time
}

// ConversationPreview holds the last decrypted message for a conversation peer.
type ConversationPreview struct {
	PeerAddress string
	Sender      string
	Body        string
	Timestamp   time.Time
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
	Address      string `json:"address"`
	Network      string `json:"network,omitempty"`
	State        string `json:"state"`
	Connected    bool   `json:"connected"`
	PendingCount int    `json:"pending_count,omitempty"`
	LastError    string `json:"last_error,omitempty"`
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
	KnownIDs         []string
	Contacts         map[string]Contact
	Peers            []string
	PeerHealth       []PeerHealth
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
	return &DesktopClient{
		id:        id,
		appCfg:    appCfg,
		nodeCfg:   nodeCfg,
		localNode: localNode,
	}
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

func (c *DesktopClient) Address() string {
	return c.id.Address
}

func (c *DesktopClient) BootstrapPeers() []transport.Peer {
	peers := make([]transport.Peer, 0, len(c.nodeCfg.BootstrapPeers))
	for i, addr := range c.nodeCfg.BootstrapPeers {
		peers = append(peers, transport.Peer{
			ID:      peerID(i),
			Address: addr,
		})
	}
	return peers
}

func (c *DesktopClient) SubscribeLocalChanges() (<-chan struct{}, func()) {
	if c.localNode == nil {
		ch := make(chan struct{})
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

func peerHealthFromFrame(frame protocol.Frame) []PeerHealth {
	items := make([]PeerHealth, 0, len(frame.PeerHealth))
	for _, item := range frame.PeerHealth {
		items = append(items, PeerHealth{
			Address:             item.Address,
			ClientVersion:       item.ClientVersion,
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
		if message.Recipient != self || message.Sender == self {
			continue
		}
		if _, ok := trustedContacts[message.Sender]; ok {
			continue
		}
		contact, ok := decryptContacts[message.Sender]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[message.Sender] = protocol.ContactFrame{
			Address: message.Sender,
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

	allPeers := make([]ConsolePeerStatus, 0, len(peers))
	connected := make([]ConsolePeerStatus, 0, len(peers))
	knownWithState := make([]ConsolePeerStatus, 0, len(health))
	knownOnly := make([]string, 0, len(peers))

	for _, address := range peers {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		item, ok := byAddress[address]
		if !ok {
			knownOnly = append(knownOnly, address)
			allPeers = append(allPeers, ConsolePeerStatus{Address: address, Network: node.ClassifyAddress(address).String(), State: "known"})
			continue
		}

		status := ConsolePeerStatus{
			Address:      address,
			Network:      node.ClassifyAddress(address).String(),
			State:        "known",
			Connected:    item.Connected,
			PendingCount: item.PendingCount,
			LastError:    item.LastError,
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
		"You can also paste a raw JSON protocol frame.",
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

func (c *DesktopClient) SendDirectMessage(ctx context.Context, to, body string) error {
	to = strings.TrimSpace(to)
	body = strings.TrimSpace(body)
	if to == "" || body == "" {
		return fmt.Errorf("recipient and message are required")
	}

	recipient, err := c.ensureRecipientContact(ctx, to)
	if err != nil {
		return err
	}

	ciphertext, err := directmsg.EncryptForParticipants(c.id, to, recipient.BoxKey, body)
	if err != nil {
		return err
	}

	messageID, err := protocol.NewMessageID()
	if err != nil {
		return err
	}

	createdAt := time.Now().UTC().Format(time.RFC3339)
	reply, err := c.localRequestFrame(protocol.Frame{
		Type:       "send_message",
		Topic:      "dm",
		ID:         string(messageID),
		Address:    c.id.Address,
		Recipient:  to,
		Flag:       string(protocol.MessageFlagSenderDelete),
		CreatedAt:  createdAt,
		TTLSeconds: 0,
		Body:       ciphertext,
	})
	if err != nil {
		return err
	}

	if reply.Type == "message_stored" || reply.Type == "message_known" {
		return nil
	}

	return fmt.Errorf("unexpected send reply: %s", reply.Type)
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

func (c *DesktopClient) MarkConversationSeen(ctx context.Context, counterparty string, messages []DirectMessage) error {
	counterparty = strings.TrimSpace(counterparty)
	if counterparty == "" {
		return nil
	}

	var firstErr error
	seenAt := time.Now().UTC().Format(time.RFC3339)

	for _, message := range messages {
		if message.Sender != counterparty || message.Recipient != c.id.Address {
			continue
		}
		if message.ReceiptStatus == protocol.ReceiptStatusSeen {
			continue
		}

		reply, err := c.localRequestFrame(protocol.Frame{
			Type:        "send_delivery_receipt",
			ID:          message.ID,
			Address:     c.id.Address,
			Recipient:   counterparty,
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

func (c *DesktopClient) openLocalSession(ctx context.Context) (net.Conn, *bufio.Reader, protocol.Frame, error) {
	if c.localNode != nil {
		return nil, nil, protocol.Frame{}, fmt.Errorf("local TCP session is disabled for embedded node mode")
	}
	return c.openSessionAt(ctx, c.localAddress(), "desktop")
}

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
	if c.localNode != nil {
		frame := c.localNode.HandleLocalFrame(request)
		if frame.Type == "error" {
			if frame.Code != "" {
				return protocol.Frame{}, protocol.ErrorFromCode(frame.Code)
			}
			return protocol.Frame{}, protocol.ErrProtocol
		}
		return frame, nil
	}

	conn, reader, _, err := c.openLocalSession(context.Background())
	if err != nil {
		return protocol.Frame{}, err
	}
	defer func() { _ = conn.Close() }()

	return c.requestFrame(conn, reader, request)
}

func peerID(index int) string {
	return fmt.Sprintf("bootstrap-%d", index)
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
			Sender:      receipt.Sender,
			Recipient:   receipt.Recipient,
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

		contact, ok := contacts[sender]
		if !ok || contact.PubKey == "" {
			continue
		}

		message, err := directmsg.DecryptForIdentity(id, sender, contact.PubKey, recipient, ciphertext)
		if err != nil {
			continue
		}

		var deliveredAt *time.Time
		receiptStatus := ""
		if receipt, ok := receiptsByMessageID[item.ID]; ok {
			deliveredCopy := receipt.DeliveredAt
			deliveredAt = &deliveredCopy
			receiptStatus = receipt.Status
		} else if item.Sender == id.Address {
			if pendingItem, ok := pending[item.ID]; ok {
				receiptStatus = pendingItem.Status
			} else {
				receiptStatus = "sent"
			}
		}

		out = append(out, DirectMessage{
			ID:            item.ID,
			Sender:        sender,
			Recipient:     recipient,
			Body:          message.Body,
			Timestamp:     item.Timestamp,
			ReceiptStatus: receiptStatus,
			DeliveredAt:   deliveredAt,
		})
	}

	return out
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

func dmHeadersFromFrame(frame protocol.Frame) []DMHeader {
	out := make([]DMHeader, 0, len(frame.DMHeaders))
	for _, h := range frame.DMHeaders {
		ts, err := time.Parse(time.RFC3339Nano, h.CreatedAt)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, h.CreatedAt)
			if err != nil {
				continue
			}
		}
		out = append(out, DMHeader{
			ID:        h.ID,
			Sender:    h.Sender,
			Recipient: h.Recipient,
			Timestamp: ts.UTC(),
		})
	}
	return out
}

func missingDMHeaderContacts(self string, contacts map[string]Contact, headers []DMHeader) []string {
	missing := make(map[string]struct{})
	for _, h := range headers {
		for _, address := range []string{h.Sender, h.Recipient} {
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
		if h.Recipient != c.id.Address || h.Sender == c.id.Address {
			continue
		}
		if _, ok := trustedContacts[h.Sender]; ok {
			continue
		}
		contact, ok := networkContacts[h.Sender]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[h.Sender] = protocol.ContactFrame{
			Address: h.Sender,
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

// FetchConversation loads the full chat history for a single peer from the
// chatlog on disk, decrypts it, and returns the messages. This is called
// on demand when the user switches to a conversation rather than keeping
// all conversations in memory.
func (c *DesktopClient) FetchConversation(ctx context.Context, peerAddress string) ([]DirectMessage, error) {
	peerAddress = strings.TrimSpace(peerAddress)
	if peerAddress == "" {
		return nil, fmt.Errorf("peer address is required")
	}

	chatlogReply, err := c.localRequestFrame(protocol.Frame{
		Type:    "fetch_chatlog",
		Topic:   "dm",
		Address: peerAddress,
	})
	if err != nil {
		return nil, err
	}

	contactsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	contacts := contactsFromFrame(contactsReply)

	// If sender contact is missing from trusted store, supplement with network contacts.
	decryptContacts := contacts
	needRefresh := false
	for _, entry := range chatlogReply.ChatEntries {
		sender := entry.Sender
		if _, ok := contacts[sender]; !ok {
			needRefresh = true
			break
		}
	}
	if needRefresh {
		networkReply, networkErr := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
		if networkErr == nil {
			networkContacts := contactsFromFrame(networkReply)
			// Merge: trusted contacts take priority, fill gaps with network contacts.
			merged := make(map[string]Contact, len(contacts)+len(networkContacts))
			for addr, c := range networkContacts {
				merged[addr] = c
			}
			for addr, c := range contacts {
				merged[addr] = c
			}
			decryptContacts = merged
		}
	}

	receiptsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_delivery_receipts", Recipient: c.id.Address})
	if err != nil {
		return nil, err
	}
	deliveryReceipts := receiptRecordsFromFrames(receiptsReply.Receipts)
	pendingReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_pending_messages", Topic: "dm"})
	if err != nil {
		return nil, err
	}
	pendingMessages := pendingMessagesFromFrame(pendingReply)

	// Convert chatlog entries to MessageRecord for decryption.
	records := make([]MessageRecord, 0, len(chatlogReply.ChatEntries))
	for _, entry := range chatlogReply.ChatEntries {
		ts, parseErr := time.Parse(time.RFC3339Nano, entry.CreatedAt)
		if parseErr != nil {
			ts, parseErr = time.Parse(time.RFC3339, entry.CreatedAt)
			if parseErr != nil {
				continue
			}
		}
		records = append(records, MessageRecord{
			ID:        entry.ID,
			Flag:      entry.Flag,
			Timestamp: ts.UTC(),
			Sender:    entry.Sender,
			Recipient: entry.Recipient,
			Body:      entry.Body,
		})
	}

	return decryptDirectMessages(c.id, decryptContacts, records, deliveryReceipts, pendingMessages), nil
}

// FetchConversationPreviews loads the last message for each DM conversation
// from the chatlog, decrypts them, and returns preview data for the sidebar.
func (c *DesktopClient) FetchConversationPreviews(ctx context.Context) ([]ConversationPreview, error) {
	previewsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_chatlog_previews"})
	if err != nil {
		return nil, err
	}

	contactsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	contacts := contactsFromFrame(contactsReply)

	// Supplement with network contacts for unknown senders.
	decryptContacts := contacts
	for _, preview := range previewsReply.ChatPreviews {
		if _, ok := contacts[preview.Sender]; !ok {
			networkReply, networkErr := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
			if networkErr == nil {
				networkContacts := contactsFromFrame(networkReply)
				// Merge: trusted contacts take priority, fill gaps with network contacts.
				merged := make(map[string]Contact, len(contacts)+len(networkContacts))
				for addr, c := range networkContacts {
					merged[addr] = c
				}
				for addr, c := range contacts {
					merged[addr] = c
				}
				decryptContacts = merged
			}
			break
		}
	}

	out := make([]ConversationPreview, 0, len(previewsReply.ChatPreviews))
	for _, preview := range previewsReply.ChatPreviews {
		sender := preview.Sender
		contact, ok := decryptContacts[sender]
		if !ok || contact.PubKey == "" {
			// Can't decrypt — show placeholder.
			ts, _ := time.Parse(time.RFC3339Nano, preview.CreatedAt)
			out = append(out, ConversationPreview{
				PeerAddress: preview.PeerAddress,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
			})
			continue
		}

		message, err := directmsg.DecryptForIdentity(c.id, sender, contact.PubKey, preview.Recipient, preview.Body)
		if err != nil {
			ts, _ := time.Parse(time.RFC3339Nano, preview.CreatedAt)
			out = append(out, ConversationPreview{
				PeerAddress: preview.PeerAddress,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
			})
			continue
		}

		ts, _ := time.Parse(time.RFC3339Nano, preview.CreatedAt)
		out = append(out, ConversationPreview{
			PeerAddress: preview.PeerAddress,
			Sender:      sender,
			Body:        message.Body,
			Timestamp:   ts.UTC(),
		})
	}

	return out, nil
}

// FetchSinglePreview loads and decrypts the last message for a single conversation.
func (c *DesktopClient) FetchSinglePreview(ctx context.Context, peerAddress string) (*ConversationPreview, error) {
	chatlogReply, err := c.localRequestFrame(protocol.Frame{
		Type:    "fetch_chatlog",
		Topic:   "dm",
		Address: peerAddress,
		Limit:   1,
	})
	if err != nil {
		return nil, err
	}
	if len(chatlogReply.ChatEntries) == 0 {
		return nil, nil
	}

	entry := chatlogReply.ChatEntries[0]
	contactsReply, err := c.localRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	contacts := contactsFromFrame(contactsReply)

	sender := entry.Sender
	contact, ok := contacts[sender]
	if !ok || contact.PubKey == "" {
		networkReply, networkErr := c.localRequestFrame(protocol.Frame{Type: "fetch_contacts"})
		if networkErr == nil {
			networkContacts := contactsFromFrame(networkReply)
			contact, ok = networkContacts[sender]
		}
	}

	ts, _ := time.Parse(time.RFC3339Nano, entry.CreatedAt)
	if !ok || contact.PubKey == "" {
		return &ConversationPreview{
			PeerAddress: peerAddress,
			Sender:      sender,
			Body:        "",
			Timestamp:   ts.UTC(),
		}, nil
	}

	message, err := directmsg.DecryptForIdentity(c.id, sender, contact.PubKey, entry.Recipient, entry.Body)
	if err != nil {
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
