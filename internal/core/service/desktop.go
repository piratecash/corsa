package service

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
	"corsa/internal/core/transport"
)

type DesktopClient struct {
	id      *identity.Identity
	appCfg  config.App
	nodeCfg config.Node
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

type NodeStatus struct {
	Address          string
	Connected        bool
	Welcome          string
	NodeID           string
	NodeType         string
	ClientVersion    string
	Services         []string
	KnownIDs         []string
	Contacts         map[string]Contact
	Peers            []string
	Stored           string
	Messages         []string
	MessageIDs       []string
	DirectMessages   []string
	DirectMessageIDs []string
	Inbox            []string
	DirectInbox      []string
	Gazeta           []string
	Error            string
	CheckedAt        time.Time
}

func NewDesktopClient(appCfg config.App, nodeCfg config.Node, id *identity.Identity) *DesktopClient {
	return &DesktopClient{
		id:      id,
		appCfg:  appCfg,
		nodeCfg: nodeCfg,
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

func (c *DesktopClient) ProbeNode(ctx context.Context) NodeStatus {
	status := NodeStatus{
		Address: c.localAddress(),
	}

	if status.Address == "" {
		status.Error = "no target address configured"
		status.CheckedAt = time.Now()
		return status
	}

	conn, reader, welcome, err := c.openLocalSession(ctx)
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	defer func() { _ = conn.Close() }()
	status.Welcome = welcome.Type
	status.NodeID = welcome.Address
	status.NodeType = welcome.NodeType
	status.ClientVersion = welcome.ClientVersion
	status.Services = welcome.Services

	peersReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "get_peers"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	idsReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_identities"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	contactsReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	messagesReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_messages", Topic: "global"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	directMessagesReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_messages", Topic: "dm"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	messageIDsReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_message_ids", Topic: "global"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	directMessageIDsReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_message_ids", Topic: "dm"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	inboxReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_inbox", Topic: "global", Recipient: c.id.Address})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	directInboxReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: c.id.Address})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	noticesReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_notices"})
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	peers := peersReply.Peers
	ids := idsReply.Identities
	contacts := contactsFromFrame(contactsReply)
	messages := messageRecordsFromFrames(messagesReply.Messages)
	directMessages := messageRecordsFromFrames(directMessagesReply.Messages)
	messageIDs := messageIDsReply.IDs
	directMessageIDs := directMessageIDsReply.IDs
	inbox := messageRecordsFromFrames(inboxReply.Messages)
	directInbox := messageRecordsFromFrames(directInboxReply.Messages)
	notices := decryptNoticeFrames(c.id, noticesReply.Notices)

	if missing := missingDirectContacts(c.id.Address, contacts, directMessages); len(missing) > 0 {
		refreshedContactsReply, refreshErr := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_contacts"})
		if refreshErr == nil {
			contacts = contactsFromFrame(refreshedContactsReply)
		}
	}

	decryptedDirectMessages := decryptDirectMessages(c.id, contacts, directMessages)

	status.Connected = true
	status.KnownIDs = ids
	status.Contacts = contacts
	status.Peers = peers
	status.Messages = stringifyMessages(messages)
	status.MessageIDs = messageIDs
	status.DirectMessages = decryptedDirectMessages
	status.DirectMessageIDs = directMessageIDs
	status.Inbox = stringifyMessages(inbox)
	status.DirectInbox = stringifyMessages(directInbox)
	status.Gazeta = notices
	status.CheckedAt = time.Now()
	return status
}

func (c *DesktopClient) FetchMessageIDs(ctx context.Context, topic string) ([]string, error) {
	conn, reader, _, err := c.openLocalSession(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	frame, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_message_ids", Topic: strings.TrimSpace(topic)})
	if err != nil {
		return nil, err
	}
	return frame.IDs, nil
}

func (c *DesktopClient) FetchMessage(ctx context.Context, topic, messageID string) (MessageRecord, error) {
	conn, reader, _, err := c.openLocalSession(ctx)
	if err != nil {
		return MessageRecord{}, err
	}
	defer func() { _ = conn.Close() }()

	frame, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_message", Topic: strings.TrimSpace(topic), ID: strings.TrimSpace(messageID)})
	if err != nil {
		return MessageRecord{}, err
	}
	if frame.Item == nil {
		return MessageRecord{}, fmt.Errorf("message item is missing")
	}
	return messageRecordFromFrame(*frame.Item)
}

func (c *DesktopClient) RebuildTrust(ctx context.Context) (int, error) {
	localConn, localReader, _, err := c.openLocalSession(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = localConn.Close() }()

	peersFrame, err := c.requestFrame(localConn, localReader, protocol.Frame{Type: "get_peers"})
	if err != nil {
		return 0, err
	}

	addresses := make(map[string]struct{})
	for _, peer := range c.BootstrapPeers() {
		address := strings.TrimSpace(peer.Address)
		if address != "" {
			addresses[address] = struct{}{}
		}
	}
	for _, address := range peersFrame.Peers {
		address = strings.TrimSpace(address)
		if address != "" {
			addresses[address] = struct{}{}
		}
	}

	collected := make(map[string]protocol.ContactFrame)
	var firstErr error

	for address := range addresses {
		if address == c.localAddress() {
			continue
		}

		remoteConn, remoteReader, welcome, err := c.openSessionAt(ctx, address, "desktop")
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		if strings.TrimSpace(welcome.Address) != "" {
			collected[welcome.Address] = protocol.ContactFrame{
				Address: welcome.Address,
				PubKey:  welcome.PubKey,
				BoxKey:  welcome.BoxKey,
				BoxSig:  welcome.BoxSig,
			}
		}

		frame, err := c.requestFrame(remoteConn, remoteReader, protocol.Frame{Type: "fetch_contacts"})
		_ = remoteConn.Close()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		for _, contact := range frame.Contacts {
			if strings.TrimSpace(contact.Address) == "" || contact.Address == c.id.Address {
				continue
			}
			collected[contact.Address] = contact
		}
	}

	if len(collected) == 0 {
		if firstErr != nil {
			return 0, firstErr
		}
		return 0, nil
	}

	contacts := make([]protocol.ContactFrame, 0, len(collected))
	for _, contact := range collected {
		contacts = append(contacts, contact)
	}

	reply, err := c.requestFrame(localConn, localReader, protocol.Frame{
		Type:     "import_contacts",
		Contacts: contacts,
	})
	if err != nil {
		return 0, err
	}

	return reply.Count, nil
}

func (c *DesktopClient) SyncDirectMessagesFromPeers(ctx context.Context, peerAddresses []string, counterparty string) (int, error) {
	counterparty = strings.TrimSpace(counterparty)
	if counterparty == "" {
		return 0, fmt.Errorf("counterparty is required")
	}

	localConn, localReader, _, err := c.openLocalSession(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = localConn.Close() }()

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

			reply, err := c.requestFrame(localConn, localReader, protocol.Frame{
				Type:       "send_message",
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

	conn, reader, _, err := c.openLocalSession(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	contactsReply, err := c.requestFrame(conn, reader, protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		return err
	}
	contacts := contactsFromFrame(contactsReply)

	recipient, ok := contacts[to]
	if !ok || recipient.BoxKey == "" {
		return fmt.Errorf("recipient box key is unknown")
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
	reply, err := c.requestFrame(conn, reader, protocol.Frame{
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

func (c *DesktopClient) localAddress() string {
	if strings.HasPrefix(c.nodeCfg.ListenAddress, ":") {
		return "127.0.0.1" + c.nodeCfg.ListenAddress
	}
	return c.nodeCfg.ListenAddress
}

func (c *DesktopClient) openLocalSession(ctx context.Context) (net.Conn, *bufio.Reader, protocol.Frame, error) {
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
		Version:       1,
		Client:        clientKind,
		ClientVersion: strings.ReplaceAll(c.appCfg.Version, " ", "-"),
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

func decryptDirectMessages(id *identity.Identity, contacts map[string]Contact, messages []MessageRecord) []string {
	out := make([]string, 0, len(messages))
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

		out = append(out, sender+">"+recipient+">"+message.Body)
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

func missingDirectContacts(self string, contacts map[string]Contact, messages []MessageRecord) []string {
	missing := make(map[string]struct{})
	for _, item := range messages {
		for _, address := range []string{item.Sender, item.Recipient} {
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
