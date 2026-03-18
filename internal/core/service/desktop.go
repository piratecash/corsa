package service

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
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

type NodeStatus struct {
	Address        string
	Connected      bool
	Welcome        string
	NodeID         string
	NodeType       string
	ClientVersion  string
	Services       []string
	KnownIDs       []string
	Contacts       map[string]Contact
	Peers          []string
	Stored         string
	Messages       []string
	DirectMessages []string
	Inbox          []string
	DirectInbox    []string
	Gazeta         []string
	Error          string
	CheckedAt      time.Time
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

	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", status.Address)
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	reader := bufio.NewReader(conn)

	if _, err := fmt.Fprintf(conn, "HELLO version=1 client=desktop client_version=%s\n", strings.ReplaceAll(c.appCfg.Version, " ", "-")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	welcome, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	status.Welcome = strings.TrimSpace(welcome)
	status.NodeID = parseWelcomeField(status.Welcome, "address")
	status.NodeType = parseWelcomeField(status.Welcome, "node_type")
	status.ClientVersion = parseWelcomeField(status.Welcome, "client_version")
	status.Services = parseCSVField(status.Welcome, "services")

	if _, err := conn.Write([]byte("GET_PEERS\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	peersReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	peers, err := parsePeers(strings.TrimSpace(peersReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	if _, err := conn.Write([]byte("FETCH_IDENTITIES\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	idsReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	ids, err := parseIdentities(strings.TrimSpace(idsReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	if _, err := conn.Write([]byte("FETCH_CONTACTS\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	contactsReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	contacts, err := parseContacts(strings.TrimSpace(contactsReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	if _, err := conn.Write([]byte("FETCH_MESSAGES global\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	messagesReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	if _, err := conn.Write([]byte("FETCH_MESSAGES dm\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	directMessagesReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	directMessages, err := parseMessages(strings.TrimSpace(directMessagesReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	messages, err := parseMessages(strings.TrimSpace(messagesReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	inboxWire := fmt.Sprintf("FETCH_INBOX global %s\n", c.id.Address)
	if _, err := conn.Write([]byte(inboxWire)); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	inboxReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	inbox, err := parseInbox(strings.TrimSpace(inboxReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	directInboxWire := fmt.Sprintf("FETCH_INBOX dm %s\n", c.id.Address)
	if _, err := conn.Write([]byte(directInboxWire)); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	directInboxReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	directInbox, err := parseInbox(strings.TrimSpace(directInboxReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	if _, err := conn.Write([]byte("FETCH_NOTICES\n")); err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	noticesReply, err := reader.ReadString('\n')
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	notices, err := decryptNotices(c.id, strings.TrimSpace(noticesReply))
	if err != nil {
		status.Error = err.Error()
		status.CheckedAt = time.Now()
		return status
	}

	decryptedDirectMessages := decryptDirectMessages(c.id, contacts, directMessages)

	status.Connected = true
	status.KnownIDs = ids
	status.Contacts = contacts
	status.Peers = peers
	status.Messages = messages
	status.DirectMessages = decryptedDirectMessages
	status.Inbox = inbox
	status.DirectInbox = directInbox
	status.Gazeta = notices
	status.CheckedAt = time.Now()
	return status
}

func (c *DesktopClient) SendDirectMessage(ctx context.Context, to, body string) error {
	to = strings.TrimSpace(to)
	body = strings.TrimSpace(body)
	if to == "" || body == "" {
		return fmt.Errorf("recipient and message are required")
	}

	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", c.localAddress())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)

	if _, err := fmt.Fprintf(conn, "HELLO version=1 client=desktop client_version=%s\n", strings.ReplaceAll(c.appCfg.Version, " ", "-")); err != nil {
		return err
	}
	if _, err := reader.ReadString('\n'); err != nil {
		return err
	}

	if _, err := conn.Write([]byte("FETCH_CONTACTS\n")); err != nil {
		return err
	}

	contactsReply, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	contacts, err := parseContacts(strings.TrimSpace(contactsReply))
	if err != nil {
		return err
	}

	recipient, ok := contacts[to]
	if !ok || recipient.BoxKey == "" {
		return fmt.Errorf("recipient box key is unknown")
	}

	ciphertext, err := directmsg.EncryptForParticipants(c.id, to, recipient.BoxKey, body)
	if err != nil {
		return err
	}

	wire := fmt.Sprintf("SEND_MESSAGE dm %s %s %s\n", c.id.Address, to, ciphertext)
	if _, err := conn.Write([]byte(wire)); err != nil {
		return err
	}

	reply, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	reply = strings.TrimSpace(reply)
	if strings.HasPrefix(reply, "MESSAGE_STORED ") || strings.HasPrefix(reply, "MESSAGE_KNOWN ") {
		return nil
	}

	return fmt.Errorf("unexpected send reply: %s", reply)
}

func (c *DesktopClient) localAddress() string {
	if strings.HasPrefix(c.nodeCfg.ListenAddress, ":") {
		return "127.0.0.1" + c.nodeCfg.ListenAddress
	}
	return c.nodeCfg.ListenAddress
}

func peerID(index int) string {
	return fmt.Sprintf("bootstrap-%d", index)
}

func parsePeers(line string) ([]string, error) {
	const prefix = "PEERS "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected peers response: %s", line)
	}

	fields := strings.Fields(line)
	for _, field := range fields {
		if strings.HasPrefix(field, "list=") {
			value := strings.TrimPrefix(field, "list=")
			if value == "" {
				return nil, nil
			}
			return strings.Split(value, ","), nil
		}
	}

	return nil, fmt.Errorf("missing peer list in response: %s", line)
}

func parseIdentities(line string) ([]string, error) {
	const prefix = "IDENTITIES "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected identities response: %s", line)
	}

	fields := strings.Fields(line)
	for _, field := range fields {
		if strings.HasPrefix(field, "list=") {
			value := strings.TrimPrefix(field, "list=")
			if value == "" {
				return nil, nil
			}
			return strings.Split(value, ","), nil
		}
	}

	return nil, fmt.Errorf("missing identity list in response: %s", line)
}

func parseContacts(line string) (map[string]Contact, error) {
	const prefix = "CONTACTS "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected contacts response: %s", line)
	}

	index := strings.Index(line, "list=")
	if index == -1 {
		return nil, fmt.Errorf("missing contact list in response: %s", line)
	}

	value := strings.TrimPrefix(line[index:], "list=")
	if value == "" {
		return map[string]Contact{}, nil
	}

	items := strings.Split(value, ",")
	out := make(map[string]Contact, len(items))
	for _, item := range items {
		parts := strings.SplitN(item, "@", 4)
		if len(parts) != 4 {
			continue
		}
		out[parts[0]] = Contact{
			BoxKey:       parts[1],
			PubKey:       parts[2],
			BoxSignature: parts[3],
		}
	}

	return out, nil
}

func parseMessages(line string) ([]string, error) {
	const prefix = "MESSAGES "
	return parseListField(line, prefix)
}

func parseInbox(line string) ([]string, error) {
	const prefix = "INBOX "
	return parseListField(line, prefix)
}

func parseWelcomeField(line, key string) string {
	prefix := key + "="
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, prefix) {
			return strings.TrimPrefix(field, prefix)
		}
	}
	return ""
}

func parseCSVField(line, key string) []string {
	value := parseWelcomeField(line, key)
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func parseListField(line, prefix string) ([]string, error) {
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected response: %s", line)
	}

	index := strings.Index(line, "list=")
	if index == -1 {
		return nil, fmt.Errorf("missing list field in response: %s", line)
	}

	value := strings.TrimPrefix(line[index:], "list=")
	if value == "" {
		return nil, nil
	}

	return strings.Split(value, "|"), nil
}

func decryptNotices(id *identity.Identity, line string) ([]string, error) {
	const prefix = "NOTICES "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("unexpected notices response: %s", line)
	}

	fields := strings.Fields(line)
	for _, field := range fields {
		if strings.HasPrefix(field, "list=") {
			value := strings.TrimPrefix(field, "list=")
			if value == "" {
				return nil, nil
			}

			items := strings.Split(value, ",")
			out := make([]string, 0, len(items))
			for _, item := range items {
				parts := strings.SplitN(item, "@", 3)
				if len(parts) != 3 {
					continue
				}

				notice, err := gazeta.DecryptForIdentity(id, parts[2])
				if err != nil {
					continue
				}

				out = append(out, notice.From+">"+notice.Body)
			}
			return out, nil
		}
	}

	return nil, fmt.Errorf("missing notice list in response: %s", line)
}

func decryptDirectMessages(id *identity.Identity, contacts map[string]Contact, messages []string) []string {
	out := make([]string, 0, len(messages))
	for _, item := range messages {
		parts := strings.SplitN(item, ">", 3)
		if len(parts) != 3 {
			continue
		}

		sender := parts[0]
		recipient := parts[1]
		ciphertext := parts[2]

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
