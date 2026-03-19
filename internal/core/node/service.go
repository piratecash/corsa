package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
	"corsa/internal/core/transport"
)

type Service struct {
	identity *identity.Identity
	cfg      config.Node
	trust    *trustStore
	mu       sync.RWMutex
	peers    []transport.Peer
	known    map[string]struct{}
	boxKeys  map[string]string
	pubKeys  map[string]string
	topics   map[string][]protocol.Envelope
	notices  map[string]gazeta.Notice
	seen     map[string]struct{}
	listener net.Listener
}

type incomingMessage struct {
	ID         protocol.MessageID
	Topic      string
	Sender     string
	Recipient  string
	Flag       protocol.MessageFlag
	CreatedAt  time.Time
	TTLSeconds int
	Body       string
}

func NewService(cfg config.Node, id *identity.Identity) *Service {
	peers := make([]transport.Peer, 0, len(cfg.BootstrapPeers))
	for i, addr := range cfg.BootstrapPeers {
		peers = append(peers, transport.Peer{
			ID:      fmt.Sprintf("bootstrap-%d", i),
			Address: addr,
		})
	}

	selfContact := trustedContact{
		Address:      id.Address,
		PubKey:       identity.PublicKeyBase64(id.PublicKey),
		BoxKey:       identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSignature: identity.SignBoxKeyBinding(id),
		Source:       "self",
	}
	trust, err := loadTrustStore(cfg.TrustStorePath, selfContact)
	if err != nil {
		panic(err)
	}

	known := map[string]struct{}{}
	boxKeys := map[string]string{}
	pubKeys := map[string]string{}
	for address, contact := range trust.trustedContacts() {
		known[address] = struct{}{}
		boxKeys[address] = contact.BoxKey
		pubKeys[address] = contact.PubKey
	}

	return &Service{
		identity: id,
		cfg:      cfg,
		trust:    trust,
		peers:    peers,
		known:    known,
		boxKeys:  boxKeys,
		pubKeys:  pubKeys,
		topics:   make(map[string][]protocol.Envelope),
		notices:  make(map[string]gazeta.Notice),
		seen:     make(map[string]struct{}),
	}
}

func (s *Service) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.ListenAddress, err)
	}
	defer func() { _ = listener.Close() }()

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go s.bootstrapLoop(ctx)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			return fmt.Errorf("accept connection: %w", err)
		}

		go s.handleConn(conn)
	}
}

func (s *Service) ListenAddress() string {
	return s.cfg.ListenAddress
}

func (s *Service) AdvertiseAddress() string {
	return s.cfg.AdvertiseAddress
}

func (s *Service) NodeType() config.NodeType {
	return s.cfg.NormalizedType()
}

func (s *Service) Services() []string {
	return s.cfg.ServiceList()
}

func (s *Service) ClientVersion() string {
	if strings.TrimSpace(s.cfg.ClientVersion) == "" {
		return "0.2-alpha"
	}
	return s.cfg.ClientVersion
}

func (s *Service) CanForward() bool {
	return s.NodeType() == config.NodeTypeFull
}

func (s *Service) Address() string {
	return s.identity.Address
}

func (s *Service) Peers() []transport.Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]transport.Peer, len(s.peers))
	copy(out, s.peers)
	return out
}

func (s *Service) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeRead})
			}
			return
		}

		if !s.handleCommand(conn, strings.TrimSpace(line)) {
			return
		}
	}
}

func (s *Service) handleCommand(conn net.Conn, line string) bool {
	if !protocol.IsJSONLine(line) {
		s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON})
		return false
	}
	return s.handleJSONCommand(conn, line)
}

func (s *Service) handleJSONCommand(conn net.Conn, line string) bool {
	frame, err := protocol.ParseFrameLine(line)
	if err != nil {
		s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON, Error: err.Error()})
		return false
	}

	switch frame.Type {
	case "ping":
		s.writeJSONFrame(conn, protocol.Frame{Type: "pong", Node: "corsa", Network: "gazeta-devnet"})
		return true
	case "hello":
		s.learnPeerFromFrame(frame)
		s.writeJSONFrame(conn, s.welcomeFrame())
		return true
	case "get_peers":
		s.writeJSONFrame(conn, s.peersFrame())
		return true
	case "fetch_identities":
		s.writeJSONFrame(conn, s.identitiesFrame())
		return true
	case "fetch_contacts":
		s.writeJSONFrame(conn, s.contactsFrame())
		return true
	case "send_message":
		s.writeJSONFrame(conn, s.storeMessageFrame(frame))
		return true
	case "fetch_messages":
		s.writeJSONFrame(conn, s.fetchMessagesFrame(frame.Topic))
		return true
	case "fetch_message_ids":
		s.writeJSONFrame(conn, s.fetchMessageIDsFrame(frame.Topic))
		return true
	case "fetch_message":
		s.writeJSONFrame(conn, s.fetchMessageFrame(frame.Topic, frame.ID))
		return true
	case "fetch_inbox":
		s.writeJSONFrame(conn, s.fetchInboxFrame(frame.Topic, frame.Recipient))
		return true
	case "publish_notice":
		s.writeJSONFrame(conn, s.publishNoticeFrame(frame))
		return true
	case "fetch_notices":
		s.writeJSONFrame(conn, s.fetchNoticesFrame())
		return true
	default:
		s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand})
		return false
	}
}

func (s *Service) writeJSONFrame(conn net.Conn, frame protocol.Frame) {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		_, _ = io.WriteString(conn, string(fallback)+"\n")
		return
	}
	_, _ = io.WriteString(conn, line)
}

func (s *Service) welcomeFrame() protocol.Frame {
	return protocol.Frame{
		Type:          "welcome",
		Version:       1,
		Node:          "corsa",
		Network:       "gazeta-devnet",
		NodeType:      string(s.NodeType()),
		ClientVersion: s.ClientVersion(),
		Services:      s.Services(),
		Address:       s.identity.Address,
		PubKey:        identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(s.identity),
	}
}

func (s *Service) peersFrame() protocol.Frame {
	peers := s.Peers()
	addresses := make([]string, 0, len(peers))
	for _, peer := range peers {
		addresses = append(addresses, peer.Address)
	}
	return protocol.Frame{
		Type:  "peers",
		Count: len(addresses),
		Peers: addresses,
	}
}

func (s *Service) identitiesFrame() protocol.Frame {
	s.mu.RLock()
	parts := make([]string, 0, len(s.known))
	for address := range s.known {
		parts = append(parts, address)
	}
	s.mu.RUnlock()

	return protocol.Frame{
		Type:       "identities",
		Count:      len(parts),
		Identities: parts,
	}
}

func (s *Service) contactsFrame() protocol.Frame {
	s.mu.RLock()
	contacts := make([]protocol.ContactFrame, 0, len(s.boxKeys))
	for address, boxKey := range s.boxKeys {
		pubKey := s.pubKeys[address]
		boxSig := ""
		if contact, ok := s.trust.trustedContacts()[address]; ok {
			boxSig = contact.BoxSignature
		}
		contacts = append(contacts, protocol.ContactFrame{
			Address: address,
			PubKey:  pubKey,
			BoxKey:  boxKey,
			BoxSig:  boxSig,
		})
	}
	s.mu.RUnlock()

	return protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	}
}

func (s *Service) storeMessageFrame(frame protocol.Frame) protocol.Frame {
	msg, err := incomingMessageFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendMessage}
	}

	stored, count, errCode := s.storeIncomingMessage(msg)
	if errCode != "" {
		return protocol.Frame{Type: "error", Code: errCode}
	}
	if !stored {
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
	}
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
}

func (s *Service) storeIncomingMessage(msg incomingMessage) (bool, int, string) {
	if err := s.validateMessageTimestamp(msg.CreatedAt); err != nil {
		return false, 0, protocol.ErrCodeMessageTimestampOutOfRange
	}

	if msg.Topic == "dm" {
		s.mu.RLock()
		senderPubKey := s.pubKeys[msg.Sender]
		s.mu.RUnlock()
		if senderPubKey == "" {
			return false, 0, protocol.ErrCodeUnknownSenderKey
		}
		if err := directmsg.VerifyEnvelope(msg.Sender, senderPubKey, msg.Recipient, msg.Body); err != nil {
			return false, 0, protocol.ErrCodeInvalidDirectMessageSig
		}
	}

	s.cleanupExpiredMessages()

	s.mu.Lock()
	s.known[msg.Sender] = struct{}{}
	if msg.Recipient != "*" {
		s.known[msg.Recipient] = struct{}{}
	}
	if _, ok := s.seen[string(msg.ID)]; ok {
		count := len(s.topics[msg.Topic])
		s.mu.Unlock()
		return false, count, ""
	}
	s.seen[string(msg.ID)] = struct{}{}

	envelope := protocol.Envelope{
		ID:         msg.ID,
		Topic:      msg.Topic,
		Sender:     msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       msg.Flag,
		TTLSeconds: msg.TTLSeconds,
		Payload:    []byte(msg.Body),
		CreatedAt:  msg.CreatedAt,
	}

	s.topics[msg.Topic] = append(s.topics[msg.Topic], envelope)
	count := len(s.topics[msg.Topic])
	s.mu.Unlock()

	if s.CanForward() {
		go s.gossipMessage(envelope)
	}

	return true, count, ""
}

func (s *Service) fetchMessagesFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessages}
	}
	s.cleanupExpiredMessages()

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	items := make([]protocol.MessageFrame, 0, len(messages))
	for _, msg := range messages {
		items = append(items, messageFrame(msg))
	}

	return protocol.Frame{Type: "messages", Topic: topic, Count: len(items), Messages: items}
}

func (s *Service) fetchMessageIDsFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessageIDs}
	}
	s.cleanupExpiredMessages()

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, string(msg.ID))
	}

	return protocol.Frame{Type: "message_ids", Topic: topic, Count: len(ids), IDs: ids}
}

func (s *Service) fetchMessageFrame(topic, messageID string) protocol.Frame {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(messageID) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchMessage}
	}
	s.cleanupExpiredMessages()

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	for _, msg := range messages {
		if string(msg.ID) == messageID {
			item := messageFrame(msg)
			return protocol.Frame{Type: "message", Topic: topic, ID: messageID, Item: &item}
		}
	}

	return protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownMessageID}
}

func (s *Service) fetchInboxFrame(topic, recipient string) protocol.Frame {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(recipient) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchInbox}
	}
	s.cleanupExpiredMessages()

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	items := make([]protocol.MessageFrame, 0, len(messages))
	for _, msg := range messages {
		if msg.Recipient == recipient || msg.Recipient == "*" {
			items = append(items, messageFrame(msg))
		}
	}

	return protocol.Frame{Type: "inbox", Topic: topic, Recipient: recipient, Count: len(items), Messages: items}
}

func (s *Service) bootstrapLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	s.syncPeers(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupExpiredMessages()
			s.cleanupExpiredNotices()
			s.syncPeers(ctx)
		}
	}
}

func (s *Service) syncPeers(ctx context.Context) {
	for _, peer := range s.Peers() {
		address := peer.Address
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		s.syncPeer(ctx, address)
	}
}

func (s *Service) syncPeer(ctx context.Context, address string) {
	dialer := net.Dialer{Timeout: 1500 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		return
	}
	welcomeLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	welcome, err := protocol.ParseFrameLine(strings.TrimSpace(welcomeLine))
	if err != nil {
		return
	}
	s.learnIdentityFromWelcome(welcome)

	if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "get_peers"}); err == nil {
		if _, err := io.WriteString(conn, line); err != nil {
			return
		}
		reply, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
		if err == nil {
			for _, peer := range frame.Peers {
				s.addPeerAddress(peer)
			}
		}
	} else {
		return
	}

	if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "fetch_contacts"}); err == nil {
		if _, err := io.WriteString(conn, line); err != nil {
			return
		}
		contactsReply, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(contactsReply))
		if err == nil {
			for _, contact := range frame.Contacts {
				s.trustContact(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig, "contacts")
			}
		}
	}
}

func (s *Service) gossipMessage(msg protocol.Envelope) {
	for _, peer := range s.Peers() {
		address := peer.Address
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendMessageToPeer(address, msg)
	}
}

func (s *Service) publishNoticeFrame(frame protocol.Frame) protocol.Frame {
	ttl := time.Duration(frame.TTLSeconds) * time.Second
	if ttl <= 0 || strings.TrimSpace(frame.Ciphertext) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidPublishNotice}
	}

	s.cleanupExpiredNotices()

	id := gazeta.ID(frame.Ciphertext)
	expiresAt := time.Now().UTC().Add(ttl)

	s.mu.Lock()
	if existing, ok := s.notices[id]; ok && existing.ExpiresAt.After(time.Now().UTC()) {
		s.mu.Unlock()
		return protocol.Frame{Type: "notice_known", ID: string(id), ExpiresAt: existing.ExpiresAt.Unix()}
	}

	s.notices[id] = gazeta.Notice{
		ID:         id,
		Ciphertext: frame.Ciphertext,
		ExpiresAt:  expiresAt,
	}
	s.mu.Unlock()

	if s.CanForward() {
		go s.gossipNotice(ttl, frame.Ciphertext)
	}

	return protocol.Frame{Type: "notice_stored", ID: string(id), ExpiresAt: expiresAt.Unix()}
}

func (s *Service) fetchNoticesFrame() protocol.Frame {
	s.cleanupExpiredNotices()

	s.mu.RLock()
	items := make([]protocol.NoticeFrame, 0, len(s.notices))
	for _, notice := range s.notices {
		items = append(items, protocol.NoticeFrame{
			ID:         string(notice.ID),
			ExpiresAt:  notice.ExpiresAt.Unix(),
			Ciphertext: notice.Ciphertext,
		})
	}
	s.mu.RUnlock()

	return protocol.Frame{Type: "notices", Count: len(items), Notices: items}
}

func (s *Service) sendMessageToPeer(address string, msg protocol.Envelope) {
	conn, err := net.DialTimeout("tcp", address, 1500*time.Millisecond)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		return
	}
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}

	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:       "send_message",
		Topic:      msg.Topic,
		ID:         string(msg.ID),
		Address:    msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       string(msg.Flag),
		CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds: msg.TTLSeconds,
		Body:       string(msg.Payload),
	})
	if err != nil {
		return
	}
	_, _ = io.WriteString(conn, line)
	_, _ = reader.ReadString('\n')
}

func (s *Service) gossipNotice(ttl time.Duration, ciphertext string) {
	for _, peer := range s.Peers() {
		address := peer.Address
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendNoticeToPeer(address, ttl, ciphertext)
	}
}

func (s *Service) sendNoticeToPeer(address string, ttl time.Duration, ciphertext string) {
	conn, err := net.DialTimeout("tcp", address, 1500*time.Millisecond)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		return
	}
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}

	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:       "publish_notice",
		TTLSeconds: int(ttl.Seconds()),
		Ciphertext: ciphertext,
	})
	if err != nil {
		return
	}
	_, _ = io.WriteString(conn, line)
	_, _ = reader.ReadString('\n')
}

func (s *Service) nodeHelloJSONLine() string {
	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:          "hello",
		Version:       1,
		Client:        "node",
		Listen:        s.cfg.AdvertiseAddress,
		NodeType:      string(s.NodeType()),
		ClientVersion: s.ClientVersion(),
		Services:      s.Services(),
		Address:       s.identity.Address,
		PubKey:        identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(s.identity),
	})
	if err != nil {
		return ""
	}
	return line
}

func (s *Service) learnPeerFromFrame(frame protocol.Frame) {
	if frame.Listen != "" {
		s.addPeerAddress(frame.Listen)
	}
	if frame.Address != "" {
		s.addKnownIdentity(frame.Address)
	}
	s.trustContact(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig, "hello")
}

func (s *Service) learnIdentityFromWelcome(frame protocol.Frame) {
	s.trustContact(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig, "welcome")
}

func (s *Service) addPeerAddress(address string) {
	if address == "" || s.isSelfAddress(address) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, peer := range s.peers {
		if peer.Address == address {
			return
		}
	}

	s.peers = append(s.peers, transport.Peer{
		ID:      fmt.Sprintf("peer-%d", len(s.peers)),
		Address: address,
	})
}

func (s *Service) addKnownIdentity(address string) {
	if address == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.known[address] = struct{}{}
}

func (s *Service) addKnownBoxKey(address, boxKey string) {
	if address == "" || boxKey == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.boxKeys[address] = boxKey
}

func (s *Service) addKnownPubKey(address, pubKey string) {
	if address == "" || pubKey == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pubKeys[address] = pubKey
}

func (s *Service) trustContact(address, pubKey, boxKey, boxSig, source string) {
	if address == "" || pubKey == "" || boxKey == "" || boxSig == "" {
		return
	}

	if err := identity.VerifyBoxKeyBinding(address, pubKey, boxKey, boxSig); err != nil {
		return
	}

	if err := s.trust.remember(trustedContact{
		Address:      address,
		PubKey:       pubKey,
		BoxKey:       boxKey,
		BoxSignature: boxSig,
		Source:       source,
	}); err != nil {
		return
	}

	s.addKnownIdentity(address)
	s.addKnownBoxKey(address, boxKey)
	s.addKnownPubKey(address, pubKey)
}

func (s *Service) externalListenAddress() string {
	if strings.HasPrefix(s.cfg.ListenAddress, ":") {
		return "127.0.0.1" + s.cfg.ListenAddress
	}
	return s.cfg.ListenAddress
}

func (s *Service) isSelfAddress(address string) bool {
	return address == s.cfg.AdvertiseAddress || address == s.externalListenAddress() || address == s.cfg.ListenAddress
}

func (s *Service) cleanupExpiredNotices() {
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, notice := range s.notices {
		if !notice.ExpiresAt.After(now) {
			delete(s.notices, id)
		}
	}
}

func (s *Service) cleanupExpiredMessages() {
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	for topic, messages := range s.topics {
		filtered := messages[:0]
		for _, message := range messages {
			if message.Flag == protocol.MessageFlagAutoDeleteTTL && message.TTLSeconds > 0 {
				expiresAt := message.CreatedAt.Add(time.Duration(message.TTLSeconds) * time.Second)
				if !expiresAt.After(now) {
					continue
				}
			}
			filtered = append(filtered, message)
		}

		if len(filtered) == 0 {
			delete(s.topics, topic)
			continue
		}

		s.topics[topic] = filtered
	}
}

func (s *Service) validateMessageTimestamp(timestamp time.Time) error {
	now := time.Now().UTC()
	drift := s.cfg.MaxClockDrift
	if drift <= 0 {
		drift = protocol.DefaultMessageTimeDrift
	}

	if timestamp.Before(now.Add(-drift)) || timestamp.After(now.Add(drift)) {
		return fmt.Errorf("message timestamp %s outside allowed drift %s", timestamp.Format(time.RFC3339), drift)
	}

	return nil
}

func incomingMessageFromFrame(frame protocol.Frame) (incomingMessage, error) {
	timestamp, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		return incomingMessage{}, fmt.Errorf("parse message timestamp: %w", err)
	}

	msg := incomingMessage{
		ID:         protocol.MessageID(strings.TrimSpace(frame.ID)),
		Topic:      strings.TrimSpace(frame.Topic),
		Sender:     strings.TrimSpace(frame.Address),
		Recipient:  strings.TrimSpace(frame.Recipient),
		Flag:       protocol.MessageFlag(strings.TrimSpace(frame.Flag)),
		CreatedAt:  timestamp.UTC(),
		TTLSeconds: frame.TTLSeconds,
		Body:       strings.TrimSpace(frame.Body),
	}

	if msg.Topic == "" || msg.Sender == "" || msg.Recipient == "" || msg.Body == "" || msg.ID == "" || !msg.Flag.Valid() {
		return incomingMessage{}, fmt.Errorf("missing required message field")
	}

	if msg.Flag == protocol.MessageFlagAutoDeleteTTL && msg.TTLSeconds <= 0 {
		return incomingMessage{}, fmt.Errorf("ttl message requires positive ttl_seconds")
	}
	if msg.Flag != protocol.MessageFlagAutoDeleteTTL {
		msg.TTLSeconds = 0
	}

	return msg, nil
}

func messageFrame(msg protocol.Envelope) protocol.MessageFrame {
	return protocol.MessageFrame{
		ID:         string(msg.ID),
		Sender:     msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       string(msg.Flag),
		CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds: msg.TTLSeconds,
		Body:       string(msg.Payload),
	}
}
