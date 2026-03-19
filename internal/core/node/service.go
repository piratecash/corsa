package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	subs     map[string]map[string]*subscriber
	sessions map[string]*peerSession
	upstream map[string]struct{}
	listener net.Listener
	lastSync time.Time
}

type subscriber struct {
	id        string
	recipient string
	conn      net.Conn
	mu        sync.Mutex
}

type peerSession struct {
	address string
	conn    net.Conn
	sendCh  chan protocol.Frame
	inboxCh chan protocol.Frame
	errCh   chan error
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
		subs:     make(map[string]map[string]*subscriber),
		sessions: make(map[string]*peerSession),
		upstream: make(map[string]struct{}),
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
		return config.CorsaWireVersion
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
	defer func() {
		s.removeSubscriberConn(conn)
		_ = conn.Close()
	}()

	log.Printf("node: incoming connection from %s", conn.RemoteAddr())
	enableTCPKeepAlive(conn)

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
		s.learnPeerFromFrame(conn.RemoteAddr().String(), frame)
		if frame.Client == "node" || frame.Client == "desktop" {
			log.Printf("node: hello client=%s address=%s listen=%s node_type=%s version=%s", frame.Client, frame.Address, frame.Listen, frame.NodeType, frame.ClientVersion)
		}
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
	case "import_contacts":
		s.writeJSONFrame(conn, s.importContactsFrame(frame.Contacts))
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
	case "subscribe_inbox":
		s.writeJSONFrame(conn, s.subscribeInboxFrame(conn, frame))
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

func (s *Service) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	switch frame.Type {
	case "hello":
		return s.welcomeFrame()
	case "ping":
		return protocol.Frame{Type: "pong", Node: "corsa", Network: "gazeta-devnet"}
	case "get_peers":
		return s.peersFrame()
	case "fetch_identities":
		return s.identitiesFrame()
	case "fetch_contacts":
		return s.contactsFrame()
	case "import_contacts":
		return s.importContactsFrame(frame.Contacts)
	case "send_message":
		return s.storeMessageFrame(frame)
	case "fetch_messages":
		return s.fetchMessagesFrame(frame.Topic)
	case "fetch_message_ids":
		return s.fetchMessageIDsFrame(frame.Topic)
	case "fetch_message":
		return s.fetchMessageFrame(frame.Topic, frame.ID)
	case "fetch_inbox":
		return s.fetchInboxFrame(frame.Topic, frame.Recipient)
	case "publish_notice":
		return s.publishNoticeFrame(frame)
	case "fetch_notices":
		return s.fetchNoticesFrame()
	default:
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand}
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
	s.refreshKnowledgeFromPeers()

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

func (s *Service) importContactsFrame(contacts []protocol.ContactFrame) protocol.Frame {
	imported := 0
	for _, contact := range contacts {
		address := strings.TrimSpace(contact.Address)
		if address == "" || address == s.identity.Address {
			continue
		}

		before := s.trust.trustedContacts()
		_, existed := before[address]

		s.trustContact(address, contact.PubKey, contact.BoxKey, contact.BoxSig, "import_contacts")

		after := s.trust.trustedContacts()
		if _, ok := after[address]; ok && !existed {
			imported++
		}
	}

	return protocol.Frame{
		Type:  "contacts_imported",
		Count: imported,
	}
}

func (s *Service) subscribeInboxFrame(conn net.Conn, frame protocol.Frame) protocol.Frame {
	topic := strings.TrimSpace(frame.Topic)
	recipient := strings.TrimSpace(frame.Recipient)
	if topic != "dm" || recipient == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSubscribeInbox}
	}

	subID := strings.TrimSpace(frame.Subscriber)
	if subID == "" {
		subID = conn.RemoteAddr().String()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}
	s.subs[recipient][subID] = &subscriber{
		id:        subID,
		recipient: recipient,
		conn:      conn,
	}
	log.Printf("node: subscribe_inbox recipient=%s subscriber=%s active=%d", recipient, subID, len(s.subs[recipient]))

	return protocol.Frame{
		Type:       "subscribed",
		Topic:      topic,
		Recipient:  recipient,
		Subscriber: subID,
		Status:     "ok",
		Count:      len(s.subs[recipient]),
	}
}

func (s *Service) refreshKnowledgeFromPeers() {
	s.mu.RLock()
	lastSync := s.lastSync
	s.mu.RUnlock()

	// Avoid dialing upstream peers on every UI poll while still making
	// contact discovery responsive for NAT/light clients.
	if !lastSync.IsZero() && time.Since(lastSync) < 3*time.Second {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	s.ensurePeerSessions(ctx)

	s.mu.Lock()
	s.lastSync = time.Now().UTC()
	s.mu.Unlock()
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

	log.Printf("node: stored message topic=%s id=%s from=%s to=%s flag=%s", msg.Topic, msg.ID, msg.Sender, msg.Recipient, msg.Flag)

	if s.CanForward() {
		go s.gossipMessage(envelope)
	}
	if msg.Topic == "dm" && msg.Recipient != "*" {
		go s.pushMessageToSubscribers(envelope)
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

	s.ensurePeerSessions(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupExpiredMessages()
			s.cleanupExpiredNotices()
			s.ensurePeerSessions(ctx)
		}
	}
}

func (s *Service) ensurePeerSessions(ctx context.Context) {
	for _, peer := range s.Peers() {
		address := strings.TrimSpace(peer.Address)
		if address == "" || s.isSelfAddress(address) {
			continue
		}

		s.mu.Lock()
		if _, ok := s.upstream[address]; ok {
			s.mu.Unlock()
			continue
		}
		s.upstream[address] = struct{}{}
		s.mu.Unlock()

		go func(peerAddress string) {
			defer func() {
				s.mu.Lock()
				delete(s.sessions, peerAddress)
				delete(s.upstream, peerAddress)
				s.mu.Unlock()
			}()
			s.runPeerSession(ctx, peerAddress)
		}(address)
	}
}

func (s *Service) syncPeer(ctx context.Context, address string) {
	if session := s.peerSession(address); session != nil {
		_ = s.syncPeerSession(session)
		return
	}

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

func (s *Service) runPeerSession(ctx context.Context, address string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := s.openPeerSession(ctx, address); err != nil {
			s.mu.Lock()
			delete(s.sessions, address)
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		return
	}
}

func (s *Service) openPeerSession(ctx context.Context, address string) error {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	enableTCPKeepAlive(conn)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	session := &peerSession{
		address: address,
		conn:    conn,
		sendCh:  make(chan protocol.Frame, 64),
		inboxCh: make(chan protocol.Frame, 64),
		errCh:   make(chan error, 1),
	}
	go s.readPeerSession(reader, session)

	welcome, err := s.peerSessionRequest(session, protocol.Frame{}, "welcome", true)
	if err != nil {
		return err
	}
	s.learnIdentityFromWelcome(welcome)

	if _, err := s.peerSessionRequest(session, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  s.identity.Address,
		Subscriber: s.cfg.AdvertiseAddress,
	}, "subscribed", false); err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Time{})
	log.Printf("node: upstream subscription established peer=%s recipient=%s", address, s.identity.Address)

	if err := s.syncPeerSession(session); err != nil {
		return err
	}

	s.mu.Lock()
	s.sessions[address] = session
	s.mu.Unlock()

	return s.servePeerSession(ctx, session)
}

func (s *Service) servePeerSession(ctx context.Context, session *peerSession) error {
	syncTicker := time.NewTicker(4 * time.Second)
	pingTicker := time.NewTicker(20 * time.Second)
	defer syncTicker.Stop()
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-session.errCh:
			log.Printf("node: upstream subscription closed peer=%s recipient=%s err=%v", session.address, s.identity.Address, err)
			return err
		case frame := <-session.inboxCh:
			s.handlePeerSessionFrame(session.address, frame)
		case <-syncTicker.C:
			if err := s.syncPeerSession(session); err != nil {
				return err
			}
		case <-pingTicker.C:
			if _, err := s.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong", false); err != nil {
				log.Printf("node: upstream ping failed peer=%s recipient=%s err=%v", session.address, s.identity.Address, err)
				return err
			}
		case outbound := <-session.sendCh:
			if _, err := s.peerSessionRequest(session, outbound, expectedReplyType(outbound.Type), false); err != nil {
				log.Printf("node: peer session send failed peer=%s type=%s err=%v", session.address, outbound.Type, err)
				return err
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

func (s *Service) pushMessageToSubscribers(msg protocol.Envelope) {
	subs := s.subscribersForRecipient(msg.Recipient)
	if len(subs) == 0 {
		log.Printf("node: no active subscribers for recipient=%s topic=%s id=%s", msg.Recipient, msg.Topic, msg.ID)
		return
	}
	log.Printf("node: push_message id=%s topic=%s recipient=%s subscribers=%d", msg.ID, msg.Topic, msg.Recipient, len(subs))

	frame := protocol.Frame{
		Type:      "push_message",
		Topic:     msg.Topic,
		Recipient: msg.Recipient,
		Item: func() *protocol.MessageFrame {
			item := messageFrame(msg)
			return &item
		}(),
	}

	for _, sub := range subs {
		go s.writePushFrame(sub, frame)
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
	frame := protocol.Frame{
		Type:       "send_message",
		Topic:      msg.Topic,
		ID:         string(msg.ID),
		Address:    msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       string(msg.Flag),
		CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds: msg.TTLSeconds,
		Body:       string(msg.Payload),
	}
	if s.enqueuePeerFrame(address, frame) {
		return
	}

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

	line, err := protocol.MarshalFrameLine(frame)
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
	frame := protocol.Frame{
		Type:       "publish_notice",
		TTLSeconds: int(ttl.Seconds()),
		Ciphertext: ciphertext,
	}
	if s.enqueuePeerFrame(address, frame) {
		return
	}

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

	line, err := protocol.MarshalFrameLine(frame)
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

func (s *Service) learnPeerFromFrame(observedAddr string, frame protocol.Frame) {
	if normalized, ok := s.normalizePeerAddress(observedAddr, frame.Listen); ok {
		s.addPeerAddress(normalized)
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

	before := s.trust.trustedContacts()
	_, existed := before[address]

	if err := s.trust.remember(trustedContact{
		Address:      address,
		PubKey:       pubKey,
		BoxKey:       boxKey,
		BoxSignature: boxSig,
		Source:       source,
	}); err != nil {
		if err == errTrustConflict {
			log.Printf("node: trust conflict for address=%s source=%s", address, source)
		}
		return
	}

	s.addKnownIdentity(address)
	s.addKnownBoxKey(address, boxKey)
	s.addKnownPubKey(address, pubKey)
	if !existed {
		log.Printf("node: trusted new contact address=%s source=%s", address, source)
	}
}

func (s *Service) readPeerSession(reader *bufio.Reader, session *peerSession) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			select {
			case session.errCh <- err:
			default:
			}
			return
		}

		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		if err != nil {
			continue
		}

		select {
		case session.inboxCh <- frame:
		default:
			select {
			case session.errCh <- fmt.Errorf("peer session inbox overflow for %s", session.address):
			default:
			}
			return
		}
	}
}

func (s *Service) peerSessionRequest(session *peerSession, frame protocol.Frame, expectedType string, hello bool) (protocol.Frame, error) {
	if hello {
		line := s.nodeHelloJSONLine()
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	} else {
		line, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			return protocol.Frame{}, err
		}
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	}

	for {
		select {
		case err := <-session.errCh:
			return protocol.Frame{}, err
		case incoming := <-session.inboxCh:
			if incoming.Type == "error" {
				return protocol.Frame{}, protocol.ErrorFromCode(incoming.Code)
			}
			if incoming.Type == "push_message" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if expectedType == "" || incoming.Type == expectedType {
				return incoming, nil
			}
			continue
		}
	}
}

func (s *Service) syncPeerSession(session *peerSession) error {
	peersFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "get_peers"}, "peers", false)
	if err != nil {
		return err
	}
	for _, peer := range peersFrame.Peers {
		s.addPeerAddress(peer)
	}

	contactsFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "fetch_contacts"}, "contacts", false)
	if err != nil {
		return err
	}
	for _, contact := range contactsFrame.Contacts {
		s.trustContact(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig, "contacts")
	}
	return nil
}

func (s *Service) handlePeerSessionFrame(address string, frame protocol.Frame) {
	if frame.Type != "push_message" || frame.Item == nil {
		return
	}

	msg, err := incomingMessageFromFrame(protocol.Frame{
		ID:         frame.Item.ID,
		Topic:      frame.Topic,
		Address:    frame.Item.Sender,
		Recipient:  frame.Item.Recipient,
		Flag:       frame.Item.Flag,
		CreatedAt:  frame.Item.CreatedAt,
		TTLSeconds: frame.Item.TTLSeconds,
		Body:       frame.Item.Body,
	})
	if err != nil {
		return
	}

	if stored, _, errCode := s.storeIncomingMessage(msg); !stored && errCode == protocol.ErrCodeUnknownSenderKey {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		s.syncPeer(refreshCtx, address)
		cancel()
		_, _, _ = s.storeIncomingMessage(msg)
	}
	log.Printf("node: received pushed message peer=%s id=%s recipient=%s", address, msg.ID, msg.Recipient)
}

func (s *Service) enqueuePeerFrame(address string, frame protocol.Frame) bool {
	session := s.peerSession(address)
	if session == nil {
		return false
	}

	select {
	case session.sendCh <- frame:
		return true
	default:
		return false
	}
}

func expectedReplyType(requestType string) string {
	switch requestType {
	case "send_message":
		return ""
	case "publish_notice":
		return ""
	default:
		return ""
	}
}

func (s *Service) peerSession(address string) *peerSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[address]
}

func (s *Service) subscribersForRecipient(recipient string) []*subscriber {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group := s.subs[recipient]
	subs := make([]*subscriber, 0, len(group))
	for _, sub := range group {
		subs = append(subs, sub)
	}
	return subs
}

func (s *Service) writePushFrame(sub *subscriber, frame protocol.Frame) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return
	}
	if _, err := io.WriteString(sub.conn, line); err != nil {
		s.removeSubscriberByID(sub.recipient, sub.id)
	}
}

func (s *Service) removeSubscriberConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for recipient, group := range s.subs {
		for id, sub := range group {
			if sub.conn == conn {
				delete(group, id)
			}
		}
		if len(group) == 0 {
			delete(s.subs, recipient)
		}
	}
}

func (s *Service) removeSubscriberByID(recipient, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := s.subs[recipient]
	if group == nil {
		return
	}
	delete(group, id)
	if len(group) == 0 {
		delete(s.subs, recipient)
	}
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

func (s *Service) normalizePeerAddress(observedAddr, advertisedAddr string) (string, bool) {
	observedHost, _, observedOK := splitHostPort(observedAddr)
	advertisedHost, advertisedPort, advertisedOK := splitHostPort(advertisedAddr)

	switch {
	case advertisedOK && observedOK:
		observedIP := net.ParseIP(observedHost)
		advertisedIP := net.ParseIP(advertisedHost)

		if advertisedIP != nil && !isUnroutableIP(advertisedIP) {
			return net.JoinHostPort(advertisedHost, advertisedPort), true
		}
		if observedIP != nil && !isUnroutableIP(observedIP) {
			return net.JoinHostPort(observedHost, advertisedPort), true
		}
		return "", false
	case advertisedOK:
		advertisedIP := net.ParseIP(advertisedHost)
		if advertisedIP != nil && isUnroutableIP(advertisedIP) {
			return "", false
		}
		return advertisedAddr, true
	case observedOK:
		observedIP := net.ParseIP(observedHost)
		if observedIP != nil && !isUnroutableIP(observedIP) {
			return observedAddr, true
		}
	}

	return "", false
}

func splitHostPort(address string) (string, string, bool) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || host == "" || port == "" {
		return "", "", false
	}
	return host, port, true
}

func isUnroutableIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func enableTCPKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
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
