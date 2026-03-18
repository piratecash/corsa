package node

import (
	"bufio"
	"context"
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
		return "0.1-alpha"
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
				_, _ = io.WriteString(conn, "ERR read\n")
			}
			return
		}

		if !s.handleCommand(conn, strings.TrimSpace(line)) {
			return
		}
	}
}

func (s *Service) handleCommand(conn net.Conn, line string) bool {
	switch {
	case line == "PING":
		_, _ = io.WriteString(conn, "PONG corsa gazeta-devnet\n")
		return true
	case strings.HasPrefix(line, "HELLO "):
		s.learnPeerFromHello(line)
		_, _ = io.WriteString(conn, s.welcomeLine())
		return true
	case line == "GET_PEERS":
		_, _ = io.WriteString(conn, s.peersLine())
		return true
	case line == "FETCH_IDENTITIES":
		_, _ = io.WriteString(conn, s.identitiesLine())
		return true
	case line == "FETCH_CONTACTS":
		_, _ = io.WriteString(conn, s.contactsLine())
		return true
	case strings.HasPrefix(line, "SEND_MESSAGE "):
		_, _ = io.WriteString(conn, s.storeMessage(line))
		return true
	case strings.HasPrefix(line, "FETCH_MESSAGES "):
		_, _ = io.WriteString(conn, s.fetchMessages(line))
		return true
	case strings.HasPrefix(line, "FETCH_INBOX "):
		_, _ = io.WriteString(conn, s.fetchInbox(line))
		return true
	case strings.HasPrefix(line, "PUBLISH_NOTICE "):
		_, _ = io.WriteString(conn, s.publishNotice(line))
		return true
	case line == "FETCH_NOTICES":
		_, _ = io.WriteString(conn, s.fetchNotices())
		return true
	default:
		_, _ = io.WriteString(conn, "ERR unknown-command\n")
		return false
	}
}

func (s *Service) welcomeLine() string {
	return fmt.Sprintf(
		"WELCOME version=1 node=corsa network=gazeta-devnet node_type=%s client_version=%s services=%s address=%s pubkey=%s boxkey=%s boxsig=%s\n",
		s.NodeType(),
		s.ClientVersion(),
		strings.Join(s.Services(), ","),
		s.identity.Address,
		identity.PublicKeyBase64(s.identity.PublicKey),
		identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		identity.SignBoxKeyBinding(s.identity),
	)
}

func (s *Service) peersLine() string {
	peers := s.Peers()
	parts := make([]string, 0, len(peers))
	for _, peer := range peers {
		parts = append(parts, peer.Address)
	}

	return fmt.Sprintf("PEERS count=%d list=%s\n", len(parts), strings.Join(parts, ","))
}

func (s *Service) identitiesLine() string {
	s.mu.RLock()
	parts := make([]string, 0, len(s.known))
	for address := range s.known {
		parts = append(parts, address)
	}
	s.mu.RUnlock()

	return fmt.Sprintf("IDENTITIES count=%d list=%s\n", len(parts), strings.Join(parts, ","))
}

func (s *Service) contactsLine() string {
	s.mu.RLock()
	parts := make([]string, 0, len(s.boxKeys))
	for address, boxKey := range s.boxKeys {
		pubKey := s.pubKeys[address]
		boxSig := ""
		if contact, ok := s.trust.trustedContacts()[address]; ok {
			boxSig = contact.BoxSignature
		}
		parts = append(parts, address+"@"+boxKey+"@"+pubKey+"@"+boxSig)
	}
	s.mu.RUnlock()

	return fmt.Sprintf("CONTACTS count=%d list=%s\n", len(parts), strings.Join(parts, ","))
}

func (s *Service) storeMessage(line string) string {
	parts := strings.SplitN(line, " ", 5)
	if len(parts) < 5 {
		return "ERR invalid-send-message\n"
	}

	topic := strings.TrimSpace(parts[1])
	from := strings.TrimSpace(parts[2])
	to := strings.TrimSpace(parts[3])
	body := strings.TrimSpace(parts[4])
	if topic == "" || from == "" || to == "" || body == "" {
		return "ERR invalid-send-message\n"
	}

	if topic == "dm" {
		s.mu.RLock()
		senderPubKey := s.pubKeys[from]
		s.mu.RUnlock()
		if senderPubKey == "" {
			return "ERR unknown-sender-key\n"
		}
		if err := directmsg.VerifyEnvelope(from, senderPubKey, to, body); err != nil {
			return "ERR invalid-direct-message-signature\n"
		}
	}

	key := topic + "|" + from + "|" + to + "|" + body

	s.mu.Lock()
	s.known[from] = struct{}{}
	if to != "*" {
		s.known[to] = struct{}{}
	}
	if _, ok := s.seen[key]; ok {
		count := len(s.topics[topic])
		s.mu.Unlock()
		return fmt.Sprintf("MESSAGE_KNOWN topic=%s count=%d\n", topic, count)
	}
	s.seen[key] = struct{}{}

	msg := protocol.Envelope{
		ID:        protocol.MessageID(fmt.Sprintf("%d", time.Now().UnixNano())),
		Topic:     topic,
		Sender:    from,
		Recipient: to,
		Payload:   []byte(body),
		CreatedAt: time.Now().UTC(),
	}

	s.topics[topic] = append(s.topics[topic], msg)
	count := len(s.topics[topic])
	s.mu.Unlock()

	if s.CanForward() {
		go s.gossipMessage(topic, from, to, body)
	}

	return fmt.Sprintf("MESSAGE_STORED topic=%s count=%d id=%s\n", topic, count, msg.ID)
}

func (s *Service) fetchMessages(line string) string {
	parts := strings.Fields(line)
	if len(parts) != 2 {
		return "ERR invalid-fetch-messages\n"
	}

	topic := parts[1]

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	items := make([]string, 0, len(messages))
	for _, msg := range messages {
		items = append(items, msg.Sender+">"+msg.Recipient+">"+string(msg.Payload))
	}

	return fmt.Sprintf("MESSAGES topic=%s count=%d list=%s\n", topic, len(messages), strings.Join(items, "|"))
}

func (s *Service) fetchInbox(line string) string {
	parts := strings.Fields(line)
	if len(parts) != 3 {
		return "ERR invalid-fetch-inbox\n"
	}

	topic := parts[1]
	recipient := parts[2]

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics[topic]...)
	s.mu.RUnlock()

	items := make([]string, 0, len(messages))
	for _, msg := range messages {
		if msg.Recipient == recipient || msg.Recipient == "*" {
			items = append(items, msg.Sender+">"+msg.Recipient+">"+string(msg.Payload))
		}
	}

	return fmt.Sprintf("INBOX topic=%s recipient=%s count=%d list=%s\n", topic, recipient, len(items), strings.Join(items, "|"))
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

	if _, err := io.WriteString(conn, s.nodeHelloLine()); err != nil {
		return
	}
	welcome, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	s.learnIdentityFromWelcome(strings.TrimSpace(welcome))

	if _, err := io.WriteString(conn, "GET_PEERS\n"); err != nil {
		return
	}
	reply, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	for _, peer := range parsePeerLine(strings.TrimSpace(reply)) {
		s.addPeerAddress(peer)
	}

	if _, err := io.WriteString(conn, "FETCH_CONTACTS\n"); err != nil {
		return
	}
	contactsReply, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	for address, contact := range parseContactsLine(strings.TrimSpace(contactsReply)) {
		s.trustContact(address, contact.PubKey, contact.BoxKey, contact.BoxSignature, "contacts")
	}
}

func (s *Service) gossipMessage(topic, from, to, body string) {
	for _, peer := range s.Peers() {
		address := peer.Address
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendMessageToPeer(address, topic, from, to, body)
	}
}

func (s *Service) publishNotice(line string) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return "ERR invalid-publish-notice\n"
	}

	ttl, err := time.ParseDuration(strings.TrimSpace(parts[1]) + "s")
	if err != nil || ttl <= 0 {
		return "ERR invalid-publish-notice\n"
	}

	ciphertext := strings.TrimSpace(parts[2])
	if ciphertext == "" {
		return "ERR invalid-publish-notice\n"
	}

	s.cleanupExpiredNotices()

	id := gazeta.ID(ciphertext)
	expiresAt := time.Now().UTC().Add(ttl)

	s.mu.Lock()
	if existing, ok := s.notices[id]; ok && existing.ExpiresAt.After(time.Now().UTC()) {
		s.mu.Unlock()
		return fmt.Sprintf("NOTICE_KNOWN id=%s expires=%d\n", id, existing.ExpiresAt.Unix())
	}

	s.notices[id] = gazeta.Notice{
		ID:         id,
		Ciphertext: ciphertext,
		ExpiresAt:  expiresAt,
	}
	s.mu.Unlock()

	if s.CanForward() {
		go s.gossipNotice(ttl, ciphertext)
	}

	return fmt.Sprintf("NOTICE_STORED id=%s expires=%d\n", id, expiresAt.Unix())
}

func (s *Service) fetchNotices() string {
	s.cleanupExpiredNotices()

	s.mu.RLock()
	items := make([]string, 0, len(s.notices))
	for _, notice := range s.notices {
		items = append(items, fmt.Sprintf("%s@%d@%s", notice.ID, notice.ExpiresAt.Unix(), notice.Ciphertext))
	}
	s.mu.RUnlock()

	return fmt.Sprintf("NOTICES count=%d list=%s\n", len(items), strings.Join(items, ","))
}

func (s *Service) sendMessageToPeer(address, topic, from, to, body string) {
	conn, err := net.DialTimeout("tcp", address, 1500*time.Millisecond)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloLine()); err != nil {
		return
	}
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}

	_, _ = io.WriteString(conn, fmt.Sprintf("SEND_MESSAGE %s %s %s %s\n", topic, from, to, body))
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

	if _, err := io.WriteString(conn, s.nodeHelloLine()); err != nil {
		return
	}
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}

	_, _ = io.WriteString(conn, fmt.Sprintf("PUBLISH_NOTICE %d %s\n", int(ttl.Seconds()), ciphertext))
	_, _ = reader.ReadString('\n')
}

func (s *Service) nodeHelloLine() string {
	return fmt.Sprintf(
		"HELLO version=1 client=node listen=%s node_type=%s client_version=%s services=%s address=%s pubkey=%s boxkey=%s boxsig=%s\n",
		s.cfg.AdvertiseAddress,
		s.NodeType(),
		s.ClientVersion(),
		strings.Join(s.Services(), ","),
		s.identity.Address,
		identity.PublicKeyBase64(s.identity.PublicKey),
		identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		identity.SignBoxKeyBinding(s.identity),
	)
}

func (s *Service) learnPeerFromHello(line string) {
	var address string
	var boxKey string
	var pubKey string
	var boxSig string

	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "listen=") {
			s.addPeerAddress(strings.TrimPrefix(field, "listen="))
		}
		if strings.HasPrefix(field, "address=") {
			address = strings.TrimPrefix(field, "address=")
			s.addKnownIdentity(address)
		}
		if strings.HasPrefix(field, "boxkey=") {
			boxKey = strings.TrimPrefix(field, "boxkey=")
		}
		if strings.HasPrefix(field, "pubkey=") {
			pubKey = strings.TrimPrefix(field, "pubkey=")
		}
		if strings.HasPrefix(field, "boxsig=") {
			boxSig = strings.TrimPrefix(field, "boxsig=")
		}
	}

	s.trustContact(address, pubKey, boxKey, boxSig, "hello")
}

func (s *Service) learnIdentityFromWelcome(line string) {
	var address string
	var boxKey string
	var pubKey string
	var boxSig string

	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "address=") {
			address = strings.TrimPrefix(field, "address=")
		}
		if strings.HasPrefix(field, "boxkey=") {
			boxKey = strings.TrimPrefix(field, "boxkey=")
		}
		if strings.HasPrefix(field, "pubkey=") {
			pubKey = strings.TrimPrefix(field, "pubkey=")
		}
		if strings.HasPrefix(field, "boxsig=") {
			boxSig = strings.TrimPrefix(field, "boxsig=")
		}
	}

	s.trustContact(address, pubKey, boxKey, boxSig, "welcome")
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

func parsePeerLine(line string) []string {
	fields := strings.Fields(line)
	for _, field := range fields {
		if strings.HasPrefix(field, "list=") {
			value := strings.TrimPrefix(field, "list=")
			if value == "" {
				return nil
			}
			return strings.Split(value, ",")
		}
	}
	return nil
}

type contactRecord struct {
	BoxKey       string
	PubKey       string
	BoxSignature string
}

func parseContactsLine(line string) map[string]contactRecord {
	index := strings.Index(line, "list=")
	if index == -1 {
		return nil
	}

	value := strings.TrimPrefix(line[index:], "list=")
	if value == "" {
		return nil
	}

	items := strings.Split(value, ",")
	out := make(map[string]contactRecord, len(items))
	for _, item := range items {
		parts := strings.SplitN(item, "@", 4)
		if len(parts) != 4 {
			continue
		}
		out[parts[0]] = contactRecord{
			BoxKey:       parts[1],
			PubKey:       parts[2],
			BoxSignature: parts[3],
		}
	}

	return out
}
