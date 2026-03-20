package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
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
	identity     *identity.Identity
	cfg          config.Node
	trust        *trustStore
	mu           sync.RWMutex
	peers        []transport.Peer
	known        map[string]struct{}
	boxKeys      map[string]string
	pubKeys      map[string]string
	boxSigs      map[string]string
	topics       map[string][]protocol.Envelope
	receipts     map[string][]protocol.DeliveryReceipt
	notices      map[string]gazeta.Notice
	seen         map[string]struct{}
	seenReceipts map[string]struct{}
	subs         map[string]map[string]*subscriber
	sessions     map[string]*peerSession
	health       map[string]*peerHealth
	peerTypes    map[string]config.NodeType
	peerIDs      map[string]string
	pending      map[string][]pendingFrame
	pendingKeys  map[string]struct{}
	relayRetry   map[string]relayAttempt
	outbound     map[string]outboundDelivery
	upstream     map[string]struct{}
	inboundConns map[net.Conn]struct{}
	events       map[chan struct{}]struct{}
	listener     net.Listener
	lastSync     time.Time
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

type peerHealth struct {
	Address             string
	Connected           bool
	State               string
	LastConnectedAt     time.Time
	LastDisconnectedAt  time.Time
	LastPingAt          time.Time
	LastPongAt          time.Time
	LastUsefulSendAt    time.Time
	LastUsefulReceiveAt time.Time
	ConsecutiveFailures int
	LastError           string
}

type pendingFrame struct {
	Frame    protocol.Frame
	QueuedAt time.Time
	Retries  int
}

type relayAttempt struct {
	FirstSeen   time.Time
	LastAttempt time.Time
	Attempts    int
}

type outboundDelivery struct {
	MessageID     string
	Recipient     string
	Status        string
	QueuedAt      time.Time
	LastAttemptAt time.Time
	Retries       int
	Error         string
}

const (
	peerStateHealthy       = "healthy"
	peerStateDegraded      = "degraded"
	peerStateStalled       = "stalled"
	peerStateReconnecting  = "reconnecting"
	peerRequestTimeout     = 12 * time.Second
	pendingFrameTTL        = 5 * time.Minute
	relayRetryTTL          = 3 * time.Minute
	maxPendingFrameRetries = 5
)

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
	boxSigs := map[string]string{}
	for address, contact := range trust.trustedContacts() {
		known[address] = struct{}{}
		boxKeys[address] = contact.BoxKey
		pubKeys[address] = contact.PubKey
		boxSigs[address] = contact.BoxSignature
	}

	queueStatePath := cfg.EffectiveQueueStatePath()
	queueState, err := loadQueueState(queueStatePath)
	if err != nil {
		log.Printf("node: queue state load failed path=%s err=%v", queueStatePath, err)
		queueState = queueStateFile{
			Pending:       map[string][]pendingFrame{},
			RelayRetry:    map[string]relayAttempt{},
			OutboundState: map[string]outboundDelivery{},
		}
	}
	pendingKeys := make(map[string]struct{})
	for address, items := range queueState.Pending {
		for _, item := range items {
			if key := pendingFrameKey(address, item.Frame); key != "" {
				pendingKeys[key] = struct{}{}
			}
		}
	}

	topics := make(map[string][]protocol.Envelope)
	seen := make(map[string]struct{})
	for _, msg := range queueState.RelayMessages {
		topics[msg.Topic] = append(topics[msg.Topic], msg)
		if msg.ID != "" {
			seen[string(msg.ID)] = struct{}{}
		}
	}
	receipts := make(map[string][]protocol.DeliveryReceipt)
	seenReceipts := make(map[string]struct{})
	for _, receipt := range queueState.RelayReceipts {
		receipts[receipt.Recipient] = append(receipts[receipt.Recipient], receipt)
		key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status
		seenReceipts[key] = struct{}{}
	}
	sanitizeRelayState(queueState.RelayRetry, queueState.RelayMessages, queueState.RelayReceipts)

	return &Service{
		identity:     id,
		cfg:          cfg,
		trust:        trust,
		peers:        peers,
		known:        known,
		boxKeys:      boxKeys,
		pubKeys:      pubKeys,
		boxSigs:      boxSigs,
		topics:       topics,
		receipts:     receipts,
		notices:      make(map[string]gazeta.Notice),
		seen:         seen,
		seenReceipts: seenReceipts,
		subs:         make(map[string]map[string]*subscriber),
		sessions:     make(map[string]*peerSession),
		health:       make(map[string]*peerHealth),
		peerTypes:    make(map[string]config.NodeType),
		peerIDs:      make(map[string]string),
		pending:      queueState.Pending,
		pendingKeys:  pendingKeys,
		relayRetry:   queueState.RelayRetry,
		outbound:     queueState.OutboundState,
		upstream:     make(map[string]struct{}),
		inboundConns: make(map[net.Conn]struct{}),
		events:       make(map[chan struct{}]struct{}),
	}
}

func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.EffectiveListenerEnabled() {
		go s.bootstrapLoop(ctx)
		<-ctx.Done()
		return nil
	}

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

func (s *Service) SubscriberCount(recipient string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs[recipient])
}

func (s *Service) SubscribeLocalChanges() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 8)

	s.mu.Lock()
	s.events[ch] = struct{}{}
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		if _, ok := s.events[ch]; ok {
			delete(s.events, ch)
			close(ch)
		}
		s.mu.Unlock()
	}

	return ch, cancel
}

func (s *Service) Peers() []transport.Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]transport.Peer, len(s.peers))
	copy(out, s.peers)
	return out
}

func (s *Service) handleConn(conn net.Conn) {
	if !s.registerInboundConn(conn) {
		log.Printf("node: reject connection from %s reason=max-connections", conn.RemoteAddr())
		_ = conn.Close()
		return
	}
	defer func() {
		s.unregisterInboundConn(conn)
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
		if err := validateProtocolHandshake(frame); err != nil {
			s.writeJSONFrame(conn, protocol.Frame{
				Type:                   "error",
				Code:                   protocol.ErrCodeIncompatibleProtocol,
				Error:                  err.Error(),
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
			})
			return true
		}
		s.learnPeerFromFrame(conn.RemoteAddr().String(), frame)
		s.registerHelloRoute(conn, frame)
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
	case "fetch_trusted_contacts":
		s.writeJSONFrame(conn, s.trustedContactsFrame())
		return true
	case "fetch_peer_health":
		s.writeJSONFrame(conn, s.peerHealthFrame())
		return true
	case "fetch_pending_messages":
		s.writeJSONFrame(conn, s.pendingMessagesFrame(frame.Topic))
		return true
	case "import_contacts":
		s.writeJSONFrame(conn, s.importContactsFrame(frame.Contacts))
		return true
	case "send_message":
		s.writeJSONFrame(conn, s.storeMessageFrame(frame))
		return true
	case "import_message":
		s.writeJSONFrame(conn, s.importMessageFrame(frame))
		return true
	case "send_delivery_receipt":
		s.writeJSONFrame(conn, s.storeDeliveryReceiptFrame(frame))
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
	case "fetch_delivery_receipts":
		s.writeJSONFrame(conn, s.fetchDeliveryReceiptsFrame(frame.Recipient))
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
		if err := validateProtocolHandshake(frame); err != nil {
			return protocol.Frame{
				Type:                   "error",
				Code:                   protocol.ErrCodeIncompatibleProtocol,
				Error:                  err.Error(),
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
			}
		}
		return s.welcomeFrame()
	case "ping":
		return protocol.Frame{Type: "pong", Node: "corsa", Network: "gazeta-devnet"}
	case "get_peers":
		return s.peersFrame()
	case "fetch_identities":
		return s.identitiesFrame()
	case "fetch_contacts":
		return s.contactsFrame()
	case "fetch_trusted_contacts":
		return s.trustedContactsFrame()
	case "fetch_peer_health":
		return s.peerHealthFrame()
	case "fetch_pending_messages":
		return s.pendingMessagesFrame(frame.Topic)
	case "import_contacts":
		return s.importContactsFrame(frame.Contacts)
	case "send_message":
		return s.storeMessageFrame(frame)
	case "import_message":
		return s.importMessageFrame(frame)
	case "send_delivery_receipt":
		return s.storeDeliveryReceiptFrame(frame)
	case "fetch_messages":
		return s.fetchMessagesFrame(frame.Topic)
	case "fetch_message_ids":
		return s.fetchMessageIDsFrame(frame.Topic)
	case "fetch_message":
		return s.fetchMessageFrame(frame.Topic, frame.ID)
	case "fetch_inbox":
		return s.fetchInboxFrame(frame.Topic, frame.Recipient)
	case "fetch_delivery_receipts":
		return s.fetchDeliveryReceiptsFrame(frame.Recipient)
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
	listen := ""
	if s.cfg.EffectiveListenerEnabled() {
		listen = s.cfg.AdvertiseAddress
	}
	return protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Node:                   "corsa",
		Network:                "gazeta-devnet",
		Listen:                 listen,
		Listener:               listenerFlag(s.cfg.EffectiveListenerEnabled()),
		NodeType:               string(s.NodeType()),
		ClientVersion:          s.ClientVersion(),
		Services:               s.Services(),
		Address:                s.identity.Address,
		PubKey:                 identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(s.identity),
	}
}

func validateProtocolHandshake(frame protocol.Frame) error {
	if frame.Version < config.MinimumProtocolVersion {
		return fmt.Errorf("protocol version %d is too old; supported %d..%d", frame.Version, config.MinimumProtocolVersion, config.ProtocolVersion)
	}
	return nil
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
		contacts = append(contacts, protocol.ContactFrame{
			Address: address,
			PubKey:  pubKey,
			BoxKey:  boxKey,
			BoxSig:  s.boxSigs[address],
		})
	}
	s.mu.RUnlock()

	return protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	}
}

func (s *Service) trustedContactsFrame() protocol.Frame {
	trusted := s.trust.trustedContacts()

	s.mu.RLock()
	contacts := make([]protocol.ContactFrame, 0, len(trusted))
	for address, contact := range trusted {
		pubKey := s.pubKeys[address]
		if pubKey == "" {
			pubKey = contact.PubKey
		}
		boxKey := s.boxKeys[address]
		if boxKey == "" {
			boxKey = contact.BoxKey
		}
		contacts = append(contacts, protocol.ContactFrame{
			Address: address,
			PubKey:  pubKey,
			BoxKey:  boxKey,
			BoxSig:  contact.BoxSignature,
		})
	}
	s.mu.RUnlock()

	return protocol.Frame{
		Type:     "contacts",
		Count:    len(contacts),
		Contacts: contacts,
	}
}

func (s *Service) peerHealthFrame() protocol.Frame {
	return protocol.Frame{
		Type:       "peer_health",
		Count:      len(s.peerHealthFrames()),
		PeerHealth: s.peerHealthFrames(),
	}
}

func (s *Service) pendingMessagesFrame(topic string) protocol.Frame {
	if strings.TrimSpace(topic) == "" {
		topic = "dm"
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0)
	seen := make(map[string]struct{})
	items := make([]protocol.PendingMessageFrame, 0)
	for _, frames := range s.pending {
		for _, item := range frames {
			frame := item.Frame
			if frame.Topic != topic || frame.Type != "send_message" || frame.ID == "" {
				continue
			}
			if _, ok := seen[frame.ID]; ok {
				continue
			}
			seen[frame.ID] = struct{}{}
			ids = append(ids, frame.ID)
			status := pendingStatusFromFrame(item)
			items = append(items, protocol.PendingMessageFrame{
				ID:            frame.ID,
				Recipient:     frame.Recipient,
				Status:        status,
				QueuedAt:      formatTime(item.QueuedAt),
				LastAttemptAt: formatTime(outboundLastAttemptLocked(s.outbound, frame.ID)),
				Retries:       outboundRetriesLocked(s.outbound, frame.ID, item.Retries),
				Error:         outboundErrorLocked(s.outbound, frame.ID),
			})
		}
	}
	for id, item := range s.outbound {
		if item.Status == "" || item.Status == "sent" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		items = append(items, protocol.PendingMessageFrame{
			ID:            id,
			Recipient:     item.Recipient,
			Status:        item.Status,
			QueuedAt:      formatTime(item.QueuedAt),
			LastAttemptAt: formatTime(item.LastAttemptAt),
			Retries:       item.Retries,
			Error:         item.Error,
		})
	}
	sort.Strings(ids)
	sort.Slice(items, func(i, j int) bool {
		if items[i].ID == items[j].ID {
			return items[i].Status < items[j].Status
		}
		return items[i].ID < items[j].ID
	})
	return protocol.Frame{Type: "pending_messages", Topic: topic, Count: len(ids), PendingIDs: ids, PendingMessages: items}
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

func (s *Service) registerHelloRoute(conn net.Conn, frame protocol.Frame) {
	if strings.TrimSpace(frame.Client) != "node" {
		return
	}

	recipient := strings.TrimSpace(frame.Address)
	if recipient == "" {
		return
	}

	subID := "node-route:" + conn.RemoteAddr().String()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}

	existing, exists := s.subs[recipient][subID]
	if exists && existing.conn == conn {
		return
	}

	s.subs[recipient][subID] = &subscriber{
		id:        subID,
		recipient: recipient,
		conn:      conn,
	}
	log.Printf("node: route_via_hello recipient=%s subscriber=%s active=%d", recipient, subID, len(s.subs[recipient]))
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

	stored, count, errCode := s.storeIncomingMessage(msg, true)
	if errCode != "" {
		return protocol.Frame{Type: "error", Code: errCode}
	}
	if !stored {
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
	}
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
}

func (s *Service) importMessageFrame(frame protocol.Frame) protocol.Frame {
	msg, err := incomingMessageFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidImportMessage}
	}

	stored, count, errCode := s.storeIncomingMessage(msg, false)
	if errCode != "" {
		return protocol.Frame{Type: "error", Code: errCode}
	}
	if !stored {
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
	}
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, Count: count, ID: string(msg.ID)}
}

func (s *Service) storeDeliveryReceiptFrame(frame protocol.Frame) protocol.Frame {
	receipt, err := receiptFromFrame(frame)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendDeliveryReceipt}
	}

	stored, count := s.storeDeliveryReceipt(receipt)
	if !stored {
		return protocol.Frame{Type: "receipt_known", Recipient: receipt.Recipient, Count: count, ID: string(receipt.MessageID)}
	}
	return protocol.Frame{Type: "receipt_stored", Recipient: receipt.Recipient, Count: count, ID: string(receipt.MessageID)}
}

func (s *Service) storeIncomingMessage(msg incomingMessage, validateTimestamp bool) (bool, int, string) {
	if validateTimestamp {
		if err := s.validateMessageTimestamp(msg.CreatedAt); err != nil {
			return false, 0, protocol.ErrCodeMessageTimestampOutOfRange
		}
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

	s.emitLocalChange()

	log.Printf("node: stored message topic=%s id=%s from=%s to=%s flag=%s", msg.Topic, msg.ID, msg.Sender, msg.Recipient, msg.Flag)

	if s.CanForward() {
		s.trackRelayMessage(envelope)
		go s.gossipMessage(envelope)
	}
	if msg.Topic == "dm" && msg.Recipient != "*" {
		go s.pushMessageToSubscribers(envelope)
		if validateTimestamp && msg.Recipient == s.identity.Address && msg.Sender != s.identity.Address {
			go s.emitDeliveryReceipt(msg)
		}
	}

	return true, count, ""
}

func (s *Service) storeDeliveryReceipt(receipt protocol.DeliveryReceipt) (bool, int) {
	key := receipt.Recipient + ":" + string(receipt.MessageID) + ":" + receipt.Status

	s.mu.Lock()
	if _, ok := s.seenReceipts[key]; ok {
		count := len(s.receipts[receipt.Recipient])
		s.mu.Unlock()
		return false, count
	}
	s.seenReceipts[key] = struct{}{}
	s.receipts[receipt.Recipient] = append(s.receipts[receipt.Recipient], receipt)
	delete(s.outbound, string(receipt.MessageID))
	count := len(s.receipts[receipt.Recipient])
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)

	s.emitLocalChange()

	log.Printf("node: stored delivery receipt message_id=%s sender=%s recipient=%s status=%s delivered_at=%s", receipt.MessageID, receipt.Sender, receipt.Recipient, receipt.Status, receipt.DeliveredAt.Format(time.RFC3339))

	s.trackRelayReceipt(receipt)
	go s.gossipReceipt(receipt)
	go s.pushReceiptToSubscribers(receipt)

	return true, count
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

func (s *Service) fetchDeliveryReceiptsFrame(recipient string) protocol.Frame {
	if strings.TrimSpace(recipient) == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidFetchReceipts}
	}

	s.mu.RLock()
	items := append([]protocol.DeliveryReceipt(nil), s.receipts[recipient]...)
	s.mu.RUnlock()

	frames := make([]protocol.ReceiptFrame, 0, len(items))
	for _, item := range items {
		frames = append(frames, receiptFrame(item))
	}

	return protocol.Frame{
		Type:      "delivery_receipts",
		Recipient: recipient,
		Count:     len(frames),
		Receipts:  frames,
	}
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
			s.retryRelayDeliveries()
		}
	}
}

func (s *Service) ensurePeerSessions(ctx context.Context) {
	for _, address := range s.peerDialCandidates() {
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

func (s *Service) peerDialCandidates() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := s.cfg.EffectiveMaxOutgoingPeers()
	active := len(s.upstream)
	if limit > 0 && active >= limit {
		return nil
	}

	candidates := make([]string, 0, len(s.peers))
	for _, peer := range s.peers {
		address := strings.TrimSpace(peer.Address)
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if _, ok := s.upstream[address]; ok {
			continue
		}
		candidates = append(candidates, address)
		if limit > 0 && active+len(candidates) >= limit {
			break
		}
	}
	return candidates
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
				s.addPeerAddress(peer, "", "")
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
				s.addKnownIdentity(contact.Address)
				s.addKnownBoxKey(contact.Address, contact.BoxKey)
				s.addKnownPubKey(contact.Address, contact.PubKey)
				s.addKnownBoxSig(contact.Address, contact.BoxSig)
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
	s.markPeerConnected(address)

	if err := s.syncPeerSession(session); err != nil {
		return err
	}

	s.mu.Lock()
	s.sessions[address] = session
	s.mu.Unlock()
	s.flushPendingPeerFrames(address)

	return s.servePeerSession(ctx, session)
}

func (s *Service) servePeerSession(ctx context.Context, session *peerSession) error {
	syncTicker := time.NewTicker(4 * time.Second)
	pingTimer := time.NewTimer(nextHeartbeatDuration())
	defer syncTicker.Stop()
	defer pingTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-session.errCh:
			log.Printf("node: upstream subscription closed peer=%s recipient=%s err=%v", session.address, s.identity.Address, err)
			s.markPeerDisconnected(session.address, err)
			return err
		case frame := <-session.inboxCh:
			s.handlePeerSessionFrame(session.address, frame)
		case <-syncTicker.C:
			if s.peerState(session.address) == peerStateStalled {
				err := fmt.Errorf("peer session stalled")
				log.Printf("node: upstream session stalled peer=%s recipient=%s", session.address, s.identity.Address)
				s.markPeerDisconnected(session.address, err)
				return err
			}
			if err := s.syncPeerSession(session); err != nil {
				s.markPeerDisconnected(session.address, err)
				return err
			}
			s.flushPendingPeerFrames(session.address)
		case <-pingTimer.C:
			if _, err := s.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong", false); err != nil {
				log.Printf("node: upstream ping failed peer=%s recipient=%s err=%v", session.address, s.identity.Address, err)
				s.markPeerDisconnected(session.address, err)
				return err
			}
			pingTimer.Reset(nextHeartbeatDuration())
		case outbound := <-session.sendCh:
			if _, err := s.peerSessionRequest(session, outbound, expectedReplyType(outbound.Type), false); err != nil {
				log.Printf("node: peer session send failed peer=%s type=%s err=%v", session.address, outbound.Type, err)
				s.markPeerDisconnected(session.address, err)
				return err
			}
			s.clearRelayRetryForOutbound(outbound)
		}
	}
}

func (s *Service) gossipMessage(msg protocol.Envelope) {
	for _, address := range s.routingTargetsForMessage(msg) {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendMessageToPeer(address, msg)
	}
}

func (s *Service) routingTargets() []string {
	return s.routingTargetsFiltered(func(_ string, peerType config.NodeType, _ string) bool {
		return peerType != config.NodeTypeClient
	})
}

func (s *Service) routingTargetsForMessage(msg protocol.Envelope) []string {
	if msg.Topic != "dm" || msg.Recipient == "*" {
		return s.routingTargets()
	}
	return s.routingTargetsFiltered(func(_ string, peerType config.NodeType, peerID string) bool {
		return peerType != config.NodeTypeClient || peerID == msg.Recipient
	})
}

func (s *Service) routingTargetsForRecipient(recipient string) []string {
	return s.routingTargetsFiltered(func(_ string, peerType config.NodeType, peerID string) bool {
		return peerType != config.NodeTypeClient || peerID == recipient
	})
}

func (s *Service) routingTargetsFiltered(allow func(address string, peerType config.NodeType, peerID string) bool) []string {
	s.mu.RLock()
	if len(s.sessions) > 0 {
		type scoredTarget struct {
			address string
			score   int64
		}
		scored := make([]scoredTarget, 0, len(s.sessions))
		for address := range s.sessions {
			if address == "" || s.isSelfAddress(address) {
				continue
			}
			peerType := s.peerTypeForAddressLocked(address)
			peerID := s.peerIDs[address]
			if !allow(address, peerType, peerID) {
				continue
			}
			health := s.health[address]
			if health == nil || !health.Connected {
				continue
			}
			if s.computePeerStateLocked(health) == peerStateStalled {
				continue
			}
			scored = append(scored, scoredTarget{
				address: address,
				score:   scorePeerTargetLocked(health),
			})
		}
		if len(scored) > 0 {
			s.mu.RUnlock()
			sort.Slice(scored, func(i, j int) bool {
				if scored[i].score == scored[j].score {
					return scored[i].address < scored[j].address
				}
				return scored[i].score > scored[j].score
			})
			limit := min(3, len(scored))
			targets := make([]string, 0, limit)
			for _, item := range scored[:limit] {
				targets = append(targets, item.address)
			}
			return targets
		}
	}
	s.mu.RUnlock()

	targets := make([]string, 0, len(s.Peers()))
	for _, peer := range s.Peers() {
		address := peer.Address
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if !allow(address, s.peerTypeForAddress(address), s.peerIdentityForAddress(address)) {
			continue
		}
		targets = append(targets, address)
	}
	sort.Strings(targets)
	return targets
}

func scorePeerTargetLocked(health *peerHealth) int64 {
	stateWeight := int64(0)
	switch health.State {
	case peerStateHealthy:
		stateWeight = 4
	case peerStateDegraded:
		stateWeight = 2
	case peerStateReconnecting:
		stateWeight = 1
	default:
		stateWeight = 0
	}

	lastUseful := health.LastUsefulReceiveAt
	if lastUseful.IsZero() {
		lastUseful = health.LastPongAt
	}
	recency := int64(0)
	if !lastUseful.IsZero() {
		recency = lastUseful.Unix()
	}

	return stateWeight*1_000_000_000_000 + recency - int64(health.ConsecutiveFailures*1000) - int64(len(health.LastError))
}

func (s *Service) registerInboundConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := s.cfg.EffectiveMaxIncomingPeers()
	if limit > 0 && len(s.inboundConns) >= limit {
		return false
	}
	s.inboundConns[conn] = struct{}{}
	return true
}

func (s *Service) unregisterInboundConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.inboundConns, conn)
	s.mu.Unlock()
}

func (s *Service) pushMessageToSubscribers(msg protocol.Envelope) {
	subs := s.subscribersForRecipient(msg.Recipient)
	if len(subs) == 0 {
		if msg.Recipient == s.identity.Address {
			return
		}
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

func (s *Service) pushReceiptToSubscribers(receipt protocol.DeliveryReceipt) {
	subs := s.subscribersForRecipient(receipt.Recipient)
	if len(subs) == 0 {
		return
	}
	log.Printf("node: push_delivery_receipt message_id=%s recipient=%s status=%s subscribers=%d", receipt.MessageID, receipt.Recipient, receipt.Status, len(subs))

	frame := protocol.Frame{
		Type:      "push_delivery_receipt",
		Recipient: receipt.Recipient,
		Receipt: func() *protocol.ReceiptFrame {
			item := receiptFrame(receipt)
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
		s.clearOutboundQueued(frame.ID)
		log.Printf("node: route_message_attempt id=%s recipient=%s peer=%s mode=session", msg.ID, msg.Recipient, address)
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Printf("node: route_message_attempt id=%s recipient=%s peer=%s mode=queued", msg.ID, msg.Recipient, address)
		return
	}
	s.markOutboundTerminal(frame, "failed", "unable to queue outbound frame")
	log.Printf("node: route_message_attempt id=%s recipient=%s peer=%s mode=dropped", msg.ID, msg.Recipient, address)
}

func (s *Service) gossipNotice(ttl time.Duration, ciphertext string) {
	for _, address := range s.routingTargets() {
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

func (s *Service) gossipReceipt(receipt protocol.DeliveryReceipt) {
	for _, address := range s.routingTargetsForRecipient(receipt.Recipient) {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendReceiptToPeer(address, receipt)
	}
}

func (s *Service) sendReceiptToPeer(address string, receipt protocol.DeliveryReceipt) {
	frame := protocol.Frame{
		Type:        "send_delivery_receipt",
		ID:          string(receipt.MessageID),
		Address:     receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Printf("node: route_receipt_attempt message_id=%s recipient=%s peer=%s mode=session status=%s", receipt.MessageID, receipt.Recipient, address, receipt.Status)
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Printf("node: route_receipt_attempt message_id=%s recipient=%s peer=%s mode=queued status=%s", receipt.MessageID, receipt.Recipient, address, receipt.Status)
		return
	}
	log.Printf("node: route_receipt_attempt message_id=%s recipient=%s peer=%s mode=dropped status=%s", receipt.MessageID, receipt.Recipient, address, receipt.Status)
}

func (s *Service) nodeHelloJSONLine() string {
	listen := ""
	if s.cfg.EffectiveListenerEnabled() {
		listen = s.cfg.AdvertiseAddress
	}
	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		Listen:        listen,
		Listener:      listenerFlag(s.cfg.EffectiveListenerEnabled()),
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
	if listenerEnabledFromFrame(frame) {
		if normalized, ok := s.normalizePeerAddress(observedAddr, frame.Listen); ok {
			s.addPeerAddress(normalized, frame.NodeType, frame.Address)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(frame.Address)
	}
	s.addKnownBoxKey(frame.Address, frame.BoxKey)
	s.addKnownPubKey(frame.Address, frame.PubKey)
	s.addKnownBoxSig(frame.Address, frame.BoxSig)
}

func listenerEnabledFromFrame(frame protocol.Frame) bool {
	switch strings.TrimSpace(frame.Listener) {
	case "1":
		return true
	case "0":
		return false
	default:
		return strings.TrimSpace(frame.Listen) != ""
	}
}

func listenerFlag(enabled bool) string {
	if enabled {
		return "1"
	}
	return "0"
}

func (s *Service) learnIdentityFromWelcome(frame protocol.Frame) {
	if listenerEnabledFromFrame(frame) {
		if normalized, ok := s.normalizePeerAddress(frame.Listen, frame.Listen); ok {
			s.addPeerAddress(normalized, frame.NodeType, frame.Address)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(frame.Address)
	}
	s.addKnownBoxKey(frame.Address, frame.BoxKey)
	s.addKnownPubKey(frame.Address, frame.PubKey)
	s.addKnownBoxSig(frame.Address, frame.BoxSig)
}

func (s *Service) addPeerAddress(address string, nodeType string, peerID string) {
	if address == "" || s.isSelfAddress(address) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, peer := range s.peers {
		if peer.Address == address {
			s.peerTypes[address] = normalizePeerNodeType(nodeType)
			if peerID != "" {
				s.peerIDs[address] = peerID
			}
			return
		}
	}

	s.peers = append(s.peers, transport.Peer{
		ID:      fmt.Sprintf("peer-%d", len(s.peers)),
		Address: address,
	})
	s.peerTypes[address] = normalizePeerNodeType(nodeType)
	if peerID != "" {
		s.peerIDs[address] = peerID
	}
}

func normalizePeerNodeType(raw string) config.NodeType {
	switch strings.TrimSpace(raw) {
	case string(config.NodeTypeClient):
		return config.NodeTypeClient
	default:
		return config.NodeTypeFull
	}
}

func (s *Service) peerTypeForAddress(address string) config.NodeType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerTypeForAddressLocked(address)
}

func (s *Service) peerTypeForAddressLocked(address string) config.NodeType {
	if peerType, ok := s.peerTypes[address]; ok {
		return peerType
	}
	return config.NodeTypeFull
}

func (s *Service) peerIdentityForAddress(address string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerIDs[address]
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

func (s *Service) addKnownBoxSig(address, boxSig string) {
	if address == "" || boxSig == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.boxSigs[address] = boxSig
}

func (s *Service) emitLocalChange() {
	s.mu.RLock()
	subs := make([]chan struct{}, 0, len(s.events))
	for ch := range s.events {
		subs = append(subs, ch)
	}
	s.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
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
	_ = session.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if hello {
		line := s.nodeHelloJSONLine()
		s.markPeerWrite(session.address, protocol.Frame{Type: "hello"})
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	} else {
		line, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			return protocol.Frame{}, err
		}
		s.markPeerWrite(session.address, frame)
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	}
	_ = session.conn.SetWriteDeadline(time.Time{})
	_ = session.conn.SetReadDeadline(time.Now().Add(peerRequestTimeout))

	for {
		select {
		case err := <-session.errCh:
			return protocol.Frame{}, err
		case incoming := <-session.inboxCh:
			s.markPeerRead(session.address, incoming)
			if incoming.Type == "error" {
				return protocol.Frame{}, protocol.ErrorFromCode(incoming.Code)
			}
			if incoming.Type == "push_message" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "push_delivery_receipt" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if expectedType == "" || incoming.Type == expectedType {
				_ = session.conn.SetReadDeadline(time.Time{})
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
		s.addPeerAddress(peer, "", "")
	}

	contactsFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "fetch_contacts"}, "contacts", false)
	if err != nil {
		return err
	}
	for _, contact := range contactsFrame.Contacts {
		s.addKnownIdentity(contact.Address)
		s.addKnownBoxKey(contact.Address, contact.BoxKey)
		s.addKnownPubKey(contact.Address, contact.PubKey)
		s.addKnownBoxSig(contact.Address, contact.BoxSig)
	}
	return nil
}

func (s *Service) handlePeerSessionFrame(address string, frame protocol.Frame) {
	s.markPeerUsefulReceive(address)
	switch frame.Type {
	case "push_message":
		if frame.Item == nil {
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

		if stored, _, errCode := s.storeIncomingMessage(msg, true); !stored && errCode == protocol.ErrCodeUnknownSenderKey {
			refreshCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			s.syncPeer(refreshCtx, address)
			cancel()
			_, _, _ = s.storeIncomingMessage(msg, true)
		}
		log.Printf("node: received pushed message peer=%s id=%s recipient=%s", address, msg.ID, msg.Recipient)
	case "push_delivery_receipt":
		if frame.Receipt == nil {
			return
		}
		receipt, err := receiptFromReceiptFrame(*frame.Receipt)
		if err != nil {
			return
		}
		s.storeDeliveryReceipt(receipt)
		log.Printf("node: received pushed delivery receipt peer=%s message_id=%s recipient=%s status=%s", address, receipt.MessageID, receipt.Recipient, receipt.Status)
	}
}

func (s *Service) markPeerConnected(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = true
	s.updatePeerStateLocked(health, peerStateHealthy)
	health.LastConnectedAt = now
	health.LastError = ""
	health.ConsecutiveFailures = 0
}

func (s *Service) markPeerDisconnected(address string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = false
	s.updatePeerStateLocked(health, peerStateReconnecting)
	health.LastDisconnectedAt = now
	health.ConsecutiveFailures++
	if err != nil {
		health.LastError = err.Error()
	}
}

func (s *Service) markPeerWrite(address string, frame protocol.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	if frame.Type == "ping" {
		health.LastPingAt = now
	} else if frame.Type != "" {
		health.LastUsefulSendAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

func (s *Service) markPeerRead(address string, frame protocol.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	if frame.Type == "pong" {
		health.LastPongAt = now
		s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
		return
	}
	if frame.Type != "" {
		health.LastUsefulReceiveAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

func (s *Service) markPeerUsefulReceive(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	health.LastUsefulReceiveAt = time.Now().UTC()
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

func (s *Service) ensurePeerHealthLocked(address string) *peerHealth {
	health := s.health[address]
	if health == nil {
		health = &peerHealth{
			Address: address,
			State:   peerStateReconnecting,
		}
		s.health[address] = health
	}
	return health
}

func (s *Service) updatePeerStateLocked(health *peerHealth, next string) {
	if health.State == next {
		return
	}
	if health.State != "" {
		log.Printf("node: peer_state_change peer=%s from=%s to=%s pending=%d failures=%d", health.Address, health.State, next, len(s.pending[health.Address]), health.ConsecutiveFailures)
	}
	health.State = next
}

func (s *Service) peerHealthFrames() []protocol.PeerHealthFrame {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]protocol.PeerHealthFrame, 0, len(s.health))
	for _, health := range s.health {
		items = append(items, protocol.PeerHealthFrame{
			Address:             health.Address,
			State:               s.computePeerStateLocked(health),
			Connected:           health.Connected,
			PendingCount:        len(s.pending[health.Address]),
			LastConnectedAt:     formatTime(health.LastConnectedAt),
			LastDisconnectedAt:  formatTime(health.LastDisconnectedAt),
			LastPingAt:          formatTime(health.LastPingAt),
			LastPongAt:          formatTime(health.LastPongAt),
			LastUsefulSendAt:    formatTime(health.LastUsefulSendAt),
			LastUsefulReceiveAt: formatTime(health.LastUsefulReceiveAt),
			ConsecutiveFailures: health.ConsecutiveFailures,
			LastError:           health.LastError,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Address < items[j].Address
	})
	return items
}

func (s *Service) computePeerStateLocked(health *peerHealth) string {
	if !health.Connected {
		return peerStateReconnecting
	}

	now := time.Now().UTC()
	lastUseful := health.LastUsefulReceiveAt
	if lastUseful.IsZero() {
		lastUseful = health.LastPongAt
	}
	if lastUseful.IsZero() {
		return peerStateDegraded
	}

	age := now.Sub(lastUseful)
	switch {
	case age >= 60*time.Second:
		return peerStateStalled
	case age >= 25*time.Second:
		return peerStateDegraded
	default:
		return peerStateHealthy
	}
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func pendingStatusFromFrame(item pendingFrame) string {
	if item.Retries > 0 {
		return "retrying"
	}
	return "queued"
}

func outboundLastAttemptLocked(items map[string]outboundDelivery, id string) time.Time {
	if item, ok := items[id]; ok {
		return item.LastAttemptAt
	}
	return time.Time{}
}

func outboundRetriesLocked(items map[string]outboundDelivery, id string, fallback int) int {
	if item, ok := items[id]; ok && item.Retries > fallback {
		return item.Retries
	}
	return fallback
}

func outboundErrorLocked(items map[string]outboundDelivery, id string) string {
	if item, ok := items[id]; ok {
		return item.Error
	}
	return ""
}

func (s *Service) enqueuePeerFrame(address string, frame protocol.Frame) bool {
	session, ok := s.activePeerSession(address)
	if !ok {
		return false
	}
	if s.peerState(address) == peerStateStalled {
		return false
	}
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

func (s *Service) queuePeerFrame(address string, frame protocol.Frame) bool {
	key := pendingFrameKey(address, frame)
	if key == "" {
		return false
	}

	s.mu.Lock()
	if _, exists := s.pendingKeys[key]; exists {
		s.mu.Unlock()
		return true
	}
	s.pending[address] = append(s.pending[address], pendingFrame{
		Frame:    frame,
		QueuedAt: time.Now().UTC(),
	})
	s.pendingKeys[key] = struct{}{}
	s.noteOutboundQueuedLocked(frame, "")
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return true
}

func pendingFrameKey(address string, frame protocol.Frame) string {
	switch frame.Type {
	case "send_message":
		return address + "|send_message|" + frame.ID + "|" + frame.Recipient
	case "send_delivery_receipt":
		return address + "|send_delivery_receipt|" + frame.ID + "|" + frame.Recipient + "|" + frame.Status
	default:
		return ""
	}
}

func (s *Service) flushPendingPeerFrames(address string) {
	session, ok := s.activePeerSession(address)
	if !ok || session == nil {
		return
	}

	s.mu.Lock()
	frames := append([]pendingFrame(nil), s.pending[address]...)
	delete(s.pending, address)
	for _, frame := range frames {
		delete(s.pendingKeys, pendingFrameKey(address, frame.Frame))
	}
	s.mu.Unlock()

	remaining := make([]pendingFrame, 0)
	now := time.Now().UTC()
	for _, item := range frames {
		if now.Sub(item.QueuedAt) > pendingFrameTTL {
			s.markOutboundTerminal(item.Frame, "expired", "pending queue expired")
			continue
		}
		select {
		case session.sendCh <- item.Frame:
			s.clearOutboundQueued(item.Frame.ID)
		default:
			item.Retries++
			if item.Retries >= maxPendingFrameRetries {
				s.markOutboundTerminal(item.Frame, "failed", "max retries exceeded")
				continue
			}
			s.markOutboundRetrying(item.Frame, item.QueuedAt, item.Retries, "retry queued delivery")
			remaining = append(remaining, item)
		}
	}
	if len(remaining) == 0 {
		snapshot := s.queueStateSnapshot()
		s.persistQueueState(snapshot)
		return
	}

	s.mu.Lock()
	s.pending[address] = append(s.pending[address], remaining...)
	for _, item := range remaining {
		s.pendingKeys[pendingFrameKey(address, item.Frame)] = struct{}{}
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func (s *Service) retryRelayDeliveries() {
	if !s.CanForward() {
		return
	}

	now := time.Now().UTC()
	for _, msg := range s.retryableRelayMessages(now) {
		log.Printf("node: relay_retry_message id=%s recipient=%s attempts=%d", msg.ID, msg.Recipient, s.noteRelayAttempt(relayMessageKey(msg.ID), now))
		go s.gossipMessage(msg)
	}
	for _, receipt := range s.retryableRelayReceipts(now) {
		log.Printf("node: relay_retry_receipt message_id=%s recipient=%s status=%s attempts=%d", receipt.MessageID, receipt.Recipient, receipt.Status, s.noteRelayAttempt(relayReceiptKey(receipt), now))
		go s.gossipReceipt(receipt)
	}
}

func (s *Service) retryableRelayMessages(now time.Time) []protocol.Envelope {
	s.mu.Lock()

	items := append([]protocol.Envelope(nil), s.topics["dm"]...)
	out := make([]protocol.Envelope, 0)
	beforeLen := len(s.relayRetry)
	for _, msg := range items {
		key := relayMessageKey(msg.ID)
		if msg.Recipient == "" || msg.Recipient == "*" || msg.Recipient == s.identity.Address {
			delete(s.relayRetry, key)
			continue
		}
		if s.hasReceiptForMessageLocked(msg.Sender, msg.ID) {
			delete(s.relayRetry, key)
			continue
		}
		if !shouldRetryRelayLocked(s.relayRetry, key, now) {
			continue
		}
		out = append(out, msg)
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return out
	}
	s.mu.Unlock()
	return out
}

func (s *Service) retryableRelayReceipts(now time.Time) []protocol.DeliveryReceipt {
	s.mu.Lock()

	out := make([]protocol.DeliveryReceipt, 0)
	beforeLen := len(s.relayRetry)
	for _, list := range s.receipts {
		for _, receipt := range list {
			key := relayReceiptKey(receipt)
			if receipt.Recipient == "" || receipt.Recipient == s.identity.Address {
				delete(s.relayRetry, key)
				continue
			}
			if !shouldRetryRelayLocked(s.relayRetry, key, now) {
				continue
			}
			out = append(out, receipt)
		}
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return out
	}
	s.mu.Unlock()
	return out
}

func shouldRetryRelayLocked(items map[string]relayAttempt, key string, now time.Time) bool {
	state, ok := items[key]
	if !ok {
		return true
	}
	firstSeen := state.FirstSeen
	if firstSeen.IsZero() {
		firstSeen = now
	}
	if now.Sub(firstSeen) > relayRetryTTL {
		delete(items, key)
		return false
	}
	if state.LastAttempt.IsZero() {
		return true
	}
	return now.Sub(state.LastAttempt) >= relayRetryBackoff(state.Attempts)
}

func relayRetryBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 5 * time.Second
	}
	backoff := 5 * time.Second
	for i := 1; i < attempts; i++ {
		backoff *= 2
		if backoff >= 30*time.Second {
			return 30 * time.Second
		}
	}
	return backoff
}

func (s *Service) noteRelayAttempt(key string, now time.Time) int {
	s.mu.Lock()
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		state.FirstSeen = now
	}
	state.LastAttempt = now
	state.Attempts++
	s.relayRetry[key] = state
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return state.Attempts
}

func (s *Service) trackRelayMessage(msg protocol.Envelope) {
	if msg.Topic != "dm" || msg.Recipient == "" || msg.Recipient == "*" {
		return
	}
	s.mu.Lock()
	key := relayMessageKey(msg.ID)
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		state.FirstSeen = time.Now().UTC()
		s.relayRetry[key] = state
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return
	}
	s.mu.Unlock()
}

func (s *Service) trackRelayReceipt(receipt protocol.DeliveryReceipt) {
	s.mu.Lock()
	key := relayReceiptKey(receipt)
	state := s.relayRetry[key]
	dirty := false
	if state.FirstSeen.IsZero() {
		state.FirstSeen = time.Now().UTC()
		s.relayRetry[key] = state
		dirty = true
	}
	if _, ok := s.relayRetry[relayMessageKey(receipt.MessageID)]; ok {
		delete(s.relayRetry, relayMessageKey(receipt.MessageID))
		dirty = true
	}
	if !dirty {
		s.mu.Unlock()
		return
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func relayMessageKey(id protocol.MessageID) string {
	return "msg|" + string(id)
}

func relayReceiptKey(receipt protocol.DeliveryReceipt) string {
	return "receipt|" + receipt.Recipient + "|" + string(receipt.MessageID) + "|" + receipt.Status
}

func (s *Service) hasReceiptForMessageLocked(originalSender string, messageID protocol.MessageID) bool {
	for _, receipt := range s.receipts[originalSender] {
		if receipt.MessageID == messageID {
			return true
		}
	}
	return false
}

func (s *Service) queueStateSnapshot() queueStateFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queueStateSnapshotLocked()
}

func (s *Service) queueStateSnapshotLocked() queueStateFile {
	pending := make(map[string][]pendingFrame, len(s.pending))
	for address, items := range s.pending {
		if len(items) == 0 {
			continue
		}
		frames := make([]pendingFrame, len(items))
		copy(frames, items)
		pending[address] = frames
	}

	relayRetry := make(map[string]relayAttempt, len(s.relayRetry))
	for key, item := range s.relayRetry {
		relayRetry[key] = item
	}
	relayMessages := make([]protocol.Envelope, 0, len(s.topics["dm"]))
	for _, msg := range s.topics["dm"] {
		if _, ok := relayRetry[relayMessageKey(msg.ID)]; ok {
			relayMessages = append(relayMessages, msg)
		}
	}
	relayReceipts := make([]protocol.DeliveryReceipt, 0)
	for _, list := range s.receipts {
		for _, receipt := range list {
			if _, ok := relayRetry[relayReceiptKey(receipt)]; ok {
				relayReceipts = append(relayReceipts, receipt)
			}
		}
	}
	outbound := make(map[string]outboundDelivery, len(s.outbound))
	for key, item := range s.outbound {
		outbound[key] = item
	}

	return queueStateFile{
		Pending:       pending,
		RelayRetry:    relayRetry,
		RelayMessages: relayMessages,
		RelayReceipts: relayReceipts,
		OutboundState: outbound,
	}
}

func (s *Service) persistQueueState(snapshot queueStateFile) {
	path := s.cfg.EffectiveQueueStatePath()
	if err := saveQueueState(path, snapshot); err != nil {
		log.Printf("node: queue state save failed path=%s err=%v", path, err)
	}
}

func sanitizeRelayState(items map[string]relayAttempt, messages []protocol.Envelope, receipts []protocol.DeliveryReceipt) {
	valid := make(map[string]struct{}, len(messages)+len(receipts))
	for _, msg := range messages {
		valid[relayMessageKey(msg.ID)] = struct{}{}
	}
	for _, receipt := range receipts {
		valid[relayReceiptKey(receipt)] = struct{}{}
	}
	for key := range items {
		if _, ok := valid[key]; !ok {
			delete(items, key)
		}
	}
}

func (s *Service) noteOutboundQueuedLocked(frame protocol.Frame, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = "queued"
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) markOutboundRetrying(frame protocol.Frame, queuedAt time.Time, retries int, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	s.mu.Lock()
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
	}
	if state.QueuedAt.IsZero() {
		state.QueuedAt = queuedAt
	}
	state.Status = "retrying"
	state.Retries = retries
	state.LastAttemptAt = time.Now().UTC()
	state.Error = errText
	s.outbound[frame.ID] = state
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func (s *Service) markOutboundTerminal(frame protocol.Frame, status, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	s.mu.Lock()
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = status
	state.LastAttemptAt = time.Now().UTC()
	if status == "failed" {
		state.Retries++
	}
	state.Error = errText
	s.outbound[frame.ID] = state
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func (s *Service) clearOutboundQueued(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.outbound[messageID]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.outbound, messageID)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func (s *Service) clearRelayRetryForOutbound(frame protocol.Frame) {
	if frame.Type != "send_delivery_receipt" || frame.ID == "" || frame.Recipient == "" || frame.Status == "" {
		return
	}

	key := relayReceiptKey(protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(frame.ID),
		Recipient: frame.Recipient,
		Status:    frame.Status,
	})

	s.mu.Lock()
	if _, ok := s.relayRetry[key]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.relayRetry, key)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
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

func (s *Service) activePeerSession(address string) (*peerSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session := s.sessions[address]
	if session == nil {
		return nil, false
	}
	health := s.health[address]
	if health == nil || !health.Connected {
		return nil, false
	}
	return session, true
}

func (s *Service) peerState(address string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	health := s.health[address]
	if health == nil {
		return peerStateReconnecting
	}
	return s.computePeerStateLocked(health)
}

func nextHeartbeatDuration() time.Duration {
	base := 15 * time.Second
	jitter := time.Duration(time.Now().UTC().UnixNano()%7) * time.Second
	return base + jitter
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
	if !s.cfg.EffectiveListenerEnabled() {
		return ""
	}
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
		// The observed remote port is an ephemeral source port, not a
		// stable listening endpoint. Without a valid advertised port we
		// should not learn a dialable peer address from RemoteAddr alone.
		return "", false
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

func (s *Service) emitDeliveryReceipt(msg incomingMessage) {
	receipt := protocol.DeliveryReceipt{
		MessageID:   msg.ID,
		Sender:      s.identity.Address,
		Recipient:   msg.Sender,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
	s.storeDeliveryReceipt(receipt)
}

func receiptFrame(receipt protocol.DeliveryReceipt) protocol.ReceiptFrame {
	return protocol.ReceiptFrame{
		MessageID:   string(receipt.MessageID),
		Sender:      receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
}

func receiptFromFrame(frame protocol.Frame) (protocol.DeliveryReceipt, error) {
	if strings.TrimSpace(frame.ID) == "" || strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Recipient) == "" || strings.TrimSpace(frame.Status) == "" || strings.TrimSpace(frame.DeliveredAt) == "" {
		return protocol.DeliveryReceipt{}, fmt.Errorf("missing delivery receipt fields")
	}
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}

	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}

	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.ID),
		Sender:      frame.Address,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
}

func receiptFromReceiptFrame(frame protocol.ReceiptFrame) (protocol.DeliveryReceipt, error) {
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}
	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.MessageID),
		Sender:      frame.Sender,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
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
