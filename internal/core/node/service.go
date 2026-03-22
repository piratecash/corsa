package node

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
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
	identity       *identity.Identity
	selfBoxSig     string // cached ed25519 signature binding identity.BoxPublicKey to identity.Address
	cfg            config.Node
	trust          *trustStore
	mu             sync.RWMutex
	peers          []transport.Peer
	known          map[string]struct{}
	boxKeys        map[string]string
	pubKeys        map[string]string
	boxSigs        map[string]string
	topics         map[string][]protocol.Envelope
	receipts       map[string][]protocol.DeliveryReceipt
	notices        map[string]gazeta.Notice
	seen           map[string]struct{}
	seenReceipts   map[string]struct{}
	subs           map[string]map[string]*subscriber
	sessions       map[string]*peerSession
	health         map[string]*peerHealth
	peerTypes      map[string]config.NodeType
	peerIDs        map[string]string
	peerVersions   map[string]string
	pending        map[string][]pendingFrame
	pendingKeys    map[string]struct{}
	orphaned       map[string][]pendingFrame // legacy fallback-keyed frames that could not be migrated
	relayRetry     map[string]relayAttempt
	outbound       map[string]outboundDelivery
	upstream       map[string]struct{}
	inboundConns   map[net.Conn]struct{}
	connAuth       map[net.Conn]*connAuthState
	connPeerInfo   map[net.Conn]*connPeerHello // inbound conn → peer info from hello frame
	bans           map[string]banEntry
	events         map[chan struct{}]struct{}
	listener       net.Listener
	lastSync       time.Time
	peersStatePath string
	lastPeerSave    time.Time
	lastPeerEvict   time.Time
	dialOrigin      map[string]string     // dial address → primary peer address (for fallback port tracking)
	persistedMeta   map[string]*peerEntry // stable metadata from peers.json, keyed by address
	observedAddrs   map[string]string     // peer identity (fingerprint) → observed IP they reported for us
	reachableGroups map[NetGroup]struct{} // network groups this node can reach (computed at startup)
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
	version int
	authOK  bool
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
	Score               int // peer quality score for persistence priority
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

type connAuthState struct {
	Hello     protocol.Frame
	Challenge string
	Verified  bool
}

// connPeerHello stores info extracted from a peer's hello frame, so that
// later frames on the same connection can look up the peer's identity and
// self-declared reachability without re-parsing the hello.
type connPeerHello struct {
	address  string              // peer's advertised listen address or identity fingerprint
	networks map[NetGroup]struct{} // self-declared reachable groups (from hello "networks" field)
}

type banEntry struct {
	Score       int
	Blacklisted time.Time
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
	banThreshold           = 1000
	banIncrementInvalidSig = 100
	banDuration            = 24 * time.Hour
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
	// Load persisted peer state and merge with bootstrap peers.
	// Bootstrap peers always appear first; persisted peers are appended
	// in score-descending order, skipping duplicates.
	peersStatePath := cfg.EffectivePeersStatePath()
	peerState, err := loadPeerState(peersStatePath)
	if err != nil {
		log.Printf("node: peer state load failed path=%s err=%v", peersStatePath, err)
		peerState = peerStateFile{Version: peerStateVersion, Peers: []peerEntry{}}
	}

	peers := make([]transport.Peer, 0, len(cfg.BootstrapPeers)+len(peerState.Peers))
	seenAddrs := make(map[string]struct{})
	for i, addr := range cfg.BootstrapPeers {
		peers = append(peers, transport.Peer{
			ID:      fmt.Sprintf("bootstrap-%d", i),
			Address: addr,
		})
		seenAddrs[addr] = struct{}{}
	}
	sortPeerEntries(peerState.Peers)
	// Index persisted entries so we can seed health from their metadata.
	persistedByAddr := make(map[string]*peerEntry, len(peerState.Peers))
	for i, entry := range peerState.Peers {
		if _, dup := seenAddrs[entry.Address]; dup {
			// Even for duplicates (bootstrap overlap) keep the metadata for health seeding.
			persistedByAddr[entry.Address] = &peerState.Peers[i]
			continue
		}
		seenAddrs[entry.Address] = struct{}{}
		persistedByAddr[entry.Address] = &peerState.Peers[i]
		peers = append(peers, transport.Peer{
			ID:      fmt.Sprintf("persisted-%d", i),
			Address: entry.Address,
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
	// One-time migration: pending queue entries written by pre-v1 code may be
	// keyed by fallback dial addresses instead of the canonical primary.  The
	// runtime only drains primary-keyed entries, so legacy keys must be
	// resolved now.  After the first save the version is bumped and this
	// block is skipped on subsequent restarts.
	if queueState.Version < queueStateVersion {
		hostPrimaries := make(map[string][]string, len(seenAddrs))
		for addr := range seenAddrs {
			if h, _, ok := splitHostPort(addr); ok {
				hostPrimaries[h] = append(hostPrimaries[h], addr)
			}
		}
		for address := range queueState.Pending {
			if _, isPrimary := seenAddrs[address]; isPrimary {
				continue
			}
			h, _, ok := splitHostPort(address)
			if !ok {
				// Malformed address — orphan to avoid silent loss.
				queueState.Orphaned[address] = append(queueState.Orphaned[address], queueState.Pending[address]...)
				delete(queueState.Pending, address)
				log.Printf("node: orphaned %d pending frames for malformed address %s", len(queueState.Orphaned[address]), address)
				continue
			}
			candidates := hostPrimaries[h]
			switch len(candidates) {
			case 0:
				// Unknown host — no known primary to migrate to.  The
				// runtime will never flush this key.  Orphan it so the
				// data is preserved on disk for manual recovery.
				queueState.Orphaned[address] = append(queueState.Orphaned[address], queueState.Pending[address]...)
				delete(queueState.Pending, address)
				log.Printf("node: orphaned %d pending frames for unknown host %s", len(queueState.Orphaned[address]), address)
			case 1:
				primary := candidates[0]
				if primary == address {
					continue
				}
				// Single candidate — move frames to the canonical primary.
				queueState.Pending[primary] = append(queueState.Pending[primary], queueState.Pending[address]...)
				delete(queueState.Pending, address)
			default:
				// Multiple primaries on the same host — ambiguous.
				// Orphan so data survives for manual recovery.
				queueState.Orphaned[address] = append(queueState.Orphaned[address], queueState.Pending[address]...)
				delete(queueState.Pending, address)
				log.Printf("node: orphaned %d pending frames for ambiguous address %s (candidates=%d)", len(queueState.Orphaned[address]), address, len(candidates))
			}
		}
		queueState.Version = queueStateVersion
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

	// Seed health map from persisted peer metadata so that scores,
	// failure counts and timestamps survive a restart+flush cycle
	// even if the peer hasn't reconnected yet.
	restoredHealth := make(map[string]*peerHealth, len(persistedByAddr))
	for addr, entry := range persistedByAddr {
		h := &peerHealth{
			Address:             addr,
			State:               peerStateReconnecting,
			ConsecutiveFailures: entry.ConsecutiveFailures,
			LastError:           entry.LastError,
			Score:               entry.Score,
		}
		if entry.LastConnectedAt != nil {
			h.LastConnectedAt = *entry.LastConnectedAt
		}
		if entry.LastDisconnectedAt != nil {
			h.LastDisconnectedAt = *entry.LastDisconnectedAt
		}
		restoredHealth[addr] = h
	}

	return &Service{
		identity:       id,
		cfg:            cfg,
		selfBoxSig:     selfContact.BoxSignature,
		trust:          trust,
		peers:          peers,
		peersStatePath: peersStatePath,
		persistedMeta:  persistedByAddr,
		known:          known,
		boxKeys:        boxKeys,
		pubKeys:        pubKeys,
		boxSigs:        boxSigs,
		topics:         topics,
		receipts:       receipts,
		notices:        make(map[string]gazeta.Notice),
		seen:           seen,
		seenReceipts:   seenReceipts,
		subs:           make(map[string]map[string]*subscriber),
		sessions:       make(map[string]*peerSession),
		health:         restoredHealth,
		peerTypes:      make(map[string]config.NodeType),
		peerIDs:        make(map[string]string),
		peerVersions:   make(map[string]string),
		pending:        queueState.Pending,
		pendingKeys:    pendingKeys,
		orphaned:       queueState.Orphaned,
		relayRetry:     queueState.RelayRetry,
		outbound:       queueState.OutboundState,
		upstream:       make(map[string]struct{}),
		dialOrigin:      make(map[string]string),
		observedAddrs:   make(map[string]string),
		reachableGroups: computeReachableGroups(cfg),
		inboundConns:   make(map[net.Conn]struct{}),
		connAuth:       make(map[net.Conn]*connAuthState),
		connPeerInfo:   make(map[net.Conn]*connPeerHello),
		bans:           make(map[string]banEntry),
		events:         make(map[chan struct{}]struct{}),
	}
}

func (s *Service) Run(ctx context.Context) error {
	bootstrapDone := make(chan struct{})
	go func() {
		s.bootstrapLoop(ctx)
		close(bootstrapDone)
	}()

	if !s.cfg.EffectiveListenerEnabled() {
		<-ctx.Done()
		<-bootstrapDone
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				<-bootstrapDone
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
	if s.isBlacklistedConn(conn) {
		_ = conn.Close()
		return
	}
	if !s.registerInboundConn(conn) {
		log.Printf("node: reject connection from %s reason=max-connections", conn.RemoteAddr())
		_ = conn.Close()
		return
	}
	defer func() {
		s.unregisterInboundConn(conn)
		s.removeSubscriberConn(conn)
		s.clearConnAuth(conn)
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

	if !s.isCommandAllowedForConn(conn, frame.Type) {
		s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired})
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
		if requiresSessionAuth(frame) {
			challenge, err := s.prepareConnAuth(conn, frame)
			if err != nil {
				s.addBanScore(conn, banIncrementInvalidSig)
				s.writeJSONFrame(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()})
				return false
			}
			s.rememberConnPeerAddr(conn, frame)
			if frame.Client == "node" || frame.Client == "desktop" {
				log.Printf("node: hello client=%s address=%s listen=%s node_type=%s version=%s", frame.Client, frame.Address, frame.Listen, frame.NodeType, frame.ClientVersion)
			}
			s.writeJSONFrame(conn, s.welcomeFrame(challenge, remoteIP(conn.RemoteAddr())))
			return true
		}
		s.learnPeerFromFrame(conn.RemoteAddr().String(), frame)
		s.registerHelloRoute(conn, frame)
		s.rememberConnPeerAddr(conn, frame)
		if frame.Client == "node" || frame.Client == "desktop" {
			log.Printf("node: hello client=%s address=%s listen=%s node_type=%s version=%s", frame.Client, frame.Address, frame.Listen, frame.NodeType, frame.ClientVersion)
		}
		s.writeJSONFrame(conn, s.welcomeFrame("", remoteIP(conn.RemoteAddr())))
		return true
	case "auth_session":
		reply, ok := s.handleAuthSessionFrame(conn, frame)
		s.writeJSONFrame(conn, reply)
		return ok
	case "ack_delete":
		reply, ok := s.handleAckDeleteFrame(conn, frame)
		s.writeJSONFrame(conn, reply)
		return ok
	case "get_peers":
		s.writeJSONFrame(conn, s.peersFrame(s.connPeerReachableGroups(conn), false))
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
		return s.welcomeFrame("", "")
	case "ping":
		return protocol.Frame{Type: "pong", Node: "corsa", Network: "gazeta-devnet"}
	case "get_peers":
		return s.peersFrame(nil, true) // local command — unfiltered
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

func (s *Service) welcomeFrame(challenge string, observedAddr string) protocol.Frame {
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
		BoxSig:                 s.selfBoxSig,
		ObservedAddress:        observedAddr,
		Challenge:              challenge,
	}
}

func validateProtocolHandshake(frame protocol.Frame) error {
	if frame.Version < config.MinimumProtocolVersion {
		return fmt.Errorf("protocol version %d is too old; supported %d..%d", frame.Version, config.MinimumProtocolVersion, config.ProtocolVersion)
	}
	return nil
}

func requiresSessionAuth(frame protocol.Frame) bool {
	return strings.TrimSpace(frame.Client) == "node" || strings.TrimSpace(frame.Client) == "desktop"
}

func (s *Service) isCommandAllowedForConn(conn net.Conn, command string) bool {
	if command == "hello" || command == "ping" || command == "auth_session" {
		return true
	}
	s.mu.RLock()
	state := s.connAuth[conn]
	s.mu.RUnlock()
	return state == nil || state.Verified
}

func (s *Service) prepareConnAuth(conn net.Conn, hello protocol.Frame) (string, error) {
	if strings.TrimSpace(hello.Address) == "" || strings.TrimSpace(hello.PubKey) == "" || strings.TrimSpace(hello.BoxKey) == "" || strings.TrimSpace(hello.BoxSig) == "" {
		return "", fmt.Errorf("missing identity fields for authenticated session")
	}
	if err := identity.VerifyBoxKeyBinding(hello.Address, hello.PubKey, hello.BoxKey, hello.BoxSig); err != nil {
		return "", err
	}
	challenge, err := randomChallenge()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.connAuth[conn] = &connAuthState{
		Hello:     hello,
		Challenge: challenge,
	}
	s.mu.Unlock()
	return challenge, nil
}

func randomChallenge() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func sessionAuthPayload(challenge, address string) []byte {
	return []byte("corsa-session-auth-v1|" + challenge + "|" + address)
}

func ackDeletePayload(address, ackType, id, status string) []byte {
	return []byte("corsa-ack-delete-v1|" + address + "|" + ackType + "|" + id + "|" + status)
}

func (s *Service) handleAuthSessionFrame(conn net.Conn, frame protocol.Frame) (protocol.Frame, bool) {
	s.mu.Lock()
	state := s.connAuth[conn]
	s.mu.Unlock()
	if state == nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired}, false
	}
	if state.Verified {
		return protocol.Frame{Type: "auth_ok", Address: state.Hello.Address, Status: "ok"}, true
	}
	if strings.TrimSpace(frame.Address) != strings.TrimSpace(state.Hello.Address) {
		s.addBanScore(conn, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: "authenticated address mismatch"}, false
	}
	if err := identity.VerifyPayload(state.Hello.Address, state.Hello.PubKey, sessionAuthPayload(state.Challenge, state.Hello.Address), frame.Signature); err != nil {
		s.addBanScore(conn, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()}, false
	}

	s.mu.Lock()
	state.Verified = true
	state.Challenge = ""
	s.connAuth[conn] = state
	s.mu.Unlock()

	s.learnPeerFromFrame(conn.RemoteAddr().String(), state.Hello)
	s.registerHelloRoute(conn, state.Hello)
	return protocol.Frame{Type: "auth_ok", Address: state.Hello.Address, Status: "ok"}, true
}

func (s *Service) authenticatedAddressForConn(conn net.Conn) (protocol.Frame, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.connAuth[conn]
	if state == nil || !state.Verified {
		return protocol.Frame{}, false
	}
	return state.Hello, true
}

func (s *Service) clearConnAuth(conn net.Conn) {
	s.mu.Lock()
	delete(s.connAuth, conn)
	delete(s.connPeerInfo, conn)
	s.mu.Unlock()
}

// rememberConnPeerAddr stores info from the peer's hello frame so that
// later frames on this connection can look up the overlay identity and
// self-declared reachable networks without relying on conn.RemoteAddr().
func (s *Service) rememberConnPeerAddr(conn net.Conn, hello protocol.Frame) {
	addr := strings.TrimSpace(hello.Listen)
	if addr == "" {
		addr = strings.TrimSpace(hello.Address)
	}
	info := &connPeerHello{
		address:  addr,
		networks: parseNetGroups(hello.Networks),
	}
	s.mu.Lock()
	s.connPeerInfo[conn] = info
	s.mu.Unlock()
}

// connPeerReachableGroups returns the set of network groups the remote
// peer can reach, for use in peer exchange filtering.
//
// Priority:
//  1. If the peer declared "networks" in its hello, validate them against
//     the advertised address and use the intersection.  This prevents a
//     clearnet peer from claiming overlay reachability to harvest .onion
//     or .i2p addresses.
//  2. Otherwise infer from the peer's advertised address (listen or identity).
//  3. If we have no usable information, return nil (= no filtering, include
//     all routable addresses).  This is the safe backward-compatible default
//     for old clients that don't send "networks".
func (s *Service) connPeerReachableGroups(conn net.Conn) map[NetGroup]struct{} {
	s.mu.RLock()
	info := s.connPeerInfo[conn]
	s.mu.RUnlock()

	if info == nil {
		return nil
	}

	// If the peer declared its networks, validate against advertised address.
	if len(info.networks) > 0 {
		return validateDeclaredNetworks(info.networks, info.address)
	}

	// Infer from advertised address if it classifies to a known routable group.
	if info.address != "" {
		g := classifyAddress(info.address)
		if g != NetGroupUnknown && g != NetGroupLocal {
			return peerReachableGroups(info.address)
		}
	}

	// No meaningful address (fingerprint or empty).  Return nil so
	// peersFrame includes all routable addresses.
	return nil
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err == nil {
		return host
	}
	return addr.String()
}

// observedAddrConsensusThreshold is the minimum number of distinct peers
// that must report the same observed IP before the node trusts it.
const observedAddrConsensusThreshold = 2

// recordObservedAddress stores the IP that a remote peer observed for our
// outbound connection.  Observations are keyed by peer identity (fingerprint),
// not by dial address, so the same node always contributes exactly one vote
// regardless of how many address aliases it has.  When enough distinct peers
// agree on the same public IP and it differs from our advertise address,
// the node logs a NAT detection event.
func (s *Service) recordObservedAddress(peerID, observedIP string) {
	if peerID == "" || observedIP == "" {
		return
	}
	ip := net.ParseIP(observedIP)
	if ip == nil {
		return
	}
	// Ignore private, loopback, and link-local addresses — they are never
	// useful as externally-visible observations.
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.observedAddrs[peerID] = observedIP

	// Count how many distinct peers agree on the same IP.
	votes := make(map[string]int, len(s.observedAddrs))
	for _, obs := range s.observedAddrs {
		votes[obs]++
	}

	best, bestCount := "", 0
	for addr, cnt := range votes {
		if cnt > bestCount {
			best, bestCount = addr, cnt
		}
	}

	if bestCount < observedAddrConsensusThreshold {
		return
	}

	// Compare with our advertise address to detect NAT.
	advHost, _, ok := splitHostPort(s.cfg.AdvertiseAddress)
	if !ok {
		return
	}
	if advHost == best {
		return // our advertise address matches what peers see — no NAT
	}
	advIP := net.ParseIP(advHost)
	if advIP != nil && !advIP.IsPrivate() && !advIP.IsLoopback() {
		return // we already advertise a public IP — don't override
	}
	log.Printf("node: NAT detected — %d peers observe our IP as %s (advertise=%s)",
		bestCount, best, s.cfg.AdvertiseAddress)
}

func (s *Service) isBlacklistedConn(conn net.Conn) bool {
	ip := remoteIP(conn.RemoteAddr())
	if ip == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.bans[ip]
	if !ok {
		return false
	}
	if !entry.Blacklisted.IsZero() && time.Now().UTC().Before(entry.Blacklisted) {
		log.Printf("node: reject connection from %s reason=blacklisted until=%s", ip, entry.Blacklisted.Format(time.RFC3339))
		return true
	}
	if !entry.Blacklisted.IsZero() && time.Now().UTC().After(entry.Blacklisted) {
		delete(s.bans, ip)
	}
	return false
}

func (s *Service) addBanScore(conn net.Conn, delta int) {
	ip := remoteIP(conn.RemoteAddr())
	if ip == "" || delta <= 0 {
		return
	}
	s.mu.Lock()
	entry := s.bans[ip]
	entry.Score += delta
	if entry.Score >= banThreshold {
		entry.Blacklisted = time.Now().UTC().Add(banDuration)
	}
	s.bans[ip] = entry
	s.mu.Unlock()
	if !entry.Blacklisted.IsZero() {
		log.Printf("node: blacklist ip=%s score=%d until=%s", ip, entry.Score, entry.Blacklisted.Format(time.RFC3339))
	}
}

// peersFrame builds a "peers" response.  When localCaller is true the
// full unfiltered peer list is returned (operator / CLI).  For remote
// peers non-routable addresses (local, unknown) are always stripped —
// they must never be relayed.  When remoteGroups is non-nil the result
// is further narrowed to groups the remote peer can reach.
func (s *Service) peersFrame(remoteGroups map[NetGroup]struct{}, localCaller bool) protocol.Frame {
	peers := s.Peers()
	addresses := make([]string, 0, len(peers))

	for _, peer := range peers {
		if !localCaller {
			g := classifyAddress(peer.Address)
			// Non-routable addresses (local/unknown) are never relayed
			// to remote peers.
			if !g.IsRoutable() {
				continue
			}
			// When the remote peer's reachable groups are known, skip
			// groups it cannot reach.
			if remoteGroups != nil {
				if _, ok := remoteGroups[g]; !ok {
					continue
				}
			}
		}
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
	items := s.peerHealthFrames()
	return protocol.Frame{
		Type:       "peer_health",
		Count:      len(items),
		PeerHealth: items,
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
	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}
	s.removeSubscriberConnLocked(recipient, conn)
	if _, ok := s.subs[recipient]; !ok {
		s.subs[recipient] = make(map[string]*subscriber)
	}
	sub := &subscriber{
		id:        subID,
		recipient: recipient,
		conn:      conn,
	}
	s.subs[recipient][subID] = sub
	count := len(s.subs[recipient])
	s.mu.Unlock()
	log.Printf("node: subscribe_inbox recipient=%s subscriber=%s active=%d", recipient, subID, count)
	go s.pushBacklogToSubscriber(sub)

	return protocol.Frame{
		Type:       "subscribed",
		Topic:      topic,
		Recipient:  recipient,
		Subscriber: subID,
		Status:     "ok",
		Count:      count,
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
	s.removeSubscriberConnLocked(recipient, conn)
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

func (s *Service) removeSubscriberConnLocked(recipient string, conn net.Conn) {
	if recipient == "" || conn == nil {
		return
	}
	subs := s.subs[recipient]
	for id, sub := range subs {
		if sub != nil && sub.conn == conn {
			delete(subs, id)
		}
	}
	if len(subs) == 0 {
		delete(s.subs, recipient)
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

func (s *Service) handleAckDeleteFrame(conn net.Conn, frame protocol.Frame) (protocol.Frame, bool) {
	hello, ok := s.authenticatedAddressForConn(conn)
	if !ok {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired}, false
	}
	if strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Address) != strings.TrimSpace(hello.Address) || strings.TrimSpace(frame.ID) == "" {
		s.addBanScore(conn, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAckDelete, Error: "invalid ack identity or id"}, false
	}
	if err := identity.VerifyPayload(hello.Address, hello.PubKey, ackDeletePayload(frame.Address, frame.AckType, frame.ID, frame.Status), frame.Signature); err != nil {
		s.addBanScore(conn, banIncrementInvalidSig)
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()}, false
	}

	count := 0
	switch strings.TrimSpace(frame.AckType) {
	case "dm":
		count = s.deleteBacklogMessageForRecipient(frame.Address, protocol.MessageID(frame.ID))
	case "receipt":
		count = s.deleteBacklogReceiptForRecipient(frame.Address, protocol.MessageID(frame.ID), frame.Status)
	default:
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAckDelete, Error: "unknown ack type"}, false
	}
	log.Printf("node: ack_delete_applied address=%s type=%s id=%s status=%s removed=%d", frame.Address, frame.AckType, frame.ID, frame.Status, count)
	return protocol.Frame{Type: "ack_deleted", AckType: frame.AckType, ID: frame.ID, Count: count, Status: "ok"}, true
}

func (s *Service) storeIncomingMessage(msg incomingMessage, validateTimestamp bool) (bool, int, string) {
	if validateTimestamp {
		if err := s.validateMessageTiming(msg); err != nil {
			return false, 0, protocol.ErrCodeMessageTimestampOutOfRange
		}
	}

	if msg.Topic == "dm" {
		s.mu.RLock()
		senderPubKey := s.pubKeys[msg.Sender]
		senderBoxKey := s.boxKeys[msg.Sender]
		senderBoxSig := s.boxSigs[msg.Sender]
		s.mu.RUnlock()
		if senderPubKey == "" {
			return false, 0, protocol.ErrCodeUnknownSenderKey
		}
		// Verify boxkey binding signature at ingest as required by encryption.md.
		if senderBoxKey != "" && senderBoxSig != "" {
			if err := identity.VerifyBoxKeyBinding(msg.Sender, senderPubKey, senderBoxKey, senderBoxSig); err != nil {
				return false, 0, protocol.ErrCodeInvalidDirectMessageSig
			}
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

	if s.shouldRouteStoredMessage(msg) {
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

func (s *Service) shouldRouteStoredMessage(msg incomingMessage) bool {
	if msg.Topic == "dm" && msg.Recipient == s.identity.Address {
		return false
	}
	if s.CanForward() {
		return true
	}
	if msg.Topic != "dm" {
		return false
	}
	if msg.Sender != s.identity.Address {
		return false
	}
	return msg.Recipient != "" && msg.Recipient != "*"
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
	s.clearPendingMessageLocked(receipt.MessageID)
	s.clearPendingReceiptLocked(receipt.MessageID, receipt.Recipient, receipt.Status)
	delete(s.relayRetry, relayMessageKey(receipt.MessageID))
	delete(s.relayRetry, relayReceiptKey(receipt))
	count := len(s.receipts[receipt.Recipient])
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)

	s.emitLocalChange()

	log.Printf("node: stored delivery receipt message_id=%s sender=%s recipient=%s status=%s delivered_at=%s", receipt.MessageID, receipt.Sender, receipt.Recipient, receipt.Status, receipt.DeliveredAt.Format(time.RFC3339))

	if receipt.Recipient == s.identity.Address {
		return true, count
	}

	s.trackRelayReceipt(receipt)
	go s.gossipReceipt(receipt)
	go s.pushReceiptToSubscribers(receipt)

	return true, count
}

func (s *Service) clearPendingMessageLocked(messageID protocol.MessageID) {
	if strings.TrimSpace(string(messageID)) == "" {
		return
	}
	for address, items := range s.pending {
		remaining := items[:0]
		for _, item := range items {
			if item.Frame.Type == "send_message" && item.Frame.ID == string(messageID) {
				delete(s.pendingKeys, pendingFrameKey(address, item.Frame))
				continue
			}
			remaining = append(remaining, item)
		}
		if len(remaining) == 0 {
			delete(s.pending, address)
			continue
		}
		s.pending[address] = append([]pendingFrame(nil), remaining...)
	}
}

func (s *Service) clearPendingReceiptLocked(messageID protocol.MessageID, recipient, status string) {
	if strings.TrimSpace(string(messageID)) == "" || strings.TrimSpace(recipient) == "" || strings.TrimSpace(status) == "" {
		return
	}
	for address, items := range s.pending {
		remaining := items[:0]
		for _, item := range items {
			if item.Frame.Type == "send_delivery_receipt" &&
				item.Frame.ID == string(messageID) &&
				item.Frame.Recipient == recipient &&
				item.Frame.Status == status {
				delete(s.pendingKeys, pendingFrameKey(address, item.Frame))
				continue
			}
			remaining = append(remaining, item)
		}
		if len(remaining) == 0 {
			delete(s.pending, address)
			continue
		}
		s.pending[address] = append([]pendingFrame(nil), remaining...)
	}
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
		if topic == "dm" && msg.Recipient == recipient {
			s.mu.RLock()
			delivered := s.hasReceiptForMessageLocked(msg.Sender, msg.ID)
			s.mu.RUnlock()
			if delivered {
				continue
			}
		}
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
			s.flushPeerState()
			return
		case <-ticker.C:
			s.cleanupExpiredMessages()
			s.cleanupExpiredNotices()
			s.evictStalePeers()
			s.ensurePeerSessions(ctx)
			s.retryRelayDeliveries()
			s.maybeSavePeerState()
		}
	}
}

// maybeSavePeerState persists peer addresses if enough time has elapsed
// since the last flush.
func (s *Service) maybeSavePeerState() {
	s.mu.RLock()
	elapsed := time.Since(s.lastPeerSave)
	s.mu.RUnlock()

	if elapsed < time.Duration(peerStateSaveMinutes)*time.Minute {
		return
	}
	s.flushPeerState()
}

// flushPeerState builds a snapshot from in-memory state and writes it to disk.
func (s *Service) flushPeerState() {
	s.mu.Lock()
	entries := s.buildPeerEntriesLocked()
	path := s.peersStatePath
	s.mu.Unlock()

	sortPeerEntries(entries)
	entries = trimPeerEntries(entries)

	state := peerStateFile{
		Version: peerStateVersion,
		Peers:   entries,
	}
	if err := savePeerState(path, state); err != nil {
		log.Printf("node: peer state save failed path=%s err=%v", path, err)
		return
	}

	s.mu.Lock()
	s.lastPeerSave = time.Now()
	s.mu.Unlock()
}

// evictStalePeers removes in-memory peers whose score has dropped below
// peerEvictScoreThreshold and that have not successfully connected within
// peerEvictStaleWindow.  Bad addresses are purged so they stop consuming dial attempts
// and make room for fresh peer-exchange discoveries.
// Bootstrap peers are never evicted — they act as permanent seeds.
func (s *Service) evictStalePeers() {
	s.mu.RLock()
	elapsed := time.Since(s.lastPeerEvict)
	s.mu.RUnlock()
	if elapsed < peerEvictInterval {
		return
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPeerEvict = now

	kept := make([]transport.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		health := s.health[peer.Address]
		if health == nil {
			// No health info yet — keep; might be a freshly discovered peer.
			kept = append(kept, peer)
			continue
		}
		// Never evict bootstrap peers.
		if peerSource(peer.ID) == "bootstrap" {
			kept = append(kept, peer)
			continue
		}
		// Never evict currently connected peers.
		if health.Connected {
			kept = append(kept, peer)
			continue
		}
		// Evict if score is terrible AND last successful connection (or
		// first discovery, if never connected) was more than staleWindow ago.
		// Importantly, LastDisconnectedAt is NOT used here — it refreshes on
		// every failed retry and would prevent eviction of perpetually-failing
		// peers.  Only LastConnectedAt (actual success) matters for eviction.
		if health.Score <= peerEvictScoreThreshold {
			lastSuccess := health.LastConnectedAt
			// If peer was never successfully connected, fall back to AddedAt
			// (the time it was first discovered via peer exchange or config).
			if lastSuccess.IsZero() {
				if pm := s.persistedMeta[peer.Address]; pm != nil && pm.AddedAt != nil {
					lastSuccess = *pm.AddedAt
				}
			}
			if !lastSuccess.IsZero() && now.Sub(lastSuccess) > peerEvictStaleWindow {
				// Evict: clean up associated state.
				delete(s.health, peer.Address)
				delete(s.peerTypes, peer.Address)
				delete(s.peerIDs, peer.Address)
				delete(s.peerVersions, peer.Address)
				delete(s.persistedMeta, peer.Address)
				continue
			}
		}
		kept = append(kept, peer)
	}
	s.peers = kept
}

// buildPeerEntriesLocked snapshots all known peers with their health metadata.
// Stable metadata (NodeType, Source, AddedAt) is read from persistedMeta so
// that values loaded from disk survive a restart+flush cycle without being
// overwritten by transient runtime state.  Only truly new peers (not yet in
// persistedMeta) derive these fields from runtime maps.
// Must be called with s.mu held (write lock required — updates persistedMeta
// for newly discovered peers).
func (s *Service) buildPeerEntriesLocked() []peerEntry {
	entries := make([]peerEntry, 0, len(s.peers))
	now := time.Now().UTC()
	for _, peer := range s.peers {
		if peer.Address == "" {
			continue
		}
		var entry peerEntry
		if pm := s.persistedMeta[peer.Address]; pm != nil {
			// Preserve stable metadata from the persisted snapshot.
			entry = peerEntry{
				Address:  peer.Address,
				NodeType: pm.NodeType,
				Source:   pm.Source,
				AddedAt:  pm.AddedAt,
			}
			// If runtime has a fresher NodeType (e.g. from a hello/welcome),
			// prefer it over the persisted value.
			if rt := string(s.peerTypes[peer.Address]); rt != "" {
				entry.NodeType = rt
			}
		} else {
			// New peer discovered at runtime — derive from live state.
			entry = peerEntry{
				Address:  peer.Address,
				NodeType: string(s.peerTypes[peer.Address]),
				Source:   peerSource(peer.ID),
				AddedAt:  &now,
			}
			// Store so that subsequent flushes are stable.
			clone := entry
			s.persistedMeta[peer.Address] = &clone
		}
		if health := s.health[peer.Address]; health != nil {
			if !health.LastConnectedAt.IsZero() {
				t := health.LastConnectedAt
				entry.LastConnectedAt = &t
			}
			if !health.LastDisconnectedAt.IsZero() {
				t := health.LastDisconnectedAt
				entry.LastDisconnectedAt = &t
			}
			entry.ConsecutiveFailures = health.ConsecutiveFailures
			entry.LastError = health.LastError
			entry.Score = health.Score
		}
		entry.Network = classifyAddress(entry.Address).String()
		entries = append(entries, entry)
	}
	return entries
}

// peerSource infers the source tag from the peer ID prefix.
func peerSource(id string) string {
	switch {
	case len(id) >= 9 && id[:9] == "bootstrap":
		return "bootstrap"
	case len(id) >= 9 && id[:9] == "persisted":
		return "persisted"
	default:
		return "peer_exchange"
	}
}

func (s *Service) ensurePeerSessions(ctx context.Context) {
	for _, candidate := range s.peerDialCandidates() {
		s.mu.Lock()
		if _, ok := s.upstream[candidate.address]; ok {
			s.mu.Unlock()
			continue
		}
		s.upstream[candidate.address] = struct{}{}
		// Record the mapping from dial address to primary peer address
		// so that health updates (markPeerConnected/Disconnected) always
		// accumulate on the primary entry, even when a fallback port is used.
		if candidate.primary != candidate.address {
			s.dialOrigin[candidate.address] = candidate.primary
		}
		s.mu.Unlock()
		go func(c peerDialCandidate) {
			defer func() {
				s.mu.Lock()
				delete(s.sessions, c.address)
				delete(s.upstream, c.address)
				delete(s.dialOrigin, c.address)
				s.mu.Unlock()
			}()
			s.runPeerSession(ctx, c.address)
		}(candidate)
	}
}

// peerDialCandidate is a scored candidate for outgoing connection attempts.
type peerDialCandidate struct {
	address string // actual address to dial (may be a fallback port variant)
	primary string // primary peer address in s.peers (health/score are tracked here)
	score   int
	index   int // insertion order for stable tie-breaking (preserves bootstrap-first ordering)
}

func (s *Service) peerDialCandidates() []peerDialCandidate {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := s.cfg.EffectiveMaxOutgoingPeers()
	active := len(s.upstream)
	if limit > 0 && active >= limit {
		return nil
	}

	now := time.Now()
	var scored []peerDialCandidate
	seen := make(map[string]struct{})
	for _, peer := range s.peers {
		primaryAddr := strings.TrimSpace(peer.Address)

		// Look up health/score/cooldown from the primary address — the one
		// stored in s.peers and tracked by markPeerConnected/Disconnected.
		// Fallback dial variants (e.g. same host with default port) share
		// the primary's reputation so that cooldown cannot be bypassed by
		// dialling an alternative port.
		primaryHealth := s.health[primaryAddr]
		peerScore := 0
		if primaryHealth != nil {
			peerScore = primaryHealth.Score
			// Exponential cooldown: skip ALL dial variants for this peer
			// while the backoff window is active.  A single failure does
			// NOT trigger cooldown — the peer gets an immediate retry
			// on the next bootstrapLoop tick.  This avoids stalling
			// reconnection when a peer was simply not started yet.
			if primaryHealth.ConsecutiveFailures > 1 && !primaryHealth.LastDisconnectedAt.IsZero() {
				cooldown := peerCooldownDuration(primaryHealth.ConsecutiveFailures - 1)
				if now.Sub(primaryHealth.LastDisconnectedAt) < cooldown {
					continue
				}
			}
		}

		for _, address := range s.dialAttemptAddressesLocked(primaryAddr) {
			if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
				continue
			}
			// Skip addresses in network groups we cannot reach (e.g.
			// .onion without a proxy, I2P without a tunnel, etc.).
			if !s.canReach(address) {
				continue
			}
			if _, ok := s.upstream[address]; ok {
				continue
			}
			if _, ok := seen[address]; ok {
				continue
			}
			seen[address] = struct{}{}
			scored = append(scored, peerDialCandidate{address: address, primary: primaryAddr, score: peerScore, index: len(scored)})
		}
	}

	// Sort by score descending so the healthiest peers
	// are dialled first and degraded peers sink to the bottom.
	// Stable tie-breaker by insertion index preserves bootstrap-first ordering.
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].index < scored[j].index
	})

	needed := len(scored)
	if limit > 0 && active+needed > limit {
		needed = limit - active
	}
	if needed > len(scored) {
		needed = len(scored)
	}
	if needed < len(scored) {
		scored = scored[:needed]
	}
	return scored
}

func (s *Service) syncPeer(ctx context.Context, address string) {
	if session := s.peerSession(address); session != nil {
		_ = s.syncPeerSession(session)
		return
	}

	conn, err := s.dialPeer(ctx, address, 1500*time.Millisecond)
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
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			return
		}
		authReply, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(authReply))
		if err != nil || frame.Type != "auth_ok" {
			return
		}
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
				// Verify box key binding before accepting peer-advertised contacts.
				if contact.Address == "" || contact.PubKey == "" || contact.BoxKey == "" || contact.BoxSig == "" {
					continue
				}
				if identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig) != nil {
					continue
				}
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

		connected, err := s.openPeerSession(ctx, address)
		if err != nil {
			s.mu.Lock()
			delete(s.sessions, address)
			s.mu.Unlock()
			// servePeerSession calls markPeerDisconnected before
			// returning its error.  For all other error paths (dial
			// failure, handshake, subscribe, sync) we must call it here
			// so that Score decreases and cooldown engages.
			if connected {
				s.mu.RLock()
				h := s.health[s.resolveHealthAddress(address)]
				stillConnected := h != nil && h.Connected
				s.mu.RUnlock()
				if stillConnected {
					s.markPeerDisconnected(address, err)
				}
			} else {
				s.markPeerDisconnected(address, err)
			}
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

func (s *Service) openPeerSession(ctx context.Context, address string) (bool, error) {
	conn, err := s.dialPeer(ctx, address, 2*time.Second)
	if err != nil {
		return false, err
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
		return false, err
	}
	session.version = welcome.Version
	s.learnIdentityFromWelcome(welcome)
	if err := s.authenticatePeerSession(session, welcome); err != nil {
		return false, err
	}
	// Record observed address only after authentication succeeds so that
	// an unauthenticated responder cannot influence NAT consensus.
	s.recordObservedAddress(welcome.Address, welcome.ObservedAddress)
	s.mu.Lock()
	s.sessions[address] = session
	s.mu.Unlock()
	s.markPeerConnected(address)

	if _, err := s.peerSessionRequest(session, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  s.identity.Address,
		Subscriber: s.cfg.AdvertiseAddress,
	}, "subscribed", false); err != nil {
		return true, err
	}
	_ = conn.SetDeadline(time.Time{})
	log.Printf("node: upstream subscription established peer=%s recipient=%s", address, s.identity.Address)

	if err := s.syncPeerSession(session); err != nil {
		return true, err
	}

	s.flushPendingPeerFrames(address)

	return true, s.servePeerSession(ctx, session)
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

func (s *Service) authenticatePeerSession(session *peerSession, welcome protocol.Frame) error {
	if strings.TrimSpace(welcome.Challenge) == "" {
		return protocol.ErrAuthRequired
	}
	reply, err := s.peerSessionRequest(session, protocol.Frame{
		Type:      "auth_session",
		Address:   s.identity.Address,
		Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
	}, "auth_ok", false)
	if err != nil {
		return err
	}
	if reply.Type != "auth_ok" {
		return protocol.ErrAuthRequired
	}
	session.authOK = true
	return nil
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
			primaryAddr := s.resolveHealthAddress(address)
			peerType := s.peerTypeForAddressLocked(primaryAddr)
			peerID := s.peerIDs[primaryAddr]
			if !allow(address, peerType, peerID) {
				continue
			}
			health := s.health[primaryAddr]
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
	delete(s.connPeerInfo, conn)
	s.mu.Unlock()
}

func (s *Service) pushMessageToSubscribers(msg protocol.Envelope) {
	if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
		return
	}
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

func (s *Service) pushBacklogToSubscriber(sub *subscriber) {
	if sub == nil || strings.TrimSpace(sub.recipient) == "" {
		return
	}

	inbox := s.fetchInboxFrame("dm", sub.recipient)
	for _, item := range inbox.Messages {
		if createdAt, err := time.Parse(time.RFC3339, item.CreatedAt); err == nil && s.messageDeliveryExpired(createdAt.UTC(), item.TTLSeconds) {
			continue
		}
		msgFrame := item
		s.writePushFrame(sub, protocol.Frame{
			Type:      "push_message",
			Topic:     "dm",
			Recipient: sub.recipient,
			Item:      &msgFrame,
		})
	}

	receipts := s.fetchDeliveryReceiptsFrame(sub.recipient)
	for _, item := range receipts.Receipts {
		receiptFrame := item
		s.writePushFrame(sub, protocol.Frame{
			Type:      "push_delivery_receipt",
			Recipient: sub.recipient,
			Receipt:   &receiptFrame,
		})
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
	welcomeLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	welcome, err := protocol.ParseFrameLine(strings.TrimSpace(welcomeLine))
	if err != nil {
		return
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			return
		}
		reply, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		authReply, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
		if err != nil || authReply.Type != "auth_ok" {
			return
		}
	}
	if welcome.Type == "error" {
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
		Networks:      reachableGroupNames(s.reachableGroups),
		Address:       s.identity.Address,
		PubKey:        identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:        s.selfBoxSig,
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
			s.addPeerVersion(normalized, frame.ClientVersion)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(frame.Address)
	}
	// When all key fields are present, verify the box key binding before storing.
	// If verification fails the keys are discarded; if any field is absent the
	// existing behaviour is preserved for backward compatibility.
	if frame.Address != "" && frame.PubKey != "" && frame.BoxKey != "" && frame.BoxSig != "" {
		if identity.VerifyBoxKeyBinding(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig) != nil {
			return
		}
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
			s.addPeerVersion(normalized, frame.ClientVersion)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(frame.Address)
	}
	// When all key fields are present, verify the box key binding before storing.
	if frame.Address != "" && frame.PubKey != "" && frame.BoxKey != "" && frame.BoxSig != "" {
		if identity.VerifyBoxKeyBinding(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig) != nil {
			return
		}
	}
	s.addKnownBoxKey(frame.Address, frame.BoxKey)
	s.addKnownPubKey(frame.Address, frame.PubKey)
	s.addKnownBoxSig(frame.Address, frame.BoxSig)
}

func (s *Service) addPeerAddress(address string, nodeType string, peerID string) {
	if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
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
	// Eagerly populate persistedMeta so that AddedAt is available for
	// eviction decisions immediately, without waiting for a flush cycle.
	if _, ok := s.persistedMeta[address]; !ok {
		now := time.Now().UTC()
		s.persistedMeta[address] = &peerEntry{
			Address:  address,
			NodeType: string(normalizePeerNodeType(nodeType)),
			Source:   "peer_exchange",
			AddedAt:  &now,
		}
	}
}

func (s *Service) addPeerVersion(address, clientVersion string) {
	address = strings.TrimSpace(address)
	clientVersion = strings.TrimSpace(clientVersion)
	if address == "" || clientVersion == "" {
		return
	}

	s.mu.Lock()
	s.peerVersions[address] = clientVersion
	s.mu.Unlock()
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
		// Verify box key binding before accepting keys from third-party contacts
		// advertised by peers (encryption.md: signed box-key advertisement).
		// Network-discovered contacts are stored in-memory only and are NOT
		// written to the trust store; that distinction is preserved by fetch_trusted_contacts.
		if contact.Address == "" || contact.PubKey == "" || contact.BoxKey == "" || contact.BoxSig == "" {
			continue
		}
		if identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig) != nil {
			continue
		}
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
		s.sendAckDeleteToPeer(address, "dm", msg.ID, "")
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
		s.sendAckDeleteToPeer(address, "receipt", receipt.MessageID, receipt.Status)
		log.Printf("node: received pushed delivery receipt peer=%s message_id=%s recipient=%s status=%s", address, receipt.MessageID, receipt.Recipient, receipt.Status)
	}
}

func (s *Service) sendAckDeleteToPeer(address, ackType string, id protocol.MessageID, status string) {
	session := s.peerSession(address)
	if session == nil || !session.authOK {
		return
	}
	frame := protocol.Frame{
		Type:      "ack_delete",
		Address:   s.identity.Address,
		AckType:   ackType,
		ID:        string(id),
		Status:    status,
		Signature: identity.SignPayload(s.identity, ackDeletePayload(s.identity.Address, ackType, string(id), status)),
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Printf("node: ack_delete_send peer=%s type=%s id=%s status=%s mode=session", address, ackType, id, status)
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Printf("node: ack_delete_send peer=%s type=%s id=%s status=%s mode=queued", address, ackType, id, status)
	}
}

func (s *Service) markPeerConnected(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = true
	s.updatePeerStateLocked(health, peerStateHealthy)
	health.LastConnectedAt = now
	health.LastError = ""
	health.ConsecutiveFailures = 0
	health.Score = clampScore(health.Score + peerScoreConnect)
}

func (s *Service) markPeerDisconnected(address string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = false
	s.updatePeerStateLocked(health, peerStateReconnecting)
	health.LastDisconnectedAt = now
	if err != nil {
		health.ConsecutiveFailures++
		health.LastError = err.Error()
		health.Score = clampScore(health.Score + peerScoreFailure)
	} else {
		health.ConsecutiveFailures = 0
		health.Score = clampScore(health.Score + peerScoreDisconnect)
	}
	if peerID := s.peerIDs[address]; peerID != "" {
		delete(s.observedAddrs, peerID)
	}
}

func (s *Service) markPeerWrite(address string, frame protocol.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
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

	address = s.resolveHealthAddress(address)
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
			ClientVersion:       s.peerVersions[health.Address],
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
			Score:               health.Score,
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
	s.mu.Lock()
	primary := s.resolveHealthAddress(address)

	key := pendingFrameKey(primary, frame)
	if key == "" {
		s.mu.Unlock()
		return false
	}

	if _, exists := s.pendingKeys[key]; exists {
		s.mu.Unlock()
		return true
	}
	s.pending[primary] = append(s.pending[primary], pendingFrame{
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
	primary := s.resolveHealthAddress(address)
	frames := append([]pendingFrame(nil), s.pending[primary]...)
	delete(s.pending, primary)
	for _, frame := range frames {
		delete(s.pendingKeys, pendingFrameKey(primary, frame.Frame))
	}
	s.mu.Unlock()

	remaining := make([]pendingFrame, 0)
	now := time.Now().UTC()
	for _, item := range frames {
		if s.pendingFrameExpired(item.Frame, item.QueuedAt, now) {
			s.markOutboundTerminal(item.Frame, "expired", "message delivery expired")
			continue
		}
		if item.Frame.Type != "send_message" && now.Sub(item.QueuedAt) > pendingFrameTTL {
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
	s.pending[primary] = append(s.pending[primary], remaining...)
	for _, item := range remaining {
		s.pendingKeys[pendingFrameKey(primary, item.Frame)] = struct{}{}
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
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
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
		return false
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

func (s *Service) deleteBacklogMessageForRecipient(recipient string, messageID protocol.MessageID) int {
	s.mu.Lock()
	before := len(s.topics["dm"])
	filtered := s.topics["dm"][:0]
	for _, msg := range s.topics["dm"] {
		if msg.ID == messageID && msg.Recipient == recipient {
			delete(s.relayRetry, relayMessageKey(msg.ID))
			continue
		}
		filtered = append(filtered, msg)
	}
	if len(filtered) == 0 {
		delete(s.topics, "dm")
	} else {
		s.topics["dm"] = filtered
	}
	removed := before - len(filtered)
	if removed <= 0 {
		s.mu.Unlock()
		return 0
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return removed
}

func (s *Service) deleteBacklogReceiptForRecipient(recipient string, messageID protocol.MessageID, status string) int {
	s.mu.Lock()
	list := s.receipts[recipient]
	if len(list) == 0 {
		s.mu.Unlock()
		return 0
	}
	filtered := list[:0]
	removed := 0
	for _, receipt := range list {
		if receipt.MessageID == messageID && receipt.Recipient == recipient && receipt.Status == status {
			delete(s.relayRetry, relayReceiptKey(receipt))
			removed++
			continue
		}
		filtered = append(filtered, receipt)
	}
	if len(filtered) == 0 {
		delete(s.receipts, recipient)
	} else {
		s.receipts[recipient] = filtered
	}
	if removed <= 0 {
		s.mu.Unlock()
		return 0
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return removed
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

	orphaned := make(map[string][]pendingFrame, len(s.orphaned))
	for addr, items := range s.orphaned {
		orphaned[addr] = append([]pendingFrame(nil), items...)
	}

	return queueStateFile{
		Version:       queueStateVersion,
		Pending:       pending,
		Orphaned:      orphaned,
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
	health := s.health[s.resolveHealthAddress(address)]
	if health == nil || !health.Connected {
		return nil, false
	}
	return session, true
}

func (s *Service) peerState(address string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	health := s.health[s.resolveHealthAddress(address)]
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
	if address == s.cfg.AdvertiseAddress || address == s.externalListenAddress() || address == s.cfg.ListenAddress {
		return true
	}
	host, _, ok := splitHostPort(address)
	if !ok {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return s.isSelfDialIP(ip)
}

func (s *Service) normalizePeerAddress(observedAddr, advertisedAddr string) (string, bool) {
	observedHost, _, observedOK := splitHostPort(observedAddr)
	advertisedHost, advertisedPort, advertisedOK := splitHostPort(advertisedAddr)
	if advertisedPort == "" {
		advertisedPort = config.DefaultPeerPort
	}

	// .onion addresses are accepted as-is from the advertised field;
	// the observed TCP address is meaningless for Tor connections.
	if advertisedOK && isOnionAddress(advertisedHost) {
		return net.JoinHostPort(advertisedHost, advertisedPort), true
	}
	// Reject any .onion-suffixed hostname that failed the strict validator
	// above (wrong length, invalid base32 chars, etc.) so it cannot leak
	// through the generic hostname branches below.
	if advertisedOK && strings.HasSuffix(strings.ToLower(advertisedHost), ".onion") {
		return "", false
	}

	switch {
	case advertisedOK && observedOK:
		observedIP := net.ParseIP(observedHost)
		advertisedIP := net.ParseIP(advertisedHost)

		if advertisedIP != nil && !isForbiddenAdvertisedIP(advertisedIP) && advertisedHost == observedHost {
			return net.JoinHostPort(advertisedHost, advertisedPort), true
		}
		if observedIP != nil && !s.isForbiddenDialIP(observedIP) {
			if advertisedIP != nil && isForbiddenAdvertisedIP(advertisedIP) {
				return net.JoinHostPort(observedHost, config.DefaultPeerPort), true
			}
			return net.JoinHostPort(observedHost, advertisedPort), true
		}
		return "", false
	case advertisedOK:
		advertisedIP := net.ParseIP(advertisedHost)
		if advertisedIP != nil && (isForbiddenAdvertisedIP(advertisedIP) || s.isForbiddenDialIP(advertisedIP)) {
			return "", false
		}
		return net.JoinHostPort(advertisedHost, advertisedPort), true
	case observedOK:
		// The observed remote port is an ephemeral source port, not a
		// stable listening endpoint. Without a valid advertised port we
		// should not learn a dialable peer address from RemoteAddr alone.
		return "", false
	}

	return "", false
}

// isOnionAddress returns true if the host is a valid Tor .onion address.
// Tor v3 addresses are 56 base32 characters + ".onion" (62 total).
// Tor v2 addresses (deprecated) are 16 base32 characters + ".onion" (22 total).
func isOnionAddress(host string) bool {
	lower := strings.ToLower(host)
	if !strings.HasSuffix(lower, ".onion") {
		return false
	}
	name := lower[:len(lower)-6] // strip ".onion"
	if len(name) != 56 && len(name) != 16 {
		return false
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

// isI2PAddress returns true if the host is an I2P .b32.i2p address.
// I2P base32 addresses are 52 base32 characters + ".b32.i2p".
func isI2PAddress(host string) bool {
	return strings.HasSuffix(strings.ToLower(host), ".b32.i2p")
}

func splitHostPort(address string) (string, string, bool) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || host == "" || port == "" {
		return "", "", false
	}
	return host, port, true
}

func isForbiddenAdvertisedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if inCIDR(ip, "192.168.0.0/16") {
		return true
	}
	if inCIDR(ip, "172.16.0.0/21") {
		return true
	}
	return false
}

func inCIDR(ip net.IP, cidr string) bool {
	_, block, err := net.ParseCIDR(cidr)
	if err != nil || block == nil {
		return false
	}
	return block.Contains(ip)
}

func (s *Service) isForbiddenDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return !s.allowLoopbackPeers()
	}
	if isForbiddenAdvertisedIP(ip) || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	return false
}

func (s *Service) allowLoopbackPeers() bool {
	for _, address := range []string{s.cfg.AdvertiseAddress, s.externalListenAddress(), s.cfg.ListenAddress} {
		host, _, ok := splitHostPort(address)
		if !ok {
			continue
		}
		ip := net.ParseIP(host)
		if ip != nil && ip.IsLoopback() {
			return true
		}
	}
	return false
}

func (s *Service) isSelfDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, address := range []string{s.cfg.AdvertiseAddress, s.externalListenAddress(), s.cfg.ListenAddress} {
		host, _, ok := splitHostPort(address)
		if !ok {
			continue
		}
		selfIP := net.ParseIP(host)
		if selfIP == nil {
			continue
		}
		if selfIP.Equal(ip) {
			if ip.IsLoopback() && s.allowLoopbackPeers() {
				return false
			}
			return true
		}
	}
	return false
}

func (s *Service) shouldSkipDialAddress(address string) bool {
	host, _, ok := splitHostPort(address)
	if !ok {
		return true
	}
	ip := net.ParseIP(host)
	return s.isForbiddenDialIP(ip)
}

func (s *Service) dialAttemptAddressesLocked(address string) []string {
	host, port, ok := splitHostPort(address)
	if !ok {
		return nil
	}
	addresses := []string{net.JoinHostPort(host, port)}
	ip := net.ParseIP(host)
	if port != config.DefaultPeerPort && ip != nil && !s.isForbiddenDialIP(ip) && !ip.IsLoopback() {
		addresses = append(addresses, net.JoinHostPort(host, config.DefaultPeerPort))
	}
	return addresses
}

func enableTCPKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
}

// resolveHealthAddress returns the primary peer address to use as the
// health map key.  When a dial candidate is a fallback variant (e.g.
// host:defaultPort instead of the original host:customPort), the origin
// map translates back to the primary address so that score/cooldown
// accumulate on a single entry regardless of which port was dialled.
// Must be called with s.mu held (read lock sufficient).
func (s *Service) resolveHealthAddress(address string) string {
	if origin, ok := s.dialOrigin[address]; ok {
		return origin
	}
	return address
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

func (s *Service) validateMessageTiming(msg incomingMessage) error {
	now := time.Now().UTC()
	drift := s.cfg.MaxClockDrift
	if drift <= 0 {
		drift = protocol.DefaultMessageTimeDrift
	}

	if msg.CreatedAt.After(now.Add(drift)) {
		return fmt.Errorf("message timestamp %s outside allowed future drift %s", msg.CreatedAt.Format(time.RFC3339), drift)
	}

	if msg.Topic == "dm" && msg.Recipient != "" && msg.Recipient != "*" {
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
			return fmt.Errorf("message timestamp %s expired for delivery", msg.CreatedAt.Format(time.RFC3339))
		}
		return nil
	}

	if msg.CreatedAt.Before(now.Add(-drift)) {
		return fmt.Errorf("message timestamp %s outside allowed drift %s", msg.CreatedAt.Format(time.RFC3339), drift)
	}

	return nil
}

func (s *Service) messageDeliveryExpired(createdAt time.Time, ttlSeconds int) bool {
	if ttlSeconds <= 0 {
		return false
	}
	expiresAt := createdAt.Add(time.Duration(ttlSeconds) * time.Second)
	return !expiresAt.After(time.Now().UTC())
}

func (s *Service) pendingFrameExpired(frame protocol.Frame, queuedAt time.Time, now time.Time) bool {
	if frame.Type != "send_message" || frame.Topic != "dm" {
		return false
	}
	createdAt, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		return false
	}
	if frame.TTLSeconds <= 0 {
		return false
	}
	expiresAt := createdAt.UTC().Add(time.Duration(frame.TTLSeconds) * time.Second)
	return !expiresAt.After(now)
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
	if msg.TTLSeconds < 0 {
		return incomingMessage{}, fmt.Errorf("ttl_seconds must not be negative")
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
