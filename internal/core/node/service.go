package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
	"github.com/piratecash/corsa/internal/core/transport"
)

// errIncompatibleProtocol is returned when a peer's protocol version is
// below MinimumProtocolVersion. The retry loop treats this as a permanent
// failure and stops reconnection attempts — the peer must upgrade first.
var errIncompatibleProtocol = errors.New("incompatible protocol version")

// StoreResult describes the outcome of MessageStore.StoreMessage.
// Three distinct states prevent conflating "already seen" with "write error",
// allowing the caller to make the right decision for each case.
type StoreResult int

const (
	// StoreInserted means the message was genuinely new and persisted.
	StoreInserted StoreResult = iota
	// StoreDuplicate means the message already existed in the durable store.
	// The caller should NOT add it to s.topics or emit UI events.
	StoreDuplicate
	// StoreFailed means a write error occurred.  The message was NOT persisted
	// but the caller should still keep it in-memory (s.topics) so it is not
	// lost from the network.
	StoreFailed
)

// MessageStore allows the desktop layer to own message persistence, while
// relay-only nodes (corsa-node) leave this nil to skip local storage.
type MessageStore interface {
	StoreMessage(envelope protocol.Envelope, isOutgoing bool) StoreResult
	UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool
}

type Service struct {
	identity              *identity.Identity
	selfBoxSig            string // cached ed25519 signature binding identity.BoxPublicKey to identity.Address
	cfg                   config.Node
	trust                 *trustStore
	mu                    sync.RWMutex
	peers                 []transport.Peer // dial candidates (typed: Address + Source)
	known                 map[string]struct{}
	boxKeys               map[string]string
	pubKeys               map[string]string
	boxSigs               map[string]string
	topics                map[string][]protocol.Envelope
	receipts              map[string][]protocol.DeliveryReceipt
	notices               map[string]gazeta.Notice
	seen                  map[string]struct{}
	seenReceipts          map[string]struct{}
	subs                  map[string]map[string]*subscriber
	sessions              map[domain.PeerAddress]*peerSession
	health                map[domain.PeerAddress]*peerHealth
	peerTypes             map[domain.PeerAddress]domain.NodeType
	peerIDs               map[domain.PeerAddress]domain.PeerIdentity
	peerVersions          map[domain.PeerAddress]string
	peerBuilds            map[domain.PeerAddress]int
	pending               map[domain.PeerAddress][]pendingFrame
	pendingKeys           map[string]struct{}
	orphaned              map[domain.PeerAddress][]pendingFrame // legacy fallback-keyed frames that could not be migrated
	relayRetry            map[string]relayAttempt
	outbound              map[string]outboundDelivery
	upstream              map[domain.PeerAddress]struct{}
	inboundHealthRefs     map[domain.PeerAddress]int // resolved overlay address → active inbound connection count
	connWg                sync.WaitGroup             // tracks active handleConn goroutines for graceful shutdown
	backgroundWg          sync.WaitGroup             // tracks fire-and-forget goroutines (receipts, gossip) for clean test shutdown
	conns                 map[net.Conn]*connEntry    // primary registry: conn → connEntry (core + metered + tracked). See §2.6.7 of docs/netcore-migration.md for the lifecycle invariant and §2.6.8 for the PR 9.4a consolidation that replaced inboundConns / inboundMetered / inboundTracked / inboundNetCores.
	connIDCounter         uint64                     // monotonic counter for connection IDs (protected by mu)
	bans                  map[string]banEntry
	events                map[chan protocol.LocalChangeEvent]struct{}
	listener              net.Listener
	lastSync              time.Time
	peersStatePath        string
	lastPeerSave          time.Time
	lastPeerEvict         time.Time
	dialOrigin            map[domain.PeerAddress]domain.PeerAddress // dial address → primary peer address (for fallback port tracking)
	persistedMeta         map[domain.PeerAddress]*peerEntry         // stable metadata from peers.json, keyed by address
	observedAddrs         map[domain.PeerIdentity]string            // peer identity (fingerprint) → observed IP they reported for us
	reachableGroups       map[domain.NetGroup]struct{}              // network groups this node can reach (computed at startup)
	messageStore          MessageStore                              // optional: persistence handler registered by desktop layer
	router                Router                                    // routing strategy for outbound message delivery
	relayStates           *relayStateStore                          // hop-by-hop relay forwarding state (Iteration 1)
	relayLimiter          *relayRateLimiter                         // per-peer token bucket for relay fan-out
	connLimiter           *connRateLimiter                          // per-IP connection rate limiter at accept level
	cmdLimiter            *commandRateLimiter                       // per-connection command rate limiter for non-relay frames
	inboundByIP           map[string]int                            // IP → active inbound connection count (per-IP cap)
	routingTable          *routing.Table                            // distance-vector routing table (Phase 1.2)
	announceLoop          *routing.AnnounceLoop                     // periodic + triggered announce_routes sender (Phase 1.2)
	identitySessions      map[domain.PeerIdentity]int               // peer identity → active session count (multi-session awareness)
	identityRelaySessions map[domain.PeerIdentity]int               // peer identity → relay-capable session count (direct-route lifecycle)
	disableRateLimiting   bool                                      // test hook: skip per-IP rate limiting, connection caps, and blacklist checks
	drainDone             func()                                    // test hook: called after drainPendingForIdentities completes; nil in production
	done                  chan struct{}                             // closed when Run() exits; drain goroutines check this to avoid work after shutdown
	primeBootstrapOnRun   bool                                      // startup hook: apply compiled bootstrap peers via add_peer once CM is ready

	// aggregateStatus is the materialized aggregate network health of the
	// node. It is recomputed on every per-peer state transition via
	// refreshAggregateStatusLocked and is the single source of truth for
	// policy decisions (shouldRequestPeers) and for the Desktop UI.
	// Protected by s.mu.
	aggregateStatus domain.AggregateStatusSnapshot

	// runCtx is the context passed to Run(). Stored so that callbacks
	// (e.g. onCMSessionEstablished) can start goroutines bound to the
	// Service lifecycle instead of context.Background().
	runCtx context.Context

	// Connection management subsystem (Stage 3 integration).
	peerProvider *PeerProvider                   // single source of dial candidates — replaces peers[] + peerDialCandidates()
	connManager  *ConnectionManager              // event-driven outbound connection lifecycle — replaces ensurePeerSessions()
	bannedIPSet  map[string]domain.BannedIPEntry // IP-wide bans, persisted independently from top-500 trim

	// File transfer subsystem (Iteration 21).
	fileStore    *filetransfer.FileStore // content-addressed file storage in transmit dir
	fileTransfer *filetransfer.Manager   // sender/receiver state machines
	fileRouter   *filerouter.Router      // routing file commands through the mesh
}

type subscriber struct {
	id        string
	recipient string
	conn      net.Conn
}

type peerSession struct {
	address      domain.PeerAddress
	peerIdentity domain.PeerIdentity // peer's Ed25519 identity fingerprint from welcome.Address
	connID       uint64              // monotonic connection ID for diagnostics
	conn         net.Conn
	metered      *MeteredConn // tracks bytes for this session; nil when conn is not metered
	sendCh       chan protocol.Frame
	inboxCh      chan protocol.Frame
	errCh        chan error
	version      int
	authOK       bool
	capabilities []domain.Capability // intersection of local and remote capabilities negotiated during handshake

	// netCore owns the outbound connection and is the single writer to the
	// socket. Set at construction by openPeerSession / openPeerSessionForCM
	// before the first peerSessionRequest so that welcome/auth frames go
	// through the managed writer path. nil only in unit tests that build a
	// peerSession manually without the service wiring.
	netCore *NetCore

	// onClose is invoked by Close() before the underlying connection is shut
	// down. Outbound sessions set it to unregister the netCore from the
	// service-level connection map. nil for unit tests.
	onClose func()

	// welcomeMeta carries handshake metadata that must NOT be applied to
	// Service-level maps until the CM generation check passes. Stale dials
	// (generation mismatch) are discarded without activating side-effects;
	// only onCMSessionEstablished writes these into shared state.
	welcomeMeta *peerWelcomeMeta

	// closeOnce guarantees that the teardown sequence (NetCore.Close +
	// onClose) runs exactly once even under concurrent Close() calls from
	// the defer in openPeerSession, the ctx-watcher, closeOnError in
	// openPeerSessionForCM and the CM failure path.
	closeOnce sync.Once
	closeErr  error
}

// peerWelcomeMeta holds welcome-frame data deferred until CM activation.
type peerWelcomeMeta struct {
	welcome         protocol.Frame // raw welcome frame for learnIdentityFromWelcome
	clientVersion   string
	clientBuild     int
	observedAddress string
}

// Close shuts down the session. When netCore is set, teardown runs in this
// order:
//  1. netCore.Close() — closes the raw socket, closes sendCh, waits for the
//     writer goroutine to drain and exit.
//  2. onClose() — removes the NetCore registration from s.conns.
//
// The order matters for the single-writer invariant. While writerLoop is
// still alive, the registration MUST remain visible to netCoreFor() so
// that any concurrent writeJSONFrame(session.conn, ...) goes through the
// managed path (enqueueFrame → sendRaw). Unregistering first would cause
// netCoreFor to return nil and the writeJSONFrame fallback would then call
// conn.Write / io.WriteString directly — a second writer racing with a
// still-live writerLoop. Only after netCore.Close() has returned is it
// safe to remove the map entry: the socket is closed, sendCh is closed,
// and the writer goroutine has exited.
//
// Concurrent-safe and idempotent via sync.Once (mirrors NetCore.closeOnce).
// The first caller performs the teardown; subsequent callers observe the
// same stored result without re-running the callbacks.
func (ps *peerSession) Close() error {
	if ps == nil {
		return nil
	}
	ps.closeOnce.Do(func() {
		if ps.netCore != nil {
			ps.netCore.Close()
			if ps.onClose != nil {
				ps.onClose()
			}
			return
		}
		if ps.onClose != nil {
			ps.onClose()
		}
		if ps.conn != nil {
			ps.closeErr = ps.conn.Close()
		}
	})
	return ps.closeErr
}

type peerHealth struct {
	Address             domain.PeerAddress
	Connected           bool
	Direction           domain.PeerDirection // outbound, inbound, or "" (unknown)
	State               string
	LastConnectedAt     time.Time
	LastDisconnectedAt  time.Time
	LastPingAt          time.Time
	LastPongAt          time.Time
	LastUsefulSendAt    time.Time
	LastUsefulReceiveAt time.Time
	ConsecutiveFailures int
	LastError           string
	Score               int       // peer quality score for persistence priority
	BannedUntil         time.Time // peer is not dialled until this time expires
	BytesSent           int64     // total bytes sent to this peer across all sessions
	BytesReceived       int64     // total bytes received from this peer across all sessions
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

type banEntry struct {
	Score       int
	Blacklisted time.Time
}

const (
	peerStateHealthy                = "healthy"
	peerStateDegraded               = "degraded"
	peerStateStalled                = "stalled"
	peerStateReconnecting           = "reconnecting"
	peerDirectionOutbound           = domain.PeerDirectionOutbound
	peerDirectionInbound            = domain.PeerDirectionInbound
	peerRequestTimeout              = 12 * time.Second
	pendingFrameTTL                 = 5 * time.Minute
	relayRetryTTL                   = 3 * time.Minute
	maxPendingFrameRetries          = 5
	banThreshold                    = 1000
	banIncrementInvalidSig          = 100
	banIncrementIncompatibleVersion = 1000 // immediate blacklist: incompatible protocol wastes resources on every connect
	banIncrementRateLimit           = 200  // command rate limit violation signals intentional abuse
	banDuration                     = 24 * time.Hour
	// nodeName and networkName are the protocol-level identifiers included
	// in handshake, ping/pong and other frames. Defined once here so that
	// all call-sites stay consistent. When these values become configurable
	// at runtime they should move to config.Node or config.App.
	nodeName    = "corsa"
	networkName = "gazeta-devnet"
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
		log.Error().Str("path", peersStatePath).Err(err).Msg("peer state load failed")
		peerState = peerStateFile{Version: peerStateVersion, Peers: []peerEntry{}}
	}

	peers := make([]transport.Peer, 0, len(cfg.BootstrapPeers)+len(peerState.Peers))
	seenAddrs := make(map[domain.PeerAddress]struct{})
	for _, addr := range cfg.BootstrapPeers {
		pa := domain.PeerAddress(addr)
		peers = append(peers, transport.Peer{
			Address: pa,
			Source:  domain.PeerSourceBootstrap,
		})
		seenAddrs[pa] = struct{}{}
	}
	sortPeerEntries(peerState.Peers)
	// Index persisted entries so we can seed health from their metadata.
	persistedByAddr := make(map[domain.PeerAddress]*peerEntry, len(peerState.Peers))
	for i, entry := range peerState.Peers {
		if _, dup := seenAddrs[entry.Address]; dup {
			// Even for duplicates (bootstrap overlap) keep the metadata for health seeding.
			persistedByAddr[entry.Address] = &peerState.Peers[i]
			continue
		}
		seenAddrs[entry.Address] = struct{}{}
		persistedByAddr[entry.Address] = &peerState.Peers[i]
		peers = append(peers, transport.Peer{
			Address: entry.Address,
			Source:  domain.PeerSourcePersisted,
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
		log.Error().Str("path", queueStatePath).Err(err).Msg("queue state load failed")
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
			if h, _, ok := splitHostPort(string(addr)); ok {
				hostPrimaries[h] = append(hostPrimaries[h], string(addr))
			}
		}
		for address := range queueState.Pending {
			if _, isPrimary := seenAddrs[domain.PeerAddress(address)]; isPrimary {
				continue
			}
			h, _, ok := splitHostPort(address)
			if !ok {
				// Malformed address — orphan to avoid silent loss.
				queueState.Orphaned[address] = append(queueState.Orphaned[address], queueState.Pending[address]...)
				delete(queueState.Pending, address)
				log.Warn().Str("address", address).Int("count", len(queueState.Orphaned[address])).Msg("orphaned pending frames for malformed address")
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
				log.Warn().Str("address", address).Int("count", len(queueState.Orphaned[address])).Msg("orphaned pending frames for unknown host")
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
				log.Warn().Str("address", address).Int("count", len(queueState.Orphaned[address])).Int("candidates", len(candidates)).Msg("orphaned pending frames for ambiguous address")
			}
		}
		queueState.Version = queueStateVersion
	}

	pendingKeys := make(map[string]struct{})
	for address, items := range queueState.Pending {
		for _, item := range items {
			if key := pendingFrameKey(domain.PeerAddress(address), item.Frame); key != "" {
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
	if len(queueState.RelayMessages) > 0 {
		ids := make([]string, len(queueState.RelayMessages))
		for i, m := range queueState.RelayMessages {
			ids[i] = string(m.ID)
		}
		log.Info().Str("path", queueStatePath).Int("count", len(queueState.RelayMessages)).Strs("ids", ids).Msg("restored relay messages from queue state")
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
	restoredHealth := make(map[domain.PeerAddress]*peerHealth, len(persistedByAddr))
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
		if entry.BannedUntil != nil && time.Now().UTC().Before(*entry.BannedUntil) {
			h.BannedUntil = *entry.BannedUntil
		}
		restoredHealth[addr] = h
	}

	// Migrate queueState maps from string-keyed to domain.PeerAddress-keyed.
	// queueState.Pending and queueState.Orphaned are keyed by string addresses in JSON;
	// convert to domain.PeerAddress for internal use.
	pending := make(map[domain.PeerAddress][]pendingFrame)
	for addr, frames := range queueState.Pending {
		pending[domain.PeerAddress(addr)] = frames
	}
	orphaned := make(map[domain.PeerAddress][]pendingFrame)
	for addr, frames := range queueState.Orphaned {
		orphaned[domain.PeerAddress(addr)] = frames
	}
	relayRetry := make(map[string]relayAttempt)
	for addr, attempt := range queueState.RelayRetry {
		relayRetry[addr] = attempt
	}

	svc := &Service{
		// Default lifecycle context: replaced by Run(ctx) with the real
		// cancellable ctx. The default prevents nil-deref in code paths
		// (e.g. handleInboundPushMessage sender-key recovery) that derive
		// a timeout from s.runCtx before Run() has been called — notably in
		// unit tests that exercise handlers directly without Run().
		runCtx:                context.Background(),
		identity:              id,
		cfg:                   cfg,
		selfBoxSig:            selfContact.BoxSignature,
		trust:                 trust,
		peers:                 peers,
		peersStatePath:        peersStatePath,
		persistedMeta:         persistedByAddr,
		known:                 known,
		boxKeys:               boxKeys,
		pubKeys:               pubKeys,
		boxSigs:               boxSigs,
		topics:                topics,
		receipts:              receipts,
		notices:               make(map[string]gazeta.Notice),
		seen:                  seen,
		seenReceipts:          seenReceipts,
		subs:                  make(map[string]map[string]*subscriber),
		sessions:              make(map[domain.PeerAddress]*peerSession),
		health:                restoredHealth,
		peerTypes:             make(map[domain.PeerAddress]domain.NodeType),
		peerIDs:               make(map[domain.PeerAddress]domain.PeerIdentity),
		peerVersions:          make(map[domain.PeerAddress]string),
		peerBuilds:            make(map[domain.PeerAddress]int),
		pending:               pending,
		pendingKeys:           pendingKeys,
		orphaned:              orphaned,
		relayRetry:            relayRetry,
		outbound:              queueState.OutboundState,
		upstream:              make(map[domain.PeerAddress]struct{}),
		dialOrigin:            make(map[domain.PeerAddress]domain.PeerAddress),
		observedAddrs:         make(map[domain.PeerIdentity]string),
		reachableGroups:       computeReachableGroups(cfg),
		inboundHealthRefs:     make(map[domain.PeerAddress]int),
		conns:                 make(map[net.Conn]*connEntry),
		bans:                  make(map[string]banEntry),
		events:                make(map[chan protocol.LocalChangeEvent]struct{}),
		identitySessions:      make(map[domain.PeerIdentity]int),
		identityRelaySessions: make(map[domain.PeerIdentity]int),
		bannedIPSet:           make(map[string]domain.BannedIPEntry),
		aggregateStatus:       domain.AggregateStatusSnapshot{Status: domain.NetworkStatusOffline},
		done:                  make(chan struct{}),
	}

	// Initialize PeerProvider (Stage 3: connection management integration).
	svc.peerProvider = NewPeerProvider(PeerProviderConfig{
		HealthFn: func(addr domain.PeerAddress) *PeerHealthView {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			addr = svc.resolveHealthAddress(addr)
			h := svc.health[addr]
			if h == nil {
				return nil
			}
			return &PeerHealthView{
				Score:               h.Score,
				ConsecutiveFailures: h.ConsecutiveFailures,
				LastDisconnectedAt:  h.LastDisconnectedAt,
				BannedUntil:         h.BannedUntil,
				Connected:           h.Connected,
			}
		},
		ConnectedFn: func() map[string]struct{} {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			return svc.connectedHostsLocked()
		},
		QueuedFn: func() map[string]struct{} {
			if svc.connManager == nil {
				return make(map[string]struct{})
			}
			return svc.connManager.QueuedIPs()
		},
		ForbiddenFn:   svc.isForbiddenDialIP,
		IsSelfAddress: svc.isSelfAddress,
		NetworksFn: func() map[domain.NetGroup]struct{} {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			groups := make(map[domain.NetGroup]struct{}, len(svc.reachableGroups))
			for g := range svc.reachableGroups {
				groups[g] = struct{}{}
			}
			return groups
		},
		BannedIPsFn: func() map[string]domain.BannedIPEntry {
			svc.mu.RLock()
			defer svc.mu.RUnlock()
			result := make(map[string]domain.BannedIPEntry, len(svc.bannedIPSet))
			for ip, entry := range svc.bannedIPSet {
				result[ip] = entry
			}
			return result
		},
		ListenAddr:  domain.ListenAddress(cfg.ListenAddress),
		DefaultPort: config.DefaultPeerPort,
	})

	// Load persisted peers into PeerProvider.
	for _, entry := range peerState.Peers {
		svc.peerProvider.Restore(domain.RestoreEntry{
			Address: entry.Address,
			Source:  entry.Source,
			AddedAt: func() time.Time {
				if entry.AddedAt != nil {
					return *entry.AddedAt
				}
				return time.Now().UTC()
			}(),
			Network: entry.Network,
		})
	}
	// Add bootstrap peers (merge: Source updated, AddedAt/Network preserved).
	for _, addr := range cfg.BootstrapPeers {
		svc.peerProvider.Add(domain.PeerAddress(addr), domain.PeerSourceBootstrap)
	}

	// Restore IP-wide bans from persisted state. Expired entries are
	// silently dropped — they would be filtered anyway by BannedIPsFn.
	now := time.Now().UTC()
	for _, b := range peerState.BannedIPs {
		if b.BannedUntil.After(now) {
			affected := make([]domain.PeerAddress, len(b.AffectedPeers))
			for i, a := range b.AffectedPeers {
				affected[i] = domain.PeerAddress(a)
			}
			svc.bannedIPSet[b.IP] = domain.BannedIPEntry{
				IP:            b.IP,
				BannedUntil:   b.BannedUntil,
				BanOrigin:     domain.PeerAddress(b.BanOrigin),
				BanReason:     b.BanReason,
				AffectedPeers: affected,
			}
		}
	}

	// Initialize ConnectionManager (Stage 3: connection management integration).
	svc.connManager = NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return svc.cfg.EffectiveMaxOutgoingPeers() },
		Provider:   svc.peerProvider,
		DialFn: func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
			return svc.dialForCM(ctx, addresses)
		},
		OnSessionEstablished: func(info SessionInfo) {
			svc.onCMSessionEstablished(info)
		},
		OnSessionTeardown: func(info SessionInfo) {
			svc.onCMSessionTeardown(info)
		},
		OnStaleSession: func(session *peerSession) {
			svc.onCMStaleSession(session)
		},
		OnDialFailed: func(address domain.PeerAddress, err error, incompatible bool) {
			svc.onCMDialFailed(address, err, incompatible)
		},
	})

	// Initialize distance-vector routing table (Phase 1.2).
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(routing.PeerIdentity(id.Address)),
	)
	svc.router = NewTableRouter(svc, svc.routingTable)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		svc,
		svc.routingCapablePeers,
	)

	svc.relayStates = newRelayStateStore()
	svc.relayStates.restore(queueState.RelayForwardStates)
	svc.relayLimiter = newRelayRateLimiter()
	svc.connLimiter = newConnRateLimiter()
	svc.cmdLimiter = newCommandRateLimiter()
	svc.inboundByIP = make(map[string]int)

	// Compute initial aggregate status from restored health entries so that
	// fetch_aggregate_status returns a correct value immediately after
	// restart, before any peer events arrive. Without this, the status
	// stays at the zero-value "offline" even when restored peers are in
	// reconnecting state — which mis-drives bootstrap policy decisions.
	svc.refreshAggregateStatusLocked()

	return svc
}

// RegisterMessageStore sets the optional handler for message persistence.
// Must be called before Run(). Desktop nodes register a store so the UI layer
// owns chatlog; relay-only nodes skip this — messages are relayed but not stored.
func (s *Service) RegisterMessageStore(store MessageStore) {
	s.messageStore = store
}

func (s *Service) Run(ctx context.Context) error {
	// Store context so CM callbacks can start goroutines bound to the
	// Service lifecycle (see onCMSessionEstablished).
	s.runCtx = ctx

	// Signal drain goroutines to stop when Run exits. Drain goroutines
	// launched by onPeerSessionEstablished and handleAnnounceRoutes check
	// this channel before doing any work — prevents them from running
	// against a half-torn-down Service during shutdown.
	defer close(s.done)

	// Wire relay state persistence: when the TTL ticker evicts expired
	// entries, persist the updated state so disk stays in sync with memory.
	s.relayStates.onEvict = func() { s.persistRelayState() }
	// Start relay state TTL ticker; stopped on shutdown.
	s.relayStates.start()
	defer s.relayStates.stop()

	// Initialize file transfer subsystem (Iteration 21).
	s.initFileTransfer()
	defer s.stopFileTransfer()

	// Start routing table TTL ticker and announce loop (Phase 1.2).
	routingCtx, routingCancel := context.WithCancel(ctx)
	defer routingCancel()

	go func() {
		defer crashlog.DeferRecover()
		s.announceLoop.Run(routingCtx)
	}()

	go func() {
		defer crashlog.DeferRecover()
		s.routingTableTTLLoop(routingCtx)
	}()

	// On shutdown: close all inbound connections so handleConn goroutines
	// exit, wait for them to finish.
	defer func() {
		s.closeAllInboundConns()
		log.Info().Msg("waiting for inbound connections to finish")
		s.connWg.Wait()
	}()

	// Start ConnectionManager event loop (Stage 3).
	cmDone := make(chan struct{})
	go func() {
		defer crashlog.DeferRecover()
		s.connManager.Run(ctx)
		close(cmDone)
	}()
	// Wait for CM event loop to be ready before starting bootstrap.
	<-s.connManager.Ready()
	if s.primeBootstrapOnRun {
		s.primeStartupBootstrapPeers()
	}

	bootstrapDone := make(chan struct{})
	go func() {
		defer crashlog.DeferRecover()
		s.bootstrapLoop(ctx)
		close(bootstrapDone)
	}()

	if !s.cfg.EffectiveListenerEnabled() {
		<-ctx.Done()
		<-bootstrapDone
		<-cmDone
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
				<-cmDone
				return nil
			default:
			}

			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			return fmt.Errorf("accept connection: %w", err)
		}

		// Per-IP connection rate limiting — reject before allocating any
		// per-connection resources (goroutines, maps, buffers). Prevents
		// SYN flood and connection exhaustion from a single source.
		ip := remoteIP(conn.RemoteAddr())
		if !s.disableRateLimiting && !s.connLimiter.allowConnect(ip) {
			log.Warn().Str("ip", ip).Str("reason", "conn-rate-limit").Msg("reject connection")
			_ = conn.Close()
			continue
		}

		// Per-IP concurrent connection cap — even within rate limits, no
		// single IP should monopolize connection slots.
		if !s.disableRateLimiting && !s.tryIncrementIPConn(ip) {
			log.Warn().Str("ip", ip).Str("reason", "max-conn-per-ip").Msg("reject connection")
			_ = conn.Close()
			continue
		}

		s.connWg.Add(1)
		go func(c net.Conn, cip string) {
			defer s.connWg.Done()
			defer crashlog.DeferRecover()
			defer s.decrementIPConn(cip)
			s.handleConn(c)
		}(conn, ip)
	}
}

func (s *Service) ListenAddress() string {
	return s.cfg.ListenAddress
}

func (s *Service) AdvertiseAddress() string {
	return s.cfg.AdvertiseAddress
}

func (s *Service) NodeType() domain.NodeType {
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
	return s.NodeType().IsFull()
}

func (s *Service) Address() string {
	return s.identity.Address
}

// WaitBackground blocks until all fire-and-forget goroutines tracked by
// backgroundWg have finished. Tests call this before TempDir cleanup to
// avoid "directory not empty" races caused by async disk writes.
func (s *Service) WaitBackground() {
	s.backgroundWg.Wait()
}

func (s *Service) SubscriberCount(recipient string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs[recipient])
}

func (s *Service) SubscribeLocalChanges() (<-chan protocol.LocalChangeEvent, func()) {
	ch := make(chan protocol.LocalChangeEvent, 16)

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

func (s *Service) handleConn(conn net.Conn) {
	if !s.disableRateLimiting && s.isBlacklistedConn(conn) {
		_ = conn.Close()
		return
	}
	metered := NewMeteredConn(conn)
	if !s.registerInboundConn(metered) {
		log.Warn().Str("addr", conn.RemoteAddr().String()).Str("reason", "max-connections").Msg("reject connection")
		_ = conn.Close()
		return
	}
	defer func() {
		if addr := s.inboundPeerAddress(metered); addr != "" {
			s.trackInboundDisconnect(metered, addr)
		}
		s.accumulateInboundTraffic(metered)
		// Close TCP before waiting for the writer goroutine to drain.
		// Without this, the writer might be stuck in conn.Write with a
		// 30-second deadline and unregisterInboundConn would hang waiting
		// for writerDone. Closing the socket unblocks conn.Write with an
		// error, letting the writer exit promptly.
		_ = metered.Close()
		s.unregisterInboundConn(metered)
		s.removeSubscriberConn(metered)
		s.clearConnAuth(metered)
	}()

	log.Info().Str("addr", conn.RemoteAddr().String()).Msg("incoming connection")
	enableTCPKeepAlive(conn)
	conn = metered

	var heartbeatStop chan struct{}
	defer func() {
		if heartbeatStop != nil {
			close(heartbeatStop)
		}
	}()

	reader := bufio.NewReader(conn)
	connKey := conn.RemoteAddr().String()
	defer s.cmdLimiter.removeConn(connKey)
	for {
		// Set a read deadline before each frame read. This prevents
		// Slowloris-style attacks where a peer opens a connection and
		// sends data extremely slowly (or not at all) to hold the
		// connection slot and goroutine indefinitely. Legitimate peers
		// send heartbeat pings every 30s, so 120s is generous.
		if tc, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = tc.SetReadDeadline(time.Now().Add(inboundReadTimeout))
		}

		line, err := readFrameLine(reader, maxCommandLineBytes)
		if err != nil {
			if errors.Is(err, errFrameTooLarge) {
				log.Debug().Str("addr", conn.RemoteAddr().String()).
					Msg("inbound_read_loop: closing connection — frame exceeds max size")
				s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeFrameTooLarge})
			} else if err != io.EOF {
				log.Debug().Err(err).Str("addr", conn.RemoteAddr().String()).
					Msg("inbound_read_loop: closing connection — read error")
				s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeRead})
			} else {
				log.Debug().Str("addr", conn.RemoteAddr().String()).
					Msg("inbound_read_loop: peer closed connection (EOF)")
			}
			return
		}

		// Per-connection command rate limiting — prevents a single peer
		// from flooding with valid commands to exhaust CPU.
		//
		// file_command frames are exempt: they are high-throughput data-plane
		// frames (chunk_request / chunk_response) that easily exceed the
		// control-plane rate (30 cmd/s). They are already gated behind
		// authentication + file_transfer_v1 capability in dispatchNetworkFrame,
		// so the attack surface is limited to authenticated peers.
		if peekFrameType(line) != protocol.FileCommandFrameType &&
			!s.cmdLimiter.allowCommand(connKey) {
			log.Debug().Str("addr", conn.RemoteAddr().String()).
				Msg("inbound_read_loop: closing connection — command rate limit exceeded")
			s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeRateLimited})
			s.addBanScore(conn, banIncrementRateLimit)
			return
		}

		if !s.handleCommand(conn, strings.TrimSpace(line)) {
			return
		}

		// Start the inbound heartbeat goroutine once the peer is fully
		// connected (tracked in inboundHealthRefs). For authenticated
		// peers this happens after auth_session; for unauthenticated
		// peers — immediately after hello. Both sides ping independently
		// so that each node has its own proof of liveness regardless of
		// who initiated the TCP connection.
		if heartbeatStop == nil {
			if addr := s.trackedInboundPeerAddress(conn); addr != "" {
				heartbeatStop = make(chan struct{})
				go s.inboundHeartbeat(conn, addr, heartbeatStop)
			}
		}
	}
}

func (s *Service) handleCommand(conn net.Conn, line string) bool {
	if !protocol.IsJSONLine(line) {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", conn.RemoteAddr().String()).
			Str("direction", "recv").
			Str("command", "non-json").
			Bool("accepted", false).
			Msg("protocol_trace")
		s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON})
		return false
	}
	return s.dispatchNetworkFrame(conn, line)
}

func (s *Service) dispatchNetworkFrame(conn net.Conn, line string) bool {
	frame, err := protocol.ParseFrameLine(line)
	if err != nil {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", conn.RemoteAddr().String()).
			Str("direction", "recv").
			Str("command", "").
			Bool("accepted", false).
			Msg("protocol_trace")
		s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidJSON, Error: err.Error()})
		return false
	}

	// Auth guard for P2P commands. Handshake commands (hello, ping, pong,
	// auth_session) are available to all connections. Every other command
	// on the data port is part of the authenticated P2P wire protocol and
	// requires a completed auth_session. Data-only commands (fetch_messages,
	// send_message, etc.) are not handled here at all — they live
	// exclusively in handleLocalFrameDispatch / RPC HTTP.
	//
	// The role is derived from server-side auth state (ConnAuthState),
	// never from frame.Client which the attacker controls — eliminating
	// GAP-0.

	accepted := true
	defer func() {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", conn.RemoteAddr().String()).
			Str("direction", "recv").
			Str("command", frame.Type).
			Bool("accepted", accepted).
			Msg("protocol_trace")
	}()

	// Update per-connection last activity timestamp for staleness checks.
	s.touchConnActivity(conn)

	// --- Handshake commands (no auth required) ---
	switch frame.Type {
	case "ping":
		if addr := s.trackedInboundPeerAddress(conn); addr != "" {
			s.markPeerRead(addr, frame)
		}
		pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
		s.writeJSONFrame(conn, pongFrame)
		if addr := s.trackedInboundPeerAddress(conn); addr != "" {
			s.markPeerWrite(addr, pongFrame)
		}
		return true
	case "pong":
		if addr := s.trackedInboundPeerAddress(conn); addr != "" {
			s.markPeerRead(addr, frame)
		}
		return true
	case "hello":
		// Reject re-hello once auth has been initiated (challenge issued)
		// or completed (Verified). Without this guard a second hello
		// between challenge issuance and auth_session would overwrite
		// NetCore identity/address/caps via rememberConnPeerAddr while
		// handleAuthSession still verifies against the original
		// state.Hello — allowing an attacker to authenticate as identity A
		// but bind the connection to an unverified address B, poisoning
		// health tracking and capability context.
		if s.isAuthInitiated(conn) {
			accepted = false
			s.writeJSONFrameSync(conn, protocol.Frame{
				Type:  "error",
				Code:  protocol.ErrCodeHelloAfterAuth,
				Error: "re-hello rejected: authentication in progress or completed",
			})
			return true
		}
		if err := validateProtocolHandshake(frame); err != nil {
			accepted = false
			log.Warn().Err(err).Str("addr", conn.RemoteAddr().String()).Int("version", frame.Version).Msg("inbound_peer_protocol_too_old")
			s.writeJSONFrame(conn, protocol.Frame{
				Type:                   "error",
				Code:                   protocol.ErrCodeIncompatibleProtocol,
				Error:                  err.Error(),
				Version:                config.ProtocolVersion,
				MinimumProtocolVersion: config.MinimumProtocolVersion,
			})
			// Immediate IP blacklist (transport level): prevents the same
			// remote IP from opening new TCP connections for 24 hours.
			s.addBanScore(conn, banIncrementIncompatibleVersion)
			// Health-level ban (overlay level): if the hello declares a
			// listen address that matches a known peer, apply the same
			// BannedUntil + score penalty as the outbound path so that
			// peerDialCandidates also skips outbound dials to this peer.
			if peerAddr := strings.TrimSpace(frame.Listen); peerAddr != "" {
				s.penalizeOldProtocolPeer(domain.PeerAddress(peerAddr))
			} else if peerAddr = strings.TrimSpace(frame.Address); peerAddr != "" {
				s.penalizeOldProtocolPeer(domain.PeerAddress(peerAddr))
			}
			return true
		}
		// Determine auth path by checking server-verifiable identity fields
		// (Address, PubKey, BoxKey, BoxSig), NOT frame.Client which the
		// attacker controls. This eliminates GAP-0.
		if connauth.HasIdentityFields(frame) {
			authState, err := connauth.PrepareAuth(frame)
			if err != nil {
				accepted = false
				s.addBanScore(conn, banIncrementInvalidSig)
				s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature, Error: err.Error()})
				return false
			}
			s.SetConnAuthState(conn, authState)
			s.rememberConnPeerAddr(conn, frame)
			if frame.Client == "node" || frame.Client == "desktop" {
				log.Info().Str("client", frame.Client).Str("address", frame.Address).Str("listen", frame.Listen).Str("node_type", frame.NodeType).Str("version", frame.ClientVersion).Msg("hello")
			}
			s.writeJSONFrame(conn, s.welcomeFrame(authState.Challenge, remoteIP(conn.RemoteAddr())))
			return true
		}
		// No identity fields → unauthenticated peer. It stays
		// unauthenticated with access limited to handshake commands
		// (hello, ping, pong, auth_session). All P2P wire commands
		// are blocked until auth_session completes.
		s.rememberConnPeerAddr(conn, frame)
		log.Debug().
			Str("client", frame.Client).
			Str("addr", conn.RemoteAddr().String()).
			Msg("hello_without_identity_fields")
		s.writeJSONFrame(conn, s.welcomeFrame("", remoteIP(conn.RemoteAddr())))
		return true
	case "auth_session":
		reply, ok := s.handleAuthSession(conn, frame)
		if !ok {
			accepted = false
			s.writeJSONFrameSync(conn, reply)
			return false
		}
		s.writeJSONFrame(conn, reply)
		return true

	default:
		// --- P2P wire protocol (auth required) ---
		//
		// Everything below requires a completed auth_session. The role is
		// derived from server-side auth state, never from frame.Client
		// (GAP-0). Data commands (fetch_messages, send_message, etc.) are
		// not handled here — they live exclusively in
		// handleLocalFrameDispatch / RPC HTTP and fall through to the
		// unknown_command response at the bottom.
	}

	// Auth gate for P2P commands. Handshake commands returned above;
	// everything reaching this point is either a P2P command or unknown.
	if !s.isConnAuthenticated(conn) {
		// Check whether the command is a known P2P command before deciding
		// on the error code: known P2P → auth_required, unknown → unknown_command.
		if isP2PWireCommand(frame.Type) {
			accepted = false
			s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired})
			return false
		}
		accepted = false
		s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand})
		return false
	}

	switch frame.Type {
	case "get_peers":
		// P2P peer discovery: authenticated peers request the peer list
		// for network synchronization (syncPeer, syncPeerSession).
		// Merges active CM slots with PeerProvider candidates, deduplicated
		// by IP. Network group filtering prevents leaking clearnet addresses
		// to Tor/I2P peers and vice versa.
		exchanged := s.buildPeerExchangeResponse(s.connPeerReachableGroups(conn))
		peers := make([]string, len(exchanged))
		for i, a := range exchanged {
			peers[i] = string(a)
		}
		s.writeJSONFrame(conn, protocol.Frame{
			Type:  "peers",
			Count: len(peers),
			Peers: peers,
		})
		return true
	case "fetch_contacts":
		// P2P contact sync: authenticated peers fetch the contact list
		// for key material synchronization (syncPeer, syncContactsViaSession).
		s.writeJSONFrame(conn, s.contactsFrame())
		return true
	case "ack_delete":
		reply, ok := s.handleAckDeleteFrame(conn, frame)
		if !ok {
			accepted = false
			s.writeJSONFrameSync(conn, reply)
			return false
		}
		s.writeJSONFrame(conn, reply)
		return true
	case "subscribe_inbox":
		// Auth gate enforced above. Identity binding: the authenticated
		// peer may only subscribe to
		// its own identity. Without this check, an authenticated peer could
		// subscribe to a different identity's inbox and receive their messages.
		peerIdentity := s.inboundPeerIdentity(conn)
		requestedRecipient := strings.TrimSpace(frame.Recipient)
		if requestedRecipient != "" && requestedRecipient != string(peerIdentity) {
			accepted = false
			log.Warn().
				Str("peer_identity", string(peerIdentity)).
				Str("requested_recipient", requestedRecipient).
				Msg("subscribe_inbox identity mismatch")
			s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired, Error: "subscribe_inbox: recipient must match authenticated identity"})
			s.addBanScore(conn, banIncrementInvalidSig)
			return true
		}
		reply, sub := s.subscribeInboxFrame(conn, frame)
		s.writeJSONFrame(conn, reply)
		if sub != nil {
			go s.pushBacklogToSubscriber(sub)
		}
		// Reverse subscription: subscribe on the outbound peer so that this
		// node receives both the stored backlog and live updates for its own
		// identity.  subscribe_inbox is a superset of the old request_inbox
		// (it pushes backlog AND registers a live subscriber), so a single
		// frame in each direction is sufficient for full bidirectional sync.
		// Sent here (not after auth_ok) so that short-lived connections like
		// syncPeer — which never send subscribe_inbox — are not affected.
		s.writeJSONFrame(conn, protocol.Frame{
			Type:       "subscribe_inbox",
			Topic:      "dm",
			Recipient:  s.identity.Address,
			Subscriber: s.cfg.AdvertiseAddress,
		})
		return true
	case "subscribed":
		// Response to our reverse subscribe_inbox sent to the outbound peer.
		// The outbound side has registered us as a subscriber and will push
		// backlog + live updates.  Nothing to do here — just acknowledge.
		log.Info().Str("recipient", frame.Recipient).Str("subscriber", frame.Subscriber).Msg("reverse subscription confirmed by outbound peer")
		return true
	case "push_message":
		s.handleInboundPushMessage(conn, frame)
		return true
	case "push_delivery_receipt":
		s.handleInboundPushDeliveryReceipt(conn, frame)
		return true
	case "relay_delivery_receipt":
		// Gossip receipt path: a peer forwards a delivery receipt via
		// the flat-field format (ID, Address, Recipient, Status,
		// DeliveredAt) rather than the ReceiptFrame used by push.
		// Without this handler the frame hits unknown_command and
		// kills the connection — breaking receipt delivery for client
		// nodes whose only return path is gossip through a full node.
		//
		// Deliberately separated from "send_delivery_receipt" (local-only
		// RPC command) to maintain the command-isolation boundary.
		s.handleInboundRelayDeliveryReceipt(conn, frame)
		return true
	case "push_notice":
		s.handleInboundPushNotice(frame)
		return true
	case "announce_peer":
		// Auth gate enforced above. Only authenticated peers may announce,
		// so we always promote.
		nodeType := frame.NodeType
		// node_type is validated for wire compatibility only. For third-party
		// gossip we learn the address, but we do not trust the sender to set
		// or override the announced peer's local role.
		if !isKnownNodeType(nodeType) {
			s.writeJSONFrame(conn, protocol.Frame{Type: "announce_peer_ack"})
			return true
		}
		peers := frame.Peers
		if len(peers) > maxAnnouncePeers {
			peers = peers[:maxAnnouncePeers]
		}
		for _, peer := range peers {
			if peer == "" || classifyAddress(domain.PeerAddress(peer)) == domain.NetGroupLocal {
				continue
			}
			s.promotePeerAddress(domain.PeerAddress(peer))
		}
		s.writeJSONFrame(conn, protocol.Frame{Type: "announce_peer_ack"})
		return true
	case "relay_message":
		// Auth gate enforced above (INV-9).
		if admit := admitRelayFrame(s.connHasCapability(conn, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			accepted = false
			return true
		}
		senderAddr := s.inboundPeerAddress(conn)
		if senderAddr == "" {
			accepted = false
			return true
		}
		// Look up an outbound session to the relay peer for on-demand
		// key sync. This is safe because the relay arrived on an inbound
		// connection — the outbound session is a separate conn/inboxCh,
		// so peerSessionRequest won't deadlock.
		syncSession, _ := s.activePeerSession(domain.PeerAddress(senderAddr))
		ackStatus := s.handleRelayMessage(domain.PeerAddress(senderAddr), syncSession, frame)
		// Write a single relay_hop_ack with the semantic status directly
		// on the inbound connection. This is the only ack the sender
		// receives — handleRelayMessage itself does not send acks.
		// Empty status means the message was dropped (dedupe, max hops,
		// client node); no ack is sent for drops (INV-5).
		if ackStatus != "" {
			s.writeJSONFrame(conn, protocol.Frame{
				Type:   "relay_hop_ack",
				ID:     frame.ID,
				Status: ackStatus,
			})
		}
		return true
	case "relay_hop_ack":
		// Auth gate enforced above.
		if admit := admitRelayFrame(s.connHasCapability(conn, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			accepted = false
			return true
		}
		senderAddr := s.inboundPeerAddress(conn)
		if senderAddr != "" {
			s.handleRelayHopAck(domain.PeerAddress(senderAddr), frame)
		}
		return true
	case "announce_routes":
		// Auth gate enforced above.
		if !s.connHasCapability(conn, domain.CapMeshRoutingV1) {
			accepted = false
			return true
		}
		// A routing-only peer (mesh_routing_v1 without mesh_relay_v1) can
		// advertise routes, but this node can never send relay_message to
		// it, making every route through it data-plane unusable. Reject
		// announcements from such peers to avoid storing dead NextHop entries.
		if !s.connHasCapability(conn, domain.CapMeshRelayV1) {
			accepted = false
			return true
		}
		senderIdentity := s.inboundPeerIdentity(conn)
		s.handleAnnounceRoutes(senderIdentity, frame)
		return true
	case "file_command":
		// Auth gate enforced above. Capability check: file_transfer_v1 must
		// be negotiated.
		if !s.connHasCapability(conn, domain.CapFileTransferV1) {
			accepted = false
			return true
		}
		// Pass the inbound peer identity so the file router applies
		// split-horizon forwarding and never reflects the frame back
		// to the neighbor that just delivered it.
		s.handleFileCommandFrame(json.RawMessage(line), s.inboundPeerIdentity(conn))
		return true
	default:
		accepted = false
		s.writeJSONFrameSync(conn, protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand})
		return false
	}
}

// p2pWireCommands is the set of commands that belong to the authenticated
// P2P wire protocol. Used by the auth gate in dispatchNetworkFrame to
// distinguish "command exists but you need auth" (→ auth_required) from
// "command does not exist on this port" (→ unknown_command).
var p2pWireCommands = map[string]bool{
	"get_peers":              true,
	"fetch_contacts":         true,
	"ack_delete":             true,
	"subscribe_inbox":        true,
	"subscribed":             true,
	"push_message":           true,
	"push_delivery_receipt":  true,
	"relay_delivery_receipt": true,
	"push_notice":            true,
	"announce_peer":          true,
	"relay_message":          true,
	"relay_hop_ack":          true,
	"announce_routes":        true,
	"file_command":           true,
}

// isP2PWireCommand returns true if the command name belongs to the
// authenticated P2P wire protocol handled by dispatchNetworkFrame.
func isP2PWireCommand(cmd string) bool {
	return p2pWireCommands[cmd]
}

func (s *Service) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	resp := s.handleLocalFrameDispatch(frame)
	accepted := resp.Type != "error"
	log.Trace().
		Str("protocol", "json/local").
		Str("addr", "local").
		Str("direction", "recv").
		Str("command", frame.Type).
		Bool("accepted", accepted).
		Msg("protocol_trace")
	return resp
}

func (s *Service) handleLocalFrameDispatch(frame protocol.Frame) protocol.Frame {
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
		return protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
	case "get_peers":
		// Local RPC: unfiltered merge of active CM slots + PeerProvider
		// candidates, no network group filtering.
		exchanged := s.buildPeerExchangeResponse(nil)
		peers := make([]string, len(exchanged))
		for i, a := range exchanged {
			peers[i] = string(a)
		}
		return protocol.Frame{
			Type:  "peers",
			Count: len(peers),
			Peers: peers,
		}
	case "fetch_identities":
		return s.identitiesFrame()
	case "fetch_contacts":
		return s.contactsFrame()
	case "fetch_trusted_contacts":
		return s.trustedContactsFrame()
	case "delete_trusted_contact":
		return s.deleteTrustedContactFrame(domain.PeerIdentity(frame.Address))
	case "fetch_peer_health":
		return s.peerHealthFrame()
	case "fetch_network_stats":
		return s.networkStatsFrame()
	case "fetch_aggregate_status":
		return s.aggregateStatusFrame()
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
	case "add_peer":
		return s.addPeerFrame(frame)
	case "fetch_dm_headers":
		return s.fetchDMHeadersFrame()
	case "fetch_relay_status":
		return s.relayStatusFrame()
	case "fetch_reachable_ids":
		return s.reachableIDsFrame()
	default:
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeUnknownCommand}
	}
}

// netCoreFor returns the NetCore registered for this connection, or nil
// if the connection predates the managed writer path (bootstrap dial edges
// in syncPeer / routing_integration that intentionally stay on direct
// io.WriteString — see migration doc 4.4). Inbound and outbound peer
// sessions are both registered here after PR 2 of checkpoint 9.2.
func (s *Service) netCoreFor(conn net.Conn) *NetCore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.conns[conn]
	if entry == nil {
		return nil
	}
	return entry.core
}

// meteredFor returns the MeteredConn wrapper for the given conn, or nil if
// the conn is not registered or was not wrapped in a MeteredConn (e.g.
// outbound dials that do not measure bytes).
func (s *Service) meteredFor(conn net.Conn) *MeteredConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.conns[conn]
	if entry == nil {
		return nil
	}
	return entry.metered
}

// isInboundTracked returns true when the conn has been promoted via
// trackInboundConnect (auth complete or auth not required) and has not
// been untracked yet.
func (s *Service) isInboundTracked(conn net.Conn) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.conns[conn]
	if entry == nil {
		return false
	}
	return entry.tracked
}

const connWriteTimeout = 30 * time.Second

// connWriter is now replaced by NetCore.writerLoop(). The single writer
// goroutine per inbound connection is started inside newNetCore() and
// drains NetCore.sendCh. See net_core.go for sendItem and the implementation.

type enqueueResult int

const (
	enqueueSent enqueueResult = iota // data accepted into the channel
	// enqueueUnregistered signals that the conn is not in s.conns.
	// After PR 2 every live connection — inbound via registerInboundConn and
	// outbound via attachOutboundNetCore — is registered before any send,
	// so this outcome now means "invariant violation / state inconsistency",
	// not "legitimate outbound peer session without send channel". Callers
	// must fail closed on this result; bypassing the managed writer with a
	// direct conn.Write would reintroduce the broken single-writer property
	// the migration is closing.
	enqueueUnregistered
	enqueueDropped // channel full or closed — data lost, conn closing
)

// String returns a stable machine-friendly label for each enqueueResult,
// used by emitProtocolTrace to surface the effective send outcome in
// observability. The labels are part of the log contract — renaming them
// is an operations-visible change.
func (r enqueueResult) String() string {
	switch r {
	case enqueueSent:
		return "sent"
	case enqueueUnregistered:
		return "unregistered"
	case enqueueDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

// enqueueFrame sends the serialised bytes to the per-connection writer
// goroutine via NetCore.sendRaw. Fire-and-forget: the caller does not
// wait for the write.
func (s *Service) enqueueFrame(conn net.Conn, data []byte) enqueueResult {
	pc := s.netCoreFor(conn)
	if pc == nil {
		return enqueueUnregistered
	}
	switch st := pc.sendRaw(data); st {
	case sendOK:
		return enqueueSent
	case sendBufferFull:
		// Peer too slow — evict by closing the connection.
		log.Warn().Str("addr", conn.RemoteAddr().String()).Msg("send buffer full, disconnecting slow peer")
		_ = conn.Close()
		return enqueueDropped
	case sendChanClosed:
		// Connection already shutting down, don't close again.
		return enqueueDropped
	default:
		// sendStatusInvalid or any unexpected value — programming error.
		log.Error().Str("addr", conn.RemoteAddr().String()).Str("status", st.String()).Msg("enqueueFrame: unexpected sendStatus")
		return enqueueDropped
	}
}

const syncFlushTimeout = 5 * time.Second

// enqueueFrameSync sends the serialised bytes to the per-connection writer
// goroutine via NetCore.sendRawSync and blocks until the writer has handed
// the data to the socket.
// Used by writeJSONFrameSync for error-path frames that must be delivered
// before the connection is torn down.
func (s *Service) enqueueFrameSync(conn net.Conn, data []byte) enqueueResult {
	pc := s.netCoreFor(conn)
	if pc == nil {
		return enqueueUnregistered
	}
	// Inbound error paths use fast-fail semantics: a saturated queue
	// means the peer is unresponsive and must be evicted rather than
	// kept alive while the caller blocks. Outbound control-plane writes
	// that must not be starved by fire-and-forget traffic use the
	// sendRawSyncBlocking variant via peerSessionRequest — that path
	// does not reach this helper.
	switch st := pc.sendRawSync(data); st {
	case sendOK:
		return enqueueSent
	case sendBufferFull:
		log.Warn().Str("addr", conn.RemoteAddr().String()).Msg("send buffer full, disconnecting slow peer")
		_ = conn.Close()
		return enqueueDropped
	case sendTimeout:
		log.Warn().Str("addr", conn.RemoteAddr().String()).Msg("sync flush timeout, disconnecting peer")
		_ = conn.Close()
		return enqueueDropped
	case sendWriterDone, sendChanClosed:
		// Connection already dying — don't close, let handleConn's
		// deferred cleanup do it.
		return enqueueDropped
	default:
		// sendStatusInvalid or any unexpected value — programming error.
		log.Error().Str("addr", conn.RemoteAddr().String()).Str("status", st.String()).Msg("enqueueFrameSync: unexpected sendStatus")
		return enqueueDropped
	}
}

// sendFrameToIdentity sends a protocol frame to the peer identified by its
// Ed25519 identity fingerprint. It searches outbound sessions first, then
// inbound connections, checking that the matched connection has the required
// capability. This is the identity-based counterpart of sendFrameToAddress.
//
// Returns true if the frame was accepted into the peer's write queue.
func (s *Service) sendFrameToIdentity(dst domain.PeerIdentity, frame protocol.Frame, requiredCap domain.Capability) bool {
	s.mu.RLock()
	now := time.Now().UTC()
	var outbound []*peerSession
	var inbound []*NetCore

	// 1. Outbound sessions — preferred path, one writer per session (servePeerSession).
	//
	// Activation gate: outbound bring-up inserts into s.sessions BEFORE
	// calling markPeerConnected (see openPeerSession / openPeerSessionForCM).
	// During that window health == nil for this address, and the session is
	// not yet authoritative for outbound sends. Require a present and
	// Connected health entry — missing health means "not yet activated" and
	// must be treated as a reject, not as an accept.
	for _, sess := range s.sessions {
		if sess.peerIdentity != dst || sess.conn == nil {
			continue
		}
		if !hasCapability(sess.capabilities, requiredCap) {
			continue
		}
		health := s.health[s.resolveHealthAddress(sess.address)]
		if health == nil || !health.Connected || s.computePeerStateAtLocked(health, now) == peerStateStalled {
			continue
		}
		outbound = append(outbound, sess)
	}

	// 2. Inbound connections — fallback, one writer per NetCore.
	//
	// The registry (s.conns) now holds both directions after
	// PR 2, but outbound entries must stay invisible on this identity
	// fallback path until they are promoted through s.sessions + health
	// (visibility boundary enforced by step 1 above and by
	// markPeerConnected). An outbound NetCore that has completed handshake
	// but is not yet in s.sessions would otherwise be reachable here with
	// health == nil and bypass the activation gate — so skip Outbound and
	// only consider Inbound NetCores on the fallback path.
	//
	// Same activation gate applies for inbound: registerInboundConn creates
	// the NetCore before hello/auth, and identity/capabilities land before
	// markPeerConnected. Require health != nil && Connected here too so a
	// partially-handshaken inbound NetCore cannot receive identity-routed
	// frames ahead of activation.
	for _, entry := range s.conns {
		if entry == nil {
			continue
		}
		pc := entry.core
		if pc == nil || pc.Dir() != Inbound || pc.Identity() != dst {
			continue
		}
		if !pc.HasCapability(requiredCap) {
			continue
		}
		health := s.health[s.resolveHealthAddress(pc.Address())]
		if health == nil || !health.Connected || s.computePeerStateAtLocked(health, now) == peerStateStalled {
			continue
		}
		inbound = append(inbound, pc)
	}

	s.mu.RUnlock()

	for _, sess := range outbound {
		if tryEnqueuePeerSessionFrame(sess, frame) {
			return true
		}
	}

	if len(inbound) == 0 {
		return false
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		log.Warn().Err(err).
			Str("peer", string(dst)).
			Str("frame_type", frame.Type).
			Msg("sendFrameToIdentity: marshal failed")
		return false
	}

	data := []byte(line)
	for _, target := range inbound {
		// NetCore writer loop retains the slice after enqueue, so each
		// candidate gets its own immutable copy.
		payload := append([]byte(nil), data...)
		if target.sendRaw(payload) == sendOK {
			return true
		}
	}
	return false
}

// tryEnqueuePeerSessionFrame performs a non-blocking send to an outbound
// session queue. It recovers from send-on-closed-channel so callers can
// safely race with future teardown changes.
func tryEnqueuePeerSessionFrame(sess *peerSession, frame protocol.Frame) (accepted bool) {
	defer func() {
		if recover() != nil {
			accepted = false
		}
	}()

	select {
	case sess.sendCh <- frame:
		return true
	default:
		return false
	}
}

func (s *Service) writeJSONFrame(conn net.Conn, frame protocol.Frame) {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		data := append(fallback, '\n')
		res := s.enqueueFrame(conn, data)
		emitProtocolTrace(conn, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(conn, frame, "writeJSONFrame.marshal_fallback")
		}
		return
	}
	res := s.enqueueFrame(conn, []byte(line))
	emitProtocolTrace(conn, frame, res)
	if res == enqueueUnregistered {
		logUnregisteredWrite(conn, frame, "writeJSONFrame")
	}
}

// enqueueSessionFrame is the session-scoped counterpart of enqueueFrame:
// instead of re-resolving the transport via s.conns (netCoreFor), it uses
// the NetCore that the peerSession already owns. Session-local reply paths
// — pong on outbound sessions, push_message/push_delivery_receipt on
// respondToInboxRequest, subscribe_inbox reply — must not fail closed with
// unregistered_write just because the registry entry has been reaped or
// never existed (tests); the authoritative writer is session.netCore.
//
// TRANSLATION (RU): аналог enqueueFrame, но без обращения к s.conns —
// writer берётся напрямую из session.netCore. Используется на всех
// session-local ответных путях, где peerSession и так держит
// authoritative transport owner. Снимает зависимость reply-путей от
// реестра s.conns и устраняет класс unregistered_write на валидных
// сессиях (см. P1 ревью 9.4a).
func (s *Service) enqueueSessionFrame(session *peerSession, data []byte) enqueueResult {
	if session == nil || session.netCore == nil {
		return enqueueUnregistered
	}
	switch st := session.netCore.sendRaw(data); st {
	case sendOK:
		return enqueueSent
	case sendBufferFull:
		addr := "unknown"
		if session.conn != nil {
			addr = session.conn.RemoteAddr().String()
		}
		log.Warn().Str("addr", addr).Msg("send buffer full, disconnecting slow peer")
		if session.conn != nil {
			_ = session.conn.Close()
		}
		return enqueueDropped
	case sendChanClosed:
		return enqueueDropped
	default:
		addr := "unknown"
		if session.conn != nil {
			addr = session.conn.RemoteAddr().String()
		}
		log.Error().Str("addr", addr).Str("status", st.String()).Msg("enqueueSessionFrame: unexpected sendStatus")
		return enqueueDropped
	}
}

// writeSessionFrame marshals the frame and enqueues it on session.netCore
// via enqueueSessionFrame. Use this on session-local reply paths where the
// peerSession is the authoritative transport owner; callers must not use
// writeJSONFrame(session.conn, ...) on these paths because that re-resolves
// the transport through s.conns and fails closed whenever the session
// exists without a matching registry entry (addressed in P1 review 9.4a).
//
// TRANSLATION (RU): обёртка над enqueueSessionFrame: сериализует фрейм и
// маршрутизирует его через session.netCore. Применять на всех
// session-local reply-путях — pong, push_message из respondToInboxRequest,
// subscribe_inbox ответ — где peerSession уже владеет authoritative
// transport'ом. Заменяет небезопасный writeJSONFrame(session.conn, …) на
// этих путях.
func (s *Service) writeSessionFrame(session *peerSession, frame protocol.Frame) {
	var conn net.Conn
	if session != nil {
		conn = session.conn
	}
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		data := append(fallback, '\n')
		res := s.enqueueSessionFrame(session, data)
		emitProtocolTrace(conn, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(conn, frame, "writeSessionFrame.marshal_fallback")
		}
		return
	}
	res := s.enqueueSessionFrame(session, []byte(line))
	emitProtocolTrace(conn, frame, res)
	if res == enqueueUnregistered {
		logUnregisteredWrite(conn, frame, "writeSessionFrame")
	}
}

// writeJSONFrameSync serialises a protocol frame and blocks until the
// per-connection writer goroutine has handed the bytes to the socket.
// Use this instead of writeJSONFrame on error paths where the caller is
// about to return false and the deferred cleanup will close the connection:
// the sync variant guarantees the error frame reaches the wire before
// teardown, preserving the "write completed before return" contract that
// error-path callers rely on.
func (s *Service) writeJSONFrameSync(conn net.Conn, frame protocol.Frame) {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		fallback, _ := json.Marshal(protocol.Frame{Type: "error", Code: protocol.ErrCodeEncodeFailed, Error: err.Error()})
		data := append(fallback, '\n')
		res := s.enqueueFrameSync(conn, data)
		emitProtocolTrace(conn, frame, res)
		if res == enqueueUnregistered {
			logUnregisteredWrite(conn, frame, "writeJSONFrameSync.marshal_fallback")
		}
		return
	}
	res := s.enqueueFrameSync(conn, []byte(line))
	emitProtocolTrace(conn, frame, res)
	if res == enqueueUnregistered {
		logUnregisteredWrite(conn, frame, "writeJSONFrameSync")
	}
}

// emitProtocolTrace writes the send-side protocol_trace entry after the
// enqueue outcome is known. accepted reflects the *effective* send result:
// a frame that was dropped (unregistered conn, buffer full, writer done)
// must not show up as accepted=true — otherwise operators would see a
// successful trace followed by a drop log for the same frame, which is
// exactly the observability lie the PR 3 reviewer flagged. For a non-error
// frame, accepted == true only when the frame actually reached the managed
// writer queue (enqueueSent); any drop path forces accepted=false and adds
// the concrete outcome so the trace is self-describing.
func emitProtocolTrace(conn net.Conn, frame protocol.Frame, res enqueueResult) {
	addr := ""
	if conn != nil {
		if ra := conn.RemoteAddr(); ra != nil {
			addr = ra.String()
		}
	}
	ev := log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", addr).
		Str("direction", "send").
		Str("command", frame.Type).
		Str("send_outcome", res.String()).
		Bool("accepted", frame.Type != "error" && res == enqueueSent)
	if frame.Type == "error" {
		ev = ev.Str("code", frame.Code).Str("error", frame.Error)
	}
	ev.Msg("protocol_trace")
}

// logUnregisteredWrite records the invariant violation that the reviewer
// behind PR 3 is asking us to surface: after PR 2 every live connection
// must be registered in s.conns before the first write — an
// enqueueUnregistered outcome means the single-writer invariant was
// attempted to be bypassed. The frame is dropped (fail closed) rather than
// slipped around the managed writer via a direct conn.Write: a silent direct
// write would reintroduce exactly the broken state the migration is closing.
// The log includes origin, remote address, and the command type so the
// responsible call-site is immediately identifiable in production logs.
func logUnregisteredWrite(conn net.Conn, frame protocol.Frame, origin string) {
	addr := ""
	if conn != nil {
		if ra := conn.RemoteAddr(); ra != nil {
			addr = ra.String()
		}
	}
	log.Error().
		Str("origin", origin).
		Str("addr", addr).
		Str("command", frame.Type).
		Msg("unregistered_write: conn missing NetCore — single-writer invariant violation, frame dropped")
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
		Node:                   nodeName,
		Network:                networkName,
		Listen:                 listen,
		Listener:               listenerFlag(s.cfg.EffectiveListenerEnabled()),
		NodeType:               string(s.NodeType()),
		ClientVersion:          s.ClientVersion(),
		ClientBuild:            config.ClientBuild,
		Services:               s.Services(),
		Address:                s.identity.Address,
		PubKey:                 identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:                 s.selfBoxSig,
		ObservedAddress:        observedAddr,
		Challenge:              challenge,
		Capabilities:           localCapabilityStrings(),
	}
}

func validateProtocolHandshake(frame protocol.Frame) error {
	if frame.Version < config.MinimumProtocolVersion {
		return fmt.Errorf("protocol version %d is too old; supported %d..%d", frame.Version, config.MinimumProtocolVersion, config.ProtocolVersion)
	}
	return nil
}

// isConnAuthenticated returns true when the connection has completed
// session auth (auth_session verified). Connections that never initiated
// auth (NetCore.auth is nil) are considered unauthenticated — they may
// still issue handshake commands, but they should not trigger high-trust
// side effects such as peer promotion.
func (s *Service) isConnAuthenticated(conn net.Conn) bool {
	state := s.ConnAuthState(conn)
	return state != nil && state.Verified
}

// isAuthInitiated returns true if the connection has started (challenge
// issued, Verified=false) or completed (Verified=true) the auth handshake.
// Used by the re-hello guard to block identity/address overwrites once
// PrepareAuth has recorded the initial hello and challenge.
func (s *Service) isAuthInitiated(conn net.Conn) bool {
	return s.ConnAuthState(conn) != nil
}

func ackDeletePayload(address, ackType, id, status string) []byte {
	return []byte("corsa-ack-delete-v1|" + address + "|" + ackType + "|" + id + "|" + status)
}

// handleAuthSession verifies the auth_session frame's Ed25519 signature via
// connauth.VerifyAuthSession, then applies post-auth side effects (peer
// learning, route registration, peer announcement, health tracking).
//
// Ban scoring for invalid signatures is applied here because it depends on
// the connection rate limiter which lives in Service.
func (s *Service) handleAuthSession(conn net.Conn, frame protocol.Frame) (protocol.Frame, bool) {
	state := s.ConnAuthState(conn)
	verified, reply, ok := connauth.VerifyAuthSession(state, frame)
	if !ok {
		if reply.Code == protocol.ErrCodeInvalidAuthSignature {
			s.addBanScore(conn, banIncrementInvalidSig)
		}
		return reply, false
	}
	// Already verified — idempotent re-auth returns success immediately.
	if state != nil && state.Verified {
		return reply, true
	}

	s.SetConnAuthState(conn, verified)

	s.learnPeerFromFrame(conn.RemoteAddr().String(), verified.Hello)
	s.registerHelloRoute(conn, verified.Hello)

	// Announce the newly authenticated peer to all active outbound sessions.
	// Only direct neighbors are notified (no recursive relay) and local
	// addresses are excluded to avoid leaking private network topology.
	if addr := peerListenAddress(verified.Hello); addr != "" && classifyAddress(domain.PeerAddress(addr)) != domain.NetGroupLocal {
		go s.announcePeerToSessions(addr, verified.Hello.NodeType)
	}

	if addr := s.inboundPeerAddress(conn); addr != "" {
		// Log duplicate but allow: see the hello-path comment for the
		// full rationale — rejecting breaks one-way gossip when both
		// sides dial simultaneously.
		if s.hasOutboundSessionForInbound(addr) {
			log.Info().Str("peer", string(addr)).Msg("duplicate_inbound_auth_session_allowed")
		}
		s.addPeerID(addr, domain.PeerIdentity(verified.Hello.Address))
		s.trackInboundConnect(conn, addr, domain.PeerIdentity(verified.Hello.Address))
		s.addPeerVersion(addr, verified.Hello.ClientVersion)
		s.addPeerBuild(addr, verified.Hello.ClientBuild)
	}

	return reply, true
}

func (s *Service) authenticatedAddressForConn(conn net.Conn) (protocol.Frame, bool) {
	pc := s.netCoreFor(conn)
	if pc == nil {
		return protocol.Frame{}, false
	}
	state := pc.Auth()
	if state == nil || !state.Verified {
		return protocol.Frame{}, false
	}
	return state.Hello, true
}

func (s *Service) clearConnAuth(conn net.Conn) {
	if pc := s.netCoreFor(conn); pc != nil {
		pc.ClearAuth()
	}
}

// ConnAuthState implements connauth.AuthStore. Returns the auth state for
// the connection, or nil if the connection is not registered or has no
// auth state.
func (s *Service) ConnAuthState(conn net.Conn) *connauth.State {
	pc := s.netCoreFor(conn)
	if pc == nil {
		return nil
	}
	return pc.Auth()
}

// SetConnAuthState implements connauth.AuthStore. Stores auth state for
// the connection. The connection must already be registered.
func (s *Service) SetConnAuthState(conn net.Conn, state *connauth.State) {
	if pc := s.netCoreFor(conn); pc != nil {
		pc.SetAuth(state)
	}
}

// rememberConnPeerAddr populates the NetCore with identity, address,
// capabilities, and networks from the hello frame. All subsequent lookups
// use NetCore as the single source of truth for inbound connection state.
//
// The identity field stores the Ed25519 fingerprint (hello.Address) for
// routing purposes. The address field stores the listen address for health
// tracking. These are kept separate because a NATed peer's listen address
// (e.g. 127.0.0.1:64646) is not a valid identity fingerprint.
func (s *Service) rememberConnPeerAddr(conn net.Conn, hello protocol.Frame) {
	addr := strings.TrimSpace(hello.Listen)
	if addr == "" {
		addr = strings.TrimSpace(hello.Address)
	}
	pc := s.netCoreFor(conn)
	if pc == nil {
		return
	}
	pc.ApplyOpts(NetCoreOpts{
		Address:      domain.PeerAddress(addr),
		Identity:     domain.PeerIdentity(strings.TrimSpace(hello.Address)),
		LastActivity: time.Now().UTC(),
		Networks:     domain.ParseNetGroups(hello.Networks),
		Caps:         intersectCapabilities(localCapabilities(), hello.Capabilities),
	})
}

// inboundPeerAddress returns the overlay address declared by the
// remote peer during the hello handshake, or "" if no address is
// known yet. The returned address is suitable for health tracking.
func (s *Service) inboundPeerAddress(conn net.Conn) domain.PeerAddress {
	pc := s.netCoreFor(conn)
	if pc == nil {
		return ""
	}
	return pc.Address()
}

// trackedInboundPeerAddress returns the peer overlay address only when
// this specific connection has been promoted via trackInboundConnect
// (i.e. after successful authentication or for peers that do not require
// auth). Returns "" if the peer address is unknown or this connection
// has not been promoted, preventing unauthenticated connections from
// creating or refreshing health entries — even if another legitimate
// connection for the same address is already tracked.
func (s *Service) trackedInboundPeerAddress(conn net.Conn) domain.PeerAddress {
	s.mu.RLock()
	entry := s.conns[conn]
	s.mu.RUnlock()
	if entry == nil || !entry.tracked || entry.core == nil {
		return ""
	}
	return entry.core.Address()
}

// trackInboundConnect increments the inbound connection reference count
// for the given overlay address, marks the concrete connection as promoted,
// and marks the peer as connected when this is the first active inbound
// connection for that address. peerIdentity is the Ed25519 fingerprint
// from the hello/auth frame — used for routing table registration instead
// of the transport address.
func (s *Service) trackInboundConnect(conn net.Conn, address domain.PeerAddress, peerIdentity domain.PeerIdentity) {
	s.mu.Lock()
	resolved := s.resolveHealthAddress(address)
	first := s.inboundHealthRefs[resolved] == 0
	s.inboundHealthRefs[resolved]++
	if e := s.conns[conn]; e != nil {
		e.tracked = true
	}
	s.mu.Unlock()

	log.Info().Str("node", s.identity.Address).Str("peer_identity", string(peerIdentity)).Str("address", string(address)).Str("resolved", string(resolved)).Bool("first", first).Msg("track_inbound_connect")

	if first {
		s.markPeerConnected(resolved, peerDirectionInbound)
	}

	// Routing table: register direct peer using the identity fingerprint,
	// not the transport address. The relay capability flag ensures only
	// peers that can accept relay_message become direct routes.
	s.onPeerSessionEstablished(peerIdentity, s.connHasCapability(conn, domain.CapMeshRelayV1))

	// Send full table sync to the inbound peer (Phase 1.2: full sync on
	// connect, symmetric with the outbound path).
	// Both capabilities required: mesh_routing_v1 (understands announce_routes)
	// and mesh_relay_v1 (can carry relay traffic). A routing-only peer would
	// learn routes it cannot deliver on the data plane.
	if s.connHasCapability(conn, domain.CapMeshRoutingV1) && s.connHasCapability(conn, domain.CapMeshRelayV1) {
		s.sendFullTableSyncToInbound(conn, peerIdentity)
	}

	// Drain fire-and-forget frames (push_message, push_notice) that were
	// queued for this peer before the inbound connection was authenticated.
	// The outbound session might not exist (CM slot full), but the inbound
	// conn can carry these frames. Only fire-and-forget frames are safe to
	// send on the inbound conn because they don't expect a response that
	// would interleave with the peer's request/reply traffic.
	s.flushPendingFireAndForget(conn, resolved)
}

// trackInboundDisconnect decrements the inbound connection reference count
// and removes the per-connection tracked flag.
// Only when the last tracked connection for an address closes is the peer
// marked as disconnected — earlier closes are silent so that the health
// row stays connected while at least one TCP session remains alive.
// If trackInboundConnect was never called for this connection (e.g. auth
// failed), the disconnect is silently ignored to avoid creating phantom
// health entries for unauthenticated connections.
func (s *Service) trackInboundDisconnect(conn net.Conn, address domain.PeerAddress) {
	s.mu.Lock()
	var wasTracked bool
	if e := s.conns[conn]; e != nil {
		wasTracked = e.tracked
		e.tracked = false
	}
	resolved := s.resolveHealthAddress(address)
	peerIdentity := s.peerIDs[resolved]
	var last bool
	if wasTracked && s.inboundHealthRefs[resolved] > 0 {
		s.inboundHealthRefs[resolved]--
		last = s.inboundHealthRefs[resolved] == 0
		if last {
			delete(s.inboundHealthRefs, resolved)
		}
	}
	s.mu.Unlock()

	if last {
		s.markPeerDisconnected(resolved, nil)

		// Notify the ConnectionManager that an inbound slot freed up.
		// This is a hint (non-blocking, safe to drop) — CM may choose
		// to backfill the lost inbound with an outgoing dial.
		if s.connManager != nil {
			ip, _, _ := splitHostPort(string(resolved))
			s.connManager.EmitHint(InboundClosed{IP: ip, Identity: peerIdentity})
		}
	}

	// Routing table: deregister direct peer when the last relay-capable
	// inbound session closes. Uses the identity fingerprint, not transport
	// address, to match what was passed to onPeerSessionEstablished.
	if wasTracked {
		s.onPeerSessionClosed(peerIdentity, s.connHasCapability(conn, domain.CapMeshRelayV1))
	}
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
func (s *Service) connPeerReachableGroups(conn net.Conn) map[domain.NetGroup]struct{} {
	pc := s.netCoreFor(conn)

	if pc == nil {
		return nil
	}

	// Authenticated peers: trust declared networks and advertised address
	// from the hello frame — the identity behind these claims has been
	// verified by auth_session.
	if s.isConnAuthenticated(conn) {
		if nets := pc.Networks(); len(nets) > 0 {
			return validateDeclaredNetworks(nets, pc.Address())
		}
		if addr := pc.Address(); addr != "" {
			g := classifyAddress(addr)
			if g != domain.NetGroupUnknown && g != domain.NetGroupLocal {
				return peerReachableGroups(addr)
			}
		}
		return nil
	}

	// Unauthenticated peers: do NOT trust hello-declared networks or
	// advertised address — both are attacker-controlled. An unauth peer
	// could claim .onion/.i2p reachability to extract overlay addresses
	// it has no proven right to access. Instead, classify by the actual
	// TCP remote address.
	if remote := conn.RemoteAddr(); remote != nil {
		host, _, err := net.SplitHostPort(remote.String())
		if err == nil {
			g := classifyAddress(domain.PeerAddress(host))
			if g != domain.NetGroupUnknown && g != domain.NetGroupLocal {
				return peerReachableGroups(domain.PeerAddress(host))
			}
		}
	}

	// Cannot determine network from TCP endpoint (e.g. Tor circuit,
	// localhost). Return nil so peersFrame includes all routable addresses.
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
func (s *Service) recordObservedAddress(peerID domain.PeerIdentity, observedIP string) {
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
	log.Warn().Int("count", bestCount).Str("observed_ip", best).Str("advertise", s.cfg.AdvertiseAddress).Msg("NAT detected")
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
		log.Debug().Str("addr", ip).Time("until", entry.Blacklisted).Msg("reject connection: blacklisted")
		return true
	}
	if !entry.Blacklisted.IsZero() && time.Now().UTC().After(entry.Blacklisted) {
		delete(s.bans, ip)
	}
	return false
}

// tryIncrementIPConn atomically checks and increments the per-IP inbound
// connection counter. Returns false if the IP has reached maxConnPerIP.
func (s *Service) tryIncrementIPConn(ip string) bool {
	if ip == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inboundByIP[ip] >= maxConnPerIP {
		return false
	}
	s.inboundByIP[ip]++
	return true
}

// decrementIPConn decrements the per-IP inbound connection counter.
func (s *Service) decrementIPConn(ip string) {
	if ip == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inboundByIP[ip] > 1 {
		s.inboundByIP[ip]--
	} else {
		delete(s.inboundByIP, ip)
	}
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
		log.Warn().Str("ip", ip).Int("score", entry.Score).Time("until", entry.Blacklisted).Msg("blacklist")
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

func (s *Service) deleteTrustedContactFrame(identity domain.PeerIdentity) protocol.Frame {
	identity = domain.PeerIdentity(strings.TrimSpace(string(identity)))
	if identity == "" {
		return protocol.Frame{Type: "error", Error: "address is required"}
	}

	_, err := s.trust.forget(identity)
	if err != nil {
		return protocol.Frame{Type: "error", Error: err.Error()}
	}

	// Drop pending outbound messages destined for the deleted contact.
	// The user explicitly removed this identity, so queued messages
	// should not be delivered.
	s.dropPendingForRecipient(string(identity))

	// If the contact was not in the trust store, that is not an error —
	// it may have originated from network discovery rather than the
	// trusted contacts list.
	return protocol.Frame{Type: "ok", Address: string(identity)}
}

// dropPendingForRecipient removes all pending send_message frames addressed
// to the given recipient across every peer queue. It also clears the
// corresponding outbound delivery tracking entries and pending dedup keys,
// then persists the updated queue state to disk.
func (s *Service) dropPendingForRecipient(recipient string) {
	s.mu.Lock()

	var dropped int
	for addr, frames := range s.pending {
		kept := frames[:0]
		for _, pf := range frames {
			if pf.Frame.Type == "send_message" && pf.Frame.Recipient == recipient {
				key := pendingFrameKey(addr, pf.Frame)
				delete(s.pendingKeys, key)
				dropped++
				continue
			}
			kept = append(kept, pf)
		}
		if len(kept) == 0 {
			delete(s.pending, addr)
		} else {
			s.pending[addr] = kept
		}
	}

	// Remove outbound delivery entries for the deleted recipient so the
	// UI no longer shows stale "queued"/"retrying" statuses.
	var outboundDropped int
	for id, ob := range s.outbound {
		if ob.Recipient == recipient {
			delete(s.outbound, id)
			outboundDropped++
		}
	}

	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()

	if dropped > 0 || outboundDropped > 0 {
		log.Info().Str("recipient", recipient).Int("pending_dropped", dropped).Int("outbound_dropped", outboundDropped).Msg("dropped_pending_for_deleted_contact")
		s.persistQueueState(snapshot)
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

func (s *Service) subscribeInboxFrame(conn net.Conn, frame protocol.Frame) (protocol.Frame, *subscriber) {
	topic := strings.TrimSpace(frame.Topic)
	recipient := strings.TrimSpace(frame.Recipient)
	if topic != "dm" || recipient == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSubscribeInbox}, nil
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
	log.Info().Str("recipient", recipient).Str("subscriber", subID).Int("active", count).Msg("subscribe_inbox")

	return protocol.Frame{
		Type:       "subscribed",
		Topic:      topic,
		Recipient:  recipient,
		Subscriber: subID,
		Status:     "ok",
		Count:      count,
	}, sub
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
	log.Debug().Str("recipient", recipient).Str("subscriber", subID).Int("active", len(s.subs[recipient])).Msg("route_via_hello")
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
	// When ConnectionManager is active it owns the outbound session
	// lifecycle — slots are filled continuously via the event loop.
	// Calling the legacy ensurePeerSessions here would bypass CM slot
	// accounting, retry/backoff and generation guards.
	if s.connManager != nil {
		return
	}

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
	log.Info().Str("node", s.identity.Address).Str("address", frame.Address).Str("type", frame.AckType).Str("id", frame.ID).Str("status", frame.Status).Int("removed", count).Msg("ack_delete_applied")
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
			log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Msg("storeIncomingMessage: unknown sender key")
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
	// DM senders are cryptographically verified above (VerifyEnvelope),
	// so they are safe to register as known identities unconditionally.
	// Non-DM senders are only registered when the node already holds
	// their public key — this prevents an attacker from injecting
	// arbitrary strings into the known-identities set via forged
	// sender fields on non-DM messages.
	if msg.Topic == "dm" {
		s.known[msg.Sender] = struct{}{}
	} else if _, hasPK := s.pubKeys[msg.Sender]; hasPK {
		s.known[msg.Sender] = struct{}{}
	}
	if msg.Recipient != "*" {
		s.known[msg.Recipient] = struct{}{}
	}
	if _, ok := s.seen[string(msg.ID)]; ok {
		count := len(s.topics[msg.Topic])
		s.mu.Unlock()
		log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("topic", msg.Topic).Int("topic_count", count).Msg("store_incoming_message_dedup")
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

	// Only messages that belong to this node (sender or recipient) get
	// persisted to chatlog, emit UI events, and push to local subscribers.
	// Transit messages (relayed DMs where neither party is us) have their
	// own persistence via queue-<port>.json / relayRetry and must NOT
	// pollute the local chat history or wake up the desktop UI.
	isLocal := s.isLocalMessage(msg)

	// Persist message via the registered MessageStore (owned by the desktop
	// layer) BEFORE adding to s.topics. The store result determines whether
	// the message enters in-memory state:
	//
	//   StoreInserted  → add to s.topics, emit event, gossip
	//   StoreDuplicate → skip s.topics (already in chatlog on disk),
	//                     skip event (no beep/unread). This closes both
	//                     the event-path and the DMHeaders header-path
	//                     that could otherwise re-trigger unread counts
	//                     via repairUnreadFromHeaders after a restart.
	//   StoreFailed    → add to s.topics (don't lose the message from
	//                     the network), skip event (stale data).
	//
	// When no store is registered (relay-only node) or for transit messages,
	// the message always enters s.topics.
	storeResult := StoreInserted
	if isLocal && s.messageStore != nil {
		isOutgoing := msg.Sender == s.identity.Address
		// Unlock before calling into the store — it may do SQLite I/O.
		s.mu.Unlock()
		storeResult = s.messageStore.StoreMessage(envelope, isOutgoing)
		s.mu.Lock()
	}

	// Duplicate local messages must NOT enter s.topics. The message is
	// already persisted in chatlog, and adding it here would cause
	// fetchDMHeadersFrame() to include it in DMHeaders, which lets
	// repairUnreadFromHeaders() re-increment unread counts on the UI.
	beforeCount := len(s.topics[msg.Topic])
	if storeResult != StoreDuplicate {
		s.topics[msg.Topic] = append(s.topics[msg.Topic], envelope)
	}
	count := len(s.topics[msg.Topic])
	// Collect existing IDs for duplicate diagnostics.
	var existingIDs []string
	if count > 1 {
		for _, e := range s.topics[msg.Topic] {
			existingIDs = append(existingIDs, string(e.ID))
		}
	}
	s.mu.Unlock()

	// Only notify the desktop UI for messages this node participates in.
	// Transit relay traffic must not wake up the UI.
	// Emit event only for genuinely new messages (StoreInserted).
	if isLocal && storeResult == StoreInserted {
		s.emitLocalChange(protocol.LocalChangeEvent{
			Type:       protocol.LocalChangeNewMessage,
			Topic:      msg.Topic,
			MessageID:  string(msg.ID),
			Sender:     msg.Sender,
			Recipient:  msg.Recipient,
			Body:       msg.Body,
			Flag:       string(msg.Flag),
			CreatedAt:  msg.CreatedAt.Format(time.RFC3339Nano),
			TTLSeconds: msg.TTLSeconds,
		})
	}

	_, callerFile, callerLine, _ := runtime.Caller(1)
	log.Info().Str("node", s.identity.Address).Str("topic", msg.Topic).Str("id", string(msg.ID)).Str("from", msg.Sender).Str("to", msg.Recipient).Str("flag", string(msg.Flag)).Int("before_count", beforeCount).Int("topic_count", count).Bool("is_local", isLocal).Str("caller", fmt.Sprintf("%s:%d", callerFile, callerLine)).Strs("topic_ids", existingIDs).Msg("stored message")

	// Push and gossip are independent delivery mechanisms:
	//
	// Push  — instant delivery to locally connected subscribers.
	//         An optimization; not a guarantee.  The subscriber may
	//         disconnect before the write completes.
	//
	// Gossip — mesh-wide propagation to peer nodes.  Ensures every
	//          relay in the network stores a copy so the recipient can
	//          retrieve the message from any node it reconnects to.
	//
	// Both happen unconditionally when applicable.  Push without gossip
	// would strand the message on this relay if the subscriber
	// reconnects to a different node.  Gossip without push would add
	// up to relayRetryTTL latency for a locally connected recipient.
	decision := s.router.Route(envelope)

	if len(decision.PushSubscribers) > 0 {
		go s.pushToSubscriberSnapshot(envelope, decision.PushSubscribers)
	}

	if s.shouldRouteStoredMessage(msg) {
		s.trackRelayMessage(envelope)

		// Gossip is the baseline delivery mechanism — always runs.
		// It ensures store-and-forward (transit DMs stored on relay
		// nodes for offline recipients), push delivery to connected
		// clients, and backlog drain. Never skip or filter gossip.
		go s.executeGossipTargets(envelope, decision.GossipTargets)

		// Table-directed relay (Phase 1.2): when the routing table knows a
		// next-hop for this recipient, send relay_message directly to that
		// peer. This is the primary directed delivery path that replaces
		// blind gossip relay for known routes. Gossip still runs above as
		// fallback — receivers dedupe via seen[messageID].
		if msg.Topic == "dm" && msg.Recipient != "" && msg.Recipient != "*" {
			if decision.RelayNextHop != nil {
				s.sendTableDirectedRelay(envelope, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops)
			} else {
				// No table route — fall back to blind gossip relay to
				// capable full nodes (pre-Phase 1.2 behavior).
				s.tryRelayToCapableFullNodes(envelope, decision.GossipTargets)
			}
		}
	}

	// Delivery receipts are emitted when this node IS the final recipient,
	// regardless of how the message arrived (gossip, relay, or local).
	// Receipt emission must not be gated by validateTimestamp — relayed
	// DMs arrive via deliverRelayedMessage which passes validateTimestamp=false,
	// but the recipient still needs to acknowledge delivery to the sender.
	// Skip for duplicates — the receipt was already sent on the first store.
	if isLocal && storeResult != StoreDuplicate && msg.Topic == "dm" && msg.Recipient != "*" {
		if msg.Recipient == s.identity.Address && msg.Sender != s.identity.Address {
			s.backgroundWg.Add(1)
			go func() {
				defer s.backgroundWg.Done()
				s.emitDeliveryReceipt(msg)
			}()
		}
	}

	return true, count, ""
}

// isLocalMessage returns true if this node is a party to the message
// (sender or recipient), meaning the message should be persisted locally.
// Broadcast messages (recipient="*") and global topics are always local.
// Transit DMs — where this node is merely relaying between two other parties —
// return false; their persistence is handled by queue-<port>.json / relayRetry.
func (s *Service) isLocalMessage(msg incomingMessage) bool {
	if msg.Topic != "dm" {
		return true // global/broadcast messages are always local
	}
	if msg.Recipient == "*" || msg.Recipient == "" {
		return true
	}
	return msg.Sender == s.identity.Address || msg.Recipient == s.identity.Address
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

	// Update delivery status via the registered MessageStore BEFORE emitting
	// local change so the desktop UI can safely read the new status from
	// SQLite when it reacts to the event. Same invariant as storeIncomingMessage.
	receiptStoreOK := true
	if s.messageStore != nil {
		receiptStoreOK = s.messageStore.UpdateDeliveryStatus(receipt)
	}

	if receiptStoreOK {
		s.emitLocalChange(protocol.LocalChangeEvent{
			Type:        protocol.LocalChangeReceiptUpdate,
			Topic:       "dm",
			MessageID:   string(receipt.MessageID),
			Sender:      receipt.Sender,
			Recipient:   receipt.Recipient,
			Status:      receipt.Status,
			DeliveredAt: receipt.DeliveredAt,
		})
	}

	log.Info().Str("message_id", string(receipt.MessageID)).Str("sender", receipt.Sender).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Time("delivered_at", receipt.DeliveredAt).Msg("stored delivery receipt")

	if receipt.Recipient == s.identity.Address {
		return true, count
	}

	s.trackRelayReceipt(receipt)
	// Try relay receipt return path first. Only fall back to gossip if the
	// relay chain could not forward (no state, or hop unavailable).
	relayForwarded := s.handleRelayReceipt(receipt)
	if !relayForwarded {
		go s.gossipReceipt(receipt)
	}
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
			if item.Frame.Type == "relay_delivery_receipt" &&
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

	if len(items) > 0 {
		ids := make([]string, len(items))
		for i, m := range items {
			ids[i] = m.ID
		}
		log.Debug().Str("node", s.identity.Address).Str("topic", topic).Int("count", len(items)).Strs("ids", ids).Msg("fetch_messages_result")
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

// countInboundConnsLocked returns the number of inbound entries currently
// in the primary registry. Outbound entries (created by
// attachOutboundNetCore) are not counted — the inbound cap only governs
// incoming TCP acceptance. Caller must hold s.mu at least for read.
func (s *Service) countInboundConnsLocked() int {
	n := 0
	for _, e := range s.conns {
		if e != nil && e.core != nil && e.core.Dir() == Inbound {
			n++
		}
	}
	return n
}

func (s *Service) registerInboundConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := s.cfg.EffectiveMaxIncomingPeers()
	if limit > 0 && s.countInboundConnsLocked() >= limit {
		return false
	}

	s.connIDCounter++
	pc := newNetCore(connID(s.connIDCounter), conn, Inbound, NetCoreOpts{})
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok && addr.IP.IsLoopback() {
		pc.SetLocal(true)
	}

	entry := &connEntry{core: pc}
	if mc, ok := conn.(*MeteredConn); ok {
		entry.metered = mc
	}
	s.conns[conn] = entry
	return true
}

// attachOutboundNetCore creates an outbound NetCore for the given dialled
// connection, registers it in s.conns (the single primary registry that
// holds both inbound and outbound entries after checkpoint 9.4a) and
// wires session.netCore / session.onClose so that peerSession.Close()
// removes the registration atomically with the NetCore teardown.
//
// The NetCore must exist before peerSessionRequest runs so that the welcome
// and auth frames are routed through the managed single-writer path instead
// of raw io.WriteString. This is the Phase 1 gate C1 — all outbound writes
// share the same back-pressure and deadline discipline as inbound.
func (s *Service) attachOutboundNetCore(session *peerSession) *NetCore {
	pc := newNetCore(connID(session.connID), session.conn, Outbound, NetCoreOpts{})

	s.mu.Lock()
	s.conns[session.conn] = &connEntry{core: pc}
	s.mu.Unlock()

	session.netCore = pc
	conn := session.conn
	session.onClose = func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}
	return pc
}

func (s *Service) unregisterInboundConn(conn net.Conn) {
	s.mu.Lock()
	entry := s.conns[conn]
	delete(s.conns, conn)
	s.mu.Unlock()

	// NetCore.Close() handles the full shutdown sequence:
	// 1. rawConn.Close() — unblocks writer stuck in conn.Write
	// 2. close(sendCh) — signals writer to drain and exit
	// 3. <-writerDone — waits for drain to complete
	//
	// Note: handleConn calls metered.Close() before unregisterInboundConn,
	// which is now redundant (NetCore.Close does it). The double Close on
	// net.Conn is safe — subsequent calls return an error but don't panic.
	if entry != nil && entry.core != nil {
		entry.core.Close()
	}
}

// closeAllInboundConns closes every tracked inbound connection so that
// handleConn goroutines unblock and exit. Called during graceful shutdown
// before connWg.Wait().
func (s *Service) closeAllInboundConns() {
	s.mu.Lock()
	inbound := make([]net.Conn, 0, len(s.conns))
	for c, e := range s.conns {
		if e == nil || e.core == nil || e.core.Dir() != Inbound {
			continue
		}
		inbound = append(inbound, c)
	}
	s.mu.Unlock()

	for _, c := range inbound {
		_ = c.Close()
	}
	if len(inbound) > 0 {
		log.Info().Int("count", len(inbound)).Msg("closed inbound connections for shutdown")
	}
}

// pushToSubscriberSnapshot delivers a message to a pre-captured snapshot of
// subscribers. The snapshot is taken under s.mu by the caller so that the
// decision "route exists → push, not gossip" and the actual send targets
// are determined atomically. If a subscriber's connection has broken by the
// time we write, the message is still safe in s.topics and will be delivered
// via backlog on the next subscribe_inbox.
func (s *Service) pushToSubscriberSnapshot(msg protocol.Envelope, subs []*subscriber) {
	defer crashlog.DeferRecover()
	if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
		return
	}
	log.Info().Str("id", string(msg.ID)).Str("topic", msg.Topic).Str("recipient", msg.Recipient).Int("subscribers", len(subs)).Msg("push_message")

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
	defer crashlog.DeferRecover()
	subs := s.subscribersForRecipient(receipt.Recipient)
	if len(subs) == 0 {
		return
	}
	log.Info().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Int("subscribers", len(subs)).Msg("push_delivery_receipt")

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
	defer crashlog.DeferRecover()
	if sub == nil || strings.TrimSpace(sub.recipient) == "" {
		return
	}

	inbox := s.fetchInboxFrame("dm", sub.recipient)
	log.Info().Str("node", s.identity.Address).Str("recipient", sub.recipient).Int("backlog_count", len(inbox.Messages)).Msg("pushBacklogToSubscriber")
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
		return protocol.Frame{Type: "notice_known", ID: id, ExpiresAt: existing.ExpiresAt.Unix()}
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

	return protocol.Frame{Type: "notice_stored", ID: id, ExpiresAt: expiresAt.Unix()}
}

func (s *Service) fetchNoticesFrame() protocol.Frame {
	s.cleanupExpiredNotices()

	s.mu.RLock()
	items := make([]protocol.NoticeFrame, 0, len(s.notices))
	for _, notice := range s.notices {
		items = append(items, protocol.NoticeFrame{
			ID:         notice.ID,
			ExpiresAt:  notice.ExpiresAt.Unix(),
			Ciphertext: notice.Ciphertext,
		})
	}
	s.mu.RUnlock()

	return protocol.Frame{Type: "notices", Count: len(items), Notices: items}
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
		ClientBuild:   config.ClientBuild,
		Services:      s.Services(),
		Networks:      reachableGroupNames(s.reachableGroups),
		Address:       s.identity.Address,
		PubKey:        identity.PublicKeyBase64(s.identity.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		BoxSig:        s.selfBoxSig,
		Capabilities:  localCapabilityStrings(),
	})
	if err != nil {
		return ""
	}
	return line
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
		if normalizedAddr, ok := s.normalizePeerAddress(domain.PeerAddress(frame.Listen), domain.PeerAddress(frame.Listen)); ok {
			s.promotePeerAddress(normalizedAddr)
			s.rememberPeerType(normalizedAddr, frame.NodeType)
			s.addPeerID(normalizedAddr, domain.PeerIdentity(frame.Address))
			s.addPeerVersion(normalizedAddr, frame.ClientVersion)
			s.addPeerBuild(normalizedAddr, frame.ClientBuild)
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

// addPeerFrame handles the local "add_peer" console command.
// The peer is prepended to the peer list so it becomes the first dial
// candidate on the next bootstrap tick.
func (s *Service) fetchDMHeadersFrame() protocol.Frame {
	s.cleanupExpiredMessages()

	s.mu.RLock()
	messages := append([]protocol.Envelope(nil), s.topics["dm"]...)
	s.mu.RUnlock()

	myAddr := s.identity.Address
	headers := make([]protocol.DMHeaderFrame, 0, len(messages))
	for _, msg := range messages {
		// Skip transit DMs — only include messages where this node is sender or recipient.
		if msg.Sender != myAddr && msg.Recipient != myAddr {
			continue
		}
		headers = append(headers, protocol.DMHeaderFrame{
			ID:        string(msg.ID),
			Sender:    msg.Sender,
			Recipient: msg.Recipient,
			CreatedAt: msg.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	return protocol.Frame{
		Type:      "dm_headers",
		DMHeaders: headers,
		Count:     len(headers),
	}
}

// isKnownNodeType returns true if the node type is one we recognize and can
// work with. Unknown types from future protocol versions are rejected so we
// don't add peers we cannot meaningfully interact with.
func isKnownNodeType(raw string) bool {
	_, ok := domain.ParseNodeType(raw)
	return ok
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

func (s *Service) emitLocalChange(event protocol.LocalChangeEvent) {
	s.mu.RLock()
	subs := make([]chan protocol.LocalChangeEvent, 0, len(s.events))
	for ch := range s.events {
		subs = append(subs, ch)
	}
	s.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
			log.Warn().Str("type", string(event.Type)).Str("message_id", event.MessageID).Msg("local change event dropped (channel full)")
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
		if errors.Is(err, errTrustConflict) {
			log.Warn().Str("address", address).Str("source", source).Msg("trust conflict")
		}
		return
	}

	s.addKnownIdentity(address)
	s.addKnownBoxKey(address, boxKey)
	s.addKnownPubKey(address, pubKey)
	if !existed {
		log.Info().Str("address", address).Str("source", source).Msg("trusted new contact")
	}
}

// isVerifiedSender checks whether the given sender address corresponds to
// a known, cryptographically authenticated identity. The sender is accepted
// when any of these conditions is true:
//
//  1. sender is this node's own identity (local authorship)
//  2. sender matches the relay peer's authenticated identity (direct authorship)
//  3. sender has a registered public key in s.pubKeys (previously authenticated
//     through identity exchange — hello/welcome, fetch_contacts, or trust store)
//
// This prevents arbitrary sender strings from entering the message store
// and poisoning s.known. For DM messages, storeIncomingMessage enforces
// VerifyEnvelope independently, so this gate targets non-DM topics only.
func (s *Service) isVerifiedSender(sender string, relayPeerIdentity domain.PeerIdentity) bool {
	if sender == s.identity.Address {
		return true
	}
	if relayPeerIdentity != "" && sender == string(relayPeerIdentity) {
		return true
	}
	s.mu.RLock()
	_, hasPubKey := s.pubKeys[sender]
	s.mu.RUnlock()
	return hasPubKey
}

// handleInboundPushMessage processes a push_message frame received on an
// authenticated inbound TCP connection. Two delivery paths converge here:
//
//  1. Backlog push — remote peer responds to subscribe_inbox with stored
//     messages for this node's identity.
//  2. Gossip push — remote peer forwards a message as part of epidemic
//     dissemination (sender ≠ relay peer, same as Bitcoin's tx relay).
//
// Sender spoofing protection:
//   - DM messages: VerifyEnvelope validates the cryptographic signature
//     against the sender's public key — spoofing is impossible without
//     the private key.
//   - Non-DM messages: the sender must be a verified identity — either
//     the relay peer itself, this node, or a peer whose public key was
//     previously exchanged through the identity protocol. Unverified
//     senders are rejected and the relay peer's ban score is incremented.
func (s *Service) handleInboundPushMessage(conn net.Conn, frame protocol.Frame) {
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

	peerAddr := s.inboundPeerAddress(conn)
	peerIdentity := s.inboundPeerIdentity(conn)

	// Non-DM sender verification: reject messages whose sender is not a
	// known identity. DM messages have their own cryptographic verification
	// in storeIncomingMessage (VerifyEnvelope), so this gate targets only
	// non-DM topics where no per-message signature exists.
	if msg.Topic != "dm" && !s.isVerifiedSender(msg.Sender, peerIdentity) {
		log.Warn().
			Str("node", s.identity.Address).
			Str("peer", string(peerAddr)).
			Str("relay_identity", string(peerIdentity)).
			Str("id", string(msg.ID)).
			Str("sender", msg.Sender).
			Str("topic", msg.Topic).
			Msg("push_message rejected: non-DM sender identity not verified")
		s.addBanScore(conn, banIncrementInvalidSig)
		return
	}

	stored, _, errCode := s.storeIncomingMessage(msg, true)
	if !stored && errCode == protocol.ErrCodeUnknownSenderKey {
		if peerAddr != "" {
			// Narrow sender-key recovery: only contact/key sync, no peer
			// exchange. See docs/peer-discovery-conditional-get-peers.ru.md
			// § Шаг 5 — частная ошибка (unknown sender key) не должна
			// превращаться в eager peer exchange.
			//
			// Use s.runCtx as the parent: this handler runs on the inbound
			// read loop started by handleConn, which is itself bounded by
			// the service lifecycle. Synthesising context.Background() here
			// would discard shutdown cancellation — forbidden by CLAUDE.md.
			refreshCtx, cancel := context.WithTimeout(s.runCtx, 1500*time.Millisecond)
			// Narrow recovery: always skip peer exchange. Logged so this path
			// is visible in observability alongside the other skip sites.
			// See docs/peer-discovery-conditional-get-peers.ru.md § Шаг 6.
			s.logPeerExchangeSkipped(peerExchangePathUnknownSenderRecovery, peerAddr, peerExchangeSkipByNarrowRecovery)
			s.syncPeer(refreshCtx, peerAddr, false)
			cancel()
			stored, _, _ = s.storeIncomingMessage(msg, true)
		}
	}
	if stored && msg.Topic == "dm" {
		// Prefer the outbound session for ack_delete (single write queue,
		// no interleaving risk). Fall back to the inbound conn when no
		// outbound session exists — this is the fix for the case where the
		// remote peer connected to us but we haven't dialed them back.
		if session := s.peerSession(peerAddr); session != nil && session.authOK {
			s.sendAckDeleteToPeer(peerAddr, "dm", msg.ID, "")
		} else {
			s.sendAckDeleteOnConn(conn, "dm", msg.ID, "")
		}
	} else if !stored {
		log.Warn().Str("node", s.identity.Address).Str("peer", string(peerAddr)).Str("relay_identity", string(peerIdentity)).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Str("err_code", errCode).Msg("push_message_store_failed")
	}
	log.Info().Str("node", s.identity.Address).Str("peer", string(peerAddr)).Str("relay_identity", string(peerIdentity)).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Str("topic", msg.Topic).Bool("stored", stored).Msg("received pushed message (inbound)")
}

// handleInboundPushDeliveryReceipt processes a push_delivery_receipt frame
// received on an inbound connection. This happens when the remote peer
// responds to our subscribe_inbox with delivery receipts destined for this
// node's identity.
//
// Identity binding: the receipt's Recipient (the DM sender who should receive
// the delivery confirmation) must match either our own identity or an identity
// we actively subscribe to via the inbound peer. Without this check an
// authenticated peer could push a receipt with arbitrary Sender/Recipient and
// corrupt the delivery state for a conversation it does not participate in.
func (s *Service) handleInboundPushDeliveryReceipt(conn net.Conn, frame protocol.Frame) {
	if frame.Receipt == nil {
		return
	}
	receipt, err := receiptFromReceiptFrame(*frame.Receipt)
	if err != nil {
		return
	}

	peerAddr := s.inboundPeerAddress(conn)

	// Identity gate: accept only receipts whose Recipient matches our own
	// identity or an identity with an active inbound subscriber (full-node
	// relay holding receipts for connected clients).
	if receipt.Recipient != s.identity.Address && !s.hasSubscriber(receipt.Recipient) {
		log.Warn().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("receipt_recipient", receipt.Recipient).
			Str("local_identity", s.identity.Address).
			Msg("push_delivery_receipt rejected: recipient does not match local identity or active subscriber")
		s.addBanScore(conn, banIncrementInvalidSig)
		return
	}

	s.storeDeliveryReceipt(receipt)
	if session := s.peerSession(peerAddr); session != nil && session.authOK {
		s.sendAckDeleteToPeer(peerAddr, "receipt", receipt.MessageID, receipt.Status)
	} else {
		s.sendAckDeleteOnConn(conn, "receipt", receipt.MessageID, receipt.Status)
	}
	log.Info().Str("peer", string(peerAddr)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received pushed delivery receipt (inbound)")
}

// handleInboundRelayDeliveryReceipt processes a relay_delivery_receipt frame
// received on an inbound TCP connection. This is the gossip receipt path:
// the remote peer forwards a receipt using flat Frame fields (ID, Address,
// Recipient, Status, DeliveredAt) rather than the nested ReceiptFrame used
// by push_delivery_receipt.
//
// This command is intentionally named differently from the local-only
// "send_delivery_receipt" to enforce command-isolation: local RPC commands
// must never be callable from the P2P wire (see docs/command-isolation.md).
//
// Identity binding: same gate as push_delivery_receipt — the receipt's
// Recipient must match this node's identity or an active subscriber.
// Without this, any authenticated peer could inject a forged receipt and
// advance another conversation's delivery state via storeDeliveryReceipt
// (which clears pending outbound state and updates MessageStore).
func (s *Service) handleInboundRelayDeliveryReceipt(conn net.Conn, frame protocol.Frame) {
	receipt, err := receiptFromFrame(frame)
	if err != nil {
		return
	}

	peerAddr := s.inboundPeerAddress(conn)

	if receipt.Recipient != s.identity.Address && !s.hasSubscriber(receipt.Recipient) {
		log.Warn().
			Str("peer", string(peerAddr)).
			Str("message_id", string(receipt.MessageID)).
			Str("receipt_recipient", receipt.Recipient).
			Str("local_identity", s.identity.Address).
			Msg("relay_delivery_receipt rejected: recipient does not match local identity or active subscriber")
		s.addBanScore(conn, banIncrementInvalidSig)
		return
	}

	s.storeDeliveryReceipt(receipt)
	log.Info().Str("peer", string(peerAddr)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received relay_delivery_receipt (inbound)")
}

// handleInboundPushNotice processes a push_notice frame received on an
// authenticated P2P connection (inbound TCP or outbound session). The remote
// peer gossips an encrypted notice; we store it locally and re-gossip to our
// own routing targets if new. Deduplication via notice ID prevents infinite
// loops.
func (s *Service) handleInboundPushNotice(frame protocol.Frame) {
	ttl := time.Duration(frame.TTLSeconds) * time.Second
	if ttl <= 0 || strings.TrimSpace(frame.Ciphertext) == "" {
		return
	}

	s.cleanupExpiredNotices()

	id := gazeta.ID(frame.Ciphertext)
	expiresAt := time.Now().UTC().Add(ttl)

	s.mu.Lock()
	if existing, ok := s.notices[id]; ok && existing.ExpiresAt.After(time.Now().UTC()) {
		s.mu.Unlock()
		return
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

// isFireAndForgetFrame returns true for frame types that should be written to
// the peer session without waiting for a response. These frames are delivered
// on a best-effort basis; the remote side may send an ack asynchronously, but
// the sender must not block the session waiting for it.
//
// push_message, push_notice, and relay_delivery_receipt are fire-and-forget
// because the gossip path writes them via enqueuePeerFrame → sendCh, and
// the remote dispatcher stores the payload without writing a response frame.
// Blocking the session on a reply that never comes would stall gossip delivery.
func isFireAndForgetFrame(frameType string) bool {
	switch frameType {
	case "announce_routes", "push_message", "push_notice", "relay_delivery_receipt":
		return true
	default:
		return isRelayFrame(frameType) || frameType == protocol.FileCommandFrameType
	}
}

// heartbeatInterval is the base interval between ping/pong heartbeats.
// Mirrors Bitcoin's 2-minute PING_INTERVAL to reduce idle chatter while
// keeping the connection provably alive.
const heartbeatInterval = 2 * time.Minute

// pongStallTimeout is the maximum time to wait for a pong reply before
// declaring the peer stalled and tearing down the session.
const pongStallTimeout = 45 * time.Second

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

// hasSubscriber returns true if at least one active subscriber exists for
// the given recipient identity. Used by push_delivery_receipt identity
// binding to allow a full-node relay to accept receipts for identities it
// serves (i.e., identities with active subscribe_inbox subscriptions).
func (s *Service) hasSubscriber(recipient string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs[recipient]) > 0
}

func (s *Service) writePushFrame(sub *subscriber, frame protocol.Frame) {
	defer crashlog.DeferRecover()

	log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", sub.conn.RemoteAddr().String()).
		Str("direction", "send").
		Str("command", frame.Type).
		Bool("accepted", true).
		Msg("protocol_trace")

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return
	}
	if s.enqueueFrame(sub.conn, []byte(line)) != enqueueSent {
		// Connection unregistered or send buffer full — remove stale subscriber.
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
					log.Debug().Str("node", s.identity.Address).Str("topic", topic).Str("id", string(message.ID)).Msg("cleanupExpiredMessages: removing expired")
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
