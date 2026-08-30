package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/transport"
)

// errNoLocalNode is the sentinel returned by file-transfer pass-throughs
// when DesktopClient was constructed without an embedded node.Service.
// Callers use errors.Is(err, errNoLocalNode) to branch on the
// standalone-RPC mode; transport mapping must not rely on the text.
var errNoLocalNode = errors.New("no local node (embedded mode required)")

// DesktopClient is the composition root for the desktop sub-services that
// grew out of the original god-type. It owns each sub-service, exposes
// narrow accessors for callers that need one directly (e.g. DMRouter grabs
// the DMCrypto surface, FileTransferBridge grabs the embedded node), and
// keeps thin delegator methods so existing call sites continue to compile.
//
// Responsibilities after decomposition:
//
//   - AppInfo    — immutable config + identity snapshot.
//   - LocalRPCClient — in-process RPC frame dispatch + remote TCP handshakes.
//   - ChatlogGateway — the SQLite-backed chatlog repository. The database
//     itself belongs to internal/core/storage and is opened and closed by
//     the process composition root (desktop.Run / sdk.New).
//   - MessageStoreAdapter — satisfies node.MessageStore for the embedded node.
//   - DMCrypto   — direct-message encryption, decryption, send, and
//     on-demand per-peer sync.
//   - NodeProber — ProbeNode + Fetch* + routing snapshot + contact delete.
//
// Methods on DesktopClient that survived decomposition are thin wrappers
// that forward to the corresponding sub-service. New code should prefer
// the sub-service accessors (DMCrypto(), NodeProber(), etc.) when a
// narrower dependency is acceptable.
type DesktopClient struct {
	// Legacy fields retained for test constructors that build DesktopClient
	// via struct literals (internal/core/service/*_test.go). Production
	// construction always goes through NewDesktopClient which also wires
	// the sub-services below. wireSubServices is idempotent and callable
	// from tests that need to re-wire after mutating chatLog directly.
	id        *identity.Identity
	appCfg    config.App
	nodeCfg   config.Node
	localNode *node.Service
	chatLog   *chatlog.Store

	// Sub-services — composition root.
	info    AppInfo
	rpc     *LocalRPCClient
	chatlog *ChatlogGateway
	store   *MessageStoreAdapter

	// reactionControl is the door a peer's reactions come in through, and the
	// pager the periodic re-offer reads. Built by RegisterConversationControl
	// because it needs the event bus, which the composition root makes beside
	// this client rather than before it.
	reactionControl *ReactionControlAdapter

	// wipeTombstones refuses a re-delivery of a message this node has
	// deleted. Owned here, not by the DMRouter, because it guards the
	// door into the chatlog and has to be loaded before the node starts;
	// see wireSubServices.
	wipeTombstones *wipeTombstoneSet

	// removals names the conversations being removed right now. Owned here
	// rather than by the router for the same reason the refusals are: the
	// node holds the store adapter from the moment it is registered, and a
	// gate wired later is open for exactly the window it exists to close.
	removals *removalGate

	// cancelConversationDeliveryFn is a test-only override for the node
	// round-trip: what a wipe does with the answer — which messages the
	// peer is asked about — cannot be exercised without controlling
	// which of them the node claims never went out.
	cancelConversationDeliveryFn func(context.Context, domain.PeerIdentity) (ConversationCancellation, error)
	// freezeConversationDeliveryFn / thawConversationDeliveryFn are the
	// same seam for the two halves that bracket it: a test has to be able
	// to say what the node knew when it stopped sending, and to observe
	// that an aborted wipe puts the deliveries back.
	freezeConversationDeliveryFn func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error)
	// freezeMessageDeliveryFn is the single-message half: the delete
	// classifies under it, so a test that wants to say "this one never
	// went out" has to say it here.
	freezeMessageDeliveryFn    func(context.Context, domain.MessageID) (bool, error)
	thawConversationDeliveryFn func(context.Context, domain.PeerIdentity, []domain.MessageID) error
	dm                         *DMCrypto
	prober                     *NodeProber
}

// Contact is the service-layer view of a trusted peer's cryptographic
// material. Produced by contactsFromFrame / DMCrypto.ensureRecipientContact
// and consumed by the decryption path.
type Contact struct {
	BoxKey       string
	PubKey       string
	BoxSignature string
	// LastOnlineAt is the node's own observation of this identity being up.
	// What chat history says lives only in memory (RouterPeerState) and is
	// recomputed at startup — one writer, one source of truth.
	LastOnlineAt domain.OptionalTime
}

// MessageRecord is the normalized form of a persisted envelope handed to
// the decryption path. It differs from DirectMessage because the body is
// still ciphertext at this stage.
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

// DeliveryReceipt is the service-layer representation of a delivery-state
// update persisted by the node and replayed through ProbeNode.
type DeliveryReceipt struct {
	MessageID   string
	Sender      domain.PeerIdentity
	Recipient   domain.PeerIdentity
	Status      string
	DeliveredAt time.Time
}

// PeerHealth carries the per-connection diagnostic snapshot returned by
// fetch_peer_health and surfaced in the P2P tab.
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
	LastConnectedAt     domain.OptionalTime
	LastDisconnectedAt  domain.OptionalTime
	LastPingAt          domain.OptionalTime
	LastPongAt          domain.OptionalTime
	LastUsefulSendAt    domain.OptionalTime
	LastUsefulReceiveAt domain.OptionalTime
	ConsecutiveFailures int
	LastError           string
	Score               int
	BannedUntil         domain.OptionalTime
	BytesSent           int64
	BytesReceived       int64
	TotalTraffic        int64
	SlotState           string // CM slot lifecycle: queued, dialing, active, reconnecting, retry_wait
	SlotRetryCount      int
	SlotGeneration      uint64
	SlotConnectedAddr   string // actual TCP address for the active connection

	// Machine-readable disconnect diagnostics (version upgrade detection §5.1).
	LastErrorCode               string
	LastDisconnectCode          string
	IncompatibleVersionAttempts int
	LastIncompatibleVersionAt   domain.OptionalTime
	ObservedPeerVersion         int
	ObservedPeerMinimumVersion  int
	VersionLockoutActive        bool
}

// CaptureSession is the UI-visible record of a single traffic-capture
// session keyed by the ConnID of the recorded connection. It lives in its
// own map on NodeStatus so capture bookkeeping is independent of
// PeerHealth row pruning:
//
//   - Capture-start does not need to invent a placeholder PeerHealth row
//     before the first health delta arrives; the recording indicator reads
//     from CaptureSessions, which is keyed by ConnID alone.
//
//   - Capture-stop does not need to decide whether to remove a row or
//     strip fields — it just updates the CaptureSession entry. PeerHealth
//     rows are owned exclusively by network-layer evidence.
//
// Stopped sessions linger for NodeStatusMonitor.captureRetention so the
// UI can surface terminal diagnostics (Error, DroppedEvents) after the
// writer goes away. Active=false combined with a Valid() StoppedAt
// distinguishes a terminal session from a still-running one.
type CaptureSession struct {
	ConnID        domain.ConnID
	Address       domain.PeerAddress
	PeerID        domain.PeerIdentity
	Direction     domain.PeerDirection
	FilePath      string
	StartedAt     domain.OptionalTime
	Scope         domain.CaptureScope
	Format        domain.CaptureFormat
	Active        bool                // true while the session is recording
	StoppedAt     domain.OptionalTime // Valid() once the session has stopped (drives TTL cleanup)
	Error         string              // terminal error reason (empty on clean stop)
	DroppedEvents int64               // drop counter accumulated during the session
}

// DirectMessage is the decrypted DM surfaced in the chat UI. It combines
// the persisted ciphertext (now plaintext Body), the receipt lifecycle,
// and any embedded command (file transfer).
type DirectMessage struct {
	ID        string
	Sender    domain.PeerIdentity
	Recipient domain.PeerIdentity
	Body      string
	ReplyTo   domain.MessageID
	// RetryOf links a §4.10 recovery re-send to the original message it
	// replaces; empty for ordinary messages.
	RetryOf       domain.MessageID
	Command       domain.DMCommand // e.g. DMCommandFileAnnounce for file transfers; empty for regular DMs
	CommandData   string           // JSON-encoded payload (e.g. FileAnnouncePayload); empty for regular DMs
	Timestamp     time.Time
	ReceiptStatus string
	DeliveredAt   domain.OptionalTime
	// Seq is where this message landed in the local arrival order (the
	// chatlog row's sequence). Timestamp cannot serve that purpose: it is the
	// SENDER's clock, and two peers do not share one. Zero means the store
	// could not be asked or does not hold the row — the value is spent only
	// on ordering, so an unknown one degrades to "cannot be ordered" rather
	// than to "first".
	Seq int64
}

// DMHeader is the minimal DM metadata used for sidebar population without
// decryption. It is returned by fetch_dm_headers to let the UI show
// conversations promptly while the bodies decrypt lazily.
type DMHeader struct {
	ID        string
	Sender    domain.PeerIdentity
	Recipient domain.PeerIdentity
	Timestamp time.Time
}

// ConversationPreview carries the last-message summary for the chat list.
type ConversationPreview struct {
	PeerAddress domain.PeerIdentity
	Sender      domain.PeerIdentity
	Body        string
	Timestamp   time.Time
	UnreadCount int // number of incoming messages with delivery_status != 'seen'
	// Seq orders one preview against another: it is the arrival sequence of
	// the message this preview describes. The sidebar is fed from two roads —
	// a read of the store and the live event stream — that can answer in
	// either order, and Timestamp cannot separate them because it belongs to
	// the sender's clock. Zero means unknown (see DirectMessage.Seq); two
	// previews of which either is unknown cannot be ordered, and the later
	// writer wins as it did before this field existed.
	Seq int64
}

// PendingMessage is the service-layer view of a pending (not yet
// acknowledged) outbound message entry.
type PendingMessage struct {
	ID            string
	Recipient     string
	Status        string
	QueuedAt      domain.OptionalTime
	LastAttemptAt domain.OptionalTime
	Retries       int
	Error         string
}

// NodeStatus is the composite snapshot ProbeNode returns every poll
// interval. Consumers treat the struct as immutable between polls.
type NodeStatus struct {
	Address         string
	Connected       bool
	Welcome         string
	NodeID          string
	NodeType        string
	ListenerEnabled bool
	ListenerAddress string
	ClientVersion   string
	// ProtocolVersion mirrors welcome.Version — the wire protocol version
	// the local node emits in hello/welcome (see config.ProtocolVersion).
	// Surfaced here so the desktop console info tab can render it without
	// reaching into the core config package directly.
	ProtocolVersion  int
	Services         []string
	Capabilities     []string
	KnownIDs         []string
	Contacts         map[string]Contact
	Peers            []string
	PeerHealth       []PeerHealth
	CaptureSessions  map[domain.ConnID]CaptureSession // active + recently-stopped capture sessions keyed by ConnID
	AggregateStatus  *AggregateStatus                 // node-computed aggregate network health; nil when node does not support the command yet
	ResourceUsage    *ResourceUsage                   // node process memory + uptime; nil until first sample / when node does not support the command
	ReachableIDs     map[domain.PeerIdentity]bool     // identity reachable via routing table (at least one live route exists)
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

// AggregateStatus holds the node-computed aggregate network health
// snapshot. Desktop consumes this value directly instead of recomputing it
// from per-peer states, keeping the node layer as the single source of
// truth.
type AggregateStatus struct {
	Status          string
	UsablePeers     int
	ConnectedPeers  int
	TotalPeers      int
	PendingMessages int

	// Version policy snapshot — node-computed update signal.
	UpdateAvailable              bool
	UpdateReason                 string
	IncompatibleVersionReporters int
	MaxObservedPeerBuild         int
	MaxObservedPeerVersion       int
}

// ResourceUsage is the service-layer view of the node process memory
// footprint, cgroup memory, connection count, and uptime, parsed from a
// fetch_resource_usage reply — the full field set the wire frame and
// public RPC carry. Both machine-readable numbers and the node's
// human-formatted strings are kept so the desktop renders without
// re-deriving units. Flows through NodeStatus like every other
// probe/monitor field; sampled by NodeStatusMonitor's resource ticker
// (see RunResourceSampler) so the stop-the-world runtime.MemStats read
// happens at a controlled cadence off the UI render path.
type ResourceUsage struct {
	MemSysBytes       uint64
	MemSysHuman       string
	MemHeapAllocBytes uint64
	MemHeapAllocHuman string

	HeapInuseBytes    uint64
	HeapInuseHuman    string
	HeapIdleBytes     uint64
	HeapIdleHuman     string
	HeapReleasedBytes uint64
	HeapReleasedHuman string
	GCSysBytes        uint64
	GCSysHuman        string

	CgroupMemLimitBytes uint64
	CgroupMemLimitHuman string
	CgroupMemUsageBytes uint64
	CgroupMemUsageHuman string

	ConnectionCount int

	UptimeSeconds int64
	UptimeHuman   string

	// SampledAt is the RFC3339Nano UTC instant the node took the sample,
	// carried verbatim from the wire frame (empty when absent).
	SampledAt string
}

// NewDesktopClient wires the composition root: builds every sub-service on
// top of the shared state database and registers the MessageStoreAdapter with
// the embedded node so the node delegates message persistence to the desktop
// layer instead of managing its own chatlog.
//
// database is the already opened, already migrated state database. It may be
// nil only in tests that run the client without persistence; production
// callers open it first so a storage failure aborts startup instead of
// surfacing later as silently dropped messages.
func NewDesktopClient(appCfg config.App, nodeCfg config.Node, id *identity.Identity, localNode *node.Service, database *storage.Database) *DesktopClient {
	var store *chatlog.Store
	if database != nil {
		store = chatlog.NewStore(database.Executor(), domain.PeerIdentityFromWire(id.Address))
	}
	c := &DesktopClient{
		id:        id,
		appCfg:    appCfg,
		nodeCfg:   nodeCfg,
		localNode: localNode,
		chatLog:   store,
	}
	c.wireSubServices()
	if localNode != nil {
		localNode.RegisterMessageStore(c.store)
		// Durable arm of the sender-side delivery retry: reseed the
		// scheduler from chatlog rows still in "sent" so delayed delivery
		// survives a restart of this node.
		localNode.RegisterDeliveryOutbox(c.store)
	}
	return c
}

// wireSubServices constructs the composition-root sub-services from the
// base fields (id, appCfg, nodeCfg, localNode, chatLog). Callers must set
// those fields first — typically only NewDesktopClient invokes this.
// Tests that hand-build a DesktopClient via struct literal must call
// wireSubServices explicitly before exercising method surfaces.
//
// Idempotent: running wireSubServices again rebuilds all sub-services from
// the current base-field snapshot. Test hooks that mutate chatLog should
// call setChatLogForTest, which updates the gateway in place.
func (c *DesktopClient) wireSubServices() {
	c.info = NewAppInfo(c.appCfg, c.nodeCfg, c.id)
	c.rpc = NewLocalRPCClient(c.info, c.localNode)
	c.chatlog = NewChatlogGateway(c.chatLog, c.info.Address())
	// The refusal set is built and LOADED here, before the node is
	// started and before anything can hand this process a message. It
	// belongs to the client rather than to the DMRouter because it
	// guards the chatlog, and the router is constructed later — a set
	// wired after the node is already accepting connections would be
	// empty for exactly the window in which a replay of a message
	// deleted before the restart arrives.
	c.wipeTombstones = newWipeTombstoneSet(func() deleteTaskList {
		if c.chatlog == nil {
			return nil
		}
		store := c.chatlog.Store()
		if store == nil {
			return nil
		}
		return store
	})
	c.wipeTombstones.Hydrate(context.Background(), time.Now().UTC())
	// A deletion spans two stores that no transaction covers together; this
	// finishes the ones whose file half never landed. See
	// attachment_reconcile.go.
	c.reconcileOrphanAttachments(context.Background())
	c.removals = newRemovalGate()
	c.store = NewMessageStoreAdapter(c.chatlog, c.id, c.wipeTombstones, c.removals)
	c.dm = NewDMCrypto(c.rpc, c.chatlog, c.id)
	c.prober = NewNodeProber(c.rpc, c.dm, c.info)
}

// setChatLogForTest replaces the owned chatlog.Store pointer and keeps the
// gateway in sync so sub-services observe the new store. Test-only —
// production code must not mutate the chatlog after construction.
func (c *DesktopClient) setChatLogForTest(store *chatlog.Store) {
	c.chatLog = store
	if c.chatlog != nil {
		c.chatlog.setStoreForTest(store)
	}
}

// BackfillEstablished seeds the monotonic established facts from chat history
// that predates the peer_established table. Idempotent; the composition root
// calls it once, after the state database is open and before the node starts
// classifying peers.
func (c *DesktopClient) BackfillEstablished(ctx context.Context, now time.Time) error {
	return c.chatlog.BackfillEstablished(ctx, now)
}

// RegisterConversationControl opens the door for reactions a peer states over
// the datagram plane.
//
// Separate from NewDesktopClient because it needs the event bus, which the
// composition root builds alongside the client rather than before it. It must
// run before the node starts: the node accepts a dm_control frame the moment it
// is running, and until this is registered it can only refuse them.
func (c *DesktopClient) RegisterConversationControl(events *ebus.Bus) {
	if c == nil || c.localNode == nil {
		return
	}
	c.reactionControl = NewReactionControlAdapter(
		c.chatlog, c.wipeTombstones, c.removals, events, nil)
	c.localNode.RegisterConversationControlStore(c.reactionControl)
	// The other half of the same story: a message landing releases the
	// reactions that were waiting for it, and the UI has to be told.
	c.store.attachEventBus(events)
}

// SendReactionFacts hands this user's own reaction decisions to the node, which
// batches them and puts them on the wire a second or so later.
//
// It returns once they are queued, not once they are sent: the outcome of the
// send is not something this layer can act on. A fact is idempotent and ordered
// by its author's clock, so a lost frame is not a delivery to retry but a
// divergence for reconciliation to find.
func (c *DesktopClient) SendReactionFacts(peer domain.PeerIdentity, facts []domain.ReactionFact) error {
	if c == nil || c.localNode == nil {
		return nil
	}
	return c.localNode.QueueReactionFacts(peer, facts)
}

// HoldReactionSends stops this node's queued reactions for a peer from going out
// until the returned release runs, and waits for the frames already being
// sealed.
//
// Called around the deletion of a SINGLE message. The reaction queue names
// reactions, not messages, and its frames are built from what the record said a
// moment earlier — so without this bracket a frame built just before the delete
// still goes out, telling the peer about a reaction on a message this node has
// erased. What is paused is not lost: it is offered again on the next pass,
// built from the record as it stands after the delete.
func (c *DesktopClient) HoldReactionSends(peer domain.PeerIdentity) func() {
	if c == nil || c.localNode == nil {
		return func() {}
	}
	return c.localNode.HoldReactionSends(peer)
}

// ForgetContactState drops everything about one CONTACT that lives outside the
// database, and ForgetConversationState drops everything about one of their
// THREADS. The difference is what is being removed, and it decides one thing:
// whether what this node believes about that peer's build goes too.
//
// The shared inventory — read it as one when something new is added, because
// the database is erased by the deletion paths themselves and anything a
// conversation leaves in memory has to be listed here or it outlives what it
// belongs to:
//
//   - the node's send queue for that peer, and the batch it may have out of the
//     queue being sent right now;
//   - the re-offer cursor and its backoff for that conversation, which nothing
//     else prunes: the database stops returning the conversation, but the entry
//     would sit in memory until the process ended;
//   - CONTACT REMOVAL ONLY: what this node believes about that peer's build —
//     whether it can receive reactions at all. That belief is about the peer,
//     not the thread, so a wipe keeps it: clearing it would make the next
//     reaction to a contact the user still has look delivered until the refusal
//     is learned again.
//
// Call either UNDER the removal gate, from inside the section that erases the
// database. Outside it, a re-offer that has already read a page can queue that
// page after this has run, and the queue is rebuilt from rows that are gone.
func (c *DesktopClient) ForgetContactState(peer domain.PeerIdentity) {
	c.forgetReactionState(peer, true)
}

// ForgetConversationState is the wipe half of ForgetContactState: the queue and
// the cursor go, the beliefs stay.
func (c *DesktopClient) ForgetConversationState(peer domain.PeerIdentity) {
	c.forgetReactionState(peer, false)
}

func (c *DesktopClient) forgetReactionState(peer domain.PeerIdentity, contactGone bool) {
	if c == nil {
		return
	}
	if c.reactionControl != nil {
		c.reactionControl.ForgetConversation(domain.ReactionScopeForPeer(peer))
	}
	if c.localNode == nil {
		return
	}
	if contactGone {
		c.localNode.ForgetPeerReactions(peer)
		return
	}
	c.localNode.DropQueuedReactions(peer)
}

// ReactionsUnsupportedBy reports whether this peer runs a build that cannot
// receive reactions at all — as opposed to being merely offline. Telling those
// two apart is what the datagram transport buys, and the UI has to say so.
func (c *DesktopClient) ReactionsUnsupportedBy(peer domain.PeerIdentity) bool {
	if c == nil || c.localNode == nil {
		return false
	}
	return c.localNode.ReactionsUnsupportedBy(peer)
}

// ---------------------------------------------------------------------------
// Sub-service accessors — new code should prefer these over the broad
// DesktopClient surface so the narrower dependency shows up in types.
// ---------------------------------------------------------------------------

// AppInfo returns the immutable app-config snapshot.
func (c *DesktopClient) AppInfo() AppInfo { return c.info }

// RPC returns the LocalRPCClient for callers that need to dispatch frames
// directly (file transfer bridge, diagnostics).
func (c *DesktopClient) RPC() *LocalRPCClient { return c.rpc }

// ChatlogGateway returns the persistent-history gateway for callers that
// read the chatlog directly (RPC CommandTable).
func (c *DesktopClient) ChatlogGateway() *ChatlogGateway { return c.chatlog }

// DMCrypto returns the direct-message encryption surface.
func (c *DesktopClient) DMCrypto() *DMCrypto { return c.dm }

// NodeProber returns the node-status prober.
func (c *DesktopClient) NodeProber() *NodeProber { return c.prober }

// ---------------------------------------------------------------------------
// node.MessageStore implementation — delegates to MessageStoreAdapter so
// the node→desktop persistence contract is served through its dedicated
// type even if callers still hold a *DesktopClient.
// ---------------------------------------------------------------------------

// StoreMessage forwards to MessageStoreAdapter. See MessageStoreAdapter
// for the classification contract.
func (c *DesktopClient) StoreMessage(envelope protocol.Envelope, isOutgoing bool) node.StoreResult {
	return c.store.StoreMessage(envelope, isOutgoing)
}

// UpdateDeliveryStatus forwards to MessageStoreAdapter. See
// MessageStoreAdapter for the sender/recipient disambiguation.
func (c *DesktopClient) UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool {
	return c.store.UpdateDeliveryStatus(receipt)
}

// ---------------------------------------------------------------------------
// AppInfo pass-throughs.
// ---------------------------------------------------------------------------

// NetworkName returns the configured network name.
func (c *DesktopClient) NetworkName() string { return c.info.NetworkName() }

// ProfileName returns the active configuration profile name.
func (c *DesktopClient) ProfileName() string { return c.info.ProfileName() }

// AppName returns the human-readable application name.
func (c *DesktopClient) AppName() string { return c.info.AppName() }

// Language returns the configured UI language tag.
func (c *DesktopClient) Language() string { return c.info.Language() }

// Version returns the application version string.
func (c *DesktopClient) Version() string { return c.info.Version() }

// ListenAddress returns the configured local listener address.
func (c *DesktopClient) ListenAddress() string { return c.info.ListenAddress() }

// Address returns the local node identity.
func (c *DesktopClient) Address() domain.PeerIdentity { return c.info.Address() }

// TransmitDir returns the absolute path of the transmit directory.
func (c *DesktopClient) TransmitDir() string { return c.info.TransmitDir() }

// BootstrapPeers returns the transport-layer bootstrap peer list.
func (c *DesktopClient) BootstrapPeers() []transport.Peer { return c.info.BootstrapPeers() }

// DesktopVersion returns the desktop application version. Implements
// rpc.DiagnosticProvider so the RPC diagnostics endpoint does not need to
// know about the underlying sub-services.
func (c *DesktopClient) DesktopVersion() string { return c.info.DesktopVersion() }

// ---------------------------------------------------------------------------
// ChatlogGateway pass-throughs.
// ---------------------------------------------------------------------------

// DeletePeerHistory removes all chat messages for identity.
func (c *DesktopClient) DeletePeerHistory(ctx context.Context, identity domain.PeerIdentity) (int64, error) {
	return c.chatlog.DeletePeerHistory(ctx, identity)
}

// FetchChatlog reads the chat entries for a peer and returns a formatted
// JSON payload suitable for console / RPC consumption.
func (c *DesktopClient) FetchChatlog(ctx context.Context, topic, peerAddress string) (string, error) {
	return c.chatlog.FetchChatlog(ctx, topic, peerAddress)
}

// FetchChatlogPreviews reads the last entry per peer and returns a
// formatted JSON payload with preview-sized fields.
func (c *DesktopClient) FetchChatlogPreviews(ctx context.Context) (string, error) {
	return c.chatlog.FetchChatlogPreviews(ctx)
}

// FetchConversations lists all conversations with their message counts.
func (c *DesktopClient) FetchConversations(ctx context.Context) (string, error) {
	return c.chatlog.FetchConversations(ctx)
}

// HasEntryInConversation reports whether a message with the given ID
// exists in the conversation with peerAddress.
func (c *DesktopClient) HasEntryInConversation(ctx context.Context, peerAddress, messageID string) bool {
	return c.chatlog.HasEntryInConversation(ctx, peerAddress, messageID)
}

// LookupEntryInConversation is HasEntryInConversation for callers that must
// tell absence from a failed lookup.
func (c *DesktopClient) LookupEntryInConversation(ctx context.Context, peerAddress, messageID string) (bool, error) {
	return c.chatlog.LookupEntryInConversation(ctx, peerAddress, messageID)
}

// ---------------------------------------------------------------------------
// NodeProber pass-throughs.
// ---------------------------------------------------------------------------

// DeleteContact removes a trusted contact from the node's trust store.
func (c *DesktopClient) DeleteContact(identity domain.PeerIdentity) error {
	return c.prober.DeleteContact(identity)
}

// SubscribeLocalChanges subscribes to local-change events from the embedded
// node. Returns a receive-only channel and a cancel func.
func (c *DesktopClient) SubscribeLocalChanges() (<-chan protocol.LocalChangeEvent, func()) {
	return c.prober.SubscribeLocalChanges()
}

// ProbeNode performs a full node-status handshake and returns a populated
// NodeStatus snapshot.
func (c *DesktopClient) ProbeNode(ctx context.Context) NodeStatus {
	return c.prober.ProbeNode(ctx)
}

// BuildContactLink renders this node's own corsa: contact link — the
// offline key-handover channel of docs/protocol/identity-lookup.md §4.8.
func (c *DesktopClient) BuildContactLink() (string, error) {
	return contactlink.Build(c.id, domain.NetworkID(c.NetworkName()))
}

// ImportContactLink verifies a pasted corsa: link and imports the contact:
// verify-then-import — the fingerprint and box binding are checked by the
// parser, the node-side import re-verifies and pins the contact in the
// trust store. Works fully offline.
func (c *DesktopClient) ImportContactLink(ctx context.Context, raw string) (domain.PeerIdentity, error) {
	contact, err := contactlink.Parse(raw, domain.NetworkID(c.NetworkName()))
	if err != nil {
		return domain.PeerIdentity{}, err
	}
	reply, err := c.rpc.LocalRequestFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: contact.Address.String(),
			PubKey:  string(contact.PubKey),
			BoxKey:  string(contact.BoxKey),
			BoxSig:  string(contact.BoxSig),
		}},
	})
	if err != nil {
		return domain.PeerIdentity{}, err
	}
	if reply.Type != "contacts_imported" || reply.Count == 0 {
		return domain.PeerIdentity{}, fmt.Errorf("contact link import refused: %s", reply.Type)
	}
	// A manual import is a §4.10 qualifying event for the established fact.
	if c.chatLog != nil {
		if err := c.chatLog.MarkEstablished(ctx, contact.Address.String(), chatlog.EstablishedReasonManual, time.Now().UTC()); err != nil {
			return contact.Address, nil // the import itself succeeded; the mark is best-effort
		}
	}
	return contact.Address, nil
}

// BuildReachableIDs returns identities that have at least one live route
// in the routing table.
func (c *DesktopClient) BuildReachableIDs() map[domain.PeerIdentity]bool {
	return c.prober.BuildReachableIDs()
}

// FetchResourceUsage queries the node for its current process memory +
// uptime — the lightweight single-fetch the NodeStatusMonitor resource
// ticker uses to keep status.ResourceUsage fresh between full probes.
// Thin delegator to NodeProber.FetchResourceUsage, matching the other
// read-side helpers (FetchContacts, FetchKnownIDs, …).
func (c *DesktopClient) FetchResourceUsage(ctx context.Context) *ResourceUsage {
	return c.prober.FetchResourceUsage(ctx)
}

// FetchContacts queries the node for the current trusted contacts map.
func (c *DesktopClient) FetchContacts(ctx context.Context) (map[string]Contact, error) {
	return c.prober.FetchContacts(ctx)
}

// FetchKnownIDs queries the node for the current identity list.
func (c *DesktopClient) FetchKnownIDs(ctx context.Context) ([]string, error) {
	return c.prober.FetchKnownIDs(ctx)
}

// FetchPeerHealth queries the node for the current peer health snapshot.
func (c *DesktopClient) FetchPeerHealth(ctx context.Context) ([]PeerHealth, error) {
	return c.prober.FetchPeerHealth(ctx)
}

// FetchMessageIDs returns the stored message IDs for topic.
func (c *DesktopClient) FetchMessageIDs(ctx context.Context, topic string) ([]string, error) {
	return c.prober.FetchMessageIDs(ctx, topic)
}

// FetchMessage returns a single persisted message by (topic, id).
func (c *DesktopClient) FetchMessage(ctx context.Context, topic, messageID string) (MessageRecord, error) {
	return c.prober.FetchMessage(ctx, topic, messageID)
}

// ---------------------------------------------------------------------------
// DMCrypto pass-throughs.
// ---------------------------------------------------------------------------

// SendDirectMessage encrypts and submits a DM.
func (c *DesktopClient) SendDirectMessage(ctx context.Context, to domain.PeerIdentity, msg domain.OutgoingDM) (*DirectMessage, error) {
	return c.dm.SendDirectMessage(ctx, to, msg)
}

// CancelMessageDelivery asks the node to stop delivering a DM we sent
// earlier, so the local row can be removed without the message reaching
// the recipient later.
func (c *DesktopClient) CancelMessageDelivery(ctx context.Context, to domain.PeerIdentity, messageID domain.MessageID) (DeliveryCancellation, error) {
	return c.dm.CancelMessageDelivery(ctx, to, messageID)
}

// CancelConversationDelivery asks the node to stop delivering everything
// we still owe the peer, so a wiped thread cannot be handed over later.
func (c *DesktopClient) CancelConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) (ConversationCancellation, error) {
	if c.cancelConversationDeliveryFn != nil {
		return c.cancelConversationDeliveryFn(ctx, peer)
	}
	return c.dm.CancelConversationDelivery(ctx, peer, scope)
}

// FreezeMessageDelivery stops the node from sending one message so a
// delete can classify from its row.
func (c *DesktopClient) FreezeMessageDelivery(ctx context.Context, messageID domain.MessageID) (bool, error) {
	if c.freezeMessageDeliveryFn != nil {
		return c.freezeMessageDeliveryFn(ctx, messageID)
	}
	return c.dm.FreezeMessageDelivery(ctx, messageID)
}

// FreezeConversationDelivery stops the node from sending anything we still
// owe the peer, without withdrawing it, so a wipe can classify against a
// state that cannot move under it.
func (c *DesktopClient) FreezeConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) (ConversationFreeze, error) {
	if c.freezeConversationDeliveryFn != nil {
		return c.freezeConversationDeliveryFn(ctx, peer, scope)
	}
	return c.dm.FreezeConversationDelivery(ctx, peer, scope)
}

// ThawConversationDelivery ends a freeze whose wipe did not commit.
func (c *DesktopClient) ThawConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) error {
	if c.thawConversationDeliveryFn != nil {
		return c.thawConversationDeliveryFn(ctx, peer, scope)
	}
	return c.dm.ThawConversationDelivery(ctx, peer, scope)
}

// DecryptIncomingMessage decrypts a local-change event into a DirectMessage.
func (c *DesktopClient) DecryptIncomingMessage(ctx context.Context, event protocol.LocalChangeEvent) *DirectMessage {
	return c.dm.DecryptIncomingMessage(ctx, event)
}

// SendControlMessage submits a control DM (message_delete,
// message_delete_ack, ...) on the dedicated control wire path. Unlike
// SendDirectMessage, the message is not persisted and does not surface
// in the chat thread on either side. See docs/dm-commands.md.
func (c *DesktopClient) SendControlMessage(ctx context.Context, to domain.PeerIdentity, cmd domain.DMCommand, payload string) (domain.MessageID, error) {
	return c.dm.SendControlMessage(ctx, to, cmd, payload)
}

// DecryptIncomingControlMessage decrypts a LocalChangeNewControlMessage
// event into a DMCommand and its JSON payload. ok=false signals an
// envelope that failed verification or carries a non-control inner
// command.
func (c *DesktopClient) DecryptIncomingControlMessage(event protocol.LocalChangeEvent) (domain.DMCommand, string, domain.PeerIdentity, bool) {
	return c.dm.DecryptIncomingControlMessage(event)
}

// SyncDirectMessagesFromPeers pulls DM IDs from remote peers and imports
// any that the local store does not have yet.
func (c *DesktopClient) SyncDirectMessagesFromPeers(ctx context.Context, peerAddresses []string, counterparty string) (int, error) {
	return c.dm.SyncDirectMessagesFromPeers(ctx, peerAddresses, counterparty)
}

// FetchConversation loads the full chat history for a single peer.
func (c *DesktopClient) FetchConversation(ctx context.Context, peerAddress domain.PeerIdentity) ([]DirectMessage, error) {
	return c.dm.FetchConversation(ctx, peerAddress)
}

// FetchConversationPreviews loads the last message for each DM thread.
func (c *DesktopClient) FetchConversationPreviews(ctx context.Context) ([]ConversationPreview, error) {
	return c.dm.FetchConversationPreviews(ctx)
}

// FetchSinglePreview loads and decrypts the last message for a single peer.
func (c *DesktopClient) FetchSinglePreview(ctx context.Context, peerAddress domain.PeerIdentity) (*ConversationPreview, error) {
	return c.dm.FetchSinglePreview(ctx, peerAddress)
}

// MarkConversationSeen fires delivery-seen receipts for each unseen
// message sent by counterparty.
func (c *DesktopClient) MarkConversationSeen(ctx context.Context, counterparty domain.PeerIdentity, messages []DirectMessage) error {
	return c.dm.MarkConversationSeen(ctx, counterparty, messages)
}

// ---------------------------------------------------------------------------
// File-transfer pass-throughs — these still delegate straight to localNode
// because the logic lives on node.Service. A future FileTransferCoordinator
// can absorb them without touching callers.
// ---------------------------------------------------------------------------

// StoreFileForTransmit copies the source file into the transmit directory.
func (c *DesktopClient) StoreFileForTransmit(sourcePath string) (string, error) {
	if c.localNode == nil {
		return "", errNoLocalNode
	}
	return c.localNode.StoreFileForTransmit(sourcePath)
}

// TransmitFileSize returns the byte size of the stored transmit blob.
func (c *DesktopClient) TransmitFileSize(fileHash string) (uint64, error) {
	if c.localNode == nil {
		return 0, errNoLocalNode
	}
	return c.localNode.TransmitFileSize(fileHash)
}

// RemoveUnreferencedTransmitFile deletes the transmit blob for the given
// hash if no active sender mapping or pending reservation protects it.
func (c *DesktopClient) RemoveUnreferencedTransmitFile(fileHash string) {
	if c.localNode == nil {
		return
	}
	c.localNode.RemoveUnreferencedTransmitFile(fileHash)
}

// PrepareFileAnnounce atomically validates transmit file availability and
// reserves a sender quota slot.
func (c *DesktopClient) PrepareFileAnnounce(fileHash, fileName, contentType string, fileSize uint64) (*filetransfer.SenderAnnounceToken, error) {
	if c.localNode == nil {
		return nil, errNoLocalNode
	}
	return c.localNode.PrepareFileAnnounce(fileHash, fileName, contentType, fileSize)
}

// RegisterIncomingFileTransfer registers a receiver-side file mapping.
func (c *DesktopClient) RegisterIncomingFileTransfer(fileID domain.FileID, fileHash, fileName, contentType string, fileSize uint64, sender domain.PeerIdentity) error {
	if c.localNode == nil {
		return errNoLocalNode
	}
	return c.localNode.RegisterIncomingFileTransfer(fileID, fileHash, fileName, contentType, fileSize, sender)
}

// CancelFileDownload aborts an active download and resets the mapping.
func (c *DesktopClient) CancelFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return errNoLocalNode
	}
	return c.localNode.CancelFileDownload(fileID)
}

// StartFileDownload begins downloading a previously registered incoming file.
func (c *DesktopClient) StartFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return errNoLocalNode
	}
	return c.localNode.StartFileDownload(fileID)
}

// RestartFileDownload resets a failed download back to available state.
func (c *DesktopClient) RestartFileDownload(fileID domain.FileID) error {
	if c.localNode == nil {
		return errNoLocalNode
	}
	return c.localNode.RestartFileDownload(fileID)
}

// FileTransferProgress returns the transfer progress for a given file.
func (c *DesktopClient) FileTransferProgress(fileID domain.FileID, isSender bool) (bytesTransferred, totalSize uint64, state string, found bool) {
	if c.localNode == nil {
		return 0, 0, "", false
	}
	return c.localNode.FileTransferProgress(fileID, isSender)
}

// AllFileTransfers returns every sender/receiver mapping (active and
// terminal) as typed snapshots, used by the desktop UI's file tab to
// render history alongside in-flight transfers. Returns an empty
// non-nil slice in standalone-RPC mode (no embedded node) so the UI
// doesn't have to special-case nil.
func (c *DesktopClient) AllFileTransfers() []filetransfer.TransferSnapshot {
	if c.localNode == nil {
		return []filetransfer.TransferSnapshot{}
	}
	return c.localNode.AllFileTransfersSnapshot()
}

// FileTransferFilePath returns the on-disk path for a transferred file.
func (c *DesktopClient) FileTransferFilePath(fileID domain.FileID, isSender bool) string {
	if c.localNode == nil {
		return ""
	}
	return c.localNode.FileTransferFilePath(fileID, isSender)
}

// DeleteLocalFileCopy erases this node's copy of a transferred file,
// keeping the mapping and the message. Returns an error when the
// file-transfer subsystem is not available (standalone-RPC desktop), which
// is the same answer as "nothing was deleted".
func (c *DesktopClient) DeleteLocalFileCopy(fileID domain.FileID) error {
	if c.localNode == nil {
		return errNoLocalNode
	}
	return c.localNode.DeleteLocalFileCopy(fileID)
}

// CleanupPeerTransfers removes all file transfer mappings and files
// associated with the given peer identity.
func (c *DesktopClient) CleanupPeerTransfers(peer domain.PeerIdentity) {
	if c.localNode == nil {
		return
	}
	c.localNode.CleanupPeerTransfers(peer)
}

// CleanupTransferByMessageID releases all file-transfer state attached
// to a single DM (sender/receiver mappings, transmit-blob ref,
// partial/completed downloaded files). Called from the DM-router
// delete hook (FileTransferBridge.OnMessageDeleted). Idempotent — a
// message ID with no associated file-transfer state is a silent no-op.
func (c *DesktopClient) CleanupTransferByMessageID(fileID domain.FileID) {
	if c.localNode == nil {
		return
	}
	c.localNode.CleanupTransferByMessageID(fileID)
}

// RemoveSenderMapping removes a single sender mapping by fileID.
func (c *DesktopClient) RemoveSenderMapping(fileID domain.FileID) bool {
	if c.localNode == nil {
		return false
	}
	return c.localNode.RemoveSenderMapping(fileID)
}
