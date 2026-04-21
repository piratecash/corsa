package service

import (
	"context"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
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
//   - ChatlogGateway — single owner of the SQLite-backed chatlog store.
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
	dm      *DMCrypto
	prober  *NodeProber
}

// Contact is the service-layer view of a trusted peer's cryptographic
// material. Produced by contactsFromFrame / DMCrypto.ensureRecipientContact
// and consumed by the decryption path.
type Contact struct {
	BoxKey       string
	PubKey       string
	BoxSignature string
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
	ID            string
	Sender        domain.PeerIdentity
	Recipient     domain.PeerIdentity
	Body          string
	ReplyTo       domain.MessageID
	Command       domain.FileAction // e.g. FileActionAnnounce for file transfers; empty for regular DMs
	CommandData   string            // JSON-encoded payload (e.g. FileAnnouncePayload); empty for regular DMs
	Timestamp     time.Time
	ReceiptStatus string
	DeliveredAt   domain.OptionalTime
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
	Address          string
	Connected        bool
	Welcome          string
	NodeID           string
	NodeType         string
	ListenerEnabled  bool
	ListenerAddress  string
	ClientVersion    string
	Services         []string
	Capabilities     []string
	KnownIDs         []string
	Contacts         map[string]Contact
	Peers            []string
	PeerHealth       []PeerHealth
	CaptureSessions  map[domain.ConnID]CaptureSession // active + recently-stopped capture sessions keyed by ConnID
	AggregateStatus  *AggregateStatus                 // node-computed aggregate network health; nil when node does not support the command yet
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

// NewDesktopClient wires the composition root: opens (or attaches to) the
// chatlog, builds every sub-service, and registers the MessageStoreAdapter
// with the embedded node so the node delegates message persistence to the
// desktop layer instead of managing its own chatlog.
func NewDesktopClient(appCfg config.App, nodeCfg config.Node, id *identity.Identity, localNode *node.Service) *DesktopClient {
	store := chatlog.NewStore(nodeCfg.EffectiveChatLogDir(), domain.PeerIdentity(id.Address), domain.ListenAddress(nodeCfg.ListenAddress))
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
	c.chatlog = &ChatlogGateway{store: c.chatLog, selfAddr: c.info.Address()}
	c.store = NewMessageStoreAdapter(c.chatlog, c.id)
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

// Close releases the chatlog SQLite database through the gateway. Called
// at shutdown so WAL checkpoint and file handles are released cleanly.
func (c *DesktopClient) Close() error {
	if c.chatlog != nil {
		return c.chatlog.Close()
	}
	if c.chatLog != nil {
		return c.chatLog.Close()
	}
	return nil
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
func (c *DesktopClient) DeletePeerHistory(identity domain.PeerIdentity) (int64, error) {
	return c.chatlog.DeletePeerHistory(identity)
}

// FetchChatlog reads the chat entries for a peer and returns a formatted
// JSON payload suitable for console / RPC consumption.
func (c *DesktopClient) FetchChatlog(topic, peerAddress string) (string, error) {
	return c.chatlog.FetchChatlog(topic, peerAddress)
}

// FetchChatlogPreviews reads the last entry per peer and returns a
// formatted JSON payload with preview-sized fields.
func (c *DesktopClient) FetchChatlogPreviews() (string, error) {
	return c.chatlog.FetchChatlogPreviews()
}

// FetchConversations lists all conversations with their message counts.
func (c *DesktopClient) FetchConversations() (string, error) {
	return c.chatlog.FetchConversations()
}

// HasEntryInConversation reports whether a message with the given ID
// exists in the conversation with peerAddress.
func (c *DesktopClient) HasEntryInConversation(peerAddress, messageID string) bool {
	return c.chatlog.HasEntryInConversation(peerAddress, messageID)
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

// BuildReachableIDs returns identities that have at least one live route
// in the routing table.
func (c *DesktopClient) BuildReachableIDs() map[domain.PeerIdentity]bool {
	return c.prober.BuildReachableIDs()
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

// DecryptIncomingMessage decrypts a local-change event into a DirectMessage.
func (c *DesktopClient) DecryptIncomingMessage(event protocol.LocalChangeEvent) *DirectMessage {
	return c.dm.DecryptIncomingMessage(event)
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

// FileTransferFilePath returns the on-disk path for a transferred file.
func (c *DesktopClient) FileTransferFilePath(fileID domain.FileID, isSender bool) string {
	if c.localNode == nil {
		return ""
	}
	return c.localNode.FileTransferFilePath(fileID, isSender)
}

// CleanupPeerTransfers removes all file transfer mappings and files
// associated with the given peer identity.
func (c *DesktopClient) CleanupPeerTransfers(peer domain.PeerIdentity) {
	if c.localNode == nil {
		return
	}
	c.localNode.CleanupPeerTransfers(peer)
}

// RemoveSenderMapping removes a single sender mapping by fileID.
func (c *DesktopClient) RemoveSenderMapping(fileID domain.FileID) bool {
	if c.localNode == nil {
		return false
	}
	return c.localNode.RemoveSenderMapping(fileID)
}

