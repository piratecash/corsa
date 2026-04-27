package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type UIEventType int

const (
	// UIEventMessagesUpdated — activeMessages changed (new message, receipt update, conversation switch).
	UIEventMessagesUpdated UIEventType = iota + 1
	// UIEventSidebarUpdated — peers/peerOrder/unread/preview changed.
	UIEventSidebarUpdated
	// UIEventStatusUpdated — node status changed (monitor update or health poll).
	UIEventStatusUpdated
	// UIEventBeep — play notification sound for every incoming DM (sender ≠ us),
	// regardless of which chat is active. Emitted from all three code paths in
	// onNewMessage (non-active, mid-switch, active) and the repair-path in
	// repairUnreadFromHeaders as a fallback.
	UIEventBeep
)

// UIEvent is the payload the router sends to the UI layer.
// The UI does NOT interpret the event — it just calls Snapshot() to get fresh state.
type UIEvent struct {
	Type UIEventType
}

type RouterPeerState struct {
	Preview ConversationPreview
	Unread  int
}

// RouterSnapshot is guaranteed consistent. The UI never writes to it.
type RouterSnapshot struct {
	ActivePeer     domain.PeerIdentity
	PeerClicked    bool
	Peers          map[domain.PeerIdentity]*RouterPeerState // shallow copy, safe for read
	PeerOrder      []domain.PeerIdentity
	ActiveMessages []DirectMessage
	CacheReady     bool // true when cache is loaded for ActivePeer (empty chat vs still loading)
	NodeStatus     NodeStatus
	SendStatus     string
	MyAddress      domain.PeerIdentity

	// Generation is a monotonically increasing counter bumped on every
	// state mutation inside DMRouter. UI-side caches can compare this
	// single value instead of sampling individual fields — a generation
	// change guarantees that at least one piece of state differs.
	Generation uint64
}

type DMRouter struct {
	client        *DesktopClient
	fileBridge    *FileTransferBridge
	eventBus      *ebus.Bus
	statusMonitor NodeStatusProvider

	mu               sync.RWMutex
	activePeer       domain.PeerIdentity
	peerClicked      bool
	peers            map[domain.PeerIdentity]*RouterPeerState
	peerOrder        []domain.PeerIdentity
	activeMessages   []DirectMessage
	cache            *ConversationCache
	sendStatus       string
	seenMessageIDs   map[string]struct{}
	peerGen          map[domain.PeerIdentity]uint64 // bumped by RemovePeer; goroutines compare to detect stale sends
	initialSynced    bool
	previewsSeeded   bool // true after seedPreviews() successfully set unread counts from SQL
	replayingStartup bool // true during buffered-event replay; suppresses Unread++ in updateSidebarFromEvent

	// startupComplete gates ebus message/receipt handlers. While false,
	// events are buffered in startupEventBuf. Set to true after runStartup
	// finishes replaying buffered events — all subsequent ebus events are
	// processed as live.
	startupComplete bool
	startupEventBuf []protocol.LocalChangeEvent
	startupDropped  int

	// snapGen is a monotonic counter bumped inside notify() under r.mu.Lock.
	// Each generation corresponds to a fresh snapshot stored in snapCache.
	// The UI goroutine (via Snapshot()) reads snapCache lock-free — it never
	// acquires r.mu, so writers cannot starve the UI.
	snapGen   atomic.Uint64
	snapCache atomic.Pointer[routerSnapshotCache]

	uiEvents        chan UIEvent
	uiOverflowCount atomic.Int64  // number of active retry goroutines in notify()
	startupDone     chan struct{} // closed after runStartup completes; used by external waiters

	// deleteRetry holds the sender-side pendingDelete map for in-flight
	// message_delete control DMs. Populated by SendMessageDelete, drained
	// by handleInboundMessageDeleteAck (any terminal status) or by the
	// retry loop (attempt budget exhausted). See dm_router_delete.go.
	deleteRetry *deleteRetryState

	// dispatchControlDeleteFn is a test-only override for the
	// dispatchMessageDelete wire path. When non-nil, dispatchMessageDelete
	// invokes this function instead of building a payload and calling
	// r.client.SendControlMessage. Production code leaves this nil and
	// runs the real dispatch. Tests that need to count dispatches or
	// avoid the rpc/identity stack assign a counter here.
	dispatchControlDeleteFn func(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) error

	// Pending UI widget actions (Gio widgets are NOT thread-safe).
	pendingScrollToEnd   bool
	pendingClearEditor   bool
	pendingClearReply    bool
	pendingRecipientText domain.PeerIdentity
}

// routerSnapshotCache holds a pre-built snapshot and the generation at which
// it was captured. Only the UI goroutine reads and writes the atomic pointer,
// so no additional synchronization is needed between snapshot consumers.
type routerSnapshotCache struct {
	gen  uint64
	snap RouterSnapshot
}

// PendingActions holds deferred widget mutations that must be applied
// on the UI goroutine (Gio widgets are NOT thread-safe).
type PendingActions struct {
	ScrollToEnd   bool
	ClearEditor   bool
	ClearReply    bool
	RecipientText domain.PeerIdentity
}

func NewDMRouter(client *DesktopClient, fileBridge *FileTransferBridge, eventBus *ebus.Bus, statusMonitor NodeStatusProvider) *DMRouter {
	r := &DMRouter{
		client:         client,
		fileBridge:     fileBridge,
		eventBus:       eventBus,
		statusMonitor:  statusMonitor,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    make(chan struct{}),
		deleteRetry:    newDeleteRetryState(),
	}
	// Seed snapCache so Snapshot() returns a valid (though minimal) state
	// immediately. Without this, the first frames after Start() render an
	// all-zero RouterSnapshot until runStartup completes asynchronously.
	r.snapCache.Store(&routerSnapshotCache{
		gen:  0,
		snap: r.buildSnapshotLocked(0),
	})
	return r
}

func (r *DMRouter) Subscribe() <-chan UIEvent {
	return r.uiEvents
}

// Snapshot returns the latest immutable state snapshot. Completely
// lock-free — the snapshot is built by writers (under their Lock hold)
// and stored via atomic.Pointer, so the UI goroutine never competes
// for r.mu. This eliminates the RWMutex writer-preference starvation
// that caused permanent UI freezes during ebus event bursts.
//
// CacheReady is recomputed on every call because ConversationCache
// state is guarded by its own mutex, independent of DMRouter mutations.
func (r *DMRouter) Snapshot() RouterSnapshot {
	cached := r.snapCache.Load()
	if cached == nil {
		return RouterSnapshot{}
	}
	snap := cached.snap
	snap.CacheReady = snap.ActivePeer != "" && r.cache.MatchesPeer(snap.ActivePeer)
	return snap
}

func (r *DMRouter) ConsumePendingActions() PendingActions {
	r.mu.Lock()
	pa := PendingActions{
		ScrollToEnd:   r.pendingScrollToEnd,
		ClearEditor:   r.pendingClearEditor,
		ClearReply:    r.pendingClearReply,
		RecipientText: r.pendingRecipientText,
	}
	r.pendingScrollToEnd = false
	r.pendingClearEditor = false
	r.pendingClearReply = false
	r.pendingRecipientText = ""
	r.mu.Unlock()
	return pa
}

func (r *DMRouter) SelectPeer(peerAddress domain.PeerIdentity) {
	r.selectPeerCore(peerAddress, true)
}

// AutoSelectPeer selects a peer programmatically (e.g. startup, UI fallback).
// When the peer changes, behaves identically to SelectPeer: optimistic
// unread clear, loadConversation, doMarkSeen, rollback on failure.
// When the peer is the same (re-selection), it is a true no-op: no state
// mutations, no unread clear, no doMarkSeen, no UI events, no goroutines.
// SelectPeer differs: same-peer re-click retries a failed load (cache miss)
// or retries doMarkSeen when Unread > 0 (stuck badge after rollback).
func (r *DMRouter) AutoSelectPeer(peerAddress domain.PeerIdentity) {
	r.selectPeerCore(peerAddress, false)
}

// selectPeerCore shares logic for SelectPeer and AutoSelectPeer.
// Both paths clear the unread badge optimistically and send seen receipts.
// userClicked affects retry behaviour on same-peer re-selection:
//   - cache miss → retries loadConversation + doMarkSeen
//   - cache valid, Unread > 0 (stuck badge after rollback) → retries doMarkSeen
//   - cache valid, Unread == 0 → true no-op
//
// Programmatic re-selection (AutoSelectPeer) of the same peer is always
// a true no-op regardless of cache or unread state.
func (r *DMRouter) selectPeerCore(peerAddress domain.PeerIdentity, userClicked bool) {
	peerAddress = normalizePeer(peerAddress)
	r.mu.Lock()
	changed := r.activePeer != peerAddress
	needLoad := changed
	needRetryMark := false
	if !changed && userClicked && !r.cache.MatchesPeer(peerAddress) {
		// Same peer re-clicked but cache never loaded (previous load failed).
		// Treat as needing a fresh load. Only for explicit clicks —
		// programmatic re-selection of the same peer is a true no-op.
		needLoad = true
	}
	if !changed && !needLoad && userClicked {
		// Cache is valid but check if unread badge is stuck (e.g. after
		// restorePeerUnread rolled back a failed doMarkSeen). Explicit
		// user re-click must retry clearing the badge.
		if ps, ok := r.peers[peerAddress]; ok && ps.Unread > 0 {
			needRetryMark = true
		}
	}

	if !changed && !needLoad && !needRetryMark {
		// True no-op: same peer, cache valid, no stuck badge.
		// No state mutations, no unread clear, no doMarkSeen, no UI events.
		r.mu.Unlock()
		return
	}

	// Past the no-op guard — we are either switching peers or retrying
	// a failed load. Commit state changes.
	r.activePeer = peerAddress
	r.peerClicked = true // chat is on screen — always treat as "seen"
	if changed {
		// Clear stale messages immediately so the UI never renders
		// the previous peer's conversation under the new header.
		r.activeMessages = nil
	}

	// Snapshot the current unread count so we can restore it if the
	// background doMarkSeen fails (optimistic clear with rollback).
	oldUnread := 0
	if ps, ok := r.peers[peerAddress]; ok {
		oldUnread = ps.Unread
	}
	r.mu.Unlock()

	// Optimistically clear the unread badge so the UI updates instantly.
	// If the background goroutine fails, the badge is restored to oldUnread.
	r.clearPeerUnread(peerAddress)

	// Notify synchronously so the UI re-renders with cleared messages
	// and cleared unread badge before the background load starts.
	if changed {
		r.notify(UIEventMessagesUpdated)
	}
	r.notify(UIEventSidebarUpdated)

	label := "SelectPeer"
	if !userClicked {
		label = "AutoSelectPeer"
	}
	go func() {
		defer recoverLog(label)
		if needLoad {
			if !r.loadConversation(peerAddress) {
				r.restorePeerUnread(peerAddress, oldUnread)
				return
			}
		}
		if !r.doMarkSeen(peerAddress) {
			r.restorePeerUnread(peerAddress, oldUnread)
		}
		r.notify(UIEventMessagesUpdated)
	}()
}

func (r *DMRouter) SendMessage(to domain.PeerIdentity, msg domain.OutgoingDM) {
	to = normalizePeer(to)

	r.mu.RLock()
	gen := r.peerGen[to]
	r.mu.RUnlock()

	r.setSendStatusNotify("sending…")

	go func() {
		defer recoverLog("SendMessage")

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		sent, err := r.client.SendDirectMessage(ctx, to, msg)
		cancel()

		r.mu.Lock()
		if r.peerGen[to] != gen {
			// Peer was removed while the send was in flight. Do not
			// recreate the sidebar entry from stale async work.
			r.mu.Unlock()
			log.Info().
				Str("peer", string(to)).
				Msg("dm_router: peer removed during in-flight send, discarding result")
			return
		}

		if err != nil {
			r.sendStatus = "send failed: " + err.Error()
			r.mu.Unlock()
			r.notify(UIEventStatusUpdated)
			ebus.PublishMessageSendFailed(r.eventBus, ebus.MessageSendFailedResult{
				To:  to,
				Err: err,
			})
			return
		}

		r.sendStatus = "message sent"
		r.pendingClearEditor = true
		r.pendingClearReply = true

		if sent != nil && r.cache.MatchesPeer(to) {
			r.cache.AppendMessage(*sent)
			r.activeMessages = r.cache.Messages()
			r.pendingScrollToEnd = true
		}

		if sent != nil {
			r.ensurePeerLocked(to)
			r.peers[to].Preview = ConversationPreview{
				PeerAddress: to,
				Sender:      sent.Sender,
				Body:        sent.Body,
				Timestamp:   sent.Timestamp,
			}
			r.promotePeerLocked(to)
		}
		r.mu.Unlock()

		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
		ebus.PublishMessageSent(r.eventBus, ebus.MessageSentResult{
			To:      to,
			Body:    msg.Body,
			ReplyTo: msg.ReplyTo,
		})
	}()
}

// SendFileAnnounce sends a file_announce DM and registers the sender-side
// file mapping. File transfer orchestration (prepare → send → commit/rollback)
// is delegated to FileTransferBridge; DMRouter handles only the peerGen
// stale-send guard and UI state updates.
func (r *DMRouter) SendFileAnnounce(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure func()) error {
	if r.fileBridge == nil {
		return fmt.Errorf("file transfer not available")
	}

	to = normalizePeer(to)

	r.mu.RLock()
	gen := r.peerGen[to]
	r.mu.RUnlock()

	r.setSendStatusNotify("sending…")

	go func() {
		defer recoverLog("SendFileAnnounce")

		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()

		result, err := r.fileBridge.PrepareAndSend(ctx, to, msg, meta)
		if err != nil {
			if onAsyncFailure != nil {
				onAsyncFailure()
			}
			r.setSendStatusNotify("file announce failed: " + err.Error())
			ebus.PublishFileSendFailed(r.eventBus, ebus.FileSendFailedResult{
				To:  to,
				Err: err,
			})
			return
		}

		r.mu.Lock()
		if r.peerGen[to] != gen {
			// Peer was removed while we were sending. The sender mapping
			// is committed but orphaned — clean up only this specific
			// mapping to avoid destroying legitimate transfers for the
			// same peer in a newer generation.
			r.mu.Unlock()
			r.fileBridge.RollbackMapping(result.FileID)
			if onAsyncFailure != nil {
				onAsyncFailure()
			}
			r.setSendStatusNotify("file announce cancelled: peer removed")
			ebus.PublishFileSendFailed(r.eventBus, ebus.FileSendFailedResult{
				To:     to,
				FileID: result.FileID,
				Err:    fmt.Errorf("peer removed during in-flight file announce"),
			})
			log.Info().
				Str("peer", string(to)).
				Str("file_id", string(result.FileID)).
				Msg("dm_router: peer removed after file commit, removed orphaned sender mapping")
			return
		}
		r.sendStatus = "message sent"
		r.pendingClearEditor = true
		r.pendingClearReply = true

		if r.cache.MatchesPeer(to) {
			r.cache.AppendMessage(*result.Sent)
			r.activeMessages = r.cache.Messages()
			r.pendingScrollToEnd = true
		}

		r.ensurePeerLocked(to)
		r.peers[to].Preview = ConversationPreview{
			PeerAddress: to,
			Sender:      result.Sent.Sender,
			Body:        result.Sent.Body,
			Timestamp:   result.Sent.Timestamp,
		}
		r.promotePeerLocked(to)
		r.mu.Unlock()

		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
		ebus.PublishFileSent(r.eventBus, ebus.FileSentResult{
			To:     to,
			FileID: result.FileID,
		})
	}()

	return nil
}

// FileBridge returns the file transfer bridge for callers that need
// direct access to file transfer operations (GUI, RPC).
func (r *DMRouter) FileBridge() *FileTransferBridge {
	return r.fileBridge
}

// tryRegisterFileReceive checks if a decrypted message is a file_announce
// from a remote peer and registers the receiver-side mapping if so. Safe to
// call multiple times for the same message — RegisterFileReceive is idempotent.
//
// Thread safety: called only from the DMRouter event loop (single goroutine)
// or from loadConversation (under router lifecycle). The underlying
// RegisterIncomingFileTransfer → FileTransferManager.RegisterFileReceive is
// protected by its own mutex.
func (r *DMRouter) tryRegisterFileReceive(msg *DirectMessage) {
	if msg == nil || r.fileBridge == nil {
		return
	}
	if msg.Command != domain.DMCommandFileAnnounce || msg.CommandData == "" {
		return
	}
	// Only register for incoming messages (sender is not us).
	if msg.Sender == r.client.Address() {
		return
	}
	r.fileBridge.RegisterIncoming(*msg)
}

func (r *DMRouter) ActivePeer() domain.PeerIdentity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activePeer
}

func (r *DMRouter) MyAddress() domain.PeerIdentity {
	return r.client.Address()
}

func (r *DMRouter) SetSendStatus(s string) {
	r.setSendStatusNotify(s)
}

// NotifyStatusChanged is called by NodeStatusMonitor when network state
// changes. It rebuilds the snapshot and emits UIEventStatusUpdated so
// the UI picks up the new NodeStatus.
func (r *DMRouter) NotifyStatusChanged() {
	r.notify(UIEventStatusUpdated)
}

// RemovePeer deletes an identity from the sidebar, the node's trust store,
// the conversation cache, and all chat history from the local database.
// If the removed identity was active, the selection is cleared so the UI
// shows the placeholder.
//
// Chat history is deleted first; if that fails, the in-memory state remains
// unchanged and the error is returned so the caller can display it.
// Trust store deletion is best-effort: it goes through an RPC to the local
// node, which may be unavailable. A failure is logged but does not prevent
// removal — the sidebar is built from in-memory peers, not from the trust
// store, so the identity will not reappear.
//
// The first return value is true when the removed identity was the active
// one (so the caller can decide what to select next).
func (r *DMRouter) RemovePeer(identity domain.PeerIdentity) (bool, error) {
	identity = normalizePeer(identity)
	id := string(identity)

	// Delete chat history from the local database. This is the gate
	// operation: if it fails, the peer is NOT removed and peerGen stays
	// unchanged so in-flight goroutines remain valid.
	if _, err := r.client.DeletePeerHistory(identity); err != nil {
		log.Error().Str("identity", id).Err(err).Msg("failed to delete identity chat history")
		return false, fmt.Errorf("delete identity %s: %w", id, err)
	}

	// Bump generation BEFORE any best-effort cleanup. In-flight goroutines
	// (SendMessage, SendFileAnnounce) that captured gen before this point
	// will see a stale generation and discard their results. Without this
	// ordering, a slow CleanupPeer could leave a window where in-flight
	// sends slip through the peerGen guard.
	r.mu.Lock()
	r.peerGen[identity]++
	r.mu.Unlock()

	// Best-effort: remove from the node trust store. The RPC requires a
	// live connection to the local node, which may be absent (e.g. during
	// shutdown or reconnect). Log the error but proceed with in-memory
	// cleanup — the sidebar no longer depends on the trust store.
	if err := r.client.DeleteContact(identity); err != nil {
		log.Warn().Str("identity", id).Err(err).Msg("trust store cleanup failed (best-effort)")
	}

	// Best-effort: clean up file transfer mappings and associated files
	// (transmit refs, downloaded files, partial downloads).
	if r.fileBridge != nil {
		r.fileBridge.CleanupPeer(identity)
	}

	r.mu.Lock()
	delete(r.peers, identity)
	r.removePeerLocked(identity)
	r.cache.Evict(identity)

	wasActive := r.activePeer == identity
	if wasActive {
		r.activePeer = ""
		r.peerClicked = false
		r.activeMessages = nil
	}

	r.mu.Unlock()

	r.notify(UIEventSidebarUpdated)
	if wasActive {
		r.notify(UIEventMessagesUpdated)
	}

	return wasActive, nil
}

// Start launches background goroutines and subscribes to DM-specific
// ebus events (messages, receipts). Network-layer events (peer health,
// aggregate status, contacts, identities) are handled by the
// NodeStatusMonitor — DMRouter does not subscribe to them.
func (r *DMRouter) Start() {
	// 1. Subscribe to DM-specific ebus events.
	r.subscribeEvents()

	// 2. Startup: load previews, auto-select first peer, seed the monitor.
	go r.runStartup()

	// 3. Background retry sweeper for pending message_delete control
	// DMs. Runs for the process lifetime — pending state is in-memory
	// only, so a process restart starts from a clean slate (see the
	// note in dm_router_delete.go about JSON persistence as a planned
	// follow-up).
	go r.deleteRetryLoop(context.Background())
}

// runStartup loads initial data from the database and then replays any
// message/receipt events that arrived via ebus during initialization.
// Buffered events are replayed under replayingStartup=true to suppress
// Unread++ (seedPreviews already loaded correct counts from SQL). Events
// that arrive during replay are drained as live (replayingStartup=false).
func (r *DMRouter) runStartup() {
	defer close(r.startupDone)
	defer recoverLog("initializeFromDB")
	r.initializeFromDB()

	// Phase 1: replay events buffered during initializeFromDB under
	// replayingStartup=true (these are pre-snapshot, suppress Unread++).
	r.mu.Lock()
	r.replayingStartup = true
	buf := r.startupEventBuf
	r.startupEventBuf = nil
	dropped := r.startupDropped
	r.startupDropped = 0
	r.mu.Unlock()

	for _, ev := range buf {
		r.safeHandleEvent(ev)
	}

	if dropped > 0 {
		log.Warn().Int("dropped", dropped).Msg("startup ebus buffer overflow: some events dropped, UI will reload from chatlog")
	}

	// Phase 2: switch to live mode. Any events that arrived during Phase 1
	// replay are drained as live (Unread++ and UIEventBeep enabled).
	r.mu.Lock()
	r.replayingStartup = false
	r.startupComplete = true
	remaining := r.startupEventBuf
	r.startupEventBuf = nil
	droppedLive := r.startupDropped
	r.startupDropped = 0
	r.mu.Unlock()

	for _, ev := range remaining {
		r.safeHandleEvent(ev)
	}

	if droppedLive > 0 {
		log.Warn().Int("dropped", droppedLive).Msg("startup ebus live buffer overflow: some events dropped, UI will reload from chatlog")
	}

	r.notify(UIEventMessagesUpdated)
	r.notify(UIEventSidebarUpdated)
}

// onEbusLocalChange handles TopicMessageNew and TopicReceiptUpdated events
// from ebus. Before startup completes, events are buffered (up to 256) and
// replayed by runStartup. After startup, events are processed immediately.
func (r *DMRouter) onEbusLocalChange(event protocol.LocalChangeEvent) {
	r.mu.Lock()
	if !r.startupComplete {
		const maxStartupBuf = 256
		if len(r.startupEventBuf) < maxStartupBuf {
			r.startupEventBuf = append(r.startupEventBuf, event)
		} else {
			r.startupDropped++
		}
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.safeHandleEvent(event)
}

func (r *DMRouter) safeHandleEvent(event protocol.LocalChangeEvent) {
	defer recoverLog("handleEvent")
	r.handleEvent(event)
}

// subscribeEvents wires up DM-specific ebus handlers. Network-layer
// events (peer health, aggregate status, contacts, identities) are
// handled by NodeStatusMonitor — DMRouter only subscribes to message
// and receipt topics.
func (r *DMRouter) subscribeEvents() {
	if r.eventBus == nil {
		return
	}

	// New direct message stored in chatlog.
	r.eventBus.Subscribe(ebus.TopicMessageNew, func(event protocol.LocalChangeEvent) {
		r.onEbusLocalChange(event)
	})

	// Delivery receipt status changed.
	r.eventBus.Subscribe(ebus.TopicReceiptUpdated, func(event protocol.LocalChangeEvent) {
		r.onEbusLocalChange(event)
	})

	// Inbound control DM (message_delete, message_delete_ack, ...).
	// Routed through onEbusLocalChange so the buffered-during-startup
	// pipeline applies uniformly. handleEvent dispatches by event.Type.
	r.eventBus.Subscribe(ebus.TopicMessageControl, func(event protocol.LocalChangeEvent) {
		r.onEbusLocalChange(event)
	})
}

func (r *DMRouter) handleEvent(event protocol.LocalChangeEvent) {
	switch event.Type {
	case protocol.LocalChangeNewMessage:
		if event.Topic != "dm" {
			return
		}
		r.onNewMessage(event)
	case protocol.LocalChangeReceiptUpdate:
		r.onReceiptUpdate(event)
	case protocol.LocalChangeNewControlMessage:
		r.onControlMessage(event)
	}
}

// onControlMessage is implemented in dm_router_delete.go. Keeping the
// dispatch in a sibling file isolates the message_delete /
// message_delete_ack code path so the high-traffic data-DM logic in
// this file is not contaminated by control-flow concerns.

// normalizePeer trims whitespace so that raw strings from events or headers
// never create duplicate keys in peers map or peerOrder.
func normalizePeer(p domain.PeerIdentity) domain.PeerIdentity {
	return domain.PeerIdentity(strings.TrimSpace(string(p)))
}

func (r *DMRouter) peerForMessage(event protocol.LocalChangeEvent) domain.PeerIdentity {
	if event.Sender == string(r.client.Address()) {
		return normalizePeer(domain.PeerIdentity(event.Recipient))
	}
	return normalizePeer(domain.PeerIdentity(event.Sender))
}

func (r *DMRouter) isActivePeer(peer domain.PeerIdentity) bool {
	r.mu.RLock()
	active := r.activePeer
	r.mu.RUnlock()
	return active == peer
}

func (r *DMRouter) onNewMessage(event protocol.LocalChangeEvent) {
	peerID := r.peerForMessage(event)

	// Register this message so the repair-path (repairUnreadFromHeaders)
	// won't double-count it as a new unread or trigger a duplicate beep.
	if event.MessageID != "" {
		r.mu.Lock()
		r.seenMessageIDs[event.MessageID] = struct{}{}
		r.mu.Unlock()
	}

	// During startup replay, suppress beep — these are old messages
	// already counted by seedPreviews().
	r.mu.RLock()
	replaying := r.replayingStartup
	r.mu.RUnlock()

	if !r.isActivePeer(peerID) {
		// Definitely not the active conversation — update sidebar only.
		if !r.updateSidebarFromEvent(event, peerID) {
			// Inline decrypt failed (missing contact keys, etc.) —
			// fall back to reading the latest preview from SQLite.
			isIncoming := event.Sender != string(r.client.Address())
			go func() {
				defer recoverLog("onNewMessage.nonActive.decryptFail")
				if !r.updatePreviewFromStore(peerID) {
					r.evictSeenMessages(event.MessageID)
					return
				}
				r.mu.Lock()
				r.ensurePeerLocked(peerID)
				// Re-check: if the user opened this chat while the
				// goroutine was running, SelectPeer already cleared
				// unread. Incrementing now would re-add the badge on
				// an already-visible conversation.
				if isIncoming && !replaying && r.activePeer != peerID {
					r.peers[peerID].Unread++
				}
				r.promotePeerLocked(peerID)
				r.mu.Unlock()
				r.notify(UIEventSidebarUpdated)
			}()
		} else {
			r.notify(UIEventSidebarUpdated)
		}
		// Sound notification for incoming messages (sender is not us).
		if event.Sender != string(r.client.Address()) && !replaying {
			r.notify(UIEventBeep)
		}
		return
	}

	// Active peer, but cache may still be loading (peer just switched).
	if !r.cache.MatchesPeer(peerID) {
		// Cache not ready yet — trigger a full reload which will pick up
		// the new message. Try to decrypt inline so (a) the sidebar preview
		// updates immediately and (b) we have the DirectMessage as a fallback
		// if the full reload fails.
		var decryptedMsg *DirectMessage
		msg := r.client.DecryptIncomingMessage(event)
		if msg != nil {
			r.tryRegisterFileReceive(msg)
			decryptedMsg = msg
			r.mu.Lock()
			r.ensurePeerLocked(peerID)
			r.peers[peerID].Preview = ConversationPreview{
				PeerAddress: peerID,
				Sender:      msg.Sender,
				Body:        msg.Body,
				Timestamp:   msg.Timestamp,
			}
			r.promotePeerLocked(peerID)
			r.mu.Unlock()
			r.notify(UIEventSidebarUpdated)
		}
		if event.Sender != string(r.client.Address()) && !replaying {
			r.notify(UIEventBeep)
		}
		go func() {
			defer recoverLog("onNewMessage.midSwitch")
			if !r.reloadAndRefreshPreview(peerID, event.MessageID) {
				// Full reload failed. If we decrypted the message inline
				// and the user is still on this peer, seed the cache so
				// the user sees the message instead of a blank screen.
				// The activePeer check MUST guard cache.Load — without it
				// a peer switch during the goroutine would overwrite the
				// ConversationCache for the new selection, corrupting
				// MatchesPeer/HasMessage for subsequent paths.
				if decryptedMsg != nil {
					r.mu.Lock()
					seeded := r.activePeer == peerID
					if seeded {
						r.cache.Load(peerID, []DirectMessage{*decryptedMsg})
						r.activeMessages = r.cache.Messages()
						r.pendingScrollToEnd = true
					}
					r.mu.Unlock()
					if seeded {
						r.notify(UIEventMessagesUpdated)
						r.notify(UIEventSidebarUpdated)
						// Message is now visible on screen — send seen
						// receipt to maintain the "on screen = read"
						// invariant (same as the success path below).
						r.doMarkSeen(peerID)
					}
				}
				return
			}
			r.notify(UIEventMessagesUpdated)
			r.notify(UIEventSidebarUpdated)
			// Active chat is on screen — always send seen receipts
			// now that the conversation has loaded.
			r.doMarkSeen(peerID)
		}()
		return
	}

	if r.cache.HasMessage(event.MessageID) {
		return
	}

	msg := r.client.DecryptIncomingMessage(event)
	if msg == nil {
		isIncoming := event.Sender != string(r.client.Address())
		if isIncoming && !replaying {
			r.notify(UIEventBeep)
		}
		go func() {
			defer recoverLog("onNewMessage.decryptFail")
			if !r.reloadAndRefreshPreview(peerID, event.MessageID) {
				return
			}
			r.notify(UIEventMessagesUpdated)
			r.notify(UIEventSidebarUpdated)
			if isIncoming {
				r.doMarkSeen(peerID)
			}
		}()
		return
	}

	r.tryRegisterFileReceive(msg)
	r.cache.AppendMessage(*msg)
	r.mu.Lock()
	r.activeMessages = r.cache.Messages()
	r.pendingScrollToEnd = true
	isIncoming := msg.Sender != r.client.Address()
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      msg.Sender,
		Body:        msg.Body,
		Timestamp:   msg.Timestamp,
	}
	r.promotePeerLocked(peerID)
	r.mu.Unlock()

	r.notify(UIEventMessagesUpdated)
	r.notify(UIEventSidebarUpdated)

	// Sound notification for every incoming message, regardless of which
	// chat is currently active. Suppressed during startup replay.
	if isIncoming && !replaying {
		r.notify(UIEventBeep)
	}

	// The active chat is on screen — mark incoming messages as seen
	// regardless of how the peer was selected (click or auto-select).
	if isIncoming {
		go r.doMarkSeen(peerID)
	}
}

func (r *DMRouter) onReceiptUpdate(event protocol.LocalChangeEvent) {
	receiptPeer := r.peerForMessage(event)

	if !r.isActivePeer(receiptPeer) {
		// Not the active peer — ignore; repair-path picks it up later.
		return
	}

	if !r.cache.MatchesPeer(receiptPeer) {
		// Active peer but cache still loading — trigger reload to pick up
		// the receipt update.
		go func() {
			r.loadConversation(receiptPeer)
			r.notify(UIEventMessagesUpdated)
		}()
		return
	}

	deliveredAt := domain.TimeFromNonZero(event.DeliveredAt)

	if r.cache.UpdateStatus(event.MessageID, event.Status, deliveredAt) {
		r.mu.Lock()
		r.activeMessages = r.cache.Messages()
		r.mu.Unlock()
		r.notify(UIEventMessagesUpdated)
	} else if !r.cache.HasMessage(event.MessageID) {
		go func() {
			r.loadConversation(receiptPeer)
			r.notify(UIEventMessagesUpdated)
		}()
	}
}

func (r *DMRouter) initializeFromDB() {
	r.resetIdentityState()

	// Always run an initial health probe at the end so contacts appear
	// in sidebar immediately, even when previews are empty.
	defer r.pollHealth()

	var previews []ConversationPreview
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		previews, err = r.client.FetchConversationPreviews(ctx)
		cancel()
		if err == nil && len(previews) > 0 {
			break
		}
		if attempt < maxRetries {
			log.Warn().Err(err).Int("attempt", attempt).Msg("seedPreviews fetch failed, retrying")
			time.Sleep(time.Duration(attempt) * time.Second)
		} else {
			log.Warn().Err(err).Msg("seedPreviews fetch failed after all retries")
		}
	}
	if len(previews) == 0 {
		return
	}

	r.seedPreviews(previews)

	me := r.client.Address()
	var firstPeer domain.PeerIdentity
	r.mu.RLock()
	if len(r.peerOrder) > 0 {
		firstPeer = r.peerOrder[0]
	}
	r.mu.RUnlock()

	if firstPeer == "" {
		for _, p := range previews {
			if p.PeerAddress != me && p.PeerAddress != "" {
				firstPeer = p.PeerAddress
				break
			}
		}
	}
	if firstPeer == "" {
		return
	}

	r.mu.Lock()
	var selectedPeer domain.PeerIdentity
	if strings.TrimSpace(string(r.activePeer)) != "" {
		selectedPeer = r.activePeer
	} else {
		selectedPeer = firstPeer
		r.pendingRecipientText = firstPeer
	}
	// Clear activePeer so AutoSelectPeer always sees a peer switch
	// and triggers a full load. Without this, a reconnect (activePeer
	// already set) would skip loadConversation because selectPeerCore
	// treats same-peer + programmatic selection as a no-op.
	r.activePeer = ""
	r.mu.Unlock()

	// Delegate to AutoSelectPeer which handles the full lifecycle:
	// set activePeer, peerClicked=true, optimistic unread clear,
	// loadConversation, doMarkSeen, and rollback on failure.
	r.AutoSelectPeer(selectedPeer)
	// pollHealth() is called via defer at function start.
}

func (r *DMRouter) resetIdentityState() {
	r.mu.Lock()
	r.peers = make(map[domain.PeerIdentity]*RouterPeerState)
	r.peerOrder = nil
	r.activePeer = ""
	r.peerClicked = false
	r.activeMessages = nil
	r.seenMessageIDs = make(map[string]struct{})
	r.initialSynced = false
	r.previewsSeeded = false
	r.sendStatus = ""
	r.pendingScrollToEnd = false
	r.pendingClearEditor = false
	r.pendingClearReply = false
	r.pendingRecipientText = ""
	r.mu.Unlock()

	// Clear the monitor so the next FetchAndSeed seeds fresh data from
	// ProbeNode instead of preserving stale state from a previous session.
	if r.statusMonitor != nil {
		r.statusMonitor.Reset()
	}

	r.cache.Load("", nil)
}

// pollHealth seeds the NodeStatusMonitor from a ProbeNode RPC and
// performs DM-specific repairs (unread badges, delivery receipts).
// Called once during startup (initializeFromDB).
//
// Network-layer fields (PeerHealth, AggregateStatus, Contacts, etc.)
// are delegated to NodeStatusMonitor.FetchAndSeed which handles
// ebus-aware merging. DMRouter only processes DM-specific data
// (DMHeaders, DeliveryReceipts) from the probe result.
func (r *DMRouter) pollHealth() {
	ctx := context.Background()

	var status NodeStatus
	if m, ok := r.statusMonitor.(*NodeStatusMonitor); ok {
		status = m.FetchAndSeed(ctx)
	} else {
		// Test doubles / mock providers — fall back to direct probe.
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		status = r.client.ProbeNode(probeCtx)
		cancel()
	}

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	activePeer := r.activePeer
	r.mu.RUnlock()

	if activePeer != "" {
		r.applyReceiptRepair(activePeer, status.DeliveryReceipts)
	}

	r.notify(UIEventStatusUpdated)
}

func (r *DMRouter) loadConversation(peerAddress domain.PeerIdentity) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	messages, err := r.client.FetchConversation(ctx, peerAddress)
	cancel()

	if err != nil {
		log.Warn().Err(err).Str("peer", string(peerAddress)).Msg("conversation load failed")
		return false
	}

	r.mu.RLock()
	current := r.activePeer
	r.mu.RUnlock()
	if current != peerAddress {
		return false
	}

	// Register receiver-side mappings for any file_announce messages loaded
	// from the database — ensures transfer state is restored after restart.
	myAddr := r.client.Address()
	for i := range messages {
		if messages[i].Command == domain.DMCommandFileAnnounce && messages[i].Sender != myAddr {
			r.tryRegisterFileReceive(&messages[i])
		}
	}

	r.cache.Load(peerAddress, messages)

	r.mu.Lock()
	if r.activePeer != peerAddress {
		r.mu.Unlock()
		return false
	}
	r.activeMessages = r.cache.Messages()
	r.pendingScrollToEnd = true
	r.mu.Unlock()
	return true
}

// doMarkSeen sends "seen" receipts for the active conversation.
// Verifies that activePeer still matches peerAddress before copying activeMessages.
// Without this check, a fast peer switch could cause doMarkSeen to grab messages
// from the new peer, send an empty/irrelevant MarkConversationSeen (which succeeds
// vacuously), and then falsely clear unread for the old peer — a permanent badge loss.
func (r *DMRouter) doMarkSeen(peerAddress domain.PeerIdentity) bool {
	r.mu.RLock()
	if r.activePeer != peerAddress {
		r.mu.RUnlock()
		// Peer switched since we started — activeMessages belong to a
		// different conversation.  Return false so the caller restores
		// the optimistic unread clear.
		return false
	}
	var msgs []DirectMessage
	if len(r.activeMessages) > 0 {
		msgs = append([]DirectMessage(nil), r.activeMessages...)
	}
	r.mu.RUnlock()

	if len(msgs) == 0 {
		// No messages loaded — conversation may not have loaded yet.
		return false
	}

	seenCtx, seenCancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := r.client.MarkConversationSeen(seenCtx, peerAddress, msgs)
	seenCancel()

	if err != nil {
		log.Warn().Err(err).Str("peer", string(peerAddress)).Msg("MarkConversationSeen failed")
		return false
	}
	r.clearPeerUnread(peerAddress)
	r.notify(UIEventSidebarUpdated)
	return true
}

func (r *DMRouter) applyReceiptRepair(activePeer domain.PeerIdentity, receipts []DeliveryReceipt) {
	if activePeer == "" || !r.cache.MatchesPeer(activePeer) {
		return
	}

	myAddr := r.client.Address()
	updated := false
	for _, rc := range receipts {
		var peer domain.PeerIdentity
		if rc.Sender == myAddr {
			peer = rc.Recipient
		} else if rc.Recipient == myAddr {
			peer = rc.Sender
		} else {
			continue
		}
		if peer != activePeer {
			continue
		}

		if r.cache.UpdateStatus(rc.MessageID, rc.Status, domain.TimeFromNonZero(rc.DeliveredAt)) {
			updated = true
		}
	}

	if updated {
		r.mu.Lock()
		r.activeMessages = r.cache.Messages()
		r.mu.Unlock()
		r.notify(UIEventMessagesUpdated)
	}
}

func (r *DMRouter) seedPreviews(previews []ConversationPreview) {
	me := r.client.Address()

	// Sort: unread first by unread count descending, then by most recent activity.
	// Within the unread group, higher unread counts rank first.
	// This ensures deterministic sidebar order on startup. The UI layer applies
	// its own 4-tier sort (online/offline × unread/read) using the snapshot
	// data, so this order serves as a stable tiebreaker.
	sort.SliceStable(previews, func(i, j int) bool {
		ui, uj := previews[i].UnreadCount, previews[j].UnreadCount
		if (ui > 0) != (uj > 0) {
			return ui > 0 // unread before read
		}
		if ui > 0 && uj > 0 && ui != uj {
			return ui > uj // within unread group: higher count first
		}
		return previews[i].Timestamp.After(previews[j].Timestamp)
	})

	r.mu.Lock()
	// Track peers whose event-path data is fresher than the SQL snapshot.
	// These peers keep their current peerOrder position instead of being
	// repositioned by the stale startup sort.
	fresherPeers := make(map[domain.PeerIdentity]struct{})
	for _, p := range previews {
		if p.PeerAddress == me || p.PeerAddress == "" {
			continue
		}
		r.ensurePeerLocked(p.PeerAddress)
		existing := r.peers[p.PeerAddress]
		// Skip if the ebus event-path already delivered fresher data for this
		// peer (ebus handlers run in parallel with initializeFromDB).
		if !existing.Preview.Timestamp.IsZero() && !existing.Preview.Timestamp.Before(p.Timestamp) {
			fresherPeers[p.PeerAddress] = struct{}{}
			continue
		}
		existing.Preview = p
		// Apply the SQL unread count unconditionally. ListConversationsCtx
		// is the source of truth for unread after startup — if it reports 0,
		// any stale event-path badge must be cleared, not just increased.
		existing.Unread = p.UnreadCount
	}

	// Rebuild peerOrder: peers whose SQL data was applied are repositioned
	// according to the startup sort; fresher/event-only peers keep their
	// current relative position in peerOrder.
	//
	// Strategy: walk current peerOrder and collect slot indices that belong
	// to "SQL-applied" peers (not fresher, present in previews). Then fill
	// those slots with the SQL-sorted order while leaving other slots
	// untouched. Finally, append any SQL-applied peers that were newly
	// created by ensurePeerLocked (not yet in peerOrder).
	sqlApplied := make(map[domain.PeerIdentity]struct{}, len(previews))
	sqlSorted := make([]domain.PeerIdentity, 0, len(previews))
	seen := make(map[domain.PeerIdentity]struct{}, len(previews))
	for _, p := range previews {
		if p.PeerAddress == me || p.PeerAddress == "" {
			continue
		}
		if _, dup := seen[p.PeerAddress]; dup {
			continue
		}
		seen[p.PeerAddress] = struct{}{}
		if _, fresher := fresherPeers[p.PeerAddress]; fresher {
			continue
		}
		sqlApplied[p.PeerAddress] = struct{}{}
		sqlSorted = append(sqlSorted, p.PeerAddress)
	}

	newOrder := make([]domain.PeerIdentity, 0, len(r.peerOrder))
	sqlIdx := 0
	for _, peer := range r.peerOrder {
		if _, ok := sqlApplied[peer]; ok {
			// This slot held a SQL-applied peer; replace with next
			// peer from the sorted SQL order.
			if sqlIdx < len(sqlSorted) {
				newOrder = append(newOrder, sqlSorted[sqlIdx])
				sqlIdx++
			}
		} else {
			// Fresher or event-only peer — keep in place.
			newOrder = append(newOrder, peer)
		}
	}
	// Append any remaining SQL-sorted peers that were newly created by
	// ensurePeerLocked and didn't occupy a slot in the old peerOrder.
	for ; sqlIdx < len(sqlSorted); sqlIdx++ {
		newOrder = append(newOrder, sqlSorted[sqlIdx])
	}
	r.peerOrder = newOrder
	r.previewsSeeded = true

	r.mu.Unlock()
}

// updateSidebarFromEvent tries to decrypt the incoming event inline and update
// peers[].Preview + Unread. Returns true when the preview was updated
// successfully, false when decryption failed and the caller must fall back to
// updatePreviewFromStore.
func (r *DMRouter) updateSidebarFromEvent(event protocol.LocalChangeEvent, peerID domain.PeerIdentity) bool {
	msg := r.client.DecryptIncomingMessage(event)
	if msg == nil {
		return false
	}

	isIncoming := msg.Sender != r.client.Address()

	r.mu.Lock()
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: peerID,
		Sender:      msg.Sender,
		Body:        msg.Body,
		Timestamp:   msg.Timestamp,
	}
	if isIncoming && !r.replayingStartup && r.activePeer != peerID {
		// Skip Unread++ when:
		// - startup replay (seedPreviews() already set correct counts)
		// - active peer (chat is on screen, message is visible — doMarkSeen
		//   will send seen receipts; incrementing would show a false badge)
		r.peers[peerID].Unread++
	}
	r.promotePeerLocked(peerID)
	r.mu.Unlock()
	return true
}

// updatePreviewFromStore fetches the last message for peer from the local
// chatlog and updates peers[].Preview. It only touches router state — UI
// notification is the caller's responsibility. Returns true if the preview
// was successfully fetched (even if nil — meaning no messages in chatlog yet).
// Returns false on transient errors (chatlog unavailable, timeout, etc.).
//
// When the preview is nil (no chatlog entries), the peer state is left
// untouched — the peer may have been created by repairUnreadFromHeaders
// based on DMHeaders for a message not yet persisted to chatlog. Deleting
// the peer here would erase the unread badge and sidebar entry permanently.
func (r *DMRouter) updatePreviewFromStore(peer domain.PeerIdentity) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	preview, err := r.client.FetchSinglePreview(ctx, peer)
	cancel()

	if err != nil {
		return false
	}

	if preview != nil {
		r.mu.Lock()
		r.ensurePeerLocked(peer)
		r.peers[peer].Preview = *preview
		r.mu.Unlock()
	}
	// nil preview → no chatlog entries yet. Leave peer state as-is.
	return true
}

// updatePreviewFromCache builds RouterPeerState.Preview from the last
// message in activeMessages. Used as a fallback when updatePreviewFromStore
// fails but loadConversation already populated the cache.
// Guards against stale-peer race: if the user switched away (activePeer !=
// peer), activeMessages belong to a different conversation and must not be
// used to rebuild this peer's preview.
func (r *DMRouter) updatePreviewFromCache(peer domain.PeerIdentity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.activePeer != peer {
		return
	}
	if len(r.activeMessages) == 0 {
		return
	}
	last := r.activeMessages[len(r.activeMessages)-1]
	r.ensurePeerLocked(peer)
	r.peers[peer].Preview = ConversationPreview{
		PeerAddress: peer,
		Sender:      last.Sender,
		Body:        last.Body,
		Timestamp:   last.Timestamp,
	}
}

// reloadAndRefreshPreview runs loadConversation followed by
// updatePreviewFromStore. If loadConversation fails, the messageID is
// evicted from seenMessageIDs so repairUnreadFromHeaders can rediscover it.
// Returns false only when loadConversation fails (no messages loaded).
// Returns true when loadConversation succeeds, even if the subsequent
// preview refresh fails — the caller should still emit MessagesUpdated
// and run doMarkSeen because the conversation data is in cache.
// On partial success (load OK, preview fail), the message is already in
// cache so eviction is NOT performed — the dedup gate must stay closed
// to prevent redundant rediscovery on the next health poll.
func (r *DMRouter) reloadAndRefreshPreview(peerID domain.PeerIdentity, messageID string) bool {
	if !r.loadConversation(peerID) {
		r.evictSeenMessages(messageID)
		return false
	}
	if !r.updatePreviewFromStore(peerID) {
		// chatlog query failed, but messages are in cache.
		// Build the preview from the last cached message so the
		// sidebar stays current. No eviction: the message is already
		// loaded into cache, so the dedup gate must remain closed.
		r.updatePreviewFromCache(peerID)
	}
	return true
}

// evictSeenMessages removes message IDs from seenMessageIDs so that
// repairUnreadFromHeaders can rediscover them on the next health poll.
// Called when a fallback operation (updatePreviewFromStore, loadConversation)
// fails transiently — without this rollback the dedup gate would permanently
// suppress the message.
func (r *DMRouter) evictSeenMessages(ids ...string) {
	r.mu.Lock()
	for _, id := range ids {
		if id != "" {
			delete(r.seenMessageIDs, id)
		}
	}
	r.mu.Unlock()
}

func (r *DMRouter) refreshPreviewForPeer(peer domain.PeerIdentity, messageIDs []string) {
	defer recoverLog("refreshPreviewForPeer")
	if !r.updatePreviewFromStore(peer) {
		// Preview load failed — evict from seenMessageIDs so the next
		// repair cycle can rediscover and retry these messages.
		r.evictSeenMessages(messageIDs...)
	}
	r.notify(UIEventSidebarUpdated)
}

func (r *DMRouter) repairUnreadFromHeaders(status NodeStatus) {
	me := r.client.Address()

	// ---------------------------------------------------------------
	// Phase 1 (short lock): read mutable flags, snapshot seenMessageIDs.
	// The write lock is released before the O(N) header scan so that
	// Snapshot() callers (UI goroutine, 60 FPS) are not blocked for the
	// duration of the full iteration. Previously the entire loop ran
	// under r.mu.Lock, causing writer starvation and UI freezes.
	// ---------------------------------------------------------------
	r.mu.Lock()
	selected := r.activePeer

	firstSync := !r.initialSynced
	if firstSync {
		r.initialSynced = true
	}
	// skipUnreadCount is true only when seedPreviews() already set unread
	// counts from SQL. DMHeaders don't carry delivery_status, so blindly
	// incrementing would double-count every incoming message. When
	// seedPreviews never ran (empty/failed preview load), we must count
	// normally — there is nothing to double-count.
	skipUnreadCount := firstSync && r.previewsSeeded

	// Copy seenMessageIDs keys for the lock-free classification pass.
	// The copy is O(len(seenMessageIDs)) but is bounded by the total
	// number of ever-seen messages and runs once per 5-second poll —
	// far cheaper than holding the write lock during the header scan.
	seenCopy := make(map[string]struct{}, len(r.seenMessageIDs))
	for id := range r.seenMessageIDs {
		seenCopy[id] = struct{}{}
	}
	r.mu.Unlock()

	// ---------------------------------------------------------------
	// Phase 2 (lock-free): classify each header into new/seen using
	// the snapshot. cache.HasMessage has its own internal lock.
	// ---------------------------------------------------------------
	type headerAction struct {
		peer            domain.PeerIdentity
		id              string
		isIncoming      bool
		incrementUnread bool
		triggerReload   bool
		refreshSidebar  bool
		triggerBeep     bool
	}
	var actions []headerAction

	for _, header := range status.DMHeaders {
		if _, ok := seenCopy[header.ID]; ok {
			continue
		}

		var peer domain.PeerIdentity
		isIncoming := false
		triggerBeep := false
		if header.Sender == me {
			peer = normalizePeer(header.Recipient)
		} else if header.Recipient == me {
			peer = normalizePeer(header.Sender)
			isIncoming = true
			if !firstSync && peer != selected {
				triggerBeep = true
			}
		} else {
			continue
		}

		incrementUnread := isIncoming && peer != selected && !skipUnreadCount
		triggerReload := peer == selected && selected != "" && !r.cache.HasMessage(header.ID)
		refreshSidebar := peer != "" && peer != selected

		actions = append(actions, headerAction{
			peer:            peer,
			id:              header.ID,
			isIncoming:      isIncoming,
			incrementUnread: incrementUnread,
			triggerReload:   triggerReload,
			refreshSidebar:  refreshSidebar,
			triggerBeep:     triggerBeep,
		})
	}

	// ---------------------------------------------------------------
	// Phase 3 (short lock): apply classified mutations.
	// ---------------------------------------------------------------
	hasNew := false
	needReload := false
	peerMessageIDs := make(map[domain.PeerIdentity][]string)
	var reloadMessageIDs []string

	r.mu.Lock()
	for _, a := range actions {
		// Re-check under lock: another goroutine may have inserted the
		// same ID between Phase 1 snapshot and now.
		if _, ok := r.seenMessageIDs[a.id]; ok {
			continue
		}
		r.seenMessageIDs[a.id] = struct{}{}

		if a.isIncoming {
			r.ensurePeerLocked(a.peer)
		}

		if a.triggerBeep {
			hasNew = true
		}
		if a.incrementUnread {
			r.ensurePeerLocked(a.peer)
			r.peers[a.peer].Unread++
			r.promotePeerLocked(a.peer)
		}
		if a.triggerReload {
			needReload = true
			reloadMessageIDs = append(reloadMessageIDs, a.id)
		}
		// Skip the active peer from sidebar-only preview refresh — its
		// messages are handled by the loadConversation path below, which
		// has its own rollback via reloadMessageIDs. Running both in
		// parallel would cause refreshPreviewForPeer to evict IDs that
		// loadConversation already recovered, triggering duplicate
		// rediscovery and spurious UIEventBeep on the next health poll.
		if a.refreshSidebar {
			peerMessageIDs[a.peer] = append(peerMessageIDs[a.peer], a.id)
		}
	}
	r.mu.Unlock()

	for peer, ids := range peerMessageIDs {
		go r.refreshPreviewForPeer(peer, ids)
	}

	if hasNew {
		r.notify(UIEventBeep)
	}

	if needReload && selected != "" {
		if r.loadConversation(selected) {
			if !r.updatePreviewFromStore(selected) {
				// chatlog query failed, but messages are in cache.
				// Build the preview from the last cached message so the
				// sidebar stays current even when SQLite is transiently
				// unavailable.
				r.updatePreviewFromCache(selected)
			}
			r.notify(UIEventMessagesUpdated)
			// Active chat is on screen — always send seen receipts,
			// regardless of how the peer was selected.
			go r.doMarkSeen(selected)
		} else {
			// Reload failed — the new messages are not in activeMessages.
			// Evict their IDs from seenMessageIDs so the next repair cycle
			// re-discovers them and retries the reload.
			r.evictSeenMessages(reloadMessageIDs...)
		}
	}

	r.notify(UIEventSidebarUpdated)
}

func (r *DMRouter) clearPeerUnread(peer domain.PeerIdentity) {
	r.mu.Lock()
	if ps, ok := r.peers[peer]; ok {
		ps.Unread = 0
	}
	r.mu.Unlock()
}

// restorePeerUnread restores unread badge after optimistic clear failed
// (e.g., doMarkSeen network error). If count is 0, no update is needed.
func (r *DMRouter) restorePeerUnread(peer domain.PeerIdentity, count int) {
	if count <= 0 {
		return
	}
	r.mu.Lock()
	if ps, ok := r.peers[peer]; ok {
		ps.Unread = count
	}
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
}

func (r *DMRouter) ensurePeerLocked(peer domain.PeerIdentity) {
	if _, ok := r.peers[peer]; !ok {
		r.peers[peer] = &RouterPeerState{}
		r.peerOrder = append(r.peerOrder, peer)
	}
}

func (r *DMRouter) promotePeerLocked(peer domain.PeerIdentity) {
	if peer == "" {
		return
	}
	filtered := r.peerOrder[:0]
	for _, item := range r.peerOrder {
		if item == peer {
			continue
		}
		filtered = append(filtered, item)
	}
	r.peerOrder = append([]domain.PeerIdentity{peer}, filtered...)
}

func (r *DMRouter) removePeerLocked(peer domain.PeerIdentity) {
	filtered := r.peerOrder[:0]
	for _, item := range r.peerOrder {
		if item == peer {
			continue
		}
		filtered = append(filtered, item)
	}
	r.peerOrder = filtered[:len(filtered):len(filtered)]
}

func (r *DMRouter) setSendStatus(s string) {
	r.mu.Lock()
	r.sendStatus = s
	r.mu.Unlock()
}

// setSendStatusNotify updates the send status and emits UIEventStatusUpdated.
// Consolidates the repeated lock→set→unlock→notify(UIEventStatusUpdated)
// pattern used in error/success paths of SendFileAnnounce.
func (r *DMRouter) setSendStatusNotify(s string) {
	r.setSendStatus(s)
	r.notify(UIEventStatusUpdated)
}

// buildSnapshotLocked constructs an immutable RouterSnapshot from the
// current mutable state. Must be called under r.mu (read or write lock).
//
// The snapshot is a deep copy — all maps, slices, and pointers in
// NodeStatus are cloned so the UI goroutine can read the snapshot
// without any lock, even while ebus handlers mutate the live state.
func (r *DMRouter) buildSnapshotLocked(gen uint64) RouterSnapshot {
	peersCopy := make(map[domain.PeerIdentity]*RouterPeerState, len(r.peers))
	for k, v := range r.peers {
		clone := *v
		peersCopy[k] = &clone
	}

	// NodeStatus is owned by the monitor; reading it here avoids
	// duplicating the data inside DMRouter. The monitor returns a
	// deep copy, so the snapshot is fully immutable.
	var ns NodeStatus
	if r.statusMonitor != nil {
		ns = r.statusMonitor.NodeStatus()
	}

	return RouterSnapshot{
		Generation:     gen,
		ActivePeer:     r.activePeer,
		PeerClicked:    r.peerClicked,
		Peers:          peersCopy,
		PeerOrder:      append([]domain.PeerIdentity(nil), r.peerOrder...),
		ActiveMessages: append([]DirectMessage(nil), r.activeMessages...),
		CacheReady:     r.activePeer != "" && r.cache.MatchesPeer(r.activePeer),
		NodeStatus:     ns,
		SendStatus:     r.sendStatus,
		MyAddress:      r.client.Address(),
	}
}

// deepCopyNodeStatus creates an independent copy of NodeStatus with all
// reference types (maps, slices, the AggregateStatus pointer) cloned.
// Without this, the lock-free Snapshot() path would expose live maps to
// the UI while ebus handlers mutate them under r.mu — a concurrent map
// read/write panic.
//
// Timestamp optionality is expressed via domain.OptionalTime (a value
// type) in every snapshot-visible struct (PeerHealth, CaptureSession,
// DirectMessage, PendingMessage). Copying those structs by value is a
// true deep copy of the timestamp state — there are no shared *time.Time
// pointers that could alias monitor-owned memory. New optional-time
// fields added to these structs need no plumbing change here.
func deepCopyNodeStatus(src NodeStatus) NodeStatus {
	dst := src // shallow copy of all scalar fields

	// Maps — must be cloned to avoid aliasing live state. Values are
	// value-types (or value-type snapshots like CaptureSession), so the
	// per-entry assignment is a deep copy.
	if src.Contacts != nil {
		dst.Contacts = make(map[string]Contact, len(src.Contacts))
		for k, v := range src.Contacts {
			dst.Contacts[k] = v
		}
	}
	if src.ReachableIDs != nil {
		dst.ReachableIDs = make(map[domain.PeerIdentity]bool, len(src.ReachableIDs))
		for k, v := range src.ReachableIDs {
			dst.ReachableIDs[k] = v
		}
	}
	if src.CaptureSessions != nil {
		dst.CaptureSessions = make(map[domain.ConnID]CaptureSession, len(src.CaptureSessions))
		for k, v := range src.CaptureSessions {
			dst.CaptureSessions[k] = v
		}
	}

	// Pointer — clone the pointed-to struct. AggregateStatus is the only
	// pointer field that remains intentionally heap-bound (nil = "node
	// does not support this command yet").
	if src.AggregateStatus != nil {
		clone := *src.AggregateStatus
		dst.AggregateStatus = &clone
	}

	// Slices — append(nil, src...) creates an independent backing array.
	// All element types are value types (domain.OptionalTime et al.), so
	// the element copy is complete.
	dst.Services = append([]string(nil), src.Services...)
	dst.Capabilities = append([]string(nil), src.Capabilities...)
	dst.KnownIDs = append([]string(nil), src.KnownIDs...)
	dst.Peers = append([]string(nil), src.Peers...)
	dst.Messages = append([]string(nil), src.Messages...)
	dst.MessageIDs = append([]string(nil), src.MessageIDs...)
	dst.DirectMessageIDs = append([]string(nil), src.DirectMessageIDs...)
	dst.Gazeta = append([]string(nil), src.Gazeta...)
	dst.PeerHealth = append([]PeerHealth(nil), src.PeerHealth...)
	dst.DirectMessages = append([]DirectMessage(nil), src.DirectMessages...)
	dst.DMHeaders = append([]DMHeader(nil), src.DMHeaders...)
	dst.PendingMessages = append([]PendingMessage(nil), src.PendingMessages...)
	dst.DeliveryReceipts = append([]DeliveryReceipt(nil), src.DeliveryReceipts...)

	return dst
}

// notify builds a fresh snapshot under Lock, stores it atomically, and
// sends a UIEvent. The Lock acquisition is safe because every call site
// invokes notify() AFTER releasing r.mu — there is no nested-lock risk.
//
// This design makes Snapshot() completely lock-free: the UI goroutine
// never competes with writers for r.mu, eliminating the RWMutex
// writer-preference starvation that caused permanent UI freezes during
// ebus event bursts.
//
// If the channel is full, a per-event background retry with exponential
// backoff (50ms → 100ms → 200ms) ensures the event is eventually
// delivered. Atomic counter caps concurrent retry goroutines at 8.
func (r *DMRouter) notify(eventType UIEventType) {
	// Build and cache the snapshot under Lock. The snapshot is an
	// immutable deep copy — once stored via atomic.Pointer, the UI
	// goroutine reads it without any lock.
	r.mu.Lock()
	gen := r.snapGen.Add(1)
	snap := r.buildSnapshotLocked(gen)
	r.snapCache.Store(&routerSnapshotCache{gen: gen, snap: snap})
	r.mu.Unlock()

	ev := UIEvent{Type: eventType}
	select {
	case r.uiEvents <- ev:
		return
	default:
	}

	// Channel full — launch a per-event retry so no event type is lost.
	const maxRetryGoroutines = 8
	if r.uiOverflowCount.Add(1) > int64(maxRetryGoroutines) {
		r.uiOverflowCount.Add(-1)
		log.Warn().Int("event", int(eventType)).Msg("UIEvent overflow: too many pending retries, dropping event")
		return
	}
	log.Warn().Int("event", int(eventType)).Msg("UIEvent channel full, scheduling deferred retry")
	go func() {
		defer r.uiOverflowCount.Add(-1)
		delay := 50 * time.Millisecond
		for i := 0; i < 3; i++ {
			time.Sleep(delay)
			select {
			case r.uiEvents <- ev:
				return
			default:
			}
			delay *= 2 // 50ms → 100ms → 200ms
		}
		log.Warn().Int("event", int(eventType)).Msg("UIEvent retry exhausted after 3 attempts")
	}()
}

func recoverLog(label string) {
	if r := recover(); r != nil {
		log.Error().Interface("panic", r).Str("label", label).Msg("recovered panic in DMRouter")
	}
}
