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

	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
)

type UIEventType int

const (
	// UIEventMessagesUpdated — activeMessages changed (new message, receipt update, conversation switch).
	UIEventMessagesUpdated UIEventType = iota + 1
	// UIEventSidebarUpdated — peers/peerOrder/unread changed.
	UIEventSidebarUpdated
	// UIEventStatusUpdated — nodeStatus changed (health poll).
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
	MyAddress      string
}

type DMRouter struct {
	client *DesktopClient

	mu               sync.RWMutex
	activePeer       domain.PeerIdentity
	peerClicked      bool
	peers            map[domain.PeerIdentity]*RouterPeerState
	peerOrder        []domain.PeerIdentity
	activeMessages   []DirectMessage
	cache            *ConversationCache
	nodeStatus       NodeStatus
	sendStatus       string
	seenMessageIDs   map[string]struct{}
	initialSynced    bool
	previewsSeeded   bool // true after seedPreviews() successfully set unread counts from SQL
	replayingStartup bool // true during buffered-event replay; suppresses Unread++ in updateSidebarFromEvent

	uiEvents        chan UIEvent
	uiOverflowCount atomic.Int64  // number of active retry goroutines in notify()
	startupDone     chan struct{} // closed after initializeFromDB completes; gates event listener

	// Pending UI widget actions (Gio widgets are NOT thread-safe).
	pendingScrollToEnd   bool
	pendingClearEditor   bool
	pendingRecipientText domain.PeerIdentity
}

// PendingActions holds deferred widget mutations that must be applied
// on the UI goroutine (Gio widgets are NOT thread-safe).
type PendingActions struct {
	ScrollToEnd   bool
	ClearEditor   bool
	RecipientText domain.PeerIdentity
}

func NewDMRouter(client *DesktopClient) *DMRouter {
	return &DMRouter{
		client:         client,
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:      make([]domain.PeerIdentity, 0),
		seenMessageIDs: make(map[string]struct{}),
		cache:          NewConversationCache(),
		uiEvents:       make(chan UIEvent, 32),
		startupDone:    make(chan struct{}),
	}
}

func (r *DMRouter) Subscribe() <-chan UIEvent {
	return r.uiEvents
}

// Snapshot is safe to call from the UI goroutine at any time.
func (r *DMRouter) Snapshot() RouterSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peersCopy := make(map[domain.PeerIdentity]*RouterPeerState, len(r.peers))
	for k, v := range r.peers {
		clone := *v
		peersCopy[k] = &clone
	}

	return RouterSnapshot{
		ActivePeer:     r.activePeer,
		PeerClicked:    r.peerClicked,
		Peers:          peersCopy,
		PeerOrder:      append([]domain.PeerIdentity(nil), r.peerOrder...),
		ActiveMessages: append([]DirectMessage(nil), r.activeMessages...),
		CacheReady:     r.activePeer != "" && r.cache.MatchesPeer(string(r.activePeer)),
		NodeStatus:     r.nodeStatus,
		SendStatus:     r.sendStatus,
		MyAddress:      r.client.Address(),
	}
}

func (r *DMRouter) ConsumePendingActions() PendingActions {
	r.mu.Lock()
	pa := PendingActions{
		ScrollToEnd:   r.pendingScrollToEnd,
		ClearEditor:   r.pendingClearEditor,
		RecipientText: r.pendingRecipientText,
	}
	r.pendingScrollToEnd = false
	r.pendingClearEditor = false
	r.pendingRecipientText = ""
	r.mu.Unlock()
	return pa
}

func (r *DMRouter) SelectPeer(peerAddress domain.PeerIdentity) {
	r.selectPeerCore(peerAddress, true)
}

// AutoSelectPeer selects a peer programmatically (e.g. startup, UI fallback).
// Behaves like SelectPeer in terms of seen receipts and unread clearing —
// if the chat is on screen, it counts as read. The only difference from
// SelectPeer: re-selecting the same peer with a failed cache is a no-op
// (no retry without an explicit user click).
func (r *DMRouter) AutoSelectPeer(peerAddress domain.PeerIdentity) {
	r.selectPeerCore(peerAddress, false)
}

// selectPeerCore shares logic for SelectPeer and AutoSelectPeer.
// Both paths clear the unread badge optimistically and send seen receipts.
// userClicked only affects retry behaviour: re-clicking the same peer
// retries a failed load, while programmatic re-selection is a no-op.
func (r *DMRouter) selectPeerCore(peerAddress domain.PeerIdentity, userClicked bool) {
	addr := string(peerAddress)
	r.mu.Lock()
	changed := r.activePeer != peerAddress
	r.activePeer = peerAddress
	r.peerClicked = true // chat is on screen — always treat as "seen"
	needLoad := changed
	// Snapshot the current unread count so we can restore it if the
	// background doMarkSeen fails (optimistic clear with rollback).
	oldUnread := 0
	if ps, ok := r.peers[peerAddress]; ok {
		oldUnread = ps.Unread
	}
	if changed {
		// Clear stale messages immediately so the UI never renders
		// the previous peer's conversation under the new header.
		r.activeMessages = nil
	} else if userClicked && !r.cache.MatchesPeer(addr) {
		// Same peer re-clicked but cache never loaded (previous load failed).
		// Treat as needing a fresh load. Only for explicit clicks —
		// programmatic re-selection of the same peer is a no-op.
		needLoad = true
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
			if !r.loadConversation(addr) {
				r.restorePeerUnread(peerAddress, oldUnread)
				return
			}
		}
		if !r.doMarkSeen(addr) {
			r.restorePeerUnread(peerAddress, oldUnread)
		}
		r.notify(UIEventMessagesUpdated)
	}()
}

func (r *DMRouter) SendMessage(to domain.PeerIdentity, body string) {
	r.setSendStatus("sending…")
	toStr := string(to)

	go func() {
		defer recoverLog("SendMessage")

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msg, err := r.client.SendDirectMessage(ctx, toStr, body)
		cancel()

		r.mu.Lock()
		if err != nil {
			r.sendStatus = "send failed: " + err.Error()
			r.mu.Unlock()
			r.notify(UIEventStatusUpdated)
			return
		}

		r.sendStatus = "message sent"
		r.pendingClearEditor = true

		if msg != nil && r.cache.MatchesPeer(toStr) {
			r.cache.AppendMessage(*msg)
			r.activeMessages = r.cache.Messages()
			r.pendingScrollToEnd = true
		}

		if msg != nil {
			r.ensurePeerLocked(to)
			r.peers[to].Preview = ConversationPreview{
				PeerAddress: toStr,
				Sender:      msg.Sender,
				Body:        msg.Body,
				Timestamp:   msg.Timestamp,
			}
			r.promotePeerLocked(to)
		}
		r.mu.Unlock()

		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
	}()
}

func (r *DMRouter) ActivePeer() domain.PeerIdentity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activePeer
}

func (r *DMRouter) MyAddress() string {
	return r.client.Address()
}

func (r *DMRouter) SetSendStatus(s string) {
	r.setSendStatus(s)
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
	id := string(identity)

	// Delete chat history from the local database.
	if _, err := r.client.DeletePeerHistory(identity); err != nil {
		log.Error().Str("identity", id).Err(err).Msg("failed to delete identity chat history")
		return false, fmt.Errorf("delete identity %s: %w", id, err)
	}

	// Best-effort: remove from the node trust store. The RPC requires a
	// live connection to the local node, which may be absent (e.g. during
	// shutdown or reconnect). Log the error but proceed with in-memory
	// cleanup — the sidebar no longer depends on the trust store.
	if err := r.client.DeleteContact(identity); err != nil {
		log.Warn().Str("identity", id).Err(err).Msg("trust store cleanup failed (best-effort)")
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

// Start launches three background goroutines:
// 1. Startup (initializeFromDB)
// 2. Event listener (handleEvent)
// 3. Health ticker (pollHealth)
func (r *DMRouter) Start() {
	// 1. Startup: load previews, auto-select first peer, run initial pollHealth.
	go r.runStartup()

	// 2. Event listener — drains the subscription channel immediately to
	//    prevent the node's emitLocalChange() from dropping events.
	events, cancel := r.client.SubscribeLocalChanges()
	go r.runEventListener(events, cancel)

	// 3. Periodic health ticker (supplements the eager call in initializeFromDB).
	//    Waits for startupDone before the first tick so that pollHealth
	//    (which calls repairUnreadFromHeaders) doesn't race with the
	//    buffered-event replay and double-count unread messages.
	go func() {
		<-r.startupDone
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			r.safePollHealth()
		}
	}()
}

// runStartup closes startupDone in defer so it fires even on panic —
// without this the event listener would stay blocked forever.
func (r *DMRouter) runStartup() {
	defer close(r.startupDone)
	defer recoverLog("initializeFromDB")
	r.initializeFromDB()
	r.notify(UIEventMessagesUpdated)
	r.notify(UIEventSidebarUpdated)
}

// runEventListener buffers events before startup completes to prevent the node's
// 16-slot buffer from overflowing. Events are buffered in a slice and replayed
// once startupDone closes, preventing resetIdentityState() from wiping
// already-applied event-driven updates.
func (r *DMRouter) runEventListener(events <-chan protocol.LocalChangeEvent, cancel func()) {
	defer cancel()

	// Cap buffer at maxStartupBuf to prevent unbounded memory growth
	// (each LocalChangeEvent may carry full encrypted Body).
	// Excess events are dropped; UI reload picks up missed messages from chatlog.
	const maxStartupBuf = 256
	var buf []protocol.LocalChangeEvent
	dropped := 0

	for {
		select {
		case <-r.startupDone:
			// Replay buffered events, then switch to live processing.
			r.replayAndListen(buf, dropped, events)
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if len(buf) < maxStartupBuf {
				buf = append(buf, event)
			} else {
				dropped++
			}
		}
	}
}

// replayAndListen replays buffered startup events and processes live events.
// Extracted from runEventListener so buffered slice goes out of scope for GC.
func (r *DMRouter) replayAndListen(buf []protocol.LocalChangeEvent, dropped int, events <-chan protocol.LocalChangeEvent) {
	// During replay, suppress Unread++ in updateSidebarFromEvent because
	// seedPreviews() already loaded the correct unread counts from SQL.
	// Without this, every buffered incoming-message event would double-count.
	r.mu.Lock()
	r.replayingStartup = true
	r.mu.Unlock()

	// Buffer live events that arrive during replay to prevent node-side
	// 16-slot buffer from overflowing. These events arrived AFTER seedPreviews()
	// SQL snapshot and must be processed with replayingStartup=false to trigger
	// Unread++ and UIEventBeep. Cap buffer to prevent unbounded memory growth.
	const maxReplayLiveBuf = 256
	var pendingLive []protocol.LocalChangeEvent
	droppedLive := 0

	for _, ev := range buf {
		r.safeHandleEvent(ev)
		// Drain pending live events into the buffer so the node-side
		// subscription channel (capacity 16) doesn't overflow while
		// we're busy replaying.
		pendingLive, droppedLive = r.bufferPendingLiveEvents(events, pendingLive, maxReplayLiveBuf, droppedLive)
	}
	if dropped > 0 {
		log.Warn().Int("dropped", dropped).Msg("startup buffer overflow: some events were dropped, UI will reload from chatlog")
		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
	}
	if droppedLive > 0 {
		log.Warn().Int("dropped", droppedLive).Msg("replay live buffer overflow: some live events were dropped, UI will reload from chatlog")
		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
	}

	// Reset replayingStartup BEFORE processing any live events.
	// Buffered startup events (buf) were already handled above under
	// replayingStartup=true — they are pre-snapshot and correctly
	// suppressed.  Everything below is post-snapshot and must be
	// treated as live.
	r.mu.Lock()
	r.replayingStartup = false
	r.mu.Unlock()

	// Process live events that were drained during replay — now with
	// replayingStartup=false so they correctly trigger Unread++ and
	// UIEventBeep.
	for _, ev := range pendingLive {
		r.safeHandleEvent(ev)
	}

	for event := range events {
		r.safeHandleEvent(event)
	}
}

// bufferPendingLiveEvents buffers queued events without processing them,
// preventing live events from being incorrectly suppressed under replayingStartup.
func (r *DMRouter) bufferPendingLiveEvents(events <-chan protocol.LocalChangeEvent, buf []protocol.LocalChangeEvent, maxBuf int, dropped int) ([]protocol.LocalChangeEvent, int) {
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return buf, dropped
			}
			if len(buf) < maxBuf {
				buf = append(buf, ev)
			} else {
				dropped++
			}
		default:
			return buf, dropped
		}
	}
}

func (r *DMRouter) safeHandleEvent(event protocol.LocalChangeEvent) {
	defer recoverLog("handleEvent")
	r.handleEvent(event)
}

func (r *DMRouter) safePollHealth() {
	defer recoverLog("pollHealth")
	r.pollHealth()
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
	}
}

func (r *DMRouter) peerForMessage(event protocol.LocalChangeEvent) domain.PeerIdentity {
	if event.Sender == r.client.Address() {
		return domain.PeerIdentity(event.Recipient)
	}
	return domain.PeerIdentity(event.Sender)
}

func (r *DMRouter) isActivePeer(peer domain.PeerIdentity) bool {
	r.mu.RLock()
	active := r.activePeer
	r.mu.RUnlock()
	return active == peer
}

func (r *DMRouter) onNewMessage(event protocol.LocalChangeEvent) {
	peerID := r.peerForMessage(event)
	peerAddr := string(peerID)

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
		r.updateSidebarFromEvent(event, peerID)
		r.notify(UIEventSidebarUpdated)
		// Sound notification for incoming messages (sender is not us).
		if event.Sender != r.client.Address() && !replaying {
			r.notify(UIEventBeep)
		}
		return
	}

	// Active peer, but cache may still be loading (peer just switched).
	if !r.cache.MatchesPeer(peerAddr) {
		// Cache not ready yet — trigger a full reload which will pick up
		// the new message. Also update sidebar preview so the user sees
		// the latest text immediately.
		r.updateSidebarFromEvent(event, peerID)
		if event.Sender != r.client.Address() && !replaying {
			r.notify(UIEventBeep)
		}
		go func() {
			defer recoverLog("onNewMessage.midSwitch")
			if !r.loadConversation(peerAddr) {
				return
			}
			r.notify(UIEventMessagesUpdated)
			// Active chat is on screen — always send seen receipts
			// now that the conversation has loaded.
			r.doMarkSeen(peerAddr)
		}()
		return
	}

	if r.cache.HasMessage(event.MessageID) {
		return
	}

	msg := r.client.DecryptIncomingMessage(event)
	if msg == nil {
		go func() {
			r.loadConversation(peerAddr)
			r.notify(UIEventMessagesUpdated)
		}()
		return
	}

	r.cache.AppendMessage(*msg)
	r.mu.Lock()
	r.activeMessages = r.cache.Messages()
	r.pendingScrollToEnd = true
	isIncoming := msg.Sender != r.client.Address()
	r.mu.Unlock()

	r.notify(UIEventMessagesUpdated)

	// Sound notification for every incoming message, regardless of which
	// chat is currently active. Suppressed during startup replay.
	if isIncoming && !replaying {
		r.notify(UIEventBeep)
	}

	// The active chat is on screen — mark incoming messages as seen
	// regardless of how the peer was selected (click or auto-select).
	if isIncoming {
		go r.doMarkSeen(peerAddr)
	}
}

func (r *DMRouter) onReceiptUpdate(event protocol.LocalChangeEvent) {
	receiptPeer := r.peerForMessage(event)
	receiptPeerStr := string(receiptPeer)

	if !r.isActivePeer(receiptPeer) {
		// Not the active peer — ignore; repair-path picks it up later.
		return
	}

	if !r.cache.MatchesPeer(receiptPeerStr) {
		// Active peer but cache still loading — trigger reload to pick up
		// the receipt update.
		go func() {
			r.loadConversation(receiptPeerStr)
			r.notify(UIEventMessagesUpdated)
		}()
		return
	}

	deliveredAt := event.DeliveredAt
	var deliveredPtr *time.Time
	if !deliveredAt.IsZero() {
		deliveredPtr = &deliveredAt
	}

	if r.cache.UpdateStatus(event.MessageID, event.Status, deliveredPtr) {
		r.mu.Lock()
		r.activeMessages = r.cache.Messages()
		r.mu.Unlock()
		r.notify(UIEventMessagesUpdated)
	} else if !r.cache.HasMessage(event.MessageID) {
		go func() {
			r.loadConversation(receiptPeerStr)
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
				firstPeer = domain.PeerIdentity(p.PeerAddress)
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
	r.pendingRecipientText = ""
	r.mu.Unlock()

	r.cache.Load("", nil)
}

func (r *DMRouter) pollHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	status := r.client.ProbeNode(ctx)
	cancel()

	r.repairUnreadFromHeaders(status)

	r.mu.RLock()
	activePeer := r.activePeer
	r.mu.RUnlock()
	if activePeer != "" {
		r.applyReceiptRepair(string(activePeer), status.DeliveryReceipts)
	}

	r.mu.Lock()
	r.nodeStatus = status
	r.mu.Unlock()

	r.notify(UIEventStatusUpdated)
}

func (r *DMRouter) loadConversation(peerAddress string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	messages, err := r.client.FetchConversation(ctx, peerAddress)
	cancel()

	if err != nil {
		log.Warn().Err(err).Str("peer", peerAddress).Msg("conversation load failed")
		return false
	}

	r.mu.RLock()
	current := strings.TrimSpace(string(r.activePeer))
	r.mu.RUnlock()
	if current != peerAddress {
		return false
	}

	r.cache.Load(peerAddress, messages)

	r.mu.Lock()
	if strings.TrimSpace(string(r.activePeer)) != peerAddress {
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
func (r *DMRouter) doMarkSeen(peerAddress string) bool {
	peerID := domain.PeerIdentity(peerAddress)
	r.mu.RLock()
	if r.activePeer != peerID {
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
		log.Warn().Err(err).Str("peer", peerAddress).Msg("MarkConversationSeen failed")
		return false
	}
	r.clearPeerUnread(peerID)
	r.notify(UIEventSidebarUpdated)
	return true
}

func (r *DMRouter) applyReceiptRepair(activePeer string, receipts []DeliveryReceipt) {
	if activePeer == "" || !r.cache.MatchesPeer(activePeer) {
		return
	}

	myAddr := r.client.Address()
	updated := false
	for _, rc := range receipts {
		var peer string
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

		var deliveredPtr *time.Time
		if !rc.DeliveredAt.IsZero() {
			deliveredPtr = &rc.DeliveredAt
		}
		if r.cache.UpdateStatus(rc.MessageID, rc.Status, deliveredPtr) {
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
		pid := domain.PeerIdentity(p.PeerAddress)
		r.ensurePeerLocked(pid)
		existing := r.peers[pid]
		// Skip if the event-path already delivered fresher data for this peer
		// (race: SubscribeLocalChanges runs in parallel with initializeFromDB).
		if !existing.Preview.Timestamp.IsZero() && !existing.Preview.Timestamp.Before(p.Timestamp) {
			fresherPeers[pid] = struct{}{}
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
		pid := domain.PeerIdentity(p.PeerAddress)
		if _, dup := seen[pid]; dup {
			continue
		}
		seen[pid] = struct{}{}
		if _, fresher := fresherPeers[pid]; fresher {
			continue
		}
		sqlApplied[pid] = struct{}{}
		sqlSorted = append(sqlSorted, pid)
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

func (r *DMRouter) updateSidebarFromEvent(event protocol.LocalChangeEvent, peerID domain.PeerIdentity) {
	msg := r.client.DecryptIncomingMessage(event)
	if msg == nil {
		return
	}

	isIncoming := msg.Sender != r.client.Address()

	r.mu.Lock()
	r.ensurePeerLocked(peerID)
	r.peers[peerID].Preview = ConversationPreview{
		PeerAddress: string(peerID),
		Sender:      msg.Sender,
		Body:        msg.Body,
		Timestamp:   msg.Timestamp,
	}
	if isIncoming && !r.replayingStartup {
		// Skip Unread++ during startup replay — seedPreviews() already set
		// the correct count from SQL. Incrementing here would double-count.
		r.peers[peerID].Unread++
	}
	r.promotePeerLocked(peerID)
	r.mu.Unlock()
}

func (r *DMRouter) refreshPreviewForPeer(peerAddress string) {
	defer recoverLog("refreshPreviewForPeer")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	preview, err := r.client.FetchSinglePreview(ctx, peerAddress)
	cancel()

	if err != nil {
		return
	}

	pid := domain.PeerIdentity(peerAddress)
	r.mu.Lock()
	if preview != nil {
		r.ensurePeerLocked(pid)
		r.peers[pid].Preview = *preview
	} else {
		delete(r.peers, pid)
		r.removePeerLocked(pid)
	}
	r.mu.Unlock()

	r.notify(UIEventSidebarUpdated)
}

func (r *DMRouter) repairUnreadFromHeaders(status NodeStatus) {
	me := r.client.Address()

	r.mu.Lock()
	selected := domain.PeerIdentity(strings.TrimSpace(string(r.activePeer)))

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

	hasNew := false
	needReload := false
	peersToRefresh := make(map[string]struct{})
	var reloadMessageIDs []string // IDs that triggered needReload for rollback on failure

	for _, header := range status.DMHeaders {
		if _, ok := r.seenMessageIDs[header.ID]; ok {
			continue
		}

		var peer domain.PeerIdentity
		if header.Sender == me {
			peer = domain.PeerIdentity(header.Recipient)
		} else if header.Recipient == me {
			peer = domain.PeerIdentity(header.Sender)
			r.ensurePeerLocked(peer)
			if !firstSync {
				hasNew = true
			}
		} else {
			continue
		}

		r.seenMessageIDs[header.ID] = struct{}{}

		senderID := domain.PeerIdentity(header.Sender)
		if peer != selected && header.Sender != me && header.Recipient == me {
			r.ensurePeerLocked(senderID)
			if !skipUnreadCount {
				r.peers[senderID].Unread++
				r.promotePeerLocked(senderID)
			}
		}

		if peer == selected && selected != "" && !r.cache.HasMessage(header.ID) {
			needReload = true
			reloadMessageIDs = append(reloadMessageIDs, header.ID)
		}

		if peer != "" {
			peersToRefresh[string(peer)] = struct{}{}
		}
	}
	r.mu.Unlock()

	for peer := range peersToRefresh {
		go r.refreshPreviewForPeer(peer)
	}

	if hasNew {
		r.notify(UIEventBeep)
	}

	selectedStr := string(selected)
	if needReload && selected != "" {
		if r.loadConversation(selectedStr) {
			r.notify(UIEventMessagesUpdated)
			// Active chat is on screen — always send seen receipts,
			// regardless of how the peer was selected.
			go r.doMarkSeen(selectedStr)
		} else {
			// Reload failed — the new messages are not in activeMessages.
			// Evict their IDs from seenMessageIDs so the next repair cycle
			// re-discovers them and retries the reload.
			r.mu.Lock()
			for _, id := range reloadMessageIDs {
				delete(r.seenMessageIDs, id)
			}
			r.mu.Unlock()
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
	peer = domain.PeerIdentity(strings.TrimSpace(string(peer)))
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

// notify sends a UIEvent without blocking. If the channel is full, a
// per-event background retry with exponential backoff (50ms → 100ms → 200ms)
// ensures the event is eventually delivered. Atomic counter caps concurrent
// retry goroutines at 8 to prevent accumulation during sustained bursts.
func (r *DMRouter) notify(eventType UIEventType) {
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
