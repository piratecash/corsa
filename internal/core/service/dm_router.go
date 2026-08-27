package service

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
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
	// LastIncomingAt is when this peer last wrote to us, as recorded in the
	// chatlog. Preview answers a different question — it is the last row of
	// the thread, which in an ordinary conversation is our own reply — so a
	// consumer looking for evidence about the PEER cannot use it. The UI
	// spends this as the weakest form of "last online": a message can only
	// have been written by a node that was running.
	//
	// Held next to Preview rather than inside it because the two have
	// different update rules: Preview is replaced wholesale on every new
	// message, while this one advances only on incoming ones and is
	// recomputed from SQL when messages are deleted.
	LastIncomingAt domain.OptionalTime
	Unread         int
	// PendingDeletes is how many of this peer's messages the user has
	// deleted here and the peer has not confirmed deleting yet. It is the
	// only lasting trace of a deletion in the UI — the row itself is gone
	// the moment the user asks — so without it a request handed to an
	// offline peer would be invisible until the day it completes.
	PendingDeletes int
	// PendingConversationDelete says the user cleared this chat and the peer
	// has not confirmed clearing theirs. Apart from the count because it is
	// a different sentence: a wipe is one request about the conversation, so
	// folding it in would report "1 message waiting" for a thread of a
	// thousand.
	PendingConversationDelete bool
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

	// DMGeneration counts rebuilds of the DM half only (Peers, PeerOrder,
	// ActiveMessages). Generation is the wrong gate for a cache derived
	// from those three: it also advances for resource samples and
	// peer-health deltas, 2-3 times a second, while this half is reused
	// byte-for-byte — a UI cache keyed on Generation therefore rebuilds
	// continuously over an unchanged conversation. Equal DMGeneration
	// guarantees the three collections are the SAME values (identical
	// backing arrays, not merely equal), so a consumer may keep anything
	// derived from them. It does not track the scalars (ActivePeer,
	// SendStatus, ...), which composeSnapshotLocked reads live.
	DMGeneration uint64
}

type DMRouter struct {
	client        *DesktopClient
	fileBridge    *FileTransferBridge
	eventBus      *ebus.Bus
	statusMonitor NodeStatusProvider

	// prepareAndSend performs the file_announce DM send inside
	// SendFileAnnounceFromComposer's goroutine. nil in production — the call
	// site falls back to fileBridge.PrepareAndSend. Overridable ONLY in tests,
	// so the stale-reply degrade (which strips ReplyTo before this call) can be
	// exercised without standing up a full node.
	prepareAndSend func(ctx context.Context, to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload) (*AnnounceResult, error)

	// Shutdown tracking for every router-owned goroutine that can touch
	// the chatlog: fire-and-forget sends/selects, the startup goroutine,
	// ebus-triggered reload goroutines and the long-lived retry loops.
	// beginOp/endOp register work with inflight under opMu; ShutdownDrain
	// closes the gate (opClosed) so no NEW work can register — closing
	// the gate and waiting under the same mutex is what keeps
	// sync.WaitGroup's "no Add concurrent with Wait" contract — then
	// cancels the retry loops (loopCancel) and drains. The desktop
	// shutdown path runs this before closing sqlite.
	// noStartupAutoSelect suppresses opening the first conversation
	// during startup (initializeFromDB). Desktop wants the auto-open (a
	// chat pane with nothing in it is dead space); the phone-sized
	// single-pane layout does not — there the first screen must be the
	// contact list, and auto-opening would also clear that
	// conversation's unread badge for a chat the user never looked at.
	// Negated on purpose so the zero value keeps the historical
	// behaviour for routers built as struct literals (tests). Set via
	// SetStartupAutoSelect before Start; read under mu.
	noStartupAutoSelect bool

	opMu        sync.RWMutex
	opClosed    bool
	sendsClosed bool
	inflight    sync.WaitGroup
	sendOps     sync.WaitGroup
	loopOps     sync.WaitGroup
	loopCtx     context.Context
	// opCtx outlives loopCtx. It is the context of repository work that no
	// caller supplied one for — UI actions and ebus handlers — and it is
	// cancelled only at the very end of ShutdownDrain, after the bus has
	// drained. Sharing loopCtx here was a bug: StopLoops cancels that one
	// while handlers are still running, so the terminal delete and recovery
	// writes those handlers make would fail with context canceled.
	opCtx      context.Context
	opCancel   context.CancelFunc
	loopCancel context.CancelFunc

	// presenceClock is the source of "now" for last-online evidence: a
	// message dated after it is refused as evidence, so the value has to be
	// injectable for a test to place one in the future without waiting for
	// one. An injected func rather than a Clock interface is the project
	// convention (routing.Table, node.Service, datagram.Config), and it
	// defaults to time.Now in the constructor — a nil check at every call
	// site is a default nobody can see.
	presenceClock func() time.Time

	// history is where the sidebar's chatlog-derived state is read from. It is
	// the chatlog store in production and is injected here so a test can
	// supply a reader that fails — a read that fails while its siblings
	// succeed, or once and then not, is what the all-or-nothing rule and the
	// startup retry exist for, and a working SQLite produces neither on
	// demand.
	history chatHistoryReader

	// removals is the shared removal gate — the same object the message
	// store consults, so the router's rule ("no row while a removal runs")
	// and the store's rule ("no write while a removal runs") can never
	// disagree. Owned by the client, which builds it before the node holds
	// the store adapter.
	removals *removalGate

	// fileOpMu serializes file-transfer registration against the cleanups.
	// The map is guarded by mu; the mutexes in it are not — they are held
	// across the file bridge, which mu never is.
	fileOpMu map[domain.PeerIdentity]*sync.Mutex

	// backwardsEpoch counts, per peer, how many times something moved this
	// peer's derived state BACKWARDS. It exists because idempotent merges
	// only order ADDITIONS against each other — two writers adding the same
	// message end up in the same place whatever the order, but a read taken
	// before a deletion and applied after it puts the deleted message back,
	// and no amount of "take the maximum" prevents that.
	//
	// The rule every chatlog-derived write follows: capture the epoch BEFORE
	// the query, apply only if it is unchanged. A changed epoch means the
	// answer describes a conversation that no longer exists in that form,
	// and the work has to be redone rather than merged.
	//
	// Two counters, because the two things that move backwards are not the
	// same thing. Marking a conversation read lowers the BADGE and removes
	// no rows, so it cannot make a last-incoming answer wrong; a deletion
	// removes rows and invalidates both. One counter for both would make
	// every launch invalidate its own history scan — the conversation that
	// opens automatically is marked read while the scan is still running.
	//
	// Guarded by mu.
	backwardsEpoch map[domain.PeerIdentity]peerEpochs

	// unreadIDs is the set of unseen incoming message ids per peer, and
	// RouterPeerState.Unread is its size. A SET rather than a counter because
	// the two things that feed it — a SQL read and the event stream — cannot
	// be ordered against each other: the database is ahead of the events, so
	// the same message appears in both. Adding an id twice changes nothing,
	// which makes the badge independent of who saw the message first.
	// Guarded by mu.
	unreadIDs map[domain.PeerIdentity]map[domain.MessageID]struct{}

	// pendingDeleteReconcile holds peers whose post-deletion reconciliation
	// did not land: a transient chatlog error, or an answer the event path
	// kept overtaking. Nothing else re-reads a peer's history, so without
	// this the sidebar would quote the deleted message until the next launch.
	// Drained by the delete sweep. Guarded by mu.
	pendingDeleteReconcile map[domain.PeerIdentity]int

	// peerRefreshMu serializes recomputation per peer. The revision alone
	// cannot order two readers that started together: both would see the
	// same revision, and the slower query — holding the older answer — would
	// land last. Two deletions of two messages in one conversation are
	// exactly that case, and the UI runs each in its own goroutine.
	// The map itself is guarded by mu; the mutexes inside it are held across
	// SQL I/O and must never be taken while mu is held.
	peerRefreshMu map[domain.PeerIdentity]*sync.Mutex

	mu             sync.RWMutex
	activePeer     domain.PeerIdentity
	peerClicked    bool
	peers          map[domain.PeerIdentity]*RouterPeerState
	peerOrder      []domain.PeerIdentity
	activeMessages []DirectMessage
	cache          *ConversationCache
	sendStatus     string
	seenMessageIDs map[string]struct{}
	peerGen        map[domain.PeerIdentity]uint64 // bumped by RemovePeer; goroutines compare to detect stale sends
	initialSynced  bool
	// replayingStartup is true while the startup buffer is being drained. It
	// suppresses the notification SOUND only: history is not news. The unread
	// badge is deliberately NOT suppressed — it is a set of ids, so a message
	// the startup read also reported costs nothing, while skipping the replay
	// loses every message stored after that read.
	replayingStartup bool

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

	// Cached snapshot halves. notify() refreshes ONLY the half whose
	// domain actually changed and recomposes RouterSnapshot from both,
	// so a status-only update (UIEventStatusUpdated — e.g. the 1s
	// resource sample, peer-health deltas, aggregate counters) never
	// re-copies the DM data (peers / activeMessages), and a DM update
	// (messages / sidebar) never re-deep-copies the NodeStatus
	// (PeerHealth slice + maps). Both halves are immutable once built;
	// composeSnapshotLocked shares their backing arrays across snapshots
	// (a refresh allocates a NEW half, leaving older snapshots' arrays
	// untouched), so the lock-free reader contract holds without the
	// previous full deep copy on every notify. Guarded by r.mu.
	cachedDMPart dmSnapshotPart
	cachedNS     NodeStatus

	// dmGen counts rebuilds of cachedDMPart and is surfaced as
	// RouterSnapshot.DMGeneration. It is bumped INSIDE buildDMPartLocked
	// rather than at the two assignment sites, because that function is the
	// only producer of a DM half: a new half therefore cannot reach
	// cachedDMPart without moving the counter. The remaining asymmetry is
	// the harmless one — a buildDMPartLocked result that is discarded would
	// bump without a content change, costing a consumer one redundant
	// rebuild, whereas a silent content change would let a consumer keep a
	// cache describing messages that no longer exist. Guarded by r.mu, like
	// the cache it describes.
	dmGen uint64

	uiEvents        chan UIEvent
	uiOverflowCount atomic.Int64  // number of active retry goroutines in notify()
	startupDone     chan struct{} // closed after runStartup completes; used by external waiters

	// convDeleteRetry holds the outgoing barrier of a wipe in progress:
	// one reservation per peer, latched synchronously by
	// BeginConversationDelete so a send cannot slip in between the click
	// and the wipe, released by CompleteConversationDelete — or by the
	// delete sweep, if the goroutine that owned it never came back. It
	// schedules nothing: the peer's half is N ordinary delete intents.
	// See dm_router_conversation_delete.go.
	convDeleteRetry *conversationDeleteRetryState

	// withdrawals are the delivery withdrawals a deletion could not
	// complete. Until one succeeds the node still holds the payload of a
	// deleted message, kept off the wire only by the freeze, and no later
	// deletion can name it because the row is gone. Retried by the delete
	// sweep. See delivery_withdrawal.go.
	withdrawals *withdrawalBacklog

	// recovery is the §4.10 decrypt-recovery subsystem
	// (dm_router_recovery.go). Immutable after NewDMRouter; its mutable
	// state sits behind the manager's own mutex and the chatlog.
	recovery *recoveryManager

	// wipeTombstones records message IDs the bulk wipe just removed
	// from chatlog so a delayed re-delivery of the same encrypted
	// envelope cannot resurrect a row the user thought they had wiped.
	// Entries expire on a TTL (wipeTombstoneTTL); the wipeTombstoneReaperLoop
	// goroutine launched from Start() prunes stale entries.
	wipeTombstones *wipeTombstoneSet
	// beforeDropDeleteIntentForTest runs between the ack handler's read of a
	// request and its attempt to retire it. Production leaves it nil; a test
	// installs it to occupy that window, which is the only way to reach the
	// state where the row is retired by somebody else while an answer for it is
	// in flight.
	beforeDropDeleteIntentForTest func()

	// deleteCheckpoint coalesces the WAL truncations that follow
	// deletions. A thread wipe reaches this node as one message_delete
	// per message, and a truncation per row would run for hours on a
	// long thread; see delete_checkpoint.go.
	deleteCheckpoint *deleteCheckpointer

	// dispatchControlDeleteFn is a test-only override for the
	// dispatchMessageDelete wire path. When non-nil, dispatchMessageDelete
	// invokes this function instead of building a payload and calling
	// r.client.SendControlMessage. Production code leaves this nil and
	// runs the real dispatch. Tests that need to count dispatches or
	// avoid the rpc/identity stack assign a counter here.
	dispatchControlDeleteFn func(ctx context.Context, peer domain.PeerIdentity, target domain.MessageID) error

	// peerReachableFn is a test-only override for the reachability
	// lookup behind peerReachable. Production code leaves it nil and
	// reads the status monitor's snapshot; tests that need to place a
	// peer offline without standing up the status monitor assign a
	// predicate here.
	peerReachableFn func(peer domain.PeerIdentity) bool

	// dispatchControlConversationDeleteFn is a test-only override for
	// the dispatchConversationDelete wire path. Mirrors
	// dispatchControlDeleteFn, for the wipe path.
	// Production code leaves this nil and runs the real dispatch.
	dispatchControlConversationDeleteFn func(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error

	// dispatchControlConversationDeleteAckFn is the same seam for the ANSWER.
	// A test that drives a request through the real inbound handler needs to
	// read what went back without standing up the rpc/identity stack — and the
	// answer is half of the contract, so a test that could not see it would be
	// testing the easy half.
	dispatchControlConversationDeleteAckFn func(ctx context.Context, peer domain.PeerIdentity, ack domain.ConversationDeleteAckPayload) error

	// Pending UI widget actions (Gio widgets are NOT thread-safe).
	pendingScrollToEnd     bool
	pendingComposerRestore []ComposerRestore
	pendingRecipientText   domain.PeerIdentity
}

// routerSnapshotCache holds a pre-built snapshot and the generation at which
// it was captured. Only the UI goroutine reads and writes the atomic pointer,
// so no additional synchronization is needed between snapshot consumers.
type routerSnapshotCache struct {
	gen  uint64
	snap RouterSnapshot
}

// dmSnapshotPart caches the EXPENSIVE DMRouter-owned collections of a
// RouterSnapshot — the sidebar peers map, the peer order, and the active
// conversation messages. These change only on DM-data events (messages /
// sidebar), so status-only notifies (the frequent ones: 1s resource
// sample, 500ms peer-health deltas) reuse them without re-cloning. The
// cheap scalar fields (ActivePeer, PeerClicked, SendStatus, MyAddress)
// are NOT cached here — composeSnapshotLocked reads them live under
// r.mu, so a status notify that flips only sendStatus is reflected
// without touching the collections. Every reference field is freshly
// cloned on refresh (including a per-entry *RouterPeerState clone),
// making the value immutable and safe to share across snapshots.
type dmSnapshotPart struct {
	Peers          map[domain.PeerIdentity]*RouterPeerState
	PeerOrder      []domain.PeerIdentity
	ActiveMessages []DirectMessage
}

// ComposerRestore carries an unsent message back to the UI after a send
// failed, so the desktop can surface it as a retriable "not sent" entry.
type ComposerRestore struct {
	Peer    domain.PeerIdentity
	Body    string
	ReplyTo domain.MessageID
	// Epoch is OutgoingDM.ComposerEpoch echoed back verbatim: the UI's per-peer
	// forget-epoch captured when the composer dispatched this send. The UI drops
	// the restore if the peer's epoch has since advanced (contact removed).
	Epoch uint64
}

// PendingActions holds deferred widget mutations that must be applied
// on the UI goroutine (Gio widgets are NOT thread-safe).
type PendingActions struct {
	ScrollToEnd     bool
	ComposerRestore []ComposerRestore
	RecipientText   domain.PeerIdentity
}

func NewDMRouter(client *DesktopClient, fileBridge *FileTransferBridge, eventBus *ebus.Bus, statusMonitor NodeStatusProvider) *DMRouter {
	r := &DMRouter{
		client:          client,
		fileBridge:      fileBridge,
		eventBus:        eventBus,
		statusMonitor:   statusMonitor,
		peers:           make(map[domain.PeerIdentity]*RouterPeerState),
		peerOrder:       make([]domain.PeerIdentity, 0),
		seenMessageIDs:  make(map[string]struct{}),
		peerGen:         make(map[domain.PeerIdentity]uint64),
		backwardsEpoch:  make(map[domain.PeerIdentity]peerEpochs),
		fileOpMu:        make(map[domain.PeerIdentity]*sync.Mutex),
		cache:           NewConversationCache(),
		uiEvents:        make(chan UIEvent, 32),
		startupDone:     make(chan struct{}),
		presenceClock:   time.Now,
		convDeleteRetry: newConversationDeleteRetryState(),
		withdrawals:     newWithdrawalBacklog(),
	}
	// Resolved lazily: the chatlog store outlives the router but is not
	// necessarily open yet here, and a nil captured now would quietly
	// turn every answered wipe back into in-memory-only state.
	// The refusal set is the client's: it guards the door into the
	// chatlog, and it is loaded before the node starts. The router reads
	// and writes the same set.
	if client != nil {
		r.wipeTombstones = client.wipeTombstones
		r.removals = client.removals
	}
	r.deleteCheckpoint = newDeleteCheckpointer(
		func() *chatlog.Store {
			if r.client == nil || r.client.chatlog == nil {
				return nil
			}
			return r.client.chatlog.Store()
		},
		r.opContext,
	)
	// The §4.10 decrypt-recovery subsystem: the manager owns the durable
	// jobs, and every DMCrypto decrypt path reports through the hook. Set
	// before Start(), so no decrypt can race the wiring.
	r.recovery = newRecoveryManager(r)
	if client != nil && client.dm != nil {
		client.dm.onDecryptFailure = r.recovery.Report
		client.dm.onDecryptSuccess = r.recovery.noteDecryptedIncoming
		client.dm.onSendSuccess = r.recovery.noteOutgoingSent
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
	snap.CacheReady = !snap.ActivePeer.IsZero() && r.cache.MatchesPeer(snap.ActivePeer)
	return snap
}

func (r *DMRouter) ConsumePendingActions() PendingActions {
	r.mu.Lock()
	pa := PendingActions{
		ScrollToEnd:     r.pendingScrollToEnd,
		ComposerRestore: r.pendingComposerRestore,
		RecipientText:   r.pendingRecipientText,
	}
	r.pendingScrollToEnd = false
	r.pendingComposerRestore = nil
	r.pendingRecipientText = domain.PeerIdentity{}
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

// SetStartupAutoSelect enables or disables opening the first
// conversation during startup (initializeFromDB). Call before Start.
// Disabled by the desktop app on Android, whose single-pane layout must
// come up on the contact list — see noStartupAutoSelect.
func (r *DMRouter) SetStartupAutoSelect(enabled bool) {
	r.mu.Lock()
	r.noStartupAutoSelect = !enabled
	r.mu.Unlock()
}

// DeselectPeer clears the active conversation without selecting another
// one — the compact (single-pane) layout calls it when navigating back
// from an open chat to the contact list. After it returns no chat is on
// screen, so peerClicked is reset: incoming messages must accumulate
// unread badges again instead of being auto-marked seen for a
// conversation nobody is looking at. No-op when nothing is selected.
func (r *DMRouter) DeselectPeer() {
	r.mu.Lock()
	if r.activePeer.IsZero() {
		r.mu.Unlock()
		return
	}
	r.activePeer = domain.PeerIdentity{}
	r.peerClicked = false
	// Clear the stale conversation immediately so a later frame never
	// renders the previous peer's messages without its header — same
	// rule as the peer-switch branch in selectPeerCore.
	r.activeMessages = nil
	r.mu.Unlock()

	r.notify(UIEventMessagesUpdated)
	r.notify(UIEventSidebarUpdated)
}

// ErrRouterShuttingDown is returned by send entry points once
// ShutdownDrain has closed the operation gate: the process is exiting
// and sqlite is about to close, so accepting new sends would either
// lose them or write to a closed database.
var ErrRouterShuttingDown = errors.New("dm_router: shutting down")

// beginOp registers a chatlog-touching operation with the shutdown
// tracker. It returns false once ShutdownDrain has closed the gate — the
// caller must skip the operation entirely (the app is exiting; sqlite is
// about to close). The Add happens under opMu so it can never interleave
// with ShutdownDrain's close-then-Wait sequence.
func (r *DMRouter) beginOp() bool {
	r.opMu.RLock()
	defer r.opMu.RUnlock()
	if r.opClosed {
		return false
	}
	r.inflight.Add(1)
	return true
}

// endOp releases a slot taken by beginOp.
func (r *DMRouter) endOp() { r.inflight.Done() }

// beginSendOp is beginOp for OUTBOUND sends (text / file announce).
// Sends have their own earlier gate (sendsClosed) and counter (sendOps)
// because the shutdown sequence drains them while the node is still up —
// a send drained after node cancellation could only fail. Registered in
// both counters so the final ShutdownDrain covers them too.
func (r *DMRouter) beginSendOp() bool {
	r.opMu.RLock()
	defer r.opMu.RUnlock()
	if r.opClosed || r.sendsClosed {
		return false
	}
	r.inflight.Add(1)
	r.sendOps.Add(1)
	return true
}

// endSendOp releases a slot taken by beginSendOp.
func (r *DMRouter) endSendOp() {
	r.sendOps.Done()
	r.inflight.Done()
}

// DrainSends closes the send gate (new SendMessage / file announces are
// refused with ErrRouterShuttingDown) and waits — bounded — for sends
// already in flight to finish. Called while the node is STILL RUNNING so
// those sends can reach the wire; the rest of the router keeps working
// until ShutdownDrain.
func (r *DMRouter) DrainSends(timeout time.Duration) bool {
	r.opMu.Lock()
	r.sendsClosed = true
	r.opMu.Unlock()

	done := make(chan struct{})
	go func() {
		r.sendOps.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// StopLoops cancels the long-lived retry/reaper loops and waits —
// bounded — for them to exit. Runs BEFORE the event-bus drain in the
// shutdown sequence: the delete/conversation retry loops publish
// terminal outcomes to the bus, so cancelling them only later (in
// ShutdownDrain, after the bus is gone) would silently drop those
// events. Does not touch the operation gates — the router keeps
// accepting handler work until ShutdownDrain.
func (r *DMRouter) StopLoops(timeout time.Duration) bool {
	if r.loopCancel != nil {
		r.loopCancel()
	}
	done := make(chan struct{})
	go func() {
		r.loopOps.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ShutdownDrain stops the router for process exit: it closes the
// operation gate (no new tracked work can start), cancels the long-lived
// retry/reaper loops, and waits — bounded by timeout — for everything
// already in flight to finish. Reports whether the drain completed.
// Part of the shutdown ordering documented in desktop.Run: UI-side
// sends → router drain → ebus shutdown → node stop → chatlog close.
func (r *DMRouter) ShutdownDrain(timeout time.Duration) bool {
	r.opMu.Lock()
	r.opClosed = true
	r.opMu.Unlock()

	if r.loopCancel != nil {
		r.loopCancel()
	}

	done := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(done)
	}()
	drained := true
	select {
	case <-done:
	case <-time.After(timeout):
		drained = false
	}

	// AFTER the drain, not before it. A deletion finishing inside the drain
	// asks for a checkpoint, and a checkpointer stopped first refuses that
	// request outright — so the pages of the last message the user deleted
	// stay legible in the write-ahead log, on a path that reported the
	// deletion as done. Stopping here means every request the drain produced
	// is either already run or owed, and stop() runs what is owed
	// synchronously before the composition root closes the database.
	r.deleteCheckpoint.stop()

	// Repository work stops LAST: everything above may still have been
	// writing a terminal outcome, and cancelling their context earlier is
	// what would lose it. On a timed-out drain it is cancelled too — the
	// stragglers are no longer allowed to reach a database the composition
	// root is about to release.
	if r.opCancel != nil {
		r.opCancel()
	}
	return drained
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

	// Snapshot the unread set so it can be restored if the background
	// doMarkSeen fails (optimistic clear with rollback).
	oldUnread := r.unreadSnapshotLocked(peerAddress)
	// And the backwards counters as they are BEFORE the optimistic clear
	// below, so the load that follows is judged against the conversation the
	// user asked for, not against the state this selection is about to
	// create.
	epochAtSelect := r.backwardsEpoch[peerAddress]
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
	if !r.beginOp() {
		return
	}
	go func() {
		defer r.endOp()
		defer recoverLog(label)
		if needLoad {
			if !r.loadConversation(peerAddress, epochAtSelect) {
				r.restorePeerUnread(peerAddress, oldUnread)
				_ = r.repairBadgeFromStore(peerAddress)
				return
			}
		}
		if !r.doMarkSeen(peerAddress) {
			r.restorePeerUnread(peerAddress, oldUnread)
			_ = r.repairBadgeFromStore(peerAddress)
		}
		r.notify(UIEventMessagesUpdated)
	}()
}

// SendMessage queues a text DM to the peer. Returns
// ErrConversationDeleteInflight when an in-progress wipe
// is pending for this peer — the caller (UI/RPC) maps it to a
// "wipe in progress" hint / 503 instead of letting the new outgoing
// message reach the peer's chatlog after the peer's wipe ran (which
// would leave the row gone on the receiver and present on the sender
// after the eventual ack).
//
// acquireSendIfNoPending atomically checks the barrier AND increments
// the in-flight counter under one mutex hold. The counter is released
// in the send goroutine (defer) so CompleteConversationDelete's drain
// wait sees the slot disappear once the send actually finishes.
// Without the atomic acquire, a separate has() check could observe
// "no pending" and the send goroutine could land in chatlog AFTER
// BeginConversationDelete reserved the barrier, expanding scope past
// the thread the sender wiped.
func (r *DMRouter) SendMessage(to domain.PeerIdentity, msg domain.OutgoingDM) error {
	to = normalizePeer(to)

	if !r.convDeleteRetry.acquireSendIfNoPending(to) {
		return ErrConversationDeleteInflight
	}

	r.mu.RLock()
	gen := r.peerGen[to]
	r.mu.RUnlock()

	r.setSendStatusNotify("sending…")

	if !r.beginSendOp() {
		r.convDeleteRetry.releaseSend(to)
		return ErrRouterShuttingDown
	}
	go func() {
		defer r.endSendOp()
		defer recoverLog("SendMessage")
		defer r.convDeleteRetry.releaseSend(to)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		sent, err := r.client.SendDirectMessage(ctx, to, msg)
		cancel()

		// The quoted message can vanish between composing and sending:
		// the peer deletes it (single message_delete or a full
		// conversation wipe) while the reply is still sitting in the
		// editor. The UI drops the stale quote as soon as the deletion
		// reaches its snapshot (Window.dropStaleReply), but a send
		// dispatched inside that same window still carries the dead
		// reference and fails reply-reference validation. Degrade to a
		// plain send instead of surfacing "send failed" for a message
		// the user can no longer even see — matching what they would
		// have gotten had they pressed send a frame later.
		//
		// The sentinel only proves the reference is ABSENT, not that it
		// was deleted (see ErrReplyToNotFound). Reading absence as
		// deleted-mid-compose is sound HERE because this router is the
		// composer entry point: ReplyTo provenance is a message bubble
		// the user clicked in this very conversation, so a never-valid
		// or cross-conversation ID can only mean a UI bug — logged at
		// warn below so it cannot hide, but not worth failing the
		// user's message over. Direct DesktopClient/SDK callers bypass
		// this path and still get the validation error verbatim. A
		// chatlog lookup FAILURE is a distinct error (not the
		// sentinel), so a broken DB keeps failing loudly here too.
		//
		// msg itself is stripped (not a copy) so everything downstream
		// of the retry — the TopicMessageSent publish included —
		// describes the message actually sent, without a phantom
		// ReplyTo.
		if err != nil && msg.ReplyTo != "" && errors.Is(err, ErrReplyToNotFound) {
			log.Warn().
				Str("peer", to.String()).
				Str("reply_to", string(msg.ReplyTo)).
				Msg("dm_router: reply_to absent at send time (deleted while composing?), resending without quote")
			msg.ReplyTo = ""
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			sent, err = r.client.SendDirectMessage(ctx, to, msg)
			cancel()
		}

		// The §4.10 established fact fires inside SendDirectMessage via the
		// onSendSuccess chokepoint — every send surface shares it, so no
		// per-caller marking here.
		r.mu.Lock()
		if r.peerGen[to] != gen {
			// Peer was removed while the send was in flight. Do not
			// recreate the sidebar entry from stale async work.
			r.mu.Unlock()
			log.Info().
				Str("peer", to.String()).
				Msg("dm_router: peer removed during in-flight send, discarding result")
			return
		}

		if err != nil {
			// The wording is NOT decided here. A deferred store is not a
			// failed send — "wait a moment" and "this will never work"
			// are different things to tell a user — but which sentence
			// says so, and in which language, belongs to the UI: the
			// error travels on TopicMessageSendFailed and the desktop
			// subscriber replaces this line with a localised one. This
			// stays as the fallback for a runtime with no UI attached.
			r.sendStatus = "send failed: " + err.Error()
			// Hand the unsent text back to the composer to restore for retry
			// (it cleared synchronously at send). Only for composer-originated
			// sends: RPC sends have no composer and headless runtimes never
			// drain PendingActions, so appending there would leak unbounded.
			if msg.FromComposer {
				r.pendingComposerRestore = append(r.pendingComposerRestore, ComposerRestore{Peer: to, Body: msg.Body, ReplyTo: msg.ReplyTo, Epoch: msg.ComposerEpoch})
			}
			r.mu.Unlock()
			r.notify(UIEventStatusUpdated)
			ebus.PublishMessageSendFailed(r.eventBus, ebus.MessageSendFailedResult{
				To:  to,
				Err: err,
			})
			return
		}

		r.sendStatus = "message sent"

		if sent != nil && r.cache.MatchesPeer(to) {
			r.cache.AppendMessage(*sent)
			r.activeMessages = r.cache.Messages()
			r.pendingScrollToEnd = true
		}

		if sent != nil {
			r.setPeerPreviewLocked(to, *sent)
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
	return nil
}

// SendFileAnnounce sends a file_announce DM and registers the sender-side
// file mapping. File transfer orchestration (prepare → send → commit/rollback)
// is delegated to FileTransferBridge; DMRouter handles only the peerGen
// stale-send guard and UI state updates.
func (r *DMRouter) SendFileAnnounce(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure func()) error {
	return r.sendFileAnnounceWithBaseline(to, msg, meta, onAsyncFailure, nil, nil)
}

// SendFileAnnounceFromComposer is like SendFileAnnounce but pins the removal
// generation the caller captured BEFORE its own slow local work (the desktop
// file import). Guarding against that earlier baseline closes the TOCTOU where
// a contact deleted between the caller's own pre-check and this call would be
// measured against a freshly captured (already bumped) generation and slip
// through, re-importing the identity and re-adding it to the sidebar.
func (r *DMRouter) SendFileAnnounceFromComposer(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure func(), baselineGen uint64) error {
	return r.sendFileAnnounceWithBaseline(to, msg, meta, onAsyncFailure, nil, &baselineGen)
}

// SendFileAnnounceFromComposerDone is SendFileAnnounceFromComposer with an
// additional onAsyncSuccess callback, fired after the announce is fully
// settled (chatlog written, TopicFileSent published). Exactly one of
// onAsyncFailure / onAsyncSuccess runs per call that returned nil; a
// non-nil sync return fires neither. The desktop uses the success hook to
// release the picker staging copy of the sent file — the failure path must
// NOT release it because the retry queue re-reads the source path.
func (r *DMRouter) SendFileAnnounceFromComposerDone(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure, onAsyncSuccess func(), baselineGen uint64) error {
	return r.sendFileAnnounceWithBaseline(to, msg, meta, onAsyncFailure, onAsyncSuccess, &baselineGen)
}

// sendFileAnnounceWithBaseline is the shared body. baseline, when non-nil, is
// the removal generation to guard against (captured by the caller before its
// own slow work); when nil the current generation is captured here.
func (r *DMRouter) sendFileAnnounceWithBaseline(to domain.PeerIdentity, msg domain.OutgoingDM, meta domain.FileAnnouncePayload, onAsyncFailure, onAsyncSuccess func(), baseline *uint64) error {
	if r.fileBridge == nil {
		return fmt.Errorf("file transfer not available")
	}

	to = normalizePeer(to)

	// Same atomic acquire pattern as SendMessage: barrier check +
	// inflight increment under one mutex hold. PrepareAndSend below
	// runs the heavyweight transfer setup which may stretch into
	// seconds; releasing inflight only when the goroutine returns
	// keeps CompleteConversationDelete's drain wait honest about
	// when this send is actually settled.
	if !r.convDeleteRetry.acquireSendIfNoPending(to) {
		return ErrConversationDeleteInflight
	}

	var gen uint64
	if baseline != nil {
		gen = *baseline
	} else {
		r.mu.RLock()
		gen = r.peerGen[to]
		r.mu.RUnlock()
	}

	r.setSendStatusNotify("sending…")

	if !r.beginSendOp() {
		r.convDeleteRetry.releaseSend(to)
		return ErrRouterShuttingDown
	}
	go func() {
		defer r.endSendOp()
		defer recoverLog("SendFileAnnounce")
		defer r.convDeleteRetry.releaseSend(to)

		// Guard BEFORE PrepareAndSend: it performs the actual DM send + chatlog
		// write, which RollbackMapping cannot undo. If the peer was already
		// removed (deleted during the caller's local import, or between the
		// caller's pre-check and here), abandon WITHOUT sending. Same status +
		// ebus ordering as the failure paths below.
		r.mu.RLock()
		stale := r.peerGen[to] != gen
		r.mu.RUnlock()
		if stale {
			// The desktop already imported the file (prepareFileForTransmit) and
			// PrepareAndSend — which owns the normal cleanup — will not run, so
			// remove the now-unreferenced blob here. The caller saw a nil return,
			// so its synchronous-error cleanup path never fires either.
			r.client.RemoveUnreferencedTransmitFile(meta.FileHash)
			r.setSendStatusNotify("file announce cancelled: peer removed")
			ebus.PublishFileSendFailed(r.eventBus, ebus.FileSendFailedResult{
				To:  to,
				Err: fmt.Errorf("peer removed before file announce"),
			})
			if onAsyncFailure != nil {
				onAsyncFailure()
			}
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()

		// Degrade a stale reply reference BEFORE consuming the transmit blob.
		// Unlike the text send (SendMessage), PrepareAndSend cannot be retried
		// in place on ErrReplyToNotFound: its failed attempt runs token.Rollback,
		// which deletes the now-unreferenced blob, so a second call would fail
		// with "file deleted" instead. Every desktop retry re-imports the file
		// but reuses the same stale ReplyTo, so without this pre-check a file
		// whose quoted message was deleted would fail on every retry forever.
		// Strip the quote only when the chatlog DEFINITIVELY reports it absent;
		// a lookup error (broken DB) leaves the quote intact and lets the send
		// fail loudly, matching SendMessage's degrade contract.
		if msg.ReplyTo != "" {
			if gw := r.client.ChatlogGateway(); gw != nil {
				if store := gw.Store(); store != nil {
					if found, lookErr := store.LookupEntryInConversation(r.opContext(), to, domain.MessageID(msg.ReplyTo)); lookErr == nil && !found {
						log.Warn().
							Str("peer", to.String()).
							Str("reply_to", string(msg.ReplyTo)).
							Msg("dm_router: reply_to absent at file announce (deleted while composing?), sending without quote")
						msg.ReplyTo = ""
					}
				}
			}
		}

		sendFn := r.fileBridge.PrepareAndSend
		if r.prepareAndSend != nil {
			sendFn = r.prepareAndSend
		}
		result, err := sendFn(ctx, to, msg, meta)
		if err != nil {
			// Order matters: status + ebus event MUST be published
			// before the callback unblocks the caller. Tests (and any
			// real consumer) treat the callback as the "async failure
			// is settled" signal and immediately read r.sendStatus —
			// firing the callback first would race the status write
			// and surface a stale "sending…" snapshot.
			r.setSendStatusNotify("file announce failed: " + err.Error())
			ebus.PublishFileSendFailed(r.eventBus, ebus.FileSendFailedResult{
				To:  to,
				Err: err,
			})
			if onAsyncFailure != nil {
				onAsyncFailure()
			}
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
			// Same ordering invariant as the err != nil branch above:
			// status + ebus event publish before the callback so a
			// caller waiting on the callback sees the final visible
			// state.
			r.setSendStatusNotify("file announce cancelled: peer removed")
			ebus.PublishFileSendFailed(r.eventBus, ebus.FileSendFailedResult{
				To:     to,
				FileID: result.FileID,
				Err:    fmt.Errorf("peer removed during in-flight file announce"),
			})
			if onAsyncFailure != nil {
				onAsyncFailure()
			}
			log.Info().
				Str("peer", to.String()).
				Str("file_id", string(result.FileID)).
				Msg("dm_router: peer removed after file commit, removed orphaned sender mapping")
			return
		}
		r.sendStatus = "message sent"

		if r.cache.MatchesPeer(to) {
			r.cache.AppendMessage(*result.Sent)
			r.activeMessages = r.cache.Messages()
			r.pendingScrollToEnd = true
		}

		r.setPeerPreviewLocked(to, *result.Sent)
		r.promotePeerLocked(to)
		r.mu.Unlock()

		r.notify(UIEventMessagesUpdated)
		r.notify(UIEventSidebarUpdated)
		ebus.PublishFileSent(r.eventBus, ebus.FileSentResult{
			To:     to,
			FileID: result.FileID,
		})
		// Success is settled (same ordering rule as the failure paths:
		// all visible state published before the callback).
		if onAsyncSuccess != nil {
			onAsyncSuccess()
		}
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
// TopicFileReceived is published ONLY when the receiver-side
// registration succeeded. The contract on subscribers is "a new (or
// re-registered) row appears in AllTransfersSnapshot"; emitting on
// registration failure (malformed payload, no localNode in
// standalone-RPC mode, manager-side validation reject) would
// announce a mapping that never exists and the file tab would
// invalidate for nothing.
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
	if err := r.fileBridge.RegisterIncoming(*msg); err != nil {
		// Bridge has already logged the cause; nothing to publish.
		// AllTransfersSnapshot has no new row to surface.
		return
	}
	ebus.PublishFileReceived(r.eventBus, ebus.FileReceivedResult{
		From:   msg.Sender,
		FileID: domain.FileID(msg.ID),
	})
}

func (r *DMRouter) ActivePeer() domain.PeerIdentity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activePeer
}

func (r *DMRouter) MyAddress() domain.PeerIdentity {
	return r.client.Address()
}

// BuildContactLink renders this node's own corsa: contact link (§4.8).
func (r *DMRouter) BuildContactLink() (string, error) {
	return r.client.BuildContactLink()
}

// ResolveIdentity starts (or joins) the on-demand key lookup for target and
// returns its resolution id. Never blocks on the network: progress arrives
// via ebus.TopicIdentityResolutionChanged.
func (r *DMRouter) ResolveIdentity(target domain.PeerIdentity) (string, error) {
	reply, err := r.client.rpc.LocalRequestFrame(protocol.Frame{Type: "resolve_identity", Address: target.String()})
	if err != nil {
		return "", err
	}
	if reply.Type == "error" || reply.Resolution == nil {
		return "", fmt.Errorf("resolve_identity: %s", reply.Error)
	}
	return reply.Resolution.ResolutionID, nil
}

// ImportContactLink verifies and imports a pasted corsa: link, returning
// the imported identity.
func (r *DMRouter) ImportContactLink(raw string) (domain.PeerIdentity, error) {
	return r.client.ImportContactLink(r.opContext(), raw)
}

func (r *DMRouter) SetSendStatus(s string) {
	r.setSendStatusNotify(s)
}

// SetSendStatusIfCurrent atomically replaces sendStatus with replacement
// IFF the current value still equals expected. Returns true on success
// (UIEventStatusUpdated emitted), false otherwise. Used by the
// wipe goroutine to write "wipe request sent"
// without overwriting a terminal status that a fast peer ACK pushed
// through the ebus subscriber first.
func (r *DMRouter) SetSendStatusIfCurrent(expected, replacement string) bool {
	r.mu.Lock()
	if r.sendStatus != expected {
		r.mu.Unlock()
		return false
	}
	r.sendStatus = replacement
	r.mu.Unlock()
	r.notify(UIEventStatusUpdated)
	return true
}

// NotifyStatusChanged is called by NodeStatusMonitor when network state
// changes. It rebuilds the snapshot and emits UIEventStatusUpdated so
// the UI picks up the new NodeStatus.
func (r *DMRouter) NotifyStatusChanged() {
	r.notify(UIEventStatusUpdated)
}

// ErrHistorySweepFailed marks a removal that took the contact out of the
// sidebar, the trust store and the conversation cache, but could not prove
// that its history left the disk with it.
//
// It exists because the two failures a removal can report are not the same
// failure. The first history delete fails BEFORE anything is touched, and
// the contact is still there — the caller must leave its own state alone.
// The final sweep fails at the very end, when the in-memory state is already
// gone: the caller has to finish its own cleanup (drafts, aliases,
// selection) and tell the user that the history may have survived. Callers
// separate the two with errors.Is.
var ErrHistorySweepFailed = errors.New("the history sweep after removing a contact failed")

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
// one (so the caller can decide what to select next). It is meaningful on
// the ErrHistorySweepFailed path too: by then the contact IS out of the
// sidebar, and the caller still has to pick what to show instead.
func (r *DMRouter) RemovePeer(identity domain.PeerIdentity) (bool, error) {
	identity = normalizePeer(identity)
	id := identity.String()

	// The gate goes up FIRST, before this function waits for anything —
	// the history delete below is disk I/O, the file barrier after it is a
	// lock, and a gate raised later is open for exactly the length of those
	// waits. That window is what a second removal, or a message the node is
	// about to store, walks through.
	// The send queue is stopped FIRST, for the same reason the gate is raised
	// before anything is waited for. The gate stops writes and re-offers; it
	// does not reach the queue, which by then may already hold RESOLVED facts of
	// this conversation — and a pass that read them before the history delete
	// would hand its frame over after it, with the forgetting below only waiting
	// for a frame that has already gone. Stopping it after begin() would leave
	// it open for exactly the length of that wait.
	//
	// Released once the queue is empty; the defer is a net for the error return
	// in between, and the release is idempotent.
	resumeReactions := r.client.HoldReactionSends(identity)
	defer resumeReactions()

	releaseGate := r.removals.begin(identity)
	defer releaseGate()

	// Delete chat history from the local database. This is the gate
	// operation: if it fails, the peer is NOT removed and peerGen stays
	// unchanged so in-flight goroutines remain valid.
	if _, err := r.client.DeletePeerHistory(r.opContext(), identity); err != nil {
		log.Error().Str("identity", id).Err(err).Msg("failed to delete identity chat history")
		return false, fmt.Errorf("delete identity %s: %w", id, err)
	}

	// Everything this conversation left outside the database goes with the
	// history, under the same gate — see ForgetContactState for what that
	// is and why it cannot wait until the gate comes down.
	r.client.ForgetContactState(identity)
	// The queue is empty now, so nothing of this conversation can reach the
	// wire any more and the pause has nothing left to protect.
	resumeReactions()

	// Bump generation BEFORE any best-effort cleanup. In-flight goroutines
	// (SendMessage, SendFileAnnounce) that captured gen before this point
	// will see a stale generation and discard their results. Without this
	// ordering, a slow CleanupPeer could leave a window where in-flight
	// sends slip through the peerGen guard.
	// The version move, the transfer cleanup and the drop of the in-memory
	// state are ONE section under the file barrier. Split apart, a message
	// that took its stamp before the bump would wait for the barrier and
	// then register a mapping AFTER the last cleanup ran — a transfer for a
	// contact the user deleted, outliving the deletion. Inside the section
	// the bump comes first, so whoever waited finds both a new generation
	// and, by the end, no row at all.
	fileLock := r.fileOpLock(identity)
	fileLock.Lock()

	r.mu.Lock()
	r.peerGen[identity]++
	// Removal takes the whole conversation, so both counters move: peerGen
	// alone is checked only by the paths that know to look for it.
	r.moveHistoryBackwardsLocked(identity)
	r.mu.Unlock()

	// Best-effort: clean up file transfer mappings and associated files
	// (transmit refs, downloaded files, partial downloads).
	if r.fileBridge != nil {
		r.fileBridge.CleanupPeer(identity)
	}

	r.mu.Lock()
	delete(r.peers, identity)
	delete(r.unreadIDs, identity)
	delete(r.peerRefreshMu, identity)
	delete(r.pendingDeleteReconcile, identity)
	r.removePeerLocked(identity)
	r.cache.Evict(identity)

	wasActive := r.activePeer == identity
	if wasActive {
		r.activePeer = domain.PeerIdentity{}
		r.peerClicked = false
		r.activeMessages = nil
	}

	r.mu.Unlock()
	// The barrier comes down only now: everything a waiting registration
	// would have to see — the new generation, the cleaned transfers, the
	// missing row — is in place.
	fileLock.Unlock()

	// Best-effort: remove from the node trust store. The RPC requires a
	// live connection to the local node, which may be absent (e.g. during
	// shutdown or reconnect). Log the error but proceed — the sidebar no
	// longer depends on the trust store. Outside the file barrier (it is an
	// RPC, and no transfer waits on it) but still inside the removal
	// window, so nothing re-creates the conversation underneath it.
	if err := r.client.DeleteContact(identity); err != nil {
		log.Warn().Str("identity", id).Err(err).Msg("trust store cleanup failed (best-effort)")
	}

	// One last sweep of the history before the gate lifts. The store defers
	// writes for a conversation being removed, so nothing new should be
	// here — but a write already inside the chatlog when the gate went up
	// is not covered by that, and a row left behind is what the next
	// startup would rebuild the conversation from.
	//
	// A sweep that FAILS is reported, not logged and forgotten: the gate is
	// about to lift, and telling the user the contact is gone while its
	// history is still on disk is the one answer this function must not
	// give.
	_, sweepErr := r.client.DeletePeerHistory(r.opContext(), identity)

	// The in-memory state is gone either way, so the UI is told either way;
	// what the error changes is the ANSWER the caller gets, not the redraw.
	r.notify(UIEventSidebarUpdated)
	if wasActive {
		r.notify(UIEventMessagesUpdated)
	}

	if sweepErr != nil {
		log.Error().Str("identity", id).Err(sweepErr).Msg("dm_router: the sweep after removing the contact failed; its history may still be on disk")
		return wasActive, fmt.Errorf("delete identity %s: %w: %w", id, ErrHistorySweepFailed, sweepErr)
	}

	return wasActive, nil
}

// Start launches background goroutines and subscribes to DM-specific
// ebus events (messages, receipts). Network-layer events (peer health,
// aggregate status, contacts, identities) are handled by the
// NodeStatusMonitor — DMRouter does not subscribe to them.
// opContext is the context for repository work that no caller supplied one
// for: UI actions and ebus event handlers, which have no request of their own.
//
// It is deliberately NOT loopCtx. StopLoops cancels the loops early in the
// shutdown order — before the event bus drains — and handlers still running at
// that point publish terminal delete and recovery outcomes that must reach the
// database. opCtx is cancelled at the end of ShutdownDrain instead, once the
// bus is drained and nothing can start new work.
//
// Background is the fallback for a router built directly in a test and never
// started; a nil context panics inside database/sql.
func (r *DMRouter) opContext() context.Context {
	if r.opCtx != nil {
		return r.opCtx
	}
	return context.Background()
}

func (r *DMRouter) Start() {
	// Cancellable context for the long-lived loops below; ShutdownDrain
	// cancels it so the loops exit and release their inflight slots.
	r.loopCtx, r.loopCancel = context.WithCancel(context.Background())
	r.opCtx, r.opCancel = context.WithCancel(context.Background())

	// One checkpoint at startup, on the retrying path. storage.Open truncates
	// the log before handing the database over, but a busy reader can defeat it
	// there and it only WARNS on an ordinary open — refusing to start over a
	// checkpoint would be an outage of our own making. What the log may hold at
	// that moment is the previous run's deletions, so somebody has to keep
	// asking, and this is the component that does.
	//
	// Asked HERE and not in the constructor. The checkpointer runs on a timer
	// and reads opCtx when it fires; arming it before this line is a read of a
	// field Start is about to write. It also gave a router that was built and
	// never started — every fixture that only needs the struct — a background
	// timer that kept retrying against a database the test had already closed.
	r.deleteCheckpoint.request()

	// 1. Subscribe to DM-specific ebus events.
	r.subscribeEvents()

	// 2. Startup: load previews, auto-select first peer, seed the monitor.
	// Tracked: it reads and writes through the chatlog (previews,
	// mark-seen via AutoSelectPeer).
	if r.beginOp() {
		go func() {
			defer r.endOp()
			r.runStartup()
		}()
	}

	// 3. Background sweeper for the durable delete intents: whatever
	// peers still owe us a deletion, including requests scheduled by a
	// process that has since been restarted. Runs for the process
	// lifetime; see dm_router_delete.go.
	if r.beginOp() {
		r.loopOps.Add(1)
		go func() {
			defer r.loopOps.Done()
			defer r.endOp()
			r.deleteRetryLoop(r.loopCtx)
		}()
	}

	// 3b. The §4.10 decrypt-recovery scheduler: durable jobs live in the
	// chatlog, so a restart resumes notice retries instead of restarting
	// lookups. Same lifetime as the delete sweepers.
	if r.recovery != nil && r.beginOp() {
		r.loopOps.Add(1)
		go func() {
			defer r.loopOps.Done()
			defer r.endOp()
			r.recovery.run(r.loopCtx)
		}()
	}
	// Resolution completions feed the proof gate of both recovery legs; an
	// authoritative result also unblocks queued sender-side re-sends.
	if r.recovery != nil && r.eventBus != nil {
		r.eventBus.Subscribe(ebus.TopicIdentityResolutionChanged, func(state ebus.IdentityResolutionState) {
			r.recovery.noteResolution(state)
		})
	}

	// 4. Wipe-tombstone reaper: prunes expired entries from the
	// in-memory tombstone set so a long-lived process does not
	// accumulate tombstones for the rest of its lifetime.
	if r.beginOp() {
		r.loopOps.Add(1)
		go func() {
			defer r.loopOps.Done()
			defer r.endOp()
			r.wipeTombstoneReaperLoop(r.loopCtx)
		}()
	}
}

// runStartup loads initial data from the database and then replays any
// message/receipt events that arrived via ebus during initialization.
// replayingStartup marks that replay, and now suppresses only the beep: the
// badge is a set, so a message counted by both the startup read and its own
// replayed event is one unread message, while suppressing the replay would
// lose the ones stored after the read was taken.
func (r *DMRouter) runStartup() {
	defer close(r.startupDone)
	defer recoverLog("initializeFromDB")
	r.initializeFromDB()

	// Deletions the user asked for in an earlier run are still owed by
	// their peers; the counts have to be on screen from the first frame,
	// not only after the next one settles.
	r.refreshPendingDeleteCounts()

	// Phase 1: replay events buffered during initializeFromDB under
	// replayingStartup=true — history, so no beeps.
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
	// replay are drained as live, which for them means the beep is no longer
	// suppressed; the badge never was.
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

	// The badge needs no reconciliation here. It is a SET of message ids: the
	// startup read and the replayed events both contribute to it, the same id
	// from both changes nothing, and a message stored between the two is
	// added by whichever of them saw it.

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

	// A peer completing a handshake is the cheapest reliable "they are
	// back" signal there is, and it is what un-parks the deletions they
	// still owe us. Without it a request would wait out the sweep's
	// parking interval instead of going out on the next tick; the
	// interval remains the ceiling for a peer that becomes routable
	// without connecting to us directly.
	r.eventBus.Subscribe(ebus.TopicPeerConnected, func(_ domain.PeerAddress, identity domain.PeerIdentity) {
		r.reviveDeleteIntentsForPeer(identity)
	})
}

func (r *DMRouter) handleEvent(event protocol.LocalChangeEvent) {
	switch event.Type {
	case protocol.LocalChangeNewMessage:
		if event.Topic != "dm" {
			return
		}
		// Tombstone gate runs BEFORE onNewMessage so a replayed
		// envelope for a recently-wiped id is re-DELETED and its
		// UI path (decrypt + sidebar bubble + notification) is
		// short-circuited. evictWipedConversationFromUI clears
		// seenMessageIDs at wipe time, so the dedup gate alone
		// would not catch this — the explicit tombstone check is
		// load-bearing for the late-relay-replay window. Without
		// it the row reappears in the active conversation even
		// though the user has already deleted it. The guard is a
		// no-op when the id is not tombstoned, so the cost on the
		// fast path is one map lookup.
		if r.suppressIfWipeTombstoned(event) {
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
	return domain.PeerIdentityFromWire(strings.TrimSpace(p.String()))
}

func (r *DMRouter) peerForMessage(event protocol.LocalChangeEvent) domain.PeerIdentity {
	if event.Sender == r.client.Address().String() {
		return normalizePeer(domain.PeerIdentityFromWire(event.Recipient))
	}
	return normalizePeer(domain.PeerIdentityFromWire(event.Sender))
}

func (r *DMRouter) isActivePeer(peer domain.PeerIdentity) bool {
	r.mu.RLock()
	active := r.activePeer
	r.mu.RUnlock()
	return active == peer
}

// PeerGeneration returns the current removal generation for a peer. It is
// bumped by RemovePeer, so a caller can capture it before a slow local
// operation (e.g. a desktop file import) and re-check afterwards: a changed
// value means the contact was deleted in the meantime and the operation must
// be abandoned rather than resurrecting the contact.
func (r *DMRouter) PeerGeneration(peer domain.PeerIdentity) uint64 {
	peer = normalizePeer(peer)
	r.mu.RLock()
	gen := r.peerGen[peer]
	r.mu.RUnlock()
	return gen
}

func (r *DMRouter) onNewMessage(event protocol.LocalChangeEvent) {
	peerID := r.peerForMessage(event)

	// Register this message so the repair-path (repairUnreadFromHeaders)
	// won't double-count it as a new unread or trigger a duplicate beep.
	//
	// The lifecycle generation is captured in the same breath, because
	// everything below it is slow: decrypting a message is an RPC, and a
	// contact removed while it runs must not be recreated by the sidebar row
	// this would otherwise write for it. A conversation new to this process
	// has generation zero on both sides and is created normally.
	r.mu.Lock()
	if event.MessageID != "" {
		r.seenMessageIDs[event.MessageID] = struct{}{}
	}
	stampAtEvent := peerStamp{gen: r.peerGen[peerID], epochs: r.backwardsEpoch[peerID]}
	r.mu.Unlock()

	// During startup replay, suppress beep — these are old messages
	// already counted by seedPreviews().
	r.mu.RLock()
	replaying := r.replayingStartup
	r.mu.RUnlock()

	if !r.isActivePeer(peerID) {
		// Definitely not the active conversation — update sidebar only.
		if !r.updateSidebarFromEvent(event, peerID, stampAtEvent) {
			// Inline decrypt failed (missing contact keys, etc.) —
			// fall back to reading the latest preview from SQLite.
			isIncoming := event.Sender != r.client.Address().String()
			// The conversation may be new to this process, so the row is
			// created HERE — synchronously with the message that introduces
			// it, and before the goroutine is queued. Doing it inside the
			// goroutine would race a removal that completes first: the
			// goroutine would recreate the row and then find its own fresh
			// generation perfectly consistent.
			r.ensurePeerForReconcile(peerID, stampAtEvent.gen)
			if !r.beginOp() {
				return
			}
			go func() {
				defer r.endOp()
				defer recoverLog("onNewMessage.nonActive.decryptFail")
				switch r.reconcilePeerFromStore(r.opContext(), peerID, false, true) {
				case reconcileApplied:
					// The sidebar now matches the chatlog; fall through to
					// the badge, which this path owns.
				case reconcilePeerGone:
					// The user removed the contact while this was queued.
					// Marking it unread would put the conversation back.
					return
				default:
					// Nothing was reconciled — a failed read, a contended
					// peer, or no history at all. Reopen the dedup gate so
					// the repair path can rediscover the message.
					r.evictSeenMessages(event.MessageID)
					return
				}
				r.mu.Lock()
				if _, alive := r.peers[peerID]; !alive {
					r.mu.Unlock()
					return
				}
				// The badge is event-driven on this path: the
				// reconciliation above deliberately leaves Unread
				// alone, so this is the only record the message gets.
				// Startup replay is NOT suppressed — the id is a set
				// member, so the snapshot reporting it too costs
				// nothing, while skipping it loses every message
				// stored after the snapshot was taken.
				//
				// Re-check: if the user opened this chat while the
				// goroutine was running, SelectPeer already cleared
				// unread. Marking now would re-add the badge on an
				// already-visible conversation.
				if isIncoming && r.activePeer != peerID {
					r.markUnreadLocked(peerID, domain.MessageID(event.MessageID))
				}
				r.promotePeerLocked(peerID)
				r.mu.Unlock()
				r.notify(UIEventSidebarUpdated)
			}()
		} else {
			r.notify(UIEventSidebarUpdated)
		}
		// Sound notification for incoming messages (sender is not us).
		if event.Sender != r.client.Address().String() && !replaying {
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
		msg := r.client.DecryptIncomingMessage(r.opContext(), event)
		if msg != nil {
			decryptedMsg = msg
			r.mu.Lock()
			// The decrypt is an RPC; the contact may be gone by now, and
			// setPeerPreviewLocked would create its row again.
			outcome := r.applyIncomingMessageLocked(peerID, *msg, stampAtEvent)
			r.mu.Unlock()
			switch outcome {
			case applyApplied:
				// The file mapping goes through the same guard, and only
				// after it: registering first would put a transfer back for
				// a contact whose transfers were just cleaned up.
				r.registerFileReceiveForLivePeer(msg, peerID, stampAtEvent)
				r.notify(UIEventSidebarUpdated)
			case applyStale:
				// Something in this conversation was deleted meanwhile; the
				// database knows what survived, this goroutine does not.
				if !r.recoverFromStaleApply(peerID, msg) {
					r.evictSeenMessages(event.MessageID)
				}
			case applyPeerGone:
			}
		}
		if event.Sender != r.client.Address().String() && !replaying {
			r.notify(UIEventBeep)
		}
		// Before the goroutine, not inside it: a removal that completes
		// first must not be undone by a row the queued work creates for
		// itself.
		r.ensurePeerForReconcile(peerID, stampAtEvent.gen)
		if !r.beginOp() {
			return
		}
		go func() {
			defer r.endOp()
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
					// The generation as well as the selection: a contact
					// removed and added back is the same peer by name, and
					// seeding this message into the new conversation would
					// show a message from the old one.
					seeded := r.activePeer == peerID &&
						r.stampIsCurrentLocked(peerID, stampAtEvent)
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

	msg := r.client.DecryptIncomingMessage(r.opContext(), event)
	if msg == nil {
		isIncoming := event.Sender != r.client.Address().String()
		if isIncoming && !replaying {
			r.notify(UIEventBeep)
		}
		r.ensurePeerForReconcile(peerID, stampAtEvent.gen)
		if !r.beginOp() {
			return
		}
		go func() {
			defer r.endOp()
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

	// The §4.10 receiver-side qualifying work (established fact, retry_of
	// acceptance) fired inside DecryptIncomingMessage via the
	// onDecryptSuccess chokepoint — every decrypt path shares it, so no
	// per-branch handling here.

	isIncoming := msg.Sender != r.client.Address()
	if !r.deliverDecryptedMessage(msg, peerID, stampAtEvent) {
		// The conversation moved off screen while the message was being
		// decrypted; it has been handled as a background arrival, badge
		// included.
		r.notify(UIEventSidebarUpdated)
		if isIncoming && !replaying {
			r.notify(UIEventBeep)
		}
		return
	}

	r.notify(UIEventMessagesUpdated)
	r.notify(UIEventSidebarUpdated)

	// Sound notification for every incoming message, regardless of which
	// chat is currently active. Suppressed during startup replay.
	if isIncoming && !replaying {
		r.notify(UIEventBeep)
	}

	// The active chat is on screen — mark incoming messages as seen
	// regardless of how the peer was selected (click or auto-select).
	if isIncoming && r.beginOp() {
		go func() {
			defer r.endOp()
			r.doMarkSeen(peerID)
		}()
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
		if !r.beginOp() {
			return
		}
		go func() {
			defer r.endOp()
			r.loadConversation(receiptPeer, r.peerEpochsOf(receiptPeer))
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
		if !r.beginOp() {
			return
		}
		go func() {
			defer r.endOp()
			r.loadConversation(receiptPeer, r.peerEpochsOf(receiptPeer))
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
	// Captured before the fetch for the same reason every other chatlog read
	// captures it: the retry loop can spend seconds here, and the answer must
	// not create a row for a contact removed in the meantime. The sidebar is
	// normally empty at this point, which is why this was the last read
	// without a guard — and "normally" is not an invariant.
	var previewsBefore map[domain.PeerIdentity]peerEpochs
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		previewsBefore = r.backwardsEpochSnapshot()
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
	if len(previews) != 0 {
		r.seedPreviews(previews, previewsBefore)
	}

	// Both history reads happen off the startup path. Startup holds the ebus
	// buffer open until it finishes, and that buffer DROPS its overflow at
	// 256 events, so a slow database must not be allowed to lengthen it.
	// Both merge idempotently, so they can land whenever they land.
	//
	// Started even when the preview fetch came back empty: the badge scan is
	// the only thing that reads delivery_status for a conversation with no
	// live header traffic, so skipping it would leave a quiet conversation's
	// unread messages badgeless for the session. (The header path no longer
	// depends on it — it reads the stored statuses itself — but it only ever
	// sees the messages the node still holds in memory.)
	if r.beginOp() {
		go func() {
			defer r.endOp()
			defer recoverLog("seedHistoryEvidence")
			r.seedHistoryEvidence(r.opContext())
		}()
	}

	if len(previews) == 0 {
		return
	}

	me := r.client.Address()
	var firstPeer domain.PeerIdentity
	r.mu.RLock()
	if len(r.peerOrder) > 0 {
		firstPeer = r.peerOrder[0]
	}
	r.mu.RUnlock()

	if firstPeer.IsZero() {
		for _, p := range previews {
			if p.PeerAddress != me && !p.PeerAddress.IsZero() {
				firstPeer = p.PeerAddress
				break
			}
		}
	}
	if firstPeer.IsZero() {
		return
	}

	r.mu.Lock()
	var selectedPeer domain.PeerIdentity
	switch {
	case strings.TrimSpace(r.activePeer.String()) != "":
		selectedPeer = r.activePeer
	case r.noStartupAutoSelect:
		// Nothing was open and this build must not open anything (phone
		// layout): leave the selection empty so the UI shows the contact
		// list, and do not mark any conversation seen.
		r.mu.Unlock()
		return
	default:
		selectedPeer = firstPeer
		r.pendingRecipientText = firstPeer
	}
	// Clear activePeer so AutoSelectPeer always sees a peer switch
	// and triggers a full load. Without this, a reconnect (activePeer
	// already set) would skip loadConversation because selectPeerCore
	// treats same-peer + programmatic selection as a no-op.
	r.activePeer = domain.PeerIdentity{}
	r.mu.Unlock()

	// Delegate to AutoSelectPeer which handles the full lifecycle:
	// set activePeer, peerClicked=true, optimistic unread clear,
	// loadConversation, doMarkSeen, and rollback on failure.
	r.AutoSelectPeer(selectedPeer)
	// pollHealth() is called via defer at function start.
}

func (r *DMRouter) resetIdentityState() {
	r.mu.Lock()
	// Every chatlog read still in flight belongs to the previous identity.
	// Bumped BEFORE the sidebar is wiped, and for every peer that had one —
	// including the ones that never moved backwards and so have no entry
	// yet, whose epoch would otherwise still read zero and let a stale
	// answer look current. The map itself is deliberately never cleared: an
	// epoch that starts over is an epoch that proves nothing.
	for peer, epochs := range r.backwardsEpoch {
		epochs.unread++
		epochs.history++
		r.backwardsEpoch[peer] = epochs
	}
	for peer := range r.peers {
		if _, tracked := r.backwardsEpoch[peer]; !tracked {
			r.moveHistoryBackwardsLocked(peer)
		}
	}
	r.peers = make(map[domain.PeerIdentity]*RouterPeerState)
	r.unreadIDs = nil
	r.peerRefreshMu = nil
	r.pendingDeleteReconcile = nil
	r.peerOrder = nil
	r.activePeer = domain.PeerIdentity{}
	r.peerClicked = false
	r.activeMessages = nil
	r.seenMessageIDs = make(map[string]struct{})
	r.initialSynced = false
	r.sendStatus = ""
	r.pendingScrollToEnd = false
	r.pendingComposerRestore = nil
	r.pendingRecipientText = domain.PeerIdentity{}
	r.mu.Unlock()

	// Clear the monitor so the next FetchAndSeed seeds fresh data from
	// ProbeNode instead of preserving stale state from a previous session.
	if r.statusMonitor != nil {
		r.statusMonitor.Reset()
	}

	r.cache.Load(domain.PeerIdentity{}, nil)
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

	if !activePeer.IsZero() {
		r.applyReceiptRepair(activePeer, status.DeliveryReceipts)
	}

	r.notify(UIEventStatusUpdated)
}

// epochBefore is the peer's backwards-move counters as they were BEFORE the
// caller began the work this load belongs to. It is a parameter rather than
// something read here because the caller's own slow steps count: by the time
// this function starts, a deletion may already have happened, and reading the
// counters now would compare the answer against a state that already includes
// it.
func (r *DMRouter) loadConversation(peerAddress domain.PeerIdentity, epochBefore peerEpochs) bool {
	// The generation as it is now, the counters as the caller captured them
	// before its own slow steps.
	stamp := r.peerStampOf(peerAddress)
	stamp.epochs = epochBefore

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	messages, err := r.client.FetchConversation(ctx, peerAddress)
	cancel()

	if err != nil {
		log.Warn().Err(err).Str("peer", peerAddress.String()).Msg("conversation load failed")
		return false
	}

	// One check, in one place, BEFORE anything is applied — the load and
	// the side effects both hang off it. Checking early and applying later
	// is what let a deletion land in between: the snapshot was discarded
	// but the transfers it named had already been registered again.
	r.mu.Lock()
	if r.activePeer != peerAddress || r.backwardsEpoch[peerAddress].history != epochBefore.history {
		// Either the user moved on, or a row left this conversation while
		// the three-second read ran — the eviction that removed it from the
		// cache has already happened, and this answer predates it. The
		// caller treats false as "try again".
		r.mu.Unlock()
		return false
	}
	r.cache.Load(peerAddress, messages)
	r.activeMessages = r.cache.Messages()
	r.pendingScrollToEnd = true
	r.mu.Unlock()

	// Register receiver-side mappings for the file announcements in what was
	// just loaded — transfer state has to survive a restart. After the
	// check, never before it: a contact removed while the fetch ran has had
	// its transfers cleaned up, and re-registering them here would put back
	// exactly what the removal took away.
	myAddr := r.client.Address()
	for i := range messages {
		if messages[i].Command == domain.DMCommandFileAnnounce && messages[i].Sender != myAddr {
			r.registerFileReceiveForLivePeer(&messages[i], peerAddress, stamp)
		}
	}
	return true
}

// alreadyReadHeaderIDs answers, for the incoming headers of a first sync,
// which messages the DATABASE calls read.
//
// The header carries no delivery status, and the node's in-memory topic
// outlives a desktop session: attach a UI to a running node and the first
// poll offers back every message of the previous session. Those are the ones
// this suppresses. Everything else — unread in the database, or not in the
// database at all — is the header's to badge, so this path needs no other
// read to finish first. That independence is the point: an earlier version
// deferred to the startup badge seed, and a seed that never ran (or never
// succeeded) left every stored message badgeless for the session.
//
// A failed read suppresses nothing, so every header decides for itself: a
// badge too many is cleared by opening the conversation, a badge lost is not.
func (r *DMRouter) alreadyReadHeaderIDs(ctx context.Context, headers []DMHeader, me domain.PeerIdentity, seen map[string]struct{}) map[domain.MessageID]struct{} {
	reader := r.chatHistory()
	if reader == nil {
		return nil
	}

	candidates := make([]domain.MessageID, 0, len(headers))
	for _, header := range headers {
		if _, already := seen[header.ID]; already {
			continue
		}
		if header.Recipient != me || header.Sender == me {
			continue
		}
		candidates = append(candidates, domain.MessageID(header.ID))
	}
	if len(candidates) == 0 {
		return nil
	}

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	statuses, err := reader.StoredMessageStatuses(readCtx, candidates)
	if err != nil {
		log.Warn().Err(err).Int("headers", len(candidates)).Msg("first sync: read state unknown, headers badge on their own")
		return nil
	}

	read := make(map[domain.MessageID]struct{}, len(statuses))
	for id, status := range statuses {
		if status == chatlog.StatusSeen {
			read[id] = struct{}{}
		}
	}
	return read
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
		log.Warn().Err(err).Str("peer", peerAddress.String()).Msg("MarkConversationSeen failed")
		return false
	}

	// Only the ids that were actually sent are cleared. The RPC took time,
	// and a message that arrived while it ran was never in the batch — its
	// receipt was never sent, so it is still unread.
	seen := make([]domain.MessageID, 0, len(msgs))
	for i := range msgs {
		seen = append(seen, domain.MessageID(msgs[i].ID))
	}
	r.mu.Lock()
	r.dropUnreadLocked(peerAddress, seen...)
	r.mu.Unlock()

	r.notify(UIEventSidebarUpdated)
	return true
}

func (r *DMRouter) applyReceiptRepair(activePeer domain.PeerIdentity, receipts []DeliveryReceipt) {
	if activePeer.IsZero() || !r.cache.MatchesPeer(activePeer) {
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

// historyReadAttempts bounds the startup retries. These reads are the only
// ones of their kind on the startup path — nothing else re-reads
// delivery_status, and nothing else gives an existing conversation its
// last-online line — so a contact who never writes again would otherwise go
// the whole session without either. Three attempts cover a database still
// settling after launch.
const historyReadAttempts = 3

// historyReadRetryDelay is the linear step between those attempts.
const historyReadRetryDelay = time.Second

// historyReadTimeout bounds one attempt.
const historyReadTimeout = 5 * time.Second

// seedHistoryEvidence runs both history reads and publishes the result.
//
// Publishing is the part that is easy to forget: Snapshot() serves a cache
// that only notify() rebuilds, so what these two write stays invisible until
// some unrelated event notifies. On the retry path there is no unrelated
// event to wait for — startup's own notify fired long ago, and a contact who
// is about to write again is not the one this retried for.
func (r *DMRouter) seedHistoryEvidence(ctx context.Context) {
	changed := r.seedUnreadIDs(ctx)
	if r.seedLastIncoming(ctx) {
		changed = true
	}
	if changed {
		r.notify(UIEventSidebarUpdated)
	}
}

// backwardsEpochSnapshot captures every peer's counters in one pass, for a
// read that answers about many conversations at once. Taken immediately
// before the query it belongs to — one snapshot shared by two reads would
// make the second one refuse everything the first one's wait allowed to
// change.
func (r *DMRouter) backwardsEpochSnapshot() map[domain.PeerIdentity]peerEpochs {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[domain.PeerIdentity]peerEpochs, len(r.peers)+len(r.backwardsEpoch))
	for peer := range r.peers {
		out[peer] = r.backwardsEpoch[peer]
	}
	// Also the peers with no row: a removed conversation keeps its counters
	// precisely so a read that predates the removal cannot bring it back, and
	// snapshotting zero for it would hand that read a matching baseline.
	for peer, epochs := range r.backwardsEpoch {
		out[peer] = epochs
	}
	return out
}

// readHistoryWithRetry runs one history read until it answers, giving up
// after historyReadAttempts. name identifies the read in the logs and in the
// warning that ends the last attempt.
func readHistoryWithRetry[T any](ctx context.Context, name string, read func(context.Context) (T, error)) (T, bool) {
	var zero T
	for attempt := 1; attempt <= historyReadAttempts; attempt++ {
		readCtx, cancel := context.WithTimeout(ctx, historyReadTimeout)
		result, err := read(readCtx)
		cancel()
		if err == nil {
			return result, true
		}
		if attempt == historyReadAttempts {
			log.Warn().Err(err).Int("attempts", attempt).Str("read", name).Msg("dm_router: chat history unavailable at startup")
			return zero, false
		}
		log.Warn().Err(err).Int("attempt", attempt).Str("read", name).Msg("dm_router: chat history read failed, retrying")

		select {
		case <-ctx.Done():
			return zero, false
		case <-time.After(time.Duration(attempt) * historyReadRetryDelay):
		}
	}
	return zero, false
}

// seedLastIncoming recomputes, for every sidebar peer, the newest message
// that peer wrote, and holds it in memory.
//
// It is deliberately recomputed rather than persisted. The value is derived
// from the chatlog — messages arrive, messages get deleted — so a durable
// copy would be a second thing to keep in step with the first, needing a
// version to order its writers that a sidebar label does not justify.
// Reading it back costs one scan of a covering index at startup, in a
// background goroutine.
//
// The scan MERGES rather than assigns: it runs while events are already
// being handled, and the value it carries can only be older than what an
// event wrote, so taking the maximum is both correct and order-independent.
// The bool reports whether any peer changed, so the caller can publish a
// snapshot: nothing else re-reads this, and the UI shows only what notify
// composed.
func (r *DMRouter) seedLastIncoming(ctx context.Context) bool {
	reader := r.chatHistory()
	if reader == nil {
		return false
	}
	// Captured inside the closure so it belongs to the attempt that answers,
	// not to the first one: the retries span up to eighteen seconds, and a
	// snapshot older than the read would refuse peers that moved during a
	// wait this read already survived.
	var before map[domain.PeerIdentity]peerEpochs
	result, ok := readHistoryWithRetry(ctx, "last_incoming", func(ctx context.Context) (map[domain.PeerIdentity]time.Time, error) {
		before = r.backwardsEpochSnapshot()
		return reader.LastIncomingAtPerPeer(ctx, r.now())
	})
	if !ok {
		// The sidebar keeps whatever presence data the node has. A contact
		// who writes again repairs their own row; one who does not is the
		// reason this retried at all.
		return false
	}
	return r.applyScannedLastIncoming(result, before)
}

// chatHistory returns the chatlog surface these reads go through: the
// injected one when a test supplied it, otherwise the store itself. nil means
// there is no history to read — a client without persistence.
func (r *DMRouter) chatHistory() chatHistoryReader {
	if r.history != nil {
		return r.history
	}
	if r.client == nil || r.client.chatlog == nil {
		return nil
	}
	store := r.client.chatlog.Store()
	if store == nil {
		return nil
	}
	return store
}

// seedUnreadIDs loads the unseen incoming ids from the chatlog into the
// per-peer sets. It is a union, not an assignment: events replayed from the
// startup buffer may already have added some of these ids, and adding them
// again is what makes the two sources safe to combine in any order.
//
// It retries like the scan beside it rather than settling for one attempt:
// for a conversation with no live header traffic it is the only thing that
// reads delivery_status at all.
//
// The bool reports whether any badge changed, so the caller can publish a
// snapshot.
func (r *DMRouter) seedUnreadIDs(ctx context.Context) bool {
	reader := r.chatHistory()
	if reader == nil {
		return false
	}
	var before map[domain.PeerIdentity]peerEpochs
	unseen, ok := readHistoryWithRetry(ctx, "unseen_incoming", func(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error) {
		before = r.backwardsEpochSnapshot()
		return reader.UnseenIncomingIDs(ctx)
	})
	if !ok {
		log.Warn().Msg("seedUnreadIDs: unread badges start from the events alone")
		return false
	}

	me := r.client.Address()
	changed := false
	r.mu.Lock()
	defer r.mu.Unlock()
	for peer, ids := range unseen {
		if peer.IsZero() || peer == me {
			continue
		}
		if r.backwardsEpoch[peer].unread != before[peer].unread {
			// Read before something lowered this peer's badge — a mark-seen
			// for the conversation the user opened, a deletion — and applied
			// after. Adding these ids now would badge messages that have
			// since been read or removed. This covers the open conversation
			// too, whose mark-seen bumps the same counter, without needing a
			// rule about the active peer: the badge for a chat that failed
			// to open is rebuilt from the database by repairBadgeFromStore.
			continue
		}
		if _, alive := r.peers[peer]; !alive {
			// The scan reads the database, which still holds the messages of
			// a conversation the user removed while it ran. Creating the row
			// would put that conversation back on the sidebar, badge first —
			// the seed is a reconciliation like any other, and a
			// reconciliation never creates a peer.
			continue
		}
		before := len(r.unreadIDs[peer])
		for _, id := range ids {
			r.markUnreadLocked(peer, id)
		}
		if len(r.unreadIDs[peer]) != before {
			changed = true
		}
	}
	return changed
}

// applyScannedLastIncoming merges the scan's answer into every peer it found
// something for. A merge, not an assignment: the scan runs while events are
// already being handled, and the value it carries can only be older than what
// an event wrote — never newer — so taking the maximum is both correct and
// order-independent. Peers the scan found nothing for are left alone, because
// "the query returned no row" and "this peer has no incoming message" are
// only the same statement if nothing arrived meanwhile.
// The bool reports whether any peer changed, so the caller can publish a
// snapshot.
func (r *DMRouter) applyScannedLastIncoming(lastIncoming map[domain.PeerIdentity]time.Time, before map[domain.PeerIdentity]peerEpochs) bool {
	me := r.client.Address()

	changed := false
	r.mu.Lock()
	defer r.mu.Unlock()
	for peer, at := range lastIncoming {
		if peer.IsZero() || peer == me {
			continue
		}
		state, alive := r.peers[peer]
		if !alive {
			continue
		}
		if r.backwardsEpoch[peer].history != before[peer].history {
			// A deletion landed while the scan ran, and the row it read may
			// be the one that was deleted. The delete path has already
			// recomputed this peer from the database; taking a maximum
			// against a pre-deletion answer would undo exactly that.
			//
			// The HISTORY counter only: marking a conversation read removes
			// no rows, and gating this on the badge's movers would make the
			// conversation that opens at launch refuse its own scan.
			continue
		}
		before := state.LastIncomingAt
		r.noteIncomingAtLocked(peer, at)
		if r.peers[peer].LastIncomingAt != before {
			changed = true
		}
	}
	return changed
}

func (r *DMRouter) seedPreviews(previews []ConversationPreview, before map[domain.PeerIdentity]peerEpochs) {
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
		if p.PeerAddress == me || p.PeerAddress.IsZero() {
			continue
		}
		if r.backwardsEpoch[p.PeerAddress].history != before[p.PeerAddress].history {
			// The conversation moved — a message deleted, the thread wiped,
			// the contact removed (all three bump the history counter) —
			// while the fetch ran. Creating its row would put it back with
			// the message the user removed inside it.
			fresherPeers[p.PeerAddress] = struct{}{}
			continue
		}
		if !r.tryEnsurePeerLocked(p.PeerAddress) {
			// Being removed right now: the seed must not put it back.
			fresherPeers[p.PeerAddress] = struct{}{}
			continue
		}
		existing := r.peers[p.PeerAddress]
		// Skip if the ebus event-path already delivered fresher data for this
		// peer (ebus handlers run in parallel with initializeFromDB).
		if !existing.Preview.Timestamp.IsZero() && !existing.Preview.Timestamp.Before(p.Timestamp) {
			fresherPeers[p.PeerAddress] = struct{}{}
			continue
		}
		if r.previewIsFuture(p) {
			// A forward-dated row would pin the sidebar from the first
			// frame, and this path assigns rather than merges.
			fresherPeers[p.PeerAddress] = struct{}{}
			continue
		}
		existing.Preview = p
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
		if p.PeerAddress == me || p.PeerAddress.IsZero() {
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

	r.mu.Unlock()
}

// updateSidebarFromEvent tries to decrypt the incoming event inline and update
// peers[].Preview + Unread. Returns true when the preview was updated
// successfully, false when decryption failed and the caller must fall back to
// updatePreviewFromStore.
//
// The decrypt step is isolated from the apply step (see
// applyDecryptedMessageToSidebar) so tests can drive the apply
// branch with synthetic DirectMessages — notably, the regression
// test that verifies inbound file_announce messages registered for
// non-active conversations actually appear in
// FileTransferManager.AllTransfersSnapshot.
func (r *DMRouter) updateSidebarFromEvent(event protocol.LocalChangeEvent, peerID domain.PeerIdentity, stamp peerStamp) bool {
	msg := r.client.DecryptIncomingMessage(r.opContext(), event)
	if msg == nil {
		return false
	}
	r.applyDecryptedMessageToSidebar(msg, peerID, stamp)
	return true
}

// applyDecryptedMessageToSidebar updates peers[].Preview + Unread
// for an already-decrypted DirectMessage, runs the receiver-side
// file_announce registration, and is the entry point used by both
// the production inline-decrypt path and the regression test that
// drives non-active inbound announces with a synthetic message.
//
// Must be called outside r.mu — the function acquires it itself.
// gen is the peer's lifecycle generation as it was before the decrypt that
// produced msg. Decrypting is an RPC; a contact removed while it ran must not
// be brought back by the row this would write for it.
func (r *DMRouter) applyDecryptedMessageToSidebar(msg *DirectMessage, peerID domain.PeerIdentity, stamp peerStamp) {
	if msg == nil {
		return
	}

	isIncoming := msg.Sender != r.client.Address()

	r.mu.Lock()
	// The shared helper records the presence evidence and the order: unlike
	// the unread badge below, they are recorded on every incoming message —
	// startup replay and the open conversation are exactly the cases where
	// the peer demonstrably wrote to us.
	outcome := r.applyIncomingMessageLocked(peerID, *msg, stamp)
	if outcome != applyApplied {
		r.mu.Unlock()
		if outcome == applyStale {
			// Something in this conversation was deleted while the message
			// was being decrypted. Which row that was is not knowable from
			// here, so the database is asked instead of guessing.
			if !r.recoverFromStaleApply(peerID, msg) {
				r.evictSeenMessages(msg.ID)
			}
		}
		return
	}
	// Every incoming message that reaches this path is badged, with no
	// exception for the active peer. This function IS the not-on-screen
	// path: a caller that had the message on screen delivered it into the
	// open conversation and never got here. The case that made the old
	// exception wrong is the conversation SELECTED BUT NOT YET LOADED —
	// active by name, with nothing on screen to read — where skipping the
	// badge left the message invisible and uncounted, its id already
	// through the dedup gate.
	//
	// Startup replay is not suppressed either: with a set, the same id from
	// the startup read and from its own replayed event is one unread
	// message, while suppressing the replay loses the messages stored after
	// the read was taken.
	if isIncoming {
		r.markUnreadLocked(peerID, domain.MessageID(msg.ID))
	}
	r.mu.Unlock()

	// Register the receiver-side mapping for an inbound file_announce —
	// after the lifecycle guard above, never before it. Without this a
	// file announced into a background conversation would have no receiver
	// mapping until the user opened that chat, and the file tab would miss
	// it entirely.
	r.registerFileReceiveForLivePeer(msg, peerID, stamp)
}

// fileOpLock returns the per-peer mutex that serializes file-transfer
// REGISTRATION against every path that CLEANS UP transfers.
//
// The version check and the registration cannot be one atomic step on their
// own: the check needs r.mu, the registration goes through the file bridge,
// and holding a domain mutex across an external component is forbidden. So
// the two are made atomic with respect to the cleanups instead — everything
// that removes transfers takes this lock, bumps the version under it, and
// only then cleans up, while a registration holds it from its check until
// the mapping exists. A registration that started earlier therefore either
// finishes before the cleanup begins, or finds the bumped version and stands
// down; it can no longer land in between and re-create what was just removed.
func (r *DMRouter) fileOpLock(peer domain.PeerIdentity) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fileOpMu == nil {
		r.fileOpMu = make(map[domain.PeerIdentity]*sync.Mutex)
	}
	lock, ok := r.fileOpMu[peer]
	if !ok {
		lock = &sync.Mutex{}
		r.fileOpMu[peer] = lock
	}
	return lock
}

// withFileOps runs a transfer cleanup under the barrier.
//
// removedRows says whether rows actually left the conversation. Only then is
// the history counter moved, and it is moved HERE, under the barrier and
// before the cleanup, so a registration in flight sees the deletion rather
// than the state that preceded it. A cleanup that removed nothing — an ack
// for a row deleted long ago, a wipe that matched no message — must not move
// it: every false move marks a load or a decrypt that is perfectly current
// as stale, and sends it through recovery for nothing.
func (r *DMRouter) withFileOps(peer domain.PeerIdentity, removedRows bool, cleanup func()) {
	if peer.IsZero() {
		cleanup()
		return
	}
	lock := r.fileOpLock(peer)
	lock.Lock()
	defer lock.Unlock()

	if removedRows {
		r.mu.Lock()
		r.moveHistoryBackwardsLocked(peer)
		r.mu.Unlock()
	}

	cleanup()
}

// registerFileReceiveForLivePeer registers an inbound file announcement,
// unless the conversation moved while the message was being decrypted or
// loaded.
//
// Two things can have moved. gen is the peer's lifecycle generation: a
// contact removed since has had its transfers cleaned up, and registering
// this one would put a transfer back for a conversation that no longer
// exists. epoch is the peer's backwards-move counters: a message deleted or
// a thread wiped since means this announcement may name a row that is gone,
// and the generation says nothing about it — the contact is still there.
//
// Nothing is registered speculatively and rolled back afterwards. A rollback
// by message id cannot tell its own registration from an identical one made
// by a newer generation, and would take that one's downloaded file with it.
func (r *DMRouter) registerFileReceiveForLivePeer(msg *DirectMessage, peer domain.PeerIdentity, stamp peerStamp) {
	if msg == nil {
		return
	}
	// Held from the check until the mapping exists, so a cleanup cannot slip
	// in between and have this registration undo it.
	lock := r.fileOpLock(peer)
	lock.Lock()
	defer lock.Unlock()

	r.mu.RLock()
	_, alive := r.peers[peer]
	current := alive && r.stampIsCurrentLocked(peer, stamp)
	r.mu.RUnlock()
	if !current {
		// Either the conversation moved, or it is gone. A stamp alone does
		// not answer the second: a removal that is running right now bumps
		// the counters under this same barrier, and whoever waited for it
		// must find the row missing rather than register into a
		// conversation that no longer exists.
		return
	}
	r.tryRegisterFileReceive(msg)
}

// updatePreviewFromStore refreshes a peer from the chatlog on the
// new-message path. It reports whether the reconciliation applied — false
// means the caller should reopen its dedup gate so the message can be
// rediscovered — and a removed peer counts as applied: there is nothing left
// to rediscover.
func (r *DMRouter) updatePreviewFromStore(peer domain.PeerIdentity) bool {
	switch r.reconcilePeerFromStore(r.opContext(), peer, false, true) {
	case reconcileApplied, reconcilePeerGone:
		// Applied, or there is no conversation left to apply it to. Either
		// way the caller has nothing to undo.
		return true
	default:
		// Nothing was refreshed — a failed read, a contended peer, or a
		// client with no history at all. The caller reopens its dedup gate
		// so the message can be rediscovered.
		return false
	}
}

// reconcilePeerFromStore recomputes a peer's chatlog-derived state — preview,
// unread count and last-incoming evidence — and applies all three together.
//
// Two mechanisms keep it honest, and they answer different problems:
//
//   - the per-peer refresh lock ORDERS concurrent reconciliations. Two
//     deletions in one conversation run in their own goroutines, and without
//     it both could read, then apply in either order, leaving the older answer
//     on top;
//   - the revision ORDERS this reconciliation against the event path. The
//     queries are I/O and run outside r.mu, so a message arriving meanwhile
//     has already written values this read could not have seen. Its answer is
//     the newer one, so the read is repeated rather than applied.
//
// The three fields are applied in one critical section under one revision:
// accepting some and rejecting others would publish a mixture of two moments
// that never existed together.
//
// afterDelete says what an EMPTY conversation means. On the delete path it
// means the user removed the last row, so the preview is cleared and unread
// forced to zero; everywhere else it means the rows have not been written yet
// (a peer created from DM headers), and clearing would erase a badge that is
// about to be justified. Reports whether the store answered at all.
func (r *DMRouter) reconcilePeerFromStore(ctx context.Context, peer domain.PeerIdentity, afterDelete, waitForLock bool) reconcileOutcome {
	if r.client == nil {
		return reconcileRetry
	}
	if peer.IsZero() {
		// A malformed event — an unparseable sender — is not a removal. The
		// caller has to tell the two apart: "gone" means stop, this one
		// means nothing was reconciled and the message id must stay
		// rediscoverable.
		return reconcileRetry
	}

	// One reconciliation per peer at a time. Two deletions in one
	// conversation run in their own goroutines, and without this the slower
	// query would land last holding the older answer. It is the only
	// ordering this needs: a message arriving meanwhile merges idempotently
	// (a preview never moves backwards in time, an unread id is a set
	// member), so only the deletion — the one authoritative step backwards —
	// has to be serialized.
	//
	// The lock is taken only for a peer that still exists: creating one for a
	// removed conversation would leave an entry nobody prunes, and taking it
	// would serialize work that is about to be discarded anyway.
	lock, alive := r.peerRefreshLock(peer)
	if !alive {
		return reconcilePeerGone
	}
	if waitForLock {
		lock.Lock()
	} else if !lock.TryLock() {
		// Someone is already reconciling this peer. Waiting would hold the
		// caller — the shared delete sweep — behind two query timeouts it
		// has no budget for; the peer stays queued for the next tick.
		return reconcileBusy
	}
	defer lock.Unlock()

	// A reconciliation UPDATES a peer; it never creates one. The peer may
	// have been removed before this goroutine was even scheduled — the queue
	// is asynchronous, so no token taken here can see that — and creating the
	// row would put the deleted conversation back on the sidebar with its
	// last message in it. Callers that introduce a new conversation create
	// the peer themselves, before asking for the reconciliation.
	genBefore, observed := r.peerReconcileStart(peer)

	reader := r.chatHistory()
	if reader == nil {
		// No chatlog on this client at all. Nothing to reconcile against,
		// and nothing a retry would fix.
		return reconcileNoHistory
	}

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	preview, err := r.client.FetchSinglePreview(readCtx, peer)
	cancel()
	if err != nil {
		// Transient chatlog error — leave the peer as it is rather than
		// wiping it on a flaky read, and ask to be called again.
		return reconcileRetry
	}

	lastIncoming, incomingResolved := r.fetchLastIncoming(ctx, peer)

	// The badge is re-derived only on the delete path, and only there
	// because it is the one place that has to be able to REMOVE ids: the
	// event stream carries no delivery status, so everywhere else it can
	// only add. This is also what heals a badge that drifted — an id the
	// user has since read elsewhere, say — since nothing else re-reads it.
	var unseen []domain.MessageID
	unseenResolved := true
	if afterDelete {
		unseen, unseenResolved = r.fetchUnseenIncoming(ctx, reader, peer)
		if unseenResolved {
			unseen, unseenResolved = r.keepUnwrittenUnread(ctx, reader, unseen, observed.unreadIDs)
		}
	}

	if !incomingResolved || !unseenResolved {
		// Half a read is not a reconciliation: applying the preview while the
		// last-incoming query failed would leave the two describing different
		// conversations, and after a deletion nothing else re-reads this
		// peer. This covers the EMPTY conversation too — "no rows left" is a
		// claim about the same conversation, and it is the most destructive
		// one to get wrong.
		//
		// The three reads are three statements, not one database snapshot: a
		// row committed between them is visible to the later ones only. That
		// skew is bounded and self-healing — the event carrying that row
		// merges idempotently afterwards (preview by timestamp, last-incoming
		// by maximum, unread by set) — while the skew that would NOT heal,
		// this reconciliation lowering a value onto a peer that moved
		// meanwhile, is what the check below catches.
		return reconcileRetry
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.peerAliveLocked(peer, genBefore) {
		// The contact is gone — removed while this ran, or before it
		// started. Nothing to reconcile and nothing to retry.
		return reconcilePeerGone
	}

	if preview == nil {
		if !afterDelete {
			return reconcileApplied
		}
		// No rows left for this peer: the user just removed the last one. An
		// empty conversation cannot have unread messages, and nothing in the
		// history supports a last-online date — but this is a step BACKWARDS
		// like any other on the delete path, so it needs the same check that
		// nothing arrived while the queries ran. Wiping a message that landed
		// meanwhile would leave the sidebar blank for a message the database
		// holds, with nothing to re-read it.
		if !r.derivedStateMatchesLocked(peer, observed) {
			return reconcileRetry
		}
		r.peers[peer].Preview = ConversationPreview{PeerAddress: peer}
		r.peers[peer].LastIncomingAt = domain.OptionalTime{}
		r.replaceUnreadLocked(peer, unseen)
		r.moveHistoryBackwardsLocked(peer)
		return reconcileApplied
	}

	if afterDelete {
		// The deletion is the one authority allowed to move these backwards:
		// the message they described may be the row the user just removed.
		// It is therefore also the only path that can undo somebody else's
		// work, so it applies only if the values it read are still the ones
		// on screen. A message that landed while the queries ran is newer
		// than anything the deletion removed, and lowering onto it would
		// take the sidebar back to before it arrived.
		if !r.derivedStateMatchesLocked(peer, observed) {
			return reconcileRetry
		}
		if !r.previewIsFuture(*preview) {
			// The deletion may move the preview backwards, but not onto a
			// forward-dated row: that ceiling outlives the deletion.
			r.peers[peer].Preview = *preview
		}
		r.peers[peer].LastIncomingAt = optionalTimeOrUnset(lastIncoming)
		r.replaceUnreadLocked(peer, unseen)
		// This assignment is itself a backwards move: a read that started
		// before it must not merge its older answer on top.
		r.moveHistoryBackwardsLocked(peer)
		return reconcileApplied
	}

	if r.backwardsEpoch[peer].history != observed.epochs.history {
		// Rows left this conversation while these queries ran. The merges
		// below only move forward, so applying an answer read before that
		// would put back exactly what was removed. Re-read.
		return reconcileRetry
	}
	r.mergePreviewLocked(peer, *preview)
	r.noteIncomingAtLocked(peer, lastIncoming)
	return reconcileApplied
}

// chatHistoryReader is the chatlog surface the sidebar's derived state is
// read through. Narrow on purpose: these are the only questions the router
// asks of history, and an interface this size is one a test can stand in for
// — including standing in for a database that fails one read, or fails once
// and then recovers, which a working SQLite refuses to do on demand.
type chatHistoryReader interface {
	// LastIncomingAtFor is the newest non-future message the peer wrote.
	LastIncomingAtFor(ctx context.Context, peer domain.PeerIdentity, now time.Time) (time.Time, error)
	// LastIncomingAtPerPeer is the same question for every conversation.
	LastIncomingAtPerPeer(ctx context.Context, now time.Time) (map[domain.PeerIdentity]time.Time, error)
	// UnseenIncomingIDs is every conversation's unread ids as the database
	// sees them, and the only startup reader of delivery_status.
	UnseenIncomingIDs(ctx context.Context) (map[domain.PeerIdentity][]domain.MessageID, error)
	// UnseenIncomingIDsFor is one peer's. It is the only reconciliation the
	// badge gets after startup: the event stream carries no delivery status,
	// so it can only ever add.
	UnseenIncomingIDsFor(ctx context.Context, peer domain.PeerIdentity) ([]domain.MessageID, error)
	// StoredMessageStatuses is the delivery status of every one of these ids
	// the database holds; an absent id is one it does not hold at all. One
	// read serves both questions asked of it — "has the user read this
	// already", which a header cannot answer, and "is this id merely not
	// written yet", which the delete path must not mistake for "read".
	StoredMessageStatuses(ctx context.Context, ids []domain.MessageID) (map[domain.MessageID]string, error)
}

// reconcileOutcome says what happened, because the three cases need different
// answers from the caller: a retry has to be scheduled, a gone peer must not
// be, and an applied result closes the matter. A bool collapsed the last two
// and let a deletion drop its retry.
type reconcileOutcome int

const (
	// reconcileApplied — the peer now matches what the chatlog said.
	reconcileApplied reconcileOutcome = iota
	// reconcileRetry — nothing was applied; the read has to be repeated.
	reconcileRetry
	// reconcilePeerGone — the conversation no longer exists. Nothing to do,
	// now or later.
	reconcilePeerGone
	// reconcileNoHistory — this client keeps no chatlog, so there is nothing
	// to reconcile against. Distinct from a failed read: retrying would
	// never succeed, and a caller that queued it would burn its whole budget
	// and then warn about a database that does not exist.
	reconcileNoHistory
	// reconcileBusy — another reconciliation holds this peer. Nothing was
	// read, so nothing failed; the caller may come back without spending an
	// attempt.
	reconcileBusy
)

// ensurePeerForReconcile creates the sidebar row for a conversation that is
// new to this process, so the reconciliation that follows has something to
// update. Reconciliation itself never creates: it runs asynchronously, so a
// row it created could be one the user removed while the work was queued.
// Introducing the conversation is the caller's decision and happens
// synchronously with the event that justifies it.
func (r *DMRouter) ensurePeerForReconcile(peer domain.PeerIdentity, gen uint64) {
	if peer.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.peerGenUnchangedLocked(peer, gen) {
		// Removed since the caller started. Creating the row now would put
		// the deleted conversation back, and the reconciliation that follows
		// would find its own fresh generation perfectly consistent.
		return
	}
	r.tryEnsurePeerLocked(peer)
}

// derivedPeerState is the pair a deletion has to find unchanged before it may
// lower it. Comparing the VALUES rather than a counter keeps the check honest
// without a second bookkeeping field: these are exactly what the deletion is
// about to overwrite.
type derivedPeerState struct {
	preview      ConversationPreview
	lastIncoming domain.OptionalTime
	// unreadIDs is the badge set itself. The delete path needs it twice: to
	// tell "not written yet" from "read" (an id the database does not hold
	// at all), and to compare sets rather than sizes — one id dropped while
	// another arrives leaves the SIZE untouched, and a check on the size
	// would then wave through a reconciliation that drops the new id.
	unreadIDs map[domain.MessageID]struct{}
	// epochs are the peer's backwards-move counters as they were before the
	// queries. The delete path overwrites the badge AND the history-derived
	// values, so it checks both.
	epochs peerEpochs
}

// peerReconcileStart captures the lifecycle generation and the derived values
// as they are before the queries run.
func (r *DMRouter) peerReconcileStart(peer domain.PeerIdentity) (gen uint64, observed derivedPeerState) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// The epoch is read outside the row check: a peer whose row is gone can
	// still have one, and reading zero for it would make a stale answer look
	// current.
	observed.epochs = r.backwardsEpoch[peer]
	if state, ok := r.peers[peer]; ok {
		observed.preview = state.Preview
		observed.lastIncoming = state.LastIncomingAt
		observed.unreadIDs = r.unreadSnapshotLocked(peer)
	}
	return r.peerGen[peer], observed
}

// derivedStateMatchesLocked reports whether the peer still holds the values
// the caller read before its queries. Callers hold r.mu.
func (r *DMRouter) derivedStateMatchesLocked(peer domain.PeerIdentity, observed derivedPeerState) bool {
	state, ok := r.peers[peer]
	if !ok {
		return false
	}
	if r.backwardsEpoch[peer] != observed.epochs {
		return false
	}
	return state.Preview == observed.preview &&
		state.LastIncomingAt == observed.lastIncoming &&
		maps.Equal(r.unreadIDs[peer], observed.unreadIDs)
}

// peerEpochs is the pair of backwards-move counters a peer carries. unread
// counts the moves that only lower the BADGE (mark-seen, the optimistic clear
// on opening a conversation); history counts the ones that remove ROWS
// (message deletion, conversation wipe, contact removal, identity reset) and
// therefore invalidate the preview and the last-incoming date as well.
//
// A history move implies an unread move — the deleted rows may be badged —
// so moveHistoryBackwardsLocked bumps both.
type peerEpochs struct {
	unread  uint64
	history uint64
}

// moveUnreadBackwardsLocked records that this peer's BADGE just moved
// backwards: a mark-seen, an optimistic clear. Callers hold r.mu.
func (r *DMRouter) moveUnreadBackwardsLocked(peer domain.PeerIdentity) {
	if peer.IsZero() {
		return
	}
	if r.backwardsEpoch == nil {
		r.backwardsEpoch = make(map[domain.PeerIdentity]peerEpochs)
	}
	epochs := r.backwardsEpoch[peer]
	epochs.unread++
	r.backwardsEpoch[peer] = epochs
}

// moveHistoryBackwardsLocked records that rows left this peer's conversation,
// which invalidates every chatlog-derived value it has — preview,
// last-incoming date and badge alike. Callers hold r.mu.
func (r *DMRouter) moveHistoryBackwardsLocked(peer domain.PeerIdentity) {
	if peer.IsZero() {
		return
	}
	if r.backwardsEpoch == nil {
		r.backwardsEpoch = make(map[domain.PeerIdentity]peerEpochs)
	}
	epochs := r.backwardsEpoch[peer]
	epochs.unread++
	epochs.history++
	r.backwardsEpoch[peer] = epochs
}

// peerEpochsOf reads a peer's counters, to be captured before a chatlog read
// and re-checked before its result is applied.
func (r *DMRouter) peerEpochsOf(peer domain.PeerIdentity) peerEpochs {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backwardsEpoch[peer]
}

// peerGenUnchangedLocked reports whether the peer's lifecycle generation is
// still the one the caller captured before its slow work. Unlike
// peerAliveLocked it does NOT require the row to exist: a conversation new to
// this process has no row yet and must still be allowed to appear. Callers
// hold r.mu.
func (r *DMRouter) peerGenUnchangedLocked(peer domain.PeerIdentity, gen uint64) bool {
	return r.peerGen[peer] == gen
}

// peerAliveLocked reports whether the peer is still on the sidebar and still
// the same generation of it. Both halves are needed: the row answers a
// removal that happened before this reconciliation was scheduled, the
// generation answers one that happened while it ran and was followed by the
// contact being added back. Callers hold r.mu.
func (r *DMRouter) peerAliveLocked(peer domain.PeerIdentity, gen uint64) bool {
	if r.peerGen[peer] != gen {
		return false
	}
	_, alive := r.peers[peer]
	return alive
}

// mergePreviewLocked accepts a preview only if it is not older than the one
// on screen. The rule is what lets a SQL read and the event stream run in any
// order: the database is ahead of the events, so both can carry the same
// message, and neither may undo the other's newer one. Callers hold r.mu.
// previewIsFuture refuses a preview dated after now. The timestamp is the
// SENDER's own created_at and the node accepts minutes of clock drift, so a
// message dated forward would not merely show a wrong time — it would become
// the ceiling that every later preview, including our own replies, fails to
// beat, pinning whatever the peer chose in the sidebar for as long as they
// keep it up. Every path that assigns Preview goes through this, including
// the two that assign rather than merge (the startup seed, and the deletion
// that is allowed to move the value backwards).
func (r *DMRouter) previewIsFuture(preview ConversationPreview) bool {
	return !preview.Timestamp.IsZero() && preview.Timestamp.After(r.now())
}

func (r *DMRouter) mergePreviewLocked(peer domain.PeerIdentity, preview ConversationPreview) {
	state, ok := r.peers[peer]
	if !ok {
		return
	}
	if r.previewIsFuture(preview) {
		return
	}
	if !state.Preview.Timestamp.IsZero() && preview.Timestamp.Before(state.Preview.Timestamp) {
		return
	}
	state.Preview = preview
}

// fetchUnseenIncoming reads the peer's unread ids as the database sees them.
// The second result separates "the store answered" from "the store did not",
// because only the former may replace what is on screen.
func (r *DMRouter) fetchUnseenIncoming(ctx context.Context, reader chatHistoryReader, peer domain.PeerIdentity) ([]domain.MessageID, bool) {
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	ids, err := reader.UnseenIncomingIDsFor(readCtx, peer)
	cancel()
	if err != nil {
		return nil, false
	}
	return ids, true
}

// keepUnwrittenUnread adds back the badged ids the database does not hold at
// all.
//
// The header path badges a message as soon as the node reports it, which can
// be before its chatlog row is written — the preview path has the same rule
// and says so. Re-deriving the badge purely from the database would read
// "not in the unseen list" as "read", drop that id, and never get it back:
// the event stream cannot re-add it (the dedup gate has already seen it) and
// only another deletion re-reads delivery_status.
//
// An id the database DOES hold and does not report as unseen is genuinely
// read or gone, and stays dropped. The second result is false when the
// lookup failed, because a reconciliation applies all of its reads or none.
func (r *DMRouter) keepUnwrittenUnread(
	ctx context.Context,
	reader chatHistoryReader,
	unseen []domain.MessageID,
	badged map[domain.MessageID]struct{},
) ([]domain.MessageID, bool) {
	if len(badged) == 0 {
		return unseen, true
	}
	inUnseen := make(map[domain.MessageID]struct{}, len(unseen))
	for _, id := range unseen {
		inUnseen[id] = struct{}{}
	}
	pending := make([]domain.MessageID, 0, len(badged))
	for id := range badged {
		if _, ok := inUnseen[id]; ok {
			continue
		}
		pending = append(pending, id)
	}
	if len(pending) == 0 {
		return unseen, true
	}

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	statuses, err := reader.StoredMessageStatuses(readCtx, pending)
	cancel()
	if err != nil {
		return nil, false
	}
	for _, id := range pending {
		if _, stored := statuses[id]; stored {
			continue
		}
		unseen = append(unseen, id)
	}
	return unseen, true
}

// replaceUnreadLocked makes the badge exactly what the database reported.
// Used only where the caller has just verified that nothing moved under it —
// otherwise a message that arrived during the read would be dropped, and the
// event that carried it has already been consumed. Callers hold r.mu.
func (r *DMRouter) replaceUnreadLocked(peer domain.PeerIdentity, ids []domain.MessageID) {
	// Whatever this drops, a read still in flight may be holding. Only the
	// badge: the caller assigns the preview and the date itself, and bumps
	// the history counter when it does.
	r.moveUnreadBackwardsLocked(peer)
	delete(r.unreadIDs, peer)
	for _, id := range ids {
		r.markUnreadLocked(peer, id)
	}
	r.syncUnreadCountLocked(peer)
}

// fetchLastIncoming reads the peer's newest incoming message time from the
// chatlog. The second result separates "the store answered, and the answer is
// no incoming message" from "the store did not answer" — only the former is
// grounds for a caller to overwrite what it already knows.
//
// now is passed down: the store refuses rows dated after it, and skipping one
// does not skip the conversation — the honest message behind a forged future
// date is still the answer.
func (r *DMRouter) fetchLastIncoming(ctx context.Context, peer domain.PeerIdentity) (time.Time, bool) {
	reader := r.chatHistory()
	if reader == nil {
		return time.Time{}, false
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	at, err := reader.LastIncomingAtFor(readCtx, peer, r.now())
	cancel()
	if err != nil {
		return time.Time{}, false
	}
	return at, true
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
	r.setPeerPreviewLocked(peer, last)
	// The cache holds the whole conversation, so the newest incoming message
	// is available even when the last row is our own reply — the case that
	// setPeerPreviewLocked alone cannot see.
	for i := len(r.activeMessages) - 1; i >= 0; i-- {
		if r.activeMessages[i].Sender != peer {
			continue
		}
		r.noteIncomingAtLocked(peer, r.activeMessages[i].Timestamp)
		break
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
	if !r.loadConversation(peerID, r.peerEpochsOf(peerID)) {
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
	firstSync := !r.initialSynced
	if firstSync {
		r.initialSynced = true
	}
	// The first repair after startup used to skip the badge entirely,
	// because the badge was a counter and the startup snapshot had already
	// counted these messages. With a set that guard only causes losses: a
	// message the snapshot missed — or every message, when the snapshot
	// itself failed — would never be marked unread. Adding an id the set
	// already holds costs nothing, so the repair reports what it finds.
	//
	// DMHeaders carry no delivery_status, which is why the header path can
	// only ADD ids; removal stays with mark-seen and deletion. On the FIRST
	// sync that is not enough on its own: the node's topic may hold messages
	// this client read in an earlier session — a desktop reattaching to a
	// long-running node sees them all — and badging them again would undo
	// reads the database has already recorded. So on the first sync the
	// header only decides for messages the database does not know; for the
	// rest the database decides, through seedUnreadIDs.

	// Copy seenMessageIDs keys for the lock-free classification pass.
	// The copy is O(len(seenMessageIDs)) but is bounded by the total
	// number of ever-seen messages and runs once per 5-second poll —
	// far cheaper than holding the write lock during the header scan.
	seenCopy := make(map[string]struct{}, len(r.seenMessageIDs))
	for id := range r.seenMessageIDs {
		seenCopy[id] = struct{}{}
	}
	// The lifecycle generation of every peer we may touch, captured BEFORE
	// the header scan and the status read. Both happen outside the lock, and
	// a contact removed while they run must not be recreated by the row this
	// is about to write for it. peerGen starts at zero, so a conversation
	// that is new to this process compares equal and is created normally.
	genBefore := make(map[domain.PeerIdentity]uint64, len(r.peerGen))
	for peer, gen := range r.peerGen {
		genBefore[peer] = gen
	}
	epochBefore := make(map[domain.PeerIdentity]peerEpochs, len(r.backwardsEpoch))
	for peer, epochs := range r.backwardsEpoch {
		epochBefore[peer] = epochs
	}
	r.mu.Unlock()

	// ---------------------------------------------------------------
	// Phase 2 (lock-free): classify each header into new/seen using
	// the snapshot. cache.HasMessage has its own internal lock.
	// ---------------------------------------------------------------
	// The action carries FACTS about the header — who wrote it, whether the
	// database already calls it read, whether the cache holds it — and not
	// the decisions that depend on which conversation is on screen. That
	// question is answered in phase 3, under the lock: `selected` was read
	// before the header scan and the stored-status query, and the user can
	// switch conversations while those run. Deciding here would classify a
	// message as visible after the user left it, which skips its badge for
	// good — its id goes through the dedup gate either way.
	type headerAction struct {
		peer          domain.PeerIdentity
		id            string
		isIncoming    bool
		readInDB      bool
		inCache       bool
		firstSyncBeep bool
	}
	var actions []headerAction

	alreadyRead := map[domain.MessageID]struct{}{}
	if firstSync {
		alreadyRead = r.alreadyReadHeaderIDs(r.opContext(), status.DMHeaders, me, seenCopy)
	}

	for _, header := range status.DMHeaders {
		if _, ok := seenCopy[header.ID]; ok {
			continue
		}
		var peer domain.PeerIdentity
		isIncoming := false
		if header.Sender == me {
			peer = normalizePeer(header.Recipient)
		} else if header.Recipient == me {
			peer = normalizePeer(header.Sender)
			isIncoming = true
		} else {
			continue
		}

		_, readInDatabase := alreadyRead[domain.MessageID(header.ID)]

		actions = append(actions, headerAction{
			peer:          peer,
			id:            header.ID,
			isIncoming:    isIncoming,
			readInDB:      readInDatabase,
			inCache:       r.cache.HasMessage(header.ID),
			firstSyncBeep: firstSync,
		})
	}

	// ---------------------------------------------------------------
	// Phase 3 (short lock): apply classified mutations.
	// ---------------------------------------------------------------
	hasNew := false
	needReload := false
	peerMessageIDs := make(map[domain.PeerIdentity][]string)
	rebuild := make(map[domain.PeerIdentity]struct{})
	var reloadMessageIDs []string

	r.mu.Lock()
	// The conversation that is on screen NOW, not the one that was when the
	// scan started. It also decides which conversation the reload below
	// belongs to.
	selected := r.activePeer
	for _, a := range actions {
		// Re-check under lock: another goroutine may have inserted the
		// same ID between Phase 1 snapshot and now.
		if _, ok := r.seenMessageIDs[a.id]; ok {
			continue
		}
		onScreen := a.peer == selected && !selected.IsZero()
		incrementUnread := a.isIncoming && !onScreen && !a.readInDB
		triggerReload := onScreen && !a.inCache
		refreshSidebar := !a.peer.IsZero() && !onScreen
		triggerBeep := a.isIncoming && !a.firstSyncBeep && !onScreen
		if r.peerGen[a.peer] != genBefore[a.peer] {
			// The contact was removed while the scan ran. Creating the row
			// would put a deleted conversation back on the sidebar, and
			// there is nothing to recover: the conversation is gone.
			continue
		}
		if r.backwardsEpoch[a.peer] != epochBefore[a.peer] {
			// The badge moved backwards — a mark-seen, a deletion — while
			// the scan ran, so these ids may already be read or gone.
			// This runs ONCE per process (pollHealth is deferred from
			// initializeFromDB and has no other caller), so skipping would
			// drop the badge for the session; the peer is handed to the
			// database instead, which is the only thing that still knows.
			//
			// Except for the conversation on screen, which has no badge to
			// rebuild: the user is reading it, the mark-seen that moved the
			// counter is the receipt for these very messages, and a rebuild
			// would put a count on the chat in front of them.
			if !onScreen {
				rebuild[a.peer] = struct{}{}
			}
			continue
		}
		if a.isIncoming && !r.tryEnsurePeerLocked(a.peer) {
			// Being removed: no row, and therefore no badge, no order and
			// no dedup entry either — the id is deliberately left OUT of
			// seenMessageIDs, because writing it off as handled while
			// nothing was applied is how a message disappears.
			continue
		}
		r.seenMessageIDs[a.id] = struct{}{}

		if triggerBeep {
			hasNew = true
		}
		if incrementUnread {
			r.markUnreadLocked(a.peer, domain.MessageID(a.id))
			r.promotePeerLocked(a.peer)
		}
		if triggerReload {
			needReload = true
			reloadMessageIDs = append(reloadMessageIDs, a.id)
		}
		// Skip the active peer from sidebar-only preview refresh — its
		// messages are handled by the loadConversation path below, which
		// has its own rollback via reloadMessageIDs. Running both in
		// parallel would cause refreshPreviewForPeer to evict IDs that
		// loadConversation already recovered, triggering duplicate
		// rediscovery and spurious UIEventBeep on the next health poll.
		if refreshSidebar {
			peerMessageIDs[a.peer] = append(peerMessageIDs[a.peer], a.id)
		}
	}
	r.mu.Unlock()

	for peer := range rebuild {
		// Outside the lock: this reads the database.
		_ = r.repairBadgeFromStore(peer)
	}

	for peer, ids := range peerMessageIDs {
		// Created before the goroutine is queued, for the same reason as on
		// the new-message path: a removal that completes first must not be
		// undone by a row the queued work creates for itself.
		r.ensurePeerForReconcile(peer, genBefore[peer])
		if !r.beginOp() {
			break
		}
		go func(peer domain.PeerIdentity, ids []string) {
			defer r.endOp()
			r.refreshPreviewForPeer(peer, ids)
		}(peer, ids)
	}

	if hasNew {
		r.notify(UIEventBeep)
	}

	if needReload && !selected.IsZero() {
		if r.loadConversation(selected, r.peerEpochsOf(selected)) {
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
			if r.beginOp() {
				go func() {
					defer r.endOp()
					r.doMarkSeen(selected)
				}()
			}
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
	r.clearUnreadLocked(peer)
	r.mu.Unlock()
}

// repairBadgeFromStore rebuilds a peer's badge from the database, for the
// path where opening the conversation failed.
//
// The snapshot the rollback restores is only as good as what was in memory
// when the conversation was opened, and at startup that can be nothing at
// all: the badge seed may not have applied yet, or may have skipped this peer
// precisely because the open was moving it. The database is the one place
// that still knows, and outside the delete path nothing else re-reads
// delivery_status.
//
// It REPLACES rather than unions, because a mark-seen that failed halfway
// still marked some ids seen in the database and a union would keep badging
// them. Two things are carried across the replacement: ids the database does
// not hold at all (badged from a header whose row has not landed) and ids
// that arrived while this was reading — additions move no counter, so the
// epoch check cannot see them, and the event that carried them has already
// been consumed by the dedup gate.
func (r *DMRouter) repairBadgeFromStore(peer domain.PeerIdentity) bool {
	reader := r.chatHistory()
	if reader == nil {
		// No history to rebuild from. Not a failure of this call, and not a
		// recovery either — the caller decides what that means for it.
		return true
	}
	before := r.peerEpochsOf(peer)
	badgedBefore := r.unreadSnapshot(peer)

	ctx, cancel := context.WithTimeout(r.opContext(), 2*time.Second)
	unseen, err := reader.UnseenIncomingIDsFor(ctx, peer)
	cancel()
	if err != nil {
		log.Warn().Err(err).Str("peer", peer.String()).Msg("dm_router: could not rebuild the badge after a failed open")
		return false
	}

	unseen, ok := r.keepUnwrittenUnread(r.opContext(), reader, unseen, badgedBefore)
	if !ok {
		return false
	}

	changed := false
	r.mu.Lock()
	if _, alive := r.peers[peer]; alive && r.backwardsEpoch[peer] == before {
		for id := range r.unreadIDs[peer] {
			if _, had := badgedBefore[id]; had {
				continue
			}
			// Added while the queries ran. Nothing else will re-add it.
			unseen = append(unseen, id)
		}
		had := len(r.unreadIDs[peer])
		r.replaceUnreadLocked(peer, unseen)
		changed = len(r.unreadIDs[peer]) != had
	}
	r.mu.Unlock()

	if changed {
		r.notify(UIEventSidebarUpdated)
	}
	return true
}

// unreadSnapshot is unreadSnapshotLocked for a caller that holds nothing.
func (r *DMRouter) unreadSnapshot(peer domain.PeerIdentity) map[domain.MessageID]struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.unreadSnapshotLocked(peer)
}

// restorePeerUnread puts back the set the optimistic clear removed, after the
// background mark-seen failed. The SET is restored rather than a count: the
// ids are what the next mark-seen has to work through, and a bare number
// would have to be reinvented from SQL.
func (r *DMRouter) restorePeerUnread(peer domain.PeerIdentity, ids map[domain.MessageID]struct{}) {
	if len(ids) == 0 {
		return
	}
	r.mu.Lock()
	r.restoreUnreadLocked(peer, ids)
	r.mu.Unlock()
	r.notify(UIEventSidebarUpdated)
}

// now reads THIS NODE'S clock, always in UTC. nil means production, where the
// clock is the wall clock; tests that reason about future or past timestamps
// install their own instead of sleeping.
//
// The deletion paths make decisions with it, which is why it matters that they
// go through here rather than calling time.Now: what a wipe reaches depends on
// where the requester's boundary lands relative to the moment their request
// arrived HERE (see wipeBoundaryInOurFrame), and two nodes whose clocks differ
// is the case that has to be provable in a test.
func (r *DMRouter) now() time.Time {
	if r.presenceClock == nil {
		// A router built as a bare struct literal — only tests do that, and
		// only the ones with no interest in time.
		return time.Now().UTC()
	}
	return r.presenceClock().UTC()
}

// deliverDecryptedMessage puts a freshly decrypted message where it belongs
// now that the decrypt has finished, and reports whether that was the open
// conversation.
//
// The "is this the conversation on screen" answer the caller had is as old as
// the decrypt, which is an RPC. If the user switched away while it ran, both
// halves of that answer are wrong: appending to the cache would splice this
// message into the OTHER peer's thread, and treating it as visible would skip
// its unread badge for good, because its id is already through the dedup
// gate. So the question is asked again here, against the cache as well as the
// selection, and a message that lost its place is delivered as a background
// arrival instead.
//
// false means "handled as background" — the caller still has to notify.
func (r *DMRouter) deliverDecryptedMessage(msg *DirectMessage, peerID domain.PeerIdentity, stamp peerStamp) bool {
	// All three questions in ONE critical section, and the LIFECYCLE one
	// first: a contact removed and added back carries a new generation, and
	// appending before that check leaves the old message sitting in the new
	// conversation's cache even though the sidebar write is refused. The
	// selection and the cache owner are checked here too — split apart, a
	// switch in between splices the message into the other peer's thread.
	// The cache has its own mutex and never calls back into the router, so
	// taking it here inverts no order.
	r.mu.Lock()
	gone := !r.peerGenUnchangedLocked(peerID, stamp.gen)
	stale := !gone && r.backwardsEpoch[peerID].history != stamp.epochs.history
	stillOpen := !gone && !stale &&
		r.activePeer == peerID && !peerID.IsZero() &&
		r.cache.AppendForPeer(peerID, *msg)
	if stillOpen {
		r.applyIncomingMessageLocked(peerID, *msg, stamp)
		r.activeMessages = r.cache.Messages()
		r.pendingScrollToEnd = true
	}
	r.mu.Unlock()

	if gone {
		// The contact is gone: nothing to show and nothing to badge. It
		// counts as delivered so the caller does not fall back to the
		// sidebar path for a conversation that no longer exists.
		return true
	}
	if stale {
		// A row left this conversation while the message was being
		// decrypted, and which row it was is not knowable from a per-peer
		// counter. The database is asked rather than the message dropped:
		// its id is already through the dedup gate, so a wrong guess here
		// loses it for good. The conversation on screen is reloaded from
		// the same authority.
		recovered := r.recoverFromStaleApply(peerID, msg)
		if r.loadConversation(peerID, r.peerEpochsOf(peerID)) {
			r.notify(UIEventMessagesUpdated)
		} else {
			// The reload failed, so the message is not on screen either.
			// Nothing here can put it there, and the cache still belongs to
			// this peer, so re-selecting the conversation would not reload
			// it: the id goes back out of the dedup gate for the header
			// repair to find.
			recovered = false
		}
		if !recovered {
			r.evictSeenMessages(msg.ID)
		}
		return true
	}
	if !stillOpen {
		r.applyDecryptedMessageToSidebar(msg, peerID, stamp)
		return false
	}
	r.registerFileReceiveForLivePeer(msg, peerID, stamp)
	return true
}

// peerStamp is what a slow step has to carry: the peer as it was when the
// work started. Two values, because they answer different questions — the
// contact may be gone (generation), or still there with the message that was
// being decrypted already deleted (history counter). Passing them as one
// value is what keeps a branch from checking half of it.
type peerStamp struct {
	gen    uint64
	epochs peerEpochs
}

// peerStampOf captures the pair before a decrypt, a fetch, or anything else
// that takes long enough for the conversation to move.
func (r *DMRouter) peerStampOf(peer domain.PeerIdentity) peerStamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return peerStamp{gen: r.peerGen[peer], epochs: r.backwardsEpoch[peer]}
}

// stampIsCurrentLocked reports whether the peer still is what the stamp says.
//
// Only the HISTORY half of the epoch is compared. Marking a conversation read
// bumps the unread counter and removes no rows, so it cannot make a message
// stale — comparing the whole pair would make opening a chat discard the
// messages arriving into it. Callers hold r.mu.
func (r *DMRouter) stampIsCurrentLocked(peer domain.PeerIdentity, stamp peerStamp) bool {
	return r.peerGenUnchangedLocked(peer, stamp.gen) &&
		r.backwardsEpoch[peer].history == stamp.epochs.history
}

// applyIncomingMessageLocked writes a decrypted message into the sidebar —
// preview, presence evidence, and conversation order — and reports whether it
// did.
//
// stamp is the peer as it was BEFORE the decrypt, which is an RPC. Two things
// can have moved since: the contact can be gone, and setPeerPreviewLocked
// creates the row it writes to, so the write would bring it back; or the
// message itself can have been deleted, which the generation cannot see —
// the contact is still there, and this write would put its preview, its
// last-online evidence and its badge back. Every branch that applies a
// decrypted message goes through here precisely so that one of them cannot
// forget the check: there are three, they look different, and the one that
// was missed was the one for the conversation on screen.
//
// Callers hold r.mu.
func (r *DMRouter) applyIncomingMessageLocked(peer domain.PeerIdentity, msg DirectMessage, stamp peerStamp) applyOutcome {
	if !r.peerGenUnchangedLocked(peer, stamp.gen) {
		return applyPeerGone
	}
	if r.backwardsEpoch[peer].history != stamp.epochs.history {
		return applyStale
	}
	r.setPeerPreviewLocked(peer, msg)
	if _, ok := r.peers[peer]; !ok {
		// The row was refused: a removal is running, and the stamp cannot
		// see it — the counters it compares were already bumped by that
		// same removal.
		return applyPeerGone
	}
	r.promotePeerLocked(peer)
	return applyApplied
}

// applyOutcome separates the two reasons a decrypted message is not applied,
// because they need opposite answers.
type applyOutcome int

const (
	// applyApplied — the message is on the sidebar.
	applyApplied applyOutcome = iota
	// applyPeerGone — the contact was removed while the message was being
	// decrypted. There is nothing to show and nothing to recover.
	applyPeerGone
	// applyStale — a row left this conversation while the message was being
	// decrypted. The counter is per-PEER, so it cannot say whether the
	// deleted row was this message or a different one — and dropping the
	// message on that ambiguity loses a live message whose id is already
	// through the dedup gate. The caller re-reads the conversation from the
	// database instead, which knows the answer exactly.
	applyStale
)

// recoverFromStaleApply rebuilds a peer from the database after a decrypted
// message could not be applied because the conversation moved under it, and
// reports whether the rebuild is complete.
//
// Everything the apply would have done is redone from the one authority that
// knows what survived the deletion: the preview and the last-online evidence
// through the reconciliation, the badge through its own re-derivation from
// delivery_status, and — only if the database still holds this message — the
// promotion and the receiver mapping for a file announcement. If the message
// was the row that was deleted, none of that happens, which is the
// distinction the peer-level counter could not make.
//
// false means the rebuild is INCOMPLETE: a read failed, so this message is
// neither on screen nor counted, and its id must go back out of the dedup
// gate for the header repair to find.
func (r *DMRouter) recoverFromStaleApply(peer domain.PeerIdentity, msg *DirectMessage) bool {
	if !r.updatePreviewFromStore(peer) {
		return false
	}
	if !r.repairBadgeFromStore(peer) {
		return false
	}

	// The stamp for the last step is taken BEFORE the question is asked, and
	// checked when the answer is used. A fresh stamp would be an answer
	// about the wrong moment: the contact can be removed, or this very
	// message deleted, while the query runs, and the work below would then
	// put a removed conversation back in the order and re-register a
	// transfer that was just cleaned up.
	commitStamp := r.peerStampOf(peer)
	stored, ok := r.messageStillStored(peer, msg)
	if !ok {
		return false
	}
	if !stored {
		r.notify(UIEventSidebarUpdated)
		return true
	}

	r.mu.Lock()
	_, alive := r.peers[peer]
	current := alive && r.stampIsCurrentLocked(peer, commitStamp)
	if current {
		// Promotion requires a LIVE row, not merely an unchanged stamp: a
		// peer with no row would be put back into peerOrder by it.
		r.promotePeerLocked(peer)
	}
	r.mu.Unlock()
	if !current {
		return false
	}
	r.registerFileReceiveForLivePeer(msg, peer, commitStamp)

	r.notify(UIEventSidebarUpdated)
	return true
}

// messageStillStored asks the database whether this message survived the
// deletion that made the apply stale. The second result is false when the
// question could not be answered at all.
func (r *DMRouter) messageStillStored(peer domain.PeerIdentity, msg *DirectMessage) (bool, bool) {
	if msg == nil || msg.ID == "" {
		return false, true
	}
	reader := r.chatHistory()
	if reader == nil {
		return false, true
	}
	ctx, cancel := context.WithTimeout(r.opContext(), 2*time.Second)
	statuses, err := reader.StoredMessageStatuses(ctx, []domain.MessageID{domain.MessageID(msg.ID)})
	cancel()
	if err != nil {
		log.Warn().Err(err).Str("peer", peer.String()).Msg("dm_router: could not tell whether the arriving message survived the deletion")
		return false, false
	}
	_, held := statuses[domain.MessageID(msg.ID)]
	return held, true
}

// setPeerPreviewLocked records msg as the newest row of the conversation and,
// when the peer is the one who wrote it, as presence evidence. Every path that
// learns of a new message goes through here: assigning Preview alone would
// leave LastIncomingAt behind on whichever path forgot it, and the symptom —
// a contact whose "last online" silently disappears — is invisible until
// someone reads the sidebar days later. Callers hold r.mu.
func (r *DMRouter) setPeerPreviewLocked(peer domain.PeerIdentity, msg DirectMessage) {
	if !r.tryEnsurePeerLocked(peer) {
		return
	}
	r.mergePreviewLocked(peer, ConversationPreview{
		PeerAddress: peer,
		Sender:      msg.Sender,
		Body:        msg.Body,
		Timestamp:   msg.Timestamp,
	})
	if msg.Sender == peer {
		r.noteIncomingAtLocked(peer, msg.Timestamp)
	}
}

// noteIncomingAtLocked advances the peer's last-incoming stamp. Monotone by
// construction: history arriving out of order (startup replay, a relayed
// message that took the long way) must not walk the evidence backwards.
// Callers hold r.mu.
//
// A timestamp in the future is refused. It comes from the message, which
// means it comes from the SENDER's clock — the one party that gains from
// looking recently online — and the node applies the same rule to the
// durable field. Displaying what we refuse to persist would let a forged
// date win in the sidebar precisely because it was too dishonest to store.
func (r *DMRouter) noteIncomingAtLocked(peer domain.PeerIdentity, at time.Time) {
	if peer.IsZero() || at.IsZero() || at.After(r.now()) {
		return
	}
	if !r.tryEnsurePeerLocked(peer) {
		return
	}
	state := r.peers[peer]
	current := state.LastIncomingAt
	if current.Valid() && !at.After(current.Time()) {
		return
	}
	state.LastIncomingAt = domain.TimeOf(at)
}

// markUnreadLocked records one unseen incoming message. Idempotent: the same
// id twice is one unread message, whichever path reports it. Callers hold
// r.mu.
func (r *DMRouter) markUnreadLocked(peer domain.PeerIdentity, messageID domain.MessageID) {
	// Emptiness is the only rejection: the set is keyed by whatever id the
	// message carries, and refusing anything that is not a v4 UUID would
	// silently drop the badge for a peer whose ids come from an older build
	// or another implementation.
	if peer.IsZero() || messageID == "" {
		return
	}
	if r.unreadIDs == nil {
		r.unreadIDs = make(map[domain.PeerIdentity]map[domain.MessageID]struct{})
	}
	ids, ok := r.unreadIDs[peer]
	if !ok {
		ids = make(map[domain.MessageID]struct{})
		r.unreadIDs[peer] = ids
	}
	if _, already := ids[messageID]; already {
		return
	}
	ids[messageID] = struct{}{}
	r.syncUnreadCountLocked(peer)
}

// dropUnreadLocked forgets messages that are no longer unread — read, or
// deleted. Callers hold r.mu.
func (r *DMRouter) dropUnreadLocked(peer domain.PeerIdentity, messageIDs ...domain.MessageID) {
	if len(messageIDs) > 0 {
		// Bumped whether or not the set held these ids, because the move
		// that matters happened in the DATABASE: mark-seen wrote `seen`, the
		// deletion removed the row. At startup the in-memory set is empty by
		// construction — the badge seed is the first thing to fill it and it
		// has not landed yet — so a bump conditional on in-memory contents
		// would silently do nothing exactly when the seed is in flight, and
		// the pre-mark-seen answer would badge messages the user just read.
		r.moveUnreadBackwardsLocked(peer)
	}
	for _, id := range messageIDs {
		delete(r.unreadIDs[peer], id)
	}
	// The projection is republished even when the set was already empty: the
	// set is the value, the count is a view of it, and a view that can be off
	// is a badge nobody can explain.
	r.syncUnreadCountLocked(peer)
}

// clearUnreadLocked marks the whole conversation read. Callers hold r.mu.
func (r *DMRouter) clearUnreadLocked(peer domain.PeerIdentity) {
	// Unconditional, for the same reason as dropUnreadLocked: an empty set
	// in memory says nothing about what the database was just told.
	r.moveUnreadBackwardsLocked(peer)
	delete(r.unreadIDs, peer)
	r.syncUnreadCountLocked(peer)
}

// restoreUnreadLocked puts a previously cleared set back, for the optimistic
// clear whose background mark-seen failed. Callers hold r.mu.
func (r *DMRouter) restoreUnreadLocked(peer domain.PeerIdentity, ids map[domain.MessageID]struct{}) {
	if len(ids) == 0 {
		return
	}
	if _, alive := r.peers[peer]; !alive {
		// The conversation was removed while the mark-seen was in flight.
		// Restoring would leave ids nobody owns, and re-adding the contact
		// later would republish a badge for messages that no longer exist.
		return
	}
	if r.unreadIDs == nil {
		r.unreadIDs = make(map[domain.PeerIdentity]map[domain.MessageID]struct{})
	}
	// A UNION, not an assignment: messages may have arrived while the
	// optimistic clear was in flight, and replacing the set would drop them.
	current, ok := r.unreadIDs[peer]
	if !ok {
		current = make(map[domain.MessageID]struct{}, len(ids))
		r.unreadIDs[peer] = current
	}
	for id := range ids {
		current[id] = struct{}{}
	}
	r.syncUnreadCountLocked(peer)
}

// unreadSnapshotLocked copies the peer's unread set. Callers hold r.mu.
func (r *DMRouter) unreadSnapshotLocked(peer domain.PeerIdentity) map[domain.MessageID]struct{} {
	ids := r.unreadIDs[peer]
	if len(ids) == 0 {
		return nil
	}
	out := make(map[domain.MessageID]struct{}, len(ids))
	for id := range ids {
		out[id] = struct{}{}
	}
	return out
}

// syncUnreadCountLocked republishes the set size on the peer state the UI
// reads. The count is a projection; the set is the value. Callers hold r.mu.
func (r *DMRouter) syncUnreadCountLocked(peer domain.PeerIdentity) {
	count := len(r.unreadIDs[peer])
	// The empty-set cleanup runs even for a peer that no longer has a row:
	// leaving an orphan set behind is how a re-added contact inherits a badge
	// for messages that are long gone.
	if count == 0 {
		delete(r.unreadIDs, peer)
	}
	if state, ok := r.peers[peer]; ok {
		state.Unread = count
	}
}

// peerRefreshLock returns the per-peer recomputation mutex, creating it on
// first use. Held across SQL I/O, so it is never taken while r.mu is held.
func (r *DMRouter) peerRefreshLock(peer domain.PeerIdentity) (*sync.Mutex, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, alive := r.peers[peer]; !alive {
		return nil, false
	}
	if r.peerRefreshMu == nil {
		r.peerRefreshMu = make(map[domain.PeerIdentity]*sync.Mutex)
	}
	lock, ok := r.peerRefreshMu[peer]
	if !ok {
		lock = &sync.Mutex{}
		r.peerRefreshMu[peer] = lock
	}
	return lock, true
}

// tryEnsurePeerLocked creates the sidebar row if it is missing and reports
// whether the row exists afterwards.
//
// It can REFUSE: while a removal is running, the row it just dropped must not
// be born again behind it, or the cleanup that follows leaves the
// conversation half-deleted — a sidebar row with no history and transfers
// nobody will clean up. Every caller has to look at the answer; the ones that
// wrote to r.peers[peer] straight after the old void helper would panic on
// the refusal, and the ones that only added to unreadIDs or peerOrder would
// leave that state orphaned. Callers hold r.mu.
func (r *DMRouter) tryEnsurePeerLocked(peer domain.PeerIdentity) bool {
	if r.removals.removing(peer) {
		return false
	}
	if _, ok := r.peers[peer]; !ok {
		r.peers[peer] = &RouterPeerState{}
		r.peerOrder = append(r.peerOrder, peer)
	}
	return true
}

func (r *DMRouter) promotePeerLocked(peer domain.PeerIdentity) {
	if peer.IsZero() {
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

// RouterSnapshot is assembled from two independently-cached halves so a
// notify only pays for the half that changed:
//
//   - the DM half (dmSnapshotPart: peers / peerOrder / activeMessages),
//     refreshed by buildDMPartLocked on DM-data events;
//   - the monitor-owned NodeStatus half (cachedNS), refreshed by
//     refreshNodeStatusLocked on status events.
//
// composeSnapshotLocked stitches the cached halves with the live cheap
// scalars into the immutable RouterSnapshot stored for lock-free UI
// reads. The previous code deep-rebuilt BOTH halves on EVERY notify,
// which profiling flagged as a major allocation source (status-only
// updates re-copying activeMessages/peers; DM updates re-deep-copying
// the whole NodeStatus).

// buildDMPartLocked clones the DM-owned collections into an immutable
// half. Allocates a fresh peers map (with per-entry *RouterPeerState
// clones) + peerOrder + activeMessages slices; called only when DM data
// actually changed. Also bumps dmGen: this is the only producer of a DM
// half, so the revision is moved here rather than at the two assignment
// sites (see the field).
func (r *DMRouter) buildDMPartLocked() dmSnapshotPart {
	r.dmGen++
	peersCopy := make(map[domain.PeerIdentity]*RouterPeerState, len(r.peers))
	for k, v := range r.peers {
		clone := *v
		peersCopy[k] = &clone
	}
	return dmSnapshotPart{
		Peers:          peersCopy,
		PeerOrder:      append([]domain.PeerIdentity(nil), r.peerOrder...),
		ActiveMessages: append([]DirectMessage(nil), r.activeMessages...),
	}
}

// refreshNodeStatusLocked re-reads the monitor-owned NodeStatus half.
// The monitor returns a deep copy, so the cached value is independent
// of live state and safe to share across composed snapshots. Called
// only on status-domain notifies.
func (r *DMRouter) refreshNodeStatusLocked() {
	if r.statusMonitor != nil {
		r.cachedNS = r.statusMonitor.NodeStatus()
	}
}

// composeSnapshotLocked assembles a RouterSnapshot from the two cached
// halves. Pure field assembly — it shares the halves' backing arrays
// (both immutable) rather than deep-copying, so this is cheap regardless
// of which half last changed. CacheReady is recomputed here because the
// conversation cache is guarded by its own mutex.
func (r *DMRouter) composeSnapshotLocked(gen uint64) RouterSnapshot {
	dm := r.cachedDMPart
	return RouterSnapshot{
		Generation: gen,
		// The DM half's own revision, so a consumer whose cache is derived
		// from Peers / PeerOrder / ActiveMessages can skip the ~2-3 notifies
		// a second that change neither.
		DMGeneration: r.dmGen,
		// Cheap scalars read live under r.mu so a status-only notify
		// (which refreshes neither half's collections) still reflects a
		// sendStatus / selection flip.
		ActivePeer:  r.activePeer,
		PeerClicked: r.peerClicked,
		SendStatus:  r.sendStatus,
		MyAddress:   r.client.Address(),
		// Expensive collections shared from the cached DM half.
		Peers:          dm.Peers,
		PeerOrder:      dm.PeerOrder,
		ActiveMessages: dm.ActiveMessages,
		CacheReady:     !r.activePeer.IsZero() && r.cache.MatchesPeer(r.activePeer),
		// Monitor-owned half, refreshed only on status notifies.
		NodeStatus: r.cachedNS,
	}
}

// buildSnapshotLocked rebuilds BOTH halves and composes — the full
// rebuild used at construction (initial seed). Per-event notifies use
// the targeted single-half refresh in notify().
func (r *DMRouter) buildSnapshotLocked(gen uint64) RouterSnapshot {
	r.cachedDMPart = r.buildDMPartLocked()
	r.refreshNodeStatusLocked()
	return r.composeSnapshotLocked(gen)
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

	// Pointers — clone the pointed-to struct (nil = "node does not
	// support this command yet" / "not sampled yet"). The sampler/probe
	// always assign a fresh pointer rather than mutating in place, but
	// cloning keeps the snapshot fully independent and matches the
	// AggregateStatus contract.
	if src.AggregateStatus != nil {
		clone := *src.AggregateStatus
		dst.AggregateStatus = &clone
	}
	if src.ResourceUsage != nil {
		clone := *src.ResourceUsage
		dst.ResourceUsage = &clone
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
	// Refresh ONLY the snapshot half whose domain changed, then
	// recompose. The previous code deep-rebuilt both halves on every
	// notify; profiling showed that as a major allocation source
	// (status-only updates re-copying activeMessages/peers, DM updates
	// re-deep-copying the whole NodeStatus). Once composed the snapshot
	// is immutable and stored via atomic.Pointer for lock-free UI reads.
	r.mu.Lock()
	gen := r.snapGen.Add(1)
	switch eventType {
	case UIEventStatusUpdated:
		// NodeStatus-domain change (resource sample, peer-health delta,
		// aggregate counters). DM data is unchanged — reuse cachedDMPart.
		r.refreshNodeStatusLocked()
	case UIEventBeep:
		// Pure UI signal, no data change — reuse both cached halves.
	default:
		// DM-data change (UIEventMessagesUpdated / UIEventSidebarUpdated).
		// NodeStatus is unchanged — reuse cachedNS.
		r.cachedDMPart = r.buildDMPartLocked()
	}
	snap := r.composeSnapshotLocked(gen)
	r.snapCache.Store(&routerSnapshotCache{gen: gen, snap: snap})
	r.mu.Unlock()

	r.emitUIEvent(eventType)
}

// NotifyStatusDomainChanged is the lightweight analogue of
// NotifyStatusChanged: instead of deep-copying the whole NodeStatus, it
// patches just the one field the monitor reports as changed and recomposes.
// Profiling flagged deepCopyNodeStatus (PeerHealth ~19MB, KnownIDs ~11MB per
// copy) as the dominant allocator under a status-event storm on a large mesh;
// resource/traffic/route/identity/aggregate events each touch a single field,
// so re-cloning the rest is pure waste.
//
// The monitor field snapshot is taken INSIDE r.mu so read + patch + store is
// one atomic update ordered against the full NotifyStatusChanged path. Both
// take r.mu first and the monitor lock second (NodeStatus / *Snapshot), so a
// fresher cachedNS published by a full rebuild between the snapshot and the
// patch can never be clobbered by a stale field — which for PeerHealth would
// roll back not just byte counters but State/Connected/conn rows. The lock
// order (r.mu outer, monitor lock inner) matches the full path, so there is no
// inversion: the monitor never holds its lock while calling back into the
// router. An unknown domain falls back to a full refresh so a forgotten wiring
// degrades to correct-but-slow rather than publishing a stale snapshot.
//
// Each patch replaces the field's reference (pointer/slice/map header) rather
// than mutating it, so already-composed snapshots that share the old backing
// store are untouched (copy-on-write); the pre-cloned snapshots from the
// monitor keep the new store independent of monitor-owned memory.
func (r *DMRouter) NotifyStatusDomainChanged(d NodeStatusDomain) {
	if r.statusMonitor == nil {
		return
	}

	r.mu.Lock()
	gen := r.snapGen.Add(1)
	switch d {
	case NodeStatusDomainResourceUsage:
		r.cachedNS.ResourceUsage = r.statusMonitor.ResourceUsageSnapshot()
	case NodeStatusDomainPeerHealth:
		r.cachedNS.PeerHealth = r.statusMonitor.PeerHealthSnapshot()
	case NodeStatusDomainReachableIDs:
		r.cachedNS.ReachableIDs = r.statusMonitor.ReachableIDsSnapshot()
	case NodeStatusDomainPresence:
		r.cachedNS.Contacts = r.statusMonitor.Contacts()
	case NodeStatusDomainKnownIDs:
		r.cachedNS.KnownIDs = r.statusMonitor.KnownIDsSnapshot()
	case NodeStatusDomainAggregate:
		r.cachedNS.AggregateStatus, r.cachedNS.CheckedAt = r.statusMonitor.AggregateStatusSnapshot()
	default:
		r.refreshNodeStatusLocked()
	}
	snap := r.composeSnapshotLocked(gen)
	r.snapCache.Store(&routerSnapshotCache{gen: gen, snap: snap})
	r.mu.Unlock()

	r.emitUIEvent(UIEventStatusUpdated)
}

// emitUIEvent delivers a UIEvent to the UI channel, falling back to a
// bounded set of deferred retry goroutines when the channel is momentarily
// full so no event type is silently lost. Shared by notify() and the
// resource-only fast path.
func (r *DMRouter) emitUIEvent(eventType UIEventType) {
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
