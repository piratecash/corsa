package desktop

import (
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
	"gioui.org/x/explorer"
)

type Window struct {
	router   *service.DMRouter
	client   *service.DesktopClient
	eventBus *ebus.Bus
	cmdTable *rpc.CommandTable
	runtime  *NodeRuntime
	prefs    *Preferences
	theme    *material.Theme
	ops      op.Ops

	recipientEditor      widget.Editor
	identitySearchEditor widget.Editor
	messageEditor        widget.Editor
	focusComposerPending bool
	contactsList         widget.List
	chatList             widget.List
	consoleButton        widget.Clickable
	updateButton         widget.Clickable
	sendButton           widget.Clickable
	copyIdentityButton   widget.Clickable
	languageToggle       widget.Clickable
	languageOptions      map[string]*widget.Clickable
	recipientButtons     map[domain.PeerIdentity]*widget.Clickable
	recipientRightClick  map[domain.PeerIdentity]*rightClickState
	messageSelectables   map[string]*widget.Selectable
	sendStatusSelectable widget.Selectable
	lastChatPeer         domain.PeerIdentity
	language             string
	showLanguageMenu     bool
	consoleOpen          bool

	// Global cursor tracking for context menu positioning.
	cursorTracker int // tag for window-level pointer events
	lastCursorPos image.Point

	// Context menu state for right-click on recipient buttons.
	contextMenuPeer         domain.PeerIdentity // fingerprint of the peer whose context menu is open
	contextMenuPos          image.Point
	ctxMenuCopy             widget.Clickable
	ctxMenuDelete           widget.Clickable
	ctxMenuDeleteConfirm    widget.Clickable
	ctxMenuDeleteCancel     widget.Clickable
	ctxMenuClearChat        widget.Clickable // "Delete chat for everyone" — bulk wipe both sides
	ctxMenuClearChatConfirm widget.Clickable
	ctxMenuClearChatCancel  widget.Clickable
	ctxMenuAlias            widget.Clickable
	ctxMenuAliasSave        widget.Clickable
	ctxMenuAliasCancel      widget.Clickable
	showDeleteConfirm       bool // true when "Delete identity" confirmation step is shown
	showClearChatConfirm    bool // true when "Delete chat for everyone" confirmation step is shown
	showAliasEditor         bool // true when alias input is shown
	aliasEditor             widget.Editor

	// Context menu state for right-click on chat messages.
	msgContextMsg *service.DirectMessage // message whose context menu is open (nil = closed)
	msgContextPos image.Point
	msgCtxReply   widget.Clickable
	msgCtxCopy    widget.Clickable
	msgCtxDelete  widget.Clickable
	msgRightClick map[string]*rightClickState // keyed by message ID

	// Reply state: when the user replies to a message, we remember the
	// target UUID and show a quote preview above the composer.
	replyToMsg        *service.DirectMessage // message being replied to (nil = no active reply)
	replyCancelButton widget.Clickable

	// msgCacheByID stores message metadata for O(1) lookup when rendering
	// reply quotes (body, sender, timestamp). Rebuilt when the snapshot
	// generation changes — this catches every mutation including body
	// edits, ReplyTo updates, and same-shape conversation reloads that
	// the old count+first/last heuristic missed.
	msgCacheByID map[string]cachedMsg
	msgCacheGen  uint64 // snapshot Generation when cache was built

	// replyQuoteTags maps message IDs to stable pointer event tags for
	// click-to-scroll behavior on reply quotes.
	replyQuoteTags map[string]*widget.Clickable

	// scrollToMsgID is set when the user clicks a reply quote. The actual
	// scroll is deferred to the next frame's layout() — applying Position
	// changes inside list.Layout() is unreliable because the list overwrites
	// them during its own position computation.
	scrollToMsgID string
	// scrollClickY stores the cursor Y position relative to the chat
	// viewport at the moment the user clicked the reply quote.
	scrollClickY int
	// chatViewportH stores the chat list viewport height (pixels) from
	// the most recent layout pass, used for cursor-relative scroll math.
	chatViewportH int
	// chatCursorY tracks the cursor Y relative to the chat viewport,
	// updated by a pointer tracker scoped to layoutConversation.
	chatCursorY   int
	chatCursorTag int // stable tag for the chat-area pointer tracker

	// File attachment state: when the user picks a file via the native dialog,
	// these fields hold the selected file path and metadata until Send is pressed.
	// attachedFile and attachGeneration are only mutated on the UI goroutine —
	// background goroutines deliver the selected path via pendingAttach
	// (buffered channel, drained in handlePendingActions) and call
	// window.Invalidate() to trigger a frame.
	//
	// attachGeneration is a monotonic counter bumped every time the composer's
	// attachment slot transitions (new user pick, explicit cancel). triggerFileSend
	// captures the current generation as sendGen and embeds it in restoreAttach's
	// closure; the drain in handlePendingActions only honors a restore message
	// when its generation still matches attachGeneration. This prevents a late
	// async-send-failure from overwriting a newer user-selected attachment
	// through the shared single-slot channel.
	attachButton     widget.Clickable
	attachedFile     string // absolute path to the selected file (empty = no attachment)
	attachGeneration uint64 // bumped on every new pick / cancel to invalidate stale restore deliveries
	attachCancelBtn  widget.Clickable
	pendingAttach    chan pendingAttachMsg // delivers attach updates from background goroutines → UI goroutine

	// File download buttons for incoming file cards (keyed by message ID).
	fileDownloadBtns       map[string]*widget.Clickable
	fileCancelDownloadBtns map[string]*widget.Clickable
	fileRestartBtns        map[string]*widget.Clickable

	// Image thumbnail cache for file transfer preview in chat bubbles.
	thumbCache     thumbnailCache
	thumbClickBtns map[string]*widget.Clickable // keyed by message ID — click to open image

	// File action buttons for completed transfers (keyed by message ID).
	fileRevealBtns    map[string]*widget.Clickable // "Show in Folder"
	fileOpenBtns      map[string]*widget.Clickable // "Open" with system viewer
	fileRowDeleteBtns map[string]*widget.Clickable // "Delete" inline with Reveal/Open (per-row, separate from msgCtxDelete which is the context-menu single)

	// Native file dialog via gioui.org/x/explorer. Initialized once in Run()
	// together with the app.Window. ChooseFile is blocking and must be called
	// from a separate goroutine; ListenEvents must be called in the event loop.
	fileExplorer *explorer.Explorer

	snap service.RouterSnapshot

	consoleMu sync.Mutex

	window *app.Window

	transferInvalidateMu      sync.Mutex
	transferInvalidatePending bool
}

// pendingAttachMsg is the payload delivered over the pendingAttach channel
// from background goroutines to the UI goroutine drain. Two producers write
// to this channel:
//
//   - triggerFileAttach (user picked a new file through the native dialog):
//     restore=false. User picks unconditionally replace the composer
//     attachment and bump the generation counter.
//
//   - restoreAttach inside triggerFileSend (async send failure — put the
//     file back so the user can retry): restore=true with the generation
//     captured at send start. The drain only honors the restore if
//     generation still matches Window.attachGeneration. A mismatch means
//     the user has already picked a different file or cancelled the
//     attachment, and the stale restore must be dropped to avoid
//     overwriting the newer selection.
type pendingAttachMsg struct {
	path       string
	restore    bool
	generation uint64
}

// restoreShouldYield decides whether an incoming message must yield its slot
// in the pendingAttach channel to an already-queued message. It is pure and
// reads no shared state so it is safe to call from a background goroutine
// during the convergent drain loop in restoreAttach.
//
// Yield rules:
//   - A user pick (existing.restore == false) always wins over any restore.
//     The user is actively selecting a file and the UI must reflect that.
//   - An adopted user pick (incoming.restore == false, carried forward from
//     a previous drain iteration) never yields — once the loop adopts a
//     user pick as its candidate, it dominates all subsequent comparisons.
//   - Between two restore messages, the higher-generation one wins. Queued
//     restores from later sends supersede older ones; re-queueing the newer
//     message preserves the fix for cross-send failure ordering.
func restoreShouldYield(existing, incoming pendingAttachMsg) bool {
	if !incoming.restore {
		// incoming is a user pick (or an adopted user pick from a prior
		// drain iteration) — never yield.
		return false
	}
	if !existing.restore {
		return true
	}
	return existing.generation >= incoming.generation
}

const (
	languageButtonWidth = 76
	languageMenuWidth   = 220
	languageOverlayTop  = 58
	windowInset         = 24
	languageMenuHeight  = 316
)

// newAppTheme creates a fresh material.Theme with the application colour scheme.
// Each window must own its own Theme because the embedded text.Shaper uses an
// unsynchronised map cache and is therefore not safe for concurrent use.
func newAppTheme() *material.Theme {
	theme := material.NewTheme()
	theme.Bg = color.NRGBA{R: 18, G: 21, B: 26, A: 255}
	theme.Fg = color.NRGBA{R: 235, G: 239, B: 244, A: 255}
	theme.ContrastBg = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	theme.ContrastFg = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	return theme
}

func NewWindow(client *service.DesktopClient, router *service.DMRouter, eventBus *ebus.Bus, cmdTable *rpc.CommandTable, runtime *NodeRuntime, prefs *Preferences) *Window {
	theme := newAppTheme()

	language := normalizeLanguage(client.Language())
	if prefs != nil && prefs.Language != "" {
		language = normalizeLanguage(prefs.Language)
	}

	w := &Window{
		router:              router,
		client:              client,
		eventBus:            eventBus,
		cmdTable:            cmdTable,
		runtime:             runtime,
		prefs:               prefs,
		theme:               theme,
		language:            language,
		languageOptions:     make(map[string]*widget.Clickable),
		recipientButtons:    make(map[domain.PeerIdentity]*widget.Clickable),
		recipientRightClick: make(map[domain.PeerIdentity]*rightClickState),
		messageSelectables:  make(map[string]*widget.Selectable),
		msgRightClick:       make(map[string]*rightClickState),
		contactsList:        widget.List{List: layout.List{Axis: layout.Vertical}},
		chatList:            widget.List{List: layout.List{Axis: layout.Vertical, ScrollToEnd: true}},
		pendingAttach:       make(chan pendingAttachMsg, 1),
	}
	w.aliasEditor.SingleLine = true
	w.aliasEditor.Submit = true
	return w
}

func (w *Window) Run() error {
	go func() {
		defer crashlog.DeferRecover()

		window := new(app.Window)
		w.window = window
		w.fileExplorer = explorer.NewExplorer(window)
		window.Option(
			app.Title(w.t("app.title")+" — "+w.t("app.subtitle")),
			app.Size(unit.Dp(1100), unit.Dp(1100)),
		)

		w.startPolling(window)

		if err := w.loop(window); err != nil {
			panic(err)
		}
		os.Exit(0)
	}()

	app.Main()
	return nil
}

// uiHeartbeatInterval is the periodic fallback that guarantees the UI
// redraws even when all UIEvents are dropped during a burst. Without
// this, a sustained ebus write flood can exhaust the notify() retry
// budget, leaving the Gio event loop with no pending Invalidate() calls
// — resulting in a permanent freeze. 2 seconds is imperceptible for
// status updates while keeping CPU cost negligible.
const uiHeartbeatInterval = 2 * time.Second

func (w *Window) startPolling(window *app.Window) {
	w.router.Start()

	// Subscribe to terminal message_delete outcomes so the UI can
	// surface peer rejection (denied / immutable) and retry-budget
	// abandonment, instead of always showing the optimistic
	// "Deleting…" / "Deleted." pair from handleMsgContextMenuActions.
	// The synchronous SendMessageDelete return only reports local
	// errors; the wire-side outcome arrives asynchronously through
	// this event.
	if w.eventBus != nil {
		w.eventBus.Subscribe(ebus.TopicMessageDeleteCompleted, func(outcome ebus.MessageDeleteOutcome) {
			w.handleMessageDeleteOutcome(outcome)
		})
		// Conversation-wide wipe (sidebar "Delete chat for everyone")
		// reaches its terminal status the same way: the UI runs the
		// two-phase BeginConversationDelete + CompleteConversationDelete
		// which only report local errors, while the wire-side
		// outcome arrives later via this event.
		w.eventBus.Subscribe(ebus.TopicConversationDeleteCompleted, func(outcome ebus.ConversationDeleteOutcome) {
			w.handleConversationDeleteOutcome(outcome)
		})
	}

	go func() {
		heartbeat := time.NewTicker(uiHeartbeatInterval)
		defer heartbeat.Stop()
		events := w.router.Subscribe()

		for {
			select {
			case ev, ok := <-events:
				if !ok {
					return
				}
				if ev.Type == service.UIEventBeep {
					go systemBeep()
				}
				if w.window != nil {
					w.window.Invalidate()
				}
			case <-heartbeat.C:
				// Periodic recovery: ensure the UI redraws at least
				// every uiHeartbeatInterval even if all event-driven
				// Invalidate() calls were lost to channel overflow.
				if w.window != nil {
					w.window.Invalidate()
				}
			}
		}
	}()
}

func (w *Window) loop(window *app.Window) error {
	for {
		e := window.Event()
		w.fileExplorer.ListenEvents(e)
		switch e := e.(type) {
		case app.DestroyEvent:
			return e.Err
		case app.FrameEvent:
			gtx := app.NewContext(&w.ops, e)
			w.layout(gtx)
			e.Frame(gtx.Ops)
		}
	}
}

func (w *Window) layout(gtx layout.Context) layout.Dimensions {
	w.snap = w.router.Snapshot()
	w.rebuildMsgCache()
	w.applyDeferredScroll()
	w.resetReplyOnPeerChange()
	w.handlePendingActions()
	w.handleActions(gtx)
	fill(gtx, color.NRGBA{R: 12, G: 15, B: 20, A: 255})

	// Track cursor position at window level for accurate context menu placement.
	defer clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, &w.cursorTracker)
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: &w.cursorTracker,
			Kinds:  pointer.Move | pointer.Press | pointer.Drag,
		})
		if !ok {
			break
		}
		if pe, ok := ev.(pointer.Event); ok {
			w.lastCursorPos = pe.Position.Round()
		}
	}

	inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(6), Right: unit.Dp(6)}
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(w.layoutHeader),
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Flexed(1, w.layoutMain),
				)
			})
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if !w.showLanguageMenu {
				return layout.Dimensions{}
			}
			return w.layoutLanguageOverlay(gtx)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if w.contextMenuPeer == "" {
				return layout.Dimensions{}
			}
			return w.layoutContextMenuOverlay(gtx)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if w.msgContextMsg == nil {
				return layout.Dimensions{}
			}
			return w.layoutMsgContextMenuOverlay(gtx)
		}),
	)
}

// resetReplyOnPeerChange clears reply and message-context-menu state when
// the active conversation changes. This runs every frame so that even
// switching to an empty chat (where no message bubbles are rendered)
// properly discards stale reply references from the previous peer.
func (w *Window) resetReplyOnPeerChange() {
	peer := w.snap.ActivePeer
	if peer == w.lastChatPeer {
		return
	}
	w.replyToMsg = nil
	w.msgContextMsg = nil
	w.scrollToMsgID = ""
	// lastChatPeer and per-message widget caches (messageSelectables,
	// msgRightClick) are updated lazily in messageSelectable() when the
	// first bubble is rendered, which also handles the non-empty case.
}

// Gio widgets are single-threaded — mutations MUST happen on the UI goroutine.
func (w *Window) handlePendingActions() {
	pa := w.router.ConsumePendingActions()

	if pa.ClearEditor {
		w.messageEditor.SetText("")
	}
	if pa.ClearReply {
		w.replyToMsg = nil
	}
	if pa.ScrollToEnd {
		w.chatList.Position.BeforeEnd = false
	}
	if pa.RecipientText != "" {
		w.recipientEditor.SetText(string(pa.RecipientText))
	}

	// Drain attachment update delivered by a background goroutine
	// (triggerFileAttach for user picks, restoreAttach for async send
	// failures). This keeps all w.attachedFile / w.attachGeneration
	// mutations on the UI goroutine.
	select {
	case msg := <-w.pendingAttach:
		w.applyPendingAttach(msg)
	default:
	}
}

// applyPendingAttach commits a pendingAttachMsg to the composer. Must be
// called on the UI goroutine.
//
// User picks always win: they unconditionally replace the current attachment
// and bump attachGeneration, invalidating any in-flight restore from an
// earlier send. Restores are conditional: they are honored only when the
// generation they captured at send start still matches attachGeneration AND
// the composer slot is currently empty. Either condition failing means the
// user has already moved on (picked a different file, picked the same file
// again, or explicitly cancelled), and replaying the old path would clobber
// the newer state.
func (w *Window) applyPendingAttach(msg pendingAttachMsg) {
	if !msg.restore {
		w.attachedFile = msg.path
		w.attachGeneration++
		return
	}
	if msg.generation != w.attachGeneration || w.attachedFile != "" {
		return
	}
	w.attachedFile = msg.path
}

func (w *Window) handleActions(gtx layout.Context) {
	for w.languageToggle.Clicked(gtx) {
		w.showLanguageMenu = !w.showLanguageMenu
	}

	for w.consoleButton.Clicked(gtx) {
		w.openConsoleWindow()
	}

	for w.updateButton.Clicked(gtx) {
		openBrowser("https://github.com/piratecash/corsa/releases")
	}

	for w.sendButton.Clicked(gtx) {
		w.triggerSend()
	}

	for w.attachButton.Clicked(gtx) {
		w.triggerFileAttach()
	}

	for w.attachCancelBtn.Clicked(gtx) {
		w.attachedFile = ""
		// Bump generation so that any in-flight restoreAttach from a
		// previously failing send is rejected by the drain — the user
		// explicitly dismissed the attachment and must not see it
		// reappear later.
		w.attachGeneration++
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	w.handleMessageSubmitShortcut(gtx)

	for w.copyIdentityButton.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(string(w.snap.MyAddress))),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	w.handleContextMenuActions(gtx)
	w.handleMsgContextMenuActions(gtx)
	w.handleReplyCancel(gtx)
}

func (w *Window) handleContextMenuActions(gtx layout.Context) {
	if w.contextMenuPeer == "" {
		return
	}

	if w.ctxMenuCopy.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(string(w.contextMenuPeer))),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		w.contextMenuPeer = ""
		w.showDeleteConfirm = false
		w.showClearChatConfirm = false
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuAlias.Clicked(gtx) {
		w.showAliasEditor = true
		existing := ""
		if w.prefs != nil {
			existing = w.prefs.Alias(w.contextMenuPeer)
		}
		w.aliasEditor.SetText(existing)
		gtx.Execute(key.FocusCmd{Tag: &w.aliasEditor})
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuAliasSave.Clicked(gtx) {
		alias := strings.TrimSpace(w.aliasEditor.Text())
		if w.prefs != nil {
			w.prefs.SetAlias(w.contextMenuPeer, alias)
			_ = w.prefs.Save()
		}
		w.contextMenuPeer = ""
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuAliasCancel.Clicked(gtx) {
		w.showAliasEditor = false
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	for w.ctxMenuDelete.Clicked(gtx) {
		w.showDeleteConfirm = true
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuDeleteConfirm.Clicked(gtx) {
		peer := w.contextMenuPeer
		w.contextMenuPeer = ""
		w.showDeleteConfirm = false

		// Capture the ordered recipient list before deletion so we can
		// pick the nearest neighbor for auto-selection in the UI layer.
		recipients := w.snapRecipients()
		removedIdx := -1
		for i, r := range recipients {
			if r == peer {
				removedIdx = i
				break
			}
		}

		wasActive, err := w.router.RemovePeer(peer)
		if err != nil {
			w.router.SetSendStatus(w.t("status.delete_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		// Remove saved alias together with the identity.
		if w.prefs != nil {
			w.prefs.SetAlias(peer, "")
			_ = w.prefs.Save()
		}

		if wasActive && removedIdx >= 0 && len(recipients) > 1 {
			remaining := make([]domain.PeerIdentity, 0, len(recipients)-1)
			remaining = append(remaining, recipients[:removedIdx]...)
			remaining = append(remaining, recipients[removedIdx+1:]...)
			nextIdx := removedIdx
			if nextIdx >= len(remaining) {
				nextIdx = len(remaining) - 1
			}
			w.router.SelectPeer(remaining[nextIdx])
			w.recipientEditor.SetText(string(remaining[nextIdx]))
			w.focusComposerPending = true
		}

		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuDeleteCancel.Clicked(gtx) {
		w.showDeleteConfirm = false
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	// "Delete chat for everyone" — opens its own confirm step. Mirrors
	// the "Delete identity" two-click flow above; kept as a separate
	// state flag so the user can see at a glance which destructive
	// action they are about to confirm (the two share the menu card
	// surface).
	for w.ctxMenuClearChat.Clicked(gtx) {
		// Defensive offline gate. The menu item is rendered as a
		// non-clickable disabled pill when the peer is offline (see
		// layoutContextMenuItems), but routing the gate through the
		// click handler too keeps the state machine consistent in
		// case a future menu refactor wires the Clickable
		// unconditionally — issuing a wipe to an offline peer would
		// burn the entire retry budget waiting for an ack that
		// cannot come.
		if !w.peerOnline(w.contextMenuPeer) {
			return
		}
		w.showClearChatConfirm = true
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuClearChatConfirm.Clicked(gtx) {
		peer := w.contextMenuPeer
		w.contextMenuPeer = ""
		w.showClearChatConfirm = false

		// Re-check peerOnline at confirm time. The menu item gates
		// at open time, but the peer may have gone offline between
		// the user clicking the menu entry and clicking Yes —
		// dispatching a wipe to an offline peer would burn the
		// retry budget waiting for an ack that cannot come, and
		// pessimistic ordering means the local rows would stay
		// alive on the eventual abandonment, leaving the user
		// staring at a chat that "did nothing". Surfacing the
		// offline status as a status-bar message is more honest
		// than silent abandonment 5 minutes later.
		if !w.peerOnline(peer) {
			w.router.SetSendStatus(w.t("status.clear_chat_peer_offline"))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		w.dispatchConversationDeleteAsync(peer)

		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	for w.ctxMenuClearChatCancel.Clicked(gtx) {
		w.showClearChatConfirm = false
		if w.window != nil {
			w.window.Invalidate()
		}
	}
}

func (w *Window) handleMessageSubmitShortcut(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(
			key.Filter{Focus: &w.messageEditor, Name: key.NameEnter, Optional: key.ModShift},
			key.Filter{Focus: &w.messageEditor, Name: key.NameReturn, Optional: key.ModShift},
		)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		if ke.Modifiers.Contain(key.ModShift) {
			continue
		}
		w.triggerSend()
	}
}

func (w *Window) triggerSend() {
	to := domain.PeerIdentity(strings.TrimSpace(string(w.snap.ActivePeer)))
	if to == "" {
		to = domain.PeerIdentity(strings.TrimSpace(w.recipientEditor.Text()))
	}

	// Synchronous composer barrier while a conversation_delete is
	// in flight for this peer. The service layer also rejects the
	// send via ErrConversationDeleteInflight (see SendMessage /
	// SendFileAnnounce), but checking here saves the file-attach
	// path from clearing the attachment and spinning up
	// prepareFileForTransmit only to fail later with a generic
	// "file prepare failed" status that does not explain why.
	if to != "" && w.router.IsConversationDeletePending(to) {
		w.router.SetSendStatus(w.t("status.compose_blocked_during_wipe"))
		return
	}

	// File attachment takes priority: if a file is attached, send file_announce DM.
	if w.attachedFile != "" {
		w.triggerFileSend(to)
		return
	}

	body := strings.TrimSpace(w.messageEditor.Text())
	if body == "" {
		return
	}
	outgoing := domain.OutgoingDM{Body: body}
	if w.replyToMsg != nil {
		outgoing.ReplyTo = domain.MessageID(w.replyToMsg.ID)
	}
	// replyToMsg is cleared by handlePendingActions (ClearReply) after the
	// send succeeds. On failure the editor text is preserved and the reply
	// context stays intact so the user can retry without losing the quote.
	if err := w.router.SendMessage(to, outgoing); err != nil {
		// Outgoing barrier while a conversation_delete is in
		// flight to this peer — render a localised hint so the
		// user understands the input is intentionally blocked
		// until the wipe terminates, not a transient transport
		// failure.
		if errors.Is(err, service.ErrConversationDeleteInflight) {
			w.router.SetSendStatus(w.t("status.compose_blocked_during_wipe"))
			return
		}
		w.router.SetSendStatus(w.t("status.send_failed", err.Error()))
	}
}

// triggerFileAttach opens the native file picker dialog via Gio explorer
// in a background goroutine (ChooseFile is blocking). The selected path
// is delivered to the UI goroutine via pendingAttach channel, drained in
// handlePendingActions — Window fields are never mutated from the
// background goroutine.
func (w *Window) triggerFileAttach() {
	if w.fileExplorer == nil {
		return
	}
	go func() {
		rc, err := w.fileExplorer.ChooseFile()
		if err != nil {
			// User cancelled or platform error — silently ignore cancel.
			return
		}
		defer func() { _ = rc.Close() }()

		// On desktop platforms the returned io.ReadCloser is *os.File,
		// which gives us the full path needed for SHA-256 hashing,
		// filename extraction, and copy to transmit directory.
		f, ok := rc.(*os.File)
		if !ok {
			w.router.SetSendStatus(w.t("file.prepare_failed", "unsupported platform"))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		// Deliver the path to the UI goroutine via buffered channel.
		// User picks are authoritative: they always win over any stale
		// queued message (previous pick the UI hasn't drained yet, or a
		// pending restore from an earlier failing send) — the drain
		// unconditionally applies user picks and bumps the generation,
		// invalidating any in-flight restore.
		msg := pendingAttachMsg{path: f.Name(), restore: false}
		select {
		case w.pendingAttach <- msg:
		default:
			// Channel full — drain stale value, then send the new one.
			select {
			case <-w.pendingAttach:
			default:
			}
			w.pendingAttach <- msg
		}
		if w.window != nil {
			w.window.Invalidate()
		}
	}()
}

// triggerFileSend prepares the attached file and sends a file_announce DM.
// Any text in the message editor is included as the file caption (user-visible
// description alongside the file card).
func (w *Window) triggerFileSend(to domain.PeerIdentity) {
	if to == "" {
		return
	}
	srcPath := w.attachedFile
	caption := w.messageEditor.Text() // capture before clearing
	// Capture the attach generation BEFORE clearing. triggerFileSend does
	// not bump the generation — it hands ownership of the current slot to
	// the send goroutine, which may later restore if the send fails. Any
	// user action after this point (new pick, cancel) bumps the generation
	// and invalidates the pending restore.
	sendGen := w.attachGeneration
	w.attachedFile = "" // clear immediately so user cannot double-send
	w.router.SetSendStatus(w.t("file.sending"))

	// restoreAttach delivers the source path back to the UI goroutine via
	// pendingAttach so the user can retry without re-picking the file.
	// attachedFile is only mutated on the UI goroutine (handlePendingActions
	// drains the channel each frame), preserving the thread-safety invariant.
	//
	// The restore message carries sendGen, captured above. The UI drain
	// honours the restore only if attachGeneration still equals sendGen
	// (no user pick or cancel happened while the send was in flight) and
	// the composer slot is currently empty. Cross-send races are also
	// handled here: if the channel already holds a newer message (a
	// user pick or a restore from a later send), this restore yields the
	// slot rather than overwriting it.
	restoreAttach := func() {
		// Deliver the restore message to the UI goroutine via the
		// pendingAttach channel (capacity 1). The loop converges
		// because each iteration either succeeds (sends the winner)
		// or makes progress by draining one message and picking the
		// higher-priority survivor via restoreShouldYield.
		//
		// The previous drain-inspect-requeue approach had a TOCTOU
		// race: between draining the existing message and requeueing
		// it (non-blocking), another goroutine could fill the channel,
		// causing the requeue to silently drop the existing message.
		// When existing was a user pick, the composer ended up empty
		// even though the user had just selected a replacement.
		msg := pendingAttachMsg{path: srcPath, restore: true, generation: sendGen}
		for {
			select {
			case w.pendingAttach <- msg:
				return
			default:
			}
			// Channel full — drain and decide which message survives.
			select {
			case existing := <-w.pendingAttach:
				if restoreShouldYield(existing, msg) {
					// existing wins — adopt it as our message so the
					// next loop iteration requeues the winner instead
					// of the stale restore. No non-blocking requeue
					// that a concurrent producer could knock out.
					msg = existing
				}
				// else: our msg wins over the drained message — loop
				// will try to send msg on the next iteration.
			default:
				// Channel was drained between the two selects (UI
				// goroutine or another producer). Loop back — the
				// next iteration's send will likely succeed.
			}
		}
	}

	go func() {
		result, err := prepareFileForTransmit(
			w.client.StoreFileForTransmit,
			w.client.TransmitFileSize,
			w.client.RemoveUnreferencedTransmitFile,
			srcPath,
		)
		if err != nil {
			restoreAttach()
			w.router.SetSendStatus(w.t("file.prepare_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		outgoing, err := buildFileAnnounceOutgoing(result, caption)
		if err != nil {
			// Blob is stored but no token/mapping will ever reference it.
			w.client.RemoveUnreferencedTransmitFile(result.FileHash)
			restoreAttach()
			w.router.SetSendStatus(w.t("file.prepare_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		meta := domain.FileAnnouncePayload{
			FileHash:    result.FileHash,
			FileName:    result.FileName,
			FileSize:    result.FileSize,
			ContentType: result.ContentType,
		}
		if err := w.router.SendFileAnnounce(to, outgoing, meta, restoreAttach); err != nil {
			// SendFileAnnounce failed synchronously (e.g. fileBridge == nil,
			// or a conversation_delete wipe started while
			// prepareFileForTransmit was running and tripped the outgoing
			// barrier) before the goroutine that calls PrepareAndSend
			// could take ownership. The blob has no ref and no pending —
			// clean it up.
			w.client.RemoveUnreferencedTransmitFile(result.FileHash)
			restoreAttach()
			// The wipe-pending barrier is a deliberate refusal, not a
			// file-prep bug; surface the same status the text-send path
			// uses (compose_blocked_during_wipe) so the user sees the
			// real reason their attachment was not sent. Other errors
			// (genuine prep / transport failures) still fall through to
			// the generic "file.prepare_failed" overlay.
			if errors.Is(err, service.ErrConversationDeleteInflight) {
				w.router.SetSendStatus(w.t("status.compose_blocked_during_wipe"))
			} else {
				w.router.SetSendStatus(w.t("file.prepare_failed", err))
			}
		}
		if w.window != nil {
			w.window.Invalidate()
		}
	}()
}

func (w *Window) layoutHeader(gtx layout.Context) layout.Dimensions {
	title := material.Label(w.theme, unit.Sp(24), w.t("app.title"))
	title.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	title.Font.Weight = 700

	return layout.Flex{
		Axis:      layout.Horizontal,
		Spacing:   layout.SpaceBetween,
		Alignment: layout.Middle,
	}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return title.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis:      layout.Horizontal,
				Alignment: layout.Middle,
			}.Layout(gtx,
				layout.Rigid(w.layoutUpdateBadge),
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
				layout.Rigid(w.layoutConsoleButton),
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
				layout.Rigid(w.layoutLanguageSelectorInline),
			)
		}),
	)
}

func (w *Window) layoutMain(gtx layout.Context) layout.Dimensions {
	status := w.snap.NodeStatus
	recipients := w.snapRecipients()
	w.ensureSelectedRecipient(recipients)

	return layout.Flex{
		Axis:    layout.Horizontal,
		Spacing: layout.SpaceBetween,
	}.Layout(gtx,
		layout.Flexed(0.3, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					lbl := material.Label(w.theme, unit.Sp(15), w.t("app.subtitle"))
					lbl.Color = color.NRGBA{R: 144, G: 156, B: 173, A: 255}
					return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, lbl.Layout)
				}),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return w.layoutContactsCard(gtx, status, recipients)
				}),
			)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Flexed(0.7, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					recipient := w.snap.ActivePeer
					if recipient == "" {
						return layout.Dimensions{}
					}
					lbl := material.Label(w.theme, unit.Sp(15), w.t("chat.with", w.peerDisplayName(recipient)))
					lbl.Color = color.NRGBA{R: 200, G: 212, B: 228, A: 255}
					lbl.Font.Weight = 600
					return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, lbl.Layout)
				}),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return w.layoutChatCard(gtx, status)
				}),
				layout.Rigid(w.layoutComposerCard),
			)
		}),
	)
}

func (w *Window) layoutContactsCard(gtx layout.Context, status service.NodeStatus, recipients []domain.PeerIdentity) layout.Dimensions {
	rows := []string{
		w.t("clients.you", w.snap.MyAddress),
		w.t("clients.known", len(recipients)),
	}

	return w.card(gtx, w.t("clients.title"), rows, func(gtx layout.Context) layout.Dimensions {
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(220)))
			btn := material.Button(w.theme, &w.copyIdentityButton, w.t("clients.copy_identity"))
			return btn.Layout(gtx)
		})
	}, func(gtx layout.Context) layout.Dimensions {
		searchResults := searchKnownIdentities(status.KnownIDs, recipients, w.snap.MyAddress, w.identitySearchEditor.Text())

		children := []layout.FlexChild{
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.identitySearchCard(gtx, status, searchResults)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		}

		if len(recipients) == 0 {
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body1(w.theme, w.t("clients.empty"))
				label.Color = color.NRGBA{R: 165, G: 177, B: 194, A: 255}
				return label.Layout(gtx)
			}))
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
		}

		list := material.List(w.theme, &w.contactsList)
		children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return list.Layout(gtx, len(recipients), func(gtx layout.Context, index int) layout.Dimensions {
				return w.layoutRecipientButton(gtx, status, recipients[index], true)
			})
		}))
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	})
}

func (w *Window) identitySearchCard(gtx layout.Context, status service.NodeStatus, results []domain.PeerIdentity) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Left: unit.Dp(4), Right: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
				backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
				cardHeight := gtx.Dp(unit.Dp(78))
				return layout.Stack{}.Layout(gtx,
					layout.Expanded(func(gtx layout.Context) layout.Dimensions {
						gtx.Constraints.Min.Y = cardHeight
						gtx.Constraints.Max.Y = cardHeight
						fill(gtx, borderColor)
						return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							fill(gtx, backgroundColor)
							return layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										label := material.Body2(w.theme, w.t("clients.search_label"))
										label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
										return label.Layout(gtx)
									}),
									layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										w.identitySearchEditor.SingleLine = true
										editor := material.Editor(w.theme, &w.identitySearchEditor, w.t("clients.search_placeholder"))
										editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
										editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
										return editor.Layout(gtx)
									}),
								)
							})
						})
					}),
				)
			})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			if len(results) == 0 {
				return layout.Dimensions{}
			}
			if len(results) > 4 {
				results = results[:4]
			}
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, recipientsToChildren(results, func(gtx layout.Context, identity domain.PeerIdentity) layout.Dimensions {
				return layout.Inset{Left: unit.Dp(4), Right: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return w.layoutRecipientButton(gtx, status, identity, false)
				})
			})...)
		}),
	)
}

func (w *Window) layoutRecipientButton(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity, showUnread bool) layout.Dimensions {
	btn := w.recipientButton(fingerprint)
	rc := w.recipientRightClickState(fingerprint)

	return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fpStr := string(fingerprint)
		for btn.Clicked(gtx) {
			w.recipientEditor.SetText(fpStr)
			w.router.SetSendStatus(w.t("status.chat_selected"))
			w.router.SelectPeer(fingerprint)
			w.focusComposerPending = true
		}

		// Detect right-click (secondary button) for context menu.
		// Position comes from the window-level cursor tracker (lastCursorPos)
		// because card-local coordinates don't account for scroll offset
		// and nested layout transforms.
		for {
			ev, ok := gtx.Event(pointer.Filter{
				Target: rc,
				Kinds:  pointer.Press | pointer.Release,
			})
			if !ok {
				break
			}
			pe, ok := ev.(pointer.Event)
			if !ok {
				continue
			}
			if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonSecondary) {
				rc.pressed = true
			}
			if pe.Kind == pointer.Release && rc.pressed {
				rc.pressed = false
				w.contextMenuPeer = fingerprint
				w.contextMenuPos = w.lastCursorPos
				w.showDeleteConfirm = false
				w.showClearChatConfirm = false
				w.showAliasEditor = false
				// Auto-select this identity so the user sees the chat.
				w.recipientEditor.SetText(fpStr)
				w.router.SelectPeer(fingerprint)
				w.focusComposerPending = true
			}
		}

		bg := color.NRGBA{R: 34, G: 46, B: 62, A: 255}
		if fingerprint == w.snap.ActivePeer {
			bg = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
		}

		return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
			// Register right-click tag inside the Clickable's clip area.
			// Clickable only handles ButtonPrimary, so ButtonSecondary
			// events propagate to our tag without conflict.
			event.Op(gtx.Ops, rc)

			fill(gtx, bg)
			return layout.UniformInset(unit.Dp(14)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{
							Axis:      layout.Horizontal,
							Spacing:   layout.SpaceBetween,
							Alignment: layout.Middle,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return layout.Inset{Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
									return w.layoutReachableIndicator(gtx, status, fingerprint)
								})
							}),
							layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
								title := material.Body1(w.theme, w.peerDisplayName(fingerprint))
								title.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
								title.Font.Weight = 600
								return title.Layout(gtx)
							}),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								if !showUnread {
									return layout.Dimensions{}
								}
								ps := w.snap.Peers[fingerprint]
								if ps == nil || ps.Unread == 0 {
									return layout.Dimensions{}
								}
								return w.layoutUnreadBadge(gtx, ps.Unread)
							}),
						)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						preview := w.snapPreview(fingerprint)
						if strings.TrimSpace(preview) == "" {
							preview = fpStr
						}
						label := material.Body2(w.theme, ellipsize(preview, 44))
						label.Color = color.NRGBA{R: 187, G: 197, B: 212, A: 255}
						label.MaxLines = 1
						return label.Layout(gtx)
					}),
				)
			})
		})
	})
}

func (w *Window) layoutChatCard(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	recipient := w.snap.ActivePeer
	var rows []string

	if recipient == "" {
		rows = append(rows, w.t("chat.choose"))
		return w.card(gtx, w.t("chat.title"), rows)
	}

	conversation := w.snap.ActiveMessages
	if len(conversation) == 0 {
		if !w.snap.CacheReady {
			return w.layoutLoadingCard(gtx, "")
		}
		rows = append(rows, w.t("chat.empty"))
		return w.card(gtx, "", rows)
	}

	return w.card(gtx, "", rows, func(gtx layout.Context) layout.Dimensions {
		return w.layoutConversation(gtx, recipient, conversation)
	})
}

func (w *Window) layoutLoadingCard(gtx layout.Context, title string) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

		inset := layout.UniformInset(unit.Dp(8))
		return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(w.theme, unit.Sp(20), title)
					label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{
							Axis:      layout.Vertical,
							Alignment: layout.Middle,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Label(w.theme, unit.Sp(32), ". . .")
								label.Color = color.NRGBA{R: 120, G: 144, B: 176, A: 255}
								label.Alignment = text.Middle
								return label.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Label(w.theme, unit.Sp(16), w.t("chat.loading"))
								label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
								label.Alignment = text.Middle
								return label.Layout(gtx)
							}),
						)
					})
				}),
			)
		})
	})
}

func (w *Window) layoutComposerCard(gtx layout.Context) layout.Dimensions {
	recipient := w.snap.ActivePeer
	status := w.snap.NodeStatus
	sendStatus := w.snap.SendStatus
	maxInputHeight := max(gtx.Constraints.Max.Y/3-gtx.Dp(unit.Dp(76)), gtx.Dp(unit.Dp(62)))

	return w.card(gtx, "", nil, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				if sendStatus == "" {
					return layout.Dimensions{}
				}
				return w.layoutSendStatusRow(gtx, sendStatus)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutReplyPreview(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.messageInputCard(gtx, recipient, maxInputHeight)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis:      layout.Horizontal,
					Spacing:   layout.SpaceBetween,
					Alignment: layout.Middle,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutNetworkStatus(gtx, status)
					}),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Dimensions{}
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := w.t("compose.send")
						if recipient == "" {
							label = w.t("compose.select_first")
						}
						// Visual + interactive disable while a
						// conversation_delete is in flight for
						// the active peer. We render through a
						// dummy Clickable (not w.sendButton) so
						// even queued click events from the
						// active sendButton are ignored — the
						// service-layer ErrConversationDeleteInflight
						// gate is still enforced as defence in
						// depth.
						pending := recipient != "" && w.router.IsConversationDeletePending(recipient)
						if pending {
							label = w.t("compose.send_blocked_during_wipe")
						}
						return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(260)))
							if pending {
								var inert widget.Clickable
								btn := material.Button(w.theme, &inert, label)
								btn.Background = color.NRGBA{R: 60, G: 60, B: 64, A: 255}
								btn.Color = color.NRGBA{R: 130, G: 130, B: 130, A: 255}
								return btn.Layout(gtx)
							}
							btn := material.Button(w.theme, &w.sendButton, label)
							return btn.Layout(gtx)
						})
					}),
				)
			}),
		)
	})
}

func (w *Window) layoutSendStatusRow(gtx layout.Context, statusText string) layout.Dimensions {
	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 196, G: 205, B: 218, A: 255}}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()

	selectionMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 72, G: 96, B: 140, A: 180}}.Add(gtx.Ops)
	selectionMaterial := selectionMacro.Stop()

	w.sendStatusSelectable.SetText(statusText)

	return layout.Inset{Bottom: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return w.sendStatusSelectable.Layout(
			gtx,
			w.theme.Shaper,
			font.Font{Typeface: w.theme.Face},
			w.theme.TextSize,
			textMaterial,
			selectionMaterial,
		)
	})
}

func (w *Window) layoutNetworkStatus(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	state, connected, total, pending := networkStatusSummary(status)
	labelText := w.t("compose.network_status", strings.ToUpper(state), connected, total, pending)
	if labelText == "compose.network_status" {
		labelText = "NET " + strings.ToUpper(state) + " | " + strconv.Itoa(connected) + "/" + strconv.Itoa(total) + " peers | " + strconv.Itoa(pending) + " pending"
	}
	breakdownText := w.networkBreakdownText(status)
	bg, fg := networkStateColors(state)

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(8), Right: unit.Dp(8)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Caption(w.theme, labelText)
					label.Color = fg
					return label.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(breakdownText) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Top: unit.Dp(2)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						label := material.Caption(w.theme, breakdownText)
						label.Color = color.NRGBA{R: 214, G: 221, B: 232, A: 220}
						return label.Layout(gtx)
					})
				}),
			)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(12))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: bg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

// networkStatusSummary returns the aggregate network status for the main UI
// badge. The node layer is the single source of truth: when ProbeNode returns
// an AggregateStatus from fetch_aggregate_status, we use it directly.
// The local fallback is kept only for backward compatibility with older node
// versions that do not yet serve fetch_aggregate_status.
func networkStatusSummary(status service.NodeStatus) (string, int, int, int) {
	if as := status.AggregateStatus; as != nil {
		return as.Status, as.ConnectedPeers, as.TotalPeers, as.PendingMessages
	}

	// Fallback: local computation for backward compatibility.
	usable := 0  // healthy + degraded — can route messages
	stalled := 0 // connected at TCP level but not routing
	reconnecting := 0
	pending := 0

	for _, item := range status.PeerHealth {
		switch item.State {
		case "healthy", "degraded":
			usable++
		case "stalled":
			stalled++
		case "reconnecting":
			reconnecting++
		}
		pending += item.PendingCount
	}

	connected := usable + stalled
	total := connected + reconnecting

	switch {
	case total == 0:
		return "offline", 0, 0, pending
	case connected == 0:
		return "reconnecting", 0, total, pending
	case usable == 0:
		return "limited", connected, total, pending
	case usable == 1:
		return "limited", connected, total, pending
	case usable*2 < connected:
		return "warning", connected, total, pending
	default:
		return "healthy", connected, total, pending
	}
}

func (w *Window) networkBreakdownText(status service.NodeStatus) string {
	healthy := 0
	degraded := 0
	stalled := 0
	reconnecting := 0
	outbound := 0
	inbound := 0

	for _, item := range status.PeerHealth {
		switch item.State {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "stalled":
			stalled++
		case "reconnecting":
			reconnecting++
		}
		switch item.Direction {
		case "outbound":
			outbound++
		case "inbound":
			inbound++
		}
	}

	if healthy == 0 && degraded == 0 && stalled == 0 && reconnecting == 0 {
		return ""
	}

	text := w.t("compose.network_breakdown", healthy, degraded, stalled, reconnecting)
	if text == "compose.network_breakdown" {
		return "H " + strconv.Itoa(healthy) + " | D " + strconv.Itoa(degraded) + " | S " + strconv.Itoa(stalled) + " | R " + strconv.Itoa(reconnecting) + " | ↑" + strconv.Itoa(outbound) + " ↓" + strconv.Itoa(inbound)
	}
	return text
}

func networkStateColors(state string) (color.NRGBA, color.NRGBA) {
	switch state {
	case "healthy":
		return color.NRGBA{R: 36, G: 92, B: 63, A: 255}, color.NRGBA{R: 231, G: 255, B: 239, A: 255}
	case "limited", "warning":
		return color.NRGBA{R: 140, G: 110, B: 20, A: 255}, color.NRGBA{R: 255, G: 240, B: 180, A: 255}
	case "reconnecting":
		return color.NRGBA{R: 57, G: 67, B: 84, A: 255}, color.NRGBA{R: 231, G: 237, B: 246, A: 255}
	default: // offline, unknown
		return color.NRGBA{R: 51, G: 56, B: 66, A: 255}, color.NRGBA{R: 214, G: 221, B: 232, A: 255}
	}
}

func (w *Window) messageInputCard(gtx layout.Context, recipient domain.PeerIdentity, maxInputHeight int) layout.Dimensions {
	borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
	backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
	editorBg := backgroundColor
	scrollTrack := color.NRGBA{R: 38, G: 46, B: 58, A: 255}
	scrollThumb := color.NRGBA{R: 112, G: 132, B: 164, A: 255}
	lineStep := gtx.Dp(unit.Dp(18))
	baseEditorHeight := gtx.Dp(unit.Dp(36))
	chromeHeight := gtx.Dp(unit.Dp(26))
	if w.attachedFile != "" {
		chromeHeight += gtx.Dp(unit.Dp(40))
	}

	if w.focusComposerPending {
		gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
		w.focusComposerPending = false
	}

	line, _ := w.messageEditor.CaretPos()
	totalLines := max(line+1, strings.Count(w.messageEditor.Text(), "\n")+1)
	extraLines := max(0, totalLines-2)
	editorHeight := baseEditorHeight + extraLines*lineStep
	maxEditorHeight := maxInputHeight - chromeHeight
	if maxEditorHeight < baseEditorHeight {
		maxEditorHeight = baseEditorHeight
	}
	if editorHeight > maxEditorHeight {
		editorHeight = maxEditorHeight
	}
	cardHeight := chromeHeight + editorHeight
	visibleLines := max(2, editorHeight/lineStep)
	showScrollbar := totalLines > visibleLines

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.Y = cardHeight
		gtx.Constraints.Max.Y = cardHeight
		fill(gtx, borderColor)

		return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			fill(gtx, backgroundColor)

			return layout.Inset{Top: unit.Dp(2), Bottom: unit.Dp(1), Left: unit.Dp(8), Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						if recipient == "" {
							label := material.Body2(w.theme, w.t("compose.body"))
							label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
							return label.Layout(gtx)
						}
						return layout.Flex{
							Axis:      layout.Horizontal,
							Alignment: layout.Baseline,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Body2(w.theme, w.t("compose.body_for"))
								label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
								return label.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								name := w.peerDisplayName(recipient)
								lbl := material.Body1(w.theme, name)
								lbl.Font.Weight = font.Bold
								lbl.TextSize = unit.Sp(17)
								lbl.Color = color.NRGBA{R: 150, G: 210, B: 255, A: 255}
								return lbl.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								lbl := material.Caption(w.theme, w.t("compose.identity_label"))
								lbl.Color = color.NRGBA{R: 160, G: 170, B: 190, A: 255}
								return lbl.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(4)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								lbl := material.Body1(w.theme, shortFingerprint(string(recipient)))
								lbl.Font.Weight = font.Bold
								lbl.TextSize = unit.Sp(15)
								lbl.Color = color.NRGBA{R: 130, G: 235, B: 190, A: 255}
								return lbl.Layout(gtx)
							}),
						)
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						if w.attachedFile == "" {
							return layout.Dimensions{}
						}
						return w.layoutAttachedFilePreview(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(0)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{
							Axis:      layout.Horizontal,
							Alignment: layout.Middle,
						}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								btn := material.Button(w.theme, &w.attachButton, w.t("file.attach_icon"))
								btn.Background = color.NRGBA{R: 55, G: 65, B: 85, A: 255}
								btn.Color = color.NRGBA{R: 200, G: 210, B: 230, A: 255}
								btn.TextSize = unit.Sp(18)
								btn.Inset = layout.UniformInset(unit.Dp(2))
								gtx.Constraints.Min.X = gtx.Dp(unit.Dp(28))
								gtx.Constraints.Max.X = gtx.Dp(unit.Dp(28))
								gtx.Constraints.Min.Y = gtx.Dp(unit.Dp(28))
								gtx.Constraints.Max.Y = gtx.Dp(unit.Dp(28))
								return btn.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
							layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
								w.messageEditor.SingleLine = false
								w.messageEditor.Submit = false
								editor := material.Editor(w.theme, &w.messageEditor, w.t("compose.placeholder"))
								editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
								editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
								editor.TextSize = unit.Sp(15)
								editor.LineHeight = unit.Sp(18)

								radius := gtx.Dp(unit.Dp(12))
								defer clip.UniformRRect(image.Rectangle{Max: image.Pt(gtx.Constraints.Max.X, editorHeight)}, radius).Push(gtx.Ops).Pop()
								return layout.Stack{}.Layout(gtx,
									layout.Expanded(func(gtx layout.Context) layout.Dimensions {
										gtx.Constraints.Min.Y = editorHeight
										gtx.Constraints.Max.Y = editorHeight
										fill(gtx, editorBg)
										return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, editorHeight)}
									}),
									layout.Stacked(func(gtx layout.Context) layout.Dimensions {
										gtx.Constraints.Min.Y = editorHeight
										gtx.Constraints.Max.Y = editorHeight
										return layout.Inset{
											Top:    unit.Dp(0),
											Bottom: unit.Dp(0),
											Left:   unit.Dp(9),
											Right:  unit.Dp(6),
										}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
											return layout.Flex{
												Axis:      layout.Horizontal,
												Alignment: layout.Middle,
											}.Layout(gtx,
												layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
													return editor.Layout(gtx)
												}),
												layout.Rigid(func(gtx layout.Context) layout.Dimensions {
													if !showScrollbar {
														return layout.Dimensions{}
													}
													return w.layoutComposerScrollbar(gtx, totalLines, visibleLines, editorHeight, scrollTrack, scrollThumb)
												}),
											)
										})
									}),
								)
							}),
						)
					}),
				)
			})
		})
	})
}

func (w *Window) layoutComposerScrollbar(gtx layout.Context, totalLines, visibleLines, editorHeight int, track, thumb color.NRGBA) layout.Dimensions {
	if totalLines <= visibleLines {
		return layout.Dimensions{}
	}

	width := gtx.Dp(unit.Dp(4))
	height := editorHeight - gtx.Dp(unit.Dp(4))
	minHeight := gtx.Dp(unit.Dp(16))
	if height < minHeight {
		height = minHeight
	}
	if height <= 0 {
		return layout.Dimensions{}
	}

	caretLine, _ := w.messageEditor.CaretPos()
	maxOffset := totalLines - visibleLines
	scrollOffset := caretLine - (visibleLines - 1)
	if scrollOffset < 0 {
		scrollOffset = 0
	}
	if scrollOffset > maxOffset {
		scrollOffset = maxOffset
	}

	thumbHeight := height * visibleLines / totalLines
	minThumb := gtx.Dp(unit.Dp(10))
	if thumbHeight < minThumb {
		thumbHeight = minThumb
	}
	if thumbHeight > height {
		thumbHeight = height
	}

	thumbOffset := 0
	if maxOffset > 0 && height > thumbHeight {
		thumbOffset = (height - thumbHeight) * scrollOffset / maxOffset
	}

	return layout.Inset{Left: unit.Dp(8), Right: unit.Dp(2)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min = image.Pt(width, height)
		gtx.Constraints.Max = image.Pt(width, height)

		defer clip.UniformRRect(image.Rectangle{Max: image.Pt(width, height)}, width/2).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: track}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)

		thumbRect := image.Rect(0, thumbOffset, width, thumbOffset+thumbHeight)
		defer clip.UniformRRect(thumbRect, width/2).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: thumb}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		return layout.Dimensions{Size: image.Pt(width, height)}
	})
}

// layoutAttachedFilePreview renders a self-contained chip/badge card showing
// the attached file. Visually separated from the editor, it floats above
// the text input like a removable tag (similar to Claude's file attach UI).
func (w *Window) layoutAttachedFilePreview(gtx layout.Context) layout.Dimensions {
	chipBg := color.NRGBA{R: 40, G: 48, B: 62, A: 255}
	chipBorder := color.NRGBA{R: 72, G: 85, B: 110, A: 255}
	nameFg := color.NRGBA{R: 235, G: 240, B: 248, A: 255}
	iconFg := color.NRGBA{R: 160, G: 175, B: 200, A: 255}

	fileName := filepath.Base(w.attachedFile)

	// Outer inset to separate the chip from the editor below.
	return layout.Inset{Top: unit.Dp(2), Bottom: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		// Draw chip with a 1px border and rounded corners.
		macro := op.Record(gtx.Ops)
		dims := layout.Inset{Top: unit.Dp(1), Bottom: unit.Dp(1), Left: unit.Dp(1), Right: unit.Dp(1)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			innerMacro := op.Record(gtx.Ops)
			innerDims := layout.Inset{Top: unit.Dp(6), Bottom: unit.Dp(6), Left: unit.Dp(10), Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis:      layout.Horizontal,
					Alignment: layout.Middle,
				}.Layout(gtx,
					// File icon.
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						lbl := material.Caption(w.theme, w.t("file.icon"))
						lbl.Color = iconFg
						return lbl.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
					// File name (truncated if needed by Gio constraints).
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						lbl := material.Body2(w.theme, fileName)
						lbl.Color = nameFg
						lbl.MaxLines = 1
						return lbl.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
					// Close / cancel button "×".
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						btn := material.Button(w.theme, &w.attachCancelBtn, "\u00d7") // × character
						btn.Background = color.NRGBA{R: 65, G: 70, B: 85, A: 255}
						btn.Color = color.NRGBA{R: 210, G: 215, B: 225, A: 255}
						btn.TextSize = unit.Sp(14)
						btn.Inset = layout.UniformInset(unit.Dp(2))
						btn.CornerRadius = unit.Dp(10)
						gtx.Constraints.Min.X = gtx.Dp(unit.Dp(24))
						gtx.Constraints.Max.X = gtx.Dp(unit.Dp(24))
						gtx.Constraints.Min.Y = gtx.Dp(unit.Dp(24))
						gtx.Constraints.Max.Y = gtx.Dp(unit.Dp(24))
						return btn.Layout(gtx)
					}),
				)
			})
			innerCall := innerMacro.Stop()

			// Fill chip background.
			r := gtx.Dp(unit.Dp(8))
			defer clip.UniformRRect(image.Rectangle{Max: innerDims.Size}, r).Push(gtx.Ops).Pop()
			paint.ColorOp{Color: chipBg}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			innerCall.Add(gtx.Ops)
			return innerDims
		})
		call := macro.Stop()

		// Border: draw a slightly larger rounded rect behind the chip.
		borderR := gtx.Dp(unit.Dp(9))
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, borderR).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: chipBorder}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

// layoutFileCard renders a file transfer card inside a chat bubble.
// Sender sees: file name, size, bytes transferred, percentage, and a progress bar.
// Receiver sees: file name, size, and a download button (non-functional for now).
// If the message body contains a user caption (not the "[file]" sentinel),
// it is displayed above the file card.
func (w *Window) layoutFileCard(gtx layout.Context, message service.DirectMessage, isMine bool) layout.Dimensions {
	var payload domain.FileAnnouncePayload
	if err := json.Unmarshal([]byte(message.CommandData), &payload); err != nil {
		label := material.Caption(w.theme, w.t("file.invalid"))
		label.Color = color.NRGBA{R: 255, G: 100, B: 100, A: 255}
		return label.Layout(gtx)
	}

	cardBg := color.NRGBA{R: 35, G: 45, B: 60, A: 255}
	nameFg := color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	sizeFg := color.NRGBA{R: 160, G: 175, B: 200, A: 255}
	captionFg := color.NRGBA{R: 230, G: 235, B: 245, A: 255}
	progressBg := color.NRGBA{R: 50, G: 60, B: 80, A: 255}
	progressFg := color.NRGBA{R: 72, G: 150, B: 255, A: 255}

	// Determine if there is a user caption (body != sentinel).
	caption := ""
	if message.Body != domain.FileDMBodySentinel {
		caption = message.Body
	}

	// Query real transfer progress from FileTransferManager.
	fileID := domain.FileID(message.ID)
	bytesTransferred, _, transferState, transferFound := w.router.FileBridge().Progress(fileID, isMine)
	percent := 0
	if payload.FileSize > 0 && bytesTransferred > 0 {
		percent = int(bytesTransferred * 100 / payload.FileSize)
		if percent > 100 {
			percent = 100
		}
	}

	// Receiver: determine if download is actively transferring data
	// (show progress bar + cancel). Terminal states and waiting_ack
	// do NOT show progress bar.
	receiverDownloadActive := !isMine && (transferState == "downloading" || transferState == "verifying")

	// Sender/receiver terminal states — no progress bar needed.
	senderCompleted := isMine && (transferState == "completed" || transferState == "tombstone")
	receiverTerminal := !isMine && (transferState == "completed" ||
		transferState == "waiting_ack" || transferState == "waiting_route")
	receiverFailed := !isMine && transferState == "failed"

	// Schedule a delayed redraw while transfer is in progress or
	// awaiting confirmation (waiting_ack needs redraw for ack arrival).
	transferInProgress := (isMine && !senderCompleted && transferState != "") ||
		receiverDownloadActive || transferState == "waiting_ack"
	if transferInProgress {
		w.scheduleTransferInvalidate(500 * time.Millisecond)
	}

	macro := op.Record(gtx.Ops)
	dims := layout.Inset{Top: unit.Dp(6), Bottom: unit.Dp(6), Left: unit.Dp(10), Right: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := []layout.FlexChild{}

		// User caption above the file card (if present).
		if caption != "" {
			children = append(children,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					lbl := material.Body2(w.theme, caption)
					lbl.Color = captionFg
					return lbl.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			)
		}

		// Image thumbnail preview (only for image content types when file
		// is available on disk). Click on thumbnail opens the file with the
		// system default viewer.
		if isImageContentType(payload.ContentType) {
			filePath := w.router.FileBridge().FilePath(fileID, isMine)
			// get() returns non-nil only when the image is decoded and
			// ready (thumbReady). While decoding is in progress or if
			// it failed, nil is returned and we skip the thumbnail.
			entry := w.thumbCache.get(filePath, w.window)
			if entry != nil {
				imgOp := entry.op
				imgBounds := entry.bounds
				openPath := filePath

				// Ensure clickable widget exists for this message.
				if w.thumbClickBtns == nil {
					w.thumbClickBtns = make(map[string]*widget.Clickable)
				}
				thumbBtn, ok := w.thumbClickBtns[message.ID]
				if !ok {
					thumbBtn = new(widget.Clickable)
					w.thumbClickBtns[message.ID] = thumbBtn
				}

				for thumbBtn.Clicked(gtx) {
					if openPath != "" {
						go openFile(openPath)
					}
				}

				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return thumbBtn.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							// Hand cursor on hover so the click
							// affordance is discoverable — same UX
							// the file-tab thumbnail and the donate
							// link already provide. CursorPointer
							// must be added INSIDE the clickable's
							// layout callback so the cursor area
							// matches the clickable's hit area.
							pointer.CursorPointer.Add(gtx.Ops)

							dispW, dispH := thumbnailDisplaySize(
								imgBounds.X, imgBounds.Y,
								gtx.Dp(unit.Dp(thumbnailMaxWidth)),
								gtx.Dp(unit.Dp(thumbnailMaxHeight)),
							)

							// Apply rounded clip before rendering the image.
							size := image.Pt(dispW, dispH)
							defer clip.UniformRRect(image.Rectangle{Max: size}, gtx.Dp(unit.Dp(6))).Push(gtx.Ops).Pop()

							imgWidget := widget.Image{
								Src:      imgOp,
								Fit:      widget.ScaleDown,
								Position: layout.NW,
							}
							gtx.Constraints = layout.Exact(size)
							return imgWidget.Layout(gtx)
						})
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				)
			}
		}

		// File icon + name.
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						lbl := material.Body2(w.theme, w.t("file.icon"))
						lbl.Color = color.NRGBA{R: 100, G: 180, B: 255, A: 255}
						return lbl.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						lbl := material.Body2(w.theme, payload.FileName)
						lbl.Font.Weight = font.Bold
						lbl.Color = nameFg
						return lbl.Layout(gtx)
					}),
				)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		)

		// File size display: full size for terminal states, progress for active.
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				var sizeText string
				if senderCompleted || receiverTerminal || receiverFailed {
					sizeText = formatFileSize(payload.FileSize)
				} else {
					sizeText = formatFileSize(bytesTransferred) + " / " + formatFileSize(payload.FileSize) +
						"  (" + strconv.Itoa(percent) + "%)"
				}
				lbl := material.Caption(w.theme, sizeText)
				lbl.Color = sizeFg
				return lbl.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
		)

		// Sender: show progress bar only while serving, hide when completed.
		// Receiver: show download button in "available" state, progress bar + cancel
		// during active download, restart button for failed, nothing for terminal states.
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				if isMine && !senderCompleted {
					return w.layoutFileProgressBar(gtx, progressBg, progressFg, percent)
				}
				if receiverDownloadActive {
					return w.layoutReceiverProgress(gtx, progressBg, progressFg, percent, message.ID)
				}
				if receiverFailed {
					return w.layoutFileRestartButton(gtx, message.ID)
				}
				if !isMine && !receiverTerminal && transferFound {
					return w.layoutFileDownloadButton(gtx, message.ID)
				}
				return layout.Dimensions{}
			}),
		)

		// Show status label for terminal/informational states.
		var stateLabel string
		var stateLabelColor color.NRGBA
		showLabel := false

		switch {
		// Sender: "downloaded" when receiver confirmed.
		case isMine && transferState == "completed":
			stateLabel = "downloaded"
			stateLabelColor = color.NRGBA{R: 100, G: 220, B: 130, A: 255}
			showLabel = true

		// Receiver: "completed" only after file_downloaded_ack.
		case !isMine && transferState == "completed":
			stateLabel = "completed"
			stateLabelColor = color.NRGBA{R: 100, G: 220, B: 130, A: 255}
			showLabel = true

		// Receiver: waiting for sender ack — show "confirming...".
		case !isMine && transferState == "waiting_ack":
			stateLabel = "confirming..."
			stateLabelColor = color.NRGBA{R: 180, G: 180, B: 180, A: 255}
			showLabel = true

		case !isMine && transferState == "failed":
			stateLabel = "failed"
			stateLabelColor = color.NRGBA{R: 255, G: 100, B: 100, A: 255}
			showLabel = true

		case !isMine && transferState == "waiting_route":
			stateLabel = "sender offline"
			stateLabelColor = color.NRGBA{R: 255, G: 200, B: 80, A: 255}
			showLabel = true

		// Receiver: no mapping exists — registration was rejected
		// (quota, invalid metadata, etc.). Show "unavailable" instead
		// of a misleading Download button.
		case !isMine && !transferFound:
			stateLabel = "unavailable"
			stateLabelColor = color.NRGBA{R: 180, G: 180, B: 180, A: 255}
			showLabel = true
		}

		if showLabel {
			labelText := stateLabel
			labelColor := stateLabelColor
			children = append(children,
				layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					lbl := material.Caption(w.theme, labelText)
					lbl.Color = labelColor
					return lbl.Layout(gtx)
				}),
			)
		}

		// "Show in Folder" + "Open" + "Delete" action buttons for
		// completed transfers where the file is available on disk.
		fileOnDisk := w.router.FileBridge().FilePath(fileID, isMine)
		if fileOnDisk != "" {
			revealPath := fileOnDisk
			msgCopy := message
			children = append(children,
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return w.layoutFileActionButtons(gtx, msgCopy, isMine, revealPath)
				}),
			)
		}

		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	})
	call := macro.Stop()

	defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(8))).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: cardBg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	call.Add(gtx.Ops)
	return dims
}

// scheduleTransferInvalidate coalesces redraw requests for in-progress file
// transfers. layoutFileCard runs every frame; spawning a new timer on each
// frame leads to an unbounded timer/goroutine backlog under active transfers,
// which can starve the Gio event loop and present as a frozen window.
func (w *Window) scheduleTransferInvalidate(delay time.Duration) {
	w.transferInvalidateMu.Lock()
	if w.transferInvalidatePending {
		w.transferInvalidateMu.Unlock()
		return
	}
	w.transferInvalidatePending = true
	w.transferInvalidateMu.Unlock()

	time.AfterFunc(delay, func() {
		w.transferInvalidateMu.Lock()
		w.transferInvalidatePending = false
		window := w.window
		w.transferInvalidateMu.Unlock()

		if window != nil {
			window.Invalidate()
		}
	})
}

// layoutFileProgressBar renders a progress bar for the sender side.
// percent is the current transfer progress (0–100). When percent is 0
// a minimal sliver is shown to indicate the transfer has been initiated.
func (w *Window) layoutFileProgressBar(gtx layout.Context, bg, fg color.NRGBA, percent int) layout.Dimensions {
	barHeight := gtx.Dp(unit.Dp(6))
	barWidth := gtx.Constraints.Max.X
	if barWidth > gtx.Dp(unit.Dp(260)) {
		barWidth = gtx.Dp(unit.Dp(260))
	}

	// Background track.
	stack := clip.UniformRRect(image.Rectangle{Max: image.Pt(barWidth, barHeight)}, gtx.Dp(unit.Dp(3))).Push(gtx.Ops)
	paint.ColorOp{Color: bg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	stack.Pop()

	// Progress fill based on actual percentage.
	fillPercent := percent
	if fillPercent <= 0 {
		fillPercent = 0
	}
	if fillPercent > 100 {
		fillPercent = 100
	}
	fillWidth := barWidth * fillPercent / 100
	if fillWidth < 2 && fillPercent > 0 {
		fillWidth = 2
	}
	if fillWidth > 0 {
		fillStack := clip.UniformRRect(image.Rectangle{Max: image.Pt(fillWidth, barHeight)}, gtx.Dp(unit.Dp(3))).Push(gtx.Ops)
		paint.ColorOp{Color: fg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		fillStack.Pop()
	}

	return layout.Dimensions{Size: image.Pt(barWidth, barHeight)}
}

// layoutReceiverProgress renders the progress bar with a cancel button (✕)
// for receiver-side active downloads. The cancel button resets the transfer
// to available state and deletes the partial file.
func (w *Window) layoutReceiverProgress(gtx layout.Context, bg, fg color.NRGBA, percent int, messageID string) layout.Dimensions {
	if w.fileCancelDownloadBtns == nil {
		w.fileCancelDownloadBtns = make(map[string]*widget.Clickable)
	}
	cancelBtn, ok := w.fileCancelDownloadBtns[messageID]
	if !ok {
		cancelBtn = new(widget.Clickable)
		w.fileCancelDownloadBtns[messageID] = cancelBtn
	}

	for cancelBtn.Clicked(gtx) {
		fileID := domain.FileID(messageID)
		go func() {
			if err := w.router.FileBridge().CancelDownload(fileID); err != nil {
				log.Error().Err(err).Str("file_id", messageID).
					Msg("file_download: CancelFileDownload failed")
			}
		}()
	}

	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle, Spacing: layout.SpaceBetween}.Layout(gtx,
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return w.layoutFileProgressBar(gtx, bg, fg, percent)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			matBtn := material.Button(w.theme, cancelBtn, "✕")
			matBtn.Background = color.NRGBA{R: 65, G: 70, B: 85, A: 255}
			matBtn.Color = color.NRGBA{R: 220, G: 220, B: 220, A: 255}
			matBtn.Inset = layout.UniformInset(unit.Dp(2))
			matBtn.CornerRadius = unit.Dp(10)
			matBtn.TextSize = unit.Sp(12)
			gtx.Constraints.Min = image.Pt(gtx.Dp(unit.Dp(24)), gtx.Dp(unit.Dp(24)))
			gtx.Constraints.Max = gtx.Constraints.Min
			return matBtn.Layout(gtx)
		}),
	)
}

// layoutFileDownloadButton renders a download button for the receiver side.
// When clicked, triggers FileTransferManager.StartDownload which sends the
// first chunk_request and transitions the receiver state to downloading.
func (w *Window) layoutFileDownloadButton(gtx layout.Context, messageID string) layout.Dimensions {
	if w.fileDownloadBtns == nil {
		w.fileDownloadBtns = make(map[string]*widget.Clickable)
	}
	btn, ok := w.fileDownloadBtns[messageID]
	if !ok {
		btn = new(widget.Clickable)
		w.fileDownloadBtns[messageID] = btn
	}

	for btn.Clicked(gtx) {
		fileID := domain.FileID(messageID)
		go func() {
			if err := w.router.FileBridge().StartDownload(fileID); err != nil {
				log.Error().Err(err).Str("file_id", messageID).
					Msg("file_download: StartFileDownload failed")
			}
		}()
	}

	matBtn := material.Button(w.theme, btn, w.t("file.download"))
	matBtn.Background = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	matBtn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	matBtn.Inset = layout.Inset{
		Top: unit.Dp(4), Bottom: unit.Dp(4),
		Left: unit.Dp(12), Right: unit.Dp(12),
	}
	matBtn.CornerRadius = unit.Dp(6)
	matBtn.TextSize = unit.Sp(13)
	return layout.Flex{}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.X = gtx.Dp(unit.Dp(100))
			return matBtn.Layout(gtx)
		}),
	)
}

// layoutFileRestartButton renders a restart button for failed receiver-side
// downloads. When clicked, resets the receiver mapping to available state
// (RestartDownload) and immediately initiates a new download (StartDownload).
func (w *Window) layoutFileRestartButton(gtx layout.Context, messageID string) layout.Dimensions {
	if w.fileRestartBtns == nil {
		w.fileRestartBtns = make(map[string]*widget.Clickable)
	}
	btn, ok := w.fileRestartBtns[messageID]
	if !ok {
		btn = new(widget.Clickable)
		w.fileRestartBtns[messageID] = btn
	}

	for btn.Clicked(gtx) {
		fileID := domain.FileID(messageID)
		go func() {
			if err := w.router.FileBridge().RestartDownload(fileID); err != nil {
				log.Error().Err(err).Str("file_id", messageID).
					Msg("file_download: RestartFileDownload failed")
				return
			}
			if err := w.router.FileBridge().StartDownload(fileID); err != nil {
				log.Error().Err(err).Str("file_id", messageID).
					Msg("file_download: StartFileDownload after restart failed")
			}
		}()
	}

	matBtn := material.Button(w.theme, btn, w.t("file.restart"))
	matBtn.Background = color.NRGBA{R: 180, G: 80, B: 60, A: 255}
	matBtn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	matBtn.Inset = layout.Inset{
		Top: unit.Dp(4), Bottom: unit.Dp(4),
		Left: unit.Dp(12), Right: unit.Dp(12),
	}
	matBtn.CornerRadius = unit.Dp(6)
	matBtn.TextSize = unit.Sp(13)
	return layout.Flex{}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.X = gtx.Dp(unit.Dp(100))
			return matBtn.Layout(gtx)
		}),
	)
}

// layoutFileActionButtons renders "Show in Folder" and "Open" for
// every completed transfer that has a file on disk, plus a
// "Delete message" button that appears ONLY on outgoing rows
// ("кнопка delete должна быть только на моем файле, который я
// отправляю" — the user wants the inline Delete only on the file
// they own as the sender).
//
// Why outgoing-only: the destructive action propagates over the
// wire (message_delete) and asks the recipient to mirror the
// deletion. The user only wants that round-trip-bearing button
// surfaced on files they originated. Incoming rows keep the row
// clean; users who want to remove an inbound file row still have
// the right-click context-menu Delete, which dispatches the
// local-only path inside DMRouter.SendMessageDelete.
//
// Outgoing Delete is offline-gated: when the recipient is offline
// the wire-side delete cannot land and would burn the entire retry
// budget, so the button renders as a static neutral-gray "disabled"
// pill (no Clickable, no hover ripple) — see
// layoutFileCardDeleteButton + layoutFileCardDeleteDisabled.
func (w *Window) layoutFileActionButtons(gtx layout.Context, msg service.DirectMessage, isMine bool, filePath string) layout.Dimensions {
	// Ensure button maps are initialised.
	if w.fileRevealBtns == nil {
		w.fileRevealBtns = make(map[string]*widget.Clickable)
	}
	if w.fileOpenBtns == nil {
		w.fileOpenBtns = make(map[string]*widget.Clickable)
	}
	if w.fileRowDeleteBtns == nil {
		w.fileRowDeleteBtns = make(map[string]*widget.Clickable)
	}

	revealBtn, ok := w.fileRevealBtns[msg.ID]
	if !ok {
		revealBtn = new(widget.Clickable)
		w.fileRevealBtns[msg.ID] = revealBtn
	}
	openBtn, ok := w.fileOpenBtns[msg.ID]
	if !ok {
		openBtn = new(widget.Clickable)
		w.fileOpenBtns[msg.ID] = openBtn
	}

	revealPath := filePath
	for revealBtn.Clicked(gtx) {
		go revealFileInDir(revealPath)
	}
	for openBtn.Clicked(gtx) {
		go openFile(revealPath)
	}

	btnBg := color.NRGBA{R: 50, G: 60, B: 80, A: 255}
	btnFg := color.NRGBA{R: 180, G: 200, B: 230, A: 255}

	// Layout: [Show in Folder] [Open] left-aligned, plus [Delete]
	// only on outgoing rows. Default Spacing (SpaceEnd) packs items
	// to the start so the row width grows naturally with the
	// number of children — outgoing rows have three buttons,
	// incoming rows have two.
	children := []layout.FlexChild{
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			matBtn := material.Button(w.theme, revealBtn, w.t("file.show_in_folder"))
			matBtn.Background = btnBg
			matBtn.Color = btnFg
			matBtn.Inset = layout.Inset{
				Top: unit.Dp(3), Bottom: unit.Dp(3),
				Left: unit.Dp(8), Right: unit.Dp(8),
			}
			matBtn.CornerRadius = unit.Dp(5)
			matBtn.TextSize = unit.Sp(11)
			return matBtn.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			matBtn := material.Button(w.theme, openBtn, w.t("file.open_file"))
			matBtn.Background = btnBg
			matBtn.Color = btnFg
			matBtn.Inset = layout.Inset{
				Top: unit.Dp(3), Bottom: unit.Dp(3),
				Left: unit.Dp(8), Right: unit.Dp(8),
			}
			matBtn.CornerRadius = unit.Dp(5)
			matBtn.TextSize = unit.Sp(11)
			return matBtn.Layout(gtx)
		}),
	}
	if isMine {
		children = append(children,
			layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutFileCardDeleteButton(gtx, msg, isMine)
			}),
		)
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
}

// layoutFileCardDeleteButton renders the per-row Delete button for
// a chat-thread file card. Currently called only for outgoing
// rows (the layoutFileActionButtons caller gates on isMine), but
// the function keeps the direction-aware branching so future
// callers — e.g. an "always-on per-row Delete" mode toggled in
// preferences — get the right enable rule:
//
//   - Outgoing (isMine == true): gate on peerOnline. Wire-side
//     delete needs an ack; render as a static gray box without
//     a Clickable when offline so there's no hover ripple, no
//     hit area, no click ripple — visually unmistakably inert.
//
//   - Incoming (isMine == false): always enabled (local-only
//     delete path inside SendMessageDelete). No peer check.
func (w *Window) layoutFileCardDeleteButton(gtx layout.Context, msg service.DirectMessage, isMine bool) layout.Dimensions {
	// Determine the conversation peer relative to self — same
	// rule as the chat context-menu Delete handler.
	me := w.router.MyAddress()
	peer := msg.Recipient
	if msg.Sender != me {
		peer = msg.Sender
	}

	// Receive-direction deletes are local-only; ignore peerOnline.
	enabled := !isMine || w.peerOnline(peer)
	if !enabled {
		return w.layoutFileCardDeleteDisabled(gtx)
	}

	btn, ok := w.fileRowDeleteBtns[msg.ID]
	if !ok {
		btn = new(widget.Clickable)
		w.fileRowDeleteBtns[msg.ID] = btn
	}

	target := domain.MessageID(msg.ID)
	for btn.Clicked(gtx) {
		w.dispatchMessageDeleteAsync(peer, target)
	}

	matBtn := material.Button(w.theme, btn, w.t("context.delete_message"))
	matBtn.Background = color.NRGBA{R: 120, G: 50, B: 60, A: 255}
	matBtn.Color = color.NRGBA{R: 250, G: 240, B: 240, A: 255}
	matBtn.Inset = layout.Inset{
		Top: unit.Dp(3), Bottom: unit.Dp(3),
		Left: unit.Dp(8), Right: unit.Dp(8),
	}
	matBtn.CornerRadius = unit.Dp(5)
	matBtn.TextSize = unit.Sp(11)
	return matBtn.Layout(gtx)
}

// layoutFileCardDeleteDisabled renders the offline-state Delete
// pill via the shared layoutDisabledDeletePill helper. See that
// helper's doc for the design rationale.
func (w *Window) layoutFileCardDeleteDisabled(gtx layout.Context) layout.Dimensions {
	return layoutDisabledDeletePill(gtx, w.theme, w.t("context.delete_message"))
}

// layoutDisabledDeletePill renders a Delete-pill visual that is
// unmistakably disabled: neutral medium-gray box, lighter gray
// label, no Clickable, no hit area, no hover ripple. Shared
// between the chat-thread file card (Window.layoutFileCardDeleteDisabled)
// and the file tab (ConsoleWindow.layoutFileRowDeleteDisabled) so
// the visual is identical across surfaces.
//
// Why no Clickable: material.Button always wires hover-highlight
// ops via its internal Clickable, so the user would see a colour
// ripple on hover even when we semantically refuse the click —
// visually misleading. With no Clickable the region has no hit
// area, so the cursor stays as the surrounding default (not the
// hand pointer that the active red Delete button advertises) and
// there is no hover feedback at all.
//
// Colours are deliberately neutral medium gray (no red tint, no
// blue tint) so the user reads "disabled" rather than "tombstone"
// (gray-blue) or "ready to click" (red).
func layoutDisabledDeletePill(gtx layout.Context, theme *material.Theme, labelText string) layout.Dimensions {
	bg := color.NRGBA{R: 60, G: 60, B: 64, A: 255}
	fg := color.NRGBA{R: 130, G: 130, B: 130, A: 255}
	inset := layout.Inset{
		Top: unit.Dp(3), Bottom: unit.Dp(3),
		Left: unit.Dp(8), Right: unit.Dp(8),
	}
	macro := op.Record(gtx.Ops)
	dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		lbl := material.Caption(theme, labelText)
		lbl.Color = fg
		lbl.Font.Weight = 600
		lbl.TextSize = unit.Sp(11)
		return lbl.Layout(gtx)
	})
	call := macro.Stop()
	defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(5))).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: bg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	call.Add(gtx.Ops)
	return dims
}

// dispatchMessageDeleteAsync runs SendMessageDelete on a background
// goroutine with the standard 10s timeout and surfaces success /
// failure on the router status line. Shared between the chat
// context-menu Delete handler and the per-row Delete button on a
// file card. Pessimistic ordering: for outgoing the local row stays
// alive until the peer's message_delete_ack confirms — terminal
// status is then surfaced via TopicMessageDeleteCompleted, which
// the existing handleMessageDeleteOutcome subscriber picks up.
func (w *Window) dispatchMessageDeleteAsync(peer domain.PeerIdentity, target domain.MessageID) {
	w.router.SetSendStatus(w.t("status.message_deleting"))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := w.router.SendMessageDelete(ctx, peer, target); err != nil {
			w.router.SetSendStatus(w.t("status.message_delete_failed", err.Error()))
			return
		}
		w.router.SetSendStatus(w.t("status.message_delete_dispatched"))
	}()
}

func (w *Window) localNodeErrorRow() string {
	if err := w.runtime.Error(); err != "" {
		return w.t("node.error", err)
	}
	return w.t("node.error", w.t("node.error.none"))
}

func (w *Window) snapPreview(recipient domain.PeerIdentity) string {
	me := w.snap.MyAddress
	ps, ok := w.snap.Peers[recipient]
	if ok && ps.Preview.Body != "" {
		if ps.Preview.Sender == me {
			return "You: " + ps.Preview.Body
		}
		return ps.Preview.Body
	}
	return ""
}

// snapRecipients builds the sidebar recipient list from the router's peer
// state. Contacts are managed exclusively by the router: loaded from
// chatlog at startup, added on incoming messages, removed via RemovePeer.
// No polling or external contact source is involved.
func (w *Window) snapRecipients() []domain.PeerIdentity {
	recipients := make([]domain.PeerIdentity, 0, len(w.snap.Peers))
	me := w.snap.MyAddress
	for id := range w.snap.Peers {
		if id != me && id != "" {
			recipients = append(recipients, id)
		}
	}
	sort.Slice(recipients, func(i, j int) bool { return recipients[i] < recipients[j] })
	merged := mergeRecipientOrder(recipients, w.snap.PeerOrder)
	sortSidebarPeers(merged, w.snap)
	return merged
}

func (w *Window) recipientButton(id domain.PeerIdentity) *widget.Clickable {
	if btn, ok := w.recipientButtons[id]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.recipientButtons[id] = btn
	return btn
}

// rightClickState is a tag for receiving secondary-button pointer events
// on a recipient card. Each recipient gets its own tag so Gio routes
// events correctly.
type rightClickState struct {
	pressed bool
}

// cachedMsg holds pre-indexed message metadata for O(1) reply quote rendering.
type cachedMsg struct {
	Body      string
	Sender    domain.PeerIdentity
	Timestamp time.Time
	Index     int // position in ActiveMessages slice for scroll-to
}

func (w *Window) msgRightClickState(id string) *rightClickState {
	if s, ok := w.msgRightClick[id]; ok {
		return s
	}
	s := new(rightClickState)
	w.msgRightClick[id] = s
	return s
}

func (w *Window) recipientRightClickState(id domain.PeerIdentity) *rightClickState {
	if s, ok := w.recipientRightClick[id]; ok {
		return s
	}
	s := new(rightClickState)
	w.recipientRightClick[id] = s
	return s
}

func (w *Window) ensureSelectedRecipient(recipients []domain.PeerIdentity) {
	selected := w.snap.ActivePeer

	if len(recipients) == 0 {
		if strings.TrimSpace(string(selected)) != "" {
			w.recipientEditor.SetText(string(selected))
		} else {
			w.recipientEditor.SetText("")
		}
		return
	}

	for _, recipient := range recipients {
		if recipient == selected {
			w.recipientEditor.SetText(string(recipient))
			return
		}
	}

	if strings.TrimSpace(string(selected)) != "" {
		w.recipientEditor.SetText(string(selected))
		return
	}

	// Auto-select first recipient. AutoSelectPeer sends seen receipts
	// the same way SelectPeer does — the chat is on screen.
	w.router.AutoSelectPeer(recipients[0])
	w.recipientEditor.SetText(string(recipients[0]))
	w.focusComposerPending = true
}

func mergeRecipientOrder(recipients, order []domain.PeerIdentity) []domain.PeerIdentity {
	if len(recipients) == 0 {
		return nil
	}
	known := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range recipients {
		known[recipient] = struct{}{}
	}
	out := make([]domain.PeerIdentity, 0, len(recipients))
	used := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range order {
		if _, ok := known[recipient]; !ok {
			continue
		}
		if _, ok := used[recipient]; ok {
			continue
		}
		used[recipient] = struct{}{}
		out = append(out, recipient)
	}
	for _, recipient := range recipients {
		if _, ok := used[recipient]; ok {
			continue
		}
		used[recipient] = struct{}{}
		out = append(out, recipient)
	}
	return out
}

func (w *Window) layoutUnreadBadge(gtx layout.Context, count int) layout.Dimensions {
	height := gtx.Dp(unit.Dp(24))
	width := gtx.Dp(unit.Dp(28))
	labelText := intToString(count)
	if count > 9 {
		labelText = "9+"
		width = gtx.Dp(unit.Dp(34))
	}
	gtx.Constraints.Min = image.Pt(width, height)
	gtx.Constraints.Max = image.Pt(width, height)
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			rr := clip.UniformRRect(image.Rectangle{Max: image.Pt(width, height)}, height/2)
			stack := clip.Stroke{
				Path:  rr.Path(gtx.Ops),
				Width: float32(gtx.Dp(unit.Dp(1))),
			}.Op().Push(gtx.Ops)
			defer stack.Pop()
			paint.ColorOp{Color: color.NRGBA{R: 221, G: 228, B: 240, A: 255}}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			return layout.Dimensions{Size: image.Pt(width, height)}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, labelText)
			label.Color = color.NRGBA{R: 232, G: 237, B: 247, A: 255}
			return layout.Inset{Left: unit.Dp(10), Top: unit.Dp(3)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Center.Layout(gtx, label.Layout)
			})
		}),
	)
}

// layoutReachableIndicator draws a small circle reflecting the routing table
// reachability of the identity. Three visual states:
//
//   - ReachableIDs == nil         → gray outline only (probe failed / data unavailable)
//   - ReachableIDs[id] == true    → green filled (at least one live route)
//   - ReachableIDs[id] == false   → gray filled  (no live route)
func (w *Window) layoutReachableIndicator(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity) layout.Dimensions {
	sz := gtx.Dp(unit.Dp(10))
	bounds := image.Pt(sz, sz)

	if status.ReachableIDs == nil {
		// Stroke-only circle: no reachability data available.
		strokeWidth := float32(gtx.Dp(unit.Dp(1.5)))
		stk := clip.Stroke{
			Path:  clip.Ellipse{Max: bounds}.Path(gtx.Ops),
			Width: strokeWidth,
		}.Op().Push(gtx.Ops)
		paint.ColorOp{Color: color.NRGBA{R: 96, G: 110, B: 130, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		stk.Pop()
		return layout.Dimensions{Size: bounds}
	}

	indicatorColor := color.NRGBA{R: 96, G: 110, B: 130, A: 255} // gray — unreachable
	if status.ReachableIDs[fingerprint] {
		indicatorColor = color.NRGBA{R: 72, G: 199, B: 142, A: 255} // green — reachable
	}
	defer clip.Ellipse{Max: bounds}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: indicatorColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	return layout.Dimensions{Size: bounds}
}

func ellipsize(s string, limit int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if limit <= 0 || len([]rune(s)) <= limit {
		return s
	}
	runes := []rune(s)
	if limit <= 1 {
		return string(runes[:limit])
	}
	return string(runes[:limit-1]) + "…"
}

func intToString(v int) string {
	return strconv.Itoa(v)
}

func (w *Window) layoutConversation(gtx layout.Context, recipient domain.PeerIdentity, conversation []service.DirectMessage) layout.Dimensions {
	w.chatViewportH = gtx.Constraints.Max.Y

	// Track cursor Y relative to the chat viewport (not the window).
	// This scoped tracker gives correct coordinates for scroll math
	// in applyDeferredScroll, since the chat area is offset from the
	// top of the window by headers, paddings, etc.
	defer clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, &w.chatCursorTag)
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: &w.chatCursorTag,
			Kinds:  pointer.Move | pointer.Press | pointer.Drag,
		})
		if !ok {
			break
		}
		if pe, ok := ev.(pointer.Event); ok {
			w.chatCursorY = int(pe.Position.Y)
		}
	}

	list := material.List(w.theme, &w.chatList)
	return list.Layout(gtx, len(conversation), func(gtx layout.Context, index int) layout.Dimensions {
		message := conversation[index]
		return layout.Inset{Bottom: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.layoutChatBubble(gtx, recipient, message)
		})
	})
}

func (w *Window) layoutChatBubble(gtx layout.Context, recipient domain.PeerIdentity, message service.DirectMessage) layout.Dimensions {
	me := w.snap.MyAddress
	isMine := message.Sender == me

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		if isMine {
			return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.chatBubbleCard(gtx, message, true, w.t("chat.you_label"))
			})
		}
		return layout.W.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.chatBubbleCard(gtx, message, false, w.peerDisplayName(recipient))
		})
	})
}

func (w *Window) chatBubbleCard(gtx layout.Context, message service.DirectMessage, isMine bool, author string) layout.Dimensions {
	maxWidth := gtx.Dp(unit.Dp(520))
	if gtx.Constraints.Max.X < maxWidth {
		maxWidth = gtx.Constraints.Max.X
	}
	gtx.Constraints.Max.X = maxWidth

	// Read right-click events from the previous frame.
	rc := w.msgRightClickState(message.ID)
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: rc,
			Kinds:  pointer.Press,
		})
		if !ok {
			break
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonSecondary) {
			msgCopy := message
			w.msgContextMsg = &msgCopy
			w.msgContextPos = w.lastCursorPos
		}
	}

	borderColor := color.NRGBA{R: 55, G: 68, B: 86, A: 255}
	authorColor := color.NRGBA{R: 162, G: 176, B: 196, A: 255}
	statusColor := color.NRGBA{R: 110, G: 130, B: 160, A: 180}
	if isMine {
		borderColor = color.NRGBA{R: 74, G: 109, B: 176, A: 255}
		authorColor = color.NRGBA{R: 173, G: 205, B: 255, A: 255}
	}

	border := widget.Border{Color: borderColor, CornerRadius: unit.Dp(8), Width: unit.Dp(1)}

	// Record the bubble content first to measure its size.
	macro := op.Record(gtx.Ops)
	dims := border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			children := []layout.FlexChild{}

			// Show quoted message if this is a reply.
			if message.ReplyTo != "" {
				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutReplyQuote(gtx, message.ReplyTo, isMine)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
				)
			}

			children = append(children,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{
						Axis:      layout.Horizontal,
						Alignment: layout.Middle,
					}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, author)
							label.Color = authorColor
							return label.Layout(gtx)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, message.Timestamp.Local().Format("02.01.2006 15:04"))
							label.Color = color.NRGBA{R: 160, G: 185, B: 220, A: 255}
							return label.Layout(gtx)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					// Render file card for file_announce messages instead of plain text.
					if message.Command == domain.DMCommandFileAnnounce && message.CommandData != "" {
						return w.layoutFileCard(gtx, message, isMine)
					}

					sel := w.messageSelectable(message.ID)
					sel.SetText(message.Body)
					textColor := color.NRGBA{R: 245, G: 247, B: 250, A: 255}
					selColor := color.NRGBA{R: 72, G: 96, B: 140, A: 180}

					textMacro := op.Record(gtx.Ops)
					paint.ColorOp{Color: textColor}.Add(gtx.Ops)
					textMaterial := textMacro.Stop()

					selMacro := op.Record(gtx.Ops)
					paint.ColorOp{Color: selColor}.Add(gtx.Ops)
					selMaterial := selMacro.Stop()

					return sel.Layout(gtx, w.theme.Shaper, font.Font{Typeface: w.theme.Face}, w.theme.TextSize, textMaterial, selMaterial)
				}),
			)

			if isMine && (message.DeliveredAt.Valid() || message.ReceiptStatus == "queued" || message.ReceiptStatus == "retrying" || message.ReceiptStatus == "failed" || message.ReceiptStatus == "expired" || message.ReceiptStatus == "sent" || message.ReceiptStatus == "delivered" || message.ReceiptStatus == "seen") {
				children = append(children,
					layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						statusText := ""
						switch {
						case message.ReceiptStatus == "seen" && message.DeliveredAt.Valid():
							statusText = "✓✓ " + message.DeliveredAt.Time().Local().Format("02.01.2006 15:04")
						case message.ReceiptStatus == "seen":
							statusText = "✓✓"
						case message.ReceiptStatus == "delivered" && message.DeliveredAt.Valid():
							statusText = "✓ " + message.DeliveredAt.Time().Local().Format("02.01.2006 15:04")
						case message.ReceiptStatus == "delivered":
							statusText = "✓"
						case message.DeliveredAt.Valid():
							statusText = "✓ " + message.DeliveredAt.Time().Local().Format("02.01.2006 15:04")
						case message.ReceiptStatus == "queued":
							statusText = w.t("chat.status.queued")
						case message.ReceiptStatus == "retrying":
							statusText = w.t("chat.status.retrying")
						case message.ReceiptStatus == "failed":
							statusText = w.t("chat.status.failed")
						case message.ReceiptStatus == "expired":
							statusText = w.t("chat.status.expired")
						case message.ReceiptStatus == "sent":
							statusText = w.t("chat.status.sent")
						}
						return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							label := material.Caption(w.theme, statusText)
							label.Color = statusColor
							return label.Layout(gtx)
						})
					}),
				)
			}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
		})
	})
	bubbleCall := macro.Stop()

	// Create a clip area sized to the bubble so that event.Op is scoped
	// exclusively to this message. Without this, widget.Border does not
	// create an input clip and all message tags share the parent list's
	// area, causing the wrong message to be selected on right-click.
	defer clip.Rect(image.Rectangle{Max: dims.Size}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, rc)
	bubbleCall.Add(gtx.Ops)

	return dims
}

// messageSelectable returns a reusable Selectable widget for the given
// message ID, creating one on first access. This allows users to select
// and copy message text in the chat view. The cache is cleared when the
// active conversation changes to prevent unbounded growth across
// different chat peers.
func (w *Window) messageSelectable(id string) *widget.Selectable {
	if peer := w.snap.ActivePeer; peer != w.lastChatPeer {
		w.messageSelectables = make(map[string]*widget.Selectable)
		w.msgRightClick = make(map[string]*rightClickState)
		w.replyQuoteTags = make(map[string]*widget.Clickable)
		w.lastChatPeer = peer
	}
	sel := w.messageSelectables[id]
	if sel == nil {
		sel = &widget.Selectable{}
		w.messageSelectables[id] = sel
	}
	return sel
}

func searchKnownIdentities(knownIDs []string, recipients []domain.PeerIdentity, self domain.PeerIdentity, query string) []domain.PeerIdentity {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return nil
	}

	alreadyListed := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range recipients {
		alreadyListed[recipient] = struct{}{}
	}

	results := make([]domain.PeerIdentity, 0, len(knownIDs))
	seen := make(map[domain.PeerIdentity]struct{}, len(knownIDs))
	for _, raw := range knownIDs {
		raw = strings.TrimSpace(raw)
		if raw == "" || domain.PeerIdentity(raw) == self {
			continue
		}
		id := domain.PeerIdentity(raw)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := alreadyListed[id]; ok {
			continue
		}
		if !strings.Contains(strings.ToLower(raw), query) {
			continue
		}
		results = append(results, id)
	}

	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	return results
}

func recipientsToChildren(values []domain.PeerIdentity, render func(layout.Context, domain.PeerIdentity) layout.Dimensions) []layout.FlexChild {
	children := make([]layout.FlexChild, 0, len(values))
	for _, value := range values {
		value := value
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return render(gtx, value)
		}))
	}
	return children
}

func shortFingerprint(value string) string {
	if len(value) <= 14 {
		return value
	}
	return value[:8] + "..." + value[len(value)-6:]
}

func joinOrNone(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ", ")
}

func fallback(value, alt string) string {
	if strings.TrimSpace(value) == "" {
		return alt
	}
	return value
}

// peerDisplayName returns the user-assigned alias for the identity,
// falling back to shortFingerprint when no alias is set.
func (w *Window) peerDisplayName(identity domain.PeerIdentity) string {
	if w.prefs != nil {
		if alias := w.prefs.Alias(identity); alias != "" {
			return alias
		}
	}
	return shortFingerprint(string(identity))
}

func (w *Window) t(key string, args ...any) string {
	return translate(w.language, key, args...)
}

// peerOnline reports whether the peer with the given identity has at
// least one usable next-hop right now (direct session OR live route).
// Used to gate destructive UI actions (Delete, Download, Restart)
// that require the peer to be reachable on the wire — issuing them
// when the peer is offline would either fail immediately (RPC) or
// queue indefinitely (DM router retry budget).
//
// Source of truth is NodeStatus.ReachableIDs, which is rebuilt every
// status poll from the routing table. A nil ReachableIDs map means
// "unknown" (status not yet polled); we treat unknown as offline so
// the UI errs on the safe side.
func (w *Window) peerOnline(identity domain.PeerIdentity) bool {
	if w == nil || w.router == nil {
		return false
	}
	snap := w.router.Snapshot()
	if snap.NodeStatus.ReachableIDs == nil {
		return false
	}
	return snap.NodeStatus.ReachableIDs[identity]
}

// contextMenuDeleteEnabled reports whether the Delete item in the
// open message context menu is actionable. Direction-aware:
//
//   - Outgoing (Sender == self): gate on peerOnline(Recipient).
//     The wire-side delete dispatches a message_delete control
//     DM and waits for the peer's ack (pessimistic ordering);
//     burning the retry budget on an offline recipient is the
//     visible failure mode this guard prevents.
//
//   - Incoming (Sender != self): always enabled. The router
//     path for inbound messages (DMRouter.SendMessageDelete with
//     direction == receive) is local-only — it removes the local
//     chatlog row + any backing file synchronously and never
//     dispatches a control DM. Gating on peerOnline at the UI
//     layer would block users from cleaning up incoming rows
//     from an offline peer for no underlying reason.
//
// Returns false when no menu is open (msgContextMsg is nil) so
// callers laying out the disabled visual style fall back to the
// safe default.
func (w *Window) contextMenuDeleteEnabled() bool {
	if w == nil || w.msgContextMsg == nil {
		return false
	}
	msg := *w.msgContextMsg
	me := w.router.MyAddress()
	if msg.Sender != me {
		// Incoming: local-only delete, no peer round-trip.
		return true
	}
	return w.peerOnline(msg.Recipient)
}

func (w *Window) layoutConsoleButton(gtx layout.Context) layout.Dimensions {
	btn := material.Button(w.theme, &w.consoleButton, w.t("header.console"))
	btn.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
	btn.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
	return btn.Layout(gtx)
}

func (w *Window) layoutUpdateBadge(gtx layout.Context) layout.Dimensions {
	if !w.nodeUpdateAvailable() {
		return layout.Dimensions{}
	}
	btn := material.Button(w.theme, &w.updateButton, w.t("header.update"))
	btn.Background = color.NRGBA{R: 230, G: 126, B: 34, A: 255}
	btn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	return btn.Layout(gtx)
}

// nodeUpdateAvailable returns the node-computed update_available signal.
// The policy decision (which peers reported, how many, threshold) lives
// in the node layer — Desktop only renders the pre-computed result.
func (w *Window) nodeUpdateAvailable() bool {
	if w.snap.NodeStatus.AggregateStatus == nil {
		return false
	}
	return w.snap.NodeStatus.AggregateStatus.UpdateAvailable
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

// openFile opens a local file with the system default application.
// On macOS: open, on Windows: rundll32, on Linux: xdg-open.
func openFile(path string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", path)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", path)
	default:
		cmd = exec.Command("xdg-open", path)
	}
	_ = cmd.Start()
}

// revealFileInDir opens the system file manager with the file selected
// (highlighted). On macOS Finder selects the file via "open -R". On
// Windows Explorer selects via "/select,". On Linux there is no universal
// "select file" protocol, so we open the containing directory and, as a
// best-effort, try dbus-based file selection (Nautilus/Dolphin/Thunar)
// before falling back to xdg-open on the parent directory.
func revealFileInDir(path string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		// -R = reveal in Finder and select the file.
		cmd = exec.Command("open", "-R", path)
	case "windows":
		// /select, highlights the file in Explorer.
		cmd = exec.Command("explorer", "/select,", path)
	default:
		// Best-effort: try dbus-send to org.freedesktop.FileManager1 which
		// is supported by Nautilus, Dolphin, Thunar, and other modern file
		// managers. If it fails, fall back to opening the directory.
		//
		// Build a properly escaped file:// URI via net/url so that paths
		// with spaces, #, %, Cyrillic, and other special characters are
		// transmitted correctly over D-Bus.
		fileURI := (&url.URL{Scheme: "file", Path: path}).String()
		dbusCmd := exec.Command("dbus-send", "--print-reply",
			"--dest=org.freedesktop.FileManager1",
			"/org/freedesktop/FileManager1",
			"org.freedesktop.FileManager1.ShowItems",
			"array:string:"+fileURI, "string:")
		if err := dbusCmd.Start(); err == nil {
			_ = dbusCmd.Wait()
			return
		}
		// Fallback: open the containing directory.
		cmd = exec.Command("xdg-open", filepath.Dir(path))
	}
	_ = cmd.Start()
}

func (w *Window) openConsoleWindow() {
	w.consoleMu.Lock()
	if w.consoleOpen {
		w.consoleMu.Unlock()
		return
	}
	w.consoleOpen = true
	w.consoleMu.Unlock()

	console := NewConsoleWindow(w, func() {
		w.consoleMu.Lock()
		w.consoleOpen = false
		w.consoleMu.Unlock()
	})
	console.Open()
}

func (w *Window) languageButton(code string) *widget.Clickable {
	if btn, ok := w.languageOptions[code]; ok {
		return btn
	}

	btn := new(widget.Clickable)
	w.languageOptions[code] = btn
	return btn
}

func (w *Window) layoutLanguageSelectorInline(gtx layout.Context) layout.Dimensions {
	return layout.Flex{
		Axis:      layout.Horizontal,
		Alignment: layout.Middle,
	}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(w.theme, w.t("header.language"))
			label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
		layout.Rigid(w.layoutLanguageDropdown),
	)
}

func (w *Window) layoutLanguageDropdown(gtx layout.Context) layout.Dimensions {
	label := currentLanguageLabel(w.language)
	btn := material.Button(w.theme, &w.languageToggle, label+"  v")
	btn.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
	btn.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
	return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		width := gtx.Dp(unit.Dp(languageButtonWidth))
		if gtx.Constraints.Max.X < width {
			width = gtx.Constraints.Max.X
		}
		gtx.Constraints.Min.X = width
		gtx.Constraints.Max.X = width
		return btn.Layout(gtx)
	})
}

func (w *Window) layoutLanguageOverlay(gtx layout.Context) layout.Dimensions {
	x := gtx.Constraints.Max.X - gtx.Dp(unit.Dp(windowInset)) - gtx.Dp(unit.Dp(languageMenuWidth))
	if x < 0 {
		x = 0
	}
	y := gtx.Dp(unit.Dp(windowInset + languageOverlayTop))

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	defer stack.Pop()

	menuGTX := gtx
	menuGTX.Constraints.Min.X = gtx.Dp(unit.Dp(languageMenuWidth))
	menuGTX.Constraints.Max.X = gtx.Dp(unit.Dp(languageMenuWidth))
	menuGTX.Constraints.Min.Y = gtx.Dp(unit.Dp(languageMenuHeight))
	menuGTX.Constraints.Max.Y = gtx.Dp(unit.Dp(languageMenuHeight))
	_ = w.languageMenuCard(menuGTX)

	return layout.Dimensions{}
}

// layoutContextMenuOverlay renders the right-click context menu for a recipient identity.
// It shows "Copy identity" and "Delete identity" options. Delete requires a confirmation step.
func (w *Window) layoutContextMenuOverlay(gtx layout.Context) layout.Dimensions {
	// Dismiss context menu on click outside.
	// We draw a full-screen transparent clickable area behind the menu.
	dismissArea := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, &w.contextMenuPeer)
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: &w.contextMenuPeer, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			w.contextMenuPeer = ""
			w.showDeleteConfirm = false
			w.showClearChatConfirm = false
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
		}
	}
	dismissArea.Pop()

	menuWidth := gtx.Dp(unit.Dp(220))
	windowW := gtx.Constraints.Max.X
	windowH := gtx.Constraints.Max.Y

	// Measure menu height first so we can flip upward when near the bottom.
	measureGTX := gtx
	measureGTX.Constraints.Min.X = menuWidth
	measureGTX.Constraints.Max.X = menuWidth
	measureGTX.Constraints.Min.Y = 0
	measureGTX.Constraints.Max.Y = windowH
	macro := op.Record(measureGTX.Ops)
	dims := w.contextMenuCard(measureGTX)
	menuCall := macro.Stop()

	x := w.contextMenuPos.X
	y := w.contextMenuPos.Y

	// Flip horizontally if overflows right edge.
	if x+menuWidth > windowW {
		x = windowW - menuWidth
	}
	if x < 0 {
		x = 0
	}

	// Flip vertically if overflows bottom edge.
	if y+dims.Size.Y > windowH {
		y = y - dims.Size.Y
		if y < 0 {
			y = 0
		}
	}

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	menuCall.Add(gtx.Ops)
	stack.Pop()

	return layout.Dimensions{}
}

func (w *Window) contextMenuCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 72, G: 85, B: 106, A: 255}
	bgColor := color.NRGBA{R: 28, G: 34, B: 44, A: 255}
	rr := gtx.Dp(unit.Dp(8))
	borderWidth := gtx.Dp(unit.Dp(1))

	// Measure content to know the total size.
	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(6)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			if w.showDeleteConfirm {
				return w.layoutDeleteConfirmMenu(gtx)
			}
			if w.showClearChatConfirm {
				return w.layoutClearChatConfirmMenu(gtx)
			}
			if w.showAliasEditor {
				return w.layoutAliasEditorMenu(gtx)
			}
			return w.layoutContextMenuItems(gtx)
		})
	})
	contentCall := macro.Stop()

	bounds := image.Rectangle{Max: dims.Size}

	// Clip everything to the rounded rectangle so corners are clean.
	defer clip.UniformRRect(bounds, rr).Push(gtx.Ops).Pop()

	// 1. Border fill (covers the full rounded rect).
	paint.ColorOp{Color: borderColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	// 2. Background fill inset by border width.
	innerBounds := image.Rectangle{
		Min: image.Pt(borderWidth, borderWidth),
		Max: image.Pt(dims.Size.X-borderWidth, dims.Size.Y-borderWidth),
	}
	innerRR := rr - borderWidth
	if innerRR < 0 {
		innerRR = 0
	}
	defer clip.UniformRRect(innerBounds, innerRR).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: bgColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	// 3. Replay content on top.
	contentCall.Add(gtx.Ops)

	return dims
}

func (w *Window) layoutContextMenuItems(gtx layout.Context) layout.Dimensions {
	aliasLabel := w.t("context.set_alias")
	if w.prefs != nil && w.prefs.Alias(w.contextMenuPeer) != "" {
		aliasLabel = w.t("context.edit_alias")
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAlias, aliasLabel,
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuCopy, w.t("context.copy_identity"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDelete, w.t("context.delete_identity"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			// "Delete chat for everyone" (bulk wipe both sides). The
			// item is direction-symmetric (no sender/receiver split
			// like for per-message delete) but still needs the peer
			// online for the wire round-trip to converge — render
			// disabled when offline. The disabled variant has no
			// hit area at all (no Clickable), so the same defensive
			// offline check inside the click handler is a belt-and-
			// braces guard rather than the only line of defence.
			if !w.peerOnline(w.contextMenuPeer) {
				return w.contextMenuItemDisabled(gtx, w.t("context.clear_chat_both"))
			}
			return w.contextMenuItem(gtx, &w.ctxMenuClearChat, w.t("context.clear_chat_both"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
	)
}

func (w *Window) contextMenuHeader(gtx layout.Context) layout.Dimensions {
	return layout.Inset{
		Left: unit.Dp(12), Right: unit.Dp(12),
		Top: unit.Dp(8), Bottom: unit.Dp(4),
	}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		label := material.Caption(w.theme, w.peerDisplayName(w.contextMenuPeer))
		label.Color = color.NRGBA{R: 140, G: 155, B: 178, A: 255}
		return label.Layout(gtx)
	})
}

func (w *Window) contextMenuSeparator(gtx layout.Context) layout.Dimensions {
	return layout.Inset{
		Left: unit.Dp(8), Right: unit.Dp(8),
		Top: unit.Dp(2), Bottom: unit.Dp(4),
	}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		height := gtx.Dp(unit.Dp(1))
		sz := image.Pt(gtx.Constraints.Max.X, height)
		defer clip.Rect(image.Rectangle{Max: sz}).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: color.NRGBA{R: 55, G: 65, B: 82, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		return layout.Dimensions{Size: sz}
	})
}

func (w *Window) layoutDeleteConfirmMenu(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("context.delete_confirm"))
			label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
			return layout.Inset{Left: unit.Dp(12), Right: unit.Dp(12), Top: unit.Dp(2), Bottom: unit.Dp(6)}.Layout(gtx, label.Layout)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteConfirm, w.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteCancel, w.t("context.delete_no"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

// layoutClearChatConfirmMenu renders the confirmation step for the
// "Delete chat for everyone" sidebar action. Visual structure mirrors
// layoutDeleteConfirmMenu so the user reads the same shape for both
// destructive sidebar actions; the body text and button widgets are
// the only differences. Kept as a separate menu (rather than reusing
// layoutDeleteConfirmMenu with a parameterised label) so the
// confirm/cancel widget targets stay distinct — sharing widgets
// would let a stale click event from the per-identity delete path
// fire the wipe path on the next frame.
func (w *Window) layoutClearChatConfirmMenu(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("context.clear_chat_confirm"))
			label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
			return layout.Inset{Left: unit.Dp(12), Right: unit.Dp(12), Top: unit.Dp(2), Bottom: unit.Dp(6)}.Layout(gtx, label.Layout)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuClearChatConfirm, w.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuClearChatCancel, w.t("context.delete_no"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

func (w *Window) layoutAliasEditorMenu(gtx layout.Context) layout.Dimensions {
	// Handle Enter key as save shortcut.
	for {
		ev, ok := w.aliasEditor.Update(gtx)
		if !ok {
			break
		}
		if submit, ok := ev.(widget.SubmitEvent); ok {
			alias := strings.TrimSpace(submit.Text)
			if w.prefs != nil {
				w.prefs.SetAlias(w.contextMenuPeer, alias)
				_ = w.prefs.Save()
			}
			w.contextMenuPeer = ""
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
			return layout.Dimensions{}
		}
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Left: unit.Dp(12), Right: unit.Dp(12),
				Top: unit.Dp(4), Bottom: unit.Dp(6),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				ed := material.Editor(w.theme, &w.aliasEditor, w.t("context.alias_placeholder"))
				ed.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				ed.HintColor = color.NRGBA{R: 120, G: 135, B: 158, A: 255}
				gtx.Constraints.Min.X = gtx.Dp(unit.Dp(160))
				return ed.Layout(gtx)
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasSave, w.t("context.alias_save"),
				color.NRGBA{R: 130, G: 200, B: 130, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasCancel, w.t("context.alias_cancel"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

func (w *Window) contextMenuItem(gtx layout.Context, btn *widget.Clickable, label string, fg color.NRGBA) layout.Dimensions {
	hoverBg := color.NRGBA{R: 42, G: 52, B: 68, A: 255}

	return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
		if btn.Hovered() {
			fill(gtx, hoverBg)
		}
		return layout.Inset{
			Top: unit.Dp(8), Bottom: unit.Dp(8),
			Left: unit.Dp(12), Right: unit.Dp(12),
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			lbl := material.Body2(w.theme, label)
			lbl.Color = fg
			return lbl.Layout(gtx)
		})
	})
}

// contextMenuItemDisabled renders a context-menu entry as a
// neutral-gray label with no Clickable wrapping — no hover ripple,
// no item-selected highlight, no event consumption.
//
// Earlier iterations passed the active-item colour through and dimmed
// it (the idea being that a faint-red Delete entry would read as
// "this is the Delete row, just disabled"). User feedback was that
// faint red still looks active, especially against the menu's dark
// background — gray reads more decisively as "this option is not
// available right now".
//
// Why this and not contextMenuItem with a dimmed colour: the
// material.Clickable wrapping in contextMenuItem still draws a
// hover background and consumes pointer events even with reduced
// label alpha — so the user sees a hover ripple on what should be
// an unactionable item. The static path here drops the wrapping
// entirely, leaving just an inset Body2 label with no event
// handling. The dims, padding, and shape match contextMenuItem's
// so the menu doesn't reflow when an item flips between enabled
// and disabled.
func (w *Window) contextMenuItemDisabled(gtx layout.Context, label string) layout.Dimensions {
	fg := color.NRGBA{R: 130, G: 130, B: 130, A: 255}
	return layout.Inset{
		Top: unit.Dp(8), Bottom: unit.Dp(8),
		Left: unit.Dp(12), Right: unit.Dp(12),
	}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		lbl := material.Body2(w.theme, label)
		lbl.Color = fg
		return lbl.Layout(gtx)
	})
}

func (w *Window) languageMenuCard(gtx layout.Context) layout.Dimensions {
	fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

	return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		items := make([]layout.FlexChild, 0, len(supportedLanguages)*2)
		for i, option := range supportedLanguages {
			opt := option
			btn := w.languageButton(opt.Code)
			items = append(items, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				for btn.Clicked(gtx) {
					w.language = normalizeLanguage(opt.Code)
					w.showLanguageMenu = false
					if w.prefs != nil {
						w.prefs.Language = w.language
						_ = w.prefs.Save()
					}
					if w.window != nil {
						w.window.Invalidate()
					}
				}

				style := material.Button(w.theme, btn, opt.Label+" - "+localizedLanguageName(opt.Code))
				if opt.Code == w.language {
					style.Background = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
				} else {
					style.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
				}
				style.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				return style.Layout(gtx)
			}))
			if i < len(supportedLanguages)-1 {
				items = append(items, layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout))
			}
		}
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, items...)
	})
}

func (w *Window) card(gtx layout.Context, titleText string, rows []string, extras ...func(layout.Context) layout.Dimensions) layout.Dimensions {
	// Record the content layout first so we know the actual height,
	// then draw the background to match. Without this, fill() would
	// use gtx.Constraints.Max.Y which stretches Rigid cards (like the
	// composer) to the bottom of the window.
	macro := op.Record(gtx.Ops)

	inset := layout.UniformInset(unit.Dp(8))
	dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := make([]layout.FlexChild, 0, len(rows)+len(extras)+2)
		if strings.TrimSpace(titleText) != "" {
			children = append(children,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(w.theme, unit.Sp(16), titleText)
					label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
			)
		}

		for _, row := range rows {
			text := row
			children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(w.theme, text)
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}))
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout))
		}

		for _, extra := range extras {
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout))
			children = append(children, layout.Rigid(extra))
		}

		return layout.Flex{
			Axis: layout.Vertical,
		}.Layout(gtx, children...)
	})

	contentOps := macro.Stop()

	// Draw the background sized to the actual content (or Max.Y for
	// Flexed cards like the chat area that should fill available space).
	bgHeight := dims.Size.Y
	if gtx.Constraints.Min.Y > bgHeight {
		bgHeight = gtx.Constraints.Min.Y
	}
	bgStack := clip.Rect{Max: image.Pt(gtx.Constraints.Max.X, bgHeight)}.Push(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 21, G: 26, B: 34, A: 255}}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	bgStack.Pop()

	contentOps.Add(gtx.Ops)
	return dims
}

// handleMsgContextMenuActions processes clicks on the message context menu items.
func (w *Window) handleMsgContextMenuActions(gtx layout.Context) {
	if w.msgContextMsg == nil {
		return
	}

	if w.msgCtxReply.Clicked(gtx) {
		msgCopy := *w.msgContextMsg
		w.replyToMsg = &msgCopy
		w.msgContextMsg = nil
		gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	if w.msgCtxCopy.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(w.msgContextMsg.Body)),
		})
		w.router.SetSendStatus(w.t("status.message_copied"))
		w.msgContextMsg = nil
		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}

	if w.msgCtxDelete.Clicked(gtx) {
		// Determine the conversation peer relative to this user. For an
		// outgoing message Sender == self, so peer = Recipient. For an
		// incoming message Sender != self, so peer = Sender. Either way
		// the peer is the "other party" we will tell to mirror the
		// deletion (the recipient may reject if the message Flag does
		// not authorize them — that's the wire-side concern, not ours).
		msg := *w.msgContextMsg
		me := w.router.MyAddress()
		peer := msg.Recipient
		if msg.Sender != me {
			peer = msg.Sender
		}

		// Offline-peer guard, OUTGOING only. Incoming messages take
		// the local-only delete path inside DMRouter.SendMessageDelete:
		// the local row + any backing file are removed synchronously
		// without a wire round-trip, so peer reachability is
		// irrelevant for incoming. Outgoing messages need the peer
		// to ack the deletion (pessimistic ordering — see
		// docs/dm-commands.md), so we refuse the click when the
		// peer is offline rather than burning the retry budget.
		isOutgoing := msg.Sender == me
		if isOutgoing && !w.peerOnline(peer) {
			w.msgContextMsg = nil
			w.router.SetSendStatus(w.t("status.message_delete_peer_offline"))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		targetID := domain.MessageID(msg.ID)

		w.msgContextMsg = nil
		w.router.SetSendStatus(w.t("status.message_deleting"))

		go func(peer domain.PeerIdentity, target domain.MessageID, outgoing bool) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := w.router.SendMessageDelete(ctx, peer, target); err != nil {
				w.router.SetSendStatus(w.t("status.message_delete_failed", err.Error()))
				return
			}
			if !outgoing {
				// Incoming = local-only path. SendMessageDelete has
				// already removed the chatlog row, cleaned up file
				// state, and published TopicMessageDeleteCompleted
				// with the terminal "deleted" outcome. The Window's
				// async handleMessageDeleteOutcome subscriber may
				// have already written the final status before this
				// goroutine resumes; setting "Delete request sent;
				// waiting for peer…" here would race-overwrite that
				// terminal status with a misleading wire-progress
				// caption (no peer ack is coming because no peer
				// round-trip happened). Skip the dispatched status.
				return
			}
			// Outgoing: do NOT mark this as "deleted" yet. The local
			// row stays alive until the peer's message_delete_ack
			// confirms a success status — see SendMessageDelete +
			// handleInboundMessageDeleteAck for pessimistic ordering.
			// The terminal UI status (deleted / not_found / denied /
			// immutable / abandoned) arrives via
			// TopicMessageDeleteCompleted; handleMessageDeleteOutcome
			// owns the final update.
			w.router.SetSendStatus(w.t("status.message_delete_dispatched"))
		}(peer, targetID, isOutgoing)

		if w.window != nil {
			w.window.Invalidate()
		}
		return
	}
}

// handleMessageDeleteOutcome is the ebus subscriber invoked when an
// in-flight message_delete reaches a terminal state. The synchronous
// SendMessageDelete return cannot tell us whether the peer accepted,
// rejected, or never replied — this callback is the only place where
// the UI learns the wire-side outcome.
//
// Status messages are intentionally short and non-modal: they go
// through the same status bar the chat send path uses, so they get
// replaced naturally when the next user action takes place.
func (w *Window) handleMessageDeleteOutcome(outcome ebus.MessageDeleteOutcome) {
	if w.router == nil {
		return
	}
	var msg string
	switch {
	case outcome.Abandoned:
		msg = w.t("status.message_delete_abandoned")
	case outcome.Status == domain.MessageDeleteStatusDeleted:
		msg = w.t("status.message_deleted")
	case outcome.Status == domain.MessageDeleteStatusNotFound:
		// Idempotent success: the peer never had the message or had
		// already deleted it on a previous attempt.
		msg = w.t("status.message_deleted")
	case outcome.Status == domain.MessageDeleteStatusDenied:
		msg = w.t("status.message_delete_denied")
	case outcome.Status == domain.MessageDeleteStatusImmutable:
		msg = w.t("status.message_delete_immutable")
	default:
		// Unknown wire-level status — fall back to a neutral abandoned
		// message rather than silently lying that delivery succeeded.
		msg = w.t("status.message_delete_abandoned")
	}
	w.router.SetSendStatus(msg)
	if w.window != nil {
		w.window.Invalidate()
	}
}

// dispatchConversationDeleteAsync reserves the wipe slot
// synchronously (raising the outgoing barrier before this function
// returns) and runs the chatlog snapshot + initial wire dispatch on
// a background goroutine. Mirrors dispatchMessageDeleteAsync above.
//
// Why two phases: dispatching the whole flow on the goroutine
// leaves a scheduling gap between confirm-click and reservation. A
// fast Enter / click during that gap can pass through SendMessage's
// barrier check and reach the peer ahead of conversation_delete —
// the receiver would sweep the row, the requester's snapshot would
// not contain it (it had not yet committed when the snapshot ran),
// and the post-ack local sweep would leave it alive on the
// requester side. Calling BeginConversationDelete on the UI thread
// closes that window: by the time we return to the event loop,
// IsConversationDeletePending = true and SendMessage /
// SendFileAnnounce return ErrConversationDeleteInflight for this
// peer. CompleteConversationDelete then runs the snapshot + wire
// dispatch on the goroutine without re-opening the gap.
//
// Pessimistic ordering: local rows STAY in chatlog until the peer's
// conversation_delete_ack arrives with status applied. The
// goroutine waits only on the wire submission so the UI status
// reflects "request dispatched, awaiting peer"; the terminal
// outcome (applied / abandoned / applied+LocalCleanupFailed)
// arrives asynchronously through TopicConversationDeleteCompleted
// and is handled by handleConversationDeleteOutcome, which is also
// the place where the local sweep runs (inside the router's ack
// handler, only after the peer confirms).
func (w *Window) dispatchConversationDeleteAsync(peer domain.PeerIdentity) {
	if w.router == nil {
		return
	}
	requestID, err := w.router.BeginConversationDelete(peer)
	if err != nil {
		w.router.SetSendStatus(w.t("status.clear_chat_failed", err.Error()))
		return
	}
	if requestID == "" {
		// Duplicate click — a wipe is already in flight for this
		// peer. The existing request continues; nothing to do.
		return
	}
	dispatching := w.t("status.clear_chat_dispatching")
	w.router.SetSendStatus(dispatching)
	go func(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, dispatching string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := w.router.CompleteConversationDelete(ctx, peer, requestID)
		if err == nil {
			// Atomic compare-and-swap to defeat the TOCTOU race
			// against a fast peer ACK. handleConversationDeleteOutcome
			// runs in a separate goroutine driven by the ebus
			// subscriber and may have already set the terminal
			// status (applied / abandoned / applied+LocalCleanupFailed)
			// by the time we reach this line. SetSendStatusIfCurrent
			// only writes "wipe request sent" when the live
			// status is still the "dispatching…" we wrote above;
			// any other value means a higher-priority update
			// landed first and must be preserved.
			w.router.SetSendStatusIfCurrent(dispatching, w.t("status.clear_chat_dispatched"))
			return
		}
		// Reservation lost between Begin and Complete — typically
		// because the goroutine startup gap exceeded the TTL and
		// the reaper already dropped the entry (and published the
		// Abandoned outcome that handleConversationDeleteOutcome
		// has already turned into a "abandoned" status string).
		// No wire command went out under this requestID, so we
		// must NOT advertise "dispatched"; we also avoid a
		// generic "wipe failed" overlay, since the Abandoned
		// status is already the correct user-facing message.
		if errors.Is(err, service.ErrConversationDeleteReservationLost) {
			return
		}
		w.router.SetSendStatus(w.t("status.clear_chat_failed", err.Error()))
	}(peer, requestID, dispatching)
}

// handleConversationDeleteOutcome is the ebus subscriber invoked when
// an in-flight conversation_delete reaches a terminal state. Counterpart
// of handleMessageDeleteOutcome for the bulk wipe-the-thread variant.
//
// Pessimistic ordering: local rows survive abandonment, so the
// "abandoned" status string explicitly tells the user the local chat
// is unchanged and they can re-issue once the peer is reachable.
// LocalCleanupFailed flags a separate failure mode — the peer
// confirmed the wipe but the local sweep could not finish, so the UI
// must NOT promise "wiped on both sides" without qualification when
// that flag is set.
func (w *Window) handleConversationDeleteOutcome(outcome ebus.ConversationDeleteOutcome) {
	if w.router == nil {
		return
	}
	var msg string
	switch {
	case outcome.Abandoned:
		msg = w.t("status.clear_chat_abandoned")
	case outcome.Status == domain.ConversationDeleteStatusApplied && outcome.LocalCleanupFailed:
		// Peer is consistent (it wiped its side) but the local
		// chatlog still holds some rows — chatlog read failure or a
		// per-row DeleteByID failure during the post-ack sweep.
		// Surface the asymmetry instead of claiming "wiped on
		// both sides".
		msg = w.t("status.clear_chat_local_cleanup_failed", outcome.DeletedRemote)
	case outcome.Status == domain.ConversationDeleteStatusApplied:
		msg = w.t("status.clear_chat_applied", outcome.DeletedRemote)
	default:
		// Unknown wire-level status — surface as abandoned rather
		// than silently lying about peer-side convergence.
		msg = w.t("status.clear_chat_abandoned")
	}
	w.router.SetSendStatus(msg)
	if w.window != nil {
		w.window.Invalidate()
	}
}

// handleReplyCancel clears the reply state when the cancel button is clicked.
func (w *Window) handleReplyCancel(gtx layout.Context) {
	if w.replyToMsg == nil {
		return
	}
	for w.replyCancelButton.Clicked(gtx) {
		w.replyToMsg = nil
		if w.window != nil {
			w.window.Invalidate()
		}
	}
}

// layoutMsgContextMenuOverlay renders the right-click context menu for a chat message.
func (w *Window) layoutMsgContextMenuOverlay(gtx layout.Context) layout.Dimensions {
	// Dismiss on click outside.
	dismissArea := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, w.msgContextMsg)
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: w.msgContextMsg, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			w.msgContextMsg = nil
			if w.window != nil {
				w.window.Invalidate()
			}
		}
	}
	dismissArea.Pop()

	menuWidth := gtx.Dp(unit.Dp(180))
	windowW := gtx.Constraints.Max.X
	windowH := gtx.Constraints.Max.Y

	// Measure the menu.
	measureGTX := gtx
	measureGTX.Constraints.Min.X = menuWidth
	measureGTX.Constraints.Max.X = menuWidth
	measureGTX.Constraints.Min.Y = 0
	measureGTX.Constraints.Max.Y = windowH
	macro := op.Record(measureGTX.Ops)
	dims := w.msgContextMenuCard(measureGTX)
	menuCall := macro.Stop()

	x := w.msgContextPos.X
	y := w.msgContextPos.Y
	if x+menuWidth > windowW {
		x = windowW - menuWidth
	}
	if x < 0 {
		x = 0
	}
	if y+dims.Size.Y > windowH {
		y = y - dims.Size.Y
		if y < 0 {
			y = 0
		}
	}

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	menuCall.Add(gtx.Ops)
	stack.Pop()

	return layout.Dimensions{}
}

func (w *Window) msgContextMenuCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 72, G: 85, B: 106, A: 255}
	bgColor := color.NRGBA{R: 28, G: 34, B: 44, A: 255}
	rr := gtx.Dp(unit.Dp(8))
	borderWidth := gtx.Dp(unit.Dp(1))

	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(6)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return w.layoutMsgContextMenuItems(gtx)
		})
	})
	contentCall := macro.Stop()
	bounds := image.Rectangle{Max: dims.Size}

	defer clip.UniformRRect(bounds, rr).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: borderColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	innerBounds := image.Rectangle{
		Min: image.Pt(borderWidth, borderWidth),
		Max: image.Pt(dims.Size.X-borderWidth, dims.Size.Y-borderWidth),
	}
	innerRR := rr - borderWidth
	if innerRR < 0 {
		innerRR = 0
	}
	defer clip.UniformRRect(innerBounds, innerRR).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: bgColor}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)

	contentCall.Add(gtx.Ops)
	return dims
}

func (w *Window) layoutMsgContextMenuItems(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.msgCtxReply, w.t("context.reply"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.msgCtxCopy, w.t("context.copy_message"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			// Delete uses a warning-tinted label so the destructive
			// nature is visible. The handler in
			// handleMsgContextMenuActions invokes router.SendMessageDelete
			// asynchronously, and the path differs by message
			// direction:
			//
			//   - Outgoing DM (Sender == self): a message_delete
			//     control DM is dispatched to the recipient with
			//     retry budget; the local chatlog row is kept
			//     until the peer's message_delete_ack confirms
			//     deletion (pessimistic ordering). The terminal
			//     UI status (deleted / not_found / denied /
			//     immutable / abandoned) arrives via
			//     TopicMessageDeleteCompleted.
			//
			//   - Incoming DM (Sender != self): the local row +
			//     any backing file are removed synchronously
			//     inside SendMessageDelete. No control DM is
			//     dispatched and no peer ack is required —
			//     incoming deletes are local-only.
			//
			// Offline guard: only outgoing rows need the peer
			// online (the incoming local-only path doesn't talk
			// to the peer). When the recipient is offline AND the
			// message is outgoing, the wire-level delete cannot
			// reach the recipient and would burn the entire retry
			// budget waiting for an ack that will never come;
			// contextMenuDeleteEnabled returns false and we render
			// the item dimmed. Incoming rows always render fully
			// enabled regardless of peer reachability. The click
			// handler in handleMsgContextMenuActions enforces the
			// same direction-aware rule as a defence-in-depth
			// check against a click racing a just-now-disconnected
			// peer.
			if !w.contextMenuDeleteEnabled() {
				// Inert disabled item: no Clickable, no hover
				// ripple, neutral gray label. The user
				// indicated faint red still reads as active
				// against the dark menu background; gray
				// reads more decisively as "not available
				// right now".
				return w.contextMenuItemDisabled(gtx, w.t("context.delete_message"))
			}
			fg := color.NRGBA{R: 240, G: 158, B: 158, A: 255}
			return w.contextMenuItem(gtx, &w.msgCtxDelete, w.t("context.delete_message"), fg)
		}),
	)
}

// layoutReplyQuote renders a compact quote block for the referenced message.
// Shows sender name and date above the quoted text. Clicking scrolls to the
// original message in the conversation.
func (w *Window) layoutReplyQuote(gtx layout.Context, replyTo domain.MessageID, isMine bool) layout.Dimensions {
	replyToStr := string(replyTo)
	quotedBody := w.findMessageBody(replyToStr)
	if quotedBody == "" {
		quotedBody = w.t("chat.reply_unknown")
	}
	quotedBody = ellipsize(quotedBody, 80)

	// Resolve sender display name and timestamp from cache.
	var quotedAuthor string
	var quotedTime string
	if cm, ok := w.findCachedMsg(replyToStr); ok {
		if cm.Sender == w.snap.MyAddress {
			quotedAuthor = w.t("chat.you_label")
		} else {
			quotedAuthor = w.peerDisplayName(cm.Sender)
		}
		quotedTime = cm.Timestamp.Local().Format("02.01.2006 15:04")
	}

	barColor := color.NRGBA{R: 100, G: 140, B: 200, A: 255}
	if !isMine {
		barColor = color.NRGBA{R: 120, G: 150, B: 180, A: 255}
	}
	bgColor := color.NRGBA{R: 30, G: 38, B: 50, A: 180}

	// Use raw pointer events for click-to-scroll. widget.Clickable inside
	// op.Record is unreliable — its pointer filters may not replay correctly
	// through the macro. We use the same op.Record/clip.Rect/event.Op pattern
	// as the right-click handler on the bubble.
	tag := w.replyQuoteTag(replyToStr) // reuse as stable tag identity
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: tag,
			Kinds:  pointer.Press,
		})
		if !ok {
			break
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonPrimary) {
			w.scrollToMsgID = replyToStr
			w.scrollClickY = w.chatCursorY
		}
	}

	// Record content to measure, then create a clip area for pointer events.
	quoteMacro := op.Record(gtx.Ops)
	dims := layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			defer clip.UniformRRect(image.Rectangle{Max: gtx.Constraints.Min}, gtx.Dp(unit.Dp(4))).Push(gtx.Ops).Pop()
			paint.ColorOp{Color: bgColor}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			return layout.Dimensions{Size: gtx.Constraints.Min}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					barW := gtx.Dp(unit.Dp(3))
					barH := gtx.Dp(unit.Dp(40))
					sz := image.Pt(barW, barH)
					defer clip.UniformRRect(image.Rectangle{Max: sz}, gtx.Dp(unit.Dp(1))).Push(gtx.Ops).Pop()
					paint.ColorOp{Color: barColor}.Add(gtx.Ops)
					paint.PaintOp{}.Add(gtx.Ops)
					return layout.Dimensions{Size: sz}
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Inset{Left: unit.Dp(6), Top: unit.Dp(2), Bottom: unit.Dp(2), Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								if quotedAuthor == "" && quotedTime == "" {
									return layout.Dimensions{}
								}
								header := quotedAuthor
								if quotedTime != "" {
									if header != "" {
										header += " · "
									}
									header += quotedTime
								}
								lbl := material.Caption(w.theme, header)
								lbl.Color = barColor
								lbl.Font.Weight = font.Bold
								return lbl.Layout(gtx)
							}),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Caption(w.theme, quotedBody)
								label.Color = color.NRGBA{R: 170, G: 185, B: 210, A: 255}
								label.MaxLines = 2
								return label.Layout(gtx)
							}),
						)
					})
				}),
			)
		}),
	)
	quoteCall := quoteMacro.Stop()

	// Create a dedicated clip area for the quote so pointer events are scoped
	// to this element, not the parent bubble.
	defer clip.Rect(image.Rectangle{Max: dims.Size}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, tag)
	quoteCall.Add(gtx.Ops)
	return dims
}

// applyDeferredScroll scrolls the chat list so that the target message
// appears at the same vertical level where the user clicked the quote.
//
// Uses scrollClickY (cursor Y at click time) and chatViewportH (chat
// area height from the previous layout pass) to compute the fraction
// of the viewport, then offsets Position.First so the target message
// lands at that fraction of the visible item range.
func (w *Window) applyDeferredScroll() {
	if w.scrollToMsgID == "" {
		return
	}
	target := w.scrollToMsgID
	w.scrollToMsgID = ""
	cm, ok := w.findCachedMsg(target)
	if !ok {
		return
	}

	visibleCount := w.chatList.Position.Count
	if visibleCount <= 0 {
		visibleCount = 1
	}

	// Estimate how many items above the target we need to show so
	// that the target ends up at the cursor's vertical position.
	// fraction=0 → top of viewport, fraction=1 → bottom.
	// Subtract 1 to compensate for item spacing and partial-item
	// rendering that shifts the target below the cursor.
	itemsAbove := visibleCount / 2 // default: center
	if w.chatViewportH > 0 {
		fraction := float64(w.scrollClickY) / float64(w.chatViewportH)
		if fraction < 0 {
			fraction = 0
		}
		if fraction > 1 {
			fraction = 1
		}
		itemsAbove = int(fraction*float64(visibleCount) + 0.5)
	}

	first := cm.Index - itemsAbove
	if first < 0 {
		first = 0
	}
	w.chatList.Position.First = first
	w.chatList.Position.Offset = 0
	w.chatList.Position.BeforeEnd = true
}

// rebuildMsgCache populates msgCacheByID from the current snapshot.
// Called once per frame from layout(), before any rendering that needs
// reply quote lookups. Stores body, sender, timestamp and index for
// scroll-to-original support.
//
// The rebuild is skipped when the snapshot generation has not changed.
// Generation is bumped on every DMRouter state mutation, so any change
// to message bodies, ReplyTo fields, receipt statuses, or conversation
// switches is detected — including same-shape reloads that the old
// count+first/last heuristic missed. O(1) per no-change frame.
func (w *Window) rebuildMsgCache() {
	gen := w.snap.Generation
	msgs := w.snap.ActiveMessages

	if len(msgs) == 0 {
		if w.msgCacheByID != nil {
			w.msgCacheByID = nil
			w.msgCacheGen = 0
		}
		return
	}

	if w.msgCacheByID != nil && w.msgCacheGen == gen {
		return
	}

	m := make(map[string]cachedMsg, len(msgs))
	for i := range msgs {
		m[msgs[i].ID] = cachedMsg{
			Body:      msgs[i].Body,
			Sender:    msgs[i].Sender,
			Timestamp: msgs[i].Timestamp,
			Index:     i,
		}
	}
	w.msgCacheByID = m
	w.msgCacheGen = gen
}

// findMessageBody looks up a message body by ID using the per-frame cache.
func (w *Window) findMessageBody(id string) string {
	if w.msgCacheByID == nil {
		return ""
	}
	if cm, ok := w.msgCacheByID[id]; ok {
		return cm.Body
	}
	return ""
}

// findCachedMsg returns full cached metadata for a message ID.
func (w *Window) findCachedMsg(id string) (cachedMsg, bool) {
	if w.msgCacheByID == nil {
		return cachedMsg{}, false
	}
	cm, ok := w.msgCacheByID[id]
	return cm, ok
}

// replyQuoteTag returns a stable pointer event tag for a reply quote,
// keyed by the referenced message ID. Uses *widget.Clickable as a
// convenient heap-allocated identity — only its address matters.
func (w *Window) replyQuoteTag(replyToID string) *widget.Clickable {
	if w.replyQuoteTags == nil {
		w.replyQuoteTags = make(map[string]*widget.Clickable)
	}
	if c, ok := w.replyQuoteTags[replyToID]; ok {
		return c
	}
	c := &widget.Clickable{}
	w.replyQuoteTags[replyToID] = c
	return c
}

// layoutReplyPreview renders the reply quote banner above the composer input.
func (w *Window) layoutReplyPreview(gtx layout.Context) layout.Dimensions {
	if w.replyToMsg == nil {
		return layout.Dimensions{}
	}

	quotedBody := ellipsize(w.replyToMsg.Body, 80)
	bgColor := color.NRGBA{R: 30, G: 40, B: 55, A: 255}
	barColor := color.NRGBA{R: 100, G: 140, B: 200, A: 255}

	return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Stack{}.Layout(gtx,
			layout.Expanded(func(gtx layout.Context) layout.Dimensions {
				defer clip.UniformRRect(image.Rectangle{Max: gtx.Constraints.Min}, gtx.Dp(unit.Dp(6))).Push(gtx.Ops).Pop()
				paint.ColorOp{Color: bgColor}.Add(gtx.Ops)
				paint.PaintOp{}.Add(gtx.Ops)
				return layout.Dimensions{Size: gtx.Constraints.Min}
			}),
			layout.Stacked(func(gtx layout.Context) layout.Dimensions {
				return layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(4), Right: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							barW := gtx.Dp(unit.Dp(3))
							barH := gtx.Dp(unit.Dp(24))
							sz := image.Pt(barW, barH)
							defer clip.UniformRRect(image.Rectangle{Max: sz}, gtx.Dp(unit.Dp(1))).Push(gtx.Ops).Pop()
							paint.ColorOp{Color: barColor}.Add(gtx.Ops)
							paint.PaintOp{}.Add(gtx.Ops)
							return layout.Dimensions{Size: sz}
						}),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return layout.Inset{Left: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										label := material.Caption(w.theme, w.t("compose.replying_to"))
										label.Color = color.NRGBA{R: 130, G: 155, B: 195, A: 255}
										return label.Layout(gtx)
									}),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										label := material.Caption(w.theme, quotedBody)
										label.Color = color.NRGBA{R: 180, G: 195, B: 218, A: 255}
										label.MaxLines = 1
										return label.Layout(gtx)
									}),
								)
							})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return layout.Inset{Left: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								btn := material.Button(w.theme, &w.replyCancelButton, "✕")
								btn.Background = color.NRGBA{R: 50, G: 60, B: 78, A: 255}
								btn.Color = color.NRGBA{R: 200, G: 210, B: 225, A: 255}
								btn.Inset = layout.Inset{Top: unit.Dp(2), Bottom: unit.Dp(2), Left: unit.Dp(6), Right: unit.Dp(6)}
								return btn.Layout(gtx)
							})
						}),
					)
				})
			}),
		)
	})
}

func fill(gtx layout.Context, c color.NRGBA) {
	stack := clip.Rect{Max: image.Pt(gtx.Constraints.Max.X, gtx.Constraints.Max.Y)}.Push(gtx.Ops)
	defer stack.Pop()
	paint.ColorOp{Color: c}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}
