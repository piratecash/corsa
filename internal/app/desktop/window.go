package desktop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/app"
	"gioui.org/f32"
	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
	"gioui.org/x/explorer"
	"golang.org/x/exp/shiny/materialdesign/icons"
)

type Window struct {
	router *service.DMRouter
	// reactionRouter narrows what a reaction decision may reach, and is nil in
	// production — reactions() falls back to router. A test sets it to observe
	// the one call that distinguishes a wired chip row from an unwired one.
	reactionRouter reactionRouter
	client         *service.DesktopClient
	eventBus       *ebus.Bus
	cmdTable       *rpc.CommandTable
	runtime        *NodeRuntime
	prefs          *Preferences
	theme          *material.Theme
	ops            op.Ops

	recipientEditor      widget.Editor
	identitySearchEditor widget.Editor
	messageEditor        widget.Editor
	focusComposerPending bool
	// composerKeyboardPending rides alongside focusComposerPending but is
	// set ONLY when a touch-driven action wants the on-screen keyboard up
	// (selecting a contact to type to). On Windows a programmatic FocusCmd
	// does not raise the keyboard, so the composer would gain focus with no
	// keyboard. It is deliberately NOT set by the long-press context-menu
	// path, which focuses the composer but should leave the keyboard down.
	composerKeyboardPending  bool
	contactsList             widget.List
	chatList                 widget.List
	consoleButton            widget.Clickable
	updateButton             widget.Clickable
	compactBackBtn           widget.Clickable
	sendButton               widget.Clickable
	myIdentityButton         widget.Clickable
	copyIdentityButton       widget.Clickable
	shareContactButton       widget.Clickable
	identityPanelClose       widget.Clickable
	identityPanelDismissTag  struct{}
	identityPanelVisible     bool
	identityPanelQRImage     widget.Image
	identityPanelContactLink string
	identityPanelList        layout.List
	searchIcon               *widget.Icon
	fingerprintIcon          *widget.Icon
	personIcon               *widget.Icon
	personOutlineIcon        *widget.Icon
	chevronIcon              *widget.Icon
	chevronDownIcon          *widget.Icon
	copyIcon                 *widget.Icon
	shareIcon                *widget.Icon
	closeIcon                *widget.Icon
	attachIcon               *widget.Icon
	emojiIcon                *widget.Icon
	sendIcon                 *widget.Icon
	shieldIcon               *widget.Icon
	consoleIcon              *widget.Icon
	chevronLeftIcon          *widget.Icon
	zoomInIcon               *widget.Icon
	zoomOutIcon              *widget.Icon
	downloadIcon             *widget.Icon
	deleteIcon               *widget.Icon
	brokenImageIcon          *widget.Icon
	hourglassIcon            *widget.Icon
	emojiCategoryIcons       map[emojiCategoryID]*widget.Icon
	emojiPicker              emojiPickerState
	// lastContactLinkTried edge-triggers the search-paste import
	// (contact_share.go).
	lastContactLinkTried string
	languageToggle       widget.Clickable
	languageOptions      map[string]*widget.Clickable
	languageMenuList     widget.List
	// languageMenuDismissTag is the popup backdrop's pointer target.
	languageMenuDismissTag struct{}
	// headerHeight and languageButtonSize are what the last drawn frame
	// measured for the header row and for the language button inside it. They
	// are the only way to know where that button IS: Gio exposes no absolute
	// position, so the popup reconstructs the anchor from the window padding
	// plus these two. Both keep their last value on a frame that does not draw
	// the header — see languageMenuAnchor.
	headerHeight       int
	languageButtonSize image.Point
	shutdown           func()
	shutdownOnce       sync.Once
	uiOpMu             sync.RWMutex
	uiOpClosed         bool
	uiStopOnce         sync.Once
	uiStopCh           chan struct{}
	// sendWG tracks UI-side goroutines that write through the router /
	// chatlog: sendFileCore (transmit import + handoff), async message
	// deletes and conversation-delete completion. The shutdown path
	// waits for it before the router's own drain — these goroutines
	// spawn router work and write transmit files, so cutting them off
	// would leave partial state or lose the operation entirely.
	sendWG               sync.WaitGroup
	recipientButtons     map[domain.PeerIdentity]*widget.Clickable
	recipientRightClick  map[domain.PeerIdentity]*rightClickState
	recipientMenuBtns    map[domain.PeerIdentity]*widget.Clickable // per-card "⋯" menu buttons
	messageSelectables   map[string]*widget.Selectable
	sendStatusSelectable widget.Selectable
	lastChatPeer         domain.PeerIdentity
	language             string
	showLanguageMenu     bool
	// consoleFocusReturn asks the next frame to hand the keyboard back to the
	// Console button, once the modal that took it has closed. See
	// restoreConsoleFocus.
	// consoleModal is the console. Created on first open and kept for the rest
	// of the process so command history and the selected tab survive a close;
	// nil until then, which is the whole session for most users. Whether it is
	// showing lives on it, not here: the traffic ticker reads that off the UI
	// goroutine (see consoleModal.visible).
	consoleModal *consoleModal

	// imageViewer is the full-window image viewer (image_viewer.go). Created
	// on first use and kept afterwards, like the console: what it holds while
	// it is closed is one empty struct, and what it holds while it is open —
	// decoded bitmaps measured in tens of megabytes — is released by the
	// close itself.
	imageViewer *imageViewer

	// Global cursor tracking for context menu positioning.
	cursorTracker   int // tag for window-level pointer events
	lastCursorPos   image.Point
	rootSize        image.Point                // window size captured at layout() root (for menu anchors computed inside lists)
	touchPressPos   map[pointer.ID]image.Point // active TOUCH presses (multi-touch guard)
	pointerPressPos map[pointer.ID]pressPoint  // press position + press FRAME of ALL pointers (menu anchors)
	pressCleanup    []pointer.ID               // IDs released this frame; maps cleaned NEXT frame
	pressCancelAll  bool                       // Cancel seen this frame; maps cleared NEXT frame
	multiTouchAt    time.Time                  // frame time when a touch press overlapped an already-active touch
	lastInputTouch  bool                       // the most recent pointer press came from a touch screen
	lastPressAt     time.Time                  // frame time of that press (recency gate for touchDrivenInput)

	// Touch input: pointer tags for the on-screen keyboard invocation
	// areas — composer, contact search, alias editor, emoji search — plus this window's
	// keyboard occlusion state (see touch_input.go).
	touchKbdTags [4]int8
	touchKbd     touchKeyboardState

	// Context menu state for right-click on recipient buttons.
	contextMenuPeer         domain.PeerIdentity // fingerprint of the peer whose context menu is open
	contextMenuPos          image.Point
	ctxMenuCopy             widget.Clickable
	ctxMenuDelete           widget.Clickable
	ctxMenuDeleteConfirm    widget.Clickable
	ctxMenuDeleteCancel     widget.Clickable
	ctxMenuClearChat        widget.Clickable // "Delete chat for both sides" — wipe here, request theirs
	ctxMenuClearChatConfirm widget.Clickable
	ctxMenuClearChatCancel  widget.Clickable
	ctxMenuAlias            widget.Clickable
	ctxMenuAliasSave        widget.Clickable
	ctxMenuAliasCancel      widget.Clickable
	showDeleteConfirm       bool // true when "Delete identity" confirmation step is shown
	showClearChatConfirm    bool // true when "Delete chat for both sides" confirmation step is shown
	showAliasEditor         bool // true when alias input is shown
	aliasEditor             widget.Editor

	// Context menu state for right-click on chat messages.
	msgContextMsg *service.DirectMessage // message whose context menu is open (nil = closed)
	msgContextPos image.Point
	msgCtxReply   widget.Clickable
	msgCtxCopy    widget.Clickable
	msgCtxDelete  widget.Clickable
	msgRightClick map[string]*rightClickState  // keyed by message ID
	msgMenuBtns   map[string]*widget.Clickable // per-message "⋯" menu buttons

	// Reactions: the chip row's widgets per message, the state those chips are
	// drawn from, and the quick-choice pill that opens with the message menu.
	// See reactions.go.
	msgReactionChips map[domain.MessageID]*ui.ReactionChipsState
	msgReactionState map[domain.MessageID][]domain.Reaction
	// reactionsStale is raised by the event-bus goroutine and lowered by the
	// layout goroutine. It is the ONLY thing that crosses between them for this
	// feature: msgReactionState above is a map every bubble reads each frame,
	// so a write from a subscriber is a concurrent map access rather than a
	// stale read. See noteReactionsChanged.
	reactionsStale atomic.Bool
	reactionRow    reactionRowState
	// reactionsLocalOnlyFor names the conversations the "your reaction stayed
	// here" notice has already been shown for. Layout goroutine only.
	//
	// Once per conversation, not once per reload: the news arrives
	// asynchronously (the node learns the refusal a second after the tap), and
	// every later reload of that conversation would repeat the notice over
	// whatever else the status line was saying.
	//
	// A SET rather than "the last conversation announced", because the check
	// also runs on every chat switch: with one slot, walking A → B → A would
	// announce A twice, and A → B with both refusing would lose B's notice the
	// moment the user came back to A.
	reactionsLocalOnlyFor map[domain.PeerIdentity]bool
	// reactionsLocalOnlyText is the notice AS IT WAS WRITTEN. Taking it back
	// compares against the line's current contents, and re-translating the
	// notice to do that compares against a string that was never on the line if
	// the user has changed language since — the notice then stays up for good,
	// because the flag above is cleared either way.
	reactionsLocalOnlyText string

	// Keyboard/Narrator focus contract of the two context menus above: which
	// "⋯" button opened one, whether it holds focus, and where focus goes back
	// to when it closes. See context_menu_focus.go.
	peerMenuFocus      menuFocusState
	msgMenuFocus       menuFocusState
	identityPanelFocus menuFocusState

	// menuBtnRects holds the last known WINDOW-space rectangle of each "⋯"
	// button, captured during layout on frames where a pointer press lets us
	// correlate the button's local coordinates with the window cursor. It is
	// the anchor for a menu opened by a NON-pointer (keyboard/Narrator)
	// activation, which carries no fresh cursor — far better than a fixed
	// fraction of the window. Keyed by the button pointer; buttons are few.
	//
	// A captured rectangle is only valid while the buttons have not moved, so
	// menuLayoutSig snapshots the layout state under which the rectangles were
	// taken — both lists' full scroll Position, the row count and row ORDER each
	// was laid out with, the identity-search hits (rows belonging to no list at
	// all) with the position they were laid out at, and the window size (see
	// menuRectSig, which explains why none of those is optional). When it
	// changes the whole cache is dropped
	// (see layout()), and a non-pointer menu falls back to the always-visible
	// anchor rather than opening at a position the content has invalidated.
	menuBtnRects   map[*widget.Clickable]image.Rectangle
	menuLayoutSig  menuRectSig
	menuRectSigSet bool
	// Item counts the two scrollable lists are being laid out with this frame.
	// They live here because the slices are derived deep inside the layout tree
	// and are not reachable from currentMenuRectSig, and they are folded into
	// menuLayoutSig so an added or deleted row invalidates the rectangles it
	// moved even though the scroll offset never changed. Recorded through
	// setMenuListItems at the top of the owning CARD — above every early return
	// that lays out no list, and before any row lays out — not at the List call
	// site itself, which those returns never reach.
	chatItems     int
	contactsItems int
	// Order digests over the row IDENTITIES of the same two lists, in the order
	// they are laid out. Counts and geometry cannot see a PERMUTATION: rows of
	// equal height re-ranked by sortSidebarPeers leave layout.Position, the
	// count and rootSize byte-identical while every "⋯" moves, and menuBtnRects
	// is keyed by button identity, so its rectangle would then name a different
	// row. contactsOrder is hashed per frame from the slice handed to the card;
	// chatOrder rides rebuildMsgCache's existing once-per-generation pass,
	// because a conversation is unbounded and must not be hashed every frame.
	chatOrder     uint64
	contactsOrder uint64

	// Same pair for the identity-search hits, which are neither list. They are
	// Rigid rows above the contacts list, selected by the search query — so
	// nothing else recorded here moves when they do, and they carry the SAME
	// per-contact ⋯ buttons. Written by resolveIdentitySearchRows.
	searchItems int
	searchOrder uint64
	// And WHERE those rows are, which the pair above cannot see: everything
	// else in the signature is translation-blind, so the whole card sliding up
	// when the keyboard takes the header away changes none of it. This is the
	// space left BELOW the rows' top edge — what a vertical Flex hands a Rigid
	// child — so it moves whenever anything above them does. Written by
	// recordSearchRowAnchor.
	searchAvail int

	// Hide generation PLUS ONE that a surface's last "please go away" was
	// dispatched with, 0 for "never asked" — see requestTouchKeyboardRoom.
	// One marker per surface: a context menu and the emoji picker defer
	// independently, and a shared marker would let whichever asked first
	// throttle the other's first ask.
	menuKbdHideAskedGen  int64
	emojiKbdHideAskedGen int64

	// Context menus scroll when taller than the space above the keyboard (in
	// landscape the keyboard can leave too little height for every row — Delete
	// / Clear chat, or an alias editor's Save / Cancel — which a fixed layout
	// would squeeze to nothing). The menu content is one oversized list item; a
	// List sizes to content when it fits and clamps-with-scroll (drag to scroll)
	// when it does not. Plain layout.List, not material's — its scrollbar's
	// default Occupy strategy would reserve a gutter and narrow the menu even
	// when it fits.
	ctxMenuList    layout.List
	msgCtxMenuList layout.List
	// The scroll position is reset to the top whenever a menu is CLOSED (see
	// layout()), so every fresh open — including reopening the same peer — starts
	// at the top instead of mid-scroll. lastCtxMenuMode additionally resets it
	// when an OPEN recipient menu switches to/from a confirm/alias sub-view, so
	// the confirmation header can't already be scrolled out of sight.
	lastCtxMenuMode uint8

	// Dragging is not the only thing that has to move those lists. Keyboard
	// focus steps from row to row INSIDE the single list item, which by itself
	// scrolls nothing and walks focus off the bottom edge of the card. These
	// measure the rows so the overlays can scroll the focused one into view.
	ctxMenuScroll    menuScroll
	msgCtxMenuScroll menuScroll

	// Reply state: when the user replies to a message, we remember the
	// target UUID and show a quote preview above the composer.
	replyToMsg        *service.DirectMessage // message being replied to (nil = no active reply)
	replyCancelButton widget.Clickable

	// msgCacheByID stores message metadata for O(1) lookup when rendering
	// reply quotes (body, sender, timestamp). Rebuilt when the snapshot's
	// DM generation changes — this catches every mutation including body
	// edits, ReplyTo updates, and same-shape conversation reloads that
	// the old count+first/last heuristic missed.
	msgCacheByID map[string]cachedMsg
	msgCacheGen  uint64 // snapshot DMGeneration when cache was built

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
	// these fields hold the selected file path until Send is pressed.
	// attachedFile and attachGen are only mutated on the UI goroutine —
	// background goroutines deliver the selected path via pendingAttach
	// (buffered channel, fully drained in handlePendingActions) and call
	// window.Invalidate() to trigger a frame.
	//
	// attachGen is a PER-PEER monotonic counter bumped every time a
	// conversation's attachment slot transitions (new user pick, explicit
	// cancel). It guards the file-picker delivery path in applyPendingAttach,
	// which honors a delivery only when the generation still matches that
	// peer's counter. Per-peer (not global) so an attachment action in one
	// chat can never reject a valid delivery for a different chat.
	attachButton    widget.Clickable
	attachedFile    string // absolute path to the selected file for the OPEN chat (empty = none)
	attachGen       map[domain.PeerIdentity]uint64
	attachCancelBtn widget.Clickable
	pendingAttach   chan pendingAttachMsg // delivers attach updates from background goroutines → UI goroutine

	// Failed sends. When a send fails, the composer is NOT touched; the unsent
	// message becomes a retriable entry here, keyed by recipient, and is shown
	// as a banner above the composer with retry/dismiss. This keeps every
	// failed send a distinct entity (no merging into the live composer, no
	// silent loss). pendingFailed marshals file-send failures from their
	// background goroutine onto the UI goroutine.
	failedSends         map[domain.PeerIdentity][]failedSend
	failedRetryButton   widget.Clickable
	failedDismissButton widget.Clickable
	pendingFailed       chan pendingFailedMsg
	// failedShown is the number of entries the banner actually RENDERED for a
	// peer on the last frame. New failures are only ever appended, so the shown
	// set is always the prefix failedSends[peer][:failedShown[peer]]. Retry and
	// Dismiss act on exactly that prefix, so a failure that arrives between the
	// rendered frame and the user's click (which the user could not have seen)
	// is neither silently re-sent nor discarded — it stays in the banner.
	failedShown map[domain.PeerIdentity]int

	// Per-contact composer drafts. When the user switches conversation, the
	// unsent composer text and the currently selected attachment are stashed
	// under the peer being left and restored when that peer is reopened. Held
	// in memory only (lost on app exit). draftPeer is kept separate from
	// lastChatPeer: they change in different places (draft swap vs the
	// per-message cache reset in resetReplyOnPeerChange) and coupling them
	// historically let the swap clobber live input.
	drafts    map[domain.PeerIdentity]composerDraft
	draftPeer domain.PeerIdentity

	// peerForgetEpoch bumps whenever a conversation is removed
	// (forgetPeerComposerState). A file pick / failed-send restore captures
	// the epoch when its background work starts; applyPendingAttach drops the
	// delivery if the epoch has since advanced — i.e. the target conversation
	// was deleted while a native file dialog or a send was still in flight, so
	// its result must not resurrect a draft for a peer that no longer exists.
	peerForgetEpoch map[domain.PeerIdentity]uint64

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
	// peerLastOnlineByIdentity is rebuilt once per frame from PeerHealth and
	// reused by every visible contact row. This keeps row layout O(1) instead of
	// decoding the full peer-health slice for every contact.
	peerLastOnlineByIdentity map[domain.PeerIdentity]time.Time

	window *app.Window

	transferInvalidateMu      sync.Mutex
	transferInvalidatePending bool
}

// pendingAttachMsg is the payload delivered over the pendingAttach channel
// from background goroutines to the UI goroutine drain. Two producers write
// to this channel:
//
//   - triggerFileAttach (user picked a new file through the native dialog):
//     restore=false. Applied to msg.peer's slot and bumps that peer's
//     generation.
//
//   - restore=true (per-peer generation captured at send start; applied only
//     if that peer's generation still matches and the target slot is empty).
//     This branch has no live producer: async file-send failures now surface
//     as a retriable "not sent" banner (failedSends) instead of re-populating
//     the composer. The branch and its generation guard are retained (and
//     still unit-tested in attach_generation_test.go) as the safe delivery
//     path; they can be dropped in a later cleanup pass alongside those tests.
//
// The channel is generously buffered and fully drained each frame, and every
// message carries peer, so producers block-send without any cross-peer
// arbitration — nothing is dropped.
type pendingAttachMsg struct {
	path       string
	restore    bool
	generation uint64
	// peer is the conversation the attachment belongs to, captured when the
	// file dialog was opened (user pick) or when the send was dispatched
	// (restore). applyPendingAttach routes the delivery to that conversation:
	// the live composer if it is still open, otherwise that peer's draft — so
	// a file picked or restored for one contact never lands on another.
	peer domain.PeerIdentity
	// epoch is peerForgetEpoch[peer] captured when the background work started.
	// If it no longer matches at apply time the conversation was removed
	// meanwhile, and the delivery is dropped rather than resurrecting a draft.
	epoch uint64
	// caption carries the file's caption text on a failed-send restore, so the
	// text cleared synchronously at send is put back alongside the file. Empty
	// for user picks.
	caption string
}

// failedSend is one unsent message retained after a send failed, so the user
// can retry or dismiss it without the composer ever being touched.
type failedSend struct {
	body    string           // message text / file caption
	replyTo domain.MessageID // reply target, empty if none
	file    string           // attached file path; empty for a plain text send
}

// pendingFailedMsg marshals a file-send failure from its background goroutine
// onto the UI goroutine (via the pendingFailed channel), where it is turned
// into a failedSend entry. epoch is the recipient's forget-epoch at send time;
// a mismatch on apply means the contact was removed meanwhile and the entry is
// dropped instead of resurrecting it.
type pendingFailedMsg struct {
	peer    domain.PeerIdentity
	body    string
	replyTo domain.MessageID
	file    string
	epoch   uint64
}

const (
	// windowPadXDp and windowPadYDp are the window's own margin, applied once
	// in Window.layout. The language popup measures its anchor against them,
	// so they are constants rather than literals in one place.
	windowPadXDp       = 6
	windowPadYDp       = 4
	languageMenuHeight = 316
)

// editorTags lists every widget in this window that a caret can sit in.
//
// It is one list rather than a condition written out at its single call site
// because leaving a field out of it is silent and expensive: the touch-keyboard
// tracker reads "no editor focused" as "the user is done typing" and asks the
// keyboard down 400ms later. The reaction panel's search field was missed
// exactly that way, and on a Windows tablet the keyboard closed mid-word.
func (w *Window) editorTags() []event.Tag {
	return []event.Tag{
		&w.messageEditor,
		&w.identitySearchEditor,
		&w.aliasEditor,
		&w.emojiPicker.panel.Search,
		&w.reactionRow.panel.Search,
	}
}

// anyEditorFocused reports whether the caret is in any of them this frame.
func (w *Window) anyEditorFocused(gtx layout.Context) bool {
	for _, tag := range w.editorTags() {
		if gtx.Focused(tag) {
			return true
		}
	}
	return false
}

// kit is what the shared components (internal/app/desktop/ui) need from this
// window: its theme, its close icon and the family emoji are drawn in.
//
// Derived on each call rather than stored, because a stored copy is a field
// somebody has to remember to set — and the places that build a Window by
// literal, every test in this package among them, would each have to know to
// set it. Two pointer copies per call is not a cost worth a class of nil
// panics.
func (w *Window) kit() ui.Kit {
	return ui.Kit{Theme: w.theme, CloseIcon: w.closeIcon, EmojiFace: emojiTypeface}
}

// newAppTheme creates a fresh material.Theme with the application colour scheme.
// Each window must own its own Theme because the embedded text.Shaper uses an
// unsynchronised map cache and is therefore not safe for concurrent use.
func newAppTheme() *material.Theme {
	theme := material.NewTheme()
	theme.Shaper = text.NewShaper(text.WithCollection(appFontCollection()))
	// Keep the bundled Go face for normal text and fall back to the bundled
	// emoji family for picker choices and emoji embedded in messages. Both
	// families ship with the binary — see emoji_font.go for why the emoji one
	// cannot be left to the host.
	theme.Face = font.Typeface("Go, " + string(emojiTypeface))
	theme.Bg = color.NRGBA{R: 18, G: 21, B: 26, A: 255}
	theme.Fg = color.NRGBA{R: 235, G: 239, B: 244, A: 255}
	theme.ContrastBg = color.NRGBA{R: 36, G: 67, B: 126, A: 255}
	theme.ContrastFg = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	return theme
}

func loadUIIcon(name string, data []byte) (*widget.Icon, error) {
	icon, err := widget.NewIcon(data)
	if err != nil {
		return nil, fmt.Errorf("decode embedded UI icon %s: %w", name, err)
	}
	return icon, nil
}

type windowIcons struct {
	search          *widget.Icon
	fingerprint     *widget.Icon
	person          *widget.Icon
	personOutline   *widget.Icon
	chevron         *widget.Icon
	chevronDown     *widget.Icon
	copy            *widget.Icon
	share           *widget.Icon
	close           *widget.Icon
	attach          *widget.Icon
	emoji           *widget.Icon
	send            *widget.Icon
	shield          *widget.Icon
	console         *widget.Icon
	chevronLeft     *widget.Icon
	zoomIn          *widget.Icon
	zoomOut         *widget.Icon
	download        *widget.Icon
	remove          *widget.Icon
	brokenImage     *widget.Icon
	hourglass       *widget.Icon
	emojiCategories map[emojiCategoryID]*widget.Icon
}

func loadWindowIcons() (windowIcons, error) {
	var loaded windowIcons
	definitions := []struct {
		name string
		data []byte
		dst  **widget.Icon
	}{
		{name: "search", data: icons.ActionSearch, dst: &loaded.search},
		{name: "fingerprint", data: icons.ActionFingerprint, dst: &loaded.fingerprint},
		{name: "person", data: icons.SocialPerson, dst: &loaded.person},
		{name: "person-outline", data: icons.SocialPersonOutline, dst: &loaded.personOutline},
		{name: "chevron-right", data: icons.NavigationChevronRight, dst: &loaded.chevron},
		{name: "chevron-down", data: icons.NavigationExpandMore, dst: &loaded.chevronDown},
		{name: "copy", data: icons.ContentContentCopy, dst: &loaded.copy},
		{name: "share", data: icons.SocialShare, dst: &loaded.share},
		{name: "close", data: icons.NavigationClose, dst: &loaded.close},
		{name: "attachment", data: icons.EditorAttachFile, dst: &loaded.attach},
		{name: "emoji", data: icons.EditorInsertEmoticon, dst: &loaded.emoji},
		{name: "send", data: icons.ContentSend, dst: &loaded.send},
		{name: "shield", data: icons.ActionVerifiedUser, dst: &loaded.shield},
		{name: "console", data: icons.EditorShowChart, dst: &loaded.console},
		{name: "chevron-left", data: icons.NavigationChevronLeft, dst: &loaded.chevronLeft},
		{name: "zoom-in", data: icons.ActionZoomIn, dst: &loaded.zoomIn},
		{name: "zoom-out", data: icons.ActionZoomOut, dst: &loaded.zoomOut},
		{name: "download", data: icons.FileFileDownload, dst: &loaded.download},
		{name: "delete", data: icons.ActionDelete, dst: &loaded.remove},
		{name: "broken-image", data: icons.ImageBrokenImage, dst: &loaded.brokenImage},
		{name: "hourglass", data: icons.ActionHourglassEmpty, dst: &loaded.hourglass},
	}
	for _, definition := range definitions {
		icon, err := loadUIIcon(definition.name, definition.data)
		if err != nil {
			return windowIcons{}, err
		}
		*definition.dst = icon
	}
	loaded.emojiCategories = make(map[emojiCategoryID]*widget.Icon, len(emojiCategories)+1)
	categoryDefinitions := []struct {
		id   emojiCategoryID
		name string
		data []byte
	}{
		{id: emojiCategoryRecent, name: "emoji-recent", data: icons.ActionHistory},
		{id: emojiCategorySmileys, name: "emoji-smileys", data: icons.SocialMood},
		{id: emojiCategoryGestures, name: "emoji-gestures", data: icons.SocialPeople},
		{id: emojiCategoryAnimals, name: "emoji-animals", data: icons.ActionPets},
		{id: emojiCategoryFood, name: "emoji-food", data: icons.MapsRestaurant},
		{id: emojiCategoryTravel, name: "emoji-travel", data: icons.MapsDirectionsCar},
		{id: emojiCategoryActivities, name: "emoji-activities", data: icons.HardwareVideogameAsset},
		{id: emojiCategorySymbols, name: "emoji-symbols", data: icons.ActionFavorite},
		{id: emojiCategoryFlags, name: "emoji-flags", data: icons.ContentFlag},
	}
	for _, definition := range categoryDefinitions {
		icon, err := loadUIIcon(definition.name, definition.data)
		if err != nil {
			return windowIcons{}, err
		}
		loaded.emojiCategories[definition.id] = icon
	}
	return loaded, nil
}

func NewWindow(client *service.DesktopClient, router *service.DMRouter, eventBus *ebus.Bus, cmdTable *rpc.CommandTable, runtime *NodeRuntime, prefs *Preferences) (*Window, error) {
	loadedIcons, err := loadWindowIcons()
	if err != nil {
		return nil, err
	}
	theme := newAppTheme()

	language := normalizeLanguage(client.Language())
	if prefs != nil && prefs.Language != "" {
		language = normalizeLanguage(prefs.Language)
	}
	var recentEmojis []string
	if prefs != nil {
		recentEmojis = prefs.RecentEmojis
	}
	emojiPicker := newEmojiPickerStateWithRecents(recentEmojis)

	w := &Window{
		router:                   router,
		client:                   client,
		eventBus:                 eventBus,
		cmdTable:                 cmdTable,
		runtime:                  runtime,
		prefs:                    prefs,
		theme:                    theme,
		language:                 language,
		languageOptions:          make(map[string]*widget.Clickable),
		recipientButtons:         make(map[domain.PeerIdentity]*widget.Clickable),
		recipientRightClick:      make(map[domain.PeerIdentity]*rightClickState),
		messageSelectables:       make(map[string]*widget.Selectable),
		msgRightClick:            make(map[string]*rightClickState),
		msgMenuBtns:              make(map[string]*widget.Clickable),
		msgReactionChips:         make(map[domain.MessageID]*ui.ReactionChipsState),
		recipientMenuBtns:        make(map[domain.PeerIdentity]*widget.Clickable),
		menuBtnRects:             make(map[*widget.Clickable]image.Rectangle),
		touchPressPos:            make(map[pointer.ID]image.Point),
		pointerPressPos:          make(map[pointer.ID]pressPoint),
		drafts:                   make(map[domain.PeerIdentity]composerDraft),
		attachGen:                make(map[domain.PeerIdentity]uint64),
		peerForgetEpoch:          make(map[domain.PeerIdentity]uint64),
		peerLastOnlineByIdentity: make(map[domain.PeerIdentity]time.Time),
		failedSends:              make(map[domain.PeerIdentity][]failedSend),
		failedShown:              make(map[domain.PeerIdentity]int),
		pendingFailed:            make(chan pendingFailedMsg, 64),
		contactsList:             widget.List{List: layout.List{Axis: layout.Vertical}},
		ctxMenuList:              layout.List{Axis: layout.Vertical},
		msgCtxMenuList:           layout.List{Axis: layout.Vertical},
		identityPanelList:        layout.List{Axis: layout.Vertical},
		chatList:                 widget.List{List: layout.List{Axis: layout.Vertical, ScrollToEnd: true}},
		searchIcon:               loadedIcons.search,
		fingerprintIcon:          loadedIcons.fingerprint,
		personIcon:               loadedIcons.person,
		personOutlineIcon:        loadedIcons.personOutline,
		chevronIcon:              loadedIcons.chevron,
		chevronDownIcon:          loadedIcons.chevronDown,
		copyIcon:                 loadedIcons.copy,
		shareIcon:                loadedIcons.share,
		closeIcon:                loadedIcons.close,
		attachIcon:               loadedIcons.attach,
		emojiIcon:                loadedIcons.emoji,
		sendIcon:                 loadedIcons.send,
		shieldIcon:               loadedIcons.shield,
		consoleIcon:              loadedIcons.console,
		chevronLeftIcon:          loadedIcons.chevronLeft,
		zoomInIcon:               loadedIcons.zoomIn,
		zoomOutIcon:              loadedIcons.zoomOut,
		downloadIcon:             loadedIcons.download,
		deleteIcon:               loadedIcons.remove,
		brokenImageIcon:          loadedIcons.brokenImage,
		hourglassIcon:            loadedIcons.hourglass,
		emojiCategoryIcons:       loadedIcons.emojiCategories,
		emojiPicker:              emojiPicker,
		// Generously buffered and fully drained each frame so background
		// producers (file picks, failed-send restores) block-send without
		// dropping cross-conversation events.
		pendingAttach: make(chan pendingAttachMsg, 64),
	}
	w.aliasEditor.SingleLine = true
	w.aliasEditor.Submit = true
	return w, nil
}

// SetShutdown registers a teardown callback that runs before the process
// exits on the UI-driven paths (window closed, Android DestroyEvent).
//
// It is where the data-integrity part of the teardown (node stop, chatlog
// close) happens, because desktop.Run's own defers cannot do it: on desktop
// those paths exit the process straight from the event-loop goroutine and the
// defers never run, while on Android app.Main returns as soon as the Activity
// is up — there they WOULD run, with the UI still live, which is why Run hands
// ownership to this callback before starting the window. Call before Run.
func (w *Window) SetShutdown(fn func()) {
	w.shutdown = fn
}

// runShutdown invokes the SetShutdown callback exactly once.
func (w *Window) runShutdown() {
	w.shutdownOnce.Do(func() {
		w.flushRecentEmojiPreferences(time.Time{}, true)
		// The console's overflow files used to be removed when its window was
		// destroyed. It has no window any more and its entries live as long as
		// the process, so this is the one place left that can take them.
		if w.consoleModal != nil {
			w.consoleModal.shutdown()
		}
		// Unregister the touch-keyboard pane handler. The console window used
		// to be the only caller of this — it was destroyed and recreated on
		// every open — and with it gone the main window's own registration is
		// the one left to release.
		platformReleaseKeyboardEvents(&w.touchKbd)
		if w.shutdown != nil {
			w.shutdown()
		}
	})
}

// beginUIOp registers a UI-side goroutine that writes through the
// router / chatlog / file bridge with the shutdown tracker. Returns
// false once drainUIOps has closed the gate — the caller must skip the
// operation (the app is exiting). The Add happens under uiOpMu so it
// can never interleave with drainUIOps's close-then-Wait sequence.
// Shared by ALL windows: the console window registers through its
// parent, so console commands and file operations drain here too.
func (w *Window) beginUIOp() bool {
	w.uiOpMu.RLock()
	defer w.uiOpMu.RUnlock()
	if w.uiOpClosed {
		return false
	}
	w.sendWG.Add(1)
	return true
}

// endUIOp releases a slot taken by beginUIOp.
func (w *Window) endUIOp() { w.sendWG.Done() }

// invalidate asks the window for a redraw, from any goroutine. It is the one
// safe way to say that: the app.Window is created inside Run, so w.window is
// nil for the whole of construction and for anything a test drives directly,
// and app.Window.Invalidate is documented as safe to call concurrently.
func (w *Window) invalidate() {
	if w.window != nil {
		w.window.Invalidate()
	}
}

// uiStop returns the channel closed when drainUIOps begins: cancellable
// UI operations select on it to abort promptly on shutdown.
func (w *Window) uiStop() chan struct{} {
	w.uiStopOnce.Do(func() {
		w.uiStopCh = make(chan struct{})
	})
	return w.uiStopCh
}

// uiOpContext returns a context cancelled when shutdown begins, for the
// UI operations that run long enough to be worth aborting. It carries no
// deadline of its own: an operation that needs one sets it where it knows
// what it is bounding.
func (w *Window) uiOpContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := w.uiStop()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// uiStopping reports whether shutdown has begun. Used by operations that
// run OUTSIDE the gate (the file-picker phases) to bail out instead of
// touching a Window/Activity that is being destroyed. Advisory: the gate
// can close right after the check, so it narrows the window rather than
// closing it — the authoritative refusal is beginUIOp.
func (w *Window) uiStopping() bool {
	select {
	case <-w.uiStop():
		return true
	default:
		return false
	}
}

// pickerAllowed gates the blocking platform file dialogs
// (explorer.ChooseFile / CreateFile) that run outside the UI-op gate.
//
// Why they are outside the gate: both block until the user picks
// something, which can take minutes. Holding a gate slot across that
// would make every shutdown with an open dialog exceed drainUIOps's
// budget and go unclean.
//
// What this guard does NOT do: it is a check, not a lock, so it cannot
// be atomic with the call that follows. Shutdown may begin in between —
// and on Android that matters, because Gio's Window.Run executes the
// callback DIRECTLY on the calling goroutine once the driver is gone
// (app/window.go: `if w.driver == nil { f(); return }`), so the JNI work
// would run against a stale view rather than being dropped. Fusing check
// and call would mean holding uiOpMu across the dialog, i.e. blocking
// drainUIOps for its lifetime — the very thing this design avoids.
// Closing the gap for real needs a context-aware explorer API upstream
// in gioui.org/x; until then this narrows the window to a few
// instructions and every refusal is logged.
func (w *Window) pickerAllowed(op string) bool {
	if w.uiStopping() {
		log.Warn().Str("op", op).Msg("file dialog skipped: shutdown in progress")
		return false
	}
	return true
}

// drainUIOps closes the UI operation gate (no new tracked goroutines
// can start, in any window) and waits — bounded — for the ones already
// running; reports whether they all completed.
func (w *Window) drainUIOps(timeout time.Duration) bool {
	w.uiOpMu.Lock()
	alreadyClosed := w.uiOpClosed
	w.uiOpClosed = true
	w.uiOpMu.Unlock()
	if !alreadyClosed {
		// Signal cancellable long-running UI operations (SAF export
		// copy loop) so they stop and close their destinations instead
		// of being cut off by os.Exit mid-write.
		close(w.uiStop())
	}

	done := make(chan struct{})
	go func() {
		w.sendWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (w *Window) Run() error {
	go func() {
		defer crashlog.DeferRecover()

		window := new(app.Window)
		w.window = window
		w.fileExplorer = explorer.NewExplorer(window)
		window.Option(
			app.Title(w.t("app.title")+" — "+w.t("app.subtitle")),
			app.Size(unit.Dp(768), unit.Dp(550)),
		)

		w.touchKbd.setInvalidate(window.Invalidate)

		w.startPolling(window)

		if err := w.loop(window); err != nil {
			// Run the teardown before crashlog gets the panic: the
			// chatlog must reach disk consistently even on a UI-loop
			// failure.
			w.runShutdown()
			panic(err)
		}
		// Normal exit (window closed / Android activity destroyed):
		// close the node and the chatlog cleanly instead of letting
		// os.Exit cut sqlite off mid-write.
		w.runShutdown()
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
	// The synchronous SendMessageDelete return names the route and
	// reports local errors; the wire-side outcome arrives
	// asynchronously through this event.
	if w.eventBus != nil {
		w.eventBus.Subscribe(ebus.TopicMessageDeleteCompleted, func(outcome ebus.MessageDeleteOutcome) {
			w.handleMessageDeleteOutcome(outcome)
		})
		// Conversation-wide wipe (sidebar "Delete chat for both sides")
		// reaches its terminal status the same way: the UI runs the
		// two-phase BeginConversationDelete + CompleteConversationDelete
		// which only report local errors, while the wire-side
		// outcome arrives later via this event.
		w.eventBus.Subscribe(ebus.TopicConversationDeleteCompleted, func(outcome ebus.ConversationDeleteOutcome) {
			w.handleConversationDeleteOutcome(outcome)
		})
		// A send the node REFUSED rather than failed. The service layer
		// leaves the wording to us — it publishes the error and writes a
		// generic fallback line for runtimes with no UI — because "the
		// store is busy, try again" has to be said in the user's
		// language, like every other status of this feature.
		w.eventBus.Subscribe(ebus.TopicMessageSendFailed, func(result ebus.MessageSendFailedResult) {
			w.handleMessageSendFailed(result)
		})
		// Receiver-side download completion: filetransfer.Manager has
		// just verified and stored the file at its CompletedPath. Play
		// the download-done audio cue so the user notices the transfer
		// finished even when the file tab is not in the foreground.
		// The handler hops onto its own goroutine because the ebus
		// subscriber inbox is bounded (64) and a blocking 5s playback
		// here would let publisher overflow drop other notifications.
		w.eventBus.Subscribe(ebus.TopicFileDownloadCompleted, func(_ ebus.FileDownloadCompletedResult) {
			go playDownloadDone()
		})
		// Identity lookup progress (§4.9): the axes arrive as full-state
		// events; the UI surfaces the ones a user can act on.
		w.eventBus.Subscribe(ebus.TopicIdentityResolutionChanged, func(state ebus.IdentityResolutionState) {
			w.handleIdentityResolutionState(state)
		})
		// A peer's reactions have been merged. The event names the
		// conversation and not the change, because the chip rows are drawn
		// from a whole-conversation cache — reloading it is both the
		// cheapest and the only correct response.
		w.eventBus.Subscribe(ebus.TopicReactionsChanged, func(domain.PeerIdentity) {
			w.noteReactionsChanged()
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
		case app.ViewEvent:
			// Raw macOS executables (including `go run`) have no bundle from
			// which AppKit could load CFBundleIconFile. Install the embedded
			// application icon once the native application/window exists.
			platformSetAppIcon()
			// The window's native handle is now known: bind it and register
			// the touch-keyboard Showing/Hiding handler proactively, so a
			// keyboard the user opens before their first editor tap is tracked.
			platformBindKeyboardWindow(&w.touchKbd, platformViewHWND(e))
		case app.FrameEvent:
			gtx := app.NewContext(&w.ops, e)
			w.layout(gtx)
			e.Frame(gtx.Ops)
		}
	}
}

func (w *Window) layout(gtx layout.Context) layout.Dimensions {
	w.rootSize = gtx.Constraints.Max // window size, for anchors computed deep in the tree
	// Park each closed context menu's scroll at the top so its NEXT open starts
	// fresh (this runs the frame before/as it opens, since the open is triggered
	// during event handling below); an open menu's own scroll is left alone.
	// A menu that closed while it held keyboard focus hands focus back to the
	// "⋯" button that opened it, here, for the same reason: the close happens
	// during event handling further down, so the frame after it is the first one
	// that can see it. restoreOnClose is a no-op for a menu that never held
	// focus, and yields to anything that has already claimed focus meanwhile.
	//
	// The composer is the fallback for a trigger that is no longer drawn — the
	// "⋯" of a message deleted under its own menu, of a peer the menu just
	// removed, of a row scrolled out of the list. It is the one focus target
	// laid out on every frame of this window: layoutMain is the base of the
	// stack rather than one of several screens, and the composer card inside it
	// is a Rigid with no condition on it. Handing focus to a widget that is not
	// in the frame is the same as handing it to nothing, which is the state
	// restoreOnClose exists to avoid.
	if w.contextMenuPeer.IsZero() {
		w.ctxMenuList.Position = layout.Position{}
		w.lastCtxMenuMode = 0
		w.peerMenuFocus.restoreOnClose(gtx, &w.messageEditor)
	}
	if w.msgContextMsg == nil {
		w.msgCtxMenuList.Position = layout.Position{}
		w.msgMenuFocus.restoreOnClose(gtx, &w.messageEditor)
	}
	if !w.identityPanelVisible {
		w.identityPanelFocus.restoreOnClose(gtx, &w.myIdentityButton)
	}
	if !w.consoleModalVisible() && w.consoleModal != nil {
		// The console hands the keyboard back to the button that opened it.
		// The composer is the fallback for the same reason the menus use it:
		// it is the one focus target every frame of this window draws.
		w.consoleModal.focusRing.restoreOnClose(gtx, &w.messageEditor)
	}
	if !w.imageViewerVisible() && w.imageViewer != nil {
		// The viewer has no trigger to hand the keyboard back to — the
		// thumbnail that opened it belongs to a row that may be gone — so the
		// composer takes it, which is where focus was before the viewer.
		w.imageViewer.focusRing.restoreOnClose(gtx, &w.messageEditor)
	}
	w.snap = w.router.Snapshot()
	// Before rebuildMsgCache, which is what the chip rows are drawn against:
	// a peer's reactions arrive on the event bus goroutine, which can only
	// raise a flag (see noteReactionsChanged), and this is the first point on
	// the goroutine that owns the cache at which they can be read.
	w.reloadStaleReactions()
	w.rebuildPeerLastOnlineIndex()
	w.rebuildMsgCache()
	// AFTER the snapshot, and after rebuildMsgCache: the signature carries
	// chatOrder, which rebuildMsgCache derives from the messages this snapshot
	// brought. Running above either would compare the PREVIOUS frame's digest
	// and then record it as current, so the check would permanently trail
	// reality by one frame here. Nothing between the top of layout and this
	// line reads menuBtnRects, so the move costs nothing, and the scroll/resize
	// half of the check still happens before any row lays out.
	w.invalidateStaleMenuRects() // drop ⋯ rects a reorder, scroll or resize moved
	w.applyDeferredScroll()
	w.swapComposerDraftOnPeerChange()
	w.resetReplyOnPeerChange()
	w.dropStaleReply()
	w.dropStaleMsgMenu()
	// Evaluate LAST frame's outside-tap records BEFORE any action handlers:
	// a touch outside every editor cancels pending keyboard shows and
	// clears editor focus (→ blur-driven hide), while handlers below that
	// intentionally focus an editor issue their FocusCmd afterwards and win.
	//
	// That clear is the only time focus is emptied on purpose here, so it also
	// cancels the menus' pending restore — the hand-back itself as well as the
	// check on it, since the tap that cleared focus is usually the one on a menu
	// item and the menu is still open right here (its handler closes it below).
	// Without this, restoreOnClose would read the empty focus on a later frame as
	// the close's own doing, hand focus to the trigger and from there to the
	// composer, and cancel the blur-driven hide in trackEditorFocus below —
	// leaving up the keyboard the tap asked to dismiss.
	if w.touchKbd.dismissOnOutsideTap(gtx) {
		w.peerMenuFocus.abandonRestore()
		w.msgMenuFocus.abandonRestore()
		w.identityPanelFocus.abandonRestore()
	}

	// Track cursor position at window level for accurate context menu
	// placement. Runs BEFORE the action handlers below so that a press and
	// release landing in one frame still update the input-source flag the
	// handlers consult (Reply/Alias raising the keyboard).
	// Press positions are additionally kept per PointerID, for EVERY source
	// (touch, mouse, pen) and with the frame each press began on:
	// lastCursorPos ends up holding the LAST event of the frame, which a
	// same-frame drag, a second finger or a mouse moving after a tap would
	// move away from the press point a menu must anchor to. The frame stamp
	// serves the widget.Clickable path, whose Press record carries no
	// PointerID to look up by (see pressWindowPos).
	// Deferred cleanup of the ANCHOR map only: card handlers run AFTER
	// this tracker in the same frame, so a press whose Release arrived in
	// the same frame must stay resolvable for them — anchor entries are
	// removed one frame later. The touch ACTIVE set (touchPressPos) is by
	// contrast updated immediately in the event loop below: keeping a
	// released finger in it would make the next finger pressed in the
	// same frame read as multi-touch and instantly cancel its long-press.
	if w.pressCancelAll {
		clear(w.pointerPressPos)
		w.pressCancelAll = false
		w.pressCleanup = w.pressCleanup[:0]
	}
	for _, id := range w.pressCleanup {
		delete(w.pointerPressPos, id)
	}
	w.pressCleanup = w.pressCleanup[:0]

	defer clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, &w.cursorTracker)
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: &w.cursorTracker,
			Kinds:  pointer.Move | pointer.Press | pointer.Drag | pointer.Release | pointer.Cancel,
		})
		if !ok {
			break
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		switch pe.Kind {
		case pointer.Press:
			w.lastCursorPos = pe.Position.Round()
			w.lastInputTouch = pe.Source == pointer.Touch
			w.lastPressAt = gtx.Now
			w.pointerPressPos[pe.PointerID] = pressPoint{pos: pe.Position.Round(), at: gtx.Now}
			if pe.Source == pointer.Touch {
				if len(w.touchPressPos) > 0 {
					// Second concurrent touch: remember the moment so
					// long-press guards see it even when this finger
					// releases within the same frame.
					w.multiTouchAt = gtx.Now
				}
				w.touchPressPos[pe.PointerID] = pe.Position.Round()
				w.touchKbd.noteWindowTouchPress(pe.PointerID)
			}
		case pointer.Move, pointer.Drag:
			w.lastCursorPos = pe.Position.Round()
		case pointer.Release:
			delete(w.touchPressPos, pe.PointerID) // active set: immediate
			w.pressCleanup = append(w.pressCleanup, pe.PointerID)
		case pointer.Cancel:
			// Gio broadcasts Cancel with a zero PointerID on pointer grabs
			// and WM_CANCELMODE — deleting only pe.PointerID would leave
			// the real touch IDs stranded in the map, and every later
			// touch would read as multi-touch, killing long-press. Clear
			// the whole active-touch set.
			clear(w.touchPressPos) // active set: immediate
			w.pressCancelAll = true
		}
	}

	// Not while the console covers it: the reply row's Cancel is a background
	// widget, and reading its clicks would put it in the focus traversal.
	if !w.consoleModalVisible() {
		w.handleReplyContextClicks(gtx)
	}
	w.handlePendingActions()
	w.handleActions(gtx)
	ui.Fill(gtx, color.NRGBA{R: 12, G: 15, B: 20, A: 255})

	// Symmetric keyboard hide: when every editor of this window has lost
	// focus (tap on a button, chat, etc.), ask the keyboard we opened to
	// hide — Gio's ShowTextInput is a no-op on Windows in both directions.
	w.touchKbd.trackEditorFocus(gtx, w.anyEditorFocused(gtx))

	// Publish what THIS frame measures, at its end — deferred so it happens
	// however the function returns. The header yields on what the composer and
	// its label actually came out to; keyboardYieldingChrome below reads the
	// number the previous frame left, and endTailFrame explains why the frame
	// that measures a new one has to ask for the frame that uses it.
	defer w.touchKbd.endTailFrame(gtx)

	// Hand focus to the composer after a contact was picked. This used to sit
	// inside messageInputCard, which was fine until keyboardTailRow started
	// laying the composer out a second time to measure it: a one-shot action
	// performed during layout depends on how many times the layout runs, and
	// the measuring pass is deliberately unable to complete it (gtx.Execute is
	// dropped on a disabled source). It belongs to the frame, not to a widget,
	// so it runs here — once, whatever the layout does. It stays after
	// trackEditorFocus so noteExplicitEditorFocus still suppresses the NEXT
	// frame's dismiss evaluation, exactly as it did from inside the composer.
	//
	// Focusing a tag before the frame registers it is fine: the router only
	// drops focus at Frame time for tags the frame never mentioned, and the
	// composer is laid out below. What is NOT free is the move from the middle
	// of the frame to the top — see the invalidate at the end of this function.
	//
	// A context menu standing over the composer VOIDS the request instead of
	// postponing it, and does so here rather than at the setters. Here,
	// because every setter runs from inside a widget laid out BEFORE the menu
	// opens in the same frame — the row's Clickable completes at the top of
	// layoutRecipientButton while openMenu runs further down it, and the "⋯"
	// button is nested inside the row's own Clickable, so both fire — which
	// leaves the setters no menu to see. Voided rather than deferred, because
	// a request kept alive until the menu closes would hand focus to the
	// composer on the very frame restoreOnClose hands it back to the trigger.
	focusComposer, raiseKeyboard := consumeComposerFocus(w.focusComposerPending, w.composerKeyboardPending, w.contextMenuOpen())
	w.focusComposerPending = false
	w.composerKeyboardPending = false
	if focusComposer {
		w.touchKbd.noteExplicitEditorFocus()
		gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
		if raiseKeyboard {
			// Touch-driven contact selection: FocusCmd alone won't raise the
			// keyboard on Windows, so ask explicitly.
			showTouchKeyboard(&w.touchKbd)
		}
	}

	// When the Windows touch keyboard overlaps this window, the keyboard
	// inset goes on layoutMain — the panel that actually carries the
	// composer — and not on the window padding here. The header and the
	// spacer under it yield together once what the keyboard leaves free is
	// too short to hold them and an input row both; the spacer has to go
	// with the header, because keeping it spends 6dp of exactly the strip
	// the composer is short of in the case this is for.
	//
	// keyboardYieldingChrome carries the argument for why the padding had
	// to move and why the yield is the part that does the work.
	inset := layout.Inset{
		Top:    unit.Dp(windowPadYDp),
		Bottom: unit.Dp(windowPadYDp),
		Left:   unit.Dp(windowPadXDp),
		Right:  unit.Dp(windowPadXDp),
	}
	// The viewer is not one more Stacked overlay beside the others: it covers
	// the console too (a thumbnail in the console's Files tab opens it), so
	// everything the window draws — the console included — goes underneath it
	// and is laid out with input disabled while it is up.
	dims := layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			return w.layoutWindowSurfaces(w.disableUnderImageViewer(gtx), inset)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if !w.imageViewerVisible() {
				return layout.Dimensions{}
			}
			return w.viewer().layout(gtx)
		}),
	)

	// Every one of the four places that raises focusComposerPending does it
	// from inside a widget — a contact tapped in the list — which happens
	// while this frame is being laid out, long after the block at the top has
	// run. Handling it therefore waits for the next frame, and nothing else
	// guarantees there will be one: Gio draws in response to input, and the
	// tap that set the flag has already been drawn. Ask for that frame, or the
	// composer would take focus (and on a touch tap raise the keyboard) only
	// whenever the user happened to move next. While the flag lived inside the
	// composer this could not arise — it was set and consumed in the same
	// frame, in list-then-composer order.
	if w.focusComposerPending {
		gtx.Execute(op.InvalidateCmd{})
	}
	return dims
}

// layoutWindowSurfaces draws the window itself and every overlay that lives
// inside it: the main panel, the two context menus, the language dropdown,
// identity details and the console.
//
// It was extracted from layout() when the image viewer arrived, because the
// viewer is the one surface that goes OVER all of these rather than beside
// them, and a stack of eight children where one of them disables the other
// seven reads as an accident.
func (w *Window) layoutWindowSurfaces(gtx layout.Context, inset layout.Inset) layout.Dimensions {
	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			gtx = w.disableUnderConsoleModal(gtx)
			return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return keyboardYieldingChrome(gtx, &w.touchKbd, func(gtx layout.Context) layout.Dimensions {
							return layout.Flex{
								Axis: layout.Vertical,
							}.Layout(gtx,
								layout.Rigid(w.layoutMeasuredHeader),
								layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
							)
						})
					}),
					// The inset can never exceed what this child is
					// given: while the chrome is drawn the yield test
					// has already established that the free strip holds
					// it with room to spare, and once it yields this
					// child gets the whole container, which bounds the
					// inset anyway.
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Inset{
							Bottom: keyboardInsetDp(gtx, &w.touchKbd),
						}.Layout(gtx, w.layoutMain)
					}),
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
			if w.contextMenuPeer.IsZero() {
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
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if !w.identityPanelVisible {
				return layout.Dimensions{}
			}
			return w.layoutIdentityPanelOverlay(gtx)
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if !w.consoleModalVisible() {
				return layout.Dimensions{}
			}
			return w.layoutConsoleOverlay(gtx)
		}),
	)
}

// disableUnderImageViewer takes input away from the window — the console
// included — while the image viewer covers it.
//
// Same mechanism as disableUnderConsoleModal, and the same two guarantees:
// nothing underneath declares a key.FocusFilter, so Tab has only the viewer's
// own controls to walk, and a press that lands beside the picture cannot
// reach a contact row through it.
func (w *Window) disableUnderImageViewer(gtx layout.Context) layout.Context {
	if !w.imageViewerVisible() {
		return gtx
	}
	return gtx.Disabled()
}

// disableUnderConsoleModal takes input away from the window while the console
// modal covers it.
//
// This is what keeps the keyboard inside the modal, and it works because Gio
// decides what Tab can reach from what each widget DECLARES: a widget lays out
// its key.FocusFilter through gtx.Event, and a disabled Source returns from
// gtx.Event without registering anything. Nothing under here is focusable for
// the frames it is disabled, so the focus traversal has only the modal's own
// widgets to walk.
//
// The alternative was a focus ring listing the modal's items, the way the
// context menus do it. It cannot work here: a menu has four rows, while the
// console's tabs carry a Copy button per history entry, a delete/download/
// restart set per file transfer, the donate rows and the recording controls.
// Enumerating them made everything NOT enumerated unreachable — a worse bug
// than the one it fixed. Removing what is outside scales; listing what is
// inside does not.
//
// It also settles the pointer half for free: a press on the contact list under
// the modal is refused here as well as by the shell's backdrop.
func (w *Window) disableUnderConsoleModal(gtx layout.Context) layout.Context {
	if !w.consoleModalVisible() {
		return gtx
	}
	return gtx.Disabled()
}

// composerDraft is the unsent state stashed per conversation: the message
// text and the path of a picked-but-not-yet-sent attachment.
type composerDraft struct {
	text         string
	attachedFile string
}

// swapComposerDraftOnPeerChange saves the current composer (text + selected
// file) under the conversation being left and restores whatever was stashed
// for the newly opened conversation. A draft is only kept when something was
// actually typed or attached; an empty composer clears the slot so stale
// drafts do not linger. Runs every frame before handlePendingActions so the
// swap settles before any pending attachment delivery is applied.
func (w *Window) swapComposerDraftOnPeerChange() {
	peer := w.snap.ActivePeer
	if peer == w.draftPeer {
		return
	}

	// Stash the composer state we are leaving behind. Any typed character
	// counts (not just non-whitespace) so an in-progress line of spaces or
	// newlines is preserved, as is a picked-but-unsent attachment.
	text := w.messageEditor.Text()
	if text != "" || w.attachedFile != "" {
		if w.drafts == nil {
			w.drafts = make(map[domain.PeerIdentity]composerDraft)
		}
		w.drafts[w.draftPeer] = composerDraft{text: text, attachedFile: w.attachedFile}
	} else {
		delete(w.drafts, w.draftPeer)
	}

	// Restore (or clear) for the conversation we are entering.
	d := w.drafts[peer]
	w.messageEditor.SetText(d.text)
	end := w.messageEditor.Len()
	w.messageEditor.SetCaret(end, end)
	w.attachedFile = d.attachedFile
	// No attachGen bump here: async attachment deliveries (user picks and
	// failed-send restores) are routed by their target conversation in
	// applyPendingAttach, so a delivery for the peer we just left can no longer
	// clobber this slot — and bumping would wrongly reject a valid restore
	// whose only "staleness" was the user switching away and back.

	w.draftPeer = peer
	w.maybeResolveIdentityKeys(peer)
}

// maybeResolveIdentityKeys starts the on-demand key lookup for a freshly
// opened conversation whose partner has no usable box key yet (§4.9:
// opening the chat is what starts key discovery). The resolver itself is
// single-flight with a cooldown, so a repeated open costs nothing. The RPC
// persists a durable intent (disk), hence the goroutine.
func (w *Window) maybeResolveIdentityKeys(peer domain.PeerIdentity) {
	if peer.IsZero() || w.router == nil {
		return
	}
	if contact, ok := w.snap.NodeStatus.Contacts[peer.String()]; ok && contact.BoxKey != "" {
		return
	}
	go func() {
		if _, err := w.router.ResolveIdentity(peer); err != nil {
			log.Debug().Err(err).Str("peer", peer.String()).Msg("identity_resolve_kick_failed")
		}
	}()
}

// setReplyContext sets (or, with nil, clears) the reply quote. Kept as a single
// mutation point for readability. The composer is cleared synchronously at
// send, so no revision bookkeeping is needed here.
func (w *Window) setReplyContext(m *service.DirectMessage) {
	w.replyToMsg = m
}

// clearReplyQuiet drops the reply quote. Alias of setReplyContext(nil), kept for
// call-site readability at navigational/external reset points.
func (w *Window) clearReplyQuiet() {
	w.replyToMsg = nil
}

// forgetPeerComposerState drops all composer bookkeeping for a conversation
// that has been removed, so re-adding the same identity does not resurrect its
// stashed draft text/file and the next peer swap cannot re-save it. When the
// removed conversation was the open one, the live composer (text, attachment,
// reply) is cleared too. Pending send records for the peer are dropped so a
// late completion cannot touch a rebuilt slot.
func (w *Window) forgetPeerComposerState(peer domain.PeerIdentity, wasActive bool) {
	// The dropped draft and retry entries were the last owners of any
	// picker staging copies they referenced — release them (no-op for
	// regular filesystem paths).
	if d, ok := w.drafts[peer]; ok {
		releaseStagedAttachment(d.attachedFile)
	}
	for _, fs := range w.failedSends[peer] {
		releaseStagedAttachment(fs.file)
	}
	delete(w.drafts, peer)
	delete(w.attachGen, peer)
	delete(w.failedSends, peer)
	delete(w.failedShown, peer)
	// Advance the forget-epoch so an in-flight file pick or restore whose
	// dialog/send started before this removal is dropped by applyPendingAttach
	// instead of resurrecting the deleted conversation's draft.
	if w.peerForgetEpoch == nil {
		w.peerForgetEpoch = make(map[domain.PeerIdentity]uint64)
	}
	w.peerForgetEpoch[peer]++
	if wasActive {
		w.messageEditor.SetText("")
		releaseStagedAttachment(w.attachedFile)
		w.attachedFile = ""
		w.clearReplyQuiet()
	}
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
	w.clearReplyQuiet()
	w.msgContextMsg = nil
	w.scrollToMsgID = ""
	// Reset ALL per-message widget caches HERE, at the top of layout,
	// rather than lazily inside messageSelectable(): the lazy reset ran
	// mid-frame, AFTER the first bubble had already registered its
	// rightClickState via event.Op — recreating the map then orphaned
	// that registered tag, and the first right-click/long-press after a
	// chat switch was delivered to a state no handler would ever read.
	w.messageSelectables = make(map[string]*widget.Selectable)
	w.msgRightClick = make(map[string]*rightClickState)
	w.replyQuoteTags = make(map[string]*widget.Clickable)
	w.msgMenuBtns = make(map[string]*widget.Clickable)
	w.msgReactionChips = make(map[domain.MessageID]*ui.ReactionChipsState)
	// The reactions on screen belong to the conversation being left. Reloading
	// rather than clearing keeps the new conversation's chips from appearing a
	// frame late, which reads as them being added by the switch.
	w.reloadReactions()
	// And the conversation being ENTERED may already be known not to take
	// reactions — the refusal can have been learned while another chat was
	// open, and the event that carried it was consumed by that other chat.
	w.announceReactionsAreLocalOnly()
	// The per-message ⋯ buttons just recreated above are new pointers;
	// drop their cached rectangles so the map cannot accumulate entries
	// keyed by buttons that no longer exist.
	w.menuBtnRects = make(map[*widget.Clickable]image.Rectangle)
	w.lastChatPeer = peer
}

// dropStaleReply clears the reply context when the quoted message no
// longer exists in the active conversation — the peer deleted it (a
// single message_delete or a full conversation wipe) while the user was
// still composing. Without this the composer keeps rendering a quote of
// a message that is gone and the eventual send fails reply-reference
// validation ("reply_to message not found in conversation"). The editor
// text is untouched: only the quote is dropped, matching the delete
// semantics everywhere else in the UI.
//
// The lookup rides msgCacheByID (rebuilt in rebuildMsgCache only when
// the snapshot generation changes), so the per-frame cost is one nil
// check + one map hit while a reply is active. Gated on CacheReady so a
// transiently empty snapshot mid conversation-load cannot wipe a reply
// that is still valid; the peer-switch case is already handled by
// resetReplyOnPeerChange above.
func (w *Window) dropStaleReply() {
	if w.replyToMsg == nil || !w.snap.CacheReady {
		return
	}
	if _, ok := w.msgCacheByID[w.replyToMsg.ID]; ok {
		return
	}
	w.clearReplyQuiet()
}

// dropStaleMsgMenu closes the message context menu when the message it acts on
// is no longer in the conversation. msgContextMsg holds a COPY taken when the
// menu opened, so nothing about it goes stale on its own; the row behind it
// does. An incoming message_delete, a peer-side wipe or a local clear removes
// the row while the overlay keeps offering actions for it: Reply would quote a
// message that is gone (focusing the composer and raising the keyboard on the
// way, only for dropStaleReply to drop the quote a frame later), and Delete
// would dispatch a delete command for an ID the conversation no longer has.
//
// Same mechanism and same gate as dropStaleReply just above, deliberately: one
// map hit per frame while a menu is open, and CacheReady keeps a transiently
// empty snapshot mid conversation-load from closing a menu that is still valid.
// The peer-switch case is already covered by resetReplyOnPeerChange.
//
// Called from layout() before every handler that reads msgContextMsg, so the
// clear is what those handlers see: handleReplyContextClicks and
// handleMsgContextMenuActions both open with a nil check and return, which is
// how a click already queued against the vanished row is discarded rather than
// acted on. The menu's focus contract needs no help — the close happens below
// layout's own closed-menu check, exactly like every other close, so
// restoreOnClose picks it up on the next frame, which the Invalidate asks for.
//
// The identity menu needs no twin of this. Its subject leaves only through
// RemovePeer, whose one caller is the confirmation inside that very menu, and
// that handler closes the menu itself.
func (w *Window) dropStaleMsgMenu() {
	if w.msgContextMsg == nil || !w.snap.CacheReady {
		return
	}
	if _, ok := w.msgCacheByID[w.msgContextMsg.ID]; ok {
		return
	}
	w.msgContextMsg = nil
	if w.window != nil {
		w.window.Invalidate()
	}
}

// Gio widgets are single-threaded — mutations MUST happen on the UI goroutine.
func (w *Window) handlePendingActions() {
	pa := w.router.ConsumePendingActions()

	// Drain ALL queued attachment updates (triggerFileAttach picks). Each
	// carries its target conversation and is routed by applyPendingAttach, so
	// draining the whole queue never lets one
	// peer's delivery displace another's. All w.attachedFile / w.attachGen
	// mutations stay on the UI goroutine.
	w.drainPendingAttach()

	// A failed TEXT send arrives here from the router (marshaled via
	// PendingActions since the send completes on a background goroutine). A
	// failed FILE send arrives over the pendingFailed channel. Both become
	// retriable failedSend entries — the composer is never touched.
	for _, r := range pa.ComposerRestore {
		// Drop the restore if the contact was removed while this text send was
		// in flight (its forget-epoch advanced past the one captured at send).
		if w.peerForgetEpoch[r.Peer] != r.Epoch {
			continue
		}
		w.addFailedSend(r.Peer, failedSend{body: r.Body, replyTo: r.ReplyTo})
	}
	for drained := false; !drained; {
		select {
		case m := <-w.pendingFailed:
			if w.peerForgetEpoch[m.peer] == m.epoch {
				w.addFailedSend(m.peer, failedSend{body: m.body, replyTo: m.replyTo, file: m.file})
			} else {
				// Contact removed while the failure was in flight: the
				// dropped entry was the last owner of the staged copy.
				releaseStagedAttachment(m.file)
			}
		default:
			drained = true
		}
	}
	if pa.ScrollToEnd {
		w.chatList.Position.BeforeEnd = false
	}
	if !pa.RecipientText.IsZero() {
		w.recipientEditor.SetText(pa.RecipientText.String())
	}
}

// drainPendingAttach applies every attachment delivery currently queued on the
// pendingAttach channel (user picks, failed-send restores). Non-blocking: it
// returns as soon as the channel is empty. Runs on the UI goroutine.
func (w *Window) drainPendingAttach() {
	for {
		select {
		case msg := <-w.pendingAttach:
			w.applyPendingAttach(msg)
		default:
			return
		}
	}
}

// addFailedSend records an unsent message for retry. The composer is never
// touched. Empty entries (no body, no file) are ignored. Callers are
// responsible for the removed-contact guard: both the text (ComposerRestore)
// and file (pendingFailed) drains in handlePendingActions compare the send's
// captured forget-epoch against the peer's current one and skip a stale
// restore, so a failure for a since-deleted contact never reaches here. A
// peer absent from snap.Peers is NOT a drop reason — a Known-ID contact only
// enters snap.Peers after its first successful send, and its failures must
// still be retriable.
func (w *Window) addFailedSend(peer domain.PeerIdentity, fs failedSend) {
	if fs.body == "" && fs.file == "" {
		return
	}
	if w.failedSends == nil {
		w.failedSends = make(map[domain.PeerIdentity][]failedSend)
	}
	w.failedSends[peer] = append(w.failedSends[peer], fs)
}

// shownFailedPrefix splits a peer's failed sends into the entries the banner
// last rendered (the prefix the user saw) and the rest that arrived afterwards.
// Entries are only ever appended, so the shown set is always a leading prefix;
// the count is clamped in case the list shrank (e.g. contact removed).
func (w *Window) shownFailedPrefix(peer domain.PeerIdentity) (shown, unseen []failedSend) {
	list := w.failedSends[peer]
	n := w.failedShown[peer]
	if n > len(list) {
		n = len(list)
	}
	// Copy so the caller can reassign w.failedSends[peer] without aliasing the
	// backing array of the slice it is iterating.
	shown = append([]failedSend(nil), list[:n]...)
	unseen = append([]failedSend(nil), list[n:]...)
	return shown, unseen
}

// setFailedSends replaces a peer's failed-send list (deleting the key when
// empty). failedShown is reset to 0, NOT len(list): the replacement list has
// not been rendered yet, so nothing in it counts as "shown" until the next
// layoutFailedSends records the real count. This is what makes a second
// Retry/Dismiss click queued in the SAME frame a no-op instead of acting on
// the unseen tail the first click preserved.
func (w *Window) setFailedSends(peer domain.PeerIdentity, list []failedSend) {
	if len(list) == 0 {
		delete(w.failedSends, peer)
		delete(w.failedShown, peer)
		return
	}
	w.failedSends[peer] = list
	w.failedShown[peer] = 0
}

// retryFailedSends re-dispatches only the failed sends the banner actually
// showed (see shownFailedPrefix); entries that arrived after the last render
// stay in the banner. Each re-dispatch that fails again re-appends itself
// through the normal failure path.
func (w *Window) retryFailedSends(peer domain.PeerIdentity) {
	retrying, unseen := w.shownFailedPrefix(peer)
	if len(retrying) == 0 {
		return
	}
	w.setFailedSends(peer, unseen)
	for _, fs := range retrying {
		if fs.file != "" {
			w.sendFileCore(peer, fs.file, fs.body, fs.replyTo)
			continue
		}
		outgoing := domain.OutgoingDM{Body: fs.body, ReplyTo: fs.replyTo, FromComposer: true, ComposerEpoch: w.peerForgetEpoch[peer]}
		if err := w.router.SendMessage(peer, outgoing); err != nil {
			// Immediate rejection: keep it in the list to retry later.
			w.setFailedSends(peer, append(w.failedSends[peer], fs))
			switch {
			case errors.Is(err, service.ErrConversationDeleteInflight):
			case errors.Is(err, service.ErrRecipientKeysUnknown):
				w.router.SetSendStatus(w.t("status.recipient_keys_unknown"))
			default:
				w.router.SetSendStatus(w.t("status.send_failed", err.Error()))
			}
		}
	}
}

// dismissShownFailedSends clears only the entries the banner showed, keeping
// any that arrived after the last render (the user has not seen those yet).
func (w *Window) dismissShownFailedSends(peer domain.PeerIdentity) {
	shown, unseen := w.shownFailedPrefix(peer)
	// Dismissal discards the entries for good — release any staged
	// picker copies they were the last owners of.
	for _, fs := range shown {
		releaseStagedAttachment(fs.file)
	}
	w.setFailedSends(peer, unseen)
}

// applyPendingAttach commits a pendingAttachMsg to the composer. Must be
// called on the UI goroutine.
//
// The delivery is first routed to the conversation it belongs to (msg.peer).
// If that conversation is not the one currently open, the attachment is stored
// in that peer's draft rather than applied to the live composer — so a file
// picked in (or restored for) contact A never appears on contact B just because
// the user switched while the background dialog/send was in flight.
//
// Generation checks are PER-PEER (attachGen[msg.peer]): a user pick bumps its
// conversation's counter, so an in-flight restore for THAT conversation is
// invalidated — but an attachment action in a different chat never is. A
// restore is honored only when its captured generation still matches the
// target conversation's counter AND that conversation's attachment slot is
// empty, whether the slot is the open composer or a stashed draft.
func (w *Window) applyPendingAttach(msg pendingAttachMsg) {
	// Drop deliveries for a conversation removed since the pick/send started —
	// otherwise a late file-dialog result would resurrect a deleted contact's
	// draft. (Non-removed peers keep epoch 0/unchanged, so normal picks pass.)
	// The rejected delivery was the last owner of its staged copy (a user
	// pick just materialized it; a restore's retry entry is already gone).
	if w.peerForgetEpoch[msg.peer] != msg.epoch {
		releaseStagedAttachment(msg.path)
		return
	}
	if w.attachGen == nil {
		w.attachGen = make(map[domain.PeerIdentity]uint64)
	}
	if msg.peer != w.draftPeer {
		// Belongs to a conversation the user has left — stash into its draft.
		if w.drafts == nil {
			w.drafts = make(map[domain.PeerIdentity]composerDraft)
		}
		d := w.drafts[msg.peer]
		if !msg.restore {
			// User pick: authoritative for its conversation's draft. The
			// displaced attachment (if any) loses its last owner here.
			if d.attachedFile != msg.path {
				releaseStagedAttachment(d.attachedFile)
			}
			d.attachedFile = msg.path
			w.drafts[msg.peer] = d
			w.attachGen[msg.peer]++
		} else if w.attachGen[msg.peer] == msg.generation {
			// Failed-send restore into a since-left conversation's draft. Never
			// drop: attach the file only if the draft slot is free, and always
			// preserve the caption text (prepend if the draft already has some).
			if d.attachedFile == "" {
				d.attachedFile = msg.path
			} else if d.attachedFile != msg.path {
				// Slot occupied: the restored file has no owner left.
				releaseStagedAttachment(msg.path)
			}
			if msg.caption != "" {
				if d.text == "" {
					d.text = msg.caption
				} else {
					d.text = msg.caption + "\n" + d.text
				}
			}
			w.drafts[msg.peer] = d
		} else {
			// Stale restore (a newer pick bumped the generation): its
			// staged copy has no owner anymore.
			releaseStagedAttachment(msg.path)
		}
		return
	}
	if !msg.restore {
		// The displaced live-composer attachment loses its last owner.
		if w.attachedFile != msg.path {
			releaseStagedAttachment(w.attachedFile)
		}
		w.attachedFile = msg.path
		w.attachGen[msg.peer]++
		return
	}
	if msg.generation != w.attachGen[msg.peer] {
		releaseStagedAttachment(msg.path)
		return
	}
	if w.attachedFile == "" && w.messageEditor.Text() == "" {
		// Slot fully empty: replay the file and its caption for retry.
		w.attachedFile = msg.path
		if msg.caption != "" {
			w.messageEditor.SetText(msg.caption)
			end := w.messageEditor.Len()
			w.messageEditor.SetCaret(end, end)
		}
		return
	}
	// The user is already composing here (text and/or a new attachment). Do NOT
	// hijack that composition by re-attaching the old file or overwriting the
	// caption. Preserve the failed caption text losslessly by prepending it.
	// The restored file itself is dropped — release its staged copy.
	if w.attachedFile != msg.path {
		releaseStagedAttachment(msg.path)
	}
	if msg.caption != "" {
		if cur := w.messageEditor.Text(); cur == "" {
			w.messageEditor.SetText(msg.caption)
		} else {
			w.messageEditor.SetText(msg.caption + "\n" + cur)
		}
		end := w.messageEditor.Len()
		w.messageEditor.SetCaret(end, end)
	}
}

func (w *Window) handleActions(gtx layout.Context) {
	w.handleBackNavigation(gtx)

	// The image viewer covers everything, the console included, so nothing
	// below it may be read for clicks — reading a widget is what puts it in
	// Gio's focus traversal, and Enter on a Send button nobody can see still
	// posts the draft. Same rule and same exception (Back, above) as the
	// console guard further down.
	if w.imageViewerVisible() {
		return
	}

	// The Console button comes first so that the frame which OPENS the modal
	// stops at the guard below too. Checking only at the top let that one
	// frame run on and register every other control for the next one.
	if !w.consoleModalVisible() {
		for w.consoleButton.Clicked(gtx) {
			w.openConsoleModal(gtx)
		}
	}

	// While the console modal covers the window, none of the window's own
	// controls are there to be used — and reading them is not free. Clicked
	// registers the widget's key.FocusFilter, which is what puts it in Gio's
	// focus traversal, so draining Send, Attach or the language button here
	// would keep them Tab-reachable from inside the modal no matter that
	// layoutMain is drawn with input disabled. Enter on a focused Send would
	// then post the hidden draft.
	//
	// Back is the exception above: it is a key filter, not a widget, and it is
	// how the modal is dismissed on Android.
	if w.consoleModalVisible() {
		return
	}

	w.handleEmojiEscapeNavigation(gtx)

	for w.languageToggle.Clicked(gtx) {
		w.showLanguageMenu = !w.showLanguageMenu
	}
	w.handleLanguageMenu(gtx)

	for w.updateButton.Clicked(gtx) {
		openBrowser("https://github.com/piratecash/corsa/releases")
	}

	// Compact (single-pane) layout only: return from an open chat to the
	// contact list. The button is not laid out in the two-pane mode, so
	// this never fires there.
	for w.compactBackBtn.Clicked(gtx) {
		w.router.DeselectPeer()
	}

	w.handleEmojiActions(gtx)

	for w.sendButton.Clicked(gtx) {
		w.triggerSend(gtx)
	}

	for w.failedRetryButton.Clicked(gtx) {
		w.retryFailedSends(w.draftPeer)
	}

	for w.failedDismissButton.Clicked(gtx) {
		w.dismissShownFailedSends(w.draftPeer)
	}

	for w.attachButton.Clicked(gtx) {
		w.triggerFileAttach()
	}

	for w.attachCancelBtn.Clicked(gtx) {
		// Explicit dismissal is the last reference to a staged picker
		// copy (no draft or retry entry holds it while it sits in the
		// live composer slot).
		releaseStagedAttachment(w.attachedFile)
		w.attachedFile = ""
		// Bump this conversation's generation so any in-flight attach delivery
		// is rejected — the user explicitly dismissed the attachment and must
		// not see it reappear later.
		if w.attachGen == nil {
			w.attachGen = make(map[domain.PeerIdentity]uint64)
		}
		w.attachGen[w.draftPeer]++
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	w.handleMessageSubmitShortcut(gtx)
	w.handleMyIdentityPanel(gtx)

	for w.copyIdentityButton.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(w.snap.MyAddress.String())),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		if w.window != nil {
			w.window.Invalidate()
		}
	}
	w.handleShareContact(gtx)
	w.handleContactLinkPaste()

	w.handleContextMenuActions(gtx)
	w.handleMsgContextMenuActions(gtx)
	// Reply set/cancel clicks are handled by handleReplyContextClicks earlier in
	// the frame (before handlePendingActions), not here.
}

func (w *Window) handleContextMenuActions(gtx layout.Context) {
	if w.contextMenuPeer.IsZero() {
		return
	}

	if w.ctxMenuCopy.Clicked(gtx) {
		gtx.Execute(clipboard.WriteCmd{
			Type: "text/plain",
			Data: io.NopCloser(strings.NewReader(w.contextMenuPeer.String())),
		})
		w.router.SetSendStatus(w.t("status.identity_copied"))
		w.contextMenuPeer = domain.PeerIdentity{}
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
		w.touchKbd.noteExplicitEditorFocus()
		gtx.Execute(key.FocusCmd{Tag: &w.aliasEditor})
		if pointerClickedThisFrame(&w.ctxMenuAlias, gtx) && w.touchDrivenInput(gtx) {
			// The menu item was TAPPED (not activated by Return/Space, which
			// records no press): focusing alone won't bring the keyboard up on
			// Windows (and a pending blur-hide may be closing it).
			showTouchKeyboard(&w.touchKbd)
		}
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
		w.contextMenuPeer = domain.PeerIdentity{}
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
		w.contextMenuPeer = domain.PeerIdentity{}
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
		}
		// Two different failures. A history delete that fails before
		// anything is touched leaves the contact where it was — the
		// composer state, the alias and the selection must stay with it.
		// A final sweep that fails leaves the contact GONE from the
		// sidebar, the cache and the trust store, and only its history in
		// doubt: stopping here would strand the draft, the attachment and
		// the alias of a conversation the user can no longer open, and
		// leave the deleted chat selected.
		if err != nil && !errors.Is(err, service.ErrHistorySweepFailed) {
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		// Drop every trace of this conversation's composer state so a later
		// re-add starts clean (no resurrected draft text/file) and the next
		// peer swap cannot re-save a draft for the peer just deleted.
		w.forgetPeerComposerState(peer, wasActive)
		// Including what the user was told about their app: a re-added contact
		// is a new conversation and hears it again if it is still true.
		w.forgetPeerReactionNotice(peer)

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
			w.recipientEditor.SetText(remaining[nextIdx].String())
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

	// "Delete chat for both sides" — opens its own confirm step. Mirrors
	// the "Delete identity" two-click flow above; kept as a separate
	// state flag so the user can see at a glance which destructive
	// action they are about to confirm (the two share the menu card
	// surface).
	for w.ctxMenuClearChat.Clicked(gtx) {
		w.showClearChatConfirm = true
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if w.ctxMenuClearChatConfirm.Clicked(gtx) {
		peer := w.contextMenuPeer
		w.contextMenuPeer = domain.PeerIdentity{}
		w.showClearChatConfirm = false

		// No reachability check: the local thread is erased the
		// moment the wipe runs, and the peer's half is a durable
		// request the scheduler carries until they answer. Refusing
		// here would leave the user staring at a conversation they
		// asked to destroy because somebody else is offline.
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

// handleMessageSubmitShortcut sends the message on a bare Enter/Return
// press. The filters deliberately do NOT declare Optional key.ModShift:
// this handler runs before the editor's Update in the frame, and in
// Gio's router the first matching filter consumes a key event. With
// ModShift optional here, Shift+Enter was matched, consumed, and then
// discarded — the editor never saw the key, so no newline was inserted.
// Leaving Shift out of the filter lets Shift+Enter fall through to the
// editor's own Enter filter, which inserts "\n" (Submit is false).
func (w *Window) handleMessageSubmitShortcut(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(
			key.Filter{Focus: &w.messageEditor, Name: key.NameEnter},
			key.Filter{Focus: &w.messageEditor, Name: key.NameReturn},
		)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		w.triggerSend(gtx)
	}
}

// triggerSend sends what the composer holds.
//
// It takes the frame context because sending ENDS the composing gesture, and
// the emoji surfaces that gesture put on screen have to come down with it —
// closing them is focus and keyboard work, which only exists on a frame.
func (w *Window) triggerSend(gtx layout.Context) {
	// Before the send and not after it: every branch below returns somewhere,
	// and a picker left standing over a sent message is the same wrong state
	// whether the send succeeded, was blocked by a wipe, or turned out to be a
	// contact link.
	w.closeEmojiSurfaces(gtx)

	to := domain.PeerIdentityFromWire(strings.TrimSpace(w.snap.ActivePeer.String()))
	if to.IsZero() {
		to = domain.PeerIdentityFromWire(strings.TrimSpace(w.recipientEditor.Text()))
	}

	// Synchronous composer barrier while a wipe is
	// in flight for this peer. The service layer also rejects the
	// send via ErrConversationDeleteInflight (see SendMessage /
	// SendFileAnnounce), but checking here saves the file-attach
	// path from clearing the attachment and spinning up
	// prepareFileForTransmit only to fail later with a generic
	// "file prepare failed" status that does not explain why.
	if !to.IsZero() && w.router.IsConversationDeletePending(to) {
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
	// A corsa: link in the composer is a contact hand-over, not a message:
	// import it (§4.8) instead of sending the keys as chat text.
	if contactlink.IsContactLink(body) {
		if w.importContactLink(body) {
			w.messageEditor.SetText("")
		}
		return
	}
	outgoing := domain.OutgoingDM{Body: body, FromComposer: true, ComposerEpoch: w.peerForgetEpoch[to]}
	if w.replyToMsg != nil {
		outgoing.ReplyTo = domain.MessageID(w.replyToMsg.ID)
	}
	if err := w.router.SendMessage(to, outgoing); err != nil {
		// Immediate rejection: keep the composer intact so the user can retry.
		// Outgoing barrier while a wipe is in progress for this peer
		// — render a localised hint so the user understands the input is
		// intentionally blocked until the wipe terminates.
		if errors.Is(err, service.ErrConversationDeleteInflight) {
			w.router.SetSendStatus(w.t("status.compose_blocked_during_wipe"))
			return
		}
		// A keyless recipient is a waiting state, not a failure: the message
		// text survives in the composer and key discovery runs on its own.
		if errors.Is(err, service.ErrRecipientKeysUnknown) {
			w.router.SetSendStatus(w.t("status.recipient_keys_unknown"))
			return
		}
		w.router.SetSendStatus(w.t("status.send_failed", err.Error()))
		return
	}
	// Dispatched: clear the composer synchronously (UI goroutine). If the send
	// later fails on the background goroutine, the router hands the text back
	// via PendingActions.ComposerRestore, and handlePendingActions records it
	// as a retriable failedSend (surfaced in the "not sent" banner) rather than
	// re-filling the composer, which may have moved on to another conversation.
	w.messageEditor.SetText("")
	w.clearReplyQuiet()
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
	// Capture the conversation the pick belongs to now, on the UI goroutine,
	// before the blocking dialog runs. If the user switches away while the
	// dialog is open, applyPendingAttach routes the file to this peer's draft
	// rather than the composer that happens to be open when it resolves. The
	// forget-epoch lets applyPendingAttach drop the result if the conversation
	// is deleted while the dialog is open.
	pickPeer := w.snap.ActivePeer
	pickEpoch := w.peerForgetEpoch[pickPeer]
	go func() {
		// Same picker-phase rules as exportReceivedFile: outside the
		// UI-op gate (the dialog is unbounded in time), guarded by
		// pickerAllowed so a goroutine scheduled after DestroyEvent does
		// not open a dialog on a dying Activity.
		if !w.pickerAllowed("attach") {
			return
		}
		rc, err := w.fileExplorer.ChooseFile()
		if err != nil {
			// A dismissed picker is not an error and says nothing. Anything
			// else is, and it used to say nothing either — which is how a
			// picker that could not start on Android (its Java classes were
			// missing from the APK) presented as a button that does nothing
			// at all. See ANDROID_JAVA_PKGS in the Makefile.
			if !errors.Is(err, explorer.ErrUserDecline) {
				log.Error().Err(err).Msg("desktop: file picker failed")
				w.router.SetSendStatus(w.t("file.prepare_failed", err.Error()))
				w.invalidate()
			}
			return
		}
		// Picker done — everything below (materializeAttachment copies
		// the whole stream to disk, then the delivery) is real work that
		// shutdown must drain, so take a gate slot now. Mirrors the
		// export path. Refused means the app is exiting: drop the pick
		// instead of starting a copy that os.Exit would truncate.
		// Registered before the rc.Close defer so the slot is released
		// only after the stream is actually closed.
		if !w.beginUIOp() {
			_ = rc.Close()
			log.Warn().Msg("file attach dropped: shutdown in progress")
			return
		}
		defer w.endUIOp()
		defer func() { _ = rc.Close() }()

		// On desktop platforms the returned io.ReadCloser is *os.File,
		// which gives us the full path needed for SHA-256 hashing,
		// filename extraction, and copy to transmit directory. On
		// Android (and iOS) the explorer returns a content stream with
		// no filesystem path — materialize it into a temp file under
		// the app data dir so the rest of the pipeline sees a real path.
		var path string
		if f, ok := rc.(*os.File); ok {
			path = f.Name()
		} else {
			materialized, err := materializeAttachment(rc)
			if err != nil {
				w.router.SetSendStatus(w.t("file.prepare_failed", err.Error()))
				if w.window != nil {
					w.window.Invalidate()
				}
				return
			}
			path = materialized
		}

		// Deliver the pick to the UI goroutine via the buffered channel. It
		// carries pickPeer, so applyPendingAttach routes it to that
		// conversation (live composer or draft) regardless of what is open
		// when it is drained. The channel is generously buffered and fully
		// drained each frame, so a blocking send never displaces another
		// conversation's pending event.
		w.pendingAttach <- pendingAttachMsg{path: path, restore: false, peer: pickPeer, epoch: pickEpoch}
		if w.window != nil {
			w.window.Invalidate()
		}
	}()
}

// exportReceivedFile copies the stored file at path to a user-chosen
// destination, offering displayName in the save dialog. The two are
// separate on purpose: for OUTGOING files the on-disk path is a
// content-addressed transmit blob (<sha256>.ext), so deriving the name
// from the path would offer the user "<sha256>.pdf" instead of
// "report.pdf". Callers pass the announce payload's FileName; an empty
// displayName falls back to the path's base name.
//
// The copy goes through the platform save dialog (explorer.CreateFile
// — on Android the SAF ACTION_CREATE_DOCUMENT picker). This is the Android substitute for
// openFile/revealFileInDir: app-private storage is invisible to other
// apps and gogio ships no FileProvider, so a user-driven export is the
// supported way to hand a received document to the rest of the system.
// CreateFile blocks on the dialog — run everything on a background
// goroutine, mirroring triggerFileAttach.
func (w *Window) exportReceivedFile(path, displayName string) {
	if w.fileExplorer == nil || path == "" {
		return
	}
	go func() {
		// The picker phase runs OUTSIDE the UI-op gate on purpose:
		// CreateFile blocks until the user chooses a destination, which
		// can take minutes. Holding a gate slot across it would make
		// every shutdown with an open picker time out (12s) and go
		// unclean — while the phase itself touches no chatlog, no
		// router state and no disk of ours. The gate is taken for the
		// COPY, which is what must be drained.
		//
		// Bail out before doing any work if shutdown already began: this
		// goroutine may only get scheduled after DestroyEvent.
		if !w.pickerAllowed("export") {
			return
		}
		src, err := os.Open(path)
		if err != nil {
			w.router.SetSendStatus(w.t("file.export_failed", err.Error()))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}
		defer func() { _ = src.Close() }()

		// exportFileName is platform-selected: the Android variant asks
		// the device MimeTypeMap (the registry Gio's exporter actually
		// uses) and appends ".bin" when the extension does not resolve —
		// an ACTION_CREATE_DOCUMENT intent without a valid type may find
		// no handler, and for app-private storage there is no other way
		// out. See open_android.go.
		name := strings.TrimSpace(displayName)
		if base := filepath.Base(name); name == "" || base == "." || base == string(filepath.Separator) {
			name = filepath.Base(path)
		} else {
			// Never let a peer-supplied name escape the picker's file
			// name field (path separators, traversal).
			name = base
		}

		// Last check before handing control to the platform picker; see
		// pickerAllowed for what this does and does not guarantee.
		if !w.pickerAllowed("export") {
			return
		}
		dst, err := w.fileExplorer.CreateFile(exportFileName(name))
		if err != nil {
			// Dialog dismissed — not an error worth surfacing.
			if !errors.Is(err, explorer.ErrUserDecline) {
				w.router.SetSendStatus(w.t("file.export_failed", err.Error()))
				if w.window != nil {
					w.window.Invalidate()
				}
			}
			return
		}

		// Destination chosen — from here on the operation writes bytes
		// and must be drained on shutdown.
		if !w.beginUIOp() {
			// Shutdown started while the picker was open: close the
			// (empty) destination and skip the copy rather than start
			// one that cannot finish.
			_ = dst.Close()
			log.Warn().Str("path", path).Msg("file export skipped: shutdown in progress")
			return
		}
		defer w.endUIOp()

		// Chunked copy so shutdown can interrupt it BETWEEN chunks: a
		// plain io.Copy is uncancellable, and exiting the process
		// mid-copy would leave a silently truncated document. The
		// gioui.org/x/explorer File is NOT safe for concurrent use, so
		// there is deliberately no out-of-band Close from another
		// goroutine — Close racing a Write drops JNI refs the Write is
		// still touching and can native-crash Gio's Android backend.
		// The stop check is therefore between chunks only: a Write or
		// Close BLOCKED inside a slow content provider cannot be
		// interrupted, and after the 12s drain os.Exit may cut it,
		// leaving a partial file (SAF exposes no delete-API for the
		// picked document). On a normal-speed provider the abort is
		// prompt; either way the destination is Closed on the copy
		// goroutine and the outcome is logged.
		stop := w.uiStop()
		buf := make([]byte, 256<<10)
		var copyErr error
		aborted := false
	copyLoop:
		for {
			select {
			case <-stop:
				aborted = true
				break copyLoop
			default:
			}
			n, rerr := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					copyErr = werr
					break copyLoop
				}
			}
			if rerr == io.EOF {
				break copyLoop
			}
			if rerr != nil {
				copyErr = rerr
				break copyLoop
			}
		}
		closeErr := dst.Close()
		if copyErr == nil {
			copyErr = closeErr
		}
		switch {
		case aborted:
			log.Warn().Str("path", path).Msg("file export aborted by shutdown; destination may be incomplete")
		case copyErr != nil:
			w.router.SetSendStatus(w.t("file.export_failed", copyErr.Error()))
		default:
			w.router.SetSendStatus(w.t("file.export_done"))
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
	if to.IsZero() {
		return
	}
	srcPath := w.attachedFile
	caption := w.messageEditor.Text()
	var replyTo domain.MessageID
	if w.replyToMsg != nil {
		replyTo = domain.MessageID(w.replyToMsg.ID)
	}
	// Clear the composer synchronously (UI goroutine). On failure the file is
	// NOT put back into the composer — it becomes a retriable failedSend entry.
	w.attachedFile = ""
	w.messageEditor.SetText("")
	w.clearReplyQuiet()
	w.sendFileCore(to, srcPath, caption, replyTo)
}

// sendFileCore imports the file and sends a file_announce DM. Shared by the
// composer path (triggerFileSend) and retry (retryFailedSends); it reads no
// composer state, so it never touches the UI's editor. On any failure the send
// is recorded as a retriable failedSend via the pendingFailed channel.
func (w *Window) sendFileCore(to domain.PeerIdentity, srcPath, caption string, replyTo domain.MessageID) {
	if to.IsZero() {
		return
	}
	// Forget-epoch so a failure entry is dropped if this contact is deleted
	// while the send is in flight.
	sendEpoch := w.peerForgetEpoch[to]
	// Router removal generation captured BEFORE the (potentially seconds-long)
	// local file import below, so a delete during import abandons the send
	// instead of re-importing the contact and resurrecting it in the sidebar.
	sendPeerGen := w.router.PeerGeneration(to)
	w.router.SetSendStatus(w.t("file.sending"))

	// failFile records the failed file send for retry (marshaled to the UI
	// goroutine), instead of putting the file back into the composer. It runs on
	// a background goroutine — including the router's async onAsyncFailure
	// callback, which has no other repaint trigger — so it must schedule a frame
	// itself, otherwise the "not sent" banner would appear only on the next
	// unrelated redraw (heartbeat).
	failFile := func() {
		w.pendingFailed <- pendingFailedMsg{peer: to, body: caption, replyTo: replyTo, file: srcPath, epoch: sendEpoch}
		if w.window != nil {
			w.window.Invalidate()
		}
	}

	if !w.beginUIOp() {
		return
	}
	go func() {
		defer w.endUIOp()
		result, err := prepareFileForTransmit(
			w.client.StoreFileForTransmit,
			w.client.TransmitFileSize,
			w.client.RemoveUnreferencedTransmitFile,
			srcPath,
		)
		if err != nil {
			failFile()
			w.router.SetSendStatus(w.t("file.prepare_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		outgoing, err := buildFileAnnounceOutgoing(result, caption)
		if err == nil {
			// Preserve the reply context on the file DM (both first send and
			// retry); buildFileAnnounceOutgoing does not set it.
			outgoing.ReplyTo = replyTo
		}
		if err != nil {
			// Blob is stored but no token/mapping will ever reference it.
			w.client.RemoveUnreferencedTransmitFile(result.FileHash)
			failFile()
			w.router.SetSendStatus(w.t("file.prepare_failed", err))
			if w.window != nil {
				w.window.Invalidate()
			}
			return
		}

		// Fast short-circuit: if the contact was deleted while the file was
		// being imported/hashed, abandon the send WITHOUT running PrepareAndSend
		// (no network send, no ensureRecipientContact re-import). Drop the
		// prepared blob; do not restore the attachment (the conversation and its
		// composer state are gone).
		if w.router.PeerGeneration(to) != sendPeerGen {
			w.client.RemoveUnreferencedTransmitFile(result.FileHash)
			// No failedSend entry will be created and the composer slot
			// was cleared at trigger time — this goroutine held the last
			// reference to a staged picker copy.
			releaseStagedAttachment(srcPath)
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
		// Pass sendPeerGen (captured before the import) as the stale-send
		// baseline so the router's own guard measures against it, not against a
		// freshly captured generation — closing the TOCTOU between the check
		// above and the router capturing its baseline.
		// On settled async success the staged picker copy (if the source
		// was one) loses its last owner: the transmit store holds its own
		// blob and no retry entry exists. The failure callback must NOT
		// release it — the retry queue re-reads the source path.
		releaseStaged := func() {
			releaseStagedAttachment(srcPath)
		}
		if err := w.router.SendFileAnnounceFromComposerDone(to, outgoing, meta, failFile, releaseStaged, sendPeerGen); err != nil {
			// SendFileAnnounce failed synchronously (e.g. fileBridge == nil,
			// or a wipe started while
			// prepareFileForTransmit was running and tripped the outgoing
			// barrier) before the goroutine that calls PrepareAndSend
			// could take ownership. The blob has no ref and no pending —
			// clean it up.
			w.client.RemoveUnreferencedTransmitFile(result.FileHash)
			failFile()
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

// layoutMeasuredHeader draws the header and records its height for the
// language popup's anchor. It is a wrapper rather than a line inside
// layoutHeader because the header is also laid out by measuring passes that
// must not publish anything (keyboardYieldingChrome records it before deciding
// whether to draw it) — this is the call site that ends up on screen.
func (w *Window) layoutMeasuredHeader(gtx layout.Context) layout.Dimensions {
	dims := w.layoutHeader(gtx)
	w.headerHeight = dims.Size.Y
	return dims
}

func (w *Window) layoutHeader(gtx layout.Context) layout.Dimensions {
	titleText := w.t("app.title")
	if w.isCompactLayout(gtx) {
		// Phone width — which is every Android screen: the full product name
		// wraps into two lines under the header buttons, so the brand alone
		// goes there. It is its own message rather than a literal even though
		// no locale translates the brand, so the one place a translator would
		// look for the header title has both forms of it.
		titleText = w.t("app.title.compact")
	}
	title := material.Label(w.theme, unit.Sp(24), titleText)
	title.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	title.Font.Weight = 700
	title.MaxLines = 1

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
				layout.Rigid(w.layoutLanguageSelectorInline),
			)
		}),
	)
}

// navigationDismissTarget identifies the top in-app surface handled by Back
// or Escape before navigation is allowed to reach the platform.
type navigationDismissTarget uint8

const (
	dismissNothing navigationDismissTarget = iota
	dismissImageViewer
	dismissConsoleModal
	dismissIdentityPanel
	dismissMessageMenu
	dismissIdentityMenu
	dismissLanguageMenu
	dismissEmojiPicker
	dismissCompactChat
)

// topNavigationDismissTarget is the single source of truth for overlay priority
// shared by Back and Escape. It follows reverse draw order, then falls through
// to the non-modal picker and finally compact-chat navigation.
func (w *Window) topNavigationDismissTarget(gtx layout.Context) navigationDismissTarget {
	switch {
	case w.imageViewerVisible():
		return dismissImageViewer
	case w.consoleModalVisible():
		return dismissConsoleModal
	case w.identityPanelVisible:
		return dismissIdentityPanel
	case w.msgContextMsg != nil:
		return dismissMessageMenu
	case !w.contextMenuPeer.IsZero():
		return dismissIdentityMenu
	case w.showLanguageMenu:
		return dismissLanguageMenu
	case w.emojiPicker.visible:
		return dismissEmojiPicker
	case w.isCompactLayout(gtx) && !w.snap.ActivePeer.IsZero():
		return dismissCompactChat
	default:
		return dismissNothing
	}
}

// handleBackNavigation consumes the system Back key (Android hardware /
// gesture Back; XF86 Back keys on desktops route here too) while there
// is something in-app to dismiss, top-most first:
//
//  1. an open overlay, in REVERSE draw order — the overlays are Stacked
//     language → identity menu → message menu → my identity (see layout()),
//     so identity details close first, followed by the message menu and then
//     the identity menu (backing out of its confirmation / alias sub-views one
//     step at a time, exactly like Escape — reusing escapePeerMenu keeps the
//     focus-restore invariants of the menu machinery intact), and the language
//     dropdown last. The language dropdown now carries the shared popup
//     backdrop (menu_popup.go), so nothing new can be opened while it is up —
//     but one opened BEFORE it still outranks it here, which is why it stays
//     at the bottom of the list rather than being assumed unreachable;
//  2. the non-modal emoji picker;
//  3. in the compact layout, an open chat — Back returns to the contact
//     list (DeselectPeer).
//
// The filter is registered ONLY while such a target exists: an
// unconsumed Back reaches the platform (Java_org_gioui_GioView_onBack
// sees processEvent return false) and closes the Activity — the expected
// behaviour on the contact list with nothing open. Overlay dismissal is
// not gated on the compact mode: menus overlay both layouts.
func (w *Window) handleBackNavigation(gtx layout.Context) {
	if w.topNavigationDismissTarget(gtx) == dismissNothing {
		return
	}
	for {
		ev, ok := gtx.Event(key.Filter{Name: key.NameBack})
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		switch w.topNavigationDismissTarget(gtx) {
		case dismissImageViewer:
			w.escapeImageViewer()
		case dismissConsoleModal:
			w.escapeConsoleModal(gtx)
		case dismissIdentityPanel:
			w.closeIdentityPanel()
		case dismissMessageMenu:
			w.escapeMsgMenu()
		case dismissIdentityMenu:
			w.escapePeerMenu()
		case dismissLanguageMenu:
			w.closeLanguageMenu()
		case dismissEmojiPicker:
			w.closeEmojiPicker(gtx)
			w.dropEmojiToggleClicks(gtx)
		case dismissCompactChat:
			w.router.DeselectPeer()
		}
	}
}

// compactLayoutMaxDp is the width breakpoint for the single-pane layout.
// Below it the two-pane 30/70 split leaves the sidebar unusably narrow
// (~100dp on a 360dp phone), so the UI shows either the contact list or
// the open chat, with an explicit back affordance between them.
const compactLayoutMaxDp = 600

// isCompactLayout reports whether the available width calls for the
// single-pane phone layout.
func (w *Window) isCompactLayout(gtx layout.Context) bool {
	return gtx.Constraints.Max.X < gtx.Dp(unit.Dp(compactLayoutMaxDp))
}

func (w *Window) layoutMain(gtx layout.Context) layout.Dimensions {
	status := w.snap.NodeStatus
	recipients := w.snapRecipients()
	compact := w.isCompactLayout(gtx)
	w.ensureSelectedRecipient(recipients, compact)

	if compact {
		return w.layoutMainCompact(gtx, status, recipients)
	}

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
				// This label and the composer card are the rows the touch
				// keyboard must not cover; the chat card between them is free
				// to shrink to nothing. keyboardTailRow reports how tall they
				// really came out, which is what lets the window header yield
				// in time instead of on a guess. The label counts even though
				// nobody needs to READ it under a keyboard — it sits above the
				// composer and pushes it down by its full height either way.
				layout.Rigid(keyboardTailRow(&w.touchKbd, func(gtx layout.Context) layout.Dimensions {
					recipient := w.snap.ActivePeer
					if recipient.IsZero() {
						return layout.Dimensions{}
					}
					return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								lbl := material.Label(w.theme, unit.Sp(15), w.t("chat.with", w.peerDisplayName(recipient)))
								lbl.Color = color.NRGBA{R: 200, G: 212, B: 228, A: 255}
								lbl.Font.Weight = 600
								return lbl.Layout(gtx)
							}),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return w.layoutPendingDeletesCaption(gtx, recipient)
							}),
						)
					})
				})),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return w.layoutChatCard(gtx, status)
				}),
				layout.Rigid(keyboardTailRow(&w.touchKbd, w.layoutComposerCard)),
			)
		}),
	)
}

// layoutMainCompact is the single-pane phone variant of layoutMain: the
// contact list when no conversation is active, the open chat otherwise.
// Navigation back to the list goes through the header button
// (compactBackBtn → DeselectPeer, handled in handleActions). The
// keyboardTailRow wrappers mirror the two-pane layout: header and
// composer are the rows the touch keyboard must not cover. The contact-list
// pane also keeps the aggregate network badge pinned at the bottom: without
// the composer visible, compact layouts would otherwise hide the connection
// count entirely.
func (w *Window) layoutMainCompact(gtx layout.Context, status service.NodeStatus, recipients []domain.PeerIdentity) layout.Dimensions {
	if w.snap.ActivePeer.IsZero() {
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
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutComposerFooter(gtx, status)
			}),
		)
	}

	return layout.Flex{
		Axis: layout.Vertical,
	}.Layout(gtx,
		layout.Rigid(keyboardTailRow(&w.touchKbd, w.layoutCompactChatHeader)),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return w.layoutChatCard(gtx, status)
		}),
		layout.Rigid(keyboardTailRow(&w.touchKbd, w.layoutComposerCard)),
	)
}

// layoutCompactChatHeader replaces the plain "chat with X" label of the
// two-pane layout with a back-button + status + label row for the
// single-pane mode. The reachability dot mirrors the contact-list rows:
// in compact mode the sidebar (and its online/offline indicators) is off
// screen while a chat is open, so the header is the only place the
// user can see whether the peer is reachable.
func (w *Window) layoutCompactChatHeader(gtx layout.Context) layout.Dimensions {
	recipient := w.snap.ActivePeer
	if recipient.IsZero() {
		return layout.Dimensions{}
	}
	status := w.snap.NodeStatus
	return layout.Inset{Bottom: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{
			Axis:      layout.Horizontal,
			Alignment: layout.Middle,
		}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				btn := material.Button(w.theme, &w.compactBackBtn, w.t("chat.back"))
				btn.Background = color.NRGBA{R: 34, G: 46, B: 62, A: 255}
				btn.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				btn.Inset = layout.Inset{
					Top: unit.Dp(3), Bottom: unit.Dp(3),
					Left: unit.Dp(8), Right: unit.Dp(8),
				}
				btn.CornerRadius = unit.Dp(5)
				btn.TextSize = unit.Sp(12)
				return btn.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutReachableIndicator(gtx, status, recipient)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				lbl := material.Label(w.theme, unit.Sp(15), w.t("chat.with", w.peerDisplayName(recipient)))
				lbl.Color = color.NRGBA{R: 200, G: 212, B: 228, A: 255}
				lbl.Font.Weight = 600
				lbl.MaxLines = 1
				return lbl.Layout(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutPendingDeletesCaption(gtx, recipient)
			}),
		)
	})
}

// layoutPendingDeletesCaption renders "N waiting to be deleted for the
// peer" next to the conversation title, and nothing at all when the
// count is zero.
//
// A deletion removes the bubble immediately, so once the user has
// clicked there is no message left to hang a per-row indicator on. This
// caption is the only place a request handed to an offline peer stays
// visible, which is why it lives in the header rather than in the
// transient status line: the status line is gone with the next event,
// and the request can outlive it by days.
func (w *Window) layoutPendingDeletesCaption(gtx layout.Context, peer domain.PeerIdentity) layout.Dimensions {
	caption := w.pendingDeletesCaption(peer)
	if caption == "" {
		return layout.Dimensions{}
	}
	return layout.Inset{Left: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		lbl := material.Caption(w.theme, caption)
		lbl.Color = color.NRGBA{R: 230, G: 176, B: 96, A: 255}
		lbl.MaxLines = 1
		return lbl.Layout(gtx)
	})
}

// pendingDeletesCaption is what the header says about deletions this peer
// still owes us, or "" when they owe none.
//
// A pending whole-thread wipe wins over the per-message count: it is the
// larger request and it subsumes them, so reporting "3 messages" while a
// wipe of the entire conversation is outstanding would understate what is
// waiting.
func (w *Window) pendingDeletesCaption(peer domain.PeerIdentity) string {
	state, ok := w.snap.Peers[peer]
	if !ok || state == nil {
		return ""
	}
	if state.PendingConversationDelete {
		return w.t("chat.wipe_pending")
	}
	if state.PendingDeletes == 0 {
		return ""
	}
	return w.tCount("chat.deletes_pending", state.PendingDeletes)
}

func (w *Window) layoutContactsCard(gtx layout.Context, status service.NodeStatus, recipients []domain.PeerIdentity) layout.Dimensions {
	// Rows the recipients list will be laid out with below — zero takes the
	// empty-state early return, which lays out no list. Recorded here rather
	// than at the list itself so that return cannot skip it: see
	// setMenuListItems. The digest is taken from THIS slice, the one the card
	// goes on to lay out, rather than re-derived from the snapshot, so it
	// cannot describe an order other than the one drawn; it is written before
	// setMenuListItems because that call re-checks the signature.
	w.contactsOrder = peerOrderDigest(recipients)
	w.setMenuListItems(&w.contactsItems, len(recipients))
	// The identity-search hits are the OTHER rows in this card that carry a ⋯
	// button, and they are resolved HERE, above the card, for the same reason
	// the count above is: the digest has to describe the rows that get laid
	// out, and it has to land before the first of them does — a keyboard or
	// Narrator activation is answered from menuBtnRects while they lay out.
	// Reading the editor at this point rather than inside the card's content
	// changes nothing, since both are before the editor lays out and takes this
	// frame's keystrokes.
	searchResults := w.resolveIdentitySearchRows(status, recipients)

	return w.card(gtx, "", nil, func(gtx layout.Context) layout.Dimensions {
		return w.layoutMyIdentityButton(gtx, len(recipients))
	}, func(gtx layout.Context) layout.Dimensions {
		children := []layout.FlexChild{}
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.identitySearchCard(gtx, status, searchResults)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
			layout.Rigid(w.layoutKnownIdentitiesHeader),
			layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
		)

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

func (w *Window) layoutMyIdentityButton(gtx layout.Context, known int) layout.Dimensions {
	minHeight := min(gtx.Dp(unit.Dp(96)), gtx.Constraints.Max.Y)
	if gtx.Constraints.Min.Y < minHeight {
		gtx.Constraints.Min.Y = minHeight
	}
	border := widget.Border{
		Color:        color.NRGBA{R: 54, G: 69, B: 89, A: 255},
		CornerRadius: unit.Dp(10),
		Width:        unit.Dp(1),
	}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		button := material.ButtonLayout(w.theme, &w.myIdentityButton)
		button.Background = color.NRGBA{R: 22, G: 31, B: 42, A: 255}
		button.CornerRadius = unit.Dp(10)
		return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Top: unit.Dp(12), Bottom: unit.Dp(12), Left: unit.Dp(12), Right: unit.Dp(10)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						side := gtx.Dp(unit.Dp(44))
						iconGTX := gtx
						iconGTX.Constraints.Min = image.Pt(side, side)
						iconGTX.Constraints.Max = image.Pt(side, side)
						defer clip.Ellipse(image.Rect(0, 0, side, side)).Push(gtx.Ops).Pop()
						ui.Fill(iconGTX, color.NRGBA{R: 25, G: 119, B: 67, A: 255})
						return layout.Center.Layout(iconGTX, func(gtx layout.Context) layout.Dimensions {
							return ui.Icon(gtx, w.fingerprintIcon, unit.Dp(25), color.NRGBA{R: 240, G: 250, B: 244, A: 255})
						})
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Body1(w.theme, w.t("clients.my_identity"))
								label.Color = color.NRGBA{R: 247, G: 249, B: 252, A: 255}
								label.Font.Weight = 600
								label.MaxLines = 1
								return label.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(2)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								dims, _ := w.layoutMyIdentityAddress(gtx)
								return dims
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(5)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Caption(w.theme, w.t("clients.known", known))
								label.Color = color.NRGBA{R: 180, G: 194, B: 211, A: 255}
								label.MaxLines = 1
								return label.Layout(gtx)
							}),
						)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.Icon(gtx, w.chevronIcon, unit.Dp(22), color.NRGBA{R: 218, G: 228, B: 240, A: 255})
					}),
				)
			})
		})
	})
}

func (w *Window) layoutMyIdentityAddress(gtx layout.Context) (layout.Dimensions, widget.TextInfo) {
	style := material.Caption(w.theme, w.snap.MyAddress.String())
	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 167, G: 181, B: 199, A: 255}}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()
	label := widget.Label{WrapPolicy: text.WrapGraphemes}
	return label.LayoutDetailed(gtx, style.Shaper, style.Font, style.TextSize, style.Text, textMaterial)
}

func (w *Window) layoutKnownIdentitiesHeader(gtx layout.Context) layout.Dimensions {
	label := material.Caption(w.theme, strings.ToUpper(w.t("clients.known_title")))
	label.Color = color.NRGBA{R: 143, G: 158, B: 178, A: 255}
	return label.Layout(gtx)
}

// identitySearchMaxRows caps how many search hits get a row. The cap belongs to
// resolveIdentitySearchRows and not to the card, because it has to be applied to
// the same slice the digest is taken from: a cap in the card would let the rows
// actually drawn disagree with the signature guarding their cached rectangles.
const identitySearchMaxRows = 4

// identitySearchTextTopInset compensates for the editor's font metrics so its
// visible glyphs, rather than its line box, align with the search icon.
const identitySearchTextTopInset = unit.Dp(2)

// resolveIdentitySearchRows returns the identity-search hits this frame will lay
// out, and records them in the menu-rect signature.
//
// The recording is the point, and it closes a real hole. searchKnownIdentities
// sorts its output by identity, so editing the query does not merely lengthen or
// shorten the list — it can put a different hit in a given row while the count,
// the row heights, both scroll Positions, the contacts digest and the window
// size all stay exactly as they were. These rows carry the per-contact ⋯
// buttons (layoutRecipientButton), whose rectangles menuBtnRects caches by
// button identity, so without this a keyboard or Narrator activation after a
// query change would open a real menu at the coordinates of a row that has
// since moved. See menuRectSig.
func (w *Window) resolveIdentitySearchRows(status service.NodeStatus, recipients []domain.PeerIdentity) []domain.PeerIdentity {
	results := searchKnownIdentities(status.KnownIDs, status.ReachableIDs, recipients, w.snap.MyAddress, w.identitySearchEditor.Text())
	if len(results) > identitySearchMaxRows {
		results = results[:identitySearchMaxRows]
	}
	w.searchOrder = peerOrderDigest(results)
	w.setMenuListItems(&w.searchItems, len(results))
	return results
}

// recordSearchRowAnchor notes where this frame is about to put the search hits.
//
// Gio hands a Rigid child of a vertical Flex a Max main-axis constraint equal
// to the space left beneath its top edge, so this number IS that edge, measured
// up from the bottom of the contacts card. It is the only readback an
// immediate-mode layout offers here — there is no transform to inspect — and it
// is enough, because every way the rows can move is a height change somewhere
// above them, and every such change lands in it. See menuRectSig for what it
// closes and what it does not.
//
// Zero rows records zero rather than the live constraint: with no rows there is
// no edge, and remembering one would clear the whole cache every time an empty
// search box drifted for reasons that moved nothing.
//
// The invalidation this triggers happens in the middle of the frame, which is
// safe and has to be: no ⋯ rectangle is captured before this point (the sidebar
// is the first column laid out, and these are its first rows), while a
// keyboard or Narrator activation dispatched from the rows below is answered
// out of menuBtnRects as they lay out.
func (w *Window) recordSearchRowAnchor(gtx layout.Context, rows int) {
	avail := 0
	if rows > 0 {
		avail = gtx.Constraints.Max.Y
	}
	w.searchAvail = avail
	w.invalidateStaleMenuRects()
}

// identitySearchCard lays out the search box and one row per hit. Every hit it
// is handed is laid out: the caller capped the slice and fingerprinted what it
// capped, and those two must describe the same rows.
func (w *Window) identitySearchCard(gtx layout.Context, status service.NodeStatus, results []domain.PeerIdentity) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			fieldHeight := min(gtx.Dp(unit.Dp(48)), gtx.Constraints.Max.Y)
			gtx.Constraints.Min.Y = fieldHeight
			gtx.Constraints.Max.Y = fieldHeight
			border := widget.Border{
				Color:        color.NRGBA{R: 55, G: 70, B: 91, A: 255},
				CornerRadius: unit.Dp(9),
				Width:        unit.Dp(1),
			}
			return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				ui.FillRounded(gtx, color.NRGBA{R: 20, G: 29, B: 39, A: 255}, unit.Dp(9))
				return layout.Inset{Top: unit.Dp(11), Bottom: unit.Dp(11), Left: unit.Dp(12), Right: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return ui.Icon(gtx, w.searchIcon, unit.Dp(20), color.NRGBA{R: 139, G: 158, B: 183, A: 255})
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return w.layoutIdentitySearchEditor(gtx)
						}),
					)
				})
			})
		}),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			if len(results) == 0 {
				w.recordSearchRowAnchor(gtx, 0)
				return layout.Dimensions{}
			}
			return layout.Inset{Top: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				// Record the actual top edge of the first result, after the gap
				// between the search field and its hits has been applied.
				w.recordSearchRowAnchor(gtx, len(results))
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx, recipientsToChildren(results, func(gtx layout.Context, identity domain.PeerIdentity) layout.Dimensions {
					return w.layoutRecipientButton(gtx, status, identity, false)
				})...)
			})
		}),
	)
}

func (w *Window) identitySearchEditorStyle() material.EditorStyle {
	w.identitySearchEditor.SingleLine = true
	editor := material.Editor(w.theme, &w.identitySearchEditor, w.t("clients.search_placeholder"))
	editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	editor.HintColor = color.NRGBA{R: 139, G: 153, B: 174, A: 255}
	editor.TextSize = unit.Sp(14)
	return editor
}

func (w *Window) layoutIdentitySearchEditor(gtx layout.Context) layout.Dimensions {
	return layout.Inset{Top: identitySearchTextTopInset}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.DescriptionOp(w.t("clients.search_label")).Add(gtx.Ops)
		return editorTouchKeyboardArea(gtx, &w.touchKbdTags[1], &w.touchKbd, w.identitySearchEditorStyle().Layout)
	})
}

func (w *Window) layoutRecipientButton(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity, contactRow bool) layout.Dimensions {
	btn := w.recipientButton(fingerprint)
	rc := w.recipientRightClickState(fingerprint)
	// On a compact sidebar the full localized label (for example,
	// "3 дня назад") must yield to the contact name. Screen readers still get
	// the complete last-online text from the row description below.
	showLastOnlineLabel := shouldShowContactLastOnlineLabel(gtx, contactRow)

	return layout.Inset{Bottom: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		fpStr := fingerprint.String()
		for btn.Clicked(gtx) {
			w.recipientEditor.SetText(fpStr)
			w.router.SetSendStatus(w.t("status.chat_selected"))
			w.router.SelectPeer(fingerprint)
			w.focusComposerPending = true
			// A touch tap on a contact means "I want to type to them": raise
			// the keyboard, because the FocusCmd below won't on Windows. A
			// mouse/keyboard selection leaves it down (touchDrivenInput is a
			// recency gate on the last touch press).
			//
			// EXCEPT after a long-press: the long-press timer opens the
			// context menu while the finger is still down, then the finger's
			// Release still completes this outer Clickable — so btn.Clicked
			// fires a frame LATER, after openMenu's own reset. Guard on the
			// live menu state instead: if this peer's menu is open right now,
			// the click is the tail of a long-press and must NOT raise the
			// keyboard over the menu. A normal tap dismisses any open menu on
			// Press (clearing contextMenuPeer) before this Release fires, so
			// it is not caught by this guard.
			menuOpenForPeer := !w.contextMenuPeer.IsZero() && w.contextMenuPeer.String() == fpStr
			if pointerClickedThisFrame(btn, gtx) && w.touchDrivenInput(gtx) && !menuOpenForPeer {
				w.composerKeyboardPending = true
			}
		}

		// Detect right-click (secondary button) or touch long-press for
		// the context menu. Position comes from the window-level cursor
		// tracker (lastCursorPos) because card-local coordinates don't
		// account for scroll offset and nested layout transforms.
		openMenu := func(pos image.Point) {
			w.contextMenuPeer = fingerprint
			w.contextMenuPos = pos
			w.showDeleteConfirm = false
			w.showClearChatConfirm = false
			w.showAliasEditor = false
			// Auto-select this identity so the user sees the chat.
			w.recipientEditor.SetText(fpStr)
			w.router.SelectPeer(fingerprint)
			// Focus goes to the MENU's first item, not to the composer.
			// Focusing the composer (which this used to do) put focus on a
			// widget hidden under the overlay: Tab kept walking the background,
			// and the next Enter could send the draft instead of activating the
			// highlighted menu item. Returned to this row's "⋯" button on close.
			w.peerMenuFocus.open(w.recipientMenuButton(fingerprint))
			// Whatever gains focus, opening the menu must NOT raise the
			// keyboard — the user reached for a menu, not the input. Suppress
			// any raise a same-frame selection might have set.
			w.composerKeyboardPending = false
		}
		slopPx := float32(gtx.Dp(longPressSlop))
		for {
			ev, ok := gtx.Event(pointer.Filter{
				Target: rc,
				Kinds:  pointer.Press | pointer.Release | pointer.Move | pointer.Drag | pointer.Cancel,
			})
			if !ok {
				break
			}
			pe, ok := ev.(pointer.Event)
			if !ok {
				continue
			}
			rc.handleTouchLongPress(pe, gtx.Now, slopPx, w.pressAnchor(pe))
			if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonSecondary) {
				rc.pressed = true
				rc.pressID = pe.PointerID
				// Anchor at THIS pointer's press position: lastCursorPos
				// may already belong to another pointer's later event.
				rc.pressCursor = w.pressAnchor(pe)
			}
			if pe.Kind == pointer.Cancel {
				// A grab or WM_CANCELMODE voided this press — a later
				// Release (possibly of another pointer) must not open the
				// menu at a stale position.
				rc.pressed = false
			}
			if pe.Kind == pointer.Release && rc.pressed && pe.PointerID == rc.pressID {
				// Bound to the pointer that armed the press: on a tablet a
				// finger releasing concurrently with a mouse/pen right-
				// click must not open the menu for it.
				rc.pressed = false
				openMenu(rc.pressCursor)
			}
		}
		w.cancelLongPressOnMultiTouch(rc)
		if rc.longPressTriggered(gtx) {
			openMenu(rc.pressCursor)
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

			ui.Fill(gtx, bg)
			// Tight padding matching the window-edge inset (Top/Bottom 4,
			// Left/Right 6) so the card content sits close to its border.
			dims := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(6), Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return w.layoutContactPresenceAvatar(gtx, status, fingerprint)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{
									Axis:      layout.Horizontal,
									Spacing:   layout.SpaceBetween,
									Alignment: layout.Middle,
								}.Layout(gtx,
									layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
										title := material.Body1(w.theme, w.peerDisplayName(fingerprint))
										title.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
										title.Font.Weight = 600
										title.MaxLines = 1
										return title.Layout(gtx)
									}),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										if !contactRow {
											return layout.Dimensions{}
										}
										ps := w.snap.Peers[fingerprint]
										if ps == nil || ps.Unread == 0 {
											return layout.Dimensions{}
										}
										return layout.Inset{Left: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
											return w.layoutUnreadBadge(gtx, ps.Unread)
										})
									}),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										if !showLastOnlineLabel {
											return layout.Dimensions{}
										}
										return layout.Inset{Left: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
											return w.layoutContactLastOnline(gtx, status, fingerprint)
										})
									}),
									layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										// "⋯" — always-available touch path to the
										// contact menu (long-press is limited by
										// Gio's pointer-grab threshold).
										mb := w.recipientMenuButton(fingerprint)
										for mb.Clicked(gtx) {
											openMenu(w.menuAnchorForClick(mb, gtx))
										}
										return w.menuDotsButton(gtx, mb, color.NRGBA{R: 160, G: 175, B: 195, A: 255}, w.t("context.menu_button_contact"))
									}),
								)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
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
					}),
				)
			})
			// DescriptionOp is deliberately emitted after every child in the
			// clickable area. Gio keeps one description per semantic area, so
			// avatar/timestamp descriptions emitted earlier would overwrite one
			// another depending on layout order.
			semantic.DescriptionOp(w.contactPresenceDescription(gtx.Now, status, fingerprint, contactRow)).Add(gtx.Ops)
			return dims
		})
	})
}

func (w *Window) layoutChatCard(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	recipient := w.snap.ActivePeer
	// Rows the conversation list will actually be laid out with below — zero on
	// every early-return path here, each of which lays out no list at all.
	// Recorded BEFORE those returns and before any row lays out, so a menu
	// activated by keyboard/Narrator later in THIS frame already sees the cache
	// dropped rather than a rectangle the new content moved (see menuRectSig).
	listRows := len(w.snap.ActiveMessages)
	if recipient.IsZero() {
		listRows = 0
	}
	w.setMenuListItems(&w.chatItems, listRows)
	var rows []string

	if recipient.IsZero() {
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
		ui.Fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

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
		footerMacro := op.Record(gtx.Ops)
		footerDims := w.layoutComposerFooter(gtx, status)
		footerCall := footerMacro.Stop()
		footer := func(gtx layout.Context) layout.Dimensions {
			footerCall.Add(gtx.Ops)
			return footerDims
		}
		footerReserve := footerDims.Size.Y + gtx.Dp(unit.Dp(6))

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
				return w.layoutFailedSends(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutReplyPreview(gtx)
			}),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.messageInputCard(gtx, recipient, maxInputHeight, footerReserve)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(footer),
		)
	})
}

const composerFooterStackMaxDp = 360

func (w *Window) layoutComposerFooter(gtx layout.Context, status service.NodeStatus) layout.Dimensions {
	statusRow := func(gtx layout.Context) layout.Dimensions {
		return w.layoutNetworkStatus(gtx, status)
	}

	// Android used to get the network bar alone — the console was a second
	// app.Window there, which Android has no way to show. As a modal it is
	// reachable on every platform, so the button is laid out unconditionally.
	consoleButton := func(gtx layout.Context) layout.Dimensions {
		return layout.E.Layout(gtx, w.layoutConsoleButton)
	}
	if gtx.Constraints.Max.X < gtx.Dp(unit.Dp(composerFooterStackMaxDp)) {
		return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(statusRow),
			layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
			layout.Rigid(consoleButton),
		)
	}

	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Flexed(1, statusRow),
		layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
		layout.Rigid(consoleButton),
	)
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
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
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
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					semantic.DescriptionOp(w.t("compose.network_security")).Add(gtx.Ops)
					return ui.Icon(gtx, w.shieldIcon, unit.Dp(24), fg)
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

func layoutComposerEditorContent(gtx layout.Context, lines int, content layout.Widget) layout.Dimensions {
	if lines <= 1 {
		return layoutVerticallyCentered(gtx, content)
	}
	return content(gtx)
}

const composerEditorLineHeight = unit.Sp(21)

func composerEditorStyle(theme *material.Theme, editor *widget.Editor, hint string) material.EditorStyle {
	style := material.Editor(theme, editor, hint)
	style.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	style.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
	style.TextSize = unit.Sp(15)
	style.LineHeight = composerEditorLineHeight
	// Gio otherwise applies its default 1.2 multiplier. The editor-height and
	// scrollbar arithmetic below deliberately budgets this exact fixed step.
	style.LineHeightScale = 1
	return style
}

// composerEditorMetrics sizes the editor in WHOLE lines. The growth steps are
// whole lines by construction, but the cap is not: it arrives as a third of
// the window minus the composer's chrome, and a 480dp window leaves 58px where
// two lines cost 42 — the remaining 16px draw the top slice of a third line
// that can never be read. Every window height produced one: 6px at 640dp,
// 12px at 720dp, 6px at 1080dp. So the cap is floored to the line step here,
// where visibleLines is computed from it, and the two cannot disagree about
// what fits.
//
// The floor never eats the last line: baseEditorHeight is the caller's own
// two-line minimum, so a cap below it was already being raised to it.
func composerEditorMetrics(totalLines, maxEditorHeight, baseEditorHeight, lineStep int) (editorHeight, visibleLines int, showScrollbar bool) {
	if lineStep <= 0 {
		lineStep = 1
	}
	maxEditorHeight = max(baseEditorHeight, maxEditorHeight/lineStep*lineStep)
	extraLines := max(0, totalLines-2)
	editorHeight = min(baseEditorHeight+extraLines*lineStep, maxEditorHeight)
	visibleLines = max(1, editorHeight/lineStep)
	return editorHeight, visibleLines, totalLines > visibleLines
}

// composerPickerHeight is the height the emoji picker may take under the
// editor, or 0 when what is left over cannot hold minHeight — the picker's own
// chrome plus one row of cells. Anything between the two would draw a clipped
// strip with no reachable cell in it, so the caller defers the picker rather
// than shrinking it past the point of being usable.
func composerPickerHeight(availableHeight, chromeHeight, editorHeight, footerReserve, minHeight, desiredHeight int) int {
	available := availableHeight - chromeHeight - editorHeight - footerReserve
	if available < minHeight {
		return 0
	}
	return min(desiredHeight, available)
}

func composerSendActionState(hasRecipient, deletePending, hasContent bool) (enabled bool, reasonKey string) {
	switch {
	case !hasRecipient:
		return false, "compose.select_first"
	case deletePending:
		return false, "compose.send_blocked_during_wipe"
	default:
		return hasContent, ""
	}
}

func (w *Window) messageInputCard(gtx layout.Context, recipient domain.PeerIdentity, maxInputHeight, footerReserve int) layout.Dimensions {
	borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
	backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
	editorBg := backgroundColor
	scrollTrack := color.NRGBA{R: 38, G: 46, B: 58, A: 255}
	scrollThumb := color.NRGBA{R: 112, G: 132, B: 164, A: 255}
	lineStep := gtx.Sp(composerEditorLineHeight)
	baseEditorHeight := 2 * lineStep
	chromeHeight := gtx.Dp(unit.Dp(26))
	if w.attachedFile != "" {
		chromeHeight += gtx.Dp(unit.Dp(40))
	}

	line, _ := w.messageEditor.CaretPos()
	totalLines := max(line+1, strings.Count(w.messageEditor.Text(), "\n")+1)
	maxEditorHeight := maxInputHeight - chromeHeight
	editorHeight, visibleLines, showScrollbar := composerEditorMetrics(totalLines, maxEditorHeight, baseEditorHeight, lineStep)
	pickerHeight := 0
	if w.emojiPicker.visible {
		pickerHeight = w.emojiPickerRoom(gtx, chromeHeight, editorHeight, footerReserve)
	}
	cardHeight := chromeHeight + editorHeight + pickerHeight

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.Y = cardHeight
		gtx.Constraints.Max.Y = cardHeight
		ui.Fill(gtx, borderColor)

		return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			ui.Fill(gtx, backgroundColor)

			return layout.Inset{Top: unit.Dp(2), Bottom: unit.Dp(1), Left: unit.Dp(8), Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{
					Axis: layout.Vertical,
				}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						if recipient.IsZero() {
							label := material.Body2(w.theme, w.t("compose.body"))
							label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
							return label.Layout(gtx)
						}
						// cardHeight above budgets a FIXED chrome height,
						// which assumes this header stays on one line. If
						// it wraps, the editor row is pushed below the
						// painted card rectangle (visible on narrow phone
						// widths). Keep the single-line invariant: the name
						// is Flexed and truncated instead of wrapping, and
						// the decorative ID chunk is dropped when the row
						// is too narrow to plausibly hold it.
						showID := gtx.Constraints.Max.X >= gtx.Dp(unit.Dp(420))
						children := []layout.FlexChild{
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								label := material.Body2(w.theme, w.t("compose.body_for"))
								label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
								label.MaxLines = 1
								return label.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
							layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
								name := w.peerDisplayName(recipient)
								lbl := material.Body1(w.theme, name)
								lbl.Font.Weight = font.Bold
								lbl.TextSize = unit.Sp(17)
								lbl.Color = color.NRGBA{R: 150, G: 210, B: 255, A: 255}
								lbl.MaxLines = 1
								return lbl.Layout(gtx)
							}),
						}
						if showID {
							children = append(children,
								layout.Rigid(layout.Spacer{Width: unit.Dp(10)}.Layout),
								layout.Rigid(func(gtx layout.Context) layout.Dimensions {
									lbl := material.Caption(w.theme, w.t("compose.identity_label"))
									lbl.Color = color.NRGBA{R: 160, G: 170, B: 190, A: 255}
									lbl.MaxLines = 1
									return lbl.Layout(gtx)
								}),
								layout.Rigid(layout.Spacer{Width: unit.Dp(4)}.Layout),
								layout.Rigid(func(gtx layout.Context) layout.Dimensions {
									lbl := material.Body1(w.theme, shortFingerprint(recipient.String()))
									lbl.Font.Weight = font.Bold
									lbl.TextSize = unit.Sp(15)
									lbl.Color = color.NRGBA{R: 130, G: 235, B: 190, A: 255}
									lbl.MaxLines = 1
									return lbl.Layout(gtx)
								}),
							)
						}
						return layout.Flex{
							Axis:      layout.Horizontal,
							Alignment: layout.Baseline,
						}.Layout(gtx, children...)
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
								btn := material.IconButton(w.theme, &w.attachButton, w.attachIcon, w.t("file.attach"))
								btn.Background = backgroundColor
								btn.Color = color.NRGBA{R: 157, G: 176, B: 201, A: 255}
								btn.Size = unit.Dp(22)
								btn.Inset = layout.UniformInset(unit.Dp(6))
								return btn.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
							layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
								w.messageEditor.SingleLine = false
								w.messageEditor.Submit = false
								editor := composerEditorStyle(w.theme, &w.messageEditor, w.t("compose.placeholder"))

								radius := gtx.Dp(unit.Dp(12))
								defer clip.UniformRRect(image.Rectangle{Max: image.Pt(gtx.Constraints.Max.X, editorHeight)}, radius).Push(gtx.Ops).Pop()
								return layout.Stack{}.Layout(gtx,
									layout.Expanded(func(gtx layout.Context) layout.Dimensions {
										gtx.Constraints.Min.Y = editorHeight
										gtx.Constraints.Max.Y = editorHeight
										ui.Fill(gtx, editorBg)
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
													return editorTouchKeyboardArea(gtx, &w.touchKbdTags[0], &w.touchKbd, func(gtx layout.Context) layout.Dimensions {
														dims := layoutComposerEditorContent(gtx, totalLines, editor.Layout)
														if w.emojiPicker.takeSoftKeyboardSuppression(gtx.Enabled()) {
															// Editor.Layout may emit Show:true for the FocusEvent
															// caused by opening the picker. Close wins once, on that
															// enabled layout; later taps in the editor can show the
															// keyboard normally while the picker remains open.
															gtx.Execute(key.SoftKeyboardCmd{Show: false})
														}
														return dims
													})
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
							layout.Rigid(layout.Spacer{Width: unit.Dp(4)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								description := w.t("emoji.open")
								background := backgroundColor
								iconColor := color.NRGBA{R: 157, G: 176, B: 201, A: 255}
								if w.emojiPicker.visible {
									description = w.t("emoji.close")
									background = color.NRGBA{R: 34, G: 83, B: 151, A: 255}
									iconColor = color.NRGBA{R: 225, G: 240, B: 255, A: 255}
								}
								btn := material.IconButton(w.theme, &w.emojiPicker.toggleButton, w.emojiIcon, description)
								btn.Background = background
								btn.Color = iconColor
								btn.Size = unit.Dp(22)
								btn.Inset = layout.UniformInset(unit.Dp(6))
								return btn.Layout(gtx)
							}),
							layout.Rigid(layout.Spacer{Width: unit.Dp(4)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								pending := !recipient.IsZero() && w.router.IsConversationDeletePending(recipient)
								hasContent := strings.TrimSpace(w.messageEditor.Text()) != "" || w.attachedFile != ""
								enabled, reasonKey := composerSendActionState(!recipient.IsZero(), pending, hasContent)
								description := w.t("compose.send")
								if reasonKey != "" {
									description = w.t(reasonKey)
								}
								return w.layoutComposerSendButton(gtx, enabled, description, reasonKey != "")
							}),
						)
					}),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						if !w.emojiPicker.visible || pickerHeight <= 0 {
							return layout.Dimensions{}
						}
						return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							height := max(0, pickerHeight-gtx.Dp(unit.Dp(6)))
							gtx.Constraints.Min.Y = height
							gtx.Constraints.Max.Y = height
							return w.layoutEmojiPicker(gtx)
						})
					}),
				)
			})
		})
	})
}

func (w *Window) layoutComposerSendButton(gtx layout.Context, enabled bool, description string, showReason bool) layout.Dimensions {
	background := color.NRGBA{R: 31, G: 91, B: 176, A: 255}
	foreground := color.NRGBA{R: 245, G: 249, B: 255, A: 255}
	if !enabled {
		gtx = gtx.Disabled()
		foreground = color.NRGBA{R: 190, G: 201, B: 216, A: 255}
	}
	if !showReason {
		button := material.IconButton(w.theme, &w.sendButton, w.sendIcon, description)
		button.Background = background
		button.Color = foreground
		button.Size = unit.Dp(22)
		button.Inset = layout.UniformInset(unit.Dp(7))
		return button.Layout(gtx)
	}

	gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(200)))
	button := material.ButtonLayout(w.theme, &w.sendButton)
	button.Background = background
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.DescriptionOp(description).Add(gtx.Ops)
		return layout.Inset{Top: unit.Dp(7), Bottom: unit.Dp(7), Left: unit.Dp(9), Right: unit.Dp(9)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Label(w.theme, unit.Sp(11), description)
					label.Color = foreground
					label.MaxLines = 1
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return ui.Icon(gtx, w.sendIcon, unit.Dp(18), foreground)
				}),
			)
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

		// Image attachments open in the in-app viewer. What the card offers
		// as the way in depends on what it has: the preview when the
		// thumbnail is decoded, and the name row otherwise — a picture whose
		// thumbnail failed, or one still downloading, is exactly what the
		// viewer's fallback and loading states are for, and a card with no
		// click target at all is what made them unreachable.
		var nameRowButton *widget.Clickable
		if isImageContentType(payload.ContentType) {
			filePath := w.router.FileBridge().FilePath(fileID, isMine)
			// receiverDownloadActive is the "it is on its way" half: the file
			// is not on disk yet, and the viewer says so rather than
			// pretending the attachment is not an image.
			if filePath != "" || receiverDownloadActive {
				openItem := viewerItem{
					messageID: domain.MessageID(message.ID),
					peer:      conversationPeer(message, w.snap.MyAddress),
					path:      filePath,
					name:      payload.FileName,
					size:      payload.FileSize,
					mine:      isMine,
				}

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
					w.openImageViewer(openItem, gtx.Now)
				}

				// get() returns non-nil only when the image is decoded and
				// ready (thumbReady). While decoding is in progress or if it
				// failed, nil is returned and the name row carries the click
				// instead — one Clickable, two possible hosts, only ever one
				// of them in the frame.
				entry := w.thumbCache.get(filePath, w.window)
				if entry == nil {
					nameRowButton = thumbBtn
				} else {
					imgOp := entry.op
					imgBounds := entry.bounds
					renderThumb := func(gtx layout.Context) layout.Dimensions {
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
					}

					children = append(children,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							// Clickable on every platform now. The Android
							// exception here was openFile's: it is a stub
							// there (no FileProvider — see open_android.go),
							// so the tap target would have been dead. The
							// viewer this opens is drawn by the application
							// itself.
							return thumbBtn.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								// Hand cursor on hover so the click
								// affordance is discoverable — same UX
								// the file-tab thumbnail and the donate
								// link already provide. CursorPointer
								// must be added INSIDE the clickable's
								// layout callback so the cursor area
								// matches the clickable's hit area.
								pointer.CursorPointer.Add(gtx.Ops)
								return renderThumb(gtx)
							})
						}),
						layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
					)
				}
			}
		}

		// File icon + name.
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				row := func(gtx layout.Context) layout.Dimensions {
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
				}
				if nameRowButton == nil {
					return row(gtx)
				}
				return nameRowButton.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					pointer.CursorPointer.Add(gtx.Ops)
					return row(gtx)
				})
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
					return w.layoutFileActionButtons(gtx, msgCopy, isMine, revealPath, payload.FileName)
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
		if !w.beginUIOp() {
			continue
		}
		go func() {
			defer w.endUIOp()
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
		if !w.beginUIOp() {
			continue
		}
		go func() {
			defer w.endUIOp()
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
		if !w.beginUIOp() {
			continue
		}
		go func() {
			defer w.endUIOp()
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
// Delete follows messageDeleteRoute: only a card the peer has
// confirmed while the peer is unreachable has nothing left to do,
// and it renders as a static neutral-gray "disabled" pill (no
// Clickable, no hover ripple) — see layoutFileCardDeleteButton +
// layoutFileCardDeleteDisabled.
func (w *Window) layoutFileActionButtons(gtx layout.Context, msg service.DirectMessage, isMine bool, filePath, exportName string) layout.Dimensions {
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
		if runtime.GOOS == "android" {
			// The reveal slot doubles as the SAF export button on
			// Android — see the layout branch below.
			w.exportReceivedFile(revealPath, exportName)
		} else {
			go revealFileInDir(revealPath)
		}
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
	//
	// Android has neither a file-manager "reveal" nor an external-open
	// path for app-private files (gogio ships no FileProvider — see
	// open_android.go). Both buttons are replaced there by a single
	// [Save as…] button that exports the file through the system
	// document picker (SAF), reusing the reveal Clickable slot.
	var children []layout.FlexChild
	if runtime.GOOS == "android" {
		children = []layout.FlexChild{
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				matBtn := material.Button(w.theme, revealBtn, w.t("file.export"))
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
	} else {
		children = []layout.FlexChild{
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
	}
	if isMine {
		if len(children) > 0 {
			children = append(children,
				layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
			)
		}
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.layoutFileCardDeleteButton(gtx, msg)
			}),
		)
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
}

// layoutFileCardDeleteButton renders the per-row Delete button for
// a chat-thread file card. Enablement follows messageDeleteRoute,
// the same classification the context-menu item and the router
// use: an unavailable route renders as a static gray box without
// a Clickable, so there is no hover ripple, no hit area and no
// click ripple — visually unmistakably inert.
//
// Direction comes from the row itself, so a future caller that
// renders the button on inbound cards too (layoutFileActionButtons
// only draws it for outgoing rows today) gets the right rule for
// free.
func (w *Window) layoutFileCardDeleteButton(gtx layout.Context, msg service.DirectMessage) layout.Dimensions {
	peer := conversationPeer(msg, w.router.MyAddress())

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

// conversationPeer is the OTHER party of a message, relative to this node:
// the recipient of one we sent, the sender of one we received.
//
// Every action that reaches across a message — deleting it, deleting the
// image it carries — is addressed to that peer, and each of the three places
// that needed it had worked it out again from the same two fields.
func conversationPeer(message service.DirectMessage, me domain.PeerIdentity) domain.PeerIdentity {
	if message.Sender != me {
		return message.Sender
	}
	return message.Recipient
}

// dispatchMessageDeleteAsync runs SendMessageDelete on a background
// goroutine with the standard 10s timeout and surfaces success /
// failure on the router status line. Shared between the chat
// context-menu Delete handler and the per-row Delete button on a
// file card.
//
// The local copy is gone by the time SendMessageDelete returns, so the
// caption only ever describes what is still owed to the peer: a route
// that owes nothing has already published its terminal outcome and must
// not be overwritten, a reachable peer is being asked right now, and an
// unreachable one will be asked when they come back. The route comes
// from the router rather than being guessed here a second time.
func (w *Window) dispatchMessageDeleteAsync(peer domain.PeerIdentity, target domain.MessageID) {
	w.router.SetSendStatus(w.t("status.message_deleting"))
	if !w.beginUIOp() {
		return
	}
	go func() {
		defer w.endUIOp()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		route, err := w.router.SendMessageDelete(ctx, peer, target)
		if err != nil {
			w.router.SetSendStatus(w.t("status.message_delete_failed", err.Error()))
			return
		}
		caption := w.messageDeleteStatusFor(route, w.peerOnline(peer))
		if caption == "" {
			return
		}
		// Only replace our own "Deleting…" caption: a fast peer ack can
		// publish the terminal outcome before this goroutine resumes,
		// and overwriting that with a progress line would walk the
		// status backwards.
		w.router.SetSendStatusIfCurrent(w.t("status.message_deleting"), caption)
	}()
}

// messageDeleteStatusFor picks the caption for a delete that just
// returned. An empty string means "leave the status line alone": the
// route owes the peer nothing and handleMessageDeleteOutcome has
// already written the terminal status.
func (w *Window) messageDeleteStatusFor(route domain.MessageDeleteRoute, peerReachable bool) string {
	switch {
	case !route.SchedulesPeerDeletion():
		return ""
	case peerReachable:
		return w.t("status.message_delete_dispatched")
	default:
		return w.t("status.message_delete_scheduled")
	}
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
		if id != me && !id.IsZero() {
			recipients = append(recipients, id)
		}
	}
	sort.Slice(recipients, func(i, j int) bool { return recipients[i].Compare(recipients[j]) < 0 })
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
// events correctly. It also tracks touch long-press state, which opens
// the same context menu on touch screens (see touch_input.go).
type rightClickState struct {
	pressed bool

	touchDown      bool
	longPressFired bool
	matured        bool       // hold reached longPressDuration by EVENT time on Release (frame may be delayed)
	pressID        pointer.ID // pointer that armed the secondary-button press
	touchID        pointer.ID
	touchStart     time.Time     // frame time at press (ongoing frame-time maturation)
	pressTime      time.Duration // EVENT time at press (release-time maturation, robust to a delayed frame)
	touchPos       f32.Point
	pressCursor    image.Point // window-level cursor at press time (menu anchor)
}

// cachedMsg holds pre-indexed message metadata for O(1) reply quote rendering.
type cachedMsg struct {
	Body      string
	Sender    domain.PeerIdentity
	Timestamp time.Time
	Index     int // position in ActiveMessages slice for scroll-to
	// IsImageFile marks a file_announce DM with an image content type.
	// Precomputed here so reply quotes never parse the announce JSON on
	// the frame path — the payload is immutable for a given message ID,
	// which is exactly the granularity of this cache.
	IsImageFile bool
}

// menuAnchorForClick returns the screen anchor for a menu opened by a ⋯
// button. widget.Clickable fires for a POINTER click (which records a press in
// History, ended this frame) and for a KEYBOARD/accessibility activation
// (which records none). For a pointer click the window-level cursor is correct;
// for a non-pointer one it is stale, so the button's own window rectangle —
// captured opportunistically during layout (recordMenuButtonRect) — is the
// anchor, falling back to a fixed visible spot only for a button that has
// never been pointer-touched.
// menuRectSig captures the layout state that governs where the "⋯" buttons
// sit. When any of it changes the buttons have moved, so cached menuBtnRects
// are stale.
//
// Scroll offset and window size are NOT enough, which is the trap this struct
// used to fall into. A message arriving, a message being deleted, or a card
// above growing (a quote resolving, a longer body replacing a shorter one)
// moves every button below it while First, Offset and the window size all stay
// exactly as they were — and the next keyboard/Narrator activation would then
// open the menu at coordinates the frame no longer has. So the signature also
// carries, per list, the WHOLE layout.Position and the item count it was laid
// out with:
//
//   - Position.Length is Gio's total content size (laidOutTotalLength*len/
//     numLaidOut + gaps), so it moves when a visible card's height moves.
//   - Position.Count / OffsetLast move when the viewport itself shrinks — a
//     growing multi-line composer pushes the chat list up without touching
//     rootSize or the scroll offset.
//   - the item count is the exact answer for add/remove, which Length only
//     answers by estimate; an estimate that can in principle round back onto
//     its old value is not something a cache invariant should rest on.
//
// Geometry plus counts still miss REORDERING. sortSidebarPeers re-ranks the
// contacts list whenever a peer goes online or offline, gains or loses unread,
// or its preview timestamp moves; with rows of equal height that permutation
// leaves Position, the count and rootSize all byte-identical while every "⋯"
// button changes row. menuBtnRects is keyed by button IDENTITY, so the same
// widget.Clickable now sits somewhere else and its cached rectangle points at
// whatever moved into the old spot — a mis-placed menu, not a missing one.
// Chat has its own version: an optimistic local message replaced by the
// confirmed one carries a new ID and a corrected timestamp, so the count can
// stay put while the order does not.
//
// Hence the two order digests, FNV-1a over the row identities IN ORDER — peer
// identities for contacts, message IDs for chat. A permutation changes them; so
// does a swap that leaves the count alone. They are not a substitute for the
// counts, which stay: the count is an exact, collision-free answer for the
// add/remove case, and a 64-bit digest is only overwhelmingly likely to be.
//
// The identity-search hits get the same pair, and they need it more than either
// list does, because NOTHING else here can see them move. They are not a list —
// up to identitySearchMaxRows Rigid rows between the search box and the
// contacts list — so no layout.Position describes them, the contacts count and
// digest below them do not change when they do, and the query that selects them
// appears nowhere in this struct. Editing it re-runs searchKnownIdentities,
// whose output is sorted by identity: a hit changes row, or the whole set turns
// over, while the contacts underneath are untouched and the card can come out
// exactly as tall. And these rows are contacts rows — layoutRecipientButton —
// so their ⋯ IS the widget.Clickable that peer would use anywhere else, cached
// under that same key. A rectangle left over from the previous query therefore
// does not merely go unused: it opens a real menu beside the row that has since
// moved into the old place.
//
// Everything named so far describes CONTENT and VIEWPORT, and both are blind
// to a pure translation: a card that slides bodily up or down while holding
// the same rows in the same order changes not one of them. Two things do that
// on a tablet. The touch keyboard makes keyboardYieldingChrome drop the window
// header, and everything under it rises by the header's height. A language
// change re-wraps the labels the contacts card carries above its search box —
// its title, "you", "known: N", the copy-identity button — and the rows below
// them move by the difference.
//
// Where a list is actually laid out this is caught anyway, though by accident
// rather than by design: the Flexed list under those rows absorbs the slack,
// so a translation of the card is also a change of the list's viewport, and
// layout.Position carries OffsetLast (= viewport - laid-out size) and Count.
// The accident runs out exactly where it matters. With no contacts the card
// takes its empty-state early return and lays out no list at all, so
// contactsList.Position is frozen at whatever the last frame that DID lay it
// out left — and that is precisely the state in which the identity-search hits
// are the only rows on screen carrying a ⋯ button. The two lists have no such
// hole, because for them "not laid out" implies a recorded count of zero,
// which has already dropped their rectangles.
//
// Hence searchAvail, the one positional term: gtx.Constraints.Max.Y as handed
// to the Rigid child that lays the hits out, which Gio defines as the space
// remaining beneath that child's top edge. Measured, not enumerated — it
// answers "did anything above these rows change height", whatever that
// something was, rather than listing the causes known today. The floor it is
// measured from is the bottom of the contacts card, which moves only with the
// window size (already here) and with the keyboard inset; the latter changes
// only on the same transitions that move the rows anyway, and a clear too many
// is the safe direction. It also pins the contacts list below: that list's top
// is this edge plus a block whose height is a function of searchItems.
//
// Deliberately NOT snapshot generation, which would be a single compare and
// exact. RouterSnapshot.Generation bumps on every published snapshot including
// the 1s resource sample and the 500ms peer-health delta, so the cache would be
// cleared two or three times a second while nothing moved — and unlike a scroll
// or a resize, a spurious clear here is NOT free. recordMenuButtonRect captures
// only on the frame a press BEGINS on that button (that is the one frame where
// the origin is derivable); nothing re-measures a button that is merely on
// screen. Clearing on a timer would therefore leave almost every
// keyboard/Narrator activation falling back to the fixed anchor, which is
// deleting the feature rather than correcting it. RouterSnapshot.DMGeneration
// (the message cache's gate) is rejected for the same reason and not merely
// the same argument: it still advances on every message sent or received and
// every sidebar refresh, none of which has to move a single ⋯ button, and one
// spurious clear costs a whole activation. The digests change when the
// rows change and at no other time.
//
// The residual is a sub-pixel-scale height change that survives Length's
// integer division. It is left uncovered on purpose: it can move a button by
// about a pixel, which is not a mis-placed menu, and the only exact alternative
// is re-measuring every button every frame.
type menuRectSig struct {
	chat, contacts           layout.Position
	chatItems, contactsItems int
	chatOrder, contactsOrder uint64
	rootW, rootH             int

	// The identity-search hits, which have no list and therefore no Position:
	// which rows, and where they sit.
	searchItems int
	searchOrder uint64
	searchAvail int
}

func (w *Window) currentMenuRectSig() menuRectSig {
	return menuRectSig{
		chat:          w.chatList.Position,
		contacts:      w.contactsList.Position,
		chatItems:     w.chatItems,
		contactsItems: w.contactsItems,
		chatOrder:     w.chatOrder,
		contactsOrder: w.contactsOrder,
		rootW:         w.rootSize.X,
		rootH:         w.rootSize.Y,
		searchItems:   w.searchItems,
		searchOrder:   w.searchOrder,
		searchAvail:   w.searchAvail,
	}
}

// FNV-1a 64-bit parameters. Chosen over maphash/crc for having no state, no
// allocation and no import: these run inside layout.
const (
	menuDigestOffset uint64 = 14695981039346656037
	menuDigestPrime  uint64 = 1099511628211
)

// peerOrderDigest fingerprints the sidebar rows in the order they will be laid
// out. PeerIdentity is a fixed 20 bytes, so concatenation is unambiguous and no
// separator is needed. Recomputed every frame rather than cached against a
// snapshot generation: the sidebar is bounded by the contact count, and the
// caller already merges and SORTS this very slice on every frame
// (snapRecipients), so a linear pass over it adds no term the frame did not
// already pay — and hashing the slice HANDED TO THE CARD, instead of
// re-deriving it from the snapshot, is what guarantees the digest describes the
// order actually laid out. A conversation has no such bound, which is why the
// chat digest rides rebuildMsgCache instead.
func peerOrderDigest(peers []domain.PeerIdentity) uint64 {
	h := menuDigestOffset
	for i := range peers {
		for _, b := range peers[i] {
			h ^= uint64(b)
			h *= menuDigestPrime
		}
	}
	return h
}

// setMenuListItems records the row count a list is about to be laid out with
// and re-checks the rect cache immediately. Called from the top of the owning
// card, not from the List call site.
//
// The timing is the whole point, and getting it wrong was a real defect. For the
// GEOMETRY half of the signature the call in layout() can only ever compare the
// previous frame against the one before it, because a list's layout.Position
// does not exist until the list has laid out; for scroll and resize that
// one-frame lag is unavoidable. For CONTENT it is not: the row count is an
// INPUT, known before a single row lays out — and a keyboard/Narrator activation
// is resolved through menuAnchorForClick while those rows lay out, so recording
// the count ahead of them closes the window entirely instead of leaving the
// first frame after an insert or delete reading coordinates the new content has
// already moved. The two order digests are inputs on the same terms: the
// contacts one is written here, beside the count, from the very slice the card
// is about to lay out, and the chat one is already current by the time layout()
// takes the signature, since rebuildMsgCache recomputes it immediately after the
// snapshot is adopted.
//
// Hence the card and not the List: a card can return early on paths that lay
// out no list at all, and those returns are never far enough down to reach the
// List. Call it on them too, with zero. "The list is not there this frame"
// moves the buttons exactly as surely as reordering it, and an early return
// that simply skips the assignment leaves the stale count in place — which is
// how a rect could survive the last contact being deleted and still be handed
// out after one was added back.
//
// The identity-search hits go through here too (resolveIdentitySearchRows)
// although they are not a list. What the name is about is the discipline, not
// the widget: a set of rows that carries ⋯ buttons, counted and fingerprinted
// before the first of them lays out.
//
// One accepted cost: the signature is global, so the contacts card changing
// count also clears any chat rect, and the chat card (laid out second) can
// clear contacts rects captured earlier in the SAME frame. Both directions only
// ever discard, never fabricate, and a missing rect falls back to the anchor —
// so the loss is conservatism, not correctness, and it is not worth a per-list
// cache to recover.
func (w *Window) setMenuListItems(dst *int, n int) {
	*dst = n
	w.invalidateStaleMenuRects()
}

// invalidateStaleMenuRects drops all cached button rectangles when the layout
// state that positioned them has changed since capture — a list scrolled, its
// CONTENT changed or was REORDERED, or the window resized. Idempotent (it
// compares before it clears), so it is safe to call several times per frame, and
// it is: once in layout() just after the new snapshot lands, again from
// setMenuListItems at the head of each card that owns a list, and once from
// recordSearchRowAnchor as the identity-search hits are about to lay out.
//
// The call sites cover different things. Content — the per-list counts and
// the two order digests — is caught within the frame that changes it: the
// layout() call takes the chat digest the moment rebuildMsgCache derives it from
// the new snapshot, and setMenuListItems adds the counts and the contacts digest
// before the first row of either list lays out, which is before any
// keyboard/Narrator activation can be resolved through menuAnchorForClick. So is
// searchAvail, and for a reason worth naming: it is a constraint handed DOWN to
// the rows, an input known before they lay out, not a measurement read back
// after — which is why the one positional term in the signature is the one term
// that does not lag. Scroll offset and viewport can only be caught on the NEXT
// frame, since they are outputs of the layout being checked; the exposure is
// therefore the remainder of the frame in which the list actually moved, and
// only for a non-pointer activation dispatched during it. Closing that too would
// mean re-measuring every button every frame.
//
// The lag also errs the other way — a rect captured on a frame that itself
// moved the list is dropped on the next one even though it was correct at
// capture. That direction is harmless: the cache is a convenience for
// non-pointer activations and its absence falls back to a deterministic
// on-screen anchor, whereas a surviving stale rect silently mis-places every
// later keyboard/Narrator menu.
func (w *Window) invalidateStaleMenuRects() {
	sig := w.currentMenuRectSig()
	if !w.menuRectSigSet || sig != w.menuLayoutSig {
		if len(w.menuBtnRects) > 0 {
			clear(w.menuBtnRects)
		}
		w.menuLayoutSig = sig
		w.menuRectSigSet = true
	}
}

// pointerClickedThisFrame reports whether btn was activated by a POINTER
// release this frame. widget.Clickable.Clicked also fires for a Return/Space
// or accessibility (Narrator) activation, which records NO press in History —
// so a caller that raises the touch keyboard on a click must gate on this,
// otherwise a hardware Return within touchInputRecency of an unrelated touch
// (lastInputTouch is not cleared by key events) would wrongly pop the keyboard
// while the user types on real keys. Same package, so consoleModal uses it too.
func pointerClickedThisFrame(btn *widget.Clickable, gtx layout.Context) bool {
	h := btn.History()
	return len(h) > 0 && h[len(h)-1].End.Equal(gtx.Now)
}

// pressWindowPos resolves the WINDOW-space position of the pointer press that
// widget.Clickable recorded as p. It exists because Gio v0.10's widget.Press
// carries no PointerID: the button knows WHERE it was pressed in its OWN
// coordinates and WHEN, but not by whom. The frame is the correlation — Gio
// stamps Press.Start with the gtx.Now of the frame the press began, the same
// value the root cursor tracker stamps into pointerPressPos — and the map
// holds each pointer's own press point rather than a single global one.
//
// Reports false unless the match is UNIQUE. Two pointers pressing on one frame
// leaves nothing here to tell them apart, and guessing is precisely the bug
// this replaces: the menu opening under the wrong finger. Callers fall back to
// what they did before, which is no worse in that case and correct in every
// other. (The map still holds a pointer released THIS frame — entries are
// dropped one frame later — so a click's press is always resolvable, however
// many frames the press was held.)
func (w *Window) pressWindowPos(p widget.Press) (image.Point, bool) {
	var pos image.Point
	n := 0
	for _, pp := range w.pointerPressPos {
		if pp.at.Equal(p.Start) {
			pos, n = pp.pos, n+1
		}
	}
	return pos, n == 1
}

func (w *Window) menuAnchorForClick(btn *widget.Clickable, gtx layout.Context) image.Point {
	if pointerClickedThisFrame(btn, gtx) {
		// Anchor at THIS click's own press point. lastCursorPos is the last
		// pointer event of the frame from ANY source, so a second finger, a
		// mouse or a pen moving after the tap on "⋯" would drop the menu under
		// it — the same defect the long-press and right-click paths already fix
		// by going through pressAnchor. Those have a pointer.Event and can look
		// up by PointerID; a Clickable has only its press record, so resolve by
		// the frame the press began on instead.
		h := btn.History()
		if pos, ok := w.pressWindowPos(h[len(h)-1]); ok {
			return pos
		}
		return w.lastCursorPos // two pointers pressed that frame: nothing better exists
	}
	// Keyboard/accessibility: no fresh cursor. Anchor at the button's own
	// window rectangle if we captured it during a prior pointer interaction
	// (see menuDotsButton) — drop the menu just below the button's lower-left,
	// exactly where a real context menu appears. placeMenu flips/clamps it if
	// there is no room below.
	if r, ok := w.menuBtnRects[btn]; ok {
		return image.Pt(r.Min.X, r.Max.Y)
	}
	// Never pointer-touched (pure keyboard/Narrator navigation): fall back to
	// a deterministic, always-visible spot (placeMenu clamps it on-screen).
	// Use the ROOT window size — gtx here is a list item's nested context whose
	// Max.Y is near-unbounded, so its quarter would be off-screen.
	return image.Pt(w.rootSize.X/4, w.rootSize.Y/4)
}

// recordMenuButtonRect stores btn's window-space rectangle when THIS frame
// carries a pointer press on it: the press appears in History with local
// coordinates, and pressWindowPos resolves the SAME press to its window
// coordinates (by the frame it began on — not via lastCursorPos, which by then
// may hold a different pointer's later event), so their difference is the
// button's window origin. This is the only readback
// Gio's immediate-mode layout allows — there is no transform inspection — so
// the rectangle is captured opportunistically here and reused by
// menuAnchorForClick for later non-pointer activations.
func (w *Window) recordMenuButtonRect(btn *widget.Clickable, gtx layout.Context, dims layout.Dimensions) {
	h := btn.History()
	if len(h) == 0 {
		return
	}
	p := h[len(h)-1]
	if !p.Start.Equal(gtx.Now) {
		return // capture only on the frame the press BEGAN. The origin is a
		// DIFFERENCE between two coordinates of one instant: p.Position, this
		// press in the button's own space, and pressWindowPos(p), the same
		// press in window space. p.Position is frozen at the press, but the
		// button is re-laid-out every frame — on a later frame (drag, release)
		// a scrolling list has moved it, and the difference would name an
		// origin the button no longer has. Note this is a freshness guard
		// only: the correlation itself is p.Start, which pressWindowPos
		// matches against the tracker's press stamps and which stays valid for
		// as long as the press does.
	}
	win, ok := w.pressWindowPos(p)
	if !ok {
		// Two pointers pressed this frame, so which window point belongs to
		// THIS press is unknowable. Record nothing: a wrong rectangle is worse
		// than none — it would silently mis-place every later keyboard/Narrator
		// menu, while none falls back to the deterministic on-screen anchor and
		// the next unambiguous press captures the real one.
		return
	}
	origin := win.Sub(p.Position)
	w.menuBtnRects[btn] = image.Rectangle{Min: origin, Max: origin.Add(dims.Size)}
}

// menuDotsButton lays out a "⋯" glyph as a menu button. It uses a PLAIN
// Clickable (no material ink/hover), so there is no grey press box, and gives
// the glyph a small symmetric inset for a finger-reachable hit area WITHOUT a
// fixed 40dp square — that square was taller than the header row it sits in and
// pushed the timestamp down and the glyph in from the edge. desc is the
// localized accessibility name (a glyph-only button says nothing to Narrator).
func (w *Window) menuDotsButton(gtx layout.Context, btn *widget.Clickable, fg color.NRGBA, desc string) layout.Dimensions {
	dims := btn.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		// Restore the accessibility role the plain Clickable drops (material's
		// added it): Narrator needs to know this is an interactive Button, not
		// just a labelled glyph. This is a semantic op only — no visuals, no ink.
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(desc).Add(gtx.Ops)
		return layout.Inset{
			Top: unit.Dp(2), Bottom: unit.Dp(2),
			Left: unit.Dp(6), Right: unit.Dp(2),
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			lbl := material.Body1(w.theme, "⋯")
			lbl.Color = fg
			return lbl.Layout(gtx)
		})
	})
	// Capture this button's window rectangle when a pointer press this frame
	// makes it derivable, for later keyboard/Narrator activations.
	w.recordMenuButtonRect(btn, gtx, dims)
	return dims
}

// msgMenuButton returns the per-message "⋯" button — a guaranteed touch
// path to the context menu. Long-press works only while the finger stays
// within Gio's grab threshold (see handleTouchLongPress); the button works
// always.
func (w *Window) msgMenuButton(id string) *widget.Clickable {
	if b, ok := w.msgMenuBtns[id]; ok {
		return b
	}
	b := new(widget.Clickable)
	w.msgMenuBtns[id] = b
	return b
}

// recipientMenuButton mirrors msgMenuButton for contact cards.
func (w *Window) recipientMenuButton(id domain.PeerIdentity) *widget.Clickable {
	if b, ok := w.recipientMenuBtns[id]; ok {
		return b
	}
	b := new(widget.Clickable)
	w.recipientMenuBtns[id] = b
	return b
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

func (w *Window) ensureSelectedRecipient(recipients []domain.PeerIdentity, compact bool) {
	selected := w.snap.ActivePeer

	if len(recipients) == 0 {
		if strings.TrimSpace(selected.String()) != "" {
			w.recipientEditor.SetText(selected.String())
		} else {
			w.recipientEditor.SetText("")
		}
		return
	}

	for _, recipient := range recipients {
		if recipient == selected {
			w.recipientEditor.SetText(recipient.String())
			return
		}
	}

	if strings.TrimSpace(selected.String()) != "" {
		w.recipientEditor.SetText(selected.String())
		return
	}

	// Single-pane layout: an empty selection means "show the contact
	// list" — never auto-open a conversation. Auto-selecting here would
	// make the list unreachable (DeselectPeer would be undone on the
	// next frame) and would auto-mark messages seen for a chat that is
	// not actually on screen.
	if compact {
		return
	}

	// Auto-select first recipient. AutoSelectPeer sends seen receipts
	// the same way SelectPeer does — the chat is on screen.
	w.router.AutoSelectPeer(recipients[0])
	w.recipientEditor.SetText(recipients[0].String())
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

type contactPresenceState uint8

const (
	contactPresenceUnknown contactPresenceState = iota
	contactPresenceOffline
	contactPresenceOnline
)

func contactPresence(status service.NodeStatus, fingerprint domain.PeerIdentity) contactPresenceState {
	if status.ReachableIDs == nil {
		return contactPresenceUnknown
	}
	if status.ReachableIDs[fingerprint] {
		return contactPresenceOnline
	}
	return contactPresenceOffline
}

// layoutContactPresenceAvatar is the contact-list-specific replacement for
// the old 10dp reachability dot. Other status surfaces intentionally keep
// layoutReachableIndicator: this redesign belongs to the user list only.
func (w *Window) layoutContactPresenceAvatar(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity) layout.Dimensions {
	const avatarSize = unit.Dp(38)
	const avatarIconSize = unit.Dp(23)

	state := contactPresence(status, fingerprint)
	background := color.NRGBA{R: 83, G: 101, B: 124, A: 255}
	iconColor := color.NRGBA{R: 246, G: 249, B: 252, A: 255}
	icon := w.personIcon

	switch state {
	case contactPresenceOnline:
		background = color.NRGBA{R: 25, G: 137, B: 65, A: 255}
	case contactPresenceUnknown:
		iconColor = color.NRGBA{R: 174, G: 193, B: 216, A: 255}
		icon = w.personOutlineIcon
	}

	side := gtx.Dp(avatarSize)
	gtx.Constraints.Min = image.Pt(side, side)
	gtx.Constraints.Max = image.Pt(side, side)
	bounds := image.Pt(side, side)
	// Explicit centering prevents the smaller foreground icon from inheriting
	// Stack's north-west zero-value alignment inside the avatar circle.
	return layout.Stack{Alignment: layout.Center}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			if state == contactPresenceUnknown {
				stroke := clip.Stroke{
					Path:  clip.Ellipse{Max: bounds}.Path(gtx.Ops),
					Width: float32(gtx.Dp(unit.Dp(1.5))),
				}.Op().Push(gtx.Ops)
				paint.ColorOp{Color: iconColor}.Add(gtx.Ops)
				paint.PaintOp{}.Add(gtx.Ops)
				stroke.Pop()
				return layout.Dimensions{Size: bounds}
			}
			defer clip.Ellipse{Max: bounds}.Push(gtx.Ops).Pop()
			paint.ColorOp{Color: background}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			return layout.Dimensions{Size: bounds}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			if icon == nil {
				return layout.Dimensions{}
			}
			return ui.Icon(gtx, icon, avatarIconSize, iconColor)
		}),
	)
}

func shouldShowContactLastOnlineLabel(gtx layout.Context, contactRow bool) bool {
	return contactRow && gtx.Constraints.Max.X >= gtx.Dp(unit.Dp(280))
}

// contactLastOnlineAt returns the best identity-scoped activity timestamp the
// desktop currently owns. A live route is represented by the online avatar,
// so it returns no timestamp instead of a moving wall clock. Outgoing
// messages are never peer-presence evidence.
//
// Evidence comes in two classes, and they are ranked rather than compared.
//
// OBSERVATIONS are what this node saw with its own clock: Contact.LastOnlineAt
// (a lost route, or a DM the sender handed us over its own session) and the
// peer-health timestamps. Within the class the newest wins.
//
// MESSAGE-DERIVED evidence is RouterPeerState.LastIncomingAt: the newest
// message this contact wrote, recomputed by the router from the chatlog and
// never persisted. It carries the SENDER's clock — the one party who gains
// from appearing recently online — so it is spent only when no observation
// exists at all.
//
// The trade that ranking makes, stated plainly: freshness is given up for
// provenance. A week-old peer-health stamp beats a message the contact sent
// an hour ago through a relay, so a contact who is only ever reachable
// indirectly can read as older than they are. Forward-dated timestamps are
// already refused on the way in (the store skips rows dated after now, and
// the router's merge refuses them again), so this is not about the lie a
// sender can tell in one message — it is about not letting a value the peer
// chose outrank one this node witnessed.
//
// None of them is the preview. The preview is the last row of the thread,
// which is our own message in every conversation we answered last — the
// ordinary case — and reading presence from it silently loses the contact's
// own message behind our reply.
func contactLastOnlineAt(status service.NodeStatus, state *service.RouterPeerState, fingerprint domain.PeerIdentity, peerHealthLastOnline time.Time) time.Time {
	if status.ReachableIDs != nil && status.ReachableIDs[fingerprint] {
		return time.Time{}
	}
	observed := peerHealthLastOnline
	if contact, ok := status.Contacts[fingerprint.String()]; ok && contact.LastOnlineAt.Valid() {
		if persisted := contact.LastOnlineAt.Time(); persisted.After(observed) {
			observed = persisted
		}
	}
	if !observed.IsZero() {
		return observed
	}

	if state != nil && state.LastIncomingAt.Valid() {
		return state.LastIncomingAt.Time()
	}
	return time.Time{}
}

func (w *Window) rebuildPeerLastOnlineIndex() {
	if w.peerLastOnlineByIdentity == nil {
		w.peerLastOnlineByIdentity = make(map[domain.PeerIdentity]time.Time)
	} else {
		clear(w.peerLastOnlineByIdentity)
	}
	for _, peer := range w.snap.NodeStatus.PeerHealth {
		identity := domain.PeerIdentityFromWire(peer.PeerID)
		if identity.IsZero() {
			continue
		}
		last := w.peerLastOnlineByIdentity[identity]
		last = newerOptionalTime(last, peer.LastDisconnectedAt)
		last = newerOptionalTime(last, peer.LastUsefulReceiveAt)
		last = newerOptionalTime(last, peer.LastPongAt)
		last = newerOptionalTime(last, peer.LastConnectedAt)
		if !last.IsZero() {
			w.peerLastOnlineByIdentity[identity] = last
		}
	}
}

func newerOptionalTime(current time.Time, candidate domain.OptionalTime) time.Time {
	if candidate.Valid() && candidate.Time().After(current) {
		return candidate.Time()
	}
	return current
}

func formatContactLastOnline(now, last time.Time, language string) string {
	if now.IsZero() || last.IsZero() {
		return ""
	}
	last = last.In(now.Location())
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	lastDay := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, now.Location())

	daysAgo := calendarDayDistance(today, lastDay)
	switch {
	case daysAgo <= 0:
		return last.Format("15:04")
	case daysAgo == 1:
		return translate(language, "clients.last_online_yesterday")
	case daysAgo <= 6:
		return translateCount(language, "clients.last_online_days", daysAgo)
	default:
		return last.Format(translate(language, "clients.last_online_date_format"))
	}
}

func calendarDayDistance(later, earlier time.Time) int {
	laterYear, laterMonth, laterDay := later.Date()
	earlierYear, earlierMonth, earlierDay := earlier.Date()
	laterUTC := time.Date(laterYear, laterMonth, laterDay, 0, 0, 0, 0, time.UTC)
	earlierUTC := time.Date(earlierYear, earlierMonth, earlierDay, 0, 0, 0, 0, time.UTC)
	return int(laterUTC.Sub(earlierUTC) / (24 * time.Hour))
}

func (w *Window) contactPresenceDescription(now time.Time, status service.NodeStatus, fingerprint domain.PeerIdentity, includeLastOnline bool) string {
	presence := contactPresence(status, fingerprint)
	if presence == contactPresenceOnline {
		return w.t("clients.presence.online")
	}
	if !includeLastOnline {
		if presence == contactPresenceUnknown {
			return w.t("clients.presence.unknown")
		}
		return w.t("clients.presence.offline")
	}

	last := contactLastOnlineAt(status, w.snap.Peers[fingerprint], fingerprint, w.peerLastOnlineByIdentity[fingerprint])
	formatted := formatContactLastOnline(now, last, w.language)
	if formatted == "" {
		if presence == contactPresenceUnknown {
			return w.t("clients.presence.unknown")
		}
		return w.t("clients.presence.offline")
	}
	if presence == contactPresenceUnknown {
		return w.t("clients.presence.unknown_last_online", formatted)
	}
	return w.t("clients.last_online", formatted)
}

func (w *Window) layoutContactLastOnline(gtx layout.Context, status service.NodeStatus, fingerprint domain.PeerIdentity) layout.Dimensions {
	last := contactLastOnlineAt(status, w.snap.Peers[fingerprint], fingerprint, w.peerLastOnlineByIdentity[fingerprint])
	formatted := formatContactLastOnline(gtx.Now, last, w.language)
	if formatted == "" {
		return layout.Dimensions{}
	}
	label := material.Caption(w.theme, formatted)
	label.Color = color.NRGBA{R: 162, G: 177, B: 198, A: 255}
	label.MaxLines = 1
	return label.Layout(gtx)
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

	// The row count behind menuRectSig is recorded in layoutChatCard, above the
	// early returns that lay out no list — not here, where those paths never
	// reach it.
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
	// Read right-click / touch long-press events from the previous frame.
	rc := w.msgRightClickState(message.ID)
	openMsgMenu := func(pos image.Point) {
		msgCopy := message
		w.msgContextMsg = &msgCopy
		w.msgContextPos = pos
		// Same contract as the identity menu: the menu takes focus, and hands
		// it back to this bubble's "⋯" button when it closes.
		w.msgMenuFocus.open(w.msgMenuButton(message.ID))
	}
	slopPx := float32(gtx.Dp(longPressSlop))
	for {
		ev, ok := gtx.Event(pointer.Filter{
			Target: rc,
			Kinds:  pointer.Press | pointer.Release | pointer.Move | pointer.Drag | pointer.Cancel,
		})
		if !ok {
			break
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		rc.handleTouchLongPress(pe, gtx.Now, slopPx, w.pressAnchor(pe))
		if pe.Kind == pointer.Press && pe.Buttons.Contain(pointer.ButtonSecondary) {
			// Anchor at THIS pointer's press position (see recipient site).
			openMsgMenu(w.pressAnchor(pe))
		}
	}
	w.cancelLongPressOnMultiTouch(rc)
	if rc.longPressTriggered(gtx) {
		openMsgMenu(rc.pressCursor)
	}

	// Record the bubble content first to measure its size.
	macro := op.Record(gtx.Ops)
	dims := w.kit().MessageBubble(gtx, ui.MessageBubble{
		Mine:      isMine,
		Quote:     w.bubbleQuote(message, isMine),
		Header:    w.bubbleHeader(message, author, isMine, openMsgMenu),
		Body:      w.bubbleBody(message, isMine),
		Reactions: w.bubbleReactions(message),
		Status:    w.bubbleStatus(message, isMine),
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

// bubbleQuote is the reply block, or nil when the message answers nothing.
func (w *Window) bubbleQuote(message service.DirectMessage, isMine bool) layout.Widget {
	if message.ReplyTo == "" {
		return nil
	}
	return func(gtx layout.Context) layout.Dimensions {
		return w.layoutReplyQuote(gtx, message.ReplyTo, isMine)
	}
}

// bubbleHeader is the author, the timestamp and the "⋯" button.
//
// The author is Flexed with a single ellipsized line, so a long alias can never
// push the timestamp or the button past the clip; the rigid trailing children
// are measured first and always keep their space, landing the button in the
// bubble's top-right corner.
func (w *Window) bubbleHeader(message service.DirectMessage, author string, isMine bool, openMenu func(image.Point)) layout.Widget {
	return func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				label := material.Caption(w.theme, author)
				label.Color = ui.MessageAuthorColor(isMine)
				label.MaxLines = 1
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Caption(w.theme, message.Timestamp.Local().Format(chatTimestampLayout))
				label.Color = color.NRGBA{R: 160, G: 185, B: 220, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				// "⋯" — always-available touch path to the message menu
				// (long-press is limited by Gio's pointer-grab threshold).
				btn := w.msgMenuButton(message.ID)
				for btn.Clicked(gtx) {
					openMenu(w.menuAnchorForClick(btn, gtx))
				}
				return w.menuDotsButton(gtx, btn, ui.MessageStatusColor(), w.t("context.menu_button_message"))
			}),
		)
	}
}

// bubbleBody is the message text, or the file card for a file announcement.
func (w *Window) bubbleBody(message service.DirectMessage, isMine bool) layout.Widget {
	return func(gtx layout.Context) layout.Dimensions {
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
	}
}

// bubbleReactions is the chip row under the body, and nil while the message
// carries no reactions.
//
// What it draws comes from the conversation cache rather than from the message,
// because a reaction is not a property of a message: it is a fact by an actor,
// merged from what this node decided and what peers stated, and it changes
// without the message changing. See reactions.go and
// docs/refactoring/reactions-protocol.md.
func (w *Window) bubbleReactions(message service.DirectMessage) layout.Widget {
	reactions := w.messageReactions(message)
	if len(reactions) == 0 {
		return nil
	}
	return func(gtx layout.Context) layout.Dimensions {
		chips := w.messageReactionChips(domain.MessageID(message.ID))
		// Drained on the frame they were drawn on, against the set that was
		// drawn: a chip is a button with a button's semantics, so leaving the
		// presses unread would announce an action to a screen reader and then
		// not perform it — and the press would still be queued when the row is
		// next laid out, on whatever the message is by then.
		w.handleReactionChipTap(gtx, chips, message, reactions)
		return w.kit().ReactionChips(gtx, chips, reactions)
	}
}

// bubbleStatus is the delivery line under the caller's own messages, and nil
// when there is nothing to say — an incoming message, or an outgoing one whose
// status has not come back yet.
func (w *Window) bubbleStatus(message service.DirectMessage, isMine bool) layout.Widget {
	if !isMine {
		return nil
	}
	text, ok := messageStatusText(message, w.t)
	if !ok {
		return nil
	}
	return func(gtx layout.Context) layout.Dimensions {
		label := material.Caption(w.theme, text)
		label.Color = ui.MessageStatusColor()
		return label.Layout(gtx)
	}
}

// chatTimestampLayout is how every date in the chat is written: the bubble
// header, the delivery line and the reply quote all use it, and a date that
// changed shape between them would read as two different clocks.
const chatTimestampLayout = "02.01.2006 15:04"

// messageStatusText renders the delivery line for one outgoing message and
// reports whether there is a line at all.
//
// The receipt status and the delivery timestamp are read together rather than
// in sequence: "delivered" with a time and "delivered" without are different
// lines, and a message whose only evidence of arrival is the timestamp still
// gets the single tick. tr is the window's t method, injected so the mapping
// stays a pure function for tests.
func messageStatusText(message service.DirectMessage, tr func(string, ...any) string) (string, bool) {
	deliveredAt := func() string {
		return message.DeliveredAt.Time().Local().Format(chatTimestampLayout)
	}
	switch {
	case message.ReceiptStatus == "seen" && message.DeliveredAt.Valid():
		return "✓✓ " + deliveredAt(), true
	case message.ReceiptStatus == "seen":
		return "✓✓", true
	case message.ReceiptStatus == "delivered" && message.DeliveredAt.Valid():
		return "✓ " + deliveredAt(), true
	case message.ReceiptStatus == "delivered":
		return "✓", true
	case message.DeliveredAt.Valid():
		return "✓ " + deliveredAt(), true
	}
	pending := map[string]string{
		"queued":   "chat.status.queued",
		"retrying": "chat.status.retrying",
		"failed":   "chat.status.failed",
		"expired":  "chat.status.expired",
		"sent":     "chat.status.sent",
	}
	key, ok := pending[message.ReceiptStatus]
	if !ok {
		return "", false
	}
	return tr(key), true
}

// messageSelectable returns a reusable Selectable widget for the given
// message ID, creating one on first access. This allows users to select
// and copy message text in the chat view. All per-message caches
// (including this one) are reset on conversation change in
// resetReplyOnPeerChange — early in layout, before any bubble registers
// event tags — to prevent unbounded growth across chat peers.
func (w *Window) messageSelectable(id string) *widget.Selectable {
	sel := w.messageSelectables[id]
	if sel == nil {
		sel = &widget.Selectable{}
		w.messageSelectables[id] = sel
	}
	return sel
}

// searchKnownIdentities matches the query against the UNION of the observed
// identities (KnownIDs) and the routed ones (ReachableIDs). The two sets
// answer different questions — "whose keys have I seen" and "whom can I
// reach" — and a freshly announced node lives in the second long before it
// reaches the first, which is exactly the row the search used to lose
// (docs/protocol/identity-lookup.md).
//
// A full, valid 40-hex query absent from BOTH sets still yields a candidate
// row: absence from ReachableIDs does not prove absence of a route — a nil
// map means "state unknown", the snapshot has lawful staleness, and in the
// DHT era no full reachable set will exist at all. "No route" is only ever
// stated by the resolver's outcome, never by this filter.
func searchKnownIdentities(knownIDs []string, reachable map[domain.PeerIdentity]bool, recipients []domain.PeerIdentity, self domain.PeerIdentity, query string) []domain.PeerIdentity {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return nil
	}

	alreadyListed := make(map[domain.PeerIdentity]struct{}, len(recipients))
	for _, recipient := range recipients {
		alreadyListed[recipient] = struct{}{}
	}

	results := make([]domain.PeerIdentity, 0, len(knownIDs))
	seen := make(map[domain.PeerIdentity]struct{}, len(knownIDs)+len(reachable))
	admit := func(raw string, id domain.PeerIdentity) {
		if raw == "" || id == self || id.IsZero() {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		if _, ok := alreadyListed[id]; ok {
			return
		}
		if !strings.Contains(strings.ToLower(raw), query) {
			return
		}
		results = append(results, id)
	}
	for _, raw := range knownIDs {
		raw = strings.TrimSpace(raw)
		admit(raw, domain.PeerIdentityFromWire(raw))
	}
	for id, hasRoute := range reachable {
		if hasRoute {
			admit(id.String(), id)
		}
	}

	// The candidate row: the user pasted a complete address nobody here has
	// heard of yet. It must be selectable — opening the chat is what starts
	// the key discovery.
	if candidate, err := domain.ParsePeerIdentity(query); err == nil && !candidate.IsZero() {
		admit(candidate.String(), candidate)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Compare(results[j]) < 0 })
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
	return shortFingerprint(identity.String())
}

func (w *Window) t(key string, args ...any) string {
	return translate(w.language, key, args...)
}

// tCount renders a phrase whose wording depends on the number in it —
// "1 сообщение" but "2 сообщения" — by picking the catalogue entry for
// the count's plural form. See i18n_plural.go.
func (w *Window) tCount(key string, count int, args ...any) string {
	return translateCount(w.language, key, count, args...)
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

// contextMenuDeleteEnabled reports whether the Delete item in the open
// message context menu is actionable. It is, whenever a menu is open:
// deleting always removes the local copy at once, and the peer-side half
// is scheduled rather than refused — an unreachable peer delays the
// request, it never blocks the user from destroying their own copy (see
// docs/dm-commands.md §"Scheduled deletion").
//
// False when no menu is open (msgContextMsg is nil), which is what keeps
// the row out of the focus ring in msgMenuItems, and false with no
// router: the click handler dereferences it, and a menu that offers an
// action nothing can carry out is worse than one that omits it.
func (w *Window) contextMenuDeleteEnabled() bool {
	return w != nil && w.msgContextMsg != nil && w.router != nil
}

func (w *Window) layoutConsoleButton(gtx layout.Context) layout.Dimensions {
	return w.kit().ToolbarButton(gtx, &w.consoleButton, ui.ToolbarButtonOpts{
		Label:    w.t("header.console"),
		Icon:     w.consoleIcon,
		IconSide: ui.IconLeading,
		Active:   w.consoleModalVisible(),
	})
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

// openBrowser, openFile and revealFileInDir are platform-selected:
// exec-based implementations for desktop OSes live in open_default.go,
// the Android intent/stub implementations in open_android.go.

func (w *Window) languageButton(code string) *widget.Clickable {
	if w.languageOptions == nil {
		w.languageOptions = make(map[string]*widget.Clickable, len(supportedLanguages))
	}
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

// languageToolbarButton describes the header's language button. Active while
// its menu is open, the same way a selected console tab is — that is what says
// which button the popup belongs to.
func (w *Window) languageToolbarButton() ui.ToolbarButtonOpts {
	return ui.ToolbarButtonOpts{
		Label:    currentLanguageLabel(w.language),
		Icon:     w.chevronDownIcon,
		IconSide: ui.IconTrailing,
		Active:   w.showLanguageMenu,
	}
}

func (w *Window) layoutLanguageDropdown(gtx layout.Context) layout.Dimensions {
	return layout.E.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		dims := w.kit().ToolbarButton(gtx, &w.languageToggle, w.languageToolbarButton())
		w.languageButtonSize = dims.Size
		return dims
	})
}

func (w *Window) layoutIdentityPanelOverlay(gtx layout.Context) layout.Dimensions {
	if w.identityPanelFocus.drive(gtx, w.identityPanelItems(), menuNavKeys{Tab: true}) {
		w.closeIdentityPanel()
		return layout.Dimensions{}
	}
	return w.kit().Modal(gtx, ui.Modal{
		Title:      w.t("clients.my_identity"),
		CloseHint:  w.t("clients.close_identity"),
		Close:      &w.identityPanelClose,
		DismissTag: &w.identityPanelDismissTag,
		Dismiss:    w.closeIdentityPanel,
		// Identity details keeps the rounder corner the design gives it; the
		// other modals use ui.ModalCardRadiusDp.
		CornerRadius: unit.Dp(ui.ModalIdentityRadiusDp),
		Sizing:       ui.ModalSizingCentered,
		Compact:      w.isCompactLayout(gtx),
		Content:      w.layoutIdentityPanelList,
	})
}

func (w *Window) identityPanelItems() []event.Tag {
	return []event.Tag{&w.identityPanelClose, &w.copyIdentityButton, &w.shareContactButton}
}

func (w *Window) layoutIdentityPanelList(gtx layout.Context) layout.Dimensions {
	return w.identityPanelList.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
		return w.layoutIdentityPanelContent(gtx)
	})
}

func (w *Window) layoutIdentityPanelContent(gtx layout.Context) layout.Dimensions {
	// The title and the close button belong to the modal shell around this
	// content (modal_shell.go), not to the content itself — every modal in the
	// application wears the same header.
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.layoutIdentityQR(gtx, unit.Dp(220))
			})
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(16)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body1(w.theme, w.snap.MyAddress.String())
			label.Color = color.NRGBA{R: 190, G: 204, B: 222, A: 255}
			label.Alignment = text.Middle
			label.MaxLines = 2
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(18)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.Y = gtx.Dp(unit.Dp(1))
			gtx.Constraints.Max.Y = gtx.Constraints.Min.Y
			ui.Fill(gtx, color.NRGBA{R: 50, G: 65, B: 84, A: 255})
			return layout.Dimensions{Size: image.Pt(gtx.Constraints.Max.X, gtx.Constraints.Min.Y)}
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(18)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.layoutIdentityActionButton(gtx, &w.copyIdentityButton, w.copyIcon, w.t("clients.copy_identity"))
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return w.layoutIdentityActionButton(gtx, &w.shareContactButton, w.shareIcon, w.t("clients.share_contact"))
		}),
	)
}

func (w *Window) layoutIdentityActionButton(gtx layout.Context, button *widget.Clickable, icon *widget.Icon, labelText string) layout.Dimensions {
	gtx.Constraints.Min.Y = gtx.Dp(unit.Dp(52))
	gtx.Constraints.Max.Y = gtx.Constraints.Min.Y
	border := widget.Border{
		Color:        color.NRGBA{R: 57, G: 75, B: 98, A: 255},
		CornerRadius: unit.Dp(10),
		Width:        unit.Dp(1),
	}
	return border.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		buttonStyle := material.ButtonLayout(w.theme, button)
		buttonStyle.Background = color.NRGBA{R: 27, G: 39, B: 53, A: 255}
		buttonStyle.CornerRadius = unit.Dp(10)
		return buttonStyle.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{Left: unit.Dp(14), Right: unit.Dp(14), Top: unit.Dp(13), Bottom: unit.Dp(13)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						return ui.Icon(gtx, icon, unit.Dp(23), color.NRGBA{R: 202, G: 215, B: 231, A: 255})
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						label := material.Body1(w.theme, labelText)
						label.Color = color.NRGBA{R: 242, G: 245, B: 249, A: 255}
						label.MaxLines = 1
						return label.Layout(gtx)
					}),
				)
			})
		})
	})
}

func (w *Window) layoutLanguageOverlay(gtx layout.Context) layout.Dimensions {
	// The backdrop goes down first, under the card and over everything else.
	// Until this existed the overlay let input through, so the click a user
	// aims at empty space to dismiss the menu also hit whatever was there. It
	// does not tint: a wash over the whole application for a six-row dropdown
	// reads as a modal dialogue, and the design does not ask for one here.
	w.kit().MenuPopupBackdrop(gtx, &w.languageMenuDismissTag, ui.MenuPopupScrimNone, w.closeLanguageMenu)

	anchor := w.languageMenuAnchor(gtx)
	width := gtx.Dp(unit.Dp(ui.MenuPopupLanguageWidthDp))
	// Right-aligned with the button, and just under it. The offset used to be
	// the constant 58dp below a 24dp window inset, which stopped matching the
	// header the day its padding changed — the menu opened a finger's width
	// below the button it belongs to.
	x := ui.MenuPopupAnchorX(anchor.Max.X-width, width, gtx.Constraints.Max.X)
	y := anchor.Max.Y + gtx.Dp(unit.Dp(ui.MenuPopupAnchorGapDp))

	stack := op.Offset(image.Pt(x, y)).Push(gtx.Ops)
	defer stack.Pop()

	// The height is a CAP, never a size: the card hugs its rows and scrolls
	// only what does not fit. In phone landscape the window below the anchor
	// can be shorter than the full list (six rows need ≈250dp), which used to
	// clip the bottom languages with no way to reach them.
	h := gtx.Dp(unit.Dp(languageMenuHeight))
	if avail := gtx.Constraints.Max.Y - y - gtx.Dp(unit.Dp(windowPadYDp)); avail < h {
		h = avail
	}
	if h < gtx.Dp(unit.Dp(menuMinUsableDp)) {
		h = gtx.Dp(unit.Dp(menuMinUsableDp))
	}

	menuGTX := gtx
	menuGTX.Constraints.Min.X = width
	menuGTX.Constraints.Max.X = width
	menuGTX.Constraints.Min.Y = 0
	menuGTX.Constraints.Max.Y = h
	_ = w.kit().MenuPopupCard(menuGTX, ui.MenuPopup{
		Items:  w.languageMenuItems(),
		Scroll: &w.languageMenuList,
	})

	return layout.Dimensions{}
}

// languageMenuAnchor is the language button's rectangle in window
// coordinates, which is where its popup hangs from.
//
// Gio gives no way to read a widget's absolute position, so the rectangle is
// reconstructed from the two things that DO place it: the window padding the
// header sits inside, and the header's own measured height. Both are recorded
// as the header is laid out (Window.layout), on the same frame this reads
// them, so the anchor cannot lag the button.
//
// The button is flush with the right edge of the header (layout.E), which is
// what makes "window width less the padding" its right edge.
//
// The fallbacks matter: the header YIELDS its whole row when the touch
// keyboard leaves too little space (keyboardYieldingChrome), so on the frames
// where it is not drawn its height is zero and the popup would climb to the
// top of the window. Holding the last measured height keeps the menu where the
// user last saw the button.
func (w *Window) languageMenuAnchor(gtx layout.Context) image.Rectangle {
	right := gtx.Constraints.Max.X - gtx.Dp(unit.Dp(windowPadXDp))
	bottom := gtx.Dp(unit.Dp(windowPadYDp)) + w.headerHeight
	size := w.languageButtonSize
	return image.Rect(right-size.X, bottom-size.Y, right, bottom)
}

// menuMinUsableDp is the smallest height in which drawing a context menu is
// worth anything: 8+8dp of inset around a Body2 line (~17dp) is 33dp, and the
// card adds a 1dp border plus a 6dp inset on each side, so 47dp shows exactly
// one actionable row and the card's List scrolls to the rest.
//
// Below it the menu is not drawn SMALLER — it is not drawn at all. Shrinking
// is what a menu measured against a floored inset does, and it does not help:
// the floor buys its room by claiming space the keyboard is standing on, so
// the one row it produces is mostly under the keyboard, which takes the touch.
//
// It is also what makes "measured against availH" a real bound rather than a
// hope. The card's chrome — 1dp border + 6dp inset on each side, 14dp — is
// FIXED: layout.Inset subtracts it from the constraint, floors that at 0 and
// then adds it back, so on a constraint below 14dp the card comes out TALLER
// than it was allowed and placeMenu's final y clamp goes negative and gives
// up. Refusing under 48dp keeps the scrolling List with 34dp to absorb, which
// is why menuAvailableHeight may return a degenerate number safely.
const menuMinUsableDp = 48

// menuAvailableHeight is the window height genuinely above the touch keyboard,
// used both to MEASURE a context menu (it becomes Constraints.Max.Y) and to
// place it. Context menus are separate Stacked layers that do NOT receive the
// main content's bottom keyboard inset, so the number has to describe the
// physically clear strip — exactly, including 0.
//
// keyboardInsetDp is the whole occlusion and reserves nothing for anyone, which
// is what makes it usable here: a number that had set some strip aside for the
// composer would be describing content the menu does not have, and a menu
// measured against it is drawn partly beneath the keyboard. Nothing here rounds
// the answer UP to keep the layout non-degenerate either — a degenerate answer
// is the truthful one, and menuOverlayRoom is the single place that decides
// what to do about it.
func (w *Window) menuAvailableHeight(gtx layout.Context) int {
	h := gtx.Constraints.Max.Y
	occ := gtx.Dp(keyboardInsetDp(gtx, &w.touchKbd))
	if occ <= 0 {
		return h
	}
	if occ > h {
		// The inset is already bounded by the window; this only absorbs the
		// dp→px rounding, and keeps the result from going negative.
		occ = h
	}
	return h - occ
}

// menuOverlayRoom reports the height a context-menu overlay may occupy and
// whether that is enough to draw one at all. When it is not, it asks for the
// keyboard to be taken away, so the room appears within a frame or two and the
// still-open menu draws itself then — see requestTouchKeyboardRoom for how
// that ask is throttled and what it cannot do. The caller must skip its card —
// but NOT its dismiss area — on a false, and must not close the menu: the menu
// is deferred, not cancelled.
func (w *Window) menuOverlayRoom(gtx layout.Context) (int, bool) {
	availH := w.menuAvailableHeight(gtx)
	if availH >= gtx.Dp(unit.Dp(menuMinUsableDp)) {
		w.menuKbdHideAskedGen = 0
		return availH, true
	}
	requestTouchKeyboardRoom(&w.touchKbd, &w.menuKbdHideAskedGen)
	return availH, false
}

// layoutContextMenuOverlay renders the right-click context menu for a recipient identity.
// It shows "Copy identity" and "Delete identity" options. Delete requires a confirmation step.
func (w *Window) layoutContextMenuOverlay(gtx layout.Context) layout.Dimensions {
	// Dismiss context menu on click outside.
	// We draw a full-screen transparent clickable area behind the menu.
	dismissArea := clip.Rect(image.Rectangle{Max: gtx.Constraints.Max}).Push(gtx.Ops)
	event.Op(gtx.Ops, &w.contextMenuPeer)
	dismissed := false
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: &w.contextMenuPeer, Kinds: pointer.Press})
		if !ok {
			break
		}
		if _, ok := ev.(pointer.Event); ok {
			w.contextMenuPeer = domain.PeerIdentity{}
			w.showDeleteConfirm = false
			w.showClearChatConfirm = false
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
			dismissed = true
			break
		}
	}
	dismissArea.Pop()
	// Nothing below this line speaks for a menu that has just been dismissed:
	// menuOverlayRoom would ask the keyboard to get out of the way of an
	// overlay that no longer needs the room, drive would run the focus
	// contract of a closed menu, and the header would draw the zero peer this
	// handler has just stored. Returning is also what makes the restore
	// prompt: skipping the card leaves the items out of the frame, so Gio
	// drops their focus at Frame time and the invalidate above brings a frame
	// where restoreOnClose sees focus free — rather than one where it spends
	// its frame of grace and then waits for a frame nothing asked for.
	if dismissed {
		return layout.Dimensions{}
	}

	// Reset scroll to the top when this OPEN menu switches to/from a confirm or
	// alias sub-view (fresh opens are handled by the closed-reset in layout()).
	var mode uint8
	switch {
	case w.showDeleteConfirm:
		mode = 1
	case w.showClearChatConfirm:
		mode = 2
	case w.showAliasEditor:
		mode = 3
	}
	if mode != w.lastCtxMenuMode {
		w.ctxMenuList.Position = layout.Position{}
		w.lastCtxMenuMode = mode
	}

	menuWidth := gtx.Dp(unit.Dp(220))
	windowW := gtx.Constraints.Max.X
	availH, room := w.menuOverlayRoom(gtx)
	if !room {
		// Too little clear height to draw even one row. The menu stays OPEN
		// (this is a deferred draw, not a dismissal) and menuOverlayRoom has
		// asked for the keyboard; the dismiss area above is already live.
		//
		// Escape must keep working here, or a keyboard user is stuck with a menu
		// they cannot see and cannot dismiss. Drive with no items: the keys are
		// still read, and first-item focus stays armed for the frame the room
		// comes back.
		if w.peerMenuFocus.drive(gtx, nil, menuNavKeys{Arrows: !w.showAliasEditor, Tab: true}) {
			w.escapePeerMenu()
		}
		return layout.Dimensions{}
	}

	// Keyboard/Narrator: claim focus for the menu, keep Tab inside it, and give
	// Escape a meaning. Deliberately here — after the room check, since a
	// deferred draw lays out no item to focus, and before the measure below,
	// whose disabled source would drop the FocusCmd. Arrow keys are left to the
	// alias editor's caret while it is on screen.
	if w.peerMenuFocus.drive(gtx, w.peerMenuItems(), menuNavKeys{Arrows: !w.showAliasEditor, Tab: true}) {
		w.escapePeerMenu()
		if w.contextMenuPeer.IsZero() {
			// Escape closed the menu outright; drawing a card for a peer that
			// is no longer selected would show one frame of an empty header.
			return layout.Dimensions{}
		}
		// Escape only stepped back out of a sub-view — draw the item list it
		// returned to, this frame, so the menu does not blink.
	}

	// Measure the menu, bounded to the usable height above the keyboard. The
	// card wraps its rows in a scrolling List, so when the content is taller
	// than availH it clamps to availH and scrolls (Delete / Clear chat, or an
	// alias editor's Save / Cancel, stay reachable) instead of squeezing the
	// bottom rows to nothing; when it fits, the List sizes to the content.
	measureGTX := gtx
	measureGTX.Constraints.Min.X = menuWidth
	measureGTX.Constraints.Max.X = menuWidth
	measureGTX.Constraints.Min.Y = 0
	measureGTX.Constraints.Max.Y = availH
	macro := op.Record(measureGTX.Ops)
	dims := w.contextMenuCard(measureGTX)
	menuCall := macro.Stop()

	// Now that the rows have been measured, scroll the one keyboard focus was
	// just placed on into view. Deliberately NOT by measuring a second time:
	// contextMenuCard drains the alias editor's events and every Clickable's,
	// and running it twice in one frame would consume them twice. The corrected
	// offset lands on the next frame instead, which has to be asked for — a
	// keyboard user waiting on it produces no input that would draw one. Same
	// reasoning, and the same call, as restoreOnClose.
	if w.ctxMenuScroll.into(&w.ctxMenuList, w.peerMenuFocus.want) {
		gtx.Execute(op.InvalidateCmd{})
	}

	x, y := placeMenu(w.contextMenuPos.X, w.contextMenuPos.Y, menuWidth, dims.Size.Y, windowW, availH)

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

	// Measure content to know the total size. The rows go through a vertical
	// List as a single item so an overflowing menu scrolls within availH rather
	// than dropping its bottom rows (see ctxMenuList).
	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(6)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			// List.Layout hands its element an unbounded main axis so the
			// element can report its natural size, so the viewport height is
			// only knowable out here — see menuScroll.
			// Reset here rather than inside the element: a List whose
			// viewport has collapsed to nothing lays its element out zero
			// times, and last frame's spans must not outlive the frame that
			// took them.
			w.ctxMenuScroll.begin(gtx.Constraints.Max.Y)
			return w.ctxMenuList.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
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

	// The clear-chat row's focus target, and whether it has one at all, follow
	// exactly the rule the row itself applies below — and the one peerMenuItems
	// applies, which is what makes the measured spans line up with the tags
	// focus actually visits.
	clearTag := event.Tag(&w.ctxMenuClearChat)

	sc := &w.ctxMenuScroll
	return sc.flex(gtx,
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		sc.row(&w.ctxMenuAlias, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAlias, aliasLabel,
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.ctxMenuCopy, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuCopy, w.t("context.copy_identity"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.ctxMenuDelete, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDelete, w.t("context.delete_identity"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(clearTag, func(gtx layout.Context) layout.Dimensions {
			// "Delete chat for both sides" (bulk wipe both sides), never
			// dimmed: the local thread goes at once and the peer's
			// half is scheduled until they acknowledge it, so an
			// offline peer delays the request rather than blocking
			// the user from erasing their own history.
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
	sc := &w.ctxMenuScroll
	return sc.flex(gtx,
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("context.delete_confirm"))
			label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
			return layout.Inset{Left: unit.Dp(12), Right: unit.Dp(12), Top: unit.Dp(2), Bottom: unit.Dp(6)}.Layout(gtx, label.Layout)
		}),
		sc.row(&w.ctxMenuDeleteConfirm, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteConfirm, w.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.ctxMenuDeleteCancel, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuDeleteCancel, w.t("context.delete_no"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

// layoutClearChatConfirmMenu renders the confirmation step for the
// "Delete chat for both sides" sidebar action. Visual structure mirrors
// layoutDeleteConfirmMenu so the user reads the same shape for both
// destructive sidebar actions; the body text and button widgets are
// the only differences. Kept as a separate menu (rather than reusing
// layoutDeleteConfirmMenu with a parameterised label) so the
// confirm/cancel widget targets stay distinct — sharing widgets
// would let a stale click event from the per-identity delete path
// fire the wipe path on the next frame.
func (w *Window) layoutClearChatConfirmMenu(gtx layout.Context) layout.Dimensions {
	sc := &w.ctxMenuScroll
	return sc.flex(gtx,
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("context.clear_chat_confirm"))
			label.Color = color.NRGBA{R: 230, G: 200, B: 140, A: 255}
			return layout.Inset{Left: unit.Dp(12), Right: unit.Dp(12), Top: unit.Dp(2), Bottom: unit.Dp(6)}.Layout(gtx, label.Layout)
		}),
		sc.row(&w.ctxMenuClearChatConfirm, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuClearChatConfirm, w.t("context.delete_yes"),
				color.NRGBA{R: 230, G: 90, B: 90, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.ctxMenuClearChatCancel, func(gtx layout.Context) layout.Dimensions {
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
			w.contextMenuPeer = domain.PeerIdentity{}
			w.showAliasEditor = false
			if w.window != nil {
				w.window.Invalidate()
			}
			return layout.Dimensions{}
		}
	}

	sc := &w.ctxMenuScroll
	return sc.flex(gtx,
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuHeader(gtx)
		}),
		sc.row(nil, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuSeparator(gtx)
		}),
		sc.row(&w.aliasEditor, func(gtx layout.Context) layout.Dimensions {
			return layout.Inset{
				Left: unit.Dp(12), Right: unit.Dp(12),
				Top: unit.Dp(4), Bottom: unit.Dp(6),
			}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				ed := material.Editor(w.theme, &w.aliasEditor, w.t("context.alias_placeholder"))
				ed.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
				ed.HintColor = color.NRGBA{R: 120, G: 135, B: 158, A: 255}
				gtx.Constraints.Min.X = gtx.Dp(unit.Dp(160))
				return editorTouchKeyboardArea(gtx, &w.touchKbdTags[2], &w.touchKbd, ed.Layout)
			})
		}),
		sc.row(&w.ctxMenuAliasSave, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasSave, w.t("context.alias_save"),
				color.NRGBA{R: 130, G: 200, B: 130, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.ctxMenuAliasCancel, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.ctxMenuAliasCancel, w.t("context.alias_cancel"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
	)
}

func (w *Window) contextMenuItem(gtx layout.Context, btn *widget.Clickable, label string, fg color.NRGBA) layout.Dimensions {
	hoverBg := color.NRGBA{R: 42, G: 52, B: 68, A: 255}

	return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
		if btn.Hovered() {
			ui.Fill(gtx, hoverBg)
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

// languageMenuItems builds the language menu's rows. The em dash between code
// and name is the design's (screen 7e), not a hyphen.
func (w *Window) languageMenuItems() []ui.MenuPopupItem {
	current := normalizeLanguage(w.language)
	items := make([]ui.MenuPopupItem, 0, len(supportedLanguages))
	for _, option := range supportedLanguages {
		items = append(items, ui.MenuPopupItem{
			Label:    option.Label + " — " + localizedLanguageName(option.Code),
			Button:   w.languageButton(option.Code),
			Selected: option.Code == current,
		})
	}
	return items
}

// handleLanguageMenu drains the language rows' clicks. It runs from
// handleActions rather than from inside the row builder, so that choosing a
// language is not a side effect of drawing one — the popup component lays rows
// out and nothing else.
func (w *Window) handleLanguageMenu(gtx layout.Context) {
	for _, option := range supportedLanguages {
		for w.languageButton(option.Code).Clicked(gtx) {
			w.selectLanguage(option.Code)
		}
	}
}

func (w *Window) selectLanguage(code string) {
	w.language = normalizeLanguage(code)
	w.showLanguageMenu = false
	if w.prefs != nil {
		w.prefs.Language = w.language
		_ = w.prefs.Save()
	}
	w.invalidate()
}

func (w *Window) closeLanguageMenu() {
	w.showLanguageMenu = false
	w.invalidate()
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

	// Note: the reply click is handled earlier in the frame by
	// handleReplyContextClicks (before handlePendingActions) so replyRev is
	// bumped before any same-frame send completion checks it.

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
		peer := conversationPeer(msg, w.router.MyAddress())

		targetID := domain.MessageID(msg.ID)

		w.msgContextMsg = nil

		w.dispatchMessageDeleteAsync(peer, targetID)

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
// handleIdentityResolutionState surfaces the actionable identity-lookup
// states in the status line (§4.9): the interactive timeout with its
// corsa:-link hint, success, exhaustion without keys, the no-route flag and
// an authoritative DM opt-out. Runs on the subscriber goroutine — it only
// touches the thread-safe router surface.
func (w *Window) handleIdentityResolutionState(state ebus.IdentityResolutionState) {
	if w.router == nil {
		return
	}
	short := shortFingerprint(state.Target.String())
	var msg string
	switch {
	case state.DMAvailable == domain.DMAvailabilityNo:
		msg = w.t("status.identity_lookup_dm_disabled", short)
	case state.Lifecycle == domain.IdentityResolutionSucceeded:
		msg = w.t("status.identity_lookup_succeeded", short)
	case state.Lifecycle == domain.IdentityResolutionExhausted && !state.Usable:
		msg = w.t("status.identity_lookup_exhausted", short)
	case state.InteractiveTimeout && !state.Usable && !state.Lifecycle.Terminal():
		msg = w.t("status.identity_lookup_timeout", short)
	case state.NoRoute && !state.Usable && !state.Lifecycle.Terminal():
		msg = w.t("status.identity_lookup_no_route", short)
	default:
		return
	}
	w.router.SetSendStatus(msg)
	if w.window != nil {
		w.window.Invalidate()
	}
}

func (w *Window) handleMessageDeleteOutcome(outcome ebus.MessageDeleteOutcome) {
	if w.router == nil {
		return
	}
	w.router.SetSendStatus(w.messageDeleteOutcomeCaption(outcome))
	if w.window != nil {
		w.window.Invalidate()
	}
}

// messageDeleteOutcomeCaption is the wording for a finished deletion.
//
// The route is consulted before the status because "deleted" alone does
// not tell the two immediate outcomes apart: a recalled message never
// left this node and the peer never saw it, while a local one was ours
// to remove all along. Outcomes that arrive later (a peer ack, an
// expired intent) carry no route and read by status.
func (w *Window) messageDeleteOutcomeCaption(outcome ebus.MessageDeleteOutcome) string {
	switch {
	case outcome.Abandoned:
		return w.t("status.message_delete_abandoned")
	case outcome.Route == domain.MessageDeleteRouteRecalled:
		return w.t("status.message_delete_recalled")
	case outcome.Status == domain.MessageDeleteStatusDeleted:
		return w.t("status.message_deleted")
	case outcome.Status == domain.MessageDeleteStatusNotFound:
		// Idempotent success: the peer never had the message or had
		// already deleted it on a previous attempt.
		return w.t("status.message_deleted")
	case outcome.Status == domain.MessageDeleteStatusDenied:
		return w.t("status.message_delete_denied")
	case outcome.Status == domain.MessageDeleteStatusImmutable:
		return w.t("status.message_delete_immutable")
	default:
		// Unknown wire-level status — fall back to a neutral abandoned
		// message rather than silently lying that delivery succeeded.
		return w.t("status.message_delete_abandoned")
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
// barrier check and land in chatlog just after the wipe read it,
// surviving a wipe the user believes erased the thread. Calling
// BeginConversationDelete on the UI thread closes that window: by
// the time we return to the event loop, IsConversationDeletePending
// = true and SendMessage / SendFileAnnounce return
// ErrConversationDeleteInflight for this peer.
// CompleteConversationDelete then runs the wipe and the dispatch on
// the goroutine without re-opening the gap.
//
// Ordering: the local thread is erased by Complete itself, before it
// returns. The caption this goroutine writes therefore describes only
// what the PEER still owes — being asked now, or scheduled until they
// are reachable — and the terminal outcome (applied / abandoned /
// local cleanup failed) arrives asynchronously through
// TopicConversationDeleteCompleted.
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
	w.router.SetSendStatus(w.t("status.clear_chat_dispatching"))
	if !w.beginUIOp() {
		return
	}
	go func(peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) {
		defer w.endUIOp()
		// No timeout of our own: the wipe bounds each of its three steps
		// separately (drain, transaction, compensation) and a deadline
		// here would only be the smaller of two clocks, hiding which one
		// actually applies. What this context DOES carry is shutdown —
		// the wipe is one of the cancellable UI operations uiStop exists
		// for, and without it the app would stop waiting after
		// drainUIOps' timeout while the wipe kept running.
		ctx, cancel := w.uiOpContext()
		defer cancel()
		err := w.router.CompleteConversationDelete(ctx, peer, requestID)
		if err == nil {
			// The outcome subscriber writes the real status the
			// moment the wipe commits, and it knows the count this
			// goroutine does not. Nothing to say here — clearing
			// the "dispatching…" line is the subscriber's job, and
			// overwriting it would only race it.
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
	}(peer, requestID)
}

// handleMessageSendFailed localises the one send refusal whose meaning
// the user can act on: a DEFERRED store, where the node declined to
// decide whether it may keep the message — the refusals of deleted ids
// are unreadable, so nothing goes into this conversation, sends included.
// The composer already has the text back; the only thing left to say is
// that trying again shortly is worth it.
//
// Every other failure keeps the service's line: it names the underlying
// error, which is what a diagnosis needs, and translating an arbitrary
// error string would say no more than the English one does.
func (w *Window) handleMessageSendFailed(result ebus.MessageSendFailedResult) {
	if w.router == nil || !errors.Is(result.Err, protocol.ErrStoreDeferred) {
		return
	}
	w.router.SetSendStatus(w.t("status.send_deferred"))
	if w.window != nil {
		w.window.Invalidate()
	}
}

// handleConversationDeleteOutcome is the ebus subscriber invoked when
// a wipe finishes locally. Counterpart
// of handleMessageDeleteOutcome for the bulk wipe-the-thread variant.
//
// Every outcome here describes the PEER's side: the local thread went
// at click time. "Abandoned" therefore means their copy may remain,
// and LocalCleanupFailed is the one outcome about THIS disk — it is
// published at click time when the wipe transaction rolled back and
// nothing changed on either side, so it is checked first and
// independently of any status.
func (w *Window) handleConversationDeleteOutcome(outcome ebus.ConversationDeleteOutcome) {
	if w.router == nil {
		return
	}
	w.router.SetSendStatus(w.conversationDeleteOutcomeCaption(outcome))
	if w.window != nil {
		w.window.Invalidate()
	}
}

// conversationDeleteOutcomeCaption is the wording for a wipe, at each of the
// two moments it has: the click, when this side is finished, and the peer's
// answer, when the other side is.
//
// The click-time wording says the deletion at the peer is scheduled rather
// than counting messages: there is nothing to count, and nothing to decide —
// the peer carries the request out, it is not asked to consent. The lasting
// feedback is the conversation header's pending line, which stands until the
// peer confirms, because the request is never given up on.
func (w *Window) conversationDeleteOutcomeCaption(outcome ebus.ConversationDeleteOutcome) string {
	switch {
	case outcome.LocalCleanupFailed:
		// No request either: the transaction rolled back, so nothing was
		// removed here and no peer was ever asked.
		return w.t("status.clear_chat_local_cleanup_failed")
	case outcome.Settled:
		return w.t("status.clear_chat_confirmed")
	case outcome.Requested:
		return w.t("status.clear_chat_scheduled")
	default:
		return w.t("status.clear_chat_empty")
	}
}

// handleReplyContextClicks processes the user's reply set (from the message
// context menu) and reply cancel clicks. It is invoked early in layout(),
// before handlePendingActions, so a reply change bumps replyRev before a
// same-frame send completion reads it — otherwise the completion could clear
// text the user is re-targeting to a different reply.
func (w *Window) handleReplyContextClicks(gtx layout.Context) {
	if w.msgContextMsg != nil && w.msgCtxReply.Clicked(gtx) {
		msgCopy := *w.msgContextMsg
		w.setReplyContext(&msgCopy)
		w.msgContextMsg = nil
		w.touchKbd.noteExplicitEditorFocus()
		gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
		if pointerClickedThisFrame(&w.msgCtxReply, gtx) && w.touchDrivenInput(gtx) {
			// Reply item TAPPED (not Return/Space): raise the keyboard for the
			// composer.
			showTouchKeyboard(&w.touchKbd)
		}
		if w.window != nil {
			w.window.Invalidate()
		}
	}
	if w.replyToMsg != nil {
		for w.replyCancelButton.Clicked(gtx) {
			w.setReplyContext(nil)
			if w.window != nil {
				w.window.Invalidate()
			}
		}
	}
}

// msgOverlayEdgeDp is how far the message overlay's surfaces stay from the
// window edges. It is the design's phone inset, and it is used TWICE — to work
// out how many quick reactions fit across the window, and to place what that
// produced. Reserving it in one place and ignoring it in the other made the
// reservation a fiction: placeMenu clamps into [0, windowW-blockW], so a menu
// opened near an edge put the pill flush against it.
const msgOverlayEdgeDp = 8

// msgMenuWidthDp is what the message menu card asks for. It is a want, not a
// promise: msgOverlayWidth cuts it down on a window that cannot hold it.
const msgMenuWidthDp = 180

// The menu card's own chrome. These are the figures menuMinUsableDp reasons
// about on the other axis, named here so the card and the widths derived from
// it cannot drift apart.
const (
	msgMenuCardBorderDp = 1
	msgMenuCardPadDp    = 6
)

// msgMenuCardChromePx is what the card puts either side of its rows, summed in
// PIXELS from the terms the card itself draws — not by converting their total.
//
// The two differ, and the difference is the whole bug this replaced: at 1.5
// px/dp the card's own 2×Dp(1) + 2×Dp(6) is 22px while Dp(14) is 21, so a gate
// asking for "more than 14dp" admitted a window that left the card's content
// exactly nothing. emojiPickerChromeHeight names the same rule on the other
// axis; this is that rule applied here.
func msgMenuCardChromePx(gtx layout.Context) int {
	return 2*gtx.Dp(unit.Dp(msgMenuCardBorderDp)) + 2*gtx.Dp(unit.Dp(msgMenuCardPadDp))
}

// msgOverlayFitsWidth reports whether the window is wide enough to draw
// anything in this overlay at all.
//
// The floor is the menu card's own chrome, and it is not a taste judgement:
// layout.Inset subtracts its inset, floors the result at zero and adds it back,
// so a card given less than its chrome comes out WIDER than it was allowed —
// the same trap menuMinUsableDp names for the height. Below that the surfaces
// were still "open" at a size of zero: nothing was drawn, yet the chat stayed
// dimmed behind a menu that was not there and the focus ring went on listing
// widgets no frame mentioned.
func msgOverlayFitsWidth(gtx layout.Context, windowW int) bool {
	return msgOverlayRoom(gtx, windowW) > msgMenuCardChromePx(gtx)
}

// msgOverlayScrim is how the backdrop looks while the overlay is in a given
// state. It tints only when there is a surface to tint BEHIND: the backdrop's
// other job — swallowing the press that dismisses — is wanted either way, but a
// deferred draw put a 40% wash over the whole chat with no menu on it, which
// reads as an application that has hung rather than one waiting for room.
func msgOverlayScrim(drawn bool) ui.MenuPopupScrim {
	if drawn {
		return ui.MenuPopupScrimDim
	}
	return ui.MenuPopupScrimNone
}

// msgOverlayRoom is how wide a surface in this overlay may be: the window less
// both edges, never negative.
func msgOverlayRoom(gtx layout.Context, windowW int) int {
	return max(0, windowW-2*gtx.Dp(unit.Dp(msgOverlayEdgeDp)))
}

// msgOverlayWidth is that room, or the width the surface wanted if it is
// smaller.
//
// Every surface in this overlay goes through it, and the menu card was the one
// that did not: the pill and the panel were made to fit the window while the
// card kept a flat 180dp, so on a narrow window the two surfaces it is placed
// with stayed inside and the card ran off the edge under them.
func msgOverlayWidth(gtx layout.Context, want, windowW int) int {
	return min(want, msgOverlayRoom(gtx, windowW))
}

// placeMsgOverlay is placeMenu plus that edge. The horizontal clamp is the only
// part that changes: the vertical one is already governed by availH, which is
// the room above the on-screen keyboard rather than the window.
//
// A surface too wide for the window less both edges gets the left edge and
// overhangs the right, which is the same answer placeMenu gives and is only
// reachable when the caller has ignored the width budget.
func placeMsgOverlay(gtx layout.Context, anchor image.Point, size image.Point, windowW, availH int) (int, int) {
	x, y := placeMenu(anchor.X, anchor.Y, size.X, size.Y, windowW, availH)
	edge := gtx.Dp(unit.Dp(msgOverlayEdgeDp))
	return min(max(x, edge), max(edge, windowW-size.X-edge)), y
}

// msgReactionRowGapDp is the air between the reaction pill and the menu card
// under it. Small enough that the two read as one surface opened by one
// gesture, wide enough that a 40dp slot is not mistaken for a menu row.
const msgReactionRowGapDp = 6

// layoutMsgContextMenuOverlay renders what a right-click on a chat message
// opens: the reaction pill (screens 3e/3f) with the menu card under it, or —
// once "more" has been pressed — the full emoji panel in its place (screen 3h).
//
// The two surfaces are ONE overlay because they are one open state. A pill with
// its own dismissal, its own focus ring and its own backdrop would be a second
// thing to close, and the first stray press that closed one but not the other
// would leave a menu floating over a chat with nothing under it.
func (w *Window) layoutMsgContextMenuOverlay(gtx layout.Context) layout.Dimensions {
	// Decided before the backdrop is drawn, because it decides how the backdrop
	// LOOKS: a wash over a chat with no menu on it is worse than no wash.
	windowW := gtx.Constraints.Max.X
	availH, room := w.menuOverlayRoom(gtx)
	drawn := room && msgOverlayFitsWidth(gtx, windowW)

	// The backdrop swallows every press, whether or not that press dismisses
	// anything — a click aimed at empty space must not also select the contact
	// underneath — and it does so even while nothing is drawn, so a press is
	// still the way out of a deferred overlay.
	dismissed := false
	w.kit().MenuPopupBackdrop(gtx, w.msgContextMsg, msgOverlayScrim(drawn), func() {
		if dismissed {
			return
		}
		dismissed = true
		w.msgContextMsg = nil
		if w.window != nil {
			w.window.Invalidate()
		}
	})
	if dismissed {
		return layout.Dimensions{}
	}

	if !drawn {
		// See the recipient menu: deferred until the keyboard frees the room,
		// with Escape kept alive throughout. A window too NARROW takes the same
		// path — nothing is drawn either way, and a resize can free the room the
		// way the keyboard coming down does.
		//
		// The presses of the LAST frame are still read here. The overlay can be
		// on screen when a slot is pressed and gone by the frame that reads the
		// release — the keyboard comes up, the window shrinks — and a press
		// nobody asks about is discarded at Frame time rather than postponed,
		// which loses the tap the user actually made.
		w.handleReactionRowActions(gtx)
		// The empty item list is what keeps the ring honest: it lists nothing
		// while nothing is drawn, so no focus is claimed for a widget that is
		// not in the frame.
		if w.msgMenuFocus.drive(gtx, nil, w.msgMenuNavKeys()) {
			w.escapeMsgMenu()
			w.dropReactionClicks(gtx)
		}
		return layout.Dimensions{}
	}

	// Whether the pill is drawn at all is decided BEFORE the focus ring is
	// built, because the ring lists it. A ring holding an item the frame never
	// draws is worse than a shorter ring: Gio drops the focus of any tag the
	// frame did not mention, so the next frame would pull it back to the same
	// invisible slot, every frame, for as long as the menu is open.
	if w.reactionPickerOpen() && w.reactionPickerSize(gtx, availH) == (image.Point{}) {
		// No room for the panel. It steps back to the pill rather than staying
		// open and invisible: an open surface that is never drawn owns the
		// focus ring and swallows Escape while showing the user nothing.
		w.closeReactionPicker()
	}
	w.reactionRow.quick = w.quickReactionsFor(gtx, windowW)
	w.reactionRow.shown = w.reactionRowFits(gtx, availH)

	// Focus contract, as for the identity menu above. Escape always closes the
	// whole overlay except while the emoji panel is up, where it steps back to
	// the pill — the same "one step out of a sub-view" rule the identity menu
	// applies to its confirmations.
	if w.msgMenuFocus.drive(gtx, w.msgMenuItems(), w.msgMenuNavKeys()) {
		w.escapeMsgMenu()
		w.dropReactionClicks(gtx)
		return layout.Dimensions{}
	}

	// Whatever the ring just focused has to be brought on screen by the layout
	// below; see ui.EmojiPickerState.RevealTag.
	w.reactionRow.panel.RevealTag(w.msgMenuFocus.want)
	w.handleReactionRowActions(gtx)
	if w.msgContextMsg == nil {
		// A reaction was chosen and closed the overlay mid-frame.
		return layout.Dimensions{}
	}

	if w.reactionPickerOpen() {
		w.placeReactionPicker(gtx, windowW, availH)
		return layout.Dimensions{}
	}
	w.placeReactionRowAndMenu(gtx, windowW, availH)
	return layout.Dimensions{}
}

// reactionRowFits reports whether the pill is worth drawing at all this frame:
// whether the window is wide enough to hold even one quick choice beside the
// "more" button, and whether the pill and the menu can BOTH have room in the
// height available.
//
// The menu comes first when they cannot fit vertically. Reply, Copy and Delete
// are the only way to act on a message; the pill is a shortcut to a reaction
// that the menu can reach anyway. Splitting the little room there is would leave
// the menu at one clipped row AND the pill at nothing — worse than either alone.
//
// The width is checked for a different reason: nothing clips the pill to the
// window. It is drawn at its own size and placed by an anchor, so a pill wider
// than the screen simply hangs off the right edge — which is what a fixed
// seven-slot row did on a 320dp phone, taking the "more" button with it while
// the focus ring went on offering it. quickReactionsFor drops slots until the
// row fits; this is the floor below which there is nothing left to drop.
func (w *Window) reactionRowFits(gtx layout.Context, availH int) bool {
	if len(w.reactionRow.quick) == 0 {
		return false
	}
	block := w.reactionRowSize(gtx).Y + gtx.Dp(unit.Dp(msgReactionRowGapDp))
	return availH-block >= gtx.Dp(unit.Dp(menuMinUsableDp))
}

// msgMenuNavKeys is which navigation keys the message overlay's focus ring may
// take. The arrows go back to the caret whenever the emoji panel is up: its
// first item is a text field, and a ring that stole Up and Down from it would
// make the search box unusable. The identity menu does the same for its alias
// editor.
func (w *Window) msgMenuNavKeys() menuNavKeys {
	return menuNavKeys{Arrows: !w.reactionPickerOpen(), Tab: true}
}

// placeReactionRowAndMenu draws the pill and the menu card as one block: the
// block is anchored where the gesture happened, and the two parts keep their
// order inside it.
//
// They are placed together rather than separately because placeMenu flips a
// surface above its anchor when it does not fit below. Anchoring each on its
// own let the flip apply to one and not the other, which put the pill under the
// menu near the bottom of the window — the one place the order matters most,
// since that is where a thumb reaches first.
func (w *Window) placeReactionRowAndMenu(gtx layout.Context, windowW, availH int) {
	menuWidth := msgOverlayWidth(gtx, gtx.Dp(unit.Dp(msgMenuWidthDp)), windowW)
	rowSize := w.reactionRowSize(gtx)
	gap := gtx.Dp(unit.Dp(msgReactionRowGapDp))

	if !w.reactionRow.shown {
		rowSize, gap = image.Point{}, 0
	}

	// Measure the menu, bounded to the usable height above the keyboard less
	// what the pill takes; the card's scrolling List clamps-with-scroll on
	// overflow instead of squeezing the bottom rows (see the recipient menu).
	measureGTX := gtx
	measureGTX.Constraints.Min.X = menuWidth
	measureGTX.Constraints.Max.X = menuWidth
	measureGTX.Constraints.Min.Y = 0
	measureGTX.Constraints.Max.Y = max(0, availH-rowSize.Y-gap)
	macro := op.Record(measureGTX.Ops)
	menuDims := w.msgContextMenuCard(measureGTX)
	menuCall := macro.Stop()

	// Scroll the freshly focused row into view; see the recipient menu above.
	if w.msgCtxMenuScroll.into(&w.msgCtxMenuList, w.msgMenuFocus.want) {
		gtx.Execute(op.InvalidateCmd{})
	}

	blockW := max(rowSize.X, menuWidth)
	blockH := rowSize.Y + gap + menuDims.Size.Y
	x, y := placeMsgOverlay(gtx, w.msgContextPos, image.Pt(blockW, blockH), windowW, availH)

	if rowSize.Y > 0 {
		drawAt(gtx, image.Pt(x, y), rowSize, w.layoutReactionRow)
	}

	menuStack := op.Offset(image.Pt(x, y+rowSize.Y+gap)).Push(gtx.Ops)
	menuCall.Add(gtx.Ops)
	menuStack.Pop()
}

// placeReactionPicker draws the full emoji panel where the pill was. It stands
// in place of the pill and the menu rather than over them: the panel is the
// same choice the pill offers, made from a longer list, and leaving a menu
// half-visible behind a 250dp surface only invites a press that lands on
// neither.
func (w *Window) placeReactionPicker(gtx layout.Context, windowW, availH int) {
	size := w.reactionPickerSize(gtx, availH)
	if size.X == 0 || size.Y == 0 {
		return
	}
	x, y := placeMsgOverlay(gtx, w.msgContextPos, size, windowW, availH)
	drawAt(gtx, image.Pt(x, y), size, w.layoutReactionPicker)
}

func (w *Window) msgContextMenuCard(gtx layout.Context) layout.Dimensions {
	borderColor := color.NRGBA{R: 72, G: 85, B: 106, A: 255}
	bgColor := color.NRGBA{R: 28, G: 34, B: 44, A: 255}
	rr := gtx.Dp(unit.Dp(8))
	borderWidth := gtx.Dp(unit.Dp(msgMenuCardBorderDp))

	macro := op.Record(gtx.Ops)
	dims := layout.UniformInset(unit.Dp(msgMenuCardBorderDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.UniformInset(unit.Dp(msgMenuCardPadDp)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			// One list item so an overflowing menu scrolls within availH.
			// Measured out here for the reason given in contextMenuCard.
			w.msgCtxMenuScroll.begin(gtx.Constraints.Max.Y)
			return w.msgCtxMenuList.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
				return w.layoutMsgContextMenuItems(gtx)
			})
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
	// As in layoutContextMenuItems: the delete row's tag follows the same
	// enablement rule the row and msgMenuItems both apply.
	var deleteTag event.Tag
	if w.contextMenuDeleteEnabled() {
		deleteTag = &w.msgCtxDelete
	}

	sc := &w.msgCtxMenuScroll
	return sc.flex(gtx,
		sc.row(&w.msgCtxReply, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.msgCtxReply, w.t("context.reply"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(&w.msgCtxCopy, func(gtx layout.Context) layout.Dimensions {
			return w.contextMenuItem(gtx, &w.msgCtxCopy, w.t("context.copy_message"),
				color.NRGBA{R: 245, G: 247, B: 250, A: 255})
		}),
		sc.row(nil, layout.Spacer{Height: unit.Dp(2)}.Layout),
		sc.row(deleteTag, func(gtx layout.Context) layout.Dimensions {
			// Delete uses a warning-tinted label so the destructive
			// nature is visible. The handler in
			// handleMsgContextMenuActions invokes
			// router.SendMessageDelete asynchronously; it always
			// removes the local copy, and what else happens
			// depends on the route (domain.MessageDeleteRoute):
			//
			//   - Incoming DM: nothing is asked of the author.
			//     The terminal outcome is published at once.
			//
			//   - Outgoing DM the peer never confirmed: the
			//     delivery this node still owns is cancelled
			//     first, so a deleted message cannot be handed
			//     to the peer afterwards.
			//
			//   - Outgoing DM either way: the peer-side deletion
			//     is scheduled — dispatched now if they are
			//     reachable, otherwise the moment they are, and
			//     it survives a restart. The terminal status
			//     (deleted / not_found / denied / immutable /
			//     abandoned) arrives later via
			//     TopicMessageDeleteCompleted.
			//
			// The item is dimmed for one reason only, and it is
			// not about the peer: a window with no router has
			// nothing to carry the action out with
			// (contextMenuDeleteEnabled). An unreachable peer
			// delays the request, it never blocks the user from
			// destroying their own copy.
			if !w.contextMenuDeleteEnabled() {
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
	cm, cmFound := w.findCachedMsg(replyToStr)

	quotedBody := replyBodyForDisplay(cm.Body, cmFound && cm.IsImageFile, w.t)
	if quotedBody == "" {
		quotedBody = w.t("chat.reply_unknown")
	}
	quotedBody = ellipsize(quotedBody, 80)

	// Resolve sender display name and timestamp from cache.
	var quotedAuthor string
	var quotedTime string
	if cmFound {
		if cm.Sender == w.snap.MyAddress {
			quotedAuthor = w.t("chat.you_label")
		} else {
			quotedAuthor = w.peerDisplayName(cm.Sender)
		}
		quotedTime = cm.Timestamp.Local().Format(chatTimestampLayout)
	}

	// Mini image preview for quoted image files. Nil while the file is
	// not on disk yet or the decode has not finished — the quote then
	// renders text-only.
	var quotedThumb *thumbnailEntry
	if cmFound && cm.IsImageFile {
		quotedThumb = w.replyThumb(replyToStr, cm.Sender)
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
					if quotedThumb == nil {
						return layout.Dimensions{}
					}
					return layout.Inset{Left: unit.Dp(6), Top: unit.Dp(2), Bottom: unit.Dp(2)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return layoutReplyThumb(gtx, quotedThumb, replyQuoteThumbDp)
					})
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

// replyBodyForDisplay maps a quoted message body to what the reply UI
// should show. A quoted image without a user caption carries the
// FileDMBodySentinel body — the raw sentinel is a wire detail, so it is
// replaced with the localized "Photo" label next to the mini preview.
// Every other body (captions, plain text, non-image files) is returned
// verbatim. tr is the Window's t method, injected so the mapping stays
// a pure function for tests (and to avoid shadowing the package-level
// translate).
func replyBodyForDisplay(body string, isImageFile bool, tr func(string, ...any) string) string {
	if isImageFile && body == domain.FileDMBodySentinel {
		return tr("chat.photo_label")
	}
	return body
}

// replyThumb resolves the decoded thumbnail for a quoted image message.
// Returns nil while the file is absent from disk (e.g. not downloaded
// yet on the receiver side), the decode is still in flight, or decoding
// failed permanently — callers render a text-only quote then. When an
// in-flight decode completes, the decode goroutine invalidates the
// window, so the preview appears on the next frame without polling.
func (w *Window) replyThumb(msgID string, sender domain.PeerIdentity) *thumbnailEntry {
	isSender := sender == w.snap.MyAddress
	path := w.router.FileBridge().FilePath(domain.FileID(msgID), isSender)
	if path == "" {
		return nil
	}
	return w.thumbCache.get(path, w.window)
}

// layoutReplyThumb renders a small square, center-cropped image preview
// inside a reply quote. Cover fit crops the source to fill the square —
// the messenger convention for quote previews — so the block height
// stays fixed regardless of the source aspect ratio.
func layoutReplyThumb(gtx layout.Context, entry *thumbnailEntry, edge unit.Dp) layout.Dimensions {
	sz := image.Pt(gtx.Dp(edge), gtx.Dp(edge))
	defer clip.UniformRRect(image.Rectangle{Max: sz}, gtx.Dp(unit.Dp(4))).Push(gtx.Ops).Pop()
	imgWidget := widget.Image{
		Src:      entry.op,
		Fit:      widget.Cover,
		Position: layout.Center,
	}
	gtx.Constraints = layout.Exact(sz)
	return imgWidget.Layout(gtx)
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
// The rebuild is skipped when the snapshot's DMGeneration has not changed.
// That counter — NOT Generation — is the gate. Generation is bumped on every
// DMRouter state mutation including the ones that touch no DM data at all: the
// 1s resource sample and the 500ms peer-health deltas advance it 2-3 times a
// second while ActiveMessages is reused byte-for-byte, so gating on it rebuilt
// the whole map and re-hashed every ID on the UI goroutine several times a
// second over a conversation that had not changed, unbounded in the length of
// the conversation. DMGeneration moves only when the router rebuilds the DM
// half, which is the exact granularity of this cache; every mutation the old
// comment claimed is still detected, because a change to a body, a ReplyTo, a
// receipt status or the selected conversation reaches activeMessages through a
// DM-typed notify (see applyReceiptRepair for the receipt case, which
// deliberately notifies UIEventMessagesUpdated). O(1) per no-change frame.
//
// It also carries chatOrder, the menu-rect signature's fingerprint of the
// conversation's row order (see menuRectSig). That belongs here and nowhere
// else: a conversation has no cap — ConversationCache.Messages() returns all of
// it — so hashing the IDs on every frame is unbounded work, while this loop
// already visits each message exactly once per DM generation, which is exactly
// the granularity at which the order can change. The skip path leaves chatOrder
// alone on purpose: an unchanged DM generation means unchanged messages.
func (w *Window) rebuildMsgCache() {
	gen := w.snap.DMGeneration
	msgs := w.snap.ActiveMessages

	if len(msgs) == 0 {
		// Unconditionally, not inside the nil check: an empty conversation has
		// an empty order, and leaving a previous conversation's digest behind
		// here would make the signature claim rows that are not on screen.
		w.chatOrder = 0
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
	order := menuDigestOffset
	for i := range msgs {
		m[msgs[i].ID] = cachedMsg{
			Body:        msgs[i].Body,
			Sender:      msgs[i].Sender,
			Timestamp:   msgs[i].Timestamp,
			Index:       i,
			IsImageFile: isImageFileAnnounce(msgs[i].Command, msgs[i].CommandData),
		}
		// Message IDs are variable length, unlike PeerIdentity, so the
		// concatenation is ambiguous on its own: "ab","c" and "a","bc" would
		// hash alike. Terminated with the LENGTH rather than a separator byte,
		// because an incoming message carries the sender's ID verbatim off the
		// wire and nothing on this path promises which bytes it excludes — a
		// separator would only be unambiguous for IDs that avoid it, whereas a
		// length is unambiguous for any bytes at all.
		for _, b := range []byte(msgs[i].ID) {
			order ^= uint64(b)
			order *= menuDigestPrime
		}
		order ^= uint64(len(msgs[i].ID))
		order *= menuDigestPrime
	}
	w.msgCacheByID = m
	w.msgCacheGen = gen
	w.chatOrder = order
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
// layoutFailedSends renders the "not sent" banner above the composer for the
// open conversation: how many messages failed, plus retry and dismiss buttons.
// The composer itself is never touched by a failed send.
func (w *Window) layoutFailedSends(gtx layout.Context) layout.Dimensions {
	n := len(w.failedSends[w.draftPeer])
	// Record the rendered prefix length so Retry/Dismiss (processed on a later
	// frame) act only on what the user actually saw, never on failures that
	// arrived after this frame was drawn.
	if w.failedShown == nil {
		w.failedShown = make(map[domain.PeerIdentity]int)
	}
	w.failedShown[w.draftPeer] = n
	if n == 0 {
		return layout.Dimensions{}
	}
	preview := ""
	if last := w.failedSends[w.draftPeer][n-1]; last.body != "" {
		preview = ellipsize(last.body, 60)
	} else if n > 0 {
		preview = "📎"
	}
	bgColor := color.NRGBA{R: 60, G: 34, B: 34, A: 255}
	barColor := color.NRGBA{R: 210, G: 90, B: 90, A: 255}

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
										label := material.Caption(w.theme, w.t("compose.not_sent", n))
										label.Color = color.NRGBA{R: 225, G: 150, B: 150, A: 255}
										return label.Layout(gtx)
									}),
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										label := material.Caption(w.theme, preview)
										label.Color = color.NRGBA{R: 210, G: 185, B: 185, A: 255}
										label.MaxLines = 1
										return label.Layout(gtx)
									}),
								)
							})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return layout.Inset{Left: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								btn := material.Button(w.theme, &w.failedRetryButton, w.t("compose.retry"))
								btn.Background = color.NRGBA{R: 70, G: 90, B: 60, A: 255}
								btn.Color = color.NRGBA{R: 210, G: 225, B: 200, A: 255}
								btn.Inset = layout.Inset{Top: unit.Dp(2), Bottom: unit.Dp(2), Left: unit.Dp(6), Right: unit.Dp(6)}
								return btn.Layout(gtx)
							})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							return layout.Inset{Left: unit.Dp(4)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								btn := material.Button(w.theme, &w.failedDismissButton, "✕")
								btn.Background = color.NRGBA{R: 78, G: 50, B: 50, A: 255}
								btn.Color = color.NRGBA{R: 225, G: 200, B: 200, A: 255}
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

func (w *Window) layoutReplyPreview(gtx layout.Context) layout.Dimensions {
	if w.replyToMsg == nil {
		return layout.Dimensions{}
	}

	// IsImageFile comes from the message cache rather than re-parsing
	// the announce payload every frame. dropStaleReply guarantees the
	// quoted ID is present in the cache whenever CacheReady holds; on
	// the transient not-found frames the banner degrades to text-only.
	cm, cmFound := w.findCachedMsg(w.replyToMsg.ID)
	replyIsImage := cmFound && cm.IsImageFile

	var replyThumbEntry *thumbnailEntry
	if replyIsImage {
		replyThumbEntry = w.replyThumb(w.replyToMsg.ID, w.replyToMsg.Sender)
	}

	quotedBody := ellipsize(replyBodyForDisplay(w.replyToMsg.Body, replyIsImage, w.t), 80)
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
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							if replyThumbEntry == nil {
								return layout.Dimensions{}
							}
							return layout.Inset{Left: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return layoutReplyThumb(gtx, replyThumbEntry, composerReplyThumbDp)
							})
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
