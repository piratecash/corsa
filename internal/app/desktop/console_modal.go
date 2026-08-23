package desktop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/font"
	"gioui.org/io/clipboard"
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

const (
	// maxSuggestions caps the completion popup. The panel hugs its rows and
	// scrolls past this only on a window too short to show them all.
	maxSuggestions = 6
	// maxConsoleCommandHistory bounds the in-memory ring of submitted commands
	// reachable via Up/Down arrows in the console input. Older entries are
	// dropped from the front when the cap is exceeded. The ring lives as long
	// as the process now that the modal is reused, so this cap is what keeps a
	// long session from growing it without limit.
	maxConsoleCommandHistory = 200
	// consoleVisibleTabs is how many tabs stay on the strip once it folds the
	// tail into a "More" menu. The width that triggers the fold is the
	// application's own compact breakpoint (compactLayoutMaxDp, read through
	// Window.isCompactLayout) rather than a number of its own: the card is
	// full-screen at exactly that width, which is exactly when six tabs stop
	// fitting.
	consoleVisibleTabs = 4
	// Tab pill geometry, from §4 of docs/design/CHANGES.md. The text size is
	// part of it: material.Body1 takes the theme's size (16sp), which made the
	// pills half again as tall as the design and pushed six of them off any
	// window narrower than a desktop.
	consoleTabRadiusDp   = 5
	consoleTabPaddingXDp = 10
	consoleTabPaddingYDp = 7
	consoleTabGapDp      = 4
	consoleTabTextSp     = 13
	// The "More" slot carries a trailing chevron: 1dp less side padding, a
	// 4dp gap and a 16dp glyph, per §4 of docs/design/CHANGES.md.
	consoleTabIconPaddingXDp = 9
	consoleTabIconGapDp      = 4
	consoleTabIconDp         = 16
)

type consoleTab int32

func (c *consoleModal) currentTab() consoleTab {
	return consoleTab(atomic.LoadInt32(&c.activeTab))
}

const (
	consoleTabConsole consoleTab = iota
	consoleTabPeers
	consoleTabTraffic
	consoleTabFile
	consoleTabInfo
	consoleTabDonate
)

type consoleEntry struct {
	Command string
	// Output is the DISPLAY text: capped in bytes and lines before any
	// widget sees it (console_output.go). The complete output, when it
	// exceeded the caps, lives in the overflow file.
	Output string
	// OverflowPath is the temp file carrying the full output; empty when
	// Output is complete. Deleted when the entry is evicted and when the
	// console window closes.
	OverflowPath string
	// FullBytes is the size of the full output, shown in the marker.
	FullBytes  int
	Failed     bool
	CreatedAt  time.Time
	OutputText widget.Selectable
	CopyButton widget.Clickable
}

type consoleSuggestion struct {
	Label  string
	Insert string
}

// peerCardSelectables holds widget.Selectable instances for each text field
// in a peer health card, enabling mouse text selection and copy.
type peerCardSelectables struct {
	Address       widget.Selectable
	Version       widget.Selectable
	Meta          widget.Selectable
	Error         widget.Selectable
	RecordingInfo widget.Selectable
}

type consoleDonateEntry struct {
	Label      string
	Address    string
	Text       widget.Selectable
	Scroll     widget.List
	CopyButton widget.Clickable
}

// consoleModal is the console: a modal window drawn over the main window on
// the modal shell (modal_shell.go), not a window of its own.
//
// It used to own an app.Window, an event loop goroutine, a theme and a
// touchKeyboardState, all because it was a second native window. As a modal it
// borrows every one of those from parent — same goroutine, same text shaper,
// same on-screen keyboard — and what is left here is console state: the
// command history, the tabs, the traffic samples and the per-row widgets.
//
// One instance is created on the first open and reused for the rest of the
// process (Window.consoleModal), so command history and the selected tab
// survive closing the modal. Everything with a cost that should not outlive
// the modal — the ebus subscriptions and the traffic ticker — is started on
// open and stopped on close.
type consoleModal struct {
	parent *Window
	// visible is whether the console modal is on screen.
	//
	// Atomic because the traffic ticker goroutine reads it once a second to
	// decide whether it still has an audience, while the UI goroutine writes
	// it from the click handlers. Every other consoleModal field that crosses
	// goroutines is either atomic (activeTab) or under c.mu; this one is read
	// on the layout path of every frame, where taking c.mu for a bool would be
	// the only lock on that path.
	visible atomic.Bool
	// focusPending asks the next laid-out frame to move the keyboard onto this
	// tab's focus target. Set when the modal opens and when the tab changes;
	// see claimFocus.
	focusPending bool
	// focusRing is used for ONE half of its contract: handing the keyboard back
	// to the Console button when the modal closes. Its Tab containment is not
	// used — nothing calls drive. Containment comes from Window.layout
	// disabling the window underneath, which is what keeps every control
	// INSIDE the console reachable; a ring would have had to enumerate them,
	// and the console's tabs carry per-row buttons that no list can track.
	focusRing menuFocusState
	// closeButton and dismissTag are this content's half of the modal shell
	// around it.
	closeButton widget.Clickable
	dismissTag  struct{}
	// touchKbdTag is the pointer tag of the console editor's
	// touch-keyboard area. The state it feeds is the PARENT's — there is one
	// on-screen keyboard and one window for it to overlap.
	touchKbdTag     int8
	peerList        widget.List
	peerSectionList widget.List
	peerSelectables map[string]*peerCardSelectables // keyed by peer address; lazily created
	historyList     widget.List
	// suggestList scrolls the completion popup. The list is capped at
	// maxSuggestions rows and the panel hugs them, so it scrolls only when the
	// window is too short to show them all — which is exactly when arrow
	// navigation would otherwise walk onto a row of zero height.
	suggestList widget.List
	donateList  widget.List
	fileList    widget.List

	// peerRows memoizes per-frame-derived peers/info-tab data so the O(peers)
	// derivations run on state change instead of on every frame. The peers
	// tab redraws at 60 fps while peers are connected (layoutPeersTab) and
	// each derivation allocated fresh — the active-rows merge was the top
	// desktop-side allocator in the heap-churn profile (activePeerHealth
	// ~240 MB).
	//
	// Two independently-keyed parts so the info tab (counts only) never pays
	// for the active-rows merge:
	//   - counts (connected/unique) — both tabs read these;
	//   - activeRows — the filtered+merged slice, peers tab only.
	//
	// Cache key is RouterSnapshot.Generation. NOTE this is bumped on EVERY
	// DMRouter state mutation — DM/sidebar/beep events included (UIEventBeep
	// bumps it even though it carries no data change) — not only on
	// peer-status changes. So a burst of incoming messages can invalidate
	// this cache even when the peer set is unchanged; that only costs a
	// recompute of cheap O(peers) derivations on the visible tab, never a
	// correctness problem (an unchanged generation still guarantees
	// identical inputs). Read and written only on the UI goroutine, no lock.
	peerRows peerRowCache

	// File-tab per-row Clickables, keyed by FileID. Created lazily
	// on layout and garbage-collected by pruneFileTabButtons when
	// the FileID disappears from the snapshot.
	//
	// We own dedicated maps for Delete / Download / Restart / Thumb
	// (instead of reusing the chat-thread *Window's maps) so the
	// click handlers can `defer c.invalidateWindow()` after the
	// async StartDownload / RestartDownload settles. Without that
	// guarantee, clicking Restart on a terminal "failed" row leaves
	// the file tab showing the old state until an unrelated event
	// triggers a redraw — the polled timer is gated on
	// hasActiveFileTransfer, which the click hasn't yet flipped on.
	//
	// Cancel and Show-in-folder/Open are routed through the
	// chat-thread *Window methods because their post-click state is
	// always polled-active or terminal-with-disk-actions-only, so
	// the missing invalidate doesn't manifest.
	fileDeleteButtons   map[domain.FileID]*widget.Clickable
	fileDownloadButtons map[domain.FileID]*widget.Clickable
	fileRestartButtons  map[domain.FileID]*widget.Clickable
	fileThumbButtons    map[domain.FileID]*widget.Clickable

	// fileRowSelectables holds widget.Selectable instances per file
	// row so the user can click-drag to highlight and Cmd/Ctrl-C
	// the filename, peer identity, and meta line (size + timestamp).
	// Same lazy-allocate / prune pattern as the Clickable maps.
	fileRowSelectables map[domain.FileID]*fileRowSelectables

	consoleEditor widget.Editor
	runButton     widget.Clickable
	// tabButtons holds one Clickable per tab, created on demand. A map rather
	// than six named fields because every use — the strip, the More menu, the
	// click handlers — walks the tabs as a list, and six parallel branches is
	// how the strip ended up unable to fit a phone in the first place.
	tabButtons map[consoleTab]*widget.Clickable
	// tabMenuButton opens the "More" menu that holds the tabs the compact
	// strip has no room for; tabMenuOpen is whether that menu is showing.
	//
	// tabMenuAnchor is that button's rectangle on the strip, recorded by
	// layoutTabs so the dropdown can hang from its right edge. It is written
	// and read in the same frame, strip before menu, so it is never stale.
	//
	// tabMenuDismissTag catches the press that closes an open menu without
	// picking anything from it.
	tabMenuButton     widget.Clickable
	tabMenuOpen       bool
	tabMenuAnchor     image.Rectangle
	tabMenuDismissTag struct{}
	tabMenuList       widget.List
	activeTab         int32     // consoleTab value; accessed atomically (UI writes, ticker reads)
	trafficSamplesIn  []float32 // per-second received bytes/s (newest last)
	trafficSamplesOut []float32 // per-second sent bytes/s (newest last)
	trafficTotalSent  int64     // cumulative sent (for totals display)
	trafficTotalRecv  int64     // cumulative received (for totals display)
	trafficLastTS     string    // RFC3339 timestamp of the newest applied collector sample (incremental-fetch cursor; "" = none yet)
	trafficTicker     *time.Ticker
	mu                sync.RWMutex
	consoleEntries    []consoleEntry
	consoleBusy       bool
	suggestButtons    map[string]*widget.Clickable
	lastSuggestQuery  string
	hideSuggestions   bool
	selectedSuggest   int
	suggestBaseQuery  string
	suggestSnapshot   []consoleSuggestion
	cachedCommands    []consoleSuggestion // loaded from CommandTable at init
	// commandHistory is the chronological ring of submitted commands;
	// commandHistory[0] is the oldest, commandHistory[len-1] the most recent.
	// Up/Down arrows in the editor walk this ring when no completion
	// suggestions are visible.
	commandHistory []string
	// historyCursor is the index in commandHistory currently shown in the
	// editor. When the user is not navigating history it equals
	// len(commandHistory) (one past the end), and historyDraft is empty.
	historyCursor int
	// historyDraft preserves the text the user had typed at the moment they
	// began history navigation, so Down past the most recent entry restores it.
	historyDraft string
	// historyText is the value last written into the editor by history
	// navigation. Used to detect manual edits — once the editor text diverges
	// from this, navigation state is reset on the next frame.
	historyText         string
	donateEntries       []consoleDonateEntry
	donateLink          widget.Selectable
	donateLinkButton    widget.Clickable
	stopRecordingButton widget.Clickable
	// overflow owns the temp files carrying full command outputs whose
	// display text was capped (console_output.go).
	overflow            *consoleOverflowStore
	ebusSubscriptions   []ebus.SubscriptionID // cleaned up on close to prevent handler leak
	uptimeInvalidating  int32                 // atomic flag; coalesces uptime redraw requests
	fileTabInvalidating int32                 // atomic flag; coalesces file-tab redraw requests during active transfers
}

func newConsoleModal(parent *Window) *consoleModal {
	console := &consoleModal{
		parent: parent,
		peerList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		peerSectionList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		historyList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		suggestList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		donateList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		fileList: widget.List{
			List: layout.List{Axis: layout.Vertical},
		},
		fileDeleteButtons:   make(map[domain.FileID]*widget.Clickable),
		fileDownloadButtons: make(map[domain.FileID]*widget.Clickable),
		fileRestartButtons:  make(map[domain.FileID]*widget.Clickable),
		fileThumbButtons:    make(map[domain.FileID]*widget.Clickable),
		fileRowSelectables:  make(map[domain.FileID]*fileRowSelectables),
		peerSelectables:     make(map[string]*peerCardSelectables),
		suggestButtons:      make(map[string]*widget.Clickable),
		selectedSuggest:     -1,
		donateEntries:       newConsoleDonateEntries(),
		overflow:            newConsoleOverflowStore(),
	}
	console.consoleEditor.SingleLine = true
	console.donateLink.SetText(consoleDonateURL)
	console.consoleEntries = []consoleEntry{
		newConsoleEntry(consoleEntry{
			Command:   "help",
			Output:    parent.t("console.welcome"),
			CreatedAt: time.Now(),
		}),
	}

	// Load available commands directly from CommandTable — no HTTP, always available.
	if parent.cmdTable != nil {
		console.loadCommands()
	}

	return console
}

// console returns the one console instance, creating it on first use.
//
// Lazily, because most sessions never open the console and building it loads
// the whole command table; once, because closing the modal must not throw away
// the command history the user is working through.
func (w *Window) console() *consoleModal {
	if w.consoleModal == nil {
		w.consoleModal = newConsoleModal(w)
	}
	return w.consoleModal
}

// consoleModalVisible reports whether the console modal is on screen. It
// tolerates a console that was never opened, which is the normal case.
func (w *Window) consoleModalVisible() bool {
	return w.consoleModal != nil && w.consoleModal.visible.Load()
}

// openConsoleModal shows the console over the main window and starts the
// subscriptions that keep it current.
//
// It closes every other overlay first, for the same reason openIdentityPanel
// does: these are modal surfaces and two of them stacked leave the user with a
// dismissal order nothing on screen explains.
func (w *Window) openConsoleModal(gtx layout.Context) {
	console := w.console()
	if console.visible.Load() {
		return
	}
	w.closeOverlaysForModal(gtx)
	console.visible.Store(true)
	console.focusPending = true
	console.focusRing.open(&w.consoleButton)
	// held is what restoreOnClose checks before handing focus back, and drive
	// is what would normally set it. Nothing drives this ring, so the modal
	// claims the focus here: it does take it (claimFocus), which is exactly
	// what held means.
	console.focusRing.held = true
	console.subscribeConsoleEvents()
	// The Traffic tab may be the one restored from the previous open, and its
	// ticker was stopped when the modal closed.
	console.startTrafficTickerIfShowing()
	w.invalidate()
}

// claimFocus moves the keyboard into the console on the frame after it opens.
//
// It has to happen, and it has to happen here. Gio leaves focus where it was,
// which is the message composer the modal is now covering: everything the user
// types goes to a contact they cannot see, and Enter SENDS it instead of
// running the command. The command line is the right target — it is what the
// console is for, and it is laid out on every frame of every tab.
//
// A flag consumed during layout rather than a FocusCmd at the click, because
// gtx.Execute is dropped by the measuring passes a frame may run before the
// real one (see keyboardYieldingChrome), and the console editor is not in the
// frame yet when the button that opened the modal is clicked.
func (c *consoleModal) claimFocus(gtx layout.Context) {
	if !c.focusPending {
		return
	}
	c.focusPending = false
	target := c.focusTarget()
	if target == &c.consoleEditor {
		c.parent.touchKbd.noteExplicitEditorFocus()
	}
	gtx.Execute(key.FocusCmd{Tag: target})
}

// focusTarget is where the keyboard goes when the console opens, or when the
// tab changes under it.
//
// The command line only exists on the Console tab. The selected tab survives a
// close, so the console can reopen on Peers or Donate — and focusing a widget
// that is not in the frame is the same as focusing nothing: Gio drops it at
// Frame time and the user is left with no focus anywhere. The close button is
// in the header of every tab, so it is the fallback.
func (c *consoleModal) focusTarget() event.Tag {
	if c.currentTab() == consoleTabConsole {
		return &c.consoleEditor
	}
	return &c.closeButton
}

// escapeConsoleModal backs out one layer of the console: the completion popup
// or the More menu first, the modal itself once neither is open.
//
// Escape and system Back share it. They used to disagree — Escape stepped out
// of the inner surface while Back closed the whole console from inside an open
// menu — and a user on Android had no way to dismiss the menu alone.
func (w *Window) escapeConsoleModal(gtx layout.Context) {
	if !w.consoleModalVisible() {
		return
	}
	if w.consoleModal.dismissInnerSurface(gtx) {
		w.invalidate()
		return
	}
	w.closeConsoleModal()
}

// dismissInnerSurface closes the topmost thing open INSIDE the console and
// reports whether there was one.
//
// The More menu comes first because it covers the completion popup's own
// anchor. dismissSuggestions hands focus back to the command line as it closes,
// which is why this wants a layout context and why Back is routed through
// handleBackNavigation's rather than given a context-free path of its own.
func (c *consoleModal) dismissInnerSurface(gtx layout.Context) bool {
	if c.tabMenuOpen {
		c.tabMenuOpen = false
		return true
	}
	return c.dismissSuggestions(gtx)
}

// closeConsoleModal hides the console and releases what only a visible console
// needs. The console itself, and everything the user typed into it, stays.
//
// The traffic ticker is not stopped here: it stops itself on its next tick,
// which is when it re-reads trafficViewVisible. Reaching in to stop it from
// this goroutine would race the goroutine that owns it.
func (w *Window) closeConsoleModal() {
	if !w.consoleModalVisible() {
		return
	}
	w.consoleModal.visible.Store(false)
	w.consoleModal.closeInnerSurfaces()
	w.consoleModal.unsubscribeConsoleEvents()
	w.invalidate()
}

// closeInnerSurfaces puts away everything open INSIDE the console. Nothing
// open inside it survives its close: the More menu came back over a tab the
// user had not chosen, and the completion popup came back over a command they
// had stopped typing.
//
// What the user TYPED is not an open surface and stays, like the command
// history does.
func (c *consoleModal) closeInnerSurfaces() {
	c.tabMenuOpen = false
	c.restoreTypedQuery()
	c.hideSuggestionsUntilRetyped()
}

// shutdown releases what outlives every close: the temp files holding command
// output too large to display. They live exactly as long as the entries that
// point at them, and those now live as long as the process.
func (c *consoleModal) shutdown() {
	c.unsubscribeConsoleEvents()
	c.overflow.removeAll()
}

// closeOverlaysForModal dismisses whatever else is open before a modal takes
// over the window. Mirrors the block in openIdentityPanel.
func (w *Window) closeOverlaysForModal(gtx layout.Context) {
	w.showLanguageMenu = false
	w.contextMenuPeer = domain.PeerIdentity{}
	w.showDeleteConfirm = false
	w.showClearChatConfirm = false
	w.showAliasEditor = false
	w.msgContextMsg = nil
	// The emoji picker is non-modal and would otherwise stay open, and live,
	// under a modal that covers it. Through its own close, not by clearing the
	// flag: that path also drops the search query and the grid offset, and
	// settles what it owes the on-screen keyboard. Clearing the flag alone
	// reopened the picker filtered by a query the user had forgotten typing.
	w.closeEmojiPicker(gtx)
	w.peerMenuFocus.abandonRestore()
	w.msgMenuFocus.abandonRestore()
	w.closeIdentityPanel()
}

// layoutConsoleOverlay draws the console modal.
func (w *Window) layoutConsoleOverlay(gtx layout.Context) layout.Dimensions {
	console := w.consoleModal
	console.handleCloseButton(gtx)
	return w.kit().Modal(gtx, ui.Modal{
		Title:        w.t("console.title"),
		CloseHint:    w.t("console.close"),
		Close:        &console.closeButton,
		DismissTag:   &console.dismissTag,
		Dismiss:      w.closeConsoleModal,
		CornerRadius: unit.Dp(ui.ModalCardRadiusDp),
		Sizing:       ui.ModalSizingInset,
		Compact:      w.isCompactLayout(gtx),
		Content:      keyboardTailOwner(&w.touchKbd, console.layoutContent),
	})
}

// handleCloseButton drains the header close button.
//
// It runs BEFORE the shell lays that button out, and the order is the whole
// point: Clickable.Layout drains the click queue itself, so a drain that comes
// after it — from layoutContent, say, which the shell reaches only after the
// header — finds nothing and the button does nothing. That is what broke it
// for mouse, touch and Enter alike.
//
// It lives here rather than in Window.handleActions because that function
// stops at its first lines while the modal is open: reading a background
// widget's clicks is what makes it Tab-reachable, and the window's controls
// must not be.
func (c *consoleModal) handleCloseButton(gtx layout.Context) {
	for c.closeButton.Clicked(gtx) {
		c.parent.closeConsoleModal()
	}
}

// theme is the parent window's theme. The console used to build its own,
// because it ran on its own goroutine and Gio's text shaper is not safe to
// share across them. A modal is laid out by the window's own event loop, so
// that reason is gone and a second shaper would only mean a second font cache
// and two sets of measurements for the same strings.
func (c *consoleModal) theme() *material.Theme {
	return c.parent.theme
}

// touchDrivenInput reports whether the most recent press came from a finger,
// recently enough to act on. The presses themselves are recorded by the parent
// window's cursor tracker — the console no longer has a window to track them in.
func (c *consoleModal) touchDrivenInput(gtx layout.Context) bool {
	return c.parent.touchDrivenInput(gtx)
}

// layoutContent fills the modal shell's card below the header: the tab strip
// and the active tab.
//
// The title and the close button are the shell's (modal_shell.go), and so is
// the card padding — which is why the 4/6dp window margin this used to apply
// is gone.
func (c *consoleModal) layoutContent(gtx layout.Context) layout.Dimensions {
	c.claimFocus(gtx)
	c.handleEscape(gtx)
	c.handleActions(gtx)
	c.parent.touchKbd.trackEditorFocus(gtx, gtx.Focused(&c.consoleEditor))

	// Same shape as the main window: the keyboard inset goes on the tab
	// content rather than on the card padding, and the tab strip with its
	// spacer yields when the free strip cannot hold them and an input row
	// both. The console leans on the yield even harder than the composer
	// does — layoutConsoleTab puts the input at the TOP of the tab, so
	// bottom padding structurally cannot move it and dropping the strip
	// above it is the only thing that raises it at all. The inset still
	// earns its place there by keeping the history below the input out from
	// under the keyboard.
	strip := consoleTabStripFor(c.parent.isCompactLayout(gtx), c.currentTab())
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return keyboardYieldingChrome(gtx, &c.parent.touchKbd, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
					layout.Rigid(c.layoutTabs),
					layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				)
			})
		}),
		layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			content := layout.Inset{
				Bottom: keyboardInsetDp(gtx, &c.parent.touchKbd),
			}
			// The open More menu hangs over the tab below it rather than
			// pushing it down: a menu that reflowed the tab would move the
			// console input out from under the finger that opened it.
			menuOpen := c.tabMenuOpen && len(strip.Menu) > 0
			return layout.Stack{Alignment: layout.NW}.Layout(gtx,
				layout.Expanded(func(gtx layout.Context) layout.Dimensions {
					return content.Layout(gtx, c.layoutActiveTab)
				}),
				layout.Expanded(func(gtx layout.Context) layout.Dimensions {
					if !menuOpen {
						return layout.Dimensions{}
					}
					return c.layoutTabMenuDismissLayer(gtx)
				}),
				layout.Stacked(func(gtx layout.Context) layout.Dimensions {
					if !menuOpen {
						return layout.Dimensions{}
					}
					return c.layoutTabMenu(gtx, strip)
				}),
			)
		}),
	)
}

func (c *consoleModal) handleActions(gtx layout.Context) {
	c.syncSuggestionVisibility()
	c.syncHistoryNavigation()
	suggestions := c.consoleSuggestions()

	c.handleTabActions(gtx, c.parent.isCompactLayout(gtx))

	for c.donateLinkButton.Clicked(gtx) {
		go func() {
			_ = openExternalURL(consoleDonateURL)
		}()
	}
	for c.runButton.Clicked(gtx) {
		c.submitConsoleCommand()
	}
	for _, item := range suggestions {
		btn := c.suggestionButton(item.Label)
		for btn.Clicked(gtx) {
			c.applySuggestion(gtx, item.Insert)
			// Raise the keyboard ONLY when the suggestion was TAPPED. A focused
			// suggestion button also activates on Return/Space, which records no
			// press in History (pointerClickedThisFrame is false) — without this
			// gate a hardware key within touchInputRecency of an earlier touch
			// would pop the keyboard while the user types on real keys. The
			// keyboard-driven picks (Tab/Enter/Escape/RightArrow) reach
			// applySuggestion & co. through key handlers.
			if pointerClickedThisFrame(btn, gtx) && c.touchDrivenInput(gtx) {
				showTouchKeyboard(&c.parent.touchKbd)
			}
		}
	}

	for {
		ev, ok := gtx.Event(
			key.Filter{Focus: &c.consoleEditor, Name: key.NameDownArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameUpArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameRightArrow},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameTab},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameEnter},
			key.Filter{Focus: &c.consoleEditor, Name: key.NameReturn},
		)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		switch ke.Name {
		case key.NameDownArrow:
			if len(suggestions) > 0 {
				c.moveSuggestionSelection(1, suggestions)
			} else {
				c.navigateHistory(1)
			}
			continue
		case key.NameUpArrow:
			if len(suggestions) > 0 {
				c.moveSuggestionSelection(-1, suggestions)
			} else {
				c.navigateHistory(-1)
			}
			continue
		case key.NameRightArrow:
			if c.commitSuggestionForArguments(gtx, suggestions) {
				continue
			}
		case key.NameTab:
			if c.applySelectedSuggestion(gtx, suggestions, true) {
				continue
			}
		case key.NameEnter, key.NameReturn:
			if c.selectedSuggest >= 0 && len(suggestions) > 0 {
				c.submitConsoleCommand()
				continue
			}
			if c.applySelectedSuggestion(gtx, suggestions, false) {
				continue
			}
		}
		c.submitConsoleCommand()
	}
}

// handleEscape applies Escape to the console: back out of the completion popup
// or the More menu first, close the modal once neither is open.
//
// The filter carries no Focus target, for the same reason readMenuNavKeys does
// not: Escape has to reach the console wherever focus happens to sit, which is
// the command line most of the time but a Copy button or a file action after a
// Tab.
func (c *consoleModal) handleEscape(gtx layout.Context) {
	for {
		ev, ok := gtx.Event(key.Filter{Name: key.NameEscape})
		if !ok {
			return
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		c.parent.escapeConsoleModal(gtx)
	}
}

// consoleTabOrder is the tabs left to right. It is the single list every part
// of the tab machinery walks — the strip, the More menu and the click
// handlers — so a tab can never be drawn somewhere it cannot be clicked.
func consoleTabOrder() []consoleTab {
	return []consoleTab{
		consoleTabConsole,
		consoleTabPeers,
		consoleTabTraffic,
		consoleTabFile,
		consoleTabInfo,
		consoleTabDonate,
	}
}

var consoleTabLabelKeys = map[consoleTab]string{
	consoleTabConsole: "console.tab.console",
	consoleTabPeers:   "console.tab.peers",
	consoleTabTraffic: "console.tab.traffic",
	consoleTabFile:    "console.tab.file",
	consoleTabInfo:    "console.tab.info",
	consoleTabDonate:  "console.tab.donate",
}

func (c *consoleModal) tabLabel(tab consoleTab) string {
	return c.parent.t(consoleTabLabelKeys[tab])
}

// tabButton returns one tab's Clickable, creating it on first use. The tabs
// are a fixed set, so the map never grows past six entries and needs no
// pruning.
func (c *consoleModal) tabButton(tab consoleTab) *widget.Clickable {
	if c.tabButtons == nil {
		c.tabButtons = make(map[consoleTab]*widget.Clickable, len(consoleTabOrder()))
	}
	if button, ok := c.tabButtons[tab]; ok {
		return button
	}
	button := new(widget.Clickable)
	c.tabButtons[tab] = button
	return button
}

// consoleTabStrip is what the tab strip shows at one width.
type consoleTabStrip struct {
	// Visible are the tabs with a button of their own, left to right.
	Visible []consoleTab
	// Menu are the tabs folded behind the "More" button, empty when they all
	// fit on the strip.
	Menu []consoleTab
	// MenuActive is the folded tab that is currently selected and
	// MenuHasActive whether there is one. When there is, its name labels the
	// More button, so the strip still says where the user is instead of
	// showing four unselected tabs and a menu.
	//
	// The two fields are separate because consoleTab zero is a real tab
	// (Console) and cannot double as "none".
	MenuActive    consoleTab
	MenuHasActive bool
}

// consoleTabStripFor decides how many tabs the strip can show. Six of them
// need about 700dp and a phone has 360—420, so below the breakpoint the tail
// folds into a menu rather than running off the edge of the screen, which is
// what it used to do.
func consoleTabStripFor(compact bool, active consoleTab) consoleTabStrip {
	all := consoleTabOrder()
	if !compact {
		return consoleTabStrip{Visible: all}
	}

	strip := consoleTabStrip{
		Visible: all[:consoleVisibleTabs],
		Menu:    all[consoleVisibleTabs:],
	}
	for _, tab := range strip.Menu {
		if tab == active {
			strip.MenuActive = tab
			strip.MenuHasActive = true
			break
		}
	}
	return strip
}

// selectTab switches tabs and puts the More menu away — a tab picked from the
// menu is the menu's job done.
func (c *consoleModal) selectTab(tab consoleTab) {
	previous := c.currentTab()
	atomic.StoreInt32(&c.activeTab, int32(tab))
	c.tabMenuOpen = false
	// Leaving or entering the Console tab takes the command line out of the
	// frame or puts it back, and focus has to move with it — see focusTarget.
	if (previous == consoleTabConsole) != (tab == consoleTabConsole) {
		c.focusPending = true
	}
	if tab == consoleTabTraffic {
		c.startTrafficTicker()
	}
}

// handleTabActions runs the tab strip's click handlers. Every tab is handled,
// including the ones the compact strip has folded away: their buttons are laid
// out by the More menu, and a Clickable whose clicks nobody drains keeps them
// queued for whenever it is next asked.
func (c *consoleModal) handleTabActions(gtx layout.Context, compact bool) {
	for _, tab := range consoleTabOrder() {
		for c.tabButton(tab).Clicked(gtx) {
			c.selectTab(tab)
		}
	}
	for c.tabMenuButton.Clicked(gtx) {
		c.tabMenuOpen = !c.tabMenuOpen
	}
	// A window widened past the breakpoint puts every tab back on the strip,
	// and the menu button goes with them.
	if !compact {
		c.tabMenuOpen = false
	}
	if c.focusPending {
		// claimFocus already ran this frame, with the old tab. The frame that
		// will act on the move has to be asked for: a focus DROP is a state
		// change, not an event anybody filters for, so a tab switched by
		// keyboard would otherwise leave focus on nothing until the user
		// produced some unrelated input.
		gtx.Execute(op.InvalidateCmd{})
	}
}

// layoutTabs draws the strip and records where the "More" slot ended up, so
// the dropdown can hang directly under it.
//
// The row is placed by hand rather than by layout.Flex because Flex reports
// only the total size, and the menu needs the x of one particular child. Doing
// it here also keeps the pills at their natural width instead of the equal
// shares a Flexed layout would give them.
func (c *consoleModal) layoutTabs(gtx layout.Context) layout.Dimensions {
	strip := consoleTabStripFor(c.parent.isCompactLayout(gtx), c.currentTab())
	gap := gtx.Dp(unit.Dp(consoleTabGapDp))

	// Each pill is measured at its natural size. The minimum has to be dropped
	// explicitly: this is laid out as a Rigid child of a vertical Flex, which
	// passes the container's CROSS-axis minimum straight through, and
	// material.Clickable sizes itself to the minimum it is given — so every
	// pill would come out as wide as the whole card.
	childGtx := gtx
	childGtx.Constraints.Min = image.Point{}

	x, height := 0, 0
	place := func(w layout.Widget) {
		macro := op.Record(gtx.Ops)
		dims := w(childGtx)
		call := macro.Stop()
		offset := op.Offset(image.Pt(x, 0)).Push(gtx.Ops)
		call.Add(gtx.Ops)
		offset.Pop()
		x += dims.Size.X + gap
		height = max(height, dims.Size.Y)
	}

	for _, tab := range strip.Visible {
		place(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabButton(gtx, c.tabButton(tab), c.currentTab() == tab, c.tabLabel(tab))
		})
	}
	if len(strip.Menu) > 0 {
		left := x
		place(func(gtx layout.Context) layout.Dimensions {
			return c.layoutTabMenuButton(gtx, strip)
		})
		// x has already moved past the slot and its trailing gap.
		c.tabMenuAnchor = image.Rect(left, 0, x-gap, height)
	}
	if x > 0 {
		x -= gap // the trailing gap belongs to no child
	}
	return layout.Dimensions{Size: image.Pt(x, height)}
}

// layoutTabMenuDismissLayer is the popup's backdrop, scoped to the console
// card: a press anywhere but on the menu puts it away instead of reaching the
// tab underneath. The menu itself is Stacked ABOVE this layer, so a press on
// one of its items still reaches the item.
//
// Without it the only ways out of an accidentally opened menu are picking a
// tab from it or Escape, and the press that would naturally dismiss it — the
// one aimed at whatever the menu is covering — silently does something else.
func (c *consoleModal) layoutTabMenuDismissLayer(gtx layout.Context) layout.Dimensions {
	return c.parent.kit().MenuPopupBackdrop(gtx, &c.tabMenuDismissTag, ui.MenuPopupScrimDim, func() {
		c.tabMenuOpen = false
	})
}

// layoutTabMenuButton draws the "More ∨" slot. It reads as the selected tab
// when the selection is inside the menu, which is what keeps the strip honest
// about where the user is.
func (c *consoleModal) layoutTabMenuButton(gtx layout.Context, strip consoleTabStrip) layout.Dimensions {
	label := c.parent.t("console.tab.more")
	if strip.MenuHasActive {
		label = c.tabLabel(strip.MenuActive)
	}
	return c.layoutTabPill(gtx, &c.tabMenuButton, consoleTabPill{
		Label: label,
		// A real chevron glyph from the icon set, not the "∨" character the
		// first cut used. That character is a mathematical operator: it sits
		// on the text baseline at text size, so it rendered larger and higher
		// than the design's 16dp icon and made the slot taller than the tabs
		// beside it.
		Icon:   c.parent.chevronDownIcon,
		Active: strip.MenuHasActive,
	})
}

// layoutTabMenu draws the open More menu: a panel of the folded tabs hanging
// under the slot that opened it, over the tab content rather than pushing it
// down — a menu that reflowed the tab would move the console input out from
// under the finger that just opened it.
//
// It is laid out into the FULL content area and positions itself with an
// offset, because the panel has to line up with a button in a different part
// of the tree (the anchor recorded by layoutTabs) and to be free to hang past
// the bottom of anything it is nested in.
func (c *consoleModal) layoutTabMenu(gtx layout.Context, strip consoleTabStrip) layout.Dimensions {
	macro := op.Record(gtx.Ops)
	dims := c.layoutTabMenuPanel(gtx, strip)
	panel := macro.Stop()

	// Right-aligned with the slot that opened it, as the design has it: the
	// "More" slot is the LAST thing on the strip, so aligning its left edge
	// pushes the card off the card's own right edge as soon as the menu is
	// wider than the slot.
	x := ui.MenuPopupAnchorX(c.tabMenuAnchor.Max.X-dims.Size.X, dims.Size.X, gtx.Constraints.Max.X)
	offset := op.Offset(image.Pt(x, 0)).Push(gtx.Ops)
	panel.Add(gtx.Ops)
	offset.Pop()
	return dims
}

// tabMenuItems turns the folded tabs into popup rows.
func (c *consoleModal) tabMenuItems(strip consoleTabStrip) []ui.MenuPopupItem {
	items := make([]ui.MenuPopupItem, 0, len(strip.Menu))
	for _, tab := range strip.Menu {
		items = append(items, ui.MenuPopupItem{
			Label:    c.tabLabel(tab),
			Button:   c.tabButton(tab),
			Selected: c.currentTab() == tab,
		})
	}
	return items
}

// layoutTabMenuPanel is the shared popup card (menu_popup.go), sized to its
// content: the folded tabs are one word each, and the language menu's fixed
// 220dp would cover most of a phone-width tab strip.
func (c *consoleModal) layoutTabMenuPanel(gtx layout.Context, strip consoleTabStrip) layout.Dimensions {
	gtx.Constraints.Min = image.Point{}
	return c.parent.kit().MenuPopupCard(gtx, ui.MenuPopup{
		Items:  c.tabMenuItems(strip),
		Scroll: &c.tabMenuList,
		Width:  ui.MenuPopupWidthFit,
	})
}

// consoleTabPill describes one pill on the tab strip.
type consoleTabPill struct {
	// Label is the tab's name.
	Label string
	// Icon trails the label; nil on a plain tab. Only the "More" slot has one.
	Icon *widget.Icon
	// Active paints the pill as the selected tab.
	Active bool
}

func (c *consoleModal) layoutTabButton(gtx layout.Context, clickable *widget.Clickable, active bool, labelText string) layout.Dimensions {
	return c.layoutTabPill(gtx, clickable, consoleTabPill{Label: labelText, Active: active})
}

func (c *consoleModal) layoutTabPill(gtx layout.Context, clickable *widget.Clickable, pill consoleTabPill) layout.Dimensions {
	fg := color.NRGBA{R: 0xdc, G: 0xe4, B: 0xf0, A: 255}
	if pill.Active {
		fg = ui.ChipActiveLabel()
	}

	// The design gives the icon slot 1dp less horizontal padding than a plain
	// tab, so the two come out the same visual weight despite the glyph's own
	// side bearings.
	padX := unit.Dp(consoleTabPaddingXDp)
	if pill.Icon != nil {
		padX = unit.Dp(consoleTabIconPaddingXDp)
	}

	return c.parent.kit().Chip(gtx, clickable, ui.ChipFill(pill.Active), unit.Dp(consoleTabRadiusDp), func(gtx layout.Context) layout.Dimensions {
		return layout.Inset{
			Top: unit.Dp(consoleTabPaddingYDp), Bottom: unit.Dp(consoleTabPaddingYDp),
			Left: padX, Right: padX,
		}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			text := layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Label(c.theme(), unit.Sp(consoleTabTextSp), pill.Label)
				label.Color = fg
				label.Font.Weight = 600
				label.MaxLines = 1
				return label.Layout(gtx)
			})
			if pill.Icon == nil {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, text)
			}
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				text,
				layout.Rigid(layout.Spacer{Width: unit.Dp(consoleTabIconGapDp)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return ui.Icon(gtx, pill.Icon, unit.Dp(consoleTabIconDp), fg)
				}),
			)
		})
	})
}

func (c *consoleModal) layoutActiveTab(gtx layout.Context) layout.Dimensions {
	// The full snapshot, not just NodeStatus: the peers/info tabs key their
	// per-frame derived-data cache on snap.Generation.
	//
	// It comes from the parent rather than from the router directly. The
	// console used to be its own window with its own frame loop and had to ask
	// for its own; drawn inside the main window's frame it must see the SAME
	// snapshot as everything else in that frame, or the console can show a peer
	// count one generation ahead of the contact list beside it.
	snap := c.parent.snap
	switch c.currentTab() {
	case consoleTabPeers:
		return c.layoutPeersTab(gtx, snap)
	case consoleTabTraffic:
		return c.layoutTrafficTab(gtx)
	case consoleTabFile:
		return c.layoutFileTab(gtx)
	case consoleTabInfo:
		return c.layoutInfoTab(gtx, snap)
	case consoleTabDonate:
		return c.layoutDonateTab(gtx)
	default:
		return c.layoutConsoleTab(gtx)
	}
}

func (c *consoleModal) infoRows(snap service.RouterSnapshot) []string {
	status := snap.NodeStatus
	// Both counters consume the full NodeStatus: orphan CaptureSessions count
	// as connected/known peers during the capture-start race, matching the
	// peers-tab liveness contract enshrined by activeRowsForTab. peerCounts
	// shares the counts cache with the peers tab and — unlike the old
	// inline path — never builds the active-rows merge the info tab has no
	// use for.
	connectedPeers, uniquePeers := c.peerCounts(snap)
	// Pre-probe NodeStatus has ProtocolVersion = 0 because the first welcome
	// has not yet populated the field. Fall back to the runtime's compiled
	// value (config.ProtocolVersion) so the row never renders a misleading
	// "Protocol version: 0" — same fallback shape `node.listen` and
	// `node.type` use for their respective fields.
	protocolVersion := status.ProtocolVersion
	if protocolVersion == 0 {
		protocolVersion = c.parent.runtime.ProtocolVersion()
	}
	rows := []string{
		c.parent.t("node.client_version", c.parent.client.Version()),
		c.parent.t("node.protocol_version", protocolVersion),
		c.parent.t("node.listener", status.ListenerEnabled),
		c.parent.t("node.listen", fallback(status.ListenerAddress, c.parent.runtime.ListenAddress())),
		c.parent.t("node.type", fallback(status.NodeType, "full")),
		c.parent.t("node.services", fallback(joinOrNone(status.Services), "identity,contacts,messages,gazeta,relay")),
		c.parent.t("node.capabilities", fallback(joinOrNone(status.Capabilities), "none")),
		c.parent.t("node.connected", status.Connected),
		c.parent.t("node.known_peers", uniquePeers),
		c.parent.t("node.connected_peers", connectedPeers),
	}

	// Process memory + uptime — read from status.ResourceUsage, the
	// same NodeStatus snapshot every other Info-tab field comes from.
	// The figure is sampled off the render path by the monitor's
	// resource ticker (NodeStatusMonitor.RunResourceSampler) once a
	// second and pushed in via onChanged, so it ticks live without
	// this layout doing any work. nil before the first sample or on a
	// node too old to support the command, in which case the rows are
	// omitted.
	if usage := status.ResourceUsage; usage != nil {
		rows = append(rows,
			c.parent.t("node.memory_usage", usage.MemSysHuman, usage.MemHeapAllocHuman),
			c.parent.t("node.uptime", usage.UptimeHuman),
		)
	}

	rows = append(rows, c.parent.localNodeErrorRow())

	if !status.CheckedAt.IsZero() {
		rows = append(rows, c.parent.t("node.checked", status.CheckedAt.Format(time.RFC3339)))
	}

	return rows
}

func (c *consoleModal) layoutInfoTab(gtx layout.Context, snap service.RouterSnapshot) layout.Dimensions {
	return c.card(gtx, c.parent.t("console.info_title"), c.infoRows(snap))
}

func (c *consoleModal) layoutDonateTab(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		// 8dp panel padding matching the main window cards (window.go card).
		return layout.UniformInset(unit.Dp(8)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme(), unit.Sp(20), c.parent.t("console.donate_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					list := material.List(c.theme(), &c.donateList)
					return list.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
						return c.layoutDonateSection(gtx)
					})
				}),
			)
		})
	})
}

func (c *consoleModal) layoutPeersTab(gtx layout.Context, snap service.RouterSnapshot) layout.Dimensions {
	status := snap.NodeStatus
	// activeRowsForTab merges orphan CaptureSessions into the active-peer
	// set so the empty-state gate, summary, and section renderer all agree
	// on what "active" means during the capture-start race. The counter,
	// however, MUST report distinct peers (deduped by identity / address)
	// — not connection rows — to match the info-tab semantics. A peer with
	// multiple inbound conn_id rows, or one row plus an orphan capture for
	// the same identity, would inflate len(activePeers) relative to the
	// label's meaning.
	//
	// Both the merged rows and the connected count come from the
	// generation-keyed cache so this 60-fps path re-derives them only when
	// the snapshot actually changed (see activePeerRows / peerCounts).
	activePeers := c.activePeerRows(snap)
	connectedPeers, _ := c.peerCounts(snap)
	rows := []string{
		c.parent.t("node.connected_peers", connectedPeers),
	}

	// Schedule a redraw every second so the uptime counter stays fresh.
	// Uptime is computed from LastConnectedAt at render time — without
	// periodic invalidation it would freeze between ebus events.
	if len(activePeers) > 0 {
		c.scheduleUptimeInvalidate()
	}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		// 8dp panel padding matching the main window cards (window.go card).
		return layout.UniformInset(unit.Dp(8)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			title := material.Label(c.theme(), unit.Sp(20), c.parent.t("console.peers_title"))
			title.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

			summary := material.Body1(c.theme(), activePeerSummary(c.parent, activePeers))
			summary.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}

			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(title.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutInfoRows(gtx, rows)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
				layout.Rigid(summary.Layout),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					if len(activePeers) == 0 {
						label := material.Body1(c.theme(), c.parent.t("console.peers_empty"))
						label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
						return label.Layout(gtx)
					}
					return c.layoutActivePeersContent(gtx, activePeers, status.CaptureSessions)
				}),
			)
		})
	})
}

func (c *consoleModal) layoutConsoleTab(gtx layout.Context) layout.Dimensions {
	rows := []string{
		c.parent.t("console.help"),
	}
	// No title on this card: the modal shell's header already says "Console"
	// directly above it, and the tab strip in between names the tab.
	const title = ""

	// The console input is the FIRST row of this card, so what the keyboard
	// must not cover is everything from the card's top down through it: the
	// help line pushes the input down exactly as a header would. It is not
	// reachable as a widget from here — card builds it around whatever content
	// it is handed — so it is measured by laying the same card out around
	// nothing. That also counts the card's own bottom padding and the gap it
	// puts before its content, over-stating the tail by those few dp, in the
	// direction that retires the tab strip slightly early.
	keyboardMeasureTail(gtx, &c.parent.touchKbd, func(gtx layout.Context) layout.Dimensions {
		return c.card(gtx, title, rows, func(layout.Context) layout.Dimensions {
			return layout.Dimensions{}
		})
	})

	return c.card(gtx, title, rows, func(gtx layout.Context) layout.Dimensions {
		return layout.Stack{}.Layout(gtx,
			layout.Expanded(func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
					// The history below scrolls and may shrink to nothing;
					// this row may not.
					layout.Rigid(keyboardTailRow(&c.parent.touchKbd, c.layoutConsoleInput)),
					layout.Rigid(layout.Spacer{Height: unit.Dp(16)}.Layout),
					layout.Flexed(1, c.layoutConsoleHistory),
				)
			}),
			layout.Stacked(func(gtx layout.Context) layout.Dimensions {
				suggestions := c.consoleSuggestions()
				if len(suggestions) == 0 {
					return layout.Dimensions{}
				}
				return layout.Inset{Top: unit.Dp(66)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					gtx.Constraints.Max.X = min(gtx.Constraints.Max.X, gtx.Dp(unit.Dp(560)))
					return c.layoutConsoleSuggestions(gtx, suggestions)
				})
			}),
		)
	})
}

func (c *consoleModal) layoutConsoleInput(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(c.theme(), c.parent.t("console.input_label"))
			label.Color = color.NRGBA{R: 176, G: 187, B: 205, A: 255}
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					borderColor := color.NRGBA{R: 96, G: 114, B: 142, A: 255}
					backgroundColor := color.NRGBA{R: 25, G: 31, B: 40, A: 255}
					height := gtx.Dp(unit.Dp(54))
					return layout.Stack{}.Layout(gtx,
						layout.Expanded(func(gtx layout.Context) layout.Dimensions {
							gtx.Constraints.Min.Y = height
							gtx.Constraints.Max.Y = height
							ui.Fill(gtx, borderColor)
							return layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								ui.Fill(gtx, backgroundColor)
								return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
									editor := material.Editor(c.theme(), &c.consoleEditor, c.parent.t("console.placeholder"))
									editor.Color = color.NRGBA{R: 244, G: 247, B: 252, A: 255}
									editor.HintColor = color.NRGBA{R: 117, G: 130, B: 148, A: 255}
									return editorTouchKeyboardArea(gtx, &c.touchKbdTag, &c.parent.touchKbd, editor.Layout)
								})
							})
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := c.parent.t("console.run")
					if c.isConsoleBusy() {
						label = c.parent.t("console.running")
					}
					btn := material.Button(c.theme(), &c.runButton, label)
					if c.isConsoleBusy() {
						btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
					}
					return btn.Layout(gtx)
				}),
			)
		}),
	)
}

func (c *consoleModal) layoutConsoleSuggestions(gtx layout.Context, suggestions []consoleSuggestion) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		// The panel is as tall as its rows and no taller — it used to reserve
		// a fixed 62dp per row, and a row is nowhere near that, so the card
		// ended in a slab of empty background.
		//
		// The rows go in a scrolling List all the same. Hugging alone is not
		// enough: on a short window, or with the on-screen keyboard up, the
		// rows past the fold would be laid out at zero height while arrow
		// navigation went on counting them — the user could select, and run, a
		// command they could not see.
		macro := op.Record(gtx.Ops)
		dims := layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			gtx.Constraints.Min.Y = 0
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Caption(c.theme(), c.parent.t("console.suggestions_hint"))
					label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					list := material.List(c.theme(), &c.suggestList)
					// Overlay for the same reason the popup menu uses it: the
					// reserved gutter of Gio's default takes its width out of
					// the rows and leaves the card visibly lopsided.
					list.AnchorStrategy = material.Overlay
					return list.Layout(gtx, len(suggestions), func(gtx layout.Context, index int) layout.Dimensions {
						top := unit.Dp(0)
						if index > 0 {
							top = unit.Dp(6)
						}
						return layout.Inset{Top: top}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
							return c.layoutConsoleSuggestionItem(gtx, suggestions[index].Label, index == c.selectedSuggest)
						})
					})
				}),
			)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: color.NRGBA{R: 24, G: 30, B: 39, A: 255}}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

func (c *consoleModal) layoutConsoleSuggestionItem(gtx layout.Context, command string, selected bool) layout.Dimensions {
	btn := c.suggestionButton(command)
	return material.Clickable(gtx, btn, func(gtx layout.Context) layout.Dimensions {
		bg := color.NRGBA{R: 34, G: 46, B: 62, A: 255}
		if selected {
			bg = color.NRGBA{R: 57, G: 98, B: 170, A: 255}
		}
		ui.Fill(gtx, bg)
		return layout.Inset{Top: unit.Dp(10), Bottom: unit.Dp(10), Left: unit.Dp(12), Right: unit.Dp(12)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Body2(c.theme(), command)
			label.Color = color.NRGBA{R: 231, G: 237, B: 246, A: 255}
			return label.Layout(gtx)
		})
	})
}

func (c *consoleModal) layoutConsoleHistory(gtx layout.Context) layout.Dimensions {
	entries := c.consoleHistory()
	return c.historyList.Layout(gtx, len(entries), func(gtx layout.Context, index int) layout.Dimensions {
		entry := entries[index]
		top := unit.Dp(0)
		if index > 0 {
			top = unit.Dp(10)
		}
		return layout.Inset{Top: top}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return c.layoutConsoleHistoryCard(gtx, entry)
		})
	})
}

func (c *consoleModal) layoutConsoleHistoryCard(gtx layout.Context, entry *consoleEntry) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
		return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			for entry.CopyButton.Clicked(gtx) {
				// Copy hands over the COMPLETE output: the display text was
				// capped before layout, the overflow file was not.
				gtx.Execute(clipboard.WriteCmd{
					Type: "text/plain",
					Data: io.NopCloser(strings.NewReader(c.fullConsoleOutput(entry))),
				})
			}
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							command := material.Body1(c.theme(), "> "+entry.Command)
							command.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
							command.Font.Weight = 600
							return command.Layout(gtx)
						}),
						layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							btn := material.Button(c.theme(), &entry.CopyButton, c.parent.t("console.copy"))
							btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
							return btn.Layout(gtx)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(6)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body2(c.theme(), entry.CreatedAt.Format("2006-01-02 15:04:05"))
					label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutSelectableOutput(gtx, entry)
				}),
			)
		})
	})
}

func (c *consoleModal) submitConsoleCommand() {
	if c.isConsoleBusy() {
		return
	}

	command := strings.TrimSpace(c.consoleEditor.Text())
	if command == "" {
		return
	}

	c.appendCommandHistory(command)
	c.resetHistoryNavigation()

	c.setConsoleBusy(true)
	c.consoleEditor.SetText("")
	c.hideSuggestions = false
	c.lastSuggestQuery = ""
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil

	// Registered with the parent window's UI-op gate: console commands
	// call straight into the CommandTable → router/chatlog, and must be
	// drained (or refused) on shutdown like every other UI operation.
	if !c.parent.beginUIOp() {
		return
	}
	go func(command string) {
		defer c.parent.endUIOp()
		type cmdResult struct {
			output string
			err    error
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan cmdResult, 1)
		go func() {
			out, err := c.executeCommand(ctx, command)
			ch <- cmdResult{out, err}
		}()

		var output string
		var err error
		timedOut := false
		select {
		case r := <-ch:
			output, err = r.output, r.err
		case <-time.After(10 * time.Second):
			err = fmt.Errorf("command timed out after 10s (still running in background)")
			timedOut = true
		}

		full := output
		failed := false
		if err != nil {
			full = err.Error()
			failed = true
		}
		c.appendConsoleEntry(c.composeConsoleEntry(consoleEntry{
			Command:   command,
			CreatedAt: time.Now(),
			Failed:    failed,
		}, full))

		if !timedOut {
			// Command completed normally — release the busy guard.
			c.mu.Lock()
			c.consoleBusy = false
			c.mu.Unlock()
		}
		c.invalidateWindow()

		if timedOut {
			// Wait for the background command to finish before releasing
			// the busy guard. This prevents the user from submitting the
			// same command again while the previous one is still mutating
			// node state. A hard deadline prevents a stuck command from
			// wedging the console permanently.
			select {
			case r := <-ch:
				lateFull := r.output
				lateFailed := false
				if r.err != nil {
					lateFull = r.err.Error()
					lateFailed = true
				}
				c.appendConsoleEntry(c.composeConsoleEntry(consoleEntry{
					Command:   command + " (late result)",
					CreatedAt: time.Now(),
					Failed:    lateFailed,
				}, lateFull))

				c.mu.Lock()
				c.consoleBusy = false
				c.mu.Unlock()
				c.invalidateWindow()

			case <-time.After(2 * time.Minute):
				// Command is truly stuck — cancel its context so any
				// context-aware handler stops early, then unblock the
				// console. Without cancellation the orphaned goroutine
				// could complete and mutate node state while the user
				// has already retried the same command.
				cancel()

				c.appendConsoleEntry(c.composeConsoleEntry(consoleEntry{
					Command:   command + " (abandoned)",
					CreatedAt: time.Now(),
					Failed:    true,
				}, "command did not complete within 2m30s — console unlocked (command may still finish in background)"))

				c.mu.Lock()
				c.consoleBusy = false
				c.mu.Unlock()
				c.invalidateWindow()
			}
		}
	}(command)
}

// appendCommandHistory records cmd as the most recent history entry.
// Consecutive duplicates collapse into a single entry — re-running the same
// command should not push the previous distinct command out of arrow-reach.
// The ring is capped at maxConsoleCommandHistory; older entries are dropped
// from the front.
func (c *consoleModal) appendCommandHistory(cmd string) {
	if cmd == "" {
		return
	}
	if n := len(c.commandHistory); n > 0 && c.commandHistory[n-1] == cmd {
		return
	}
	c.commandHistory = append(c.commandHistory, cmd)
	if over := len(c.commandHistory) - maxConsoleCommandHistory; over > 0 {
		c.commandHistory = append(c.commandHistory[:0:0], c.commandHistory[over:]...)
	}
}

// resetHistoryNavigation parks the cursor one past the most recent entry, so
// the next Up arrow starts a fresh walk from the latest command. Called after
// a successful submit and whenever syncHistoryNavigation detects manual edits.
func (c *consoleModal) resetHistoryNavigation() {
	c.historyCursor = len(c.commandHistory)
	c.historyDraft = ""
	c.historyText = ""
}

// navigateHistory walks the command-history ring in response to Up/Down
// arrows in the console input. delta == -1 selects an older entry, +1 a newer
// one. The first arrow press from a non-navigating state snapshots the
// current editor contents into historyDraft so Down past the most recent
// entry restores what the user was typing.
//
// While history is being browsed, suggestion completions are suppressed —
// otherwise the next frame would treat the inserted command as fresh user
// input and re-open the suggestion list, which would hijack the next arrow
// press into completion-navigation instead of continuing through history.
func (c *consoleModal) navigateHistory(delta int) {
	end := len(c.commandHistory)
	if end == 0 {
		return
	}

	// First step away from the live input — capture the in-progress draft so
	// Down past the most recent entry can restore it verbatim.
	if c.historyCursor >= end {
		c.historyCursor = end
		c.historyDraft = c.consoleEditor.Text()
	}

	next := c.historyCursor + delta
	switch {
	case next < 0:
		next = 0
	case next > end:
		next = end
	}
	c.historyCursor = next

	text := c.historyDraft
	if next < end {
		text = c.commandHistory[next]
	}

	c.consoleEditor.SetText(text)
	pos := len([]rune(text))
	c.consoleEditor.SetCaret(pos, pos)
	c.historyText = text

	c.hideSuggestionsUntilRetyped()
}

// syncHistoryNavigation drops the history-navigation state when the editor
// text no longer matches what navigateHistory last wrote. The only way the
// two can diverge is the user typing into the editor mid-browse, which means
// they have committed to that text as their new draft and the next Up should
// snapshot it fresh.
func (c *consoleModal) syncHistoryNavigation() {
	if c.historyCursor >= len(c.commandHistory) {
		return
	}
	if c.consoleEditor.Text() == c.historyText {
		return
	}
	c.resetHistoryNavigation()
}

// scheduleUptimeInvalidate coalesces per-second redraw requests for the
// Peers tab uptime counter. Only one timer goroutine is in flight at a
// time — the atomic flag prevents unbounded goroutine spawning when
// layoutPeersTab runs at 60 fps while peers are connected.
func (c *consoleModal) scheduleUptimeInvalidate() {
	if !atomic.CompareAndSwapInt32(&c.uptimeInvalidating, 0, 1) {
		return
	}
	go func() {
		time.Sleep(time.Second)
		atomic.StoreInt32(&c.uptimeInvalidating, 0)
		c.invalidateWindow()
	}()
}

// invalidateWindow asks for a redraw from any goroutine. The console draws
// into the main window now, so this is the parent's invalidate — which is
// itself safe to call after shutdown has begun.
//
// The name is kept because ~15 call sites and their comments describe the
// coalescing contract around it, and none of them care which window redraws.
func (c *consoleModal) invalidateWindow() {
	c.parent.invalidate()
}

// subscribeConsoleEvents registers ebus handlers that redraw the console when
// node state changes. Subscription IDs are stored in c.ebusSubscriptions so
// unsubscribeConsoleEvents can remove them when the modal closes: the handlers
// exist to keep a VISIBLE console current, and a closed one that kept them
// would pay for every peer event for the rest of the process.
func (c *consoleModal) subscribeConsoleEvents() {
	bus := c.parent.eventBus
	if bus == nil {
		return
	}
	// Re-subscribing on top of a live set would double every handler. The
	// modal can be reopened any number of times, so the guard is not
	// theoretical.
	if len(c.ebusSubscriptions) > 0 {
		return
	}

	invalidate := c.invalidateWindow

	// Peer list and health data changed — affects Peers tab.
	ids := []ebus.SubscriptionID{
		bus.Subscribe(ebus.TopicPeerConnected, func(domain.PeerAddress, domain.PeerIdentity) { invalidate() }),
		bus.Subscribe(ebus.TopicPeerDisconnected, func(domain.PeerAddress, domain.PeerIdentity) { invalidate() }),
		bus.Subscribe(ebus.TopicPeerHealthChanged, func(ebus.PeerHealthDelta) { invalidate() }),
		bus.Subscribe(ebus.TopicSlotStateChanged, func(domain.PeerAddress, string) { invalidate() }),

		// Per-peer pending count changed — affects pending badge on peer cards.
		bus.Subscribe(ebus.TopicPeerPendingChanged, func(ebus.PeerPendingDelta) { invalidate() }),

		// Peer traffic updated — affects Traffic tab and peer cards byte counters.
		bus.Subscribe(ebus.TopicPeerTrafficUpdated, func(ebus.PeerTrafficBatch) { invalidate() }),

		// Aggregate network status — affects Info tab header and Peers tab summary.
		bus.Subscribe(ebus.TopicAggregateStatusChanged, func(domain.AggregateStatusSnapshot) { invalidate() }),

		// Version policy changed — affects update banner in Info tab.
		bus.Subscribe(ebus.TopicVersionPolicyChanged, func(domain.VersionPolicySnapshot) { invalidate() }),

		// Identity/contacts changes — affects Info tab.
		bus.Subscribe(ebus.TopicContactAdded, func(ebus.ContactAddedEvent) { invalidate() }),
		bus.Subscribe(ebus.TopicContactRemoved, func(domain.PeerIdentity) { invalidate() }),
		bus.Subscribe(ebus.TopicIdentityAdded, func(domain.PeerIdentity) { invalidate() }),

		// File-transfer lifecycle — affects File tab. Sender lifecycle
		// (sent, send-failed), receiver registration on every inbound
		// file_announce regardless of which chat is active, and
		// delete-completed events all change the snapshot the tab
		// renders. Chunk-progress events are intentionally NOT
		// subscribed here (they fire at high frequency); the file tab
		// schedules its own coalesced redraw via fileTabInvalidating
		// while any transfer is mid-flight.
		bus.Subscribe(ebus.TopicFileSent, func(ebus.FileSentResult) { invalidate() }),
		bus.Subscribe(ebus.TopicFileSendFailed, func(ebus.FileSendFailedResult) { invalidate() }),
		bus.Subscribe(ebus.TopicFileReceived, func(ebus.FileReceivedResult) { invalidate() }),
		bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(ebus.MessageDeleteOutcome) { invalidate() }),
	}
	c.ebusSubscriptions = ids
}

// unsubscribeConsoleEvents removes all ebus handlers registered by
// subscribeConsoleEvents. Called on window close to prevent handler leak.
func (c *consoleModal) unsubscribeConsoleEvents() {
	bus := c.parent.eventBus
	if bus == nil {
		return
	}
	bus.UnsubscribeAll(c.ebusSubscriptions)
	c.ebusSubscriptions = nil
}

func (c *consoleModal) isConsoleBusy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.consoleBusy
}

func (c *consoleModal) setConsoleBusy(value bool) {
	c.mu.Lock()
	c.consoleBusy = value
	c.mu.Unlock()
}

func (c *consoleModal) consoleHistory() []*consoleEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*consoleEntry, 0, len(c.consoleEntries))
	for i := range c.consoleEntries {
		out = append(out, &c.consoleEntries[i])
	}
	return out
}

func (c *consoleModal) suggestionButton(command string) *widget.Clickable {
	if btn, ok := c.suggestButtons[command]; ok {
		return btn
	}
	btn := new(widget.Clickable)
	c.suggestButtons[command] = btn
	return btn
}

func (c *consoleModal) consoleSuggestions() []consoleSuggestion {
	// The popup is drawn by layoutConsoleTab and nowhere else, so off that tab
	// it is not showing whatever the state says. Reporting otherwise let a
	// stale popup swallow the Escape or Back that the user aimed at the modal,
	// with nothing on screen to explain where the key went.
	if c.currentTab() != consoleTabConsole {
		return nil
	}
	if len(c.suggestSnapshot) > 0 {
		return append([]consoleSuggestion(nil), c.suggestSnapshot...)
	}
	query := strings.TrimSpace(strings.ToLower(c.consoleEditor.Text()))
	if c.hideSuggestions {
		return nil
	}
	all := c.getCommands()
	if query == "" {
		return nil
	}

	matches := make([]consoleSuggestion, 0, len(all))
	for _, item := range all {
		if strings.HasPrefix(strings.ToLower(item.Label), query) || strings.Contains(strings.ToLower(item.Label), query) {
			matches = append(matches, item)
		}
	}
	if len(matches) > maxSuggestions {
		return matches[:maxSuggestions]
	}
	return matches
}

func (c *consoleModal) syncSuggestionVisibility() {
	query := strings.TrimSpace(c.consoleEditor.Text())
	if query != c.lastSuggestQuery {
		if len(c.suggestSnapshot) > 0 && query == c.currentSuggestionText() {
			c.lastSuggestQuery = query
			return
		}
		c.hideSuggestions = false
		c.lastSuggestQuery = query
		c.selectedSuggest = -1
		if len(c.suggestSnapshot) > 0 {
			base := strings.TrimSpace(c.suggestBaseQuery)
			current := strings.TrimSpace(query)
			// If the user resumed typing after arrow-navigation, drop the frozen
			// snapshot and return to live filtering from the current input.
			if current != base {
				c.suggestBaseQuery = ""
				c.suggestSnapshot = nil
			}
		}
	}
}

func (c *consoleModal) moveSuggestionSelection(delta int, suggestions []consoleSuggestion) {
	if len(c.suggestSnapshot) == 0 {
		c.suggestBaseQuery = strings.TrimSpace(c.consoleEditor.Text())
		c.suggestSnapshot = c.computeConsoleSuggestions(c.suggestBaseQuery)
		suggestions = append([]consoleSuggestion(nil), c.suggestSnapshot...)
	}
	if len(suggestions) == 0 {
		c.selectedSuggest = -1
		return
	}
	if c.selectedSuggest < 0 {
		if delta > 0 {
			c.selectedSuggest = 0
		} else {
			c.selectedSuggest = len(suggestions) - 1
		}
	} else {
		c.selectedSuggest += delta
		if c.selectedSuggest < 0 {
			c.selectedSuggest = len(suggestions) - 1
		}
		if c.selectedSuggest >= len(suggestions) {
			c.selectedSuggest = 0
		}
	}
	c.scrollSuggestionIntoView(len(suggestions))
	c.consoleEditor.SetText(suggestions[c.selectedSuggest].Insert)
	pos := len([]rune(suggestions[c.selectedSuggest].Insert))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestions = false
	c.lastSuggestQuery = strings.TrimSpace(c.consoleEditor.Text())
}

func (c *consoleModal) applySelectedSuggestion(gtx layout.Context, suggestions []consoleSuggestion, chooseFirst bool) bool {
	if len(suggestions) == 0 {
		return false
	}
	if c.selectedSuggest < 0 && chooseFirst {
		c.selectedSuggest = 0
	}
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(suggestions) {
		return false
	}
	c.applySuggestion(gtx, suggestions[c.selectedSuggest].Insert)
	return true
}

func (c *consoleModal) commitSuggestionForArguments(gtx layout.Context, suggestions []consoleSuggestion) bool {
	if len(suggestions) == 0 {
		return false
	}
	if c.selectedSuggest < 0 {
		c.selectedSuggest = 0
	}
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(suggestions) {
		return false
	}

	item := suggestions[c.selectedSuggest].Insert
	if !strings.HasSuffix(item, " ") {
		item += " "
	}
	c.consoleEditor.SetText(item)
	pos := len([]rune(item))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestionsUntilRetyped()
	c.parent.touchKbd.noteExplicitEditorFocus()
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
	return true
}

// dismissSuggestions closes the completion popup if it is showing, and reports
// whether it was.
//
// The test is what the user can SEE — consoleSuggestions() being non-empty —
// and this is the second attempt at it. The first asked whether a frozen
// snapshot existed or the list had been hidden, which is inside out: an
// ordinary filtered list has neither, so it answered "nothing to close" and
// Escape took the whole modal instead; and a list already dismissed by picking
// from it answered "yes", swallowed the key AND reset the editor to an empty
// base query, wiping the command the user had typed.
func (c *consoleModal) dismissSuggestions(gtx layout.Context) bool {
	if len(c.consoleSuggestions()) == 0 {
		return false
	}

	c.restoreTypedQuery()
	c.hideSuggestionsUntilRetyped()
	c.parent.touchKbd.noteExplicitEditorFocus()
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
	return true
}

// scrollSuggestionIntoView scrolls the completion popup only when the
// highlighted row is not already on screen.
//
// Scrolling unconditionally is what the first cut did, with List.ScrollTo,
// and it makes the selection the FIRST element — layout.List draws nothing
// before First, so stepping Down to the second suggestion hid the first, and
// stepping Up from nothing left a single row on screen. A list that jumps
// under the user is worse than one that does not move.
//
// Position.Count is how many rows the last frame actually drew, so the visible
// span is [First, First+Count). Zero Count means nothing has been laid out yet
// and there is nothing to be off-screen from.
func (c *consoleModal) scrollSuggestionIntoView(total int) {
	selected := c.selectedSuggest
	if selected < 0 || total == 0 {
		return
	}
	first, count := c.suggestList.Position.First, c.suggestList.Position.Count
	if count <= 0 {
		return
	}
	switch {
	case selected < first:
		c.suggestList.ScrollTo(selected)
	case selected >= first+count:
		// Bring it to the BOTTOM of the visible span, so the rows above it
		// stay where the user last saw them.
		c.suggestList.ScrollTo(selected - count + 1)
	}
}

// restoreTypedQuery puts back the text the user actually typed.
//
// Arrow navigation rewrites the command line with the highlighted suggestion
// and parks the real input in suggestBaseQuery. Anything that ends that
// navigation owes the user their own text back — closing the modal mid-walk
// used to drop it and reopen showing a command they never wrote.
//
// A plain filtered list never touched the editor and there is nothing to
// restore, which is what the snapshot check means.
func (c *consoleModal) restoreTypedQuery() {
	if len(c.suggestSnapshot) == 0 {
		return
	}
	base := strings.TrimSpace(c.suggestBaseQuery)
	c.consoleEditor.SetText(base)
	pos := len([]rune(base))
	c.consoleEditor.SetCaret(pos, pos)
	c.lastSuggestQuery = base
}

// hideSuggestionsUntilRetyped puts the completion popup away and keeps it away
// until the command line changes.
//
// lastSuggestQuery is pinned to the text as it stands because
// syncSuggestionVisibility un-hides on any query it has not seen; leaving it
// stale would reopen the popup on the very next frame.
// Every path that puts the popup away goes through it — Escape, closing the
// modal, and accepting a suggestion by Tab, click, Enter or Right Arrow. Those
// last four used to clear the same five fields by hand, which is how the
// scroll reset added here reached only some of them.
func (c *consoleModal) hideSuggestionsUntilRetyped() {
	// The next query is a different list, and it opens at its own first row —
	// a kept scroll position would open it partway down.
	c.suggestList.Position = layout.Position{}
	c.hideSuggestions = true
	c.selectedSuggest = -1
	c.suggestBaseQuery = ""
	c.suggestSnapshot = nil
	c.lastSuggestQuery = strings.TrimSpace(c.consoleEditor.Text())
}

func (c *consoleModal) applySuggestion(gtx layout.Context, item string) {
	c.consoleEditor.SetText(item)
	pos := len([]rune(item))
	c.consoleEditor.SetCaret(pos, pos)
	c.hideSuggestionsUntilRetyped()
	c.parent.touchKbd.noteExplicitEditorFocus()
	gtx.Execute(key.FocusCmd{Tag: &c.consoleEditor})
	c.invalidateWindow()
}

func (c *consoleModal) currentSuggestionText() string {
	if c.selectedSuggest < 0 || c.selectedSuggest >= len(c.suggestSnapshot) {
		return ""
	}
	return c.suggestSnapshot[c.selectedSuggest].Label
}

func (c *consoleModal) computeConsoleSuggestions(query string) []consoleSuggestion {
	query = strings.TrimSpace(strings.ToLower(query))
	if c.hideSuggestions || query == "" {
		return nil
	}
	all := c.getCommands()
	matches := make([]consoleSuggestion, 0, len(all))
	for _, item := range all {
		if strings.HasPrefix(strings.ToLower(item.Label), query) || strings.Contains(strings.ToLower(item.Label), query) {
			matches = append(matches, item)
		}
	}
	if len(matches) > maxSuggestions {
		return matches[:maxSuggestions]
	}
	return matches
}

func newConsoleEntry(entry consoleEntry) consoleEntry {
	entry.OutputText.SetText(entry.Output)
	return entry
}

const consoleDonateURL = "https://pirate.cash/donate/"

func newConsoleDonateEntries() []consoleDonateEntry {
	entries := []consoleDonateEntry{
		{Label: "PirateCash", Address: "PB2vfGqfagNb12DyYTZBYWGnreyt7E4Pug"},
		{Label: "Cosanta", Address: "Cbbp3meofT1ESU5p4d9ucXpXw9pxKCMEyi"},
		{Label: "PIRATE / COSANTA (BEP-20)", Address: "0x52be29951B0D10d5eFa48D58363a25fE5Cc097e9"},
		{Label: "Bitcoin", Address: "bc1q2ph64sryt6skegze6726fp98u44kjsc5exktap"},
		{Label: "Dash", Address: "Xv7U37XKp5d4fjvbeuganwhqXN7Sm4JJkt"},
		{Label: "Zcash", Address: "zs1hwyqs4mfrynq0ysjmhv8wuau5zam0gwpx8ujfv8epgyufkmmsp6t7cfk9y0th7qyx7fsc5azm08"},
		{Label: "Monero", Address: "4AzdEoZxeGMFkdtAxaNLAZakqEVsWpVb2at4u6966WGDiXkS7ZPyi7haeThTGUAWXVKDTmQ9DYTWRHMjGVSBW82xRQqPxkg"},
	}
	for i := range entries {
		entries[i].Text.SetText(entries[i].Address)
		entries[i].Scroll = widget.List{List: layout.List{Axis: layout.Horizontal}}
	}
	return entries
}

func (c *consoleModal) layoutSelectableOutput(gtx layout.Context, entry *consoleEntry) layout.Dimensions {
	textColor := color.NRGBA{R: 208, G: 216, B: 228, A: 255}
	if entry.Failed {
		textColor = color.NRGBA{R: 255, G: 168, B: 168, A: 255}
	}

	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: textColor}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()

	selectionMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 72, G: 96, B: 140, A: 180}}.Add(gtx.Ops)
	selectionMaterial := selectionMacro.Stop()

	entry.OutputText.SetText(entry.Output)
	return entry.OutputText.Layout(gtx, c.theme().Shaper, font.Font{Typeface: c.theme().Face}, c.theme().TextSize, textMaterial, selectionMaterial)
}

func (c *consoleModal) layoutSelectableText(gtx layout.Context, sel *widget.Selectable, text string, textColor color.NRGBA) layout.Dimensions {
	textMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: textColor}.Add(gtx.Ops)
	textMaterial := textMacro.Stop()

	selectionMacro := op.Record(gtx.Ops)
	paint.ColorOp{Color: color.NRGBA{R: 72, G: 96, B: 140, A: 180}}.Add(gtx.Ops)
	selectionMaterial := selectionMacro.Stop()

	sel.SetText(text)
	return sel.Layout(gtx, c.theme().Shaper, font.Font{Typeface: c.theme().Face}, c.theme().TextSize, textMaterial, selectionMaterial)
}

func (c *consoleModal) layoutDonateSection(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		children := []layout.FlexChild{
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(c.theme(), c.parent.t("console.donate_description"))
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body2(c.theme(), c.parent.t("console.donate_source"))
				label.Color = color.NRGBA{R: 167, G: 179, B: 196, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(4)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return c.donateLinkButton.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					pointer.CursorPointer.Add(gtx.Ops)
					label := material.Body2(c.theme(), consoleDonateURL)
					label.Color = color.NRGBA{R: 124, G: 177, B: 255, A: 255}
					label.Font.Weight = 600
					return label.Layout(gtx)
				})
			}),
		}

		for i := range c.donateEntries {
			entry := &c.donateEntries[i]
			children = append(children,
				layout.Rigid(layout.Spacer{Height: unit.Dp(14)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					for entry.CopyButton.Clicked(gtx) {
						gtx.Execute(clipboard.WriteCmd{
							Type: "text/plain",
							Data: io.NopCloser(strings.NewReader(entry.Address)),
						})
					}
					return c.layoutDonateAddressCard(gtx, entry)
				}),
			)
		}

		return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
	})
}

func (c *consoleModal) layoutDonateAddressCard(gtx layout.Context, entry *consoleDonateEntry) layout.Dimensions {
	border := color.NRGBA{R: 56, G: 68, B: 86, A: 255}
	bg := color.NRGBA{R: 28, G: 35, B: 46, A: 255}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		borderMacro := op.Record(gtx.Ops)
		dims := layout.UniformInset(unit.Dp(1)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			contentMacro := op.Record(gtx.Ops)
			contentDims := layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
					layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
						return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return layout.Flex{Axis: layout.Horizontal}.Layout(gtx,
									layout.Rigid(func(gtx layout.Context) layout.Dimensions {
										return c.layoutDonateBadge(gtx, entry.Label)
									}),
								)
							}),
							layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
							layout.Rigid(func(gtx layout.Context) layout.Dimensions {
								return entry.Scroll.Layout(gtx, 1, func(gtx layout.Context, _ int) layout.Dimensions {
									return c.layoutSelectableText(gtx, &entry.Text, entry.Address, color.NRGBA{R: 245, G: 247, B: 250, A: 255})
								})
							}),
						)
					}),
					layout.Rigid(layout.Spacer{Width: unit.Dp(12)}.Layout),
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						btn := material.Button(c.theme(), &entry.CopyButton, c.parent.t("console.copy"))
						btn.Background = color.NRGBA{R: 48, G: 56, B: 70, A: 255}
						return btn.Layout(gtx)
					}),
				)
			})
			contentCall := contentMacro.Stop()

			defer clip.UniformRRect(image.Rectangle{Max: contentDims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
			paint.ColorOp{Color: bg}.Add(gtx.Ops)
			paint.PaintOp{}.Add(gtx.Ops)
			contentCall.Add(gtx.Ops)
			return contentDims
		})
		borderCall := borderMacro.Stop()

		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(11))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: border}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		borderCall.Add(gtx.Ops)
		return dims
	})
}

func (c *consoleModal) layoutDonateBadge(gtx layout.Context, text string) layout.Dimensions {
	bg := color.NRGBA{R: 42, G: 51, B: 64, A: 255}
	fg := color.NRGBA{R: 198, G: 210, B: 226, A: 255}

	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		gtx.Constraints.Min.X = 0
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(10), Right: unit.Dp(10)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(c.theme(), strings.ToUpper(text))
			label.Color = fg
			label.Font.Weight = 600
			return label.Layout(gtx)
		})
		call := macro.Stop()

		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: bg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

func openExternalURL(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

// card renders a styled card using the console window's own theme to avoid
// a data race with the parent window's text shaper (not thread-safe on Linux).
func (c *consoleModal) card(gtx layout.Context, titleText string, rows []string, extras ...func(layout.Context) layout.Dimensions) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})

		// 8dp panel padding matching the main window cards (window.go card).
		inset := layout.UniformInset(unit.Dp(8))
		return inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			children := make([]layout.FlexChild, 0, len(rows)+len(extras)+2)
			if strings.TrimSpace(titleText) != "" {
				children = append(children,
					layout.Rigid(func(gtx layout.Context) layout.Dimensions {
						label := material.Label(c.theme(), unit.Sp(20), titleText)
						label.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
						return label.Layout(gtx)
					}),
					layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				)
			}

			for _, row := range rows {
				text := row
				children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.Body1(c.theme(), text)
					label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
					return label.Layout(gtx)
				}))
				children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout))
			}

			for _, extra := range extras {
				children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout))
				children = append(children, layout.Rigid(extra))
			}

			return layout.Flex{
				Axis: layout.Vertical,
			}.Layout(gtx, children...)
		})
	})
}

func (c *consoleModal) layoutInfoRows(gtx layout.Context, rows []string) layout.Dimensions {
	children := make([]layout.FlexChild, 0, len(rows)*2)
	for _, row := range rows {
		text := row
		children = append(children,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				label := material.Body1(c.theme(), text)
				label.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
				return label.Layout(gtx)
			}),
			layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
		)
	}
	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

// peerSelectablesFor returns or creates the set of Selectable widgets for a peer address.
func (c *consoleModal) peerSelectablesFor(address string) *peerCardSelectables {
	sel, ok := c.peerSelectables[address]
	if !ok {
		sel = &peerCardSelectables{}
		c.peerSelectables[address] = sel
	}
	return sel
}

// peerSlotGroup defines the display order and label for a CM slot group.
// Peers are sorted into groups by effectiveSlotState and rendered top-to-bottom
// in the order defined here: active first, then dialing, reconnecting, etc.
var peerSlotGroups = []struct {
	state string
	label string
}{
	{"active", "Active"},
	{"initializing", "Initializing"},
	{"dialing", "Dialing"},
	{"reconnecting", "Reconnecting"},
	{"queued", "Queued"},
	{"retry_wait", "Retry Wait"},
	{"", "Inbound"},
}

// effectiveSlotState returns the grouping key for a peer.
// CM-managed peers use SlotState; inbound-only peers (Connected, no slot)
// return "" which maps to the "Inbound" group.
func effectiveSlotState(p service.PeerHealth) string {
	if p.SlotState != "" {
		return p.SlotState
	}
	return ""
}

// synthesizePeerHealthFromCapture builds a minimal PeerHealth view for a
// CaptureSession that has no matching health-delta row yet. The resulting
// row carries identity (address, peer id, conn id, direction) so the card
// renderer can show the recording dot and address; all other health/traffic
// fields stay zero, matching the "no signal yet" state of an idle conn that
// just started recording. SlotState is set to "active" for outbound captures
// (the slot already produced a live conn) and left empty for inbound, so the
// row lands in the correct group via effectiveSlotState.
func synthesizePeerHealthFromCapture(s service.CaptureSession) service.PeerHealth {
	slotState := ""
	if s.Direction == domain.PeerDirectionOutbound {
		slotState = "active"
	}
	return service.PeerHealth{
		Address:   string(s.Address),
		PeerID:    s.PeerID.String(),
		ConnID:    uint64(s.ConnID),
		Direction: string(s.Direction),
		SlotState: slotState,
		Connected: true,
	}
}

// mergeCapturesIntoPeers reconciles peers with active CaptureSessions so
// the UI renders exactly one card per peer while still surfacing captures
// whose health delta has not landed. This is the UI-side fallback that
// handles the capture-only state the CaptureSessions/PeerHealth split was
// specifically designed to allow.
//
// Reconciliation rules, applied per active capture:
//  1. Captures without any resolvable identity (both Address and PeerID
//     empty) are skipped entirely — see captureHasIdentity. The session
//     remains on NodeStatus.CaptureSessions so the writer stays visible
//     to "Stop all recordings", but it cannot back a peer card.
//  2. If peers already contains a row with the capture's ConnID, it is
//     authoritative — no action.
//  3. If peers contains a ConnID=0 same-Address placeholder (seeded by
//     applySlotStateDelta/applyPeerPendingDelta before the first health
//     delta), the placeholder is promoted in place: ConnID, Direction,
//     and Connected come from the capture; SlotState, PendingCount, and
//     any already-observed PeerID stay (they carry earlier evidence the
//     capture cannot override). Without this step a slot-only placeholder
//     plus a capture for the same peer would briefly render as two
//     separate cards — the split-state bug resurfacing at the UI layer.
//  4. Otherwise the capture is surfaced through a freshly synthesized
//     PeerHealth row appended after the existing entries.
//
// The function does not mutate the caller's slice. When a promotion is
// required the slice is cloned first (copy-on-write) so any caller still
// reading the original (e.g., diagnostic snapshots) sees the placeholder
// unchanged. Allocation is skipped entirely when there are no captures
// at all — the common quiet-node path returns the input slice as-is.
func mergeCapturesIntoPeers(
	peers []service.PeerHealth,
	captures map[domain.ConnID]service.CaptureSession,
) []service.PeerHealth {
	if len(captures) == 0 {
		return peers
	}
	seen := make(map[domain.ConnID]struct{}, len(peers))
	// placeholderByAddr indexes ConnID=0 rows with a non-empty Address so
	// an incoming capture for the same peer can reuse the existing card
	// instead of appending a duplicate. First-index-wins is deliberate:
	// applySlotStateDelta never creates duplicates, so collisions here
	// would already be a bug we should not paper over.
	placeholderByAddr := make(map[string]int, len(peers))
	for i, p := range peers {
		if p.ConnID != 0 {
			seen[domain.ConnID(p.ConnID)] = struct{}{}
			continue
		}
		if p.Address != "" {
			if _, exists := placeholderByAddr[p.Address]; !exists {
				placeholderByAddr[p.Address] = i
			}
		}
	}

	output := peers
	cloned := false
	cloneForMutation := func() {
		if cloned {
			return
		}
		out := make([]service.PeerHealth, len(peers))
		copy(out, peers)
		output = out
		cloned = true
	}

	var orphans []service.PeerHealth
	for id, s := range captures {
		if !s.Active {
			continue
		}
		if !captureHasIdentity(s) {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		if idx, ok := placeholderByAddr[string(s.Address)]; ok {
			cloneForMutation()
			promotePlaceholderFromCapture(&output[idx], s)
			seen[id] = struct{}{}
			delete(placeholderByAddr, string(s.Address))
			continue
		}
		orphans = append(orphans, synthesizePeerHealthFromCapture(s))
	}
	if len(orphans) == 0 {
		return output
	}
	merged := make([]service.PeerHealth, 0, len(output)+len(orphans))
	merged = append(merged, output...)
	merged = append(merged, orphans...)
	return merged
}

// promotePlaceholderFromCapture grafts a capture's live-conn evidence
// onto an address-level ConnID=0 placeholder without discarding earlier
// observations carried by that placeholder.
//
// ConnID, Direction, and Connected are authoritative from the capture:
// a recording implies a real open connection whose identifying ConnID is
// the one carried by the capture event.
//
// SlotState and PendingCount are preserved unconditionally — they reflect
// ConnectionManager lifecycle state the capture does not observe and
// cannot authoritatively update. Overwriting them would silently regress
// the peer card's slot-lifecycle display.
//
// PeerID is preserved when already non-empty: an enrichment path (probe
// snapshot, out-of-band identity delta) may have observed it before the
// capture event, and the capture's PeerID is at best the same value.
func promotePlaceholderFromCapture(p *service.PeerHealth, s service.CaptureSession) {
	p.ConnID = uint64(s.ConnID)
	if p.PeerID == "" {
		p.PeerID = s.PeerID.String()
	}
	if p.Direction == "" {
		p.Direction = string(s.Direction)
	}
	p.Connected = true
}

// layoutActivePeersContent groups peers by CM slot state and renders each
// group as a titled section. Groups appear in a fixed priority order:
// Active → Dialing → Reconnecting → Queued → Retry Wait → Inbound.
//
// peers is expected to be the post-merge set produced by activeRowsForTab,
// i.e. real PeerHealth rows plus synthetic rows for orphan CaptureSessions.
// This function does not merge again — the merge is owned by the caller so
// all tab-level consumers share one view of the active set.
//
// captures is the current CaptureSessions map from NodeStatus — the UI keys
// recording visuals (red dot, info line, stop-all banner) off this map rather
// than fields on PeerHealth so that capture bookkeeping is independent of
// peer-health row lifecycle.
func (c *consoleModal) layoutActivePeersContent(
	gtx layout.Context,
	peers []service.PeerHealth,
	captures map[domain.ConnID]service.CaptureSession,
) layout.Dimensions {
	// Handle stop-recording button click.
	if c.stopRecordingButton.Clicked(gtx) {
		go func() {
			_, _ = c.executeCommand(context.Background(), "stopPeerTrafficRecording scope=all")
		}()
	}

	grouped := make(map[string][]service.PeerHealth, len(peerSlotGroups))
	for _, p := range peers {
		key := effectiveSlotState(p)
		grouped[key] = append(grouped[key], p)
	}
	hasRecording := hasActiveCapture(captures)

	type section struct {
		top    unit.Dp
		render func(layout.Context) layout.Dimensions
	}
	sections := make([]section, 0, len(peerSlotGroups)+1)

	// Global stop-recording banner when at least one peer is recording.
	if hasRecording {
		sections = append(sections, section{
			top: 0,
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutStopRecordingBanner(gtx)
			},
		})
	}

	for _, g := range peerSlotGroups {
		items := grouped[g.state]
		if len(items) == 0 {
			continue
		}
		top := unit.Dp(14)
		if len(sections) == 0 {
			top = 0
		}
		label := fmt.Sprintf("%s (%d)", g.label, len(items))
		groupItems := items // capture for closure
		sections = append(sections, section{
			top: top,
			render: func(gtx layout.Context) layout.Dimensions {
				return c.layoutPeerSection(gtx, label, groupItems, captures)
			},
		})
	}

	list := material.List(c.theme(), &c.peerSectionList)
	return list.Layout(gtx, len(sections), func(gtx layout.Context, index int) layout.Dimensions {
		item := sections[index]
		return layout.Inset{Top: item.top}.Layout(gtx, item.render)
	})
}

// activePeerSummary builds a one-line status summary for the active peer list.
// Counts both health states (healthy/degraded/stalled) and CM slot states
// (dialing/queued/retry_wait) so the user sees the full connection picture.
func activePeerSummary(parent *Window, peers []service.PeerHealth) string {
	healthy := 0
	degraded := 0
	stalled := 0
	dialing := 0
	initializing := 0
	queued := 0
	retryWait := 0
	var totalIn, totalOut int64
	for _, item := range peers {
		switch item.State {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "stalled":
			stalled++
		}
		switch item.SlotState {
		case "dialing":
			dialing++
		case "initializing":
			initializing++
		case "queued":
			queued++
		case "retry_wait":
			retryWait++
		}
		totalIn += item.BytesReceived
		totalOut += item.BytesSent
	}
	summary := parent.t("node.active_peer.summary", healthy, degraded, stalled)
	if summary == "node.active_peer.summary" {
		base := fmt.Sprintf("Healthy: %d, Degraded: %d, Stalled: %d", healthy, degraded, stalled)
		if dialing > 0 || initializing > 0 || queued > 0 || retryWait > 0 {
			base += fmt.Sprintf(" | Dialing: %d, Init: %d, Queued: %d, RetryWait: %d", dialing, initializing, queued, retryWait)
		}
		base += fmt.Sprintf(" | In: %s, Out: %s", formatBytes(totalIn), formatBytes(totalOut))
		return base
	}
	return summary
}

// executeCommand parses console input and dispatches it through CommandTable.
func (c *consoleModal) executeCommand(ctx context.Context, input string) (string, error) {
	if c.parent.cmdTable == nil {
		return "", fmt.Errorf("command table not initialized")
	}

	req, err := rpc.ParseConsoleInput(input)
	if err != nil {
		return "", err
	}

	// Console-specific "help" — human-readable text with categories,
	// defaults, and self-address. The CommandTable help handler returns
	// machine JSON for API consumers; the console needs a different format.
	if req.Name == "help" {
		addr := ""
		if c.parent.client != nil {
			addr = c.parent.client.Address().String()
		}
		return consoleHelpText(c.parent.cmdTable, addr), nil
	}

	req.Ctx = ctx
	resp := c.parent.cmdTable.Execute(req)

	if resp.Error != nil {
		return "", resp.Error
	}

	// Pretty-print JSON response for console display
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, resp.Data, "", "  "); err != nil {
		return string(resp.Data), nil
	}
	return prettyJSON.String(), nil
}

// loadCommands populates command suggestions from CommandTable — synchronous, no HTTP.
func (c *consoleModal) loadCommands() {
	commands := c.parent.cmdTable.Commands()
	suggestions := commandInfoToSuggestions(commands)
	c.mu.Lock()
	c.cachedCommands = suggestions
	c.mu.Unlock()
}

// getCommands returns the current command suggestions.
func (c *consoleModal) getCommands() []consoleSuggestion {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cachedCommands
}

// defaultPrefills maps command names to default argument templates
// that should be inserted on autocomplete selection. This preserves
// the old desktop console UX where fetchChatlog prefilled "dm" as topic.
var defaultPrefills = map[string]string{
	"fetchChatlog": "fetchChatlog dm",
}

func commandInfoToSuggestions(commands []rpc.CommandInfo) []consoleSuggestion {
	suggestions := make([]consoleSuggestion, 0, len(commands))
	for _, cmd := range commands {
		label := cmd.Name
		if cmd.Usage != "" {
			label = cmd.Name + " " + cmd.Usage
		}
		insert := cmd.Name
		if prefill, ok := defaultPrefills[cmd.Name]; ok {
			insert = prefill
		}
		suggestions = append(suggestions, consoleSuggestion{
			Label:  label,
			Insert: insert,
		})
	}
	return suggestions
}

// consoleHelpText formats CommandTable metadata into a human-readable help
// screen for the desktop console. Grouped by category with usage hints,
// defaults, and self-address — matching the legacy consoleHelpText from
// DesktopClient but generated dynamically from CommandTable.
func consoleHelpText(table *rpc.CommandTable, selfAddress string) string {
	commands := table.Commands()

	// Group by category, preserving display order.
	categoryOrder := []string{"system", "network", "routing", "metrics", "diagnostic", "identity", "message", "file", "chatlog", "notice", "view"}
	categoryLabels := map[string]string{
		"system":     "Control",
		"network":    "Network",
		"routing":    "Routing",
		"metrics":    "Metrics",
		"diagnostic": "Diagnostic",
		"identity":   "Identity & Contacts",
		"message":    "Messages",
		"file":       "File Transfer",
		"chatlog":    "Chat History",
		"notice":     "Notices",
		"view":       "Desktop Views",
	}
	grouped := make(map[string][]rpc.CommandInfo)
	for _, cmd := range commands {
		grouped[cmd.Category] = append(grouped[cmd.Category], cmd)
	}

	var lines []string
	for _, cat := range categoryOrder {
		cmds := grouped[cat]
		if len(cmds) == 0 {
			continue
		}
		label := categoryLabels[cat]
		if label == "" {
			label = cat
		}
		lines = append(lines, fmt.Sprintf("== %s ==", label))
		for _, cmd := range cmds {
			if cmd.Usage != "" {
				lines = append(lines, cmd.Name+" "+cmd.Usage)
			} else {
				lines = append(lines, cmd.Name)
			}
		}
		lines = append(lines, "")
	}

	lines = append(lines,
		"Defaults:",
		"  topic for fetch_messages/fetch_message_ids: global",
		"  topic for fetch_pending_messages/fetch_inbox: dm",
		"  recipient: "+selfAddress,
		"",
		"You can also paste a raw JSON frame for any registered command.",
	)

	return strings.Join(lines, "\n")
}

// peerIdentityKey returns the dedup key for "distinct peer" counters.
// Prefer PeerID (stable across reconnects); fall back to Address when the
// identity is not yet known (pre-handshake rows, slot-only placeholders,
// capture-start races before the handshake handler fills the identity).
func peerIdentityKey(peerID, address string) string {
	if peerID != "" {
		return peerID
	}
	return address
}

// captureHasIdentity reports whether a CaptureSession carries any
// renderable peer identity. CaptureSessionStarted explicitly permits an
// empty Address when the publisher could not resolve the connection —
// the writer is still active on the node, so the session is recorded,
// but the desktop fallback has nothing to render as a peer. Without this
// gate such sessions would produce blank PeerHealth cards through
// mergeCapturesIntoPeers and all collapse into a single empty-string
// key in the distinct-peer counters (peerIdentityKey("", "") == ""),
// inflating known_peers / connected_peers by exactly one phantom entry
// regardless of how many unlabeled captures are active.
func captureHasIdentity(s service.CaptureSession) bool {
	return !s.PeerID.IsZero() || s.Address != ""
}

// countUniquePeers returns the number of distinct peers the node has any
// evidence of: observed PeerHealth rows plus identities from active
// CaptureSessions. Pending-only placeholder rows (created by
// applyPeerPendingDelta before any real health delta arrives) are excluded
// so queued-but-never-seen addresses do not inflate the known_peers metric.
// Active captures contribute their identity even when no PeerHealth row
// exists yet — capture-start is positive evidence of a real peer. Captures
// without any identity (see captureHasIdentity) are ignored so unresolved
// sessions do not collapse into one phantom entry under the empty key.
func countUniquePeers(status service.NodeStatus) int {
	seen := make(map[string]struct{}, len(status.PeerHealth)+len(status.CaptureSessions))
	for _, item := range status.PeerHealth {
		if !isPeerObserved(item) {
			continue
		}
		seen[peerIdentityKey(item.PeerID, item.Address)] = struct{}{}
	}
	for _, s := range status.CaptureSessions {
		if !s.Active {
			continue
		}
		if !captureHasIdentity(s) {
			continue
		}
		seen[peerIdentityKey(s.PeerID.String(), string(s.Address))] = struct{}{}
	}
	return len(seen)
}

// isPeerObserved returns true when the PeerHealth entry carries evidence
// of a real connection, CM slot management, or health snapshot — not just
// a pending-queue placeholder created by applyPeerPendingDelta.
// SlotState covers peers the CM is actively managing (queued, dialing,
// retry_wait, etc.) that may not yet have a health delta.
func isPeerObserved(p service.PeerHealth) bool {
	return p.PeerID != "" || p.Connected || p.State != "" || p.Direction != "" || p.SlotState != ""
}

// countConnectedPeers returns the number of distinct peers with at least
// one open connection. An open connection is evidenced by a PeerHealth row
// whose Connected=true or by an active CaptureSession (recording implies
// the transport is live even before the first health delta lands). The
// two sources are deduplicated by peerIdentityKey so a connection that
// has both a health row and an active capture counts once. Captures
// without any identity (see captureHasIdentity) are ignored so the
// counter does not gain a phantom entry when the publisher could not
// resolve the recording connection.
func countConnectedPeers(status service.NodeStatus) int {
	seen := make(map[string]struct{}, len(status.PeerHealth)+len(status.CaptureSessions))
	for _, item := range status.PeerHealth {
		if !item.Connected {
			continue
		}
		seen[peerIdentityKey(item.PeerID, item.Address)] = struct{}{}
	}
	for _, s := range status.CaptureSessions {
		if !s.Active {
			continue
		}
		if !captureHasIdentity(s) {
			continue
		}
		seen[peerIdentityKey(s.PeerID.String(), string(s.Address))] = struct{}{}
	}
	return len(seen)
}

// activePeerHealth returns peers that the ConnectionManager is actively
// managing (any SlotState: queued, dialing, active, reconnecting, retry_wait)
// plus inbound peers that are Connected but have no CM slot.
// This matches the scope of the getActivePeers RPC command — all CM slots
// plus live inbound connections — and excludes "known-only" peers that
// have a health entry but no slot and no active TCP connection.
func activePeerHealth(peers []service.PeerHealth) []service.PeerHealth {
	active := make([]service.PeerHealth, 0, len(peers))
	for _, item := range peers {
		if item.SlotState != "" || item.Connected {
			active = append(active, item)
		}
	}
	return active
}

// activeRowsForTab is the single source of truth for "what rows should the
// peers tab render". It pairs the slot-state filter (active PeerHealth rows)
// with orphan-capture surfacing (CaptureSessions whose ConnID has no health
// delta yet) so every downstream consumer — empty-state gate, connected-peers
// count, summary line, per-group sections, uptime redraw scheduler — observes
// the same set. Splitting the filter from the merge and gating on the raw
// filter result, as an earlier version did, allowed capture-only sessions
// to vanish from the UI when no real health deltas had arrived.
//
// --- Architectural contract for PeerHealth / CaptureSessions consumers ---
//
// The desktop UI must treat PeerHealth and CaptureSessions as two projections
// of the same underlying reality, not as interchangeable lists:
//
//  1. "Liveness" questions — is this conn open, how many peers are connected,
//     what rows do we render, what identities have we observed — MUST go
//     through activeRowsForTab / countConnectedPeers / countUniquePeers. All
//     three fold in active CaptureSessions so orphan captures are surfaced.
//
//  2. "Health evidence" questions — how many peers are healthy/degraded/
//     stalled/reconnecting, what direction breakdown — MUST read
//     status.PeerHealth directly. An orphan capture carries no health
//     evidence; counting it as healthy would fabricate a signal.
//
// New readers of status.PeerHealth or status.CaptureSessions in the desktop
// package must pick a side. The rule of thumb: if the answer changes when a
// connection exists without a health delta, it belongs to category 1 and
// must consult CaptureSessions. Otherwise it is category 2 and should not.
func activeRowsForTab(status service.NodeStatus) []service.PeerHealth {
	return mergeCapturesIntoPeers(activePeerHealth(status.PeerHealth), status.CaptureSessions)
}

// peerRowCache holds peers/info-tab derived data, split into two
// independently-keyed parts so the info tab never builds the active-rows
// merge it does not use. Both keys are a RouterSnapshot.Generation.
// UI-goroutine-only — no synchronisation.
type peerRowCache struct {
	countsGen      uint64
	countsValid    bool
	connectedPeers int
	uniquePeers    int

	rowsGen    uint64
	rowsValid  bool
	activeRows []service.PeerHealth
}

// peerCounts returns the connected/unique peer counts for snap, recomputing
// only when the generation advanced. Used by both the peers tab (connected)
// and the info tab (connected + unique). Does NOT build the active-rows merge.
func (c *consoleModal) peerCounts(snap service.RouterSnapshot) (connected, unique int) {
	if c.peerRows.countsValid && c.peerRows.countsGen == snap.Generation {
		return c.peerRows.connectedPeers, c.peerRows.uniquePeers
	}
	status := snap.NodeStatus
	c.peerRows.connectedPeers = countConnectedPeers(status)
	c.peerRows.uniquePeers = countUniquePeers(status)
	c.peerRows.countsGen = snap.Generation
	c.peerRows.countsValid = true
	return c.peerRows.connectedPeers, c.peerRows.uniquePeers
}

// activePeerRows returns the filtered+merged active-peer rows for snap,
// recomputing only when the generation advanced. Peers tab only — the info
// tab must not call this. An unchanged generation guarantees identical inputs
// (PeerHealth + CaptureSessions), so the cached slice is safe to reuse;
// callers MUST treat it as read-only — it is shared across frames until the
// next generation.
func (c *consoleModal) activePeerRows(snap service.RouterSnapshot) []service.PeerHealth {
	if c.peerRows.rowsValid && c.peerRows.rowsGen == snap.Generation {
		return c.peerRows.activeRows
	}
	c.peerRows.activeRows = activeRowsForTab(snap.NodeStatus)
	c.peerRows.rowsGen = snap.Generation
	c.peerRows.rowsValid = true
	return c.peerRows.activeRows
}

func (c *consoleModal) layoutPeerSection(
	gtx layout.Context,
	title string,
	peers []service.PeerHealth,
	captures map[domain.ConnID]service.CaptureSession,
) layout.Dimensions {
	if len(peers) == 0 {
		return layout.Dimensions{}
	}

	children := []layout.FlexChild{
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			label := material.Body1(c.theme(), title)
			label.Color = color.NRGBA{R: 232, G: 237, B: 247, A: 255}
			label.Font.Weight = 600
			return label.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout),
	}

	for i, peer := range peers {
		if i > 0 {
			children = append(children, layout.Rigid(layout.Spacer{Height: unit.Dp(10)}.Layout))
		}
		peer := peer
		// Look up the capture session for this row by ConnID. ConnID==0
		// rows (address-level placeholders) cannot host a capture, so the
		// lookup is intentionally skipped.
		var capture *service.CaptureSession
		if peer.ConnID != 0 {
			if s, ok := captures[domain.ConnID(peer.ConnID)]; ok && s.Active {
				cp := s
				capture = &cp
			}
		}
		children = append(children, layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutPeerHealthCard(gtx, peer, capture)
		}))
	}

	return layout.Flex{Axis: layout.Vertical}.Layout(gtx, children...)
}

func (c *consoleModal) layoutPeerHealthCard(gtx layout.Context, item service.PeerHealth, capture *service.CaptureSession) layout.Dimensions {
	sel := c.peerSelectablesFor(item.Address)
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 30, G: 39, B: 52, A: 255})
		return layout.UniformInset(unit.Dp(12)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Alignment: layout.Middle}.Layout(gtx,
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							arrow := peerDirectionArrow(item.Direction)
							if arrow == "" {
								return layout.Dimensions{}
							}
							return layout.Inset{Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								label := material.Body1(c.theme(), arrow)
								label.Color = peerDirectionColor(item.Direction)
								return label.Layout(gtx)
							})
						}),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							return c.layoutSelectableText(gtx, &sel.Address, item.Address, color.NRGBA{R: 245, G: 247, B: 250, A: 255})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							if capture == nil {
								return layout.Dimensions{}
							}
							return layout.Inset{Right: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
								return c.layoutRecordingDot(gtx)
							})
						}),
						layout.Rigid(func(gtx layout.Context) layout.Dimensions {
							// Prefer CM slot state for badge when available;
							// fall back to health-derived state for inbound-only peers.
							badgeState := item.State
							if item.SlotState != "" {
								badgeState = item.SlotState
							}
							return c.layoutStateBadge(gtx, badgeState)
						}),
					)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.ClientVersion) == "" {
						return layout.Dimensions{}
					}
					versionText := item.ClientVersion
					if item.ProtocolVersion > 0 {
						versionText = fmt.Sprintf("%s (proto v%d)", item.ClientVersion, item.ProtocolVersion)
					}
					return layout.Inset{Bottom: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return c.layoutSelectableText(gtx, &sel.Version, versionText, color.NRGBA{R: 167, G: 179, B: 196, A: 255})
					})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutSelectableText(gtx, &sel.Meta, c.peerHealthMeta(item), color.NRGBA{R: 196, G: 205, B: 218, A: 255})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if strings.TrimSpace(item.LastError) == "" {
						return layout.Dimensions{}
					}
					return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return c.layoutSelectableText(gtx, &sel.Error, item.LastError, color.NRGBA{R: 255, G: 168, B: 168, A: 255})
					})
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					if capture == nil {
						return layout.Dimensions{}
					}
					info := recordingInfoText(*capture)
					return layout.Inset{Top: unit.Dp(6)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
						return c.layoutSelectableText(gtx, &sel.RecordingInfo, info, color.NRGBA{R: 255, G: 100, B: 100, A: 255})
					})
				}),
			)
		})
	})
}

func (c *consoleModal) layoutStateBadge(gtx layout.Context, state string) layout.Dimensions {
	bg, fg := peerStateColors(state)
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		inset := layout.Inset{Top: unit.Dp(4), Bottom: unit.Dp(4), Left: unit.Dp(10), Right: unit.Dp(10)}
		macro := op.Record(gtx.Ops)
		dims := inset.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(c.theme(), strings.ToUpper(c.parent.t("node.peer_state."+state)))
			label.Color = fg
			return label.Layout(gtx)
		})
		call := macro.Stop()
		defer clip.UniformRRect(image.Rectangle{Max: dims.Size}, gtx.Dp(unit.Dp(10))).Push(gtx.Ops).Pop()
		paint.ColorOp{Color: bg}.Add(gtx.Ops)
		paint.PaintOp{}.Add(gtx.Ops)
		call.Add(gtx.Ops)
		return dims
	})
}

func (c *consoleModal) peerHealthMeta(item service.PeerHealth) string {
	lastRecv := "-"
	if item.LastUsefulReceiveAt.Valid() {
		lastRecv = item.LastUsefulReceiveAt.Time().Format("15:04:05")
	}
	lastPong := "-"
	if item.LastPongAt.Valid() {
		lastPong = item.LastPongAt.Time().Format("15:04:05")
	}
	connected := c.parent.t("node.link.down")
	if item.Connected {
		connected = c.parent.t("node.link.up")
	}
	dirLabel := ""
	if item.Direction != "" {
		dirLabel = " " + item.Direction
	}
	uptime := "-"
	if item.Connected && item.LastConnectedAt.Valid() {
		uptime = formatUptime(time.Since(item.LastConnectedAt.Time()))
	}

	// Build slot suffix for CM-managed outbound peers.
	slotSuffix := ""
	if item.SlotState != "" {
		slotSuffix = fmt.Sprintf(" | slot %s", item.SlotState)
		if item.SlotRetryCount > 0 {
			slotSuffix += fmt.Sprintf(" retry %d", item.SlotRetryCount)
		}
		if item.SlotConnectedAddr != "" && item.SlotConnectedAddr != item.Address {
			slotSuffix += fmt.Sprintf(" via %s", item.SlotConnectedAddr)
		}
	}

	text := c.parent.t("node.peer_health.meta", connected+dirLabel, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score, formatBytes(item.BytesReceived), formatBytes(item.BytesSent))
	if text == "node.peer_health.meta" {
		return fmt.Sprintf("%s%s | uptime %s | pending %d | recv %s | pong %s | fails %d | score %d | in %s | out %s%s", connected, dirLabel, uptime, item.PendingCount, lastRecv, lastPong, item.ConsecutiveFailures, item.Score, formatBytes(item.BytesReceived), formatBytes(item.BytesSent), slotSuffix)
	}
	return text + " | uptime " + uptime + slotSuffix
}

// layoutStopRecordingBanner renders a red-tinted banner with a "Stop all recordings"
// button, visible only when at least one peer has an active capture.
func (c *consoleModal) layoutStopRecordingBanner(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 60, G: 25, B: 25, A: 255})
		return layout.UniformInset(unit.Dp(10)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Alignment: layout.Middle}.Layout(gtx,
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutRecordingDot(gtx)
				}),
				layout.Rigid(layout.Spacer{Width: unit.Dp(8)}.Layout),
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					label := material.Body2(c.theme(), "Traffic recording active")
					label.Color = color.NRGBA{R: 255, G: 180, B: 180, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					btn := material.Button(c.theme(), &c.stopRecordingButton, "Stop all")
					btn.Background = color.NRGBA{R: 180, G: 40, B: 40, A: 255}
					btn.Color = color.NRGBA{R: 255, G: 255, B: 255, A: 255}
					return btn.Layout(gtx)
				}),
			)
		})
	})
}

// layoutRecordingDot draws a small red filled circle as the recording indicator.
func (c *consoleModal) layoutRecordingDot(gtx layout.Context) layout.Dimensions {
	size := gtx.Dp(unit.Dp(10))
	defer clip.Ellipse{Max: image.Pt(size, size)}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: color.NRGBA{R: 230, G: 50, B: 50, A: 255}}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	return layout.Dimensions{Size: image.Pt(size, size)}
}

// recordingInfoText builds a human-readable summary of the capture for a
// peer card. The session is passed by value because the UI snapshot already
// owns its own copy and the function is read-only.
func recordingInfoText(session service.CaptureSession) string {
	startedAt := ""
	if session.StartedAt.Valid() {
		startedAt = session.StartedAt.Time().Format("15:04:05")
	}
	text := fmt.Sprintf("REC %s | %s", string(session.Scope), session.FilePath)
	if startedAt != "" {
		text += " | since " + startedAt
	}
	if session.DroppedEvents > 0 {
		text += fmt.Sprintf(" | dropped %d", session.DroppedEvents)
	}
	if session.Error != "" {
		text += " | err: " + session.Error
	}
	return text
}

// hasActiveCapture reports whether the capture-sessions map contains any
// session that is still recording. Stopped entries (kept around for the
// retention TTL so the user can see terminal diagnostics) do not trigger
// the "stop all" banner.
func hasActiveCapture(captures map[domain.ConnID]service.CaptureSession) bool {
	for _, s := range captures {
		if s.Active {
			return true
		}
	}
	return false
}

// formatBytes formats a byte count into a human-readable string (B, KB, MB, GB, TB).
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTP"[exp])
}

// formatUptime formats a duration as a human-readable uptime string.
// Uses the largest applicable unit: "42s", "15m32s", "3h10m", "2d5h".
func formatUptime(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	totalSeconds := int(d.Seconds())
	if totalSeconds < 60 {
		return fmt.Sprintf("%ds", totalSeconds)
	}
	totalMinutes := totalSeconds / 60
	seconds := totalSeconds % 60
	if totalMinutes < 60 {
		return fmt.Sprintf("%dm%ds", totalMinutes, seconds)
	}
	hours := totalMinutes / 60
	minutes := totalMinutes % 60
	if hours < 24 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	days := hours / 24
	hours = hours % 24
	return fmt.Sprintf("%dd%dh", days, hours)
}

// --- Traffic tab ---

const (
	trafficGraphVisiblePoints = 600  // visible data points (10 min at 1 sample/sec)
	trafficMaxSamples         = 3600 // hard cap matching metrics.Collector ring buffer
)

// Shared palette for traffic visualization (bars, line, legend, badges).
var (
	trafficInColor    = color.NRGBA{R: 59, G: 186, B: 130, A: 200} // green bars
	trafficOutColor   = color.NRGBA{R: 86, G: 156, B: 231, A: 200} // blue bars
	trafficTotalColor = color.NRGBA{R: 230, G: 180, B: 60, A: 255} // yellow line
	trafficInSolid    = color.NRGBA{R: 59, G: 186, B: 130, A: 255} // green solid (legend/badges)
	trafficOutSolid   = color.NRGBA{R: 86, G: 156, B: 231, A: 255} // blue solid (legend/badges)
)

// trafficSamplePoint mirrors protocol.TrafficSampleFrame for the fields the
// traffic tab consumes. Timestamps are RFC3339 UTC strings produced by a
// single writer (metrics.TrafficHistory.Record) in a fixed-width "...Z"
// form, so lexicographic comparison is chronological.
type trafficSamplePoint struct {
	Timestamp     string `json:"timestamp"`
	BytesSentPS   int64  `json:"bytes_sent_ps"`
	BytesRecvPS   int64  `json:"bytes_recv_ps"`
	TotalSent     int64  `json:"total_sent"`
	TotalReceived int64  `json:"total_received"`
}

// fetchTrafficSamples executes fetchTrafficHistory and decodes the samples.
// When since is non-empty it is passed as the incremental cursor so the
// collector returns only samples strictly newer than that timestamp.
// ok=false means RPC failure, unmarshal failure, or missing frame — an empty
// sample slice with ok=true is a valid response (collector just restarted or
// nothing newer than the cursor).
func (c *consoleModal) fetchTrafficSamples(ctx context.Context, since string) (samples []trafficSamplePoint, ok bool) {
	if c.parent.cmdTable == nil {
		return nil, false
	}
	req := rpc.CommandRequest{Name: "fetchTrafficHistory", Ctx: ctx}
	if since != "" {
		req.Args = map[string]interface{}{"since": since}
	}
	resp := c.parent.cmdTable.Execute(req)
	if resp.Error != nil {
		return nil, false
	}
	var frame struct {
		TrafficHistory *struct {
			Samples []trafficSamplePoint `json:"samples"`
		} `json:"traffic_history"`
	}
	if err := json.Unmarshal(resp.Data, &frame); err != nil || frame.TrafficHistory == nil {
		return nil, false
	}
	return frame.TrafficHistory.Samples, true
}

// loadTrafficHistory fetches the full history from the metrics collector
// and populates the local sample slices. Called when the tab opens and on
// every ticker restart so reopening shows accurate data.
// The provided context allows the caller to bound the RPC duration — a
// hung fetchTrafficHistory is cancelled when the context expires.
// Returns true when the RPC succeeded (even if the collector returned an
// empty history), false on any failure (RPC error, unmarshal error, nil
// collector, context cancellation). On failure the cached graph state is
// cleared to prevent rendering stale data from a previous session.
func (c *consoleModal) loadTrafficHistory(ctx context.Context) bool {
	samples, ok := c.fetchTrafficSamples(ctx, "")
	if !ok {
		c.resetTrafficState()
		return false
	}

	c.mu.Lock()
	c.trafficSamplesIn = make([]float32, 0, len(samples))
	c.trafficSamplesOut = make([]float32, 0, len(samples))
	for _, s := range samples {
		c.trafficSamplesIn = append(c.trafficSamplesIn, float32(s.BytesRecvPS))
		c.trafficSamplesOut = append(c.trafficSamplesOut, float32(s.BytesSentPS))
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		c.trafficTotalSent = last.TotalSent
		c.trafficTotalRecv = last.TotalReceived
		c.trafficLastTS = last.Timestamp
	} else {
		// Empty history (e.g. collector just restarted). An empty cursor
		// means the next appendNewTrafficSamples tick picks up everything
		// the collector has recorded since.
		c.trafficTotalSent = 0
		c.trafficTotalRecv = 0
		c.trafficLastTS = ""
	}
	c.mu.Unlock()
	return true
}

// resetTrafficState clears all cached traffic graph data so the UI does not
// render stale samples from a previous session or failed reload.
func (c *consoleModal) resetTrafficState() {
	c.mu.Lock()
	c.trafficSamplesIn = nil
	c.trafficSamplesOut = nil
	c.trafficTotalSent = 0
	c.trafficTotalRecv = 0
	c.trafficLastTS = ""
	c.mu.Unlock()
}

// startTrafficTicker launches a 1-second ticker that samples traffic stats
// and invalidates the window. Reloads the full history from the collector
// on every call so that reopening the tab shows accurate per-second data
// instead of compressing the missed interval into a single spike.
// Called from the UI goroutine (handleActions); all RPC work runs in the
// background goroutine to avoid blocking the Gio event loop.
//
// The ticker is created only after the initial history load finishes so
// that appendNewTrafficSamples() ticks cannot race with and be overwritten by
// a slow loadTrafficHistory() response. A stopped sentinel ticker is
// stored during the load phase to prevent concurrent clicks from
// spawning a second goroutine. The history load has a 30-second timeout;
// on timeout or failure the sentinel is cleared so the user can retry.
// trafficViewVisible reports whether the traffic graph is on screen: the
// console modal is open AND the Traffic tab is the selected one. It is the
// ticker's audience test, so it is read from the ticker goroutine — hence the
// atomics behind both halves.
func (c *consoleModal) trafficViewVisible() bool {
	return c.visible.Load() && c.currentTab() == consoleTabTraffic
}

// startTrafficTickerIfShowing restarts sampling for a console reopened onto
// the Traffic tab. Closing the modal lets the ticker retire, and nothing else
// would notice that the tab it was serving is back.
func (c *consoleModal) startTrafficTickerIfShowing() {
	if c.trafficViewVisible() {
		c.startTrafficTicker()
	}
}

func (c *consoleModal) startTrafficTicker() {
	c.mu.Lock()
	if c.trafficTicker != nil {
		c.mu.Unlock()
		return
	}
	// Sentinel: a stopped ticker is non-nil, so a second click while
	// the history RPC is in flight hits the guard above and returns.
	sentinel := time.NewTicker(24 * time.Hour)
	sentinel.Stop()
	c.trafficTicker = sentinel
	c.mu.Unlock()

	go func() {
		// Reload full history from the collector in the background — the
		// collector kept sampling while the tab was inactive, so we get
		// accurate per-second data. Running this off the UI goroutine
		// prevents blocking the Gio event loop on slow RPC calls.
		//
		// A 30-second timeout prevents a hung fetchTrafficHistory RPC
		// from keeping the sentinel alive forever, which would block all
		// future retry attempts when the user clicks the Traffic tab.
		loadCtx, loadCancel := context.WithTimeout(context.Background(), 30*time.Second)
		loadOK := c.loadTrafficHistory(loadCtx)
		loadCancel()
		c.invalidateWindow()

		// If the user navigated away — or closed the modal — while history
		// was loading, clean up the sentinel and exit without starting the
		// real ticker.
		if !c.trafficViewVisible() {
			c.mu.Lock()
			c.trafficTicker = nil
			c.mu.Unlock()
			return
		}

		// If the RPC failed or timed out, clear the sentinel so the
		// user can retry by clicking the Traffic tab again.
		// Note: loadOK is true even when the collector returned an empty
		// history (the cursor stays empty in that case) — empty history
		// is a valid response after a collector restart, not a failure.
		if !loadOK {
			c.mu.Lock()
			c.trafficTicker = nil
			c.mu.Unlock()
			return
		}

		// History is loaded — now start the real 1-second ticker. Each
		// tick appends only collector samples newer than the cursor set
		// by the load above, so no tick can race the load into producing
		// duplicate or bogus points.
		ticker := time.NewTicker(1 * time.Second)
		c.mu.Lock()
		c.trafficTicker = ticker
		c.mu.Unlock()

		stop := c.parent.uiStop()
		for {
			select {
			case <-stop:
				ticker.Stop()
				return
			case <-ticker.C:
				if !c.trafficViewVisible() {
					ticker.Stop()
					c.mu.Lock()
					c.trafficTicker = nil
					c.mu.Unlock()
					return
				}
				c.appendNewTrafficSamples()
				c.invalidateWindow()
			}
		}
	}()
}

// appendNewTrafficSamples pulls the tail of the collector's history (samples
// strictly newer than trafficLastTS) and appends it to the local graph state.
// Called once a second by the traffic ticker.
//
// The collector is the single source of truth for per-second deltas. The
// previous implementation computed deltas client-side from fetchNetworkStats,
// whose reply is a CACHED snapshot rebuilt at most every 500ms and only while
// a reader was active in the last 5s (see networkStatsRebuildIdleAfter). Any
// freeze→jump of that snapshot (gate re-arming after the tab was inactive, or
// the rebuild stalling on peerMu under a writer storm) packed several seconds
// of traffic into one "per-second" delta — a phantom spike that vanished on
// tab reopen because the refetched collector history never contained it.
// Pulling the collector's own samples makes the live view and the reloaded
// history identical by construction.
//
// If a tick is late (UI stall, system sleep), the collector kept sampling, so
// the next pull simply appends several correct 1-second bars instead of one
// inflated one.
func (c *consoleModal) appendNewTrafficSamples() {
	c.mu.RLock()
	since := c.trafficLastTS
	c.mu.RUnlock()

	samples, ok := c.fetchTrafficSamples(context.Background(), since)
	if !ok || len(samples) == 0 {
		return
	}

	c.mu.Lock()
	applied := false
	for _, s := range samples {
		// Cursor guard: skip anything not strictly newer than the last
		// applied sample. Defense-in-depth for a server that ignores the
		// "since" arg; also makes duplicate delivery harmless.
		if s.Timestamp <= c.trafficLastTS {
			continue
		}
		c.trafficSamplesIn = append(c.trafficSamplesIn, float32(s.BytesRecvPS))
		c.trafficSamplesOut = append(c.trafficSamplesOut, float32(s.BytesSentPS))
		c.trafficLastTS = s.Timestamp
		c.trafficTotalSent = s.TotalSent
		c.trafficTotalRecv = s.TotalReceived
		applied = true
	}
	if applied {
		// Trim to trafficMaxSamples to prevent unbounded growth.
		if len(c.trafficSamplesIn) > trafficMaxSamples {
			c.trafficSamplesIn = c.trafficSamplesIn[len(c.trafficSamplesIn)-trafficMaxSamples:]
		}
		if len(c.trafficSamplesOut) > trafficMaxSamples {
			c.trafficSamplesOut = c.trafficSamplesOut[len(c.trafficSamplesOut)-trafficMaxSamples:]
		}
	}
	c.mu.Unlock()
}

func (c *consoleModal) layoutTrafficTab(gtx layout.Context) layout.Dimensions {
	return layout.UniformInset(unit.Dp(0)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		ui.Fill(gtx, color.NRGBA{R: 21, G: 26, B: 34, A: 255})
		// 8dp panel padding matching the main window cards (window.go card).
		return layout.UniformInset(unit.Dp(8)).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
				// Title
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					label := material.H6(c.theme(), c.parent.t("console.traffic_title"))
					label.Color = color.NRGBA{R: 245, G: 247, B: 250, A: 255}
					return label.Layout(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(12)}.Layout),
				// Graph area
				layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
					return c.layoutTrafficGraph(gtx)
				}),
				layout.Rigid(layout.Spacer{Height: unit.Dp(8)}.Layout),
				// Legend (below graph)
				layout.Rigid(func(gtx layout.Context) layout.Dimensions {
					return c.layoutTrafficLegend(gtx)
				}),
			)
		})
	})
}

func (c *consoleModal) layoutTrafficLegend(gtx layout.Context) layout.Dimensions {
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficInSolid)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme(), c.parent.t("console.traffic_in"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(20)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficOutSolid)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme(), c.parent.t("console.traffic_out"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(20)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			return c.layoutColorDot(gtx, trafficTotalColor)
		}),
		layout.Rigid(layout.Spacer{Width: unit.Dp(6)}.Layout),
		layout.Rigid(func(gtx layout.Context) layout.Dimensions {
			lbl := material.Caption(c.theme(), c.parent.t("console.traffic_total"))
			lbl.Color = color.NRGBA{R: 196, G: 205, B: 218, A: 255}
			return lbl.Layout(gtx)
		}),
	)
}

func (c *consoleModal) layoutColorDot(gtx layout.Context, clr color.NRGBA) layout.Dimensions {
	sz := gtx.Dp(unit.Dp(10))
	defer clip.UniformRRect(image.Rectangle{Max: image.Pt(sz, sz)}, sz/2).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	return layout.Dimensions{Size: image.Pt(sz, sz)}
}

// trafficVisibleSlice returns a copy of the tail portion of data visible on the graph.
// A copy is required because the caller reads under RLock while the ticker goroutine
// may append to the original slice — sharing the backing array would be a data race.
func trafficVisibleSlice(data []float32, maxVisible int) []float32 {
	src := data
	if len(src) > maxVisible {
		src = src[len(src)-maxVisible:]
	}
	out := make([]float32, len(src))
	copy(out, src)
	return out
}

func (c *consoleModal) layoutTrafficGraph(gtx layout.Context) layout.Dimensions {
	// snapshot traffic data under read-lock to avoid races with ticker goroutine
	c.mu.RLock()
	visIn := trafficVisibleSlice(c.trafficSamplesIn, trafficGraphVisiblePoints)
	visOut := trafficVisibleSlice(c.trafficSamplesOut, trafficGraphVisiblePoints)
	totalSent := c.trafficTotalSent
	totalRecv := c.trafficTotalRecv
	c.mu.RUnlock()

	// reserve left margin for Y-axis labels
	yAxisWidth := gtx.Dp(unit.Dp(60))
	totalWidth := gtx.Constraints.Max.X
	height := gtx.Constraints.Max.Y
	if height <= 0 {
		height = gtx.Dp(unit.Dp(200))
	}
	graphWidth := totalWidth - yAxisWidth
	if graphWidth < 10 {
		graphWidth = 10
	}

	// dark background for entire area
	graphBg := color.NRGBA{R: 15, G: 19, B: 27, A: 255}
	defer clip.Rect{Max: image.Pt(totalWidth, height)}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: graphBg}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
	count := len(visIn)
	if len(visOut) > count {
		count = len(visOut)
	}

	// find max across in, out, and total for unified scale
	var maxVal float32
	for i := 0; i < count; i++ {
		var inVal, outVal float32
		if i < len(visIn) {
			inVal = visIn[i]
		}
		if i < len(visOut) {
			outVal = visOut[i]
		}
		total := inVal + outVal
		if total > maxVal {
			maxVal = total
		}
	}
	if maxVal < 1 {
		maxVal = 1
	}
	maxVal *= 1.1 // 10% headroom

	// draw Y-axis labels and horizontal grid lines (4 lines)
	gridColor := color.NRGBA{R: 40, G: 48, B: 60, A: 255}
	labelColor := color.NRGBA{R: 100, G: 110, B: 125, A: 255}
	for i := 1; i <= 4; i++ {
		y := height * i / 5
		drawHLine(gtx, yAxisWidth, totalWidth, y, gridColor)
		gridVal := maxVal * float32(5-i) / 5.0
		lbl := material.Caption(c.theme(), formatBytes(int64(gridVal))+"/s")
		lbl.Color = labelColor
		stack := op.Offset(image.Pt(0, y-gtx.Dp(unit.Dp(7)))).Push(gtx.Ops)
		lbl.Layout(gtx)
		stack.Pop()
	}

	if count == 0 {
		return layout.Dimensions{Size: image.Pt(totalWidth, height)}
	}

	fHeight := float32(height)
	fGraphW := float32(graphWidth)

	// all samples are spread across the full graph width
	visibleCount := count
	if visibleCount > trafficGraphVisiblePoints {
		visibleCount = trafficGraphVisiblePoints
	}

	// pixels per sample (fractional for smooth distribution)
	pxPerSample := fGraphW / float32(visibleCount)

	// bar width: half of step, minimum 1px; gap = 1px between in and out
	barW := int(pxPerSample/2) - 1
	if barW < 1 {
		barW = 1
	}

	// draw IN and OUT bars side by side across full width
	for i := 0; i < visibleCount; i++ {
		dataIdx := count - visibleCount + i
		var inVal, outVal float32
		if dataIdx < len(visIn) {
			inVal = visIn[dataIdx]
		}
		if dataIdx < len(visOut) {
			outVal = visOut[dataIdx]
		}

		// center of this sample on X axis
		centerX := yAxisWidth + int(float32(i)*pxPerSample+pxPerSample/2)

		// IN bar (green) — left of center
		if inVal > 0 {
			inH := int((inVal / maxVal) * fHeight)
			if inH < 1 {
				inH = 1
			}
			x0 := centerX - barW - 1
			drawRect(gtx, image.Rect(x0, height-inH, x0+barW, height), trafficInColor)
		}

		// OUT bar (blue) — right of center
		if outVal > 0 {
			outH := int((outVal / maxVal) * fHeight)
			if outH < 1 {
				outH = 1
			}
			x0 := centerX + 1
			drawRect(gtx, image.Rect(x0, height-outH, x0+barW, height), trafficOutColor)
		}
	}

	// draw Total line (in+out) across full width on top of bars
	if visibleCount >= 2 {
		drawTrafficLine(gtx, visIn, visOut, count, visibleCount, yAxisWidth, pxPerSample, fHeight, maxVal, trafficTotalColor)
	}

	// draw badges (Total In / Total Out) in the top-right corner of the graph
	c.drawTrafficBadges(gtx, totalWidth, height, totalSent, totalRecv)

	return layout.Dimensions{Size: image.Pt(totalWidth, height)}
}

// drawTrafficBadges renders Total In / Total Out stacked vertically
// inside a single rounded rectangle in the top-right corner of the graph.
func (c *consoleModal) drawTrafficBadges(gtx layout.Context, totalWidth, height int, totalSent, totalRecv int64) {
	inText := c.parent.t("console.traffic_total_in", formatBytes(totalRecv))
	outText := c.parent.t("console.traffic_total_out", formatBytes(totalSent))

	padH := gtx.Dp(unit.Dp(10))
	padV := gtx.Dp(unit.Dp(8))
	lineGap := gtx.Dp(unit.Dp(4))
	margin := gtx.Dp(unit.Dp(10))
	radius := gtx.Dp(unit.Dp(6))

	badgeBg := color.NRGBA{R: 30, G: 36, B: 48, A: 220}

	// measure text with unconstrained width
	measureGtx := gtx
	measureGtx.Constraints.Min = image.Point{}
	measureGtx.Constraints.Max = image.Pt(totalWidth, height)

	inMacro := op.Record(gtx.Ops)
	inLbl := material.Caption(c.theme(), inText)
	inLbl.Color = trafficInSolid
	inDims := inLbl.Layout(measureGtx)
	inCall := inMacro.Stop()

	outMacro := op.Record(gtx.Ops)
	outLbl := material.Caption(c.theme(), outText)
	outLbl.Color = trafficOutSolid
	outDims := outLbl.Layout(measureGtx)
	outCall := outMacro.Stop()

	// box size: widest text + padding, both lines stacked
	textW := inDims.Size.X
	if outDims.Size.X > textW {
		textW = outDims.Size.X
	}
	boxW := textW + padH*2
	boxH := inDims.Size.Y + lineGap + outDims.Size.Y + padV*2

	// position: top-right corner
	boxX := totalWidth - boxW - margin
	boxY := margin

	// draw single rounded background
	drawRoundedRect(gtx, image.Rect(boxX, boxY, boxX+boxW, boxY+boxH), radius, badgeBg)

	// draw In text
	s1 := op.Offset(image.Pt(boxX+padH, boxY+padV)).Push(gtx.Ops)
	inCall.Add(gtx.Ops)
	s1.Pop()

	// draw Out text below In
	s2 := op.Offset(image.Pt(boxX+padH, boxY+padV+inDims.Size.Y+lineGap)).Push(gtx.Ops)
	outCall.Add(gtx.Ops)
	s2.Pop()
}

func drawRoundedRect(gtx layout.Context, r image.Rectangle, radius int, clr color.NRGBA) {
	defer clip.UniformRRect(r, radius).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

// drawTrafficLine draws the total (in+out) as a continuous line
// connecting all non-zero sample points across zero gaps between clusters.
func drawTrafficLine(gtx layout.Context, visIn, visOut []float32, totalCount, visibleCount, yAxisWidth int, pxPerSample, height, maxVal float32, clr color.NRGBA) {
	const lineW = 2

	startIdx := totalCount - visibleCount
	prevX, prevY := -1, -1
	for i := 0; i < visibleCount; i++ {
		dataIdx := startIdx + i
		var inVal, outVal float32
		if dataIdx < len(visIn) {
			inVal = visIn[dataIdx]
		}
		if dataIdx < len(visOut) {
			outVal = visOut[dataIdx]
		}
		total := inVal + outVal
		if total <= 0 {
			continue // skip zeros but keep prev point for continuity
		}

		curX := yAxisWidth + int(float32(i)*pxPerSample+pxPerSample/2)
		curY := int(height - (total/maxVal)*height)

		if prevX >= 0 {
			drawLineBresenham(gtx, prevX, prevY, curX, curY, lineW, clr)
		} else {
			drawRect(gtx, image.Rect(curX, curY, curX+lineW, curY+lineW), clr)
		}

		prevX = curX
		prevY = curY
	}
}

// drawLineBresenham draws a line from (x0,y0) to (x1,y1) using
// Bresenham's algorithm, rendering each point as a [w x w] square.
func drawLineBresenham(gtx layout.Context, x0, y0, x1, y1, w int, clr color.NRGBA) {
	dx := x1 - x0
	dy := y1 - y0
	if dx < 0 {
		dx = -dx
	}
	if dy < 0 {
		dy = -dy
	}

	sx := 1
	if x0 > x1 {
		sx = -1
	}
	sy := 1
	if y0 > y1 {
		sy = -1
	}

	err := dx - dy
	for {
		drawRect(gtx, image.Rect(x0, y0, x0+w, y0+w), clr)
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x0 += sx
		}
		if e2 < dx {
			err += dx
			y0 += sy
		}
	}
}

func drawRect(gtx layout.Context, r image.Rectangle, clr color.NRGBA) {
	defer clip.Rect(r).Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

func drawHLine(gtx layout.Context, x0, x1, y int, clr color.NRGBA) {
	defer clip.Rect{Min: image.Pt(x0, y), Max: image.Pt(x1, y+1)}.Push(gtx.Ops).Pop()
	paint.ColorOp{Color: clr}.Add(gtx.Ops)
	paint.PaintOp{}.Add(gtx.Ops)
}

// peerDirectionArrow returns a Unicode arrow for the connection direction:
// ↑ for outbound (we initiated), ↓ for inbound (they connected to us).
func peerDirectionArrow(direction string) string {
	switch direction {
	case "outbound":
		return "↑"
	case "inbound":
		return "↓"
	default:
		return ""
	}
}

// peerDirectionColor returns the color for the direction arrow.
func peerDirectionColor(direction string) color.NRGBA {
	switch direction {
	case "outbound":
		return color.NRGBA{R: 100, G: 200, B: 255, A: 255} // light blue
	case "inbound":
		return color.NRGBA{R: 180, G: 130, B: 255, A: 255} // light purple
	default:
		return color.NRGBA{R: 196, G: 205, B: 218, A: 255} // gray
	}
}

func peerStateColors(state string) (color.NRGBA, color.NRGBA) {
	switch state {
	case "healthy", "active":
		return color.NRGBA{R: 36, G: 92, B: 63, A: 255}, color.NRGBA{R: 231, G: 255, B: 239, A: 255}
	case "degraded", "reconnecting":
		return color.NRGBA{R: 110, G: 82, B: 25, A: 255}, color.NRGBA{R: 255, G: 244, B: 210, A: 255}
	case "stalled", "retry_wait":
		return color.NRGBA{R: 118, G: 50, B: 37, A: 255}, color.NRGBA{R: 255, G: 225, B: 220, A: 255}
	case "dialing", "queued":
		return color.NRGBA{R: 40, G: 70, B: 110, A: 255}, color.NRGBA{R: 210, G: 230, B: 255, A: 255}
	default:
		return color.NRGBA{R: 57, G: 67, B: 84, A: 255}, color.NRGBA{R: 231, G: 237, B: 246, A: 255}
	}
}
