package desktop

import (
	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"

	"github.com/piratecash/corsa/internal/core/domain"
)

// menuFocusState carries the keyboard/Narrator focus contract of one context
// menu: which widget opened it, whether focus has been placed inside it yet,
// and whether focus should return to the opener once it closes.
//
// Before this existed, opening the identity menu pushed focus into the message
// composer sitting UNDER the overlay (openMenu set focusComposerPending), and
// opening a message menu left focus on the "⋯" button. Either way the open menu
// owned no focus at all, so Tab kept walking the background controls, Enter
// reached the composer and could send the draft, and Escape did nothing. A
// pointer user never noticed; a keyboard or Narrator user could not operate the
// menu.
//
// The contract implemented here is the usual one for a popup menu:
//
//   - on open, focus the first ENABLED item. A disabled row is not a Clickable
//     at all (see contextMenuItemDisabled), so it has no focus target to give
//     and simply never appears in the item list;
//   - while open, Tab/Shift+Tab — and Up/Down where the menu embeds no text
//     editor — cycle within the item list instead of escaping into the
//     background;
//   - Escape backs out of a confirmation or alias sub-view, or closes the menu;
//   - on close, focus returns to the trigger, unless something else has already
//     claimed it.
type menuFocusState struct {
	// pending marks a menu that has just opened, or has just returned to its
	// item list, and still needs its first item focused. It is cleared on the
	// frame that actually issues the FocusCmd.
	pending bool
	// trigger is the widget focus returns to when the menu closes: the "⋯"
	// button of the row or bubble the menu belongs to. It survives the close
	// because those buttons are cached per peer and per message ID.
	trigger event.Tag
	// held records whether focus sat on one of the menu's own items on the last
	// frame the menu was laid out. Only a menu that HELD focus restores it; one
	// the user never keyboard-touched leaves focus where it is.
	held bool
	// settle is the one frame of grace restoreOnClose allows itself before it
	// concludes that the focus it can see belongs to somebody else. See there.
	settle bool
	// verify marks the frame right after focus was handed back to the trigger.
	// Nothing in a frame reports whether a tag was laid out, so the hand-back
	// is checked by its result on the frame after it. See restoreOnClose.
	verify bool
	// want is the row this frame handed keyboard focus to, nil if this frame
	// moved no focus. menuScroll reads it to scroll that row into view. It is
	// deliberately set only where this type issues a FocusCmd, so a menu the
	// user is scrolling with a finger is never scrolled back by the keyboard
	// contract.
	want event.Tag
}

// open arms the contract for a menu being opened right now. trigger is the
// widget to hand focus back to on close.
func (m *menuFocusState) open(trigger event.Tag) {
	m.pending = true
	m.trigger = trigger
	m.held = false
	m.settle = false
	// A menu reopened between a hand-back and its check would otherwise have
	// the check answered by the new menu's focus.
	m.verify = false
}

// reopen re-arms first-item focus without touching the trigger. Escape backing
// out of a sub-view swaps the whole item set for a different one, and the
// widget that had focus is about to leave the frame.
func (m *menuFocusState) reopen() {
	m.pending = true
}

// menuNavKeys is which keys a surface lets its focus ring take.
//
// Every one of them is a key some surface needs for itself, so none can be
// assumed. Arrows must stay with a text editor's caret. Tab must stay with the
// console's completion popup, which uses it to accept the highlighted command
// — but only while that popup is showing, which is why this is decided per
// frame rather than per surface.
type menuNavKeys struct {
	// Arrows lets Up/Down step between items.
	Arrows bool
	// Tab lets Tab and Shift+Tab step between items. Turning it off gives up
	// containment for that frame: an unconsumed Tab reaches Gio's own focus
	// traversal and can leave the surface.
	Tab bool
}

// drive runs one frame of the contract for an OPEN menu. items are the menu's
// focusable widgets in the order they are drawn; keys selects which navigation
// keys the ring may take. It reports whether Escape was pressed; what Escape
// MEANS differs per menu, so the caller decides.
//
// Call it from the overlay, on the real frame context, AFTER the "is there room
// to draw" check and BEFORE the card is laid out:
//
//   - on the real context, because gtx.Execute is dropped by a disabled source
//     and the card is measured through one;
//   - after the room check, because a deferred draw lays out no items at all,
//     and focusing a tag the frame never mentions only drops focus;
//   - before the card, so the items read this frame's focus, not last frame's.
//
// Focusing a tag before the frame has registered it is fine: the router only
// drops focus at Frame time, and by then the card below has mentioned every
// item.
func (m *menuFocusState) drive(gtx layout.Context, items []event.Tag, keys menuNavKeys) bool {
	m.want = nil
	escape, step := readMenuNavKeys(gtx, keys)
	if escape {
		return true
	}
	if len(items) == 0 {
		// A deferred draw, not a close: the menu is open, its items are simply
		// not in this frame, so Gio drops their focus at Frame time. Re-arm
		// instead of forgetting — the frame that gets the room back reclaims
		// focus, and a close while deferred still finds held set and hands
		// focus to the trigger.
		//
		// Only for a menu that HELD focus. One the user opened by finger has
		// no claim on the keyboard, and yanking focus into it when the
		// keyboard finishes animating is the opposite of what they asked for.
		//
		// Focus resumes at the FIRST item rather than where it was. Nothing
		// survives to resume from — the router has dropped the focus and the
		// item set may have been swapped by a sub-view in the meantime — and a
		// remembered index would silently point at a different row.
		if m.held {
			m.pending = true
		}
		return false
	}
	cur := -1
	for i, it := range items {
		if gtx.Focused(it) {
			cur = i
			break
		}
	}
	switch {
	case m.pending:
		// Freshly opened, or freshly back from a sub-view: claim focus.
		gtx.Execute(key.FocusCmd{Tag: items[0]})
		m.want = items[0]
		m.pending = false
		m.held = true
	case cur >= 0:
		// Focus is ours. Step off it only when a navigation key says so.
		if step != 0 {
			next := ((cur+step)%len(items) + len(items)) % len(items)
			gtx.Execute(key.FocusCmd{Tag: items[next]})
			m.want = items[next]
		}
		m.held = true
	case !m.held && step == 0:
		// Focus is outside a menu that never held it: opened by pointer and
		// left alone. Not ours to take, and taking it would yank the caret out
		// of whatever the user is actually typing in. A navigation key changes
		// that, and is handled below.
	default:
		// Focus is outside the menu and we want it back. Two ways to get here.
		//
		// A navigation key while focus sits elsewhere: the background is not
		// ours to walk while a menu is open, so pull focus in rather than step
		// from a position the menu does not own.
		//
		// Or the menu HELD focus and the item holding it is no longer in the
		// list. That is not an edge case: items is rebuilt every frame from
		// live state, so "Delete chat and ask the peer" leaves the moment the peer
		// goes offline, "Delete message" the moment contextMenuDeleteEnabled
		// stops agreeing, and choosing a row that opens a confirmation swaps
		// the whole set. Whatever the cause, the frame is about to end without
		// mentioning the tag that holds focus, Gio will drop it at Frame time,
		// and an OPEN menu would be left with no focus at all and no key that
		// gets it back — the state this whole type exists to prevent.
		//
		// Focus lands on the first item, for the reason given in the deferred
		// branch above: nothing survives to resume from, and an index
		// remembered across a rebuilt list points at a different row.
		gtx.Execute(key.FocusCmd{Tag: items[0]})
		m.want = items[0]
		m.held = true
	}
	return false
}

// restoreOnClose hands focus back to the trigger after the menu has closed, and
// a frame later checks that the hand-back actually landed — moving focus to
// fallback when it did not. Call it once per frame, from layout(), for a menu
// that is NOT open.
//
// The "nothing is focused" guard is what keeps this from fighting the handlers
// that close a menu and deliberately focus something else — Reply focuses the
// composer, confirming an identity delete does too. Those run their FocusCmd on
// the frame of the click; this runs on the next one and finds focus taken. Gio
// drops focus at Frame time for any tag the frame did not mention, so a menu
// that closed while holding focus on one of its own items leaves focus nil,
// which is exactly, and only, the case that wants restoring.
//
// Focus being taken does not by itself mean somebody else took it: a menu that
// closes MID-frame keeps its focus until the end of that frame, because the drop
// happens in Router.Frame. Today every close sits below layout's closed-menu
// check, so the first frame that gets here already sees nil — but a future close
// moved above it would silently cost the user their place. Hence the one frame
// of grace: it costs a frame in the "somebody else took it" case and nothing
// else, since the restore below only ever fires into free focus.
//
// The trigger can be gone by the time it is offered focus. The "⋯" buttons are
// cached per peer and per message ID, so the widget itself outlives its row —
// but a widget that is not laid out is not a focus target. Router.Frame runs
// executeCommands FIRST and only then drops focus for a tag with no visible,
// focusable handler, so the FocusCmd is accepted and undone inside the same
// frame, and the user is left with focus on nothing: no origin for Tab, nothing
// for Narrator to announce, and no key that gets it back. A message deleted
// under its own open menu does exactly this — dropStaleMsgMenu closes the menu
// and the bubble that carried the trigger is already gone — and so does a row
// scrolled out of a long list, or a peer removed by the very menu standing on
// it.
//
// A frame cannot be asked whether a tag is in it, so this asks by doing: issue
// the FocusCmd, request one more frame, and read the answer there. Focus still
// empty means the trigger did not take it, and fallback — the composer, the one
// focus target every frame of this window draws — gets it instead. Focus on
// anything else means the trigger took it, or somebody with a better claim
// spoke up meanwhile; either way this stands down.
//
// The single deliberate focus clear in the window, dismissOnOutsideTap blurring
// the editor so the touch keyboard can come down, cancels the restore through
// abandonRestore rather than being read as a missing trigger.
func (m *menuFocusState) restoreOnClose(gtx layout.Context, fallback event.Tag) {
	m.pending = false
	if m.verify {
		m.verify = false
		if gtx.Focused(nil) && fallback != nil {
			gtx.Execute(key.FocusCmd{Tag: fallback})
		}
		return
	}
	if !m.held {
		return
	}
	if !gtx.Focused(nil) {
		if m.settle {
			m.held, m.settle = false, false
			return
		}
		m.settle = true
		// The grace frame has to be asked for, exactly like the verify frame
		// below. A surface closed mid-layout still draws its own widgets for
		// the rest of that frame, so the focus is dropped at the END of the
		// NEXT one — and a focus drop is a state change nobody filters for. A
		// close with no other invalidate behind it (Escape, a press on the
		// backdrop) would otherwise leave the hand-back parked here until some
		// unrelated input woke the loop.
		gtx.Execute(op.InvalidateCmd{})
		return
	}
	m.held, m.settle = false, false
	if m.trigger == nil {
		// Nothing to hand back to, so there is nothing to check either.
		if fallback != nil {
			gtx.Execute(key.FocusCmd{Tag: fallback})
		}
		return
	}
	gtx.Execute(key.FocusCmd{Tag: m.trigger})
	m.verify = true
	// The frame that reads the answer has to be asked for. A FocusCmd Gio
	// undoes at Frame time wakes nobody: the drop is a state change, not an
	// event anybody filters for, and a keyboard user waiting on the result is
	// producing no input that would draw the next frame by itself.
	gtx.Execute(op.InvalidateCmd{})
}

// abandonRestore cancels this menu's focus restore outright. Call it when focus
// was emptied on purpose, because empty focus has two readers above and both
// would undo the clear: the armed check reads it as "the trigger was not there to
// take it" and moves to the fallback, and the close itself reads it as its own
// doing and hands focus back to the trigger — then to the fallback a frame later
// when the trigger has left the frame.
//
// The caller runs while the menu can still be OPEN. A tap on a menu item is a tap
// outside every editor, so it clears focus near the top of the frame while the
// item's own handler closes the menu further down it. Dropping the check alone
// was a no-op there, and the whole restore ran on the next frame.
//
// A menu still open after this holds focus on nothing until a navigation key
// pulls it back in, which is the honest reading of a clear that just happened:
// the menu does not hold focus any more, and only a menu that HELD it restores it.
//
// pending and trigger stay. The tap that OPENS a menu is an outside tap too, so
// clearing pending could leave a touch-opened menu unfocused and unwalkable;
// trigger is where a restore this menu earns again later still belongs.
func (m *menuFocusState) abandonRestore() {
	m.held, m.settle, m.verify = false, false, false
}

// readMenuNavKeys drains this frame's menu navigation keys and reports the
// Escape flag plus the net navigation step (positive = forward).
//
// The filters carry no Focus target on purpose: they have to match wherever
// focus currently is, which is how an open menu takes Tab away from the
// background controls. Gio runs its own focus traversal only for a Tab that
// NOTHING handled (see the SystemEvent path in app.Window.processEvent), so
// consuming it here is what actually contains the menu.
//
// Escape is always taken. A surface that wants Escape for something of its own
// gets it through the caller's return value, not by leaving the key here.
func readMenuNavKeys(gtx layout.Context, keys menuNavKeys) (escape bool, step int) {
	filters := []event.Filter{key.Filter{Name: key.NameEscape}}
	if keys.Tab {
		filters = append(filters, key.Filter{Name: key.NameTab, Optional: key.ModShift})
	}
	if keys.Arrows {
		filters = append(filters,
			key.Filter{Name: key.NameUpArrow},
			key.Filter{Name: key.NameDownArrow},
		)
	}
	for {
		ev, ok := gtx.Event(filters...)
		if !ok {
			break
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		switch ke.Name {
		case key.NameEscape:
			escape = true
		case key.NameTab:
			if ke.Modifiers.Contain(key.ModShift) {
				step--
			} else {
				step++
			}
		case key.NameUpArrow:
			step--
		case key.NameDownArrow:
			step++
		}
	}
	return escape, step
}

// peerMenuItems lists the identity context menu's focusable widgets in the
// order they are drawn, for whichever sub-view is showing. The precedence
// mirrors contextMenuCard exactly, so the list can never describe a sub-view
// other than the one on screen.
func (w *Window) peerMenuItems() []event.Tag {
	switch {
	case w.showDeleteConfirm:
		return []event.Tag{&w.ctxMenuDeleteConfirm, &w.ctxMenuDeleteCancel}
	case w.showClearChatConfirm:
		return []event.Tag{&w.ctxMenuClearChatConfirm, &w.ctxMenuClearChatCancel}
	case w.showAliasEditor:
		// The editor comes first: it is where an alias is actually typed, and
		// the click handler for "Set alias" already focuses it directly.
		return []event.Tag{&w.aliasEditor, &w.ctxMenuAliasSave, &w.ctxMenuAliasCancel}
	}
	return []event.Tag{&w.ctxMenuAlias, &w.ctxMenuCopy, &w.ctxMenuDelete, &w.ctxMenuClearChat}
}

// msgMenuItems mirrors peerMenuItems for the message context menu, and for the
// reaction surfaces opened by the same gesture. The precedence mirrors
// layoutMsgContextMenuOverlay exactly, so the list can never describe a surface
// other than the one on screen.
//
// With the emoji panel up the ring is the panel's own controls: the search
// field, the close button, the nine category chips, and ONE stop for the whole
// grid — the cell the keyboard cursor is on, which the arrows then move
// (navigateReactionGrid).
//
// The grid is one stop rather than several hundred because a ring that listed
// every cell would take upwards of a minute to Tab past; the categories are all
// nine because nine is walkable, and leaving them out stranded a keyboard user
// in whichever category the panel happened to open on.
//
// The search field is hoisted to the front, ahead of the close button drawn
// above it. That is the same exception peerMenuItems makes for the alias
// editor: a panel is opened to pick something, and the field that filters is
// where picking starts.
//
// Delete drops out of the list on exactly the condition that renders it as a
// disabled label.
func (w *Window) msgMenuItems() []event.Tag {
	if w.reactionPickerOpen() {
		panel := &w.reactionRow.panel
		items := []event.Tag{&panel.Search, &panel.Close}
		items = append(items, panel.CategoryTags(w.emojiPickerCategories())...)
		if cursor := panel.CursorTag(); cursor != nil {
			items = append(items, cursor)
		}
		return items
	}
	items := []event.Tag{}
	if w.reactionRow.shown {
		items = w.reactionRow.row.Tags(w.reactionRow.quick)
	}
	items = append(items, &w.msgCtxReply, &w.msgCtxCopy)
	if w.contextMenuDeleteEnabled() {
		items = append(items, &w.msgCtxDelete)
	}
	return items
}

// escapePeerMenu applies Escape to the identity menu: one step back out of a
// confirmation or alias sub-view when one is showing, otherwise close. Stepping
// back rather than closing outright is what that sub-view's own Cancel item
// does, and is what a user who opened a confirmation by accident expects.
func (w *Window) escapePeerMenu() {
	backOut := func() {
		// The item list is mode 0 and starts at the top, same as a fresh open.
		// Doing it here rather than leaving it to the mode check in the overlay
		// keeps the restored list from showing one frame of the sub-view's
		// scroll offset.
		w.ctxMenuList.Position = layout.Position{}
		w.lastCtxMenuMode = 0
		w.peerMenuFocus.reopen()
	}
	switch {
	case w.showDeleteConfirm:
		w.showDeleteConfirm = false
		backOut()
	case w.showClearChatConfirm:
		w.showClearChatConfirm = false
		backOut()
	case w.showAliasEditor:
		w.showAliasEditor = false
		backOut()
	default:
		w.contextMenuPeer = domain.PeerIdentity{}
	}
	if w.window != nil {
		w.window.Invalidate()
	}
}

// contextMenuOpen reports whether an interaction overlay is currently up. The
// two context menus and identity details cover the composer, so for anything
// that asks "may I take focus?" they are one condition.
func (w *Window) contextMenuOpen() bool {
	return w.identityPanelVisible || !w.contextMenuPeer.IsZero() || w.msgContextMsg != nil ||
		w.consoleModalVisible()
}

// consumeComposerFocus decides what a pending "focus the composer" request may
// still do on the frame that consumes it. pending is the request, raiseKeyboard
// the touch-driven wish to bring the on-screen keyboard up with it, menuOpen
// whether a context menu is covering the composer.
//
// The request is raised from inside widgets and consumed a frame or more later,
// so by the time it is read the world may have moved on. A menu over the
// composer is the case that matters: the menu owns focus while it is up, and
// focus dropping to the editor underneath would leave Enter sending the draft
// instead of activating the highlighted item.
//
// The caller clears both flags either way. Returning "no" here means the
// request is gone, not queued.
func consumeComposerFocus(pending, raiseKeyboard, menuOpen bool) (focus, raise bool) {
	if !pending || menuOpen {
		return false, false
	}
	return true, raiseKeyboard
}

// escapeMsgMenu applies Escape to the message overlay: one step back out of the
// emoji panel to the reaction pill when the panel is up, otherwise close.
//
// Stepping back rather than closing outright is what the panel's own close
// button does, and is what a user who pressed "more" by accident expects — the
// same rule escapePeerMenu applies to the identity menu's sub-views.
func (w *Window) escapeMsgMenu() {
	if w.reactionPickerOpen() {
		w.closeReactionPicker()
		return
	}
	w.msgContextMsg = nil
	if w.window != nil {
		w.window.Invalidate()
	}
}
