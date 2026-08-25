package desktop

import (
	"context"
	"errors"
	"image"
	"slices"
	"time"

	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// reactions.go is the application's half of message reactions: the quick set
// offered above a message's context menu, the panel that opens behind "more",
// the chips under a bubble, and the cache they are drawn from.
//
// The cache is the point of this file's shape. A chat view lays out dozens of
// bubbles per frame, so what a bubble reads must already be in memory: the
// conversation's reactions are loaded once when it changes and after every
// local decision, never inside a frame.
//
// Every surface that offers a reaction — the pill, the panel behind "more", and
// a chip under the message — goes through toggleReactionOn, so there is one
// place a decision is made and one place its failure is reported.
//
// What is NOT here is the wire: the decision is stored and shown, and the node
// carries it to the peer a second or so later. See
// docs/refactoring/reactions-protocol.md.

// reactionRouter is everything a reaction decision needs from the router, and
// nothing else.
//
// It exists so the decision can be exercised without a node and a database
// behind it. That matters more here than elsewhere: the chip row is the one
// reaction surface whose press cannot be observed through its own widget —
// widget.Clickable discards pending clicks at the top of its Layout, so an
// UNWIRED row swallows a press exactly as silently as a wired one, and the only
// place the difference shows is the call this interface names.
type reactionRouter interface {
	MessageReactions(ctx context.Context, peer domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error)
	ToggleReaction(ctx context.Context, peer domain.PeerIdentity, messageID domain.MessageID, emoji string, now time.Time) (domain.ReactionFact, error)
	ReactionsUnsupportedBy(peer domain.PeerIdentity) bool
	// SetSendStatusIfCurrent replaces the status line only while it still says
	// what the caller expects, so a message that has become false can be taken
	// back without wiping whatever was written since.
	SetSendStatusIfCurrent(expected, replacement string) bool
	SetSendStatus(status string)
}

// reactions resolves the surface above. The Window still holds the concrete
// router for everything else; this narrows the dependency to the one place a
// reaction is decided, and lets a test stand in for it.
func (w *Window) reactions() reactionRouter {
	if w.reactionRouter != nil {
		return w.reactionRouter
	}
	if w.router == nil {
		return nil
	}
	return w.router
}

// defaultQuickReactions is the row offered above the message menu, in the order
// the design draws it (screen `3e`), LONGEST FIRST.
//
// Seven slots and the "more" button come to 365dp, which fits a 412dp phone with
// 8dp of inset either side; the eighth does not. That is not a reason to keep
// the list at seven: quickReactionsFor takes as many as the window has room for,
// so the tail is what a narrow screen loses, and everything is still one tap
// away behind the "more" button.
var defaultQuickReactions = []string{"👍", "👌", "❤️", "🔥", "😂", "😮", "😢", "🙏", "👎"}

// messageReactions is what the chip row under a message draws, read from the
// cache this file keeps rather than from the database: see the file comment.
func (w *Window) messageReactions(message service.DirectMessage) []ui.Reaction {
	reactions := w.msgReactionState[domain.MessageID(message.ID)]
	if len(reactions) == 0 {
		return nil
	}
	chips := make([]ui.Reaction, 0, len(reactions))
	for _, reaction := range reactions {
		chips = append(chips, ui.Reaction{
			Emoji: reaction.Emoji,
			Count: reaction.Count(),
			Mine:  reaction.Mine,
		})
	}
	return chips
}

// reloadReactions refreshes the whole conversation's reactions.
//
// Whole conversation rather than one message: the reads are one query either
// way, and a partial refresh would need a reason to believe nothing else
// changed — which after a peer's facts arrive is exactly the belief that is
// wrong.
func (w *Window) reloadReactions() {
	peer := w.snap.ActivePeer
	router := w.reactions()
	if router == nil || peer.IsZero() {
		w.msgReactionState = nil
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), reactionReadTimeout)
	defer cancel()

	state, err := router.MessageReactions(ctx, peer)
	if err != nil {
		// A node without chat history has nowhere to keep reactions; every
		// other failure is a database that will be asked again on the next
		// change. Neither is worth a message to the user, and neither may
		// leave stale chips from another conversation on screen.
		if !errors.Is(err, service.ErrNoReactionStore) {
			log.Warn().Err(err).Str("peer", peer.String()).Msg("load reactions")
		}
		w.msgReactionState = nil
		return
	}
	w.msgReactionState = state
}

// reactionReadTimeout bounds the two reads this file makes. They are local
// SQLite queries on the UI's own goroutine, so the budget is there to keep a
// stalled database from freezing the window rather than because the work is
// slow.
const reactionReadTimeout = 2 * time.Second

// messageReactionChips returns the chip row's widget state for one message,
// creating it on first access. Like every other per-message cache it is dropped
// on a conversation change (resetReplyOnPeerChange), which is what keeps it
// from growing across peers.
func (w *Window) messageReactionChips(id domain.MessageID) *ui.ReactionChipsState {
	chips := w.msgReactionChips[id]
	if chips == nil {
		if w.msgReactionChips == nil {
			w.msgReactionChips = make(map[domain.MessageID]*ui.ReactionChipsState)
		}
		chips = &ui.ReactionChipsState{Describe: w.describeReactionChip}
		w.msgReactionChips[id] = chips
	}
	return chips
}

// forgetPeerReactionNotice drops what was announced about one contact, so a
// later re-add is told again.
//
// Tied to the REMOVAL and not to "no conversation is open": the two are
// different events, and the UI reaches the second one on an ordinary Back. It is
// also the only place the first is visible — removing an active contact with
// others beside it selects the next one straight away, so the zero state never
// appears.
func (w *Window) forgetPeerReactionNotice(peer domain.PeerIdentity) {
	delete(w.reactionsLocalOnlyFor, peer)
}

// takeBackLocalOnlyNotice removes the "your reaction stayed here" line, if it is
// still what the line says.
//
// Conditional, which is what keeps it from wiping an unrelated status written
// since — a send, a delete. Compared against what was WRITTEN rather than
// against a fresh translation: the user may have changed language in between,
// and then the two differ and the notice would stay up for good.
func (w *Window) takeBackLocalOnlyNotice(router reactionRouter) {
	if w.reactionsLocalOnlyText == "" {
		return
	}
	router.SetSendStatusIfCurrent(w.reactionsLocalOnlyText, "")
	w.reactionsLocalOnlyText = ""
}

// describeReactionChip is what a screen reader is told about one chip.
//
// A press on a chip the user is part of takes their reaction back, and on any
// other one adds theirs, so the two cannot share an announcement — the same rule
// the quick-pick pill follows.
func (w *Window) describeReactionChip(reaction ui.Reaction) string {
	if reaction.Mine {
		return w.t("reaction.clear", reaction.Emoji)
	}
	return w.t("reaction.apply", reaction.Emoji)
}

// reactionRowState is the pill above the message menu: its widgets, and which
// message has the full emoji panel open over it.
type reactionRowState struct {
	row ui.ReactionPickerRowState
	// panel is the "more" surface's own state, NOT the composer's.
	//
	// Sharing one would alias two panels that can be on screen at the same
	// time — a message menu opens over a composer whose picker is already up —
	// onto one search field, one scroll offset and one set of grid buttons. The
	// visible half of that is two panels typing into each other; the sharp half
	// is that handleEmojiActions drains the composer's clicks before the
	// overlay is laid out, so a tap meant as a reaction would have been
	// inserted into the draft instead. Only the recents are shared, through the
	// filter (emojiChoicesFor).
	panel ui.EmojiPickerState
	// pickerFor is the OPEN MENU whose "more" panel (screen `3h`) is up, and
	// nil when none is. It is compared against msgContextMsg by pointer.
	//
	// Not a bool, because the panel belongs to the open menu and must not
	// outlive it. Nine different paths close that menu — Escape, the backdrop,
	// each of its three items, a console modal opening over it, the
	// conversation changing, the message being deleted — and a flag would have
	// to be cleared at all nine. Eight of them would be found; the ninth would
	// reopen a menu with a 250dp emoji panel already covering it.
	//
	// Not the message's ID either, which was the first cut and worked for every
	// message but the one it mattered for: dismissing the panel by pressing the
	// backdrop cleared msgContextMsg and left the ID behind, so reopening the
	// menu on THAT SAME message came straight back up as a panel, on the query
	// the user had abandoned. openMsgMenu stores a fresh copy of the message
	// each time, so the pointer is one menu's identity rather than one
	// message's, and every close invalidates it for free.
	pickerFor *service.DirectMessage
	// shown is whether the pill was given room on the frame being laid out, and
	// quick is which of the default reactions fitted across it. Both are written
	// by the overlay before it builds the focus ring and read by msgMenuItems,
	// so the ring and the surface can never disagree about which slots are on
	// screen — a ring item the frame does not draw loses its focus at Frame time
	// and is pulled back every frame after.
	shown bool
	quick []string
	// drawn is the list the pill was last LAID OUT with, which is not always
	// `quick`: the overlay recomputes quick for this frame's width before the
	// presses of the previous frame are read. A window narrowed between the two
	// drops slots from quick, and a press on one of them would then never be
	// drained — Gio discards the event at Frame time for a tag nothing asked
	// about, so the tap is simply lost.
	drawn []string
}

// reactionPickerOpen reports whether the full emoji panel is standing over the
// message menu right now.
func (w *Window) reactionPickerOpen() bool {
	return w.msgContextMsg != nil && w.reactionRow.pickerFor == w.msgContextMsg
}

// closeReactionPicker steps back from the panel to the pill, leaving the menu
// open. This is what the panel's own close button and Escape do; closing the
// menu outright needs no counterpart, since the panel is keyed by the open menu
// itself (see pickerFor).
func (w *Window) closeReactionPicker() {
	w.reactionRow.pickerFor = nil
	// The ring's first item is about to leave the frame with the panel.
	w.msgMenuFocus.reopen()
	if w.window != nil {
		w.window.Invalidate()
	}
}

// noteReactionsChanged marks the reaction cache stale and asks for a frame.
//
// It runs on the EVENT BUS's goroutine, and everything a Window holds belongs to
// the layout goroutine: w.msgReactionState is a map read by every bubble on
// every frame, and a write from here is a concurrent map access — a hard crash,
// not a stale value. Invalidate() is not a barrier and would not make it one.
//
// So the only thing crossing the goroutine boundary is one atomic flag, and the
// reload itself happens in reloadStaleReactions at the top of the next frame.
// The peer is deliberately NOT compared here either: w.snap belongs to the
// layout goroutine too, and a flag costs one query on a frame that was going to
// be drawn anyway.
func (w *Window) noteReactionsChanged() {
	w.reactionsStale.Store(true)
	if w.window != nil {
		w.window.Invalidate()
	}
}

// reloadStaleReactions picks up what noteReactionsChanged flagged. Called from
// layout, on the goroutine that owns the cache.
func (w *Window) reloadStaleReactions() {
	if w.reactionsStale.Swap(false) {
		w.reloadReactions()
		w.announceReactionsAreLocalOnly()
	}
}

// announceReactionsAreLocalOnly says once, per conversation, that this peer's
// build cannot receive reactions.
//
// The tap cannot say it on its own. A tap QUEUES the fact and the send happens a
// second or so later, so at the moment the user is told "sent" the refusal is
// not yet known — and without this the first reaction to an old client always
// looks delivered and the news arrives on the NEXT tap, about the previous one.
//
// Called from two places, and both are needed. After a reload, because that is
// what the node's announcement triggers. And on a chat SWITCH, because the
// announcement carries a peer while the flag that crosses into the layout
// goroutine does not: a refusal learned for A while B is open is picked up by
// B's frame, which asks about B and finds nothing to say. Checking on entry is
// what makes A's notice appear when the user goes back to it.
func (w *Window) announceReactionsAreLocalOnly() {
	peer := w.snap.ActivePeer
	router := w.reactions()
	if router == nil {
		return
	}
	if peer.IsZero() {
		// No conversation open — Back in the compact layout, nothing picked yet,
		// or the last contact removed. The notice describes a chat that is not
		// on screen, and in the two-panel layout the status line still is, so it
		// would sit there talking about somebody the user is not looking at.
		//
		// Only the visible line goes. What has been ANNOUNCED per conversation
		// is not forgotten here: this state is reached by an ordinary Back, and
		// clearing it would say the same thing again on the way in. The record
		// of a conversation that no longer exists is dropped where the contact
		// is (forgetPeerReactionNotice).
		w.takeBackLocalOnlyNotice(router)
		return
	}
	if !router.ReactionsUnsupportedBy(peer) {
		// Not (or no longer) refused, so the notice goes — WITHOUT asking
		// whether it was this conversation that put it there. The status line is
		// one line for the whole window: a notice raised in a chat that cannot
		// take reactions is still on it after the user walks into one that can,
		// and a check per conversation leaves it standing exactly there.
		//
		w.takeBackLocalOnlyNotice(router)
		// And a peer that upgrades gets to be announced again if it ever turns
		// out not to have.
		delete(w.reactionsLocalOnlyFor, peer)
		return
	}
	if w.reactionsLocalOnlyFor[peer] {
		return
	}
	if w.reactionsLocalOnlyFor == nil {
		w.reactionsLocalOnlyFor = map[domain.PeerIdentity]bool{}
	}
	w.reactionsLocalOnlyFor[peer] = true
	w.reactionsLocalOnlyText = w.t("status.reaction_local_only")
	router.SetSendStatus(w.reactionsLocalOnlyText)
}

// applyReaction records the chosen emoji against the message the menu is open
// on, and closes the surfaces that offered it.
//
// Closing first and deciding second: every branch of the decision returns
// somewhere, and a menu left standing over a reaction that has already been made
// is the same wrong state whichever branch it was.
func (w *Window) applyReaction(emoji string) {
	message := w.msgContextMsg
	w.reactionRow.pickerFor = nil
	w.msgContextMsg = nil
	defer func() {
		if w.window != nil {
			w.window.Invalidate()
		}
	}()
	if message == nil {
		return
	}
	w.toggleReactionOn(domain.MessageID(message.ID), emoji)
}

// toggleReactionOn is the one place a reaction decision is made, whichever
// surface asked for it: the pill above a menu, the panel behind "more", or a
// chip under the message.
//
// One tap means "the opposite of what I have now", and which of the two that is
// is decided by the service against STORED state — not here against the chips,
// which are a frame old, so two quick taps read from them would both decide
// "set".
func (w *Window) toggleReactionOn(messageID domain.MessageID, emoji string) {
	peer := w.snap.ActivePeer
	router := w.reactions()
	if router == nil || peer.IsZero() || messageID == "" || emoji == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), reactionReadTimeout)
	defer cancel()
	// Choosing an emoji as a reaction is choosing it, so it joins the recents
	// the same as picking one for the draft does — the panel serves both, and a
	// recents list that only learns from the composer is one the reaction picker
	// never fills. Taking a reaction BACK counts too: the user picked that emoji
	// to act on, and which direction the toggle went is not what "recently used"
	// is about.
	w.rememberEmojiAt(emoji, time.Now())
	if _, err := router.ToggleReaction(ctx, peer, messageID, emoji, time.Now().UTC()); err != nil {
		if errors.Is(err, service.ErrNoReactionStore) {
			// This node keeps no chat history, so there is nowhere for a
			// reaction to live and there never will be. Nothing to redraw and
			// nothing the user can do about it.
			return
		}
		log.Warn().Err(err).Str("message", string(messageID)).Str("emoji", emoji).Msg("toggle reaction")
		// The tap did not take — the write lost a race, it crossed a storage
		// ceiling, or the conversation is being wiped. All three leave the chips
		// exactly as they were, and saying so is the point of the error: without
		// it the surface closes on a reaction the user believes they made.
		// Reloaded because the state they were shown may also be what the
		// winning write changed.
		w.reloadReactions()
		router.SetSendStatus(w.t("status.reaction_not_saved"))
		w.redrawAfterReaction()
		return
	}
	w.reloadReactions()
	// Says it now if the answer is already known — a peer refused earlier in
	// this session — and announceReactionsAreLocalOnly says it if the node only
	// finds out after the frame goes out.
	w.announceReactionsAreLocalOnly()
	w.redrawAfterReaction()
}

// redrawAfterReaction asks for the frame that shows what the tap did.
//
// The chip route needs it and the menu routes do not, which is why it is here
// rather than at one call site: a bubble's chips are laid out from a slice taken
// BEFORE the presses are drained, so the frame that handles the tap still draws
// the old counter — and the frame after it is only guaranteed by the two-second
// heartbeat. Taking back the last reaction leaves the chip on screen for that
// long.
func (w *Window) redrawAfterReaction() {
	if w.window != nil {
		w.window.Invalidate()
	}
}

// handleReactionChipTap answers a press on a chip under a message.
//
// A chip is the shortest way to say "me too" — or to take it back — so it means
// the same as picking that emoji from the pill: the opposite of what this user
// holds now. Which of the two that is stays the service's decision, against
// stored state rather than against the chip on screen.
//
// It runs on the message's own layout, not through the context menu, so the
// menu's `msgContextMsg` is untouched: tapping a chip is not opening a menu.
func (w *Window) handleReactionChipTap(
	gtx layout.Context,
	chips *ui.ReactionChipsState,
	message service.DirectMessage,
	drawn []ui.Reaction,
) {
	for {
		emoji, ok := chips.Clicked(gtx, drawn)
		if !ok {
			return
		}
		w.toggleReactionOn(domain.MessageID(message.ID), emoji)
	}
}

// closeEmojiSurfaces takes down everything the emoji and reaction gestures put
// on screen: the composer's picker, the reaction pill and the panel over it,
// and the message menu they belong to.
//
// Sending a message ends the composing gesture, and a surface that outlives its
// gesture is a surface the user has to dismiss by hand before they can see what
// they just sent. The message menu goes with the pill rather than being left
// behind: the pill is drawn as part of that menu, so closing one and not the
// other leaves a menu with a hole where its top half was.
func (w *Window) closeEmojiSurfaces(gtx layout.Context) {
	w.closeEmojiPicker(gtx)
	if w.msgContextMsg == nil && w.reactionRow.pickerFor == nil {
		return
	}
	w.reactionRow.pickerFor = nil
	w.msgContextMsg = nil
	// The press that was queued this frame is dropped with them. Without it,
	// tapping a quick slot in the same frame as a send leaves the click waiting
	// and it answers the NEXT menu that opens — on whatever message that is.
	// Every other dismissal path pairs the two for the same reason.
	w.dropReactionClicks(gtx)
	// The focus ring's items are leaving the frame with them; without this the
	// keyboard would be left on a widget the next frame does not draw.
	w.msgMenuFocus.reopen()
	if w.window != nil {
		w.window.Invalidate()
	}
}

// dropReactionClicks discards every press queued this frame on the reaction
// surfaces. A key that already dismissed them has answered the gesture; the tap
// behind it must not answer it again the next time a menu opens — the same
// bargain dropEmojiToggleClicks makes for the composer's toggle.
func (w *Window) dropReactionClicks(gtx layout.Context) {
	w.reactionRow.row.DropClicks(gtx)
	w.reactionRow.panel.DropClicks(gtx)
}

// navigateReactionGrid moves the keyboard cursor around the open panel's grid.
//
// The arrows are only claimed while the cursor already HOLDS focus. Left and
// Right belong to the caret otherwise, and a filter is consumed by reading it:
// registering for them unconditionally would take them away from the search
// field the user is typing in. Enter needs no handling at all — a focused
// widget.Clickable activates itself on Return and Space, so the existing click
// drain reports the choice.
//
// Stepping off the TOP of the grid returns to the search field, which is where
// the grid was entered from. Stepping off the other three edges does nothing:
// there is nowhere better to go, and wrapping a grid moves the cursor somewhere
// the user was not pointing.
func (w *Window) navigateReactionGrid(gtx layout.Context) {
	panel := &w.reactionRow.panel
	if !panel.ChoiceFocused(gtx) {
		return
	}
	steps := map[key.Name]image.Point{
		key.NameLeftArrow:  {X: -1},
		key.NameRightArrow: {X: 1},
		key.NameUpArrow:    {Y: -1},
		key.NameDownArrow:  {Y: 1},
	}
	filters := make([]event.Filter, 0, len(steps))
	for name := range steps {
		filters = append(filters, key.Filter{Name: name})
	}
	for {
		ev, ok := gtx.Event(filters...)
		if !ok {
			return
		}
		ke, ok := ev.(key.Event)
		if !ok || ke.State != key.Press {
			continue
		}
		step := steps[ke.Name]
		switch {
		case panel.MoveCursor(step.X, step.Y):
			gtx.Execute(key.FocusCmd{Tag: panel.CursorTag()})
		case step.Y < 0:
			gtx.Execute(key.FocusCmd{Tag: &panel.Search})
		}
	}
}

// handleReactionRowActions drains the pill and the panel behind it. Called from
// the overlay, on the real frame context, before either is laid out.
func (w *Window) handleReactionRowActions(gtx layout.Context) {
	if w.reactionPickerOpen() {
		w.navigateReactionGrid(gtx)
		if w.reactionRow.panel.CloseClicked(gtx) {
			w.closeReactionPicker()
			return
		}
		for {
			categoryID, ok := w.reactionRow.panel.CategoryClicked(gtx)
			if !ok {
				break
			}
			w.reactionRow.panel.SelectCategory(categoryID)
		}
		for {
			value, ok := w.reactionRow.panel.Clicked(gtx)
			if !ok {
				break
			}
			w.applyReaction(value)
		}
		return
	}

	// Drained whether or not the pill has room THIS frame. A press is read on
	// the frame after the one that drew it, and between the two the window can
	// narrow or a keyboard can come up and leave no room at all — and a press
	// nobody asks about is discarded at Frame time rather than postponed. The
	// user pressed a slot that was on screen; that it has since gone is not
	// their answer.
	if w.reactionRow.row.MoreClicked(gtx) {
		w.openReactionPicker()
	}
	for {
		value, ok := w.reactionRow.row.Clicked(gtx, w.reactionRowPressable())
		if !ok {
			break
		}
		w.applyReaction(value)
	}
}

// reactionRowPressable is every slot a press could still be waiting on: the ones
// this frame will draw, plus the ones the LAST frame drew.
//
// The two differ exactly when the window has been resized between them, which is
// when the tail slots disappear — and a slot that is not asked about does not
// just go unanswered, it takes the tap with it.
func (w *Window) reactionRowPressable() []string {
	pressable := append([]string{}, w.reactionRow.quick...)
	for _, value := range w.reactionRow.drawn {
		if !slices.Contains(pressable, value) {
			pressable = append(pressable, value)
		}
	}
	return pressable
}

// openReactionPicker raises the full panel over the row.
//
// The query is cleared on every open: a panel that opened on the previous
// message's search shows one stale match with no category highlighted, which
// reads as broken and is explained only by small text nobody is looking at.
func (w *Window) openReactionPicker() {
	if w.msgContextMsg == nil {
		return
	}
	if w.reactionRow.panel.Category() == "" {
		w.reactionRow.panel.SelectCategory(string(emojiCategorySmileys))
	}
	w.reactionRow.panel.ResetSearch()
	w.reactionRow.panel.RevealCategory()
	w.reactionRow.panel.ResetCursor()
	w.reactionRow.pickerFor = w.msgContextMsg
	// The ring's items are about to be replaced by the panel's.
	w.msgMenuFocus.reopen()
}

// layoutReactionRow draws the pill at the position the caller has decided, and
// reports how much room it took so the menu below it can be placed under it.
func (w *Window) layoutReactionRow(gtx layout.Context) layout.Dimensions {
	// What is about to be drawn is what the NEXT frame has to drain, whatever
	// the window does to the count in between.
	w.reactionRow.drawn = w.reactionRow.quick
	return w.kit().ReactionPickerRow(gtx, &w.reactionRow.row, ui.ReactionPickerRow{
		Emojis:   w.reactionRow.quick,
		Selected: w.holdsReaction,
		MoreIcon: w.chevronDownIcon,
		MoreHint: w.t("reaction.more"),
		Describe: func(value string, held bool) string {
			// A tap on a reaction the user holds takes it back, so the two
			// states cannot share one announcement.
			if held {
				return w.t("reaction.clear", value)
			}
			return w.t("reaction.apply", value)
		},
	})
}

// holdsReaction reports whether this user already holds one emoji on the message
// the menu is open on.
//
// The pill fills every slot it answers true for, so the row says which way each
// tap will go: the same tap clears a reaction that stands and sets one that does
// not, and without the fill the two are indistinguishable until after the fact.
//
// EVERY one, not the first: a user holds as many reactions as they like, and
// marking one of five made the other four look un-chosen — which is what the
// first cut did and what made the pill unreadable.
//
// Read from the cache the chips are drawn from, so the pill and the chips under
// the message cannot disagree.
func (w *Window) holdsReaction(emoji string) bool {
	if w.msgContextMsg == nil {
		return false
	}
	for _, reaction := range w.msgReactionState[domain.MessageID(w.msgContextMsg.ID)] {
		if reaction.Mine && reaction.Emoji == emoji {
			return true
		}
	}
	return false
}

// quickReactionsFor is the set of quick choices the pill can show across the
// window it is drawn in, longest first. The design's seven need 365dp; a
// narrower screen gets fewer rather than a pill hanging off its right edge.
//
// The budget is the window less the edge the overlay is placed with
// (msgOverlayEdgeDp), so the count and the placement agree on how much room
// there really is.
func (w *Window) quickReactionsFor(gtx layout.Context, windowW int) []string {
	capacity := ui.ReactionPickerRowCapacity(gtx, msgOverlayRoom(gtx, windowW))
	return defaultQuickReactions[:min(capacity, len(defaultQuickReactions))]
}

// reactionRowSize is how big the pill comes out this frame. The overlay needs
// it before the pill is drawn, to place the pill and the menu card as one
// block; measuring by laying the pill out would drain the clicks it is about to
// answer.
func (w *Window) reactionRowSize(gtx layout.Context) image.Point {
	return ui.ReactionPickerRowSize(gtx, len(w.reactionRow.quick))
}

// layoutReactionPicker draws the full emoji panel over the row, at the size the
// caller has already applied.
func (w *Window) layoutReactionPicker(gtx layout.Context) layout.Dimensions {
	panel := &w.reactionRow.panel
	choices := emojiChoicesFor(panel, gtx.Enabled(), w.emojiPicker.recents)
	return w.kit().EmojiPicker(gtx, panel, w.emojiPickerDescriptor(panel, ui.EmojiPickerModeReaction, choices))
}

// reactionPickerSize is how big the panel wants to be, bounded by availH — the
// height above the on-screen keyboard, not the whole window — and by the width
// of the window it is drawn in. Height is the panel's own preference, clamped.
// A zero size means there is no room for it at all.
//
// The width is the one that holds the design's seven columns, NOT the width of
// the pill it replaces: matching the pill looked tidy and cost a column (see
// ui.EmojiPickerWidthForColumns).
func (w *Window) reactionPickerSize(gtx layout.Context, availH int) image.Point {
	width := msgOverlayWidth(gtx,
		ui.EmojiPickerWidthForColumns(gtx, ui.EmojiPickerGridColumns), gtx.Constraints.Max.X)
	height := min(gtx.Dp(ui.EmojiPickerDesiredHeightDp), availH)
	if height < ui.EmojiPickerMinHeight(gtx, ui.EmojiPickerModeReaction) {
		// Too short to show a single emoji. Drawing a clipped strip with no
		// reachable cell is worse than drawing nothing, for the reason given on
		// ui.EmojiPickerMinHeight.
		return image.Point{}
	}
	if width < ui.EmojiPickerMinWidth(gtx, ui.EmojiPickerModeReaction) {
		// Too narrow for the panel's own header. The window gate upstream only
		// asks for the MENU CARD's chrome, which is a fraction of what a close
		// button laid out at an exact square needs; without this the panel kept
		// being drawn with its only way out hanging off the edge.
		return image.Point{}
	}
	return image.Pt(width, height)
}

// drawAt records a widget and replays it at an offset, which is how both the
// pill and the panel are placed: they are measured against the whole overlay
// and then moved, and neither can be laid out twice to find that out.
func drawAt(gtx layout.Context, origin image.Point, size image.Point, widget layout.Widget) layout.Dimensions {
	inner := gtx
	inner.Constraints = layout.Exact(size)
	macro := op.Record(gtx.Ops)
	dims := widget(inner)
	call := macro.Stop()

	offset := op.Offset(origin).Push(gtx.Ops)
	call.Add(gtx.Ops)
	offset.Pop()
	return dims
}
