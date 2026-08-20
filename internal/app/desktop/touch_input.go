package desktop

import (
	"image"
	"os"
	"sync/atomic"
	"time"

	"gioui.org/io/event"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/unit"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Touch input support. Gio's Windows backend registers the window for raw
// WM_POINTER input, which disables the system "press and hold → right
// click" promotion, and its ShowTextInput is a no-op, so the on-screen
// keyboard never appears for our custom-rendered editors. Both gaps are
// filled here: a long-press gesture that opens the same context menus as a
// right click, and an explicit touch-keyboard invocation when an editor is
// tapped with a finger.
const (
	// longPressDuration is how long a touch must stay put before it is
	// treated as a context-menu request (matches common OS defaults).
	longPressDuration = 500 * time.Millisecond
	// longPressSlop is the maximum finger travel allowed during the hold.
	longPressSlop = unit.Dp(12)
	// keyboardHideDelay is how long every editor in a window must stay
	// unfocused before a keyboard we opened is asked to hide (absorbs
	// focus hops between editors across frames).
	keyboardHideDelay = 400 * time.Millisecond
	// keyboardShowDebounce collapses duplicate show dispatches from one
	// physical tap. Deliberately short: a longer window would block the
	// user from quickly re-opening a keyboard they closed manually.
	keyboardShowDebounce = 300 * time.Millisecond
	// keyboardShowCoalesce is how long a dispatched show that has NOT yet put
	// a keyboard on screen keeps the field to itself. Past the debounce, a
	// re-tap used to bump showGen, which cancels the in-flight show and starts
	// a fresh one — and a fresh one begins its retry budget at zero, so a user
	// tapping every second at a field that is showing nothing would restart the
	// attempt forever and never reach the fallback that would have worked.
	//
	// The number has to cover the whole chain a single show can still be
	// winning at, measured from the tap:
	//
	//	 ~0.15s  the settle before TryShow
	//	  0.7s   tkShowVerifyDelay, after which doVerifyShow asks whether the
	//	         accepted show actually produced a keyboard
	//	  1.2s   tkLegacyVerifyPolls × tkLegacyVerifyDelay, the fallback
	//	         waiting for evidence from the legacy host
	//	 ~0.5s   starting TabTip.exe
	//	  1.5s   tkLegacyHostPolls × tkLegacyVerifyDelay after that restart
	//
	// which lands just past four seconds; 5s is that with room for COM latency
	// on a cold tablet. An earlier revision of this constant said 3.5s and
	// derived it from the legacy path alone — it forgot that the legacy path
	// does not even BEGIN until the 700ms verification has come back negative,
	// so it cut the fallback off in its final second, which is precisely the
	// second that produces the keyboard.
	//
	// It is deliberately NOT the ~15s of the full retry ladder
	// (keyboardShowRetryMax), even though a tap at 6s does still restart that
	// ladder. Locking a field out for fifteen seconds is the worse failure: the
	// ladder's long tail is COM init failing over and over, which is not a case
	// where waiting quietly has ever produced a keyboard, and taking the user's
	// tap away is how this subsystem looked broken in the first place. The
	// window is also released early by any keyboard that DOES appear (see
	// keyboardTapVerdict), so the full 5s is only ever spent on a show that has
	// put nothing on screen at all.
	keyboardShowCoalesce = 5000 * time.Millisecond
	// keyboardLegacyClearanceDp is the bottom clearance published on the
	// LEGACY show path (old Win10 with no WinRT InputPane factory), where
	// OccludedRect cannot be measured at all. It is a deliberately
	// conservative fixed value — enough to lift the composer clear of a
	// docked TabTip keyboard — because no real geometry is available there;
	// keyboardInsetDp still bounds it by the window on short windows. Only
	// reached when the factory is genuinely absent, never on modern Windows.
	keyboardLegacyClearanceDp = 280 //nolint:unused // referenced only from touch_keyboard_windows.go
)

// touchKeyboardState is per-UI-window on-screen-keyboard state: the height
// of that window's area occluded by the keyboard, and the trigger that
// wakes its frame loop. The main and console windows are separate OS
// windows with separate frame loops, so each owns its own state — the
// platform layer binds it to the tapped window's native handle at show
// time.
type touchKeyboardState struct {
	occludedDp        atomic.Int32
	invalidate        atomic.Value   // func()
	monitorOwner      atomic.Int64   //nolint:unused // referenced only from touch_keyboard_windows.go; Windows: id of the running occlusion monitor (0 = none)
	monitorSeq        atomic.Int64   //nolint:unused // referenced only from touch_keyboard_windows.go; Windows: monitor id allocator
	hwnd              atomic.Uintptr // native handle, bound at tap/show time (Windows)
	viewHwnd          atomic.Uintptr // THIS window's own native handle from ViewEvent (Windows); authoritative, never overwritten by a foreground read
	shownByUs         atomic.Bool    // the keyboard was opened by our TryShow
	lastShow          atomic.Int64   // unix nanos of the last show request (per-window debounce)
	showGen           atomic.Int64   // show-request generation; dispatched shows, outside-taps and release bump it to cancel stale in-flight shows
	showActiveGen     atomic.Int64   // the showGen of the show that is IN FLIGHT right now (0 = none). Stored at dispatch, cleared by finishShow at every terminal end of that show and by a seen keyboard. The coalesce holds a tap only while this still equals showGen, so a show that died early stops claiming to be working
	showVisibleGen    atomic.Int64   // visibleGen as it stood when the last show was DISPATCHED; while visibleGen still equals it, that show has yet to put a keyboard on screen, and a re-tap inside keyboardShowCoalesce is coalesced into it rather than restarting it
	visibleGen        atomic.Int64   // Windows: bumped ONLY where a keyboard has been SEEN — a pane Showing event, an occluding or located pane, the legacy host window on screen. Never on a merely accepted TryShow. This is the coalesce's release signal and nothing weaker may be substituted for it
	sessionGen        atomic.Int64   // Windows: bumped on ACCEPTED shows (keyboard sessions); OwnerExpire binds to it, so cancelled/refused requests can't strand ownership. Deliberately NOT the coalesce signal: TryShow returning true is a statement about the request, not about the screen
	paneEventGen      atomic.Int64   //nolint:unused // referenced only from touch_keyboard_windows.go; Windows: pane Showing/Hiding event sequence, assigned IN the callbacks in event order (Showing Add, Hiding Load) so a Hiding always carries its own Showing's number regardless of command-processing lag
	shownGen          atomic.Int64   //nolint:unused // referenced only from touch_keyboard_windows.go; Windows: paneEventGen of the last PROCESSED Showing (set by the Showing handler / reconcile); the Hiding handler drops any Hiding whose carried gen != this (a newer Showing has since superseded it)
	adoptedGen        atomic.Int64   // Windows: shownGen of a session ADOPTED by reconcilePaneVisible (0 = none), i.e. a pane that was already up when we started listening. Its Hiding may have been raised BEFORE the synthesizing CAS and so carries the PRE-CAS generation; while this equals shownGen the Hiding handler honors that one generation lower too. A CAS cannot detect an in-flight Hiding the way it detects an in-flight Showing, because Hiding only Loads the counter and never advances it
	expectOwnPaneShow atomic.Int64   // Windows: DEADLINE on the process-monotonic tkNowNs() clock (NOT unix time — a tablet resyncs its clock on resume from sleep), set before a TryShow that may need compensating; while unexpired, the next Showing on this window is that pane appearing, so the Showing callback claims the mark and does NOT count it as an independent user open (tkPaneShowSeq). A deadline, not a flag: an accepted show whose pane never appears must stop being credited, or a keyboard the user opens by hand claims the mark and the pending compensation closes it (see tkClaimOwnPaneShow)
	hideGen           atomic.Int64   // hide-request generation; EVERY editor tap bumps it (even a debounced one) so a pending blur-hide can't close a keyboard the user just came back to
	monitorPing       atomic.Int64   //nolint:unused // referenced only from touch_keyboard_windows.go; Windows: occlusion-monitor restart requests (epoch)
	occlusionEpoch    atomic.Int64   // Windows: bumped by every pane show/hide boundary; samples straddling a boundary are dropped
	legacyShow        atomic.Bool    // Windows: our show went through the legacy Toggle path — hide must Toggle too (a HIDE-METHOD flag; follows ownership on transfer)
	expectHiding      atomic.Bool    // Windows: this session is a WinRT pane that WILL raise a Hiding event, so its monitor waits for Hiding instead of self-expiring on zero occlusion — a SESSION-TYPE flag, independent of ownership and of legacyShow (a user-opened legacy keyboard has expectHiding=false even if we advised it)
	eventsBound       atomic.Bool    // Windows: pane Showing/Hiding events registered for this window
	paneVisible       atomic.Bool    // Windows: a pane session is live for this window — set by Showing/AlreadyVisible EVIDENCE (incl. user-opened keyboards), cleared by Hiding/session end; NOT derivable from ownership or padding (a user-opened floating keyboard has neither); corrected against the physical pane by tkCmdPaneTruth when the two disagree
	released          atomic.Bool    // window destroyed; no further platform work for this state

	// What a non-modal surface restores when it closes: a keyboard was asked
	// for and focus has not since been fully lost. Both are WINDOW-scoped and
	// neither names an editor — every editor routed through
	// editorTouchKeyboardArea can set them, the composer as much as the
	// identity search, the alias editor or the picker's own search field.
	//
	// That scope is the right one for the only question asked of them, which
	// is "was there a keyboard to come back to", not "whose". The restore is
	// not aimed at whoever set the flag: closeEmojiPicker focuses the composer
	// and only then re-raises a keyboard, so the answer belongs to the window.
	// A surface that needs to know WHICH editor had the keyboard has to keep
	// that itself; it cannot be read out of these.
	softKeyboardExpected atomic.Bool // an editor pointer requested Gio's soft keyboard — the cross-platform half, set for mouse presses too
	// Set on EVERY platform, by touch alone: it records that the keyboard was
	// raised by a finger rather than a mouse, which is what makes it safe to
	// raise again on restore. Only its consumer is Windows-specific — the
	// TabTip dispatch, which the other platforms stub out — so a restore that
	// re-runs showTouchKeyboard there is not idle: it puts these two flags
	// back, and Gio raises the keyboard from the SoftKeyboardCmd beside it.
	platformTouchKeyboardExpected atomic.Bool

	// Editor-focus tracking for symmetric hide; touched only from this
	// window's frame loop, so plain fields suffice.
	focusSeen   bool
	hidePending bool
	blurAt      time.Time

	// Outside-tap detection (frame-loop only): touch presses seen by the
	// window-level tracker vs. presses claimed by editor areas during the
	// same frame. Evaluated at the start of the next frame.
	framePresses    []pointer.ID
	editorPresses   []pointer.ID
	suppressDismiss bool // an explicit editor FocusCmd was issued; skip the next evaluation

	// Touch presses that landed inside an editor area and have NOT yet
	// resolved into a release. Unlike editorPresses above, this OUTLIVES the
	// frame: the finger drift that cancels a press arrives frames after it.
	// See the Cancel branch of editorTouchKeyboardArea.
	editorHeld []tkHeldTouch

	// Keyboard-tail measurement (frame-loop only, like the fields above):
	// how tall the rows that must not go under the keyboard came out while
	// the frame in flight is being built, and what the last COMPLETED frame
	// measured. See endTailFrame.
	tailFrame int
	tailDone  int
}

// registerKeyboardState maintains the process-wide set of LIVE window
// keyboard states used for ownership transfer. Released states are removed
// and never re-added — a late Publish or retry command for a destroyed
// console must not resurrect its entry (each entry pins the whole window
// through the invalidate closure, so a stale entry is a leak of the entire
// ConsoleWindow per open/close cycle).
func registerKeyboardState(known map[*touchKeyboardState]bool, kbd *touchKeyboardState) {
	if kbd.released.Load() {
		delete(known, kbd)
		return
	}
	known[kbd] = true
}

// transferKeyboardOwnership moves the app's keyboard-session ownership to
// kbd when another live window currently holds it, carrying the legacy
// flag along: a keyboard shown through the legacy Toggle path must be
// hidden through Toggle regardless of which window ends up hiding it.
// Returns whether ownership was taken over. Scenario: the main window
// showed the keyboard, the user switched to the console and tapped its
// editor — without the transfer neither window could ever hide the
// keyboard (the old owner's hide is correctly rejected while its window
// is not foreground, and the new window's blur found shownByUs unset).
//
// legacyShow (the HIDE-METHOD) is carried to the new owner and cleared on the
// old one: it only tells whoever hides HOW to hide. The old window's monitor
// no longer depends on it — session type is now a separate expectHiding flag
// that transfer does not touch, so the old monitor still self-expires
// correctly whether or not it was a legacy session.
func transferKeyboardOwnership(known map[*touchKeyboardState]bool, kbd *touchKeyboardState) bool {
	for other := range known {
		if other == kbd || !other.shownByUs.Load() {
			continue
		}
		legacy := other.legacyShow.Load()
		// The keyboard is still up: do NOT publish zero for the old
		// owner — its occlusion monitor keeps reporting reality.
		other.shownByUs.Store(false)
		other.legacyShow.Store(false)
		kbd.legacyShow.Store(legacy)
		kbd.shownByUs.Store(true)
		return true
	}
	return false
}

// keyboardShowRetryMax bounds the re-enqueues of a show command whose COM
// initialization keeps failing. The delay ladder is 1s,2s,…,5s (~15s total)
// — long enough to ride out a startup hiccup, short enough that a genuinely
// broken COM runtime doesn't queue stale shows forever (the next tap makes
// a fresh command anyway).
const keyboardShowRetryMax = 5

// keyboardShowRetryDelay returns how long to wait before re-enqueueing a
// show command whose COM initialization failed, and whether to retry at
// all. Dropping the command instead would reproduce the original bug on a
// transient failure: the FIRST tap after app start showing no keyboard.
// The retried command re-validates generation, HWND binding and foreground
// when it runs, so a stale retry cancels itself naturally.
func keyboardShowRetryDelay(retries int8) (time.Duration, bool) {
	if retries >= keyboardShowRetryMax {
		return 0, false
	}
	return time.Duration(retries+1) * time.Second, true
}

// keyboardMonitorInitDelay returns the sleep before the next RoInitialize
// attempt of an occlusion monitor: a growing ladder capped at 30s, retried
// WITHOUT an attempt limit. If the Showing that started the monitor already
// happened, no new event would ever restart a dead monitor — only a
// released window may stop the retries.
func keyboardMonitorInitDelay(attempt int) time.Duration {
	d := time.Duration(attempt+1) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// advisableKeyboardStates returns the live states from known that still
// need a Showing/Hiding registration. Used when the FrameworkInputPane
// becomes available AFTER earlier failures: windows whose show already
// happened would otherwise run the rest of their session without a Hiding
// callback (ensureAdvised is normally only reached inside a show command).
func advisableKeyboardStates(known map[*touchKeyboardState]bool) []*touchKeyboardState {
	var out []*touchKeyboardState
	for kbd := range known {
		if kbd == nil || kbd.released.Load() {
			continue
		}
		if kbd.hwnd.Load() == 0 || kbd.eventsBound.Load() {
			continue
		}
		out = append(out, kbd)
	}
	return out
}

// endKeyboardSession ends a window's local keyboard session: ownership and
// the legacy flag are cleared, the occlusion epoch is bumped so in-flight
// samples from before this boundary are voided, and the bottom padding is
// removed. Used by every hide-side path that concludes the keyboard is (or
// is being) dismissed — including the legacy path discovering the keyboard
// already gone, where no pane event and possibly no monitor would ever
// clean the state otherwise.
func endKeyboardSession(kbd *touchKeyboardState) {
	kbd.occlusionEpoch.Add(1)
	kbd.legacyShow.Store(false)
	kbd.expectHiding.Store(false)
	kbd.shownByUs.Store(false)
	kbd.paneVisible.Store(false)
	kbd.softKeyboardExpected.Store(false)
	kbd.platformTouchKeyboardExpected.Store(false)
	kbd.expectOwnPaneShow.Store(0) // no pending own-pane Showing after teardown
	kbd.adoptedGen.Store(0)        // the adopted session is over; its Hiding has nothing left to clear
	kbd.publishOccludedDp(0)
}

// setInvalidate registers the window's redraw trigger (app.Window.Invalidate).
func (s *touchKeyboardState) setInvalidate(fn func()) {
	s.invalidate.Store(fn)
}

// occludedHeightDp returns the height (dp) of this window's area currently
// occluded by the touch keyboard.
func (s *touchKeyboardState) occludedHeightDp() float32 {
	return float32(s.occludedDp.Load())
}

// placeMenu positions a menu of size (menuW, menuH) at its preferred anchor
// (anchorX, anchorY) inside a window of width windowW and USABLE height availH
// (window height minus the touch-keyboard occlusion). It flips left/up on
// right/bottom overflow, then clamps the WHOLE menu into [0,windowW]×[0,availH]
// — the flip alone is not enough: if the anchor sat below the usable area (the
// keyboard appeared under it), the flipped menu's bottom would still fall under
// the keyboard without the final clamp. Shared by both context menus.
func placeMenu(anchorX, anchorY, menuW, menuH, windowW, availH int) (int, int) {
	x := anchorX
	if x+menuW > windowW {
		x = windowW - menuW
	}
	if x < 0 {
		x = 0
	}
	y := anchorY
	if y+menuH > availH {
		y = anchorY - menuH // flip above the anchor
	}
	if y+menuH > availH {
		y = availH - menuH // final clamp
	}
	if y < 0 {
		y = 0
	}
	return x, y
}

// publishOccludedDp stores a new occlusion height and wakes the window's
// frame loop when the value changed.
func (s *touchKeyboardState) publishOccludedDp(h int32) {
	if s.occludedDp.Swap(h) != h {
		if fn, _ := s.invalidate.Load().(func()); fn != nil {
			fn()
		}
	}
}

// keyboardInsetDp reports how much of the container's bottom (dp) the touch
// keyboard covers: the published occlusion, bounded only by the container
// itself, and 0 when nothing is occluded. Every caller gets this same number —
// the padding under the content, and the room an overlay drawn outside that
// content has to work with.
//
// It reserves NOTHING for the content, and every reserve tried here was wrong
// in the same way: a reserve makes this function LIE about the screen, and
// each caller then paid for the lie differently.
//
// The overlay caller paid immediately. A menu reading a number that had
// already set aside 96dp for the composer mistakes that reserved strip for
// free space: in a 500dp window under a 450dp keyboard it was told 404dp,
// leaving 96dp of room, while only 50dp is physically clear. A smaller reserve
// is the same mistake with a smaller constant — under a 490dp keyboard a 48dp
// reserve still claims 48dp of room where 10dp exists, and 38dp of the menu's
// only row is drawn beneath the keyboard, where the system takes the touch and
// the row does not respond. A visible row that swallows taps is worse than a
// row that was never drawn.
//
// The padding caller paid the other way, and this is the r69 finding. The
// reserve was there so the composer would be "the last thing to sink", but
// subtracting it never lifted the composer by a single dp. Read
// keyboardYieldingChrome: while the content fits, the pad already lands the
// input row exactly on the keyboard's edge and any reserve only pushes the
// whole box DOWN; once it does not fit, the row's position stops depending on
// the pad at all. So the reserve bought nothing at the bottom of the window
// and spent rows off the message list above it.
//
// This function therefore answers a question about the SCREEN and only the
// screen, honestly, including when the honest answer leaves nothing. What to
// do when that answer is too small to use is a policy that belongs to the
// caller: menuOverlayRoom implements it by asking for the keyboard to be taken
// away, and keyboardYieldingChrome by taking the header off the screen —
// neither by inflating this number.
func keyboardInsetDp(gtx layout.Context, kbd *touchKeyboardState) unit.Dp {
	occl := kbd.occludedHeightDp()
	if occl <= 0 {
		return 0
	}
	if maxOccl := float32(gtx.Constraints.Max.Y) / gtx.Metric.PxPerDp; occl > maxOccl {
		occl = maxOccl
	}
	if occl <= 0 {
		return 0
	}
	return unit.Dp(occl)
}

// The rows that must stay above the keyboard are MEASURED, not guessed. Each
// window marks the rows it cannot afford to have covered — the
// per-conversation label and the composer card in the main window, the console
// card's title, help line and input in the console — and every frame reports
// how tall they actually came out. keyboardYieldingChrome reads that number.
//
// It replaces a constant, and the constant is worth a paragraph because three
// rounds of review died on it. 96dp was never a measurement of anything: the
// composer alone runs well past it once its send-status and reply-preview rows
// are counted, and the console puts a card title and a help line above its
// input. A number invented in this file cannot know either of those, so the
// chrome kept its place on exactly the frames it needed to give it up. Writing
// the gap into a LIMIT comment (r69) documented the hole rather than closing
// it — a LIMIT is honest where a quantity is genuinely unknowable in-process,
// and this one was one int away from being known.
//
// The number is one frame old, and that is sound rather than merely tolerable:
//
//   - It does not depend on the occlusion AT ALL. The keyboard inset lives
//     inside the content panel and never reaches the constraints these rows
//     are laid out with, so the frame the keyboard appears on already holds
//     the right number, measured while the keyboard was still down. This is
//     the only lag that would have mattered.
//   - Where it does depend on the room the panel got — layoutComposerCard caps
//     its editor at a third of the height it is given — it is MONOTONE in that
//     room. Yielding the chrome can only make the tail taller, and a taller
//     tail can only keep the chrome yielded; coming back needs a tail strictly
//     shorter than the yielded state ever reports. The two states therefore
//     cannot alternate, and the cost of the lag is one frame drawn with the
//     previous decision. Monotone is not the same as correct, though: see
//     keyboardTailRow for why the room the keyboard took has to be handed back
//     before the row is measured at all.
//   - The frame that measures a new number asks for the frame that will act
//     on it before it ends (endTailFrame), so the lag is exactly one frame
//     and never waits on an unrelated redraw.
//
// A tail of zero means NOT MEASURED — the panel has not been laid out yet, or
// this window has no rows to protect — and the chrome then stays. Yielding is
// destructive, so it happens on evidence and never on the absence of it. That
// rule is also what keeps the console's tab strip on the tabs which have no
// input at all: Peers, Traffic, Files, Info and Donate register no rows, so a
// keyboard raised by hand over one of them takes nothing away. A tab check
// here would say the same thing twice and drift the day a tab grows an
// editor; what the strip yields to is a row that reported a height.

// endTailFrame publishes the tail this frame measured and, when it changed,
// asks for the frame that will act on it. Each window calls it once, at the
// END of its layout — every row that reports into the tail has run by then.
//
// The end is where it has to be, and this is the correction to the first cut
// of the refactor, which published at the START of the next frame. Doing it
// there compares two numbers that were both already known when the previous
// frame finished, and by then nobody has asked for the frame doing the
// comparing: Gio draws in response to input, and the paste that made the
// composer three lines tall, or the send-status row that appeared under it,
// has already been drawn. The header would keep last frame's decision — and
// the composer its place under the keyboard — until something unrelated woke
// the loop: the main window's two-second heartbeat, or in the console nothing
// at all. Measuring and reacting have to be the same frame.
//
// The wake-up is op.InvalidateCmd rather than the invalidate closure on
// purpose. app.Window.Invalidate is documented for externally triggered
// updates and does nothing while a redraw is already pending — which is
// exactly the state a window is in while it lays a frame out, so the request
// would be dropped precisely when it is made from here. The command instead
// goes through the router, whose wakeup the window loop reads immediately
// after Frame, and cannot be lost. (This is also why the call must not end up
// under keyboardTailRow, where Execute is dropped by design.)
func (s *touchKeyboardState) endTailFrame(gtx layout.Context) {
	if s.tailFrame != s.tailDone {
		s.tailDone = s.tailFrame
		gtx.Execute(op.InvalidateCmd{})
	}
	s.tailFrame = 0
}

// noteTailPx adds px to the tail being measured for the frame in flight.
func (s *touchKeyboardState) noteTailPx(px int) {
	if px > 0 {
		s.tailFrame += px
	}
}

// requiredTailPx is the tail the last completed frame measured, 0 if it
// measured none.
func (s *touchKeyboardState) requiredTailPx() int {
	return s.tailDone
}

// A row must be measured UNSQUEEZED, and this is the trap the first cut of
// this refactor fell into. Every one of these rows is laid out inside the panel
// the keyboard has already taken its bite out of, and a card clamps itself to
// the room it is offered — layout.Flex ends in cs.Constrain(sz), and the
// composer additionally caps its editor at a third of the height it is given.
// So the height a squeezed row reports is a measure of the squeeze and not of
// the row, and it is smallest exactly when the keyboard is tallest.
//
// Feeding that number back to keyboardYieldingChrome makes the decision
// confirm itself. At 500dp with 330dp occluded the header stays up, which
// leaves the composer 72dp, so it reports 72; 66 of chrome plus 72 fits in the
// 162dp strip, so the header stays up again, forever — the exact r70 defect,
// preserved through the refactor meant to remove it. What the tail has to
// answer is how tall the row would be with the keyboard down, so the
// measurement hands the occlusion back before asking.
//
// keyboardOccludedPx is that number of pixels, deliberately NOT clamped to the
// container: the point is to undo the container's own shrinking.
func keyboardOccludedPx(gtx layout.Context, kbd *touchKeyboardState) int {
	occl := kbd.occludedHeightDp()
	if occl <= 0 {
		return 0
	}
	return gtx.Dp(unit.Dp(occl))
}

// keyboardTailMeasurePx reports how tall w comes out with addPx of room handed
// back to it, without drawing it or letting it touch anything.
//
// The pass is real but inert. The source is Disabled, so w is delivered no
// events and changes no state — everything under here reads input through
// gtx.Event, which a disabled source answers with nothing — and the macro is
// dropped, so nothing is drawn and no hit area is registered. Min.Y is zeroed
// because a container asked for a minimum reports at least that minimum, and
// what is wanted is the height of the content; Min.X is left alone so the row
// is measured at the width it will really have and wraps its text the same way.
//
// LIMIT: this is an honest measurement only for content with no flexible parts
// in the vertical axis. A Flexed child still expands to the space offered and
// would measure the offer instead of itself.
func keyboardTailMeasurePx(gtx layout.Context, addPx int, w layout.Widget) int {
	gtx = gtx.Disabled()
	gtx.Constraints.Min.Y = 0
	if addPx > 0 {
		gtx.Constraints.Max.Y += addPx
	}
	macro := op.Record(gtx.Ops)
	dims := w(gtx)
	macro.Stop() // dropped: this pass measures, it does not draw
	return dims.Size.Y
}

// keyboardTailRow wraps a widget whose whole height has to stay above the
// keyboard and adds what it needs to this frame's tail. The widget is laid out
// and drawn exactly as it would be without the wrapper.
//
// While the keyboard is up the row is laid out twice: measured first, drawn
// second. The order is deliberate and it is the REAL pass that has to be last,
// because these rows are stateful widgets — an editor clamps its scroll offset
// to the box it was just given — and the measuring pass hands them a box that
// is not the one on screen. Running it last would leave every widget under here
// ending the frame holding the state of a pass nobody saw.
//
// The price of that order is a rule for what may live under this wrapper: NO
// one-shot side effect performed during layout, because the measuring pass
// cannot complete one. Reading input is safe — gtx.Event on a disabled source
// returns nothing, so an event-gated action simply does not fire in that pass
// and fires in the real one — but gtx.Execute is silently DROPPED, so an action
// gated on a plain flag would clear the flag in the measuring pass and have its
// command thrown away. messageInputCard used to focus the composer exactly that
// way; the block now runs in Window.layout, where no measurement can reach it,
// and anything similar belongs there too.
func keyboardTailRow(kbd *touchKeyboardState, w layout.Widget) layout.Widget {
	return func(gtx layout.Context) layout.Dimensions {
		if addPx := keyboardOccludedPx(gtx, kbd); addPx > 0 {
			kbd.noteTailPx(keyboardTailMeasurePx(gtx, addPx, w))
			return w(gtx)
		}
		// Nothing is occluded, so nothing squeezed it: what it draws at is
		// what it needs, and one pass answers both questions.
		dims := w(gtx)
		kbd.noteTailPx(dims.Size.Y)
		return dims
	}
}

// keyboardMeasureTail adds the height of w to this frame's tail without
// drawing it. It is for rows that are not reachable as a widget where they
// have to be counted: the console card builds its title and help lines around
// whatever content it is handed, so the only way to ask how tall they are is
// to lay the same card out around nothing.
func keyboardMeasureTail(gtx layout.Context, kbd *touchKeyboardState, w layout.Widget) {
	kbd.noteTailPx(keyboardTailMeasurePx(gtx, keyboardOccludedPx(gtx, kbd), w))
}

// keyboardYieldingChrome lays chrome out and draws it only while the strip the
// keyboard leaves free can hold both it and the measured tail of content below
// it. When it cannot, the chrome takes up no space and the rows below it
// move up by its whole height. This is how the bottom input row is kept out
// from under the keyboard.
//
// Padding cannot do that job, which is why this exists. The padded content is
// a vertical Flex, and layout/flex.go lays each Rigid child out with main-axis
// constraints (0, remaining): a Rigid child MAY return more than remaining and
// Flex draws whatever it returns, past the bottom of the padded box. So once
// header, label and the input row together outgrow the strip above the
// keyboard, the input row's y is topInset + chrome + spacer + label — an
// entirely pad-free expression, and no pad value moves it. And while they do
// fit, padding by the full occlusion already puts the row's bottom exactly on
// the keyboard's top edge, so there is nothing left for a pad to win either.
// Between those two cases the only quantity that decides whether the row is
// visible is how much of the free strip the fixed chrome eats — this function
// is the one lever there is.
//
// The chrome is laid out either way, so it reads its events and updates its
// state exactly once per frame whether or not it is shown; only the recorded
// DRAW is dropped, which takes its hit areas with it.
//
// Yielding cannot oscillate. Two of the three inputs — the container height
// and the occlusion — do not depend on the decision at all. The third, the
// measured tail, can: yielding hands the content more room and the composer
// may then allow itself a taller editor. But it can only grow that way, and a
// taller tail can only keep the chrome yielded, so the yielded state never
// reports a tail that would bring the chrome back. See the tail measurement
// above for why that monotonicity holds.
//
// The strip is measured from the constraints this call is given, which is the
// space left where the chrome sits rather than the window: both callers put it
// first in their Flex, so it sees the whole padded container, whose bottom
// already sits ~4dp above the window's. The free strip is therefore
// under-stated by those few dp and the chrome yields a touch earlier than
// strictly needed, which is the safe direction of the two.
//
// LIMIT: yielding is all-or-nothing and covers only the chrome passed in. A
// caller that wants a header to shrink rather than vanish, or wants two pieces
// of chrome to yield in order, needs something else here.
func keyboardYieldingChrome(gtx layout.Context, kbd *touchKeyboardState, chrome layout.Widget) layout.Dimensions {
	macro := op.Record(gtx.Ops)
	dims := chrome(gtx)
	call := macro.Stop()
	if tail := kbd.requiredTailPx(); tail > 0 {
		if occl := gtx.Dp(keyboardInsetDp(gtx, kbd)); occl > 0 {
			if free := gtx.Constraints.Max.Y - occl; free < dims.Size.Y+tail {
				return layout.Dimensions{}
			}
		}
	}
	call.Add(gtx.Ops)
	return dims
}

// keyboardSessionIdle reports that a window's keyboard session is fully
// over: nobody owns it, no padding is published, and no pane session is
// live. A failing occlusion monitor uses this to decide that retrying
// forever serves no one — with an ACTIVE session it must outlive any error
// burst (nothing else would clean the state), but once a Hiding event or
// endKeyboardSession already cleaned everything, holding the monitor slot
// and polling a broken API is pure waste, and a future show can always
// start a fresh monitor.
//
// paneVisible is what keeps this sound for USER-opened keyboards: their
// Showing event starts a monitor with shownByUs=false and (until the first
// successful sample, or forever for a floating keyboard) zero padding —
// ownership and padding alone would misread that live session as idle and
// let one early COM failure kill the only monitor it will ever get.
func keyboardSessionIdle(kbd *touchKeyboardState) bool {
	return !kbd.shownByUs.Load() && kbd.occludedDp.Load() == 0 && !kbd.paneVisible.Load()
}

// handleTouchLongPress updates long-press tracking state from a pointer
// event. now is the frame time (gtx.Now); slopPx is longPressSlop in
// pixels; cursor is this pointer's press position from the window-level
// tracker (captured per PointerID so a same-frame drag or a second finger
// can't relocate the menu). Tracking is bound to the PointerID of the
// initiating touch.
//
// KNOWN LIMITATION (deliberate): pointer.Cancel cancels the hold, and Gio
// broadcasts Cancel as soon as any gesture grabs the pointer — for list
// scrolling that is roughly 3dp of travel, well under longPressSlop. So
// the EFFECTIVE jitter tolerance of long-press equals Gio's grab
// threshold, not our 12dp. Three attempts to keep holds alive across the
// grab (fire-time scroll guards, VK_LBUTTON, GetPointerInfo) all turned
// out unsound — after the grab this state receives no events at all, and
// no thread-safe OS oracle for "is this contact still down" exists. If
// on-device testing shows the threshold is too strict, the correct fix is
// upstream (configurable gesture slop in Gio, or a list variant that
// defers its grab), not another heuristic here.
func (rc *rightClickState) handleTouchLongPress(pe pointer.Event, now time.Time, slopPx float32, cursor image.Point) {
	switch pe.Kind {
	case pointer.Press:
		if !rc.touchDown && pe.Source == pointer.Touch && pe.Buttons == pointer.ButtonPrimary {
			rc.touchDown = true
			rc.longPressFired = false
			rc.matured = false
			rc.touchID = pe.PointerID
			rc.touchStart = now
			rc.pressTime = pe.Time
			rc.touchPos = pe.Position
			rc.pressCursor = cursor
		} else {
			// A press while already tracking (second finger), or a
			// non-touch/non-primary press: not a long-press gesture.
			rc.touchDown = false
		}
	case pointer.Move, pointer.Drag:
		if rc.touchDown && pe.PointerID == rc.touchID {
			d := pe.Position.Sub(rc.touchPos)
			if d.X*d.X+d.Y*d.Y > slopPx*slopPx {
				// Finger moved too far — this is a scroll, not a long press.
				rc.touchDown = false
			}
		}
	case pointer.Release:
		if pe.PointerID == rc.touchID {
			// If the hold ALREADY matured by the time of this Release — judged
			// by the events' OWN timestamps, not the frame clock — let it
			// fire. A delayed UI frame can batch a press and a >500ms-later
			// release together; frame-time alone would then read the hold as
			// instantaneous and wrongly cancel it.
			if rc.touchDown && !rc.longPressFired && pe.Time-rc.pressTime >= longPressDuration {
				rc.matured = true
			}
			rc.touchDown = false
		}
	case pointer.Cancel:
		// Grab-induced or system cancel: the event stream for this hold is
		// over (the Release will never arrive), so the hold must die here —
		// see the limitation note above.
		rc.touchDown = false
	}
}

// longPressTriggered reports whether the tracked touch has been held long
// enough to open a context menu. While the hold is still maturing it
// schedules a redraw so the trigger fires without further input events.
func (rc *rightClickState) longPressTriggered(gtx layout.Context) bool {
	if rc.longPressFired {
		return false
	}
	// Matured at Release by event time (frame may have been delayed): fire
	// even though touchDown was already cleared by that Release.
	if rc.matured {
		rc.longPressFired = true
		rc.matured = false
		return true
	}
	if !rc.touchDown {
		return false
	}
	if gtx.Now.Sub(rc.touchStart) >= longPressDuration {
		rc.longPressFired = true
		rc.touchDown = false
		return true
	}
	gtx.Execute(op.InvalidateCmd{At: rc.touchStart.Add(longPressDuration)})
	return false
}

// touchKbdTraceOn enables the touch-keyboard show-path trace.
//
// This subsystem can only be exercised on real hardware, and there every one
// of its diagnostics is log.Debug() on a FAILURE path while crashlog defaults
// the global level to warn — so a device run that shows no keyboard produces
// no evidence whatsoever about WHERE it stopped. Setting CORSA_TOUCHKBD_TRACE=1
// promotes the decision points of the whole chain (tap seen -> request gates ->
// dispatch -> platform status) to Warn, which passes the default filter. One
// switch, no interaction with CORSA_LOG_LEVEL, and nothing to remember about
// log levels while standing at the tablet.
var touchKbdTraceOn = os.Getenv("CORSA_TOUCHKBD_TRACE") == "1"

// tkTraceEvent returns a log event for the show path, or nil when tracing is
// off. zerolog treats a nil *Event as a no-op on every method, so call sites
// chain fields unconditionally and pay a single bool read when disabled.
func tkTraceEvent(stage string) *zerolog.Event {
	if !touchKbdTraceOn {
		return nil
	}
	return log.Warn().Str("tk", stage)
}

// tkDiagEvent is tkTraceEvent for the diagnostics that were ALREADY being
// logged, and it exists because those were the ones the trace could not show.
// crashlog.Setup leaves zerolog at WarnLevel, so a Debug line explaining why a
// show failed — the stage, the HRESULT — is invisible on the machine that has
// the problem, which is exactly the machine CORSA_TOUCHKBD_TRACE=1 is turned on
// for. With tracing on the line is promoted to Warn; with it off it stays the
// Debug line it always was, for anyone who has lowered the level by hand.
func tkDiagEvent(stage string) *zerolog.Event {
	if e := tkTraceEvent(stage); e != nil {
		return e
	}
	return log.Debug().Str("tk", stage)
}

// tkVisibilityVerdict is why tkAwaitVisible stopped waiting. The four answers
// are deliberately distinct: three of them mean "do not escalate", and they
// mean it for reasons that call for opposite follow-ups.
type tkVisibilityVerdict int

const (
	// tkVisibleSeen: a probe answered "a keyboard is on screen".
	tkVisibleSeen tkVisibilityVerdict = iota
	// tkVisibleCancelled: the show was superseded or the window changed while
	// we waited. A keyboard may still be coming up, so this compensates.
	tkVisibleCancelled
	// tkVisibleUnknown: the wait ran out with at least one poll that could
	// not answer. Neither evidence of absence nor evidence of a keyboard.
	tkVisibleUnknown
	// tkVisibleAbsent: every poll answered, and every answer was "nothing
	// there". The only verdict that justifies escalating.
	tkVisibleAbsent
)

// tkAwaitVisible polls for a keyboard that some invocation was supposed to
// raise, and exists because "the call was accepted" and "a keyboard appeared"
// are different claims — InputPane.TryShow is documented best-effort, and
// ITipInvocation::Toggle reports only that the host took the call.
//
// The order of the checks inside the loop is the contract:
//
//   - cancellation is read BEFORE the probe, because a show that no longer
//     belongs to this window must not be committed on evidence of a keyboard
//     that now belongs to someone else;
//   - a positive probe wins immediately, so a keyboard that appears on the
//     first poll costs one delay rather than all of them;
//   - an INCONCLUSIVE probe does NOT end the wait. It is remembered and the
//     polling continues, because a COM probe that fails once usually answers
//     the next time, and the alternatives are both wrong: stopping and
//     reporting absence would fire a global non-idempotent Toggle at a
//     keyboard that may be up, while stopping and reporting success — what
//     this function did before — declares a keyboard nobody has seen and ends
//     the retries, which is precisely how a tap produces no keyboard at all.
//     Only a wait that ran to its end with an unanswered poll in it is
//     unknown; a wait every poll answered "nothing there" is absent.
//
// sleep is injected so the decision table can be tested without spending the
// wall-clock time it describes.
func tkAwaitVisible(polls int, delay time.Duration, sleep func(time.Duration), stillValid func() bool, probe func() (up, conclusive bool)) tkVisibilityVerdict {
	unanswered := false
	for i := 0; i < polls; i++ {
		sleep(delay)
		if !stillValid() {
			return tkVisibleCancelled
		}
		up, conclusive := probe()
		if up {
			return tkVisibleSeen
		}
		if !conclusive {
			unanswered = true
		}
	}
	if unanswered {
		return tkVisibleUnknown
	}
	return tkVisibleAbsent
}

// tkPumpedWait spends d without leaving the apartment unserviced.
//
// A single-threaded apartment that blocks is an apartment that answers no COM
// calls: incoming calls are delivered to an STA through a hidden window in
// this thread's message queue, and a thread parked in time.Sleep dispatches
// nothing. The pane events this process registered for ARE such calls, and
// IFrameworkInputPaneHandler::Showing is raised BEFORE the panel appears — so
// a sleep taken while looking for the keyboard is the very thing stopping the
// news of it from arriving, and it holds up the shell thread raising the event
// for as long as it lasts.
//
// So the time is spent in a loop: drain the queue, then block on a wait that
// wakes on new input as well as on the deadline. The drain comes BEFORE each
// wait and not only after it, because a wait entered with messages already
// queued would otherwise sit on them.
//
// now, pump, wait and sleep are injected because the loop is the part worth
// testing and none of the Win32 primitives it drives exist off Windows.
//
// wait reports whether it worked. A wait that did not is not retried: a broken
// wait returns immediately, so retrying it would spin a core for the length of
// d instead of waiting out. The remainder is slept through instead — no worse
// than the sleep this replaces, and reached only when the OS refuses the wait.
func tkPumpedWait(d time.Duration, now func() time.Time, pump func(), wait func(timeout time.Duration) bool, sleep func(time.Duration)) {
	deadline := now().Add(d)
	for {
		pump()
		// Recomputed against the deadline rather than counted down, so a wait
		// that returns early — which is what every incoming call does — cannot
		// stretch the total, and one that overshoots cannot be waited on twice.
		left := deadline.Sub(now())
		if left <= 0 {
			return
		}
		if !wait(left) {
			sleep(left)
			pump()
			return
		}
	}
}

// tkMayRetoggle is the ration on the second Toggle — the one the recovery pass
// fires at a host it has just (re)started. It is a function so the rule can be
// read and tested on its own, because getting it wrong is invisible in both
// directions: too strict and the pass merely WATCHES a fresh host that was
// never asked for anything, which is how a tap ends in "no keyboard on screen"
// with every call reporting success; too loose and a Toggle lands on top of an
// outstanding one and closes the keyboard it was raising.
//
//   - retoggled: the pass gets exactly one. Toggle is global and not
//     idempotent; a second would undo the first.
//   - poll < settle: a host that is still starting has no COM server to accept
//     anything, so an early Toggle spends the one attempt on nobody.
//   - conclusiveAbsent: a probe that ANSWERED "nothing on screen". Required
//     while an earlier Toggle is still outstanding, because that Toggle may be
//     raising a keyboard the probes simply cannot see yet.
//   - !hostToggled: nothing is outstanding, so there is no keyboard a Toggle
//     could cancel, and the caller's precondition — every legacyShow caller
//     established an empty screen — is the evidence.
func tkMayRetoggle(poll, settle int, retoggled, hostToggled, conclusiveAbsent bool) bool {
	if retoggled || poll < settle {
		return false
	}
	return conclusiveAbsent || !hostToggled
}

// tkLegacyPlan is what the legacy show does with a visibility verdict.
// escalateNote is set — and only set — when neither of the other two applies,
// so the three outcomes are mutually exclusive by construction.
type tkLegacyPlan struct {
	commit       bool
	compensate   bool
	escalateNote string
}

// tkLegacyVerdictPlan maps a verdict to that decision. It is a function purely
// so the mapping can be pinned by a test, and it earns that: the same mistake
// has now been made twice in this file — treating a verdict that is NOT
// evidence of a keyboard as one — and each time the cost was the same. A
// commit sets paneVisible, so the layout reserves room for a keyboard nobody
// has seen; it opens a session, so the debounce eats the user's next tap; and
// it returns, so nothing tries again. "The keyboard never appears", from a
// path that reports success at every step.
//
// Only tkVisibleSeen is a keyboard. tkVisibleCancelled is not a failure at all
// — a keyboard may well be coming up for a show that no longer belongs to this
// window, so it compensates rather than escalating. The other two escalate, and
// they are kept apart because they mean different things to whoever reads the
// log: one says the probes agreed there was nothing there, the other says the
// probes could not tell.
func tkLegacyVerdictPlan(v tkVisibilityVerdict) tkLegacyPlan {
	switch v {
	case tkVisibleSeen:
		return tkLegacyPlan{commit: true}
	case tkVisibleCancelled:
		return tkLegacyPlan{compensate: true}
	case tkVisibleUnknown:
		return tkLegacyPlan{escalateNote: "touch keyboard: legacy show could not be verified; escalating without committing"}
	default:
		return tkLegacyPlan{escalateNote: "touch keyboard: legacy Toggle accepted but no keyboard appeared; starting the host"}
	}
}

// HRESULTs that are a final answer from the WinRT input-pane path rather than a
// hiccup in it. Anything not listed here — registry read errors, a broken
// implementation's E_NOTIMPL, a shell that is still starting — is transient and
// stays on the retry ladder.
const (
	// REGDB_E_CLASSNOTREG: the class is not registered on this system.
	tkRegdbEClassNotReg = 0x80040154
	// E_NOINTERFACE: the InputPane class exists (Win10 1507+) but
	// IInputPaneInterop, the HWND desktop interop, only arrived in 1607.
	tkENoInterface = 0x80004002
	// RO_E_UNSUPPORTED_FROM_MTA: InputPane is a single-threaded WinRT class and
	// the calling thread is in the multi-threaded apartment. A running thread's
	// apartment cannot be changed, so on that thread this is as permanent as a
	// missing class, however healthy the machine is.
	tkROEUnsupportedFromMTA = 0x8000001D
)

// tkPaneHRPermanent reports whether such an HRESULT means the input pane can
// never be reached from here, so the show must go to the legacy host instead of
// onto the retry ladder.
//
// The distinction decides whether a tap produces a keyboard. A "transient"
// verdict requeues the show, and that ladder deliberately withholds the legacy
// host — a blind Toggle could close a keyboard the user is looking at. So a
// permanent failure misfiled as transient spends the full ~15s repeating a call
// whose answer is already known, with the one path that could still have
// produced a keyboard locked out for the whole time, and then abandons the tap.
// A device trace showed exactly that: RO_E_UNSUPPORTED_FROM_MTA, once a second,
// and no keyboard.
func tkPaneHRPermanent(hr uintptr) bool {
	switch hr {
	case tkRegdbEClassNotReg, tkENoInterface, tkROEUnsupportedFromMTA:
		return true
	}
	return false
}

// tkPaneHRApartment reports whether hr is the APARTMENT refusal specifically:
// the InputPane activation factory is registered on this machine and COM
// simply refuses to hand it to a thread in the multithreaded apartment.
//
// tkPaneHRPermanent covers the same HRESULT, and rightly so: for SHOW ROUTING
// the three are one answer, "no retry will help, go to the legacy host". But
// they are opposite answers to the other question the same status gets asked,
// "is a keyboard on screen right now". REGDB_E_CLASSNOTREG and E_NOINTERFACE
// mean there is no WinRT pane on this build at all, so the legacy IPTip host
// IS the keyboard here and not finding one is real evidence that none is up.
// RO_E_UNSUPPORTED_FROM_MTA means the pane exists and THIS THREAD cannot look
// at it — evidence about nothing.
//
// Reading the apartment refusal as "hidden" is not theoretical. On Windows 11
// the keyboard on screen is frequently not an IPTip_Main_Window, so the legacy
// probe answers "nothing there" for a keyboard the user is typing on. Fed to
// legacyShow that makes tkMayRetoggle conclude the first Toggle failed and
// Toggle a second time, closing the keyboard it just raised; fed to the
// occlusion monitor it becomes a zero-height sample every 250ms, and twelve of
// them tear down a live session.
func tkPaneHRApartment(hr uintptr) bool {
	return hr == tkROEUnsupportedFromMTA
}

// tkLegacyPaneSample folds one occlusion-monitor poll of a session whose WinRT
// InputPane factory came back tkPaneUnavailable. legacyUp is what the IPTip
// host-window probe saw. It reports whether the poll counts as a SAMPLE at all
// and, if so, whether a keyboard is up.
//
// A sighting is a sighting under either HRESULT. Only the negative is split:
// with the class genuinely absent it is the teardown signal the legacy path
// depends on, while under the apartment refusal it is not an observation, and
// counting it as one is the round-94 defect. The cost of the split is that a
// keyboard closed by hand in that configuration leaves its clearance behind
// until something else ends the session — a stale gap at the bottom of the
// window, against padding pulled out from under a keyboard the user is still
// typing on. The monitor's !sampled path already backs off to 2s polls and
// still exits once the session is idle, so the unknown is cheap to hold.
func tkLegacyPaneSample(hr uintptr, legacyUp bool) (sampled, up bool) {
	if legacyUp {
		return true, true
	}
	return !tkPaneHRApartment(hr), false
}

// editorTouchKeyboardArea wraps an editor in the hit area that raises the
// touch keyboard, and is the reason a finger tap on a text field opens one.
//
// It listens for Cancel as well as Release, and treats a cancelled press as a
// tap, because on a touch screen the release frequently never arrives. Gio's
// router hands a pointer to EVERY handler under it, and the moment any one of
// them calls pointer.GrabCmd the rest are sent a bare pointer.Cancel and
// dropped from the pointer's handler list (io/input/pointer.go, grab()). Two
// handlers below every one of our editors do exactly that: widget.Editor's own
// gesture.Drag (text selection) and any enclosing layout.List's gesture.Scroll
// both grab as soon as the finger travels past gesture.touchSlop, which is
// 3dp — a distance an ordinary finger tap covers just by rolling. So the
// sequence a tablet actually produces is Press, drift, Cancel, and nothing
// else. With Cancel absent from the filter the router does not even deliver it
// (pointerFilter.Matches is a bitmask test on Kind), which is why the field
// looked completely dead rather than flaky.
//
// Counting a cancelled press as a tap does mean a scroll that BEGAN with the
// finger inside a text field raises the keyboard. That is the deliberate side
// of the trade: the grabber's identity is not observable from here, the two
// cases are indistinguishable by travel or by timing (both grab at the same
// slop, in the same millisecond), and one costs a keyboard the user can
// dismiss while the other costs every keyboard in the app.
func editorTouchKeyboardArea(gtx layout.Context, tag event.Tag, kbd *touchKeyboardState, content layout.Widget) layout.Dimensions {
	for {
		ev, ok := gtx.Event(pointer.Filter{Target: tag, Kinds: pointer.Press | pointer.Release | pointer.Cancel})
		if !ok {
			break
		}
		pe, ok := ev.(pointer.Event)
		if !ok {
			continue
		}
		if pe.Kind == pointer.Cancel {
			// Handled BEFORE the Source test, and without consulting
			// PointerID, because a cancel is mostly not a real pointer event.
			// Two of its three sources synthesize a bare pointer.Event{Kind:
			// Cancel} — the router's grab() and the WM_CANCELMODE branch of
			// Gio's Windows backend — carrying a zero Source, which reads as
			// pointer.Mouse and would be rejected below, and a zero PointerID,
			// which is a legitimate touch id. The press WE recorded is the only
			// trustworthy evidence that a finger was down on this editor, so it
			// is what decides, and any cancel resolves all of it — provided the
			// press is recent enough to belong to the same touch, since the
			// FIRST cancel a re-registered handler receives is this bare kind
			// too (see tkHeldTouch).
			if !kbd.takeEditorTouches(tag, gtx.Now) {
				continue
			}
			tkTraceEvent("editor-area").Msg("touch keyboard: touch on editor cancelled by a grab, treating it as a tap")
			showTouchKeyboard(kbd)
			continue
		}
		if pe.Source != pointer.Touch {
			if pe.Source == pointer.Mouse && pe.Kind == pointer.Press {
				// widget.Editor emits SoftKeyboardCmd{Show:true} for this same
				// mouse press. Remember the generic intent so a non-modal
				// surface can restore it, but do not request Windows TabTip:
				// an ordinary desktop mouse must never open the touch keyboard.
				kbd.softKeyboardExpected.Store(true)
			}
			// Traced, not silent: "the editor was tapped and nothing
			// happened" and "the tap arrived as a mouse/stylus event, which
			// this area deliberately ignores" are indistinguishable on a
			// device without this line, and they have opposite fixes.
			tkTraceEvent("editor-area").Str("kind", pe.Kind.String()).Str("source", pe.Source.String()).Msg("touch keyboard: pointer event ignored, not a touch")
			continue
		}
		switch pe.Kind {
		case pointer.Press:
			// Claim the press so the window-level outside-tap detector
			// doesn't treat it as a tap outside the editors.
			tkTraceEvent("editor-area").Msg("touch keyboard: touch press on editor")
			kbd.noteEditorTouchPress(pe.PointerID)
			kbd.holdEditorTouch(tag, pe.PointerID, gtx.Now)
		case pointer.Release:
			kbd.dropEditorTouch(tag, pe.PointerID)
			tkTraceEvent("editor-area").Msg("touch keyboard: touch release on editor, requesting show")
			showTouchKeyboard(kbd)
		}
	}
	macro := op.Record(gtx.Ops)
	dims := content(gtx)
	call := macro.Stop()
	defer clip.Rect(image.Rectangle{Max: dims.Size}).Push(gtx.Ops).Pop()
	event.Op(gtx.Ops, tag)
	call.Add(gtx.Ops)
	return dims
}

// keyboardDebounceBlocks reports whether a show request stamped `now`
// (wall-clock unix nanos) must be dropped because another show was
// dispatched within keyboardShowDebounce before it. Crucially, a `now` at
// or BEFORE `last` is NEVER blocked: time.Now().UnixNano() is wall-clock
// (the monotonic reading is stripped by UnixNano), so a backward system
// clock step would otherwise make now-last negative — smaller than the
// window — and reject every tap until real time crawled back past the old
// stamp. Treating now <= last as a fresh request (and the caller then
// stores `now`, moving the stamp back) keeps the keyboard responsive
// across clock changes; a forward jump merely ends the debounce early,
// which is harmless.
//
// This stamp stays on the wall clock ON PURPOSE, unlike the Windows
// ownership deadlines (expectOwnPaneShow, tkHideDeadlineNs), which moved to
// a process-monotonic counter: a debounce compares two stamps of the same
// kind and the guard above already absorbs a step in either direction,
// whereas a deadline compares a stored stamp against a LATER read of the
// clock and has no such symmetry to lean on.
func keyboardDebounceBlocks(now, last int64) bool {
	return now > last && now-last < int64(keyboardShowDebounce)
}

// keyboardCoalesceBlocks is the same comparison over the longer window, and
// inherits the same backward-clock guard for the same reason: now <= last is
// never treated as "inside the window", so a clock step cannot make a tap look
// like it arrived during a show that has long since finished.
func keyboardCoalesceBlocks(now, last int64) bool {
	return now > last && now-last < int64(keyboardShowCoalesce)
}

// keyboardShowActive reports whether the newest dispatched show is still in
// flight, from the two generations alone: activeGen is the showGen stored when
// a show was dispatched, curGen is showGen now.
//
// Two ways to be inactive, and they are different events. curGen has moved
// past activeGen — a NEWER show superseded this one, and the newer one stored
// its own mark, so this only reads false in the gap between showGen.Add and
// the store beside it. Or activeGen is 0 — the platform layer, or a cancel,
// declared the show over. Zero is unambiguous because a dispatched generation
// is never 0: showGen.Add(1) returns 1 for the first show of a window.
//
// Everywhere but Windows nothing ever clears the mark, so this stays true for
// the life of a show and the coalesce keeps its previous purely time-based
// behaviour. That is deliberate: the platform layer is the only thing that
// knows a show has failed, and a platform that never says so must not have
// its shows treated as instantly dead.
func keyboardShowActive(activeGen, curGen int64) bool {
	return activeGen != 0 && activeGen == curGen
}

// tkTapVerdict is what a tap in an editor field is allowed to do. The two
// refusals are kept apart because they are refusals for opposite reasons, and
// a trace that calls both "debounced" hides the one that is worth knowing
// about: a debounce drops a duplicate of a show that WORKED, a coalesce
// declines to disturb a show that is still trying.
type tkTapVerdict int

const (
	tkTapDispatch  tkTapVerdict = iota // send a show to the platform
	tkTapDebounced                     // duplicate dispatch from one physical tap
	tkTapCoalesced                     // an earlier show is still working on it
)

// keyboardTapVerdict decides a tap from five values read together at the tap:
// its wall-clock stamp, the stamp of the last dispatched show, the visibleGen
// snapshot taken when that show was dispatched, visibleGen now, and whether a
// hide is animating.
//
// The order matters. closing wins outright, because mid-hide the premise
// behind BOTH refusals is inverted — the keyboard a dropped tap would have
// relied on is on its way out, and nothing else will bring it back.
//
// The coalesce is released the moment visibleGen moves, and that is what keeps
// it from being a lockout. visibleGen moves only where a keyboard has actually
// been SEEN, so a changed value means the show is genuinely finished and any
// tap after it is a real request — the "user closed the keyboard by hand and
// tapped the field again" flow lands here and dispatches, exactly as it did
// before this window existed. Only a show that has produced nothing at all
// holds the field, and only for as long as it could still plausibly be working.
//
// The release signal used to be sessionGen, and that was wrong in the one case
// this whole window exists for. sessionGen is bumped the instant TryShow
// returns true — which the code twenty lines below that bump openly documents
// as NOT meaning a keyboard appeared, since it schedules a verification for
// 700ms later on exactly that premise. So a show that was accepted and put
// nothing on screen released the coalesce at ~150ms; the next tap dispatched,
// bumped showGen, and the pending verification failed its valid() check and
// never ran — taking the legacy fallback with it. The starvation was intact in
// the documented "accepted but invisible" case, i.e. always, on the device
// where this was reported. A generation counter is only as good as the event
// it counts, and "the request was accepted" is not the event.
//
// visibleGen is bumped by the Windows layer alone. Everywhere else it stays 0
// and the coalesce is therefore purely time-based, which is harmless: on those
// platforms platformActiveWindowHandle returns 0 and requestTouchKeyboard has
// already returned before it gets here.
//
// showActive is the second half of the same correction, and it closes the gap
// visibleGen alone leaves. visibleGen answers "did a keyboard appear"; it
// cannot answer "did this show END", and the two are not the same question in
// the case that matters. A show that fails outright — TabTip.exe will not
// start, the COM ladder ran out, a verification probe could not be read —
// leaves visibleGen exactly where it was and lastShow exactly where it was, so
// for the rest of the five-second window every tap was told an earlier show
// was still working on it. Nothing was. The user tapped a dead field and
// waited out a timer for a show that had already given up.
//
// So the platform layer now says when it gives up, and this refuses to coalesce
// into a show that is not running. Note the direction of the remaining risk: a
// terminal exit that forgets to clear the mark costs at most the five seconds
// that were already being lost, while clearing it somewhere the show is still
// working would let the next tap cancel a live fallback — the starvation this
// window was built to stop. That asymmetry is why the mark is cleared by
// explicit calls at the exits rather than by a defer that releases unless
// something remembered to claim the show.
func keyboardTapVerdict(now, last, visibleAtDispatch, visibleNow int64, showActive, closing bool) tkTapVerdict {
	if closing {
		return tkTapDispatch
	}
	if keyboardDebounceBlocks(now, last) {
		return tkTapDebounced
	}
	if showActive && visibleNow == visibleAtDispatch && keyboardCoalesceBlocks(now, last) {
		return tkTapCoalesced
	}
	return tkTapDispatch
}

// beginShow marks the show generation gen as the one in flight.
//
// It refuses to move the mark BACKWARDS, and that is not defensive padding: the
// caller reaches this a few instructions after showGen.Add(1), and two taps
// arriving together — which the stamp CAS a few lines above already treats as a
// real possibility — can interleave so that the older generation stores last.
// A plain Store would then leave the mark one behind showGen, keyboardShowActive
// would read false for a show that had only just been dispatched, and the very
// next tap would cancel it. That is the starvation again, reached by a route
// with no failure of the platform in it at all.
//
// Raising only is safe in the other direction too: a generation that is already
// stale by the time it gets here finds a higher value and leaves it alone,
// which is the same rule finishShow follows for the same reason.
func (s *touchKeyboardState) beginShow(gen int64) {
	for {
		cur := s.showActiveGen.Load()
		if cur >= gen || s.showActiveGen.CompareAndSwap(cur, gen) {
			return
		}
	}
}

// finishShow records that the show generation gen has ENDED without putting a
// keyboard on screen: it is not retrying, not polling, nothing is scheduled for
// it, and a tap arriving next must be dispatched rather than folded into it.
//
// The CAS is the generation guard, and it is the reason this design is safe
// where a plain "in flight" flag would not be. Every stage of a show can outlive
// the tap that started it — a retry ladder runs for fifteen seconds, a legacy
// verification for three and a half — so a stale stage regularly finishes after
// the user has tapped again and a newer show is running. Storing 0 there would
// free the coalesce that the NEWER show is entitled to, and the newer show is
// the one still doing work. Comparing first means an ended generation can only
// ever clear its own mark; anything later is left alone.
//
// It is deliberately safe to call on an exit that turns out not to be terminal
// after all, and deliberately safe to call twice: the second call finds 0 or a
// newer generation and does nothing.
func (s *touchKeyboardState) finishShow(gen int64) {
	s.showActiveGen.CompareAndSwap(gen, 0)
}

// activeWindowHandleHook lets tests inject a deterministic native handle;
// nil in production, where the real platformActiveWindowHandle is used.
var activeWindowHandleHook func() uintptr

func activeWindowHandle() uintptr {
	if activeWindowHandleHook != nil {
		return activeWindowHandleHook()
	}
	return platformActiveWindowHandle()
}

// keyboardClosingHook lets tests inject an in-flight hide. It exists because
// the debounce bypass below is Windows-only behaviour reached through a
// platform predicate that is a compile-time `return false` everywhere else —
// so without a seam the one rule that keeps a re-tap during a close from being
// swallowed could not be exercised by the suite at all. nil in production.
var keyboardClosingHook func() bool

func keyboardClosing() bool {
	if keyboardClosingHook != nil {
		return keyboardClosingHook()
	}
	return platformKeyboardClosing()
}

// tkHideStillClosing reports whether a hide that has STARTED is still animating
// shut, from the two independent records of its start.
//
// handled is the deadline the SERVICE thread arms once it has PROCESSED a hide
// — our own accepted TryHide, or the marshaled command a pane Hiding left
// behind. raised is the deadline the pane's Hiding CALLBACK arms the instant
// the event is delivered, before any command exists.
//
// Neither record is redundant. handled is the durable one, and the only one at
// all for a hide WE begin: our TryHide raises no Hiding until the pane obeys.
// raised is the timely one. A callback is delivered whenever the apartment is
// pumped — including from inside a command handler, which pumps precisely so
// that callbacks are not held up — but all the callback may do there is ENQUEUE
// its command, and that command waits behind the handler that is running. For
// the length of that gap handled still says nothing has happened. A show
// running in that gap would ask whether the pane is closing, be told no, adopt
// a pane already on its way out, and finish, leaving a focused field whose
// keyboard then disappears with nothing left to re-open it.
//
// Either record alone being live is enough: both describe one physical event,
// and each covers a case where the other is silent.
func tkHideStillClosing(now, handled, raised int64) bool {
	return (handled != 0 && now < handled) || (raised != 0 && now < raised)
}

// showTouchKeyboard records that a touch asked for the keyboard — both the
// generic soft keyboard and the platform touch keyboard are expected from here
// on — and then dispatches the platform show path. The two records are
// unconditional because they describe the REQUEST, which is the same
// everywhere; it is the dispatch that is platform-specific, and on
// non-Windows it stubs itself out and leaves the generic request to Gio.
func showTouchKeyboard(kbd *touchKeyboardState) {
	kbd.softKeyboardExpected.Store(true)
	kbd.platformTouchKeyboardExpected.Store(true)
	requestTouchKeyboard(kbd)
}

// requestTouchKeyboard asks the platform to show the on-screen keyboard
// for kbd's window. Debounced per window (a tap in the OTHER window must
// not be swallowed — its monitor and occlusion state still need to start)
// so repeated taps in one editor don't toggle the keyboard back and forth.
func requestTouchKeyboard(kbd *touchKeyboardState) {
	// Resolve and validate the native handle BEFORE touching ANY generation.
	// A zero handle (our window not foreground) or a foreground that belongs
	// to a DIFFERENT window of ours means this event is not really a tap in
	// THIS window — bumping hideGen here would wrongly cancel this window's
	// pending blur-hide (leaving an app-opened keyboard up), and touching
	// debounce/showGen would swallow a real re-tap or orphan an in-flight
	// retry. So bail first, mutate nothing.
	hwnd := activeWindowHandle()
	if hwnd == 0 {
		tkTraceEvent("request").Msg("touch keyboard: dropped, no foreground window of ours")
		return
	}
	if vh := kbd.viewHwnd.Load(); vh != 0 {
		if hwnd != vh {
			tkTraceEvent("request").Uint64("fg", uint64(hwnd)).Uint64("view", uint64(vh)).Msg("touch keyboard: dropped, foreground is a different window of ours")
			return
		}
		hwnd = vh // authoritative own-handle
	}

	// A real tap in THIS window: invalidate pending hides first — even when
	// the show below is debounced away (keyboard already up), a queued
	// blur-hide must not close the keyboard the user just returned to. Kept
	// separate from showGen so a debounced tap cannot orphan an in-flight show.
	kbd.hideGen.Add(1)

	// Whether a hide is animating is read ONCE, here, and drives BOTH decisions
	// this tap makes: passing the debounce, and (carried into the platform show)
	// arming a re-show poll. They must not be taken from two reads — the
	// deadline can expire between them, and a tap admitted precisely BECAUSE the
	// keyboard was closing would then be dispatched as an ordinary show, adopt
	// the pane mid-close and be lost exactly like the tap the bypass exists to
	// save. One read, one decision.
	//
	// The debounce collapses duplicate dispatches from ONE physical tap, and it
	// is safe only because a dropped show leaves the keyboard the previous one
	// opened standing. While that keyboard is CLOSING the premise is inverted:
	// the pane is on its way out, no other command will bring it back, and the
	// dropped show is the one thing that would have. That is the "user closed
	// the keyboard by hand, then immediately tapped the field again" flow — a
	// re-tap that fast lands well inside the 300ms window — so a tap made mid-
	// hide goes through. It cannot pile up work: every dispatch bumps showGen,
	// so the newest supersedes the rest and the service drops them as stale.
	closing := keyboardClosing()
	now := time.Now().UnixNano()
	last := kbd.lastShow.Load()
	visible := kbd.visibleGen.Load()
	active := keyboardShowActive(kbd.showActiveGen.Load(), kbd.showGen.Load())
	switch keyboardTapVerdict(now, last, kbd.showVisibleGen.Load(), visible, active, closing) {
	case tkTapDebounced:
		tkTraceEvent("request").Msg("touch keyboard: dropped by show debounce")
		return
	case tkTapCoalesced:
		// Not a duplicate of a tap that worked — a tap on top of a show that
		// is still trying. Say so distinctly: on a device trace this is the
		// line that separates "the user got their keyboard and tapped again"
		// from "nothing has appeared yet", and the previous message claimed
		// the first while meaning the second.
		tkTraceEvent("request").Int64("gen", kbd.showGen.Load()).Msg("touch keyboard: dropped, an earlier show is still working and no keyboard has appeared yet")
		return
	}
	if !kbd.lastShow.CompareAndSwap(last, now) {
		tkTraceEvent("request").Msg("touch keyboard: dropped, concurrent request won the CAS")
		return
	}
	// Which SEEN keyboard this show starts FROM. Deliberately the value read
	// above rather than a fresh Load: if a keyboard appeared in between, the
	// stale snapshot makes the next tap see a changed visibleGen and dispatch,
	// and dispatching a show too many is recoverable in a way that swallowing
	// a tap is not.
	kbd.showVisibleGen.Store(visible)
	// A dispatched show supersedes any in-flight show.
	gen := kbd.showGen.Add(1)
	// ...and becomes the one that is in flight. Marked immediately after the
	// bump rather than after the platform hand-off below: between the two lines
	// showGen and showActiveGen disagree and keyboardShowActive reads false. The
	// gap cannot be closed entirely — the generation does not exist until Add
	// returns it — but reading false inside it only lets a simultaneous tap
	// dispatch a show too many, which is the recoverable direction.
	kbd.beginShow(gen)
	kbd.hwnd.Store(hwnd)
	tkTraceEvent("request").Uint64("hwnd", uint64(hwnd)).Int64("gen", gen).Bool("closing", closing).Msg("touch keyboard: show dispatched to platform")
	showPlatformTouchKeyboard(kbd, hwnd, gen, closing)
}

// trackEditorFocus implements the symmetric hide: Gio's Windows backend
// never calls the OS (ShowTextInput is a no-op there in both directions),
// so when the last editor of this window loses focus we ask the pane we
// opened to hide. Only keyboards WE showed are hidden (shownByUs) — a
// keyboard the user opened themselves is left alone. focused is whether
// any editor of this window currently has focus.
func (s *touchKeyboardState) trackEditorFocus(gtx layout.Context, focused bool) {
	if focused {
		if !s.focusSeen || s.hidePending {
			// Focus (re)gained after being lost — a blur-hide may already
			// be ENQUEUED (not just pending here): invalidate it, or the
			// service would close the keyboard right after a programmatic
			// focus return or keyboard navigation brought focus back.
			s.hideGen.Add(1)
		}
		s.focusSeen = true
		s.hidePending = false
		return
	}
	if !s.focusSeen {
		return
	}
	if !s.hidePending {
		s.hidePending = true
		s.blurAt = gtx.Now
		gtx.Execute(op.InvalidateCmd{At: s.blurAt.Add(keyboardHideDelay)})
		return
	}
	if gtx.Now.Sub(s.blurAt) < keyboardHideDelay {
		gtx.Execute(op.InvalidateCmd{At: s.blurAt.Add(keyboardHideDelay)})
		return
	}
	s.hidePending = false
	s.focusSeen = false
	s.softKeyboardExpected.Store(false)
	s.platformTouchKeyboardExpected.Store(false)
	// Cancel any still-pending SHOW too, not just hide an already-shown
	// keyboard: a show dispatched moments before the blur may still be in its
	// ~150ms settle or on the retry ladder and has no idea focus has left —
	// without this it would pop the keyboard up AFTER the editor lost focus.
	// requestTouchKeyboardHide alone is a no-op until shownByUs is set.
	s.cancelPendingShow()
	requestTouchKeyboardHide(s)
}

// noteWindowTouchPress records a touch press seen by the window-level
// pointer tracker (i.e. anywhere in the window).
func (s *touchKeyboardState) noteWindowTouchPress(id pointer.ID) {
	s.framePresses = append(s.framePresses, id)
}

// noteEditorTouchPress records a touch press that landed inside one of the
// window's editor areas.
func (s *touchKeyboardState) noteEditorTouchPress(id pointer.ID) {
	s.editorPresses = append(s.editorPresses, id)
}

// tkHeldTouch is a touch press waiting for its terminator, remembered with
// the editor area it landed on. The tag is what keeps one area's abandoned
// press from being spent by another area's cancel: every editor in a window
// shares one touchKeyboardState, Gio hands a brand-new handler a bare Cancel
// the first time it asks for events, and a press whose widget disappeared
// before its release never gets one of its own. Without the tag, opening a
// screen with a text field on it while a finger was down somewhere else would
// raise the keyboard nobody asked for.
// The stamp is the frame time of the press, and it is what stops the record
// from outliving the finger. The tag alone cannot: tags are stable pointers
// (&w.aliasEditor, w.touchKbdTags[i]), so a field that disappears mid-touch and
// comes back later comes back as the SAME tag — and Gio hands the re-registered
// handler a bare Cancel the first time it asks for events, which would spend the
// press abandoned on the previous visit and raise a keyboard the user never
// asked for. Switching tabs away from the console or the alias editor with a
// finger down is the sequence that does it.
type tkHeldTouch struct {
	tag event.Tag
	id  pointer.ID
	at  time.Time
}

// tkMaxHeldEditorTouches bounds editorHeld. Gio terminates every press with a
// release or a cancel, so the bound is unreachable by any sequence it produces;
// it is here so that a backend which ever drops a terminator leaks a fixed
// handful of ints instead of growing for the life of the process.
const tkMaxHeldEditorTouches = 10

// tkHeldTouchTTL is how long a press may wait for its terminator and still be
// treated as part of the same touch.
//
// The window is a compromise between two mistakes with different costs. Too
// short and a genuine press-then-grab loses its keyboard: the cancel arrives
// only once the finger has moved past the 3dp drag slop, which a slow, careful
// tap can take a moment to do — the user gets nothing, which is the complaint
// this whole path exists to answer. Too long and an abandoned press survives to
// be spent by the startup cancel of a field that has come back — the user gets a
// keyboard they did not ask for, which is annoying but self-correcting, since
// tapping anywhere else dismisses it.
//
// A second and a half sits above any within-gesture delay and below any
// realistic away-and-back, so both mistakes need a deliberate effort to
// provoke. It does not make the stale case impossible — a field that vanishes
// and returns inside the window still slips through — and that is the side to
// err on.
//
// The clock is gtx.Now, which Gio fills from the frame event. Should it ever
// arrive zero, every press reads as young and every cancel raises a keyboard:
// the comparison fails towards showing one, which is the harmless direction.
const tkHeldTouchTTL = 1500 * time.Millisecond

// holdEditorTouch records a touch press inside the editor area `tag` as
// unresolved, stamped with the frame time `now`.
func (s *touchKeyboardState) holdEditorTouch(tag event.Tag, id pointer.ID, now time.Time) {
	s.expireEditorTouches(now)
	for i, h := range s.editorHeld {
		if h.tag == tag && h.id == id {
			// Same finger, same area, pressed again without a terminator in
			// between: re-stamp rather than keep the older press alive, or a
			// stale record could be refreshed into eternity.
			s.editorHeld[i].at = now
			return
		}
	}
	if len(s.editorHeld) >= tkMaxHeldEditorTouches {
		copy(s.editorHeld, s.editorHeld[1:])
		s.editorHeld = s.editorHeld[:len(s.editorHeld)-1]
	}
	s.editorHeld = append(s.editorHeld, tkHeldTouch{tag: tag, id: id, at: now})
}

// expireEditorTouches forgets presses older than tkHeldTouchTTL. It runs from
// the hold path only. The take path deliberately does NOT call it: it already
// removes every record for its own area and ages each one as it goes, so an
// expiry sweep in front of that loop would make the age test there unreachable
// — and that test is the one thing standing between a stale record and a
// keyboard nobody asked for. Records belonging to areas that never come back
// are drained by the next press instead, and bounded meanwhile by
// tkMaxHeldEditorTouches.
func (s *touchKeyboardState) expireEditorTouches(now time.Time) {
	kept := s.editorHeld[:0]
	for _, h := range s.editorHeld {
		if now.Sub(h.at) > tkHeldTouchTTL {
			continue
		}
		kept = append(kept, h)
	}
	s.editorHeld = kept
}

// dropEditorTouch resolves one held press, identified by area and pointer id.
// Only the Release path can do this: a release is a genuine pointer event and
// carries a meaningful id.
func (s *touchKeyboardState) dropEditorTouch(tag event.Tag, id pointer.ID) {
	for i, h := range s.editorHeld {
		if h.tag == tag && h.id == id {
			s.editorHeld = append(s.editorHeld[:i], s.editorHeld[i+1:]...)
			return
		}
	}
}

// takeEditorTouches resolves every press held on the editor area `tag` and
// reports whether there was at least one. This is the cancel path's answer to
// "was a finger down on THIS editor", since a cancel cannot be attributed to a
// pointer id. Taking all of that area's presses at once is what keeps a
// multi-finger cancel from raising the keyboard once per finger; leaving other
// areas' presses alone is what keeps this cancel from spending them.
func (s *touchKeyboardState) takeEditorTouches(tag event.Tag, now time.Time) bool {
	held := false
	kept := s.editorHeld[:0]
	for _, h := range s.editorHeld {
		if h.tag == tag {
			// Taken either way — a stale press must not survive the cancel to
			// be spent by the next one — but only a press young enough to
			// belong to this touch answers "yes, a finger was down here".
			if now.Sub(h.at) <= tkHeldTouchTTL {
				held = true
			}
			continue
		}
		kept = append(kept, h)
	}
	s.editorHeld = kept
	return held
}

// outsideTapPending reports whether the previous frame contained a touch
// press outside every editor area, and resets the per-frame records.
func (s *touchKeyboardState) outsideTapPending() bool {
	outside := false
	for _, id := range s.framePresses {
		claimed := false
		for _, e := range s.editorPresses {
			if e == id {
				claimed = true
				break
			}
		}
		if !claimed {
			outside = true
			break
		}
	}
	s.framePresses = s.framePresses[:0]
	s.editorPresses = s.editorPresses[:0]
	return outside
}

// dismissOnOutsideTap reacts to a touch press outside every editor area in
// the previous frame: it cancels any in-flight show request (the platform
// helper delays ~150ms before TryShow — a quick tap on a button must stop
// that show even though the keyboard is not up yet), and clears editor
// focus when our keyboard is up. Gio's Clickable does not steal key focus,
// so without the explicit clear a tap on a button or the chat would leave
// the editor focused forever and the blur-driven TryHide would never fire.
//
// Call at the VERY START of layout, before any action handlers: handlers
// that intentionally focus an editor (Reply, Alias, console suggestions)
// then issue their FocusCmd after ours and win. For taps whose press,
// release and handler all land in one frame, noteExplicitEditorFocus
// suppresses the next evaluation so the handler's focus survives.
//
// Reports whether it cleared focus. That clear is deliberate, and anything that
// reads an empty focus as a signal has to be told to ignore this one — see the
// call site.
func (s *touchKeyboardState) dismissOnOutsideTap(gtx layout.Context) bool {
	if s.suppressDismiss {
		s.suppressDismiss = false
		s.framePresses = s.framePresses[:0]
		s.editorPresses = s.editorPresses[:0]
		return false
	}
	if !s.outsideTapPending() {
		return false
	}
	s.cancelPendingShow()
	if !s.shownByUs.Load() {
		return false
	}
	gtx.Execute(key.FocusCmd{}) // nil Tag clears focus
	return true
}

// cancelPendingShow invalidates any in-flight show dispatch (a newer showGen
// makes the service drop it) AND clears the show stamp. Clearing the stamp is
// essential: an outside tap cancels the pending show, so a legitimate editor
// re-tap right after must dispatch a FRESH show rather than be swallowed by
// the window of the show that was just cancelled. Without it the sequence
// editor→button→editor leaves the editor focused with no keyboard (first show
// cancelled, second dropped by the tap verdict).
//
// One store covers both refusals, and that is not an accident of
// implementation: the debounce and the coalesce are both measured from
// lastShow, so a zero stamp is outside both windows at once. It has to be
// both. The coalesce declines to disturb a show that is still working, and
// the show this cancels is by definition no longer working.
// The in-flight mark goes with them. Unconditionally rather than through
// finishShow, which is safe because the bump above has already made every live
// generation stale: there is no newer generation left whose mark a store could
// damage. It is worth being plain that this line is hygiene and not load
// bearing — keyboardShowActive compares the mark against showGen, so a mark
// left behind by a cancel reads inactive anyway, and the next dispatch raises
// it past this value regardless. Deleting it would not reopen the defect. It
// stays because leaving a generation marked in flight after its cancellation
// is a lie about the state, and the next reader of this struct should not have
// to re-derive that it happens to be a harmless one.
func (s *touchKeyboardState) cancelPendingShow() {
	s.showGen.Add(1)
	s.lastShow.Store(0)
	s.showActiveGen.Store(0)
}

// noteExplicitEditorFocus must accompany every intentional FocusCmd that
// moves focus INTO an editor (Reply, Alias, console suggestion pick): the
// tap that triggered it landed outside the editor areas, and without this
// the next frame's outside-tap evaluation would immediately defocus the
// editor the handler just focused (and cancel the pending keyboard show).
func (s *touchKeyboardState) noteExplicitEditorFocus() {
	s.framePresses = s.framePresses[:0]
	s.editorPresses = s.editorPresses[:0]
	s.suppressDismiss = true
}

// requestTouchKeyboardHide hides the keyboard for kbd's window if it was
// opened by us. The captured hide generation invalidates this hide as soon
// as any editor tap (even a debounced one) happens before it runs.
//
// It reports the generation the hide was DISPATCHED with, and false when
// nothing was dispatched because the keyboard is not ours to hide. A caller
// that throttles repeat asks must key on this value rather than re-read
// hideGen: the service goroutine bumps hideGen too (tkCmdRelease), so a
// second read can name a generation the queued command does not carry, and
// the caller would then treat a dead ask as still in flight forever.
func requestTouchKeyboardHide(kbd *touchKeyboardState) (int64, bool) {
	if !kbd.shownByUs.Load() {
		return 0, false
	}
	gen := kbd.hideGen.Load()
	hidePlatformTouchKeyboard(kbd, gen)
	return gen, true
}

// requestTouchKeyboardRoom asks for the touch keyboard to be taken away on
// behalf of a surface that has too little clear height to draw itself, so the
// room appears within a frame or two and the still-open surface draws itself
// then. askedGen is that surface's OWN throttle marker; the caller clears it
// to 0 on the frames it has room, which is what re-arms the next ask.
//
// The ask is throttled by the hide GENERATION it was dispatched with — not by
// a bool, not by a clock, and not by the occlusion it was made for. Occlusion
// described the keyboard, but what a repeat ask has to know is whether the
// PREVIOUS ask is still alive, and the two come apart exactly where it
// matters: every editor tap bumps hideGen, doHide then drops the command, and
// because the retry ladder re-enqueues the SAME generation the hide is
// cancelled rather than eventually retried. So after "open the surface → close
// it → tap the editor → open it again" the keyboard stands at the same height,
// an occlusion key calls that the same ask, no new hide is sent and the
// surface sits open and undrawn for good. hideGen moves on precisely the
// events that kill an ask, so keying on it needs no clearing rule at any site
// that closes a surface — and a rule that has to be repeated at N sites is the
// exact shape that has produced regressions in this code. A wall-clock
// deadline would misbehave across the sleep/resume time jumps this tablet
// actually does.
//
// Both the ask and the caller's own reset of askedGen are statements about
// what is on screen NOW, so neither may run in an inert measuring pass — and
// the two callers reach that rule from opposite sides, which is deliberate
// rather than an inconsistency to be tidied away. A context menu is a Stacked
// overlay no measurement reaches, so menuOverlayRoom needs no guard and does
// not look at gtx.Enabled(); the emoji picker sits inside the composer, which
// keyboardTailRow measures with the keyboard's height handed back, so
// emojiPickerRoom returns before BOTH branches on a disabled source. Making
// either one look like the other would be a bug: a guard in the menu is dead
// code, and dropping the picker's guard would reset the marker off a
// measurement of a window that is not the one on screen, and ask again every
// frame.
//
// The marker stores the generation PLUS ONE so that 0 can mean "nothing
// asked": hideGen legitimately starts at 0, and a bare 0 would suppress the
// first ask of the session. The generation is taken from the dispatch itself
// instead of being re-read, because the service goroutine bumps hideGen too.
// An ask that was NOT dispatched stores nothing — no command is in flight, so
// there is nothing to throttle and the next frame may try again.
//
// LIMIT, stated rather than papered over: requestTouchKeyboardHide only hides
// a keyboard WE opened. If the user raised it themselves we cannot take it
// away, and on a window this short the surface stays open but undrawn until
// they lower it — at which point it appears. Every caller therefore has to
// keep a way OUT of the deferred surface live the whole time: a dismiss area,
// the Escape handler, the toggle that opened it.
func requestTouchKeyboardRoom(kbd *touchKeyboardState, askedGen *int64) {
	if *askedGen != 0 && kbd.hideGen.Load() == *askedGen-1 {
		return
	}
	*askedGen = 0
	if gen, sent := requestTouchKeyboardHide(kbd); sent {
		*askedGen = gen + 1
	}
}

// cancelLongPressOnMultiTouch cancels an armed long-press when a second
// touch has been active window-wide during the hold. Per-card state only
// sees events routed to its own area, so a second finger landing elsewhere
// in the list would otherwise never reach the first card's state and the
// menu would still open after 500ms. Two signals are needed: the live
// active-touch set (touchPressPos: Press adds, Release/Cancel removes) AND
// multiTouchAt — the tracker records the moment a touch press overlapped
// an already-active touch, because a second finger that pressed and
// released within one frame is already gone from the map by the time this
// guard runs.
func (w *Window) cancelLongPressOnMultiTouch(rc *rightClickState) {
	// Also consider a hold that already MATURED on a late Release: in a
	// delayed frame the release can set matured=true and clear touchDown
	// before this guard runs, so checking touchDown alone would let a
	// multi-touch (scroll/zoom) still open the menu.
	if !rc.touchDown && !rc.matured {
		return
	}
	if len(w.touchPressPos) > 1 || !rc.touchStart.After(w.multiTouchAt) {
		rc.touchDown = false
		rc.matured = false
	}
}

// touchInputRecency bounds how long after the last pointer press an action
// still counts as driven by that press. Menu/suggestion picks happen right
// after their press; a stale touch flag from minutes ago must not make a
// KEYBOARD-driven pick (Enter/Tab on console suggestions) raise the touch
// keyboard while the user types on real keys.
const touchInputRecency = 2 * time.Second

// touchDrivenInput reports whether the action being handled this frame was
// plausibly driven by a touch press.
func (w *Window) touchDrivenInput(gtx layout.Context) bool {
	return w.lastInputTouch && gtx.Now.Sub(w.lastPressAt) < touchInputRecency
}

// pressPoint is one pointer's press: WHERE it landed in window space and WHEN
// (the gtx.Now of the frame it was processed on). The frame stamp exists for
// the widget.Clickable path: Gio v0.10's widget.Press carries no PointerID, so
// the only handle back to the pointer that made a button press is the frame it
// began on, which Gio stamps into Press.Start from the same gtx.Now the root
// cursor tracker records here (see Window.pressWindowPos).
type pressPoint struct {
	pos image.Point
	at  time.Time
}

// pressAnchor returns the window-level position of a pointer's press,
// looked up by PointerID from the window tracker (all sources — touch,
// mouse, pen). The global lastCursorPos is only a fallback: within one
// frame it already holds the LAST event's coordinate (a same-frame drag,
// or another concurrently active pointer), while the per-ID map preserves
// each pointer's own press point — so a mouse right-click menu can't be
// relocated by a finger that moved later in the same frame.
func (w *Window) pressAnchor(pe pointer.Event) image.Point {
	if p, ok := w.pointerPressPos[pe.PointerID]; ok {
		return p.pos
	}
	return w.lastCursorPos
}
