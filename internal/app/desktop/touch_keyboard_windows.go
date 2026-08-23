//go:build windows

package desktop

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"gioui.org/app"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/windows"
)

// Gio's Windows backend does not implement ShowTextInput, and because the
// whole window is custom-rendered there is no text control Windows could
// detect to auto-invoke the touch keyboard. We invoke it explicitly:
//
//  1. Preferred: InputPane.TryShow through the IInputPaneInterop WinRT
//     interop. TryShow is idempotent (shows a hidden pane, never hides a
//     visible one).
//  2. Fallback to the legacy ITipInvocation.Toggle exposed by the
//     touch-keyboard host (starting TabTip.exe first when its
//     out-of-process COM server is not yet running) in the two cases where
//     the WinRT path produced NO keyboard and we can PROVE none is on
//     screen:
//       • tkPaneUnavailable — the API is genuinely absent (missing exports
//         or unregistered class on old Win10 builds);
//       • tkPaneRefused — the API exists and declined. Microsoft documents
//         TryShow as best effort, and it returns false for a foreground
//         check we cannot influence; respecting that refusal by doing
//         nothing leaves the tapped field with no way to type into it. The
//         refusal is only reached after BOTH the OccludedRect probe and
//         IFrameworkInputPane::Location have conclusively answered "not
//         visible", so the hazard that once justified stopping here — a
//         blind global Toggle closing a keyboard the user is looking at —
//         is already excluded at that point.
//     A third case is verified rather than reported: TryShow can return
//     TRUE and still show nothing ("the touch keyboard is shown only if a
//     hardware keyboard is not available"), so an accepted show is checked
//     once, tkShowVerifyDelay later, and joins the same fallback when four
//     independent probes agree that no keyboard exists (see doVerifyShow).
//     Transient WinRT errors (tkPaneError) never reach Toggle — they prove
//     nothing about what is on screen, and on Windows 11 a blind Toggle
//     could close an already-open keyboard.
//
// Keyboard visibility is judged by OccludedRect, not InputPane.Visible —
// Microsoft documents the Visible property for Xbox only.
//
// While the pane is up, a per-window monitor polls OccludedRect for that
// window's HWND and publishes the occluded height into the window's
// touchKeyboardState, so its layout can pad the bottom (the composer and
// the console input live there and would otherwise be covered).

var (
	tkOle32                = windows.NewLazySystemDLL("ole32.dll")
	tkProcCoCreateInstance = tkOle32.NewProc("CoCreateInstance")

	tkUser32             = windows.NewLazySystemDLL("user32.dll")
	tkProcFindWindowW    = tkUser32.NewProc("FindWindowW")
	tkProcIsWindowVis    = tkUser32.NewProc("IsWindowVisible")
	tkProcGetWindowLongW = tkUser32.NewProc("GetWindowLongW")
	// The single-threaded apartment's message pump. Kept in its own group so
	// the longer names here cannot widen the alignment of the four above.
	tkProcPeekMessageW     = tkUser32.NewProc("PeekMessageW")
	tkProcDispatchMessageW = tkUser32.NewProc("DispatchMessageW")
	tkProcMsgWait          = tkUser32.NewProc("MsgWaitForMultipleObjectsEx")

	tkCombase                    = windows.NewLazySystemDLL("combase.dll")
	tkProcRoInitialize           = tkCombase.NewProc("RoInitialize")
	tkProcRoUninitialize         = tkCombase.NewProc("RoUninitialize")
	tkProcRoGetActivationFactory = tkCombase.NewProc("RoGetActivationFactory")
	tkProcWindowsCreateString    = tkCombase.NewProc("WindowsCreateString")
	tkProcWindowsDeleteString    = tkCombase.NewProc("WindowsDeleteString")
)

type tkGUID struct {
	Data1 uint32
	Data2 uint16
	Data3 uint16
	Data4 [8]byte
}

var (
	// CLSID {4CE576FA-83DC-4F88-951C-9D0782B4E376} — UIHostNoLaunch (TabTip).
	tkClsidUIHostNoLaunch = tkGUID{
		0x4CE576FA, 0x83DC, 0x4F88,
		[8]byte{0x95, 0x1C, 0x9D, 0x07, 0x82, 0xB4, 0xE3, 0x76},
	}
	// IID {37C994E7-432B-4834-A2F7-DCE1F13B834B} — ITipInvocation.
	tkIidITipInvocation = tkGUID{
		0x37C994E7, 0x432B, 0x4834,
		[8]byte{0xA2, 0xF7, 0xDC, 0xE1, 0xF1, 0x3B, 0x83, 0x4B},
	}
	// IID {75CF2C57-9195-4931-8332-F0B409E916AF} — IInputPaneInterop.
	tkIidIInputPaneInterop = tkGUID{
		0x75CF2C57, 0x9195, 0x4931,
		[8]byte{0x83, 0x32, 0xF0, 0xB4, 0x09, 0xE9, 0x16, 0xAF},
	}
	// IID {640ADA70-06F3-4C87-A678-9829C9127C28} — IInputPane (OccludedRect).
	tkIidIInputPane = tkGUID{
		0x640ADA70, 0x06F3, 0x4C87,
		[8]byte{0xA6, 0x78, 0x98, 0x29, 0xC9, 0x12, 0x7C, 0x28},
	}
	// IID {8A6B3F26-7090-4793-944C-C3F2CDE26276} — IInputPane2 (TryShow).
	tkIidIInputPane2 = tkGUID{
		0x8A6B3F26, 0x7090, 0x4793,
		[8]byte{0x94, 0x4C, 0xC3, 0xF2, 0xCD, 0xE2, 0x62, 0x76},
	}
)

// tkTipVtbl is the ITipInvocation vtable: IUnknown methods followed by Toggle.
type tkTipVtbl struct {
	QueryInterface uintptr
	AddRef         uintptr
	Release        uintptr
	Toggle         uintptr
}

// tkInteropVtbl is the IInputPaneInterop vtable (IInspectable + GetForWindow).
type tkInteropVtbl struct {
	QueryInterface      uintptr
	AddRef              uintptr
	Release             uintptr
	GetIids             uintptr
	GetRuntimeClassName uintptr
	GetTrustLevel       uintptr
	GetForWindow        uintptr
}

// tkPaneVtbl is the IInputPane vtable (IInspectable + Showing/Hiding event
// registration + OccludedRect, in declaration order of the interface).
type tkPaneVtbl struct {
	QueryInterface      uintptr
	AddRef              uintptr
	Release             uintptr
	GetIids             uintptr
	GetRuntimeClassName uintptr
	GetTrustLevel       uintptr
	AddShowing          uintptr
	RemoveShowing       uintptr
	AddHiding           uintptr
	RemoveHiding        uintptr
	GetOccludedRect     uintptr
}

// tkPane2Vtbl is the IInputPane2 vtable (IInspectable + TryShow, TryHide).
type tkPane2Vtbl struct {
	QueryInterface      uintptr
	AddRef              uintptr
	Release             uintptr
	GetIids             uintptr
	GetRuntimeClassName uintptr
	GetTrustLevel       uintptr
	TryShow             uintptr
	TryHide             uintptr
}

type (
	tkTipObj     struct{ vtbl *tkTipVtbl }
	tkInteropObj struct{ vtbl *tkInteropVtbl }
	tkPaneObj    struct{ vtbl *tkPaneVtbl }
	tkPane2Obj   struct{ vtbl *tkPane2Vtbl }
)

// tkRect is Windows.Foundation.Rect (float32 fields).
type tkRect struct {
	X, Y, Width, Height float32
}

func tkRelease(vtblRelease uintptr, obj unsafe.Pointer) {
	syscall.SyscallN(vtblRelease, uintptr(obj))
}

// ---- Keyboard command service ------------------------------------------
//
// EVERY platform keyboard operation — show, hide, event (un)registration,
// window release — is a command processed by ONE service goroutine on one
// locked, WinRT-initialized thread. UI threads only enqueue (lossless,
// non-blocking) and read atomics. All validity checks (released, request
// generation, foreground window) happen inside the service immediately
// before the COM calls they guard, so there is no window between "checked
// on thread A" and "acted on thread B": the entire class of
// check-then-act races between show, hide, console destruction and event
// registration is structurally impossible rather than individually
// patched.

type tkCmdKind int

const (
	tkCmdShow tkCmdKind = iota
	tkCmdHide
	tkCmdRelease
	tkCmdAdvise         // proactively register Showing/Hiding for a window (ViewEvent)
	tkCmdShowingEvent   // pane Showing callback, marshaled onto the service thread
	tkCmdHidingEvent    // pane Hiding callback, marshaled onto the service thread
	tkCmdFwRetry        // timer wake: retry a transient FrameworkInputPane failure
	tkCmdReconcile      // timer wake: re-check pane visibility after an inconclusive post-advise probe
	tkCmdCompensateHide // internal: hide a keyboard opened by a show that was cancelled mid-call, retried
	tkCmdUnadviseRetry  // internal: re-attempt a failed Unadvise
	tkCmdOwnerExpire    // internal: occlusion monitor ended a hidden session
	tkCmdPublish        // internal: occlusion monitor height sample
	tkCmdVerifyShow     // internal: did the accepted TryShow actually produce a keyboard?
	tkCmdPaneTruth      // internal: the physical pane disagrees with paneVisible
)

type tkCommand struct {
	kind    tkCmdKind
	kbd     *touchKeyboardState
	hwnd    windows.HWND // show only
	gen     int64        // show/hide: originating request generation
	gen0    int64        // reconcile only: paneEventGen baseline, captured pre-pin by the scheduler
	height  int32        // publish only: sampled occlusion height (dp)
	epoch   int64        // publish/paneTruth: occlusionEpoch when sampled; verify/compensate: tkPaneShowSeq baseline
	retries int8         // show: COM-init/transient-error re-enqueues (own ladder)
	polls   int8         // show: re-show poll iterations (own ladder, NOT retries)
	fgWaits int8         // hide only: re-attempts spent waiting for our window to return to foreground
	legacy  bool         // compensate only: the cancelled show used the legacy Toggle path
	sawPane bool         // compensate: the accepted pane was seen visible; paneTruth: the pane is physically UP
	toggled bool         // compensate/legacy only: our one permitted ITipInvocation.Toggle has SUCCEEDED
	reshow  bool         // show only: this command IS a re-show poll iteration
	closing bool         // show only: a hide was animating at TAP time -> may arm a re-show poll
}

var (
	tkCmdMu   sync.Mutex
	tkCmdList []tkCommand
	tkCmdKick = make(chan struct{}, 1)
	tkSvcOnce sync.Once
)

// tkCmdEvent is the service thread's other wakeup: a Win32 auto-reset event,
// signalled alongside tkCmdKick by tkEnqueue. The channel on its own cannot
// serve a single-threaded apartment — a goroutine parked on a receive
// dispatches no window messages, so the Showing/Hiding callbacks the pane
// registered would never be delivered and the shell thread making them would
// block until it gave up. MsgWaitForMultipleObjectsEx can wait on the message
// queue and on a wakeup at the same time, but the wakeup has to be a HANDLE,
// so the channel needs an event twin.
//
// Zero if CreateEvent failed. That degrades the service back to the plain
// channel receive: commands still arrive and shows still run, only the
// callbacks stop — which is why tkEnqueue signals both and tkAwaitCommands
// falls back rather than either of them assuming an event exists.
var tkCmdEvent = func() windows.Handle {
	h, err := windows.CreateEvent(nil, 0, 0, nil)
	if err != nil {
		return 0
	}
	return h
}()

// tkPaneShowSeq counts INDEPENDENT user pane appearances — a Showing event
// (any window) with shownByUs still false that is NOT the pane a compensation
// itself just opened (see touchKeyboardState.expectOwnPaneShow). It is bumped
// in the Showing CALLBACK, in true event order, so a compensation captured
// before a manual keyboard open reliably sees the advance and aborts rather
// than closing that keyboard. It is bumped a SECOND way, by the same
// predicate, in reconcilePaneVisible: a pane that appeared while no handler
// was advised produces no callback at all, and a counter that only the
// callback maintains would report "nothing happened" for the entire window in
// which we were deaf — the window a user is most likely to open the keyboard
// by hand in. The two paths do not double-count, and it takes BOTH halves of a
// guard to say that, because the callback can land on either side of the
// synthesis. A callback that already ran is caught by the CAS: reconcile's bump
// is gated on the same compare-and-swap that claims the pane's generation, and
// that gate is only as good as its baseline, which is why the baseline is the
// CALLER's and is taken before the handler is even reachable. A callback still
// in flight cannot be caught that way at all — nothing has happened yet to
// compare against — so reconcile arms tkSynthShowMark, naming the registration
// and the exact generation that callback will stamp, and only that callback can
// claim it. Either way the pane is judged exactly once, which matters less for
// the count itself than for what judging twice would do: the second judge finds
// expectOwnPaneShow already consumed by the first and reads OUR pane as the
// user's, aborting the compensation that pane needs.
// Both ends must be honest about WHEN: the bump is
// stamped in the callback, and the compensation's baseline is taken on the
// FIRST line of the show attempt that may produce it — ahead of the COM ladder,
// the settle sleep and the platform call alike, every one of which the user's
// open can race and which would then be read as "no change". Package-level +
// atomic because the callback runs on an arbitrary RPC thread while the service
// reads it.
var tkPaneShowSeq atomic.Int64

// tkHideDeadlineNs is a tkNowNs() timestamp until which a hide that has STARTED
// (our accepted TryHide, or any pane Hiding) is still animating closed. It is
// PROCESS-WIDE, not per-window, because there is exactly one touch keyboard
// for the process: a hide begun for the main window is still closing when the
// user taps a field in the console, and that tap must see it — a per-window
// deadline read zero there and the console adopted a pane that then finished
// closing, leaving a focused field with no keyboard. Zeroed only by a show the
// SERVICE THREAD commits itself — an act it is performing, not news it is being
// told — and otherwise left to self-expire after tkHideAnimWindow, so a stale
// value cannot misfire on a stable pane. Deliberately NOT cleared by
// endKeyboardSession: one window's teardown must not erase a close another
// window's tap still needs to see. Nor by a Showing command: that is news about
// ONE registration, reaching this thread through the queue with nothing on it to
// say how old the event is — the callback runs whenever the apartment happens to
// pump, and the two windows advised over the one pane each raise their own — and
// a process record zeroed from an event of foreign scope and unknown age loses
// the close it was holding.
//
// This is the record the SERVICE THREAD keeps, and it lags: a Hiding delivered
// to a callback is recorded here only once the command that callback enqueued
// gets its turn. The timely half of the answer lives on tkPaneHandler.hide, one
// slot per registration; platformKeyboardClosing consults both.
var tkHideDeadlineNs atomic.Int64

// tkSynthMark identifies ONE pane appearance that reconcilePaneVisible has
// already classified, so the Showing still in flight for that same appearance
// can be recognized and skipped instead of classifying it a second time.
//
// It exists because the CAS in reconcilePaneVisible proves less than it looks
// like it proves: it establishes only that no Showing had been delivered AT
// THAT INSTANT. A pane the probe found was raised by the system, so its Showing
// may still be travelling on an RPC thread and land a moment AFTER the
// synthesis — and then both paths classify one physical pane. Whichever runs
// first consumes expectOwnPaneShow; the second finds the mark gone, reads that
// as "not ours", and bumps tkPaneShowSeq. For a cancelled TryShow of OUR OWN
// that is the worst possible answer: the compensation cleaning up that pane
// sees the sequence move, concludes the user opened it by hand, backs off, and
// leaves an orphan keyboard open over the app. Ordering the two paths is
// impossible (one is an OS callback on a thread we do not own), so they are
// deduped instead.
//
// The mark is an IDENTITY, not just a time window, and the identity is what
// makes it safe. Registrations are per-window (regs is keyed by
// *touchKeyboardState), so a bare process-wide flag would let a Showing raised
// on a DIFFERENT window's registration swallow the mark, and a Hiding on a
// different registration clear it. Both directions are wrong, and they are not
// equally wrong:
//
//   - Failing to consume a mark that WAS ours costs one extra tkPaneShowSeq
//     bump. A pending compensation then aborts and leaves an orphan keyboard
//     the user can close.
//   - Consuming a mark that was NOT ours costs a MISSING bump for a keyboard
//     the user opened by hand, and the pending compensation then closes it.
//
// That is the same asymmetry tkOwnPaneShowWindow is sized around, and the same
// conclusion follows: the mark must be as NARROW as it can be made, because
// over-bumping is the recoverable side. So it names both the state that
// classified the pane and the exact generation the correlated callback will
// carry, and only that callback can claim it:
//
//	kbd  — the registration reconcile was reconciling.
//	gen  — the value paneEventGen.Add(1) will return in the in-flight Showing
//	       for this same pane. A winning CAS in reconcile moves the counter from
//	       gen0 to gen0+1, so the next Add yields gen0+2. Derived, never
//	       re-read: a fresh Load could already include that very Add. It also
//	       makes the mark inert during the interval BEFORE that CAS, where a
//	       Showing can only stamp gen0+1 — which is what lets the mark be armed
//	       ahead of the CAS without ever swallowing the wrong event.
//	deadlineNs — tkNowNs() outer bound, because gen alone is not unique
//	       forever: with no Showing in flight at all (the ordinary missed-event
//	       case the reconciliation exists for) the counter simply waits, and the
//	       user's NEXT hand-opened pane would arrive carrying exactly gen0+2.
//
// PACKAGE-LEVEL because there is one touch keyboard per process and at most one
// pane appearance can be mid-classification at a time; an atomic.Pointer swap
// keeps the callback's RPC thread lock-free, as required of every callback here.
type tkSynthMark struct {
	kbd        *touchKeyboardState
	gen        int64
	deadlineNs int64
}

var tkSynthShowMark atomic.Pointer[tkSynthMark]

// tkMonoAnchor / tkNowNs give a PROCESS-MONOTONIC nanosecond counter for the
// ownership deadlines above. They deliberately do not use time.Now().UnixNano():
// UnixNano strips the monotonic reading, leaving a bare wall-clock value, and a
// Windows tablet resyncs its clock on every resume from sleep — exactly the
// moment this code is busiest. A backwards step would hold a deadline open far
// past its nominal window (a late hand-opened Showing then claims the own-pane
// mark, or ordinary taps keep reading "a hide is still closing"); a forward step
// expires it early. time.Since subtracts monotonic readings, so the difference
// is immune to both. The anchor is taken at package init, so the counter is
// always > 0 once any nonzero window is added to it, which keeps 0 usable as
// the "no deadline set" sentinel.
//
// The limit worth naming: whether this counter advances across a machine
// suspend is up to the platform timer (S3 typically freezes it, modern standby
// typically does not), so a deadline straddling sleep may come back either
// still pending or already expired. Both outcomes are acceptable, and the
// asymmetry is deliberate: an EXPIRED deadline is the safe one — the own-pane
// mark is dropped, so the first Showing after resume counts as the user's and
// aborts a pending compensation instead of licensing a hide — and a still-
// pending one is bounded by its own sub-second window. What the wall clock
// offered instead was an UNBOUNDED overrun on a backward resync, which is why
// nothing here should be moved back to it.
var tkMonoAnchor = time.Now()

func tkNowNs() int64 { return int64(time.Since(tkMonoAnchor)) }

// tkClaimOwnPaneShow reports whether this Showing is the pane one of OUR
// TryShow calls opened, consuming the mark so only the FIRST Showing after it
// can claim it. The mark carries a DEADLINE: an accepted TryShow whose pane
// never materialized would otherwise leave it claimable for the compensation's
// whole ~16s budget, and the next keyboard the USER opens by hand would
// silently claim it — skipping the tkPaneShowSeq bump that is the only thing
// aborting a pending compensation before it hides that keyboard. Past the
// deadline the mark is dropped and NOT claimed, so a late Showing counts as an
// independent user open. The deadline is the OUTER bound: a compensation that
// sights the pane clears the mark right then, since one TryShow yields at most
// one pane. Runs on the callback's arbitrary RPC thread; the CAS is what keeps
// two concurrent Showings from both claiming it.
func tkClaimOwnPaneShow(kbd *touchKeyboardState) bool {
	d := kbd.expectOwnPaneShow.Load()
	if d == 0 || !kbd.expectOwnPaneShow.CompareAndSwap(d, 0) {
		return false
	}
	return tkNowNs() < d
}

// tkClaimSynthShow reports whether the Showing that kbd's callback just stamped
// with gen is the pane reconcilePaneVisible already classified, and consumes the
// mark if so — true means the caller must NOT classify it again.
//
// Both halves of the identity are required. A different registration's Showing
// carries a different kbd and is left alone, so it still counts as the user's;
// a later Showing on the SAME registration carries a later gen and is likewise
// left alone. The deadline is the third guard, for the case where the mark was
// armed with nothing in flight behind it: gen alone would then still match the
// user's next hand-opened pane, whenever it came. An expired mark is dropped
// rather than honored, so that pane is never silently swallowed. Claimed at most
// once, and lock-free (a single CAS) because this runs on an arbitrary RPC
// thread.
func tkClaimSynthShow(kbd *touchKeyboardState, gen int64) bool {
	m := tkSynthShowMark.Load()
	if m == nil || m.kbd != kbd || m.gen != gen {
		return false
	}
	if !tkSynthShowMark.CompareAndSwap(m, nil) {
		return false
	}
	return tkNowNs() < m.deadlineNs
}

// tkDropSynthShowMark clears a reconciliation mark belonging to kbd, called when
// kbd's pane is ending. The Hiding is the real terminator; the deadline only
// covers a Hiding that never arrives. Scoped to kbd for the same reason the
// claim is: a Hiding on ANOTHER window's registration must not clear a mark
// armed for this one, or the callback it was guarding would classify its pane a
// second time. Called from inside tkStampHiding's section, which is also where
// the claim above runs, so the two cannot interleave — and that, not any
// delivery order COM promises for an MTA object, is what stops a Hiding from
// clearing a mark a Showing is in the middle of matching.
func tkDropSynthShowMark(kbd *touchKeyboardState) {
	if m := tkSynthShowMark.Load(); m != nil && m.kbd == kbd {
		tkSynthShowMark.CompareAndSwap(m, nil)
	}
}

// tkEnqueue hands a command to the service. Never blocks, never drops
// (mutex-guarded slice, not a bounded channel): the console's DestroyEvent
// fire-and-forgets its release, and FIFO order serializes release-after-
// advise and hide-after-show interleavings.
func tkEnqueue(cmd tkCommand) {
	tkSvcOnce.Do(func() { go tkKeyboardService() })
	tkCmdMu.Lock()
	tkCmdList = append(tkCmdList, cmd)
	tkCmdMu.Unlock()
	// Both wakeups, always: the event is what the pumping wait blocks on, the
	// channel is what the degraded wait blocks on, and which one the service
	// chose is not knowable from here. Signalling an auto-reset event that is
	// already set is a no-op, and the channel send is non-blocking, so the
	// redundant one costs nothing.
	if tkCmdEvent != 0 {
		_ = windows.SetEvent(tkCmdEvent)
	}
	select {
	case tkCmdKick <- struct{}{}:
	default:
	}
}

// platformActiveWindowHandle returns the native handle of the foreground
// window when it belongs to this process, or 0. Called at tap time so a
// window's touchKeyboardState binds to ITS handle, not to whatever window
// is foreground after the settle delay.
func platformActiveWindowHandle() uintptr {
	return uintptr(tkOwnForegroundWindow())
}

// showPlatformTouchKeyboard enqueues a show for kbd's window (handle bound at
// tap time). Non-blocking.
//
// closing is DECIDED BY THE CALLER and passed in; this function must not
// re-read the deadline. requestTouchKeyboard reads it once and uses that one
// value for two coupled decisions — whether the tap may pass the show debounce,
// and whether the show may arm a re-show poll. Re-reading here would let the
// two disagree: the deadline can expire between the reads, and then a tap
// admitted PRECISELY BECAUSE the keyboard was closing would be dispatched with
// closing=false, arm no poll, adopt the pane mid-close and lose the very re-tap
// the bypass existed to honor. One read, one decision, carried on the command
// for the tap's whole life across generation bumps.
//
// Reading it at the tap rather than in doShow is deliberate for the same
// reason in time: doShow runs after a queue backlog and its COM calls, by which
// point a real mid-close tap can look late. The hide that stamps
// tkHideDeadlineNs (a blur or a user close) is an EARLIER UI/pane event than
// the re-tap, so its deadline is already published when the caller reads it.
func showPlatformTouchKeyboard(kbd *touchKeyboardState, hwndU uintptr, gen int64, closing bool) {
	tkTraceEvent("enqueue").Uint64("hwnd", uint64(hwndU)).Int64("gen", gen).Bool("closing", closing).Msg("touch keyboard: show queued for the service thread")
	tkEnqueue(tkCommand{kind: tkCmdShow, kbd: kbd, hwnd: windows.HWND(hwndU), gen: gen, closing: closing})
}

// platformKeyboardClosing reports whether a hide of the one process-wide touch
// keyboard has STARTED and is still animating shut. Read ONCE per tap by
// requestTouchKeyboard, which both lets the tap past its show debounce and
// hands the value back as showPlatformTouchKeyboard's closing argument.
//
// The debounce bypass needs it because dropping a duplicate show is only
// harmless while the keyboard the previous one opened is still up; mid-hide
// that premise is inverted — the pane is on its way out and nothing else will
// bring it back, so the debounced tap would leave its field bare.
//
// The deadline is the PROCESS-WIDE one, not any window's: the keyboard is a
// single shared window, so a hide started for the OTHER window (main losing
// focus) is still animating when this window's field is tapped.
//
// Both records of a started hide are consulted, the service thread's and the
// Hiding callbacks', because between them lies a gap in which only the second
// one knows (see tkPaneHandler.hide). The callback record is asked of every
// live registration rather than of one shared word — see tkAnyPaneHiding for
// why a shared word cannot survive two windows advising independently.
func platformKeyboardClosing() bool {
	return tkHideStillClosing(tkNowNs(), tkHideDeadlineNs.Load(), tkAnyPaneHiding())
}

// hidePlatformTouchKeyboard enqueues a hide for kbd's window. Non-blocking;
// the service drops it if a newer show request superseded gen.
func hidePlatformTouchKeyboard(kbd *touchKeyboardState, gen int64) {
	tkEnqueue(tkCommand{kind: tkCmdHide, kbd: kbd, gen: gen})
}

// platformReleaseKeyboardEvents marks kbd's window destroyed and enqueues
// its release (unregister events, drop ownership, invalidate in-flight
// show/hide generations). Non-blocking — safe from DestroyEvent.
func platformReleaseKeyboardEvents(kbd *touchKeyboardState) {
	kbd.released.Store(true)
	tkEnqueue(tkCommand{kind: tkCmdRelease, kbd: kbd})
}

// platformViewHWND extracts the native window handle from a Gio ViewEvent on
// Windows (0 for the invalid ViewEvent Gio sends on teardown). This is the
// per-window handle with no GetForegroundWindow ambiguity.
func platformViewHWND(e app.ViewEvent) uintptr {
	if v, ok := e.(app.Win32ViewEvent); ok {
		return v.HWND
	}
	return 0
}

// platformBindKeyboardWindow proactively binds kbd's native handle and
// registers its Showing/Hiding handler as soon as the window exists, so a
// keyboard the user opens BEFORE the first editor tap is still tracked.
// Non-blocking; a zero handle (ViewEvent teardown) is ignored.
func platformBindKeyboardWindow(kbd *touchKeyboardState, hwndU uintptr) {
	if hwndU == 0 || kbd.released.Load() {
		return
	}
	kbd.viewHwnd.Store(hwndU) // authoritative own-handle for the foreground-match guard
	tkEnqueue(tkCommand{kind: tkCmdAdvise, kbd: kbd, hwnd: windows.HWND(hwndU)})
}

// tkRoInit initializes the Windows Runtime on the current (locked) thread.
// Returns (usable, mustUninitialize, sta, hr). The failure HRESULT is RETURNED,
// not stashed in shared state: the service thread and every monitor
// goroutine each call this, so a package global would let one thread's
// failure hr be logged against another's (or read stale when the proc was
// simply absent). Each caller keeps its own hr in a local and logs that.
//
// The apartment is SINGLE-threaded, and that is load-bearing rather than a
// preference. Windows.UI.ViewManagement.InputPane is a single-threaded WinRT
// class, so RoGetActivationFactory for it from an MTA thread does not fail
// gracefully or degrade — it returns RO_E_UNSUPPORTED_FROM_MTA and the input
// pane is simply unreachable for the life of that thread. This code asked for
// the multi-threaded apartment for years, which is why every one of the show
// path's careful retries and fallbacks was operating downstream of a call that
// never once got off the ground: a tap logged its way through the whole ladder
// and put no keyboard on screen.
//
// The sta return says whether we actually got it. RPC_E_CHANGED_MODE means the
// thread was already initialized the other way and cannot be changed while it
// runs, so it is usable — the legacy host needs no apartment in particular —
// but the WinRT path on that thread is dead and the caller should say so once
// rather than rediscover it per call.
func tkRoInit() (bool, bool, bool, uintptr) {
	const (
		roInitSingleThreaded = 0
		sFalse               = 1
		rpcEChangedMode      = 0x80010106
	)
	if tkProcRoInitialize.Find() != nil {
		return false, false, false, 0
	}
	hr, _, _ := tkProcRoInitialize.Call(roInitSingleThreaded)
	switch hr {
	case 0, sFalse:
		// Both S_OK and S_FALSE must be balanced with RoUninitialize.
		return true, true, true, 0
	case rpcEChangedMode:
		// Thread already initialized with a different model — usable, but
		// the init/uninit pair belongs to whoever set it up, and the
		// apartment is not the one the input pane requires.
		return true, false, false, 0
	default:
		// No log here: this runs inside retry loops (service batches,
		// monitor init) — the CALLERS log, rate-limited, with context.
		return false, false, false, hr
	}
}

// tkMsg mirrors the Win32 MSG structure, which x/sys/windows does not export.
// PeekMessageW writes all of it, so the layout has to be the right size and
// shape; the pump reads only message.
type tkMsg struct {
	hwnd     uintptr
	message  uint32
	wParam   uintptr
	lParam   uintptr
	time     uint32
	pt       struct{ x, y int32 }
	lPrivate uint32
}

// tkStaPumpBudget bounds one drain so a message storm cannot starve the command
// queue behind it. This apartment owns no user-facing window — everything
// arriving here is COM plumbing, and 256 in a row is already pathological.
const tkStaPumpBudget = 256

// tkStaPump dispatches whatever is waiting in this thread's message queue.
//
// It is the price of the single-threaded apartment, and not an optional part of
// it. COM delivers incoming calls to an STA through a hidden window, so an STA
// that never dispatches never receives the Showing/Hiding callbacks it
// registered for, and the shell thread raising them blocks until it gives up.
// The keyboard would open and this process would never be told — which is the
// same end state as the bug being fixed here, reached from the other side.
//
// TranslateMessage is deliberately absent: it only matters for keyboard input,
// and this thread has no window that could receive any.
// tkMarkKeyboardSeen records the ONE fact the tap coalesce in touch_input.go
// is allowed to be released by: a keyboard has been seen on screen for this
// window. Every call site below has evidence — a pane Showing event, a pane
// that occludes or reports a location, or the legacy host's own window.
//
// It exists as a named function rather than an inline visibleGen.Add(1)
// because the rule it carries is a rule about which FACTS may be counted, and
// that rule has already been broken once. The coalesce was originally released
// by sessionGen, which is bumped the moment TryShow returns true — and TryShow
// returning true is a statement about the request, not about the screen; the
// show path says so itself when it schedules a verification 700ms later on
// precisely that doubt. A show that was accepted and produced nothing
// therefore released the coalesce almost immediately, the next tap cancelled
// the pending verification, and the legacy fallback it would have started
// never ran. That is the starvation the coalesce was built to prevent,
// surviving inside the coalesce.
//
// So: do not call this anywhere a keyboard has not been SEEN. sessionGen still
// tracks accepted sessions and is still what OwnerExpire binds to — the two
// counters answer different questions and neither substitutes for the other.
func tkMarkKeyboardSeen(kbd *touchKeyboardState) {
	kbd.visibleGen.Add(1)
	// A keyboard is on screen, so whatever show was working towards one is
	// over — the successful ending, as against the terminal failures that call
	// finishShow. Stored rather than compared: unlike a failure, this is not a
	// claim about one generation. Some call sites here have no generation at
	// all (a pane Showing event arrives on its own), and every one of them is
	// reporting the same screen, which no pending show can be racing towards
	// differently. The bump above is what actually releases the coalesce; this
	// keeps the two pieces of state from disagreeing.
	kbd.showActiveGen.Store(0)
}

func tkStaPump() {
	if tkProcPeekMessageW.Find() != nil || tkProcDispatchMessageW.Find() != nil {
		return
	}
	const (
		pmRemove = 0x0001
		wmQuit   = 0x0012
	)
	var msg tkMsg
	for i := 0; i < tkStaPumpBudget; i++ {
		r, _, _ := tkProcPeekMessageW.Call(uintptr(unsafe.Pointer(&msg)), 0, 0, 0, pmRemove)
		if r == 0 || msg.message == wmQuit {
			return
		}
		tkProcDispatchMessageW.Call(uintptr(unsafe.Pointer(&msg)))
	}
}

// tkStaWait is the sleep this thread is not allowed to take.
//
// Everything the service thread waits for goes through here: it waits out d
// while continuing to dispatch, so a pane callback arriving mid-wait is still
// answered instead of being made to queue behind the wait that is looking for
// its result. tkPumpedWait holds the loop; this is only the Win32 end of it.
func tkStaWait(d time.Duration) {
	if tkProcMsgWait.Find() != nil || tkProcPeekMessageW.Find() != nil || tkProcDispatchMessageW.Find() != nil {
		// Either nothing to wait on or nothing to drain with. The second is
		// the dangerous one: an inert pump leaves the message in the queue,
		// the wait reports input available and returns at once, and the loop
		// becomes a spin. This apartment is already unserviceable, so take the
		// plain sleep and do not pretend otherwise.
		time.Sleep(d)
		return
	}
	const (
		qsAllInput         = 0x04FF
		mwmoInputAvailable = 0x0004
		// INFINITE minus one: a timeout is never allowed to round up into
		// "wait forever" on a thread whose caller expects it back.
		maxWaitMS = 0xFFFFFFFE
	)
	tkPumpedWait(d, time.Now, tkStaPump, func(timeout time.Duration) bool {
		// Rounded up, because a sub-millisecond remainder truncates to a zero
		// timeout, and a zero timeout polls where this is meant to wait.
		ms := (int64(timeout) + int64(time.Millisecond) - 1) / int64(time.Millisecond)
		if ms > maxWaitMS {
			ms = maxWaitMS
		}
		// nCount 0 with a nil handle array: wait on this thread's input alone.
		// MWMO_INPUTAVAILABLE counts what is already queued, which matters
		// because tkStaPump has a budget and may have left some behind.
		r, _, _ := tkProcMsgWait.Call(0, 0, uintptr(ms), qsAllInput, mwmoInputAvailable)
		return r != uintptr(windows.WAIT_FAILED)
	}, time.Sleep)
}

// tkAwaitCommands blocks until tkEnqueue has work for the service thread,
// dispatching messages the whole time it waits.
//
// A plain channel receive cannot do this. The apartment has to service its
// message queue, and a goroutine parked on a Go channel services nothing — so
// the wait is a Win32 one, on an event tkEnqueue signals, with QS_ALLINPUT so
// an incoming COM call wakes it just as a command does.
func tkAwaitCommands() {
	if tkCmdEvent == 0 || tkProcMsgWait.Find() != nil {
		// Nothing to wait on but the channel. The apartment then goes
		// unpumped, so the WinRT path will not work; the legacy host still
		// will, and a service that keeps running is what makes that possible.
		<-tkCmdKick
		return
	}
	const (
		qsAllInput         = 0x04FF
		mwmoInputAvailable = 0x0004
	)
	h := tkCmdEvent
	for {
		tkStaPump()
		// Checked BEFORE the wait, every time: tkEnqueue appends and only then
		// signals, so a command that landed while we were pumping is already in
		// the list and must not be slept through.
		tkCmdMu.Lock()
		pending := len(tkCmdList) > 0
		tkCmdMu.Unlock()
		if pending {
			return
		}
		r, _, _ := tkProcMsgWait.Call(1, uintptr(unsafe.Pointer(&h)), uintptr(windows.INFINITE), qsAllInput, mwmoInputAvailable)
		if r == uintptr(windows.WAIT_FAILED) {
			// Do not spin on a broken wait — that would burn a core for the
			// life of the process. Fall back to the channel for this round.
			<-tkCmdKick
			return
		}
		if r == uintptr(windows.WAIT_OBJECT_0) {
			return
		}
		// Otherwise the queue has messages: loop round and pump them.
	}
}

// tkOwnForegroundWindow returns the foreground window handle if it belongs
// to this process (i.e. it is our Gio window that was just tapped), or 0.
func tkOwnForegroundWindow() windows.HWND {
	hwnd := windows.GetForegroundWindow()
	if hwnd == 0 {
		return 0
	}
	var pid uint32
	if _, err := windows.GetWindowThreadProcessId(hwnd, &pid); err != nil {
		return 0
	}
	if pid != windows.GetCurrentProcessId() {
		return 0
	}
	return hwnd
}

type tkPaneStatus int

const (
	tkPaneShown tkPaneStatus = iota
	tkPaneAlreadyVisible
	tkPaneRefused
	tkPaneError       // WinRT failure that a retry may clear — ladder first
	tkPaneUnavailable // input pane unreachable from here — legacy path allowed
)

// tkInputPaneFactory obtains the IInputPaneInterop activation factory. The
// caller must Release the returned object when status is tkPaneShown.
// status is tkPaneShown (ok), tkPaneError, or tkPaneUnavailable. On failure
// hr and stage identify WHAT broke; the factory itself never logs — it is
// called from the monitor's polling loop, where per-call logs would bypass
// the callers' rate-limiting (user-action call sites log every failure,
// the monitor logs once per error burst).
func tkInputPaneFactory() (interop *tkInteropObj, status tkPaneStatus, hrOut uintptr, stage string) {
	if tkProcRoGetActivationFactory.Find() != nil ||
		tkProcWindowsCreateString.Find() != nil ||
		tkProcWindowsDeleteString.Find() != nil {
		return nil, tkPaneUnavailable, 0, "combase-procs"
	}
	name, err := windows.UTF16FromString("Windows.UI.ViewManagement.InputPane")
	if err != nil {
		return nil, tkPaneError, 0, "UTF16FromString"
	}
	var hstr uintptr
	hr, _, _ := tkProcWindowsCreateString.Call(
		uintptr(unsafe.Pointer(&name[0])),
		uintptr(len(name)-1), // exclude NUL terminator
		uintptr(unsafe.Pointer(&hstr)),
	)
	if hr != 0 {
		return nil, tkPaneError, hr, "WindowsCreateString"
	}
	defer tkProcWindowsDeleteString.Call(hstr)

	hr, _, _ = tkProcRoGetActivationFactory.Call(
		hstr,
		uintptr(unsafe.Pointer(&tkIidIInputPaneInterop)),
		uintptr(unsafe.Pointer(&interop)),
	)
	if hr != 0 || interop == nil {
		// tkPaneHRPermanent (in touch_input.go, where the platform-independent
		// test suite can reach it) holds the answers that a retry cannot
		// change: the class is not registered, the HWND interop predates this
		// Windows, or this thread is in the wrong apartment. Those go to the
		// legacy host now instead of onto a ladder that would spend fifteen
		// seconds re-asking a question already answered.
		if tkPaneHRPermanent(hr) {
			return nil, tkPaneUnavailable, hr, "RoGetActivationFactory"
		}
		return nil, tkPaneError, hr, "RoGetActivationFactory"
	}
	return interop, tkPaneShown, 0, ""
}

// tkOccSample is the typed result of one OccludedRect read: on failure it
// carries the failing stage and HRESULT so the monitor can log WHAT broke
// when it degrades to slow polling, instead of silently discarding both.
// width is the raw rect width, kept SEPARATELY from the occlusion height:
// a FLOATING keyboard reports a rect with zero occlusion height but a
// non-zero width, so width is the only OccludedRect evidence that a
// non-occluding keyboard is on screen at all.
type tkOccSample struct {
	height float32
	width  float32
	ok     bool
	hr     uintptr
	stage  string
}

// tkPaneOccludedHeight reads the input pane's OccludedRect height for hwnd.
func tkPaneOccludedHeight(interop *tkInteropObj, hwnd windows.HWND) tkOccSample {
	var pane *tkPaneObj
	hr, _, _ := syscall.SyscallN(interop.vtbl.GetForWindow,
		uintptr(unsafe.Pointer(interop)),
		uintptr(hwnd),
		uintptr(unsafe.Pointer(&tkIidIInputPane)),
		uintptr(unsafe.Pointer(&pane)),
	)
	if hr != 0 || pane == nil {
		return tkOccSample{hr: hr, stage: "GetForWindow"}
	}
	defer tkRelease(pane.vtbl.Release, unsafe.Pointer(pane))
	var rect tkRect
	hr, _, _ = syscall.SyscallN(pane.vtbl.GetOccludedRect,
		uintptr(unsafe.Pointer(pane)),
		uintptr(unsafe.Pointer(&rect)),
	)
	if hr != 0 {
		return tkOccSample{hr: hr, stage: "GetOccludedRect"}
	}
	// Per the OccludedRect contract, Top == 0 means the pane does not
	// occlude this window even when the keyboard is visible (floating /
	// non-overlapping layouts) — only Y > 0 with a height counts. The raw
	// width is carried either way as visibility evidence.
	if rect.Y <= 0 {
		return tkOccSample{width: rect.Width, ok: true}
	}
	return tkOccSample{height: rect.Height, width: rect.Width, ok: true}
}

// String names the status for logs — an integer here is unreadable in the
// one place these values matter, a trace captured on a device.
func (s tkPaneStatus) String() string {
	switch s {
	case tkPaneShown:
		return "shown"
	case tkPaneAlreadyVisible:
		return "already-visible"
	case tkPaneRefused:
		return "refused"
	case tkPaneError:
		return "error"
	case tkPaneUnavailable:
		return "unavailable"
	}
	return "unknown"
}

// tkPaneShow shows the input pane for hwnd via the WinRT interop and
// reports what actually happened. Visibility is judged by OccludedRect
// (InputPane.Visible is documented for Xbox only).
func tkPaneShow(hwnd windows.HWND, fwPane *tkFrameworkPaneObj) tkPaneStatus {
	interop, status, fhr, fstage := tkInputPaneFactory()
	if status != tkPaneShown {
		if status == tkPaneError {
			tkDiagEvent("pane").Uint64("hr", uint64(fhr)).Str("stage", fstage).Uint64("hwnd", uint64(hwnd)).Msg("touch keyboard: pane factory failed")
		}
		return status
	}
	defer tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))

	var pane2 *tkPane2Obj
	hr, _, _ := syscall.SyscallN(interop.vtbl.GetForWindow,
		uintptr(unsafe.Pointer(interop)),
		uintptr(hwnd),
		uintptr(unsafe.Pointer(&tkIidIInputPane2)),
		uintptr(unsafe.Pointer(&pane2)),
	)
	if hr != 0 || pane2 == nil {
		if tkPaneHRPermanent(hr) {
			return tkPaneUnavailable
		}
		tkDiagEvent("pane").Uint64("hr", uint64(hr)).Uint64("hwnd", uint64(hwnd)).Str("stage", "GetForWindow").Msg("touch keyboard: pane show failed")
		return tkPaneError
	}
	defer tkRelease(pane2.vtbl.Release, unsafe.Pointer(pane2))

	var shown int32
	hr, _, _ = syscall.SyscallN(pane2.vtbl.TryShow,
		uintptr(unsafe.Pointer(pane2)),
		uintptr(unsafe.Pointer(&shown)),
	)
	if hr != 0 {
		tkDiagEvent("pane").Uint64("hr", uint64(hr)).Uint64("hwnd", uint64(hwnd)).Str("stage", "TryShow").Msg("touch keyboard: pane show failed")
		return tkPaneError // call itself failed — transient, no fallback
	}
	if shown != 0 {
		return tkPaneShown
	}
	// TryShow returned false. If the pane already occludes the window it is
	// up (idempotent no-op). A FLOATING keyboard occludes nothing (zero
	// height) but reports a non-zero rect WIDTH — that too is a visible
	// keyboard, and must still take ownership/monitoring for this window;
	// treating it as a refusal would strand the session on the old owner
	// after a main→console switch. Only an all-zero rect is a refusal —
	// a FAILED probe proves nothing either way and must stay retryable
	// (tkPaneError), not become a terminal Refused.
	s := tkPaneOccludedHeight(interop, hwnd)
	if !s.ok {
		tkDiagEvent("pane").Uint64("hr", uint64(s.hr)).Str("stage", s.stage).Uint64("hwnd", uint64(hwnd)).Msg("touch keyboard: visibility probe failed after TryShow returned false")
		return tkPaneError
	}
	if s.height > 0 || s.width > 0 {
		return tkPaneAlreadyVisible
	}
	// All-zero OccludedRect for THIS window is not a refusal if the pane is
	// up but simply not over us — take it as already-visible so ownership and
	// the monitor still move to this window. An inconclusive Location (pane
	// not ready / transient error) is retryable, NOT a refusal.
	vis, ok := tkFwPaneLocationVisible(fwPane)
	if !ok {
		return tkPaneError
	}
	if vis {
		return tkPaneAlreadyVisible
	}
	return tkPaneRefused
}

// tkTryHide asks the pane for hwnd to hide. Returns true when the pane
// reported hiding.
func tkTryHide(hwnd windows.HWND) bool {
	interop, status, fhr, fstage := tkInputPaneFactory()
	if status != tkPaneShown {
		if status == tkPaneError {
			tkDiagEvent("pane").Uint64("hr", uint64(fhr)).Str("stage", fstage).Uint64("hwnd", uint64(hwnd)).Msg("touch keyboard: pane factory failed")
		}
		return false
	}
	defer tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))

	var pane2 *tkPane2Obj
	hr, _, _ := syscall.SyscallN(interop.vtbl.GetForWindow,
		uintptr(unsafe.Pointer(interop)),
		uintptr(hwnd),
		uintptr(unsafe.Pointer(&tkIidIInputPane2)),
		uintptr(unsafe.Pointer(&pane2)),
	)
	if hr != 0 || pane2 == nil {
		tkDiagEvent("pane").Uint64("hr", uint64(hr)).Uint64("hwnd", uint64(hwnd)).Str("stage", "GetForWindow").Msg("touch keyboard: pane hide failed")
		return false
	}
	defer tkRelease(pane2.vtbl.Release, unsafe.Pointer(pane2))

	var hidden int32
	hr, _, _ = syscall.SyscallN(pane2.vtbl.TryHide,
		uintptr(unsafe.Pointer(pane2)),
		uintptr(unsafe.Pointer(&hidden)),
	)
	if hr != 0 {
		tkDiagEvent("pane").Uint64("hr", uint64(hr)).Uint64("hwnd", uint64(hwnd)).Str("stage", "TryHide").Msg("touch keyboard: pane hide failed")
	}
	return hr == 0 && hidden != 0
}

// tkWin32Rect is a Win32 RECT (LONG left/top/right/bottom), as returned by
// IFrameworkInputPane::Location.
type tkWin32Rect struct {
	Left, Top, Right, Bottom int32
}

// tkFwPaneLocationVisible reports whether the input pane is showing right now,
// via IFrameworkInputPane::Location — the MODERN pane's own screen geometry.
// Unlike touchKeyboardVisible (which reads the legacy Win10 IPTip_Main_Window
// class and is unreliable for the current keyboard), this queries the same
// FrameworkInputPane object the Showing/Hiding events come from, so a non-empty
// rectangle is an authoritative "a WinRT keyboard is up" — even when it does
// not occlude our window. Any HRESULT error is treated as "not visible".
// Returns (visible, ok): ok is false when the call could not determine the
// state (no pane, or an HRESULT error), so a negative result is inconclusive
// and worth retrying.
func tkFwPaneLocationVisible(fwPane *tkFrameworkPaneObj) (bool, bool) {
	if fwPane == nil {
		return false, false
	}
	var rect tkWin32Rect
	hr, _, _ := syscall.SyscallN(fwPane.vtbl.Location,
		uintptr(unsafe.Pointer(fwPane)),
		uintptr(unsafe.Pointer(&rect)),
	)
	if hr != 0 {
		return false, false
	}
	return rect.Right > rect.Left && rect.Bottom > rect.Top, true
}

// ---- Pane Showing/Hiding events (IFrameworkInputPane) -------------------
//
// Ownership of the keyboard (shownByUs) must end exactly when the pane
// hides — zero occlusion is not a proxy (floating keyboards never occlude).
// IFrameworkInputPane is the documented desktop-app interface for these
// notifications: we register one handler per window HWND on a dedicated,
//process-lifetime service thread and clear ownership from the Hiding
// callback. A Showing callback (any show, including user-initiated ones)
// restarts the occlusion monitor so bottom padding also works for
// keyboards the user opened manually.

var (
	// CLSID {D5120AA3-46BA-44C5-822D-CA8092C1FC72} — FrameworkInputPane.
	tkClsidFrameworkInputPane = tkGUID{
		0xD5120AA3, 0x46BA, 0x44C5,
		[8]byte{0x82, 0x2D, 0xCA, 0x80, 0x92, 0xC1, 0xFC, 0x72},
	}
	// IID {5752238B-24F0-495A-82F1-2FD593056796} — IFrameworkInputPane.
	tkIidIFrameworkInputPane = tkGUID{
		0x5752238B, 0x24F0, 0x495A,
		[8]byte{0x82, 0xF1, 0x2F, 0xD5, 0x93, 0x05, 0x67, 0x96},
	}
	// IID {226C537B-1E76-4D9E-A760-33DB29922F18} — IFrameworkInputPaneHandler.
	tkIidIFrameworkInputPaneHandler = tkGUID{
		0x226C537B, 0x1E76, 0x4D9E,
		[8]byte{0xA7, 0x60, 0x33, 0xDB, 0x29, 0x92, 0x2F, 0x18},
	}
	// IID {00000000-0000-0000-C000-000000000046} — IUnknown.
	tkIidIUnknown = tkGUID{
		0x00000000, 0x0000, 0x0000,
		[8]byte{0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46},
	}
)

// tkFrameworkPaneVtbl is the IFrameworkInputPane vtable (IUnknown +
// Advise, AdviseWithHWND, Unadvise, Location).
type tkFrameworkPaneVtbl struct {
	QueryInterface uintptr
	AddRef         uintptr
	Release        uintptr
	Advise         uintptr
	AdviseWithHWND uintptr
	Unadvise       uintptr
	Location       uintptr
}

type tkFrameworkPaneObj struct{ vtbl *tkFrameworkPaneVtbl }

// tkPaneHandler is our COM object implementing IFrameworkInputPaneHandler.
// It is allocated once per advised window and kept alive in tkHandlers.
//
// The hide field is THIS REGISTRATION's record that a hide has started: a
// tkNowNs() deadline armed by its Hiding callback and retired by its Showing,
// both of which run on arbitrary RPC threads. It exists because
// tkHideDeadlineNs, the process-wide record, is written by the service thread
// and so says nothing until the command the callback enqueued has been
// processed. A show running inside that gap asks whether the pane is closing,
// is told no, adopts a pane already on its way out, and reports itself done,
// leaving a focused field whose keyboard then vanishes with nothing left to
// re-open it.
//
// PER REGISTRATION, and that is the whole point of it. Only within a single
// registration are the two events raised in a defined order at all, so only
// there may a Showing be trusted to retire nothing newer than itself. RAISED in
// order — not executed in it: this is an in-proc MTA object, COM serializes no
// call on one, and both callbacks can be running at once on two RPC threads.
// What tkHandlerMu adds is that their EFFECTS cannot interleave.
// The main window and the console window advise separately; on one word shared
// between them a late Showing from either would erase a hide the other had
// genuinely begun — the very misfire this record exists to prevent, and worst
// where it is hardest to see, a console whose Unadvise failed and whose handler
// therefore stays pinned and live. The process-wide question is asked instead
// by tkAnyPaneHiding, which folds over the registrations that are still live
// and over the record of the hides whose registration is not.
//
// Self-expires after tkHideAnimWindow like tkHideDeadlineNs, so a hide whose
// Showing never arrives cannot leave it stuck on; and a slot still live when its
// registration is dropped is folded into tkOrphanHidingNs by tkForgetHandler
// first, so retiring a window does not retire the hide that window saw. Into
// that word and not into tkHideDeadlineNs, which any window's show may zero:
// what is being handed on is precisely a hide that no other window was ever
// entitled to retire.
//
// Written ONLY under tkHandlerMu, by tkStampHiding and tkRetireHiding, in the
// same critical section that resolves the handler AND that the rest of the
// callback's work runs in — the pane-event generation and the enqueue of the
// command carrying it included. Unadvise deletes the handler under that lock as
// well, so there are exactly two orders and both are safe: the stamp lands
// before the delete and tkForgetHandler carries it into tkOrphanHidingNs, or the
// handler is already gone when the callback arrives and tkStampHiding writes
// that same word directly. A resolution that let go of the lock before storing
// would admit a third order, in which the hide reaches neither.
type tkPaneHandler struct {
	vtbl *tkPaneHandlerVtbl
	kbd  *touchKeyboardState
	hwnd windows.HWND
	hide atomic.Int64
}

type tkPaneHandlerVtbl struct {
	QueryInterface uintptr
	AddRef         uintptr
	Release        uintptr
	Showing        uintptr
	Hiding         uintptr
}

var (
	tkHandlerVtbl tkPaneHandlerVtbl
	tkHandlerOnce sync.Once

	// tkHandlerByPtr resolves the COM `this` pointer back to the Go
	// handler without a uintptr→unsafe.Pointer cast, and pins handlers
	// for the GC while their SYSTEM registration is live (a handler is
	// only removed after a successful Unadvise — never while Windows may
	// still call it).
	tkHandlerMu    sync.Mutex
	tkHandlerByPtr = map[uintptr]*tkPaneHandler{}

	// tkOrphanHidingNs holds a hide of the one shared pane that no live
	// registration can account for: a Hiding raised for a registration that
	// is already out of tkHandlerByPtr, and the still-live stamp of one on
	// its way out of it (tkForgetHandler). Those are the same event seen
	// from either side of the delete, and both end up here. Unadvise stops
	// FUTURE notifications; it does not wait for a callback that has already
	// begun, and the API does not promise it will. So a delivery can arrive
	// after the slot it belongs to is gone — and the command such a callback
	// would enqueue is dropped too, once the keyboard it names is released,
	// which is exactly what the window being unadvised is doing. Without this
	// word that hide of the one shared pane is held nowhere, and the next
	// window's show reads a settled keyboard and adopts it.
	//
	// ARMED ONLY, never cleared, and the asymmetry is the point: this word IS
	// shared by every window, and a Showing that could zero it would be able
	// to erase a hide some other window began — the defect the per-
	// registration slot exists to prevent. Nothing retires it but its own
	// expiry, tkHideAnimWindow after it was written. The cost is at most one
	// animation window of reading a settled pane as closing, and that error
	// arms a re-show poll rather than dropping a keyboard.
	//
	// Read and written only under tkHandlerMu, which is what orders it against
	// the delete that produced the orphan in the first place.
	tkOrphanHidingNs atomic.Int64
)

// tkStampHiding resolves the COM `this` pointer to its handler, records on it
// that a hide has begun, and runs then — all inside ONE hold of tkHandlerMu.
// That is the whole reason this is a function rather than a lookup followed by a
// store: between a lookup that has released the lock and the store that follows
// it, the service thread can take the lock, read the slot it is about to be
// given (finding the zero still there), fold nothing, and delete the handler.
// The store then lands in an object no fold will visit again, and nothing else
// holds that hide — the command this callback goes on to enqueue is dropped once
// the keyboard it names is released, which is precisely what the window being
// unadvised is doing.
//
// then runs under the lock, and this returns NOTHING, so the caller is left with
// no handler to work with after the section ends. The stamp, the pane-event
// generation and the enqueue of the command carrying it are one indivisible
// step, and they have to be: COM serializes no call on an MTA object, this
// handler is in-proc with no marshaling, and the Showing and Hiding of one
// registration can therefore be executing AT THE SAME TIME on two RPC threads.
// Split apart, a Hiding takes its generation from the gap between a Showing's
// retire and that Showing's own Add, and queues its command ahead of it: the
// Hiding then reads as stale against the newer shownGen and is dropped, or it is
// honored and the Showing behind it republishes paneVisible for a keyboard that
// is already gone. The lock does not recover the order the events were raised in
// — nothing in this process can — but whichever callback takes it first, that
// one's generation and its position in the queue agree, which is the only thing
// the service thread's staleness test needs to mean what it says.
//
// What that leaves, and where it is answered. A generation agreeing with a queue
// position is not agreement with the SCREEN. When the raise order is inverted —
// COM may run two methods of an MTA object at once and promises nothing about
// which reaches a user lock first — both callbacks are still individually
// consistent, and the service simply ends on the verdict of whichever was
// applied last, which is the one raised first. No arithmetic downstream can
// undo it: an inverted Hiding carries exactly the generation a legitimately
// earlier Hiding carries. So nothing tries to re-derive the order. tkCmdPaneTruth
// settles the disagreement against the pane itself, once it has stopped moving.
//
// Where the registration is already gone, then does not run. The hide is no less
// real for it — the pane is one, shared by every window — so it is handed to
// tkOrphanHidingNs rather than dropped, and there is simply no window left to do
// anything further for.
//
// Takes tkHandlerMu, which is safe only because nothing here makes a COM call
// and so nothing here pumps the apartment: a pump under this lock would admit
// an incoming callback that re-enters this same lock on this same thread, and
// sync.Mutex is not reentrant. That now covers then as well — everything the
// callback does happens inside this section. tkEnqueue qualifies: a slice append
// under tkCmdMu and a SetEvent, no dispatch anywhere in it, and tkHandlerMu →
// tkCmdMu is the only nesting of those two in the file.
func tkStampHiding(this uintptr, then func(h *tkPaneHandler)) {
	deadline := tkNowNs() + int64(tkHideAnimWindow)
	tkHandlerMu.Lock()
	defer tkHandlerMu.Unlock()
	h := tkHandlerByPtr[this]
	if h == nil {
		if deadline > tkOrphanHidingNs.Load() {
			tkOrphanHidingNs.Store(deadline)
		}
		return
	}
	h.hide.Store(deadline)
	then(h)
}

// tkRetireHiding resolves `this`, clears that registration's hide record, and
// runs then, under one hold of tkHandlerMu for the same reasons tkStampHiding
// is: a store issued after the handler has left the map writes to nothing, and a
// generation taken after the lock is given back is ordered against nothing. The
// pane is UP, so no hide is animating any more, and retiring the record HERE
// rather than in the marshaled handler keeps that fact as timely as the one it
// retires — left to the command queue it would lag by exactly the gap the record
// exists to cover, in the direction that reads a settled keyboard as a closing
// one.
//
// THIS registration and no other; and where there is no handler to retire,
// nothing is retired and then does not run. A Showing outranks only the events
// of its own registration, so it may not reach tkOrphanHidingNs, where a hide
// another window began may be sitting. Returns nothing, and keeps the no-pump
// rule, both as tkStampHiding does and for the same reasons.
func tkRetireHiding(this uintptr, then func(h *tkPaneHandler)) {
	tkHandlerMu.Lock()
	defer tkHandlerMu.Unlock()
	h := tkHandlerByPtr[this]
	if h == nil {
		return
	}
	h.hide.Store(0)
	then(h)
}

// tkAnyPaneHiding reports the latest un-retired Hiding stamp the process holds
// anywhere — over every live registration, and over the orphan word that keeps
// a hide whose registration is gone — or 0 if there is none. The latest is the
// right fold, not an approximation of one: a stamp is live exactly while now is
// below it, so the largest is live whenever any of them is.
//
// Same no-pump rule as tkStampHiding, for the same lock and the same reason.
func tkAnyPaneHiding() int64 {
	tkHandlerMu.Lock()
	defer tkHandlerMu.Unlock()
	latest := tkOrphanHidingNs.Load()
	for _, h := range tkHandlerByPtr {
		if d := h.hide.Load(); d > latest {
			latest = d
		}
	}
	return latest
}

// tkForgetHandler drops a registration from tkHandlerByPtr and hands on the hide
// it may still be holding. EVERY removal goes through here — there is no other
// delete — because a slot discarded with a live stamp in it is a close of the
// one shared keyboard that then exists nowhere at all: the command that stamp's
// callback enqueued is thrown away as soon as the keyboard it names is released,
// which is exactly what a window being unadvised is doing.
//
// The stamp goes to tkOrphanHidingNs and NOT to tkHideDeadlineNs. What is handed
// on is a hide that only its own registration's Showing was ever entitled to
// retire, and that registration is the one going away; tkHideDeadlineNs is a
// record every window's events reach and a show zeroes. Parking it there would
// let an unrelated window cancel a close it never saw — the same defect, one
// indirection further along, that moved this record off a shared word to begin
// with. The orphan word is only ever armed, so nothing but time can cancel it,
// and it is already where a Hiding arriving AFTER this delete is put: both
// orders of that race then end in the same place.
//
// Keeping the later of the two never shortens a deadline already standing, and a
// retired (zero) slot arms nothing. Same no-pump rule as tkStampHiding: no COM
// call may appear here, or an incoming callback would re-enter this same
// non-reentrant lock on this same thread.
func tkForgetHandler(hPtr uintptr) {
	tkHandlerMu.Lock()
	defer tkHandlerMu.Unlock()
	if h := tkHandlerByPtr[hPtr]; h != nil {
		if d := h.hide.Load(); d > tkOrphanHidingNs.Load() {
			tkOrphanHidingNs.Store(d)
		}
	}
	delete(tkHandlerByPtr, hPtr)
}

// tkInitHandlerVtbl builds the shared vtable with Go callbacks. Pointer-
// typed callback parameters keep the code vet-clean: syscall.NewCallback
// accepts any uintptr-sized argument types. AddRef/Release are no-op
// refcounts — handler lifetime is managed by the advise/unadvise pair.
func tkInitHandlerVtbl() {
	tkHandlerVtbl = tkPaneHandlerVtbl{
		QueryInterface: syscall.NewCallback(func(this uintptr, riid *tkGUID, ppv *uintptr) uintptr {
			if ppv == nil {
				return 0x80004003 // E_POINTER
			}
			if riid != nil && (*riid == tkIidIUnknown || *riid == tkIidIFrameworkInputPaneHandler) {
				*ppv = this
				return 0
			}
			*ppv = 0
			return 0x80004002 // E_NOINTERFACE
		}),
		AddRef:  syscall.NewCallback(func(this uintptr) uintptr { return 2 }),
		Release: syscall.NewCallback(func(this uintptr) uintptr { return 1 }),
		// These callbacks arrive on an arbitrary shell/RPC thread (our handler
		// is an in-proc MTA object with no marshaling), so they must NOT touch
		// shared state directly — doing so raced tkCmdPublish/OwnerExpire on
		// the service thread (a Hiding slipping between a publish's epoch check
		// and its store would resurrect stale padding after hide). Instead each
		// just ENQUEUES a command; all state mutation happens on the one
		// service goroutine, FIFO-ordered with every other command, which is
		// the serialization guarantee the rest of this file relies on. Being
		// in-proc also means COM serializes nothing BETWEEN them, so each does
		// its whole job inside the hold of tkHandlerMu that resolved its
		// handler — see tkStampHiding.
		Showing: syscall.NewCallback(func(this, prc, fEnsure uintptr) uintptr {
			// Resolving, retiring and reporting are ONE step (see
			// tkRetireHiding): the record lives on the handler, a store issued
			// after the handler has left the map is a store into nothing, and a
			// generation this callback hands to the queue after giving the lock
			// back is ordered against nothing. Where the registration is already
			// gone the body does not run at all — a Showing has nothing to do
			// for a window that no longer exists.
			tkRetireHiding(this, func(h *tkPaneHandler) {
				// Assign the pane-event generation HERE, in the callback, so it
				// follows the events and not the queue. A Showing bumps the
				// sequence, the matching Hiding (below) reads it. Doing this in
				// the callback — not the marshaled handler — is what lets a
				// Hiding that fires right after its Showing carry the SAME
				// generation even if the Showing command has not been processed
				// yet (processing lag would otherwise stamp the Hiding with a
				// stale value and the real hide would be dropped).
				g := h.kbd.paneEventGen.Add(1)
				// Count INDEPENDENT user pane appearances so a pending
				// compensation can abort rather than close a hand-opened keyboard.
				// Skip our own committed shows (shownByUs) and the very pane a
				// compensation is cleaning up (expectOwnPaneShow, claimed once and
				// only while unexpired):
				// only a genuine taskbar/keyboard-button open advances the seq.
				// tkClaimSynthShow comes BEFORE tkClaimOwnPaneShow and is not a
				// mere optimization: if reconcilePaneVisible already judged this
				// very pane, it has already consumed (or correctly declined to
				// consume) the own-pane mark, and re-running that predicate here
				// would find the mark gone and read OUR pane as the user's. It
				// is handed g — the stamp this callback just produced — so only
				// the one Showing reconcile was actually racing can match.
				if !h.kbd.shownByUs.Load() && !tkClaimSynthShow(h.kbd, g) && !tkClaimOwnPaneShow(h.kbd) {
					tkPaneShowSeq.Add(1)
				}
				// Inside the section too, and that is the point of the round
				// that put it here: the number above and this command's place in
				// the queue are one fact, and a Hiding that got between them
				// would carry a generation the service thread sees in the other
				// order.
				tkEnqueue(tkCommand{kind: tkCmdShowingEvent, kbd: h.kbd, hwnd: h.hwnd, gen: g})
			})
			return 0
		}),
		Hiding: syscall.NewCallback(func(this, fEnsure uintptr) uintptr {
			// The stamp comes WITH the resolution (see tkStampHiding) and the
			// command goes on the queue before that section ends, so neither can
			// fall behind anything else in this callback and neither can be
			// separated from it by an unadvise: a hide has begun, and until the
			// command is processed that stamp is the only place that says so. A
			// show already running on the service thread reads it in
			// tkHideStillClosing and arms its re-show poll; without it that show
			// sees an AlreadyVisible pane, adopts it, finishes, and only then
			// lets the Hiding through. Where the registration is gone the body
			// does not run: the hide has been recorded process-wide, and there
			// is no window left to enqueue a command for.
			tkStampHiding(this, func(h *tkPaneHandler) {
				// The pane reconciliation may have classified on THIS
				// registration is ending, so drop its dedupe mark now rather than
				// waiting out the deadline (see tkDropSynthShowMark).
				tkDropSynthShowMark(h.kbd)
				// Read the sequence its Showing set (event order): this Hiding
				// belongs to the most recent Showing on this window. A later
				// window/session that has not yet raised its own Showing has not
				// advanced paneEventGen, so a genuinely stale Hiding still carries
				// an older number and the handler drops it; the CURRENT session's
				// Hiding always matches and is honored. Read and enqueued under
				// the lock the Showing callback also takes, so no Showing can
				// slip between the two and leave this command behind a generation
				// it never saw.
				tkEnqueue(tkCommand{kind: tkCmdHidingEvent, kbd: h.kbd, gen: h.kbd.paneEventGen.Load()})
			})
			return 0
		}),
	}
}

// tkPaneReg is the service-local record of a live registration.
type tkPaneReg struct {
	hPtr            uintptr
	cookie          uint32
	unadviseRetries int
}

// tkUnadviseRetryMax bounds the FAST retries of a failing Unadvise; past
// it retries continue at tkUnadviseSlowRetry pace, without a final limit —
// giving up entirely would pin the handler, and through its state pointer the
// whole window, forever.
const (
	tkUnadviseRetryMax   = 5
	tkUnadviseSlowRetry  = time.Minute
	tkFwPaneRetryBackoff = 5 * time.Second
	// A pending hide whose window is not foreground (a native file picker
	// opened from an attachment tap owns it) waits for the window to return
	// rather than dropping the hide. It NEVER abandons — a dialog can stay
	// open arbitrarily long — but polls fast only briefly, then slowly: the
	// first tkHideForegroundFastWaits attempts are tkHideForegroundWaitDelay
	// apart (snappy return), the rest are tkHideForegroundSlowWait apart. The
	// wait self-cancels via hideGen / shownByUs / released the moment it is
	// moot.
	tkHideForegroundWaitDelay = 400 * time.Millisecond
	tkHideForegroundFastWaits = 12 // ~5s of fast polling before slowing down
	tkHideForegroundSlowWait  = 2 * time.Second
	// A hide whose TryHide/Toggle keeps failing retries fast a few times, then
	// slowly forever — never abandoning an app-opened keyboard. Self-cancels
	// via doHide's released / hideGen / shownByUs guards.
	tkHideRetryFast = 6 // ~3s of 500ms retries before slowing down
	tkHideRetrySlow = 2 * time.Second
	// A re-tap that adopts a pane still animating closed (AlreadyVisible while a
	// modern hide is in flight) polls a fresh show until the pane finishes
	// closing and TryShow can re-open it. Bounded: once these elapse the pane is
	// stably up (the tap got what it wanted), so the poll stops and adopts.
	tkReshowPollDelay = 120 * time.Millisecond
	tkReshowPollMax   = 5
	// How many consecutive occlusion samples (250ms apart) must contradict
	// paneVisible before the monitor reports the disagreement. One sample proves
	// nothing: a pane appears a little before the command recording it is
	// applied, and that ordinary gap must not be read as the callback race.
	// Four is ~1s — well past any queue latency, well inside the ~3s zero streak
	// the opposite direction already waits out.
	tkPaneTruthStreak = 4
	// How long after a hide STARTS (our TryHide, or a system/manual Hiding) the
	// pane is treated as still closing, so a re-tap adopting it triggers a
	// re-show. Generous on purpose: the re-tap's doShow only reaches the check
	// after its own 150ms settle + COM calls, so a tap made early in the close
	// is still processed inside this window. A physical bound on the close, NOT
	// a stable-vs-hiding heuristic — armed only by an actual hide, so a re-tap
	// onto a keyboard nobody is hiding never sees it; and a completed hide with
	// no re-tap leaves nothing to adopt, so a stale deadline can't misfire.
	tkHideAnimWindow = time.Second
	// Compensation (tkCmdCompensateHide) cleans up a keyboard opened by a show
	// that lost a cancel race — unlike a user-requested hide it is BOUNDED, so
	// a released window cannot leave a retry chain running forever. ~6 fast
	// (400ms) + the rest slow (2s) ≈ 16s total before giving up.
	tkCompensateMaxRetries = 12
	// How long after a TryShow the pane that call accepted may still raise the
	// Showing belonging to it (touchKeyboardState.expectOwnPaneShow). Sized to
	// the PANE's animation, not to our retry budget: a pane that call produced is
	// on screen within a few hundred ms, whereas a user who sees nothing happen
	// must first notice the absence and then reach the taskbar keyboard button —
	// they cannot beat this window, and the moment right after a failed show is
	// exactly when they try. A budget-sized window merged those two openers;
	// this one separates them. Past it the mark is dropped and NOT claimed: a
	// late own pane then costs an orphan keyboard the user can close, while
	// claiming the user's would cost them the keyboard they just asked for.
	tkOwnPaneShowWindow = 800 * time.Millisecond
	// Reconciliation dedupe (tkSynthMark): how long a pane classified by
	// reconcilePaneVisible stays recognizable to a Showing callback still in
	// flight for the SAME pane. Bounds only the delivery lag of an event the
	// system had ALREADY raised when our probe saw the pane — the handler is an
	// in-proc MTA object, so this is thread scheduling on a loaded tablet, not
	// marshaling. It is the OUTER bound: the mark is normally dropped far
	// earlier, by the Hiding that ends that pane. What it really guards against
	// is the case where NO Showing was in flight — then the named generation is
	// simply never produced, and without an expiry the user's next hand-opened
	// pane would eventually stamp it and be swallowed.
	tkSynthShowWindow = 800 * time.Millisecond
	// Legacy compensation only: attempts allowed with NO keyboard ever sighted
	// before we stop crediting one to our own Toggle (~6s: 6 fast + 2 slow).
	tkCompensateLegacyMax = 8
	// Post-advise visibility reconciliation: retry only when the probe was
	// inconclusive (a transient COM error), so a missed Showing for an
	// up-but-not-occluding keyboard is still caught. Retries continue with a
	// growing backoff (capped) until a definitive result, release, or an
	// HWND change — Windows will NOT resend the missed Showing, so giving up
	// after a fixed count would strand an open keyboard on a longer outage.
	tkReconcileRetryDelay = time.Second
	tkReconcileMaxBackoff = 5 * time.Second
	// How long after a TryShow that REPORTED success we check back to see
	// whether a keyboard actually appeared. Long enough for the pane's open
	// animation to have put something on screen (the same order as
	// tkOwnPaneShowWindow, which bounds that pane's own Showing), short enough
	// that a user who tapped a field is not left staring at nothing.
	tkShowVerifyDelay = 700 * time.Millisecond
	// The legacy host answers a Toggle with an HRESULT, which reports that the
	// call was ACCEPTED and says nothing at all about the screen. These bound
	// the wait for actual evidence: long enough to cover TabTip's own start and
	// open animation, short enough that a tap does not hold the show path for a
	// noticeable time before the escalation below gets its turn.
	tkLegacyVerifyDelay = 150 * time.Millisecond
	tkLegacyVerifyPolls = 8
	// The recovery pass after (re)starting TabTip.exe. The settle count is how
	// long the freshly started host is given to register its COM server before
	// it is asked for anything: a Toggle fired into a process that is still
	// starting is accepted by nobody and wastes the one attempt this pass gets.
	tkLegacyHostPolls       = 10
	tkLegacyHostSettlePolls = 3
)

// tkKeyboardService is the single owner of all platform keyboard work. It
// runs forever on one locked thread whose WinRT apartment is never
// uninitialized (it hosts the FrameworkInputPane proxy and our callback
// objects). Commands are processed strictly in order; every command
// re-validates its preconditions here, immediately before its COM calls.
func tkKeyboardService() {
	runtime.LockOSThread()

	// COM/WinRT initialization is RETRYABLE: a transient failure at app
	// start must not permanently disable the keyboard (the original
	// symptom this whole layer exists to fix). ensureCOM re-attempts
	// RoInitialize before each command batch until it succeeds; the
	// FrameworkInputPane factory likewise retries, throttled, so a shell
	// hiccup at startup doesn't permanently silence Showing/Hiding events.
	roReady := false
	roMTAWarned := false
	roLastHR := uintptr(0) // service-thread-local; safe to read in doShow's log
	ensureCOM := func() bool {
		if roReady {
			return true
		}
		ok, _, sta, hr := tkRoInit() // success path never uninitializes — apartment hosts our proxies
		roReady = ok
		if !ok {
			roLastHR = hr
		}
		if ok && !sta && !roMTAWarned {
			// RPC_E_CHANGED_MODE: something initialized this thread into the
			// multi-threaded apartment before we got here. InputPane is a
			// single-threaded WinRT class, so from now on every activation on
			// this thread answers RO_E_UNSUPPORTED_FROM_MTA — a running
			// thread's apartment cannot be changed, so this is settled for the
			// life of the process. Warn level, once: the shows still work, via
			// the legacy host, but a trace that shows only the HRESULT gives
			// nobody a way to find out why.
			roMTAWarned = true
			log.Warn().Msg("touch keyboard: service thread is in the multi-threaded apartment; InputPane cannot be activated from it and every show will use the legacy host")
		}
		return ok
	}

	var fwPane *tkFrameworkPaneObj
	var fwLastAttempt time.Time
	// ensureFwPane (re)creates the FrameworkInputPane factory, throttled.
	// It reports the nil→ready TRANSITION: the caller must then advise the
	// already-live windows, because a window whose show ran while the pane
	// was down would otherwise finish its whole session without a Hiding
	// callback (ensureAdvised is normally only reached inside a show).
	ensureFwPane := func() bool {
		if fwPane != nil || !ensureCOM() {
			return false
		}
		if !fwLastAttempt.IsZero() && time.Since(fwLastAttempt) < tkFwPaneRetryBackoff {
			return false
		}
		recovering := !fwLastAttempt.IsZero()
		fwLastAttempt = time.Now()
		hr, _, _ := tkProcCoCreateInstance.Call(
			uintptr(unsafe.Pointer(&tkClsidFrameworkInputPane)),
			0,
			0x1, // CLSCTX_INPROC_SERVER — shell in-proc object
			uintptr(unsafe.Pointer(&tkIidIFrameworkInputPane)),
			uintptr(unsafe.Pointer(&fwPane)),
		)
		if hr != 0 {
			fwPane = nil
			log.Debug().Uint64("hr", uint64(hr)).Msg("touch keyboard: FrameworkInputPane unavailable; will retry (occlusion-based ownership meanwhile)")
			return false
		}
		if recovering {
			log.Debug().Msg("touch keyboard: FrameworkInputPane recovered")
		}
		tkHandlerOnce.Do(tkInitHandlerVtbl)
		return true
	}

	regs := map[*touchKeyboardState]*tkPaneReg{}

	// known is the process-wide set of LIVE window keyboard states —
	// maintained by registerKeyboardState (adds live, removes released)
	// and consumed by transferKeyboardOwnership. Both are pure functions
	// in touch_input.go so the ownership rules are covered by the
	// platform-independent test suite.
	known := map[*touchKeyboardState]bool{}

	// globalShowEpoch counts keyboard sessions committed by ANY window (all
	// doShow success branches bump it). A compensating hide captures it at the
	// moment its cancelled show was scheduled; if it has advanced by the time
	// the compensation runs, some window has since shown a keyboard — and the
	// legacy Toggle is GLOBAL, so firing it would close THAT session's keyboard
	// (e.g. the console's cancelled show must not toggle off a keyboard the
	// main window has since opened). The compensation aborts on any advance.
	// Service-thread-local: doShow, the compensate enqueue sites, and the
	// handler all run on this one goroutine, so no atomic is needed.
	globalShowEpoch := int64(0)

	// scheduleFwRetry arms a single deferred wake so a TRANSIENT
	// FrameworkInputPane failure is retried even when no further user command
	// arrives. ensureFwPane is throttled and only runs at a batch top, so
	// without this a keyboard opened via the taskbar before the first tap —
	// whose ViewEvent advise failed because the pane wasn't up yet — would
	// stay unregistered forever (no command, no event, no batch). It fires
	// only while the pane is down AND a live window still needs advising, and
	// dedupes on fwRetryPending so at most one timer is outstanding.
	fwRetryPending := false
	scheduleFwRetry := func() {
		// Arm whenever a live window still needs advising — the cause may be a
		// down FrameworkInputPane OR an AdviseWithHWND that FAILED while the
		// pane was up (the window stays !eventsBound either way). Do NOT
		// short-circuit on fwPane!=nil: that skipped the advise-retry case and
		// left a window whose registration failed permanently untracked.
		if fwRetryPending || len(advisableKeyboardStates(known)) == 0 {
			return
		}
		fwRetryPending = true
		time.AfterFunc(tkFwPaneRetryBackoff, func() { tkEnqueue(tkCommand{kind: tkCmdFwRetry}) })
	}

	// valid reports whether a show/hide-style command is still what the
	// user wants, right before acting on it.
	valid := func(kbd *touchKeyboardState, hwnd windows.HWND, gen int64) bool {
		return !kbd.released.Load() &&
			kbd.showGen.Load() == gen &&
			tkOwnForegroundWindow() == hwnd
	}

	// reconcilePaneVisible checks whether a keyboard is up for kbd's window
	// right now and, if so, synthesizes the Showing that was missed before a
	// (retried) advise succeeded — mark it visible, claim a pane generation,
	// count it as an independent user open unless it is demonstrably ours, and
	// start the monitor. "Missed" is verified, not assumed: a real Showing
	// delivered while this ran wins the generation, and then nothing is
	// synthesized at all.
	//
	// gen0 is the caller's paneEventGen baseline — the value before any real
	// Showing could have been delivered for this reconciliation. It is a
	// PARAMETER, not a first-line read, because only the caller knows where
	// that instant is: it is captured before the handler is pinned (the callback
	// resolves `this` through tkHandlerByPtr, so a Showing can fire while
	// AdviseWithHWND is still running), which is strictly earlier than anything
	// this function could read for itself. The SAME value is carried through the
	// tkCmdReconcile retry chain, because a retry cannot re-read it either: the
	// callback advances paneEventGen from an RPC thread at delivery time, so a
	// Showing still queued behind the retry has already moved the counter, and a
	// fresh read would call that delivered event "missed". Same rule as doShow's
	// seq0: a baseline belongs ahead of everything that can block, and a
	// baseline taken too early costs nothing — the CAS below simply fails and
	// this synthesizes nothing, which is what a delivered event deserves.
	// Probe in order of certainty: OccludedRect>0 (WinRT occluding us) →
	// IFrameworkInputPane::Location (modern pane up but not over us) →
	// touchKeyboardVisible (legacy IPTip, no Hiding). expectHiding is set from
	// which probe fired. Returns found, and uncertain (a probe errored, so a
	// negative is inconclusive and worth a retry).
	reconcilePaneVisible := func(kbd *touchKeyboardState, hwnd windows.HWND, gen0 int64) (found, uncertain bool) {
		winrt := false
		switch interop, status, _, _ := tkInputPaneFactory(); status {
		case tkPaneShown:
			s := tkPaneOccludedHeight(interop, hwnd)
			tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))
			switch {
			case !s.ok:
				uncertain = true
			case s.height > 0 || s.width > 0:
				found, winrt = true, true
			}
		case tkPaneError:
			// A TRANSIENT factory failure (combase/activation hiccup) is NOT
			// evidence that the pane is down, so a negative from the probes
			// below must stay inconclusive. Without this the caller's
			// `!found && uncertain` never fires and reconciliation ends for
			// good — a Showing missed before a retried advise succeeded is then
			// never synthesized, leaving an open keyboard with no occlusion
			// monitor and no bottom padding for the rest of the session. The
			// window need not even be foreground for this to bite: the two
			// probes below are foreground-gated, so they cannot supply the
			// uncertainty in their place.
			//
			// tkPaneUnavailable deliberately stays certain here even for the
			// apartment refusal, which the two visibility readers in this file
			// now treat as unknown (see tkPaneHRApartment). The asymmetry is
			// the cost, not an oversight: the refusal is a property of the
			// calling thread and does not go away, so an uncertain verdict here
			// would re-enqueue this reconcile for the life of the process,
			// while what it buys is one synthesized Showing on a path that
			// under MTA shows through the legacy host anyway. The two readers
			// that did change decide whether a keyboard gets closed and whether
			// a live session is torn down; this one decides only whether a pane
			// nobody announced acquires a monitor.
			uncertain = true
		}
		// Location and touchKeyboardVisible are GLOBAL (not per-HWND): a
		// keyboard open in ANOTHER app would falsely look like ours and make
		// this background window wait for a Hiding that never comes for its
		// HWND. Trust them only while OUR window is foreground; the per-HWND
		// OccludedRect above needs no such guard.
		foreground := tkOwnForegroundWindow() == hwnd
		if !found && foreground {
			if vis, ok := tkFwPaneLocationVisible(fwPane); !ok {
				uncertain = true
			} else if vis {
				found, winrt = true, true
			}
		}
		if !found && foreground && touchKeyboardVisible() {
			found, winrt = true, false
		}
		if found {
			// This pane is a Showing that was never DELIVERED — nothing was
			// advised when it fired — so everything the callback would have
			// done has to happen here too, and that includes the compensation
			// guard, not just the local bookkeeping below. Without the bump a
			// compensation scheduled before this pane appeared still reads its
			// starting epoch, concludes nothing has changed since, and hides a
			// keyboard the user opened by hand during exactly the interval we
			// were deaf in. The deaf interval is also the WORST one to be blind
			// in: a failing advise is precisely when the user gives up waiting
			// and opens the keyboard themselves.
			//
			// The predicate is the callback's, character for character, rather
			// than an unconditional bump: two places deciding "was this an
			// INDEPENDENT user open" must not be able to disagree, and a pane
			// that is ours (committed via shownByUs, or still carrying an
			// unexpired own-pane mark) is not one. Where the mark was never set
			// because the advise had failed, the answer comes out "the user's"
			// and a pending compensation aborts — that is the direction this
			// code chooses everywhere: a stranded keyboard the user can close
			// beats one taken away from them.
			//
			// Bumped for the legacy sighting too, not only the WinRT one. The
			// legacy COMPENSATION cannot read this counter (it raises no
			// Showing and is documented blind), but a MODERN compensation can
			// be pending while this probe is the thing that found the pane, and
			// nothing about a legacy sighting makes that keyboard more ours.
			// A WinRT pane confirmed here WILL raise a Hiding, so it needs a
			// pane-event generation the Hiding can match against. Claim one with
			// a CAS rather than a bare Add: the CAS is what makes "has a real
			// Showing been delivered since we started" a SINGLE atomic decision.
			// A Load-then-Add would leave the gap between them open, and a
			// Showing landing in it is the whole failure this guards:
			//
			//   callback bumps paneEventGen to N and enqueues its command;
			//   we then Add → N+1 and Store shownGen = N+1;
			//   the queued command runs and Stores shownGen = N — BACKWARDS;
			//   the Hiding callback reads paneEventGen = N+1, its handler
			//   compares against shownGen = N, drops it as stale, and
			//   paneVisible / ownership / bottom padding survive a keyboard
			//   that has already closed.
			//
			// A failed CAS is therefore not an error: it means the Showing was
			// DELIVERED, not missed, and tkCmdShowingEvent — already queued —
			// does strictly more than this branch would (it also bumps
			// sessionGen and arms expectHiding). Synthesizing nothing is
			// the correct answer, and it is why the seq bump below defaults to
			// this same flag: the callback has already run that predicate once,
			// and running it again here would double-count a user open, or
			// worse, find the own-pane mark already consumed by the callback
			// and so count OUR pane as the user's — aborting the very
			// compensation that pane needs. (The one case where a losing CAS
			// still owes a bump is spelled out at classify, below.)
			// Arm the dedupe mark BEFORE the CAS, and retract it if the CAS
			// fails. Arming AFTER would leave the interval between the two
			// instructions unguarded, and that interval is precisely the one
			// this mark exists for: a Showing delivered there stamps gen0+2,
			// finds no mark, classifies its own pane, and then this path
			// classifies it a second time — the r63 defect, merely narrowed.
			// Armed first, every interleaving is covered, because a Showing can
			// only carry gen0+2 once paneEventGen has already left gen0, which
			// is exactly when the CAS below fails:
			//
			//   Showing lands before the Store  → stamps gen0+1, no match, it
			//     classifies; our CAS fails, we retract and classify nothing.
			//   Showing lands after the Store, before the CAS → same stamp,
			//     same outcome.
			//   Showing lands after a WINNING CAS → stamps gen0+2, claims the
			//     mark, skips; we classify. Once, either way.
			//
			// Only the WinRT path arms: a legacy pane raises no Showing at all,
			// so there is no late callback to dedupe, and a mark left lying
			// there could only swallow an unrelated one.
			//
			// gen0+2 is the stamp the correlated callback will carry: a winning
			// CAS moves paneEventGen from gen0 to gen0+1, and the Showing's own
			// Add(1) yields the next. Computed from the CALLER's baseline rather
			// than re-read, because a fresh Load could already contain that Add
			// and would then name a generation nobody will ever produce, leaving
			// the mark to expire against a later pane.
			var mark *tkSynthMark
			synth := true
			if winrt {
				mark = &tkSynthMark{
					kbd:        kbd,
					gen:        gen0 + 2,
					deadlineNs: tkNowNs() + int64(tkSynthShowWindow),
				}
				tkSynthShowMark.Store(mark)
				synth = kbd.paneEventGen.CompareAndSwap(gen0, gen0+1)
				if synth {
					kbd.shownGen.Store(gen0 + 1)
					// Remember that THIS session was adopted, not observed. The
					// CAS proves no Showing was delivered since gen0; it proves
					// nothing about a HIDING, which only Loads the counter and
					// never advances it — so no CAS can see one in flight. If
					// the pane started closing between the probe above and the
					// CAS, its Hiding callback already read gen0 and enqueued
					// that number, and the handler's exact-match rule would drop
					// it as stale against the gen0+1 just stored. Nothing else
					// would ever clear the session: the pane opened before we
					// were listening, so no further Showing is coming, and
					// expectHiding (set below) stops the occlusion monitor from
					// expiring on its own. paneVisible, ownership and the
					// monitor would survive the keyboard forever, and a later
					// hide would close whatever keyboard the user had open by
					// then — the unrecoverable direction.
					//
					// So record the adoption and let the handler accept gen0 as
					// well for exactly this session. The ambiguity is only ever
					// between two adjacent numbers: a Hiding raised before the
					// CAS carries gen0, one raised after carries gen0+1, and
					// the alternate rule retires the moment a real Showing
					// pushes shownGen past this value.
					kbd.adoptedGen.Store(gen0 + 1)
				}
			}
			// classify reports whether THIS path still owes the pane the single
			// classification it must receive. Normally that is just synth. The
			// exception is the one interleaving where a losing CAS is not the
			// whole story: if the retraction below fails, the mark was CLAIMED
			// between the Store and here, so some Showing skipped its own
			// classification on the strength of a mark we were about to
			// withdraw. Nobody else will judge that pane, so we must — otherwise
			// a hand-opened keyboard goes uncounted and a pending compensation
			// closes it, which is the one failure direction that is not
			// recoverable (see tkSynthMark).
			classify := synth
			if winrt && !synth && !tkSynthShowMark.CompareAndSwap(mark, nil) {
				classify = true
			}
			if classify && !kbd.shownByUs.Load() && !tkClaimOwnPaneShow(kbd) {
				tkPaneShowSeq.Add(1)
			}
			kbd.paneVisible.Store(true)
			// found is true, which in this function means the pane was OBSERVED
			// — an occluding or sized rect, or one of the legacy probes. That is
			// seen-evidence by the same standard as everywhere else, and this is
			// the path that matters most for it: reconciliation runs precisely
			// when the Showing event never arrived, so on a device where the
			// events are not delivered this is the ONLY place a keyboard is ever
			// discovered. Without the mark, that keyboard is on screen and being
			// typed on while the tap coalesce still holds the field for its full
			// window.
			tkMarkKeyboardSeen(kbd)
			kbd.expectHiding.Store(winrt)
			kbd.occlusionEpoch.Add(1)
			tkEnsureOcclusionMonitor(kbd, hwnd)
		}
		return found, uncertain
	}

	// ensureAdvised registers the Showing/Hiding handler for kbd before a
	// show. Synchronous — same thread, no handshakes. Failure is
	// definitive (fallback ownership); only a genuinely absent
	// FrameworkInputPane skips registration entirely.
	ensureAdvised := func(kbd *touchKeyboardState, hwnd windows.HWND) {
		if fwPane == nil || kbd.eventsBound.Load() {
			return
		}
		if _, dup := regs[kbd]; dup {
			return
		}
		// Pane-event baseline for the reconciliation below, taken BEFORE the
		// handler becomes reachable. One line later it would already be too
		// late: pinning is what makes `this` resolvable, so from the next
		// instruction a Showing can fire on an RPC thread and bump
		// paneEventGen — and a bump folded into the baseline is a delivered
		// event that reconcile would then duplicate.
		gen0 := kbd.paneEventGen.Load()

		h := &tkPaneHandler{vtbl: &tkHandlerVtbl, kbd: kbd, hwnd: hwnd}
		hPtr := uintptr(unsafe.Pointer(h))
		tkHandlerMu.Lock()
		tkHandlerByPtr[hPtr] = h // pin + make `this` resolvable in callbacks
		tkHandlerMu.Unlock()

		var cookie uint32
		hr, _, _ := syscall.SyscallN(fwPane.vtbl.AdviseWithHWND,
			uintptr(unsafe.Pointer(fwPane)),
			uintptr(hwnd),
			hPtr,
			uintptr(unsafe.Pointer(&cookie)),
		)
		if hr == 0 {
			regs[kbd] = &tkPaneReg{hPtr: hPtr, cookie: cookie}
			kbd.eventsBound.Store(true)
			// A Showing that fired BEFORE this (possibly retried) registration
			// was missed — the shell won't resend it. Reconcile now; if the
			// probe was inconclusive (a transient COM error, not a clean "no
			// keyboard"), retry a few times, decoupled from advise retry.
			if found, uncertain := reconcilePaneVisible(kbd, hwnd, gen0); !found && uncertain {
				time.AfterFunc(tkReconcileRetryDelay, func() {
					tkEnqueue(tkCommand{kind: tkCmdReconcile, kbd: kbd, hwnd: hwnd, gen0: gen0})
				})
			}
		} else {
			tkForgetHandler(hPtr)
			log.Debug().Uint64("hr", uint64(hr)).Msg("touch keyboard: AdviseWithHWND failed")
		}
	}

	// adviseAll (re)registers every live, still-unadvised window. Called ONLY
	// on the fwPane nil→ready transition and from the throttled tkCmdFwRetry
	// timer — NOT on every batch: an active monitor publishes every 250ms, so
	// a per-batch advise of a window whose AdviseWithHWND keeps failing would
	// fire the COM call ~4×/s, defeating tkFwPaneRetryBackoff. ensureAdvised
	// is idempotent and advised windows drop out of advisableKeyboardStates.
	adviseAll := func() {
		if fwPane == nil {
			return
		}
		for _, kbd := range advisableKeyboardStates(known) {
			ensureAdvised(kbd, windows.HWND(kbd.hwnd.Load()))
		}
	}

	// unadvise removes kbd's registration. The handler and eventsBound are
	// only dropped on a successful HRESULT: while the SYSTEM registration
	// is live, releasing our only reference could hand a later callback
	// freed memory. Failures retry with backoff, capped.
	unadvise := func(kbd *touchKeyboardState) {
		reg, okReg := regs[kbd]
		if !okReg {
			return
		}
		hr, _, _ := syscall.SyscallN(fwPane.vtbl.Unadvise,
			uintptr(unsafe.Pointer(fwPane)),
			uintptr(reg.cookie),
		)
		if hr != 0 {
			reg.unadviseRetries++
			log.Warn().Uint64("hr", uint64(hr)).Int("retry", reg.unadviseRetries).Msg("touch keyboard: Unadvise failed")
			delay := time.Second
			if reg.unadviseRetries > tkUnadviseRetryMax {
				// Never give up entirely: a permanently pinned handler
				// holds the whole window through its state pointer.
				delay = tkUnadviseSlowRetry
			}
			kbdRetry := kbd
			time.AfterFunc(delay, func() {
				tkEnqueue(tkCommand{kind: tkCmdUnadviseRetry, kbd: kbdRetry})
			})
			return
		}
		delete(regs, kbd)
		// A hide this registration saw can outlive the registration itself,
		// and its slot is the only record of it until the enqueued command
		// runs — which, for the window being torn down here, is never, because
		// that command is dropped the moment its keyboard reads as released.
		// tkForgetHandler hands the stamp on before the handler goes; without
		// it, unadvising a window silently cancels a close still animating on
		// the one shared keyboard.
		tkForgetHandler(reg.hPtr)
		kbd.eventsBound.Store(false)
	}

	// commitLegacyShow records a keyboard the legacy host has actually put on
	// screen FOR US: ownership, the legacy flag (the hide must Toggle too,
	// because IInputPane2 does not exist here), a fresh session and occlusion
	// tracking. Every call site has already seen evidence of a keyboard.
	commitLegacyShow := func(cmd tkCommand) {
		// Clear any stale owner first: a legacy show is OUR show.
		transferKeyboardOwnership(known, cmd.kbd)
		cmd.kbd.legacyShow.Store(true)
		cmd.kbd.expectHiding.Store(false) // legacy host raises no Hiding
		cmd.kbd.shownByUs.Store(true)
		cmd.kbd.paneVisible.Store(true)
		cmd.kbd.sessionGen.Add(1)
		// Per the doc on tkMarkKeyboardSeen: every call site here has seen one.
		tkMarkKeyboardSeen(cmd.kbd)
		globalShowEpoch++ // a committed session; invalidates pending compensations
		cmd.kbd.occlusionEpoch.Add(1)
		tkEnsureOcclusionMonitor(cmd.kbd, cmd.hwnd)
	}

	// legacyKeyboardUp answers "is a touch keyboard on screen right now", and
	// says whether it actually KNOWS. Both halves matter here, in opposite
	// directions: a false negative makes us Toggle a non-idempotent host and
	// hide the keyboard the user is typing on, while treating "cannot tell" as
	// "nothing there" would abandon ownership, occlusion tracking and blur-hide
	// for a keyboard that is up. So an unanswerable probe makes the verdict
	// inconclusive rather than negative — the rule doVerifyShow already follows.
	legacyKeyboardUp := func(hwnd windows.HWND) (up, conclusive bool) {
		if touchKeyboardVisible() {
			return true, true
		}
		// The legacy host window class is not the whole story: on Windows 11
		// the touch keyboard is frequently NOT an IPTip_Main_Window, so its
		// absence alone is not evidence. Ask the WinRT probes too — unless
		// there is no WinRT pane on this build at all, in which case the legacy
		// host IS the keyboard here and its absence is a real answer.
		interop, status, hr, _ := tkInputPaneFactory()
		switch status {
		case tkPaneUnavailable:
			// Unavailable is two different facts wearing one status (see
			// tkPaneHRApartment). Only "the class is absent" makes the legacy
			// probe authoritative; the apartment refusal leaves this thread
			// unable to see a pane that is there, and answering "no keyboard"
			// from it is what lets tkMayRetoggle fire the second Toggle that
			// closes the keyboard the first one raised.
			if tkPaneHRApartment(hr) {
				return false, false
			}
			return false, true
		case tkPaneShown:
		default:
			return false, false // transient failure — no answer either way
		}
		occ := tkPaneOccludedHeight(interop, hwnd)
		tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))
		if !occ.ok {
			return false, false
		}
		if occ.height > 0 || occ.width > 0 {
			// A floating keyboard occludes nothing but still reports a width,
			// exactly as in tkPaneShow.
			return true, true
		}
		vis, ok := tkFwPaneLocationVisible(fwPane)
		if !ok {
			return false, false
		}
		return vis, true
	}

	// legacyShow drives the legacy touch-keyboard host: start TabTip.exe when
	// its out-of-process COM server is not running, then ITipInvocation.Toggle.
	// Every attempt is re-validated, because Toggle is not idempotent and
	// targets the CURRENT foreground window — a window switch between the COM
	// calls above and the Toggle must cancel the show.
	//
	// Three of the four callers have the same precondition, whatever their
	// diagnosis: each has already established that NO keyboard is on screen —
	// tkPaneUnavailable (the WinRT pane cannot be reached from this process),
	// tkPaneRefused (it can, it declined, and both visibility probes answered
	// "not visible" on the way to that verdict) and doVerifyShow (TryShow was
	// accepted but four independent probes agree nothing appeared). That is the
	// condition under which a GLOBAL, non-idempotent Toggle is plainly safe.
	//
	// The fourth — an exhausted tkPaneError ladder — cannot say that, and the
	// difference is worth being honest about: all it knows is that the pane
	// calls kept failing for some fifteen seconds. It is admitted anyway
	// because the alternative was measured on a device and it is a tap that
	// produces nothing at all, permanently. The exposure is narrower than it
	// looks: the first thing the loop below does is ask legacyKeyboardUp, whose
	// FIRST probe is the window-class one, which needs no WinRT at all and so
	// still answers when the pane path is the thing that is broken. A keyboard
	// the user is actually typing on is very likely to be seen there.
	//
	// Either way the loop re-checks the precondition before every one of its
	// own attempts.
	//
	// seq0 is the caller's pane-appearance baseline, carried into any
	// compensation this raises.
	legacyShow := func(cmd tkCommand, seq0 int64) {
		cmd.kbd.expectOwnPaneShow.Store(0)
		if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		if up, _ := legacyKeyboardUp(cmd.hwnd); up {
			// Visible already. If ANOTHER of our windows owns this
			// keyboard, ownership (with its legacy flag) follows the
			// user here — same rule as the AlreadyVisible pane branch;
			// otherwise it is the user's keyboard and we only track
			// occlusion.
			//
			// An INCONCLUSIVE verdict deliberately falls through to the
			// Toggle below, which is what this branch did before it consulted
			// anything but the window class: never Toggling on an unanswered
			// probe would strand the tap.
			transferKeyboardOwnership(known, cmd.kbd)
			cmd.kbd.expectHiding.Store(false) // legacy host raises no Hiding
			cmd.kbd.paneVisible.Store(true)
			cmd.kbd.sessionGen.Add(1)
			// legacyKeyboardUp reports up only when it KNOWS: seen evidence.
			tkMarkKeyboardSeen(cmd.kbd)
			globalShowEpoch++ // a committed session; invalidates pending compensations
			cmd.kbd.occlusionEpoch.Add(1)
			tkEnsureOcclusionMonitor(cmd.kbd, cmd.hwnd)
			return
		}
		// One Toggle, at most, per host instance. It is not idempotent, so a
		// second one fired while the first is still resolving would close the
		// keyboard the first is opening. hostToggled records that the host
		// running RIGHT NOW has one outstanding.
		hostToggled := tkInvokeToggle()
		tkTraceEvent("legacy").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Bool("accepted", hostToggled).Msg("touch keyboard: legacy Toggle attempted")
		if hostToggled {
			// Re-validate after the (non-idempotent) Toggle: if the show
			// was cancelled during it, hand it to the retryable
			// compensation to toggle the keyboard back off.
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				cmd.kbd.finishShow(cmd.gen)
				tkEnqueue(tkCommand{kind: tkCmdCompensateHide, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: globalShowEpoch, epoch: seq0, legacy: true})
				return
			}
			// An accepted Toggle is NOT a shown keyboard. The HRESULT reports
			// that the host took the call; committing on it alone declares
			// success for a tap that produced nothing, and the damage outlives
			// the tap: paneVisible drives the layout, so the UI reserves room
			// for a keyboard that is not there, and the session it opens
			// debounces the user's next tap away. Wait for evidence.
			plan := tkLegacyVerdictPlan(tkAwaitVisible(tkLegacyVerifyPolls, tkLegacyVerifyDelay, tkStaWait,
				func() bool { return valid(cmd.kbd, cmd.hwnd, cmd.gen) },
				func() (bool, bool) { return legacyKeyboardUp(cmd.hwnd) },
			))
			switch {
			case plan.compensate:
				cmd.kbd.finishShow(cmd.gen)
				tkEnqueue(tkCommand{kind: tkCmdCompensateHide, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: globalShowEpoch, epoch: seq0, legacy: true})
				return
			case plan.commit:
				commitLegacyShow(cmd)
				return
			}
			// Neither. Whatever the probes did or did not see, no keyboard has
			// been observed, so nothing is recorded as shown — the escalation
			// below is the whole point of getting here, and a commit would end
			// the attempt instead of continuing it.
			tkDiagEvent("legacy").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg(plan.escalateNote)
		}
		if !tkStartTabTip() {
			// The end of the road for this tap, and the fastest way to reach it:
			// on a machine with no TabTip.exe this return can happen within a few
			// hundred milliseconds of the tap. Every path out of here used to
			// leave lastShow where the dispatch put it, so the next second, two
			// seconds, five seconds of tapping were answered with "an earlier
			// show is still working on it" about a show that had already run out
			// of places to try. Say it is over.
			cmd.kbd.finishShow(cmd.gen)
			tkDiagEvent("legacy").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg("touch keyboard: could not start the legacy host")
			return
		}
		// A host has just been launched, so the Toggle budget starts over: the
		// outstanding one (if any) was accepted by the host that was running
		// BEFORE this launch, and that host is the reason we are here. Carrying
		// its budget forward is what made this loop a pure observer — it
		// watched a newly started host that had never been asked for anything,
		// so unless the launch alone raised a keyboard the tap was guaranteed
		// to end in the warning at the bottom of this function.
		//
		// The retry is still rationed. One Toggle, not before the host has had
		// tkLegacyHostSettlePolls to come up, and — while an earlier Toggle is
		// outstanding — only on a probe that conclusively reports an empty
		// screen, since toggling blind could close a keyboard that first Toggle
		// is raising. With nothing outstanding there is no such keyboard to
		// lose, and the caller's precondition (an empty screen) stands.
		retoggled := false
		for i := 0; i < tkLegacyHostPolls; i++ {
			tkStaWait(tkLegacyVerifyDelay)
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				cmd.kbd.finishShow(cmd.gen)
				// Cancelled while waiting for TabTip: it may already have
				// shown the keyboard, so compensate (retryable) rather than
				// leaving an ownerless keyboard up.
				tkEnqueue(tkCommand{kind: tkCmdCompensateHide, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: globalShowEpoch, epoch: seq0, legacy: true})
				return
			}
			up, conclusive := legacyKeyboardUp(cmd.hwnd)
			if up {
				// WE just started TabTip.exe — a keyboard appearing
				// now is our doing even without an explicit Toggle:
				// mark ownership so blur-hide can close it, exactly
				// like the Toggle-success branch.
				commitLegacyShow(cmd)
				return
			}
			if !tkMayRetoggle(i, tkLegacyHostSettlePolls, retoggled, hostToggled, conclusive) {
				continue
			}
			if !tkInvokeToggle() {
				continue
			}
			retoggled = true
			tkTraceEvent("legacy").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg("touch keyboard: legacy Toggle accepted after starting the host")
			// Deliberately no commit here: the next iteration's visibility
			// check is this Toggle's verification, for the same reason as
			// above. Only the cancellation check is urgent enough to run now.
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				cmd.kbd.finishShow(cmd.gen)
				tkEnqueue(tkCommand{kind: tkCmdCompensateHide, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: globalShowEpoch, epoch: seq0, legacy: true})
				return
			}
		}
		// Falling out of the poll loop is the last ending this show has: every
		// invocation path is spent and nothing further is scheduled for this
		// generation. It is also the slowest one to arrive, which is exactly why
		// it must still be said out loud — by now the coalesce has been holding
		// the field for most of its window and the user has been tapping into it.
		cmd.kbd.finishShow(cmd.gen)
		tkDiagEvent("legacy").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Bool("toggled", hostToggled).Bool("retoggled", retoggled).Msg("touch keyboard: legacy host produced no keyboard")
		log.Warn().Uint64("hwnd", uint64(cmd.hwnd)).Bool("toggled", hostToggled).Bool("retoggled", retoggled).Msg("touch keyboard: all invocation paths failed, no keyboard on screen")
	}

	// doVerifyShow answers the one question TryShow cannot: did a keyboard
	// actually appear? It runs tkShowVerifyDelay after a show this service
	// committed, and exists because "request accepted" and "keyboard on screen"
	// are different claims — the documented hardware-keyboard suppression makes
	// the gap between them a normal outcome rather than a fault.
	//
	// Acting on the answer means firing the GLOBAL, non-idempotent legacy
	// Toggle, so the verdict must be conclusive, not merely unconvincing. Four
	// independent negatives are required, and every one of them must be an
	// ANSWER: a probe that fails proves nothing and cancels the fallback, since
	// the cost of being wrong is toggling off a keyboard the user is typing on.
	//
	//  1. the pane factory works (an unavailable/erroring factory is not
	//     evidence about the screen),
	//  2. OccludedRect for our window succeeded and returned an all-zero rect
	//     (a FLOATING keyboard occludes nothing but still reports a width, so
	//     width counts as visible just as it does in tkPaneShow),
	//  3. IFrameworkInputPane::Location succeeded and reported not visible,
	//  4. the legacy host window is not on screen.
	//
	// Ownership state is left alone rather than torn down first: every success
	// path in legacyShow re-commits it (ownership, legacyShow flag, session,
	// occlusion monitor), and if the fallback fails outright we are no worse off
	// than the optimistic commit already left us.
	//
	// Every return in here that is not the fallback is the END of this show, and
	// each one is an ending of the awkward kind: the verification could not be
	// carried out, so no keyboard was seen AND no fallback was started. Nothing
	// remains scheduled for this generation. The coalesce must be told, or a
	// probe that merely failed to read costs the user the rest of the window.
	doVerifyShow := func(cmd tkCommand) {
		if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		// Only a WinRT show of OUR OWN is verified. A session we do not own is
		// not ours to re-open, and one that already went through the legacy host
		// has nothing left to fall back to.
		if !cmd.kbd.shownByUs.Load() || cmd.kbd.legacyShow.Load() {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		interop, status, _, _ := tkInputPaneFactory()
		if status != tkPaneShown {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		occ := tkPaneOccludedHeight(interop, cmd.hwnd)
		tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))
		// From here the three probes are read one at a time rather than as one
		// boolean, because "the probe failed" and "the probe says a keyboard is
		// there" lead to opposite places. Both stop the fallback, but only the
		// second is evidence, and only evidence may release the tap coalesce.
		// Folded together, a failed probe would count as a keyboard and hand the
		// field back while nothing is on screen.
		if !occ.ok {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		if occ.height > 0 || occ.width > 0 {
			tkMarkKeyboardSeen(cmd.kbd)
			return
		}
		vis, ok := tkFwPaneLocationVisible(fwPane)
		if !ok {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		if vis {
			tkMarkKeyboardSeen(cmd.kbd)
			return
		}
		if touchKeyboardVisible() {
			tkMarkKeyboardSeen(cmd.kbd)
			return
		}
		tkDiagEvent("verify").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg("touch keyboard: accepted show produced no keyboard; falling back to the legacy host")
		legacyShow(cmd, cmd.epoch)
	}

	doShow := func(cmd tkCommand) {
		// Baseline for "has the user opened a keyboard BY HAND since this
		// attempt began" — taken on the FIRST line, before anything that can
		// block, and carried to every compensation this doShow may enqueue
		// below. The baseline's whole job is to make a hand-open that happens
		// while we work visible to the compensation, so it must precede every
		// interval in which the user can reach the keyboard: the COM-init
		// ladder, the 150ms settle sleep, ensureAdvised, and the platform call
		// itself. Captured any later, a bump landing in the skipped interval is
		// folded into the baseline — and a compensation cannot notice an
		// advance it started from, so it would go on to hide the very keyboard
		// that bump represents. Earlier rounds moved it in from enqueue time to
		// after ensureAdvised; that still left the settle sleep, the single
		// longest exposed stretch, on the wrong side of it.
		//
		// Being TOO early is free: the extra span before the own-pane mark
		// exists is unattributable anyway, so any Showing in it bumps the seq
		// and aborts the compensation. That is the SAFE way to be wrong — a
		// stranded keyboard the user can close, rather than one taken away from
		// them. And a doShow that dies in the ladder below enqueues no
		// compensation at all, so its baseline simply goes unused.
		seq0 := tkPaneShowSeq.Load()

		if !ensureCOM() {
			// Do NOT drop the command: losing it here reproduces the
			// original bug on a transient failure (first tap shows no
			// keyboard). Re-enqueue the SAME command — but only while it
			// still reflects the user's intent: without the valid() check a
			// CANCELLED command (new tap bumped the generation, window
			// switched, window released) would keep riding the retry ladder
			// for its full ~15s instead of dying at the next hop.
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				cmd.kbd.finishShow(cmd.gen)
				return
			}
			// A requeue is NOT an ending: the same generation rides the ladder,
			// so the mark stays and the coalesce goes on covering it. That is
			// the "retries preserve activity" half of the rule, and it needs no
			// code — only the discipline of not clearing here.
			if delay, again := keyboardShowRetryDelay(cmd.retries); again {
				re := cmd
				re.retries++
				time.AfterFunc(delay, func() { tkEnqueue(re) })
				tkDiagEvent("show").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Int8("retry", re.retries).Uint64("hr", uint64(roLastHR)).Msg("touch keyboard: COM init failing; show requeued")
			} else {
				cmd.kbd.finishShow(cmd.gen)
				log.Warn().Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Uint64("hr", uint64(roLastHR)).Msg("touch keyboard: COM init failing; show abandoned after retries")
			}
			return
		}
		// Let the originating tap finish (focus/foreground settle) before
		// touching the pane. Waiting here also serializes bursts.
		tkStaWait(150 * time.Millisecond)
		if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
			cmd.kbd.finishShow(cmd.gen)
			return
		}
		// Register events BEFORE TryShow so an immediate user close can't
		// slip its Hiding event past us. Same thread — no wait, no timeout.
		ensureAdvised(cmd.kbd, cmd.hwnd)
		if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
			cmd.kbd.finishShow(cmd.gen)
			return
		}

		// Mark, BEFORE TryShow, that the next pane Showing on this window is
		// ours. TryShow only guarantees the request is ACCEPTED, not that its
		// Showing callback can't already be firing on another thread during the
		// call — setting the flag afterwards would race that. If this show is
		// then cancelled, the pane's Showing is not miscounted as an independent
		// user open (tkPaneShowSeq) that would abort the compensation. Only when
		// advised (else no Showing arrives). Every non-tkPaneShown branch below
		// clears it (they open no new uncommitted pane); a committed tkPaneShown
		// leaves it for endKeyboardSession; a cancelled one for the compensation.
		if cmd.kbd.eventsBound.Load() {
			cmd.kbd.expectOwnPaneShow.Store(tkNowNs() + int64(tkOwnPaneShowWindow))
		}

		// reshow drives the re-show poll for a tap that met a pane a hide is
		// still closing. It returns true when the CALLER must return (stop
		// processing this command); false when the caller should fall through to
		// adopt the visible pane. Two distinct cases, one call site per branch:
		//
		//   • cmd.reshow (this IS a poll): keep polling until the pane finishes
		//     closing (→ tkPaneShown re-opens it) or the bound elapses. It must
		//     survive BOTH tkPaneAlreadyVisible (still occluding) and tkPaneRefused
		//     (pane vanished mid-probe) — a poll that dies on Refused loses the
		//     re-tap it was created to honor. cmd.polls is a SEPARATE ladder from
		//     cmd.retries: a poll iteration must not spend the COM/transient-error
		//     budget, or one flaky COM call after five polls would abandon a show
		//     that still deserves its full retry ladder. While polls remain, return
		//     true (do not adopt this transient frame); once exhausted, false so the
		//     now-stable pane is adopted.
		//
		//   • !cmd.reshow (first adoption during a hide): arm ONE poll for THIS
		//     generation, then return false so the caller still adopts now — the
		//     pane is up, which the tap wanted; the poll only guards against it
		//     finishing its close. cmd.closing was captured at the tap, so the tap
		//     moment is represented — but it is ORed with a fresh read, never
		//     replaced by one. A hide can also START while this very command runs:
		//     the apartment is pumped throughout, so the Hiding is DELIVERED here
		//     and only its callback stamp is available before the command it
		//     enqueued gets its turn. The other direction is why this is an OR and
		//     not an assignment: the fresh read can have expired since the tap, and
		//     a tap admitted PRECISELY BECAUSE the keyboard was closing must not
		//     then be dispatched with no poll. The deadline is NOT consumed here:
		//     a later re-tap (which bumps showGen and kills THIS gen's poll at its
		//     next valid() check) must be able to arm ITS OWN poll — a single-shot
		//     consume would leave that newer tap unable to replace the poll it just
		//     invalidated, and if the pane were still closing the keyboard would be
		//     lost. Redundant trains self-prune: a superseded gen's poll dies at
		//     valid() within one tkReshowPollDelay. tkHideDeadlineNs is cleared
		//     only by a show the service thread commits itself (tkPaneShown), or
		//     by its own expiry, so it stays a level flag any live tap can read.
		//     NOT gated on eventsBound: the poll drives TryShow directly, needs
		//     no pane events.
		reshow := func() bool {
			if cmd.reshow {
				if cmd.polls < tkReshowPollMax {
					again := cmd
					again.polls++
					time.AfterFunc(tkReshowPollDelay, func() { tkEnqueue(again) })
					return true
				}
				return false // poll bound reached; adopt the stable pane
			}
			if (cmd.closing || platformKeyboardClosing()) && valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				tkEnqueue(tkCommand{kind: tkCmdShow, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: cmd.gen, reshow: true})
			}
			return false
		}

		status := tkPaneShow(cmd.hwnd, fwPane)
		tkTraceEvent("pane").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Str("status", status.String()).Msg("touch keyboard: InputPane show attempt finished")
		switch status {
		case tkPaneShown:
			// Re-validate AFTER TryShow: a blur, outside-tap, or console
			// close during the call bumps showGen / sets released, and since
			// shownByUs was still false then, no blur-hide was queued to
			// close this. We just opened a keyboard nobody wants — hand it to
			// the RETRYABLE compensation (a single TryHide can be rejected)
			// and commit nothing.
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				// Cancelled after we opened this pane. expectOwnPaneShow (set
				// before TryShow) keeps its Showing from being miscounted as an
				// independent user open; the compensation clears it on teardown.
				cmd.kbd.finishShow(cmd.gen)
				tkEnqueue(tkCommand{kind: tkCmdCompensateHide, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: globalShowEpoch, epoch: seq0})
				return
			}
			// We opened it — losing editor focus may hide it again (a
			// keyboard the user opened themselves is left alone). Any
			// stale ownership held by the OTHER window is cleared: there
			// is one keyboard, and its owner is now this window.
			transferKeyboardOwnership(known, cmd.kbd)
			cmd.kbd.legacyShow.Store(false)
			// Hiding only arrives if the event registration actually succeeded;
			// a late successful advise flips this via reconciliation.
			cmd.kbd.expectHiding.Store(cmd.kbd.eventsBound.Load())
			// Park shownGen at an UNMATCHABLE placeholder until THIS session's
			// Showing arrives. We just opened a NEW pane, so a stale Hiding from
			// the previous session is still in flight; it carries a paneEventGen
			// value <= the current one, so setting shownGen one PAST the current
			// value (Load()+1, WITHOUT advancing paneEventGen — advancing it would
			// let that stale Hiding read the new value and match again) guarantees
			// no in-flight Hiding matches. This session's own Showing overwrites
			// shownGen with the real number its Hiding will carry. Only meaningful
			// when advised — otherwise no Showing/Hiding comes at all.
			if cmd.kbd.eventsBound.Load() {
				cmd.kbd.shownGen.Store(cmd.kbd.paneEventGen.Load() + 1)
				// This write supersedes any adopted session, so retire the
				// Hiding handler's one-lower relaxation with it. The relaxation
				// is already inert once shownGen moves — it fires only while
				// adoptedGen EQUALS shownGen, and shownGen never returns to an
				// earlier value — but that is arithmetic, and the rule this
				// project keeps re-learning is that an invariant belongs at the
				// write site rather than in a reader's head. Note the branch
				// below (pane ALREADY visible) deliberately clears neither: that
				// session may BE the adopted one, whose real Hiding still needs
				// the relaxation to land.
				cmd.kbd.adoptedGen.Store(0)
			}
			// A fresh real show supersedes any in-flight hide (this IS the show).
			tkHideDeadlineNs.Store(0)
			cmd.kbd.shownByUs.Store(true)
			cmd.kbd.paneVisible.Store(true)
			cmd.kbd.sessionGen.Add(1)
			globalShowEpoch++ // a committed session; invalidates pending compensations
			cmd.kbd.occlusionEpoch.Add(1)
			tkEnsureOcclusionMonitor(cmd.kbd, cmd.hwnd)
			// TryShow accepted the REQUEST; that is all it promises. Microsoft
			// documents the pane as shown "only if a hardware keyboard is not
			// available", so a true return with nothing on screen is a documented
			// outcome — and the one outcome that leaves a focused field with no
			// way to type into it. Check back once.
			verify := tkCommand{kind: tkCmdVerifyShow, kbd: cmd.kbd, hwnd: cmd.hwnd, gen: cmd.gen, epoch: seq0}
			time.AfterFunc(tkShowVerifyDelay, func() { tkEnqueue(verify) })
		case tkPaneAlreadyVisible:
			// The pane was already up (this call did not open it), so on a
			// cancel just don't commit ownership — leave the keyboard alone.
			// This call opened NO new uncommitted pane, so the own-pane mark set
			// before TryShow does not apply — clear it (whether or not we commit)
			// so it can't linger and swallow a later real user open.
			cmd.kbd.expectOwnPaneShow.Store(0)
			if !valid(cmd.kbd, cmd.hwnd, cmd.gen) {
				cmd.kbd.finishShow(cmd.gen)
				return
			}
			// Re-show handling. A poll that STILL sees the pane occluding keeps
			// polling until it closes and a fresh TryShow re-opens it (→
			// tkPaneShown); when the poll bound is reached the pane is stable and
			// we fall through to adopt. A first adoption inside a hide's window
			// arms one poll (so a re-tap during the close is not lost) and also
			// falls through to adopt now — reshow() returns true only when this
			// command must stop here.
			if reshow() {
				return
			}
			// If ANOTHER of our windows owns this keyboard session, ownership
			// follows the user to this window so its blur-hide can close it.
			transferKeyboardOwnership(known, cmd.kbd)
			cmd.kbd.expectHiding.Store(cmd.kbd.eventsBound.Load()) // only if advised
			// No shownGen change here: the pane was ALREADY visible, so its
			// Showing already ran and set shownGen to the value its Hiding will
			// carry. Re-parking it would wrongly drop that real Hiding.
			cmd.kbd.paneVisible.Store(true)
			cmd.kbd.sessionGen.Add(1)
			// The pane occludes us or reports a location: it is on screen.
			tkMarkKeyboardSeen(cmd.kbd)
			globalShowEpoch++ // a committed session; invalidates pending compensations
			cmd.kbd.occlusionEpoch.Add(1)
			tkEnsureOcclusionMonitor(cmd.kbd, cmd.hwnd)
		case tkPaneRefused:
			// Pane machinery works but declined (hardware keyboard, policy, a
			// foreground check we cannot influence).
			cmd.kbd.expectOwnPaneShow.Store(0) // no pane opened; drop the own-pane mark set before TryShow
			// A "refusal" can also be a pane that was mid-hide and VANISHED
			// between TryShow and the rect probe (all-zero rect -> Refused). Route
			// through the same reshow() the visible branch uses: an in-progress
			// poll MUST continue here (a poll that dies on Refused loses the re-tap
			// it exists to honor), and a first tap inside a hide's window arms one.
			// reshow() returns true only when this command must stop; a genuine
			// decline (no poll, no live deadline) returns false and just logs.
			if reshow() {
				return
			}
			// An EXHAUSTED poll that lands HERE is not the same as one that lands
			// on a visible pane: there the keyboard is up, so adopting it is the
			// right end state, while here the pane never came back — ending now
			// silently drops the very re-tap the poll existed to honor. reshow()
			// returns false for both (it only reports "stop polling"), so the two
			// are separated at the call site. Hand the show to the transient
			// ladder: a fresh, re-validated attempt on its OWN budget after a
			// backoff. Poll mode is cleared on the requeue (reshow/polls/closing),
			// so this cannot start another poll chain — one extra attempt, then a
			// second refusal is taken at face value as a genuine decline.
			if cmd.reshow {
				if delay, again := keyboardShowRetryDelay(cmd.retries); again {
					re := cmd
					re.reshow, re.closing, re.polls = false, false, 0
					re.retries++
					time.AfterFunc(delay, func() { tkEnqueue(re) })
					tkDiagEvent("show").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Int8("retry", re.retries).Msg("touch keyboard: re-show poll exhausted with pane gone; show requeued")
					return
				}
			}
			// A genuine decline. Historically this branch stopped here, on the
			// grounds that a blind Toggle could close a keyboard the user is
			// looking at — but tkPaneShow only returns Refused after BOTH the
			// OccludedRect probe and IFrameworkInputPane::Location have
			// conclusively reported "not visible" (a probe that merely FAILED
			// yields tkPaneError instead). So there is no keyboard to close, and
			// stopping here is not caution, it is the user tapping a field and
			// getting nothing. Hand it to the legacy host, which re-checks
			// visibility once more before it toggles anything.
			tkDiagEvent("show").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg("touch keyboard: InputPane.TryShow declined; falling back to the legacy host")
			legacyShow(cmd, seq0)
		case tkPaneError:
			// No pane opened (transient failure); drop the own-pane mark set
			// before TryShow — a retry re-sets it before its own TryShow.
			cmd.kbd.expectOwnPaneShow.Store(0)
			// A WinRT failure that a retry may still clear (stage+hr
			// logged at the failure site). While that is true, do not fall
			// back to Toggle: on Windows 11 it could close an already-open
			// keyboard. But do NOT drop the command
			// either — a transient factory/GetForWindow/TryShow error on
			// the FIRST tap is the original bug all over again. Requeue the
			// same command on the retry ladder; it re-validates
			// generation/HWND/foreground when it runs.
			if delay, again := keyboardShowRetryDelay(cmd.retries); again {
				re := cmd
				re.retries++
				time.AfterFunc(delay, func() { tkEnqueue(re) })
				tkDiagEvent("show").Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Int8("retry", re.retries).Msg("touch keyboard: transient InputPane failure; show requeued")
			} else {
				// The ladder is spent, and this is exactly what the device
				// trace ended with: a tap, fifteen seconds of the same
				// failing call, and no keyboard. Withholding the legacy
				// host was only ever justified while a retry might still
				// succeed; once it cannot, the choice is between a Toggle
				// and nothing at all. legacyShow re-checks visibility
				// itself before it toggles, so it will not close a
				// keyboard the user is looking at.
				log.Warn().Uint64("hwnd", uint64(cmd.hwnd)).Int64("gen", cmd.gen).Msg("touch keyboard: InputPane kept failing; falling back to the legacy host")
				legacyShow(cmd, seq0)
			}
		case tkPaneUnavailable:
			// WinRT input pane genuinely absent (old Win10) — hand it to the
			// legacy host. The legacy path raises NO WinRT Showing, so the
			// own-pane mark (set before TryShow if the FrameworkInputPane advise
			// had succeeded) would never be consumed and would linger through
			// that path's several early exits; legacyShow drops it first.
			legacyShow(cmd, seq0)
		}
	}

	doHide := func(cmd tkCommand) {
		kbd := cmd.kbd
		// TryHide (and legacy Toggle) are best-effort: a false return only
		// means the request was not honored right now. Do NOT give up after a
		// few tries — a transient failure would then leave an app-opened
		// keyboard on screen with no new trigger (the blur that requested the
		// hide is already consumed). Retry fast at first, then fall back to a
		// rare heartbeat indefinitely; doHide's top guards (released, hideGen
		// bumped by an editor re-focus, ownership moved so !shownByUs) cancel
		// it the instant it is moot.
		retryHide := func() {
			again := cmd
			delay := 500 * time.Millisecond
			if cmd.retries >= tkHideRetryFast {
				delay = tkHideRetrySlow
			} else {
				again.retries++ // count only through the fast phase (no int8 overflow)
			}
			time.AfterFunc(delay, func() { tkEnqueue(again) })
		}
		hwnd := windows.HWND(kbd.hwnd.Load())
		// A hide is honored only when nothing changed since it was
		// requested: the window is alive, no editor tap bumped the hide
		// generation, the keyboard is still ours, and the OWNING window is
		// foreground (a weaker our-process check would let the main
		// window's delayed hide close a keyboard being used with the
		// console; TryHide itself only rejects background apps).
		if kbd.released.Load() || kbd.hideGen.Load() != cmd.gen || !kbd.shownByUs.Load() {
			return
		}
		if hwnd == 0 {
			return
		}
		if tkOwnForegroundWindow() != hwnd {
			// Our window is not foreground right now. This is NOT necessarily
			// a reason to drop the hide: a native dialog (e.g. the file picker
			// an attachment tap opens) transiently steals the foreground, and
			// trackEditorFocus has already consumed the blur — no fresh hide
			// will be enqueued. So WAIT for the foreground to return instead of
			// losing the hide and leaving the keyboard up after the dialog
			// closes. Do NOT abandon after a fixed timeout: a file dialog can
			// stay open arbitrarily long. Poll fast (tkHideForegroundWaitDelay)
			// at first for a snappy return, then fall back to a rare heartbeat
			// (tkHideForegroundSlowWait) indefinitely — this is safe because
			// the top guards cancel the wait the instant it is moot: an editor
			// re-focus bumps hideGen, an ownership move clears shownByUs, and
			// release stops it outright.
			again := cmd
			delay := tkHideForegroundWaitDelay
			if cmd.fgWaits >= tkHideForegroundFastWaits {
				delay = tkHideForegroundSlowWait
			} else {
				again.fgWaits++ // only count up through the fast phase (no int8 overflow)
			}
			time.AfterFunc(delay, func() { tkEnqueue(again) })
			return
		}
		if kbd.legacyShow.Load() {
			// The show went through the legacy Toggle path because
			// IInputPane2 is absent — TryHide would just fail. Toggle
			// hides a VISIBLE keyboard; the visibility gate keeps this
			// from accidentally showing one.
			if touchKeyboardVisible() {
				if tkInvokeToggle() {
					endKeyboardSession(kbd)
				} else {
					retryHide()
				}
			} else {
				// The keyboard is ALREADY gone (the user closed it via its
				// own X). On the legacy path there are no pane events, and
				// the monitor may be dead — nothing else would ever clear
				// shownByUs/legacyShow or the padding. The same visibility
				// probe that gates Toggle is trusted here to end the local
				// session.
				endKeyboardSession(kbd)
			}
			return
		}
		if tkTryHide(hwnd) {
			endKeyboardSession(kbd)
			// The pane accepted the hide and is now animating closed. Arm a
			// window during which a re-tap that adopts this still-occluding pane
			// (AlreadyVisible) knows it is genuinely closing and triggers a
			// re-show. A deadline, not a flag: the Hiding event only means hiding
			// STARTED, and a flag would stay set after the pane is gone and later
			// mis-fire on a stable pane. Process-wide, so a tap in the OTHER window
			// sees this close too; endKeyboardSession no longer clears it, so the
			// order of these two lines no longer matters.
			tkHideDeadlineNs.Store(tkNowNs() + int64(tkHideAnimWindow))
		} else {
			retryHide()
		}
	}

	// Not a channel receive. This thread is a single-threaded apartment, and an
	// STA that never dispatches never receives the Showing/Hiding callbacks it
	// registered — COM delivers them through a hidden window on this thread's
	// message queue, and the shell thread making the call blocks until someone
	// drains it. tkAwaitCommands waits on the command wakeup and on that queue
	// together, pumping the one and returning on the other.
	for {
		tkAwaitCommands()

		tkCmdMu.Lock()
		cmds := tkCmdList
		tkCmdList = nil
		tkCmdMu.Unlock()

		if ensureFwPane() {
			// nil→ready transition (startup or recovery after failures):
			// advise every window that was waiting for the pane. Retrying a
			// window whose AdviseWithHWND itself failed is NOT done here — it
			// goes through the throttled tkCmdFwRetry timer so a persistent
			// failure can't spin the COM call every batch.
			adviseAll()
		}
		for _, cmd := range cmds {
			if cmd.kbd != nil { // tkCmdFwRetry carries no window
				registerKeyboardState(known, cmd.kbd)
			}
			switch cmd.kind {
			case tkCmdShow:
				doShow(cmd)
			case tkCmdHide:
				doHide(cmd)
			case tkCmdVerifyShow:
				doVerifyShow(cmd)
			case tkCmdRelease:
				delete(known, cmd.kbd)
				// Window destroyed: end ownership, clear the handle
				// binding, invalidate any queued/in-flight show and hide
				// generations, and unregister events. Later commands for
				// this kbd fail their released/gen/hwnd checks.
				endKeyboardSession(cmd.kbd)
				cmd.kbd.hwnd.Store(0)
				cmd.kbd.showGen.Add(1)
				cmd.kbd.hideGen.Add(1)
				if fwPane != nil {
					unadvise(cmd.kbd)
				}
			case tkCmdAdvise:
				// Proactively register the Showing/Hiding handler as soon as a
				// window's native handle is known (Gio ViewEvent), NOT only on
				// first editor tap. Without this, a keyboard the user opens via
				// the Windows taskbar button BEFORE any tap raises a Showing we
				// never registered for — no monitor starts and a docked
				// keyboard covers the composer, contradicting the documented
				// "user-opened keyboard is tracked" behavior. If fwPane isn't
				// ready yet, the batch-top ensureFwPane transition re-advises
				// every live bound window (advisableKeyboardStates), so the
				// hwnd stored here is enough for it to be picked up later.
				if cmd.kbd.released.Load() {
					break
				}
				if cmd.hwnd != 0 {
					cmd.kbd.hwnd.Store(uintptr(cmd.hwnd))
				}
				if fwPane != nil && cmd.hwnd != 0 {
					ensureAdvised(cmd.kbd, cmd.hwnd)
				}
			case tkCmdShowingEvent:
				// Marshaled pane Showing (any show, ours or user-initiated). A
				// visibility boundary: bump the epoch so a stale zero sample
				// taken before a MANUALLY opened keyboard appeared can't pass
				// the publish check, mark the pane visible, and run the monitor
				// so padding tracks even a user-opened keyboard.
				if cmd.kbd.released.Load() {
					break
				}
				cmd.kbd.occlusionEpoch.Add(1)
				cmd.kbd.paneVisible.Store(true)
				// tkHideDeadlineNs is deliberately NOT cleared here. This is
				// news about ONE registration, delivered through the queue, and
				// nothing on it says how old the event is: the callback runs
				// whenever the apartment happens to pump, so a Showing raised
				// before a hide can be handled after it, and the two windows
				// advised over the one shared pane each raise their own. Zeroing
				// a PROCESS record from an event of foreign scope and unknown
				// age is how a started close gets lost — the shape this file has
				// now met three times. Leaving it standing costs at most one
				// animation window of reading an up pane as closing, and that
				// error arms a re-show poll rather than dropping a keyboard. A
				// show WE commit still clears it outright, in doShow.
				//
				// Record the event-ordered generation this Showing carried (it was
				// assigned in the callback) as the CURRENT live pane session. The
				// matching Hiding carries the same number and is honored; a stale
				// Hiding from an older pane carries a smaller number and is dropped.
				//
				// NEVER backwards. That drop rule only works while shownGen is
				// monotone, and this is not the only writer: reconcilePaneVisible
				// may have claimed a later generation for a pane it believed was
				// missed. Its CAS makes that nearly impossible now, but "nearly"
				// is not an invariant — the callback runs on an RPC thread and
				// can advance paneEventGen in the few instructions between advise
				// succeeding and reconcile reading its baseline. Regressing
				// shownGen there would make the NEXT Hiding look stale and strand
				// padding under a closed keyboard, so refuse the write instead:
				// both numbers describe the same physical pane, and the higher
				// one is what the pending Hiding will carry.
				if cmd.gen > cmd.kbd.shownGen.Load() {
					cmd.kbd.shownGen.Store(cmd.gen)
					// A real Showing supersedes any adopted session, so the
					// Hiding handler's one-lower relaxation must retire with it.
					// The equality test would already fail against the new
					// shownGen; clearing makes that an invariant of the write
					// rather than an inference from arithmetic.
					cmd.kbd.adoptedGen.Store(0)
				}
				// A Showing is the pane physically appearing: the hardest
				// evidence anywhere in this file, and the earliest moment a show
				// of ours is known to have worked. Deliberately NOT gated on
				// shownByUs, unlike the sessionGen bump below — for OUR shows
				// this is the only seen-signal that arrives before the 700ms
				// verification, and withholding it would make the tap coalesce
				// hold the field while the user is already typing.
				tkMarkKeyboardSeen(cmd.kbd)
				// A user-initiated Showing (not one of our TryShow calls, which
				// already bumped sessionGen) starts a distinct keyboard session:
				// bump sessionGen so its own OwnerExpire binds to a value distinct
				// from any prior session. (The compensation-abort signal, formerly
				// bumped here, now lives in the Showing CALLBACK as tkPaneShowSeq
				// so it is ordered with the events, not the command processing.)
				if !cmd.kbd.shownByUs.Load() {
					cmd.kbd.sessionGen.Add(1)
				}
				// A real Showing means a WinRT pane that WILL raise the
				// matching Hiding, so its monitor waits for that event rather
				// than self-expiring on zero occlusion.
				cmd.kbd.expectHiding.Store(true)
				// Bind the handle the Showing pertains to (a user-opened
				// keyboard may have had no tap to bind it) before monitoring.
				if cmd.hwnd != 0 {
					cmd.kbd.hwnd.Store(uintptr(cmd.hwnd))
				}
				tkEnsureOcclusionMonitor(cmd.kbd, cmd.hwnd)
			case tkCmdHidingEvent:
				// Marshaled pane Hiding: our show session (if any) is over.
				// Bumping the epoch HERE (not in the callback) is what makes it
				// truly serialized with tkCmdPublish — a sample published just
				// before this can no longer resurrect padding after the hide.
				// Drop a STALE Hiding: cmd.gen is the paneEventGen its own Showing
				// assigned in the callback. If a newer Showing has since been
				// PROCESSED (advancing shownGen past cmd.gen), this Hiding refers to
				// an older pane and must not wipe the new session's ownership/
				// padding. The current session's Hiding carries shownGen and matches.
				//
				// One session type matches one lower as well. A session ADOPTED by
				// reconcilePaneVisible was never announced by a Showing; reconcile
				// invented its generation with a CAS, and a Hiding raised in the
				// window between the visibility probe and that CAS carries the
				// PRE-CAS number. Exact match would drop it and strand the session
				// permanently, because an adopted pane has no further Showing coming
				// (see adoptedGen). adoptedGen names the one shownGen this applies
				// to and is cleared as soon as it is used or superseded, so the
				// relaxation covers exactly that session and never a later one.
				if cmd.kbd.released.Load() {
					break
				}
				shown := cmd.kbd.shownGen.Load()
				if cmd.gen != shown &&
					!(cmd.gen+1 == shown && cmd.kbd.adoptedGen.Load() == shown) {
					break
				}
				cmd.kbd.adoptedGen.Store(0)
				// The pane is going away, so the synthetic-Showing dedupe mark
				// reconcile may have armed for it names a generation that will
				// never be stamped. The Hiding CALLBACK drops it too, but only if
				// it ran after the mark was armed — in the adopted case above it
				// ran BEFORE. Dropping it here, on the service goroutine (which is
				// strictly after reconcile), covers that ordering; leaving it to
				// expire would let it swallow the classification of the next pane
				// the USER opens by hand within tkSynthShowWindow.
				//
				// Unconditional, i.e. also on the ordinary exact-match path where
				// the mark may still be owed to a synthetic Showing genuinely in
				// flight. That direction is the cheap one: an un-deduped Showing
				// bumps tkPaneShowSeq, compensation aborts and a keyboard is left
				// open, which the user can close. Keeping a mark that outlives its
				// pane costs the opposite — the user's own next pane classified as
				// ours and closed under them, with nothing they can do about it.
				tkDropSynthShowMark(cmd.kbd)
				cmd.kbd.occlusionEpoch.Add(1)
				cmd.kbd.shownByUs.Store(false)
				cmd.kbd.paneVisible.Store(false)
				// This session is over, so drop the own-pane mark. A committed show
				// never had it consumed (its Showing short-circuited on shownByUs),
				// and without this a manual open AFTER this hide would be miscounted
				// as our own pane and skip its tkPaneShowSeq bump.
				cmd.kbd.expectOwnPaneShow.Store(0)
				// A Hiding means the pane STARTED closing — from ANY cause, incl. a
				// system/manual close that never went through our doHide. (Re)arm
				// the hide window so a re-tap landing during this animation still
				// triggers a re-show. Not zeroed here: the pane is not gone yet.
				tkHideDeadlineNs.Store(tkNowNs() + int64(tkHideAnimWindow))
				cmd.kbd.publishOccludedDp(0)
			case tkCmdFwRetry:
				// Throttled (tkFwPaneRetryBackoff) timer wake. ensureFwPane
				// already retried the pane at batch top; now retry the
				// per-window registration too — this is the ONLY place a
				// failed AdviseWithHWND is re-attempted, so its cadence is
				// bounded by the timer, not the 250ms publish rate. Clear the
				// dedupe flag; the post-loop scheduleFwRetry re-arms if a
				// window is still waiting.
				fwRetryPending = false
				adviseAll()
			case tkCmdReconcile:
				// Re-check pane visibility after an inconclusive post-advise
				// probe. A RUNNING monitor must NOT block this: if that monitor
				// was started with expectHiding=false (shown before advise
				// succeeded), reconcile is what upgrades it to true for a WinRT
				// pane — otherwise the old monitor would wrongly end a live
				// not-occluding session on its zero streak. Stop when the
				// window is gone, expectHiding is already confirmed, or the
				// window's handle changed since this reconcile was scheduled.
				if cmd.kbd.released.Load() || cmd.kbd.expectHiding.Load() {
					break
				}
				if vh := cmd.kbd.viewHwnd.Load(); vh != 0 && windows.HWND(vh) != cmd.hwnd {
					break
				}
				// Baseline: the scheduler's pre-pin value, carried through the
				// whole retry chain — NOT a fresh read here. A fresh read is the
				// bug it looks like the fix for: the callback bumps paneEventGen
				// and enqueues its Showing command from an RPC thread, so a
				// Showing delivered while this very command sat in the queue has
				// ALREADY advanced the counter but has NOT been processed. Reading
				// now would fold that advance into the baseline, the CAS would
				// succeed, and we would re-run the ownership predicate the
				// callback just ran — finding the own-pane mark already consumed
				// and so counting OUR pane as the user's, aborting its own
				// compensation. The guard above only covers the PROCESSED case
				// (tkCmdShowingEvent sets expectHiding), which is precisely the
				// half a fresh read does not need help with.
				//
				// Carrying a stale baseline instead costs nothing: it can only
				// make the CAS fail, and a CAS failure means "a real Showing owns
				// this pane" — whose command does strictly more than this branch.
				if found, uncertain := reconcilePaneVisible(cmd.kbd, cmd.hwnd, cmd.gen0); !found && uncertain {
					// Inconclusive still — Windows won't resend the missed
					// Showing, so keep retrying with a growing (capped) backoff
					// until definitive/released/HWND-change rather than giving
					// up and leaving an open keyboard unmonitored.
					again := cmd
					if again.retries < 10 {
						again.retries++
					}
					delay := time.Duration(again.retries) * tkReconcileRetryDelay
					if delay > tkReconcileMaxBackoff {
						delay = tkReconcileMaxBackoff
					}
					time.AfterFunc(delay, func() { tkEnqueue(again) })
				}
			case tkCmdCompensateHide:
				// A show that was cancelled mid-platform-call may have left a
				// keyboard up with no owner (shownByUs still false). Hide it,
				// and — unlike a fire-and-forget TryHide/Toggle — verify and
				// retry, because a single rejected hide would strand it open.
				//
				// Window CLOSE is the PRIMARY trigger for this race (closing the
				// console cancels a show mid-COM-call), so a released window must
				// NOT abort the cleanup — the keyboard (especially TabTip)
				// outlives the window that opened it.
				//
				// Abort if a keyboard session was ESTABLISHED since this
				// compensation was scheduled, because either lever we would use
				// (global Toggle, or TryHide on the shared pane) would close it:
				//   - globalShowEpoch: a window COMMITTED a show via doShow (incl.
				//     a re-show of this window — this subsumes the old shownByUs
				//     check, and covers a user RE-FOCUS, which goes through doShow).
				//   - tkPaneShowSeq: the user opened a keyboard BY HAND (taskbar /
				//     keyboard button) — a Showing with shownByUs false that never
				//     reached doShow. Bumped in the Showing callback, in event
				//     order, and skips the very pane THIS compensation opened
				//     (expectOwnPaneShow), so it never aborts on its own keyboard.
				//     Also bumped by reconcilePaneVisible, so a hand-open that
				//     landed while no handler was advised (its Showing was never
				//     delivered and the shell will not resend it) still reaches
				//     this guard once the retried advise discovers the pane.
				// Both guards apply to BOTH paths: TryHide is window-targeted but a
				// hand-opened keyboard on cmd.hwnd's window is still its pane.
				//
				// LIMIT, stated plainly because the code cannot enforce more: a
				// Showing carries no opener identity, so inside tkOwnPaneShowWindow
				// after an accepted-but-empty TryShow, a keyboard the user raises
				// from the taskbar is indistinguishable from our own pane finally
				// arriving, and this compensation will hide it once. Every OTHER
				// route is covered: a re-tap on a field goes through doShow and
				// trips globalShowEpoch, and the user's SECOND open trips
				// tkPaneShowSeq (the first consumed the mark), so the mistake
				// cannot repeat. The honest guarantee is "at most once", not
				// "never" — buying "never" means never hiding a late pane, trading
				// a rare stolen keyboard for a routine stranded one.
				if globalShowEpoch != cmd.gen || tkPaneShowSeq.Load() != cmd.epoch {
					// A newer session owns teardown now; drop our own-pane mark so
					// it can't outlive this compensation and swallow a later open.
					cmd.kbd.expectOwnPaneShow.Store(0)
					break
				}
				comp := cmd
				visibleNow := false
				gone := false
				if cmd.legacy {
					// The legacy path raises no Showing, so tkPaneShowSeq can NEVER
					// advance here and neither guard above can abort us when the user
					// opens a keyboard by hand — on this path they are blind. The one
					// attribution signal left is time: the TabTip our own Toggle
					// launched shows up within the fast phase, cold start included. If
					// we have not ACTED by then, stop — either nothing appeared at all,
					// or a keyboard is up that we could not Toggle off (foreign
					// foreground, failing COM), and past this point one still standing
					// belongs to the user rather than to us. (This SHRINKS the window;
					// on this path the two cases are genuinely indistinguishable.)
					//
					// The test is "did our Toggle LAND", not "did we see a keyboard":
					// a sighting justifies acting INSIDE the window, it does not buy
					// time outside it, and reading it as licence to keep swinging for
					// the full ~16s budget is exactly how a keyboard the user opened
					// at second twelve gets closed. What it costs: a hide whose
					// foreground only came back late is abandoned. That leaves an
					// orphan keyboard the user can close, against taking away one they
					// asked for. After a Toggle that DID land, the guard below ends
					// the compensation on the next pass anyway.
					if !cmd.toggled && cmd.retries >= tkCompensateLegacyMax {
						cmd.kbd.expectOwnPaneShow.Store(0)
						log.Debug().Uint64("hwnd", uint64(cmd.hwnd)).Bool("sawPane", cmd.sawPane).Msg("touch keyboard: legacy compensating hide stopped; nothing was toggled off inside the launch window")
						break
					}
					// The legacy ITipInvocation.Toggle is GLOBAL and NOT
					// idempotent — it would SHOW a hidden keyboard — so fire it
					// only when one is actually visible AND our own process holds
					// the foreground (otherwise a retry after the user switched
					// apps, or after our window closed and another app took focus,
					// would close THAT app's keyboard). A foreign foreground just
					// skips this attempt; the retry gives focus a chance to return.
					if touchKeyboardVisible() {
						visibleNow = true
						// At most ONE SUCCESSFUL Toggle per compensation (a
						// call that failed to reach the host toggled nothing
						// and does not count — see below). We fire it only
						// while our process holds the foreground, where it
						// effectively always takes — so a keyboard STILL visible
						// on the next attempt is far more likely a new one (the
						// user's) than our Toggle having missed. Toggling again
						// would then close theirs, and every extra attempt is
						// another guess with no new evidence behind it. One
						// guess, bounded by the launch window above; after that
						// we stop rather than keep swinging. If our Toggle really
						// did miss, the cost is a keyboard left up, which the
						// user can close — the opposite mistake takes away a
						// keyboard they just asked for.
						if cmd.toggled {
							cmd.kbd.expectOwnPaneShow.Store(0)
							log.Debug().Uint64("hwnd", uint64(cmd.hwnd)).Msg("touch keyboard: legacy compensating hide stopped; a keyboard is still up after our Toggle, treating it as the user's")
							break
						}
						if tkOwnForegroundWindow() != 0 {
							// Only a Toggle that actually REACHED the host
							// counts as our one guess. tkInvokeToggle reports
							// whether the COM call went through; a transient
							// failure did NOTHING, so recording it would spend
							// the single attempt on a no-op and make the next
							// pass read our own still-visible keyboard as the
							// user's and stop. Retry instead — the launch
							// window and the retry budget still bound us.
							if tkInvokeToggle() {
								comp.toggled = true
							} else {
								log.Debug().Uint64("hwnd", uint64(cmd.hwnd)).Msg("touch keyboard: legacy compensating Toggle failed; retrying")
							}
						}
					} else {
						gone = true
					}
				} else {
					// TryHide is window-targeted (GetForWindow(hwnd)) AND
					// idempotent (a no-op when nothing is up), so fire it EVERY
					// attempt — TryShow only ACCEPTS the show, the pane can become
					// visible a beat later (its Showing lags TryShow), and gating
					// on current visibility would stop before that late pane
					// appears and strand it ownerless. Safe even after release.
					// Its RETURN reports whether a pane was actually there to hide:
					// use that for sawPane, because Location read AFTER a fast hide
					// is already empty and would never record that we saw the pane.
					if tkTryHide(cmd.hwnd) {
						visibleNow = true
					}
					if vis, ok := tkFwPaneLocationVisible(fwPane); ok {
						if vis {
							visibleNow = true
						}
						gone = !vis
					}
				}
				if visibleNow {
					// The pane our cancelled TryShow accepted has now been SEEN
					// (and hidden). One TryShow yields at most one pane, so nothing
					// appearing after this instant can be it: drop the own-pane mark
					// here instead of waiting out its deadline, or a keyboard the
					// user opens during the retries still to come would claim it and
					// skip the tkPaneShowSeq bump that aborts us. Unlike the deadline
					// this is an OBSERVATION, not a guess about elapsed time.
					comp.sawPane = true
					cmd.kbd.expectOwnPaneShow.Store(0)
				}
				// Terminate only when the pane we set out to hide has actually
				// been SEEN and is now gone (success), or the retry budget is
				// exhausted. Do NOT stop merely because it looks gone right now —
				// an accepted pane can appear anywhere within the shell's delay,
				// not just the fast window — so keep TryHide-ing across the whole
				// bounded budget until it has appeared and been hidden.
				done := comp.sawPane && gone
				if done || cmd.retries >= tkCompensateMaxRetries {
					if !done {
						log.Debug().Uint64("hwnd", uint64(cmd.hwnd)).Bool("legacy", cmd.legacy).Msg("touch keyboard: compensating hide abandoned after retries")
					}
					// Consumed or moot: drop the own-pane mark so it can't outlive
					// this compensation and swallow a later real user open.
					cmd.kbd.expectOwnPaneShow.Store(0)
					break
				}
				compDelay := tkHideForegroundWaitDelay
				if cmd.retries >= tkHideRetryFast {
					compDelay = tkHideRetrySlow
				}
				comp.retries++
				time.AfterFunc(compDelay, func() { tkEnqueue(comp) })
			case tkCmdUnadviseRetry:
				if fwPane != nil {
					unadvise(cmd.kbd)
				}
			case tkCmdPublish:
				// Occlusion samples flow through the service so they are
				// SERIALIZED with release, and each sample carries the
				// OWNER ID of the monitor that took it: a sample from a
				// monitor that has since exited or been replaced (its id
				// no longer owns the slot) is dropped, so a stale zero
				// can't strip a new session's padding and a stale height
				// can't resurrect padding after the pane hid.
				if cmd.kbd.released.Load() || cmd.kbd.monitorOwner.Load() != cmd.gen ||
					cmd.kbd.occlusionEpoch.Load() != cmd.epoch {
					// The sampling monitor is gone, or a pane show/hide
					// boundary passed after the sample was taken — a stale
					// height must not resurrect padding after a hide, and
					// a stale zero must not strip a new session's padding.
					break
				}
				// Also gate on paneVisible: the epoch check alone protects
				// only samples taken BEFORE Hiding, but the monitor keeps
				// polling and can read the NEW epoch together with a still-
				// nonzero rect of the closing keyboard right after Hiding.
				// Hiding clears paneVisible, so this stops that transient
				// resurrection of padding. (A zero sample is harmless; the
				// hide already published zero.)
				if !cmd.kbd.paneVisible.Load() && cmd.height > 0 {
					break
				}
				cmd.kbd.publishOccludedDp(cmd.height)
			case tkCmdOwnerExpire:
				// The monitor only sends this for a session that will get NO
				// Hiding (expectHiding=false: legacy/Toggle, or a window with
				// no registration) — a session that expects Hiding stays alive
				// until it, and never expires here. So the matured zero streak
				// IS the real teardown signal. The cleanup runs HERE,
				// serialized with shows: it applies only if no newer successful
				// SESSION started since the monitor matured (sessionGen —
				// deliberately not showGen, which is also bumped by
				// outside-taps and cancelled/refused requests that start no
				// monitor and must not strand shownByUs) AND no new monitor is
				// running.
				if cmd.kbd.sessionGen.Load() != cmd.gen || cmd.kbd.monitorOwner.Load() != 0 {
					break
				}
				// No Hiding is coming, so this expiry is the only teardown:
				// clear the padding, the visibility flag (gates
				// keyboardSessionIdle) and ownership (else the app could later
				// hide a keyboard the user reopened).
				cmd.kbd.publishOccludedDp(0)
				cmd.kbd.paneVisible.Store(false)
				cmd.kbd.shownByUs.Store(false)
				cmd.kbd.legacyShow.Store(false)
			case tkCmdPaneTruth:
				// The monitor has watched the physical pane disagree with
				// paneVisible for longer than any event still in flight could
				// explain. That is the residue of the one race the handler mutex
				// cannot cover (see tkStampHiding): the lock orders the callbacks
				// by acquisition, the apartment does not order them at all, and
				// an inverted pair is indistinguishable from a legitimate one by
				// its generations alone. So this does not re-derive the order the
				// events were raised in; it overrides the conclusion with the
				// state of the screen, at a moment when the screen has settled
				// and therefore MEANS something — which is why the check lives
				// here and not at the events themselves, where a Showing arrives
				// before its pane is up and reads identically to a pane that has
				// already gone.
				//
				// Bound to the reporting monitor's owner id, like every other
				// sample, AND to the occlusion epoch the report was formed at.
				// The owner id alone is NOT a session binding: one monitor
				// outlives any number of sessions, because
				// tkEnsureOcclusionMonitor bumps the ping and returns when one
				// is already running. A verdict formed for a pane that has since
				// closed therefore still names the live monitor after the user
				// has reopened the keyboard, and would be applied to the NEW
				// session — whose pane is accepted but not yet on screen, so
				// Location answers "down", the correction ends a session that
				// just began, and clearing shownByUs with it disarms the
				// deferred check that is the only thing left to notice a TryShow
				// that was accepted and never materialized. The tap would be
				// swallowed exactly as in the bug this correction exists for.
				//
				// The epoch rather than sessionGen because it is the coarser of
				// the two: every accepted show bumps both, and it moves on the
				// pane events and on any teardown besides, each of which equally
				// voids a conclusion drawn before it. WinRT sessions only.
				if cmd.kbd.released.Load() || cmd.kbd.monitorOwner.Load() != cmd.gen ||
					cmd.kbd.occlusionEpoch.Load() != cmd.epoch || !cmd.kbd.expectHiding.Load() {
					break
				}
				if cmd.sawPane {
					// UP, and already proven: OccludedRect named THIS window, so
					// there is no question a second probe could answer. The write
					// happens here anyway to keep every paneVisible write on this
					// thread, ordered against the events and against release.
					if cmd.kbd.paneVisible.Load() {
						break
					}
					cmd.kbd.paneVisible.Store(true)
					tkMarkKeyboardSeen(cmd.kbd)
					log.Debug().Uint64("hwnd", uint64(cmd.kbd.hwnd.Load())).Msg("touch keyboard: pane occludes the window though the session read as closed; restoring it")
					break
				}
				if !cmd.kbd.paneVisible.Load() {
					break // a Hiding got there first — nothing to correct
				}
				// DOWN, and NOT yet proven. The monitor's zero rect says only
				// that no pane occludes that window; a docked pane the window
				// sits above reads the same. Location answers globally, and the
				// asymmetry is what makes a global answer usable here: a
				// rectangle SOMEWHERE could belong to another app and would prove
				// nothing about us, but no rectangle AT ALL is a fact about the
				// whole desktop, and so about us. It is also the same object the
				// Showing and Hiding events come from, which is the point: this
				// asks the pane what it is doing rather than what it said.
				if up, ok := tkFwPaneLocationVisible(fwPane); up || !ok {
					// A pane really is up, or the pane could not be asked (none
					// yet, or an HRESULT error). Neither is grounds to end a live
					// session: stay put and let the monitor ask again, which is
					// precisely the behaviour this branch had before.
					break
				}
				// The keyboard is gone and its Hiding is not coming — it was
				// already spent, out of order. Apply what that Hiding would have,
				// minus the hide window: that window exists so a re-tap landing
				// DURING the close animation still re-shows, and the animation
				// ended at least a zero streak ago. Arming it now would suppress
				// the very re-show this correction exists to unblock.
				cmd.kbd.adoptedGen.Store(0)
				tkDropSynthShowMark(cmd.kbd)
				cmd.kbd.occlusionEpoch.Add(1)
				cmd.kbd.shownByUs.Store(false)
				cmd.kbd.paneVisible.Store(false)
				cmd.kbd.expectOwnPaneShow.Store(0)
				cmd.kbd.publishOccludedDp(0)
				log.Debug().Uint64("hwnd", uint64(cmd.kbd.hwnd.Load())).Msg("touch keyboard: pane is down though the session read as open; ending it")
			}
		}
		// AFTER the command loop, so a tkCmdAdvise processed this batch has
		// already entered its window into `known`: arm the deferred pane
		// retry if the pane is still down with a window waiting. (Running this
		// BEFORE the loop missed a window whose very first tkCmdAdvise was in
		// this same batch — known was still empty then, so no timer armed.)
		scheduleFwRetry()
	}
}

// tkEnsureOcclusionMonitor starts (once per window) a goroutine that polls
// the input pane's OccludedRect for that window's HWND while the keyboard
// is up and publishes the occluded height in dp into the window's state,
// waking its frame loop on changes so the layout can pad the bottom.
//
// Ownership protocol: monitorOwner holds the unique id of the running
// monitor (0 = none). Every call bumps monitorPing BEFORE trying to claim
// the slot, so a live monitor observes the ping. The monitor itself never
// performs session cleanup: when its zero-occlusion streak matures it
// releases the slot and enqueues tkCmdOwnerExpire, and the SERVICE applies
// the cleanup only if no newer show and no new monitor exist — a dying
// monitor therefore cannot wipe a successor session's state, no matter how
// the ping/exit races interleave.
func tkEnsureOcclusionMonitor(kbd *touchKeyboardState, hwnd windows.HWND) {
	if kbd.released.Load() {
		return
	}
	kbd.monitorPing.Add(1)
	id := kbd.monitorSeq.Add(1)
	if !kbd.monitorOwner.CompareAndSwap(0, id) {
		return // a live monitor will observe the ping
	}
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		// RoInitialize is retried with backoff instead of aborting — and
		// WITHOUT an attempt limit: if the Showing that started this monitor
		// already happened, no new event would ever restart a dead monitor,
		// and the composer could stay under the keyboard for the rest of the
		// session. Only a released window stops the retries (its release
		// command has already cleaned the state; a successor show re-claims
		// the slot after the CAS below).
		var uninit bool
		var roHR uintptr // monitor-goroutine-local failure hr for the Warn below
		initPing := kbd.monitorPing.Load()
		// RoInitialize here can only fail transiently: this monitor was
		// started by a successful show, and doShow already gates the entire
		// show on ensureCOM() (RoInitialize) succeeding — so a system with
		// no WinRT at all never reaches a monitor in the first place (its
		// show is abandoned earlier). The retry loop therefore rides out a
		// transient init failure and never spins on a permanently-absent
		// export.
		for attempt := 0; ; attempt++ {
			var ok bool
			ok, uninit, _, roHR = tkRoInit()
			if ok {
				if attempt > 0 {
					log.Debug().Int64("monitor", id).Int("attempts", attempt+1).Msg("touch keyboard: monitor COM init recovered")
				}
				break
			}
			if kbd.released.Load() {
				kbd.monitorOwner.CompareAndSwap(id, 0)
				return
			}
			// The retries exist to serve a LIVE session (nothing else would
			// clean its state). Once the session is already over — a Hiding
			// event or endKeyboardSession cleaned everything — a monitor
			// that never got COM up must not squat on the slot forever.
			// Exit through the ping/release protocol: a show that slipped
			// in re-activates the retries instead.
			if keyboardSessionIdle(kbd) {
				if p := kbd.monitorPing.Load(); p != initPing {
					initPing = p // new show while COM was down — keep trying
				} else {
					kbd.monitorOwner.CompareAndSwap(id, 0)
					if p2 := kbd.monitorPing.Load(); p2 != initPing && kbd.monitorOwner.CompareAndSwap(0, id) {
						initPing = p2 // ping slipped into the release gap
					} else {
						return
					}
				}
			}
			if attempt == 0 {
				log.Warn().Uint64("hwnd", uint64(hwnd)).Int64("monitor", id).Uint64("hr", uint64(roHR)).Msg("touch keyboard: monitor RoInitialize failing; retrying with backoff")
			}
			time.Sleep(keyboardMonitorInitDelay(attempt))
		}
		if uninit {
			defer tkProcRoUninitialize.Call()
		}

		lastPing := kbd.monitorPing.Load()
		zeroStreak := 0
		errStreak := 0
		truthStreak := 0
		for {
			// Window destroyed: stop immediately without publishing
			// anything — the release command already cleaned the state,
			// and a reused HWND must not be polled by a stale monitor.
			if kbd.released.Load() {
				kbd.monitorOwner.CompareAndSwap(id, 0)
				return
			}

			// This goroutine is an apartment of its own (tkRoInit asks for a
			// single-threaded one), and the pane proxies it holds are serviced
			// through its message queue. It never calls GetMessage, so nothing
			// else would ever drain that queue. Once per poll is enough: the
			// poll interval is the same order as a caller's patience.
			tkStaPump()

			epoch := kbd.occlusionEpoch.Load() // before sampling: a boundary during the poll voids the sample
			sampled := false
			visible := false
			var height int32
			var errHR uintptr
			var errStage string
			if interop, status, fhr, fstage := tkInputPaneFactory(); status == tkPaneShown {
				if s := tkPaneOccludedHeight(interop, hwnd); s.ok {
					sampled = true
					// A FLOATING keyboard reports zero occlusion HEIGHT but a
					// non-zero rect WIDTH — visible, just not occluding. It is
					// DELIBERATELY not padded around: OccludedRect gives real
					// reflow geometry only for a docked pane, and (as on other
					// Windows apps) the user repositions a floating keyboard
					// themselves. Width still marks it visible so the session,
					// ownership and this monitor stay alive — the moment the
					// user docks it, height becomes non-zero and the composer
					// reflows with no new Showing event.
					visible = s.height > 0 || s.width > 0
					if s.height > 0 {
						height = int32(s.height + 0.5)
					}
				} else {
					errHR, errStage = s.hr, s.stage
				}
				tkRelease(interop.vtbl.Release, unsafe.Pointer(interop))
			} else if status == tkPaneUnavailable {
				// The WinRT InputPane factory is GENUINELY absent (old Win10),
				// so OccludedRect can never be read for this session — the
				// show went through the legacy TabTip/Toggle path. Re-probing
				// the missing factory every 250ms forever is pure waste AND
				// leaves the composer under the keyboard. Fall back to the
				// IPTip host-window visibility probe (plain user32, always
				// present) and publish a conservative fixed clearance while it
				// is up; invisibility is the teardown signal, since the legacy
				// host raises no Hiding event. (E_NOINTERFACE — factory
				// present, only IInputPane2 absent — never reaches here: the
				// factory still returns tkPaneShown, so the real OccludedRect
				// branch above runs and measures true occlusion.)
				//
				// Unless the factory was refused for the APARTMENT rather than
				// being absent, in which case the pane is there and unreadable
				// from here, and a legacy no-show is not a teardown signal but
				// an unanswered question — see tkLegacyPaneSample.
				ok, up := tkLegacyPaneSample(fhr, touchKeyboardVisible())
				if up {
					visible = true
					height = keyboardLegacyClearanceDp
				}
				if ok {
					sampled = true
				} else {
					errHR, errStage = fhr, fstage
				}
			} else {
				errHR, errStage = fhr, fstage // transient tkPaneError — carried into the burst Warn
			}
			if !sampled {
				// A FAILED read is not a zero occlusion (OccludedRect's
				// zero is a valid value of its own): publish nothing and
				// leave zeroStreak untouched, so one transient COM error
				// can't strip the padding and twelve can't kill the
				// monitor. Persistent failure DEGRADES to slow polling
				// instead of exiting: if the keyboard stays visible there
				// will be no new Showing to restart a dead monitor, and
				// without Framework events nobody else would ever clean
				// the state — so the monitor must outlive the error burst
				// and recover on its own.
				errStreak++
				delay := 250 * time.Millisecond
				if errStreak == 40 {
					// One rate-limited Warn per error BURST (errStreak resets
					// on success), carrying the last failing stage+hr that
					// per-sample silence would otherwise discard.
					log.Warn().Uint64("hwnd", uint64(hwnd)).Int64("monitor", id).Uint64("hr", uint64(errHR)).Str("stage", errStage).Msg("touch keyboard: occlusion sampling failing ~10s; backing off to 2s polls")
				}
				if errStreak >= 40 { // ~10s of failures → back off to 2s
					delay = 2 * time.Second
				}
				if kbd.released.Load() {
					kbd.monitorOwner.CompareAndSwap(id, 0)
					return
				}
				// "Outlive the error burst" is for LIVE sessions. If the
				// session ended while sampling was broken (Hiding event or
				// endKeyboardSession already cleaned the state), a monitor
				// that has been failing ~10s stops squatting on the slot —
				// via the same ping/release protocol as streak expiry, so a
				// racing show re-activates it instead.
				if errStreak >= 40 && keyboardSessionIdle(kbd) {
					if p := kbd.monitorPing.Load(); p != lastPing {
						lastPing, zeroStreak, errStreak = p, 0, 0
					} else {
						kbd.monitorOwner.CompareAndSwap(id, 0)
						if p2 := kbd.monitorPing.Load(); p2 != lastPing && kbd.monitorOwner.CompareAndSwap(0, id) {
							lastPing, zeroStreak, errStreak = p2, 0, 0
						} else {
							return
						}
					}
				}
				time.Sleep(delay)
				continue
			}
			if errStreak >= 40 {
				log.Debug().Int64("monitor", id).Int("errors", errStreak).Msg("touch keyboard: occlusion sampling recovered")
			}
			errStreak = 0
			// The expiry streak counts only ALL-ZERO rects — genuinely no
			// keyboard on screen. A visible floating keyboard (width > 0,
			// height 0) publishes zero PADDING but must keep the monitor
			// alive: if the user then docks it or drags the window under
			// it, occlusion reappears with no new Showing event, and only
			// a live monitor can restore the padding.
			if visible {
				zeroStreak = 0
			} else {
				zeroStreak++
			}
			// A pane occluding THIS window while the state holds no session is
			// live is the inverted-callback race running the other way: a Hiding
			// raised before a Showing but applied after it leaves paneVisible
			// false over a keyboard that is up, and tkCmdPublish then refuses
			// every non-zero sample for as long as the pane lasts. OccludedRect
			// is per-HWND, so a non-empty one is already proof FOR US and needs
			// no second opinion; the streak is only here so a sample taken in the
			// ordinary gap between a pane appearing and its Showing command being
			// applied is not mistaken for the race. WinRT sessions only — a
			// legacy one has no callback pair to invert.
			//
			// A close in progress is excluded outright, not merely outwaited.
			// An ordinary hide clears paneVisible while the pane is still on
			// screen shutting — samples taken then contradict the state for
			// entirely legitimate reasons, and a streak alone would eventually
			// admit them and republish padding under a keyboard that is leaving.
			// platformKeyboardClosing is the same both-records test a re-tap
			// uses, so the exclusion ends exactly when the close does.
			if visible && kbd.expectHiding.Load() && !kbd.paneVisible.Load() && !platformKeyboardClosing() {
				truthStreak++
			} else {
				truthStreak = 0
			}
			if truthStreak >= tkPaneTruthStreak {
				truthStreak = 0
				tkEnqueue(tkCommand{kind: tkCmdPaneTruth, kbd: kbd, gen: id, epoch: epoch, sawPane: true})
			}
			// Publish through the service, not directly: the service
			// checks released AND that this monitor (by owner id) still
			// owns the slot right before applying the sample, fully
			// serializing occlusion writes with release and with monitor
			// replacement.
			tkEnqueue(tkCommand{kind: tkCmdPublish, kbd: kbd, gen: id, height: height, epoch: epoch})

			if zeroStreak >= 12 { // ~3s with no occlusion of THIS window
				if p := kbd.monitorPing.Load(); p != lastPing {
					// A show arrived while the streak matured — restart
					// the cycle instead of exiting.
					lastPing, zeroStreak = p, 0
					continue
				}
				if kbd.expectHiding.Load() {
					// A zero OccludedRect is NOT proof the keyboard closed —
					// per the OccludedRect contract, Y==0 only means the pane
					// does not occlude THIS window (e.g. the window sits above
					// a docked keyboard). This session WILL raise a Hiding
					// (it is a WinRT pane — set at show/Showing/reconcile time,
					// independent of ownership or the Toggle hide-method), so
					// that event is the authoritative close signal: keep the
					// monitor ALIVE at a relaxed poll instead of exiting, so if
					// the window is later moved or maximized under the keyboard,
					// occlusion reappears and padding is restored with NO new
					// Showing. Exit only once Hiding actually ended the session
					// (paneVisible cleared) or the window was released (loop
					// top). Publishing zero padding while Y==0 already happened
					// above.
					//
					// Legacy sessions (expectHiding=false, incl. a user-opened
					// TabTip keyboard we merely advised) are NOT here — the
					// TabTip host raises no Hiding, so waiting for it would
					// strand the session and this monitor forever; they fall
					// through to the zero-streak teardown below.
					if !kbd.paneVisible.Load() {
						kbd.monitorOwner.CompareAndSwap(id, 0)
						return
					}
					// Waiting for a Hiding that may already have been SPENT. If
					// its callback lost the race described over tkStampHiding, the
					// event was applied before the Showing that outlived it, and
					// the record now says a keyboard is up that closed seconds ago;
					// no further event is coming and this loop would sleep here for
					// the rest of the session, with no re-show ever scheduled. What
					// this goroutine sees cannot decide it — a zero rect is equally
					// consistent with a pane docked past this window — so ask the
					// service, which alone may query the pane's own geometry (the
					// fwPane object belongs to its apartment). Until it answers, or
					// if it cannot, nothing changes and the wait continues exactly
					// as before.
					tkEnqueue(tkCommand{kind: tkCmdPaneTruth, kbd: kbd, gen: id, epoch: epoch})
					time.Sleep(time.Second)
					continue
				}
				// No Hiding will come (legacy/Toggle session, or a window with
				// no registration at all). The zero streak is the ONLY teardown
				// signal available. Bind the expire to the CURRENT show
				// generation, release the slot, and let the service clean up.
				// Re-claim if a ping slipped into the release gap.
				expireGen := kbd.sessionGen.Load()
				kbd.monitorOwner.CompareAndSwap(id, 0)
				if p := kbd.monitorPing.Load(); p != lastPing && kbd.monitorOwner.CompareAndSwap(0, id) {
					lastPing, zeroStreak = p, 0
					continue
				}
				tkEnqueue(tkCommand{kind: tkCmdOwnerExpire, kbd: kbd, gen: expireGen})
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
}

// touchKeyboardVisible reports whether the touch keyboard window is already
// on screen (Win10 host window class; only consulted on the legacy path,
// which is unreachable on systems without this window class).
func touchKeyboardVisible() bool {
	className, err := windows.UTF16PtrFromString("IPTip_Main_Window")
	if err != nil {
		return false
	}
	hwnd, _, _ := tkProcFindWindowW.Call(uintptr(unsafe.Pointer(className)), 0)
	if hwnd == 0 {
		return false
	}
	vis, _, _ := tkProcIsWindowVis.Call(hwnd)
	if vis == 0 {
		return false
	}
	const (
		gwlStyle   = ^uintptr(15) // -16
		wsDisabled = 0x08000000
	)
	style, _, _ := tkProcGetWindowLongW.Call(hwnd, gwlStyle)
	return style&wsDisabled == 0
}

// tkInvokeToggle asks the running touch-keyboard host to show itself
// (legacy Win10 path; callers must ensure the keyboard is not visible and
// our window is foreground, since Toggle is not idempotent).
func tkInvokeToggle() bool {
	const clsctxAll = 0x17 // INPROC_SERVER|INPROC_HANDLER|LOCAL_SERVER|REMOTE_SERVER

	var obj *tkTipObj
	hr, _, _ := tkProcCoCreateInstance.Call(
		uintptr(unsafe.Pointer(&tkClsidUIHostNoLaunch)),
		0,
		clsctxAll,
		uintptr(unsafe.Pointer(&tkIidITipInvocation)),
		uintptr(unsafe.Pointer(&obj)),
	)
	if hr != 0 || obj == nil {
		return false
	}
	toggleHr, _, _ := syscall.SyscallN(obj.vtbl.Toggle,
		uintptr(unsafe.Pointer(obj)),
		uintptr(windows.GetDesktopWindow()),
	)
	tkRelease(obj.vtbl.Release, unsafe.Pointer(obj))
	return toggleHr == 0
}

// tkStartTabTip launches the touch-keyboard host process.
func tkStartTabTip() bool {
	base := os.Getenv("CommonProgramW6432")
	if base == "" {
		base = os.Getenv("CommonProgramFiles")
	}
	if base == "" {
		base = `C:\Program Files\Common Files`
	}
	path := filepath.Join(base, "microsoft shared", "ink", "TabTip.exe")
	cmd := exec.Command(path)
	if err := cmd.Start(); err != nil {
		log.Warn().Err(err).Str("path", path).Msg("touch keyboard: failed to start TabTip.exe")
		return false
	}
	go func() { _ = cmd.Wait() }()
	return true
}
