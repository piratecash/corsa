package datagram

import (
	"errors"
	"runtime/debug"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// hook_guard.go is the layer's boundary around FOREIGN CODE.
//
// The layer is built out of seams: a type owns its handler and its authorizer,
// and the node owns the route resolver, the session lookup, the node secret and
// the writer. None of that code is ours, all of it runs on our goroutines, and
// two of those goroutines serve everything at once — the session reader and the
// outbound pump. A panic in either used to reach crashlog.DeferRecover, which
// logs and RE-PANICS by design, so one broken hook took the whole process down.
//
// # The convention, identical to the node's own boundary
//
//  1. the boundary wraps the FOREIGN CALL, never the loop around it. The
//     caller's own invariants — the reservation release, the reverse slot
//     release, the state transition — must still run, and they only do if the
//     panic is converted before it reaches them;
//  2. the panic becomes the hook's own documented FAILURE VALUE, never a
//     sentinel error and never a silent success. Every seam of this layer has
//     one, and every caller already has a path for a hook that did not do its
//     work;
//  3. it is logged at Error with the operation's identity — which hook, which
//     replay key, which peer — plus the recovered value and the stack. A
//     recovered panic with no identity is a crash report nobody can act on;
//  4. every panic is treated alike, runtime errors included. A boundary that
//     re-panicked on some classes would be a boundary only for the panics
//     somebody thought of.
//
// # Where the line between "foreign" and "ours" is drawn
//
// Foreign is code with NO in-tree implementation behind the interface: the
// type's handler and authorizer, the node's route resolver, peer metadata,
// direct-session lookup, node secret and frame emitter. Those are guarded here.
//
// Ours is everything the layer implements itself and merely lets the owner
// wire — the admission and crypto budgets, the limits, the class queue, the
// registry, the base replay cache, the metric sinks. A panic in those is a bug
// in THIS package, and converting it into a documented failure value would hide
// the bug behind a degraded mode. They are deliberately not guarded.
//
// Reference: docs/protocol/datagram.md §4.5, §7.

// errHookPanicked is the CAUSE carried inside a converted failure, never the
// failure itself: the outcome is the hook's own documented `failed` variant,
// and this only answers "why" for the log and for an errors.Is at a call site
// that wants to tell a crash apart from an honest I/O error.
var errHookPanicked = errors.New("datagram: the hook panicked and did not do its work")

// hookSite identifies ONE foreign call for the crash report. Every field is
// optional because the sites differ in what they know — the send path knows a
// peer, the handler seam knows a dtype — and a zero field is simply not logged.
type hookSite struct {
	// hook is the method name as the contract spells it, e.g. "EmitTo".
	hook string
	// peer is the neighbour the call is about.
	peer domain.PeerIdentity
	// dtype is the type whose handler or authorizer this is.
	dtype domain.DType
}

// logPanic writes the one crash report a recovered panic leaves behind.
//
// debug.Stack() is captured INSIDE the deferred function, while the panicking
// frames are still on the stack: taken anywhere else it would describe the
// recovery instead of the fault, which is the difference between a report an
// owner can act on and a line saying a panic happened somewhere.
func (s hookSite) logPanic(recovered any) {
	event := log.Error().
		Str("hook", s.hook).
		Interface("panic", recovered).
		Str("stack", string(debug.Stack()))
	if !s.peer.IsZero() {
		event = event.Str("peer", s.peer.String())
	}
	if s.dtype != "" {
		event = event.Str("dtype", s.dtype.String())
	}
	event.Msg("datagram: a foreign hook panicked, the layer converted it into its documented failure")
}

// guardHook runs ONE foreign call and converts a panic into `failure`.
//
// It takes the call as a closure rather than being written out at each site so
// that every seam shares ONE recovery, ONE conversion rule and ONE crash
// report. A per-site
// `defer func(){ if r := recover() ... }()` would be the same six lines copied
// thirty times, and the thirty-first site would be the one that forgot.
//
// The failure value is a PARAMETER and not a zero value on purpose: several
// outcome types of this layer have a zero value the layer reads as `failed`
// already, but reading a documented failure out of a zero value is exactly the
// implicit signal CLAUDE.md forbids. The call site names what it means.
func guardHook[T any](site hookSite, failure T, call func() T) (result T) {
	defer func() {
		// recover() has to be called by the deferred function ITSELF: from a
		// nested helper it returns nil and the guard would be decorative.
		recovered := recover()
		if recovered == nil {
			return
		}
		result = failure
		site.logPanic(recovered)
	}()
	return call()
}

// guardHookPair is guardHook for a foreign call that answers a value and the
// bool that says whether the value means anything. The bool is forced to false
// together with the value, because "the hook did not answer" is precisely what
// that bool exists to say.
func guardHookPair[T any](site hookSite, failure T, call func() (T, bool)) (T, bool) {
	type answer struct {
		value T
		ok    bool
	}
	got := guardHook(site, answer{value: failure}, func() answer {
		value, ok := call()
		return answer{value: value, ok: ok}
	})
	return got.value, got.ok
}
