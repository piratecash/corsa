package datagram

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
)

// authorize.go is the authorization hook of §7 — the interface behind the
// pipeline's requirement "authorize at dst == self BEFORE the replay key is
// committed". Without it the migrations would hit an empty space: the file
// transport has to ask its trust policy, receipts have to check the sender is
// admissible and connected to the message they claim to acknowledge.
//
// Three properties are load-bearing:
//
//   - it runs ONLY on local delivery, in all three modes. In `routed` it must
//     finish before the reservation, so a refused frame never occupies a slot
//     of the bounded LRU; in request/response there is nothing to commit and
//     it is simply the last gate before the handler;
//   - `reject` is a SILENT drop WITHOUT committing the replay key, so an
//     authentic but untrusted sender cannot evict other people's records;
//   - in request/response `header.src` is not authenticated — it is a
//     one-shot label, not a sender (§2.1.1). The hook is not entitled to
//     build a decision on it, and DeliveryHeader makes that structural: in
//     those modes there is no accessor returning src as a sender. The
//     authentic sender, if a type needs one, arrives signed INSIDE the
//     payload; the neighbour that handed the frame over arrives as
//     IncomingPeer, with the level of proof behind it attached.
//
// THE FOURTH PROPERTY IS ABOUT IncomingPeer ITSELF, and it is enforced in TWO
// places, neither of them here. Only one direction authenticates: the handshake
// proves the INITIATOR's identity to the RESPONDER, so on a session this node DIALLED the
// welcome address is a name the remote picked, and a hook trusting a fingerprint
// would admit anybody willing to write it down. So:
//
//   - the VALUE says which it is. IngressPeer.Identity answers only where the
//     proof exists, and the claim leaves the layer only together with its level
//     (PresentedIdentity) — so a hook cannot mistake one for the other by
//     reading the field it always read;
//   - a type that DECLARES it needs the proof (SenderProofPolicy) is not
//     delivered to at all on such a direction, with a reason of its own
//     (DropUnprovenSender). That gate is the conveyor's because only it knows
//     how the frame was billed, which is where the proof is recorded
//     (Pipeline.senderProofGate, inboundFrame.authority).
//
// The declaration is what decides, and NOT the presence of a hook in this file.
// A hook that authenticates its sender from a signature in the payload — the
// path §7 describes — reads nothing about the neighbour and must keep running on
// every direction; inferring the requirement from the hook took exactly those
// types off every session this node dialled.
//
// Reference: docs/refactoring/datagram-transport.md §4.1, §7.

// AuthorizationOutcome is the two-way verdict of §7.
type AuthorizationOutcome uint8

const (
	// AuthorizationUnset is the zero value. The layer treats it as a reject:
	// a hook that returned nothing has not accepted anything, and "accepted
	// by omission" is the one reading that must never be inferred silently.
	AuthorizationUnset AuthorizationOutcome = iota
	// AuthorizationAccepted lets the frame reach the handler.
	AuthorizationAccepted
	// AuthorizationRejected is a silent drop with a metric, no ban, and no
	// replay-key commit.
	AuthorizationRejected
)

var authorizationOutcomeNames = map[AuthorizationOutcome]string{
	AuthorizationUnset:    "unset",
	AuthorizationAccepted: "accept",
	AuthorizationRejected: "reject",
}

// String returns the metric label of the verdict.
func (o AuthorizationOutcome) String() string { return enumName(authorizationOutcomeNames, o) }

// AuthorizationDecision is what the hook returns. It is a struct rather than
// a bare enum so a refusal can carry its cause into the log without the
// caller matching on error text.
type AuthorizationDecision struct {
	err     error
	outcome AuthorizationOutcome
}

// Accept admits the frame.
func Accept() AuthorizationDecision {
	return AuthorizationDecision{outcome: AuthorizationAccepted}
}

// Reject refuses the frame, with a reason for the log.
func Reject(err error) AuthorizationDecision {
	return AuthorizationDecision{outcome: AuthorizationRejected, err: err}
}

// Outcome reports the verdict.
func (d AuthorizationDecision) Outcome() AuthorizationOutcome { return d.outcome }

// Accepted reports whether the frame may proceed. The zero value is NOT
// accepted, by construction.
func (d AuthorizationDecision) Accepted() bool { return d.outcome == AuthorizationAccepted }

// Err returns the refusal cause, for logs.
func (d AuthorizationDecision) Err() error { return d.err }

// Authorizer is the read-only authorization hook of a type.
//
// Contract, enforced by the shape of the arguments and by review:
//
//   - READ-ONLY: it touches neither layer state, nor the header, nor a queue,
//     and it has no side effects. Everything it receives is a value copy;
//   - it is called exactly once per local delivery, after the registry has
//     admitted the mode and the class and before the handler;
//   - an unknown dtype never reaches it at all: such a frame is dropped at
//     the registry step and occupies no replay slot (§7).
type Authorizer interface {
	Authorize(ctx context.Context, delivery DeliveryContext, payload []byte) AuthorizationDecision
}

// AuthorizerFunc adapts a plain function to Authorizer.
type AuthorizerFunc func(ctx context.Context, delivery DeliveryContext, payload []byte) AuthorizationDecision

// Authorize implements Authorizer.
func (f AuthorizerFunc) Authorize(ctx context.Context, delivery DeliveryContext, payload []byte) AuthorizationDecision {
	return f(ctx, delivery, payload)
}

// authorizeLocalDelivery runs the hook of a registered type. A type WITHOUT a
// hook is authorized trivially with `accept` (§7) — expressed here once, so
// no call site has to remember it and none can decide it differently.
//
// It is a free function rather than a method of the pipeline because it is a
// pure decision over its inputs: nothing it does depends on pipeline state,
// and keeping it that way is what makes "read-only, no side effects" checkable
// by reading twenty lines.
// A panic in the hook is a REJECT, and this is the one boundary of the layer
// where the conversion needs no argument at all: the zero value of the outcome
// is already documented as a reject because "a hook that returned nothing has
// not accepted anything, and accepted by omission is the one reading that must
// never be inferred silently". A hook that crashed returned nothing.
func authorizeLocalDelivery(
	ctx context.Context,
	entry RegisteredType,
	delivery DeliveryContext,
	payload []byte,
) AuthorizationDecision {
	hook, present := entry.Authorizer()
	if !present {
		return Accept()
	}
	site := hookSite{hook: "Authorize", dtype: delivery.Header().DType()}
	// The PRESENTED name, deliberately: a log line is where an operator looks to
	// see WHICH neighbour a hook crashed on, and refusing to print an unproven
	// name would leave the one arrival worth investigating anonymous. The level
	// travels with it in the log site's own rendering (IngressPeer.String).
	site.peer, _ = delivery.IncomingPeer().PresentedIdentity()
	return guardHook(site, Reject(errHookPanicked), func() AuthorizationDecision {
		return hook.Authorize(ctx, delivery, payload)
	})
}

// admitRegisteredFrame is the registry gate of §4.1 step 10 and of both
// unsigned local branches: mode and class must be admissible for the type.
//
// It is what makes the demotion of §3.6 harmless: a receipt that lost its
// auth block and arrived as `request` is refused here — before the handler,
// before the authorization hook and before any reservation — because its type
// declared `routed` only.
func admitRegisteredFrame(entry RegisteredType, mode domain.DatagramMode, class domain.DatagramClass) DropReason {
	switch {
	case !entry.AllowsMode(mode):
		return DropModeNotAllowedForType
	case !entry.AllowsClass(class):
		return DropClassNotAllowedForType
	default:
		return DropReasonUnset
	}
}
