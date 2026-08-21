package domain

// MessageDeleteRoute is how a user-issued "delete this message" request
// resolves for one chat row.
//
// The local copy always goes away immediately — keeping a message the user
// asked to destroy, waiting for a peer who may be offline for days, is the
// exposure the deletion was meant to end. Nothing, including a delivery
// cancellation that fails, holds that back. What varies is what has to
// happen around it: whether a delivery this node still owns must be
// stopped first, and whether anybody else can still be holding a copy.
//
// Whether the PEER complies is not part of this decision. A deletion the
// user issues is a request to remove the message from the conversation,
// and the conversation has two sides; the author's flag is their answer to
// that request, delivered by their ack, not a filter on whether we make
// it. Deciding here that they would refuse only hides the request from the
// user, who then sees a message vanish from their screen with nothing
// said about the copy that remains.
//
// The classification is shared: the router executes it, the UI reads it to
// describe what just happened, and the RPC surface reports it. Neither side
// invents its own rule.
type MessageDeleteRoute string

const (
	// MessageDeleteRouteWithdraw is an outgoing row the recipient has never
	// confirmed. The delivery may still be sitting in this node's own
	// queues, so it is cancelled first — a message the user deleted must
	// not be handed to the peer afterwards — and only then is the row
	// removed and the peer-side deletion scheduled, in case a copy did get
	// out before the cancellation landed.
	MessageDeleteRouteWithdraw MessageDeleteRoute = "withdraw"

	// MessageDeleteRouteRecalled is the same row when the node can prove
	// the envelope never reached the wire. Nobody else has ever seen the
	// message, so nothing is owed and nothing is scheduled: asking a peer
	// to delete a message they never received would tell them one existed.
	//
	// It is a claim, not a guess — the delivery cancellation reports it
	// only when the sender-owned retry entry was still present and no
	// attempt of it had ever emitted.
	MessageDeleteRouteRecalled MessageDeleteRoute = "recalled"

	// MessageDeleteRouteScheduled has nothing of ours left to cancel but
	// a peer who has to be told: an outgoing row the recipient confirmed,
	// or any incoming one. The row is removed and the peer-side deletion
	// is scheduled — dispatched at once if the peer is reachable, and
	// otherwise the moment they come back. What the peer does with the
	// request is reported by their ack: `deleted` when they honoured it,
	// `denied` when their flag reserved the message to its author, and
	// either way the user is told instead of left guessing.
	MessageDeleteRouteScheduled MessageDeleteRoute = "scheduled"
)

// MessageDeleteContext carries the facts a route is decided from. Peer
// reachability is deliberately NOT among them: it decides WHEN the peer is
// told, which the delete scheduler owns, never WHETHER the user's own copy
// goes away.
type MessageDeleteContext struct {
	// Outgoing is true when this node authored the row.
	Outgoing bool

	// ConfirmedByPeer is true when a delivered or seen receipt for the row
	// has arrived: the recipient demonstrably holds a copy, and no delivery
	// of ours is still in flight to be recalled.
	ConfirmedByPeer bool

	// NeverEmitted is true only when the node PROVED the envelope never
	// reached the wire. It is set after the delivery cancellation runs, so
	// the route is classified twice: once to decide whether to cancel at
	// all, and again with the answer.
	NeverEmitted bool
}

// Route classifies the request.
func (c MessageDeleteContext) Route() MessageDeleteRoute {
	switch {
	case !c.Outgoing:
		// An incoming row: nothing of ours was ever in flight, so there
		// is nothing to cancel — just the local copy to remove and the
		// author to ask.
		return MessageDeleteRouteScheduled
	case c.ConfirmedByPeer:
		return MessageDeleteRouteScheduled
	case c.NeverEmitted:
		return MessageDeleteRouteRecalled
	default:
		return MessageDeleteRouteWithdraw
	}
}

// SchedulesPeerDeletion reports whether the route leaves a durable intent
// behind — the peer still has to be asked, and the request outlives both
// this call and the process. Only a recalled message owes nothing: the
// node proved it never went out, so nobody else can have it.
func (r MessageDeleteRoute) SchedulesPeerDeletion() bool {
	return r == MessageDeleteRouteWithdraw || r == MessageDeleteRouteScheduled
}

// CancelsDelivery reports whether the route has to stop a delivery this
// node may still own before touching the row.
func (r MessageDeleteRoute) CancelsDelivery() bool {
	return r == MessageDeleteRouteWithdraw || r == MessageDeleteRouteRecalled
}
