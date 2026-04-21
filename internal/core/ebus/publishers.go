package ebus

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// Typed publish helpers for the domain-typed topics.
//
// Bus.Publish accepts args ...interface{} and routes them to handlers via
// reflection, so a direct call like
//
//	bus.Publish(TopicIdentityAdded, string(id))
//
// compiles cleanly today and only fails at runtime inside safeCall when
// reflection fails to match the subscriber signature. That defeats the
// purpose of putting domain types on the payload structs — the boundary is
// only as strong as the weakest call site.
//
// These helpers close that gap for every topic whose handler signature is
// specified on topics.go. Call the helper instead of Bus.Publish at every
// new publish site; reviewers should treat a raw bus.Publish of one of
// these topics as a bug. The helper is the only channel through which the
// compiler can enforce the payload contract — the payload struct alone
// cannot, because Publish erases the type at the argument boundary.

// PublishContactAdded emits TopicContactAdded with a typed ContactAddedEvent.
func PublishContactAdded(bus *Bus, ev ContactAddedEvent) {
	if bus == nil {
		return
	}
	bus.Publish(TopicContactAdded, ev)
}

// PublishContactRemoved emits TopicContactRemoved with a typed peer identity.
func PublishContactRemoved(bus *Bus, identity domain.PeerIdentity) {
	if bus == nil {
		return
	}
	bus.Publish(TopicContactRemoved, identity)
}

// PublishIdentityAdded emits TopicIdentityAdded with a typed peer identity.
func PublishIdentityAdded(bus *Bus, identity domain.PeerIdentity) {
	if bus == nil {
		return
	}
	bus.Publish(TopicIdentityAdded, identity)
}

// PublishPeerConnected emits TopicPeerConnected with a typed
// (address, identity) pair. Use this instead of bus.Publish at every new
// call site so the compiler enforces the (PeerAddress, PeerIdentity)
// order — a raw variadic publish would allow the two strings to be swapped
// silently.
func PublishPeerConnected(bus *Bus, address domain.PeerAddress, identity domain.PeerIdentity) {
	if bus == nil {
		return
	}
	bus.Publish(TopicPeerConnected, address, identity)
}

// PublishPeerDisconnected emits TopicPeerDisconnected with a typed
// (address, identity) pair.
func PublishPeerDisconnected(bus *Bus, address domain.PeerAddress, identity domain.PeerIdentity) {
	if bus == nil {
		return
	}
	bus.Publish(TopicPeerDisconnected, address, identity)
}

// PublishSlotStateChanged emits TopicSlotStateChanged with a typed peer
// address and the new slot-state string. An empty state string signals
// slot removal (documented contract on TopicSlotStateChanged).
func PublishSlotStateChanged(bus *Bus, address domain.PeerAddress, state string) {
	if bus == nil {
		return
	}
	bus.Publish(TopicSlotStateChanged, address, state)
}

// PublishMessageSent emits TopicMessageSent with a typed MessageSentResult.
func PublishMessageSent(bus *Bus, result MessageSentResult) {
	if bus == nil {
		return
	}
	bus.Publish(TopicMessageSent, result)
}

// PublishMessageSendFailed emits TopicMessageSendFailed with a typed result.
func PublishMessageSendFailed(bus *Bus, result MessageSendFailedResult) {
	if bus == nil {
		return
	}
	bus.Publish(TopicMessageSendFailed, result)
}

// PublishFileSent emits TopicFileSent with a typed FileSentResult.
func PublishFileSent(bus *Bus, result FileSentResult) {
	if bus == nil {
		return
	}
	bus.Publish(TopicFileSent, result)
}

// PublishFileSendFailed emits TopicFileSendFailed with a typed result.
func PublishFileSendFailed(bus *Bus, result FileSendFailedResult) {
	if bus == nil {
		return
	}
	bus.Publish(TopicFileSendFailed, result)
}
