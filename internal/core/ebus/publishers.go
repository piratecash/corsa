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
