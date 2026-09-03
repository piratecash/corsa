package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// envelope_retention.go — unified message-lifetime model for the gossip
// plane. It replaces the scattered, per-incident lifetime checks (the
// transit StoredAt window, the AutoDeleteTTL-only cleanup case, the
// ttl=0 no-op in messageDeliveryExpired) with ONE classifier plus a
// policy table, consulted at the choke points (admission, cleanup).
//
// Root cause it fixes: the only age bound on transit envelopes was
// anchored on the LOCAL StoredAt, which storeIncomingMessage re-stamps
// on every admission. After the rotating bloom forgets an id (5–10 min)
// a re-injected transit DM is admitted afresh, its StoredAt resets, and
// the in-flight window restarts — so a months-old DM circulates the mesh
// forever (the transit gossip storm).
//
// The first answer to that was an absolute ceiling anchored on the sender's
// CreatedAt, and it was wrong in a way that took a user report to see: a
// date inside a message says nothing about whether the message is
// circulating. It equally describes a live sender's ordinary re-send to a
// recipient who is online right now, and those were being refused for good
// — a DM older than a day could not cross a relay again, ever. The DM
// classes therefore carry NO age ceiling — not a disabled one, none at all:
// the ceiling, its config field and its env var were removed. See
// defaultBroadcastMaxAge for what bounds circulation instead, all of it
// measuring the forwarding rather than the message.
//
// Model decision ("online-overlap"): the MESH holds nothing for a
// long-offline recipient. Transit is strictly ephemeral forwarding, and
// recovery belongs to the sender-owned engine — which has no age ceiling
// of its own, because it emits only into a route that exists and therefore
// costs nothing while it waits.

// EnvelopeClass partitions gossip-plane envelopes by who owns their
// lifetime. Each class maps to exactly one retentionPolicy.
type EnvelopeClass int

const (
	// ClassLocalInbox: this node is the final recipient. Lifetime is
	// owned by chatlog; the s.topics copy is a runtime cache and is
	// never re-propagated by this node.
	ClassLocalInbox EnvelopeClass = iota
	// ClassLocalOutbox: this node authored the DM. Lifetime is owned by
	// the sender-owned delivery engine, and it is NOT bounded by a clock
	// or an attempt count: it ends on the recipient's receipt, on the
	// author withdrawing the message, or on the message's own TTL if it
	// has one. Ageing our own undelivered message out from here would
	// throw away the only copy that can still be delivered.
	ClassLocalOutbox
	// ClassTransitDM: neither party is this node. Forwarding-only and
	// ephemeral, bounded by the forwarding itself (hop budget, in-flight
	// window, rings) rather than by the message's own age.
	ClassTransitDM
	// ClassBroadcast: broadcast/global topics (recipient "*"/empty or a
	// non-DM topic). The ONE class with an age ceiling: nobody owns a
	// global topic's delivery, so nothing else would retire its history.
	ClassBroadcast
	// ClassControlDM: control-DM plane; retry is owned at the app layer,
	// and it rides with the transit DM it belongs to — same bounds.
	ClassControlDM
)

// retentionPolicy is the per-class lifetime/propagation contract.
type retentionPolicy struct {
	// MaxAge is the absolute lifetime ceiling, anchored on the immutable
	// sender CreatedAt (a future-dated CreatedAt beyond clock-skew tolerance
	// is treated as aged, not fresh). Zero means the lifetime is governed by
	// another layer (chatlog / sender-owned engine) and the age ceiling does
	// not apply to this class.
	MaxAge time.Duration
	// Repropagate reports whether this node may re-gossip / relay the
	// envelope onward.
	Repropagate bool
}

// classifyEnvelope partitions a gossip-plane message by topic / sender /
// recipient relative to self. Pure: no locks, no clock. Mirrors the
// split used by isLocalMessage / isTransitEnvelope so the three agree.
func classifyEnvelope(topic, sender, recipient, self string) EnvelopeClass {
	if topic == protocol.TopicControlDM {
		return ClassControlDM
	}
	if topic != "dm" {
		// Any non-DM topic is broadcast/global by contract (its history
		// is local to every node — see isLocalMessage).
		return ClassBroadcast
	}
	if recipient == "*" || recipient == "" {
		return ClassBroadcast
	}
	if recipient == self {
		return ClassLocalInbox
	}
	if sender == self {
		return ClassLocalOutbox
	}
	return ClassTransitDM
}

// retentionPolicyFor maps a class to its policy. broadcastMaxAge bounds
// broadcast, the only class with an age ceiling left; everything else
// carries MaxAge=0 because its lifetime is owned elsewhere — the chatlog,
// the sender-owned engine, or (for transit) the forwarding bounds that
// measure the operation rather than the message.
func retentionPolicyFor(class EnvelopeClass, broadcastMaxAge time.Duration) retentionPolicy {
	switch class {
	case ClassBroadcast:
		return retentionPolicy{MaxAge: broadcastMaxAge, Repropagate: true}
	case ClassTransitDM, ClassControlDM, ClassLocalOutbox:
		return retentionPolicy{MaxAge: 0, Repropagate: true}
	default: // ClassLocalInbox
		return retentionPolicy{MaxAge: 0, Repropagate: false}
	}
}

// envelopeAgeExceeded reports whether an envelope has outlived maxAge,
// anchored on the IMMUTABLE sender CreatedAt (not the resettable local
// StoredAt), so a re-injection cannot revive a dead envelope.
//
// Future-dated CreatedAt does NOT buy immortality: clamping the anchor to
// `now` was wrong — it left age perpetually ~0 until the wall clock reached
// the faked date. Instead a timestamp beyond the clock-skew tolerance is
// treated as AGED (bogus — a message cannot legitimately come from the
// future), while a timestamp within tolerance counts as genuinely fresh.
// maxAge<=0 disables the ceiling (lifetime owned elsewhere); a zero
// CreatedAt has no usable anchor and is left to the other bounds (StoredAt
// in-flight window, transit caps) — transit callers refuse it explicitly.
func envelopeAgeExceeded(createdAt, now time.Time, maxAge, skew time.Duration) bool {
	if maxAge <= 0 || createdAt.IsZero() {
		return false
	}
	if createdAt.After(now.Add(skew)) {
		return true // future beyond clock-skew tolerance → bogus, never "fresh"
	}
	if createdAt.After(now) {
		return false // within skew tolerance → genuinely recent
	}
	return now.Sub(createdAt) > maxAge
}

// envelopeFutureDated reports whether a timestamp claims to come from
// further ahead than clock skew can explain. A message cannot legitimately
// originate in the future, so this is a forgery test, not an age test —
// which is why it survives the removal of the transit age ceiling and is
// applied on its own rather than from inside envelopeAgeExceeded.
func envelopeFutureDated(createdAt, now time.Time, skew time.Duration) bool {
	return !createdAt.IsZero() && createdAt.After(now.Add(skew))
}

// effectiveClockDrift is the tolerance for future-dated timestamps: the
// operator-configured CORSA_MAX_CLOCK_DRIFT_SECONDS (s.cfg.MaxClockDrift) or
// the protocol default. Shared by message-timing validation and the retention
// age ceiling so both treat clock skew identically.
func (s *Service) effectiveClockDrift() time.Duration {
	if s.cfg.MaxClockDrift > 0 {
		return s.cfg.MaxClockDrift
	}
	return protocol.DefaultMessageTimeDrift
}

// defaultBroadcastMaxAge bounds broadcast/global topics, whose history has
// no other bound: nobody owns their delivery, so nothing else would ever
// retire them.
//
// There is NO transit counterpart, and its removal is the point. The
// transit ceiling was 24h (bitchat's store-and-forward TTL) and it could
// not tell the two cases apart that matter:
//
//   - an envelope CIRCULATING on its own, re-admitted forever because the
//     bloom forgets it — the storm this layer was built for;
//   - a LIVE sender re-sending an old message to a recipient who is right
//     there — an ordinary delivery whose only unusual property is the date
//     inside it.
//
// It dropped both, so a message older than a day could never traverse a
// relay again, no matter how many times its author tried. Worse, the drop
// is silent by contract (no hop-ack), so the sender read the relay's
// refusal as a dead uplink and charged it a routing failure — old messages
// were quietly degrading the reputation of working routes.
//
// The knob went with it (CORSA_TRANSIT_MAX_AGE_HOURS, config.TransitMaxAge).
// A node that could still be configured to refuse old DMs would advertise
// protocol v30 — "I carry old messages" — while behaving like a pre-v30
// one, and once the network floor rises and the sender-side re-stamp is
// deleted, that node becomes a silent black hole with no way for a sender
// to tell.
//
// What bounds circulation instead is everything that measures the
// FORWARDING rather than the message: the hop budget (transit_retention.go),
// max_hops on the relay chain, forward-once, the transit in-flight window
// and its per-recipient rings, ingress suppression and the origin-echo
// drop. Those bound what a node carries for other people, which is what
// the ceiling was really for; the date a human typed a message is not that.
const defaultBroadcastMaxAge = 24 * time.Hour

// broadcastMaxAge resolves the effective broadcast ceiling: zero when the
// retention layer is disabled (kill-switch), the configured value when set,
// else the default.
func (s *Service) broadcastMaxAge() time.Duration {
	if !s.cfg.EnvelopeRetentionEnabled {
		return 0
	}
	if s.cfg.BroadcastMaxAge > 0 {
		return s.cfg.BroadcastMaxAge
	}
	return defaultBroadcastMaxAge
}

// envelopeRetentionPolicy resolves the policy for an in-memory envelope
// addressed by (topic, sender, recipient). Pure read of s.identity
// (immutable after New) and s.cfg — takes no locks.
func (s *Service) envelopeRetentionPolicy(topic, sender, recipient string) retentionPolicy {
	self := ""
	if s.identity != nil {
		self = s.identity.Address
	}
	class := classifyEnvelope(topic, sender, recipient, self)
	return retentionPolicyFor(class, s.broadcastMaxAge())
}

// transitAgedOnAdmission reports whether an inbound TRANSIT DM must be
// refused on arrival. Despite the name it is no longer an age test — the
// transit ceiling is gone — but a test of the frame: no created_at at all,
// or one dated beyond clock skew into the future. Only transit is gated
// here; local/broadcast envelopes are retained for local history and
// subscribers. Caller passes now so the decision is testable.
//
// A transit DM with NO usable CreatedAt is refused outright when the
// ceiling is enabled: relay_message skips timestamp validation
// (handleRelayMessage passes validateTimestamp=false) and the outer
// CreatedAt is not bound to the DM signature, so a zero outer timestamp
// would otherwise fall back to the resettable StoredAt window — the very
// re-injection bypass this layer exists to close. We cannot age-bound
// what carries no immutable anchor, so we do not forward it.
func (s *Service) transitAgedOnAdmission(msg incomingMessage, now time.Time) bool {
	self := ""
	if s.identity != nil {
		self = s.identity.Address
	}
	if classifyEnvelope(msg.Topic, msg.Sender, msg.Recipient, self) != ClassTransitDM {
		return false
	}
	// The missing-anchor refusal is checked FIRST, and it no longer depends
	// on the ceiling being on. The two are different rules that happened to
	// share a branch: the ceiling asks "is this too old to carry", which is
	// now nobody's business here, while this asks "does this frame carry a
	// timestamp at all". A legitimate DM always does — SendDirectMessage and
	// buildControlMessageFrame stamp it, and a missing field is rejected at
	// the local send boundary — so a relayed copy without one is anomalous
	// on a path that skips timestamp validation entirely
	// (handleRelayMessage passes validateTimestamp=false).
	if msg.CreatedAt.IsZero() {
		return true
	}
	// Future-dating is refused on its own, and it has to be: it used to be
	// enforced inside envelopeAgeExceeded, which is now unreachable for this
	// class because the ceiling is gone. A relay_message skips timestamp
	// validation entirely (handleRelayMessage passes validateTimestamp=false)
	// and the outer created_at is not covered by the DM signature, so without
	// this a forged date years ahead would be forwarded and land in a
	// recipient's history at the top of their conversation forever.
	// No age test follows, and there is none to reach: transit DMs have no
	// ceiling at all, and the whole point is that this function cannot
	// refuse a message for being old.
	return envelopeFutureDated(msg.CreatedAt, now, s.effectiveClockDrift())
}

// envelopePropagationAged reports whether an envelope must NOT be
// (re-)propagated under its class age ceiling. The single gate shared by the
// store-path emit (storeIncomingMessage) and the fast relay path
// (relayFrameAged). Two ways to be "aged":
//
//   - past the class MaxAge (anchored on the immutable CreatedAt); or
//   - a DM-CLASS message (transit or control) with NO usable CreatedAt —
//     those always carry one when legitimate (buildControlMessageFrame /
//     SendDirectMessage stamp it; a missing field is rejected at the local
//     send boundary), so a relayed copy without it is anomalous and cannot be
//     age-bounded. Broadcast keeps the lenient zero-CreatedAt behaviour (its
//     history is local, not a relay flood).
//
// MaxAge==0 classes (local inbox/outbox, or the disabled kill-switch) are
// never gated.
func (s *Service) envelopePropagationAged(topic, sender, recipient string, createdAt, now time.Time) bool {
	self := ""
	if s.identity != nil {
		self = s.identity.Address
	}
	class := classifyEnvelope(topic, sender, recipient, self)
	// Same reordering as transitAgedOnAdmission: the missing-anchor refusal
	// is a rule of its own and survives the ceiling being off, which is now
	// the default for the DM classes.
	if createdAt.IsZero() {
		return class == ClassTransitDM || class == ClassControlDM
	}
	// Future-dating gates re-propagation for the DM classes for the same
	// reason it gates admission — see transitAgedOnAdmission. Local classes
	// are exempt: our own outbox and inbox are governed by the chatlog and
	// the sender engine, and a clock that jumped forward on this machine
	// must not silently stop delivering the user's own messages.
	if (class == ClassTransitDM || class == ClassControlDM) &&
		envelopeFutureDated(createdAt, now, s.effectiveClockDrift()) {
		return true
	}
	pol := retentionPolicyFor(class, s.broadcastMaxAge())
	if pol.MaxAge <= 0 {
		return false
	}
	return envelopeAgeExceeded(createdAt, now, pol.MaxAge, s.effectiveClockDrift())
}
