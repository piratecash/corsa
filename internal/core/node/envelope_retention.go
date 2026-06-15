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
// forever (the transit gossip storm). The absolute ceiling here is
// anchored on the IMMUTABLE sender CreatedAt, which a re-injection cannot
// reset; a forging sender cannot extend it either — a CreatedAt beyond the
// clock-skew tolerance into the future is treated as AGED, not as fresh.
//
// Model decision ("online-overlap"): there is no durable long-offline
// delivery. Transit is strictly ephemeral forwarding; durable recovery
// is the sender-owned engine, bounded by its own attempts cap / ttl.

// EnvelopeClass partitions gossip-plane envelopes by who owns their
// lifetime. Each class maps to exactly one retentionPolicy.
type EnvelopeClass int

const (
	// ClassLocalInbox: this node is the final recipient. Lifetime is
	// owned by chatlog; the s.topics copy is a runtime cache and is
	// never re-propagated by this node.
	ClassLocalInbox EnvelopeClass = iota
	// ClassLocalOutbox: this node authored the DM. Lifetime is owned by
	// the sender-owned delivery engine (finite: attempts cap / ttl).
	ClassLocalOutbox
	// ClassTransitDM: neither party is this node. Forwarding-only and
	// ephemeral — subject to the absolute age ceiling.
	ClassTransitDM
	// ClassBroadcast: broadcast/global topics (recipient "*"/empty or a
	// non-DM topic). Previously had NO age bound at all.
	ClassBroadcast
	// ClassControlDM: control-DM plane; retry is owned at the app layer,
	// so the ceiling is only a backstop against runaway re-circulation.
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

// retentionPolicyFor maps a class to its policy. transitMaxAge bounds the
// transit and control-DM backstop; broadcastMaxAge bounds broadcast.
// Local classes carry MaxAge=0 — their lifetime is owned elsewhere.
func retentionPolicyFor(class EnvelopeClass, transitMaxAge, broadcastMaxAge time.Duration) retentionPolicy {
	switch class {
	case ClassTransitDM:
		return retentionPolicy{MaxAge: transitMaxAge, Repropagate: true}
	case ClassControlDM:
		return retentionPolicy{MaxAge: transitMaxAge, Repropagate: true}
	case ClassBroadcast:
		return retentionPolicy{MaxAge: broadcastMaxAge, Repropagate: true}
	case ClassLocalOutbox:
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

// defaultTransitMaxAge / defaultBroadcastMaxAge are the node-package
// fallbacks used when config carries no explicit value (e.g. literal
// config.Node in tests). 24h matches bitchat's store-and-forward TTL —
// long enough to bridge ordinary reconnect gaps, short enough that a
// long-gone recipient's traffic dies instead of circulating forever.
const (
	defaultTransitMaxAge   = 24 * time.Hour
	defaultBroadcastMaxAge = 24 * time.Hour
)

// transitMaxAge resolves the effective transit/control ceiling: zero when
// the retention layer is disabled (kill-switch — restores the legacy
// no-ceiling behaviour), the configured value when set, else the default.
func (s *Service) transitMaxAge() time.Duration {
	if !s.cfg.EnvelopeRetentionEnabled {
		return 0
	}
	if s.cfg.TransitMaxAge > 0 {
		return s.cfg.TransitMaxAge
	}
	return defaultTransitMaxAge
}

// broadcastMaxAge resolves the effective broadcast ceiling. See
// transitMaxAge for the kill-switch / config / default precedence.
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
	return retentionPolicyFor(class, s.transitMaxAge(), s.broadcastMaxAge())
}

// transitAgedOnAdmission reports whether an inbound message is a TRANSIT
// DM that has already outlived the transit ceiling. Only transit is
// gated at admission — local/broadcast envelopes are retained for local
// history/subscribers and aged out by cleanup instead of refused on
// arrival. Caller passes now so the decision is testable.
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
	maxAge := s.transitMaxAge()
	if maxAge <= 0 {
		return false // ceiling disabled (kill-switch) — legacy behaviour
	}
	if msg.CreatedAt.IsZero() {
		return true // no immutable anchor → cannot bound a transit forward
	}
	return envelopeAgeExceeded(msg.CreatedAt, now, maxAge, s.effectiveClockDrift())
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
	pol := retentionPolicyFor(class, s.transitMaxAge(), s.broadcastMaxAge())
	if pol.MaxAge <= 0 {
		return false
	}
	if createdAt.IsZero() {
		return class == ClassTransitDM || class == ClassControlDM
	}
	return envelopeAgeExceeded(createdAt, now, pol.MaxAge, s.effectiveClockDrift())
}
