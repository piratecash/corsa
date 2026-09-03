package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// legacy_transit_restamp.go — the whole of the temporary measure that keeps
// old messages moving across a network that is not yet all on protocol v30.
//
// TODO(transit-age-restamp-removal): DELETE THIS FILE, its test and
// config.ProtocolVersionNoTransitAgeCeiling once
// config.MinimumProtocolVersion reaches 30. From then on every node carries
// an old DM unchanged and an envelope can travel with the date its author
// actually wrote it at. The single call to legacyTransitRestampNeeded() is
// what makes the whole file dead code at that moment, and the test
// TestTransitAgeRestampIsStillNeeded fails to say so.
//
// Why it exists: a node below v30 refuses to forward a DM older than its own
// 24-hour ceiling and, by contract, answers NOTHING (no hop-ack). The sender
// reads that silence as a dead uplink, charges the route a failure and fails
// over to another uplink that refuses it too. So a message older than a day
// could not cross a mixed-version mesh at all, and every attempt degraded a
// working route. Until the floor rises, whatever this node hands to TRANSIT
// carries a fresh created_at.
//
// Why it is safe: created_at on the wire is not covered by the DM signature
// — directmsg.marshalUnsignedEnvelope signs version/from/to plus the two
// sealed blocks — and the plaintext's own copy inside the ciphertext is read
// by nobody. The message id does not change, so a recipient who already
// holds the message dedupes it by id and answers with the delivered receipt
// the sender has been waiting for. Only a recipient who never received it
// sees the re-send date instead of the writing date, which is the price of
// reaching them at all.
//
// WHERE IT IS DECIDED, and why that is the whole design: AT THE DOORS. A
// message enters this node in exactly three ways, and each one normalises
// the date once:
//
//   - storeIncomingMessage — ADMISSION, the door every foreign message comes
//     through: both push_message handlers, the relay fallback
//     (deliverRelayedMessage), the inbox paths. Transit only;
//   - handleRelayMessage — the relay FAST path, which forwards before
//     admission and so needs its own;
//   - dispatchEnvelopeRetry — a message this node authored, coming out of
//     its own outbox.
//
// After a door, the envelope this node handles has ONE date, and every
// onward emission copies it: gossip, directed relay, the relay-retry
// contour, the stored copy in s.topics, the failover line.
//
// Three earlier shapes of this fix all failed the same way, by deciding
// somewhere on the way OUT. In the frame builders, five places had to answer
// "is this target the recipient" from an address — and addresses come in
// several shapes ("inbound:<remote>"), so an inbound recipient was not
// recognised. At the transport exits, every exit had to be found — and
// writeFrameToInboundConnErr serialises its own frame and passed none of
// them. At "the start of the outgoing path", every way OUT was covered but
// not every way IN: a transit push_message reached gossip with its original
// date.
//
// Exits and outgoing paths are open-ended; a new send site is added without
// anyone thinking about dates. Doors are few, enumerable, and already the
// place this node decides what a message IS.
//
// The price, stated plainly: a recipient who is directly connected also sees
// the re-send date rather than the writing date, for a message older than
// legacyTransitRestampAfter that had not reached them. That is the same
// price every other recipient pays and it buys determinism; the alternative
// bought a promise that held only on the paths somebody had remembered.
//
// Where it does NOT apply: BROADCAST (legacyTransitAddressed), which keeps a
// real age ceiling of its own and would be handed a fresh lifetime per hop;
// and messageFrame() — fetch_inbox / fetch_messages, where the node is
// answering a query about its own history rather than forwarding anything.

// legacyTransitRestampAfter is how old a message has to be before its
// transit copy is re-dated. Below it the real date is kept, because
// replacing a true timestamp with a slightly different one buys nothing.
//
// Half an hour, and the number is a compromise between two errors of
// opposite kinds.
//
// Too high and the measure fails to do its job: CORSA_TRANSIT_MAX_AGE_HOURS
// took whole hours, so the smallest ceiling a pre-v30 node can be running is
// ONE hour, and a frame is built before it is written — queuePeerFrame
// freezes it, flushPendingPeerFrames sends it later — then still has hops to
// cross. A threshold at the ceiling itself would let a 59-minute-old message
// keep its date and arrive already expired.
//
// Too low and it rewrites dates it had no reason to touch. Every re-stamp
// costs a recipient who never received the message the true time it was
// written, so the measure should reach only messages whose real date is
// actually what gets them refused. Thirty minutes leaves half of the worst
// ceiling for the queue, the flush and the remaining hops, while leaving
// ordinary traffic — anything delivered within half an hour of being
// written, which is very nearly all of it — carrying its real timestamp.
const legacyTransitRestampAfter = 30 * time.Minute

// legacyTransitSmallestCeiling is the shortest age ceiling a pre-v30 node
// can have been configured with: CORSA_TRANSIT_MAX_AGE_HOURS parsed whole
// positive hours. It exists so the margin above is stated rather than
// assumed, and is pinned by a test.
const legacyTransitSmallestCeiling = time.Hour

// legacyTransitRestampNeeded reports whether the network floor still
// contains nodes that would refuse an old DM. It is the ONE condition
// guarding every use of this file.
func legacyTransitRestampNeeded() bool {
	return config.MinimumProtocolVersion < config.ProtocolVersionNoTransitAgeCeiling
}

// legacyTransitAddressed reports whether a frame is the kind this measure
// applies to: an ADDRESSED direct or control message. Broadcast is excluded
// deliberately and this is the whole reason the topic and recipient are
// threaded through the builders. Broadcast keeps a real age ceiling
// (BroadcastMaxAge) because nobody owns its delivery, and re-dating one
// would hand it a fresh lifetime on every hop — turning the measure into
// exactly the re-circulation bypass the retention layer exists to stop.
func legacyTransitAddressed(topic, recipient string) bool {
	if topic != "dm" && topic != protocol.TopicControlDM {
		return false
	}
	return recipient != "" && recipient != "*"
}

// legacyTransitStamp is the date AND the TTL a transit copy should carry.
//
// The two cannot be decided separately, because TTL is stored as a duration
// from created_at: moving the date forward without shortening the TTL moves
// the DEADLINE with it, so a message with an hour to live would get another
// hour at every hop, and one that had already expired would come back to
// life. The absolute deadline is what the author set, and it is preserved:
// the new TTL is what remains of it.
//
// Returns the original pair when the measure is not needed, when the frame
// is not an addressed DM, when the message is recent enough to cross a
// pre-v30 node unaided, when there is no date to replace (a transit DM
// without one is refused as anomalous, and inventing one here would launder
// it), or when the deadline has already passed — an expired message is not
// re-dated at all, so the gates that drop it still see the truth.
func legacyTransitStamp(topic, recipient string, createdAt time.Time, ttlSeconds int, now time.Time) (time.Time, int) {
	if !legacyTransitRestampNeeded() || createdAt.IsZero() {
		return createdAt, ttlSeconds
	}
	if !legacyTransitAddressed(topic, recipient) {
		return createdAt, ttlSeconds
	}
	if now.Sub(createdAt) < legacyTransitRestampAfter {
		return createdAt, ttlSeconds
	}
	if ttlSeconds <= 0 {
		return now.UTC(), ttlSeconds // no deadline to preserve
	}
	// Truncating division, never rounding up: the re-dated copy may live a
	// fraction of a second LESS than the author allowed, never more. A
	// remainder under one second leaves the message alone — it is about to
	// expire anyway, and a TTL of zero would mean "no deadline".
	remaining := int(createdAt.Add(time.Duration(ttlSeconds) * time.Second).Sub(now).Seconds())
	if remaining <= 0 {
		return createdAt, ttlSeconds // already expired: let the gates see it
	}
	return now.UTC(), remaining
}

// legacyTransitCreatedAt is legacyTransitStamp for a caller with no TTL to
// carry. Used only where the frame genuinely has none.
func legacyTransitCreatedAt(topic, recipient string, createdAt, now time.Time) time.Time {
	stamped, _ := legacyTransitStamp(topic, recipient, createdAt, 0, now)
	return stamped
}

// legacyTransitWireStamp is legacyTransitStamp for a builder that already
// holds the wire string — the fast-path forward copies the incoming frame's
// fields rather than an Envelope. An unparseable value is passed through
// untouched: this measure is not the place to reject a malformed frame, and
// rewriting what we could not read would destroy evidence.
func legacyTransitWireStamp(topic, recipient, raw string, ttlSeconds int, now time.Time) (string, int) {
	if !legacyTransitRestampNeeded() || raw == "" {
		return raw, ttlSeconds
	}
	createdAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return raw, ttlSeconds
	}
	stamped, ttl := legacyTransitStamp(topic, recipient, createdAt.UTC(), ttlSeconds, now)
	if stamped.Equal(createdAt.UTC()) {
		return raw, ttlSeconds
	}
	return stamped.UTC().Format(time.RFC3339), ttl
}

// legacyTransitAdmissionStamp is the date AND TTL a message keeps once this node has
// admitted it. TRANSIT is normalised — this node is about to forward it and
// a pre-v30 hop would refuse the original date — while a message addressed
// to US, one we authored, or a broadcast keeps what its author wrote: those
// are stored and shown, not forwarded, and their date is not ours to state.
//
// TODO(transit-age-restamp-removal): delete; the caller keeps msg.CreatedAt
// and msg.TTLSeconds.
func (s *Service) legacyTransitAdmissionStamp(msg incomingMessage, now time.Time) (time.Time, int) {
	self := ""
	if s.identity != nil {
		self = s.identity.Address
	}
	// NEITHER party is this node: that, and only that, is transit. The
	// envelope classifier answers ClassControlDM on topic alone, before it
	// looks at who the message is for — so a control DM addressed to US
	// classifies as control and would have been re-dated by a switch on the
	// class. It is ours to read, not to forward.
	if msg.Sender == self || msg.Recipient == self {
		return msg.CreatedAt, msg.TTLSeconds
	}
	switch classifyEnvelope(msg.Topic, msg.Sender, msg.Recipient, self) {
	case ClassTransitDM, ClassControlDM:
		return legacyTransitStamp(msg.Topic, msg.Recipient, msg.CreatedAt, msg.TTLSeconds, now)
	default:
		return msg.CreatedAt, msg.TTLSeconds
	}
}

// legacyTransitRestamp returns the copy of an envelope that may be handed to
// transit. The caller keeps its own value: the entry in awaitingDelivered and
// the chatlog row hold the real date, so nothing the user sees moves.
func legacyTransitRestamp(envelope protocol.Envelope, now time.Time) protocol.Envelope {
	stamped, ttl := legacyTransitStamp(envelope.Topic, envelope.Recipient, envelope.CreatedAt, envelope.TTLSeconds, now)
	if stamped.Equal(envelope.CreatedAt) {
		return envelope
	}
	envelope.TTLSeconds = ttl
	log.Debug().Str("message_id", string(envelope.ID)).Str("recipient", envelope.Recipient).
		Time("original_created_at", envelope.CreatedAt).
		Msg("transit_envelope_restamped_for_legacy_ceiling")
	envelope.CreatedAt = stamped
	return envelope
}
