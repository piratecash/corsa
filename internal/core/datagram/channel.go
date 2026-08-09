package datagram

import (
	"strconv"

	"github.com/piratecash/corsa/internal/core/domain"
)

// channel.go names the two facts a receive path can defend about an arrival and
// that nothing derived from the FRAME can substitute for: the transport CHANNEL
// it came in through, and how much this node has been SHOWN about the neighbour
// on the other end of it.
//
// Both exist because the previous rounds keyed channel-relative state on
// domain.PeerIdentity and called that identity "the channel's label". It is not
// one. On a session THIS node dialled, the identity is the welcome address the
// remote chose for itself (§2.1, connauth: the challenge of that handshake
// travels the other way), so a neighbour willing to write somebody else's
// fingerprint into its welcome shared that node's reverse quota, and an answer
// addressed to "the neighbour the question came from" was resolved through the
// identity map and could leave over a DIFFERENT session belonging to the node
// whose name was borrowed.
//
// A channel is not an identity and the two must never be interchangeable, which
// is why ChannelID is a type of its own with its own constructor rather than a
// second meaning bolted onto domain.PeerIdentity — the same argument Label
// (reverse_state.go) and AdmissionKey (admission.go) already make.
//
// Reference: docs/refactoring/datagram-transport.md §2.1.1, §4.2, §4.3, §7.

// ChannelID names ONE live transport channel of this node.
//
// It wraps domain.ConnID because that identifier is already exactly this fact
// and already node-wide: BOTH receive directions mint it from the same
// monotonic sequence (Service.connIDCounter — an accepted connection through
// registerInboundConn, a dialled one through nextConnIDLocked), and it is stable
// for the lifetime of one socket and never reused. There is deliberately NO
// second namespace the way AdmissionKey has one: that key needs two because the
// two directions can defend two DIFFERENT identifiers, while here both defend
// the same one, and inventing a direction discriminator would be a distinction
// the transport does not have.
//
// The `set` field is what makes absence explicit rather than inferred from
// domain.ConnID(0). The counter never issues zero, so the two would agree today
// — and CLAUDE.md's rule is that an optional state is modelled, not read off a
// zero value, precisely so that agreement does not have to be rechecked every
// time the allocator changes.
type ChannelID struct {
	conn domain.ConnID
	set  bool
}

// NetworkChannel names the channel a connection carries. A zero connection id
// yields the zero channel: the counter never issues one, so a caller handing it
// over has nothing to name.
func NetworkChannel(conn domain.ConnID) ChannelID {
	if conn == 0 {
		return ChannelID{}
	}
	return ChannelID{conn: conn, set: true}
}

// NoChannel is the explicit absence of a channel: a frame this node created —
// it crossed no socket to get here. It is spelled out rather than left to
// ChannelID{} so a call site says that is what it means.
func NoChannel() ChannelID { return ChannelID{} }

// IsZero reports whether the value names no channel.
func (c ChannelID) IsZero() bool { return !c.set }

// ConnID returns the connection this channel is. The bool is false for the zero
// value, so a caller cannot address connection zero by accident.
func (c ChannelID) ConnID() (domain.ConnID, bool) {
	if !c.set {
		return 0, false
	}
	return c.conn, true
}

// String renders the channel for logs.
func (c ChannelID) String() string {
	if !c.set {
		return "no_channel"
	}
	return "conn:" + strconv.FormatUint(uint64(c.conn), 10)
}

// IngressAuthority is what THIS NODE has actually been SHOWN about the
// neighbour that handed over a frame.
//
// It is EXPORTED, and that export is the whole point of this round's second
// finding. The level used to be an unexported derivation read by one gate inside
// the conveyor, while the claimed identity itself travelled on into the handler,
// and every hook — described in their public comments as
// "the authenticated neighbour". Foreign code cannot be asked to know that a
// value it is handed is unprovable unless the value says so, so the level
// travels WITH the identity now (IngressPeer.PresentedIdentity) and the
// identity-shaped accessor answers only where the proof exists
// (IngressPeer.Identity).
type IngressAuthority uint8

const (
	// AuthorityClaimed means nothing about the neighbour is proven: the
	// identity is the welcome address of a session this node DIALLED, which the
	// remote chose for itself, and a fingerprint is public.
	//
	// It is the ZERO VALUE deliberately, and that is the opposite of a zero
	// value standing in for a business signal: there are exactly two states, the
	// weaker one is the default, and a value nobody set therefore grants
	// nothing. Making `proven` the zero would hand the strongest verdict to
	// every uninitialised struct.
	AuthorityClaimed IngressAuthority = iota
	// AuthorityProven means the neighbour signed a challenge THIS node
	// generated, with a key whose fingerprint is the identity it presents
	// (connauth.VerifyAuthSession, on an accepted connection). Nobody else can
	// present that identity on that connection.
	AuthorityProven
)

var ingressAuthorityNames = map[IngressAuthority]string{
	AuthorityClaimed: "claimed",
	AuthorityProven:  "proven",
}

// String returns the log and metric label of the level.
func (a IngressAuthority) String() string { return enumName(ingressAuthorityNames, a) }

// Proven reports whether this node has been shown who the neighbour is.
func (a IngressAuthority) Proven() bool { return a == AuthorityProven }

// ---------------------------------------------------------------------------
// Egress
// ---------------------------------------------------------------------------

// egress is WHERE the layer hands one frame over, and it exists so the two
// kinds of hand-over cannot be confused at a call site.
//
// A ROUTED hand-over names a neighbour: the scheduler picked an identity, and
// which of that peer's sockets carries the bytes is the transport's choice
// (§4.3 — the candidate walk). Nothing on that plane remembers the socket, so
// nothing on it can be misled about one.
//
// A CHANNEL hand-over names the socket itself, and it is the only correct shape
// for every frame that belongs to ONE EXCHANGE the layer keeps state about: an
// answer to a `request`, a `response` travelling back to a reverse record's
// upstream, and a forwarded `request`, whose record stores the channel it left
// over so that only that socket may answer it. The first two mean "back to
// whoever asked" and the third means "the way I went out"; resolving any of them
// through the identity map turns it into "to whoever was NAMED" — which on a
// dialled session is a name the other side chose. A channel is this node's own
// socket and cannot be borrowed.
type egress struct {
	peer    domain.PeerIdentity
	channel ChannelID
}

// nextHopEgress hands a frame to a neighbour the scheduler chose. The transport
// picks the socket.
func nextHopEgress(peer domain.PeerIdentity) egress {
	return egress{peer: peer}
}

// channelEgress pins a frame to ONE channel. peer travels along as the name of
// the neighbour for logs and for the writer's own diagnostics; it is NOT how the
// frame is addressed, and an emitter that cannot reach the channel must refuse
// the frame rather than fall back to it.
func channelEgress(channel ChannelID, peer domain.PeerIdentity) egress {
	return egress{peer: peer, channel: channel}
}
