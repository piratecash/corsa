package datagram

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// resolver.go declares everything the route scheduler (§4.3) needs from the
// node and nothing else. The layer never reaches for a routing table, a
// session map or a socket: it asks these interfaces, and the node satisfies
// them in M9.
//
// The indirection is not ceremony. §4.3 states it outright: "the candidate
// source is a RESOLVER INTERFACE, not today's routing table — today a
// distance-vector plane sits behind it, tomorrow the DHT structure, and the
// layer will not notice".
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §6, §6.1.

// ---------------------------------------------------------------------------
// Route hints
// ---------------------------------------------------------------------------

// RouteHint is one route to a destination as the resolver knows it. It is
// deliberately a layer-local shape rather than routing.RouteEntry: the
// scheduler must keep working when the control plane behind the resolver is
// replaced, and it needs exactly four facts to rank a hop — plus one it does
// not rank by at all, see Attribution.
//
// ExpiresAt is an ABSOLUTE deadline, not a remaining TTL, because §4.3
// requires expiry to be judged against the clock AT SELECTION TIME: a
// cached snapshot republishes on a dirty flag, so a finite-TTL route that
// quietly aged out between two publishes still looks alive in the snapshot.
// A relative TTL captured at publish time could not express that.
type RouteHint struct {
	// ExpiresAt is the absolute expiry of the route. The zero value means
	// the route does not expire on its own.
	ExpiresAt time.Time
	// Attribution is who says so and which plane found it — a pair of facts
	// that travels with the route and takes part in NO ranking key (see
	// routeCandidateLess). It is carried because an operator, a metric and
	// step 09's cross-plane comparison all have to tell "arrived over the
	// mesh" from "arrived over the overlay", and because a route learned
	// through the overlay can still be a direct session: one field would
	// keep one of those facts and lose the other.
	//
	// A resolver that fills nothing leaves it UnattributedRoute, which the
	// diagnostics render as absence rather than as a plane. It is not
	// defaulted to mesh here: the layer does not know which plane its
	// resolver is, and guessing would put a claim in an operator's console
	// that nobody made.
	Attribution domain.RouteAttribution
	// NextHop is the neighbour this route goes through.
	NextHop domain.PeerIdentity
	// Hops is the distance to the destination; 1 is directly connected.
	Hops int
	// Withdrawn marks a route the control plane has retracted (the
	// distance-vector plane expresses this as Hops >= HopsInfinity; the
	// adapter translates, so the layer never learns that encoding).
	Withdrawn bool
}

// IsExpired reports whether the route is dead by the given clock. The check
// is inclusive, matching routing.RouteEntry.IsExpired: a route whose
// ExpiresAt equals now is expired, so "ttl_seconds = 0" and "expired" never
// disagree.
func (h RouteHint) IsExpired(now time.Time) bool {
	return !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt)
}

// RouteResolver is the candidate source of §4.3. The two methods exist
// separately because the freshness contract differs and the difference is
// load-bearing, not stylistic:
//
//   - FreshRoutes is the per-destination lookup used by LOCALLY ORIGINATED
//     sends, the reachability probe and the route plan. A route accepted
//     moments before a user-initiated send must be visible immediately;
//     reading the cached snapshot here reproduces the file-transfer
//     regression where the probe says "reachable" and the very next send
//     fails because the snapshot has not republished yet.
//   - CachedRoutes is the coalesced snapshot used by the TRANSIT path,
//     where a frame in flight carries its own budget and a ~1–1.5 s delay
//     on a freshly added route is harmless — while a full table copy per
//     forwarded frame would not be.
//
// Both return the raw route set for dst. Filtering, deduplication and
// ranking belong to the layer (candidates.go), so the two sources cannot
// drift apart in their idea of "which next hop is best".
type RouteResolver interface {
	FreshRoutes(ctx context.Context, dst domain.PeerIdentity) []RouteHint
	CachedRoutes(ctx context.Context, dst domain.PeerIdentity) []RouteHint
}

// ---------------------------------------------------------------------------
// Per-connection peer metadata
// ---------------------------------------------------------------------------

// PeerConnection is the metadata of ONE CONCRETE CONNECTION — never an
// aggregate across a peer's sockets.
//
// §4.3 is explicit about why: the ranking keys (version and connectedAt)
// must come from the connection the live send path would actually try
// first — outbound preferred over inbound — and from the same helper the
// send itself uses. Aggregating (max version from one socket, oldest
// connectedAt from another) already produced a real bug in the file
// router: ranking promised an inbound path of a newer version while the
// bytes left over an outbound session of an older one.
//
// ReportedProtocolVersion is the RAW handshake value, exactly as the peer
// claimed it. The scheduler normalizes it to min(reported, local) for
// ranking and keeps the raw value for diagnostics (§4.3); a resolver that
// pre-capped it would hide an inflation attempt from the audit log.
type PeerConnection struct {
	// ConnectedAt is when this connection was established. The zero value
	// means "unknown" and sorts last in the uptime tie-break.
	ConnectedAt time.Time
	// Advertised is the VALIDATED RAW capability set of this connection — the
	// set kept beside the typed one, so a name this build never heard of
	// still has something to match against.
	Advertised AdvertisedCapabilities
	// DTypes is the dtype set this peer declared in the handshake, and the
	// ONLY statement about which types it can handle as an endpoint. The zero
	// value is the empty set, which is also what an absent field says (§6.1):
	// a peer that declared nothing is an endpoint for nothing.
	DTypes DeclaredDTypes
	// Channel NAMES the connection this metadata describes, turning it from
	// something the layer can only judge into something the layer can also
	// ADDRESS.
	//
	// The request plane is what needs it. A forwarded request's reverse record
	// stores the channel the frame left over, and §4.2 phase 3 requires that
	// channel to be in the record BEFORE the frame is handed to the writer — so
	// learning it from the hand-over is too late by construction, and behind the
	// class queue of §5 it is never learned at all, because the socket is chosen
	// on the far side of a lane the frame may sit in for seconds.
	//
	// A resolver that leaves it zero still serves the routed plane in full: there
	// the scheduler picks a NEIGHBOUR and the transport picks the socket (§4.3).
	// The request plane refuses such a candidate instead of falling back to the
	// peer's name, which is the closed direction — the name is precisely what may
	// not decide where an answer comes back from.
	Channel ChannelID
	// Discovery is the plane this CONNECTION was found through — not the
	// plane of any route that happens to travel over it.
	//
	// It exists for the direct branch of §4.3, where the next hop IS the
	// destination and no RouteHint is involved at all: without it, a session
	// opened because the overlay answered a lookup would be reported as a
	// plain direct neighbour and the overlay's contribution would be
	// invisible exactly where it mattered. The trust axis needs no field
	// here — a live session is RouteSourceDirect by construction.
	//
	// The zero value is DiscoveryPlaneUnset and stays that way for a
	// resolver that names no plane; nothing downstream substitutes mesh for
	// it.
	Discovery domain.DiscoveryPlane
	// ReportedProtocolVersion is what the peer claimed on THIS connection.
	ReportedProtocolVersion domain.ProtocolVersion
}

// PeerMetadata resolves a peer to the single connection a send would use FOR
// ONE FRAME.
//
// Implementations MUST answer from the same helper that picks the socket
// for an actual send (outbound session first, inbound as fall-back, both
// filtered by the same liveness policy). §4.3 makes this a contract rather
// than advice: "the liveness filter follows the same rules by which the
// send picks its socket — let the two sets diverge and you get candidates
// you cannot send into, and sends that were not in the plan".
//
// # Why the frame is an argument
//
// §4.3 line 574 requires the metadata to describe "the connection the send
// will actually try FIRST", and that connection depends on the frame: a peer
// may hold several connections, and the emitter offers the frame only to the
// ones that pass its gates (the role gate of §6 and the last-hop dtype
// gate). A frame-blind head-of-list answer made the requirement true in
// one direction only — the head, when it passed — and false in the other: a
// peer whose HEAD failed the gates was discarded whole, although its second
// connection would have carried the frame, and a live route became
// unreachable while the emitter was ready to use it.
//
// Passing the frame is what makes the two directions one statement. It is the
// frame and not a (dtype, role) pair because that pair IS the frame as far as
// the gates are concerned, and AdmitPeer — the layer's own
// exported decision, which an implementation MUST use rather than re-spelling
// the rule — already takes it in this shape.
//
// # The contract of the answer
//
//   - return the FIRST connection, in the send's own attempt order, that
//     passes AdmitPeer for this frame;
//   - when NO connection of the peer passes, return the first LIVE connection
//     with ok = true anyway. The gate decision belongs to the layer, which
//     re-applies AdmitPeer and needs a connection to name the refusal with;
//     answering false here would collapse "gated out" (`rejected`, pointless
//     to wait on) into "not connected" (`no_route`, worth waiting on), and
//     §4.3 keeps those two apart on purpose;
//   - ok = false means one thing only: the peer has NO usable connection at
//     all — never connected, dropped, or stalled. Liveness is the second
//     return value and not a field, because a zero-valued PeerConnection
//     would make "stalled" indistinguishable from "connected at the zero
//     time".
type PeerMetadata interface {
	SendableConnection(
		ctx context.Context,
		peer domain.PeerIdentity,
		frame protocol.DatagramFrame,
	) (PeerConnection, bool)
}

// DirectSession answers step 1 of §4.3: is there a live direct session to
// dst that would carry THIS frame, and what does that connection look like?
//
// It is separate from PeerMetadata because the two questions are asked at
// different points and about different roles — "can I hand this frame to my
// neighbour X as a relay" versus "is the destination itself my neighbour" —
// even though M9 backs both with one helper. Keeping them apart lets a test
// make the direct path fail while relays stay healthy, which is exactly the
// fall-back §4.3 item 3 describes.
//
// The frame is an argument for exactly the reason it is one on PeerMetadata,
// and the answer obeys the same contract: the first connection that passes the
// frame's gates, the first live one when none passes, false only when the
// destination is not a live neighbour at all. The direct branch is not the
// cheap case here — it is the one §4.3 tries FIRST, so a destination whose
// head connection fails the gates while a second one passes must not be
// demoted to the routing table.
type DirectSession interface {
	LookupDirectSession(
		ctx context.Context,
		dst domain.PeerIdentity,
		frame protocol.DatagramFrame,
	) (PeerConnection, bool)
}

// NodeSecret supplies node_local_secret, the HMAC key that decorrelates the
// explore rotation's starting offset between nodes (§4.3). No context: this
// is a memory read of a value fixed at node start, not I/O.
//
// The secret never leaves the node and never appears in a log: it is the
// only thing that stops an observer from predicting which candidate a given
// node will try first for a given destination.
type NodeSecret interface {
	NodeLocalSecret() []byte
}

// hopSendOutcomeKind is the three-way result of handing a frame to one hop.
type hopSendOutcomeKind uint8

const (
	// hopSendUnset is the zero value; it is never a valid answer.
	hopSendUnset hopSendOutcomeKind = iota
	// hopEnqueued means the write queue accepted the frame.
	hopEnqueued
	// hopFailed means the hand-over to this hop was not CONFIRMED: a saturated
	// queue, a connection dying under the write, or a local step of the request
	// plane that could not be completed.
	//
	// It does not prove the frame stayed home. A queue answers about ADMISSION,
	// and a refusal read once the frame is already in it can follow a completed
	// write on a link that died afterwards (docs/protocol/network_core.md,
	// "Tracked sends"). Retrying the same attempt with a backoff is right
	// anyway, and the reason is the receiver's: the anti-replay cache of §5
	// drops the duplicate.
	hopFailed
)

var hopSendOutcomeKindNames = map[hopSendOutcomeKind]string{
	hopSendUnset: "unset",
	hopEnqueued:  "enqueued",
	hopFailed:    "failed",
}

// String returns the metric label of the kind.
func (k hopSendOutcomeKind) String() string { return enumName(hopSendOutcomeKindNames, k) }

// hopSendOutcome is what one hop answered.
//
// It is a typed outcome rather than a bool because the two negatives §4.3 keeps
// apart start here: a hop that did not confirm the frame is a failure worth a
// retry, while an empty candidate set is the gate verdict the selection
// recorded. Flattening the first into "no route" would send a caller waiting for
// a route it already has.
//
// The `rejected` variant is gone with the caps that produced it: a publisher is
// now an enqueue and nothing else, so the only refusal it can report is that the
// queue would not take the frame.
type hopSendOutcome struct {
	err  error
	kind hopSendOutcomeKind
}

// hopEnqueuedOutcome reports acceptance by the queue.
func hopEnqueuedOutcome() hopSendOutcome {
	return hopSendOutcome{kind: hopEnqueued}
}

// hopFailedOutcome reports a hand-over this node could not confirm.
func hopFailedOutcome(err error) hopSendOutcome {
	return hopSendOutcome{kind: hopFailed, err: err}
}

// Kind reports which of the two happened.
func (o hopSendOutcome) Kind() hopSendOutcomeKind { return o.kind }

// Enqueued reports whether the queue took the frame.
func (o hopSendOutcome) Enqueued() bool { return o.kind == hopEnqueued }

// Err returns the cause of a failed outcome, nil otherwise.
func (o hopSendOutcome) Err() error { return o.err }

// hopPublisher hands the frame to ONE next hop and reports what happened.
//
// It is the half of §4.3 the scheduler does not own: the ORDER of the
// candidates and the OUTCOME vocabulary are the scheduler's, while the
// publication itself — the emitter, the deadline, the reverse record's
// downstream — is the pipeline's. Splitting the walk this way is what keeps a
// single implementation of §4.3 in the tree.
//
// "Enqueued" is not "written to the socket" — §4.3 is precise about this:
// queued means queued, and the writer may still drop the frame later on
// send_until. Every non-enqueued outcome is a refusal of THIS hop and makes
// the scheduler try the next candidate; what differs is what the caller is
// told once the walk ends.
//
// It takes the whole CANDIDATE and not just its identity, because the two planes
// that use it need different amounts of it: the routed plane hands the frame to
// a neighbour and lets the transport pick the socket, while the request plane
// has to pin the frame to the candidate's own channel and store that channel in
// the reverse record before publishing (§4.2). Passing the identity alone made
// the second impossible and hid the fact that a candidate describes ONE
// CONNECTION rather than a peer (§4.3 line 574).
type hopPublisher func(ctx context.Context, hop RouteCandidate) hopSendOutcome

// ---------------------------------------------------------------------------
// Declared dtypes (§6.1)
// ---------------------------------------------------------------------------

// DeclaredDTypes is the dtype set a peer announced in the handshake, in the
// form the last-hop gate asks its question in. It is the layer's view of
// domain.DeclaredDTypeSet, and it holds ONE rule (§6.1):
//
//   - the set is EXACTLY what the peer listed. A name it does not carry is
//     unsupported, and an ABSENT field carries no name at all — a peer that
//     declared nothing is an endpoint for nothing. The zero value of this
//     type is that empty set, which is also what a peer whose handshake was
//     never recorded says;
//   - order is not significant and duplicates collapse: it is a set;
//   - a bounds breach does NOT tear the session down: the whole field is read
//     as absent, hence as the empty set. Dropping a handshake over an
//     extensible field would contradict the point of the layer, and degrading
//     to "this peer is no endpoint" is the conservative direction.
//
// There is deliberately no set a silent peer is credited with. An implied
// baseline existed here for one release that never shipped, and while it did,
// every v27 peer advertising mesh_datagram_v1 counted as an endpoint for four
// types no build implements — an unproven promise the last-hop gate then acted
// on. §6.1 makes unproven support equal to no support, and this type is where
// that is enforced.
//
// The endpoint promise lives HERE and nowhere else. mesh_datagram_v1 says
// only that the envelope is understood, and every node with the layer
// enabled advertises it whatever its registry holds (§6), so "can this peer
// handle this type" has exactly one place to be answered.
//
// The set is fixed for the lifetime of the session: changing it means a new
// build, hence a restart, hence new sessions.
type DeclaredDTypes struct {
	declared map[domain.DType]struct{}
}

// NewDeclaredDTypes projects a parsed declaration into the layer's lookup
// form. An absent field carries no names, so it becomes the empty set.
func NewDeclaredDTypes(set domain.DeclaredDTypeSet) DeclaredDTypes {
	types := set.Types()
	declared := make(map[domain.DType]struct{}, len(types))
	for _, dtype := range types {
		declared[dtype] = struct{}{}
	}
	return DeclaredDTypes{declared: declared}
}

// Supports reports whether the peer can handle this dtype as an ENDPOINT.
//
// The declared field IS the set, in both directions: a name it carries is
// supported, and a name it does not — including one this build happens to
// implement itself — is not.
func (d DeclaredDTypes) Supports(dtype domain.DType) bool {
	_, ok := d.declared[dtype]
	return ok
}
