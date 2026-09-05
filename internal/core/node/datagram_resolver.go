package node

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// datagram_resolver.go satisfies the four seams the datagram layer declares in
// internal/core/datagram/resolver.go — RouteResolver, PeerMetadata,
// DirectSession, NodeSecret — plus the FrameEmitter that hands a finished wire
// line to the transport.
//
// The layer never reaches into the routing table, the session map or a socket;
// it asks these adapters. Everything here is therefore a projection of state
// the node already owns, and the ONE rule that shapes all of it is §4.3's:
// what the plan promises and what the send does must come from the same
// helper. That is why every adapter below funnels through
// peerSendableConnectionsLocked — the same list sendFrameToIdentity walks.
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §6, §6.1, §9.

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// datagramRouteResolver answers the two freshness contracts of §4.3 from the
// node's distance-vector plane.
//
// The split is the same one the file router already makes
// (collectFreshRouteCandidates vs collectRouteCandidates) and for the same
// reason: a locally originated send, the reachability probe and the route plan
// must see a route the table accepted a moment ago, while the transit path can
// afford the ~1–1.5 s coalescing delay of the cached snapshot and cannot
// afford a full-table deep copy per forwarded frame.
type datagramRouteResolver struct {
	service *Service
}

// FreshRoutes reads the per-destination oracle: routing.Table.Lookup takes the
// table's own RLock for an O(K) scan over one identity's claims, filters
// withdrawn and expired entries against the table clock and pre-sorts them.
// This is the same call isPeerReachable uses, which is what stops the probe
// and the send from disagreeing inside the snapshot's republish window.
func (r datagramRouteResolver) FreshRoutes(_ context.Context, dst domain.PeerIdentity) []datagram.RouteHint {
	if r.service.routingTable == nil {
		return nil
	}
	return datagramRouteHints(r.service.routingTable.Lookup(dst))
}

// CachedRoutes reads the coalesced snapshot maintained by the hot-reads
// refresher — an atomic.Pointer load, no table lock, no deep copy.
func (r datagramRouteResolver) CachedRoutes(_ context.Context, dst domain.PeerIdentity) []datagram.RouteHint {
	snapshot := r.service.loadRoutingSnapshot()
	routes, ok := snapshot.Routes[dst]
	if !ok {
		return nil
	}
	return datagramRouteHints(routes)
}

// datagramRouteHints projects routing entries onto the layer's hint shape.
//
// Three translations happen here and nowhere else, which is the point of the
// interface: the layer must not learn that this control plane encodes a
// withdrawal as Hops >= HopsInfinity; it must receive an ABSOLUTE expiry so it
// can judge staleness against the clock at SELECTION time rather than against
// the moment a snapshot was published (§4.3); and it must be told WHICH plane
// answered, which this resolver is the only one able to say about itself.
//
// The attribution keeps both axes and invents neither. RouteEntry.Source is
// carried across unchanged — the trust hierarchy stays the routing table's
// property, and nothing here re-ranks by it — while the plane is mesh because
// this resolver IS the distance-vector plane. That is a statement about the
// resolver, not a default for a missing value: a hint that reached the layer
// through this function was produced by the mesh.
func datagramRouteHints(routes []routing.RouteEntry) []datagram.RouteHint {
	if len(routes) == 0 {
		return nil
	}
	hints := make([]datagram.RouteHint, 0, len(routes))
	for _, route := range routes {
		hints = append(hints, datagram.RouteHint{
			ExpiresAt:   route.ExpiresAt,
			Attribution: domain.MeshRouteAttribution(route.Source),
			NextHop:     route.NextHop,
			Hops:        route.Hops,
			Withdrawn:   route.Hops >= routing.HopsInfinity,
		})
	}
	return hints
}

// ---------------------------------------------------------------------------
// Per-connection peer metadata
// ---------------------------------------------------------------------------

// datagramSendTarget is ONE concrete connection to a peer, together with
// everything both the ranking and the write path need from it.
//
// It exists so that "the metadata the scheduler ranks by" and "the socket the
// emitter writes into" are literally the same record. The file router learned
// this the hard way: aggregating max(version) from one socket with the oldest
// connectedAt from another promised an inbound path of a newer version while
// the bytes left over an outbound session of an older one.
type datagramSendTarget struct {
	// session is the outbound peerSession this target IS, and on that tier it
	// is also HOW the frame is handed over: the queue element goes to this
	// object rather than to whatever session the peer's address resolves to at
	// the moment of the hand-over. nil discriminates the other tier — an
	// accepted connection owns no upper queue and is addressed by connID.
	//
	// Addressing by the record instead of by the address is not a
	// micro-optimisation: between the selection and the enqueue the peer can
	// reconnect, and s.sessions[address] is then a DIFFERENT socket with a
	// different handshake and a different ConnID. The scheduler judged this
	// one.
	session *peerSession
	// conn is the layer-facing metadata of THIS connection.
	conn datagram.PeerConnection
	// connID is the channel this target IS. A frame the layer pinned to a
	// channel — an answer, a response travelling to a record's upstream, or a
	// forwarded request whose reverse record stores the channel it left over —
	// may leave over this target only if the two match: the layer's pin exists
	// precisely because addressing by NAME hands the frame to any session of
	// the node whose name the exchange carried, which on a dialled session is
	// a name the neighbour chose for itself.
	//
	// It is also what the layer RANKED, since datagramPeerConnection carries the
	// same id into PeerConnection.Channel: the connection the scheduler judged
	// and the connection the pin names are one record, not two lookups that can
	// disagree. On the accepted tier it is additionally the WRITE ADDRESS, for
	// the same reason session is one on the other tier.
	connID domain.ConnID
}

// datagramTargetsOnChannel narrows the walk to the ONE connection a pinned
// frame may leave over.
//
// An empty result is the correct answer and not a degradation: the return path
// of the question is gone, and falling back to another socket of the peer whose
// NAME the question carried is exactly the substitution the pin exists to
// prevent. The caller turns it into the refusal every other unreachable target
// produces, so the attempt still gets its one terminal.
func datagramTargetsOnChannel(targets []datagramSendTarget, channel domain.ConnID) []datagramSendTarget {
	for _, target := range targets {
		if target.connID == channel {
			return []datagramSendTarget{target}
		}
	}
	return nil
}

// datagramSendTargetsLocked returns the connections a datagram send would try,
// in the order it would try them.
//
// It is a thin projection of peerSendableConnectionsLocked — the canonical
// attempt order shared with sendFrameToIdentity and the file router — with the
// raw handshake declarations of §2.2 / §6.1 folded in per connection. Using
// the shared helper is not a style choice: §4.3 makes it a contract that the
// liveness filter and the send agree on which sockets exist, "or you get
// candidates you cannot send into, and sends that were not in the plan".
//
// The required capability is the NEGOTIATED mesh_datagram_v1, the same gate
// both inbound dispatchers apply before handing a frame to the ingress. This
// helper answers "which sockets of this peer are alive, in attempt order", and
// nothing about a particular frame — that question has its own helper below,
// and every path that really sends bytes goes through THAT one.
//
// The caller must hold s.peerMu (R or W). The returned slice is safe to retain
// after the lock is released: every field is a value or an immutable handle.
func (s *Service) datagramSendTargetsLocked(peer domain.PeerIdentity, now time.Time) []datagramSendTarget {
	candidates := s.peerSendableConnectionsLocked(peer, domain.CapMeshDatagramV1, now)
	if len(candidates) == 0 {
		return nil
	}
	targets := make([]datagramSendTarget, 0, len(candidates))
	for _, candidate := range candidates {
		target, ok := s.datagramSendTargetLocked(candidate)
		if !ok {
			continue
		}
		targets = append(targets, target)
	}
	return targets
}

// datagramFrameSendTargetsLocked returns the connections of peer that may
// carry THIS frame, in the order the send would try them.
//
// It is the selection every emission goes through, and the reason it exists
// apart from the liveness list above is the sentence of §4.3 that says the
// metadata a candidate is judged by "describes ONE concrete connection — the
// one the send will try — and comes from the same helper". The scheduler judged
// the HEAD of the liveness list (PeerMetadata.SendableConnection) and ran the
// full gate set of §4.3 over it: the role gate of §6 and the last-hop dtype
// gate. The emitter then walked the WHOLE list, so the bytes
// could leave over a second socket of the same peer whose advertised set and
// declared dtypes nobody had looked at — a frame refused for the head being
// incapable was delivered anyway through a connection that was more incapable
// still.
//
// Filtering here rather than truncating the walk to the head is the choice
// §4.3 supports: the fall-back socket of one peer is a legitimate part of the
// attempt order (item 3 — "try in order until one queue accepts"), and what the
// spec forbids is not the fall-back but a fall-back nobody gated. With this
// filter the guarantee is structural: every connection the frame can leave
// through has passed exactly the checks the candidate's metadata was judged by.
//
// The caller must hold s.peerMu (R or W).
func (s *Service) datagramFrameSendTargetsLocked(
	frame protocol.DatagramFrame,
	peer domain.PeerIdentity,
	now time.Time,
) []datagramSendTarget {
	live := s.datagramSendTargetsLocked(peer, now)
	if !datagramFrameIsGated(frame) {
		return live
	}
	admitted := make([]datagramSendTarget, 0, len(live))
	for _, target := range live {
		if !admitDatagramSendTarget(frame, peer, target.conn) {
			continue
		}
		admitted = append(admitted, target)
	}
	return admitted
}

// datagramFrameHeadTargetLocked answers the layer's PeerMetadata /
// DirectSession question: which ONE connection of this peer would the send
// offer THIS frame to first, and is there any usable connection at all.
//
// The three-part contract is the layer's, stated word for word in
// datagram/resolver.go, and each part answers a different failure:
//
//   - the FIRST connection, in the send's own attempt order, that passes
//     AdmitPeer for this frame. That is what makes "the metadata describes the
//     connection the send will try" (§4.3 line 574) true when the head is not
//     the connection that ends up carrying the bytes;
//   - when NO connection passes, the first LIVE one, with ok = true. The
//     refusal belongs to the layer, which re-applies AdmitPeer and needs a
//     connection to name it with; answering false here would collapse "gated
//     out" (`rejected`, pointless to wait on) into "not connected"
//     (`no_route`, worth waiting on), and §4.3 keeps those apart;
//   - ok = false only when the peer has no usable connection at all.
//
// The gate is datagram.AdmitPeer and never a second spelling of the rule: one
// decision, two callers (this one and the emitter's per-connection filter), so
// a third gate added inside the layer cannot be lost here.
//
// The caller must hold s.peerMu (R or W).
func (s *Service) datagramFrameHeadTargetLocked(
	frame protocol.DatagramFrame,
	peer domain.PeerIdentity,
	now time.Time,
) (datagramSendTarget, bool) {
	live := s.datagramSendTargetsLocked(peer, now)
	if len(live) == 0 {
		return datagramSendTarget{}, false
	}
	if !datagramFrameIsGated(frame) {
		return live[0], true
	}
	for _, target := range live {
		if admitDatagramSendTarget(frame, peer, target.conn) {
			return target, true
		}
	}
	return live[0], true
}

// datagramFrameIsGated reports whether the candidate gates of §4.3 apply to
// the hand-over of this frame at all.
//
// They do not for `response`, and that is a property of the plane rather than
// an exemption: a response is not routed and has no candidate. Its next hop is
// the `upstream` of the reverse-state record (§4.2), which the layer never put
// through AdmitCandidate — there is no metadata for the sockets to agree with,
// and demanding mesh_datagram_transit_v1 from the neighbour a reply is owed to
// would drop exactly the answers the reverse state exists to deliver.
//
// `routed` and `request` both reach their next hop through candidate selection,
// so both are gated.
func datagramFrameIsGated(frame protocol.DatagramFrame) bool {
	return frame.Mode != domain.DatagramModeResponse
}

// admitDatagramSendTarget applies the full §4.3 gate set to ONE connection.
//
// It delegates to datagram.AdmitPeer rather than composing the role gate and
// the last-hop dtype gate here. The rule is a security rule with two callers —
// the layer's own candidate walk and this emitter — and a second spelling of it
// is how one of them silently loses a check the day a third gate is added.
func admitDatagramSendTarget(
	frame protocol.DatagramFrame,
	peer domain.PeerIdentity,
	conn datagram.PeerConnection,
) bool {
	return datagram.AdmitPeer(frame, peer, conn)
}

// datagramSendTargetLocked resolves one sendable connection into its address
// and its declarations. The bool is false for a connection whose transport
// handle has gone between the snapshot and this read.
//
// The caller must hold s.peerMu (R or W).
func (s *Service) datagramSendTargetLocked(candidate peerSendableConnection) (datagramSendTarget, bool) {
	if candidate.outbound != nil {
		return datagramSendTarget{
			session: candidate.outbound,
			conn: datagramPeerConnection(
				candidate.connectedAt,
				candidate.protocolVersion,
				candidate.outbound.declarations,
				candidate.outbound.connID,
			),
			connID: candidate.outbound.connID,
		}, true
	}
	// Inbound: NetCore owns the authoritative declarations, and there is no
	// peerSession mirror to read them from. coreForIDLocked is the lock-free
	// registry read; NetCore.Declarations takes the connection's OWN mutex,
	// which is a leaf and adds no edge to the domain lock order.
	//
	// The write address is the candidate's own ConnID, which is the registry's
	// primary key — the same one this read just resolved the declarations
	// through. It used to be the "inbound:<remoteAddr>" key, and the sender
	// behind that key walked the tracked inbound connections looking for a
	// matching remote address: after a reconnect that walk finds the NEW socket
	// under the same host:port, so the frame left over a connection whose
	// declarations nobody had read.
	core := s.coreForIDLocked(candidate.inboundID)
	if core == nil {
		return datagramSendTarget{}, false
	}
	return datagramSendTarget{
		conn: datagramPeerConnection(
			candidate.connectedAt,
			candidate.protocolVersion,
			core.Declarations(),
			candidate.inboundID,
		),
		connID: candidate.inboundID,
	}, true
}

// datagramPeerConnection builds the layer's per-connection metadata.
//
// ReportedProtocolVersion is handed over RAW, exactly as the peer claimed it.
// The layer normalizes to min(reported, local) for ranking and keeps the raw
// value for diagnostics (§4.3); pre-capping here would hide an inflation
// attempt from the audit log and would duplicate a rule that already has one
// implementation inside the layer.
//
// The connection id travels with it as the layer's ChannelID, which is what
// turns this record from "the metadata the send will be judged by" into "the
// metadata OF the socket the send will use". The request plane pins its frame to
// that channel and stores it in the reverse record, so an answer is admitted by
// the socket the question left over rather than by the name the answering
// neighbour presents (datagram.Downstream). It is the same identifier
// datagramSendTarget.connID carries, taken from the same candidate in the same
// expression — one fact, one read, no second source to drift from.
func datagramPeerConnection(
	connectedAt time.Time,
	reported domain.ProtocolVersion,
	declarations netcore.HandshakeDeclarations,
	connID domain.ConnID,
) datagram.PeerConnection {
	return datagram.PeerConnection{
		ConnectedAt:             connectedAt,
		Advertised:              datagramAdvertisedCapabilities(declarations),
		DTypes:                  datagramDeclaredDTypes(declarations),
		Channel:                 datagram.NetworkChannel(connID),
		Discovery:               datagramConnectionPlane(),
		ReportedProtocolVersion: reported,
	}
}

// datagramConnectionPlane names the plane every connection of this build was
// found through.
//
// It answers mesh for all of them, and that is a fact rather than a
// placeholder: every path that produces a session here — configured peers, the
// peer provider's candidates, gossip-learned addresses, an accepted inbound
// dial — belongs to the distance-vector plane, and no structured overlay
// exists in the tree to produce any other. The constant lives behind a named
// function so the claim has exactly one place to be revisited when a second
// plane starts opening sessions, instead of being spelled inline at whichever
// call sites happen to build connection metadata by then.
func datagramConnectionPlane() domain.DiscoveryPlane { return domain.DiscoveryPlaneMesh }

// datagramPeerMetadata answers §4.3's "resolve a peer to the ONE connection a
// send would use".
type datagramPeerMetadata struct {
	service *Service
}

// SendableConnection returns the metadata of the connection the emitter would
// offer THIS frame to first, and reports liveness through the bool rather than
// through a field.
//
// Not an aggregate and not a blind head-of-list: datagramSendTargetsLocked is
// sorted outbound-first by the shared helper, and the frame's own gates decide
// which element of that order the send really reaches — see
// datagramFrameHeadTargetLocked for the contract this implements and for why
// "no connection passes" still answers true. A peer with no usable connection
// has no metadata to describe, and a zero-valued PeerConnection would make
// "stalled" look like "connected at the zero time".
//
// The gates are re-applied per connection at emission
// (datagramFrameSendTargetsLocked): the two together are what make "the
// metadata describes the connection the send tries" hold for the head and "no
// ungated socket ever carries the frame" hold for the rest.
func (m datagramPeerMetadata) SendableConnection(
	_ context.Context,
	peer domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (datagram.PeerConnection, bool) {
	m.service.peerMu.RLock()
	defer m.service.peerMu.RUnlock()
	target, ok := m.service.datagramFrameHeadTargetLocked(frame, peer, time.Now().UTC())
	if !ok {
		return datagram.PeerConnection{}, false
	}
	return target.conn, true
}

// datagramDirectSession answers step 1 of §4.3: is the destination itself a
// live neighbour, and what does that connection look like?
//
// It is a distinct interface from PeerMetadata because the two questions are
// asked about different roles — "can my neighbour X relay this" versus "is the
// destination my neighbour" — even though the node backs both with one helper.
// Backing them with one helper is what makes the direct branch and the ranked
// branch agree about liveness; keeping the types apart is what lets a test
// break one without the other.
type datagramDirectSession struct {
	service *Service
}

// LookupDirectSession resolves the destination as a direct neighbour that
// would carry THIS frame, under the same three-part contract SendableConnection
// answers — first admitted connection, first live one when none is admitted,
// false only when the destination is not a live neighbour at all.
//
// §4.3 tries the direct branch FIRST, so a destination whose head connection
// fails the frame's gates while a second one passes must not be demoted to the
// routing table.
func (d datagramDirectSession) LookupDirectSession(
	ctx context.Context,
	dst domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (datagram.PeerConnection, bool) {
	return datagramPeerMetadata(d).SendableConnection(ctx, dst, frame)
}

// ---------------------------------------------------------------------------
// node_local_secret
// ---------------------------------------------------------------------------

// datagramSecretDomainTag separates this derivation from every other use of
// the identity key. Domain separation is not decoration: the same private key
// signs datagrams, announces and DM handshakes, and a derivation without a tag
// would let one construction's output become another's input if a future
// protocol ever hashes the key with a different prefix.
const datagramSecretDomainTag = "corsa/datagram/v1/node-local-secret"

// datagramNodeSecret supplies node_local_secret — the HMAC key behind the
// starting offset of the explore rotation (§4.3).
//
// It is DERIVED from the node's Ed25519 private key rather than generated
// randomly at start, for one reason that matters and one that follows from it:
//
//   - the offset must be stable for the lifetime of the process and identical
//     across restarts of the SAME node, so a restart does not silently
//     re-shuffle which candidate every destination starts at. A random secret
//     would make the rotation's decorrelation property hold between nodes but
//     not across a node's own restarts, and an operator debugging a route
//     would see the first hop change for no observable reason;
//   - it must be unpredictable to an observer, because the whole point of the
//     offset is that nobody outside can predict which candidate a given node
//     tries first for a given destination. Deriving from the private key gives
//     that for free: recovering the secret means recovering the key.
//
// HMAC-SHA256 over the private key with a fixed domain tag is the derivation.
// It is one-way, so the secret leaks nothing about the key even if a future
// change did expose it — and nothing does: the value never leaves the node,
// never reaches a log line and is not part of any diagnostic surface.
type datagramNodeSecret struct {
	secret []byte
}

// newDatagramNodeSecret derives the secret once, at construction.
func newDatagramNodeSecret(privateKey []byte) datagramNodeSecret {
	mac := hmac.New(sha256.New, privateKey)
	mac.Write([]byte(datagramSecretDomainTag))
	return datagramNodeSecret{secret: mac.Sum(nil)}
}

// NodeLocalSecret returns the derived key. The slice is the layer's to read
// and never to publish; see the type comment for why it is not logged.
func (s datagramNodeSecret) NodeLocalSecret() []byte { return s.secret }

// ---------------------------------------------------------------------------
// Frame emitter
// ---------------------------------------------------------------------------

// datagramFrameEmitter hands a finished datagram to the transport.
//
// Three properties are the whole reason it exists as a type of its own:
//
//   - it writes OutboundFrame.Line and never re-serializes. The layer already
//     produced the wire line — the class queue accounts in exactly those bytes
//     and so does the neighbour's inbound budget — and protocol.Frame.RawLine
//     is passed through by MarshalFrameLine untouched, so the bytes that reach
//     the socket are byte-for-byte the ones the layer measured;
//   - it assembles netcore.OutboundWrite from the frame's own class and
//     deadline, so "the writer checks send_until" (§4.2) holds for every frame
//     the layer emits and not only for some of them;
//   - it hands the bytes to EXACTLY ONE queue: the walk below stops at the
//     first connection of the peer that takes the frame. What no layer can
//     promise is exactly one WRITE — a queue answers about admission, and only
//     a refusal at the door proves that nothing left the socket
//     (docs/protocol/network_core.md, "Tracked sends") — so a link that died
//     after writing this frame sends the walk on to the next connection and
//     the receiving side drops the duplicate.
type datagramFrameEmitter struct {
	service *Service

	// selectionBarrier, when non-nil, runs inside EmitTo between the candidate
	// selection and the first hand-over to a queue. Production never installs
	// one, so the call site is a load and a branch.
	//
	// It exists because the window this emitter has to survive — the peer
	// reconnecting after its connections were selected and before the frame is
	// queued — lives entirely between two statements of one function and cannot
	// be produced from outside. A test that approximated it with a sleep would
	// pin the scheduler rather than the addressing, and would pass on an
	// implementation that re-resolves the peer's address at the hand-over
	// whenever the reconnect happened to lose the race. Same shape and same
	// reason as netcore.NetCore.enqueueBarrier.
	//
	// Installed at construction and never changed afterwards.
	selectionBarrier func()
}

// EmitTo places one frame on the neighbour's write queue and reports whether
// the queue accepted it.
//
// "Accepted by the queue" is not "written to the socket", and §4.3 depends on
// exactly that reading: a queued outcome is final for the scheduler, while the
// writer may still drop the frame on its send deadline. The reverse reading is
// not available either: a refusal read after the frame was already queued does
// not prove the bytes stayed home, so false means "not confirmed", not "not
// sent".
//
// The candidate walk mirrors sendFrameToIdentity: outbound sessions first,
// inbound connections as the fall-back, in the order
// datagramFrameSendTargetsLocked produced — which is the order PeerMetadata
// ranked by, MINUS every connection of the peer that fails the frame's own
// gates. Walking the unfiltered list was the hole: the scheduler judged the
// head and the bytes could leave over a socket whose advertised set and
// declared dtypes nobody had checked (§4.3, §6). Nothing here holds peerMu:
// both tracked senders take their own locks.
//
// A frame that never reaches a queue simply reports false. There is nothing to
// close on the way out: the ticket travels one way and brings nothing back, so
// a frame that found no queue leaves no record behind.
func (e datagramFrameEmitter) EmitTo(_ context.Context, out datagram.OutboundFrame) bool {
	write, err := e.outboundWrite(out)
	if err != nil {
		// An unknown class cannot come off the strict parser, so this is a
		// locally constructed frame with a class the matrix never allowed.
		// Refusing it here is the honest answer: writing it with the
		// connection's default deadline would silently give it a hop budget
		// the reverse-state formula of §4.2 was not computed for.
		log.Warn().
			Err(err).
			Str("peer", out.Peer.String()).
			Str("class", out.Class.String()).
			Msg("datagram_emit_unknown_class")
		return false
	}
	if len(out.Line) == 0 || out.Peer.IsZero() {
		return false
	}
	e.service.peerMu.RLock()
	targets := e.service.datagramFrameSendTargetsLocked(out.Frame, out.Peer, time.Now().UTC())
	e.service.peerMu.RUnlock()
	if channel, pinned := out.Channel.ConnID(); pinned {
		// The layer pinned this frame to one channel of one exchange — the
		// channel a question arrived on, or the one a forwarded request must
		// leave over because its reverse record stores exactly that. Narrowing to
		// that connection is the whole point: without it the walk addresses by
		// NAME, and on a dialled session the name is what the neighbour wrote
		// into its own welcome — so an answer to B's question could leave over a
		// session belonging to whoever B called itself, and a request could leave
		// over a socket the record does not expect the answer back on.
		targets = datagramTargetsOnChannel(targets, channel)
	}
	if len(targets) == 0 {
		return false
	}
	e.runSelectionBarrier()

	frame := protocol.Frame{Type: protocol.DatagramFrameType, RawLine: string(out.Line)}
	// The witness is minted here and nowhere else, because this is the last
	// place that still knows WHICH datagram these bytes are: below it the
	// frame is an opaque line. It is nil for all but the handful of frames
	// somebody upstream is waiting to hear about — see watchDatagramWrite.
	return emitOverCandidates(targets, frame, write, e.send, e.service.watchDatagramWrite(out.Frame))
}

// watchDatagramWrite mints a write witness for the frames whose sender keeps a
// per-send record, and nil for every other frame.
//
// Today that is exactly one sender: the liveness prober. The gate is its own
// bookkeeping — "is this label a probe of mine in flight" — rather than a
// property of the frame, because the SAME dtype and mode are used by identity
// resolution, which has no such record and needs no witness. Asking the owner
// is also what keeps this off the hot path: a transit frame, a message, an
// announce all get nil without touching a lock beyond the prober's own leaf
// mutex.
//
// The channel is handed to the prober BEFORE it goes to the transport, so an
// answer that overtakes the write still finds an attempt that knows about it.
func (s *Service) watchDatagramWrite(frame protocol.DatagramFrame) writeWitness {
	prober := s.presenceProber
	if prober == nil || !prober.ownsAttempt(frame.Src) {
		return writeWitness{}
	}
	label := frame.Src
	return writeWitness{mintOne: func() chan struct{} {
		onWire := make(chan struct{})
		if !prober.noteProbeOnWire(label, onWire) {
			// The attempt is already resolved, or has collected as many
			// witnesses as one send can honestly produce. Handing the writer a
			// channel nobody reads would still be correct, but returning nil
			// keeps the walk allocation-free once there is nothing to learn.
			return nil
		}
		return onWire
	}}
}

// runSelectionBarrier fires the test-only synchronisation point described on
// the selectionBarrier field.
func (e datagramFrameEmitter) runSelectionBarrier() {
	if e.selectionBarrier != nil {
		e.selectionBarrier()
	}
}

// candidateSender is the seam emitOverCandidates is tested through: the two
// tracked senders behind one signature.
type candidateSender func(datagramSendTarget, protocol.Frame, *netcore.WriteTicket, chan struct{}) bool

// emitOverCandidates offers the frame to the connections of ONE peer until one
// takes it.
//
// The ticket is built ONCE for the whole walk and the same pointer is offered
// to every candidate. That is sound because a ticket is a read-only carrier:
// it holds the send deadline and the write grace of this frame and nothing
// else, and the writer only asks it questions (expiredAt, writeDeadlineAt).
// The walk used to mint one per candidate, back when the ticket also carried a
// terminal notification guarded by a sync.Once and a refusal would burn it —
// that machinery is gone (netcore/write_ticket.go), and with it the reason to
// pay an allocation for every fallback socket.
//
// What the walk owes its caller is the bool, and it is the ACCEPTING queue's
// answer — the first connection that takes the frame ends the walk, exactly as
// the write contract requires bytes to be offered to one socket only.
func emitOverCandidates(
	targets []datagramSendTarget,
	frame protocol.Frame,
	write netcore.OutboundWrite,
	send candidateSender,
	witness writeWitness,
) bool {
	ticket := netcore.NewWriteTicket(write)
	for _, target := range targets {
		// ONE CHANNEL PER OFFER, never one for the walk.
		//
		// The ack is closed by whichever writer accepted the item, and an
		// accepted item is not the end of the walk: a queue can take the frame
		// and then answer a refusal, because the gate it reads is checked
		// AFTER the offer (settleEnqueuedFrame) — the socket was shut in
		// between. Its writer nevertheless keeps draining what it holds and
		// closes the ack. The walk meanwhile moves on and a second writer
		// accepts the same frame; with a shared channel the second close
		// panics and takes the process down.
		//
		// Per-offer channels make the double-write harmless: two closes land on
		// two channels, and "the frame reached the wire" is the OR of them.
		ack, observable := witness.mint()
		if !observable {
			// A WITNESSED frame that can no longer be witnessed is not offered
			// any further. The walk is over one peer's connections and the
			// per-attempt witness slice is bounded, so a peer holding an
			// unusual number of them could exhaust it — and continuing past
			// that point would put a frame on the wire that nothing is
			// watching. Its silence would then never become a strike, so a
			// contact who is genuinely gone would stay `probing` forever.
			//
			// Stopping instead makes the send report "not queued", which the
			// prober reads as "we never managed to ask" and answers by
			// dropping the attempt — the same treatment a refused send gets,
			// and the honest one.
			// No identifying field: at this depth the frame is an opaque line
			// and the only honest thing to name is the decision itself.
			log.Debug().Msg("datagram: witnessed frame not offered further, witness budget spent")
			return false
		}
		if send(target, frame, ticket, ack) {
			return true
		}
	}
	return false
}

// writeWitness mints one write-observation channel per offer and hands each to
// whoever is collecting them. The zero value mints nothing, which is what every
// frame except a liveness probe gets.
type writeWitness struct {
	// mintOne is nil when nobody is watching this frame.
	mintOne func() chan struct{}
}

// mint returns this offer's channel and whether the walk may continue.
//
// The second value is the part that matters, and it is not the same question as
// the first. An UNWATCHED frame gets a nil channel and a free walk: nobody is
// waiting to hear about it. A WATCHED frame whose collector has no room left
// gets nil and a stop, because offering it anyway would send bytes nothing is
// observing — and for the one caller that watches, an unobserved send is worse
// than no send at all: its silence can never be attributed, so a contact who
// really left would never turn grey.
func (w writeWitness) mint() (chan struct{}, bool) {
	if w.mintOne == nil {
		return nil, true
	}
	ack := w.mintOne()
	return ack, ack != nil
}

// send hands the frame to the one queue this connection has.
//
// Both branches address the CONNECTION the selection chose — the session object
// on one tier, the ConnID on the other — and neither goes back through the
// peer's address. That is the same discipline sendFrameToIdentity already
// follows on the untracked path, and it is what makes the walk's earlier work
// mean anything: a reconnect between the selection and this call rebinds the
// address to a socket with a different handshake and a different ConnID, so an
// address-keyed hand-over would deliver a frame that was gated for one
// connection over another, and a frame PINNED to a channel over a channel its
// reverse record does not name.
func (e datagramFrameEmitter) send(
	target datagramSendTarget,
	frame protocol.Frame,
	ticket *netcore.WriteTicket,
	writeAck chan struct{},
) bool {
	if target.session != nil {
		return e.service.sendTrackedFrameToSession(target.session, frame, ticket, writeAck)
	}
	return e.service.sendTrackedFrameToConn(target.connID, frame, ticket, writeAck)
}

// outboundWrite builds the per-item outbound contract of one frame.
//
// The deadline is the layer's already-clamped send_until, and the write grace
// is the class constant of §4.2: numerically equal to the class's queue
// residence, because the hop budget behind the 240 s reverse-state window is
// queue time PLUS write time, and leaving the write tail at the connection's
// 30 s default would let a legitimate frame reach the next hop long after the
// record that gives it meaning has died.
//
// The contract travels ONE WAY: the TICKET is read-only and tells nobody how
// the write ended, which is what makes the candidate walk above a plain loop
// over a single shared ticket rather than one ticket per offer.
//
// A separate, narrow witness does travel back for the frames whose sender has
// a per-send record to put it in — today only the liveness probe, which must
// tell "they did not answer" from "we never managed to ask". It rides beside
// the ticket rather than inside it (netcore.SendTrackedObserved), and unlike
// the ticket it is minted PER OFFER: an accepted item can still be refused
// afterwards while its writer goes on to close the ack, so a channel shared
// across the walk would be closed twice and panic. The sender therefore
// collects one witness per offer and reads them as an OR.
func (e datagramFrameEmitter) outboundWrite(out datagram.OutboundFrame) (netcore.OutboundWrite, error) {
	grace, err := domain.WriteGrace(out.Class)
	if err != nil {
		return netcore.OutboundWrite{}, err
	}
	return netcore.OutboundWrite{
		SendUntil:  domain.TimeFromNonZero(out.SendUntil),
		WriteGrace: grace,
	}, nil
}
